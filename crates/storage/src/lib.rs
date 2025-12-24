//! # Multi-Tenant Hybrid Storage Engine - CameoDB
//!
//! This crate provides a production-grade multi-tenant hybrid storage engine that combines:
//! - **redb**: ACID-compliant shared key-value storage for durability and consistency
//! - **tantivy**: Per-index full-text search indexing for query capabilities
//!
//! ## Multi-Tenant Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────┐
//! │           HybridStore                   │
//! ├─────────────────────────────────────────┤
//! │ Shared redb Database                    │
//! │ ├── data_index1 table                   │
//! │ ├── wal_index1 table                    │
//! │ ├── data_index2 table                   │
//! │ ├── wal_index2 table                    │
//! │ └── schema table (shared)               │
//! │                                         │
//! │ Per-Index Tantivy Indices               │
//! │ ├── indices/index1/                     │
//! │ └── indices/index2/                     │
//! └─────────────────────────────────────────┘
//! ```

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use tantivy::query::QueryParserError;
use tantivy::schema::{Document, Field, STORED, STRING, Schema, TEXT};
use tantivy::{Index, IndexReader, IndexWriter, doc};
use thiserror::Error;

/// Schema metadata table: maps index names to their schema definitions.
const TABLE_SCHEMA: TableDefinition<&str, &[u8]> = TableDefinition::new("schema");

/// Configuration for the multi-tenant hybrid storage engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// The root folder for this shard's data files.
    pub shard_path: PathBuf,
    /// Default memory budget for each tantivy IndexWriter in bytes.
    pub writer_memory_budget: usize,
    /// Minimum memory budget for IndexWriter in bytes.
    pub writer_memory_min_mb: usize,
    /// Maximum memory budget for IndexWriter in bytes.
    pub writer_memory_max_mb: usize,
    /// Default batch size for smart commit calculations.
    pub default_batch_size: usize,
    /// Whether to call fsync() on every redb commit.
    pub wal_sync: bool,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            shard_path: PathBuf::from("/tmp/cameodb_default_shard"),
            writer_memory_budget: 32 * 1024 * 1024, // 32MB default
            writer_memory_min_mb: 16,               // 16MB minimum
            writer_memory_max_mb: 256,              // 256MB maximum
            default_batch_size: 1000,               // 1000 operations default
            wal_sync: true,
        }
    }
}

impl StorageConfig {
    /// Calculate optimal memory budget based on index size and configurable range
    pub fn get_optimal_memory_budget(&self, index_path: &PathBuf) -> usize {
        let min_budget_bytes = self.writer_memory_min_mb * 1024 * 1024;
        let max_budget_bytes = self.writer_memory_max_mb * 1024 * 1024;
        let default_budget_bytes = self.writer_memory_budget;

        // Check index size and adjust budget dynamically within configurable range
        if let Ok(metadata) = std::fs::metadata(index_path) {
            let size_mb = metadata.len() / (1024 * 1024);
            let optimal_budget = match size_mb {
                0..=50 => min_budget_bytes,       // Very small indices: min budget (16MB)
                51..=200 => default_budget_bytes, // Small indices: default budget (32MB)
                201..=1000 => (min_budget_bytes + max_budget_bytes) / 2, // Medium indices: mid-range (136MB)
                1001..=5000 => (max_budget_bytes * 3) / 4, // Large indices: 75% of max (192MB)
                _ => max_budget_bytes,                     // Very large indices: max budget (256MB)
            };

            // Ensure result is within configured bounds
            optimal_budget.max(min_budget_bytes).min(max_budget_bytes)
        } else {
            // New index, use default budget
            default_budget_bytes
                .max(min_budget_bytes)
                .min(max_budget_bytes)
        }
    }
}

/// Field definition for schema evolution and validation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FieldDef {
    pub name: String,
    pub field_type: String,
    pub indexed: bool,
}

/// Index schema definition for validation and evolution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexSchema {
    pub shard_count: u32,
    pub fields: HashMap<String, FieldDef>,
}

impl Default for IndexSchema {
    fn default() -> Self {
        Self {
            shard_count: 256,
            fields: HashMap::new(),
        }
    }
}

/// Statistics for an index.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexStats {
    pub document_count: u64,
    pub total_size_bytes: u64,
    pub tantivy_index_exists: bool,
}

/// Information about an index including schema and statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexInfo {
    pub name: String,
    pub schema: IndexSchema,
    pub document_count: u64,
    pub total_size_bytes: u64,
    pub tantivy_index_exists: bool,
}

/// Comprehensive error types for storage engine operations.
#[derive(Debug, Error)]
pub enum StoreError {
    #[error("redb error: {0}")]
    Redb(#[from] redb::Error),

    #[error("redb database error: {0}")]
    Database(#[from] redb::DatabaseError),

    #[error("redb transaction error: {0}")]
    Transaction(#[from] redb::TransactionError),

    #[error("redb table error: {0}")]
    Table(#[from] redb::TableError),

    #[error("redb storage error: {0}")]
    Storage(#[from] redb::StorageError),

    #[error("redb commit error: {0}")]
    Commit(#[from] redb::CommitError),

    #[error("tantivy error: {0}")]
    Tantivy(#[from] tantivy::TantivyError),

    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("field not found: {0}")]
    FieldNotFound(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("query parser error: {0}")]
    QueryParser(#[from] QueryParserError),

    #[error("index not found: {0}")]
    IndexNotFound(String),
}

/// Write-Ahead Log operations for atomic dual-write.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalOp {
    Put {
        id: String,
        body: String,
        json_blob: Option<JsonValue>,
    },
    Delete {
        id: String,
    },
}

/// Helper struct for zero-copy serialization of stored documents
#[derive(Serialize)]
struct StoredDoc<'a> {
    body: &'a str,
    json_blob: Option<&'a JsonValue>,
}

/// Owned version for deserialization from redb
#[derive(Deserialize)]
struct StoredDocOwned {
    body: String,
    json_blob: Option<JsonValue>,
}

/// Internal schema field mappings for Tantivy.
#[derive(Debug, Clone)]
struct SchemaFields {
    id: Field,
    body: Field,
    json_blob: Field,
}

/// Helper function to calculate directory size recursively
fn get_directory_size(path: &PathBuf) -> Result<u64, std::io::Error> {
    let mut total_size = 0u64;

    if path.is_dir() {
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let entry_path = entry.path();

            if entry_path.is_dir() {
                total_size += get_directory_size(&entry_path)?;
            } else {
                total_size += entry.metadata()?.len();
            }
        }
    } else {
        total_size = path.metadata()?.len();
    }

    Ok(total_size)
}

/// Multi-tenant hybrid storage engine combining redb and tantivy.
pub struct HybridStore {
    /// Shared redb database across all indices
    kv: Database,
    /// Cache of IndexWriters keyed by index name
    writers: Arc<RwLock<HashMap<String, Arc<Mutex<IndexWriter>>>>>,
    /// Cache of IndexReaders keyed by index name
    readers: Arc<RwLock<HashMap<String, IndexReader>>>,
    /// Atomic counters for WAL sequence IDs per index
    current_seq: Arc<RwLock<HashMap<String, AtomicU64>>>,
    /// Operation counters for smart commits per index
    operations_counter: Arc<RwLock<HashMap<String, AtomicU64>>>,
    /// Simple per-index read cache for frequently accessed documents
    read_cache: Arc<RwLock<HashMap<String, HashMap<String, Vec<u8>>>>>,
    /// Cache of optimal memory budgets per index to avoid frequent syscalls
    budget_cache: Arc<RwLock<HashMap<String, usize>>>,
    /// Cache of schemas per index to avoid repeated redb reads
    schema_cache: Arc<RwLock<HashMap<String, Arc<IndexSchema>>>>,
    /// Storage configuration
    config: StorageConfig,
}

impl HybridStore {
    /// Creates a new multi-tenant HybridStore.
    pub fn new(config: StorageConfig) -> Result<Self, StoreError> {
        // Create directory structure
        fs::create_dir_all(&config.shard_path)?;
        let kv_path = config.shard_path.join("store.redb");
        let indices_path = config.shard_path.join("indices");
        fs::create_dir_all(&indices_path)?;

        // Create shared redb database
        let kv = Database::create(&kv_path)?;

        Ok(HybridStore {
            kv,
            writers: Arc::new(RwLock::new(HashMap::new())),
            readers: Arc::new(RwLock::new(HashMap::new())),
            current_seq: Arc::new(RwLock::new(HashMap::new())),
            operations_counter: Arc::new(RwLock::new(HashMap::new())),
            read_cache: Arc::new(RwLock::new(HashMap::new())),
            budget_cache: Arc::new(RwLock::new(HashMap::new())),
            schema_cache: Arc::new(RwLock::new(HashMap::new())),
            config,
        })
    }

    /// Get a value from the read cache if present.
    fn get_from_cache(&self, index: &str, key: &str) -> Option<Vec<u8>> {
        if let Ok(cache_map) = self.read_cache.read() {
            if let Some(index_cache) = cache_map.get(index) {
                if let Some(value) = index_cache.get(key) {
                    return Some(value.clone());
                }
            }
        }
        None
    }

    /// Insert a value into the read cache with a simple per-index size bound.
    fn insert_into_cache(&self, index: &str, key: &str, value: Vec<u8>) {
        const MAX_CACHE_ENTRIES_PER_INDEX: usize = 1024;

        if let Ok(mut cache_map) = self.read_cache.write() {
            let index_cache = cache_map
                .entry(index.to_string())
                .or_insert_with(HashMap::new);

            if index_cache.len() >= MAX_CACHE_ENTRIES_PER_INDEX {
                if let Some(first_key) = index_cache.keys().next().cloned() {
                    index_cache.remove(&first_key);
                }
            }

            index_cache.insert(key.to_string(), value);
        }
    }

    /// Creates the default Tantivy schema.
    fn create_default_schema() -> (Schema, SchemaFields) {
        let mut schema_builder = Schema::builder();
        let id_field = schema_builder.add_text_field("id", STRING | STORED);
        let body_field = schema_builder.add_text_field("body", TEXT);
        let json_blob_field = schema_builder.add_text_field("json_blob", TEXT | STORED);
        let schema = schema_builder.build();

        let fields = SchemaFields {
            id: id_field,
            body: body_field,
            json_blob: json_blob_field,
        };

        (schema, fields)
    }

    /// Helper method: get_or_create_index
    fn get_or_create_index(
        &self,
        index: &str,
    ) -> Result<(Arc<Mutex<IndexWriter>>, SchemaFields), StoreError> {
        // Check writers cache first
        {
            let readers = self.writers.read().unwrap();
            if let Some(writer) = readers.get(index) {
                let (_, fields) = Self::create_default_schema();
                return Ok((Arc::clone(writer), fields));
            }
        }

        // Create index directory and Tantivy index if it doesn't exist
        let index_path = self.config.shard_path.join("indices").join(index);

        let (schema, fields) = Self::create_default_schema();

        // Create or open tantivy index
        let tantivy_index = if index_path.join("meta.json").exists() {
            Index::open_in_dir(&index_path)?
        } else {
            fs::create_dir_all(&index_path)?;
            Index::create_in_dir(&index_path, schema)?
        };

        // Create writer with dynamic memory budget based on index size
        let optimal_budget = self.config.get_optimal_memory_budget(&index_path);

        // Cache the budget
        if let Ok(mut cache) = self.budget_cache.write() {
            cache.insert(index.to_string(), optimal_budget);
        }

        let writer = tantivy_index.writer(optimal_budget)?;
        let writer_arc = Arc::new(Mutex::new(writer));

        // Store in cache
        {
            let mut writers = self.writers.write().unwrap();
            writers.insert(index.to_string(), Arc::clone(&writer_arc));
        }

        // Initialize sequence counter for this index if needed
        {
            let mut seq_map = self.current_seq.write().unwrap();
            if !seq_map.contains_key(index) {
                let max_seq = self.get_max_wal_id_for_index(index)?;
                seq_map.insert(index.to_string(), AtomicU64::new(max_seq));
            }
        }

        Ok((writer_arc, fields))
    }

    /// Track document count and perform smart commits based on operation thresholds
    fn should_commit_writer(&self, index: &str, operations_since_commit: u64) -> bool {
        // Get dynamic memory budget for this specific index
        // Use cached budget if available to avoid syscalls on every write
        let budget = {
            let cache_hit = if let Ok(cache) = self.budget_cache.read() {
                cache.get(index).cloned()
            } else {
                None
            };

            if let Some(b) = cache_hit {
                b
            } else {
                // Fallback: calculate and cache
                let index_path = self.config.shard_path.join("indices").join(index);
                let b = self.config.get_optimal_memory_budget(&index_path);
                if let Ok(mut cache) = self.budget_cache.write() {
                    cache.insert(index.to_string(), b);
                }
                b
            }
        };

        // Commit strategy based on document count and configurable memory budget range
        // Scale commit frequency with memory budget: more memory = fewer commits
        let min_budget = self.config.writer_memory_min_mb * 1024 * 1024;
        let max_budget = self.config.writer_memory_max_mb * 1024 * 1024;

        // Calculate adaptive threshold based on default_batch_size and memory budget ratio
        let budget_ratio = (budget - min_budget) as f64 / (max_budget - min_budget) as f64;
        let default_batch = self.config.default_batch_size as f64;

        // Scale from 50% of default (min memory) to 800% of default (max memory)
        // e.g., default=1000: 500 ops (16MB) -> 8000 ops (256MB)
        let base_ops = (default_batch * (0.5 + budget_ratio * 7.5)) as u64;

        operations_since_commit >= base_ops
    }

    /// Get operation count for an index since last commit
    fn get_operations_count(&self, index: &str) -> u64 {
        if let Ok(counter_map) = self.operations_counter.read()
            && let Some(counter) = counter_map.get(index)
        {
            return counter.load(Ordering::SeqCst);
        }
        0
    }

    /// Increment operation count and return new count
    fn increment_operations(&self, index: &str) -> u64 {
        // Ensure counter exists for this index
        {
            let mut counter_map = self.operations_counter.write().unwrap();
            if !counter_map.contains_key(index) {
                counter_map.insert(index.to_string(), AtomicU64::new(0));
            }
        }

        // Increment and return new count
        if let Ok(counter_map) = self.operations_counter.read()
            && let Some(counter) = counter_map.get(index)
        {
            return counter.fetch_add(1, Ordering::SeqCst) + 1;
        }
        0
    }

    /// Reset operation counter after commit
    fn reset_operations_counter(&self, index: &str) {
        if let Ok(counter_map) = self.operations_counter.read()
            && let Some(counter) = counter_map.get(index)
        {
            counter.store(0, Ordering::SeqCst);
        }
    }

    /// Perform smart commit based on operation count
    fn maybe_commit_writer(&self, index: &str) -> Result<bool, StoreError> {
        let ops_count = self.get_operations_count(index);

        if self.should_commit_writer(index, ops_count)
            && let Ok(writers) = self.writers.read()
            && let Some(writer_arc) = writers.get(index)
        {
            let mut writer = writer_arc.lock().unwrap();
            writer.commit()?;
            self.reset_operations_counter(index);

            // Refresh budget cache after commit since index size likely changed
            let index_path = self.config.shard_path.join("indices").join(index);
            let new_budget = self.config.get_optimal_memory_budget(&index_path);
            if let Ok(mut cache) = self.budget_cache.write() {
                cache.insert(index.to_string(), new_budget);
            }

            return Ok(true); // Commit performed
        }
        Ok(false) // No commit needed
    }

    /// Multi-tenant apply_write method
    pub fn apply_write(&self, index: &str, op: WalOp) -> Result<u64, StoreError> {
        // Get or create the index
        let (writer_arc, fields) = self.get_or_create_index(index)?;

        // Get sequence ID for this index
        let seq_id = {
            let seq_map = self.current_seq.read().unwrap();
            let counter = seq_map.get(index).ok_or_else(|| {
                StoreError::IndexNotFound(format!(
                    "Sequence counter not found for index: {}",
                    index
                ))
            })?;
            counter.fetch_add(1, Ordering::SeqCst) + 1
        };

        // Create dynamic table definitions
        let data_table_name = format!("data_{}", index);
        let wal_table_name = format!("wal_{}", index);
        let data_table_def = TableDefinition::<&str, &[u8]>::new(&data_table_name);
        let wal_table_def = TableDefinition::<u64, &[u8]>::new(&wal_table_name);

        // Write to WAL first
        let wal_data =
            serde_json::to_vec(&op).map_err(|e| StoreError::Serialization(e.to_string()))?;

        let write_txn = self.kv.begin_write()?;
        {
            let mut wal_table = write_txn.open_table(wal_table_def)?;
            wal_table.insert(seq_id, wal_data.as_slice())?;

            // Apply to data table
            match op {
                WalOp::Put {
                    id,
                    body,
                    json_blob,
                } => {
                    // Step 1: Get cached schema for field filtering
                    let schema = self
                        .get_schema_cached(index)?
                        .unwrap_or_else(|| Arc::new(IndexSchema::default()));

                    // Step 2: Serialize complete document for redb (all fields)
                    let doc_data = StoredDoc {
                        body: &body,
                        json_blob: json_blob.as_ref(),
                    };
                    let doc_bytes = serde_json::to_vec(&doc_data)
                        .map_err(|e| StoreError::Serialization(e.to_string()))?;

                    let mut data_table = write_txn.open_table(data_table_def)?;
                    data_table.insert(id.as_str(), doc_bytes.as_slice())?;

                    // Step 3: Build tantivy document with ONLY indexed fields
                    let mut tantivy_doc = doc!(fields.id => id.as_str());

                    // Check if 'body' field should be indexed
                    let body_indexed = schema.fields.get("body").map_or(true, |f| f.indexed);

                    if body_indexed {
                        tantivy_doc.add_text(fields.body, &body);
                    }

                    // Step 4: Process json_blob fields selectively
                    if let Some(json_obj) = json_blob.as_ref().and_then(|v| v.as_object()) {
                        for (field_name, field_value) in json_obj {
                            // Check if field should be indexed
                            let should_index =
                                schema.fields.get(field_name).map_or(false, |f| f.indexed);

                            if should_index {
                                // Only serialize and index if marked as indexed
                                let field_str = serde_json::to_string(field_value)
                                    .map_err(|e| StoreError::Serialization(e.to_string()))?;
                                tantivy_doc.add_text(fields.json_blob, &field_str);
                            }
                        }
                    }

                    let writer = writer_arc.lock().unwrap();
                    writer.add_document(tantivy_doc)?;
                }
                WalOp::Delete { id } => {
                    let mut data_table = write_txn.open_table(data_table_def)?;
                    data_table.remove(id.as_str())?;

                    // Delete from tantivy index
                    let term = tantivy::Term::from_field_text(fields.id, &id);
                    let writer = writer_arc.lock().unwrap();
                    writer.delete_term(term);
                }
            }
        }

        write_txn.commit()?;

        // Increment operation counter and perform smart commit if needed
        self.increment_operations(index);
        self.maybe_commit_writer(index)?;

        Ok(seq_id)
    }

    /// Delete all data for an index using redb's efficient delete_table() function
    pub fn delete_index_data(&self, index: &str) -> Result<(), StoreError> {
        // Remove from caches first
        {
            let mut writers = self.writers.write().unwrap();
            writers.remove(index);
        }
        {
            let mut readers = self.readers.write().unwrap();
            readers.remove(index);
        }
        {
            let mut seq_map = self.current_seq.write().unwrap();
            seq_map.remove(index);
        }

        // Delete redb tables completely using delete_table() for efficiency
        let write_txn = self.kv.begin_write()?;
        {
            let data_table_name = format!("data_{}", index);
            let wal_table_name = format!("wal_{}", index);
            let data_table_def = TableDefinition::<&str, &[u8]>::new(&data_table_name);
            let wal_table_def = TableDefinition::<u64, &[u8]>::new(&wal_table_name);

            // Delete tables using redb's delete_table function (more efficient than manual clearing)
            // Note: delete_table returns bool indicating if table existed, we ignore the result
            let _ = write_txn.delete_table(data_table_def)?;
            let _ = write_txn.delete_table(wal_table_def)?;
        }
        write_txn.commit()?;

        // Remove tantivy directory
        let index_path = self.config.shard_path.join("indices").join(index);
        if index_path.exists() {
            fs::remove_dir_all(index_path)?;
        }

        Ok(())
    }

    /// Get document by key from specific index
    pub fn get_by_key(&self, index: &str, key: &str) -> Result<Option<Vec<u8>>, StoreError> {
        if let Some(cached) = self.get_from_cache(index, key) {
            return Ok(Some(cached));
        }

        let data_table_name = format!("data_{}", index);
        let data_table_def = TableDefinition::<&str, &[u8]>::new(&data_table_name);

        let read_txn = self.kv.begin_read()?;

        match read_txn.open_table(data_table_def) {
            Ok(data_table) => match data_table.get(key)? {
                Some(value) => {
                    let bytes = value.value().to_vec();
                    self.insert_into_cache(index, key, bytes.clone());
                    Ok(Some(bytes))
                }
                None => Ok(None),
            },
            Err(_) => Ok(None), // Table doesn't exist (index was deleted)
        }
    }

    /// Batch retrieve documents by keys from specific index
    /// More efficient than multiple get_by_key calls - uses single transaction
    pub fn get_batch_by_keys(
        &self,
        index: &str,
        keys: &[String],
    ) -> Result<Vec<(String, Vec<u8>)>, StoreError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let data_table_name = format!("data_{}", index);
        let data_table_def = TableDefinition::<&str, &[u8]>::new(&data_table_name);

        // Single read transaction for all keys
        let read_txn = self.kv.begin_read()?;
        let data_table = match read_txn.open_table(data_table_def) {
            Ok(table) => table,
            Err(_) => return Ok(Vec::new()), // Table doesn't exist
        };

        let mut results = Vec::with_capacity(keys.len());

        for key in keys {
            // Check cache first
            if let Some(cached) = self.get_from_cache(index, key) {
                results.push((key.clone(), cached));
                continue;
            }

            // Fetch from redb
            if let Some(value) = data_table.get(key.as_str())? {
                let bytes = value.value().to_vec();
                self.insert_into_cache(index, key, bytes.clone());
                results.push((key.clone(), bytes));
            }
            // Skip keys that don't exist (document may have been deleted)
        }

        Ok(results)
    }

    /// Store schema for an index
    pub fn store_schema(&self, index_name: &str, schema: &IndexSchema) -> Result<(), StoreError> {
        let schema_bytes =
            serde_json::to_vec(schema).map_err(|e| StoreError::Serialization(e.to_string()))?;

        let write_txn = self.kv.begin_write()?;
        {
            let mut schema_table = write_txn.open_table(TABLE_SCHEMA)?;
            schema_table.insert(index_name, schema_bytes.as_slice())?;
        }
        write_txn.commit()?;

        Ok(())
    }

    /// Get schema for an index
    pub fn get_schema(&self, index_name: &str) -> Result<Option<IndexSchema>, StoreError> {
        let read_txn = self.kv.begin_read()?;

        match read_txn.open_table(TABLE_SCHEMA) {
            Ok(schema_table) => match schema_table.get(index_name)? {
                Some(value) => {
                    let schema: IndexSchema = serde_json::from_slice(value.value())
                        .map_err(|e| StoreError::Serialization(e.to_string()))?;
                    Ok(Some(schema))
                }
                None => Ok(None),
            },
            Err(_) => Ok(None), // Table doesn't exist yet
        }
    }

    /// Get schema from cache, or load from redb and cache it
    pub fn get_schema_cached(&self, index: &str) -> Result<Option<Arc<IndexSchema>>, StoreError> {
        // Fast path: check cache
        if let Ok(cache) = self.schema_cache.read() {
            if let Some(schema) = cache.get(index) {
                return Ok(Some(Arc::clone(schema)));
            }
        }

        // Slow path: load from redb
        if let Some(schema) = self.get_schema(index)? {
            let schema_arc = Arc::new(schema);

            // Update cache
            if let Ok(mut cache) = self.schema_cache.write() {
                cache.insert(index.to_string(), Arc::clone(&schema_arc));
            }

            Ok(Some(schema_arc))
        } else {
            Ok(None)
        }
    }

    /// Invalidate cache entry when schema is updated
    pub fn invalidate_schema_cache(&self, index: &str) {
        if let Ok(mut cache) = self.schema_cache.write() {
            cache.remove(index);
        }
    }

    /// Update both redb and cache atomically
    pub fn store_schema_and_cache(
        &self,
        index: &str,
        schema: &IndexSchema,
    ) -> Result<(), StoreError> {
        // Persist to redb first
        self.store_schema(index, schema)?;

        // Update cache
        let schema_arc = Arc::new(schema.clone());
        if let Ok(mut cache) = self.schema_cache.write() {
            cache.insert(index.to_string(), schema_arc);
        }

        Ok(())
    }

    /// Get max WAL ID for a specific index
    fn get_max_wal_id_for_index(&self, index: &str) -> Result<u64, StoreError> {
        let wal_table_name = format!("wal_{}", index);
        let wal_table_def = TableDefinition::<u64, &[u8]>::new(&wal_table_name);

        let read_txn = self.kv.begin_read()?;

        match read_txn.open_table(wal_table_def) {
            Ok(wal_table) => {
                let mut max_id = 0u64;
                for result in wal_table.iter()? {
                    let (key, _) = result?;
                    let id = key.value();
                    if id > max_id {
                        max_id = id;
                    }
                }
                Ok(max_id)
            }
            Err(_) => Ok(0), // Table doesn't exist yet
        }
    }

    /// Get or create a cached IndexReader for the given index
    fn get_reader(&self, index: &str) -> Result<Option<IndexReader>, StoreError> {
        // Check cache first
        {
            let readers = self.readers.read().unwrap();
            if let Some(reader) = readers.get(index) {
                reader.reload()?;
                return Ok(Some(reader.clone()));
            }
        }

        // Check if index directory exists
        let index_path = self.config.shard_path.join("indices").join(index);
        if !index_path.exists() || !index_path.join("meta.json").exists() {
            return Ok(None);
        }

        // Open index and create reader
        let tantivy_index = Index::open_in_dir(&index_path)?;
        let reader = tantivy_index.reader()?;

        // Cache the reader
        {
            let mut readers = self.readers.write().unwrap();
            readers.insert(index.to_string(), reader.clone());
        }

        Ok(Some(reader))
    }

    /// Search documents in a specific index
    /// Uses tantivy for search, then batch-retrieves complete documents from redb
    pub fn search_documents(
        &self,
        index: &str,
        query: &str,
        limit: usize,
    ) -> Result<Vec<(f32, JsonValue)>, StoreError> {
        // Get schema fields (lightweight, no IO)
        let (_, fields) = Self::create_default_schema();

        // Get reader from cache or disk
        let reader = match self.get_reader(index)? {
            Some(r) => r,
            None => return Ok(Vec::new()),
        };

        let searcher = reader.searcher();
        let tantivy_index = searcher.index();

        // Get cached schema to determine which fields are indexed
        let schema = self
            .get_schema_cached(index)?
            .unwrap_or_else(|| Arc::new(IndexSchema::default()));

        // Build query parser with only indexed fields
        let mut query_fields = vec![];

        // Check if body field is indexed
        if schema.fields.get("body").map_or(true, |f| f.indexed) {
            query_fields.push(fields.body);
        }

        // Check if any json_blob fields are indexed
        let has_indexed_json = schema
            .fields
            .values()
            .any(|f| f.name != "body" && f.indexed);
        if has_indexed_json {
            query_fields.push(fields.json_blob);
        }

        if query_fields.is_empty() {
            // No indexed fields - cannot search
            return Ok(Vec::new());
        }

        // Create query parser and execute search
        let query_parser = tantivy::query::QueryParser::for_index(tantivy_index, query_fields);
        let parsed_query = query_parser.parse_query(query)?;

        // Execute search on tantivy index
        let top_docs = searcher.search(
            &parsed_query,
            &tantivy::collector::TopDocs::with_limit(limit),
        )?;

        if top_docs.is_empty() {
            return Ok(Vec::new());
        }

        // Step 1: Extract document IDs from tantivy results
        let mut doc_ids_with_scores = Vec::with_capacity(top_docs.len());
        for (score, doc_address) in top_docs {
            let doc: tantivy::TantivyDocument = searcher.doc(doc_address)?;

            // Extract ID field - use to_json to get the document and parse ID
            let json_str = doc.to_json(&tantivy_index.schema());
            if let Ok(json_val) = serde_json::from_str::<JsonValue>(&json_str) {
                if let Some(id_str) = json_val.get("id").and_then(|v| v.as_str()) {
                    doc_ids_with_scores.push((score, id_str.to_string()));
                }
            }
        }

        // Step 2: Batch retrieve complete documents from redb (single transaction)
        let doc_ids: Vec<String> = doc_ids_with_scores
            .iter()
            .map(|(_, id)| id.clone())
            .collect();

        let redb_docs = self.get_batch_by_keys(index, &doc_ids)?;

        // Create lookup map for O(1) access
        let doc_map: std::collections::HashMap<String, Vec<u8>> = redb_docs.into_iter().collect();

        // Step 3: Combine scores with complete documents
        let mut results = Vec::new();
        for (score, doc_id) in doc_ids_with_scores {
            if let Some(doc_bytes) = doc_map.get(&doc_id) {
                // Deserialize complete document from redb
                let stored_doc: StoredDocOwned = serde_json::from_slice(doc_bytes)
                    .map_err(|e| StoreError::Serialization(e.to_string()))?;

                // Build complete JSON document with all fields
                let mut complete_doc = serde_json::json!({
                    "id": doc_id,
                    "body": stored_doc.body,
                });

                // Merge json_blob fields into root document
                if let Some(json_blob) = stored_doc.json_blob {
                    if let Some(obj) = complete_doc.as_object_mut() {
                        if let Some(blob_obj) = json_blob.as_object() {
                            for (k, v) in blob_obj {
                                obj.insert(k.clone(), v.clone());
                            }
                        }
                    }
                }

                results.push((score, complete_doc));
            }
        }

        Ok(results)
    }

    /// Apply multiple write operations atomically to a specific index
    pub fn apply_batch(&self, index: &str, ops: Vec<WalOp>) -> Result<Vec<u64>, StoreError> {
        if ops.is_empty() {
            return Ok(Vec::new());
        }

        // Get or create the index
        let (writer_arc, fields) = self.get_or_create_index(index)?;

        // Generate sequence IDs for all operations
        let mut seq_ids = Vec::with_capacity(ops.len());
        {
            let seq_map = self.current_seq.read().unwrap();
            let counter = seq_map.get(index).ok_or_else(|| {
                StoreError::IndexNotFound(format!(
                    "Sequence counter not found for index: {}",
                    index
                ))
            })?;

            for _ in 0..ops.len() {
                seq_ids.push(counter.fetch_add(1, Ordering::SeqCst) + 1);
            }
        }

        // Create dynamic table definitions
        let data_table_name = format!("data_{}", index);
        let wal_table_name = format!("wal_{}", index);
        let data_table_def = TableDefinition::<&str, &[u8]>::new(&data_table_name);
        let wal_table_def = TableDefinition::<u64, &[u8]>::new(&wal_table_name);

        // Single transaction for all operations
        let write_txn = self.kv.begin_write()?;
        let mut tantivy_ops = Vec::new();
        let batch_size = ops.len() as u64;

        {
            let mut wal_table = write_txn.open_table(wal_table_def)?;
            let mut data_table = write_txn.open_table(data_table_def)?;

            for (op, seq_id) in ops.into_iter().zip(seq_ids.iter()) {
                // Write to WAL
                let wal_data = serde_json::to_vec(&op)
                    .map_err(|e| StoreError::Serialization(e.to_string()))?;
                wal_table.insert(*seq_id, wal_data.as_slice())?;

                // Apply to data table and prepare tantivy operations
                match op {
                    WalOp::Put {
                        id,
                        body,
                        json_blob,
                    } => {
                        // Step 1: Get cached schema for field filtering
                        let schema = self
                            .get_schema_cached(index)?
                            .unwrap_or_else(|| Arc::new(IndexSchema::default()));

                        // Step 2: Serialize complete document for redb (all fields)
                        let doc_data = StoredDoc {
                            body: &body,
                            json_blob: json_blob.as_ref(),
                        };
                        let doc_bytes = serde_json::to_vec(&doc_data)
                            .map_err(|e| StoreError::Serialization(e.to_string()))?;

                        data_table.insert(id.as_str(), doc_bytes.as_slice())?;

                        // Step 3: Build tantivy document with ONLY indexed fields
                        let mut tantivy_doc = doc!(fields.id => id.as_str());

                        // Check if 'body' field should be indexed
                        let body_indexed = schema.fields.get("body").map_or(true, |f| f.indexed);

                        if body_indexed {
                            tantivy_doc.add_text(fields.body, &body);
                        }

                        // Step 4: Process json_blob fields selectively
                        if let Some(json_obj) = json_blob.as_ref().and_then(|v| v.as_object()) {
                            for (field_name, field_value) in json_obj {
                                // Check if field should be indexed
                                let should_index =
                                    schema.fields.get(field_name).map_or(false, |f| f.indexed);

                                if should_index {
                                    // Only serialize and index if marked as indexed
                                    let field_str = serde_json::to_string(field_value)
                                        .map_err(|e| StoreError::Serialization(e.to_string()))?;
                                    tantivy_doc.add_text(fields.json_blob, &field_str);
                                }
                            }
                        }

                        tantivy_ops.push(("add", tantivy_doc, id));
                    }
                    WalOp::Delete { id } => {
                        data_table.remove(id.as_str())?;
                        tantivy_ops.push(("delete", doc!(), id));
                    }
                }
            }
        }

        write_txn.commit()?;

        // Check for pre-emptive commit before batch processing
        let initial_ops_count = self.get_operations_count(index);
        let should_pre_commit = self.should_commit_writer(index, initial_ops_count);

        if should_pre_commit {
            // Pre-emptive commit to free up memory before large batch
            let mut writer = writer_arc.lock().unwrap();
            writer.commit()?;
            self.reset_operations_counter(index);
        }

        // Apply all tantivy operations
        {
            let mut writer = writer_arc.lock().unwrap();
            for (op_type, tantivy_doc, id) in tantivy_ops {
                match op_type {
                    "add" => {
                        writer.add_document(tantivy_doc)?;
                    }
                    "delete" => {
                        let term = tantivy::Term::from_field_text(fields.id, &id);
                        writer.delete_term(term);
                    }
                    _ => unreachable!(),
                }
            }

            // Increment operations counter by batch size
            for _ in 0..batch_size {
                self.increment_operations(index);
            }

            // Perform smart commit if needed, or force commit for very large batches
            if batch_size >= 1000 {
                // Force commit for very large batches
                writer.commit()?;
                self.reset_operations_counter(index);
            }
        }

        // Use smart commit logic for normal batches (when writer lock is released)
        if batch_size < 1000 {
            self.maybe_commit_writer(index)?;
        }

        Ok(seq_ids)
    }

    /// List all available indexes with their statistics
    pub fn list_indexes(&self) -> Result<Vec<IndexInfo>, StoreError> {
        let mut indexes = Vec::new();

        // Get all schemas from the schema table
        let read_txn = self.kv.begin_read()?;

        match read_txn.open_table(TABLE_SCHEMA) {
            Ok(schema_table) => {
                for result in schema_table.iter()? {
                    let (index_name, schema_bytes) = result?;
                    let index_name = index_name.value().to_string();

                    // Parse schema
                    let schema: IndexSchema = serde_json::from_slice(schema_bytes.value())
                        .map_err(|e| StoreError::Serialization(e.to_string()))?;

                    // Get statistics for this index
                    let stats = self.get_index_statistics(&index_name)?;

                    indexes.push(IndexInfo {
                        name: index_name,
                        schema,
                        document_count: stats.document_count,
                        total_size_bytes: stats.total_size_bytes,
                        tantivy_index_exists: stats.tantivy_index_exists,
                    });
                }
            }
            Err(_) => {
                // Schema table doesn't exist yet, check for any existing Tantivy indices
                let indices_dir = self.config.shard_path.join("indices");
                if indices_dir.exists() {
                    for entry in fs::read_dir(&indices_dir)? {
                        let entry = entry?;
                        if entry.file_type()?.is_dir() {
                            let index_name = entry.file_name().to_string_lossy().to_string();
                            let stats = self.get_index_statistics(&index_name)?;

                            // Create default schema for legacy indices
                            let default_schema = IndexSchema {
                                shard_count: 256,
                                fields: HashMap::new(),
                            };

                            indexes.push(IndexInfo {
                                name: index_name,
                                schema: default_schema,
                                document_count: stats.document_count,
                                total_size_bytes: stats.total_size_bytes,
                                tantivy_index_exists: stats.tantivy_index_exists,
                            });
                        }
                    }
                }
            }
        }

        Ok(indexes)
    }

    /// Get statistics for a specific index
    pub fn get_index_statistics(&self, index: &str) -> Result<IndexStats, StoreError> {
        let data_table_name = format!("data_{}", index);
        let data_table_def = TableDefinition::<&str, &[u8]>::new(&data_table_name);

        let read_txn = self.kv.begin_read()?;

        let mut document_count = 0u64;
        let mut total_size_bytes = 0u64;

        // Count documents and calculate size from redb data table
        match read_txn.open_table(data_table_def) {
            Ok(data_table) => {
                for result in data_table.iter()? {
                    let (_, value) = result?;
                    document_count += 1;
                    total_size_bytes += value.value().len() as u64;
                }
            }
            Err(_) => {
                // Table doesn't exist, keep counts at 0
            }
        }

        // Check if Tantivy index exists
        let index_path = self.config.shard_path.join("indices").join(index);
        let tantivy_index_exists = index_path.exists() && index_path.is_dir();

        // Add Tantivy index size if it exists
        if tantivy_index_exists && let Ok(tantivy_size) = get_directory_size(&index_path) {
            total_size_bytes += tantivy_size;
        }

        Ok(IndexStats {
            document_count,
            total_size_bytes,
            tantivy_index_exists,
        })
    }

    /// Get list of index names from schema table and filesystem
    pub fn get_index_names(&self) -> Result<Vec<String>, StoreError> {
        let mut index_names = std::collections::HashSet::new();

        let read_txn = self.kv.begin_read()?;

        // Get index names from schema table
        match read_txn.open_table(TABLE_SCHEMA) {
            Ok(schema_table) => {
                for result in schema_table.iter()? {
                    let (index_name, _) = result?;
                    index_names.insert(index_name.value().to_string());
                }
            }
            Err(_) => {
                // Schema table doesn't exist yet
            }
        }

        // Also check for indices in filesystem (legacy support)
        let indices_dir = self.config.shard_path.join("indices");
        if indices_dir.exists() {
            for entry in fs::read_dir(&indices_dir)? {
                let entry = entry?;
                if entry.file_type()?.is_dir() {
                    let index_name = entry.file_name().to_string_lossy().to_string();
                    index_names.insert(index_name);
                }
            }
        }

        Ok(index_names.into_iter().collect())
    }

    /// Get field names from actual documents in an index by sampling
    pub fn get_index_field_names(&self, index: &str) -> Result<Vec<String>, StoreError> {
        let data_table_name = format!("data_{}", index);
        let data_table_def = TableDefinition::<&str, &[u8]>::new(&data_table_name);

        let read_txn = self.kv.begin_read()?;
        let mut field_names = std::collections::HashSet::new();

        match read_txn.open_table(data_table_def) {
            Ok(data_table) => {
                const MAX_SAMPLES: usize = 100; // Sample up to 100 documents

                for (sample_count, result) in data_table.iter()?.enumerate() {
                    if sample_count >= MAX_SAMPLES {
                        break;
                    }

                    let (_, value) = result?;

                    // Parse the document JSON to extract field names
                    if let Ok(doc_data) = serde_json::from_slice::<JsonValue>(value.value()) {
                        if let Some(json_blob) = doc_data.get("json_blob")
                            && let Some(json_obj) = json_blob.as_object()
                        {
                            for field_name in json_obj.keys() {
                                field_names.insert(field_name.clone());
                            }
                        }

                        // Also check top-level fields in the document
                        if let Some(doc_obj) = doc_data.as_object() {
                            for field_name in doc_obj.keys() {
                                if field_name != "body" && field_name != "json_blob" {
                                    field_names.insert(field_name.clone());
                                }
                            }
                        }
                    }
                }
            }
            Err(_) => {
                // Table doesn't exist, return empty list
            }
        }

        let mut field_names_vec: Vec<String> = field_names.into_iter().collect();

        // Sort fields with "id" first, then alphabetically
        field_names_vec.sort_by(|a, b| {
            match (a.as_str(), b.as_str()) {
                ("id", "id") => std::cmp::Ordering::Equal,
                ("id", _) => std::cmp::Ordering::Less, // "id" comes first
                (_, "id") => std::cmp::Ordering::Greater, // "id" comes first
                (a, b) => a.cmp(b),                    // alphabetical for others
            }
        });

        Ok(field_names_vec)
    }
}

// Safe because all components are Send+Sync
unsafe impl Send for HybridStore {}
unsafe impl Sync for HybridStore {}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_multi_tenant_storage() {
        let temp_dir = TempDir::new().unwrap();
        let config = StorageConfig {
            shard_path: temp_dir.path().to_path_buf(),
            writer_memory_budget: 32 * 1024 * 1024,
            writer_memory_min_mb: 16,
            writer_memory_max_mb: 256,
            default_batch_size: 1000,
            wal_sync: true,
        };

        let store = HybridStore::new(config).unwrap();

        // Write to index1
        let op1 = WalOp::Put {
            id: "doc1".to_string(),
            body: "content for index1".to_string(),
            json_blob: None,
        };
        let seq1 = store.apply_write("index1", op1).unwrap();
        assert_eq!(seq1, 1);

        // Write to index2
        let op2 = WalOp::Put {
            id: "doc1".to_string(),
            body: "content for index2".to_string(),
            json_blob: None,
        };
        let seq2 = store.apply_write("index2", op2).unwrap();
        assert_eq!(seq2, 1); // Independent sequence

        // Verify directories exist
        let index1_path = temp_dir.path().join("indices").join("index1");
        let index2_path = temp_dir.path().join("indices").join("index2");
        assert!(index1_path.exists());
        assert!(index2_path.exists());

        // Delete index1
        store.delete_index_data("index1").unwrap();

        // Verify index1 is gone but index2 remains
        assert!(!index1_path.exists());
        assert!(index2_path.exists());

        // Verify index2 still works
        let data = store.get_by_key("index2", "doc1").unwrap();
        assert!(data.is_some());
    }
}
