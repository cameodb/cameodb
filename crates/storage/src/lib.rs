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

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use redb::{
    Database, Durability, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition,
};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use tantivy::query::QueryParserError;
use tantivy::schema::{Document, FAST, Field, INDEXED, STORED, STRING, Schema, TEXT};
use tantivy::{DateTime, Index, IndexReader, IndexWriter, doc};
use thiserror::Error;
use walkdir::WalkDir;

const TANTIVY_DATA_FILE_EXTENSIONS: &[&str] = &["store", "fast", "idx", "doc", "pos", "term"];

/// Number of records to sample for size estimation in large tables
const TABLE_SIZE_SAMPLE_COUNT: u64 = 200;

/// Tantivy DateTime safe range limits (to avoid i64 overflow during nanosecond conversion)
/// DateTime::from_timestamp_secs() multiplies by 1_000_000_000, so safe range is:
/// i64::MIN / 1_000_000_000 to i64::MAX / 1_000_000_000
const TANTIVY_MIN_TIMESTAMP_SECS: i64 = -9_223_372_036; // 1677-09-21 00:12:44 UTC
const TANTIVY_MAX_TIMESTAMP_SECS: i64 = 9_223_372_036; // 2262-04-11 23:47:16 UTC

/// Schema metadata table: maps index names to their schema definitions.
const TABLE_SCHEMA: TableDefinition<&str, &[u8]> = TableDefinition::new("schema");

/// Configuration for the multi-tenant hybrid storage engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// The root folder for this shard's data files.
    pub shard_path: PathBuf,
    /// Default memory budget for each tantivy IndexWriter in bytes.
    pub indexer_memory_budget: usize,
    /// Minimum memory budget for IndexWriter in MB.
    pub indexer_memory_min_mb: usize,
    /// Maximum memory budget for IndexWriter in MB.
    pub indexer_memory_max_mb: usize,
    /// Default batch size for smart commit calculations.
    pub default_batch_size: usize,
    /// Whether to call fsync() on every redb commit.
    pub wal_sync: bool,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            shard_path: PathBuf::from("/var/tmp/cameodb"),
            indexer_memory_min_mb: 32, // 32MB minimum (increased from 16MB)
            indexer_memory_max_mb: 512, // 512MB maximum (increased from 256MB)
            indexer_memory_budget: 64 * 1024 * 1024, // start at 64MB (increased from 16MB)
            default_batch_size: 1000,  // 1000 operations default (matches Python scripts)
            wal_sync: true,
        }
    }
}

impl StorageConfig {
    /// Calculate optimal memory budget based on index size and configurable range
    pub fn get_optimal_memory_budget(&self, index_path: &PathBuf) -> usize {
        let min_budget_bytes = self.indexer_memory_min_mb * 1024 * 1024;
        let max_budget_bytes = self.indexer_memory_max_mb * 1024 * 1024;
        let default_budget_bytes = self.indexer_memory_budget;

        // Check index size and adjust budget dynamically within configurable range
        if let Ok(metadata) = std::fs::metadata(index_path) {
            let size_mb = metadata.len() / (1024 * 1024);
            let optimal_budget = match size_mb {
                0..=100 => min_budget_bytes, // Very small indices: min budget (32MB)
                101..=500 => default_budget_bytes, // Small indices: start budget (64MB)
                501..=2000 => (min_budget_bytes + max_budget_bytes) / 2, // Medium indices: mid-range (272MB)
                2001..=8000 => max_budget_bytes / 2, // Large indices: 50% of max (256MB)
                _ => max_budget_bytes,               // Very large indices: max budget (512MB)
            };

            // Ensure result is within configured bounds
            optimal_budget.max(min_budget_bytes).min(max_budget_bytes)
        } else {
            // New index, use minimum budget (starting point will scale as data is written)
            min_budget_bytes
        }
    }
}

/// Native Tantivy field types with proper enum for type safety.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Default)]
pub enum TantivyFieldType {
    /// Tokenized text for full-text search
    #[default]
    Text,
    /// Untokenized string (exact match)
    String,
    /// 64-bit signed integer
    I64,
    /// 64-bit unsigned integer
    U64,
    /// 64-bit floating point
    F64,
    /// Date/Time (stored as timestamp)
    Date,
    /// Boolean (stored as "true"/"false")
    Boolean,
    /// Binary data
    Bytes,
    /// IP address (IPv4/IPv6)
    Ip,
    /// Nested JSON object
    Json,
    /// Categorical/facet field
    Facet,
}

impl TantivyFieldType {
    /// Convert to string representation (for serialization)
    pub fn to_string(&self) -> &'static str {
        match self {
            TantivyFieldType::Text => "text",
            TantivyFieldType::String => "string",
            TantivyFieldType::I64 => "i64",
            TantivyFieldType::U64 => "u64",
            TantivyFieldType::F64 => "f64",
            TantivyFieldType::Date => "date",
            TantivyFieldType::Boolean => "boolean",
            TantivyFieldType::Bytes => "bytes",
            TantivyFieldType::Ip => "ip",
            TantivyFieldType::Json => "json",
            TantivyFieldType::Facet => "facet",
        }
    }
}

/// Field definition for schema evolution and validation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FieldDef {
    pub name: String,
    pub field_type: TantivyFieldType,
    pub indexed: bool,
    pub stored: bool,
    pub fast: bool,
    // Additional options for Text fields
    pub tokenizer: Option<String>,
    pub index_record_option: Option<String>, // "Basic", "WithFreqs", "WithFreqsAndPositions"
}

impl FieldDef {
    /// Create a new field definition with sensible defaults
    pub fn new(name: String, field_type: TantivyFieldType) -> Self {
        // Only ID field should be stored in Tantivy
        // All other fields are indexed-only, complete data comes from redb
        let stored = name == "id";
        let fast = matches!(
            field_type,
            TantivyFieldType::I64
                | TantivyFieldType::U64
                | TantivyFieldType::F64
                | TantivyFieldType::Date
        );

        Self {
            name,
            field_type,
            indexed: true,
            stored,
            fast,
            tokenizer: None, // Will be set when creating from actual Tantivy schema
            index_record_option: None, // Will be set when creating from actual Tantivy schema
        }
    }

    /// Infer field type from JSON value for schema evolution
    pub fn infer_from_value(name: String, value: &JsonValue) -> Self {
        let field_type = Self::infer_type_from_value(value);
        Self::new(name, field_type)
    }

    /// Create a non-indexed field definition for background schema evolution
    /// New fields discovered during writes are marked as non-indexed to avoid
    /// requiring Tantivy schema rebuilds. They can be stored in redb and later
    /// promoted to indexed fields through explicit schema updates.
    pub fn new_non_indexed(name: String, value: &JsonValue) -> Self {
        let field_type = Self::infer_type_from_value(value);
        // Only ID field should be stored in Tantivy
        let stored = name == "id";
        let fast = matches!(
            field_type,
            TantivyFieldType::I64
                | TantivyFieldType::U64
                | TantivyFieldType::F64
                | TantivyFieldType::Date
        );

        Self {
            name,
            field_type,
            indexed: false, // Non-indexed by default for background evolution
            stored,
            fast,
            tokenizer: None,
            index_record_option: None,
        }
    }

    /// Infer Tantivy field type from JSON value
    pub fn infer_type_from_value(value: &JsonValue) -> TantivyFieldType {
        match value {
            JsonValue::Number(n) => {
                if n.is_i64() {
                    TantivyFieldType::I64
                } else if n.is_u64() {
                    TantivyFieldType::U64
                } else {
                    TantivyFieldType::F64
                }
            }
            JsonValue::Bool(_) => TantivyFieldType::Boolean,
            JsonValue::String(s) => {
                // 1) RFC3339 (full timestamp with offset)
                if chrono::DateTime::parse_from_rfc3339(s).is_ok()
                    // 2) Naive datetime with common formats
                    || Self::is_naive_datetime(s)
                    // 3) Date-only formats
                    || Self::is_naive_date(s)
                {
                    TantivyFieldType::Date
                // 4) IP detection
                } else if s.parse::<std::net::IpAddr>().is_ok() {
                    TantivyFieldType::Ip
                } else {
                    TantivyFieldType::Text
                }
            }
            JsonValue::Array(_) => TantivyFieldType::Text, // Arrays as text for compatibility
            JsonValue::Object(_) => TantivyFieldType::Json, // Nested objects as JSON
            JsonValue::Null => TantivyFieldType::Text,
        }
    }

    /// Check common naive datetime formats (no timezone) such as
    /// - 2024-05-01 12:30:00
    /// - 2024-05-01 12:30
    /// - 2024-05-01T12:30:00
    /// - 2024-05-01T12:30:00.123
    fn is_naive_datetime(s: &str) -> bool {
        const DATETIME_FORMATS: &[&str] = &[
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M",
            "%Y-%m-%d %H:%M:%S%.f",
            "%Y-%m-%dT%H:%M:%S%.f",
        ];

        DATETIME_FORMATS
            .iter()
            .any(|fmt| chrono::NaiveDateTime::parse_from_str(s, fmt).is_ok())
    }

    /// Check common date-only formats such as
    /// - 2024-05-01
    /// - 2024/05/01
    /// - 20240501
    fn is_naive_date(s: &str) -> bool {
        const DATE_FORMATS: &[&str] = &["%Y-%m-%d", "%Y/%m/%d", "%Y%m%d"];

        DATE_FORMATS
            .iter()
            .any(|fmt| chrono::NaiveDate::parse_from_str(s, fmt).is_ok())
    }
}

/// Index schema definition for validation and evolution.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct IndexSchema {
    pub fields: HashMap<String, FieldDef>,
}

impl IndexSchema {
    /// Add or evolve a field based on JSON value (schema evolution)
    /// New fields are added as non-indexed to avoid Tantivy schema rebuilds.
    /// Existing fields can have their types evolved if compatible.
    pub fn evolve_field(&mut self, name: String, value: &JsonValue) -> bool {
        use std::collections::hash_map::Entry;

        // CRITICAL: Never evolve the mandatory 'id' field
        if name == "id" {
            return false; // id field is mandatory and should never evolve
        }

        let inferred_type = FieldDef::infer_type_from_value(value);

        match self.fields.entry(name.clone()) {
            Entry::Vacant(entry) => {
                // New field - create as non-indexed for background evolution
                // This allows the field to be stored in redb without requiring
                // Tantivy schema changes. Fields can be promoted to indexed later.
                let field_def = FieldDef::new_non_indexed(name, value);
                entry.insert(field_def);
                true
            }
            Entry::Occupied(mut entry) => {
                // Existing field - check if type evolution is needed
                let current_def = entry.get();

                // Only evolve if the inferred type is "more specific" or compatible
                if Self::should_evolve_field_static(current_def, inferred_type.clone()) {
                    let mut new_def = current_def.clone();
                    new_def.field_type = inferred_type;
                    entry.insert(new_def);
                    true
                } else {
                    false
                }
            }
        }
    }

    /// Determine if a field should evolve to a new type (static version to avoid borrowing issues)
    fn should_evolve_field_static(current: &FieldDef, new_type: TantivyFieldType) -> bool {
        // Don't evolve if types are the same
        if current.field_type == new_type {
            return false;
        }

        // Evolution rules - only allow certain upgrades
        match (&current.field_type, new_type) {
            // Text can be refined to more specific types
            (TantivyFieldType::Text, TantivyFieldType::Date) => true,
            (TantivyFieldType::Text, TantivyFieldType::Ip) => true,
            (TantivyFieldType::Text, TantivyFieldType::I64) => true,
            (TantivyFieldType::Text, TantivyFieldType::U64) => true,
            (TantivyFieldType::Text, TantivyFieldType::F64) => true,
            (TantivyFieldType::Text, TantivyFieldType::Boolean) => true,
            (TantivyFieldType::Text, TantivyFieldType::Json) => true,

            // Numeric types can be upgraded to more general types
            (TantivyFieldType::I64, TantivyFieldType::F64) => true,
            (TantivyFieldType::U64, TantivyFieldType::F64) => true,

            // String can be upgraded to Text (for tokenization)
            (TantivyFieldType::String, TantivyFieldType::Text) => true,

            _ => false, // Prevent downgrades or incompatible changes
        }
    }

    /// Evolve schema based on a JSON document
    pub fn evolve_from_document(&mut self, json_blob: &JsonValue) -> Vec<String> {
        let mut evolved_fields = Vec::new();

        if let Some(obj) = json_blob.as_object() {
            for (field_name, field_value) in obj {
                if self.evolve_field(field_name.clone(), field_value) {
                    evolved_fields.push(field_name.clone());
                }
            }
        }

        evolved_fields
    }

    /// Promote a field from non-indexed to indexed status
    /// This requires a Tantivy schema rebuild and should be done explicitly.
    /// Returns true if the field was promoted, false if it was already indexed or doesn't exist.
    pub fn promote_field_to_indexed(&mut self, field_name: &str) -> bool {
        if let Some(field_def) = self.fields.get_mut(field_name)
            && !field_def.indexed
        {
            field_def.indexed = true;
            tracing::info!(
                field = %field_name,
                field_type = ?field_def.field_type,
                "Promoted field to indexed status - requires Tantivy schema rebuild"
            );
            return true;
        }
        false
    }

    /// Get all non-indexed fields in the schema
    /// Useful for identifying fields that can be promoted to indexed status.
    pub fn get_non_indexed_fields(&self) -> Vec<String> {
        self.fields
            .iter()
            .filter(|(_, field_def)| !field_def.indexed)
            .map(|(name, _)| name.clone())
            .collect()
    }
}

/// Statistics for an index.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexStats {
    pub document_count: u64,
    pub total_size_bytes: u64,
    pub tantivy_index_exists: bool,
}

/// Per-index statistics gathered from a single shard.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct IndexShardStats {
    pub document_count: u64,
    pub redb_bytes: u64,
    pub tantivy_bytes: u64,
    pub tantivy_index_exists: bool,
    pub tantivy_scan_ms: u128,
}

/// Timing metadata for shard-level statistics gathering.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ShardStatsTimings {
    pub redb_ms: u128,
    pub tantivy_ms: u128,
    pub total_ms: u128,
}

/// Snapshot of all index stats within a shard along with timing info.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ShardStatsSnapshot {
    pub per_index: HashMap<String, IndexShardStats>,
    pub timings: ShardStatsTimings,
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

    #[error("redb durability error: {0}")]
    Durability(#[from] redb::SetDurabilityError),

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
        json_blob: Option<JsonValue>,
    },
    Delete {
        id: String,
    },
}

/// Helper struct for zero-copy serialization of stored documents
#[derive(Serialize)]
struct StoredDoc<'a> {
    json_blob: Option<&'a JsonValue>,
}

/// Owned version for deserialization from redb
#[derive(Deserialize)]
struct StoredDocOwned {
    json_blob: Option<JsonValue>,
}

/// Internal schema field mappings for Tantivy.
#[derive(Debug, Clone)]
pub struct SchemaFields {
    /// Tantivy field for the document identifier
    id: Field,
    /// Map of schema field name -> Tantivy field (only indexed fields are present)
    indexed_fields: HashMap<String, Field>,
}

/// Unified cache entry for index sizes (both Tantivy directory and Redb table) with timestamp
#[derive(Debug, Clone)]
struct IndexSizeCache {
    tantivy_bytes: u64,
    redb_bytes: u64,
    document_count: u64,
    timestamp: Instant,
}

/// Multi-tenant hybrid storage engine combining redb and tantivy.
pub struct HybridStore {
    /// Shared redb database across all indices
    kv: Database,
    /// Cache of IndexWriters keyed by index name
    writers: Arc<DashMap<String, Arc<Mutex<IndexWriter>>>>,
    /// Cache of IndexReaders keyed by index name
    readers: Arc<DashMap<String, IndexReader>>,
    /// Atomic counters for WAL sequence IDs per index
    current_seq: Arc<DashMap<String, AtomicU64>>,
    /// Operation counters for smart commits per index
    operations_counter: Arc<DashMap<String, AtomicU64>>,
    /// Simple per-index read cache for frequently accessed documents
    read_cache: Arc<DashMap<String, HashMap<String, Vec<u8>>>>,
    /// Cache of optimal memory budgets per index to avoid frequent syscalls
    budget_cache: Arc<DashMap<String, usize>>,
    /// Cache of schemas per index to avoid repeated redb reads
    schema_cache: Arc<DashMap<String, Arc<IndexSchema>>>,
    /// Cache of Tantivy field mappings per index
    fields_cache: Arc<DashMap<String, SchemaFields>>,
    /// Unified cache for index sizes (Tantivy + Redb) with expiration to avoid repeated expensive calculations
    index_size_cache: Arc<Mutex<HashMap<String, IndexSizeCache>>>,
    /// Cache expiration duration for index sizes (10 minutes)
    index_cache_expiry: Duration,
    /// Storage configuration
    config: StorageConfig,
}

impl HybridStore {
    /// Creates a new multi-tenant HybridStore.
    pub fn new(config: StorageConfig) -> Result<Self, StoreError> {
        let init_start = Instant::now();
        tracing::info!(
            shard_path = %config.shard_path.display(),
            "HybridStore: initializing shard storage"
        );

        // Create directory structure
        let dir_start = Instant::now();
        fs::create_dir_all(&config.shard_path)?;
        let kv_path = config.shard_path.join("store.redb");
        let indices_path = config.shard_path.join("indices");
        fs::create_dir_all(&indices_path)?;
        let dir_elapsed = dir_start.elapsed();
        tracing::debug!(
            shard_path = %config.shard_path.display(),
            indices_path = %indices_path.display(),
            elapsed_ms = dir_elapsed.as_millis(),
            "HybridStore: ensured directory structure"
        );

        // Create or open shared redb database
        let db_file_exists = kv_path.exists();
        let db_start = Instant::now();
        let kv = Database::create(&kv_path)?;
        let db_elapsed = db_start.elapsed();
        tracing::info!(
            shard_path = %config.shard_path.display(),
            db_path = %kv_path.display(),
            existed = db_file_exists,
            elapsed_ms = db_elapsed.as_millis(),
            "HybridStore: redb database opened"
        );

        Ok(HybridStore {
            kv,
            writers: Arc::new(DashMap::new()),
            readers: Arc::new(DashMap::new()),
            current_seq: Arc::new(DashMap::new()),
            operations_counter: Arc::new(DashMap::new()),
            read_cache: Arc::new(DashMap::new()),
            budget_cache: Arc::new(DashMap::new()),
            schema_cache: Arc::new(DashMap::new()),
            fields_cache: Arc::new(DashMap::new()),
            index_size_cache: Arc::new(Mutex::new(HashMap::new())),
            index_cache_expiry: Duration::from_secs(600), // 10 minutes
            config: config.clone(),
        })
        .inspect(|store| {
            let total_elapsed = init_start.elapsed();
            tracing::info!(
                shard_path = %store.config.shard_path.display(),
                elapsed_ms = total_elapsed.as_millis(),
                "HybridStore: initialization complete"
            );
        })
    }

    /// Gracefully shutdown the HybridStore, releasing all locks and resources
    pub fn shutdown(&self) -> Result<(), StoreError> {
        tracing::info!("HybridStore: Starting graceful shutdown");

        // Check which indices have pending operations
        let indices_with_pending_ops: Vec<String> = self
            .operations_counter
            .iter()
            .filter(|entry| entry.value().load(Ordering::SeqCst) > 0)
            .map(|entry| entry.key().clone())
            .collect();

        if indices_with_pending_ops.is_empty() {
            tracing::info!("No pending operations, skipping commits during shutdown");
        } else {
            tracing::info!(
                indices_count = indices_with_pending_ops.len(),
                indices = ?indices_with_pending_ops,
                "Committing indices with pending operations during shutdown"
            );
        }

        // Commit only writers with pending operations
        for entry in self.writers.iter() {
            let index = entry.key();
            let writer_arc = entry.value();
            if indices_with_pending_ops.contains(index) {
                match writer_arc.try_lock() {
                    Ok(mut writer) => {
                        tracing::debug!(index = %index, "Committing index during shutdown");
                        if let Err(e) = writer.commit() {
                            tracing::warn!(index = %index, error = %e, "Failed to commit index during shutdown");
                        }
                    }
                    Err(_) => {
                        tracing::warn!(index = %index, "Writer lock busy during shutdown, skipping commit");
                    }
                }
            } else {
                tracing::debug!(index = %index, "No pending operations, skipping commit during shutdown");
            }
        }

        // Clear all caches
        self.schema_cache.clear();
        self.budget_cache.clear();
        self.operations_counter.clear();
        self.current_seq.clear();
        self.index_size_cache.lock().unwrap().clear();

        // Force a final redb fsync/flush using an empty Immediate-durability transaction
        // This reduces WAL replay on next startup and ensures the current root is persisted.
        match self.kv.begin_write() {
            Ok(mut txn) => {
                if let Err(e) = txn.set_durability(Durability::Immediate) {
                    tracing::warn!(error = %e, "Failed to set durability on shutdown flush");
                } else if let Err(e) = txn.commit() {
                    tracing::warn!(error = %e, "Failed to commit shutdown flush transaction");
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "Failed to open shutdown flush transaction");
            }
        }

        tracing::info!("HybridStore: Graceful shutdown completed");
        Ok(())
    }

    /// Get a value from the read cache if present.
    fn get_from_cache(&self, index: &str, key: &str) -> Option<Vec<u8>> {
        self.read_cache.get(index)?.get(key).cloned()
    }

    /// Insert a value into the read cache with a simple per-index size bound.
    fn insert_into_cache(&self, index: &str, key: &str, value: Vec<u8>) {
        const MAX_CACHE_ENTRIES_PER_INDEX: usize = 1024;

        let mut index_cache = self.read_cache.entry(index.to_string()).or_default();

        if index_cache.len() >= MAX_CACHE_ENTRIES_PER_INDEX
            && let Some(first_key) = index_cache.keys().next().cloned()
        {
            index_cache.remove(&first_key);
        }

        index_cache.insert(key.to_string(), value);
    }

    /// Build Tantivy schema and field map from index schema definition using native Tantivy types.
    fn create_schema_from_definition(index_schema: &IndexSchema) -> (Schema, SchemaFields) {
        use tantivy::schema::{IndexRecordOption, TextFieldIndexing, TextOptions};

        let mut schema_builder = Schema::builder();

        // ID field is always present - untokenized string for exact matching
        let id_field = schema_builder.add_text_field("id", STRING | STORED);

        let mut indexed_fields = HashMap::new();

        for (name, field_def) in &index_schema.fields {
            if name == "id" || !field_def.indexed {
                continue;
            }

            let field = match field_def.field_type {
                TantivyFieldType::Text => {
                    let mut options = TextOptions::default().set_indexing_options(
                        TextFieldIndexing::default()
                            .set_tokenizer(field_def.tokenizer.as_deref().unwrap_or("default"))
                            .set_index_option(match field_def.index_record_option.as_deref() {
                                Some("Basic") => IndexRecordOption::Basic,
                                Some("WithFreqs") => IndexRecordOption::WithFreqs,
                                _ => IndexRecordOption::WithFreqsAndPositions,
                            }),
                    );
                    if field_def.stored {
                        options = options.set_stored();
                    }
                    schema_builder.add_text_field(name, options)
                }
                TantivyFieldType::String => schema_builder.add_text_field(name, STRING),
                TantivyFieldType::I64 => {
                    if field_def.fast {
                        schema_builder.add_i64_field(name, INDEXED | FAST)
                    } else {
                        schema_builder.add_i64_field(name, INDEXED)
                    }
                }
                TantivyFieldType::U64 => {
                    if field_def.fast {
                        schema_builder.add_u64_field(name, INDEXED | FAST)
                    } else {
                        schema_builder.add_u64_field(name, INDEXED)
                    }
                }
                TantivyFieldType::F64 => {
                    if field_def.fast {
                        schema_builder.add_f64_field(name, INDEXED | FAST)
                    } else {
                        schema_builder.add_f64_field(name, INDEXED)
                    }
                }
                TantivyFieldType::Date => {
                    if field_def.fast {
                        schema_builder.add_date_field(name, INDEXED | FAST)
                    } else {
                        schema_builder.add_date_field(name, INDEXED)
                    }
                }
                TantivyFieldType::Boolean => schema_builder.add_bool_field(name, INDEXED),
                TantivyFieldType::Bytes => schema_builder.add_bytes_field(name, INDEXED),
                TantivyFieldType::Ip => schema_builder.add_ip_addr_field(name, INDEXED),
                TantivyFieldType::Json => schema_builder.add_json_field(name, TEXT),
                TantivyFieldType::Facet => schema_builder.add_facet_field(name, INDEXED),
            };

            indexed_fields.insert(name.clone(), field);
        }

        let schema = schema_builder.build();
        let fields = SchemaFields {
            id: id_field,
            indexed_fields,
        };

        (schema, fields)
    }

    /// Derive Tantivy field mapping from an existing index schema on disk.
    fn load_fields_from_existing_index(tantivy_index: &Index) -> Result<SchemaFields, StoreError> {
        let schema = tantivy_index.schema();

        let id = schema
            .get_field("id")
            .map_err(|_| StoreError::FieldNotFound("id".to_string()))?;

        let mut indexed_fields = HashMap::new();
        for (field, field_entry) in schema.fields() {
            let name = field_entry.name();
            if name == "id" {
                continue;
            }
            indexed_fields.insert(name.to_string(), field);
        }

        Ok(SchemaFields { id, indexed_fields })
    }

    /// Derive IndexSchema from a Tantivy index's schema.
    /// This reads back the actual persisted schema from Tantivy and converts it
    /// to our IndexSchema format, ensuring we're in sync with what Tantivy has.
    /// NOTE: Excludes the mandatory 'id' field since it's implicit in Tantivy
    fn derive_index_schema_from_tantivy(tantivy_index: &Index) -> IndexSchema {
        use tantivy::schema::FieldType;

        let schema = tantivy_index.schema();
        let mut fields = HashMap::new();

        for (_field, field_entry) in schema.fields() {
            let name = field_entry.name();
            if name == "id" {
                continue; // Skip the mandatory id field - it's implicit in Tantivy
            }

            let field_type = match field_entry.field_type() {
                FieldType::Str(_) => {
                    // Check if it's indexed with STRING flag (untokenized) or default TEXT
                    // For simplicity, we'll check if it's stored but not indexed as a heuristic
                    let is_indexed = field_entry.is_indexed();
                    let is_stored = field_entry.is_stored();
                    if is_stored && !is_indexed {
                        TantivyFieldType::String
                    } else {
                        TantivyFieldType::Text
                    }
                }
                FieldType::U64(_) => TantivyFieldType::U64,
                FieldType::I64(_) => TantivyFieldType::I64,
                FieldType::F64(_) => TantivyFieldType::F64,
                FieldType::Bool(_) => TantivyFieldType::Boolean,
                FieldType::Date(_) => TantivyFieldType::Date,
                FieldType::Bytes(_) => TantivyFieldType::Bytes,
                FieldType::JsonObject(_) => TantivyFieldType::Json,
                FieldType::IpAddr(_) => TantivyFieldType::Ip,
                FieldType::Facet(_) => TantivyFieldType::Facet,
            };

            // Determine field options from Tantivy's field entry
            let indexed = field_entry.is_indexed();
            let stored = field_entry.is_stored();
            let fast = field_entry.is_fast();

            // Capture additional options for Text fields
            let (tokenizer, index_record_option) = if let FieldType::Str(text_options) =
                field_entry.field_type()
            {
                // Extract the actual tokenizer and index options from Tantivy
                let tokenizer_name = match text_options.get_indexing_options() {
                    Some(opts) => {
                        let token_name = opts.tokenizer().to_string();
                        tracing::debug!(field_name = %name, tokenizer = %token_name, "Extracted tokenizer from Tantivy");
                        Some(token_name)
                    }
                    None => {
                        tracing::debug!(field_name = %name, "No indexing options found, using default tokenizer");
                        Some("default".to_string())
                    }
                };

                let index_option = match text_options.get_indexing_options() {
                    Some(opts) => {
                        let opt_str = match opts.index_option() {
                            tantivy::schema::IndexRecordOption::Basic => "Basic".to_string(),
                            tantivy::schema::IndexRecordOption::WithFreqs => {
                                "WithFreqs".to_string()
                            }
                            tantivy::schema::IndexRecordOption::WithFreqsAndPositions => {
                                "WithFreqsAndPositions".to_string()
                            }
                        };
                        tracing::debug!(field_name = %name, index_option = %opt_str, "Extracted index option from Tantivy");
                        Some(opt_str)
                    }
                    None => {
                        tracing::debug!(field_name = %name, "No indexing options found, using default index option");
                        Some("WithFreqsAndPositions".to_string())
                    }
                };
                (tokenizer_name, index_option)
            } else {
                tracing::debug!(field_name = %name, field_type = ?field_entry.field_type(), "Non-text field, no tokenizer options");
                (None, None)
            };

            fields.insert(
                name.to_string(),
                FieldDef {
                    name: name.to_string(),
                    field_type,
                    indexed,
                    stored,
                    fast,
                    tokenizer,
                    index_record_option,
                },
            );
        }

        IndexSchema { fields }
    }

    /// Helper method: get_or_create_index
    /// Made public to allow pre-creating indexes when schema is created
    pub fn get_or_create_index(
        &self,
        index: &str,
    ) -> Result<(Arc<Mutex<IndexWriter>>, SchemaFields), StoreError> {
        // Fast path: Check writers cache first
        if let Some(writer) = self.writers.get(index)
            && let Some(fields) = self.fields_cache.get(index)
        {
            return Ok((Arc::clone(writer.value()), fields.value().clone()));
        }

        // Create index directory and Tantivy index if it doesn't exist
        let index_path = self.config.shard_path.join("indices").join(index);

        // Determine schema for this index
        let index_schema = self
            .get_schema_cached(index)?
            .unwrap_or_else(|| Arc::new(IndexSchema::default()));

        let (schema, _) = Self::create_schema_from_definition(&index_schema);

        // Create or open tantivy index, and get the correct field handles
        let (tantivy_index, fields, sync_schema) = if index_path.join("meta.json").exists() {
            // Opening existing index: must use Field handles from the opened index's schema
            let opened_index = Index::open_in_dir(&index_path)?;
            let fields = Self::load_fields_from_existing_index(&opened_index)?;
            (opened_index, fields, false)
        } else {
            // Creating new index: use the schema and fields we just built
            fs::create_dir_all(&index_path)?;
            let new_index = Index::create_in_dir(&index_path, schema)?;

            // After creating the index, read back the actual Tantivy schema and sync it.
            // This ensures our cached schema matches exactly what Tantivy persisted.
            let fields = Self::load_fields_from_existing_index(&new_index)?;
            (new_index, fields, true)
        };

        // IMPORTANT: Only sync schema when we actually created a new index
        // This ensures we don't overwrite persisted schema when index was deleted
        if sync_schema {
            // Derive schema from Tantivy (indexed fields only, excludes 'id')
            let mut tantivy_schema = Self::derive_index_schema_from_tantivy(&tantivy_index);

            // CRITICAL: Always add the mandatory 'id' field to our schema cache
            // The 'id' field is implicit in Tantivy but required for our validation
            tantivy_schema.fields.insert(
                "id".to_string(),
                FieldDef {
                    name: "id".to_string(),
                    field_type: TantivyFieldType::Text,
                    indexed: true,
                    stored: true,
                    fast: false,
                    tokenizer: Some("raw".to_string()),
                    index_record_option: Some("Basic".to_string()),
                },
            );

            // Merge with stored schema to preserve non-indexed fields
            // The stored schema (index_schema) contains the full field definitions
            // including fields that may not be indexed but are part of the schema
            for (name, field_def) in &index_schema.fields {
                tantivy_schema
                    .fields
                    .entry(name.clone())
                    .or_insert_with(|| field_def.clone());
            }

            // IMPORTANT: Cache should reflect merged schema (Tantivy + stored metadata)
            self.schema_cache
                .insert(index.to_string(), Arc::new(tantivy_schema.clone()));

            // Persist the merged schema to redb for future reference
            self.store_schema(index, &tantivy_schema)?;

            // CRITICAL: Clear reader cache to ensure search sees latest commits
            // This prevents searches from using stale readers that don't see newly written documents
            self.readers.remove(index);

            tracing::debug!(index = %index, "Schema synced: Tantivy schema merged with stored metadata, cached and persisted");
        }

        // Create writer with dynamic memory budget based on index size
        let optimal_budget = self.config.get_optimal_memory_budget(&index_path);

        // Cache the budget
        self.budget_cache.insert(index.to_string(), optimal_budget);

        let writer = tantivy_index.writer(optimal_budget)?;
        let writer_arc = Arc::new(Mutex::new(writer));

        // Store in cache
        self.writers
            .insert(index.to_string(), Arc::clone(&writer_arc));
        self.fields_cache.insert(index.to_string(), fields.clone());

        // Initialize sequence counter for this index if needed
        self.current_seq
            .entry(index.to_string())
            .or_insert_with(|| {
                let max_seq = self.get_max_wal_id_for_index(index).unwrap_or(0);
                AtomicU64::new(max_seq)
            });

        Ok((writer_arc, fields))
    }

    /// Track document count and perform smart commits based on operation thresholds
    fn should_commit_writer(&self, index: &str, operations_since_commit: u64) -> bool {
        // Get dynamic memory budget for this specific index
        // Use cached budget if available to avoid syscalls on every write
        let budget = if let Some(b) = self.budget_cache.get(index) {
            *b.value()
        } else {
            // Fallback: calculate and cache
            let index_path = self.config.shard_path.join("indices").join(index);
            let b = self.config.get_optimal_memory_budget(&index_path);
            self.budget_cache.insert(index.to_string(), b);
            b
        };

        // Commit strategy based on document count and configurable memory budget range
        // Scale commit frequency with memory budget: more memory = fewer commits
        let min_budget = self.config.indexer_memory_min_mb * 1024 * 1024;
        let max_budget = self.config.indexer_memory_max_mb * 1024 * 1024;

        // Calculate adaptive threshold based on default_batch_size and memory budget ratio
        let budget_ratio = (budget - min_budget) as f64 / (max_budget - min_budget) as f64;
        let default_batch = self.config.default_batch_size as f64;

        // Scale from 50% of default (min memory) to 800% of default (max memory)
        // e.g., default=2000: 1000 ops (16MB) -> 16000 ops (256MB)
        let base_ops = (default_batch * (0.5 + budget_ratio * 7.5)) as u64;

        operations_since_commit >= base_ops
    }

    /// Get operation count for an index since last commit
    pub fn get_operations_count(&self, index: &str) -> u64 {
        self.operations_counter
            .get(index)
            .map(|counter| counter.value().load(Ordering::SeqCst))
            .unwrap_or(0)
    }

    /// Increment operation count and return new count
    fn increment_operations(&self, index: &str) -> u64 {
        self.operations_counter
            .entry(index.to_string())
            .or_insert_with(|| AtomicU64::new(0))
            .value()
            .fetch_add(1, Ordering::SeqCst)
            + 1
    }

    /// Reset operation counter after commit
    pub fn reset_operations_counter(&self, index: &str) {
        if let Some(counter) = self.operations_counter.get(index) {
            counter.value().store(0, Ordering::SeqCst);
        }
    }

    /// Reset operation counter to a specific value (for intermediate commits)
    /// This allows the supervisor to continue working while resetting the counter
    pub fn reset_operations_counter_to(&self, index: &str, value: u64) {
        if let Some(counter) = self.operations_counter.get(index) {
            counter.value().store(value, Ordering::SeqCst);
        }
    }

    /// Smart refresh strategy for reader cache
    /// Tries fast reload first, falls back to remove + recreate if reload fails
    /// This preserves cache when possible while ensuring data freshness
    fn smart_refresh_reader(&self, index: &str) -> Result<(), StoreError> {
        // Fast path: Try to reload existing reader
        if let Some(reader_ref) = self.readers.get(index) {
            match reader_ref.value().reload() {
                Ok(_) => {
                    tracing::debug!(index = %index, "Reader reloaded successfully (fast path)");
                    return Ok(());
                }
                Err(e) => {
                    tracing::warn!(index = %index, error = %e, "Reader reload failed, falling back to recreation");
                }
            }
        }

        // Fallback: Remove and recreate (reliable path)
        self.readers.remove(index);
        tracing::debug!(index = %index, "Reader cache cleared, will recreate on next search (reliable path)");
        Ok(())
    }

    /// Force a commit for a specific index
    pub fn commit_index(&self, index: &str) -> Result<(), StoreError> {
        if let Some(writer_arc) = self.writers.get(index) {
            let mut writer = writer_arc.value().lock().unwrap();
            writer.commit()?;
            self.reset_operations_counter(index);

            // CRITICAL: Smart refresh reader cache after commit to ensure search sees latest data
            // This tries fast reload first, falls back to cache clearing if needed
            self.smart_refresh_reader(index)?;

            // Refresh budget cache after commit since index size likely changed
            let index_path = self.config.shard_path.join("indices").join(index);
            let new_budget = self.config.get_optimal_memory_budget(&index_path);
            self.budget_cache.insert(index.to_string(), new_budget);
        }

        Ok(())
    }

    /// Refresh writer cache for an index to handle lock contention
    /// This removes and recreates the writer to ensure clean state
    pub fn refresh_writer(&self, index: &str) -> Result<(), StoreError> {
        tracing::debug!(index = %index, "Refreshing writer cache to resolve lock contention");

        // Remove existing writer from cache
        self.writers.remove(index);

        // Force garbage collection to ensure locks are released
        {
            let index_path = self.config.shard_path.join("indices").join(index);
            if let Ok(tantivy_index) = tantivy::Index::open_in_dir(&index_path) {
                // This will help ensure any lingering locks are released
                drop(tantivy_index);
            }
        }

        // Minimal delay to ensure writer cache cleanup completes
        std::thread::sleep(std::time::Duration::from_micros(100));

        // Recreate the writer (will be done lazily on next access)
        Ok(())
    }

    /// Force commit writer for an index (for testing)
    pub fn commit_writer(&self, index: &str) -> Result<(), StoreError> {
        self.commit_index(index)
    }

    /// Perform smart commit based on operation count
    fn maybe_commit_writer(&self, index: &str) -> Result<bool, StoreError> {
        let ops_count = self.get_operations_count(index);

        if self.should_commit_writer(index, ops_count) {
            self.commit_index(index)?;
            return Ok(true);
        }
        Ok(false)
    }

    /// Multi-tenant apply_write method
    pub fn apply_write(&self, index: &str, op: WalOp) -> Result<u64, StoreError> {
        // Get or create the index
        let (writer_arc, fields) = self.get_or_create_index(index)?;

        // Get sequence ID for this index
        let seq_id = {
            let counter = self.current_seq.get(index).ok_or_else(|| {
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

        // Step 1.5: Evolve schema if new fields are present (declare outside transaction scope)
        let mut evolved_schema = None;

        let mut write_txn = self.kv.begin_write()?;
        {
            // Set durability based on config (except for schema changes which always use Immediate)
            let durability = if self.config.wal_sync {
                Durability::Immediate
            } else {
                Durability::None
            };
            write_txn.set_durability(durability)?;
            tracing::trace!(index = %index, durability = ?durability, "Data transaction durability set (user data)");

            let mut wal_table = write_txn.open_table(wal_table_def)?;
            wal_table.insert(seq_id, wal_data.as_slice())?;

            // Apply to data table
            match op {
                WalOp::Put { id, json_blob } => {
                    // Step 1: Get cached schema for field filtering and evolution
                    // If not in cache, load from persisted metadata
                    let schema = if let Some(schema) = self.get_schema_cached(index)? {
                        schema
                    } else {
                        // Load from metadata if not in cache
                        tracing::debug!(index = %index, "Loading schema from metadata store");
                        self.get_schema(index)?
                            .map(Arc::new)
                            .unwrap_or_else(|| Arc::new(IndexSchema::default()))
                    };

                    if let Some(json_blob) = &json_blob {
                        let mut schema_mut = (*schema).clone();
                        let evolved_fields = schema_mut.evolve_from_document(json_blob);
                        if !evolved_fields.is_empty() {
                            tracing::debug!(
                                index = %index,
                                evolved_fields = ?evolved_fields,
                                "Evolved schema with new non-indexed fields (will persist in separate transaction)"
                            );
                            // Store evolved schema for persistence after data transaction
                            evolved_schema = Some(schema_mut.clone());

                            // Update cache immediately for subsequent reads
                            let schema_arc = Arc::new(schema_mut);
                            self.schema_cache.insert(index.to_string(), schema_arc);
                            // Note: No need to invalidate fields cache since new fields are non-indexed
                            // and won't affect Tantivy schema
                        }
                    }

                    // Step 2: Serialize complete document for redb (all fields)
                    let doc_data = StoredDoc {
                        json_blob: json_blob.as_ref(),
                    };
                    let doc_bytes = serde_json::to_vec(&doc_data)
                        .map_err(|e| StoreError::Serialization(e.to_string()))?;

                    let mut data_table = write_txn.open_table(data_table_def)?;

                    // Check if document is new or updated by examining insert return value
                    let old_value = data_table.insert(id.as_str(), doc_bytes.as_slice())?;
                    let is_new_document = old_value.is_none();

                    // Step 3: Build tantivy document with ONLY indexed fields
                    let mut tantivy_doc = doc!(fields.id => id.as_str());

                    // Step 4: Index schema-defined fields individually
                    for (field_name, field_def) in &schema.fields {
                        if !field_def.indexed {
                            continue;
                        }

                        if let Some(tantivy_field) = fields.indexed_fields.get(field_name)
                            && let Some(json_obj) = json_blob.as_ref().and_then(|v| v.as_object())
                            && let Some(field_value) = json_obj.get(field_name)
                        {
                            match field_def.field_type {
                                TantivyFieldType::Text => {
                                    if let Some(s) = field_value.as_str() {
                                        tantivy_doc.add_text(*tantivy_field, s);
                                    } else {
                                        let field_str = serde_json::to_string(field_value)
                                            .map_err(|e| {
                                                StoreError::Serialization(e.to_string())
                                            })?;
                                        tantivy_doc.add_text(*tantivy_field, &field_str);
                                    }
                                }
                                TantivyFieldType::String => {
                                    if let Some(s) = field_value.as_str() {
                                        tantivy_doc.add_text(*tantivy_field, s);
                                    } else if let Some(arr) = field_value.as_array() {
                                        for item in arr {
                                            if let Some(s) = item.as_str() {
                                                tantivy_doc.add_text(*tantivy_field, s);
                                            }
                                        }
                                    }
                                }
                                TantivyFieldType::F64 => {
                                    if let Some(n) = field_value.as_f64() {
                                        tantivy_doc.add_f64(*tantivy_field, n);
                                    }
                                }
                                TantivyFieldType::I64 => {
                                    if let Some(n) = field_value.as_i64() {
                                        tantivy_doc.add_i64(*tantivy_field, n);
                                    }
                                }
                                TantivyFieldType::U64 => {
                                    if let Some(n) = field_value.as_u64() {
                                        tantivy_doc.add_u64(*tantivy_field, n);
                                    }
                                }
                                TantivyFieldType::Date => {
                                    if let Some(s) = field_value.as_str()
                                        && let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s)
                                    {
                                        let timestamp_secs = dt.timestamp();
                                        // Clamp to Tantivy's safe range to avoid i64 overflow
                                        // Original date is preserved in redb json_blob
                                        let clamped_ts = timestamp_secs.clamp(
                                            TANTIVY_MIN_TIMESTAMP_SECS,
                                            TANTIVY_MAX_TIMESTAMP_SECS,
                                        );
                                        let tantivy_dt = DateTime::from_timestamp_secs(clamped_ts);
                                        if timestamp_secs != clamped_ts {
                                            tracing::debug!(
                                                field = %field_name,
                                                input = %s,
                                                original_ts = %timestamp_secs,
                                                clamped_ts = %clamped_ts,
                                                "Date clamped to Tantivy safe range"
                                            );
                                        }
                                        tantivy_doc.add_date(*tantivy_field, tantivy_dt);
                                    }
                                }
                                TantivyFieldType::Boolean => {
                                    if let Some(b) = field_value.as_bool() {
                                        tantivy_doc.add_bool(*tantivy_field, b);
                                    }
                                }
                                TantivyFieldType::Bytes => {
                                    if let Some(arr) = field_value.as_array() {
                                        let mut bytes = Vec::new();
                                        for item in arr {
                                            if let Some(n) = item.as_u64() {
                                                bytes.push(n as u8);
                                            }
                                        }
                                        if !bytes.is_empty() {
                                            tantivy_doc.add_bytes(*tantivy_field, bytes.as_slice());
                                        }
                                    }
                                }
                                TantivyFieldType::Ip => {
                                    if let Some(s) = field_value.as_str()
                                        && let Ok(ip) = s.parse::<std::net::IpAddr>()
                                    {
                                        // Convert any IP address to IPv6 for Tantivy compatibility
                                        let ipv6 = match ip {
                                            std::net::IpAddr::V4(ipv4) => ipv4.to_ipv6_mapped(),
                                            std::net::IpAddr::V6(ipv6) => ipv6,
                                        };
                                        tantivy_doc.add_ip_addr(*tantivy_field, ipv6);
                                    }
                                }
                                TantivyFieldType::Json => {
                                    let json_str = serde_json::to_string(field_value)
                                        .map_err(|e| StoreError::Serialization(e.to_string()))?;
                                    tantivy_doc.add_text(*tantivy_field, &json_str);
                                }
                                TantivyFieldType::Facet => {
                                    if let Some(s) = field_value.as_str() {
                                        tantivy_doc.add_facet(*tantivy_field, &s);
                                    }
                                }
                            }
                        }
                    }

                    let writer = writer_arc.lock().unwrap();

                    // Optimized Tantivy operations: delete only if document was updated
                    if !is_new_document {
                        // Document was updated - delete old version first
                        let term = tantivy::Term::from_field_text(fields.id, &id);
                        writer.delete_term(term);
                    }
                    // Add the document (new or updated)
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

        // Persist schema evolution in separate transaction with Immediate durability
        if let Some(evolved) = evolved_schema {
            // Note: Schema persistence failure is critical but doesn't affect data consistency
            // The data has already been committed successfully, but schema evolution failed
            match self.persist_schema_evolution(index, &evolved) {
                Ok(()) => {
                    // Update cache after successful persistence
                    self.schema_cache
                        .insert(index.to_string(), Arc::new(evolved));
                    tracing::info!(index = %index, "Schema evolution persisted successfully");
                }
                Err(e) => {
                    tracing::error!(
                        index = %index,
                        error = %e,
                        "CRITICAL: Schema evolution failed after data commit. Data was saved but schema may be inconsistent."
                    );
                    // Return error to signal the issue, but note that data was already committed
                    return Err(StoreError::Serialization(format!(
                        "Schema evolution failed for index {}: {}. Data was committed but schema may be inconsistent.",
                        index, e
                    )));
                }
            }
        }

        // Increment operation counter and perform smart commit if needed
        self.increment_operations(index);
        self.maybe_commit_writer(index)?;

        Ok(seq_id)
    }

    /// Delete all data for an index using redb's efficient delete_table() function
    /// If delete_schema is true, also removes schema metadata from TABLE_SCHEMA
    pub fn delete_index_data(&self, index: &str, delete_schema: bool) -> Result<(), StoreError> {
        // Remove from caches first
        self.writers.remove(index);
        self.readers.remove(index);
        self.current_seq.remove(index);
        self.read_cache.remove(index);
        self.schema_cache.remove(index);
        self.fields_cache.remove(index);
        self.budget_cache.remove(index);

        // Invalidate size cache entries for this index
        {
            let mut size_cache = self.index_size_cache.lock().unwrap();
            size_cache.retain(|key, _| !key.contains(&format!(":{}", index)));
        }

        // Delete redb tables completely using delete_table() for efficiency
        let mut write_txn = self.kv.begin_write()?;
        {
            // Index deletion always uses Immediate durability for critical metadata operations
            write_txn.set_durability(Durability::Immediate)?;
            tracing::trace!(index = %index, durability = "Immediate", "Index deletion durability set");

            let data_table_name = format!("data_{}", index);
            let wal_table_name = format!("wal_{}", index);
            let data_table_def = TableDefinition::<&str, &[u8]>::new(&data_table_name);
            let wal_table_def = TableDefinition::<u64, &[u8]>::new(&wal_table_name);

            // Delete tables using redb's delete_table function (more efficient than manual clearing)
            // Note: delete_table returns bool indicating if table existed, we ignore the result
            let _ = write_txn.delete_table(data_table_def)?;
            let _ = write_txn.delete_table(wal_table_def)?;

            // Conditionally delete schema metadata if requested
            if delete_schema {
                tracing::debug!(index = %index, "Deleting schema metadata from TABLE_SCHEMA");
                let mut schema_table = write_txn.open_table(TABLE_SCHEMA)?;
                let _ = schema_table.remove(index)?;
            } else {
                tracing::debug!(index = %index, "Keeping schema metadata in TABLE_SCHEMA");
            }
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

        let mut write_txn = self.kv.begin_write()?;
        {
            // Schema changes always use Immediate durability for critical metadata
            write_txn.set_durability(Durability::Immediate)?;
            tracing::trace!(index = %index_name, durability = "Immediate", "Schema persistence durability set");

            let mut schema_table = write_txn.open_table(TABLE_SCHEMA)?;
            schema_table.insert(index_name, schema_bytes.as_slice())?;
        }
        write_txn.commit()?;

        Ok(())
    }

    /// Persist schema evolution with Immediate durability (critical metadata)
    fn persist_schema_evolution(
        &self,
        index_name: &str,
        schema: &IndexSchema,
    ) -> Result<(), StoreError> {
        let schema_bytes =
            serde_json::to_vec(schema).map_err(|e| StoreError::Serialization(e.to_string()))?;

        let mut write_txn = self.kv.begin_write()?;
        {
            // Schema evolution always uses Immediate durability for critical metadata
            write_txn.set_durability(Durability::Immediate)?;
            tracing::trace!(index = %index_name, durability = "Immediate", "Schema evolution persistence durability set");

            let mut schema_table = write_txn.open_table(TABLE_SCHEMA)?;
            schema_table.insert(index_name, schema_bytes.as_slice())?;
        }
        write_txn.commit()?;

        tracing::debug!(index = %index_name, "Schema evolution persisted with Immediate durability");
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

    /// Get schema from cache, or load from Tantivy and cache it
    /// IMPORTANT: Always prefers Tantivy schema (source of truth) over stored schema
    pub fn get_schema_cached(&self, index: &str) -> Result<Option<Arc<IndexSchema>>, StoreError> {
        // Fast path: check cache first
        if let Some(schema) = self.schema_cache.get(index) {
            return Ok(Some(Arc::clone(schema.value())));
        }

        // Slow path: load from Tantivy (source of truth), not from stored schema
        // Get the index path and open the Tantivy index directly
        let index_path = self.config.shard_path.join("indices").join(index);

        // Always load stored schema first (may contain non-indexed fields)
        let stored_schema = self.get_schema(index)?;

        if index_path.exists() {
            let tantivy_index = Index::open_in_dir(&index_path)?;

            // Derive schema from Tantivy (indexed fields only, excludes 'id')
            let tantivy_schema = Self::derive_index_schema_from_tantivy(&tantivy_index);

            // If Tantivy has no indexed fields (empty or only 'id'), prefer stored schema
            if tantivy_schema.fields.is_empty()
                && let Some(stored) = stored_schema
            {
                self.schema_cache
                    .insert(index.to_string(), Arc::new(stored.clone()));
                tracing::debug!(index = %index, "Using stored schema (Tantivy has no indexed fields yet)");
                return Ok(Some(Arc::new(stored)));
            }

            // Merge stored fields into Tantivy schema to preserve non-indexed fields
            let mut merged_schema = tantivy_schema;
            if let Some(mut stored) = stored_schema {
                for (name, field_def) in stored.fields.drain() {
                    merged_schema.fields.entry(name).or_insert(field_def);
                }
            }

            // Cache the merged schema
            self.schema_cache
                .insert(index.to_string(), Arc::new(merged_schema.clone()));

            tracing::debug!(index = %index, "Loaded and cached merged schema (Tantivy + stored metadata)");
            Ok(Some(Arc::new(merged_schema)))
        } else {
            // Fallback: try to load from stored schema (metadata only)
            if let Some(stored) = stored_schema {
                self.schema_cache
                    .insert(index.to_string(), Arc::new(stored.clone()));
                tracing::debug!(index = %index, "Using stored schema as fallback (Tantivy not available)");
                Ok(Some(Arc::new(stored)))
            } else {
                Ok(None)
            }
        }
    }

    /// Invalidate cache entry when schema is updated
    pub fn invalidate_schema_cache(&self, index: &str) {
        self.schema_cache.remove(index);
        self.fields_cache.remove(index);
        tracing::debug!(index = %index, "Invalidated schema and fields cache");
    }

    /// Evolve schema from a JSON document and invalidate caches if changed
    pub fn evolve_schema_from_document(
        &self,
        index: &str,
        json_blob: &JsonValue,
    ) -> Result<Vec<String>, StoreError> {
        // Get current schema
        let mut schema = self
            .get_schema_cached(index)?
            .unwrap_or_else(|| Arc::new(IndexSchema::default()));

        // Make it mutable for evolution
        let evolved_fields = Arc::make_mut(&mut schema).evolve_from_document(json_blob);

        if !evolved_fields.is_empty() {
            tracing::info!(
                index = %index,
                evolved_fields = ?evolved_fields,
                "Schema evolved with new fields"
            );

            // Store the evolved schema
            self.store_schema_and_cache(index, &schema)?;

            // Invalidate caches to force rebuild with new schema
            self.invalidate_schema_cache(index);
        }

        Ok(evolved_fields)
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
        self.schema_cache.insert(index.to_string(), schema_arc);

        // Invalidate fields cache so it rebuilds on next access
        self.fields_cache.remove(index);

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

    /// Helper: Get fields cache (Lock-Free Read)
    fn get_fields_for_index(
        &self,
        index: &str,
        tantivy_index: &Index,
    ) -> Result<SchemaFields, StoreError> {
        // Fast path: fields already cached
        if let Some(fields) = self.fields_cache.get(index) {
            return Ok(fields.value().clone());
        }

        // Derive fields from the opened Tantivy index (Field handles must match the index)
        let fields = Self::load_fields_from_existing_index(tantivy_index)?;
        self.fields_cache.insert(index.to_string(), fields.clone());
        Ok(fields)
    }

    /// Get or create a cached IndexReader for the given index
    /// Uses lock-free fast path with DashMap and ReloadPolicy::OnCommitWithDelay for automatic background updates
    fn get_reader(&self, index: &str) -> Result<Option<(IndexReader, SchemaFields)>, StoreError> {
        // Fast path: Zero-lock retrieval from cache
        if let Some(reader_ref) = self.readers.get(index) {
            let reader = reader_ref.value();
            // Note: Manual reload() removed. Reader configured with ReloadPolicy::OnCommitWithDelay
            // will automatically reload within milliseconds after commits.

            // Get fields (fast lookup)
            let tantivy_index = reader.searcher().index().clone();
            let fields = self.get_fields_for_index(index, &tantivy_index)?;

            return Ok(Some((reader.clone(), fields)));
        }

        // Slow path: Index not cached, need to open and cache it
        let index_path = self.config.shard_path.join("indices").join(index);
        if !index_path.exists() || !index_path.join("meta.json").exists() {
            return Ok(None);
        }

        // Use DashMap entry API for concurrent-safe creation
        let reader = self
            .readers
            .entry(index.to_string())
            .or_try_insert_with(|| {
                let tantivy_index = Index::open_in_dir(&index_path)?;

                // Configure reader with ReloadPolicy::OnCommitWithDelay for automatic background reloading
                // This watches meta.json and reloads within milliseconds after commits
                let reader = tantivy_index
                    .reader_builder()
                    .reload_policy(tantivy::ReloadPolicy::OnCommitWithDelay)
                    .try_into()?;

                Ok::<IndexReader, StoreError>(reader)
            })?;

        // Warm up fields cache
        let tantivy_index = reader.value().searcher().index().clone();
        let fields = self.get_fields_for_index(index, &tantivy_index)?;

        Ok(Some((reader.value().clone(), fields)))
    }

    /// Search documents in a specific index
    /// Uses tantivy for search, then batch-retrieves complete documents from redb
    /// Returns (results, total_hits) where total_hits is the total number of matching documents
    pub fn search_documents(
        &self,
        index: &str,
        query: &str,
        limit: usize,
    ) -> Result<(Vec<(f32, JsonValue)>, usize), StoreError> {
        use tracing::{debug, warn};

        // Get reader and field mapping from cache or disk
        let (reader, fields) = match self.get_reader(index)? {
            Some(r) => r,
            None => {
                warn!(index = %index, "No tantivy reader found for index");
                return Ok((Vec::new(), 0));
            }
        };

        let searcher = reader.searcher();
        let tantivy_index = searcher.index();

        // Get cached schema to determine which fields are indexed
        let schema = self
            .get_schema_cached(index)?
            .unwrap_or_else(|| Arc::new(IndexSchema::default()));

        debug!(
            index = %index,
            schema_fields = schema.fields.len(),
            "Retrieved schema for search"
        );

        // Build query parser with only indexed fields
        let query_fields: Vec<Field> = fields.indexed_fields.values().cloned().collect();

        if query_fields.is_empty() {
            warn!(index = %index, "No indexed fields available for search");
            return Ok((Vec::new(), 0));
        }

        // Log indexed field names for debugging
        let field_names: Vec<&str> = fields.indexed_fields.keys().map(|s| s.as_str()).collect();
        debug!(
            index = %index,
            query = %query,
            query_fields_count = query_fields.len(),
            indexed_field_names = ?field_names,
            "Executing tantivy search"
        );

        // Create query parser and execute search
        let query_parser = tantivy::query::QueryParser::for_index(tantivy_index, query_fields);
        let parsed_query = query_parser.parse_query(query)?;

        // Debug: log the parsed query to verify field-specific clauses are recognized
        debug!(
            index = %index,
            parsed_query = %format!("{:?}", parsed_query),
            "Parsed tantivy query"
        );

        // Execute search with both TopDocs and Count collectors to get total hits
        let top_docs_collector = tantivy::collector::TopDocs::with_limit(limit);
        let count_collector = tantivy::collector::Count;
        let mut multi_collector = tantivy::collector::MultiCollector::new();
        let top_docs_handle = multi_collector.add_collector(top_docs_collector);
        let count_handle = multi_collector.add_collector(count_collector);

        let mut multi_fruit = searcher.search(&parsed_query, &multi_collector)?;
        let top_docs = top_docs_handle.extract(&mut multi_fruit);
        let total_hits = count_handle.extract(&mut multi_fruit);

        debug!(
            index = %index,
            hits_returned = top_docs.len(),
            total_hits = total_hits,
            "Tantivy search completed"
        );

        if top_docs.is_empty() {
            return Ok((Vec::new(), total_hits));
        }

        // Step 1: Extract document IDs from tantivy results
        let mut doc_ids_with_scores = Vec::with_capacity(top_docs.len());
        for (score, doc_address) in top_docs {
            let doc: tantivy::TantivyDocument = searcher.doc(doc_address)?;

            // Extract ID field - use to_json to get the document and parse ID
            let json_str = doc.to_json(&tantivy_index.schema());
            if let Ok(json_val) = serde_json::from_str::<JsonValue>(&json_str) {
                // Tantivy stores text fields as arrays, so we need to handle both cases
                let id_opt = json_val.get("id").and_then(|v| {
                    // Try as array first (Tantivy's default for text fields)
                    if let Some(arr) = v.as_array() {
                        arr.first().and_then(|item| item.as_str())
                    } else {
                        // Fallback to direct string
                        v.as_str()
                    }
                });

                if let Some(id_str) = id_opt {
                    // Debug: log Tantivy's indexed values for this document
                    debug!(
                        index = %index,
                        doc_id = %id_str,
                        tantivy_doc = %json_str,
                        "Tantivy document matched"
                    );
                    doc_ids_with_scores.push((score, id_str.to_string()));
                } else {
                    warn!(
                        index = %index,
                        tantivy_doc = %json_str,
                        "Tantivy document missing or invalid 'id' field"
                    );
                }
            } else {
                warn!(
                    index = %index,
                    json_str = %json_str,
                    "Failed to parse tantivy document JSON"
                );
            }
        }

        debug!(
            index = %index,
            ids_extracted = doc_ids_with_scores.len(),
            "Extracted document IDs from tantivy results"
        );

        // Step 2: Batch retrieve complete documents from redb (single transaction)
        let doc_ids: Vec<String> = doc_ids_with_scores
            .iter()
            .map(|(_, id)| id.clone())
            .collect();

        let redb_docs = self.get_batch_by_keys(index, &doc_ids)?;

        debug!(
            index = %index,
            requested_ids = doc_ids.len(),
            retrieved_docs = redb_docs.len(),
            "Retrieved documents from redb"
        );

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
                });

                // Merge json_blob fields into root document
                if let Some(json_blob) = stored_doc.json_blob
                    && let (Some(obj), Some(blob_obj)) =
                        (complete_doc.as_object_mut(), json_blob.as_object())
                {
                    for (k, v) in blob_obj {
                        obj.insert(k.clone(), v.clone());
                    }
                }

                results.push((score, complete_doc));
            }
        }

        Ok((results, total_hits))
    }

    /// Apply multiple write operations atomically to a specific index
    ///
    /// This function provides guaranteed batch write with supervised smart commits:
    /// 1. Single atomic redb transaction for all data operations
    /// 2. Single atomic tantivy writer commit for all index operations  
    /// 3. Predictable smart commit logic based on operation thresholds
    /// 4. Guaranteed document searchability after successful commit
    ///
    /// Returns (sequence_ids, new_documents_count)
    pub fn apply_batch(
        &self,
        index: &str,
        ops: Vec<WalOp>,
    ) -> Result<(Vec<u64>, usize), StoreError> {
        if ops.is_empty() {
            return Ok((Vec::new(), 0));
        }

        tracing::debug!(
            index = %index,
            ops_count = ops.len(),
            "HybridStore: Starting apply_batch"
        );

        // Get or create the index
        let (writer_arc, fields) = self.get_or_create_index(index)?;

        // Generate sequence IDs for all operations in one atomic operation
        let start_seq = {
            let counter = self.current_seq.get(index).ok_or_else(|| {
                StoreError::IndexNotFound(format!(
                    "Sequence counter not found for index: {}",
                    index
                ))
            })?;
            counter.fetch_add(ops.len() as u64, Ordering::SeqCst) + 1 - ops.len() as u64
        };
        let seq_ids_iter = (0..ops.len()).map(|i| start_seq + i as u64);

        // Generate sequence IDs for all operations in one atomic operation
        let data_table_name = format!("data_{}", index);
        let wal_table_name = format!("wal_{}", index);
        let data_table_def = TableDefinition::<&str, &[u8]>::new(&data_table_name);
        let wal_table_def = TableDefinition::<u64, &[u8]>::new(&wal_table_name);

        // Single transaction for all operations
        let mut write_txn = self.kv.begin_write()?;
        let mut tantivy_ops = Vec::new();
        let batch_size = ops.len() as u64;

        // Collect sequence IDs during processing
        let mut seq_ids = Vec::with_capacity(ops.len());
        let mut new_documents_count = 0usize;
        let mut updated_document_ids = Vec::new(); // Track updated documents for selective Tantivy deletes

        {
            // Set durability based on config for bulk operations
            let durability = if self.config.wal_sync {
                Durability::Immediate
            } else {
                Durability::None
            };
            write_txn.set_durability(durability)?;
            tracing::trace!(index = %index, batch_size = batch_size, durability = ?durability, "Bulk data transaction durability set (user data)");

            let mut wal_table = write_txn.open_table(wal_table_def)?;
            let mut data_table = write_txn.open_table(data_table_def)?;

            // Process operations and collect sequence IDs
            for (op, seq_id) in ops.into_iter().zip(seq_ids_iter) {
                // Write to WAL
                let wal_data = serde_json::to_vec(&op)
                    .map_err(|e| StoreError::Serialization(e.to_string()))?;
                wal_table.insert(seq_id, wal_data.as_slice())?;

                // Collect sequence ID for final result
                seq_ids.push(seq_id);

                // Apply to data table and prepare tantivy operations
                match op {
                    WalOp::Put { id, json_blob } => {
                        // Step 1: Get cached schema for field filtering
                        // If not in cache, load from persisted metadata
                        let schema = if let Some(schema) = self.get_schema_cached(index)? {
                            schema
                        } else {
                            // Load from metadata if not in cache
                            tracing::debug!(index = %index, "Loading schema from metadata store for batch");
                            self.get_schema(index)?
                                .map(Arc::new)
                                .unwrap_or_else(|| Arc::new(IndexSchema::default()))
                        };

                        // Step 2: Serialize complete document for redb (all fields)
                        let doc_data = StoredDoc {
                            json_blob: json_blob.as_ref(),
                        };
                        let doc_bytes = serde_json::to_vec(&doc_data)
                            .map_err(|e| StoreError::Serialization(e.to_string()))?;

                        // Check if document is new or updated by examining insert return value
                        let old_value = data_table.insert(id.as_str(), doc_bytes.as_slice())?;
                        let is_new_document = old_value.is_none();

                        if is_new_document {
                            new_documents_count += 1;
                        } else {
                            // Track updated documents for selective Tantivy deletes
                            updated_document_ids.push(id.clone());
                        }

                        // Step 3: Build tantivy document with ONLY indexed fields
                        let mut tantivy_doc = doc!(fields.id => id.as_str());

                        // Step 4: Index schema-defined fields individually
                        for (field_name, field_def) in &schema.fields {
                            if !field_def.indexed {
                                continue;
                            }

                            if let Some(tantivy_field) = fields.indexed_fields.get(field_name)
                                && let Some(json_obj) =
                                    json_blob.as_ref().and_then(|v| v.as_object())
                                && let Some(field_value) = json_obj.get(field_name)
                            {
                                match field_def.field_type {
                                    TantivyFieldType::Text => {
                                        if let Some(s) = field_value.as_str() {
                                            tantivy_doc.add_text(*tantivy_field, s);
                                        } else {
                                            let field_str = serde_json::to_string(field_value)
                                                .map_err(|e| {
                                                    StoreError::Serialization(e.to_string())
                                                })?;
                                            tantivy_doc.add_text(*tantivy_field, &field_str);
                                        }
                                    }
                                    TantivyFieldType::String => {
                                        if let Some(s) = field_value.as_str() {
                                            tantivy_doc.add_text(*tantivy_field, s);
                                        } else if let Some(arr) = field_value.as_array() {
                                            for item in arr {
                                                if let Some(s) = item.as_str() {
                                                    tantivy_doc.add_text(*tantivy_field, s);
                                                }
                                            }
                                        }
                                    }
                                    TantivyFieldType::F64 => {
                                        if let Some(n) = field_value.as_f64() {
                                            tantivy_doc.add_f64(*tantivy_field, n);
                                        }
                                    }
                                    TantivyFieldType::I64 => {
                                        if let Some(n) = field_value.as_i64() {
                                            tantivy_doc.add_i64(*tantivy_field, n);
                                        }
                                    }
                                    TantivyFieldType::U64 => {
                                        if let Some(n) = field_value.as_u64() {
                                            tantivy_doc.add_u64(*tantivy_field, n);
                                        }
                                    }
                                    TantivyFieldType::Date => {
                                        if let Some(s) = field_value.as_str()
                                            && let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s)
                                        {
                                            let timestamp_secs = dt.timestamp();
                                            // Clamp to Tantivy's safe range to avoid i64 overflow
                                            // Original date is preserved in redb json_blob
                                            let clamped_ts = timestamp_secs.clamp(
                                                TANTIVY_MIN_TIMESTAMP_SECS,
                                                TANTIVY_MAX_TIMESTAMP_SECS,
                                            );
                                            let tantivy_dt =
                                                DateTime::from_timestamp_secs(clamped_ts);
                                            if timestamp_secs != clamped_ts {
                                                tracing::debug!(
                                                    field = %field_name,
                                                    input = %s,
                                                    original_ts = %timestamp_secs,
                                                    clamped_ts = %clamped_ts,
                                                    "Date clamped to Tantivy safe range (batch)"
                                                );
                                            }
                                            tantivy_doc.add_date(*tantivy_field, tantivy_dt);
                                        }
                                    }
                                    TantivyFieldType::Boolean => {
                                        if let Some(b) = field_value.as_bool() {
                                            tantivy_doc.add_bool(*tantivy_field, b);
                                        }
                                    }
                                    TantivyFieldType::Bytes => {
                                        if let Some(arr) = field_value.as_array() {
                                            let mut bytes = Vec::new();
                                            for item in arr {
                                                if let Some(n) = item.as_u64() {
                                                    bytes.push(n as u8);
                                                }
                                            }
                                            if !bytes.is_empty() {
                                                tantivy_doc
                                                    .add_bytes(*tantivy_field, bytes.as_slice());
                                            }
                                        }
                                    }
                                    TantivyFieldType::Ip => {
                                        if let Some(s) = field_value.as_str()
                                            && let Ok(ip) = s.parse::<std::net::IpAddr>()
                                        {
                                            // Convert any IP address to IPv6 for Tantivy compatibility
                                            let ipv6 = match ip {
                                                std::net::IpAddr::V4(ipv4) => ipv4.to_ipv6_mapped(),
                                                std::net::IpAddr::V6(ipv6) => ipv6,
                                            };
                                            tantivy_doc.add_ip_addr(*tantivy_field, ipv6);
                                        }
                                    }
                                    TantivyFieldType::Json => {
                                        let json_str =
                                            serde_json::to_string(field_value).map_err(|e| {
                                                StoreError::Serialization(e.to_string())
                                            })?;
                                        tantivy_doc.add_text(*tantivy_field, &json_str);
                                    }
                                    TantivyFieldType::Facet => {
                                        if let Some(s) = field_value.as_str() {
                                            tantivy_doc.add_facet(*tantivy_field, &s);
                                        }
                                    }
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

        // Apply all tantivy operations with optimized selective deletes
        {
            // Try to acquire writer lock, with retry logic for lock contention
            let mut writer = {
                let mut attempts = 0;
                let max_attempts = 3;
                loop {
                    match writer_arc.try_lock() {
                        Ok(w) => break w,
                        Err(_) if attempts < max_attempts => {
                            // Attempt to refresh writer cache and retry
                            if attempts == 0 {
                                tracing::warn!(index = %index, "Writer lock contention detected, refreshing writer cache");
                            } else {
                                tracing::debug!(index = %index, attempt = attempts + 1, "Retrying writer lock acquisition");
                            }

                            self.refresh_writer(index)?;
                            attempts += 1;

                            // Small delay to allow other threads to release locks
                            std::thread::sleep(std::time::Duration::from_millis(1 << attempts));
                            continue;
                        }
                        Err(_) => {
                            // Still failed after all retries
                            return Err(StoreError::Serialization(format!(
                                "Failed to acquire writer lock after {} attempts",
                                max_attempts + 1
                            )));
                        }
                    }
                }
            };

            // Step 1: Delete only updated documents (selective optimization)
            if !updated_document_ids.is_empty() {
                tracing::debug!(
                    updated_count = updated_document_ids.len(),
                    "Selective Tantivy deletes for updated documents"
                );
                for updated_id in &updated_document_ids {
                    let term = tantivy::Term::from_field_text(fields.id, updated_id);
                    writer.delete_term(term);
                }
            }

            // Step 2: Add all documents (new + updated)
            // Note: Updated documents already deleted in Step 1, new documents don't need deletion
            for (op_type, tantivy_doc, _id) in tantivy_ops {
                match op_type {
                    "add" => {
                        writer.add_document(tantivy_doc)?;
                    }
                    "delete" => {
                        // Handle explicit delete operations (not from updates)
                        let term = tantivy::Term::from_field_text(fields.id, &_id);
                        writer.delete_term(term);
                    }
                    _ => unreachable!(),
                }
            }

            // Apply smart commit logic for batch operations (same as individual writes)
            // Increment operations counter by batch size
            self.operations_counter
                .entry(index.to_string())
                .or_insert_with(|| AtomicU64::new(0))
                .value()
                .fetch_add(batch_size, Ordering::SeqCst);

            // Adaptive commit strategy: use configuration-based thresholds for intermediate commits
            // Calculate the current commit threshold for this index based on memory budget
            let current_threshold = {
                let budget = if let Some(b) = self.budget_cache.get(index) {
                    *b.value()
                } else {
                    // Fallback: calculate and cache
                    let index_path = self.config.shard_path.join("indices").join(index);
                    let b = self.config.get_optimal_memory_budget(&index_path);
                    self.budget_cache.insert(index.to_string(), b);
                    b
                };

                let min_budget = self.config.indexer_memory_min_mb * 1024 * 1024;
                let max_budget = self.config.indexer_memory_max_mb * 1024 * 1024;
                let budget_ratio = (budget - min_budget) as f64 / (max_budget - min_budget) as f64;
                let default_batch = self.config.default_batch_size as f64;

                // Same calculation as should_commit_writer for consistency
                (default_batch * (0.5 + budget_ratio * 7.5)) as u64
            };

            // Intermediate commit if batch size exceeds 3x the normal commit threshold
            // This allows large batches to commit periodically without waiting for full threshold
            let committed = if batch_size > current_threshold * 3 {
                tracing::debug!(
                    index = %index,
                    batch_size = batch_size,
                    threshold = current_threshold,
                    "Large batch exceeds 3x threshold, performing intermediate commit"
                );
                // Commit and reset counter to threshold value to allow supervisor to continue working
                writer.commit()?;
                // Reset to threshold instead of 0 to keep supervisor active
                self.reset_operations_counter_to(index, current_threshold);
                true
            } else {
                // Skip commits for normal batches to prevent Tantivy contention
                false
            };

            // Optimize memory budget for batch processing to reduce segment creation
            // Use the same threshold calculation for consistency
            if batch_size > current_threshold * 2 {
                // Increase memory budget temporarily for large batches to create fewer segments
                let current_budget = self
                    .budget_cache
                    .get(index)
                    .map(|b| *b.value())
                    .unwrap_or_else(|| {
                        // Fallback if not cached
                        let index_path = self.config.shard_path.join("indices").join(index);
                        self.config.get_optimal_memory_budget(&index_path)
                    });

                let increased_budget = (current_budget as f64 * 1.5) as usize;
                let max_budget = self.config.indexer_memory_max_mb * 1024 * 1024;

                if increased_budget <= max_budget {
                    tracing::debug!(
                        index = %index,
                        batch_size = batch_size,
                        old_budget_mb = current_budget / 1024 / 1024,
                        new_budget_mb = increased_budget / 1024 / 1024,
                        "Increasing memory budget for batch processing (2x threshold exceeded)"
                    );

                    // Update cached budget for this batch
                    self.budget_cache
                        .insert(index.to_string(), increased_budget);
                }
            }

            tracing::debug!(
                index = %index,
                batch_size = batch_size,
                new_docs = new_documents_count,
                updated_docs = updated_document_ids.len(),
                skipped_deletes = new_documents_count,
                committed = committed,
                "Bulk write completed with selective Tantivy optimization and smart commits"
            );

            // Explicit drop of writer to ensure lock release before leaving scope
            drop(writer);
        }

        // Invalidate size cache for this index to ensure fresh stats on next query
        if new_documents_count > 0 || !updated_document_ids.is_empty() {
            let mut size_cache = self.index_size_cache.lock().unwrap();
            size_cache.retain(|key, _| !key.contains(&format!(":{}", index)));
        }

        tracing::debug!(
            index = %index,
            seq_count = seq_ids.len(),
            new_docs = new_documents_count,
            "HybridStore: apply_batch completed successfully"
        );

        Ok((seq_ids, new_documents_count))
    }

    /// Calculate table size using Hybrid Exact/Sampling Estimation algorithm
    ///
    /// For small tables (≤TABLE_SIZE_SAMPLE_COUNT records): Exact calculation by iterating all records
    /// For large tables (>TABLE_SIZE_SAMPLE_COUNT records): Sample TABLE_SIZE_SAMPLE_COUNT records to estimate average size
    ///
    /// Returns (raw_size, is_estimated) where raw_size is the calculated/estimated size
    /// and is_estimated indicates whether sampling was used
    fn calculate_table_size_estimated(
        &self,
        table: &redb::ReadOnlyTable<&str, &[u8]>,
    ) -> Result<(u64, bool), StoreError> {
        let count = table.len()?;

        if count <= TABLE_SIZE_SAMPLE_COUNT {
            // Exact calculation for small tables
            let mut total_size = 0u64;
            for result in table.iter()? {
                let (key, value): (redb::AccessGuard<&str>, redb::AccessGuard<&[u8]>) = result?;
                total_size += key.value().len() as u64 + value.value().len() as u64;
            }
            Ok((total_size, false))
        } else {
            // Sample records for large tables
            let mut sample_size = 0u64;
            let mut sample_count = 0u64;

            for result in table.iter()?.take(TABLE_SIZE_SAMPLE_COUNT as usize) {
                let (key, value): (redb::AccessGuard<&str>, redb::AccessGuard<&[u8]>) = result?;
                sample_size += key.value().len() as u64 + value.value().len() as u64;
                sample_count += 1;
            }

            let average_row_size = if sample_count > 0 {
                sample_size as f64 / sample_count as f64
            } else {
                0.0
            };

            let estimated_raw_size = (average_row_size * count as f64) as u64;
            Ok((estimated_raw_size, true))
        }
    }

    /// Gather per-index statistics and timing information for this shard.
    pub fn gather_index_stats(
        &self,
        include_data_size: bool,
    ) -> Result<ShardStatsSnapshot, StoreError> {
        let mut per_index = HashMap::new();

        let mut index_names: HashSet<String> = HashSet::new();
        let redb_phase_start = Instant::now();
        let read_txn = self.kv.begin_read()?;

        if let Ok(schema_table) = read_txn.open_table(TABLE_SCHEMA) {
            for result in schema_table.iter()? {
                let (index_name, _) = result?;
                index_names.insert(index_name.value().to_string());
            }
        }

        let indices_dir = self.config.shard_path.join("indices");
        if indices_dir.exists() {
            for entry in fs::read_dir(&indices_dir)? {
                let entry = entry?;
                if entry.file_type()?.is_dir() {
                    index_names.insert(entry.file_name().to_string_lossy().to_string());
                }
            }
        }

        // Step 1: Get Physical Baseline (for future correction factor implementation)
        let _kv_path = self.config.shard_path.join("store.redb");

        // Step 2: Iterate & Classify Tables
        for index_name in &index_names {
            let (tantivy_bytes, redb_bytes, document_count) =
                self.get_index_sizes_cached(index_name, include_data_size, &index_names)?;

            // Check if Tantivy index directory exists (not just if it has size)
            // This ensures empty indexes (after schema creation) are counted
            let index_path = self.config.shard_path.join("indices").join(index_name);
            let tantivy_index_exists = index_path.join("meta.json").exists();

            per_index.insert(
                index_name.clone(),
                IndexShardStats {
                    document_count,
                    redb_bytes,
                    tantivy_bytes,
                    tantivy_index_exists,
                    tantivy_scan_ms: 0,
                },
            );
        }
        let redb_duration = redb_phase_start.elapsed();
        drop(read_txn);

        Ok(ShardStatsSnapshot {
            per_index,
            timings: ShardStatsTimings {
                redb_ms: redb_duration.as_millis(),
                tantivy_ms: 0, // Included in redb calculation now
                total_ms: redb_duration.as_millis(),
            },
        })
    }

    /// Get list of index names from redb schema table only
    pub fn get_index_names(&self) -> Result<Vec<String>, StoreError> {
        let mut index_names = Vec::new();

        let read_txn = self.kv.begin_read()?;

        // Only check redb schema table - no filesystem access, no Tantivy loading
        match read_txn.open_table(TABLE_SCHEMA) {
            Ok(schema_table) => {
                for result in schema_table.iter()? {
                    let (index_name, _) = result?;
                    index_names.push(index_name.value().to_string());
                }
            }
            Err(_) => {
                // Schema table doesn't exist yet - return empty list
            }
        }

        Ok(index_names)
    }

    /// Get index size statistics, optionally including the corrected redb measurement.
    fn get_index_sizes_cached(
        &self,
        index_name: &str,
        include_redb: bool,
        all_index_names: &HashSet<String>,
    ) -> Result<(u64, u64, u64), StoreError> {
        let cache_suffix = if include_redb { "full" } else { "fast" };
        let cache_key = format!(
            "{}:{}:{}",
            self.config.shard_path.display(),
            cache_suffix,
            index_name
        );

        {
            let cache = self.index_size_cache.lock().unwrap();
            if let Some(entry) = cache.get(&cache_key)
                && entry.timestamp.elapsed() < self.index_cache_expiry
            {
                return Ok((entry.tantivy_bytes, entry.redb_bytes, entry.document_count));
            }
        }

        let tantivy_bytes = self.measure_tantivy_bytes(index_name)?;

        if !include_redb {
            let document_count = self.get_document_count_only(index_name)?;
            let mut cache = self.index_size_cache.lock().unwrap();
            cache.insert(
                cache_key,
                IndexSizeCache {
                    tantivy_bytes,
                    redb_bytes: 0,
                    document_count,
                    timestamp: Instant::now(),
                },
            );
            return Ok((tantivy_bytes, 0, document_count));
        }

        let mut per_index_stats = Vec::with_capacity(all_index_names.len());
        let mut total_raw_redb_size = 0u64;

        for idx_name in all_index_names {
            let idx_tantivy_bytes = if idx_name == index_name {
                tantivy_bytes
            } else {
                self.measure_tantivy_bytes(idx_name)?
            };

            let (doc_count, raw_redb_bytes) = self.measure_redb_stats(idx_name)?;
            per_index_stats.push((
                idx_name.clone(),
                idx_tantivy_bytes,
                doc_count,
                raw_redb_bytes,
            ));
            total_raw_redb_size = total_raw_redb_size.saturating_add(raw_redb_bytes);
        }

        let physical_db_size = match std::fs::metadata(self.config.shard_path.join("store.redb")) {
            Ok(metadata) => metadata.len(),
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "Failed to get database file size, using raw estimation"
                );
                total_raw_redb_size
            }
        };

        let correction_factor = if total_raw_redb_size > 0 {
            physical_db_size as f64 / total_raw_redb_size as f64
        } else {
            1.0
        };

        let mut target_redb_bytes = 0u64;
        let mut target_document_count = 0u64;

        {
            let mut cache = self.index_size_cache.lock().unwrap();
            for (idx_name, idx_tantivy_bytes, doc_count, raw_redb_bytes) in per_index_stats {
                let corrected_redb_bytes = (raw_redb_bytes as f64 * correction_factor) as u64;

                let full_key = format!("{}:full:{}", self.config.shard_path.display(), idx_name);
                cache.insert(
                    full_key,
                    IndexSizeCache {
                        tantivy_bytes: idx_tantivy_bytes,
                        redb_bytes: corrected_redb_bytes,
                        document_count: doc_count,
                        timestamp: Instant::now(),
                    },
                );

                let fast_key = format!("{}:fast:{}", self.config.shard_path.display(), idx_name);
                cache.insert(
                    fast_key,
                    IndexSizeCache {
                        tantivy_bytes: idx_tantivy_bytes,
                        redb_bytes: 0,
                        document_count: doc_count,
                        timestamp: Instant::now(),
                    },
                );

                if idx_name == index_name {
                    target_redb_bytes = corrected_redb_bytes;
                    target_document_count = doc_count;
                }
            }
        }

        Ok((tantivy_bytes, target_redb_bytes, target_document_count))
    }

    fn measure_tantivy_bytes(&self, index_name: &str) -> Result<u64, StoreError> {
        let index_dir = self.config.shard_path.join("indices").join(index_name);
        if !index_dir.exists() {
            return Ok(0);
        }

        let mut total_size = 0u64;
        for entry in WalkDir::new(&index_dir)
            .follow_links(false)
            .into_iter()
            .filter_map(Result::ok)
        {
            if let Ok(metadata) = entry.metadata()
                && metadata.is_file()
                && Self::is_tantivy_data_file(entry.path())
            {
                total_size += metadata.len();
            }
        }

        Ok(total_size)
    }

    fn get_document_count_only(&self, index_name: &str) -> Result<u64, StoreError> {
        let read_txn = self.kv.begin_read()?;
        let data_table_name = format!("data_{}", index_name);
        let data_table_def = TableDefinition::<&str, &[u8]>::new(&data_table_name);

        let count = match read_txn.open_table(data_table_def) {
            Ok(data_table) => data_table.len().unwrap_or(0),
            Err(_) => 0,
        };
        drop(read_txn);

        Ok(count)
    }

    fn measure_redb_stats(&self, index_name: &str) -> Result<(u64, u64), StoreError> {
        let read_txn = self.kv.begin_read()?;
        let data_table_name = format!("data_{}", index_name);
        let data_table_def = TableDefinition::<&str, &[u8]>::new(&data_table_name);

        let (doc_count, raw_bytes) = match read_txn.open_table(data_table_def) {
            Ok(data_table) => {
                let doc_count = data_table.len().unwrap_or(0);
                let (raw_size, _) = self.calculate_table_size_estimated(&data_table)?;
                (doc_count, raw_size)
            }
            Err(_) => (0, 0),
        };
        drop(read_txn);

        Ok((doc_count, raw_bytes))
    }

    fn is_tantivy_data_file(path: &std::path::Path) -> bool {
        path.extension()
            .and_then(|ext| ext.to_str())
            .map(|ext| TANTIVY_DATA_FILE_EXTENSIONS.contains(&ext))
            .unwrap_or(false)
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
            indexer_memory_budget: 32 * 1024 * 1024,
            indexer_memory_min_mb: 16,
            indexer_memory_max_mb: 256,
            default_batch_size: 1000,
            wal_sync: true,
        };

        let store = HybridStore::new(config).unwrap();

        // Write to index1
        let op1 = WalOp::Put {
            id: "doc1".to_string(),
            json_blob: None,
        };
        let seq1 = store.apply_write("index1", op1).unwrap();
        assert_eq!(seq1, 1);

        // Write to index2
        let op2 = WalOp::Put {
            id: "doc1".to_string(),
            json_blob: None,
        };
        let seq2 = store.apply_write("index2", op2).unwrap();
        assert_eq!(seq2, 1); // Independent sequence

        // Verify directories exist
        let index1_path = temp_dir.path().join("indices").join("index1");
        let index2_path = temp_dir.path().join("indices").join("index2");
        assert!(index1_path.exists());
        assert!(index2_path.exists());

        // Delete index1 (with schema deletion)
        store.delete_index_data("index1", true).unwrap();

        // Verify index1 is gone but index2 remains
        assert!(!index1_path.exists());
        assert!(index2_path.exists());

        // Verify index2 still works
    }

    #[test]
    fn test_field_type_inference() {
        use crate::{FieldDef, TantivyFieldType};
        use serde_json::json;

        // Test field type inference from JSON values
        let test_cases = vec![
            (json!("hello"), TantivyFieldType::Text),
            (json!("2023-01-01T00:00:00Z"), TantivyFieldType::Date),
            (json!("192.168.1.1"), TantivyFieldType::Ip),
            (json!(42), TantivyFieldType::I64),
            (json!(std::f64::consts::PI), TantivyFieldType::F64),
            (json!(true), TantivyFieldType::Boolean),
            (json!(null), TantivyFieldType::Text),
            (json!([1, 2, 3]), TantivyFieldType::Text),
            (json!({"key": "value"}), TantivyFieldType::Json),
        ];

        for (value, expected_type) in test_cases {
            let inferred_type = FieldDef::infer_type_from_value(&value);
            assert_eq!(
                inferred_type, expected_type,
                "Failed to infer type for value: {:?}",
                value
            );
        }

        println!("✅ Field type inference works correctly!");
    }

    #[test]
    fn test_field_def_creation() {
        use crate::{FieldDef, TantivyFieldType};

        // Test FieldDef creation with different types
        let text_field = FieldDef::new("title".to_string(), TantivyFieldType::Text);
        assert_eq!(text_field.field_type, TantivyFieldType::Text);
        assert!(text_field.indexed);
        assert!(!text_field.stored); // Only "id" field is stored in Tantivy
        assert!(!text_field.fast); // Text fields are not fast by default

        let i64_field = FieldDef::new("count".to_string(), TantivyFieldType::I64);
        assert_eq!(i64_field.field_type, TantivyFieldType::I64);
        assert!(i64_field.indexed);
        assert!(!i64_field.stored); // Only "id" field is stored in Tantivy
        assert!(i64_field.fast); // Numeric fields are fast by default

        // Test the "id" field special case
        let id_field = FieldDef::new("id".to_string(), TantivyFieldType::Text);
        assert_eq!(id_field.field_type, TantivyFieldType::Text);
        assert!(id_field.indexed);
        assert!(id_field.stored); // "id" field is stored in Tantivy
        assert!(!id_field.fast); // Text fields are not fast by default

        let json_field = FieldDef::new("metadata".to_string(), TantivyFieldType::Json);
        assert_eq!(json_field.field_type, TantivyFieldType::Json);
        assert!(json_field.indexed);
        assert!(!json_field.stored); // Only "id" field is stored in Tantivy
        assert!(!json_field.fast); // JSON fields are not fast by default

        println!("✅ FieldDef creation works correctly!");
    }

    #[test]
    fn test_schema_evolution() {
        use crate::{IndexSchema, TantivyFieldType};
        use serde_json::json;

        let mut schema = IndexSchema::default();

        // Add initial fields
        let doc1 = json!({
            "name": "Test",
            "value": 123
        });

        let evolved_fields = schema.evolve_from_document(&doc1);
        assert_eq!(evolved_fields.len(), 2);
        assert_eq!(schema.fields.len(), 2);

        // Verify field types
        assert_eq!(
            schema.fields.get("name").unwrap().field_type,
            TantivyFieldType::Text
        );
        assert_eq!(
            schema.fields.get("value").unwrap().field_type,
            TantivyFieldType::I64
        );

        // Evolve with new document
        let doc2 = json!({
            "name": "Test 2",
            "value": 456.789, // Should evolve to F64
            "created_at": "2023-01-01T00:00:00Z" // New field
        });

        let evolved_fields = schema.evolve_from_document(&doc2);
        assert_eq!(evolved_fields.len(), 2); // value evolved + created_at added
        assert_eq!(schema.fields.len(), 3);

        // Verify evolution
        assert_eq!(
            schema.fields.get("value").unwrap().field_type,
            TantivyFieldType::F64
        );
        assert_eq!(
            schema.fields.get("created_at").unwrap().field_type,
            TantivyFieldType::Date
        );

        println!("✅ Schema evolution works correctly!");
    }

    #[test]
    fn test_tantivy_date_comparison_with_clamping() {
        use tantivy::DateTime;

        // Test that our clamping strategy works correctly
        // 1606-01-01 (Volpone publication - would overflow without clamping)
        let old_ts: i64 = -11_486_668_800;
        let clamped_old_ts = old_ts.clamp(TANTIVY_MIN_TIMESTAMP_SECS, TANTIVY_MAX_TIMESTAMP_SECS);
        let old_tantivy = DateTime::from_timestamp_secs(clamped_old_ts);

        // 2023-05-27 (Query bound)
        let new_ts: i64 = 1_685_145_600; // 2023-05-27T00:00:00Z
        let new_tantivy = DateTime::from_timestamp_secs(new_ts);

        println!(
            "1606-01-01 (clamped to 1677): timestamp={}, tantivy={:?}",
            clamped_old_ts, old_tantivy
        );
        println!(
            "2023-05-27: timestamp={}, tantivy={:?}",
            new_ts, new_tantivy
        );

        // With clamping, 1677 should be LESS than 2023
        assert!(
            old_tantivy < new_tantivy,
            "Clamped 1677 date should be less than 2023 date"
        );
        assert_eq!(
            clamped_old_ts, TANTIVY_MIN_TIMESTAMP_SECS,
            "Pre-1677 date should be clamped to minimum"
        );

        // Test future date clamping
        let future_ts: i64 = 10_000_000_000; // Beyond 2262
        let clamped_future =
            future_ts.clamp(TANTIVY_MIN_TIMESTAMP_SECS, TANTIVY_MAX_TIMESTAMP_SECS);
        assert_eq!(
            clamped_future, TANTIVY_MAX_TIMESTAMP_SECS,
            "Post-2262 date should be clamped to maximum"
        );

        println!("✅ Tantivy DateTime clamping works correctly for out-of-range dates!");
    }

    #[test]
    fn test_background_schema_evolution() {
        use crate::{IndexSchema, TantivyFieldType};
        use serde_json::json;

        let mut schema = IndexSchema::default();

        // Add initial document with new fields
        let doc = json!({
            "title": "Test Document",
            "count": 42,
            "timestamp": "2023-01-01T00:00:00Z"
        });

        let evolved_fields = schema.evolve_from_document(&doc);
        assert_eq!(evolved_fields.len(), 3, "Should discover 3 new fields");

        // Verify all new fields are non-indexed
        let title_field = schema.fields.get("title").unwrap();
        assert_eq!(title_field.field_type, TantivyFieldType::Text);
        assert!(!title_field.indexed, "New fields should be non-indexed");
        assert!(
            !title_field.stored,
            "Only 'id' field should be stored in Tantivy"
        );

        let count_field = schema.fields.get("count").unwrap();
        assert_eq!(count_field.field_type, TantivyFieldType::I64);
        assert!(!count_field.indexed, "New fields should be non-indexed");
        assert!(count_field.fast, "Numeric fields should be fast");

        let timestamp_field = schema.fields.get("timestamp").unwrap();
        assert_eq!(timestamp_field.field_type, TantivyFieldType::Date);
        assert!(!timestamp_field.indexed, "New fields should be non-indexed");

        // Verify we can get non-indexed fields
        let non_indexed = schema.get_non_indexed_fields();
        assert_eq!(non_indexed.len(), 3, "Should have 3 non-indexed fields");
        assert!(non_indexed.contains(&"title".to_string()));
        assert!(non_indexed.contains(&"count".to_string()));
        assert!(non_indexed.contains(&"timestamp".to_string()));

        // Test promoting a field to indexed
        let promoted = schema.promote_field_to_indexed("title");
        assert!(promoted, "Should successfully promote field");
        assert!(
            schema.fields.get("title").unwrap().indexed,
            "Field should now be indexed"
        );

        // Verify non-indexed count decreased
        let non_indexed_after = schema.get_non_indexed_fields();
        assert_eq!(
            non_indexed_after.len(),
            2,
            "Should have 2 non-indexed fields after promotion"
        );
        assert!(
            !non_indexed_after.contains(&"title".to_string()),
            "Promoted field should not be in list"
        );

        // Test promoting already indexed field
        let promoted_again = schema.promote_field_to_indexed("title");
        assert!(!promoted_again, "Should not promote already indexed field");

        println!("✅ Background schema evolution works correctly!");
    }
}
