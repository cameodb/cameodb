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
use tantivy::schema::{Document, Field, Schema, STORED, STRING, TEXT};
use tantivy::{doc, Index, IndexReader, IndexWriter};
use thiserror::Error;

/// Schema metadata table: maps index names to their schema definitions.
const TABLE_SCHEMA: TableDefinition<&str, &[u8]> = TableDefinition::new("schema");

/// Configuration for the multi-tenant hybrid storage engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// The root folder for this shard's data files.
    pub shard_path: PathBuf,
    /// Memory budget for each tantivy IndexWriter in bytes.
    pub writer_memory_budget: usize,
    /// Whether to call fsync() on every redb commit.
    pub wal_sync: bool,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            shard_path: PathBuf::from("/tmp/cameodb_default_shard"),
            writer_memory_budget: 32 * 1024 * 1024, // 32MB
            wal_sync: true,
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

/// Internal schema field mappings for Tantivy.
#[derive(Debug, Clone)]
struct SchemaFields {
    id: Field,
    body: Field,
    json_blob: Field,
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
            config,
        })
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

        // Create writer
        let writer = tantivy_index.writer(self.config.writer_memory_budget)?;
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
            match &op {
                WalOp::Put {
                    id,
                    body,
                    json_blob,
                } => {
                    let doc_data = serde_json::json!({
                        "body": body,
                        "json_blob": json_blob
                    });
                    let doc_bytes = serde_json::to_vec(&doc_data)
                        .map_err(|e| StoreError::Serialization(e.to_string()))?;

                    let mut data_table = write_txn.open_table(data_table_def)?;
                    data_table.insert(id.as_str(), doc_bytes.as_slice())?;

                    // Add to tantivy index
                    let mut tantivy_doc =
                        doc!(fields.id => id.as_str(), fields.body => body.as_str());
                    if let Some(json_data) = json_blob {
                        let json_str = serde_json::to_string(json_data)
                            .map_err(|e| StoreError::Serialization(e.to_string()))?;
                        tantivy_doc.add_text(fields.json_blob, &json_str);
                    }

                    let writer = writer_arc.lock().unwrap();
                    writer.add_document(tantivy_doc)?;
                }
                WalOp::Delete { id } => {
                    let mut data_table = write_txn.open_table(data_table_def)?;
                    data_table.remove(id.as_str())?;

                    // Delete from tantivy index
                    let term = tantivy::Term::from_field_text(fields.id, id);
                    let writer = writer_arc.lock().unwrap();
                    writer.delete_term(term);
                }
            }
        }

        write_txn.commit()?;

        // Commit tantivy changes
        {
            let mut writer = writer_arc.lock().unwrap();
            writer.commit()?;
        }

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
        let data_table_name = format!("data_{}", index);
        let data_table_def = TableDefinition::<&str, &[u8]>::new(&data_table_name);

        let read_txn = self.kv.begin_read()?;

        match read_txn.open_table(data_table_def) {
            Ok(data_table) => match data_table.get(key)? {
                Some(value) => Ok(Some(value.value().to_vec())),
                None => Ok(None),
            },
            Err(_) => Ok(None), // Table doesn't exist (index was deleted)
        }
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

    /// Search documents in a specific index
    pub fn search_documents(
        &self,
        index: &str,
        query: &str,
        limit: usize,
    ) -> Result<Vec<(f32, JsonValue)>, StoreError> {
        // Get or create the index to ensure it exists
        let (_, fields) = self.get_or_create_index(index)?;

        // Get the Tantivy index path
        let index_path = self.config.shard_path.join("indices").join(index);

        if !index_path.exists() {
            return Ok(Vec::new()); // No results if index doesn't exist
        }

        // Open the Tantivy index
        let tantivy_index = Index::open_in_dir(&index_path)?;
        let reader = tantivy_index.reader()?;
        let searcher = reader.searcher();

        // Create query parser for the body field
        let query_parser =
            tantivy::query::QueryParser::for_index(&tantivy_index, vec![fields.body]);
        let parsed_query = query_parser.parse_query(query)?;

        // Execute search
        let top_docs = searcher.search(
            &parsed_query,
            &tantivy::collector::TopDocs::with_limit(limit),
        )?;

        // Convert results to (score, JsonValue) format
        let mut results = Vec::new();
        for (score, doc_address) in top_docs {
            let doc: tantivy::TantivyDocument = searcher.doc(doc_address)?;

            // Convert tantivy document to JSON
            let json_str = doc.to_json(&tantivy_index.schema());
            let json_value: JsonValue = serde_json::from_str(&json_str)
                .map_err(|e| StoreError::Serialization(e.to_string()))?;

            results.push((score, json_value));
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

        {
            let mut wal_table = write_txn.open_table(wal_table_def)?;
            let mut data_table = write_txn.open_table(data_table_def)?;

            for (op, seq_id) in ops.iter().zip(seq_ids.iter()) {
                // Write to WAL
                let wal_data =
                    serde_json::to_vec(op).map_err(|e| StoreError::Serialization(e.to_string()))?;
                wal_table.insert(*seq_id, wal_data.as_slice())?;

                // Apply to data table and prepare tantivy operations
                match op {
                    WalOp::Put {
                        id,
                        body,
                        json_blob,
                    } => {
                        let doc_data = serde_json::json!({
                            "body": body,
                            "json_blob": json_blob
                        });
                        let doc_bytes = serde_json::to_vec(&doc_data)
                            .map_err(|e| StoreError::Serialization(e.to_string()))?;

                        data_table.insert(id.as_str(), doc_bytes.as_slice())?;

                        // Prepare tantivy document
                        let mut tantivy_doc =
                            doc!(fields.id => id.as_str(), fields.body => body.as_str());
                        if let Some(json_data) = json_blob {
                            let json_str = serde_json::to_string(json_data)
                                .map_err(|e| StoreError::Serialization(e.to_string()))?;
                            tantivy_doc.add_text(fields.json_blob, &json_str);
                        }
                        tantivy_ops.push(("add", tantivy_doc, id.clone()));
                    }
                    WalOp::Delete { id } => {
                        data_table.remove(id.as_str())?;
                        tantivy_ops.push(("delete", doc!(), id.clone()));
                    }
                }
            }
        }

        write_txn.commit()?;

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
            writer.commit()?;
        }

        Ok(seq_ids)
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
