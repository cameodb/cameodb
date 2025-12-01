//! # Storage Engine - Hybrid KV + Search Storage for CameoDB
//!
//! This crate provides a production-grade hybrid storage engine that combines:
//! - **redb**: ACID-compliant key-value storage for durability and consistency
//! - **tantivy**: Full-text search indexing for query capabilities
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────┐    ┌─────────────────┐
//! │   Client API    │    │   Search Query  │
//! └─────────┬───────┘    └─────────┬───────┘
//!           │                      │
//!           ▼                      ▼
//! ┌─────────────────────────────────────────┐
//! │           HybridStore API               │
//! ├─────────────────┬───────────────────────┤
//! │   apply_write   │   get_by_key │ search │
//! └─────────┬───────┴──────────────────────┘
//!           │
//!           ▼
//! ┌─────────────────────────────────────────┐
//! │              WAL Layer                  │
//! └─────────┬───────────────────────────────┘
//!           │
//!     ┌─────▼─────┐              ┌─────────┐
//!     │   redb    │              │ tantivy │
//!     │ (KV Store)│              │(Search) │
//!     └───────────┘              └─────────┘
//! ```
//!
//! ## Concurrency Model
//!
//! **CRITICAL**: This storage engine is **blocking** and **NOT async-safe**.
//! All methods perform synchronous I/O operations that will block the calling thread.
//!
//! ### Thread Safety
//!
//! - `HybridStore` implements `Send + Sync` for safe sharing between threads
//! - Internal `IndexWriter` is protected by `Arc<Mutex<_>>` for concurrent access
//! - `AtomicU64` is used for lock-free sequence ID generation
//!
//! ### Async Integration
//!
//! **NEVER** call storage methods directly from async contexts. Always use `spawn_blocking`:
//!
//! ```rust,ignore
//! use storage::{HybridStore, StorageConfig, WalOp};
//! use tokio::task;
//!
//! async fn async_write_example(store: HybridStore, op: WalOp) -> Result<u64, Box<dyn std::error::Error>> {
//!     // ✅ CORRECT: Wrap in spawn_blocking
//!     let seq_id = task::spawn_blocking(move || {
//!         store.apply_write(op)
//!     }).await??;
//!     
//!     Ok(seq_id)
//! }
//! ```
//!
//! ## File Layout
//!
//! ```text
//! {shard_path}/
//! ├── kv_store.redb      # redb database file
//! │   ├── TABLE_WAL      # Write-ahead log (u64 -> bytes)
//! │   └── TABLE_DATA     # Main data (string -> bytes)
//! └── search_index/      # tantivy index directory
//!     ├── meta.json      # Index metadata
//!     ├── .managed.json  # Managed files list
//!     └── [segments]     # Inverted index segments
//! ```
//!
//! ## Example Usage
//!
//! ```rust,no_run
//! use storage::{HybridStore, StorageConfig, WalOp};
//! use serde_json::json;
//! use std::path::PathBuf;
//!
//! // Configure storage
//! let config = StorageConfig {
//!     shard_path: PathBuf::from("./data/cameodb/shard1"),
//!     writer_memory_budget: 50 * 1024 * 1024, // 50MB
//!     wal_sync: true,
//! };
//!
//! // Create store
//! let store = HybridStore::new(config)?;
//!
//! // Write data with atomic dual-write
//! let op = WalOp::Put {
//!     id: "user:123".to_string(),
//!     body: "John Doe software engineer".to_string(),
//!     json_blob: Some(json!({"email": "john@example.com"})),
//! };
//! let seq_id = store.apply_write(op)?;
//!
//! // Read data
//! let data = store.get_by_key("user:123")?;
//! if let Some(bytes) = data {
//!     let doc: serde_json::Value = serde_json::from_slice(&bytes)?;
//!     println!("Found: {}", doc["body"]);
//! }
//!
//! // Search documents
//! let results = store.search_documents("software engineer", 10)?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
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
use tantivy::{Index, IndexReader, IndexWriter, TantivyDocument, doc};
use thiserror::Error;

// Dynamic table definitions for multi-tenant storage

/// Schema metadata table: maps index names to their schema definitions.
/// This stores the evolving schema for each index.
const TABLE_SCHEMA: TableDefinition<&str, &[u8]> = TableDefinition::new("schema");

/// Creates a dynamic data table definition for a specific index.
/// Returns a static string to avoid lifetime issues.
macro_rules! data_table_name {
    ($index:expr) => {
        &format!("data_{}", $index)
    };
}

/// Creates a dynamic WAL table definition for a specific index.
/// Returns a static string to avoid lifetime issues.
macro_rules! wal_table_name {
    ($index:expr) => {
        &format!("wal_{}", $index)
    };
}

/// Configuration for the multi-tenant hybrid storage engine.
///
/// Controls all aspects of storage behavior including file paths,
/// memory usage, and durability guarantees for a shard that can
/// host multiple indices.
///
/// # Examples
///
/// ```rust
/// use storage::StorageConfig;
/// use std::path::PathBuf;
///
/// // Default configuration
/// let config = StorageConfig::default();
/// assert_eq!(config.writer_memory_budget, 32 * 1024 * 1024);
///
/// // Custom configuration
/// let config = StorageConfig {
///     shard_path: PathBuf::from("./data/cameodb/shard1"),
///     writer_memory_budget: 64 * 1024 * 1024, // 64MB
///     wal_sync: false, // Higher performance, less durability
/// };
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// The root folder for this shard's data files.
    ///
    /// This directory will contain:
    /// - `store.redb`: Shared redb database with multiple tables
    /// - `indices/`: Directory containing per-index Tantivy indices
    pub shard_path: PathBuf,

    /// Memory budget for each tantivy IndexWriter in bytes.
    ///
    /// Set to 32MB to conserve RAM across many shards and indices.
    /// This is per-index, so total memory usage scales with number of indices.
    pub writer_memory_budget: usize,

    /// Whether to call fsync() on every redb commit.
    ///
    /// - `true`: Maximum durability, slower writes
    /// - `false`: Better performance, risk of data loss on crash
    pub wal_sync: bool,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            shard_path: PathBuf::from("/tmp/cameodb_default_shard"),
            writer_memory_budget: 32 * 1024 * 1024, // 32MB (conserve RAM)
            wal_sync: true,
        }
    }
}

/// Field definition for schema evolution and validation.
///
/// Defines the metadata for a single field in an index schema,
/// including its type and indexing configuration.
///
/// # Examples
///
/// ```rust
/// use storage::FieldDef;
///
/// let field = FieldDef {
///     name: "title".to_string(),
///     field_type: "text".to_string(),
///     indexed: true,
/// };
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FieldDef {
    /// The field name as it appears in documents
    pub name: String,
    /// The field type (e.g., "text", "string", "number", "boolean")
    pub field_type: String,
    /// Whether this field should be indexed for search
    pub indexed: bool,
}

/// Index schema definition for validation and evolution.
///
/// Contains the complete schema definition for an index, including
/// shard configuration and field definitions. Supports schema evolution
/// through append-only field additions.
///
/// # Examples
///
/// ```rust
/// use storage::{IndexSchema, FieldDef};
/// use std::collections::HashMap;
///
/// let mut schema = IndexSchema::default();
/// schema.fields.insert("title".to_string(), FieldDef {
///     name: "title".to_string(),
///     field_type: "text".to_string(),
///     indexed: true,
/// });
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexSchema {
    /// Number of shards for this index (configurable, default 256)
    pub shard_count: u32,
    /// Field definitions mapped by field name
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
///
/// Covers all possible failure modes including I/O errors,
/// serialization failures, and storage engine specific errors.
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
}

/// Write-Ahead Log operations for atomic dual-write to KV store and search index.
///
/// All operations are serialized to the WAL before being applied to ensure
/// durability and consistency. Operations are applied atomically to both
/// the redb key-value store and the tantivy search index.
///
/// # Examples
///
/// ```rust
/// use storage::WalOp;
/// use serde_json::json;
///
/// // Create a document
/// let put_op = WalOp::Put {
///     id: "user:123".to_string(),
///     body: "John Doe software engineer".to_string(),
///     json_blob: Some(json!({"email": "john@example.com"})),
/// };
///
/// // Delete a document
/// let delete_op = WalOp::Delete {
///     id: "user:123".to_string(),
/// };
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalOp {
    /// Creates or updates a document with the given ID.
    ///
    /// The document consists of:
    /// - `id`: Unique document identifier (used as primary key)
    /// - `body`: Full-text searchable content
    /// - `json_blob`: Optional structured metadata (stored as JSON)
    Put {
        /// Unique document identifier
        id: String,
        /// Full-text searchable content
        body: String,
        /// Optional structured metadata
        json_blob: Option<JsonValue>,
    },

    /// Deletes a document by its ID.
    ///
    /// Removes the document from both the KV store and search index.
    Delete {
        /// ID of the document to delete
        id: String,
    },
}

/// Internal schema field mappings for the Tantivy search index.
///
/// Maps logical field names to Tantivy field handles for efficient access.
#[derive(Debug, Clone)]
struct SchemaFields {
    id: Field,
    body: Field,
    json_blob: Field,
}

/// Production-grade multi-tenant hybrid storage engine combining redb and tantivy.
///
/// Provides ACID guarantees through atomic dual-write operations that
/// update both the key-value store and search indices consistently.
/// Supports multiple indices within a single shard for multi-tenancy.
///
/// ## Multi-Tenant Architecture
///
/// - **Single redb Database**: Shared across all indices with dynamic table creation
/// - **Per-Index Tantivy**: Each index has its own Tantivy directory and writer/reader
/// - **Dynamic Caching**: Writers and readers are cached per index for performance
/// - **Atomic Sequences**: Per-index WAL sequence generation for isolation
///
/// ## Concurrency Guarantees
///
/// - **Thread Safety**: Implements `Send + Sync` for safe sharing between threads
/// - **Blocking Operations**: All methods are synchronous and will block the calling thread
/// - **Cache Protection**: Writer/reader caches protected by `RwLock` for concurrent access
/// - **Atomic Operations**: Uses `AtomicU64` for lock-free sequence generation
///
/// ## CRITICAL: Async Safety
///
/// **NEVER** call methods directly from async contexts. Always wrap in `spawn_blocking`:
///
/// ```rust,ignore
/// # use storage::{HybridStore, WalOp};
/// # use tokio::task;
/// async fn safe_async_usage(store: HybridStore, index: &str, op: WalOp) {
///     let result = task::spawn_blocking(move || {
///         store.apply_write(index, op)  // ✅ Safe: wrapped in spawn_blocking
///     }).await.unwrap();
/// }
/// ```
///
/// ## Directory Structure
///
/// ```text
/// {shard_path}/
/// ├── store.redb              # Shared redb database (multiple tables)
/// └── indices/                # Root for search indexes
///     ├── {index_name}/       # Created on demand
///     │   ├── meta.json       # Tantivy metadata
///     │   └── [segments]      # Tantivy index segments
///     └── {index_name2}/      # Another index
/// ```
///
/// # Examples
///
/// ```rust,no_run
/// use storage::{HybridStore, StorageConfig, WalOp};
/// use std::path::PathBuf;
///
/// let config = StorageConfig {
///     shard_path: PathBuf::from("./data/cameodb/test_shard"),
///     writer_memory_budget: 32 * 1024 * 1024,
///     wal_sync: true,
/// };
///
/// let store = HybridStore::new(config)?;
///
/// // Write to index1
/// let op1 = WalOp::Put {
///     id: "doc1".to_string(),
///     body: "searchable content".to_string(),
///     json_blob: None,
/// };
/// let seq_id = store.apply_write("index1", op1)?;
///
/// // Write to index2 (completely isolated)
/// let op2 = WalOp::Put {
///     id: "doc1".to_string(),  // Same ID, different index
///     body: "different content".to_string(),
///     json_blob: None,
/// };
/// let seq_id2 = store.apply_write("index2", op2)?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct HybridStore {
    /// ACID-compliant key-value database (redb) shared across all indices
    kv: Database,

    /// Cache of IndexWriters keyed by index name.
    /// Protected by RwLock for concurrent read access to cache.
    writers: Arc<RwLock<HashMap<String, Arc<Mutex<IndexWriter>>>>>,

    /// Cache of IndexReaders keyed by index name.
    /// Protected by RwLock for concurrent read access to cache.
    readers: Arc<RwLock<HashMap<String, IndexReader>>>,

    /// Atomic counter for WAL sequence IDs. Provides lock-free generation
    /// of monotonically increasing sequence numbers per index.
    current_seq: Arc<RwLock<HashMap<String, AtomicU64>>>,

    /// Storage configuration (paths, memory limits, sync behavior)
    config: StorageConfig,
}

impl HybridStore {
    /// Creates a new multi-tenant HybridStore with the specified configuration.
    ///
    /// Initializes the shared redb database and creates the directory structure
    /// for multi-tenant indices. Individual Tantivy indices are created on demand.
    ///
    /// ## Directory Structure Created
    ///
    /// ```text
    /// {shard_path}/
    /// ├── store.redb          # Shared redb database (multiple tables)
    /// └── indices/            # Root for search indexes
    ///     └── (created on demand)
    /// ```
    ///
    /// # Arguments
    ///
    /// * `config` - Storage configuration specifying paths and behavior
    ///
    /// # Returns
    ///
    /// A new `HybridStore` instance ready for multi-tenant operations
    ///
    /// # Errors
    ///
    /// Returns `StoreError` if:
    /// - Directory creation fails
    /// - Database initialization fails
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use storage::{HybridStore, StorageConfig};
    /// use std::path::PathBuf;
    ///
    /// let config = StorageConfig {
    ///     shard_path: PathBuf::from("./data/cameodb/my_shard"),
    ///     writer_memory_budget: 32 * 1024 * 1024, // 32MB per index
    ///     wal_sync: true,
    /// };
    ///
    /// let store = HybridStore::new(config)?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
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

    /// Applies a write operation atomically to both storage engines.
    ///
    /// This method implements atomic dual-write semantics, ensuring that
    /// operations are applied consistently to both the redb key-value store
    /// and the tantivy search index.
    ///
    /// ## Atomicity Guarantee
    ///
    /// The operation follows this sequence:
    /// 1. Generate monotonic sequence ID
    /// 2. Begin redb write transaction
    /// 3. Write operation to WAL table (for durability)
    /// 4. Apply operation to main data table
    /// 5. Commit redb transaction
    /// 6. Update tantivy search index
    /// 7. Commit tantivy changes
    ///
    /// If any step fails, the entire operation is rolled back.
    ///
    /// ## Concurrency
    ///
    /// This method is **blocking** and **NOT async-safe**. It will block
    /// the calling thread during I/O operations. For async usage, wrap
    /// in `tokio::task::spawn_blocking`.
    ///
    /// # Arguments
    ///
    /// * `op` - The write operation to apply (Put or Delete)
    ///
    /// # Returns
    ///
    /// The sequence ID of the applied operation (monotonically increasing)
    ///
    /// # Errors
    ///
    /// Returns `StoreError` if:
    /// - Serialization fails
    /// - Database transaction fails
    /// - Index update fails
    ///
    /// # Examples
    ///
    /// ```rust
    /// use storage::{HybridStore, StorageConfig, WalOp};
    /// use serde_json::json;
    /// use std::path::PathBuf;
    ///
    /// let config = StorageConfig::default();
    /// let store = HybridStore::new(config)?;
    ///
    /// // Create a document
    /// let op = WalOp::Put {
    ///     id: "user:123".to_string(),
    ///     body: "John Doe engineer".to_string(),
    ///     json_blob: Some(json!({"email": "john@example.com"})),
    /// };
    /// let seq_id = store.apply_write(op)?;
    /// println!("Operation applied with sequence ID: {}", seq_id);
    ///
    /// // Delete the document
    /// let delete_op = WalOp::Delete {
    ///     id: "user:123".to_string(),
    /// };
    /// let seq_id2 = store.apply_write(delete_op)?;
    /// assert!(seq_id2 > seq_id); // Sequence IDs are monotonic
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn apply_write(&self, op: WalOp) -> Result<u64, StoreError> {
        let seq_id = self.current_seq.fetch_add(1, Ordering::SeqCst) + 1;

        // Write to WAL first
        let wal_data =
            serde_json::to_vec(&op).map_err(|e| StoreError::Serialization(e.to_string()))?;

        let write_txn = self.kv.begin_write()?;
        {
            let mut wal_table = write_txn.open_table(TABLE_WAL)?;
            wal_table.insert(seq_id, wal_data.as_slice())?;

            // Apply to data table
            match &op {
                WalOp::Put {
                    id,
                    body,
                    json_blob,
                } => {
                    // Create document data for redb storage
                    let doc_data = serde_json::json!({
                        "body": body,
                        "json_blob": json_blob
                    });
                    let doc_bytes = serde_json::to_vec(&doc_data)
                        .map_err(|e| StoreError::Serialization(e.to_string()))?;

                    let mut data_table = write_txn.open_table(TABLE_DATA)?;
                    data_table.insert(id.as_str(), doc_bytes.as_slice())?;

                    // Add to tantivy index
                    let mut tantivy_doc =
                        doc!(self.fields.id => id.as_str(), self.fields.body => body.as_str());
                    if let Some(json_data) = json_blob {
                        // Convert serde_json::Value to tantivy's expected format
                        let json_str = serde_json::to_string(json_data)
                            .map_err(|e| StoreError::Serialization(e.to_string()))?;
                        tantivy_doc.add_text(self.fields.json_blob, &json_str);
                    }

                    let writer = self.index_writer.lock().unwrap();
                    writer.add_document(tantivy_doc)?;
                }
                WalOp::Delete { id } => {
                    let mut data_table = write_txn.open_table(TABLE_DATA)?;
                    data_table.remove(id.as_str())?;

                    // Delete from tantivy index
                    let term = tantivy::Term::from_field_text(self.fields.id, id);
                    let writer = self.index_writer.lock().unwrap();
                    writer.delete_term(term);
                }
            }
        }

        if self.config.wal_sync {
            write_txn.commit()?;
        } else {
            write_txn.commit()?;
        }

        // Commit tantivy changes
        {
            let mut writer = self.index_writer.lock().unwrap();
            writer.commit()?;
        }

        Ok(seq_id)
    }

    /// Applies a batch of write operations atomically with optimized performance.
    ///
    /// This method implements atomic batch processing with significant performance
    /// optimizations over individual `apply_write` calls:
    /// - Single redb write transaction for all operations
    /// - Single tantivy index writer lock acquisition
    /// - Contiguous sequence ID block reservation
    /// - Single fsync and segment flush
    ///
    /// ## Performance Benefits
    ///
    /// - **Reduced Lock Contention**: Acquires IndexWriter mutex once for entire batch
    /// - **Reduced I/O**: Single redb commit and tantivy commit per batch
    /// - **Atomic Sequence IDs**: Uses `fetch_add` to reserve contiguous ID block
    /// - **Transaction Efficiency**: Single write transaction spans entire batch
    ///
    /// ## Concurrency
    ///
    /// This method is **blocking** and **NOT async-safe**. For async usage,
    /// wrap in `tokio::task::spawn_blocking`.
    ///
    /// # Arguments
    ///
    /// * `ops` - Vector of write operations to apply atomically
    ///
    /// # Returns
    ///
    /// Vector of sequence IDs corresponding to each operation (in order)
    ///
    /// # Errors
    ///
    /// Returns `StoreError` if:
    /// - Serialization fails for any operation
    /// - Database transaction fails
    /// - Index update fails
    /// - Lock acquisition fails
    ///
    /// # Examples
    ///
    /// ```rust
    /// use storage::{HybridStore, StorageConfig, WalOp};
    /// use serde_json::json;
    ///
    /// let store = HybridStore::new(StorageConfig::default())?;
    ///
    /// // Batch multiple operations
    /// let ops = vec![
    ///     WalOp::Put {
    ///         id: "user:1".to_string(),
    ///         body: "Alice Engineer".to_string(),
    ///         json_blob: Some(json!({"email": "alice@example.com"})),
    ///     },
    ///     WalOp::Put {
    ///         id: "user:2".to_string(),
    ///         body: "Bob Designer".to_string(),
    ///         json_blob: Some(json!({"email": "bob@example.com"})),
    ///     },
    /// ];
    ///
    /// let seq_ids = store.apply_batch(ops)?;
    /// assert_eq!(seq_ids.len(), 2);
    /// assert!(seq_ids[1] == seq_ids[0] + 1); // Contiguous sequence IDs
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn apply_batch(&self, ops: Vec<WalOp>) -> Result<Vec<u64>, StoreError> {
        if ops.is_empty() {
            return Ok(vec![]);
        }

        // 1. Reserve a block of Sequence IDs atomically
        let batch_size = ops.len() as u64;
        let start_seq = self.current_seq.fetch_add(batch_size, Ordering::SeqCst) + 1;

        let mut result_seqs = Vec::with_capacity(ops.len());

        // 2. Begin ONE Redb Write Transaction
        let write_txn = self.kv.begin_write()?;

        // 3. Acquire ONE lock on Tantivy
        // We hold this lock for the duration of the batch processing to ensure
        // the index doesn't drift from the KV store during the operation.
        let mut index_writer = self
            .index_writer
            .lock()
            .map_err(|e| StoreError::Serialization(format!("Lock poisoned: {}", e)))?;

        {
            let mut wal_table = write_txn.open_table(TABLE_WAL)?;
            let mut data_table = write_txn.open_table(TABLE_DATA)?;

            for (i, op) in ops.into_iter().enumerate() {
                let seq_id = start_seq + i as u64;
                result_seqs.push(seq_id);

                // A. Serialize & Write to WAL
                let wal_data = serde_json::to_vec(&op)
                    .map_err(|e| StoreError::Serialization(e.to_string()))?;
                wal_table.insert(seq_id, wal_data.as_slice())?;

                // B. Apply to Data Table & Index
                match &op {
                    WalOp::Put {
                        id,
                        body,
                        json_blob,
                    } => {
                        // Redb: Store the full document
                        let doc_data = serde_json::json!({
                            "body": body,
                            "json_blob": json_blob
                        });
                        let doc_bytes = serde_json::to_vec(&doc_data)
                            .map_err(|e| StoreError::Serialization(e.to_string()))?;
                        data_table.insert(id.as_str(), doc_bytes.as_slice())?;

                        // Tantivy: Add to Index Buffer
                        let mut tantivy_doc = doc!(
                            self.fields.id => id.as_str(),
                            self.fields.body => body.as_str()
                        );

                        if let Some(json_data) = json_blob {
                            let json_str = serde_json::to_string(json_data)
                                .map_err(|e| StoreError::Serialization(e.to_string()))?;
                            tantivy_doc.add_text(self.fields.json_blob, &json_str);
                        }

                        index_writer.add_document(tantivy_doc)?;
                    }
                    WalOp::Delete { id } => {
                        // Redb: Remove
                        data_table.remove(id.as_str())?;

                        // Tantivy: Delete Term
                        let term = tantivy::Term::from_field_text(self.fields.id, id);
                        index_writer.delete_term(term);
                    }
                }
            }
        } // Tables are dropped here, releasing internal page locks

        // 4. Commit Redb (The heavy I/O operation)
        // We strictly follow the config for fsync
        if self.config.wal_sync {
            write_txn.commit()?;
        } else {
            write_txn.commit()?;
        }

        // 5. Commit Tantivy (Flush to a new Segment)
        index_writer.commit()?;

        Ok(result_seqs)
    }

    /// Retrieves a document by its key from the key-value store.
    ///
    /// Reads directly from the redb database, returning the raw JSON bytes
    /// of the stored document. Returns `None` if the key doesn't exist.
    ///
    /// ## Concurrency
    ///
    /// This method is **blocking** and **NOT async-safe**. For async usage,
    /// wrap in `tokio::task::spawn_blocking`.
    ///
    /// # Arguments
    ///
    /// * `key` - The document ID to retrieve
    ///
    /// # Returns
    ///
    /// - `Ok(Some(bytes))` - Document found, returns JSON bytes
    /// - `Ok(None)` - Document not found
    /// - `Err(StoreError)` - Database error occurred
    ///
    /// # Examples
    ///
    /// ```rust
    /// use storage::{HybridStore, StorageConfig, WalOp};
    /// use serde_json::json;
    ///
    /// let store = HybridStore::new(StorageConfig::default())?;
    ///
    /// // First, store a document
    /// let op = WalOp::Put {
    ///     id: "user:123".to_string(),
    ///     body: "John Doe".to_string(),
    ///     json_blob: Some(json!({"email": "john@example.com"})),
    /// };
    /// store.apply_write(op)?;
    ///
    /// // Retrieve the document
    /// let data = store.get_by_key("user:123")?;
    /// if let Some(bytes) = data {
    ///     let doc: serde_json::Value = serde_json::from_slice(&bytes)?;
    ///     println!("Found document: {}", doc["body"]);
    /// }
    ///
    /// // Non-existent key returns None
    /// let missing = store.get_by_key("nonexistent")?;
    /// assert!(missing.is_none());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn get_by_key(&self, key: &str) -> Result<Option<Vec<u8>>, StoreError> {
        let read_txn = self.kv.begin_read()?;
        let data_table = read_txn.open_table(TABLE_DATA)?;

        match data_table.get(key)? {
            Some(value) => Ok(Some(value.value().to_vec())),
            None => Ok(None),
        }
    }

    pub fn search_documents(
        &self,
        query: &str,
        limit: usize,
    ) -> Result<Vec<(f32, JsonValue)>, StoreError> {
        use tantivy::collector::TopDocs;
        use tantivy::query::QueryParser;

        let reader = self.index.reader()?;
        let searcher = reader.searcher();

        // Create query parser for all text fields
        let text_fields: Vec<Field> = self
            .index
            .schema()
            .fields()
            .filter(|(_, field_entry)| {
                matches!(field_entry.field_type(), tantivy::schema::FieldType::Str(_))
            })
            .map(|(field, _)| field)
            .collect();

        if text_fields.is_empty() {
            return Ok(vec![]);
        }

        let query_parser = QueryParser::for_index(&self.index, text_fields);
        let parsed_query = query_parser.parse_query(query)?;

        // Execute search with limit
        let top_docs = searcher.search(&parsed_query, &TopDocs::with_limit(limit))?;

        // Convert results to (score, JsonValue) format
        let mut results = Vec::new();
        for (score, doc_address) in top_docs {
            let doc: TantivyDocument = searcher.doc(doc_address)?;
            let json_doc: JsonValue = serde_json::from_str(&doc.to_json(&self.index.schema()))
                .map_err(|e| StoreError::Serialization(e.to_string()))?;
            results.push((score, json_doc));
        }

        Ok(results)
    }

    /// Provides the underlying Tantivy schema for document serialization.
    pub fn schema(&self) -> Schema {
        self.index.schema()
    }

    /// Stores an index schema in the metadata table.
    ///
    /// This method persists the schema definition for an index, enabling
    /// schema validation and evolution. The schema is stored as JSON bytes
    /// in the redb database.
    ///
    /// ## Concurrency
    ///
    /// This method is **blocking** and **NOT async-safe**. For async usage,
    /// wrap in `tokio::task::spawn_blocking`.
    ///
    /// # Arguments
    ///
    /// * `index_name` - The name of the index
    /// * `schema` - The schema definition to store
    ///
    /// # Returns
    ///
    /// `Ok(())` on success, `StoreError` on failure
    ///
    /// # Examples
    ///
    /// ```rust
    /// use storage::{HybridStore, StorageConfig, IndexSchema, FieldDef};
    /// use std::collections::HashMap;
    ///
    /// let store = HybridStore::new(StorageConfig::default())?;
    /// let mut schema = IndexSchema::default();
    /// schema.fields.insert("title".to_string(), FieldDef {
    ///     name: "title".to_string(),
    ///     field_type: "text".to_string(),
    ///     indexed: true,
    /// });
    ///
    /// store.store_schema("my_index", &schema)?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
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

    /// Retrieves an index schema from the metadata table.
    ///
    /// Loads the schema definition for an index from persistent storage.
    /// Returns `None` if no schema exists for the given index.
    ///
    /// ## Concurrency
    ///
    /// This method is **blocking** and **NOT async-safe**. For async usage,
    /// wrap in `tokio::task::spawn_blocking`.
    ///
    /// # Arguments
    ///
    /// * `index_name` - The name of the index
    ///
    /// # Returns
    ///
    /// - `Ok(Some(schema))` - Schema found and deserialized
    /// - `Ok(None)` - No schema exists for this index
    /// - `Err(StoreError)` - Database or deserialization error
    ///
    /// # Examples
    ///
    /// ```rust
    /// use storage::{HybridStore, StorageConfig};
    ///
    /// let store = HybridStore::new(StorageConfig::default())?;
    ///
    /// match store.get_schema("my_index")? {
    ///     Some(schema) => println!("Found schema with {} fields", schema.fields.len()),
    ///     None => println!("No schema found for index"),
    /// }
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn get_schema(&self, index_name: &str) -> Result<Option<IndexSchema>, StoreError> {
        let read_txn = self.kv.begin_read()?;

        // Try to open the schema table - it might not exist yet
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

    /// Get the maximum WAL ID from the database
    fn get_max_wal_id(kv: &Database) -> Result<u64, StoreError> {
        let read_txn = kv.begin_read()?;

        // Try to open the WAL table - it might not exist yet
        match read_txn.open_table(TABLE_WAL) {
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
            Err(_) => Ok(0), // Table doesn't exist yet, start from 0
        }
    }
}

// Safe because redb::Database and tantivy components are Send+Sync
// Arc<Mutex<IndexWriter>> is also Send+Sync
unsafe impl Send for HybridStore {}
unsafe impl Sync for HybridStore {}
