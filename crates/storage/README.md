# Storage Engine - Multi-Tenant Hybrid KV + Search Storage

The `storage` crate provides a production-grade multi-tenant hybrid storage engine built with **Rust 2024 Edition** that combines ACID-compliant key-value storage with full-text search capabilities. Each index operates as an isolated tenant with independent data tables, search indices, and sequence counters.

## Architecture: The "Hybrid" Concept

CameoDB's storage engine combines two complementary storage systems:

### redb: Durable Key-Value Store
- **Purpose**: Primary data storage with ACID guarantees
- **Strengths**: Fast random access, transactions, durability
- **Use Cases**: Document storage, metadata, write-ahead logging

### tantivy: Full-Text Search Index
- **Purpose**: Fast text search and query capabilities  
- **Strengths**: Inverted indexes, relevance scoring, complex queries
- **Use Cases**: Full-text search, filtering, analytics
- **Storage Strategy**: Index-only (except `id` field) - complete data retrieved from redb

### Multi-Tenant Architecture

CameoDB's storage engine supports multiple isolated indices within a single storage instance:

| Feature | Implementation | Benefit |
|---------|---------------|---------|
| **Data Isolation** | `data_{index}` and `wal_{index}` tables per index | Complete tenant separation |
| **Search Isolation** | `indices/{index_name}/` directories per index | Independent search performance |
| **Sequence Independence** | Per-index AtomicU64 counters | Parallel write scaling |
| **Cache Efficiency** | Per-index IndexWriter and IndexReader caches | Optimized memory usage |

### Why Both Storage Engines?

| Operation | redb | tantivy | Hybrid Advantage |
|-----------|------|---------|------------------|
| Get by ID | ✅ Fast | ❌ Slow | Best of both |
| Full-text search | ❌ No support | ✅ Fast | Complete solution |
| ACID transactions | ✅ Yes | ❌ No | Consistency guaranteed |
| Range queries | ✅ B-tree | ✅ Inverted index | Multiple access patterns |

**The hybrid approach provides:**
- **Fast point lookups** via redb's B-tree structure
- **Rich search capabilities** via tantivy's inverted indexes
- **ACID guarantees** for all operations
- **Single API** for both access patterns

## Atomic Write Operations

All write operations follow a strict sequence to ensure atomicity across both storage engines:

### Write Sequence (apply_write)

```
1. Generate Sequence ID
   ├─ AtomicU64::fetch_add(1) → seq_id
   └─ Ensures monotonic ordering

2. Begin redb Transaction  
   ├─ Database::begin_write() → write_txn
   └─ Provides ACID isolation

3. Write to WAL Table
   ├─ Serialize WalOp → JSON bytes
   ├─ TABLE_WAL.insert(seq_id, bytes)
   └─ Ensures durability (operation logged before applied)

4. Write to Data Table
   ├─ For Put: TABLE_DATA.insert(id, complete_document_json)
   ├─ For Delete: TABLE_DATA.remove(id)
   └─ Apply the actual operation (redb stores ALL fields)

5. Update tantivy Index (In-Memory)
   ├─ For Put: IndexWriter.add_document(id_only_doc)
   ├─ For Delete: IndexWriter.delete_term(id_term)
   └─ Update search index buffer (Tantivy stores ONLY indexed fields, id always stored)

6. Commit redb Transaction
   ├─ write_txn.commit()
   ├─ Optional fsync() based on wal_sync config
   └─ Durability checkpoint

7. Commit tantivy Changes
   ├─ IndexWriter.commit()
   └─ Make search changes visible
```

### Storage Optimization: Index-Only Tantivy Strategy

CameoDB implements an optimized storage strategy to minimize disk usage:

**Tantivy Storage:**
- **Only stores**: `id` field (always) + indexed fields (without STORED flag)
- **Purpose**: Pure search indexing and document ID retrieval
- **Benefit**: Minimal index size, faster search performance

**redb Storage:**
- **Stores**: Complete JSON documents with ALL fields
- **Purpose**: Authoritative data source and document retrieval
- **Benefit**: Full document fidelity, single source of truth

**Search & Retrieval Flow:**
1. **Search**: Query Tantivy → get matching document IDs + scores
2. **Retrieve**: Batch fetch complete documents from redb using IDs
3. **Return**: Full JSON documents with all original fields

**This split-storage strategy provides:**
- Smaller Tantivy indices (no duplicated stored fields)
- Faster search performance due to leaner segments
- Full-fidelity documents retrieved from redb every time
- A single source of truth for API responses

### Date Field Range Limits

Tantivy stores dates as nanosecond timestamps (`DateTime::from_timestamp_secs`). The conversion uses `i64` math, so the supported range is finite:

| Limit | RFC3339 Timestamp | Unix Seconds |
|-------|-------------------|--------------|
| Minimum | `1677-09-21T00:12:44Z` | `-9_223_372_036` |
| Maximum | `2262-04-11T23:47:16Z` | `9_223_372_036` |

To keep indexing safe we clamp any out-of-range date **only when writing to Tantivy**. The original RFC3339 string remains untouched in redb's `json_blob`, so API responses still return the real publication (or ingestion) date. Practical implications:

- Historical documents (e.g., books from the 1500s) will retrieve their true date but are indexed at the minimum bound (`1677-09-21`), so they won't satisfy queries that look for newer publication dates.
- Far-future timestamps are clamped to the maximum bound.
- Modern datasets (roughly 1900–present) fall inside the supported interval and incur no special handling.

If you rely on date range queries, ensure that emitted timestamps stay within Tantivy's supported window or account for this clamping behavior when interpreting search results.

```rust
let timestamp_secs = dt.timestamp();
let clamped = timestamp_secs
    .clamp(TANTIVY_MIN_TIMESTAMP_SECS, TANTIVY_MAX_TIMESTAMP_SECS);
let tantivy_dt = DateTime::from_timestamp_secs(clamped);
if timestamp_secs != clamped {
    tracing::debug!(
        input = %s,
        original_ts = %timestamp_secs,
        clamped_ts = %clamped,
        "Date clamped to Tantivy safe range"
    );
}
tantivy_doc.add_date(field, tantivy_dt);
```

### Atomicity Guarantees

- **All-or-Nothing**: If any step fails, entire operation rolls back
- **Consistency**: Both stores always reflect the same logical state
- **Isolation**: Concurrent operations don't interfere
- **Durability**: WAL ensures operations survive crashes

### Error Handling

```rust
pub fn apply_write(&self, index: &str, op: WalOp) -> Result<u64, StoreError> {
    // Get or create the index and its Tantivy writer
    let (writer_arc, fields) = self.get_or_create_index(index)?;

    // Per-index sequence ID
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

    // WAL + data tables are per-index
    let data_table_name = format!("data_{}", index);
    let wal_table_name = format!("wal_{}", index);
    let data_table_def = TableDefinition::<&str, &[u8]>::new(&data_table_name);
    let wal_table_def = TableDefinition::<u64, &[u8]>::new(&wal_table_name);

    // WAL write (durability) + data update in a single redb transaction
    let wal_data = serde_json::to_vec(&op)
        .map_err(|e| StoreError::Serialization(e.to_string()))?;

    let write_txn = self.kv.begin_write()?;
    {
        let mut wal_table = write_txn.open_table(wal_table_def)?;
        wal_table.insert(seq_id, wal_data.as_slice())?;

        match &op {
            WalOp::Put { id, body, json_blob } => {
                let doc_data = serde_json::json!({
                    "body": body,
                    "json_blob": json_blob
                });
                let doc_bytes = serde_json::to_vec(&doc_data)
                    .map_err(|e| StoreError::Serialization(e.to_string()))?;

                let mut data_table = write_txn.open_table(data_table_def)?;
                data_table.insert(id.as_str(), doc_bytes.as_slice())?;

                // Prepare Tantivy document
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

                // Delete from Tantivy index
                let term = tantivy::Term::from_field_text(fields.id, id);
                let writer = writer_arc.lock().unwrap();
                writer.delete_term(term);
            }
        }
    }

    // Commit redb transaction (WAL + data)
    write_txn.commit()?;

    // Supervised Smart Commits: may or may not commit Tantivy immediately
    self.increment_operations(index);
    self.maybe_commit_writer(index)?;

    Ok(seq_id)
}
```

**Recovery Strategy**: If tantivy update fails after redb commit, the WAL entry remains and can be replayed during recovery.

## File Layout

The storage engine creates a well-organized directory structure:

```
{shard_path}/
├── store.redb                      # redb database file (shared across all indices)
│   ├── data_index_1                # Data table for index_1 (string → bytes)
│   ├── wal_index_1                 # WAL table for index_1 (u64 → bytes)
│   ├── data_index_2                # Data table for index_2
│   ├── wal_index_2                 # WAL table for index_2
│   └── schema                      # Schema metadata table (index name → bytes)
└── indices/                        # tantivy index directories (per index)
    ├── index_1/
    │   ├── meta.json               # Index metadata and schema
    │   ├── .managed.json           # Managed files list
    │   ├── seg_0*                  # Index segments (fieldnorm, idx, pos, store, term, ...)
    │   └── ...
    └── index_2/
        └── ...
```

### File Descriptions

#### redb Files
- **`store.redb`**: Single-file database containing all KV data and metadata
  - Per-index tables: `data_{index}`, `wal_{index}`
  - Shared `schema` table for index schemas
  - Uses B+ trees for efficient range queries
  - Includes transaction log for ACID compliance
  - Supports concurrent readers with MVCC

#### tantivy Directories
- **`indices/{index_name}/`**: Tantivy index for a specific tenant
  - **`meta.json`**: Index configuration and schema definition
  - **`.managed.json`**: Tracks active index segments
  - **`seg_*`**: Index segments containing inverted indexes
    - **`.idx`**: Inverted index (term → document list)
    - **`.pos`**: Term positions for phrase queries
    - **`.store`**: Stored field values for retrieval
    - **`.term`**: Term dictionary for efficient lookups
    - **`.fieldnorm`**: Field normalization factors for scoring

### Storage Characteristics

| Aspect | redb | tantivy |
|--------|------|---------|
| **File Format** | Single file | Multiple segments |
| **Concurrency** | MVCC readers | Immutable segments |
| **Durability** | Per-index WAL tables + fsync | Atomic segment creation |
| **Compression** | Built-in | Per-segment compression |
| **Size Growth** | Append-only pages | Segment merging |

### Index Metadata and Statistics

Beyond raw storage, the engine tracks per-index metadata and stats:

- **Schema metadata**
  - Stored in a shared `schema` table inside `store.redb`
  - API:
    - `store.store_schema(index_name, &IndexSchema)`
    - `store.get_schema(index_name) -> Option<IndexSchema>`

- **Index listing**
  - Discover all known indices with statistics:
    - `store.get_index_names() -> Vec<String>`
    - `store.gather_index_stats(include_data_size) -> ShardStatsSnapshot`

- **Caching**
  - Schema cache: In-memory caching of parsed schemas for fast access
  - Directory size cache: Cached Tantivy index directory sizes with TTL
  - Read cache: Optional bounded cache for frequently accessed documents

## Usage Examples

### Basic Operations

```rust
use storage::{HybridStore, StorageConfig, WalOp};
use serde_json::json;
use std::path::PathBuf;

// Configure storage with performance optimizations
let config = StorageConfig {
    shard_path: PathBuf::from("./data/shard1"),
    indexer_memory_budget: 64 * 1024 * 1024, // 64MB default (increased from 32MB)
    indexer_memory_min_mb: 32,               // 32MB minimum (increased from 16MB)
    indexer_memory_max_mb: 512,              // 512MB maximum (increased from 256MB)
    default_batch_size: 1000,               // Supervised Smart Commits threshold
    wal_sync: true,                         // Maximum durability
};

// Initialize store
let store = HybridStore::new(config)?;

// Multi-tenant write operations
let put_op = WalOp::Put {
    id: "user:123".to_string(),
    body: "John Doe software engineer at Acme Corp".to_string(),
    json_blob: Some(json!({
        "email": "john@acme.com",
        "department": "engineering",
        "hire_date": "2024-01-15"
    })),
};

// Write to specific index (tenant)
let seq_id = store.apply_write("employees", put_op)?;
println!("Document stored with sequence ID: {}", seq_id);

// Read operations from specific index
let data = store.get_by_key("employees", "user:123")?;
if let Some(bytes) = data {
    let doc: serde_json::Value = serde_json::from_slice(&bytes)?;
    println!("Found: {}", doc["body"]);
    println!("Email: {}", doc["json_blob"]["email"]);
}

// Search within specific index
let results = store.search_documents("employees", "software engineer", 10)?;
for (score, doc) in results {
    println!("Score: {:.3}, ID: {}", score, doc["id"]);
}

// Delete all data for an index
store.delete_index_data("employees")?;
println!("Index 'employees' deleted successfully");
```

### Async Integration (Critical)

**⚠️ NEVER call storage methods directly from async contexts!**

```rust
use tokio::task;

// ❌ WRONG: Will block async runtime
async fn wrong_usage(store: HybridStore, index: String, op: WalOp) {
    let result = store.apply_write(&index, op); // Blocks entire async runtime!
}

// ✅ CORRECT: Use spawn_blocking
async fn correct_usage(store: HybridStore, index: String, op: WalOp) -> Result<u64, StoreError> {
    let result = task::spawn_blocking(move || {
        store.apply_write(&index, op) // Safe: runs on blocking thread pool
    }).await.map_err(|_| StoreError::Serialization("Task panicked".to_string()))??;
    
    Ok(result)
}
```

### Multi-Tenant Batch Operations

```rust
use storage::{HybridStore, WalOp};

async fn batch_insert(
    store: HybridStore, 
    index: &str,
    documents: Vec<(String, String)>
) -> Result<Vec<u64>, StoreError> {
    let ops: Vec<WalOp> = documents.into_iter().map(|(id, content)| {
        WalOp::Put {
            id,
            body: content,
            json_blob: None,
        }
    }).collect();
    
    // Atomic batch processing with Supervised Smart Commits
    let sequence_ids = tokio::task::spawn_blocking({
        let store = store.clone();
        let index = index.to_string();
        move || store.apply_batch(&index, ops)
    }).await??;
    
    Ok(sequence_ids)
}

// Alternative: Individual operations (less efficient)
async fn individual_insert(
    store: HybridStore, 
    index: &str,
    documents: Vec<(String, String)>
) -> Result<Vec<u64>, StoreError> {
    let mut sequence_ids = Vec::new();
    
    for (id, content) in documents {
        let op = WalOp::Put {
            id,
            body: content,
            json_blob: None,
        };
        
        let seq_id = tokio::task::spawn_blocking({
            let store = store.clone();
            let index = index.to_string();
            move || store.apply_write(&index, op)
        }).await??;
        
        sequence_ids.push(seq_id);
    }
    
    Ok(sequence_ids)
}
```

### Configuration Tuning

```rust
use storage::StorageConfig;
use std::path::PathBuf;

// High-performance configuration with Supervised Smart Commits
let high_perf_config = StorageConfig {
    shard_path: PathBuf::from("/fast-ssd/shard1"),
    indexer_memory_budget: 64 * 1024 * 1024,  // 64MB default
    indexer_memory_min_mb: 32,               // 32MB minimum (increased from 16MB)
    indexer_memory_max_mb: 512,              // 512MB maximum (increased from 256MB)
    default_batch_size: 2000,                // Higher commit threshold
    wal_sync: false,                         // Skip fsync for speed
};

// High-durability configuration with frequent commits
let high_durability_config = StorageConfig {
    shard_path: PathBuf::from("/redundant-storage/shard1"),
    indexer_memory_budget: 32 * 1024 * 1024,  // 32MB default
    indexer_memory_min_mb: 32,               // 32MB minimum (increased from 16MB)
    indexer_memory_max_mb: 128,              // 128MB maximum
    default_batch_size: 500,                 // Lower commit threshold
    wal_sync: true,                          // Always fsync
};

// Memory-constrained configuration
let low_memory_config = StorageConfig {
    shard_path: PathBuf::from("./shard1"),
    indexer_memory_budget: 32 * 1024 * 1024,  // 32MB default (increased from 16MB)
    indexer_memory_min_mb: 32,                // 32MB minimum (increased from 8MB)
    indexer_memory_max_mb: 64,               // 64MB maximum (increased from 32MB)
    default_batch_size: 250,                 // Very low commit threshold
    wal_sync: true,
};
```

## Supervised Smart Commits

CameoDB implements sophisticated **Supervised Smart Commits** that provide both high performance and strong durability guarantees for Tantivy search indices.

### Architecture Overview

The system combines two complementary commit mechanisms:

#### 1. Smart Commits (Immediate Performance)
- **Trigger**: Operation count threshold reached (adaptive based on memory budget)
- **Behavior**: Immediate Tantivy commit during write operations
- **Purpose**: Memory management and performance optimization
- **Adaptive Threshold**: 500-8000 operations based on index memory budget (32MB-512MB)

#### 2. Supervised Eventual Commits (Durability Guarantee)
- **Trigger**: 5 seconds of inactivity after last write
- **Behavior**: Background commit via async supervisor tasks
- **Purpose**: Data durability guarantee for low-volume write patterns
- **Self-cleanup**: Supervisors automatically remove themselves after successful commits

### Implementation Details

#### Smart Commit Algorithm
```rust
// Adaptive threshold calculation:
let budget_ratio = (budget - min_budget) as f64 / (max_budget - min_budget) as f64;
let base_ops = (default_batch_size * (0.5 + budget_ratio * 7.5)) as u64;
// Result: 500 ops (32MB) -> 8000 ops (512MB)
```

#### Supervisor Mechanism
```rust
// Per-index async supervisor:
tokio::spawn(async move {
    loop {
        tokio::select! {
            Some(()) = rx.recv() => continue;  // Write activity - reset timer
            _ = tokio::time::sleep(Duration::from_secs(5)) => {
                // Timeout - eventual commit
                store_inner.commit_index(&index_inner);
                break; // Self-cleanup
            }
        }
    }
});
```

### Behavior Scenarios

#### High-Volume Workloads
```
Write 1 → Signal supervisor (timer: 5s)
Write 2 → Signal supervisor (timer: 5s)
... (smart commit triggers at default_batch_size ops)
Commit → Supervisor self-cleanup
```

#### Low-Volume Workloads
```
Write 1 → Signal supervisor (timer: 5s)
... (no more writes)
5s pass → Timeout → Eventual commit
Commit → Supervisor self-cleanup
```

### Benefits

#### Performance Benefits
- **High-Volume**: Smart commits reduce commit overhead during bursts
- **Low-Volume**: Eventual commits prevent unnecessary commits
- **Memory Efficiency**: Adaptive thresholds based on index size
- **Multi-Tenant**: Per-index supervision and commit strategies

#### Durability Guarantees
- **Maximum 5-second window**: Data loss window bounded by supervisor timeout
- **Large Batches (≥ default_batch_size)**: **Immediate commit** regardless of threshold
- **Consistency**: All writes eventually become searchable
- **Crash Safety**: Supervisor state is lightweight, easily recreated

#### Operational Simplicity
- **Zero Configuration**: Works out of the box with sensible defaults
- **Self-Healing**: Automatic retry and cleanup mechanisms
- **Resource Efficient**: Supervisors only exist for active indices
- **Async/Sync Safe**: Proper isolation between async supervision and sync storage operations

### Configuration

```toml
[search]
indexer_memory_min_mb = 32      # Minimum memory budget
indexer_memory_max_mb = 512     # Maximum memory budget  
default_batch_size = 1000       # Base smart commit threshold
# Supervisor timeout is fixed at 5 seconds (configurable in future versions)
```

### Environment Variables

You can also configure the batch size via environment variable:

```bash
# Set default batch size to 500 operations
export CAMEODB_STORAGE_DEFAULT_BATCH_SIZE=500

# Set default batch size to 2000 operations for high-throughput workloads
export CAMEODB_STORAGE_DEFAULT_BATCH_SIZE=2000
```

**Priority Order**: Environment variables override TOML configuration values.

This supervised strategy ensures optimal performance across all write patterns while maintaining strong data durability guarantees.

## Performance Characteristics

### Write Performance (Rust 2024 Optimized)
- **Single Operations**: ~0.5-3ms per operation (depends on fsync setting)
- **Batch Operations**: ~0.05-0.5ms per operation in batch (significant improvement)
- **Throughput**: ~2000-15000 ops/sec individual, ~10000-100000 ops/sec batched
- **Supervised Smart Commits**: Adaptive commit frequency based on memory budget and batch size, with eventual durability guarantees via async supervision
- **Modern Error Handling**: Rust 2024 `std::io::Error::other()` for better performance
- **Bottlenecks**: Disk I/O (fsync), tantivy indexing, mutex contention

### Multi-Tenant Performance
- **Index Isolation**: No performance interference between indices
- **Memory Scaling**: Dynamic memory budgets based on index size (32MB-512MB)
- **Commit Optimization**: Per-index Supervised Smart Commits with configurable thresholds and eventual durability guarantees
- **Cache Efficiency**: Independent IndexWriter/IndexReader caches per index

### Read Performance  
- **Point Queries**: ~0.1ms (redb B-tree lookup)
- **Range Queries**: ~1-10ms (depends on range size)
- **Search Queries**: ~10-100ms (depends on index size and query complexity)
- **Phrase Queries**: Supported via Tantivy's positional indexes (cost similar to search queries)

### Storage Efficiency
- **Compression**: Both redb and tantivy use compression
- **Overhead**: ~20-50% overhead for search indexes
- **Growth**: WAL grows monotonically (compaction planned)

### Memory Usage
- **Base**: ~10MB for empty store
- **Per Document**: ~1KB overhead for indexing
- **Configurable**: `indexer_memory_budget` controls tantivy buffer size

## Concurrency Model

### Thread Safety
```rust
// HybridStore implements Send + Sync
let store = Arc::new(HybridStore::new(config)?);

// Safe to share across threads and actors
let store1 = Arc::clone(&store);
let store2 = Arc::clone(&store);

tokio::spawn(async move {
    // Each task offloads blocking work to a dedicated thread pool
    let result = tokio::task::spawn_blocking(move || {
        store1.get_by_key("employees", "user:123")
    }).await;
});
```

### Locking & Caching Strategy

- **IndexWriter cache**
  - `IndexWriter` is cached per index in an `Arc<Mutex<IndexWriter>>`
  - Writers are created on first access and reused across writes
  - Supervised Smart Commits logic uses per-index operation counters to decide when to call `commit()`

- **IndexReader cache (internal)**
  - `HybridStore` maintains an internal reader cache, but `search_documents` currently:
    - Ensures the index exists via `get_or_create_index`
    - Opens a fresh Tantivy `Index` and `IndexReader` for each call
  - This leaves room to hook in reader reuse later without changing the external API.

- **Sequence counters**
  - Per-index `AtomicU64` counters stored in a `HashMap` under `Arc<RwLock<...>>`
  - Ensure per-tenant monotonic WAL IDs

- **redb concurrency**
  - Relies on redb’s internal MVCC and transaction model for safe concurrent reads/writes

### Read Behavior and Caching

- **Per-index read cache**
  - `get_by_key(index, key)` first checks an in-memory cache keyed by `(index, id)`
  - On cache hit: returns the cached `Vec<u8>` without touching redb
  - On cache miss: performs a redb lookup, then inserts the bytes into the cache
  - Bounded to a fixed number of entries per index (simple eviction when full)

- **Planned enhancements**
  - A more advanced LRU policy and negative caching are planned, but the current design
    already provides good locality for hot keys while keeping the implementation simple.

## Error Handling and Recovery

### Error Types

At a high level, storage operations can fail with the following **error categories**:

- **redb errors**
  - Low-level issues from the embedded KV store (I/O, storage, transaction, commit)
  - Typically indicate disk, filesystem, or data corruption problems

- **Indexing errors (tantivy)**
  - Failures while writing to or reading from the search index
  - Examples: index directory missing, schema mismatch, segment corruption

- **Serialization errors**
  - JSON or bincode encoding/decoding issues for WAL entries, documents, or schemas

- **Query errors**
  - Problems parsing or executing search queries (invalid syntax, unknown fields)

- **Not-found / logical errors**
  - Index or field does not exist (`IndexNotFound`, `FieldNotFound`)
  - Usually indicate a bug in higher layers or an incorrect API call

The `StoreError` enum captures these categories in a strongly typed way:

```rust
#[derive(Debug, thiserror::Error)]
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
```

### Recovery Scenarios

#### Partial Write Failure
```
Scenario: redb commit succeeds, tantivy commit fails
Recovery: WAL entry exists, can replay tantivy update
Status: Consistent (redb authoritative)
```

#### Crash During Write
```
Scenario: Process crashes mid-operation
Recovery: WAL replay on restart (planned feature)
Status: Consistent (uncommitted operations lost)
```

#### Index Corruption
```
Scenario: tantivy index becomes corrupted
Recovery: Rebuild index from redb data using WAL
Status: Recoverable (redb contains all data)
```

## Testing

The storage engine includes comprehensive integration tests:

```bash
# Run all storage tests
cargo test -p storage

# Run specific test suites
cargo test -p storage --test integration
cargo test -p storage search_documents
cargo test -p storage test_persistence
cargo test -p storage test_atomic_operations
```

### Test Coverage
- **Basic Operations**: Put, Get, Delete operations
- **Search Functionality**: Full-text search with relevance scoring
- **Serialization**: JSON conversion and serde compatibility
- **Atomicity**: Consistency across both storage engines
- **Persistence**: Data survives process restart
- **Concurrency**: Thread safety and performance
- **Error Handling**: Various failure scenarios
- **Async Integration**: spawn_blocking patterns with actors

## Search Functionality

### Overview
The storage engine now provides full-text search capabilities through the `search_documents` method, which integrates tantivy's search engine with serde-compatible serialization.

### Multi-Tenant Search API
```rust
pub fn search_documents(
    &self,
    index: &str,
    query: &str,
    limit: usize,
) -> Result<Vec<(f32, JsonValue)>, StoreError>
```

### Return Format
- **`f32`**: Relevance score (higher = more relevant)
- **`JsonValue`**: Document content as serde-compatible JSON
- **Vector**: Sorted by relevance score (descending)

### Usage Example
```rust
use storage::{HybridStore, StorageConfig};
use serde_json::Value as JsonValue;

// Multi-tenant search
let results: Vec<(f32, JsonValue)> = store.search_documents("employees", "software engineer", 10)?;

for (score, document) in results {
    println!("Score: {:.3}", score);
    println!("Title: {}", document["title"].as_str().unwrap_or("N/A"));
    println!("Content: {}", document["body"].as_str().unwrap_or("N/A"));
    println!("Index: employees");
    println!("---");
}

// Search across multiple indices
let indices = ["employees", "contractors", "vendors"];
for index in &indices {
    let results = store.search_documents(index, "software engineer", 5)?;
    println!("Results from {}: {} matches", index, results.len());
}
```

### Async Integration for Search
```rust
use tokio::task;

async fn async_multi_tenant_search(
    store: HybridStore,
    index: String,
    query: String,
    limit: usize
) -> Result<Vec<(f32, JsonValue)>, StoreError> {
    let results = task::spawn_blocking(move || {
        store.search_documents(&index, &query, limit)
    }).await.map_err(|_| StoreError::Serialization("Task panicked".to_string()))??;
    
    Ok(results)
}
```

### Query Syntax
Supports standard tantivy query syntax:
- **Simple terms**: `"engineer"`
- **Phrase queries**: `"software engineer"`
- **Boolean operators**: `"software AND engineer"`
- **Field-specific**: `"title:manager"` (if fields are properly indexed)

### Search Performance Characteristics
- **Latency**: ~10-100ms per index (depends on index size and query complexity)
- **Multi-tenant Isolation**: Each index maintains independent search performance
- **Memory Scaling**: Dynamic memory budgets (32MB-512MB) based on index size
- **Indexing**: Automatic when documents are written via `apply_write` or `apply_batch`
- **Supervised Smart Commits**: Configurable commit frequency optimizes search freshness vs performance with eventual durability guarantees

## Supported Schema Field Types

CameoDB supports a rich set of field types for indexing and storage. These types map directly to Tantivy schema definitions:

| Type | Description | Tantivy Mapping |
|------|-------------|-----------------|
| **`text`** | Standard full-text search field. Tokenized and indexed. | `TEXT` |
| **`exact`** | Exact match field. Not tokenized; punctuation preserved. Case-sensitive. | `STRING` |
| **`boolean`** | Boolean value. Stored as "true"/"false" strings. | `STRING` |
| **`i64`** | 64-bit signed integer. Supports range queries and sorting. | `i64` (FAST) |
| **`u64`** | 64-bit unsigned integer. Supports range queries and sorting. | `u64` (FAST) |
| **`f64`** / **`number`** | 64-bit floating point. Supports range queries and sorting. | `f64` (FAST) |
| **`date`** | DateTime field (RFC3339). Supports range queries and sorting. | `date` (FAST) |
| **`array`** | Multi-valued text field. Each element is tokenized. | `TEXT` (multi-valued) |

> **Note:** All fields are `STORED` by default, meaning the original JSON value is retrievable.

## Serialization and Tantivy Integration

### The Challenge: Document Serialization

Initially, we attempted to return `Vec<tantivy::Document>` directly from actors for network transmission. However, this approach faced a critical limitation:

**Problem**: Tantivy 0.25 does not provide serde features for `Document` serialization.

### Our Solution: JSON Conversion

Instead of trying to serialize tantivy documents directly, we leverage tantivy's built-in JSON conversion:

```rust
// ❌ ATTEMPTED: Direct serde serialization (not supported in tantivy 0.25)
// Vec<tantivy::Document> // Cannot be serialized for actor communication

// ✅ SOLUTION: Convert to JSON via tantivy's built-in method
let doc: TantivyDocument = searcher.doc(doc_address)?;
let json_string = doc.to_json(&schema); // tantivy's built-in serialization
let json_doc: JsonValue = serde_json::from_str(&json_string)?; // Parse to serde type
```

### Key Technical Insights

#### Tantivy Type System
- **`tantivy::schema::Document`**: A trait defining document behavior
- **`tantivy::TantivyDocument`**: The concrete type implementing the Document trait
- **Trait Requirements**: The `to_json()` method requires the `Document` trait to be in scope

#### Import Strategy
```rust
use tantivy::schema::{Document, Field, Schema, STORED, STRING, TEXT};
use tantivy::TantivyDocument;

// Document trait must be in scope for to_json() method
let doc: TantivyDocument = searcher.doc(doc_address)?;
let json = doc.to_json(&schema); // Works because Document trait is imported
```

#### Serde Interoperability
```rust
// tantivy document → JSON string → serde Value → network serializable
TantivyDocument → String → JsonValue → Vec<u8> (for network transmission)
```

### Actor Integration Benefits

This approach enables distributed search across the actor system:

1. **Serializable Results**: `Vec<(f32, JsonValue)>` can be sent between actors
2. **Network Compatible**: JSON format works across language boundaries
3. **Type Safety**: Maintains Rust's type safety through the conversion pipeline
4. **Performance**: Leverages tantivy's optimized JSON serialization

### Thread Safety and Async Compatibility

```rust
// ✅ CORRECT: Actor usage pattern
impl MicroshardActor {
    pub async fn handle_search(&self, request: SearchRequest) -> Result<Vec<(f32, JsonValue)>, Error> {
        let store = Arc::clone(&self.store);
        
        // Offload blocking tantivy operations to thread pool
        let results = tokio::task::spawn_blocking(move || {
            store.search_documents(&request.query, request.limit)
        }).await??;
        
        Ok(results) // Can be serialized and sent to other actors
    }
}
```

## Future Enhancements

### Planned Features
- **Schema Evolution**: Dynamic field addition with type validation
- **Advanced Query Support**: Range queries, faceted search, aggregations
- **WAL Compaction**: Periodic cleanup of old WAL entries
- **Cross-Index Search**: Federated search across multiple indices
- **Index Templates**: Predefined schemas for new indices
- **Backup/Restore**: Point-in-time backup capabilities per index
- **Replication**: Multi-node consistency with per-index replication
- **Compression**: Configurable compression algorithms
- **Metrics**: Performance monitoring and health checks per index

### Performance Optimizations
- **Supervised Smart Commits**: ✅ Implemented - Adaptive commit frequency based on memory budget with eventual durability guarantees via async supervision
- **Dynamic Memory Budgets**: ✅ Implemented - Per-index memory scaling (32MB-512MB)
- **Atomic Batch Processing**: ✅ Implemented - Single transaction for multiple operations
- **Multi-tenant Caching**: ✅ Implemented - Independent caches per index
- **Operation Counting**: ✅ Implemented - Lock-free AtomicU64 counters per index
- **Read Caching**: ✅ Implemented - Simple bounded cache (LRU policy planned)
- **Index Optimization**: Periodic segment merging (planned)
- **Async I/O**: Non-blocking operations where possible (planned)
