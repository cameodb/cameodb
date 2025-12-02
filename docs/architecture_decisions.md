# Architecture Decision Records (ADRs)

This document captures the key architectural decisions made during CameoDB's development, including the rationale, alternatives considered, and trade-offs.

## ADR 001: Topology - SHA256 and 256 Virtual Nodes

### Status
**Accepted** - Implemented in `crates/cluster`

### Context
CameoDB requires a distributed topology system that can:
- Distribute data evenly across nodes
- Minimize data movement during cluster changes
- Provide deterministic routing without coordination
- Scale to hundreds of nodes

### Decision
We chose **consistent hashing** with:
- **SHA256** for hash function
- **256 virtual nodes** per physical node

### Rationale

#### Why SHA256?

| Hash Function | Pros | Cons | Decision |
|---------------|------|------|----------|
| **SHA256** | Cryptographically secure, excellent distribution, collision-resistant | Slower than non-crypto hashes | ✅ **Chosen** |
| CRC32 | Very fast | Poor distribution, high collision rate | ❌ Rejected |
| xxHash | Fast, good distribution | Not cryptographically secure | ❌ Rejected |
| SipHash | Fast, DoS-resistant | Weaker distribution than SHA256 | ❌ Rejected |

**SHA256 Benefits:**
- **Excellent Distribution**: Cryptographic properties ensure uniform distribution
- **Collision Resistance**: Virtually eliminates hash collisions
- **Deterministic**: Same input always produces same output
- **Future-Proof**: Widely supported and battle-tested

**Performance Impact:**
- SHA256 adds ~1-2μs per key lookup
- Acceptable overhead for the distribution quality gained
- Can be optimized with hardware acceleration if needed

#### Why 256 Virtual Nodes?

**Analysis of VNode Count Impact:**

| VNodes | Memory per Node | Distribution Quality | Rebalancing Efficiency | Decision |
|--------|----------------|---------------------|----------------------|----------|
| 64 | 512 bytes | Poor with <10 nodes | ~1.5% keys move | ❌ Too few |
| 128 | 1 KB | Acceptable | ~0.8% keys move | ⚠️ Marginal |
| **256** | **2 KB** | **Good** | **~0.4% keys move** | ✅ **Optimal** |
| 512 | 4 KB | Excellent | ~0.2% keys move | ⚠️ Diminishing returns |
| 1024 | 8 KB | Excellent | ~0.1% keys move | ❌ Excessive overhead |

**256 VNodes provides:**
- **Stability**: Good load distribution even with small clusters (3-5 nodes)
- **Memory Efficiency**: Only 2KB per node (256 × 8 bytes)
- **Rebalancing**: Minimal data movement (~1/256 ≈ 0.4% of keys per node change)

### Implementation

```rust
const VNODE_COUNT: usize = 256;

fn generate_tokens(uuid: Uuid) -> Vec<u64> {
    (0..VNODE_COUNT as u32)
        .map(|index| {
            let mut hasher = Sha256::new();
            hasher.update(uuid.as_bytes());      // Node identity
            hasher.update(&index.to_be_bytes()); // VNode index
            let digest = hasher.finalize();
            u64::from_be_bytes(digest[0..8].try_into().unwrap())
        })
        .collect()
}
```

### Alternatives Considered

1. **Range-Based Sharding**: Rejected due to hotspot issues and complex rebalancing
2. **Directory-Based Routing**: Rejected due to single point of failure
3. **Rendezvous Hashing**: Rejected due to O(N) lookup complexity

### Consequences

**Positive:**
- Excellent load distribution across cluster sizes
- Minimal data movement during topology changes
- No coordination required for routing decisions
- Scales to large cluster sizes

**Negative:**
- 2KB memory overhead per node
- SHA256 computation adds minor latency
- Fixed VNode count (not dynamically adjustable)

---

## ADR 002: Storage - redb + tantivy Hybrid Architecture

### Status
**Accepted** - Implemented in `crates/storage`

### Context
CameoDB requires a storage engine that provides:
- ACID transactions for consistency
- Fast key-value lookups
- Full-text search capabilities
- High write throughput
- Durability guarantees

### Decision
We chose a **hybrid architecture** combining:
- **redb**: ACID-compliant key-value store for primary data
- **tantivy**: Full-text search engine for query capabilities

### Rationale

#### Why Not Single-Engine Solutions?

| Single Engine | Pros | Cons | Decision |
|---------------|------|------|----------|
| **PostgreSQL** | ACID, SQL, mature | Heavy, complex setup, not embedded | ❌ Too heavy |
| **SQLite** | Embedded, ACID, simple | No full-text search, single writer | ❌ Limited search |
| **Elasticsearch** | Excellent search | No ACID, eventual consistency | ❌ No transactions |
| **RocksDB** | Fast KV, embedded | No search, no transactions | ❌ Missing features |

#### Why redb + tantivy?

**redb Strengths:**
- **ACID Transactions**: Full ACID compliance with MVCC
- **Embedded**: No separate server process required
- **Performance**: Optimized B+ trees for fast lookups
- **Rust Native**: Zero-copy operations, memory safety
- **Single File**: Simple deployment and backup

**tantivy Strengths:**
- **Fast Search**: Optimized inverted indexes
- **Rich Queries**: Boolean, phrase, fuzzy, range queries
- **Rust Native**: Type safety and performance
- **Configurable**: Memory usage, compression, scoring
- **Concurrent**: Multiple readers, single writer

**Hybrid Benefits:**
- **Best of Both**: Fast lookups + rich search
- **Complementary**: redb handles consistency, tantivy handles search
- **Single API**: Unified interface for both access patterns
- **Atomic Updates**: Dual-write ensures consistency

### Architecture

```
┌─────────────────────────────────────────┐
│              Client API                 │
└─────────────────┬───────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────┐
│           HybridStore                   │
│         (Atomic Dual-Write)             │
└─────────┬───────────────────────────────┘
          │
    ┌─────▼─────┐              ┌─────────┐
    │   redb    │              │ tantivy │
    │ (Primary) │◄────────────►│(Search) │
    │   ACID    │   Sync       │ Index   │
    └───────────┘              └─────────┘
```

### Implementation Strategy

#### Atomic Dual-Write Protocol

```rust
pub fn apply_write(&self, index: &str, op: WalOp) -> Result<u64, StoreError> {
    // 1. Ensure per-index assets exist and grab writer
    let (writer_arc, fields) = self.get_or_create_index(index)?;

    // 2. Allocate next WAL sequence for this index (independent counters)
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

    // 3. Persist WAL + primary data inside a single redb write transaction
    let wal_table = TableDefinition::<u64, &[u8]>::new(&format!("wal_{}", index));
    let data_table = TableDefinition::<&str, &[u8]>::new(&format!("data_{}", index));
    let wal_bytes = serde_json::to_vec(&op)
        .map_err(|e| StoreError::Serialization(e.to_string()))?;

    let write_txn = self.kv.begin_write()?;
    {
        let mut wal = write_txn.open_table(wal_table)?;
        wal.insert(seq_id, wal_bytes.as_slice())?;

        let mut data = write_txn.open_table(data_table)?;
        match &op {
            WalOp::Put { id, body, json_blob } => {
                let doc_payload = serde_json::json!({
                    "body": body,
                    "json_blob": json_blob
                });
                let doc_bytes = serde_json::to_vec(&doc_payload)
                    .map_err(|e| StoreError::Serialization(e.to_string()))?;
                data.insert(id.as_str(), doc_bytes.as_slice())?;

                let mut writer = writer_arc.lock().unwrap();
                let mut tantivy_doc = doc!(fields.id => id.as_str(), fields.body => body.as_str());
                if let Some(json_data) = json_blob {
                    let json_str = serde_json::to_string(json_data)
                        .map_err(|e| StoreError::Serialization(e.to_string()))?;
                    tantivy_doc.add_text(fields.json_blob, &json_str);
                }
                writer.add_document(tantivy_doc)?;
            }
            WalOp::Delete { id } => {
                data.remove(id.as_str())?;
                let mut writer = writer_arc.lock().unwrap();
                let term = tantivy::Term::from_field_text(fields.id, id);
                writer.delete_term(term);
            }
        }
    }
    write_txn.commit()?; // ← Durability checkpoint (wal + data)

    // 4. Adaptive commit heuristics per index (operations counter + memory budget)
    self.increment_operations(index);
    self.maybe_commit_writer(index)?;

    Ok(seq_id)
}
```

**Consistency Model:**
- **redb is authoritative**: All data must exist in redb
- **tantivy is derived**: Search index derived from redb data
- **Per-index WAL**: Each index maintains an independent WAL table and sequence counter
- **WAL for recovery**: Can rebuild tantivy from redb + per-index WAL snapshots

### Alternatives Considered

#### Alternative 1: Single Database with Extensions
```
PostgreSQL + pg_trgm + FTS
```
**Rejected because:**
- Heavy deployment (separate server process)
- Complex configuration and tuning
- Not embedded (complicates distribution)
- SQL overhead for simple KV operations

#### Alternative 2: Embedded Database with Plugins
```
SQLite + FTS5 extension
```
**Rejected because:**
- Limited full-text search capabilities
- Single writer limitation
- No native Rust integration
- Extension compatibility issues

#### Alternative 3: Pure Search Engine
```
tantivy only (store documents in index)
```
**Rejected because:**
- No ACID transactions
- Slower point lookups
- Limited consistency guarantees
- Complex backup/recovery

#### Alternative 4: Distributed Database
```
TiKV + separate search service
```
**Rejected because:**
- Complex deployment (multiple processes)
- Network overhead for local operations
- Consistency challenges across services
- Over-engineered for single-node use case

### Trade-offs

#### Advantages
- **Performance**: Fast KV lookups + fast search
- **Consistency**: ACID guarantees for all operations
- **Flexibility**: Multiple access patterns supported
- **Embedded**: No external dependencies
- **Recovery**: Can rebuild search index from KV data

#### Disadvantages
- **Complexity**: Two storage engines to manage
- **Storage Overhead**: Data stored in both engines
- **Consistency Risk**: Dual-write can fail partially
- **Memory Usage**: Both engines consume memory

### Mitigation Strategies

#### Consistency & Async Boundary Mitigation
```rust
// Storage calls always run inside spawn_blocking from async actors/services
let store = self.store.clone();
let index = request.index.clone();
tokio::task::spawn_blocking(move || store.apply_write(&index, op)).await??;

// WAL-first approach ensures recoverability if Tantivy fails
if redb_commit_succeeds && tantivy_commit_fails {
    tracing::warn!(index, seq_id, "tantivy update failed, scheduling repair");
    schedule_index_repair(index, seq_id);
}
```

#### Storage Overhead Mitigation
- Store minimal data in tantivy (ID + searchable text only)
- Use compression in both engines
- Periodic index optimization to reduce size

#### Memory Usage Optimization
- Configurable tantivy memory budget
- Lazy loading of index segments
- Shared memory pools where possible

---

## ADR 003: Async Safety - Strict Blocking/Async Boundary

### Status
**Accepted** - Enforced throughout codebase

### Context
CameoDB uses both async (networking, coordination) and blocking (storage) operations. Mixing these incorrectly can cause:
- Thread pool starvation
- Deadlocks
- Poor performance
- Runtime panics

### Decision
Enforce **strict separation** between async and blocking code with the rule:

> **"Storage is Blocking, Network is Async. Boundary is `spawn_blocking`."**

### Rationale

#### The Problem with Mixed Async/Blocking

```rust
// ❌ DANGEROUS: Blocking call in async context
async fn bad_example(store: &HybridStore) {
    let data = store.get_by_key("user:123")?; // Blocks entire async runtime!
    // All other async tasks are now blocked
}
```

**Consequences:**
- **Thread Starvation**: Async runtime has limited threads (typically CPU count)
- **Cascading Delays**: One blocked task delays all others
- **Deadlocks**: Async tasks waiting for blocked tasks
- **Poor Scalability**: Cannot handle concurrent requests

#### Why Storage Must Be Blocking

| Storage Operation | Why Blocking | Alternative Considered |
|-------------------|--------------|----------------------|
| **redb I/O** | Synchronous file operations | ❌ No async redb available |
| **tantivy indexing** | CPU-intensive processing | ❌ Would complicate API |
| **WAL writes** | fsync() for durability | ❌ Async fsync unreliable |
| **B-tree traversal** | Memory-bound operations | ❌ Async adds overhead |

**Storage engines are inherently blocking because:**
- File I/O operations are synchronous
- Memory-mapped files require blocking access
- Transaction isolation needs synchronous coordination
- Durability guarantees require blocking fsync()

#### Why Network Must Be Async

| Network Operation | Why Async | Blocking Alternative |
|-------------------|-----------|---------------------|
| **HTTP requests** | Variable latency (1ms-1s+) | ❌ Wastes threads |
| **gRPC calls** | Network I/O bound | ❌ Poor concurrency |
| **Cluster gossip** | Many concurrent connections | ❌ Thread explosion |
| **Client connections** | Long-lived connections | ❌ Resource exhaustion |

### Implementation

#### Correct Async/Blocking Boundary

```rust
// ✅ CORRECT: Proper boundary management
pub struct NodeService {
    store: Arc<HybridStore>, // Shared, thread-safe storage
}

impl NodeService {
    // Async network handler
    async fn handle_request(&self, request: Request) -> Result<Response> {
        let store = Arc::clone(&self.store);
        
        // Boundary: async → blocking
        let result = tokio::task::spawn_blocking(move || {
            // Now in blocking context - safe to call storage
            match request.operation {
                Operation::Get { key } => {
                    store.get_by_key(&key) // ✅ Safe: blocking context
                }
                Operation::Put { key, value } => {
                    let op = WalOp::Put { id: key, body: value, json_blob: None };
                    store.apply_write(op) // ✅ Safe: blocking context
                }
            }
        }).await.map_err(|_| "Task panicked")??;
        
        // Back in async context
        Ok(Response::from(result))
    }
}
```

#### Thread Pool Management

```rust
// Configure separate thread pools
let rt = tokio::runtime::Builder::new_multi_thread()
    .worker_threads(num_cpus::get()) // Async workers
    .max_blocking_threads(512)       // Blocking thread pool
    .enable_all()
    .build()?;

// Async tasks use worker threads
rt.spawn(async {
    handle_network_request().await
});

// Blocking tasks use blocking thread pool
rt.spawn_blocking(|| {
    storage.apply_write(operation)
});
```

### Enforcement Mechanisms

#### 1. Type System Enforcement

```rust
// Storage types are NOT Send to async contexts
impl HybridStore {
    // Methods are NOT async - compiler prevents misuse
    pub fn apply_write(&self, op: WalOp) -> Result<u64, StoreError> {
        // Blocking implementation
    }
}

// Network types ARE async
impl NetworkService {
    pub async fn send_request(&self, req: Request) -> Result<Response> {
        // Async implementation
    }
}
```

#### 2. Documentation Enforcement

```rust
/// **CRITICAL**: This method is blocking and NOT async-safe.
/// 
/// Never call directly from async contexts. Always wrap in `spawn_blocking`:
/// 
/// ```rust
/// let result = tokio::task::spawn_blocking(move || {
///     store.apply_write(op) // ✅ Safe: wrapped in spawn_blocking
/// }).await??;
/// ```
pub fn apply_write(&self, op: WalOp) -> Result<u64, StoreError> {
    // Implementation
}
```

#### 3. Runtime Enforcement (Future)

```rust
// Potential runtime check (debug builds only)
pub fn apply_write(&self, op: WalOp) -> Result<u64, StoreError> {
    #[cfg(debug_assertions)]
    {
        if tokio::runtime::Handle::try_current().is_ok() {
            panic!("Storage method called from async context! Use spawn_blocking.");
        }
    }
    
    // Implementation
}
```

### Patterns and Anti-Patterns

#### ✅ Correct Patterns

```rust
// Pattern 1: Async wrapper for storage operations
async fn async_get(store: Arc<HybridStore>, key: String) -> Result<Option<Vec<u8>>> {
    tokio::task::spawn_blocking(move || {
        store.get_by_key(&key)
    }).await?
}

// Pattern 2: Batch operations with proper boundaries
async fn batch_write(store: Arc<HybridStore>, ops: Vec<WalOp>) -> Result<Vec<u64>> {
    let mut results = Vec::new();
    
    for op in ops {
        let store = Arc::clone(&store);
        let seq_id = tokio::task::spawn_blocking(move || {
            store.apply_write(op)
        }).await??;
        results.push(seq_id);
    }
    
    Ok(results)
}

// Pattern 3: Service layer with proper separation
struct HybridService {
    store: Arc<HybridStore>,    // Blocking storage
    client: reqwest::Client,    // Async networking
}

impl HybridService {
    async fn replicate_write(&self, op: WalOp) -> Result<u64> {
        // 1. Apply locally (blocking)
        let store = Arc::clone(&self.store);
        let seq_id = tokio::task::spawn_blocking(move || {
            store.apply_write(op.clone())
        }).await??;
        
        // 2. Replicate to peers (async)
        let futures = self.peers.iter().map(|peer| {
            self.client.post(&peer.url)
                .json(&op)
                .send()
        });
        
        try_join_all(futures).await?;
        
        Ok(seq_id)
    }
}
```

#### ❌ Anti-Patterns

```rust
// Anti-pattern 1: Blocking in async context
async fn bad_async_storage(store: &HybridStore) {
    let data = store.get_by_key("key")?; // ❌ Blocks async runtime
}

// Anti-pattern 2: Async in blocking context
fn bad_blocking_network(client: &reqwest::Client) {
    let response = client.get("http://api.example.com")
        .send()
        .await?; // ❌ Cannot await in blocking context
}

// Anti-pattern 3: Mixed boundaries in single function
async fn bad_mixed_function(store: &HybridStore, url: &str) {
    let data = store.get_by_key("key")?; // ❌ Blocking
    let response = reqwest::get(url).await?; // ❌ Mixed with async
}
```

### Performance Impact

#### Spawn Blocking Overhead

```rust
// Overhead analysis
let start = Instant::now();

// Direct call (blocking context)
let result1 = store.get_by_key("key")?; // ~0.1ms
let direct_time = start.elapsed();

// Spawn blocking call (async context)  
let result2 = tokio::task::spawn_blocking(move || {
    store.get_by_key("key") // ~0.1ms + ~0.05ms spawn overhead
}).await??;
let spawn_time = start.elapsed();

// Overhead: ~50μs per spawn_blocking call
```

**Mitigation Strategies:**
- **Batch Operations**: Group multiple storage calls in single `spawn_blocking`
- **Connection Pooling**: Reuse blocking threads where possible
- **Caching**: Reduce storage calls with intelligent caching

### Consequences

#### Positive
- **Predictable Performance**: No surprise blocking in async code
- **Scalability**: Async runtime can handle many concurrent connections
- **Correctness**: Eliminates entire class of concurrency bugs
- **Maintainability**: Clear separation of concerns

#### Negative
- **Complexity**: Developers must understand async/blocking boundary
- **Overhead**: `spawn_blocking` adds ~50μs per call
- **Memory Usage**: Larger thread pools consume more memory
- **Learning Curve**: Requires understanding of async Rust patterns

#### Mitigation
- **Documentation**: Extensive docs and examples
- **Type Safety**: Compiler prevents most mistakes
- **Tooling**: Lints and runtime checks (future)
- **Training**: Team education on async/blocking patterns
