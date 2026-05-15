# CameoDB Development & Optimization Plan

This document outlines the current development priorities and optimization roadmap for CameoDB.

## ✅ Completed Phases (Archived)

**Phase 1**: Storage Durability & WAL Recovery ✅ COMPLETED
- Added Sequence ID to Schema for WAL tracking
- Implemented WAL Replay with get_last_indexed_seq/recover_index
- Integrated automatic recovery during index open
- Shortened critical section with optimized serialization

**Phase 2**: Shadow Field Replacement ✅ COMPLETED  
- Replaced shadow field scanning with O(1) HashSet lookup
- Implemented shadow field replacement logic
- Optimized move semantics for performance
- Fixed shadow field behavior in document reconstruction

**Phase 3**: Index Warmup & Recovery ✅ COMPLETED
- Added automatic index warmup on startup
- Implemented recovery procedures for index consistency
- Enhanced index management with proper error handling

**Phase 4**: Basic Actor System ✅ COMPLETED
- Built Kameo-based actor system for shard management
- Implemented MicroshardActor with message handling
- Added StorageCommand enum for thread-safe operations
- Created writer thread pattern for isolation

**Phase 5**: Cluster Coordination ✅ COMPLETED
- Implemented distributed cluster coordination with DHT
- Added consistent hashing ring for node distribution
- Created ClusterCoordinator for swarm management
- Integrated peer discovery and metadata exchange

**Phase 6**: Storage Performance Optimizations ✅ COMPLETED
- Optimized I/O patterns with batch WAL recovery
- Implemented granular thread pool architecture
- Added writer thread write coalescing
- Enhanced ACID-compliant commit optimization
- Configured Redb cache sizes (64MB read, 32MB write)
- Verified bulk memory budget scaling with comprehensive tests

**Phase 7**: Code Review Issues & Critical Fixes ✅ COMPLETED
- Fixed read runtime resource leak with Drop trait implementation
- Prevented writer thread starvation with bounded drain limit (max 64 commands)
- Corrected batch coalescing math using integer arithmetic with remainder distribution
- All critical bugs and resource leaks resolved

**Phase 8**: RouterActor & Architecture Enhancements ✅ COMPLETED
- Implemented worker pool pattern bypassing actor mailbox for hot-path operations
- Added lock-free intelligent caching (schema cache, fingerprint index, routing ring)
- Delegated routing decisions to ClusterCoordinator
- Optimized scatter-gather with streaming search

**Phase 9**: Advanced Architecture Optimizations ✅ COMPLETED
- Parallel schema evolution: staged Rayon validation followed by sequential evolution with concurrent persistence (50‑70% faster on multi-shard clusters).
- Remote connection pooling: shared `RemotePeerPool` with channel-aware caching, automatic invalidation on `PeerLost`, and full integration across RouterActor, NodeOrchestrator bulk forwarding, and ClusterCoordinator remotes.

*Note: Phases 1-9 are fully completed with all optimizations implemented and tested.*

## Phase 10: Field Projection for Search Responses ✅ COMPLETED

**Implementation Summary:**
- **HTTP Layer**: Extended `SearchPayload` with `fields: Option<Vec<String>>` and implemented `parse_query_keywords()` to extract `limit` and `return` keywords from query strings. Both `search_handler` and `search_stream_handler` now support field projection.
- **Routing Layer**: Updated `ClientOp::Search` and `ClientOp::Stream` to carry `fields` parameter through all routing paths (local, remote, broadcast, streaming).
- **Execution Layer**: Created `apply_field_projection()` helper that filters JSON documents while preserving metadata fields (those starting with `_`). Integrated into both `engine_search()` and `orch_search()` methods.

**Query Syntax**: `<tantivy_query> [limit <n>] [return <field1,field2,...>]`  
**Example**: `title:rust return title,author,year` returns only those three fields plus metadata.

---

## Phase 11: Read/Write Workflow Hot-Path Optimizations 🚧 IN PROGRESS

**Implementation Steps:**
1. **Remove Tantivy ID roundtrip in search hits** ✅ COMPLETED
   - Replace the current `TantivyDocument -> JSON string -> serde_json::Value -> id extraction` flow with direct extraction of stored `id` field values from Tantivy search results.
   - Reduce per-hit allocation and parsing overhead in `HybridStore::search_documents()`.

2. **Tighten duplicate work inside `apply_batch()`** ✅ COMPLETED
   - Reuse already loaded schema and prepared document state throughout the batch path.
   - Eliminate repeated shadow filtering, repeated schema lookups, and avoidable re-serialization/deserialization inside the per-operation loop.

3. **Enforce configured shard and remote concurrency limits** ✅ COMPLETED
   - Apply `max_concurrent_shard_searches` and `max_concurrent_remote_searches` in scatter-gather paths with bounded concurrency.
   - Prevent search fan-out from oversubscribing local resources or remote peers under wide broadcasts.

4. **Reduce worker-pool coordination contention** ✅ COMPLETED
   - Revisit the shared receiver mutex in the orchestrator worker pool and move toward a lower-contention queue design.
   - Preserve the hot-path worker model while improving throughput under concurrent read/write load.

5. **Improve early-termination and result-merge behavior** ✅ COMPLETED
   - Refine broadcast search merging so early termination does not stop on count alone when higher-scoring remote hits may still arrive.
   - Move toward bounded top-K merging with score-aware pruning.

6. **Implement true end-to-end search streaming** ✅ COMPLETED
   - Replace buffered result aggregation with incremental shard-to-router-to-HTTP streaming for NDJSON search responses.
   - Keep projection, score metadata, and bounded backpressure-aware fan-in behavior.

7. **Implement incremental write-stream ingestion** ✅ COMPLETED
   - Replace whole-body buffering in `write_stream_handler` with incremental NDJSON decoding and bounded ingestion.
   - Preserve shard routing, batch coalescing, and backpressure while lowering peak memory for large imports.

---

## Phase 11.5: Jemalloc Memory Management ✅ COMPLETED

**Implementation Summary:**
- **Jemalloc integration**: Integrated `tikv-jemallocator` and `tikv-jemalloc-sys` (with `stats` feature) on Linux targets for production memory management.
- **Admin HTTP endpoints**: Added `GET /_admin/memory` (stats) and `POST /_admin/memory/purge` (manual purge with optional `force` flag).
- **Admin CLI commands**: Added `admin memory stats` and `admin memory purge [--force]` to the interactive CLI and command-line client.
- **Typed response structs**: `AdminMemoryReport`, `ProcessMemoryStats`, `JemallocStats` with platform-aware field omission (null fields excluded from JSON).
- **Cross-platform stats**: Linux uses `/proc/self/status`, macOS uses `proc_pidinfo` syscall, Windows uses `wmic process` — all providing RSS, VSZ, and thread count.
- **Jemalloc purge**: Decay-based purge (respects `dirty_decay_ms`) and aggressive purge (bypasses timers). Returns `process` (before) and `process_after_purge` snapshots plus `purge_result`.
- **Systemd service tuning**: `cameodb.service` ships with production `MALLOC_CONF`: `background_thread:true,percpu_arena:percpu,oversize_threshold:0,dirty_decay_ms:2000,muzzy_decay_ms:0`.

**Default `MALLOC_CONF` rationale:**
- `dirty_decay_ms:2000` — balances throughput for 8-32 parallel writers while keeping memory pressure reasonable. Override via `systemctl edit cameodb` if RSS becomes a concern.

---

## Summary & Next Steps

### **Current Status**
- ✅ **Phases 1-9**: All completed and archived
- ✅ **Phase 10 (Field Projection)**: Completed – query string parsing, routing propagation, and JSON filtering fully implemented and tested.
- ✅ **Phase 11 (Workflow Hot-Path Optimizations)**: All 7 steps completed and verified.
- ✅ **Phase 11.5 (Jemalloc Memory Management)**: Completed — cross-platform memory stats, jemalloc purge endpoints, systemd tuning.

### **Recommended Next Steps**
1. **Follow-up Validation**: Benchmark the completed Phase 11 hot-path improvements under broadcast-heavy and ingestion-heavy workloads
2. **Phase 12 (MCP Server Integration)**: Enable AI agents to efficiently search CameoDB indexes via Model Context Protocol

---

## Phase 12: MCP Server Integration for AI Agents 🎯 PLANNED

**Objective**: Implement a Model Context Protocol (MCP) server within CameoDB to expose search capabilities as tools for AI agents, enabling efficient context retrieval from indexed datasets.

**Architecture Goals:**
- Single CameoDB binary with MCP exposed through the existing HTTP server
- HTTP/SSE network transport using a shared-port model
- New `crates/mcp` package defines its own `axum::Router` but does not start a separate server
- Main `server` crate nests the MCP router into the existing application router and shares the same `AppState`
- Expose search and metadata capabilities as MCP tools while reusing the stable search path
- Support both local and cluster-wide operations through existing `RouterActor` and `ClusterCoordinator`
- Enable session-aware JSON-RPC message handling and streaming results for large datasets

**Implementation Steps:**

1. **Workspace & Dependencies** ✅ COMPLETED
   - Create `crates/mcp` package and add it to the workspace `Cargo.toml`
   - Add required dependencies to `crates/mcp/Cargo.toml`: `axum`, `axum-extra`, `tokio`, `serde`, `serde_json`, and an MCP/JSON-RPC Rust SDK
   - Add the new `cameodb_mcp` crate as a dependency of the main `server` crate
   - Keep MCP transport inside the existing application runtime; do not start a second HTTP server

2. **MCP Router & Transport Layer** ✅ COMPLETED
   - Create `crates/mcp/src/server.rs` with a function returning `Router<AppState>`
   - Implement `GET /sse` to establish SSE transport and register client sessions
   - Implement `POST /messages` to receive JSON-RPC messages, map them to sessions, and route them to MCP handlers
   - Mount the MCP router from `crates/server/src/http_server.rs` using `.nest()` on the existing Axum app
   - Reuse the main shared `AppState` so MCP handlers can call the same routing and cluster services as HTTP APIs

3. **MCP Protocol Session Handling** ✅ COMPLETED
   - Implement MCP session registry and connection lifecycle management
   - Support initialize, ping, capabilities negotiation, tools listing, and tools invocation over JSON-RPC
   - Correct notification handling (notifications/initialized, notifications/cancelled return no response per JSON-RPC spec)
   - Define transport-safe error mapping from CameoDB failures into MCP error responses
   - Add bounded session cleanup, heartbeat handling, and backpressure-aware streaming behavior

4. **Core MCP Tools** ✅ COMPLETED (MCP naming convention: verb-first snake_case, with title/annotations)
   - **`search_index`**: Execute full-text search on a single index
     - Parameters: `index`, `query`, `limit`, `fields` (optional projection)
     - Returns: JSON array of matching documents with scores
   - **`search_indexes`**: Federated search across multiple indexes
     - Parameters: `indexes[]`, `query`, `limit`
     - Returns: Combined results with `_index_source` metadata and per-index field projection
   - **`get_index`**: Retrieve schema and statistics for a single index
     - Parameters: `index`
     - Returns: Complete field definitions, types, document count, size
   - **`validate_query`**: Field-type-aware CameoDB query syntax validation, unknown field detection, structural checks (quotes/parens), fuzzy "did you mean" suggestions, and full syntax reference
   - **`get_index_stats`**: Document counts, field distributions, aggregated stats for single or all indexes
   - **`list_indexes`**: Enumerate all available indexes with schemas
     - Parameters: none
     - Returns: All index schemas with metadata (leverages existing `/_indexes` endpoint)

5. **Advanced MCP Features** ✅ COMPLETED
   - **Field Projection**: Auto-suggest relevant fields based on partial input
   - All tools include `title`, property `description`s, and `annotations` (`readOnlyHint`, `openWorldHint`) per MCP draft spec
   - **Streaming Support**: 📋 PLANNED — Large result sets via MCP streaming protocol
   - **Semantic Routing**: 📋 PLANNED — Auto-select best index(es) for query intent

6. **MCP Resource Providers** ✅ COMPLETED
   - Expose indexes as MCP resources for exploration
   - Provide schema documentation as resources
   - Enable agents to discover available datasets dynamically

7. **Security & Access Control** 📋 PLANNED
   - Optional index-level access restrictions
   - Query complexity limits (prevent resource exhaustion)
   - Rate limiting for agent requests
   - Audit logging for MCP tool invocations

8. **Documentation & Examples** 📋 PLANNED
   - MCP server setup guide
   - Example agent configurations (Claude Desktop, etc.)
   - Sample prompts and workflows
   - Best practices for index design for AI context

9. **Testing & Validation** 📋 PLANNED
   - MCP protocol compliance tests
   - Integration tests with MCP clients
   - Performance benchmarks for agent query patterns
   - Example datasets optimized for RAG workflows

**Expected Benefits:**
- Enable AI agents to query structured/unstructured data efficiently
- Provide grounded context for LLM responses from real datasets
- Support RAG (Retrieval-Augmented Generation) workflows
- Unlock new use cases: semantic search, knowledge retrieval, fact-checking
- Position CameoDB as AI-native search infrastructure

**Success Metrics:**
- MCP server responds to all standard tool calls correctly
- Search latency < 100ms for typical agent queries
- Support concurrent agent sessions without degradation
- Compatible with major MCP clients (Claude Desktop, custom agents)

---

## Phase 13: Thread-Per-Core Optimization for Write Operations 🎯 PLANNED

**Objective**: Eliminate cross-core wakeups and cache thrashing on the write hot path by implementing shard-affine worker dispatch and per-shard core pinning. Achieve true thread-per-core semantics where each shard's compute (parse → route → enqueue → wait reply) executes on the same core as its writer thread.

### Current Architecture Analysis

**Existing Threading Model:**
- **Tokio Async Runtimes (2 separate)**:
  - Main runtime: HTTP server (axum), kameo actors, orchestrator workers
  - Dedicated read runtime: `multi_thread` builder, threads named `cameodb-read`, threads = `config.search_threads` or `max(2, cpu_cores / 2)`

- **Orchestrator Worker Pool** (async, mailbox-bypass):
  - One `mpsc::channel::<OrchestratorJob>` per worker (not shared)
  - `worker_count = max(1, min(local_shards * 2, cpu_cores * 2))`
  - `per_worker_queue_capacity = ORCHESTRATOR_WORKER_QUEUE_CAPACITY (4096) / worker_count`
  - Dispatch is round-robin via `OrchestratorWorkerTx::try_send` (atomic counter, fall-through on Full)
  - Workers are tokio tasks on the main runtime — NOT pinned

- **Per-Shard Dedicated Writer Thread** (sync OS thread):
  - One OS thread per shard, named `writer-shard-<uuid>`, spawned via `std::thread::Builder`
  - Receives `StorageCommand` over bounded `mpsc::channel` (capacity = 1024)
  - Implements write coalescing: blocks on first command, then `try_recv` drains up to 256 more
  - Groups by `(operation_type, index)`, merges into single `apply_batch_and_maybe_commit` per index
  - Strictly serializes writes per shard (required by redb single-writer semantics)

- **Tantivy Indexer Threads** (per index):
  - `indexer_num_threads: 1` (default — optimal because writer thread is already serial)
  - `merge_num_threads: 2` (background segment compaction)
  - Extra threads spawned inside Tantivy per IndexWriter

**Current Hot-Path Trace (Write):**
```
HTTP req on axum tokio worker (any core)
  → AppState::router.route_and_handle(op, ...)        [main rt task]
  → OrchestratorWorkerTx::try_send (round-robin)      [atomic fetch_add]
  → mpsc::Sender<OrchestratorJob> (per-worker queue)
  → Orchestrator worker tokio task on main rt (any core, may migrate)
  → engine.execute(op) → engine_write(...)            [routing, validation]
  → MicroshardActor.handle_write_via_channel
  → mpsc::Sender<StorageCommand> (per-shard, cap 1024)
  → writer-shard-<uuid> OS thread (pinned in Stage 1)
  → reply via oneshot back across all the layers
```

**Hops:** axum task → worker task → writer thread → reply path. Three queue boundaries, three potential context switches per write.

### Stage 1: Writer Thread Core Pinning ✅ COMPLETED

**Implementation:**
- Added `core_affinity = "0.8"` dependency to `crates/server/Cargo.toml`
- Added `writer_core_affinity: bool` to `NodeConfig`, `StorageConfig`, and `MicroshardActor`
- When enabled, each shard's writer thread pins to `core_ids[xxh3_64(shard_uuid_bytes) % num_cores]`
- Configurable via `[storage].writer_core_affinity` in `cameodb.toml` (default: false)

**Benefits:**
- Improves cache locality for redb/tantivy data structures
- Reduces cross-core wakeups on the write hot path
- Zero behavioral change when disabled (default)

### Stage 2: Shard-Affine Worker Dispatch 🎯 PLANNED

**Goal:** Eliminate the cross-thread/cross-core hop between orchestrator worker and per-shard writer thread for write/search hot paths.

#### Architectural Variants

**Variant A — Shard-affine round-robin (minimal change)**
- Keep current pool topology but replace round-robin with hash-based dispatch
- ~30 LOC change, no runtime restructuring
- Shard ID must be known before dispatch (requires routing lookup before worker dispatch)
- Fall-through to neighboring workers on `Full` to preserve throughput
- Risk: one hot shard can saturate its worker queue

**Variant B — Worker-per-shard (1:1 mapping)**
- `worker_count = local_shards`
- Each worker owns exactly one shard's hot path
- Each worker is a single-threaded tokio runtime pinned to one core
- Pros: zero contention between shards, trivial co-location with writer
- Cons: multiple shards share cores when `local_shards > cpu_cores`, idle shards waste workers

**Variant C — Worker-per-core, shard-pinned (RECOMMENDED)**
- `worker_count = min(local_shards, cpu_cores - reserved)`
- Shard → worker via `xxh3(shard_id) % worker_count`
- Each worker = single-threaded tokio rt pinned to dedicated core
- Shard writer thread pinned to SAME core as its worker
- Read pool gets remaining cores
- This is the canonical "thread-per-core" model (à la Glommio/Seastar/ScyllaDB)

#### Recommended Architecture (Variant C)

**Core Partitioning:**
```
total_cores = N (e.g. 16)
reserved_cores = 2  (axum/kameo/coordinator/system tasks)
write_cores = min(local_shards, N - reserved_cores - read_cores_min)
read_cores = remaining

For N=16, local_shards=4:
  cores 0–1   → reserved (main runtime: axum, kameo, coordinator)
  cores 2–5   → write_cores (one per shard)
  cores 6–15  → read pool (10 threads)
```

**Component Topology:**

| Component | Runtime | Threads | Pinned to |
|---|---|---|---|
| axum + kameo + coordinator | main multi-thread rt | `reserved_cores` | reserved set |
| Orchestrator worker N | one single-thread rt **per shard** | 1 | `core[N + reserved]` |
| Writer thread for shard N | std::thread (existing) | 1 | same core as worker N |
| Read pool | dedicated multi-thread rt (existing) | rest | read core set |
| Tantivy merge threads | global pool | configurable | read or reserved set |

#### Implementation Plan

**Step 1 — Compute core layout once at startup**
- Add `CoreLayout` struct to `NodeOrchestrator` with `reserved`, `per_shard`, `read_pool` core vectors
- Read total via `core_affinity::get_core_ids`
- Partition by config: `storage.thread_per_core_mode = "auto"|"manual"|"disabled"`

**Step 2 — Replace generic worker pool with per-shard single-thread runtimes**
- Today: `tokio::spawn(async move { worker_loop })` on main rt
- New: spawn `PinnedWorker` struct with dedicated `current_thread` runtime per shard
- Use `on_thread_start` callback to pin each worker to its assigned core
- Use `current_thread` runtime because orchestrator worker does minimal CPU work (mostly routing + channel sends)

**Step 3 — Shard-aware dispatch**
- Replace `OrchestratorWorkerTx::try_send` round-robin with shard-affine routing
- Add `affinity_shard: Option<Uuid>` to `OrchestratorJob::Execute`
- For single-shard ops (Write, Read with routing key), compute routing key before dispatch
- For broadcast/scatter ops, use round-robin fallback

**Step 4 — Plumb shard_id through dispatch**
- Two approaches:
  - **Cheap**: Extract routing key in `try_send_affine` caller, compute ring lookup before dispatch
  - **Clean**: Add `affinity_shard: Option<Uuid>` to `OrchestratorJob::Execute`
- Recommended: Clean approach for maintainability

**Step 5 — Co-locate writer thread with its worker**
- Replace Stage 1 hash-based pinning with direct lookup from `CoreLayout`
- Use same hash as worker dispatch: `layout.shard_core(shard_id)`
- Guarantees: routing/validation/serialization (worker) + redb txn + tantivy commit (writer) all execute on same core

#### Edge Cases & Risks

1. **Broadcast/scatter-gather** — fan out across all workers; affinity hint is `None` (acceptable, rare)
2. **Dynamic shard creation** — worker assigned by hash already exists; writer pins to that core (no restructuring needed)
3. **Shard migration** — only affects new node; no issue
4. **`current_thread` runtime drawback** — only one task at a time on that core; mitigated by keeping validation cheap, spawning heavy work to read pool
5. **Tokio's `LocalSet` semantics** — `Runtime::spawn` works without `block_on` (we're fine)
6. **Backpressure semantics change** — affine routing exposes per-shard imbalance; expose per-worker queue depth as metric
7. **`indexer_num_threads > 1`** — extra tantivy indexer threads spill to other cores (user's choice)
8. **Tantivy merge threads** — currently global; recommend pinning to read core set (Stage 2.5)
9. **Memory ordering** — `Relaxed` on round-robin counter only used for non-affine ops (semantics unchanged)
10. **Test impact** — additive change, default `None` preserves current behavior

#### Configuration Surface

```toml
[storage]
writer_core_affinity = false              # Stage 1 — already done

[runtime]                                  # NEW section
thread_per_core_mode = "auto"             # "disabled" | "auto" | "manual"
reserved_cores = 2                        # for main rt (axum/kameo)
read_cores_min = 2                        # min cores for read pool
# manual override (only when mode = "manual"):
# reserved_cores_list = [0, 1]
# shard_cores_list   = [2, 3, 4, 5]
# read_cores_list    = [6, 7, 8, 9, 10, 11, 12, 13, 14, 15]
```

#### Phased Roll-Out

| Phase | Change | Risk | Rollback |
|---|---|---|---|
| **2a** | Shard-affine dispatch only (Variant A), keep multi-thread main rt | low | flag-gated via `runtime.thread_per_core_mode = "auto"` |
| **2b** | Per-shard `current_thread` workers, pinned, without changing main rt | medium | flag-gated |
| **2c** | Co-locate writer thread to worker's core (replace Stage-1 hash) | low | reverts to Stage 1 hash |
| **2d** | Pin Tantivy merge threads to read core set | low | unflagged |

#### Expected Impact

- **Per-write reduction**: 1 cross-core wakeup (router → worker) → 0 (worker is shard-affine)
- **Cache locality**: `Arc<HybridStore>`, `routing_ring`, `schema_cache` all stay hot on same core
- **Tail latency (p99)**: Significant improvement under heavy load due to reduced scheduling jitter
- **Throughput**: Modest improvement when CPU-bound on orchestrator side (currently I/O-bound on redb commits, gains smaller unless `wal_sync = false`)
- **Predictability**: Biggest win — SLO-driven workloads benefit from reduced jitter

### Recommended Execution Order

1. **Phase 2a** (highest ROI, lowest risk): Shard-affine routing in `OrchestratorWorkerTx`. ~50 LOC, additive, gated. Measure write p99 before/after.
2. **Phase 2c**: Co-locate writer pinning with worker hash. ~10 LOC change to existing Stage 1 code.
3. **Phase 2b**: Convert workers to pinned `current_thread` runtimes — only after 2a/2c validated. ~150 LOC + careful shutdown handling.
4. **Phase 2d**: Tantivy merge thread pinning — independent, can be done any time.

**Success Metrics:**
- Write p99 latency reduced by 20-40% under high concurrent load
- Cache miss rate reduced on shard-specific data structures
- No degradation in throughput for broadcast/scatter operations
- Clean rollback path via config flags at each phase

---
