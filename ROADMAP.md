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

## Phase 11: Read/Write Workflow Hot-Path Optimizations ✅ COMPLETED

**Implementation Summary:**
1. **Remove Tantivy ID roundtrip in search hits** ✅ — Direct extraction of stored `id` field values from Tantivy search results, eliminating per-hit JSON parse overhead.
2. **Tighten duplicate work inside `apply_batch()`** ✅ — Reuse schema and prepared document state; eliminate repeated shadow filtering and re-serialization.
3. **Enforce configured shard and remote concurrency limits** ✅ — Bounded concurrency in scatter-gather paths.
4. **Reduce worker-pool coordination contention** ✅ — Lower-contention queue design for hot-path workers.
5. **Improve early-termination and result-merge behavior** ✅ — Bounded top-K merging with score-aware pruning.
6. **Implement true end-to-end search streaming** ✅ — Incremental NDJSON streaming with backpressure-aware fan-in.
7. **Implement incremental write-stream ingestion** ✅ — Incremental NDJSON decoding with bounded ingestion.

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

## Phase 13 Stage 2a: Shard-Affine Worker Observability ✅ COMPLETED

**Implementation Summary:**
- **Per-worker atomic counters**: Added `WorkerCounters` struct with `queue_depth` (AtomicUsize) and `jobs_completed` (AtomicU64) to track per-worker queue state and throughput.
- **Dispatch-level counters**: Added `DispatchCounters` struct tracking `affine_sends`, `affine_full_fallbacks`, `round_robin_sends`, and `actor_mailbox_fallbacks` (all AtomicU64) to measure dispatch behavior.
- **Counter wiring**: Integrated counters into `OrchestratorWorkerTx::try_send` and `try_send_affine` to increment on send, and into `orchestrator_worker_loop` to decrement queue depth and increment jobs completed on receive.
- **Snapshot API**: Added `OrchestratorWorkerTx::snapshot()` method to generate `WorkerPoolReport` with per-worker stats (id, core_id, queue_depth, queue_capacity, jobs_completed) and dispatch metrics.
- **RouterActor integration**: Added `RouterActor::admin_worker_stats()` method to expose worker pool stats via direct method call (no kameo message routing needed for this admin endpoint).
- **HTTP endpoint**: Added `GET /_admin/workers` route and handler in `http_server.rs` returning JSON `WorkerPoolReport`.
- **Client SDK**: Added `admin_worker_stats()` method in `crates/client/src/sdk.rs` with corresponding response structs (`AdminWorkersResponse`, `WorkerStatsResponse`, `DispatchStatsResponse`).
- **CLI integration**: Added `AdminCommand::Workers` variant and dispatch handling in both command-line and interactive REPL modes, with tab-completion support and help text updates.

**Usage:**
- HTTP: `GET /_admin/workers` returns JSON with worker pool state and dispatch metrics
- CLI: `cameodb admin workers` displays the same stats in formatted JSON
- REPL: `admin workers` command in interactive shell

---

## Summary & Next Steps

### **Current Status**
- ✅ **Phases 1-9**: All completed and archived
- ✅ **Phase 10 (Field Projection)**: Completed
- ✅ **Phase 11 (Workflow Hot-Path Optimizations)**: All 7 steps completed
- ✅ **Phase 11.5 (Jemalloc Memory Management)**: Completed
- ✅ **Phase 12 (MCP Server Integration)**: Core tools, transport, and resources completed; security/docs/testing planned
- 🎯 **Phase 13 (Thread-Per-Core & Memory Ops)**: Stages 1, 2a, 2b, 2c, 2d, 2e completed; Stage 2f planned

### **Recommended Next Steps**
1. **Phase 13 Stage 2f**: Tantivy merge thread pinning + per-arena jemalloc stats
2. **Phase 12 remaining**: MCP security, streaming, integration tests

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

## Phase 13: Thread-Per-Core & Memory Operations 🎯 IN PROGRESS

**Objective**: Eliminate cross-core wakeups and cache thrashing on the write hot path, improve memory observability, and extract admin code into maintainable modules. Each stage is linear, flag-gated, and independently testable.

### Current Architecture Analysis

**Existing Threading Model:**
- **Tokio Async Runtimes (2 separate)**:
  - Main runtime: HTTP server (axum), kameo actors, orchestrator workers
  - Dedicated read runtime: `multi_thread` builder, threads named `cameodb-read`, threads = `config.search_threads` or `max(2, cpu_cores / 2)`

- **Orchestrator Worker Pool** (async, mailbox-bypass):
  - One `mpsc::channel::<OrchestratorJob>` per worker (not shared)
  - `worker_count = max(1, min(local_shards * 2, cpu_cores * 2))`
  - Dispatch is round-robin via `OrchestratorWorkerTx::try_send` (atomic counter, fall-through on Full)
  - Workers are tokio tasks on the main runtime — NOT pinned

- **Per-Shard Dedicated Writer Thread** (sync OS thread):
  - One OS thread per shard, named `writer-shard-<uuid>`
  - Receives `StorageCommand` over bounded `mpsc::channel` (capacity = 1024)
  - Implements write coalescing: blocks on first command, then `try_recv` drains up to 256 more
  - Strictly serializes writes per shard (required by redb single-writer semantics)

**Current Hot-Path Trace (Write):**
```
HTTP req on axum tokio worker (any core)
  → AppState::router.route_and_handle(op, ...)
  → OrchestratorWorkerTx::try_send (round-robin)      [atomic fetch_add]
  → Orchestrator worker tokio task on main rt (any core, may migrate)
  → engine.execute(op) → engine_write(...)
  → MicroshardActor.handle_write_via_channel
  → writer-shard-<uuid> OS thread (pinned in Stage 1)
  → reply via oneshot back across all the layers
```

---

### Stage 1: Writer Thread Core Pinning ✅ COMPLETED

- Added `core_affinity = "0.8"` dependency to `crates/server/Cargo.toml`
- Added `writer_core_affinity: bool` to `NodeConfig`, `StorageConfig`, and `MicroshardActor`
- When enabled, each shard's writer thread pins to `core_ids[xxh3_64(shard_uuid_bytes) % num_cores]`
- Configurable via `[storage].writer_core_affinity` in `cameodb.toml` (default: true)

---

### Stage 2a: Shard-Affine Worker Dispatch ✅ DONE

**Risk:** Low | **LOC:** ~80 | **Prerequisite:** None

**Goal:** Replace round-robin dispatch with shard-affine routing so that operations targeting the same shard always land on the same worker, reducing cross-core wakeups when writer pinning is enabled.

**Implementation:**
- Add `affinity_shard: Option<Uuid>` to `OrchestratorJob::Execute`
- Add `try_send_affine(&self, job, shard_id: Option<Uuid>)` to `OrchestratorWorkerTx`
  - When `shard_id` is `Some`, route to `workers[xxh3(shard_id) % worker_count]`
  - Fall through to neighboring workers on `Full` (preserve throughput)
  - When `shard_id` is `None` (broadcast/scatter), fall back to round-robin
- In `handle_client_op`, extract routing key from `ClientOp::Write` before dispatch
- Engine fast path: `engine_write` skips redundant `route_write` ring lookup when `affinity_shard` is `Some`
- Flag-gated via `shard_affine_dispatch` config, default `false` preserves round-robin behavior

**Expected Impact:**
- Eliminates 1 cross-core wakeup per write when writer pinning is enabled
- Cache locality: `Arc<HybridStore>`, `routing_ring`, `schema_cache` stay hot on same worker
- Zero impact on broadcast/scatter operations (round-robin fallback)

---

### Stage 2b: Extract Admin Memory Module ✅ COMPLETED

**Risk:** Low | **LOC:** ~200 (mostly move) | **Prerequisite:** None (independent of 2a)

**Goal:** Move memory-related types and functions out of the 6700-line `node_orchestrator.rs` into a dedicated module for maintainability and testability.

**Implementation:**
- Create `crates/server/src/admin/memory.rs` (new module)
- Move into it:
  - `ProcessMemoryStats`, `JemallocStats`, `AdminMemoryReport` structs
  - `read_process_memory_stats()` (all platform variants)
  - `read_jemalloc_stats()`, `call_memory_purge()`
  - `PurgeAdminMemory` message struct
- Add `pub mod admin;` to `main.rs` and `use` imports in `node_orchestrator.rs`
- No behavioral changes — pure refactoring

---

### Stage 2c: Per-Index Memory Stats ✅ COMPLETED

**Risk:** Low | **LOC:** ~5 | **Prerequisite:** Stage 2b

**Goal:** Add per-index memory visibility in the `/_indexes` response.

**2c.1 — Auto-Purge Timer:** ⏭️ SKIPPED
- Jemalloc's built-in `dirty_decay_ms` auto-release is working stably; no additional timer needed.

**2c.2 — Per-Index Memory in `/_indexes`:** ✅ COMPLETED
- Added `memory_mb` field to each index in the `list_indexes` response
- Derived from `redb_bytes + tantivy_bytes` per index (always present, not gated by `include_data_size`)
- Helps operators identify bloated indexes without hitting `/_admin/memory`

---

### Stage 2d: Co-Locate Writer Pinning with Worker Hash ✅ DONE

**Risk:** Low | **LOC:** ~15 | **Prerequisite:** Stage 2a

**Goal:** Ensure the writer thread for shard X uses the same hash bucket as the worker that handles shard X's operations.

**Implementation (delivered):**
- In `NodeOrchestrator::spawn_worker_pool`, when `shard_affine_dispatch && writer_core_affinity` are both enabled, force `worker_count = cpu_cores`.
- This makes `xxh3(shard_id) % worker_count == xxh3(shard_id) % num_cores`, so for any shard S: the worker handling S dispatches into the writer pinned on the matching core.
- Tokio worker tasks aren't OS-pinned, but the scheduler keeps frequently-running tasks near their last core under sustained load — co-locating dispatch with writer thread on the same hash bucket maximizes that locality.
- Behind a config gate: default behavior (either flag off) preserves the existing `min(local_shards * 2, cpu_cores * 2)` worker sizing.

**Deferred to Stage 2e:**
- Explicit `CoreLayout` struct (`reserved`, `per_shard`, `read_pool` cores) becomes valuable only when workers are pinned as dedicated OS threads with single-thread runtimes (Stage 2e). For pure hash alignment, the implicit `% cpu_cores` math is sufficient.

---

### Stage 2e: Per-Worker Single-Thread Runtimes ✅ DONE

**Risk:** Medium | **LOC:** ~70 | **Prerequisite:** Stages 2a + 2d

**Goal:** Convert workers from `tokio::spawn` on main runtime to dedicated `current_thread` runtimes pinned per core — completing the thread-per-core model for the write hot path.

**Implementation (delivered):**
- Extracted worker body into `orchestrator_worker_loop` helper (one body, two spawn paths).
- New config flag `[storage].worker_core_affinity` (default: `false`). Requires `shard_affine_dispatch` AND `writer_core_affinity` to take effect; otherwise silently no-op.
- When all three flags are on, `spawn_worker_pool`:
  - Sizes `worker_count = num_cores` (inherited from Stage 2d alignment).
  - Spawns each worker as a dedicated `std::thread::Builder` thread named `orch-worker-N`.
  - Pins the OS thread to `core_ids[worker_id % num_cores]` via `core_affinity::set_for_current`.
  - Runs an isolated `tokio::runtime::Builder::new_current_thread()` runtime with `max_blocking_threads(4)` (kept tiny because search delegates to the shared `read_runtime` and writes go through the pinned writer thread).
  - Falls back gracefully on macOS / when pinning fails (logged, runs unpinned on a dedicated thread).
- `NodeOrchestrator.worker_threads: Vec<std::thread::JoinHandle<()>>` stores handles; `shutdown_worker_pool` sends shutdown messages then joins them via `spawn_blocking`.

**Why minimal:**
- No new `[runtime]` config section — just one boolean. Reserved-core layout (`CoreLayout` from the original plan) deferred until a concrete need.
- No changes to `OrchestratorJob`, `OrchestratorWorkerTx`, `OrchestratorEngine`, `RouterActor`, `MicroshardActor`, or `engine.execute()` body — they work identically across both runtimes.
- The shared `read_runtime` continues handling all heavy I/O, preserving search throughput.

**Wakeup math:**
- Default mode: router-task → mpsc → worker-task → channel → writer-thread (cross-core wakeup if worker scheduled away from writer's pinned core).
- Pinned mode: router-task → mpsc cross-runtime → worker-thread (pinned core C) → channel → writer-thread (pinned core C) — second hop becomes a same-core mpsc push (no wakeup syscall). Cache locality wins for schema cache, routing ring, and shard map.

**Edge cases handled:**
1. Broadcast/scatter — `affinity_shard = None`, falls through to round-robin send across pinned workers.
2. Dynamic shard creation — workers already cover all cores; hash determines the new shard's worker.
3. `current_thread` runtime — fine because the worker only awaits channels and delegates blocking work elsewhere.
4. Shutdown — JoinHandles ensure runtimes drop before the orchestrator returns.

---

### Stage 2f: Tantivy Merge Thread Pinning & Per-Arena Jemalloc Stats 🎯 PLANNED

**Risk:** Low–Medium | **LOC:** ~80 | **Prerequisite:** Stage 2e

**2f.1 — Tantivy Merge Thread Pinning:**
- Pin Tantivy merge threads to the read core set to avoid interfering with writer threads
- Currently merge threads run unbounded; with `merge_num_threads: 1` they still run on whatever core the OS picks

**2f.2 — Per-Arena Jemalloc Stats:**
- `read_jemalloc_stats()` currently reads global stats only
- With `percpu_arena:percpu`, expose per-arena stats via `mallctl("arena.i.allocated", ...)` and `mallctl("arena.i.resident", ...)`
- Useful for diagnosing which shard/core is consuming the most memory
- Requires Stage 2e (CoreLayout) to map arena IDs to shard/core

---

### Phase 13 Execution Order & Risk Matrix

| Order | Stage | Risk | LOC | Prerequisite | Gain |
|-------|-------|------|-----|-------------|------|
| **1** | 2a: Shard-affine dispatch | Low | ~50 | None | Eliminates 1 cross-core wakeup/write |
| **2** | 2b: Extract memory module | Low | ~200 | None | Maintainability, testability |
| **3** | 2c: Auto-purge + per-index memory | Low | ~70 | 2b | Operational safety, observability |
| **4** | 2d: Co-locate writer pinning | Low | ~10 | 2a | Full core co-location |
| **5** | 2e: Per-shard single-thread rt | Medium | ~150 | 2a+2d | True thread-per-core |
| **6** | 2f: Merge pinning + per-arena stats | Low–Med | ~80 | 2e | Reduced interference, diagnostics |

**Success Metrics:**
- Write p99 latency reduced by 20-40% under high concurrent load
- Cache miss rate reduced on shard-specific data structures
- No degradation in throughput for broadcast/scatter operations
- Clean rollback path via config flags at each stage
- Memory module independently testable with unit tests
- Auto-purge prevents RSS creep under sustained writes

---
