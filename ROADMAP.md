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
- ✅ **Phase 12 (MCP Server Integration)**: Core tools, transport, resources, and query syntax docs completed; security moved to Phase 14, streaming/docs/testing planned
- 🎯 **Phase 13 (Thread-Per-Core & Memory Ops)**: Stages 1, 2a, 2b, 2c, 2d, 2e completed; Stage 2f partially done (merge thread count control implemented via `IndexWriterOptions`; core pinning and per-arena stats planned)
- 🔒 **Phase 14 (Security Hardening)**: Stage A3 (TLS bypass removal) and B2 (HTTPS/TLS) completed; A1, A2, A4, A5, B1, B3, C1–C3 planned

### **Recommended Next Steps**
1. **Phase 14 Stage A1**: Index name validation (critical security gap)
2. **Phase 14 Stage A2**: Wire CORS config from `cors_allowed_origins`
3. **Phase 13 Stage 2f**: Tantivy merge thread core pinning + per-arena jemalloc stats
4. **Phase 12 remaining**: MCP streaming, documentation, integration tests

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
     - Tool description includes full Tantivy query syntax quick reference and field-type operator matrix
   - **`search_indexes`**: Federated search across multiple indexes
     - Parameters: `indexes[]`, `query`, `limit`
     - Returns: Combined results with `_index_source` metadata and per-index field projection
   - **`get_index`**: Retrieve schema and statistics for a single index
     - Parameters: `index`
     - Returns: Complete field definitions, types, document count, size
   - **`validate_query`**: Field-type-aware CameoDB query syntax validation, unknown field detection, structural checks (quotes/parens), fuzzy "did you mean" suggestions, and full syntax reference with agent pro tips
   - **`get_index_stats`**: Document counts, field distributions, aggregated stats for single or all indexes
   - **`list_indexes`**: Enumerate all available indexes with schemas
     - Parameters: none
     - Returns: All index schemas with metadata (leverages existing `/_indexes` endpoint)
   - **MCP README** (`crates/mcp/README.md`): Full query syntax reference with operator examples and field-type compatibility table

5. **Advanced MCP Features** ✅ COMPLETED
   - **Field Projection**: Auto-suggest relevant fields based on partial input
   - All tools include `title`, property `description`s, and `annotations` (`readOnlyHint`, `openWorldHint`) per MCP draft spec
   - **Streaming Support**: 📋 PLANNED — Large result sets via MCP streaming protocol
   - **Semantic Routing**: 📋 PLANNED — Auto-select best index(es) for query intent

6. **MCP Resource Providers** ✅ COMPLETED
   - Expose indexes as MCP resources for exploration
   - Provide schema documentation as resources
   - Enable agents to discover available datasets dynamically

7. **Security & Access Control** ➡️ MOVED to Phase 14
   - Authentication, authorization, TLS, and hardening are tracked as a dedicated
     security project — see **Phase 14: Security Hardening** below.
   - MCP-specific security (rate limiting, query complexity, audit logging) is
     covered under Phase 14 Stage C once the core auth layer exists.

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

## Phase 13: Thread-Per-Core & Memory Operations 🎯 NEARLY COMPLETE

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

### Stage 2f: Tantivy Merge Thread Pinning & Per-Arena Jemalloc Stats 🎯 PARTIALLY DONE

**Risk:** Low–Medium | **LOC:** ~80 | **Prerequisite:** Stage 2e

**2f.1 — Tantivy Merge Thread Control:** ✅ COMPLETED
- Merge thread count is now configurable via `StorageConfig.merge_num_threads` (default: 1)
- Implemented via `tantivy::indexer::IndexWriterOptions::builder()` with explicit `num_merge_threads()`
- Replaces Tantivy's default of 4 merge threads, preventing mmap storms on memory-constrained nodes

**2f.2 — Tantivy Merge Thread Core Pinning:** 📋 PLANNED
- Pin merge threads to the read core set to avoid interfering with writer threads
- Currently merge threads run on whatever core the OS picks even with `merge_num_threads: 1`

**2f.3 — Per-Arena Jemalloc Stats:** 📋 PLANNED
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
| **6** | 2f: Merge control + pinning + per-arena stats | Low–Med | ~80 | 2e | Reduced interference, diagnostics (2f.1 done; 2f.2+2f.3 planned) |

**Success Metrics:**
- Write p99 latency reduced by 20-40% under high concurrent load
- Cache miss rate reduced on shard-specific data structures
- No degradation in throughput for broadcast/scatter operations
- Clean rollback path via config flags at each stage
- Memory module independently testable with unit tests
- Auto-purge prevents RSS creep under sustained writes

---

## Phase 14: Security Hardening 🔒 IN PROGRESS

**Objective**: Close the security gaps identified in the code security review (2026-07-30). The remaining critical gap is that CameoDB has **no authentication and no authorization** — every HTTP and MCP endpoint is open. TLS (B2), index-name validation (A1), and CORS wiring (A2) are done. This phase turns CameoDB from a trusted-LAN-only system into one that can be safely exposed to untrusted networks.

**Current state (verified by audit):**
- ✅ No hardcoded secrets, no command execution, no regex/ReDoS surface, no SSRF
- ✅ libp2p cluster transport already uses Noise encryption
- ⚠️ All HTTP/MCP endpoints unauthenticated (write, delete, admin included)
- ✅ Index names validated at creation and resolved through `HybridStore::index_dir()`, which rejects any name that is not a single path component (Stage A1)
- ✅ `cors_allowed_origins` wired into the router with fail-fast config validation; default remains `["*"]` (Stage A2)
- ✅ TLS on HTTP implemented via rustls (Stage B2); default bind `0.0.0.0:9480` still plaintext unless explicitly enabled
- ⚠️ No cluster join authentication (any reachable node can join)
- ⚠️ No rate limiting / concurrency caps; 576MB default body limit + decompression layer = memory DoS vector
- ✅ `CAMEODB_ACCEPT_INVALID_CERTS` removed entirely; replaced with per-command `--insecure` flag

### Execution Order (impact-per-effort ranked)

| Order | Stage | Effort | Impact | Risk if unfixed |
|-------|-------|--------|--------|-----------------|
| **1** | A1: Index name validation | ✅ Done | Critical | Arbitrary dir deletion (RCE-adjacent) |
| **2** | A2: CORS config wiring | 🟡 Partial | High | Drive-by browser attacks on local instances |
| **3** | A3: `ACCEPT_INVALID_CERTS` removal | ✅ Done | Medium | Accidental TLS bypass |
| **4** | A4: Body limits + concurrency caps | ~1 day | High | Memory DoS / decompression bomb |
| **5** | A5: CI security tooling (`cargo audit`, `cargo deny`) | ~2 hrs | Medium | Silent vulnerable deps |
| **6** | B1: API key authentication | ~3–5 days | Critical | Full unauthenticated R/W/D access |
| **7** | B2: HTTPS/TLS via rustls | ✅ Done | High | Traffic interception |
| **8** | B3: Cluster join secret (PSK) | ~2–3 days | High | Rogue node data access |
| **9** | C1: MCP rate limiting + query complexity | ~2 days | Medium | Agent-driven resource exhaustion |
| **10** | C2: Audit logging | ~2 days | Medium | No forensic trail |
| **11** | C3: Index-level authorization (RBAC) | ~5+ days | Medium | Multi-tenant isolation |

### Stage A: Quick Wins (no protocol changes)

**A1 — Index Name Validation** ✅ COMPLETED
- Two-tier approach at the HTTP boundary (`http_server.rs`):
  1. **Index creation** (`PUT /api/{index}/_config`): `validate_index_name()` rejects `..`, path separators, empty, length > 255, non-alphanumeric first character, and anything outside `[A-Za-z0-9_.-]`. This is the only route where a new name enters the system.
  2. **Delete** (`DELETE /api/{index}`): requires the index to exist; returns 404 when absent and 500 when the lookup itself fails
- Defense-in-depth at the storage boundary: `HybridStore::index_dir()` resolves every caller-supplied name and rejects anything that is not a single normal path component. The check is **lexical**, not `canonicalize()`-based, so it also holds for indexes that do not exist yet — the case where a traversal name would otherwise reach `create_dir_all` and escape the shard. Applied to `get_or_create_index` (creates dirs), `delete_index_data` (removes dirs, validated before any mutation), and both `Index::open_in_dir` slow paths.
- Tests: 7 unit tests on `validate_index_name`, 3 on `resolve_index_dir`, plus an end-to-end test that drives the real write and delete paths with `../victim`, `..`, `../../etc`, and `a/b` and asserts nothing outside the shard is created or removed

**A2 — Wire CORS Config** � PARTIALLY COMPLETED
- ✅ Replaced hardcoded `CorsLayer::permissive()` with origins from `network.http.cors_allowed_origins`, threaded through `create_router`
- ✅ Explicit methods (`GET/POST/PUT/PATCH/DELETE`) and headers (`Content-Type`, `Authorization`) for the non-wildcard path
- ✅ Credentials are never combined with a wildcard origin (`permissive()` does not set them)
- ✅ Fail-fast validation in `CameoDbConfig::validate()`: rejects an empty list, `"*"` mixed with specific origins, origins that are not valid header values, and origins without a scheme — a typo can no longer degrade silently into deny-all
- ✅ Effective policy is logged at startup (`warn!` for wildcard, `info!` with the origin list otherwise)
- 📋 **Remaining**: default is still `["*"]`, so out of the box the server is permissive. Defaulting to loopback origins when `bind_address` is local is a behavioural change still to be made.

**A3 — TLS Bypass Handling** ✅ COMPLETED
- Removed `CAMEODB_ACCEPT_INVALID_CERTS` environment variable entirely
- Replaced with `--insecure` flag: per-command for single operations, per-session for interactive REPL
- No global TLS bypass via environment variables; must be explicitly requested via CLI flag

**A4 — DoS Hardening** ✅ COMPLETED
- ✅ Lowered default `max_record_size_mb` from 512MB → 64MB; all derived limits (HTTP body, Kameo remote messaging, request timeout) scale accordingly
- ✅ Added `max_concurrent_requests` to `HttpConfig` (default: 128) with CLI/env override (`--max-concurrent-requests` / `CAMEODB_MAX_CONCURRENT_REQUESTS`); semaphore-based concurrency guard middleware rejects excess requests with HTTP 503
- ✅ Fixed `DefaultBodyLimit` ordering: decompressed body limit is applied *after* `DecompressionLayer` (inner) so compression bombs are caught; a second `DefaultBodyLimit` *before* decompression (outer) caps raw wire bytes
- ✅ Config validation rejects `max_concurrent_requests = 0`
- ✅ Startup log prints effective concurrency limit; example config updated

**A5 — CI Security Tooling** 🟡 PARTIALLY COMPLETED
- ✅ `cargo audit` installed (v0.22.2), runs clean — 0 vulnerabilities across 588 dependencies
- ✅ `cargo-deny` installed (v0.20.2) with `deny.toml` covering advisories, bans (wildcard deny, duplicate warn), licenses (permissive allowlist, copyleft deny), and sources (crates.io only)
- ✅ Fixed wildcard path dependencies in `server` and `client` Cargo.toml (added explicit version constraints)
- ✅ Fixed unparseable `FSL-1.1-Apache-2.0` license fields → `Apache-2.0` (valid SPDX; actual FSL license file remains in repo)
- ✅ Documented 3 transitive advisories from libp2p 0.56.0 (hickory-proto vulnerabilities + unmaintained `paste`) with ignore reasons — no upstream fix available yet
- 📋 **Remaining**: Create GitHub Actions CI workflow (`.github/workflows/ci.yml`) running `cargo audit`, `cargo deny check`, `cargo clippy -- -D warnings`, and `cargo test`; add Dependabot/Renovate config

### Stage B: Core Auth & Transport Security (the "auth project")

**B1 — API Key Authentication with Role-Based Access** 🔴 CRITICAL
- Design: `Authorization: Bearer <key>` middleware (axum `from_fn`) covering all `/api/*`, `/_admin/*`, `/_indexes`, `/mcp/*` routes; `/_cluster/health` stays open for load balancers
- Three roles per API key:
  - **Admin**: Full access — all routes including `/_admin/*` (memory, workers, purge), destructive operations (delete index, schema evolution), and all user/restricted operations
  - **User**: Index-level CRUD — search, write, bulk ingest, schema read, index listing; no `/_admin/*` routes, no destructive operations (delete index, schema modification)
  - **Restricted**: Read-only MCP access — `search_index`, `search_indexes`, `get_index`, `list_indexes`, `get_index_stats`, `validate_query`; designed for AI agents querying knowledge bases
- Config:
  ```toml
  [security]
  # Auth disabled by default (trusted-LAN mode)
  enabled = false

  [[security.api_keys]]
  key = "cameo-admin-xxx"        # SHA-256 hashed, constant-time compare
  role = "admin"
  label = "ops-team"             # optional human-readable label for audit log

  [[security.api_keys]]
  key = "cameo-user-xxx"
  role = "user"
  label = "data-engineering"

  [[security.api_keys]]
  key = "cameo-agent-xxx"
  role = "restricted"
  label = "claude-desktop"
  allowed_indexes = ["docs", "wiki"]  # optional index allow-list for restricted keys
  ```
- Env override: `CAMEODB_API_KEY` for single-key deployments (role is always resolved server-side from the key's config entry)
- Backward compat: auth disabled by default; when enabled, fail-fast at startup if `bind = 0.0.0.0` without auth configured
- Client SDK + CLI: `--api-key` flag, `CAMEODB_API_KEY` env, persisted per-connection in REPL
- Route-level enforcement matrix:

  | Route group | Admin | User | Restricted |
  |-------------|-------|------|------------|
  | `/_admin/*` (memory, workers, purge, commit, evict) | ✅ | ❌ | ❌ |
  | `PUT /api/{index}/_config` (create index) | ✅ | ❌ | ❌ |
  | `DELETE /api/{index}` (delete index) | ✅ | ❌ | ❌ |
  | `PATCH /api/{index}/_schema` (schema evolution) | ✅ | ❌ | ❌ |
  | `POST /api/{index}/write` (write) | ✅ | ✅ | ❌ |
  | `POST /api/{index}/bulk` (bulk ingest) | ✅ | ✅ | ❌ |
  | `GET /api/{index}/search` (search) | ✅ | ✅ | ✅ |
  | `GET /_indexes` (list indexes) | ✅ | ✅ | ✅ |
  | `/mcp/*` (MCP tools) | ✅ | ✅ | ✅ (respects `allowed_indexes`)

**B2 — HTTPS/TLS via rustls** ✅ COMPLETED
- Implemented axum-server with rustls for HTTPS support; config `[network.http.tls] enabled, cert_file, key_file`
- Added TLS validation to config (cert/key file existence, required fields when enabled)
- Client-side: added `--insecure` flag for accepting invalid TLS certificates (self-signed certs in development)
- Per-command `--insecure` for remote schema/data loading operations (fine-grained control)
- Removed `CAMEODB_ACCEPT_INVALID_CERTS` environment variable (simplified to flag-only interface)
- Documentation updated with TLS configuration, Linux system certificate paths, and security best practices
- Keep `native-tls-vendored` for musl static builds regardless (OpenSSL vendored works fine there)
- Optional mTLS for client verification later

**B3 — Cluster Join Authentication** ✅ COMPLETED
- PSK for libp2p swarm via `pnet` (XSalsa20 private network encryption)
- Config `[network.cluster] psk` (inline hex string) and `psk_file` (path to file)
- CLI overrides: `--cluster-psk`, `--cluster-psk-file`; env: `CAMEODB_CLUSTER_PSK`, `CAMEODB_CLUSTER_PSK_FILE`
- When PSK is set, TCP is wrapped with PnetConfig and QUIC is disabled (pnet only supports TCP)
- PSK fingerprint logged at startup (not the key itself) for operational verification
- Config validation: warns if cluster enabled without PSK; validates hex format (64 chars = 32 bytes)
- Covers kameo remote messaging (all libp2p protocols are gated by the pnet handshake)
- Disabled by default (backward compatible); opt-in for production clusters
- Future: PSK rotation with primary + secondary for zero-downtime rolling upgrades

### Stage C: Defense in Depth (post-auth)

**C1 — MCP-Specific Limits** 🟡 MEDIUM
- Rate limiting per session/key on MCP tool invocations (especially important for restricted keys used by AI agents)
- Query complexity caps: max boolean clauses, max prefix-expansion terms, per-request timeout already exists — wire it into MCP path
- Index allow-list for restricted keys already covered in B1 (`allowed_indexes` per key)

**C2 — Audit Logging** 🟡 MEDIUM
- Structured `tracing` events: who (key id / peer), what (op, index), when, result
- Append-only audit ring buffer + optional file sink; admin endpoint to query recent events

**C3 — Per-Index Role Refinement (RBAC)** 🟢 LOWER (needed for multi-tenant)
- Extend B1's three-role model with per-index role overrides: a key with `role = "user"` could be granted `"restricted"` (read-only) on sensitive indexes
- Enforced at `RouterActor` boundary so local + remote paths are both covered
- Depends on B1 identity model and route-level enforcement

### TLS Inventory (verified 2026-08-03)

| Component | Current TLS | Notes |
|-----------|-------------|-------|
| HTTP server | ✅ rustls via axum-server | Implemented with `[network.http.tls]` config (enabled, cert_file, key_file) |
| Client SDK (`reqwest 0.13`) | ✅ default `native-tls`; features: `native-tls`, `native-tls-vendored`, `rustls-tls` | Added `--insecure` flag for accepting invalid TLS certificates (self-signed certs) |
| musl static builds | ✅ `native-tls-vendored` via `scripts/build/build-musl.sh` + `build-dist.sh` | keep as-is |
| libp2p cluster transport | ✅ Noise (`noise::Config`) + yamux mux | encrypted but unauthenticated membership; Stage B3 adds PSK |
| kameo remote messaging | ⚠️ rides libp2p swarm | inherits B3 protection |
| Client TLS bypass | ✅ `--insecure` flag only | Removed `CAMEODB_ACCEPT_INVALID_CERTS` env var; per-command or per-session (interactive REPL) via CLI flag |

**Success Metrics:**
- No unauthenticated write/delete path reachable in default config
- Path-traversal regression tests in CI
- `cargo audit` green in CI
- TLS + auth enabled = zero plaintext credentials on the wire
- Cluster rejects unknown peers without valid PSK

---
