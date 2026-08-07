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
- 🔒 **Phase 14 (Security Hardening)**: A1–A5, B2, B3 completed and verified by `scripts/validate/`; posture presets added (`local` / `internal` / `external`); B1 (authentication) remains the open critical gap, design agreed 2026-08-08 and ready to implement; C1–C3 planned, C3 shrunk because B1 absorbs index scoping

### **Recommended Next Steps**
1. **Phase 14 Stage B1**: API key authentication + index scoping — the last critical gap, and what the `external` profile is waiting on
2. **Phase 13 Stage 2f**: Tantivy merge thread core pinning + per-arena jemalloc stats
3. **Phase 12 remaining**: MCP streaming, documentation, integration tests
4. **Phase 14 Stage C1–C3**: MCP rate limiting, audit logging, per-index role overrides (all depend on B1)

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
- ⚠️ All HTTP/MCP endpoints unauthenticated (write, delete, admin included) — the one remaining critical gap. `/_admin/*` can now be removed entirely with `admin_enabled = false`, and the `external` profile refuses to start until B1 lands
- ✅ Index names validated at creation and resolved through `HybridStore::index_dir()`, which rejects any name that is not a single path component (Stage A1)
- ✅ `cors_allowed_origins` wired into the router with fail-fast validation; default is now `[]` (no cross-origin access) and `"*"` is local-only (Stage A2)
- ✅ TLS on HTTP via rustls (Stage B2), verified serving; default bind is now `127.0.0.1:9480` and a reachable bind requires a declared security profile
- ✅ Cluster join gated by an optional PSK; required by the `internal` and `external` profiles
- ✅ Wire-level body limit, per-record cap, request timeout, and concurrency shedding, all verified live by `scripts/validate/posture.sh`
- ✅ `CAMEODB_ACCEPT_INVALID_CERTS` removed entirely; replaced with per-command `--insecure` flag

### Execution Order (impact-per-effort ranked)

| Order | Stage | Effort | Impact | Risk if unfixed |
|-------|-------|--------|--------|-----------------|
| **1** | A1: Index name validation | ✅ Done | Critical | Arbitrary dir deletion (RCE-adjacent) |
| **2** | A2: CORS config wiring | ✅ Done | High | Drive-by browser attacks on local instances |
| **3** | A3: `ACCEPT_INVALID_CERTS` removal | ✅ Done | Medium | Accidental TLS bypass |
| **4** | A4: Body limits + concurrency caps | ✅ Done | High | Memory DoS / decompression bomb |
| **5** | A5: Security tooling (`cargo audit`, `cargo deny`) | ✅ Done (manual) | Medium | Silent vulnerable deps |
| **6** | B1: API key authentication + index scoping | ~1–2 weeks | Critical | Full unauthenticated R/W/D access |
| **7** | B2: HTTPS/TLS via rustls | ✅ Done | High | Traffic interception |
| **8** | B3: Cluster join secret (PSK) | ✅ Done | High | Rogue node data access |
| **9** | C1: MCP rate limiting + query complexity | ~2 days | Medium | Agent-driven resource exhaustion |
| **10** | C2: Audit logging | ~2 days | Medium | No forensic trail |
| **11** | C3: Per-index role overrides | ~2 days (was ~5+) | Medium | Multi-tenant isolation |

The B1 estimate is up from the original ~3–5 days for two reasons, both decided deliberately
(see B1 below): index scoping applies to **every** role rather than read-only keys, and MCP
enforcement reaches per-tool and per-index rather than stopping at the path. The second is
why C3 drops — most of what it described is B1's scoping mechanism, leaving only per-index
*overrides* on top of it.

### Stage A: Quick Wins (no protocol changes)

**A1 — Index Name Validation** ✅ COMPLETED
- Two-tier approach at the HTTP boundary (`http_server.rs`):
  1. **Index creation** (`PUT /api/{index}/_config`): `validate_index_name()` rejects `..`, path separators, empty, length > 255, non-alphanumeric first character, and anything outside `[A-Za-z0-9_.-]`. This is the only route where a new name enters the system.
  2. **Delete** (`DELETE /api/{index}`): requires the index to exist; returns 404 when absent and 500 when the lookup itself fails
- Defense-in-depth at the storage boundary: `HybridStore::index_dir()` resolves every caller-supplied name and rejects anything that is not a single normal path component. The check is **lexical**, not `canonicalize()`-based, so it also holds for indexes that do not exist yet — the case where a traversal name would otherwise reach `create_dir_all` and escape the shard. Applied to `get_or_create_index` (creates dirs), `delete_index_data` (removes dirs, validated before any mutation), and both `Index::open_in_dir` slow paths.
- Tests: 7 unit tests on `validate_index_name`, 3 on `resolve_index_dir`, plus an end-to-end test that drives the real write and delete paths with `../victim`, `..`, `../../etc`, and `a/b` and asserts nothing outside the shard is created or removed

**A2 — Wire CORS Config** ✅ COMPLETED
- ✅ Replaced hardcoded `CorsLayer::permissive()` with origins from `network.http.cors_allowed_origins`, threaded through `create_router`
- ✅ Explicit methods (`GET/POST/PUT/PATCH/DELETE`) and headers (`Content-Type`, `Authorization`) for the non-wildcard path
- ✅ Credentials are never combined with a wildcard origin (`permissive()` does not set them)
- ✅ Fail-fast validation in `CameoDbConfig::validate()`: rejects an empty list, `"*"` mixed with specific origins, origins that are not valid header values, and origins without a scheme — a typo can no longer degrade silently into deny-all
- ✅ Effective policy is logged at startup (`warn!` for wildcard, `info!` with the origin list otherwise)
- ✅ Default is now `[]` — no cross-origin browser access. CORS governs browsers only, so this costs API and MCP clients nothing while removing the drive-by surface that mattered precisely because no endpoint requires auth
- ✅ `"*"` is accepted only under the `local` profile; `internal` and `external` reject it
- ✅ `mcp-session-id` and `accept` are allowed request headers and `mcp-session-id` is exposed, so restricting origins no longer breaks browser-based MCP clients — a collision between this stage and Phase 12 that the original change introduced

**A3 — TLS Bypass Handling** ✅ COMPLETED
- Removed `CAMEODB_ACCEPT_INVALID_CERTS` environment variable entirely
- Replaced with `--insecure` flag: per-command for single operations, per-session for interactive REPL
- No global TLS bypass via environment variables; must be explicitly requested via CLI flag

**A4 — DoS Hardening** ✅ COMPLETED (re-done; first attempt did not hold)
- ✅ Lowered default `max_record_size_mb` from 512MB → 64MB; all derived limits (HTTP body, Kameo remote messaging, request timeout) scale accordingly
- ✅ Added `max_concurrent_requests` to `HttpConfig` (default: 128) with CLI/env override (`--max-concurrent-requests` / `CAMEODB_MAX_CONCURRENT_REQUESTS`); semaphore-based concurrency guard middleware rejects excess requests with HTTP 503
- ✅ `DefaultBodyLimit` after `DecompressionLayer` so compression bombs are measured expanded
- ✅ `RequestBodyLimitLayer` counts bytes on the wire. **The earlier claim that a second `DefaultBodyLimit` capped raw wire bytes was wrong**: `DefaultBodyLimit` is an extractor-level limit, so handlers taking a raw `Body` — the NDJSON streaming ingest path — were unbounded. A 150 MB single-line request under a 1 MB configured limit was accepted and drove RSS from 44 MB to 889 MB
- ✅ Per-record cap inside `write_stream_handler`: an unterminated line can no longer buffer the whole request allowance
- ✅ `TimeoutLayer` wired to `effective_request_timeout_secs()`. **`request_timeout_secs` was previously never applied to HTTP at all**, so the concurrency guard made a DoS *cheaper*: four trickle uploads at 300 B/s held every permit indefinitely and took the node offline, health check included
- ✅ `/_cluster/health` exempted from the concurrency guard; 503 responses carry `Retry-After`
- ✅ Config validation rejects `max_concurrent_requests = 0`; posture rules bound concurrency × body size jointly
- ✅ Verified by `scripts/validate/posture.sh` (413 on both limit paths, 408 at the configured timeout, health available while saturated)

**A5 — Security Tooling** ✅ COMPLETED (manual, by design)
- ✅ `cargo audit` installed (v0.22.2), runs clean — 0 vulnerabilities across 588 dependencies
- ✅ `cargo-deny` installed (v0.20.2) with `deny.toml` covering advisories, bans (wildcard deny, duplicate warn), licenses (permissive allowlist, copyleft deny), and sources (crates.io only)
- ✅ Fixed wildcard path dependencies in `server` and `client` Cargo.toml (added explicit version constraints)
- ✅ Fixed unparseable `FSL-1.1-Apache-2.0` license fields → `Apache-2.0` (valid SPDX; actual FSL license file remains in repo)
- ✅ Documented 3 transitive advisories from libp2p 0.56.0 (hickory-proto vulnerabilities + unmaintained `paste`) with ignore reasons — no upstream fix available yet
- ✅ `scripts/validate/deps.sh` runs `cargo fmt --check`, `cargo clippy -D warnings`, `cargo audit`, and `cargo deny check`
- ✅ Advisory exceptions carry `review-by` dates; the script fails once one expires, so an exception cannot quietly outlive its justification
- ✅ Added `CDLA-Permissive-2.0` to the licence allowlist (Mozilla CA bundle via `rustls-platform-verifier`), reviewed as a permissive data licence
- **No CI by decision.** Execution is manual; [RELEASE-CHECKLIST.md](RELEASE-CHECKLIST.md) is the record

### Stage B: Core Auth & Transport Security (the "auth project")

**B1 — API Key Authentication with Capability and Index Scoping** 🔴 CRITICAL

Design agreed 2026-08-08. This replaces an earlier sketch whose route matrix did not match
the router that exists — it named `POST /api/{index}/write`, `POST /api/{index}/bulk`, and
`GET /api/{index}/search`, none of which are real paths, and omitted four routes entirely
including the streaming ingest path that Stage A4 had already had to fix once. The table
below is transcribed from `create_router` and is guarded by a test rather than by review.

*Capabilities, not roles, are what routes require.* Roles are bundles of capabilities, so
the route table stays role-agnostic and C3 can add per-index overrides without touching it.

| Capability | Covers |
|------------|--------|
| `Read` | search, streaming search, read config, list indexes |
| `Write` | document write, streaming ingest, bulk |
| `IndexAdmin` | create index, schema evolution, delete index |
| `NodeAdmin` | `/_admin/*` — memory, purge, workers, commit, evict-writer |

`admin` = all four · `writer` = Read + Write · `reader` = Read.

Renamed from the earlier `user` / `restricted`: those two were not on the same axis, and
"restricted" was described as read-only *MCP* access while the same sketch also granted it
HTTP search. Nothing has shipped with the old names.

- **Transport**: `Authorization: Bearer <key>`, header-only. A key in a query parameter is a
  non-goal — it lands in access logs and `Referer` headers.
- **Config** — entry-level `key_hash` or `key_hash_file`, the exact `psk` / `psk_file`
  analogue from B3: inline wins, the file is permission-checked, world-readable warns.
  ```toml
  [security]
  enabled = false                        # off by default; the posture rules decide if that is allowed

  [[security.api_keys]]
  key_hash = "sha256:3f9a…"              # or: key_hash_file = "/etc/cameodb/keys/ops"
  role  = "admin"
  label = "ops-team"                     # audit identity, not a secret

  [[security.api_keys]]
  key_hash_file = "/etc/cameodb/keys/team-a"
  role  = "writer"
  label = "team-a"
  allowed_indexes = ["docs", "wiki"]     # honored for every role; omitted = all indexes
  ```
- **The config never holds a usable credential.** `cameodb keygen --role <r> [--label <l>]
  [--allowed-indexes a,b]` mints a key, prints it once, and prints the stanza to paste.
- **Key format is enforced at authentication time**: a presented token must match
  `cameo_v1_<43 base64url chars>` before it is hashed. This is what makes an unsalted
  SHA-256 defensible — a hand-chosen passphrase can never authenticate even if someone
  pastes its digest into the config. Verification hashes the token and compares digests with
  `subtle::ConstantTimeEq` across all entries; `sha2`, `subtle`, `zeroize`, `hex`, and `rand`
  are already in `Cargo.lock` transitively, so `cargo deny` and `cargo audit` see nothing new.
- **Secrets follow the `ClusterPsk` precedent**: redacted `Debug`, never serialized, scrubbed
  on drop. `key_id` (first 8 hex of the digest) plus `label` are the log identity; the key
  itself never reaches a log line.
- **Env overrides**: `CAMEODB_SECURITY_ENABLED`, `CAMEODB_API_KEY_HASH`, `CAMEODB_API_KEY_ROLE`
  for the single-key case. Note the earlier sketch gave the *server* `CAMEODB_API_KEY` — that
  is a plaintext key, which contradicts hash-only storage, and it collides with the name the
  *client* needs the moment both run in one compose file. `CAMEODB_API_KEY` is client-only.
- **Backward compatibility**: auth off by default. The earlier sketch also wanted a fail-fast
  when `bind = 0.0.0.0` without auth; dropped, because the posture rules already answer that
  question per profile (Warn under `internal`, Fail under `external`). Two mechanisms
  disagreeing about one condition is how this rots.

- **Route table — deny by default.** Classification lives in one table keyed by (method, path
  pattern). The middleware runs *before* routing, extracts the index segment lexically, and
  enforces capability and scope centrally, so no handler can forget to check.

  | Route | Requires | Index-scoped |
  |-------|----------|--------------|
  | `GET /_cluster/health` | public (minimal body) / `Read` (full body) | — |
  | `POST /api/{index}/search` | `Read` | yes |
  | `POST /api/{index}/search/stream` | `Read` | yes |
  | `GET /api/{index}/_config` | `Read` | yes |
  | `GET /_indexes` | `Read` | **filtered** |
  | `GET /_cluster/_indexes` | `Read` | **filtered** |
  | `PUT /api/{index}/document` | `Write` | yes |
  | `POST /api/{index}/document/stream` | `Write` | yes |
  | `POST /api/{index}/_bulk` | `Write` | yes |
  | `PUT /api/{index}/_config` | `IndexAdmin` | yes |
  | `PATCH /api/{index}/_schema` | `IndexAdmin` | yes |
  | `DELETE /api/{index}` | `IndexAdmin` | yes |
  | `GET /_admin/memory`, `POST /_admin/memory/purge`, `GET /_admin/workers` | `NodeAdmin` | — |
  | `POST /_admin/index/{index}/commit`, `POST …/evict-writer` | `NodeAdmin` | yes |
  | `POST\|GET\|DELETE /mcp/*` | `Read` + per-tool check inside | inside |
  | anything else (fallback) | **deny** | — |

  Consequences accepted deliberately: an unknown path answers **401 without a key and 404
  with one**, since auth precedes routing — which also stops path-existence probing. Named
  access to a disallowed index is **403**, while *listing* filters silently: asking by name
  deserves an honest answer, enumeration does not.

  Completeness is guarded by a test that `include_str!`s `http_server.rs`, extracts every
  `.route("…")` literal, and fails if any lacks a classification. A new route cannot ship
  unclassified, which a hand-maintained matrix could not promise.

- **Layer placement** in the existing stack:
  ```
  TraceLayer → CORS → AUTH → Timeout → ConcurrencyGuard → wire body limit
    → Decompression → extractor limit → Compression → routes
  ```
  Inside CORS, so browser preflight `OPTIONS` — which never carries `Authorization` — still
  gets its headers. Outside the concurrency guard and the body limits, so a 401 flood neither
  takes a semaphore permit nor gets its body buffered; `/_cluster/health` is exempted the way
  the guard already exempts it. Accepted cost: rejecting before the body is read means hyper
  drops the connection instead of reusing it.

- **MCP enforcement reaches the tool, not just the path.** `/mcp` is a single JSON-RPC
  endpoint, so path-level middleware cannot see which tool or index is in play.
  - New `McpAuthz` trait **in the mcp crate** (`allows_index`, `has(Capability)`, `key_id`),
    implemented by the server's auth context, so identity threads router → dispatch →
    `McpBackend` without the mcp crate learning any server types.
  - `tool_capability(name) -> Option<Capability>` with a deny default, so the day a write
    tool is added it fails closed instead of inheriting `Read`.
  - `list_indexes` filters to the caller's scope; `search_index` 403s a named disallowed
    index; `search_indexes` 403s rather than silently returning partial results.
  - Auth enforced on the GET (SSE) and DELETE session routes too, not only the POST. Sessions
    record the creating `key_id` and reject a request presenting a different key.

- **Client SDK + CLI**: `--api-key`, `--api-key-file`, `CAMEODB_API_KEY`; precedence inline >
  file > env, matching the server's PSK convention. `--api-key` is documented as `ps`-visible.
  The client **refuses to send a key to a plaintext non-loopback URL** unless `--insecure`,
  and in the REPL `connect <different-origin>` **drops** the key rather than forwarding it —
  the same failure the `TlsTrust` split already had to fix once.

- **Posture rules** — the stubbed `auth` check becomes evaluated:

  | Condition | Outcome |
  |-----------|---------|
  | enabled + ≥1 key | Pass — *N keys: 1 admin, 2 writer, …* |
  | `external` + disabled | **Fail** (unchanged) |
  | `internal` + disabled | Warn (unchanged wording) |
  | `local` + disabled | Pass — "unauthenticated (loopback only)", mirroring how `tls` passes plaintext for `local`. A profile that warns on every boot teaches operators to ignore warnings |
  | enabled + **0 keys** | **Fail** — every request would 401; fail loudly, not silently |
  | enabled + no key holding `Write`/`IndexAdmin` | Warn — read-only node |
  | enabled + TLS off + non-loopback bind | Warn under `internal` (tokens in the clear); `external` already fails on `tls` |
  | `admin_api` rule | "reachable off-box and unauthenticated" becomes Pass once auth and an admin key exist |

- **Trust boundary, stated so it is not assumed away**: enforcement is at the HTTP/MCP
  ingress, where identity exists. Peer-to-peer traffic is kameo-over-libp2p and is trusted by
  the B3 PSK, so **index scoping is not a defense against a rogue cluster member**. This also
  corrects the earlier C3 sketch, which proposed enforcing at the `RouterActor` boundary —
  that boundary is driven by peers as well as by HTTP, where no API key exists.

- **Non-goals, recorded so they are not re-litigated**: no lockout or throttle on failed auth
  (against a 256-bit key it buys nothing and is itself a DoS lever — count and log for C2);
  no hot config reload (rotation is add-key → migrate → remove-key → restart, already better
  than the PSK's "stop every node" gap).

- **Order of work**:
  1. `[security]` config + key types + `keygen` + posture rules + `check-config` (no enforcement yet)
  2. Classification table + middleware + completeness test + health-response shrink
  3. Index scoping enforcement + list filtering
  4. MCP threading + session binding + tool capability table
  5. Client/SDK/CLI/REPL plumbing
  6. `scripts/validate/auth.sh` into `all.sh`; docs, example config, CHANGELOG, and the
     RELEASE-CHECKLIST known-gap removal

- **`scripts/validate/auth.sh` proves**: 401 on every classified route bare · 403 per wrong
  role per capability class · preflight passes without a key · unknown path 401 → 404 ·
  health minimal vs full · scoped key allowed / denied / filtered · MCP per-tool and
  per-index, plus session binding · an unauthenticated flood does not starve authenticated
  requests (which is what proves the layer order) · no key in any log line · `check-config`
  fails `external` + auth-off and passes `external` + auth-on + TLS.

**B2 — HTTPS/TLS via rustls** ✅ COMPLETED (the first implementation never ran)
- **The original implementation panicked on every TLS startup** and was marked complete without a single HTTPS request being served. `axum-server/tls-rustls` force-enables `rustls/aws-lc-rs` while libp2p-quic enables `rustls/ring`; rustls 0.23 refuses to pick between two providers, and the panic landed *after* the startup banner, so it read as a healthy boot
- Fixed by using `axum-server/tls-rustls-no-provider` and installing `ring` explicitly at the top of `main`, on both the server and client paths
- TLS material is now loaded before storage init and before the banner, so bad certificates fail early and legibly
- Graceful shutdown under TLS via `axum_server::Handle`; previously the drain signal only reached the plaintext listener and every TLS shutdown burned the full 10 s timeout before cutting in-flight requests
- Implemented axum-server with rustls for HTTPS support; config `[network.http.tls] enabled, cert_file, key_file`
- Added TLS validation to config (cert/key file existence, required fields when enabled)
- Client-side: added `--insecure` flag for accepting invalid TLS certificates (self-signed certs in development)
- Per-command `--insecure` for remote schema/data loading operations (fine-grained control)
- Removed `CAMEODB_ACCEPT_INVALID_CERTS` environment variable (simplified to flag-only interface)
- Documentation updated with TLS configuration, Linux system certificate paths, and security best practices
- Single TLS stack across the workspace: `reqwest/rustls-no-provider` replaced native-tls, verified against `dl.cameodb.com` and other real sources. `rustls-platform-verifier` uses the OS trust store, which is what native-tls provided and what a corporate CA needs. Vendored OpenSSL is gone from every build path
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
- ✅ Format validation lives in `load_psk()` alone; `validate()` calls the same path, so a config that validates is one the swarm can start with
- ✅ The key is held in a `ClusterPsk` newtype that redacts its `Debug`, is never serialized, and zeroizes on drop; `psk_file` permissions are checked and a world-readable file warns
- ✅ A PSK combined with a `/quic-v1` address is rejected at config time rather than failing as a dial error, since `pnet` wraps TCP only
- Wording corrected: PSK is a **membership gate**, not a confidentiality upgrade — the transport is already encrypted by Noise
- Future: PSK rotation with primary + secondary for zero-downtime rolling upgrades

### Stage C: Defense in Depth (post-auth)

**C1 — MCP-Specific Limits** 🟡 MEDIUM
- Rate limiting per session/key on MCP tool invocations (especially for `reader` keys held by AI agents)
- Query complexity caps: max boolean clauses, max prefix-expansion terms, per-request timeout already exists — wire it into MCP path
- Per-key index scoping is covered in B1 (`allowed_indexes`, all roles), as is the failure counting this stage would rate-limit on

**C2 — Audit Logging** 🟡 MEDIUM
- Structured `tracing` events: who (`key_id` / `label` / peer), what (op, index), when, result
- B1 already emits `key_id`, `role`, and `label` into the request span, so this stage is a sink rather than a re-plumbing
- Append-only audit ring buffer + optional file sink; admin endpoint to query recent events

**C3 — Per-Index Role Overrides** 🟢 LOWER (needed for multi-tenant)
- Most of what this stage originally described is B1's scoping mechanism, which now applies to every role. What remains is *overrides*: a key with `role = "writer"` granted read-only on a named sensitive index, i.e. per-index capability subtraction rather than a second allow-list
- Enforced at B1's ingress chokepoint, **not** at the `RouterActor` boundary as first sketched — that boundary is also driven by cluster peers over kameo, where no API key exists (see the trust boundary note in B1)
- Depends on B1's capability model and route classification table

### TLS Inventory (verified 2026-08-07)

| Component | Current TLS | Notes |
|-----------|-------------|-------|
| HTTP server | ✅ rustls via axum-server | Implemented with `[network.http.tls]` config (enabled, cert_file, key_file) |
| Client SDK (`reqwest 0.13`) | ✅ rustls + `ring`, OS trust store via `rustls-platform-verifier` | No TLS feature flags; `--insecure` (server) and `--insecure-source` (data sources) are separate |
| musl static builds | ✅ rustls + `ring`; no vendored OpenSSL, no C toolchain | Image needs `ca-certificates`; verify per target with `scripts/validate/remote-sources.sh` |
| libp2p cluster transport | ✅ Noise (`noise::Config`) + yamux mux, optional `pnet` PSK | Noise provides confidentiality; the PSK gates membership (B3). QUIC is disabled when a PSK is set |
| kameo remote messaging | ✅ rides libp2p swarm | inherits Noise encryption and the B3 membership gate |
| Client TLS bypass | ✅ explicit flags only | `--insecure` (server connection) and `--insecure-source` (remote sources) are independent; no env-var bypass |

**Success Metrics:**
- No unauthenticated write/delete path reachable once `[security] enabled = true`
- Every route in `create_router` carries a capability classification, enforced by a test that
  reads the router's own source — an unclassified route is denied, not allowed
- The `external` profile starts: TLS on, auth on, `/_admin/*` off, verified by `check-config`
- Path-traversal regression tests pass in `scripts/validate/unit.sh`
- `cargo audit` and `cargo deny` green via `scripts/validate/deps.sh`
- TLS + auth enabled = zero plaintext credentials on the wire; no key in any log line
- Cluster rejects unknown peers without a valid PSK

(Metrics say `scripts/validate/`, not CI: there is no CI by decision — see Stage A5 and
[RELEASE-CHECKLIST.md](RELEASE-CHECKLIST.md).)

---
