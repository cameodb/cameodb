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
- ✅ **Phase 12 (MCP Server Integration)**: Core tools, transport, resources, and query syntax docs completed; security moved to Phase 14. Integration testing is no longer absent — `crates/server/tests/mcp_rate_limit.rs` and the MCP cases in `crates/server/tests/audit_trail.rs` drive `tools/call` against a real node over JSON-RPC — but streaming and the MCP-specific documentation pass remain
- 🎯 **Phase 13 (Thread-Per-Core & Memory Ops)**: Stages 1, 2a, 2b, 2c, 2d, 2e completed; Stage 2f partially done (merge thread count control implemented via `IndexWriterOptions`; core pinning and per-arena stats planned). Shard placement reworked 2026-08-08: dense ordinals replace `xxh3(shard_id) % n` on both the dispatch and writer-pinning sides, and a single `CoreLayout` reconciles `get_core_ids()` with `available_parallelism()`. `/_admin/workers` reports the pin outcome per worker and per shard, not the request. **Verified on Linux (aarch64 container, 8 cores) 2026-08-08: 8/8 workers pinned to their target cores and all four writer threads to cores 0–3, confirmed independently against `Cpus_allowed_list` in `/proc/<pid>/task/*/status` — one CPU per worker thread, one per writer, no collisions.** Pinning is a no-op on macOS, so it must be validated on Linux; the whole suite passes there too. **Measured 2026-08-09 and re-measured 2026-08-10 (see "Worker concurrency, measured"): Stages 2d and 2e cost throughput rather than gaining it, and both flags stay off. The first diagnosis blamed the per-worker serial loop from Stage 2; that loop is gone as of 2026-08-10 and the flags still lose, so the cause is the affine constraint itself — a shard's jobs may only run on one worker, and skew leaves workers idle.**
- 🔒 **Phase 14 (Security Hardening)**: A1–A5, B2, B3 completed and verified by `scripts/validate/`; posture presets added (`local` / `internal` / `external`); B1 (authentication) complete, landed 2026-08-08 — steps 1–5 plus hardening (6a) and documentation (6b): credential model, `keygen`, `[security]` config, enforcement at the HTTP/MCP ingress with capability and index scoping (so `external` can now start), `--api-key` / `--api-key-file` / `CAMEODB_API_KEY` on the bundled client, index list filtering, and MCP per-tool authorization with sessions bound to their key. C1 (MCP rate limiting) completed 2026-08-10 and C2 (audit trail) completed 2026-08-10 — `[security.limits]` and `[security.audit]`, both off by default, with `GET /_admin/audit` for reading the trail back. **C3 is the only stage still open**, and it shrank because B1 absorbed index scoping

### **Recommended Next Steps**
1. ~~**A latency harness.**~~ ✅ Landed 2026-08-09 as `cameodb-bench` (`crates/bench`): percentiles for writes and searches, the node's `took_ms` beside the client-observed figure, and the worker-pool delta over the measured window. Closed-loop, so runs are comparable at equal concurrency rather than being an SLA
2. ~~**Document and default the affinity flags.**~~ ✅ Landed 2026-08-09, and the answer was *no*: see "The affinity flags, measured" below. Both stay `false`, now present and explained in `cameodb.example.toml`, `crates/server/cameodb.toml`, `docker/cameodb-docker.toml` and `docs/CONFIGURATION.md`
3. ~~**Give a worker more than one operation at a time.**~~ ✅ Landed 2026-08-10. A worker now carries up to 8 operations, bounded by a semaphore acquired *before* the receive so the channel stays the backpressure signal. Worth **+65-70% write throughput and −64% on p90** where the pool is the constraint, and nothing where it is not — see "Worker concurrency, measured". It did *not* redeem the affinity flags, which was the other reason to do it
4. ~~**A bounded linger before the writer commits.**~~ ❌ Built and rejected 2026-08-10 — no measurable gain at any concurrency tested, and the arrival arithmetic says there cannot be one against a closed-loop client. Removed; the reasoning is recorded in "Mixed read/write load, measured" so it is not rebuilt. **An open-loop load generator is the prerequisite for revisiting it** — and is worth having anyway, since every number in this document is closed-loop
5. **The cost of a durable commit under read load.** What the linger was meant to paper over, still open: a commit costs ~12.5ms with searches running against ~4.6ms without, and `wal_sync = false` recovers +86% of write throughput. The lever is the fsync itself — WAL device and placement, or a durability level between "every commit" and "none" — not how the writer groups writes
6. **Take unkeyed searches off the coordinator.** A keyed write now resolves locally from the published ring and shard placement, but a search still pays a mailbox round trip to a single actor because the decision depends on cluster size, which the router has no cheap way to know. Needs the node count published alongside the ring
7. **Phase 13 Stage 2f**: Tantivy merge thread core pinning + per-arena jemalloc stats. No longer blocked behind step 3 — but the evidence against it got stronger, not weaker. Per-arena jemalloc stats are worth having on their own; more *pinning* now has two independent measurements saying it does not pay, and should not be attempted without a specific hypothesis neither of them covers
8. **Phase 14 Stage C3**: per-index role overrides — capability *subtraction* on a named index, on top of B1's scoping. The only security stage left; C1 and C2 landed 2026-08-10, and it matters only for multi-tenant deployments
9. **Phase 12 remaining**: MCP streaming and the documentation pass. Integration tests are no longer part of this item — `tools/call` is now driven end to end against a real node by the C1 and C2 suites

### **The affinity flags, measured**

Recorded 2026-08-09 with `cameodb-bench` against a Linux node in a container (aarch64,
8 cores, 8 shards, `wal_sync = true`), client on the host, three repeats per arm, medians.
Every arm ran from an empty data volume.

| Arm | write ok/s @c16 | write p90 | write p99 | search ok/s @c16 | search p99 |
|---|---|---|---|---|---|
| no affinity at all | 3 339 | 6.57ms | 11.59ms | — | — |
| `writer_core_affinity` only (the default) | 3 375 | 6.63ms | 11.12ms | 5 055 | 8.3ms |
| `+ shard_affine_dispatch` | 2 797 | 10.15ms | 17.39ms | 4 850 | 8.9ms |
| `+ worker_core_affinity` | 2 815 | 9.94ms | 18.90ms | 4 320 | 16.5ms |

The write regression held at concurrency 8, 16 and 32 — 13% to 20% — so it is not an artifact
of one operating point. Pinning was confirmed to have actually taken effect in each arm via
`/_admin/workers` (8/8 workers on their target cores, writers on cores 0–7), not assumed.

**Stages 2d and 2e are a loss as built, and the reason is Stage 2's worker loop, not the
pinning.** A worker awaits `execute` inline, so it carries exactly one operation; enabling
affine dispatch forces `worker_count` from `min(shards × 2, cores × 2)` down to `cores`, which
halves the node's in-flight operations. That would be fine if workers were CPU-bound, but an
operation is mostly spent awaiting the shard writer — the node sat at ~135% CPU of 800%
available during the write runs. Affine assignment also loads workers unevenly where
round-robin does not, which is why the loss persists even at concurrency 8 where 8 workers
should be enough. Searches fail differently: they *are* CPU-heavy (~530% during search runs)
and fan out across every shard, so pinning the driving worker to one core is a plain loss.

Writer pinning itself is free — neutral against no affinity at all — and is what gives the
other two something to align to, so it stays on.

The dense-ordinal placement work was still worth doing: it is what makes the flags
measurable at all, and `xxh3`-based placement was strictly worse than what was measured here
(it left cores empty).

> **Superseded in part, 2026-08-10.** The closing prediction here — that the flags would pay
> off once a worker could carry several operations — was tested and is wrong. The diagnosis
> of *why* the flags lose was incomplete rather than the measurement: see "Worker
> concurrency, measured" below.

### **Worker concurrency, measured**

Recorded 2026-08-10 with `cameodb-bench`, same rig as above (Linux container, aarch64,
8 cores, 8 shards, `wal_sync = true`), client on the host, three repeats per point, medians,
every run from an empty data volume. The host was otherwise idle — a first attempt at this
sweep ran while the machine was compiling and produced nothing but noise.

**How wide should a worker be?** `default.toml`, single writes, concurrency 64:

| width | write ok/s | p50 | p90 | p99 |
|---|---|---|---|---|
| 1 (the old inline loop) | 4 178 | 11.32ms | 29.30ms | 81.75ms |
| 2 | 5 438 | 9.26ms | 18.61ms | 78.78ms |
| 4 | 6 826 | 7.58ms | 11.25ms | 77.08ms |
| **8 (chosen)** | **7 118** | 7.68ms | **10.45ms** | 61.53ms |
| 16 | 6 444 | 7.97ms | 11.42ms | 88.44ms |

**+70% throughput and p90 down 64%** against the pre-change baseline, and the curve turns
over rather than flattening: every width-8 repeat beat every width-16 repeat, and width 8's
worst repeat beat width 4's median. Eight is a measured peak, not the largest value tried.

The width-8 point was then re-measured on the final build — constant compiled in, sweep hook
removed, worker loop refactored to take its operation runner as a parameter — and came back
at **6 901 ok/s median over six runs** (5 690 … 7 079) against the sweep's 7 118 over three.
That is +65% rather than +70% on the same baseline. The two sets overlap and the spread is
wider than the gap, so this is not evidence of a cost in the refactor; it is the honest width
of the measurement. Quote the range, not the best number in it.

The control matters as much as the sweep. At **concurrency 16 the same widths are flat**
(4 293 / 3 906 / 4 023 / 4 156 ok/s, within run-to-run spread), and they should be: the
default pool is 16 workers, so even at width 1 the node can hold every request a closed-loop
client at c16 has outstanding. Width only buys anything once demand exceeds `worker_count`.
Read that as the scope of the win — this is a saturation fix, not a free speed-up.

**The affinity flags were re-measured at width 8, and they still lose.** This is the part
that did not go as predicted:

| Arm | write ok/s @c64 | write p99 | search ok/s @c16 | search p99 |
|---|---|---|---|---|
| default (writer pinning only) | 7 118 | 61.53ms | 6 326 | 5.75ms |
| `+ shard_affine_dispatch` | 5 393 | 141.62ms | 6 298 | 5.78ms |
| `+ worker_core_affinity` | 6 735 | 92.78ms | 5 618 | 8.62ms |

Affine dispatch costs 24% of write throughput with a worker eight operations wide, and the
separation is clean: default's *worst* repeat (6 995) beat every affinity repeat. The affine
and pinned write arms are noisier than the default one and their ranges overlap, so the
ordering *between* them is not resolved here — only that both sit below the default.

So the earlier diagnosis was half right. Halving `worker_count` did hurt, but it was never
the whole story, and the surviving cause is the constraint itself: a job for shard S may only
run on worker `S % worker_count`, so any instantaneous skew across shards leaves workers idle
while their neighbours queue. Round-robin cannot be unlucky that way. Searches confirm the
split from the other side — affine dispatch is *neutral* for them, because searches dispatch
round-robin regardless, while pinning the driving worker costs 11% and half again on p99.

Both flags stay off, now for a reason that has survived a test designed to overturn it.

### **What the audit trail costs, measured**

Recorded 2026-08-10 — and **not on the rig every other figure here comes from**. This ran
natively on the macOS development host, so the absolute numbers are not comparable to
anything else in this document; only the two arms are comparable to each other.

`--mode write --concurrency 64 --duration 20`, security off in both arms so the only variable
is the trail, three repeats each, arms alternated, host otherwise idle.

| `[security.audit]` | Repeats (ok/s) | Median | Within-arm spread |
|---|---|---|---|
| `enabled = false` | 1 803, 1 637, 1 695 | 1 695 | 10.1% |
| `enabled = true`, file sink | 1 753, 1 740, 1 610 | 1 740 | 8.9% |

**The cost is below this measurement's noise floor.** The arms overlap completely and the
median difference (+2.7%) runs the *wrong way* — the audited arm was nominally faster, which
is a statement about the spread and not about auditing. What can be claimed is bounded: on
this host, at this sample size, a difference large enough to matter would have shown, and did
not. That is not the same as "free", and this is three repeats on a laptop.

The design predicts as much. A write costs one `Instant`, one timestamp format and a
`try_send` on the emitting side; the record is then folded into a `HashMap` entry rather than
serialized, so the per-write work never includes the JSON. The 44 171 writes of one run
produced **three lines**:

```json
{"event":"write_stats","index":"bench","ops":16821,"errors":0,"window_start":"…07.212Z"}
{"event":"write_stats","index":"bench","ops":15498,"errors":0,"window_start":"…16.737Z"}
{"event":"write_stats","index":"bench","ops":11852,"errors":0,"window_start":"…26.749Z"}
```

while the `PUT /_config`, the commit, the two `/_admin/workers` polls and the `DELETE` each
kept their own. That ratio — five figures of ingest against a handful of lines — is the whole
argument for rolling writes up, and it is what the file actually contains.

Not measured, and worth knowing before trusting the above: the read path, where a record *is*
serialized per request; and the trail under a workload heavy enough to overrun the queue,
which is the case the `gap` record exists for. Both want the open-loop generator that item 4
already blocks on.

### **Mixed read/write load, measured**

Recorded 2026-08-10, same rig. **Every performance number this repository had published
until now was taken with writes alone or searches alone.** Running them together is a
different machine, and the question it answers — should reads and writes be isolated onto
separate cores? — could not have been answered by any earlier arm.

| workload | alone @c16 | in mixed @c16 | change |
|---|---|---|---|
| writes | 4 074 ok/s, p99 8.5ms | 1 776 ok/s, p99 27.0ms | **−56%, p99 3.2x** |
| searches | 5 880 ok/s, p99 6.5ms | 3 284 ok/s, p99 15.4ms | −44%, p99 2.4x |

Container CPU during the mixed runs sat at **~620% of 800% — one and a half to two cores
idle** while both workloads lost roughly half their throughput. Work is being lost, not
shared, so there is real headroom here.

**It is not a core-contention problem, and core isolation would not fix it.** Three results
say so, and the first was a hypothesis this section set out to confirm:

- **Unpinning the writers changed nothing** (1 758 vs 1 776 ok/s). The theory was that a
  pinned writer returning from fsync cannot resume until *its* core is free, even with other
  cores idle. Measured, and false.
- **Capping the read pool helps a little, consistently**: `search_threads` 16 -> 6 moved
  search p99 15.44 -> 12.49ms and write p99 27.0 -> 23.1ms, and collapsed run-to-run spread
  from 1 477-1 853 to 1 837-1 842. `= 8`, the code default, lands between the two. Bounded
  read concurrency is the mechanism this design already chose over partitioning, and it
  works — modestly.
- **The write path is waiting on disk, not CPU.** With `wal_sync = false` under the same
  mixed load, writes go 1 837 -> 3 416 ok/s (+86%), write p99 23.1 -> 12.1ms, and CPU rises
  to ~724% as searches finally start competing for cores in earnest.

Partitioning cores would take cores from searches — the one workload here that *is*
CPU-bound, drawing ~600% of 800% on its own — to give them to writers that need ~127% and
spend most of it blocked in `fsync`. That is the same trade `worker_core_affinity` already
lost by 11%.

**What actually limits mixed writes is the cost of each commit.** The shard writer already
coalesces: it drains every queued command and merges same-index writes into one redb
transaction, so one fsync serves the whole group. Measured mean group size:

| case | write ok/s | mean coalesced group | implied cost per commit |
|---|---|---|---|
| pure write @c64 | 6 669 | 4.49 | ~5.4ms |
| pure write @c16 | 4 165 | 2.40 | ~4.6ms |
| mixed @c16 | 1 597 | 2.49 | **~12.5ms** |

Coalescing does not degrade under mixed load — 2.49 against 2.40. The *commit* gets three
times more expensive, because tantivy segment reads contend with WAL fsync for IO and page
cache. And the group cannot grow to absorb it: the writer commits whatever is queued at that
instant, and at concurrency 16 across 8 shards only about two writes are ever in flight per
shard.

The obvious response is a **bounded linger before commit** — wait a short, capped interval
for more writes rather than committing the two already queued, amortising a 12.5ms fsync
over 8 writes instead of 2.5.

**It was built and measured on 2026-08-10, and it does not pay. The code was removed.**
Lingering only when the instant drain already found company (so an isolated write never
waits), swept at 200µs / 500µs / 1000µs against a 0µs control, three repeats at c16 mixed
and c16 pure, then six at c64 where it had the best chance:

| linger | mixed write ok/s @c16 | pure write ok/s @c16 | pure write ok/s @c64 (n=6) |
|---|---|---|---|
| 0 (control) | 1 851 | 4 272 | 6 435 |
| 200µs | 1 938 | 4 429 | 6 803 |
| 500µs | 1 782 | 4 299 | — |
| 1000µs | 1 718 | 4 348 | — |

Every arm has a bad repeat and the between-arm gaps are smaller than the within-arm scatter;
at c64 the two distributions overlap almost entirely and the 200µs arm owns the single worst
run of the twelve. Nothing here is resolvable.

The arithmetic says why, and it is the useful part. At c16 the node writes ~1 850/s across 8
shards — 231/s per shard, so **0.046 writes arrive at a shard during a 200µs window**; at
c64 it is still only ~0.18. Worse, the bench client is closed-loop, so it cannot issue the
next write until the current one is answered: the writer would be waiting for writes that
cannot arrive until it commits and replies. **The linger waits on itself.**

A linger can only work where many independent clients hold requests outstanding at once —
an open-loop arrival process. Do not rebuild it from the reasoning above without first
having a workload generator that can produce one; against this harness it is untestable, and
against a closed-loop client it is provably useless. The remaining honest levers on mixed
write cost are the fsync itself (device, `wal_sync`, WAL placement) rather than how the
writer groups.

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
  - When `shard_id` is `Some`, route to `workers[ordinal(shard_id) % worker_count]`
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

### Stage 2d: Co-Locate Writer Pinning with Worker Placement ✅ DONE

**Risk:** Low | **LOC:** ~15 | **Prerequisite:** Stage 2a

**Goal:** Ensure the writer thread for shard X lands on the same core as the worker that handles shard X's operations.

**Implementation (delivered):**
- In `NodeOrchestrator::spawn_worker_pool`, when `shard_affine_dispatch && writer_core_affinity` are both enabled, force `worker_count = cpu_cores`.
- Worker and writer both derive from the shard's dense ordinal, so for any shard S the worker handling S dispatches into the writer pinned on the matching core.
- Tokio worker tasks aren't OS-pinned, but the scheduler keeps frequently-running tasks near their last core under sustained load — co-locating dispatch with the writer thread maximizes that locality.
- Behind a config gate: default behavior (either flag off) preserves the existing `min(local_shards * 2, cpu_cores * 2)` worker sizing.

**Superseded (2026-08-08):** originally hashed — `xxh3(shard_id) % worker_count` against
`xxh3(shard_id) % num_cores`. Both sides agreed, but the hash domain is the shard set, which
is smaller than the core count, so it collided: measured with the shipped defaults (4 shards,
8 cores), 40 affine writes reached 3 of 8 workers and two shards' writers shared a core.
Replaced by `ShardPlacement`, which assigns a dense ordinal per shard. Same guarantee, no
collisions — the same run now reaches 4 of 4 possible workers, one writer per core.

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
  - Pins the OS thread to `CoreLayout::core_for(worker_id)` via `core_affinity::set_for_current`.
  - Runs an isolated `tokio::runtime::Builder::new_current_thread()` runtime with `max_blocking_threads(4)` (kept tiny because search delegates to the shared `read_runtime` and writes go through the pinned writer thread).
  - Falls back gracefully on macOS / when pinning fails (logged, runs unpinned on a dedicated thread).
- `NodeOrchestrator.worker_threads: Vec<std::thread::JoinHandle<()>>` stores handles; `shutdown_worker_pool` sends shutdown messages then joins them via `spawn_blocking`.

**Why minimal:**
- No new `[runtime]` config section — just one boolean. A `CoreLayout` now exists, but only as the single source of which cores this process may use (`get_core_ids()` reconciled with `available_parallelism()`, so a cgroup CPU quota cannot make worker sizing and pin targets count different cores). Splitting it into reserved / per-shard / read-pool sets is still deferred — that is Stage 2f.2's work.
- No changes to `OrchestratorJob`, `OrchestratorWorkerTx`, `OrchestratorEngine`, `RouterActor`, `MicroshardActor`, or `engine.execute()` body — they work identically across both runtimes.
- The shared `read_runtime` continues handling all heavy I/O, preserving search throughput.

**Wakeup math:**
- Default mode: router-task → mpsc → worker-task → channel → writer-thread (cross-core wakeup if worker scheduled away from writer's pinned core).
- Pinned mode: router-task → mpsc cross-runtime → worker-thread (pinned core C) → channel → writer-thread (pinned core C) — second hop becomes a same-core mpsc push (no wakeup syscall). Cache locality wins for schema cache, routing ring, and shard map.

**Edge cases handled:**
1. Broadcast/scatter — `affinity_shard = None`, falls through to round-robin send across pinned workers.
2. Dynamic shard creation — workers already cover all cores; the new shard takes the next ordinal, which determines its worker.
3. `current_thread` runtime — fine because the worker only awaits channels and delegates blocking work elsewhere.
4. Shutdown — JoinHandles ensure runtimes drop before the orchestrator returns.

---

### Stage 2f: CPU Arenas & Per-Arena Jemalloc Stats 🎯 PARTIALLY DONE

**Risk:** Medium | **LOC:** ~250 | **Prerequisite:** Stage 2e, plus a latency harness for the parts whose value is unproven

**2f.1 — Tantivy Merge Thread Control:** ✅ COMPLETED
- Merge thread count is configurable via `StorageConfig.merge_num_threads` (default: **2**)
- Implemented via `tantivy::indexer::IndexWriterOptions::builder()` with explicit `num_merge_threads()`
- Replaces Tantivy's default of 4 merge threads, preventing mmap storms on memory-constrained nodes. Two rather than one is deliberate: it leaves headroom to merge in parallel under load instead of serialising compaction behind a single thread
- Note the count is **per open index**, so merge threads scale with how many indices are open, not with shard count

**2f.2 — CPU Arenas for Write / Read / Merge:** 📋 PLANNED — analysed 2026-08-08, not implemented

Design investigation, measured on Linux (aarch64 container, 8 cores, 4 shards × 4 indexes,
all affinity flags on). The observations below come from `Cpus_allowed_list` in
`/proc/<pid>/task/*/status`, not from what the process reports about itself.

**The defect this fixes already exists, and enabling `writer_core_affinity` is what causes
it.** Linux threads inherit their creator's affinity mask, and tantivy spawns its threads
from whichever thread happens to build the `IndexWriter` or drive the commit:

| Index created by | `merge_thread_*` | `segment_updater` | `thrd-tantivy-index*` |
|---|---|---|---|
| `PUT _config` (unpinned thread) | `0-7` | `0-7` | **single core** |
| a write (pinned writer thread) | **single core** | **single core** | **single core** |

So an index created by writing to it — the normal path — gets its two merge threads confined
to the *same single core as the writer they contend with*, making `merge_num_threads = 2` two
threads timesharing one core. Indexer threads are confined in both cases, because
`prepare_commit` calls `add_indexing_worker` on every commit and that runs on our pinned
writer thread. Nothing in CameoDB asks for this; it is inheritance, unnoticed.

**Mechanism available.** Tantivy builds its merge pool via `ThreadPoolBuilder` with no
`start_handler` and exposes no hook to supply one, so those threads cannot be pinned
directly. Inheritance is the lever, and it suffices because we own both creation sites:
`IndexWriter` construction (spawns `segment_updater` + merge pool eagerly) and
`prepare_commit` (respawns indexer threads). Set the creating thread's mask, create, restore.
`core_affinity::set_for_current` takes a single core and cannot express a set, so this needs
`libc::sched_setaffinity` with `CPU_SET` — `libc` is already a direct dependency of the
server crate.

**Proposed layout.** `CoreLayout` splits into two disjoint sets, sized from config with
`0 = auto`: a **write arena** of `clamp(local_shards, 1, cores - max(1, cores/4))` cores (one
core per shard is the ceiling that matters — a shard's writes serialise on one writer
thread), and a **read/merge arena** of the remainder.

| Threads | Arena | How |
|---|---|---|
| `orch-worker-N`, `writer-shard-*` | write, single core each | as today, indexed within the arena |
| `thrd-tantivy-index*` | write arena (all its cores) | widen the writer's mask around `commit()`, restore after |
| `merge_thread_*`, `segment_updater` | read/merge arena | set mask before constructing the `IndexWriter`, restore after |
| `cameodb-read` blocking pool | read arena | tokio `on_thread_start` |
| `warmup-shard-*` | read arena | at thread start |
| global rayon | read arena | build the global pool explicitly with a `start_handler` |

**Oversubscription is the point, not a limitation.** Arenas are affinity *masks*, not
reservations, so `shards × indexes` threads share an arena's cores and the kernel timeshares
them. What an arena guarantees is the negative: nothing in it can preempt a writer core.
Note that arenas bound *where* threads run, not *how many* — thread count is
`shards × open_indexes × (1 + merge_num_threads + indexer_num_threads)`, which was 64 tantivy
threads (98 total) at 4 shards × 4 indexes. The only lever on the count is
`merge_num_threads`; tantivy has no shared merge executor across `IndexWriter`s.

**Expected impact.** Certain: removes the confinement above, restoring the meaning of
`merge_num_threads`. Likely: better write tail latency under merge pressure, since compaction
can no longer preempt a writer mid-commit. Uncertain and deliberately not predicted: whether
disjoint arenas beat free OS scheduling on aggregate throughput — reserving cores leaves some
idle at low load while others queue, which typically helps p99 and can cost p50. That
uncertainty is why the latency harness comes first.

**Bonus for 2f.3.** With `percpu_arena:percpu`, confining thread populations to disjoint core
sets also separates their jemalloc arenas, making per-arena stats attributable to write
versus read work instead of an undifferentiated total.

**Risks.** Linux-only; macOS keeps the current no-op, so the platforms genuinely differ. Mask
save/restore around `IndexWriter` creation needs a drop guard. Below ~4 cores the split
degenerates and should be disabled. It depends on tantivy internals (eager pool construction,
commit-time worker respawn) that are not part of its public contract, so the `/proc` affinity
check should become a Linux validation-suite check rather than a one-off.

**2f.3 — Per-Arena Jemalloc Stats:** 📋 PLANNED
- `read_jemalloc_stats()` currently reads global stats only
- With `percpu_arena:percpu`, expose per-arena stats via `mallctl("arena.i.allocated", ...)` and `mallctl("arena.i.resident", ...)`
- Useful for diagnosing which shard/core is consuming the most memory
- Requires 2f.2's core sets to map arena IDs to shard/core

---

### Phase 13 Execution Order & Risk Matrix

| Order | Stage | Risk | LOC | Prerequisite | Gain |
|-------|-------|------|-----|-------------|------|
| **1** | 2a: Shard-affine dispatch | Low | ~50 | None | Eliminates 1 cross-core wakeup/write |
| **2** | 2b: Extract memory module | Low | ~200 | None | Maintainability, testability |
| **3** | 2c: Auto-purge + per-index memory | Low | ~70 | 2b | Operational safety, observability |
| **4** | 2d: Co-locate writer pinning | Low | ~10 | 2a | Full core co-location |
| **5** | 2e: Per-shard single-thread rt | Medium | ~150 | 2a+2d | True thread-per-core |
| **6** | 2f: CPU arenas + per-arena stats | Medium | ~250 | 2e + harness | Merge threads stop sharing the writer's core; diagnostics (2f.1 done; 2f.2 analysed, 2f.3 planned) |

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
| **6** | B1: API key authentication + index scoping | ✅ Done | Critical | Was full unauthenticated R/W/D access |
| **7** | B2: HTTPS/TLS via rustls | ✅ Done | High | Traffic interception |
| **8** | B3: Cluster join secret (PSK) | ✅ Done | High | Rogue node data access |
| **9** | C1: MCP rate limiting + query complexity | ✅ Done (caps deferred) | Medium | Agent-driven resource exhaustion |
| **10** | C2: Audit logging | ✅ Done | Medium | No forensic trail |
| **11** | C3: Per-index role overrides | ~2 days (was ~5+), **the only stage left** | Medium | Multi-tenant isolation |

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
  1. ✅ **Landed 2026-08-08.** `[security]` config + key types + `keygen` + posture rules +
     `check-config`, with no enforcement. `crates/server/src/auth.rs` holds the whole
     credential model: `Capability` / `Role` bundles, `ApiKey` (redacted `Debug`, zeroized on
     drop, minted from `getrandom`), `KeyDigest` (constant-time `PartialEq`), and `KeyRing`
     with the shape gate in front of the hash. Two deviations from the sketch above, both
     deliberate:
     - The `auth` posture rule reports the configured keys but **still fails `external`**,
       because the middleware does not exist yet. A posture that claimed a guarantee the
       router does not make is the one failure mode this module exists to prevent, so the
       `external` Fail and the `admin_api` wording flip in step 2, not here.
     - `key_hash_file` warns when it is **writable** by group or others, not when it is
       readable. A digest is not a secret, so a readable hash file is not a leak — but a
       writable one lets anyone mint themselves a role, which is worse than the case
       `psk_file` warns about.
  2. ✅ **Landed 2026-08-08.** `crates/server/src/authz.rs`: the route table, the
     `classify` matcher, and the `authorize` middleware, mounted inside CORS and outside the
     timeout, the concurrency guard and both body limits. Deny by default — an unclassified
     path needs a key like any other. Health now answers an anonymous caller with liveness
     alone. Both posture outcomes step 1 left pending are flipped, so `external` starts for
     the first time. Beyond the sketch:
     - **Index scoping for named routes landed here too**, not in step 3. The middleware
       already had the `{index}` segment in hand, and shipping a `allowed_indexes` setting
       that parsed but did nothing would have told operators their key was scoped when it
       was not. What remains for step 3 is *list filtering*, which is a handler change.
     - **An index-scoped key is refused at `/mcp`.** MCP is one JSON-RPC path, so the scope
       cannot be enforced from outside it until step 4. Refusing beats letting a scoped key
       read every index through the side door.
     - `scripts/validate/auth.sh` (56 checks) landed with it rather than waiting for step 6:
       a middleware in the wrong place in the layer stack passes every unit test there is.
  3. ✅ **Landed 2026-08-08.** `filter_index_listing` in `authz.rs` narrows a listing to
     the caller's scope, applied by `/_indexes` and `/_cluster/_indexes`. The cluster
     response repeats every index name under each node that answered, with its own count, so
     the filter recurses and rewrites both — a top-level-only filter would have leaked the
     same names one level down. An entry whose shape it does not recognise is **dropped**:
     if the listing changes underneath it, the failure has to be a missing row, not a leak.
  4. ✅ **Landed 2026-08-08.** `McpAuthz` / `McpCapability` / `McpAuthzRef` in the mcp
     crate, implemented by the server for `Authz`, so identity reaches the dispatcher without
     the mcp crate learning a server type. `tool_capability` denies by default and is held to
     `mcp_tools()` by a completeness test. `call_tool` checks the capability *before* parsing
     arguments, then the named index; `search_indexes` refuses the whole call rather than
     narrowing it, because partial results that look complete are worse than an error.
     Sessions are bound to the `key_id` that created them on all three verbs. The `/mcp`
     refusal for index-scoped keys is gone, and with it the posture note that advertised it.
     Two deviations from the sketch:
     - **Backend methods take the caller only where they enumerate.** Methods that *name*
       their index are checked once in `call_tool`; `list_indexes`, `get_index_stats`,
       `list_resources` and `read_resource` take an `McpAuthzRef`, because only the
       implementation knows which part of its response is a list of index names.
     - **`read_resource` checks the scope itself.** A URI like
       `cameodb://indexes/payroll/schema` is a read of `payroll`, and only the host knows
       that. Not being *offered* a URI is not the same as being refused it, so both are
       tested.
  5. ✅ **Landed 2026-08-08**, taken before steps 3–4: with authentication enforced but no
     way for `cameodb client` to present a key, enabling `[security]` locked an operator out
     of their own tooling, which is the gap most likely to be hit first. `Credential` in
     `crates/client/src/sdk.rs` mirrors the server's `ApiKey` (redacted `Debug`, zeroized on
     drop, `key_id` fingerprint), and the key rides in the `http` client's default headers —
     so every existing call site carries it and none can be forgotten — while `source_http`
     is built without it, keeping the database key off requests to third-party data sources.
     Four deviations from the sketch above:
     - **Precedence is file > inline, not inline > file.** clap resolves each flag against
       its own environment variable first, so the remaining question was only which of the
       two wins. Preferring the file means a stale `CAMEODB_API_KEY` left exported in a shell
       cannot silently override the key a command names explicitly.
     - **The plaintext gate is its own flag, `--allow-plaintext-key`, not `--insecure`.**
       `--insecure` accepts a bad certificate on a connection that is still encrypted; this
       puts a bearer token on the wire in the clear. Folding them together would repeat the
       exact mistake the `TlsTrust` split was made to fix. Loopback is exempt, which is what
       keeps the single-node default usable without any flag.
     - **`HealthResponse` had to be made partial.** Step 2 shrank the anonymous health body
       to `status` alone, but the client's struct still required `node_id` and
       `active_shards` — so an anonymous 200 would have failed to *parse*. A shrunk response
       is only safe once every reader of it tolerates the shrink.
     - **A latent bug surfaced**: the SDK asked for `/_admin/index/{index}/evict_writer`
       while the route has always been `evict-writer`, so that command had never worked. With
       authentication in front of the router its 404 would have become a 401 — an unrelated
       bug wearing an auth costume. Fixed, and `auth.sh` now drives the command end to end.
  6a. ✅ **Landed 2026-08-08.** Hardening found by auditing what steps 1–5 left:
     - `keygen --key-out` / `--hash-out` write the two files the design already assumed an
       operator would create by hand — `0600`, `create_new` so neither is ever overwritten,
       and files written before anything is printed. Closes the loop between `keygen`,
       `key_hash_file` and `--api-key-file`.
     - `Authz::Anonymous` now **denies** in both places it was permissive (the `McpAuthz`
       impl and index-listing filter). Unreachable today because no MCP or listing route is
       `Public` — which is why it must not be the permissive branch, since reclassifying one
       later would silently open it.
     - The unauthenticated-refusal log is thinned to the first few, then powers of two, then
       every hundred thousand. It was one `warn!` per request, so anyone who could reach the
       port could fill the disk with a loop. A 403 still gets a line each: it needs a valid
       key first, so its volume is bounded by someone who already holds credentials.
     - `tools/list` is filtered by capability, and a tool with no row is not advertised —
       the deny default applies to the catalogue as much as to the call.
     - REPL `key file <path>` / `key <api-key>` / `key show` / `key clear`, so `connect`
       dropping a key is no longer a dead end that needs a restart.
     - The client says which credential won when both `--api-key` and `--api-key-file` are
       given, instead of silently preferring the file.
     - Fixed a stale line in `keygen`'s own guidance claiming requests were not yet checked.
  6b. ✅ **Landed 2026-08-08.** The docs tree had never mentioned authentication; only the
     README had. Added `## Security and Posture` to `docs/CONFIGURATION.md` (profiles,
     `[security]`, roles × capabilities, key minting, file modes, rotation, and the cluster
     PSK, which was also undocumented), a `### Authentication` section to
     `docs/API_REFERENCE.md` with the capability required per endpoint and what 401 vs 403
     mean, `## Securing a Deployment` to `docs/DEPLOYMENT.md`, a `## Security` section to
     `docker/README.md`, commented key material in the docker config, compose file and
     systemd unit, and the CHANGELOG entry. Two **examples that could not start** were fixed
     rather than documented around:
     - `docker/cameodb-docker.toml` declared no `profile` while binding `0.0.0.0`, so it
       failed the posture gate the previous commit added. Now `internal` — what a published
       container port actually is — with `cors_allowed_origins = []` to match.
     - The recommended production config in `docs/CONFIGURATION.md` had the same problem and
       enabled the cluster with no PSK. Rewritten as an `external` node with TLS, two keys and
       a PSK, then verified to pass `check-config` with zero warnings.
     The systemd unit gained `ExecStartPre=cameodb check-config`, so a node refuses to start
     in a posture its config does not satisfy before the port ever opens.

- **`scripts/validate/auth.sh` proves** (111 checks, in `all.sh`): 401 on every classified
  route bare · 403 per wrong role per capability class · preflight passes without a key ·
  unknown path 401 → 404 · health minimal vs full · scoped key allowed / denied, including
  against a percent-encoded index name · an unauthenticated flood does not shed
  authenticated requests (which is what proves the layer order) · no key in any log line,
  and `key_id` in place of one · `check-config` fails `external` + auth-off and passes
  `external` + auth-on + TLS · the bundled client authenticating from a flag, a file and the
  environment, refusing a malformed key before sending it, refusing to carry one to a
  non-loopback plaintext host, and explaining a 401 and a 403 differently · `/_indexes` and
  `/_cluster/_indexes` filtered to a key's scope, count included · every MCP tool that names
  an index refused off-scope, the catalog and the resource list filtered, a resource URI
  refused when read directly, an unknown tool refused rather than dispatched · an MCP session
  refused to any key but the one that opened it, on POST, GET and DELETE.

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

**C1 — MCP-Specific Limits** ✅ COMPLETED (rate limiting; complexity caps deferred)
- ✅ **Rate limiting landed 2026-08-10.** `[security.limits]`, a token bucket per key, off by
  default. Enforced in the `tools/call` arm *before* the capability check, so a refusal does
  not leak which tools the key could otherwise call, and the budget is shared across tools
  because what it bounds is the node's cost, not any one tool's frequency. Metered by
  `key_id` rather than by session: a session id is chosen by the caller's host, so metering
  per session would let an agent reset its own limit by reconnecting. Policy lives in the
  server crate behind a `McpBackend` hook — the `mcp` crate keeps no deployment opinions,
  the same split B1 used for authorization. Nine tests, three of them end to end
  (`crates/server/tests/mcp_rate_limit.rs`)
- 💭 **Query complexity caps — deferred, not planned.** Max boolean clauses, max
  prefix-expansion terms, and wiring the existing per-request timeout into the MCP path.
  Judged unnecessary at this stage of the auth module: rate limiting already bounds what a
  key costs the node per unit time, which is the resource-exhaustion risk C1 was written
  for, and a per-request timeout already exists on the HTTP path. A single expensive query
  is a different and much narrower problem than a loop of ordinary ones. Revisit if a
  workload appears where one query — not a stream of them — is the threat; the two
  `parse_query_lenient` call sites in `crates/storage/src/lib.rs` are where a cap would go
- Per-key index scoping is covered in B1 (`allowed_indexes`, all roles), as is the failure counting this stage would rate-limit on

**C2 — Audit Logging** ✅ COMPLETED
- `[security.audit]`, off by default. The prediction that this stage would be "a sink rather
  than a re-plumbing" held: [`decide`](crates/server/src/authz.rs) already resolved `key_id`,
  `label`, `role` and index at one chokepoint, and nothing about that had to move
- **Detail for reads, totals for writes** — the one design decision that was *not* obvious.
  A knowledge base ingests far more than it retrieves (the working assumption is ~100k:1), so
  at the measured ~6 900 writes/s a record per write buries the handful of reads worth
  looking at. Writes fold into a per-key, per-index count flushed every `rollup_secs`; reads,
  MCP tool calls and admin actions keep a line each
- The same rule keeps the trail from being a DoS lever. A refusal of a **valid** key is
  listed — its volume is bounded by the credentials in circulation, and it is the shape of
  both a misconfiguration and a stolen key. A refusal of an *unidentified* caller is counted,
  because its volume is chosen by anyone who can reach the port. This is the same reasoning
  that already thins those `warn!` lines in `should_log_refusal`
- Off the request path entirely: a timestamp and a non-blocking hand-off to a dedicated OS
  thread — not a tokio task, so the trail keeps draining while the runtime is saturated,
  which is when it matters most. A full queue drops, counts, and writes a `gap` record naming
  the loss; silent loss would make the file lie about what it contains
- Two sinks: a bounded in-memory ring served by `GET /_admin/audit` (node-admin, and reading
  it is itself audited), and an optional rotating JSON Lines file. Also emitted on the
  `tracing` target `cameodb::audit`, so an existing log collector gets it for free
- `record_query_text` is off by default and documented as keeping *data*, not metadata: a
  search for a person's name records that name
- A redb table was considered and rejected — it would couple the trail to the storage engine
  being healthy, which is precisely when it is needed, and put a WAL fsync on the request path
- MCP needed its own hook (`McpBackend::record_tool_call`): from the HTTP layer every agent
  call is `POST /mcp`, and which tool and index are in play exist only inside the dispatcher.
  Same host-owns-the-policy split as B1 and C1 — the mcp crate keeps no deployment opinions
- 14 unit tests over the rollup, ring, rotation and drop accounting; 9 integration tests
  driving a real node with three keys (`crates/server/tests/audit_trail.rs`), including that
  no key — accepted or rejected — ever reaches the trail
- Costs nothing measurable on the write path: three repeats each way, audit off vs on with a
  file sink, and the arms overlap completely (see "What the audit trail costs, measured").
  The claim is bounded rather than absolute — that is three repeats on a laptop, and the read
  path, where a record really is serialized per request, was not measured
- 💭 **Not done, and deliberately:** no query interface beyond "most recent N", no
  retention policy beyond file rotation, no signing or tamper-evidence. The first is what a
  log collector is for; the last would need a threat model where the node itself is
  untrusted, which is not the one this stage was written against

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
