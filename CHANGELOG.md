# Changelog

All notable changes to CameoDB are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed
- **Writes to an index with no schema returned 500.** The worker pool's engine holds `ArcSwap`
  snapshots, so it can read a schema but not evolve one — that needs the actor. It signalled
  this by returning a sentinel error, and the caller, which had already moved the op into the
  worker job, had nothing left to retry with and surfaced the sentinel to the client instead.
  Since a new index has an empty schema, *every* first write to an index failed. The engine
  now hands the op back (`WorkerOutcome::UseActor`) and the caller retries it on the actor,
  with no clone on the fast path. Creating an index by writing to it works again.
- **Auto-created indexes were write-only.** Fields inferred when an index is first written
  were marked non-indexed, so documents went in and nothing but `id` could find them again —
  permanently, because a tantivy schema is fixed at index creation and nothing promotes a
  field afterwards. Fields discovered at creation are now indexed (and, as before, not
  stored: hits are rebuilt from redb). Fields that arrive later stay unindexed, which is the
  only thing tantivy allows; they exist for redb/tantivy schema parity. The bundled client
  already applied this rule before PUTting a detected schema, so `cameodb data load` was
  unaffected — the two now agree instead of one compensating for the other.
- **The initial schema was never persisted, and fields past the sampling limit were dropped.**
  Sampling filled the in-memory schema, which made the evolution stage — the only thing that
  wrote to storage — decide there was nothing new, so the storage layer re-derived its own
  schema from the document. Two more places recomputed "is this initial creation?" from
  `fields.is_empty()` *after* sampling had filled it, reading false on exactly the call where
  it was true; one of them selected a validator that does not report new fields, so a field
  first appearing past document 200 of a bulk load never reached the schema at all.

### Added
- **`cameodb-bench`, a latency harness** (`crates/bench`, not shipped). Reports
  p50/p90/p95/p99/p99.9 for writes and searches, the node's own `took_ms` beside the
  client-observed figure so the gap shows queueing rather than query cost, and per-worker job
  counts, core placement and dispatch counters over the measured window. Closed-loop, and it
  says so: compare runs at equal concurrency rather than reading the percentiles as an SLA.
  `scripts/testing/load-test.sh` remains as a smoke test but is now marked not to be quoted —
  it forks `curl` per request and times it in bash, so its latencies are process spawn.
  The harness doubles as a worked example of the client SDK: it depends on `client` and never
  on the server crate, and issues no request the SDK cannot express. `--mode bulk` measures
  batched ingest per request and reports docs/s, which on a 4-shard node showed batching
  worth ~9× at 50 documents per request and ~24× at 500 against one-per-request writes.
- **`CameoClient::write_document`.** Writing one document was reachable over HTTP but absent
  from the SDK, so any consumer needing it had to hand-roll the request.

### Changed
- **Shard-affine dispatch no longer collides.** Worker selection and writer-thread pinning
  both hashed the shard id, and the hash domain (the shard set) is smaller than the core
  count: with the shipped defaults — 4 shards, 8 cores — 40 affine writes reached 3 of 8
  workers and two shards' writer threads shared a core. Both now derive from a dense
  per-shard ordinal, so the same run reaches one worker per shard with one writer per core.
  Searches are unaffected; they round-robin the whole pool as before.
- **Worker sizing and thread pinning count the same cores.** Sizing read
  `available_parallelism()` while pinning indexed `core_affinity::get_core_ids()`. Under a
  cgroup CPU quota those disagree — `docker --cpus=4` on a 32-core host reports 4 and 32 —
  so the co-location the design exists for silently stopped holding. A single `CoreLayout`
  now reconciles them.
- **Keyed operations skip the coordinator.** Every write and every search took a mailbox
  round trip to a single actor to ask where to route, in front of a worker pool built to
  avoid exactly that. A keyed operation whose shard is local is now decided from the
  published routing ring and shard placement, both already in hand. Unkeyed operations
  (searches) still ask — that decision depends on cluster size.
- **`GET /_admin/workers` reports pinning outcomes, not requests.** It previously showed
  `pinned: true` and a `core_id` per worker on hosts where every `set_for_current` call had
  failed, which is every call on macOS. `pinned` is replaced by `pinning_requested` plus
  `pinned_workers`; `hash_aligned` is now `core_aligned` (there is no hash any more); each
  worker carries both `target_core_id` and the `core_id` it actually took; and a new `shards`
  section reports per-shard ordinal, requested core, taken core, and whether the shard is
  serving.
- **`[search] supervisor_timeout_secs` was silently ignored.** The idle-commit supervisor read
  `CAMEODB_SUPERVISOR_TIMEOUT_SECS` from the environment directly rather than from the config,
  so the setting in a config file and the `--supervisor-timeout-secs` flag both did nothing —
  the environment variable appeared to work only because it bypassed the config system
  entirely. It now comes from the config, which still maps that variable onto the field, so
  the env var keeps working and the file and flag start working. Its doc comment also claimed
  a default of 10 while the code used 5; the code was right.
- **The client SDK's worker-report type went stale when the node's field names changed.**
  `AdminWorkersResponse` still required `pinned` and `hash_aligned`, so `cameodb client admin
  workers` would have failed to parse a report from the node it shipped with. Every field is
  now `#[serde(default)]`: a client and a node version independently, and a renamed or added
  counter should degrade to a zero rather than take down the whole report. Covered by tests
  that parse both the current payload and a sparse one.
- **`scripts/validate/auth.sh` file-mode checks were broken on Linux.** They tried BSD `stat -f`
  first and fell back to GNU `stat -c`, but GNU `stat` reads `-f` as "filesystem", takes the
  format string as a filename, still exits 0 for the operand that existed, and returns a
  paragraph of filesystem info — so the fallback never fired and the comparison failed against
  files that were correctly 0600. Order reversed; macOS rejects `-c` cleanly, which is what
  makes GNU-first safe on both.
- **Hot-path logging moved to `debug`.** One search at `RUST_LOG=info` emitted seven lines —
  two per-request routing lines from the coordinator, a handler line carrying the caller's
  query text, and one `No tantivy reader found` warning *per shard* for the normal case of an
  index with no commits. A write and a search now emit none.

### Fixed (security)
- **Corporate CA certificates were silently dropped by both compose files.** The `zscaler` →
  `corporate-ca` rename reached the Dockerfiles but not `docker-compose.yml`,
  `docker-compose-cluster.yml` or `docs/BUILDING.md`, and a secret id that does not match the
  one the Dockerfile mounts fails without an error — the build reports "No corporate CA
  certificate provided" and produces an image that cannot reach a TLS-intercepting proxy. All
  of them now use `corporate-ca`, sourced from `CAMEODB_CA_CERT` (default `/dev/null`, so a
  build with no corporate CA needs nothing set). `scripts/build/docker-push.sh` reads the same
  variable; its hardcoded path had also disagreed with the one the docs told you to use.
- **The shipped Docker config could not start.** It declared no `[node] profile` while binding
  `0.0.0.0`, which the posture check refuses rather than guessing at — so the example config
  failed the gate the same release added. Now `profile = "internal"`, which is what a published
  container port actually is, with `cors_allowed_origins = []` to match.
- **`port` under `[network.cluster]` was silently ignored.** The field is `cluster_port`, and
  unrecognised keys were not reported, so every shipped config and the configuration guide set
  a cluster port that had no effect.
- **Unrecognised-key detection reported every `Option` field as a typo.** The schema it
  compared against was built by serializing to TOML, which drops `None`, so `node.profile`,
  `tls.cert_file`, `tls.key_file` and `cluster.psk_file` were all flagged as unknown settings.
- **TLS never worked.** The server panicked on every HTTPS startup — `axum-server/tls-rustls`
  force-enables `rustls/aws-lc-rs` while libp2p-quic enables `rustls/ring`, and rustls 0.23
  refuses to choose between two providers. The panic landed after the startup banner, so a
  failed boot looked like a successful one. Now uses `tls-rustls-no-provider` with `ring`
  installed explicitly at the top of `main`.
- **Streaming ingest ignored every body limit.** `DefaultBodyLimit` only constrains
  extractors, so `POST /api/{index}/document/stream`, which takes a raw `Body`, was
  unbounded: a 150 MB single-line request under a 1 MB configured limit was accepted and
  drove RSS from 44 MB to 889 MB. Added `RequestBodyLimitLayer` (wire bytes) and a
  per-record cap inside the handler.
- **`request_timeout_secs` was never applied to HTTP.** With no timeout, the new
  concurrency guard made denial of service *cheaper*: four uploads at 300 B/s held every
  permit indefinitely and took the node offline, health check included. Added
  `TimeoutLayer`, exempted `/_cluster/health` from the guard, and added `Retry-After` to
  the 503.
- **TLS lost graceful shutdown.** The drain signal only reached the plaintext listener, so
  every TLS shutdown burned the full 10 s timeout and then cut in-flight requests. Now
  driven by `axum_server::Handle`.
- **Restricting CORS broke browser MCP clients.** `mcp-session-id` was neither an allowed
  request header nor exposed on responses, so the Streamable HTTP transport could not work
  from a browser once origins were restricted.
- TLS material is loaded before storage init and before the banner, so bad certificates
  fail early rather than mid-flight.

### Added
- **API key authentication.** Off by default. With `[security] enabled = true`, every route
  except `/_cluster/health` requires `Authorization: Bearer <key>`. Keys are `cameo_v1_`
  followed by 256 bits of OS entropy; the config stores only `sha256:<hex>`, compared in
  constant time, so a leaked config file holds nothing that can authenticate. The key-shape
  check runs before hashing, which is what makes an unsalted digest defensible: a passphrase
  or a UUID can never authenticate regardless of what digest is configured.
  - `cameodb keygen --role <admin|writer|reader>` mints one, printing the key to stdout and
    the config stanza to stderr. `--key-out` / `--hash-out` write the two files instead —
    `0600`, never overwriting an existing one.
  - Three roles bundle four capabilities: `admin` (all), `writer` (read + write), `reader`
    (read). `allowed_indexes` restricts a key to named indexes for any role.
  - Authorization is one route table and one middleware in front of the router, mounted
    inside CORS and outside the timeout, concurrency guard and body limits — a refused
    request takes no permit and buffers no body. Deny by default: an unclassified path needs
    a key like any other, so no handler can forget to check because no handler checks. Unit
    tests read the router's own source and fail the build if a route has no row.
  - Scoping holds through enumeration, not only when an index is named: `/_indexes`,
    `/_cluster/_indexes`, the MCP catalog and the MCP resource list return only what a key
    may see, counts included.
  - MCP is authorized per tool and per index, not just at the endpoint, with a capability
    table that denies by default. Sessions are bound to the key that opened them on all three
    verbs, and `tools/list` advertises only what the caller could call.
  - An anonymous caller gets `{"status": …}` from the health endpoint and nothing more. Node
    identity and cluster shape are free reconnaissance for anyone who can reach the port.
  - `--api-key`, `--api-key-file` and `CAMEODB_API_KEY` on the client, with the key in the
    HTTP client's default headers so no call site can omit it — and never on the client used
    for remote data sources. `--allow-plaintext-key` is deliberately separate from
    `--insecure`: one accepts a bad certificate on an encrypted connection, the other puts a
    bearer token on the wire in the clear. In the REPL a key is bound to its origin, and
    `key file <path>` / `key show` / `key clear` change it mid-session.
  - Rotation is add key → restart → migrate clients → remove key → restart. Keys are read at
    startup; there is no hot reload, and no lockout on failed authentication (against a
    256-bit key it buys nothing and is itself a denial-of-service lever).
- **Security posture profiles.** `[node] profile = "local" | "internal" | "external"`
  declares how far a node can be reached; the server enforces the matching rules and refuses
  to start if the config contradicts them. Profiles assert, they never rewrite values.
  Omitting it is valid only for a loopback bind, which infers `local`. The names describe
  reach rather than an environment (`dev`, `staging`) because every rule keys off the bind
  address — a lifecycle name invites picking by what the box is for and being rejected for it.
- `cameodb check-config [-c <path>]` prints the posture matrix and exits non-zero on
  failure — the manual equivalent of a CI gate.
- `--profile` / `CAMEODB_PROFILE` override.
- `[network.http] admin_enabled` (default `true`) removes the unauthenticated `/_admin/*`
  routes entirely when disabled; required off by the `external` profile.
- `--insecure-source` on the client, separate from `--insecure`. Accepting an untrusted
  data source no longer disables verification on the connection to CameoDB itself.
- [`scripts/validate/`](scripts/validate/README.md): manual validation suite (deps, unit,
  posture, auth, tls, remote-sources) with a single `all.sh` entry point, plus
  [RELEASE-CHECKLIST.md](RELEASE-CHECKLIST.md). There is no CI by design; this is the gate.

### Changed
- **Single TLS stack.** The client moved from native-tls/vendored OpenSSL to
  `reqwest/rustls-no-provider`. `rustls-platform-verifier` uses the OS trust store, which
  is what native-tls provided and what a corporate CA requires — verified against
  `dl.cameodb.com` and other real sources. Vendored OpenSSL and `aws-lc-rs` are gone from
  every build path, so musl and Windows cross-builds no longer need a C toolchain for TLS.
  The `client/native-tls*` features were removed; build invocations no longer pass them.
- **Default bind is now `127.0.0.1`** (was `0.0.0.0`). A reachable bind additionally
  requires a declared profile.
- **Default `cors_allowed_origins` is now `[]`** (was `["*"]`). CORS governs browsers only,
  so this costs API and MCP clients nothing; `"*"` is accepted only under `local`. An empty
  list is no longer a config error.
- Cluster PSK is held in a `ClusterPsk` newtype that redacts its `Debug`, is never
  serialized, and zeroizes on drop. Format validation lives in `load_psk()` alone, which
  `validate()` now calls, so a config that validates is one the swarm can start with.
  `psk_file` permissions are checked, and a PSK combined with a `/quic-v1` address is
  rejected at config time (`pnet` wraps TCP only).
- Every index path is built through `HybridStore::index_dir`, including internally sourced
  names, so the traversal guarantee has one construction site.
- Renamed the cluster messaging `default_max_concurrent_requests` to
  `default_messaging_max_concurrent_requests` to distinguish it from the HTTP knob.
- `deny.toml`: advisory exceptions carry `review-by` dates that `deps.sh` enforces; added
  `CDLA-Permissive-2.0` for the Mozilla CA bundle shipped via `rustls-platform-verifier`.

### Migration
- Configs binding a non-loopback address must add `[node] profile = "..."`.
- Configs with `cors_allowed_origins = ["*"]` must list explicit origins or use `[]`, unless
  the profile is `local`.
- `cameodb client ... --insecure` for a remote source URL is now `--insecure-source`.
- Build scripts passing `--features client/native-tls-vendored` must drop the flag.

### Added
- TLS/HTTPS support for the HTTP server via `axum-server` with rustls
  - Config: `[network.http.tls]` with `enabled`, `cert_file`, `key_file` fields
  - Config validation for cert/key file existence and required fields when enabled
- `--insecure` CLI flag for accepting invalid TLS certificates (self-signed certs)
  - Per-command for single operations (remote schema/data loading)
  - Per-session for interactive REPL (persists across `connect` commands)
- Docker build support for corporate CA certificates via generic `corporate-ca` secret
  - `update-ca-certificates` in initial apt-get install chain
  - `--mount=type=secret,id=corporate-ca` for BuildKit-based builds

### Changed
- Renamed Docker secret from `zscaler` to `corporate-ca` for vendor neutrality
- Fixed rust-std manual download URL: canonical path without date prefix
- Fixed tar extraction: use `-xJf` flag and `--strip-components=1`
- Updated ROADMAP.md with accurate status for Phase 12, 13, and 14

### Removed
- `CAMEODB_ACCEPT_INVALID_CERTS` environment variable (replaced with `--insecure` flag)

---

## [0.2.3] - 2026-06-30

### Added
- MCP Streamable HTTP transport mode alongside existing SSE transport
- Worker pool statistics endpoint (`GET /_admin/workers`) with per-worker and dispatch metrics
- Admin CLI commands for worker stats (`cameodb admin workers`)
- Shard-affine worker dispatch: route operations targeting the same shard to the same worker
- Hash-space alignment between worker pool and writer thread pinning
- Pinned worker runtimes: dedicated `current_thread` tokio runtimes per core when all affinity flags enabled
- Configurable Tantivy merge thread count via `StorageConfig.merge_num_threads` (default: 1)
  - Implemented via `IndexWriterOptions::builder()` with explicit `num_merge_threads()`
- Per-index memory stats (`memory_mb` field) in `/_indexes` response
- Admin memory module extracted to `crates/server/src/admin/memory.rs`
- `--force` flag for aggressive jemalloc purge bypassing decay timers
- Cross-node field-sort merge with date normalization and i64 key support
- `limit 0` as count-only query mode
- Inline query sort modifiers in search syntax

### Changed
- Upgraded kameo to 0.22, yamux to 0.14, tikv-jemalloc to 0.7
- Removed `axum-extra` dependency (inlined functionality)
- Upgraded Rust toolchain to 1.95
- Migrated cluster state serialization from bincode to JSON
- Use lenient query parsing in storage layer
- Restrict default search fields to text types only
- Upgraded core dependencies to latest versions

### Fixed
- Corrected WAL checkpoint semantics and index initialization races
- Fixed two-phase index warmup with persisted recovery metadata
- Stopped reloading readers twice per commit and warming discarded segments
- Fixed read/write pool sizing and serialized index deletion
- Corrected sequence counter initialization (Tantivy descending sort key inversion)
- Fixed federated search document sort and projection for MCP agents
- Preserved projection field order and expanded sort capabilities

---

## [0.2.2] - 2026-03-20

### Added
- MCP server integration for AI agent search capabilities
  - 6 MCP tools: `search_index`, `search_indexes`, `get_index`, `validate_query`, `get_index_stats`, `list_indexes`
  - MCP prompts capability with `cameodb-orchestrator` skill for agent context injection
  - 4 resource URIs for index exploration (indexes, metadata, schema, stats)
  - Spec-compliant SSE transport with session lifecycle management
  - Direct HTTP JSON-RPC transport mode
  - Field-type-aware query validation with syntax reference and "did you mean" suggestions
  - Compact field list and deduplicated query hints in index metadata
- Transparent gzip and zip compression support for all data sources
- Sort support for search queries with inline syntax and JSON payload options
- Inline query modifiers applied to MCP search tools
- Graceful MCP server shutdown with session cleanup
- Release build profile with thin LTO and reduced codegen units
- Comprehensive query syntax reference in MCP README

### Changed
- Upgraded workspace dependencies to latest stable versions
- Restructured MCP index metadata responses (removed schema, added compact field display)
- Streamlined Docker build configuration with unified `release-docker` profile
- Upgraded Rust toolchain to 1.94
- Improved Docker CA certificate handling with conditional corporate proxy support

### Fixed
- Corrected sequence counter initialization by inverting Tantivy descending sort keys
- Ensured all CSV fields are marked as indexed during single-pass data loading

---

## [0.2.1] - 2026-01-17

### Added
- Interactive CLI shell with rustyline-based completion, history persistence, and field-aware query suggestions
- CSV/TSV schema detection and bulk data ingestion with delimiter auto-detection
- Tab completion for schema, data, delete, and connect commands
- File path completion for data loading commands
- Interactive delete command with confirmation prompt and `--delete-schema` flag
- Connection management with `connect` command in interactive REPL
- Comprehensive JSON/JSONL/NDJSON support with automatic format detection and schema inference
- True end-to-end search streaming with incremental NDJSON response delivery
- Incremental NDJSON write-stream ingestion with bounded micro-batching
- Bounded top-K merge with score-aware pruning for distributed search results
- Bounded concurrency for scatter-gather search operations
- Field projection for search responses with inline query syntax (`return field1,field2`)
- Query modifier completion and hints for CLI search
- 4-phase graceful shutdown with tiered redb cache sizing and per-shard memory budgeting
- `u64::MAX` guard to sequence counter initialization with corruption detection
- `stream_batch_size` config and CPU-scaled shard hydration concurrency
- Background index warmup with bounded concurrency
- RPM packaging support with systemd integration and cross-compilation
- Cosign signing and verification for release artifacts
- Fingerprint-based schema versioning with pre-computed shadow field cache and routing field auto-detection
- Deterministic shard placement across multiple storage paths with balanced UUID mining
- Index-only Tantivy storage strategy to eliminate redundant field storage (50-80% index size reduction)
- Per-index idle-timeout commit supervision with async RwLock migration
- Parallel schema validation and document routing using rayon for bulk write performance
- Parallel local shard processing in bulk write operations
- Configurable WAL durability with environment variable override
- NDJSON streaming support for search results with per-hit chunked responses
- Brotli compression/decompression support
- CLI client mode with clap-based command interface
- `exact` field type with untokenized string indexing for efficient exact match queries

### Changed
- Migrated from `serde_yaml` to `serde_yml`
- Migrated route path syntax from `:param` to `{param}` for axum 0.8 compatibility
- Disabled default reqwest features and removed OpenSSL dependency from Docker build
- Replaced SHA256 with XXH3 for consistent hashing (improved performance)
- Implemented DHT-based shard discovery and ring reconstruction from persisted state
- Topology subscription pattern for real-time ring updates to orchestrator
- Event-driven cluster metadata persistence and state reconciliation
- Schema-driven field indexing with per-field Tantivy mapping and `fields_cache`
- Early-exit search optimization with local-first result merging and global limit enforcement
- Remote bulk write forwarding with shard-aware routing and XXH3-based routing hints
- Upsert semantics for Tantivy indexing (delete existing documents before add operations)
- Bidirectional shard metadata push to fix race condition between early and late joining peers
- Traffic light health model (green/yellow/red) based on missing node count
- Deterministic node identity from libp2p PeerId with push-based shard synchronization
- Standardized terminology: "bootstrap" → "seed" nodes, "writer" → "indexer" memory
- Reorganized config schema with node identity, network sections, and search defaults
- Parallel reader warmup during node startup using DashMap for concurrent caches
- Smart reader cache refresh strategy after commits
- Index statistics caching with separate fast/full modes and hybrid redb size estimation

### Fixed
- Schema cache staleness by always loading from Tantivy source of truth
- Prevented `id` field evolution during schema updates
- Date parsing robustness with Tantivy DateTime range clamping for out-of-bounds dates
- Self-dial prevention with DNS-to-IP conversion and IPv4 preference for seed node resolution
- Cluster state management with accurate node tracking and health transitions

---

## [0.2.0] - 2025-12-28

### Added
- Distributed actor system with Kameo remote actors and Docker cluster deployment
- Multi-tenant hybrid storage with production-grade optimizations
- Bulk write API with optimized batch processing and shard-aware distribution
- Automatic shard initialization and routing-key based write distribution
- Comprehensive configuration system with TOML/YAML/ENV support
- HTTP API with streaming search and channel-based result aggregation
- MicroshardActor and RouterActor with distributed search
- Index listing API with comprehensive statistics and multi-dataset ingestion support
- Dynamic memory budgets and smart commit strategy for multi-tenant performance
- Solr-style query timing metrics with shard aggregation
- Schema-driven selective indexing with PATCH API and `default_indexed=false` for new fields
- Optimized write path with routing key defaults, zero-copy serialization, and budget caching
- Schema caching in NodeOrchestrator with ordered field serialization
- Deterministic routing key derivation with document ID fallback
- Remote actor wiring and cross-node scatter-gather for distributed operations
- Routing key-based shard lookup and ring distribution tests
- Swarm event wiring to coordinator with shard registration and tracking
- ClusterCoordinator actor with message-based swarm lifecycle management
- Dedicated swarm runtime task with graceful shutdown controls
- Kademlia DHT swarm adoption with config documentation
- NodeOrchestrator `ClientOp` message handler with routing and schema validation
- Cluster-wide coordinated index deletion with single-node routing optimization
- Intelligent shard exchange with generation-based deduplication
- Lightweight index listing endpoint with schema preloading
- Cached directory size calculation with deterministic keys
- Streaming write endpoint and renamed search stream route for API consistency
- DELETE `/api/{index}` endpoint for permanent index deletion
- Environment variable support for node configuration
- Node name in cluster peer information and API responses

### Changed
- Upgraded axum to 0.8 and tower-http to 0.6
- Renamed binary from `server` to `cameodb`
- Standardized data directory path from `cameodb-data/` to `data/cameodb/`
- Replaced string-based field types with `TantivyFieldType` enum
- Enhanced schema evolution with type inference and compatibility rules
- Persisted schema updates to all local shards
- Added support for numeric, date, and boolean field types in schema evolution
- Marked new schema fields as indexed by default during evolution
- Included `id` field in schema evolution and explicit definition in index creation

### Fixed
- Applied clippy lint fixes for Rust 2024 compliance

---

## [0.1.0] - 2025-11-21

### Added
- Initial CameoDB implementation with hybrid storage and distributed topology
- Hybrid storage engine combining redb (KV store) and Tantivy (full-text search)
- Microshard architecture: each shard contains both a redb file and Tantivy directory
- Sequence ID tracking and WAL (Write-Ahead Log) for durability and recovery
- WAL replay with `get_last_indexed_seq`/`recover_index` and automatic recovery on index open
- Shadow field replacement with O(1) HashSet lookup
- Automatic index warmup on startup with recovery procedures
- Kameo-based actor system for shard management with `MicroshardActor`
- `StorageCommand` enum for thread-safe operations
- Writer thread pattern for async/sync isolation
- Consistent hashing ring for node distribution
- Basic HTTP API with search, write, and bulk operations
- Configuration system with TOML support
- Development scripts and tooling
- Project structure with workspace crates: `cluster`, `storage`, `server`, `client`
