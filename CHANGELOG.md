# Changelog

All notable changes to CameoDB are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed (security)
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
  posture, tls, remote-sources) with a single `all.sh` entry point, plus
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
