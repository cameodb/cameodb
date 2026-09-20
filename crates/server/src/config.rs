//! CameoDB Configuration Management
//!
//! This module provides comprehensive configuration management for CameoDB,
//! supporting multiple configuration sources: files, environment variables,
//! and command-line arguments.
//!
//! ## Configuration Structure
//!
//! ```toml
//! network:
//!   http:
//!     port: 9480
//!     bind_address: "0.0.0.0"
//! storage:
//!   data_paths:
//!     - "/mnt/disk1/cameodb"
//!     - "/mnt/disk2/cameodb"
//! search:
//!   indexer_memory_min_mb: 16
//!   indexer_memory_max_mb: 256
//!   memory_pressure_threshold_percent: 80
//! ```

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fs;
use std::path::PathBuf;
use thiserror::Error;
use tracing::{info, warn};

mod overrides;

#[cfg(test)]
mod tests;

pub use overrides::*;

/// Configuration errors that can occur during loading or validation
#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("Configuration file not found: {path}")]
    FileNotFound { path: String },

    #[error("Memory configuration error: {message}")]
    MemoryConfig { message: String },

    #[error("Storage configuration error: {message}")]
    StorageConfig { message: String },

    #[error("Network configuration error: {message}")]
    NetworkConfig { message: String },

    #[error("Security configuration error: {message}")]
    SecurityConfig { message: String },

    #[error("MCP configuration error: {message}")]
    McpConfig { message: String },

    #[error("{message}\n\nRun `cameodb --help` for the list of options.")]
    CommandLine { message: String },
}

/// Complete CameoDB configuration structure.
///
/// Every struct in this module carries a container-level `#[serde(default)]`, so a config
/// file may contain as much or as little as it wants: name only the settings you are
/// changing, and everything else — whole sections included — comes from [`Default`]. Each
/// Every section carries its own `Default`, built from the same `default_*()` functions its
/// `#[serde(default = "...")]` attributes use, so the two can never disagree about a value.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct CameoDbConfig {
    /// Node-level configuration (sharding, identity)
    pub node: NodeConfig,

    /// Network configuration (HTTP, cluster)
    pub network: NetworkConfig,

    /// Storage configuration (data paths, sharding)
    pub storage: StorageConfig,

    /// Search engine configuration (Tantivy settings)
    pub search: SearchConfig,

    /// Authentication: API keys, their roles, and their index scopes.
    #[serde(default)]
    pub security: crate::auth::SecurityConfig,

    /// How large a thing may get on this node: `[limits]`.
    #[serde(default)]
    pub limits: LimitsConfig,

    /// The MCP transport: `[mcp]`.
    #[serde(default)]
    pub mcp: McpConfig,

    /// Moved to `[limits] max_record_size_mb`. Read from here until 0.4.0.
    #[serde(default)]
    pub max_record_size_mb: Option<usize>,
}

/// `[mcp]` — the MCP endpoint and how long a conversation with it lives.
///
/// Deliberately not `[security.limits]`, which meters what one *key* may spend, and not
/// `[limits]`, which bounds how large a thing may get. Everything here is about the transport:
/// which of it is mounted, and how long the server keeps the state a client's session id names.
/// None of it changes what a client is told — the protocol is the same either way.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields, default)]
pub struct McpConfig {
    /// Whether `/mcp` is mounted at all (default: true).
    ///
    /// Unmounts the routes rather than refusing them, the same way `[network.http]
    /// admin_enabled` withholds the admin API: a route that is absent has nothing to probe and
    /// no guard to misconfigure.
    pub enabled: bool,

    /// How long a session may sit idle before the server forgets it (default: 1800).
    ///
    /// Idle means nothing arrived on it *and* no connection is open for it: a client holding
    /// its listening stream open is never idle, however long it pauses. So this is the gap a
    /// disconnected client may leave and still find its session — after which its next call is
    /// answered `404` and it has to `initialize` again.
    ///
    /// Thirty minutes rather than the five this used to be fixed at. A session holds no
    /// conversation state — an id, an activity clock, and the key that owns it — so holding one
    /// longer costs almost nothing, while losing one costs an agent a re-initialize in the
    /// middle of a task. `max_sessions` is what bounds the memory, not this.
    pub session_idle_timeout_secs: u64,

    /// The most sessions the node will hold at once (default: 1024).
    ///
    /// `initialize` is reachable before any rate limit and mints a session every time, so
    /// without a cap the registry's size is chosen by whoever sends requests fastest. At the cap
    /// the longest-idle session is evicted rather than the new one refused.
    pub max_sessions: usize,

    /// How often an idle SSE stream is written to (default: 15).
    ///
    /// The number that decides whether an intermediary calls the connection dead, so it has to
    /// be below the shortest idle-read timeout between this node and its clients — which is a
    /// property of someone else's load balancer, and the reason this is configurable at all.
    pub sse_keepalive_secs: u64,

    /// Whether the superseded HTTP+SSE transport is mounted (default: true).
    ///
    /// `/mcp/sse` and `/mcp/messages`, the 2024-11-05 transport. Current clients negotiate
    /// Streamable HTTP on `/mcp` and never touch these; turning them off removes the surface.
    /// On by default because turning it off strands any client still configured for it.
    pub legacy_sse_enabled: bool,

    /// The most requests one session may hold in flight at once (default: 32).
    ///
    /// Bounds the legacy transport's spawned work: `/mcp/messages` answers `202` and runs the
    /// request on a task afterwards, so without this a session's queued work grows without
    /// bound — the request's own concurrency permit and timeout end when the `202` is written.
    /// Past the bound a request is refused `429` rather than queued. Well past what an agent's
    /// parallel tool calls reach for; the loop this stops is the one that does not stop.
    pub max_in_flight_per_session: usize,
}

/// Written out rather than derived, so a config built in code and one parsed from an absent
/// `[mcp]` are the same config. A derived `Default` would zero every number here, and each of
/// them means something else at zero.
impl Default for McpConfig {
    fn default() -> Self {
        Self {
            enabled: default_mcp_enabled(),
            session_idle_timeout_secs: default_session_idle_timeout_secs(),
            max_sessions: default_max_sessions(),
            sse_keepalive_secs: default_sse_keepalive_secs(),
            legacy_sse_enabled: default_legacy_sse_enabled(),
            max_in_flight_per_session: default_max_in_flight_per_session(),
        }
    }
}

impl McpConfig {
    /// This section as the MCP crate's own transport configuration.
    ///
    /// The conversion lives here rather than in that crate so it depends on nothing of the
    /// server's — and it is the only place the seconds an operator wrote become durations.
    pub fn transport(&self) -> cameodb_mcp::McpTransportConfig {
        cameodb_mcp::McpTransportConfig {
            session_idle_timeout: std::time::Duration::from_secs(self.session_idle_timeout_secs),
            max_sessions: self.max_sessions,
            sse_keepalive: std::time::Duration::from_secs(self.sse_keepalive_secs),
            legacy_sse_enabled: self.legacy_sse_enabled,
            max_in_flight_per_session: self.max_in_flight_per_session,
        }
    }
}

/// What this node accepts and holds, as opposed to what a caller may ask for.
///
/// The size ceilings live together because they are one chain rather than four settings:
/// `max_record_size_mb` is the source of truth and the rest derive from it unless an operator
/// says otherwise.
///
/// | Derived limit                     | Formula                                     |
/// |-----------------------------------|---------------------------------------------|
/// | HTTP max body size                | `max_record_size_mb + 64` MB (overhead)     |
/// | Kameo remote request/response max | `max_record_size_mb * 1.25` (25 % headroom) |
/// | HTTP request timeout              | `max(60, max_record_size_mb / 10)` seconds  |
/// | Largest MCP search response       | the HTTP max body size — what is accepted in one message is what is sent in one |
///
/// `[security.limits]` is the other half of the question and deliberately separate: it bounds
/// what one caller may ask of the node, where this bounds what the node can do at all.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LimitsConfig {
    /// Largest single record, in MB (default: 64).
    ///
    /// The single source of truth for message size: the table above derives from it, so raising
    /// it for large documents moves every dependent limit with it.
    #[serde(default = "default_max_record_size_mb")]
    pub max_record_size_mb: usize,

    /// HTTP request body ceiling, in MB. `0` derives it from `max_record_size_mb`.
    #[serde(default)]
    pub max_body_size_mb: usize,

    /// Largest MCP search response, in bytes. Unset follows the HTTP body ceiling.
    ///
    /// A search well inside `security.limits.max_search_limit` can still be more bytes than
    /// this node carries in one message. Past this the hits that do not fit are left out and the
    /// response says so, carrying `_truncated`, `_omitted_hits` and advice to narrow the query.
    /// Set it only to go *below* the message size, which is worth doing when the callers are
    /// agents whose context is smaller than what the node can send.
    #[serde(default)]
    pub max_response_bytes: Option<usize>,

    /// Largest number of indexes held open at once. `0` derives it from the memory budget.
    ///
    /// An open index costs an indexing arena and, with it, `indexer_num_threads +
    /// merge_num_threads` OS threads. None of that is proportional to how much data the index
    /// holds, so a node whose callers choose their own index names — a tenant-per-index or
    /// date-partitioned layout — has a footprint set by a number it does not control. Past
    /// this many, the least recently used index is committed and closed; its data is untouched
    /// and the next reference to it reopens it.
    ///
    /// `0` derives rather than disables, the same way `max_body_size_mb` does: there is no
    /// setting for "unbounded", because a node that was unbounded here is what this release
    /// exists to stop. An operator who wants the old behaviour sets a number larger than the
    /// index count they expect, and owns the arithmetic.
    #[serde(default)]
    pub max_open_indexes: usize,

    /// The node's memory budget, in MB (default: 2048).
    ///
    /// Sizes the indexer pool and is what `max_body_size_mb × max_concurrent_requests` is
    /// weighed against at startup: in-flight request bodies are held in memory, so a body
    /// ceiling and a concurrency limit that multiply past this are a way to run the node out of
    /// memory from outside.
    #[serde(default = "default_total_memory_limit_mb")]
    pub total_memory_limit_mb: usize,
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            max_record_size_mb: default_max_record_size_mb(),
            max_body_size_mb: 0,
            max_open_indexes: 0,
            max_response_bytes: None,
            total_memory_limit_mb: default_total_memory_limit_mb(),
        }
    }
}

/// Network configuration wrapper
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct NetworkConfig {
    /// HTTP server configuration
    pub http: HttpConfig,

    /// Cluster configuration for distributed deployment
    #[serde(default)]
    pub cluster: ClusterConfig,
}

/// HTTP server specific configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct HttpConfig {
    /// Bind address for HTTP server (default: "127.0.0.1", loopback only)
    #[serde(default = "default_http_bind_address")]
    pub bind_address: String,

    /// Port for HTTP server (default: 9480)
    #[serde(default = "default_http_port")]
    pub port: u16,

    /// Request timeout in seconds. Unset, it is derived from
    /// [`LimitsConfig::max_record_size_mb`]; see
    /// [`CameoDbConfig::effective_request_timeout_secs`].
    ///
    /// `Option` rather than a defaulted `u64` because the two states are different answers and
    /// the node acts differently on them. It was a `u64` defaulting to 30, with "differs from
    /// the default" standing in for "the operator set it" — which made 30 the one value the
    /// file could not express, and every example config shipped with it written out.
    #[serde(default)]
    pub request_timeout_secs: Option<u64>,

    /// CORS allowed origins (default: ["*"])
    #[serde(default = "default_cors_allowed_origins")]
    pub cors_allowed_origins: Vec<String>,

    /// Maximum concurrent in-flight HTTP requests (default: 128).
    ///
    /// Requests exceeding this limit receive HTTP 503 Service Unavailable.
    /// This protects against connection-flooding DoS attacks.
    #[serde(default = "default_http_max_concurrent_requests")]
    pub max_concurrent_requests: usize,

    /// Expose the `/_admin/*` endpoints (default: true).
    ///
    /// These allow memory purges, forced commits, and writer eviction, and are
    /// unauthenticated like everything else. Turning them off removes the routes
    /// entirely, so they 404 rather than merely erroring.
    #[serde(default = "default_admin_enabled")]
    pub admin_enabled: bool,

    /// TLS configuration for HTTPS (optional)
    /// When enabled, server will use HTTPS instead of HTTP
    #[serde(default)]
    pub tls: TlsConfig,

    /// Moved to `[limits] max_body_size_mb`. Read from here until 0.4.0.
    #[serde(default)]
    pub max_body_size_mb: Option<usize>,
}

/// TLS configuration for HTTPS
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct TlsConfig {
    /// Enable TLS/HTTPS (default: false)
    #[serde(default)]
    pub enabled: bool,

    /// Path to TLS certificate file (PEM format)
    /// Required when tls.enabled = true
    #[serde(default)]
    pub cert_file: Option<PathBuf>,

    /// Path to TLS private key file (PEM format)
    /// Required when tls.enabled = true
    #[serde(default)]
    pub key_file: Option<PathBuf>,
}

/// Node-level configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct NodeConfig {
    /// Human-readable label for this node (optional, for logs/dashboards)
    #[serde(default = "default_node_label_opt")]
    pub label: Option<String>,

    /// Topology zone for rack/datacenter awareness (default: "default")
    #[serde(default = "default_node_zone")]
    pub zone: String,

    /// Security posture preset: `local`, `internal`, or `external`.
    ///
    /// Declares where this node sits on the network; the rules that go with that answer
    /// are enforced at startup (see [`crate::posture`]). When omitted, a loopback bind
    /// infers `local` and any other bind is an error — a node reachable from other hosts
    /// has to state its posture rather than inherit the most permissive one.
    #[serde(default)]
    pub profile: Option<crate::posture::Profile>,
}

/// Storage configuration for data persistence
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct StorageConfig {
    /// List of data directories for multi-disk setups
    /// Each path serves as a mount point for data storage
    pub data_paths: Vec<PathBuf>,

    /// Disk usage threshold in percent (0-100, default: 90)
    #[serde(default = "default_disk_usage_threshold_percent")]
    pub disk_usage_threshold_percent: u8,

    /// Enable WAL fsync for durability (default: true)
    #[serde(default = "default_wal_sync")]
    pub wal_sync: bool,

    /// WAL segment size in MB (default: 64)
    #[serde(default = "default_wal_segment_size_mb")]
    pub wal_segment_size_mb: usize,

    /// Default batch size for smart commit calculations (default: 1000)
    #[serde(default = "default_default_batch_size")]
    pub default_batch_size: usize,

    /// If no shards exist on first startup, create this many shards (default: 4)
    /// Set to 0 to disable automatic initialization.
    #[serde(default = "default_num_shards_init")]
    pub num_shards_init: usize,

    /// Maximum number of shards this node can host (default: 8)
    #[serde(default = "default_max_shards_per_node")]
    pub max_shards_per_node: usize,

    /// Pin per-shard writer threads to a CPU core (default: true).
    /// Each shard's writer thread is pinned to the core matching its placement
    /// ordinal, so a shard keeps its redb and tantivy structures cache-hot on one
    /// core. Measured neutral on throughput and latency, and it is what gives the
    /// two flags below something to align to; no reason to turn it off.
    /// A no-op on platforms that cannot pin (macOS), where it is reported as
    /// requested-but-refused rather than silently ignored.
    #[serde(default = "default_writer_core_affinity")]
    pub writer_core_affinity: bool,

    /// Enable shard-affine worker dispatch (default: false).
    /// Routes operations for a shard to the worker whose index matches that
    /// shard's placement ordinal, so the op and the writer it hands off to share
    /// a core. Scatter-gather operations still dispatch round-robin.
    ///
    /// **Measured a regression twice; leave it off.** On an 8-core Linux node with
    /// 8 shards it cost 13-20% of write throughput and roughly doubled write p90.
    /// That was first blamed on workers running one operation at a time, halved by
    /// the `worker_count` this flag forces down to `cores`. Workers now carry eight
    /// each, and re-measuring changed nothing: still 24% off write throughput at
    /// concurrency 64, with every affine repeat below every default repeat.
    ///
    /// The cause is the constraint itself. A job for shard S may only run on worker
    /// `S % worker_count`, so any instantaneous skew across shards leaves workers
    /// idle while their neighbours queue; round-robin cannot be unlucky that way.
    /// Neutral for searches, which dispatch round-robin regardless.
    #[serde(default = "default_shard_affine_dispatch")]
    pub shard_affine_dispatch: bool,

    /// Pin orchestrator workers to CPU cores as dedicated OS threads
    /// (default: false). Requires `shard_affine_dispatch = true` AND
    /// `writer_core_affinity = true` to take effect; otherwise silently no-op.
    ///
    /// Each worker runs on its own `tokio::current_thread` runtime pinned to
    /// `core_for(worker_id)`, which together with the dispatch flag puts the worker
    /// and the writer for the same shard on one core.
    ///
    /// **Measured a regression; leave it off.** It adds nothing to the write cost
    /// of `shard_affine_dispatch` above, and takes a further 11% off search
    /// throughput with p99 up by half (6 326 -> 5 618 ok/s, 5.75ms -> 8.62ms on an
    /// 8-core node) — searches are CPU-heavy and fan out across every shard, so
    /// confining the driving worker to one core hurts. Re-confirmed after workers
    /// gained per-operation concurrency; that changed neither figure.
    #[serde(default = "default_worker_core_affinity")]
    pub worker_core_affinity: bool,
}

impl StorageConfig {
    /// Sort and de-duplicate data paths to ensure deterministic ordering.
    pub fn normalize_paths(&mut self) {
        self.data_paths.retain(|path| !path.as_os_str().is_empty());
        self.data_paths.sort();
        self.data_paths.dedup();
    }

    /// Return the primary data path (first in sorted list), if configured.
    pub fn primary_path(&self) -> Option<&PathBuf> {
        self.data_paths.first()
    }
}

/// Cluster configuration for distributed actor system
#[derive(Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ClusterConfig {
    /// Enable distributed cluster mode (default: false)
    #[serde(default = "default_cluster_enabled")]
    pub enabled: bool,

    /// Bind address for cluster communication (default: "0.0.0.0")
    #[serde(default = "default_cluster_bind_address")]
    pub bind_address: String,

    /// Cluster communication port for libp2p (default: 9580)
    #[serde(default = "default_cluster_port")]
    pub cluster_port: u16,

    /// Seed nodes for initial cluster discovery
    #[serde(default)]
    pub seed_nodes: Vec<String>,

    /// Optional: Expected cluster nodes for validation (not used for strict cluster formation)
    /// Used to compare against discovered nodes and emit warnings if mismatched
    /// Format: same as seed_nodes (e.g., "/ip4/10.0.1.5/tcp/9580" or "hostname:port")
    #[serde(default)]
    pub cluster_nodes: Vec<String>,

    // Peer discovery handled by Kademlia DHT
    /// Cluster name for isolation (default: "cameodb-cluster")
    #[serde(default = "default_cluster_name")]
    pub cluster_name: String,

    /// Listen addresses for swarm (default: auto)
    #[serde(default)]
    pub listen_addrs: Vec<String>,

    /// Bootstrap peers with peer IDs (format: "/ip4/1.2.3.4/tcp/9580/p2p/12D3KooW...")
    #[serde(default)]
    pub bootstrap_peers: Vec<String>,

    /// Inline pre-shared key (PSK) for cluster join authentication.
    /// 32-byte key hex-encoded (64 characters). When set, all TCP connections
    /// are wrapped with XSalsa20 encryption. Peers without the matching key
    /// cannot join the cluster. QUIC is disabled when PSK is enabled.
    ///
    /// Never serialized: the value is read from config but omitted from any output, so a
    /// config dump cannot leak it. Prefer `psk_file` — an inline key is visible in `ps`
    /// when it arrives via `--cluster-psk`.
    #[serde(default, skip_serializing)]
    pub psk: Option<String>,

    /// Path to a file containing the cluster PSK (same format as `psk`).
    /// Useful for secrets management — the file can have restricted permissions.
    /// If both `psk` and `psk_file` are set, `psk` takes precedence.
    #[serde(default)]
    pub psk_file: Option<PathBuf>,

    /// Messaging configuration
    #[serde(default)]
    pub messaging: MessagingConfig,
}

impl fmt::Debug for ClusterConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClusterConfig")
            .field("enabled", &self.enabled)
            .field("bind_address", &self.bind_address)
            .field("cluster_port", &self.cluster_port)
            .field("seed_nodes", &self.seed_nodes)
            .field("cluster_nodes", &self.cluster_nodes)
            .field("cluster_name", &self.cluster_name)
            .field("listen_addrs", &self.listen_addrs)
            .field("bootstrap_peers", &self.bootstrap_peers)
            .field("psk", &self.psk.as_ref().map(|_| "<redacted>"))
            .field("psk_file", &self.psk_file)
            .field("messaging", &self.messaging)
            .finish()
    }
}

/// Messaging configuration for Kameo remote actors
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct MessagingConfig {
    /// Request timeout in seconds for inter-node asks. Unset, it follows the HTTP timeout; see
    /// [`CameoDbConfig::effective_remote_timeout_secs`].
    ///
    /// `Option` for the same reason as its HTTP counterpart.
    #[serde(default)]
    pub request_timeout_secs: Option<u64>,

    /// Maximum concurrent requests per peer (default: 100)
    #[serde(default = "default_messaging_max_concurrent_requests")]
    pub max_concurrent_requests: usize,

    /// Connection pool size (default: 10)
    #[serde(default = "default_connection_pool_size")]
    pub connection_pool_size: usize,

    /// How many remote retry attempts to perform before surfacing an error
    #[serde(default = "default_remote_retry_attempts")]
    pub remote_retry_attempts: u8,

    /// Timeout for broadcast scatter-gather operations in seconds
    #[serde(default = "default_broadcast_timeout_secs")]
    pub broadcast_timeout_secs: u64,

    /// Maximum number of local shards to fan out to when broadcasting
    #[serde(default = "default_broadcast_fanout_limit")]
    pub broadcast_fanout_limit: usize,
}

/// Tantivy search engine configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SearchConfig {
    /// Minimum indexer memory in MB (default: 16)
    #[serde(default = "default_indexer_memory_min_mb")]
    pub indexer_memory_min_mb: usize,

    /// Maximum indexer memory in MB (default: 256)
    #[serde(default = "default_indexer_memory_max_mb")]
    pub indexer_memory_max_mb: usize,

    /// Memory pressure threshold in percent (0-100, default: 80)
    #[serde(default = "default_memory_pressure_threshold_percent")]
    pub memory_pressure_threshold_percent: u8,

    /// Maximum number of searches executing concurrently on this node (default: 8).
    ///
    /// Sizes the dedicated read pool's blocking threads, which is where search and stats
    /// work actually runs. Queries beyond this limit queue rather than adding threads, so
    /// raising it trades memory and CPU contention for concurrency. Setting it to 0 derives
    /// `max(2, cpu_cores / 2)`.
    ///
    /// Note this bounds concurrency across all queries; `max_concurrent_shard_searches`
    /// separately bounds the shard fan-out of a single query, and cannot exceed this in
    /// practice for local shards.
    #[serde(default = "default_search_threads")]
    pub search_threads: usize,

    /// Enable streaming search results for improved performance
    #[serde(default = "default_enable_streaming_search")]
    pub enable_streaming_search: bool,

    /// Maximum concurrent local shard searches when streaming
    #[serde(default = "default_max_concurrent_shard_searches")]
    pub max_concurrent_shard_searches: usize,

    /// Maximum concurrent remote node searches when streaming
    #[serde(default = "default_max_concurrent_remote_searches")]
    pub max_concurrent_remote_searches: usize,

    /// Accepted and inert. Kept so a config that sets it still loads.
    ///
    /// It gated one `break` in the streaming merge, and that `break` was only ever reachable
    /// when no work remained to skip — so it saved nothing — while discarding a source's results
    /// that had already arrived. Any search whose limit was below the match count lost a whole
    /// node from both its count and its merge: `total_hits` came back 29 or 33 where the answer
    /// was 46, and a sorted top-5 over three nodes returned the wrong five documents depending
    /// on which node answered last.
    ///
    /// There is no safe early exit to put in its place. Stopping a scatter-gather before every
    /// source has answered means a merged top-k that may not hold the top k, and a `total_hits`
    /// that under-reports — both silently. A correct one would need a per-source bound on the
    /// best score or sort value it could still contribute, which nothing here computes.
    #[serde(default = "default_enable_early_termination")]
    pub enable_early_termination: bool,

    /// Default search result limit when not specified in request (default: 10)
    #[serde(default = "default_search_limit")]
    pub default_search_limit: usize,

    /// Seconds of write inactivity on an index before it is committed anyway (default: 5).
    ///
    /// The safety net under the operation-count threshold. Writes are committed once enough
    /// have accumulated, which is what keeps steady ingest cheap; a trickle that never
    /// reaches the threshold would otherwise stay uncommitted — and therefore unsearchable —
    /// until the next write arrived. This bounds that window.
    ///
    /// Lower it to make small writes visible to search sooner, at the cost of more frequent
    /// commits and the segment churn that follows.
    #[serde(default = "default_supervisor_timeout_secs")]
    pub supervisor_timeout_secs: u64,

    /// Number of documents per micro-batch when ingesting NDJSON write streams (default: 500)
    #[serde(default = "default_stream_batch_size")]
    pub stream_batch_size: usize,

    /// Number of indexing worker threads per tantivy IndexWriter (default: 1).
    /// Each worker creates one segment per commit.
    #[serde(default = "default_indexer_num_threads")]
    pub indexer_num_threads: usize,

    /// Number of background merge (compaction) threads per IndexWriter (default: 2).
    ///
    /// Tantivy's own default is 4, which on memory-constrained nodes with many indices
    /// causes mmap storms. Two rather than one leaves headroom to merge in parallel while
    /// the node is under load, instead of serialising compaction behind a single thread.
    ///
    /// Note this is *per open index*, so the thread count grows with how many indices are
    /// open, not with shard count. Scale up on nodes with ample RAM and few indices.
    #[serde(default = "default_merge_num_threads")]
    pub merge_num_threads: usize,

    /// Moved to `[limits] total_memory_limit_mb`. Read from here until 0.4.0.
    #[serde(default)]
    pub total_memory_limit_mb: Option<usize>,
}

impl CameoDbConfig {
    /// Effective HTTP max body size in MB.
    ///
    /// If the user set `limits.max_body_size_mb` explicitly (non-zero),
    /// that value wins.  Otherwise it is derived as `max_record_size_mb + 64`
    /// to leave headroom for JSON framing, bulk-write arrays, etc.
    pub fn effective_max_body_size_mb(&self) -> usize {
        if self.limits.max_body_size_mb > 0 {
            self.limits.max_body_size_mb
        } else {
            self.limits.max_record_size_mb + 64
        }
    }

    /// Largest number of indexes held open at once, across the node.
    ///
    /// Derived from the memory budget when unset, for the same reason the body ceiling is
    /// derived from the record size: an operator who has already told the node how much memory
    /// it may use has said most of what is needed, and a second unrelated number to keep in
    /// step with the first is a number that drifts. One writer arena per open index, at the
    /// smallest size an arena is ever given, is the worst case this divides out.
    ///
    /// Clamped at both ends. The floor keeps a node with a small budget usable — below a
    /// handful of indexes the cap would be evicting on nearly every request. The ceiling is
    /// about the *other* resource an open index costs: at three OS threads apiece, a derived
    /// cap in the thousands would bound the megabytes and let the thread count be the thing
    /// that takes the node down.
    pub fn effective_max_open_indexes(&self) -> usize {
        if self.limits.max_open_indexes > 0 {
            return self.limits.max_open_indexes;
        }
        const FLOOR: usize = 8;
        const CEILING: usize = 256;
        let arena_mb = self.search.indexer_memory_min_mb.max(1);
        (self.limits.total_memory_limit_mb / arena_mb).clamp(FLOOR, CEILING)
    }

    /// Largest MCP search response in **bytes**.
    ///
    /// Derived from the HTTP body limit rather than chosen independently: a response is a
    /// message, and the node has already been told how large one message may be. Deriving it
    /// means an operator who raises `max_record_size_mb` for large documents does not then find
    /// searches over those documents trimmed by a bound nobody moved.
    ///
    /// `[security.limits] max_response_bytes` overrides it, and is worth setting when the
    /// callers are agents whose context is smaller than the node's message size.
    pub fn effective_max_response_bytes(&self) -> usize {
        self.limits
            .max_response_bytes
            .unwrap_or_else(|| self.effective_max_body_size_mb() * 1024 * 1024)
    }

    /// Effective Kameo remote messaging size limit in **bytes**.
    ///
    /// The envelope must accommodate a single large record plus serialization
    /// overhead (JSON framing, field names, routing metadata).  We add 25 %
    /// headroom on top of the configured record size.
    pub fn effective_remote_message_size_bytes(&self) -> usize {
        // max_record_size_mb converted to bytes + 25 % overhead
        let base = self.limits.max_record_size_mb * 1024 * 1024;
        base + base / 4
    }

    /// Effective HTTP request timeout in seconds.
    ///
    /// Written, it is honoured — any value, including one equal to a default. Unset, it scales
    /// with record size: `max(60, max_record_size_mb / 10)`, because a node that accepts a
    /// 2 GB record has to allow time to receive one.
    ///
    /// The floor in that formula is also what makes a written value sound or not:
    /// `max_record_size_mb / 10` is the time a maximum-size record needs at the ~10 MB/s the
    /// derivation assumes, so a timeout under it means the configured record size can never be
    /// received. [`Self::timeout_floor_secs`] computes it and `validate` warns on it; the
    /// value is still honoured, because a search-only node that wants a short timeout is
    /// entitled to one.
    pub fn effective_request_timeout_secs(&self) -> u64 {
        self.network
            .http
            .request_timeout_secs
            .unwrap_or_else(|| self.timeout_floor_secs().max(60))
    }

    /// Seconds a maximum-size record needs on the wire, at the ~10 MB/s the derived timeout
    /// assumes. The lower bound a written timeout is measured against, and the variable part
    /// of the derived one.
    pub fn timeout_floor_secs(&self) -> u64 {
        (self.limits.max_record_size_mb as u64) / 10
    }

    /// Effective Kameo remote messaging timeout in seconds.
    ///
    /// Unset, it follows the HTTP timeout, so that a forwarded request is not cut short before
    /// the origin request it is serving has given up. Every consumer of a remote deadline must
    /// come through here rather than read the field: `RouterActor` read the raw field and so
    /// forwarded with 30 s under a 60 s HTTP timeout, which is the bug this accessor exists to
    /// prevent.
    pub fn effective_remote_timeout_secs(&self) -> u64 {
        self.network
            .cluster
            .messaging
            .request_timeout_secs
            .unwrap_or_else(|| self.effective_request_timeout_secs())
    }

    /// Load configuration from every source, in this order of precedence:
    ///
    /// 1. Command-line arguments (`--http-port 9999`) — highest
    /// 2. Environment variables (`CAMEODB_HTTP_PORT=9999`)
    /// 3. Configuration file (`--config <path>`, `CAMEODB_CONFIG`, or the search list in
    ///    [`CameoDbConfig::load_from_file`])
    /// 4. Defaults — lowest
    ///
    /// The same order picks the config file itself: `--config` wins over `CAMEODB_CONFIG`.
    /// Every overridable setting has both a flag and an environment variable, defined once as
    /// a pair in [`OVERRIDES`], so the two layers can never drift apart.
    pub fn load_with_cli(cli: &CliOverrides) -> Result<Self> {
        let config = Self::load_unvalidated(cli)?;
        config.validate()?;
        Ok(config)
    }

    /// Resolve the configuration from all sources without validating it.
    ///
    /// Only for tooling that needs to *report* on an invalid config — `check-config` has
    /// to show which rules a bad file breaks, which it cannot do if loading refuses to
    /// hand it over. Server startup always goes through [`Self::load_with_cli`].
    pub fn load_unvalidated(cli: &CliOverrides) -> Result<Self> {
        // Start with defaults
        let mut config = Self::default();

        // Layer the config file on top, if there is one
        match Self::load_from_file(cli.config_path.as_deref()) {
            // The file is already a complete config — every field it omits arrived at
            // its serde default inside the parse — so layering is replacement, not merge.
            Ok(file_config) => config = file_config,
            // Only a *missing* file from the implicit search list is survivable. A file the
            // operator explicitly named, or one that exists but does not parse, is an error:
            // booting on defaults because a config was unreadable is how a node quietly comes
            // up on the wrong port with the wrong data directory.
            Err(e) => match e.downcast_ref::<ConfigError>() {
                Some(ConfigError::FileNotFound { .. }) if !cli.has_explicit_config_path() => {
                    info!("No configuration file found; using defaults, environment and flags");
                }
                _ => return Err(e),
            },
        }

        // Apply environment then command-line overrides — command line last, so it wins.
        config = Self::apply_overrides(config, cli)?;

        // Normalize storage paths for deterministic behavior
        config.storage.normalize_paths();

        Ok(config)
    }

    /// Load configuration from a YAML or TOML file.
    ///
    /// `cli_path` is the `--config` argument; it takes precedence over `CAMEODB_CONFIG`, per
    /// the precedence documented on [`CameoDbConfig::load_with_cli`]. When both are set and
    /// disagree, the losing one is logged rather than silently ignored. If either is set, the
    /// implicit search list is skipped entirely — an operator who named a file gets that file
    /// or an error, never a different one.
    pub fn load_from_file(cli_path: Option<&str>) -> Result<Self> {
        let env_path = std::env::var("CAMEODB_CONFIG").ok();

        if let (Some(env_path), Some(cli_path)) = (env_path.as_deref(), cli_path)
            && env_path != cli_path
        {
            info!(
                "--config {} overrides CAMEODB_CONFIG={} (command line has higher precedence)",
                cli_path, env_path
            );
        }

        if let Some(path) = cli_path.or(env_path.as_deref()) {
            let content = fs::read_to_string(path)
                .with_context(|| format!("Failed to read config file: {path}"))?;
            info!("📄 Loading configuration from: {}", path);
            return Self::parse_config_content(&content, path)
                .with_context(|| format!("Failed to parse config file: {path}"));
        }

        let config_paths = [
            "cameodb.toml",
            "cameodb.yaml",
            "cameodb.yml",
            "config/cameodb.toml",
            "config/cameodb.yaml",
            "/etc/cameodb/cameodb.toml",
            "/etc/cameodb/config.toml",
        ];

        for path in &config_paths {
            if let Ok(content) = fs::read_to_string(path) {
                info!("📄 Loading configuration from: {}", path);
                return Self::parse_config_content(&content, path)
                    .with_context(|| format!("Failed to parse config file: {path}"));
            }
        }

        Err(ConfigError::FileNotFound {
            path: config_paths.join(", "),
        }
        .into())
    }

    /// Parse configuration content based on file extension.
    ///
    /// Files are partial by design (see [`CameoDbConfig`]), which means a misspelled key is
    /// indistinguishable from an omitted one — it silently leaves the default in place. So
    /// every key that survived parsing without landing anywhere is reported.
    fn parse_config_content(content: &str, path: &str) -> Result<Self> {
        let mut config: Self = if path.ends_with(".toml") {
            toml::from_str(content).with_context(|| "Failed to parse TOML configuration")?
        } else if path.ends_with(".yaml") || path.ends_with(".yml") {
            serde_saphyr::from_str(content).with_context(|| "Failed to parse YAML configuration")?
        } else {
            // Try TOML first, then YAML
            toml::from_str(content)
                .or_else(|_| serde_saphyr::from_str(content))
                .with_context(|| "Failed to parse configuration (tried TOML and YAML)")?
        };

        config.adopt_moved_settings(path);

        for key in unrecognized_keys(content) {
            warn!("Ignoring unknown setting in {}: {}", path, key);
        }

        Ok(config)
    }

    /// Fold the pre-`[limits]` spellings into `[limits]`, naming where each one went.
    ///
    /// The four ceilings were scattered across three tables and the file root before they
    /// were grouped, and an operator's file outlives a release. Each old key is still read
    /// and applied, so an upgrade changes nothing until the file is edited, and each one
    /// warns, so the edit is not deferred forever. They go in 0.4.0.
    ///
    /// `[limits]` wins where it says anything: an old key is adopted only when its
    /// counterpart is still at the default, which is the closest thing to "the operator did
    /// not set it" that survives both TOML and YAML.
    fn adopt_moved_settings(&mut self, path: &str) {
        let defaults = LimitsConfig::default();

        adopt_moved(
            path,
            "max_record_size_mb",
            "max_record_size_mb",
            self.max_record_size_mb.take(),
            &mut self.limits.max_record_size_mb,
            &defaults.max_record_size_mb,
        );
        adopt_moved(
            path,
            "network.http.max_body_size_mb",
            "max_body_size_mb",
            self.network.http.max_body_size_mb.take(),
            &mut self.limits.max_body_size_mb,
            &defaults.max_body_size_mb,
        );
        adopt_moved(
            path,
            "search.total_memory_limit_mb",
            "total_memory_limit_mb",
            self.search.total_memory_limit_mb.take(),
            &mut self.limits.total_memory_limit_mb,
            &defaults.total_memory_limit_mb,
        );
        adopt_moved(
            path,
            "security.limits.max_response_bytes",
            "max_response_bytes",
            self.security.limits.max_response_bytes.take(),
            &mut self.limits.max_response_bytes,
            &defaults.max_response_bytes,
        );
    }

    /// Apply the environment and then the command line over `config`.
    ///
    /// Both layers walk the same [`OVERRIDES`] table and hand the same raw string to the same
    /// setter, so a flag and its environment variable cannot disagree about what a value
    /// means. The command line is applied second and therefore wins.
    fn apply_overrides(mut config: Self, cli: &CliOverrides) -> Result<Self> {
        for entry in OVERRIDES {
            if let Ok(value) = std::env::var(entry.env) {
                (entry.apply)(&mut config, &value)
                    .with_context(|| format!("Invalid {}: {value:?}", entry.env))?;
            }

            if let Some(value) = cli.value_for(entry.flag) {
                (entry.apply)(&mut config, value)
                    .with_context(|| format!("Invalid {}: {value:?}", entry.flag))?;
            }
        }

        // Guard: default_search_limit must be >= 1 to prevent tantivy panic
        if config.search.default_search_limit == 0 {
            warn!("Configured default_search_limit is 0, clamping to 1");
            config.search.default_search_limit = 1;
        }

        Ok(config)
    }

    /// Validate the configuration for consistency and constraints
    pub fn validate(&self) -> Result<()> {
        self.validate_security()?;
        self.validate_mcp()?;
        self.validate_network()?;
        self.validate_storage()?;
        self.validate_memory()?;

        // A trail configured into uselessness — a zero-length buffer, a zero-slot queue —
        // is caught here rather than discovered empty during the incident it was turned on
        // for.
        self.security
            .audit
            .validate()
            .map_err(|message| ConfigError::SecurityConfig { message })?;

        // Posture rules run last: the checks above establish that individual values are
        // usable, and this decides whether the combination is allowed where this node sits.
        self.check_posture()?;

        Ok(())
    }

    fn validate_security(&self) -> Result<()> {
        // A ceiling of zero would mean no search may return anything. Refused rather than read
        // as "no ceiling", because a bound whose zero inverts its meaning is a trap: an
        // operator who wants a high ceiling should write a high number.
        if self.security.limits.max_search_limit == 0 {
            return Err(ConfigError::SecurityConfig {
                message: "security.limits.max_search_limit is 0, which would refuse every \
                          search; set the largest limit a search may ask for"
                    .to_string(),
            }
            .into());
        }

        // Nothing fits in zero bytes, and one hit always survives the trim — so a ceiling of
        // zero would not refuse a response, it would just describe every response as truncated.
        if self.limits.max_response_bytes == Some(0) {
            return Err(ConfigError::SecurityConfig {
                message: "limits.max_response_bytes is 0, which would report every response as \
                          truncated; set the largest response a search may return"
                    .to_string(),
            }
            .into());
        }

        // A default above the ceiling contradicts itself, and the contradiction is invisible
        // from the caller's side: a search naming no limit is filled in with the default, and
        // would then run past the bound the MCP tools advertise. Refused rather than clamped,
        // so that the number an operator wrote is the number that runs — or startup says why
        // not.
        if self.search.default_search_limit > self.security.limits.max_search_limit {
            return Err(ConfigError::SecurityConfig {
                message: format!(
                    "search.default_search_limit ({}) is above \
                     security.limits.max_search_limit ({}); a search that names no limit \
                     would exceed the ceiling the MCP tools advertise",
                    self.search.default_search_limit, self.security.limits.max_search_limit
                ),
            }
            .into());
        }

        // A fan-out of zero would refuse every federated search, and there is no reading of
        // "no bound" that is a number — the same trap as `max_search_limit` above.
        if self.security.limits.max_federated_indexes == 0 {
            return Err(ConfigError::SecurityConfig {
                message: "security.limits.max_federated_indexes is 0, which would refuse every \
                          federated search; set the most indexes one search may name"
                    .to_string(),
            }
            .into());
        }

        Ok(())
    }

    fn validate_mcp(&self) -> Result<()> {
        // A timeout of zero expires a session before its next request can arrive, so every
        // call after `initialize` would be answered 404. Refused rather than read as "never
        // expire", which is what an operator wanting a long-lived session should write as a
        // long number.
        if self.mcp.session_idle_timeout_secs == 0 {
            return Err(ConfigError::McpConfig {
                message: "mcp.session_idle_timeout_secs is 0, which would expire every session \
                          before its next request; set how long a client may pause"
                    .to_string(),
            }
            .into());
        }

        // Zero sessions means `initialize` mints one and immediately evicts it.
        if self.mcp.max_sessions == 0 {
            return Err(ConfigError::McpConfig {
                message: "mcp.max_sessions is 0, which would evict every session as it is \
                          created; set how many concurrent MCP clients this node will hold"
                    .to_string(),
            }
            .into());
        }

        // A keep-alive of zero writes to every open stream as fast as the runtime will let it.
        if self.mcp.sse_keepalive_secs == 0 {
            return Err(ConfigError::McpConfig {
                message: "mcp.sse_keepalive_secs is 0, which would write to every open stream \
                          continuously; set how often an idle stream is kept alive"
                    .to_string(),
            }
            .into());
        }

        // Zero in flight means every legacy request is refused the moment it arrives — the
        // transport would accept the session and then refuse the work forever.
        if self.mcp.max_in_flight_per_session == 0 {
            return Err(ConfigError::McpConfig {
                message: "mcp.max_in_flight_per_session is 0, which would refuse every request \
                          on a session; set how many in-flight requests one session may hold"
                    .to_string(),
            }
            .into());
        }

        // A keep-alive that fires less often than a session expires cannot hold one open: the
        // stream would be swept between two writes. Refused because the two settings look
        // independent and are not — an operator who shortened the timeout for a reason should
        // hear that this is the number that now decides it.
        if self.mcp.sse_keepalive_secs >= self.mcp.session_idle_timeout_secs {
            return Err(ConfigError::McpConfig {
                message: format!(
                    "mcp.sse_keepalive_secs ({}) is not below mcp.session_idle_timeout_secs \
                     ({}); a stream written to less often than its session expires cannot keep \
                     that session open",
                    self.mcp.sse_keepalive_secs, self.mcp.session_idle_timeout_secs
                ),
            }
            .into());
        }

        Ok(())
    }

    fn validate_network(&self) -> Result<()> {
        if self.network.http.port == 0 {
            return Err(ConfigError::NetworkConfig {
                message: "HTTP port cannot be 0".to_string(),
            }
            .into());
        }

        if self.network.http.request_timeout_secs == Some(0) {
            return Err(ConfigError::NetworkConfig {
                message: "Request timeout must be positive".to_string(),
            }
            .into());
        }

        if self.network.cluster.messaging.request_timeout_secs == Some(0) {
            return Err(ConfigError::NetworkConfig {
                message: "Cluster messaging request timeout must be positive".to_string(),
            }
            .into());
        }

        // A written timeout is honoured whatever it says, but one below the time a
        // maximum-size record needs on the wire means `max_record_size_mb` describes a record
        // this node will never finish receiving. Warn rather than refuse: a short timeout is a
        // reasonable choice on a node that serves searches and accepts no large writes, and
        // refusing would make that node unconfigurable.
        let floor = self.timeout_floor_secs();
        if let Some(written) = self.network.http.request_timeout_secs
            && written < floor
        {
            warn!(
                "network.http.request_timeout_secs is {written}s, below the {floor}s a \
                 {}MB record needs to arrive. Honouring {written}s — writes near the record \
                 limit will time out. Raise the timeout or lower limits.max_record_size_mb.",
                self.limits.max_record_size_mb
            );
        }

        // The remote deadline serves an HTTP request that has its own. Shorter, a forwarded
        // ask is abandoned while the client is still waiting, turning a slow cross-node read
        // into an error the origin had time to avoid.
        let remote = self.effective_remote_timeout_secs();
        let http = self.effective_request_timeout_secs();
        if remote < http {
            warn!(
                "network.cluster.messaging.request_timeout_secs resolves to {remote}s, under \
                 the {http}s HTTP timeout: a forwarded request gives up while its client is \
                 still waiting."
            );
        }

        // Validate TLS configuration
        if self.network.http.tls.enabled {
            if self.network.http.tls.cert_file.is_none() {
                return Err(ConfigError::NetworkConfig {
                    message: "TLS enabled but cert_file not configured".to_string(),
                }
                .into());
            }
            if self.network.http.tls.key_file.is_none() {
                return Err(ConfigError::NetworkConfig {
                    message: "TLS enabled but key_file not configured".to_string(),
                }
                .into());
            }

            // Validate TLS files exist
            if let Some(cert_file) = &self.network.http.tls.cert_file
                && !cert_file.exists()
            {
                return Err(ConfigError::NetworkConfig {
                    message: format!("TLS certificate file not found: {}", cert_file.display()),
                }
                .into());
            }
            if let Some(key_file) = &self.network.http.tls.key_file
                && !key_file.exists()
            {
                return Err(ConfigError::NetworkConfig {
                    message: format!("TLS key file not found: {}", key_file.display()),
                }
                .into());
            }
        }

        // Validate record size limit
        if self.limits.max_record_size_mb == 0 {
            return Err(ConfigError::NetworkConfig {
                message: "limits.max_record_size_mb must be positive".to_string(),
            }
            .into());
        }

        // Validate concurrency limit
        if self.network.http.max_concurrent_requests == 0 {
            return Err(ConfigError::NetworkConfig {
                message: "max_concurrent_requests must be positive".to_string(),
            }
            .into());
        }

        // Validate CORS origins. An unparseable origin would otherwise be dropped
        // silently when building the CORS layer, turning a typo into deny-all.
        let cors_origins = &self.network.http.cors_allowed_origins;
        if cors_origins.iter().any(|o| o == "*") && cors_origins.len() > 1 {
            return Err(ConfigError::NetworkConfig {
                message: "cors_allowed_origins cannot mix \"*\" with specific origins".to_string(),
            }
            .into());
        }
        for origin in cors_origins {
            if origin == "*" {
                continue;
            }
            if origin.parse::<axum::http::HeaderValue>().is_err() {
                return Err(ConfigError::NetworkConfig {
                    message: format!("invalid CORS origin '{}': not a valid header value", origin),
                }
                .into());
            }
            if !origin.starts_with("http://") && !origin.starts_with("https://") {
                return Err(ConfigError::NetworkConfig {
                    message: format!(
                        "invalid CORS origin '{}': must include scheme (http:// or https://)",
                        origin
                    ),
                }
                .into());
            }
        }

        // Validate the cluster PSK by loading it through exactly the same path the swarm
        // will use at startup. Re-implementing the format check here is what previously
        // left three copies of the same rules free to drift apart.
        self.network
            .cluster
            .load_psk()
            .map_err(|e| ConfigError::NetworkConfig {
                message: e.to_string(),
            })?;

        // pnet only wraps TCP, so enabling a PSK silently drops QUIC support. An address
        // that can never be used should fail here rather than as a dial error at runtime.
        if self.network.cluster.psk.is_some() || self.network.cluster.psk_file.is_some() {
            let quic_addrs: Vec<&String> = self
                .network
                .cluster
                .listen_addrs
                .iter()
                .chain(self.network.cluster.seed_nodes.iter())
                .chain(self.network.cluster.bootstrap_peers.iter())
                .filter(|a| a.contains("/quic"))
                .collect();
            if !quic_addrs.is_empty() {
                return Err(ConfigError::NetworkConfig {
                    message: format!(
                        "cluster PSK is set, which disables QUIC (pnet wraps TCP only), but these \
                         addresses use QUIC: {:?}. Use /tcp/ addresses or remove the PSK",
                        quic_addrs
                    ),
                }
                .into());
            }
        }

        Ok(())
    }

    fn validate_storage(&self) -> Result<()> {
        if self.storage.data_paths.is_empty() {
            return Err(ConfigError::StorageConfig {
                message: "At least one data path must be specified".to_string(),
            }
            .into());
        }

        if self.storage.disk_usage_threshold_percent > 100 {
            return Err(ConfigError::StorageConfig {
                message: "Disk usage threshold must be between 0 and 100 percent".to_string(),
            }
            .into());
        }

        Ok(())
    }

    fn validate_memory(&self) -> Result<()> {
        if self.search.indexer_memory_min_mb < 16 {
            return Err(ConfigError::MemoryConfig {
                message: "Indexer memory minimum cannot be less than 16MB".to_string(),
            }
            .into());
        }

        if self.search.indexer_memory_max_mb > 4096 {
            return Err(ConfigError::MemoryConfig {
                message: "Indexer memory maximum cannot exceed 4096MB".to_string(),
            }
            .into());
        }

        if self.search.indexer_memory_min_mb >= self.search.indexer_memory_max_mb {
            return Err(ConfigError::MemoryConfig {
                message: "Indexer memory minimum must be less than maximum".to_string(),
            }
            .into());
        }

        if self.search.memory_pressure_threshold_percent > 100 {
            return Err(ConfigError::MemoryConfig {
                message: "Memory pressure threshold must be between 0 and 100 percent".to_string(),
            }
            .into());
        }

        if self.limits.total_memory_limit_mb < self.search.indexer_memory_max_mb {
            return Err(ConfigError::MemoryConfig {
                message: "Total memory limit must be at least as large as max indexer memory"
                    .to_string(),
            }
            .into());
        }

        Ok(())
    }

    /// Evaluate the security posture and reject a config that contradicts its profile.
    ///
    /// Warnings are logged rather than returned — they describe accepted risk, and a
    /// posture that blocked on every one of them would just teach operators to pick a
    /// weaker profile.
    pub fn check_posture(&self) -> Result<crate::posture::Posture> {
        let posture = crate::posture::evaluate(self)
            .map_err(|message| ConfigError::NetworkConfig { message })?;

        for check in posture.warnings() {
            warn!(
                profile = %posture.profile,
                rule = check.rule,
                "posture: {}",
                check.outcome.message()
            );
        }

        let failures: Vec<String> = posture
            .failures()
            .map(|c| format!("[{}] {}", c.rule, c.outcome.message()))
            .collect();
        if !failures.is_empty() {
            return Err(ConfigError::NetworkConfig {
                message: format!(
                    "security profile '{}' rejected this configuration:\n  - {}",
                    posture.profile,
                    failures.join("\n  - ")
                ),
            }
            .into());
        }

        Ok(posture)
    }

    /// Generate a sample configuration file for reference
    pub fn generate_sample_config() -> Result<String> {
        let sample_config = Self::default();
        toml::to_string_pretty(&sample_config)
            .with_context(|| "Failed to serialize sample configuration")
    }
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            bind_address: default_http_bind_address(),
            port: default_http_port(),
            request_timeout_secs: None,
            max_concurrent_requests: default_http_max_concurrent_requests(),
            cors_allowed_origins: default_cors_allowed_origins(),
            admin_enabled: default_admin_enabled(),
            tls: TlsConfig::default(),
            max_body_size_mb: None,
        }
    }
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            label: default_node_label_opt(),
            zone: default_node_zone(),
            profile: None,
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_paths: vec![PathBuf::from("./data/cameodb")],
            disk_usage_threshold_percent: default_disk_usage_threshold_percent(),
            wal_sync: default_wal_sync(),
            wal_segment_size_mb: default_wal_segment_size_mb(),
            default_batch_size: default_default_batch_size(),
            num_shards_init: default_num_shards_init(),
            max_shards_per_node: default_max_shards_per_node(),
            writer_core_affinity: default_writer_core_affinity(),
            shard_affine_dispatch: default_shard_affine_dispatch(),
            worker_core_affinity: default_worker_core_affinity(),
        }
    }
}

impl Default for SearchConfig {
    fn default() -> Self {
        Self {
            indexer_memory_min_mb: default_indexer_memory_min_mb(),
            indexer_memory_max_mb: default_indexer_memory_max_mb(),
            memory_pressure_threshold_percent: default_memory_pressure_threshold_percent(),
            search_threads: default_search_threads(),
            enable_streaming_search: default_enable_streaming_search(),
            max_concurrent_shard_searches: default_max_concurrent_shard_searches(),
            max_concurrent_remote_searches: default_max_concurrent_remote_searches(),
            enable_early_termination: default_enable_early_termination(),
            default_search_limit: default_search_limit(),
            supervisor_timeout_secs: default_supervisor_timeout_secs(),
            stream_batch_size: default_stream_batch_size(),
            indexer_num_threads: default_indexer_num_threads(),
            merge_num_threads: default_merge_num_threads(),
            total_memory_limit_mb: None,
        }
    }
}

impl Default for MessagingConfig {
    fn default() -> Self {
        Self {
            request_timeout_secs: None,
            max_concurrent_requests: default_messaging_max_concurrent_requests(),
            connection_pool_size: default_connection_pool_size(),
            remote_retry_attempts: default_remote_retry_attempts(),
            broadcast_timeout_secs: default_broadcast_timeout_secs(),
            broadcast_fanout_limit: default_broadcast_fanout_limit(),
        }
    }
}

/// A 32-byte cluster pre-shared key that cannot be printed or serialized by accident.
///
/// The key used to live in a plain `String` inside a `Debug + Serialize` struct, so any
/// future config dump, debug log, or admin endpoint would have leaked it. Wrapping it
/// makes the safe behaviour the default rather than something every call site has to
/// remember, and zeroizes the bytes on drop.
pub struct ClusterPsk([u8; 32]);

impl ClusterPsk {
    pub fn bytes(&self) -> [u8; 32] {
        self.0
    }

    /// Short, non-reversible identifier for logs, so two nodes can be confirmed to hold
    /// the same key without either of them printing it.
    pub fn fingerprint(&self) -> String {
        let digest = xxhash_rust::xxh3::xxh3_128(&self.0);
        format!("{:032x}", digest)[..16].to_string()
    }
}

impl fmt::Debug for ClusterPsk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ClusterPsk(<redacted:{}>)", self.fingerprint())
    }
}

impl Drop for ClusterPsk {
    fn drop(&mut self) {
        // Best-effort scrub. `write_volatile` is not elided by the optimizer the way a
        // plain assignment to a soon-to-be-dead value can be.
        for byte in self.0.iter_mut() {
            unsafe { std::ptr::write_volatile(byte, 0) };
        }
    }
}

impl ClusterConfig {
    /// Load, validate, and decode the cluster pre-shared key.
    ///
    /// The single place PSK format rules live: `validate()` calls this too, so a config
    /// that passes validation is exactly one that the swarm can start with.
    ///
    /// `psk` takes precedence over `psk_file`. Returns `None` when neither is configured.
    pub fn load_psk(&self) -> Result<Option<ClusterPsk>> {
        let hex_str = match (&self.psk, &self.psk_file) {
            (Some(psk), _) => psk.trim().to_string(),
            (None, Some(path)) => {
                if !path.exists() {
                    anyhow::bail!("cluster psk_file not found: {}", path.display());
                }
                Self::warn_if_psk_file_is_readable_by_others(path);
                std::fs::read_to_string(path)
                    .map_err(|e| {
                        anyhow::anyhow!("failed to read cluster psk_file {}: {}", path.display(), e)
                    })?
                    .trim()
                    .to_string()
            }
            (None, None) => return Ok(None),
        };

        if hex_str.len() != 64 || !hex_str.chars().all(|c| c.is_ascii_hexdigit()) {
            // Deliberately says nothing about the value itself — an error message is the
            // one place a malformed secret would otherwise end up in a log.
            anyhow::bail!(
                "cluster PSK must be exactly 64 hex characters (32 bytes); got {} character(s). \
                 Generate one with: openssl rand -hex 32",
                hex_str.len()
            );
        }

        let mut bytes = [0u8; 32];
        for (i, chunk) in hex_str.as_bytes().chunks_exact(2).enumerate() {
            // Both bytes are ASCII hex digits per the check above, so this cannot fail.
            let hex = std::str::from_utf8(chunk).expect("ascii hex");
            bytes[i] = u8::from_str_radix(hex, 16).expect("validated hex digits");
        }

        Ok(Some(ClusterPsk(bytes)))
    }

    /// Warn when the key file is readable beyond its owner.
    ///
    /// A warning rather than an error: refusing to start over a permission bit would
    /// strand nodes on deployments where the file is managed by an orchestrator.
    #[cfg(unix)]
    fn warn_if_psk_file_is_readable_by_others(path: &std::path::Path) {
        use std::os::unix::fs::PermissionsExt;
        if let Ok(meta) = std::fs::metadata(path) {
            let mode = meta.permissions().mode() & 0o077;
            if mode != 0 {
                warn!(
                    path = %path.display(),
                    mode = format!("{:o}", meta.permissions().mode() & 0o777),
                    "cluster psk_file is readable by group or others; chmod 600 it"
                );
            }
        }
    }

    #[cfg(not(unix))]
    fn warn_if_psk_file_is_readable_by_others(_path: &std::path::Path) {}
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            enabled: default_cluster_enabled(),
            bind_address: default_cluster_bind_address(),
            cluster_port: default_cluster_port(),
            seed_nodes: Vec::new(),
            cluster_nodes: Vec::new(),
            cluster_name: default_cluster_name(),
            listen_addrs: Vec::new(),
            bootstrap_peers: Vec::new(),
            psk: None,
            psk_file: None,
            messaging: MessagingConfig::default(),
        }
    }
}

/// Every `#[serde(default = "...")]` name in this module resolves to one of these
/// functions — each exists only so serde can name it, so the macro writes each
/// constant exactly once.
macro_rules! config_defaults {
    ($($(#[$meta:meta])* $name:ident -> $ty:ty = $value:expr;)*) => {
        $(
            $(#[$meta])*
            fn $name() -> $ty {
                $value
            }
        )*
    };
}

config_defaults! {
    /// Loopback by default.
    ///
    /// Binding every interface out of the box put an unauthenticated read/write/delete API on
    /// the network the moment the binary ran. Reaching the node from other hosts is now a
    /// deliberate act — set `bind_address` and declare a `profile` to go with it.
    default_http_bind_address -> String = "127.0.0.1".to_string();
    default_http_port -> u16 = 9480;
    default_mcp_enabled -> bool = true;
    /// Thirty minutes. See [`McpConfig::session_idle_timeout_secs`] for why it is not five.
    default_session_idle_timeout_secs -> u64 = 1800;
    default_max_sessions -> usize = 1024;
    /// Fifteen seconds, which is under every idle-read timeout common in front of a node: 30 s on
    /// nginx by default, 60 s on an AWS ALB.
    default_sse_keepalive_secs -> u64 = 15;
    default_legacy_sse_enabled -> bool = true;
    /// Thirty-two — see [`McpConfig::max_in_flight_per_session`].
    default_max_in_flight_per_session -> usize = 32;
    default_max_record_size_mb -> usize = 64;
    default_http_max_concurrent_requests -> usize = 128;
    default_admin_enabled -> bool = true;
    /// No cross-origin browser access by default.
    ///
    /// CORS governs browsers and nothing else, so an empty list costs API and MCP clients
    /// nothing while removing the drive-by attack surface that `["*"]` handed to any web page
    /// the operator happened to visit — which mattered because no endpoint requires auth.
    default_cors_allowed_origins -> Vec<String> = Vec::new();
    default_node_label_opt -> Option<String> = Some("cameodb".to_string());
    default_node_zone -> String = "default".to_string();
    default_disk_usage_threshold_percent -> u8 = 90;
    default_wal_sync -> bool = true;
    default_wal_segment_size_mb -> usize = 64;
    default_default_batch_size -> usize = 1000;
    default_num_shards_init -> usize = 4;
    default_max_shards_per_node -> usize = 8;
    default_writer_core_affinity -> bool = true;
    default_shard_affine_dispatch -> bool = false;
    default_worker_core_affinity -> bool = false;
    default_indexer_memory_min_mb -> usize = 64;
    default_indexer_memory_max_mb -> usize = 512;
    default_total_memory_limit_mb -> usize = 2048;
    default_memory_pressure_threshold_percent -> u8 = 80;
    default_search_threads -> usize = 8;
    default_search_limit -> usize = 10;
    default_indexer_num_threads -> usize = 1;
    default_merge_num_threads -> usize = 2;
    default_supervisor_timeout_secs -> u64 = 5;
    default_cluster_enabled -> bool = false;
    default_cluster_bind_address -> String = "0.0.0.0".to_string();
    default_cluster_port -> u16 = 9580;
    default_cluster_name -> String = "cameodb-cluster".to_string();
    default_messaging_max_concurrent_requests -> usize = 100;
    default_connection_pool_size -> usize = 10;
    default_remote_retry_attempts -> u8 = 2;
    default_broadcast_timeout_secs -> u64 = 5;
    default_broadcast_fanout_limit -> usize = 16;
    default_enable_streaming_search -> bool = true;
    default_max_concurrent_shard_searches -> usize = 32;
    default_max_concurrent_remote_searches -> usize = 8;
    default_enable_early_termination -> bool = true;
    default_stream_batch_size -> usize = 400;
}
