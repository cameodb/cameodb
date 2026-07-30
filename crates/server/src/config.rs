//! CameoDB Configuration Management
//!
//! This module provides comprehensive configuration management for CameoDB,
//! supporting multiple configuration sources: files, environment variables,
//! and command-line arguments.
//!
//! ## Configuration Structure
//!
//! ```
//! server:
//!   http:
//!     port: 9480
//!     host: "0.0.0.0"
//!   storage:
//!     data_paths:
//!       - "/mnt/disk1/cameodb"
//!       - "/mnt/disk2/cameodb"
//!   search:
//!     indexer_memory_min_mb: 16
//!     indexer_memory_max_mb: 256
//!     total_memory_limit_mb: 2048
//!     pressure_threshold: 0.8
//! ```

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;
use thiserror::Error;
use tracing::{info, warn};

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

    #[error("{message}\n\nRun `cameodb --help` for the list of options.")]
    CommandLine { message: String },
}

/// Whether a flag carries a value or is a bare switch.
#[derive(Clone, Copy, PartialEq, Eq)]
enum FlagKind {
    /// `--http-port 9480` or `--http-port=9480`.
    Value,
    /// `--cluster-enabled` (means `true`), or an explicit `--cluster-enabled=false`.
    Switch,
}

/// One setting that can be overridden after the config file is read.
///
/// Each entry ties a flag and an environment variable to a single setter, which is the point:
/// the two layers are declared together, so neither can be added, renamed or given different
/// parsing rules without the other. [`CameoDbConfig::apply_overrides`] walks this table, and
/// [`cli_help`] renders it into `--help`, so the flag list cannot go stale either.
struct Override {
    /// Long flag, including the leading dashes.
    flag: &'static str,
    /// Environment variable with the same effect.
    env: &'static str,
    kind: FlagKind,
    /// Value placeholder shown in `--help` (empty for switches).
    placeholder: &'static str,
    help: &'static str,
    /// Applies a raw string — from either layer — to the config.
    apply: fn(&mut CameoDbConfig, &str) -> Result<()>,
}

/// Parse the boolean spellings the environment layer has always accepted; anything else is
/// false. Kept lenient, and identical for flags, so `FOO=yes` and `--foo=yes` agree.
fn parse_bool(raw: &str) -> bool {
    matches!(
        raw.trim().to_ascii_lowercase().as_str(),
        "true" | "1" | "yes"
    )
}

/// Split a comma- or semicolon-separated list, dropping empty entries.
fn parse_list(raw: &str) -> Vec<String> {
    raw.split([',', ';'])
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(str::to_string)
        .collect()
}

/// Every setting overridable from the command line or the environment.
#[rustfmt::skip]
const OVERRIDES: &[Override] = &[
    Override {
        flag: "--http-port", env: "CAMEODB_HTTP_PORT", kind: FlagKind::Value,
        placeholder: "<PORT>", help: "HTTP API listen port",
        apply: |c, v| { c.network.http.port = v.parse()?; Ok(()) },
    },
    Override {
        flag: "--http-bind-address", env: "CAMEODB_HTTP_BIND_ADDRESS", kind: FlagKind::Value,
        placeholder: "<ADDR>", help: "HTTP API bind address",
        apply: |c, v| { c.network.http.bind_address = v.to_string(); Ok(()) },
    },
    Override {
        flag: "--max-record-size-mb", env: "CAMEODB_MAX_RECORD_SIZE_MB", kind: FlagKind::Value,
        placeholder: "<MB>", help: "Largest accepted record; derives body and message limits",
        apply: |c, v| { c.max_record_size_mb = v.parse()?; Ok(()) },
    },
    Override {
        flag: "--max-body-size-mb", env: "CAMEODB_MAX_BODY_SIZE_MB", kind: FlagKind::Value,
        placeholder: "<MB>", help: "HTTP body limit (defaults to derived from record size)",
        apply: |c, v| { c.network.http.max_body_size_mb = v.parse()?; Ok(()) },
    },
    Override {
        flag: "--data-paths", env: "CAMEODB_DATA_PATHS", kind: FlagKind::Value,
        placeholder: "<PATHS>", help: "Colon-separated storage directories",
        apply: |c, v| { c.storage.data_paths = v.split(':').map(PathBuf::from).collect(); Ok(()) },
    },
    Override {
        flag: "--storage-wal-sync", env: "CAMEODB_STORAGE_WAL_SYNC", kind: FlagKind::Switch,
        placeholder: "", help: "fsync the WAL on every write",
        apply: |c, v| { c.storage.wal_sync = parse_bool(v); Ok(()) },
    },
    Override {
        flag: "--storage-default-batch-size", env: "CAMEODB_STORAGE_DEFAULT_BATCH_SIZE", kind: FlagKind::Value,
        placeholder: "<N>", help: "Documents per write batch before an automatic commit",
        apply: |c, v| { c.storage.default_batch_size = v.parse()?; Ok(()) },
    },
    Override {
        flag: "--indexer-memory-min-mb", env: "CAMEODB_INDEXER_MEMORY_MIN_MB", kind: FlagKind::Value,
        placeholder: "<MB>", help: "Lower bound on per-index writer memory",
        apply: |c, v| { c.search.indexer_memory_min_mb = v.parse()?; Ok(()) },
    },
    Override {
        flag: "--indexer-memory-max-mb", env: "CAMEODB_INDEXER_MEMORY_MAX_MB", kind: FlagKind::Value,
        placeholder: "<MB>", help: "Upper bound on per-index writer memory",
        apply: |c, v| { c.search.indexer_memory_max_mb = v.parse()?; Ok(()) },
    },
    Override {
        flag: "--total-memory-limit-mb", env: "CAMEODB_TOTAL_MEMORY_LIMIT_MB", kind: FlagKind::Value,
        placeholder: "<MB>", help: "Memory budget shared by all indices on this node",
        apply: |c, v| { c.search.total_memory_limit_mb = v.parse()?; Ok(()) },
    },
    Override {
        flag: "--memory-pressure-threshold-percent", env: "CAMEODB_MEMORY_PRESSURE_THRESHOLD_PERCENT", kind: FlagKind::Value,
        placeholder: "<PCT>", help: "Percent of the budget that counts as memory pressure",
        apply: |c, v| { c.search.memory_pressure_threshold_percent = v.parse()?; Ok(()) },
    },
    Override {
        flag: "--default-search-limit", env: "CAMEODB_DEFAULT_SEARCH_LIMIT", kind: FlagKind::Value,
        placeholder: "<N>", help: "Hits returned when a query names no limit",
        apply: |c, v| { c.search.default_search_limit = v.parse()?; Ok(()) },
    },
    Override {
        flag: "--supervisor-timeout-secs", env: "CAMEODB_SUPERVISOR_TIMEOUT_SECS", kind: FlagKind::Value,
        placeholder: "<SECS>", help: "Shard supervisor timeout",
        apply: |c, v| { c.search.supervisor_timeout_secs = v.parse()?; Ok(()) },
    },
    Override {
        flag: "--node-label", env: "CAMEODB_NODE_LABEL", kind: FlagKind::Value,
        placeholder: "<NAME>", help: "Human-readable name for this node",
        apply: |c, v| { c.node.label = Some(v.to_string()); Ok(()) },
    },
    Override {
        flag: "--node-zone", env: "CAMEODB_NODE_ZONE", kind: FlagKind::Value,
        placeholder: "<ZONE>", help: "Availability zone this node reports",
        apply: |c, v| { c.node.zone = v.to_string(); Ok(()) },
    },
    Override {
        flag: "--cluster-enabled", env: "CAMEODB_CLUSTER_ENABLED", kind: FlagKind::Switch,
        placeholder: "", help: "Join a cluster instead of running single-node",
        apply: |c, v| { c.network.cluster.enabled = parse_bool(v); Ok(()) },
    },
    Override {
        flag: "--cluster-bind-address", env: "CAMEODB_CLUSTER_BIND_ADDRESS", kind: FlagKind::Value,
        placeholder: "<ADDR>", help: "Cluster transport bind address",
        apply: |c, v| { c.network.cluster.bind_address = v.to_string(); Ok(()) },
    },
    Override {
        flag: "--cluster-port", env: "CAMEODB_CLUSTER_PORT", kind: FlagKind::Value,
        placeholder: "<PORT>", help: "Cluster transport port",
        apply: |c, v| { c.network.cluster.cluster_port = v.parse()?; Ok(()) },
    },
    Override {
        flag: "--cluster-name", env: "CAMEODB_CLUSTER_NAME", kind: FlagKind::Value,
        placeholder: "<NAME>", help: "Cluster this node belongs to (ignored if empty)",
        apply: |c, v| {
            if !v.trim().is_empty() { c.network.cluster.cluster_name = v.to_string(); }
            Ok(())
        },
    },
    Override {
        flag: "--seed-nodes", env: "CAMEODB_SEED_NODES", kind: FlagKind::Value,
        placeholder: "<ADDRS>", help: "Comma-separated seed node addresses (ignored if empty)",
        apply: |c, v| {
            let parsed = parse_list(v);
            if !parsed.is_empty() { c.network.cluster.seed_nodes = parsed; }
            Ok(())
        },
    },
    Override {
        flag: "--cluster-nodes", env: "CAMEODB_CLUSTER_NODES", kind: FlagKind::Value,
        placeholder: "<ADDRS>", help: "Comma-separated static cluster members (ignored if empty)",
        apply: |c, v| {
            let parsed = parse_list(v);
            if !parsed.is_empty() { c.network.cluster.cluster_nodes = parsed; }
            Ok(())
        },
    },
];

/// Configuration overrides collected from the command line.
///
/// Values are kept as raw strings and interpreted by [`OVERRIDES`] during
/// [`CameoDbConfig::load_with_cli`], so parsing a command line never depends on config state
/// and can be unit-tested on its own.
#[derive(Debug, Default, Clone)]
pub struct CliOverrides {
    /// `--config <path>`, if given.
    pub config_path: Option<String>,
    /// `(flag, raw value)` in the order the flags appeared; a repeated flag keeps the last.
    values: Vec<(&'static str, String)>,
}

impl CliOverrides {
    /// Parse server flags from `args`, which must not include the program name.
    ///
    /// Unknown flags and missing values are hard errors. Silently ignoring them is what let
    /// `cameodb --config foo.toml` start on a completely different configuration than asked.
    pub fn parse<I>(args: I) -> Result<Self>
    where
        I: IntoIterator<Item = String>,
    {
        let mut parsed = Self::default();
        let mut args = args.into_iter().peekable();

        while let Some(arg) = args.next() {
            let (name, inline_value) = match arg.split_once('=') {
                Some((name, value)) => (name.to_string(), Some(value.to_string())),
                None => (arg.clone(), None),
            };

            if name == "--config" || name == "-c" {
                let path = match inline_value {
                    Some(value) => value,
                    None => args.next().ok_or_else(|| ConfigError::CommandLine {
                        message: format!("{name} requires a path"),
                    })?,
                };
                parsed.config_path = Some(path);
                continue;
            }

            let Some(entry) = OVERRIDES.iter().find(|entry| entry.flag == name) else {
                return Err(ConfigError::CommandLine {
                    message: format!("Unknown option: {arg}"),
                }
                .into());
            };

            let value = match (inline_value, entry.kind) {
                (Some(value), _) => value,
                // A bare switch means "true"; it must not swallow the next argument.
                (None, FlagKind::Switch) => "true".to_string(),
                (None, FlagKind::Value) => args.next().ok_or_else(|| ConfigError::CommandLine {
                    message: format!("{} requires a value {}", entry.flag, entry.placeholder),
                })?,
            };

            parsed.values.retain(|(flag, _)| *flag != entry.flag);
            parsed.values.push((entry.flag, value));
        }

        Ok(parsed)
    }

    /// Whether the operator named a config file on the command line.
    fn has_explicit_config_path(&self) -> bool {
        self.config_path.is_some() || std::env::var_os("CAMEODB_CONFIG").is_some()
    }

    /// The raw value given for `flag`, if any.
    fn value_for(&self, flag: &str) -> Option<&str> {
        self.values
            .iter()
            .find(|(name, _)| *name == flag)
            .map(|(_, value)| value.as_str())
    }
}

/// Dotted paths in `content` that no configuration field claims, deepest name last
/// (`storrage`, `network.http.prot`).
///
/// TOML only: it is the documented format, the one `generate-config` emits, and the one a
/// generic value tree is cheap to build for. A YAML file simply gets no report.
fn unrecognized_keys(content: &str) -> Vec<String> {
    let Ok(parsed) = toml::from_str::<toml::Value>(content) else {
        return Vec::new();
    };
    // The schema is the serialized default config: exactly the set of keys that mean
    // something, derived from the structs themselves rather than a hand-maintained list.
    let Ok(schema) = toml::Value::try_from(CameoDbConfig::default()) else {
        return Vec::new();
    };

    let mut unknown = Vec::new();
    collect_unrecognized(&parsed, &schema, "", &mut unknown);
    unknown
}

/// Walk `parsed` against `schema`, recording paths absent from the schema.
fn collect_unrecognized(
    parsed: &toml::Value,
    schema: &toml::Value,
    prefix: &str,
    unknown: &mut Vec<String>,
) {
    let (Some(parsed), Some(schema)) = (parsed.as_table(), schema.as_table()) else {
        return;
    };

    for (key, value) in parsed {
        let path = if prefix.is_empty() {
            key.clone()
        } else {
            format!("{prefix}.{key}")
        };

        match schema.get(key) {
            // Recurse only into nested tables; a table where the schema wants a value (or the
            // reverse) is a type error the parse above would already have rejected.
            Some(known) => collect_unrecognized(value, known, &path, unknown),
            None => unknown.push(path),
        }
    }
}

/// Render the server options for `--help`, straight from [`OVERRIDES`].
pub fn cli_help() -> String {
    let mut lines = vec![format!(
        "  {:<44}{}",
        "-c, --config <PATH>", "Configuration file to load (TOML or YAML)"
    )];

    for entry in OVERRIDES {
        let flag = if entry.placeholder.is_empty() {
            entry.flag.to_string()
        } else {
            format!("{} {}", entry.flag, entry.placeholder)
        };
        lines.push(format!("  {:<44}{} [{}]", flag, entry.help, entry.env));
    }

    lines.join("\n")
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

/// Complete CameoDB configuration structure.
///
/// Every struct in this module carries a container-level `#[serde(default)]`, so a config
/// file may contain as much or as little as it wants: name only the settings you are
/// changing, and everything else — whole sections included — comes from [`Default`]. Each
/// `Default` impl is built from the same `default_*()` functions the per-field
/// `#[serde(default = "...")]` attributes use, so the two can never disagree about a value.
#[derive(Debug, Clone, Serialize, Deserialize)]
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

    /// Maximum single-record size in MB (default: 512).
    ///
    /// This is the **single source of truth** for record size limits across
    /// the entire system. On startup the following dependent limits are
    /// derived automatically:
    ///
    /// | Derived limit                       | Formula                          |
    /// |-------------------------------------|----------------------------------|
    /// | HTTP max body size                   | `max_record_size_mb + 64` MB (overhead) |
    /// | Kameo remote request/response max    | `max_record_size_mb * 1.25` (25 % headroom) |
    /// | HTTP request timeout                 | `max(60, max_record_size_mb / 10)` seconds |
    #[serde(default = "default_max_record_size_mb")]
    pub max_record_size_mb: usize,
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
    /// Bind address for HTTP server (default: "0.0.0.0")
    #[serde(default = "default_http_bind_address")]
    pub bind_address: String,

    /// Port for HTTP server (default: 9480)
    #[serde(default = "default_http_port")]
    pub port: u16,

    /// Request timeout in seconds (default: 30)
    #[serde(default = "default_request_timeout")]
    pub request_timeout_secs: u64,

    /// Maximum request body size in MB.
    ///
    /// When omitted (or 0) the effective value is derived from the top-level
    /// `max_record_size_mb` setting.  Set explicitly only to override.
    #[serde(default)]
    pub max_body_size_mb: usize,

    /// CORS allowed origins (default: ["*"])
    #[serde(default = "default_cors_allowed_origins")]
    pub cors_allowed_origins: Vec<String>,
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

    /// Pin per-shard writer threads to a CPU core (default: false).
    /// When enabled, each shard's writer thread is pinned to a deterministic
    /// CPU core derived from `xxh3(shard_id) % num_cores`, improving cache
    /// locality and reducing cross-core wakeups under heavy write load.
    #[serde(default = "default_writer_core_affinity")]
    pub writer_core_affinity: bool,

    /// Enable shard-affine worker dispatch (default: false).
    /// When enabled, operations targeting the same shard are routed to the same
    /// orchestrator worker, reducing cross-core wakeups when writer pinning is
    /// also enabled. Uses `xxh3(shard_id) % worker_count` for deterministic
    /// worker selection. Falls back to round-robin for scatter-gather ops.
    #[serde(default = "default_shard_affine_dispatch")]
    pub shard_affine_dispatch: bool,

    /// Pin orchestrator worker tasks to CPU cores as dedicated OS threads
    /// (default: false). Requires `shard_affine_dispatch = true` AND
    /// `writer_core_affinity = true` to take effect; otherwise silently no-op.
    ///
    /// When enabled, each worker runs on its own `tokio::current_thread` runtime
    /// pinned to `core_ids[worker_id]`. Combined with hash-aligned dispatch
    /// (Stage 2d) this guarantees worker and writer for the same shard land on
    /// the same CPU core, maximizing cache locality.
    #[serde(default = "default_worker_core_affinity")]
    pub worker_core_affinity: bool,
}

/// Cluster configuration for distributed actor system
#[derive(Debug, Clone, Serialize, Deserialize)]
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

    /// Messaging configuration
    #[serde(default)]
    pub messaging: MessagingConfig,
}

/// Messaging configuration for Kameo remote actors
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct MessagingConfig {
    /// Request timeout in seconds (default: 30)
    #[serde(default = "default_request_timeout_secs")]
    pub request_timeout_secs: u64,

    /// Maximum concurrent requests per peer (default: 100)
    #[serde(default = "default_max_concurrent_requests")]
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

    /// Total memory limit in MB (default: 2048)
    #[serde(default = "default_total_memory_limit_mb")]
    pub total_memory_limit_mb: usize,

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

    /// Enable early termination when result limit is reached
    #[serde(default = "default_enable_early_termination")]
    pub enable_early_termination: bool,

    /// Default search result limit when not specified in request (default: 10)
    #[serde(default = "default_search_limit")]
    pub default_search_limit: usize,

    /// Supervisor idle timeout in seconds before auto-commit (default: 10)
    #[serde(default = "default_supervisor_timeout_secs")]
    pub supervisor_timeout_secs: u64,

    /// Number of documents per micro-batch when ingesting NDJSON write streams (default: 500)
    #[serde(default = "default_stream_batch_size")]
    pub stream_batch_size: usize,

    /// Number of indexing worker threads per tantivy IndexWriter (default: 1).
    /// Each worker creates one segment per commit.
    #[serde(default = "default_indexer_num_threads")]
    pub indexer_num_threads: usize,

    /// Number of background merge (compaction) threads per IndexWriter (default: 1).
    /// Tantivy default is 4, but on memory-constrained nodes with many indices
    /// this causes mmap storms. Scale up on nodes with ample RAM.
    #[serde(default = "default_merge_num_threads")]
    pub merge_num_threads: usize,
}

impl CameoDbConfig {
    /// Effective HTTP max body size in MB.
    ///
    /// If the user set `network.http.max_body_size_mb` explicitly (non-zero),
    /// that value wins.  Otherwise it is derived as `max_record_size_mb + 64`
    /// to leave headroom for JSON framing, bulk-write arrays, etc.
    pub fn effective_max_body_size_mb(&self) -> usize {
        if self.network.http.max_body_size_mb > 0 {
            self.network.http.max_body_size_mb
        } else {
            self.max_record_size_mb + 64
        }
    }

    /// Effective Kameo remote messaging size limit in **bytes**.
    ///
    /// The envelope must accommodate a single large record plus serialization
    /// overhead (JSON framing, field names, routing metadata).  We add 25 %
    /// headroom on top of the configured record size.
    pub fn effective_remote_message_size_bytes(&self) -> usize {
        // max_record_size_mb converted to bytes + 25 % overhead
        let base = self.max_record_size_mb * 1024 * 1024;
        base + base / 4
    }

    /// Effective HTTP request timeout in seconds.
    ///
    /// For large records the default 30 s is insufficient.  Scale linearly
    /// with record size: `max(60, max_record_size_mb / 10)`.
    /// An explicit non-default `request_timeout_secs` takes precedence.
    pub fn effective_request_timeout_secs(&self) -> u64 {
        if self.network.http.request_timeout_secs != default_request_timeout() {
            // User provided an explicit override – honour it.
            self.network.http.request_timeout_secs
        } else {
            let scaled = (self.max_record_size_mb as u64) / 10;
            scaled.max(60)
        }
    }

    /// Effective Kameo remote messaging timeout in seconds.
    ///
    /// Uses the same scaled timeout as HTTP so that inter-node forwarding
    /// does not time out before the origin request.
    pub fn effective_remote_timeout_secs(&self) -> u64 {
        let messaging = &self.network.cluster.messaging;
        if messaging.request_timeout_secs != default_request_timeout_secs() {
            messaging.request_timeout_secs
        } else {
            self.effective_request_timeout_secs()
        }
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
        // Start with defaults
        let mut config = Self::default();

        // Layer the config file on top, if there is one
        match Self::load_from_file(cli.config_path.as_deref()) {
            Ok(file_config) => config = Self::merge_configs(config, file_config),
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

        // Validate the final configuration
        config.validate()?;

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
        let config: Self = if path.ends_with(".toml") {
            toml::from_str(content).with_context(|| "Failed to parse TOML configuration")?
        } else if path.ends_with(".yaml") || path.ends_with(".yml") {
            serde_saphyr::from_str(content).with_context(|| "Failed to parse YAML configuration")?
        } else {
            // Try TOML first, then YAML
            toml::from_str(content)
                .or_else(|_| serde_saphyr::from_str(content))
                .with_context(|| "Failed to parse configuration (tried TOML and YAML)")?
        };

        for key in unrecognized_keys(content) {
            warn!("Ignoring unknown setting in {}: {}", path, key);
        }

        Ok(config)
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

    /// Merge two configurations, with `override_config` taking precedence
    fn merge_configs(_base: Self, override_config: Self) -> Self {
        // For now, override completely replaces base
        // In the future, we could implement more sophisticated merging
        override_config
    }

    /// Validate the configuration for consistency and constraints
    pub fn validate(&self) -> Result<()> {
        // Validate HTTP configuration
        if self.network.http.port == 0 {
            return Err(ConfigError::NetworkConfig {
                message: "HTTP port cannot be 0".to_string(),
            }
            .into());
        }

        if self.network.http.request_timeout_secs == 0 {
            return Err(ConfigError::NetworkConfig {
                message: "Request timeout must be positive".to_string(),
            }
            .into());
        }

        // Validate record size limit
        if self.max_record_size_mb == 0 {
            return Err(ConfigError::NetworkConfig {
                message: "max_record_size_mb must be positive".to_string(),
            }
            .into());
        }

        // Validate storage configuration
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

        // Validate memory configuration
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

        if self.search.total_memory_limit_mb < self.search.indexer_memory_max_mb {
            return Err(ConfigError::MemoryConfig {
                message: "Total memory limit must be at least as large as max indexer memory"
                    .to_string(),
            }
            .into());
        }

        Ok(())
    }

    /// Generate a sample configuration file for reference
    pub fn generate_sample_config() -> Result<String> {
        let sample_config = Self::default();
        toml::to_string_pretty(&sample_config)
            .with_context(|| "Failed to serialize sample configuration")
    }
}

impl Default for CameoDbConfig {
    fn default() -> Self {
        Self {
            node: NodeConfig::default(),
            network: NetworkConfig::default(),
            storage: StorageConfig::default(),
            search: SearchConfig::default(),
            max_record_size_mb: default_max_record_size_mb(),
        }
    }
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            bind_address: default_http_bind_address(),
            port: default_http_port(),
            request_timeout_secs: default_request_timeout(),
            max_body_size_mb: 0, // derived from max_record_size_mb
            cors_allowed_origins: default_cors_allowed_origins(),
        }
    }
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            label: default_node_label_opt(),
            zone: default_node_zone(),
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
            total_memory_limit_mb: default_total_memory_limit_mb(),
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
        }
    }
}

impl Default for MessagingConfig {
    fn default() -> Self {
        Self {
            request_timeout_secs: default_request_timeout_secs(),
            max_concurrent_requests: default_max_concurrent_requests(),
            connection_pool_size: default_connection_pool_size(),
            remote_retry_attempts: default_remote_retry_attempts(),
            broadcast_timeout_secs: default_broadcast_timeout_secs(),
            broadcast_fanout_limit: default_broadcast_fanout_limit(),
        }
    }
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
            messaging: MessagingConfig::default(),
        }
    }
}

// Default value functions for serde
fn default_http_bind_address() -> String {
    "0.0.0.0".to_string()
}

fn default_http_port() -> u16 {
    9480
}

fn default_request_timeout() -> u64 {
    30
}

fn default_max_record_size_mb() -> usize {
    512
}

fn default_cors_allowed_origins() -> Vec<String> {
    vec!["*".to_string()]
}

fn default_node_label_opt() -> Option<String> {
    Some("cameodb".to_string())
}

fn default_node_zone() -> String {
    "default".to_string()
}

fn default_disk_usage_threshold_percent() -> u8 {
    90
}
fn default_wal_sync() -> bool {
    true
}
fn default_wal_segment_size_mb() -> usize {
    64
}
fn default_default_batch_size() -> usize {
    1000
}

fn default_num_shards_init() -> usize {
    4
}
fn default_max_shards_per_node() -> usize {
    8
}
fn default_writer_core_affinity() -> bool {
    true
}

fn default_shard_affine_dispatch() -> bool {
    false
}

fn default_worker_core_affinity() -> bool {
    false
}

fn default_indexer_memory_min_mb() -> usize {
    64
}
fn default_indexer_memory_max_mb() -> usize {
    512
}
fn default_total_memory_limit_mb() -> usize {
    2048
}
fn default_memory_pressure_threshold_percent() -> u8 {
    80
}
fn default_search_threads() -> usize {
    8
}
fn default_search_limit() -> usize {
    10
}

fn default_indexer_num_threads() -> usize {
    1
}
fn default_merge_num_threads() -> usize {
    2
}
fn default_supervisor_timeout_secs() -> u64 {
    5
}

fn default_cluster_enabled() -> bool {
    false
}

fn default_cluster_bind_address() -> String {
    "0.0.0.0".to_string()
}

fn default_cluster_port() -> u16 {
    9580
}

fn default_cluster_name() -> String {
    "cameodb-cluster".to_string()
}

fn default_request_timeout_secs() -> u64 {
    30
}

fn default_max_concurrent_requests() -> usize {
    100
}

fn default_connection_pool_size() -> usize {
    10
}

fn default_remote_retry_attempts() -> u8 {
    2
}

fn default_broadcast_timeout_secs() -> u64 {
    5
}

fn default_broadcast_fanout_limit() -> usize {
    16
}

fn default_enable_streaming_search() -> bool {
    true
}

fn default_max_concurrent_shard_searches() -> usize {
    32
}

fn default_max_concurrent_remote_searches() -> usize {
    8
}

fn default_enable_early_termination() -> bool {
    true
}

fn default_stream_batch_size() -> usize {
    400
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cli(args: &[&str]) -> CliOverrides {
        CliOverrides::parse(args.iter().map(|arg| arg.to_string())).expect("parse")
    }

    #[test]
    fn cli_parses_both_value_syntaxes() {
        let parsed = cli(&["--http-port", "1234", "--node-label=alpha"]);
        assert_eq!(parsed.value_for("--http-port"), Some("1234"));
        assert_eq!(parsed.value_for("--node-label"), Some("alpha"));
    }

    #[test]
    fn cli_parses_config_path_including_short_form() {
        assert_eq!(
            cli(&["--config", "a.toml"]).config_path.as_deref(),
            Some("a.toml")
        );
        assert_eq!(
            cli(&["-c", "b.toml"]).config_path.as_deref(),
            Some("b.toml")
        );
        assert_eq!(
            cli(&["--config=c.yaml"]).config_path.as_deref(),
            Some("c.yaml")
        );
    }

    /// A bare switch must not eat the argument after it — `--cluster-enabled --http-port 1`
    /// once would have consumed `--http-port` as the switch's value.
    #[test]
    fn cli_switch_defaults_to_true_without_consuming_the_next_argument() {
        let parsed = cli(&["--cluster-enabled", "--http-port", "1234"]);
        assert_eq!(parsed.value_for("--cluster-enabled"), Some("true"));
        assert_eq!(parsed.value_for("--http-port"), Some("1234"));

        let explicit = cli(&["--cluster-enabled=false"]);
        assert_eq!(explicit.value_for("--cluster-enabled"), Some("false"));
    }

    #[test]
    fn cli_last_occurrence_of_a_flag_wins() {
        let parsed = cli(&["--http-port", "1", "--http-port", "2"]);
        assert_eq!(parsed.value_for("--http-port"), Some("2"));
    }

    /// The bug this whole path exists to prevent: an unrecognized option used to be ignored,
    /// so the server booted on a configuration nobody asked for.
    #[test]
    fn cli_rejects_unknown_options_and_missing_values() {
        let unknown = CliOverrides::parse(["--nope".to_string()]).unwrap_err();
        assert!(unknown.to_string().contains("Unknown option: --nope"));

        let missing = CliOverrides::parse(["--http-port".to_string()]).unwrap_err();
        assert!(missing.to_string().contains("--http-port requires a value"));

        let no_path = CliOverrides::parse(["--config".to_string()]).unwrap_err();
        assert!(no_path.to_string().contains("--config requires a path"));
    }

    #[test]
    fn every_override_is_reachable_from_both_layers() {
        for entry in OVERRIDES {
            assert!(
                entry.flag.starts_with("--"),
                "{} must be a long flag",
                entry.flag
            );
            assert!(
                entry.env.starts_with("CAMEODB_"),
                "{} must be a CAMEODB_ variable",
                entry.env
            );
            assert_eq!(
                entry.placeholder.is_empty(),
                entry.kind == FlagKind::Switch,
                "{} must have a placeholder iff it takes a value",
                entry.flag
            );
            assert!(
                cli_help().contains(entry.flag),
                "{} is missing from --help",
                entry.flag
            );
        }
    }

    /// Precedence, on one setting, through all four layers at once.
    #[test]
    fn command_line_beats_environment_beats_file_beats_default() {
        let dir = std::env::temp_dir().join(format!("cameodb-cfg-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        let file = dir.join("cameodb.toml");
        // Config files are deserialized strictly, so write a complete one.
        let mut on_disk = CameoDbConfig::default();
        on_disk.network.http.port = 7001;
        std::fs::write(&file, toml::to_string_pretty(&on_disk).expect("serialize"))
            .expect("write config");
        let path = file.to_str().expect("utf-8 path").to_string();

        // Defaults only.
        let config = CameoDbConfig::load_with_cli(&CliOverrides::default()).expect("defaults");
        assert_eq!(config.network.http.port, 9480);

        // File over defaults.
        let from_file =
            CameoDbConfig::load_with_cli(&cli(&["--config", &path])).expect("file config");
        assert_eq!(from_file.network.http.port, 7001);

        unsafe { std::env::set_var("CAMEODB_HTTP_PORT", "7002") };

        // Environment over file.
        let from_env =
            CameoDbConfig::load_with_cli(&cli(&["--config", &path])).expect("env override");
        assert_eq!(from_env.network.http.port, 7002);

        // Command line over environment.
        let from_cli =
            CameoDbConfig::load_with_cli(&cli(&["--config", &path, "--http-port", "7003"]))
                .expect("cli override");
        assert_eq!(from_cli.network.http.port, 7003);

        unsafe { std::env::remove_var("CAMEODB_HTTP_PORT") };
        std::fs::remove_dir_all(&dir).ok();
    }

    /// The long-standing contract: name only what you are changing.
    #[test]
    fn partial_config_files_keep_defaults_for_everything_omitted() {
        let config: CameoDbConfig = toml::from_str(
            "[network.http]\n\
             port = 7001\n",
        )
        .expect("a partial config must parse");

        assert_eq!(config.network.http.port, 7001, "the named setting applies");
        // Neighbours in the same table, sibling sections, and whole missing sections.
        assert_eq!(config.network.http.bind_address, "0.0.0.0");
        assert_eq!(config.network.cluster.cluster_port, 9580);
        assert_eq!(
            config.storage.data_paths,
            vec![PathBuf::from("./data/cameodb")]
        );
        assert_eq!(config.search.indexer_memory_min_mb, 64);
        assert_eq!(config.max_record_size_mb, 512);
        assert!(config.validate().is_ok());
    }

    /// An empty file is the degenerate partial config and must behave like no file at all.
    #[test]
    fn empty_config_file_is_all_defaults() {
        let config: CameoDbConfig = toml::from_str("").expect("empty config must parse");
        let defaults = CameoDbConfig::default();
        assert_eq!(config.network.http.port, defaults.network.http.port);
        assert_eq!(config.storage.data_paths, defaults.storage.data_paths);
    }

    /// Partial files make a typo indistinguishable from an omission, so typos get reported.
    #[test]
    fn unknown_keys_are_reported_and_known_ones_are_not() {
        let unknown = unrecognized_keys(
            "[network.http]\n\
             port = 7001\n\
             prot = 7002\n\n\
             [storrage]\n\
             data_paths = [\"/tmp/x\"]\n",
        );
        assert_eq!(unknown, vec!["network.http.prot", "storrage"]);

        let sample = CameoDbConfig::generate_sample_config().expect("sample");
        assert!(
            unrecognized_keys(&sample).is_empty(),
            "the generated sample must not report against its own schema"
        );
    }

    /// A named config file that cannot be read must fail the boot, not fall back to defaults.
    #[test]
    fn explicitly_named_config_file_must_exist() {
        let err = CameoDbConfig::load_with_cli(&cli(&["--config", "/nonexistent/cameodb.toml"]))
            .unwrap_err();
        assert!(err.to_string().contains("Failed to read config file"));
    }

    #[test]
    fn test_default_configuration() {
        let config = CameoDbConfig::default();
        assert_eq!(config.network.http.port, 9480);
        assert_eq!(config.network.http.bind_address, "0.0.0.0");
        assert_eq!(config.search.indexer_memory_min_mb, 64);
        assert_eq!(config.search.indexer_memory_max_mb, 512);
        assert_eq!(config.storage.default_batch_size, 1000);
        assert_eq!(config.max_record_size_mb, 512);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_memory_validation() {
        let mut config = CameoDbConfig::default();

        // Test invalid memory range
        config.search.indexer_memory_min_mb = 600;
        config.search.indexer_memory_max_mb = 512;
        assert!(config.validate().is_err());

        // Test memory too small (below new floor of 16)
        config.search.indexer_memory_min_mb = 8;
        config.search.indexer_memory_max_mb = 512;
        assert!(config.validate().is_err());

        // Test memory at new floor (should be valid)
        config.search.indexer_memory_min_mb = 16;
        config.search.indexer_memory_max_mb = 512;
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_storage_validation() {
        let mut config = CameoDbConfig::default();

        // Test empty data paths
        config.storage.data_paths.clear();
        assert!(config.validate().is_err());

        // Test invalid disk threshold
        config.storage.data_paths = vec![PathBuf::from("./data")];
        config.storage.disk_usage_threshold_percent = 150;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_sample_config_generation() {
        let sample = CameoDbConfig::generate_sample_config().unwrap();
        assert!(sample.contains("port = 9480"));
        assert!(sample.contains("indexer_memory_min_mb = 64"));
        assert!(sample.contains("default_batch_size = 1000"));
        assert!(sample.contains("max_record_size_mb = 512"));
        assert!(sample.contains("data_paths"));
    }

    #[test]
    fn test_default_batch_size_setting() {
        // Set a specific batch size via environment variable
        unsafe {
            std::env::set_var("CAMEODB_STORAGE_DEFAULT_BATCH_SIZE", "750");
        }

        // Load config and verify the value
        let config = CameoDbConfig::load_with_cli(&CliOverrides::default()).unwrap();
        assert_eq!(config.storage.default_batch_size, 750);

        // Clean up
        unsafe {
            std::env::remove_var("CAMEODB_STORAGE_DEFAULT_BATCH_SIZE");
        }

        println!("✅ Batch size configuration works!");
    }

    #[test]
    fn test_derived_limits_defaults() {
        let config = CameoDbConfig::default();
        // HTTP body: max_record_size_mb + 64 = 512 + 64 = 576
        assert_eq!(config.effective_max_body_size_mb(), 576);
        // Remote message: 512 MB + 25% overhead = 640 MB in bytes
        assert_eq!(
            config.effective_remote_message_size_bytes(),
            512 * 1024 * 1024 + 512 * 1024 * 1024 / 4
        );
        // Timeout: max(60, 512/10) = max(60, 51) = 60
        assert_eq!(config.effective_request_timeout_secs(), 60);
    }

    #[test]
    fn test_derived_limits_large_record() {
        let config = CameoDbConfig {
            max_record_size_mb: 2048,
            ..Default::default()
        };
        // HTTP body: 2048 + 64 = 2112
        assert_eq!(config.effective_max_body_size_mb(), 2112);
        // Remote message: 2048 MB + 25% overhead = 2560 MB in bytes
        assert_eq!(
            config.effective_remote_message_size_bytes(),
            2048 * 1024 * 1024 + 2048 * 1024 * 1024 / 4
        );
        // Timeout: max(60, 2048/10) = max(60, 204) = 204
        assert_eq!(config.effective_request_timeout_secs(), 204);
    }

    #[test]
    fn test_explicit_body_size_override() {
        let mut config = CameoDbConfig::default();
        config.network.http.max_body_size_mb = 100;
        // Explicit override wins
        assert_eq!(config.effective_max_body_size_mb(), 100);
    }

    #[test]
    fn test_explicit_timeout_override() {
        let mut config = CameoDbConfig::default();
        config.network.http.request_timeout_secs = 120;
        // Explicit override wins (120 != default 30)
        assert_eq!(config.effective_request_timeout_secs(), 120);
    }

    #[test]
    fn test_zero_record_size_fails_validation() {
        let config = CameoDbConfig {
            max_record_size_mb: 0,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }
}
