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
use std::fmt;
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

    #[error("Security configuration error: {message}")]
    SecurityConfig { message: String },

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
        flag: "--max-concurrent-requests", env: "CAMEODB_MAX_CONCURRENT_REQUESTS", kind: FlagKind::Value,
        placeholder: "<N>", help: "Max concurrent in-flight HTTP requests (default: 128)",
        apply: |c, v| { c.network.http.max_concurrent_requests = v.parse()?; Ok(()) },
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
        flag: "--profile", env: "CAMEODB_PROFILE", kind: FlagKind::Value,
        placeholder: "<NAME>", help: "Security posture: local, internal, or external",
        apply: |c, v| {
            c.node.profile = Some(crate::posture::Profile::parse(v).map_err(|e| ConfigError::NetworkConfig { message: e })?);
            Ok(())
        },
    },
    Override {
        flag: "--security-enabled", env: "CAMEODB_SECURITY_ENABLED", kind: FlagKind::Switch,
        placeholder: "", help: "Require an API key on HTTP and MCP requests",
        apply: |c, v| { c.security.enabled = parse_bool(v); Ok(()) },
    },
    // A *hash*, never a key: the server has no use for a key and nothing that holds one can
    // leak it. This is also why there is no `CAMEODB_API_KEY` here — that name belongs to
    // the client, and the two would collide the first time both ran in one compose file.
    Override {
        flag: "--api-key-hash", env: "CAMEODB_API_KEY_HASH", kind: FlagKind::Value,
        placeholder: "<SHA256>", help: "Single API key digest, 'sha256:<hex>' from `cameodb keygen`",
        apply: |c, v| { c.security.override_key_mut().key_hash = Some(v.to_string()); Ok(()) },
    },
    Override {
        flag: "--api-key-role", env: "CAMEODB_API_KEY_ROLE", kind: FlagKind::Value,
        placeholder: "<ROLE>", help: "Role for --api-key-hash: admin, writer, or reader",
        apply: |c, v| {
            c.security.override_key_mut().role = Some(crate::auth::Role::parse(v).map_err(|e| ConfigError::SecurityConfig { message: e })?);
            Ok(())
        },
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
    Override {
        flag: "--cluster-psk", env: "CAMEODB_CLUSTER_PSK", kind: FlagKind::Value,
        placeholder: "<HEX>", help: "Inline hex-encoded 32-byte cluster pre-shared key",
        apply: |c, v| { c.network.cluster.psk = Some(v.to_string()); Ok(()) },
    },
    Override {
        flag: "--cluster-psk-file", env: "CAMEODB_CLUSTER_PSK_FILE", kind: FlagKind::Value,
        placeholder: "<PATH>", help: "Path to file containing hex-encoded 32-byte cluster PSK",
        apply: |c, v| { c.network.cluster.psk_file = Some(PathBuf::from(v)); Ok(()) },
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
    //
    // Serialized as JSON rather than TOML because `None` becomes `null` and therefore still
    // *appears*. A TOML schema silently drops every optional setting that happens to default
    // to unset, so `node.profile`, `tls.cert_file`, `tls.key_file` and `cluster.psk_file`
    // were each reported as a typo to anyone who set them — the opposite of this function's
    // job.
    let Ok(schema) = serde_json::to_value(CameoDbConfig::default()) else {
        return Vec::new();
    };

    let mut unknown = Vec::new();
    collect_unrecognized(&parsed, &schema, "", &mut unknown);
    unknown.retain(|key| !NEVER_SERIALIZED_SETTINGS.contains(&key.as_str()));
    unknown
}

/// Settings a config file may set that no serialization can contain, and which therefore
/// cannot appear in the schema above.
///
/// One entry, and it earns it: the cluster PSK is `skip_serializing` precisely so that no
/// config dump can leak it, which also means the schema cannot see it.
const NEVER_SERIALIZED_SETTINGS: &[&str] = &["network.cluster.psk"];

/// Walk `parsed` against `schema`, recording paths absent from the schema.
///
/// Only tables are recursed into. Arrays of tables — `[[security.api_keys]]` — are checked
/// for existence but not for the keys inside them, which is why every field of an entry is
/// optional and validated by name in [`crate::auth::SecurityConfig::load_keyring`].
fn collect_unrecognized(
    parsed: &toml::Value,
    schema: &serde_json::Value,
    prefix: &str,
    unknown: &mut Vec<String>,
) {
    let (Some(parsed), Some(schema)) = (parsed.as_table(), schema.as_object()) else {
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

    /// Authentication: API keys, their roles, and their index scopes.
    #[serde(default)]
    pub security: crate::auth::SecurityConfig,

    /// Maximum single-record size in MB (default: 64).
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
    /// Bind address for HTTP server (default: "127.0.0.1", loopback only)
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

/// Messaging configuration for Kameo remote actors
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct MessagingConfig {
    /// Request timeout in seconds (default: 30)
    #[serde(default = "default_request_timeout_secs")]
    pub request_timeout_secs: u64,

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
        if self.max_record_size_mb == 0 {
            return Err(ConfigError::NetworkConfig {
                message: "max_record_size_mb must be positive".to_string(),
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

        // Posture rules run last: the checks above establish that individual values are
        // usable, and this decides whether the combination is allowed where this node sits.
        self.check_posture()?;

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

impl Default for CameoDbConfig {
    fn default() -> Self {
        Self {
            node: NodeConfig::default(),
            network: NetworkConfig::default(),
            storage: StorageConfig::default(),
            search: SearchConfig::default(),
            security: crate::auth::SecurityConfig::default(),
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
            max_concurrent_requests: default_http_max_concurrent_requests(),
            cors_allowed_origins: default_cors_allowed_origins(),
            admin_enabled: default_admin_enabled(),
            tls: TlsConfig::default(),
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

// Default value functions for serde
/// Loopback by default.
///
/// Binding every interface out of the box put an unauthenticated read/write/delete API on
/// the network the moment the binary ran. Reaching the node from other hosts is now a
/// deliberate act — set `bind_address` and declare a `profile` to go with it.
fn default_http_bind_address() -> String {
    "127.0.0.1".to_string()
}

fn default_http_port() -> u16 {
    9480
}

fn default_request_timeout() -> u64 {
    30
}

fn default_max_record_size_mb() -> usize {
    64
}

fn default_http_max_concurrent_requests() -> usize {
    128
}

fn default_admin_enabled() -> bool {
    true
}

/// No cross-origin browser access by default.
///
/// CORS governs browsers and nothing else, so an empty list costs API and MCP clients
/// nothing while removing the drive-by attack surface that `["*"]` handed to any web page
/// the operator happened to visit — which mattered because no endpoint requires auth.
fn default_cors_allowed_origins() -> Vec<String> {
    Vec::new()
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

fn default_messaging_max_concurrent_requests() -> usize {
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

    /// A setting reaching the config struct is not the same as it reaching the code that
    /// acts on it. The supervisor read `CAMEODB_SUPERVISOR_TIMEOUT_SECS` from the
    /// environment directly, so this field was populated and then ignored — the file and the
    /// `--supervisor-timeout-secs` flag did nothing, and only the env var appeared to work.
    #[test]
    fn the_supervisor_timeout_is_configurable_from_the_file() {
        let config: CameoDbConfig = toml::from_str(
            "[search]\n\
             supervisor_timeout_secs = 30\n",
        )
        .expect("partial config");

        assert_eq!(config.search.supervisor_timeout_secs, 30);
        assert_eq!(
            CameoDbConfig::default().search.supervisor_timeout_secs,
            5,
            "the documented default and the code's default have to agree"
        );
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
        assert_eq!(config.network.http.bind_address, "127.0.0.1");
        assert_eq!(config.network.cluster.cluster_port, 9580);
        assert_eq!(
            config.storage.data_paths,
            vec![PathBuf::from("./data/cameodb")]
        );
        assert_eq!(config.search.indexer_memory_min_mb, 64);
        assert_eq!(config.max_record_size_mb, 64);
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
        // Loopback by default: a fresh node is not reachable off-box until asked.
        assert_eq!(config.network.http.bind_address, "127.0.0.1");
        assert_eq!(config.search.indexer_memory_min_mb, 64);
        assert_eq!(config.search.indexer_memory_max_mb, 512);
        assert_eq!(config.storage.default_batch_size, 1000);
        assert_eq!(config.max_record_size_mb, 64);
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
        assert!(sample.contains("max_record_size_mb = 64"));
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
        // HTTP body: max_record_size_mb + 64 = 64 + 64 = 128
        assert_eq!(config.effective_max_body_size_mb(), 128);
        // Remote message: 64 MB + 25% overhead = 80 MB in bytes
        assert_eq!(
            config.effective_remote_message_size_bytes(),
            64 * 1024 * 1024 + 64 * 1024 * 1024 / 4
        );
        // Timeout: max(60, 64/10) = max(60, 6) = 60
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

    /// A PSK that round-trips is the only kind the swarm can start with, and `validate()`
    /// now shares this code path, so a config that validates is one that will boot.
    #[test]
    fn psk_hex_round_trips_through_load() {
        let mut config = CameoDbConfig::default();
        config.network.cluster.psk =
            Some("00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff".to_string());
        let psk = config
            .network
            .cluster
            .load_psk()
            .expect("valid psk")
            .expect("psk present");
        assert_eq!(psk.bytes()[0], 0x00);
        assert_eq!(psk.bytes()[1], 0x11);
        assert_eq!(psk.bytes()[31], 0xff);
    }

    #[test]
    fn psk_rejects_wrong_length_and_non_hex() {
        let mut config = CameoDbConfig::default();
        for bad in ["abc", &"a".repeat(63), &"a".repeat(65), &"z".repeat(64)] {
            config.network.cluster.psk = Some(bad.to_string());
            assert!(
                config.network.cluster.load_psk().is_err(),
                "'{}' must be rejected",
                bad
            );
            // validate() must agree — it is the same code path, and that is the point.
            assert!(config.validate().is_err(), "validate accepted '{}'", bad);
        }
    }

    #[test]
    fn psk_is_trimmed_so_a_file_with_a_trailing_newline_works() {
        let dir = std::env::temp_dir().join(format!("cameodb-psk-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        let path = dir.join("cluster.psk");
        std::fs::write(&path, format!("{}\n", "ab".repeat(32))).expect("write psk");

        let mut config = CameoDbConfig::default();
        config.network.cluster.psk_file = Some(path.clone());
        let psk = config
            .network
            .cluster
            .load_psk()
            .expect("valid psk file")
            .expect("psk present");
        assert_eq!(psk.bytes(), [0xab; 32]);
        std::fs::remove_file(&path).ok();
    }

    /// The secret must not reach a log or a config dump by any ordinary route.
    #[test]
    fn psk_is_not_printable_or_serializable() {
        let secret = "ab".repeat(32);
        let mut config = CameoDbConfig::default();
        config.network.cluster.psk = Some(secret.clone());

        let serialized = toml::to_string_pretty(&config).expect("serialize");
        assert!(
            !serialized.contains(&secret),
            "psk leaked into serialized config"
        );

        let psk = config.network.cluster.load_psk().unwrap().unwrap();
        let debug = format!("{:?}", psk);
        assert!(!debug.contains(&secret), "psk leaked via Debug: {}", debug);
        assert!(debug.contains("redacted"), "{}", debug);
    }

    /// pnet disables QUIC, so a QUIC address alongside a PSK can never connect. Catching
    /// it here beats a dial-time warning nobody reads.
    #[test]
    fn psk_with_quic_addresses_is_rejected() {
        let mut config = CameoDbConfig::default();
        config.network.cluster.psk = Some("ab".repeat(32));
        config.network.cluster.seed_nodes = vec!["/ip4/10.0.0.5/udp/9580/quic-v1".to_string()];
        let err = config.validate().expect_err("quic + psk must be rejected");
        assert!(err.to_string().contains("QUIC"), "{}", err);

        config.network.cluster.seed_nodes = vec!["/ip4/10.0.0.5/tcp/9580".to_string()];
        assert!(config.validate().is_ok(), "tcp seed must be accepted");
    }

    /// TLS config is loaded before the banner now, but validation still has to reject the
    /// half-configured cases up front.
    #[test]
    fn tls_requires_both_files_and_they_must_exist() {
        let mut config = CameoDbConfig::default();
        config.network.http.tls.enabled = true;
        assert!(config.validate().is_err(), "no cert/key configured");

        config.network.http.tls.cert_file = Some(PathBuf::from("/nonexistent/cert.pem"));
        assert!(config.validate().is_err(), "no key configured");

        config.network.http.tls.key_file = Some(PathBuf::from("/nonexistent/key.pem"));
        let err = config
            .validate()
            .expect_err("missing files must be rejected");
        assert!(err.to_string().contains("not found"), "{}", err);
    }

    #[test]
    fn wildcard_cors_is_rejected_outside_dev() {
        let mut config = CameoDbConfig::default();
        config.node.profile = Some(crate::posture::Profile::Internal);
        config.network.http.bind_address = "0.0.0.0".to_string();
        config.network.http.cors_allowed_origins = vec!["*".to_string()];
        let err = config.validate().expect_err("wildcard must be rejected");
        assert!(err.to_string().contains("cors"), "{}", err);
    }

    /// An empty origin list used to be a config error, which pushed operators towards
    /// "*". It is now the default and must validate.
    #[test]
    fn empty_cors_validates() {
        let mut config = CameoDbConfig::default();
        config.network.http.cors_allowed_origins = vec![];
        assert!(config.validate().is_ok());
    }

    #[test]
    fn profile_flag_and_env_are_parsed() {
        let parsed = cli(&["--profile", "external"]);
        let mut config = CameoDbConfig::default();
        config = CameoDbConfig::apply_overrides(config, &parsed).expect("apply");
        assert_eq!(config.node.profile, Some(crate::posture::Profile::External));

        assert!(cli_help().contains("--profile"));
        assert!(CliOverrides::parse(["--profile", "nonsense"].map(String::from)).is_ok());
        let bad = cli(&["--profile", "nonsense"]);
        assert!(CameoDbConfig::apply_overrides(CameoDbConfig::default(), &bad).is_err());
    }
}
