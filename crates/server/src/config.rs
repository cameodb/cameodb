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
//!     total_memory_limit_mb: 1024
//!     pressure_threshold: 0.8
//! ```

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;
use thiserror::Error;
use tracing::info;

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
}

/// Complete CameoDB configuration structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CameoDbConfig {
    /// Node-level configuration (sharding, identity)
    #[serde(default)]
    pub node: NodeConfig,

    /// Network configuration (HTTP, cluster)
    pub network: NetworkConfig,

    /// Storage configuration (data paths, sharding)
    pub storage: StorageConfig,

    /// Search engine configuration (Tantivy settings)
    pub search: SearchConfig,
}

/// Network configuration wrapper
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkConfig {
    /// HTTP server configuration
    pub http: HttpConfig,

    /// Cluster configuration for distributed deployment
    #[serde(default)]
    pub cluster: ClusterConfig,
}

/// HTTP server specific configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
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

    /// Maximum request body size in MB (default: 20)
    #[serde(default = "default_max_body_size_mb")]
    pub max_body_size_mb: usize,

    /// CORS allowed origins (default: ["*"])
    #[serde(default = "default_cors_allowed_origins")]
    pub cors_allowed_origins: Vec<String>,
}

/// Node-level configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
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
}

/// Cluster configuration for distributed actor system
#[derive(Debug, Clone, Serialize, Deserialize)]
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
pub struct SearchConfig {
    /// Minimum indexer memory in MB (default: 16)
    #[serde(default = "default_indexer_memory_min_mb")]
    pub indexer_memory_min_mb: usize,

    /// Maximum indexer memory in MB (default: 256)
    #[serde(default = "default_indexer_memory_max_mb")]
    pub indexer_memory_max_mb: usize,

    /// Total memory limit in MB (default: 1024)
    #[serde(default = "default_total_memory_limit_mb")]
    pub total_memory_limit_mb: usize,

    /// Memory pressure threshold in percent (0-100, default: 80)
    #[serde(default = "default_memory_pressure_threshold_percent")]
    pub memory_pressure_threshold_percent: u8,

    /// Number of search threads (default: num_cpus)
    #[serde(default = "default_search_threads")]
    pub search_threads: usize,

    /// Default search result limit when not specified in request (default: 10)
    #[serde(default = "default_search_limit")]
    pub default_search_limit: usize,
}

impl CameoDbConfig {
    /// Load configuration from multiple sources with precedence:
    /// 1. Command line arguments (highest priority)
    /// 2. Environment variables
    /// 3. Configuration file
    /// 4. Defaults (lowest priority)
    pub fn load() -> Result<Self> {
        // Start with defaults
        let mut config = Self::default();

        // Try to load from config file
        if let Ok(file_config) = Self::load_from_file() {
            config = Self::merge_configs(config, file_config);
        }

        // Apply environment variable overrides
        config = Self::apply_env_overrides(config)?;

        // Validate the final configuration
        config.validate()?;

        Ok(config)
    }

    /// Load configuration from YAML or TOML file
    pub fn load_from_file() -> Result<Self> {
        let mut config_paths = vec![
            "cameodb.toml",
            "cameodb.yaml",
            "cameodb.yml",
            "config/cameodb.toml",
            "config/cameodb.yaml",
            "/etc/cameodb/cameodb.toml",
            "/etc/cameodb/config.toml",
        ];

        // If CAMEODB_CONFIG env var is set, prepend it to the search list
        let env_config = std::env::var("CAMEODB_CONFIG").ok();
        if let Some(path) = env_config.as_deref() {
            // Insert at the beginning to give it highest priority
            config_paths.insert(0, path);
        }

        for path in &config_paths {
            if let Ok(content) = fs::read_to_string(path) {
                info!("📄 Loading configuration from: {}", path);
                return Self::parse_config_content(&content, path)
                    .with_context(|| format!("Failed to parse config file: {}", path));
            }
        }

        Err(ConfigError::FileNotFound {
            path: config_paths.join(", "),
        }
        .into())
    }

    /// Parse configuration content based on file extension
    fn parse_config_content(content: &str, path: &str) -> Result<Self> {
        if path.ends_with(".toml") {
            toml::from_str(content).with_context(|| "Failed to parse TOML configuration")
        } else if path.ends_with(".yaml") || path.ends_with(".yml") {
            serde_yml::from_str(content).with_context(|| "Failed to parse YAML configuration")
        } else {
            // Try TOML first, then YAML
            toml::from_str(content)
                .or_else(|_| serde_yml::from_str(content))
                .with_context(|| "Failed to parse configuration (tried TOML and YAML)")
        }
    }

    /// Apply environment variable overrides
    fn apply_env_overrides(mut config: Self) -> Result<Self> {
        // HTTP configuration
        if let Ok(port) = std::env::var("CAMEODB_HTTP_PORT") {
            config.network.http.port = port.parse().with_context(|| "Invalid CAMEODB_HTTP_PORT")?;
        }

        if let Ok(bind_addr) = std::env::var("CAMEODB_HTTP_BIND_ADDRESS") {
            config.network.http.bind_address = bind_addr;
        }

        // Storage configuration
        if let Ok(data_paths) = std::env::var("CAMEODB_DATA_PATHS") {
            config.storage.data_paths = data_paths.split(':').map(PathBuf::from).collect();
        }

        if let Ok(wal_sync) = std::env::var("CAMEODB_STORAGE_WAL_SYNC") {
            let normalized = wal_sync.trim().to_ascii_lowercase();
            config.storage.wal_sync = matches!(normalized.as_str(), "true" | "1" | "yes");
        }

        if let Ok(batch_size) = std::env::var("CAMEODB_STORAGE_DEFAULT_BATCH_SIZE") {
            config.storage.default_batch_size = batch_size
                .parse()
                .with_context(|| "Invalid CAMEODB_STORAGE_DEFAULT_BATCH_SIZE")?;
        }

        // Search configuration
        if let Ok(min_mem) = std::env::var("CAMEODB_INDEXER_MEMORY_MIN_MB") {
            config.search.indexer_memory_min_mb = min_mem
                .parse()
                .with_context(|| "Invalid CAMEODB_INDEXER_MEMORY_MIN_MB")?;
        }

        if let Ok(max_mem) = std::env::var("CAMEODB_INDEXER_MEMORY_MAX_MB") {
            config.search.indexer_memory_max_mb = max_mem
                .parse()
                .with_context(|| "Invalid CAMEODB_INDEXER_MEMORY_MAX_MB")?;
        }

        if let Ok(total_mem) = std::env::var("CAMEODB_TOTAL_MEMORY_LIMIT_MB") {
            config.search.total_memory_limit_mb = total_mem
                .parse()
                .with_context(|| "Invalid CAMEODB_TOTAL_MEMORY_LIMIT_MB")?;
        }

        if let Ok(threshold) = std::env::var("CAMEODB_MEMORY_PRESSURE_THRESHOLD_PERCENT") {
            config.search.memory_pressure_threshold_percent = threshold
                .parse()
                .with_context(|| "Invalid CAMEODB_MEMORY_PRESSURE_THRESHOLD_PERCENT")?;
        }

        if let Ok(limit) = std::env::var("CAMEODB_DEFAULT_SEARCH_LIMIT") {
            config.search.default_search_limit = limit
                .parse()
                .with_context(|| "Invalid CAMEODB_DEFAULT_SEARCH_LIMIT")?;
        }

        // Node configuration
        if let Ok(label) = std::env::var("CAMEODB_NODE_LABEL") {
            config.node.label = Some(label);
        }

        if let Ok(zone) = std::env::var("CAMEODB_NODE_ZONE") {
            config.node.zone = zone;
        }

        // Cluster configuration
        if let Ok(enabled) = std::env::var("CAMEODB_CLUSTER_ENABLED") {
            let normalized = enabled.trim().to_ascii_lowercase();
            config.network.cluster.enabled = matches!(normalized.as_str(), "true" | "1" | "yes");
        }

        if let Ok(bind_addr) = std::env::var("CAMEODB_CLUSTER_BIND_ADDRESS") {
            config.network.cluster.bind_address = bind_addr;
        }

        if let Ok(port) = std::env::var("CAMEODB_CLUSTER_PORT") {
            config.network.cluster.cluster_port = port
                .parse()
                .with_context(|| "Invalid CAMEODB_CLUSTER_PORT")?;
        }

        if let Ok(name) = std::env::var("CAMEODB_CLUSTER_NAME") {
            if !name.trim().is_empty() {
                config.network.cluster.cluster_name = name;
            }
        }

        if let Ok(nodes) = std::env::var("CAMEODB_SEED_NODES") {
            let parsed: Vec<String> = nodes
                .split([',', ';'])
                .filter_map(|entry| {
                    let trimmed = entry.trim();
                    if trimmed.is_empty() {
                        None
                    } else {
                        Some(trimmed.to_string())
                    }
                })
                .collect();

            if !parsed.is_empty() {
                config.network.cluster.seed_nodes = parsed;
            }
        }

        if let Ok(nodes) = std::env::var("CAMEODB_CLUSTER_NODES") {
            let parsed: Vec<String> = nodes
                .split([',', ';'])
                .filter_map(|entry| {
                    let trimmed = entry.trim();
                    if trimmed.is_empty() {
                        None
                    } else {
                        Some(trimmed.to_string())
                    }
                })
                .collect();

            if !parsed.is_empty() {
                config.network.cluster.cluster_nodes = parsed;
            }
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

        if self.search.indexer_memory_max_mb > 1024 {
            return Err(ConfigError::MemoryConfig {
                message: "Indexer memory maximum cannot exceed 1024MB".to_string(),
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
        }
    }
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            http: HttpConfig::default(),
            cluster: ClusterConfig::default(),
        }
    }
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            bind_address: default_http_bind_address(),
            port: default_http_port(),
            request_timeout_secs: default_request_timeout(),
            max_body_size_mb: default_max_body_size_mb(),
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
            default_search_limit: default_search_limit(),
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

fn default_max_body_size_mb() -> usize {
    32
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

fn default_indexer_memory_min_mb() -> usize {
    16
}
fn default_indexer_memory_max_mb() -> usize {
    256
}
fn default_total_memory_limit_mb() -> usize {
    1024
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_configuration() {
        let config = CameoDbConfig::default();
        assert_eq!(config.network.http.port, 9480);
        assert_eq!(config.network.http.bind_address, "0.0.0.0");
        assert_eq!(config.search.indexer_memory_min_mb, 16);
        assert_eq!(config.search.indexer_memory_max_mb, 256);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_memory_validation() {
        let mut config = CameoDbConfig::default();

        // Test invalid memory range
        config.search.indexer_memory_min_mb = 300;
        config.search.indexer_memory_max_mb = 256;
        assert!(config.validate().is_err());

        // Test memory too small
        config.search.indexer_memory_min_mb = 8;
        config.search.indexer_memory_max_mb = 256;
        assert!(config.validate().is_err());
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
        assert!(sample.contains("indexer_memory_min_mb = 16"));
        assert!(sample.contains("data_paths"));
    }
}
