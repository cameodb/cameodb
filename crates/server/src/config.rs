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
//!     writer_memory_min_mb: 16
//!     writer_memory_max_mb: 256
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
    /// Server configuration (HTTP, networking)
    pub server: ServerConfig,

    /// Storage configuration (data paths, sharding)
    pub storage: StorageConfig,

    /// Search engine configuration (Tantivy settings)
    pub search: SearchConfig,

    /// Cluster configuration for distributed deployment
    #[serde(default)]
    pub cluster: ClusterConfig,
}

/// HTTP server and networking configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    /// HTTP server configuration
    pub http: HttpConfig,

    /// Node-level configuration
    pub node: NodeConfig,
}

/// HTTP server specific configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpConfig {
    /// Port for HTTP server (default: 9480)
    #[serde(default = "default_http_port")]
    pub port: u16,

    /// Host to bind HTTP server (default: "0.0.0.0")
    #[serde(default = "default_http_host")]
    pub host: String,

    /// Request timeout in seconds (default: 30)
    #[serde(default = "default_request_timeout")]
    pub request_timeout_secs: u64,

    /// Maximum request body size in MB (default: 20)
    #[serde(default = "default_max_body_size_mb")]
    pub max_body_size_mb: usize,

    /// Enable CORS (default: true)
    #[serde(default = "default_cors_enabled")]
    pub cors_enabled: bool,
}

/// Node-level configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    /// Maximum number of shards this node can host (default: 10)
    #[serde(default = "default_max_shards")]
    pub max_shards: usize,

    /// Default writer memory per shard in MB (default: 50)
    #[serde(default = "default_writer_memory_default_mb")]
    pub writer_memory_default_mb: usize,

    /// If no shards exist on first startup, create this many shards (default: 4)
    /// Set to 0 to disable automatic initialization.
    #[serde(default = "default_init_shards")]
    pub init_shards: usize,
}

/// Storage configuration for data persistence
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// List of data directories for multi-disk setups
    /// Each path serves as a mount point for data storage
    pub data_paths: Vec<PathBuf>,

    /// Disk usage threshold before rejecting new data (0.0-1.0, default: 0.9)
    #[serde(default = "default_disk_usage_threshold")]
    pub disk_usage_threshold: f64,

    /// Enable WAL fsync for durability (default: true)
    #[serde(default = "default_wal_sync")]
    pub wal_sync: bool,

    /// WAL segment size in MB (default: 64)
    #[serde(default = "default_wal_segment_size_mb")]
    pub wal_segment_size_mb: usize,

    /// Default batch size for smart commit calculations (default: 1000)
    #[serde(default = "default_default_batch_size")]
    pub default_batch_size: usize,
}

/// Cluster configuration for distributed actor system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    /// Enable distributed actor system (default: false)
    #[serde(default = "default_distributed_actors")]
    pub distributed_actors: bool,

    /// Cluster communication port for libp2p (default: 9580)
    #[serde(default = "default_cluster_port")]
    pub cluster_port: u16,

    /// Bootstrap nodes for cluster discovery
    #[serde(default)]
    pub bootstrap_nodes: Vec<String>,

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

    /// mDNS interface filtering options
    #[serde(default)]
    pub mdns_filter: MdnsFilterConfig,
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

/// mDNS interface filtering configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MdnsFilterConfig {
    /// Additional interface patterns to allow (besides defaults)
    #[serde(default)]
    pub allow_patterns: Vec<String>,

    /// Additional interface patterns to deny
    #[serde(default)]
    pub deny_patterns: Vec<String>,

    /// Enable IPv6 mDNS discovery (default: false)
    #[serde(default = "default_ipv6_enabled")]
    pub ipv6_enabled: bool,
}

/// Tantivy search engine configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchConfig {
    /// Minimum writer memory per shard in MB (default: 16)
    #[serde(default = "default_writer_memory_min_mb")]
    pub writer_memory_min_mb: usize,

    /// Maximum writer memory per shard in MB (default: 256)
    #[serde(default = "default_writer_memory_max_mb")]
    pub writer_memory_max_mb: usize,

    /// Total memory limit for all search operations in MB (default: 1024)
    #[serde(default = "default_total_memory_limit_mb")]
    pub total_memory_limit_mb: usize,

    /// Memory pressure threshold (0.0-1.0) to trigger cleanup (default: 0.8)
    #[serde(default = "default_pressure_threshold")]
    pub memory_pressure_threshold: f64,

    /// Number of search threads (default: num_cpus)
    #[serde(default = "default_search_threads")]
    pub search_threads: usize,
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
            serde_yaml::from_str(content).with_context(|| "Failed to parse YAML configuration")
        } else {
            // Try TOML first, then YAML
            toml::from_str(content)
                .or_else(|_| serde_yaml::from_str(content))
                .with_context(|| "Failed to parse configuration (tried TOML and YAML)")
        }
    }

    /// Apply environment variable overrides
    fn apply_env_overrides(mut config: Self) -> Result<Self> {
        // HTTP configuration
        if let Ok(port) = std::env::var("CAMEODB_HTTP_PORT") {
            config.server.http.port = port.parse().with_context(|| "Invalid CAMEODB_HTTP_PORT")?;
        }

        if let Ok(host) = std::env::var("CAMEODB_HTTP_HOST") {
            config.server.http.host = host;
        }

        // Storage configuration
        if let Ok(data_paths) = std::env::var("CAMEODB_DATA_PATHS") {
            config.storage.data_paths = data_paths.split(':').map(PathBuf::from).collect();
        }

        // Search configuration
        if let Ok(min_mem) = std::env::var("CAMEODB_WRITER_MEMORY_MIN_MB") {
            config.search.writer_memory_min_mb = min_mem
                .parse()
                .with_context(|| "Invalid CAMEODB_WRITER_MEMORY_MIN_MB")?;
        }

        if let Ok(max_mem) = std::env::var("CAMEODB_WRITER_MEMORY_MAX_MB") {
            config.search.writer_memory_max_mb = max_mem
                .parse()
                .with_context(|| "Invalid CAMEODB_WRITER_MEMORY_MAX_MB")?;
        }

        if let Ok(total_mem) = std::env::var("CAMEODB_TOTAL_MEMORY_LIMIT_MB") {
            config.search.total_memory_limit_mb = total_mem
                .parse()
                .with_context(|| "Invalid CAMEODB_TOTAL_MEMORY_LIMIT_MB")?;
        }

        if let Ok(threshold) = std::env::var("CAMEODB_MEMORY_PRESSURE_THRESHOLD") {
            config.search.memory_pressure_threshold = threshold
                .parse()
                .with_context(|| "Invalid CAMEODB_MEMORY_PRESSURE_THRESHOLD")?;
        }

        // Cluster configuration
        if let Ok(distributed) = std::env::var("CAMEODB_DISTRIBUTED_ACTORS") {
            let normalized = distributed.trim().to_ascii_lowercase();
            config.cluster.distributed_actors = matches!(normalized.as_str(), "true" | "1" | "yes");
        }

        if let Ok(port) = std::env::var("CAMEODB_CLUSTER_PORT") {
            config.cluster.cluster_port = port
                .parse()
                .with_context(|| "Invalid CAMEODB_CLUSTER_PORT")?;
        }

        if let Ok(name) = std::env::var("CAMEODB_CLUSTER_NAME") {
            if !name.trim().is_empty() {
                config.cluster.cluster_name = name;
            }
        }

        if let Ok(nodes) = std::env::var("CAMEODB_BOOTSTRAP_NODES") {
            let parsed: Vec<String> = nodes
                .split(|c| c == ',' || c == ';')
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
                config.cluster.bootstrap_nodes = parsed;
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
        if self.server.http.port == 0 {
            return Err(ConfigError::NetworkConfig {
                message: "HTTP port cannot be 0".to_string(),
            }
            .into());
        }

        if self.server.http.request_timeout_secs == 0 {
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

        if self.storage.disk_usage_threshold < 0.1 || self.storage.disk_usage_threshold > 1.0 {
            return Err(ConfigError::StorageConfig {
                message: "Disk usage threshold must be between 0.1 and 1.0".to_string(),
            }
            .into());
        }

        // Validate memory configuration
        if self.search.writer_memory_min_mb < 16 {
            return Err(ConfigError::MemoryConfig {
                message: "Writer memory minimum cannot be less than 16MB".to_string(),
            }
            .into());
        }

        if self.search.writer_memory_max_mb > 1024 {
            return Err(ConfigError::MemoryConfig {
                message: "Writer memory maximum cannot exceed 1024MB".to_string(),
            }
            .into());
        }

        if self.search.writer_memory_min_mb >= self.search.writer_memory_max_mb {
            return Err(ConfigError::MemoryConfig {
                message: "Writer memory minimum must be less than maximum".to_string(),
            }
            .into());
        }

        if self.search.memory_pressure_threshold < 0.1
            || self.search.memory_pressure_threshold > 1.0
        {
            return Err(ConfigError::MemoryConfig {
                message: "Memory pressure threshold must be between 0.1 and 1.0".to_string(),
            }
            .into());
        }

        if self.search.total_memory_limit_mb < self.search.writer_memory_max_mb {
            return Err(ConfigError::MemoryConfig {
                message: "Total memory limit must be at least as large as max writer memory"
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
            server: ServerConfig {
                http: HttpConfig::default(),
                node: NodeConfig::default(),
            },
            storage: StorageConfig::default(),
            search: SearchConfig::default(),
            cluster: ClusterConfig::default(),
        }
    }
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            port: default_http_port(),
            host: default_http_host(),
            request_timeout_secs: default_request_timeout(),
            max_body_size_mb: default_max_body_size_mb(),
            cors_enabled: default_cors_enabled(),
        }
    }
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            max_shards: default_max_shards(),
            writer_memory_default_mb: default_writer_memory_default_mb(),
            init_shards: default_init_shards(),
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_paths: vec![PathBuf::from("./data/cameodb")],
            disk_usage_threshold: default_disk_usage_threshold(),
            wal_sync: default_wal_sync(),
            wal_segment_size_mb: default_wal_segment_size_mb(),
            default_batch_size: default_default_batch_size(),
        }
    }
}

impl Default for SearchConfig {
    fn default() -> Self {
        Self {
            writer_memory_min_mb: default_writer_memory_min_mb(),
            writer_memory_max_mb: default_writer_memory_max_mb(),
            total_memory_limit_mb: default_total_memory_limit_mb(),
            memory_pressure_threshold: default_pressure_threshold(),
            search_threads: default_search_threads(),
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

impl Default for MdnsFilterConfig {
    fn default() -> Self {
        Self {
            allow_patterns: Vec::new(),
            deny_patterns: Vec::new(),
            ipv6_enabled: default_ipv6_enabled(),
        }
    }
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            distributed_actors: default_distributed_actors(),
            cluster_port: default_cluster_port(),
            bootstrap_nodes: Vec::new(),
            cluster_name: default_cluster_name(),
            listen_addrs: Vec::new(),
            bootstrap_peers: Vec::new(),
            messaging: MessagingConfig::default(),
            mdns_filter: MdnsFilterConfig::default(),
        }
    }
}

// Default value functions for serde
fn default_http_port() -> u16 {
    9480
}
fn default_http_host() -> String {
    "0.0.0.0".to_string()
}
fn default_request_timeout() -> u64 {
    30
}
fn default_max_body_size_mb() -> usize {
    20
}
fn default_cors_enabled() -> bool {
    true
}

fn default_max_shards() -> usize {
    8
}
fn default_writer_memory_default_mb() -> usize {
    32
}
fn default_init_shards() -> usize {
    4
}

fn default_disk_usage_threshold() -> f64 {
    0.9
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

fn default_writer_memory_min_mb() -> usize {
    16
}
fn default_writer_memory_max_mb() -> usize {
    256
}
fn default_total_memory_limit_mb() -> usize {
    1024
}
fn default_pressure_threshold() -> f64 {
    0.8
}
fn default_search_threads() -> usize {
    num_cpus::get()
}

fn default_distributed_actors() -> bool {
    false
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

fn default_ipv6_enabled() -> bool {
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_configuration() {
        let config = CameoDbConfig::default();
        assert_eq!(config.server.http.port, 9480);
        assert_eq!(config.server.http.host, "0.0.0.0");
        assert_eq!(config.search.writer_memory_min_mb, 16);
        assert_eq!(config.search.writer_memory_max_mb, 256);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_memory_validation() {
        let mut config = CameoDbConfig::default();

        // Test invalid memory range
        config.search.writer_memory_min_mb = 300;
        config.search.writer_memory_max_mb = 256;
        assert!(config.validate().is_err());

        // Test memory too small
        config.search.writer_memory_min_mb = 8;
        config.search.writer_memory_max_mb = 256;
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
        config.storage.disk_usage_threshold = 1.5;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_sample_config_generation() {
        let sample = CameoDbConfig::generate_sample_config().unwrap();
        assert!(sample.contains("port = 9480"));
        assert!(sample.contains("writer_memory_min_mb = 16"));
        assert!(sample.contains("data_paths"));
    }
}
