# CameoDB Configuration Guide

This guide covers comprehensive configuration management for CameoDB, including network settings, storage paths, and Tantivy search engine tuning.

## Table of Contents

- [Quick Start](#quick-start)
- [Configuration Sources](#configuration-sources)
- [Configuration Reference](#configuration-reference)
- [Environment Variables](#environment-variables)
- [Multi-Disk Setup](#multi-disk-setup)
- [Performance Tuning](#performance-tuning)
- [Production Deployment](#production-deployment)
- [Troubleshooting](#troubleshooting)

## Quick Start

### 1. Generate Default Configuration

```bash
# Generate sample configuration file
cargo run --release --bin cameodb generate-config > cameodb.toml

# Or use the configuration manager
./scripts/setup/config-manager.sh generate
```

### 2. Basic Configuration

Edit `cameodb.toml`:

```toml
[node]
label = "cameo-node-01"

[network.http]
bind_address = "0.0.0.0"
port = 9480

[storage]
data_paths = ["./data/cameodb"]

[search]
indexer_memory_min_mb = 64
indexer_memory_max_mb = 512
total_memory_limit_mb = 2048
default_search_limit = 10
```

### 3. Start CameoDB

```bash
cargo run --release --bin cameodb
```

## Configuration Sources

CameoDB loads configuration from multiple sources with the following precedence (highest to lowest):

1. **Environment Variables** (highest priority)
2. **Configuration Files**
3. **Default Values** (lowest priority)

### Configuration File Locations

CameoDB searches for configuration files in this order:

1. `cameodb.toml` (current directory)
2. `cameodb.yaml` (current directory)
3. `config/cameodb.toml`
4. `config/cameodb.yaml`
5. `/etc/cameodb/config.toml`

Both TOML and YAML formats are supported.

## Configuration Reference

### Network Configuration

```toml
[network.http]
# Port for HTTP (default: 9480)
port = 9480

# Bind address for HTTP (default: "0.0.0.0")
bind_address = "0.0.0.0"

# Request timeout in seconds (default: 30)
request_timeout_secs = 30

# Maximum request body size in MB (default: 200)
max_body_size_mb = 200

# CORS allowed origins (default: ["*"])
cors_allowed_origins = ["*"]
```

### Node Configuration

```toml
[node]
# Human-readable label for this node (optional)
label = "cameo-node-01"

# Topology zone for rack/datacenter awareness (default: "default")
zone = "default"
```

### Search Configuration

```toml
[search]
# Minimum memory for each indexer thread in MB (default: 64)
indexer_memory_min_mb = 64

# Maximum memory for each indexer thread in MB (default: 512)
indexer_memory_max_mb = 512

# Total memory limit for all search operations in MB (default: 2048)
total_memory_limit_mb = 2048

# Threshold for memory pressure (percent, default: 80)
memory_pressure_threshold_percent = 80

# Number of search threads (default: 8, fallback to max(2, CPU/2) if set to 0)
search_threads = 8

# Default search result limit (default: 10)
default_search_limit = 10
```

### Cluster Configuration

```toml
[network.cluster]
# Enable distributed cluster mode (default: false)
enabled = true

# Bind address for cluster communication (default: "0.0.0.0")
bind_address = "0.0.0.0"

# Cluster communication port (default: 9580)
port = 9580

# Cluster name for isolation (default: "cameodb-cluster")
cluster_name = "cameodb-cluster"

# Seed nodes for initial discovery
seed_nodes = ["10.0.1.5:9580", "10.0.1.6:9580"]
```

## Environment Variables

CameoDB supports environment variable overrides for all major settings. Prefix variable names with `CAMEODB_`.

### Node Configuration
- `CAMEODB_NODE_LABEL`: Node label
- `CAMEODB_NODE_ZONE`: Topology zone

### Network Configuration
- `CAMEODB_HTTP_PORT`: HTTP port
- `CAMEODB_HTTP_BIND_ADDRESS`: HTTP bind address
- `CAMEODB_CLUSTER_ENABLED`: Enable/disable cluster (`true`/`false`)
- `CAMEODB_CLUSTER_PORT`: Cluster communication port
- `CAMEODB_CLUSTER_BIND_ADDRESS`: Cluster bind address
- `CAMEODB_CLUSTER_NAME`: Cluster name
- `CAMEODB_SEED_NODES`: Comma-separated list of seed nodes

### Storage Configuration
- `CAMEODB_DATA_PATHS`: Colon-separated list of data paths

### Search Configuration
- `CAMEODB_INDEXER_MEMORY_MIN_MB`: Minimum indexer memory
- `CAMEODB_INDEXER_MEMORY_MAX_MB`: Maximum indexer memory
- `CAMEODB_TOTAL_MEMORY_LIMIT_MB`: Total memory limit
- `CAMEODB_MEMORY_PRESSURE_THRESHOLD_PERCENT`: Memory pressure threshold
- `CAMEODB_DEFAULT_SEARCH_LIMIT`: Default search result limit

## Multi-Disk Setup

For high-throughput deployments with multiple storage devices:

### Generate Multi-Disk Configuration

```bash
./scripts/setup/config-manager.sh multi-disk
```

### Manual Configuration

```toml
[node]
label = "cameodb-multi-disk"

[network.http]
port = 9480
bind_address = "0.0.0.0"

[storage]
data_paths = [
  "/mnt/nvme1/cameodb",
  "/mnt/nvme2/cameodb", 
  "/mnt/ssd1/cameodb",
  "/mnt/ssd2/cameodb"
]
disk_usage_threshold_percent = 85
wal_segment_size_mb = 128
max_shards_per_node = 50

[search]
indexer_memory_max_mb = 512
total_memory_limit_mb = 4096
search_threads = 16
```

### Benefits

- **Parallel I/O**: Distribute shards across multiple disks
- **Fault Tolerance**: Continue operation if one disk fails
- **Performance**: Increased throughput and reduced latency

## Performance Tuning

### High-Performance Configuration

```bash
./scripts/setup/config-manager.sh performance
```

### Key Performance Parameters

#### Memory Configuration

```toml
[search]
# Higher memory allocation for better write performance
indexer_memory_min_mb = 64
indexer_memory_max_mb = 1024
total_memory_limit_mb = 8192

# Aggressive memory usage
memory_pressure_threshold_percent = 90
search_threads = 16
default_batch_size = 2000
```

#### Storage Optimization

```toml
[storage]
# Disable fsync for maximum write speed (less durable)
wal_sync = false

# Large WAL segments reduce overhead
wal_segment_size_mb = 256

# Use more disk space
disk_usage_threshold_percent = 95
```

#### Threading Configuration

```toml
[search]
# Maximize CPU utilization
search_threads = 32
```

### Performance vs Durability Trade-offs

| Setting | Performance | Durability | Note |
|---------|-------------|------------|------|
| `wal_sync = false` | ⬆️ High | ⬇️ Low | Risk of data loss on crash |
| `indexer_memory_max_mb = 1024` | ⬆️ High | ➡️ Same | Uses more RAM |
| `memory_pressure_threshold_percent = 90` | ⬆️ High | ➡️ Same | Higher memory usage |

## Production Deployment

### Recommended Production Configuration

```toml
[node]
label = "cameo-prod-01"
zone = "us-east-1a"

[network.http]
port = 9480
bind_address = "0.0.0.0"
request_timeout_secs = 60
max_body_size_mb = 50
cors_allowed_origins = []  # Disable in production for security

[storage]
data_paths = ["/data/cameodb"]  # Dedicated data volume
disk_usage_threshold_percent = 85
wal_sync = true  # Enable for durability
wal_segment_size_mb = 128
default_batch_size = 1000
max_shards_per_node = 20

[search]
indexer_memory_min_mb = 64
indexer_memory_max_mb = 512
total_memory_limit_mb = 2048
memory_pressure_threshold_percent = 80
search_threads = 8
default_search_limit = 10

[network.cluster]
enabled = true
cluster_name = "cameodb-production"
seed_nodes = ["10.0.1.5:9580", "10.0.1.6:9580"]
```

### System Requirements

| Component | Minimum | Recommended | High-Performance |
|-----------|---------|-------------|------------------|
| **CPU** | 2 cores | 4-8 cores | 16+ cores |
| **RAM** | 2GB | 8GB | 32GB+ |
| **Storage** | 10GB SSD | 100GB NVMe | Multiple NVMe drives |
| **Network** | 100Mbps | 1Gbps | 10Gbps+ |

### Monitoring

Monitor these key metrics:

- **Memory Usage**: Stay below `memory_pressure_threshold_percent`
- **Disk Usage**: Watch `disk_usage_threshold_percent`
- **Search Latency**: Monitor query response times
- **Write Throughput**: Track documents/second ingestion

## Troubleshooting

### Common Issues

#### 1. Memory Errors

**Error**: "Memory pressure threshold exceeded"

**Solution**:
```toml
[search]
total_memory_limit_mb = 4096  # Increase limit
memory_pressure_threshold_percent = 90  # Allow higher usage
```

#### 2. Disk Space Issues

**Error**: "Disk usage threshold exceeded"

**Solution**:
```toml
[storage]
disk_usage_threshold_percent = 95  # Allow more disk usage
# Or add more data paths
data_paths = ["/data1/cameodb", "/data2/cameodb"]
```

#### 3. Configuration Validation

```bash
# Validate configuration syntax
./scripts/setup/config-manager.sh validate cameodb.toml

# Test configuration loading
cargo run --release --bin cameodb  # Should start without errors
```

#### 4. Performance Issues

**Slow Writes**:
- Increase `indexer_memory_max_mb`
- Disable `wal_sync` (reduces durability)
- Use faster storage (NVMe)

**Slow Searches**:
- Increase `search_threads`
- Add more RAM for caching

### Debug Configuration Loading

Set environment variable to see configuration details:

```bash
RUST_LOG=debug cargo run --release --bin cameodb
```

## Configuration Templates

### Development

```bash
./scripts/setup/config-manager.sh minimal
```

### Production

```bash
./scripts/setup/config-manager.sh generate
# Edit data_paths, memory limits, and security settings
```

### High-Performance

```bash
./scripts/setup/config-manager.sh performance
# Review durability trade-offs
```

### Multi-Disk

```bash
./scripts/setup/config-manager.sh multi-disk
# Customize mount points
```

---

For more configuration examples and advanced scenarios, see the [scripts/setup/config-manager.sh](../scripts/setup/config-manager.sh) tool.
