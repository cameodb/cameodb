# CameoDB Configuration Guide

This guide covers comprehensive configuration management for CameoDB, including HTTP server settings, storage paths, and Tantivy search engine tuning.

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
cargo run --release --bin server generate-config > cameodb.toml

# Or use the configuration manager
./scripts/setup/config-manager.sh generate
```

### 2. Basic Configuration

Edit `cameodb.toml`:

```toml
[server.http]
port = 9480
host = "0.0.0.0"

[storage]
data_paths = ["./data/cameodb"]

[search]
writer_memory_min_mb = 16
writer_memory_max_mb = 256
total_memory_limit_mb = 1024
```

### 3. Start Server

```bash
cargo run --release --bin server
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

### HTTP Server Configuration

```toml
[server.http]
# Port for HTTP server (default: 9480)
port = 9480

# Host to bind HTTP server (default: "0.0.0.0")
host = "0.0.0.0"

# Request timeout in seconds (default: 30)
request_timeout_secs = 30

# Maximum request body size in MB (default: 20)
max_body_size_mb = 20

# Enable CORS support (default: true)
cors_enabled = true
```

### Node Configuration

```toml
[server.node]
# Maximum number of shards this node can host (default: 8)
max_shards = 8

# Default writer memory per shard in MB (default: 32)
writer_memory_default_mb = 32
```

### Storage Configuration

```toml
[storage]
# List of data directories for multi-disk configurations
data_paths = [
  "./data/cameodb",
  "/mnt/disk1/cameodb",
  "/mnt/disk2/cameodb"
]

# Disk usage threshold before rejecting new data (0.0-1.0, default: 0.9)
disk_usage_threshold = 0.9

# Enable WAL fsync for durability (default: true)
wal_sync = true

# WAL segment size in MB (default: 64)
wal_segment_size_mb = 64
```

### Tantivy Search Configuration

```toml

# Storage Configuration (colon-separated paths)
export CAMEODB_DATA_PATHS="./data/cameodb:/mnt/disk1/cameodb"

# Search Configuration
export CAMEODB_WRITER_MEMORY_MIN_MB=32
export CAMEODB_WRITER_MEMORY_MAX_MB=512
export CAMEODB_TOTAL_MEMORY_LIMIT_MB=2048
export CAMEODB_MEMORY_PRESSURE_THRESHOLD=0.75
```

### Docker/Kubernetes Example

```yaml
env:
  - name: CAMEODB_HTTP_PORT
    value: "9480"
  - name: CAMEODB_DATA_PATHS
    value: "/data/cameodb"
  - name: CAMEODB_TOTAL_MEMORY_LIMIT_MB
    value: "4096"
```

## Multi-Disk Setup

For high-throughput deployments with multiple storage devices:

### Generate Multi-Disk Configuration

```bash
./scripts/setup/config-manager.sh multi-disk
```

### Manual Configuration

```toml
[storage]
data_paths = [
  "/mnt/nvme1/cameodb",
  "/mnt/nvme2/cameodb", 
  "/mnt/ssd1/cameodb",
  "/mnt/ssd2/cameodb"
]
disk_usage_threshold = 0.85
wal_segment_size_mb = 128

[server.node]
max_shards = 50
writer_memory_default_mb = 100

[search]
writer_memory_max_mb = 512
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
writer_memory_min_mb = 64
writer_memory_max_mb = 1024
total_memory_limit_mb = 8192

# Aggressive memory usage
memory_pressure_threshold = 0.9
```

#### Storage Optimization

```toml
[storage]
# Disable fsync for maximum write speed (less durable)
wal_sync = false

# Large WAL segments reduce overhead
wal_segment_size_mb = 256

# Use more disk space
disk_usage_threshold = 0.95
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
| `writer_memory_max_mb = 1024` | ⬆️ High | ➡️ Same | Uses more RAM |
| `memory_pressure_threshold = 0.9` | ⬆️ High | ➡️ Same | Higher memory usage |

## Production Deployment

### Recommended Production Configuration

```toml
[server.http]
port = 9480
host = "0.0.0.0"
request_timeout_secs = 60
max_body_size_mb = 50
cors_enabled = false  # Disable in production for security

[server.node]
max_shards = 20
writer_memory_default_mb = 100

[storage]
data_paths = ["/data/cameodb"]  # Dedicated data volume
disk_usage_threshold = 0.85
wal_sync = true  # Enable for durability
wal_segment_size_mb = 128
default_batch_size = 1000

[search]
writer_memory_min_mb = 16
writer_memory_max_mb = 256
total_memory_limit_mb = 2048
memory_pressure_threshold = 0.8
search_threads = 8

[cluster]
# Cluster configuration
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

- **Memory Usage**: Stay below `memory_pressure_threshold`
- **Disk Usage**: Watch `disk_usage_threshold`
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
memory_pressure_threshold = 0.9  # Allow higher usage
```

#### 2. Disk Space Issues

**Error**: "Disk usage threshold exceeded"

**Solution**:
```toml
[storage]
disk_usage_threshold = 0.95  # Allow more disk usage
# Or add more data paths
data_paths = ["/data1/cameodb", "/data2/cameodb"]
```

#### 3. Configuration Validation

```bash
# Validate configuration syntax
./scripts/setup/config-manager.sh validate cameodb.toml

# Test configuration loading
cargo run --release --bin server  # Should start without errors
```

#### 4. Performance Issues

**Slow Writes**:
- Increase `writer_memory_max_mb`
- Disable `wal_sync` (reduces durability)
- Use faster storage (NVMe)

**Slow Searches**:
- Increase `search_threads`
- Add more RAM for caching

### Debug Configuration Loading

Set environment variable to see configuration details:

```bash
RUST_LOG=debug cargo run --release --bin server
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
