#!/bin/bash

# CameoDB Configuration Manager
# Tool for generating, validating, and managing CameoDB configurations

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

echo "🔧 CameoDB Configuration Manager"
echo

# Function to show help
show_help() {
    cat << EOF
Usage: $0 <command> [options]

Commands:
  generate [FILE]     Generate sample configuration (default: cameodb.toml)
  validate [FILE]     Validate configuration file
  env-template        Generate environment variable template
  multi-disk          Generate multi-disk configuration template
  performance         Generate high-performance configuration template
  minimal             Generate minimal configuration template

Options:
  -h, --help         Show this help message

Examples:
  $0 generate                          # Generate default config
  $0 generate config/production.toml   # Generate config to specific file
  $0 validate cameodb.toml            # Validate existing config
  $0 env-template                     # Show environment variables
  $0 multi-disk                       # Multi-disk configuration
  $0 performance                      # High-performance setup
EOF
}

# Function to generate sample configuration
generate_config() {
    local output_file="${1:-cameodb.toml}"
    
    echo "📝 Generating sample configuration: $output_file"
    
    # Use server binary to generate config
    cd "$PROJECT_ROOT"
    if [[ ! -f "target/release/server" ]]; then
        echo "Building server binary..."
        cargo build --release --bin server
    fi
    
    ./target/release/server generate-config > "$output_file"
    
    echo "✅ Configuration generated: $output_file"
    echo "   Edit this file to customize your CameoDB setup"
    echo "   Documentation: https://docs.cameodb.io/config"
}

# Function to validate configuration
validate_config() {
    local config_file="${1:-cameodb.toml}"
    
    if [[ ! -f "$config_file" ]]; then
        echo "❌ Configuration file not found: $config_file"
        exit 1
    fi
    
    echo "🔍 Validating configuration: $config_file"
    
    cd "$PROJECT_ROOT"
    
    # Try to load the configuration by starting server in dry-run mode
    # Since we don't have a dry-run mode yet, we'll do a basic TOML validation
    if command -v toml &> /dev/null; then
        if toml validate "$config_file" 2>/dev/null; then
            echo "✅ Configuration syntax is valid"
        else
            echo "❌ Configuration syntax errors found"
            exit 1
        fi
    else
        echo "⚠️  TOML validator not found, skipping syntax check"
        echo "   Install with: cargo install toml-cli"
    fi
    
    echo "✅ Configuration appears valid"
    echo "   To test runtime validation, run: cargo run --release --bin server"
}

# Function to generate environment variable template
generate_env_template() {
    echo "📄 Environment Variable Template"
    echo "Add these to your .env file or export them:"
    echo
    
    cat << EOF
# HTTP Server Configuration
export CAMEODB_HTTP_PORT=9480
export CAMEODB_HTTP_HOST="0.0.0.0"

# Storage Configuration (colon-separated paths for multi-disk)
export CAMEODB_DATA_PATHS="./cameodb-data:/mnt/disk1/cameodb:/mnt/disk2/cameodb"

# Tantivy Memory Configuration (MB)
export CAMEODB_WRITER_MEMORY_MIN_MB=16
export CAMEODB_WRITER_MEMORY_MAX_MB=256
export CAMEODB_TOTAL_MEMORY_LIMIT_MB=1024
export CAMEODB_MEMORY_PRESSURE_THRESHOLD=0.8

# Example usage:
# source .env && cargo run --release --bin server
EOF
}

# Function to generate multi-disk configuration
generate_multi_disk_config() {
    local output_file="${1:-cameodb-multi-disk.toml}"
    
    echo "💾 Generating multi-disk configuration: $output_file"
    
    cat > "$output_file" << 'EOF'
# CameoDB Multi-Disk Configuration
# Optimized for systems with multiple storage devices

[server.http]
port = 9480
host = "0.0.0.0"
request_timeout_secs = 60  # Longer timeout for large operations
max_body_size_mb = 50      # Larger request size
cors_enabled = true

[server.node]
max_shards = 50            # More shards for multi-disk setup
writer_memory_default_mb = 100

[storage]
# Multiple mount points for distributed data
data_paths = [
  "/mnt/nvme1/cameodb",
  "/mnt/nvme2/cameodb",
  "/mnt/ssd1/cameodb",
  "/mnt/ssd2/cameodb"
]
disk_usage_threshold = 0.85  # Conservative threshold
wal_sync = true
wal_segment_size_mb = 128    # Larger WAL segments

[search]
writer_memory_min_mb = 32
writer_memory_max_mb = 512   # More memory per shard
total_memory_limit_mb = 4096 # 4GB total limit
memory_pressure_threshold = 0.75
search_threads = 16          # More threads for parallel search
index_refresh_interval_secs = 3
EOF
    
    echo "✅ Multi-disk configuration generated: $output_file"
    echo "   Customize the data_paths for your specific mount points"
}

# Function to generate performance configuration
generate_performance_config() {
    local output_file="${1:-cameodb-performance.toml}"
    
    echo "🚀 Generating high-performance configuration: $output_file"
    
    cat > "$output_file" << 'EOF'
# CameoDB High-Performance Configuration
# Optimized for maximum throughput and low latency

[server.http]
port = 9480
host = "0.0.0.0"
request_timeout_secs = 15    # Aggressive timeout
max_body_size_mb = 100       # Large request size
cors_enabled = false         # Disable CORS for performance

[server.node]
max_shards = 100             # Many shards for parallelism
writer_memory_default_mb = 200 # More memory per shard

[storage]
data_paths = ["./cameodb-data"]
disk_usage_threshold = 0.95  # Use more disk space
wal_sync = false             # Disable fsync for speed (less durable)
wal_segment_size_mb = 256    # Large WAL segments

[search]
writer_memory_min_mb = 64    # Higher baseline memory
writer_memory_max_mb = 1024  # Maximum memory allocation
total_memory_limit_mb = 8192 # 8GB total limit
memory_pressure_threshold = 0.9  # High memory usage
search_threads = 32          # Maximum parallelism
index_refresh_interval_secs = 1   # Frequent refresh
EOF
    
    echo "✅ High-performance configuration generated: $output_file"
    echo "   ⚠️  Note: This config prioritizes speed over durability"
}

# Function to generate minimal configuration
generate_minimal_config() {
    local output_file="${1:-cameodb-minimal.toml}"
    
    echo "🔍 Generating minimal configuration: $output_file"
    
    cat > "$output_file" << 'EOF'
# CameoDB Minimal Configuration
# Lightweight setup for development and testing

[server.http]
port = 9480

[server.node]
max_shards = 5
writer_memory_default_mb = 25

[storage]
data_paths = ["./cameodb-data"]

[search]
writer_memory_min_mb = 16
writer_memory_max_mb = 64
total_memory_limit_mb = 256
search_threads = 2
EOF
    
    echo "✅ Minimal configuration generated: $output_file"
    echo "   Perfect for development and resource-constrained environments"
}

# Main command processing
case "${1:-help}" in
    generate)
        generate_config "$2"
        ;;
    validate)
        validate_config "$2"
        ;;
    env-template)
        generate_env_template
        ;;
    multi-disk)
        generate_multi_disk_config "$2"
        ;;
    performance)
        generate_performance_config "$2"
        ;;
    minimal)
        generate_minimal_config "$2"
        ;;
    help|--help|-h)
        show_help
        ;;
    *)
        echo "❌ Unknown command: $1"
        echo
        show_help
        exit 1
        ;;
esac
