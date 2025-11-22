# CameoDB Scripts and Utilities

This directory contains scripts and utilities for CameoDB development, testing, and operations. All scripts follow database industry best practices and provide comprehensive tooling for the entire development lifecycle.

## Quick Start

For a quick overview of all available scripts and project status:

```bash
./scripts/dev-info.sh
```

## Directory Structure & Scripts

### 🛠️ `setup/` - Development Environment Setup

#### `install-deps.sh`
**Purpose**: Automated setup of CameoDB development environment
- **Features**: 
  - Cross-platform support (macOS/Linux)
  - Auto-detects and installs missing dependencies (curl, jq)  
  - Verifies Rust toolchain and project compilation
  - Creates necessary data directories
- **Usage**: `./scripts/setup/install-deps.sh`
- **Audience**: New developers, CI/CD systems

#### `init-cluster.sh [port]`
**Purpose**: Initialize and start a development cluster with sample data
- **Features**:
  - Starts CameoDB server on specified port (default: 9480)
  - Adds 5 sample documents for testing
  - Validates all API endpoints (search, write, stream, health)
  - Interactive mode - keeps server running until Ctrl+C
- **Usage**: 
  ```bash
  ./scripts/setup/init-cluster.sh        # Default port 9480
  ./scripts/setup/init-cluster.sh 8080   # Custom port
  ```
- **Audience**: Developers, demo environments

### 🧪 `testing/` - Testing and Validation

#### `test-api.sh`
**Purpose**: Comprehensive API endpoint testing
- **Features**:
  - Tests all HTTP endpoints (health, search, write, stream)
  - JSON response validation
  - Automatic server startup and cleanup
  - NDJSON streaming test with timeout
- **Usage**: `./scripts/testing/test-api.sh`
- **Requirements**: Server must be running or script will start it
- **Audience**: Developers, QA, CI/CD pipelines

#### `load-test.sh [port] [users] [requests_per_user]`
**Purpose**: Performance and load testing with detailed metrics
- **Features**:
  - Concurrent write and search load testing
  - Detailed performance metrics (avg/min/max/95th percentile)
  - Configurable user count and request volume
  - Response time analysis and error tracking
- **Usage**:
  ```bash
  ./scripts/testing/load-test.sh                    # Default: 10 users, 50 requests each
  ./scripts/testing/load-test.sh 9480 20 100        # 20 users, 100 requests each
  ```
- **Output**: Creates test data in 'loadtest' index
- **Audience**: Performance engineers, DevOps

### 📊 `data/` - Data Management

#### `sample-data.sh [port] [index] [count]`
**Purpose**: Generate realistic sample data for development and testing
- **Features**:
  - Configurable document count (default: 100)
  - Realistic document structure with categories, topics, tags
  - Progress tracking and error reporting
  - Automatic search validation after data load
- **Usage**:
  ```bash
  ./scripts/data/sample-data.sh                     # 100 docs in 'sample' index
  ./scripts/data/sample-data.sh 9480 mydata 500     # 500 docs in 'mydata' index
  ```
- **Data Types**: Technology, science, business, education, entertainment, sports, health, travel
- **Audience**: Developers, QA, demo environments

### 🔧 `ops/` - Operations and Monitoring

#### `health-check.sh [port] [timeout]`
**Purpose**: Comprehensive health monitoring and diagnostics
- **Features**:
  - Server connectivity and response time testing
  - API endpoint validation (search, write, stream, health)
  - Performance metrics and memory usage monitoring
  - Colored output with clear status indicators
  - Overall health assessment with exit codes
- **Usage**:
  ```bash
  ./scripts/ops/health-check.sh         # Default port 9480, 10s timeout
  ./scripts/ops/health-check.sh 8080 5  # Port 8080, 5s timeout
  ```
- **Exit Codes**: 0 = healthy, 1 = degraded
- **Audience**: DevOps, monitoring systems, production operations

## Utility Scripts

### `dev-info.sh`
**Purpose**: Quick project overview and script documentation
- **Features**:
  - Lists all available scripts with descriptions
  - Shows current project build status
  - Displays server running status  
  - Quick start guide and documentation links
- **Usage**: `./scripts/dev-info.sh`
- **Audience**: All developers, new contributors

## Usage Guidelines

### Running Scripts
All scripts must be run from the **workspace root directory**:

```bash
# ✅ Correct - from workspace root
./scripts/testing/test-api.sh

# ❌ Wrong - from scripts directory
cd scripts && ./testing/test-api.sh
```

### Common Workflows

#### New Developer Setup
```bash
./scripts/setup/install-deps.sh     # Install dependencies
cargo build --release               # Build project
./scripts/setup/init-cluster.sh     # Start with sample data
./scripts/testing/test-api.sh       # Validate installation
```

#### Development Testing
```bash
./scripts/data/sample-data.sh       # Load test data
./scripts/testing/load-test.sh      # Performance testing
./scripts/ops/health-check.sh       # System health
```

#### CI/CD Integration
```bash
./scripts/setup/install-deps.sh     # Environment setup
cargo test --workspace              # Unit tests
./scripts/testing/test-api.sh       # Integration tests
./scripts/ops/health-check.sh       # Health validation
```

## Configuration & Customization

### Default Settings
- **Server Port**: 9480
- **Health Timeout**: 10 seconds
- **Load Test**: 10 users, 50 requests each
- **Sample Data**: 100 documents

### Environment Variables
Scripts respect these environment variables when available:
- `CAMEODB_PORT`: Default server port
- `CAMEODB_HOST`: Default server host (default: localhost)

### Data Directories
- **Production Data**: `./cameodb-data/` (git-ignored, created at runtime)
- **Test Data**: `/tmp/cameodb_tests/` (temporary, auto-cleanup with UUID isolation)

## Contributing

### Adding New Scripts

1. **Placement**: Choose appropriate subdirectory based on purpose
2. **Naming**: Use kebab-case (`my-script.sh`)
3. **Permissions**: Make executable (`chmod +x`)
4. **Structure**: Follow existing patterns:
   ```bash
   #!/bin/bash
   set -e  # Exit on error
   
   # Configuration with defaults
   DEFAULT_PORT=9480
   PORT=${1:-$DEFAULT_PORT}
   
   # Clear documentation and help
   # Main functionality
   # Error handling and cleanup
   ```

5. **Documentation**: Update this README with script details
6. **Testing**: Test from workspace root directory

### Best Practices
- **Error Handling**: Use `set -e` and proper cleanup
- **User Feedback**: Provide clear status messages with colors/emojis
- **Configuration**: Support command-line parameters with sensible defaults  
- **Cross-Platform**: Support both macOS and Linux when possible
- **Self-Documenting**: Include usage examples in script headers

## Requirements

### System Dependencies
- **Bash**: Version 4.0+ (most scripts)
- **curl**: HTTP client for API interactions
- **jq**: JSON processing and validation
- **timeout**: Command execution limits (GNU coreutils)

### CameoDB Dependencies
- **Rust Toolchain**: 1.70+ with Cargo
- **CameoDB Project**: Must be built (`cargo build --release`)

### Development Tools (Optional)
- **git**: Version control
- **tree**: Directory structure visualization
- **htop/ps**: Process monitoring

## Troubleshooting

### Common Issues

**"Command not found: jq"**
- Run `./scripts/setup/install-deps.sh` to install missing dependencies

**"Server not running on port 9480"**
- Start server: `cargo run --release --bin server`
- Or use init script: `./scripts/setup/init-cluster.sh`

**"Permission denied"**
- Make script executable: `chmod +x scripts/path/to/script.sh`

**"Project build failed"**
- Verify Rust installation: `cargo --version`
- Check dependencies: `cargo check --workspace`

### Getting Help
- Run `./scripts/dev-info.sh` for project overview
- Check individual script headers for usage examples
- See `./docs/` directory for detailed project documentation
- Review `./ARCHITECTURE.md` for system design information
