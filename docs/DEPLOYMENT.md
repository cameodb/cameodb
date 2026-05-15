## 🐳 Docker Deployment

CameoDB provides configurations for both single-node and multi-node cluster deployments using Docker.

### Build Local Docker Image (Development)

Build a single-platform image for local testing:

```bash
# Build for current platform (loads into Docker Desktop)
./scripts/build/docker-push.sh --no-push

# Or build manually
docker buildx build -t cameodb:local --load .
```

### Build and Push to DockerHub

Build multi-platform images (amd64 + arm64) and push to DockerHub:

```bash
# Build + push with latest tag
./scripts/build/docker-push.sh

# Build + push with version tag
./scripts/build/docker-push.sh 0.2.2

# Build only (no push) for testing
./scripts/build/docker-push.sh 0.2.2 --no-push
```

**Prerequisites:**
- Docker Desktop with buildx enabled
- Logged in to DockerHub: `docker login`

### Build Distribution Packages (Binary + DEB/RPM)

Build optimized binaries and packages using Docker:

```bash
# Build for amd64 (default)
./scripts/build/build-dist.sh

# Build for arm64
./scripts/build/build-dist.sh arm64

# Build for both architectures
./scripts/build/build-dist.sh amd64 arm64
```

**Outputs:**
- Binary: `target/{triple}/release-docker/cameodb`
- DEB package: `cameodb_{version}_{arch}.deb`
- RPM package: `cameodb-{version}-1.{arch}.rpm`

### SBOM Generation (Software Bill of Materials)

CameoDB provides SBOM generation for supply chain security and compliance using [syft](https://github.com/anchore/syft). Both SPDX and CycloneDX formats are generated and published.

**Prerequisites:**
```bash
# Install syft 1.42.3+
brew install syft  # macOS
# Or download from: https://github.com/anchore/syft/releases
```

**Generate SBOMs (both formats):**

```bash
# From Docker image (default)
./scripts/security/generate-sbom.sh                    # latest tag
./scripts/security/generate-sbom.sh 0.2.2               # specific version

# From native binary (M1 Mac, Linux)
cargo build --release
./scripts/security/generate-sbom.sh --native

# From source code (most complete)
./scripts/security/generate-sbom.sh --source
```

**Outputs:**
- `cameodb.spdx.json` - SPDX 2.3 format (written to `scripts/security/`)
- `cameodb.cyclonedx.json` - CycloneDX 1.5 format (written to `scripts/security/`)

**Verify and Inspect SBOMs:**

```bash
# SPDX - uses 'packages' array
jq -r '.packages[].name' scripts/security/cameodb.spdx.json
jq '.packages | length' scripts/security/cameodb.spdx.json

# CycloneDX - uses 'components' array
jq -r '.components[].name' scripts/security/cameodb.cyclonedx.json
jq '.components | length' scripts/security/cameodb.cyclonedx.json

# Show tool/version info
jq '.creationInfo' scripts/security/cameodb.spdx.json
jq '.metadata.tools' scripts/security/cameodb.cyclonedx.json
```

**Manual Generation (single format):**

```bash
# SPDX only
syft goranc/cameodb:latest -o spdx-json --file cameodb.spdx.json

# CycloneDX only
syft goranc/cameodb:latest -o cyclonedx-json --file cameodb.cyclonedx.json

# From binary
syft target/aarch64-apple-darwin/release/cameodb \
  -o spdx-json --file cameodb.spdx.json
```

**Publish SBOMs:**

```bash
# Upload both formats from scripts/security/
scp scripts/security/cameodb.spdx.json scripts/security/cameodb.cyclonedx.json \
  user@dl.cameodb.com:/var/www/dl.cameodb.com/
```

**Available at:**
- https://dl.cameodb.com/cameodb.spdx.json
- https://dl.cameodb.com/cameodb.cyclonedx.json

### 1. Single-Node Deployment

Ideal for local development and testing. This uses the `docker/docker-compose.yml` file with the default `cameodb-docker.toml` configuration (with Kademlia discovery).

**Setup & Run:**
```bash
# 1. Ensure the data directory exists
mkdir -p data/cameodb

# 2. From the project root, start the container
docker-compose -f docker/docker-compose.yml up -d
```

- **Access Point**: `http://localhost:9480`
- **Data Persistence**: Data is stored in the project's `data/cameodb` directory.

### 2. Multi-Node Cluster Deployment

Runs a 3-node cluster with a load balancer. This uses the `docker/docker-compose-cluster.yml` file. The cluster relies on static bootstrap peers and the new swarm runtime (Kademlia discovery).

**Setup & Run:**
```bash
# 1. Create data directories for each node
mkdir -p data/cameodb/node{1,2,3}

# 2. From the project root, start the cluster
docker-compose -f docker/docker-compose-cluster.yml up -d
```

- **Access Points**:
  - **Load Balanced**: `http://localhost:9480` (via NGINX)
  - **Node 1 (Direct)**: `http://localhost:9481`
  - **Node 2 (Direct)**: `http://localhost:9482`
  - **Node 3 (Direct)**: `http://localhost:9483`
- **Data Persistence**: Each node's data is stored in a separate subdirectory within `data/cameodb/`.
- **Swarm Configuration**: `CAMEODB_CLUSTER_NAME`, `CAMEODB_CLUSTER_PORT`, `CAMEODB_SEED_NODES`, and `CAMEODB_CLUSTER_ENABLED` environment variables drive the Kademlia swarm. Update them per deployment needs.

### Custom Data Directory Setup

For production deployments, you may want to store CameoDB data on a separate disk or partition. Create a custom data directory with proper permissions:

```bash
# Create custom data directory (example: /data01/cameodb)
sudo mkdir -p /data01/cameodb

# Set ownership to cameodb user and group
sudo chown cameodb:cameodb /data01/cameodb

# Set secure permissions (read/write only for cameodb user)
sudo chmod 700 /data01/cameodb
```

After creating the custom directory, update the `data_paths` in your `cameodb-docker.toml` configuration file:

```toml
[storage]
data_paths = ["/data01/cameodb"]
```

Then restart the CameoDB service to apply the new configuration:

```bash
docker-compose -f docker/docker-compose.yml restart
```

### Common Docker Commands

```bash
# Check status (use -f for the cluster file)
docker-compose ps

# View logs
docker-compose logs -f

# Stop and remove containers
docker-compose down -v
```

## 🐧 Production Deployment with systemd

CameoDB ships with a systemd service file (`crates/server/cameodb.service`) configured for production workloads with jemalloc memory tuning.

### Jemalloc Memory Tuning

CameoDB uses `tikv-jemallocator` as its memory allocator on Linux. The service file sets `MALLOC_CONF` for optimal performance with pinned shard threads:

```ini
Environment=MALLOC_CONF=background_thread:true,percpu_arena:percpu,oversize_threshold:0,dirty_decay_ms:2000,muzzy_decay_ms:0
```

| Setting | Value | Purpose |
|---------|-------|---------|
| `background_thread:true` | — | Background purging doesn't block writer threads |
| `percpu_arena:percpu` | — | One arena per CPU core, optimal for pinned shard threads |
| `oversize_threshold:0` | — | All allocations share per-CPU arenas |
| `dirty_decay_ms:2000` | 2s | Dirty pages held for 2s before background purge (tuned for 8-32 parallel writers) |
| `muzzy_decay_ms:0` | immediate | Muzzy pages released immediately |

To override per-deployment, use `systemctl edit cameodb` rather than editing the packaged service file:

```bash
sudo systemctl edit cameodb
# Add:
[Service]
Environment=MALLOC_CONF=background_thread:true,percpu_arena:percpu,oversize_threshold:0,dirty_decay_ms:1000,muzzy_decay_ms=0
```

### Admin Memory Operations

CameoDB exposes memory management endpoints for runtime diagnostics and manual intervention:

```bash
# Get process + jemalloc memory statistics
curl -s http://localhost:9480/_admin/memory

# Trigger decay-based memory purge (respects dirty_decay_ms)
curl -s -X POST http://localhost:9480/_admin/memory/purge

# Trigger aggressive purge (bypasses decay timers)
curl -s -X POST 'http://localhost:9480/_admin/memory/purge?force=true'
```

These endpoints are useful after large bulk ingestions to verify memory has been returned to the OS, or during memory pressure investigations.

For more details, see the [Docker README](docker/README.md), which includes the latest swarm environment variables and configuration guidance.

