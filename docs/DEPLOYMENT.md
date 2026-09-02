## 🐳 Docker Deployment

CameoDB provides configurations for both single-node and multi-node cluster deployments using Docker.

### Build Local Docker Image (Development)

`--no-push` builds every platform the push would publish — it is the rehearsal for a release,
so checking only one architecture would let the other fail for the first time mid-push — and
leaves a runnable image in the local store:

```bash
# Build amd64 + arm64, load locally, publish nothing
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
./scripts/build/docker-push.sh 0.3.0

# Build only (no push) for testing
./scripts/build/docker-push.sh 0.3.0 --no-push
```

**Prerequisites:**
- Docker Desktop with buildx enabled
- Logged in to DockerHub: `docker login`

### Build Distribution Packages (Binary + DEB/RPM)

Build optimized binaries and packages using Docker:

```bash
# Build for amd64 (default)
./scripts/build/build-packages.sh

# Build for arm64
./scripts/build/build-packages.sh arm64

# Build for both architectures
./scripts/build/build-packages.sh amd64 arm64
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
./scripts/security/generate-sbom.sh 0.3.0               # specific version

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

## 🔐 Securing a Deployment

Authentication is off by default, and a `local` node is allowed to stay that way. Anything
reachable from another host should not.

### Before you start the node

```bash
cameodb check-config -c /etc/cameodb/cameodb.toml
```

One line per rule, non-zero exit on any failure — run it in the deploy step, before the
service starts. A node whose config contradicts its `[node] profile` refuses to boot rather
than starting in a posture you did not ask for; `external` will not start without TLS,
authentication, and `/_admin/*` disabled. See
[Configuration → Security and Posture](CONFIGURATION.md#security-and-posture).

### Key material on disk

```bash
sudo install -d -o cameodb -g cameodb -m 700 /etc/cameodb/keys

# Writes the digest for the server, and the key for whoever will use it.
# Both 0600; neither is ever overwritten.
sudo -u cameodb cameodb keygen --role writer --label ingest \
  --hash-out /etc/cameodb/keys/ingest \
  --key-out /tmp/ingest.key

# Hand /tmp/ingest.key to its owner over a channel you trust, then remove it.
```

```toml
[security]
enabled = true

[[security.api_keys]]
key_hash_file = "/etc/cameodb/keys/ingest"
role = "writer"
label = "ingest"
```

| File | Mode | Owner | Why |
|------|------|-------|-----|
| `/etc/cameodb/keys/*` | `600` | the service user | Read at startup. A digest is not secret, but a **writable** one lets anyone mint themselves a role — CameoDB warns if group or others can write it |
| `/etc/cameodb/cameodb.toml` | `640` | root:cameodb | Holds digests and paths, never a key |
| A client's key file | `600` | the human or service using it | This one *is* the credential. The client warns if anyone else can read it |

Only digests ever reach the server. A key exists in the clear exactly twice: on the terminal
that minted it, and wherever its holder stores it.

### Configuring from the environment

For a node whose config comes from a secret manager rather than a file, one key can be
supplied entirely through the environment:

```ini
# /etc/cameodb/cameodb.env  (0640 root:cameodb)
CAMEODB_SECURITY_ENABLED=true
CAMEODB_API_KEY_HASH=sha256:1db44a37dcf74ef70439a8887862839803d9686a41fe7c9d75d8fdfa0c72cdb1
CAMEODB_API_KEY_ROLE=admin
```

```ini
# systemctl edit cameodb
[Service]
EnvironmentFile=/etc/cameodb/cameodb.env
```

There is no `CAMEODB_API_KEY` on the server side on purpose: a node never needs a key in the
clear. That variable belongs to the client.

### Rotation

Keys are read once at startup — there is no hot reload, and no way to revoke a key without a
restart. Plan for two restarts:

1. `keygen` the replacement and add it as a second `[[security.api_keys]]` entry
2. Restart — the node now accepts both keys
3. Move clients across
4. Remove the old entry and restart again

Rolling this across a cluster is a rolling restart, one node at a time; a node with the new
key configured still accepts the old one until step 4.

### The audit trail

Off by default. Turned on, it answers who read which index and how much each key wrote —
which is the question an incident asks and the one nothing else here can answer.

```toml
[security.audit]
enabled = true
file = "/var/log/cameodb/audit.jsonl"
max_file_bytes = 104857600      # rotate past 100 MiB
max_files = 5                   # keep .1 … .5, oldest deleted
```

Operational points:

- **The file is written by the node**, which creates the parent directory if needed and
  rotates in place. It needs write access to that directory as the user the service runs as
  — for the packaged systemd unit that is `cameodb`, so `install -d -o cameodb -g cameodb
  /var/log/cameodb` before first start.
- **Do not point `logrotate` at it.** Rotation is internal, by size; an external rotator that
  renames the active file leaves the node writing to a file nobody can find until restart.
- **In a container, put it on a mounted volume.** A path inside the container's own
  filesystem is exactly as durable as the container.
- **Budget for volume by shape, not by request rate.** Reads and admin actions cost a line
  each; writes are counted per key and index per `rollup_secs`, so ingest volume barely moves
  the file. A read-heavy deployment is the one to size for.
- **Without `file`, the trail is memory only** — `buffer_capacity` records, readable at
  `GET /_admin/audit`, gone on restart.
- **Watch `dropped`.** `GET /_admin/audit` reports a running total of records lost to a full
  writer queue; non-zero means the trail has gaps, and a `gap` record marks each one. It
  should be zero.

Every record is also emitted on the `tracing` target `cameodb::audit`, so a deployment
already shipping logs can route the trail with `RUST_LOG=warn,cameodb::audit=info` and skip
the file entirely.

Settings reference: [CONFIGURATION.md](CONFIGURATION.md). Record shapes and the endpoint:
[API_REFERENCE.md](API_REFERENCE.md).

### What this does not cover

Worth stating plainly before you rely on it:

- **Cluster peers are trusted by the PSK, not by API keys.** Peer-to-peer traffic is
  kameo-over-libp2p with Noise, and `allowed_indexes` is not a defense against a compromised
  cluster member — it is enforced at the HTTP/MCP ingress, where identity exists.
- **The cluster PSK has no rotation path.** Changing it means stopping every node.
- **No lockout or throttle on failed authentication.** Against a 256-bit key it buys nothing
  and is itself a denial-of-service lever. Refusals are counted and logged instead.
- **The audit trail is not tamper-evident.** It is a file the node writes; nothing signs or
  chains the records. Ship it off the node if you need it to survive the node.
- **An MCP client authenticates with an HTTP header or not at all.** No OAuth flow, no
  per-client credential issuance.

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
# On a node with authentication enabled these need a key holding node-admin,
# which in practice means an admin key: -H "Authorization: Bearer $CAMEODB_API_KEY"

# Get process + jemalloc memory statistics
curl -s http://localhost:9480/_admin/memory

# Trigger decay-based memory purge (respects dirty_decay_ms)
curl -s -X POST http://localhost:9480/_admin/memory/purge

# Trigger aggressive purge (bypasses decay timers)
curl -s -X POST 'http://localhost:9480/_admin/memory/purge?force=true'
```

Or through the client, which reads `CAMEODB_API_KEY` / `--api-key-file` itself:

```bash
cameodb client admin memory stats
cameodb client admin memory purge --force
```

These endpoints are useful after large bulk ingestions to verify memory has been returned to the OS, or during memory pressure investigations.

For more details, see the [Docker README](../docker/README.md), which includes the latest swarm environment variables and configuration guidance.

