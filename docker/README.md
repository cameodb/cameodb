# 🐳 CameoDB Docker Configuration

This directory contains configurations for running CameoDB using Docker for both single-node and multi-node cluster deployments.

## What's Included

- **`Dockerfile`**: A multi-stage `Dockerfile` that builds a minimal, secure production image using a non-root user. It uses Docker secrets to handle build-time CA certificates without leaking them into the final image.
- **`docker-compose.yml`**: A simple configuration for running a single, standalone CameoDB node. Ideal for local development and testing.
- **`docker-compose-cluster.yml`**: A 3-node distributed cluster configuration with an NGINX load balancer. Demonstrates a production-like setup.

## Deployment Scenarios

### 1. Single-Node Deployment

This is the simplest way to run CameoDB. It uses the `docker-compose.yml` file.

**Setup & Run:**
```bash
# Ensure the data directory exists in the project root
mkdir -p ../data/cameodb

# From the /docker directory, start the container
docker-compose up -d
```

- **Access Point**: `http://localhost:9480`
- **Data Persistence**: Data is stored in the project's `data/cameodb` directory.

### 2. Multi-Node Cluster Deployment

This setup runs a 3-node cluster and is defined in `docker-compose-cluster.yml`. Swarm discovery relies on Kademlia with static bootstrap peers.

**Setup & Run:**
```bash
# Create data directories for each node
mkdir -p ../data/cameodb/node{1,2,3}

# From the /docker directory, start the cluster
docker compose -f docker-compose-cluster.yml up -d
```

**The cluster pre-shared key.** `profile = "internal"` refuses to start a node with the cluster
enabled and no PSK: without one, anything that can reach `:9580` can join the swarm. The compose
file carries a throwaway default so the cluster comes up with no setup. Generate a real one for
anything that outlives the test — 64 hex characters, identical on every node:

```bash
CAMEODB_CLUSTER_PSK=$(openssl rand -hex 32) \
  docker compose -f docker-compose-cluster.yml up -d
```

There is no rotation path short of stopping every node.

- **Access Points**:
  - **Load Balanced**: `http://localhost:9480` (via NGINX)
  - **Node 1 (Direct)**: `http://localhost:9481`
  - **Node 2 (Direct)**: `http://localhost:9482`
  - **Node 3 (Direct)**: `http://localhost:9483`
- **Data Persistence**: Each node's data is stored in a separate subdirectory within `data/cameodb/`.
- **Swarm Configuration**: Update the `CAMEODB_CLUSTER_NAME`, `CAMEODB_CLUSTER_PORT`, `CAMEODB_SEED_NODES`, and `CAMEODB_CLUSTER_ENABLED` environment variables to reflect your deployment topology.
- **Body size**: the nodes accept 128 MB, derived from `limits.max_record_size_mb` (64) plus
  framing, and NGINX is set to match. Raise `CAMEODB_MAX_RECORD_SIZE_MB` to move both — and
  `max_concurrent_requests` × the body ceiling has to stay inside `total_memory_limit_mb`, which
  `check-config` will tell you if it does not.

## Security

The containers ship **unauthenticated**, with `profile = "internal"` — honest about a
published port being reachable from your network, and it is what makes `check-config` pass.
Every HTTP and MCP endpoint is open to whoever can reach the port, `/_admin/*` included.

Turning that on is one command and one stanza:

```bash
# Digest for the node, key for whoever will use it. Both files are created 0600.
mkdir -p config/keys
cameodb keygen --role admin --label ops   --hash-out config/keys/ops   --key-out ~/.cameodb/ops.key

# The container runs as uid 65532 and reads the digest at startup
chown 65532:65532 config/keys/ops
```

Then uncomment the `[security]` block in [`cameodb-docker.toml`](cameodb-docker.toml) and the
key volume in [`docker-compose.yml`](docker-compose.yml), and restart. Verify before you do:

```bash
docker compose run --rm cameodb check-config --config /etc/cameodb/cameodb.toml
cameodb client --api-key-file ~/.cameodb/ops.key list indexes
```

Only the SHA-256 digest is ever mounted into the container. A single key can also be supplied
entirely through the environment with `CAMEODB_SECURITY_ENABLED`, `CAMEODB_API_KEY_HASH` and
`CAMEODB_API_KEY_ROLE` — there is deliberately no server-side `CAMEODB_API_KEY`, since a node
needs digests and never keys.

`/_cluster/health` stays public either way, so health probes and load balancers need no
credential. For the cluster compose file, mount the same digest into every node: a key is
accepted by the node it is configured on, and NGINX will route you to any of them.

See [Configuration → Security and Posture](../docs/CONFIGURATION.md#security-and-posture) and
[Deployment → Securing a Deployment](../docs/DEPLOYMENT.md#-securing-a-deployment).

## Building Docker Images

### Quick Build (Local Development)

Use the `docker-push.sh` script for easy building:

```bash
# Build and load local image (single platform, for testing)
../scripts/build/docker-push.sh --no-push

# Test the local build
docker run --rm cameodb:latest --version
```

### Build and Push to DockerHub (Multi-Platform)

Build for multiple platforms (amd64 + arm64) and push to DockerHub:

```bash
# Build + push with latest tag
../scripts/build/docker-push.sh

# Build + push with specific version
../scripts/build/docker-push.sh 0.3.0

# Build only (no push) for testing multi-platform builds
../scripts/build/docker-push.sh 0.3.0 --no-push
```

**Prerequisites:**
- Docker Desktop with buildx enabled
- Logged in to DockerHub:
  ```bash
  docker login -u <user_name_on_github>
  # Enter your DockerHub username and password/personal access token when prompted
  ```

**Behind Corporate Firewall:**
The build needs your CA certificate or `cargo fetch` fails on every crates.io request. Place
it at the default path, or point `CAMEODB_CA_CERT` at it:
```bash
/var/tmp/buildkit-ca/corporate-ca.crt        # picked up automatically
CAMEODB_CA_CERT=/path/to/ca.crt ./scripts/build/docker-push.sh --no-push
```
It is passed to the build as the `corporate-ca` secret — mounted into the build stage only,
never into the runtime image — and skipped when the file is absent or empty.

### Manual Build (Advanced)

If you prefer manual control over the build process:

```bash
# Create buildx builder
docker buildx create --name cameo-builder --use \
  --driver docker-container

# Build and push multi-platform
docker buildx build \
  --builder cameo-builder \
  --platform linux/amd64,linux/arm64 \
  -t goranc/cameodb:latest \
  -t goranc/cameodb:0.3.0 \
  --push \
  ..
```

## Common Commands

```bash
# Check status of containers (use -f for cluster file)
docker-compose [-f docker-compose-cluster.yml] ps

# View logs
docker-compose [-f docker-compose-cluster.yml] logs -f

# Stop and remove containers
docker-compose [-f docker-compose-cluster.yml] down
```

## Security & Best Practices

- **Non-Root User**: The container runs as a `nonroot` user (`65532:65532`) for enhanced security.
- **Multi-Stage Build**: The `Dockerfile` uses a builder stage for compilation and a minimal `distroless` image for runtime, reducing the attack surface.
- **Secret Management**: Build-time secrets (like CA certificates) are mounted using `--mount=type=secret` and are not persisted in the final image layers.
