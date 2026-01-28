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
docker-compose -f docker-compose-cluster.yml up -d
```

- **Access Points**:
  - **Load Balanced**: `http://localhost:9480` (via NGINX)
  - **Node 1 (Direct)**: `http://localhost:9481`
  - **Node 2 (Direct)**: `http://localhost:9482`
  - **Node 3 (Direct)**: `http://localhost:9483`
- **Data Persistence**: Each node's data is stored in a separate subdirectory within `data/cameodb/`.
- **Swarm Configuration**: Update the `CAMEODB_CLUSTER_NAME`, `CAMEODB_CLUSTER_PORT`, `CAMEODB_SEED_NODES`, and `CAMEODB_CLUSTER_ENABLED` environment variables to reflect your deployment topology.

## Building Multi-Platform Images

To build and push multi-platform Docker images (amd64 and arm64) with custom CA certificates for corporate firewalls:

### Prerequisites

1. Ensure your corporate CA certificate is installed at:
```bash
/usr/local/share/ca-certificates/zscaler.crt
```

### Step 1: Create Custom BuildKit Image

Create a custom BuildKit image that trusts your CA certificate:

```bash
# Create build context directory
mkdir -p /tmp/buildkit-ca

# Copy certificate to build context
cp /usr/local/share/ca-certificates/zscaler.crt /tmp/buildkit-ca/

# Create custom BuildKit Dockerfile
cat > /tmp/buildkit-ca/Dockerfile.buildkit <<'EOF'
FROM moby/buildkit:latest
COPY zscaler.crt /usr/local/share/ca-certificates/zscaler.crt
RUN mkdir -p /etc/ssl/certs && \
    cat /usr/local/share/ca-certificates/zscaler.crt >> /etc/ssl/certs/ca-certificates.crt
EOF

# Build the custom BuildKit image
docker build -f /tmp/buildkit-ca/Dockerfile.buildkit -t buildkit-with-ca /tmp/buildkit-ca --progress=plain
```

### Step 2: Configure Buildx Builder

Create a buildx builder using the custom BuildKit image:

```bash
# Remove existing builder (if any)
docker buildx rm cameo-builder || true

# Create new builder with custom image and host network
docker buildx create --name cameo-builder --use \
  --driver docker-container \
  --driver-opt image=buildkit-with-ca \
  --driver-opt network=host
```

### Step 3: Build and Push Multi-Platform Images

Build and push the multi-platform image to Docker Hub:

```bash
# Build and push for multiple platforms
docker buildx build \
  --builder cameo-builder \
  --platform linux/amd64,linux/arm64 \
  -t goranc/cameodb:latest \
  --secret id=zscaler,src=/usr/local/share/ca-certificates/zscaler.crt \
  --push \
  .
```

### Notes

- The `--secret id=zscaler` mounts your certificate during the build process
- The custom BuildKit image ensures base image pulls work through corporate firewalls
- The Dockerfile is configured to trust the certificate for Cargo crate downloads
- Replace `goranc/cameodb:latest` with your desired repository and tag

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
