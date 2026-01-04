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
