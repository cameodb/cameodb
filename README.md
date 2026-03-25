# CameoDB

A high-performance, distributed, shared-nothing hybrid-search database built in **Rust 2024 Edition**. CameoDB combines the reliability of ACID-compliant key-value storage (redb) with the power of full-text search (Tantivy) in a multi-tenant, horizontally scalable architecture.

## 🌟 **Key Features**

- 🔄 **Multi-Tenant Architecture** - Complete index isolation with dynamic scaling
- ⚡ **Atomic Batch Operations** - High-throughput bulk processing with ACID guarantees
- 🔍 **Hybrid Storage** - Combined KV store (redb) + full-text search (Tantivy)  
- 📊 **Schema Management** - Dynamic schema evolution with type validation
- 🌐 **Distributed Ready** - Actor-based architecture with consistent hashing
- 🔁 **Event-Driven Persistence** - Zero-polling cluster metadata with state reconciliation
- 🐳 **Docker Deployment** - Production-ready containerized setup
- 📈 **Performance Optimized** - Supervised Smart Commits, memory budgets, and adaptive batching
- 🦀 **Modern Rust 2024** - Built with latest Rust standards and performance optimizations

## 🚀 Quick Start

### Option 1: Run from Docker Hub (Recommended)

```bash
# Create data directory with proper permissions
# (Required: prevents Docker from creating it with root ownership)
mkdir -p $(pwd)/data/cameodb

# Pull and run CameoDB from Docker Hub
docker run -d \
  --name cameodb-server \
  --user $(id -u):$(id -g) \
  -p 9480:9480 \
  -p 9580:9580 \
  -v $(pwd)/data/cameodb:/data/cameodb \
  -e RUST_LOG=error \
  goranc/cameodb:latest

# CameoDB starts on http://localhost:9480 by default
```

**Note:** The image is configured to run as non-root user (65532:65532). Pre-creating the directory ensures proper ownership when mounting volumes.

**Or use Docker Compose (recommended for production):**
```bash
cd docker
docker-compose up -d
```
The docker-compose.yml already includes proper user configuration.

### Test the Server

```bash
# Check server health/version
curl -s http://localhost:9480/_cluster/health | jq

# List all indexes
curl -s "http://localhost:9480/_indexes" | jq
```

### Load and Search Sample Data

```bash
# Run client in interactive mode and load sample data
docker run --rm -it \
  --name cameodb-client \
  --network host \
  goranc/cameodb:latest \
  client --interactive

# Option 1: Load Books Dataset
# Then inside the interactive shell, run:
data load books https://dl.cameodb.com/examples/data/booksummaries.tsv

# After data load completes, verify the index:
list indexes

# Search examples for books:
search books title:"Harry Potter" limit 5
search books author:"J.K. Rowling" limit 3
search books categories:"Fantasy" limit 10

# Option 2: Load TED Talks Dataset
# Inside the interactive shell, run:
data load ted https://dl.cameodb.com/examples/data/youtube_ted_2024.csv

# After data load completes, verify the index:
list indexes

# Search examples for TED talks:
search ted title:"artificial intelligence" limit 5
search ted speaker:"Simon Sinek" limit 3
search ted tags:"technology" limit 10
search ted description:"climate change" limit 5
```

### Option 2: Build from Source

```bash
# Start CameoDB
cargo run --bin cameodb

# CameoDB starts on http://localhost:9480 by default
```

### Docker Run vs Docker Compose

The `docker run` command above is equivalent to the `docker-compose.yml` configuration:

| Docker Run Flag | Docker Compose Equivalent |
|----------------|---------------------------|
| `-p 9480:9480 -p 9580:9580` | `ports: ["9480:9480", "9580:9580"]` |
| `-v $(pwd)/data/cameodb:/data/cameodb` | `volumes: ["../data/cameodb:/data/cameodb"]` |
| `-e RUST_LOG=info` | `environment: ["RUST_LOG=info"]` |
| `--restart unless-stopped` | `restart: unless-stopped` |
| `--user 65532:65532` | `user: "65532:65532"` (handled by image) |

**Note**: The Docker image includes a built-in configuration file. For custom configurations, mount your own `cameodb.toml`:

```bash
docker run -d \
  --name cameodb-server \
  -p 9480:9480 \
  -p 9580:9580 \
  -v $(pwd)/data/cameodb:/data/cameodb \
  -v $(pwd)/cameodb.toml:/etc/cameodb/cameodb.toml:ro \
  -e RUST_LOG=info \
  --restart unless-stopped \
  goranc/cameodb:latest
```

## 🧠 Distributed Architecture Overview

CameoDB is designed as a **distributed, shared-nothing cluster**:

- **Per-node storage** is handled by the `server` crate with actors (`NodeOrchestrator`, `MicroshardActor`) on top of redb + Tantivy.
- **Routing & clustering** use a `ClusterCoordinator` actor with a consistent hash ring and libp2p Kademlia DHT.
- **Remote execution** is powered by Kameo remote actors over a custom libp2p swarm (TCP/QUIC/Noise/Yamux, no mDNS).
- **Scatter–gather** search and multi-node writes are implemented via a `RouterActor` that fans out to peers and aggregates results.
- **Event-driven metadata** - Cluster state transitions and persistence triggered purely by actor messages (`PeerDiscovered`, `PeerLost`, `MergeRemoteShards`) with no background polling or timeouts.
- **State reconciliation** - On boot, nodes compare expected cluster topology from snapshots vs actual peer reports, logging discrepancies and converging to distributed reality.

For a detailed walkthrough of the node-side actors, routing decisions, remote flows, and metadata persistence, see:

- [`crates/server/README.md`](crates/server/README.md)

## � Operation Routing Workflows

Every client request follows the same top-level path: **HTTP handler → RouterActor → ClusterCoordinator routing decision → execute**. The routing decision determines whether the operation runs locally, is forwarded to a single remote node (unicast), or is fanned out to all nodes (broadcast).

### Routing Decision Logic

```
                         ┌──────────────────────┐
                         │  ClusterCoordinator  │
                         │  RouteOperation msg  │
                         └─────────┬────────────┘
                                    │
                         routing_key present?
                           ┌────────┴────────┐
                          YES                NO
                           │                 │
                    Hash ring lookup    RoutingDecision::
                           │              Broadcast
                    owner == local?
                     ┌─────┴─────┐
                    YES          NO
                     │           │
              RoutingDecision  RoutingDecision::Remote
                ::Local        { node_id, peer_addr }
```

- **Local**: The owning shard lives on this node. Execute directly.
- **Remote**: The owning shard lives on another node. Forward via cached `RemoteActorRef`.
- **Broadcast**: No routing key (e.g. search). Fan out to local + all known peers, merge results.

### Read (Search) Workflow

Searches have no routing key, so they always broadcast to gather results from all nodes.

```
HTTP POST /api/{index}/search
  │
  ▼
RouterActor::route_and_handle(routing_key=None)
  │
  ▼ RoutingDecision::Broadcast
  │
  ├── LOCAL ──→ Worker Pool (or actor mailbox fallback)
  │               └── OrchestratorEngine::orch_search()
  │                     └── Fan out to all local MicroshardActors
  │                           └── spawn_blocking { store.search() }
  │
  └── REMOTE (per peer, up to fanout_limit) ──→ try_remote()
        │
        ▼
      RemotePeerPool::get_orchestrator(node_id)    ◄── cache hit: O(1)
        ├── RwLock read → HashMap lookup           ◄── cache miss: swarm lookup, then cached
        │
        ▼
      remote_ref.ask(&ClientOp::Search)
        │
        ▼
      Remote node executes same local search path
        │
        ▼
  ┌────────────────────────────────────────────┐
  │  Merge: bounded score-aware top-K merge,   │
  │  then truncate to the requested limit      │
  └────────────────────────────────────────────┘
```

**Key characteristics:**
- Concurrent local + remote execution via `tokio::join!`
- Bounded shard and remote fan-out using configured concurrency limits
- Score-aware top-K merge keeps the strongest hits even when better remote results arrive later
- Configurable `broadcast_timeout` and `broadcast_fanout_limit`
- Streaming search variant available (`/search/stream`) returning NDJSON

### Write (Single Document) Workflow

Single writes always have a routing key (defaults to `doc.id`), so they are unicast to the owning node.

```
HTTP PUT /api/{index}/document
  │
  ▼
RouterActor::route_and_handle(routing_key=Some(doc.id))
  │
  ▼ Hash ring lookup → shard owner
  │
  ├── RoutingDecision::Local
  │     │
  │     ▼
  │   Worker Pool (Write is hot-path eligible)
  │     └── OrchestratorEngine::orch_write()
  │           └── Route to specific MicroshardActor via hash ring
  │                 └── writer_thread → redb WAL + Tantivy index
  │
  └── RoutingDecision::Remote { node_id, peer_addr }
        │
        ▼
      RouterActor::handle_remote() ──→ retry loop (configurable attempts)
        │
        ▼
      RouterActor::try_remote()
        │
        ▼
      RemotePeerPool::get_orchestrator(node_id)    ◄── cached lookup
        │
        ▼
      remote_ref.ask(&ClientOp::Write)
        │
        ▼
      Remote node executes same local write path
```

**Key characteristics:**
- Writes are **never broadcast** — the router rejects broadcast routing for writes
- Retry with configurable `remote_retry_attempts` and `remote_timeout`
- On repeated failure, triggers `RequestBootstrapRedial` to recover connectivity
- Each shard has a dedicated writer thread (no lock contention)

### Bulk Write Workflow

Bulk writes are the most complex path: documents are routed individually, then grouped by owning node for batched forwarding.

```
HTTP POST /api/{index}/_bulk
  │
  ▼
RouterActor::route_and_handle(routing_hint=first_doc.id)
  │
  ▼ Routed to one node (usually local for the first doc)
  │
  ▼
NodeOrchestrator::orch_bulk_write(index, docs[])
  │
  ├── 1. Schema Resolution
  │     └── Fingerprint cache → shard fallback
  │
  ├── 2. Staged Schema Validation
  │     └── Parallel Rayon validation + sequential evolution
  │
  ├── 3. Per-Document Routing (spawn_blocking + Rayon par_iter)
  │     └── For each doc: hash(routing_key) → ConsistentRing → target shard
  │
  ├── 4. Separate Local vs Remote
  │     ├── shard in self.shards → local_docs
  │     └── shard owned by other node → remote_docs (grouped by node_id)
  │
  ├── 5. Phase 3.1: Parallel Local Shard Processing
  │     └── Per-shard MicroshardActor::write_batch()
  │           └── writer_thread → redb WAL + Tantivy index
  │
  └── 6. Phase 3.2: Parallel Remote Forwarding (futures::join_all)
        │
        for each (node_id, docs_for_remote):
          │
          ▼
        NodeOrchestrator::forward_bulk_to_remote()
          │
          ▼
        RemotePeerPool::get_orchestrator(node_id)    ◄── cached lookup
          │
          ▼
        remote_ref.ask(&ClientOp::BulkWrite)
          │
          ▼
        Remote node runs orch_bulk_write() (recursive, same path)
```

**Key characteristics:**
- Documents are individually routed then batched by destination node
- Local and remote processing run in parallel
- Schema validation happens once on the entry node before routing
- Remote forwarding uses the same `RemotePeerPool` cache as other operations

### Connection Pool & Cache Invalidation

The `RemotePeerPool` eliminates repeated swarm registry/DHT lookups on every remote operation:

```
                    ┌───────────────────────────────────┐
                    │         RemotePeerPool            │
                    │  RwLock<HashMap<(Uuid, Channel),  │
                    │         RemoteActorRef>>          │
                    ├───────────────────────────────────┤
                    │  get_orchestrator(node, channel)  │──→ cache hit: clone ref
                    │  get_coordinator(node)            │──→ cache miss: lookup + cache
                    │  invalidate_peer(node)            │──→ evict all refs for node
                    │  invalidate_all()                 │──→ full cache clear
                    └───────────────────────────────────┘
                                    ▲
                                    │ invalidate_peer()
                    ┌───────────────┴───────────────┐
                    │  ClusterCoordinator           │
                    │  handle(PeerLost { node_id }) │
                    └───────────────────────────────┘
                                    ▲
                                    │ swarm event
                              Peer disconnected
```

**Integration points:**

| Call Site | Lookup Type | Purpose |
|---|---|---|
| `RouterActor::try_remote` | Orchestrator | Routed single operations (search, write) |
| `NodeOrchestrator::forward_bulk_to_remote` | Orchestrator | Bulk write forwarding |
| `ClusterCoordinator::exchange_shards_with_peer` | Coordinator | Shard metadata exchange |
| `ClusterCoordinator` stability sync | Coordinator | Post-bootstrap shard push |
| `ClusterCoordinator` peer discovery | Coordinator | New peer shard fetch |
| `ClusterCoordinator` delete forwarding | Orchestrator | Cross-cluster index deletion |

## �📡 HTTP API Reference

CameoDB provides a comprehensive REST API for document management, search, and system administration.

### 🔍 Search Operations

#### Standard Search
Search documents within an index with relevance scoring. Returns a single JSON payload (non-streaming).

```bash
POST /api/{index}/search
```

**Example:**
```bash
curl -s -X POST http://localhost:9480/api/books/search \
  -H "Content-Type: application/json" \
  -d '{
    "query": "science fiction space",
    "limit": 10
  }'
```

> **Return fields list:** You can ask CameoDB to return only a subset of document fields by either:
>
> 1. Supplying an explicit list in the payload: `"fields": ["title", "author", "year"]`
> 2. Embedding a `return` clause at the end of the Tantivy query: `"query": "space opera return title,author"`
>
> The JSON payload always wins over inline `return` clauses. Metadata keys (those starting with `_`, e.g. `_score`, `_id`, `shard_id`) are preserved automatically.

**Response:**
```json
{
  "hits": [
    {
      "_score": 2.45,
      "shard_id": "a1b2c3d4-...",
      "id": "2080",
      "title": "A Fire Upon the Deep",
      "author": "Vernor Vinge",
      "genres": ["Hard science fiction", "Science Fiction"]
    }
  ],
  "hits_returned": 1,
  "total_hits": 42,
  "limit": 10,
  "total_shards": 4,
  "nodes_contacted": 1,
  "failed_shards": 0,
  "took_ms": 12
}
```

#### Streaming Search
Get search results as a real-time stream for large result sets.

```bash
POST /api/{index}/search/stream
```

**Example:**
```bash
curl -s -X POST http://localhost:9480/api/books/search/stream \
  -H "Content-Type: application/json" \
  -d '{"query": "fantasy adventure"}' \
  --no-buffer
```

**Response:** NDJSON stream (one hit per line). If no `hits` array is present, falls back to a single JSON body.

> **Note:** Streaming search returns results as NDJSON for improved performance with large result sets.

### 📝 Document Operations

#### Write Single Document
Insert or update a single document.

```bash
PUT /api/{index}/document
```

**Example:**
```bash
curl -s -X PUT http://localhost:9480/api/books/document \
  -H "Content-Type: application/json" \
  -d '{
    "id": "book_001",
    "routing_key": "book_001",
    "doc": {
      "title": "The Rust Programming Language",
      "author": "Steve Klabnik",
      "publication_year": 2018,
      "genres": ["Programming", "Technical"],
      "description": "The official guide to Rust programming language"
    }
  }'
```

**Response:**
```json
{
  "id": "book_001",
  "result": "created",
  "version": 1001,
  "shard_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
}
```

#### Bulk Write Documents
Insert or update multiple documents in a single atomic operation.

```bash
POST /api/{index}/_bulk
```

**Example:**
```bash
curl -s -X POST http://localhost:9480/api/books/_bulk \
  -H "Content-Type: application/json" \
  -d '[
    {
      "id": "book_002",
      "doc": {
        "title": "Clean Code",
        "author": "Robert C. Martin",
        "genres": ["Programming"]
      }
    },
    {
      "id": "book_003", 
      "doc": {
        "title": "Design Patterns",
        "author": "Gang of Four",
        "genres": ["Programming", "Software Engineering"]
      }
    }
  ]'
```

**Response:**
```json
{
  "items_received": 2,
  "items_written": 2,
  "errors": [],
  "took_ms": 45
}
```

#### Streaming Write Documents
Insert or update multiple documents using NDJSON streaming for large datasets.

```bash
POST /api/{index}/document/stream
```

**Example:**
```bash
cat << 'EOF' | curl -s -X POST http://localhost:9480/api/books/document/stream \
  -H "Content-Type: application/json" \
  --data-binary @-
{"id": "book_002", "doc": {"title": "Clean Code", "author": "Robert C. Martin", "genres": ["Programming"]}}
{"id": "book_003", "doc": {"title": "Design Patterns", "author": "Gang of Four", "genres": ["Programming", "Software Engineering"]}}
EOF
```

**Response:**
```json
{
  "took_ms": 42,
  "items_received": 2,
  "items_written": 2,
  "errors": []
}
```

> **Note:** Streaming write accepts NDJSON (one JSON document per line) for memory-efficient processing of large datasets.

### ⚙️ Index Management

#### Create/Update Index Schema
Define or modify the schema for an index.

```bash
PUT /api/{index}/_config
```

**Example:**
```bash
curl -s -X PUT http://localhost:9480/api/books/_config \
  -H "Content-Type: application/json" \
  -d '{
    "shard_count": 256,
    "fields": {
      "title": {
        "name": "title",
        "field_type": "text",
        "indexed": true
      },
      "author": {
        "name": "author", 
        "field_type": "text",
        "indexed": true
      },
      "publication_year": {
        "name": "publication_year",
        "field_type": "number",
        "indexed": false
      }
    }
  }'
```

**Response:**
```json
{
  "acknowledged": true,
  "index": "books",
  "shard_count": 256,
  "field_names": ["id", "author", "title", "publication_year"]
}
```

#### Get Index Schema
Retrieve the current schema for an index.

```bash
GET /api/{index}/_config
```

**Example:**
```bash
curl -s http://localhost:9480/api/books/_config
```

**Response:**
```json
{
  "field_names": ["author", "title"],
  "fields": {
    "title": {"name": "title", "field_type": "text", "indexed": true},
    "author": {"name": "author", "field_type": "text", "indexed": true}
  },
  "shard_count": 256
}
```

#### 🗑️ Delete Index
Permanently delete an index and all its data across the cluster.

```bash
DELETE /api/{index}
```

**Example:**
```bash
curl -s -X DELETE http://localhost:9480/api/books
```

**Response:**
```json
{
  "success": true,
  "index": "books",
  "deleted_from_shards": 4,
  "total_shards": 4,
  "errors": null
}
```

#### List All Indexes
Get comprehensive information about all available indexes.

```bash
GET /_indexes
```

**Example:**
```bash
curl -s http://localhost:9480/_indexes
```

**Response:**
```json
{
  "indexes": [
    {
      "name": "books",
      "document_count": 16559,
      "total_size_bytes": 45231680,
      "size_mb": 43,
      "shard_count": 4,
      "field_names": ["id", "author", "genres", "publication_date", "summary", "title"]
    },
    {
      "name": "ted",
      "document_count": 4641,
      "total_size_bytes": 12458752,
      "size_mb": 12,
      "shard_count": 4,
      "field_names": ["id", "description", "like_count", "speaker", "tags", "title", "view_count"]
    }
  ],
  "total_indexes": 2,
  "total_shards": 4,
  "node_id": "550e8400-e29b-41d4-a716-446655440000"
}
```

### 🏥 System Operations

#### Health Check
Get cluster health and node information.

```bash
GET /_cluster/health
```

**Example:**
```bash
curl -s http://localhost:9480/_cluster/health
```

**Response:**
```json
{
  "status": "green",
  "node_id": "550e8400-e29b-41d4-a716-446655440000",
  "active_shards": 4
}
```

## 📊 Data Ingestion Examples

CameoDB includes optimized ingestion scripts for common datasets:

### TED Talks Dataset
```bash
# Ingest TED talks (CSV format, ~4,600 documents)
python3 scripts/examples/ingest_ted.py

# Custom configuration
python3 scripts/examples/ingest_ted.py --index talks --batch-size 200
```

### Book Summaries Dataset
```bash
# Ingest book summaries (TSV format, 16,559 documents)  
python3 scripts/examples/ingest_books.py

# Test with dry run
python3 scripts/examples/ingest_books.py --dry-run
```

## 🏗️ Architecture

- **Distributed**: Shared-nothing architecture with consistent hashing and Kameo actor system
- **Multi-tenant**: Complete isolation between indexes  
- **Hybrid Storage**: ACID-compliant KV store + full-text search
- **High Performance**: Optimized batch operations and memory management
- **Rust-native**: Built for safety, performance, and concurrency
- **Actor-based**: Transparent distributed actors with automatic service discovery

## 📈 Performance Features

- **Batch Processing**: Atomic bulk operations across shards
- **Supervised Smart Commits**: Memory-aware commit strategies with eventual durability guarantees
- **Consistent Hashing**: Optimal data distribution
- **Streaming Search**: NDJSON search responses with bounded internal fan-out and score-aware top-K merging
- **Schema Evolution**: Dynamic field addition and validation
- **Query Timing**: Detailed performance metrics like Lucene/Solr (QTime, component timing)

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

### Common Docker Commands

```bash
# Check status (use -f for the cluster file)
docker-compose -f docker/docker-compose-cluster.yml ps

# View logs
docker-compose -f docker/docker-compose.yml logs -f

# Stop and remove containers
docker-compose -f docker/docker-compose-cluster.yml down
```

For more details, see the [Docker README](docker/README.md), which includes the latest swarm environment variables and configuration guidance.

## 🔧 Configuration

CameoDB configuration via `cameodb.toml` now mirrors the runtime struct layout:

```toml
[node]
label = "cameo-node-01"
zone = "default"

[network.http]
bind_address = "0.0.0.0"
port = 9480
request_timeout_secs = 30
max_body_size_mb = 200
cors_allowed_origins = ["*"]

[network.cluster]
enabled = true
bind_address = "0.0.0.0"
port = 9580
cluster_name = "cameodb-cluster"
seed_nodes = []
# cluster_nodes = ["/ip4/10.0.1.5/tcp/9580"] # Optional validation list

[storage]
data_paths = ["./data/cameodb"]
disk_usage_threshold_percent = 90
wal_sync = true
wal_segment_size_mb = 64
default_batch_size = 1000
num_shards_init = 4
max_shards_per_node = 8

[search]
indexer_memory_min_mb = 32
indexer_memory_max_mb = 512
total_memory_limit_mb = 4096
memory_pressure_threshold_percent = 80
search_threads = 8
enable_streaming_search = true
max_concurrent_shard_searches = 32
max_concurrent_remote_searches = 8
enable_early_termination = true
supervisor_timeout_secs = 5
default_search_limit = 10
```

- `node` provides human-friendly identity fields (`label`, `zone`).
- `network` separates HTTP and cluster transport while clarifying `bind_address`.
- `storage` centralizes shard configuration plus disk thresholds.
- `search` exposes indexer memory budgets, streaming search settings, shard/remote search concurrency caps, supervisor timeout for auto-commits, and `default_search_limit` for response pagination.

## � System Requirements

### **Development**
- **Rust**: 1.90.0+ with Rust 2024 Edition support
- **OS**: macOS 11+, Ubuntu 20.04+, Fedora Linux 39+, or Windows 10+ with WSL2
- **Memory**: 4GB RAM minimum (8GB+ recommended)
- **Storage**: 10GB+ available space
- **Network**: Ports 9480 (HTTP API), 9580 (cluster communication)

### **Dependencies**
- **System**: `curl`, `jq` (for scripts and testing)
- **Python**: 3.9+ with `requests` (for data ingestion examples)
- **Docker**: Docker Desktop (for distributed deployment)

### **Quick Environment Setup**
```bash
# Automated dependency installation
./scripts/setup/install-deps.sh

# Initialize development cluster with sample data
./scripts/setup/init-cluster.sh

# Verify installation  
./scripts/testing/test-api.sh
```

## 📚 Documentation

- [Storage Engine Details](crates/storage/README.md) - Multi-tenant hybrid storage architecture
- [Cluster Management](crates/cluster/README.md) - Distributed topology and consistent hashing  
- [Development Scripts](scripts/README.md) - Testing, data generation, and operations
- [Ingestion Examples](scripts/examples/README.md) - TED talks and book summaries datasets
- [Docker Deployment](docker/README.md) - Container deployment and cluster setup
- [Website](docs/web/README.md) - Live documentation and feature showcase

## 🚀 Getting Started

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd cameodb
   ```

2. **Build and run**
   ```bash
   cargo run --bin cameodb
   ```

3. **Ingest sample data**
   ```bash
   python3 scripts/examples/ingest_books.py --dry-run
   python3 scripts/examples/ingest_books.py
   ```

4. **Query your data**
   ```bash
   curl -s -X POST http://localhost:9480/api/books/search \
     -H "Content-Type: application/json" \
     -d '{"query": "science fiction", "limit": 5}'
   ```

## 📄 License

CameoDB uses a multi-license policy inspired by Sentry:

| Component / Path | License | Notes |
|------------------|---------|-------|
| Core crates (e.g. `crates/cluster`, `crates/storage`, supporting libraries) | Apache-2.0 | Fully open source core with patent protection |
| SDKs and client tooling (e.g. `crates/client`) | MIT | Maximizes compatibility (incl. GPL) |
| Product / node application (`crates/server`) | FSL-1.1-Apache-2.0 | Restricts competitive hosting for 2 years, then reverts to Apache-2.0 |

License texts are available under [`licenses/`](licenses/):

- `licenses/LICENSE-APACHE-2.0`
- `licenses/LICENSE-MIT`
- `licenses/LICENSE-FSL-1.1-APACHE-2.0`

See the top-level [LICENSE](LICENSE) file for details.


### **Development Quick Start**
```bash
# Setup development environment
./scripts/setup/install-deps.sh

# Run full test suite
cargo test --all
cargo clippy --all-targets -- -D warnings

# Test with sample data
./scripts/data/sample-data.sh
./scripts/testing/load-test.sh
```

## 📦 RPM Package Building

CameoDB supports building RPM packages for x86_64 Linux distributions using cargo-zigbuild for cross-compilation.

### Prerequisites

Install the required cargo extensions for cross-compilation and RPM generation:
```bash
# Install cargo-zigbuild for cross-compilation
cargo install cargo-zigbuild

# Install cargo-generate-rpm for RPM package generation
cargo install cargo-generate-rpm
```

### Build RPM Package

**Option 1: Native x86_64 Linux Build (Recommended for hardened executables)**
```bash
# Build hardened executable with security mitigations (flags in .cargo/config.toml)
cargo build --release --target x86_64-unknown-linux-musl

# OR override with explicit RUSTFLAGS:
RUSTFLAGS="-C relocation-model=pie -C relro-level=full -C link-arg=-Wl,-z,now -C link-arg=-fstack-protector -C link-arg=-D_FORTIFY_SOURCE=2" \
cargo build --release --target x86_64-unknown-linux-musl

# Generate RPM package (run from project root directory)
cargo generate-rpm -p crates/server --target x86_64-unknown-linux-musl --auto-req disabled \
  -o target/x86_64-unknown-linux-musl/release/cameodb-0.2.2-1.x86_64.rpm \
  --set-metadata 'package.name="cameodb"'
```

**Option 2: Cross-compilation with cargo-zigbuild (supports hardening)**
```bash
# Build hardened binary for Linux x86_64 musl target (flags in .cargo/config.toml)
cargo zigbuild --release --target x86_64-unknown-linux-musl \
    --no-default-features \
    --features client/native-tls-vendored

# OR override with explicit RUSTFLAGS:
RUSTFLAGS="-C target-feature=+crt-static -C relocation-model=pie -C relro-level=full -C link-arg=-pie -C link-arg=-static -C link-arg=-Wl,-z,now -C link-arg=-Wl,-z,relro -C link-arg=-fstack-protector-strong -C link-arg=-D_FORTIFY_SOURCE=2" \
cargo zigbuild --release --target x86_64-unknown-linux-musl \
    --no-default-features \
    --features client/native-tls-vendored

# Generate RPM package with standard naming (run from project root directory)
cargo generate-rpm -p crates/server --target x86_64-unknown-linux-musl --auto-req disabled \
  -o target/x86_64-unknown-linux-musl/release/cameodb-0.2.2-1.x86_64.rpm \
  --set-metadata 'package.name="cameodb"'

# The RPM package will be available at:
# target/x86_64-unknown-linux-musl/release/cameodb-0.2.2-1.x86_64.rpm
```

**Option 3: DEB Package Generation (Ubuntu/Debian)**
```bash
# Install cargo-deb
cargo install cargo-deb

# Build hardened binary using Docker (native musl toolchain)
# This avoids Zig cross-compilation issues with C dependencies
# IMPORTANT: Use --platform linux/amd64 to get x86_64 container (not ARM64)
# Use pre-built builder image (dependencies pre-installed)
# Build the builder image once:
docker buildx build --platform linux/amd64 \
  --builder cameo-builder \
  --load \
  -t cameo-builder -f builder.Dockerfile .

# Then use it for fast builds:
docker run --rm --platform linux/amd64 \
  -v "$PWD":/workspace -w /workspace \
  -v /tmp/buildkit-ca/zscaler.crt:/usr/local/share/ca-certificates/zscaler.crt:ro \
  -e CC_x86_64_unknown_linux_musl=musl-gcc \
  -e AR_x86_64_unknown_linux_musl=ar \
  -e RANLIB_x86_64_unknown_linux_musl=ranlib \
  cameo-builder bash -c "
    cat /usr/local/share/ca-certificates/zscaler.crt >> /etc/ssl/certs/ca-certificates.crt && \
    export SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt && \
    cargo build --release --target x86_64-unknown-linux-musl \
      --no-default-features \
      --features client/native-tls-vendored
  "

# Generate DEB package (run on host after Docker build)
# Use --no-build to package the existing binary without rebuilding
# Use --no-strip on macOS (macOS strip/objcopy don't support Linux binaries)
# Note: Binary is automatically stripped by Cargo's [profile.release] strip = "symbols"
# The debug symbols warning from cargo-deb is cosmetic and can be ignored.
cargo deb --no-build --no-strip --target x86_64-unknown-linux-musl -p server

# With custom output path (follows DEB naming standards)
cargo deb --no-build --no-strip --target x86_64-unknown-linux-musl -p server \
  --output target/x86_64-unknown-linux-musl/release/cameodb_0.2.2_amd64.deb

# The DEB package will be available at:
# target/x86_64-unknown-linux-musl/debian/cameodb_0.2.2_amd64.deb
# OR with custom output: target/x86_64-unknown-linux-musl/release/cameodb_0.2.2_amd64.deb
```

**Option 4: Automated Build Script (Recommended for CI/CD)**
```bash
# Use the optimized build script with persistent caching
# This script handles both RPM and DEB package generation in one run
./scripts/build/build-dist.sh
```

The `build-dist.sh` script provides:
- **Persistent Docker volumes** for cargo registry and target cache (dramatic speed improvements on subsequent builds)
- **Corporate CA certificate handling** for network trust
- **Automatic binary stripping** via Cargo profile optimization
- **Both RPM and DEB package generation** in a single run
- **Colored output and progress indicators**

**Prerequisites for build-dist.sh:**
```bash
# Make the script executable
chmod +x build-dist.sh

# Ensure Docker buildx builder is running
docker buildx ls
```

### Signing Release Artifacts

Cosign 2.x defaults to the new bundle format. Generate one `.bundle` file per artifact and ship it together with the binary and `cosign.pub` so downstream users can verify releases.

```bash
cosign sign-blob \
  --key /usr/local/share/ca-certificates/cosign.key \
  --bundle target/release/cameodb.bundle \
  target/release/cameodb

cosign sign-blob \
  --key /usr/local/share/ca-certificates/cosign.key \
  --bundle target/x86_64-unknown-linux-musl/release/cameodb.bundle \
  target/x86_64-unknown-linux-musl/release/cameodb

cosign sign-blob \
  --key /usr/local/share/ca-certificates/cosign.key \
  --bundle target/x86_64-unknown-linux-musl/release/cameodb-0.2.2-1.x86_64.rpm.bundle \
  target/x86_64-unknown-linux-musl/release/cameodb-0.2.2-1.x86_64.rpm

cosign sign-blob \
  --key /usr/local/share/ca-certificates/cosign.key \
  --bundle target/x86_64-unknown-linux-musl/release/cameodb_0.2.2_amd64.deb.bundle \
  target/x86_64-unknown-linux-musl/release/cameodb_0.2.2_amd64.deb
```

**Verification example:**

```bash
cosign verify-blob \
  --key cosign.pub \
  --bundle cameodb.bundle \
  cameodb
```

If you need legacy `.sig`/`.cert` files instead, add `--legacy-signatures` (or set `COSIGN_EXPERIMENTAL=0`) and keep the previous `--output-signature` / `--output-certificate` flags.

**Note**: Two approaches for hardening flags:
1. **Pre-configured**: Hardening flags are set in `.cargo/config.toml` and applied automatically
2. **Explicit override**: Use `RUSTFLAGS="..."` to override or customize flags as shown above

Hardening flags explained:
- `-C target-feature=+crt-static` enables static C runtime linking
- `-C relocation-model=pie` enables Position Independent Executable for ASLR support
- `-C relro-level=full` enables Full RELRO (Relocation Read-Only) 
- `-C link-arg=-pie` + `-C link-arg=-static` creates static PIE executable (separated flags)
- `-C link-arg=-Wl,-z,now` enables immediate symbol binding
- `-C link-arg=-Wl,-z,relro` enables RELRO protection
- `-C link-arg=-fstack-protector-strong` enables strong stack protection against buffer overflows
- `-C link-arg=-D_FORTIFY_SOURCE=2` enables fortified memory functions for additional safety
- `opt-level = 3` (release profile) required for fortified functions to work properly
- Both cargo build and cargo-zigbuild support these rustc-native flags

**Windows Hardening** (when building for Windows targets):
- `/SDL` enables Security Development Lifecycle checks (equivalent to VS /SDL)
- `/DYNAMICBASE` enables ASLR (Address Space Layout Randomization)
- `/HIGHENTROPYVA` enables 64-bit ASLR with high entropy
- `/NXCOMPAT` enables DEP (Data Execution Prevention)
- `/GUARD:CF` enables Control Flow Guard

**Verification**: 
- For dynamic binaries (gnu): `file` shows "pie executable"
- For static binaries (musl): `file` shows "executable" but hardening is still applied
- Use `greadelf -d` or check binary headers to verify PIE and RELRO on static binaries
- Fortified functions replace unsafe C library calls with checked versions

### RPM Package Contents

- **Binary**: `/usr/local/bin/cameodb` (statically linked, no external dependencies)
- **Config**: `/etc/cameodb/cameodb.toml`
- **Service**: `/usr/lib/systemd/system/cameodb.service`
- **User/Group**: `cameodb` (created automatically during install)
- **Data Directory**: `/var/lib/cameodb` (created with proper permissions)

### DEB Package Contents

- **Binary**: `/usr/local/bin/cameodb` (statically linked, no external dependencies)
- **Config**: `/etc/cameodb/cameodb.toml` (marked as config file, preserved on upgrades)
- **Service**: `/lib/systemd/system/cameodb.service`
- **User/Group**: `cameodb` (created automatically during install)
- **Data Directory**: `/var/lib/cameodb` (created with proper permissions)

### Installation on Target System

**For RPM-based systems (RHEL, CentOS, Fedora):**
```bash
# Verify RPM package before installation
rpm -qpi cameodb-0.2.2-1.x86_64.rpm

# Check package contents
rpm -qpl cameodb-0.2.2-1.x86_64.rpm

# Install the RPM package
sudo rpm -i cameodb-0.2.2-1.x86_64.rpm

# Start and enable the service
sudo systemctl daemon-reload
sudo systemctl enable cameodb
sudo systemctl start cameodb
```

**For DEB-based systems (Ubuntu, Debian):**
```bash
# Verify DEB package before installation
dpkg -I cameodb_0.2.2_amd64.deb

# Check package contents
dpkg -c cameodb_0.2.2_amd64.deb

# Install the DEB package
sudo dpkg -i cameodb_0.2.2_amd64.deb

# Start and enable the service
sudo systemctl daemon-reload
sudo systemctl enable cameodb
sudo systemctl start cameodb
```
# Check status
sudo systemctl status cameodb

### Custom Data Directory Setup

For production deployments, you may want to store CameoDB data on a separate disk or partition. Create a custom data directory with proper permissions:

```bash
# Create custom data directory (example: /data01/cameodb)
sudo mkdir /data01/cameodb

# Set ownership to cameodb user and group
sudo chown cameodb:cameodb /data01/cameodb

# Set secure permissions (read/write only for cameodb user)
sudo chmod 700 /data01/cameodb
```

After creating the custom directory, update the `data_paths` in your `/etc/cameodb/cameodb.toml` configuration file:

```toml
[storage]
data_paths = ["/data01/cameodb"]
```

Then restart the CameoDB service to apply the new configuration:

```bash
sudo systemctl restart cameodb
```

---

## 🤝 Contributing

**CameoDB** - High-performance distributed hybrid-search database built in Rust

We welcome contributions! Please see our [Contributing Guidelines](CONTRIBUTING.md) for details on:

- 🐛 **Bug Reports** - Help us improve by reporting issues
- 💡 **Feature Requests** - Suggest new capabilities  
- 🔧 **Pull Requests** - Submit code improvements
- 📝 **Documentation** - Help improve our docs
- 🧪 **Testing** - Add test cases and benchmarks