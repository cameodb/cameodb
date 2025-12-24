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
- 📈 **Performance Optimized** - Smart commits, memory budgets, and adaptive batching
- 🦀 **Modern Rust 2024** - Built with latest Rust standards and performance optimizations

## 🚀 Quick Start

```bash
# Start the server
cargo run --bin cameodb

# Server starts on http://localhost:9480 by default
```

## 🧠 Distributed Architecture Overview

CameoDB is designed as a **distributed, shared-nothing cluster**:

- **Per-node storage** is handled by the `server` crate with actors (`NodeOrchestrator`, `MicroshardActor`) on top of redb + Tantivy.
- **Routing & clustering** use a `ClusterCoordinator` actor with a consistent hash ring and libp2p Kademlia DHT.
- **Remote execution** is powered by Kameo remote actors over a custom libp2p swarm (TCP/QUIC/Noise/Yamux, no mDNS).
- **Scatter–gather** search and multi-node writes are implemented via a `RouterActor` that fans out to peers and aggregates results.
- **Event-driven metadata** - Cluster state transitions and persistence triggered purely by actor messages (`PeerDiscovered`, `PeerLost`, `MergeRemoteShards`) with no background polling or timeouts.
- **State reconciliation** - On boot, nodes compare expected cluster topology from snapshots vs actual peer reports, logging discrepancies and converging to distributed reality.

For a detailed walkthrough of the server-side actors, routing decisions, remote flows, and metadata persistence, see:

- [`crates/server/README.md`](crates/server/README.md)

## 📡 HTTP API Reference

CameoDB provides a comprehensive REST API for document management, search, and system administration.

### 🔍 Search Operations

#### Standard Search
Search documents within an index with relevance scoring.

```bash
POST /api/:index/search
```

**Example:**
```bash
curl -X POST http://localhost:9480/api/books/search \
  -H "Content-Type: application/json" \
  -d '{
    "query": "science fiction space",
    "limit": 10
  }'
```

**Response:**
```json
{
  "results": [
    {
      "score": 2.45,
      "document": {
        "id": "2080",
        "title": "A Fire Upon the Deep",
        "author": "Vernor Vinge",
        "genres": ["Hard science fiction", "Science Fiction"]
      }
    }
  ],
  "total_results": 42,
  "total_shards": 4,
  "successful_shards": 4,
  "failed_shards": 0,
  "query": "science fiction space",
  "QTime": 12,
  "timing": {
    "total_ms": 12,
    "query": "science fiction space"
  }
}
```

#### Streaming Search
Get search results as a real-time stream for large result sets.

```bash
POST /api/:index/stream
```

**Example:**
```bash
curl -X POST http://localhost:9480/api/books/stream \
  -H "Content-Type: application/json" \
  -d '{"query": "fantasy adventure"}' \
  --no-buffer
```

**Response:** NDJSON stream
```json
{"_score": 3.2, "id": "123", "title": "The Hobbit", "author": "J.R.R. Tolkien"}
{"_score": 2.8, "id": "456", "title": "Dune", "author": "Frank Herbert"}
```

### 📝 Document Operations

#### Write Single Document
Insert or update a single document.

```bash
PUT /api/:index/document
```

**Example:**
```bash
curl -X PUT http://localhost:9480/api/books/document \
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
  "indexed": true,
  "sequence_id": 1001,
  "shard_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
}
```

#### Bulk Write Documents
Insert or update multiple documents in a single atomic operation.

```bash
POST /api/:index/_bulk
```

**Example:**
```bash
curl -X POST http://localhost:9480/api/books/_bulk \
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
  "items_indexed": 2,
  "successful_shards": 4,
  "failed_shards": 0,
  "shard_results": [
    {"shard_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890", "items_indexed": 1, "status": "success"},
    {"shard_id": "b2c3d4e5-f6g7-8901-bcde-f12345678901", "items_indexed": 1, "status": "success"}
  ],
  "QTime": 45,
  "timing": {
    "total_ms": 45,
    "documents_processed": 2
  }
}
```

### ⚙️ Index Management

#### Create/Update Index Schema
Define or modify the schema for an index.

```bash
PUT /api/:index/_config
```

**Example:**
```bash
curl -X PUT http://localhost:9480/api/books/_config \
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

#### Get Index Schema
Retrieve the current schema for an index.

```bash
GET /api/:index/_config
```

**Example:**
```bash
curl http://localhost:9480/api/books/_config
```

**Response:**
```json
{
  "index": "books",
  "shard_count": 256,
  "fields": {
    "title": {"name": "title", "field_type": "text", "indexed": true},
    "author": {"name": "author", "field_type": "text", "indexed": true}
  },
  "status": "found"
}
```

#### List All Indexes
Get comprehensive information about all available indexes.

```bash
GET /_indexes
```

**Example:**
```bash
curl http://localhost:9480/_indexes
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
      "tantivy_shards": 4,
      "field_names": ["id", "author", "genres", "publication_date", "summary", "title"]
    },
    {
      "name": "ted",
      "document_count": 4641,
      "total_size_bytes": 12458752,
      "size_mb": 12,
      "shard_count": 4,
      "tantivy_shards": 4,
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
curl http://localhost:9480/_cluster/health
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
- **Smart Commits**: Memory-aware commit strategies
- **Consistent Hashing**: Optimal data distribution
- **Streaming Search**: Real-time result streaming for large queries
- **Schema Evolution**: Dynamic field addition and validation
- **Query Timing**: Detailed performance metrics like Lucene/Solr (QTime, component timing)

## 🐳 Docker Deployment

CameoDB provides configurations for both single-node and multi-node cluster deployments using Docker.

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
- **Swarm Configuration**: `CAMEODB_CLUSTER_NAME`, `CAMEODB_CLUSTER_PORT`, `CAMEODB_BOOTSTRAP_NODES`, and `CAMEODB_DISTRIBUTED_ACTORS` environment variables drive the Kademlia swarm. Update them per deployment needs.

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

Server configuration via `cameodb.toml`:

```toml
[server.http]
host = "0.0.0.0"
port = 9480

[search]
writer_memory_min_mb = 16
writer_memory_max_mb = 256

[storage]
data_paths = ["./data/cameodb"]
wal_sync = true
default_batch_size = 1000
```

## � System Requirements

### **Development**
- **Rust**: 1.90.0+ with Rust 2024 Edition support
- **OS**: macOS 11+, Ubuntu 20.04+, or Windows 10+ with WSL2
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
   curl -X POST http://localhost:9480/api/books/search \
     -H "Content-Type: application/json" \
     -d '{"query": "science fiction", "limit": 5}'
   ```

## 📄 License

CameoDB uses a multi-license policy inspired by Sentry:

| Component / Path | License | Notes |
|------------------|---------|-------|
| Core crates (e.g. `crates/cluster`, `crates/storage`, supporting libraries) | Apache-2.0 | Fully open source core with patent protection |
| SDKs and client tooling (e.g. `crates/client`) | MIT | Maximizes compatibility (incl. GPL) |
| Product / server application (`crates/server`) | FSL-1.1-Apache-2.0 | Restricts competitive hosting for 2 years, then reverts to Apache-2.0 |

License texts are available under [`licenses/`](licenses/):

- `licenses/LICENSE-APACHE-2.0`
- `licenses/LICENSE-MIT`
- `licenses/LICENSE-FSL-1.1-APACHE-2.0`

See the top-level [LICENSE](LICENSE) file for details.

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guidelines](CONTRIBUTING.md) for details on:

- 🐛 **Bug Reports** - Help us improve by reporting issues
- 💡 **Feature Requests** - Suggest new capabilities  
- 🔧 **Pull Requests** - Submit code improvements
- 📝 **Documentation** - Help improve our docs
- 🧪 **Testing** - Add test cases and benchmarks

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

---

**CameoDB** - High-performance distributed hybrid-search database built in Rust
