# CameoDB

A high-performance, distributed, shared-nothing hybrid-search database built in Rust. CameoDB combines the reliability of ACID-compliant key-value storage (redb) with the power of full-text search (Tantivy) in a multi-tenant, horizontally scalable architecture.

## 🚀 Quick Start

```bash
# Start the server
cargo run --bin server

# Server starts on http://localhost:9480 by default
```

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

## 🌐 Distributed Deployment

CameoDB supports true distributed deployment using Kameo's remote actor system:

### Quick Start - 3-Node Cluster (Docker Desktop)

#### **Deploy the Cluster**
```bash
# Deploy distributed CameoDB cluster
cd docker && docker-compose up -d --build

# Check all containers are running
docker-compose ps
```

#### **Docker Build Options**
```bash
# Build for specific architecture
docker build --platform linux/amd64 -t cameodb:latest .
docker build --platform linux/arm64 -t cameodb:latest .

# Build with different Rust version
docker build --build-arg RUST_VERSION=1.75 -t cameodb:latest .

# Multi-platform build
docker buildx build --platform linux/amd64,linux/arm64 -t cameodb:latest .
```

#### **Port Mapping Strategy**
```
External Access    Internal Service    Purpose
──────────────────────────────────────────────────
localhost:9481  → Node 1:9480       Direct node access
localhost:9482  → Node 2:9480       Direct node access  
localhost:9483  → Node 3:9480       Direct node access
localhost:80    → Load Balancer     Production access
─────────────────────────────────────────────  
9581            → 9580           Node 1 Cluster
9582            → 9580           Node 2 Cluster  
9583            → 9580           Node 3 Cluster
```

#### **Simple Load Balancer**
Uses nginx with inline configuration for external ports:
```nginx
upstream cameodb_cluster {
    server host.docker.internal:9481;  # External port to Node 1
    server host.docker.internal:9482;  # External port to Node 2
    server host.docker.internal:9483;  # External port to Node 3
}
```

#### **Access Patterns**
```bash
# Direct node access
curl http://localhost:9481/api/books/search -d '{"query": "distributed systems"}'  # Node 1
curl http://localhost:9482/api/books/search -d '{"query": "rust programming"}'     # Node 2
curl http://localhost:9483/api/books/search -d '{"query": "database design"}'     # Node 3

# Load-balanced access (automatically distributed across all 3 nodes)
curl http://localhost/api/books/search -d '{"query": "microservices"}'
curl http://localhost/_health
curl http://localhost/api/books/_bulk -d '[{"id":"1","title":"Test"}]'
```

### Configuration Example
```toml
[cluster]
# Enable distributed actor system
distributed_actors = true
# Cluster communication port (aligned with gRPC best practices)
cluster_port = 9580
# Bootstrap nodes for discovery
bootstrap_nodes = ["node1.cameodb.com:9580", "node2.cameodb.com:9580"]
# Enable local network discovery
mdns_discovery = true
# Cluster isolation name
cluster_name = "cameodb-production"
```

### Key Benefits
- **🎯 Zero-Configuration Networking**: Actors automatically discover each other
- **🔄 Transparent Routing**: Same API works for local and remote shards
- **⚡ Automatic Failover**: Failed nodes are automatically excluded
- **📈 Dynamic Scaling**: Add/remove nodes without downtime
- **🏷️  Service Discovery**: DHT + mDNS for automatic peer discovery

### Development Workflow

#### **Testing Individual Nodes**
```bash
# Test each node individually
for port in 9481 9482 9483; do
  echo "Testing Node on port $port:"
  curl -s http://localhost:$port/_health | jq
done
```

#### **Load Testing**
```bash
# Test load balancing across all nodes
for i in {1..10}; do
  curl -s http://localhost/api/books/search -d '{"query": "test", "limit": 1}' | jq '.results | length'
done
```

#### **Container Management**
```bash
# Follow logs for debugging (from docker folder)
cd docker && docker-compose logs -f

# Restart specific node
docker-compose restart cameodb-node2

# Stop all containers
docker-compose down

# Full cleanup (removes volumes)
docker-compose down -v
```

### Resource Requirements (Docker Desktop)

- **CPU**: 4+ cores recommended (each node gets 1/3 allocation)
- **Memory**: 4GB+ recommended (512MB per node minimum)
- **Storage**: 10GB+ available space
- **Network**: Uses `172.20.0.0/16` subnet with mDNS discovery

### Troubleshooting

#### **Port Conflicts**
```bash
# Check if ports are in use
lsof -i :9481 -i :9482 -i :9483 -i :80

# Kill conflicting processes
sudo lsof -ti:9481 | xargs kill -9
```

#### **Container Health Checks**
```bash
# Check container status (from docker folder)
cd docker && docker-compose ps

# Individual health endpoints
curl http://localhost:9481/_health  # Node 1
curl http://localhost:9482/_health  # Node 2  
curl http://localhost:9483/_health  # Node 3
```

This configuration provides a seamless distributed CameoDB experience optimized for Docker Desktop development on macOS!

## 🔧 Configuration

Server configuration via `cameodb.toml`:

```toml
[server.http]
host = "0.0.0.0"
port = 9480

[search]
writer_memory_min_mb = 16
writer_memory_max_mb = 256
default_batch_size = 1000

[storage]
data_paths = ["./cameodb-data"]
wal_sync = true
```

## 📚 Documentation

- [Architecture Overview](ARCHITECTURE.md)
- [Configuration Guide](docs/CONFIGURATION.md)
- [Contributing Guidelines](docs/CONTRIBUTING.md)
- [Ingestion Examples](scripts/examples/README.md)

## 🚀 Getting Started

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd cameodb
   ```

2. **Build and run**
   ```bash
   cargo run --bin server
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

[Add your license information here]

---

**CameoDB** - High-performance distributed hybrid-search database built in Rust
