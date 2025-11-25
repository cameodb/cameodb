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
  "total": 42,
  "took_ms": 12
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
  "sequence_ids": [1002, 1003],
  "took_ms": 45
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
      "size_mb": 43.1,
      "tantivy_index_exists": true,
      "schema": {
        "shard_count": 256,
        "fields": {
          "title": {"name": "title", "field_type": "text", "indexed": true},
          "author": {"name": "author", "field_type": "text", "indexed": true}
        }
      }
    },
    {
      "name": "ted",
      "document_count": 4641,
      "total_size_bytes": 12458752,
      "size_mb": 11.9,
      "tantivy_index_exists": true,
      "schema": {
        "shard_count": 256,
        "fields": {}
      }
    }
  ],
  "total_indexes": 2,
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

- **Distributed**: Shared-nothing architecture with consistent hashing
- **Multi-tenant**: Complete isolation between indexes
- **Hybrid Storage**: ACID-compliant KV store + full-text search
- **High Performance**: Optimized batch operations and memory management
- **Rust-native**: Built for safety, performance, and concurrency

## 📈 Performance Features

- **Batch Processing**: Atomic bulk operations across shards
- **Smart Commits**: Memory-aware commit strategies
- **Consistent Hashing**: Optimal data distribution
- **Streaming Search**: Real-time result streaming for large queries
- **Schema Evolution**: Dynamic field addition and validation

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
