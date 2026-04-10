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

> **Sort results:** You can sort search results by a FAST field (u64 or date type) by either:
>
> 1. Supplying a sort specification in the payload: `"sort": {"field": "year", "order": "desc"}`
> 2. Embedding a `sort` clause at the end of the Tantivy query: `"query": "space opera sort year:desc"`
>
> Supported field types: `u64` and `date` (both must be marked as FAST). Order can be `asc` or `desc` (defaults to `desc`). The JSON payload always wins over inline `sort` clauses.

**Example with sort:**
```bash
curl -s -X POST http://localhost:9480/api/books/search \
  -H "Content-Type: application/json" \
  -d '{
    "query": "science fiction space",
    "sort": {"field": "publication_year", "order": "desc"},
    "limit": 10
  }'
```

**Example with inline sort:**
```bash
curl -s -X POST http://localhost:9480/api/books/search \
  -H "Content-Type: application/json" \
  -d '{
    "query": "science fiction space sort publication_year:desc limit 10"
  }'
```

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

