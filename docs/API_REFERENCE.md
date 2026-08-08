## �📡 HTTP API Reference

CameoDB provides a comprehensive REST API for document management, search, and system administration.

### 🔐 Authentication

Authentication is **off by default**. With `[security] enabled = true`
([configuration](CONFIGURATION.md#authentication-security)), every route below except the
health check requires a key:

```bash
curl -H "Authorization: Bearer cameo_v1_…" http://localhost:9480/_indexes
```

The header is the only accepted place for a key — a query parameter is not a credential and is
refused. Refusals are:

| Status | Meaning |
|--------|---------|
| `401 Unauthorized` | No key, or a key this node does not know. Carries `WWW-Authenticate: Bearer realm="cameodb"` |
| `403 Forbidden` | Authenticated, but the role or `allowed_indexes` do not cover this request |

Both return `{"error": …, "message": …}`. An unknown path answers `401` without a key and
`404` with one, so path probing tells an unauthenticated caller nothing.

#### Capability required per endpoint

| Method | Path | Capability |
|--------|------|-----------|
| `GET` | `/_cluster/health` | none (public) |
| `POST` | `/api/{index}/search` | `read` |
| `POST` | `/api/{index}/search/stream` | `read` |
| `GET` | `/api/{index}/_config` | `read` |
| `GET` | `/_indexes` | `read` |
| `GET` | `/_cluster/_indexes` | `read` |
| `PUT` | `/api/{index}/document` | `write` |
| `POST` | `/api/{index}/document/stream` | `write` |
| `POST` | `/api/{index}/_bulk` | `write` |
| `PUT` | `/api/{index}/_config` | `index-admin` |
| `PATCH` | `/api/{index}/_schema` | `index-admin` |
| `DELETE` | `/api/{index}` | `index-admin` |
| `GET` | `/_admin/memory` | `node-admin` |
| `POST` | `/_admin/memory/purge` | `node-admin` |
| `GET` | `/_admin/workers` | `node-admin` |
| `POST` | `/_admin/index/{index}/commit` | `node-admin` |
| `POST` | `/_admin/index/{index}/evict-writer` | `node-admin` |
| `POST` `GET` `DELETE` | `/mcp` | `read` at the endpoint, then per tool |
| `GET` `POST` | `/mcp/sse` | `read` at the endpoint, then per tool |
| `POST` | `/mcp/messages` | `read` at the endpoint, then per tool |

Roles bundle these: `admin` holds all four, `writer` holds `read` and `write`, `reader` holds
`read`. Where a path contains `{index}`, a key restricted with `allowed_indexes` is refused
unless that index is in its list.

**Health is the one public route, and its body depends on who is asking.** An anonymous
caller gets liveness only; presenting any valid key returns the full body:

```bash
# anonymous
{"status":"green"}

# with a key
{"status":"green","node_id":"…","active_shards":4,"cluster_enabled":false,…}
```

Node identity and cluster shape are free reconnaissance for anyone who can reach the port, and
a load balancer needs neither.

**Listings are filtered, not refused.** `/_indexes` and `/_cluster/_indexes` return only the
indexes the key may see, with `total_indexes` adjusted to match — a count over a shorter list
would itself disclose how many were withheld.

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

> **Count-Only Mode (`limit: 0`):** Pass `"limit": 0` (or inline `limit 0` in the query) to retrieve only the total hit count without any document data. The response will contain an empty `hits` array, but `total_hits`, `took_ms`, and shard statistics are still returned. This is significantly faster than a regular search because it skips document retrieval from the KV store entirely — only the Tantivy `Count` collector runs.
>
> ```bash
> curl -s -X POST http://localhost:9480/api/books/search \
>   -H "Content-Type: application/json" \
>   -d '{"query": "science fiction", "limit": 0}'
> ```
>
> **Response (count-only):**
> ```json
> {
>   "hits": [],
>   "hits_returned": 0,
>   "total_hits": 42,
>   "limit": 0,
>   "took_ms": 3
> }
> ```

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

#### Memory Statistics
Get process memory and jemalloc allocator statistics.

```bash
GET /_admin/memory
```

**Example:**
```bash
curl -s http://localhost:9480/_admin/memory
```

**Response (Linux with jemalloc):**
```json
{
  "process": {
    "vm_rss_kb": 46208,
    "vm_size_kb": 2150400,
    "rss_anon_kb": 39000,
    "rss_file_kb": 7208,
    "vm_data_kb": 123000,
    "threads": 12
  },
  "jemalloc": {
    "allocated": 33554432,
    "active": 41943040,
    "resident": 47349760,
    "metadata": 1048576,
    "retained": 8388608
  }
}
```

> **Note:** Fields vary by platform. macOS and Windows provide `vm_rss_kb`, `vm_size_kb`, and `threads`. Linux additionally provides `rss_anon_kb`, `rss_file_kb`, `rss_shmem_kb`, `vm_data_kb`, `vm_swap_kb`, and jemalloc-native stats (when jemalloc is enabled). Fields that cannot be determined on a platform are omitted.

#### Memory Purge
Trigger a jemalloc memory purge to return dirty pages to the OS.

```bash
POST /_admin/memory/purge?force=<bool>
```

**Parameters:**
- `force` (query, optional, default: `false`): When `true`, performs an aggressive purge that bypasses decay timers and immediately purges all dirty and muzzy pages. When `false`, uses decay-based purge respecting `dirty_decay_ms` / `muzzy_decay_ms`.

**Example (decay-based purge):**
```bash
curl -s -X POST http://localhost:9480/_admin/memory/purge
```

**Example (aggressive purge):**
```bash
curl -s -X POST 'http://localhost:9480/_admin/memory/purge?force=true'
```

**Response:**
```json
{
  "process": {
    "vm_rss_kb": 46208,
    "vm_size_kb": 2150400,
    "threads": 12
  },
  "process_after_purge": {
    "vm_rss_kb": 32100,
    "vm_size_kb": 2150400,
    "threads": 12
  },
  "jemalloc": {
    "allocated": 33554432,
    "active": 41943040,
    "resident": 32833536
  },
  "purge_result": 0
}
```

> **Note:** `purge_result` is `0` on success, non-zero jemalloc `mallctl` error code on failure. `process_after_purge` shows memory state after the purge. On non-Linux platforms, jemalloc and `purge_result` are omitted.

#### Index Commit
Force an index writer commit across all shards for the given index.

```bash
POST /_admin/index/{index}/commit
```

**Example:**
```bash
curl -s -X POST http://localhost:9480/_admin/index/books/commit
```

**Response:**
```json
{
  "index": "books",
  "shards_total": 4,
  "shards_committed": 4,
  "errors": []
}
```

#### Evict Index Writer
Evict the index writer from cache for the given index, freeing its memory.

```bash
POST /_admin/index/{index}/evict_writer
```

**Example:**
```bash
curl -s -X POST http://localhost:9480/_admin/index/books/evict_writer
```

**Response:**
```json
{
  "index": "books",
  "shards_total": 4,
  "writers_evicted": 4,
  "writers_missing": 0,
  "errors": []
}
```

