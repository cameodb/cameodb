# 📡 HTTP API Reference

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
| `DELETE` | `/api/{index}/document` | `write` |
| `POST` | `/api/{index}/document/stream` | `write` |
| `POST` | `/api/{index}/_bulk` | `write` |
| `POST` | `/api/{index}/_bulk/delete` | `write` |
| `PUT` | `/api/{index}/_config` | `index-admin` |
| `PATCH` | `/api/{index}/_schema` | `index-admin` |
| `DELETE` | `/api/{index}` | `index-admin` |
| `GET` | `/_admin/memory` | `node-admin` |
| `POST` | `/_admin/memory/purge` | `node-admin` |
| `GET` | `/_admin/workers` | `node-admin` |
| `GET` | `/_admin/audit` | `node-admin` |
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
>
> A field list needs a comma between names, and the clause must sit in one run of `return`/`limit`/`offset`/`sort` at the end of the query with query text in front of it. Anywhere else the keyword is searched for as a word, so `find tax return forms` is four terms. A `return` naming a field the index does not have is reported in `_discarded_clauses`.

> **Paging (`offset`):** `limit` is the page size and `offset` is where the page starts, so page N
> is `offset = N * limit`. Supply it in the payload — `"offset": 20` — or inline, as
> `"query": "space opera limit 10 offset 20"`; the payload wins. The response reports both, and
> there are more results when `offset + hits_returned < total_hits`.
>
> **The ceiling applies to `offset + limit`, not to either alone.** A page is served by asking every
> shard for the whole window from the front and taking the slice after merging them — the skip
> cannot be pushed down into a shard, because every hit on the page may have come from one of them.
> So the node fetches `offset + limit` hits however small the page is, and
> `[security.limits] max_search_limit` bounds that sum: a request past it is refused with `400`
> naming the window and the bound. An `offset` with no `limit` still counts the node's default limit
> against the ceiling.
>
> A page needs an order to be a page of. With a `sort` on a `sortable` field the order is total and
> pages are consecutive slices of it; without one the order is by relevance, which is stable for one
> query against unchanged data but not across writes. Paging is not a cursor and does not snapshot
> the index. Do not page through an approximate sort — see below.
>
> Paging past the end returns an empty `hits` with `total_hits` intact, rather than an error.
>
> ```bash
> curl -s -X POST http://localhost:9480/api/books/search \
>   -H "Content-Type: application/json" \
>   -d '{"query": "science fiction", "limit": 10, "offset": 20, "sort": {"field": "publication_year", "order": "desc"}}'
> ```

> **Sort results:** You can sort search results by either:
>
> 1. Supplying a sort specification in the payload: `"sort": {"field": "year", "order": "desc"}`
> 2. Embedding a `sort` clause at the end of the Tantivy query: `"query": "space opera sort year:desc"`
>
> Sorting is exact on a field the built index has a fast column for, which `GET /api/{index}/_config` reports per field as `sortable`. A numeric or date field needs one to be sorted at all. A text or string field without one is sorted *approximately* rather than refused: the top `2 × limit` matches by relevance are collected and then ordered alphabetically, so the result is not the alphabetically first documents in the index. Such a response carries `_approximate_sort` naming the field — do not page through one, since each page orders a different set of candidates and the pages are not slices of a single order.
>
> The column is written when the index is built, so `sortable` cannot be turned on for an index that already holds data: declare the field `fast` in the schema before writing to it. `fast` is the declaration and `sortable` is whether the built index carries it; the two differ for a field declared after the fact.
>
> Order is `asc` unless `desc` is given, and an inline order must be exactly one of those words. The JSON payload always wins over inline `sort` clauses, and a `sort` naming a field the index does not have is reported in `_discarded_clauses`.

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

> **Dropped clauses (`_discarded_clauses`):** a clause the query parser cannot interpret is
> dropped and whatever is left runs. In a conjunction that widens the result set; in a
> disjunction it narrows it; in a negation it disables the exclusion. None of that is visible in
> the hits, so every dropped clause is reported:
>
> ```json
> {
>   "hits": [ /* ... */ ],
>   "total_hits": 42,
>   "_discarded_clauses": [
>     "unknown field 'athor' — the clause naming it was dropped, so this result set does not match what the query asked for"
>   ]
> }
> ```
>
> The key is absent on a clean parse rather than present and empty, so testing for its presence is
> enough. Execution stays lenient here — the hits are returned alongside the note, because a person
> reading a result page can see it. The one exception is a query with *nothing* left after the
> drop, which is refused with `400` instead: there are no hits to attach the note to, and the zero
> it would otherwise report cannot be told apart from a search that ran and matched nothing. The MCP tools make the opposite choice and fail the call, since
> an agent presents rows as fact. Causes include an unknown or unindexed field name, a value that
> does not match its field's type, an unsupported form such as `field:*`, a lowercase `to` or `in`
> where the keyword was meant, and an inline `return` or `sort` naming a field the index does not
> have.
>
> **Keyword case:** `AND`, `OR`, `NOT`, `TO` and `IN` are keywords in uppercase only, and lowercase
> is query text. `to` and `in` break the clause around them, so they surface as dropped clauses
> above. `and`, `or` and `not` do not: they are searched for as ordinary words, which widens a query
> silently — `a not b` matches everything rather than excluding `b`.

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

> **No `offset` here.** A stream carries the whole result as it is produced, so there is no page to
> skip to, and a non-zero `offset` on this route is refused with `400` rather than ignored — a
> silently dropped offset would return the first page for every page requested. Use
> `POST /api/{index}/search` for a page, or read the stream and skip what you do not want.
> `"offset": 0` is accepted, so a client that always sends the field needs no special case.
> `limit` applies as usual and is bounded by `max_search_limit`.

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

#### Delete Single Document
Remove one document by its key.

```bash
DELETE /api/{index}/document?id=<id>[&routing_key=<key>]
```

**Example:**
```bash
curl -s -X DELETE "http://localhost:9480/api/books/document?id=book_001"
```

**Response:**
```json
{
  "id": "book_001",
  "result": "deleted",
  "version": 1042,
  "shard_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
}
```

#### Bulk Delete Documents
Remove many documents in a single request.

```bash
POST /api/{index}/_bulk/delete
```

Each entry is either a bare id or an object naming its routing key, and the two may be mixed:

**Example:**
```bash
curl -s -X POST http://localhost:9480/api/books/_bulk/delete \
  -H "Content-Type: application/json" \
  -d '["book_002", {"id": "book_003", "routing_key": "acme"}]'
```

**Response:**
```json
{
  "items_received": 2,
  "items_deleted": 2,
  "errors": [],
  "duration_ms": 3
}
```

An id that cannot be routed is reported in `errors` against that id; the rest of the batch is still applied.

#### What deletion promises

**Deleting is idempotent.** An id the index does not hold is answered as `deleted`, the same way writing over an existing document is answered as `created`. A *missing index* is different: it is a 404, and the delete creates nothing on its way to saying so.

**Visibility has two tiers, because two engines answer.** The document body lives in the key-value store and the searchable terms live in the index:

| Query | When the delete shows |
|---|---|
| `id:VALUE` | **Immediately.** An exact key lookup is answered from the key-value store without consulting the search index. |
| Any content query | **At the next commit** — the idle-commit timeout (`supervisor_timeout_secs`, 5 s by default), sooner under load, or at once via `POST /_admin/index/{index}/commit`. |

Until that commit, a deleted document is counted in `total_hits` but absent from `hits`: the count comes from the search index while the bodies come from the key-value store. This is the honest reading of a mid-flight delete — reducing the count instead would break the arithmetic that pages through results.

**Routing.** On a normal index, and on one with a shadow key such as `sha1`, the id routes the delete on its own and `routing_key` is unnecessary. On an index that routes by some *other* field — a tenant, a customer — the id does not say which shard holds the row, so the delete must carry the same `routing_key` the write used. Without it the request is refused with a `400` naming the field. Its value is on the document, one search away:

```bash
curl -s -X POST http://localhost:9480/api/orders/search \
  -H "Content-Type: application/json" -d '{"query": "id:order_77", "limit": 1}'
# → read tenant_id off the hit, then:
curl -s -X DELETE "http://localhost:9480/api/orders/document?id=order_77&routing_key=acme"
```

Deleting *by query* is not available; the ids have to be named.

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
    "description": "Library catalogue, one document per edition.",
    "fields": {
      "title": {
        "name": "title",
        "field_type": "text",
        "indexed": true,
        "description": "Title as printed on the edition."
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

**Descriptions.** `description` is optional on the index and on each field, and is the only part
of a schema that says what the data *is* rather than how it is shaped — field names and types
describe the shape. Nothing infers one; it is carried verbatim to every reader, including the
discovery tools an agent uses to choose an index. An index description may be up to 512
characters and a field description up to 200, counted in characters rather than bytes; a longer
one is refused with `400` naming the offending field, since a description truncated mid-sentence
still reads as the whole statement. A blank description is stored as no description, and an
absent one is omitted from the schema entirely rather than serialised as null.

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
  "name": "books",
  "description": "Library catalogue, one document per edition.",
  "field_count": 3,
  "fields": [
    {"name": "id", "type": "text", "indexed": true, "stored": true, "fast": false, "shadow": false, "searchable": true, "sortable": false},
    {"name": "author", "type": "text", "indexed": true, "stored": false, "fast": false, "shadow": false, "searchable": true, "sortable": false},
    {"name": "title", "type": "text", "indexed": true, "stored": false, "fast": false, "shadow": false, "searchable": true, "sortable": false,
     "description": "Title as printed on the edition."}
  ]
}
```

Every field carries the same keys, in the same spelling, here and in `GET /_indexes` — one index
has one description. `fields` is ordered with `id` first, then alphabetically.

| Key | Meaning |
|-----|---------|
| `type` | Lowercase, and the same name the query syntax reference uses — `text`, `date`, `i64` |
| `indexed` | What the schema **declares** |
| `searchable` | Whether the built index can actually match on it. Differs from `indexed` for a field declared after the index was built — see `PATCH /api/{index}/_schema` |
| `sortable` | Whether the built index can sort on it exactly. The same distinction `searchable` draws, for the fast column a sort orders on: `fast` is the declaration, this is whether the column exists. A numeric sort on a field that is `fast` but not `sortable` fails; a text sort on one returns an approximate order |
| `fast` | Required to sort on a numeric or date field |
| `shadow` | The field carries the identifier under its original name; queried through `id` |
| `stored` | Kept in the search index as well as the document store |

#### Change Field Indexing Flags
Turn a field's `indexed` flag on or off on an existing schema.

```bash
PATCH /api/{index}/_schema
```

**Example:**
```bash
curl -s -X PATCH http://localhost:9480/api/books/_schema \
  -H "Content-Type: application/json" \
  -d '{"field_updates": {"publication_year": false}}'
```

**Response:**
```json
{
  "acknowledged": true,
  "index": "books",
  "updated_fields": ["publication_year"],
  "unchanged_fields": []
}
```

A name no shard recognises refuses the whole request with `409`, and nothing is written. An
empty `field_updates` is a `400`.

**Declaring a field that the index cannot search yet.** A field is only searchable if the
Tantivy index has a column for it, and that is fixed when the index is built. Fields present on
the **first** write are indexed then; a field that first appears in a **later** document is
recorded non-indexed, so marking it `indexed` does not make it searchable immediately. The edit
is still accepted, because the stored schema is the *declaration* the index gets rebuilt from —
so this is the first step of making the field searchable, not a mistake. Such fields come back
under `pending_reindex_fields`:

```json
{
  "acknowledged": true,
  "index": "books",
  "updated_fields": ["notes"],
  "unchanged_fields": [],
  "pending_reindex_fields": ["notes"],
  "note": "Marked indexed and saved, but not searchable yet: notes. …"
}
```

To complete it, rebuild the index data from the schema — delete the index data *without*
deleting the schema, then re-ingest:

```bash
curl -s -X PATCH http://localhost:9480/api/books/_schema \
  -H "Content-Type: application/json" \
  -d '{"field_updates": {"notes": true}}'

curl -s -X DELETE "http://localhost:9480/api/books?delete_schema=false"
# re-ingest, and `notes` is now searchable
```

Until the rebuild, a query naming the field matches nothing — and says so: the search reports it
under `_discarded_clauses`, and the MCP tools refuse such a search outright rather than returning
a narrower answer as though it were complete.

To avoid the situation entirely, declare fields up front with `PUT /api/{index}/_config`, or make
sure the first document written carries every field you intend to search.

Demoting a field (`true` → `false`) takes effect on the next write; documents already indexed
under it stay searchable until the index is rebuilt.

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
      "description": "Library catalogue, one document per edition.",
      "document_count": 16559,
      "index_size_bytes": 45231680,
      "memory_bytes": 47316480,
      "shard_count": 4,
      "warm_shards": 4,
      "field_count": 3,
      "fields": [
        {"name": "id", "type": "text", "indexed": true, "stored": true, "fast": false, "shadow": false, "searchable": true},
        {"name": "publication_date", "type": "date", "indexed": true, "stored": false, "fast": true, "shadow": false, "searchable": true},
        {"name": "title", "type": "text", "indexed": true, "stored": false, "fast": false, "shadow": false, "searchable": true}
      ]
    }
  ],
  "total_indexes": 1,
  "total_shards": 4,
  "node_id": "550e8400-e29b-41d4-a716-446655440000",
  "node_name": "node-a",
  "took_ms": 3
}
```

The listing describes each index in full, so learning a field's type costs no extra request.
Field entries are exactly those of `GET /api/{index}/_config`.

Sizes are **bytes**, not megabytes: `GET /_cluster/_indexes` sums them across nodes, and summing
values already rounded to whole megabytes lost up to a megabyte per node. `data_size_bytes` and
`total_size_bytes` appear only with `?data_size=true`, since measuring the document store is the
expensive half of the call.

`GET /_cluster/_indexes` returns the same per-index shape, plus `nodes_contacted`, `nodes_failed`
and a `nodes[]` array carrying each node's own listing.

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
POST /_admin/index/{index}/evict-writer
```

**Example:**
```bash
curl -s -X POST http://localhost:9480/_admin/index/books/evict-writer
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

#### Audit Trail
Return the most recent audit records, newest first. Requires `[security.audit] enabled = true`;
on a node without it, `records` is empty and `enabled` is `false`.

```bash
GET /_admin/audit?limit=<1..1000>       # default 100
```

**Example:**
```bash
curl -s -H "Authorization: Bearer $ADMIN_KEY" \
  'http://localhost:9480/_admin/audit?limit=50'
```

**Response:**
```json
{
  "enabled": true,
  "dropped": 0,
  "count": 3,
  "records": [
    {
      "ts": "2026-08-10T09:14:02.331Z",
      "event": "http",
      "outcome": "allowed",
      "key_id": "k_1c8e",
      "label": "analyst",
      "role": "reader",
      "peer": "10.0.4.19",
      "method": "POST",
      "path": "/api/customers/search",
      "index": "customers",
      "status": 200
    },
    {
      "ts": "2026-08-10T09:14:00.000Z",
      "event": "write_stats",
      "outcome": "allowed",
      "key_id": "k_7f3a",
      "label": "ingest-pipeline",
      "role": "writer",
      "index": "docs",
      "ops": 48213,
      "errors": 2,
      "window_start": "2026-08-10T09:13:50.000Z"
    },
    {
      "ts": "2026-08-10T09:13:58.104Z",
      "event": "mcp_tool",
      "outcome": "denied",
      "key_id": "k_1c8e",
      "label": "analyst",
      "role": "reader",
      "tool": "search_index",
      "index": "payroll",
      "reason": "this key is not permitted on index 'payroll'"
    }
  ]
}
```

`dropped` is the running count of records lost to a full writer queue since the node started;
a non-zero value means the window shown is incomplete. Reads, MCP tool calls, admin actions
and refusals of a valid key are recorded individually; writes, health checks and
unauthenticated refusals arrive as counted `*_stats` records. Reading this endpoint is itself
recorded. Field-by-field detail is in [CONFIGURATION.md](CONFIGURATION.md).
