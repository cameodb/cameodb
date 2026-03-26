# CameoDB MCP Server

Model Context Protocol (MCP) server implementation for CameoDB, enabling AI agents to efficiently search and explore indexed datasets.

## Overview

The `cameodb_mcp` crate provides a standards-compliant MCP server that exposes CameoDB's search capabilities as tools for AI agents. It implements the [Model Context Protocol](https://modelcontextprotocol.io) specification (version 2024-11-05) using HTTP/SSE transport.

### Architecture

- **Shared-Port Design**: MCP endpoints are nested under `/mcp` in the main CameoDB HTTP server
- **No Separate Process**: Runs in the same binary as CameoDB, sharing the same `AppState` and actor system
- **Transport**: HTTP/SSE with strict MCP spec compliance (2024-11-05)
- **Protocol**: JSON-RPC 2.0 over SSE with proper event types
- **Session Management**: Automatic session registry with 5-minute timeout cleanup
- **Asynchronous Processing**: Non-blocking POST requests with background task execution

### Key Features

- ✅ **6 MCP Tools** for search, metadata, and query validation
- ✅ **4 Resource Providers** for index exploration
- ✅ **Field-Type-Aware Query Validation** with syntax reference
- ✅ **Federated Search** across multiple indexes
- ✅ **Per-Index Field Projection** for efficient data retrieval
- ✅ **Read-Only Operations** (all tools are annotated as `readOnlyHint: true`)
- ✅ **MCP Spec Compliant** (2024-11-05) with proper SSE event handling
- ✅ **Asynchronous Processing** - non-blocking POST with 202 Accepted response
- ✅ **Automatic Session Cleanup** with configurable timeout (5 minutes)
- ✅ **Self-Contained Schema Discovery** — every index response includes per-field query hints

### Self-Contained Discovery

CameoDB is designed as a self-contained document store where indexed fields, schemas, and data types drive automatic agent adaptation. When new indexes are created or fields evolve, the MCP tools **automatically reflect the changes** — no configuration or manual updates needed.

**Agent workflow (no prior knowledge required):**

1. **`list_indexes`** → Discover all indexes with schemas and per-field `query_hint` (what operators work with each field type)
2. **`get_index`** → Deep-dive into a specific index: field definitions, types, stats, and `queryable_fields` with operator guidance
3. **`search_index`** → Construct queries using the field names and operators learned from the schema
4. **`validate_query`** *(optional)* → Get structural validation, typo detection ("did you mean?"), and the full syntax reference with operator-by-field-type matrix

Each `queryable_fields` entry tells the agent exactly what it can do:
- A `text` field → phrases, slop, prefix, IN set, boost, range
- A `date` field → exact date, comparisons (>/<), ranges
- A `numeric` field → exact, ranges (inclusive/exclusive), boost
- A `boolean` field → true/false only

This means an agent can go from zero knowledge to well-formed queries in **two tool calls** (`list_indexes` → `search_index`), with the schema metadata providing all the guidance needed for operator selection.

## MCP Tools

All tools follow MCP naming conventions (verb-first `snake_case`) and include `title`, property descriptions, and annotations.

### 1. `search_index`

Execute full-text search on a single CameoDB index.

**Parameters:**
- `index` (string, required): Name of the CameoDB index to search
- `query` (string, required): Search query string. Supports:
  - Field targeting: `title:rust`
  - Phrase queries: `title:"rust programming"`
  - Phrase slop (proximity): `body:"small bike"~2`
  - Phrase prefix: `"big bad wo"*`
  - Boolean operators: `title:rust AND author:doe` (AND, OR, NOT — UPPERCASE)
  - Must/must-not: `+title:rust -author:smith`
  - Grouping: `(title:rust OR title:go) AND year:[2020 TO 2024]`
  - Range queries: `year:[2020 TO 2024]` (inclusive `[]`, exclusive `{}`)
  - Set operator: `status: IN [active pending review]`
  - Boosting: `title:rust^3 OR body:rust`
  - Date comparisons: `created_at:>2024-01-01`
  - All docs: `*`
  - Inline modifiers: `title:rust return title,author limit 5`
- `limit` (integer, optional): Maximum number of results to return
- `fields` (array of strings, optional): Field names to include in results (field projection)

**Returns:** JSON array of matching documents with relevance scores.

**Example:**
```json
{
  "name": "search_index",
  "arguments": {
    "index": "papers",
    "query": "machine learning AND year:[2020 TO 2024]",
    "limit": 10,
    "fields": ["title", "author", "year"]
  }
}
```

### 2. `search_indexes`

Execute federated search across multiple CameoDB indexes with optional per-index field projection.

**Parameters:**
- `indexes` (array, required): List of indexes to search, each with:
  - `index` (string, required): Name of the CameoDB index
  - `fields` (array of strings, optional): Fields to include from this index
- `query` (string, required): Search query applied to all specified indexes
- `limit` (integer, optional): Maximum total results across all indexes

**Returns:** Combined results merged by relevance score. Each hit includes an `_index_source` field indicating its origin index.

**Example:**
```json
{
  "name": "search_indexes",
  "arguments": {
    "indexes": [
      {"index": "papers", "fields": ["title", "author"]},
      {"index": "books", "fields": ["title", "isbn"]}
    ],
    "query": "rust programming",
    "limit": 20
  }
}
```

### 3. `get_index`

Retrieve schema and statistics for a single CameoDB index.

**Parameters:**
- `index` (string, required): Name of the CameoDB index

**Returns:** Complete field definitions, types, document count, size, metadata, and a `queryable_fields` array with per-field `query_hint` showing exactly which operators work with each field's data type.

**Example:**
```json
{
  "name": "get_index",
  "arguments": {
    "index": "papers"
  }
}
```

**Response includes `queryable_fields`:**
```json
"queryable_fields": [
  {
    "field": "id",
    "type": "text",
    "query_hint": "Exact match (no tokenization). Supports: field:exact_value, field: IN [val1 val2]..."
  },
  {
    "field": "title",
    "type": "text",
    "query_hint": "Tokenized full-text. Supports: field:term, field:\"phrase\", field:\"phrase\"~N (slop)..."
  },
  {
    "field": "year",
    "type": "i64",
    "query_hint": "Numeric field. Supports: field:value, field:[low TO high], field:{low TO high}..."
  }
]
```

### 4. `list_indexes`

List all available CameoDB indexes with their schemas and metadata. **New indexes are automatically available here** — no configuration needed.

**Parameters:** None

**Returns:** All index schemas with metadata (document counts, field definitions, sizes). Each index includes a `queryable_fields` array with per-field type and `query_hint`.

**Example:**
```json
{
  "name": "list_indexes",
  "arguments": {}
}
```

### 5. `validate_query`

Validate and get guidance on CameoDB search query syntax. This is the **primary syntax guide** for agents.

**Parameters:**
- `index` (string, optional): Index name for schema-aware field validation
- `partial_field` (string, optional): Partial field name for autocomplete suggestions
- `query` (string, optional): Query string to validate and analyze

**Usage patterns for agents:**
1. **No arguments**: Returns complete query syntax reference with operator-by-field-type compatibility matrix
2. **Index only**: Returns schema-aware field list with type-specific operator hints per field
3. **Index + partial_field**: Returns autocomplete suggestions matching available fields
4. **Index + query**: Returns structural validation, field recognition, typo detection, and per-field operator guidance

**Returns:**
- `syntax_reference`: Full query syntax documentation with all operators, examples, and field-type compatibility
- `available_fields`: Schema fields with types, indexed status, and per-field query hints
- `field_suggestions`: Autocomplete matches for partial field names
- `query_analysis`: Structural validation, recognized/unknown fields, warnings, and suggestions
- `searchable_field_names`: List of all queryable field names

**Example:**
```json
{
  "name": "validate_query",
  "arguments": {
    "index": "papers",
    "query": "titel:rust AND author:doe"
  }
}
```

**Response includes:**
- `warnings`: `["Unknown field 'titel'. Did you mean: title?"]`
- `suggestions`: Field corrections and syntax tips
- `field_hints`: Type-specific query guidance (e.g., "text supports phrases, slop, prefix, IN set, boost, range")
- `syntax_reference`: Complete query syntax with operator-by-field-type matrix

### 6. `get_index_stats`

Return statistics for a single CameoDB index or aggregated statistics across all indexes.

**Parameters:**
- `index` (string, optional): Index name. If omitted, returns aggregated statistics for all indexes.

**Returns:**
- Single index: document count, field count, field names, size, metadata
- All indexes: total documents, total size, total fields, per-index breakdown

**Example:**
```json
{
  "name": "get_index_stats",
  "arguments": {
    "index": "papers"
  }
}
```

## MCP Resources

CameoDB exposes indexes as MCP resources for exploration via `resources/list` and `resources/read`.

### Resource URIs

- `cameodb://indexes` — Index catalog (all indexes with schemas)
- `cameodb://indexes/{index}` — Single index metadata
- `cameodb://indexes/{index}/schema` — Index schema only
- `cameodb://indexes/{index}/stats` — Index statistics only

**Example:**
```json
{
  "method": "resources/read",
  "params": {
    "uri": "cameodb://indexes/papers/schema"
  }
}
```

## CameoDB Query Syntax Reference

The `validate_query` tool returns a comprehensive syntax reference with an operator-by-field-type compatibility matrix. Below is the full reference.

### Basic Search
```
rust database              # AND by default: matches docs with both terms
machine learning           # searches all default indexed text fields
```

> **Note**: `field:term` only applies to the term immediately after the colon. `body:rust programming` searches `rust` in body, `programming` in default fields.

### Field-Targeted Search
```
title:rust
author:doe
body:rust programming      # only 'rust' targets body
```

### Phrase Queries
```
title:"rust programming"         # exact phrase (terms in order)
description:"machine learning"
title:"Barack Obama"
```

### Phrase Slop (Proximity)
```
body:"small bike"~1        # matches 'small blue bike' (1 word between)
body:"small bike"~3        # matches 'small, rusty, and yellow bike'
title:"big wolf"~1         # transposition costs 2: "A B"~1 does NOT match "B A"
```

### Phrase Prefix
```
"big bad wo"*              # matches 'big bad wolf' (* applies to last term)
"rust prog"*               # matches 'rust programming'
```

### Boolean Operators
```
title:rust AND author:doe           # both required
title:rust OR title:go              # either matches
title:rust NOT author:smith         # exclude
a AND b OR c                        # parsed as: (a AND b) OR c
```

> **Note**: AND, OR, NOT must be UPPERCASE. AND takes precedence over OR.

### Must / Must-Not Operators
```
+rust +database                     # equivalent to rust AND database
apple -fruit                        # apple required, fruit excluded
+title:rust -author:smith
(+title:rust +year:[2020 TO 2024]) author:doe   # author optional, boosts score
```

### Grouping
```
(title:rust OR title:go) AND year:[2020 TO 2024]
(color:red OR color:green) AND size:large
(+title:rust +author:doe) OR title:"systems programming"
```

### Range Queries
```
year:[2020 TO 2024]        # inclusive both bounds []
score:{0 TO 100}           # exclusive both bounds {}
title:[a TO c}             # mixed: inclusive lower, exclusive upper
price:[10.0 TO *]          # unbounded upper
age:[* TO 30]              # unbounded lower
```

### Set Operator (IN)
```
status: IN [active pending review]   # more CPU-efficient than OR-ing
color: IN [red green blue]
category: IN [rust go python]
```

> **Note**: Must specify field. `title: IN [a b c]` is more efficient than `title:a OR title:b OR title:c`.

### Boosting
```
"SRE"^2.0 OR devops^0.4                        # boost SRE over devops
title:rust^3 OR body:rust                       # boost title matches
title:"machine learning"^2.5 OR description:"deep learning"
```

> Default boost is 1.0. No negative boosts allowed. Boost affects ranking, not filtering.

### All-Docs Query
```
*                          # matches every document
* limit 10                 # all docs, limited to 10 results
```

### Date Queries
```
created_at:2024-01-15                              # exact date
created_at:>2024-01-01                             # after
created_at:<2024-12-31                             # before
created_at:>=2024-06-01                            # on or after
created_at:<=2024-06-30                            # on or before
created_at:[2024-01-01 TO 2024-12-31]              # inclusive range
timestamp:[2024-01-01T00:00:00Z TO 2024-01-02T00:00:00Z}  # exclusive upper
```

> Accepts YYYY-MM-DD or full RFC3339 (e.g. `2024-01-15T10:30:00Z`). Dates are auto-normalized internally.

### Exact ID Lookup
```
id:my-document-id
id:doc-12345
```

### Escape Characters
```
title:C\+\+               # escape special characters with backslash
name:O\'Brien
field:hello\ world         # escape space
```

Reserved characters: `+ ^ ` `: { } " [ ] ( ) ~ ! \ * SPACE`

### Inline Modifiers (CameoDB-specific)
```
title:rust return title,author,year               # field projection
title:rust limit 5                                 # result limit
title:rust AND author:doe return title,author limit 10  # combined
```

### Field Type ↔ Operator Compatibility

| Operator | text | string/exact | numeric | date | boolean | ip | json | facet |
|---|---|---|---|---|---|---|---|---|
| `field:term` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `field:"phrase"` | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | — | ❌ |
| `"phrase"~N` (slop) | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | — | ❌ |
| `"phrase"*` (prefix) | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | — | ❌ |
| `field:[a TO z]` (range) | ✅ | ❌ | ✅ | ✅ | ❌ | ✅ | — | ❌ |
| `field:>val` (comparison) | ❌ | ❌ | ❌ | ✅ | ❌ | ❌ | — | ❌ |
| `field: IN [a b]` (set) | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ | — | ❌ |
| `term^boost` | ✅ | ❌ | ✅ | ❌ | ❌ | ❌ | — | ❌ |
| `AND/OR/NOT` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `+/-` (must/must-not) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |

## Client Configuration

**Note**: CameoDB defaults to port `9480`. You can change this in the configuration file (`[network.http] port = 9480`) or via command line arguments.

### Claude Desktop

Add to `~/Library/Application Support/Claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "cameodb": {
      "command": "curl",
      "args": [
        "-N",
        "-H", "Accept: text/event-stream",
        "http://localhost:9480/mcp/sse"
      ],
      "env": {}
    }
  }
}
```

**Note**: Claude Desktop uses stdio transport. For HTTP/SSE, you'll need a bridge or use the SSE endpoint directly via the MCP Inspector.

### Windsurf

Add to Windsurf MCP settings (`.windsurf/mcp.json` or via Settings → MCP):

```json
{
  "mcpServers": {
    "cameodb": {
      "url": "http://localhost:9480/mcp/sse",
      "transport": "sse"
    }
  }
}
```

### Cursor

Add to Cursor MCP configuration:

```json
{
  "mcpServers": {
    "cameodb": {
      "url": "http://localhost:9480/mcp/sse",
      "transport": "sse"
    }
  }
}
```

### MCP Inspector (Testing)

For testing and debugging, use the [MCP Inspector](https://github.com/modelcontextprotocol/inspector):

```bash
npx @modelcontextprotocol/inspector http://localhost:9480/mcp/sse
```

## Implementation Details

### Transport Layer

The MCP server uses HTTP/SSE transport with strict MCP specification compliance:

- **SSE Endpoint**: `GET /mcp/sse` — Establishes SSE connection and emits `endpoint` event
- **Message Endpoint**: `POST /mcp/messages?session_id={id}` — Receives JSON-RPC messages (returns 202 Accepted)

### MCP Specification Compliance

The implementation follows the official MCP HTTP/SSE transport specification (version 2024-11-05):

#### SSE Handshake
1. Client connects to `/mcp/sse`
2. Server emits `endpoint` event with POST endpoint URL: `event: endpoint\ndata: /mcp/messages?session_id=xxx`
3. Client uses this URL for message posting

#### Asynchronous Message Processing
1. Client POSTs JSON-RPC message to `/mcp/messages?session_id=xxx`
2. Server immediately returns `202 Accepted` (non-blocking)
3. Server processes message in background task
4. Server sends response as `message` event: `event: message\ndata: {json-rpc-response}`

### Session Management

- Sessions are created on SSE connection with unique ID: `mcp-session-{counter}`
- Server emits structured `Event` objects (not raw strings) for proper MCP compliance
- Sessions are automatically cleaned up after 5 minutes of inactivity
- Keepalive messages sent every 15 seconds to maintain connection

### JSON-RPC Methods

The server implements these JSON-RPC methods:

- `initialize` — Capability negotiation
- `ping` — Health check
- `notifications/initialized` — Client initialization complete (no response)
- `notifications/cancelled` — Task cancellation (no response)
- `tools/list` — List available tools
- `tools/call` — Invoke a tool
- `resources/list` — List available resources
- `resources/read` — Read a resource

### Error Handling

All errors are mapped to JSON-RPC error codes:

- `-32600`: Invalid JSON-RPC request
- `-32601`: Method not found
- `-32602`: Invalid params
- `-32603`: Internal error (backend failures)

### Backend Trait

The `McpBackend` trait defines the interface between the MCP protocol layer and CameoDB's search engine:

```rust
pub trait McpBackend: Clone + Send + Sync + 'static {
    fn search_index(...) -> BoxFuture<'_, Result<JsonValue, String>>;
    fn search_indexes(...) -> BoxFuture<'_, Result<JsonValue, String>>;
    fn get_index(...) -> BoxFuture<'_, Result<JsonValue, String>>;
    fn list_indexes(...) -> BoxFuture<'_, Result<JsonValue, String>>;
    fn validate_query(...) -> BoxFuture<'_, Result<JsonValue, String>>;
    fn get_index_stats(...) -> BoxFuture<'_, Result<JsonValue, String>>;
    fn list_resources(...) -> BoxFuture<'_, Result<JsonValue, String>>;
    fn read_resource(...) -> BoxFuture<'_, Result<JsonValue, String>>;
}
```

The `AppState` from the server crate implements this trait, bridging MCP calls to the existing `RouterActor` and `ClientOp` message passing system.

## Usage Example

Here's a complete workflow using the MCP tools:

1. **Discover available indexes:**
   ```json
   {"name": "list_indexes", "arguments": {}}
   ```

2. **Inspect a specific index schema:**
   ```json
   {"name": "get_index", "arguments": {"index": "papers"}}
   ```

3. **Validate a query before executing:**
   ```json
   {
     "name": "validate_query",
     "arguments": {
       "index": "papers",
       "query": "titel:rust AND author:doe"
     }
   }
   ```
   Response will warn about unknown field `titel` and suggest `title`.

4. **Execute corrected search:**
   ```json
   {
     "name": "search_index",
     "arguments": {
       "index": "papers",
       "query": "title:rust AND author:doe",
       "limit": 10,
       "fields": ["title", "author", "year", "abstract"]
     }
   }
   ```

5. **Search across multiple indexes:**
   ```json
   {
     "name": "search_indexes",
     "arguments": {
       "indexes": [
         {"index": "papers", "fields": ["title", "author"]},
         {"index": "books", "fields": ["title", "isbn"]}
       ],
       "query": "rust programming",
       "limit": 20
     }
   }
   ```

## Development

### Running Tests

```bash
cargo test -p cameodb_mcp
```

### Linting

```bash
cargo clippy -p cameodb_mcp -- -D warnings
```

### Recent Changes (v0.1.0)

#### MCP Specification Compliance
- **Fixed SSE Handshake**: Now emits proper `endpoint` event per MCP spec
- **Asynchronous POST Processing**: Returns `202 Accepted` immediately, processes in background
- **Structured Events**: Uses Axum SSE `Event` objects instead of raw strings
- **Session Cleanup**: 5-minute timeout with automatic cleanup
- **Error Handling**: Graceful handling of dropped receivers in async tasks

#### Technical Improvements
- Removed unused `MessageAck` struct
- Updated channel types from `String` to `Event`
- Added proper event type mapping (`endpoint`, `message`)
- Non-blocking message processing with `tokio::spawn`
- Improved logging for debug scenarios

### Integration with Main Server

The MCP router is nested into the main HTTP server in `crates/server/src/http_server.rs`:

```rust
Router::new()
    .nest("/mcp", mcp_router::<AppState>())
    // ... other routes
```

## License

FSL-1.1-Apache-2.0 (same as CameoDB)

## References

- [Model Context Protocol Specification](https://modelcontextprotocol.io/specification)
- [MCP TypeScript SDK](https://github.com/modelcontextprotocol/typescript-sdk)
- [CameoDB Documentation](../../README.md)
