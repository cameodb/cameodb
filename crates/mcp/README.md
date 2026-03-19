# CameoDB MCP Server

Model Context Protocol (MCP) server implementation for CameoDB, enabling AI agents to efficiently search and explore indexed datasets.

## Overview

The `cameodb_mcp` crate provides a standards-compliant MCP server that exposes CameoDB's search capabilities as tools for AI agents. It implements the [Model Context Protocol](https://modelcontextprotocol.io) specification (version 2024-11-05) using HTTP/SSE transport.

### Architecture

- **Shared-Port Design**: MCP endpoints are nested under `/mcp` in the main CameoDB HTTP server
- **No Separate Process**: Runs in the same binary as CameoDB, sharing the same `AppState` and actor system
- **Transport**: HTTP/SSE for real-time bidirectional communication
- **Protocol**: JSON-RPC 2.0 over SSE
- **Session Management**: Automatic session registry with cleanup

### Key Features

- ✅ **6 MCP Tools** for search, metadata, and query validation
- ✅ **4 Resource Providers** for index exploration
- ✅ **Field-Type-Aware Query Validation** with syntax reference
- ✅ **Federated Search** across multiple indexes
- ✅ **Per-Index Field Projection** for efficient data retrieval
- ✅ **Read-Only Operations** (all tools are annotated as `readOnlyHint: true`)

## MCP Tools

All tools follow MCP naming conventions (verb-first `snake_case`) and include `title`, property descriptions, and annotations.

### 1. `search_index`

Execute full-text search on a single CameoDB index.

**Parameters:**
- `index` (string, required): Name of the CameoDB index to search
- `query` (string, required): Search query string. Supports:
  - Field targeting: `title:rust`
  - Phrase queries: `title:"rust programming"`
  - Boolean operators: `title:rust AND author:doe`
  - Range queries: `year:[2020 TO 2024]`
  - Date comparisons: `created_at:>2024-01-01`
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

**Returns:** Complete field definitions, types, document count, size, and metadata.

**Example:**
```json
{
  "name": "get_index",
  "arguments": {
    "index": "papers"
  }
}
```

### 4. `list_indexes`

List all available CameoDB indexes with their schemas and metadata.

**Parameters:** None

**Returns:** All index schemas with metadata (document counts, field definitions, sizes).

**Example:**
```json
{
  "name": "list_indexes",
  "arguments": {}
}
```

### 5. `validate_query`

Validate and get guidance on CameoDB search query syntax.

**Parameters:**
- `index` (string, optional): Index name for schema-aware field validation
- `partial_field` (string, optional): Partial field name for autocomplete suggestions
- `query` (string, optional): Query string to validate and analyze

**Returns:**
- Field suggestions based on partial input
- Query analysis with recognized/unknown/non-indexed fields
- Structural validation (unbalanced quotes/parens)
- Field-type-aware hints for recognized fields
- Fuzzy "did you mean" suggestions for unknown fields
- Complete CameoDB query syntax reference

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
- `warnings`: ["Unknown field 'titel'. Did you mean: title?"]
- `suggestions`: Field corrections and syntax tips
- `field_hints`: Type-specific query guidance for each recognized field
- `syntax_reference`: Full CameoDB query syntax documentation

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

The `validate_query` tool returns a comprehensive syntax reference, but here's a quick overview:

### Basic Search
```
rust database
machine learning
```

### Field-Targeted Search
```
title:rust
author:doe
```

### Phrase Queries
```
title:"rust programming"
description:"machine learning"
```

### Boolean Operators
```
title:rust AND author:doe
title:rust OR title:go
title:rust NOT author:smith
(title:rust OR title:go) AND year:[2020 TO 2024]
```

### Range Queries
```
year:[2020 TO 2024]
price:[10.0 TO *]
age:[* TO 30]
```

### Date Queries
```
created_at:2024-01-15
created_at:>2024-01-01
created_at:<2024-12-31
created_at:[2024-01-01 TO 2024-12-31]
```

### Exact ID Lookup
```
id:my-document-id
```

### Inline Modifiers (CameoDB-specific)
```
title:rust return title,author,year
title:rust limit 5
title:rust AND author:doe return title,author limit 10
```

### Field Types

CameoDB supports 11 field types with type-specific query syntax:

- **text**: Tokenized full-text search. Use `field:value` or `field:"phrase query"`
- **string/exact**: Exact match only (no tokenization). Use `field:exact_value`
- **i64/u64/f64**: Numeric fields. Use `field:value` or `field:[low TO high]`
- **date**: Date/datetime. Use `field:2024-01-15` or `field:>2024-01-01`
- **boolean**: Boolean. Use `field:true` or `field:false`
- **ip**: IP address. Use `field:192.168.1.1`
- **json**: Nested JSON. Use `field.subfield:value` for nested access
- **facet**: Hierarchical category. Use `field:/path/to/category`

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

The MCP server uses HTTP/SSE transport:

- **SSE Endpoint**: `GET /mcp/sse` — Establishes SSE connection and returns session ID
- **Message Endpoint**: `POST /mcp/messages?session_id={id}` — Receives JSON-RPC messages

### Session Management

- Sessions are created on SSE connection
- Each session gets a unique ID: `mcp-session-{counter}`
- Sessions are automatically cleaned up when the SSE connection closes
- Keepalive messages sent every 15 seconds

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
