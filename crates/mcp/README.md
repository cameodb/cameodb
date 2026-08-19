# CameoDB MCP Server

Model Context Protocol (MCP) server implementation for CameoDB, enabling AI agents to efficiently search and explore indexed datasets.

## Overview

The `cameodb_mcp` crate provides a standards-compliant MCP server that exposes CameoDB's search capabilities as tools for AI agents. It implements the [Model Context Protocol](https://modelcontextprotocol.io) specification, negotiating `2025-06-18` by default and accepting `2025-03-26` and `2024-11-05` from clients that ask for them, over Streamable HTTP and the legacy HTTP+SSE transport.

### Architecture

- **Shared-Port Design**: MCP endpoints are nested under `/mcp` in the main CameoDB HTTP server
- **No Separate Process**: Runs in the same binary as CameoDB, sharing the same `AppState` and actor system
- **Transport**: Streamable HTTP (2025-03-26+) on `/mcp`, plus legacy HTTP+SSE (2024-11-05) for already-configured clients
- **Protocol**: JSON-RPC 2.0, negotiated at `initialize` — `2025-06-18`, `2025-03-26`, or `2024-11-05`
- **Session Management**: Automatic session registry with 5-minute timeout cleanup
- **Asynchronous Processing**: Non-blocking POST requests with background task execution

### Key Features

- ✅ **6 MCP Tools** for search, metadata, and query validation
- ✅ **1 MCP Prompt** (`cameodb-orchestrator`) — universal data retrieval & orchestration skill injected into agent context
- ✅ **4 Resource URIs** for index exploration (indexes, index metadata, schema, stats)
- ✅ **Field-Type-Aware Query Validation** with syntax reference
- ✅ **Federated Search** across multiple indexes
- ✅ **Per-Index Field Projection** for efficient data retrieval
- ✅ **Read-Only Operations** (all tools are annotated as `readOnlyHint: true`)
- ✅ **MCP Spec Compliant** (2025-06-18, negotiable down to 2024-11-05) with proper SSE event handling
- ✅ **Asynchronous Processing** - non-blocking POST with 202 Accepted response
- ✅ **Automatic Session Cleanup** with configurable timeout (5 minutes)
- ✅ **Self-Contained Schema Discovery** — every index response includes per-field query hints

### Self-Contained Discovery

CameoDB is designed as a self-contained document store where indexed fields, schemas, and data types drive automatic agent adaptation. When new indexes are created or fields evolve, the MCP tools **automatically reflect the changes** — no configuration or manual updates needed.

**Optimized Schema Responses:**
Schema and field structures are optimized to return only relevant information for AI clients to build effective queries. Responses avoid overwhelming agents with redundant or irrelevant metadata, focusing on:
- `searchable_field_names`: List of all queryable field names (for quick reference)
- `fields` (from `describe_index`/`list_indexes`): Per-field type with compact details
- `available_fields` (from `validate_query`): Per-field type with detailed query hints
- `query_hints`: Section showing which operators work with each field type
- Essential statistics and metadata (for context)

**Agent workflow (no prior knowledge required):**

1. **`list_indexes`** → Discover all indexes with schemas and per-field `query_hint` (what operators work with each field type)
2. **`describe_index`** → Deep-dive into a specific index: field definitions, types, stats, `fields` array, and `query_hints` section
3. **`search_index`** → Construct queries using the field names and operators learned from the schema
4. **`validate_query`** *(optional)* → Parse a query with the same parser a search uses: whether it parses, where it fails, the form the engine will actually run, and which clauses can never match. Also typo detection ("did you mean?") and the full syntax reference

Each `available_fields` entry (from `validate_query`) and `fields` entry (from `describe_index`/`list_indexes`) carries a `query_hint` naming the operators that field's type supports, plus the `indexed`, `fast` and `shadow` flags that decide whether it can be queried and sorted at all. The hints are rendered from [`src/syntax.rs`](src/syntax.rs), so they cannot disagree with the reference.

This means an agent can go from zero knowledge to well-formed queries in **two tool calls** (`list_indexes` → `search_index`), with the schema metadata providing all the guidance needed for operator selection.

## MCP Tools

All tools follow MCP naming conventions (verb-first `snake_case`) and include a display name in both places a client looks for one — the top-level `title` and `annotations.title` — plus property descriptions and annotations.

Every tool is annotated `readOnlyHint: true` and `openWorldHint: false`. The closed world is deliberate: these tools reach nothing but this node's own indexes, and documents arriving in them from external ingestion does not change what the tool interacts with. `destructiveHint` and `idempotentHint` are absent because the spec reads them only on tools whose `readOnlyHint` is false; a test requires them the moment a tool is added that is not a read.

Every tool takes the arguments listed below and no others. Each `inputSchema` says `additionalProperties: false`, and a call carrying an argument its tool does not take is refused by name rather than run without it — a misspelled `limit` would otherwise return the default ten hits and read as the whole answer. Tools whose arguments are all optional may be called with `arguments` omitted entirely.

Bounds are advertised where they are enforced: the search tools carry a `maximum` on `limit` and `minItems`/`maxItems` on `indexes`, taken from the node's own configuration. Read them from `tools/list` rather than from this page — an operator may have set the ceiling lower than the default.

### How a tool result arrives

A successful call returns its result as the serialized JSON of a single text block in `content`:

```json
{
  "result": {
    "content": [
      {
        "type": "text",
        "text": "{\"hits\":[ ... ],\"hits_returned\":2,\"total_hits\":3,\"limit\":2}"
      }
    ],
    "isError": false
  }
}
```

**One shape, for every client.** The negotiated protocol revision does not change it, and neither
does the `MCP-Protocol-Version` header: `2025-06-18`, `2025-03-26`, `2024-11-05` and the legacy
HTTP+SSE transport all receive the response above.

CameoDB does not use `structuredContent`, and advertises no `outputSchema`. Structured content is
an optional feature, and opting into it is not free: the spec asks a tool that returns structured
content to *also* serialize it into a text block, so a result would travel twice in every message —
measured at 1038 bytes against 520 for a two-hit search — and an agent's context is the scarce
resource.

Sending *only* `structuredContent` is not an option either, which earlier versions learned the hard
way. A client's revision states which spec it speaks, not which part of a result it reads, and
several hosts negotiate `2025-06-18` while rendering `content` alone. Those clients received an
empty array and reported that nothing matched — indistinguishable from a query that genuinely
matched nothing. No header or capability distinguishes them (MCP's client capabilities are `roots`,
`sampling`, `elicitation` and `experimental`), so the only channel that is always read is the one
always populated.

`outputSchema` is absent for the same reason and not separately: advertising one obliges the server
to return conforming structured results, so a schema without them would be a promise to any client
that validates it that could not be kept. `inputSchema` is unaffected and still enforced on every
call.

**A failure travels the same way**, with `isError` set and prose in place of the serialized result,
because a failure is a message rather than data:

```json
{
  "result": {
    "content": [{"type": "text", "text": "limit 20000 is above the maximum of 10000; ..."}],
    "isError": true
  }
}
```

So: read `content[0].text` either way, and let `isError` decide whether to parse it as the result
or read it as an explanation.

### 1. `search_index`

Execute full-text search on a single CameoDB index.

> **CRITICAL ANTI-HALLUCINATION RULE FOR AGENTS:**
> When answering questions based on CameoDB results, you MUST use ONLY the exact data returned by this tool. Do NOT combine database results with your own prior knowledge. If the index returns partial or incomplete information, state exactly what was found and nothing more. NEVER invent or hallucinate fields or values not explicitly present in the query results.

**Error Handling:**
- A query whose clause was dropped fails, naming the clause — results that would answer a different question are never returned
- Errors naming a missing field are appended with the index's field list
- Zero results carry a `_warning` naming what narrowed the query — a phrase, an `AND`, a required `+clause` or an exclusion — and nothing when the query contains none of them, since bare terms are ORed and were never narrowed

**Parameters:**
- `index` (string, required): Name of the CameoDB index to search
- `query` (string, required): Search query string. See [CameoDB Query Syntax Reference](#cameodb-query-syntax-reference); the tool's own description carries a summary, and `validate_query` returns the full reference.

  **Default search fields**: a query with no `field:` prefix searches only `text`, `string` and `json` fields. Numeric, date, boolean, ip and facet fields must be named explicitly.

  **Dropped clauses**: a clause the engine cannot interpret is dropped and whatever is left runs, which widens a conjunction, narrows a disjunction, disables a negation, and matches nothing at all when the dropped clause was the only one. This tool fails rather than returning those results, naming the clause it could not use.
- `limit` (integer, optional): Maximum number of results to return, up to the node's configured ceiling (`[security.limits] max_search_limit`, 10000 by default). The tool's own `inputSchema` carries that number as its `maximum`, so read it there rather than assuming the default. Pass `0` for count-only mode (returns `total_hits` without document data). If omitted, defaults to 10. A larger value is refused, whether it arrives as this argument or as an inline `limit` modifier in the query.
- `offset` (integer, optional): How many hits to skip before the first one returned. With `limit` as the page size, page N is `offset = N * limit`; there are more results when `offset + hits_returned < total_hits`. Defaults to 0. **The ceiling applies to `offset + limit`**, not to either alone: the engine fetches the whole window from the front of every shard and takes the page after merging them, so a deep page costs what a large limit costs and is refused the same way. An offset at or past `total_hits` is answered with an empty page and a `_warning` saying so, rather than an error — see [Paging](#paging).
- `fields` (array of strings, optional): Field names to include in results (field projection)
- `sort` (object, optional): Sort results by a field — the same object `search_across_indexes` takes per index. Takes precedence over an inline `sort` clause in the query.
  - `field` (string, required): Field name to sort by (u64, i64, f64, date, or text/string for alphabetic sort)
  - `order` (string, optional): `asc` or `desc` (defaults to `asc`)

**Returns:** JSON array of matching documents with relevance scores.

A response larger than the largest single message the node is configured to carry — its HTTP body size, 128 MB by default, overridable as `[security.limits] max_response_bytes` — is trimmed to fit and carries `_truncated: true`, `_omitted_hits: N` and a `_warning` naming the figure it hit. The hits returned are the highest ranked, in order; `total_hits` still reports everything that matched. Treat the flag as instruction to narrow the query rather than reading the trimmed set as the whole result.

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

### 2. `search_across_indexes`

Execute federated search across multiple CameoDB indexes with optional per-index field projection. Searches execute **concurrently** across all specified indexes, and results are merged into a single ranked list.

> **CRITICAL ANTI-HALLUCINATION RULE FOR AGENTS:**
> When answering questions based on CameoDB results, you MUST use ONLY the exact data returned by this tool. Do NOT combine database results with your own prior knowledge. If the index returns partial or incomplete information, state exactly what was found and nothing more. NEVER invent or hallucinate fields or values not explicitly present in the query results.

**Error Handling:**
- One query string is applied to every index, so a dropped clause affects the whole merge; the call fails rather than returning partial results
- Errors naming a missing field are appended with that index's field list

**Parameters:**
- `indexes` (array, required): The indexes to search, at least one and at most 20. Naming the same index twice is refused rather than searched twice — each mention would be counted separately, reporting more documents than the index holds. An entry is **either a bare index name** (`"papers"`) or an object naming one, and the two mix freely; use the bare form unless the index needs a projection or a sort of its own. The object form takes:
  - `index` (string, required): Name of the CameoDB index
  - `fields` (array of strings, optional): Fields to include from this index
  - `sort` (object, optional): Sort results by a field within this index
    - `field` (string, required): Field name to sort by (u64, i64, f64, date, or text/string for alphabetic sort)
    - `order` (string, optional): `asc` or `desc` (defaults to `asc`)
- `query` (string, required): Search query applied to all specified indexes
- `limit` (integer, optional): Maximum total results across all indexes, up to the node's configured ceiling — see `search_index` above. Pass `0` for count-only mode (returns `total_hits` without document data). If omitted, defaults to 10.
- `offset` (integer, optional): How many hits to skip, applied **once, to the merged order** — so page N of a federated search is page N of the combined result, not page N of each index. Each index is asked for `offset + limit` hits from the front and the skip happens after the merge; the same ceiling bounds the sum. Defaults to 0.

**Returns:** Combined results merged by relevance score (or by the sort field if specified). Each hit includes an `_index_source` field indicating its origin index. The response contains only the merged `hits` array — per-index results are not duplicated, keeping token usage proportional to the limit.

**Example:**
```json
{
  "name": "search_across_indexes",
  "arguments": {
    "indexes": [
      {"index": "papers", "fields": ["title", "author"], "sort": {"field": "year", "order": "asc"}},
      "books"
    ],
    "query": "rust programming",
    "limit": 20
  }
}
```

### 3. `describe_index`

Retrieve schema and statistics for a single CameoDB index.

**Parameters:**
- `index` (string, required): Name of the CameoDB index

**Returns:** Complete field definitions, types, document count, size, metadata, and a `fields` array with per-field details, plus `query_hints` section showing which operators work with each field type.

`description` appears on the index and on a field only when an operator wrote one. It is the one part of a schema that says what the data is rather than how it is shaped, so prefer it over anything a field name suggests; most indexes carry none, and its absence says nothing about them. See [Create/Update Index Schema](../../docs/API_REFERENCE.md#createupdate-index-schema) for how they are set and the length limits.

Two of the per-field flags are answers rather than declarations, and they are the ones to read
before writing a query. `indexed` is what the schema says; `searchable` is whether the built index
can actually reach the field — they differ for a field declared after the index was built, which is
`indexed` and matches nothing. `fast` is likewise a declaration, and `sortable` is whether the built
index carries the column a sort orders on: a numeric sort on a field that is `fast` but not
`sortable` fails, and a text sort on one silently returns an approximate order. Sort on `sortable`
fields; query `searchable` ones.

A field marked `shadow` is the document identifier under the name the source data gave it. The value lives only in `id` and is not stored or indexed again under that name, so the field is a name in the schema rather than data in the index — and the name is applied to `id` in both directions. It is queryable despite being unindexed (`shadow_field:VALUE` on its own is the same key-value lookup as `id:VALUE`), and every hit returns the identifier under the shadow name with no `id` field, so `indexed` alone answers neither what may be queried nor what may be projected.

**Example:**
```json
{
  "name": "describe_index",
  "arguments": {
    "index": "papers"
  }
}
```

**Response includes `fields` and `query_hints`:**
```json
{
  "description": "Peer-reviewed papers, one document per paper.",
  "fields": [
    {
      "field": "id",
      "type": "text",
      "indexed": true,
      "searchable": true,
      "fast": false,
      "sortable": false,
      "shadow": false
    },
    {
      "field": "sha1",
      "type": "text",
      "indexed": false,
      "searchable": true,
      "fast": false,
      "sortable": false,
      "shadow": true
    },
    {
      "field": "year",
      "type": "i64",
      "indexed": true,
      "searchable": true,
      "fast": true,
      "sortable": true,
      "shadow": false,
      "description": "Year of publication, not of submission."
    }
  ],
  "query_hints": [
    {
      "type": "text",
      "query_hint": "Tokenized full-text. Supports: field:term, field:\"phrase\", field:\"phrase\"~N (slop)..."
    },
    {
      "type": "i64",
      "query_hint": "Numeric field. Supports: field:value, field:[low TO high], field:{low TO high}..."
    }
  ]
}
```

### 4. `list_indexes`

List every CameoDB index this key can see, with enough about each to choose between them. **A new index appears here automatically** — no configuration needed.

**Parameters:** None

**Returns:** One entry per index carrying `index`, its `description` where an operator wrote one, `document_count`, `field_count` and `field_names`. Enough to choose which index holds the answer; the field types, the `indexed`/`fast`/`shadow` flags and the per-type `query_hints` come from `describe_index` on the one you pick. A listing that repeated `describe_index` for every index would spend most of an agent's context before it had chosen an index — on five indexes of fourteen fields it was 13.5 KB against 1.3 KB.

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
4. **Index + query**: Runs the real parser. This is the only combination that can tell you whether the query parses — a query on its own gets a structural check that passes things like `title:` and `title:[2020 TO`, neither of which parse

**Returns:**
- `syntax_reference`: Full query syntax documentation with all operators, examples, and field-type compatibility
- `available_fields`: Schema fields with types, indexed status, and per-field query hints (includes `query_hint` per field)
- `field_suggestions`: Autocomplete matches for partial field names
- `query_analysis`: The parser's verdict plus the structural pass — see below
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

**`query_analysis` fields** (present when both `index` and `query` are supplied):

| Field | Meaning |
|---|---|
| `parses` | `true` if the query is well-formed. `false` if not — `syntax_errors` says where. **`null` means it could not be checked**, not that it passed |
| `syntax_errors` | The parser's own messages, each with the position it reached |
| `normalized_query` | What the engine actually runs, after date, facet and prefix rewriting |
| `discarded_clauses` | Clauses that parse but can never match — unknown fields, non-indexed fields, unsupported constructs. Exactly what a search would drop |
| `warnings` | Structural findings, plus a line per syntax error |
| `suggestions` | Field corrections, e.g. `Unknown field 'titel'. Did you mean: title?` |
| `field_hints` | Type-specific query guidance per referenced field |

A clause in `discarded_clauses` is the dangerous case: the search runs, returns results, and
answers a narrower question than the one asked. `search_index` refuses such a query outright;
validating first is how to find out why.

### 6. `get_catalog_stats`

Return statistics for a single CameoDB index or aggregated statistics across all indexes.

**Parameters:**
- `index` (string, optional): Index name. If omitted, returns aggregated statistics for all indexes.

**Returns:**
- Single index: document count, field count, field names, size, metadata
- All indexes: total documents, total size, total fields, per-index breakdown

**Example:**
```json
{
  "name": "get_catalog_stats",
  "arguments": {
    "index": "papers"
  }
}
```

## Paging

`limit` is the page size and `offset` is where the page starts, so page N is `offset = N * limit`.
Both search tools take them, and both are also writable inline — `title:rust limit 10 offset 20` —
which is how the bundled client and the REPL express them. An argument wins over the inline form.

Every response says what it ran with. There are more results when
`offset + hits_returned < total_hits`, which is the test to walk a result to its end; `total_hits`
counts what matched and is unaffected by either number.

Three things worth knowing before paging through a large result:

**The ceiling applies to `offset + limit`.** A page is served by asking every shard and every index
for the whole window from the front and taking the slice after merging them — the skip cannot be
pushed down, because all of a page may come from one source. So the node fetches `offset + limit`
hits however small the page is, a deep page costs exactly what a large limit costs, and
`[security.limits] max_search_limit` bounds the sum. `offset: 9990` with no `limit` is refused on a
node whose ceiling is 10000, because the default limit of 10 is counted too.

**A page needs an order to be a page of.** With a `sort` on a `sortable` field the order is total
and pages are consecutive slices of it. Without one the order is by relevance, which is stable for
one query against unchanged data but says nothing across writes — a document written between two
requests can shift everything after it. Neither is a snapshot: paging is not a cursor.

**Do not page through an approximate sort.** A sort on a text field with no fast column orders the
top `2 × limit` scoring candidates rather than everything that matched, and each page collects a
*different* set of candidates — so the pages are not slices of one order and cannot be assembled.
Such a response says so: `_approximate_sort` names the field, and `_warning` explains it.
`describe_index` reports `sortable: false` for the field beforehand.

Paging past the end is not an error. It returns an empty `hits` with `total_hits` intact and a
`_warning` naming the last offset that holds a document, so an empty page is never mistaken for a
query that stopped matching.

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

The syntax is defined in exactly one place: the tables in [`src/syntax.rs`](src/syntax.rs). Every
surface that tells a caller what a query may contain renders from them — the `search_index` tool
description, the reference `validate_query` returns, the per-field hints `describe_index` attaches to a
schema, and the table below, which is generated and checked against the tables by
`the_readme_syntax_table_matches_the_table`. Regenerate it with
`UPDATE_DOCS=1 cargo test -p cameodb_mcp readme` rather than editing it by hand.

<!-- BEGIN GENERATED SYNTAX -->

| Syntax | Meaning | Field types |
|---|---|---|
| `term` | Match a term; several terms are ORed, so a document matching any one of them is returned. | any |
| `*` | Match every document. | any |
| `field:value` | Match a value in one field. | text, string, i64, u64, f64, date, boolean, ip, json, facet |
| `field:"a b"` | Exact phrase, terms in order. Text fields only. | text |
| `field:"a b"~N` | Phrase allowing N extra words between terms. | text |
| `"a b pre"*` | Phrase whose last term is a prefix. Two or more terms. | text |
| `field:pre*` | Match every term starting with `pre`. One term, and the field is required. | text, string |
| `AND / OR / NOT` | Combine clauses. Uppercase only. | any |
| `+term / -term` | Require or exclude a clause. | any |
| `(...)` | Group clauses to control precedence. | any |
| `field:value^N` | Weight a clause's score contribution. | text, string, i64, u64, f64, date, boolean, ip, json, facet |
| `field:[low TO high]` | Range, bounds inclusive. | text, string, i64, u64, f64, date, ip |
| `field:{low TO high}` | Range, bounds exclusive. | text, string, i64, u64, f64, date, ip |
| `field:[low TO *]` | Range with one side unbounded. | text, string, i64, u64, f64, date, ip |
| `field:>value` | Comparison: `>` `<` `>=` `<=`. | i64, u64, f64, date |
| `field: IN [a b c]` | Match any of several values. | text, string, i64, u64, f64, date, boolean |
| `field:/path/to/value` | Match a facet path. | facet |
| `id:value` | Look up one document by id. Fastest retrieval path. | any |

**Not supported**

- `field:*` — Field-presence tests are not supported for any field type. The clause is dropped and reported. Use a bounded range, or match an explicit value.
- `pre*` — A prefix needs a field name; without one the `*` is dropped and `pre` is matched as a whole term. Name the field, or OR one clause per field.
- `field.subfield:value` — Paths into a json field are not queryable. A json field is searchable only as unstructured text, so `field:value` matches any key or value inside it.
- `field:/regex/` — Regular expressions are disabled.

**Inline modifiers**, in one run at the end of the query

| Syntax | Meaning | Example |
|---|---|---|
| `return f1,f2` | Return only these fields, in this order. | `title:rust return title,author` |
| `limit N` | Cap the number of results. | `title:rust limit 5` |
| `offset K` | Skip the first K results — the page, where `limit` is the page size. `offset + limit` is bounded by the same maximum `limit` is. | `title:rust limit 10 offset 20` |
| `sort field:desc` | Order by a field. See the sorting rules. | `title:rust sort year:desc` |

- A modifier counts only where it opens an unbroken run of clauses reaching the end of the query, with query text left in front of it. Anything else is searched for, so `find tax return forms` is four terms and `* limit 10` is how to ask for a bare limit.
- A field list needs a comma between names, a limit needs a number, and a sort order must be exactly `asc` or `desc`. A clause that does not parse stays in the query rather than being applied in part.
- A modifier naming a field the index does not have is reported as a dropped clause, since a projection would otherwise return documents with no fields.

**Rules**

- A field name containing a dot is written as it is, unescaped: `k8s.node:worker-1`. Escaping the dot makes the lookup miss.
- A field that exists in the schema but is not indexed cannot be queried, unless it is marked `shadow`. Fields discovered from a document are added unindexed, and stay that way until a schema update promotes them, so check the `indexed` flag before naming a field.
- A field marked `shadow` is the name the source data used for its identifier. The value lives only in `id`, the key in both the key-value store and the search index, and is never stored or indexed again under the descriptive name — the schema carries the name and nothing carries a second copy of the data, which is what makes the field a shadow. Querying it is therefore querying `id`: `shadowfield:VALUE` on its own is the fastest retrieval CameoDB has, answered without the search index, and it is the only form that works — named inside a larger query the clause is dropped and reported, and a `*` in the value counts as part of the identifier rather than as a prefix, so it matches nothing. Results come back the same way round: every hit carries the identifier under the shadow name and has no `id` field, so name the shadow field in `fields` or `return`. Asking for `id` there returns a document with nothing in it, and no warning that the field it named is one no document has.
- `_seq` is an internal sequence number used to track write-ahead-log position. It is present in every index and technically queryable, but it carries no meaning for a search and should be ignored.
- `AND`, `OR`, `NOT`, `TO` and `IN` are keywords in uppercase only. Lowercase is query text: `to` and `in` break the clause around them and are reported, while `and`, `or` and `not` are searched for as ordinary words and change what the query means without any warning.
- A clause the engine cannot interpret is dropped and whatever is left runs, which widens a conjunction, narrows a disjunction, disables a negation, and matches nothing at all when the dropped clause was the only one. Every dropped clause is reported: the HTTP API attaches `_discarded_clauses` to the response — or refuses with 400 when no clause survived at all — and an MCP tool call fails with the reason. Results are never returned as though the query had been understood.

**Sorting**

- Sorting is exact on a field with a fast column, and `describe_index` reports which those are as `sortable`. A numeric or date field needs one to be sorted at all; a text or string field without one is sorted approximately rather than refused.
- An approximate sort collects the top `2 × limit` matches by relevance and orders those alphabetically, so the result is not the alphabetically first documents in the index and paging through it re-orders a different sample on each page. The response says so: it carries `_approximate_sort` naming the field, and a `_warning`.
- A field's fast column is written when the index is built, so `sortable` cannot be turned on for an index that already has data. Declare the field `fast` before writing to it.
- Under a numeric or date sort every hit carries `_score` of 1.0, because no relevance score is computed. Do not read it as a ranking.
- Ascending unless `desc` is given.

Reserved characters, which must be escaped with a backslash to be matched literally: + ^ ` : { } " ' [ ] ( ) ! \ * and space

<!-- END GENERATED SYNTAX -->

To read the same reference at runtime, with examples and per-type detail:

```bash
# The full reference: operators with examples, per-type support, sorting rules, unsupported forms
curl -s localhost:9480/mcp -H 'content-type: application/json' -d '{
  "jsonrpc":"2.0","id":1,"method":"tools/call",
  "params":{"name":"validate_query","arguments":{}}
}' | jq -r '.result.content[0].text'

# What one index's fields support, per field
curl -s localhost:9480/mcp -H 'content-type: application/json' -d '{
  "jsonrpc":"2.0","id":1,"method":"tools/call",
  "params":{"name":"describe_index","arguments":{"index":"my_index"}}
}' | jq -r '.result.content[0].text'
```

Three behaviours are worth knowing before reading either, because they change results rather than
merely constraining syntax:

- **A clause the engine cannot interpret is dropped, not rejected.** The rest of the query then
  runs, which widens a conjunction, narrows a disjunction, disables a negation, and matches
  nothing at all when the dropped clause was the only one. Every dropped clause is reported: the
  HTTP API attaches `_discarded_clauses` to the response — or refuses with 400 when no clause
  survived at all — and an MCP tool call fails naming the
  clause. Results are never returned as though the query had been understood.
- **Only indexed fields are queryable.** A field discovered from a document is added unindexed and
  stays that way until a schema update promotes it. `describe_index` reports the flag per field.
- **Sorting a text or string field is approximate.** The top `2 × limit` matches by relevance are
  collected and then ordered alphabetically, so the result is not the alphabetically first
  documents in the index. Numeric and date sorts are exact but require the field's `fast` flag,
  and set every `_score` to 1.0.


## Client Configuration

**Note**: CameoDB defaults to port `9480`. You can change this in the configuration file (`[network.http] port = 9480`) or via command line arguments.

### Claude Code (Recommended)

Claude Code supports **native SSE transport** — no bridge or curl workaround needed.

```bash
# Add via CLI (project-scoped)
claude mcp add --transport sse cameodb http://localhost:9480/mcp/sse

# Or add as Streamable HTTP
claude mcp add --transport http cameodb http://localhost:9480/mcp

# Verify connection
claude mcp get cameodb
claude mcp list
```

Inside Claude Code, type `/mcp` to check server status and available tools.

**Project-level config** (`.mcp.json` in repo root, shareable with team):
```json
{
  "mcpServers": {
    "cameodb": {
      "type": "sse",
      "url": "http://localhost:9480/mcp/sse"
    }
  }
}
```

### Claude Desktop

Add to `~/Library/Application Support/Claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "cameodb": {
      "type": "sse",
      "url": "http://localhost:9480/mcp/sse"
    }
  }
}
```

For servers requiring authentication headers:
```json
{
  "mcpServers": {
    "cameodb": {
      "type": "sse",
      "url": "http://localhost:9480/mcp/sse",
      "headers": {
        "Authorization": "Bearer your-token"
      }
    }
  }
}
```

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

The MCP server supports multiple HTTP transport modes for maximum client compatibility:

- **Streamable HTTP** (2025-03-26+): `POST /mcp` — Processes a JSON-RPC message and returns the response inline; `initialize` also returns an `MCP-Session-Id` header. `GET /mcp` — Opens the server-to-client SSE stream (keep-alive only; CameoDB initiates no server-side requests). `DELETE /mcp` — Terminates the session named by `MCP-Session-Id`
- **SSE Endpoint** (legacy, 2024-11-05): `GET /mcp/sse` — Establishes SSE connection and emits `endpoint` event
- **Message Endpoint** (legacy): `POST /mcp/messages?session_id={id}` — Receives JSON-RPC messages for SSE sessions (returns `202 Accepted`)
- **Compatibility Endpoint**: `POST /mcp/sse` — Accepts direct JSON-RPC requests for clients that are hard-coded to post to the SSE path
- **OpenAI ChatGPT Compatible**: The same POST protocol (`POST /mcp` or `POST /mcp/sse`) works for OpenAI ChatGPT-type requests, enabling support for a wider range of AI clients and different implementation approaches

### MCP Specification Compliance

The implementation follows the official MCP Streamable HTTP transport specification (2025-03-26 onwards) and keeps the older HTTP+SSE transport (2024-11-05) for clients already configured against it.

The protocol version is negotiated on `initialize`: the server echoes the client's requested version when it is one of `2025-06-18`, `2025-03-26` or `2024-11-05`, and otherwise answers with `2025-06-18`. On the Streamable HTTP endpoint, an `MCP-Protocol-Version` request header outside that set is rejected with `400 Bad Request`.

#### SSE Handshake
1. Client connects to `/mcp/sse`
2. Server emits `endpoint` event with POST endpoint URL: `event: endpoint\ndata: /mcp/messages?session_id=xxx`
3. Client uses this URL for message posting

#### Asynchronous Message Processing
1. Client POSTs JSON-RPC message to `/mcp/messages?session_id=xxx`
2. Server immediately returns `202 Accepted` (non-blocking)
3. Server processes message in background task
4. Server sends response as `message` event: `event: message\ndata: {json-rpc-response}`

#### Streamable HTTP
1. Client POSTs a JSON-RPC request to `/mcp`
2. Server processes the request immediately
3. Server returns the JSON-RPC response in the HTTP response body — for `initialize`, with an `MCP-Session-Id` header the client replays on later requests
4. Messages that produce no reply (notifications, responses) return `202 Accepted`
5. `GET /mcp` opens the listening SSE stream; `DELETE /mcp` ends the session

For compatibility with some MCP client integrations, `POST /mcp/sse` is also accepted and handled the same way as `POST /mcp`.

### Session Management

- Session IDs are cryptographically random UUIDs on both transports, so an id cannot be guessed even when authorization is off and sessions are bound to nobody
- A request naming a session the server no longer holds is answered `404 Not Found`, per the Streamable HTTP spec — the signal telling the client to start over with `initialize` (an `initialize` carrying a stale id already is that fresh start, so it proceeds)
- A session created by an identified key can only be continued or terminated by that key; another key is refused with `403 Forbidden`
- The registry holds at most 1024 sessions; at the cap the longest-idle one is evicted rather than the new one refused
- Server emits structured `Event` objects (not raw strings) for proper MCP compliance
- Sessions are kept alive while the SSE connection remains open
- Sessions are cleaned up after SSE disconnect + 5 minutes of POST inactivity
- Keepalive messages sent every 15 seconds to maintain connection

### JSON-RPC Methods

The server implements these JSON-RPC methods:

- `initialize` — Capability negotiation (advertises `tools`, `resources`, `prompts`)
- `ping` — Health check
- `notifications/initialized` — Client initialization complete (no response)
- `notifications/cancelled` — Task cancellation (no response)
- `tools/list` — List available tools
- `tools/call` — Invoke a tool
- `resources/list` — List available resources
- `resources/read` — Read a resource
- `prompts/list` — List available prompts (`cameodb-orchestrator`)
- `prompts/get` — Retrieve the orchestration skill prompt

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
    fn search_across_indexes(...) -> BoxFuture<'_, Result<JsonValue, String>>;
    fn describe_index(...) -> BoxFuture<'_, Result<JsonValue, String>>;
    fn list_indexes(...) -> BoxFuture<'_, Result<JsonValue, String>>;
    fn validate_query(...) -> BoxFuture<'_, Result<JsonValue, String>>;
    fn get_catalog_stats(...) -> BoxFuture<'_, Result<JsonValue, String>>;
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
   {"name": "describe_index", "arguments": {"index": "papers"}}
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
     "name": "search_across_indexes",
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

### Recent Changes

#### v0.2.3 — Federated Search Overhaul & Sort Improvements

- **Concurrent Multi-Index Search**: `search_across_indexes` now executes all index searches concurrently using `FuturesUnordered`, reducing latency from sum-of-all-searches to max-of-all-searches
- **Fixed Merge Sort**: Relevance merge now reads `_score` (the actual field name) instead of `score`, which was a no-op causing arbitrary truncation order
- **Removed `results_by_index`**: Response no longer includes per-index result duplicates — only the merged `hits` array is returned, cutting token usage by 2-4x for LLM consumers
- **Sort-Aware Merge**: When a per-index sort spec is provided, the federated merge orders hits by `_sort_key` (internal metadata) instead of score, preserving sort order across indexes
- **Default Sort Order**: Changed from `desc` to `asc` across MCP, storage, and HTTP server layers
- **Expanded Sortable Types**: Text/string fields now supported for alphabetic post-fetch sort in addition to FAST fields (u64, i64, f64, date)
- **Field Projection Order**: `apply_field_projection` now inserts user-specified fields first in projection order, then metadata fields after, ensuring consistent response field ordering regardless of sort

#### v0.1.0 — MCP Specification Compliance
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
