# MCP — the Model Context Protocol surface

CameoDB serves MCP on the same HTTP listener as the REST API, so an agent can discover,
inspect and search indexes without a bespoke integration. This page covers the operator's
side — what is mounted, how to enable and secure it, and how to shape an index so an agent
can use it. The caller's side — tool signatures, the paging rules, the full query-syntax
reference and per-client setup snippets — lives in
[crates/mcp/README.md](../crates/mcp/README.md), which the tool catalogue and a
synchronization test keep current; this page points at it rather than duplicating it.

## What is mounted

- `POST /mcp` — Streamable HTTP, the current MCP transport: a request is answered inline on
  its own POST, and `initialize` hands back an `MCP-Session-Id` the client replays.
- `/mcp/sse` + `/mcp/messages` — the superseded 2024-11-05 HTTP+SSE transport, kept for
  clients that have not moved.
- Six tools: `list_indexes`, `describe_index`, `validate_query`, `search_index`,
  `search_across_indexes` and `get_catalog_stats` — all reads.
- `cameodb://indexes[...]` resources — the catalogue as browsable documents — and the
  `cameodb-orchestrator` prompt over `prompts/get`.

## Enabling and configuring

Everything the endpoint runs on is under `[mcp]`; `enabled = false` unmounts `/mcp`
entirely, which is the right answer for a node whose callers all use the HTTP API. The
knobs — session lifetime, keepalive pacing, per-session in-flight bounds, the legacy
transport switch — are documented with their defaults in
[Configuration](CONFIGURATION.md#the-mcp-endpoint-mcp). Per-caller spend is separately
metered under [`[security.limits]`](CONFIGURATION.md#rate-limiting-tool-calls-search-and-writes-securitylimits).

## Securing it

An MCP client authenticates with `Authorization: Bearer <key>`, the same header the REST
API takes, and the key's capabilities and `allowed_indexes` apply unchanged — see
[Authentication](CONFIGURATION.md#authentication-security). Every tool today is a read, so
a key scoped to `read` covers the whole surface. Two details worth knowing:

- The tool *catalogue* is already filtered to what the key may call, so an agent is not
  offered a tool its key would be refused for.
- A client that cannot set headers — ChatGPT, claude.ai, Claude Desktop connecting by
  URL — signs in with OAuth instead, once [`[security.oauth]`](#oauth-for-mcp-clients) is
  on: the user pastes the key they hold on the node's approval page, and the token the
  client receives acts as that key with `read` alone, on `/mcp` alone.

## Connecting clients

Setup snippets for Claude Code, Claude Desktop, Windsurf, Cursor and the MCP Inspector are
maintained in [crates/mcp/README.md](../crates/mcp/README.md#client-configuration). The
short version: `http://<node>:9480/mcp` for a current client, `http://<node>:9480/mcp/sse`
for one still on the legacy transport, and `9480` being the default port.

## Shaping an index for agent use

An agent arrives knowing nothing about the deployment; what it can do is bounded by what
it can read off `list_indexes` and `describe_index`. Three decisions in the schema decide
most of that:

- **Write the descriptions.** `description` on the index and on each field is the only
  part of a schema that says what the data *is* — the names and types describe the shape.
  Nothing infers one, and both listings carry it verbatim to the agent choosing between
  indexes and composing a query. An index without one asks the agent to guess from the
  name, and it will guess.
- **Declare fields up front** with `PUT /api/{index}/_config`, or make sure the first
  write carries every field you intend to be searchable. A field that appears only in
  later writes stays unindexed: it cannot be queried, a search naming it is refused on
  the MCP surface rather than answered silently, and the fix — reindexing — does not
  exist yet. `default_fields` deserves the same attention: it is the list a bare,
  unqualified term searches, which is exactly what a first-pass agent query is.
- **Pick types for the operators you want used.** A numeric or date field sorts only with
  its `fast` column; a `text` field is matched by word under its tokenizer, where a `string`
  field matches its whole value in the case it is written — so a field of names or codes whose
  case varies in the data is better as `text` with `raw_fold` (the whole value, in any case),
  and one a reader will search by a word of is better as `text` under a word tokenizer;
  `indexed: false` keeps a payload field returned in hits but out of queries.
  `describe_index` reports each field's `type`, `tokenizer`, whether it is `searchable`,
  `sortable` and `returned`, a `query_hint` per type naming its operators and a
  `tokenizer_hint` per tokenizer saying how it matches — an agent builds from that, so the
  type chosen is the vocabulary offered.

When in doubt, `validate_query` — also exposed as a tool — reports a query's normalized
form and the clauses it would discard before any search is run.
