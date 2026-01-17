# CameoDB Client CLI

An interactive command-line client for CameoDB with rich ergonomics, persistent history, and index-aware completions. Built with Rustyline on top of the CameoDB HTTP SDK, it is designed for day-to-day cluster administration and exploratory search.

## ✨ Capabilities

- **Interactive REPL** with prompt `cameodb@<host> ▶`
- **Persistent history** stored at `~/.cameodb/client_history`
- **Command completion** for:
  - Top-level commands (`health`, `list`, `search`, `connect`, ...)
  - `list index` suggestions for known indexes
  - `search <index>` field completions with type hints (e.g., `title: [text]`)
- **Field-aware caching** that fetches schema metadata and surfaces field types
- **Smart index cache refresh** after `connect`, `list index`, and `list indexes`
- **Robust Rustyline navigation** (arrow keys, Home/End, Ctrl-R reverse search)
- **Optional search limit** parsing (e.g., `search books "rust" 25`)
- **Safe async/blocking isolation** via Tokio + `spawn_blocking`

## 🚀 Getting Started

```bash
# Single binary distribution includes the client subcommand
cargo run --bin cameodb -- client -i

# Once built/installed, run the same binary directly
./target/debug/cameodb client -i
```

Flags:
- `--connect http://host:port` – default `http://localhost:9480`
- `--interactive` / `-i` – launch the REPL

## 🧭 Available Commands

| Command | Description |
| --- | --- |
| `health` | Fetch `_cluster/health` and pretty-print response |
| `list indexes` | List all indexes with stats + cached field names |
| `list index <name>` | Show detailed stats and schema for one index |
| `search <index> <query> [limit]` | Run hybrid search with optional limit |
| `connect <host[:port]>` | Switch target server and refresh cache |
| `help` | Display built-in command reference |
| `exit` / `quit` / `\q` | Leave the REPL |

## ⌨️ Completion & History

- Completions are context-aware:
  - Empty line → show commands
  - `list <TAB>` → `indexes`, `index`
  - `list index <TAB>` → known index names
  - `search <index> <TAB>` → field names with `[type]` hints
- History file resides at `~/.cameodb/client_history` and is automatically created.

## 🧠 Index Metadata Cache

The session maintains an `Arc<RwLock<HashMap<String, IndexMetadata>>>` that stores:
- `fields: Vec<FieldInfo>` where every entry includes `{ name, field_type }`

Cache refresh occurs:
1. On REPL startup
2. After `connect` / `conn`
3. After successful `list indexes`
4. After `list index <name>`

This enables:
- Tab completion of index names/fields
- Field type hints for search clauses

## 🔍 Search UX Enhancements

- Commands accept optional numeric limit at the end (`search books author:doe 15`).
- Field completions insert `field_name:` so you can immediately type values.
- Boolean fields display `[true/false]` hints to avoid mis-typed literals.
- Field hints surface human-friendly labels (`[numeric]`, `[decimal]`, `[text]`, `[exact]`, `[true/false]`).
- Leading modifiers (`+required`, `-excluded`, `!prohibited`) are preserved when completing field names.

### Query syntax shortcuts

The CLI mirrors Tantivy's query parser (see [docs](https://docs.rs/tantivy/latest/tantivy/query/struct.QueryParser.html)) and the REPL helps with these patterns:

| Syntax | Example | Notes |
| --- | --- | --- |
| Field scoping | `title:rust` | Works with dotted JSON paths (`cart.product_id:103`). |
| Phrase | `"hybrid search"` | Requires fields indexed with positions. |
| Boolean | `foo AND bar`, `foo OR -bar` | Unary `+`/`-` preserved via completion. |
| Range / comparisons | `price:[10 TO 20]`, `rating:>=4` | Hinter ignores `> < = !` so hints still show. |
| Prefix / wildcard | `tag:rust*` | Type `*` manually after completion. |
| Boost | `title:rust^2` | Manual entry; CLI does not alter caret syntax. |

If Tantivy adds more operators, update this table and (optionally) extend the completer to understand them.

## 🛡️ Async + Blocking Safety

- All HTTP calls remain async (`reqwest::Client`).
- Blocking Rustyline loop runs inside `tokio::task::spawn_blocking` to keep the runtime healthy.
- Cache refreshes perform HTTP fetches without holding locks (build updates first, then swap into `RwLock`).

## 🧪 Development

```bash
# Lint the client crate
cargo clippy -p client --all-targets

# Run tests
cargo test -p client
```

## 📚 Related Files

- `crates/client/src/cli.rs` – REPL entry point & interactive session
- `crates/client/src/sdk.rs` – HTTP client wrapper for CameoDB API
- `crates/client/Cargo.toml` – dependencies (`rustyline`, `dirs`, `reqwest`, ...)

---
The client is ready for cluster operators and developers to explore indexes, run searches, and inspect schemas with ergonomic completions and safe async runtime integration.
