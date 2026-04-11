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
- **Colorized JSON output** for all responses (similar to `jq`), with plain fallback

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
| `search <index> <query> [limit]` | Run hybrid search with optional limit (inline `limit N` or `--limit N`) |
| `schema detect <file> [--delimiter ...]` | Detect schema from CSV/TSV/JSON/JSONL/NDJSON (auto or forced delimiter, supports compression & HTTP(S)) |
| `schema load <index> <file> [--delimiter ...]` | Detect schema from CSV/TSV/JSON/JSONL/NDJSON and apply to an index (supports compression & HTTP(S)) |
| `data load <index> <file> [--delimiter ...] [--batch-size N]` | Ingest CSV/TSV/JSON/JSONL/NDJSON data in batches (supports compression & HTTP(S)) |
| `delete <index> [--delete-schema]` | Delete an index; prompts `Delete index "<name>"? [yes/NO]:` and only proceeds on `yes` |
| `connect <host[:port]>` | Switch target server and refresh cache |
| `help` | Display built-in command reference |
| `exit` / `quit` / `\q` | Leave the REPL |

### Data & Schema helpers

- `schema detect <file> [--delimiter detect|comma|tab|semicolon]` – auto-detect schema from CSV, TSV, JSON, JSONL, or NDJSON. Supports local files and HTTP(S) URLs. Automatically decompresses Gzip (.gz/.gzip) and Zip archives.
- `schema load <index> <file> [--delimiter ...]` – detect schema from CSV, TSV, JSON, JSONL, or NDJSON and apply it to an index. Supports local files and HTTP(S) URLs. Automatically decompresses Gzip (.gz/.gzip) and Zip archives.
- `data load <index> <file> [--delimiter ...] [--batch-size N]` – ingest CSV, TSV, JSON, JSONL, or NDJSON data in batches. Supports local files and HTTP(S) URLs. Automatically decompresses Gzip (.gz/.gzip) and Zip archives. Default batch size is 4000 documents.

### 🗜️ Compression & Remote Sources

The CLI client features robust support for compressed data and remote HTTP(S) sources:

- **Compression Formats:** Automatically detects and decompresses `Gzip (.gz/.gzip)` and `Zip` archives on the fly.
- **HTTP(S) Streaming:** Load data directly from public URLs (e.g., Hugging Face datasets, S3 presigned URLs) without downloading locally first.
- **Zero-Copy Processing:** Decompression happens in-memory for optimal performance.

**Examples:**
```bash
# Load from a compressed remote JSONL dataset
cameodb client data load hugdata https://huggingface.co/datasets/.../data.jsonl.gz

# Load from a local Zip archive
cameodb client schema detect ./data/archive.zip
```

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

- Commands accept optional numeric limit either as a flag (`--limit 10` or `-l 10`) or inline at the end of the query (`search books author:doe 15`).
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
| Sort | `sort field:desc` | Sort by FAST field (u64/date). Order optional (defaults to desc). |

If Tantivy adds more operators, update this table and (optionally) extend the completer to understand them.

### Inline query modifiers

CameoDB supports inline modifiers in the query string for convenience:

| Modifier | Syntax | Example | Description |
| --- | --- | --- | --- |
| Return fields | `return field1,field2` | `title:rust return title,author` | Project only specific fields |
| Limit results | `limit N` | `title:rust limit 10` | Limit number of results |
| Sort results | `sort field:order` | `title:rust sort year:desc` | Sort by FAST field |

**Sorting Details:**
- Supported field types: `u64` and `date` (both must be marked as FAST)
- Order can be `asc` or `desc` (defaults to `desc`)
- CLI provides autocomplete for sortable fields and `:asc`/:desc` suffixes
- Example: `search books title:rust sort publication_year:asc limit 20`

**Combined Example:**
```bash
search books +title:rust +publication_date:[2020 TO *] return title,author,year limit 10 sort year:desc
```

### Date field queries

CameoDB automatically normalizes date literals in search queries to match the indexed format. You can use **naive date formats** directly without manual RFC3339 conversion:

**Supported Date Query Formats:**

| Input Format | Example Query | Normalized To | Use Case |
| --- | --- | --- | --- |
| Year-month | `publication_date:2024-06` | `publication_date:2024-06-01T00:00:00Z` | Match documents from a specific month |
| Year-only | `publication_date:2024` | `publication_date:2024-01-01T00:00:00Z` | Match documents from a specific year |
| Date-only | `publication_date:2024-01-05` | `publication_date:2024-01-05T00:00:00Z` | Match documents from a specific day |
| Naive datetime | `publication_date:2024-01-05 12:00:00` | `publication_date:2024-01-05T12:00:00Z` | Match documents at a specific time |
| RFC3339 | `publication_date:2024-01-05T12:00:00Z` | No change | Already normalized |

**Range Queries:**

```bash
# All documents from 2001 onwards
search books publication_date:[2001 TO *]

# Documents between 2001 and 2014 (inclusive start of each year)
search books publication_date:[2001 TO 2014]

# Documents from a specific date range
search books publication_date:[2020-01-01 TO 2020-12-31]

# Documents from a specific month range
search books publication_date:[2020-06 TO 2020-12]

# Documents from a specific month to present
search books publication_date:[2024-06 TO *]
```

**Comparison Queries:**

```bash
# Documents published after 2011
search books publication_date:>2011

# Documents published before 2009-01-01
search books publication_date:<2009-01-01

# Documents published on or after a specific date
search books publication_date:>=2020-06-15

# Documents published after a specific month
search books publication_date:>2024-06

# Documents published before a specific month
search books publication_date:<2020-12
```

**Combined Queries:**

```bash
# Recent Rust books
search books +title:rust +publication_date:[2020 TO *]

# Classic database books
search books +title:database +publication_date:<2000

# Books from a specific year
search books +title:programming +publication_date:2024

# Books from a specific month
search books +title:machine +publication_date:2024-06
```

**Important Notes:**
- All naive dates are interpreted as **UTC midnight** for date-only, year-month, and year-only inputs
- Naive datetime inputs (without timezone) are assumed to be **UTC**
- The server automatically normalizes your input before querying Tantivy
- Original document JSON is preserved exactly as written (normalization only affects indexing and search)

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
- `crates/client/Cargo.toml` – dependencies (`rustyline`, `dirs`, `reqwest`, `flate2`, `zip`, ...)
- `examples/ingest_books.py` – book summaries loader (defaults to `examples/data/booksummaries.tsv`, tab-delimited, skips header row)
- `examples/ingest_ted.py` – TED YouTube loader (defaults to `examples/data/youtube_ted_2024.csv`, semicolon-delimited, skips header row)

---
The client is ready for cluster operators and developers to explore indexes, run searches, and inspect schemas with ergonomic completions and safe async runtime integration.
