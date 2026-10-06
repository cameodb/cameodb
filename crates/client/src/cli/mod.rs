//! The non-interactive command surface: the clap grammar, top-level dispatch, and
//! the list/output helpers shared with the interactive shell in `shell.rs`. The ingest
//! pipeline lives in `ingest.rs`, and what a source's sample says about its fields in
//! `detect.rs`.

mod detect;
mod ingest;
mod profile;
mod scan;
mod shell;

#[cfg(test)]
mod tests;

pub(crate) use detect::*;
pub(crate) use ingest::*;
pub(crate) use profile::*;
pub(crate) use scan::*;
pub(crate) use shell::*;

use crate::sdk::{CameoClient, ClientAuth, Credential, IndexInfo, ListIndexesResponse, TlsTrust};
use anyhow::{Context, Result, anyhow};
use clap::{Parser, Subcommand, ValueEnum};
use colored_json;
use reqwest::Url;
use serde::Serialize;
use serde_json::Map as JsonMap;
use serde_json::Value as JsonValue;
use serde_json::json;
use std::fs;
use std::path::PathBuf;

// Only import colored on non-Windows platforms

#[derive(Parser)]
#[command(name = "cameodb-client", about = "CameoDB CLI Client")]
pub struct ClientCli {
    /// Enable interactive shell mode
    #[arg(short = 'i', long = "interactive", global = true)]
    pub interactive: bool,

    #[command(subcommand)]
    pub command: Option<ClientCommand>,

    /// CameoDB Server URL
    #[arg(
        short = 'c',
        long = "connect",
        alias = "url",
        default_value = "http://localhost:9480",
        global = true
    )]
    pub connect: String,

    /// Accept invalid TLS certificates from the CameoDB server (self-signed certs in development)
    #[arg(long = "insecure", global = true)]
    pub insecure: bool,

    /// Accept invalid TLS certificates when fetching remote schema/data source URLs.
    ///
    /// Deliberately separate from --insecure: relaxing trust for a data source must not
    /// also relax it for the connection carrying your writes.
    #[arg(long = "insecure-source", global = true)]
    pub insecure_source: bool,

    /// Path to a file holding one API key. Preferred over --api-key: a path is not visible
    /// in `ps` and does not land in shell history.
    #[arg(long = "api-key-file", env = "CAMEODB_API_KEY_FILE", global = true)]
    pub api_key_file: Option<PathBuf>,

    /// API key to present to the server.
    ///
    /// On most systems the full command line of a running process is readable by other
    /// users, so prefer CAMEODB_API_KEY or --api-key-file for anything but a scratch node.
    #[arg(
        long = "api-key",
        env = "CAMEODB_API_KEY",
        hide_env_values = true,
        global = true
    )]
    pub api_key: Option<String>,

    /// Send the API key over plaintext HTTP to a host that is not loopback.
    ///
    /// Deliberately separate from --insecure, which accepts a bad certificate on a
    /// connection that is still encrypted. This one puts a bearer token on the wire in the
    /// clear; only pass it when something else already protects the hop.
    #[arg(long = "allow-plaintext-key", global = true)]
    pub allow_plaintext_key: bool,
}

/// Resolve the key from the four places it may come from.
///
/// A file beats an inline key, and clap has already resolved each flag against its
/// environment variable. So: `--api-key-file` > `CAMEODB_API_KEY_FILE` > `--api-key` >
/// `CAMEODB_API_KEY`. Preferring the file means an exported `CAMEODB_API_KEY` left over from
/// another session cannot quietly override the key a command names explicitly.
fn resolve_credential(cli: &ClientCli) -> Result<Option<Credential>> {
    if let Some(path) = &cli.api_key_file {
        // Silently preferring one of two keys is how someone spends an afternoon wondering
        // which identity their command ran as.
        if cli.api_key.is_some() {
            eprintln!(
                "⚠️  Both an API key and a key file were given; using the file {}.",
                path.display()
            );
        }
        return Credential::from_file(path).map(Some);
    }
    match cli.api_key.as_deref() {
        Some(raw) => Credential::parse(raw)
            .context("--api-key / CAMEODB_API_KEY")
            .map(Some),
        None => Ok(None),
    }
}

fn normalize_connect_target(raw: &str) -> Result<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("Connection target cannot be empty"));
    }

    let candidate = if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        trimmed.to_string()
    } else if trimmed.contains(':') {
        format!("http://{}", trimmed)
    } else {
        format!("http://{}:9480", trimmed)
    };

    // Validate URL format
    Url::parse(&candidate).map_err(|e| anyhow!("Invalid connection URL: {}", e))?;
    Ok(candidate)
}

async fn handle_list_command(
    client: &CameoClient,
    resource: ListResource,
    name: Option<String>,
    include_data_size: bool,
    extended: bool,
) -> Result<Option<ListIndexesResponse>> {
    match resource {
        ListResource::Indexes => {
            let indexes = client.list_indexes(include_data_size).await?;
            let mut entries = Vec::new();
            let mut enriched_indexes = Vec::new();

            // One request for the whole catalogue. Each entry already describes its fields, so
            // the `_config` call this used to make per index — sequentially, so a catalogue of
            // two hundred indexes was two hundred round trips — is gone.
            for index_info in &indexes.indexes {
                let stats = index_stats_json(index_info);
                let compact_fields = format_compact_fields(&index_info.fields);
                entries.push((index_info.name.clone(), stats.clone(), compact_fields));

                if extended {
                    enriched_indexes.push(json!({
                        "name": index_info.name,
                        "stats": stats,
                        "schema": json!({ "fields": index_info.fields }),
                    }));
                }
            }

            if extended {
                let response = json!({
                    "indexes": enriched_indexes,
                    "total_indexes": indexes.total_indexes,
                    "total_shards": indexes.total_shards,
                    "node_id": indexes.node_id,
                });
                print_json(&response)?;
            } else {
                print_compact_indexes_output(
                    &entries,
                    indexes.total_indexes,
                    indexes.total_shards,
                    &indexes.node_id,
                )?;
            }
            Ok(Some(indexes))
        }
        ListResource::Index => {
            let index_name = name.ok_or_else(|| anyhow!("Usage: list index <name>"))?;
            let indexes = client.list_indexes(include_data_size).await?;
            let info = indexes
                .indexes
                .iter()
                .find(|idx| idx.name.eq_ignore_ascii_case(&index_name))
                .ok_or_else(|| {
                    anyhow!(
                        "Index '{}' not found. Use 'list indexes' to see available indexes.",
                        index_name
                    )
                })?;

            // No second request: the listing entry already describes the index in full. It is
            // the same shape `/api/{index}/_config` returns, so both read the same either way.
            let stats = index_stats_json(info);

            if extended {
                let enriched = json!({
                    "index": info.name,
                    "stats": stats,
                    "schema": json!({ "fields": info.fields }),
                });
                print_json(&enriched)?;
            } else {
                let compact_fields = format_compact_fields(&info.fields);
                print_compact_index_output(&info.name, &stats, &compact_fields)?;
            }
            Ok(Some(indexes))
        }
    }
}

fn print_compact_index_output(index: &str, stats: &JsonValue, fields: &JsonValue) -> Result<()> {
    let (open, close) = color_json_braces();
    println!("{}", open);
    print_compact_index_entry_body("index", index, stats, fields, "  ")?;
    println!("{}", close);
    Ok(())
}

/// Shared helper to print the body of a compact index entry (name/stats/fields).
fn print_compact_index_entry_body(
    key: &str,
    value: &str,
    stats: &JsonValue,
    fields: &JsonValue,
    indent: &str,
) -> Result<()> {
    let fields_obj = fields
        .as_object()
        .ok_or_else(|| anyhow!("Expected compact fields to be an object"))?;

    // Print key-value line
    println!(
        "{}  {}: {},",
        indent,
        color_json_key(key),
        color_json_string(value)
    );

    // Print stats via colored_json
    let stats_wrapper = json!({ "stats": stats });
    let colored_stats = colored_json::to_colored_json_auto(&stats_wrapper).unwrap_or_default();
    let stats_lines: Vec<&str> = colored_stats.lines().collect();
    for line in stats_lines
        .iter()
        .skip(1)
        .take(stats_lines.len().saturating_sub(2))
    {
        println!("{}  {}", indent, line.trim());
    }

    // Print compact fields section with colored key and braces
    let fields_key_colored = color_json_key("fields");
    let (open, close) = color_json_braces();
    println!("{}  {}: {}", indent, fields_key_colored, open);
    let field_indent = format!("{}    ", indent);
    let mut iter = fields_obj.iter().peekable();
    while let Some((field_name, props)) = iter.next() {
        let comma = if iter.peek().is_some() { "," } else { "" };
        let entry = json!({ field_name: props });
        let colored = colored_json::to_colored_json_auto(&entry).unwrap_or_default();
        let inline = collapse_single_field_colored(&colored);
        println!("{}{}{}", field_indent, inline, comma);
    }

    println!("{}  {}", indent, close);
    Ok(())
}

/// Collapse a single-field colored JSON object to an inline entry.
///
/// Input (from colored_json pretty-print of `{"field_name": {...}}`):
///   {
///     "field_name": {
///       "type": "text"
///     }
///   }
///
/// Output:
///   "field_name": { "type": "text" }
///
/// This works because we strip the first `{` and last `}` lines, then
/// join the remaining (indented) lines with single spaces.
fn collapse_single_field_colored(colored: &str) -> String {
    let lines: Vec<&str> = colored.lines().collect();
    if lines.len() < 3 {
        return colored.to_string();
    }
    lines[1..lines.len() - 1] // skip first `{` and last `}`
        .iter()
        .map(|l| l.trim())
        .collect::<Vec<_>>()
        .join(" ")
}

/// Print multiple indexes with compact single-line field entries.
fn print_compact_indexes_output(
    indexes: &[(String, JsonValue, JsonValue)],
    total_indexes: usize,
    total_shards: usize,
    node_id: &str,
) -> Result<()> {
    let (open, close) = color_json_braces();
    println!("{}", open);
    println!("  {}: [", color_json_key("indexes"));

    for (i, (name, stats, fields)) in indexes.iter().enumerate() {
        let comma = if i < indexes.len() - 1 { "," } else { "" };
        println!("    {}", open);
        print_compact_index_entry_body("name", name, stats, fields, "      ")?;
        println!("    {}{}", close, comma);
    }

    println!("  ],");
    println!("  {}: {},", color_json_key("total_indexes"), total_indexes);
    println!("  {}: {},", color_json_key("total_shards"), total_shards);
    println!(
        "  {}: {},",
        color_json_key("node_id"),
        serde_json::to_string(node_id)?
    );
    println!("{}", close);
    Ok(())
}

/// Extract a colored JSON key string matching colored_json's cyan key color scheme.
fn color_json_key(key: &str) -> String {
    let dummy = json!({ key: serde_json::Value::Null });
    let colored = colored_json::to_colored_json_auto(&dummy).unwrap_or_default();
    colored
        .lines()
        .nth(1)
        .and_then(|line| line.split(':').next())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| format!("\"{}\"", key))
}

/// Extract a colored JSON string value matching colored_json's green string color scheme.
fn color_json_string(value: &str) -> String {
    let dummy = json!({ "k": value });
    let colored = colored_json::to_colored_json_auto(&dummy).unwrap_or_default();
    colored
        .lines()
        .nth(1)
        .and_then(|line| line.split(':').nth(1))
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| serde_json::to_string(value).unwrap_or_default())
}

/// Extract colored braces from colored_json output for consistent coloring.
fn color_json_braces() -> (String, String) {
    ("{".to_string(), "}".to_string())
}

/// The statistics a listing entry carries, as the client displays them.
///
/// Megabytes are computed here, once, from the bytes the node reports. They used to arrive
/// pre-rounded, which the cluster listing then summed across nodes.
fn index_stats_json(info: &IndexInfo) -> JsonValue {
    let mut stats = serde_json::Map::new();
    stats.insert("document_count".to_string(), json!(info.document_count));
    if let Some(total_size) = info.total_size_bytes {
        stats.insert("total_size_bytes".to_string(), json!(total_size));
    }
    if let Some(mb) = IndexInfo::megabytes(info.index_size_bytes) {
        stats.insert("index_size_mb".to_string(), json!(mb));
    }
    if let Some(mb) = IndexInfo::megabytes(info.data_size_bytes) {
        stats.insert("data_size_mb".to_string(), json!(mb));
    }
    stats.insert("shard_count".to_string(), json!(info.shard_count));
    JsonValue::Object(stats)
}

/// Format schema fields as compact one-line-per-field JSON objects.
///
/// Only non-default properties are shown:
/// - `indexed` is omitted when true (all fields are indexed by default)
/// - `stored` is omitted when false (most fields are not stored)
/// - `fast` is omitted when false
/// - `tokenizer` is omitted when absent or "default"
/// - `shadow` is omitted when false
/// - `searchable` is omitted when true — it differs from `indexed` only for a field the
///   schema declares but the built index has no column for
/// - `description` is omitted when absent
fn format_compact_fields(fields: &[JsonValue]) -> JsonValue {
    let mut result = JsonMap::new();
    for field_val in fields {
        let Some(name) = field_val.get("name").and_then(|v| v.as_str()) else {
            continue;
        };
        let mut compact = JsonMap::new();

        if let Some(ft) = field_val.get("type") {
            compact.insert("type".to_string(), ft.clone());
        }

        // indexed: only show when false (default is true)
        if let Some(JsonValue::Bool(false)) = field_val.get("indexed") {
            compact.insert("indexed".to_string(), json!(false));
        }

        // A field the schema declares but the built index has no column for. It is `indexed`
        // and yet matches nothing until the index data is rebuilt, so saying only `indexed`
        // here would describe it as ready to query.
        if let Some(JsonValue::Bool(false)) = field_val.get("searchable") {
            compact.insert("searchable".to_string(), json!(false));
        }

        // stored: only show when true (default is false)
        if let Some(JsonValue::Bool(true)) = field_val.get("stored") {
            compact.insert("stored".to_string(), json!(true));
        }

        // fast: only show when true (default is false)
        if let Some(JsonValue::Bool(true)) = field_val.get("fast") {
            compact.insert("fast".to_string(), json!(true));
        }

        // tokenizer: only show when present and not "default"
        if let Some(JsonValue::String(tok)) = field_val.get("tokenizer")
            && tok != "default"
        {
            compact.insert("tokenizer".to_string(), json!(tok));
        }

        // shadow: only show when true
        if let Some(JsonValue::Bool(true)) = field_val.get("shadow") {
            compact.insert("shadow".to_string(), json!(true));
        }

        // description: the one property with no default to fall back on, so it is shown
        // whenever it exists.
        if let Some(JsonValue::String(text)) = field_val.get("description") {
            compact.insert("description".to_string(), json!(text));
        }

        result.insert(name.to_string(), JsonValue::Object(compact));
    }
    JsonValue::Object(result)
}

#[derive(Subcommand)]
pub enum ClientCommand {
    /// Check cluster health
    Health,

    /// List cluster resources
    List {
        /// Resource type to list
        #[arg(value_enum, default_value_t = ListResource::Indexes)]
        resource: ListResource,
        /// Name of the resource (required for `list index <name>`)
        name: Option<String>,
        /// Show full extended schema with all field properties (for list indexes, also fetches and displays field schemas for each index)
        #[arg(long, default_value_t = false)]
        extended: bool,
        /// Include data size information (default: false)
        #[arg(long, default_value_t = false)]
        data_size: bool,
    },

    /// Search an index
    Search {
        /// Index name
        index: String,
        /// Query string
        query: String,
        /// Max results
        #[arg(short, long)]
        limit: Option<usize>,
        /// Skip this many results — the page, where --limit is the page size
        #[arg(short = 'o', long)]
        offset: Option<usize>,
    },

    /// Schema utilities
    Schema {
        /// Operation to perform
        #[arg(value_enum)]
        operation: SchemaOperation,
        /// `detect <FILE>`, or `load <INDEX> <FILE>` — the index first, as `data load` takes it.
        /// FILE is a path or HTTP(S) URL to a schema or data source
        #[arg(value_name = "[INDEX] FILE", num_args = 1..=2, required = true)]
        args: Vec<String>,
        /// Target index name for `load`, when it is not given before the file
        #[arg(long, short = 'n')]
        index: Option<String>,
        /// Delimiter override (default: auto-detect first line)
        #[arg(long, value_enum, default_value_t = Delimiter::Detect)]
        delimiter: Delimiter,
        /// The column that identifies each row, or several comma-separated whose values joined
        /// with `|` make the id. Several stay fields of their own; one not named `id` becomes a
        /// shadow field. Default: detected from the scanned rows.
        #[arg(long = "id", value_name = "COLUMN[,COLUMN...]")]
        id: Option<String>,
        /// Print how the source was scanned and why each column became the field it did,
        /// instead of the schema (`detect` only)
        #[arg(long, default_value_t = false)]
        report: bool,
    },

    /// Data ingestion
    Data {
        /// Operation to perform
        #[arg(value_enum)]
        operation: DataOperation,
        /// Target index name
        index: String,
        /// Path or HTTP(S) URL to data source
        file: String,
        /// Delimiter override (default: auto-detect first line)
        #[arg(long, value_enum, default_value_t = Delimiter::Detect)]
        delimiter: Delimiter,
        /// Maximum documents per batch
        #[arg(long, default_value_t = DEFAULT_BATCH_SIZE)]
        batch_size: usize,
        /// The column that identifies each row, or several comma-separated whose values joined
        /// with `|` make the id. Default: the id the index records, else detected from the
        /// scanned rows. Another id than the index records needs the index empty, or --recreate.
        #[arg(long = "id", value_name = "COLUMN[,COLUMN...]")]
        id: Option<String>,
        /// Delete the index's documents first, keeping its schema, then load. What a change of
        /// --id, or of a field's type or tokenizer, needs on an index that holds documents.
        #[arg(long, default_value_t = false)]
        recreate: bool,
    },

    /// Delete documents from an index, or the whole index
    ///
    /// With no `--id` and no `--ids-file`, deletes the index itself, which is what this
    /// command has always meant. Naming documents deletes those instead and leaves the
    /// index in place.
    Delete {
        /// Target index name
        index: String,
        /// Delete these documents rather than the index: one id, or several comma-separated.
        /// Repeatable.
        #[arg(long = "id", value_name = "ID[,ID...]")]
        ids: Vec<String>,
        /// Delete the documents whose ids are in this file, one per line. A line is one id
        /// taken whole, so this is where an id containing a comma can be named.
        #[arg(long, value_name = "PATH")]
        ids_file: Option<String>,
        /// Routing key for the named documents, where the index routes by a field that is
        /// not the document key
        #[arg(long, value_name = "KEY")]
        routing_key: Option<String>,
        /// Also delete stored schema/config. Only meaningful when deleting the index.
        #[arg(long, default_value_t = false)]
        delete_schema: bool,
    },

    /// Admin operations
    Admin {
        /// Admin subcommand
        #[command(subcommand)]
        subcommand: AdminCommand,
    },
}

#[derive(Subcommand)]
pub enum AdminCommand {
    /// Memory management operations
    Memory {
        /// Memory operation to perform
        #[arg(value_enum)]
        operation: MemoryOperation,
        /// Force aggressive purge (ignore decay timers, purge all pages immediately)
        #[arg(long, default_value_t = false)]
        force: bool,
    },
    /// Index admin operations
    Index {
        /// Target index name
        index: String,
        /// Operation to perform
        #[arg(value_enum)]
        operation: IndexAdminOperation,
    },
    /// Worker pool statistics (queue depth, jobs completed, dispatch metrics)
    Workers,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum MemoryOperation {
    /// Show memory statistics (process + jemalloc)
    Stats,
    /// Trigger jemalloc memory purge
    Purge,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum IndexAdminOperation {
    /// Force commit the index writer
    Commit,
    /// Evict the index writer from cache
    EvictWriter,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum SchemaOperation {
    /// Detect schema from CSV, JSON, JSONL, or NDJSON
    Detect,
    /// Detect schema and apply it to an index
    Load,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum DataOperation {
    /// Load CSV, JSON, JSONL, or NDJSON data into an index
    Load,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum Delimiter {
    /// Auto-detect using first line (default)
    Detect,
    /// Comma-separated
    Comma,
    /// Tab-separated
    Tab,
    /// Semicolon-separated
    Semicolon,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum ListResource {
    /// List all indexes (default)
    Indexes,
    /// Show details for a single index (requires a name)
    Index,
}

/// One `--id` value, which may name several ids: `--id b1,b2` and `--id b1 --id b2` are the same
/// request.
///
/// Commas rather than spaces, because a space-separated list would need quoting to survive the
/// shell and would be ambiguous against the positional index name. Blank segments are dropped, so
/// a trailing comma or a doubled one costs nothing.
///
/// Shared with the REPL, which parses its own flags: one rule, or the two syntaxes drift.
fn split_ids(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|id| !id.is_empty())
        .map(str::to_string)
        .collect()
}

/// The document ids a `delete` names, or `None` when it names none and therefore means the index.
///
/// Both sources are read together so `--id` and `--ids-file` compose; a file that contains no ids
/// is an error rather than a silent fall-through to deleting the index, which is the one mistake
/// this command must not make quietly.
///
/// A `--id` value may name several ids at once, comma-separated. A file line may not: it is one
/// id, taken whole. That asymmetry is deliberate — an id containing a comma is a legal id, and
/// the file is where it can still be named.
///
/// The rule is uniform in the direction that matters: naming ids in *any* form and yielding none
/// is an error, and only naming none at all means the index.
fn collect_delete_ids(ids: &[String], ids_file: Option<&str>) -> Result<Option<Vec<String>>> {
    let mut named: Vec<String> = ids.iter().flat_map(|value| split_ids(value)).collect();

    // `--id` was given and named nothing usable — `--id ,` or `--id ""`. Falling through would
    // delete the *index*, which is the one direction this command must never take by accident,
    // and it is the same refusal an ids file with no ids gets below.
    if !ids.is_empty() && named.is_empty() {
        return Err(anyhow!(
            "--id named no document ids; refusing to fall back to deleting the index"
        ));
    }

    if let Some(path) = ids_file {
        let contents = fs::read_to_string(path)
            .with_context(|| format!("Failed to read ids from '{}'", path))?;
        let from_file: Vec<String> = contents
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty() && !line.starts_with('#'))
            .map(str::to_string)
            .collect();
        if from_file.is_empty() {
            return Err(anyhow!(
                "'{}' contains no ids; refusing to fall back to deleting the index",
                path
            ));
        }
        named.extend(from_file);
    }

    named.retain(|id| !id.is_empty());
    Ok((!named.is_empty()).then_some(named))
}

/// Delete one document or many, choosing the endpoint that fits.
///
/// One id goes to the single-document route so the answer names the shard that served it; more
/// than one goes to the bulk route, which groups them by shard and takes one transaction each.
async fn delete_named_documents(
    client: &CameoClient,
    index: &str,
    ids: &[String],
    routing_key: Option<&str>,
) -> Result<JsonValue> {
    if let [only] = ids {
        return client.delete_document(index, only, routing_key).await;
    }

    let entries: Vec<JsonValue> = ids
        .iter()
        .map(|id| match routing_key {
            Some(key) => json!({"id": id, "routing_key": key}),
            None => json!(id),
        })
        .collect();
    client.delete_documents(index, &entries).await
}

pub async fn run_cli() -> Result<()> {
    let raw_args: Vec<String> = std::env::args().collect();
    let program = raw_args
        .first()
        .cloned()
        .unwrap_or_else(|| "cameodb".to_string());

    let mut filtered_args = Vec::with_capacity(raw_args.len());
    filtered_args.push(program);

    // Drop the first occurrence of "client" (if any) so users can run
    // `cameodb client ...`, `cameodb -i client`, etc.
    let mut client_removed = false;
    for arg in raw_args.into_iter().skip(1) {
        if !client_removed && arg == "client" {
            client_removed = true;
            continue;
        }
        filtered_args.push(arg);
    }

    let cli = ClientCli::parse_from(filtered_args);

    let normalized_connect = normalize_connect_target(&cli.connect)?;
    let trust = TlsTrust {
        insecure_server: cli.insecure,
        insecure_source: cli.insecure_source,
    };
    let auth = ClientAuth {
        credential: resolve_credential(&cli)?,
        allow_plaintext: cli.allow_plaintext_key,
    };

    if cli.interactive {
        return run_interactive_shell(normalized_connect, trust, auth).await;
    }

    let client = CameoClient::new_with_options(&normalized_connect, trust, auth)?;

    let command = cli.command.ok_or_else(|| {
        anyhow!(
            "No command provided. Provide a subcommand (e.g. `health`) or use --interactive/-i."
        )
    })?;

    match command {
        ClientCommand::Health => {
            let health = client.health().await?;
            print_json(&health)?;
        }
        ClientCommand::List {
            resource,
            name,
            extended,
            data_size,
        } => {
            handle_list_command(&client, resource, name, data_size, extended).await?;
        }
        ClientCommand::Search {
            index,
            query,
            limit,
            offset,
        } => {
            if query.trim().is_empty() {
                anyhow::bail!("Query cannot be empty");
            }
            // Inline `return`, `limit`, `offset` and `sort` are read by the server, which owns
            // the one definition of where a modifier run may appear. The flags here win over
            // the inline form, which is what lets a caller page through a query it did not
            // write itself.
            let results = client
                .search(&index, &query, limit, offset, None, None)
                .await?;
            print_json(&results)?;
        }
        ClientCommand::Schema {
            operation,
            args,
            index,
            delimiter,
            id,
            report,
        } => {
            let id = id.as_deref().map(IdSpec::parse).transpose()?;
            let (index, file) = schema_targets(operation, args, index)?;
            match operation {
                SchemaOperation::Detect if report => {
                    print!(
                        "{}",
                        report_source(&client, &file, delimiter, id.as_ref()).await?
                    );
                }
                SchemaOperation::Detect => {
                    let schema_json =
                        detect_schema_from_source(&client, &file, delimiter, id.as_ref()).await?;
                    print_json(&schema_json)?;
                }
                SchemaOperation::Load => {
                    let index_name = index
                        .as_deref()
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                        .ok_or_else(|| {
                            anyhow!("Index name is required. Usage: schema load <index> <file>")
                        })?;

                    if report {
                        return Err(anyhow!("--report applies to schema detect"));
                    }
                    let schema_json =
                        load_schema_from_source(&client, &file, delimiter, id.as_ref()).await?;
                    client.put_index_config(index_name, &schema_json).await?;
                    println!("Schema applied to index '{}'", index_name);
                }
            }
        }
        ClientCommand::Data {
            operation,
            index,
            file,
            delimiter,
            batch_size,
            id,
            recreate,
        } => match operation {
            DataOperation::Load => {
                let id = id.as_deref().map(IdSpec::parse).transpose()?;
                load_data_from_source(
                    &client,
                    &index,
                    &file,
                    delimiter,
                    batch_size,
                    id.as_ref(),
                    recreate,
                )
                .await?;
            }
        },
        ClientCommand::Delete {
            index,
            ids,
            ids_file,
            routing_key,
            delete_schema,
        } => {
            let named = collect_delete_ids(&ids, ids_file.as_deref())?;
            match named {
                Some(named) => {
                    if delete_schema {
                        return Err(anyhow!(
                            "--delete-schema deletes the index, which is not what naming \
                             documents asks for; drop one of the two"
                        ));
                    }
                    let result =
                        delete_named_documents(&client, &index, &named, routing_key.as_deref())
                            .await?;
                    print_json(&result)?;
                }
                None => {
                    let result = client.delete_index(&index, delete_schema).await?;
                    print_json(&result)?;
                }
            }
        }
        ClientCommand::Admin { subcommand } => match subcommand {
            AdminCommand::Memory { operation, force } => match operation {
                MemoryOperation::Stats => {
                    let result = client.admin_memory_stats().await?;
                    print_json(&result)?;
                }
                MemoryOperation::Purge => {
                    let result = client.admin_memory_purge(force).await?;
                    print_json(&result)?;
                }
            },
            AdminCommand::Index { index, operation } => match operation {
                IndexAdminOperation::Commit => {
                    let result = client.admin_index_commit(&index).await?;
                    print_json(&result)?;
                }
                IndexAdminOperation::EvictWriter => {
                    let result = client.admin_index_evict_writer(&index).await?;
                    print_json(&result)?;
                }
            },
            AdminCommand::Workers => {
                let result = client.admin_worker_stats().await?;
                print_json(&result)?;
            }
        },
    }

    Ok(())
}

fn print_json<T: Serialize>(val: &T) -> Result<()> {
    let pretty = serde_json::to_string_pretty(val)?;
    let value: JsonValue = serde_json::from_str(&pretty)?;
    match colored_json::to_colored_json_auto(&value) {
        Ok(colored) => {
            println!("{}", colored);
        }
        Err(_) => {
            println!("{}", pretty);
        }
    }
    Ok(())
}

/// The index and file `schema` names: `detect <file>`, `load <index> <file>`, or — as it was
/// first written — `load <file> --index <index>`.
fn schema_targets(
    operation: SchemaOperation,
    args: Vec<String>,
    index: Option<String>,
) -> Result<(Option<String>, String)> {
    let mut args = args.into_iter();
    let (first, second) = (args.next(), args.next());
    match (operation, first, second) {
        (SchemaOperation::Detect, Some(file), None) => Ok((index, file)),
        (SchemaOperation::Detect, _, _) => Err(anyhow!("Usage: schema detect <file>")),
        (SchemaOperation::Load, Some(named), Some(file)) => match index {
            Some(flag) if flag != named => Err(anyhow!(
                "schema load names two indexes, '{named}' and --index '{flag}'"
            )),
            _ => Ok((Some(named), file)),
        },
        (SchemaOperation::Load, Some(file), None) => Ok((index, file)),
        (SchemaOperation::Load, None, _) => Err(anyhow!("Usage: schema load <index> <file>")),
    }
}
