use crate::sdk::{CameoClient, ListIndexesResponse};
use anyhow::{Context, Result, anyhow};
use clap::{Parser, Subcommand, ValueEnum};
use colored_json;
use csv::ReaderBuilder;
use flate2::read::GzDecoder;
use reqwest::Url;
use rustyline::completion::{Completer, Pair};
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::validate::{ValidationContext, ValidationResult, Validator};
use rustyline::{Editor, Helper, error::ReadlineError};
use serde::Serialize;
use serde_json::Map as JsonMap;
use serde_json::Value as JsonValue;
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{BufRead, BufReader, Cursor, Read};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::Duration;
use storage::{FieldDef, IndexSchema, TantivyFieldType};

// Only import colored on non-Windows platforms
#[cfg(not(target_os = "windows"))]
use colored::Colorize;

const SCHEMA_SAMPLE_LIMIT: usize = 200;
const DEFAULT_BATCH_SIZE: usize = 4000;
const SOURCE_SNIFF_BYTES: usize = 64 * 1024;

/// Simple progress spinner for long-running operations
struct ProgressSpinner {
    active: Arc<std::sync::atomic::AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
}

impl ProgressSpinner {
    fn new() -> Self {
        let active = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let active_clone = active.clone();

        let handle = thread::spawn(move || {
            // Use different spinner characters based on platform
            let spinner_chars: Vec<char> = if cfg!(target_os = "windows") {
                // Windows PowerShell/Command Prompt compatible characters
                vec!['|', '/', '-', '\\']
            } else {
                // Unix-like systems - Unicode braille characters
                vec!['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏']
            };
            let mut i = 0;

            while active_clone.load(std::sync::atomic::Ordering::Relaxed) {
                print!("\r{} ", spinner_chars[i % spinner_chars.len()]);
                std::io::Write::flush(&mut std::io::stdout()).ok();
                i += 1;
                // Use slightly slower timing on Windows for better visibility
                let sleep_ms = if cfg!(target_os = "windows") {
                    150
                } else {
                    100
                };
                thread::sleep(Duration::from_millis(sleep_ms));
            }
            // Clear the spinner character when done, leaving cursor at start of line
            print!("\r");
            std::io::Write::flush(&mut std::io::stdout()).ok();
        });

        Self {
            active,
            handle: Some(handle),
        }
    }

    fn stop(&mut self) {
        // Signal the thread to stop
        self.active
            .store(false, std::sync::atomic::Ordering::Relaxed);

        // Wait for the thread to finish
        if let Some(handle) = self.handle.take() {
            handle.join().ok();
        }
    }
}

fn parse_query_modifiers(query: &str) -> (String, Option<usize>, Option<Vec<String>>) {
    let parts: Vec<&str> = query.split_whitespace().collect();
    if parts.is_empty() {
        return (String::new(), None, None);
    }

    let mut cleaned_end = parts.len();
    let mut inline_limit = None;
    let mut inline_fields = None;
    let mut return_idx = None;
    let mut limit_idx = None;

    for (idx, token) in parts.iter().enumerate() {
        if token.eq_ignore_ascii_case("return") && return_idx.is_none() {
            return_idx = Some(idx);
        } else if token.eq_ignore_ascii_case("limit") && limit_idx.is_none() {
            limit_idx = Some(idx);
        }
    }

    if let Some(idx) = return_idx
        && idx + 1 < parts.len()
    {
        let field_end = match limit_idx {
            Some(l_idx) if l_idx > idx => l_idx,
            _ => parts.len(),
        };
        let field_slice = &parts[idx + 1..field_end];
        let field_str = field_slice.join(" ");
        let fields: Vec<String> = field_str
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect();
        if !fields.is_empty() {
            inline_fields = Some(fields);
            cleaned_end = idx.min(cleaned_end);
        }
    }

    if let Some(idx) = limit_idx {
        let value_idx = idx + 1;
        if value_idx < parts.len()
            && let Ok(n) = parts[value_idx].parse::<usize>()
        {
            inline_limit = Some(n);
            cleaned_end = cleaned_end.min(idx);
        }
    }

    let cleaned_query = if cleaned_end == 0 {
        "".to_string()
    } else if cleaned_end >= parts.len() {
        query.to_string()
    } else {
        parts[..cleaned_end].join(" ")
    };

    (
        cleaned_query.trim().to_string(),
        inline_limit,
        inline_fields,
    )
}

impl Drop for ProgressSpinner {
    fn drop(&mut self) {
        self.stop();
    }
}

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
}

fn parse_header_with_hint(raw: &str) -> (String, Option<TantivyFieldType>) {
    let mut parts = raw.splitn(2, '.');
    let name = parts.next().unwrap_or("").to_string();
    let hint = parts.next().and_then(map_type_hint);
    (name, hint)
}

fn map_type_hint(hint: &str) -> Option<TantivyFieldType> {
    match hint.to_lowercase().as_str() {
        "text" | "string" => Some(TantivyFieldType::Text),
        // No dedicated Exact variant; use String (untokenized) for exact semantics
        "exact" => Some(TantivyFieldType::String),
        "numeric" | "number" | "int" | "i64" | "integer" | "u64" => Some(TantivyFieldType::I64),
        "decimal" | "float" | "double" | "f64" => Some(TantivyFieldType::F64),
        "date" => Some(TantivyFieldType::Date),
        "timestamp" => Some(TantivyFieldType::Date),
        "bool" | "boolean" | "true" | "false" => Some(TantivyFieldType::Boolean),
        "ip" => Some(TantivyFieldType::Ip),
        "json" => Some(TantivyFieldType::Json),
        _ => None,
    }
}

#[derive(Debug, Clone)]
struct FieldInfo {
    name: String,
    field_type: String,
}

#[derive(Debug, Clone)]
struct IndexMetadata {
    fields: Vec<FieldInfo>,
}

#[derive(Debug)]
struct InteractiveSession {
    current_url: String,
    client: CameoClient,
    index_cache: Arc<RwLock<HashMap<String, IndexMetadata>>>,
}

impl InteractiveSession {
    fn new(initial_url: String) -> Result<Self> {
        let client = CameoClient::new(&initial_url)?;
        Ok(Self {
            current_url: initial_url,
            client,
            index_cache: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    fn reconnect(&mut self, target: &str) -> Result<()> {
        let normalized = normalize_connect_target(target)?;
        self.client = CameoClient::new(&normalized)?;
        self.current_url = normalized;
        self.clear_index_cache();
        Ok(())
    }

    fn client(&self) -> &CameoClient {
        &self.client
    }

    fn index_cache_handle(&self) -> Arc<RwLock<HashMap<String, IndexMetadata>>> {
        Arc::clone(&self.index_cache)
    }

    fn clear_index_cache(&self) {
        if let Ok(mut cache) = self.index_cache.write() {
            cache.clear();
        }
    }

    async fn refresh_index_cache(&self) {
        if let Ok(indexes) = self.client.list_indexes(false).await {
            self.update_index_cache(&indexes).await;
        }
    }

    async fn update_index_cache(&self, response: &ListIndexesResponse) {
        let mut cache_updates = HashMap::new();

        for idx in &response.indexes {
            let mut fields = Vec::new();

            // Fetch schema to get field types
            if let Ok(config) = self.client.get_index_config(&idx.name).await
                && let JsonValue::Object(schema_fields) = config.fields
            {
                for (field_name, field_info) in schema_fields {
                    if let JsonValue::Object(info) = field_info {
                        let field_type = info
                            .get("field_type")
                            .and_then(|t| t.as_str())
                            .unwrap_or("text"); // Default to text

                        fields.push(FieldInfo {
                            name: field_name.clone(),
                            field_type: field_type.to_string(),
                        });
                    }
                }
            }

            cache_updates.insert(idx.name.clone(), IndexMetadata { fields });
        }

        // Update cache in one operation
        if let Ok(mut cache) = self.index_cache.write() {
            *cache = cache_updates;
        }
    }

    fn prompt(&self) -> String {
        let host = self.display_host();

        // Plain prompt on Windows to avoid ANSI cursor issues; colored Bash-style elsewhere
        #[cfg(target_os = "windows")]
        {
            format!("cameodb@{} ▶ ", host)
        }

        #[cfg(not(target_os = "windows"))]
        {
            format!(
                "{}{}{} ▶ ",
                "cameodb".bold().cyan(),
                "@".white(),
                host.bold().green()
            )
        }
    }

    fn display_host(&self) -> String {
        Url::parse(&self.current_url)
            .ok()
            .and_then(|u| u.host_str().map(|h| h.to_string()))
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "unknown".to_string())
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

            for index_info in &indexes.indexes {
                let config = client.get_index_config(&index_info.name).await?;
                let mut stats = serde_json::Map::new();
                stats.insert(
                    "document_count".to_string(),
                    json!(index_info.document_count),
                );
                if let Some(total_size) = index_info.total_size_bytes {
                    stats.insert("total_size_bytes".to_string(), json!(total_size));
                }
                if let Some(index_size) = index_info.index_size_mb {
                    stats.insert("index_size_mb".to_string(), json!(index_size));
                }
                if let Some(data_size) = index_info.data_size_mb {
                    stats.insert("data_size_mb".to_string(), json!(data_size));
                }
                stats.insert("shard_count".to_string(), json!(index_info.shard_count));
                let stats = serde_json::Value::Object(stats);
                let compact_fields = format_compact_fields(&config.fields);
                entries.push((index_info.name.clone(), stats.clone(), compact_fields));

                if extended {
                    let schema = json!({
                        "fields": config.fields,
                    });
                    enriched_indexes.push(json!({
                        "name": index_info.name,
                        "stats": stats,
                        "schema": schema,
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

            let config = client.get_index_config(&info.name).await?;

            if extended {
                let mut stats = serde_json::Map::new();
                stats.insert("document_count".to_string(), json!(info.document_count));
                if let Some(total_size) = info.total_size_bytes {
                    stats.insert("total_size_bytes".to_string(), json!(total_size));
                }
                if let Some(index_size) = info.index_size_mb {
                    stats.insert("index_size_mb".to_string(), json!(index_size));
                }
                if let Some(data_size) = info.data_size_mb {
                    stats.insert("data_size_mb".to_string(), json!(data_size));
                }
                stats.insert("shard_count".to_string(), json!(info.shard_count));
                let stats = serde_json::Value::Object(stats);
                let enriched = json!({
                    "index": info.name,
                    "stats": stats,
                    "schema": config,
                });
                print_json(&enriched)?;
            } else {
                let mut stats = serde_json::Map::new();
                stats.insert("document_count".to_string(), json!(info.document_count));
                if let Some(total_size) = info.total_size_bytes {
                    stats.insert("total_size_bytes".to_string(), json!(total_size));
                }
                if let Some(index_size) = info.index_size_mb {
                    stats.insert("index_size_mb".to_string(), json!(index_size));
                }
                if let Some(data_size) = info.data_size_mb {
                    stats.insert("data_size_mb".to_string(), json!(data_size));
                }
                stats.insert("shard_count".to_string(), json!(info.shard_count));
                let stats = serde_json::Value::Object(stats);
                let compact_fields = format_compact_fields(&config.fields);
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
///       "type": "Text"
///     }
///   }
///
/// Output:
///   "field_name": { "type": "Text" }
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

/// Format schema fields as compact one-line-per-field JSON objects.
///
/// Only non-default properties are shown:
/// - `indexed` is omitted when true (all fields are indexed by default)
/// - `stored` is omitted when false (most fields are not stored)
/// - `fast` is omitted when false
/// - `tokenizer` is omitted when absent or "default"
/// - `is_shadow` is omitted when false
/// - `index_record_option` is omitted when absent
fn format_compact_fields(fields_value: &JsonValue) -> JsonValue {
    let fields = match fields_value.as_object() {
        Some(f) => f,
        None => return fields_value.clone(),
    };

    let mut result = JsonMap::new();
    for (name, field_val) in fields {
        let mut compact = JsonMap::new();

        // Always show field_type
        if let Some(ft) = field_val.get("field_type") {
            compact.insert("type".to_string(), ft.clone());
        }

        // indexed: only show when false (default is true)
        if let Some(JsonValue::Bool(false)) = field_val.get("indexed") {
            compact.insert("indexed".to_string(), json!(false));
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

        // is_shadow: only show when true
        if let Some(JsonValue::Bool(true)) = field_val.get("is_shadow") {
            compact.insert("shadow".to_string(), json!(true));
        }

        result.insert(name.clone(), JsonValue::Object(compact));
    }
    JsonValue::Object(result)
}

#[derive(Clone, Debug)]
struct IndexCompleter {
    cache: Arc<RwLock<HashMap<String, IndexMetadata>>>,
}

impl IndexCompleter {
    fn new(cache: Arc<RwLock<HashMap<String, IndexMetadata>>>) -> Self {
        Self { cache }
    }

    fn friendly_type_label(&self, raw: &str) -> String {
        let normalized = raw.to_lowercase();
        match normalized.as_str() {
            "boolean" => "true/false".to_string(),
            "integer" | "i64" | "int" | "number" | "u64" => "numeric".to_string(),
            "float" | "f64" | "double" => "decimal".to_string(),
            "text" | "string" => "text".to_string(),
            "exact" => "exact".to_string(),
            _ => normalized,
        }
    }

    fn split_field_modifier<'a>(&self, token: &'a str) -> (&'a str, &'a str) {
        let mut split_idx = 0;
        for (idx, ch) in token.char_indices() {
            if matches!(ch, '+' | '-' | '!') {
                split_idx = idx + ch.len_utf8();
            } else {
                break;
            }
        }
        token.split_at(split_idx)
    }

    fn index_suggestions(&self, prefix: &str) -> Vec<Pair> {
        if let Ok(cache) = self.cache.read() {
            cache
                .keys()
                .filter(|name| name.starts_with(prefix))
                .map(|name| Pair {
                    display: name.clone(),
                    replacement: name.clone(),
                })
                .collect()
        } else {
            Vec::new()
        }
    }

    fn field_suggestions(&self, index: &str, prefix: &str) -> Vec<Pair> {
        if let Ok(cache) = self.cache.read()
            && let Some(metadata) = cache.get(index)
        {
            let (_, clean_prefix) = self.split_field_modifier(prefix);
            return metadata
                .fields
                .iter()
                .filter(|field| field.name.starts_with(clean_prefix))
                .map(|field| {
                    let label = self.friendly_type_label(&field.field_type);
                    let (modifier, _) = self.split_field_modifier(prefix);
                    Pair {
                        display: format!("{}: [{}]", field.name, label),
                        replacement: format!("{}{}:", modifier, field.name),
                    }
                })
                .collect();
        }
        Vec::new()
    }

    fn return_field_suggestions(&self, index: &str, prefix: &str) -> Vec<Pair> {
        if let Ok(cache) = self.cache.read()
            && let Some(metadata) = cache.get(index)
        {
            let clean_prefix = prefix.trim_end_matches(',');
            return metadata
                .fields
                .iter()
                .filter(|field| field.name.starts_with(clean_prefix))
                .map(|field| Pair {
                    display: field.name.clone(),
                    replacement: field.name.clone(),
                })
                .collect();
        }
        Vec::new()
    }

    fn sort_field_suggestions(&self, index: &str, prefix: &str) -> Vec<Pair> {
        if let Ok(cache) = self.cache.read()
            && let Some(metadata) = cache.get(index)
        {
            // Extract field name if prefix contains ':'
            let (field_prefix, has_colon) = if let Some(colon_pos) = prefix.find(':') {
                (&prefix[..colon_pos], true)
            } else {
                (prefix, false)
            };

            // Filter for sortable fields (u64, date types)
            // Note: We can't check FAST flag from client, so we suggest all u64/date fields
            let sortable_fields: Vec<_> = metadata
                .fields
                .iter()
                .filter(|field| {
                    // Check if field name matches prefix
                    field.name.starts_with(field_prefix)
                        // Check if field is potentially sortable (u64 or date)
                        && (field.field_type.to_lowercase().contains("u64")
                            || field.field_type.to_lowercase().contains("date"))
                })
                .collect();

            if has_colon {
                // User typed "field:", suggest :asc and :desc
                let field_name = field_prefix;
                vec![
                    Pair {
                        display: format!("{}:desc", field_name),
                        replacement: format!("{}:desc", field_name),
                    },
                    Pair {
                        display: format!("{}:asc", field_name),
                        replacement: format!("{}:asc", field_name),
                    },
                ]
            } else {
                // Suggest sortable field names with :desc suffix (default)
                sortable_fields
                    .iter()
                    .flat_map(|field| {
                        vec![
                            Pair {
                                display: format!("{}:desc (default)", field.name),
                                replacement: format!("{}:desc", field.name),
                            },
                            Pair {
                                display: format!("{}:asc", field.name),
                                replacement: format!("{}:asc", field.name),
                            },
                        ]
                    })
                    .collect()
            }
        } else {
            Vec::new()
        }
    }

    fn field_type_hint(&self, index: &str, field: &str) -> Option<String> {
        let (_, clean_field) = self.split_field_modifier(field);
        if let Ok(cache) = self.cache.read()
            && let Some(metadata) = cache.get(index)
            && let Some(info) = metadata.fields.iter().find(|f| f.name == clean_field)
        {
            let label = self.friendly_type_label(&info.field_type);
            return Some(format!("[{}]", label));
        }
        None
    }

    fn command_suggestions(&self, prefix: &str) -> Vec<Pair> {
        let commands = vec![
            "health", "list", "search", "schema", "data", "delete", "admin", "connect", "conn",
            "exit", "quit", "help",
        ];
        commands
            .into_iter()
            .filter(|cmd| cmd.starts_with(prefix))
            .map(|cmd| Pair {
                display: cmd.to_string(),
                replacement: cmd.to_string(),
            })
            .collect()
    }

    fn expand_dir_part(&self, dir_part: &str) -> PathBuf {
        if let Some(stripped) = dir_part.strip_prefix("~/")
            && let Some(home) = dirs::home_dir()
        {
            return home.join(stripped);
        }

        if dir_part.is_empty() {
            PathBuf::from(".")
        } else {
            PathBuf::from(dir_part)
        }
    }

    fn file_path_suggestions(&self, prefix: &str) -> Vec<Pair> {
        // Split prefix into directory part (with trailing slash) and file prefix
        let (dir_part, file_prefix) = if prefix.ends_with('/') {
            (prefix.to_string(), "".to_string())
        } else if let Some(pos) = prefix.rfind('/') {
            (prefix[..=pos].to_string(), prefix[pos + 1..].to_string())
        } else {
            ("".to_string(), prefix.to_string())
        };

        let fs_dir = self.expand_dir_part(&dir_part);
        let mut pairs = Vec::new();

        if let Ok(entries) = fs::read_dir(&fs_dir) {
            for entry in entries.flatten() {
                let file_name = entry.file_name();
                let name = file_name.to_string_lossy();
                if !name.starts_with(&file_prefix) {
                    continue;
                }

                let mut replacement = format!("{}{}", dir_part, name);
                let mut display = name.to_string();
                if let Ok(md) = entry.metadata()
                    && md.is_dir()
                {
                    replacement.push('/');
                    display.push('/');
                }

                pairs.push(Pair {
                    display,
                    replacement,
                });
            }
        }

        pairs
    }

    fn delimiter_flag_suggestions(&self, prefix: &str) -> Vec<Pair> {
        let options = ["detect", "comma", "tab", "semicolon"];
        options
            .iter()
            .filter(|opt| format!("--delimiter {}", opt).starts_with(prefix))
            .map(|opt| Pair {
                display: format!("--delimiter {}", opt),
                replacement: format!("--delimiter {}", opt),
            })
            .collect()
    }

    fn batch_size_flag_suggestions(&self, prefix: &str) -> Vec<Pair> {
        let flag = "--batch-size ";
        if flag.starts_with(prefix) || prefix.starts_with(flag) {
            vec![Pair {
                display: "--batch-size <n>".to_string(),
                replacement: flag.to_string(),
            }]
        } else {
            Vec::new()
        }
    }

    fn delete_flag_suggestions(&self, prefix: &str) -> Vec<Pair> {
        if "--delete-schema".starts_with(prefix) {
            vec![Pair {
                display: "--delete-schema".to_string(),
                replacement: "--delete-schema".to_string(),
            }]
        } else {
            Vec::new()
        }
    }

    fn admin_subcommand_suggestions(&self, prefix: &str) -> Vec<Pair> {
        let subcommands = vec!["memory", "index"];
        subcommands
            .into_iter()
            .filter(|sub| sub.starts_with(prefix))
            .map(|sub| Pair {
                display: sub.to_string(),
                replacement: sub.to_string(),
            })
            .collect()
    }

    fn admin_memory_operation_suggestions(&self, prefix: &str) -> Vec<Pair> {
        let operations = vec!["stats", "trim"];
        operations
            .into_iter()
            .filter(|op| op.starts_with(prefix))
            .map(|op| Pair {
                display: op.to_string(),
                replacement: op.to_string(),
            })
            .collect()
    }

    fn admin_index_operation_suggestions(&self, prefix: &str) -> Vec<Pair> {
        let operations = vec!["commit", "evict-writer"];
        operations
            .into_iter()
            .filter(|op| op.starts_with(prefix))
            .map(|op| Pair {
                display: op.to_string(),
                replacement: op.to_string(),
            })
            .collect()
    }

    fn list_subcommand_suggestions(&self, prefix: &str) -> Vec<Pair> {
        let subcommands = vec!["indexes", "index"];
        subcommands
            .into_iter()
            .filter(|sub| sub.starts_with(prefix))
            .map(|sub| Pair {
                display: sub.to_string(),
                replacement: sub.to_string(),
            })
            .collect()
    }

    fn extended_flag_suggestions(&self, current: &str, tokens: &[&str]) -> Vec<Pair> {
        let has_extended = tokens.iter().any(|t| *t == "--extended" || *t == "-e");
        let has_data_size = tokens.contains(&"--data-size");
        let mut suggestions = Vec::new();

        if !has_extended && "--extended".starts_with(current) {
            suggestions.push(Pair {
                display: "--extended".to_string(),
                replacement: "--extended".to_string(),
            });
        }
        if !has_extended && "-e".starts_with(current) {
            suggestions.push(Pair {
                display: "-e".to_string(),
                replacement: "-e".to_string(),
            });
        }
        if !has_data_size && "--data-size".starts_with(current) {
            suggestions.push(Pair {
                display: "--data-size".to_string(),
                replacement: "--data-size".to_string(),
            });
        }
        suggestions
    }

    fn schema_subcommand_suggestions(&self, prefix: &str) -> Vec<Pair> {
        let subcommands = vec!["detect", "load"];
        subcommands
            .into_iter()
            .filter(|sub| sub.starts_with(prefix))
            .map(|sub| Pair {
                display: sub.to_string(),
                replacement: sub.to_string(),
            })
            .collect()
    }

    fn data_subcommand_suggestions(&self, prefix: &str) -> Vec<Pair> {
        let subcommands = vec!["load"];
        subcommands
            .into_iter()
            .filter(|sub| sub.starts_with(prefix))
            .map(|sub| Pair {
                display: sub.to_string(),
                replacement: sub.to_string(),
            })
            .collect()
    }

    fn complete_tokens(&self, tokens: &[&str], current: &str) -> Option<(usize, Vec<Pair>)> {
        if tokens.is_empty() {
            return None;
        }

        match tokens[0] {
            // Complete main commands when first token or partial command
            _cmd if tokens.len() == 1 => {
                let suggestions = self.command_suggestions(current);
                let start = 0;
                Some((start, suggestions))
            }
            // Complete subcommands for 'list'
            "list" if tokens.len() == 2 => {
                let suggestions = self.list_subcommand_suggestions(current);
                let start = current_start(tokens, current);
                Some((start, suggestions))
            }
            "list" if tokens.len() >= 2 && tokens[1] == "indexes" => {
                if current.starts_with('-') {
                    // Suggest --extended flag after indexes
                    let suggestions = self.extended_flag_suggestions(current, tokens);
                    let start = current_start(tokens, current);
                    Some((start, suggestions))
                } else {
                    None
                }
            }
            "list" if tokens.len() >= 2 && tokens[1] == "index" => {
                if current.starts_with('-') {
                    // Suggest --extended flag after index name
                    let suggestions = self.extended_flag_suggestions(current, tokens);
                    let start = current_start(tokens, current);
                    Some((start, suggestions))
                } else {
                    let suggestions = self.index_suggestions(current);
                    let start = current_start(tokens, current);
                    Some((start, suggestions))
                }
            }
            // Complete schema subcommands and index for schema load
            "schema" if tokens.len() == 2 => {
                let suggestions = self.schema_subcommand_suggestions(current);
                let start = current_start(tokens, current);
                Some((start, suggestions))
            }
            "schema" if tokens.len() == 3 && tokens[1] == "load" => {
                let suggestions = self.index_suggestions(current);
                let start = current_start(tokens, current);
                Some((start, suggestions))
            }
            "schema"
                if (tokens.len() == 3 && tokens[1] == "detect")
                    || (tokens.len() == 4 && tokens[1] == "load") =>
            {
                let suggestions = self.file_path_suggestions(current);
                let start = current_start(tokens, current);
                Some((start, suggestions))
            }
            "schema" if current.starts_with('-') => {
                let suggestions = self.delimiter_flag_suggestions(current);
                let start = current_start(tokens, current);
                Some((start, suggestions))
            }
            // Complete index name for 'search'
            "search" if tokens.len() == 2 => {
                let suggestions = self.index_suggestions(current);
                let start = current_start(tokens, current);
                Some((start, suggestions))
            }
            // Complete field names in search query
            "search" if tokens.len() >= 3 => {
                let index = tokens[1];

                // Check if we're after a 'return' keyword to suggest fields
                let query_tokens = &tokens[2..];
                let after_return = query_tokens
                    .iter()
                    .rposition(|t| t.eq_ignore_ascii_case("return"));

                if let Some(return_pos) = after_return {
                    // We're after 'return', suggest fields (comma-separated)
                    let after_return_tokens = &query_tokens[return_pos + 1..];

                    // Check if current token is after 'limit' or 'sort' keyword
                    let after_limit = after_return_tokens
                        .iter()
                        .any(|t| t.eq_ignore_ascii_case("limit"));
                    let after_sort = after_return_tokens
                        .iter()
                        .any(|t| t.eq_ignore_ascii_case("sort"));

                    if !after_limit && !after_sort {
                        let suggestions = self.return_field_suggestions(index, current);
                        let start = current_start(tokens, current);
                        return Some((start, suggestions));
                    }
                }

                // Check if we're after a 'sort' keyword to suggest sortable fields
                let after_sort = query_tokens
                    .iter()
                    .rposition(|t| t.eq_ignore_ascii_case("sort"));

                if let Some(sort_pos) = after_sort {
                    // We're after 'sort', suggest sortable fields with :asc/:desc suffix
                    let after_sort_tokens = &query_tokens[sort_pos + 1..];

                    // Check if current token is after 'return' or 'limit' keyword
                    let after_return_kw = after_sort_tokens
                        .iter()
                        .any(|t| t.eq_ignore_ascii_case("return"));
                    let after_limit_kw = after_sort_tokens
                        .iter()
                        .any(|t| t.eq_ignore_ascii_case("limit"));

                    if !after_return_kw && !after_limit_kw {
                        let suggestions = self.sort_field_suggestions(index, current);
                        let start = current_start(tokens, current);
                        return Some((start, suggestions));
                    }
                }

                // Check if we should suggest 'return', 'limit', or 'sort' keywords
                if current.is_empty()
                    || "return".starts_with(current)
                    || "limit".starts_with(current)
                    || "sort".starts_with(current)
                {
                    let mut suggestions = Vec::new();

                    // Only suggest 'return' if not already in query
                    if !query_tokens
                        .iter()
                        .any(|t| t.eq_ignore_ascii_case("return"))
                        && "return".starts_with(current)
                    {
                        suggestions.push(Pair {
                            display: "return <fields>".to_string(),
                            replacement: "return ".to_string(),
                        });
                    }

                    // Only suggest 'limit' if not already in query
                    if !query_tokens.iter().any(|t| t.eq_ignore_ascii_case("limit"))
                        && "limit".starts_with(current)
                    {
                        suggestions.push(Pair {
                            display: "limit <n>".to_string(),
                            replacement: "limit ".to_string(),
                        });
                    }

                    // Only suggest 'sort' if not already in query
                    if !query_tokens.iter().any(|t| t.eq_ignore_ascii_case("sort"))
                        && "sort".starts_with(current)
                    {
                        suggestions.push(Pair {
                            display: "sort <field:order>".to_string(),
                            replacement: "sort ".to_string(),
                        });
                    }

                    if !suggestions.is_empty() {
                        let start = current_start(tokens, current);
                        return Some((start, suggestions));
                    }
                }

                // Default: suggest field names for query construction
                let suggestions = self.field_suggestions(index, current);
                let start = current_start(tokens, current);
                Some((start, suggestions))
            }
            // Complete data subcommands and index for data load
            "data" if tokens.len() == 2 => {
                let suggestions = self.data_subcommand_suggestions(current);
                let start = current_start(tokens, current);
                Some((start, suggestions))
            }
            "data" if tokens.len() == 3 && tokens[1] == "load" => {
                let suggestions = self.index_suggestions(current);
                let start = current_start(tokens, current);
                Some((start, suggestions))
            }
            "data" if tokens.len() == 4 && tokens[1] == "load" => {
                let suggestions = self.file_path_suggestions(current);
                let start = current_start(tokens, current);
                Some((start, suggestions))
            }
            "data" if current.starts_with("--delimiter") => {
                let suggestions = self.delimiter_flag_suggestions(current);
                let start = current_start(tokens, current);
                Some((start, suggestions))
            }
            "data" if current.starts_with("--batch-size") || current == "--batch-size" => {
                let suggestions = self.batch_size_flag_suggestions(current);
                let start = current_start(tokens, current);
                Some((start, suggestions))
            }
            // Complete index name for delete
            "delete" if tokens.len() == 2 => {
                let suggestions = self.index_suggestions(current);
                let start = current_start(tokens, current);
                Some((start, suggestions))
            }
            "delete" if current.starts_with('-') => {
                let suggestions = self.delete_flag_suggestions(current);
                let start = current_start(tokens, current);
                Some((start, suggestions))
            }
            // Admin subcommands
            "admin" if tokens.len() == 2 => {
                let suggestions = self.admin_subcommand_suggestions(current);
                let start = current_start(tokens, current);
                Some((start, suggestions))
            }
            "admin" if tokens.len() >= 2 && tokens[1] == "memory" && tokens.len() == 3 => {
                let suggestions = self.admin_memory_operation_suggestions(current);
                let start = current_start(tokens, current);
                Some((start, suggestions))
            }
            "admin" if tokens.len() >= 2 && tokens[1] == "index" && tokens.len() == 3 => {
                let suggestions = self.index_suggestions(current);
                let start = current_start(tokens, current);
                Some((start, suggestions))
            }
            "admin" if tokens.len() >= 3 && tokens[1] == "index" && tokens.len() == 4 => {
                let suggestions = self.admin_index_operation_suggestions(current);
                let start = current_start(tokens, current);
                Some((start, suggestions))
            }
            _ => None,
        }
    }
}

impl Helper for IndexCompleter {}

impl Validator for IndexCompleter {
    fn validate(&self, _ctx: &mut ValidationContext) -> rustyline::Result<ValidationResult> {
        Ok(ValidationResult::Valid(None))
    }
}

impl Highlighter for IndexCompleter {}

impl Hinter for IndexCompleter {
    type Hint = String;

    fn hint(&self, line: &str, pos: usize, _ctx: &rustyline::Context<'_>) -> Option<String> {
        let prefix = &line[..pos];
        let mut parts = prefix.split_whitespace();
        let command = parts.next()?;

        match command {
            "list" => {
                let subcommand = parts.next()?;
                let tail = parts.collect::<Vec<_>>();
                let has_extended = tail.iter().any(|t| *t == "--extended" || *t == "-e");

                if (subcommand == "index" || subcommand == "indexes")
                    && let Some(current) = tail.last()
                {
                    if !has_extended && "--extended".starts_with(current) {
                        return Some("extended".strip_prefix(current).unwrap_or("").to_string());
                    }
                    if !has_extended && "-e".starts_with(current) {
                        return Some("e".strip_prefix(current).unwrap_or("").to_string());
                    }
                }
                None
            }
            "search" => {
                let index = parts.next()?;
                let tail = parts.collect::<Vec<_>>();

                // Check if user is typing a field value
                if let Some(current) = tail.last() {
                    if let Some((field, value_prefix)) = current.split_once(':') {
                        let trimmed_value = value_prefix
                            .trim_start_matches(&['>', '<', '=', '!'][..])
                            .trim();
                        if trimmed_value.is_empty() {
                            return self
                                .field_type_hint(index, field)
                                .map(|hint| format!(" {}", hint));
                        }
                    }

                    // Provide hints for 'return' and 'limit' keywords
                    let has_return = tail.iter().any(|t| t.eq_ignore_ascii_case("return"));
                    let has_limit = tail.iter().any(|t| t.eq_ignore_ascii_case("limit"));

                    // If typing 'r', hint 'return'
                    if current.eq_ignore_ascii_case("r") && !has_return {
                        return Some("eturn <fields>".to_string());
                    }

                    // If typing 'l', hint 'limit'
                    if current.eq_ignore_ascii_case("l") && !has_limit {
                        return Some("imit <n>".to_string());
                    }

                    // If typing 'ret', hint 'return'
                    if "return".starts_with(&current.to_lowercase())
                        && current.len() < "return".len()
                        && !has_return
                    {
                        return Some(format!("{} <fields>", &"return"[current.len()..]));
                    }

                    // If typing 'lim', hint 'limit'
                    if "limit".starts_with(&current.to_lowercase())
                        && current.len() < "limit".len()
                        && !has_limit
                    {
                        return Some(format!("{} <n>", &"limit"[current.len()..]));
                    }
                }

                None
            }
            "admin" => {
                let subcommand = parts.next()?;
                match subcommand {
                    "memory" => {
                        let op = parts.next();
                        if op.is_none() {
                            return Some(" <stats|trim>".to_string());
                        }
                        None
                    }
                    "index" => {
                        let index = parts.next();
                        if index.is_none() {
                            return Some(" <index>".to_string());
                        }
                        let op = parts.next();
                        if op.is_none() {
                            return Some(" <commit|evict-writer>".to_string());
                        }
                        None
                    }
                    _ => None,
                }
            }
            _ => None,
        }
    }
}

impl Completer for IndexCompleter {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &rustyline::Context<'_>,
    ) -> rustyline::Result<(usize, Vec<Pair>)> {
        let prefix = &line[..pos];
        let mut parts: Vec<&str> = prefix.split_whitespace().collect();

        let current = if prefix.ends_with(' ') {
            parts.push("");
            ""
        } else {
            parts.last().copied().unwrap_or("")
        };

        // Handle empty line case
        if parts.is_empty() || (parts.len() == 1 && parts[0].is_empty()) {
            let suggestions = self.command_suggestions("");
            return Ok((0, suggestions));
        }

        if let Some((_, suggestions)) = self.complete_tokens(&parts, current) {
            // Use the actual cursor position minus current token length for safe replacement
            let safe_start = pos.saturating_sub(current.len());
            Ok((safe_start, suggestions))
        } else {
            Ok((pos, Vec::new()))
        }
    }
}

fn current_start(tokens: &[&str], current: &str) -> usize {
    if current.is_empty() {
        // Position is at end of line after space
        tokens.iter().map(|t| t.len() + 1).sum::<usize>()
    } else {
        // Position is within the current token
        let before_current = tokens.len().saturating_sub(1);
        tokens
            .iter()
            .take(before_current)
            .map(|t| t.len() + 1)
            .sum::<usize>()
    }
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
    },

    /// Schema utilities
    Schema {
        /// Operation to perform
        #[arg(value_enum)]
        operation: SchemaOperation,
        /// Path or HTTP(S) URL to schema/data source
        file: String,
        /// Target index name (required for `load`, optional for `detect`)
        #[arg(long, short = 'n')]
        index: Option<String>,
        /// Delimiter override (default: auto-detect first line)
        #[arg(long, value_enum, default_value_t = Delimiter::Detect)]
        delimiter: Delimiter,
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
    },

    /// Delete an index (and optionally its schema)
    Delete {
        /// Target index name
        index: String,
        /// Also delete stored schema/config
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
    },
    /// Index admin operations
    Index {
        /// Target index name
        index: String,
        /// Operation to perform
        #[arg(value_enum)]
        operation: IndexAdminOperation,
    },
}

#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum MemoryOperation {
    /// Show memory statistics (process + jemalloc)
    Stats,
    /// Trigger jemalloc memory purge
    Trim,
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

    if cli.interactive {
        return run_interactive_shell(normalized_connect).await;
    }

    let client = CameoClient::new(&normalized_connect)?;

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
        } => {
            let (clean_query, parsed_limit, parsed_fields) = parse_query_modifiers(&query);
            if clean_query.is_empty() {
                anyhow::bail!("Query cannot be empty after removing modifiers");
            }
            let final_limit = limit.or(parsed_limit);
            let results = client
                .search(&index, &clean_query, final_limit, parsed_fields)
                .await?;
            print_json(&results)?;
        }
        ClientCommand::Schema {
            operation,
            index,
            file,
            delimiter,
        } => match operation {
            SchemaOperation::Detect => {
                let schema_json = detect_schema_from_source(&client, &file, delimiter).await?;
                print_json(&schema_json)?;
            }
            SchemaOperation::Load => {
                let index_name = index
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| anyhow!("Index name is required for schema load. Usage: schema load <index> <file>"))?;

                let schema_json = load_schema_from_source(&client, &file, delimiter).await?;
                client.put_index_config(index_name, &schema_json).await?;
                println!("Schema applied to index '{}'", index_name);
            }
        },
        ClientCommand::Data {
            operation,
            index,
            file,
            delimiter,
            batch_size,
        } => match operation {
            DataOperation::Load => {
                load_data_from_source(&client, &index, &file, delimiter, batch_size).await?;
            }
        },
        ClientCommand::Delete {
            index,
            delete_schema,
        } => {
            let result = client.delete_index(&index, delete_schema).await?;
            print_json(&result)?;
        }
        ClientCommand::Admin { subcommand } => match subcommand {
            AdminCommand::Memory { operation } => match operation {
                MemoryOperation::Stats => {
                    let result = client.admin_memory_stats().await?;
                    print_json(&result)?;
                }
                MemoryOperation::Trim => {
                    let result = client.admin_memory_trim().await?;
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
        },
    }

    Ok(())
}

fn parse_delimiter_arg<'a>(args: &'a [&'a str]) -> Result<(Delimiter, Vec<&'a str>)> {
    let mut delimiter = Delimiter::Detect;
    let mut remaining = Vec::new();
    let mut iter = args.iter().peekable();

    while let Some(&arg) = iter.next() {
        if arg == "--delimiter" {
            let value = iter
                .next()
                .copied()
                .ok_or_else(|| anyhow!("Missing value for --delimiter"))?;
            delimiter = match value {
                "detect" => Delimiter::Detect,
                "comma" => Delimiter::Comma,
                "tab" => Delimiter::Tab,
                "semicolon" => Delimiter::Semicolon,
                other => {
                    return Err(anyhow!(
                        "Invalid delimiter '{}'. Use detect|comma|tab|semicolon.",
                        other
                    ));
                }
            };
        } else {
            remaining.push(arg);
        }
    }

    Ok((delimiter, remaining))
}

fn parse_batch_size_arg<'a>(args: &'a [&'a str], default: usize) -> Result<(usize, Vec<&'a str>)> {
    let mut batch_size = default;
    let mut remaining = Vec::new();
    let mut iter = args.iter().peekable();

    while let Some(&arg) = iter.next() {
        if arg == "--batch-size" {
            let value = iter
                .next()
                .copied()
                .ok_or_else(|| anyhow!("Missing value for --batch-size"))?;
            batch_size = value
                .parse::<usize>()
                .map_err(|_| anyhow!("Invalid batch size '{}': expected number", value))?;
        } else {
            remaining.push(arg);
        }
    }

    Ok((batch_size, remaining))
}

/// Result of ID field detection
#[derive(Debug)]
struct IdFieldDetection {
    index: usize,
    original_field_name: String,
    is_shadow: bool, // true if original field != "id"
}

/// Detect the ID field from CSV headers with shadow field support.
///
/// Priority order:
/// 1. Exact match: field named "id" (case-insensitive)
/// 2. Hash algorithms: "sha256", "sha1", "md5" (in order of complexity)
/// 3. Substring match: any field containing "id" (case-insensitive)
/// 4. Fallback: first column (index 0)
fn detect_id_field(headers: &[(String, Option<TantivyFieldType>)]) -> IdFieldDetection {
    // Hash algorithms ordered by complexity (most complex first for better distribution)
    let hash_algorithms = ["sha256", "sha1", "md5"];

    // Try exact match first
    if let Some(pos) = headers.iter().position(|(h, _)| h.to_lowercase() == "id") {
        return IdFieldDetection {
            index: pos,
            original_field_name: "id".to_string(),
            is_shadow: false, // No shadow needed for canonical "id" field
        };
    }

    // Look for hash algorithm fields in priority order
    for hash_name in &hash_algorithms {
        if let Some(pos) = headers
            .iter()
            .position(|(h, _)| h.to_lowercase() == *hash_name)
        {
            return IdFieldDetection {
                index: pos,
                original_field_name: hash_name.to_string(),
                is_shadow: true, // Create shadow field for hash algorithm fields
            };
        }
    }

    // Prefer suffix-based matches (common for camelCase/PascalCase like videoId, userID)
    if let Some(pos) = headers.iter().position(|(h, _)| {
        let lower = h.to_lowercase();
        lower.ends_with("id") || lower.ends_with("_id")
    }) {
        let field_name = &headers[pos].0;
        return IdFieldDetection {
            index: pos,
            original_field_name: field_name.clone(),
            is_shadow: field_name.to_lowercase() != "id",
        };
    }

    // Look for substring match
    if let Some(pos) = headers
        .iter()
        .position(|(h, _)| h.to_lowercase().contains("id"))
    {
        let field_name = &headers[pos].0;
        return IdFieldDetection {
            index: pos,
            original_field_name: field_name.clone(),
            is_shadow: field_name.to_lowercase() != "id", // Shadow if not canonical "id"
        };
    }

    // Fallback: first column
    let field_name = &headers[0].0;
    IdFieldDetection {
        index: 0,
        original_field_name: field_name.clone(),
        is_shadow: field_name.to_lowercase() != "id", // Shadow if not canonical "id"
    }
}

async fn detect_schema_from_csv(
    client: &CameoClient,
    source: &str,
    delimiter: Delimiter,
) -> Result<JsonValue> {
    let mut reader = open_csv_reader(client, source, delimiter).await?;
    let raw_headers = reader
        .headers()
        .context("CSV file is missing headers")?
        .clone();

    let headers: Vec<(String, Option<TantivyFieldType>)> =
        raw_headers.iter().map(parse_header_with_hint).collect();

    let id_detection = detect_id_field(&headers);

    let mut schema = IndexSchema::default();
    let mut sampled = 0usize;

    // Add a single shadow field for the detected id source when its name is not "id"
    if id_detection.is_shadow {
        let field_type = headers[id_detection.index]
            .1
            .clone()
            .unwrap_or(TantivyFieldType::Text);
        schema.add_shadow_field(id_detection.original_field_name.clone(), field_type);
    }

    // Collect id-like candidates (excluding primary) ordered by priority for equality promotion
    // Tuple: (priority, idx, name, hint, all_match, seen_any)
    let mut id_like_candidates: Vec<(u8, usize, String, Option<TantivyFieldType>, bool, bool)> =
        headers
            .iter()
            .enumerate()
            .filter_map(|(idx, (name, hint))| {
                if idx == id_detection.index {
                    return None;
                }

                let lower = name.to_lowercase();
                let (priority, looks_like_id) =
                    if ["sha256", "sha1", "md5"].contains(&lower.as_str()) {
                        (0u8, true)
                    } else if lower.ends_with("_id") || lower.ends_with("id") {
                        (1u8, true)
                    } else if lower.contains("id") {
                        (2u8, true)
                    } else {
                        (u8::MAX, false)
                    };

                if looks_like_id {
                    Some((priority, idx, name.clone(), hint.clone(), true, false))
                } else {
                    None
                }
            })
            .collect();
    id_like_candidates.sort_by_key(|(priority, _, _, _, _, _)| *priority);

    for record in reader.records() {
        let record = record.context("Failed to read CSV record")?;
        let mut obj: JsonMap<String, JsonValue> = JsonMap::new();

        let canonical_id_raw = record.get(id_detection.index).unwrap_or("");

        // Update equality flags for candidates
        for (_, idx, _, _, all_match, seen_any) in id_like_candidates.iter_mut() {
            if let Some(val) = record.get(*idx) {
                *seen_any = true;
                if *all_match && val.trim() != canonical_id_raw.trim() {
                    *all_match = false;
                }
            }
        }

        // Process all fields in CSV column order
        for (idx, value) in record.iter().enumerate() {
            if let Some((header, _)) = headers.get(idx) {
                let parsed = parse_csv_cell(value);
                obj.insert(header.clone(), parsed);
            }
        }

        // Inject canonical "id" field from the detected source field
        if let Some(raw_id) = record.get(id_detection.index) {
            let id_val = raw_id.trim();
            if !id_val.is_empty() {
                obj.insert("id".to_string(), JsonValue::String(id_val.to_string()));
            }
        }

        schema.evolve_from_document(&JsonValue::Object(obj));

        sampled += 1;
        if sampled >= SCHEMA_SAMPLE_LIMIT {
            break;
        }
    }

    // If canonical name was "id", promote the first candidate whose values always matched
    if !id_detection.is_shadow
        && let Some((_, _, name, hint, _all_match, _seen_any)) = id_like_candidates
            .into_iter()
            .find(|(_, _, _, _, all_match, seen_any)| *seen_any && *all_match)
    {
        let field_type = hint.unwrap_or(TantivyFieldType::Text);
        schema.add_shadow_field(name, field_type);
    }

    // CRITICAL: Mark all fields as indexed when loading schema from CSV/TSV file
    // This is different from dynamic evolution during writes, where fields start as non-indexed
    // When explicitly creating a schema from CSV/TSV, all fields should be indexed by default
    // IMPORTANT: Only 'id' field is stored in Tantivy; all other data comes from redb
    // CRITICAL: Preserve shadow field status - shadow fields should remain non-indexed and non-stored
    for (name, field_def) in schema.fields.iter_mut() {
        // Don't modify shadow fields - they have special requirements
        if !field_def.is_shadow {
            field_def.indexed = true;
            // Only 'id' field should be stored in Tantivy (architecture rule)
            field_def.stored = name == "id";
        }
    }

    // Apply type hints where provided
    for (name, hint) in &headers {
        if let Some(t) = hint.clone() {
            // Don't overwrite shadow fields - preserve their special status
            if !schema.fields.get(name).is_some_and(|f| f.is_shadow) {
                // FieldDef::new already sets correct stored/fast flags per architecture
                let mut field_def = FieldDef::new(name.clone(), t);
                field_def.indexed = true;
                // stored flag already set correctly by FieldDef::new (only 'id' = true)
                schema.fields.insert(name.clone(), field_def);
            }
        }
    }

    // Ensure 'id' field is explicitly defined in schema with proper settings
    if !schema.fields.contains_key("id") {
        let id_field = FieldDef {
            name: "id".to_string(),
            field_type: TantivyFieldType::Text,
            indexed: true,
            stored: true,
            fast: false,
            is_shadow: false, // The canonical 'id' field is not a shadow field
            tokenizer: Some("raw".to_string()),
            index_record_option: Some("Basic".to_string()),
        };
        schema.fields.insert("id".to_string(), id_field);
    }

    // Set routing field and fingerprint from the fully-built schema
    schema.auto_detect_routing_field();
    schema.fingerprint = schema.calculate_fingerprint();

    let mut schema_json = serde_json::to_value(schema).context("Failed to serialize schema")?;

    // Reorder fields map: id first, then preserve CSV column order
    if let JsonValue::Object(ref mut root) = schema_json
        && let Some(JsonValue::Object(mut fields)) = root.remove("fields")
    {
        let mut ordered = JsonMap::new();

        // Always place 'id' first
        if let Some(id_val) = fields.remove("id") {
            ordered.insert("id".to_string(), id_val);
        }

        // Then add fields in CSV column order (preserving source structure)
        for (header_name, _) in &headers {
            if header_name != "id"
                && let Some(field_val) = fields.remove(header_name)
            {
                ordered.insert(header_name.clone(), field_val);
            }
        }

        // Add any remaining fields that weren't in headers (shouldn't happen, but safe)
        for (k, v) in fields {
            ordered.insert(k, v);
        }

        root.insert("fields".to_string(), JsonValue::Object(ordered));
    }

    Ok(schema_json)
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SourceFormat {
    SchemaJson,
    JsonDocument,
    JsonArray,
    JsonLines,
    CsvLike,
}

#[derive(Debug)]
struct JsonSourceAnalysis {
    sample_docs: Vec<JsonValue>,
    id_field: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Compression {
    None,
    Gzip,
    Zip,
}

fn detect_compression(source: &str) -> Compression {
    let path_lower = if is_http_source(source) {
        Url::parse(source)
            .ok()
            .map(|url| url.path().to_lowercase())
            .unwrap_or_default()
    } else {
        source.to_lowercase()
    };

    if path_lower.ends_with(".gz") || path_lower.ends_with(".gzip") {
        Compression::Gzip
    } else if path_lower.ends_with(".zip") {
        Compression::Zip
    } else {
        Compression::None
    }
}

fn zip_first_entry_bytes(data: &[u8]) -> Result<(Vec<u8>, Option<String>)> {
    let cursor = Cursor::new(data);
    let mut archive = zip::ZipArchive::new(cursor).context("Failed to open ZIP archive")?;

    for i in 0..archive.len() {
        let mut entry = archive
            .by_index(i)
            .with_context(|| format!("Failed to read ZIP entry {}", i))?;
        if entry.is_dir() {
            continue;
        }
        let entry_name = entry.name().to_string();
        let mut buf = Vec::new();
        entry
            .read_to_end(&mut buf)
            .with_context(|| format!("Failed to decompress ZIP entry '{}'", entry_name))?;
        return Ok((buf, Some(entry_name)));
    }

    Err(anyhow!("ZIP archive does not contain any files"))
}

fn decompress_bytes(bytes: Vec<u8>, compression: Compression) -> Result<Vec<u8>> {
    match compression {
        Compression::None => Ok(bytes),
        Compression::Gzip => {
            let mut decoder = GzDecoder::new(Cursor::new(bytes));
            let mut decompressed = Vec::new();
            decoder
                .read_to_end(&mut decompressed)
                .context("Failed to decompress gzip data")?;
            Ok(decompressed)
        }
        Compression::Zip => {
            let (data, _name) = zip_first_entry_bytes(&bytes)?;
            Ok(data)
        }
    }
}

fn open_local_reader(path: &Path, compression: Compression) -> Result<Box<dyn Read + Send>> {
    let file = fs::File::open(path)
        .with_context(|| format!("Failed to open source file: {}", path.display()))?;
    match compression {
        Compression::None => Ok(Box::new(file)),
        Compression::Gzip => Ok(Box::new(GzDecoder::new(file))),
        Compression::Zip => {
            let mut buf = Vec::new();
            BufReader::new(file)
                .read_to_end(&mut buf)
                .with_context(|| format!("Failed to read ZIP file: {}", path.display()))?;
            let (data, _name) = zip_first_entry_bytes(&buf)?;
            Ok(Box::new(Cursor::new(data)))
        }
    }
}

fn is_http_source(source: &str) -> bool {
    source.starts_with("http://") || source.starts_with("https://")
}

fn read_local_prefix_bytes(path: &Path, max_bytes: usize) -> Result<Vec<u8>> {
    let compression = detect_compression(path.to_str().unwrap_or(""));
    let reader = open_local_reader(path, compression)?;
    let mut reader = BufReader::new(reader);
    let mut buffer = vec![0u8; max_bytes];
    let bytes_read = reader
        .read(&mut buffer)
        .with_context(|| format!("Failed to read source file: {}", path.display()))?;
    buffer.truncate(bytes_read);
    Ok(buffer)
}

fn source_extension(source: &str) -> Option<String> {
    let compression = detect_compression(source);

    let extract_ext = |path: &Path| -> Option<String> {
        if compression != Compression::None {
            // Strip compression extension to get inner format extension
            // e.g. "data.csv.gz" -> stem "data.csv" -> extension "csv"
            let stem = path.file_stem()?.to_str()?;
            Path::new(stem)
                .extension()
                .and_then(|ext| ext.to_str())
                .map(|ext| ext.to_ascii_lowercase())
        } else {
            path.extension()
                .and_then(|ext| ext.to_str())
                .map(|ext| ext.to_ascii_lowercase())
        }
    };

    if is_http_source(source) {
        Url::parse(source)
            .ok()
            .and_then(|url| extract_ext(Path::new(url.path())))
    } else {
        extract_ext(Path::new(source))
    }
}

fn detect_source_format_from_hint(_extension: Option<&str>, bytes: &[u8]) -> Result<SourceFormat> {
    if bytes.iter().all(u8::is_ascii_whitespace) {
        return Err(anyhow!("Source is empty"));
    }

    let first_non_whitespace = bytes
        .iter()
        .copied()
        .find(|byte| !byte.is_ascii_whitespace())
        .ok_or_else(|| anyhow!("Source is empty"))?;

    // Content-based detection takes precedence over extension
    match first_non_whitespace {
        b'[' => Ok(SourceFormat::JsonArray),
        b'{' => {
            if let Ok(text) = std::str::from_utf8(bytes) {
                let mut non_empty_lines =
                    text.lines().map(str::trim).filter(|line| !line.is_empty());
                if let (Some(first), Some(second)) =
                    (non_empty_lines.next(), non_empty_lines.next())
                    && let Ok(first_json) = serde_json::from_str::<JsonValue>(first)
                    && let Ok(second_json) = serde_json::from_str::<JsonValue>(second)
                    && first_json.is_object()
                    && second_json.is_object()
                {
                    return Ok(SourceFormat::JsonLines);
                }
            }

            if let Ok(json) = serde_json::from_slice::<JsonValue>(bytes)
                && let JsonValue::Object(obj) = json
                && obj.contains_key("fields")
            {
                return Ok(SourceFormat::SchemaJson);
            }

            Ok(SourceFormat::JsonDocument)
        }
        _ => Ok(SourceFormat::CsvLike),
    }
}

fn detect_source_format_from_prefix(path: &Path, bytes: &[u8]) -> Result<SourceFormat> {
    let extension = path.extension().and_then(|ext| ext.to_str());
    detect_source_format_from_hint(extension, bytes)
}

fn detect_local_source_format(path: &Path) -> Result<SourceFormat> {
    let prefix = read_local_prefix_bytes(path, SOURCE_SNIFF_BYTES)?;
    detect_source_format_from_prefix(path, &prefix)
}

fn effective_json_document(doc: &JsonValue) -> Result<JsonValue> {
    let obj = doc
        .as_object()
        .ok_or_else(|| anyhow!("JSON source documents must be objects"))?;
    if let Some(inner_doc) = obj.get("doc") {
        let inner_obj = inner_doc
            .as_object()
            .ok_or_else(|| anyhow!("Doc payload field 'doc' must be an object"))?;
        Ok(JsonValue::Object(inner_obj.clone()))
    } else {
        Ok(JsonValue::Object(obj.clone()))
    }
}

fn collect_effective_json_documents(docs: &[JsonValue]) -> Result<Vec<JsonValue>> {
    docs.iter().map(effective_json_document).collect()
}

fn detect_id_field_name(field_names: &[String]) -> Option<String> {
    if let Some(name) = field_names
        .iter()
        .find(|name| name.eq_ignore_ascii_case("id"))
    {
        return Some(name.clone());
    }

    for hash_name in ["sha256", "sha1", "md5"] {
        if let Some(name) = field_names
            .iter()
            .find(|name| name.eq_ignore_ascii_case(hash_name))
        {
            return Some(name.clone());
        }
    }

    if let Some(name) = field_names.iter().find(|name| {
        let lower = name.to_lowercase();
        lower.ends_with("id") || lower.ends_with("_id")
    }) {
        return Some(name.clone());
    }

    if let Some(name) = field_names
        .iter()
        .find(|name| name.to_lowercase().contains("id"))
    {
        return Some(name.clone());
    }

    field_names.first().cloned()
}

fn detect_json_id_field_name(docs: &[JsonValue]) -> Result<String> {
    let mut field_names = Vec::new();
    let mut seen = HashSet::new();

    for doc in docs {
        let obj = doc
            .as_object()
            .ok_or_else(|| anyhow!("JSON source documents must be objects"))?;
        for key in obj.keys() {
            if seen.insert(key.clone()) {
                field_names.push(key.clone());
            }
        }
    }

    detect_id_field_name(&field_names)
        .ok_or_else(|| anyhow!("Unable to detect an id field from JSON documents"))
}

fn json_value_to_id_string(value: &JsonValue) -> Option<String> {
    match value {
        JsonValue::String(s) => {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        }
        JsonValue::Number(n) => Some(n.to_string()),
        JsonValue::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

fn infer_json_field_type(docs: &[JsonValue], field_name: &str) -> TantivyFieldType {
    docs.iter()
        .filter_map(|doc| doc.as_object())
        .filter_map(|obj| obj.get(field_name))
        .find(|value| !value.is_null())
        .map(FieldDef::infer_type_from_value)
        .unwrap_or(TantivyFieldType::Text)
}

fn normalize_json_document_for_schema(doc: &JsonValue, id_field: &str) -> Result<JsonValue> {
    let mut obj = effective_json_document(doc)?
        .as_object()
        .cloned()
        .ok_or_else(|| anyhow!("JSON source documents must be objects"))?;

    let id = obj
        .get("id")
        .and_then(json_value_to_id_string)
        .or_else(|| obj.get(id_field).and_then(json_value_to_id_string))
        .ok_or_else(|| anyhow!("JSON document is missing a usable id field"))?;

    obj.insert("id".to_string(), JsonValue::String(id));
    Ok(JsonValue::Object(obj))
}

fn build_schema_from_effective_json_documents(
    effective_docs: &[JsonValue],
    id_field: &str,
) -> Result<JsonValue> {
    let mut schema = IndexSchema::default();
    if id_field != "id" {
        let field_type = infer_json_field_type(effective_docs, id_field);
        schema.add_shadow_field(id_field.to_string(), field_type);
    }

    let mut sampled = 0usize;
    for doc in effective_docs.iter().take(SCHEMA_SAMPLE_LIMIT) {
        let normalized = normalize_json_document_for_schema(doc, id_field)?;
        schema.evolve_from_document(&normalized);
        sampled += 1;
    }

    if sampled == 0 {
        anyhow::bail!("JSON source does not contain any valid object documents");
    }

    for (name, field_def) in schema.fields.iter_mut() {
        if !field_def.is_shadow {
            field_def.indexed = true;
            field_def.stored = name == "id";
        }
    }

    if !schema.fields.contains_key("id") {
        let id_field = FieldDef {
            name: "id".to_string(),
            field_type: TantivyFieldType::Text,
            indexed: true,
            stored: true,
            fast: false,
            is_shadow: false,
            tokenizer: Some("raw".to_string()),
            index_record_option: Some("Basic".to_string()),
        };
        schema.fields.insert("id".to_string(), id_field);
    }

    schema.auto_detect_routing_field();
    schema.fingerprint = schema.calculate_fingerprint();

    serde_json::to_value(schema).context("Failed to serialize schema")
}

fn build_schema_from_json_documents(docs: &[JsonValue]) -> Result<JsonValue> {
    let effective_docs = collect_effective_json_documents(docs)?;
    let id_field = detect_json_id_field_name(&effective_docs)?;
    build_schema_from_effective_json_documents(&effective_docs, &id_field)
}

fn build_json_source_analysis_from_docs(docs: &[JsonValue]) -> Result<JsonSourceAnalysis> {
    let effective_docs = collect_effective_json_documents(docs)?;
    if effective_docs.is_empty() {
        anyhow::bail!("JSON source does not contain any valid object documents");
    }

    let id_field = detect_json_id_field_name(&effective_docs)?;
    let sample_docs = effective_docs
        .into_iter()
        .take(SCHEMA_SAMPLE_LIMIT)
        .collect::<Vec<_>>();

    Ok(JsonSourceAnalysis {
        sample_docs,
        id_field,
    })
}

fn collect_json_analysis_doc(
    raw_doc: &JsonValue,
    sample_docs: &mut Vec<JsonValue>,
    field_names: &mut Vec<String>,
    seen: &mut HashSet<String>,
) -> Result<()> {
    let effective_doc = effective_json_document(raw_doc)?;
    let obj = effective_doc
        .as_object()
        .ok_or_else(|| anyhow!("JSON source documents must be objects"))?;

    if sample_docs.len() < SCHEMA_SAMPLE_LIMIT {
        sample_docs.push(JsonValue::Object(obj.clone()));
    }

    for key in obj.keys() {
        if seen.insert(key.clone()) {
            field_names.push(key.clone());
        }
    }

    Ok(())
}

#[derive(Debug)]
struct JsonLinesChunkParser {
    buffer: Vec<u8>,
    line_number: usize,
    seen_docs: usize,
}

impl JsonLinesChunkParser {
    fn new() -> Self {
        Self {
            buffer: Vec::new(),
            line_number: 0,
            seen_docs: 0,
        }
    }

    fn push_chunk(&mut self, chunk: &[u8]) -> Result<Vec<JsonValue>> {
        self.buffer.extend_from_slice(chunk);
        let mut docs = Vec::new();

        while let Some(pos) = self.buffer.iter().position(|byte| *byte == b'\n') {
            let line_bytes: Vec<u8> = self.buffer.drain(..=pos).collect();
            if let Some(doc) = self.parse_line_bytes(&line_bytes)? {
                docs.push(doc);
            }
        }

        Ok(docs)
    }

    fn finish(mut self) -> Result<Vec<JsonValue>> {
        let mut docs = Vec::new();
        if !self.buffer.is_empty() {
            let remaining = std::mem::take(&mut self.buffer);
            if let Some(doc) = self.parse_line_bytes(&remaining)? {
                docs.push(doc);
            }
        }

        if self.seen_docs == 0 {
            anyhow::bail!("JSON lines source does not contain any documents");
        }

        Ok(docs)
    }

    fn parse_line_bytes(&mut self, line_bytes: &[u8]) -> Result<Option<JsonValue>> {
        self.line_number += 1;
        let line = std::str::from_utf8(line_bytes).with_context(|| {
            format!(
                "JSON lines source is not valid UTF-8 at line {}",
                self.line_number
            )
        })?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            return Ok(None);
        }

        let doc: JsonValue = serde_json::from_str(trimmed)
            .with_context(|| format!("Invalid JSON on line {}", self.line_number))?;
        if !doc.is_object() {
            anyhow::bail!("JSON line {} must contain an object", self.line_number);
        }
        self.seen_docs += 1;
        Ok(Some(doc))
    }
}

#[derive(Debug)]
struct JsonArrayChunkParser {
    started: bool,
    finished: bool,
    current: Vec<u8>,
    depth: usize,
    in_string: bool,
    escape: bool,
    seen_docs: usize,
}

impl JsonArrayChunkParser {
    fn new() -> Self {
        Self {
            started: false,
            finished: false,
            current: Vec::new(),
            depth: 0,
            in_string: false,
            escape: false,
            seen_docs: 0,
        }
    }

    fn push_chunk(&mut self, chunk: &[u8]) -> Result<Vec<JsonValue>> {
        let mut docs = Vec::new();

        for &byte in chunk {
            if !self.started {
                if byte.is_ascii_whitespace() {
                    continue;
                }
                if byte != b'[' {
                    anyhow::bail!("JSON array source must start with '['");
                }
                self.started = true;
                continue;
            }

            if self.finished {
                if !byte.is_ascii_whitespace() {
                    anyhow::bail!("Invalid trailing data after JSON array source");
                }
                continue;
            }

            if self.current.is_empty() {
                match byte {
                    b' ' | b'\t' | b'\r' | b'\n' | b',' => continue,
                    b']' => {
                        self.finished = true;
                        continue;
                    }
                    b'{' => {
                        self.current.push(byte);
                        self.depth = 1;
                        self.in_string = false;
                        self.escape = false;
                    }
                    _ => anyhow::bail!("JSON array documents must be objects"),
                }
                continue;
            }

            self.current.push(byte);
            if self.in_string {
                if self.escape {
                    self.escape = false;
                } else if byte == b'\\' {
                    self.escape = true;
                } else if byte == b'"' {
                    self.in_string = false;
                }
                continue;
            }

            match byte {
                b'"' => self.in_string = true,
                b'{' | b'[' => self.depth += 1,
                b'}' | b']' => {
                    self.depth = self
                        .depth
                        .checked_sub(1)
                        .ok_or_else(|| anyhow!("Invalid JSON array nesting"))?;
                    if self.depth == 0 {
                        let doc = self.finish_current_document()?;
                        docs.push(doc);
                    }
                }
                _ => {}
            }
        }

        Ok(docs)
    }

    fn finish(self) -> Result<Vec<JsonValue>> {
        if !self.current.is_empty() {
            anyhow::bail!("JSON array source ended before a document was complete");
        }
        if !self.started {
            anyhow::bail!("JSON array source is empty");
        }
        if !self.finished {
            anyhow::bail!("JSON array source ended before closing ']'");
        }
        if self.seen_docs == 0 {
            anyhow::bail!("JSON array source does not contain any documents");
        }
        Ok(Vec::new())
    }

    fn finish_current_document(&mut self) -> Result<JsonValue> {
        let raw = std::mem::take(&mut self.current);
        let doc: JsonValue =
            serde_json::from_slice(&raw).context("Failed to parse JSON array document")?;
        if !doc.is_object() {
            anyhow::bail!("JSON array documents must be objects");
        }
        self.depth = 0;
        self.in_string = false;
        self.escape = false;
        self.seen_docs += 1;
        Ok(doc)
    }
}

#[derive(Debug)]
struct JsonObjectChunkParser {
    started: bool,
    finished: bool,
    current: Vec<u8>,
    depth: usize,
    in_string: bool,
    escape: bool,
}

impl JsonObjectChunkParser {
    fn new() -> Self {
        Self {
            started: false,
            finished: false,
            current: Vec::new(),
            depth: 0,
            in_string: false,
            escape: false,
        }
    }

    fn push_chunk(&mut self, chunk: &[u8]) -> Result<Vec<JsonValue>> {
        let mut docs = Vec::new();

        for &byte in chunk {
            if !self.started {
                if byte.is_ascii_whitespace() {
                    continue;
                }
                if byte != b'{' {
                    anyhow::bail!("JSON document source must start with '{{'");
                }
                self.started = true;
                self.current.push(byte);
                self.depth = 1;
                continue;
            }

            if self.finished {
                if !byte.is_ascii_whitespace() {
                    anyhow::bail!("Invalid trailing data after JSON document source");
                }
                continue;
            }

            self.current.push(byte);
            if self.in_string {
                if self.escape {
                    self.escape = false;
                } else if byte == b'\\' {
                    self.escape = true;
                } else if byte == b'"' {
                    self.in_string = false;
                }
                continue;
            }

            match byte {
                b'"' => self.in_string = true,
                b'{' | b'[' => self.depth += 1,
                b'}' | b']' => {
                    self.depth = self
                        .depth
                        .checked_sub(1)
                        .ok_or_else(|| anyhow!("Invalid JSON document nesting"))?;
                    if self.depth == 0 {
                        let raw = std::mem::take(&mut self.current);
                        let doc: JsonValue = serde_json::from_slice(&raw)
                            .context("Failed to parse JSON document")?;
                        if !doc.is_object() {
                            anyhow::bail!("JSON document source must contain an object");
                        }
                        self.finished = true;
                        docs.push(doc);
                    }
                }
                _ => {}
            }
        }

        Ok(docs)
    }

    fn finish(self) -> Result<Vec<JsonValue>> {
        if !self.started {
            anyhow::bail!("JSON document source is empty");
        }
        if !self.finished {
            anyhow::bail!("JSON document source ended before the object was complete");
        }
        Ok(Vec::new())
    }
}

#[derive(Debug)]
enum JsonChunkParser {
    Lines(JsonLinesChunkParser),
    Array(JsonArrayChunkParser),
    Object(JsonObjectChunkParser),
}

impl JsonChunkParser {
    fn new(format: SourceFormat) -> Result<Self> {
        match format {
            SourceFormat::JsonLines => Ok(Self::Lines(JsonLinesChunkParser::new())),
            SourceFormat::JsonArray => Ok(Self::Array(JsonArrayChunkParser::new())),
            SourceFormat::JsonDocument => Ok(Self::Object(JsonObjectChunkParser::new())),
            SourceFormat::SchemaJson => Err(anyhow!(
                "Schema JSON object cannot be used as document data"
            )),
            SourceFormat::CsvLike => Err(anyhow!("Source is not JSON data")),
        }
    }

    fn push_chunk(&mut self, chunk: &[u8]) -> Result<Vec<JsonValue>> {
        match self {
            Self::Lines(parser) => parser.push_chunk(chunk),
            Self::Array(parser) => parser.push_chunk(chunk),
            Self::Object(parser) => parser.push_chunk(chunk),
        }
    }

    fn finish(self) -> Result<Vec<JsonValue>> {
        match self {
            Self::Lines(parser) => parser.finish(),
            Self::Array(parser) => parser.finish(),
            Self::Object(parser) => parser.finish(),
        }
    }
}

fn read_json_value_from_path(path: &Path) -> Result<JsonValue> {
    let compression = detect_compression(path.to_str().unwrap_or(""));
    let reader = open_local_reader(path, compression)?;
    serde_json::from_reader(BufReader::new(reader))
        .with_context(|| format!("Failed to parse JSON source file: {}", path.display()))
}

fn process_json_array_reader<R, F>(reader: R, on_doc: &mut F) -> Result<usize>
where
    R: Read,
    F: FnMut(JsonValue) -> Result<()>,
{
    use serde::de::{self, DeserializeSeed, SeqAccess, Visitor};

    struct JsonArraySeed<'a, F> {
        on_doc: &'a mut F,
    }

    struct JsonArrayVisitor<'a, F> {
        on_doc: &'a mut F,
    }

    impl<'de, 'a, F> Visitor<'de> for JsonArrayVisitor<'a, F>
    where
        F: FnMut(JsonValue) -> Result<()>,
    {
        type Value = usize;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a JSON array of object documents")
        }

        fn visit_seq<A>(self, mut seq: A) -> std::result::Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let mut count = 0usize;
            while let Some(value) = seq.next_element::<JsonValue>()? {
                if !value.is_object() {
                    return Err(de::Error::custom("JSON array documents must be objects"));
                }
                (self.on_doc)(value).map_err(de::Error::custom)?;
                count += 1;
            }

            if count == 0 {
                return Err(de::Error::custom(
                    "JSON array source does not contain any documents",
                ));
            }

            Ok(count)
        }
    }

    impl<'de, 'a, F> DeserializeSeed<'de> for JsonArraySeed<'a, F>
    where
        F: FnMut(JsonValue) -> Result<()>,
    {
        type Value = usize;

        fn deserialize<D>(self, deserializer: D) -> std::result::Result<Self::Value, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            deserializer.deserialize_seq(JsonArrayVisitor {
                on_doc: self.on_doc,
            })
        }
    }

    let mut deserializer = serde_json::Deserializer::from_reader(reader);
    let count = JsonArraySeed { on_doc }
        .deserialize(&mut deserializer)
        .context("Failed to parse JSON array source")?;
    deserializer
        .end()
        .context("Invalid trailing data after JSON array source")?;
    Ok(count)
}

fn for_each_json_document_in_reader<R, F>(
    reader: R,
    format: SourceFormat,
    mut on_doc: F,
) -> Result<usize>
where
    R: Read,
    F: FnMut(JsonValue) -> Result<()>,
{
    match format {
        SourceFormat::JsonLines => {
            let buf_reader = BufReader::new(reader);
            let mut count = 0usize;

            for (line_no, line_result) in buf_reader.lines().enumerate() {
                let line = line_result
                    .with_context(|| format!("Failed to read JSONL source line {}", line_no + 1))?;
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }

                let doc: JsonValue = serde_json::from_str(trimmed)
                    .with_context(|| format!("Invalid JSON on line {}", line_no + 1))?;
                if !doc.is_object() {
                    anyhow::bail!("JSON line {} must contain an object", line_no + 1);
                }
                on_doc(doc)?;
                count += 1;
            }

            if count == 0 {
                anyhow::bail!("JSON lines source does not contain any documents");
            }

            Ok(count)
        }
        SourceFormat::JsonArray => process_json_array_reader(BufReader::new(reader), &mut on_doc),
        SourceFormat::JsonDocument => {
            let value: JsonValue = serde_json::from_reader(BufReader::new(reader))
                .context("Failed to parse JSON source")?;
            if !value.is_object() {
                anyhow::bail!("JSON document source must contain an object");
            }
            on_doc(value)?;
            Ok(1)
        }
        SourceFormat::SchemaJson => Err(anyhow!(
            "Schema JSON object cannot be used as document data"
        )),
        SourceFormat::CsvLike => Err(anyhow!("Source is not JSON data")),
    }
}

fn for_each_json_document_in_local_source<F>(
    source: &str,
    format: SourceFormat,
    on_doc: F,
) -> Result<usize>
where
    F: FnMut(JsonValue) -> Result<()>,
{
    let path = Path::new(source);
    let compression = detect_compression(source);
    let reader = open_local_reader(path, compression)?;
    for_each_json_document_in_reader(reader, format, on_doc)
}

fn analyze_local_json_source_for_schema(
    source: &str,
    format: SourceFormat,
) -> Result<JsonSourceAnalysis> {
    let mut sample_docs = Vec::new();
    let mut field_names = Vec::new();
    let mut seen = HashSet::new();

    let count = for_each_json_document_in_local_source(source, format, |raw_doc| {
        let effective_doc = effective_json_document(&raw_doc)?;
        let obj = effective_doc
            .as_object()
            .ok_or_else(|| anyhow!("JSON source documents must be objects"))?;

        if sample_docs.len() < SCHEMA_SAMPLE_LIMIT {
            sample_docs.push(JsonValue::Object(obj.clone()));
        }

        for key in obj.keys() {
            if seen.insert(key.clone()) {
                field_names.push(key.clone());
            }
        }

        Ok(())
    })?;

    if count == 0 || sample_docs.is_empty() {
        anyhow::bail!("JSON source does not contain any valid object documents");
    }

    let id_field = detect_id_field_name(&field_names)
        .ok_or_else(|| anyhow!("Unable to detect an id field from JSON documents"))?;

    Ok(JsonSourceAnalysis {
        sample_docs,
        id_field,
    })
}

fn analyze_reader_json_source_for_schema<R: Read>(
    reader: R,
    format: SourceFormat,
) -> Result<JsonSourceAnalysis> {
    let mut sample_docs = Vec::new();
    let mut field_names = Vec::new();
    let mut seen = HashSet::new();

    let count = for_each_json_document_in_reader(reader, format, |raw_doc| {
        let effective_doc = effective_json_document(&raw_doc)?;
        let obj = effective_doc
            .as_object()
            .ok_or_else(|| anyhow!("JSON source documents must be objects"))?;

        if sample_docs.len() < SCHEMA_SAMPLE_LIMIT {
            sample_docs.push(JsonValue::Object(obj.clone()));
        }

        for key in obj.keys() {
            if seen.insert(key.clone()) {
                field_names.push(key.clone());
            }
        }

        Ok(())
    })?;

    if count == 0 || sample_docs.is_empty() {
        anyhow::bail!("JSON source does not contain any valid object documents");
    }

    let id_field = detect_id_field_name(&field_names)
        .ok_or_else(|| anyhow!("Unable to detect an id field from JSON documents"))?;

    Ok(JsonSourceAnalysis {
        sample_docs,
        id_field,
    })
}

fn build_doc_payload_from_json_document(raw_doc: &JsonValue, id_field: &str) -> Result<JsonValue> {
    let raw_obj = raw_doc
        .as_object()
        .ok_or_else(|| anyhow!("JSON source documents must be objects"))?;

    let mut doc_obj = if let Some(inner_doc) = raw_obj.get("doc") {
        inner_doc
            .as_object()
            .cloned()
            .ok_or_else(|| anyhow!("Doc payload field 'doc' must be an object"))?
    } else {
        raw_obj.clone()
    };

    let id = raw_obj
        .get("id")
        .and_then(json_value_to_id_string)
        .or_else(|| doc_obj.get("id").and_then(json_value_to_id_string))
        .or_else(|| doc_obj.get(id_field).and_then(json_value_to_id_string))
        .ok_or_else(|| anyhow!("JSON document is missing a usable id field"))?;

    doc_obj.insert("id".to_string(), JsonValue::String(id.clone()));

    let routing_key = raw_obj
        .get("routing_key")
        .and_then(json_value_to_id_string)
        .or_else(|| doc_obj.get(id_field).and_then(json_value_to_id_string))
        .unwrap_or_else(|| id.clone());

    Ok(json!({
        "id": id,
        "routing_key": routing_key,
        "doc": JsonValue::Object(doc_obj),
    }))
}

fn record_ingest_response(response: &JsonValue, total_sent: &mut usize, total_failed: &mut usize) {
    let written = response
        .get("items_written")
        .and_then(|v| v.as_u64())
        .unwrap_or(0) as usize;
    let errors_json = response.get("errors").and_then(|v| v.as_array());
    let error_count = errors_json.map(|e| e.len()).unwrap_or(0);

    *total_sent += written;
    *total_failed += error_count;

    if error_count > 0 {
        eprintln!(
            "⚠️  Batch warning: {} items failed validation.",
            error_count
        );
        if let Some(errs) = errors_json {
            for err in errs.iter().take(3) {
                eprintln!("   - {}", err.as_str().unwrap_or("Unknown error"));
            }
            if errs.len() > 3 {
                eprintln!("   ... and {} more", errs.len() - 3);
            }
        }
    }
}

async fn fetch_source_prefix_bytes(
    client: &CameoClient,
    source: &str,
    max_bytes: usize,
) -> Result<Vec<u8>> {
    if is_http_source(source) {
        let compression = detect_compression(source);
        if compression != Compression::None {
            // Compressed remote: download all bytes, decompress, return prefix
            let all_bytes = fetch_bytes_source(client, source).await?;
            let len = all_bytes.len().min(max_bytes);
            return Ok(all_bytes[..len].to_vec());
        }

        let url = Url::parse(source).context("Invalid URL for source")?;
        let mut response = client
            .http()
            .get(url)
            .send()
            .await
            .context("Failed to fetch remote source")?;
        let status = response.status();
        if !status.is_success() {
            let text = response.text().await.unwrap_or_default();
            anyhow::bail!("Failed to fetch remote source: {} - {}", status, text);
        }

        let mut prefix = Vec::new();
        while prefix.len() < max_bytes {
            match response
                .chunk()
                .await
                .context("Failed to read remote source body")?
            {
                Some(chunk) => {
                    let remaining = max_bytes - prefix.len();
                    let take_len = remaining.min(chunk.len());
                    prefix.extend_from_slice(&chunk[..take_len]);
                    if take_len < chunk.len() {
                        break;
                    }
                }
                None => break,
            }
        }

        Ok(prefix)
    } else {
        read_local_prefix_bytes(Path::new(source), max_bytes)
    }
}

async fn detect_source_format_for_source(
    client: &CameoClient,
    source: &str,
) -> Result<SourceFormat> {
    if is_http_source(source) {
        let prefix = fetch_source_prefix_bytes(client, source, SOURCE_SNIFF_BYTES).await?;
        let extension = source_extension(source);
        detect_source_format_from_hint(extension.as_deref(), &prefix)
    } else {
        detect_local_source_format(Path::new(source))
    }
}

async fn load_json_value_from_source(client: &CameoClient, source: &str) -> Result<JsonValue> {
    if is_http_source(source) {
        let raw_bytes = fetch_bytes_source(client, source).await?;
        serde_json::from_slice(&raw_bytes).context("Failed to parse JSON source")
    } else {
        let source = source.to_string();
        tokio::task::spawn_blocking(move || read_json_value_from_path(Path::new(&source)))
            .await
            .map_err(|err| anyhow!("Local JSON source parsing failed: {}", err))?
    }
}

async fn for_each_json_document_in_http_source<F>(
    client: &CameoClient,
    source: &str,
    format: SourceFormat,
    mut on_doc: F,
) -> Result<usize>
where
    F: FnMut(JsonValue) -> Result<()>,
{
    let url = Url::parse(source).context("Invalid URL for JSON source")?;
    let mut response = client
        .http()
        .get(url)
        .send()
        .await
        .context("Failed to fetch remote JSON source")?;
    let status = response.status();
    if !status.is_success() {
        let text = response.text().await.unwrap_or_default();
        anyhow::bail!("Failed to fetch remote JSON source: {} - {}", status, text);
    }

    let mut parser = JsonChunkParser::new(format)?;
    let mut count = 0usize;

    while let Some(chunk) = response
        .chunk()
        .await
        .context("Failed to read remote JSON source body")?
    {
        let docs = parser.push_chunk(&chunk)?;
        for doc in docs {
            on_doc(doc)?;
            count += 1;
        }
    }

    for doc in parser.finish()? {
        on_doc(doc)?;
        count += 1;
    }

    Ok(count)
}

async fn analyze_http_json_source_for_schema(
    client: &CameoClient,
    source: &str,
    format: SourceFormat,
) -> Result<JsonSourceAnalysis> {
    let mut sample_docs = Vec::new();
    let mut field_names = Vec::new();
    let mut seen = HashSet::new();

    let count = for_each_json_document_in_http_source(client, source, format, |raw_doc| {
        collect_json_analysis_doc(&raw_doc, &mut sample_docs, &mut field_names, &mut seen)
    })
    .await?;

    if count == 0 || sample_docs.is_empty() {
        anyhow::bail!("JSON source does not contain any valid object documents");
    }

    let id_field = detect_id_field_name(&field_names)
        .ok_or_else(|| anyhow!("Unable to detect an id field from JSON documents"))?;

    Ok(JsonSourceAnalysis {
        sample_docs,
        id_field,
    })
}

async fn analyze_json_source_for_schema(
    client: &CameoClient,
    source: &str,
    format: SourceFormat,
) -> Result<JsonSourceAnalysis> {
    match format {
        SourceFormat::JsonDocument => {
            let value = load_json_value_from_source(client, source).await?;
            if value.get("fields").is_some() {
                anyhow::bail!("Schema JSON object cannot be used as document data");
            }
            build_json_source_analysis_from_docs(&[value])
        }
        SourceFormat::JsonArray | SourceFormat::JsonLines => {
            let compression = detect_compression(source);
            if is_http_source(source) && compression == Compression::None {
                analyze_http_json_source_for_schema(client, source, format).await
            } else if is_http_source(source) {
                // Compressed remote: download all, decompress, analyze in memory
                let bytes = fetch_bytes_source(client, source).await?;
                tokio::task::spawn_blocking(move || {
                    let reader = Cursor::new(bytes);
                    analyze_reader_json_source_for_schema(reader, format)
                })
                .await
                .map_err(|err| anyhow!("Compressed JSON source analysis failed: {}", err))?
            } else {
                let source = source.to_string();
                tokio::task::spawn_blocking(move || {
                    analyze_local_json_source_for_schema(&source, format)
                })
                .await
                .map_err(|err| anyhow!("Local JSON source analysis failed: {}", err))?
            }
        }
        SourceFormat::SchemaJson => Err(anyhow!(
            "Schema JSON object cannot be used as document data"
        )),
        SourceFormat::CsvLike => Err(anyhow!("Source is not JSON data")),
    }
}

async fn flush_ndjson_batch(
    client: &CameoClient,
    index: &str,
    batch_body: &mut Vec<u8>,
    total_sent: &mut usize,
    total_failed: &mut usize,
) -> Result<()> {
    if batch_body.is_empty() {
        return Ok(());
    }

    let response = client
        .stream_index_ndjson(index, std::mem::take(batch_body))
        .await?;
    record_ingest_response(&response, total_sent, total_failed);
    Ok(())
}

async fn load_data_from_http_json_source_single_pass(
    client: &CameoClient,
    index: &str,
    source: &str,
    format: SourceFormat,
    batch_size: usize,
    schema_exists: bool,
) -> Result<()> {
    let batch_size = batch_size.max(1);
    let mut spinner = ProgressSpinner::new();
    let url = Url::parse(source).context("Invalid URL for JSON source")?;
    let mut response = client
        .http()
        .get(url)
        .send()
        .await
        .context("Failed to fetch remote JSON source")?;
    let status = response.status();
    if !status.is_success() {
        let text = response.text().await.unwrap_or_default();
        spinner.stop();
        anyhow::bail!("Failed to fetch remote JSON source: {} - {}", status, text);
    }

    let mut parser = JsonChunkParser::new(format)?;
    let mut sample_docs = Vec::new();
    let mut field_names = Vec::new();
    let mut seen = HashSet::new();
    let mut raw_sample_docs: Vec<JsonValue> = Vec::new();
    let mut batch_body = Vec::new();
    let mut docs_in_batch = 0usize;
    let mut total_sent = 0usize;
    let mut total_failed = 0usize;
    let mut id_field_detected: Option<String> = None;
    let mut samples_flushed = false;

    // Helper: collect effective doc fields for schema sampling
    let collect_sample = |raw_doc: &JsonValue,
                          sample_docs: &mut Vec<JsonValue>,
                          raw_sample_docs: &mut Vec<JsonValue>,
                          field_names: &mut Vec<String>,
                          seen: &mut HashSet<String>|
     -> Result<()> {
        let effective_doc = effective_json_document(raw_doc)?;
        let obj = effective_doc
            .as_object()
            .ok_or_else(|| anyhow!("JSON source documents must be objects"))?;
        if sample_docs.len() < SCHEMA_SAMPLE_LIMIT {
            sample_docs.push(JsonValue::Object(obj.clone()));
            raw_sample_docs.push(raw_doc.clone());
            for key in obj.keys() {
                if seen.insert(key.clone()) {
                    field_names.push(key.clone());
                }
            }
        }
        Ok(())
    };

    // Helper: serialize a single doc into the NDJSON batch buffer
    let append_doc_to_batch = |raw_doc: &JsonValue,
                               id_field: &str,
                               batch_body: &mut Vec<u8>,
                               docs_in_batch: &mut usize|
     -> Result<()> {
        let payload = build_doc_payload_from_json_document(raw_doc, id_field)?;
        let mut line = serde_json::to_vec(&payload).context("Failed to serialize JSON payload")?;
        line.push(b'\n');
        batch_body.extend_from_slice(&line);
        *docs_in_batch += 1;
        Ok(())
    };

    let result: Result<JsonSourceAnalysis> = async {
        while let Some(chunk) = response
            .chunk()
            .await
            .context("Failed to read remote JSON source body")?
        {
            for raw_doc in parser.push_chunk(&chunk)? {
                if !samples_flushed {
                    collect_sample(
                        &raw_doc,
                        &mut sample_docs,
                        &mut raw_sample_docs,
                        &mut field_names,
                        &mut seen,
                    )?;
                    if id_field_detected.is_none() && sample_docs.len() >= SCHEMA_SAMPLE_LIMIT {
                        id_field_detected = Some(
                            detect_id_field_name(&field_names)
                                .ok_or_else(|| anyhow!("Unable to detect id field"))?,
                        );
                    }
                    if let Some(ref id_field) = id_field_detected {
                        if !schema_exists {
                            let schema =
                                build_schema_from_effective_json_documents(&sample_docs, id_field)?;
                            client
                                .put_index_config(index, &schema)
                                .await
                                .with_context(|| {
                                    format!("Failed to create schema for index '{}'", index)
                                })?;
                            println!(
                                "Schema was missing; detected and applied schema to index '{}'",
                                index
                            );
                        }
                        // Flush all buffered sample docs
                        for buffered_doc in raw_sample_docs.drain(..) {
                            append_doc_to_batch(
                                &buffered_doc,
                                id_field,
                                &mut batch_body,
                                &mut docs_in_batch,
                            )?;
                            if docs_in_batch >= batch_size {
                                flush_ndjson_batch(
                                    client,
                                    index,
                                    &mut batch_body,
                                    &mut total_sent,
                                    &mut total_failed,
                                )
                                .await?;
                                docs_in_batch = 0;
                            }
                        }
                        samples_flushed = true;
                    }
                    continue;
                }
                if let Some(ref id_field) = id_field_detected {
                    append_doc_to_batch(&raw_doc, id_field, &mut batch_body, &mut docs_in_batch)?;
                    if docs_in_batch >= batch_size {
                        flush_ndjson_batch(
                            client,
                            index,
                            &mut batch_body,
                            &mut total_sent,
                            &mut total_failed,
                        )
                        .await?;
                        docs_in_batch = 0;
                    }
                }
            }
        }
        for raw_doc in parser.finish()? {
            if !samples_flushed {
                collect_sample(
                    &raw_doc,
                    &mut sample_docs,
                    &mut raw_sample_docs,
                    &mut field_names,
                    &mut seen,
                )?;
            }
            if id_field_detected.is_none() {
                id_field_detected = Some(
                    detect_id_field_name(&field_names)
                        .ok_or_else(|| anyhow!("Unable to detect id field"))?,
                );
            }
            if !samples_flushed {
                if let Some(ref id_field) = id_field_detected {
                    if !schema_exists {
                        let schema =
                            build_schema_from_effective_json_documents(&sample_docs, id_field)?;
                        client
                            .put_index_config(index, &schema)
                            .await
                            .with_context(|| {
                                format!("Failed to create schema for index '{}'", index)
                            })?;
                        println!(
                            "Schema was missing; detected and applied schema to index '{}'",
                            index
                        );
                    }
                    for buffered_doc in raw_sample_docs.drain(..) {
                        append_doc_to_batch(
                            &buffered_doc,
                            id_field,
                            &mut batch_body,
                            &mut docs_in_batch,
                        )?;
                        if docs_in_batch >= batch_size {
                            flush_ndjson_batch(
                                client,
                                index,
                                &mut batch_body,
                                &mut total_sent,
                                &mut total_failed,
                            )
                            .await?;
                            docs_in_batch = 0;
                        }
                    }
                    samples_flushed = true;
                }
                continue;
            }
            if let Some(ref id_field) = id_field_detected {
                append_doc_to_batch(&raw_doc, id_field, &mut batch_body, &mut docs_in_batch)?;
                if docs_in_batch >= batch_size {
                    flush_ndjson_batch(
                        client,
                        index,
                        &mut batch_body,
                        &mut total_sent,
                        &mut total_failed,
                    )
                    .await?;
                    docs_in_batch = 0;
                }
            }
        }
        flush_ndjson_batch(
            client,
            index,
            &mut batch_body,
            &mut total_sent,
            &mut total_failed,
        )
        .await?;
        let id_field = id_field_detected.ok_or_else(|| anyhow!("Unable to detect id field"))?;
        Ok(JsonSourceAnalysis {
            sample_docs,
            id_field,
        })
    }
    .await;

    spinner.stop();
    let _analysis = result?;

    println!(
        "Ingestion complete for index '{}': loaded={} failed={} (batch size {})",
        index, total_sent, total_failed, batch_size
    );
    Ok(())
}

enum LocalJsonProducerMsg {
    CreateSchema(JsonSourceAnalysis),
    DataBatch(Vec<u8>),
}

async fn load_data_from_reader_json_source_single_pass(
    client: &CameoClient,
    index: &str,
    reader: Box<dyn Read + Send + 'static>,
    format: SourceFormat,
    batch_size: usize,
    schema_exists: bool,
) -> Result<()> {
    let batch_size = batch_size.max(1);
    let mut spinner = ProgressSpinner::new();
    let (tx, mut rx) = tokio::sync::mpsc::channel::<LocalJsonProducerMsg>(2);

    let producer = tokio::task::spawn_blocking(move || -> Result<usize> {
        let mut sample_docs = Vec::new();
        let mut field_names = Vec::new();
        let mut seen = HashSet::new();
        let mut raw_sample_docs: Vec<JsonValue> = Vec::new();
        let mut batch_body = Vec::new();
        let mut docs_in_batch = 0usize;
        let mut total_docs = 0usize;
        let mut id_field_detected: Option<String> = None;
        let mut samples_flushed = false;

        let send_err = || anyhow!("Failed to send message because receiver was dropped");

        for_each_json_document_in_reader(reader, format, |raw_doc| {
            let effective_doc = effective_json_document(&raw_doc)?;
            let obj = effective_doc
                .as_object()
                .ok_or_else(|| anyhow!("JSON source documents must be objects"))?;

            // Phase 1: Collect samples
            if !samples_flushed {
                if sample_docs.len() < SCHEMA_SAMPLE_LIMIT {
                    sample_docs.push(JsonValue::Object(obj.clone()));
                    raw_sample_docs.push(raw_doc.clone());
                    for key in obj.keys() {
                        if seen.insert(key.clone()) {
                            field_names.push(key.clone());
                        }
                    }
                }

                if id_field_detected.is_none() && sample_docs.len() >= SCHEMA_SAMPLE_LIMIT {
                    id_field_detected =
                        Some(detect_id_field_name(&field_names).ok_or_else(|| {
                            anyhow!("Unable to detect an id field from JSON documents")
                        })?);
                }

                if let Some(ref id_field) = id_field_detected {
                    if !schema_exists {
                        tx.blocking_send(LocalJsonProducerMsg::CreateSchema(JsonSourceAnalysis {
                            sample_docs: sample_docs.clone(),
                            id_field: id_field.clone(),
                        }))
                        .map_err(|_| send_err())?;
                    }

                    // Flush all buffered raw docs as data batches
                    for buffered_doc in raw_sample_docs.drain(..) {
                        let payload =
                            build_doc_payload_from_json_document(&buffered_doc, id_field)?;
                        let mut line = serde_json::to_vec(&payload)?;
                        line.push(b'\n');
                        batch_body.extend_from_slice(&line);
                        docs_in_batch += 1;
                        total_docs += 1;

                        if docs_in_batch >= batch_size {
                            tx.blocking_send(LocalJsonProducerMsg::DataBatch(std::mem::take(
                                &mut batch_body,
                            )))
                            .map_err(|_| send_err())?;
                            docs_in_batch = 0;
                        }
                    }
                    samples_flushed = true;
                }
                return Ok(());
            }

            // Phase 2: Normal ingestion after samples flushed
            let id_field = id_field_detected.as_ref().unwrap();
            let payload = build_doc_payload_from_json_document(&raw_doc, id_field)?;
            let mut line = serde_json::to_vec(&payload)?;
            line.push(b'\n');
            batch_body.extend_from_slice(&line);
            docs_in_batch += 1;
            total_docs += 1;

            if docs_in_batch >= batch_size {
                tx.blocking_send(LocalJsonProducerMsg::DataBatch(std::mem::take(
                    &mut batch_body,
                )))
                .map_err(|_| send_err())?;
                docs_in_batch = 0;
            }

            Ok(())
        })?;

        // Handle small files (fewer than SCHEMA_SAMPLE_LIMIT docs)
        if !samples_flushed && !sample_docs.is_empty() {
            let id_field = id_field_detected
                .or_else(|| detect_id_field_name(&field_names))
                .ok_or_else(|| anyhow!("Unable to detect an id field from JSON documents"))?;

            if !schema_exists {
                tx.blocking_send(LocalJsonProducerMsg::CreateSchema(JsonSourceAnalysis {
                    sample_docs,
                    id_field: id_field.clone(),
                }))
                .map_err(|_| send_err())?;
            }

            for buffered_doc in raw_sample_docs.drain(..) {
                let payload = build_doc_payload_from_json_document(&buffered_doc, &id_field)?;
                let mut line = serde_json::to_vec(&payload)?;
                line.push(b'\n');
                batch_body.extend_from_slice(&line);
                docs_in_batch += 1;
                total_docs += 1;

                if docs_in_batch >= batch_size {
                    tx.blocking_send(LocalJsonProducerMsg::DataBatch(std::mem::take(
                        &mut batch_body,
                    )))
                    .map_err(|_| send_err())?;
                    docs_in_batch = 0;
                }
            }
        }

        if !batch_body.is_empty() {
            tx.blocking_send(LocalJsonProducerMsg::DataBatch(batch_body))
                .map_err(|_| send_err())?;
        }

        Ok(total_docs)
    });

    let mut total_sent = 0usize;
    let mut total_failed = 0usize;

    let send_result: Result<()> = async {
        while let Some(msg) = rx.recv().await {
            match msg {
                LocalJsonProducerMsg::CreateSchema(analysis) => {
                    let schema = build_schema_from_effective_json_documents(
                        &analysis.sample_docs,
                        &analysis.id_field,
                    )
                    .context("Failed to detect schema while auto-creating index schema")?;
                    client
                        .put_index_config(index, &schema)
                        .await
                        .with_context(|| {
                            format!("Failed to create schema for index '{}'", index)
                        })?;
                    println!(
                        "Schema was missing; detected and applied schema to index '{}'",
                        index
                    );
                }
                LocalJsonProducerMsg::DataBatch(batch_body) => {
                    let response = client.stream_index_ndjson(index, batch_body).await?;
                    record_ingest_response(&response, &mut total_sent, &mut total_failed);
                }
            }
        }
        Ok(())
    }
    .await;

    drop(rx);
    let _total_docs = producer
        .await
        .map_err(|err| anyhow!("Local JSON batch producer failed: {}", err))??;

    spinner.stop();
    send_result?;

    println!(
        "Ingestion complete for index '{}': loaded={} failed={} (batch size {})",
        index, total_sent, total_failed, batch_size
    );
    Ok(())
}

async fn detect_schema_from_source(
    client: &CameoClient,
    source: &str,
    delimiter: Delimiter,
) -> Result<JsonValue> {
    load_schema_from_source(client, source, delimiter).await
}

async fn load_schema_from_source(
    client: &CameoClient,
    source: &str,
    delimiter: Delimiter,
) -> Result<JsonValue> {
    let mut spinner = ProgressSpinner::new();
    let format = detect_source_format_for_source(client, source).await?;

    let result = match format {
        SourceFormat::CsvLike => detect_schema_from_csv(client, source, delimiter).await,
        SourceFormat::SchemaJson => load_json_value_from_source(client, source).await,
        SourceFormat::JsonDocument => {
            let value = load_json_value_from_source(client, source).await?;
            if value.get("fields").is_some() {
                Ok(value)
            } else {
                build_schema_from_json_documents(&[value])
            }
        }
        SourceFormat::JsonArray | SourceFormat::JsonLines => {
            let analysis = analyze_json_source_for_schema(client, source, format).await?;
            build_schema_from_effective_json_documents(&analysis.sample_docs, &analysis.id_field)
        }
    };

    spinner.stop();
    result
}

async fn load_data_from_source(
    client: &CameoClient,
    index: &str,
    source: &str,
    delimiter: Delimiter,
    batch_size: usize,
) -> Result<()> {
    let format = detect_source_format_for_source(client, source).await?;

    match format {
        SourceFormat::CsvLike => {
            let schema_exists = client.get_index_config(index).await.is_ok();
            load_data_from_csv_single_pass(
                client,
                index,
                source,
                delimiter,
                batch_size,
                schema_exists,
            )
            .await
        }
        SourceFormat::SchemaJson => {
            Err(anyhow!("Schema JSON object cannot be loaded as index data"))
        }
        SourceFormat::JsonDocument | SourceFormat::JsonArray | SourceFormat::JsonLines => {
            let schema_exists = client.get_index_config(index).await.is_ok();
            let compression = detect_compression(source);

            if is_http_source(source) && compression == Compression::None {
                load_data_from_http_json_source_single_pass(
                    client,
                    index,
                    source,
                    format,
                    batch_size,
                    schema_exists,
                )
                .await
            } else if is_http_source(source) {
                // Compressed remote: download all, decompress, process via reader
                let bytes = fetch_bytes_source(client, source).await?;
                let reader: Box<dyn Read + Send> = Box::new(Cursor::new(bytes));
                load_data_from_reader_json_source_single_pass(
                    client,
                    index,
                    reader,
                    format,
                    batch_size,
                    schema_exists,
                )
                .await
            } else {
                let path = Path::new(source);
                let reader = open_local_reader(path, compression)?;
                load_data_from_reader_json_source_single_pass(
                    client,
                    index,
                    reader,
                    format,
                    batch_size,
                    schema_exists,
                )
                .await
            }
        }
    }
}

async fn load_data_from_csv_single_pass(
    client: &CameoClient,
    index: &str,
    source: &str,
    delimiter: Delimiter,
    batch_size: usize,
    schema_exists: bool,
) -> Result<()> {
    let batch_size = batch_size.max(1);
    let mut spinner = ProgressSpinner::new();
    let mut reader = open_csv_reader(client, source, delimiter).await?;
    let raw_headers = reader
        .headers()
        .context("CSV file is missing headers")?
        .clone();
    let headers: Vec<(String, Option<TantivyFieldType>)> =
        raw_headers.iter().map(parse_header_with_hint).collect();
    let id_detection = detect_id_field(&headers);
    let id_header = id_detection.original_field_name.clone();

    let mut sample_rows: Vec<csv::StringRecord> = Vec::new();
    let mut batch_body = Vec::new();
    let mut docs_in_batch = 0usize;
    let mut total_sent = 0usize;
    let mut total_failed = 0usize;
    let mut schema_created = schema_exists;

    // Helper: convert a CSV record into an NDJSON payload line
    let build_csv_ndjson_line = |record: &csv::StringRecord,
                                 headers: &[(String, Option<TantivyFieldType>)],
                                 id_detection: &IdFieldDetection,
                                 id_header: &str|
     -> Result<Vec<u8>> {
        let id_value = record
            .get(id_detection.index)
            .unwrap_or_default()
            .trim()
            .to_string();
        let mut doc_obj: JsonMap<String, JsonValue> = JsonMap::new();
        for (idx, value) in record.iter().enumerate() {
            if let Some((header, _)) = headers.get(idx) {
                doc_obj.insert(header.clone(), parse_csv_cell(value));
            }
        }
        doc_obj.insert("id".to_string(), JsonValue::String(id_value.clone()));
        let routing_key = doc_obj
            .get(id_header)
            .and_then(|v| v.as_str())
            .map(str::to_string)
            .unwrap_or_else(|| id_value.clone());
        let payload = json!({"id": id_value, "routing_key": routing_key, "doc": doc_obj});
        let mut line = serde_json::to_vec(&payload).context("Failed to serialize CSV payload")?;
        line.push(b'\n');
        Ok(line)
    };

    for record in reader.records() {
        let record = record.context("Failed to read CSV record")?;
        let id_value_raw = record.get(id_detection.index).unwrap_or_default();
        if id_value_raw.trim().is_empty() {
            continue;
        }

        if !schema_created {
            sample_rows.push(record.clone());
            if sample_rows.len() >= SCHEMA_SAMPLE_LIMIT {
                let mut schema = IndexSchema::default();
                if id_detection.is_shadow {
                    let field_type = headers[id_detection.index]
                        .1
                        .clone()
                        .unwrap_or(TantivyFieldType::Text);
                    schema.add_shadow_field(id_detection.original_field_name.clone(), field_type);
                }
                // Use the same schema detection logic as detect_schema_from_csv
                // This ensures all fields are marked as indexed (not just evolved as non-indexed)
                for row in &sample_rows {
                    let mut obj: JsonMap<String, JsonValue> = JsonMap::new();
                    for (idx, value) in row.iter().enumerate() {
                        if let Some((header, _)) = headers.get(idx) {
                            obj.insert(header.clone(), parse_csv_cell(value));
                        }
                    }
                    if let Some(raw_id) = row.get(id_detection.index) {
                        let id_val = raw_id.trim();
                        if !id_val.is_empty() {
                            obj.insert("id".to_string(), JsonValue::String(id_val.to_string()));
                        }
                    }
                    schema.evolve_from_document(&JsonValue::Object(obj));
                }

                // CRITICAL: Apply the same field indexing logic as detect_schema_from_csv
                // This ensures all fields are marked as indexed when loading data, not just during schema detection
                for (name, field_def) in schema.fields.iter_mut() {
                    // Don't modify shadow fields - they have special requirements
                    if !field_def.is_shadow {
                        field_def.indexed = true;
                        // Only 'id' field should be stored in Tantivy (architecture rule)
                        field_def.stored = name == "id";
                    }
                }

                // Apply type hints where provided
                for (name, hint) in &headers {
                    if let Some(t) = hint.clone() {
                        // Don't overwrite shadow fields - preserve their special status
                        if !schema.fields.get(name).is_some_and(|f| f.is_shadow) {
                            // FieldDef::new already sets correct stored/fast flags per architecture
                            let mut field_def = FieldDef::new(name.clone(), t);
                            field_def.indexed = true;
                            // stored flag already set correctly by FieldDef::new (only 'id' = true)
                            schema.fields.insert(name.clone(), field_def);
                        }
                    }
                }
                let schema_json =
                    serde_json::to_value(&schema).context("Failed to serialize schema")?;
                client
                    .put_index_config(index, &schema_json)
                    .await
                    .with_context(|| format!("Failed to create schema for index '{}'", index))?;
                schema_created = true;

                // Flush all buffered sample rows as NDJSON payloads
                for row in sample_rows.drain(..) {
                    let line = build_csv_ndjson_line(&row, &headers, &id_detection, &id_header)?;
                    batch_body.extend_from_slice(&line);
                    docs_in_batch += 1;
                    if docs_in_batch >= batch_size {
                        flush_ndjson_batch(
                            client,
                            index,
                            &mut batch_body,
                            &mut total_sent,
                            &mut total_failed,
                        )
                        .await?;
                        docs_in_batch = 0;
                    }
                }
            }
            continue;
        }

        let line = build_csv_ndjson_line(&record, &headers, &id_detection, &id_header)?;
        batch_body.extend_from_slice(&line);
        docs_in_batch += 1;

        if docs_in_batch >= batch_size {
            flush_ndjson_batch(
                client,
                index,
                &mut batch_body,
                &mut total_sent,
                &mut total_failed,
            )
            .await?;
            docs_in_batch = 0;
        }
    }

    // Handle sources with fewer rows than SCHEMA_SAMPLE_LIMIT
    if !schema_created && !sample_rows.is_empty() {
        let mut schema = IndexSchema::default();
        if id_detection.is_shadow {
            let field_type = headers[id_detection.index]
                .1
                .clone()
                .unwrap_or(TantivyFieldType::Text);
            schema.add_shadow_field(id_detection.original_field_name.clone(), field_type);
        }
        for row in &sample_rows {
            let mut obj: JsonMap<String, JsonValue> = JsonMap::new();
            for (idx, value) in row.iter().enumerate() {
                if let Some((header, _)) = headers.get(idx) {
                    obj.insert(header.clone(), parse_csv_cell(value));
                }
            }
            if let Some(raw_id) = row.get(id_detection.index) {
                let id_val = raw_id.trim();
                if !id_val.is_empty() {
                    obj.insert("id".to_string(), JsonValue::String(id_val.to_string()));
                }
            }
            schema.evolve_from_document(&JsonValue::Object(obj));
        }
        let schema_json = serde_json::to_value(&schema).context("Failed to serialize schema")?;
        client
            .put_index_config(index, &schema_json)
            .await
            .with_context(|| format!("Failed to create schema for index '{}'", index))?;
        schema_created = true;

        for row in sample_rows.drain(..) {
            let line = build_csv_ndjson_line(&row, &headers, &id_detection, &id_header)?;
            batch_body.extend_from_slice(&line);
            docs_in_batch += 1;
            if docs_in_batch >= batch_size {
                flush_ndjson_batch(
                    client,
                    index,
                    &mut batch_body,
                    &mut total_sent,
                    &mut total_failed,
                )
                .await?;
                docs_in_batch = 0;
            }
        }
    }

    flush_ndjson_batch(
        client,
        index,
        &mut batch_body,
        &mut total_sent,
        &mut total_failed,
    )
    .await?;
    spinner.stop();

    if schema_created {
        println!(
            "Schema was missing; detected and applied schema to index '{}'",
            index
        );
    }
    println!(
        "Ingestion complete for index '{}': loaded={} failed={} (batch size {})",
        index, total_sent, total_failed, batch_size
    );
    Ok(())
}

fn parse_csv_cell(raw: &str) -> JsonValue {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return JsonValue::Null;
    }

    // Booleans
    match trimmed.to_ascii_lowercase().as_str() {
        "true" => return JsonValue::Bool(true),
        "false" => return JsonValue::Bool(false),
        _ => {}
    }

    // Integers (prefer unsigned for non-negative)
    if !trimmed.contains('.') && !trimmed.contains(['e', 'E']) {
        if trimmed.starts_with('-') {
            if let Ok(v) = trimmed.parse::<i64>() {
                return JsonValue::Number(v.into());
            }
        } else if let Ok(v) = trimmed.parse::<u64>() {
            return JsonValue::Number(serde_json::Number::from(v));
        } else if let Ok(v) = trimmed.parse::<i64>() {
            return JsonValue::Number(v.into());
        }
    }

    // Floating point
    if let Ok(v) = trimmed.parse::<f64>()
        && let Some(num) = serde_json::Number::from_f64(v)
    {
        return JsonValue::Number(num);
    }

    // Fallback to string (dates/IPs will be inferred from string content)
    JsonValue::String(trimmed.to_string())
}

async fn open_csv_source(client: &CameoClient, source: &str) -> Result<Box<dyn Read + Send>> {
    let compression = detect_compression(source);
    let is_http = source.starts_with("http://") || source.starts_with("https://");

    if is_http {
        let url = Url::parse(source).context("Invalid URL for CSV source")?;
        let raw_bytes = client
            .http()
            .get(url)
            .send()
            .await
            .context("Failed to fetch remote CSV")?
            .bytes()
            .await
            .context("Failed to read remote CSV body")?;
        let decompressed = decompress_bytes(raw_bytes.to_vec(), compression)?;
        Ok(Box::new(Cursor::new(decompressed)) as Box<dyn Read + Send>)
    } else {
        let path = Path::new(source);
        open_local_reader(path, compression)
    }
}

async fn open_csv_reader(
    client: &CameoClient,
    source: &str,
    delimiter: Delimiter,
) -> Result<csv::Reader<Box<dyn Read + Send>>> {
    let mut builder = ReaderBuilder::new();
    // Remote TSV samples (e.g., book summaries) sometimes contain stray delimiters;
    // allow variable-length records so schema detection doesn't abort early.
    builder.flexible(true);
    match delimiter {
        Delimiter::Detect => {
            let bytes = fetch_bytes_source(client, source).await?;
            // Detect delimiter on first line
            let first_line_end = bytes
                .iter()
                .position(|b| *b == b'\n')
                .unwrap_or(bytes.len());
            let first_line = &bytes[..first_line_end];
            let tab_count = first_line.iter().filter(|b| **b == b'\t').count();
            let comma_count = first_line.iter().filter(|b| **b == b',').count();
            let semi_count = first_line.iter().filter(|b| **b == b';').count();

            let detected = if semi_count >= tab_count && semi_count >= comma_count {
                b';'
            } else if tab_count >= comma_count {
                b'\t'
            } else {
                b','
            };
            builder.delimiter(detected);
            return Ok(builder.from_reader(Box::new(Cursor::new(bytes)) as Box<dyn Read + Send>));
        }
        Delimiter::Comma => {
            builder.delimiter(b',');
        }
        Delimiter::Tab => {
            builder.delimiter(b'\t');
        }
        Delimiter::Semicolon => {
            builder.delimiter(b';');
        }
    }
    // Re-open source since detect may have consumed none; builder will read fresh
    let reader_source = open_csv_source(client, source).await?;
    Ok(builder.from_reader(reader_source))
}

async fn fetch_bytes_source(client: &CameoClient, source: &str) -> Result<Vec<u8>> {
    let compression = detect_compression(source);
    let is_http = source.starts_with("http://") || source.starts_with("https://");

    let raw_bytes = if is_http {
        let url = Url::parse(source).context("Invalid URL for schema source")?;
        let bytes = client
            .http()
            .get(url)
            .send()
            .await
            .context("Failed to fetch remote schema")?
            .bytes()
            .await
            .context("Failed to read remote schema body")?;
        bytes.to_vec()
    } else {
        let path = Path::new(source);
        fs::read(path).with_context(|| format!("Failed to read schema file: {}", path.display()))?
    };

    decompress_bytes(raw_bytes, compression)
}

async fn run_interactive_shell(initial_url: String) -> Result<()> {
    println!(
        "🛠️  CameoDB interactive client. Type 'help' for supported commands, 'exit' to quit.\n"
    );

    let session = InteractiveSession::new(initial_url)?;
    let history_path = history_file_path()?;
    let handle = tokio::runtime::Handle::current();
    session.refresh_index_cache().await;

    tokio::task::spawn_blocking(move || interactive_loop(session, history_path, handle))
        .await
        .map_err(|e| anyhow!("Interactive shell join failed: {}", e))??;

    println!("Goodbye!");
    Ok(())
}

fn interactive_loop(
    mut session: InteractiveSession,
    history_path: PathBuf,
    handle: tokio::runtime::Handle,
) -> Result<()> {
    let completer = IndexCompleter::new(session.index_cache_handle());

    // Configure editor with platform-specific settings
    let config = if cfg!(target_os = "windows") {
        // Windows: use simpler config to avoid cursor positioning issues
        rustyline::Config::builder()
            .auto_add_history(true)
            .history_ignore_space(true)
            .completion_type(rustyline::CompletionType::List)
            .edit_mode(rustyline::EditMode::Emacs)
            .build()
    } else {
        // Unix/Linux/macOS: full featured config
        rustyline::Config::builder()
            .auto_add_history(true)
            .history_ignore_space(true)
            .completion_type(rustyline::CompletionType::List)
            .edit_mode(rustyline::EditMode::Emacs)
            .build()
    };

    let mut editor = Editor::with_config(config).context("Failed to initialize line editor")?;
    editor.set_helper(Some(completer));
    if history_path.exists() {
        let _ = editor.load_history(&history_path);
    }

    loop {
        let line = match editor.readline(&session.prompt()) {
            Ok(line) => line,
            Err(ReadlineError::Interrupted) => {
                println!();
                continue;
            }
            Err(ReadlineError::Eof) => break,
            Err(err) => return Err(anyhow!("Input error: {}", err)),
        };

        let input = line.trim().to_string();
        if input.is_empty() {
            continue;
        }

        if matches!(input.as_str(), "exit" | "quit" | "\\q") {
            break;
        }

        if matches!(input.as_str(), "help" | "\\h") {
            println!(
                "Available commands:\n  health\n  list indexes [--extended] [--data-size]\n  list index <name> [--extended] [--data-size]\n  search <index> <query> [limit]\n  schema detect <file> [--delimiter <delim>]\n  schema load <index> <file> [--delimiter <delim>]\n  data load <index> <file> [--delimiter <delim>] [--batch-size <n>]\n  delete <index> [--delete-schema]\n  admin memory stats\n  admin memory trim\n  admin index <name> commit\n  admin index <name> evict-writer\n  connect <host[:port]>\n  exit | quit | \\q\n\nSupported source formats for schema/data commands:\n  CSV, TSV, semicolon-delimited CSV, JSON object, JSON array, JSONL/NDJSON"
            );
            continue;
        }

        let _ = editor.add_history_entry(line.as_str());

        if let Err(err) = handle.block_on(dispatch_interactive_command(
            &mut session,
            &mut editor,
            &input,
        )) {
            eprintln!("⚠️  {}", err);
        }
    }

    editor
        .save_history(&history_path)
        .or_else(|_| editor.append_history(&history_path))
        .ok();

    Ok(())
}

fn history_file_path() -> Result<PathBuf> {
    let mut path = dirs::home_dir().context("Unable to determine home directory")?;
    path.push(".cameodb");
    fs::create_dir_all(&path).context("Failed to create CameoDB config directory")?;
    path.push("client_history");
    Ok(path)
}

async fn dispatch_interactive_command(
    session: &mut InteractiveSession,
    editor: &mut Editor<IndexCompleter, rustyline::history::DefaultHistory>,
    input: &str,
) -> Result<()> {
    let mut parts = input.split_whitespace();
    let command = parts.next().unwrap_or_default();

    match command {
        "health" => {
            let health = session.client().health().await?;
            print_json(&health)?;
        }
        "list" => {
            let resource = parts.next().unwrap_or("indexes");
            match resource {
                "indexes" => {
                    let remaining: Vec<&str> = parts.collect();
                    let extended = remaining.iter().any(|s| *s == "--extended" || *s == "-e");
                    let data_size = remaining.contains(&"--data-size");
                    if let Some(result) = handle_list_command(
                        session.client(),
                        ListResource::Indexes,
                        None,
                        data_size,
                        extended,
                    )
                    .await?
                    {
                        session.update_index_cache(&result).await;
                    }
                }
                "index" => {
                    let name = parts.next().ok_or_else(|| {
                        anyhow!("Usage: list index <name> [--extended] [--data-size]")
                    })?;
                    let remaining: Vec<&str> = parts.collect();
                    let extended = remaining.iter().any(|s| *s == "--extended" || *s == "-e");
                    let data_size = remaining.contains(&"--data-size");
                    if let Some(result) = handle_list_command(
                        session.client(),
                        ListResource::Index,
                        Some(name.to_string()),
                        data_size,
                        extended,
                    )
                    .await?
                    {
                        session.update_index_cache(&result).await;
                    }
                }
                "--extended" | "-e" => {
                    let remaining: Vec<&str> = parts.collect();
                    let data_size = remaining.contains(&"--data-size");
                    if let Some(result) = handle_list_command(
                        session.client(),
                        ListResource::Indexes,
                        None,
                        data_size,
                        true,
                    )
                    .await?
                    {
                        session.update_index_cache(&result).await;
                    }
                }
                other => {
                    return Err(anyhow!(
                        "Unknown list target '{}'. Use 'list indexes' or 'list index <name>'.",
                        other
                    ));
                }
            }
        }
        "search" => {
            let index = parts
                .next()
                .ok_or_else(|| anyhow!("Usage: search <index> <query> [limit N]"))?;
            let mut query_parts: Vec<&str> = parts.collect();

            // Preserve legacy behavior: detect trailing numeric limit with optional "limit" keyword
            let trailing_limit = if let Some(last) = query_parts.last().copied() {
                if let Ok(num) = last.parse::<usize>() {
                    query_parts.pop();
                    if query_parts.last().map(|s| s.eq_ignore_ascii_case("limit")) == Some(true) {
                        query_parts.pop();
                    }
                    Some(num)
                } else {
                    None
                }
            } else {
                None
            };

            let raw_query = query_parts.join(" ");
            if raw_query.trim().is_empty() {
                return Err(anyhow!("Usage: search <index> <query> [limit N]"));
            }

            let (clean_query, keyword_limit, keyword_fields) = parse_query_modifiers(&raw_query);
            if clean_query.is_empty() {
                return Err(anyhow!("Search query cannot be empty"));
            }

            let final_limit = trailing_limit.or(keyword_limit);
            let results = session
                .client()
                .search(index, &clean_query, final_limit, keyword_fields)
                .await?;
            print_json(&results)?;
        }
        "schema" => {
            let sub = parts
                .next()
                .ok_or_else(|| anyhow!("Usage: schema <detect|load> ..."))?;

            match sub {
                "detect" => {
                    let remaining: Vec<&str> = parts.collect();
                    let (delimiter, positional) = parse_delimiter_arg(&remaining)?;
                    let file = positional.first().copied().ok_or_else(|| {
                        anyhow!("Usage: schema detect <file> [--delimiter <delim>]")
                    })?;

                    let schema_json =
                        detect_schema_from_source(session.client(), file, delimiter).await?;
                    print_json(&schema_json)?;
                }
                "load" => {
                    let remaining: Vec<&str> = parts.collect();
                    let (delimiter, positional) = parse_delimiter_arg(&remaining)?;
                    let index = positional.first().copied().ok_or_else(|| {
                        anyhow!("Usage: schema load <index> <file> [--delimiter <delim>]")
                    })?;
                    let file = positional.get(1).copied().ok_or_else(|| {
                        anyhow!("Usage: schema load <index> <file> [--delimiter <delim>]")
                    })?;

                    let schema_json =
                        load_schema_from_source(session.client(), file, delimiter).await?;
                    session
                        .client()
                        .put_index_config(index, &schema_json)
                        .await?;
                    println!("Schema applied to index '{}'", index);
                    session.refresh_index_cache().await;
                }
                other => {
                    return Err(anyhow!(
                        "Unknown schema operation '{}'. Use 'schema detect' or 'schema load'.",
                        other
                    ));
                }
            }
        }
        "data" => {
            let sub = parts.next().ok_or_else(|| {
                anyhow!("Usage: data load <index> <file> [--delimiter <delim>] [--batch-size <n>]")
            })?;

            match sub {
                "load" => {
                    let remaining: Vec<&str> = parts.collect();
                    let (delimiter, positional_after_delim) = parse_delimiter_arg(&remaining)?;
                    let (batch_size, positional) =
                        parse_batch_size_arg(&positional_after_delim, DEFAULT_BATCH_SIZE)?;

                    let index = positional
                        .first()
                        .copied()
                        .ok_or_else(|| anyhow!("Usage: data load <index> <file> [--delimiter <delim>] [--batch-size <n>]"))?;
                    let file = positional
                        .get(1)
                        .copied()
                        .ok_or_else(|| anyhow!("Usage: data load <index> <file> [--delimiter <delim>] [--batch-size <n>]"))?;

                    load_data_from_source(session.client(), index, file, delimiter, batch_size)
                        .await?;
                }
                other => {
                    return Err(anyhow!(
                        "Unknown data operation '{}'. Use 'data load'.",
                        other
                    ));
                }
            }
        }
        "delete" => {
            let index = parts
                .next()
                .ok_or_else(|| anyhow!("Usage: delete <index> [--delete-schema]"))?;

            // Parse optional flag --delete-schema
            let delete_schema = parts.any(|p| p == "--delete-schema");

            // Use rustyline for confirmation to avoid stdin conflicts in interactive mode
            let prompt = format!("Delete index \"{}\"? [yes/NO]: ", index);
            match editor.readline(&prompt) {
                Ok(answer) => {
                    let confirmed = answer.trim().eq_ignore_ascii_case("yes");
                    if !confirmed {
                        println!("Aborted delete.");
                        return Ok(());
                    }
                }
                Err(ReadlineError::Interrupted | ReadlineError::Eof) => {
                    println!("\nAborted delete.");
                    return Ok(());
                }
                Err(err) => return Err(anyhow!("Failed to read confirmation: {}", err)),
            }

            let result = session.client().delete_index(index, delete_schema).await?;
            print_json(&result)?;
            session.refresh_index_cache().await;
        }
        "connect" | "conn" => {
            let target = parts.collect::<Vec<_>>().join(" ");
            if target.is_empty() {
                return Err(anyhow!("Usage: connect <host[:port]>"));
            }
            session.reconnect(&target)?;
            println!("Connected to {}", session.display_host());
            session.refresh_index_cache().await;
        }
        "admin" => {
            let subcommand = parts
                .next()
                .ok_or_else(|| anyhow!("Usage: admin <memory|index> ..."))?;
            match subcommand {
                "memory" => {
                    let op = parts
                        .next()
                        .ok_or_else(|| anyhow!("Usage: admin memory <stats|trim>"))?;
                    match op {
                        "stats" => {
                            let result = session.client().admin_memory_stats().await?;
                            print_json(&result)?;
                        }
                        "trim" => {
                            let result = session.client().admin_memory_trim().await?;
                            print_json(&result)?;
                        }
                        other => {
                            return Err(anyhow!(
                                "Unknown memory operation '{}'. Use 'stats' or 'trim'.",
                                other
                            ));
                        }
                    }
                }
                "index" => {
                    let index = parts.next().ok_or_else(|| {
                        anyhow!("Usage: admin index <name> <commit|evict-writer>")
                    })?;
                    let op = parts.next().ok_or_else(|| {
                        anyhow!("Usage: admin index <name> <commit|evict-writer>")
                    })?;
                    match op {
                        "commit" => {
                            let result = session.client().admin_index_commit(index).await?;
                            print_json(&result)?;
                        }
                        "evict-writer" => {
                            let result = session.client().admin_index_evict_writer(index).await?;
                            print_json(&result)?;
                        }
                        other => {
                            return Err(anyhow!(
                                "Unknown index operation '{}'. Use 'commit' or 'evict-writer'.",
                                other
                            ));
                        }
                    }
                }
                other => {
                    return Err(anyhow!(
                        "Unknown admin subcommand '{}'. Use 'memory' or 'index'.",
                        other
                    ));
                }
            }
        }
        other => {
            return Err(anyhow!(
                "Unknown command '{}'. Type 'help' for the supported commands.",
                other
            ));
        }
    }

    Ok(())
}
