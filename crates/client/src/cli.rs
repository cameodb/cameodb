use crate::sdk::{CameoClient, ListIndexesResponse};
use anyhow::{Context, Result, anyhow};
use clap::{Parser, Subcommand, ValueEnum};
use csv::ReaderBuilder;
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
use std::collections::HashMap;
use std::fs;
use std::io::{Cursor, Read};
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
        if let Ok(indexes) = self.client.list_indexes(true).await {
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
) -> Result<Option<ListIndexesResponse>> {
    match resource {
        ListResource::Indexes => {
            let indexes = client.list_indexes(include_data_size).await?;
            print_json(&indexes)?;
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
            let mut stats = serde_json::to_value(info)?;
            if let serde_json::Value::Object(ref mut map) = stats {
                map.remove("field_names");
            }
            let enriched = json!({
                "index": info.name,
                "stats": stats,
                "schema": config,
            });
            print_json(&enriched)?;
            Ok(Some(indexes))
        }
    }
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
            "health", "list", "search", "schema", "data", "delete", "connect", "conn", "exit",
            "quit", "help",
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
            "list" if tokens.len() >= 2 && tokens[1] == "index" => {
                let suggestions = self.index_suggestions(current);
                let start = current_start(tokens, current);
                Some((start, suggestions))
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
            "search" => {
                let index = parts.next()?;
                let tail = parts.collect::<Vec<_>>();
                let current = tail.last()?;
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
                None
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
        /// Target index name (required for `load`, optional for `detect`)
        index: Option<String>,
        /// Path or HTTP(S) URL to CSV/TSV file to parse
        file: String,
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
        /// Path or HTTP(S) URL to CSV/TSV data file
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
}

#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum SchemaOperation {
    /// Detect schema from a CSV file (samples first 200 rows)
    Detect,
    /// Detect schema and apply it to an index
    Load,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum DataOperation {
    /// Load CSV data into an index in batches
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
        ClientCommand::List { resource, name } => {
            handle_list_command(&client, resource, name, true).await?;
        }
        ClientCommand::Search {
            index,
            query,
            limit,
        } => {
            let results = client.search(&index, &query, limit).await?;
            print_json(&results)?;
        }
        ClientCommand::Schema {
            operation,
            index,
            file,
            delimiter,
        } => match operation {
            SchemaOperation::Detect => {
                let schema_json = detect_schema_from_csv(&client, &file, delimiter).await?;
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
                load_data_from_csv(&client, &index, &file, delimiter, batch_size).await?;
            }
        },
        ClientCommand::Delete {
            index,
            delete_schema,
        } => {
            let result = client.delete_index(&index, delete_schema).await?;
            print_json(&result)?;
        }
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

    // Add shadow field if needed (before processing records)
    if id_detection.is_shadow {
        let field_type = headers[id_detection.index]
            .1
            .clone()
            .unwrap_or(TantivyFieldType::Text);
        schema.add_shadow_field(id_detection.original_field_name.clone(), field_type);
    }

    for record in reader.records() {
        let record = record.context("Failed to read CSV record")?;
        let mut obj: JsonMap<String, JsonValue> = JsonMap::new();

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

async fn load_schema_from_source(
    client: &CameoClient,
    source: &str,
    delimiter: Delimiter,
) -> Result<JsonValue> {
    // OPTIMIZATION: Start spinner immediately - we know schema processing will happen
    let mut spinner = ProgressSpinner::new();

    // Try to parse as JSON schema first
    let raw_bytes = fetch_bytes_source(client, source).await?;

    if let Ok(json) = serde_json::from_slice::<JsonValue>(&raw_bytes)
        && json.get("fields").is_some()
    {
        spinner.stop();
        return Ok(json);
    }

    // Fallback: treat as CSV and detect
    let result = detect_schema_from_csv(client, source, delimiter).await;
    spinner.stop();
    result
}

async fn load_data_from_csv(
    client: &CameoClient,
    index: &str,
    source: &str,
    delimiter: Delimiter,
    batch_size: usize,
) -> Result<()> {
    // Ensure index/schema exists before ingesting; if missing, auto-create from source
    let schema_exists = client.get_index_config(index).await.is_ok();
    if !schema_exists {
        let schema = load_schema_from_source(client, source, delimiter)
            .await
            .context("Failed to detect schema while auto-creating index schema")?;
        client
            .put_index_config(index, &schema)
            .await
            .with_context(|| format!("Failed to create schema for index '{}'", index))?;
        println!(
            "Schema was missing; detected and applied schema to index '{}' before ingest",
            index
        );
    }

    // OPTIMIZATION: Start spinner immediately after schema check, before any file processing
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

    let mut batch: Vec<JsonValue> = Vec::with_capacity(batch_size);
    let mut total_sent = 0usize;

    for record in reader.records() {
        let record = record.context("Failed to read CSV record")?;
        let mut doc_obj: JsonMap<String, JsonValue> = JsonMap::new();

        let id_value_raw = record.get(id_detection.index).unwrap_or_default();
        if id_value_raw.trim().is_empty() {
            // Skip rows without an id; CameoDB requires an id field
            continue;
        }
        let id_value = id_value_raw.trim().to_string();

        for (idx, value) in record.iter().enumerate() {
            if let Some((header, _)) = headers.get(idx) {
                doc_obj.insert(header.clone(), parse_csv_cell(value));
            }
        }

        // Ensure the canonical id field is present in the document body as well
        doc_obj.insert("id".to_string(), JsonValue::String(id_value.clone()));

        let routing_key = doc_obj
            .get(&id_header)
            .and_then(|v| v.as_str())
            .map(str::to_string)
            .unwrap_or_else(|| id_value.clone());

        let payload = json!({
            "id": id_value,
            "routing_key": routing_key,
            "doc": doc_obj,
        });

        batch.push(payload);

        if batch.len() >= batch_size {
            client.bulk_index(index, &batch).await?;
            total_sent += batch.len();
            batch.clear();
        }
    }

    if !batch.is_empty() {
        client.bulk_index(index, &batch).await?;
        total_sent += batch.len();
    }

    // Stop the progress spinner
    spinner.stop();

    println!(
        "Loaded {} documents into index '{}' (batch size {})",
        total_sent, index, batch_size
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
    let is_http = source.starts_with("http://") || source.starts_with("https://");

    if is_http {
        let url = Url::parse(source).context("Invalid URL for CSV source")?;
        let bytes = client
            .http()
            .get(url)
            .send()
            .await
            .context("Failed to fetch remote CSV")?
            .bytes()
            .await
            .context("Failed to read remote CSV body")?;
        Ok(Box::new(Cursor::new(bytes.to_vec())) as Box<dyn Read + Send>)
    } else {
        let path = Path::new(source);
        let file = std::fs::File::open(path)
            .with_context(|| format!("Failed to open CSV file: {}", path.display()))?;
        Ok(Box::new(file) as Box<dyn Read + Send>)
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
    let is_http = source.starts_with("http://") || source.starts_with("https://");

    if is_http {
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
        Ok(bytes.to_vec())
    } else {
        let path = Path::new(source);
        fs::read(path).with_context(|| format!("Failed to read schema file: {}", path.display()))
    }
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
                "Available commands:\n  health\n  list indexes\n  list index <name>\n  search <index> <query> [limit]\n  schema detect <file> [--delimiter <delim>]\n  schema load <index> <file> [--delimiter <delim>]\n  data load <index> <file> [--delimiter <delim>] [--batch-size <n>]\n  delete <index> [--delete-schema]\n  connect <host[:port]>\n  exit | quit | \\q"
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
                    if let Some(result) =
                        handle_list_command(session.client(), ListResource::Indexes, None, true)
                            .await?
                    {
                        session.update_index_cache(&result).await;
                    }
                }
                "index" => {
                    let name = parts
                        .next()
                        .ok_or_else(|| anyhow!("Usage: list index <name>"))?;
                    if let Some(result) = handle_list_command(
                        session.client(),
                        ListResource::Index,
                        Some(name.to_string()),
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
            // Parse optional limit: either "limit N" or just "N" at the end
            let limit = if let Some(last) = query_parts.last() {
                if let Ok(num) = last.parse::<usize>() {
                    query_parts.pop();
                    // Also strip "limit" keyword if present before the number
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
            let query = query_parts.join(" ");
            if query.is_empty() {
                return Err(anyhow!("Usage: search <index> <query> [limit N]"));
            }
            let results = session.client().search(index, &query, limit).await?;
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
                        detect_schema_from_csv(session.client(), file, delimiter).await?;
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

                    load_data_from_csv(session.client(), index, file, delimiter, batch_size)
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
        other => {
            return Err(anyhow!(
                "Unknown command '{}'. Type 'help' for the supported commands.",
                other
            ));
        }
    }

    Ok(())
}
