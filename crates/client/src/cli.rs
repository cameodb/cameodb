use crate::sdk::{CameoClient, ListIndexesResponse};
use anyhow::{Context, Result, anyhow};
use clap::{Parser, Subcommand, ValueEnum};
use reqwest::Url;
use rustyline::completion::{Completer, Pair};
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::validate::{ValidationContext, ValidationResult, Validator};
use rustyline::{Editor, Helper, error::ReadlineError};
use serde_json::Value as JsonValue;
use serde_json::{json, to_string_pretty};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

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
        format!("cameodb@{} ▶ ", self.display_host())
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
            println!("{}", to_string_pretty(&indexes)?);
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
            println!("{}", to_string_pretty(&enriched)?);
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
            "health", "list", "search", "connect", "conn", "exit", "quit", "help",
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

    fn complete_tokens(&self, tokens: &[&str], current: &str) -> Option<(usize, Vec<Pair>)> {
        if tokens.is_empty() {
            return None;
        }

        match tokens[0] {
            // Complete main commands when first token or partial command
            cmd if tokens.len() == 1 => {
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
            println!("{}", to_string_pretty(&health)?);
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
            println!("{}", to_string_pretty(&results)?);
        }
    }

    Ok(())
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
    let mut editor = Editor::new().context("Failed to initialize line editor")?;
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
                "Available commands:\n  health\n  list indexes\n  list index <name>\n  search <index> <query> [limit]\n  connect <host[:port]>\n  exit | quit | \\q"
            );
            continue;
        }

        let _ = editor.add_history_entry(line.as_str());

        if let Err(err) = handle.block_on(dispatch_interactive_command(&mut session, &input)) {
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

async fn dispatch_interactive_command(session: &mut InteractiveSession, input: &str) -> Result<()> {
    let mut parts = input.split_whitespace();
    let command = parts.next().unwrap_or_default();

    match command {
        "health" => {
            let health = session.client().health().await?;
            println!("{}", to_string_pretty(&health)?);
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
                .ok_or_else(|| anyhow!("Usage: search <index> <query> [limit]"))?;
            let mut query_parts: Vec<&str> = parts.collect();
            let limit = if let Some(last) = query_parts.last() {
                if let Ok(num) = last.parse::<usize>() {
                    query_parts.pop();
                    Some(num)
                } else {
                    None
                }
            } else {
                None
            };
            let query = query_parts.join(" ");
            if query.is_empty() {
                return Err(anyhow!("Usage: search <index> <query> [limit]"));
            }
            let results = session.client().search(index, &query, limit).await?;
            println!("{}", to_string_pretty(&results)?);
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
