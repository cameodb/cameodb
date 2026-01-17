use crate::sdk::CameoClient;
use anyhow::{Result, anyhow};
use clap::{Parser, Subcommand, ValueEnum};
use reqwest::Url;
use serde_json::{json, to_string_pretty};
use std::io::Write;
use tokio::io::{self, AsyncBufReadExt, BufReader};

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

#[derive(Debug)]
struct InteractiveSession {
    current_url: String,
    client: CameoClient,
}

impl InteractiveSession {
    fn new(initial_url: String) -> Result<Self> {
        let client = CameoClient::new(&initial_url)?;
        Ok(Self {
            current_url: initial_url,
            client,
        })
    }

    fn reconnect(&mut self, target: &str) -> Result<()> {
        let normalized = normalize_connect_target(target)?;
        self.client = CameoClient::new(&normalized)?;
        self.current_url = normalized;
        Ok(())
    }

    fn client(&self) -> &CameoClient {
        &self.client
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
) -> Result<()> {
    match resource {
        ListResource::Indexes => {
            let indexes = client.list_indexes(include_data_size).await?;
            println!("{}", to_string_pretty(&indexes)?);
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
        }
    }

    Ok(())
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

    let mut session = InteractiveSession::new(initial_url)?;
    let stdin = io::stdin();
    let mut reader = BufReader::new(stdin);
    let mut line = String::new();

    loop {
        print!("{}", session.prompt());
        std::io::stdout().flush().ok();

        line.clear();
        let bytes = reader.read_line(&mut line).await?;
        if bytes == 0 {
            println!();
            break;
        }

        let input = line.trim();
        if input.is_empty() {
            continue;
        }

        if matches!(input, "exit" | "quit" | "\\q") {
            break;
        }

        if matches!(input, "help" | "\\h") {
            println!(
                "Available commands:\n  health\n  list indexes\n  list index <name>\n  search <index> <query>\n  connect <host[:port]>\n  exit | quit | \\q"
            );
            continue;
        }

        if let Err(err) = dispatch_interactive_command(&mut session, input).await {
            eprintln!("⚠️  {}", err);
        }
    }

    println!("Goodbye!");
    Ok(())
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
                    handle_list_command(session.client(), ListResource::Indexes, None, true)
                        .await?;
                }
                "index" => {
                    let name = parts
                        .next()
                        .ok_or_else(|| anyhow!("Usage: list index <name>"))?;
                    handle_list_command(
                        session.client(),
                        ListResource::Index,
                        Some(name.to_string()),
                        true,
                    )
                    .await?;
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
                .ok_or_else(|| anyhow!("Usage: search <index> <query>"))?;
            let query = parts.collect::<Vec<_>>().join(" ");
            if query.is_empty() {
                return Err(anyhow!("Usage: search <index> <query>"));
            }
            let results = session.client().search(index, &query, None).await?;
            println!("{}", to_string_pretty(&results)?);
        }
        "connect" | "conn" => {
            let target = parts.collect::<Vec<_>>().join(" ");
            if target.is_empty() {
                return Err(anyhow!("Usage: connect <host[:port]>"));
            }
            session.reconnect(&target)?;
            println!("Connected to {}", session.display_host());
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
