use crate::sdk::CameoClient;
use anyhow::Result;
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "cameodb-client", about = "CameoDB CLI Client")]
pub struct ClientCli {
    #[command(subcommand)]
    pub command: ClientCommand,

    /// CameoDB Server URL
    #[arg(short, long, default_value = "http://localhost:9480", global = true)]
    pub url: String,
}

#[derive(Subcommand)]
pub enum ClientCommand {
    /// Check cluster health
    Health,

    /// Manage indexes
    Index {
        #[command(subcommand)]
        command: IndexCommand,
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

#[derive(Subcommand)]
pub enum IndexCommand {
    /// List all indexes
    List,
}

pub async fn run_cli() -> Result<()> {
    // We need to skip the first argument if it's "client" because this is called
    // from the main binary as `cameodb client ...`
    // However, clap expects the program name as the first arg.
    // If we run `cameodb client`, we might want to parse from the second arg onwards.
    // Or we can just let the main binary handle the dispatch and pass the args here.

    // Simplest approach: Parse from env, but we might need to adjust if "client" is present.
    // If the user runs `cameodb client health`, argv is `["cameodb", "client", "health"]`.
    // If we define the parser here, we might need to handle the "client" subcommand matching
    // if we were strictly using clap for the whole binary.
    // But since `server` does manual parsing, we should probably strip "client" before calling parse,
    // or configure clap to ignore the first arg if it matches "client".

    // Actually, a cleaner way is to expect the caller to pass the args, or use `parse_from`.

    let args: Vec<String> = std::env::args().collect();
    // If called as `cameodb client ...`, we want to treat `cameodb client` as the "bin name" essentially,
    // or just filter out "client".

    let cli = if args.get(1).map(|s| s.as_str()) == Some("client") {
        ClientCli::parse_from(std::iter::once(args[0].clone()).chain(args.iter().skip(2).cloned()))
    } else {
        ClientCli::parse()
    };

    let client = CameoClient::new(&cli.url)?;

    match cli.command {
        ClientCommand::Health => {
            let health = client.health().await?;
            println!("{}", serde_json::to_string_pretty(&health)?);
        }
        ClientCommand::Index { command } => match command {
            IndexCommand::List => {
                let indexes = client.list_indexes().await?;
                println!("{}", serde_json::to_string_pretty(&indexes)?);
            }
        },
        ClientCommand::Search {
            index,
            query,
            limit,
        } => {
            let results = client.search(&index, &query, limit).await?;
            println!("{}", serde_json::to_string_pretty(&results)?);
        }
    }

    Ok(())
}
