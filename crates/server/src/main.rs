use anyhow::Result;
use std::path::PathBuf;

use tracing_subscriber;

mod node_orchestrator;
use node_orchestrator::{NodeConfig, NodeOrchestrator};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing for logging
    tracing_subscriber::fmt::init();

    // Create node configuration
    let config = NodeConfig {
        storage_path: PathBuf::from("./data/node"),
        max_shards: 10,
        shard_memory_budget: 50 * 1024 * 1024, // 50MB per shard
    };

    // Create the NodeOrchestrator
    let orchestrator = NodeOrchestrator::new(config).await?;

    println!("NodeOrchestrator started successfully");
    println!(
        "Node identity: {} ({})",
        orchestrator.identity().name,
        orchestrator.identity().uuid
    );
    println!("Active shards: {}", orchestrator.shard_count());

    // Keep the server running
    tokio::signal::ctrl_c().await?;
    println!("Shutting down...");

    Ok(())
}
