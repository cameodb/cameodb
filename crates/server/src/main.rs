use anyhow::Result;
use std::path::PathBuf;
use std::sync::Arc;

use tracing_subscriber;

mod http_server;
mod node_orchestrator;

use http_server::{create_router, AppState};
use node_orchestrator::{NodeConfig, NodeOrchestrator, RouterActor};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing for logging
    tracing_subscriber::fmt::init();

    // Create node configuration
    // Use data/server when running from workspace root, or ./data/server when running locally
    let config = NodeConfig {
        storage_path: PathBuf::from("./data/server"),
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

    // Create shared state for HTTP server
    let orchestrator_arc = Arc::new(tokio::sync::RwLock::new(orchestrator));
    let router_actor = RouterActor::new(orchestrator_arc.clone());

    let app_state = AppState {
        router: router_actor,
        orchestrator: orchestrator_arc,
    };

    // Create HTTP server
    let app = create_router(app_state);
    let listener = tokio::net::TcpListener::bind("0.0.0.0:9480").await?;

    println!("🚀 CameoDB HTTP Server starting on http://0.0.0.0:9480");
    println!("📡 API endpoints:");
    println!("  POST /api/:index/search - Standard search");
    println!("  POST /api/:index/stream - Streaming search");
    println!("  PUT  /api/:index/document - Write document");
    println!("  GET  /_cluster/health - Health check");
    println!();
    println!("Press Ctrl+C to shutdown...");

    // Start the HTTP server
    let server_handle = tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).await {
            eprintln!("Server error: {}", e);
        }
    });

    // Wait for shutdown signal
    tokio::signal::ctrl_c().await?;
    println!("Shutting down...");

    server_handle.abort();
    Ok(())
}
