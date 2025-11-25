use anyhow::Result;
use std::sync::Arc;

use tracing_subscriber;

mod config;
mod distributed;
mod http_server;
mod node_orchestrator;

use config::CameoDbConfig;
use distributed::DistributedCluster;
use http_server::{AppState, create_router};
use node_orchestrator::{NodeConfig, NodeOrchestrator, ProposeShard, RouterActor};

#[tokio::main]
async fn main() -> Result<()> {
    // Handle CLI arguments for configuration utilities
    let args: Vec<String> = std::env::args().collect();
    if args.len() > 1 && args[1] == "generate-config" {
        println!("{}", CameoDbConfig::generate_sample_config()?);
        return Ok(());
    }

    // Load configuration from multiple sources
    let cameodb_config = CameoDbConfig::load()?;

    // Initialize tracing with configuration
    tracing_subscriber::fmt::init();

    // Create node configuration from loaded config
    let node_config = NodeConfig {
        storage_path: cameodb_config.storage.data_paths[0].clone(),
        max_shards: cameodb_config.server.node.max_shards,
        writer_memory_min_mb: cameodb_config.search.writer_memory_min_mb,
        writer_memory_max_mb: cameodb_config.search.writer_memory_max_mb,
        writer_memory_default_mb: cameodb_config.server.node.writer_memory_default_mb,
        wal_sync: cameodb_config.storage.wal_sync,
    };

    // Create the NodeOrchestrator
    let mut orchestrator = NodeOrchestrator::new(node_config).await?;

    // Initialize default shards on first boot if none exist
    let init_shards = cameodb_config.server.node.init_shards;
    if orchestrator.shard_count() == 0 && init_shards > 0 {
        for _ in 0..init_shards {
            let shard_id = uuid::Uuid::new_v4();
            if let Err(err) = orchestrator
                .handle_propose_shard(ProposeShard { shard_id })
                .await
            {
                tracing::warn!(%shard_id, %err, "Failed to create initial shard");
            }
        }
        println!("Initialized {} shards", init_shards);
    }

    println!("NodeOrchestrator started successfully");
    println!(
        "Node identity: {} ({})",
        orchestrator.identity().name,
        orchestrator.identity().uuid
    );
    println!("Active shards: {}", orchestrator.shard_count());

    // Initialize distributed cluster if enabled
    let mut distributed_cluster =
        DistributedCluster::new(cameodb_config.cluster.clone(), orchestrator.identity().uuid);

    match distributed_cluster.bootstrap().await {
        Err(err) => {
            tracing::warn!(%err, "Failed to bootstrap distributed cluster");
            println!("⚠️  Distributed cluster bootstrap failed, continuing in single-node mode");
        }
        _ => {
            let cluster_status = distributed_cluster.get_cluster_status();
            if cluster_status.distributed_enabled {
                println!("🌐 Distributed cluster initialized:");
                println!("  📡 Cluster: {}", cluster_status.cluster_name);
                println!("  🔗 Total nodes: {}", cluster_status.total_nodes);
                println!("  ✅ Connected: {}", cluster_status.connected_nodes);

                // Discover peers
                if let Ok(peers) = distributed_cluster.discover_peers().await
                    && !peers.is_empty()
                {
                    println!("  👥 Discovered {} peer nodes", peers.len());
                }
            }
        }
    }

    // Create shared state for HTTP server
    let orchestrator_arc = Arc::new(tokio::sync::RwLock::new(orchestrator));
    let router_actor = RouterActor::new(orchestrator_arc.clone());

    let app_state = AppState {
        router: router_actor,
        orchestrator: orchestrator_arc,
    };

    // Create the HTTP router with shared state
    let app = create_router(app_state);

    // Extract HTTP configuration
    let http_config = &cameodb_config.server.http;
    let bind_address = format!("{}:{}", http_config.host, http_config.port);

    // Print startup information
    println!("🚀 CameoDB HTTP Server starting on http://{}", bind_address);
    println!("🎯 API endpoints:");
    println!("  POST /api/:index/search - Standard search");
    println!("  POST /api/:index/stream - Streaming search");
    println!("  PUT  /api/:index/document - Write document");
    println!("  POST /api/:index/_bulk - Bulk write documents");
    println!("  PUT  /api/:index/_config - Create/update index schema");
    println!("  GET  /api/:index/_config - Retrieve index schema");
    println!("  GET  /_indexes - List all indexes with statistics");
    println!("  GET  /_cluster/health - Health check");
    println!();
    println!("⚙️  Configuration:");
    println!("  Data Paths: {:?}", cameodb_config.storage.data_paths);
    println!("  Max Shards: {}", cameodb_config.server.node.max_shards);
    println!(
        "  Writer Memory: {}-{}MB",
        cameodb_config.search.writer_memory_min_mb, cameodb_config.search.writer_memory_max_mb
    );
    println!(
        "  Total Memory Limit: {}MB",
        cameodb_config.search.total_memory_limit_mb
    );
    println!();
    println!("Press Ctrl+C to shutdown...");
    println!();

    // Start the HTTP server with configured address
    let listener = tokio::net::TcpListener::bind(&bind_address).await?;

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
