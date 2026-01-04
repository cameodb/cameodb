use anyhow::Result;
use kameo::actor::Spawn;

mod cluster_coordinator;
mod cluster_state;
mod cluster_state_machine;
mod config;
mod distributed;
mod http_server;
mod node_orchestrator;
mod swarm;

use cluster_coordinator::{
    ClusterCoordinator, DiscoverPeers, GetStatus, InitSwarm, ShutdownSwarm, SubscribeTopology,
};
use cluster_state::ClusterStateStore;
use config::CameoDbConfig;
use distributed::DistributedCluster;
use http_server::{AppState, create_router};
use node_orchestrator::{
    NodeConfig, NodeOrchestrator, ProposeShard, RouterActor, UpdateTopology,
    orchestrator_remote_name,
};
use std::sync::Arc;
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> Result<()> {
    // Handle CLI arguments for configuration utilities and client mode
    let args: Vec<String> = std::env::args().collect();
    if let Some(arg) = args.get(1).map(String::as_str) {
        match arg {
            "client" => {
                return client::run_cli().await;
            }
            "--version" | "-V" => {
                println!("cameodb {}", env!("CARGO_PKG_VERSION"));
                return Ok(());
            }
            "generate-config" => {
                println!("{}", CameoDbConfig::generate_sample_config()?);
                return Ok(());
            }
            _ => {}
        }
    }

    // Load configuration from multiple sources
    let cameodb_config = CameoDbConfig::load()?;

    // Initialize tracing with configuration
    tracing_subscriber::fmt::init();

    // Establish deterministic node identity from libp2p keypair
    let (_keypair, identity) =
        swarm::load_or_generate_keypair(&cameodb_config.storage.data_paths[0])
            .expect("Failed to establish node identity");

    // Create node configuration from loaded config
    let node_config = NodeConfig {
        storage_path: cameodb_config.storage.data_paths[0].clone(),
        max_shards: cameodb_config.storage.max_shards_per_node,
        writer_memory_min_mb: cameodb_config.search.indexer_memory_min_mb,
        writer_memory_max_mb: cameodb_config.search.indexer_memory_max_mb,
        wal_sync: cameodb_config.storage.wal_sync,
        default_batch_size: cameodb_config.storage.default_batch_size,
    };

    // Create the NodeOrchestrator actor
    let mut orchestrator = NodeOrchestrator::new(
        node_config,
        identity,
        cameodb_config.search.default_search_limit,
    )
    .await?;

    // Initialize default shards on first boot if none exist
    let init_shards = cameodb_config.storage.num_shards_init;
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

    // Capture node_id early for remote registration
    let node_id = orchestrator.identity().uuid;

    // Initialize cluster state store for persistent metadata
    let state_store = Arc::new(
        ClusterStateStore::new(cameodb_config.storage.data_paths[0].clone())
            .expect("Failed to initialize cluster state store"),
    );

    // Load persisted cluster topology (if exists)
    let persisted_cluster = state_store
        .load_persisted_cluster()
        .expect("Failed to load persisted cluster state");

    // Initialize distributed cluster
    let distributed_cluster = DistributedCluster::new(
        cameodb_config.network.cluster.clone(),
        node_id,
        orchestrator.identity().name.clone(),
        cameodb_config.storage.data_paths[0].clone(),
    );

    let coordinator_actor = if let Some(persisted) = persisted_cluster {
        tracing::info!(
            "Restoring cluster from persisted state: {} nodes, {} shards expected",
            persisted.nodes.len(),
            persisted.shards.len()
        );
        ClusterCoordinator::spawn(ClusterCoordinator::new_with_persisted_state(
            distributed_cluster,
            persisted,
            state_store.clone(),
        ))
    } else {
        tracing::info!("Fresh cluster boot, no persisted state");
        let mut coordinator = ClusterCoordinator::new(distributed_cluster);
        coordinator.set_state_store(state_store.clone());
        ClusterCoordinator::spawn(coordinator)
    };

    // Initialize swarm via actor message (MUST be done before actor registration)
    match coordinator_actor.ask(InitSwarm).await {
        Err(err) => {
            tracing::warn!(error = ?err, "Failed to initialize distributed swarm");
            println!("⚠️  Distributed swarm initialization failed, continuing in single-node mode");
        }
        Ok(peer_id) => {
            // Get cluster status via actor
            if let Ok(cluster_status) = coordinator_actor.ask(GetStatus).await
                && cluster_status.distributed_enabled
            {
                println!("🌐 Distributed swarm initialized:");
                println!("  📡 Cluster: {}", cluster_status.cluster_name);
                println!("  🆔 Peer ID: {}", peer_id);
                println!("  🔗 Total nodes: {}", cluster_status.total_nodes);
                println!("  ✅ Connected: {}", cluster_status.connected_nodes);

                // Discover peers via actor
                if let Ok(peers) = coordinator_actor.ask(DiscoverPeers).await
                    && !peers.is_empty()
                {
                    println!("  👥 Discovered {} peer nodes", peers.len());
                }
            }
        }
    }

    // Register coordinator for remote access so peers can query shard metadata
    let coordinator_remote_name = format!("coordinator-{}", node_id);
    if let Err(e) = coordinator_actor
        .register(coordinator_remote_name.clone())
        .await
    {
        tracing::warn!(name = %coordinator_remote_name, error = %e, "Failed to register coordinator for remote access");
    } else {
        tracing::info!(name = %coordinator_remote_name, "Registered coordinator for remote access");
    }

    // Give orchestrator a handle to the coordinator for shard registration before spawning it.
    orchestrator.set_coordinator(coordinator_actor.clone());
    // Register any hydrated shards with the coordinator (no-op if none).
    orchestrator
        .register_all_shards_with_coordinator()
        .await
        .ok();

    // Spawn NodeOrchestrator as an actor and register with remote registry
    let orchestrator_ref = NodeOrchestrator::spawn(orchestrator);
    let remote_name = orchestrator_remote_name(&node_id);
    if let Err(e) = orchestrator_ref.register(remote_name.clone()).await {
        tracing::warn!(name = %remote_name, error = %e, "Failed to register orchestrator for remote access");
    } else {
        tracing::info!(name = %remote_name, "Registered orchestrator for remote access");
    }

    // Subscribe orchestrator to cluster topology updates to maintain global routing awareness
    let (ring_tx, mut ring_rx) = mpsc::channel(16);
    if let Err(e) = coordinator_actor
        .tell(SubscribeTopology {
            subscriber: ring_tx,
        })
        .await
    {
        tracing::warn!(error = %e, "Failed to subscribe orchestrator to topology updates");
    }

    // Spawn task to forward topology updates from coordinator to orchestrator
    let orchestrator_for_updates = orchestrator_ref.clone();
    tokio::spawn(async move {
        while let Some(ring) = ring_rx.recv().await {
            if let Err(e) = orchestrator_for_updates.tell(UpdateTopology { ring }).await {
                tracing::warn!(error = %e, "Failed to forward topology update to orchestrator");
            }
        }
    });

    // Create RouterActor with ActorRefs and messaging config
    let router_actor = RouterActor::with_config(
        orchestrator_ref.clone(),
        coordinator_actor.clone(),
        &cameodb_config.network.cluster.messaging,
        cameodb_config.search.default_search_limit,
    );

    let app_state = AppState {
        router: router_actor,
        coordinator: coordinator_actor.clone(),
    };

    // Create the HTTP router with shared state
    let app = create_router(app_state);

    // Extract HTTP configuration
    let http_config = &cameodb_config.network.http;
    let bind_address = format!("{}:{}", http_config.bind_address, http_config.port);

    // Print startup information
    println!("🚀 CameoDB HTTP Server starting on http://{}", bind_address);
    println!("🎯 API endpoints:");
    println!("  POST /api/{{index}}/search - Standard search");
    println!("  POST /api/{{index}}/stream - Streaming search");
    println!("  PUT  /api/{{index}}/document - Write document");
    println!("  POST /api/{{index}}/_bulk - Bulk write documents");
    println!("  PUT  /api/{{index}}/_config - Create/update index schema");
    println!("  GET  /api/{{index}}/_config - Retrieve index schema");
    println!("  PATCH /api/{{index}}/_schema - Update index schema");
    println!("  GET  /_indexes - List all indexes with statistics");
    println!("  GET  /_cluster/health - Health check");
    println!();
    println!("⚙️  Configuration:");
    println!("  Data Paths: {:?}", cameodb_config.storage.data_paths);
    println!(
        "  Max Shards: {}",
        cameodb_config.storage.max_shards_per_node
    );
    println!(
        "  Indexer Memory: {}-{}MB",
        cameodb_config.search.indexer_memory_min_mb, cameodb_config.search.indexer_memory_max_mb
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

    // Start the HTTP server (with connect info for client addr extraction)
    let server_handle = tokio::spawn(async move {
        if let Err(e) = axum::serve(
            listener,
            app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await
        {
            eprintln!("Server error: {}", e);
        }
    });

    // Wait for shutdown signal
    tokio::signal::ctrl_c().await?;
    println!("Shutting down...");

    // Signal coordinator to shutdown swarm gracefully
    let _ = coordinator_actor.ask(ShutdownSwarm).await;

    server_handle.abort();
    Ok(())
}
