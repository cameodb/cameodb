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
use distributed::{ClusterStatus, DistributedCluster};
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

    if args.len() > 1 {
        let wants_client = args
            .iter()
            .skip(1)
            .any(|arg| matches!(arg.as_str(), "client" | "health" | "index" | "search"));
        let interactive_requested = args
            .iter()
            .skip(1)
            .any(|arg| arg == "-i" || arg == "--interactive");

        if wants_client || interactive_requested {
            return client::run_cli().await;
        }

        if let Some(arg) = args.get(1).map(String::as_str) {
            match arg {
                "--version" | "-V" => {
                    println!("cameodb {}", env!("CARGO_PKG_VERSION"));
                    return Ok(());
                }
                "generate-config" => {
                    println!("{}", CameoDbConfig::generate_sample_config()?);
                    return Ok(());
                }
                "--help" | "-h" => {
                    println!(
                        "cameodb {}\n\n\
                         Usage:\n  \
                         cameodb [OPTIONS]\n  \
                         cameodb generate-config\n  \
                         cameodb client <subcommand>\n\n\
                         Options:\n  \
                         -h, --help       Show this help message\n  \
                         -V, --version    Show version information\n\n\
                         Commands:\n  \
                         generate-config  Print a sample configuration file\n  \
                         client           Run the bundled client CLI (health, index, search)\n\n\
                         Client examples:\n  \
                         cameodb client health\n  \
                         cameodb client index list\n  \
                         cameodb client search myindex \"foo bar\" --limit 5 --url http://host:9480\n\n\
                         When no command is provided, cameodb starts the server using configuration\n  \
                         loaded from config files and environment variables.",
                        env!("CARGO_PKG_VERSION")
                    );
                    return Ok(());
                }
                _ => {}
            }
        }
    }

    // Load configuration from multiple sources
    let cameodb_config = CameoDbConfig::load()?;

    // Initialize tracing with configuration
    tracing_subscriber::fmt::init();

    // Establish deterministic node identity from libp2p keypair
    let primary_path = cameodb_config
        .storage
        .primary_path()
        .cloned()
        .expect("storage.data_paths must contain at least one entry");

    let (_keypair, identity) =
        swarm::load_or_generate_keypair(&primary_path).expect("Failed to establish node identity");

    // Create node configuration from loaded config
    let node_config = NodeConfig {
        storage_path: primary_path.clone(),
        storage_paths: cameodb_config.storage.data_paths.clone(),
        max_shards: cameodb_config.storage.max_shards_per_node,
        indexer_memory_min_mb: cameodb_config.search.indexer_memory_min_mb,
        indexer_memory_max_mb: cameodb_config.search.indexer_memory_max_mb,
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

    // Capture node_id early for remote registration
    let node_id = orchestrator.identity().uuid;

    // Initialize cluster state store for persistent metadata
    let state_store = Arc::new(
        ClusterStateStore::new(primary_path.clone())
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
        primary_path.clone(),
    );

    // Create ClusterCoordinator but DON'T start swarm yet
    let coordinator = if let Some(persisted) = persisted_cluster {
        tracing::info!(
            "Restoring cluster from persisted state: {} nodes, {} shards expected",
            persisted.nodes.len(),
            persisted.shards.len()
        );
        ClusterCoordinator::new_with_persisted_state(
            distributed_cluster,
            persisted,
            state_store.clone(),
        )
    } else {
        tracing::info!("Fresh cluster boot, no persisted state");
        let mut coordinator = ClusterCoordinator::new(distributed_cluster);
        coordinator.set_state_store(state_store.clone());
        coordinator
    };

    // NOW spawn the coordinator actor
    let coordinator_actor = ClusterCoordinator::spawn(coordinator);

    // Set coordinator reference on orchestrator FIRST
    orchestrator.set_coordinator(coordinator_actor.clone());

    // NOW initialize default shards (after coordinator is set)
    let init_shards = cameodb_config.storage.num_shards_init;
    if orchestrator.shard_count() == 0 && init_shards > 0 {
        for _ in 0..init_shards {
            // Use balanced UUID generation for uniform distribution across data paths
            let shard_id = orchestrator.generate_balanced_shard_id();
            if let Err(err) = orchestrator
                .handle_propose_shard(ProposeShard { shard_id })
                .await
            {
                tracing::warn!(%shard_id, %err, "Failed to create initial shard");
            }
        }
        println!("Initialized {} shards", init_shards);
    }

    // Register all shards with coordinator (including newly created ones)
    if let Err(err) = orchestrator.register_all_shards_with_coordinator().await {
        tracing::warn!(error = %err, "Failed to register shards with coordinator");
    }

    println!("NodeOrchestrator started successfully");
    println!(
        "Node identity: {} ({})",
        orchestrator.identity().name,
        orchestrator.identity().uuid
    );
    if let Some(label) = cameodb_config.node.label.as_deref() {
        println!("Node label: {}", label);
    }
    println!("Active shards: {}", orchestrator.shard_count());

    // NOW spawn the NodeOrchestrator as an actor (after all setup is done)
    let orchestrator_ref = NodeOrchestrator::spawn(orchestrator);
    let remote_name = orchestrator_remote_name(&node_id);

    // Set orchestrator reference on coordinator for coordinated operations
    let _ = coordinator_actor
        .ask(crate::cluster_coordinator::SetLocalOrchestrator {
            orchestrator: orchestrator_ref.clone(),
        })
        .await;

    // NOW initialize swarm via actor message (after shards are registered)
    let (swarm_initialized, cluster_enabled) = match coordinator_actor.ask(InitSwarm).await {
        Err(err) => {
            tracing::warn!(error = ?err, "Failed to initialize distributed swarm");
            println!("⚠️  Distributed swarm initialization failed, continuing in single-node mode");
            (false, false)
        }
        Ok(peer_id) => {
            let peer_id: String = peer_id;
            // Get cluster status via actor
            let status_result: Result<ClusterStatus, _> = coordinator_actor.ask(GetStatus).await;
            if let Ok(cluster_status) = status_result {
                if cluster_status.cluster_enabled {
                    println!("🌐 Distributed swarm initialized:");
                    println!("  📡 Cluster: {}", cluster_status.cluster_name);
                    println!("  🆔 Peer ID: {}", peer_id);
                    println!("  🔗 Total nodes: {}", cluster_status.total_nodes);
                    println!("  ✅ Connected: {}", cluster_status.connected_nodes);

                    // Discover peers via actor
                    let discover_result: Result<Vec<crate::distributed::NodeInfo>, _> =
                        coordinator_actor.ask(DiscoverPeers).await;
                    if let Ok(peers) = discover_result
                        && !peers.is_empty()
                    {
                        println!("  👥 Discovered {} peer nodes", peers.len());
                    }
                } else {
                    println!("🏠 Running in single-node mode (cluster disabled)");
                }
                (true, cluster_status.cluster_enabled)
            } else {
                // If we can't get status, assume cluster is disabled for safety
                println!("🏠 Running in single-node mode (cluster status unknown)");
                (true, false)
            }
        }
    };

    // Register coordinator for remote access AFTER swarm is initialized
    let coordinator_remote_name = format!("coordinator-{}", node_id);
    if swarm_initialized && cluster_enabled {
        if let Err(e) = coordinator_actor
            .register(coordinator_remote_name.clone())
            .await
        {
            tracing::warn!(name = %coordinator_remote_name, error = %e, "Failed to register coordinator for remote access");
        } else {
            tracing::info!(name = %coordinator_remote_name, "Registered coordinator for remote access");
        }
    } else if !cluster_enabled {
        tracing::info!("Cluster disabled, skipping coordinator remote registration");
    } else {
        tracing::warn!("Swarm not initialized, skipping coordinator remote registration");
    }

    // Register orchestrator for remote access ONLY after swarm is initialized
    if swarm_initialized && cluster_enabled {
        if let Err(e) = orchestrator_ref.register(remote_name.clone()).await {
            tracing::warn!(name = %remote_name, error = %e, "Failed to register orchestrator for remote access");
        } else {
            tracing::info!(name = %remote_name, "Registered orchestrator for remote access");
        }
    } else if !cluster_enabled {
        tracing::info!("Cluster disabled, skipping orchestrator remote registration");
    } else {
        tracing::warn!("Swarm not initialized, skipping orchestrator remote registration");
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
        &cameodb_config.search,
        cameodb_config.search.default_search_limit,
    );

    let app_state = AppState {
        router: router_actor,
        coordinator: coordinator_actor.clone(),
    };

    // Create the HTTP router with shared state and configured body limit
    let app = create_router(app_state, cameodb_config.network.http.max_body_size_mb);

    // Extract HTTP configuration
    let http_config = &cameodb_config.network.http;
    let bind_address = format!("{}:{}", http_config.bind_address, http_config.port);

    // Print startup information
    println!("🚀 CameoDB HTTP Server starting on http://{}", bind_address);
    println!("🎯 API endpoints:");
    println!("  POST /api/{{index}}/search - Standard search");
    println!("  POST /api/{{index}}/search/stream - Streaming search");
    println!("  PUT  /api/{{index}}/document - Write document");
    println!("  POST /api/{{index}}/document/stream - Streaming write");
    println!("  POST /api/{{index}}/_bulk - Bulk write documents");
    println!("  PUT  /api/{{index}}/_config - Create/update index schema");
    println!("  GET  /api/{{index}}/_config - Retrieve index schema");
    println!("  PATCH /api/{{index}}/_schema - Update index schema");
    println!("  DELETE /api/{{index}} - Delete index (?delete_schema=true/false)");
    println!("  GET  /_indexes - List all indexes with statistics");
    println!("  GET  /_cluster/_indexes - List cluster indexes");
    println!("  GET  /_cluster/health - Health check");
    println!();
    println!("⚙️  Configuration:");
    println!("  Data Paths: {:?}", cameodb_config.storage.data_paths);
    println!(
        "  Number of shards: {}",
        cameodb_config.storage.num_shards_init
    );
    let durability_label = if cameodb_config.storage.wal_sync {
        "Immediate"
    } else {
        "Eventual"
    };
    println!("  Durability: {}", durability_label);
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

    // Shutdown all shards gracefully to commit pending writes
    tracing::info!("Shutting down all shards...");
    if let Err(e) = orchestrator_ref
        .ask(crate::node_orchestrator::ShutdownAllShards)
        .await
    {
        tracing::error!(error = %e, "Failed to shutdown shards gracefully");
    }

    // Signal coordinator to shutdown swarm gracefully
    let _ = coordinator_actor.ask(ShutdownSwarm).await;

    server_handle.abort();
    Ok(())
}
