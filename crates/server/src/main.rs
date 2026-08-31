#[cfg(target_os = "linux")]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use anyhow::Result;
use kameo::actor::Spawn;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::time::timeout;

mod admin;
mod audit;
mod auth;
mod authz;
mod cluster_coordinator;
mod cluster_state;
mod cluster_state_machine;
mod config;
mod distributed;
mod http_server;
mod mcp;
mod node_orchestrator;
mod posture;
mod query;
mod ratelimit;
mod remote_peer_pool;
mod state;
mod swarm;

use cluster_coordinator::{
    ClusterCoordinator, DiscoverPeers, GetStatus, InitSwarm, ShutdownSwarm, SubscribeTopology,
};
use cluster_state::ClusterStateStore;
use config::CameoDbConfig;
use distributed::{ClusterStatus, DistributedCluster};
use http_server::create_router;
use node_orchestrator::{
    NodeConfig, NodeOrchestrator, ProposeShard, RouterActor, ShardAffineConfig,
    StreamingSearchConfig, UpdateTopology, orchestrator_remote_name,
};
use remote_peer_pool::RemotePeerPool;
use state::AppState;
use tokio::sync::mpsc;

/// Global shutdown flag to prevent double-shutdown issues.
/// Set to true when shutdown begins, checked by signal handlers.
static SHUTDOWN_IN_PROGRESS: AtomicBool = AtomicBool::new(false);

/// Maximum time to wait for HTTP server to drain connections.
const HTTP_DRAIN_TIMEOUT_SECS: u64 = 10;

/// Maximum time to wait for all shards to shutdown.
const SHARD_SHUTDOWN_TIMEOUT_SECS: u64 = 60;

/// Maximum time to wait for MCP sessions to close.
const MCP_SHUTDOWN_TIMEOUT_SECS: u64 = 5;

/// Maximum time to wait for coordinator swarm shutdown.
const COORDINATOR_SHUTDOWN_TIMEOUT_SECS: u64 = 10;

/// Maximum time to wait for in-flight reads before abandoning the read pool's threads.
const READ_RUNTIME_SHUTDOWN_TIMEOUT_SECS: u64 = 10;

/// Emergency shutdown timeout: forces process exit if total shutdown exceeds this time.
/// Prevents indefinite hangs from stuck resources.
const EMERGENCY_SHUTDOWN_TIMEOUT_SECS: u64 = 120;

/// Install the process-level rustls crypto provider.
///
/// Must run before any TLS is used, on both the server and client paths. rustls 0.23
/// refuses to choose for itself when more than one provider feature is active in the
/// dependency graph, and this binary links libp2p-quic (ring) alongside axum-server and
/// reqwest; leaving the choice implicit is what made every HTTPS startup panic. `ring`
/// is chosen over `aws-lc-rs` because it needs no C toolchain, which keeps the musl and
/// Windows cross-builds simple.
fn install_crypto_provider() {
    // An error means another call won the race and a provider is already installed,
    // which is the desired end state either way.
    let _ = rustls::crypto::ring::default_provider().install_default();
}

#[tokio::main]
async fn main() -> Result<()> {
    install_crypto_provider();

    // Handle CLI arguments for configuration utilities and client mode
    let args: Vec<String> = std::env::args().collect();

    // Subcommands are dispatched on the first argument only. Matching anywhere in the
    // argument list would let a server flag's *value* — `--node-label search`, say — hijack
    // the process into client mode.
    let subcommand = args.get(1).map(String::as_str).unwrap_or_default();
    let interactive_requested = args
        .iter()
        .skip(1)
        .any(|arg| arg == "-i" || arg == "--interactive");

    if interactive_requested
        || matches!(
            subcommand,
            "client" | "health" | "index" | "search" | "schema" | "data" | "list" | "delete"
        )
    {
        return client::run_cli().await;
    }

    if args
        .iter()
        .skip(1)
        .any(|arg| arg == "--version" || arg == "-V")
    {
        println!("cameodb {}", env!("CARGO_PKG_VERSION"));
        return Ok(());
    }

    if subcommand == "generate-config" {
        println!("{}", CameoDbConfig::generate_sample_config()?);
        return Ok(());
    }

    // Mints a key and prints the configuration that accepts it. Needs no config file and no
    // running node — it is a hashing utility, not a server operation.
    if subcommand == "keygen" {
        return auth::run_keygen(args.into_iter().skip(2));
    }

    // Posture check without starting the node: the manual equivalent of a CI gate, so a
    // config can be verified before it is deployed rather than by watching a server boot.
    if subcommand == "check-config" {
        // Logs to stderr, so the matrix on stdout stays parseable. Without a subscriber the
        // things this command exists to surface — which file was loaded, unknown settings,
        // a world-writable key file — were being discarded.
        tracing_subscriber::fmt()
            .with_writer(std::io::stderr)
            .init();

        let cli_overrides = config::CliOverrides::parse(args.into_iter().skip(2))?;
        let cameodb_config = CameoDbConfig::load_unvalidated(&cli_overrides)?;

        // The resolved size limits, because most of them are derived rather than written: an
        // operator reading the file cannot tell what the node will actually enforce.
        println!(
            "Limits: record {}MB, HTTP body {}MB, remote msg {}MB, MCP response {}MB, \
             timeout {}s, memory budget {}MB",
            cameodb_config.limits.max_record_size_mb,
            cameodb_config.effective_max_body_size_mb(),
            cameodb_config.effective_remote_message_size_bytes() / (1024 * 1024),
            cameodb_config.effective_max_response_bytes() / (1024 * 1024),
            cameodb_config.effective_request_timeout_secs(),
            cameodb_config.limits.total_memory_limit_mb,
        );

        // Render the matrix before deciding, so a failure arrives with the context of
        // everything that passed rather than as a single line.
        let warnings = match posture::evaluate(&cameodb_config) {
            Ok(posture) => {
                print!("{}", posture.render());
                posture.warnings().count()
            }
            Err(e) => {
                eprintln!("Result: FAILED\n{}", e);
                std::process::exit(1);
            }
        };

        if let Err(e) = cameodb_config.validate() {
            eprintln!("\nResult: FAILED\n{}", e);
            std::process::exit(1);
        }
        println!(
            "\nResult: OK{}",
            if warnings > 0 {
                format!(
                    " ({} warning(s) — accepted risk for this profile)",
                    warnings
                )
            } else {
                String::new()
            }
        );
        return Ok(());
    }

    if args
        .iter()
        .skip(1)
        .any(|arg| arg == "--help" || arg == "-h")
    {
        println!(
            "cameodb {}\n\n\
             Usage:\n  \
             cameodb [OPTIONS]\n  \
             cameodb generate-config\n  \
             cameodb check-config [-c <PATH>]\n  \
             cameodb keygen --role <admin|writer|reader>\n  \
             cameodb client <subcommand>\n\n\
             Options:\n\
             {}\n  \
             -h, --help                                  Show this help message\n  \
             -V, --version                               Show version information\n\n\
             Commands:\n  \
             generate-config  Print a sample configuration file\n  \
             check-config     Report the security posture of a config and exit non-zero if it fails\n  \
             keygen           Mint an API key and print the [[security.api_keys]] stanza for it\n  \
             client           Run the bundled client CLI (health, index, search)\n\n\
             Client examples:\n  \
             cameodb client health\n  \
             cameodb client index list\n  \
             cameodb client search myindex \"foo bar\" --limit 5 --url http://host:9480\n\n\
             Configuration sources, highest precedence first: command-line options, then the\n  \
             CAMEODB_* environment variables shown above, then the config file, then defaults.",
            env!("CARGO_PKG_VERSION"),
            config::cli_help(),
        );
        return Ok(());
    }

    // Initialize tracing before loading config, so that which config file was chosen — and
    // any complaint about it — is visible rather than swallowed.
    tracing_subscriber::fmt::init();

    // Load configuration: command line over environment over file over defaults.
    let cli_overrides = config::CliOverrides::parse(args.into_iter().skip(1))?;
    let cameodb_config = CameoDbConfig::load_with_cli(&cli_overrides)?;

    // Name every configured key once at startup. The point is the `key_id`: an audit line or
    // a rejection carrying one has to be traceable back to a team without the key itself ever
    // appearing in a log. `validate()` already resolved these, so this cannot fail.
    let keyring = Arc::new(cameodb_config.security.load_keyring()?);
    for entry in keyring.entries() {
        tracing::info!(
            key_id = %entry.key_id(),
            label = %entry.label(),
            role = %entry.role(),
            indexes = %entry.scope_summary(),
            auth_enabled = keyring.enabled(),
            "🔑 API key loaded"
        );
    }

    // Load the TLS material here, before storage is opened and before the startup banner
    // is printed. Loading it at bind time meant a bad certificate — or a missing crypto
    // provider — surfaced as a panic *after* the banner claimed the server was up.
    let tls_config = match &cameodb_config.network.http.tls {
        tls if tls.enabled => {
            let cert_file = tls
                .cert_file
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("TLS enabled but cert_file not configured"))?;
            let key_file = tls
                .key_file
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("TLS enabled but key_file not configured"))?;
            tracing::info!(
                cert_file = %cert_file.display(),
                key_file = %key_file.display(),
                "Loading TLS certificate and key"
            );
            let loaded = axum_server::tls_rustls::RustlsConfig::from_pem_file(cert_file, key_file)
                .await
                .map_err(|e| {
                    anyhow::anyhow!(
                        "Failed to load TLS certificate '{}' and key '{}': {}",
                        cert_file.display(),
                        key_file.display(),
                        e
                    )
                })?;
            Some(loaded)
        }
        _ => None,
    };

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
        total_memory_limit_mb: cameodb_config.limits.total_memory_limit_mb,
        memory_pressure_threshold_percent: cameodb_config.search.memory_pressure_threshold_percent,
        search_threads: cameodb_config.search.search_threads,
        wal_sync: cameodb_config.storage.wal_sync,
        default_batch_size: cameodb_config.storage.default_batch_size,
        indexer_num_threads: cameodb_config.search.indexer_num_threads,
        merge_num_threads: cameodb_config.search.merge_num_threads,
        writer_shutdown_timeout_secs: 30,
        supervisor_timeout_secs: cameodb_config.search.supervisor_timeout_secs,
        writer_core_affinity: cameodb_config.storage.writer_core_affinity,
        shard_affine_dispatch: cameodb_config.storage.shard_affine_dispatch,
        worker_core_affinity: cameodb_config.storage.worker_core_affinity,
    };

    // Create the NodeOrchestrator actor
    let mut orchestrator = NodeOrchestrator::new(
        node_config,
        identity,
        cameodb_config.search.default_search_limit,
        cameodb_config.search.max_concurrent_shard_searches,
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

    // Initialize distributed cluster with derived remote messaging limits
    let distributed_cluster = DistributedCluster::new(
        cameodb_config.network.cluster.clone(),
        node_id,
        orchestrator.identity().name.clone(),
        primary_path.clone(),
        cameodb_config.effective_remote_message_size_bytes(),
        cameodb_config.effective_remote_timeout_secs(),
    );

    // Create shared remote peer pool for cached actor ref lookups
    let remote_peer_pool = Arc::new(RemotePeerPool::new());

    // Create ClusterCoordinator but DON'T start swarm yet
    let coordinator = if let Some(persisted) = persisted_cluster {
        tracing::info!(
            "Restoring cluster from persisted state: {} nodes, {} shards expected",
            persisted.nodes.len(),
            persisted.shards.len()
        );
        let mut c = ClusterCoordinator::new_with_persisted_state(
            distributed_cluster,
            persisted,
            state_store.clone(),
        );
        c.set_remote_peer_pool(Arc::clone(&remote_peer_pool));
        c
    } else {
        tracing::info!("Fresh cluster boot, no persisted state");
        let mut coordinator = ClusterCoordinator::new(distributed_cluster);
        coordinator.set_state_store(state_store.clone());
        coordinator.set_remote_peer_pool(Arc::clone(&remote_peer_pool));
        coordinator
    };

    // NOW spawn the coordinator actor
    let coordinator_actor = ClusterCoordinator::spawn(coordinator);

    // Set coordinator reference on orchestrator FIRST
    orchestrator.set_coordinator(coordinator_actor.clone());
    orchestrator.set_remote_peer_pool(Arc::clone(&remote_peer_pool));

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

    // Spawn the worker pool BEFORE spawning the actor (we need &mut access)
    orchestrator.spawn_worker_pool();
    let worker_tx = orchestrator.worker_tx();
    let shared_routing_ring = orchestrator.shared_routing_ring();
    let shard_placement = orchestrator.shard_placement();

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
        StreamingSearchConfig::from_search_config(&cameodb_config.search),
        cameodb_config.search.default_search_limit,
        worker_tx,
        Arc::clone(&remote_peer_pool),
        ShardAffineConfig {
            routing_ring: shared_routing_ring,
            enabled: cameodb_config.storage.shard_affine_dispatch,
        },
        shard_placement,
    );

    // Started before the router, so the writer thread is already draining when the first
    // request arrives rather than being spun up on the request path.
    let audit_sink = audit::AuditSink::start(&cameodb_config.security.audit);

    let app_state = AppState {
        router: router_actor,
        coordinator: coordinator_actor.clone(),
        stream_batch_size: cameodb_config.search.stream_batch_size,
        max_record_size_bytes: cameodb_config.limits.max_record_size_mb * 1024 * 1024,
        tool_limiter: std::sync::Arc::new(ratelimit::ToolRateLimiter::new(
            cameodb_config.security.limits.clone(),
        )),
        max_search_limit: cameodb_config.security.limits.max_search_limit,
        max_response_bytes: cameodb_config.effective_max_response_bytes(),
        audit: Arc::clone(&audit_sink),
    };

    // Create the HTTP router with shared state and body limit derived from max_record_size_mb
    let (app, mcp_handle) = create_router(
        app_state,
        cameodb_config.effective_max_body_size_mb(),
        &cameodb_config.network.http.cors_allowed_origins,
        cameodb_config.network.http.max_concurrent_requests,
        cameodb_config.effective_request_timeout_secs(),
        cameodb_config.network.http.admin_enabled,
        keyring.clone(),
    );

    // Extract HTTP configuration
    let http_config = &cameodb_config.network.http;
    let bind_address = format!("{}:{}", http_config.bind_address, http_config.port);

    // Print startup information
    let protocol = if http_config.tls.enabled {
        "https"
    } else {
        "http"
    };
    println!(
        "🚀 CameoDB {} Server starting on {}://{}",
        protocol.to_uppercase(),
        protocol,
        bind_address
    );
    println!("🎯 API endpoints:");
    println!("  POST /api/{{index}}/search - Standard search");
    println!("  POST /api/{{index}}/search/stream - Streaming search");
    println!("  PUT  /api/{{index}}/document - Write document");
    println!("  DELETE /api/{{index}}/document - Delete document");
    println!("  POST /api/{{index}}/document/stream - Streaming write");
    println!("  POST /api/{{index}}/_bulk - Bulk write documents");
    println!("  POST /api/{{index}}/_bulk/delete - Bulk delete documents");
    println!("  PUT  /api/{{index}}/_config - Create/update index schema");
    println!("  GET  /api/{{index}}/_config - Retrieve index schema");
    println!("  PATCH /api/{{index}}/_schema - Update index schema");
    println!("  DELETE /api/{{index}} - Delete index (?delete_schema=true/false)");
    println!("  GET  /_indexes - List all indexes with statistics");
    println!("  GET  /_cluster/_indexes - List cluster indexes");
    println!("  GET  /_cluster/health - Health check");
    println!("  GET  /_admin/memory - Memory statistics (jemalloc + process)");
    println!(
        "  POST /_admin/memory/purge - Trigger jemalloc memory purge (?force=true for aggressive)"
    );
    println!("  POST /_admin/index/{{index}}/commit - Force index writer commit");
    println!("  GET  /_admin/workers - Worker pool statistics");
    println!("  POST /_admin/index/{{index}}/evict-writer - Evict index writer from cache");
    println!(
        "  POST /mcp - MCP Streamable HTTP endpoint (JSON-RPC, returns MCP-Session-Id on initialize)"
    );
    println!("  GET  /mcp - MCP Streamable HTTP listening stream (SSE)");
    println!("  DELETE /mcp - MCP Streamable HTTP session termination (MCP-Session-Id header)");
    println!("  GET  /mcp/sse - MCP legacy SSE transport endpoint");
    println!("  POST /mcp/sse - MCP legacy compatibility HTTP endpoint");
    println!("  POST /mcp/messages?session_id=... - MCP legacy JSON-RPC message endpoint");
    println!(
        "  MCP protocol: 2025-06-18 (negotiated), capabilities: tools (6), resources (4), prompts (1)"
    );
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
        cameodb_config.limits.total_memory_limit_mb
    );
    println!(
        "  Max Record Size: {}MB (HTTP body: {}MB, remote msg: {}MB, timeout: {}s)",
        cameodb_config.limits.max_record_size_mb,
        cameodb_config.effective_max_body_size_mb(),
        cameodb_config.effective_remote_message_size_bytes() / (1024 * 1024),
        cameodb_config.effective_request_timeout_secs()
    );
    println!(
        "  Max Concurrent Requests: {}",
        cameodb_config.network.http.max_concurrent_requests
    );
    println!();
    // `validate()` already accepted this config, so this only re-renders the decision.
    match posture::evaluate(&cameodb_config) {
        Ok(p) => print!("🔒 {}", p.render()),
        Err(e) => println!("🔒 Security profile: unavailable ({})", e),
    }
    println!();
    println!("Press Ctrl+C to shutdown...");
    println!();

    // Start the HTTP server with configured address
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

    // Under TLS the drain is driven by an `axum_server::Handle` rather than the oneshot,
    // so Phase 2 of shutdown can actually finish instead of waiting out its full timeout
    // and then cutting in-flight requests.
    let tls_handle = tls_config.as_ref().map(|_| axum_server::Handle::new());

    let server_handle = if let Some(tls_config) = tls_config {
        let addr: std::net::SocketAddr = bind_address
            .parse()
            .map_err(|e| anyhow::anyhow!("Invalid bind address {}: {}", bind_address, e))?;
        let handle = tls_handle.clone().expect("handle created alongside config");

        tokio::spawn(async move {
            if let Err(e) = axum_server::bind_rustls(addr, tls_config)
                .handle(handle)
                .serve(app.into_make_service_with_connect_info::<std::net::SocketAddr>())
                .await
            {
                eprintln!("HTTPS server error: {}", e);
            }
        })
    } else {
        // TLS disabled: use regular axum with TCP listener
        let listener = tokio::net::TcpListener::bind(&bind_address).await?;

        tokio::spawn(async move {
            if let Err(e) = axum::serve(
                listener,
                app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
            )
            .with_graceful_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await
            {
                eprintln!("HTTP server error: {}", e);
            }
        })
    };

    // Wait for shutdown signal (Ctrl+C or systemctl stop)
    #[cfg(unix)]
    {
        use tokio::signal::unix;
        let mut sigterm_recv = unix::signal(unix::SignalKind::terminate())
            .map_err(|e| anyhow::anyhow!("Failed to setup SIGTERM handler: {}", e))?;
        let sigint_recv = tokio::signal::ctrl_c();

        tokio::select! {
            _ = sigint_recv => {
                if SHUTDOWN_IN_PROGRESS.swap(true, Ordering::SeqCst) {
                    tracing::warn!("Second SIGINT received, forcing immediate exit");
                    std::process::exit(1);
                }
                tracing::info!("Received SIGINT (Ctrl+C), shutting down...");
            }
            _ = sigterm_recv.recv() => {
                if SHUTDOWN_IN_PROGRESS.swap(true, Ordering::SeqCst) {
                    tracing::warn!("Second SIGTERM received, forcing immediate exit");
                    std::process::exit(1);
                }
                tracing::info!("Received SIGTERM (systemctl stop), shutting down...");
            }
        }
    }

    #[cfg(not(unix))]
    {
        use tokio::signal::windows;

        let mut sigint = windows::ctrl_c()
            .map_err(|e| anyhow::anyhow!("Failed to setup Ctrl+C handler: {}", e))?;
        let mut sigclose = windows::ctrl_close()
            .map_err(|e| anyhow::anyhow!("Failed to setup CTRL_CLOSE handler: {}", e))?;
        let mut sigshutdown = windows::ctrl_shutdown()
            .map_err(|e| anyhow::anyhow!("Failed to setup CTRL_SHUTDOWN handler: {}", e))?;

        tokio::select! {
            _ = sigint.recv() => {
                if SHUTDOWN_IN_PROGRESS.swap(true, Ordering::SeqCst) {
                    tracing::warn!("Second CTRL+C received, forcing immediate exit");
                    std::process::exit(1);
                }
                tracing::info!("Received CTRL+C, shutting down...");
            }
            _ = sigclose.recv() => {
                if SHUTDOWN_IN_PROGRESS.swap(true, Ordering::SeqCst) {
                    tracing::warn!("Second CTRL_CLOSE received, forcing immediate exit");
                    std::process::exit(1);
                }
                tracing::info!("Received CTRL_CLOSE (service stop/console close), shutting down...");
            }
            _ = sigshutdown.recv() => {
                if SHUTDOWN_IN_PROGRESS.swap(true, Ordering::SeqCst) {
                    tracing::warn!("Second CTRL_SHUTDOWN received, forcing immediate exit");
                    std::process::exit(1);
                }
                tracing::info!("Received CTRL_SHUTDOWN (service stop), shutting down...");
            }
        }
    }
    println!("Shutting down gracefully (press Ctrl+C again to force)...");

    // Start emergency shutdown timer
    let shutdown_start = std::time::Instant::now();
    tracing::info!(
        "Starting graceful shutdown (emergency timeout: {}s)",
        EMERGENCY_SHUTDOWN_TIMEOUT_SECS
    );

    // Phase 1: Shutdown MCP sessions (timeout: 5s)
    tracing::info!(
        "Phase 1/5: Closing MCP sessions (timeout: {}s)...",
        MCP_SHUTDOWN_TIMEOUT_SECS
    );
    match timeout(
        Duration::from_secs(MCP_SHUTDOWN_TIMEOUT_SECS),
        mcp_handle.shutdown(),
    )
    .await
    {
        Ok(()) => tracing::info!("MCP sessions closed successfully"),
        Err(_) => tracing::warn!(
            "MCP shutdown timed out after {}s, continuing...",
            MCP_SHUTDOWN_TIMEOUT_SECS
        ),
    }

    // Emergency timeout check
    if shutdown_start.elapsed() > Duration::from_secs(EMERGENCY_SHUTDOWN_TIMEOUT_SECS) {
        tracing::error!("EMERGENCY: Shutdown timeout exceeded after Phase 1 - forcing exit");
        std::process::exit(1);
    }

    // Phase 2: Drain HTTP server (timeout: 10s)
    tracing::info!(
        "Phase 2/5: Draining HTTP connections (timeout: {}s)...",
        HTTP_DRAIN_TIMEOUT_SECS
    );
    let _ = shutdown_tx.send(());
    if let Some(handle) = &tls_handle {
        handle.graceful_shutdown(Some(Duration::from_secs(HTTP_DRAIN_TIMEOUT_SECS)));
    }
    match timeout(Duration::from_secs(HTTP_DRAIN_TIMEOUT_SECS), server_handle).await {
        Ok(Ok(())) => tracing::info!("HTTP server drained successfully"),
        Ok(Err(e)) => tracing::warn!("HTTP server ended with error: {}", e),
        Err(_) => tracing::warn!(
            "HTTP drain timed out after {}s, forcing close...",
            HTTP_DRAIN_TIMEOUT_SECS
        ),
    }

    // Emergency timeout check
    if shutdown_start.elapsed() > Duration::from_secs(EMERGENCY_SHUTDOWN_TIMEOUT_SECS) {
        tracing::error!("EMERGENCY: Shutdown timeout exceeded after Phase 2 - forcing exit");
        std::process::exit(1);
    }

    // Phase 3: Shutdown all shards (timeout: 60s)
    tracing::info!(
        "Phase 3/5: Shutting down all shards (timeout: {}s)...",
        SHARD_SHUTDOWN_TIMEOUT_SECS
    );
    match timeout(
        Duration::from_secs(SHARD_SHUTDOWN_TIMEOUT_SECS),
        orchestrator_ref.ask(crate::node_orchestrator::ShutdownAllShards),
    )
    .await
    {
        Ok(Ok(())) => tracing::info!("All shards shut down successfully"),
        Ok(Err(e)) => tracing::error!(error = %e, "Shard shutdown failed"),
        Err(_) => tracing::error!(
            "Shard shutdown timed out after {}s - data may not be persisted!",
            SHARD_SHUTDOWN_TIMEOUT_SECS
        ),
    }

    // Emergency timeout check
    if shutdown_start.elapsed() > Duration::from_secs(EMERGENCY_SHUTDOWN_TIMEOUT_SECS) {
        tracing::error!("EMERGENCY: Shutdown timeout exceeded after Phase 3 - forcing exit");
        std::process::exit(1);
    }

    // Phase 4: Shut down the dedicated read thread pool (timeout: 10s).
    //
    // After the shards, because their shutdown is what the last reads run against, and before
    // the completion message, because `Drop` can only detach these threads — leaving the line
    // that says the process is exiting cleanly to be printed while eight of them are still
    // being torn down.
    tracing::info!(
        "Phase 4/5: Shutting down read thread pool (timeout: {}s)...",
        READ_RUNTIME_SHUTDOWN_TIMEOUT_SECS
    );
    // The handler's own bound is `READ_RUNTIME_SHUTDOWN_TIMEOUT_SECS`; this one is the
    // backstop for a reply that never arrives, so it has to be the looser of the two or a
    // shutdown that used its full budget would be reported as a hang.
    let read_pool_reply_timeout = Duration::from_secs(READ_RUNTIME_SHUTDOWN_TIMEOUT_SECS + 5);
    match timeout(
        read_pool_reply_timeout,
        orchestrator_ref.ask(crate::node_orchestrator::ShutdownReadRuntime {
            timeout: Duration::from_secs(READ_RUNTIME_SHUTDOWN_TIMEOUT_SECS),
        }),
    )
    .await
    {
        Ok(Ok(())) => tracing::info!("Read thread pool shut down successfully"),
        Ok(Err(e)) => {
            tracing::error!(error = %e, "Could not reach the orchestrator to shut down the read thread pool")
        }
        Err(_) => tracing::error!(
            "Read thread pool shutdown did not report back within {}s",
            READ_RUNTIME_SHUTDOWN_TIMEOUT_SECS + 5
        ),
    }

    // Phase 5: Shutdown coordinator (timeout: 10s)
    tracing::info!(
        "Phase 5/5: Shutting down coordinator (timeout: {}s)...",
        COORDINATOR_SHUTDOWN_TIMEOUT_SECS
    );
    match timeout(
        Duration::from_secs(COORDINATOR_SHUTDOWN_TIMEOUT_SECS),
        coordinator_actor.ask(ShutdownSwarm),
    )
    .await
    {
        Ok(Ok(())) => tracing::info!("Coordinator shut down successfully"),
        Ok(Err(e)) => tracing::warn!("Coordinator shutdown error: {}", e),
        Err(_) => tracing::warn!(
            "Coordinator shutdown timed out after {}s",
            COORDINATOR_SHUTDOWN_TIMEOUT_SECS
        ),
    }

    // Last, after every other subsystem has had its say, so their final refusals and
    // rollups are in the trail rather than lost with the process. On a busy ingest node the
    // open rollup window is every write of the last ten seconds.
    audit_sink.shutdown();

    // Final emergency timeout check
    let shutdown_elapsed = shutdown_start.elapsed();
    if shutdown_elapsed > Duration::from_secs(EMERGENCY_SHUTDOWN_TIMEOUT_SECS) {
        tracing::error!(
            elapsed_secs = shutdown_elapsed.as_secs(),
            timeout_secs = EMERGENCY_SHUTDOWN_TIMEOUT_SECS,
            "EMERGENCY: Shutdown exceeded {}s - forcing exit",
            EMERGENCY_SHUTDOWN_TIMEOUT_SECS
        );
        std::process::exit(1);
    }

    tracing::info!(
        elapsed_ms = shutdown_elapsed.as_millis(),
        "Shutdown complete - process exiting cleanly"
    );
    Ok(())
}
