//! Custom Swarm Orchestrator & Event Loop for CameoDB Distributed Database
//!
//! This module implements the main orchestration logic for the custom libp2p swarm,
//! following the ANOTHER APPROACH architecture. It provides the entry point for
//! swarm initialization and manages the event loop processing.

pub mod behaviour;
pub mod utils;

use crate::config::ClusterConfig;
use anyhow::Result;
use behaviour::{DhtBehaviour, DhtBehaviourEvent};
use cluster::NodeIdentity;
use futures::StreamExt;
use libp2p::{
    Multiaddr, PeerId, SwarmBuilder, identify, identity::Keypair, kad, noise, swarm::SwarmEvent,
    tcp, yamux,
};
use std::collections::HashMap;
use std::net::{IpAddr, ToSocketAddrs};
use std::path::Path;
use std::time::Duration;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::{select, sync::watch};
use tracing::{debug, info, warn};
use uuid::Uuid;

// Re-export key types for convenience
pub use utils::resolve_listen_address;

/// Result returned after the swarm runtime has been launched
#[derive(Debug)]
pub struct SwarmStartup {
    pub peer_id: PeerId,
    pub listen_addr: Multiaddr,
    pub bootstrap_peer_count: usize,
    pub runtime: SwarmRuntimeHandle,
    pub events: Option<UnboundedReceiver<CoordinatorEvent>>,
}

/// Events emitted from the swarm runtime to be forwarded to the coordinator actor.
#[derive(Debug)]
pub enum CoordinatorEvent {
    RoutingUpdated {
        #[allow(dead_code)]
        peer_id: String,
        #[allow(dead_code)]
        address_count: usize,
    },
    PeerDiscovered {
        peer_id: String,
        address: Option<String>,
    },
    PeerLost {
        peer_id: String,
        node_uuid: Option<String>,
        address: Option<String>,
    },
    DialFailed {
        peer_id: Option<String>,
        error: String,
    },
    PeerUuidDiscovered {
        peer_id: String,
        node_uuid: String,
        address: Option<String>,
    },
    PeerNodeMetadataDiscovered {
        node_uuid: String,
        node_name: String,
        shard_count: u32,
        generation: u64,
        checksum: u64,
        address: Option<String>,
        status: String,
        total_storage_bytes: u64,
        total_document_count: u64,
    },
    PeerShardDiscovered {
        node_uuid: String,
        shard: crate::cluster_coordinator::ShardMetadata,
    },
}

/// Handle used to manage the background swarm runtime task
#[derive(Debug, Clone)]
pub struct SwarmRuntimeHandle {
    shutdown_tx: Option<watch::Sender<SwarmControl>>,
    cmd_tx: Option<UnboundedSender<SwarmCommand>>,
}

impl SwarmRuntimeHandle {
    fn new(
        shutdown_tx: watch::Sender<SwarmControl>,
        cmd_tx: UnboundedSender<SwarmCommand>,
    ) -> Self {
        Self {
            shutdown_tx: Some(shutdown_tx),
            cmd_tx: Some(cmd_tx),
        }
    }

    fn inert() -> Self {
        Self {
            shutdown_tx: None,
            cmd_tx: None,
        }
    }

    /// Request a graceful shutdown of the swarm runtime task
    pub fn shutdown(&self) -> Result<()> {
        if let Some(tx) = &self.shutdown_tx {
            tx.send(SwarmControl::Shutdown)
                .map_err(|err| anyhow::anyhow!("failed to signal swarm shutdown: {}", err))?
        }
        Ok(())
    }

    /// Returns true if the runtime task is still active
    pub fn is_running(&self) -> bool {
        self.shutdown_tx
            .as_ref()
            .map(|tx| !matches!(*tx.borrow(), SwarmControl::Shutdown))
            .unwrap_or(false)
    }

    /// Publish local shards to the DHT
    pub fn publish_shards(
        &self,
        node_uuid: Uuid,
        node_name: String,
        shards: Vec<crate::cluster_coordinator::ShardMetadata>,
        generation: u64,
        checksum: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let Some(ref cmd_tx) = self.cmd_tx {
            cmd_tx.send(SwarmCommand::PublishShards {
                node_uuid,
                node_name,
                shards,
                generation,
                checksum,
            })?;
        }
        Ok(())
    }

    /// Query node metadata for a remote node from the DHT
    pub fn query_node_metadata(&self, node_uuid: Uuid) -> Result<()> {
        if let Some(tx) = &self.cmd_tx {
            tx.send(SwarmCommand::QueryNodeMetadata { node_uuid })
                .map_err(|_| anyhow::anyhow!("Swarm runtime channel closed"))?;
        }
        Ok(())
    }

    /// Query a specific shard from the DHT
    #[allow(dead_code)]
    pub fn query_shard(&self, node_uuid: Uuid, shard_id: Uuid) -> Result<()> {
        if let Some(tx) = &self.cmd_tx {
            tx.send(SwarmCommand::QueryShard {
                node_uuid,
                shard_id,
            })
            .map_err(|_| anyhow::anyhow!("Swarm runtime channel closed"))?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SwarmControl {
    Run,
    Shutdown,
}

#[derive(Debug, Default)]
struct SwarmRuntimeMetrics {
    total_events: u64,
    behaviour_events: u64,
    kademlia_updates: u64,
    connections_established: u64,
    connections_closed: u64,
    bootstrapped: bool,
}

impl SwarmRuntimeMetrics {
    fn log_summary(&self) {
        info!(
            total_events = self.total_events,
            behaviour_events = self.behaviour_events,
            kademlia_updates = self.kademlia_updates,
            connections_established = self.connections_established,
            connections_closed = self.connections_closed,
            "📊 Swarm runtime summary"
        );
    }
}

#[derive(Default)]
struct PeerBook {
    uuid_by_peer: HashMap<String, String>,
    addr_by_peer: HashMap<String, String>, // last known good address (from established conn or identify)
}

/// Check if a multiaddr resolves to a local address
fn resolves_to_local(
    addr: &Multiaddr,
    listen_ip4: Option<std::net::Ipv4Addr>,
    listen_ip6: Option<std::net::Ipv6Addr>,
) -> bool {
    let addr_str = addr.to_string();

    // Check for DNS addresses
    if let Some(dns_part) = addr_str.strip_prefix("/dns4/") {
        // Parse /dns4/hostname/tcp/port format
        if let Some((hostname, port_str)) = dns_part.split_once("/tcp/") {
            if let Ok(port) = port_str.parse::<u16>() {
                if let Ok(addrs) = (hostname, port).to_socket_addrs() {
                    let addrs_vec: Vec<_> = addrs.collect();
                    debug!(
                        "🔍 Checking DNS4 {}:{} resolves to {:?} (local IP: {:?})",
                        hostname, port, addrs_vec, listen_ip4
                    );
                    for sa in addrs_vec {
                        match sa.ip() {
                            IpAddr::V4(ip) => {
                                if Some(ip) == listen_ip4 {
                                    info!("🎯 DNS4 {} resolves to local IP {}", hostname, ip);
                                    return true;
                                }
                            }
                            IpAddr::V6(ip) => {
                                if Some(ip) == listen_ip6 {
                                    info!("🎯 DNS4 {} resolves to local IP {}", hostname, ip);
                                    return true;
                                }
                            }
                        }
                    }
                } else {
                    debug!("⚠️  Failed to resolve DNS4 {}:{}", hostname, port);
                }
            } else {
                debug!("⚠️  Invalid port in DNS4 address: {}", port_str);
            }
        } else {
            debug!("⚠️  Invalid DNS4 format: {}", dns_part);
        }
    } else if let Some(dns_part) = addr_str.strip_prefix("/dns6/") {
        // Parse /dns6/hostname/tcp/port format
        if let Some((hostname, port_str)) = dns_part.split_once("/tcp/") {
            if let Ok(port) = port_str.parse::<u16>() {
                if let Ok(addrs) = (hostname, port).to_socket_addrs() {
                    let addrs_vec: Vec<_> = addrs.collect();
                    debug!(
                        "🔍 Checking DNS6 {}:{} resolves to {:?} (local IP: {:?})",
                        hostname, port, addrs_vec, listen_ip6
                    );
                    for sa in addrs_vec {
                        match sa.ip() {
                            IpAddr::V4(ip) => {
                                if Some(ip) == listen_ip4 {
                                    info!("🎯 DNS6 {} resolves to local IP {}", hostname, ip);
                                    return true;
                                }
                            }
                            IpAddr::V6(ip) => {
                                if Some(ip) == listen_ip6 {
                                    info!("🎯 DNS6 {} resolves to local IP {}", hostname, ip);
                                    return true;
                                }
                            }
                        }
                    }
                } else {
                    debug!("⚠️  Failed to resolve DNS6 {}:{}", hostname, port);
                }
            } else {
                debug!("⚠️  Invalid port in DNS6 address: {}", port_str);
            }
        } else {
            debug!("⚠️  Invalid DNS6 format: {}", dns_part);
        }
    } else if addr_str.starts_with("/dns4/") || addr_str.starts_with("/dns6/") {
        debug!("⚠️  DNS resolution failed for address: {}", addr_str);
    }

    false
}

fn select_preferred_address(addrs: &[Multiaddr]) -> Option<Multiaddr> {
    // Priority order: dns4 -> ip4 -> dns6 -> ip6 -> anything else
    if let Some(addr) = addrs
        .iter()
        .find(|a| a.to_string().starts_with("/dns4/"))
        .cloned()
    {
        return Some(addr);
    }
    if let Some(addr) = addrs
        .iter()
        .find(|a| a.to_string().starts_with("/ip4/"))
        .cloned()
    {
        return Some(addr);
    }
    if let Some(addr) = addrs
        .iter()
        .find(|a| a.to_string().starts_with("/dns6/"))
        .cloned()
    {
        return Some(addr);
    }
    if let Some(addr) = addrs
        .iter()
        .find(|a| a.to_string().starts_with("/ip6/"))
        .cloned()
    {
        return Some(addr);
    }
    addrs.first().cloned()
}

/// Initialize the distributed swarm for peer-to-peer communication
pub async fn init_distributed_swarm(
    config: &ClusterConfig,
    node_uuid: Uuid,
    node_name: String,
    storage_path: &Path,
) -> Result<SwarmStartup> {
    if !config.enabled {
        info!("Cluster mode disabled, running in standalone single-node mode");
        return Ok(SwarmStartup {
            peer_id: PeerId::random(),
            listen_addr: "/ip4/127.0.0.1/tcp/0".parse().unwrap(),
            bootstrap_peer_count: 0,
            runtime: SwarmRuntimeHandle::inert(),
            events: None,
        });
    }

    info!("🚀 Initializing distributed libp2p swarm");

    // Create production-ready swarm with Kademlia DHT
    let startup = create_production_swarm(config, node_uuid, node_name, storage_path).await?;

    info!("✅ Production swarm initialized successfully");
    info!("   📡 Peer ID: {}", startup.peer_id);
    info!("   🎧 Listen Address: {}", startup.listen_addr);
    info!("   🚀 Cluster Port: {} (from config)", config.cluster_port);
    info!("   🌐 Discovery: Kademlia DHT");
    info!("   📊 Bootstrap Peers: {}", startup.bootstrap_peer_count);

    // TODO: Future enhancements:
    // - Cluster state actor integration for peer management
    // - Enhanced event loop with distributed state synchronization

    Ok(startup)
}

/// Create a production-ready libp2p swarm with custom behaviour
async fn create_production_swarm(
    config: &ClusterConfig,
    node_uuid: Uuid,
    node_name: String,
    storage_path: &Path,
) -> Result<SwarmStartup> {
    // Load or generate cryptographic identity for this node
    let (keypair, _identity) = load_or_generate_keypair(storage_path)?;
    let peer_id = PeerId::from(keypair.public());

    info!("🔐 Node identity: {}", peer_id);

    // Get optimized listen address using configured bind + interfaces (fallback handled inside)
    let listen_addr = resolve_listen_address(
        &config.bind_address,
        &config.listen_addrs,
        config.cluster_port,
    )?;

    // Capture our own listen IPs for self-dial filtering
    let mut listen_ip4 = None;
    let mut listen_ip6 = None;
    for p in listen_addr.iter() {
        match p {
            libp2p::multiaddr::Protocol::Ip4(ip) => listen_ip4 = Some(ip),
            libp2p::multiaddr::Protocol::Ip6(ip) => listen_ip6 = Some(ip),
            _ => {}
        }
    }

    // Create custom network behaviour with production settings
    let behaviour = DhtBehaviour::new(
        peer_id,
        Some(libp2p::kad::Mode::Server), // Server mode for stable operation
        keypair.public(),
        node_uuid,
        node_name,
    )?;

    info!("🏗️  Created Kademlia DHT behaviour for peer discovery");

    // Build the libp2p swarm with full transport stack including DNS
    let mut swarm = SwarmBuilder::with_existing_identity(keypair)
        .with_tokio()
        .with_tcp(
            tcp::Config::default().nodelay(true),
            noise::Config::new,
            || {
                let mut config = yamux::Config::default();
                // Allow more concurrent streams for high-throughput cluster communication
                config.set_max_num_streams(8192);
                config
            },
        )?
        .with_quic() // Add QUIC support for better connectivity
        .with_dns()? // Enable DNS resolution for hostname-based multiaddrs
        .with_behaviour(|_key| Ok(behaviour))?
        .with_swarm_config(|c| {
            c.with_idle_connection_timeout(Duration::from_secs(300))
                .with_max_negotiating_inbound_streams(2048)
        })
        .build();

    // Initialize Kameo remote registry so remote actors can be registered/looked up.
    swarm.behaviour_mut().kameo.init_global();
    info!("🎭 Kameo remote actor registry initialized");

    // Publish node UUID to DHT for peer discovery
    if let Err(e) = swarm.behaviour_mut().publish_node_uuid(&peer_id, node_uuid) {
        warn!("⚠️  Failed to publish node UUID to DHT: {}", e);
    }

    // Start listening on the optimized address
    swarm.listen_on(listen_addr.clone())?;
    info!("🎧 Swarm listening on: {}", listen_addr);
    // Log all active listeners to show OS-resolved interfaces/ports (after potential port rebinding)
    for addr in swarm.listeners() {
        info!("   📡 Active listener: {}", addr);
    }

    // Connect to seed nodes for DHT initialization
    let seed_addrs = convert_seed_nodes_to_multiaddrs(&config.seed_nodes);
    let mut connected_peers = 0;

    info!(
        "🔍 Seed node configuration: {} nodes configured",
        config.seed_nodes.len()
    );
    for node in &config.seed_nodes {
        info!("   - Seed node: {}", node);
    }

    for addr in seed_addrs {
        debug!(
            "🔍 Checking seed address: {} (local IPs: {:?}, {:?})",
            addr, listen_ip4, listen_ip6
        );

        // Skip self-dialing by checking against our listeners and resolved listen IPs
        if swarm.listeners().any(|l| l == &addr) {
            info!(
                "⏭️  Skipping self-dial to local seed node (listener match): {}",
                addr
            );
            continue;
        }
        if let Some(ip4) = listen_ip4
            && addr.to_string().starts_with(&format!("/ip4/{}/tcp/", ip4))
        {
            info!(
                "⏭️  Skipping self-dial to local seed node (ip4 match): {}",
                addr
            );
            continue;
        }
        if let Some(ip6) = listen_ip6
            && addr.to_string().starts_with(&format!("/ip6/{}/tcp/", ip6))
        {
            info!(
                "⏭️  Skipping self-dial to local seed node (ip6 match): {}",
                addr
            );
            continue;
        }

        // Check if DNS address resolves to local
        debug!("🔍 About to check DNS resolution for: {}", addr);
        if resolves_to_local(&addr, listen_ip4, listen_ip6) {
            info!(
                "⏭️  Skipping self-dial to local seed node (DNS resolves to local): {}",
                addr
            );
            continue;
        }

        info!("📞 Attempting to dial seed node: {}", addr);
        match swarm.dial(addr.clone()) {
            Ok(_) => {
                connected_peers += 1;
                info!("✅ Successfully initiated dial to: {}", addr);
            }
            Err(e) => {
                warn!("⚠️  Failed to dial seed node {}: {:?}", addr, e);
            }
        }
    }

    info!(
        "📊 Seed node dial summary: {} successful, {} total",
        connected_peers,
        config.seed_nodes.len()
    );

    // Bootstrap is deferred to the swarm runtime and will trigger on first non-self peer connect.
    if connected_peers == 0 {
        info!("📋 No seed nodes available - running in standalone mode");
    }

    // Start the swarm runtime task to process events
    let (event_tx, event_rx) = unbounded_channel();
    let (cmd_tx, cmd_rx) = unbounded_channel();
    let runtime = launch_swarm_runtime(swarm, event_tx, cmd_rx, cmd_tx.clone());

    Ok(SwarmStartup {
        peer_id,
        listen_addr,
        bootstrap_peer_count: connected_peers,
        runtime,
        events: Some(event_rx),
    })
}

/// Load existing keypair from node_identity.json or generate a new one
pub fn load_or_generate_keypair(storage_path: &Path) -> Result<(Keypair, NodeIdentity)> {
    let identity_path = storage_path.join("node_identity.json");

    // 1. Try to load existing identity to get the keypair
    let existing_identity = if identity_path.exists() {
        match NodeIdentity::load(identity_path.clone()) {
            Ok(id) => Some(id),
            Err(e) => {
                warn!(
                    "⚠️  Failed to load existing identity: {}. Will recreate.",
                    e
                );
                None
            }
        }
    } else {
        None
    };

    // 2. Get or generate the keypair
    let keypair = if let Some(ref identity) = existing_identity {
        if let Some(key_bytes) = &identity.keypair {
            info!("🔑 Loading existing libp2p keypair from node_identity.json");
            match Keypair::from_protobuf_encoding(key_bytes) {
                Ok(kp) => kp,
                Err(e) => {
                    warn!(
                        "⚠️  Failed to decode existing keypair: {}. Generating new one.",
                        e
                    );
                    Keypair::generate_ed25519()
                }
            }
        } else {
            info!("🔑 Generating new Ed25519 keypair (no keypair in identity)");
            Keypair::generate_ed25519()
        }
    } else {
        info!("🔑 Generating new Ed25519 keypair for libp2p");
        Keypair::generate_ed25519()
    };

    // 3. Derive deterministic node identity from PeerId
    let peer_id = libp2p::PeerId::from(keypair.public());
    let mut identity = NodeIdentity::from_peer_id_bytes(&peer_id.to_bytes());

    // 4. Attach the keypair bytes to identity for persistence
    if let Ok(bytes) = keypair.to_protobuf_encoding() {
        identity.keypair = Some(bytes);
    }

    // 5. Save the consolidated identity (overwrites old random UUID if it existed)
    if let Err(e) = identity.save(&identity_path) {
        warn!(
            "⚠️  Failed to save consolidated identity to node_identity.json: {}",
            e
        );
    } else {
        info!("💾 Consolidated node identity saved to {:?}", identity_path);
        info!("✨ Node UUID (deterministic): {}", identity.uuid);
    }

    Ok((keypair, identity))
}

/// Convert seed nodes to prioritized multiaddr list.
/// Priority order: dns4 -> ip4 -> dns6 -> ip6
/// For hostnames: try dns4 first, then fallback to dns6
/// For IP addresses: use ip4/ip6 directly
fn convert_seed_nodes_to_multiaddrs(seed_nodes: &[String]) -> Vec<Multiaddr> {
    use std::net::IpAddr;

    let mut multiaddrs = Vec::new();

    for node in seed_nodes {
        // Handle IP:port or Host:port format (e.g., "192.168.1.100:9580" or "cameodb-node2:9580" or "[::1]:9580")
        // Use rsplit_once to correctly handle IPv6 addresses that contain colons
        if let Some((host, port)) = node.rsplit_once(':') {
            if let Ok(port_num) = port.parse::<u16>() {
                // Strip brackets if present (common for IPv6 literals)
                let clean_host = if host.starts_with('[') && host.ends_with(']') {
                    &host[1..host.len() - 1]
                } else {
                    host
                };

                match clean_host.parse::<IpAddr>() {
                    // For IP addresses, use ip4/ip6 directly (priority 2 and 4)
                    Ok(IpAddr::V4(_)) => {
                        let addr = format!("/ip4/{}/tcp/{}", clean_host, port_num);
                        if let Ok(ma) = addr.parse::<Multiaddr>() {
                            info!("✅ Converted bootstrap node {} to {}", node, ma);
                            multiaddrs.push(ma);
                        }
                    }
                    Ok(IpAddr::V6(_)) => {
                        let addr = format!("/ip6/{}/tcp/{}", clean_host, port_num);
                        if let Ok(ma) = addr.parse::<Multiaddr>() {
                            info!("✅ Converted bootstrap node {} to {}", node, ma);
                            multiaddrs.push(ma);
                        }
                    }
                    // For hostnames, prioritize dns4 first (priority 1), then fallback to dns6 (priority 3)
                    Err(_) => {
                        // Try dns4 first (priority 1)
                        let addr4 = format!("/dns4/{}/tcp/{}", clean_host, port_num);
                        if let Ok(ma) = addr4.parse::<Multiaddr>() {
                            info!("✅ Bootstrap node {} as dns4: {}", node, ma);
                            multiaddrs.push(ma);
                        } else {
                            // Fallback to dns6 (priority 3)
                            let addr6 = format!("/dns6/{}/tcp/{}", clean_host, port_num);
                            if let Ok(ma) = addr6.parse::<Multiaddr>() {
                                info!("✅ Bootstrap node {} as dns6: {}", node, ma);
                                multiaddrs.push(ma);
                            } else {
                                warn!(
                                    "⚠️  Failed to create multiaddr for bootstrap node '{}'",
                                    node
                                );
                            }
                        }
                    }
                }
            } else {
                warn!("⚠️  Invalid port in bootstrap node '{}': {}", node, port);
            }
        } else {
            // Try to parse as full multiaddr (backward compatibility)
            match node.parse::<Multiaddr>() {
                Ok(addr) => {
                    info!("✅ Using full multiaddr bootstrap node: {}", addr);
                    multiaddrs.push(addr);
                }
                Err(e) => {
                    warn!("⚠️  Invalid bootstrap node format '{}': {}", node, e);
                }
            }
        }
    }

    multiaddrs
}

fn launch_swarm_runtime(
    mut swarm: libp2p::Swarm<DhtBehaviour>,
    event_tx: UnboundedSender<CoordinatorEvent>,
    mut cmd_rx: UnboundedReceiver<SwarmCommand>,
    cmd_tx: UnboundedSender<SwarmCommand>,
) -> SwarmRuntimeHandle {
    let (shutdown_signal_tx, mut shutdown_signal_rx) = watch::channel(SwarmControl::Run);

    tokio::spawn(async move {
        info!("🔄 Swarm runtime task started");
        let mut metrics = SwarmRuntimeMetrics::default();
        let mut peer_book = PeerBook::default();

        loop {
            select! {
                _ = shutdown_signal_rx.changed() => {
                    if matches!(*shutdown_signal_rx.borrow(), SwarmControl::Shutdown) {
                        info!("🛑 Swarm shutdown signal received");
                        break;
                    }
                }
                Some(cmd) = cmd_rx.recv() => {
                    handle_swarm_command(cmd, &mut swarm);
                }
                event = swarm.select_next_some() => {
                    metrics.total_events += 1;
                    handle_swarm_event(event, &mut metrics, &event_tx, &mut swarm, &mut peer_book);
                }
            }
        }

        metrics.log_summary();
        info!("✅ Swarm runtime task completed");
    });

    SwarmRuntimeHandle::new(shutdown_signal_tx, cmd_tx)
}

/// Commands that can be sent to the swarm runtime
#[derive(Debug)]
pub enum SwarmCommand {
    PublishShards {
        node_uuid: Uuid,
        node_name: String,
        shards: Vec<crate::cluster_coordinator::ShardMetadata>,
        generation: u64,
        checksum: u64,
    },
    QueryNodeMetadata {
        node_uuid: Uuid,
    },
    #[allow(dead_code)]
    QueryShard {
        node_uuid: Uuid,
        shard_id: Uuid,
    },
}

fn handle_swarm_command(cmd: SwarmCommand, swarm: &mut libp2p::Swarm<DhtBehaviour>) {
    match cmd {
        SwarmCommand::PublishShards {
            node_uuid,
            node_name,
            shards,
            generation,
            checksum,
        } => {
            if let Err(e) = swarm
                .behaviour_mut()
                .publish_shards(node_uuid, node_name, &shards, generation, checksum)
            {
                warn!("⚠️  Failed to publish shards to DHT: {}", e);
            }
        }
        SwarmCommand::QueryNodeMetadata { node_uuid } => {
            swarm.behaviour_mut().query_node_metadata(node_uuid);
        }
        SwarmCommand::QueryShard {
            node_uuid,
            shard_id,
        } => {
            swarm
                .behaviour_mut()
                .query_shard_metadata(node_uuid, shard_id);
        }
    }
}

fn handle_swarm_event(
    event: SwarmEvent<DhtBehaviourEvent>,
    metrics: &mut SwarmRuntimeMetrics,
    event_tx: &UnboundedSender<CoordinatorEvent>,
    swarm: &mut libp2p::Swarm<DhtBehaviour>,
    peer_book: &mut PeerBook,
) {
    match event {
        SwarmEvent::Behaviour(behaviour_event) => {
            metrics.behaviour_events += 1;
            handle_behaviour_event(behaviour_event, metrics, event_tx, swarm, peer_book);
        }
        SwarmEvent::NewListenAddr { address, .. } => {
            info!("🎧 Swarm listening on: {}", address);
        }
        SwarmEvent::ExpiredListenAddr { address, .. } => {
            warn!("⚠️ Listen address expired: {}", address);
        }
        SwarmEvent::ConnectionEstablished {
            peer_id,
            established_in,
            endpoint,
            ..
        } => {
            metrics.connections_established += 1;
            let addr = Some(endpoint.get_remote_address().to_string());
            peer_book
                .addr_by_peer
                .insert(peer_id.to_string(), addr.clone().unwrap_or_default());
            let _ = event_tx.send(CoordinatorEvent::PeerDiscovered {
                peer_id: peer_id.to_string(),
                address: addr,
            });
            info!(
                "🔗 Connection established with {} ({} ms)",
                peer_id,
                established_in.as_millis()
            );

            // Trigger bootstrap on first non-self peer connection
            if !metrics.bootstrapped && peer_id != *swarm.local_peer_id() {
                let kad_has_peer = {
                    let kad = &mut swarm.behaviour_mut().kademlia;
                    kad.kbuckets().any(|b| !b.is_empty())
                };

                if kad_has_peer {
                    match swarm.behaviour_mut().bootstrap_kademlia() {
                        Ok(_) => {
                            metrics.bootstrapped = true;
                            info!(
                                "🚀 Kademlia DHT bootstrap triggered on first peer connect ({})",
                                peer_id
                            );
                        }
                        Err(e) => {
                            warn!(
                                "⚠️  Deferred bootstrap failed on peer connect {}: {}",
                                peer_id, e
                            );
                        }
                    }
                } else {
                    info!("⌛ Deferring bootstrap: Kademlia has no known peers yet");
                }
            }
        }
        SwarmEvent::ConnectionClosed { peer_id, cause, .. } => {
            metrics.connections_closed += 1;
            info!("🔒 Connection closed with {} ({:?})", peer_id, cause);
            let node_uuid = peer_book.uuid_by_peer.remove(&peer_id.to_string());
            let address = peer_book.addr_by_peer.remove(&peer_id.to_string());
            let _ = event_tx.send(CoordinatorEvent::PeerLost {
                peer_id: peer_id.to_string(),
                node_uuid,
                address,
            });
        }
        SwarmEvent::Dialing {
            peer_id,
            connection_id,
        } => match peer_id {
            Some(peer) => debug!("📞 Dialing peer: {} (conn {:?})", peer, connection_id),
            None => debug!("📞 Dialing new peer address (conn {:?})", connection_id),
        },
        SwarmEvent::IncomingConnection {
            local_addr,
            send_back_addr,
            connection_id,
        } => {
            info!(
                "📥 Incoming connection on {} from {} (conn {:?})",
                local_addr, send_back_addr, connection_id
            );
        }
        SwarmEvent::IncomingConnectionError {
            local_addr,
            send_back_addr,
            connection_id,
            error,
            peer_id,
        } => {
            warn!(
                "⚠️ Incoming connection error on {} from {:?} (conn {:?}, peer {:?}): {}",
                local_addr, send_back_addr, connection_id, peer_id, error
            );
        }
        SwarmEvent::OutgoingConnectionError {
            peer_id,
            connection_id,
            error,
        } => {
            warn!(
                "⚠️ Outgoing connection error to {:?} (conn {:?}): {}",
                peer_id, connection_id, error
            );
            let _ = event_tx.send(CoordinatorEvent::DialFailed {
                peer_id: peer_id.map(|p| p.to_string()),
                error: error.to_string(),
            });
        }
        SwarmEvent::ListenerClosed {
            listener_id,
            addresses,
            reason,
            ..
        } => {
            warn!(
                "⚠️ Listener {:?} closed: {:?} (addresses: {:?})",
                listener_id, reason, addresses
            );
        }
        SwarmEvent::ListenerError { listener_id, error } => {
            warn!("⚠️ Listener {:?} error: {}", listener_id, error);
        }
        other => {
            debug!("📡 Swarm event: {:?}", other);
        }
    }
}

fn handle_behaviour_event(
    event: DhtBehaviourEvent,
    metrics: &mut SwarmRuntimeMetrics,
    event_tx: &UnboundedSender<CoordinatorEvent>,
    swarm: &mut libp2p::Swarm<DhtBehaviour>,
    peer_book: &mut PeerBook,
) {
    match event {
        DhtBehaviourEvent::Kademlia(kad_event) => {
            handle_kademlia_event(kad_event, metrics, event_tx, swarm, peer_book)
        }
        DhtBehaviourEvent::Kameo(kameo_event) => {
            handle_kameo_event(kameo_event, swarm);
        }
        DhtBehaviourEvent::Identify(identify_event) => {
            handle_identify_event(identify_event, metrics, event_tx, swarm, peer_book);
        }
    }
}

fn handle_kademlia_event(
    event: kad::Event,
    metrics: &mut SwarmRuntimeMetrics,
    event_tx: &UnboundedSender<CoordinatorEvent>,
    swarm: &mut libp2p::Swarm<DhtBehaviour>,
    peer_book: &mut PeerBook,
) {
    match event {
        kad::Event::RoutingUpdated {
            peer, addresses, ..
        } => {
            metrics.kademlia_updates += 1;
            let addr_vec: Vec<_> = addresses.iter().cloned().collect();
            let addr_count = addr_vec.len();
            info!(
                "🛰️  Routing table updated for {} ({} addresses)",
                peer, addr_count
            );

            // Add addresses to Kademlia routing table
            for addr in &addr_vec {
                swarm
                    .behaviour_mut()
                    .kademlia
                    .add_address(&peer, addr.clone());
            }

            // Dial the peer to establish connection for Kameo actor communication
            // Skip self-dialing
            if peer != *swarm.local_peer_id() {
                if let Some(addr) = select_preferred_address(&addr_vec) {
                    match swarm.dial(addr.clone()) {
                        Ok(_) => {
                            peer_book
                                .addr_by_peer
                                .insert(peer.to_string(), addr.to_string());
                            info!("📞 Dialing Kademlia-discovered peer: {} at {}", peer, addr);
                        }
                        Err(e) => {
                            debug!("⚠️  Failed to dial peer {}: {}", peer, e);
                        }
                    }
                }
            } else {
                debug!("⏭️  Skipping self-dial to local peer: {}", peer);
            }

            let _ = event_tx.send(CoordinatorEvent::RoutingUpdated {
                peer_id: peer.to_string(),
                address_count: addr_count,
            });
        }
        kad::Event::OutboundQueryProgressed {
            id, result, stats, ..
        } => match result {
            kad::QueryResult::GetRecord(Ok(kad::GetRecordOk::FoundRecord(kad::PeerRecord {
                record,
                ..
            }))) => {
                let key_str = String::from_utf8_lossy(record.key.as_ref());
                if key_str.starts_with("cameodb-peer-") {
                    let peer_id_str = key_str.trim_start_matches("cameodb-peer-");
                    let uuid_str = String::from_utf8_lossy(&record.value);

                    peer_book
                        .uuid_by_peer
                        .insert(peer_id_str.to_string(), uuid_str.to_string());

                    info!(
                        "🎯 DHT Record Found: Peer {} -> UUID {}",
                        peer_id_str, uuid_str
                    );

                    let _ = event_tx.send(CoordinatorEvent::PeerUuidDiscovered {
                        peer_id: peer_id_str.to_string(),
                        node_uuid: uuid_str.to_string(),
                        address: None,
                    });
                } else if key_str.starts_with("cameodb-node-") {
                    match serde_json::from_slice::<crate::swarm::behaviour::NodeMetadata>(
                        &record.value,
                    ) {
                        Ok(metadata) => {
                            info!(
                                "🎯 DHT Node Metadata Found: Node {} -> {} shards, gen={}, storage={} docs={}",
                                metadata.node_uuid,
                                metadata.shard_count,
                                metadata.generation,
                                metadata.total_storage_bytes,
                                metadata.total_document_count
                            );

                            let _ = event_tx.send(CoordinatorEvent::PeerNodeMetadataDiscovered {
                                node_uuid: metadata.node_uuid.to_string(),
                                node_name: metadata.node_name,
                                shard_count: metadata.shard_count,
                                generation: metadata.generation,
                                checksum: metadata.checksum,
                                address: metadata.address,
                                status: metadata.status,
                                total_storage_bytes: metadata.total_storage_bytes,
                                total_document_count: metadata.total_document_count,
                            });
                        }
                        Err(e) => {
                            warn!("Failed to deserialize node metadata: {}", e);
                        }
                    }
                } else if key_str.starts_with("cameodb-shard-") {
                    let parts: Vec<&str> = key_str.split('-').collect();

                    if parts.len() >= 4 {
                        match serde_json::from_slice::<crate::cluster_coordinator::ShardMetadata>(
                            &record.value,
                        ) {
                            Ok(shard) => {
                                info!(
                                    "🎯 DHT Shard Found: Node {} -> Shard {} ({})",
                                    shard.node_id, shard.shard_id, shard.document_count
                                );

                                let _ = event_tx.send(CoordinatorEvent::PeerShardDiscovered {
                                    node_uuid: shard.node_id.to_string(),
                                    shard,
                                });
                            }
                            Err(e) => {
                                warn!("Failed to deserialize shard metadata: {}", e);
                            }
                        }
                    }
                }
            }
            _ => {
                debug!(
                    "📊 Kademlia query {:?} progressed: result={:?}, stats={:?}",
                    id, result, stats
                );
            }
        },
        kad::Event::InboundRequest { request } => {
            debug!("📨 Kademlia inbound request: {:?}", request);
        }
        other => {
            debug!("📡 Kademlia event: {:?}", other);
        }
    }
}

fn handle_kameo_event(event: kameo::remote::Event, _swarm: &mut libp2p::Swarm<DhtBehaviour>) {
    use kameo::remote::Event;

    match event {
        Event::Registry(registry_event) => {
            debug!("📡 Kameo registry event: {:?}", registry_event);
        }
        Event::Messaging(msg_event) => {
            debug!("📬 Kameo messaging event: {:?}", msg_event);
        }
    }
}

fn handle_identify_event(
    event: identify::Event,
    _metrics: &mut SwarmRuntimeMetrics,
    event_tx: &UnboundedSender<CoordinatorEvent>,
    swarm: &mut libp2p::Swarm<DhtBehaviour>,
    peer_book: &mut PeerBook,
) {
    if let identify::Event::Received {
        peer_id,
        info,
        connection_id: _,
    } = event
    {
        info!(
            "🆔 Identify: Received info from peer {} ({} addrs, agent: {})",
            peer_id,
            info.listen_addrs.len(),
            info.agent_version
        );

        // Add discovered addresses to Kademlia routing table
        for addr in &info.listen_addrs {
            info!("   - Address: {}", addr);
            swarm
                .behaviour_mut()
                .kademlia
                .add_address(&peer_id, addr.clone());
        }

        // Try to extract Node name and UUID from agent version string: "cameodb/1.0.0/{NAME}/{UUID}"
        let parts: Vec<&str> = info.agent_version.split('/').collect();
        if parts.len() >= 4 {
            let node_name = parts[2];
            let uuid_str = parts[3];
            if let Ok(uuid) = uuid::Uuid::parse_str(uuid_str) {
                peer_book
                    .uuid_by_peer
                    .insert(peer_id.to_string(), uuid.to_string());
                if let Some(addr) = select_preferred_address(&info.listen_addrs) {
                    peer_book
                        .addr_by_peer
                        .insert(peer_id.to_string(), addr.to_string());
                }

                info!(
                    "✨ Discovered Node identity from Identify protocol: {} ({})",
                    node_name, uuid
                );

                // Trigger peer resolution immediately without waiting for DHT
                let _ = event_tx.send(CoordinatorEvent::PeerUuidDiscovered {
                    peer_id: peer_id.to_string(),
                    node_uuid: uuid.to_string(),
                    address: select_preferred_address(&info.listen_addrs).map(|a| a.to_string()),
                });
            } else {
                warn!("⚠️  Invalid UUID in agent version: {}", uuid_str);
            }
        } else if parts.len() >= 3 {
            // Fallback for old format without node name: "cameodb/1.0.0/{UUID}"
            let uuid_str = parts[2];
            if let Ok(uuid) = uuid::Uuid::parse_str(uuid_str) {
                peer_book
                    .uuid_by_peer
                    .insert(peer_id.to_string(), uuid.to_string());
                if let Some(addr) = select_preferred_address(&info.listen_addrs) {
                    peer_book
                        .addr_by_peer
                        .insert(peer_id.to_string(), addr.to_string());
                }

                info!(
                    "✨ Discovered Node UUID from Identify protocol (legacy format): {}",
                    uuid
                );

                let _ = event_tx.send(CoordinatorEvent::PeerUuidDiscovered {
                    peer_id: peer_id.to_string(),
                    node_uuid: uuid.to_string(),
                    address: select_preferred_address(&info.listen_addrs).map(|a| a.to_string()),
                });
            } else {
                warn!("⚠️  Invalid UUID in agent version: {}", uuid_str);
            }
        }

        // Fallback: Query the peer's UUID from the DHT (just in case Identify didn't have it or parsing failed)
        // This is now redundant if Identify succeeds, but harmless as a backup.
        swarm.behaviour_mut().query_peer_uuid(&peer_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_convert_seed_nodes() {
        let inputs = vec![
            "127.0.0.1:9580".to_string(),
            "cameodb-node2:9580".to_string(),
            "[::1]:9580".to_string(),
            "192.168.1.50:4000".to_string(),
            "/ip4/10.0.0.1/tcp/8000".to_string(), // Direct multiaddr
        ];

        let results = convert_seed_nodes_to_multiaddrs(&inputs);

        assert_eq!(results.len(), 5);
        assert_eq!(results[0].to_string(), "/ip4/127.0.0.1/tcp/9580");
        assert_eq!(results[1].to_string(), "/dns4/cameodb-node2/tcp/9580");
        assert_eq!(results[2].to_string(), "/ip6/::1/tcp/9580");
        assert_eq!(results[3].to_string(), "/ip4/192.168.1.50/tcp/4000");
        assert_eq!(results[4].to_string(), "/ip4/10.0.0.1/tcp/8000");
    }

    #[test]
    fn test_convert_invalid_nodes() {
        let inputs = vec![
            "invalid:port".to_string(), // Invalid port
            "nodoport".to_string(),     // No port
        ];

        let results = convert_seed_nodes_to_multiaddrs(&inputs);
        assert_eq!(results.len(), 0);
    }
}
