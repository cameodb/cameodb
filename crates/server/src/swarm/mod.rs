//! Custom Swarm Orchestrator & Event Loop for CameoDB Distributed Database
//!
//! This module implements the main orchestration logic for the custom libp2p swarm,
//! following the ANOTHER APPROACH architecture. It provides the entry point for
//! swarm initialization and manages the event loop processing.

pub mod behaviour;
pub mod cluster_actor;
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
use std::path::Path;
use std::time::Duration;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::{select, sync::watch};
use tracing::{debug, info, warn};
use uuid::Uuid;
// TODO: Add cluster state actor integration for peer management
// use cluster_actor::{ClusterStateActor, PeerDiscovered, PeerLost};
// use kameo::prelude::{ActorRef, spawn};

// Re-export key types for convenience
// TODO: Enable cluster actor exports when integration is complete
// pub use cluster_actor::{GetActivePeers, PeerInfo};
pub use utils::get_preferred_listen_address;

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
    PeerShardsDiscovered {
        peer_id: String,
        shards: Vec<crate::cluster_coordinator::ShardMetadata>,
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
        shards: Vec<crate::cluster_coordinator::ShardMetadata>,
    ) -> Result<()> {
        if let Some(tx) = &self.cmd_tx {
            tx.send(SwarmCommand::PublishShards { node_uuid, shards })
                .map_err(|_| anyhow::anyhow!("Swarm runtime channel closed"))?;
        }
        Ok(())
    }

    /// Query shards for a remote node from the DHT
    pub fn query_shards(&self, node_uuid: Uuid) -> Result<()> {
        if let Some(tx) = &self.cmd_tx {
            tx.send(SwarmCommand::QueryShards { node_uuid })
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

/// Initialize the distributed swarm for peer-to-peer communication
pub async fn init_distributed_swarm(
    config: &ClusterConfig,
    node_uuid: Uuid,
    storage_path: &Path,
) -> Result<SwarmStartup> {
    if !config.distributed_actors {
        info!("Distributed actors disabled, running in single-node mode");
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
    let startup = create_production_swarm(config, node_uuid, storage_path).await?;

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
    storage_path: &Path,
) -> Result<SwarmStartup> {
    // Load or generate cryptographic identity for this node
    let (keypair, _identity) = load_or_generate_keypair(storage_path)?;
    let peer_id = PeerId::from(keypair.public());

    info!("🔐 Node identity: {}", peer_id);

    // Get optimized listen address using smart interface binding
    let listen_addr = get_preferred_listen_address(config.cluster_port)?;

    // Create custom network behaviour with production settings
    let behaviour = DhtBehaviour::new(
        peer_id,
        Some(libp2p::kad::Mode::Server), // Server mode for stable operation
        keypair.public(),
    )?;

    info!("🏗️  Created Kademlia DHT behaviour for peer discovery");

    // Build the libp2p swarm with full transport stack including DNS
    let mut swarm = SwarmBuilder::with_existing_identity(keypair)
        .with_tokio()
        .with_tcp(
            tcp::Config::default().nodelay(true),
            noise::Config::new,
            yamux::Config::default,
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

    // Connect to bootstrap peers for DHT initialization
    let bootstrap_addrs = convert_bootstrap_nodes_to_multiaddrs(&config.bootstrap_nodes);
    let mut connected_peers = 0;

    info!(
        "🔍 Bootstrap configuration: {} nodes configured",
        config.bootstrap_nodes.len()
    );
    for node in &config.bootstrap_nodes {
        info!("   - Bootstrap node: {}", node);
    }

    for addr in bootstrap_addrs {
        info!("📞 Attempting to dial bootstrap peer: {}", addr);
        match swarm.dial(addr.clone()) {
            Ok(_) => {
                connected_peers += 1;
                info!("✅ Successfully initiated dial to: {}", addr);
            }
            Err(e) => {
                warn!("⚠️  Failed to dial bootstrap peer {}: {:?}", addr, e);
            }
        }
    }

    info!(
        "📊 Bootstrap dial summary: {} successful, {} total",
        connected_peers,
        config.bootstrap_nodes.len()
    );

    // Bootstrap Kademlia DHT if we have peers
    if connected_peers > 0 {
        match swarm.behaviour_mut().bootstrap_kademlia() {
            Ok(_) => info!(
                "🚀 Kademlia DHT bootstrap initiated with {} peers",
                connected_peers
            ),
            Err(e) => warn!("⚠️  Kademlia bootstrap failed: {}", e),
        }
    } else {
        info!("📋 No bootstrap peers available - running in standalone mode");
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
fn load_or_generate_keypair(storage_path: &Path) -> Result<(Keypair, NodeIdentity)> {
    let identity_path = storage_path.join("node_identity.json");

    // Load the identity (creates if doesn't exist, though NodeOrchestrator should have created it)
    let mut identity = NodeIdentity::load_or_create(identity_path.clone())
        .map_err(|e| anyhow::anyhow!("Failed to load node identity: {}", e))?;

    // Check if we have a valid keypair stored
    if let Some(key_bytes) = &identity.keypair {
        info!("🔑 Loading existing libp2p keypair from node_identity.json");
        match Keypair::from_protobuf_encoding(key_bytes) {
            Ok(kp) => return Ok((kp, identity)),
            Err(e) => {
                warn!(
                    "⚠️  Failed to decode existing keypair from identity: {}. Generating new one.",
                    e
                );
            }
        }
    }

    info!("🔑 Generating new Ed25519 keypair for libp2p");
    let keypair = Keypair::generate_ed25519();

    // Save the keypair to the identity file
    if let Ok(bytes) = keypair.to_protobuf_encoding() {
        identity.keypair = Some(bytes);
        if let Err(e) = identity.save(&identity_path) {
            warn!("⚠️  Failed to save keypair to node_identity.json: {}", e);
        } else {
            info!("💾 Saved new keypair to {:?}", identity_path);
        }
    }

    Ok((keypair, identity))
}

/// Convert IP:port format bootstrap nodes to full multiaddr format
fn convert_bootstrap_nodes_to_multiaddrs(bootstrap_nodes: &[String]) -> Vec<Multiaddr> {
    use std::net::IpAddr;

    let mut multiaddrs = Vec::new();

    for node in bootstrap_nodes {
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

                // Determine protocol based on whether host is an IP or DNS name
                let multiaddr_str = match clean_host.parse::<IpAddr>() {
                    Ok(IpAddr::V4(_)) => format!("/ip4/{}/tcp/{}", clean_host, port_num),
                    Ok(IpAddr::V6(_)) => format!("/ip6/{}/tcp/{}", clean_host, port_num),
                    Err(_) => format!("/dns/{}/tcp/{}", clean_host, port_num),
                };

                match multiaddr_str.parse::<Multiaddr>() {
                    Ok(addr) => {
                        info!("✅ Converted bootstrap node {} to {}", node, addr);
                        multiaddrs.push(addr);
                    }
                    Err(e) => {
                        warn!(
                            "⚠️  Failed to parse bootstrap node '{}' as multiaddr: {}",
                            node, e
                        );
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
                    handle_swarm_event(event, &mut metrics, &event_tx, &mut swarm);
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
        shards: Vec<crate::cluster_coordinator::ShardMetadata>,
    },
    QueryShards {
        node_uuid: Uuid,
    },
}

fn handle_swarm_command(cmd: SwarmCommand, swarm: &mut libp2p::Swarm<DhtBehaviour>) {
    match cmd {
        SwarmCommand::PublishShards { node_uuid, shards } => {
            if let Err(e) = swarm.behaviour_mut().publish_shards(node_uuid, &shards) {
                warn!("⚠️  Failed to publish shards to DHT: {}", e);
            }
        }
        SwarmCommand::QueryShards { node_uuid } => {
            swarm.behaviour_mut().query_shards(node_uuid);
        }
    }
}

fn handle_swarm_event(
    event: SwarmEvent<DhtBehaviourEvent>,
    metrics: &mut SwarmRuntimeMetrics,
    event_tx: &UnboundedSender<CoordinatorEvent>,
    swarm: &mut libp2p::Swarm<DhtBehaviour>,
) {
    match event {
        SwarmEvent::Behaviour(behaviour_event) => {
            metrics.behaviour_events += 1;
            handle_behaviour_event(behaviour_event, metrics, event_tx, swarm);
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
            let _ = event_tx.send(CoordinatorEvent::PeerDiscovered {
                peer_id: peer_id.to_string(),
                address: addr,
            });
            info!(
                "🔗 Connection established with {} ({} ms)",
                peer_id,
                established_in.as_millis()
            );
        }
        SwarmEvent::ConnectionClosed { peer_id, cause, .. } => {
            metrics.connections_closed += 1;
            info!("🔒 Connection closed with {} ({:?})", peer_id, cause);
            let _ = event_tx.send(CoordinatorEvent::PeerLost {
                peer_id: peer_id.to_string(),
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
) {
    match event {
        DhtBehaviourEvent::Kademlia(kad_event) => {
            handle_kademlia_event(kad_event, metrics, event_tx, swarm)
        }
        DhtBehaviourEvent::Kameo(kameo_event) => {
            handle_kameo_event(kameo_event, swarm);
        }
        DhtBehaviourEvent::Identify(identify_event) => {
            handle_identify_event(identify_event, metrics, swarm);
        }
    }
}

fn handle_kademlia_event(
    event: kad::Event,
    metrics: &mut SwarmRuntimeMetrics,
    event_tx: &UnboundedSender<CoordinatorEvent>,
    swarm: &mut libp2p::Swarm<DhtBehaviour>,
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
            // Use the first address from the routing update
            if let Some(addr) = addr_vec.first() {
                match swarm.dial(addr.clone()) {
                    Ok(_) => {
                        info!("📞 Dialing Kademlia-discovered peer: {} at {}", peer, addr);
                    }
                    Err(e) => {
                        debug!("⚠️  Failed to dial peer {}: {}", peer, e);
                    }
                }
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
                if key_str.starts_with("cameodb-uuid-") {
                    let peer_id_str = key_str.trim_start_matches("cameodb-uuid-");
                    let uuid_str = String::from_utf8_lossy(&record.value);

                    info!(
                        "🎯 DHT Record Found: Peer {} -> UUID {}",
                        peer_id_str, uuid_str
                    );

                    let _ = event_tx.send(CoordinatorEvent::PeerUuidDiscovered {
                        peer_id: peer_id_str.to_string(),
                        node_uuid: uuid_str.to_string(),
                        address: None,
                    });
                } else if key_str.starts_with("cameodb-shards-") {
                    let peer_id_str = key_str.trim_start_matches("cameodb-shards-");

                    match serde_json::from_slice::<Vec<crate::cluster_coordinator::ShardMetadata>>(
                        &record.value,
                    ) {
                        Ok(shards) => {
                            info!(
                                "🎯 DHT Shards Found: Peer {} -> {} shards",
                                peer_id_str,
                                shards.len()
                            );

                            let _ = event_tx.send(CoordinatorEvent::PeerShardsDiscovered {
                                peer_id: peer_id_str.to_string(),
                                shards,
                            });
                        }
                        Err(e) => {
                            warn!(
                                "⚠️  Failed to deserialize shards from DHT for peer {}: {}",
                                peer_id_str, e
                            );
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
    swarm: &mut libp2p::Swarm<DhtBehaviour>,
) {
    if let identify::Event::Received {
        peer_id,
        info,
        connection_id: _,
    } = event
    {
        info!(
            "🆔 Identify: Received info from peer {} ({} addrs)",
            peer_id,
            info.listen_addrs.len()
        );

        // Add discovered addresses to Kademlia routing table
        for addr in info.listen_addrs {
            info!("   - Address: {}", addr);
            swarm.behaviour_mut().kademlia.add_address(&peer_id, addr);
        }

        // Query the peer's UUID from the DHT to verify identity and trigger cluster join
        swarm.behaviour_mut().query_peer_uuid(&peer_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_convert_bootstrap_nodes() {
        let inputs = vec![
            "127.0.0.1:9580".to_string(),
            "cameodb-node2:9580".to_string(),
            "[::1]:9580".to_string(),
            "192.168.1.50:4000".to_string(),
            "/ip4/10.0.0.1/tcp/8000".to_string(), // Direct multiaddr
        ];

        let results = convert_bootstrap_nodes_to_multiaddrs(&inputs);

        assert_eq!(results.len(), 5);
        assert_eq!(results[0].to_string(), "/ip4/127.0.0.1/tcp/9580");
        assert_eq!(results[1].to_string(), "/dns/cameodb-node2/tcp/9580");
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

        let results = convert_bootstrap_nodes_to_multiaddrs(&inputs);
        assert_eq!(results.len(), 0);
    }
}
