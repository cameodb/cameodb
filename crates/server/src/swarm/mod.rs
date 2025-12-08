//! Custom Swarm Orchestrator & Event Loop for CameoDB Distributed Database
//!
//! This module implements the main orchestration logic for the custom libp2p swarm,
//! following the ANOTHER APPROACH architecture. It provides the entry point for
//! swarm initialization and manages the event loop processing.

pub mod behaviour;
pub mod cluster_actor;
pub mod utils;

use anyhow::Result;
use futures::StreamExt;
use libp2p::{
    Multiaddr, PeerId, SwarmBuilder, identity::Keypair, kad, noise, swarm::SwarmEvent, tcp, yamux,
};
use std::time::Duration;
use tokio::{select, sync::watch};
use tracing::{debug, info, warn};

use crate::config::ClusterConfig;
use behaviour::{DhtBehaviour, DhtBehaviourEvent};
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
}

/// Handle used to manage the background swarm runtime task
#[derive(Debug, Clone)]
pub struct SwarmRuntimeHandle {
    shutdown_tx: Option<watch::Sender<SwarmControl>>,
}

impl SwarmRuntimeHandle {
    fn new(shutdown_tx: watch::Sender<SwarmControl>) -> Self {
        Self {
            shutdown_tx: Some(shutdown_tx),
        }
    }

    fn inert() -> Self {
        Self { shutdown_tx: None }
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
pub async fn init_distributed_swarm(config: &ClusterConfig) -> Result<SwarmStartup> {
    if !config.distributed_actors {
        info!("Distributed actors disabled, running in single-node mode");
        return Ok(SwarmStartup {
            peer_id: PeerId::random(),
            listen_addr: "/ip4/127.0.0.1/tcp/0".parse().unwrap(),
            bootstrap_peer_count: 0,
            runtime: SwarmRuntimeHandle::inert(),
        });
    }

    info!("🚀 Initializing distributed libp2p swarm");

    // Create production-ready swarm with Kademlia DHT
    let startup = create_production_swarm(config).await?;

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
async fn create_production_swarm(config: &ClusterConfig) -> Result<SwarmStartup> {
    // Generate cryptographic identity for this node
    let keypair = Keypair::generate_ed25519();
    let peer_id = PeerId::from(keypair.public());

    info!("🔐 Generated node identity: {}", peer_id);

    // Get optimized listen address using smart interface binding
    let listen_addr = get_preferred_listen_address(config.cluster_port)?;

    // Create custom network behaviour with production settings
    let behaviour = DhtBehaviour::new(
        peer_id,
        Some(libp2p::kad::Mode::Server), // Server mode for stable operation
    )?;

    info!("🏗️  Created Kademlia DHT behaviour for peer discovery");

    // Build the libp2p swarm with full transport stack
    let mut swarm = SwarmBuilder::with_existing_identity(keypair)
        .with_tokio()
        .with_tcp(
            tcp::Config::default().port_reuse(true).nodelay(true),
            noise::Config::new,
            yamux::Config::default,
        )?
        .with_quic() // Add QUIC support for better connectivity
        .with_behaviour(|_key| Ok(behaviour))?
        .with_swarm_config(|c| {
            c.with_idle_connection_timeout(Duration::from_secs(300))
                .with_max_negotiating_inbound_streams(2048)
        })
        .build();

    // Start listening on the optimized address
    swarm.listen_on(listen_addr.clone())?;
    info!("🎧 Swarm listening on: {}", listen_addr);

    // Connect to bootstrap peers for DHT initialization
    let bootstrap_addrs = convert_bootstrap_nodes_to_multiaddrs(&config.bootstrap_nodes);
    let mut connected_peers = 0;

    for addr in bootstrap_addrs {
        info!("📞 Connecting to bootstrap peer: {}", addr);
        match swarm.dial(addr.clone()) {
            Ok(_) => {
                connected_peers += 1;
                info!("✅ Dialing bootstrap peer: {}", addr);
            }
            Err(e) => {
                warn!("⚠️  Failed to dial bootstrap peer {}: {}", addr, e);
            }
        }
    }

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
    let runtime = launch_swarm_runtime(swarm);

    Ok(SwarmStartup {
        peer_id,
        listen_addr,
        bootstrap_peer_count: connected_peers,
        runtime,
    })
}

/// Convert IP:port format bootstrap nodes to full multiaddr format
fn convert_bootstrap_nodes_to_multiaddrs(bootstrap_nodes: &[String]) -> Vec<Multiaddr> {
    let mut multiaddrs = Vec::new();

    for node in bootstrap_nodes {
        // Handle IP:port format (e.g., "192.168.1.100:9580")
        if let Some((ip, port)) = node.split_once(':') {
            if let Ok(port_num) = port.parse::<u16>() {
                let multiaddr_str = format!("/ip4/{}/tcp/{}", ip, port_num);
                match multiaddr_str.parse::<Multiaddr>() {
                    Ok(addr) => {
                        info!("✅ Converted bootstrap node {} to {}", node, addr);
                        multiaddrs.push(addr);
                    }
                    Err(e) => {
                        warn!("⚠️  Failed to parse bootstrap node '{}': {}", node, e);
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

fn launch_swarm_runtime(mut swarm: libp2p::Swarm<DhtBehaviour>) -> SwarmRuntimeHandle {
    let (shutdown_tx, mut shutdown_rx) = watch::channel(SwarmControl::Run);

    tokio::spawn(async move {
        info!("🔄 Swarm runtime task started");
        let mut metrics = SwarmRuntimeMetrics::default();

        loop {
            select! {
                _ = shutdown_rx.changed() => {
                    if matches!(*shutdown_rx.borrow(), SwarmControl::Shutdown) {
                        info!("🛑 Swarm shutdown signal received");
                        break;
                    }
                }
                event = swarm.select_next_some() => {
                    metrics.total_events += 1;
                    handle_swarm_event(event, &mut metrics);
                }
            }
        }

        metrics.log_summary();
        info!("✅ Swarm runtime task completed");
    });

    SwarmRuntimeHandle::new(shutdown_tx)
}

fn handle_swarm_event(event: SwarmEvent<DhtBehaviourEvent>, metrics: &mut SwarmRuntimeMetrics) {
    match event {
        SwarmEvent::Behaviour(behaviour_event) => {
            metrics.behaviour_events += 1;
            handle_behaviour_event(behaviour_event, metrics);
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
            ..
        } => {
            metrics.connections_established += 1;
            info!(
                "🔗 Connection established with {} ({} ms)",
                peer_id,
                established_in.as_millis()
            );
        }
        SwarmEvent::ConnectionClosed { peer_id, cause, .. } => {
            metrics.connections_closed += 1;
            info!("� Connection closed with {} ({:?})", peer_id, cause);
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
        } => {
            warn!(
                "⚠️ Incoming connection error on {} from {:?} (conn {:?}): {}",
                local_addr, send_back_addr, connection_id, error
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

fn handle_behaviour_event(event: DhtBehaviourEvent, metrics: &mut SwarmRuntimeMetrics) {
    match event {
        DhtBehaviourEvent::Kademlia(kad_event) => handle_kademlia_event(kad_event, metrics),
    }
}

fn handle_kademlia_event(event: kad::Event, metrics: &mut SwarmRuntimeMetrics) {
    match event {
        kad::Event::RoutingUpdated {
            peer, addresses, ..
        } => {
            metrics.kademlia_updates += 1;
            let addr_count = addresses.len();
            info!(
                "🛰️  Routing table updated for {} ({} addresses)",
                peer, addr_count
            );
        }
        kad::Event::OutboundQueryProgressed {
            id, result, stats, ..
        } => {
            debug!(
                "📊 Kademlia query {:?} progressed: result={:?}, stats={:?}",
                id, result, stats
            );
        }
        kad::Event::InboundRequest { request } => {
            debug!("📨 Kademlia inbound request: {:?}", request);
        }
        other => {
            debug!("📡 Kademlia event: {:?}", other);
        }
    }
}
