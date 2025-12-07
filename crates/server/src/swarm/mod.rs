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
    Multiaddr, PeerId, SwarmBuilder, identity::Keypair, noise, swarm::SwarmEvent, tcp, yamux,
};
use std::time::Duration;
use tokio::select;
use tracing::{debug, info, warn};

use crate::config::ClusterConfig;
use behaviour::DhtBehaviour;
// TODO: Add cluster state actor integration for peer management
// use cluster_actor::{ClusterStateActor, PeerDiscovered, PeerLost};
// use kameo::prelude::{ActorRef, spawn};

// Re-export key types for convenience
// TODO: Enable cluster actor exports when integration is complete
// pub use cluster_actor::{GetActivePeers, PeerInfo};
pub use utils::get_preferred_listen_address;

/// Initialize the distributed swarm for peer-to-peer communication
pub async fn init_distributed_swarm(config: &ClusterConfig) -> Result<String> {
    if !config.distributed_actors {
        info!("Distributed actors disabled, running in single-node mode");
        return Ok("single-node-mode".to_string());
    }

    info!("🚀 Initializing distributed libp2p swarm");

    // Create production-ready swarm with Kademlia DHT
    let swarm_result = create_production_swarm(config).await?;

    info!("✅ Production swarm initialized successfully");
    info!("   📡 Peer ID: {}", swarm_result.peer_id);
    info!("   🎧 Listen Address: {}", swarm_result.listen_addr);
    info!("   🚀 Cluster Port: {} (from config)", config.cluster_port);
    info!("   🌐 Discovery: Kademlia DHT");
    info!(
        "   📊 Bootstrap Peers: {}",
        swarm_result.bootstrap_peer_count
    );

    // TODO: Future enhancements:
    // - Cluster state actor integration for peer management
    // - Enhanced event loop with distributed state synchronization

    Ok(swarm_result.peer_id.to_string())
}

/// Production swarm creation result
struct SwarmInitResult {
    peer_id: PeerId,
    listen_addr: Multiaddr,
    bootstrap_peer_count: usize,
    // TODO: Add cluster state actor reference
    // cluster_actor: ActorRef<ClusterStateActor>,
}

/// Create a production-ready libp2p swarm with custom behaviour
async fn create_production_swarm(config: &ClusterConfig) -> Result<SwarmInitResult> {
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

    // Spawn a basic event monitoring task (Phase 6 will expand this)
    spawn_basic_event_monitor(swarm).await;

    Ok(SwarmInitResult {
        peer_id,
        listen_addr,
        bootstrap_peer_count: connected_peers,
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

/// Spawn a basic swarm event monitor for network activity
async fn spawn_basic_event_monitor(mut swarm: libp2p::Swarm<DhtBehaviour>) {
    tokio::spawn(async move {
        info!("🔄 Starting basic swarm event monitor");

        let mut event_count = 0;
        let mut peer_discovery_count = 0;

        loop {
            select! {
                event = swarm.select_next_some() => {
                    event_count += 1;

                    match event {
                        SwarmEvent::Behaviour(event) => {
                            debug!("📨 Behaviour event #{}: {:?}", event_count, event);
                            // TODO: Phase 5 will add ClusterStateActor integration here
                        }
                        SwarmEvent::NewListenAddr { address, .. } => {
                            info!("🎧 Now listening on: {}", address);
                        }
                        SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                            peer_discovery_count += 1;
                            info!("🔗 Connected to peer #{}: {}", peer_discovery_count, peer_id);
                        }
                        SwarmEvent::ConnectionClosed { peer_id, cause, .. } => {
                            info!("🔌 Disconnected from peer: {} ({:?})", peer_id, cause);
                        }
                        _ => {
                            debug!("📡 Swarm event #{}: {:?}", event_count, event);
                        }
                    }
                }
                // TODO: Phase 6 will add shutdown signal handling
            }
        }
    });
}
