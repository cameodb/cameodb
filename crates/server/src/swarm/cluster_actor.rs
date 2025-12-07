//! Cluster State Actor - Source of Truth for Active Peers
//!
//! This actor maintains the distributed cluster state, tracking discovered
//! peers, connection status, and providing queries for cluster information.

use kameo::Actor;
use libp2p::{Multiaddr, PeerId};
use std::collections::{HashMap, HashSet};
use std::time::Instant;
use tracing::{debug, info};

/// Message to notify about a newly discovered peer
#[derive(Debug, Clone)]
pub struct PeerDiscovered {
    pub peer_id: PeerId,
    pub addr: Multiaddr,
}

/// Message to notify about a lost peer connection
#[derive(Debug, Clone)]
pub struct PeerLost {
    pub peer_id: PeerId,
}

/// Query message to get all currently active peers
#[derive(Debug, Clone)]
pub struct GetActivePeers;

/// Information about a discovered peer
#[derive(Debug, Clone)]
pub struct PeerInfo {
    pub peer_id: PeerId,
    pub addresses: Vec<Multiaddr>,
    pub discovered_at: Instant,
    pub last_seen: Instant,
}

/// Actor that maintains the cluster state and peer information
#[derive(Actor)]
pub struct ClusterStateActor {
    /// Set of currently active peer IDs
    peers: HashSet<PeerId>,

    /// Detailed information about each peer
    peer_info: HashMap<PeerId, PeerInfo>,

    /// When this node started
    started_at: Instant,
}

impl Default for ClusterStateActor {
    fn default() -> Self {
        Self::new()
    }
}

impl ClusterStateActor {
    /// Create a new cluster state actor
    pub fn new() -> Self {
        let started_at = Instant::now();
        info!("🏗️  Initializing ClusterStateActor");

        Self {
            peers: HashSet::new(),
            peer_info: HashMap::new(),
            started_at,
        }
    }

    /// Get the current number of active peers
    pub fn peer_count(&self) -> usize {
        self.peers.len()
    }

    /// Get uptime of this cluster node
    pub fn uptime(&self) -> std::time::Duration {
        self.started_at.elapsed()
    }
}

impl ClusterStateActor {
    /// Handle a peer discovered event
    pub fn handle_peer_discovered(&mut self, peer_id: PeerId, addr: Multiaddr) {
        let now = Instant::now();

        if self.peers.insert(peer_id) {
            // New peer discovered
            info!("🆕 New peer registered: {} at {}", peer_id, addr);

            let peer_info = PeerInfo {
                peer_id,
                addresses: vec![addr.clone()],
                discovered_at: now,
                last_seen: now,
            };

            self.peer_info.insert(peer_id, peer_info);
        } else {
            // Existing peer, update last seen and potentially add new address
            if let Some(info) = self.peer_info.get_mut(&peer_id) {
                info.last_seen = now;

                // Add address if not already present
                if !info.addresses.contains(&addr) {
                    info.addresses.push(addr.clone());
                    debug!("📍 Added new address for {}: {}", peer_id, addr);
                }
            }
        }

        info!("📊 Cluster status: {} active peers", self.peers.len());
    }

    /// Handle a peer lost event
    pub fn handle_peer_lost(&mut self, peer_id: PeerId) {
        if self.peers.remove(&peer_id) {
            self.peer_info.remove(&peer_id);
            info!("🔌 Peer disconnected: {}", peer_id);
            info!("📊 Cluster status: {} active peers", self.peers.len());
        } else {
            debug!("⚠️  Attempted to remove unknown peer: {}", peer_id);
        }
    }

    /// Get information about active peers  
    pub fn get_active_peers(&self) -> Vec<PeerInfo> {
        let active_peers: Vec<PeerInfo> = self.peer_info.values().cloned().collect();

        debug!("📋 Current cluster has {} active peers", active_peers.len());
        for peer in &active_peers {
            debug!(
                "  🔗 Peer: {} (discovered: {:?} ago)",
                peer.peer_id,
                peer.discovered_at.elapsed()
            );
        }

        active_peers
    }
}

// Note: Kameo Handler implementations will be added in Phase 4-6
// For now, the cluster state is managed through direct method calls
