//! Distributed CameoDB Implementation Using Kameo Actors
//!
//! This module implements distributed functionality for CameoDB using Kameo's
//! actor system with remote capabilities. It provides cluster bootstrap,
//! actor registration, and distributed routing functionality.

use anyhow::Result;
use kameo::Reply;
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::sync::mpsc::UnboundedReceiver;
use tracing::{info, warn};
use uuid::Uuid;

use crate::config::ClusterConfig;
use crate::node_orchestrator::MicroshardActor;
use crate::swarm::{self, CoordinatorEvent, SwarmRuntimeHandle, SwarmStartup};

/// Distributed cluster manager for CameoDB nodes
#[derive(Debug)]
pub struct DistributedCluster {
    /// Local node configuration
    pub cluster_config: ClusterConfig,
    /// Path for persistent storage (keys, etc.)
    pub storage_path: PathBuf,
    /// Map of known remote nodes
    pub peer_nodes: HashMap<Uuid, NodeInfo>,
    /// Local node identity
    pub local_node_id: Uuid,
    /// Local node name (3-char Base36)
    pub local_node_name: String,
    /// Handle to the running swarm runtime (if started)
    swarm_handle: Option<SwarmRuntimeHandle>,
    /// Count of successful bootstrap peer connections
    bootstrap_successes: u64,
    /// Count of dial/connect failures
    dial_failures: u64,
    /// Count of routing table update events
    routing_updates: u64,
}

/// Information about a peer node in the cluster
#[derive(Debug, Clone)]
pub struct NodeInfo {
    /// Unique node identifier
    pub node_id: Uuid,
    /// Human-readable node name (3-char Base36)
    pub node_name: Option<String>,
    /// Node's cluster address
    pub address: String,
    /// Node status (Connected, Disconnected, etc.)
    pub status: NodeStatus,
    /// Number of active shards on this node
    pub shard_count: usize,
}

impl NodeInfo {
    /// Format node identity as "NAME (UUID)" for human-readable display
    pub fn format_identity(&self) -> String {
        if let Some(name) = &self.node_name {
            format!("{} ({})", name, self.node_id)
        } else {
            self.node_id.to_string()
        }
    }
}

/// Status of a node in the cluster
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeStatus {
    Connected,
    Disconnected,
}

impl DistributedCluster {
    /// Create a new distributed cluster manager
    pub fn new(
        cluster_config: ClusterConfig,
        local_node_id: Uuid,
        local_node_name: String,
        storage_path: PathBuf,
    ) -> Self {
        Self {
            cluster_config,
            storage_path,
            peer_nodes: HashMap::new(),
            local_node_id,
            local_node_name,
            swarm_handle: None,
            bootstrap_successes: 0,
            dial_failures: 0,
            routing_updates: 0,
        }
    }

    /// Expose swarm handle for coordinator shutdown hooks
    pub fn swarm_handle(&self) -> Option<SwarmRuntimeHandle> {
        self.swarm_handle.clone()
    }

    /// Initialize the distributed swarm runtime and return the peer identity
    pub async fn init_swarm(
        &mut self,
    ) -> Result<(String, Option<UnboundedReceiver<CoordinatorEvent>>)> {
        info!(
            node_id = %self.local_node_id,
            cluster_name = %self.cluster_config.cluster_name,
            port = %self.cluster_config.cluster_port,
            "Initializing kameo distributed swarm"
        );

        // Initialize distributed swarm for peer communication
        let SwarmStartup {
            peer_id,
            listen_addr,
            bootstrap_peer_count,
            runtime,
            events,
        } = swarm::init_distributed_swarm(
            &self.cluster_config,
            self.local_node_id,
            self.local_node_name.clone(),
            &self.storage_path,
        )
        .await?;

        self.swarm_handle = Some(runtime);
        self.bootstrap_successes += bootstrap_peer_count as u64;

        info!("🌐 Kameo Distributed Framework Ready:");
        info!("  📡 Cluster Port: {}", self.cluster_config.cluster_port);
        info!("  🔍 Discovery: Kademlia DHT");
        info!("  🏷️  Cluster Name: {}", self.cluster_config.cluster_name);
        info!("  🎧 Listen Address: {}", listen_addr);
        info!("  🚀 Bootstrap Peers Connected: {}", bootstrap_peer_count);

        Ok((peer_id.to_string(), events))
    }

    /// Register a local shard actor with the distributed registry
    #[allow(dead_code)] // Framework method, used when Kameo remote features are enabled
    pub async fn register_shard(&mut self, shard_id: Uuid, _actor: &MicroshardActor) -> Result<()> {
        let shard_name = format!("shard-{}", shard_id);

        info!(
            shard_id = %shard_id,
            shard_name = %shard_name,
            "Registering shard for distributed access (stub)"
        );

        // Note: Actual shard registration happens via NodeOrchestrator -> ClusterCoordinator
        // and implicit Kameo registration if MicroshardActor is marked RemoteActor.
        // This method is kept for future direct-shard-registration if needed.

        Ok(())
    }

    /// Discover and connect to peer nodes in the cluster
    pub async fn discover_peers(&mut self) -> Result<Vec<NodeInfo>> {
        info!(
            "🔍 Discovering peer nodes in cluster '{}'",
            self.cluster_config.cluster_name
        );

        // Discovery is now event-driven via Kademlia DHT.
        // The swarm runtime automatically handles:
        // 1. Bootstrapping via Identify protocol (adds peers to DHT)
        // 2. DHT RoutingUpdated events (triggers dialing)
        // 3. PeerUuidDiscovered events (updates peer_nodes map)

        // Return currently known connected peers
        let known_peers: Vec<NodeInfo> = self.peer_nodes.values().cloned().collect();

        info!("✅ Known connected peer nodes: {}", known_peers.len());
        Ok(known_peers)
    }

    /// Record a routing table update event from the swarm.
    pub fn routing_updated(&mut self) {
        self.routing_updates = self.routing_updates.saturating_add(1);
    }

    /// Record a peer discovery/update event.
    pub fn peer_discovered(&mut self, node_id: Uuid, address: String) {
        self.peer_discovered_with_name(node_id, address, None);
    }

    /// Record a peer discovery/update event with optional node name.
    pub fn peer_discovered_with_name(
        &mut self,
        node_id: Uuid,
        address: String,
        node_name: Option<String>,
    ) {
        let entry = self.peer_nodes.entry(node_id).or_insert(NodeInfo {
            node_id,
            node_name: node_name.clone(),
            address: address.clone(),
            status: NodeStatus::Connected,
            shard_count: 0,
        });
        // Update node name if provided
        if let Some(name) = node_name {
            entry.node_name = Some(name);
        }
        // Prefer the newer address only if it differs from the current one to track last-good
        if entry.address != address {
            entry.address = address;
        }
        entry.status = NodeStatus::Connected;
    }

    /// Record a peer lost/disconnected event.
    pub fn peer_lost(&mut self, node_id: Uuid) {
        if let Some(node) = self.peer_nodes.get_mut(&node_id) {
            node.status = NodeStatus::Disconnected;
        }
    }

    /// Increment dial failure counter (to be called by swarm hooks).
    pub fn dial_failed(&mut self) {
        self.dial_failures = self.dial_failures.saturating_add(1);
    }

    /// Route a request to the appropriate shard, potentially on a remote node
    #[allow(dead_code)] // Framework method, used when Kameo remote features are enabled
    pub async fn route_to_shard(&self, shard_id: Uuid, operation: &str) -> Result<String> {
        let _shard_name = format!("shard-{}", shard_id);

        info!(
            shard_id = %shard_id,
            operation = %operation,
            "Routing operation to distributed shard (stub)"
        );

        // Note: Actual routing happens via RouterActor -> ClusterCoordinator -> NodeOrchestrator.
        // This method is legacy/simulated logic and should not be used in the new architecture.
        // We return an error to ensure callers migrate to the proper flow.

        // TODO: When Kameo remote features are available and if direct shard routing is needed:
        // if let Some(remote_shard) = RemoteActorRef::<MicroshardActor>::lookup(&shard_name).await? {
        //     let result = remote_shard.ask(&operation_message).await?;
        //     return Ok(result);
        // }

        Err(anyhow::anyhow!(
            "Direct DistributedCluster routing is deprecated. Use RouterActor."
        ))
    }

    /// Get cluster status and health information
    pub fn get_cluster_status(&self) -> ClusterStatus {
        let connected_nodes = self
            .peer_nodes
            .values()
            .filter(|node| node.status == NodeStatus::Connected)
            .count();

        let total_shards = self.peer_nodes.values().map(|node| node.shard_count).sum();

        ClusterStatus {
            cluster_name: self.cluster_config.cluster_name.clone(),
            total_nodes: self.peer_nodes.len() + 1, // +1 for local node
            connected_nodes: connected_nodes + 1,   // +1 for local node
            total_shards,
            distributed_enabled: self.cluster_config.distributed_actors,
            dial_failures: self.dial_failures,
            bootstrap_successes: self.bootstrap_successes,
            routing_updates: self.routing_updates,
        }
    }
}

impl Drop for DistributedCluster {
    fn drop(&mut self) {
        if let Some(handle) = self.swarm_handle.as_ref() {
            if handle.is_running() {
                if let Err(error) = handle.shutdown() {
                    warn!(%error, "failed to signal swarm shutdown during drop");
                }
            }
        }
    }
}

/// Cluster health and status information
#[derive(Debug, Clone, Reply)]
pub struct ClusterStatus {
    pub cluster_name: String,
    pub total_nodes: usize,
    pub connected_nodes: usize,
    pub total_shards: usize,
    pub distributed_enabled: bool,
    pub dial_failures: u64,
    pub bootstrap_successes: u64,
    pub routing_updates: u64,
}
