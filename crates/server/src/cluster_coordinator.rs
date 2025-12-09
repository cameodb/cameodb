//! ClusterCoordinator actor wrapping DistributedCluster lifecycle and queries.
//!
//! This actor owns the DistributedCluster and provides message-based access
//! to swarm initialization, peer discovery, status queries, and shard routing.

use anyhow::Result;
use kameo::message::{Context, Message};
use kameo::{Actor, Reply};
use tokio::task;
use tracing::{info, warn};
use uuid::Uuid;

use crate::distributed::{ClusterStatus, DistributedCluster, NodeInfo};
use crate::swarm::CoordinatorEvent;

// ============================================================================
// Message Definitions
// ============================================================================

/// Message to initialize the distributed swarm.
#[derive(Debug, Clone)]
pub struct InitSwarm;

/// Message to gracefully shutdown the swarm.
#[derive(Debug, Clone)]
pub struct ShutdownSwarm;

/// Message to trigger peer discovery.
#[derive(Debug, Clone)]
pub struct DiscoverPeers;

/// Message to get the current cluster status.
#[derive(Debug, Clone)]
pub struct GetStatus;

/// Routing table update event from swarm.
#[derive(Debug, Clone)]
pub struct RoutingUpdated;

/// Dial/connect failure event from swarm.
#[derive(Debug, Clone)]
pub struct DialFailed {
    pub peer_id: Option<String>,
    pub error: String,
}

/// Peer discovered/updated event.
#[derive(Debug, Clone)]
pub struct PeerDiscovered {
    pub node_id: Uuid,
    pub address: String,
}

/// Peer lost/disconnected event.
#[derive(Debug, Clone)]
pub struct PeerLost {
    pub node_id: Uuid,
}

/// Message to route a shard operation (stub for future remote actor support).
#[derive(Debug, Clone)]
pub struct RouteShard {
    pub shard_id: Uuid,
    pub operation: String,
}

/// Metadata describing a shard and its owning node.
#[derive(Debug, Clone)]
pub struct ShardMetadata {
    pub shard_id: Uuid,
    pub node_id: Uuid,
    pub vnode_tokens: Vec<u64>,
    pub storage_bytes: u64,
    pub document_count: u64,
}

/// Register or refresh local shards with the coordinator so assignments can be shared.
#[derive(Debug, Clone)]
pub struct RegisterLocalShards {
    pub node_id: Uuid,
    pub shards: Vec<ShardMetadata>,
}

/// Get the current shard-to-node assignments.
#[derive(Debug, Clone)]
pub struct GetShardAssignments;

/// Response indicating where an operation should be routed.
#[derive(Debug, Clone, Reply)]
#[allow(dead_code)] // Variants used in future phases
pub enum RoutingDecision {
    /// Handle locally on this node.
    Local,
    /// Forward to a remote node (node_id, peer_addr).
    Remote { node_id: Uuid, peer_addr: String },
    /// Broadcast to all nodes (scatter-gather).
    Broadcast,
}

/// Message to determine routing for an operation based on routing key.
#[derive(Debug, Clone)]
pub struct RouteOperation {
    pub routing_key: Option<String>,
    pub operation_type: OperationType,
}

/// Type of operation for routing decisions.
#[derive(Debug, Clone)]
#[allow(dead_code)] // Used in future phases for routing logic
pub enum OperationType {
    Read,
    Write,
}

/// Stub message to request bootstrap peer redial on connection failures.
/// Used for future resilience when remote shard lookup fails.
#[derive(Debug, Clone)]
pub struct RequestBootstrapRedial {
    pub reason: String,
}

// ============================================================================
// Actor Definition
// ============================================================================

/// Actor that owns the DistributedCluster instance and coordinates cluster operations.
#[derive(Actor)]
pub struct ClusterCoordinator {
    cluster: DistributedCluster,
    shard_assignments: std::collections::HashMap<Uuid, ShardMetadata>,
}

impl ClusterCoordinator {
    /// Create a new ClusterCoordinator wrapping the given DistributedCluster.
    pub fn new(cluster: DistributedCluster) -> Self {
        Self {
            cluster,
            shard_assignments: std::collections::HashMap::new(),
        }
    }
}

impl Message<RegisterLocalShards> for ClusterCoordinator {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: RegisterLocalShards,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        for shard in msg.shards {
            self.shard_assignments.insert(shard.shard_id, shard);
        }
        info!(
            node = %msg.node_id,
            total_assignments = self.shard_assignments.len(),
            "ClusterCoordinator: registered local shards"
        );
    }
}

impl Message<GetShardAssignments> for ClusterCoordinator {
    type Reply = std::collections::HashMap<Uuid, ShardMetadata>;

    async fn handle(
        &mut self,
        _msg: GetShardAssignments,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.shard_assignments.clone()
    }
}
// ============================================================================
// Message Handlers
// ============================================================================

impl Message<InitSwarm> for ClusterCoordinator {
    type Reply = Result<String>; // Returns peer_id on success

    async fn handle(
        &mut self,
        _msg: InitSwarm,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let self_ref = ctx.actor_ref();
        match self.cluster.init_swarm().await {
            Ok((peer_id, events)) => {
                info!(peer_id = %peer_id, "ClusterCoordinator: swarm initialized");

                if let Some(mut rx) = events {
                    let coordinator = self_ref.clone();
                    task::spawn(async move {
                        while let Some(event) = rx.recv().await {
                            match event {
                                CoordinatorEvent::RoutingUpdated { .. } => {
                                    if let Err(err) = coordinator.ask(RoutingUpdated).await {
                                        warn!(error = %err, "ClusterCoordinator: failed to forward routing update");
                                    }
                                }
                                CoordinatorEvent::PeerDiscovered { peer_id, address } => {
                                    let parsed = Uuid::parse_str(&peer_id)
                                        .unwrap_or_else(|_| Uuid::new_v4());
                                    if let Err(err) = coordinator
                                        .ask(PeerDiscovered {
                                            node_id: parsed,
                                            address: address.unwrap_or_default(),
                                        })
                                        .await
                                    {
                                        warn!(error = %err, "ClusterCoordinator: failed to forward peer discovered");
                                    }
                                }
                                CoordinatorEvent::PeerLost { peer_id } => {
                                    let parsed = Uuid::parse_str(&peer_id)
                                        .unwrap_or_else(|_| Uuid::new_v4());
                                    if let Err(err) =
                                        coordinator.ask(PeerLost { node_id: parsed }).await
                                    {
                                        warn!(error = %err, "ClusterCoordinator: failed to forward peer lost");
                                    }
                                }
                                CoordinatorEvent::DialFailed { peer_id, error } => {
                                    if let Err(err) =
                                        coordinator.ask(DialFailed { peer_id, error }).await
                                    {
                                        warn!(error = %err, "ClusterCoordinator: failed to forward dial failed");
                                    }
                                }
                            }
                        }
                    });
                }

                Ok(peer_id)
            }
            Err(err) => {
                warn!(error = %err, "ClusterCoordinator: init_swarm failed");
                Err(err)
            }
        }
    }
}

impl Message<ShutdownSwarm> for ClusterCoordinator {
    type Reply = ();

    async fn handle(
        &mut self,
        _msg: ShutdownSwarm,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        if let Some(handle) = self.cluster.swarm_handle() {
            if let Err(err) = handle.shutdown() {
                warn!(error = %err, "ClusterCoordinator: shutdown signal failed");
            } else {
                info!("ClusterCoordinator: swarm shutdown signaled");
            }
        }
    }
}

impl Message<DiscoverPeers> for ClusterCoordinator {
    type Reply = Result<Vec<NodeInfo>>;

    async fn handle(
        &mut self,
        _msg: DiscoverPeers,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        match self.cluster.discover_peers().await {
            Ok(peers) => {
                info!(
                    peer_count = peers.len(),
                    "ClusterCoordinator: peers discovered"
                );
                Ok(peers)
            }
            Err(err) => {
                warn!(error = %err, "ClusterCoordinator: discover_peers failed");
                Err(err)
            }
        }
    }
}

impl Message<GetStatus> for ClusterCoordinator {
    type Reply = ClusterStatus;

    async fn handle(
        &mut self,
        _msg: GetStatus,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let status = self.cluster.get_cluster_status();
        info!(
            cluster = %status.cluster_name,
            total = status.total_nodes,
            connected = status.connected_nodes,
            "ClusterCoordinator: status snapshot"
        );
        status
    }
}

impl Message<RoutingUpdated> for ClusterCoordinator {
    type Reply = ();

    async fn handle(
        &mut self,
        _msg: RoutingUpdated,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.cluster.routing_updated();
        info!("ClusterCoordinator: routing table updated");
    }
}

impl Message<DialFailed> for ClusterCoordinator {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: DialFailed,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.cluster.dial_failed();
        warn!(
            peer = ?msg.peer_id,
            error = %msg.error,
            "ClusterCoordinator: dial failed"
        );
    }
}

impl Message<PeerDiscovered> for ClusterCoordinator {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: PeerDiscovered,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.cluster
            .peer_discovered(msg.node_id, msg.address.clone());
        info!(node = %msg.node_id, addr = %msg.address, "ClusterCoordinator: peer discovered");
    }
}

impl Message<PeerLost> for ClusterCoordinator {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: PeerLost,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.cluster.peer_lost(msg.node_id);
        warn!(node = %msg.node_id, "ClusterCoordinator: peer lost");
    }
}

impl Message<RouteShard> for ClusterCoordinator {
    type Reply = Result<String>;

    async fn handle(
        &mut self,
        msg: RouteShard,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        match self
            .cluster
            .route_to_shard(msg.shard_id, &msg.operation)
            .await
        {
            Ok(res) => Ok(res),
            Err(err) => {
                warn!(error = %err, shard_id = %msg.shard_id, "ClusterCoordinator: route_to_shard failed");
                Err(err)
            }
        }
    }
}

impl Message<RouteOperation> for ClusterCoordinator {
    type Reply = RoutingDecision;

    async fn handle(
        &mut self,
        msg: RouteOperation,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        // Phase 4 stub: Currently all operations are handled locally.
        // Future phases will implement proper routing based on:
        // - Hash ring lookup for routing_key
        // - Cluster topology from peer tracking
        // - Shard-to-node assignments
        match msg.routing_key {
            Some(_key) => {
                // TODO: Look up routing_key in hash ring to determine target node
                // For now, handle locally
                info!("RouteOperation: routing_key present, handling locally (stub)");
                RoutingDecision::Local
            }
            None => {
                // No routing key = scatter-gather for reads, round-robin for writes
                match msg.operation_type {
                    OperationType::Read => {
                        info!("RouteOperation: no routing_key, broadcast for read");
                        RoutingDecision::Broadcast
                    }
                    OperationType::Write => {
                        info!("RouteOperation: no routing_key, local round-robin for write");
                        RoutingDecision::Local
                    }
                }
            }
        }
    }
}

impl Message<RequestBootstrapRedial> for ClusterCoordinator {
    type Reply = Result<()>;

    async fn handle(
        &mut self,
        msg: RequestBootstrapRedial,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        // Stub for future resilience: when remote operations fail,
        // this message can trigger re-dialing bootstrap peers.
        warn!(
            reason = %msg.reason,
            "RequestBootstrapRedial: redial requested (stub - no action taken)"
        );
        // Future implementation:
        // 1. Check if swarm is still running
        // 2. Re-dial bootstrap peers via swarm handle
        // 3. Update cluster state after successful connections
        Ok(())
    }
}

// ============================================================================
// Cleanup on Drop
// ============================================================================

impl Drop for ClusterCoordinator {
    fn drop(&mut self) {
        if let Some(handle) = self.cluster.swarm_handle() {
            if handle.is_running() {
                if let Err(error) = handle.shutdown() {
                    warn!(%error, "ClusterCoordinator drop: failed to signal swarm shutdown");
                }
            }
        }
    }
}
