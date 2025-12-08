//! ClusterCoordinator actor wrapping DistributedCluster lifecycle and queries.
//!
//! This actor owns the DistributedCluster and provides message-based access
//! to swarm initialization, peer discovery, status queries, and shard routing.

use anyhow::Result;
use kameo::message::{Context, Message};
use kameo::{Actor, Reply};
use tracing::{info, warn};
use uuid::Uuid;

use crate::distributed::{ClusterStatus, DistributedCluster, NodeInfo};

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

/// Message to route a shard operation (stub for future remote actor support).
#[derive(Debug, Clone)]
pub struct RouteShard {
    pub shard_id: Uuid,
    pub operation: String,
}

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
}

impl ClusterCoordinator {
    /// Create a new ClusterCoordinator wrapping the given DistributedCluster.
    pub fn new(cluster: DistributedCluster) -> Self {
        Self { cluster }
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
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        match self.cluster.init_swarm().await {
            Ok(peer_id) => {
                info!(peer_id = %peer_id, "ClusterCoordinator: swarm initialized");
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
