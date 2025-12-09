//! ClusterCoordinator actor wrapping DistributedCluster lifecycle and queries.
//!
//! This actor owns the DistributedCluster and provides message-based access
//! to swarm initialization, peer discovery, status queries, and shard routing.

use anyhow::Result;
use kameo::message::{Context, Message};
use kameo::{Actor, Reply};
use std::collections::HashMap;
use tokio::task;
use tracing::{info, warn};
use uuid::Uuid;

use crate::distributed::{ClusterStatus, DistributedCluster, NodeInfo};
use crate::swarm::CoordinatorEvent;
use cluster::{ConsistentRing, NodeIdentity};

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
#[allow(dead_code)]
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
    shard_assignments: HashMap<Uuid, ShardMetadata>,
    ring: ConsistentRing,
}

impl ClusterCoordinator {
    /// Create a new ClusterCoordinator wrapping the given DistributedCluster.
    pub fn new(cluster: DistributedCluster) -> Self {
        Self {
            cluster,
            shard_assignments: HashMap::new(),
            ring: ConsistentRing::new(),
        }
    }

    fn decide_route(
        &self,
        routing_key: Option<String>,
        operation_type: OperationType,
    ) -> RoutingDecision {
        // operation_type reserved for future policy; currently unused
        let _ = operation_type;

        if let Some(key) = routing_key {
            if let Some(shard_id) = self.route_for_key(&key) {
                if let Some(owner_node) = self.shard_owner(&shard_id) {
                    if owner_node == self.cluster.local_node_id {
                        info!(%shard_id, "RouteOperation: routing locally by key");
                        return RoutingDecision::Local;
                    } else if let Some(addr) = self.node_address(&owner_node) {
                        info!(%shard_id, node = %owner_node, addr = %addr, "RouteOperation: routing remote by key");
                        return RoutingDecision::Remote {
                            node_id: owner_node,
                            peer_addr: addr,
                        };
                    } else {
                        warn!(%shard_id, node = %owner_node, "RouteOperation: owner address unknown, broadcasting");
                        return RoutingDecision::Broadcast;
                    }
                } else {
                    warn!(%shard_id, "RouteOperation: shard owner unknown, broadcasting");
                    return RoutingDecision::Broadcast;
                }
            } else {
                warn!("RouteOperation: ring empty, broadcasting");
                return RoutingDecision::Broadcast;
            }
        }

        info!("RouteOperation: no routing_key provided, broadcasting");
        RoutingDecision::Broadcast
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
        self.rebuild_ring();
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

impl ClusterCoordinator {
    fn rebuild_ring(&mut self) {
        self.ring = ConsistentRing::new();
        for (shard_id, meta) in &self.shard_assignments {
            let name: String = shard_id.simple().to_string().chars().take(3).collect();
            let identity = NodeIdentity {
                uuid: *shard_id,
                name,
                vnode_tokens: meta.vnode_tokens.clone(),
            };
            self.ring.add_node(&identity);
        }
    }

    fn route_for_key(&self, key: &str) -> Option<Uuid> {
        self.ring.get_owner(key)
    }

    fn shard_owner(&self, shard_id: &Uuid) -> Option<Uuid> {
        self.shard_assignments.get(shard_id).map(|m| m.node_id)
    }

    fn node_address(&self, node_id: &Uuid) -> Option<String> {
        self.cluster
            .peer_nodes
            .get(node_id)
            .map(|n| n.address.clone())
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
        self.decide_route(msg.routing_key, msg.operation_type)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ClusterConfig;
    use crate::distributed::NodeStatus;

    fn make_cluster() -> DistributedCluster {
        let cfg = ClusterConfig::default();
        DistributedCluster::new(cfg, Uuid::new_v4())
    }

    #[test]
    fn decide_route_defaults_to_broadcast_when_no_key() {
        let cc = ClusterCoordinator::new(make_cluster());
        let decision = cc.decide_route(None, OperationType::Read);
        assert!(matches!(decision, RoutingDecision::Broadcast));
    }

    #[test]
    fn decide_route_returns_local_when_owner_is_self() {
        let cluster = make_cluster();
        let local = cluster.local_node_id;
        let mut cc = ClusterCoordinator::new(cluster);

        let shard_id = Uuid::new_v4();
        cc.shard_assignments.insert(
            shard_id,
            ShardMetadata {
                shard_id,
                node_id: local,
                vnode_tokens: vec![1, 2, 3],
                storage_bytes: 0,
                document_count: 0,
            },
        );
        cc.rebuild_ring();

        let decision = cc.decide_route(Some("key-1".into()), OperationType::Read);
        assert!(matches!(decision, RoutingDecision::Local));
    }

    #[test]
    fn decide_route_returns_remote_when_owner_known_with_addr() {
        let mut cluster = make_cluster();
        let owner = Uuid::new_v4();
        cluster.peer_nodes.insert(
            owner,
            NodeInfo {
                node_id: owner,
                address: "127.0.0.1:9000".into(),
                status: NodeStatus::Connected,
                shard_count: 0,
            },
        );
        let mut cc = ClusterCoordinator::new(cluster);

        let shard_id = Uuid::new_v4();
        cc.shard_assignments.insert(
            shard_id,
            ShardMetadata {
                shard_id,
                node_id: owner,
                vnode_tokens: vec![1, 2, 3],
                storage_bytes: 0,
                document_count: 0,
            },
        );
        cc.rebuild_ring();

        let decision = cc.decide_route(Some("key-remote".into()), OperationType::Write);
        match decision {
            RoutingDecision::Remote { node_id, .. } => assert_eq!(node_id, owner),
            other => panic!("expected Remote, got {:?}", other),
        }
    }

    #[test]
    fn decide_route_broadcasts_when_owner_address_missing() {
        let mut cc = ClusterCoordinator::new(make_cluster());
        let shard_id = Uuid::new_v4();
        let owner = Uuid::new_v4(); // not present in peer_nodes
        cc.shard_assignments.insert(
            shard_id,
            ShardMetadata {
                shard_id,
                node_id: owner,
                vnode_tokens: vec![1, 2, 3],
                storage_bytes: 0,
                document_count: 0,
            },
        );
        cc.rebuild_ring();

        let decision = cc.decide_route(Some("key-no-addr".into()), OperationType::Read);
        assert!(matches!(decision, RoutingDecision::Broadcast));
    }

    #[test]
    fn ring_distribution_splits_across_multiple_nodes() {
        let mut cluster = make_cluster();
        let n1 = Uuid::new_v4();
        let n2 = Uuid::new_v4();
        cluster.peer_nodes.insert(
            n1,
            NodeInfo {
                node_id: n1,
                address: "127.0.0.1:9101".into(),
                status: NodeStatus::Connected,
                shard_count: 0,
            },
        );
        cluster.peer_nodes.insert(
            n2,
            NodeInfo {
                node_id: n2,
                address: "127.0.0.1:9102".into(),
                status: NodeStatus::Connected,
                shard_count: 0,
            },
        );
        let mut cc = ClusterCoordinator::new(cluster);

        let s1 = Uuid::new_v4();
        let s2 = Uuid::new_v4();
        cc.shard_assignments.insert(
            s1,
            ShardMetadata {
                shard_id: s1,
                node_id: n1,
                vnode_tokens: vec![1, 3, 5],
                storage_bytes: 0,
                document_count: 0,
            },
        );
        cc.shard_assignments.insert(
            s2,
            ShardMetadata {
                shard_id: s2,
                node_id: n2,
                vnode_tokens: vec![2, 4, 6],
                storage_bytes: 0,
                document_count: 0,
            },
        );
        cc.rebuild_ring();

        let mut counts = std::collections::HashMap::new();
        for i in 0..200 {
            let key = format!("key-{i}");
            match cc.decide_route(Some(key), OperationType::Read) {
                RoutingDecision::Remote { node_id, .. } => {
                    *counts.entry(node_id).or_insert(0usize) += 1;
                }
                RoutingDecision::Local => {
                    *counts.entry(cc.cluster.local_node_id).or_insert(0usize) += 1;
                }
                RoutingDecision::Broadcast => {
                    *counts.entry(Uuid::nil()).or_insert(0usize) += 1;
                }
            }
        }

        assert!(counts.get(&n1).copied().unwrap_or(0) > 0);
        assert!(counts.get(&n2).copied().unwrap_or(0) > 0);
    }
}
