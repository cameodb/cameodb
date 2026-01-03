//! ClusterCoordinator actor wrapping DistributedCluster lifecycle and queries.
//!
//! This actor owns the DistributedCluster and provides message-based access
//! to swarm initialization, peer discovery, status queries, and shard routing.

use anyhow::Result;
use kameo::actor::RemoteActorRef;
use kameo::message::{Context, Message};
use kameo::{Actor, RemoteActor, Reply, remote_message};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::cluster_state::{
    ClusterStateStore, PersistedClusterConfig, PersistedClusterTopology, current_timestamp,
};
use crate::cluster_state_machine::ClusterState;
use crate::distributed::{ClusterStatus, DistributedCluster, NodeInfo, NodeStatus};
use crate::swarm::CoordinatorEvent;
use cluster::{ConsistentRing, NodeIdentity};

// ============================================================================
// Message Definitions
// ============================================================================

/// Message to subscribe to topology (ring) updates.
#[derive(Debug, Clone)]
pub struct SubscribeTopology {
    pub subscriber: mpsc::Sender<ConsistentRing>,
}

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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetShardAssignments;

/// Response indicating where an operation should be routed.
#[derive(Debug, Clone, Reply)]
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

/// Message to get known peers for broadcast scatter-gather.
#[derive(Debug, Clone)]
pub struct GetKnownPeers;

/// Response containing known peer information for broadcast.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KnownPeer {
    pub node_id: Uuid,
    pub node_name: Option<String>,
    pub address: String,
}

/// Message when shards are discovered for a peer via DHT.
#[derive(Debug, Clone)]
pub struct PeerShardsDiscovered {
    pub peer_id: String,
    pub shards: Vec<ShardMetadata>,
}

/// Message to merge remote shard assignments into local coordinator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergeRemoteShards {
    pub node_id: Uuid,
    pub node_name: String,
    pub shards: HashMap<Uuid, ShardMetadata>,
}

/// Message to get complete cluster snapshot for persistence
#[derive(Debug, Clone)]
pub struct GetClusterSnapshot;

/// Internal message to record a push failure for DHT fallback tracking
#[derive(Debug, Clone)]
pub struct RecordPushFailure {
    pub node_id: Uuid,
}

/// Internal message to reset push failure count on successful push
#[derive(Debug, Clone)]
pub struct ResetPushFailure {
    pub node_id: Uuid,
}

/// Internal message to mark bootstrap as complete
#[derive(Debug, Clone)]
pub struct MarkBootstrapComplete;

/// Snapshot of cluster topology for persistence
#[derive(Debug, Clone, Reply)]
pub struct ClusterSnapshot {
    #[allow(dead_code)] // Used by GetClusterSnapshot responses and future HTTP exposure
    pub config: PersistedClusterConfig,
    #[allow(dead_code)] // Used by GetClusterSnapshot responses and future HTTP exposure
    pub shards: HashMap<Uuid, ShardMetadata>,
    #[allow(dead_code)] // Used by GetClusterSnapshot responses and future HTTP exposure
    pub nodes: HashMap<Uuid, NodeInfo>,
    #[allow(dead_code)] // Used by GetClusterSnapshot responses and future HTTP exposure
    pub ring: ConsistentRing,
}

// ============================================================================
// Actor Definition
// ============================================================================

/// Actor that owns the DistributedCluster instance and coordinates cluster operations.
#[derive(Actor, RemoteActor)]
pub struct ClusterCoordinator {
    cluster: DistributedCluster,
    shard_assignments: HashMap<Uuid, ShardMetadata>,
    ring: ConsistentRing,

    // State management
    state: ClusterState,
    /// Authoritative registry of all known cluster nodes (active or disconnected)
    expected_nodes: HashMap<Uuid, NodeInfo>,
    generation: u64,
    state_store: Option<Arc<ClusterStateStore>>,

    // Track expected shards from snapshot for reconciliation
    expected_shards: HashMap<Uuid, ShardMetadata>,

    // Subscribers for topology updates
    topology_subscribers: Vec<mpsc::Sender<ConsistentRing>>,

    // DHT Bootstrap tracking - DHT is used only during bootstrap, then push-only
    bootstrap_complete: bool,

    // Track last persisted generation to avoid redundant snapshots
    last_persisted_generation: u64,

    // Track push failures per peer for DHT fallback recovery
    push_failure_count: HashMap<Uuid, u32>,
}

impl ClusterCoordinator {
    /// Create a new ClusterCoordinator wrapping the given DistributedCluster.
    pub fn new(cluster: DistributedCluster) -> Self {
        let mut expected_nodes = HashMap::new();
        // Add local node to expected nodes
        expected_nodes.insert(
            cluster.local_node_id,
            NodeInfo {
                node_id: cluster.local_node_id,
                node_name: Some(cluster.local_node_name.clone()),
                address: format!("0.0.0.0:{}", cluster.cluster_config.cluster_port),
                status: NodeStatus::Connected,
                shard_count: 0,
            },
        );

        Self {
            cluster,
            shard_assignments: HashMap::new(),
            ring: ConsistentRing::new(),
            state: ClusterState::Active {
                generation: 1,
                active_nodes: 1,
                total_expected: 1,
            },
            expected_nodes,
            generation: 1,
            state_store: None,
            expected_shards: HashMap::new(),
            topology_subscribers: Vec::new(),
            bootstrap_complete: false,
            last_persisted_generation: 0,
            push_failure_count: HashMap::new(),
        }
    }

    /// Create ClusterCoordinator with persisted state for recovery
    /// Expected nodes from metadata are marked as Inactive until they send PeerDiscovered
    pub fn new_with_persisted_state(
        cluster: DistributedCluster,
        persisted: PersistedClusterTopology,
        state_store: Arc<ClusterStateStore>,
    ) -> Self {
        // Convert persisted nodes to expected_nodes map
        let mut expected_nodes: HashMap<Uuid, NodeInfo> = persisted
            .nodes
            .values()
            .map(|pn| {
                (
                    pn.node_id,
                    NodeInfo {
                        node_id: pn.node_id,
                        node_name: None, // Will be populated from peer discovery
                        address: pn.address.clone(),
                        status: crate::distributed::NodeStatus::Disconnected, // Start as Disconnected, wait for discovery
                        shard_count: pn.shard_count,
                    },
                )
            })
            .collect();

        // Ensure local node is always expected and Connected
        expected_nodes.insert(
            cluster.local_node_id,
            NodeInfo {
                node_id: cluster.local_node_id,
                node_name: None, // Local node name will be set separately
                address: format!("0.0.0.0:{}", cluster.cluster_config.cluster_port),
                status: crate::distributed::NodeStatus::Connected,
                shard_count: 0,
            },
        );

        let generation = persisted.config.generation;
        let total_expected = expected_nodes.len();

        // Convert persisted shards to expected shard metadata for reconciliation
        let expected_shards: HashMap<Uuid, ShardMetadata> = persisted
            .shards
            .into_iter()
            .map(|(shard_id, ps)| {
                (
                    shard_id,
                    ShardMetadata {
                        shard_id: ps.shard_id,
                        node_id: ps.node_id,
                        vnode_tokens: ps.vnode_tokens,
                        storage_bytes: ps.storage_bytes,
                        document_count: ps.document_count,
                    },
                )
            })
            .collect();

        info!(
            generation,
            expected_nodes = total_expected,
            expected_shards = expected_shards.len(),
            "ClusterCoordinator: restoring from persisted state, marking peers as Inactive"
        );

        // Start in Degraded since we only have local node active
        let state = if total_expected > 1 {
            ClusterState::Degraded {
                active_nodes: 1,
                inactive_nodes: total_expected - 1,
            }
        } else {
            ClusterState::Active {
                generation,
                active_nodes: 1,
                total_expected: 1,
            }
        };

        // Rebuild ring from expected shards immediately
        let mut ring = ConsistentRing::new();
        for (shard_id, meta) in &expected_shards {
            let name: String = shard_id.simple().to_string().chars().take(3).collect();
            let identity = NodeIdentity {
                uuid: *shard_id,
                name,
                vnode_tokens: meta.vnode_tokens.clone(),
                keypair: None,
            };
            ring.add_node(&identity);
        }

        info!(
            ring_nodes = ring.len(), // This is actually vnode count, but Close enough for log
            "ClusterCoordinator: rebuilt ring from persisted state"
        );

        Self {
            cluster,
            shard_assignments: expected_shards.clone(), // Restore assignments
            ring,
            state,
            expected_nodes,
            generation,
            state_store: Some(state_store),
            expected_shards,
            topology_subscribers: Vec::new(),
            bootstrap_complete: false, // Will be set after initial DHT queries complete
            last_persisted_generation: generation,
            push_failure_count: HashMap::new(),
        }
    }

    /// Set the state store (used when creating without persisted state)
    pub fn set_state_store(&mut self, state_store: Arc<ClusterStateStore>) {
        self.state_store = Some(state_store);
    }

    /// Set cluster state (for testing or manual overrides)
    pub fn set_state(&mut self, state: ClusterState) {
        info!(old_state = ?self.state, new_state = ?state, "ClusterCoordinator: state transition");
        self.state = state;
    }

    /// Format node identity as "NAME (UUID)" for human-readable logging
    fn format_node_identity(&self, node_id: Uuid) -> String {
        if let Some(node_info) = self.expected_nodes.get(&node_id) {
            node_info.format_identity()
        } else if let Some(peer_info) = self.cluster.peer_nodes.get(&node_id) {
            peer_info.format_identity()
        } else {
            node_id.to_string()
        }
    }

    /// Persist current cluster state snapshot to disk (event-driven)
    /// Only persists when generation changes to avoid redundant writes
    fn persist_snapshot(&mut self) {
        // Skip if generation hasn't changed since last persist
        if self.generation == self.last_persisted_generation {
            return;
        }

        if let Some(state_store) = &self.state_store {
            // 1. Calculate authoritative shard counts from assignments
            let mut shard_counts: HashMap<Uuid, usize> = HashMap::new();
            for meta in self.shard_assignments.values() {
                *shard_counts.entry(meta.node_id).or_default() += 1;
            }

            // 2. Sync expected_nodes with the source of truth (peer_nodes + local)

            // 2a. Update local node
            let local_id = self.cluster.local_node_id;
            let local_shard_count = shard_counts.get(&local_id).copied().unwrap_or(0);

            self.expected_nodes
                .entry(local_id)
                .and_modify(|n| {
                    n.status = crate::distributed::NodeStatus::Connected;
                    n.shard_count = local_shard_count;
                })
                .or_insert_with(|| NodeInfo {
                    node_id: local_id,
                    node_name: None, // Local node name
                    address: format!("0.0.0.0:{}", self.cluster.cluster_config.cluster_port),
                    status: crate::distributed::NodeStatus::Connected,
                    shard_count: local_shard_count,
                });

            // 2b. Sync from peer_nodes (active connections)
            for (node_id, peer_info) in &self.cluster.peer_nodes {
                let count = shard_counts.get(node_id).copied().unwrap_or(0);
                self.expected_nodes
                    .entry(*node_id)
                    .and_modify(|n| {
                        n.status = peer_info.status;
                        n.address = peer_info.address.clone();
                        n.shard_count = count;
                    })
                    .or_insert_with(|| {
                        let mut n = peer_info.clone();
                        n.shard_count = count;
                        n
                    });
            }

            // 2c. Update shard counts for disconnected nodes (in expected_nodes but not in peer_nodes)
            for (node_id, node_info) in self.expected_nodes.iter_mut() {
                if *node_id != local_id && !self.cluster.peer_nodes.contains_key(node_id) {
                    node_info.shard_count = shard_counts.get(node_id).copied().unwrap_or(0);
                    node_info.status = crate::distributed::NodeStatus::Disconnected;
                }
            }

            // 3. Prepare data for persistence (clone for moving to blocking task)
            let config = PersistedClusterConfig {
                expected_nodes: self.expected_nodes.len(),
                generation: self.generation,
                last_stable_at: if self.state.is_healthy() {
                    Some(current_timestamp())
                } else {
                    None
                },
                cluster_name: self.cluster.cluster_config.cluster_name.clone(),
            };

            let shard_assignments = self.shard_assignments.clone();
            // Use expected_nodes (the full registry) instead of just peer_nodes
            let nodes_to_persist = self.expected_nodes.clone();
            let ring = self.ring.clone();
            let state_store = state_store.clone();
            let generation = self.generation;

            // 4. Offload blocking I/O to thread pool
            task::spawn_blocking(move || {
                if let Err(e) = state_store.persist_cluster_snapshot(
                    &config,
                    &shard_assignments,
                    &nodes_to_persist,
                    &ring,
                ) {
                    warn!(error = %e, "Failed to persist cluster snapshot");
                } else {
                    info!(
                        generation,
                        shards = shard_assignments.len(),
                        nodes = nodes_to_persist.len(),
                        "Cluster snapshot persisted"
                    );
                }
            });

            // Update last persisted generation after successful dispatch
            self.last_persisted_generation = self.generation;
        }
    }

    /// Evaluate cluster state and transition if needed (reactive, message-driven)
    /// Called after PeerDiscovered/PeerLost to update cluster state
    fn evaluate_and_transition_state(&mut self) {
        // Count currently connected peers + local node
        let connected_peers = self
            .cluster
            .peer_nodes
            .values()
            .filter(|n| n.status == crate::distributed::NodeStatus::Connected)
            .count();
        let active_nodes = connected_peers + 1; // +1 for local node

        let total_expected = if self.expected_nodes.is_empty() {
            active_nodes // No expectations, treat current as expected
        } else {
            self.expected_nodes.len()
        };
        let inactive_nodes = total_expected.saturating_sub(active_nodes);

        let new_state = match &self.state {
            ClusterState::Active { .. } => {
                // Check for degradation: if we have any inactive nodes
                if inactive_nodes > 0 {
                    warn!(
                        active_nodes,
                        inactive_nodes, "ClusterCoordinator: cluster degraded, some nodes inactive"
                    );
                    Some(ClusterState::Degraded {
                        active_nodes,
                        inactive_nodes,
                    })
                } else {
                    None // Still healthy
                }
            }
            ClusterState::Degraded { .. } => {
                // Check if recovered (all nodes active)
                if inactive_nodes == 0 {
                    info!("ClusterCoordinator: cluster recovered, all nodes active");
                    Some(ClusterState::Active {
                        generation: self.generation,
                        active_nodes,
                        total_expected,
                    })
                } else if active_nodes < total_expected / 2 {
                    // Less than 50% nodes active - mark as failed
                    error!(
                        active_nodes,
                        total_expected,
                        "ClusterCoordinator: cluster failed, too many nodes inactive"
                    );
                    Some(ClusterState::Failed {
                        reason: format!(
                            "Cluster lost quorum: {}/{} nodes active",
                            active_nodes, total_expected
                        ),
                    })
                } else {
                    // Update counts if changed
                    Some(ClusterState::Degraded {
                        active_nodes,
                        inactive_nodes,
                    })
                }
            }
            ClusterState::Failed { .. } => {
                // Recover if we have majority of nodes
                if active_nodes >= total_expected / 2 {
                    info!("ClusterCoordinator: cluster recovering from failure");
                    if inactive_nodes == 0 {
                        Some(ClusterState::Active {
                            generation: self.generation,
                            active_nodes,
                            total_expected,
                        })
                    } else {
                        Some(ClusterState::Degraded {
                            active_nodes,
                            inactive_nodes,
                        })
                    }
                } else {
                    None
                }
            }
        };

        if let Some(new_state) = new_state {
            self.set_state(new_state);
            self.persist_snapshot();
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
        for shard in msg.shards.clone() {
            self.shard_assignments.insert(shard.shard_id, shard);
        }
        self.rebuild_ring();
        info!(
            node = %msg.node_id,
            total_assignments = self.shard_assignments.len(),
            "ClusterCoordinator: registered local shards"
        );

        // Persist snapshot after shard registration (debounced)
        self.persist_snapshot();

        // Publish local shards to DHT ONLY during bootstrap phase
        // After bootstrap, rely exclusively on Kameo push for real-time updates
        if !self.bootstrap_complete {
            if let Some(handle) = self.cluster.swarm_handle() {
                if let Err(e) = handle.publish_shards(msg.node_id, msg.shards.clone()) {
                    warn!(error = %e, "Failed to publish shards to DHT during bootstrap");
                } else {
                    info!("ClusterCoordinator: published local shards to DHT (bootstrap phase)");
                }
            }
        }

        // Broadcast local shards to all known connected peers (push on change)
        let local_node_id = self.cluster.local_node_id;
        let shards_to_broadcast = msg.shards;
        let peers: Vec<(Uuid, String)> = self
            .cluster
            .peer_nodes
            .iter()
            .filter(|(_, info)| info.status == crate::distributed::NodeStatus::Connected)
            .map(|(id, info)| (*id, info.address.clone()))
            .collect();

        if !peers.is_empty() {
            info!(
                peer_count = peers.len(),
                "ClusterCoordinator: broadcasting local shards to peers"
            );

            // Clone self reference for failure tracking callback
            let self_weak = _ctx.actor_ref().downgrade();

            for (peer_id, _peer_addr) in peers {
                let remote_coord_name = format!("coordinator-{}", peer_id);
                let msg = MergeRemoteShards {
                    node_id: local_node_id,
                    node_name: self.cluster.local_node_name.clone(),
                    shards: shards_to_broadcast
                        .iter()
                        .cloned()
                        .map(|s| (s.shard_id, s))
                        .collect(),
                };
                let self_weak_clone = self_weak.clone();

                task::spawn(async move {
                    match RemoteActorRef::<ClusterCoordinator>::lookup(remote_coord_name.clone())
                        .await
                    {
                        Ok(Some(remote_coord)) => match remote_coord.tell(&msg).send() {
                            Ok(_) => {
                                debug!(node = %peer_id, "Successfully pushed shard update to remote coordinator");
                                // Reset failure count on success
                                if let Some(self_ref) = self_weak_clone.upgrade() {
                                    let _ =
                                        self_ref.tell(ResetPushFailure { node_id: peer_id }).await;
                                }
                            }
                            Err(e) => {
                                warn!(node = %peer_id, error = %e, "Failed to push shard update to remote coordinator");
                                // Track failure and trigger DHT fallback if threshold exceeded
                                if let Some(self_ref) = self_weak_clone.upgrade() {
                                    let _ =
                                        self_ref.tell(RecordPushFailure { node_id: peer_id }).await;
                                }
                            }
                        },
                        Ok(None) => {
                            debug!(node = %peer_id, "Remote coordinator not found for push update");
                        }
                        Err(e) => {
                            error!(node = %peer_id, error = %e, "Failed to lookup remote coordinator for push update");
                        }
                    }
                });
            }
        }
    }
}

/// Remote message handler for GetShardAssignments to enable cross-node metadata exchange.
#[remote_message("cameo.coordinator.get_shard_assignments")]
impl Message<GetShardAssignments> for ClusterCoordinator {
    type Reply = std::collections::HashMap<Uuid, ShardMetadata>;

    async fn handle(
        &mut self,
        _msg: GetShardAssignments,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        info!(
            shard_count = self.shard_assignments.len(),
            "ClusterCoordinator: GetShardAssignments (local or remote)"
        );
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
                keypair: None,
            };
            self.ring.add_node(&identity);
        }

        // Notify subscribers of the new topology
        if !self.topology_subscribers.is_empty() {
            info!(
                subscriber_count = self.topology_subscribers.len(),
                "ClusterCoordinator: broadcasting topology update"
            );

            let ring_clone = self.ring.clone();
            self.topology_subscribers.retain(|tx| {
                match tx.try_send(ring_clone.clone()) {
                    Ok(_) => true,
                    Err(mpsc::error::TrySendError::Closed(_)) => false, // Prune closed channels
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        warn!("ClusterCoordinator: subscriber channel full, skipping update");
                        true
                    }
                }
            });
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

impl Message<SubscribeTopology> for ClusterCoordinator {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: SubscribeTopology,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        info!("ClusterCoordinator: new topology subscriber registered");
        // Send current ring immediately
        let _ = msg.subscriber.try_send(self.ring.clone());
        self.topology_subscribers.push(msg.subscriber);
    }
}

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
                    #[derive(Default)]
                    struct PeerMeta {
                        uuid: Option<Uuid>,
                        address: Option<String>,
                    }
                    let mut peer_meta: HashMap<String, PeerMeta> = HashMap::new();

                    task::spawn(async move {
                        while let Some(event) = rx.recv().await {
                            match event {
                                CoordinatorEvent::RoutingUpdated { .. } => {
                                    if let Err(err) = coordinator.ask(RoutingUpdated).await {
                                        warn!(error = %err, "ClusterCoordinator: failed to forward routing update");
                                    }
                                }
                                CoordinatorEvent::PeerDiscovered { peer_id, address } => {
                                    // Cache address for later UUID resolution
                                    let entry = peer_meta.entry(peer_id.clone()).or_default();
                                    entry.address = address;
                                    info!(peer_id = %peer_id, "ClusterCoordinator: connected to raw peer, waiting for identity exchange");
                                }
                                CoordinatorEvent::PeerUuidDiscovered {
                                    peer_id,
                                    node_uuid,
                                    address,
                                } => {
                                    // This event contains the actual node UUID from DHT or Identify
                                    if let Ok(uuid) = Uuid::parse_str(&node_uuid) {
                                        let meta = peer_meta.entry(peer_id.clone()).or_default();
                                        meta.uuid = Some(uuid);
                                        if address.is_some() {
                                            meta.address = address.clone();
                                        }
                                        let resolved_addr = meta
                                            .address
                                            .clone()
                                            .unwrap_or_else(|| "unknown".to_string());

                                        if let Err(err) = coordinator
                                            .ask(PeerDiscovered {
                                                node_id: uuid,
                                                address: resolved_addr.clone(),
                                            })
                                            .await
                                        {
                                            warn!(error = %err, "ClusterCoordinator: failed to forward peer UUID discovered");
                                        } else {
                                            // also persist preferred address for future losses
                                            let meta_entry =
                                                peer_meta.entry(peer_id.clone()).or_default();
                                            if meta_entry.address.as_deref() != Some(&resolved_addr)
                                            {
                                                debug!(
                                                    peer_id = %peer_id,
                                                    addr = %resolved_addr,
                                                    "ClusterCoordinator: updating preferred address from swarm event"
                                                );
                                            }
                                            meta_entry.address = Some(resolved_addr);
                                        }
                                    } else {
                                        warn!(node_uuid = %node_uuid, "Failed to parse node UUID from DHT");
                                    }
                                }
                                CoordinatorEvent::PeerShardsDiscovered { peer_id, shards } => {
                                    // peer_id here is actually the Node UUID string from the DHT key
                                    if let Err(err) = coordinator
                                        .ask(PeerShardsDiscovered { peer_id, shards })
                                        .await
                                    {
                                        warn!(error = %err, "ClusterCoordinator: failed to forward peer shards discovered");
                                    }
                                }
                                CoordinatorEvent::PeerLost {
                                    peer_id,
                                    node_uuid,
                                    address,
                                } => {
                                    let event_addr = address.unwrap_or_default();
                                    if let Some(mut meta) = peer_meta.remove(&peer_id) {
                                        if meta.address.is_none() && !event_addr.is_empty() {
                                            debug!(
                                                peer_id = %peer_id,
                                                addr = %event_addr,
                                                "ClusterCoordinator: adopting swarm-supplied address on loss"
                                            );
                                            meta.address = Some(event_addr.clone());
                                        }
                                        if let Some(uuid) = meta.uuid {
                                            if let Err(err) =
                                                coordinator.ask(PeerLost { node_id: uuid }).await
                                            {
                                                warn!(error = %err, "ClusterCoordinator: failed to forward peer lost with uuid");
                                            }
                                        } else if let Some(uuid_str) = node_uuid {
                                            if let Ok(uuid) = Uuid::parse_str(&uuid_str) {
                                                if let Err(err) = coordinator
                                                    .ask(PeerLost { node_id: uuid })
                                                    .await
                                                {
                                                    warn!(error = %err, "ClusterCoordinator: failed to forward peer lost with uuid (from swarm)");
                                                }
                                            } else {
                                                warn!(peer_id = %peer_id, uuid = %uuid_str, "ClusterCoordinator: invalid uuid supplied for peer lost");
                                            }
                                        } else {
                                            debug!(peer_id = %peer_id, address = %meta.address.clone().unwrap_or_default(), "ClusterCoordinator: peer lost before UUID resolution");
                                        }
                                    } else {
                                        if let Some(uuid_str) = node_uuid {
                                            if let Ok(uuid) = Uuid::parse_str(&uuid_str) {
                                                if let Err(err) = coordinator
                                                    .ask(PeerLost { node_id: uuid })
                                                    .await
                                                {
                                                    warn!(error = %err, address = %event_addr, "ClusterCoordinator: failed to forward peer lost with uuid (uncached)");
                                                }
                                            } else {
                                                warn!(peer_id = %peer_id, uuid = %uuid_str, "ClusterCoordinator: invalid uuid supplied for uncached peer");
                                            }
                                        } else {
                                            debug!(peer_id = %peer_id, address = %event_addr, "ClusterCoordinator: peer lost with no cached metadata");
                                        }
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
        let mut status = self.cluster.get_cluster_status();
        // Override total_shards with the authoritative count from coordinator's assignment map
        // This includes local shards + known remote shards
        status.total_shards = self.shard_assignments.len();

        info!(
            cluster = %status.cluster_name,
            total = status.total_nodes,
            connected = status.connected_nodes,
            shards = status.total_shards,
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
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.cluster
            .peer_discovered(msg.node_id, msg.address.clone());
        let node_identity = self.format_node_identity(msg.node_id);
        info!(node = %node_identity, addr = %msg.address, "ClusterCoordinator: peer discovered");

        // Persist snapshot after peer discovery (debounced)
        self.persist_snapshot();

        // Evaluate state (e.g., WaitingForPeers -> Active if all nodes joined)
        self.evaluate_and_transition_state();

        // DHT Query Guard: Only query DHT if we don't already have shard metadata for this node
        // This avoids redundant DHT queries when Kameo push has already delivered the metadata
        let needs_dht_query = !self
            .shard_assignments
            .values()
            .any(|s| s.node_id == msg.node_id);

        if needs_dht_query && !self.bootstrap_complete {
            // Trigger DHT lookup for peer shards (bootstrap phase only)
            if let Some(handle) = self.cluster.swarm_handle() {
                if let Err(e) = handle.query_shards(msg.node_id) {
                    warn!(node = %msg.node_id, error = %e, "Failed to query peer shards from DHT");
                } else {
                    info!(node = %msg.node_id, "ClusterCoordinator: querying shards from DHT (bootstrap)");
                }
            }
        } else if needs_dht_query {
            debug!(node = %msg.node_id, "Skipping DHT query (bootstrap complete, relying on Kameo push)");
        } else {
            debug!(node = %msg.node_id, "Skipping DHT query (already have shard metadata)");
        }

        // Auto-complete bootstrap if we're in single-node mode (no expected peers)
        // This allows immediate transition to push-only mode for standalone deployments
        if !self.bootstrap_complete && self.expected_nodes.len() == 1 {
            self.bootstrap_complete = true;
            info!(
                "ClusterCoordinator: Bootstrap complete (single-node mode), switching to push-only"
            );
        }

        // Fetch shard metadata from remote coordinator in background task (Fallback/Redundancy)
        let remote_coord_name = format!("coordinator-{}", msg.node_id);
        let self_weak = ctx.actor_ref().downgrade();
        let node_id = msg.node_id;

        task::spawn(async move {
            if let Some(self_ref) = self_weak.upgrade() {
                match RemoteActorRef::<ClusterCoordinator>::lookup(remote_coord_name.clone()).await
                {
                    Ok(Some(remote_coord)) => {
                        info!(coordinator = %remote_coord_name, "Fetching shard assignments from peer");
                        match remote_coord.ask(&GetShardAssignments).await {
                            Ok(remote_shards) => {
                                if !remote_shards.is_empty() {
                                    info!(
                                        node = %node_id,
                                        shard_count = remote_shards.len(),
                                        "Merging remote shard assignments"
                                    );
                                    // Merge remote shards into local coordinator
                                    // Note: node_name will be populated from the remote response
                                    if let Err(e) = self_ref
                                        .tell(MergeRemoteShards {
                                            node_id,
                                            node_name: String::new(), // Placeholder, will be updated from peer info
                                            shards: remote_shards,
                                        })
                                        .await
                                    {
                                        warn!(node = %node_id, error = %e, "Failed to merge remote shards");
                                    }
                                }
                            }
                            Err(e) => {
                                warn!(node = %node_id, error = %e, "Failed to fetch remote shard assignments");
                            }
                        }
                    }
                    Ok(None) => {
                        info!(coordinator = %remote_coord_name, "Remote coordinator not found (may not be registered yet)");
                    }
                    Err(e) => {
                        error!(coordinator = %remote_coord_name, error = %e, "Failed to lookup remote coordinator");
                    }
                }
            }
        });
    }
}

impl Message<PeerShardsDiscovered> for ClusterCoordinator {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: PeerShardsDiscovered,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        // Parse peer_id string to UUID
        let peer_uuid = match Uuid::parse_str(&msg.peer_id) {
            Ok(uuid) => uuid,
            Err(e) => {
                warn!(peer_id = %msg.peer_id, error = %e, "Failed to parse peer UUID");
                return;
            }
        };

        let node_identity = self.format_node_identity(peer_uuid);
        info!(
            peer = %node_identity,
            shard_count = msg.shards.len(),
            "ClusterCoordinator: discovered shards from DHT"
        );

        let mut changes = 0;
        for shard in msg.shards {
            // Update or insert shard metadata
            // We trust the DHT record as it's published by the owner
            self.shard_assignments.insert(shard.shard_id, shard);
            changes += 1;
        }

        if changes > 0 {
            self.rebuild_ring();
            self.persist_snapshot();
            info!(
                total_shards = self.shard_assignments.len(),
                "ClusterCoordinator: updated ring from DHT discovery"
            );
        }

        // Auto-complete bootstrap after first successful DHT shard discovery
        // This indicates DHT is functional and we can transition to push-only mode
        if !self.bootstrap_complete && changes > 0 {
            self.bootstrap_complete = true;
            info!(
                "ClusterCoordinator: Bootstrap complete after DHT shard discovery, switching to push-only mode"
            );
        }
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
        let node_identity = self.format_node_identity(msg.node_id);
        warn!(node = %node_identity, "ClusterCoordinator: peer lost");

        // Persist snapshot after peer loss
        self.persist_snapshot();

        // Evaluate state (e.g., Active -> Degraded or Degraded -> Failed)
        self.evaluate_and_transition_state();
    }
}

/// Remote message handler to merge remote shard assignments.
#[remote_message("cameo.coordinator.merge_remote_shards")]
impl Message<MergeRemoteShards> for ClusterCoordinator {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: MergeRemoteShards,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let node_id = msg.node_id;
        let node_name = msg.node_name;
        let actual_shards = msg.shards;

        // Update peer info with node name
        if let Some(peer) = self.cluster.peer_nodes.get_mut(&node_id) {
            peer.node_name = Some(node_name.clone());
            peer.shard_count = actual_shards.len();
        }

        // Update expected_nodes with node name
        if let Some(expected) = self.expected_nodes.get_mut(&node_id) {
            expected.node_name = Some(node_name.clone());
        }

        let node_identity = self.format_node_identity(node_id);
        info!(
            node = %node_identity,
            shard_count = actual_shards.len(),
            "ClusterCoordinator: receiving remote shard push"
        );

        // Extract expected shards for this node from snapshot
        let expected_for_node: HashMap<Uuid, &ShardMetadata> = self
            .expected_shards
            .iter()
            .filter(|(_, meta)| meta.node_id == node_id)
            .map(|(id, meta)| (*id, meta))
            .collect();

        // Reconcile: compare expected vs actual
        let mut added = Vec::new();
        let mut matched = Vec::new();
        let mut changed = Vec::new();
        let mut missing = Vec::new();

        // Check actual shards against expected
        for (shard_id, actual_meta) in &actual_shards {
            if let Some(expected_meta) = expected_for_node.get(shard_id) {
                // Shard was expected - check if it changed
                if actual_meta.document_count != expected_meta.document_count
                    || actual_meta.storage_bytes != expected_meta.storage_bytes
                {
                    changed.push((*shard_id, expected_meta, actual_meta));
                } else {
                    matched.push(*shard_id);
                }
            } else {
                // Shard not in snapshot - new shard on this node
                added.push(*shard_id);
            }
        }

        // Check for shards we expected but didn't receive
        for shard_id in expected_for_node.keys() {
            if !actual_shards.contains_key(shard_id) {
                missing.push(*shard_id);
            }
        }

        // Log reconciliation results
        if !matched.is_empty() || !added.is_empty() || !changed.is_empty() || !missing.is_empty() {
            info!(
                node = %node_id,
                matched = matched.len(),
                added = added.len(),
                changed = changed.len(),
                missing = missing.len(),
                "ClusterCoordinator: reconciling node state with snapshot"
            );

            if !added.is_empty() {
                info!(node = %node_id, shards = ?added, "New shards not in snapshot");
            }
            if !changed.is_empty() {
                for (shard_id, expected, actual) in &changed {
                    info!(
                        node = %node_id,
                        shard = %shard_id,
                        expected_docs = expected.document_count,
                        actual_docs = actual.document_count,
                        expected_bytes = expected.storage_bytes,
                        actual_bytes = actual.storage_bytes,
                        "Shard state changed since snapshot"
                    );
                }
            }
            if !missing.is_empty() {
                warn!(
                    node = %node_id,
                    shards = ?missing,
                    "Expected shards from snapshot not reported by node"
                );
            }
        }

        // Update local state with actual reported shards (source of truth)
        // First, remove any existing assignments for this node that are NOT in the new list
        let mut removed_count = 0;
        self.shard_assignments.retain(|shard_id, meta| {
            if meta.node_id == node_id {
                if !actual_shards.contains_key(shard_id) {
                    removed_count += 1;
                    return false;
                }
            }
            true
        });

        let mut merged_count = 0;
        for (shard_id, actual_meta) in actual_shards {
            // Always use the node's reported state as source of truth
            self.shard_assignments.insert(shard_id, actual_meta);
            merged_count += 1;

            // Remove from expected if present (now confirmed)
            self.expected_shards.remove(&shard_id);
        }

        if merged_count > 0 || removed_count > 0 {
            self.rebuild_ring();
            info!(
                node = %node_id,
                merged_shards = merged_count,
                removed_shards = removed_count,
                total_shards = self.shard_assignments.len(),
                remaining_expected = self.expected_shards.len(),
                "ClusterCoordinator: merged remote shard assignments, ring rebuilt"
            );

            // Persist snapshot after reconciliation
            self.persist_snapshot();
        }
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

// EvaluateClusterState message removed - state evaluation now happens inline
// in PeerDiscovered and PeerLost handlers (pure reactive model)

impl Message<GetClusterSnapshot> for ClusterCoordinator {
    type Reply = ClusterSnapshot;

    async fn handle(
        &mut self,
        _msg: GetClusterSnapshot,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        ClusterSnapshot {
            config: PersistedClusterConfig {
                expected_nodes: self.expected_nodes.len(),
                generation: self.generation,
                last_stable_at: if self.state.is_healthy() {
                    Some(current_timestamp())
                } else {
                    None
                },
                cluster_name: self.cluster.cluster_config.cluster_name.clone(),
            },
            shards: self.shard_assignments.clone(),
            nodes: self.cluster.peer_nodes.clone(),
            ring: self.ring.clone(),
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

impl Message<RecordPushFailure> for ClusterCoordinator {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: RecordPushFailure,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        const PUSH_FAILURE_THRESHOLD: u32 = 3;

        let count = self.push_failure_count.entry(msg.node_id).or_insert(0);
        *count += 1;

        if *count >= PUSH_FAILURE_THRESHOLD {
            warn!(
                node = %msg.node_id,
                failure_count = *count,
                "Push failures exceeded threshold, triggering DHT fallback"
            );

            // Trigger DHT query as fallback recovery mechanism
            if let Some(handle) = self.cluster.swarm_handle() {
                if let Err(e) = handle.query_shards(msg.node_id) {
                    error!(node = %msg.node_id, error = %e, "DHT fallback query failed");
                } else {
                    info!(node = %msg.node_id, "Triggered DHT fallback query after push failures");
                }
            }
        } else {
            debug!(node = %msg.node_id, failure_count = *count, "Recorded push failure");
        }
    }
}

impl Message<ResetPushFailure> for ClusterCoordinator {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: ResetPushFailure,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        if self.push_failure_count.remove(&msg.node_id).is_some() {
            debug!(node = %msg.node_id, "Reset push failure count after successful push");
        }
    }
}

impl Message<MarkBootstrapComplete> for ClusterCoordinator {
    type Reply = ();

    async fn handle(
        &mut self,
        _msg: MarkBootstrapComplete,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        if !self.bootstrap_complete {
            self.bootstrap_complete = true;
            info!("ClusterCoordinator: Bootstrap phase complete, switching to push-only mode");
            info!("DHT is now cold storage for recovery scenarios only");
        }
    }
}

impl Message<GetKnownPeers> for ClusterCoordinator {
    type Reply = Vec<KnownPeer>;

    async fn handle(
        &mut self,
        _msg: GetKnownPeers,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.cluster
            .peer_nodes
            .values()
            .map(|info| KnownPeer {
                node_id: info.node_id,
                node_name: info.node_name.clone(),
                address: info.address.clone(),
            })
            .collect()
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
        let path = std::env::temp_dir();
        DistributedCluster::new(cfg, Uuid::new_v4(), "TST".to_string(), path)
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
                node_name: None,
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
        use cluster::generate_tokens;

        let mut cluster = make_cluster();
        let n1 = Uuid::new_v4();
        let n2 = Uuid::new_v4();
        cluster.peer_nodes.insert(
            n1,
            NodeInfo {
                node_id: n1,
                node_name: None,
                address: "127.0.0.1:9101".into(),
                status: NodeStatus::Connected,
                shard_count: 0,
            },
        );
        cluster.peer_nodes.insert(
            n2,
            NodeInfo {
                node_id: n2,
                node_name: None,
                address: "127.0.0.1:9102".into(),
                status: NodeStatus::Connected,
                shard_count: 0,
            },
        );
        let mut cc = ClusterCoordinator::new(cluster);

        let s1 = Uuid::new_v4();
        let s2 = Uuid::new_v4();
        // Use realistic token distribution across the u64 hash space
        cc.shard_assignments.insert(
            s1,
            ShardMetadata {
                shard_id: s1,
                node_id: n1,
                vnode_tokens: generate_tokens(s1),
                storage_bytes: 0,
                document_count: 0,
            },
        );
        cc.shard_assignments.insert(
            s2,
            ShardMetadata {
                shard_id: s2,
                node_id: n2,
                vnode_tokens: generate_tokens(s2),
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
