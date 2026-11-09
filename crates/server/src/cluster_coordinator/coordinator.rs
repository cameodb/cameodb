//! The `ClusterCoordinator` actor: owns the `DistributedCluster` and answers every
//! message in `messages.rs`.

use super::*;

use anyhow::Result;
use kameo::actor::{RemoteActorRef, WeakActorRef};
use kameo::message::{Context, Message};
use kameo::{Actor, RemoteActor, remote_message};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::task;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::remote_peer_pool::RemotePeerPool;

use crate::cluster_state::{
    ClusterStateStore, PersistedClusterConfig, PersistedClusterTopology, current_timestamp,
};
use crate::cluster_state_machine::ClusterState;
use crate::distributed::{ClusterStatus, DistributedCluster, NodeInfo, NodeStatus};
use crate::swarm::CoordinatorEvent;
use cluster::{ConsistentRing, NodeIdentity};

/// Actor that owns the DistributedCluster instance and coordinates cluster operations.
#[derive(Actor, RemoteActor)]
pub struct ClusterCoordinator {
    pub(crate) cluster: DistributedCluster,
    pub(crate) shard_assignments: HashMap<Uuid, ShardMetadata>,
    pub(crate) ring: ConsistentRing,

    // State management
    pub(crate) state: ClusterState,
    /// Authoritative registry of all known cluster nodes (active or disconnected)
    pub(crate) expected_nodes: HashMap<Uuid, NodeInfo>,
    pub(crate) generation: u64,
    pub(crate) state_store: Option<Arc<ClusterStateStore>>,

    /// Reference to local orchestrator for coordinated operations
    pub(crate) local_orchestrator: Option<kameo::actor::ActorRef<crate::node::NodeOrchestrator>>,

    // Track expected shards from snapshot for reconciliation
    pub(crate) expected_shards: HashMap<Uuid, ShardMetadata>,

    // Subscribers for topology updates
    pub(crate) topology_subscribers: Vec<tokio::sync::watch::Sender<ConsistentRing>>,
    /// Cancels every task this coordinator spawned to talk to a peer. Fired before the swarm
    /// stops: see [`ClusterCoordinator::spawn_peer_task`].
    pub(crate) peer_tasks: tokio_util::sync::CancellationToken,

    /// The generation last handed to the snapshot writer, so an unchanged one is not written
    /// again.
    pub(crate) last_queued_generation: u64,
    /// Feeds the one task that writes snapshots, started with the first one: see
    /// [`ClusterCoordinator::persist_snapshot`].
    pub(crate) snapshot_writer: Option<tokio::sync::watch::Sender<Option<Arc<PendingSnapshot>>>>,

    /// This node's shard stores, set with the local orchestrator: read to decide a join. Weak,
    /// because shutdown releases a store's files only when the orchestrator's handle is the
    /// last; a strong one here kept every store open past the shutdown that reported it closed.
    pub(crate) local_stores: Vec<std::sync::Weak<storage::HybridStore>>,
    /// Nodes whose shards this node refused to take into its ring (V1). Kept out of the node
    /// registry, and told once.
    pub(crate) refused_nodes: std::collections::HashSet<Uuid>,

    /// Shared pool of cached RemoteActorRef handles for avoiding repeated lookups
    pub(crate) remote_peer_pool: Option<Arc<RemotePeerPool>>,
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

        let configured_nodes = cluster.cluster_config.cluster_nodes.len();
        let total_expected = configured_nodes.max(1); // At least the local node

        info!(
            generation = 1,
            expected_nodes = total_expected,
            configured_in_config = configured_nodes,
            "ClusterCoordinator: initialized"
        );

        let state = if total_expected == 1 {
            ClusterState::Active {
                generation: 1,
                active_nodes: 1,
                total_expected: 1,
            }
        } else {
            ClusterState::Degraded {
                active_nodes: 1,
                inactive_nodes: total_expected.saturating_sub(1),
            }
        };

        Self {
            cluster,
            shard_assignments: HashMap::new(),
            ring: ConsistentRing::new(),
            state,
            expected_nodes,
            generation: 1,
            state_store: None,
            local_orchestrator: None,
            expected_shards: HashMap::new(),
            topology_subscribers: Vec::new(),
            peer_tasks: tokio_util::sync::CancellationToken::new(),
            last_queued_generation: 0,
            snapshot_writer: None,
            remote_peer_pool: None,
            local_stores: Vec::new(),
            refused_nodes: std::collections::HashSet::new(),
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

        let configured_nodes = cluster.cluster_config.cluster_nodes.len();
        let discovered_nodes = expected_nodes.len();
        let total_expected = configured_nodes.max(discovered_nodes);
        let active_nodes = 1; // Only local node is initially connected
        let inactive_nodes = total_expected.saturating_sub(active_nodes);

        info!(
            generation,
            expected_nodes = total_expected,
            discovered_from_snapshot = discovered_nodes,
            configured_in_config = configured_nodes,
            expected_shards = expected_shards.len(),
            "ClusterCoordinator: restoring from persisted state"
        );

        // Start in state matching health rules
        let state = if inactive_nodes == 0 {
            ClusterState::Active {
                generation,
                active_nodes,
                total_expected,
            }
        } else if inactive_nodes == 1 {
            ClusterState::Degraded {
                active_nodes,
                inactive_nodes,
            }
        } else {
            ClusterState::Failed {
                reason: format!(
                    "Cluster restored with {}/{} nodes active ({} missing)",
                    active_nodes, total_expected, inactive_nodes
                ),
            }
        };

        // Rebuild ring from expected shards immediately
        let mut ring = ConsistentRing::new();
        for (shard_id, meta) in &expected_shards {
            let name: String = shard_id
                .simple()
                .to_string()
                .chars()
                .take(3)
                .collect::<String>();
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
            local_orchestrator: None,
            expected_shards,
            topology_subscribers: Vec::new(),
            peer_tasks: tokio_util::sync::CancellationToken::new(),
            last_queued_generation: generation,
            snapshot_writer: None,
            remote_peer_pool: None,
            local_stores: Vec::new(),
            refused_nodes: std::collections::HashSet::new(),
        }
    }

    /// Set the shared remote peer pool for cached actor ref lookups.
    pub fn set_remote_peer_pool(&mut self, pool: Arc<RemotePeerPool>) {
        self.remote_peer_pool = Some(pool);
    }

    /// Set the state store (used when creating without persisted state)
    pub fn set_state_store(&mut self, state_store: Arc<ClusterStateStore>) {
        self.state_store = Some(state_store);
    }

    /// Set cluster state (for testing or manual overrides)
    fn set_state(&mut self, state: ClusterState) {
        info!(old_state = ?self.state, new_state = ?state, "ClusterCoordinator: state transition");
        self.state = state;
        self.generation += 1;
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

    /// A checksum of the shard map, compared with a peer's to decide whether to send it.
    ///
    /// Compared across nodes, so it must come out the same on every build: xxh3 over a fixed
    /// byte layout, not `DefaultHasher`, whose output may change with the Rust release and made
    /// two nodes built apart disagree on every comparison. Shards in id order; per shard its id,
    /// owner, document count, bytes, then the token count and its vnode tokens.
    pub(crate) fn calculate_shard_checksum(&self) -> u64 {
        let mut hasher = xxhash_rust::xxh3::Xxh3::new();
        let mut shard_ids: Vec<_> = self.shard_assignments.keys().collect();
        shard_ids.sort();
        for shard_id in shard_ids {
            let meta = &self.shard_assignments[shard_id];
            hasher.update(shard_id.as_bytes());
            hasher.update(meta.node_id.as_bytes());
            hasher.update(&meta.document_count.to_le_bytes());
            hasher.update(&meta.storage_bytes.to_le_bytes());
            hasher.update(&(meta.vnode_tokens.len() as u64).to_le_bytes());
            for token in &meta.vnode_tokens {
                hasher.update(&token.to_le_bytes());
            }
        }
        hasher.digest()
    }

    /// Update local generation to match higher remote generation when data is identical
    fn sync_generation_if_needed(&mut self, remote_generation: u64, remote_checksum: u64) {
        let local_generation = self.generation;
        let local_checksum = self.calculate_shard_checksum();

        // If data is identical but remote has higher generation, update our generation
        if local_checksum == remote_checksum && remote_generation > local_generation {
            info!(
                old_generation = local_generation,
                new_generation = remote_generation,
                "Updating local generation to match remote (data identical)"
            );
            self.generation = remote_generation;
        }
    }

    /// Get current cluster state info for comparison
    fn get_cluster_state_info(&self) -> (u64, u64) {
        (self.generation, self.calculate_shard_checksum())
    }

    /// Intelligently exchange shard metadata with remote node using deduplication
    ///
    /// Takes what it reads of the coordinator as `local` rather than `&self`, because it must
    /// not run inside the coordinator's handler: it waits on the peer's coordinator, and the
    /// peer runs the same exchange on the same timer. Two coordinators each waiting on the
    /// other's mailbox from inside their own is a deadlock with no timeout to end it.
    async fn exchange_shards_with_peer(
        local: &LocalShardExchange,
        peer_id: Uuid,
        local_generation: u64,
        local_checksum: u64,
        all_shards: HashMap<Uuid, ShardMetadata>,
    ) -> Result<(), String> {
        let query_msg = QueryClusterState {
            node_id: local.node_id,
            generation: local_generation,
            shard_checksum: local_checksum,
        };
        let push_msg = MergeRemoteShards {
            node_id: local.node_id,
            node_name: local.node_name.clone(),
            shards: all_shards,
            generation: local_generation,
            shard_checksum: local_checksum,
        };

        let exchanged = converse_with_coordinator(local.pool.as_ref(), peer_id, |remote| async move {
            let response = remote.ask(&query_msg).await.map_err(|e| e.to_string())?;
            // Only push if remote node needs our data
            if response.needs_full_sync {
                debug!(
                    remote_peer = %peer_id,
                    remote_generation = response.generation,
                    remote_checksum = response.shard_checksum,
                    "Remote node needs shard update, pushing full state"
                );
                let shard_count = push_msg.shards.len();
                remote.tell(&push_msg).send().map_err(|e| e.to_string())?;
                info!(remote_peer = %peer_id, shard_count = shard_count, "Successfully pushed shard updates");
            } else {
                debug!(
                    remote_peer = %peer_id,
                    remote_generation = response.generation,
                    remote_checksum = response.shard_checksum,
                    "Remote node already has current shard state, skipping push"
                );
            }
            Ok(())
        })
        .await?;

        exchanged.ok_or_else(|| "Remote coordinator not found".to_string())
    }

    /// Hand the current cluster state to the snapshot writer, when its generation changed.
    ///
    /// One task writes every snapshot, in turn, and takes only the latest: a snapshot queued
    /// while another is being written replaces any still waiting. A blocking task per call, as
    /// before, let two writes race, so an older snapshot could commit after a newer one and the
    /// node restart on the older topology. A write that fails is logged; the next change writes
    /// the whole state again.
    pub(crate) fn persist_snapshot(&mut self) {
        if self.generation == self.last_queued_generation {
            return;
        }
        let Some(state_store) = &self.state_store else {
            return;
        };
        let snapshot = Arc::new(PendingSnapshot {
            config: PersistedClusterConfig {
                expected_nodes: self.expected_nodes.len(),
                generation: self.generation,
                last_stable_at: Some(current_timestamp()),
                cluster_name: self.cluster.cluster_config.cluster_name.clone(),
            },
            shards: self.shard_assignments.clone(),
            nodes: self.expected_nodes.clone(),
            ring: self.ring.clone(),
        });
        let writer = self.snapshot_writer.get_or_insert_with(|| {
            let (tx, rx) = tokio::sync::watch::channel(None);
            task::spawn(write_snapshots(Arc::clone(state_store), rx));
            tx
        });
        writer.send_replace(Some(snapshot));
        self.last_queued_generation = self.generation;
    }

    /// Evaluate cluster state and transition if needed (reactive, message-driven)
    /// Called after PeerDiscovered/PeerLost to update cluster state
    /// Tell the peer pool which peers are lost, so forwards to them are answered at once.
    /// Called wherever a peer's status can change.
    fn publish_lost_peers(&self) {
        if let Some(pool) = &self.remote_peer_pool {
            pool.set_lost_peers(
                self.cluster
                    .peer_nodes
                    .iter()
                    .filter(|(_, peer)| peer.status == NodeStatus::Disconnected)
                    .map(|(id, _)| *id)
                    .collect(),
            );
        }
    }

    fn evaluate_and_transition_state(&mut self) {
        // First, sync expected_nodes with current peer connections and shard counts
        self.sync_expected_nodes();
        self.publish_lost_peers();

        // Count currently connected peers + local node
        let active_nodes = self
            .expected_nodes
            .values()
            .filter(|n| n.status == crate::distributed::NodeStatus::Connected)
            .count();

        let configured_nodes = self.cluster.cluster_config.cluster_nodes.len();
        let discovered_nodes = self.expected_nodes.len();
        let total_expected = configured_nodes.max(discovered_nodes);

        let inactive_nodes = total_expected.saturating_sub(active_nodes);

        // Determine the target state based on health rules
        let target_state = if inactive_nodes == 0 {
            ClusterState::Active {
                generation: self.generation,
                active_nodes,
                total_expected,
            }
        } else if inactive_nodes == 1 {
            ClusterState::Degraded {
                active_nodes,
                inactive_nodes,
            }
        } else {
            ClusterState::Failed {
                reason: format!(
                    "Cluster failed: {}/{} nodes active ({} missing)",
                    active_nodes, total_expected, inactive_nodes
                ),
            }
        };

        // Only transition if the state variant OR internal counts have changed
        if self.state != target_state {
            self.set_state(target_state);
        }
    }

    /// Sync expected_nodes registry with current peer connections and shard counts
    fn sync_expected_nodes(&mut self) {
        // 1. Calculate authoritative shard counts from assignments
        let mut shard_counts: HashMap<Uuid, usize> = HashMap::new();
        for meta in self.shard_assignments.values() {
            *shard_counts.entry(meta.node_id).or_default() += 1;
        }

        // 2. Configured addresses carry no node id; those nodes are added on Identify, as peers.

        // 3. Update/Add nodes from shard assignments (discovery via data)
        for &node_id in shard_counts.keys() {
            self.expected_nodes
                .entry(node_id)
                .or_insert_with(|| NodeInfo {
                    node_id,
                    node_name: None,
                    address: String::new(),
                    status: NodeStatus::Disconnected,
                    shard_count: 0,
                });
        }

        // 4. Sync from peer_nodes (active connections), less the nodes this one refused (V1):
        // counted, one would keep the cluster degraded once it has gone.
        for (node_id, peer_info) in &self.cluster.peer_nodes {
            if self.refused_nodes.contains(node_id) {
                continue;
            }
            self.expected_nodes
                .entry(*node_id)
                .and_modify(|n| {
                    n.status = peer_info.status;
                    n.address = peer_info.address.clone();
                    if peer_info.node_name.is_some() {
                        n.node_name = peer_info.node_name.clone();
                    }
                })
                .or_insert_with(|| peer_info.clone());
        }

        // 5. Ensure local node is correct
        let local_id = self.cluster.local_node_id;
        self.expected_nodes
            .entry(local_id)
            .and_modify(|n| {
                n.status = NodeStatus::Connected;
                n.node_name = Some(self.cluster.local_node_name.clone());
            })
            .or_insert_with(|| NodeInfo {
                node_id: local_id,
                node_name: Some(self.cluster.local_node_name.clone()),
                address: format!("0.0.0.0:{}", self.cluster.cluster_config.cluster_port),
                status: NodeStatus::Connected,
                shard_count: 0,
            });

        // 6. Update shard counts for ALL expected nodes
        for (node_id, node_info) in self.expected_nodes.iter_mut() {
            node_info.shard_count = shard_counts.get(node_id).copied().unwrap_or(0);

            // Mark as Disconnected if not in peer_nodes and not local
            if *node_id != local_id && !self.cluster.peer_nodes.contains_key(node_id) {
                node_info.status = NodeStatus::Disconnected;
            }
        }
    }

    pub(crate) fn decide_route(
        &self,
        routing_key: Option<String>,
        operation_type: OperationType,
    ) -> RoutingDecision {
        // operation_type reserved for future policy; currently unused
        let _ = operation_type;

        // Per-request, so `debug` rather than `info`: at the level operators actually run,
        // this line and the decision below were two formatted log writes on every write and
        // every search, plus a `{:?}` of the routing key.
        debug!(
            "RouteOperation: routing_key={:?}, ring_size={}, shard_assignments={}, expected_nodes={}",
            routing_key,
            self.ring.len(),
            self.shard_assignments.len(),
            self.expected_nodes.len()
        );

        // Special handling for index deletion - always route locally
        // The local handler will coordinate with remote nodes
        if let Some(key) = routing_key {
            // Check if this looks like an index deletion (heuristic based on operation context)
            // In a proper implementation, we should pass the operation type or context
            // For now, we'll rely on the fact that delete operations come with routing keys
            // and the local orchestrator will handle the coordination

            if let Some(shard_id) = self.route_for_key(&key) {
                if let Some(owner_node) = self.shard_owner(&shard_id) {
                    if owner_node == self.cluster.local_node_id {
                        debug!(%shard_id, "RouteOperation: routing locally by key");
                        return RoutingDecision::Local;
                    } else if self.peer_is_lost(&owner_node) {
                        // Asking a lost owner waits out a timeout per attempt — 20 s for a
                        // frozen one — and ends in the same "not now". Say it at once.
                        debug!(%shard_id, node = %owner_node, "RouteOperation: owner is lost");
                        return RoutingDecision::Unavailable {
                            node_id: owner_node,
                        };
                    } else if let Some(addr) = self.node_address(&owner_node) {
                        debug!(%shard_id, node = %owner_node, addr = %addr, "RouteOperation: routing remote by key");
                        return RoutingDecision::Remote {
                            node_id: owner_node,
                            peer_addr: addr,
                        };
                    } else {
                        // An owner on the ring this process never reached: a peer still
                        // joining, or an identity that will not return — a node rebuilt with
                        // an empty data directory comes back under a new one (OB27). A
                        // broadcast cannot reach it either, and a write refused there was a
                        // `500`; it is the same "not now" as a lost owner.
                        warn!(%shard_id, node = %owner_node, "RouteOperation: owner address unknown");
                        return RoutingDecision::Unavailable {
                            node_id: owner_node,
                        };
                    }
                } else {
                    error!(%shard_id, "RouteOperation: CRITICAL - shard found but owner unknown! This indicates inconsistent state.");
                    return RoutingDecision::Broadcast;
                }
            } else {
                warn!(
                    ring_size = self.ring.len(),
                    shard_assignments = self.shard_assignments.len(),
                    "RouteOperation: ring empty or no shard found for key - this may indicate incomplete shard registration"
                );

                // For single-node clusters, route locally instead of broadcasting
                // This handles cases where shards haven't been registered yet
                if self.expected_nodes.len() <= 1 {
                    debug!("RouteOperation: single-node cluster detected, routing locally");
                    return RoutingDecision::Local;
                }

                return RoutingDecision::Broadcast;
            }
        }

        // For single-node clusters, route locally instead of broadcasting
        // This handles operations without routing keys (like some admin operations)
        if self.expected_nodes.len() <= 1 {
            debug!("RouteOperation: single-node cluster with no routing key, routing locally");
            return RoutingDecision::Local;
        }

        debug!("RouteOperation: no routing_key provided, broadcasting");
        RoutingDecision::Broadcast
    }
}

impl Message<RegisterLocalShards> for ClusterCoordinator {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: RegisterLocalShards,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        for shard in msg.shards.clone() {
            self.shard_assignments.insert(shard.shard_id, shard);
        }
        self.generation += 1;
        self.rebuild_ring();
        self.evaluate_and_transition_state();
        info!(
            node = %msg.node_id,
            total_assignments = self.shard_assignments.len(),
            "ClusterCoordinator: registered local shards"
        );

        // Persist snapshot after shard registration (debounced)
        self.persist_snapshot();

        // Exchange the whole map with every connected peer, whether or not every expected node
        // is up: one that is down is no reason to keep the others waiting (OB26).
        let local_node_id = self.cluster.local_node_id;
        let all_shards = self.shard_assignments.clone();
        let peers: Vec<Uuid> = self
            .cluster
            .peer_nodes
            .iter()
            .filter(|(id, info)| **id != local_node_id && info.status == NodeStatus::Connected)
            .map(|(id, _)| *id)
            .collect();
        if peers.is_empty() {
            return;
        }
        info!(
            peer_count = peers.len(),
            shard_count = all_shards.len(),
            "ClusterCoordinator: exchanging shard assignments with peers"
        );
        let self_weak = ctx.actor_ref().downgrade();
        let (generation, checksum) = self.get_cluster_state_info();
        for peer_id in peers {
            let self_weak = self_weak.clone();
            let shards = all_shards.clone();
            self.spawn_peer_task(async move {
                if let Some(self_ref) = self_weak.upgrade() {
                    let exchange = ExchangeShardsWithPeer {
                        peer_id,
                        generation,
                        checksum,
                        shards,
                    };
                    let _ = self_ref.tell(exchange).send().await;
                }
            });
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
        debug!(
            shard_count = self.shard_assignments.len(),
            "ClusterCoordinator: GetShardAssignments (local or remote)"
        );
        self.shard_assignments.clone()
    }
}

impl ClusterCoordinator {
    pub(crate) fn rebuild_ring(&mut self) {
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

            // Overwrites whatever the subscriber has not read yet: only the latest ring matters.
            // `send` fails only when the receiver is gone, which is the one reason to prune.
            let ring = &self.ring;
            self.topology_subscribers
                .retain(|tx| tx.send(ring.clone()).is_ok());
        }
    }

    /// Spawn a task that talks to a peer, and end it when the node starts shutting down.
    ///
    /// A peer ask still waiting when the swarm stops panics inside kameo 0.22: the swarm drops
    /// the reply channel and `ask` unwraps it (`request/ask.rs:1010`). Seen at shutdown, on an
    /// exchange spawned while the node was already going down. `ShutdownSwarm` cancels these
    /// before it stops the swarm, so a waiting task is dropped mid-await instead, and one spawned
    /// after that returns at once.
    pub(super) fn spawn_peer_task(
        &self,
        work: impl std::future::Future<Output = ()> + Send + 'static,
    ) {
        let stop = self.peer_tasks.clone();
        task::spawn(async move {
            tokio::select! {
                biased;
                _ = stop.cancelled() => {}
                _ = work => {}
            }
        });
    }

    fn route_for_key(&self, key: &str) -> Option<Uuid> {
        self.ring.get_owner(key)
    }

    fn shard_owner(&self, shard_id: &Uuid) -> Option<Uuid> {
        self.shard_assignments.get(shard_id).map(|m| m.node_id)
    }

    /// A peer this node knew and has lost: its last connection closed, or it failed a ping.
    /// A node it has never heard of is not "lost" — that is the address-unknown case.
    fn peer_is_lost(&self, node_id: &Uuid) -> bool {
        self.cluster
            .peer_nodes
            .get(node_id)
            .is_some_and(|peer| peer.status == NodeStatus::Disconnected)
    }

    /// The first live index this node holds a schema for. Schemas are the same on every member,
    /// so every member gives the same answer, which keeps their rings alike when they refuse a
    /// node — where a document count would differ from node to node.
    fn live_index(&self) -> Option<String> {
        let records = self
            .local_stores
            .first()?
            .upgrade()?
            .schema_records()
            .ok()?;
        records
            .into_iter()
            .find(|record| !record.dropped)
            .map(|record| record.index)
    }

    /// Whether any of this node's indexes holds a document. Unreadable counts as yes: the
    /// answer decides whether this node stops, and a node that cannot tell keeps running.
    pub(crate) fn holds_documents(&self) -> bool {
        // From the stored rows, not the search index: a write that is durable but not yet
        // committed to search is a document all the same.
        self.local_stores.iter().any(|store| {
            // A store already closed is a node shutting down: it is not told to stop again.
            let Some(store) = store.upgrade() else {
                return true;
            };
            store.schema_records().map_or(true, |records| {
                records
                    .iter()
                    .any(|record| store.document_count(&record.index).map_or(true, |n| n > 0))
            })
        })
    }

    /// Take out of `shards` those of every node this one has never known, when the cluster
    /// holds an index, and tell each such node once (ROADMAP V1). A node this one knows — a
    /// member restarting with its shards, its own snapshot's — is never refused, and nothing is
    /// refused while no index exists, so a fresh cluster forms as before.
    ///
    /// Returns whether `sender` is one of the refused.
    pub(crate) fn refuse_unknown_owners(
        &mut self,
        sender: Uuid,
        shards: &mut HashMap<Uuid, ShardMetadata>,
    ) -> bool {
        let local = self.cluster.local_node_id;
        let unknown: std::collections::HashSet<Uuid> = shards
            .values()
            .map(|meta| meta.node_id)
            .filter(|owner| {
                *owner != local
                    && !self
                        .shard_assignments
                        .values()
                        .any(|known| known.node_id == *owner)
            })
            .collect();
        if unknown.is_empty() {
            return false;
        }
        let Some(index) = self.live_index() else {
            return false;
        };
        shards.retain(|_, meta| !unknown.contains(&meta.node_id));
        for owner in &unknown {
            self.expected_nodes.remove(owner);
            if !self.refused_nodes.insert(*owner) {
                debug!(node = %owner, "ClusterCoordinator: shards of a refused node left out again");
                continue;
            }
            let reason = format!(
                "node {local} did not take the shards of node {owner} into its ring: the \
                 cluster holds index '{index}', and a node that brings shards the cluster has \
                 never known would take key ranges from it while no document moves, so a later \
                 write of an existing id would leave the old copy behind. Start the node on the \
                 data directory it had in this cluster, or start a new cluster for it."
            );
            error!(node = %owner, index = %index, "ClusterCoordinator: refused a node that brings new shards (V1)");
            let refusal = JoinRefused { by: local, reason };
            let pool = self.remote_peer_pool.clone();
            let owner = *owner;
            self.spawn_peer_task(async move {
                let told = converse_with_coordinator(pool.as_ref(), owner, |remote| async move {
                    remote.tell(&refusal).send().map_err(|e| e.to_string())
                })
                .await;
                if let Err(e) = told {
                    warn!(node = %owner, error = %e, "ClusterCoordinator: could not tell a refused node");
                }
            });
        }
        unknown.contains(&sender)
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
        if msg.subscriber.send(self.ring.clone()).is_ok() {
            self.topology_subscribers.push(msg.subscriber);
        }
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
                // Standalone's peer id is random: reachable at nothing, different every boot.
                if self.cluster.cluster_config.enabled {
                    info!(peer_id = %peer_id, "ClusterCoordinator: swarm initialized");
                }

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
                                    } else if let Some(uuid_str) = node_uuid {
                                        if let Ok(uuid) = Uuid::parse_str(&uuid_str) {
                                            if let Err(err) =
                                                coordinator.ask(PeerLost { node_id: uuid }).await
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
                                CoordinatorEvent::DialFailed { peer_id, error } => {
                                    if let Err(err) =
                                        coordinator.ask(DialFailed { peer_id, error }).await
                                    {
                                        warn!(error = %err, "ClusterCoordinator: failed to forward dial failed");
                                    }
                                }
                                CoordinatorEvent::PeerUnresponsive { peer_id, error } => {
                                    if let Err(err) =
                                        coordinator.ask(PeerUnresponsive { peer_id, error }).await
                                    {
                                        warn!(error = %err, "ClusterCoordinator: failed to forward peer unresponsive");
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
        // Before the swarm goes: a peer ask still waiting when it stops panics in kameo.
        self.peer_tasks.cancel();

        // `swarm_handle` is Some even in standalone, where it holds an inert handle for a
        // swarm that never started. `is_running()` is the predicate `Drop` already uses to
        // tell the two apart.
        if let Some(handle) = self.cluster.swarm_handle()
            && handle.is_running()
        {
            if let Err(err) = handle.shutdown() {
                warn!(error = %err, "ClusterCoordinator: shutdown signal failed");
                return;
            }
            info!("ClusterCoordinator: swarm shutdown signaled");

            if let Err(err) = handle
                .wait_for_shutdown(std::time::Duration::from_secs(10))
                .await
            {
                warn!(error = %err, "ClusterCoordinator: swarm runtime shutdown timed out");
            } else {
                info!("ClusterCoordinator: swarm runtime shutdown complete");
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
        // Ensure authoritative state is synced before reporting
        self.sync_expected_nodes();

        let mut status = self.cluster.get_cluster_status();

        // Use authoritative state from coordinator for health and node counts
        let (health, _total, active) = match &self.state {
            ClusterState::Active {
                active_nodes,
                total_expected,
                ..
            } => ("green", *total_expected, *active_nodes),
            ClusterState::Degraded {
                active_nodes,
                inactive_nodes,
            } => ("yellow", active_nodes + inactive_nodes, *active_nodes),
            ClusterState::Failed { .. } => {
                let connected_peers = self
                    .cluster
                    .peer_nodes
                    .values()
                    .filter(|n| n.status == NodeStatus::Connected)
                    .count();
                let active = connected_peers + 1;
                let configured_nodes = self.cluster.cluster_config.cluster_nodes.len();
                let discovered_nodes = self.expected_nodes.len();
                let _total = configured_nodes.max(discovered_nodes);
                ("red", _total, active)
            }
        };

        status.health = health.to_string();
        let configured_nodes = self.cluster.cluster_config.cluster_nodes.len();
        let discovered_nodes = self.expected_nodes.len();
        status.total_nodes = configured_nodes.max(discovered_nodes);
        status.connected_nodes = active;

        // Calculate total_shards from shard assignments which is our authoritative global map
        status.total_shards = self.shard_assignments.len();
        status.active_shards = self
            .shard_assignments
            .values()
            .filter(|s| s.node_id == self.cluster.local_node_id)
            .count();

        info!(
            cluster = %status.cluster_name,
            health = %status.health,
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

impl Message<PeerUnresponsive> for ClusterCoordinator {
    type Reply = ();

    /// Counted only; the swarm has already closed the connection, and the peer is marked lost
    /// through `PeerLost` if that was its last one.
    async fn handle(
        &mut self,
        msg: PeerUnresponsive,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.cluster.ping_failed();
        warn!(
            peer = %msg.peer_id,
            error = %msg.error,
            "ClusterCoordinator: peer failed a liveness ping"
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

        // Evaluate state (e.g., WaitingForPeers -> Active if all nodes joined)
        self.evaluate_and_transition_state();

        // A peer back from being down may hold an index dropped meanwhile, or this node may:
        // compare schema records once its orchestrator is reachable. The timer catches a peer
        // that is not by then.
        let weak = ctx.actor_ref().downgrade();
        self.spawn_peer_task(async move {
            tokio::time::sleep(SCHEMA_SWEEP_AFTER_CONNECT).await;
            if let Some(coordinator) = weak.upgrade() {
                super::sweep_schemas(&coordinator).await;
            }
        });

        // Persist snapshot after peer discovery (debounced)
        self.persist_snapshot();

        // Push this node's map to the peer and pull the peer's, on every connection: not
        // only once every expected node is up, which with one node down never happens (OB26).
        let local_node_id = self.cluster.local_node_id;
        let local_node_name = self.cluster.local_node_name.clone();
        let all_shards = self.shard_assignments.clone();
        let (local_generation, local_checksum) = self.get_cluster_state_info();

        // Fetch shard metadata from remote coordinator AND push ALL shards in background task
        let remote_coord_name = format!("coordinator-{}", msg.node_id);
        let self_weak = ctx.actor_ref().downgrade();
        let node_id = msg.node_id;
        let pool = self.remote_peer_pool.clone();

        self.spawn_peer_task(async move {
            if let Some(self_ref) = self_weak.upgrade() {
                let push_msg = MergeRemoteShards {
                    node_id: local_node_id,
                    node_name: local_node_name,
                    shards: all_shards,
                    generation: local_generation,
                    shard_checksum: local_checksum,
                };
                // Push this node's map, then fetch the peer's — one conversation, under one
                // deadline. A peer that has only just connected may not have registered its
                // coordinator yet, so that one case is asked again, backing off.
                let mut shards_result = Ok(None);
                for attempt in 0..5 {
                    let push_msg = push_msg.clone();
                    let remote_coord_name = &remote_coord_name;
                    shards_result = converse_with_coordinator(pool.as_ref(), node_id, |remote| async move {
                        match remote.tell(&push_msg).send() {
                            Ok(_) => {
                                debug!(node = %node_id, "Successfully pushed ALL shards to new peer");
                            }
                            Err(e) => {
                                warn!(node = %node_id, error = %e, "Failed to push ALL shards to new peer");
                            }
                        }
                        info!(coordinator = %remote_coord_name, "Fetching shard assignments from peer");
                        remote
                            .ask(&GetShardAssignments)
                            .await
                            .map_err(|e| e.to_string())
                    })
                    .await;
                    if !matches!(shards_result, Ok(None)) {
                        break;
                    }
                    if attempt < 4 {
                        let delay_ms = 100 * (1 << attempt); // 100, 200, 400, 800ms
                        debug!(
                            coordinator = %remote_coord_name,
                            attempt = attempt + 1,
                            delay_ms = delay_ms,
                            "Remote coordinator not found, retrying..."
                        );
                        tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                    } else {
                        info!(coordinator = %remote_coord_name, "Remote coordinator not found after 5 attempts");
                    }
                }

                if let Some(shards_result) = shards_result.transpose() {
                    match shards_result {
                        Ok(remote_shards) => {
                            if !remote_shards.is_empty() {
                                info!(
                                    node = %node_id,
                                    shard_count = remote_shards.len(),
                                    "Merging remote shard assignments"
                                );
                                // Merge remote shards into local coordinator
                                // Note: node_name will be populated from the remote response
                                // For DHT-received shards, we use placeholder generation/checksum (0) as they're from bootstrap
                                let _ = self_ref
                                    .tell::<MergeRemoteShards>(MergeRemoteShards {
                                        node_id,
                                        node_name: String::new(), // Placeholder, will be updated from peer info
                                        shards: remote_shards,
                                        generation: 0, // Placeholder for DHT bootstrap data
                                        shard_checksum: 0, // Placeholder for DHT bootstrap data
                                    })
                                    .send()
                                    .await;
                            }
                        }
                        Err(e) => {
                            warn!(node = %node_id, error = %e, "Failed to fetch remote shard assignments");
                        }
                    }
                }
            }
        });
    }
}

impl Message<SetLocalOrchestrator> for ClusterCoordinator {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: SetLocalOrchestrator,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        info!("ClusterCoordinator: received local orchestrator reference");
        self.local_orchestrator = Some(msg.orchestrator);
        self.local_stores = msg.stores.iter().map(Arc::downgrade).collect();
    }
}

/// Signalled when a member refused this node's shards and this node holds no documents: the
/// node shuts down as on a stop signal, and exits with [`JOIN_REFUSAL`] as its error.
pub static JOIN_REFUSED: tokio::sync::Notify = tokio::sync::Notify::const_new();
/// Why the cluster refused this node: the first reason a member gave.
pub static JOIN_REFUSAL: std::sync::OnceLock<String> = std::sync::OnceLock::new();

#[remote_message("cameo.coordinator.join_refused")]
impl Message<JoinRefused> for ClusterCoordinator {
    type Reply = ();

    /// A newcomer stops: it holds no documents, so stopping costs nothing, and serving would
    /// write into key ranges the cluster does not route to it. A node that holds documents is
    /// not a newcomer — two clusters met — so it keeps running, and refuses theirs in turn.
    async fn handle(
        &mut self,
        msg: JoinRefused,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        if self.holds_documents() {
            error!(
                by = %msg.by,
                reason = %msg.reason,
                "ClusterCoordinator: a node refused this node's shards; this node holds documents of its own, so it keeps running"
            );
            return;
        }
        error!(by = %msg.by, reason = %msg.reason, "ClusterCoordinator: the cluster refused this node's shards; shutting down");
        if JOIN_REFUSAL.set(msg.reason).is_ok() {
            JOIN_REFUSED.notify_one();
        }
    }
}

impl Message<GetDeleteTargets> for ClusterCoordinator {
    type Reply = DeleteTargets;

    async fn handle(
        &mut self,
        _msg: GetDeleteTargets,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        DeleteTargets {
            local_orchestrator: self.local_orchestrator.clone(),
            peers: self
                .cluster
                .peer_nodes
                .values()
                .map(|info| KnownPeer {
                    node_id: info.node_id,
                    node_name: info.node_name.clone(),
                    address: info.address.clone(),
                    connected: info.status == NodeStatus::Connected,
                })
                .collect(),
            pool: self.remote_peer_pool.clone(),
        }
    }
}

/// Delete an index on this node and on every peer, from the caller's task.
///
/// **Not a coordinator handler, and that is the point.** It was one, and it awaited the local
/// orchestrator's delete from inside the coordinator's mailbox — while the orchestrator's own
/// handlers ask the coordinator (for peers, for shard assignments, to register a shard). A
/// delete arriving while the orchestrator was inside one of those left each actor waiting on the
/// other, permanently: neither ask has a timeout. The coordinator now only hands over who to
/// ask ([`GetDeleteTargets`]), which it answers without waiting on anyone.
///
/// Returns an `OrchestratorError` rather than a `String`, so the outcome keeps its verdict. A
/// delete that could not reach one node leaves the index alive there and is worth retrying; a
/// delete the local node could not perform at all is not. Collapsed into one string both
/// arrived at the HTTP boundary as `500`, which reads as "this failed, and not because of you"
/// for the one case where a retry is exactly what the caller should do.
pub(crate) async fn delete_index_cluster(
    targets: DeleteTargets,
    msg: DeleteIndexCluster,
) -> Result<JsonValue, crate::node::OrchestratorError> {
    info!(
        index = %msg.index,
        delete_schema = %msg.delete_schema,
        "ClusterCoordinator: coordinating index deletion across cluster"
    );

    // 1. Delete from local node first
    let local_result = if let Some(local_orchestrator) = &targets.local_orchestrator {
        local_orchestrator
            .ask(crate::node::ClientOp::DeleteIndex {
                index: msg.index.clone(),
                delete_schema: msg.delete_schema,
            })
            .await
            // The orchestrator's own error, when it produced one, rather than a description
            // of it: it already carries the verdict the caller's status is read from.
            .map_err(|e| match e {
                kameo::error::SendError::HandlerError(err) => err,
                other => crate::node::OrchestratorError::NotReady(format!(
                    "Failed to communicate with local orchestrator: {}",
                    other
                )),
            })
    } else {
        Err(crate::node::OrchestratorError::NotReady(
            "Local orchestrator not available".to_string(),
        ))
    };

    // 2. Forward delete request to all remote nodes in parallel
    let known_peers = targets.peers;
    let pool = targets.pool;
    let remote_delete_futures: Vec<_> = known_peers
        .into_iter()
        .map(|peer| {
            let index = msg.index.clone();
            let delete_schema = msg.delete_schema;
            let pool = pool.clone();
            async move {
                let delete_msg = crate::node::ClientOp::DeleteIndex {
                    index: index.clone(),
                    delete_schema,
                };
                // Under the pool's deadline, lookup and ask together, the way every other
                // conversation with a peer's orchestrator runs (ROADMAP O2): a lost peer is
                // answered at once instead of dialled, and a wedged one does not hold the delete
                // past the remote timeout. `remote_answer` keeps the verdict of a peer that ran
                // the delete and refused it apart from one that never received the message.
                let answer = match &pool {
                    Some(pool) => {
                        pool.converse(peer.node_id, async {
                            let remote =
                                crate::node::lookup_peer_orchestrator(pool, peer.node_id).await?;
                            crate::node::remote_answer(remote.ask(&delete_msg).await)
                        })
                        .await
                    }
                    None => Err(crate::node::OrchestratorError::PeerUnreachable {
                        message: format!(
                            "no remote peer pool on this node to reach node {}",
                            peer.node_id
                        ),
                    }),
                };
                let result = match answer {
                    Ok(result) => {
                        info!(
                            node_id = %peer.node_id,
                            address = %peer.address,
                            "Successfully deleted index from remote node"
                        );
                        Ok(result)
                    }
                    Err(e) => {
                        warn!(
                            node_id = %peer.node_id,
                            address = %peer.address,
                            error = %e,
                            "Failed to delete index from remote node"
                        );
                        Err(format!("node {}: {}", peer.node_id, e))
                    }
                };
                (peer, result)
            }
        })
        .collect();

    let remote_results: Vec<_> = futures::future::join_all(remote_delete_futures)
        .await
        .into_iter()
        .map(|(_peer, result)| result)
        .collect();

    // Combine local and remote results
    let mut all_errors = Vec::new();

    // The local node's failure is returned as its own error, verdict intact: the caller
    // asked *this* node to delete an index and it could not, which is not a matter of
    // reaching anyone else. It is checked first for the same reason — a local fault is the
    // caller's answer even when every peer succeeded.
    if let Err(err) = local_result {
        error!(error = %err, "Index deletion failed on local node");
        return Err(err);
    }
    info!("Index deletion succeeded on local node");

    for (i, result) in remote_results.into_iter().enumerate() {
        match result {
            Ok(_) => {
                info!("Index deletion succeeded on remote node {}", i + 1);
            }
            Err(e) => {
                warn!(error = %e, "Index deletion failed on remote node {}", i + 1);
                all_errors.push(e);
            }
        }
    }

    // Return overall result
    if all_errors.is_empty() {
        return Ok(serde_json::json!({
            "status": "success",
            // "across all nodes" read as a boast on a standalone node, which has one.
            // This says the same thing and stays true whatever the node count is.
            "message": "Index deleted everywhere it was held",
            "index": msg.index,
            "delete_schema": msg.delete_schema
        }));
    }

    // Deleted here, and not confirmed on a node that did not answer. Reported as
    // unavailable rather than as a server fault because the caller's next move is to retry:
    // the delete is idempotent, the existence check ahead of it looks cluster-wide, and once
    // the node is back the retry finishes the job. A `500` would have said the opposite.
    //
    // Deliberately conservative, and it cannot be otherwise: a node that did not answer
    // cannot be asked whether it held this index, so "unreachable" and "unreachable and
    // holding it" are one case here. Announcing success while a possible holder was never
    // contacted is the worse mistake. The retry then ends in a `404` when nothing is left,
    // which is why the message says so — being told to retry and then getting a `404` reads
    // as a failure otherwise, when it is the confirmation.
    //
    // A peer that ran the delete and refused it for its own reasons is folded in here too.
    // Its verdict is not carried through: what the caller needs to know is that the index
    // may survive somewhere, and that is the same either way. The reason is preserved in the
    // message and in the warning logged above.
    Err(crate::node::OrchestratorError::PeerUnreachable {
        // The reasons go last: each one is already a sentence about a node, so any
        // phrasing that reads them as a noun ("but <reason> could not be reached")
        // comes out mangled.
        message: format!(
            "index '{}' was deleted here, but the cluster could not confirm it is gone \
                     everywhere; retry once the cluster is whole, and a 404 then means nothing \
                     is left to delete. Unconfirmed: {}",
            msg.index,
            all_errors.join("; ")
        ),
    })
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

        // Invalidate cached remote actor refs for this peer
        if let Some(pool) = &self.remote_peer_pool {
            pool.invalidate_peer(msg.node_id);
        }

        // Persist snapshot after peer loss
        self.evaluate_and_transition_state();
        self.persist_snapshot();
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
        let mut actual_shards = msg.shards;
        let remote_generation = msg.generation;
        let remote_checksum = msg.shard_checksum;

        // Check if we actually need this update
        let (local_generation, local_checksum) = self.get_cluster_state_info();

        // Check if data is identical - if so, just sync generations and skip merge
        if local_checksum == remote_checksum {
            self.sync_generation_if_needed(remote_generation, remote_checksum);
            debug!(
                remote_node = %node_id,
                remote_generation,
                remote_checksum,
                local_generation,
                local_checksum,
                "ClusterCoordinator: skipping merge - data identical, synced generations"
            );
            return;
        }

        // Shards of a node this cluster never knew are left out while it holds an index (V1).
        if self.refuse_unknown_owners(node_id, &mut actual_shards) {
            return;
        }

        // No "seen this state from this node before" skip. It keyed on the (generation,
        // checksum) a push carried, and three handlers wrote that key with three meanings: a
        // peer's DHT metadata, a state query, and a merge. A push whose pair matched the peer's
        // DHT metadata was dropped as a repeat before its shards were ever merged, and every pull
        // carries the placeholder pair (0, 0), so only a node's first pull from a peer merged —
        // a ring that missed a peer in the formation burst stayed partial for good (OB20). The
        // merge is idempotent and what it costs is decided below by what actually changed.
        debug!(
            remote_node = %node_id,
            remote_generation,
            remote_checksum,
            "ClusterCoordinator: processing shard push"
        );

        // Skip tracking if this is our own node (avoid double-counting in peer_nodes)
        let is_local_node = node_id == self.cluster.local_node_id;

        // What the sender owns, not what it sent: a push carries the sender's whole map, other
        // nodes' shards included.
        let owned_by_sender = actual_shards
            .values()
            .filter(|meta| meta.node_id == node_id)
            .count();

        if !is_local_node {
            // Ensure node is tracked in peer_nodes (may arrive before PeerDiscovered event)
            self.cluster
                .peer_nodes
                .entry(node_id)
                .and_modify(|peer| {
                    if !node_name.is_empty() {
                        peer.node_name = Some(node_name.clone());
                    }
                    peer.shard_count = owned_by_sender;
                    peer.status = NodeStatus::Connected;
                })
                .or_insert_with(|| NodeInfo {
                    node_id,
                    node_name: if node_name.is_empty() {
                        None
                    } else {
                        Some(node_name.clone())
                    },
                    address: String::new(), // Will be updated by PeerDiscovered
                    status: NodeStatus::Connected,
                    shard_count: owned_by_sender,
                });

            // Ensure node is tracked in expected_nodes (authoritative registry)
            self.expected_nodes
                .entry(node_id)
                .and_modify(|expected| {
                    if !node_name.is_empty() {
                        expected.node_name = Some(node_name.clone());
                    }
                    expected.shard_count = owned_by_sender;
                    expected.status = NodeStatus::Connected;
                })
                .or_insert_with(|| NodeInfo {
                    node_id,
                    node_name: if node_name.is_empty() {
                        None
                    } else {
                        Some(node_name.clone())
                    },
                    address: String::new(),
                    status: NodeStatus::Connected,
                    shard_count: owned_by_sender,
                });
        }

        self.publish_lost_peers();

        let node_identity = self.format_node_identity(node_id);
        debug!(
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
            if meta.node_id == node_id && !actual_shards.contains_key(shard_id) {
                removed_count += 1;
                return false;
            }
            true
        });

        // Only a change to where shards live moves the generation, the ring and the snapshot.
        // Document counts and sizes differ between any two nodes under writes, so a merge that
        // bumped the generation for them would do so on nearly every periodic sync.
        let mut merged_count = 0;
        for (shard_id, actual_meta) in actual_shards {
            let routing_changed = self.shard_assignments.get(&shard_id).is_none_or(|known| {
                known.node_id != actual_meta.node_id
                    || known.vnode_tokens != actual_meta.vnode_tokens
            });
            // Always use the node's reported state as source of truth
            self.shard_assignments.insert(shard_id, actual_meta);
            if routing_changed {
                merged_count += 1;
            }

            // Remove from expected if present (now confirmed)
            self.expected_shards.remove(&shard_id);
        }

        if merged_count > 0 || removed_count > 0 {
            self.generation += 1;
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
            self.evaluate_and_transition_state();
            self.persist_snapshot();
        }
    }
}

/// Remote message handler to query cluster state version for deduplication.
#[remote_message("cameo.coordinator.query_cluster_state")]
impl Message<QueryClusterState> for ClusterCoordinator {
    type Reply = Result<ClusterStateResponse, String>;

    async fn handle(
        &mut self,
        msg: QueryClusterState,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let (local_generation, local_checksum) = self.get_cluster_state_info();

        // Check if data is identical - if so, sync generations
        if local_checksum == msg.shard_checksum {
            self.sync_generation_if_needed(msg.generation, msg.shard_checksum);
        }

        // The caller needs our map when it holds different data. This used to record the
        // caller's state first and then ask whether that state had been seen before — which it
        // always just had, so the answer was always "no" and the exchange never pushed (OB20).
        let needs_full_sync = local_checksum != msg.shard_checksum;

        debug!(
            remote_node = %msg.node_id,
            remote_generation = msg.generation,
            remote_checksum = msg.shard_checksum,
            local_generation,
            local_checksum,
            needs_full_sync,
            "ClusterCoordinator: received cluster state query"
        );

        Ok(ClusterStateResponse {
            node_id: self.cluster.local_node_id,
            generation: local_generation,
            shard_checksum: local_checksum,
            needs_full_sync,
        })
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

impl Message<ExchangeShardsWithPeer> for ClusterCoordinator {
    type Reply = ();

    /// Snapshots what the exchange reads and runs it in a task of its own: the exchange waits
    /// on the peer's coordinator, which is running this same exchange against this node. Awaited
    /// here, each held its own mailbox while waiting on the other's — see
    /// `exchange_shards_with_peer`. What comes back arrives as messages (`MergeRemoteShards`),
    /// so nothing needs the coordinator's state after the snapshot.
    async fn handle(
        &mut self,
        msg: ExchangeShardsWithPeer,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let local = LocalShardExchange {
            pool: self.remote_peer_pool.clone(),
            node_id: self.cluster.local_node_id,
            node_name: self.cluster.local_node_name.clone(),
        };
        self.spawn_peer_task(async move {
            // Clone shards for fallback in case the main exchange fails
            let fallback_shards = msg.shards.clone();

            match Self::exchange_shards_with_peer(
                &local,
                msg.peer_id,
                msg.generation,
                msg.checksum,
                msg.shards,
            )
            .await
            {
                Ok(_) => {
                    // Success logged in exchange_shards_with_peer
                }
                Err(e) => {
                    warn!(peer = %msg.peer_id, error = %e, "Failed to exchange shards with peer");
                    // Fall back to traditional push for reliability
                    let fallback_msg = MergeRemoteShards {
                        node_id: local.node_id,
                        node_name: local.node_name.clone(),
                        shards: fallback_shards,
                        generation: msg.generation,
                        shard_checksum: msg.checksum,
                    };
                    let pushed = converse_with_coordinator(
                        local.pool.as_ref(),
                        msg.peer_id,
                        |remote| async move {
                            remote.tell(&fallback_msg).send().map_err(|e| e.to_string())
                        },
                    )
                    .await;
                    if let Err(e) = pushed {
                        debug!(peer = %msg.peer_id, error = %e, "fallback shard push failed");
                    }
                }
            }
        });
    }
}

/// How often each node pulls its peers' shard maps.
const SHARD_MAP_SYNC_INTERVAL: std::time::Duration = std::time::Duration::from_secs(10);
/// Bound on one conversation with a peer's coordinator — the lookup and the ask together — so
/// a dead peer costs a task a few seconds, not forever: kameo remote asks carry no reply
/// timeout of their own.
const SHARD_MAP_FETCH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// One conversation with a peer's coordinator — finding it, then whatever `ask` does with it —
/// under [`SHARD_MAP_FETCH_TIMEOUT`] as a whole.
///
/// Every shard-map exchange goes through here. Before, the exchange on a peer's discovery, the
/// stability push and the exchange's fallback looked the peer up and asked it with no deadline
/// at all, so a wedged peer held the task that asked it until shutdown (ROADMAP O2). A peer the
/// pool knows is lost is not asked. `Ok(None)` is a peer whose coordinator is not registered
/// yet — kept apart from a failure, because a peer that has only just connected registers in a
/// moment and is worth asking again.
pub(super) async fn converse_with_coordinator<T, Fut>(
    pool: Option<&Arc<RemotePeerPool>>,
    peer: Uuid,
    ask: impl FnOnce(RemoteActorRef<ClusterCoordinator>) -> Fut,
) -> Result<Option<T>, String>
where
    Fut: std::future::Future<Output = Result<T, String>>,
{
    if pool.is_some_and(|pool| pool.is_lost(peer)) {
        return Err(format!("node {peer} is not reachable"));
    }
    let conversation = async {
        let found = match pool {
            Some(pool) => pool
                .get_coordinator(peer)
                .await
                .map_err(|e| e.to_string())?,
            None => RemoteActorRef::<ClusterCoordinator>::lookup(format!("coordinator-{peer}"))
                .await
                .map_err(|e| e.to_string())?,
        };
        match found {
            Some(coordinator) => ask(coordinator).await.map(Some),
            None => Ok(None),
        }
    };
    match tokio::time::timeout(SHARD_MAP_FETCH_TIMEOUT, conversation).await {
        Ok(answer) => answer,
        Err(_) => {
            // A cached ref to a peer that stopped answering is the likeliest reason; drop it so
            // the next conversation looks the peer up afresh.
            if let Some(pool) = pool {
                pool.invalidate_peer(peer);
            }
            Err(format!(
                "node {peer} did not answer within {}s",
                SHARD_MAP_FETCH_TIMEOUT.as_secs()
            ))
        }
    }
}

/// How long after a peer connects its schema records are compared with this node's.
const SCHEMA_SWEEP_AFTER_CONNECT: std::time::Duration = std::time::Duration::from_secs(2);
/// How often each node compares its schema records with its peers' — see `sweep_schemas`.
const SCHEMA_SWEEP_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);

impl Message<StartPeriodicSync> for ClusterCoordinator {
    type Reply = ();

    /// Start a clustered node's two timers: the shard-map sync and the schema sweep. Each is a
    /// peer task, ended by `ShutdownSwarm` with the rest, since either can be waiting on a peer
    /// when the swarm stops. Nodes are phase-shifted by their id, so nodes started together do
    /// not tick in the same instant.
    async fn handle(
        &mut self,
        _msg: StartPeriodicSync,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let id = *self.cluster.local_node_id.as_bytes();
        self.spawn_peer_task(sync_shard_maps_every(
            ctx.actor_ref().downgrade(),
            std::time::Duration::from_millis(u64::from(id[0]) * 20),
        ));
        self.spawn_peer_task(sweep_schemas_every(
            ctx.actor_ref().downgrade(),
            std::time::Duration::from_millis(u64::from(id[1]) * 40),
        ));
    }
}

/// The periodic schema sweep. Ends with the coordinator.
async fn sweep_schemas_every(weak: WeakActorRef<ClusterCoordinator>, phase: std::time::Duration) {
    let start = tokio::time::Instant::now() + SCHEMA_SWEEP_INTERVAL + phase;
    let mut ticks = tokio::time::interval_at(start, SCHEMA_SWEEP_INTERVAL);
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticks.tick().await;
        let Some(coordinator) = weak.upgrade() else {
            break;
        };
        super::sweep_schemas(&coordinator).await;
    }
}

/// The periodic shard-map sync.
///
/// Shard maps otherwise move only when a node's own shards change, and a fresh cluster trades
/// them in one burst of tens of milliseconds, so a map missed in that burst stayed missed (OB20).
/// Each tick pulls every connected peer's map and merges it; a map that matches costs one ask per
/// peer and changes nothing. Ends with the coordinator.
async fn sync_shard_maps_every(weak: WeakActorRef<ClusterCoordinator>, phase: std::time::Duration) {
    let start = tokio::time::Instant::now() + SHARD_MAP_SYNC_INTERVAL + phase;
    let mut ticks = tokio::time::interval_at(start, SHARD_MAP_SYNC_INTERVAL);
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticks.tick().await;
        let Some(coordinator) = weak.upgrade() else {
            break;
        };
        if coordinator.tell(SyncShardMaps).await.is_err() {
            break;
        }
    }
}

/// A cluster snapshot waiting for the writer: what `persist_snapshot` read of the coordinator.
pub(crate) struct PendingSnapshot {
    config: PersistedClusterConfig,
    shards: HashMap<Uuid, ShardMetadata>,
    nodes: HashMap<Uuid, NodeInfo>,
    ring: ConsistentRing,
}

/// The snapshot writer: one write at a time, always of the latest snapshot queued. Ends when
/// the coordinator is gone, after writing one it queued last.
async fn write_snapshots(
    store: Arc<ClusterStateStore>,
    mut pending: tokio::sync::watch::Receiver<Option<Arc<PendingSnapshot>>>,
) {
    while pending.changed().await.is_ok() {
        let Some(snapshot) = pending.borrow_and_update().clone() else {
            continue;
        };
        let store = Arc::clone(&store);
        let written = task::spawn_blocking(move || {
            store
                .persist_cluster_snapshot(
                    &snapshot.config,
                    &snapshot.shards,
                    &snapshot.nodes,
                    &snapshot.ring,
                )
                .map(|()| snapshot)
        })
        .await;
        match written {
            Ok(Ok(snapshot)) => info!(
                generation = snapshot.config.generation,
                shards = snapshot.shards.len(),
                nodes = snapshot.nodes.len(),
                "Cluster snapshot persisted"
            ),
            Ok(Err(e)) => error!(error = %e, "Failed to persist cluster snapshot"),
            Err(e) => error!(error = %e, "Cluster snapshot write did not finish"),
        }
    }
}

impl Message<SyncShardMaps> for ClusterCoordinator {
    type Reply = ();

    /// Snapshots who to ask and fetches from a task per peer: the fetch waits on the peer's
    /// coordinator, and awaiting it here would hold this mailbox while the peer runs the same
    /// sync against it (the OB19 shape). What comes back arrives as `MergeRemoteShards`.
    async fn handle(
        &mut self,
        _msg: SyncShardMaps,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let Some(pool) = self.remote_peer_pool.clone() else {
            return;
        };
        let local_node_id = self.cluster.local_node_id;
        let peers: Vec<Uuid> = self
            .cluster
            .peer_nodes
            .iter()
            .filter(|(id, info)| **id != local_node_id && info.status == NodeStatus::Connected)
            .map(|(id, _)| *id)
            .collect();
        let self_ref = ctx.actor_ref().downgrade();
        for peer in peers {
            let pool = Arc::clone(&pool);
            let self_ref = self_ref.clone();
            self.spawn_peer_task(async move {
                let fetched = converse_with_coordinator(Some(&pool), peer, |remote| async move {
                    remote
                        .ask(&GetShardAssignments)
                        .await
                        .map_err(|e| e.to_string())
                })
                .await;
                let shards = match fetched {
                    Ok(Some(shards)) => shards,
                    Ok(None) => {
                        debug!(peer = %peer, "shard-map sync: peer coordinator not registered");
                        return;
                    }
                    Err(e) => {
                        debug!(peer = %peer, error = %e, "shard-map sync: fetch failed");
                        return;
                    }
                };
                if let Some(coordinator) = self_ref.upgrade() {
                    let _ = coordinator
                        .tell(MergeRemoteShards {
                            node_id: peer,
                            node_name: String::new(),
                            shards,
                            // A pull has no pair of its own to report; the merge no longer keys
                            // on it, and (0, 0) never equals a real checksum.
                            generation: 0,
                            shard_checksum: 0,
                        })
                        .await;
                }
            });
        }
    }
}

/// What a shard exchange reads of this coordinator, taken before it leaves the actor.
struct LocalShardExchange {
    pool: Option<Arc<RemotePeerPool>>,
    node_id: Uuid,
    node_name: String,
}

impl Message<RequestBootstrapRedial> for ClusterCoordinator {
    type Reply = Result<()>;

    async fn handle(
        &mut self,
        msg: RequestBootstrapRedial,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        // The swarm decides: it redials only when the node has no peer at all, so a caller
        // that hits a routing failure on a connected node costs one channel send.
        debug!(reason = %msg.reason, "RequestBootstrapRedial: asking the swarm to check its seeds");
        if let Some(handle) = self.cluster.swarm_handle()
            && let Err(e) = handle.request_seed_redial()
        {
            warn!(reason = %msg.reason, error = %e, "RequestBootstrapRedial: swarm unavailable");
        }
        Ok(())
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
                connected: info.status == NodeStatus::Connected,
            })
            .collect()
    }
}

impl Message<GetBroadcastPeers> for ClusterCoordinator {
    type Reply = Vec<KnownPeer>;

    /// The discovered peers, then every expected node not discovered yet, as not connected.
    ///
    /// A node restarting knows the cluster from its snapshot before it has rediscovered it.
    /// Fanning out to the discovered peers alone, a search on it skipped a peer it had not met
    /// again and answered 5 of 9 shards as `failed: 0` — a partial answer claiming to be whole
    /// (ROADMAP OB25). Listed here, that peer is counted as a node that did not answer, the way
    /// a lost one already is.
    async fn handle(
        &mut self,
        _msg: GetBroadcastPeers,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let local = self.cluster.local_node_id;
        let peer_nodes = &self.cluster.peer_nodes;
        let undiscovered = self
            .expected_nodes
            .values()
            .filter(|node| node.node_id != local && !peer_nodes.contains_key(&node.node_id))
            .map(|node| KnownPeer {
                node_id: node.node_id,
                node_name: node.node_name.clone(),
                address: node.address.clone(),
                connected: false,
            });
        peer_nodes
            .values()
            .map(|info| KnownPeer {
                node_id: info.node_id,
                node_name: info.node_name.clone(),
                address: info.address.clone(),
                connected: info.status == NodeStatus::Connected,
            })
            .chain(undiscovered)
            .collect()
    }
}

// ============================================================================
// Cleanup on Drop
// ============================================================================

impl Drop for ClusterCoordinator {
    fn drop(&mut self) {
        if let Some(handle) = self.cluster.swarm_handle()
            && handle.is_running()
            && let Err(error) = handle.shutdown()
        {
            warn!(%error, "ClusterCoordinator drop: failed to signal swarm shutdown");
        }
    }
}
