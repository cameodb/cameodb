//! ClusterCoordinator actor wrapping DistributedCluster lifecycle and queries.
//!
//! This actor owns the DistributedCluster and provides message-based access
//! to swarm initialization, peer discovery, status queries, and shard routing.

use anyhow::Result;
use kameo::actor::RemoteActorRef;
use kameo::message::{Context, Message};
use kameo::{Actor, RemoteActor, Reply, remote_message};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
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

/// Message when node metadata is discovered via DHT.
#[derive(Debug, Clone)]
pub struct PeerNodeMetadataDiscovered {
    pub node_uuid: String,
    pub node_name: String,
    pub shard_count: u32,
    pub generation: u64,
    pub checksum: u64,
    pub address: Option<String>,
    pub status: String,
    pub total_storage_bytes: u64,
    pub total_document_count: u64,
}

/// Message to set the local orchestrator reference
#[derive(Debug, Clone)]
pub struct SetLocalOrchestrator {
    pub orchestrator: kameo::actor::ActorRef<crate::node_orchestrator::NodeOrchestrator>,
}

/// Message to coordinate index deletion across all nodes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteIndexCluster {
    pub index: String,
    pub delete_schema: bool,
}

/// Message when a single shard is discovered via DHT.
#[derive(Debug, Clone)]
pub struct PeerShardDiscovered {
    pub node_uuid: String,
    pub shard: ShardMetadata,
}

/// Message to merge remote shard assignments into local coordinator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergeRemoteShards {
    pub node_id: Uuid,
    pub node_name: String,
    pub shards: HashMap<Uuid, ShardMetadata>,
    /// Generation of the sender's cluster state (for deduplication)
    pub generation: u64,
    /// Checksum of all shard metadata (for quick comparison)
    pub shard_checksum: u64,
}

/// Message to query remote cluster state version before pushing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryClusterState {
    pub node_id: Uuid,
    pub generation: u64,
    pub shard_checksum: u64,
}

/// Response to QueryClusterState with remote state info
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterStateResponse {
    pub node_id: Uuid,
    pub generation: u64,
    pub shard_checksum: u64,
    pub needs_full_sync: bool,
}

/// Internal message to perform intelligent shard exchange with a peer
#[derive(Debug, Clone)]
pub struct ExchangeShardsWithPeer {
    pub peer_id: Uuid,
    pub generation: u64,
    pub checksum: u64,
    pub shards: HashMap<Uuid, ShardMetadata>,
}

/// Message to get complete cluster snapshot for persistence
#[derive(Debug, Clone)]
pub struct GetClusterSnapshot;

/// Internal message to track push failures for DHT fallback
#[derive(Debug, Clone)]
pub struct TrackPushFailure {
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
#[allow(dead_code)] // Reply struct for GetClusterSnapshot; no external consumer yet
pub struct ClusterSnapshot {
    pub config: PersistedClusterConfig,
    pub shards: HashMap<Uuid, ShardMetadata>,
    pub nodes: HashMap<Uuid, NodeInfo>,
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

    /// Reference to local orchestrator for coordinated operations
    local_orchestrator: Option<kameo::actor::ActorRef<crate::node_orchestrator::NodeOrchestrator>>,

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

    // Track last seen generation and checksum per node for deduplication
    last_seen_state: HashMap<Uuid, (u64, u64)>,

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
        let active_nodes = 1;
        let _inactive_nodes = total_expected.saturating_sub(active_nodes); // Prefix with _ to suppress warning

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
            bootstrap_complete: false,
            last_persisted_generation: 0,
            push_failure_count: HashMap::new(),
            last_seen_state: HashMap::new(),
            remote_peer_pool: None,
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
            bootstrap_complete: false, // Will be set after initial DHT queries complete
            last_persisted_generation: generation,
            push_failure_count: HashMap::new(),
            last_seen_state: HashMap::new(),
            remote_peer_pool: None,
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

    /// Calculate checksum of all shard metadata for quick comparison
    fn calculate_shard_checksum(&self) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();

        // Sort shard IDs for consistent hashing
        let mut shard_ids: Vec<_> = self.shard_assignments.keys().collect();
        shard_ids.sort();

        for shard_id in shard_ids {
            if let Some(meta) = self.shard_assignments.get(shard_id) {
                // Hash the key components that determine shard state
                shard_id.hash(&mut hasher);
                meta.node_id.hash(&mut hasher);
                meta.document_count.hash(&mut hasher);
                meta.storage_bytes.hash(&mut hasher);

                // Hash vnode tokens for routing consistency
                for token in &meta.vnode_tokens {
                    token.hash(&mut hasher);
                }
            }
        }

        hasher.finish()
    }

    /// Check if remote node needs our cluster state based on generation and checksum
    fn remote_needs_update(
        &self,
        remote_generation: u64,
        remote_checksum: u64,
        node_id: Uuid,
    ) -> bool {
        let local_generation = self.generation;
        let local_checksum = self.calculate_shard_checksum();

        // Check if we've seen this exact state from this node before
        if let Some((last_gen, last_checksum)) = self.last_seen_state.get(&node_id) {
            // If this node is sending us the same state we've already recorded, skip
            if *last_gen == remote_generation && *last_checksum == remote_checksum {
                return false;
            }
        }

        // If data is identical (same checksum) but generations differ, we need to sync generations
        if local_checksum == remote_checksum {
            // Data is the same, but we need to converge on the highest generation
            return remote_generation > local_generation;
        }

        // Data differs - we need to exchange
        true
    }

    /// Update the last seen state for a node
    fn update_last_seen_state(&mut self, node_id: Uuid, generation: u64, checksum: u64) {
        self.last_seen_state.insert(node_id, (generation, checksum));
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
    async fn exchange_shards_with_peer(
        &self,
        peer_id: Uuid,
        local_generation: u64,
        local_checksum: u64,
        all_shards: HashMap<Uuid, ShardMetadata>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // First, query remote node's state — use pool if available, fallback to direct lookup
        let remote_coord = if let Some(pool) = &self.remote_peer_pool {
            pool.get_coordinator(peer_id)
                .await
                .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?
                .ok_or::<Box<dyn std::error::Error + Send + Sync>>(
                    "Remote coordinator not found".into(),
                )?
        } else {
            let remote_coord_name = format!("coordinator-{}", peer_id);
            RemoteActorRef::<ClusterCoordinator>::lookup(remote_coord_name)
                .await?
                .ok_or::<Box<dyn std::error::Error + Send + Sync>>(
                    "Remote coordinator not found".into(),
                )?
        };

        let query_msg = QueryClusterState {
            node_id: self.cluster.local_node_id,
            generation: local_generation,
            shard_checksum: local_checksum,
        };

        let response = remote_coord.ask(&query_msg).await?;

        // Only push if remote node needs our data
        if response.needs_full_sync {
            debug!(
                remote_peer = %peer_id,
                remote_generation = response.generation,
                remote_checksum = response.shard_checksum,
                "Remote node needs shard update, pushing full state"
            );

            let shard_count = all_shards.len();
            let push_msg = MergeRemoteShards {
                node_id: self.cluster.local_node_id,
                node_name: self.cluster.local_node_name.clone(),
                shards: all_shards,
                generation: local_generation,
                shard_checksum: local_checksum,
            };

            remote_coord.tell(&push_msg).send()?;
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
    }

    /// Persist current cluster state snapshot to disk (event-driven)
    /// Only persists when generation changes to avoid redundant writes
    fn persist_snapshot(&mut self) {
        // Skip if generation hasn't changed since last persist
        if self.generation == self.last_persisted_generation {
            return;
        }

        if let Some(state_store) = &self.state_store {
            // Use current state for persistence
            let nodes_to_persist = self.expected_nodes.clone();
            let ring = self.ring.clone();
            let state_store = state_store.clone();
            let generation = self.generation;
            let shard_assignments = self.shard_assignments.clone();
            let cluster_name = self.cluster.cluster_config.cluster_name.clone();

            // 4. Offload blocking I/O to thread pool
            task::spawn_blocking(move || {
                let config = PersistedClusterConfig {
                    expected_nodes: nodes_to_persist.len(),
                    generation,
                    last_stable_at: Some(current_timestamp()), // Coordinator calls this when stable
                    cluster_name: cluster_name.clone(),
                };

                if let Err(e) = state_store.persist_cluster_snapshot(
                    &config,
                    &shard_assignments,
                    &nodes_to_persist,
                    &ring,
                ) {
                    error!(error = %e, "Failed to persist cluster snapshot");
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
        // First, sync expected_nodes with current peer connections and shard counts
        self.sync_expected_nodes();

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

        // Optimization: Re-publish local shards to DHT on first peer connection if still in bootstrap.
        // This ensures that even if we published while alone, our metadata reaches the network.
        if !self.bootstrap_complete && active_nodes > 1 {
            let local_node_id = self.cluster.local_node_id;
            let local_shards: Vec<_> = self
                .shard_assignments
                .values()
                .filter(|s| s.node_id == local_node_id)
                .cloned()
                .collect();

            if !local_shards.is_empty()
                && let Some(handle) = self.cluster.swarm_handle()
            {
                if let Err(e) = handle.publish_shards(
                    local_node_id,
                    self.cluster.local_node_name.clone(),
                    local_shards,
                    self.generation,
                    self.calculate_shard_checksum(),
                ) {
                    warn!(error = %e, "Failed to re-publish local shards to DHT after peer discovery");
                } else {
                    debug!(
                        "ClusterCoordinator: re-published local shards to DHT after gaining first peer"
                    );
                }
            }
        }

        // Determine the target state based on health rules
        let target_state = if inactive_nodes == 0 {
            // Cluster is stable (all expected nodes discovered and connected)
            if !self.bootstrap_complete {
                self.bootstrap_complete = true;
                info!(
                    active = active_nodes,
                    total = total_expected,
                    "ClusterCoordinator: Cluster is STABLE. All nodes discovered. Transitioning to push-only mode."
                );

                // Trigger immediate full shard metadata synchronization via actor-push
                // This ensures that once the cluster is stable, everyone gets the full map.
                let local_node_id = self.cluster.local_node_id;
                let all_shards = self.shard_assignments.clone();
                let peers: Vec<(Uuid, String)> = self
                    .cluster
                    .peer_nodes
                    .iter()
                    .filter(|(id, info)| {
                        **id != local_node_id
                            && info.status == crate::distributed::NodeStatus::Connected
                    })
                    .map(|(id, info)| (*id, info.address.clone()))
                    .collect();

                if !peers.is_empty() {
                    info!(
                        peer_count = peers.len(),
                        shard_count = all_shards.len(),
                        "ClusterCoordinator: Triggering stability-induced shard sync to all peers"
                    );
                    let pool = self.remote_peer_pool.clone();
                    for (peer_id, _) in peers {
                        let (local_generation, local_checksum) = self.get_cluster_state_info();
                        let msg = MergeRemoteShards {
                            node_id: local_node_id,
                            node_name: self.cluster.local_node_name.clone(),
                            shards: all_shards.clone(),
                            generation: local_generation,
                            shard_checksum: local_checksum,
                        };
                        let pool_clone = pool.clone();
                        task::spawn(async move {
                            let coord_opt = if let Some(pool) = &pool_clone {
                                pool.get_coordinator(peer_id).await.ok().flatten()
                            } else {
                                let name = format!("coordinator-{}", peer_id);
                                RemoteActorRef::<ClusterCoordinator>::lookup(name)
                                    .await
                                    .ok()
                                    .flatten()
                            };
                            if let Some(remote_coord) = coord_opt {
                                let _ = remote_coord.tell(&msg).send();
                            }
                        });
                    }
                }
            }

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

        // 2. Discover nodes from configuration if not already present
        for _node_addr in &self.cluster.cluster_config.cluster_nodes {
            // If the addr is in peer_nodes or expected_nodes, we'll pick it up below.
            // But we don't have easy UUID lookup from addr here.
            // Most discovery happens via PeerDiscovered/Identify.
        }

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

        // 4. Sync from peer_nodes (active connections)
        for (node_id, peer_info) in &self.cluster.peer_nodes {
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

    fn decide_route(
        &self,
        routing_key: Option<String>,
        operation_type: OperationType,
    ) -> RoutingDecision {
        // operation_type reserved for future policy; currently unused
        let _ = operation_type;

        // Debug logging for troubleshooting
        info!(
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
                    info!("RouteOperation: single-node cluster detected, routing locally");
                    return RoutingDecision::Local;
                }

                return RoutingDecision::Broadcast;
            }
        }

        // For single-node clusters, route locally instead of broadcasting
        // This handles operations without routing keys (like some admin operations)
        if self.expected_nodes.len() <= 1 {
            info!("RouteOperation: single-node cluster with no routing key, routing locally");
            return RoutingDecision::Local;
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

        // Publish local shards to DHT ONLY during bootstrap phase
        // After bootstrap, rely exclusively on Kameo push for real-time updates
        if !self.bootstrap_complete
            && let Some(handle) = self.cluster.swarm_handle()
        {
            if let Err(e) = handle.publish_shards(
                msg.node_id,
                self.cluster.local_node_name.clone(),
                msg.shards.clone(),
                self.generation,
                self.calculate_shard_checksum(),
            ) {
                warn!(error = %e, "Failed to publish shards to DHT during bootstrap");
            } else {
                info!("ClusterCoordinator: published local shards to DHT (bootstrap phase)");
            }
        }
        // Broadcast ALL known shards to all known connected peers (transitive propagation)
        // ONLY if bootstrap is complete (cluster is stable).
        // Before stability, we rely on DHT for discovery. Once stable, we use actor-push for sync.
        if self.bootstrap_complete {
            let local_node_id = self.cluster.local_node_id;
            let all_shards = self.shard_assignments.clone();
            let peers: Vec<(Uuid, String)> = self
                .cluster
                .peer_nodes
                .iter()
                .filter(|(id, info)| {
                    **id != local_node_id
                        && info.status == crate::distributed::NodeStatus::Connected
                })
                .map(|(id, info)| (*id, info.address.clone()))
                .collect();

            if !peers.is_empty() {
                info!(
                    peer_count = peers.len(),
                    shard_count = all_shards.len(),
                    "ClusterCoordinator: intelligently exchanging shard assignments with peers (stable phase)"
                );

                // Clone self reference for failure tracking callback
                let self_weak = _ctx.actor_ref().downgrade();
                let (local_generation, local_checksum) = self.get_cluster_state_info();

                for (peer_id, _peer_addr) in peers {
                    let self_weak_clone = self_weak.clone();
                    let peer_shards = all_shards.clone();
                    let peer_generation = local_generation;
                    let peer_checksum = local_checksum;

                    task::spawn(async move {
                        match self_weak_clone.upgrade() {
                            Some(self_ref) => {
                                // Send a message to perform intelligent exchange
                                let exchange_msg = ExchangeShardsWithPeer {
                                    peer_id,
                                    generation: peer_generation,
                                    checksum: peer_checksum,
                                    shards: peer_shards,
                                };
                                let _ = self_ref.tell(exchange_msg).send().await;
                            }
                            None => {
                                debug!(
                                    "ClusterCoordinator dropped during intelligent shard exchange"
                                );
                            }
                        }
                    });
                }
            }
        } else {
            debug!("ClusterCoordinator: skipping actor-push broadcast (discovery phase)");
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
                                CoordinatorEvent::PeerNodeMetadataDiscovered {
                                    node_uuid,
                                    node_name,
                                    shard_count,
                                    generation,
                                    checksum,
                                    address,
                                    status,
                                    total_storage_bytes,
                                    total_document_count,
                                } => {
                                    if let Err(err) = coordinator
                                        .ask(PeerNodeMetadataDiscovered {
                                            node_uuid,
                                            node_name,
                                            shard_count,
                                            generation,
                                            checksum,
                                            address,
                                            status,
                                            total_storage_bytes,
                                            total_document_count,
                                        })
                                        .await
                                    {
                                        warn!(error = %err, "ClusterCoordinator: failed to forward peer node metadata discovered");
                                    }
                                }
                                CoordinatorEvent::PeerShardDiscovered { node_uuid, shard } => {
                                    if let Err(err) = coordinator
                                        .ask(PeerShardDiscovered { node_uuid, shard })
                                        .await
                                    {
                                        warn!(error = %err, "ClusterCoordinator: failed to forward peer shard discovered");
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

        // Persist snapshot after peer discovery (debounced)
        self.persist_snapshot();

        // Optimized DHT Query Strategy:
        // 1. First query node metadata only (fast)
        // 2. Only query individual shards if metadata indicates changes
        let needs_dht_query = !self
            .shard_assignments
            .values()
            .any(|s| s.node_id == msg.node_id);

        if needs_dht_query && !self.bootstrap_complete {
            // Phase 1: Query node metadata (fast, small record)
            if let Some(handle) = self.cluster.swarm_handle() {
                if let Err(e) = handle.query_node_metadata(msg.node_id) {
                    warn!(node = %msg.node_id, error = %e, "Failed to query peer node metadata from DHT");
                } else {
                    info!(node = %msg.node_id, "ClusterCoordinator: querying node metadata from DHT (bootstrap phase 1)");
                }
            }
        } else if needs_dht_query {
            debug!(node = %msg.node_id, "Skipping DHT query (bootstrap complete, relying on Kameo push)");
        } else {
            debug!(node = %msg.node_id, "Already have shard metadata for this node");
        }

        // Removed redundant single-node bootstrap completion.
        // Stability is now managed centrally in evaluate_and_transition_state.

        // Collect ALL known shards to push to the newly discovered peer
        // ONLY if bootstrap is complete (cluster is stable).
        // Before stability, we rely on DHT for discovery. Once stable, we use actor-push for sync.
        if self.bootstrap_complete {
            let local_node_id = self.cluster.local_node_id;
            let local_node_name = self.cluster.local_node_name.clone();
            let all_shards = self.shard_assignments.clone();
            let (local_generation, local_checksum) = self.get_cluster_state_info();

            // Fetch shard metadata from remote coordinator AND push ALL shards in background task
            let remote_coord_name = format!("coordinator-{}", msg.node_id);
            let self_weak = ctx.actor_ref().downgrade();
            let node_id = msg.node_id;
            let pool = self.remote_peer_pool.clone();

            task::spawn(async move {
                if let Some(self_ref) = self_weak.upgrade() {
                    // Retry loop with exponential backoff for coordinator lookup
                    let mut remote_coord_opt = None;
                    for attempt in 0..5 {
                        let lookup_result = if let Some(pool) = &pool {
                            pool.get_coordinator(node_id)
                                .await
                                .map_err(|e| e.to_string())
                        } else {
                            RemoteActorRef::<ClusterCoordinator>::lookup(remote_coord_name.clone())
                                .await
                                .map_err(|e| e.to_string())
                        };

                        match lookup_result {
                            Ok(Some(coord)) => {
                                remote_coord_opt = Some(coord);
                                break;
                            }
                            Ok(None) => {
                                if attempt < 4 {
                                    let delay_ms = 100 * (1 << attempt); // 100, 200, 400, 800, 1600ms
                                    debug!(
                                        coordinator = %remote_coord_name,
                                        attempt = attempt + 1,
                                        delay_ms = delay_ms,
                                        "Remote coordinator not found, retrying..."
                                    );
                                    tokio::time::sleep(std::time::Duration::from_millis(delay_ms))
                                        .await;
                                } else {
                                    info!(coordinator = %remote_coord_name, "Remote coordinator not found after 5 attempts");
                                }
                            }
                            Err(e) => {
                                error!(coordinator = %remote_coord_name, error = %e, "Failed to lookup remote coordinator");
                                break;
                            }
                        }
                    }

                    if let Some(remote_coord) = remote_coord_opt {
                        // 1. Push ALL known shards to the remote node
                        let push_msg = MergeRemoteShards {
                            node_id: local_node_id,
                            node_name: local_node_name,
                            shards: all_shards,
                            generation: local_generation,
                            shard_checksum: local_checksum,
                        };
                        match remote_coord.tell(&push_msg).send() {
                            Ok(_) => {
                                debug!(node = %node_id, "Successfully pushed ALL shards to new peer");
                            }
                            Err(e) => {
                                warn!(node = %node_id, error = %e, "Failed to push ALL shards to new peer");
                            }
                        }

                        // 2. Fetch remote shards (existing behavior)
                        info!(coordinator = %remote_coord_name, "Fetching shard assignments from peer");
                        let shards_result: Result<HashMap<Uuid, ShardMetadata>, _> =
                            remote_coord.ask(&GetShardAssignments).await;
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
        } else {
            debug!(node = %msg.node_id, "Skipping actor-based shard exchange (discovery phase)");
        }
    }
}

impl Message<PeerNodeMetadataDiscovered> for ClusterCoordinator {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: PeerNodeMetadataDiscovered,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        // Parse node_uuid string to UUID
        let node_uuid = match Uuid::parse_str(&msg.node_uuid) {
            Ok(uuid) => uuid,
            Err(e) => {
                warn!(node_uuid = %msg.node_uuid, error = %e, "Failed to parse node UUID from metadata");
                return;
            }
        };

        let node_identity = self.format_node_identity(node_uuid);
        info!(
            peer = %node_identity,
            node_name = %msg.node_name,
            shard_count = %msg.shard_count,
            generation = %msg.generation,
            storage = %msg.total_storage_bytes,
            documents = %msg.total_document_count,
            status = %msg.status,
            "ClusterCoordinator: discovered node metadata from DHT"
        );

        // Update expected_nodes with additional information
        let address_clone = msg.address.clone();
        self.expected_nodes
            .entry(node_uuid)
            .and_modify(|node_info| {
                if let Some(ref addr) = address_clone {
                    node_info.address = addr.clone();
                }
                node_info.node_name = Some(msg.node_name.clone());
                node_info.shard_count = msg.shard_count as usize;
                // Update status based on DHT metadata
                node_info.status = match msg.status.as_str() {
                    "Connected" => crate::distributed::NodeStatus::Connected,
                    _ => crate::distributed::NodeStatus::Disconnected, // Default to Disconnected for unknown status
                };
            })
            .or_insert_with(|| NodeInfo {
                node_id: node_uuid,
                node_name: Some(msg.node_name.clone()),
                address: address_clone.unwrap_or_else(|| "unknown".to_string()),
                status: match msg.status.as_str() {
                    "Connected" => crate::distributed::NodeStatus::Connected,
                    _ => crate::distributed::NodeStatus::Disconnected, // Default to Disconnected for unknown status
                },
                shard_count: msg.shard_count as usize,
            });

        // Also update peer_nodes with shard count for cluster status calculation
        self.cluster
            .peer_nodes
            .entry(node_uuid)
            .and_modify(|node_info| {
                node_info.shard_count = msg.shard_count as usize;
                if let Some(ref addr) = msg.address {
                    node_info.address = addr.clone();
                }
                if !msg.node_name.is_empty() {
                    node_info.node_name = Some(msg.node_name.clone());
                }
                node_info.status = match msg.status.as_str() {
                    "Connected" => crate::distributed::NodeStatus::Connected,
                    _ => crate::distributed::NodeStatus::Disconnected,
                };
            })
            .or_insert_with(|| NodeInfo {
                node_id: node_uuid,
                node_name: Some(msg.node_name.clone()),
                address: msg.address.unwrap_or_else(|| "unknown".to_string()),
                status: match msg.status.as_str() {
                    "Connected" => crate::distributed::NodeStatus::Connected,
                    _ => crate::distributed::NodeStatus::Disconnected,
                },
                shard_count: msg.shard_count as usize,
            });

        // Check if we need to query individual shards
        // Only query if metadata indicates changes
        if let Some((last_gen, last_checksum)) = self.last_seen_state.get(&node_uuid)
            && *last_gen == msg.generation
            && *last_checksum == msg.checksum
        {
            debug!(
                peer = %node_identity,
                "ClusterCoordinator: node metadata unchanged, skipping shard queries"
            );
            return;
        }

        // Metadata has changed, query individual shards that we don't have
        let existing_shard_ids: std::collections::HashSet<_> = self
            .shard_assignments
            .values()
            .filter(|s| s.node_id == node_uuid)
            .map(|s| s.shard_id)
            .collect();

        // For now, we don't know which specific shard IDs to query
        // In a full implementation, we would:
        // 1. Maintain a list of known shard IDs for each node
        // 2. Query only the missing/changed shards
        // 3. Use the query_shard method for granular updates

        // TODO: Implement individual shard queries based on shard count
        // For now, we'll trigger a full shard query if we have no shards for this node
        if existing_shard_ids.is_empty() && msg.shard_count > 0 {
            info!(
                peer = %node_identity,
                shard_count = %msg.shard_count,
                "ClusterCoordinator: no local shards for peer, will query all shards"
            );
            // Fall back to querying all shards (legacy behavior)
            // We can't query individual shards without knowing their IDs
            // So we'll need to wait for the peer to push shard metadata via Kameo
            debug!(peer = %node_identity, "Waiting for shard metadata via Kameo push");
        }

        self.last_seen_state
            .insert(node_uuid, (msg.generation, msg.checksum));
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
    }
}

impl Message<DeleteIndexCluster> for ClusterCoordinator {
    type Reply = Result<JsonValue, String>;

    async fn handle(
        &mut self,
        msg: DeleteIndexCluster,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        info!(
            index = %msg.index,
            delete_schema = %msg.delete_schema,
            "ClusterCoordinator: coordinating index deletion across cluster"
        );

        // 1. Delete from local node first
        let local_result = if let Some(local_orchestrator) = &self.local_orchestrator {
            local_orchestrator
                .ask(crate::node_orchestrator::ClientOp::DeleteIndex {
                    index: msg.index.clone(),
                    delete_schema: msg.delete_schema,
                })
                .await
                .map_err(|e| {
                    crate::node_orchestrator::OrchestratorError::Io(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        format!("Failed to communicate with local orchestrator: {}", e),
                    ))
                })
        } else {
            Err(crate::node_orchestrator::OrchestratorError::Io(
                std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "Local orchestrator not available",
                ),
            ))
        };

        // 2. Forward delete request to all remote nodes in parallel
        let known_peers: Vec<KnownPeer> = self
            .cluster
            .peer_nodes
            .values()
            .map(|info| KnownPeer {
                node_id: info.node_id,
                node_name: info.node_name.clone(),
                address: info.address.clone(),
            })
            .collect();

        let pool = self.remote_peer_pool.clone();
        let remote_delete_futures: Vec<_> = known_peers
            .into_iter()
            .map(|peer| {
                let index = msg.index.clone();
                let delete_schema = msg.delete_schema;
                let pool = pool.clone();
                async move {
                    // Lookup via pool if available, fallback to direct lookup
                    let lookup_result = if let Some(pool) = &pool {
                        use crate::remote_peer_pool::ConnectionChannel;
                        pool.get_orchestrator(peer.node_id, ConnectionChannel::Operations)
                            .await
                            .map_err(|e| format!("Lookup failed for node {}: {}", peer.node_id, e))
                    } else {
                        let remote_orchestrator_name =
                            crate::node_orchestrator::orchestrator_remote_name(&peer.node_id);
                        kameo::actor::RemoteActorRef::<
                            crate::node_orchestrator::NodeOrchestrator,
                        >::lookup(remote_orchestrator_name.as_str())
                        .await
                        .map_err(|e| format!("Lookup failed for node {}: {}", peer.node_id, e))
                    };

                    let result = match lookup_result {
                        Ok(Some(remote_orchestrator)) => {
                            let delete_msg = crate::node_orchestrator::ClientOp::DeleteIndex {
                                index: index.clone(),
                                delete_schema,
                            };

                            match remote_orchestrator.ask(&delete_msg).await {
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
                                    Err(format!("Remote node {} failed: {}", peer.node_id, e))
                                }
                            }
                        }
                        Ok(None) => {
                            warn!(
                                node_id = %peer.node_id,
                                address = %peer.address,
                                "Remote orchestrator not found for index deletion"
                            );
                            Err(format!(
                                "Remote orchestrator not found for node {}",
                                peer.node_id
                            ))
                        }
                        Err(e) => {
                            warn!(
                                node_id = %peer.node_id,
                                address = %peer.address,
                                error = %e,
                                "Failed to lookup remote orchestrator for index deletion"
                            );
                            Err(e)
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

        // Check local result
        match local_result {
            Ok(_) => {
                info!("Index deletion succeeded on local node");
            }
            Err(e) => {
                error!(error = %e, "Index deletion failed on local node");
                all_errors.push(format!("Local node: {}", e));
            }
        }

        // Check remote results
        for (i, result) in remote_results.into_iter().enumerate() {
            match result {
                Ok(_) => {
                    info!("Index deletion succeeded on remote node {}", i + 1);
                }
                Err(e) => {
                    warn!(error = %e, "Index deletion failed on remote node {}", i + 1);
                    all_errors.push(format!("Remote node {}: {}", i + 1, e));
                }
            }
        }

        // Return overall result
        if all_errors.is_empty() {
            Ok(serde_json::json!({
                "status": "success",
                "message": "Index deleted successfully across all nodes",
                "index": msg.index,
                "delete_schema": msg.delete_schema
            }))
        } else {
            Err(format!(
                "Index deletion completed with errors: {}",
                all_errors.join("; ")
            ))
        }
    }
}

impl Message<PeerShardDiscovered> for ClusterCoordinator {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: PeerShardDiscovered,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        // Parse node_uuid string to UUID
        let node_uuid = match Uuid::parse_str(&msg.node_uuid) {
            Ok(uuid) => uuid,
            Err(e) => {
                warn!(node_uuid = %msg.node_uuid, error = %e, "Failed to parse node UUID from shard discovery");
                return;
            }
        };

        let node_identity = self.format_node_identity(node_uuid);
        debug!(
            peer = %node_identity,
            shard_id = %msg.shard.shard_id,
            doc_count = %msg.shard.document_count,
            "ClusterCoordinator: discovered individual shard from DHT"
        );

        // Update or insert shard metadata
        // We trust the DHT record as it's published by the owner
        let old_shard = self
            .shard_assignments
            .insert(msg.shard.shard_id, msg.shard.clone());

        // Only increment generation if this is actually a change
        if old_shard.as_ref().map(|s| s.document_count) != Some(msg.shard.document_count)
            || old_shard.as_ref().map(|s| s.storage_bytes) != Some(msg.shard.storage_bytes)
        {
            self.generation += 1;
            self.rebuild_ring();
            self.evaluate_and_transition_state();
            self.persist_snapshot();

            info!(
                peer = %node_identity,
                shard_id = %msg.shard.shard_id,
                total_shards = self.shard_assignments.len(),
                "ClusterCoordinator: updated ring from individual shard discovery"
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
        let actual_shards = msg.shards;
        let remote_generation = msg.generation;
        let remote_checksum = msg.shard_checksum;

        // Check if we actually need this update
        let (local_generation, local_checksum) = self.get_cluster_state_info();

        // Check if data is identical - if so, just sync generations and skip merge
        if local_checksum == remote_checksum {
            self.sync_generation_if_needed(remote_generation, remote_checksum);
            self.update_last_seen_state(node_id, remote_generation, remote_checksum);
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

        // If we get here, data differs and we need to do the full merge
        if !self.remote_needs_update(remote_generation, remote_checksum, node_id) {
            debug!(
                remote_node = %node_id,
                remote_generation,
                remote_checksum,
                local_generation,
                local_checksum,
                "ClusterCoordinator: skipping redundant shard push"
            );
            return;
        }

        // Always update last seen state after checking needs_update
        self.update_last_seen_state(node_id, remote_generation, remote_checksum);

        info!(
            remote_node = %node_id,
            remote_generation,
            remote_checksum,
            "ClusterCoordinator: processing needed shard push"
        );

        // Skip tracking if this is our own node (avoid double-counting in peer_nodes)
        let is_local_node = node_id == self.cluster.local_node_id;

        if !is_local_node {
            // Ensure node is tracked in peer_nodes (may arrive before PeerDiscovered event)
            self.cluster
                .peer_nodes
                .entry(node_id)
                .and_modify(|peer| {
                    if !node_name.is_empty() {
                        peer.node_name = Some(node_name.clone());
                    }
                    peer.shard_count = actual_shards.len();
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
                    shard_count: actual_shards.len(),
                });

            // Ensure node is tracked in expected_nodes (authoritative registry)
            self.expected_nodes
                .entry(node_id)
                .and_modify(|expected| {
                    if !node_name.is_empty() {
                        expected.node_name = Some(node_name.clone());
                    }
                    expected.shard_count = actual_shards.len();
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
                    shard_count: actual_shards.len(),
                });
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
            if meta.node_id == node_id && !actual_shards.contains_key(shard_id) {
                removed_count += 1;
                return false;
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

        // Always update last seen state first
        self.update_last_seen_state(msg.node_id, msg.generation, msg.shard_checksum);

        // Check if data is identical - if so, sync generations
        if local_checksum == msg.shard_checksum {
            self.sync_generation_if_needed(msg.generation, msg.shard_checksum);
        }

        let needs_full_sync =
            self.remote_needs_update(msg.generation, msg.shard_checksum, msg.node_id);

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

impl Message<RouteShard> for ClusterCoordinator {
    type Reply = Result<String>;

    async fn handle(
        &mut self,
        msg: RouteShard,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        // This method is deprecated - direct shard routing should use RouterActor
        warn!(shard_id = %msg.shard_id, "ClusterCoordinator: RouteShard message is deprecated, use RouterActor");
        Err(anyhow::anyhow!(
            "Direct shard routing is deprecated. Use RouterActor."
        ))
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

    async fn handle(
        &mut self,
        msg: ExchangeShardsWithPeer,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        // Clone shards for fallback in case the main exchange fails
        let fallback_shards = msg.shards.clone();

        match self
            .exchange_shards_with_peer(msg.peer_id, msg.generation, msg.checksum, msg.shards)
            .await
        {
            Ok(_) => {
                // Success logged in exchange_shards_with_peer
            }
            Err(e) => {
                warn!(peer = %msg.peer_id, error = %e, "Failed to exchange shards with peer");
                // Fall back to traditional push for reliability
                let fallback_coord = if let Some(pool) = &self.remote_peer_pool {
                    pool.get_coordinator(msg.peer_id).await.ok().flatten()
                } else {
                    let remote_coord_name = format!("coordinator-{}", msg.peer_id);
                    RemoteActorRef::<ClusterCoordinator>::lookup(remote_coord_name)
                        .await
                        .ok()
                        .flatten()
                };
                if let Some(remote_coord) = fallback_coord {
                    let fallback_msg = MergeRemoteShards {
                        node_id: self.cluster.local_node_id,
                        node_name: self.cluster.local_node_name.clone(),
                        shards: fallback_shards,
                        generation: msg.generation,
                        shard_checksum: msg.checksum,
                    };
                    let _ = remote_coord.tell(&fallback_msg).send();
                }
            }
        }
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

impl Message<TrackPushFailure> for ClusterCoordinator {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: TrackPushFailure,
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
                if let Err(e) = handle.query_node_metadata(msg.node_id) {
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
        // Manual trigger still allows forcing stability, but we log it as an override
        if !self.bootstrap_complete {
            warn!("ClusterCoordinator: Bootstrap phase manually marked as complete (override)");
            self.evaluate_and_transition_state();
            // If evaluate didn't set it (because not all nodes are active), force it
            if !self.bootstrap_complete {
                self.bootstrap_complete = true;
                info!("ClusterCoordinator: Forced transition to push-only mode");
            }
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
        if let Some(handle) = self.cluster.swarm_handle()
            && handle.is_running()
            && let Err(error) = handle.shutdown()
        {
            warn!(%error, "ClusterCoordinator drop: failed to signal swarm shutdown");
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
        DistributedCluster::new(
            cfg,
            Uuid::new_v4(),
            "TST".to_string(),
            path,
            64 * 1024 * 1024,
            60,
        )
    }

    #[test]
    fn decide_route_defaults_to_local_when_no_key() {
        let cc = ClusterCoordinator::new(make_cluster());
        let decision = cc.decide_route(None, OperationType::Read);
        assert!(matches!(decision, RoutingDecision::Local));
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
