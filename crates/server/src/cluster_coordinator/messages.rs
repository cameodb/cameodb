//! The coordinator wire types: every message the actor understands, the replies it
//! returns, and the snapshot it persists.

use kameo::Reply;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

use crate::cluster_state::PersistedClusterConfig;
use crate::distributed::NodeInfo;
use cluster::ConsistentRing;

/// Message to subscribe to topology (ring) updates.
///
/// A `watch` channel: it holds the latest ring, and a subscriber that falls behind reads the
/// newest one when it catches up. The bounded queue it replaced dropped the *newest* ring once
/// full — a subscriber stalled behind a busy mailbox kept routing on an older one until the
/// next change.
#[derive(Debug, Clone)]
pub struct SubscribeTopology {
    pub subscriber: tokio::sync::watch::Sender<ConsistentRing>,
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
    pub orchestrator: kameo::actor::ActorRef<crate::node::NodeOrchestrator>,
}

/// An index deletion across all nodes: what [`delete_index_cluster`](super::delete_index_cluster)
/// is asked to do.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteIndexCluster {
    pub index: String,
    pub delete_schema: bool,
}

/// Ask the coordinator who a cluster-wide delete has to reach. Answered from its own state,
/// without waiting on anyone — see `delete_index_cluster` for why that matters.
#[derive(Debug, Clone)]
pub struct GetDeleteTargets;

/// The local orchestrator, the known peers and the pool to reach them, as the coordinator held
/// them when asked.
#[derive(Clone, Reply)]
pub struct DeleteTargets {
    pub local_orchestrator: Option<kameo::actor::ActorRef<crate::node::NodeOrchestrator>>,
    pub peers: Vec<KnownPeer>,
    pub pool: Option<std::sync::Arc<crate::remote_peer_pool::RemotePeerPool>>,
}

/// Message when a single shard is discovered via DHT.
#[derive(Debug, Clone)]
pub struct PeerShardDiscovered {
    pub node_uuid: String,
    pub shard: ShardMetadata,
}

/// Periodic tick: pull every connected peer's shard map and merge it (see
/// `spawn_shard_map_sync`). Local only; never crosses the wire.
#[derive(Debug, Clone)]
pub struct SyncShardMaps;

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
