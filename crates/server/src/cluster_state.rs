//! Persistent Cluster State Management
//!
//! This module provides persistent storage for cluster topology, node registry,
//! and consistent hash ring snapshots using a dedicated redb database.
//!
//! ## Database Location
//! `{storage_path}/metadata.redb`
//!
//! ## Tables
//! - `cluster_config`: Cluster-wide configuration and generation tracking
//! - `shard_assignments`: Shard-to-node mappings with state tracking
//! - `node_registry`: Known nodes with connectivity state
//! - `ring_snapshot`: Serialized ConsistentRing for fast recovery

use anyhow::{Context as AnyhowContext, Result};
use bincode_next::config::legacy;
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::info;
use uuid::Uuid;

use crate::cluster_coordinator::ShardMetadata;
use crate::distributed::{NodeInfo, NodeStatus};
use cluster::ConsistentRing;

// ============================================================================
// Table Definitions
// ============================================================================

/// Cluster-wide configuration (singleton record, key="current")
const TABLE_CLUSTER_CONFIG: TableDefinition<&str, &[u8]> = TableDefinition::new("cluster_config");

/// Shard assignments: shard_id (Uuid bytes) -> PersistedShardAssignment
const TABLE_SHARD_ASSIGNMENTS: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("shard_assignments");

/// Node registry: node_id (Uuid bytes) -> PersistedNodeInfo
const TABLE_NODE_REGISTRY: TableDefinition<&[u8], &[u8]> = TableDefinition::new("node_registry");

/// Ring snapshot: key="latest" -> RingSnapshot
const TABLE_RING_SNAPSHOT: TableDefinition<&str, &[u8]> = TableDefinition::new("ring_snapshot");

// ============================================================================
// Data Structures
// ============================================================================

/// Cluster-wide configuration persisted to disk
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedClusterConfig {
    /// Expected number of nodes in cluster (from config or last stable state)
    pub expected_nodes: usize,
    /// Cluster generation number (increments on topology change)
    pub generation: u64,
    /// Timestamp of last stable cluster state
    pub last_stable_at: Option<u64>,
    /// Cluster name for validation
    pub cluster_name: String,
}

/// Shard assignment with state tracking
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedShardAssignment {
    pub shard_id: Uuid,
    pub node_id: Uuid,
    pub vnode_tokens: Vec<u64>,
    pub storage_bytes: u64,
    pub document_count: u64,
    /// State of this shard assignment
    pub state: ShardAssignmentState,
    /// Last time this shard was seen active
    pub last_seen: u64,
}

/// State of a shard assignment
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ShardAssignmentState {
    /// Shard is healthy and actively serving requests
    Active,
    /// Expected from persisted state but not yet confirmed
    Pending,
    /// Not seen for a while, may be temporarily unavailable
    Stale,
    /// Shard is being migrated to another node
    Migrating,
}

/// Node information with connectivity state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedNodeInfo {
    pub node_id: Uuid,
    pub address: String,
    pub shard_count: usize,
    pub first_seen: u64,
    pub last_seen: u64,
    /// Current node state
    pub state: NodeState,
}

/// State of a node in the cluster
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum NodeState {
    /// Node is connected and healthy
    Active,
    /// Expected from last run but not yet joined
    Pending,
    /// Node is in process of joining cluster
    Joining,
    /// Node is gracefully shutting down
    Leaving,
    /// Connection lost, may return
    Lost,
    /// Permanently removed from cluster
    Removed,
}

/// Snapshot of the consistent hash ring
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RingSnapshot {
    /// Generation number matching cluster config
    pub generation: u64,
    /// Number of nodes in the ring
    pub node_count: usize,
    /// Number of shards in the ring
    pub shard_count: usize,
    /// Serialized ConsistentRing data (bincode)
    pub ring_data: Vec<u8>,
    /// When this snapshot was created
    pub created_at: u64,
}

/// Complete cluster topology from persisted state
#[derive(Debug, Clone)]
pub struct PersistedClusterTopology {
    pub config: PersistedClusterConfig,
    pub shards: HashMap<Uuid, PersistedShardAssignment>,
    pub nodes: HashMap<Uuid, PersistedNodeInfo>,
    #[allow(dead_code)] // Reserved for future ring persistence optimization
    pub ring: Option<RingSnapshot>,
}

// ============================================================================
// ClusterStateStore
// ============================================================================

/// Persistent storage manager for cluster state
pub struct ClusterStateStore {
    db: Arc<Database>,
}

impl ClusterStateStore {
    /// Create or open the cluster metadata database
    pub fn new(storage_path: PathBuf) -> Result<Self> {
        let db_path = storage_path.join("metadata.redb");

        // Create parent directory if needed
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("Failed to create metadata dir: {:?}", parent))?;
        }

        let db = Database::create(&db_path)
            .with_context(|| format!("Failed to create metadata database: {:?}", db_path))?;

        info!("Cluster state database opened at {:?}", db_path);

        Ok(Self { db: Arc::new(db) })
    }

    /// Load complete persisted cluster topology
    pub fn load_persisted_cluster(&self) -> Result<Option<PersistedClusterTopology>> {
        let txn = self.db.begin_read()?;

        // Load cluster config (if exists, indicates this is an existing cluster)
        let config = match self.load_cluster_config(&txn)? {
            Some(c) => c,
            None => {
                info!("No persisted cluster config found, treating as fresh cluster");
                return Ok(None);
            }
        };

        // Load shard assignments
        let shards = self.load_shard_assignments(&txn)?;
        info!("Loaded {} shard assignments from persistence", shards.len());

        // Load node registry
        let nodes = self.load_node_registry(&txn)?;
        info!("Loaded {} nodes from registry", nodes.len());

        // Load ring snapshot
        let ring = self.load_ring_snapshot(&txn)?;
        if ring.is_some() {
            info!("Loaded ring snapshot from persistence");
        }

        Ok(Some(PersistedClusterTopology {
            config,
            shards,
            nodes,
            ring,
        }))
    }

    /// Persist complete cluster snapshot atomically
    pub fn persist_cluster_snapshot(
        &self,
        config: &PersistedClusterConfig,
        shards: &HashMap<Uuid, ShardMetadata>,
        nodes: &HashMap<Uuid, NodeInfo>,
        ring: &ConsistentRing,
    ) -> Result<()> {
        let txn = self.db.begin_write()?;

        // Write cluster config
        {
            let mut table = txn.open_table(TABLE_CLUSTER_CONFIG)?;
            let config_bytes = bincode_next::serde::encode_to_vec(config, legacy())?;
            table.insert("current", config_bytes.as_slice())?;
        }

        // Write shard assignments and prune stale ones
        {
            let mut table = txn.open_table(TABLE_SHARD_ASSIGNMENTS)?;

            // 1. Insert/Update current shards
            for (shard_id, meta) in shards {
                let key_bytes = shard_id.as_bytes();
                let persisted = PersistedShardAssignment {
                    shard_id: *shard_id,
                    node_id: meta.node_id,
                    vnode_tokens: meta.vnode_tokens.clone(),
                    storage_bytes: meta.storage_bytes,
                    document_count: meta.document_count,
                    state: ShardAssignmentState::Active,
                    last_seen: current_timestamp(),
                };
                let value_bytes = bincode_next::serde::encode_to_vec(&persisted, legacy())?;
                table.insert(key_bytes.as_slice(), value_bytes.as_slice())?;
            }

            // 2. Prune shards not in the current map
            // We collect keys to delete first to avoid holding iterator while mutating
            let mut keys_to_delete = Vec::new();
            for result in table.iter()? {
                let (key, _) = result?;
                let key_slice = key.value();
                if key_slice.len() == 16 {
                    let uuid = Uuid::from_bytes(key_slice.try_into().unwrap());
                    if !shards.contains_key(&uuid) {
                        keys_to_delete.push(uuid);
                    }
                }
            }

            for shard_id in keys_to_delete {
                table.remove(shard_id.as_bytes().as_slice())?;
                // info!(%shard_id, "Pruned stale shard assignment from persistence");
            }
        }

        // Write node registry and prune stale nodes
        {
            let mut table = txn.open_table(TABLE_NODE_REGISTRY)?;

            // 1. Insert/Update current nodes
            for (node_id, info) in nodes {
                let key_bytes = node_id.as_bytes();
                let persisted = PersistedNodeInfo {
                    node_id: *node_id,
                    address: info.address.clone(),
                    shard_count: info.shard_count,
                    first_seen: current_timestamp(), // Ideally we'd preserve the original first_seen
                    last_seen: current_timestamp(),
                    state: match info.status {
                        NodeStatus::Connected => NodeState::Active,
                        NodeStatus::Disconnected => NodeState::Lost,
                    },
                };
                let value_bytes = bincode_next::serde::encode_to_vec(&persisted, legacy())?;
                table.insert(key_bytes.as_slice(), value_bytes.as_slice())?;
            }

            // 2. Prune nodes not in the current map
            let mut keys_to_delete = Vec::new();
            for result in table.iter()? {
                let (key, _) = result?;
                let key_slice = key.value();
                if key_slice.len() == 16 {
                    let uuid = Uuid::from_bytes(key_slice.try_into().unwrap());
                    if !nodes.contains_key(&uuid) {
                        keys_to_delete.push(uuid);
                    }
                }
            }

            for node_id in keys_to_delete {
                table.remove(node_id.as_bytes().as_slice())?;
                // info!(%node_id, "Pruned stale node from registry");
            }
        }

        // Write ring snapshot
        {
            let mut table = txn.open_table(TABLE_RING_SNAPSHOT)?;
            let ring_snapshot = RingSnapshot {
                generation: config.generation,
                node_count: nodes.len(),
                shard_count: shards.len(),
                ring_data: bincode_next::serde::encode_to_vec(ring, legacy())?,
                created_at: current_timestamp(),
            };
            let snapshot_bytes = bincode_next::serde::encode_to_vec(&ring_snapshot, legacy())?;
            table.insert("latest", snapshot_bytes.as_slice())?;
        }

        txn.commit()?;

        info!(
            generation = config.generation,
            nodes = nodes.len(),
            shards = shards.len(),
            "Persisted cluster snapshot"
        );

        Ok(())
    }

    // ========================================================================
    // Private Helper Methods
    // ========================================================================

    fn load_cluster_config(
        &self,
        txn: &redb::ReadTransaction,
    ) -> Result<Option<PersistedClusterConfig>> {
        match txn.open_table(TABLE_CLUSTER_CONFIG) {
            Ok(table) => {
                if let Some(value) = table.get("current")? {
                    let (config, _) =
                        bincode_next::serde::decode_from_slice(value.value(), legacy())?;
                    Ok(Some(config))
                } else {
                    Ok(None)
                }
            }
            Err(_) => Ok(None), // Table doesn't exist yet
        }
    }

    fn load_shard_assignments(
        &self,
        txn: &redb::ReadTransaction,
    ) -> Result<HashMap<Uuid, PersistedShardAssignment>> {
        let mut shards = HashMap::new();

        match txn.open_table(TABLE_SHARD_ASSIGNMENTS) {
            Ok(table) => {
                for result in table.iter()? {
                    let (_key, value) = result?;
                    let (shard, _): (PersistedShardAssignment, usize) =
                        bincode_next::serde::decode_from_slice(value.value(), legacy())?;
                    shards.insert(shard.shard_id, shard);
                }
            }
            Err(_) => {
                // Table doesn't exist yet, return empty
            }
        }

        Ok(shards)
    }

    fn load_node_registry(
        &self,
        txn: &redb::ReadTransaction,
    ) -> Result<HashMap<Uuid, PersistedNodeInfo>> {
        let mut nodes = HashMap::new();

        match txn.open_table(TABLE_NODE_REGISTRY) {
            Ok(table) => {
                for result in table.iter()? {
                    let (_key, value) = result?;
                    let (node, _): (PersistedNodeInfo, usize) =
                        bincode_next::serde::decode_from_slice(value.value(), legacy())?;
                    nodes.insert(node.node_id, node);
                }
            }
            Err(_) => {
                // Table doesn't exist yet, return empty
            }
        }

        Ok(nodes)
    }

    fn load_ring_snapshot(&self, txn: &redb::ReadTransaction) -> Result<Option<RingSnapshot>> {
        match txn.open_table(TABLE_RING_SNAPSHOT) {
            Ok(table) => {
                if let Some(value) = table.get("latest")? {
                    let (snapshot, _) =
                        bincode_next::serde::decode_from_slice(value.value(), legacy())?;
                    Ok(Some(snapshot))
                } else {
                    Ok(None)
                }
            }
            Err(_) => Ok(None),
        }
    }
}

// ============================================================================
// Utility Functions
// ============================================================================

/// Get current Unix timestamp in seconds
pub fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
