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
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tracing::{info, warn};
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
    /// Serialized ConsistentRing data (JSON)
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
    /// Open `metadata.redb` for a starting node, check it, and load what it holds.
    ///
    /// The file is checked with redb's `check_integrity` before anything is read from it: a
    /// file damaged on disk otherwise panics inside redb on the first read that crosses the
    /// damage, or earlier in redb's own debug checks. A panic anywhere in the open, the check
    /// or the load is caught here as a last guard and handled as the damage it is.
    ///
    /// On damage, what happens depends on whether the node is part of a cluster:
    ///
    /// - **single node**: the file is bookkeeping, not documents — the node rebuilds it from its
    ///   shards at this start. It is renamed `metadata.redb.damaged-<UTC timestamp>`, kept for
    ///   whoever wants to look at it, and a new one is created in its place.
    /// - **cluster member**: the node refuses to start, naming the file and the remedy. Its
    ///   assignments and ring are what its peers agree on, and rebuilding them alone is not a
    ///   decision to take silently.
    ///
    /// An error that is not damage — the file cannot be read for permissions, or another
    /// process holds it open — is returned as it is, and the file is left where it is.
    pub fn open_for_startup(
        storage_path: &Path,
        clustered: bool,
    ) -> Result<(Self, Option<PersistedClusterTopology>)> {
        let db_path = storage_path.join("metadata.redb");
        let damage = match Self::open_checked(storage_path) {
            Ok(opened) => return Ok(opened),
            Err(Opening::Failed(e)) => return Err(e),
            Err(Opening::Damaged(damage)) => damage,
        };

        if clustered {
            anyhow::bail!(
                "{} is damaged and cannot be read: {damage}.\n\
                 The file holds this node's cluster bookkeeping — cluster configuration, shard \
                 assignments, the node registry and the ring snapshot — not documents; the \
                 shards' data is in their own store.redb files and is not affected by this.\n\
                 A cluster member does not rebuild it on its own. To proceed, stop and either:\n  \
                 - restore {} from a backup taken while the node was stopped, or\n  \
                 - rebuild the node: move its data directory aside and start it empty, so the \
                 cluster reassigns its shards.\n\
                 With clustering disabled, a single node sets the file aside and rebuilds it at \
                 start.",
                db_path.display(),
                db_path.display(),
            );
        }

        let aside = storage_path.join(format!(
            "metadata.redb.damaged-{}",
            chrono::Utc::now().format("%Y%m%dT%H%M%SZ")
        ));
        std::fs::rename(&db_path, &aside).with_context(|| {
            format!(
                "{} is damaged ({damage}) and could not be set aside as {}",
                db_path.display(),
                aside.display()
            )
        })?;
        warn!(
            damaged = %db_path.display(),
            set_aside = %aside.display(),
            reason = %damage,
            "metadata.redb is damaged: set aside, and rebuilt from this node's shards at this \
             start. It holds cluster bookkeeping, not documents; the set-aside copy can be \
             removed once the node is serving."
        );

        match Self::open_checked(storage_path) {
            Ok(opened) => Ok(opened),
            Err(Opening::Failed(e)) => Err(e),
            Err(Opening::Damaged(damage)) => Err(anyhow::anyhow!(
                "a new {} is damaged as soon as it is created: {damage}",
                db_path.display()
            )),
        }
    }

    /// Open, check and load, catching a panic from redb as damage.
    fn open_checked(
        storage_path: &Path,
    ) -> std::result::Result<(Self, Option<PersistedClusterTopology>), Opening> {
        let db_path = storage_path.join("metadata.redb");
        std::fs::create_dir_all(storage_path)
            .with_context(|| format!("Failed to create metadata dir: {:?}", storage_path))
            .map_err(Opening::Failed)?;

        let attempt = quiet_catch_unwind(|| -> std::result::Result<_, Opening> {
            let mut db = Database::create(&db_path).map_err(|e| Opening::from_db(&db_path, e))?;

            // Before the first read: a read that crosses damaged pages is where redb panics.
            // The file is bookkeeping of a few megabytes at most; the check measured 6–7 ms on
            // files of 0.8–3 MB (release build, 2026-11-04), so it runs at every start.
            let started = Instant::now();
            match db.check_integrity() {
                Ok(true) => {}
                Ok(false) => warn!(
                    path = %db_path.display(),
                    "metadata.redb failed its integrity check and redb repaired it, rolling back \
                     to its last consistent commit"
                ),
                Err(e) => return Err(Opening::from_db(&db_path, e)),
            }
            info!(
                path = %db_path.display(),
                elapsed_ms = started.elapsed().as_secs_f64() * 1000.0,
                "Cluster state database opened and checked"
            );

            let store = Self { db: Arc::new(db) };
            let topology = store.load_persisted_cluster().map_err(Opening::from_load)?;
            Ok((store, topology))
        });

        match attempt {
            Ok(result) => result,
            Err(panic) => Err(Opening::Damaged(format!(
                "redb panicked reading it: {}",
                panic_message(&*panic)
            ))),
        }
    }

    /// Load complete persisted cluster topology
    pub fn load_persisted_cluster(&self) -> Result<Option<PersistedClusterTopology>> {
        let txn = self.db.begin_read()?;

        // Load cluster config (if exists, indicates this is an existing cluster)
        // Deserialization errors (e.g. old bincode format) are treated as no persisted state
        let config = match self.load_cluster_config(&txn) {
            Ok(Some(c)) => c,
            Ok(None) => {
                info!("No persisted cluster config found, treating as fresh cluster");
                return Ok(None);
            }
            Err(e) if e.is::<serde_json::Error>() => {
                info!(
                    error = %e,
                    "Failed to deserialize persisted cluster config, treating as fresh cluster"
                );
                return Ok(None);
            }
            Err(e) => return Err(e),
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
            let config_bytes = serde_json::to_vec(config)?;
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
                let value_bytes = serde_json::to_vec(&persisted)?;
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
                let value_bytes = serde_json::to_vec(&persisted)?;
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
                ring_data: serde_json::to_vec(ring)?,
                created_at: current_timestamp(),
            };
            let snapshot_bytes = serde_json::to_vec(&ring_snapshot)?;
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
                    let config: PersistedClusterConfig = serde_json::from_slice(value.value())?;
                    Ok(Some(config))
                } else {
                    Ok(None)
                }
            }
            // Not written yet. Any other error is the file's, and is not an absent table.
            Err(redb::TableError::TableDoesNotExist(_)) => Ok(None),
            Err(e) => Err(e.into()),
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
                    let shard: PersistedShardAssignment = serde_json::from_slice(value.value())?;
                    shards.insert(shard.shard_id, shard);
                }
            }
            Err(redb::TableError::TableDoesNotExist(_)) => {}
            Err(e) => return Err(e.into()),
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
                    let node: PersistedNodeInfo = serde_json::from_slice(value.value())?;
                    nodes.insert(node.node_id, node);
                }
            }
            Err(redb::TableError::TableDoesNotExist(_)) => {}
            Err(e) => return Err(e.into()),
        }

        Ok(nodes)
    }

    fn load_ring_snapshot(&self, txn: &redb::ReadTransaction) -> Result<Option<RingSnapshot>> {
        match txn.open_table(TABLE_RING_SNAPSHOT) {
            Ok(table) => {
                if let Some(value) = table.get("latest")? {
                    let snapshot: RingSnapshot = serde_json::from_slice(value.value())?;
                    Ok(Some(snapshot))
                } else {
                    Ok(None)
                }
            }
            Err(redb::TableError::TableDoesNotExist(_)) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
}

// ============================================================================
// Opening at startup
// ============================================================================

/// Why `metadata.redb` could not be opened at startup.
enum Opening {
    /// The file's content is damaged: what [`ClusterStateStore::open_for_startup`] sets aside
    /// or refuses on.
    Damaged(String),
    /// Anything else — an I/O error, the file held by another process, a format that needs an
    /// upgrade. Not a reason to touch the file.
    Failed(anyhow::Error),
}

impl Opening {
    fn from_db(path: &Path, e: redb::DatabaseError) -> Self {
        match e {
            redb::DatabaseError::Storage(redb::StorageError::Corrupted(detail)) => {
                Opening::Damaged(detail)
            }
            redb::DatabaseError::Storage(redb::StorageError::Io(io)) => Opening::Failed(
                anyhow::Error::new(io).context(format!("Failed to open {}", path.display())),
            ),
            other => Opening::Failed(
                anyhow::Error::new(other).context(format!("Failed to open {}", path.display())),
            ),
        }
    }

    /// An error while loading a file that passed its integrity check: an I/O error is the
    /// disk's, anything else — a corrupted page redb reports, a record that does not
    /// deserialize — is the file's.
    fn from_load(e: anyhow::Error) -> Self {
        let io = e.chain().any(|cause| {
            cause.is::<std::io::Error>()
                || matches!(
                    cause.downcast_ref::<redb::StorageError>(),
                    Some(redb::StorageError::Io(_))
                )
        });
        if io {
            Opening::Failed(e)
        } else {
            Opening::Damaged(format!("{e:#}"))
        }
    }
}

thread_local! {
    /// Set while this thread runs [`quiet_catch_unwind`].
    static QUIET: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// `catch_unwind`, without the panic hook's report for a panic on this thread.
///
/// A panic caught here is reported by the caller as the damage it is; the default hook would
/// first print `thread 'main' panicked at …redb…` on the terminal, which reads as the node
/// crashing. The hook is process-wide and other threads are running by now, so it is wrapped
/// rather than replaced: a panic on any other thread is reported exactly as before.
fn quiet_catch_unwind<R>(f: impl FnOnce() -> R + std::panic::UnwindSafe) -> std::thread::Result<R> {
    static WRAPPED: std::sync::Once = std::sync::Once::new();
    WRAPPED.call_once(|| {
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            if !QUIET.with(|quiet| quiet.get()) {
                previous(info);
            }
        }));
    });
    QUIET.with(|quiet| quiet.set(true));
    let result = std::panic::catch_unwind(f);
    QUIET.with(|quiet| quiet.set(false));
    result
}

/// The text of a caught panic's payload.
fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|s| (*s).to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "a panic without a message".to_string())
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    /// A `metadata.redb` holding a cluster config, `shards` shard assignments and one node:
    /// what a node that has run leaves behind.
    fn write_metadata(dir: &Path, shards: usize) {
        let db = Database::create(dir.join("metadata.redb")).unwrap();
        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(TABLE_CLUSTER_CONFIG).unwrap();
            let config = PersistedClusterConfig {
                expected_nodes: 1,
                generation: 7,
                last_stable_at: Some(1),
                cluster_name: "test".into(),
            };
            table
                .insert("current", serde_json::to_vec(&config).unwrap().as_slice())
                .unwrap();
        }
        {
            let mut table = txn.open_table(TABLE_SHARD_ASSIGNMENTS).unwrap();
            for _ in 0..shards {
                let shard = PersistedShardAssignment {
                    shard_id: Uuid::new_v4(),
                    node_id: Uuid::nil(),
                    vnode_tokens: (0..64).collect(),
                    storage_bytes: 0,
                    document_count: 0,
                    state: ShardAssignmentState::Active,
                    last_seen: 0,
                };
                table
                    .insert(
                        shard.shard_id.as_bytes().as_slice(),
                        serde_json::to_vec(&shard).unwrap().as_slice(),
                    )
                    .unwrap();
            }
        }
        {
            let mut table = txn.open_table(TABLE_NODE_REGISTRY).unwrap();
            let node = PersistedNodeInfo {
                node_id: Uuid::nil(),
                address: "127.0.0.1:9481".into(),
                shard_count: shards,
                first_seen: 0,
                last_seen: 0,
                state: NodeState::Active,
            };
            table
                .insert(
                    node.node_id.as_bytes().as_slice(),
                    serde_json::to_vec(&node).unwrap().as_slice(),
                )
                .unwrap();
        }
        txn.commit().unwrap();
    }

    /// Reproduces the damage found on 2026-11-03: every page that holds the table names — the
    /// user tables' root — is overwritten with the page holding redb's own system tree, so
    /// the user root points at `allocator_state` and its kin. Pages are found by content, not
    /// by redb's commit-slot layout; only the page size is read from the header, since it is
    /// the OS's (16 KiB on Apple silicon, 4 KiB on most others).
    fn damage_table_root(dir: &Path) -> usize {
        let path = dir.join("metadata.redb");
        let mut bytes = std::fs::read(&path).unwrap();
        // redb's magic number (9 bytes), its god byte, two of padding, then the page size.
        let page_size = u32::from_le_bytes(bytes[12..16].try_into().unwrap()) as usize;
        assert!(
            page_size.is_power_of_two() && page_size >= 512,
            "{page_size}"
        );
        let holds = |bytes: &[u8], page: usize, needle: &[u8]| {
            bytes[page * page_size..(page + 1) * page_size]
                .windows(needle.len())
                .any(|w| w == needle)
        };
        let pages = bytes.len() / page_size;
        let system = (1..pages)
            .find(|&p| holds(&bytes, p, b"allocator_state"))
            .expect("a page holding redb's system tree");
        let donor = bytes[system * page_size..(system + 1) * page_size].to_vec();
        let mut damaged = 0;
        for page in 1..pages {
            if page != system && holds(&bytes, page, b"shard_assignments") {
                bytes[page * page_size..(page + 1) * page_size].copy_from_slice(&donor);
                damaged += 1;
            }
        }
        assert!(damaged > 0, "no page held the table names");
        std::fs::write(&path, bytes).unwrap();
        damaged
    }

    fn set_aside(dir: &Path) -> Vec<PathBuf> {
        std::fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| {
                p.file_name()
                    .unwrap()
                    .to_string_lossy()
                    .starts_with("metadata.redb.damaged-")
            })
            .collect()
    }

    #[test]
    fn an_intact_file_is_checked_and_loaded() {
        let dir = tempfile::tempdir().unwrap();
        write_metadata(dir.path(), 40);
        for clustered in [false, true] {
            let (_store, topology) =
                ClusterStateStore::open_for_startup(dir.path(), clustered).unwrap();
            let topology = topology.expect("the persisted topology");
            assert_eq!(topology.config.generation, 7);
            assert_eq!(topology.shards.len(), 40);
            assert_eq!(topology.nodes.len(), 1);
        }
        assert!(set_aside(dir.path()).is_empty());
    }

    #[test]
    fn a_new_directory_starts_fresh() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("not-yet");
        let (_store, topology) = ClusterStateStore::open_for_startup(&data, true).unwrap();
        assert!(topology.is_none());
        assert!(data.join("metadata.redb").is_file());
    }

    #[test]
    fn a_damaged_file_on_a_single_node_is_set_aside_and_rebuilt() {
        let dir = tempfile::tempdir().unwrap();
        write_metadata(dir.path(), 40);
        damage_table_root(dir.path());

        let (store, topology) = ClusterStateStore::open_for_startup(dir.path(), false).unwrap();
        assert!(
            topology.is_none(),
            "a rebuilt file starts with nothing persisted"
        );

        let aside = set_aside(dir.path());
        assert_eq!(aside.len(), 1, "{aside:?}");
        let name = aside[0].file_name().unwrap().to_string_lossy().into_owned();
        let stamp = name.strip_prefix("metadata.redb.damaged-").unwrap();
        assert!(
            chrono::NaiveDateTime::parse_from_str(stamp, "%Y%m%dT%H%M%SZ").is_ok(),
            "{name}"
        );

        // The new file takes writes and reads them back.
        let txn = store.db.begin_write().unwrap();
        txn.open_table(TABLE_CLUSTER_CONFIG)
            .unwrap()
            .insert(
                "current",
                br#"{"expected_nodes":1,"generation":1,"last_stable_at":null,"cluster_name":"c"}"#
                    .as_slice(),
            )
            .unwrap();
        txn.commit().unwrap();
        drop(store);
        let (_, topology) = ClusterStateStore::open_for_startup(dir.path(), false).unwrap();
        assert_eq!(topology.unwrap().config.cluster_name, "c");
        assert_eq!(set_aside(dir.path()).len(), 1);
    }

    #[test]
    fn a_damaged_file_on_a_cluster_member_is_refused_and_left_in_place() {
        let dir = tempfile::tempdir().unwrap();
        write_metadata(dir.path(), 40);
        damage_table_root(dir.path());
        let path = dir.path().join("metadata.redb");

        let refusal = match ClusterStateStore::open_for_startup(dir.path(), true) {
            Ok(_) => panic!("a cluster member started on a damaged metadata.redb"),
            Err(e) => format!("{e:#}"),
        };
        assert!(refusal.contains(&path.display().to_string()), "{refusal}");
        assert!(refusal.contains("damaged"), "{refusal}");
        assert!(refusal.contains("not documents"), "{refusal}");
        assert!(refusal.contains("restore"), "{refusal}");
        assert!(refusal.contains("rebuild the node"), "{refusal}");
        assert!(path.is_file());
        assert!(set_aside(dir.path()).is_empty());
    }

    #[test]
    fn a_file_of_garbage_is_damage() {
        let dir = tempfile::tempdir().unwrap();
        write_metadata(dir.path(), 4);
        let path = dir.path().join("metadata.redb");
        let mut bytes = std::fs::read(&path).unwrap();
        // The header is kept, so redb recognises the file; everything after it is noise.
        let mut x: u32 = 0x9E37_79B9;
        for b in &mut bytes[4096..] {
            x ^= x << 13;
            x ^= x >> 17;
            x ^= x << 5;
            *b = x as u8;
        }
        std::fs::write(&path, bytes).unwrap();

        assert!(ClusterStateStore::open_for_startup(dir.path(), true).is_err());
        assert!(set_aside(dir.path()).is_empty());
        let (_, topology) = ClusterStateStore::open_for_startup(dir.path(), false).unwrap();
        assert!(topology.is_none());
        assert_eq!(set_aside(dir.path()).len(), 1);
    }

    #[test]
    fn an_error_that_is_not_damage_leaves_the_file_alone() {
        // A directory where the file should be: an I/O error, not damage to set aside.
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("metadata.redb")).unwrap();
        assert!(ClusterStateStore::open_for_startup(dir.path(), false).is_err());
        assert!(dir.path().join("metadata.redb").is_dir());
        assert!(set_aside(dir.path()).is_empty());
    }
}
