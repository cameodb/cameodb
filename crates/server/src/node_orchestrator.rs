//! # NodeOrchestrator - Distributed Node Management Actor
//!
//! The NodeOrchestrator is the central actor responsible for managing microshard actors
//! within a single CameoDB node. It handles shard lifecycle, discovery, and routing.
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────┐
//! │           NodeOrchestrator              │
//! ├─────────────────────────────────────────┤
//! │ - identity: NodeIdentity                │
//! │ - shards: HashMap<Uuid, ActorRef>       │
//! │ - config: NodeConfig                    │
//! └─────────────────────────────────────────┘
//! ```

use futures::future::join_all;
use futures::stream::{FuturesUnordered, StreamExt};
use rayon::prelude::*;
use std::collections::HashMap;
use std::fs;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering as AtomicOrdering},
};
use std::time::{Duration, Instant};

use anyhow::Result;
use kameo::actor::ActorRef;
use kameo::message::{Context, Message};
use kameo::{Actor, RemoteActor, remote_message};
use serde::{Deserialize, Serialize};
use tokio::sync::{RwLock as AsyncRwLock, mpsc};
use tokio::time::timeout;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::cluster_coordinator::{
    ClusterCoordinator, GetKnownPeers, GetShardAssignments, OperationType, RegisterLocalShards,
    RequestBootstrapRedial, RouteOperation, RoutingDecision, ShardMetadata,
};
use crate::config::{MessagingConfig, SearchConfig};
use cluster::{ConsistentRing, IdentityError, NodeIdentity, generate_tokens};
use kameo::actor::RemoteActorRef;
use serde_json::{Map as JsonMap, Value as JsonValue};
use storage::{
    FieldDef, HybridStore, IndexSchema, ShardStatsTimings, StorageConfig, StoreError,
    TantivyFieldType, WalOp,
};

/// Helper function to detect if an operation is a write operation
fn is_write_operation(op: &ClientOp) -> bool {
    matches!(
        op,
        ClientOp::Write { .. } | ClientOp::BulkWrite { .. } | ClientOp::DeleteIndex { .. }
    )
}

// ============================================================================
// Streaming Search Results
// ============================================================================

/// Represents a single search result from a shard or remote node
#[derive(Debug)]
#[allow(dead_code)] // Streaming infrastructure - will be fully utilized in production
pub enum StreamingSearchResult {
    /// Result from a local microshard
    Local {
        shard_id: Uuid,
        hits: Vec<(f32, serde_json::Value)>,
        total_hits: usize,
        took_ms: u64,
    },
    /// Result from a remote node
    Remote {
        node_id: Uuid,
        result: Result<serde_json::Value, OrchestratorError>,
    },
}

// ============================================================================
// Remote Actor Naming Constants
// ============================================================================

/// Generate the remote actor name for a NodeOrchestrator.
pub fn orchestrator_remote_name(node_id: &Uuid) -> String {
    format!("orchestrator-{}", node_id)
}

/// Generate the remote actor name for a MicroshardActor.
#[allow(dead_code)] // Will be used for direct shard-to-shard remote calls
pub fn shard_remote_name(shard_id: &Uuid) -> String {
    format!("shard-{}", shard_id)
}

/// Configuration for a CameoDB node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    /// Base path for all node data storage
    pub storage_path: PathBuf,
    /// Maximum number of shards this node can host
    pub max_shards: usize,
    /// Tantivy indexer memory configuration (per shard)
    pub indexer_memory_min_mb: usize,
    pub indexer_memory_max_mb: usize,
    /// Enable WAL fsync for durability
    pub wal_sync: bool,
    /// Default batch size for smart commit calculations
    pub default_batch_size: usize,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            storage_path: PathBuf::from("./data/cameodb"),
            max_shards: 8,
            indexer_memory_min_mb: 16,
            indexer_memory_max_mb: 256,
            wal_sync: true,
            default_batch_size: 1000,
        }
    }
}

/// Errors that can occur during node orchestration operations.
#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, thiserror::Error)]
pub enum OrchestratorError {
    #[error("identity error: {0}")]
    Identity(#[from] IdentityError),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("storage error: {0}")]
    Storage(#[from] StoreError),

    #[error("shard limit exceeded: {current}/{max}")]
    ShardLimitExceeded { current: usize, max: usize },

    #[error("shard already exists: {shard_id}")]
    ShardAlreadyExists { shard_id: Uuid },
}

// Serialize/Deserialize via display string to satisfy remote message bounds without
// requiring downstream error types to implement serde traits.
impl Serialize for OrchestratorError {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for OrchestratorError {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(OrchestratorError::Io(std::io::Error::other(s)))
    }
}

/// Document payload for write operations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocPayload {
    pub id: String,
    #[serde(default)]
    pub routing_key: Option<String>,
    pub doc: JsonValue,
}

/// Write request message for MicroshardActor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteRequest {
    pub index: String,
    pub routing_key: String,
    pub doc: JsonValue,
}

/// Response containing write result from MicroshardActor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteReply {
    pub sequence: u64,
}

/// Batch write request message for MicroshardActor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchWriteRequest {
    pub index: String,
    pub docs: Vec<DocPayload>,
}

/// Response containing batch write result from MicroshardActor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchWriteReply {
    pub items_written: u64,
    pub errors: Vec<String>,
}

/// Search request message for MicroshardActor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchRequest {
    pub index: String,
    pub query: String,
    pub limit: Option<usize>,
}

/// Message to get the current shard count.
#[derive(Debug, Clone)]
pub struct GetShardCount;

/// Message to propose creating a new shard on this node.
#[derive(Debug, Clone)]
pub struct ProposeShard {
    pub shard_id: Uuid,
}

/// Message to delete an index and all its data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShutdownShard;

/// Remote-friendly error type for cross-node microshard calls.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RemoteError {
    Io(String),
    Identity(String),
    NotFound(String),
    InvalidInput(String),
    Other(String),
}

impl From<OrchestratorError> for RemoteError {
    fn from(err: OrchestratorError) -> Self {
        match err {
            OrchestratorError::Identity(e) => RemoteError::Identity(e.to_string()),
            OrchestratorError::Io(e) => {
                if e.kind() == std::io::ErrorKind::NotFound {
                    RemoteError::NotFound(e.to_string())
                } else if e.kind() == std::io::ErrorKind::InvalidInput {
                    RemoteError::InvalidInput(e.to_string())
                } else {
                    RemoteError::Io(e.to_string())
                }
            }
            OrchestratorError::Storage(e) => RemoteError::Other(e.to_string()),
            OrchestratorError::ShardLimitExceeded { current, max } => {
                RemoteError::InvalidInput(format!("shard limit exceeded {current}/{max}"))
            }
            OrchestratorError::ShardAlreadyExists { shard_id } => {
                RemoteError::InvalidInput(format!("shard already exists: {shard_id}"))
            }
        }
    }
}

impl From<RemoteError> for OrchestratorError {
    fn from(err: RemoteError) -> Self {
        match err {
            RemoteError::Io(s)
            | RemoteError::Identity(s)
            | RemoteError::NotFound(s)
            | RemoteError::InvalidInput(s)
            | RemoteError::Other(s) => OrchestratorError::Io(std::io::Error::other(s)),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchHit {
    pub score: f32,
    pub doc: JsonValue,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchReply {
    pub hits: Vec<SearchHit>,
    pub total_hits: usize,
}

/// Client operation messages for RouterActor.
#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClientOp {
    /// Search operation across shards of an index
    Search {
        index: String,
        query: String,
        limit: Option<usize>,
    },
    /// Streaming search operation across shards of an index
    Stream {
        index: String,
        query: String,
        limit: Option<usize>,
    },
    /// Write operation to insert/update a document
    Write {
        index: String,
        id: String,
        routing_key: Option<String>,
        doc: JsonValue,
    },
    /// Bulk write operation to insert/update multiple documents
    BulkWrite {
        index: String,
        docs: Vec<DocPayload>,
    },
    /// Create or update index configuration/schema
    CreateConfig { index: String, schema: IndexSchema },
    /// Get index configuration/schema
    GetConfig { index: String },
    /// List all available indexes with statistics
    ListIndexes,
    /// Lightweight index listing without full schema parsing (optimized for _indexes endpoint)
    GetLightweightIndexes { include_data_size: bool },
    /// Get node identity information
    GetIdentity,
    /// List all indexes across the cluster (broadcast)
    ListClusterIndexes { include_data_size: bool },
    /// Delete an index and all its data
    DeleteIndex { index: String, delete_schema: bool },
}

/// Message to update the global routing topology (consistent ring).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateTopology {
    pub ring: ConsistentRing,
}

/// Message to shutdown all shards gracefully.
#[derive(Debug, Clone)]
pub struct ShutdownAllShards;

/// Response containing node identity info for actor replies.
#[derive(Debug, Clone, kameo::Reply)]
#[allow(dead_code)] // Fields will be used when RouterActor migrates to ActorRef
pub struct NodeIdentityInfo {
    pub uuid: Uuid,
    pub name: String,
}

/// Helper struct for aggregating index statistics across cluster nodes.
#[derive(Debug, Clone)]
struct IndexStats {
    name: String,
    document_count: u64,
    total_size_bytes: u64,
    shard_count: usize,
    field_names: Vec<String>,
}

/// Microshard actor that manages a single shard's storage and search operations.
#[derive(Clone, Actor, RemoteActor)]
pub struct MicroshardActor {
    shard_id: Uuid,
    store: Option<Arc<HybridStore>>,
    storage_config: StorageConfig,
    /// Default search limit for this shard
    default_search_limit: usize,
    /// Track active supervision tasks per index
    supervisors: Arc<AsyncRwLock<HashMap<String, mpsc::Sender<()>>>>,
}

impl std::fmt::Debug for MicroshardActor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MicroshardActor")
            .field("shard_id", &self.shard_id)
            .field("store_initialized", &self.store.is_some())
            .field("storage_config", &self.storage_config)
            .finish()
    }
}

impl MicroshardActor {
    pub fn new(shard_id: Uuid, storage_config: StorageConfig, default_search_limit: usize) -> Self {
        Self {
            shard_id,
            store: None,
            storage_config,
            default_search_limit,
            supervisors: Arc::new(AsyncRwLock::new(HashMap::new())),
        }
    }

    pub async fn start(&mut self) -> Result<(), OrchestratorError> {
        info!(
            shard_id = %self.shard_id,
            path = %self.storage_config.shard_path.display(),
            "MicroshardActor starting"
        );

        // Initialize HybridStore with spawn_blocking to avoid blocking async runtime
        let config = self.storage_config.clone();
        let store = tokio::task::spawn_blocking(move || HybridStore::new(config))
            .await
            .map_err(|e| OrchestratorError::Io(std::io::Error::other(e)))?
            .map_err(|e: StoreError| match e {
                StoreError::Io(io_err) => OrchestratorError::Io(io_err),
                _ => OrchestratorError::Io(std::io::Error::other(e.to_string())),
            })?;

        self.store = Some(Arc::new(store));
        info!(shard_id = %self.shard_id, "HybridStore initialized successfully");
        Ok(())
    }

    /// Handles search requests with spawn_blocking to avoid blocking the actor thread.
    #[cfg_attr(not(test), allow(dead_code))]
    pub async fn handle_search(
        &self,
        request: SearchRequest,
    ) -> Result<SearchReply, OrchestratorError> {
        let store = self.store.as_ref().ok_or_else(|| {
            OrchestratorError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "HybridStore not initialized",
            ))
        })?;

        let store = Arc::clone(store);
        let query = request.query;
        let limit = request.limit.unwrap_or(self.default_search_limit);

        // Use spawn_blocking to execute search on blocking thread pool
        let index = request.index.clone();
        let (results, total_hits) =
            tokio::task::spawn_blocking(move || store.search_documents(&index, &query, limit))
                .await
                .map_err(|e| OrchestratorError::Io(std::io::Error::other(e)))?
                .map_err(|e: StoreError| match e {
                    StoreError::Io(io_err) => OrchestratorError::Io(io_err),
                    _ => OrchestratorError::Io(std::io::Error::other(e.to_string())),
                })?;

        let search_hits: Vec<SearchHit> = results
            .into_iter()
            .map(|(score, doc)| SearchHit { score, doc })
            .collect();

        Ok(SearchReply {
            hits: search_hits,
            total_hits,
        })
    }

    /// Signal the supervisor for a specific index that a write has occurred.
    /// Spawns a new supervisor if one doesn't exist.
    async fn signal_supervisor(&self, index: String) {
        let store = match self.store.as_ref() {
            Some(s) => s.clone(),
            None => return,
        };

        // Check if data is actually pending
        // This is a fast read-only operation on AtomicU64, safe to call directly
        let ops_count = store.get_operations_count(&index);

        if ops_count == 0 {
            return;
        }

        let mut supervisors = self.supervisors.write().await;
        if let Some(tx) = supervisors.get(&index) {
            // Signal existing supervisor to reset its timer
            let _ = tx.try_send(());
        } else {
            // Spawn new supervisor task
            // Larger buffer to avoid dropping reset signals during bursts
            let (tx, mut rx) = mpsc::channel(64);
            let index_clone = index.clone();
            // Read supervisor timeout from environment variable or use default
            let supervisor_timeout_secs = std::env::var("CAMEODB_SUPERVISOR_TIMEOUT_SECS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(5); // Default to 5 seconds
            let timeout_dur = Duration::from_secs(supervisor_timeout_secs); // Configurable timeout to allow batch processing to complete
            let supervisors_arc = self.supervisors.clone();

            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = rx.recv() => {
                            // Signal received, timer implicitly resets by continuing loop
                            continue;
                        }
                        _ = tokio::time::sleep(timeout_dur) => {
                            // Timer expired without a signal, trigger commit
                            let index_inner = index_clone.clone();
                            let store_inner = store.clone();
                            let commit_ok = tokio::task::spawn_blocking(move || {
                                if let Err(e) = store_inner.commit_index(&index_inner) {
                                    error!(index = %index_inner, error = %e, "Supervisor failed to commit index");
                                    false
                                } else {
                                    info!(index = %index_inner, "Supervisor successfully committed index after idle timeout");
                                    true
                                }
                            }).await.unwrap_or(false);

                            if commit_ok {
                                // Self-cleanup from the supervisors map
                                let mut supervisors = supervisors_arc.write().await;
                                supervisors.remove(&index_clone);
                                break;
                            } else {
                                // Keep supervisor alive; next signal resets timer, next timeout retries
                                continue;
                            }
                        }
                    }
                }
            });

            supervisors.insert(index, tx);
        }
    }

    /// Reset supervisor for a specific index before final commits
    /// This prevents race condition while keeping supervisor alive for future writes
    async fn reset_supervisor_before_commit(&self, index: &str) {
        let supervisors = self.supervisors.write().await;
        if let Some(tx) = supervisors.get(index) {
            // Send reset signal to restart the timer
            if tx.try_send(()).is_ok() {
                tracing::debug!(index = %index, "Reset supervisor timer before final commits");
            }
        }
    }

    /// Handles write requests with spawn_blocking to avoid blocking the actor thread.
    #[allow(dead_code)]
    pub async fn handle_write(&self, request: WriteRequest) -> Result<u64, OrchestratorError> {
        let store = self.store.as_ref().ok_or_else(|| {
            OrchestratorError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "HybridStore not initialized",
            ))
        })?;

        let store = Arc::clone(store);
        let doc = request.doc.clone();

        // Extract ID from document
        let id = doc
            .get("id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                OrchestratorError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Document must contain an 'id' field",
                ))
            })?
            .to_string();

        // Do not synthesize or transform a body; preserve the document as json_blob only.
        let json_blob = Some(doc.clone());

        let op = WalOp::Put { id, json_blob };

        // Use spawn_blocking to execute write on blocking thread pool
        let index = request.index.clone();
        let seq_id = tokio::task::spawn_blocking(move || store.apply_write(&index, op))
            .await
            .map_err(|e| OrchestratorError::Io(std::io::Error::other(e)))?
            .map_err(|e: StoreError| match e {
                StoreError::Io(io_err) => OrchestratorError::Io(io_err),
                _ => OrchestratorError::Io(std::io::Error::other(e.to_string())),
            })?;

        // Signal supervisor for this index
        self.signal_supervisor(request.index).await;

        Ok(seq_id)
    }

    /// Handles batch write requests with spawn_blocking to avoid blocking the actor thread.
    pub async fn handle_batch_write(
        &self,
        request: BatchWriteRequest,
    ) -> Result<Vec<u64>, OrchestratorError> {
        tracing::debug!(
            shard_id = %self.shard_id,
            docs_count = request.docs.len(),
            "MicroshardActor: Starting batch write"
        );

        let store = self.store.as_ref().ok_or_else(|| {
            OrchestratorError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "HybridStore not initialized",
            ))
        })?;

        let store = Arc::clone(store);
        let docs = request.docs;
        let shard_id = self.shard_id;
        let index_name = request.index; // Use the actual index name from request

        // Group operations by index
        let mut ops_by_index: std::collections::HashMap<String, Vec<WalOp>> =
            std::collections::HashMap::new();

        for doc_payload in docs {
            let wal_op = WalOp::Put {
                id: doc_payload.id,
                json_blob: Some(doc_payload.doc),
            };

            ops_by_index
                .entry(index_name.clone()) // Use the actual index name, not routing_key
                .or_default()
                .push(wal_op);
        }

        // Collect unique indices from grouped ops
        let unique_indices: std::collections::HashSet<String> =
            ops_by_index.keys().cloned().collect();

        // Signal supervisor for each index BEFORE processing batch
        // This ensures supervisor exists for batch-only scenarios (no individual writes)
        for index in &unique_indices {
            self.signal_supervisor(index.clone()).await;
        }

        tracing::debug!(
            shard_id = %shard_id,
            unique_indices = unique_indices.len(),
            "MicroshardActor: Executing batch write on blocking thread"
        );

        // Use spawn_blocking to execute batch write on blocking thread pool
        let all_seq_ids = tokio::task::spawn_blocking(move || {
            tracing::debug!(
                shard_id = %shard_id,
                "MicroshardActor: Inside blocking thread, executing storage operations"
            );

            let mut all_results = Vec::new();
            let mut total_new_docs = 0usize;
            for (index, wal_ops) in ops_by_index {
                tracing::debug!(
                    shard_id = %shard_id,
                    index = %index,
                    ops_count = wal_ops.len(),
                    "MicroshardActor: Processing index in blocking thread"
                );

                let (seq_ids, new_docs) = store.apply_batch(&index, wal_ops)?;
                all_results.extend(seq_ids);
                total_new_docs += new_docs;
            }

            tracing::debug!(
                shard_id = %shard_id,
                total_ops = all_results.len(),
                "MicroshardActor: Storage operations completed, returning from blocking thread"
            );

            Ok::<(Vec<u64>, usize), StoreError>((all_results, total_new_docs))
        })
        .await
        .map_err(|e| OrchestratorError::Io(std::io::Error::other(e)))?
        .map_err(|e: StoreError| match e {
            StoreError::Io(io_err) => OrchestratorError::Io(io_err),
            _ => OrchestratorError::Io(std::io::Error::other(e.to_string())),
        })?;

        tracing::info!(
            shard_id = %shard_id,
            seq_count = all_seq_ids.0.len(),
            "MicroshardActor: Batch write fully completed, returning result"
        );

        // Extract just the sequence IDs to match expected return type
        let (seq_ids, _new_docs) = all_seq_ids;
        Ok(seq_ids)
    }

    /// Deletes all data for an index from this shard's storage
    pub async fn delete_index(
        &self,
        index: &str,
        delete_schema: bool,
    ) -> Result<(), OrchestratorError> {
        let store = self.store.as_ref().ok_or_else(|| {
            OrchestratorError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "HybridStore not initialized",
            ))
        })?;

        let store = Arc::clone(store);
        let index = index.to_string();

        // Use spawn_blocking to execute delete on blocking thread pool
        tokio::task::spawn_blocking(move || store.delete_index_data(&index, delete_schema))
            .await
            .map_err(|e| OrchestratorError::Io(std::io::Error::other(e)))?
            .map_err(|e: StoreError| match e {
                StoreError::Io(io_err) => OrchestratorError::Io(io_err),
                _ => OrchestratorError::Io(std::io::Error::other(e.to_string())),
            })?;

        Ok(())
    }
}

/// Result of parallel schema validation for a single document
#[derive(Debug, Clone)]
struct SchemaValidationResult {
    needs_evolution: bool,
    new_fields: Vec<(String, TantivyFieldType)>,
    validation_error: Option<String>,
}

/// Schema validation summary for batch processing
#[derive(Debug)]
struct SchemaValidationSummary {
    total_docs: usize,
    valid_docs: usize,
    evolution_needed: bool,
    all_new_fields: std::collections::HashSet<(String, TantivyFieldType)>,
    errors: Vec<String>,
}

/// Type alias for shard task result to reduce complexity
type ShardTaskResult = Result<(Uuid, Option<MicroshardActor>), OrchestratorError>;

/// Type alias for routing result to reduce complexity
type RoutingResult = Result<(DocPayload, Option<String>, Option<Uuid>), OrchestratorError>;

/// Validates and evolves schema for a document.
///
/// This function implements schema validation and evolution logic:
/// 1. Ensures the document has an "id" field (mandatory)
/// 2. Checks type compatibility for existing fields
/// 3. Adds new fields to the schema (append-only evolution)
/// 4. Persists schema updates to storage
///
/// Schema Creation vs Evolution:
/// - **Initial Creation** (empty schema): All fields set to `indexed = true`
/// - **Evolution** (existing schema): New fields set to `indexed = false`
///
/// # Arguments
///
/// * `index` - The index name
/// * `doc` - The document to validate
/// * `schema_cache` - Mutable reference to the cached schema
/// * `shards` - Map of local shards to persist schema updates to
///
/// # Returns
///
/// `Ok(bool)` - true if schema was updated/persisted, false otherwise
async fn validate_and_evolve_schema(
    index: &str,
    doc: &JsonValue,
    schema_cache: &mut IndexSchema,
    shards: &HashMap<Uuid, MicroshardActor>,
) -> Result<bool, OrchestratorError> {
    // Check 1 (Mandatory): Ensure doc["id"] exists
    if !doc.is_object() || !doc.as_object().unwrap().contains_key("id") {
        return Err(OrchestratorError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Document must contain an 'id' field",
        )));
    }

    let mut schema_updated = false;

    // Determine if this is initial schema creation (no fields defined yet)
    let is_initial_creation = schema_cache.fields.is_empty();

    // Check 2 (Evolution): Iterate keys in doc
    if let Some(obj) = doc.as_object() {
        for (key, value) in obj {
            let inferred_type = if key == "id" {
                TantivyFieldType::Text
            } else {
                match value {
                    JsonValue::String(s) => {
                        // Try to infer date from string
                        if chrono::DateTime::parse_from_rfc3339(s).is_ok() {
                            TantivyFieldType::Date
                        } else if s.parse::<std::net::IpAddr>().is_ok() {
                            TantivyFieldType::Ip
                        } else {
                            TantivyFieldType::Text
                        }
                    }
                    JsonValue::Number(n) => {
                        if n.is_i64() {
                            TantivyFieldType::I64
                        } else if n.is_u64() {
                            TantivyFieldType::U64
                        } else {
                            TantivyFieldType::F64
                        }
                    }
                    JsonValue::Bool(_) => TantivyFieldType::Boolean,
                    JsonValue::Array(_) => TantivyFieldType::Text, // Arrays as text
                    JsonValue::Object(_) => TantivyFieldType::Json, // Objects as JSON
                    JsonValue::Null => TantivyFieldType::Text,
                }
            };

            if let Some(existing_field) = schema_cache.fields.get(key) {
                // Check type compatibility
                // 1. Exact match is always allowed
                let mut is_compatible = existing_field.field_type == inferred_type;

                // 2. Allow Text to match String (for backward compatibility with exact fields)
                if !is_compatible
                    && inferred_type == TantivyFieldType::Text
                    && existing_field.field_type == TantivyFieldType::String
                {
                    is_compatible = true;
                }

                // 3. Allow Text to evolve to more specific types
                if !is_compatible && existing_field.field_type == TantivyFieldType::Text {
                    match inferred_type {
                        TantivyFieldType::Date
                        | TantivyFieldType::Ip
                        | TantivyFieldType::I64
                        | TantivyFieldType::U64
                        | TantivyFieldType::F64
                        | TantivyFieldType::Boolean
                        | TantivyFieldType::Json => {
                            is_compatible = true;
                        }
                        _ => {}
                    }
                }

                // 4. Allow numeric upgrades
                if !is_compatible {
                    match (&existing_field.field_type, inferred_type.clone()) {
                        (TantivyFieldType::I64, TantivyFieldType::F64)
                        | (TantivyFieldType::U64, TantivyFieldType::F64) => {
                            is_compatible = true;
                        }
                        _ => {}
                    }
                }

                if !is_compatible {
                    return Err(OrchestratorError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!(
                            "Type mismatch for field '{}': expected {:?}, got {:?}",
                            key, existing_field.field_type, inferred_type
                        ),
                    )));
                }
            } else {
                // New field: Update schema_cache (Append-Only)
                // Mark new fields indexed by default so they become searchable on arrival
                let new_field = FieldDef::new(key.clone(), inferred_type);
                schema_cache.fields.insert(key.clone(), new_field);
                schema_updated = true;
            }
        }
    }

    // Persist updated schema to storage if changed
    if schema_updated {
        let index_name = index.to_string();
        let schema_clone = schema_cache.clone();

        // Collect all stores from local shards
        let stores: Vec<Arc<HybridStore>> = shards
            .values()
            .filter_map(|shard| shard.store.as_ref().map(Arc::clone))
            .collect();

        if stores.is_empty() {
            return Err(OrchestratorError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "No local stores available to persist schema",
            )));
        }

        // Persist to all stores concurrently
        let handles: Vec<_> = stores
            .into_iter()
            .map(|store| {
                let idx = index_name.clone();
                let sch = schema_clone.clone();
                tokio::task::spawn_blocking(move || store.store_schema_and_cache(&idx, &sch))
            })
            .collect();

        // Await all results
        for handle in handles {
            handle
                .await
                .map_err(|e| {
                    OrchestratorError::Io(std::io::Error::other(format!(
                        "Failed to spawn schema update task: {}",
                        e
                    )))
                })?
                .map_err(|e| {
                    OrchestratorError::Io(std::io::Error::other(format!(
                        "Failed to store schema: {}",
                        e
                    )))
                })?;
        }

        if is_initial_creation {
            info!(
                index = %index,
                field_count = schema_cache.fields.len(),
                "Initial schema created with all fields indexed=true"
            );
        } else {
            info!(
                index = %index,
                total_fields = schema_cache.fields.len(),
                "Schema evolved with new fields (indexed=false by default)"
            );
        }
    }

    Ok(schema_updated)
}

// ============================================================================
// Remote Message Implementations for Distributed Actors
// ============================================================================

/// Message implementation for MicroshardActor search operations
#[remote_message("cameo.microshard.search")]
impl Message<SearchRequest> for MicroshardActor {
    type Reply = Result<SearchReply, RemoteError>;

    async fn handle(
        &mut self,
        msg: SearchRequest,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.handle_search(msg)
            .await
            .map(|result| SearchReply {
                hits: result.hits,
                total_hits: result.total_hits,
            })
            .map_err(RemoteError::from)
    }
}

/// Message implementation for MicroshardActor write operations
#[remote_message("cameo.microshard.write")]
impl Message<WriteRequest> for MicroshardActor {
    type Reply = Result<WriteReply, RemoteError>;

    async fn handle(
        &mut self,
        msg: WriteRequest,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.handle_write(msg)
            .await
            .map(|sequence_id| WriteReply {
                sequence: sequence_id,
            })
            .map_err(RemoteError::from)
    }
}

/// Message implementation for MicroshardActor batch write operations
#[remote_message("cameo.microshard.batch_write")]
impl Message<BatchWriteRequest> for MicroshardActor {
    type Reply = Result<BatchWriteReply, RemoteError>;

    async fn handle(
        &mut self,
        msg: BatchWriteRequest,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.handle_batch_write(msg)
            .await
            .map(|sequence_ids| BatchWriteReply {
                items_written: sequence_ids.len() as u64,
                errors: vec![],
            })
            .map_err(RemoteError::from)
    }
}

/// Message implementation for MicroshardActor shutdown operations
impl Message<ShutdownShard> for MicroshardActor {
    type Reply = Result<(), RemoteError>;

    async fn handle(
        &mut self,
        _msg: ShutdownShard,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        tracing::info!(shard_id = %self.shard_id, "MicroshardActor: Shutting down shard");

        if let Some(store) = self.store.as_ref() {
            let store_clone = store.clone();
            // Call shutdown in spawn_blocking since it's a blocking operation
            tokio::task::spawn_blocking(move || {
                if let Err(e) = store_clone.shutdown() {
                    tracing::error!(error = %e, "Failed to shutdown storage");
                }
            })
            .await
            .map_err(|e| RemoteError::Other(format!("Shutdown task failed: {}", e)))?;
        }

        tracing::info!(shard_id = %self.shard_id, "MicroshardActor: Shutdown completed");
        Ok(())
    }
}

/// Router actor that forwards client operations to NodeOrchestrator via actor messaging.
/// Uses actor messaging instead of Arc<RwLock> - no locks needed.
#[cfg_attr(not(test), allow(dead_code))]
#[derive(Clone, Actor)]
pub struct RouterActor {
    orchestrator: ActorRef<NodeOrchestrator>,
    coordinator: ActorRef<ClusterCoordinator>,
    remote_timeout: Duration,
    broadcast_timeout: Duration,
    broadcast_fanout_limit: usize,
    remote_retry_attempts: u8,
    default_search_limit: usize,
    broadcasts_total: Arc<AtomicU64>,
    broadcast_failures: Arc<AtomicU64>,
    // Streaming search configuration
    #[allow(dead_code)] // Streaming infrastructure - will be fully utilized in production
    enable_streaming_search: bool,
    #[allow(dead_code)] // Streaming infrastructure - will be fully utilized in production
    max_concurrent_shard_searches: usize,
    #[allow(dead_code)] // Streaming infrastructure - will be fully utilized in production
    max_concurrent_remote_searches: usize,
    #[allow(dead_code)] // Streaming infrastructure - will be fully utilized in production
    enable_early_termination: bool,
}

impl RouterActor {
    pub fn with_config(
        orchestrator: ActorRef<NodeOrchestrator>,
        coordinator: ActorRef<ClusterCoordinator>,
        messaging: &MessagingConfig,
        search_config: &SearchConfig,
        default_search_limit: usize,
    ) -> Self {
        Self {
            orchestrator,
            coordinator,
            remote_timeout: Duration::from_secs(messaging.request_timeout_secs),
            broadcast_timeout: Duration::from_secs(messaging.broadcast_timeout_secs),
            broadcast_fanout_limit: messaging.broadcast_fanout_limit,
            remote_retry_attempts: messaging.remote_retry_attempts,
            default_search_limit,
            broadcasts_total: Arc::new(AtomicU64::new(0)),
            broadcast_failures: Arc::new(AtomicU64::new(0)),
            // Streaming search configuration
            enable_streaming_search: search_config.enable_streaming_search,
            max_concurrent_shard_searches: search_config.max_concurrent_shard_searches,
            max_concurrent_remote_searches: search_config.max_concurrent_remote_searches,
            enable_early_termination: search_config.enable_early_termination,
        }
    }

    /// Handles client operations by forwarding to NodeOrchestrator actor.
    #[cfg_attr(not(test), allow(dead_code))]
    pub async fn handle_client_op(&self, op: ClientOp) -> Result<JsonValue, OrchestratorError> {
        match self.orchestrator.ask(op).await {
            Ok(result) => Ok(result),
            Err(e) => Err(OrchestratorError::Io(std::io::Error::other(format!(
                "Actor error: {}",
                e
            )))),
        }
    }

    /// Route via ClusterCoordinator then handle locally (remote/broadcast stubbed).
    #[cfg_attr(not(test), allow(dead_code))]
    pub async fn route_and_handle(
        &self,
        op: ClientOp,
        routing_key: Option<String>,
        operation_type: OperationType,
    ) -> Result<JsonValue, OrchestratorError> {
        // Metadata operations (schema/config) always execute locally - no need to broadcast
        if matches!(
            op,
            ClientOp::GetConfig { .. }
                | ClientOp::CreateConfig { .. }
                | ClientOp::ListIndexes
                | ClientOp::ListClusterIndexes { .. }
        ) {
            return self.handle_client_op(op).await;
        }

        let decision = self
            .coordinator
            .ask(RouteOperation {
                routing_key,
                operation_type,
            })
            .await;

        match decision {
            Ok(RoutingDecision::Local) => self.handle_client_op(op).await,
            Ok(RoutingDecision::Broadcast) => {
                // CRITICAL: Never broadcast write operations - this causes data duplication
                // and inconsistency. Writes must be routed to a specific shard.
                if is_write_operation(&op) {
                    return Err(OrchestratorError::Io(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        "Write operation cannot be broadcast - routing failed",
                    )));
                }

                // Use streaming for search operations if enabled
                if self.enable_streaming_search
                    && matches!(op, ClientOp::Search { .. } | ClientOp::Stream { .. })
                {
                    self.handle_broadcast_streaming(op).await
                } else {
                    self.handle_broadcast(op).await
                }
            }
            Ok(RoutingDecision::Remote { node_id, peer_addr }) => {
                self.handle_remote(op, node_id, peer_addr).await
            }
            Err(err) => {
                let reason = format!("routing failed: {}", err);
                let _ = self
                    .coordinator
                    .ask(RequestBootstrapRedial {
                        reason: reason.clone(),
                    })
                    .await;
                Err(OrchestratorError::Io(std::io::Error::other(reason)))
            }
        }
    }

    /// Get the number of active shards (for health check).
    pub async fn shard_count(&self) -> usize {
        // Forward to orchestrator actor
        (self
            .orchestrator
            .ask(crate::node_orchestrator::GetShardCount)
            .await)
            .unwrap_or_default()
    }

    async fn handle_broadcast(&self, op: ClientOp) -> Result<JsonValue, OrchestratorError> {
        use crate::cluster_coordinator::{GetKnownPeers, KnownPeer};
        use futures::future::join_all;

        self.broadcasts_total.fetch_add(1, AtomicOrdering::Relaxed);

        // Get known peers for remote fan-out
        let peers: Vec<KnownPeer> = self
            .coordinator
            .ask(GetKnownPeers)
            .await
            .unwrap_or_default();

        info!(
            "🔍 Broadcast operation: got {} known peers from coordinator",
            peers.len()
        );
        for peer in &peers {
            info!("  📍 Peer: {} at {}", peer.node_id, peer.address);
        }

        let peer_count = peers.len().min(self.broadcast_fanout_limit);
        info!(
            timeout_ms = self.broadcast_timeout.as_millis(),
            fanout_limit = self.broadcast_fanout_limit,
            known_peers = peers.len(),
            target_peers = peer_count,
            "RouterActor: broadcast routing with remote fan-out"
        );

        // Start local operation
        let local_op = op.clone();
        let local_future = self.handle_client_op(local_op);

        // Fan out to remote peers (up to fanout_limit)
        let remote_futures: Vec<_> = peers
            .into_iter()
            .take(self.broadcast_fanout_limit)
            .map(|peer| {
                let op_clone = op.clone();
                let node_id = peer.node_id;
                let peer_addr = peer.address;
                async move {
                    timeout(
                        self.broadcast_timeout,
                        self.try_remote(op_clone, node_id, &peer_addr),
                    )
                    .await
                }
            })
            .collect();

        // Execute local + remote concurrently
        let t_start = Instant::now();
        let (local_result, remote_results) = tokio::join!(local_future, join_all(remote_futures));

        // If this is a search, prefer fastest/local results and stop after hitting the limit.
        if let ClientOp::Search { limit, .. } = &op {
            let limit = limit.unwrap_or(self.default_search_limit);
            let mut merged_hits: Vec<JsonValue> = Vec::with_capacity(limit);
            let mut total_shards_queried = 0usize;
            let mut error_count = 0u64;
            let mut nodes_contacted = 0usize;
            let mut max_took_ms: Option<u64> = None;
            let mut total_hits_sum = 0usize;

            // Helper to push hits from a result up to the remaining limit
            fn push_hits(
                value: &JsonValue,
                merged_hits: &mut Vec<JsonValue>,
                limit: usize,
                total_shards_queried: &mut usize,
                nodes_contacted: &mut usize,
                max_took_ms: &mut Option<u64>,
                total_hits_sum: &mut usize,
            ) {
                if merged_hits.len() >= limit {
                    return;
                }
                if let Some(hits) = value.get("hits").and_then(|h| h.as_array()) {
                    for hit in hits {
                        if merged_hits.len() >= limit {
                            break;
                        }
                        merged_hits.push(hit.clone());
                    }
                }
                // Extract shard statistics from the response
                if let Some(stats) = value.get("stats").and_then(|s| s.as_object())
                    && let Some(shards) = stats.get("shards").and_then(|s| s.as_object())
                    && let Some(responded) = shards.get("responded").and_then(|r| r.as_u64())
                {
                    *total_shards_queried += responded as usize;
                    _ = shards.get("total").and_then(|t| t.as_u64()); // Could track total shards attempted
                }
                if let Some(total) = value.get("total_hits").and_then(|t| t.as_u64()) {
                    *total_hits_sum += total as usize;
                }
                *nodes_contacted += 1;
                if let Some(t) = value.get("took_ms").and_then(|v| v.as_u64()) {
                    *max_took_ms = match *max_took_ms {
                        Some(cur) => Some(cur.max(t)),
                        None => Some(t),
                    };
                }
            }

            // Process local result first
            match &local_result {
                Ok(val) => push_hits(
                    val,
                    &mut merged_hits,
                    limit,
                    &mut total_shards_queried,
                    &mut nodes_contacted,
                    &mut max_took_ms,
                    &mut total_hits_sum,
                ),
                Err(e) => {
                    error_count += 1;
                    warn!(error = %e, "Broadcast: local search failed");
                }
            }

            // Then process remote results in completion order until limit is reached
            for result in remote_results {
                if merged_hits.len() >= limit {
                    break;
                }
                match result {
                    Ok(Ok(val)) => push_hits(
                        &val,
                        &mut merged_hits,
                        limit,
                        &mut total_shards_queried,
                        &mut nodes_contacted,
                        &mut max_took_ms,
                        &mut total_hits_sum,
                    ),
                    Ok(Err(e)) => {
                        error_count += 1;
                        warn!(error = %e, "Broadcast: remote search failed");
                    }
                    Err(elapsed) => {
                        error_count += 1;
                        warn!(error = %elapsed, "Broadcast: remote search timed out");
                    }
                }
            }

            // Track failures
            if error_count > 0 {
                self.broadcast_failures
                    .fetch_add(error_count, AtomicOrdering::Relaxed);
            }

            // Keep local/fast-first ordering, but stabilize scores within the collected set
            merged_hits.sort_by(|a, b| {
                let score_a = a.get("_score").and_then(|s| s.as_f64()).unwrap_or(0.0);
                let score_b = b.get("_score").and_then(|s| s.as_f64()).unwrap_or(0.0);
                score_b
                    .partial_cmp(&score_a)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            merged_hits.truncate(limit);

            return Ok(serde_json::json!({
                "hits": merged_hits,
                "hits_returned": merged_hits.len(),
                "total_hits": total_hits_sum,
                "limit": limit,
                "took_ms": max_took_ms.unwrap_or_else(|| t_start.elapsed().as_millis() as u64),
                "stats": {
                    "shards": {
                        "total": total_shards_queried,
                        "responded": total_shards_queried.saturating_sub(error_count as usize),
                        "failed": error_count as usize
                    },
                    "nodes": {
                        "contacted": nodes_contacted
                    }
                }
            }));
        }

        // Aggregate results: for search, merge hits; for writes, report success/failure counts
        let mut all_results: Vec<JsonValue> = Vec::new();
        let mut error_count = 0u64;

        // Process local result
        match local_result {
            Ok(val) => all_results.push(val),
            Err(e) => {
                error_count += 1;
                warn!(error = %e, "Broadcast: local operation failed");
            }
        }

        // Process remote results
        for result in remote_results {
            match result {
                Ok(Ok(val)) => all_results.push(val),
                Ok(Err(e)) => {
                    error_count += 1;
                    warn!(error = %e, "Broadcast: remote operation failed");
                }
                Err(elapsed) => {
                    error_count += 1;
                    warn!(error = %elapsed, "Broadcast: remote operation timed out");
                }
            }
        }

        if error_count > 0 {
            self.broadcast_failures
                .fetch_add(error_count, AtomicOrdering::Relaxed);
        }

        // Merge results based on operation type
        match &op {
            ClientOp::Search { limit, .. } => {
                // Enforce a global limit across merged results to avoid returning
                // (limit * nodes) hits when broadcasting.
                let limit = limit.unwrap_or(self.default_search_limit);

                // For search operations, if we only have local results (no remote peers),
                // return the local response directly to preserve shard-level details
                if all_results.len() == 1 && peer_count == 0 {
                    return Ok(all_results[0].clone());
                }

                // Merge search results from multiple nodes: combine all hits arrays
                let mut merged_hits: Vec<JsonValue> = Vec::new();
                let mut total_shards_queried = 0usize;

                for result in &all_results {
                    if let Some(hits) = result.get("hits").and_then(|h| h.as_array()) {
                        merged_hits.extend(hits.iter().cloned());
                    }
                    // Sum up shards_responded from each node
                    if let Some(shards) = result.get("shards_responded").and_then(|s| s.as_u64()) {
                        total_shards_queried += shards as usize;
                    }
                }

                // Sort by score descending and deduplicate by _id if present
                merged_hits.sort_by(|a, b| {
                    let score_a = a.get("_score").and_then(|s| s.as_f64()).unwrap_or(0.0);
                    let score_b = b.get("_score").and_then(|s| s.as_f64()).unwrap_or(0.0);
                    score_b
                        .partial_cmp(&score_a)
                        .unwrap_or(std::cmp::Ordering::Equal)
                });
                merged_hits.truncate(limit);

                Ok(serde_json::json!({
                    "hits": merged_hits,
                    "hits_returned": merged_hits.len(),
                    "total_hits": merged_hits.iter()
                        .filter_map(|h| h.get("total_hits"))
                        .filter_map(|t| t.as_u64())
                        .sum::<u64>() as usize,
                    "limit": limit,
                    "stats": {
                        "shards": {
                            "total": total_shards_queried,
                            "responded": total_shards_queried.saturating_sub(error_count as usize),
                            "failed": error_count as usize
                        },
                        "nodes": {
                            "contacted": all_results.len()
                        }
                    }
                }))
            }
            ClientOp::Write { .. } | ClientOp::BulkWrite { .. } => {
                // For writes, return aggregate success info
                let total_nodes = all_results.len();

                // Aggregate items_written and errors from all node responses
                let mut items_written = 0u64;
                let mut errors = Vec::new();

                for result in &all_results {
                    if let Some(n) = result.get("items_written").and_then(|v| v.as_u64()) {
                        items_written += n;
                    }
                    if let Some(errs) = result.get("errors").and_then(|v| v.as_array()) {
                        errors.extend(errs.clone());
                    }
                }

                Ok(serde_json::json!({
                    "success": error_count == 0 && errors.is_empty(),
                    "nodes_contacted": total_nodes + error_count as usize,
                    "nodes_succeeded": total_nodes,
                    "nodes_failed": error_count,
                    "items_written": items_written,
                    "errors": errors
                }))
            }
            ClientOp::ListClusterIndexes { .. } => {
                // Merge index statistics from all nodes
                let mut index_map: HashMap<String, IndexStats> = HashMap::new();
                let mut node_details: Vec<JsonValue> = Vec::new();

                for result in &all_results {
                    // Extract node_id and node_name from each response
                    let node_id = result
                        .get("node_id")
                        .and_then(|v| v.as_str())
                        .unwrap_or("unknown")
                        .to_string();

                    let node_name = result
                        .get("node_name")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string());

                    // Collect per-node details with node_name immediately after node_id
                    let mut node_detail_map = serde_json::Map::new();
                    node_detail_map.insert("node_id".to_string(), serde_json::json!(node_id));
                    if let Some(name) = node_name {
                        node_detail_map.insert("node_name".to_string(), serde_json::json!(name));
                    }
                    node_detail_map.insert(
                        "indexes".to_string(),
                        result
                            .get("indexes")
                            .cloned()
                            .unwrap_or(serde_json::json!([])),
                    );
                    node_detail_map.insert(
                        "total_indexes".to_string(),
                        serde_json::json!(
                            result
                                .get("total_indexes")
                                .and_then(|v| v.as_u64())
                                .unwrap_or(0)
                        ),
                    );
                    node_detail_map.insert(
                        "total_shards".to_string(),
                        serde_json::json!(
                            result
                                .get("total_shards")
                                .and_then(|v| v.as_u64())
                                .unwrap_or(0)
                        ),
                    );

                    node_details.push(serde_json::Value::Object(node_detail_map));

                    // Aggregate index stats across nodes
                    if let Some(indexes) = result.get("indexes").and_then(|v| v.as_array()) {
                        for idx in indexes {
                            let name = idx
                                .get("name")
                                .and_then(|v| v.as_str())
                                .unwrap_or("")
                                .to_string();
                            if name.is_empty() {
                                continue;
                            }

                            let entry = index_map.entry(name.clone()).or_insert(IndexStats {
                                name: name.clone(),
                                document_count: 0,
                                total_size_bytes: 0,
                                shard_count: 0,
                                field_names: Vec::new(),
                            });

                            entry.document_count += idx
                                .get("document_count")
                                .and_then(|v| v.as_u64())
                                .unwrap_or(0);
                            entry.total_size_bytes += idx
                                .get("total_size_bytes")
                                .and_then(|v| v.as_u64())
                                .unwrap_or(0);
                            entry.shard_count +=
                                idx.get("shard_count").and_then(|v| v.as_u64()).unwrap_or(0)
                                    as usize;

                            // Merge field names (union)
                            if let Some(fields) = idx.get("field_names").and_then(|v| v.as_array())
                            {
                                for field in fields {
                                    if let Some(field_str) = field.as_str()
                                        && !entry.field_names.contains(&field_str.to_string())
                                    {
                                        entry.field_names.push(field_str.to_string());
                                    }
                                }
                            }
                        }
                    }
                }

                // Sort field names for each index
                for stats in index_map.values_mut() {
                    stats
                        .field_names
                        .sort_by(|a, b| match (a.as_str(), b.as_str()) {
                            ("id", "id") => std::cmp::Ordering::Equal,
                            ("id", _) => std::cmp::Ordering::Less,
                            (_, "id") => std::cmp::Ordering::Greater,
                            _ => a.cmp(b),
                        });
                }

                // Convert to JSON array
                let mut cluster_indexes: Vec<(String, JsonValue)> = index_map
                    .into_values()
                    .map(|stats| {
                        let name = stats.name.clone();
                        let json = serde_json::json!({
                            "name": stats.name,
                            "document_count": stats.document_count,
                            "total_size_bytes": stats.total_size_bytes,
                            "size_mb": stats.total_size_bytes / (1024 * 1024),
                            "shard_count": stats.shard_count,
                            "field_names": stats.field_names,
                        });
                        (name, json)
                    })
                    .collect();
                cluster_indexes.sort_by(|a, b| a.0.cmp(&b.0));
                let cluster_indexes: Vec<JsonValue> =
                    cluster_indexes.into_iter().map(|(_, json)| json).collect();

                Ok(serde_json::json!({
                    "indexes": cluster_indexes,
                    "total_indexes": cluster_indexes.len(),
                    "nodes_contacted": all_results.len(),
                    "nodes_failed": error_count,
                    "nodes": node_details,
                }))
            }
            _ => {
                // For other operations, return first successful result or error
                if let Some(first) = all_results.first() {
                    Ok(first.clone())
                } else {
                    self.broadcast_failures
                        .fetch_add(1, AtomicOrdering::Relaxed);
                    Err(OrchestratorError::Io(std::io::Error::other(
                        "broadcast failed: no successful responses",
                    )))
                }
            }
        }
    }

    /// Streaming version of handle_broadcast for improved search performance
    async fn handle_broadcast_streaming(
        &self,
        op: ClientOp,
    ) -> Result<JsonValue, OrchestratorError> {
        tracing::info!("🚀 Using STREAMING search for improved performance");

        use crate::cluster_coordinator::{GetKnownPeers, KnownPeer};

        self.broadcasts_total.fetch_add(1, AtomicOrdering::Relaxed);

        // Get known peers for remote fan-out
        let peers: Vec<KnownPeer> = self
            .coordinator
            .ask(GetKnownPeers)
            .await
            .unwrap_or_default();

        let start_time = std::time::Instant::now();

        // Handle search operations with streaming
        match op {
            ClientOp::Search {
                index,
                query,
                limit,
            }
            | ClientOp::Stream {
                index,
                query,
                limit,
            } => {
                let limit = limit.unwrap_or(self.default_search_limit);

                // Create local search stream using improved concurrent approach
                let local_future = async {
                    match self
                        .orchestrator
                        .ask(ClientOp::Search {
                            index: index.clone(),
                            query: query.clone(),
                            limit: Some(limit),
                        })
                        .await
                    {
                        Ok(result) => StreamingSearchResult::Local {
                            shard_id: Uuid::nil(), // Individual shard IDs are in the documents
                            hits: result
                                .get("hits")
                                .and_then(|h| h.as_array())
                                .map(|arr| arr.to_vec())
                                .unwrap_or_default()
                                .iter()
                                .filter_map(|hit| {
                                    hit.get("_score")
                                        .and_then(|s| s.as_f64())
                                        .map(|score| (score as f32, hit.clone()))
                                })
                                .collect(),
                            total_hits: result
                                .get("total_hits")
                                .and_then(|t| t.as_u64())
                                .unwrap_or(0) as usize,
                            took_ms: 0,
                        },
                        Err(_) => StreamingSearchResult::Local {
                            shard_id: Uuid::nil(),
                            hits: Vec::new(),
                            total_hits: 0,
                            took_ms: 0,
                        },
                    }
                };

                // Create remote search streams
                let remote_futures: Vec<_> = peers
                    .into_iter()
                    .take(self.broadcast_fanout_limit)
                    .map(|peer| {
                        let op_clone = ClientOp::Search {
                            index: index.clone(),
                            query: query.clone(),
                            limit: Some(limit),
                        };
                        let node_id = peer.node_id;
                        let peer_addr = peer.address;
                        async move {
                            let result = timeout(
                                self.broadcast_timeout,
                                self.try_remote(op_clone, node_id, &peer_addr),
                            )
                            .await
                            .unwrap_or(Err(OrchestratorError::Io(std::io::Error::new(
                                std::io::ErrorKind::TimedOut,
                                "Remote operation timed out",
                            ))));
                            StreamingSearchResult::Remote { node_id, result }
                        }
                    })
                    .collect();

                // Combine local and remote into a single stream using boxed futures
                let mut search_futures = FuturesUnordered::new();
                search_futures.push(Box::pin(local_future)
                    as Pin<Box<dyn Future<Output = StreamingSearchResult> + Send>>);
                for future in remote_futures {
                    search_futures.push(Box::pin(future)
                        as Pin<Box<dyn Future<Output = StreamingSearchResult> + Send>>);
                }

                // Process results as they arrive with early termination
                let mut all_hits = Vec::new();
                let mut total_hits_sum = 0usize;
                let mut shards_queried = 0usize;
                let mut nodes_contacted = 0usize;
                let mut unique_shard_ids = std::collections::HashSet::new();
                let mut errors = Vec::new();

                while let Some(search_result) = search_futures.next().await {
                    // Early termination if limit reached and enabled
                    if self.enable_early_termination && all_hits.len() >= limit {
                        break;
                    }

                    match search_result {
                        StreamingSearchResult::Local {
                            shard_id: _,
                            hits,
                            total_hits,
                            took_ms: _,
                        } => {
                            // Process streaming local search results
                            for (score, doc) in hits {
                                if all_hits.len() < limit {
                                    let mut hit_doc = doc;
                                    if let JsonValue::Object(ref mut o) = hit_doc {
                                        o.insert(
                                            "_score".to_string(),
                                            JsonValue::Number(
                                                serde_json::Number::from_f64(score as f64)
                                                    .unwrap_or(serde_json::Number::from(0)),
                                            ),
                                        );
                                        // Track unique shard IDs from individual documents
                                        if let Some(shard_id) =
                                            hit_doc.get("shard_id").and_then(|s| s.as_str())
                                            && let Ok(uuid) = Uuid::parse_str(shard_id)
                                        {
                                            unique_shard_ids.insert(uuid);
                                        }
                                    }
                                    all_hits.push(hit_doc);
                                }
                            }
                            total_hits_sum += total_hits;
                            shards_queried = unique_shard_ids.len();
                            nodes_contacted += 1;
                        }
                        StreamingSearchResult::Remote { node_id, result } => {
                            nodes_contacted += 1;
                            match result {
                                Ok(val) => {
                                    if let Some(hits) = val.get("hits").and_then(|h| h.as_array()) {
                                        for hit in hits {
                                            if all_hits.len() < limit {
                                                all_hits.push(hit.clone());
                                            }
                                        }
                                    }
                                    if let Some(total) =
                                        val.get("total_hits").and_then(|t| t.as_u64())
                                    {
                                        total_hits_sum += total as usize;
                                    }
                                    // Extract shard statistics from the response
                                    if let Some(stats) =
                                        val.get("stats").and_then(|s| s.as_object())
                                        && let Some(shards) =
                                            stats.get("shards").and_then(|s| s.as_object())
                                        && let Some(responded) =
                                            shards.get("responded").and_then(|r| r.as_u64())
                                    {
                                        shards_queried += responded as usize;
                                    }
                                }
                                Err(e) => {
                                    errors.push(format!(
                                        "Remote node {} search failed: {}",
                                        node_id, e
                                    ));
                                }
                            }
                        }
                    }
                }

                // Sort by score descending and apply limit
                all_hits.sort_by(|a, b| {
                    let score_a = a.get("_score").and_then(|s| s.as_f64()).unwrap_or(0.0);
                    let score_b = b.get("_score").and_then(|s| s.as_f64()).unwrap_or(0.0);
                    score_b
                        .partial_cmp(&score_a)
                        .unwrap_or(std::cmp::Ordering::Equal)
                });
                all_hits.truncate(limit);

                Ok(serde_json::json!({
                    "hits": all_hits,
                    "hits_returned": all_hits.len(),
                    "total_hits": total_hits_sum,
                    "limit": limit,
                    "took_ms": start_time.elapsed().as_millis(),
                    "stats": {
                        "shards": {
                            "total": shards_queried,
                            "responded": shards_queried.saturating_sub(errors.len()),
                            "failed": errors.len()
                        },
                        "nodes": {
                            "contacted": nodes_contacted
                        }
                    },
                    "errors": errors
                }))
            }
            _ => {
                // For non-search operations, fall back to broadcast request handling
                self.handle_broadcast_request(op).await
            }
        }
    }

    /// Broadcast request method for non-search operations
    async fn handle_broadcast_request(&self, op: ClientOp) -> Result<JsonValue, OrchestratorError> {
        // Implementation for non-search operations (write, bulk_write, etc.)
        // This is the existing handle_broadcast logic
        self.handle_broadcast(op).await
    }

    async fn handle_remote(
        &self,
        op: ClientOp,
        node_id: Uuid,
        peer_addr: String,
    ) -> Result<JsonValue, OrchestratorError> {
        let max_attempts = std::cmp::max(1, self.remote_retry_attempts as usize);
        let mut last_err = None;

        for attempt in 1..=max_attempts {
            let op_clone = op.clone();
            match timeout(
                self.remote_timeout,
                self.try_remote(op_clone, node_id, &peer_addr),
            )
            .await
            {
                Ok(Ok(value)) => return Ok(value),
                Ok(Err(err)) => {
                    warn!(
                        %node_id,
                        %peer_addr,
                        attempt,
                        max_attempts,
                        error = %err,
                        "RouterActor: remote attempt failed"
                    );
                    last_err = Some(err);
                }
                Err(elapsed) => {
                    warn!(
                        %node_id,
                        %peer_addr,
                        attempt,
                        max_attempts,
                        timeout_ms = self.remote_timeout.as_millis(),
                        error = %elapsed,
                        "RouterActor: remote attempt timed out"
                    );
                    last_err = Some(OrchestratorError::Io(std::io::Error::other(
                        elapsed.to_string(),
                    )));
                }
            }
        }

        let reason = last_err
            .map(|e| {
                format!(
                    "remote routing failed after {} attempts: {}",
                    max_attempts, e
                )
            })
            .unwrap_or_else(|| "remote routing failed".to_string());

        let _ = self
            .coordinator
            .ask(RequestBootstrapRedial {
                reason: reason.clone(),
            })
            .await;

        Err(OrchestratorError::Io(std::io::Error::other(reason)))
    }
}

impl RouterActor {
    /// Attempt a remote call to a microshard on another node.
    /// Looks up the remote NodeOrchestrator by name and forwards the ClientOp.
    async fn try_remote(
        &self,
        _op: ClientOp,
        node_id: Uuid,
        peer_addr: &str,
    ) -> Result<JsonValue, OrchestratorError> {
        let orchestrator_name = orchestrator_remote_name(&node_id);
        info!(
            "🔎 Attempting remote actor lookup: name='{}', node_id={}, addr={}",
            orchestrator_name, node_id, peer_addr
        );

        let remote_ref: Option<RemoteActorRef<NodeOrchestrator>> =
            RemoteActorRef::lookup(orchestrator_name.clone())
                .await
                .map_err(|e| {
                    warn!("❌ Remote actor lookup error: {}", e);
                    OrchestratorError::Io(std::io::Error::other(e.to_string()))
                })?;

        let res = match remote_ref {
            Some(remote) => {
                info!("✅ Remote actor found: {}", orchestrator_name);
                remote
                    .ask(&_op)
                    .await
                    .map_err(|e| OrchestratorError::Io(std::io::Error::other(e.to_string())))?
            }
            None => {
                warn!(
                    "❌ Remote orchestrator not found: name='{}', node_id={}",
                    orchestrator_name, node_id
                );
                Err(OrchestratorError::Io(std::io::Error::other(format!(
                    "remote orchestrator {} not found",
                    orchestrator_name
                ))))?
            }
        };
        Ok(res)
    }
}

#[derive(Debug, Actor, RemoteActor)]
pub struct NodeOrchestrator {
    /// Map of shard UUIDs to their microshard actors
    pub(crate) shards: HashMap<Uuid, MicroshardActor>,
    /// This node's identity (UUID, name, virtual tokens)
    identity: NodeIdentity,
    /// Node configuration  
    config: NodeConfig,
    /// Consistent hash ring for routing writes based on routing keys
    routing_ring: ConsistentRing,
    /// Optional coordinator reference for shard registration
    coordinator: Option<ActorRef<ClusterCoordinator>>,
    /// Per-index schema cache to avoid repeated metadata reads
    schema_cache: AsyncRwLock<HashMap<String, IndexSchema>>,
    /// Default search result limit when not specified in request
    default_search_limit: usize,
}

impl NodeOrchestrator {
    /// Validates schema for documents in parallel, then evolves schema sequentially.
    ///
    /// This method uses a two-stage approach:
    /// Stage 1: Parallel validation (read-only, CPU-bound)
    /// Stage 2: Sequential schema evolution (write operations only when needed)
    async fn staged_schema_validation(
        &self,
        index: &str,
        docs: &[DocPayload],
        schema_cache: &mut IndexSchema,
    ) -> Result<SchemaValidationSummary, OrchestratorError> {
        if docs.is_empty() {
            return Ok(SchemaValidationSummary {
                total_docs: 0,
                valid_docs: 0,
                evolution_needed: false,
                all_new_fields: std::collections::HashSet::new(),
                errors: Vec::new(),
            });
        }

        // Stage 1: Parallel validation (read-only)
        let validation_results = self
            .parallel_validate_schema(index, docs, schema_cache)
            .await?;

        // Stage 2: Aggregate results and identify evolution needs
        let mut summary = SchemaValidationSummary {
            total_docs: docs.len(),
            valid_docs: 0,
            evolution_needed: false,
            all_new_fields: std::collections::HashSet::new(),
            errors: Vec::new(),
        };

        for result in validation_results {
            if result.validation_error.is_none() {
                summary.valid_docs += 1;
                if result.needs_evolution {
                    summary.evolution_needed = true;
                    for new_field in result.new_fields {
                        summary.all_new_fields.insert(new_field);
                    }
                }
            } else {
                summary.errors.push(result.validation_error.unwrap());
            }
        }

        // Stage 3: Sequential schema evolution (only if needed)
        if summary.evolution_needed && !summary.all_new_fields.is_empty() {
            self.evolve_schema_sequential(
                index,
                schema_cache,
                &summary.all_new_fields,
                &self.shards,
            )
            .await?;
        }

        tracing::debug!(
            total_docs = summary.total_docs,
            valid_docs = summary.valid_docs,
            evolution_needed = summary.evolution_needed,
            new_fields_count = summary.all_new_fields.len(),
            errors_count = summary.errors.len(),
            "Staged schema validation completed"
        );

        Ok(summary)
    }

    /// Parallel schema validation (read-only, no mutations)
    async fn parallel_validate_schema(
        &self,
        _index: &str,
        docs: &[DocPayload],
        schema_cache: &IndexSchema,
    ) -> Result<Vec<SchemaValidationResult>, OrchestratorError> {
        tracing::debug!(
            "Using parallel Rayon validation for {} documents",
            docs.len()
        );

        let is_initial_creation = schema_cache.fields.is_empty();

        // Fast path: if schema is mature and batch is small, skip expensive clone
        let use_fast_path = !is_initial_creation && docs.len() < 1000;

        if use_fast_path {
            // Use read-only reference to avoid cloning
            tracing::debug!("Using fast path validation for {} documents", docs.len());
            let results: Vec<SchemaValidationResult> = docs
                .par_iter()
                .enumerate()
                .map(|(_doc_index, doc_payload)| {
                    self.validate_single_document_readonly_fast(
                        &doc_payload.doc,
                        schema_cache, // Pass by reference, no clone
                        is_initial_creation,
                    )
                })
                .collect();
            return Ok(results);
        }

        // For initial schema creation, we'll do field discovery in the evolution step
        // This keeps validation read-only and evolution write-only

        // Parallel validation using rayon - schema is now pre-populated
        let results: Vec<SchemaValidationResult> = docs
            .par_iter()
            .enumerate()
            .map(|(_doc_index, doc_payload)| {
                self.validate_single_document_readonly(
                    &doc_payload.doc,
                    schema_cache, // Use reference, schema is now stable
                    is_initial_creation,
                )
            })
            .collect();

        Ok(results)
    }

    /// Read-only validation for a single document (no mutations) - fast path
    fn validate_single_document_readonly_fast(
        &self,
        doc: &JsonValue,
        schema_cache: &IndexSchema, // Pass by reference, no clone needed
        _is_initial_creation: bool,
    ) -> SchemaValidationResult {
        // Check 1: Ensure doc["id"] exists
        if !doc.is_object() || !doc.as_object().unwrap().contains_key("id") {
            return SchemaValidationResult {
                needs_evolution: false,
                new_fields: Vec::new(),
                validation_error: Some("Document missing required 'id' field".to_string()),
            };
        }

        // Check 2: Validate against existing schema (no evolution in fast path)
        if let Some(obj) = doc.as_object() {
            for (key, value) in obj {
                if key == "id" {
                    continue; // Skip ID field
                }

                // Only check if field exists in schema, don't add new fields
                if !schema_cache.fields.contains_key(key) {
                    // In fast path, we don't track new fields for schema evolution
                    // This is a performance optimization for mature schemas
                    continue;
                }

                // Type validation against existing schema
                if let Some(field_def) = schema_cache.fields.get(key) {
                    let inferred_type = if key == "id" {
                        TantivyFieldType::Text
                    } else {
                        match value {
                            JsonValue::String(s) => {
                                // Try to infer date from string
                                if chrono::DateTime::parse_from_rfc3339(s).is_ok() {
                                    TantivyFieldType::Date
                                } else if s.parse::<std::net::IpAddr>().is_ok() {
                                    TantivyFieldType::Ip
                                } else {
                                    TantivyFieldType::Text
                                }
                            }
                            JsonValue::Number(n) => {
                                if n.is_i64() {
                                    TantivyFieldType::I64
                                } else if n.is_u64() {
                                    TantivyFieldType::U64
                                } else {
                                    TantivyFieldType::F64
                                }
                            }
                            JsonValue::Bool(_) => TantivyFieldType::Boolean,
                            JsonValue::Array(_) => TantivyFieldType::Text, // Arrays as text
                            JsonValue::Object(_) => TantivyFieldType::Json, // Objects as JSON
                            JsonValue::Null => TantivyFieldType::Text,
                        }
                    };

                    if inferred_type != field_def.field_type {
                        return SchemaValidationResult {
                            needs_evolution: false,
                            new_fields: Vec::new(),
                            validation_error: Some(format!(
                                "Type mismatch for field '{}': expected {:?}, got {:?}",
                                key, field_def.field_type, inferred_type
                            )),
                        };
                    }
                }
            }
        }

        SchemaValidationResult {
            needs_evolution: false, // Fast path never needs evolution
            new_fields: Vec::new(), // No new fields tracked in fast path
            validation_error: None,
        }
    }

    /// Read-only validation for a single document (no mutations)
    fn validate_single_document_readonly(
        &self,
        doc: &JsonValue,
        schema_cache: &IndexSchema,
        _is_initial_creation: bool,
    ) -> SchemaValidationResult {
        // Check 1: Ensure doc["id"] exists
        if !doc.is_object() || !doc.as_object().unwrap().contains_key("id") {
            return SchemaValidationResult {
                needs_evolution: false,
                new_fields: Vec::new(),
                validation_error: Some("Document must contain an 'id' field".to_string()),
            };
        }

        let mut needs_evolution = false;
        let mut new_fields = Vec::new();

        // Check 2: Validate fields and identify new ones
        if let Some(obj) = doc.as_object() {
            for (key, value) in obj {
                let inferred_type = if key == "id" {
                    TantivyFieldType::Text
                } else {
                    match value {
                        JsonValue::String(s) => {
                            // Try to infer date from string
                            if chrono::DateTime::parse_from_rfc3339(s).is_ok() {
                                TantivyFieldType::Date
                            } else if s.parse::<std::net::IpAddr>().is_ok() {
                                TantivyFieldType::Ip
                            } else {
                                TantivyFieldType::Text
                            }
                        }
                        JsonValue::Number(n) => {
                            if n.is_i64() {
                                TantivyFieldType::I64
                            } else if n.is_u64() {
                                TantivyFieldType::U64
                            } else {
                                TantivyFieldType::F64
                            }
                        }
                        JsonValue::Bool(_) => TantivyFieldType::Boolean,
                        JsonValue::Array(_) => TantivyFieldType::Text, // Arrays as text
                        JsonValue::Object(_) => TantivyFieldType::Json, // Objects as JSON
                        JsonValue::Null => TantivyFieldType::Text,
                    }
                };

                if let Some(existing_field) = schema_cache.fields.get(key) {
                    // Check type compatibility (read-only)
                    let mut is_compatible = existing_field.field_type == inferred_type;

                    // Allow Text to match String (backward compatibility)
                    if !is_compatible
                        && inferred_type == TantivyFieldType::Text
                        && existing_field.field_type == TantivyFieldType::String
                    {
                        is_compatible = true;
                    }

                    // Allow Text to evolve to more specific types
                    if !is_compatible && existing_field.field_type == TantivyFieldType::Text {
                        match inferred_type {
                            TantivyFieldType::Date
                            | TantivyFieldType::Ip
                            | TantivyFieldType::I64
                            | TantivyFieldType::U64
                            | TantivyFieldType::F64
                            | TantivyFieldType::Boolean
                            | TantivyFieldType::Json => {
                                is_compatible = true;
                            }
                            _ => {}
                        }
                    }

                    // Allow numeric upgrades
                    if !is_compatible {
                        match (&existing_field.field_type, inferred_type.clone()) {
                            (TantivyFieldType::I64, TantivyFieldType::F64)
                            | (TantivyFieldType::U64, TantivyFieldType::F64) => {
                                is_compatible = true;
                            }
                            _ => {}
                        }
                    }

                    if !is_compatible {
                        return SchemaValidationResult {
                            needs_evolution: false,
                            new_fields: Vec::new(),
                            validation_error: Some(format!(
                                "Type mismatch for field '{}': expected {:?}, got {:?}",
                                key, existing_field.field_type, inferred_type
                            )),
                        };
                    }
                } else {
                    // New field detected
                    needs_evolution = true;
                    new_fields.push((key.clone(), inferred_type));
                }
            }
        }

        SchemaValidationResult {
            needs_evolution,
            new_fields,
            validation_error: None,
        }
    }

    /// Optimized schema evolution for batch processing
    async fn evolve_schema_sequential(
        &self,
        index: &str,
        schema_cache: &mut IndexSchema,
        new_fields: &std::collections::HashSet<(String, TantivyFieldType)>,
        shards: &HashMap<Uuid, MicroshardActor>,
    ) -> Result<(), OrchestratorError> {
        let is_initial_creation = schema_cache.fields.is_empty();

        // For initial creation with many fields, do optimized batch processing
        if is_initial_creation && new_fields.len() > 10 {
            tracing::debug!(
                index = %index,
                fields_count = new_fields.len(),
                "Optimized initial schema creation with batch field addition"
            );

            // Add all fields at once for better performance
            let indexed = true; // All fields indexed in initial creation
            for (field_name, field_type) in new_fields {
                if !schema_cache.fields.contains_key(field_name) {
                    let mut new_field = FieldDef::new(field_name.clone(), field_type.clone());
                    new_field.indexed = indexed;
                    schema_cache.fields.insert(field_name.clone(), new_field);
                }
            }

            tracing::info!(
                index = %index,
                total_fields = schema_cache.fields.len(),
                "Initial schema created with batch optimization"
            );
        } else {
            // Filter only truly new fields to avoid redundant work
            let fields_to_add: Vec<_> = new_fields
                .iter()
                .filter(|(field_name, _)| !schema_cache.fields.contains_key(field_name))
                .collect();

            if fields_to_add.is_empty() {
                return Ok(());
            }

            tracing::debug!(
                index = %index,
                fields_count = fields_to_add.len(),
                is_initial_creation = is_initial_creation,
                "Batch adding new fields to schema"
            );

            // Batch add all fields at once for better performance
            let indexed = is_initial_creation; // All fields indexed in initial creation
            for (field_name, field_type) in &fields_to_add {
                let mut new_field = FieldDef::new(field_name.clone(), field_type.clone());
                new_field.indexed = indexed;
                schema_cache.fields.insert(field_name.clone(), new_field);
            }

            tracing::info!(
                index = %index,
                fields_count = fields_to_add.len(),
                "Schema evolution completed - batch added fields"
            );
        }

        // Persist updated schema to storage if changed
        if !new_fields.is_empty() {
            let index_name = index.to_string();
            let schema_clone = schema_cache.clone();

            // Collect all stores from local shards
            let stores: Vec<Arc<HybridStore>> = shards
                .values()
                .filter_map(|shard| shard.store.as_ref().map(Arc::clone))
                .collect();

            if stores.is_empty() {
                return Err(OrchestratorError::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "No local stores available to persist schema",
                )));
            }

            // Persist to all stores concurrently
            let handles: Vec<_> = stores
                .into_iter()
                .map(|store| {
                    let idx = index_name.clone();
                    let sch = schema_clone.clone();
                    tokio::task::spawn_blocking(move || store.store_schema_and_cache(&idx, &sch))
                })
                .collect();

            // Await all results
            for handle in handles {
                handle
                    .await
                    .map_err(|e| {
                        OrchestratorError::Io(std::io::Error::other(format!(
                            "Failed to spawn schema update task: {}",
                            e
                        )))
                    })?
                    .map_err(|e| {
                        OrchestratorError::Io(std::io::Error::other(format!(
                            "Failed to store schema: {}",
                            e
                        )))
                    })?;
            }

            if is_initial_creation {
                tracing::info!(
                    index = %index,
                    field_count = schema_cache.fields.len(),
                    "Initial schema created with all fields indexed=true"
                );
            } else {
                tracing::info!(
                    index = %index,
                    total_fields = schema_cache.fields.len(),
                    new_fields_count = new_fields.len(),
                    "Schema evolved with new fields"
                );
            }
        }

        Ok(())
    }

    /// Process local shard batches sequentially, relying on actor message queues for proper isolation.
    ///
    /// Each shard actor processes its messages sequentially from its own queue,
    /// preventing concurrent access to shared storage resources.
    async fn parallel_local_shard_processing(
        &self,
        index: &str,
        local_batches: HashMap<Uuid, Vec<(DocPayload, Option<String>)>>,
    ) -> Result<(usize, Vec<String>), OrchestratorError> {
        if local_batches.is_empty() {
            return Ok((0, Vec::new()));
        }

        let total_docs: usize = local_batches.values().map(|v| v.len()).sum();
        let shard_count = local_batches.len();

        tracing::debug!(
            local_shard_count = shard_count,
            total_docs = total_docs,
            "Starting local shard processing"
        );

        let mut total_written = 0usize;
        let mut all_errors = Vec::new();

        // Collect shard_ids before consuming local_batches in the loop
        let shard_ids: Vec<Uuid> = local_batches.keys().copied().collect();

        // Process each shard sequentially to avoid concurrent access to shared storage
        for (shard_id, batch) in local_batches {
            tracing::debug!(
                shard_id = %shard_id,
                count = batch.len(),
                "Processing bulk write batch for local shard"
            );

            if let Some(shard) = self.shards.get(&shard_id) {
                let docs: Vec<DocPayload> = batch
                    .into_iter()
                    .map(|(d, effective_routing_key)| DocPayload {
                        id: d.id,
                        routing_key: effective_routing_key,
                        doc: d.doc,
                    })
                    .collect();

                match shard
                    .handle_batch_write(BatchWriteRequest {
                        index: index.to_string(),
                        docs,
                    })
                    .await
                {
                    Ok(seq_ids) => {
                        tracing::info!(
                            shard_id = %shard_id,
                            written_count = seq_ids.len(),
                            "Local shard batch completed successfully"
                        );
                        total_written += seq_ids.len();
                    }
                    Err(e) => {
                        let error_msg = format!("Shard {}: {}", shard_id, e);
                        tracing::warn!(error = %error_msg, "Local shard batch processing failed");
                        all_errors.push(error_msg);
                    }
                }
            } else {
                let error_msg = format!("Local shard {} not found", shard_id);
                tracing::error!(error = %error_msg, "Shard not found during processing");
                all_errors.push(error_msg);
            }
        }

        tracing::info!(
            "Local shard processing completed - total_written: {}, errors: {}",
            total_written,
            all_errors.len()
        );

        // Optimized commit strategy: parallel commits with adaptive timing
        if !all_errors.is_empty() {
            tracing::warn!(
                "Skipping commit due to {} errors during batch processing",
                all_errors.len()
            );
        } else {
            // Reset supervisor timers to prevent race condition with final commits
            // This keeps supervisors alive but gives them fresh timers
            for shard_id in &shard_ids {
                if let Some(shard) = self.shards.get(shard_id) {
                    shard.reset_supervisor_before_commit(index).await;
                }
            }

            // Commit from all shards that were processed in parallel for better performance
            let commit_tasks: Vec<_> = shard_ids
                .iter()
                .filter_map(|shard_id| {
                    if let Some(shard) = self.shards.get(shard_id) {
                        if let Some(store) = &shard.store {
                            let store = store.clone();
                            let index = index.to_string();
                            let shard_id = *shard_id;
                            Some(tokio::task::spawn_blocking(move || {
                                let result = store.commit_index(&index);
                                (shard_id, result)
                            }))
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                })
                .collect();

            // Execute all commits in parallel and collect results
            let commit_results = join_all(commit_tasks).await;

            let mut successful_commits = 0;
            let mut failed_commits = 0;

            for result in commit_results {
                match result {
                    Ok((shard_id, commit_result)) => match commit_result {
                        Ok(()) => {
                            successful_commits += 1;
                            tracing::debug!(shard_id = %shard_id, "Commit successful");
                        }
                        Err(e) => {
                            failed_commits += 1;
                            tracing::warn!(shard_id = %shard_id, error = %e, "Failed to commit index after batch processing");
                        }
                    },
                    Err(e) => {
                        failed_commits += 1;
                        tracing::warn!(error = %e, "Commit task failed");
                    }
                }
            }

            tracing::info!(
                successful_commits = successful_commits,
                failed_commits = failed_commits,
                "Parallel batch commits completed"
            );
        }

        Ok((total_written, all_errors))
    }

    /// Forward a bulk batch to a remote node's orchestrator.
    async fn forward_bulk_to_remote(
        &self,
        node_id: Uuid,
        peer_addr: &str,
        index: &str,
        docs: Vec<DocPayload>,
    ) -> Result<usize, OrchestratorError> {
        let orchestrator_name = orchestrator_remote_name(&node_id);
        info!(
            "🔎 Forwarding bulk batch to remote orchestrator: name='{}', node_id={}, addr={}, docs={}",
            orchestrator_name,
            node_id,
            peer_addr,
            docs.len()
        );

        let remote_ref: Option<RemoteActorRef<NodeOrchestrator>> =
            RemoteActorRef::lookup(orchestrator_name.clone())
                .await
                .map_err(|e| {
                    warn!("❌ Remote actor lookup error: {}", e);
                    OrchestratorError::Io(std::io::Error::other(e.to_string()))
                })?;

        let remote = remote_ref.ok_or_else(|| {
            OrchestratorError::Io(std::io::Error::other(format!(
                "Remote orchestrator {} not found",
                orchestrator_name
            )))
        })?;

        // Send bulk write operation to remote node
        let _op = ClientOp::BulkWrite {
            index: index.to_string(),
            docs,
        };

        let res = remote
            .ask(&_op)
            .await
            .map_err(|e| OrchestratorError::Io(std::io::Error::other(e.to_string())))?;

        // Extract the number of items written from the response
        if let Some(items_written) = res.get("items_written").and_then(|v| v.as_u64()) {
            Ok(items_written as usize)
        } else {
            Err(OrchestratorError::Io(std::io::Error::other(
                "Invalid response from remote bulk write",
            )))
        }
    }

    /// Fetch a schema from cache if present.
    async fn get_cached_schema(&self, index: &str) -> Option<IndexSchema> {
        let map = self.schema_cache.read().await;
        map.get(index).cloned()
    }

    /// Insert or replace a schema in the cache.
    async fn put_cached_schema(&self, index: &str, schema: &IndexSchema) {
        let mut map = self.schema_cache.write().await;
        map.insert(index.to_string(), schema.clone());
    }

    fn default_shard_count(&self) -> u32 {
        std::cmp::max(1, self.shards.len() as u32)
    }

    /// Produce sorted field names with "id" first (if present), others alphabetical.
    fn sorted_field_names(schema: &IndexSchema) -> Vec<String> {
        let mut names: Vec<String> = schema.fields.keys().cloned().collect();
        names.sort_by(|a, b| match (a.as_str(), b.as_str()) {
            ("id", "id") => std::cmp::Ordering::Equal,
            ("id", _) => std::cmp::Ordering::Less,
            (_, "id") => std::cmp::Ordering::Greater,
            _ => a.cmp(b),
        });
        names
    }

    /// Produce a JSON object of fields, ordered with "id" first (if present).
    fn sorted_fields_map(schema: &IndexSchema) -> JsonMap<String, JsonValue> {
        let mut entries: Vec<_> = schema.fields.iter().collect();
        entries.sort_by(|(a, _), (b, _)| match (a.as_str(), b.as_str()) {
            ("id", "id") => std::cmp::Ordering::Equal,
            ("id", _) => std::cmp::Ordering::Less,
            (_, "id") => std::cmp::Ordering::Greater,
            _ => a.cmp(b),
        });

        let mut map = JsonMap::new();
        for (k, v) in entries {
            let value = serde_json::to_value(v).unwrap_or(JsonValue::Null);
            map.insert(k.clone(), value);
        }
        map
    }

    fn schema_response(
        field_names: Vec<String>,
        fields: JsonMap<String, JsonValue>,
        shard_count: u32,
    ) -> JsonValue {
        let mut map = JsonMap::new();
        map.insert(
            "field_names".to_string(),
            JsonValue::Array(field_names.into_iter().map(JsonValue::String).collect()),
        );
        map.insert("fields".to_string(), JsonValue::Object(fields));
        map.insert(
            "shard_count".to_string(),
            JsonValue::Number(serde_json::Number::from(shard_count)),
        );
        JsonValue::Object(map)
    }

    /// Creates a new NodeOrchestrator with the given configuration and identity.
    pub async fn new(
        config: NodeConfig,
        identity: NodeIdentity,
        default_search_limit: usize,
    ) -> Result<Self, OrchestratorError> {
        // Ensure storage directory exists
        fs::create_dir_all(&config.storage_path)?;

        info!("Node identity: {} ({})", identity.name, identity.uuid);

        let mut orchestrator = Self {
            shards: HashMap::new(),
            identity,
            config,
            routing_ring: ConsistentRing::new(),
            coordinator: None,
            schema_cache: AsyncRwLock::new(HashMap::new()),
            default_search_limit,
        };

        // Discover and hydrate existing shards
        orchestrator.hydrate_existing_shards().await?;

        Ok(orchestrator)
    }

    /// Set the coordinator ActorRef after it is spawned (used for shard registration).
    pub fn set_coordinator(&mut self, coordinator: ActorRef<ClusterCoordinator>) {
        self.coordinator = Some(coordinator);
    }

    /// Scans the storage directory for existing shard folders and hydrates them in parallel.
    async fn hydrate_existing_shards(&mut self) -> Result<(), OrchestratorError> {
        let existing_shards = self.discover_existing_shards()?;
        info!("Found {} existing shards", existing_shards.len());

        // Process all shards in parallel for maximum startup speed
        let mut shard_tasks: Vec<tokio::task::JoinHandle<ShardTaskResult>> = Vec::new();

        // Create tasks for all shards
        for &shard_id in &existing_shards {
            let storage_config = self.create_shard_storage_config(shard_id);
            let default_search_limit = self.default_search_limit;

            let task = tokio::spawn(async move {
                let mut microshard =
                    MicroshardActor::new(shard_id, storage_config, default_search_limit);

                match microshard.start().await {
                    Ok(()) => {
                        info!("Hydrated shard {}", shard_id);
                        Ok((shard_id, Some(microshard)))
                    }
                    Err(e) => {
                        error!("Failed to hydrate shard {}: {}", shard_id, e);
                        Ok((shard_id, None))
                    }
                }
            });
            shard_tasks.push(task);
        }

        // Wait for all shard tasks to complete
        for task in shard_tasks {
            match task.await {
                Ok(Ok((shard_id, Some(microshard)))) => {
                    if self.shards.len() < self.config.max_shards {
                        self.shards.insert(shard_id, microshard);
                        self.register_shard_for_routing(shard_id);
                    }
                }
                Ok(Ok((_, None))) => {
                    // Shard failed to hydrate, already logged above
                }
                Ok(Err(e)) => {
                    error!("Shard hydration task error: {}", e);
                }
                Err(e) => {
                    error!("Shard hydration task panicked: {}", e);
                }
            }
        }

        info!(
            "NodeOrchestrator startup complete with {} active shards",
            self.shards.len()
        );

        // Preload schemas for all shards for optimal runtime performance
        // Since all shards have the same schema for each index, we only need to load once per index
        info!(
            "Starting schema preloading for {} shards",
            self.shards.len()
        );

        // Get all unique index names from the first shard (all shards have the same indexes)
        let mut all_index_names = std::collections::HashSet::new();
        if let Some((first_shard_id, _)) = self.shards.iter().next()
            && let Some(shard) = self.shards.get(first_shard_id)
            && let Some(store) = &shard.store
        {
            let sc = Arc::clone(store);
            let index_names = tokio::task::spawn_blocking({
                let sc_clone = Arc::clone(&sc);
                move || sc_clone.get_index_names_lightweight()
            })
            .await
            .map_err(|e| OrchestratorError::Io(std::io::Error::other(e.to_string())))?
            .map_err(|e| OrchestratorError::Io(std::io::Error::other(e.to_string())))?;

            for index_name in index_names {
                all_index_names.insert(index_name);
            }
        }

        info!(
            "Found {} unique indexes to preload schemas for",
            all_index_names.len()
        );

        // Load schema for each index once (from any shard that has it)
        for index_name in all_index_names {
            // Try to load schema from the first available shard that has this index
            let mut schema_loaded = false;
            for (shard_id, shard) in &self.shards {
                if let Some(store) = &shard.store {
                    let sc = Arc::clone(store);
                    let index_name_clone = index_name.clone();

                    match tokio::task::spawn_blocking({
                        let sc_clone = Arc::clone(&sc);
                        let index = index_name_clone.clone();
                        move || sc_clone.get_schema(&index)
                    })
                    .await
                    .map_err(|e| OrchestratorError::Io(std::io::Error::other(e.to_string())))?
                    .map_err(|e| OrchestratorError::Io(std::io::Error::other(e.to_string())))?
                    {
                        Some(schema) => {
                            self.put_cached_schema(&index_name, &schema).await;
                            debug!(index = %index_name, field_count = schema.fields.len(), "Preloaded schema from shard {}", shard_id);
                            schema_loaded = true;
                            break; // Schema loaded successfully, no need to check other shards
                        }
                        None => {
                            // This shard doesn't have the index, try next shard
                            continue;
                        }
                    }
                }
            }

            if !schema_loaded {
                warn!(index = %index_name, "Failed to load schema from any shard");
            }
        }

        info!("Schema preloading completed for all indexes");
        Ok(())
    }

    /// Scans the storage directory for existing shard folders.
    fn discover_existing_shards(&self) -> Result<Vec<Uuid>, OrchestratorError> {
        let mut shard_ids = Vec::new();

        if !self.config.storage_path.exists() {
            return Ok(shard_ids);
        }

        for entry in fs::read_dir(&self.config.storage_path)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_dir()
                && let Some(dir_name) = path.file_name().and_then(|n| n.to_str())
                && let Some(uuid_str) = dir_name.strip_prefix("shard-")
                && let Ok(shard_id) = Uuid::parse_str(uuid_str)
            {
                shard_ids.push(shard_id);
                info!("Discovered existing shard: {}", shard_id);
            }
        }

        Ok(shard_ids)
    }

    /// Creates a storage configuration for a specific shard.
    fn create_shard_storage_config(&self, shard_id: Uuid) -> StorageConfig {
        let shard_path = self.config.storage_path.join(format!("shard-{}", shard_id));

        // Start at the minimum writer memory; storage will scale between min/max as the index grows.
        let indexer_memory_mb = self.config.indexer_memory_min_mb;

        StorageConfig {
            shard_path,
            indexer_memory_budget: indexer_memory_mb * 1024 * 1024, // Convert to bytes
            indexer_memory_min_mb: self.config.indexer_memory_min_mb,
            indexer_memory_max_mb: self.config.indexer_memory_max_mb,
            default_batch_size: self.config.default_batch_size,
            wal_sync: self.config.wal_sync,
        }
    }

    /// Handles a ProposeShard message to create a new shard.
    #[cfg_attr(not(test), allow(dead_code))]
    pub async fn handle_propose_shard(
        &mut self,
        msg: ProposeShard,
    ) -> Result<Uuid, OrchestratorError> {
        let shard_id = msg.shard_id;

        info!("Received ProposeShard request for {}", shard_id);

        // Check if shard already exists
        if self.shards.contains_key(&shard_id) {
            return Err(OrchestratorError::ShardAlreadyExists { shard_id });
        }

        // Check shard limit
        if self.shards.len() >= self.config.max_shards {
            return Err(OrchestratorError::ShardLimitExceeded {
                current: self.shards.len(),
                max: self.config.max_shards,
            });
        }

        // Create shard directory
        let shard_path = self.config.storage_path.join(format!("shard-{}", shard_id));
        fs::create_dir_all(&shard_path)?;
        info!("Created shard directory: {:?}", shard_path);

        // Create and start microshard actor
        let storage_config = self.create_shard_storage_config(shard_id);
        let mut microshard =
            MicroshardActor::new(shard_id, storage_config, self.default_search_limit);
        microshard.start().await?;

        // Add to shards map
        self.shards.insert(shard_id, microshard);
        self.register_shard_for_routing(shard_id);
        if let Err(err) = self.register_shard_with_coordinator(shard_id).await {
            warn!(%shard_id, error = %err, "Failed to register new shard with coordinator");
        }

        info!(
            "Successfully created shard {} ({}/{})",
            shard_id,
            self.shard_count(),
            self.config.max_shards
        );
        Ok(shard_id)
    }

    /// Gets the node identity.
    pub fn identity(&self) -> &NodeIdentity {
        &self.identity
    }

    /// Builds ShardMetadata for a given shard id (storage stats currently stubbed).
    fn shard_metadata(&self, shard_id: Uuid) -> ShardMetadata {
        ShardMetadata {
            shard_id,
            node_id: self.identity.uuid,
            vnode_tokens: generate_tokens(shard_id),
            storage_bytes: 0,
            document_count: 0,
        }
    }

    /// Registers a single shard with the coordinator if available.
    async fn register_shard_with_coordinator(
        &self,
        shard_id: Uuid,
    ) -> Result<(), OrchestratorError> {
        if let Some(coordinator) = &self.coordinator {
            let metadata = self.shard_metadata(shard_id);
            coordinator
                .ask(RegisterLocalShards {
                    node_id: self.identity.uuid,
                    shards: vec![metadata],
                })
                .await
                .map_err(|e| OrchestratorError::Io(std::io::Error::other(e)))?;
        } else {
            warn!(%shard_id, "Coordinator not set; skipping shard registration");
        }
        Ok(())
    }

    /// Registers all known shards with the coordinator (called on startup after coordinator set).
    pub async fn register_all_shards_with_coordinator(&self) -> Result<(), OrchestratorError> {
        if let Some(coordinator) = &self.coordinator {
            let shards: Vec<ShardMetadata> = self
                .shards
                .keys()
                .copied()
                .map(|shard_id| self.shard_metadata(shard_id))
                .collect();
            if !shards.is_empty() {
                let _: () = coordinator
                    .ask(RegisterLocalShards {
                        node_id: self.identity.uuid,
                        shards,
                    })
                    .await
                    .map_err(|e| OrchestratorError::Io(std::io::Error::other(e)))?;
            }
        } else {
            warn!("Coordinator not set; skipping bulk shard registration");
        }
        Ok(())
    }

    /// Shutdown all shards gracefully, committing pending writes and releasing resources.
    pub async fn shutdown_all_shards(&self) -> Result<(), OrchestratorError> {
        tracing::info!("NodeOrchestrator: Shutting down all shards");

        let mut errors = Vec::new();

        for (shard_id, shard) in self.shards.iter() {
            tracing::debug!(shard_id = %shard_id, "Shutting down shard");

            if let Some(store) = shard.store.as_ref() {
                let store_clone = store.clone();
                let shard_id_clone = *shard_id;

                // Call shutdown in spawn_blocking since it's a blocking operation
                match tokio::task::spawn_blocking(move || {
                    tracing::info!(shard_id = %shard_id_clone, "Calling storage shutdown");
                    store_clone.shutdown()
                })
                .await
                {
                    Ok(Ok(())) => {
                        tracing::debug!(shard_id = %shard_id, "Shard storage shutdown successful");
                    }
                    Ok(Err(e)) => {
                        tracing::error!(shard_id = %shard_id, error = %e, "Shard storage shutdown failed");
                        errors.push(format!("Shard {} shutdown error: {}", shard_id, e));
                    }
                    Err(e) => {
                        tracing::error!(shard_id = %shard_id, error = %e, "Failed to execute shutdown task");
                        errors.push(format!("Shard {} task error: {}", shard_id, e));
                    }
                }
            }
        }

        if errors.is_empty() {
            tracing::info!("NodeOrchestrator: All shards shut down successfully");
            Ok(())
        } else {
            tracing::warn!(
                error_count = errors.len(),
                "NodeOrchestrator: Some shards failed to shutdown"
            );
            Err(OrchestratorError::Io(std::io::Error::other(format!(
                "Shutdown errors: {}",
                errors.join("; ")
            ))))
        }
    }

    /// Registers a shard with the routing ring for consistent hashing.
    fn register_shard_for_routing(&mut self, shard_id: Uuid) {
        let simple = shard_id.simple().to_string();
        let name: String = simple.chars().take(3).collect();
        let identity = NodeIdentity {
            uuid: shard_id,
            name,
            vnode_tokens: generate_tokens(shard_id),
            keypair: None,
        };
        self.routing_ring.add_node(&identity);
    }

    /// Determines the shard that should handle a routing key.
    fn select_shard_for_key(&self, key: &str) -> Option<Uuid> {
        self.routing_ring.get_owner(key)
    }

    /// Returns the first shard id if any exist (fallback for empty ring).
    fn first_shard_id(&self) -> Option<Uuid> {
        self.shards.keys().copied().next()
    }

    /// Gets the number of active shards.
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    // ========================================================================
    // Client Operation Handling (for actor-based access - no locks needed)
    // ========================================================================

    /// Handles client operations. Called from Message<ClientOp> handler.
    pub async fn handle_client_op(&mut self, op: ClientOp) -> Result<JsonValue, OrchestratorError> {
        match op {
            ClientOp::Search {
                index,
                query,
                limit,
            } => {
                self.orch_search(&index, &query, limit.unwrap_or(self.default_search_limit))
                    .await
            }
            ClientOp::Stream {
                index,
                query,
                limit,
            } => {
                // Use streaming search with the same logic as Search but optimized for HTTP streaming
                let search_limit = limit.unwrap_or(self.default_search_limit);
                self.orch_search(&index, &query, search_limit).await
            }
            ClientOp::Write {
                index,
                id,
                routing_key,
                doc,
            } => self.orch_write(&index, id, routing_key, doc).await,
            ClientOp::BulkWrite { index, docs } => self.orch_bulk_write(&index, docs).await,
            ClientOp::CreateConfig { index, schema } => {
                self.orch_create_config(&index, schema).await
            }
            ClientOp::GetConfig { index } => self.orch_get_config(&index).await,
            ClientOp::ListIndexes => self.orch_list_indexes(false).await,
            ClientOp::ListClusterIndexes { include_data_size } => {
                self.orch_list_indexes(include_data_size).await
            }
            ClientOp::GetLightweightIndexes { include_data_size } => {
                self.orch_lightweight_indexes(include_data_size).await
            }
            ClientOp::GetIdentity => self.orch_get_identity().await,
            ClientOp::DeleteIndex {
                index,
                delete_schema,
            } => self.orch_delete_index(&index, delete_schema).await,
        }
    }

    /// Delete an index and all its data from all local shards
    async fn orch_delete_index(
        &self,
        index: &str,
        delete_schema: bool,
    ) -> Result<JsonValue, OrchestratorError> {
        if self.shards.is_empty() {
            return Err(OrchestratorError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "No shards available",
            )));
        }

        let mut deleted_from_shards = 0;
        let mut errors = Vec::new();

        // Delete index data from all local shards
        for (shard_id, shard) in &self.shards {
            match shard.delete_index(index, delete_schema).await {
                Ok(_) => {
                    deleted_from_shards += 1;
                    tracing::info!(
                        shard_id = %shard_id,
                        index = %index,
                        delete_schema = delete_schema,
                        "Deleted index data from shard"
                    );
                }
                Err(e) => {
                    // Log but continue - index might not exist on this shard
                    tracing::warn!(
                        shard_id = %shard_id,
                        index = %index,
                        error = %e,
                        "Failed to delete index from shard (may not exist)"
                    );
                    errors.push(format!("shard {}: {}", shard_id, e));
                }
            }
        }

        // Clear schema cache for this index
        {
            let mut cache = self.schema_cache.write().await;
            cache.remove(index);
        }

        Ok(serde_json::json!({
            "success": true,
            "index": index,
            "deleted_from_shards": deleted_from_shards,
            "total_shards": self.shards.len(),
            "errors": errors  // Always return array, empty if no errors
        }))
    }

    async fn orch_write(
        &self,
        index: &str,
        id: String,
        routing_key: Option<String>,
        doc: JsonValue,
    ) -> Result<JsonValue, OrchestratorError> {
        if self.shards.is_empty() {
            return Err(OrchestratorError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "No shards",
            )));
        }
        let mut schema_cache = self.load_schema(index).await?;

        // Evolve schema and persist to ALL local shards
        let updated =
            validate_and_evolve_schema(index, &doc, &mut schema_cache, &self.shards).await?;
        if updated {
            self.put_cached_schema(index, &schema_cache).await;
        }

        // Derive effective routing key (deterministic priority):
        // 1) Explicit routing_key from payload
        // 2) Document id argument
        // 3) Fallback to deterministic key derived from document bytes
        let effective_routing_key = routing_key
            .clone()
            .or_else(|| (!id.is_empty()).then(|| id.clone()))
            .or_else(|| derive_routing_key_from_doc(&doc));

        let target = self.route_write(&effective_routing_key)?;
        let shard = self.shards.get(&target).ok_or_else(|| {
            OrchestratorError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Shard not found",
            ))
        })?;
        let req = WriteRequest {
            index: index.to_string(),
            routing_key: effective_routing_key.unwrap_or_default(),
            doc,
        };

        match shard.handle_write(req).await {
            Ok(seq) => Ok(
                serde_json::json!({"id": id, "result": "created", "version": seq, "shard_id": target.to_string()}),
            ),
            Err(e) => Err(e),
        }
    }

    async fn orch_bulk_write(
        &self,
        index: &str,
        docs: Vec<DocPayload>,
    ) -> Result<JsonValue, OrchestratorError> {
        let start = std::time::Instant::now();
        if self.shards.is_empty() {
            return Err(OrchestratorError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "No shards",
            )));
        }

        // Load schema and perform staged validation for all documents before writing
        let mut schema_cache = self.load_schema(index).await?;

        // Use staged schema validation: parallel validation + sequential evolution
        let validation_summary = self
            .staged_schema_validation(index, &docs, &mut schema_cache)
            .await?;

        // Update cache only if schema evolved AND wasn't already cached during load
        if validation_summary.evolution_needed && self.get_cached_schema(index).await.is_none() {
            self.put_cached_schema(index, &schema_cache).await;
        }

        // Check for validation errors
        if !validation_summary.errors.is_empty() {
            tracing::warn!(
                error_count = validation_summary.errors.len(),
                total_docs = validation_summary.total_docs,
                "Some documents failed schema validation"
            );
            // Continue processing valid documents, errors are tracked separately
        }

        // Group documents by target shard using parallel routing for better performance
        let items_received = docs.len();

        // First, route all documents to determine local vs remote
        let mut local_docs = Vec::new();
        let mut remote_docs = Vec::new();

        // Clone routing ring for parallel access
        let routing_ring = self.routing_ring.clone();
        let first_shard_id = self.first_shard_id();

        // Get shard assignments to determine ownership (used later for remote routing)
        let shard_assignments = if let Some(coord) = &self.coordinator {
            coord.ask(GetShardAssignments).await.unwrap_or_default()
        } else {
            HashMap::new()
        };

        // Route documents in parallel
        let routing_results: Vec<RoutingResult> =
            docs.into_par_iter()
                .map(|doc| {
                    // Calculate effective routing key
                    let effective_routing_key = doc
                        .routing_key
                        .clone()
                        .or_else(|| (!doc.id.is_empty()).then(|| doc.id.clone()))
                        .or_else(|| derive_routing_key_from_doc(&doc.doc));

                    // Route to shard using consistent hash ring
                    let target_shard =
                        match effective_routing_key.as_ref() {
                            Some(key) => routing_ring
                                .get_owner(key)
                                .or(first_shard_id)
                                .ok_or_else(|| {
                                    OrchestratorError::Io(std::io::Error::new(
                                        std::io::ErrorKind::NotFound,
                                        "No shard available for routing",
                                    ))
                                })?,
                            None => {
                                return Err(OrchestratorError::Io(std::io::Error::new(
                                    std::io::ErrorKind::InvalidInput,
                                    "Missing routing key for document",
                                )));
                            }
                        };

                    Ok((doc, effective_routing_key, Some(target_shard)))
                })
                .collect();

        // Separate local and remote documents
        for result in routing_results {
            match result {
                Ok((doc, routing_key, Some(target_shard))) => {
                    // Check if this shard is local
                    if self.shards.contains_key(&target_shard) {
                        local_docs.push((doc, routing_key, target_shard));
                    } else {
                        remote_docs.push((doc, routing_key, target_shard));
                    }
                }
                Ok((doc, _, None)) => {
                    // No target shard - this shouldn't happen but handle gracefully
                    tracing::warn!("Document routed to no shard: {}", doc.id);
                }
                Err(e) => {
                    tracing::warn!("Routing error: {}", e);
                }
            }
        }

        // Group local documents by shard
        let batches = self.group_local_documents(local_docs).await?;
        let unique_shards = batches.len();

        tracing::debug!(
            items_received = items_received,
            unique_shards = unique_shards,
            remote_docs = remote_docs.len(),
            "BulkWrite grouped items by shard"
        );

        // Fetch shard ownership and peer addresses to forward remote batches.
        let mut peer_addrs = HashMap::new();
        if let Some(coord) = &self.coordinator {
            peer_addrs = coord
                .ask(GetKnownPeers)
                .await
                .unwrap_or_default()
                .into_iter()
                .map(|p| (p.node_id, p.address))
                .collect();
        }

        // Separate local and remote batches for parallel processing
        let mut local_batches = HashMap::new();
        let mut remote_batches = Vec::new();
        let mut written = 0usize;
        let mut errors = Vec::new();

        // Process local batches from parallel routing
        for (shard_id, batch) in batches {
            local_batches.insert(shard_id, batch);
        }

        // Group remote documents by owning node
        let mut remote_by_node: HashMap<Uuid, Vec<DocPayload>> = HashMap::new();
        for (doc, routing_key, target_shard) in remote_docs {
            if let Some(shard_meta) = shard_assignments.get(&target_shard) {
                let owner_node = shard_meta.node_id;
                let doc_payload = DocPayload {
                    id: doc.id,
                    routing_key,
                    doc: doc.doc,
                };
                remote_by_node
                    .entry(owner_node)
                    .or_default()
                    .push(doc_payload);
            } else {
                errors.push(format!(
                    "No shard assignment for shard {}; dropping document",
                    target_shard
                ));
            }
        }

        // Convert remote batches to the expected format
        for (node_id, docs) in remote_by_node {
            if let Some(addr) = peer_addrs.get(&node_id) {
                tracing::debug!(
                    node = %node_id,
                    count = docs.len(),
                    "Forwarding bulk write batch to remote node"
                );
                remote_batches.push((node_id, addr.clone(), docs));
            } else {
                errors.push(format!("No peer address for node {}", node_id));
            }
        }

        // Phase 3.1: Parallel Local Shard Processing
        let (local_written, local_errors) = self
            .parallel_local_shard_processing(index, local_batches)
            .await?;
        written += local_written;
        errors.extend(local_errors);

        // Phase 3.2: Parallel Remote Forwarding
        if !remote_batches.is_empty() {
            use futures::future::join_all;

            let remote_futures: Vec<_> = remote_batches
                .into_iter()
                .map(|(node_id, addr, docs_for_remote)| async move {
                    self.forward_bulk_to_remote(node_id, &addr, index, docs_for_remote)
                        .await
                        .map(|items| (node_id, items))
                })
                .collect();

            let remote_results = join_all(remote_futures).await;

            for result in remote_results {
                match result {
                    Ok((_, items)) => {
                        written += items;
                    }
                    Err(e) => {
                        errors.push(format!("Remote forwarding failed: {}", e));
                    }
                }
            }
        }

        let duration = start.elapsed();
        info!(
            index = %index,
            items_received = items_received,
            items_written = written,
            errors = errors.len(),
            duration_ms = duration.as_millis(),
            "BulkWrite completed"
        );

        if !errors.is_empty() {
            warn!(
                index = %index,
                error_count = errors.len(),
                "BulkWrite had some errors"
            );
        }

        Ok(serde_json::json!({
            "items_written": written,
            "items_received": items_received,
            "errors": errors,
            "duration_ms": duration.as_millis()
        }))
    }

    /// Helper method to group local documents by shard
    async fn group_local_documents(
        &self,
        local_docs: Vec<(DocPayload, Option<String>, Uuid)>,
    ) -> Result<HashMap<Uuid, Vec<(DocPayload, Option<String>)>>, OrchestratorError> {
        let mut batches: HashMap<Uuid, Vec<(DocPayload, Option<String>)>> = HashMap::new();

        for (doc, routing_key, shard_id) in local_docs {
            batches
                .entry(shard_id)
                .or_default()
                .push((doc, routing_key));
        }

        Ok(batches)
    }

    async fn orch_search(
        &self,
        index: &str,
        query: &str,
        limit: usize,
    ) -> Result<JsonValue, OrchestratorError> {
        let start = std::time::Instant::now();
        if self.shards.is_empty() {
            return Ok(
                serde_json::json!({"hits": [], "hits_returned": 0, "total_hits": 0, "took_ms": 0}),
            );
        }
        let mut handles = Vec::new();
        for (&shard_id, shard) in self.shards.iter() {
            let req = SearchRequest {
                index: index.to_string(),
                query: query.to_string(),
                limit: Some(limit),
            };
            let s = shard.clone();
            handles.push(tokio::spawn(async move {
                (shard_id, s.handle_search(req).await)
            }));
        }

        let mut results: Vec<(Uuid, f32, JsonValue)> = Vec::new();
        let mut errors = Vec::new();
        let mut shard_success = 0usize;
        let mut total_hits_sum = 0usize;
        for h in handles {
            match h.await {
                Ok((shard_id, Ok(r))) => {
                    total_hits_sum += r.total_hits;
                    for hit in r.hits {
                        results.push((shard_id, hit.score, hit.doc));
                    }
                    shard_success += 1;
                }
                Ok((shard_id, Err(err))) => {
                    warn!(%shard_id, error = %err, "Scatter search shard failed");
                    errors.push(format!("Shard {}: {}", shard_id, err));
                }
                Err(join_err) => {
                    warn!(error = %join_err, "Scatter search task join failed");
                    errors.push(format!("Join error: {}", join_err));
                }
            }
        }

        // Sort by score descending
        results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        results.truncate(limit);
        let hits: Vec<JsonValue> = results
            .into_iter()
            .map(|(shard_id, score, mut doc)| {
                if let JsonValue::Object(ref mut o) = doc {
                    o.insert(
                        "_score".to_string(),
                        serde_json::Number::from_f64(score as f64)
                            .map(JsonValue::Number)
                            .unwrap_or(JsonValue::Null),
                    );
                    o.insert(
                        "shard_id".to_string(),
                        JsonValue::String(shard_id.to_string()),
                    );
                }
                doc
            })
            .collect();
        Ok(serde_json::json!({
            "hits": hits,
            "hits_returned": hits.len(),
            "total_hits": total_hits_sum,
            "limit": limit,
            "took_ms": start.elapsed().as_millis(),
            "stats": {
                "shards": {
                    "total": self.shards.len(),
                    "responded": shard_success,
                    "failed": errors.len()
                }
            },
            "errors": errors
        }))
    }

    async fn orch_create_config(
        &self,
        index: &str,
        mut schema: IndexSchema,
    ) -> Result<JsonValue, OrchestratorError> {
        // Ensure 'id' field is explicitly in the schema for visibility
        if !schema.fields.contains_key("id") {
            schema.fields.insert(
                "id".to_string(),
                FieldDef::new("id".to_string(), TantivyFieldType::Text),
            );
        }

        let stores: Vec<Arc<HybridStore>> = self
            .shards
            .values()
            .filter_map(|shard| shard.store.as_ref().map(Arc::clone))
            .collect();

        if stores.is_empty() {
            return Err(OrchestratorError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "No local stores available to persist schema",
            )));
        }

        let index_name = index.to_string();
        let schema_clone = schema.clone();

        // Persist to all stores concurrently
        let handles: Vec<_> = stores
            .into_iter()
            .map(|store| {
                let idx = index_name.clone();
                let sch = schema_clone.clone();
                tokio::task::spawn_blocking(move || store.store_schema_and_cache(&idx, &sch))
            })
            .collect();

        for handle in handles {
            handle
                .await
                .map_err(|e| OrchestratorError::Io(std::io::Error::other(e.to_string())))?
                .map_err(|e| OrchestratorError::Io(std::io::Error::other(e.to_string())))?;
        }

        let shard_count = schema.shard_count;
        self.put_cached_schema(index, &schema).await;

        Ok(serde_json::json!({
            "acknowledged": true,
            "index": index,
            "shard_count": shard_count,
            "field_names": Self::sorted_field_names(&schema)
        }))
    }

    async fn orch_get_config(&self, index: &str) -> Result<JsonValue, OrchestratorError> {
        // IMPORTANT: Always get fresh schema from storage layer
        // The storage layer maintains the authoritative schema derived from Tantivy
        // This prevents orchestrator cache staleness issues
        for shard in self.shards.values() {
            if let Some(store) = &shard.store {
                let sc = Arc::clone(store);
                let idx = index.to_string();

                // Use spawn_blocking to safely call blocking storage function
                let schema = tokio::task::spawn_blocking(move || sc.get_schema_cached(&idx))
                    .await
                    .map_err(|e| OrchestratorError::Io(std::io::Error::other(e.to_string())))?
                    .map_err(|e| OrchestratorError::Io(std::io::Error::other(e.to_string())))?;

                if let Some(s) = schema {
                    let field_names = Self::sorted_field_names(&s);
                    let fields = Self::sorted_fields_map(&s);
                    let shard_count = self.default_shard_count();
                    return Ok(Self::schema_response(field_names, fields, shard_count));
                }
            }
        }
        Err(OrchestratorError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "No shards",
        )))
    }

    async fn orch_list_indexes(
        &self,
        include_data_size: bool,
    ) -> Result<JsonValue, OrchestratorError> {
        if self.shards.is_empty() {
            return Ok(serde_json::json!({
                "indexes": [],
                "total_indexes": 0,
                "node_id": self.identity.uuid.to_string(),
                "node_name": self.identity.name.clone(),
                "total_shards": 0
            }));
        }
        let mut all: HashMap<String, (u64, u64, u64, usize)> = HashMap::new();
        let mut field_cache: HashMap<String, Vec<String>> = HashMap::new();
        let mut shard_tasks = Vec::new();

        for (shard_id, shard) in &self.shards {
            if let Some(store) = &shard.store {
                let sc = Arc::clone(store);
                let shard_id = *shard_id;
                shard_tasks.push(tokio::task::spawn_blocking(
                    move || -> Result<_, StoreError> {
                        let snapshot = sc.gather_index_stats_snapshot(include_data_size)?;
                        Ok((shard_id, snapshot))
                    },
                ));
            }
        }

        let mut shard_timings: Vec<(Uuid, ShardStatsTimings)> = Vec::new();
        for task in shard_tasks {
            match task
                .await
                .map_err(|e| OrchestratorError::Io(std::io::Error::other(e.to_string())))?
            {
                Ok((shard_id, snapshot)) => {
                    shard_timings.push((shard_id, snapshot.timings.clone()));

                    for (index_name, stats) in snapshot.per_index {
                        let entry = all.entry(index_name).or_insert((0, 0, 0, 0));
                        entry.0 += stats.document_count;
                        entry.1 += stats.redb_bytes;
                        entry.2 += stats.tantivy_bytes;

                        if stats.document_count > 0
                            || stats.redb_bytes > 0
                            || stats.tantivy_bytes > 0
                            || stats.tantivy_index_exists
                        {
                            entry.3 += 1;
                        }
                    }
                }
                Err(e) => return Err(OrchestratorError::from(e)),
            }
        }

        let mut total_redb_ms: u128 = 0;
        let mut total_tantivy_ms: u128 = 0;
        for (shard_id, timings) in shard_timings {
            debug!(
                shard = %shard_id,
                redb_ms = timings.redb_ms,
                tantivy_ms = timings.tantivy_ms,
                total_ms = timings.total_ms,
                "Collected shard index statistics (full)"
            );

            total_redb_ms = total_redb_ms.max(timings.redb_ms);
            total_tantivy_ms = total_tantivy_ms.max(timings.tantivy_ms);
        }

        let mut indexes: Vec<(String, JsonValue)> = Vec::new();
        for (name, (doc_count, redb_bytes, tantivy_bytes, shard_count)) in all {
            let field_names = if let Some(cached) = field_cache.get(&name) {
                cached.clone()
            } else {
                let schema = self.load_schema(&name).await?;
                let sorted = Self::sorted_field_names(&schema);
                field_cache.insert(name.clone(), sorted.clone());
                sorted
            };

            let total_size_bytes = tantivy_bytes + if include_data_size { redb_bytes } else { 0 };
            let index_size_mb = tantivy_bytes / (1024 * 1024);

            let mut json_obj = JsonMap::new();
            json_obj.insert("name".to_string(), JsonValue::String(name.clone()));
            json_obj.insert("document_count".to_string(), JsonValue::from(doc_count));
            json_obj.insert(
                "total_size_bytes".to_string(),
                JsonValue::from(total_size_bytes),
            );
            json_obj.insert("index_size_mb".to_string(), JsonValue::from(index_size_mb));
            if include_data_size {
                json_obj.insert(
                    "data_size_mb".to_string(),
                    JsonValue::from(redb_bytes / (1024 * 1024)),
                );
            }
            json_obj.insert("shard_count".to_string(), JsonValue::from(shard_count));
            json_obj.insert(
                "field_names".to_string(),
                JsonValue::Array(
                    field_names
                        .iter()
                        .cloned()
                        .map(JsonValue::String)
                        .collect::<Vec<_>>(),
                ),
            );

            indexes.push((name, JsonValue::Object(json_obj)));
        }
        indexes.sort_by(|a, b| a.0.cmp(&b.0));
        let indexes: Vec<JsonValue> = indexes.into_iter().map(|(_, json)| json).collect();
        let response = serde_json::json!({
            "indexes": indexes,
            "total_indexes": indexes.len(),
            "node_id": self.identity.uuid.to_string(),
            "node_name": self.identity.name.clone(),
            "total_shards": self.shards.len(),
            "total_ms": total_redb_ms + total_tantivy_ms,
        });
        Ok(response)
    }

    /// Lightweight index listing without full schema parsing (optimized for _indexes endpoint)
    async fn orch_lightweight_indexes(
        &self,
        include_data_size: bool,
    ) -> Result<JsonValue, OrchestratorError> {
        if self.shards.is_empty() {
            return Ok(serde_json::json!({
                "indexes": [],
                "total_indexes": 0,
                "node_id": self.identity.uuid.to_string(),
                "node_name": self.identity.name.clone(),
                "total_shards": 0
            }));
        }

        let mut all: HashMap<String, (u64, u64, u64, usize)> = HashMap::new();
        let mut field_cache: HashMap<String, Vec<String>> = HashMap::new();
        let mut shard_tasks = Vec::new();

        for (shard_id, shard) in &self.shards {
            if let Some(store) = &shard.store {
                let sc = Arc::clone(store);
                let shard_id = *shard_id;
                shard_tasks.push(tokio::task::spawn_blocking(
                    move || -> Result<_, StoreError> {
                        let snapshot = sc.gather_index_stats_snapshot(include_data_size)?;
                        Ok((shard_id, snapshot))
                    },
                ));
            }
        }

        let mut shard_timings: Vec<(Uuid, ShardStatsTimings)> = Vec::new();
        for task in shard_tasks {
            match task
                .await
                .map_err(|e| OrchestratorError::Io(std::io::Error::other(e.to_string())))?
            {
                Ok((shard_id, snapshot)) => {
                    shard_timings.push((shard_id, snapshot.timings.clone()));

                    for (index_name, stats) in snapshot.per_index {
                        let entry = all.entry(index_name).or_insert((0, 0, 0, 0));
                        entry.0 += stats.document_count;
                        entry.1 += stats.redb_bytes;
                        entry.2 += stats.tantivy_bytes;

                        if stats.document_count > 0
                            || stats.redb_bytes > 0
                            || stats.tantivy_bytes > 0
                            || stats.tantivy_index_exists
                        {
                            entry.3 += 1;
                        }
                    }
                }
                Err(e) => return Err(OrchestratorError::from(e)),
            }
        }

        let mut total_redb_ms: u128 = 0;
        let mut total_tantivy_ms: u128 = 0;
        for (shard_id, timings) in shard_timings {
            debug!(
                shard = %shard_id,
                redb_ms = timings.redb_ms,
                tantivy_ms = timings.tantivy_ms,
                total_ms = timings.total_ms,
                "Collected shard index statistics"
            );

            total_redb_ms = total_redb_ms.max(timings.redb_ms);
            total_tantivy_ms = total_tantivy_ms.max(timings.tantivy_ms);
        }

        let total_ms = total_redb_ms + total_tantivy_ms;

        let mut indexes: Vec<(String, JsonValue)> = Vec::new();
        for (name, (doc_count, redb_bytes, tantivy_bytes, shard_count)) in all {
            let fields = if let Some(cached) = field_cache.get(&name) {
                cached.clone()
            } else {
                let schema = self.load_schema(&name).await?;
                let sorted = Self::sorted_field_names(&schema);
                field_cache.insert(name.clone(), sorted.clone());
                sorted
            };

            let total_size_bytes = tantivy_bytes + if include_data_size { redb_bytes } else { 0 };
            let index_size_mb = tantivy_bytes / (1024 * 1024);

            let mut json_obj = JsonMap::new();
            json_obj.insert("name".to_string(), JsonValue::String(name.clone()));
            json_obj.insert("document_count".to_string(), JsonValue::from(doc_count));
            json_obj.insert(
                "total_size_bytes".to_string(),
                JsonValue::from(total_size_bytes),
            );
            json_obj.insert("index_size_mb".to_string(), JsonValue::from(index_size_mb));
            if include_data_size {
                json_obj.insert(
                    "data_size_mb".to_string(),
                    JsonValue::from(redb_bytes / (1024 * 1024)),
                );
            }
            json_obj.insert("shard_count".to_string(), JsonValue::from(shard_count));
            json_obj.insert(
                "fields".to_string(),
                JsonValue::Array(
                    fields
                        .iter()
                        .cloned()
                        .map(JsonValue::String)
                        .collect::<Vec<_>>(),
                ),
            );

            indexes.push((name, JsonValue::Object(json_obj)));
        }

        indexes.sort_by(|a, b| a.0.cmp(&b.0));
        let indexes: Vec<JsonValue> = indexes.into_iter().map(|(_, json)| json).collect();

        Ok(serde_json::json!({
            "indexes": indexes,
            "total_indexes": indexes.len(),
            "node_id": self.identity.uuid.to_string(),
            "node_name": self.identity.name.clone(),
            "total_shards": self.shards.len(),
            "total_ms": total_ms,
        }))
    }

    /// Get node identity information
    async fn orch_get_identity(&self) -> Result<JsonValue, OrchestratorError> {
        Ok(serde_json::json!({
            "node_id": self.identity.uuid.to_string(),
            "node_name": self.identity.name.clone(),
            "total_shards": self.shards.len()
        }))
    }

    /// Helper: Load schema from first shard
    async fn load_schema(&self, index: &str) -> Result<IndexSchema, OrchestratorError> {
        if let Some(cached) = self.get_cached_schema(index).await {
            return Ok(cached);
        }

        if let Some(shard) = self.shards.values().next()
            && let Some(store) = &shard.store
        {
            let sc = Arc::clone(store);
            let idx = index.to_string();
            let schema = tokio::task::spawn_blocking(move || sc.get_schema(&idx))
                .await
                .map_err(|e| OrchestratorError::Io(std::io::Error::other(e.to_string())))?
                .map_err(|e| OrchestratorError::Io(std::io::Error::other(e.to_string())))?;
            if let Some(schema) = schema {
                self.put_cached_schema(index, &schema).await;
                return Ok(schema);
            }
        }
        Ok(IndexSchema {
            shard_count: self.default_shard_count(),
            fields: HashMap::new(),
        })
    }

    /// Helper: Route write to shard using deterministic key (no round-robin).
    fn route_write(&self, routing_key: &Option<String>) -> Result<Uuid, OrchestratorError> {
        let key = routing_key.as_ref().ok_or_else(|| {
            OrchestratorError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Missing routing key for write",
            ))
        })?;

        let target = self
            .select_shard_for_key(key)
            .or_else(|| self.first_shard_id());

        target.ok_or_else(|| {
            OrchestratorError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "No shard selected",
            ))
        })
    }
}

/// Derive a deterministic routing key from document content.
///
/// Preference order:
/// 1. If the document has an "id" field (string), use that directly.
/// 2. Otherwise, serialize the document to JSON bytes, take a prefix,
///    and hex-encode it to produce a stable routing key string.
fn derive_routing_key_from_doc(doc: &JsonValue) -> Option<String> {
    // Fallback: derive from JSON bytes (deterministic for same document)
    let mut bytes = serde_json::to_vec(doc).ok()?;
    if bytes.is_empty() {
        // Use a fixed token to remain deterministic for empty objects
        return Some("empty-doc".to_string());
    }

    // Limit the number of bytes used to keep the key reasonably sized
    const MAX_PREFIX_LEN: usize = 64;
    if bytes.len() > MAX_PREFIX_LEN {
        bytes.truncate(MAX_PREFIX_LEN);
    }

    // Hex-encode the prefix to a string key; ConsistentRing will hash it again
    let mut key = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut key, "{:02x}", b);
    }
    Some(key)
}

// ============================================================================
/// Message handler for GetShardCount
impl Message<GetShardCount> for NodeOrchestrator {
    type Reply = usize;

    async fn handle(
        &mut self,
        _msg: GetShardCount,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.shards.len()
    }
}

#[remote_message("cameo.orchestrator.client_op")]
impl Message<ClientOp> for NodeOrchestrator {
    type Reply = Result<JsonValue, OrchestratorError>;

    async fn handle(
        &mut self,
        msg: ClientOp,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        match msg {
            ClientOp::DeleteIndex {
                index,
                delete_schema,
            } => self.orch_delete_index(&index, delete_schema).await,
            _ => self.handle_client_op(msg).await,
        }
    }
}

impl Message<UpdateTopology> for NodeOrchestrator {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: UpdateTopology,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        info!(
            ring_nodes = msg.ring.len(),
            "NodeOrchestrator: received global topology update"
        );
        self.routing_ring = msg.ring;
    }
}

impl Message<ShutdownAllShards> for NodeOrchestrator {
    type Reply = Result<(), OrchestratorError>;

    async fn handle(
        &mut self,
        _msg: ShutdownAllShards,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.shutdown_all_shards().await
    }
}

#[cfg(test)]
mod tests {
    /*
    use super::*;

    // Tests disabled during refactoring
    */
}
