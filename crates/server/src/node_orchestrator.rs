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

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::{
    Arc, RwLock,
    atomic::{AtomicU64, AtomicUsize, Ordering as AtomicOrdering},
};
use std::time::{Duration, Instant};

use anyhow::Result;
use kameo::actor::ActorRef;
use kameo::message::{Context, Message};
use kameo::{Actor, RemoteActor, remote_message};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::cluster_coordinator::{
    ClusterCoordinator, GetKnownPeers, GetShardAssignments, OperationType, RegisterLocalShards,
    RequestBootstrapRedial, RouteOperation, RoutingDecision, ShardMetadata,
};
use crate::config::MessagingConfig;
use cluster::{ConsistentRing, IdentityError, NodeIdentity, generate_tokens};
use kameo::actor::RemoteActorRef;
use serde_json::{Map as JsonMap, Value as JsonValue};
use storage::{FieldDef, HybridStore, IndexSchema, StorageConfig, StoreError, WalOp};

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
#[derive(Debug, Error)]
pub enum OrchestratorError {
    #[error("identity error: {0}")]
    Identity(#[from] IdentityError),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

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

/// Message to propose creating a new shard on this node.
#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone)]
pub struct ProposeShard {
    pub shard_id: Uuid,
}

/// Search request message for MicroshardActor.
#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchRequest {
    pub index: String,
    pub query_string: String,
    pub limit: usize,
}

/// Search result with hits and total count
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResult {
    pub hits: Vec<(f32, JsonValue)>,
    pub total_hits: usize,
}

/// Search stream request message for MicroshardActor.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)] // Streaming temporarily stubbed
pub struct SearchStream {
    pub index: String,
    pub query: String,
    pub limit: usize,
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
    pub routing_key: Option<String>,
    pub doc: JsonValue,
}

/// Batch write request message for MicroshardActor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchWriteRequest {
    pub ops: Vec<ClientOp>,
}

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteReply {
    pub sequence_id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchWriteReply {
    pub sequence_ids: Vec<u64>,
    pub items_processed: usize,
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
    Stream { index: String, query: String },
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
    /// List all indexes across the cluster (broadcast)
    ListClusterIndexes,
    /// Delete an index and all its data
    DeleteIndex { index: String },
}

// ============================================================================
// NodeOrchestrator Messages (for future actor-based communication)
// ============================================================================

/// Message to get the current shard count.
#[derive(Debug, Clone)]
pub struct GetShardCount;

/// Message to get the node identity.
#[derive(Debug, Clone)]
pub struct GetIdentity;

/// Message to get all shard IDs.
#[derive(Debug, Clone)]
pub struct GetShardIds;

/// Message to update the global routing topology (consistent ring).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateTopology {
    pub ring: ConsistentRing,
}

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
    pub fn new(shard_id: Uuid, storage_config: StorageConfig) -> Self {
        Self {
            shard_id,
            store: None,
            storage_config,
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
    ) -> Result<SearchResult, OrchestratorError> {
        let store = self.store.as_ref().ok_or_else(|| {
            OrchestratorError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "HybridStore not initialized",
            ))
        })?;

        let store = Arc::clone(store);
        let query = request.query_string;
        let limit = request.limit;

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

        Ok(SearchResult {
            hits: results,
            total_hits,
        })
    }

    /// Handles streaming search requests using channel bridge pattern.
    #[allow(dead_code)] // Streaming temporarily stubbed
    pub async fn handle_search_stream(
        &self,
        request: SearchStream,
    ) -> Result<ReceiverStream<Vec<(f32, JsonValue)>>, OrchestratorError> {
        let store = self.store.as_ref().ok_or_else(|| {
            OrchestratorError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "HybridStore not initialized",
            ))
        })?;

        let store = Arc::clone(store);
        let query = request.query;
        let index = request.index;

        // Create channel for streaming results
        let (tx, rx) = mpsc::channel::<Vec<(f32, JsonValue)>>(100);

        // Spawn blocking task to handle search iteration
        tokio::task::spawn_blocking(move || {
            // For now, we'll simulate streaming by chunking a large search result
            // In a real implementation, this would use tantivy's streaming search capabilities
            match store.search_documents(&index, &query, 1000) {
                // Get more results for chunking
                Ok((results, _total_hits)) => {
                    // Send results in chunks of 50
                    const CHUNK_SIZE: usize = 50;
                    for chunk in results.chunks(CHUNK_SIZE) {
                        if tx.blocking_send(chunk.to_vec()).is_err() {
                            break; // Receiver dropped
                        }
                    }
                }
                Err(e) => {
                    warn!("Search stream error: {}", e);
                    // Send empty chunk to indicate error/completion
                    let _ = tx.blocking_send(vec![]);
                }
            }
        });

        Ok(ReceiverStream::new(rx))
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

        // Map document to WalOp::Put with proper body and json_blob mapping
        let (body, json_blob) = match &doc {
            JsonValue::Object(obj) => {
                // Extract body field if present, otherwise use entire doc as body
                let body = obj
                    .get("body")
                    .and_then(|v| v.as_str())
                    .unwrap_or(&doc.to_string())
                    .to_string();

                // Store entire doc as json_blob for structured queries
                (body, Some(doc.clone()))
            }
            JsonValue::String(s) => {
                // If doc is just a string, use it as body
                (s.clone(), None)
            }
            _ => {
                // For other types, convert to string for body and store as json_blob
                (doc.to_string(), Some(doc.clone()))
            }
        };

        let op = WalOp::Put {
            id,
            body,
            json_blob,
        };

        // Use spawn_blocking to execute write on blocking thread pool
        let index = request.index.clone();
        let seq_id = tokio::task::spawn_blocking(move || store.apply_write(&index, op))
            .await
            .map_err(|e| OrchestratorError::Io(std::io::Error::other(e)))?
            .map_err(|e: StoreError| match e {
                StoreError::Io(io_err) => OrchestratorError::Io(io_err),
                _ => OrchestratorError::Io(std::io::Error::other(e.to_string())),
            })?;

        Ok(seq_id)
    }

    /// Handles batch write requests with spawn_blocking to avoid blocking the actor thread.
    pub async fn handle_batch_write(
        &self,
        request: BatchWriteRequest,
    ) -> Result<Vec<u64>, OrchestratorError> {
        let store = self.store.as_ref().ok_or_else(|| {
            OrchestratorError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "HybridStore not initialized",
            ))
        })?;

        let store = Arc::clone(store);
        let ops = request.ops;

        // Group operations by index
        let mut ops_by_index: std::collections::HashMap<String, Vec<WalOp>> =
            std::collections::HashMap::new();

        for op in ops {
            match op {
                ClientOp::Write { index, id, doc, .. } => {
                    // Map document to WalOp::Put with proper body and json_blob mapping
                    let (body, json_blob) = match &doc {
                        JsonValue::Object(obj) => {
                            // Extract body field if present, otherwise use entire doc as body
                            let body = obj
                                .get("body")
                                .and_then(|v| v.as_str())
                                .unwrap_or(&doc.to_string())
                                .to_string();

                            // Store entire doc as json_blob for structured queries
                            (body, Some(doc.clone()))
                        }
                        JsonValue::String(s) => {
                            // If doc is just a string, use it as body
                            (s.clone(), None)
                        }
                        _ => {
                            // For other types, convert to string for body and store as json_blob
                            (doc.to_string(), Some(doc.clone()))
                        }
                    };

                    let wal_op = WalOp::Put {
                        id,
                        body,
                        json_blob,
                    };

                    ops_by_index.entry(index).or_default().push(wal_op);
                }
                _ => {
                    // For now, only support Write operations in batch
                    return Err(OrchestratorError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "Only Write operations are supported in batch requests",
                    )));
                }
            }
        }

        // Use spawn_blocking to execute batch write on blocking thread pool
        let all_seq_ids = tokio::task::spawn_blocking(move || {
            let mut all_results = Vec::new();
            for (index, wal_ops) in ops_by_index {
                let seq_ids = store.apply_batch(&index, wal_ops)?;
                all_results.extend(seq_ids);
            }
            Ok::<Vec<u64>, StoreError>(all_results)
        })
        .await
        .map_err(|e| OrchestratorError::Io(std::io::Error::other(e)))?
        .map_err(|e: StoreError| match e {
            StoreError::Io(io_err) => OrchestratorError::Io(io_err),
            _ => OrchestratorError::Io(std::io::Error::other(e.to_string())),
        })?;

        Ok(all_seq_ids)
    }

    /// Deletes all data for an index from this shard's storage
    pub async fn delete_index(&self, index: &str) -> Result<(), OrchestratorError> {
        let store = self.store.as_ref().ok_or_else(|| {
            OrchestratorError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "HybridStore not initialized",
            ))
        })?;

        let store = Arc::clone(store);
        let index = index.to_string();

        // Use spawn_blocking to execute delete on blocking thread pool
        tokio::task::spawn_blocking(move || store.delete_index_data(&index))
            .await
            .map_err(|e| OrchestratorError::Io(std::io::Error::other(e)))?
            .map_err(|e: StoreError| match e {
                StoreError::Io(io_err) => OrchestratorError::Io(io_err),
                _ => OrchestratorError::Io(std::io::Error::other(e.to_string())),
            })?;

        Ok(())
    }
}

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
                "text"
            } else {
                match value {
                    JsonValue::String(s) => {
                        // Try to infer date from string
                        if chrono::DateTime::parse_from_rfc3339(s).is_ok() {
                            "date"
                        } else {
                            "text"
                        }
                    }
                    JsonValue::Number(_) => "f64",
                    JsonValue::Bool(_) => "boolean",
                    JsonValue::Array(_) => "array",
                    JsonValue::Object(_) => "object",
                    JsonValue::Null => "null",
                }
            };

            if let Some(existing_field) = schema_cache.fields.get(key) {
                // Check type compatibility
                // 1. Exact match is always allowed
                let mut is_compatible = existing_field.field_type == inferred_type;

                // 2. Allow "text" (inferred) to match "exact" (schema)
                if !is_compatible && inferred_type == "text" {
                    if existing_field.field_type == "exact" {
                        is_compatible = true;
                    }
                }

                // 3. Allow "array" (inferred) to match "text", "exact" (schema)
                // Tantivy supports multi-valued fields for all text/string types
                if !is_compatible && inferred_type == "array" {
                    if existing_field.field_type == "text" || existing_field.field_type == "exact" {
                        is_compatible = true;
                    }
                }

                if !is_compatible {
                    return Err(OrchestratorError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!(
                            "Type mismatch for field '{}': expected '{}', got '{}'",
                            key, existing_field.field_type, inferred_type
                        ),
                    )));
                }
            } else {
                // New field: Update schema_cache (Append-Only)
                // Mark new fields indexed by default so they become searchable on arrival
                let new_field = FieldDef {
                    name: key.clone(),
                    field_type: inferred_type.to_string(),
                    indexed: true,
                };
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
                hits: result
                    .hits
                    .into_iter()
                    .map(|(score, doc)| SearchHit { score, doc })
                    .collect(),
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
            .map(|sequence_id| WriteReply { sequence_id })
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
                items_processed: sequence_ids.len(),
                sequence_ids,
            })
            .map_err(RemoteError::from)
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
}

impl RouterActor {
    #[allow(dead_code)]
    pub fn new(
        orchestrator: ActorRef<NodeOrchestrator>,
        coordinator: ActorRef<ClusterCoordinator>,
    ) -> Self {
        Self::with_config(orchestrator, coordinator, &MessagingConfig::default(), 10)
    }

    pub fn with_config(
        orchestrator: ActorRef<NodeOrchestrator>,
        coordinator: ActorRef<ClusterCoordinator>,
        messaging: &MessagingConfig,
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
            ClientOp::GetConfig { .. } | ClientOp::CreateConfig { .. } | ClientOp::ListIndexes
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
            Ok(RoutingDecision::Broadcast) => self.handle_broadcast(op).await,
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
    #[cfg_attr(not(test), allow(dead_code))]
    pub async fn get_shard_count(&self) -> Result<usize, OrchestratorError> {
        self.orchestrator.ask(GetShardCount).await.map_err(|e| {
            OrchestratorError::Io(std::io::Error::other(format!("Actor error: {}", e)))
        })
    }

    /// Get the node identity (for health check).
    #[cfg_attr(not(test), allow(dead_code))]
    pub async fn get_identity(&self) -> Result<NodeIdentityInfo, OrchestratorError> {
        self.orchestrator.ask(GetIdentity).await.map_err(|e| {
            OrchestratorError::Io(std::io::Error::other(format!("Actor error: {}", e)))
        })
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
                if let Some(shards) = value.get("shards_responded").and_then(|s| s.as_u64()) {
                    *total_shards_queried += shards as usize;
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
                "total_shards": total_shards_queried,
                "nodes_contacted": nodes_contacted,
                "failed_shards": error_count,
                "took_ms": max_took_ms.unwrap_or_else(|| t_start.elapsed().as_millis() as u64)
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
                    "total_shards": total_shards_queried,
                    "nodes_contacted": all_results.len(),
                    "failed_shards": error_count
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
            ClientOp::ListClusterIndexes => {
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
                                    if let Some(field_str) = field.as_str() {
                                        if !entry.field_names.contains(&field_str.to_string()) {
                                            entry.field_names.push(field_str.to_string());
                                        }
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
                let cluster_indexes: Vec<JsonValue> = index_map
                    .into_values()
                    .map(|stats| {
                        serde_json::json!({
                            "name": stats.name,
                            "document_count": stats.document_count,
                            "total_size_bytes": stats.total_size_bytes,
                            "size_mb": stats.total_size_bytes / (1024 * 1024),
                            "shard_count": stats.shard_count,
                            "field_names": stats.field_names,
                        })
                    })
                    .collect();

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
        op: ClientOp,
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

        match remote_ref {
            Some(remote) => {
                info!("✅ Remote actor found: {}", orchestrator_name);
                let result = remote.ask(&op).await.map_err(|e| {
                    warn!("❌ Remote actor ask failed: {}", e);
                    OrchestratorError::Io(std::io::Error::other(e.to_string()))
                })?;
                info!("✅ Remote actor responded successfully");
                Ok(result)
            }
            None => {
                warn!(
                    "❌ Remote orchestrator not found: name='{}', node_id={}",
                    orchestrator_name, node_id
                );
                Err(OrchestratorError::Io(std::io::Error::other(format!(
                    "remote orchestrator {} not found",
                    orchestrator_name
                ))))
            }
        }
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
    /// Round-robin counter for writes without routing key
    round_robin_counter: AtomicUsize,
    /// Optional coordinator reference for shard registration
    coordinator: Option<ActorRef<ClusterCoordinator>>,
    /// Per-index schema cache to avoid repeated metadata reads
    schema_cache: RwLock<HashMap<String, IndexSchema>>,
    /// Default search result limit when not specified in request
    default_search_limit: usize,
}

impl NodeOrchestrator {
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
            "🔎 Forwarding bulk batch to remote orchestrator: name='{}', node_id={}, addr={}",
            orchestrator_name, node_id, peer_addr
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
                "remote orchestrator {} not found",
                orchestrator_name
            )))
        })?;

        let op = ClientOp::BulkWrite {
            index: index.to_string(),
            docs,
        };

        let result = remote.ask(&op).await.map_err(|e| {
            warn!("❌ Remote actor ask failed: {}", e);
            OrchestratorError::Io(std::io::Error::other(e.to_string()))
        })?;

        let items_written = result
            .get("items_written")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as usize;
        Ok(items_written)
    }

    /// Fetch a schema from cache if present.
    fn get_cached_schema(&self, index: &str) -> Option<IndexSchema> {
        self.schema_cache
            .read()
            .ok()
            .and_then(|map| map.get(index).cloned())
    }

    /// Insert or replace a schema in the cache.
    fn put_cached_schema(&self, index: &str, schema: &IndexSchema) {
        if let Ok(mut map) = self.schema_cache.write() {
            map.insert(index.to_string(), schema.clone());
        }
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
            round_robin_counter: AtomicUsize::new(0),
            coordinator: None,
            schema_cache: RwLock::new(HashMap::new()),
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

    /// Scans the storage directory for existing shard folders and hydrates them.
    async fn hydrate_existing_shards(&mut self) -> Result<(), OrchestratorError> {
        let existing_shards = self.discover_existing_shards()?;
        info!("Found {} existing shards", existing_shards.len());

        for shard_id in existing_shards {
            if self.shards.len() >= self.config.max_shards {
                warn!("Shard limit reached, skipping shard {}", shard_id);
                break;
            }

            let storage_config = self.create_shard_storage_config(shard_id);
            let mut microshard = MicroshardActor::new(shard_id, storage_config);

            match microshard.start().await {
                Ok(()) => {
                    self.shards.insert(shard_id, microshard);
                    self.register_shard_for_routing(shard_id);
                    if let Err(err) = self.register_shard_with_coordinator(shard_id).await {
                        warn!(%shard_id, error = %err, "Failed to register hydrated shard");
                    }
                    info!("Hydrated shard {}", shard_id);
                }
                Err(e) => {
                    error!("Failed to hydrate shard {}: {}", shard_id, e);
                }
            }
        }

        info!(
            "NodeOrchestrator startup complete with {} active shards",
            self.shards.len()
        );
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
        let mut microshard = MicroshardActor::new(shard_id, storage_config);
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
                coordinator
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

    /// Selects a shard using round-robin distribution.
    fn select_shard_round_robin(&self) -> Option<Uuid> {
        if self.shards.is_empty() {
            return None;
        }

        let shard_ids: Vec<Uuid> = self.shards.keys().copied().collect();
        let index = self
            .round_robin_counter
            .fetch_add(1, AtomicOrdering::Relaxed)
            % shard_ids.len();
        Some(shard_ids[index])
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
            ClientOp::Stream { index, query } => Ok(
                serde_json::json!({"message": "Stream initiated", "index": index, "query": query}),
            ),
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
            ClientOp::ListIndexes | ClientOp::ListClusterIndexes => self.orch_list_indexes().await,
            ClientOp::DeleteIndex { index } => self.orch_delete_index(&index).await,
        }
    }

    /// Delete an index and all its data from all local shards
    async fn orch_delete_index(&self, index: &str) -> Result<JsonValue, OrchestratorError> {
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
            match shard.delete_index(index).await {
                Ok(_) => {
                    deleted_from_shards += 1;
                    tracing::info!(
                        shard_id = %shard_id,
                        index = %index,
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
            let mut cache = self.schema_cache.write().unwrap();
            cache.remove(index);
        }

        Ok(serde_json::json!({
            "success": true,
            "index": index,
            "deleted_from_shards": deleted_from_shards,
            "total_shards": self.shards.len(),
            "errors": if errors.is_empty() { None } else { Some(errors) }
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
            self.put_cached_schema(index, &schema_cache);
        }

        // Derive effective routing key:
        // 1) explicit routing_key from payload (if provided)
        // 2) fallback to document id field (doc["id"])
        // 3) fallback to deterministic key derived from document bytes
        let effective_routing_key = routing_key
            .clone()
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
            routing_key: effective_routing_key.clone(),
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

        // Load schema and validate/evolve for all documents before writing
        let mut schema_cache = self.load_schema(index).await?;
        let mut schema_updated = false;

        for doc_payload in &docs {
            let updated = validate_and_evolve_schema(
                index,
                &doc_payload.doc,
                &mut schema_cache,
                &self.shards,
            )
            .await?;
            if updated {
                schema_updated = true;
            }
        }

        // Update cache once after processing all docs
        if schema_updated {
            self.put_cached_schema(index, &schema_cache);
        }

        // Group documents by target shard using the same routing key strategy
        // as single-write: explicit routing_key → doc id → derived key.
        let items_received = docs.len();
        let mut batches: HashMap<Uuid, Vec<(DocPayload, Option<String>)>> = HashMap::new();
        let mut routing_errors = Vec::new();
        for doc in docs {
            let effective_routing_key = doc
                .routing_key
                .clone()
                .or_else(|| derive_routing_key_from_doc(&doc.doc));

            match self.route_write(&effective_routing_key) {
                Ok(target) => {
                    batches
                        .entry(target)
                        .or_default()
                        .push((doc, effective_routing_key));
                }
                Err(err) => {
                    routing_errors.push(format!("routing failed for doc {}: {}", doc.id, err));
                }
            }
        }

        tracing::debug!(
            items_received = items_received,
            unique_shards = batches.len(),
            "BulkWrite grouped items by shard"
        );

        // Fetch shard ownership and peer addresses to forward remote batches.
        let mut shard_assignments = HashMap::new();
        let mut peer_addrs = HashMap::new();
        if let Some(coord) = &self.coordinator {
            shard_assignments = coord.ask(GetShardAssignments).await.unwrap_or_default();
            peer_addrs = coord
                .ask(GetKnownPeers)
                .await
                .unwrap_or_default()
                .into_iter()
                .map(|p| (p.node_id, p.address))
                .collect();
        }

        let mut written = 0usize;
        let mut errors = routing_errors;
        for (shard_id, batch) in batches {
            let owner_node = shard_assignments.get(&shard_id).map(|m| m.node_id);

            match owner_node {
                Some(node_id) if node_id == self.identity.uuid => {
                    if let Some(shard) = self.shards.get(&shard_id) {
                        tracing::debug!(
                            shard_id = %shard_id,
                            count = batch.len(),
                            "Processing bulk write batch for local shard"
                        );
                        let ops: Vec<ClientOp> = batch
                            .into_iter()
                            .map(|(d, effective_routing_key)| ClientOp::Write {
                                index: index.to_string(),
                                id: d.id,
                                routing_key: effective_routing_key,
                                doc: d.doc,
                            })
                            .collect();
                        match shard.handle_batch_write(BatchWriteRequest { ops }).await {
                            Ok(seq_ids) => written += seq_ids.len(),
                            Err(e) => errors.push(format!("Shard {}: {}", shard_id, e)),
                        }
                    } else {
                        errors.push(format!("Local shard {} not found", shard_id));
                    }
                }
                Some(node_id) => {
                    let peer_addr = peer_addrs.get(&node_id).cloned();
                    if let Some(addr) = peer_addr {
                        tracing::debug!(
                            shard_id = %shard_id,
                            owner = %node_id,
                            count = batch.len(),
                            "Forwarding bulk write batch to remote owner"
                        );
                        let docs_for_remote: Vec<DocPayload> = batch
                            .into_iter()
                            .map(|(d, effective_routing_key)| DocPayload {
                                id: d.id,
                                routing_key: effective_routing_key,
                                doc: d.doc,
                            })
                            .collect();
                        match self
                            .forward_bulk_to_remote(node_id, &addr, index, docs_for_remote)
                            .await
                        {
                            Ok(items) => {
                                written += items;
                            }
                            Err(e) => errors.push(format!(
                                "Remote shard {} (node {}) forwarding failed: {}",
                                shard_id, node_id, e
                            )),
                        }
                    } else {
                        errors.push(format!(
                            "No peer address for owner {} of shard {}",
                            node_id, shard_id
                        ));
                    }
                }
                None => {
                    errors.push(format!(
                        "No shard assignment for shard {}; dropping batch",
                        shard_id
                    ));
                }
            }
        }
        Ok(
            serde_json::json!({"took_ms": start.elapsed().as_millis(), "items_received": items_received, "items_written": written, "errors": errors}),
        )
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
                query_string: query.to_string(),
                limit,
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
                    for (score, doc) in r.hits {
                        results.push((shard_id, score, doc));
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

        results.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
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
            "errors": errors,
            "shards_responded": shard_success
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
                FieldDef {
                    name: "id".to_string(),
                    field_type: "text".to_string(),
                    indexed: true,
                },
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
        self.put_cached_schema(index, &schema);

        Ok(serde_json::json!({
            "acknowledged": true,
            "index": index,
            "shard_count": shard_count,
            "field_names": Self::sorted_field_names(&schema)
        }))
    }

    async fn orch_get_config(&self, index: &str) -> Result<JsonValue, OrchestratorError> {
        if let Some(cached) = self.get_cached_schema(index) {
            let field_names = Self::sorted_field_names(&cached);
            let fields = Self::sorted_fields_map(&cached);
            let shard_count = self.default_shard_count();
            return Ok(Self::schema_response(field_names, fields, shard_count));
        }

        // Try each shard until we find a schema; this tolerates cases where the first shard
        // might not yet have the schema materialized locally.
        for shard in self.shards.values() {
            if let Some(store) = &shard.store {
                let sc = Arc::clone(store);
                let idx = index.to_string();
                let schema = tokio::task::spawn_blocking(move || sc.get_schema(&idx))
                    .await
                    .map_err(|e| OrchestratorError::Io(std::io::Error::other(e.to_string())))?
                    .map_err(|e| OrchestratorError::Io(std::io::Error::other(e.to_string())))?;
                if let Some(s) = schema {
                    let field_names = Self::sorted_field_names(&s);
                    let fields = Self::sorted_fields_map(&s);
                    self.put_cached_schema(index, &s);
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

    async fn orch_list_indexes(&self) -> Result<JsonValue, OrchestratorError> {
        if self.shards.is_empty() {
            return Ok(serde_json::json!({
                "indexes": [],
                "total_indexes": 0,
                "node_id": self.identity.uuid.to_string(),
                "node_name": self.identity.name.clone(),
                "total_shards": 0
            }));
        }
        let mut all: HashMap<String, (u64, u64, Vec<String>, usize)> = HashMap::new();
        for shard in self.shards.values() {
            if let Some(store) = &shard.store {
                let sc = Arc::clone(store);
                let stats = tokio::task::spawn_blocking(move || sc.list_indexes())
                    .await
                    .map_err(|e| OrchestratorError::Io(std::io::Error::other(e.to_string())))?
                    .map_err(|e| OrchestratorError::Io(std::io::Error::other(e.to_string())))?;
                for stat in stats {
                    let e = all
                        .entry(stat.name.clone())
                        .or_insert((0, 0, Vec::new(), 0));
                    e.0 += stat.document_count;
                    e.1 += stat.total_size_bytes;
                    for f in stat.schema.fields.keys() {
                        if !e.2.contains(f) {
                            e.2.push(f.clone());
                        }
                    }
                    e.3 += 1;
                }
            }
        }
        let indexes: Vec<JsonValue> = all
            .into_iter()
            .map(|(n, (d, s, mut f, c))| {
                // Sort fields by name, with "id" (if present) always first.
                f.sort_by(|a, b| match (a.as_str(), b.as_str()) {
                    ("id", "id") => std::cmp::Ordering::Equal,
                    ("id", _) => std::cmp::Ordering::Less,
                    (_, "id") => std::cmp::Ordering::Greater,
                    _ => a.cmp(b),
                });

                serde_json::json!({
                    "name": n,
                    "document_count": d,
                    "total_size_bytes": s,
                    "size_mb": s/(1024*1024),
                    "shard_count": c,
                    "field_names": f,
                })
            })
            .collect();
        Ok(serde_json::json!({
            "indexes": indexes,
            "total_indexes": indexes.len(),
            "node_id": self.identity.uuid.to_string(),
            "node_name": self.identity.name.clone(),
            "total_shards": self.shards.len()
        }))
    }

    /// Helper: Load schema from first shard
    async fn load_schema(&self, index: &str) -> Result<IndexSchema, OrchestratorError> {
        if let Some(cached) = self.get_cached_schema(index) {
            return Ok(cached);
        }

        if let Some(shard) = self.shards.values().next() {
            if let Some(store) = &shard.store {
                let sc = Arc::clone(store);
                let idx = index.to_string();
                let schema = tokio::task::spawn_blocking(move || sc.get_schema(&idx))
                    .await
                    .map_err(|e| OrchestratorError::Io(std::io::Error::other(e.to_string())))?
                    .map_err(|e| OrchestratorError::Io(std::io::Error::other(e.to_string())))?;
                if let Some(schema) = schema {
                    self.put_cached_schema(index, &schema);
                    return Ok(schema);
                }
            }
        }
        Ok(IndexSchema {
            shard_count: self.default_shard_count(),
            fields: HashMap::new(),
        })
    }

    /// Helper: Route write to shard
    fn route_write(&self, routing_key: &Option<String>) -> Result<Uuid, OrchestratorError> {
        let target = if let Some(key) = routing_key {
            self.select_shard_for_key(key)
                .or_else(|| self.first_shard_id())
        } else {
            self.select_shard_round_robin()
        };
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
    // Prefer explicit id field in the document body
    if let Some(id_value) = doc.get("id").and_then(|v| v.as_str())
        && !id_value.is_empty()
    {
        return Some(id_value.to_string());
    }

    // Fallback: derive from JSON bytes (deterministic for same document)
    let mut bytes = serde_json::to_vec(doc).ok()?;
    if bytes.is_empty() {
        return None;
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
// NodeOrchestrator Message Handlers (for future actor-based communication)
// ============================================================================

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

impl Message<GetIdentity> for NodeOrchestrator {
    type Reply = NodeIdentityInfo;

    async fn handle(
        &mut self,
        _msg: GetIdentity,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        NodeIdentityInfo {
            uuid: self.identity.uuid,
            name: self.identity.name.clone(),
        }
    }
}

impl Message<GetShardIds> for NodeOrchestrator {
    type Reply = Vec<Uuid>;

    async fn handle(
        &mut self,
        _msg: GetShardIds,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.shards.keys().copied().collect()
    }
}

impl Message<ProposeShard> for NodeOrchestrator {
    type Reply = Result<Uuid, OrchestratorError>;

    async fn handle(
        &mut self,
        msg: ProposeShard,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.handle_propose_shard(msg).await
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
        self.handle_client_op(msg).await
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

#[cfg(test)]
mod tests {
    /*
    use super::*;

    // Tests disabled during refactoring
    */
}
