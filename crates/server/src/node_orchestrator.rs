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
    Arc,
    atomic::{AtomicUsize, Ordering as AtomicOrdering},
};

use anyhow::Result;
use kameo::prelude::*;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{error, info, warn};
use uuid::Uuid;

use cluster::{ConsistentRing, IdentityError, NodeIdentity, generate_tokens};
use serde_json::Value as JsonValue;
use storage::{FieldDef, HybridStore, IndexSchema, StorageConfig, StoreError, WalOp};

/// Configuration for a CameoDB node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    /// Base path for all node data storage
    pub storage_path: PathBuf,
    /// Maximum number of shards this node can host
    pub max_shards: usize,
    /// Tantivy writer memory configuration (per shard)
    pub writer_memory_min_mb: usize,
    pub writer_memory_max_mb: usize,
    /// Default writer memory per shard in MB (will be clamped to min/max range)
    pub writer_memory_default_mb: usize,
    /// Enable WAL fsync for durability
    pub wal_sync: bool,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            storage_path: PathBuf::from("./data/cameodb"),
            max_shards: 8,
            writer_memory_min_mb: 16,
            writer_memory_max_mb: 256,
            writer_memory_default_mb: 32,
            wal_sync: true,
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

/// Response containing node identity info for actor replies.
#[derive(Debug, Clone, kameo::Reply)]
#[allow(dead_code)] // Fields will be used when RouterActor migrates to ActorRef
pub struct NodeIdentityInfo {
    pub uuid: Uuid,
    pub name: String,
}

/// Microshard actor that manages a single shard's storage and search operations.
#[derive(Clone, Actor)]
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
    ) -> Result<Vec<(f32, JsonValue)>, OrchestratorError> {
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
        let results =
            tokio::task::spawn_blocking(move || store.search_documents(&index, &query, limit))
                .await
                .map_err(|e| OrchestratorError::Io(std::io::Error::other(e)))?
                .map_err(|e: StoreError| match e {
                    StoreError::Io(io_err) => OrchestratorError::Io(io_err),
                    _ => OrchestratorError::Io(std::io::Error::other(e.to_string())),
                })?;

        Ok(results)
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
                Ok(results) => {
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
}

/// Validates and evolves schema for a document.
///
/// This function implements schema validation and evolution logic:
/// 1. Ensures the document has an "id" field (mandatory)
/// 2. Checks type compatibility for existing fields
/// 3. Adds new fields to the schema (append-only evolution)
/// 4. Persists schema updates to storage
///
/// # Arguments
///
/// * `index` - The index name
/// * `doc` - The document to validate
/// * `schema_cache` - Mutable reference to the cached schema
/// * `store` - Reference to the storage engine for persistence
///
/// # Returns
///
/// `Ok(())` if validation passes, `Err` if validation fails
async fn validate_and_evolve_schema(
    index: &str,
    doc: &JsonValue,
    schema_cache: &mut IndexSchema,
    store: &Arc<HybridStore>,
) -> Result<(), OrchestratorError> {
    // Check 1 (Mandatory): Ensure doc["id"] exists
    if !doc.is_object() || !doc.as_object().unwrap().contains_key("id") {
        return Err(OrchestratorError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Document must contain an 'id' field",
        )));
    }

    let mut schema_updated = false;

    // Check 2 (Evolution): Iterate keys in doc
    if let Some(obj) = doc.as_object() {
        for (key, value) in obj {
            let inferred_type = match value {
                JsonValue::String(_) => "text",
                JsonValue::Number(_) => "number",
                JsonValue::Bool(_) => "boolean",
                JsonValue::Array(_) => "array",
                JsonValue::Object(_) => "object",
                JsonValue::Null => "null",
            };

            if let Some(existing_field) = schema_cache.fields.get(key) {
                // Check type compatibility
                if existing_field.field_type != inferred_type {
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
                let new_field = FieldDef {
                    name: key.clone(),
                    field_type: inferred_type.to_string(),
                    indexed: matches!(inferred_type, "text" | "string"),
                };
                schema_cache.fields.insert(key.clone(), new_field);
                schema_updated = true;
            }
        }
    }

    // Persist updated schema to storage if changed
    if schema_updated {
        let store_clone = Arc::clone(store);
        let schema_clone = schema_cache.clone();
        let index_name = index.to_string();

        tokio::task::spawn_blocking(move || store_clone.store_schema(&index_name, &schema_clone))
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

        // TODO: Broadcast SchemaUpdate to cluster
        info!("Schema updated for index '{}' with new fields", index);
    }

    Ok(())
}

// ============================================================================
// Remote Message Implementations for Distributed Actors
// ============================================================================

/// Message implementation for MicroshardActor search operations
impl Message<SearchRequest> for MicroshardActor {
    type Reply = Result<Vec<(f32, JsonValue)>, OrchestratorError>;

    async fn handle(
        &mut self,
        msg: SearchRequest,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.handle_search(msg).await
    }
}

/// Message implementation for MicroshardActor write operations
impl Message<WriteRequest> for MicroshardActor {
    type Reply = Result<u64, OrchestratorError>;

    async fn handle(
        &mut self,
        msg: WriteRequest,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.handle_write(msg).await
    }
}

/// Message implementation for MicroshardActor batch write operations
impl Message<BatchWriteRequest> for MicroshardActor {
    type Reply = Result<JsonValue, OrchestratorError>;

    async fn handle(
        &mut self,
        msg: BatchWriteRequest,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        // Convert Vec<u64> to JsonValue to match expected return type
        match self.handle_batch_write(msg).await {
            Ok(sequence_ids) => Ok(serde_json::json!({
                "sequence_ids": sequence_ids,
                "items_processed": sequence_ids.len()
            })),
            Err(e) => Err(e),
        }
    }
}

/// Router actor that forwards client operations to NodeOrchestrator via actor messaging.
/// Uses actor messaging instead of Arc<RwLock> - no locks needed.
#[cfg_attr(not(test), allow(dead_code))]
#[derive(Clone, Actor)]
pub struct RouterActor {
    orchestrator: ActorRef<NodeOrchestrator>,
}

impl RouterActor {
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn new(orchestrator: ActorRef<NodeOrchestrator>) -> Self {
        Self { orchestrator }
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
}

#[derive(Debug, Actor)]
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
}

impl NodeOrchestrator {
    /// Creates a new NodeOrchestrator with the given configuration.
    pub async fn new(config: NodeConfig) -> Result<Self, OrchestratorError> {
        // Ensure storage directory exists
        fs::create_dir_all(&config.storage_path)?;

        // Load or create node identity
        let identity_path = config.storage_path.join("node_identity.json");
        let identity = NodeIdentity::load_or_create(identity_path)?;

        info!("Node identity: {} ({})", identity.name, identity.uuid);

        let mut orchestrator = Self {
            shards: HashMap::new(),
            identity,
            config,
            routing_ring: ConsistentRing::new(),
            round_robin_counter: AtomicUsize::new(0),
        };

        // Discover and hydrate existing shards
        orchestrator.hydrate_existing_shards().await?;

        Ok(orchestrator)
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

        // Use default writer memory, clamped to configured min/max range
        let writer_memory_mb = std::cmp::min(
            std::cmp::max(
                self.config.writer_memory_min_mb,
                self.config.writer_memory_default_mb,
            ),
            self.config.writer_memory_max_mb,
        );

        StorageConfig {
            shard_path,
            writer_memory_budget: writer_memory_mb * 1024 * 1024, // Convert to bytes
            writer_memory_min_mb: self.config.writer_memory_min_mb,
            writer_memory_max_mb: self.config.writer_memory_max_mb,
            default_batch_size: 1000, // Use default from config or hard-coded for now
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

        info!(
            "Successfully created shard {} ({}/{})",
            shard_id,
            self.shards.len(),
            self.config.max_shards
        );
        Ok(shard_id)
    }

    /// Gets the node identity.
    pub fn identity(&self) -> &NodeIdentity {
        &self.identity
    }

    /// Registers a shard with the routing ring for consistent hashing.
    fn register_shard_for_routing(&mut self, shard_id: Uuid) {
        let simple = shard_id.simple().to_string();
        let name: String = simple.chars().take(3).collect();
        let identity = NodeIdentity {
            uuid: shard_id,
            name,
            vnode_tokens: generate_tokens(shard_id),
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
            } => self.orch_search(&index, &query, limit.unwrap_or(10)).await,
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
            ClientOp::ListIndexes => self.orch_list_indexes().await,
        }
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
        if let Some(shard) = self.shards.values().next() {
            if let Some(store) = &shard.store {
                validate_and_evolve_schema(index, &doc, &mut schema_cache, store).await?;
            }
        }
        let target = self.route_write(&routing_key)?;
        let shard = self.shards.get(&target).ok_or_else(|| {
            OrchestratorError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Shard not found",
            ))
        })?;
        let req = WriteRequest {
            index: index.to_string(),
            routing_key,
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
        let mut batches: HashMap<Uuid, Vec<&DocPayload>> = HashMap::new();
        for doc in &docs {
            let target = self
                .route_write(&doc.routing_key)
                .unwrap_or_else(|_| self.shards.keys().next().copied().unwrap());
            batches.entry(target).or_default().push(doc);
        }
        let mut written = 0usize;
        let mut errors = Vec::new();
        for (shard_id, batch) in batches {
            if let Some(shard) = self.shards.get(&shard_id) {
                let ops: Vec<ClientOp> = batch
                    .iter()
                    .map(|d| ClientOp::Write {
                        index: index.to_string(),
                        id: d.id.clone(),
                        routing_key: d.routing_key.clone(),
                        doc: d.doc.clone(),
                    })
                    .collect();
                match shard.handle_batch_write(BatchWriteRequest { ops }).await {
                    Ok(seq_ids) => written += seq_ids.len(),
                    Err(e) => errors.push(format!("Shard {}: {}", shard_id, e)),
                }
            }
        }
        Ok(
            serde_json::json!({"took_ms": start.elapsed().as_millis(), "items_received": docs.len(), "items_written": written, "errors": errors}),
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
            return Ok(serde_json::json!({"hits": [], "total": 0, "took_ms": 0}));
        }
        let mut handles = Vec::new();
        for (&shard_id, shard) in &self.shards {
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
        let mut results: Vec<(f32, JsonValue)> = Vec::new();
        for h in handles {
            if let Ok((_, Ok(r))) = h.await {
                results.extend(r);
            }
        }
        results.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
        results.truncate(limit);
        let hits: Vec<JsonValue> = results
            .into_iter()
            .map(|(score, mut doc)| {
                if let JsonValue::Object(ref mut o) = doc {
                    o.insert(
                        "_score".to_string(),
                        serde_json::Number::from_f64(score as f64)
                            .map(JsonValue::Number)
                            .unwrap_or(JsonValue::Null),
                    );
                }
                doc
            })
            .collect();
        Ok(
            serde_json::json!({"hits": hits, "total": hits.len(), "took_ms": start.elapsed().as_millis()}),
        )
    }

    async fn orch_create_config(
        &self,
        index: &str,
        schema: IndexSchema,
    ) -> Result<JsonValue, OrchestratorError> {
        if let Some(shard) = self.shards.values().next() {
            if let Some(store) = &shard.store {
                let sc = Arc::clone(store);
                let idx = index.to_string();
                let sch = schema.clone();
                tokio::task::spawn_blocking(move || sc.store_schema(&idx, &sch))
                    .await
                    .map_err(|e| OrchestratorError::Io(std::io::Error::other(e.to_string())))?
                    .map_err(|e| OrchestratorError::Io(std::io::Error::other(e.to_string())))?;
                return Ok(
                    serde_json::json!({"acknowledged": true, "index": index, "shard_count": schema.shard_count}),
                );
            }
        }
        Err(OrchestratorError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "No shards",
        )))
    }

    async fn orch_get_config(&self, index: &str) -> Result<JsonValue, OrchestratorError> {
        if let Some(shard) = self.shards.values().next() {
            if let Some(store) = &shard.store {
                let sc = Arc::clone(store);
                let idx = index.to_string();
                let schema = tokio::task::spawn_blocking(move || sc.get_schema(&idx))
                    .await
                    .map_err(|e| OrchestratorError::Io(std::io::Error::other(e.to_string())))?
                    .map_err(|e| OrchestratorError::Io(std::io::Error::other(e.to_string())))?;
                return match schema {
                    Some(s) => Ok(serde_json::to_value(s).unwrap_or(serde_json::json!({}))),
                    None => Err(OrchestratorError::Io(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        format!("Index '{}' not found", index),
                    ))),
                };
            }
        }
        Err(OrchestratorError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "No shards",
        )))
    }

    async fn orch_list_indexes(&self) -> Result<JsonValue, OrchestratorError> {
        if self.shards.is_empty() {
            return Ok(
                serde_json::json!({"indexes": [], "total_indexes": 0, "node_id": self.identity.uuid.to_string(), "total_shards": 0}),
            );
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
                    for (f, _) in &stat.schema.fields {
                        if !e.2.contains(f) {
                            e.2.push(f.clone());
                        }
                    }
                    e.3 += 1;
                }
            }
        }
        let indexes: Vec<JsonValue> = all.into_iter().map(|(n, (d, s, f, c))| serde_json::json!({"name": n, "document_count": d, "total_size_bytes": s, "size_mb": s/(1024*1024), "shard_count": c, "field_names": f})).collect();
        Ok(
            serde_json::json!({"indexes": indexes, "total_indexes": indexes.len(), "node_id": self.identity.uuid.to_string(), "total_shards": self.shards.len()}),
        )
    }

    /// Helper: Load schema from first shard
    async fn load_schema(&self, index: &str) -> Result<IndexSchema, OrchestratorError> {
        if let Some(shard) = self.shards.values().next() {
            if let Some(store) = &shard.store {
                let sc = Arc::clone(store);
                let idx = index.to_string();
                return tokio::task::spawn_blocking(move || sc.get_schema(&idx))
                    .await
                    .map_err(|e| OrchestratorError::Io(std::io::Error::other(e.to_string())))?
                    .map_err(|e| OrchestratorError::Io(std::io::Error::other(e.to_string())))?
                    .ok_or_else(|| {
                        OrchestratorError::Io(std::io::Error::new(
                            std::io::ErrorKind::NotFound,
                            "Schema not found",
                        ))
                    })
                    .or_else(|_| Ok(IndexSchema::default()));
            }
        }
        Ok(IndexSchema::default())
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

#[cfg(test)]
mod tests {
    /*
    use super::*;

    // Tests disabled during refactoring
    */
}
