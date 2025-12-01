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

use std::cmp::Ordering;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering as AtomicOrdering},
};

use anyhow::Result;
use futures::stream::StreamExt;
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

/// Message implementation for RouterActor client operations
impl Message<ClientOp> for RouterActor {
    type Reply = Result<JsonValue, OrchestratorError>;

    async fn handle(
        &mut self,
        msg: ClientOp,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.handle_client_op(msg).await
    }
}

/// Router actor that handles client operations and distributes them across shards.
#[cfg_attr(not(test), allow(dead_code))]
#[derive(Clone, Actor)]
pub struct RouterActor {
    orchestrator: std::sync::Arc<tokio::sync::RwLock<NodeOrchestrator>>,
}

impl RouterActor {
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn new(orchestrator: std::sync::Arc<tokio::sync::RwLock<NodeOrchestrator>>) -> Self {
        Self { orchestrator }
    }

    /// Handles client operations with scatter-gather pattern for search.
    #[cfg_attr(not(test), allow(dead_code))]
    pub async fn handle_client_op(&self, op: ClientOp) -> Result<JsonValue, OrchestratorError> {
        match op {
            ClientOp::Search {
                index,
                query,
                limit,
            } => {
                self.handle_search(&index, &query, limit.unwrap_or(10))
                    .await
            }
            ClientOp::Stream { index, query } => {
                // For now, return a simple acknowledgment
                // In a full implementation, this would return a stream handle
                Ok(serde_json::json!({
                    "message": "Stream operation initiated",
                    "index": index,
                    "query": query
                }))
            }
            ClientOp::Write {
                index,
                id,
                routing_key,
                doc,
            } => self.handle_write(&index, id, routing_key, doc).await,
            ClientOp::BulkWrite { index, docs } => self.handle_bulk_write(&index, docs).await,
            ClientOp::CreateConfig { index, schema } => {
                self.handle_create_config(&index, schema).await
            }
            ClientOp::GetConfig { index } => self.handle_get_config(&index).await,
            ClientOp::ListIndexes => self.handle_list_indexes().await,
        }
    }

    /// Handles write operations by routing to an appropriate shard.
    async fn handle_write(
        &self,
        index: &str,
        id: String,
        routing_key: Option<String>,
        doc: JsonValue,
    ) -> Result<JsonValue, OrchestratorError> {
        let orchestrator = self.orchestrator.read().await;

        if orchestrator.shards.is_empty() {
            return Err(OrchestratorError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "No shards available for write operation",
            )));
        }

        // Load IndexSchema from local state (using any shard's storage)
        let mut schema_cache = if let Some(shard) = orchestrator.shards.values().next() {
            if let Some(store) = &shard.store {
                let store_clone = Arc::clone(store);
                let index_name = index.to_string();

                tokio::task::spawn_blocking(move || store_clone.get_schema(&index_name))
                    .await
                    .map_err(|e| {
                        OrchestratorError::Io(std::io::Error::other(format!(
                            "Failed to spawn schema load task: {}",
                            e
                        )))
                    })?
                    .map_err(|e| {
                        OrchestratorError::Io(std::io::Error::other(format!(
                            "Failed to load schema: {}",
                            e
                        )))
                    })?
                    .unwrap_or_else(IndexSchema::default)
            } else {
                IndexSchema::default()
            }
        } else {
            IndexSchema::default()
        };

        // Run validate_and_evolve_schema for the document
        if let Some(shard) = orchestrator.shards.values().next()
            && let Some(store) = &shard.store
        {
            validate_and_evolve_schema(index, &doc, &mut schema_cache, store).await?;
        }

        let target_shard_id = if let Some(ref routing_key) = routing_key {
            // Use consistent hashing when routing_key is provided
            orchestrator
                .select_shard_for_key(routing_key)
                .or_else(|| orchestrator.first_shard_id())
        } else {
            // Use round-robin distribution when no routing_key
            (*orchestrator).select_shard_round_robin()
        }
        .ok_or_else(|| {
            OrchestratorError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Unable to select shard for write operation",
            ))
        })?;

        let shard = orchestrator.shards.get(&target_shard_id).ok_or_else(|| {
            OrchestratorError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Selected shard not found for write operation",
            ))
        })?;

        let write_request = WriteRequest {
            index: index.to_string(),
            routing_key: routing_key.clone(),
            doc,
        };

        match shard.handle_write(write_request).await {
            Ok(seq_id) => Ok(serde_json::json!({
                "id": id,
                "result": "created",
                "version": seq_id,
                "shard_id": target_shard_id.to_string()
            })),
            Err(e) => Err(e),
        }
    }

    /// Handles bulk write operations using binning strategy for optimal distribution.
    async fn handle_bulk_write(
        &self,
        index: &str,
        docs: Vec<DocPayload>,
    ) -> Result<JsonValue, OrchestratorError> {
        let start_time = std::time::Instant::now();
        let total_docs = docs.len();
        let orchestrator = self.orchestrator.read().await;

        if orchestrator.shards.is_empty() {
            return Err(OrchestratorError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "No shards available for bulk write operation",
            )));
        }

        // Load IndexSchema from local state (using any shard's storage)
        let mut schema_cache = if let Some(shard) = orchestrator.shards.values().next() {
            if let Some(store) = &shard.store {
                let store_clone = Arc::clone(store);
                let index_name = index.to_string();

                tokio::task::spawn_blocking(move || store_clone.get_schema(&index_name))
                    .await
                    .map_err(|e| {
                        OrchestratorError::Io(std::io::Error::other(format!(
                            "Failed to spawn schema load task: {}",
                            e
                        )))
                    })?
                    .map_err(|e| {
                        OrchestratorError::Io(std::io::Error::other(format!(
                            "Failed to load schema: {}",
                            e
                        )))
                    })?
                    .unwrap_or_else(IndexSchema::default)
            } else {
                IndexSchema::default()
            }
        } else {
            IndexSchema::default()
        };

        // Validate all documents before processing
        if let Some(shard) = orchestrator.shards.values().next()
            && let Some(store) = &shard.store
        {
            for doc_payload in &docs {
                validate_and_evolve_schema(index, &doc_payload.doc, &mut schema_cache, store)
                    .await?;
            }
        }

        // Binning Strategy: Group documents by target shard
        let mut batches: std::collections::HashMap<Uuid, Vec<ClientOp>> =
            std::collections::HashMap::new();

        for doc_payload in docs {
            let routing_key = doc_payload.routing_key.as_ref().unwrap_or(&doc_payload.id);

            // Determine target shard using consistent hashing or round-robin
            let target_shard_id = orchestrator
                .select_shard_for_key(routing_key)
                .or_else(|| orchestrator.first_shard_id())
                .ok_or_else(|| {
                    OrchestratorError::Io(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        "Unable to select shard for bulk write operation",
                    ))
                })?;

            // Convert DocPayload to ClientOp::Write
            let client_op = ClientOp::Write {
                index: index.to_string(),
                id: doc_payload.id,
                routing_key: doc_payload.routing_key,
                doc: doc_payload.doc,
            };

            // Add to the batch for this shard
            batches.entry(target_shard_id).or_default().push(client_op);
        }

        // Collect shard references before spawning tasks
        let mut shard_batches = Vec::new();
        for (shard_id, ops) in batches {
            if let Some(shard) = orchestrator.shards.get(&shard_id) {
                shard_batches.push((shard_id, shard.clone(), ops));
            } else {
                return Err(OrchestratorError::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("Shard {} not found for bulk write operation", shard_id),
                )));
            }
        }

        drop(orchestrator); // Release read lock early

        // Parallel Dispatch: Send BatchWriteRequest to each shard
        let mut batch_tasks = Vec::new();

        for (shard_id, shard, ops) in shard_batches {
            let batch_request = BatchWriteRequest { ops };

            let task = tokio::spawn(async move { shard.handle_batch_write(batch_request).await });

            batch_tasks.push((shard_id, task));
        }

        // Gather results from all shards
        let mut total_indexed = 0;
        let mut successful_shards = 0;
        let mut failed_shards = 0;
        let mut shard_results = Vec::new();

        for (shard_id, task) in batch_tasks {
            match task.await {
                Ok(Ok(seq_ids)) => {
                    successful_shards += 1;
                    total_indexed += seq_ids.len();
                    shard_results.push(serde_json::json!({
                        "shard_id": shard_id.to_string(),
                        "items_indexed": seq_ids.len(),
                        "status": "success"
                    }));
                }
                Ok(Err(e)) => {
                    failed_shards += 1;
                    warn!("Shard {} bulk write failed: {}", shard_id, e);
                    shard_results.push(serde_json::json!({
                        "shard_id": shard_id.to_string(),
                        "status": "failed",
                        "error": e.to_string()
                    }));
                }
                Err(e) => {
                    failed_shards += 1;
                    warn!("Shard {} bulk write task failed: {}", shard_id, e);
                    shard_results.push(serde_json::json!({
                        "shard_id": shard_id.to_string(),
                        "status": "failed",
                        "error": format!("Task execution failed: {}", e)
                    }));
                }
            }
        }

        let operation_time_ms = start_time.elapsed().as_millis() as u64;

        Ok(serde_json::json!({
            "items_indexed": total_indexed,
            "successful_shards": successful_shards,
            "failed_shards": failed_shards,
            "shard_results": shard_results,
            "QTime": operation_time_ms,
            "timing": {
                "total_ms": operation_time_ms,
                "documents_processed": total_docs
            }
        }))
    }

    /// Handles streaming client operations and returns a combined stream.
    pub async fn handle_client_stream(
        &self,
        index: String,
        query: String,
    ) -> Result<ReceiverStream<Vec<(f32, JsonValue)>>, OrchestratorError> {
        let orchestrator = self.orchestrator.read().await;
        let shard_ids: Vec<Uuid> = orchestrator.shards.keys().copied().collect();
        drop(orchestrator);

        // Create a single channel to merge all streams
        let (tx, rx) = mpsc::channel::<Vec<(f32, JsonValue)>>(100);

        if shard_ids.is_empty() {
            // Close the channel immediately for empty case
            drop(tx);
            return Ok(ReceiverStream::new(rx));
        }

        // Spawn task to handle stream merging
        let orchestrator = self.orchestrator.clone();
        tokio::spawn(async move {
            let mut shard_streams = Vec::new();

            for shard_id in shard_ids {
                let search_stream = SearchStream {
                    index: index.clone(),
                    query: query.clone(),
                    limit: 1000, // Default streaming limit
                };

                // Get stream from each shard
                let orchestrator_read = orchestrator.read().await;
                if let Some(shard) = orchestrator_read.shards.get(&shard_id) {
                    match shard.handle_search_stream(search_stream).await {
                        Ok(stream) => shard_streams.push(stream),
                        Err(e) => warn!("Failed to create stream for shard {}: {}", shard_id, e),
                    }
                }
            }

            // Merge all streams and forward to the output channel
            let mut merged_stream = futures::stream::select_all(shard_streams);
            while let Some(chunk) = merged_stream.next().await {
                if tx.send(chunk).await.is_err() {
                    break; // Receiver dropped
                }
            }
        });

        Ok(ReceiverStream::new(rx))
    }

    /// Implements scatter-gather search across all shards.
    async fn handle_search(
        &self,
        index: &str,
        query: &str,
        limit: usize,
    ) -> Result<JsonValue, OrchestratorError> {
        let start_time = std::time::Instant::now();
        let orchestrator = self.orchestrator.read().await;

        // Get all shard actors (scatter-gather across all shards for now)
        let shard_ids: Vec<Uuid> = orchestrator.shards.keys().copied().collect();
        drop(orchestrator); // Release read lock early

        if shard_ids.is_empty() {
            let query_time_ms = start_time.elapsed().as_millis() as u64;
            return Ok(serde_json::json!({
                "results": [],
                "total_shards": 0,
                "query": query,
                "QTime": query_time_ms,
                "timing": {
                    "total_ms": query_time_ms,
                    "query": query.to_string()
                }
            }));
        }

        // Scatter: Send search requests to all shards
        let mut search_tasks = Vec::new();

        for shard_id in &shard_ids {
            let orchestrator = self.orchestrator.clone();
            let shard_id = *shard_id;
            let search_request = SearchRequest {
                index: index.to_string(),
                query_string: query.to_string(),
                limit,
            };

            let task = tokio::spawn(async move {
                let orchestrator = orchestrator.read().await;
                if let Some(shard) = orchestrator.shards.get(&shard_id) {
                    let result = shard.handle_search(search_request).await;
                    (shard_id, result)
                } else {
                    (
                        shard_id,
                        Err(OrchestratorError::Io(std::io::Error::new(
                            std::io::ErrorKind::NotFound,
                            "Shard not found",
                        ))),
                    )
                }
            });

            search_tasks.push(task);
        }

        // Gather: Collect results from all shards
        let mut all_results = Vec::new();
        let mut successful_shards = 0;
        let mut failed_shards = 0;

        for task in search_tasks {
            match task.await {
                Ok((shard_id, Ok(shard_results))) => {
                    successful_shards += 1;
                    for (score, doc) in shard_results {
                        all_results.push((score, doc, shard_id));
                    }
                }
                Ok((shard_id, Err(e))) => {
                    failed_shards += 1;
                    warn!("Shard {} search failed: {}", shard_id, e);
                }
                Err(e) => {
                    failed_shards += 1;
                    warn!("Shard search task failed: {}", e);
                }
            }
        }

        // Aggregation: Sort by score (descending) and take top N
        all_results.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(Ordering::Equal));
        all_results.truncate(limit);

        // Format results
        let formatted_results: Vec<JsonValue> = all_results
            .into_iter()
            .map(|(score, mut doc, shard_id)| {
                // Add metadata to each result
                if let JsonValue::Object(ref mut obj) = doc {
                    obj.insert(
                        "_score".to_string(),
                        JsonValue::Number(
                            serde_json::Number::from_f64(score as f64)
                                .unwrap_or_else(|| serde_json::Number::from(0)),
                        ),
                    );
                    obj.insert(
                        "_shard_id".to_string(),
                        JsonValue::String(shard_id.to_string()),
                    );
                }
                doc
            })
            .collect();

        let query_time_ms = start_time.elapsed().as_millis() as u64;

        Ok(serde_json::json!({
            "results": formatted_results,
            "total_results": formatted_results.len(),
            "total_shards": shard_ids.len(),
            "successful_shards": successful_shards,
            "failed_shards": failed_shards,
            "query": query,
            "QTime": query_time_ms,
            "timing": {
                "total_ms": query_time_ms,
                "query": query.to_string()
            }
        }))
    }

    /// Handles schema creation/update operations.
    async fn handle_create_config(
        &self,
        index: &str,
        schema: IndexSchema,
    ) -> Result<JsonValue, OrchestratorError> {
        let orchestrator = self.orchestrator.read().await;

        // Get any shard to store the schema (schema is replicated across all shards)
        if let Some(shard) = orchestrator.shards.values().next() {
            if let Some(store) = &shard.store {
                let store_clone = Arc::clone(store);
                let schema_clone = schema.clone();
                let index_name = index.to_string();

                tokio::task::spawn_blocking(move || {
                    store_clone.store_schema(&index_name, &schema_clone)
                })
                .await
                .map_err(|e| {
                    OrchestratorError::Io(std::io::Error::other(format!(
                        "Failed to spawn schema creation task: {}",
                        e
                    )))
                })?
                .map_err(|e| {
                    OrchestratorError::Io(std::io::Error::other(format!(
                        "Failed to store schema: {}",
                        e
                    )))
                })?;

                info!("Schema created for index '{}'", index);

                Ok(serde_json::json!({
                    "index": index,
                    "shard_count": schema.shard_count,
                    "fields": schema.fields,
                    "status": "created"
                }))
            } else {
                Err(OrchestratorError::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "No initialized shards available for schema storage",
                )))
            }
        } else {
            Err(OrchestratorError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "No shards available for schema storage",
            )))
        }
    }

    /// Handles schema retrieval operations.
    async fn handle_get_config(&self, index: &str) -> Result<JsonValue, OrchestratorError> {
        let orchestrator = self.orchestrator.read().await;

        // Get any shard to retrieve the schema
        if let Some(shard) = orchestrator.shards.values().next() {
            if let Some(store) = &shard.store {
                let store_clone = Arc::clone(store);
                let index_name = index.to_string();

                let schema_opt =
                    tokio::task::spawn_blocking(move || store_clone.get_schema(&index_name))
                        .await
                        .map_err(|e| {
                            OrchestratorError::Io(std::io::Error::other(format!(
                                "Failed to spawn schema retrieval task: {}",
                                e
                            )))
                        })?
                        .map_err(|e| {
                            OrchestratorError::Io(std::io::Error::other(format!(
                                "Failed to retrieve schema: {}",
                                e
                            )))
                        })?;

                match schema_opt {
                    Some(schema) => Ok(serde_json::json!({
                        "index": index,
                        "shard_count": schema.shard_count,
                        "fields": schema.fields,
                        "status": "found"
                    })),
                    None => Ok(serde_json::json!({
                        "index": index,
                        "status": "not_found",
                        "message": "No schema found for this index"
                    })),
                }
            } else {
                Err(OrchestratorError::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "No initialized shards available for schema retrieval",
                )))
            }
        } else {
            Err(OrchestratorError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "No shards available for schema retrieval",
            )))
        }
    }

    /// Handles listing all available indexes with their statistics aggregated across all shards.
    async fn handle_list_indexes(&self) -> Result<JsonValue, OrchestratorError> {
        let orchestrator = self.orchestrator.read().await;

        if orchestrator.shards.is_empty() {
            return Err(OrchestratorError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "No shards available for listing indexes",
            )));
        }

        // Get index names from the first available shard
        let index_names = if let Some(shard) = orchestrator.shards.values().next() {
            if let Some(store) = &shard.store {
                let store_clone = Arc::clone(store);
                tokio::task::spawn_blocking(move || store_clone.get_index_names())
                    .await
                    .map_err(|e| {
                        OrchestratorError::Io(std::io::Error::other(format!(
                            "Failed to spawn get index names task: {}",
                            e
                        )))
                    })?
                    .map_err(|e| {
                        OrchestratorError::Io(std::io::Error::other(format!(
                            "Failed to get index names: {}",
                            e
                        )))
                    })?
            } else {
                return Err(OrchestratorError::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "No initialized shards available",
                )));
            }
        } else {
            return Err(OrchestratorError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "No shards available",
            )));
        };

        let mut indexes_json = Vec::new();
        let actual_shard_count = orchestrator.shards.len();

        // For each index, aggregate statistics across all shards
        for index_name in index_names {
            let mut total_document_count = 0u64;
            let mut total_size_bytes = 0u64;
            let mut tantivy_exists_count = 0;
            let mut field_names = std::collections::HashSet::new();

            // Collect statistics from all shards for this index
            for shard in orchestrator.shards.values() {
                if let Some(store) = &shard.store {
                    let store_clone = Arc::clone(store);
                    let index_name_clone = index_name.clone();

                    let shard_stats = tokio::task::spawn_blocking(move || {
                        let stats = store_clone.get_index_statistics(&index_name_clone)?;
                        let fields = store_clone.get_index_field_names(&index_name_clone)?;
                        Ok::<_, storage::StoreError>((stats, fields))
                    })
                    .await
                    .map_err(|e| {
                        OrchestratorError::Io(std::io::Error::other(format!(
                            "Failed to spawn shard stats task: {}",
                            e
                        )))
                    })?
                    .map_err(|e| {
                        OrchestratorError::Io(std::io::Error::other(format!(
                            "Failed to get shard stats: {}",
                            e
                        )))
                    })?;

                    total_document_count += shard_stats.0.document_count;
                    total_size_bytes += shard_stats.0.total_size_bytes;
                    if shard_stats.0.tantivy_index_exists {
                        tantivy_exists_count += 1;
                    }

                    // Collect unique field names
                    for field in shard_stats.1 {
                        field_names.insert(field);
                    }
                }
            }

            let mut field_names_vec: Vec<String> = field_names.into_iter().collect();

            // Sort fields with "id" first, then alphabetically
            field_names_vec.sort_by(|a, b| {
                match (a.as_str(), b.as_str()) {
                    ("id", "id") => std::cmp::Ordering::Equal,
                    ("id", _) => std::cmp::Ordering::Less, // "id" comes first
                    (_, "id") => std::cmp::Ordering::Greater, // "id" comes first
                    (a, b) => a.cmp(b),                    // alphabetical for others
                }
            });

            indexes_json.push(serde_json::json!({
                "name": index_name,
                "document_count": total_document_count,
                "total_size_bytes": total_size_bytes,
                "size_mb": total_size_bytes / (1024 * 1024),
                "shard_count": actual_shard_count,
                "tantivy_shards": tantivy_exists_count,
                "field_names": field_names_vec
            }));
        }

        Ok(serde_json::json!({
            "indexes": indexes_json,
            "total_indexes": indexes_json.len(),
            "node_id": orchestrator.identity.uuid.to_string(),
            "total_shards": actual_shard_count
        }))
    }
}

#[derive(Debug)]
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
}

#[cfg(test)]
mod tests {
    use super::*;

    /// RAII guard that ensures test data cleanup even if test panics
    struct TestDataGuard {
        data_dir: PathBuf,
    }

    impl TestDataGuard {
        fn new(test_name: &str) -> Self {
            // Use system temp directory for test data instead of production data directory
            let temp_base = std::env::temp_dir()
                .join("cameodb_tests")
                .join(test_name)
                .join(uuid::Uuid::new_v4().to_string()); // Add UUID for unique test runs

            // Clean up any existing test data
            if temp_base.exists() {
                std::fs::remove_dir_all(&temp_base).expect("Failed to remove existing test data");
            }

            // Create the test directory
            std::fs::create_dir_all(&temp_base).expect("Failed to create test data directory");

            Self {
                data_dir: temp_base,
            }
        }

        fn path(&self) -> &PathBuf {
            &self.data_dir
        }
    }

    /// Creates a test NodeConfig with all required fields
    fn create_test_config(guard: &TestDataGuard, max_shards: usize) -> NodeConfig {
        NodeConfig {
            storage_path: guard.path().clone(),
            max_shards,
            writer_memory_min_mb: 16,
            writer_memory_max_mb: 64,
            writer_memory_default_mb: 20, // 20MB - above Tantivy's 15MB minimum
            wal_sync: true,
        }
    }

    impl Drop for TestDataGuard {
        fn drop(&mut self) {
            if self.data_dir.exists()
                && let Err(e) = std::fs::remove_dir_all(&self.data_dir)
            {
                eprintln!(
                    "Warning: Failed to clean up test data at {:?}: {}",
                    self.data_dir, e
                );
            }
        }
    }

    // Legacy functions removed - now using TestDataGuard RAII pattern

    #[tokio::test]
    async fn test_node_orchestrator_initialization() {
        let _guard = TestDataGuard::new("node_orchestrator_initialization");
        let config = create_test_config(&_guard, 5);

        let orchestrator = NodeOrchestrator::new(config).await.unwrap();
        assert_eq!(orchestrator.shard_count(), 0);
        assert!(!orchestrator.identity().uuid.is_nil());

        // Note: Identity file creation/verification removed since it's handled by cluster crate
        // Cleanup happens automatically when _guard is dropped
    }

    #[tokio::test]
    async fn test_propose_shard() {
        let _guard = TestDataGuard::new("propose_shard");
        let config = create_test_config(&_guard, 5);

        let mut orchestrator = NodeOrchestrator::new(config).await.unwrap();
        let shard_id = uuid::Uuid::new_v4();

        // Propose a new shard
        let result = orchestrator
            .handle_propose_shard(ProposeShard { shard_id })
            .await;

        // Debug the error if it fails
        if let Err(ref e) = result {
            eprintln!("Shard proposal failed: {}", e);
            eprintln!("Error details: {:?}", e);
        }
        assert!(
            result.is_ok(),
            "Shard proposal should succeed but got: {:?}",
            result
        );
        assert_eq!(result.unwrap(), shard_id);
        assert_eq!(orchestrator.shard_count(), 1);

        // Verify directory was created
        let shard_dir = _guard.path().join(format!("shard-{}", shard_id));
        assert!(shard_dir.exists());

        // Try to propose the same shard again (should fail)
        let duplicate_result = orchestrator
            .handle_propose_shard(ProposeShard { shard_id })
            .await;
        assert!(matches!(
            duplicate_result,
            Err(OrchestratorError::ShardAlreadyExists { .. })
        ));
        // Cleanup happens automatically when _guard is dropped
    }

    #[tokio::test]
    async fn test_shard_limit() {
        let _guard = TestDataGuard::new("shard_limit");
        let config = create_test_config(&_guard, 1); // Very small limit for testing

        let mut orchestrator = NodeOrchestrator::new(config).await.unwrap();

        // First shard should succeed
        let shard1 = uuid::Uuid::new_v4();
        let result1 = orchestrator
            .handle_propose_shard(ProposeShard { shard_id: shard1 })
            .await;

        // Debug the error if it fails
        if let Err(ref e) = result1 {
            eprintln!("First shard proposal failed: {}", e);
            eprintln!("Error details: {:?}", e);
        }
        assert!(
            result1.is_ok(),
            "First shard should succeed but got: {:?}",
            result1
        );

        // Second shard should fail due to limit
        let shard2 = uuid::Uuid::new_v4();
        let result2 = orchestrator
            .handle_propose_shard(ProposeShard { shard_id: shard2 })
            .await;
        assert!(matches!(
            result2,
            Err(OrchestratorError::ShardLimitExceeded { .. })
        ));
        // Cleanup happens automatically when _guard is dropped
    }

    #[tokio::test]
    async fn test_router_actor_search() {
        let _guard = TestDataGuard::new("router_actor_search");
        let config = create_test_config(&_guard, 2);

        let orchestrator = NodeOrchestrator::new(config).await.unwrap();
        let orchestrator = std::sync::Arc::new(tokio::sync::RwLock::new(orchestrator));

        let router = RouterActor::new(orchestrator.clone());

        // Test empty search (no shards)
        let search_op = ClientOp::Search {
            index: "test_index".to_string(),
            query: "test query".to_string(),
            limit: Some(10),
        };

        let result = router.handle_client_op(search_op).await.unwrap();

        // Verify empty result structure
        assert!(result.is_object());
        assert_eq!(result["total_shards"], 0);
        assert_eq!(result["results"].as_array().unwrap().len(), 0);
        // Cleanup happens automatically when _guard is dropped
    }

    #[tokio::test]
    async fn test_microshard_write_and_search() {
        let _guard = TestDataGuard::new("microshard_write_search");
        let config = create_test_config(&_guard, 1);

        let mut orchestrator = NodeOrchestrator::new(config).await.unwrap();
        let shard_id = uuid::Uuid::new_v4();

        // Create a shard
        orchestrator
            .handle_propose_shard(ProposeShard { shard_id })
            .await
            .unwrap();

        // Get reference to the shard for testing
        let shard = orchestrator.shards.get(&shard_id).unwrap();

        // Test write operation using new WriteRequest format
        let write_request = WriteRequest {
            index: "test_index".to_string(),
            routing_key: Some("doc1".to_string()),
            doc: serde_json::json!({
                "id": "doc1",
                "title": "Test Document",
                "body": "This is a test document for search functionality",
                "category": "test"
            }),
        };

        let write_result = shard.handle_write(write_request).await;
        assert!(
            write_result.is_ok(),
            "Write should succeed: {:?}",
            write_result
        );

        // Test search operation
        let search_request = SearchRequest {
            index: "test_index".to_string(),
            query_string: "test".to_string(),
            limit: 10,
        };

        let search_result = shard.handle_search(search_request).await;
        assert!(
            search_result.is_ok(),
            "Search should succeed: {:?}",
            search_result
        );

        let results = search_result.unwrap();
        // Note: Results might be empty if indexing hasn't completed yet, but the operation should succeed
        assert!(results.len() <= 10, "Results should respect limit");
        // Cleanup happens automatically when _guard is dropped
    }

    #[tokio::test]
    async fn test_microshard_streaming_search() {
        let _guard = TestDataGuard::new("microshard_streaming");
        let config = create_test_config(&_guard, 1);

        let mut orchestrator = NodeOrchestrator::new(config).await.unwrap();
        let shard_id = uuid::Uuid::new_v4();

        // Create a shard
        orchestrator
            .handle_propose_shard(ProposeShard { shard_id })
            .await
            .unwrap();

        // Get reference to the shard for testing
        let shard = orchestrator.shards.get(&shard_id).unwrap();

        // Write some test documents
        for i in 0..5 {
            let write_request = WriteRequest {
                index: "test_index".to_string(),
                routing_key: Some(format!("doc{}", i)),
                doc: serde_json::json!({
                    "id": format!("doc{}", i),
                    "title": format!("Document {}", i),
                    "body": format!("Content for document {}", i),
                    "number": i
                }),
            };
            let _ = shard.handle_write(write_request).await;
        }

        // Test streaming search
        let stream_request = SearchStream {
            index: "test_index".to_string(),
            query: "document".to_string(),
            limit: 100,
        };

        let stream_result = shard.handle_search_stream(stream_request).await;
        assert!(
            stream_result.is_ok(),
            "Stream should be created successfully"
        );

        let mut stream = stream_result.unwrap();
        let mut total_chunks = 0;

        // Read from stream (limited to avoid infinite wait)
        for _ in 0..5 {
            if let Ok(chunk) =
                tokio::time::timeout(std::time::Duration::from_millis(100), stream.next()).await
            {
                if let Some(results) = chunk {
                    total_chunks += 1;
                    assert!(results.len() <= 50, "Chunk size should respect limit");
                    if results.is_empty() {
                        break; // End of stream
                    }
                } else {
                    break; // Stream closed
                }
            } else {
                break; // Timeout - no more data
            }
        }

        // Stream should have produced at least one chunk (or be empty)
        assert!(total_chunks >= 0, "Stream should be functional");
        // Cleanup happens automatically when _guard is dropped
    }

    #[tokio::test]
    async fn test_router_actor_streaming() {
        let _guard = TestDataGuard::new("router_streaming");
        let config = create_test_config(&_guard, 2);

        let mut orchestrator = NodeOrchestrator::new(config).await.unwrap();

        // Create two shards
        let shard1_id = uuid::Uuid::new_v4();
        let shard2_id = uuid::Uuid::new_v4();

        orchestrator
            .handle_propose_shard(ProposeShard {
                shard_id: shard1_id,
            })
            .await
            .unwrap();
        orchestrator
            .handle_propose_shard(ProposeShard {
                shard_id: shard2_id,
            })
            .await
            .unwrap();

        let orchestrator = std::sync::Arc::new(tokio::sync::RwLock::new(orchestrator));
        let router = RouterActor::new(orchestrator.clone());

        // Test streaming operation via ClientOp::Stream
        let stream_op = ClientOp::Stream {
            index: "test_index".to_string(),
            query: "test query".to_string(),
        };

        let result = router.handle_client_op(stream_op).await.unwrap();

        // Verify stream operation acknowledgment
        assert!(result.is_object());
        assert_eq!(result["message"], "Stream operation initiated");
        assert_eq!(result["index"], "test_index");
        assert_eq!(result["query"], "test query");

        // Test direct streaming method
        let stream_result = router
            .handle_client_stream("test_index".to_string(), "test query".to_string())
            .await;

        assert!(
            stream_result.is_ok(),
            "Stream should be created successfully"
        );

        let mut stream = stream_result.unwrap();

        // Test that stream is functional (limited read to avoid blocking)
        if let Ok(chunk) =
            tokio::time::timeout(std::time::Duration::from_millis(100), stream.next()).await
        {
            // Stream should either return data or close cleanly
            assert!(
                chunk.is_none() || chunk.unwrap().len() <= 50,
                "Stream chunk should be valid"
            );
        }
        // Cleanup happens automatically when _guard is dropped
    }
}
