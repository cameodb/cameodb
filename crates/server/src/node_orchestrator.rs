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
use std::sync::Arc;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{error, info, warn};
use uuid::Uuid;

use cluster::{IdentityError, NodeIdentity};
use serde_json::Value as JsonValue;
use storage::{HybridStore, StorageConfig, StoreError, WalOp};

/// Configuration for a CameoDB node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    /// Root directory for all node data
    pub storage_path: PathBuf,
    /// Maximum number of shards this node can host
    pub max_shards: usize,
    /// Memory budget per shard in bytes
    pub shard_memory_budget: usize,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            storage_path: PathBuf::from("./data/server"),
            max_shards: 10,
            shard_memory_budget: 50 * 1024 * 1024, // 50MB
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
#[derive(Debug, Clone)]
pub struct SearchRequest {
    pub query_string: String,
    pub limit: usize,
}

/// Write request message for MicroshardActor.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct WriteRequest {
    pub op: WalOp,
}

/// Client operation messages for RouterActor.
#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone)]
pub enum ClientOp {
    /// Search operation across shards of an index
    Search {
        index: String,
        query: String,
        limit: Option<usize>,
    },
}

/// Microshard actor that manages a single shard's storage and search operations.
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
            .map_err(|e| OrchestratorError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?
            .map_err(|e: StoreError| match e {
                StoreError::Io(io_err) => OrchestratorError::Io(io_err),
                _ => OrchestratorError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    e.to_string(),
                )),
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
        let results = tokio::task::spawn_blocking(move || store.search_documents(&query, limit))
            .await
            .map_err(|e| OrchestratorError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?
            .map_err(|e: StoreError| match e {
                StoreError::Io(io_err) => OrchestratorError::Io(io_err),
                _ => OrchestratorError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    e.to_string(),
                )),
            })?;

        Ok(results)
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
        let op = request.op;

        // Use spawn_blocking to execute write on blocking thread pool
        let seq_id = tokio::task::spawn_blocking(move || store.apply_write(op))
            .await
            .map_err(|e| OrchestratorError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?
            .map_err(|e: StoreError| match e {
                StoreError::Io(io_err) => OrchestratorError::Io(io_err),
                _ => OrchestratorError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    e.to_string(),
                )),
            })?;

        Ok(seq_id)
    }
}

/// Router actor that handles client operations and distributes them across shards.
#[cfg_attr(not(test), allow(dead_code))]
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
        }
    }

    /// Implements scatter-gather search across all shards.
    async fn handle_search(
        &self,
        _index: &str,
        query: &str,
        limit: usize,
    ) -> Result<JsonValue, OrchestratorError> {
        let orchestrator = self.orchestrator.read().await;

        // Get all shard actors (scatter-gather across all shards for now)
        let shard_ids: Vec<Uuid> = orchestrator.shards.keys().copied().collect();
        drop(orchestrator); // Release read lock early

        if shard_ids.is_empty() {
            return Ok(serde_json::json!({
                "results": [],
                "total_shards": 0,
                "query": query
            }));
        }

        // Scatter: Send search requests to all shards
        let mut search_tasks = Vec::new();

        for shard_id in &shard_ids {
            let orchestrator = self.orchestrator.clone();
            let shard_id = *shard_id;
            let search_request = SearchRequest {
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

        Ok(serde_json::json!({
            "results": formatted_results,
            "total_results": formatted_results.len(),
            "total_shards": shard_ids.len(),
            "successful_shards": successful_shards,
            "failed_shards": failed_shards,
            "query": query
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

            if path.is_dir() {
                if let Some(dir_name) = path.file_name().and_then(|n| n.to_str()) {
                    if let Some(uuid_str) = dir_name.strip_prefix("shard-") {
                        if let Ok(shard_id) = Uuid::parse_str(uuid_str) {
                            shard_ids.push(shard_id);
                            info!("Discovered existing shard: {}", shard_id);
                        }
                    }
                }
            }
        }

        Ok(shard_ids)
    }

    /// Creates a storage configuration for a specific shard.
    fn create_shard_storage_config(&self, shard_id: Uuid) -> StorageConfig {
        let shard_path = self.config.storage_path.join(format!("shard-{}", shard_id));

        StorageConfig {
            shard_path,
            writer_memory_budget: self.config.shard_memory_budget,
            wal_sync: true, // Ensure durability
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
            // Find workspace root by going up from current crate directory
            let workspace_root =
                std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set");

            let data_dir = PathBuf::from(workspace_root)
                .parent() // Go up from crates/server to crates/
                .unwrap()
                .parent() // Go up from crates/ to workspace root
                .unwrap()
                .join("data")
                .join("server")
                .join(test_name);

            // Clean up any existing test data
            if data_dir.exists() {
                std::fs::remove_dir_all(&data_dir).expect("Failed to remove existing test data");
            }

            // Create the test directory
            std::fs::create_dir_all(&data_dir).expect("Failed to create test data directory");

            Self { data_dir }
        }

        fn path(&self) -> &PathBuf {
            &self.data_dir
        }
    }

    impl Drop for TestDataGuard {
        fn drop(&mut self) {
            if self.data_dir.exists() {
                if let Err(e) = std::fs::remove_dir_all(&self.data_dir) {
                    eprintln!(
                        "Warning: Failed to clean up test data at {:?}: {}",
                        self.data_dir, e
                    );
                }
            }
        }
    }

    // Legacy functions removed - now using TestDataGuard RAII pattern

    #[tokio::test]
    async fn test_node_orchestrator_initialization() {
        let _guard = TestDataGuard::new("node_orchestrator_initialization");
        let config = NodeConfig {
            storage_path: _guard.path().clone(),
            max_shards: 5,
            shard_memory_budget: 20 * 1024 * 1024, // 20MB - above Tantivy's 15MB minimum
        };

        let orchestrator = NodeOrchestrator::new(config).await.unwrap();
        assert_eq!(orchestrator.shard_count(), 0);
        assert!(!orchestrator.identity().uuid.is_nil());

        // Note: Identity file creation/verification removed since it's handled by cluster crate
        // Cleanup happens automatically when _guard is dropped
    }

    #[tokio::test]
    async fn test_propose_shard() {
        let _guard = TestDataGuard::new("propose_shard");
        let config = NodeConfig {
            storage_path: _guard.path().clone(),
            max_shards: 5,
            shard_memory_budget: 20 * 1024 * 1024, // 20MB - above Tantivy's 15MB minimum
        };

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
        let config = NodeConfig {
            storage_path: _guard.path().clone(),
            max_shards: 1,                         // Very small limit for testing
            shard_memory_budget: 20 * 1024 * 1024, // 20MB - above Tantivy's 15MB minimum
        };

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
        let config = NodeConfig {
            storage_path: _guard.path().clone(),
            max_shards: 2,
            shard_memory_budget: 20 * 1024 * 1024, // 20MB - above Tantivy's 15MB minimum
        };

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
}
