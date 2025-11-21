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

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{error, info, warn};
use uuid::Uuid;

use cluster::{IdentityError, NodeIdentity};
use storage::StorageConfig;

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

/// Placeholder microshard actor for now.
#[derive(Debug)]
pub struct MicroshardActor {
    shard_id: Uuid,
    storage_config: StorageConfig,
}

impl MicroshardActor {
    pub fn new(shard_id: Uuid, storage_config: StorageConfig) -> Self {
        Self {
            shard_id,
            storage_config,
        }
    }

    pub async fn start(&self) -> Result<(), OrchestratorError> {
        info!(
            shard_id = %self.shard_id,
            path = %self.storage_config.shard_path.display(),
            "MicroshardActor starting"
        );
        // TODO: Initialize HybridStore with spawn_blocking when needed
        Ok(())
    }
}

#[derive(Debug)]
pub struct NodeOrchestrator {
    /// Map of shard UUIDs to their microshard actors
    shards: HashMap<Uuid, MicroshardActor>,
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
            let microshard = MicroshardActor::new(shard_id, storage_config);

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
        let microshard = MicroshardActor::new(shard_id, storage_config);
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
    use std::fs;
    use std::path::Path;

    fn create_test_data_dir(test_name: &str) -> PathBuf {
        let workspace_root = std::env::var("CARGO_MANIFEST_DIR")
            .expect("CARGO_MANIFEST_DIR should be set during tests");

        let test_data_root = PathBuf::from(workspace_root)
            .parent() // Go up from crates/server to crates/
            .unwrap()
            .parent() // Go up from crates/ to workspace root
            .unwrap()
            .join("data")
            .join("server")
            .join(test_name);

        if test_data_root.exists() {
            fs::remove_dir_all(&test_data_root).expect("Failed to clean up existing test data");
        }

        fs::create_dir_all(&test_data_root).expect("Failed to create test data directory");

        test_data_root
    }

    fn cleanup_test_data_dir(path: &Path) {
        if path.exists() {
            let _ = fs::remove_dir_all(path);
        }
    }

    #[tokio::test]
    async fn test_node_orchestrator_initialization() {
        let data_dir = create_test_data_dir("orchestrator_initialization");
        let config = NodeConfig {
            storage_path: data_dir.clone(),
            max_shards: 5,
            shard_memory_budget: 10 * 1024 * 1024, // 10MB for testing
        };

        let orchestrator = NodeOrchestrator::new(config).await.unwrap();

        // Verify initialization
        assert_eq!(orchestrator.shard_count(), 0);
        assert!(!orchestrator.identity().uuid.is_nil());
        assert!(orchestrator.identity().name.len() >= 3);

        cleanup_test_data_dir(&data_dir);
    }

    #[tokio::test]
    async fn test_propose_shard() {
        let data_dir = create_test_data_dir("propose_shard");
        let config = NodeConfig {
            storage_path: data_dir.clone(),
            max_shards: 2,
            shard_memory_budget: 10 * 1024 * 1024,
        };

        let mut orchestrator = NodeOrchestrator::new(config).await.unwrap();
        let shard_id = uuid::Uuid::new_v4();

        // Propose a new shard
        let result = orchestrator
            .handle_propose_shard(ProposeShard { shard_id })
            .await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), shard_id);
        assert_eq!(orchestrator.shard_count(), 1);

        // Verify directory was created
        let shard_dir = data_dir.join(format!("shard-{}", shard_id));
        assert!(shard_dir.exists());

        // Try to propose the same shard again (should fail)
        let duplicate_result = orchestrator
            .handle_propose_shard(ProposeShard { shard_id })
            .await;
        assert!(matches!(
            duplicate_result,
            Err(OrchestratorError::ShardAlreadyExists { .. })
        ));

        cleanup_test_data_dir(&data_dir);
    }

    #[tokio::test]
    async fn test_shard_limit() {
        let data_dir = create_test_data_dir("shard_limit");
        let config = NodeConfig {
            storage_path: data_dir.clone(),
            max_shards: 1, // Very small limit for testing
            shard_memory_budget: 10 * 1024 * 1024,
        };

        let mut orchestrator = NodeOrchestrator::new(config).await.unwrap();

        // First shard should succeed
        let shard1 = uuid::Uuid::new_v4();
        let result1 = orchestrator
            .handle_propose_shard(ProposeShard { shard_id: shard1 })
            .await;
        assert!(result1.is_ok());

        // Second shard should fail due to limit
        let shard2 = uuid::Uuid::new_v4();
        let result2 = orchestrator
            .handle_propose_shard(ProposeShard { shard_id: shard2 })
            .await;
        assert!(matches!(
            result2,
            Err(OrchestratorError::ShardLimitExceeded { .. })
        ));

        cleanup_test_data_dir(&data_dir);
    }
}
