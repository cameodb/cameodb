//! Custom NetworkBehaviour for CameoDB Distributed System
//!
//! This module implements the production-ready network behaviour using Kademlia DHT
//! for peer discovery and distributed routing in enterprise environments.

use anyhow;
use kameo::remote;
use libp2p::{
    Multiaddr, PeerId, identify,
    kad::{self, Mode as KadMode, store::MemoryStore},
    swarm::NetworkBehaviour,
};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

/// Node metadata published to DHT
/// Contains node-level information that changes infrequently
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeMetadata {
    pub node_uuid: uuid::Uuid,
    pub node_name: String,
    pub shard_count: u32,
    pub generation: u64,
    pub checksum: u64,
    pub last_updated: chrono::DateTime<chrono::Utc>,
    /// Node's cluster address (for connectivity)
    pub address: Option<String>,
    /// Node's status (for cluster state tracking)
    pub status: String, // "Connected", "Disconnected", etc.
    /// Total storage bytes across all shards
    pub total_storage_bytes: u64,
    /// Total document count across all shards
    pub total_document_count: u64,
}

/// Parameters for publishing node metadata
#[derive(Debug, Clone)]
pub struct NodeMetadataParams {
    pub node_uuid: uuid::Uuid,
    pub node_name: String,
    pub shard_count: u32,
    pub generation: u64,
    pub checksum: u64,
    pub address: Option<String>,
    pub status: String,
    pub total_storage_bytes: u64,
    pub total_document_count: u64,
}

/// Network behaviour for distributed peer discovery and routing
///
/// This behaviour provides:
/// - **Kademlia DHT**: Distributed hash table for peer discovery and content routing
/// - **Identify**: Automatic peer identification and protocol negotiation
/// - **Bootstrap support**: Connects to configured bootstrap peers for network entry
/// - **Enterprise ready**: Designed for corporate and production environments
#[derive(NetworkBehaviour)]
pub struct DhtBehaviour {
    /// Kameo remote messaging behaviour for actor remoting
    pub kameo: remote::Behaviour,
    /// Global discovery & distributed hash table
    pub kademlia: kad::Behaviour<MemoryStore>,
    /// Identify protocol for peer recognition
    pub identify: identify::Behaviour,
}

impl DhtBehaviour {
    /// Create a new Kademlia DHT behaviour
    pub fn new(
        local_peer_id: PeerId,
        kad_mode: Option<KadMode>,
        local_public_key: libp2p::identity::PublicKey,
        local_node_uuid: uuid::Uuid,
        local_node_name: String,
    ) -> Result<Self, anyhow::Error> {
        info!("🌐 Initializing Kademlia DHT for distributed peer discovery");
        let store = MemoryStore::new(local_peer_id);
        let mut kademlia = kad::Behaviour::new(local_peer_id, store);

        if let Some(mode) = kad_mode {
            kademlia.set_mode(Some(mode));
            info!("⚙️  Kademlia mode set to: {:?}", mode);
        }

        // Configure Kameo remote messaging with larger size limits for batch forwarding
        // Default is 1MB request / 10MB response, which is too small for bulk writes
        // Keep timeout at 30s (reasonable for large batches without blocking startup)
        let messaging_config = remote::messaging::Config::default()
            .with_request_size_maximum(64 * 1024 * 1024) // 64MB for large batch requests
            .with_response_size_maximum(64 * 1024 * 1024) // 64MB for large responses
            .with_request_timeout(std::time::Duration::from_secs(30));
        let kameo = remote::Behaviour::new(local_peer_id, messaging_config);

        // Embed Node UUID and name in agent version for immediate identification during handshake
        // Format: "cameodb/1.0.0/{NAME}/{UUID}"
        let agent_version = format!("cameodb/1.0.0/{}/{}", local_node_name, local_node_uuid);
        let identify =
            identify::Behaviour::new(identify::Config::new(agent_version, local_public_key));

        Ok(Self {
            kameo,
            kademlia,
            identify,
        })
    }

    /// Publish node UUID to DHT so peers can discover it
    /// Uses local peer ID as the key to ensure uniqueness
    pub fn publish_node_uuid(
        &mut self,
        local_peer_id: &PeerId,
        node_uuid: uuid::Uuid,
    ) -> Result<(), anyhow::Error> {
        use libp2p::kad::{Record, RecordKey};

        // Use peer ID as part of the key to ensure uniqueness per node
        let key_str = format!("cameodb-uuid-{}", local_peer_id);
        let key = RecordKey::new(&key_str);
        let value = node_uuid.to_string().into_bytes();

        let record = Record {
            key,
            value,
            publisher: None,
            expires: None,
        };

        match self.kademlia.put_record(record, kad::Quorum::One) {
            Ok(_) => {
                info!(
                    "📝 Published node UUID {} to DHT with key {}",
                    node_uuid, key_str
                );
                Ok(())
            }
            Err(e) => {
                warn!("⚠️  Failed to publish node UUID to DHT: {:?}", e);
                Err(anyhow::anyhow!("Failed to publish node UUID: {:?}", e))
            }
        }
    }

    /// Query DHT for a peer's node UUID
    pub fn query_peer_uuid(&mut self, peer_id: &PeerId) -> kad::QueryId {
        let key_str = format!("cameodb-uuid-{}", peer_id);
        let key = kad::RecordKey::new(&key_str);
        info!(
            "🔍 Querying DHT for peer {} node UUID with key {}",
            peer_id, key_str
        );
        self.kademlia.get_record(key)
    }

    /// Publish node metadata to DHT
    /// Contains node-level information that changes infrequently
    pub fn publish_node_metadata(
        &mut self,
        params: NodeMetadataParams,
    ) -> Result<(), anyhow::Error> {
        use libp2p::kad::{Record, RecordKey};

        let metadata = NodeMetadata {
            node_uuid: params.node_uuid,
            node_name: params.node_name,
            shard_count: params.shard_count,
            generation: params.generation,
            checksum: params.checksum,
            last_updated: chrono::Utc::now(),
            address: params.address,
            status: params.status,
            total_storage_bytes: params.total_storage_bytes,
            total_document_count: params.total_document_count,
        };

        let metadata_bytes = serde_json::to_vec(&metadata)?;

        // Key: cameodb-node-{node_uuid}
        let key_str = format!("cameodb-node-{}", metadata.node_uuid);
        let key = RecordKey::new(&key_str);

        let record = Record {
            key: key.clone(),
            value: metadata_bytes,
            publisher: None,
            expires: None, // TODO: Set expiration
        };

        match self.kademlia.put_record(record, kad::Quorum::One) {
            Ok(_) => {
                info!(
                    "📝 Published node metadata to DHT with key {} ({} shards, gen={})",
                    key_str, metadata.shard_count, metadata.generation
                );
                Ok(())
            }
            Err(e) => {
                warn!("⚠️  Failed to publish node metadata to DHT: {:?}", e);
                Err(anyhow::anyhow!("Failed to publish node metadata: {:?}", e))
            }
        }
    }

    /// Publish individual shard metadata to DHT
    /// Each shard gets its own record for granular updates
    pub fn publish_shard_metadata(
        &mut self,
        node_uuid: uuid::Uuid,
        shard: &crate::cluster_coordinator::ShardMetadata,
    ) -> Result<(), anyhow::Error> {
        use libp2p::kad::{Record, RecordKey};

        let shard_bytes = serde_json::to_vec(shard)?;

        // Key: cameodb-shard-{node_uuid}-{shard_id}
        let key_str = format!("cameodb-shard-{}-{}", node_uuid, shard.shard_id);
        let key = RecordKey::new(&key_str);

        let record = Record {
            key: key.clone(),
            value: shard_bytes,
            publisher: None,
            expires: None, // TODO: Set expiration
        };

        match self.kademlia.put_record(record, kad::Quorum::One) {
            Ok(_) => {
                info!(
                    "📝 Published shard {} to DHT with key {}",
                    shard.shard_id, key_str
                );
                Ok(())
            }
            Err(e) => {
                warn!(
                    "⚠️  Failed to publish shard {} to DHT: {:?}",
                    e, shard.shard_id
                );
                Err(anyhow::anyhow!(
                    "Failed to publish shard {}: {:?}",
                    shard.shard_id,
                    e
                ))
            }
        }
    }

    /// Publish multiple shards efficiently
    /// Updates node metadata and publishes individual shard records
    pub fn publish_shards(
        &mut self,
        node_uuid: uuid::Uuid,
        node_name: String,
        shards: &[crate::cluster_coordinator::ShardMetadata],
        generation: u64,
        checksum: u64,
    ) -> Result<(), anyhow::Error> {
        // Calculate totals from shards
        let total_storage_bytes: u64 = shards.iter().map(|s| s.storage_bytes).sum();
        let total_document_count: u64 = shards.iter().map(|s| s.document_count).sum();

        // First publish node metadata
        let params = NodeMetadataParams {
            node_uuid,
            node_name,
            shard_count: shards.len() as u32,
            generation,
            checksum,
            address: None, // Will be filled by swarm/identify protocol
            status: "Connected".to_string(), // Default status
            total_storage_bytes,
            total_document_count,
        };

        self.publish_node_metadata(params)?;

        // Then publish each shard individually
        let mut published = 0;
        let mut failed = 0;

        for shard in shards {
            match self.publish_shard_metadata(node_uuid, shard) {
                Ok(_) => published += 1,
                Err(e) => {
                    warn!("Failed to publish shard {}: {:?}", shard.shard_id, e);
                    failed += 1;
                }
            }
        }

        info!(
            "📊 Shard publishing complete: {} published, {} failed",
            published, failed
        );

        if failed > 0 {
            warn!("⚠️  Some shards failed to publish to DHT");
        }

        Ok(())
    }

    /// Query DHT for a peer's node metadata
    pub fn query_node_metadata(&mut self, node_uuid: uuid::Uuid) -> kad::QueryId {
        let key_str = format!("cameodb-node-{}", node_uuid);
        let key = kad::RecordKey::new(&key_str);
        info!(
            "🔍 Querying DHT for node metadata of {} with key {}",
            node_uuid, key_str
        );
        self.kademlia.get_record(key)
    }

    /// Query a specific shard's metadata from the DHT
    /// TODO: Implement logic to query individual shards when node metadata changes
    #[allow(dead_code)]
    pub fn query_shard_metadata(
        &mut self,
        node_uuid: uuid::Uuid,
        shard_id: uuid::Uuid,
    ) -> kad::QueryId {
        let key_str = format!("cameodb-shard-{}-{}", node_uuid, shard_id);
        let key = kad::RecordKey::new(&key_str);
        self.kademlia.get_record(key)
    }

    /// Bootstrap the Kademlia DHT
    pub fn bootstrap_kademlia(&mut self) -> Result<(), anyhow::Error> {
        // Skip bootstrap if we have no peers; prevents noisy "No known peers" warnings in standalone mode
        let has_peer = self.kademlia.kbuckets().any(|b| !b.is_empty());
        if !has_peer {
            info!("⌛ Skipping Kademlia bootstrap: no known peers");
            return Ok(());
        }

        info!("🚀 Bootstrapping Kademlia DHT");
        match self.kademlia.bootstrap() {
            Ok(_id) => Ok(()),
            Err(e) => {
                warn!("⚠️  Kademlia bootstrap failed: {}", e);
                Err(anyhow::anyhow!("Bootstrap failed: {}", e))
            }
        }
    }

    #[allow(dead_code)]
    /// Add a peer address to Kademlia routing table
    pub fn add_peer_address(&mut self, peer_id: &PeerId, addr: Multiaddr) {
        self.kademlia.add_address(peer_id, addr);
    }

    #[allow(dead_code)]
    /// Get basic Kademlia statistics
    pub fn kademlia_stats(&mut self) -> KademliaStats {
        KademliaStats {
            bucket_count: self.kademlia.kbuckets().count(),
            peer_count: 0, // Simplified for now
        }
    }
}

/// Basic Kademlia statistics
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct KademliaStats {
    pub bucket_count: usize,
    pub peer_count: usize,
}
