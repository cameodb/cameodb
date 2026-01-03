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
use tracing::{info, warn};

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

        let kameo = remote::Behaviour::new(local_peer_id, remote::messaging::Config::default());

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

    /// Publish local shards to DHT so peers can build the consistent hash ring
    pub fn publish_shards(
        &mut self,
        node_uuid: uuid::Uuid,
        shards: &[crate::cluster_coordinator::ShardMetadata],
    ) -> Result<(), anyhow::Error> {
        use libp2p::kad::{Record, RecordKey};

        let shards_bytes = serde_json::to_vec(shards)?;

        // Key: cameodb-shards-{node_uuid}
        // We use Node UUID instead of PeerID for stability across restarts if PeerID changes
        let key_str = format!("cameodb-shards-{}", node_uuid);
        let key = RecordKey::new(&key_str);

        let record = Record {
            key: key.clone(),
            value: shards_bytes,
            publisher: None,
            expires: None, // TODO: Set expiration
        };

        match self.kademlia.put_record(record, kad::Quorum::One) {
            Ok(_) => {
                info!(
                    "📝 Published {} shards to DHT with key {}",
                    shards.len(),
                    key_str
                );
                Ok(())
            }
            Err(e) => {
                warn!("⚠️  Failed to publish shards to DHT: {:?}", e);
                Err(anyhow::anyhow!("Failed to publish shards: {:?}", e))
            }
        }
    }

    /// Query DHT for a peer's shards
    pub fn query_shards(&mut self, node_uuid: uuid::Uuid) -> kad::QueryId {
        let key_str = format!("cameodb-shards-{}", node_uuid);
        let key = kad::RecordKey::new(&key_str);
        info!(
            "🔍 Querying DHT for shards of node {} with key {}",
            node_uuid, key_str
        );
        self.kademlia.get_record(key)
    }

    /// Bootstrap the Kademlia DHT
    pub fn bootstrap_kademlia(&mut self) -> Result<(), anyhow::Error> {
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
