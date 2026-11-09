//! Custom NetworkBehaviour for CameoDB Distributed System
//!
//! This module implements the production-ready network behaviour using Kademlia DHT
//! for peer discovery and distributed routing in enterprise environments.

use anyhow;
use kameo::remote;
use libp2p::{
    PeerId, identify,
    kad::{self, Mode as KadMode, store::MemoryStore},
    ping,
    swarm::{NetworkBehaviour, behaviour::toggle::Toggle},
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
    /// Liveness pings on every connection; off when `ping_interval_secs` is `0`. A failed ping
    /// is acted on in the swarm runtime, which closes the connection — libp2p no longer does.
    pub ping: Toggle<ping::Behaviour>,
}

impl DhtBehaviour {
    /// Create a new Kademlia DHT behaviour
    ///
    /// `remote_message_size_bytes` and `remote_timeout_secs` are derived from
    /// `CameoDbConfig::max_record_size_mb` at startup so that the Kameo remote
    /// messaging layer can handle records of the configured maximum size.
    pub fn new(
        local_peer_id: PeerId,
        kad_mode: Option<KadMode>,
        local_public_key: libp2p::identity::PublicKey,
        local_node_uuid: uuid::Uuid,
        local_node_name: String,
        remote_message_size_bytes: usize,
        remote_timeout_secs: u64,
    ) -> Result<Self, anyhow::Error> {
        info!("Initializing Kademlia DHT for distributed peer discovery");
        let store = MemoryStore::new(local_peer_id);
        let mut kademlia = kad::Behaviour::new(local_peer_id, store);

        if let Some(mode) = kad_mode {
            kademlia.set_mode(Some(mode));
            info!("Kademlia mode set to: {:?}", mode);
        }

        // Configure Kameo remote messaging with size limits derived from max_record_size_mb.
        // Bulk batches can contain many records, so the envelope must be generously sized.
        info!(
            "Kameo remote messaging: max_size={}MB, timeout={}s",
            remote_message_size_bytes / (1024 * 1024),
            remote_timeout_secs
        );
        let messaging_config = remote::messaging::Config::default()
            .with_request_size_maximum(remote_message_size_bytes as u64)
            .with_response_size_maximum(remote_message_size_bytes as u64)
            .with_request_timeout(std::time::Duration::from_secs(remote_timeout_secs));
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
            ping: Toggle::from(None),
        })
    }

    /// Turn on liveness pings with these settings; `None` leaves them off.
    pub fn with_ping(mut self, ping: Option<ping::Config>) -> Self {
        self.ping = Toggle::from(ping.map(ping::Behaviour::new));
        self
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
        let key_str = format!("cameodb-peer-{}", local_peer_id);
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
                    "Published node UUID {} to DHT with key {}",
                    node_uuid, key_str
                );
                Ok(())
            }
            Err(e) => {
                warn!("Failed to publish node UUID to DHT: {:?}", e);
                Err(anyhow::anyhow!("Failed to publish node UUID: {:?}", e))
            }
        }
    }

    /// Query DHT for a peer's node UUID
    pub fn query_peer_uuid(&mut self, peer_id: &PeerId) -> kad::QueryId {
        let key_str = format!("cameodb-peer-{}", peer_id);
        let key = kad::RecordKey::new(&key_str);
        info!(
            "Querying DHT for peer {} node UUID with key {}",
            peer_id, key_str
        );
        self.kademlia.get_record(key)
    }

    /// Bootstrap the Kademlia DHT
    pub fn bootstrap_kademlia(&mut self) -> Result<(), anyhow::Error> {
        // Skip bootstrap if we have no peers; prevents noisy "No known peers" warnings in standalone mode
        let has_peer = self.kademlia.kbuckets().any(|b| !b.is_empty());
        if !has_peer {
            info!("Skipping Kademlia bootstrap: no known peers");
            return Ok(());
        }

        info!("Bootstrapping Kademlia DHT");
        match self.kademlia.bootstrap() {
            Ok(_id) => Ok(()),
            Err(e) => {
                warn!("Kademlia bootstrap failed: {}", e);
                Err(anyhow::anyhow!("Bootstrap failed: {}", e))
            }
        }
    }
}
