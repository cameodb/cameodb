//! Custom Swarm Orchestrator & Event Loop for CameoDB Distributed Database
//!
//! This module implements the main orchestration logic for the custom libp2p swarm,
//! following the ANOTHER APPROACH architecture. It provides the entry point for
//! swarm initialization and manages the event loop processing.

pub mod behaviour;
pub mod utils;

use crate::config::ClusterConfig;
use anyhow::Result;
use behaviour::{DhtBehaviour, DhtBehaviourEvent};
use cluster::NodeIdentity;
use futures::StreamExt;
use libp2p::core::Transport;
use libp2p::core::transport::upgrade::Version;
use libp2p::swarm::dial_opts::{DialOpts, PeerCondition};
use libp2p::{
    Multiaddr, PeerId, SwarmBuilder, identify,
    identity::Keypair,
    kad, noise,
    pnet::{PnetConfig, PreSharedKey},
    swarm::SwarmEvent,
    tcp, yamux,
};
use std::collections::HashMap;
use std::net::{IpAddr, ToSocketAddrs};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::{select, sync::watch};
use tracing::{debug, info, warn};
use uuid::Uuid;

// Re-export key types for convenience
pub use utils::resolve_listen_address;

/// Result returned after the swarm runtime has been launched
#[derive(Debug)]
pub struct SwarmStartup {
    pub peer_id: PeerId,
    pub listen_addr: Multiaddr,
    pub bootstrap_peer_count: usize,
    pub runtime: SwarmRuntimeHandle,
    pub events: Option<UnboundedReceiver<CoordinatorEvent>>,
}

/// Events emitted from the swarm runtime to be forwarded to the coordinator actor.
#[derive(Debug)]
pub enum CoordinatorEvent {
    RoutingUpdated {
        #[allow(dead_code)]
        peer_id: String,
        #[allow(dead_code)]
        address_count: usize,
    },
    PeerDiscovered {
        peer_id: String,
        address: Option<String>,
    },
    PeerLost {
        peer_id: String,
        node_uuid: Option<String>,
        address: Option<String>,
    },
    DialFailed {
        peer_id: Option<String>,
        error: String,
    },
    /// A connection to this peer failed a liveness ping and was closed.
    PeerUnresponsive { peer_id: String, error: String },
    PeerUuidDiscovered {
        peer_id: String,
        node_uuid: String,
        address: Option<String>,
    },
    PeerNodeMetadataDiscovered {
        node_uuid: String,
        node_name: String,
        shard_count: u32,
        generation: u64,
        checksum: u64,
        address: Option<String>,
        status: String,
        total_storage_bytes: u64,
        total_document_count: u64,
    },
    PeerShardDiscovered {
        node_uuid: String,
        shard: crate::cluster_coordinator::ShardMetadata,
    },
}

/// Handle used to manage the background swarm runtime task
#[derive(Debug, Clone)]
pub struct SwarmRuntimeHandle {
    shutdown_tx: Option<watch::Sender<SwarmControl>>,
    cmd_tx: Option<UnboundedSender<SwarmCommand>>,
    runtime_join_handle: Arc<std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>>,
}

impl SwarmRuntimeHandle {
    fn new(
        shutdown_tx: watch::Sender<SwarmControl>,
        cmd_tx: UnboundedSender<SwarmCommand>,
        runtime_join_handle: tokio::task::JoinHandle<()>,
    ) -> Self {
        Self {
            shutdown_tx: Some(shutdown_tx),
            cmd_tx: Some(cmd_tx),
            runtime_join_handle: Arc::new(std::sync::Mutex::new(Some(runtime_join_handle))),
        }
    }

    fn inert() -> Self {
        Self {
            shutdown_tx: None,
            cmd_tx: None,
            runtime_join_handle: Arc::new(std::sync::Mutex::new(None)),
        }
    }

    /// Request a graceful shutdown of the swarm runtime task
    pub fn shutdown(&self) -> Result<()> {
        if let Some(tx) = &self.shutdown_tx {
            tx.send(SwarmControl::Shutdown)
                .map_err(|err| anyhow::anyhow!("failed to signal swarm shutdown: {}", err))?
        }
        Ok(())
    }

    /// Wait for the swarm runtime task to finish, with timeout.
    pub async fn wait_for_shutdown(&self, timeout: std::time::Duration) -> Result<()> {
        let handle = {
            let mut lock = self
                .runtime_join_handle
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            lock.take()
        };
        if let Some(handle) = handle {
            tokio::time::timeout(timeout, handle)
                .await
                .map_err(|_| anyhow::anyhow!("swarm runtime shutdown timed out"))?
                .map_err(|e| anyhow::anyhow!("swarm runtime task panicked: {}", e))?;
        }
        Ok(())
    }

    /// Returns true if the runtime task is still active
    pub fn is_running(&self) -> bool {
        self.shutdown_tx
            .as_ref()
            .map(|tx| !matches!(*tx.borrow(), SwarmControl::Shutdown))
            .unwrap_or(false)
    }

    /// Publish local shards to the DHT
    pub fn publish_shards(
        &self,
        node_uuid: Uuid,
        node_name: String,
        shards: Vec<crate::cluster_coordinator::ShardMetadata>,
        generation: u64,
        checksum: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let Some(ref cmd_tx) = self.cmd_tx {
            cmd_tx.send(SwarmCommand::PublishShards {
                node_uuid,
                node_name,
                shards,
                generation,
                checksum,
            })?;
        }
        Ok(())
    }

    /// Ask the swarm to redial the seeds if the node has no peer left.
    pub fn request_seed_redial(&self) -> Result<()> {
        if let Some(tx) = &self.cmd_tx {
            tx.send(SwarmCommand::RedialSeeds)
                .map_err(|_| anyhow::anyhow!("Swarm runtime channel closed"))?;
        }
        Ok(())
    }

    /// Query node metadata for a remote node from the DHT
    pub fn query_node_metadata(&self, node_uuid: Uuid) -> Result<()> {
        if let Some(tx) = &self.cmd_tx {
            tx.send(SwarmCommand::QueryNodeMetadata { node_uuid })
                .map_err(|_| anyhow::anyhow!("Swarm runtime channel closed"))?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SwarmControl {
    Run,
    Shutdown,
}

#[derive(Debug, Default)]
struct SwarmRuntimeMetrics {
    total_events: u64,
    behaviour_events: u64,
    kademlia_updates: u64,
    connections_established: u64,
    connections_closed: u64,
    bootstrapped: bool,
}

impl SwarmRuntimeMetrics {
    fn log_summary(&self) {
        info!(
            total_events = self.total_events,
            behaviour_events = self.behaviour_events,
            kademlia_updates = self.kademlia_updates,
            connections_established = self.connections_established,
            connections_closed = self.connections_closed,
            "Swarm runtime summary"
        );
    }
}

#[derive(Default)]
struct PeerBook {
    uuid_by_peer: HashMap<String, String>,
    addr_by_peer: HashMap<String, String>, // last known good address (from established conn or identify)
}

/// Whether a seed address is this node's own listener: the same IP — written out, or what its
/// DNS name resolves to — **and** the same port.
///
/// The port is part of the answer. Two nodes on one host share every IP, and matching the IP
/// alone made each of them skip the other's address as its own, so neither ever dialed the other
/// and no cluster formed. A container has an IP of its own, which is why it never showed there.
fn is_own_address(seed: &Multiaddr, own: &Multiaddr) -> bool {
    use libp2p::multiaddr::Protocol;

    let endpoint = |addr: &Multiaddr| -> Vec<std::net::SocketAddr> {
        let mut host = None;
        let mut port = None;
        for protocol in addr.iter() {
            match protocol {
                Protocol::Ip4(ip) => host = Some(Err(IpAddr::V4(ip))),
                Protocol::Ip6(ip) => host = Some(Err(IpAddr::V6(ip))),
                Protocol::Dns(name) | Protocol::Dns4(name) | Protocol::Dns6(name) => {
                    host = Some(Ok(name.to_string()))
                }
                Protocol::Tcp(p) => port = Some(p),
                _ => {}
            }
        }
        match (host, port) {
            (Some(Err(ip)), Some(port)) => vec![std::net::SocketAddr::new(ip, port)],
            (Some(Ok(name)), Some(port)) => (name.as_str(), port)
                .to_socket_addrs()
                .map(Iterator::collect)
                .unwrap_or_default(),
            _ => Vec::new(),
        }
    };

    let own = endpoint(own);
    endpoint(seed).iter().any(|seed| own.contains(seed))
}

fn select_preferred_address(addrs: &[Multiaddr]) -> Option<Multiaddr> {
    // Priority order: dns4 -> ip4 -> dns6 -> ip6 -> anything else
    if let Some(addr) = addrs
        .iter()
        .find(|a| a.to_string().starts_with("/dns4/"))
        .cloned()
    {
        return Some(addr);
    }
    if let Some(addr) = addrs
        .iter()
        .find(|a| a.to_string().starts_with("/ip4/"))
        .cloned()
    {
        return Some(addr);
    }
    if let Some(addr) = addrs
        .iter()
        .find(|a| a.to_string().starts_with("/dns6/"))
        .cloned()
    {
        return Some(addr);
    }
    if let Some(addr) = addrs
        .iter()
        .find(|a| a.to_string().starts_with("/ip6/"))
        .cloned()
    {
        return Some(addr);
    }
    addrs.first().cloned()
}

/// Initialize the distributed swarm for peer-to-peer communication
pub async fn init_distributed_swarm(
    config: &ClusterConfig,
    node_uuid: Uuid,
    node_name: String,
    keypair: Keypair,
    remote_message_size_bytes: usize,
    remote_timeout_secs: u64,
) -> Result<SwarmStartup> {
    if !config.enabled {
        info!("Cluster mode disabled, running in standalone single-node mode");
        return Ok(SwarmStartup {
            peer_id: PeerId::random(),
            listen_addr: "/ip4/127.0.0.1/tcp/0".parse().unwrap(),
            bootstrap_peer_count: 0,
            runtime: SwarmRuntimeHandle::inert(),
            events: None,
        });
    }

    info!("Initializing distributed libp2p swarm");

    // Create production-ready swarm with Kademlia DHT
    let startup = create_production_swarm(
        config,
        node_uuid,
        node_name,
        keypair,
        remote_message_size_bytes,
        remote_timeout_secs,
    )
    .await?;

    info!("Production swarm initialized successfully");
    info!("   Peer ID: {}", startup.peer_id);
    info!("   Listen Address: {}", startup.listen_addr);
    info!("   Cluster Port: {} (from config)", config.cluster_port);
    info!("   Discovery: Kademlia DHT");
    info!("   Bootstrap Peers: {}", startup.bootstrap_peer_count);

    Ok(startup)
}

/// Create a production-ready libp2p swarm with custom behaviour
async fn create_production_swarm(
    config: &ClusterConfig,
    node_uuid: Uuid,
    node_name: String,
    keypair: Keypair,
    remote_message_size_bytes: usize,
    remote_timeout_secs: u64,
) -> Result<SwarmStartup> {
    let peer_id = PeerId::from(keypair.public());

    info!("Node identity: {}", peer_id);

    // Get optimized listen address using configured bind + interfaces (fallback handled inside)
    let listen_addr = resolve_listen_address(
        &config.bind_address,
        &config.listen_addrs,
        config.cluster_port,
    )?;

    // Create custom network behaviour with production settings
    let behaviour = DhtBehaviour::new(
        peer_id,
        Some(libp2p::kad::Mode::Server), // Server mode for stable operation
        keypair.public(),
        node_uuid,
        node_name,
        remote_message_size_bytes,
        remote_timeout_secs,
    )?
    .with_ping(ping_config(config));

    info!("Created Kademlia DHT behaviour for peer discovery");

    // Load pre-shared key for private network encryption, if configured.
    // When PSK is set, we wrap TCP with PnetConfig (XSalsa20) and skip QUIC
    // (pnet only supports TCP-based transports).
    let cluster_psk = config.load_psk()?;

    let mut swarm = if let Some(cluster_psk) = cluster_psk {
        let psk = PreSharedKey::new(cluster_psk.bytes());
        info!(
            "Cluster PSK enabled — fingerprint: {} (QUIC disabled, TCP wrapped with XSalsa20)",
            psk.fingerprint()
        );
        let pnet_config = PnetConfig::new(psk);

        let mut yamux_config = yamux::Config::default();
        yamux_config.set_max_num_streams(8192);

        SwarmBuilder::with_existing_identity(keypair)
            .with_tokio()
            .with_other_transport(|kp| {
                let tcp_transport =
                    tcp::tokio::Transport::new(tcp::Config::default().nodelay(true))
                        .and_then(move |socket, _| pnet_config.handshake(socket));
                Ok(tcp_transport
                    .upgrade(Version::V1Lazy)
                    .authenticate(noise::Config::new(kp)?)
                    .multiplex(yamux_config.clone()))
            })?
            .with_dns()?
            .with_behaviour(|_key| Ok(behaviour))?
            .with_swarm_config(|c| {
                c.with_idle_connection_timeout(Duration::from_secs(300))
                    .with_max_negotiating_inbound_streams(2048)
            })
            .build()
    } else {
        SwarmBuilder::with_existing_identity(keypair)
            .with_tokio()
            .with_tcp(
                tcp::Config::default().nodelay(true),
                noise::Config::new,
                || {
                    let mut config = yamux::Config::default();
                    config.set_max_num_streams(8192);
                    config
                },
            )?
            .with_quic()
            .with_dns()?
            .with_behaviour(|_key| Ok(behaviour))?
            .with_swarm_config(|c| {
                c.with_idle_connection_timeout(Duration::from_secs(300))
                    .with_max_negotiating_inbound_streams(2048)
            })
            .build()
    };

    // Initialize Kameo remote registry so remote actors can be registered/looked up.
    swarm.behaviour_mut().kameo.init_global();
    info!("Kameo remote actor registry initialized");

    // Publish node UUID to DHT for peer discovery
    if let Err(e) = swarm.behaviour_mut().publish_node_uuid(&peer_id, node_uuid) {
        warn!("Failed to publish node UUID to DHT: {}", e);
    }

    // Start listening on the optimized address
    swarm.listen_on(listen_addr.clone())?;
    info!("Swarm listening on: {}", listen_addr);
    // Log all active listeners to show OS-resolved interfaces/ports (after potential port rebinding)
    for addr in swarm.listeners() {
        info!("   Active listener: {}", addr);
    }

    // Connect to seed nodes for DHT initialization
    let seed_addrs = convert_seed_nodes_to_multiaddrs(&config.seed_nodes);
    let mut connected_peers = 0;
    let mut dialable_seeds = Vec::new();

    info!(
        "Seed node configuration: {} nodes configured",
        config.seed_nodes.len()
    );
    for node in &config.seed_nodes {
        info!("   - Seed node: {}", node);
    }

    for addr in seed_addrs {
        // This node's own address in the seed list — every node can share one list.
        if swarm.listeners().any(|l| l == &addr) || is_own_address(&addr, &listen_addr) {
            info!("Skipping self-dial to local seed node: {}", addr);
            continue;
        }

        dialable_seeds.push(addr.clone());
        info!("Attempting to dial seed node: {}", addr);
        match swarm.dial(addr.clone()) {
            Ok(_) => {
                connected_peers += 1;
                info!("Successfully initiated dial to: {}", addr);
            }
            Err(e) => {
                warn!("Failed to dial seed node {}: {:?}", addr, e);
            }
        }
    }

    info!(
        "Seed node dial summary: {} successful, {} total",
        connected_peers,
        config.seed_nodes.len()
    );

    // Bootstrap is deferred to the swarm runtime and will trigger on first non-self peer connect.
    if connected_peers == 0 {
        info!("No seed nodes available - running in standalone mode");
    }

    // Start the swarm runtime task to process events
    let (event_tx, event_rx) = unbounded_channel();
    let (cmd_tx, cmd_rx) = unbounded_channel();
    let runtime = launch_swarm_runtime(swarm, event_tx, cmd_rx, cmd_tx.clone(), dialable_seeds);

    Ok(SwarmStartup {
        peer_id,
        listen_addr,
        bootstrap_peer_count: connected_peers,
        runtime,
        events: Some(event_rx),
    })
}

/// Whether the identity file is already owner-only. A missing file reports `true` — there is
/// nothing to tighten, and the content check decides that case.
#[cfg(unix)]
fn identity_file_is_owner_only(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    match std::fs::metadata(path) {
        Ok(meta) => meta.permissions().mode() & 0o077 == 0,
        Err(_) => true,
    }
}

#[cfg(not(unix))]
fn identity_file_is_owner_only(_path: &Path) -> bool {
    true
}

/// The node's libp2p keypair and the identity derived from it, read from `node_identity.json`
/// under `storage_path`, and generated and saved on a first boot.
///
/// The node's UUID is derived from the key, and the persisted shard assignments, the ring and
/// every peer name the node by it. So a file that is there but cannot be read, or holds a key
/// that does not decode, stops the start with the file named: generating a key in its place
/// brought the node up as a different node over the same data, with nothing but a warning to
/// say so. A file from an earlier build, which kept a random UUID and no key, is the one file
/// given a new key — its UUID was never derived from anything to keep. A save that fails stops
/// the start too, since the next boot would otherwise generate yet another key.
pub fn load_node_identity(storage_path: &Path) -> Result<(Keypair, NodeIdentity)> {
    let identity_path = storage_path.join("node_identity.json");
    let unusable = |reason: String| {
        anyhow::anyhow!(
            "the node identity at {} cannot be used: {reason}. It holds this node's private key, \
             and the node's id is derived from it: restore it from a backup, or remove it to \
             bring the node up as a new node",
            identity_path.display()
        )
    };

    let stored = match NodeIdentity::load(identity_path.clone()) {
        Ok(stored) => Some(stored),
        Err(cluster::IdentityError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => None,
        Err(e) => return Err(unusable(e.to_string())),
    };
    let keypair = match stored.as_ref().and_then(|stored| stored.keypair.as_deref()) {
        Some(bytes) => Keypair::from_protobuf_encoding(bytes)
            .map_err(|e| unusable(format!("its key does not decode ({e})")))?,
        None if stored.is_some() => {
            info!("Node identity from an earlier build holds no key; generating one");
            Keypair::generate_ed25519()
        }
        None => {
            info!("No node identity yet; generating one");
            Keypair::generate_ed25519()
        }
    };

    let peer_id = PeerId::from(keypair.public());
    let mut identity = NodeIdentity::from_peer_id_bytes(&peer_id.to_bytes());
    identity.keypair = Some(
        keypair
            .to_protobuf_encoding()
            .map_err(|e| anyhow::anyhow!("the node's key cannot be encoded to be saved: {e}"))?,
    );

    // Saved only when it differs from what is on disk. Everything above is derived from the
    // key, so a boot that loaded a good file rebuilds it byte for byte, and replacing the only
    // copy of the node's private key to write back what is already there is risk bought for
    // nothing. The mode counts as a difference: a file from an earlier build carries the
    // umask's `0644`, and one rewrite settles it.
    if !(identity.matches_stored(&identity_path) && identity_file_is_owner_only(&identity_path)) {
        identity.save(&identity_path).map_err(|e| {
            anyhow::anyhow!(
                "the node identity could not be saved to {}: {e}",
                identity_path.display()
            )
        })?;
        info!("Node identity saved to {:?}", identity_path);
    }
    info!("Node UUID (deterministic): {}", identity.uuid);

    Ok((keypair, identity))
}

/// Convert seed nodes to prioritized multiaddr list.
/// Priority order: dns4 -> ip4 -> dns6 -> ip6
/// For hostnames: try dns4 first, then fallback to dns6
/// For IP addresses: use ip4/ip6 directly
fn convert_seed_nodes_to_multiaddrs(seed_nodes: &[String]) -> Vec<Multiaddr> {
    use std::net::IpAddr;

    let mut multiaddrs = Vec::new();

    for node in seed_nodes {
        // Handle IP:port or Host:port format (e.g., "192.168.1.100:9580" or "cameodb-node2:9580" or "[::1]:9580")
        // Use rsplit_once to correctly handle IPv6 addresses that contain colons
        if let Some((host, port)) = node.rsplit_once(':') {
            if let Ok(port_num) = port.parse::<u16>() {
                // Strip brackets if present (common for IPv6 literals)
                let clean_host = if host.starts_with('[') && host.ends_with(']') {
                    &host[1..host.len() - 1]
                } else {
                    host
                };

                match clean_host.parse::<IpAddr>() {
                    // For IP addresses, use ip4/ip6 directly (priority 2 and 4)
                    Ok(IpAddr::V4(_)) => {
                        let addr = format!("/ip4/{}/tcp/{}", clean_host, port_num);
                        if let Ok(ma) = addr.parse::<Multiaddr>() {
                            info!("Converted bootstrap node {} to {}", node, ma);
                            multiaddrs.push(ma);
                        }
                    }
                    Ok(IpAddr::V6(_)) => {
                        let addr = format!("/ip6/{}/tcp/{}", clean_host, port_num);
                        if let Ok(ma) = addr.parse::<Multiaddr>() {
                            info!("Converted bootstrap node {} to {}", node, ma);
                            multiaddrs.push(ma);
                        }
                    }
                    // For hostnames, prioritize dns4 first (priority 1), then fallback to dns6 (priority 3)
                    Err(_) => {
                        // Try dns4 first (priority 1)
                        let addr4 = format!("/dns4/{}/tcp/{}", clean_host, port_num);
                        if let Ok(ma) = addr4.parse::<Multiaddr>() {
                            info!("Bootstrap node {} as dns4: {}", node, ma);
                            multiaddrs.push(ma);
                        } else {
                            // Fallback to dns6 (priority 3)
                            let addr6 = format!("/dns6/{}/tcp/{}", clean_host, port_num);
                            if let Ok(ma) = addr6.parse::<Multiaddr>() {
                                info!("Bootstrap node {} as dns6: {}", node, ma);
                                multiaddrs.push(ma);
                            } else {
                                warn!("Failed to create multiaddr for bootstrap node '{}'", node);
                            }
                        }
                    }
                }
            } else {
                warn!("Invalid port in bootstrap node '{}': {}", node, port);
            }
        } else {
            // Try to parse as full multiaddr (backward compatibility)
            match node.parse::<Multiaddr>() {
                Ok(addr) => {
                    info!("Using full multiaddr bootstrap node: {}", addr);
                    multiaddrs.push(addr);
                }
                Err(e) => {
                    warn!("Invalid bootstrap node format '{}': {}", node, e);
                }
            }
        }
    }

    multiaddrs
}

fn launch_swarm_runtime(
    mut swarm: libp2p::Swarm<DhtBehaviour>,
    event_tx: UnboundedSender<CoordinatorEvent>,
    mut cmd_rx: UnboundedReceiver<SwarmCommand>,
    cmd_tx: UnboundedSender<SwarmCommand>,
    seeds: Vec<Multiaddr>,
) -> SwarmRuntimeHandle {
    let (shutdown_signal_tx, mut shutdown_signal_rx) = watch::channel(SwarmControl::Run);

    let runtime_handle = tokio::spawn(async move {
        info!("Swarm runtime task started");
        let mut metrics = SwarmRuntimeMetrics::default();
        let mut peer_book = PeerBook::default();
        let mut redial = SeedRedial::new(seeds);
        let mut lost = LostPeerRedial::default();

        loop {
            select! {
                _ = shutdown_signal_rx.changed() => {
                    if matches!(*shutdown_signal_rx.borrow(), SwarmControl::Shutdown) {
                        info!("Swarm shutdown signal received");
                        break;
                    }
                }
                Some(cmd) = cmd_rx.recv() => match cmd {
                    SwarmCommand::RedialSeeds => redial.check_now(),
                    cmd => handle_swarm_command(cmd, &mut swarm),
                },
                event = swarm.select_next_some() => {
                    metrics.total_events += 1;
                    lost.observe(&event);
                    handle_swarm_event(event, &mut metrics, &event_tx, &mut swarm, &mut peer_book);
                }
                _ = tokio::time::sleep_until(lost.next_due()), if lost.has_peers() => {
                    lost.redial_due(&mut swarm);
                }
                _ = tokio::time::sleep_until(redial.next_check), if redial.has_seeds() => {
                    redial.check(&mut swarm, &mut metrics);
                }
            }
        }

        metrics.log_summary();
        info!("Swarm runtime task completed");
    });

    SwarmRuntimeHandle::new(shutdown_signal_tx, cmd_tx, runtime_handle)
}

/// Commands that can be sent to the swarm runtime
#[derive(Debug)]
pub enum SwarmCommand {
    PublishShards {
        node_uuid: Uuid,
        node_name: String,
        shards: Vec<crate::cluster_coordinator::ShardMetadata>,
        generation: u64,
        checksum: u64,
    },
    QueryNodeMetadata {
        node_uuid: Uuid,
    },
    /// Check now whether the node is cut off, and redial the seeds if it is.
    RedialSeeds,
}

/// First wait before redialing the seeds of a node with no peers, doubled per attempt.
const SEED_REDIAL_FIRST: Duration = Duration::from_secs(1);
/// Longest wait between redials of a node that stays cut off.
const SEED_REDIAL_MAX: Duration = Duration::from_secs(30);
/// How often a connected node looks again at whether it still is.
const SEED_REDIAL_CONNECTED_CHECK: Duration = Duration::from_secs(5);

/// Redials the seeds while the node has no peer at all.
///
/// Seeds were dialed once, at startup. A node that came up before the seeds were listening —
/// three nodes started together — had every dial refused and stayed alone for good: nothing
/// else dials a node that is not itself a seed. Only a node with *no* connection redials: one
/// connected peer is enough for Kademlia to find the rest, and dialing a seed by address alone
/// while connected to it would open a duplicate connection.
struct SeedRedial {
    seeds: Vec<Multiaddr>,
    backoff: Duration,
    next_check: tokio::time::Instant,
}

impl SeedRedial {
    fn new(seeds: Vec<Multiaddr>) -> Self {
        Self {
            seeds,
            backoff: SEED_REDIAL_FIRST,
            next_check: tokio::time::Instant::now() + SEED_REDIAL_FIRST,
        }
    }

    fn has_seeds(&self) -> bool {
        !self.seeds.is_empty()
    }

    fn check_now(&mut self) {
        self.next_check = tokio::time::Instant::now();
    }

    fn check(
        &mut self,
        swarm: &mut libp2p::Swarm<DhtBehaviour>,
        metrics: &mut SwarmRuntimeMetrics,
    ) {
        let now = tokio::time::Instant::now();
        if swarm.connected_peers().next().is_some() {
            self.backoff = SEED_REDIAL_FIRST;
            self.next_check = now + SEED_REDIAL_CONNECTED_CHECK;
            return;
        }
        info!(
            seeds = self.seeds.len(),
            next_in_secs = self.backoff.as_secs(),
            "No connected peers; redialing seed nodes"
        );
        for addr in &self.seeds {
            if let Err(e) = swarm.dial(addr.clone()) {
                debug!("Seed redial to {} not started: {}", addr, e);
            }
        }
        // Kademlia bootstraps on the first peer connection; let the next one do it again, since
        // whatever the routing table held when this node lost its last peer is stale.
        metrics.bootstrapped = false;
        self.next_check = now + self.backoff;
        self.backoff = (self.backoff * 2).min(SEED_REDIAL_MAX);
    }
}

/// Redials peers this node has lost, until a connection to each is back.
///
/// The seed redial covers a node with no peers at all. A node that loses one peer and keeps
/// another — the lost one frozen, or cut off from this node alone — has nothing that dials it
/// again: seeds are not redialed while any peer is connected, and Kademlia raises no event for a
/// peer it already knows. Dialed by peer id, so Kademlia supplies the addresses it holds, and
/// only when not connected or dialing, so a dial left hanging on a frozen peer is not stacked on:
/// it completes when the peer resumes.
#[derive(Default)]
struct LostPeerRedial {
    peers: HashMap<PeerId, (Duration, tokio::time::Instant)>,
}

impl LostPeerRedial {
    fn has_peers(&self) -> bool {
        !self.peers.is_empty()
    }

    fn next_due(&self) -> tokio::time::Instant {
        self.peers
            .values()
            .map(|(_, due)| *due)
            .min()
            .unwrap_or_else(|| tokio::time::Instant::now() + SEED_REDIAL_MAX)
    }

    /// Start tracking a peer whose last connection closed; stop once one is established.
    fn observe(&mut self, event: &SwarmEvent<DhtBehaviourEvent>) {
        match event {
            SwarmEvent::ConnectionClosed {
                peer_id,
                num_established: 0,
                ..
            } => {
                self.peers.insert(
                    *peer_id,
                    (
                        SEED_REDIAL_FIRST,
                        tokio::time::Instant::now() + SEED_REDIAL_FIRST,
                    ),
                );
            }
            SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                self.peers.remove(peer_id);
            }
            _ => {}
        }
    }

    fn redial_due(&mut self, swarm: &mut libp2p::Swarm<DhtBehaviour>) {
        let now = tokio::time::Instant::now();
        for (peer, (backoff, due)) in self.peers.iter_mut() {
            if *due > now {
                continue;
            }
            if swarm.is_connected(peer) {
                continue;
            }
            let opts = DialOpts::peer_id(*peer)
                .condition(PeerCondition::DisconnectedAndNotDialing)
                .build();
            match swarm.dial(opts) {
                Ok(()) => debug!(%peer, next_in_secs = backoff.as_secs(), "Redialing lost peer"),
                Err(e) => debug!(%peer, error = %e, "Lost-peer redial not started"),
            }
            *due = now + *backoff;
            *backoff = (*backoff * 2).min(SEED_REDIAL_MAX);
        }
    }
}

fn handle_swarm_command(cmd: SwarmCommand, swarm: &mut libp2p::Swarm<DhtBehaviour>) {
    match cmd {
        SwarmCommand::PublishShards {
            node_uuid,
            node_name,
            shards,
            generation,
            checksum,
        } => {
            if let Err(e) = swarm
                .behaviour_mut()
                .publish_shards(node_uuid, node_name, &shards, generation, checksum)
            {
                warn!("Failed to publish shards to DHT: {}", e);
            }
        }
        SwarmCommand::QueryNodeMetadata { node_uuid } => {
            swarm.behaviour_mut().query_node_metadata(node_uuid);
        }
        // Owned by the runtime loop, which holds the redial state.
        SwarmCommand::RedialSeeds => {}
    }
}

fn handle_swarm_event(
    event: SwarmEvent<DhtBehaviourEvent>,
    metrics: &mut SwarmRuntimeMetrics,
    event_tx: &UnboundedSender<CoordinatorEvent>,
    swarm: &mut libp2p::Swarm<DhtBehaviour>,
    peer_book: &mut PeerBook,
) {
    match event {
        SwarmEvent::Behaviour(behaviour_event) => {
            metrics.behaviour_events += 1;
            handle_behaviour_event(behaviour_event, metrics, event_tx, swarm, peer_book);
        }
        SwarmEvent::NewListenAddr { address, .. } => {
            info!("Swarm listening on: {}", address);
        }
        SwarmEvent::ExpiredListenAddr { address, .. } => {
            warn!("Listen address expired: {}", address);
        }
        SwarmEvent::ConnectionEstablished {
            peer_id,
            established_in,
            endpoint,
            num_established,
            ..
        } => {
            metrics.connections_established += 1;
            info!(
                "Connection established with {} ({} ms, {} open)",
                peer_id,
                established_in.as_millis(),
                num_established
            );
            // A peer is discovered once, on its first connection. Two nodes dialing each other
            // at once always open a second one, and each extra `PeerDiscovered` re-ran the whole
            // discovery exchange against the same peer.
            if num_established.get() == 1 {
                let addr = Some(endpoint.get_remote_address().to_string());
                peer_book
                    .addr_by_peer
                    .insert(peer_id.to_string(), addr.clone().unwrap_or_default());
                let _ = event_tx.send(CoordinatorEvent::PeerDiscovered {
                    peer_id: peer_id.to_string(),
                    address: addr,
                });
            }

            // Trigger bootstrap on first non-self peer connection
            if !metrics.bootstrapped && peer_id != *swarm.local_peer_id() {
                let kad_has_peer = {
                    let kad = &mut swarm.behaviour_mut().kademlia;
                    kad.kbuckets().any(|b| !b.is_empty())
                };

                if kad_has_peer {
                    match swarm.behaviour_mut().bootstrap_kademlia() {
                        Ok(_) => {
                            metrics.bootstrapped = true;
                            info!(
                                "Kademlia DHT bootstrap triggered on first peer connect ({})",
                                peer_id
                            );
                        }
                        Err(e) => {
                            warn!(
                                "Deferred bootstrap failed on peer connect {}: {}",
                                peer_id, e
                            );
                        }
                    }
                } else {
                    info!("Deferring bootstrap: Kademlia has no known peers yet");
                }
            }
        }
        SwarmEvent::ConnectionClosed {
            peer_id,
            cause,
            num_established,
            ..
        } => {
            metrics.connections_closed += 1;
            info!(
                "Connection closed with {} ({:?}, {} still open)",
                peer_id, cause, num_established
            );
            // The peer is lost when its last connection goes, not its first. Closing one of
            // several — an idle duplicate reaching the idle timeout — used to mark a live peer
            // lost, with nothing to bring it back until some new connection opened.
            if num_established > 0 {
                return;
            }
            let node_uuid = peer_book.uuid_by_peer.remove(&peer_id.to_string());
            let address = peer_book.addr_by_peer.remove(&peer_id.to_string());
            let _ = event_tx.send(CoordinatorEvent::PeerLost {
                peer_id: peer_id.to_string(),
                node_uuid,
                address,
            });
        }
        SwarmEvent::Dialing {
            peer_id,
            connection_id,
        } => match peer_id {
            Some(peer) => debug!("Dialing peer: {} (conn {:?})", peer, connection_id),
            None => debug!("Dialing new peer address (conn {:?})", connection_id),
        },
        SwarmEvent::IncomingConnection {
            local_addr,
            send_back_addr,
            connection_id,
        } => {
            info!(
                "Incoming connection on {} from {} (conn {:?})",
                local_addr, send_back_addr, connection_id
            );
        }
        SwarmEvent::IncomingConnectionError {
            local_addr,
            send_back_addr,
            connection_id,
            error,
            peer_id,
        } => {
            warn!(
                "Incoming connection error on {} from {:?} (conn {:?}, peer {:?}): {}",
                local_addr, send_back_addr, connection_id, peer_id, error
            );
        }
        SwarmEvent::OutgoingConnectionError {
            peer_id,
            connection_id,
            error,
        } => {
            warn!(
                "Outgoing connection error to {:?} (conn {:?}): {}",
                peer_id, connection_id, error
            );
            let _ = event_tx.send(CoordinatorEvent::DialFailed {
                peer_id: peer_id.map(|p| p.to_string()),
                error: error.to_string(),
            });
        }
        SwarmEvent::ListenerClosed {
            listener_id,
            addresses,
            reason,
            ..
        } => {
            warn!(
                "Listener {:?} closed: {:?} (addresses: {:?})",
                listener_id, reason, addresses
            );
        }
        SwarmEvent::ListenerError { listener_id, error } => {
            warn!("Listener {:?} error: {}", listener_id, error);
        }
        other => {
            debug!("Swarm event: {:?}", other);
        }
    }
}

fn handle_behaviour_event(
    event: DhtBehaviourEvent,
    metrics: &mut SwarmRuntimeMetrics,
    event_tx: &UnboundedSender<CoordinatorEvent>,
    swarm: &mut libp2p::Swarm<DhtBehaviour>,
    peer_book: &mut PeerBook,
) {
    match event {
        DhtBehaviourEvent::Kademlia(kad_event) => {
            handle_kademlia_event(kad_event, metrics, event_tx, swarm, peer_book)
        }
        DhtBehaviourEvent::Kameo(kameo_event) => {
            handle_kameo_event(kameo_event, swarm);
        }
        DhtBehaviourEvent::Identify(identify_event) => {
            handle_identify_event(identify_event, metrics, event_tx, swarm, peer_book);
        }
        DhtBehaviourEvent::Ping(ping_event) => {
            handle_ping_event(ping_event, event_tx, swarm);
        }
    }
}

/// The ping settings for this node, or `None` when pinging is turned off.
fn ping_config(config: &ClusterConfig) -> Option<libp2p::ping::Config> {
    if config.ping_interval_secs == 0 {
        info!("Peer liveness pings disabled (ping_interval_secs = 0)");
        return None;
    }
    let interval = Duration::from_secs(config.ping_interval_secs);
    let timeout = Duration::from_secs(config.ping_timeout_secs.max(1));
    info!(
        interval_secs = interval.as_secs(),
        timeout_secs = timeout.as_secs(),
        "Peer liveness pings enabled"
    );
    Some(
        libp2p::ping::Config::new()
            .with_interval(interval)
            .with_timeout(timeout),
    )
}

/// Close a connection whose peer stopped answering pings.
///
/// A peer that hangs, is paused, or is cut off without its TCP connection closing stays
/// "connected" as far as the transport knows, and every request to it waits out its full
/// timeout. libp2p reports the failed ping but no longer closes anything itself; closing here
/// fails the requests in flight on that connection at once, and — when it was the peer's last
/// connection — raises `PeerLost`, which marks the peer disconnected (its shards stay assigned).
///
/// `Unsupported` is a peer without the protocol — an older node in a rolling upgrade — not a
/// dead one, and is left alone.
fn handle_ping_event(
    event: libp2p::ping::Event,
    event_tx: &UnboundedSender<CoordinatorEvent>,
    swarm: &mut libp2p::Swarm<DhtBehaviour>,
) {
    use libp2p::ping::Failure;
    match event.result {
        Ok(rtt) => debug!(peer = %event.peer, rtt_ms = rtt.as_millis(), "ping"),
        Err(Failure::Unsupported) => {
            debug!(peer = %event.peer, "peer does not support ping; not monitoring it")
        }
        Err(failure) => {
            warn!(
                peer = %event.peer,
                connection = ?event.connection,
                error = %failure,
                "Peer failed a liveness ping; closing the connection"
            );
            swarm.close_connection(event.connection);
            let _ = event_tx.send(CoordinatorEvent::PeerUnresponsive {
                peer_id: event.peer.to_string(),
                error: failure.to_string(),
            });
        }
    }
}

fn handle_kademlia_event(
    event: kad::Event,
    metrics: &mut SwarmRuntimeMetrics,
    event_tx: &UnboundedSender<CoordinatorEvent>,
    swarm: &mut libp2p::Swarm<DhtBehaviour>,
    peer_book: &mut PeerBook,
) {
    match event {
        kad::Event::RoutingUpdated {
            peer, addresses, ..
        } => {
            metrics.kademlia_updates += 1;
            let addr_vec: Vec<_> = addresses.iter().cloned().collect();
            let addr_count = addr_vec.len();
            info!(
                "Routing table updated for {} ({} addresses)",
                peer, addr_count
            );

            // Add addresses to Kademlia routing table
            for addr in &addr_vec {
                swarm
                    .behaviour_mut()
                    .kademlia
                    .add_address(&peer, addr.clone());
            }

            // Dial the peer to establish connection for Kameo actor communication
            // Skip self-dialing
            if peer != *swarm.local_peer_id() {
                if let Some(addr) = select_preferred_address(&addr_vec) {
                    // Only when not already connected or dialing: every routing update used to
                    // open another connection to a peer we already had — four or five per peer
                    // on first contact, each one re-running identify and discovery.
                    let opts = DialOpts::peer_id(peer)
                        .condition(PeerCondition::DisconnectedAndNotDialing)
                        .addresses(vec![addr.clone()])
                        .build();
                    match swarm.dial(opts) {
                        Ok(_) => {
                            peer_book
                                .addr_by_peer
                                .insert(peer.to_string(), addr.to_string());
                            info!("Dialing Kademlia-discovered peer: {} at {}", peer, addr);
                        }
                        Err(e) => {
                            debug!("Failed to dial peer {}: {}", peer, e);
                        }
                    }
                }
            } else {
                debug!("Skipping self-dial to local peer: {}", peer);
            }

            let _ = event_tx.send(CoordinatorEvent::RoutingUpdated {
                peer_id: peer.to_string(),
                address_count: addr_count,
            });
        }
        kad::Event::OutboundQueryProgressed {
            id, result, stats, ..
        } => match result {
            kad::QueryResult::GetRecord(Ok(kad::GetRecordOk::FoundRecord(kad::PeerRecord {
                record,
                ..
            }))) => {
                let key_str = String::from_utf8_lossy(record.key.as_ref());
                if key_str.starts_with("cameodb-peer-") {
                    let peer_id_str = key_str.trim_start_matches("cameodb-peer-");
                    let uuid_str = String::from_utf8_lossy(&record.value);

                    peer_book
                        .uuid_by_peer
                        .insert(peer_id_str.to_string(), uuid_str.to_string());

                    info!(
                        "DHT Record Found: Peer {} -> UUID {}",
                        peer_id_str, uuid_str
                    );

                    let _ = event_tx.send(CoordinatorEvent::PeerUuidDiscovered {
                        peer_id: peer_id_str.to_string(),
                        node_uuid: uuid_str.to_string(),
                        address: None,
                    });
                } else if key_str.starts_with("cameodb-node-") {
                    match serde_json::from_slice::<crate::swarm::behaviour::NodeMetadata>(
                        &record.value,
                    ) {
                        Ok(metadata) => {
                            info!(
                                "DHT Node Metadata Found: Node {} -> {} shards, gen={}, storage={} docs={}",
                                metadata.node_uuid,
                                metadata.shard_count,
                                metadata.generation,
                                metadata.total_storage_bytes,
                                metadata.total_document_count
                            );

                            let _ = event_tx.send(CoordinatorEvent::PeerNodeMetadataDiscovered {
                                node_uuid: metadata.node_uuid.to_string(),
                                node_name: metadata.node_name,
                                shard_count: metadata.shard_count,
                                generation: metadata.generation,
                                checksum: metadata.checksum,
                                address: metadata.address,
                                status: metadata.status,
                                total_storage_bytes: metadata.total_storage_bytes,
                                total_document_count: metadata.total_document_count,
                            });
                        }
                        Err(e) => {
                            warn!("Failed to deserialize node metadata: {}", e);
                        }
                    }
                } else if key_str.starts_with("cameodb-shard-") {
                    let parts: Vec<&str> = key_str.split('-').collect();

                    if parts.len() >= 4 {
                        match serde_json::from_slice::<crate::cluster_coordinator::ShardMetadata>(
                            &record.value,
                        ) {
                            Ok(shard) => {
                                info!(
                                    "DHT Shard Found: Node {} -> Shard {} ({})",
                                    shard.node_id, shard.shard_id, shard.document_count
                                );

                                let _ = event_tx.send(CoordinatorEvent::PeerShardDiscovered {
                                    node_uuid: shard.node_id.to_string(),
                                    shard,
                                });
                            }
                            Err(e) => {
                                warn!("Failed to deserialize shard metadata: {}", e);
                            }
                        }
                    }
                }
            }
            _ => {
                debug!(
                    "Kademlia query {:?} progressed: result={:?}, stats={:?}",
                    id, result, stats
                );
            }
        },
        kad::Event::InboundRequest { request } => {
            debug!("Kademlia inbound request: {:?}", request);
        }
        other => {
            debug!("Kademlia event: {:?}", other);
        }
    }
}

fn handle_kameo_event(event: kameo::remote::Event, _swarm: &mut libp2p::Swarm<DhtBehaviour>) {
    use kameo::remote::Event;

    match event {
        Event::Registry(registry_event) => {
            debug!("Kameo registry event: {:?}", registry_event);
        }
        Event::Messaging(msg_event) => {
            debug!("Kameo messaging event: {:?}", msg_event);
        }
    }
}

fn handle_identify_event(
    event: identify::Event,
    _metrics: &mut SwarmRuntimeMetrics,
    event_tx: &UnboundedSender<CoordinatorEvent>,
    swarm: &mut libp2p::Swarm<DhtBehaviour>,
    peer_book: &mut PeerBook,
) {
    if let identify::Event::Received {
        peer_id,
        info,
        connection_id: _,
    } = event
    {
        info!(
            "Identify: Received info from peer {} ({} addrs, agent: {})",
            peer_id,
            info.listen_addrs.len(),
            info.agent_version
        );

        // Add discovered addresses to Kademlia routing table
        for addr in &info.listen_addrs {
            info!("   - Address: {}", addr);
            swarm
                .behaviour_mut()
                .kademlia
                .add_address(&peer_id, addr.clone());
        }

        // Try to extract Node name and UUID from agent version string: "cameodb/1.0.0/{NAME}/{UUID}"
        let parts: Vec<&str> = info.agent_version.split('/').collect();
        if parts.len() >= 4 {
            let node_name = parts[2];
            let uuid_str = parts[3];
            if let Ok(uuid) = uuid::Uuid::parse_str(uuid_str) {
                peer_book
                    .uuid_by_peer
                    .insert(peer_id.to_string(), uuid.to_string());
                if let Some(addr) = select_preferred_address(&info.listen_addrs) {
                    peer_book
                        .addr_by_peer
                        .insert(peer_id.to_string(), addr.to_string());
                }

                info!(
                    "Discovered Node identity from Identify protocol: {} ({})",
                    node_name, uuid
                );

                // Trigger peer resolution immediately without waiting for DHT
                let _ = event_tx.send(CoordinatorEvent::PeerUuidDiscovered {
                    peer_id: peer_id.to_string(),
                    node_uuid: uuid.to_string(),
                    address: select_preferred_address(&info.listen_addrs).map(|a| a.to_string()),
                });
            } else {
                warn!("Invalid UUID in agent version: {}", uuid_str);
            }
        } else if parts.len() >= 3 {
            // Fallback for old format without node name: "cameodb/1.0.0/{UUID}"
            let uuid_str = parts[2];
            if let Ok(uuid) = uuid::Uuid::parse_str(uuid_str) {
                peer_book
                    .uuid_by_peer
                    .insert(peer_id.to_string(), uuid.to_string());
                if let Some(addr) = select_preferred_address(&info.listen_addrs) {
                    peer_book
                        .addr_by_peer
                        .insert(peer_id.to_string(), addr.to_string());
                }

                info!(
                    "Discovered Node UUID from Identify protocol (legacy format): {}",
                    uuid
                );

                let _ = event_tx.send(CoordinatorEvent::PeerUuidDiscovered {
                    peer_id: peer_id.to_string(),
                    node_uuid: uuid.to_string(),
                    address: select_preferred_address(&info.listen_addrs).map(|a| a.to_string()),
                });
            } else {
                warn!("Invalid UUID in agent version: {}", uuid_str);
            }
        }

        // Fallback: Query the peer's UUID from the DHT (just in case Identify didn't have it or parsing failed)
        // This is now redundant if Identify succeeds, but harmless as a backup.
        swarm.behaviour_mut().query_peer_uuid(&peer_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A seed is this node only at this node's port: two nodes on one host share the IP.
    #[test]
    fn a_seed_is_this_node_only_at_its_own_port() {
        let own: Multiaddr = "/ip4/127.0.0.1/tcp/9701".parse().unwrap();
        let seed = |addr: &str| is_own_address(&addr.parse().unwrap(), &own);
        assert!(seed("/ip4/127.0.0.1/tcp/9701"));
        assert!(
            !seed("/ip4/127.0.0.1/tcp/9702"),
            "another node on this host"
        );
        assert!(seed("/dns4/localhost/tcp/9701"));
        assert!(!seed("/dns4/localhost/tcp/9702"));
        assert!(
            !seed("/ip4/10.0.0.2/tcp/9701"),
            "another host at the same port"
        );
    }

    /// A first boot generates and saves the key; every later boot reads the same one back, and
    /// so comes up as the same node.
    #[test]
    fn a_node_identity_is_generated_once_and_then_kept() {
        let dir = tempfile::tempdir().expect("temp dir");
        let (first_key, first) = load_node_identity(dir.path()).expect("first boot");
        let (again_key, again) = load_node_identity(dir.path()).expect("second boot");
        assert_eq!(first.uuid, again.uuid);
        assert_eq!(first_key.public(), again_key.public());
    }

    /// A file that is there and cannot be used stops the start, and is left as it was for an
    /// operator to restore: a new key would bring the node up as another node over this data.
    #[test]
    fn a_damaged_node_identity_stops_the_start() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("node_identity.json");
        load_node_identity(dir.path()).expect("first boot");
        let mut stored: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&path).expect("read")).expect("json");

        for damaged in ["{ truncated".to_string(), {
            stored["keypair"] = serde_json::json!([1, 2, 3]);
            stored.to_string()
        }] {
            std::fs::write(&path, &damaged).expect("damage the file");
            let refusal = load_node_identity(dir.path())
                .expect_err("a damaged identity must stop the start")
                .to_string();
            assert!(refusal.contains("node_identity.json"), "{refusal}");
            assert_eq!(
                std::fs::read_to_string(&path).expect("read"),
                damaged,
                "the file is left for the operator"
            );
        }
    }

    /// An earlier build kept a random UUID and no key; that file is given a key, since its UUID
    /// was never derived from anything to keep.
    #[test]
    fn a_node_identity_without_a_key_is_given_one() {
        let dir = tempfile::tempdir().expect("temp dir");
        let earlier = NodeIdentity::new();
        earlier
            .save(&dir.path().join("node_identity.json"))
            .expect("save");
        let (key, identity) = load_node_identity(dir.path()).expect("boot");
        assert_ne!(identity.uuid, earlier.uuid);
        let (again, _) = load_node_identity(dir.path()).expect("next boot");
        assert_eq!(key.public(), again.public(), "and keeps it from then on");
    }

    #[test]
    fn test_convert_seed_nodes() {
        let inputs = vec![
            "127.0.0.1:9580".to_string(),
            "cameodb-node2:9580".to_string(),
            "[::1]:9580".to_string(),
            "192.168.1.50:4000".to_string(),
            "/ip4/10.0.0.1/tcp/8000".to_string(), // Direct multiaddr
        ];

        let results = convert_seed_nodes_to_multiaddrs(&inputs);

        assert_eq!(results.len(), 5);
        assert_eq!(results[0].to_string(), "/ip4/127.0.0.1/tcp/9580");
        assert_eq!(results[1].to_string(), "/dns4/cameodb-node2/tcp/9580");
        assert_eq!(results[2].to_string(), "/ip6/::1/tcp/9580");
        assert_eq!(results[3].to_string(), "/ip4/192.168.1.50/tcp/4000");
        assert_eq!(results[4].to_string(), "/ip4/10.0.0.1/tcp/8000");
    }

    #[test]
    fn test_convert_invalid_nodes() {
        let inputs = vec![
            "invalid:port".to_string(), // Invalid port
            "nodoport".to_string(),     // No port
        ];

        let results = convert_seed_nodes_to_multiaddrs(&inputs);
        assert_eq!(results.len(), 0);
    }

    /// A poisoned join-handle slot must not panic the shutdown that follows.
    ///
    /// `wait_for_shutdown` takes the runtime's join handle out of its mutex; a
    /// `.lock().unwrap()` there would turn one contained panic into a panic on every later
    /// shutdown, contradicting the `panic = "unwind"posture the release chose. The slot
    /// holds only ownership of a handle, so recovering the guard is safe and matches what the
    /// rest of the process does.
    #[tokio::test]
    async fn a_poisoned_join_handle_slot_does_not_poison_shutdown() {
        let handle = SwarmRuntimeHandle::inert();
        let slot = Arc::clone(&handle.runtime_join_handle);
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = slot.lock().unwrap();
            panic!("the contained panic the slot is expected to survive");
        }));
        assert!(
            slot.is_poisoned(),
            "the fixture must leave the slot poisoned"
        );

        handle
            .wait_for_shutdown(std::time::Duration::from_millis(10))
            .await
            .expect("shutdown recovers the guard rather than panicking");
    }
}
