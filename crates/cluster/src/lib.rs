//! # Cluster Core - Distributed Topology for CameoDB
//!
//! This crate provides the foundational topology logic for CameoDB's distributed architecture.
//! It implements consistent hashing for data distribution and node identity management.
//!
//! ## Key Features
//!
//! - **Self-Sovereign Identity**: Each node generates a unique UUID and human-readable name
//! - **Consistent Hashing**: Uses 256 virtual nodes per physical node for even distribution
//! - **Deterministic Routing**: Same key always routes to the same node (when topology is stable)
//! - **Wrap-Around Logic**: Handles edge cases in the hash ring space
//!
//! ## Architecture
//!
//! ```text
//! Key "user:123" -> SHA256 -> Hash Value -> Ring Lookup -> Node UUID
//!                     |            |            |           |
//!                     v            v            v           v
//!                "user:123"   0x1A2B3C4D   Token 42   Node A1B
//! ```
//!
//! ## Example Usage
//!
//! ```rust,no_run
//! use cluster::{NodeIdentity, ConsistentRing};
//! use std::path::PathBuf;
//!
//! // Create or load node identity (use appropriate data directory for your use case)
//! let identity = NodeIdentity::load_or_create(PathBuf::from("./data/cameodb/meta.json"))?;
//! println!("Node: {} ({})", identity.name, identity.uuid);
//!
//! // Set up consistent hash ring
//! let mut ring = ConsistentRing::new();
//! ring.add_node(&identity);
//!
//! // Route keys to nodes
//! let owner = ring.get_owner("user:123");
//! println!("Key 'user:123' belongs to: {:?}", owner);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

/// Number of virtual nodes (tokens) per physical node.
///
/// This value balances distribution quality with memory overhead:
/// - Higher values: Better load distribution, more memory usage
/// - Lower values: Less memory, potentially uneven distribution
///
/// 256 was chosen as a good balance for most use cases.
const VNODE_COUNT: usize = 256;

/// Represents a unique node identity in the CameoDB cluster.
///
/// Each node has a self-sovereign identity consisting of:
/// - A unique UUID for global identification
/// - A human-readable 3-character Base36 name derived from the UUID
/// - 256 deterministic virtual node tokens for consistent hashing
///
/// The identity can be persisted to disk and reloaded to maintain
/// consistency across node restarts.
///
/// # Examples
///
/// ```rust
/// use cluster::NodeIdentity;
///
/// // Generate a new identity
/// let identity = NodeIdentity::new();
/// assert!(identity.name.len() >= 3); // Base36 names are at least 3 chars
/// assert_eq!(identity.vnode_tokens.len(), 256);
///
/// // Identity is deterministic from UUID
/// let uuid = identity.uuid;
/// let identity2 = NodeIdentity::new();
/// // Different UUIDs will have different tokens
/// assert_ne!(identity.vnode_tokens, identity2.vnode_tokens);
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeIdentity {
    /// Unique identifier for this node (UUID v4)
    pub uuid: Uuid,
    /// Human-readable 3-character Base36 name derived from UUID
    pub name: String,
    /// 256 deterministic hash tokens for consistent hashing ring
    pub vnode_tokens: Vec<u64>,
    /// Optional libp2p keypair (protobuf encoded bytes).
    /// Stored as bytes to avoid direct dependency on libp2p in this crate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keypair: Option<Vec<u8>>,
}

#[cfg(test)]
mod ring_distribution_tests {
    use super::*;

    #[test]
    fn ring_distributes_keys_across_nodes() {
        let mut ring = ConsistentRing::new();
        let n1 = NodeIdentity::new();
        let n2 = NodeIdentity::new();
        ring.add_node(&n1);
        ring.add_node(&n2);

        let mut counts = std::collections::HashMap::new();
        for i in 0..200 {
            let key = format!("key-{i}");
            let owner = ring.get_owner(&key).expect("owner");
            *counts.entry(owner).or_insert(0usize) += 1;
        }

        let c1 = *counts.get(&n1.uuid).unwrap_or(&0);
        let c2 = *counts.get(&n2.uuid).unwrap_or(&0);
        assert!(c1 > 0 && c2 > 0, "both nodes should receive keys");
    }
}

/// Errors that can occur during node identity operations.
#[derive(Debug, Error)]
pub enum IdentityError {
    /// I/O error when reading/writing identity files
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// JSON serialization/deserialization error
    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),
}

impl Default for NodeIdentity {
    fn default() -> Self {
        Self::new()
    }
}

impl NodeIdentity {
    /// Creates a new NodeIdentity with a random UUID.
    ///
    /// This generates:
    /// - A new UUID v4
    /// - A 3-character Base36 name derived from the first 2 bytes of the UUID
    /// - 256 deterministic virtual node tokens using SHA256(uuid + index)
    ///
    /// # Examples
    ///
    /// ```rust
    /// use cluster::NodeIdentity;
    ///
    /// let identity = NodeIdentity::new();
    /// println!("Node: {} ({})", identity.name, identity.uuid);
    /// assert_eq!(identity.vnode_tokens.len(), 256);
    /// ```
    pub fn new() -> Self {
        let uuid = Uuid::new_v4();
        let name = humanize_uuid(&uuid);
        let vnode_tokens = generate_tokens(uuid);

        NodeIdentity {
            uuid,
            name,
            vnode_tokens,
            keypair: None,
        }
    }

    /// Save the identity to disk.
    pub fn save(&self, path: &std::path::Path) -> Result<(), IdentityError> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let file = File::create(path)?;
        serde_json::to_writer_pretty(file, self)?;
        Ok(())
    }

    /// Loads an existing identity from disk or creates a new one.
    ///
    /// If the file exists, it loads the identity and validates that it has
    /// the correct number of virtual node tokens (256). If the token count
    /// is incorrect, it regenerates them and saves the updated identity.
    ///
    /// If the file doesn't exist, it creates a new identity and saves it.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the identity file (typically `meta.json`)
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use cluster::NodeIdentity;
    /// use std::path::PathBuf;
    ///
    /// // Use appropriate data directory for your use case
    /// let identity_path = PathBuf::from("./data/cameodb/meta.json");
    ///
    /// // First call creates new identity
    /// let identity1 = NodeIdentity::load_or_create(identity_path.clone())?;
    ///
    /// // Second call loads the same identity
    /// let identity2 = NodeIdentity::load_or_create(identity_path)?;
    ///
    /// assert_eq!(identity1.uuid, identity2.uuid);
    /// assert_eq!(identity1.name, identity2.name);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn load_or_create(path: PathBuf) -> Result<Self, IdentityError> {
        if path.exists() {
            let data = fs::read_to_string(&path)?;
            let mut identity: NodeIdentity = serde_json::from_str(&data)?;

            if identity.vnode_tokens.len() != VNODE_COUNT {
                identity.vnode_tokens = generate_tokens(identity.uuid);
                let file = File::create(&path)?;
                serde_json::to_writer_pretty(file, &identity)?;
            }

            return Ok(identity);
        }

        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let identity = Self::new();

        let file = File::create(&path)?;
        serde_json::to_writer_pretty(file, &identity)?;

        Ok(identity)
    }
}

/// Converts a UUID to a human-readable 3-character Base36 string.
///
/// Takes the first 2 bytes of the UUID, converts to u16, then to Base36.
/// The result is zero-padded to ensure exactly 3 characters.
///
/// # Examples
///
/// ```rust,ignore
/// // This is a private function used internally
/// let uuid = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000")?;
/// let name = humanize_uuid(&uuid);
/// assert_eq!(name.len(), 3);
/// ```
fn humanize_uuid(uuid: &Uuid) -> String {
    let bytes = uuid.as_bytes();
    let prefix = u16::from_be_bytes([bytes[0], bytes[1]]) as u32;
    let mut encoded = to_base36(prefix);

    while encoded.len() < 3 {
        encoded.insert(0, '0');
    }

    encoded
}

/// Converts a u32 value to its Base36 string representation.
///
/// Base36 uses digits 0-9 and letters A-Z, providing a compact
/// representation suitable for human-readable identifiers.
fn to_base36(mut value: u32) -> String {
    if value == 0 {
        return "0".to_string();
    }

    let mut digits = Vec::new();

    while value > 0 {
        let digit = (value % 36) as u8;
        let ch = match digit {
            0..=9 => (b'0' + digit) as char,
            _ => (b'A' + (digit - 10)) as char,
        };
        digits.push(ch);
        value /= 36;
    }

    digits.iter().rev().collect()
}

/// Generates deterministic virtual node tokens for a given UUID.
///
/// Creates 256 tokens by hashing the UUID with an index using SHA256.
/// This ensures:
/// - Deterministic: Same UUID always produces same tokens
/// - Well-distributed: SHA256 provides good hash distribution
/// - Collision-resistant: Cryptographic hash minimizes collisions
///
/// # Arguments
///
/// * `uuid` - The node's UUID to generate tokens for
///
/// # Returns
///
/// A vector of 256 u64 hash tokens
///
/// # Examples
///
/// ```rust
/// use cluster::generate_tokens;
/// use uuid::Uuid;
///
/// let uuid = Uuid::new_v4();
/// let tokens = generate_tokens(uuid);
/// assert_eq!(tokens.len(), 256);
///
/// // Same UUID produces same tokens
/// let tokens2 = generate_tokens(uuid);
/// assert_eq!(tokens, tokens2);
/// ```
pub fn generate_tokens(uuid: Uuid) -> Vec<u64> {
    (0..VNODE_COUNT as u32)
        .map(|index| {
            let mut hasher = Sha256::new();
            hasher.update(uuid.as_bytes());
            hasher.update(index.to_be_bytes());
            let digest = hasher.finalize();
            u64::from_be_bytes(digest[0..8].try_into().expect("digest slice is 8 bytes"))
        })
        .collect()
}

/// A consistent hash ring for distributed data placement.
///
/// The ring maps hash tokens to node UUIDs, providing consistent
/// routing of keys to nodes. Uses a BTreeMap for O(log n) lookups
/// and automatic ordering of tokens.
///
/// ## Algorithm
///
/// 1. Hash the key using SHA256
/// 2. Find the first token >= hash value using BTreeMap::range
/// 3. If no token found, wrap around to the first token
/// 4. Return the UUID associated with that token
///
/// ## Properties
///
/// - **Consistency**: Same key always routes to same node (when topology is stable)
/// - **Load Balancing**: Virtual nodes (256 per physical node) ensure even distribution
/// - **Minimal Rebalancing**: Only ~1/N keys move when adding/removing nodes
///
/// # Examples
///
/// ```rust
/// use cluster::{ConsistentRing, NodeIdentity};
///
/// let mut ring = ConsistentRing::new();
///
/// // Add nodes to the ring
/// let node_a = NodeIdentity::new();
/// let node_b = NodeIdentity::new();
/// ring.add_node(&node_a);
/// ring.add_node(&node_b);
///
/// // Route keys to nodes
/// let owner = ring.get_owner("user:123");
/// assert!(owner.is_some());
///
/// // Same key always routes to same node
/// let owner2 = ring.get_owner("user:123");
/// assert_eq!(owner, owner2);
/// ```
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ConsistentRing {
    /// Maps hash tokens to node UUIDs. BTreeMap provides ordered iteration.
    ring: BTreeMap<u64, Uuid>,
}

impl Default for ConsistentRing {
    fn default() -> Self {
        Self::new()
    }
}

impl ConsistentRing {
    /// Creates a new empty consistent hash ring.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use cluster::ConsistentRing;
    ///
    /// let ring = ConsistentRing::new();
    /// assert_eq!(ring.get_owner("any_key"), None);
    /// ```
    pub fn new() -> Self {
        Self {
            ring: BTreeMap::new(),
        }
    }

    /// Adds a node to the consistent hash ring.
    ///
    /// Inserts all 256 virtual node tokens from the identity into the ring,
    /// mapping each token to the node's UUID.
    ///
    /// # Arguments
    ///
    /// * `identity` - The node identity containing UUID and tokens
    ///
    /// # Examples
    ///
    /// ```rust
    /// use cluster::{ConsistentRing, NodeIdentity};
    ///
    /// let mut ring = ConsistentRing::new();
    /// let identity = NodeIdentity::new();
    ///
    /// ring.add_node(&identity);
    ///
    /// // Node can now receive key assignments
    /// let owner = ring.get_owner("test_key");
    /// assert_eq!(owner, Some(identity.uuid));
    /// ```
    pub fn add_node(&mut self, identity: &NodeIdentity) {
        for &token in &identity.vnode_tokens {
            self.ring.insert(token, identity.uuid);
        }
    }

    /// Removes a node from the consistent hash ring.
    ///
    /// Removes all tokens belonging to the specified node UUID.
    /// Keys previously owned by this node will be redistributed
    /// to other nodes in the ring.
    ///
    /// # Arguments
    ///
    /// * `node_id` - UUID of the node to remove
    ///
    /// # Examples
    ///
    /// ```rust
    /// use cluster::{ConsistentRing, NodeIdentity};
    ///
    /// let mut ring = ConsistentRing::new();
    /// let node_a = NodeIdentity::new();
    /// let node_b = NodeIdentity::new();
    ///
    /// ring.add_node(&node_a);
    /// ring.add_node(&node_b);
    ///
    /// // Remove node A
    /// ring.remove_node(&node_a.uuid);
    ///
    /// // All keys now route to node B
    /// let owner = ring.get_owner("test_key");
    /// assert_eq!(owner, Some(node_b.uuid));
    /// ```
    pub fn remove_node(&mut self, node_id: &Uuid) {
        self.ring.retain(|_, uuid| uuid != node_id);
    }

    /// Determines which node should own the given key.
    ///
    /// Uses consistent hashing to route the key to a node:
    /// 1. Hash the key using SHA256
    /// 2. Find the first token >= hash value
    /// 3. If no such token exists, wrap around to the first token
    /// 4. Return the UUID of the node owning that token
    ///
    /// Returns `None` if the ring is empty.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to route (typically a document ID)
    ///
    /// # Returns
    ///
    /// The UUID of the node that should own this key, or `None` if no nodes exist.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use cluster::{ConsistentRing, NodeIdentity};
    ///
    /// let mut ring = ConsistentRing::new();
    /// let identity = NodeIdentity::new();
    /// ring.add_node(&identity);
    ///
    /// let owner = ring.get_owner("user:123");
    /// assert_eq!(owner, Some(identity.uuid));
    ///
    pub fn get_owner(&self, key: &str) -> Option<Uuid> {
        self.get_owner_with_hash(hash_key(key.as_bytes()))
    }

    /// Number of vnode tokens currently in the ring.
    pub fn len(&self) -> usize {
        self.ring.len()
    }

    /// Determines which node should own the given hash value.
    ///
    /// Uses consistent hashing to route the key to a node:
    /// 1. Find the first token >= hash value
    /// 2. If no such token exists, wrap around to the first token
    /// 3. Return the UUID of the node owning that token
    ///
    /// Returns `None` if the ring is empty.
    ///
    /// # Arguments
    ///
    /// * `hash` - The hash value to route
    ///
    /// # Returns
    ///
    /// The UUID of the node that should own this key, or `None` if no nodes exist.
    fn get_owner_with_hash(&self, hash: u64) -> Option<Uuid> {
        if self.ring.is_empty() {
            return None;
        }

        self.ring
            .range(hash..)
            .map(|(_, uuid)| *uuid)
            .next()
            .or_else(|| self.ring.values().copied().next())
    }
}

/// Hashes a byte slice to a u64 using SHA256.
///
/// Takes the first 8 bytes of the SHA256 digest and converts
/// them to a u64 in big-endian format.
///
/// # Arguments
///
/// * `bytes` - The bytes to hash
///
/// # Returns
///
/// A u64 hash value suitable for consistent hashing
fn hash_key(bytes: &[u8]) -> u64 {
    let digest = Sha256::digest(bytes);
    u64::from_be_bytes(digest[0..8].try_into().expect("digest slice is 8 bytes"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn human_name_padding() {
        let mut bytes = [0u8; 16];
        bytes[1] = 1;
        let uuid = Uuid::from_bytes(bytes);
        let name = humanize_uuid(&uuid);
        assert!(name.len() >= 3);
    }

    #[test]
    fn tokens_deterministic_and_uniqueish() {
        let uuid = Uuid::nil();
        let tokens = generate_tokens(uuid);
        assert_eq!(tokens.len(), VNODE_COUNT);
        let set: HashSet<_> = tokens.into_iter().collect();
        assert!(set.len() >= VNODE_COUNT / 2);
    }

    #[test]
    fn ring_lookup_wraps() {
        let mut ring = ConsistentRing::new();
        let first_identity = NodeIdentity {
            uuid: Uuid::nil(),
            name: "000".to_string(),
            vnode_tokens: vec![10, 20, 30],
            keypair: None,
        };
        ring.add_node(&first_identity);

        assert_eq!(ring.get_owner("key"), Some(Uuid::nil()));

        let second_identity = NodeIdentity {
            uuid: Uuid::new_v4(),
            name: "001".to_string(),
            vnode_tokens: vec![u64::MAX - 5],
            keypair: None,
        };
        ring.add_node(&second_identity);

        assert!(ring.get_owner("zzzz").is_some());
    }
}
