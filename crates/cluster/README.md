# Cluster Core - Distributed Topology for CameoDB

The `cluster_core` crate provides the foundational topology logic for CameoDB's distributed architecture, implementing consistent hashing for data distribution and node identity management.

## Self-Sovereign Identity

Each CameoDB node maintains a **self-sovereign identity** consisting of:

### UUID (Universally Unique Identifier)
- Generated using UUID v4 (random)
- Provides global uniqueness across all nodes
- Used as the canonical node identifier
- Example: `550e8400-e29b-41d4-a716-446655440000`

### Human-Readable Name (Base36)
- 3-character identifier derived from the first 2 bytes of the UUID
- Uses Base36 encoding (0-9, A-Z) for compactness
- Zero-padded to ensure exactly 3 characters
- Examples: `A1B`, `X9Z`, `007`

**Algorithm:**
```rust
fn humanize_uuid(uuid: &Uuid) -> String {
    let bytes = uuid.as_bytes();
    let prefix = u16::from_be_bytes([bytes[0], bytes[1]]) as u32;
    let encoded = to_base36(prefix);
    // Zero-pad to 3 characters
    format!("{:0>3}", encoded)
}
```

This provides operators with memorable node names while maintaining global uniqueness through the underlying UUID.

## Virtual Node (VNode) Strategy

CameoDB uses **256 virtual nodes per physical node** to ensure even data distribution across the cluster.

### Why 256 VNodes?

| VNode Count | Pros | Cons |
|-------------|------|------|
| 64 | Low memory overhead | Uneven distribution with small clusters |
| **256** | **Good balance** | **Moderate memory usage** |
| 1024 | Excellent distribution | High memory overhead |

**256 VNodes** provides the optimal balance between:
- **Distribution Quality**: Sufficient virtual nodes for even load distribution
- **Memory Overhead**: Reasonable memory usage (256 × 8 bytes = 2KB per node)
- **Rebalancing Efficiency**: Only ~1/N keys move when adding/removing nodes

### Token Generation Algorithm

Each virtual node token is generated deterministically using SHA256:

```rust
fn generate_tokens(uuid: Uuid) -> Vec<u64> {
    (0..256).map(|index| {
        let mut hasher = Sha256::new();
        hasher.update(uuid.as_bytes());      // Node identity
        hasher.update(&index.to_be_bytes()); // VNode index
        let digest = hasher.finalize();
        u64::from_be_bytes(digest[0..8].try_into().unwrap())
    }).collect()
}
```

**Properties:**
- **Deterministic**: Same UUID always produces same tokens
- **Well-Distributed**: SHA256 provides excellent hash distribution
- **Collision-Resistant**: Cryptographic hash minimizes token collisions

## Consistent Hashing Flow

The following diagram shows how keys are routed to nodes:

```mermaid
graph TD
    A[Key: "user:123"] --> B[SHA256 Hash]
    B --> C[Hash Value: 0x1A2B3C4D...]
    C --> D[Ring Lookup]
    D --> E{Find Token >= Hash}
    E -->|Found| F[Return Node UUID]
    E -->|Not Found| G[Wrap Around]
    G --> H[Return First Node]
    
    subgraph "Consistent Hash Ring"
        I[Token 42 → Node A1B]
        J[Token 108 → Node X9Z]
        K[Token 200 → Node A1B]
        L[Token 255 → Node X9Z]
    end
    
    D --> I
    D --> J
    D --> K
    D --> L
```

### Routing Algorithm

1. **Hash the Key**: `SHA256(key) → u64`
2. **Ring Lookup**: Find first token ≥ hash value using `BTreeMap::range(hash..)`
3. **Wrap-Around**: If no token found, use first token in ring
4. **Return Owner**: Return UUID associated with the token

```rust
pub fn get_owner(&self, key: &str) -> Option<Uuid> {
    if self.ring.is_empty() {
        return None;
    }

    let hash = hash_key(key.as_bytes());
    
    self.ring
        .range(hash..)           // Find tokens >= hash
        .map(|(_, uuid)| *uuid)
        .next()                  // First match
        .or_else(|| self.ring.values().copied().next()) // Wrap around
}
```

## Key Features

### Consistency
- Same key always routes to same node (when topology is stable)
- Deterministic token generation ensures reproducible behavior
- No coordination required for routing decisions

### Load Balancing
- 256 virtual nodes per physical node ensure even distribution
- Statistical distribution approaches perfect balance with more nodes
- Handles heterogeneous key distributions well

### Minimal Rebalancing
- Only ~1/N keys move when adding/removing nodes
- Virtual nodes minimize the impact of topology changes
- No global rebalancing required

### Fault Tolerance
- No single point of failure
- Nodes can join/leave independently
- Ring state is eventually consistent across all nodes

## Usage Examples

### Basic Usage

```rust
use cluster::{NodeIdentity, ConsistentRing};
use std::path::PathBuf;

// Create or load node identity
let identity = NodeIdentity::load_or_create(PathBuf::from("node.json"))?;
println!("Node: {} ({})", identity.name, identity.uuid);

// Set up consistent hash ring
let mut ring = ConsistentRing::new();
ring.add_node(&identity);

// Route keys to nodes
let owner = ring.get_owner("user:123");
println!("Key 'user:123' belongs to: {:?}", owner);
```

### Multi-Node Cluster

```rust
use cluster::{NodeIdentity, ConsistentRing};

let mut ring = ConsistentRing::new();

// Add multiple nodes
let node_a = NodeIdentity::new(); // e.g., "A1B"
let node_b = NodeIdentity::new(); // e.g., "X9Z"
let node_c = NodeIdentity::new(); // e.g., "M7K"

ring.add_node(&node_a);
ring.add_node(&node_b);
ring.add_node(&node_c);

// Keys distribute across nodes
let keys = ["user:1", "user:2", "user:3", "order:100", "product:50"];
for key in &keys {
    let owner = ring.get_owner(key).unwrap();
    println!("{} → {}", key, owner);
}
```

### Dynamic Membership

```rust
use cluster::{NodeIdentity, ConsistentRing};

let mut ring = ConsistentRing::new();
let node_a = NodeIdentity::new();
let node_b = NodeIdentity::new();

// Initial cluster
ring.add_node(&node_a);
ring.add_node(&node_b);

let key = "important:data";
let initial_owner = ring.get_owner(key);

// Add new node
let node_c = NodeIdentity::new();
ring.add_node(&node_c);

let new_owner = ring.get_owner(key);
// Key may or may not move to new node (depends on hash distribution)

// Remove node
ring.remove_node(&node_a.uuid);
let final_owner = ring.get_owner(key);
// Key will be reassigned if it was on the removed node
```

## Performance Characteristics

- **Lookup Time**: O(log N) where N = number of virtual tokens (256 × nodes)
- **Memory Usage**: O(N) for token storage (~2KB per node)
- **Rebalancing**: Only ~1/N keys move when adding/removing nodes
- **Hash Quality**: SHA256 provides excellent distribution properties

## Integration with Storage Engine

The cluster core integrates with the storage engine for request routing:

```rust
use cluster::ConsistentRing;
use storage::HybridStore;

// Determine which shard should handle a key
let ring = ConsistentRing::new();
// ... populate ring with nodes ...

let owner = ring.get_owner("user:123");
if owner == Some(local_node.uuid) {
    // Handle locally
    local_store.apply_write(operation)?;
} else {
    // Forward to remote node
    forward_to_node(owner, operation)?;
}
```

## Testing

The crate includes comprehensive tests covering:

- **Basic Operations**: Node addition, key routing
- **Wrap-Around Behavior**: Keys with high hash values
- **Node Removal**: Key redistribution after node removal
- **Distribution Quality**: Statistical distribution across multiple nodes
- **Deterministic Behavior**: Same inputs produce same outputs

Run tests with:
```bash
cargo test -p cluster
```

## Future Enhancements

- **Weighted Nodes**: Support for nodes with different capacities
- **Rack Awareness**: Ensure replicas are distributed across failure domains
- **Dynamic Rebalancing**: Automatic rebalancing based on load metrics
- **Gossip Protocol**: Distributed membership management without coordination
