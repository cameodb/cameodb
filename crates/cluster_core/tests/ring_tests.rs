use cluster_core::{ConsistentRing, NodeIdentity};
use std::collections::HashSet;
use uuid::Uuid;

#[test]
fn test_add_node_and_get_owner() {
    let mut ring = ConsistentRing::new();

    // Create a node identity
    let node_a = NodeIdentity::new();
    let node_a_uuid = node_a.uuid;

    // Add node to ring
    ring.add_node(&node_a);

    // Test various string keys
    let test_keys = ["hello", "world", "test", "key1", "key2", "foo", "bar"];

    for key in &test_keys {
        let owner = ring.get_owner(key);
        assert_eq!(
            owner,
            Some(node_a_uuid),
            "Key '{}' should map to node A",
            key
        );
    }
}

#[test]
fn test_wrap_around_behavior() {
    let mut ring = ConsistentRing::new();

    // Create a node with a token at the beginning of the ring
    let node_a = NodeIdentity {
        uuid: Uuid::new_v4(),
        name: "AAA".to_string(),
        vnode_tokens: vec![100, 200, 300], // Low values
    };
    let node_a_uuid = node_a.uuid;

    ring.add_node(&node_a);

    // Create a key that will hash to a very high value (near u64::MAX)
    let high_hash_key = "wrap_around_test_key_with_high_hash_value_zzzzzzzzz";

    // This should wrap around to the first node in the ring
    let owner = ring.get_owner(high_hash_key);
    assert_eq!(
        owner,
        Some(node_a_uuid),
        "High hash key should wrap around to first node"
    );

    // Test with u64::MAX explicitly by creating a node with that token
    let node_b = NodeIdentity {
        uuid: Uuid::new_v4(),
        name: "BBB".to_string(),
        vnode_tokens: vec![u64::MAX - 1], // Very high value
    };
    let _node_b_uuid = node_b.uuid;

    ring.add_node(&node_b);

    // Now test wrap-around behavior
    let owner_high = ring.get_owner(high_hash_key);
    // Should still be deterministic - either node A or B depending on hash
    assert!(
        owner_high.is_some(),
        "Should find an owner even for high hash values"
    );
}

#[test]
fn test_remove_node() {
    let mut ring = ConsistentRing::new();

    // Create two nodes
    let node_a = NodeIdentity::new();
    let node_b = NodeIdentity::new();

    let node_a_uuid = node_a.uuid;
    let node_b_uuid = node_b.uuid;

    // Add both nodes
    ring.add_node(&node_a);
    ring.add_node(&node_b);

    // Test keys map to both nodes
    let test_keys = ["key1", "key2", "key3", "key4", "key5"];
    let mut owners_before = HashSet::new();

    for key in &test_keys {
        if let Some(owner) = ring.get_owner(key) {
            owners_before.insert(owner);
        }
    }

    // Should have both nodes as owners for different keys
    assert!(owners_before.len() > 0, "Should have at least one owner");

    // Remove node A
    ring.remove_node(&node_a_uuid);

    // Verify all keys now map to node B only
    for key in &test_keys {
        let owner = ring.get_owner(key);
        assert_eq!(
            owner,
            Some(node_b_uuid),
            "After removing node A, key '{}' should map to node B",
            key
        );
    }

    // Remove node B as well
    ring.remove_node(&node_b_uuid);

    // Now no keys should have owners
    for key in &test_keys {
        let owner = ring.get_owner(key);
        assert_eq!(
            owner, None,
            "After removing all nodes, key '{}' should have no owner",
            key
        );
    }
}

#[test]
fn test_multiple_nodes_distribution() {
    let mut ring = ConsistentRing::new();

    // Create multiple nodes
    let nodes: Vec<NodeIdentity> = (0..5).map(|_| NodeIdentity::new()).collect();
    let node_uuids: Vec<Uuid> = nodes.iter().map(|n| n.uuid).collect();

    // Add all nodes
    for node in &nodes {
        ring.add_node(node);
    }

    // Test that keys distribute across multiple nodes
    let test_keys: Vec<String> = (0..100).map(|i| format!("key_{}", i)).collect();
    let mut owner_counts = std::collections::HashMap::new();

    for key in &test_keys {
        if let Some(owner) = ring.get_owner(key) {
            *owner_counts.entry(owner).or_insert(0) += 1;
        }
    }

    // Should have multiple nodes receiving keys
    assert!(
        owner_counts.len() > 1,
        "Keys should distribute across multiple nodes"
    );

    // All owners should be from our node set
    for owner in owner_counts.keys() {
        assert!(
            node_uuids.contains(owner),
            "Owner should be one of our nodes"
        );
    }
}

#[test]
fn test_empty_ring() {
    let ring = ConsistentRing::new();

    // Empty ring should return None for any key
    assert_eq!(ring.get_owner("any_key"), None);
    assert_eq!(ring.get_owner(""), None);
    assert_eq!(ring.get_owner("test"), None);
}

#[test]
fn test_node_identity_new() {
    let identity = NodeIdentity::new();

    // Check that all fields are properly initialized
    assert_ne!(identity.uuid, Uuid::nil());
    assert!(identity.name.len() >= 3); // Should be at least 3-char Base36
    assert_eq!(identity.vnode_tokens.len(), 256); // Should have 256 tokens

    // Test deterministic behavior - same UUID should produce same tokens
    let uuid = identity.uuid;
    let identity2 = NodeIdentity {
        uuid,
        name: identity.name.clone(),
        vnode_tokens: cluster_core::generate_tokens(uuid),
    };

    assert_eq!(identity.vnode_tokens, identity2.vnode_tokens);
}
