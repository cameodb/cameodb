use cluster::NodeIdentity;

mod common;
use common::{cleanup_test_data_dir, create_test_identity_path};

#[test]
fn test_node_identity_load_or_create() {
    // Create test identity file path using centralized test data
    let identity_path = create_test_identity_path("identity_persistence", "test_node");

    // First call should create new identity
    let identity1 =
        NodeIdentity::load_or_create(identity_path.clone()).expect("Failed to create identity");

    // Verify identity was created properly
    assert_eq!(identity1.vnode_tokens.len(), 256);
    assert!(identity1.name.len() >= 3);

    // File should exist now
    assert!(identity_path.exists(), "Identity file should be created");

    // Second call should load the same identity
    let identity2 =
        NodeIdentity::load_or_create(identity_path.clone()).expect("Failed to load identity");

    // Should be identical
    assert_eq!(identity1.uuid, identity2.uuid);
    assert_eq!(identity1.name, identity2.name);
    assert_eq!(identity1.vnode_tokens, identity2.vnode_tokens);

    // Cleanup test data
    cleanup_test_data_dir(&identity_path.parent().unwrap().to_path_buf());

    // Verify cleanup worked
    assert!(
        !identity_path.exists(),
        "Identity file should be cleaned up"
    );
}

#[test]
fn test_node_identity_vnode_regeneration() {
    // Create test identity file path
    let identity_path = create_test_identity_path("vnode_regeneration", "test_node");

    // Create identity with correct vnode count
    let mut identity = NodeIdentity::new();

    // Simulate old identity with wrong vnode count
    identity.vnode_tokens = vec![1, 2, 3]; // Wrong count (should be 256)

    // Save the corrupted identity
    std::fs::create_dir_all(identity_path.parent().unwrap()).expect("Failed to create directory");
    let file = std::fs::File::create(&identity_path).expect("Failed to create identity file");
    serde_json::to_writer_pretty(file, &identity).expect("Failed to write identity");

    // Load identity - should regenerate tokens
    let loaded_identity =
        NodeIdentity::load_or_create(identity_path.clone()).expect("Failed to load identity");

    // Should have correct vnode count now
    assert_eq!(loaded_identity.vnode_tokens.len(), 256);
    assert_eq!(loaded_identity.uuid, identity.uuid);
    assert_eq!(loaded_identity.name, identity.name);

    // Cleanup
    cleanup_test_data_dir(&identity_path.parent().unwrap().to_path_buf());
}
