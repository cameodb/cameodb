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

/// The saved file holds a private key, so it must not be left at whatever the umask allows.
#[cfg(unix)]
#[test]
fn a_saved_identity_is_readable_only_by_its_owner() {
    use std::os::unix::fs::PermissionsExt;

    let identity_path = create_test_identity_path("identity_mode", "owner_only");
    let identity = NodeIdentity::new();
    identity.save(&identity_path).expect("save");

    let mode = std::fs::metadata(&identity_path)
        .expect("metadata")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, 0o600, "expected 0600, found {:04o}", mode);

    cleanup_test_data_dir(&identity_path.parent().unwrap().to_path_buf());
}

/// Otherwise the fix never reaches a node that has already booted once.
#[cfg(unix)]
#[test]
fn saving_over_a_world_readable_identity_tightens_the_mode() {
    use std::os::unix::fs::PermissionsExt;

    let identity_path = create_test_identity_path("identity_mode", "tighten");
    std::fs::write(&identity_path, "{}").expect("seed file");
    std::fs::set_permissions(&identity_path, std::fs::Permissions::from_mode(0o644))
        .expect("chmod");

    NodeIdentity::new().save(&identity_path).expect("save");

    let mode = std::fs::metadata(&identity_path)
        .expect("metadata")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, 0o600, "expected 0600, found {:04o}", mode);

    cleanup_test_data_dir(&identity_path.parent().unwrap().to_path_buf());
}

/// This decides whether the key file is rewritten on boot, so it has to be exact.
#[test]
fn matches_stored_distinguishes_an_unchanged_identity_from_every_other_case() {
    let identity_path = create_test_identity_path("identity_matches", "unchanged");
    let identity = NodeIdentity::new();

    assert!(
        !identity.matches_stored(&identity_path),
        "a missing file cannot match"
    );

    identity.save(&identity_path).expect("save");
    assert!(
        identity.matches_stored(&identity_path),
        "the identity just written must match"
    );

    assert!(
        !NodeIdentity::new().matches_stored(&identity_path),
        "a different identity must not match"
    );

    std::fs::write(&identity_path, "{ truncated").expect("corrupt");
    assert!(
        !identity.matches_stored(&identity_path),
        "an unparseable file must not match, so the save that repairs it still happens"
    );

    cleanup_test_data_dir(&identity_path.parent().unwrap().to_path_buf());
}

/// Dropping the keypair would hand back a node that reports its UUID and cannot prove it.
#[test]
fn a_saved_identity_round_trips_including_its_keypair() {
    let identity_path = create_test_identity_path("identity_roundtrip", "keypair");
    let mut identity = NodeIdentity::new();
    identity.keypair = Some(vec![7u8; 68]);

    identity.save(&identity_path).expect("save");
    let loaded = NodeIdentity::load(identity_path.clone()).expect("load");

    assert_eq!(loaded, identity);
    assert_eq!(loaded.keypair.as_deref(), Some(&[7u8; 68][..]));

    // The temp file the atomic write goes through must not survive it.
    let leftover = identity_path.with_extension("json.tmp");
    assert!(
        !leftover.exists(),
        "temp file left behind at {:?}",
        leftover
    );

    cleanup_test_data_dir(&identity_path.parent().unwrap().to_path_buf());
}
