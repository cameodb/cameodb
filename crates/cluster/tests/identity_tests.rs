use cluster::NodeIdentity;

mod common;
use common::{cleanup_test_data_dir, create_test_identity_path};

/// The identity is the peer id's: the same bytes give the same UUID, name and tokens, which is
/// what lets a node that reloads its key come back as itself.
#[test]
fn an_identity_derived_from_a_peer_id_is_the_same_every_time() {
    let first = NodeIdentity::from_peer_id_bytes(b"a peer id");
    let again = NodeIdentity::from_peer_id_bytes(b"a peer id");
    assert_eq!(first, again);
    assert_eq!(first.vnode_tokens.len(), 256);
    assert_ne!(
        first.uuid,
        NodeIdentity::from_peer_id_bytes(b"another").uuid
    );
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
