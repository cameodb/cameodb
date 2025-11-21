use std::fs;
use std::path::PathBuf;

/// Creates a test data directory structure under the workspace root.
///
/// This ensures all test data is contained within the project and can be
/// easily cleaned up. The directory structure is:
///
/// ```
/// {workspace_root}/data/cluster/{test_name}/
/// ```
pub fn create_test_data_dir(test_name: &str) -> PathBuf {
    let workspace_root =
        std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR should be set during tests");

    let test_data_root = PathBuf::from(workspace_root)
        .parent() // Go up from crates/cluster to crates/
        .unwrap()
        .parent() // Go up from crates/ to workspace root
        .unwrap()
        .join("data")
        .join("cluster")
        .join(test_name);

    // Clean up any existing test data
    if test_data_root.exists() {
        fs::remove_dir_all(&test_data_root).expect("Failed to clean up existing test data");
    }

    // Create the directory structure
    fs::create_dir_all(&test_data_root).expect("Failed to create test data directory");

    test_data_root
}

/// Test cleanup helper that can be used in test teardown
pub fn cleanup_test_data_dir(test_data_dir: &PathBuf) {
    if test_data_dir.exists() {
        let _ = fs::remove_dir_all(test_data_dir);
    }
}

/// Creates a test node identity file path within the test data directory
pub fn create_test_identity_path(test_name: &str, node_name: &str) -> PathBuf {
    create_test_data_dir(test_name).join(format!("{}_identity.json", node_name))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_test_data_dir() {
        let test_dir = create_test_data_dir("test_utils_test");

        assert!(test_dir.exists());
        assert!(test_dir.is_dir());
        assert!(test_dir
            .to_string_lossy()
            .contains("data/cluster/test_utils_test"));

        // Cleanup
        cleanup_test_data_dir(&test_dir);
        assert!(!test_dir.exists());
    }

    #[test]
    fn test_create_test_identity_path() {
        let identity_path = create_test_identity_path("identity_test", "node_a");

        assert!(identity_path.parent().unwrap().exists());
        assert!(identity_path
            .to_string_lossy()
            .contains("data/cluster/identity_test"));
        assert!(identity_path
            .to_string_lossy()
            .ends_with("node_a_identity.json"));

        // Cleanup
        cleanup_test_data_dir(&identity_path.parent().unwrap().to_path_buf());
    }
}
