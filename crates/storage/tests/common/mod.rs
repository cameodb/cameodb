use std::fs;
use std::path::PathBuf;

/// Creates a test data directory structure in system temporary directory.
///
/// This ensures all test data is isolated and automatically cleaned up.
/// Uses UUID for unique test runs. The directory structure is:
///
/// ```
/// /tmp/cameodb_tests/storage/{test_name}/{uuid}/
/// ```
pub fn create_test_data_dir(test_name: &str) -> PathBuf {
    use uuid::Uuid;

    let test_data_root = std::env::temp_dir()
        .join("cameodb_tests")
        .join("storage")
        .join(test_name)
        .join(Uuid::new_v4().to_string());

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
            .contains("cameodb_tests/storage/test_utils_test"));

        // Cleanup
        cleanup_test_data_dir(&test_dir);
        assert!(!test_dir.exists());
    }
}
