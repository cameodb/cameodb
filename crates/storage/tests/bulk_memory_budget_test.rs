use storage::StorageConfig;
use tempfile::TempDir;

#[test]
fn test_bulk_memory_budget_scaling() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");

    let config = StorageConfig {
        shard_path: temp_dir.path().to_path_buf(),

        // Memory Budget Configuration
        indexer_memory_budget: 64 * 1024 * 1024,
        indexer_memory_min_mb: 32,
        indexer_memory_max_mb: 512,
        total_memory_limit_bytes: 4 * 1024 * 1024 * 1024, // 4GB budget for tests
        memory_pressure_threshold_percent: 80,

        // Other Configuration
        default_batch_size: 1000,
        wal_sync: true,
    };

    // Create a fake index path for testing
    let index_path = temp_dir.path().join("test_index");
    std::fs::create_dir_all(&index_path).expect("Failed to create index directory");

    // Create a larger fake index (> 500MB to trigger max budget)
    let large_index_path = temp_dir.path().join("large_index");
    std::fs::create_dir_all(&large_index_path).expect("Failed to create large index directory");

    // Pre-populate with a large file to simulate big index
    let large_file = large_index_path.join("large_file.bin");
    let content = vec![0u8; 600 * 1024 * 1024]; // 600MB
    std::fs::write(&large_file, &content).expect("Failed to create large test file");

    // Test base budget (small batch)
    let base_budget = config.get_bulk_operation_budget(&index_path, 500);
    let min_budget = config.indexer_memory_min_mb * 1024 * 1024;
    assert_eq!(
        base_budget, min_budget,
        "Small batch should use minimum budget"
    );

    // Test medium batch (1.5x scaling)
    let medium_budget = config.get_bulk_operation_budget(&index_path, 2000);
    let expected_medium = min_budget * 3 / 2;
    assert_eq!(
        medium_budget, expected_medium,
        "Medium batch should use 1.5x budget"
    );

    // Test large batch (2x scaling)
    let large_budget = config.get_bulk_operation_budget(&index_path, 10000);
    let expected_large = min_budget * 2;
    assert_eq!(
        large_budget, expected_large,
        "Large batch should use 2x budget"
    );

    // Verify scaling is capped at max budget
    let max_budget = config.indexer_memory_max_mb * 1024 * 1024;
    assert!(
        large_budget <= max_budget,
        "Budget should not exceed maximum"
    );

    println!("✅ Bulk memory budget scaling works correctly!");
    println!("   Base (500):   {}MB", base_budget / (1024 * 1024));
    println!("   Medium (2000): {}MB", medium_budget / (1024 * 1024));
    println!("   Large (10000): {}MB", large_budget / (1024 * 1024));
}

#[test]
fn test_optimal_memory_budget_by_index_size() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");

    let config = StorageConfig {
        shard_path: temp_dir.path().to_path_buf(),

        // Memory Budget Configuration
        indexer_memory_budget: 64 * 1024 * 1024,
        indexer_memory_min_mb: 32,
        indexer_memory_max_mb: 512,
        total_memory_limit_bytes: 4 * 1024 * 1024 * 1024,
        memory_pressure_threshold_percent: 80,

        // Other Configuration
        default_batch_size: 1000,
        wal_sync: true,
    };

    // Test with non-existent index (should return min budget)
    let non_existent = temp_dir.path().join("non_existent");
    let budget_new = config.get_optimal_memory_budget(&non_existent, None);
    let min_budget = config.indexer_memory_min_mb * 1024 * 1024;
    assert_eq!(
        budget_new, min_budget,
        "New index should use minimum budget"
    );

    // Test with small index (create a small file)
    let small_index = temp_dir.path().join("small_index");
    std::fs::write(&small_index, vec![0u8; 50 * 1024 * 1024]).expect("Failed to create small file");
    let budget_small = config.get_optimal_memory_budget(&small_index, None);
    assert_eq!(
        budget_small, min_budget,
        "Small index (<100MB) should use minimum budget"
    );

    // Test with medium index
    let medium_index = temp_dir.path().join("medium_index");
    std::fs::write(&medium_index, vec![0u8; 300 * 1024 * 1024])
        .expect("Failed to create medium file");
    let budget_medium = config.get_optimal_memory_budget(&medium_index, None);
    let default_budget = config.indexer_memory_budget;
    assert_eq!(
        budget_medium, default_budget,
        "Medium index (101-500MB) should use default budget"
    );

    println!("✅ Optimal memory budget by index size works correctly!");
    println!("   New index:    {}MB", budget_new / (1024 * 1024));
    println!("   Small (50MB): {}MB", budget_small / (1024 * 1024));
    println!("   Medium (300MB): {}MB", budget_medium / (1024 * 1024));
}
