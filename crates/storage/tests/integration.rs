use storage::{HybridStore, IndexSchema, StorageConfig};
use tempfile::TempDir;

/// Minimal integration test focusing on storage engine basics
#[test]
fn test_storage_engine_basics() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");

    let config = StorageConfig {
        shard_path: temp_dir.path().to_path_buf(),
        indexer_memory_budget: 32 * 1024 * 1024, // 32MB per index
        indexer_memory_min_mb: 16,               // 16MB minimum
        indexer_memory_max_mb: 256,              // 256MB maximum
        default_batch_size: 1000,                // 1000 operations default
        wal_sync: true,
    };

    // Test store creation
    let store = HybridStore::new(config).expect("Failed to create HybridStore");

    // Test that directories are created
    assert!(
        temp_dir.path().join("store.redb").exists(),
        "Redb file should be created"
    );
    assert!(
        temp_dir.path().join("indices").exists(),
        "Indices directory should be created"
    );

    // Test index listing (empty initially)
    let index_names = store.get_index_names().expect("Failed to get index names");
    assert_eq!(index_names.len(), 0, "Should start with no indexes");

    // Test index creation (directories are created lazily)
    let index_path = temp_dir.path().join("indices").join("test_index");
    assert!(!index_path.exists(), "Index directory should not exist yet");

    // Create and store a schema for the index
    let schema = IndexSchema::default();
    store
        .store_schema_and_cache("test_index", &schema)
        .expect("Failed to store schema");

    // Now the index should appear in listings
    let index_names = store.get_index_names().expect("Failed to get index names");
    assert_eq!(index_names.len(), 1, "Should have one index");
    assert!(
        index_names.contains(&"test_index".to_string()),
        "Index name should be present"
    );

    // Test index deletion
    store
        .delete_index_data("test_index", false)
        .expect("Failed to delete index");

    // Note: delete_index_data removes the index data but not the schema
    // This is by design - schemas persist for potential recovery
    // Let's verify the index data is gone by checking statistics
    let snapshot = store
        .gather_index_stats(false)
        .expect("Failed to get stats snapshot");
    let test_index_stats = snapshot.per_index.get("test_index");
    assert!(
        test_index_stats.is_some(),
        "Index should still be present in stats"
    );
    assert_eq!(
        test_index_stats.unwrap().document_count,
        0,
        "Document count should be 0 after delete"
    );

    // The index still appears because the schema exists
    // This is expected behavior
    let index_names_final = store
        .get_index_names()
        .expect("Failed to get index names after delete");
    assert_eq!(
        index_names_final.len(),
        1,
        "Index should still appear in listing due to schema persistence"
    );
    assert!(
        index_names_final.contains(&"test_index".to_string()),
        "Index name should still be present"
    );

    println!("✅ Storage engine basics work correctly!");
}

/// Test configuration and basic setup
#[test]
fn test_storage_configuration() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");

    // Test different configurations
    let config = StorageConfig {
        shard_path: temp_dir.path().to_path_buf(),
        indexer_memory_budget: 64 * 1024 * 1024, // 64MB per index
        indexer_memory_min_mb: 32,               // 32MB minimum
        indexer_memory_max_mb: 512,              // 512MB maximum
        default_batch_size: 2000,                // 2000 operations default
        wal_sync: false,                         // No sync for testing
    };

    // Test store creation with custom config
    let store = HybridStore::new(config).expect("Failed to create HybridStore");

    // Verify directories are created
    assert!(
        temp_dir.path().join("store.redb").exists(),
        "Redb file should be created"
    );
    assert!(
        temp_dir.path().join("indices").exists(),
        "Indices directory should be created"
    );

    // Test that multiple indexes can be created
    let schema1 = IndexSchema::default();
    let schema2 = IndexSchema::default();
    store
        .store_schema_and_cache("index1", &schema1)
        .expect("Failed to store schema1");
    store
        .store_schema_and_cache("index2", &schema2)
        .expect("Failed to store schema2");

    let snapshot1 = store
        .gather_index_stats(false)
        .expect("Failed to get stats snapshot for index1");
    let snapshot2 = store
        .gather_index_stats(false)
        .expect("Failed to get stats snapshot for index2");

    let stats1 = snapshot1.per_index.get("index1").unwrap();
    let stats2 = snapshot2.per_index.get("index2").unwrap();

    assert_eq!(
        stats1.document_count, 0,
        "New index1 should have 0 documents"
    );
    assert_eq!(
        stats2.document_count, 0,
        "New index2 should have 0 documents"
    );

    // Test index listing
    let index_names = store.get_index_names().expect("Failed to list indexes");
    assert_eq!(index_names.len(), 2, "Should have two indexes");

    assert!(
        index_names.contains(&"index1".to_string()),
        "Should contain index1"
    );
    assert!(
        index_names.contains(&"index2".to_string()),
        "Should contain index2"
    );

    println!("✅ Storage configuration works correctly!");
}
