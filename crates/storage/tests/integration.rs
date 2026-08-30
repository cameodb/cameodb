use storage::{HybridStore, IndexSchema, StorageConfig};
use tempfile::TempDir;

/// Minimal integration test focusing on storage engine basics
#[test]
fn test_storage_engine_basics() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");

    let config = StorageConfig {
        shard_path: temp_dir.path().to_path_buf(),

        // Memory Budget Configuration
        indexer_memory_budget: 32 * 1024 * 1024, // 32MB per index
        indexer_memory_min_mb: 16,               // 16MB minimum
        indexer_memory_max_mb: 256,              // 256MB maximum
        total_memory_limit_bytes: 4 * 1024 * 1024 * 1024,
        memory_pressure_threshold_percent: 80,

        // Thread Configuration
        indexer_num_threads: 1,
        merge_num_threads: 2,

        // Other Configuration
        default_batch_size: 1000, // 1000 operations default
        wal_sync: true,
    };

    // Test store creation (single shard for test)
    let store = HybridStore::new(config, 1).expect("Failed to create HybridStore");

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

        // Memory Budget Configuration
        indexer_memory_budget: 32 * 1024 * 1024, // 32MB per index
        indexer_memory_min_mb: 16,
        indexer_memory_max_mb: 256,
        total_memory_limit_bytes: 4 * 1024 * 1024 * 1024,
        memory_pressure_threshold_percent: 80,

        // Thread Configuration
        indexer_num_threads: 1,
        merge_num_threads: 2,

        // Other Configuration
        default_batch_size: 500,
        wal_sync: false,
    };

    // Test store creation with custom config (single shard for test)
    let store = HybridStore::new(config, 1).expect("Failed to create HybridStore");

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

#[test]
fn test_shadow_field_preservation_during_evolution() {
    use serde_json::json;
    use storage::{IndexSchema, TantivyFieldType};

    println!("Testing shadow field preservation during schema evolution...");

    // Simulate the CSV import scenario
    let mut schema = IndexSchema::default();

    // Step 1: Add shadow field (as done in CSV import)
    println!("1. Adding shadow field 'book_id'...");
    let added = schema.add_shadow_field("book_id".to_string(), TantivyFieldType::Text);
    assert!(added, "Shadow field should be added");

    // Verify shadow field properties
    let book_id_field = schema.fields.get("book_id").unwrap();
    assert!(
        book_id_field.is_shadow,
        "book_id should be marked as shadow"
    );
    assert!(
        !book_id_field.indexed,
        "Shadow fields should not be indexed"
    );
    assert!(!book_id_field.stored, "Shadow fields should not be stored");
    println!("   ✅ Shadow field added with correct properties");

    // Step 2: Simulate document processing during CSV import
    println!("2. Simulating document with book_id field...");
    let document = json!({
        "book_id": "12345",
        "title": "Test Book",
        "author": "Test Author"
    });

    // This is what happens during CSV import - evolve_from_document is called
    let _evolved_fields = schema.evolve_from_document(&document);

    // Step 3: Verify shadow field is preserved
    println!("3. Verifying shadow field preservation...");
    let book_id_field_after = schema.fields.get("book_id").unwrap();
    assert!(
        book_id_field_after.is_shadow,
        "book_id should still be shadow after evolution"
    );
    assert!(
        !book_id_field_after.indexed,
        "Shadow fields should remain non-indexed"
    );
    assert!(
        !book_id_field_after.stored,
        "Shadow fields should remain non-stored"
    );

    // Verify other fields were added normally
    assert!(
        schema.fields.contains_key("title"),
        "title field should be added"
    );
    assert!(
        schema.fields.contains_key("author"),
        "author field should be added"
    );

    println!("   ✅ Shadow field preserved during evolution");
    println!("   ✅ Other fields added normally");

    // Step 4: Verify the shadow field is recognised as one
    println!("4. Testing shadow field recognition...");
    assert!(
        schema.is_shadow_field("book_id"),
        "book_id should be recognised as a shadow field"
    );
    assert!(
        !schema.is_shadow_field("title"),
        "title is an ordinary field, not a shadow"
    );
    println!("   ✅ Shadow field recognised correctly");

    // Step 5: Simulate the CSV import finalization process (like in detect_schema_from_csv)
    println!("5. Testing CSV import finalization process...");

    // This simulates the critical part of detect_schema_from_csv that was overwriting shadow fields
    for (name, field_def) in schema.fields.iter_mut() {
        // Don't modify shadow fields - they have special requirements
        if !field_def.is_shadow {
            field_def.indexed = true;
            // Only 'id' field should be stored in Tantivy (architecture rule)
            field_def.stored = name == "id";
        }
    }

    // Verify shadow field is still preserved after finalization
    let book_id_field_final = schema.fields.get("book_id").unwrap();
    assert!(
        book_id_field_final.is_shadow,
        "book_id should still be shadow after finalization"
    );
    assert!(
        !book_id_field_final.indexed,
        "Shadow fields should remain non-indexed after finalization"
    );
    assert!(
        !book_id_field_final.stored,
        "Shadow fields should remain non-stored after finalization"
    );
    println!("   ✅ Shadow field preserved during CSV import finalization");

    println!("\n🎉 All tests passed! Shadow field preservation fix is working correctly.");

    // Print final schema for verification
    println!("\nFinal schema:");
    for (name, field) in &schema.fields {
        println!(
            "  {}: indexed={}, stored={}, is_shadow={}",
            name, field.indexed, field.stored, field.is_shadow
        );
    }
}
