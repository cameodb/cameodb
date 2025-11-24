use serde_json::json;
use storage::{HybridStore, StorageConfig, WalOp};
use tempfile::TempDir;

/// Integration test as mandated: Multi-tenant storage with index isolation
#[test]
fn test_multi_tenant_storage_integration() {
    // Use tempfile to create a shared shard
    let temp_dir = TempDir::new().expect("Failed to create temp directory");

    let config = StorageConfig {
        shard_path: temp_dir.path().to_path_buf(),
        writer_memory_budget: 32 * 1024 * 1024, // 32MB per index
        writer_memory_min_mb: 16,               // 16MB minimum
        writer_memory_max_mb: 256,              // 256MB maximum
        default_batch_size: 1000,               // 1000 operations default
        wal_sync: true,
    };

    // Create HybridStore
    let store = HybridStore::new(config).expect("Failed to create HybridStore");

    // Write Doc A to "index_1"
    let doc_a = WalOp::Put {
        id: "docA".to_string(),
        body: "content for index1".to_string(),
        json_blob: Some(json!({"source": "index_1", "type": "test"})),
    };
    let seq_a = store
        .apply_write("index_1", doc_a)
        .expect("Failed to write to index_1");
    assert_eq!(seq_a, 1, "First document should have sequence ID 1");

    // Write Doc B to "index_2"
    let doc_b = WalOp::Put {
        id: "docB".to_string(),
        body: "content for index2".to_string(),
        json_blob: Some(json!({"source": "index_2", "type": "test"})),
    };
    let seq_b = store
        .apply_write("index_2", doc_b)
        .expect("Failed to write to index_2");
    assert_eq!(
        seq_b, 1,
        "First document in index_2 should also have sequence ID 1 (independent)"
    );

    // Verify directories indices/index_1 and indices/index_2 exist
    let indices_dir = temp_dir.path().join("indices");
    let index1_dir = indices_dir.join("index_1");
    let index2_dir = indices_dir.join("index_2");

    assert!(indices_dir.exists(), "Indices directory should exist");
    assert!(index1_dir.exists(), "index_1 directory should exist");
    assert!(index2_dir.exists(), "index_2 directory should exist");
    assert!(index1_dir.is_dir(), "index_1 should be a directory");
    assert!(index2_dir.is_dir(), "index_2 should be a directory");

    // Verify data isolation - documents exist in their respective indices
    let data_a = store
        .get_by_key("index_1", "docA")
        .expect("Failed to get docA from index_1");
    assert!(data_a.is_some(), "docA should exist in index_1");

    let data_b = store
        .get_by_key("index_2", "docB")
        .expect("Failed to get docB from index_2");
    assert!(data_b.is_some(), "docB should exist in index_2");

    // Verify cross-index isolation - docA should not exist in index_2
    let cross_check = store
        .get_by_key("index_2", "docA")
        .expect("Cross-index check should not fail");
    assert!(cross_check.is_none(), "docA should not exist in index_2");

    // Call delete_index_data("index_1")
    store
        .delete_index_data("index_1")
        .expect("Failed to delete index_1 data");

    // Verify indices/index_1 directory is GONE
    assert!(!index1_dir.exists(), "index_1 directory should be deleted");

    // Verify indices/index_2 directory remains
    assert!(index2_dir.exists(), "index_2 directory should still exist");

    // Verify Redb still operates for "index_2"
    let data_b_after_delete = store
        .get_by_key("index_2", "docB")
        .expect("Failed to get docB after delete");
    assert!(
        data_b_after_delete.is_some(),
        "docB should still exist in index_2 after deleting index_1"
    );

    // Verify index_1 data is truly gone
    let data_a_after_delete = store
        .get_by_key("index_1", "docA")
        .expect("Query should not fail even for deleted index");
    assert!(
        data_a_after_delete.is_none(),
        "docA should not exist after index deletion"
    );

    // Write another document to index_2 to ensure it still works
    let doc_c = WalOp::Put {
        id: "docC".to_string(),
        body: "another document".to_string(),
        json_blob: None,
    };
    let seq_c = store
        .apply_write("index_2", doc_c)
        .expect("Failed to write docC to index_2");
    assert_eq!(
        seq_c, 2,
        "Second document in index_2 should have sequence ID 2"
    );

    // Verify both documents exist in index_2
    let data_b_final = store
        .get_by_key("index_2", "docB")
        .expect("Failed to get docB final check");
    let data_c_final = store
        .get_by_key("index_2", "docC")
        .expect("Failed to get docC final check");
    assert!(data_b_final.is_some(), "docB should still exist");
    assert!(data_c_final.is_some(), "docC should exist");

    // Verify directory structure
    assert!(
        temp_dir.path().join("store.redb").exists(),
        "Shared redb file should exist"
    );
    assert!(indices_dir.exists(), "Indices directory should exist");

    // temp_dir is automatically cleaned up when dropped
}

/// Test persistence and WAL recovery for multi-tenant indices
#[test]
fn test_multi_tenant_persistence_and_sequence() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");

    let config = StorageConfig {
        shard_path: temp_dir.path().to_path_buf(),
        writer_memory_budget: 32 * 1024 * 1024, // 32MB per index
        writer_memory_min_mb: 16,               // 16MB minimum
        writer_memory_max_mb: 256,              // 256MB maximum
        default_batch_size: 1000,               // 1000 operations default
        wal_sync: true,
    };

    // First session: create store and add data to multiple indices
    {
        let store = HybridStore::new(config.clone()).expect("Failed to create HybridStore");

        // Write to multiple indices with various documents
        let seq1 = store
            .apply_write(
                "idx1",
                WalOp::Put {
                    id: "doc1".to_string(),
                    body: "content1".to_string(),
                    json_blob: Some(json!({"session": "first"})),
                },
            )
            .expect("Failed to write to idx1");

        let seq2 = store
            .apply_write(
                "idx2",
                WalOp::Put {
                    id: "doc1".to_string(), // Same ID, different index
                    body: "content2".to_string(),
                    json_blob: Some(json!({"session": "first"})),
                },
            )
            .expect("Failed to write to idx2");

        let seq3 = store
            .apply_write(
                "idx1",
                WalOp::Put {
                    id: "doc2".to_string(),
                    body: "content3".to_string(),
                    json_blob: None,
                },
            )
            .expect("Failed to write second doc to idx1");

        // Verify sequence independence per index
        assert_eq!(seq1, 1, "First doc in idx1 should have seq 1");
        assert_eq!(seq2, 1, "First doc in idx2 should have seq 1");
        assert_eq!(seq3, 2, "Second doc in idx1 should have seq 2");

        // Test deletion in one index
        store
            .apply_write(
                "idx1",
                WalOp::Delete {
                    id: "doc1".to_string(),
                },
            )
            .expect("Failed to delete from idx1");

        // Verify deletion worked
        let deleted = store.get_by_key("idx1", "doc1").expect("Query should work");
        assert!(deleted.is_none(), "doc1 should be deleted from idx1");

        // Verify other index unaffected
        let other_index = store.get_by_key("idx2", "doc1").expect("Query should work");
        assert!(other_index.is_some(), "doc1 should still exist in idx2");
    }

    // Second session: reopen store and verify persistence
    {
        let store = HybridStore::new(config).expect("Failed to reopen HybridStore");

        // Verify data persists correctly
        let idx1_doc2 = store
            .get_by_key("idx1", "doc2")
            .expect("Failed to get idx1 doc2");
        assert!(idx1_doc2.is_some(), "doc2 should persist in idx1");

        let idx2_doc1 = store
            .get_by_key("idx2", "doc1")
            .expect("Failed to get idx2 doc1");
        assert!(idx2_doc1.is_some(), "doc1 should persist in idx2");

        // Verify deletion persisted
        let deleted_doc = store.get_by_key("idx1", "doc1").expect("Query should work");
        assert!(
            deleted_doc.is_none(),
            "Deletion should persist across restarts"
        );

        // Add new documents to verify sequence counters resumed correctly
        let new_seq1 = store
            .apply_write(
                "idx1",
                WalOp::Put {
                    id: "new_doc".to_string(),
                    body: "new content".to_string(),
                    json_blob: None,
                },
            )
            .expect("Failed to write new doc to idx1");

        // Sequence should continue from where it left off (was at 3 after delete)
        assert_eq!(
            new_seq1, 4,
            "Sequence should resume correctly after restart"
        );
    }
}
