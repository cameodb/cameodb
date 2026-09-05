//! A writer dropped with documents still buffered in it.
//!
//! `force_remove_writer` is the admin escape hatch for a stuck writer, and the orchestrator's
//! eviction path reaches it even when the commit meant to precede it failed. The sequence
//! counter is then advanced past documents that are in redb's WAL but in no Tantivy writer,
//! while `commit_index` derives the sequence it checkpoints — and so the WAL range it
//! truncates — from that counter.
//!
//! The property under test is that those documents survive. Whether by replay or by the
//! checkpoint holding is an implementation detail; losing them is not.

mod common;

use serde_json::json;
use storage::{FieldDef, HybridStore, IndexSchema, StorageConfig, TantivyFieldType, WalOp};
use tempfile::TempDir;

fn test_config(shard_path: &std::path::Path) -> StorageConfig {
    StorageConfig {
        shard_path: shard_path.to_path_buf(),
        indexer_memory_budget: 32 * 1024 * 1024,
        indexer_memory_min_mb: 16,
        indexer_memory_max_mb: 256,
        total_memory_limit_bytes: 4 * 1024 * 1024 * 1024,
        memory_pressure_threshold_percent: 80,
        indexer_num_threads: 1,
        merge_num_threads: 1,
        // No commit ever happens unless a test asks for one.
        default_batch_size: 100_000,
        wal_sync: true,
    }
}

fn open_store(shard_path: &std::path::Path) -> HybridStore {
    HybridStore::new(test_config(shard_path), 1).expect("Failed to open HybridStore")
}

fn searchable_schema() -> IndexSchema {
    let mut schema = IndexSchema::default();
    for name in ["id", "title"] {
        schema.fields.insert(
            name.to_string(),
            FieldDef::new(name.to_string(), TantivyFieldType::Text),
        );
    }
    schema.normalize_after_deserialization();
    schema
}

fn write_docs(store: &HybridStore, index: &str, ids: impl IntoIterator<Item = u32>) {
    for id in ids {
        store
            .apply_write(
                index,
                WalOp::Put {
                    id: format!("doc-{id}"),
                    json_blob: Some(json!({ "title": format!("document {id}"), "n": id })),
                },
            )
            .expect("apply_write failed");
    }
}

fn search_hit_count(store: &HybridStore, index: &str, query: &str) -> usize {
    store
        .search_documents(index, query, 0, None)
        .expect("search failed")
        .total_hits
}

/// Documents buffered in a force-removed writer are searchable once the index commits again.
///
/// The sequence counter stays where those documents left it, so the next commit stamps a
/// checkpoint above them and truncates their WAL range — which is safe only because opening
/// a writer replays the tail first.
#[test]
fn documents_buffered_in_a_force_removed_writer_are_not_lost() {
    let dir = TempDir::new().expect("temp dir");
    let store = open_store(dir.path());
    store
        .store_schema("evict", &searchable_schema())
        .expect("store schema");

    // Buffered in the writer, durable in the WAL, in no committed segment.
    write_docs(&store, "evict", 1..=30);
    assert!(
        store.has_open_writer("evict"),
        "the writes should have opened a writer"
    );

    // The failed-commit-then-evict state: the writer goes away with all 30 still in it.
    assert!(
        store.force_remove_writer("evict"),
        "the writer should have been present to remove"
    );

    // A further write reopens the index and moves the sequence counter past the lost range.
    write_docs(&store, "evict", 31..=31);
    store.commit_index("evict").expect("commit after eviction");

    let hits = search_hit_count(&store, "evict", "title:document");
    assert_eq!(
        hits, 31,
        "all 31 documents should be searchable after the writer was force-removed mid-flight, \
         got {hits}"
    );
}

/// The same documents survive a restart taken after the eviction and the commit that followed
/// it, which is where a checkpoint that had truncated too far would show up as a short index.
#[test]
fn a_restart_after_an_eviction_still_has_every_document() {
    let dir = TempDir::new().expect("temp dir");
    {
        let store = open_store(dir.path());
        store
            .store_schema("evict", &searchable_schema())
            .expect("store schema");

        write_docs(&store, "evict", 1..=30);
        store.force_remove_writer("evict");
        write_docs(&store, "evict", 31..=31);
        store.commit_index("evict").expect("commit after eviction");
    }

    let reopened = open_store(dir.path());
    let hits = search_hit_count(&reopened, "evict", "title:document");
    assert_eq!(
        hits, 31,
        "a restart after an eviction should still find all 31 documents, got {hits}"
    );
}

/// An eviction with no write after it leaves the documents only in the WAL, where startup
/// recovery replays them.
///
/// `recover_indices` is the call the shard makes in phase 1, so this is the boot path.
/// Opening the store alone does not replay, and a search issued before phase 1 finishes is
/// answered without the tail.
#[test]
fn an_eviction_with_no_write_after_it_replays_on_the_next_open() {
    let dir = TempDir::new().expect("temp dir");
    {
        let store = open_store(dir.path());
        store
            .store_schema("evict", &searchable_schema())
            .expect("store schema");

        write_docs(&store, "evict", 1..=20);
        store.force_remove_writer("evict");
        // Nothing else. The store closes with 20 documents in the WAL and none in Tantivy.
    }

    let reopened = open_store(dir.path());
    reopened.recover_indices().expect("startup recovery");
    // Recovery deliberately leaves the replayed tail in the writer's buffer and seeds the
    // operations counter so the normal commit flow flushes it. This stands in for the
    // supervisor's idle commit, which is what does that in a running shard.
    reopened
        .commit_index("evict")
        .expect("commit the recovered tail");
    let hits = search_hit_count(&reopened, "evict", "title:document");
    assert_eq!(
        hits, 20,
        "documents left in the WAL by an eviction should replay on the next open, got {hits}"
    );
}

/// A buffered delete survives the same way, which matters more than a buffered write: a tail
/// that lost it would bring the deleted document back.
#[test]
fn a_delete_buffered_in_a_force_removed_writer_stays_deleted() {
    let dir = TempDir::new().expect("temp dir");
    let store = open_store(dir.path());
    store
        .store_schema("evict", &searchable_schema())
        .expect("store schema");

    write_docs(&store, "evict", 1..=5);
    store.commit_index("evict").expect("commit the base");
    assert_eq!(search_hit_count(&store, "evict", "title:document"), 5);

    // Buffered delete, then the writer disappears under it.
    store
        .apply_write(
            "evict",
            WalOp::Delete {
                id: "doc-3".to_string(),
            },
        )
        .expect("delete");
    store.force_remove_writer("evict");

    write_docs(&store, "evict", 6..=6);
    store.commit_index("evict").expect("commit after eviction");

    let hits = search_hit_count(&store, "evict", "title:document");
    assert_eq!(
        hits, 5,
        "doc-3 should stay deleted and doc-6 should be present, so 5 documents, got {hits}"
    );
    assert!(
        store.get_by_key("evict", "doc-3").expect("get").is_none(),
        "the deleted document should be gone from the key-value store too"
    );
}
