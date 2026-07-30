//! Crash-recovery and checkpoint semantics for the WAL / Tantivy pair.
//!
//! The store keeps two copies of every write: a WAL row in redb and a document in a Tantivy
//! writer buffer. Only the WAL row is durable until `commit_index` runs. The recovery
//! checkpoint (`_recovery_meta`) records how far Tantivy is known to be caught up, and
//! startup trusts it to decide whether replay is needed — so the checkpoint must never claim
//! a sequence that is not actually in Tantivy, and the WAL must never be truncated past it.

use serde_json::json;
use storage::{HybridStore, IndexSchema, StorageConfig, WalOp};
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
        // Keep the steady-state commit threshold far above what these tests write, so a
        // commit only ever happens when the test asks for one.
        default_batch_size: 100_000,
        wal_sync: true,
    }
}

fn open_store(shard_path: &std::path::Path) -> HybridStore {
    HybridStore::new(test_config(shard_path), 1).expect("Failed to open HybridStore")
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
    let (_, total_hits) = store
        .search_documents(index, query, 0, None)
        .expect("search failed");
    total_hits
}

/// A crash before any commit must lose nothing: the WAL still holds every write, and
/// reopening the index replays them into Tantivy.
#[test]
fn uncommitted_writes_are_replayed_after_crash() {
    let temp_dir = TempDir::new().expect("temp dir");
    let shard_path = temp_dir.path();
    let index = "crash_index";

    {
        let store = open_store(shard_path);
        store
            .store_schema_and_cache(index, &IndexSchema::default())
            .expect("store schema");
        write_docs(&store, index, 1..=50);
        // Deliberately no commit and no shutdown: this is the crash.
    }

    let store = open_store(shard_path);
    let plan = store.recover_indices().expect("recovery failed");
    assert_eq!(
        plan.recovered,
        vec![index.to_string()],
        "index with uncommitted WAL entries must be recovered on startup"
    );

    // Recovery leaves the replayed documents in the writer buffer; the normal commit path
    // is what makes them searchable. The seeded operations counter is what lets that
    // commit happen without any new client traffic.
    store.commit_index(index).expect("post-recovery commit");

    assert_eq!(
        search_hit_count(&store, index, "*"),
        50,
        "all 50 documents should be searchable after WAL replay"
    );
}

/// After a commit, the checkpoint advances and the WAL entries it covers are gone, so the
/// next startup skips replay entirely.
#[test]
fn committed_writes_skip_replay_on_restart() {
    let temp_dir = TempDir::new().expect("temp dir");
    let shard_path = temp_dir.path();
    let index = "synced_index";

    {
        let store = open_store(shard_path);
        store
            .store_schema_and_cache(index, &IndexSchema::default())
            .expect("store schema");
        write_docs(&store, index, 1..=25);
        store.commit_index(index).expect("commit");
    }

    let store = open_store(shard_path);
    let plan = store.recover_indices().expect("recovery failed");
    assert!(
        plan.recovered.is_empty(),
        "committed index must not need recovery"
    );
    assert_eq!(
        plan.pending_warmup,
        vec![index.to_string()],
        "every queryable index should be queued for reader warmup"
    );
    assert_eq!(
        search_hit_count(&store, index, "*"),
        25,
        "committed documents survive the restart"
    );
}

/// A graceful shutdown commits pending writes, and must checkpoint what it committed.
/// Without the checkpoint the data is safe but every restart replays the whole WAL tail
/// again, which is the dominant startup cost on a large shard.
#[test]
fn graceful_shutdown_checkpoints_committed_data() {
    let temp_dir = TempDir::new().expect("temp dir");
    let shard_path = temp_dir.path();
    let index = "shutdown_index";

    {
        let store = open_store(shard_path);
        store
            .store_schema_and_cache(index, &IndexSchema::default())
            .expect("store schema");
        write_docs(&store, index, 1..=30);
        // No explicit commit — shutdown is responsible for flushing and checkpointing.
        store.shutdown().expect("shutdown");
    }

    let store = open_store(shard_path);
    let plan = store.recover_indices().expect("recovery failed");
    assert!(
        plan.recovered.is_empty(),
        "shutdown checkpointed its commit, so nothing should need replay"
    );
    assert_eq!(plan.pending_warmup, vec![index.to_string()]);
    assert_eq!(
        search_hit_count(&store, index, "*"),
        30,
        "documents flushed by shutdown are searchable after restart"
    );
}

/// Writes that arrive after a commit are still protected: the checkpoint covers only the
/// committed prefix, so a crash replays the tail and nothing is lost.
#[test]
fn writes_after_commit_are_replayed_after_crash() {
    let temp_dir = TempDir::new().expect("temp dir");
    let shard_path = temp_dir.path();
    let index = "partial_index";

    {
        let store = open_store(shard_path);
        store
            .store_schema_and_cache(index, &IndexSchema::default())
            .expect("store schema");
        write_docs(&store, index, 1..=10);
        store.commit_index(index).expect("commit");
        // These land after the checkpoint and are never committed — the crash window.
        write_docs(&store, index, 11..=20);
    }

    let store = open_store(shard_path);
    let plan = store.recover_indices().expect("recovery failed");
    assert_eq!(
        plan.recovered,
        vec![index.to_string()],
        "the uncommitted tail must trigger recovery"
    );
    store.commit_index(index).expect("post-recovery commit");

    assert_eq!(
        search_hit_count(&store, index, "*"),
        20,
        "committed prefix and replayed tail must both be present"
    );
    for id in 1..=20 {
        assert!(
            store
                .get_by_key(index, &format!("doc-{id}"))
                .expect("get_by_key")
                .is_some(),
            "doc-{id} should be retrievable by key"
        );
    }
}

/// Concurrent first-touch of the same index must not fail.
///
/// Tantivy guards `IndexWriter` creation with a *non-blocking* flock on
/// `.tantivy-writer.lock`, and flock conflicts across file descriptors within a single
/// process. `get_or_create_index` is reachable concurrently from the shard writer thread,
/// the read pool and the startup warmup threads, so without a per-index initialization lock
/// one of them loses the race and gets `LockBusy`.
#[test]
fn concurrent_index_initialization_does_not_race() {
    let temp_dir = TempDir::new().expect("temp dir");
    let shard_path = temp_dir.path();
    let index = "contended_index";

    let store = open_store(shard_path);
    store
        .store_schema_and_cache(index, &IndexSchema::default())
        .expect("store schema");

    let errors = std::sync::Mutex::new(Vec::new());
    std::thread::scope(|scope| {
        for _ in 0..8 {
            scope.spawn(|| {
                if let Err(e) = store.get_or_create_index(index) {
                    errors.lock().unwrap().push(e.to_string());
                }
            });
        }
    });

    let errors = errors.into_inner().unwrap();
    assert!(
        errors.is_empty(),
        "concurrent get_or_create_index must all succeed, got: {errors:?}"
    );
}
