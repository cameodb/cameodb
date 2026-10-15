//! What redb has to do to reopen a shard's store after an unclean stop.
//!
//! redb marks its file as needing recovery while it is open and clears the mark on a clean
//! close. Opened again without the mark cleared, it either loads the allocator state the last
//! commit recorded — `quick_repair` — or repairs the file, walking every page of it. The
//! repair costs time in proportion to the data the shard holds, so `quick_repair` decides
//! whether a restart after a crash or an OOM kill is milliseconds or minutes.
//!
//! The crash is a copy of the file taken while the store still has it open: the state a
//! killed process leaves on disk. Dropping the store instead would close redb cleanly, and no
//! setting would ever be exercised.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use serde_json::json;
use storage::{HybridStore, IndexSchema, StorageConfig, WalOp};
use tempfile::TempDir;

fn test_config(shard_path: &std::path::Path, quick_repair: bool) -> StorageConfig {
    StorageConfig {
        max_open_indexes: 0,
        shard_path: shard_path.to_path_buf(),
        indexer_memory_budget: 32 * 1024 * 1024,
        indexer_memory_min_mb: 16,
        indexer_memory_max_mb: 256,
        total_memory_limit_bytes: 4 * 1024 * 1024 * 1024,
        memory_pressure_threshold_percent: 80,
        indexer_num_threads: 1,
        merge_num_threads: 1,
        default_batch_size: 100_000,
        wal_sync: true,
        quick_repair,
        commit_interval_ms: 0,
        query: Default::default(),
    }
}

/// Writes and commits through a live store, then copies its redb file before the store is
/// dropped, and returns how many times redb invoked its repair callback opening the copy.
fn repair_calls_after_crash(quick_repair: bool) -> usize {
    let temp_dir = TempDir::new().expect("temp dir");
    let shard_path = temp_dir.path().join("shard");
    let crashed = temp_dir.path().join("crashed.redb");
    let index = "crash_repair";

    let store =
        HybridStore::new(test_config(&shard_path, quick_repair), 1).expect("open HybridStore");
    store
        .store_schema_and_cache(index, &IndexSchema::default())
        .expect("store schema");
    for id in 0..200u32 {
        store
            .apply_write(
                index,
                WalOp::Put {
                    id: format!("doc-{id}"),
                    json_blob: Some(json!({ "title": format!("document {id}"), "n": id })),
                },
            )
            .expect("apply_write");
    }
    // A commit writes the checkpoint transaction too, so every kind of write transaction the
    // store makes has run before the copy.
    store.commit_index(index).expect("commit");
    std::fs::copy(shard_path.join("store.redb"), &crashed).expect("copy the open store");
    drop(store);

    let calls = Arc::new(AtomicUsize::new(0));
    let mut builder = redb::Builder::new();
    builder.set_repair_callback({
        let calls = Arc::clone(&calls);
        move |_| {
            calls.fetch_add(1, Ordering::Relaxed);
        }
    });
    builder.open(&crashed).expect("open the crashed copy");
    calls.load(Ordering::Relaxed)
}

#[test]
fn quick_repair_reopens_a_crashed_store_without_repairing_it() {
    assert_eq!(
        repair_calls_after_crash(true),
        0,
        "with quick_repair every commit records the allocator state, so the reopen loads it"
    );
}

#[test]
fn without_quick_repair_a_crashed_store_is_repaired() {
    assert!(
        repair_calls_after_crash(false) > 0,
        "without quick_repair the reopen must repair the file; if this passes with 0, the \
         copy is not reproducing a crash and the test above proves nothing"
    );
}
