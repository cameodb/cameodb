//! The open-index cap: what bounds resident memory against the number of index *names*.
//!
//! Every index this shard holds open costs an indexing arena and, with it,
//! `indexer_num_threads + merge_num_threads` OS threads. Nothing about that cost is
//! proportional to how much data the index holds, so on a node whose tenants pick their own
//! index names — a tenant-per-index or date-partitioned layout — the footprint tracks a number
//! the node does not choose.
//!
//! The three properties below are the ones worth holding the implementation to: the set stays
//! bounded, the index that goes is the coldest one, and an evicted index loses nothing.

use serde_json::json;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use storage::{FieldDef, HybridStore, IndexSchema, StorageConfig, TantivyFieldType, WalOp};
use tempfile::TempDir;

/// `max_open_indexes` is a node-wide figure; one shard makes it this store's figure too.
fn capped_config(shard_path: &std::path::Path, cap: usize) -> StorageConfig {
    StorageConfig {
        shard_path: shard_path.to_path_buf(),
        max_open_indexes: cap,
        indexer_memory_budget: 16 * 1024 * 1024,
        indexer_memory_min_mb: 16,
        indexer_memory_max_mb: 64,
        total_memory_limit_bytes: 1024 * 1024 * 1024,
        memory_pressure_threshold_percent: 80,
        indexer_num_threads: 1,
        merge_num_threads: 1,
        default_batch_size: 100_000,
        wal_sync: true,
        commit_interval_ms: 0,
        query: Default::default(),
    }
}

fn schema() -> IndexSchema {
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

fn write_one(store: &HybridStore, index: &str, id: &str) {
    store
        .store_schema_and_cache(index, &schema())
        .expect("schema");
    store
        .apply_write(
            index,
            WalOp::Put {
                id: id.to_string(),
                json_blob: Some(json!({ "title": format!("in {index}") })),
            },
        )
        .expect("write");
}

/// Writing to far more index names than the cap leaves the cap's worth open.
///
/// The assertion is on the *resident set*, not on the writers map alone: an index is closed
/// when every structure keyed by its name is gone, and a check that only counted writers would
/// pass while readers, schemas and field handles stayed behind.
#[test]
fn the_open_set_stays_within_its_cap() {
    let dir = TempDir::new().unwrap();
    let cap = 4;
    let store = HybridStore::new(capped_config(dir.path(), cap), 1).expect("store");

    for i in 0..40 {
        write_one(&store, &format!("tenant-{i}"), "doc-1");
        assert!(
            store.open_index_count() <= cap,
            "the open set passed its cap at tenant-{i}: {} > {cap}",
            store.open_index_count()
        );
    }

    assert_eq!(
        store.open_index_count(),
        cap,
        "forty indexes later the shard should be holding exactly its cap"
    );
}

/// An index that was evicted still has its documents when it is next opened.
///
/// This is the property that makes the cap safe to turn on. Eviction commits before it drops,
/// and what a failed or partial commit leaves behind is still in redb's WAL, so reopening
/// replays it. Either route is fine; losing the document is not.
#[test]
fn an_evicted_index_keeps_its_documents() {
    let dir = TempDir::new().unwrap();
    let store = HybridStore::new(capped_config(dir.path(), 2), 1).expect("store");

    write_one(&store, "cold", "doc-cold");
    store.commit_index("cold").expect("commit");

    // Push it out with traffic to other names.
    for i in 0..10 {
        write_one(&store, &format!("hot-{i}"), "doc-1");
    }
    assert!(
        !store.is_index_open("cold"),
        "ten other indexes should have evicted this one"
    );

    // Reopening it is what the next reference does anyway.
    let found = store
        .get_by_key("cold", "doc-cold")
        .expect("read after eviction");
    assert!(
        found.is_some(),
        "an evicted index must keep what was written to it"
    );
}

/// The index that goes is the least recently used one, and using an index counts as use.
///
/// Without the touch on the read path this fails: `kept` would be evicted precisely because it
/// had only ever been read, which is the workload a search-heavy tenant has.
#[test]
fn the_coldest_index_is_the_one_evicted() {
    let dir = TempDir::new().unwrap();
    let store = HybridStore::new(capped_config(dir.path(), 3), 1).expect("store");

    write_one(&store, "kept", "doc-1");
    write_one(&store, "doomed", "doc-1");
    write_one(&store, "filler", "doc-1");
    assert_eq!(store.open_index_count(), 3);

    // Touch `kept` so `doomed` is the coldest of the three.
    let _ = store.get_by_key("kept", "doc-1").expect("read");

    // One more index than the cap allows.
    write_one(&store, "newcomer", "doc-1");

    assert!(
        store.is_index_open("kept"),
        "the index used most recently should have survived"
    );
    assert!(
        !store.is_index_open("doomed"),
        "the index used least recently should have been the one to go"
    );
}

/// An index closed by another thread's admission loses none of the writes in flight on it.
///
/// The close runs on whichever thread admits the next index — here a search, as on the read
/// pool — and it used to find the writer mutex free for most of a write, which took it only
/// around `add_document`. A close landing between a batch reserving its sequences and adding its
/// documents committed without them, checkpointed their sequences as durable, truncated their
/// WAL entries, and dropped the writer they were then added to. The documents stayed in redb —
/// which is why `an_evicted_index_keeps_its_documents`, reading by key, never saw it — and were
/// gone from the search index for good: the replay that should have restored them had had its
/// WAL truncated. So this counts what a *search* finds.
#[test]
fn closing_an_index_from_another_thread_loses_no_write_in_flight() {
    const BATCHES: usize = 150;
    const PER_BATCH: usize = 4;

    let dir = TempDir::new().unwrap();
    // A cap of one: every search of `other` closes `busy`, and every write to `busy` closes
    // `other`, so the two threads contend for the open set on every operation.
    let store = Arc::new(HybridStore::new(capped_config(dir.path(), 1), 1).expect("store"));
    store
        .store_schema_and_cache("busy", &schema())
        .expect("schema");
    write_one(&store, "other", "doc-1");
    store.commit_index("other").expect("commit");

    let stop = Arc::new(AtomicBool::new(false));
    let searcher = {
        let (store, stop) = (Arc::clone(&store), Arc::clone(&stop));
        std::thread::spawn(move || {
            let mut searches = 0usize;
            while !stop.load(Ordering::Relaxed) {
                store
                    .search_documents("other", "title:other", 1, None)
                    .expect("search other");
                searches += 1;
            }
            searches
        })
    };

    for batch in 0..BATCHES {
        let ops = (0..PER_BATCH)
            .map(|i| WalOp::Put {
                id: format!("b{batch}-{i}"),
                json_blob: Some(json!({ "title": "in busy" })),
            })
            .collect();
        store.apply_batch("busy", ops).expect("batch");
    }
    stop.store(true, Ordering::Relaxed);
    let searches = searcher.join().expect("searcher thread");
    assert!(searches > 0, "the searcher never ran, so nothing contended");

    // Whatever is still buffered, committed; an index already closed has nothing pending.
    store.commit_index("busy").expect("final commit");
    let found = store
        .search_documents("busy", "title:busy", 0, None)
        .expect("search busy")
        .total_hits;
    assert_eq!(
        found,
        BATCHES * PER_BATCH,
        "every acknowledged write must be searchable after closes from another thread"
    );
}
