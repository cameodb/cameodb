//! Index warmup: the background work that makes the first query fast.
//!
//! Queries go through the *reader* cache. Recovery and writes go through the *writer* cache.
//! Warmup exists to populate the former, because the first query against a cold index
//! otherwise pays for opening the index, resolving its schema, and faulting in every segment
//! structure it touches.

use serde_json::json;
use storage::{HybridStore, IndexSchema, IndexWarmupState, StorageConfig, WalOp};
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
        default_batch_size: 100_000,
        wal_sync: true,
    }
}

fn open_store(shard_path: &std::path::Path) -> HybridStore {
    HybridStore::new(test_config(shard_path), 1).expect("Failed to open HybridStore")
}

/// Populate `index` with `count` documents and commit them.
fn seed_index(store: &HybridStore, index: &str, count: u32) {
    store
        .store_schema_and_cache(index, &IndexSchema::default())
        .expect("store schema");

    let ops: Vec<WalOp> = (1..=count)
        .map(|id| WalOp::Put {
            id: format!("doc-{id}"),
            json_blob: Some(json!({
                "title": format!("document {id}"),
                "body": "the quick brown fox jumps over the lazy dog",
                "n": id,
            })),
        })
        .collect();

    store.apply_batch(index, ops).expect("apply_batch");
    store.commit_index(index).expect("commit");
}

#[test]
fn warm_index_reports_segments_and_documents() {
    let temp_dir = TempDir::new().expect("temp dir");
    let index = "warm_me";

    {
        let store = open_store(temp_dir.path());
        seed_index(&store, index, 100);
        store.shutdown().expect("shutdown");
    }

    // Fresh process view: nothing cached.
    let store = open_store(temp_dir.path());
    assert!(
        !store.is_index_warm(index),
        "a freshly opened store has no warm indices"
    );

    let stats = store
        .warm_index(index)
        .expect("warm_index failed")
        .expect("index has a Tantivy directory, so it should report stats");

    assert_eq!(stats.index, index);
    assert_eq!(
        stats.num_docs, 100,
        "warmed reader should see all documents"
    );
    assert!(stats.segments > 0, "committed data must produce a segment");
    assert!(store.is_index_warm(index), "index should be marked warm");
}

/// Warmup must populate the cache the *search* path reads, which is the reader cache. The
/// previous implementation warmed writers instead, leaving every first query cold.
#[test]
fn warm_index_makes_queries_serve_from_cache() {
    let temp_dir = TempDir::new().expect("temp dir");
    let index = "query_after_warm";

    {
        let store = open_store(temp_dir.path());
        seed_index(&store, index, 50);
        store.shutdown().expect("shutdown");
    }

    let store = open_store(temp_dir.path());
    store.warm_index(index).expect("warm_index failed");

    // The query must return the same results it would have from cold, and the index must
    // still be warm afterwards (warmup is not consumed by querying).
    let (_, total_hits) = store
        .search_documents(index, "*", 0, None)
        .expect("search failed");
    assert_eq!(total_hits, 50);
    assert!(store.is_index_warm(index));
}

/// Warmup is idempotent and safe to call from several threads, because the orchestrator
/// runs it concurrently with live traffic that may warm the same index on demand.
#[test]
fn warm_index_is_idempotent_and_thread_safe() {
    let temp_dir = TempDir::new().expect("temp dir");
    let index = "contended_warm";

    {
        let store = open_store(temp_dir.path());
        seed_index(&store, index, 40);
        store.shutdown().expect("shutdown");
    }

    let store = open_store(temp_dir.path());
    let errors = std::sync::Mutex::new(Vec::new());

    std::thread::scope(|scope| {
        for _ in 0..8 {
            scope.spawn(|| {
                if let Err(e) = store.warm_index(index) {
                    errors.lock().unwrap().push(e.to_string());
                }
            });
        }
    });

    let errors = errors.into_inner().unwrap();
    assert!(errors.is_empty(), "concurrent warmup failed: {errors:?}");
    assert!(store.is_index_warm(index));
    assert_eq!(
        store
            .search_documents(index, "*", 0, None)
            .expect("search")
            .1,
        40
    );
}

/// An index with a schema but no data has nothing to warm. That is a normal state, not an
/// error, and it must not leave the index stuck in a non-warm state.
#[test]
fn warm_index_handles_index_without_data() {
    let temp_dir = TempDir::new().expect("temp dir");
    let index = "empty_index";

    let store = open_store(temp_dir.path());
    store
        .store_schema_and_cache(index, &IndexSchema::default())
        .expect("store schema");

    let stats = store.warm_index(index).expect("warm_index failed");
    assert!(stats.is_none(), "no Tantivy directory means no stats");
    assert!(
        store.is_index_warm(index),
        "an index with nothing to warm counts as warm"
    );
}

/// The recovery phase must hand every queryable index to the warmup phase — including ones
/// it just recovered, since recovery populates the writer cache and queries read through
/// the reader cache.
#[test]
fn recovery_plan_queues_every_index_for_warmup() {
    let temp_dir = TempDir::new().expect("temp dir");

    {
        let store = open_store(temp_dir.path());
        seed_index(&store, "committed", 20);

        // Written but never committed: this one needs recovery.
        store
            .store_schema_and_cache("uncommitted", &IndexSchema::default())
            .expect("store schema");
        store
            .apply_write(
                "uncommitted",
                WalOp::Put {
                    id: "doc-1".to_string(),
                    json_blob: Some(json!({ "title": "pending" })),
                },
            )
            .expect("apply_write");
        // Crash: no commit, no shutdown.
    }

    let store = open_store(temp_dir.path());
    let plan = store.recover_indices().expect("recover_indices failed");

    assert_eq!(
        plan.recovered,
        vec!["uncommitted".to_string()],
        "only the index with an uncommitted WAL tail needs recovery"
    );
    assert!(plan.failed.is_empty());

    let mut pending = plan.pending_warmup.clone();
    pending.sort();
    assert_eq!(
        pending,
        vec!["committed".to_string(), "uncommitted".to_string()],
        "both indices need their readers warmed, recovered or not"
    );

    let warmed = store.warm_indices(&plan.pending_warmup);
    assert_eq!(warmed, 2, "both indices should warm successfully");

    let states = store.warmup_states();
    assert_eq!(states.get("committed"), Some(&IndexWarmupState::Warm));
    assert_eq!(states.get("uncommitted"), Some(&IndexWarmupState::Warm));
}

/// Warmup is ordered smallest-first so the greatest number of indices become warm soonest.
#[test]
fn warmup_plan_is_ordered_smallest_first() {
    let temp_dir = TempDir::new().expect("temp dir");

    {
        let store = open_store(temp_dir.path());
        seed_index(&store, "small", 10);
        seed_index(&store, "large", 400);
        store.shutdown().expect("shutdown");
    }

    let store = open_store(temp_dir.path());
    let plan = store.recover_indices().expect("recover_indices failed");

    assert_eq!(
        plan.pending_warmup,
        vec!["small".to_string(), "large".to_string()],
        "smaller indices should be warmed before larger ones"
    );
}

/// New segments produced after warmup are warmed automatically, because the warmer is
/// registered on the reader and runs on every new searcher generation. Without that, a
/// long-running node would go cold again as commits and merges produce fresh segments.
#[test]
fn segments_committed_after_warmup_stay_searchable() {
    let temp_dir = TempDir::new().expect("temp dir");
    let index = "growing";

    let store = open_store(temp_dir.path());
    seed_index(&store, index, 20);
    store.warm_index(index).expect("warm_index");
    assert_eq!(
        store
            .search_documents(index, "*", 0, None)
            .expect("search")
            .1,
        20
    );

    // A second batch creates a new segment on commit; the reader reloads and the warmer
    // runs against the new generation.
    let ops: Vec<WalOp> = (21..=40)
        .map(|id| WalOp::Put {
            id: format!("doc-{id}"),
            json_blob: Some(json!({ "title": format!("document {id}") })),
        })
        .collect();
    store.apply_batch(index, ops).expect("apply_batch");
    store.commit_index(index).expect("commit");

    assert_eq!(
        store
            .search_documents(index, "*", 0, None)
            .expect("search")
            .1,
        40,
        "documents from the new segment must be visible"
    );
    assert!(store.is_index_warm(index));
}
