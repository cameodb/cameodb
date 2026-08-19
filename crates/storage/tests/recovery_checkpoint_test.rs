//! Crash-recovery and checkpoint semantics for the WAL / Tantivy pair.
//!
//! The store keeps two copies of every write: a WAL row in redb and a document in a Tantivy
//! writer buffer. Only the WAL row is durable until `commit_index` runs. The recovery
//! checkpoint records how far Tantivy is known to be caught up, and startup trusts it to
//! decide whether replay is needed — so it must never claim a sequence that is not actually
//! in Tantivy, and the WAL must never be truncated past it.
//!
//! The checkpoint lives in the Tantivy commit payload, which is what makes it impossible for
//! it to describe segments that are not on disk. A commit then truncates the WAL entries it
//! covers, so an empty WAL is the boot-time signal that an index needs nothing — the property
//! these tests exist to protect, since breaking it either costs a full replay on every boot
//! or silently skips one that was needed.

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

/// A schema whose `title` is actually searchable.
///
/// `IndexSchema::default()` declares nothing, and fields discovered from a document are added
/// as non-indexed — enough for `*` and `get_by_key`, but a `title:...` query against it matches
/// nothing whatever recovery did. Tests that assert on *which version* of a document survived
/// need the field declared up front.
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

fn search_hit_count(store: &HybridStore, index: &str, query: &str) -> usize {
    store
        .search_documents(index, query, 0, None)
        .expect("search failed")
        .total_hits
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

/// Writes made after a clean restart must not reuse sequence numbers the index already
/// spent, because the checkpoint is compared against them.
///
/// A commit truncates the WAL it covers, so a cleanly stopped index reopens with an empty
/// one. Seeding the sequence counter from the WAL alone therefore restarts numbering at zero,
/// and the next crash finds a checkpoint far *above* the reissued tail and concludes there is
/// nothing to replay. redb keeps the documents either way; the search index is where they
/// vanish, which is why this asserts on the query rather than on `get_by_key`.
#[test]
fn writes_after_a_clean_restart_are_replayed_after_a_crash() {
    let temp_dir = TempDir::new().expect("temp dir");
    let shard_path = temp_dir.path();
    let index = "restart_then_crash";

    // First run: build up a checkpoint well above the handful of writes the second run makes.
    {
        let store = open_store(shard_path);
        store
            .store_schema_and_cache(index, &IndexSchema::default())
            .expect("store schema");
        write_docs(&store, index, 1..=100);
        store.shutdown().expect("shutdown");
    }

    // Second run: a few writes, then a crash before any commit.
    {
        let store = open_store(shard_path);
        write_docs(&store, index, 101..=105);
    }

    let store = open_store(shard_path);
    let plan = store.recover_indices().expect("recovery failed");
    assert_eq!(
        plan.recovered,
        vec![index.to_string()],
        "the tail written after the restart is uncommitted and must be replayed"
    );
    store.commit_index(index).expect("post-recovery commit");

    assert_eq!(
        search_hit_count(&store, index, "*"),
        105,
        "documents written after a clean restart must survive the next crash"
    );
}

/// An index whose WAL is empty must cost nothing at boot beyond the check itself.
///
/// This is the property the whole design rests on: recovery time tracks what was in flight
/// when the process stopped, not how much data the node holds. If a checkpoint read ever
/// needs the Tantivy index — a searcher, a `_seq` scan — that stops being true, and the cost
/// returns in proportion to the corpus.
#[test]
fn a_synced_index_is_partitioned_without_opening_tantivy() {
    let temp_dir = TempDir::new().expect("temp dir");
    let shard_path = temp_dir.path();
    let index = "cold_synced";

    {
        let store = open_store(shard_path);
        store
            .store_schema_and_cache(index, &IndexSchema::default())
            .expect("store schema");
        write_docs(&store, index, 1..=40);
        store.commit_index(index).expect("commit");
    }

    let store = open_store(shard_path);
    let plan = store.recover_indices().expect("recovery failed");

    assert!(
        plan.recovered.is_empty() && plan.failed.is_empty(),
        "a synced index must not be replayed"
    );
    assert!(
        !store.has_open_writer(index),
        "partitioning a synced index must not open an IndexWriter for it"
    );
}

/// A tail that has been replayed and committed is not replayed again by the next crash.
///
/// Recovery leaves replayed documents in the writer buffer; the commit that follows is what
/// makes them durable and moves the checkpoint past them. This drives that whole sequence and
/// then crashes again, which is the case where a checkpoint that failed to advance would show
/// up as the same tail being replayed on every boot forever.
///
/// Note what this does *not* cover: nothing here interrupts a replay while it is running. That
/// needs a fault-injection seam — a crash can only be placed between two calls the test makes.
#[test]
fn a_replayed_tail_is_not_replayed_again_after_the_next_crash() {
    let temp_dir = TempDir::new().expect("temp dir");
    let shard_path = temp_dir.path();
    let index = "twice_crashed";

    {
        let store = open_store(shard_path);
        store
            .store_schema_and_cache(index, &IndexSchema::default())
            .expect("store schema");
        write_docs(&store, index, 1..=60);
    }

    // First recovery replays the tail and commits it, but crashes before anything else runs.
    {
        let store = open_store(shard_path);
        store.recover_indices().expect("first recovery");
        store.commit_index(index).expect("commit the replayed tail");
    }

    let store = open_store(shard_path);
    let plan = store.recover_indices().expect("second recovery");
    assert!(
        plan.recovered.is_empty(),
        "the replayed tail was committed and checkpointed, so nothing should replay again"
    );
    assert_eq!(
        search_hit_count(&store, index, "*"),
        60,
        "the once-replayed documents are all present and searchable"
    );
}

/// A commit must leave no WAL entries behind.
///
/// This is the invariant the entire boot path rests on: startup decides an index needs nothing
/// by finding its WAL empty. If a commit ever stopped truncating, every index would be
/// classified as needing replay on every boot and the partition would go back to opening
/// Tantivy for all of them — the exact cost this design exists to remove.
#[test]
fn a_commit_leaves_no_wal_tail() {
    let temp_dir = TempDir::new().expect("temp dir");
    let shard_path = temp_dir.path();
    let index = "wal_drained";

    let store = open_store(shard_path);
    store
        .store_schema_and_cache(index, &IndexSchema::default())
        .expect("store schema");

    write_docs(&store, index, 1..=20);
    assert_eq!(
        store.pending_wal_entries(index).expect("wal depth"),
        20,
        "uncommitted writes must be waiting in the WAL"
    );

    store.commit_index(index).expect("commit");
    assert_eq!(
        store.pending_wal_entries(index).expect("wal depth"),
        0,
        "a commit must truncate every WAL entry it covers"
    );
}

/// A delete that was never committed must still be a delete after recovery.
///
/// Replay handles `WalOp::Delete` by issuing `delete_term` rather than by adding a document,
/// so it is a genuinely different path from a put — and the failure mode is silent: the
/// document simply comes back from the dead in the search index while redb correctly reports
/// it gone, which no test that only writes puts would ever notice.
#[test]
fn deletes_lost_before_a_commit_are_replayed() {
    let temp_dir = TempDir::new().expect("temp dir");
    let shard_path = temp_dir.path();
    let index = "deleted_index";

    {
        let store = open_store(shard_path);
        store
            .store_schema_and_cache(index, &IndexSchema::default())
            .expect("store schema");
        write_docs(&store, index, 1..=10);
        store.commit_index(index).expect("commit the puts");

        // These deletes land after the checkpoint and are never committed.
        for id in [3u32, 7, 9] {
            store
                .apply_write(
                    index,
                    WalOp::Delete {
                        id: format!("doc-{id}"),
                    },
                )
                .expect("delete failed");
        }
    }

    let store = open_store(shard_path);
    let plan = store.recover_indices().expect("recovery failed");
    assert_eq!(
        plan.recovered,
        vec![index.to_string()],
        "the uncommitted deletes must trigger recovery"
    );
    store.commit_index(index).expect("post-recovery commit");

    assert_eq!(
        search_hit_count(&store, index, "*"),
        7,
        "the three replayed deletes must remove their documents from the search index"
    );
    for id in [3u32, 7, 9] {
        assert!(
            store
                .get_by_key(index, &format!("doc-{id}"))
                .expect("get_by_key")
                .is_none(),
            "doc-{id} was deleted in redb and must stay deleted"
        );
    }
    for id in [1u32, 2, 4, 5, 6, 8, 10] {
        assert!(
            store
                .get_by_key(index, &format!("doc-{id}"))
                .expect("get_by_key")
                .is_some(),
            "doc-{id} was never deleted and must survive the replay"
        );
    }
}

/// A batch write is one redb transaction, so a crash loses all of it or none of it — and
/// whichever it is, recovery has to reach the same answer as for single writes.
///
/// Worth its own test because `apply_batch` reserves a contiguous block of sequence numbers
/// and writes the whole block in a single transaction, which is a different code path from
/// `apply_write` on both the write side and the sequence-counter side.
#[test]
fn a_lost_batch_is_replayed_after_a_crash() {
    let temp_dir = TempDir::new().expect("temp dir");
    let shard_path = temp_dir.path();
    let index = "batched_index";

    {
        let store = open_store(shard_path);
        store
            .store_schema_and_cache(index, &IndexSchema::default())
            .expect("store schema");

        let ops: Vec<WalOp> = (1..=40)
            .map(|id| WalOp::Put {
                id: format!("doc-{id}"),
                json_blob: Some(json!({ "title": format!("document {id}"), "n": id })),
            })
            .collect();
        store.apply_batch(index, ops).expect("apply_batch failed");
        // Crash: the batch is durable in redb but was never committed to Tantivy.
    }

    let store = open_store(shard_path);
    let plan = store.recover_indices().expect("recovery failed");
    assert_eq!(
        plan.recovered,
        vec![index.to_string()],
        "an uncommitted batch must trigger recovery"
    );
    store.commit_index(index).expect("post-recovery commit");

    assert_eq!(
        search_hit_count(&store, index, "*"),
        40,
        "every document in the lost batch must be searchable after replay"
    );
    assert_eq!(
        store.pending_wal_entries(index).expect("wal depth"),
        0,
        "the post-recovery commit must drain the replayed tail"
    );
}

/// Several indices with tails recover in one pass.
///
/// Replay runs a thread per index under a process-wide permit gate, so this is the only shape
/// that exercises the gate at all — with a single index it is a permit acquired and released.
#[test]
fn several_indices_replay_their_tails_together() {
    let temp_dir = TempDir::new().expect("temp dir");
    let shard_path = temp_dir.path();
    let indices = ["alpha", "bravo", "charlie", "delta"];

    {
        let store = open_store(shard_path);
        for index in indices {
            store
                .store_schema_and_cache(index, &IndexSchema::default())
                .expect("store schema");
            write_docs(&store, index, 1..=15);
        }
        // Crash with every index holding an uncommitted tail.
    }

    let store = open_store(shard_path);
    let plan = store.recover_indices().expect("recovery failed");

    let mut recovered = plan.recovered.clone();
    recovered.sort();
    assert_eq!(
        recovered,
        indices.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
        "every index with a tail must be recovered"
    );
    assert!(plan.failed.is_empty(), "no index should fail recovery");

    for index in indices {
        store.commit_index(index).expect("post-recovery commit");
        assert_eq!(
            search_hit_count(&store, index, "*"),
            15,
            "{index} must have all its documents after replay"
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

/// A document put and then deleted inside the same uncommitted tail must stay deleted.
///
/// Replay resolves each id against its committed row rather than re-enacting the operations
/// that produced it, so both WAL entries for this document read as "no row, therefore gone".
/// The end state is what a literal replay would have reached, in one step instead of two.
#[test]
fn a_document_put_and_deleted_in_one_tail_stays_deleted() {
    let temp_dir = TempDir::new().expect("temp dir");
    let shard_path = temp_dir.path();
    let index = "put_then_deleted";

    {
        let store = open_store(shard_path);
        store
            .store_schema_and_cache(index, &IndexSchema::default())
            .expect("store schema");
        write_docs(&store, index, 1..=5);
        store
            .apply_write(
                index,
                WalOp::Delete {
                    id: "doc-3".to_string(),
                },
            )
            .expect("delete failed");
        // Crash: doc-3 was written and deleted, neither committed to Tantivy.
    }

    let store = open_store(shard_path);
    store.recover_indices().expect("recovery failed");
    store.commit_index(index).expect("post-recovery commit");

    assert_eq!(
        search_hit_count(&store, index, "*"),
        4,
        "the document deleted within the tail must not be indexed by the replay"
    );
    assert!(
        store
            .get_by_key(index, "doc-3")
            .expect("get_by_key")
            .is_none(),
        "doc-3 must stay deleted in redb too"
    );
}

/// A document deleted and then written again in the same tail must come back.
///
/// The mirror of the case above, and the one that would break if replay treated an id's first
/// appearance as authoritative rather than its committed row.
#[test]
fn a_document_deleted_and_rewritten_in_one_tail_survives() {
    let temp_dir = TempDir::new().expect("temp dir");
    let shard_path = temp_dir.path();
    let index = "deleted_then_rewritten";

    {
        let store = open_store(shard_path);
        store
            .store_schema_and_cache(index, &searchable_schema())
            .expect("store schema");
        write_docs(&store, index, 1..=3);
        store.commit_index(index).expect("commit");

        store
            .apply_write(
                index,
                WalOp::Delete {
                    id: "doc-2".to_string(),
                },
            )
            .expect("delete failed");
        store
            .apply_write(
                index,
                WalOp::Put {
                    id: "doc-2".to_string(),
                    json_blob: Some(json!({ "title": "resurrected", "n": 2 })),
                },
            )
            .expect("rewrite failed");
        // Crash with both operations uncommitted.
    }

    let store = open_store(shard_path);
    store.recover_indices().expect("recovery failed");
    store.commit_index(index).expect("post-recovery commit");

    assert_eq!(
        search_hit_count(&store, index, "*"),
        3,
        "the rewritten document must be back in the index"
    );
    assert_eq!(
        search_hit_count(&store, index, "title:resurrected"),
        1,
        "the replayed document must carry the content it was last written with"
    );
}

/// Repeated updates to one document replay to the last version written.
///
/// Every entry for an id resolves to the same committed row, so the tail collapses to one
/// Tantivy operation — and the version that lands has to be the committed one, not the first
/// the tail happened to mention.
#[test]
fn repeated_updates_replay_as_the_committed_version() {
    let temp_dir = TempDir::new().expect("temp dir");
    let shard_path = temp_dir.path();
    let index = "hot_document";

    {
        let store = open_store(shard_path);
        store
            .store_schema_and_cache(index, &searchable_schema())
            .expect("store schema");

        for version in 1..=25 {
            store
                .apply_write(
                    index,
                    WalOp::Put {
                        id: "doc-hot".to_string(),
                        json_blob: Some(json!({ "title": format!("version{version}") })),
                    },
                )
                .expect("write failed");
        }
        // Crash with 25 uncommitted writes, all to the same document.
    }

    let store = open_store(shard_path);
    store.recover_indices().expect("recovery failed");
    store.commit_index(index).expect("post-recovery commit");

    assert_eq!(
        search_hit_count(&store, index, "*"),
        1,
        "25 writes to one id must leave exactly one document"
    );
    assert_eq!(
        search_hit_count(&store, index, "title:version25"),
        1,
        "the surviving document must be the last version written"
    );
    assert_eq!(
        search_hit_count(&store, index, "title:version24"),
        0,
        "no earlier version may survive the replay"
    );
}
