//! What a batch does with a value the schema cannot hold.
//!
//! `apply_batch` prepares every operation — shadow filtering, the stored-body serialisation and
//! the Tantivy document build — before it opens its write transaction, so a refused value is
//! found before any table is touched. It was once found midway through the transaction, with
//! rows already staged, and the abort is what kept those rows from landing. The batch is
//! all-or-nothing either way, and this pins that: the guarantee belongs to the caller, not to
//! where the check happens to sit.

use serde_json::json;
use storage::{HybridStore, IndexSchema, StorageConfig, WalOp};
use tempfile::TempDir;

const INDEX: &str = "refusals";

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
        default_batch_size: 1000,
        wal_sync: true,
    }
}

/// A facet field, because a facet path that does not start with `/` is refused rather than
/// coerced or skipped — the shortest route to `BadValue::Refuse` from a document a caller could
/// actually send.
fn declared_schema() -> IndexSchema {
    let mut schema: IndexSchema = serde_json::from_value(json!({
        "fields": {
            "id": {"field_type": "text", "indexed": true, "stored": true},
            "body": {"field_type": "text", "indexed": true},
            "path": {"field_type": "facet", "indexed": true}
        }
    }))
    .expect("a schema a caller could PUT");
    schema.normalize_after_deserialization();
    schema
}

fn store(dir: &TempDir) -> HybridStore {
    let store = HybridStore::new(test_config(dir.path()), 1).expect("build a HybridStore");
    store
        .store_schema_and_cache(INDEX, &declared_schema())
        .expect("store the schema");
    store
}

fn put(id: &str, path: &str) -> WalOp {
    WalOp::Put {
        id: id.to_string(),
        json_blob: Some(json!({"id": id, "body": "text", "path": path})),
    }
}

/// One bad value refuses the batch, and none of the batch's other documents is written — not the
/// ones before it, which is the half that a mid-transaction abort was carrying.
#[test]
fn one_refused_value_writes_none_of_the_batch() {
    let dir = TempDir::new().expect("temp dir");
    let store = store(&dir);

    let err = store
        .apply_batch(
            INDEX,
            vec![
                put("good-1", "/a/b"),
                put("good-2", "/a/c"),
                put("bad", "no-leading-slash"),
                put("good-3", "/a/d"),
            ],
        )
        .expect_err("a facet path that is not a path refuses the batch");
    let message = err.to_string();
    assert!(
        message.contains("path"),
        "the refusal names the field it refused: {message}"
    );

    // The documents ahead of the bad one in the batch are the ones a partial write would leave
    // behind, so they are what proves nothing landed.
    for id in ["good-1", "good-2", "good-3", "bad"] {
        assert!(
            store.get_by_key(INDEX, id).expect("read back").is_none(),
            "{id} must not have been written by a refused batch"
        );
    }
}

/// The refusal is not sticky: the same store takes the same documents once the bad one is gone.
#[test]
fn a_refused_batch_leaves_the_index_usable() {
    let dir = TempDir::new().expect("temp dir");
    let store = store(&dir);

    store
        .apply_batch(INDEX, vec![put("a", "/x"), put("b", "not-a-path")])
        .expect_err("refused");

    store
        .apply_batch(INDEX, vec![put("a", "/x"), put("b", "/y")])
        .expect("the same batch lands once the bad value is corrected");

    for id in ["a", "b"] {
        assert!(
            store.get_by_key(INDEX, id).expect("read back").is_some(),
            "{id} is written by the corrected batch"
        );
    }
}
