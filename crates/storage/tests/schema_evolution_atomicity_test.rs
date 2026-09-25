//! An evolving write commits its schema in the document's own transaction.
//!
//! Schema evolution used to commit twice: the data transaction, and then a second
//! `Durability::Immediate` transaction for the schema row. The seam between them was
//! acknowledged in the code itself — the error arm logged `CRITICAL: Schema evolution failed
//! after data commit` and returned an error saying the document had already been saved — and it
//! sat on the write path of the feature this engine exists for, a stream teaching an index its
//! own shape. Folding the schema row into the same transaction removes the window and the second
//! fsync at once, because redb makes the pair atomic.
//!
//! A test that cannot crash the process between two commits cannot observe atomicity directly.
//! What it *can* observe is the ordering that came with the fold: the evolved schema used to be
//! written into `schema_cache` before the transaction opened, optimistically, so a write that
//! evolved a field and then failed left the cache holding a field the store had never been told
//! about. Now nothing moves until the commit returns. That is what these tests pin, plus the
//! clean-path guarantee that the schema is in the table — not merely in the cache — the moment
//! `apply_write` returns.

use serde_json::json;
use storage::{FieldDef, HybridStore, IndexSchema, StorageConfig, TantivyFieldType, WalOp};
use tempfile::TempDir;

fn test_config(shard_path: &std::path::Path) -> StorageConfig {
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
        // The interesting half: with `wal_sync` off the data transaction used to be
        // `Durability::None` while the schema transaction was `Immediate`, so an evolving write
        // was the one write whose two halves did not even agree on durability.
        wal_sync: false,
        query: Default::default(),
    }
}

/// An index that declares one indexed `payload` field of type `Bytes`, and nothing else.
///
/// `Bytes` is here because it has a refusal that needs no ceremony to trigger: an element
/// outside 0–255 is not a byte, and the write path refuses rather than silently truncating it.
/// That gives a write that evolves the schema *and then fails*, which is the case the old
/// ordering got wrong.
fn payload_schema() -> IndexSchema {
    let mut schema = IndexSchema::default();
    schema.fields.insert(
        "id".to_string(),
        FieldDef::new("id".to_string(), TantivyFieldType::Text),
    );
    schema.fields.insert(
        "payload".to_string(),
        FieldDef::new("payload".to_string(), TantivyFieldType::Bytes),
    );
    schema.normalize_after_deserialization();
    schema
}

fn open_store(shard_path: &std::path::Path) -> HybridStore {
    HybridStore::new(test_config(shard_path), 1).expect("Failed to open HybridStore")
}

#[test]
fn an_evolving_write_has_its_schema_in_the_table_when_it_returns() {
    let dir = TempDir::new().unwrap();
    let store = open_store(dir.path());
    let index = "stream";

    store
        .store_schema_and_cache(index, &payload_schema())
        .expect("store schema");

    store
        .apply_write(
            index,
            WalOp::Put {
                id: "doc-1".to_string(),
                json_blob: Some(json!({ "payload": [1, 2, 3], "note": "a field nobody declared" })),
            },
        )
        .expect("an evolving write must succeed");

    // `get_schema` reads the redb table, not the cache, so this is the persisted row.
    let stored = store
        .get_schema(index)
        .expect("read schema")
        .expect("schema row must exist");
    assert!(
        stored.fields.contains_key("note"),
        "the evolved field must be in the schema table when apply_write returns, \
         not after a second transaction: {:?}",
        stored.fields.keys().collect::<Vec<_>>()
    );

    // And it survives a reopen, alongside the document it arrived with.
    drop(store);
    let reopened = open_store(dir.path());
    let stored = reopened
        .get_schema(index)
        .expect("read schema")
        .expect("schema row must survive a reopen");
    assert!(stored.fields.contains_key("note"));
    assert!(
        reopened
            .get_by_key(index, "doc-1")
            .expect("read document")
            .is_some(),
        "the document and the schema it evolved must come back together"
    );
}

#[test]
fn a_failed_evolving_write_leaves_the_schema_cache_alone() {
    let dir = TempDir::new().unwrap();
    let store = open_store(dir.path());
    let index = "stream";

    store
        .store_schema_and_cache(index, &payload_schema())
        .expect("store schema");

    // `note` evolves the schema; `payload` then refuses, because 999 is not a byte. The refusal
    // happens while the Tantivy document is built, which is after evolution and before the
    // transaction — exactly the gap the optimistic cache write used to fall into.
    let err = store
        .apply_write(
            index,
            WalOp::Put {
                id: "doc-1".to_string(),
                json_blob: Some(json!({ "payload": [999], "note": "a field nobody declared" })),
            },
        )
        .expect_err("a value that is not a byte must be refused");
    assert!(
        format!("{err}").contains("payload"),
        "the refusal should name the field: {err}"
    );

    let stored = store
        .get_schema(index)
        .expect("read schema")
        .expect("schema row must exist");
    assert!(
        !stored.fields.contains_key("note"),
        "a refused write must not persist the schema it would have evolved"
    );

    let cached = store
        .get_schema_cached(index)
        .expect("read cached schema")
        .expect("cached schema must exist");
    assert!(
        !cached.fields.contains_key("note"),
        "a refused write must not leave the schema cache ahead of the store: {:?}",
        cached.fields.keys().collect::<Vec<_>>()
    );

    // The next reader agrees with the store, which is the whole point of the assertion above.
    assert_eq!(
        cached.fields.contains_key("note"),
        stored.fields.contains_key("note"),
        "cache and store must not disagree about whether a field exists"
    );
}
