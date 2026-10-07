//! Changing a field's `indexed` flag, which is what `PATCH /api/{index}/_schema` exists to do.
//!
//! A field that first appears inside a written document is recorded as non-indexed, on the
//! stated expectation that it "can be promoted to indexed later". Promoting it used to fail
//! outright: persisting any schema against an open index stranded its writer on the Tantivy
//! lockfile, so every such edit answered `500`.
//!
//! What the promotion means is the subtler half. The Tantivy schema is fixed when the index is
//! built, so a newly declared field has no column until the index data is rebuilt from the
//! schema — which `delete_index_data(delete_schema = false)` plus a re-ingest does. The edit is
//! therefore a *declaration*, applied and flagged rather than refused, and these tests walk that
//! round trip: declare, observe that the clause matches nothing and says so, rebuild, find it.

use std::collections::HashMap;

use storage::{FieldDef, HybridStore, IndexSchema, StorageConfig, TantivyFieldType, WalOp};
use tempfile::TempDir;

fn config(path: std::path::PathBuf) -> StorageConfig {
    StorageConfig {
        max_open_indexes: 0,
        shard_path: path,
        indexer_memory_budget: 32 * 1024 * 1024,
        indexer_memory_min_mb: 16,
        indexer_memory_max_mb: 256,
        total_memory_limit_bytes: 2048 * 1024 * 1024,
        memory_pressure_threshold_percent: 80,
        indexer_num_threads: 1,
        merge_num_threads: 1,
        default_batch_size: 100_000,
        wal_sync: true,
        commit_interval_ms: 0,
        query: Default::default(),
    }
}

fn indexed_text(name: &str) -> FieldDef {
    let mut def = FieldDef::new(name.to_string(), TantivyFieldType::Text);
    def.indexed = true;
    def.stored = true;
    def
}

/// Mark `field` indexed or not, as a node stores an agreed schema: the edited schema, whole.
fn set_indexed(store: &HybridStore, index: &str, field: &str, indexed: bool) {
    let mut schema = store.get_schema(index).unwrap().unwrap();
    schema.fields.get_mut(field).unwrap().indexed = indexed;
    store.store_schema_and_cache(index, &schema).unwrap();
}

/// An index holding one document, with `title` indexed from the start and `author` arriving only
/// inside the document — so `author` is a discovered field, the case promotion is written for.
fn store_with_a_discovered_field(temp: &TempDir, index: &str) -> HybridStore {
    let store = HybridStore::new(config(temp.path().to_path_buf()), 1).unwrap();

    let mut fields = HashMap::new();
    fields.insert("title".to_string(), indexed_text("title"));

    let schema = IndexSchema {
        fields,
        ..Default::default()
    };
    store.store_schema_and_cache(index, &schema).unwrap();

    store
        .apply_write(
            index,
            WalOp::Put {
                id: "d1".to_string(),
                json_blob: Some(serde_json::json!({
                    "title": "Rust in Anger",
                    "author": "hoare",
                })),
            },
        )
        .unwrap();
    store.commit_index(index).unwrap();

    store
}

/// The discovered field is recorded, and recorded as non-indexed. This is the premise the other
/// two tests rest on, so it is asserted rather than assumed.
#[test]
fn a_field_first_seen_in_a_document_is_recorded_as_non_indexed() {
    let temp = TempDir::new().unwrap();
    let store = store_with_a_discovered_field(&temp, "docs");

    let schema = store.get_schema("docs").unwrap().expect("schema persisted");
    let author = schema
        .fields
        .get("author")
        .expect("`author` was discovered from the document");

    assert!(
        !author.indexed,
        "a discovered field is created non-indexed, so that it costs no Tantivy schema change"
    );
}

/// Persisting a schema against an index whose writer is already open makes the *next* acquisition
/// of that writer fail.
///
/// `store_schema_and_cache` evicts the field cache, and `get_or_create_index`'s fast path needs
/// both the writer and the fields, so evicting one of them sends a live index down the path that
/// opens a second `IndexWriter` — against a lockfile the first one still holds.
#[test]
fn persisting_a_schema_does_not_strand_a_live_writer() {
    let temp = TempDir::new().unwrap();
    let store = store_with_a_discovered_field(&temp, "docs");

    // The writer for `docs` is live at this point: `apply_write` opened it and it is cached.
    let mut schema = store.get_schema("docs").unwrap().unwrap();
    schema.fields.get_mut("author").unwrap().indexed = true;
    store.store_schema_and_cache("docs", &schema).unwrap();

    let reacquired = store.get_or_create_index("docs");

    assert!(
        reacquired.is_ok(),
        "reacquiring a writer that is already cached must not reopen the index: {:?}",
        reacquired.err()
    );
}

/// Marking a field indexed that the built index has no column for is applied, and flagged.
///
/// The stored schema is a declaration and the Tantivy index is built from it, so this edit is the
/// first step of declare-then-reingest — the workflow the next test walks end to end. Refusing it
/// would block the only way such a field is ever made searchable.
#[test]
fn promoting_a_discovered_field_is_applied_and_reported_as_pending() {
    let temp = TempDir::new().unwrap();
    let store = store_with_a_discovered_field(&temp, "docs");

    set_indexed(&store, "docs", "author", true);
    assert_eq!(
        store
            .unbuilt_fields("docs", &["author".to_string(), "title".to_string()])
            .unwrap(),
        vec!["author".to_string()],
        "the caller has to be told it is not searchable yet"
    );

    let persisted = store.get_schema("docs").unwrap().unwrap();
    assert!(
        persisted.fields["author"].indexed,
        "the declaration must be saved, or the rebuild has nothing to build from"
    );
}

/// Declare, rebuild, and the field is searchable. This is the whole reason the edit is allowed.
///
/// `delete_index_data` with `delete_schema = false` drops the documents and keeps the
/// declaration, so the next write rebuilds the Tantivy index from a schema that now has the
/// field in it.
#[test]
fn a_promoted_field_becomes_searchable_once_the_index_is_rebuilt() {
    let temp = TempDir::new().unwrap();
    let store = store_with_a_discovered_field(&temp, "docs");

    set_indexed(&store, "docs", "author", true);

    // Before the rebuild the clause matches nothing — and says so rather than passing silently.
    let before = store
        .search_documents("docs", "author:hoare", 10, None)
        .unwrap();
    assert!(before.hits.is_empty());
    assert!(
        !before.discarded.is_empty(),
        "a clause that cannot match must be reported, not dropped quietly"
    );

    store.delete_index_data("docs", false).unwrap();
    store
        .apply_write(
            "docs",
            WalOp::Put {
                id: "d1".to_string(),
                json_blob: Some(serde_json::json!({
                    "title": "Rust in Anger",
                    "author": "hoare",
                })),
            },
        )
        .unwrap();
    store.commit_index("docs").unwrap();

    let after = store
        .search_documents("docs", "author:hoare", 10, None)
        .unwrap();
    assert_eq!(after.hits.len(), 1, "the rebuild should make it searchable");
    assert!(after.discarded.is_empty(), "{:?}", after.discarded);
}

/// Before the index is materialised there is no Tantivy schema to contradict, so the flag is
/// simply the schema the index will be built from.
#[test]
fn promotion_is_allowed_while_the_index_is_still_unmaterialised() {
    let temp = TempDir::new().unwrap();
    let store = HybridStore::new(config(temp.path().to_path_buf()), 1).unwrap();

    let mut fields = HashMap::new();
    fields.insert("title".to_string(), indexed_text("title"));
    let mut author = indexed_text("author");
    author.indexed = false;
    fields.insert("author".to_string(), author);

    let schema = IndexSchema {
        fields,
        ..Default::default()
    };
    store.store_schema_and_cache("docs", &schema).unwrap();

    set_indexed(&store, "docs", "author", true);
    assert!(
        store
            .unbuilt_fields("docs", &["author".to_string()])
            .unwrap()
            .is_empty(),
        "nothing is built yet, so the first write builds the column"
    );
    assert!(store.get_schema("docs").unwrap().unwrap().fields["author"].indexed);
}

/// Demotion is the direction that does work, and it takes effect on the next write.
#[test]
fn demoting_an_indexed_field_stops_new_documents_being_indexed_into_it() {
    let temp = TempDir::new().unwrap();
    let store = store_with_a_discovered_field(&temp, "docs");

    set_indexed(&store, "docs", "title", false);

    store
        .apply_write(
            "docs",
            WalOp::Put {
                id: "d2".to_string(),
                json_blob: Some(serde_json::json!({ "title": "Fearless Concurrency" })),
            },
        )
        .expect("write after demotion");
    store.commit_index("docs").unwrap();

    let hits = store
        .search_documents("docs", "title:Fearless", 10, None)
        .unwrap()
        .hits;
    assert!(
        hits.is_empty(),
        "`title` was demoted, so a document written afterwards should not be reachable by it"
    );
}

/// `searchable_fields` reports what a query can actually reach, which is not what `indexed` says.
///
/// This is the fact no caller above the engine can work out for itself, and the reason an index
/// description is built here rather than composed by each consumer.
#[test]
fn searchable_fields_reports_the_built_index_not_the_declaration() {
    let temp = TempDir::new().unwrap();
    let store = store_with_a_discovered_field(&temp, "docs");

    let searchable = store.searchable_fields("docs");
    assert!(searchable.contains("title"), "{searchable:?}");
    assert!(
        searchable.contains("id"),
        "`id:value` is answerable, so `id` is searchable: {searchable:?}"
    );
    assert!(
        !searchable.contains("_seq"),
        "`_seq` is WAL bookkeeping, not a queryable field: {searchable:?}"
    );
    assert!(
        !searchable.contains("author"),
        "a discovered field has no column: {searchable:?}"
    );

    // Declaring it does not change what the built index holds — that is the whole distinction.
    set_indexed(&store, "docs", "author", true);
    assert!(
        store.get_schema("docs").unwrap().unwrap().fields["author"].indexed,
        "declared indexed"
    );
    assert!(
        !store.searchable_fields("docs").contains("author"),
        "but still not searchable until the index is rebuilt"
    );

    // After the rebuild the two agree again.
    store.delete_index_data("docs", false).unwrap();
    store
        .apply_write(
            "docs",
            WalOp::Put {
                id: "d1".to_string(),
                json_blob: Some(serde_json::json!({"title": "t", "author": "hoare"})),
            },
        )
        .unwrap();
    store.commit_index("docs").unwrap();
    assert!(store.searchable_fields("docs").contains("author"));
}

/// The warm and cold paths must report the same set, or a description would change with cache
/// state rather than with the index.
#[test]
fn searchable_fields_agrees_whether_the_index_is_cached_or_not() {
    let temp = TempDir::new().unwrap();
    let store = store_with_a_discovered_field(&temp, "docs");
    let warm = store.searchable_fields("docs");
    assert!(
        !warm.is_empty(),
        "the warm path should have found the cache"
    );

    // Dropping the field cache sends the next call down the open-from-disk path.
    store.invalidate_schema_cache("docs");
    let cold = store.searchable_fields("docs");

    assert_eq!(warm, cold, "cache state must not change what is reported");
}

/// An index that has a schema but was never written to has nothing searchable yet.
#[test]
fn an_unbuilt_index_reports_nothing_searchable() {
    let temp = TempDir::new().unwrap();
    let store = HybridStore::new(config(temp.path().to_path_buf()), 1).unwrap();

    let mut fields = HashMap::new();
    fields.insert("title".to_string(), indexed_text("title"));
    let schema = IndexSchema {
        fields,
        ..Default::default()
    };
    store.store_schema_and_cache("empty", &schema).unwrap();

    assert!(
        store.searchable_fields("empty").is_empty(),
        "nothing is built, so nothing is searchable"
    );
}
