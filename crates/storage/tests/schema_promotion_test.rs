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

use std::collections::{BTreeMap, HashMap};

use storage::{FieldDef, HybridStore, IndexSchema, StorageConfig, TantivyFieldType, WalOp};
use tempfile::TempDir;

fn config(path: std::path::PathBuf) -> StorageConfig {
    StorageConfig {
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
    }
}

fn indexed_text(name: &str) -> FieldDef {
    let mut def = FieldDef::new(name.to_string(), TantivyFieldType::Text);
    def.indexed = true;
    def.stored = true;
    def
}

/// An index holding one document, with `title` indexed from the start and `author` arriving only
/// inside the document — so `author` is a discovered field, the case promotion is written for.
fn store_with_a_discovered_field(temp: &TempDir, index: &str) -> HybridStore {
    let store = HybridStore::new(config(temp.path().to_path_buf()), 1).unwrap();

    let mut fields = HashMap::new();
    fields.insert("title".to_string(), indexed_text("title"));

    let mut schema = IndexSchema {
        fields,
        ..Default::default()
    };
    schema.rebuild_shadow_fields_cache();
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

    let updates = BTreeMap::from([("author".to_string(), true)]);
    let outcome = store.update_field_indexing("docs", &updates).unwrap();

    assert!(!outcome.is_rejected(), "{outcome:?}");
    assert_eq!(outcome.applied, vec!["author".to_string()]);
    assert_eq!(
        outcome.pending_reindex,
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

    let updates = BTreeMap::from([("author".to_string(), true)]);
    store.update_field_indexing("docs", &updates).unwrap();

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

    let mut schema = IndexSchema {
        fields,
        ..Default::default()
    };
    schema.rebuild_shadow_fields_cache();
    store.store_schema_and_cache("docs", &schema).unwrap();

    let updates = BTreeMap::from([("author".to_string(), true)]);
    let outcome = store.update_field_indexing("docs", &updates).unwrap();

    assert!(!outcome.is_rejected(), "{outcome:?}");
    assert_eq!(outcome.applied, vec!["author".to_string()]);
    assert!(store.get_schema("docs").unwrap().unwrap().fields["author"].indexed);
}

/// Demotion is the direction that does work, and it takes effect on the next write.
#[test]
fn demoting_an_indexed_field_stops_new_documents_being_indexed_into_it() {
    let temp = TempDir::new().unwrap();
    let store = store_with_a_discovered_field(&temp, "docs");

    let updates = BTreeMap::from([("title".to_string(), false)]);
    let outcome = store.update_field_indexing("docs", &updates).unwrap();

    assert!(!outcome.is_rejected(), "{outcome:?}");
    assert_eq!(outcome.applied, vec!["title".to_string()]);

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

/// An unknown field is reported and skipped; the rest of the request still applies.
///
/// One shard is deliberately mechanical here. Shards normally hold the same schema — one declared
/// through `PUT /_config` is fanned out to all of them, and one inferred from a bulk load is
/// sampled from the first 200 documents and persisted everywhere before the first write lands.
/// Semi-structured input written a document at a time is the exception: a field only some
/// documents carry reaches only some shards. So "absent here" does not mean "absent everywhere",
/// and only the caller spanning the shards can tell the two apart. That caller
/// (`NodeOrchestrator::orch_update_schema`) refuses the request when every shard says unknown,
/// and plans across all of them before any writes.
#[test]
fn an_unknown_field_is_reported_without_blocking_the_rest() {
    let temp = TempDir::new().unwrap();
    let store = store_with_a_discovered_field(&temp, "docs");

    let updates = BTreeMap::from([("title".to_string(), false), ("nonesuch".to_string(), true)]);
    let outcome = store.update_field_indexing("docs", &updates).unwrap();

    assert_eq!(outcome.unknown, vec!["nonesuch".to_string()]);
    assert_eq!(outcome.applied, vec!["title".to_string()]);
    assert!(
        !store.get_schema("docs").unwrap().unwrap().fields["title"].indexed,
        "the field this shard does know should still have been changed"
    );
}

/// Every property the schema carries survives a flag change.
///
/// The endpoint used to read the schema out through a response shape that carried only `fields`
/// and `description`, mutate that, and write it back — so `routing_field_name`, `version`,
/// `created_at` and the fingerprint were reset by any unrelated edit. Resetting the routing field
/// silently changes which shard a document lands on.
#[test]
fn updating_a_flag_preserves_every_other_schema_property() {
    let temp = TempDir::new().unwrap();
    let store = store_with_a_discovered_field(&temp, "docs");

    let mut schema = store.get_schema("docs").unwrap().unwrap();
    schema.version = 7;
    schema.created_at = 1_600_000_000;
    schema.description = Some("the corpus".to_string());
    schema.set_routing_field("title".to_string()).unwrap();
    store.store_schema_and_cache("docs", &schema).unwrap();

    let updates = BTreeMap::from([("title".to_string(), false)]);
    let outcome = store.update_field_indexing("docs", &updates).unwrap();
    assert!(!outcome.is_rejected(), "{outcome:?}");

    let after = store.get_schema("docs").unwrap().unwrap();
    assert_eq!(after.get_routing_field(), "title", "routing field erased");
    // Advanced by one, not reset to 1. The property this test guards is that an unrelated edit
    // does not *erase* what the schema carries — and a version that stood still would fail that
    // in the other direction, since a cluster comparing versions has to see a local edit move it.
    assert_eq!(after.version, 8, "version did not advance with the edit");
    assert_eq!(after.created_at, 1_600_000_000, "created_at reset");
    assert_eq!(
        after.description.as_deref(),
        Some("the corpus"),
        "description erased"
    );
    assert!(!after.fields["title"].indexed, "the edit itself was lost");
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
    let updates = BTreeMap::from([("author".to_string(), true)]);
    store.update_field_indexing("docs", &updates).unwrap();
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
    let mut schema = IndexSchema {
        fields,
        ..Default::default()
    };
    schema.rebuild_shadow_fields_cache();
    store.store_schema_and_cache("empty", &schema).unwrap();

    assert!(
        store.searchable_fields("empty").is_empty(),
        "nothing is built, so nothing is searchable"
    );
}
