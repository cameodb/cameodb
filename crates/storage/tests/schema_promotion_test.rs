//! Changing a field's `indexed` flag, which is what `PATCH /api/{index}/_schema` exists to do.
//!
//! A field that first appears inside a written document is recorded as non-indexed, on the
//! stated expectation that it "can be promoted to indexed later". No mechanism was ever built
//! for the second half of that sentence, and two separate defects hid it: persisting any schema
//! against an open index failed on the writer lockfile, and had it succeeded, the flag would
//! have been written while the field stayed unqueryable — the Tantivy schema is fixed when the
//! index is created and nothing rebuilds it.
//!
//! So these tests pin down three things: that persisting a schema no longer strands a live
//! writer, that the edits which *can* work do, and that the one which cannot is refused by name
//! instead of being acknowledged and dropped.

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

/// Setting `indexed` on a field the built Tantivy index does not have is refused.
///
/// Writing the flag would report success and change nothing a query can see: the Tantivy schema
/// is fixed when the index is created, nothing rebuilds it, and the write path skips a field with
/// no Tantivy field to write into. A refusal that names the field is the only honest answer until
/// there is a reindex path.
#[test]
fn promoting_a_discovered_field_is_refused_rather_than_silently_ignored() {
    let temp = TempDir::new().unwrap();
    let store = store_with_a_discovered_field(&temp, "docs");

    let updates = BTreeMap::from([("author".to_string(), true)]);
    let outcome = store.update_field_indexing("docs", &updates).unwrap();

    assert!(outcome.is_rejected(), "promotion should be refused");
    assert_eq!(outcome.needs_reindex, vec!["author".to_string()]);
    assert!(
        outcome.applied.is_empty(),
        "a refused request must write nothing"
    );

    let persisted = store.get_schema("docs").unwrap().unwrap();
    assert!(
        !persisted.fields["author"].indexed,
        "the refused flag must not have been persisted"
    );
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

/// An unknown field is refused, and refused without touching the fields that were valid.
#[test]
fn an_unknown_field_refuses_the_whole_request() {
    let temp = TempDir::new().unwrap();
    let store = store_with_a_discovered_field(&temp, "docs");

    let updates = BTreeMap::from([("title".to_string(), false), ("nonesuch".to_string(), true)]);
    let outcome = store.update_field_indexing("docs", &updates).unwrap();

    assert_eq!(outcome.unknown, vec!["nonesuch".to_string()]);
    assert!(
        store.get_schema("docs").unwrap().unwrap().fields["title"].indexed,
        "the valid half of a refused request must not be applied"
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
    assert_eq!(after.version, 7, "version reset");
    assert_eq!(after.created_at, 1_600_000_000, "created_at reset");
    assert_eq!(
        after.description.as_deref(),
        Some("the corpus"),
        "description erased"
    );
    assert!(!after.fields["title"].indexed, "the edit itself was lost");
}
