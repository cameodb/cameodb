//! `fast` is a declaration, and a caller can make it either way.
//!
//! It used to be a declaration only in one direction. `normalize_after_deserialization` ended its
//! numeric arm with an unconditional `field_def.fast = true;` — written as a default, behaving as
//! an assignment — and it runs on every deserialization, so an i64 field declared `"fast": false`
//! read back as `true` every time the schema was read (OB1). The reason it could not simply be
//! made conditional is that `#[serde(default)] pub fast: bool` cannot tell an absent key from an
//! explicit `false`: both arrive as `false`, so there was nothing for a condition to test.
//!
//! These tests pin both halves of the fix — the declaration survives, and the default still
//! applies when nobody declared anything — and then check what the built index does about it,
//! which is the part that matters to a caller: no column, so no exact sort, and ranges and
//! comparisons unaffected because they never needed one.

use serde_json::json;
use storage::{
    FieldDef, HybridStore, IndexSchema, SearchOutcome, SortOrder, SortSpec, StorageConfig,
    TantivyFieldType, WalOp,
};
use tempfile::TempDir;

const INDEX: &str = "readings";

fn test_config(path: std::path::PathBuf) -> StorageConfig {
    StorageConfig {
        shard_path: path,
        indexer_memory_budget: 32 * 1024 * 1024,
        indexer_memory_min_mb: 16,
        indexer_memory_max_mb: 256,
        total_memory_limit_bytes: 4 * 1024 * 1024 * 1024,
        memory_pressure_threshold_percent: 80,
        indexer_num_threads: 1,
        merge_num_threads: 2,
        default_batch_size: 1000,
        wal_sync: true,
    }
}

/// Three numeric fields saying the three different things a caller can say about `fast`, written
/// as JSON rather than built with `FieldDef::new` because the distinction under test — an absent
/// key against an explicit `false` — exists only on the wire.
fn declared_schema() -> IndexSchema {
    let mut schema: IndexSchema = serde_json::from_value(json!({
        "fields": {
            "id": {"field_type": "text", "indexed": true, "stored": true},
            "body": {"field_type": "text", "indexed": true},
            "declined": {"field_type": "i64", "indexed": true, "fast": false},
            "defaulted": {"field_type": "i64", "indexed": true},
            "asked": {"field_type": "i64", "indexed": true, "fast": true}
        }
    }))
    .expect("a schema a caller could PUT");
    schema.normalize_after_deserialization();
    schema
}

fn store_with_readings(dir: &TempDir) -> HybridStore {
    let store =
        HybridStore::new(test_config(dir.path().to_path_buf()), 1).expect("build a HybridStore");
    store
        .store_schema_and_cache(INDEX, &declared_schema())
        .expect("store the schema");

    let docs: Vec<WalOp> = (1..=3)
        .map(|n| WalOp::Put {
            id: format!("r{n}"),
            json_blob: Some(json!({
                "id": format!("r{n}"),
                "body": "reading",
                "declined": n,
                "defaulted": n,
                "asked": n,
            })),
        })
        .collect();

    store.apply_batch(INDEX, docs).expect("write the readings");
    store.commit_index(INDEX).expect("commit");
    store
}

/// The ids a query returns, sorted, asserting the engine ran the query as written.
fn matches(store: &HybridStore, query: &str) -> Vec<String> {
    let SearchOutcome {
        hits, discarded, ..
    } = store
        .search_documents(INDEX, query, 10, None)
        .unwrap_or_else(|e| panic!("searching {query:?} failed: {e}"));
    assert!(
        discarded.is_empty(),
        "{query:?} should run as written, but the engine discarded {discarded:?}"
    );
    let mut ids: Vec<String> = hits
        .iter()
        .map(|(_, doc)| doc["id"].as_str().unwrap_or_default().to_string())
        .collect();
    ids.sort();
    ids
}

#[test]
fn a_numeric_field_that_declines_the_fast_column_keeps_declining_it() {
    let schema = declared_schema();

    assert!(
        !schema.fields["declined"].is_fast(),
        "a declared `fast: false` is the caller's decision, not a gap to fill in"
    );
    assert!(
        schema.fields["defaulted"].is_fast(),
        "saying nothing still means the default for the type, which for a numeric is a column"
    );
    assert!(
        schema.fields["asked"].is_fast(),
        "and a declared `fast: true` is honoured as it always was"
    );
    assert!(
        !schema.fields["body"].is_fast(),
        "a text field pays for a full copy of every value, so it gets a column only when asked"
    );
}

/// The declaration has to survive being read back, because that is when it was being lost.
///
/// A schema is normalized on every deserialization — on the write path before it is stored, and
/// again by every reader that opens it — so a value that decays once per normalization decays to
/// the default within one round trip.
#[test]
fn the_declaration_survives_a_round_trip_through_the_stored_form() {
    let schema = declared_schema();
    let wire = serde_json::to_value(&schema).expect("serialise the schema");

    assert_eq!(
        wire["fields"]["declined"]["fast"],
        json!(false),
        "a normalized schema names a concrete boolean, so every existing reader of the \
         serialised form sees the shape it saw before"
    );
    assert_eq!(
        wire["fields"]["defaulted"]["fast"],
        json!(true),
        "including the resolved default, which is not left for the reader to infer"
    );

    let mut reread: IndexSchema = serde_json::from_value(wire).expect("read it back");
    reread.normalize_after_deserialization();
    assert!(
        !reread.fields["declined"].is_fast(),
        "reading a schema is not how a declaration decays"
    );
    assert!(reread.fields["defaulted"].is_fast());
}

/// What declining the column costs, and what it does not.
///
/// The engine builds the column from the declaration, so the whole visible consequence is the
/// sort: it is refused, naming the field. Ranges and comparisons are answered from the inverted
/// index and never needed a column, so they are unaffected — which is what makes declining one a
/// real choice rather than a way to break the field.
#[test]
fn declining_the_column_refuses_the_sort_and_leaves_the_ranges_alone() {
    let dir = TempDir::new().unwrap();
    let store = store_with_readings(&dir);

    let sortable = store.sortable_fields(INDEX);
    assert!(
        !sortable.contains("declined"),
        "the built index has no column for a field that declined one: {sortable:?}"
    );
    assert!(
        sortable.contains("defaulted") && sortable.contains("asked"),
        "and it has one for every numeric field that did not: {sortable:?}"
    );

    assert_eq!(
        matches(&store, "declined:[2 TO 3]"),
        vec!["r2".to_string(), "r3".to_string()],
        "a range needs no column"
    );
    assert_eq!(
        matches(&store, "declined:>=2"),
        vec!["r2".to_string(), "r3".to_string()],
        "nor does a comparison"
    );
    assert_eq!(
        matches(&store, "declined:1"),
        vec!["r1".to_string()],
        "nor does an equality"
    );

    let sort = |field: &str| {
        store.search_documents(
            INDEX,
            "body:reading",
            10,
            Some(&SortSpec {
                field: field.to_string(),
                order: SortOrder::Asc,
            }),
        )
    };

    let refusal = sort("declined").expect_err("a sort with no column to order on is refused");
    let message = refusal.to_string();
    assert!(
        message.contains("declined"),
        "the refusal names the field it refused: {message}"
    );

    let sorted = sort("defaulted").expect("the field that kept its column still sorts");
    assert_eq!(sorted.hits.len(), 3);
}

/// A shadow field is never fast, whatever the schema says.
///
/// Not the same kind of override as the one this file exists about: a shadow field is not added
/// to the Tantivy index at all, so there is no column for a declaration to ask for and reporting
/// `fast: true` would be a claim the index cannot back.
#[test]
fn a_shadow_field_is_never_fast_however_it_is_declared() {
    let mut schema = IndexSchema::default();
    let mut shadow = FieldDef::new_shadow("doi".to_string(), TantivyFieldType::I64);
    shadow.fast = Some(true);
    schema.fields.insert("doi".to_string(), shadow);
    schema.normalize_after_deserialization();

    assert!(!schema.fields["doi"].is_fast());
    assert_eq!(
        schema.fields["doi"].fast,
        Some(false),
        "resolved rather than left as asked, so the stored schema says what the index does"
    );
}

/// A type whose column is never built is never fast, whatever the schema declares.
///
/// The index builder adds a boolean, bytes, ip, json or facet field with `add_bool_field` and
/// friends, none of which reads `fast`. So a declared `true` on one of those was reported back by
/// `_config` as `true` with no column behind it — and the sort guard, which reads the declaration,
/// waved the sort through to be refused separately by every shard: a `200` with an empty page for a
/// request that never ran. Resolving it to `false` here is what makes the config and the index
/// agree, and what makes the guard refuse the sort itself.
#[test]
fn a_type_that_can_carry_no_column_is_never_fast() {
    for type_name in ["boolean", "bytes", "ip", "json", "facet"] {
        let mut schema: IndexSchema = serde_json::from_value(json!({
            "fields": {
                "asked": {"field_type": type_name, "indexed": true, "fast": true}
            }
        }))
        .unwrap_or_else(|e| panic!("a schema declaring a {type_name} field: {e}"));
        schema.normalize_after_deserialization();

        let field = &schema.fields["asked"];
        assert!(
            !field.is_fast(),
            "a {type_name} field cannot have a column, so it cannot be fast"
        );
        assert_eq!(
            field.fast,
            Some(false),
            "and the stored schema says so, rather than repeating a claim the index cannot back"
        );
        assert!(!FieldDef::can_be_fast(&field.field_type));
    }

    // The types that *can* carry one are untouched by the same rule.
    for type_name in ["text", "string", "i64", "u64", "f64", "date"] {
        let mut schema: IndexSchema = serde_json::from_value(json!({
            "fields": {
                "asked": {"field_type": type_name, "indexed": true, "fast": true}
            }
        }))
        .unwrap();
        schema.normalize_after_deserialization();
        assert!(
            schema.fields["asked"].is_fast(),
            "a {type_name} field declared fast keeps its column"
        );
    }
}
