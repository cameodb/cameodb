//! Field names the parser resolves to something ineffective, which it therefore does not report.
//!
//! A JSON field is a default query field, so Tantivy resolves an unrecognised `name:` prefix as a
//! path inside it. The clause parses cleanly and matches nothing, and in a negation it disables
//! the exclusion — with no parser error to surface. A schema check covers that.
//!
//! The check is a heuristic over query text, so most of what follows is its false-positive
//! surface: a report on a valid query is refused work at the MCP layer.

use std::collections::HashMap;

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

fn field(field_type: TantivyFieldType, indexed: bool) -> FieldDef {
    let fast = matches!(
        field_type,
        TantivyFieldType::U64
            | TantivyFieldType::I64
            | TantivyFieldType::F64
            | TantivyFieldType::Date
    );
    let mut def = FieldDef::new("placeholder".to_string(), field_type);
    def.fast = fast;
    def.indexed = indexed;
    def.stored = false;
    def
}

/// Two documents, one with `tag:active`, on an index that carries a JSON field — which is what
/// makes an unknown field name parse cleanly instead of erroring.
fn store(temp: &TempDir, index: &str) -> HybridStore {
    let store = HybridStore::new(config(temp.path().to_path_buf()), 1).unwrap();

    let mut fields = HashMap::new();
    fields.insert("title".into(), field(TantivyFieldType::Text, true));
    fields.insert("tag".into(), field(TantivyFieldType::String, true));
    fields.insert("year".into(), field(TantivyFieldType::U64, true));
    // A date field, because RFC3339 values carry colons and are the sharpest test of whether a
    // value token can be mistaken for a field reference.
    fields.insert("created".into(), field(TantivyFieldType::Date, true));
    fields.insert("meta".into(), field(TantivyFieldType::Json, true));
    fields.insert("url".into(), field(TantivyFieldType::Text, true));
    fields.insert("hidden".into(), field(TantivyFieldType::Text, false));

    let mut schema = IndexSchema {
        fields,
        ..Default::default()
    };
    schema.rebuild_shadow_fields_cache();
    store.store_schema_and_cache(index, &schema).unwrap();

    for (id, tag) in [("d1", "active"), ("d2", "archived")] {
        store
            .apply_write(
                index,
                WalOp::Put {
                    id: id.to_string(),
                    json_blob: Some(serde_json::json!({
                        "title": "rust programming",
                        "tag": tag,
                        "year": 2024u64,
                        "created": "2024-06-15T00:00:00Z",
                        "meta": {"source": "api"},
                        "url": "http://example.com/a",
                        "hidden": "invisible"
                    })),
                },
            )
            .unwrap();
    }
    store.commit_index(index).unwrap();
    store
}

fn discarded(store: &HybridStore, index: &str, query: &str) -> Vec<String> {
    store
        .search_documents(index, query, 10, None)
        .unwrap()
        .discarded
}

#[test]
fn an_unknown_field_is_reported_even_though_a_json_field_absorbs_it() {
    let temp = TempDir::new().unwrap();
    let store = store(&temp, "json_gap");

    for query in [
        "nosuch:x",
        "tag:active AND nosuch:x",
        "title:rust NOT nosuch:x",
        "+tag:active +nosuch:x",
        "(tag:active OR nosuch:x)",
    ] {
        let notes = discarded(&store, "json_gap", query);
        assert!(
            notes.iter().any(|note| note.contains("nosuch")),
            "{query:?} must report the unknown field, got: {notes:?}"
        );
    }
}

#[test]
fn a_dropped_exclusion_is_reported_even_when_the_hit_count_looks_ordinary() {
    // The shape with no visible symptom: the exclusion resolves to a JSON path that matches
    // nothing, so `NOT` excludes nothing and both documents come back.
    let temp = TempDir::new().unwrap();
    let store = store(&temp, "negation");

    let outcome = store
        .search_documents("negation", "title:rust NOT nosuch:x", 10, None)
        .unwrap();
    assert_eq!(
        outcome.total_hits, 2,
        "fixture: the exclusion has no effect"
    );
    assert!(
        !outcome.discarded.is_empty(),
        "an exclusion that had no effect must be reported"
    );
}

#[test]
fn a_non_indexed_field_is_reported() {
    let temp = TempDir::new().unwrap();
    let store = store(&temp, "not_indexed");

    let notes = discarded(&store, "not_indexed", "tag:active AND hidden:invisible");
    assert!(
        notes.iter().any(|note| note.contains("hidden")),
        "a non-indexed field must be reported, got: {notes:?}"
    );
}

#[test]
fn a_field_is_reported_once_when_both_the_parser_and_the_schema_check_see_it() {
    // On an index with no JSON field the parser reports the unknown field too. Both sources
    // produce the same note so the result carries one entry, not two.
    let temp = TempDir::new().unwrap();
    let store = HybridStore::new(config(temp.path().to_path_buf()), 1).unwrap();

    let mut fields = HashMap::new();
    fields.insert("title".into(), field(TantivyFieldType::Text, true));
    fields.insert("tag".into(), field(TantivyFieldType::String, true));
    let mut schema = IndexSchema {
        fields,
        ..Default::default()
    };
    schema.rebuild_shadow_fields_cache();
    store.store_schema_and_cache("no_json", &schema).unwrap();
    store
        .apply_write(
            "no_json",
            WalOp::Put {
                id: "d1".to_string(),
                json_blob: Some(serde_json::json!({"title": "rust", "tag": "active"})),
            },
        )
        .unwrap();
    store.commit_index("no_json").unwrap();

    let notes = discarded(&store, "no_json", "tag:active AND nosuch:x");
    let mentions = notes.iter().filter(|note| note.contains("nosuch")).count();
    assert_eq!(
        mentions, 1,
        "the same field from two sources must collapse to one note, got: {notes:?}"
    );
}

#[test]
fn valid_queries_are_never_reported() {
    // The false-positive surface. Every form here is legitimate, and a report on any of them
    // would refuse valid work at the MCP layer.
    let temp = TempDir::new().unwrap();
    let store = store(&temp, "clean");

    for query in [
        // Plain field references, including every prefix the grammar allows.
        "title:rust",
        "+title:rust -tag:archived",
        "(title:rust OR tag:active)",
        "!title:go",
        "title:rust AND tag:active",
        "title:rust NOT tag:archived",
        // Reserved words and range syntax must not read as field names.
        "year:[2020 TO 2025]",
        "year:{2020 TO 2025}",
        "year:>=2024",
        "tag: IN [active archived]",
        // Unfielded terms and phrases have no field reference at all.
        "rust",
        "rust programming",
        "\"rust programming\"",
        "*",
        // A colon inside a value is not a second field reference.
        "url:http://example.com/a",
        "title:rust url:http://example.com/a",
        // Range and set values carry colons of their own, across token boundaries.
        "year:[2020 TO 2025] AND title:rust",
        "tag: IN [active archived] AND title:rust",
        "created:[2024-01-01T00:00:00Z TO 2024-12-31T00:00:00Z]",
        "created:{2024-01-01T00:00:00Z TO 2024-12-31T00:00:00Z} AND title:rust",
        "created: IN [2024-06-15T00:00:00Z]",
        "created:2024-06-15T00:00:00Z",
        "created:>2024-01-01T00:00:00Z AND tag:active",
        // A colon inside a phrase is not a field reference either.
        "\"12:30 rust\"",
        "title:\"a: b\"",
        "\"time: 12:30\" AND tag:active",
        // `id` is a reserved field: present in the Tantivy schema, absent from
        // `IndexSchema::fields`, and still a legitimate thing to query.
        //
        // `_seq` used to be listed here for the same reason. It is not any more: indices built
        // after the field stopped being declared genuinely do not have it, so a query naming it
        // is an unknown-field query and reporting it is right. See the case below.
        "id:d1",
        // JSON paths of any depth, which is what the absorbing field is legitimately for.
        "meta.source:api",
        "meta.nested.deep:value",
        "meta:api",
        "tag:active AND meta.source:api",
    ] {
        let notes = discarded(&store, "clean", query);
        assert!(
            notes.is_empty(),
            "valid query {query:?} was reported: {notes:?}"
        );
    }
}

#[test]
fn a_field_name_containing_a_dot_resolves_unescaped() {
    // Tantivy resolves the longest field-name match before treating a dot as a path separator,
    // so a field literally named `k8s.node` is queried unescaped. Escaping the dot makes the
    // lookup miss, and the parser reports that as a dropped clause.
    let temp = TempDir::new().unwrap();
    let store = HybridStore::new(config(temp.path().to_path_buf()), 1).unwrap();

    let mut fields = HashMap::new();
    fields.insert("k8s.node".into(), field(TantivyFieldType::Text, true));
    let mut schema = IndexSchema {
        fields,
        ..Default::default()
    };
    schema.rebuild_shadow_fields_cache();
    store.store_schema_and_cache("dotted", &schema).unwrap();
    store
        .apply_write(
            "dotted",
            WalOp::Put {
                id: "d1".to_string(),
                json_blob: Some(serde_json::json!({"k8s.node": "worker-1"})),
            },
        )
        .unwrap();
    store.commit_index("dotted").unwrap();

    let resolved = store
        .search_documents("dotted", "k8s.node:worker-1", 10, None)
        .unwrap();
    assert_eq!(resolved.total_hits, 1, "the unescaped form must match");
    assert!(
        resolved.discarded.is_empty(),
        "the unescaped form must not be reported, got: {:?}",
        resolved.discarded
    );

    let escaped = store
        .search_documents("dotted", "k8s\\.node:worker-1", 10, None)
        .unwrap();
    assert_eq!(escaped.total_hits, 0);
    assert!(
        !escaped.discarded.is_empty(),
        "the escaped form loses the clause and must be reported"
    );
}

#[test]
fn a_field_discovered_during_a_write_is_reported_as_not_indexed() {
    // Fields found in a document are added to the schema non-indexed, so that a write does not
    // force a Tantivy schema rebuild. They are readable from redb but cannot be queried until an
    // explicit schema update promotes them, and the schema check says which of the two it is —
    // the parser only sees that the field is absent from the Tantivy schema and calls it unknown.
    let temp = TempDir::new().unwrap();
    let store = HybridStore::new(config(temp.path().to_path_buf()), 1).unwrap();
    store
        .store_schema_and_cache("evolving", &IndexSchema::default())
        .unwrap();
    store
        .apply_write(
            "evolving",
            WalOp::Put {
                id: "d1".to_string(),
                json_blob: Some(serde_json::json!({"title": "rust"})),
            },
        )
        .unwrap();
    store.commit_index("evolving").unwrap();

    let notes = discarded(&store, "evolving", "title:rust");
    assert_eq!(
        notes.len(),
        1,
        "one field, one note — the schema verdict replaces the parser's: {notes:?}"
    );
    assert!(
        notes[0].contains("not indexed"),
        "the more specific verdict must win over 'unknown field': {notes:?}"
    );

    // `id` is indexed on every index, so it stays queryable.
    assert!(discarded(&store, "evolving", "id:d1").is_empty());
}

#[test]
fn count_only_queries_are_checked_too() {
    let temp = TempDir::new().unwrap();
    let store = store(&temp, "counting");

    let notes = discarded(&store, "counting", "tag:active AND nosuch:x");
    assert!(!notes.is_empty(), "sanity: the search path reports");

    let outcome = store
        .search_documents("counting", "tag:active AND nosuch:x", 0, None)
        .unwrap();
    assert!(
        !outcome.discarded.is_empty(),
        "count-only mode must apply the same check"
    );
}

/// Querying `_seq` on an index built without it is reported like any other unknown field.
///
/// The field used to be forced into every index so the checkpoint scan had a column to order
/// on, and a query naming it therefore resolved and silently matched nothing meaningful. New
/// indices do not declare it, so the honest answer is that there is no such field — and saying
/// so is what stops a caller wondering why their filter had no effect.
#[test]
fn the_retired_seq_field_is_reported_as_unknown() {
    let temp = TempDir::new().expect("temp dir");
    let store = store(&temp, "clean");

    let notes = discarded(&store, "clean", "_seq:>0");
    assert!(
        notes.iter().any(|note| note.contains("_seq")),
        "a query on the retired _seq field should be reported: {notes:?}"
    );
}
