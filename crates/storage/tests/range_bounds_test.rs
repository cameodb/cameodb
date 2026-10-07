//! Range clauses on text fields, in every form the query syntax offers.
//!
//! The grammar reads a range bound as a bare word: a quote is part of it and a space ends it. On
//! an untokenized field a quoted bound therefore compared against the quote characters, and
//! `label:>"S"` matched every value while `label:["A" TO "M"]` matched none — silently. And a
//! whole query `id:>9000` was answered as a lookup of the key `>9000`. Each form here has to
//! answer what it says, and drop nothing.

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

/// Four documents: ids `100`, `2000`, `9000`, `9500`, labelled by a category kept whole.
fn store(temp: &TempDir) -> HybridStore {
    let store = HybridStore::new(config(temp.path().to_path_buf()), 1).unwrap();
    let mut fields = HashMap::new();
    fields.insert(
        "label".to_string(),
        FieldDef::new("label".to_string(), TantivyFieldType::String),
    );
    fields.insert(
        "name".to_string(),
        FieldDef::new("name".to_string(), TantivyFieldType::Text),
    );
    let schema = IndexSchema {
        fields,
        ..Default::default()
    };
    store.store_schema_and_cache("talks", &schema).unwrap();
    for (id, label, name) in [
        ("100", "Film & Animation", "rust"),
        ("2000", "Music", "rust"),
        ("9000", "Science & Technology", "go"),
        ("9500", "TED", "rust"),
    ] {
        store
            .apply_write(
                "talks",
                WalOp::Put {
                    id: id.to_string(),
                    json_blob: Some(serde_json::json!({"label": label, "name": name})),
                },
            )
            .unwrap();
    }
    store.commit_index("talks").unwrap();
    store
}

fn ids(store: &HybridStore, query: &str) -> Vec<String> {
    let outcome = store.search_documents("talks", query, 10, None).unwrap();
    assert!(
        outcome.discarded.is_empty(),
        "{query:?} discarded a clause: {:?}",
        outcome.discarded
    );
    let mut ids: Vec<String> = outcome
        .hits
        .iter()
        .filter_map(|(_, hit)| hit.get("id").and_then(|id| id.as_str()).map(str::to_string))
        .collect();
    ids.sort();
    assert_eq!(
        ids.len(),
        outcome.total_hits,
        "{query:?}: every hit fits in the page"
    );
    ids
}

#[test]
fn a_quoted_bound_on_a_whole_value_field_is_the_value_it_quotes() {
    let temp = TempDir::new().unwrap();
    let store = store(&temp);

    for (query, expected) in [
        (
            r#"label:["Film & Animation" TO "Music"]"#,
            vec!["100", "2000"],
        ),
        (r#"label:{"Film & Animation" TO "Music"]"#, vec!["2000"]),
        (r#"label:>="Science & Technology""#, vec!["9000", "9500"]),
        (r#"label:>"S""#, vec!["9000", "9500"]),
        (r#"label:<"M""#, vec!["100"]),
        (r#"label:>"Z""#, vec![]),
        (r#"label:["M" TO *]"#, vec!["2000", "9000", "9500"]),
        // The bare forms the grammar always read, unchanged.
        ("label:>S", vec!["9000", "9500"]),
        ("label:[M TO T}", vec!["2000", "9000"]),
    ] {
        assert_eq!(ids(&store, query), expected, "{query}");
    }
}

#[test]
fn every_form_of_a_range_on_the_id_answers_alike() {
    let temp = TempDir::new().unwrap();
    let store = store(&temp);

    // Ids compare as text: `9500` sorts above `9000`, and `100` between `1` and `2`.
    for query in [
        "id:>9000",
        r#"id:>"9000""#,
        "id:{9000 TO *}",
        r#"id:{"9000" TO *]"#,
    ] {
        assert_eq!(ids(&store, query), vec!["9500"], "{query}");
    }
    for query in ["id:>=9000", r#"id:["9000" TO *]"#] {
        assert_eq!(ids(&store, query), vec!["9000", "9500"], "{query}");
    }
    for query in ["id:[1 TO 2]", r#"id:["1" TO "2"]"#, "id:<2"] {
        assert_eq!(ids(&store, query), vec!["100"], "{query}");
    }
    // A whole key is still a lookup.
    assert_eq!(ids(&store, "id:9000"), vec!["9000"]);
}

#[test]
fn a_quoted_range_combines_with_the_clauses_around_it() {
    let temp = TempDir::new().unwrap();
    let store = store(&temp);

    assert_eq!(
        ids(&store, r#"name:rust AND label:>="Music""#),
        vec!["2000", "9500"]
    );
    assert_eq!(
        ids(&store, r#"label:<"M" OR label:>"T""#),
        vec!["100", "9500"]
    );
    assert_eq!(
        ids(
            &store,
            r#"name:rust -label:["Film & Animation" TO "Music"]"#
        ),
        vec!["9500"]
    );
    assert_eq!(ids(&store, r#"(label:>"S")^2 AND name:go"#), vec!["9000"]);
}
