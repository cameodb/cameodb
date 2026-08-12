//! The query shapes the pre-parse rewriters are responsible for: date literals in every
//! supported form, and facet paths.
//!
//! A shape no rewriter recognises reaches Tantivy in a form it rejects, which drops the clause
//! instead of raising an error — so each case asserts both the expected matches and that
//! nothing was discarded.

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

fn field(field_type: TantivyFieldType, fast: bool) -> FieldDef {
    let mut def = FieldDef::new("placeholder".to_string(), field_type);
    def.fast = fast;
    def.indexed = true;
    def.stored = false;
    def
}

/// One document, dated 2024-06-15, filed under `/electronics/phones`.
fn store_with_one_doc(temp: &TempDir, index: &str) -> HybridStore {
    let store = HybridStore::new(config(temp.path().to_path_buf()), 1).unwrap();

    let mut fields = HashMap::new();
    fields.insert("title".into(), field(TantivyFieldType::Text, false));
    fields.insert("created".into(), field(TantivyFieldType::Date, true));
    fields.insert("cat".into(), field(TantivyFieldType::Facet, false));

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
                    "title": "rust programming",
                    "created": "2024-06-15T00:00:00Z",
                    "cat": "/electronics/phones"
                })),
            },
        )
        .unwrap();
    store.commit_index(index).unwrap();
    store
}

/// Assert a query matches the single document and lost nothing on the way.
fn assert_matches(store: &HybridStore, index: &str, query: &str) {
    let outcome = store.search_documents(index, query, 10, None).unwrap();
    assert!(
        outcome.discarded.is_empty(),
        "{query:?} discarded a clause: {:?}",
        outcome.discarded
    );
    assert_eq!(outcome.total_hits, 1, "{query:?} matched nothing");
}

#[test]
fn exclusive_and_mixed_date_range_bounds_are_normalized() {
    // Tantivy's date parser accepts only RFC3339, so both bounds are rewritten whichever
    // delimiters enclose them.
    let temp = TempDir::new().unwrap();
    let store = store_with_one_doc(&temp, "dates");

    for query in [
        "created:[2024-01-01 TO 2024-12-31]",
        "created:{2024-01-01 TO 2024-12-31}",
        "created:[2024-01-01 TO 2024-12-31}",
        "created:{2024-01-01 TO 2024-12-31]",
    ] {
        assert_matches(&store, "dates", query);
    }
}

#[test]
fn exclusive_date_bounds_still_exclude() {
    // The document sits exactly on the lower bound, so the delimiter decides whether it
    // matches. Rewriting the literal must not change which delimiter is in force.
    let temp = TempDir::new().unwrap();
    let store = store_with_one_doc(&temp, "bounds");

    let inclusive = store
        .search_documents("bounds", "created:[2024-06-15 TO 2024-12-31]", 10, None)
        .unwrap();
    assert_eq!(inclusive.total_hits, 1, "inclusive lower bound must match");

    let exclusive = store
        .search_documents("bounds", "created:{2024-06-15 TO 2024-12-31}", 10, None)
        .unwrap();
    assert!(exclusive.discarded.is_empty());
    assert_eq!(
        exclusive.total_hits, 0,
        "exclusive lower bound must still exclude the boundary value"
    );
}

#[test]
fn date_set_queries_are_normalized() {
    // Whitespace is permitted after the colon and around `IN`, in any combination.
    let temp = TempDir::new().unwrap();
    let store = store_with_one_doc(&temp, "sets");

    for query in [
        "created: IN [2024-06-15]",
        "created: IN [2024-06-15 2023-01-01]",
        "created:IN [2024-06-15]",
        "created: IN [2024-06-15T00:00:00Z]",
    ] {
        assert_matches(&store, "sets", query);
    }

    // A set that does not contain the document's date must still not match it.
    let miss = store
        .search_documents("sets", "created: IN [2023-01-01 2022-05-05]", 10, None)
        .unwrap();
    assert!(miss.discarded.is_empty());
    assert_eq!(miss.total_hits, 0, "a non-matching set must not match");
}

#[test]
fn already_normalized_date_forms_are_untouched() {
    // Rewriting is idempotent: a literal already in RFC3339 survives a pass unchanged.
    let temp = TempDir::new().unwrap();
    let store = store_with_one_doc(&temp, "idem");

    for query in [
        "created:2024-06-15T00:00:00Z",
        "created:>2024-01-01T00:00:00Z",
        "created:[2024-01-01T00:00:00Z TO 2024-12-31T00:00:00Z]",
        "created:{2024-01-01T00:00:00Z TO 2024-12-31T00:00:00Z}",
    ] {
        assert_matches(&store, "idem", query);
    }
}

#[test]
fn facet_paths_do_not_need_quoting() {
    // The parser resolves a facet term only from a quoted value, so unquoted paths are quoted
    // on the way in. Matching is hierarchical.
    let temp = TempDir::new().unwrap();
    let store = store_with_one_doc(&temp, "facets");

    for query in [
        "cat:/electronics/phones",
        "cat:\"/electronics/phones\"",
        // A parent path matches its descendants.
        "cat:/electronics",
        "cat:\"/electronics\"",
    ] {
        assert_matches(&store, "facets", query);
    }

    let miss = store
        .search_documents("facets", "cat:/furniture", 10, None)
        .unwrap();
    assert_eq!(miss.total_hits, 0, "an unrelated facet path must not match");
}

#[test]
fn a_facet_path_composes_with_other_clauses() {
    // The path ends at whitespace or a closing paren, not at the end of the query.
    let temp = TempDir::new().unwrap();
    let store = store_with_one_doc(&temp, "facet_combo");

    for query in [
        "cat:/electronics/phones AND title:rust",
        "title:rust AND cat:/electronics/phones",
        "(cat:/electronics/phones OR cat:/books) AND title:rust",
        "cat:/electronics/phones created:[2024-01-01 TO 2024-12-31]",
    ] {
        assert_matches(&store, "facet_combo", query);
    }
}

#[test]
fn an_index_without_date_or_facet_fields_is_unaffected() {
    // Both rewriters are keyed on the schema, so a text-only index is passed through.
    let temp = TempDir::new().unwrap();
    let store = HybridStore::new(config(temp.path().to_path_buf()), 1).unwrap();

    let mut fields = HashMap::new();
    fields.insert("title".into(), field(TantivyFieldType::Text, false));
    let mut schema = IndexSchema {
        fields,
        ..Default::default()
    };
    schema.rebuild_shadow_fields_cache();
    store.store_schema_and_cache("plain", &schema).unwrap();
    store
        .apply_write(
            "plain",
            WalOp::Put {
                id: "d1".to_string(),
                json_blob: Some(serde_json::json!({"title": "rust programming"})),
            },
        )
        .unwrap();
    store.commit_index("plain").unwrap();

    assert_matches(&store, "plain", "title:rust");
    assert_matches(&store, "plain", "title:\"rust programming\"");
}
