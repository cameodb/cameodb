//! `NOT` combined with `AND`/`OR`, standing alone, or nested — the forms Tantivy parses but
//! does not evaluate, and the rewrites that make them answer the boolean they read as.
//!
//! Tantivy repairs an all-negative clause only at the top level of a query, so `a AND NOT b`
//! used to match nothing and `a OR NOT b` dropped the `NOT` arm silently. `prepare_query_parser`
//! now rewrites each `NOT` before the parser sees it — `a AND NOT b` to `a AND -b`, `a OR NOT b`
//! to `a OR (* -b)` — and `validate_query` reports the rewritten form.

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

/// d1 rust/systems, d2 go/systems, d3 rust/draft — a document for each arm of the tests below.
fn store_with_docs(temp: &TempDir, index: &str) -> HybridStore {
    let store = HybridStore::new(config(temp.path().to_path_buf()), 1).unwrap();

    let mut fields = HashMap::new();
    for name in ["title", "tag"] {
        let mut def = FieldDef::new(name.to_string(), TantivyFieldType::Text);
        def.indexed = true;
        def.stored = false;
        fields.insert(name.to_string(), def);
    }

    let schema = IndexSchema {
        fields,
        ..Default::default()
    };
    store.store_schema_and_cache(index, &schema).unwrap();

    for (id, title, tag) in [
        ("d1", "rust language", "systems"),
        ("d2", "go language", "systems"),
        ("d3", "rust cookbook", "draft"),
    ] {
        store
            .apply_write(
                index,
                WalOp::Put {
                    id: id.to_string(),
                    json_blob: Some(serde_json::json!({"id": id, "title": title, "tag": tag})),
                },
            )
            .unwrap();
    }
    store.commit_index(index).unwrap();
    store
}

fn hits(store: &HybridStore, query: &str) -> Vec<String> {
    let outcome = store.search_documents("docs", query, 10, None).unwrap();
    assert!(
        outcome.discarded.is_empty(),
        "{query:?} dropped a clause: {:?}",
        outcome.discarded
    );
    let mut ids: Vec<String> = outcome
        .hits
        .iter()
        .filter_map(|(_, doc)| doc.get("id").and_then(|id| id.as_str()).map(str::to_string))
        .collect();
    ids.sort();
    ids
}

fn normalized(store: &HybridStore, query: &str) -> String {
    store
        .validate_query("docs", query)
        .unwrap()
        .unwrap()
        .normalized_query
}

/// `a AND NOT b` is `a` less `b`. Before the rewrite it parsed and answered nothing: the
/// negated clause under `Must` matched no documents, and the query validated clean.
#[test]
fn and_not_keeps_the_first_and_excludes_the_second() {
    let temp = TempDir::new().unwrap();
    let store = store_with_docs(&temp, "docs");

    assert_eq!(hits(&store, "title:rust AND NOT tag:draft"), ["d1"]);
    assert_eq!(hits(&store, "title:rust AND NOT tag:nosuch"), ["d1", "d3"]);
    assert_eq!(
        normalized(&store, "title:rust AND NOT tag:draft"),
        "title:rust AND -tag:draft"
    );
}

/// `a OR NOT b` is `a` or anything that is not `b`. The `NOT` arm used to contribute nothing —
/// `a OR NOT b` answered what `a` alone answers.
#[test]
fn or_not_adds_everything_but_the_negated() {
    let temp = TempDir::new().unwrap();
    let store = store_with_docs(&temp, "docs");

    assert_eq!(hits(&store, "title:rust OR NOT tag:systems"), ["d1", "d3"]);
    assert_eq!(
        hits(&store, "title:rust OR NOT tag:draft"),
        ["d1", "d2", "d3"],
        "every document is rust or not draft"
    );
    // A document matching both sides of the `OR` still matches: `-b` appended at top level
    // would exclude it, which is why the rewrite parenthesises.
    assert_eq!(
        hits(&store, "title:rust OR NOT tag:systems OR title:go"),
        ["d1", "d2", "d3"],
    );
    assert_eq!(
        normalized(&store, "title:rust OR NOT tag:draft"),
        "title:rust OR (* -tag:draft)"
    );
}

/// A bare `NOT b` already matched everything but `b` — and reported a clause dropped while it
/// did. The rewrite to `(* -b)` keeps the answer and drops the report.
#[test]
fn a_leading_not_matches_and_reports_cleanly() {
    let temp = TempDir::new().unwrap();
    let store = store_with_docs(&temp, "docs");

    let validated = store
        .validate_query("docs", "NOT tag:draft")
        .unwrap()
        .unwrap();
    assert_eq!(validated.normalized_query, "(* -tag:draft)");
    assert!(validated.discarded.is_empty(), "{:?}", validated.discarded);

    assert_eq!(hits(&store, "NOT tag:draft"), ["d1", "d2"]);
}

/// `a NOT b` already meant `a` less `b`, and still does — spelled `a -b` now.
#[test]
fn a_bare_infix_not_is_the_exclusion_it_read_as() {
    let temp = TempDir::new().unwrap();
    let store = store_with_docs(&temp, "docs");

    assert_eq!(hits(&store, "title:rust NOT tag:draft"), ["d1"]);
    assert_eq!(
        normalized(&store, "title:rust NOT tag:draft"),
        "title:rust -tag:draft"
    );
}

/// A `NOT` opening the query composes with what follows: `NOT a AND b` parsed as a `Must` on a
/// clause that could not match, and `NOT a OR b` answered `b` alone.
#[test]
fn a_leading_not_combines_with_what_follows() {
    let temp = TempDir::new().unwrap();
    let store = store_with_docs(&temp, "docs");

    assert_eq!(hits(&store, "NOT title:rust AND tag:systems"), ["d2"]);
    assert_eq!(hits(&store, "NOT title:rust OR tag:draft"), ["d2", "d3"]);
}

/// `NOT` binds a leaf, and a parenthesised group is one leaf — including one holding another
/// `NOT`, which rewrites by the same rules.
#[test]
fn not_applies_to_a_whole_group() {
    let temp = TempDir::new().unwrap();
    let store = store_with_docs(&temp, "docs");

    assert_eq!(hits(&store, "NOT (title:go OR tag:draft)"), ["d1"]);
    assert_eq!(
        hits(&store, "title:rust AND NOT (tag:systems OR tag:nosuch)"),
        ["d3"]
    );
    assert_eq!(
        normalized(&store, "NOT (title:go OR tag:draft)"),
        "(* -(title:go OR tag:draft))"
    );
}

/// `NOT NOT` cancels: after a binary operator the leaf stands alone, after a leaf it needs the
/// `AND` the run implied.
#[test]
fn a_double_not_is_positive() {
    let temp = TempDir::new().unwrap();
    let store = store_with_docs(&temp, "docs");

    assert_eq!(hits(&store, "NOT NOT title:rust"), ["d1", "d3"]);
    assert_eq!(hits(&store, "title:rust AND NOT NOT tag:draft"), ["d3"]);
    assert_eq!(
        hits(&store, "title:rust OR NOT NOT tag:draft"),
        ["d1", "d3"]
    );
    assert_eq!(normalized(&store, "NOT NOT title:rust"), "title:rust");
}

/// Inside `field:( ... )` the `-` rewrites still apply — `-b` takes the field's scope.
#[test]
fn not_inside_a_field_group_negates_in_its_scope() {
    let temp = TempDir::new().unwrap();
    let store = store_with_docs(&temp, "docs");

    assert_eq!(hits(&store, "tag:(systems AND NOT draft)"), ["d1", "d2"]);
    assert_eq!(hits(&store, "tag:(systems NOT draft)"), ["d1", "d2"]);
    assert_eq!(
        normalized(&store, "tag:(systems AND NOT draft)"),
        "tag:(systems AND -draft)"
    );
}

/// What the pass must not touch: `NOT` inside a phrase is a word, inside a set a literal
/// element, mid-token part of a term, and lowercase none of this at all.
#[test]
fn not_that_isnt_an_operator_is_left_alone() {
    let temp = TempDir::new().unwrap();
    let store = store_with_docs(&temp, "docs");

    for query in [
        "title:\"a NOT b\"",
        "tag: IN [systems NOT]",
        "notes:x",
        "title:rust not tag:draft",
    ] {
        assert_eq!(
            normalized(&store, query),
            query,
            "{query:?} carries no operator `NOT` and should reach the parser as written"
        );
    }
}
