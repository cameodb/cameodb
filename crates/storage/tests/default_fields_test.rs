//! Which fields an unqualified term searches: the node's `max_default_fields` cap, and an index's
//! declared `default_fields` (ROADMAP M8, B).
//!
//! Five text fields, `a` to `e`, each holding one word found nowhere else — `wa` only in `a`, `we`
//! only in `e` — so whether a bare word matches says exactly which fields it was sent to.

use std::collections::HashMap;

use storage::{
    FieldDef, HybridStore, IndexSchema, QueryPolicy, StorageConfig, TantivyFieldType, WalOp,
    select_default_fields,
};
use tempfile::TempDir;

fn config(path: std::path::PathBuf, query: QueryPolicy) -> StorageConfig {
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
        query,
    }
}

fn text_field() -> FieldDef {
    let mut def = FieldDef::new("placeholder".to_string(), TantivyFieldType::Text);
    def.indexed = true;
    def
}

fn five_fields() -> IndexSchema {
    let mut fields = HashMap::new();
    for name in ["a", "b", "c", "d", "e"] {
        fields.insert(name.to_string(), text_field());
    }
    fields.insert(
        "n".to_string(),
        FieldDef::new("n".to_string(), TantivyFieldType::I64),
    );
    let mut schema = IndexSchema {
        fields,
        ..Default::default()
    };
    schema.normalize_after_deserialization();
    schema.rebuild_shadow_fields_cache();
    schema
}

fn open_store(temp: &TempDir, policy: QueryPolicy, declared: Option<&[&str]>) -> HybridStore {
    let store = HybridStore::new(config(temp.path().to_path_buf(), policy), 1).unwrap();
    let mut schema = five_fields();
    schema.default_fields = declared.map(|list| list.iter().map(|s| s.to_string()).collect());
    store.store_schema_and_cache("idx", &schema).unwrap();
    store
        .apply_write(
            "idx",
            WalOp::Put {
                id: "d1".to_string(),
                json_blob: Some(serde_json::json!({
                    "a": "wa", "b": "wb", "c": "wc", "d": "wd", "e": "we", "n": 1
                })),
            },
        )
        .unwrap();
    store.commit_index("idx").unwrap();
    store
}

fn capped(max_default_fields: usize) -> QueryPolicy {
    QueryPolicy {
        max_default_fields,
        ..Default::default()
    }
}

fn hits(store: &HybridStore, query: &str) -> u64 {
    let outcome = store.search_documents("idx", query, 10, None).unwrap();
    outcome.total_hits as u64
}

/// Which of the five bare words match — the fields a bare term reached.
fn reached(store: &HybridStore) -> Vec<&'static str> {
    ["a", "b", "c", "d", "e"]
        .into_iter()
        .filter(|field| hits(store, &format!("w{field}")) > 0)
        .collect()
}

#[test]
fn with_no_cap_a_bare_term_searches_every_text_field() {
    let temp = TempDir::new().unwrap();
    let store = open_store(&temp, capped(0), None);
    assert_eq!(reached(&store), ["a", "b", "c", "d", "e"]);
}

/// Past the cap, the first fields by name — the same on every shard, whatever order each
/// shard's index happens to hold them in.
#[test]
fn past_the_cap_the_first_fields_by_name_are_searched_and_nothing_is_refused() {
    let temp = TempDir::new().unwrap();
    let store = open_store(&temp, capped(3), None);
    assert_eq!(reached(&store), ["a", "b", "c"]);

    // Narrowed, not refused: the search runs and reports nothing dropped.
    let outcome = store.search_documents("idx", "we", 10, None).unwrap();
    assert!(outcome.discarded.is_empty(), "{:?}", outcome.discarded);

    // A field named in the query is reached at any cap.
    assert_eq!(hits(&store, "e:we"), 1);
}

/// A declared list chooses the fields, in its own order, and the cap cuts it from the front.
#[test]
fn a_declared_list_chooses_the_fields_and_its_order_decides_the_cut() {
    let temp = TempDir::new().unwrap();
    let store = open_store(&temp, capped(0), Some(&["e", "c"]));
    assert_eq!(reached(&store), ["c", "e"]);

    let temp = TempDir::new().unwrap();
    let store = open_store(&temp, capped(1), Some(&["e", "c"]));
    assert_eq!(
        reached(&store),
        ["e"],
        "the first declared field survives the cap"
    );
}

/// The count and validate paths prepare the query the same way.
#[test]
fn the_count_path_uses_the_same_fields() {
    let temp = TempDir::new().unwrap();
    let store = open_store(&temp, capped(3), None);
    assert_eq!(
        store
            .search_documents("idx", "we", 0, None)
            .unwrap()
            .total_hits,
        0
    );
    assert_eq!(
        store
            .search_documents("idx", "wa", 0, None)
            .unwrap()
            .total_hits,
        1
    );
}

/// An unqualified prefix expands across the same capped fields, not the whole schema.
#[test]
fn the_unqualified_prefix_expansion_respects_the_cap() {
    let temp = TempDir::new().unwrap();
    let store = open_store(
        &temp,
        QueryPolicy {
            max_default_fields: 2,
            expand_unqualified_prefix: true,
            ..Default::default()
        },
        None,
    );
    assert_eq!(hits(&store, "wa*"), 1);
    assert_eq!(hits(&store, "wc*"), 0, "c is past a cap of two");
}

#[test]
fn the_selection_rule() {
    let names = |list: &[&str]| list.iter().map(|s| s.to_string()).collect::<Vec<_>>();
    let candidates = names(&["d", "b", "a", "c"]);

    assert_eq!(
        select_default_fields(candidates.clone(), None, 0),
        (names(&["a", "b", "c", "d"]), false)
    );
    assert_eq!(
        select_default_fields(candidates.clone(), None, 2),
        (names(&["a", "b"]), true)
    );
    // Declared order is kept; a name the index cannot search this way is left out.
    let declared = names(&["c", "zz", "a"]);
    assert_eq!(
        select_default_fields(candidates.clone(), Some(&declared), 0),
        (names(&["c", "a"]), false)
    );
    assert_eq!(
        select_default_fields(candidates, Some(&declared), 1),
        (names(&["c"]), true)
    );
}

/// A declared list is checked against the schema, and every way it can be wrong is refused.
#[test]
fn a_declared_list_is_validated() {
    let mut schema = five_fields();
    let mut not_indexed = text_field();
    not_indexed.indexed = false;
    schema.fields.insert("off".to_string(), not_indexed);

    for (list, needle) in [
        (vec![], "empty"),
        (vec!["a", "a"], "twice"),
        (vec!["nope"], "not a field"),
        (vec!["n"], "cannot search"),
        (vec!["off"], "cannot search"),
    ] {
        schema.default_fields = Some(list.iter().map(|s| s.to_string()).collect());
        let err = schema.validate_default_fields().unwrap_err();
        assert!(err.contains(needle), "{list:?}: {err}");
    }

    schema.default_fields = Some(vec!["e".to_string(), "a".to_string()]);
    assert!(schema.validate_default_fields().is_ok());
    schema.default_fields = None;
    assert!(schema.validate_default_fields().is_ok());
}

/// An undeclared list adds nothing to the fingerprint, so every schema written before the field
/// existed keeps the fingerprint it had — and a declared one does change it, so two nodes that
/// disagree about the list are seen to.
#[test]
fn an_absent_list_leaves_the_fingerprint_alone() {
    let mut schema = five_fields();
    let before = schema.calculate_fingerprint();
    schema.default_fields = Some(vec!["a".to_string()]);
    assert_ne!(schema.calculate_fingerprint(), before);
    schema.default_fields = None;
    assert_eq!(schema.calculate_fingerprint(), before);
}

/// Declared at runtime, with no rebuild: the next search uses it.
#[test]
fn setting_the_list_takes_effect_without_a_rebuild() {
    let temp = TempDir::new().unwrap();
    let store = open_store(&temp, capped(0), None);
    assert_eq!(reached(&store).len(), 5);

    store
        .set_default_fields("idx", Some(vec!["b".to_string()]))
        .unwrap();
    assert_eq!(reached(&store), ["b"]);

    store.set_default_fields("idx", None).unwrap();
    assert_eq!(reached(&store).len(), 5);
}

/// The search says when the cap narrowed what a bare term reached — and only then.
///
/// Advisory, beside the hits rather than among the discarded clauses: nothing was dropped, so a
/// caller that refuses on a discarded clause must not refuse here.
#[test]
fn a_narrowed_search_says_so_and_only_when_it_was() {
    let temp = TempDir::new().unwrap();
    let store = open_store(&temp, capped(3), None);

    let outcome = store.search_documents("idx", "wa", 10, None).unwrap();
    assert!(outcome.discarded.is_empty(), "{:?}", outcome.discarded);
    assert_eq!(
        outcome.narrowed_default_fields,
        Some(storage::NarrowedDefaultFields {
            searched: vec!["a".into(), "b".into(), "c".into()],
            available: 5,
            declared: false,
        })
    );
    // Also on the count path, and inside a group or beside a qualified clause.
    for query in ["wa", "(wa OR b:wb)", "\"wa\""] {
        for limit in [0, 10] {
            assert!(
                store
                    .search_documents("idx", query, limit, None)
                    .unwrap()
                    .narrowed_default_fields
                    .is_some(),
                "{query:?} limit {limit}"
            );
        }
    }

    // A query naming every field it searches reached them all.
    for query in ["e:we", "e:(we wa)", "a:wa AND n:1"] {
        assert!(
            store
                .search_documents("idx", query, 10, None)
                .unwrap()
                .narrowed_default_fields
                .is_none(),
            "{query:?}"
        );
    }

    // Under the cap nothing was narrowed.
    let temp = TempDir::new().unwrap();
    let store = open_store(&temp, capped(5), None);
    assert!(
        store
            .search_documents("idx", "wa", 10, None)
            .unwrap()
            .narrowed_default_fields
            .is_none()
    );

    // A declared list cut by the cap says it was declared.
    let temp = TempDir::new().unwrap();
    let store = open_store(&temp, capped(1), Some(&["e", "c"]));
    let narrowed = store
        .search_documents("idx", "we", 10, None)
        .unwrap()
        .narrowed_default_fields
        .expect("narrowed");
    assert_eq!(narrowed.searched, ["e"]);
    assert_eq!(narrowed.available, 2);
    assert!(narrowed.declared);
}
