//! The two-phase search: `search_keys` answers keys and sort keys, `fetch_documents`
//! answers the bodies behind the keys a merge kept.
//!
//! What these pin is the contract the node builds on: a `KeyHit` carries everything the
//! merge needs to order and address a document — the normalized sort value beside the
//! hit, since the document is not there to read it off — and the composition of the two
//! phases returns exactly what `search_documents` always returned.

use std::collections::HashMap;

use serde_json::json;
use storage::{
    FieldDef, HybridStore, IndexSchema, SearchOptions, SortKey, SortOrder, SortSpec, StorageConfig,
    TantivyFieldType, WalOp,
};
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
        quick_repair: true,
        commit_interval_ms: 0,
        query: Default::default(),
    }
}

fn field(field_type: TantivyFieldType, fast: bool) -> FieldDef {
    let mut def = FieldDef::new("placeholder".to_string(), field_type);
    def.fast = Some(fast);
    def.indexed = true;
    def.stored = false;
    def
}

/// The fixture the handoff prescribes: `n` a u64 fast column, `created` a date fast
/// column, `code` a string fast column, `title` plain text — every document matching
/// `title:doc`, with `n`, `created` and `code` deliberately ordered differently so a
/// sort on any of them is a different answer.
const DATES: [&str; 4] = [
    "2021-03-04T00:00:00Z",
    "2019-01-01T00:00:00Z",
    "2023-12-31T00:00:00Z",
    "2020-06-15T00:00:00Z",
];
const CODES: [&str; 4] = ["zz-last", "aa-first", "mm-mid", "bb-second"];

fn store_with_docs(temp: &TempDir, index: &str) -> HybridStore {
    let store = HybridStore::new(config(temp.path().to_path_buf()), 1).unwrap();

    let mut fields = HashMap::new();
    fields.insert("title".into(), field(TantivyFieldType::Text, false));
    fields.insert("n".into(), field(TantivyFieldType::U64, true));
    fields.insert("created".into(), field(TantivyFieldType::Date, true));
    fields.insert("code".into(), field(TantivyFieldType::String, true));
    let schema = IndexSchema {
        fields,
        ..Default::default()
    };
    store.store_schema_and_cache(index, &schema).unwrap();

    let ops: Vec<WalOp> = (0..4u64)
        .map(|i| {
            let i = i as usize;
            WalOp::Put {
                id: format!("d{i}"),
                json_blob: Some(json!({
                    "title": format!("doc number {i}"),
                    "n": i as u64,
                    "created": DATES[i],
                    "code": CODES[i],
                })),
            }
        })
        .collect();
    store.apply_batch(index, ops).unwrap();
    store.commit_index(index).unwrap();
    store
}

/// A shadow-field index: `sha1` stands for the key under its source name.
fn store_with_shadow(temp: &TempDir, index: &str) -> HybridStore {
    let store = HybridStore::new(config(temp.path().to_path_buf()), 1).unwrap();

    let mut fields = HashMap::new();
    fields.insert("id".into(), field(TantivyFieldType::Text, true));
    fields.insert("title".into(), field(TantivyFieldType::Text, true));
    let mut shadow = field(TantivyFieldType::Text, false);
    shadow.is_shadow = true;
    fields.insert("sha1".into(), shadow);
    let schema = IndexSchema {
        fields,
        ..Default::default()
    };
    store.store_schema_and_cache(index, &schema).unwrap();

    let ops: Vec<WalOp> = ["ccc", "aaa", "bbb"]
        .into_iter()
        .map(|id| WalOp::Put {
            id: id.to_string(),
            json_blob: Some(json!({"id": id, "title": "doc"})),
        })
        .collect();
    store.apply_batch(index, ops).unwrap();
    store.commit_index(index).unwrap();
    store
}

fn sorted(field_name: &str, order: SortOrder) -> Option<SortSpec> {
    Some(SortSpec {
        field: field_name.to_string(),
        order,
    })
}

#[test]
fn a_numeric_sort_answers_u64_keys_in_order() {
    let temp = TempDir::new().unwrap();
    let store = store_with_docs(&temp, "bench");

    for (order, expected) in [
        (SortOrder::Asc, [0u64, 1, 2, 3]),
        (SortOrder::Desc, [3u64, 2, 1, 0]),
    ] {
        let outcome = store
            .search_keys(
                "bench",
                "title:doc",
                10,
                sorted("n", order).as_ref(),
                &SearchOptions::default(),
            )
            .unwrap();
        let keys: Vec<u64> = outcome
            .hits
            .iter()
            .map(|hit| match &hit.sort_key {
                Some(SortKey::U64(v)) => *v,
                other => panic!("expected U64 sort key, got {other:?}"),
            })
            .collect();
        assert_eq!(keys, expected, "{order:?} sort");
    }
}

#[test]
fn a_date_sort_answers_epoch_second_keys() {
    let temp = TempDir::new().unwrap();
    let store = store_with_docs(&temp, "bench");

    let outcome = store
        .search_keys(
            "bench",
            "title:doc",
            10,
            sorted("created", SortOrder::Asc).as_ref(),
            &SearchOptions::default(),
        )
        .unwrap();

    // The key is the epoch second the column ordered on — the same value
    // `date_sort_secs` reads off the stored string, so a merge comparing either sees
    // the same number.
    let mut keys = Vec::new();
    for hit in &outcome.hits {
        let secs = match hit.sort_key {
            Some(SortKey::Date(secs)) => secs,
            ref other => panic!("expected Date sort key, got {other:?}"),
        };
        keys.push(secs);
    }
    // (match on a Copy variant — `hit.sort_key` stays usable below)
    let mut expected: Vec<i64> = DATES
        .iter()
        .map(|s| storage::date_sort_secs(&json!(s)).unwrap())
        .collect();
    expected.sort_unstable();
    assert_eq!(keys, expected);
}

#[test]
fn a_string_fast_sort_answers_str_keys_in_order() {
    let temp = TempDir::new().unwrap();
    let store = store_with_docs(&temp, "bench");

    let outcome = store
        .search_keys(
            "bench",
            "title:doc",
            10,
            sorted("code", SortOrder::Asc).as_ref(),
            &SearchOptions::default(),
        )
        .unwrap();
    let keys: Vec<String> = outcome
        .hits
        .iter()
        .map(|hit| match &hit.sort_key {
            Some(SortKey::Str(s)) => s.clone(),
            other => panic!("expected Str sort key, got {other:?}"),
        })
        .collect();
    let mut expected: Vec<String> = CODES.iter().map(|s| s.to_string()).collect();
    expected.sort();
    assert_eq!(keys, expected);
}

#[test]
fn a_document_key_sort_answers_the_ids_as_keys() {
    let temp = TempDir::new().unwrap();
    let store = store_with_shadow(&temp, "files");

    let outcome = store
        .search_keys(
            "files",
            "title:doc",
            10,
            sorted("id", SortOrder::Asc).as_ref(),
            &SearchOptions::default(),
        )
        .unwrap();
    assert_eq!(outcome.hits.len(), 3);
    for hit in &outcome.hits {
        match &hit.sort_key {
            Some(SortKey::Str(s)) => assert_eq!(s, &hit.id, "the key is the id itself"),
            other => panic!("expected Str sort key, got {other:?}"),
        }
    }
    let ids: Vec<&str> = outcome.hits.iter().map(|h| h.id.as_str()).collect();
    assert_eq!(ids, ["aaa", "bbb", "ccc"]);
}

#[test]
fn an_unsorted_search_answers_scores_and_no_keys() {
    let temp = TempDir::new().unwrap();
    let store = store_with_docs(&temp, "bench");

    let outcome = store
        .search_keys("bench", "title:doc", 10, None, &SearchOptions::default())
        .unwrap();
    assert_eq!(outcome.hits.len(), 4);
    assert!(
        outcome.hits.iter().all(|hit| hit.sort_key.is_none()),
        "no sort requested means no keys carried"
    );
    for pair in outcome.hits.windows(2) {
        assert!(
            pair[0].score >= pair[1].score,
            "scores are non-increasing: {:?}",
            outcome.hits
        );
    }
}

#[test]
fn fetch_documents_answers_in_key_order_and_skips_the_unknown() {
    let temp = TempDir::new().unwrap();
    let store = store_with_docs(&temp, "bench");

    let ids = vec!["d2".to_string(), "missing".to_string(), "d0".to_string()];
    let fields = vec!["title".to_string()];
    let outcome = store
        .fetch_documents("bench", &ids, Some(&fields), None, None)
        .unwrap();

    let got: Vec<&str> = outcome.docs.iter().map(|(id, _)| id.as_str()).collect();
    assert_eq!(got, ["d2", "d0"], "input order, the unknown id skipped");
    for (_, doc) in &outcome.docs {
        let object = doc.as_object().unwrap();
        assert!(object.contains_key("title"), "projection kept the field");
        assert!(
            !object.contains_key("code") && !object.contains_key("n"),
            "projection dropped the rest: {doc}"
        );
    }
}

#[test]
fn the_composition_answers_what_the_key_phase_ordered() {
    let temp = TempDir::new().unwrap();
    let store = store_with_docs(&temp, "bench");
    let sort = sorted("n", SortOrder::Asc);

    let keys = store
        .search_keys(
            "bench",
            "title:doc",
            3,
            sort.as_ref(),
            &SearchOptions::default(),
        )
        .unwrap();
    let docs = store
        .search_documents_with(
            "bench",
            "title:doc",
            3,
            sort.as_ref(),
            &SearchOptions::default(),
        )
        .unwrap();

    let key_ids: Vec<&str> = keys.hits.iter().map(|h| h.id.as_str()).collect();
    let doc_ids: Vec<&str> = docs
        .hits
        .iter()
        .map(|(_, doc)| doc.get("id").and_then(|v| v.as_str()).unwrap())
        .collect();
    assert_eq!(key_ids, doc_ids);
    assert_eq!(docs.total_hits, keys.total_hits);
}
