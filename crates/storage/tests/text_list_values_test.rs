//! A list in a text field is several values of it, as it is in every other scalar field.
//!
//! It was once indexed as its JSON spelling — `["FRITZ!Box 6670 CM","Guest"]` as one text — so a
//! phrase matched across two elements, and under the raw tokenizer no element could be matched,
//! only the whole list's spelling.

use serde_json::json;
use storage::{
    FieldDef, HybridStore, IndexSchema, SearchOutcome, StorageConfig, TantivyFieldType, WalOp,
};
use tempfile::TempDir;

fn test_config(path: std::path::PathBuf) -> StorageConfig {
    StorageConfig {
        max_open_indexes: 0,
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
        commit_interval_ms: 0,
        query: Default::default(),
    }
}

fn store_with(docs: Vec<(&str, serde_json::Value)>) -> (TempDir, HybridStore) {
    let dir = TempDir::new().unwrap();
    let mut schema = IndexSchema::default();
    for name in ["id", "ssids", "macs"] {
        let mut field = FieldDef::new(name.to_string(), TantivyFieldType::Text);
        if name == "macs" {
            field.tokenizer = Some("raw".to_string());
            field.index_record_option = Some("Basic".to_string());
        }
        schema.fields.insert(name.to_string(), field);
    }
    schema.normalize_after_deserialization();
    let store = HybridStore::new(test_config(dir.path().to_path_buf()), 1).unwrap();
    store.store_schema_and_cache("wifi", &schema).unwrap();
    let ops = docs
        .into_iter()
        .map(|(id, doc)| WalOp::Put {
            id: id.to_string(),
            json_blob: Some(doc),
        })
        .collect();
    store.apply_batch("wifi", ops).unwrap();
    store.commit_index("wifi").unwrap();
    (dir, store)
}

fn total(store: &HybridStore, query: &str) -> usize {
    let SearchOutcome { total_hits, .. } = store
        .search_documents("wifi", query, 10, None)
        .unwrap_or_else(|e| panic!("{query}: {e}"));
    total_hits
}

#[test]
fn each_element_of_a_text_list_is_a_value_of_its_own() {
    let (_dir, store) = store_with(vec![(
        "a",
        json!({
            "id": "a",
            "ssids": ["FRITZ!Box 6670 CM", "Guest Network"],
            "macs": ["00:7A:A4:E5:90:28", "00:7A:A4:E5:90:29"],
        }),
    )]);

    // Each element is found as itself…
    assert_eq!(total(&store, r#"ssids:"FRITZ!Box 6670 CM""#), 1);
    assert_eq!(total(&store, r#"ssids:"Guest Network""#), 1);
    // …and a phrase does not run from one element into the next.
    assert_eq!(total(&store, r#"ssids:"CM Guest""#), 0);

    // Under the raw tokenizer each element is one term, matched whole.
    assert_eq!(total(&store, r#"macs:"00:7A:A4:E5:90:29""#), 1);
    assert_eq!(total(&store, r#"macs:"00:7A:A4:E5""#), 0);

    // The document comes back with the list it was given.
    let SearchOutcome { hits, .. } = store
        .search_documents("wifi", r#"macs:"00:7A:A4:E5:90:28""#, 1, None)
        .unwrap();
    assert_eq!(
        hits[0].1["ssids"],
        json!(["FRITZ!Box 6670 CM", "Guest Network"])
    );
}
