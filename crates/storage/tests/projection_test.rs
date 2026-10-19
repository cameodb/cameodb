//! Field projection pushed down to the shard.
//!
//! `apply_field_projection` used to run once, after the merge, on the node. Running it in
//! `search_documents_with` instead means the wire between shard and merge carries the
//! projected shape — which is the whole point on a query matching tens of thousands of
//! documents. These cover the two contracts: only the asked-for fields (plus `_`-prefixed
//! metadata) come back, and a shadow field — the key under its source name — answers to
//! its caller-facing name like any other field.

use std::collections::HashMap;

use storage::{
    FieldDef, HybridStore, IndexSchema, SearchOptions, StorageConfig, TantivyFieldType, WalOp,
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

/// A `SearchOptions` asking only for the named fields — the projection as a caller's
/// `return` clause becomes it.
fn only(fields: &[String]) -> SearchOptions<'_> {
    SearchOptions {
        deadline: None,
        fields: Some(fields),
        ..Default::default()
    }
}

#[test]
fn a_projection_keeps_only_the_fields_named() {
    let temp = TempDir::new().unwrap();
    let store = HybridStore::new(config(temp.path().to_path_buf()), 1).unwrap();

    let mut fields = HashMap::new();
    fields.insert("title".into(), field(TantivyFieldType::Text, true));
    fields.insert("body".into(), field(TantivyFieldType::Text, true));
    fields.insert("year".into(), field(TantivyFieldType::U64, true));
    store
        .store_schema_and_cache(
            "papers",
            &IndexSchema {
                fields,
                ..Default::default()
            },
        )
        .unwrap();

    store
        .apply_write(
            "papers",
            WalOp::Put {
                id: "d1".to_string(),
                json_blob: Some(serde_json::json!({
                    "title": "rust programming",
                    "body": "a small blue bike",
                    "year": 2024u64,
                })),
            },
        )
        .unwrap();
    store.commit_index("papers").unwrap();

    let keep = vec!["title".to_string()];
    let outcome = store
        .search_documents_with("papers", "title:rust", 10, None, &only(&keep))
        .unwrap();
    assert_eq!(outcome.hits.len(), 1);
    let hit = &outcome.hits[0].1;
    let object = hit.as_object().expect("a hit is an object");
    assert_eq!(
        hit.get("title"),
        Some(&serde_json::json!("rust programming"))
    );
    for key in object.keys() {
        assert!(
            key == "title" || key.starts_with('_'),
            "projection let '{key}' through: {hit}"
        );
    }
}

/// On an index with a shadow field the identifier travels under the shadow name, and the
/// projection is applied after reconstruction — so asking for the shadow name gets it,
/// exactly as the node-level projection did.
#[test]
fn a_projection_on_a_shadow_index_answers_the_shadow_name() {
    let temp = TempDir::new().unwrap();
    let store = HybridStore::new(config(temp.path().to_path_buf()), 1).unwrap();

    let mut fields = HashMap::new();
    fields.insert("id".into(), field(TantivyFieldType::Text, true));
    fields.insert("title".into(), field(TantivyFieldType::Text, true));
    let mut shadow = field(TantivyFieldType::Text, false);
    shadow.is_shadow = true;
    fields.insert("sha1".into(), shadow);
    store
        .store_schema_and_cache(
            "files",
            &IndexSchema {
                fields,
                ..Default::default()
            },
        )
        .unwrap();

    store
        .apply_write(
            "files",
            WalOp::Put {
                id: "d1".to_string(),
                json_blob: Some(serde_json::json!({"id": "d1", "title": "a file"})),
            },
        )
        .unwrap();
    store.commit_index("files").unwrap();

    let keep = vec!["sha1".to_string()];
    let outcome = store
        .search_documents_with("files", "title:file", 10, None, &only(&keep))
        .unwrap();
    assert_eq!(outcome.hits.len(), 1);
    let hit = &outcome.hits[0].1;
    assert_eq!(
        hit.get("sha1"),
        Some(&serde_json::json!("d1")),
        "the shadow name reconstructs the key, projected or not: {hit}"
    );
    assert!(
        hit.get("title").is_none(),
        "the projection trimmed the rest: {hit}"
    );
}

/// The array cap rides the same fetch the projection does, so every way into a hit — a
/// scored query, the `id:` lookup that skips Tantivy — comes back capped, and a projection
/// that leaves the array out leaves nothing to account for.
#[test]
fn an_array_limit_caps_every_hit_and_reports_the_original_lengths() {
    let temp = TempDir::new().unwrap();
    let store = HybridStore::new(config(temp.path().to_path_buf()), 1).unwrap();

    let mut fields = HashMap::new();
    fields.insert("title".into(), field(TantivyFieldType::Text, true));
    store
        .store_schema_and_cache(
            "intel",
            &IndexSchema {
                fields,
                ..Default::default()
            },
        )
        .unwrap();

    let addresses: Vec<String> = (0..5_000)
        .map(|n| format!("10.0.{}.{}", n / 256, n % 256))
        .collect();
    store
        .apply_write(
            "intel",
            WalOp::Put {
                id: "d1".to_string(),
                json_blob: Some(serde_json::json!({
                    "title": "sample report",
                    "string_ipv4": addresses,
                    "dns": {"names": ["a.example", "b.example", "c.example", "d.example"]},
                })),
            },
        )
        .unwrap();
    store.commit_index("intel").unwrap();

    let capped = SearchOptions {
        array_limit: Some(3),
        ..Default::default()
    };
    for query in ["title:sample", "id:d1"] {
        let outcome = store
            .search_documents_with("intel", query, 10, None, &capped)
            .unwrap();
        assert_eq!(outcome.hits.len(), 1, "{query}");
        let hit = &outcome.hits[0].1;
        assert_eq!(
            hit["string_ipv4"],
            serde_json::json!(["10.0.0.0", "10.0.0.1", "10.0.0.2"]),
            "{query}: the first elements, in stored order"
        );
        assert_eq!(hit["dns"]["names"].as_array().unwrap().len(), 3, "{query}");
        assert_eq!(
            hit[storage::TRUNCATED_ARRAYS_FIELD],
            serde_json::json!({"string_ipv4": 5000, "dns.names": 4}),
            "{query}"
        );
    }

    let keep = vec!["title".to_string()];
    let projected = SearchOptions {
        fields: Some(&keep),
        array_limit: Some(3),
        ..Default::default()
    };
    let outcome = store
        .search_documents_with("intel", "title:sample", 10, None, &projected)
        .unwrap();
    let hit = &outcome.hits[0].1;
    assert!(
        hit.get(storage::TRUNCATED_ARRAYS_FIELD).is_none(),
        "a projected-away array was not cut, so nothing says it was: {hit}"
    );

    let whole = store
        .search_documents_with("intel", "title:sample", 10, None, &SearchOptions::default())
        .unwrap();
    assert_eq!(
        whole.hits[0].1["string_ipv4"].as_array().unwrap().len(),
        5_000,
        "no cap asked for, none applied"
    );
}
