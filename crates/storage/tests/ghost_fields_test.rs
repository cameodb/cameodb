//! Ghost fields (ROADMAP Q1): columns filled from another field of the stored body, under a
//! condition or for every document, and never stored.
//!
//! The index here is a text stored once in `text`, its language in `lang`, an English stemmed
//! column `text_en` fed from `text` where `lang` is `en`, and `text_rest` fed from the rest. Each
//! document's text carries a word of its own (`doc-a`, …) and the shared word `shared`, so a
//! search says which column a document reached.

use serde_json::{Value as JsonValue, json};
use storage::{HybridStore, IndexSchema, StorageConfig, WalOp};
use tempfile::TempDir;

const INDEX: &str = "ghosts";

fn config(path: &std::path::Path) -> StorageConfig {
    StorageConfig {
        max_open_indexes: 0,
        shard_path: path.to_path_buf(),
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

fn open_store(path: &std::path::Path) -> HybridStore {
    HybridStore::new(config(path), 1).expect("store")
}

/// A schema as `PUT /_config` takes it, normalized and checked as every node stores it.
fn declared(body: JsonValue) -> IndexSchema {
    let mut schema: IndexSchema = serde_json::from_value(body).expect("declaration");
    schema.normalize_after_deserialization();
    schema.validate_ghosts().expect("valid ghosts");
    schema
}

fn languages(text_indexed: bool) -> IndexSchema {
    declared(json!({"fields": {
        "text": {"type": "text", "indexed": text_indexed},
        "lang": {"type": "string"},
        "text_en": {"type": "text", "tokenizer": "en_stem",
                    "ghost": {"source": "text", "when": {"field": "lang", "in": ["en"]}}},
        "text_rest": {"type": "text",
                      "ghost": {"source": "text", "when": {"field": "lang", "not_in": ["en"]}}},
    }}))
}

fn put(store: &HybridStore, id: &str, body: JsonValue) {
    store
        .apply_write(
            INDEX,
            WalOp::Put {
                id: id.to_string(),
                json_blob: Some(body),
            },
        )
        .expect("write");
}

/// The ids a query finds, sorted.
fn found(store: &HybridStore, query: &str) -> Vec<String> {
    let outcome = store
        .search_documents(INDEX, query, 100, None)
        .expect("search");
    let mut ids: Vec<String> = outcome
        .hits
        .iter()
        .map(|(_, hit)| hit["id"].as_str().expect("id").to_string())
        .collect();
    ids.sort();
    ids
}

fn stored(store: &HybridStore, id: &str) -> JsonValue {
    let bytes = store.get_by_key(INDEX, id).expect("read").expect("stored");
    serde_json::from_slice::<JsonValue>(&bytes).expect("json")["json_blob"].clone()
}

/// Every kind of document `lang` can describe.
fn write_languages(store: &HybridStore) {
    put(
        store,
        "en",
        json!({"lang": "en", "text": "doc-en running shared"}),
    );
    put(
        store,
        "hr",
        json!({"lang": "hr", "text": "doc-hr running shared"}),
    );
    put(
        store,
        "mixed",
        json!({"lang": ["de", "en"], "text": "doc-mixed running shared"}),
    );
    put(store, "none", json!({"text": "doc-none running shared"}));
    put(
        store,
        "number",
        json!({"lang": 7, "text": "doc-number running shared"}),
    );
    // No source: neither ghost holds anything for it.
    put(store, "empty", json!({"lang": "en"}));
}

#[test]
fn each_ghost_holds_the_documents_its_condition_selects() {
    let temp = TempDir::new().unwrap();
    let store = open_store(temp.path());
    store
        .store_schema_and_cache(INDEX, &languages(true))
        .unwrap();
    write_languages(&store);
    store.commit_index(INDEX).unwrap();

    // Stemmed under `en_stem` where `lang` is `en`, or an array holding it.
    assert_eq!(found(&store, "text_en:run"), ["en", "mixed"]);
    // The rest, under the default tokenizer: no stem, every document `in` does not take.
    assert_eq!(found(&store, "text_rest:run"), Vec::<String>::new());
    assert_eq!(found(&store, "text_rest:running"), ["hr", "none", "number"]);
    // An `in`/`not_in` pair on one list: every document with the source, exactly once.
    let mut both = found(&store, "text_en:shared");
    both.extend(found(&store, "text_rest:shared"));
    both.sort();
    assert_eq!(both, ["en", "hr", "mixed", "none", "number"]);
    // The source's own column is untouched.
    assert_eq!(found(&store, "text:running").len(), 5);
}

#[test]
fn a_ghost_with_no_condition_reads_every_document_with_the_source() {
    let temp = TempDir::new().unwrap();
    let store = open_store(temp.path());
    let schema = declared(json!({"fields": {
        "text": {"type": "text"},
        "text_stem": {"type": "text", "tokenizer": "en_stem", "ghost": {"source": "text"}},
    }}));
    store.store_schema_and_cache(INDEX, &schema).unwrap();
    put(&store, "a", json!({"text": "running"}));
    put(&store, "b", json!({"other": "running"}));
    store.commit_index(INDEX).unwrap();

    assert_eq!(found(&store, "text_stem:run"), ["a"]);
    assert!(
        found(&store, "text:run").is_empty(),
        "the source keeps its own analysis"
    );
}

/// A ghost is never stored and never in a hit — a body naming it, written by a route that did not
/// refuse it, has the name taken out before it is stored, and the column is fed from the source.
#[test]
fn a_ghost_is_neither_stored_nor_returned_and_reads_only_its_source() {
    let temp = TempDir::new().unwrap();
    let store = open_store(temp.path());
    store
        .store_schema_and_cache(INDEX, &languages(true))
        .unwrap();
    put(
        &store,
        "en",
        json!({"lang": "en", "text": "running", "text_en": "zebra"}),
    );
    store.commit_index(INDEX).unwrap();

    assert_eq!(found(&store, "text_en:run"), ["en"]);
    assert!(found(&store, "text_en:zebra").is_empty());
    assert_eq!(
        stored(&store, "en"),
        json!({"lang": "en", "text": "running"})
    );

    let outcome = store
        .search_documents(INDEX, "text_en:run", 10, None)
        .unwrap();
    let hit = &outcome.hits[0].1;
    assert!(hit.get("text_en").is_none(), "{hit}");
    assert_eq!(hit["text"], "running");
}

/// The builder is the write's and the replay's, so a document replayed from the WAL indexes
/// exactly as it did when written.
#[test]
fn a_replayed_document_feeds_its_ghosts_as_the_write_did() {
    let temp = TempDir::new().unwrap();
    {
        let store = open_store(temp.path());
        store
            .store_schema_and_cache(INDEX, &languages(true))
            .unwrap();
        write_languages(&store);
        // No commit: this is the crash.
    }
    let store = open_store(temp.path());
    store.recover_indices().expect("recovery");
    store.commit_index(INDEX).unwrap();

    assert_eq!(found(&store, "text_en:run"), ["en", "mixed"]);
    assert_eq!(found(&store, "text_rest:running"), ["hr", "none", "number"]);
}

/// With the source unindexed the text is stored once and searched only through the ghosts, and
/// a query naming the source says where.
#[test]
fn an_unindexed_source_feeds_its_ghosts_and_comes_back_in_hits() {
    let temp = TempDir::new().unwrap();
    let store = open_store(temp.path());
    store
        .store_schema_and_cache(INDEX, &languages(false))
        .unwrap();
    write_languages(&store);
    store.commit_index(INDEX).unwrap();

    assert_eq!(found(&store, "text_en:run"), ["en", "mixed"]);
    let outcome = store
        .search_documents(INDEX, "text_en:run", 10, None)
        .unwrap();
    assert!(outcome.hits.iter().all(|(_, hit)| hit["text"].is_string()));

    let outcome = store
        .search_documents(INDEX, "text:running", 10, None)
        .unwrap();
    assert!(outcome.hits.is_empty());
    assert!(
        outcome
            .discarded
            .iter()
            .any(|note| note.contains("searched through 'text_en', 'text_rest'")),
        "{:?}",
        outcome.discarded
    );
}

/// A plain field made a ghost of a populated index keeps its column: documents written before
/// and after answer alike, neither hit carries the field, and the old body is as it was.
#[test]
fn a_field_made_a_ghost_applies_from_the_change_on() {
    let temp = TempDir::new().unwrap();
    let store = open_store(temp.path());
    let plain = declared(json!({"fields": {
        "text": {"type": "text"},
        "lang": {"type": "string"},
        "text_en": {"type": "text", "tokenizer": "en_stem"},
    }}));
    store.store_schema_and_cache(INDEX, &plain).unwrap();
    let before = json!({"lang": "en", "text": "running", "text_en": "running"});
    put(&store, "before", before.clone());
    store.commit_index(INDEX).unwrap();

    let ghost = declared(json!({"fields": {
        "text": {"type": "text"},
        "lang": {"type": "string"},
        "text_en": {"type": "text", "tokenizer": "en_stem",
                    "ghost": {"source": "text", "when": {"field": "lang", "in": ["en"]}}},
    }}));
    let change = plain.rebuild_changes(&ghost);
    assert!(
        change.iter().all(|c| c.new_writes && !c.conflicts),
        "{change:?}"
    );
    store.store_schema_and_cache(INDEX, &ghost).unwrap();
    put(&store, "after", json!({"lang": "en", "text": "runs"}));
    store.commit_index(INDEX).unwrap();

    assert_eq!(found(&store, "text_en:run"), ["after", "before"]);
    let outcome = store
        .search_documents(INDEX, "text_en:run", 10, None)
        .unwrap();
    for (_, hit) in &outcome.hits {
        assert!(hit.get("text_en").is_none(), "{hit}");
    }
    assert_eq!(stored(&store, "before"), before);
}
