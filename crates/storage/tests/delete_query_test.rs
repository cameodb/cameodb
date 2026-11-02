//! What a delete by query finds on a shard (ROADMAP S1): exact clauses only, every match, and a
//! `text` clause checked against the stored value.

use serde_json::{Value as JsonValue, json};
use storage::{HybridStore, IndexSchema, StorageConfig, StoreError, WalOp};
use tempfile::TempDir;

const INDEX: &str = "events";

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

fn store_with(temp: &TempDir, docs: &[(&str, JsonValue)]) -> HybridStore {
    let store = HybridStore::new(config(temp.path()), 1).expect("store");
    let mut schema: IndexSchema = serde_json::from_value(json!({"fields": {
        "status": {"type": "string"},
        "company": {"type": "text"},
        "notes": {"type": "text"},
        "port": {"type": "u64"},
        "score": {"type": "f64"},
        "seen": {"type": "date"},
        "addr": {"type": "ip"},
        "active": {"type": "boolean"},
        "meta": {"type": "json"},
        "lang": {"type": "string"},
        "notes_en": {"type": "text", "tokenizer": "en_stem",
                     "ghost": {"source": "notes", "when": {"field": "lang", "in": ["en"]}}},
    }}))
    .expect("schema");
    schema.normalize_after_deserialization();
    schema.validate_ghosts().expect("ghosts");
    store
        .store_schema_and_cache(INDEX, &schema)
        .expect("store schema");
    let ops = docs
        .iter()
        .map(|(id, body)| WalOp::Put {
            id: id.to_string(),
            json_blob: Some(body.clone()),
        })
        .collect();
    store
        .apply_batch_and_maybe_commit(INDEX, ops)
        .expect("write");
    store.commit_index(INDEX).expect("commit");
    store
}

fn sample(temp: &TempDir) -> HybridStore {
    store_with(
        temp,
        &[
            (
                "a",
                json!({"status": "expired", "company": "Acme Corp", "port": 22, "score": 1.5,
                         "seen": "2025-01-10T00:00:00Z", "addr": "10.0.0.1", "active": false}),
            ),
            (
                "b",
                json!({"status": "expired", "company": "ACME CORP", "port": 443, "score": 7.0,
                         "seen": "2026-03-01T00:00:00Z", "addr": "10.0.9.9", "active": true}),
            ),
            (
                "c",
                json!({"status": "live", "company": ["Other Ltd", "Acme Corp"], "port": 3389,
                         "score": 9.5, "seen": "2026-06-01T00:00:00Z", "addr": "192.168.1.1",
                         "active": true}),
            ),
            (
                "d",
                json!({"status": "Expired", "company": "Beta", "port": 8080, "score": 3.0,
                         "seen": "2024-01-01T00:00:00Z", "addr": "2001:db8::1", "active": false}),
            ),
        ],
    )
}

fn matched(store: &HybridStore, query: &str) -> (Vec<String>, usize) {
    let found = store
        .delete_query_matches(INDEX, query, None)
        .unwrap_or_else(|err| panic!("{query}: {err}"));
    let mut ids = found.ids;
    ids.sort();
    (ids, found.skipped_inexact)
}

fn refusal(store: &HybridStore, query: &str) -> String {
    match store.delete_query_matches(INDEX, query, None) {
        Ok(found) => panic!("{query}: accepted, matching {:?}", found.ids),
        Err(err @ StoreError::QueryParser(_)) => err.to_string(),
        Err(other) => panic!("{query}: refused the wrong way: {other}"),
    }
}

#[test]
fn exact_clauses_on_every_allowed_type() {
    let temp = TempDir::new().unwrap();
    let store = sample(&temp);
    let ids = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();
    for (query, expected) in [
        ("status:expired", ids(&["a", "b"])),
        ("status: IN [live Expired]", ids(&["c", "d"])),
        ("status:[expired TO live]", ids(&["a", "b", "c"])),
        ("port:443", ids(&["b"])),
        ("port:[1000 TO 9000]", ids(&["c", "d"])),
        ("port: IN [22 8080]", ids(&["a", "d"])),
        ("score:>5", ids(&["b", "c"])),
        ("seen:<2025-06-01T00:00:00Z", ids(&["a", "d"])),
        ("addr:10.0.0.0/16", ids(&["a", "b"])),
        ("addr:2001:db8::1", ids(&["d"])),
        ("active:true", ids(&["b", "c"])),
        ("id:a", ids(&["a"])),
        ("id: IN [a c]", ids(&["a", "c"])),
        ("status:expired AND port:22", ids(&["a"])),
        ("status:expired OR status:live", ids(&["a", "b", "c"])),
        ("status:expired -port:22", ids(&["b"])),
    ] {
        assert_eq!(matched(&store, query), (expected, 0), "{query}");
    }
}

#[test]
fn a_text_clause_deletes_only_the_exact_stored_value() {
    let temp = TempDir::new().unwrap();
    let store = sample(&temp);
    // The phrase finds "Acme Corp", "ACME CORP" (the index lowercases) and an array holding
    // "Acme Corp"; only the exact values are kept.
    assert_eq!(
        matched(&store, r#"company:"Acme Corp""#),
        (vec!["a".to_string(), "c".to_string()], 1)
    );
    assert_eq!(
        matched(&store, r#"company:"Acme Corp" AND status:expired"#),
        (vec!["a".to_string()], 1),
        "b is expired too, and holds ACME CORP"
    );
    assert_eq!(
        matched(&store, r#"company:"acme corp""#),
        (vec![], 3),
        "case counts"
    );
}

#[test]
fn what_a_delete_cannot_run_is_refused() {
    let temp = TempDir::new().unwrap();
    let store = sample(&temp);
    for (query, says) in [
        ("expired", "names no field"),
        ("*", "every document"),
        ("status:exp*", "prefix"),
        (r#"company:"acme corp"~2"#, "slop"),
        ("company:acme", "quoted phrase"),
        (r#"company:"Acme Corp" OR status:live"#, "must be required"),
        (r#"status:live -company:"Acme Corp""#, "must be required"),
        ("company:[a TO b]", "text field"),
        ("company: IN [acme beta]", "text field"),
        ("notes_en:running", "ghost field"),
        ("meta:x", "json field"),
        ("nosuch:x", "not a field"),
        ("id:[a TO c]", "document key"),
        ("-status:live", "nothing to match"),
        ("status:live OR NOT port:22", "every document"),
    ] {
        let text = refusal(&store, query);
        assert!(text.contains(says), "{query}: {text}");
        assert!(text.contains("nothing was deleted"), "{query}: {text}");
    }
}

#[test]
fn every_match_is_found_not_a_page() {
    let temp = TempDir::new().unwrap();
    let docs: Vec<(String, JsonValue)> = (0..5_000)
        .map(|n| {
            (
                format!("doc-{n:05}"),
                json!({"status": if n % 2 == 0 { "expired" } else { "live" }, "port": n}),
            )
        })
        .collect();
    let refs: Vec<(&str, JsonValue)> = docs
        .iter()
        .map(|(id, b)| (id.as_str(), b.clone()))
        .collect();
    let store = store_with(&temp, &refs);
    let (ids, _) = matched(&store, "status:expired");
    assert_eq!(ids.len(), 2_500);
    assert_eq!(ids.first().map(String::as_str), Some("doc-00000"));
}

#[test]
fn a_shard_without_the_index_has_nothing_to_delete() {
    let temp = TempDir::new().unwrap();
    let store = HybridStore::new(config(temp.path()), 1).expect("store");
    let found = store
        .delete_query_matches("absent", "status:expired", None)
        .expect("no index is nothing to delete");
    assert!(found.ids.is_empty());
}
