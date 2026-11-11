//! A single term and match-all are counted by the index rather than by walking their
//! matches, so their relevance page can prune. The count still has to be the live count:
//! a segment with deletes answers a term's document frequency with the deleted documents
//! in it, and the count must not.

use serde_json::json;
use storage::{HybridStore, IndexSchema, SearchOptions, StorageConfig, WalOp};
use tempfile::TempDir;

const INDEX: &str = "docs";

fn store() -> (TempDir, HybridStore) {
    let temp = TempDir::new().expect("temp dir");
    let config = StorageConfig {
        shard_path: temp.path().to_path_buf(),
        default_batch_size: 100_000,
        ..StorageConfig::default()
    };
    let store = HybridStore::new(config, 1).expect("store");
    let mut schema: IndexSchema = serde_json::from_value(json!({"fields": {
        "tag": {"type": "string"},
        "title": {"type": "text"},
    }}))
    .expect("schema");
    schema.normalize_after_deserialization();
    store
        .store_schema_and_cache(INDEX, &schema)
        .expect("store schema");
    // 300 documents, tag a on the even ones; then 50 of the a's are deleted, so the
    // segment carries deletes and a's document frequency (150) is not its live count (100).
    let ops = (0..300)
        .map(|i| WalOp::Put {
            id: format!("d{i}"),
            json_blob: Some(json!({
                "tag": if i % 2 == 0 { "a" } else { "b" },
                "title": format!("doc number {i}"),
            })),
        })
        .collect();
    store.apply_batch(INDEX, ops).expect("write");
    store.commit_index(INDEX).expect("commit");
    let deletes = (0..100)
        .step_by(2)
        .map(|i| WalOp::Delete {
            id: format!("d{i}"),
        })
        .collect();
    store.apply_batch(INDEX, deletes).expect("delete");
    store.commit_index(INDEX).expect("commit");
    (temp, store)
}

#[test]
fn a_counted_query_answers_the_live_count_on_a_segment_with_deletes() {
    let (_temp, store) = store();
    for (query, live) in [
        ("tag:a", 100),
        ("tag:b", 150),
        ("title:doc", 250),
        ("*", 250),
        // A union is walked, not counted: the same answer by the other path.
        ("tag:a OR tag:b", 250),
    ] {
        for limit in [0, 10] {
            let outcome = store
                .search_keys(INDEX, query, limit, None, &SearchOptions::default())
                .expect("search");
            assert_eq!(outcome.total_hits, live, "{query} limit {limit}");
            assert_eq!(outcome.hits.len(), limit.min(live), "{query} limit {limit}");
        }
    }
}

/// The pruned page is still the page: every hit matches, none is deleted.
#[test]
fn a_pruned_page_holds_only_live_matches() {
    let (_temp, store) = store();
    let outcome = store
        .search_keys(INDEX, "tag:a", 120, None, &SearchOptions::default())
        .expect("search");
    assert_eq!(outcome.hits.len(), 100);
    for hit in &outcome.hits {
        let n: usize = hit.id[1..].parse().expect("numbered id");
        assert!(
            n.is_multiple_of(2) && n >= 100,
            "{} is not a live a",
            hit.id
        );
    }
}
