//! The per-query deadline, enforced inside the shard's own search.
//!
//! `limit` bounds the response, not the work: a `limit = 10` over five thousand matches
//! walks every match to rank them, and nothing outside the walk can stop it — a
//! `spawn_blocking` closure is uncancellable. `SearchDeadline` is what stops it, sampled
//! from inside the segment walk it wraps. These tests cover the three ways a search can
//! run — scored, sorted, counted — plus the two properties that make the deadline safe to
//! carry: it fires, and it is invisible when it does not.
//!
//! The pruning path (`DeadlineQuery` + a top-docs collector directly) is covered by unit
//! tests inside `deadline.rs`, where tantivy is a dependency rather than a dev one.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use storage::{
    FieldDef, HybridStore, IndexSchema, SearchDeadline, SearchOptions, SortOrder, SortSpec,
    StorageConfig, StoreError, TantivyFieldType, WalOp,
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

fn field(field_type: TantivyFieldType, fast: bool, indexed: bool) -> FieldDef {
    let mut def = FieldDef::new("placeholder".to_string(), field_type);
    def.fast = Some(fast);
    def.indexed = indexed;
    def.stored = false;
    def
}

/// Five thousand small documents sharing a token, with a u64 fast field to sort on. All
/// of them match `title:doc`, so a deadline that cannot stop the walk is not a deadline.
fn store_with_many_docs(temp: &TempDir, index: &str) -> HybridStore {
    let store = HybridStore::new(config(temp.path().to_path_buf()), 1).unwrap();

    let mut fields = HashMap::new();
    fields.insert("title".into(), field(TantivyFieldType::Text, false, true));
    fields.insert("n".into(), field(TantivyFieldType::U64, true, true));

    let schema = IndexSchema {
        fields,
        ..Default::default()
    };
    store.store_schema_and_cache(index, &schema).unwrap();

    // One batch, not five thousand transactions: the fixture exists to be walked, so the
    // writes go through the same coalesced path a bulk write takes.
    let ops: Vec<WalOp> = (0..5000u64)
        .map(|i| WalOp::Put {
            id: format!("d{i}"),
            json_blob: Some(serde_json::json!({
                "title": format!("doc number {i}"),
                "n": i,
            })),
        })
        .collect();
    store.apply_batch(index, ops).unwrap();
    store.commit_index(index).unwrap();
    store
}

/// A deadline that was already spent before the search started: `expired` on first check.
fn expired() -> SearchOptions<'static> {
    SearchOptions {
        deadline: Some(SearchDeadline::new(
            Instant::now() - Duration::from_secs(5),
            Duration::from_millis(1),
        )),
        fields: None,
        ..Default::default()
    }
}

fn n_asc() -> SortSpec {
    SortSpec {
        field: "n".to_string(),
        order: SortOrder::Asc,
    }
}

#[test]
fn an_expired_deadline_fails_a_scored_search() {
    let temp = TempDir::new().unwrap();
    let store = store_with_many_docs(&temp, "docs");

    let outcome = store.search_documents_with("docs", "title:doc", 10, None, &expired());
    assert!(
        matches!(outcome, Err(StoreError::SearchDeadlineExceeded { .. })),
        "a scored walk must refuse on the deadline, got: {outcome:?}"
    );
}

#[test]
fn an_expired_deadline_fails_a_sorted_search() {
    let temp = TempDir::new().unwrap();
    let store = store_with_many_docs(&temp, "docs");

    let outcome = store.search_documents_with("docs", "title:doc", 10, Some(&n_asc()), &expired());
    assert!(
        matches!(outcome, Err(StoreError::SearchDeadlineExceeded { .. })),
        "a fast-field-sorted walk must refuse on the deadline, got: {outcome:?}"
    );
}

#[test]
fn an_expired_deadline_fails_a_count_only_search() {
    let temp = TempDir::new().unwrap();
    let store = store_with_many_docs(&temp, "docs");

    let outcome = store.search_documents_with("docs", "title:doc", 0, None, &expired());
    assert!(
        matches!(outcome, Err(StoreError::SearchDeadlineExceeded { .. })),
        "a count walks matches too, so the deadline covers it, got: {outcome:?}"
    );
}

/// A deadline with room left changes nothing the search answers — hits, total, discards —
/// which is what makes it safe to carry on every request: the budget is a limit the walk
/// checks, not a part of the result.
#[test]
fn a_deadline_with_budget_answers_what_the_plain_search_does() {
    let temp = TempDir::new().unwrap();
    let store = store_with_many_docs(&temp, "docs");
    let opts = SearchOptions {
        deadline: Some(SearchDeadline::new(Instant::now(), Duration::from_secs(60))),
        fields: None,
        ..Default::default()
    };

    for (label, limit, sort) in [
        ("scored", 10, None),
        ("sorted", 10, Some(n_asc())),
        ("count", 0, None),
    ] {
        let plain = store
            .search_documents("docs", "title:doc", limit, sort.as_ref())
            .unwrap();
        let budgeted = store
            .search_documents_with("docs", "title:doc", limit, sort.as_ref(), &opts)
            .unwrap();
        assert_eq!(budgeted.total_hits, plain.total_hits, "{label}: total_hits");
        assert_eq!(budgeted.hits, plain.hits, "{label}: hits");
        assert_eq!(budgeted.discarded, plain.discarded, "{label}: discarded");
    }
}
