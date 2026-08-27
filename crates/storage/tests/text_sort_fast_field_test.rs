//! Sorting on a text field, with and without a fast column.
//!
//! A text field declared `fast` gets a string fast column, and Tantivy orders on it by term
//! ordinal — lexicographic, over every document that matched. Without the column there is
//! nothing to order on in the collector, so candidates are taken by relevance and sorted
//! afterwards: the alphabetical order of the highest-scoring `limit * 2`, which is not the
//! alphabetical order of the result.
//!
//! The difference is invisible on data where score and alphabet happen to agree, so these
//! tests are built on data where they disagree, and each asserts that premise before asserting
//! the behaviour that depends on it.

use serde_json::json;
use storage::{
    FieldDef, HybridStore, IndexSchema, SearchOutcome, SortOrder, SortSpec, StorageConfig,
    TantivyFieldType, WalOp,
};
use tempfile::TempDir;

fn test_config(path: std::path::PathBuf) -> StorageConfig {
    StorageConfig {
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
    }
}

/// `id`, a `title` to sort on, and a `body` to query — separated so the sort key and the score
/// can be varied independently.
fn build_schema(title_is_fast: bool) -> IndexSchema {
    let mut schema = IndexSchema::default();
    for name in ["id", "title", "body"] {
        schema.fields.insert(
            name.to_string(),
            FieldDef::new(name.to_string(), TantivyFieldType::Text),
        );
    }
    if let Some(title) = schema.fields.get_mut("title") {
        title.fast = Some(title_is_fast);
    }
    schema.normalize_after_deserialization();
    schema
}

/// Documents whose alphabetical order is the reverse of their relevance order.
///
/// `aaa` is alphabetically first and scores last: one occurrence of the query term in a long
/// field, against three occurrences in a short one. So a candidate window taken by score
/// excludes exactly the document a correct alphabetical sort must return first.
fn docs() -> Vec<WalOp> {
    let filler = "lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod tempor \
                  incididunt ut labore et dolore magna aliqua enim ad minim veniam quis nostrud \
                  exercitation ullamco laboris nisi aliquip ex ea commodo consequat duis aute";

    let mut ops = vec![WalOp::Put {
        id: "aaa".to_string(),
        json_blob: Some(json!({
            "id": "aaa",
            "title": "aaa",
            "body": format!("rust {filler}"),
        })),
    }];

    for tag in ["b", "c", "d", "e", "f"] {
        ops.push(WalOp::Put {
            id: tag.to_string(),
            json_blob: Some(json!({
                "id": tag,
                "title": format!("zzz-{tag}"),
                "body": "rust rust rust",
            })),
        });
    }
    ops
}

fn setup(dir: &TempDir, index: &str, title_is_fast: bool) -> HybridStore {
    let store =
        HybridStore::new(test_config(dir.path().to_path_buf()), 1).expect("create HybridStore");
    store
        .store_schema_and_cache(index, &build_schema(title_is_fast))
        .expect("store schema");
    store.apply_batch(index, docs()).expect("write docs");
    store.commit_index(index).expect("commit");
    store
}

fn ids(results: &[(f32, serde_json::Value)]) -> Vec<String> {
    results
        .iter()
        .map(|(_, doc)| doc["id"].as_str().unwrap_or_default().to_string())
        .collect()
}

fn title_sort() -> SortSpec {
    SortSpec {
        field: "title".to_string(),
        order: SortOrder::Asc,
    }
}

/// The premise the other tests rest on: `aaa` is outside the scored candidate window.
///
/// `limit = 2` gives the fallback a budget of four. If `aaa` were among the four highest
/// scoring documents, the fallback would return it first by accident and prove nothing.
#[test]
fn the_alphabetically_first_document_scores_outside_the_candidate_window() {
    let dir = TempDir::new().unwrap();
    let store = setup(&dir, "docs", false);

    let SearchOutcome { hits, .. } = store
        .search_documents("docs", "rust", 4, None)
        .expect("search");

    let returned = ids(&hits);
    assert_eq!(returned.len(), 4, "four candidates, as the fallback takes");
    assert!(
        !returned.contains(&"aaa".to_string()),
        "the test data is supposed to score `aaa` outside the top four, got {returned:?}"
    );
}

#[test]
fn a_fast_text_field_sorts_alphabetically_over_every_match() {
    let dir = TempDir::new().unwrap();
    let store = setup(&dir, "docs", true);

    let SearchOutcome {
        hits, total_hits, ..
    } = store
        .search_documents("docs", "rust", 2, Some(&title_sort()))
        .expect("search");

    assert_eq!(total_hits, 6, "every document matches the query");
    // Titles ascend `aaa`, `zzz-b`, `zzz-c`, … so the first two hits are the documents whose
    // ids are `aaa` and `b`.
    assert_eq!(
        ids(&hits),
        vec!["aaa".to_string(), "b".to_string()],
        "a fast text sort orders by the column, so the alphabetically first document leads \
         even though it scores last"
    );
}

/// The same query on the same data without the column, so the cost of not declaring one is
/// recorded rather than assumed.
#[test]
fn a_text_field_without_a_fast_column_sorts_only_its_scored_candidates() {
    let dir = TempDir::new().unwrap();
    let store = setup(&dir, "docs", false);

    let SearchOutcome { hits, .. } = store
        .search_documents("docs", "rust", 2, Some(&title_sort()))
        .expect("search");

    let returned = ids(&hits);
    assert!(
        !returned.contains(&"aaa".to_string()),
        "without a fast column the sort can only order what score selected, so the \
         alphabetically first document is absent — got {returned:?}"
    );
}

/// Descending order comes from the same column rather than from reversing a fetched page.
#[test]
fn a_fast_text_field_sorts_descending_too() {
    let dir = TempDir::new().unwrap();
    let store = setup(&dir, "docs", true);

    let sort = SortSpec {
        field: "title".to_string(),
        order: SortOrder::Desc,
    };
    let SearchOutcome { hits, .. } = store
        .search_documents("docs", "rust", 2, Some(&sort))
        .expect("search");

    assert_eq!(
        ids(&hits),
        vec!["f".to_string(), "e".to_string()],
        "descending on `title` starts at zzz-f"
    );
}
