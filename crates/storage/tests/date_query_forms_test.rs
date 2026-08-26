//! Every date literal form the syntax reference documents, checked against the engine.
//!
//! The reference is rendered into the `search_index` tool description, the answer
//! `validate_query` returns, the per-field `query_hint` on a schema listing, and the crate
//! README — so a claim made there is a claim made in four places at once, and an agent builds
//! queries from it rather than from experiment. These tests are what make the claims checkable.
//!
//! Written after an audit on 2026-08-27 found the reference naming two of the forms
//! `parse_date_str_to_tantivy` accepts, and after live probing corrected two claims the audit
//! itself got wrong: an unquoted literal containing a space does not survive the grammar, and a
//! bare date is an exact instant rather than a day.

use serde_json::json;
use storage::{
    FieldDef, HybridStore, IndexSchema, SearchOutcome, StorageConfig, TantivyFieldType, WalOp,
};
use tempfile::TempDir;

const INDEX: &str = "events";

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

/// One text field to match everything with, and a fast date field to range over.
fn schema() -> IndexSchema {
    let mut schema = IndexSchema::default();
    schema.fields.insert(
        "id".to_string(),
        FieldDef::new("id".to_string(), TantivyFieldType::Text),
    );
    schema.fields.insert(
        "title".to_string(),
        FieldDef::new("title".to_string(), TantivyFieldType::Text),
    );
    schema.fields.insert(
        "created".to_string(),
        FieldDef::new("created".to_string(), TantivyFieldType::Date),
    );
    schema.normalize_after_deserialization();
    schema
}

/// Five documents at noon UTC, so that a query written as a bare date — midnight — cannot match
/// one by accident and call the exact-instant rule into question.
fn store_with_events(dir: &TempDir) -> HybridStore {
    let store =
        HybridStore::new(test_config(dir.path().to_path_buf()), 1).expect("build a HybridStore");
    store
        .store_schema_and_cache(INDEX, &schema())
        .expect("store the schema");

    let docs: Vec<WalOp> = [
        "2023-06-15",
        "2024-01-01",
        "2024-06-15",
        "2024-07-01",
        "2025-01-01",
    ]
    .iter()
    .map(|day| WalOp::Put {
        id: (*day).to_string(),
        json_blob: Some(json!({
            "id": day,
            "title": "event",
            "created": format!("{day}T12:00:00Z"),
        })),
    })
    .collect();

    store.apply_batch(INDEX, docs).expect("write the events");
    store.commit_index(INDEX).expect("commit");
    store
}

/// The ids a query returns, sorted, plus whatever the engine discarded from it.
fn run(store: &HybridStore, query: &str) -> (Vec<String>, Vec<String>) {
    let SearchOutcome {
        hits, discarded, ..
    } = store
        .search_documents(INDEX, query, 10, None)
        .unwrap_or_else(|e| panic!("searching {query:?} failed: {e}"));

    let mut ids: Vec<String> = hits
        .iter()
        .map(|(_, doc)| doc["id"].as_str().unwrap_or_default().to_string())
        .collect();
    ids.sort();
    (ids, discarded)
}

fn matches(store: &HybridStore, query: &str) -> Vec<String> {
    let (ids, discarded) = run(store, query);
    assert!(
        discarded.is_empty(),
        "{query:?} should run as written, but the engine discarded {discarded:?}"
    );
    ids
}

/// Ranges, comparisons and sets take every literal form, and one parser reads them all.
#[test]
fn every_documented_date_literal_works_in_a_range_a_comparison_and_a_set() {
    let dir = TempDir::new().unwrap();
    let store = store_with_events(&dir);

    // A month and a year are literals in their own right, which is what makes a whole year
    // expressible without spelling out its last instant.
    assert_eq!(
        matches(&store, "created:[2024-06 TO 2024-08]"),
        vec!["2024-06-15", "2024-07-01"],
        "a month bound"
    );
    assert_eq!(
        matches(&store, "created:[2024 TO 2025}"),
        vec!["2024-01-01", "2024-06-15", "2024-07-01"],
        "a year bound, exclusive at the top, is how a whole year is asked for"
    );
    assert_eq!(
        matches(&store, "created:>2024-06"),
        vec!["2024-06-15", "2024-07-01", "2025-01-01"],
        "a month after a comparison"
    );

    // The separators the reference says are interchangeable.
    for query in [
        "created:[2024/06/01 TO 2024/08/01]",
        "created:[2024.06.01 TO 2024.08.01]",
        "created:[20240601 TO 20240801]",
    ] {
        assert_eq!(
            matches(&store, query),
            vec!["2024-06-15", "2024-07-01"],
            "{query} should read the same as the dashed form"
        );
    }

    // Epoch seconds, alone and as a bound. 1718452800 is 2024-06-15T12:00:00Z exactly.
    assert_eq!(
        matches(&store, "created:1718452800"),
        vec!["2024-06-15"],
        "epoch seconds name an instant"
    );
    assert_eq!(
        matches(&store, "created:[1718452800 TO 1718539200]"),
        vec!["2024-06-15"],
        "epoch seconds as bounds"
    );

    assert_eq!(
        matches(
            &store,
            "created: IN [2024-06-15T12:00:00Z 2025-01-01T12:00:00Z]"
        ),
        vec!["2024-06-15", "2025-01-01"],
        "a set of instants"
    );
}

/// A bare date is an instant, not a day — which is why the reference says to use a range.
///
/// The trap this pins is silent: `created:2024-06-15` parses, discards nothing, and returns
/// nothing, because midnight is not when the document was written. An agent reading zero hits as
/// "no events that day" would be wrong, and nothing in the response says so.
#[test]
fn a_bare_date_matches_the_instant_it_names_and_not_the_day() {
    let dir = TempDir::new().unwrap();
    let store = store_with_events(&dir);

    assert!(
        matches(&store, "created:2024-06-15").is_empty(),
        "a bare date is midnight exactly, and no document sits there"
    );
    assert_eq!(
        matches(&store, "created:2024-06-15T12:00:00Z"),
        vec!["2024-06-15"],
        "the instant the document carries does match"
    );
    assert_eq!(
        matches(&store, "created:[2024-06-15 TO 2024-06-16}"),
        vec!["2024-06-15"],
        "and the day is a range, which is what the reference tells a caller to write"
    );
}

/// A literal containing a space needs quoting, or the grammar reads the rest as a new clause.
#[test]
fn a_space_in_a_date_literal_must_be_quoted() {
    let dir = TempDir::new().unwrap();
    let store = store_with_events(&dir);

    let (ids, discarded) = run(&store, "created:2024-06-15 12:00:00");
    assert!(
        ids.is_empty(),
        "an unquoted space breaks the clause, so nothing should match"
    );
    assert!(
        discarded.iter().any(|note| note.contains("'12'")),
        "the time should be reported as an unknown field rather than silently ignored: \
         {discarded:?}"
    );

    assert_eq!(
        matches(&store, "created:\"2024-06-15 12:00:00\""),
        vec!["2024-06-15"],
        "quoted, the same literal is one value"
    );
    assert_eq!(
        matches(&store, "created:2024-06-15T12:00:00"),
        vec!["2024-06-15"],
        "and the `T` form needs no quoting at all"
    );
}

/// A date past what Tantivy represents is clamped, not refused.
///
/// So a sentinel far-future value compares as though it were 2262-04-11: `>` past the bound
/// matches nothing rather than erroring, and `<` past it matches everything.
#[test]
fn a_date_outside_the_representable_range_is_clamped() {
    let dir = TempDir::new().unwrap();
    let store = store_with_events(&dir);

    assert!(
        matches(&store, "created:>9999-01-01").is_empty(),
        "nothing is after the clamped maximum"
    );
    assert_eq!(
        matches(&store, "created:<9999-01-01").len(),
        5,
        "everything is before it"
    );
    assert!(
        matches(&store, "created:<0001-01-01").is_empty(),
        "nothing is before the clamped minimum"
    );
}
