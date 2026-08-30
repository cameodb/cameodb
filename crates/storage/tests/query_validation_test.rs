//! Parsing a query against an index without running it.
//!
//! The point of this entry point is the case a structural check cannot reach. Counting quotes and
//! parentheses catches `title:"unclosed` and `((a)`, and stops there — `title:`, `title:[2020 TO`
//! and a leading `AND` all balance perfectly and none of them parse. Those are asserted below
//! with their balance checked in the test itself, so the reason each case is here stays visible.

use std::collections::HashMap;

use storage::{FieldDef, HybridStore, IndexSchema, StorageConfig, TantivyFieldType, WalOp};
use tempfile::TempDir;

fn config(path: std::path::PathBuf) -> StorageConfig {
    StorageConfig {
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
    }
}

fn field(field_type: TantivyFieldType, indexed: bool) -> FieldDef {
    let mut def = FieldDef::new("placeholder".to_string(), field_type);
    def.indexed = indexed;
    def.stored = false;
    def
}

/// An index with an indexed text field, an indexed numeric field and a field that is present but
/// not indexed — the three cases a validator has to tell apart.
fn store_with_docs(temp: &TempDir, index: &str) -> HybridStore {
    let store = HybridStore::new(config(temp.path().to_path_buf()), 1).unwrap();

    let mut fields = HashMap::new();
    fields.insert("title".into(), field(TantivyFieldType::Text, true));
    fields.insert("year".into(), field(TantivyFieldType::I64, true));
    fields.insert("notes".into(), field(TantivyFieldType::Text, false));

    let mut schema = IndexSchema {
        fields,
        ..Default::default()
    };
    schema.rebuild_shadow_fields_cache();
    store.store_schema_and_cache(index, &schema).unwrap();

    store
        .apply_write(
            index,
            WalOp::Put {
                id: "d1".to_string(),
                json_blob: Some(serde_json::json!({
                    "title": "Quick Brown Fox",
                    "year": 2020,
                    "notes": "unindexed",
                })),
            },
        )
        .unwrap();
    store.commit_index(index).unwrap();
    store
}

/// Quotes and parentheses balance — what the MCP tool's structural check tests, and all it tests.
fn is_structurally_balanced(query: &str) -> bool {
    let quotes = query.chars().filter(|c| *c == '"').count();
    let open = query.chars().filter(|c| *c == '(').count();
    let close = query.chars().filter(|c| *c == ')').count();
    quotes % 2 == 0 && open == close
}

#[test]
fn a_well_formed_query_is_valid() {
    let temp = TempDir::new().unwrap();
    let store = store_with_docs(&temp, "docs");

    let outcome = store
        .validate_query("docs", "title:brown")
        .unwrap()
        .unwrap();

    assert!(outcome.is_valid(), "{outcome:?}");
    assert!(outcome.syntax_errors.is_empty());
    assert!(outcome.discarded.is_empty());
}

/// The whole reason this exists: queries that pass a structural check and still do not parse.
#[test]
fn a_query_that_balances_but_does_not_parse_is_caught() {
    let temp = TempDir::new().unwrap();
    let store = store_with_docs(&temp, "docs");

    for query in [
        "title:",             // a field with nothing to match
        "title:[2020 TO",     // brackets are not what a structural check counts
        "year:{2020 TO 2021", // nor braces
        "AND title:rust",     // an operator with nothing on its left
    ] {
        assert!(
            is_structurally_balanced(query),
            "{query:?} should reach the parser with quotes and parens balanced, \
             otherwise it does not demonstrate anything"
        );

        let outcome = store.validate_query("docs", query).unwrap().unwrap();

        assert!(
            !outcome.syntax_errors.is_empty(),
            "{query:?} does not parse, so validation should say so; got {outcome:?}"
        );
        assert!(!outcome.is_valid(), "{query:?} should not be valid");
    }
}

/// A malformed query is reported with the parser's own words, including where it gave up.
#[test]
fn a_syntax_error_says_where_it_is() {
    let temp = TempDir::new().unwrap();
    let store = store_with_docs(&temp, "docs");

    let outcome = store.validate_query("docs", "title:").unwrap().unwrap();

    assert!(
        outcome.syntax_errors.iter().any(|e| e.contains("position")),
        "a syntax error should carry its position, got {:?}",
        outcome.syntax_errors
    );
}

/// A query that parses but cannot match is not a syntax error, and the two are not mixed.
#[test]
fn a_clause_that_parses_but_cannot_match_is_discarded_not_a_syntax_error() {
    let temp = TempDir::new().unwrap();
    let store = store_with_docs(&temp, "docs");

    for (query, expected) in [
        ("nosuchfield:rust", "unknown field 'nosuchfield'"),
        ("notes:rust", "field 'notes' exists but is not indexed"),
        ("year:notanumber", "did not match its field's type"),
    ] {
        let outcome = store.validate_query("docs", query).unwrap().unwrap();

        assert!(
            outcome.syntax_errors.is_empty(),
            "{query:?} parses; it should not be reported as a syntax error, got {:?}",
            outcome.syntax_errors
        );
        assert!(
            outcome.discarded.iter().any(|d| d.contains(expected)),
            "{query:?} should be discarded with {expected:?}, got {:?}",
            outcome.discarded
        );
        assert!(!outcome.is_valid());
    }
}

/// What validation reports as discarded is what a search actually drops.
///
/// This is the property that makes the tool worth calling: an agent that validates a query and
/// then runs it should not be told two different things. Syntax errors are excluded here because
/// validation reports those separately and in better words — the search path folds them into the
/// same list.
#[test]
fn validation_agrees_with_what_a_search_discards() {
    let temp = TempDir::new().unwrap();
    let store = store_with_docs(&temp, "docs");

    for query in ["nosuchfield:rust", "notes:rust", "title:brown"] {
        let validated = store.validate_query("docs", query).unwrap().unwrap();
        let searched = store.search_documents("docs", query, 10, None).unwrap();

        assert_eq!(
            validated.discarded, searched.discarded,
            "validation and search disagree about {query:?}"
        );
    }
}

/// The query a search runs is not always the query that was typed, and validation reports the
/// rewritten form — which is usually where a surprising result comes from.
#[test]
fn validation_reports_the_query_the_engine_will_actually_run() {
    let temp = TempDir::new().unwrap();
    let store = store_with_docs(&temp, "docs");

    // A single-term prefix is rewritten into a lexicographic range before it runs.
    let outcome = store.validate_query("docs", "title:bro*").unwrap().unwrap();

    assert_ne!(
        outcome.normalized_query, "title:bro*",
        "a prefix query is rewritten, and validation should show the rewrite"
    );
    assert!(
        outcome.normalized_query.contains("TO"),
        "expected a range rewrite, got {:?}",
        outcome.normalized_query
    );
}

/// An index with a schema but no documents has nothing to resolve field names against. That is
/// not an error, and it is not a valid query either — the caller has to be able to tell.
#[test]
fn an_index_that_was_never_written_to_cannot_be_validated_against() {
    let temp = TempDir::new().unwrap();
    let store = HybridStore::new(config(temp.path().to_path_buf()), 1).unwrap();

    let mut fields = HashMap::new();
    fields.insert("title".into(), field(TantivyFieldType::Text, true));
    let mut schema = IndexSchema {
        fields,
        ..Default::default()
    };
    schema.rebuild_shadow_fields_cache();
    store.store_schema_and_cache("empty", &schema).unwrap();

    assert!(
        store
            .validate_query("empty", "title:rust")
            .unwrap()
            .is_none(),
        "an unmaterialised index should report that it cannot answer, not a verdict"
    );
}

/// An index whose key is declared under its source name: `sha1` is the shadow of `id`, so
/// `sha1:VALUE` on its own is the key-value fast path, and named inside a larger query the
/// reference is rewritten to `id` and runs against the search index.
fn store_with_shadow(temp: &TempDir, index: &str) -> HybridStore {
    let store = HybridStore::new(config(temp.path().to_path_buf()), 1).unwrap();

    let mut fields = HashMap::new();
    fields.insert("id".into(), field(TantivyFieldType::Text, true));
    fields.insert("title".into(), field(TantivyFieldType::Text, true));
    let mut shadow = field(TantivyFieldType::Text, false);
    shadow.is_shadow = true;
    fields.insert("sha1".into(), shadow);

    let mut schema = IndexSchema {
        fields,
        ..Default::default()
    };
    schema.rebuild_shadow_fields_cache();
    store.store_schema_and_cache(index, &schema).unwrap();

    store
        .apply_write(
            index,
            WalOp::Put {
                id: "d1".to_string(),
                json_blob: Some(serde_json::json!({"id": "d1", "title": "a file"})),
            },
        )
        .unwrap();
    store.commit_index(index).unwrap();
    store
}

/// A shadow field on its own is the key-value fast path, which never reaches the parser on the
/// search path — so validation must not report the identifier's own field as dropped. Before
/// this check existed the validator contradicted the search it exists to predict: it called the
/// clause dropped while the search answered it.
#[test]
fn a_standalone_shadow_lookup_validates_as_the_fast_path_it_is() {
    let temp = TempDir::new().unwrap();
    let store = store_with_shadow(&temp, "files");

    for query in ["sha1:d1", "id:d1"] {
        let outcome = store.validate_query("files", query).unwrap().unwrap();
        assert!(
            outcome.is_valid(),
            "{query:?} is the fastest query the engine has; got {outcome:?}"
        );
    }
}

/// The identifier under its source name combines with other fields: the shadow reference is
/// rewritten to `id` and runs against the search index, where `id` is indexed. The negative
/// case is what proves the clause ran rather than being dropped — a dropped clause would widen
/// the conjunction and return the document the identifier excludes.
#[test]
fn a_shadow_field_in_a_larger_query_is_rewritten_to_id() {
    let temp = TempDir::new().unwrap();
    let store = store_with_shadow(&temp, "files");

    let query = "sha1:d1 AND title:file";
    let validated = store.validate_query("files", query).unwrap().unwrap();
    let searched = store.search_documents("files", query, 10, None).unwrap();

    assert!(
        validated.discarded.is_empty() && searched.discarded.is_empty(),
        "nothing is dropped from {query:?}: validate {:?}, search {:?}",
        validated.discarded,
        searched.discarded
    );
    assert!(
        validated.normalized_query.contains("id:d1")
            && !validated.normalized_query.contains("sha1"),
        "validation should show the rewrite the engine runs: {:?}",
        validated.normalized_query
    );
    assert_eq!(searched.total_hits, 1, "{query:?} should match d1");

    // If the shadow clause were dropped instead of run, this would match d1 on title alone.
    let excluded = store
        .search_documents("files", "sha1:nope AND title:file", 10, None)
        .unwrap();
    assert_eq!(
        excluded.total_hits, 0,
        "the identifier must actually constrain the result"
    );
    assert!(excluded.discarded.is_empty(), "{:?}", excluded.discarded);
}

/// A `*` in the value keeps the query off the key-value fast path and runs it as a prefix over
/// the identifiers, which the search index can answer and the key-value store cannot.
#[test]
fn a_shadow_prefix_query_matches_the_identifiers_it_prefixes() {
    let temp = TempDir::new().unwrap();
    let store = store_with_shadow(&temp, "files");

    for query in ["sha1:d*", "id:d*", "sha1:d* AND title:file"] {
        let outcome = store.search_documents("files", query, 10, None).unwrap();
        assert_eq!(outcome.total_hits, 1, "{query:?} should prefix-match d1");
        assert!(
            outcome.discarded.is_empty(),
            "{query:?} discarded a clause: {:?}",
            outcome.discarded
        );
    }

    // And an exact lookup is still exact: the asterisk is not silently literal.
    let exact = store
        .search_documents("files", "sha1:d1", 10, None)
        .unwrap();
    assert_eq!(exact.total_hits, 1);
    assert!(exact.discarded.is_empty());
}
