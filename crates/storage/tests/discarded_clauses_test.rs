//! The reporting contract of `SearchOutcome::discarded`, which has two halves:
//!
//!  1. an ordinary, correct query reports nothing, and
//!  2. every clause the parser drops is reported.
//!
//! Both matter. A report that fires on valid queries gets ignored, and an unreported drop
//! widens a conjunction or disables a negation without changing anything visible in the hits.

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

fn field(field_type: TantivyFieldType, fast: bool, indexed: bool) -> FieldDef {
    let mut def = FieldDef::new("placeholder".to_string(), field_type);
    def.fast = Some(fast);
    def.indexed = indexed;
    def.stored = false;
    def
}

/// Two documents, of which exactly one has `tag:active`, so a widened query is visible as a
/// hit count of two.
///
/// Carries no JSON field on purpose: Tantivy resolves an unrecognised `name:` prefix as a path
/// inside a JSON default field, which parses cleanly and produces no error to report. Detecting
/// that needs schema-side validation, so it is out of scope for these tests.
fn store_with_two_docs(temp: &TempDir, index: &str) -> HybridStore {
    let store = HybridStore::new(config(temp.path().to_path_buf()), 1).unwrap();

    let mut fields = HashMap::new();
    fields.insert("title".into(), field(TantivyFieldType::Text, false, true));
    fields.insert("body".into(), field(TantivyFieldType::Text, false, true));
    // String indexes without positions, so an unfielded phrase fails against it while still
    // matching the text fields above.
    fields.insert("tag".into(), field(TantivyFieldType::String, false, true));
    fields.insert("year".into(), field(TantivyFieldType::U64, true, true));
    fields.insert("created".into(), field(TantivyFieldType::Date, true, true));
    fields.insert("flag".into(), field(TantivyFieldType::Boolean, false, true));
    fields.insert("hidden".into(), field(TantivyFieldType::Text, false, false));

    let mut schema = IndexSchema {
        fields,
        ..Default::default()
    };
    schema.rebuild_shadow_fields_cache();
    store.store_schema_and_cache(index, &schema).unwrap();

    for (id, tag) in [("d1", "active"), ("d2", "archived")] {
        store
            .apply_write(
                index,
                WalOp::Put {
                    id: id.to_string(),
                    json_blob: Some(serde_json::json!({
                        "title": "rust programming quickstart",
                        "body": "a small blue bike appeared",
                        "tag": tag,
                        "year": 2024u64,
                        "created": "2024-06-15T00:00:00Z",
                        "flag": true,
                        "hidden": "invisible"
                    })),
                },
            )
            .unwrap();
    }
    store.commit_index(index).unwrap();
    store
}

#[test]
fn an_ordinary_query_reports_no_discarded_clauses() {
    let temp = TempDir::new().unwrap();
    let store = store_with_two_docs(&temp, "clean");

    // Every form here is correct and supported.
    for query in [
        "rust",
        "\"rust programming\"",
        "\"small bike\"~2",
        "\"a small bl\"*",
        "title:\"rust programming\"",
        "title:rust",
        "tag:active",
        "year:[2020 TO 2025]",
        "year:>=2024",
        "created:>2024-01-01",
        "created:[2024-01-01 TO 2024-12-31]",
        "flag:true",
        "tag: IN [active archived]",
        "title:rust AND tag:active",
        "title:rust NOT tag:archived",
        "+title:rust -tag:archived",
        "(title:rust OR title:go) AND year:[2020 TO 2025]",
        "title:rust^3 OR body:bike",
        "*",
        "id:d1",
    ] {
        let outcome = store.search_documents("clean", query, 10, None).unwrap();
        assert!(
            outcome.discarded.is_empty(),
            "correct query {query:?} reported discards: {:?}",
            outcome.discarded
        );
    }
}

#[test]
fn an_unfielded_phrase_is_not_reported_despite_a_field_that_cannot_serve_it() {
    // `tag` is a default query field with no positions, so the phrase fails against it and
    // succeeds against the text fields. The caller lost nothing it asked for.
    let temp = TempDir::new().unwrap();
    let store = store_with_two_docs(&temp, "partial");

    let outcome = store
        .search_documents("partial", "\"rust programming\"", 10, None)
        .unwrap();
    assert_eq!(outcome.total_hits, 2);
    assert!(
        outcome.discarded.is_empty(),
        "a per-field phrase failure is not a dropped clause: {:?}",
        outcome.discarded
    );
}

#[test]
fn a_clause_dropped_from_a_conjunction_is_reported_rather_than_silently_widening() {
    let temp = TempDir::new().unwrap();
    let store = store_with_two_docs(&temp, "widen");

    // Each query asks for `tag:active` and something the parser cannot interpret, so a count of
    // two means the second clause vanished.
    let baseline = store
        .search_documents("widen", "tag:active", 10, None)
        .unwrap();
    assert_eq!(baseline.total_hits, 1, "fixture: one doc has tag:active");

    for (label, query) in [
        ("unknown field", "tag:active AND nosuch:x"),
        ("non-indexed field", "tag:active AND hidden:invisible"),
        ("field-presence test", "tag:active AND title:*"),
        (
            "numeric field, text value",
            "tag:active AND year:notanumber",
        ),
        ("boolean field, text value", "tag:active AND flag:maybe"),
        ("date field, text value", "tag:active AND created:notadate"),
        ("must-clause form", "+tag:active +nosuch:x"),
    ] {
        let outcome = store.search_documents("widen", query, 10, None).unwrap();
        assert!(
            !outcome.discarded.is_empty(),
            "{label}: {query:?} dropped a clause without reporting it \
             (returned {} hits against a baseline of 1)",
            outcome.total_hits
        );
    }
}

#[test]
fn a_dropped_negation_is_reported_because_it_returns_the_excluded_rows() {
    // A dropped exclusion returns the rows the caller excluded, at a count that looks ordinary.
    let temp = TempDir::new().unwrap();
    let store = store_with_two_docs(&temp, "negate");

    let outcome = store
        .search_documents("negate", "title:rust NOT nosuch:x", 10, None)
        .unwrap();
    assert_eq!(
        outcome.total_hits, 2,
        "fixture: the dropped exclusion lets both documents through"
    );
    assert!(
        !outcome.discarded.is_empty(),
        "a discarded negation must be reported: it returns exactly the rows the caller excluded"
    );
}

#[test]
fn a_discarded_clause_is_described_in_terms_the_caller_can_act_on() {
    let temp = TempDir::new().unwrap();
    let store = store_with_two_docs(&temp, "describe");

    let unknown = store
        .search_documents("describe", "tag:active AND nosuch:x", 10, None)
        .unwrap();
    let note = unknown.discarded.join(" | ");
    assert!(
        note.contains("nosuch"),
        "the description must name the offending field, got: {note}"
    );

    // Tantivy reports every exists leaf as "Range query need to target a specific field",
    // which would send a caller hunting a syntax error it does not have.
    let exists = store
        .search_documents("describe", "tag:active AND title:*", 10, None)
        .unwrap();
    let note = exists.discarded.join(" | ");
    assert!(
        note.contains("field-presence"),
        "an exists clause must be described as unsupported field-presence, got: {note}"
    );
    assert!(
        !note.contains("Range query need to target"),
        "tantivy's misleading wording must not reach the caller, got: {note}"
    );
}

/// A drop is not one-directional. Dropping a branch of a disjunction removes the matches that
/// branch would have contributed, so the caller gets fewer rows than they asked for — the
/// opposite of the widening a dropped conjunct causes.
#[test]
fn a_dropped_branch_of_a_disjunction_narrows_rather_than_widens() {
    let temp = TempDir::new().unwrap();
    let store = store_with_two_docs(&temp, "narrow");

    // Both documents carry a title, so a working presence test would match the pair.
    let intact = store
        .search_documents("narrow", "title:rust OR tag:archived", 10, None)
        .unwrap();
    assert_eq!(intact.total_hits, 2, "fixture: the disjunction covers both");
    assert!(intact.discarded.is_empty());

    let dropped = store
        .search_documents("narrow", "title:* OR tag:archived", 10, None)
        .unwrap();
    assert_eq!(
        dropped.total_hits, 1,
        "the dropped branch took its matches with it"
    );
    assert!(
        !dropped.discarded.is_empty(),
        "a narrowing drop must be reported like any other"
    );
    assert!(
        !dropped.emptied,
        "a branch survived, so the query ran as something"
    );
}

/// The clause that was dropped was the only one, so tantivy trims the AST to `EmptyQuery` and
/// the search matches nothing. The zero it reports answers no question: it is not "no document
/// has this field", it is "the query never ran".
#[test]
fn a_query_whose_only_clause_is_dropped_matches_nothing() {
    let temp = TempDir::new().unwrap();
    let store = store_with_two_docs(&temp, "emptied");

    let outcome = store
        .search_documents("emptied", "title:*", 10, None)
        .unwrap();
    assert_eq!(
        outcome.total_hits, 0,
        "an emptied query cannot match; a non-zero count would mean it ran as something else"
    );
    assert!(
        !outcome.discarded.is_empty(),
        "the caller's only signal that this zero is not an answer"
    );
    assert!(
        outcome.emptied,
        "nothing survived the parse, and the flag is what lets a caller refuse this zero"
    );
}

#[test]
fn count_only_queries_report_discards_too() {
    // `limit = 0` takes a separate branch with its own parse.
    let temp = TempDir::new().unwrap();
    let store = store_with_two_docs(&temp, "counting");

    let clean = store
        .search_documents("counting", "tag:active", 0, None)
        .unwrap();
    assert_eq!(clean.total_hits, 1);
    assert!(clean.discarded.is_empty());

    let widened = store
        .search_documents("counting", "tag:active AND nosuch:x", 0, None)
        .unwrap();
    assert!(
        !widened.discarded.is_empty(),
        "count-only mode must report discards; it returned {} against a baseline of 1",
        widened.total_hits
    );
}

#[test]
fn a_recovered_ambiguity_is_not_reported_but_a_real_syntax_error_still_is() {
    // The parser reports a resolved ambiguity alongside genuine drops. `field:value` whose
    // value contains a colon is read as a field name, then re-read as a term, and the clause
    // runs — so it is not a drop. A malformed clause still is.
    let temp = TempDir::new().unwrap();
    let store = store_with_two_docs(&temp, "recovery");

    // RFC3339 carries colons and triggers the ambiguity; the match proves the clause ran.
    let recovered = store
        .search_documents("recovery", "created:\"2024-06-15T00:00:00Z\"", 10, None)
        .unwrap();
    assert_eq!(recovered.total_hits, 2, "the clause must still have run");
    assert!(
        recovered.discarded.is_empty(),
        "a recovered ambiguity is not a dropped clause: {:?}",
        recovered.discarded
    );

    // A path-shaped value against a text field cannot be recovered into a term.
    let broken = store
        .search_documents("recovery", "title:/some/path", 10, None)
        .unwrap();
    assert!(
        !broken.discarded.is_empty(),
        "a genuine syntax error must still be reported, or the filter is too broad"
    );
}

#[test]
fn the_kv_bypass_reports_nothing_because_it_never_reaches_the_parser() {
    // `id:value` short-circuits to a redb lookup, so no parse happens.
    let temp = TempDir::new().unwrap();
    let store = store_with_two_docs(&temp, "bypass");

    for limit in [0, 10] {
        let outcome = store
            .search_documents("bypass", "id:d1", limit, None)
            .unwrap();
        assert!(
            outcome.discarded.is_empty(),
            "the KV bypass must not report discards (limit={limit}): {:?}",
            outcome.discarded
        );
    }
}
