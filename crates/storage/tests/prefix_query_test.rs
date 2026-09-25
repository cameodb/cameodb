//! Single-term prefix queries — `field:pre*` — and the range they are rewritten into.
//!
//! An unusable bound produces an empty result rather than an error, so every case asserts which
//! documents came back and that nothing was discarded.

use std::collections::HashMap;

use storage::{FieldDef, HybridStore, IndexSchema, StorageConfig, TantivyFieldType, WalOp};
use tempfile::TempDir;

fn config(path: std::path::PathBuf, query: storage::QueryPolicy) -> StorageConfig {
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
        commit_interval_ms: 0,
        query,
    }
}

fn field(field_type: TantivyFieldType) -> FieldDef {
    let mut def = FieldDef::new("placeholder".to_string(), field_type);
    def.indexed = true;
    def.stored = false;
    def
}

/// Three documents whose terms sit on the boundaries the rewrite has to get right: `quick` next to
/// `quid`, and a run of terms ending in the last scalar of its class (`zzz`, `fo9x`, `ÿak`).
fn store_with_docs(temp: &TempDir, index: &str) -> HybridStore {
    store_with_floor(temp, index, 0)
}

/// The same documents, on a store that expands only prefixes of `min_prefix_length` or more.
fn store_with_floor(temp: &TempDir, index: &str, min_prefix_length: usize) -> HybridStore {
    store_with_policy(
        temp,
        index,
        storage::QueryPolicy {
            min_prefix_length,
            ..Default::default()
        },
    )
}

/// The same documents, on a store running `policy`.
fn store_with_policy(temp: &TempDir, index: &str, policy: storage::QueryPolicy) -> HybridStore {
    let store = HybridStore::new(config(temp.path().to_path_buf(), policy), 1).unwrap();

    let mut fields = HashMap::new();
    fields.insert("title".into(), field(TantivyFieldType::Text));
    fields.insert("tag".into(), field(TantivyFieldType::String));

    let mut schema = IndexSchema {
        fields,
        ..Default::default()
    };
    schema.rebuild_shadow_fields_cache();
    store.store_schema_and_cache(index, &schema).unwrap();

    for (id, title, tag) in [
        ("d1", "Quick Brown Fox", "urn:cve:2024"),
        ("d2", "quid pro quo", "urn:cwe:79"),
        ("d3", "zebra fo9x zzz ÿak", "zzz"),
    ] {
        store
            .apply_write(
                index,
                WalOp::Put {
                    id: id.to_string(),
                    json_blob: Some(serde_json::json!({ "title": title, "tag": tag })),
                },
            )
            .unwrap();
    }
    store.commit_index(index).unwrap();
    store
}

/// The ids a query matched, sorted, having asserted the query lost nothing on the way.
fn matched(store: &HybridStore, index: &str, query: &str) -> Vec<String> {
    let outcome = store.search_documents(index, query, 10, None).unwrap();
    assert!(
        outcome.discarded.is_empty(),
        "{query:?} discarded a clause: {:?}",
        outcome.discarded
    );

    let mut ids: Vec<String> = outcome
        .hits
        .iter()
        .map(|(_, doc)| doc["id"].as_str().unwrap().to_string())
        .collect();
    ids.sort();
    ids
}

#[test]
fn a_prefix_matches_every_term_that_starts_with_it() {
    let temp = TempDir::new().unwrap();
    let store = store_with_docs(&temp, "prefix");

    assert_eq!(matched(&store, "prefix", "title:qui*"), ["d1", "d2"]);
    assert_eq!(matched(&store, "prefix", "title:brow*"), ["d1"]);
    assert_eq!(matched(&store, "prefix", "title:xyz*"), [] as [&str; 0]);
}

#[test]
fn a_prefix_is_bounded_by_the_term_after_it() {
    // `quic` and `quid` differ in the scalar the upper bound increments, so a bound one late
    // would pull `quid` in as well.
    let temp = TempDir::new().unwrap();
    let store = store_with_docs(&temp, "bounds");

    assert_eq!(matched(&store, "bounds", "title:quic*"), ["d1"]);
    assert_eq!(matched(&store, "bounds", "title:quid*"), ["d2"]);
}

#[test]
fn a_text_prefix_is_matched_in_the_case_the_field_indexed() {
    // Tantivy tokenizes the bounds, so the prefix arrives lowercased.
    let temp = TempDir::new().unwrap();
    let store = store_with_docs(&temp, "case");

    for query in ["title:quic*", "title:Quic*", "title:QUIC*"] {
        assert_eq!(matched(&store, "case", query), ["d1"], "{query:?}");
    }
}

#[test]
fn a_prefix_ending_at_the_top_of_its_class_still_gets_a_bound() {
    // The scalar after `z`, `9` and `ÿ` is one the tokenizer discards, so it cannot be the bound;
    // the scan carries on to the next scalar the tokenizer keeps.
    let temp = TempDir::new().unwrap();
    let store = store_with_docs(&temp, "classes");

    for query in [
        "title:z*",
        "title:zz*",
        "title:zzz*",
        "title:fo9*",
        "title:ÿ*",
        "title:ÿa*",
    ] {
        assert_eq!(matched(&store, "classes", query), ["d3"], "{query:?}");
    }
}

#[test]
fn a_string_field_prefix_keeps_case_and_punctuation() {
    // A string field is indexed raw, so its bounds pass through untouched: a colon stays part of
    // the term rather than reading as a field separator, and case is significant.
    let temp = TempDir::new().unwrap();
    let store = store_with_docs(&temp, "raw");

    assert_eq!(matched(&store, "raw", "tag:urn:c*"), ["d1", "d2"]);
    assert_eq!(matched(&store, "raw", "tag:urn:cve*"), ["d1"]);
    assert_eq!(matched(&store, "raw", "tag:z*"), ["d3"]);
    assert_eq!(matched(&store, "raw", "tag:URN*"), [] as [&str; 0]);
}

#[test]
fn a_prefix_is_rewritten_wherever_a_clause_can_appear() {
    let temp = TempDir::new().unwrap();
    let store = store_with_docs(&temp, "clauses");

    assert_eq!(matched(&store, "clauses", "(title:quic*)"), ["d1"]);
    assert_eq!(
        matched(&store, "clauses", "title:quic* AND tag:urn:cve*"),
        ["d1"]
    );
    assert_eq!(
        matched(&store, "clauses", "title:quic* OR title:zeb*"),
        ["d1", "d3"]
    );
    // The boost applies to the range just as it applied to the term.
    assert_eq!(matched(&store, "clauses", "title:quic*^2"), ["d1"]);
}

#[test]
fn the_forms_that_are_not_single_term_prefixes_are_left_alone() {
    let temp = TempDir::new().unwrap();
    let store = store_with_docs(&temp, "others");

    // A phrase prefix is handled by the grammar itself.
    assert_eq!(matched(&store, "others", "title:\"quick brown\"*"), ["d1"]);
    // A presence test is still unsupported, and still reported rather than rewritten.
    let outcome = store
        .search_documents("others", "title:*", 10, None)
        .unwrap();
    assert_eq!(outcome.total_hits, 0);
    assert_eq!(outcome.discarded.len(), 1);
}

#[test]
fn a_prefix_that_cannot_be_rewritten_is_reported() {
    // `char::MAX` has no successor, so the clause reaches the parser as the bare term. That has to
    // surface as a reported loss rather than an empty result set.
    let temp = TempDir::new().unwrap();
    let store = store_with_docs(&temp, "unrewritable");

    let outcome = store
        .search_documents("unrewritable", "tag:a\u{10FFFF}*", 10, None)
        .unwrap();
    assert_eq!(outcome.total_hits, 0);
    assert_eq!(
        outcome.discarded.len(),
        1,
        "expected one note, got {:?}",
        outcome.discarded
    );
    assert!(
        outcome.discarded[0].contains("prefix range"),
        "unexpected note: {:?}",
        outcome.discarded[0]
    );
}

#[test]
fn the_count_only_path_rewrites_the_same_way() {
    // A count that skipped the rewrite would disagree with the hits it is meant to describe.
    let temp = TempDir::new().unwrap();
    let store = store_with_docs(&temp, "counts");

    for query in ["title:qui*", "tag:urn:c*", "title:z*"] {
        let counted = store.search_documents("counts", query, 0, None).unwrap();
        let fetched = store.search_documents("counts", query, 10, None).unwrap();
        assert_eq!(
            counted.total_hits, fetched.total_hits,
            "{query:?} counted {} but fetched {}",
            counted.total_hits, fetched.total_hits
        );
    }
}

/// What a query reported losing or altering, without asserting it lost nothing.
fn notes(store: &HybridStore, index: &str, query: &str) -> Vec<String> {
    store
        .search_documents(index, query, 10, None)
        .unwrap()
        .discarded
}

/// Below the floor a prefix is not expanded: it matches the term as written, and says so once.
///
/// Not refused. One short clause in a larger query should not cost the caller the rest of it, and
/// matching the literal is exactly what an unrewritable prefix already does.
#[test]
fn a_prefix_shorter_than_the_floor_is_matched_as_written_and_reported() {
    let temp = TempDir::new().unwrap();
    let store = store_with_floor(&temp, "floor", 2);

    let outcome = store
        .search_documents("floor", "title:q*", 10, None)
        .unwrap();
    assert_eq!(outcome.total_hits, 0, "no document holds the term 'q'");
    assert_eq!(
        outcome.discarded.len(),
        1,
        "one note: {:?}",
        outcome.discarded
    );
    let note = &outcome.discarded[0];
    assert!(
        note.contains("title:q*") && note.contains("at least 2 characters"),
        "the note must name the clause and the floor: {note}"
    );

    // At the floor the prefix expands as before, and nothing is reported.
    assert_eq!(matched(&store, "floor", "title:qu*"), ["d1", "d2"]);

    // A boost does not leak into the note's account of the term.
    let boosted = notes(&store, "floor", "title:q*^2");
    assert_eq!(boosted.len(), 1, "{boosted:?}");
    assert!(boosted[0].contains("term 'q'"), "{}", boosted[0]);

    // The rest of a query still runs.
    assert_eq!(
        store
            .search_documents("floor", "title:q* OR title:zeb*", 10, None)
            .unwrap()
            .total_hits,
        1
    );
}

/// Zero expands everything — the library default, and the operator's way to turn the floor off.
#[test]
fn a_floor_of_zero_expands_a_single_character() {
    let temp = TempDir::new().unwrap();
    let store = store_with_floor(&temp, "nofloor", 0);
    assert_eq!(matched(&store, "nofloor", "title:q*"), ["d1", "d2"]);
}

/// Characters, not bytes: `ÿ` is two bytes and one character.
#[test]
fn the_floor_counts_characters() {
    let temp = TempDir::new().unwrap();
    let store = store_with_floor(&temp, "chars", 2);

    assert_eq!(notes(&store, "chars", "title:ÿ*").len(), 1);
    assert_eq!(matched(&store, "chars", "title:ÿa*"), ["d3"]);
}

/// The count and the validation paths apply the same floor as the search path, so a count never
/// describes a query the search did not run, and validation reports what the search would.
#[test]
fn the_floor_holds_on_the_count_and_validate_paths() {
    let temp = TempDir::new().unwrap();
    let store = store_with_floor(&temp, "paths", 2);

    let counted = store
        .search_documents("paths", "title:q*", 0, None)
        .unwrap();
    assert_eq!(counted.total_hits, 0);
    assert_eq!(counted.discarded.len(), 1, "{:?}", counted.discarded);

    let validated = store.validate_query("paths", "title:q*").unwrap().unwrap();
    assert_eq!(validated.discarded.len(), 1, "{:?}", validated.discarded);
    assert!(validated.discarded[0].contains("at least 2 characters"));
}

/// Every `*` the grammar drops without a word is reported, and only those.
///
/// Tantivy removes the `*` and matches what is left as an ordinary term, raising nothing — so
/// before this, each of these answered a wildcard search the caller never actually got.
#[test]
fn a_wildcard_the_grammar_would_ignore_is_reported() {
    let temp = TempDir::new().unwrap();
    let store = store_with_docs(&temp, "ignored");

    for (query, remedy) in [
        // A prefix naming no field is not rewritten unless the node expands them.
        ("qui*", "enable expand_unqualified_prefix"),
        // Leading and inner wildcards have no rewrite at all.
        ("title:*uick", "only a trailing '*'"),
        ("title:q*ck", "only a trailing '*'"),
        // Inside a field group the rewrite does not reach.
        ("title:(qui*)", "outside any group"),
    ] {
        let found = notes(&store, "ignored", query);
        assert_eq!(found.len(), 1, "{query:?}: {found:?}");
        assert!(
            found[0].contains("was ignored") && found[0].contains(remedy),
            "{query:?}: {}",
            found[0]
        );
    }

    // Forms that work are not reported: a rewritten prefix, tantivy's own phrase prefix, and a
    // raw field that keeps the `*` inside its term and so matches exactly what was written.
    assert!(notes(&store, "ignored", "title:qui*").is_empty());
    assert!(notes(&store, "ignored", "title:\"quick brown\"*").is_empty());
    assert!(notes(&store, "ignored", "tag:urn*cve").is_empty());
}

/// A clause the rewrite already reported is not reported a second time by the wildcard pass.
#[test]
fn a_short_prefix_is_reported_once() {
    let temp = TempDir::new().unwrap();
    let store = store_with_floor(&temp, "once", 3);
    let found = notes(&store, "once", "title:qu*");
    assert_eq!(found.len(), 1, "{found:?}");
}

fn expanding(min_prefix_length: usize) -> storage::QueryPolicy {
    storage::QueryPolicy {
        min_prefix_length,
        expand_unqualified_prefix: true,
        ..Default::default()
    }
}

/// With `expand_unqualified_prefix`, a bare `pre*` searches every text default field.
///
/// `qui` is only in `title`, `urn` only in the raw `tag` field: one bare prefix finding both is
/// what proves the expansion reached each field with that field's own analyzer.
#[test]
fn an_unqualified_prefix_searches_the_default_fields_when_enabled() {
    let temp = TempDir::new().unwrap();
    let store = store_with_policy(&temp, "bare", expanding(0));

    assert_eq!(matched(&store, "bare", "qui*"), ["d1", "d2"]);
    assert_eq!(matched(&store, "bare", "urn*"), ["d1", "d2"]);
    assert_eq!(matched(&store, "bare", "zeb*"), ["d3"]);
    // In every position a clause can take, with its sign and boost kept.
    assert_eq!(matched(&store, "bare", "(quic* OR zeb*)"), ["d1", "d3"]);
    assert_eq!(matched(&store, "bare", "qui*^2"), ["d1", "d2"]);
    assert_eq!(matched(&store, "bare", "+qui* -quid*"), ["d1"]);
    assert_eq!(matched(&store, "bare", "qui* AND tag:zzz"), [] as [&str; 0]);
    // Beside a qualified prefix, which is rewritten on its own terms.
    assert_eq!(matched(&store, "bare", "title:zeb* OR quic*"), ["d1", "d3"]);
}

/// The count and validation paths expand the same way the search does.
#[test]
fn the_unqualified_expansion_holds_on_the_count_and_validate_paths() {
    let temp = TempDir::new().unwrap();
    let store = store_with_policy(&temp, "barepaths", expanding(0));

    let counted = store
        .search_documents("barepaths", "qui*", 0, None)
        .unwrap();
    assert_eq!(counted.total_hits, 2);
    assert!(counted.discarded.is_empty(), "{:?}", counted.discarded);
    let validated = store.validate_query("barepaths", "qui*").unwrap().unwrap();
    assert!(validated.discarded.is_empty(), "{:?}", validated.discarded);
}

/// What the expansion must leave alone: a prefix inside a field group belongs to that field, a
/// quoted phrase prefix is tantivy's, and a range bound is not a prefix.
#[test]
fn the_unqualified_expansion_leaves_qualified_and_quoted_forms_alone() {
    let temp = TempDir::new().unwrap();
    let store = store_with_policy(&temp, "barealone", expanding(0));

    // Inside `tag:( … )` the prefix is tag's. `tag` is raw, so it keeps the `*` and looks for
    // the literal term `quid*`, which no document has; sent to the default fields instead it
    // would find `quid` in title. Nothing back is the proof it stayed tag's.
    let outcome = store
        .search_documents("barealone", "tag:(quid*)", 10, None)
        .unwrap();
    assert_eq!(
        outcome.total_hits, 0,
        "a group's prefix must not reach the default fields"
    );

    // On a text field the group's prefix is still reported rather than expanded.
    let found = notes(&store, "barealone", "title:(qui*)");
    assert_eq!(found.len(), 1, "{found:?}");
    assert!(found[0].contains("outside any group"), "{}", found[0]);

    assert_eq!(
        matched(&store, "barealone", "title:\"quick brown\"*"),
        ["d1"]
    );
    assert_eq!(
        matched(&store, "barealone", "title:[a TO *]"),
        ["d1", "d2", "d3"]
    );
}

/// The floor applies to each field the expansion reaches, and a prefix below it on every field
/// is reported once, as a short prefix.
#[test]
fn the_floor_applies_to_an_unqualified_prefix() {
    let temp = TempDir::new().unwrap();
    let store = store_with_policy(&temp, "barefloor", expanding(2));

    let outcome = store.search_documents("barefloor", "q*", 10, None).unwrap();
    assert_eq!(outcome.total_hits, 0);
    assert_eq!(outcome.discarded.len(), 1, "{:?}", outcome.discarded);
    assert!(
        outcome.discarded[0].contains("'q*'")
            && outcome.discarded[0].contains("at least 2 characters"),
        "{}",
        outcome.discarded[0]
    );
    assert_eq!(matched(&store, "barefloor", "qu*"), ["d1", "d2"]);
}
