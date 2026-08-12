//! Single-term prefix queries — `field:pre*` — and the range they are rewritten into.
//!
//! An unusable bound produces an empty result rather than an error, so every case asserts which
//! documents came back and that nothing was discarded.

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

fn field(field_type: TantivyFieldType) -> FieldDef {
    let mut def = FieldDef::new("placeholder".to_string(), field_type);
    def.indexed = true;
    def.stored = false;
    def
}

/// Three documents whose terms sit on the boundaries the rewrite has to get right: `quick` next to
/// `quid`, and a run of terms ending in the last scalar of its class (`zzz`, `fo9x`, `ÿak`).
fn store_with_docs(temp: &TempDir, index: &str) -> HybridStore {
    let store = HybridStore::new(config(temp.path().to_path_buf()), 1).unwrap();

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
