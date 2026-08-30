//! The query shapes the pre-parse rewriters are responsible for: date literals in every
//! supported form, and facet paths.
//!
//! A shape no rewriter recognises reaches Tantivy in a form it rejects, which drops the clause
//! instead of raising an error — so each case asserts both the expected matches and that
//! nothing was discarded.

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

fn field(field_type: TantivyFieldType, fast: bool) -> FieldDef {
    let mut def = FieldDef::new("placeholder".to_string(), field_type);
    def.fast = Some(fast);
    def.indexed = true;
    def.stored = false;
    def
}

/// One document, dated 2024-06-15, filed under `/electronics/phones`.
fn store_with_one_doc(temp: &TempDir, index: &str) -> HybridStore {
    let store = HybridStore::new(config(temp.path().to_path_buf()), 1).unwrap();

    let mut fields = HashMap::new();
    fields.insert("title".into(), field(TantivyFieldType::Text, false));
    fields.insert("created".into(), field(TantivyFieldType::Date, true));
    fields.insert("cat".into(), field(TantivyFieldType::Facet, false));

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
                    "title": "rust programming",
                    "created": "2024-06-15T00:00:00Z",
                    "cat": "/electronics/phones"
                })),
            },
        )
        .unwrap();
    store.commit_index(index).unwrap();
    store
}

/// Assert a query matches the single document and lost nothing on the way.
fn assert_matches(store: &HybridStore, index: &str, query: &str) {
    let outcome = store.search_documents(index, query, 10, None).unwrap();
    assert!(
        outcome.discarded.is_empty(),
        "{query:?} discarded a clause: {:?}",
        outcome.discarded
    );
    assert_eq!(outcome.total_hits, 1, "{query:?} matched nothing");
}

#[test]
fn exclusive_and_mixed_date_range_bounds_are_normalized() {
    // Tantivy's date parser accepts only RFC3339, so both bounds are rewritten whichever
    // delimiters enclose them.
    let temp = TempDir::new().unwrap();
    let store = store_with_one_doc(&temp, "dates");

    for query in [
        "created:[2024-01-01 TO 2024-12-31]",
        "created:{2024-01-01 TO 2024-12-31}",
        "created:[2024-01-01 TO 2024-12-31}",
        "created:{2024-01-01 TO 2024-12-31]",
    ] {
        assert_matches(&store, "dates", query);
    }
}

#[test]
fn exclusive_date_bounds_still_exclude() {
    // The document sits exactly on the lower bound, so the delimiter decides whether it
    // matches. Rewriting the literal must not change which delimiter is in force.
    let temp = TempDir::new().unwrap();
    let store = store_with_one_doc(&temp, "bounds");

    let inclusive = store
        .search_documents("bounds", "created:[2024-06-15 TO 2024-12-31]", 10, None)
        .unwrap();
    assert_eq!(inclusive.total_hits, 1, "inclusive lower bound must match");

    let exclusive = store
        .search_documents("bounds", "created:{2024-06-15 TO 2024-12-31}", 10, None)
        .unwrap();
    assert!(exclusive.discarded.is_empty());
    assert_eq!(
        exclusive.total_hits, 0,
        "exclusive lower bound must still exclude the boundary value"
    );
}

#[test]
fn date_set_queries_are_normalized() {
    // Whitespace is permitted after the colon and around `IN`, in any combination.
    let temp = TempDir::new().unwrap();
    let store = store_with_one_doc(&temp, "sets");

    for query in [
        "created: IN [2024-06-15]",
        "created: IN [2024-06-15 2023-01-01]",
        "created:IN [2024-06-15]",
        "created: IN [2024-06-15T00:00:00Z]",
    ] {
        assert_matches(&store, "sets", query);
    }

    // A set that does not contain the document's date must still not match it.
    let miss = store
        .search_documents("sets", "created: IN [2023-01-01 2022-05-05]", 10, None)
        .unwrap();
    assert!(miss.discarded.is_empty());
    assert_eq!(miss.total_hits, 0, "a non-matching set must not match");
}

#[test]
fn already_normalized_date_forms_are_untouched() {
    // Rewriting is idempotent: a literal already in RFC3339 survives a pass unchanged.
    let temp = TempDir::new().unwrap();
    let store = store_with_one_doc(&temp, "idem");

    for query in [
        "created:2024-06-15T00:00:00Z",
        "created:>2024-01-01T00:00:00Z",
        "created:[2024-01-01T00:00:00Z TO 2024-12-31T00:00:00Z]",
        "created:{2024-01-01T00:00:00Z TO 2024-12-31T00:00:00Z}",
    ] {
        assert_matches(&store, "idem", query);
    }
}

#[test]
fn facet_paths_do_not_need_quoting() {
    // The parser resolves a facet term only from a quoted value, so unquoted paths are quoted
    // on the way in. Matching is hierarchical.
    let temp = TempDir::new().unwrap();
    let store = store_with_one_doc(&temp, "facets");

    for query in [
        "cat:/electronics/phones",
        "cat:\"/electronics/phones\"",
        // A parent path matches its descendants.
        "cat:/electronics",
        "cat:\"/electronics\"",
    ] {
        assert_matches(&store, "facets", query);
    }

    let miss = store
        .search_documents("facets", "cat:/furniture", 10, None)
        .unwrap();
    assert_eq!(miss.total_hits, 0, "an unrelated facet path must not match");
}

#[test]
fn a_facet_path_composes_with_other_clauses() {
    // The path ends at whitespace or a closing paren, not at the end of the query.
    let temp = TempDir::new().unwrap();
    let store = store_with_one_doc(&temp, "facet_combo");

    for query in [
        "cat:/electronics/phones AND title:rust",
        "title:rust AND cat:/electronics/phones",
        "(cat:/electronics/phones OR cat:/books) AND title:rust",
        "cat:/electronics/phones created:[2024-01-01 TO 2024-12-31]",
    ] {
        assert_matches(&store, "facet_combo", query);
    }
}

#[test]
fn an_index_without_date_or_facet_fields_is_unaffected() {
    // Both rewriters are keyed on the schema, so a text-only index is passed through.
    let temp = TempDir::new().unwrap();
    let store = HybridStore::new(config(temp.path().to_path_buf()), 1).unwrap();

    let mut fields = HashMap::new();
    fields.insert("title".into(), field(TantivyFieldType::Text, false));
    let mut schema = IndexSchema {
        fields,
        ..Default::default()
    };
    schema.rebuild_shadow_fields_cache();
    store.store_schema_and_cache("plain", &schema).unwrap();
    store
        .apply_write(
            "plain",
            WalOp::Put {
                id: "d1".to_string(),
                json_blob: Some(serde_json::json!({"title": "rust programming"})),
            },
        )
        .unwrap();
    store.commit_index("plain").unwrap();

    assert_matches(&store, "plain", "title:rust");
    assert_matches(&store, "plain", "title:\"rust programming\"");
}

/// An index whose identifier is declared under its source name, `sha1` as a shadow of `id`.
///
/// One document is enough for the rewrite tests: what is asserted is the form the engine runs,
/// read back through validation, plus the match itself where the position is a real reference.
fn store_with_shadow(temp: &TempDir, index: &str) -> HybridStore {
    let store = HybridStore::new(config(temp.path().to_path_buf()), 1).unwrap();

    let mut fields = HashMap::new();
    fields.insert("id".into(), field(TantivyFieldType::Text, false));
    fields.insert("title".into(), field(TantivyFieldType::Text, false));
    let mut shadow = field(TantivyFieldType::Text, false);
    shadow.indexed = false;
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
                json_blob: Some(serde_json::json!({"id": "d1", "title": "rust programming"})),
            },
        )
        .unwrap();
    store.commit_index(index).unwrap();
    store
}

/// The query form the engine runs, with modifiers left out of it.
fn normalized(store: &HybridStore, index: &str, query: &str) -> String {
    store
        .validate_query(index, query)
        .unwrap()
        .unwrap_or_else(|| panic!("{index} should validate"))
        .normalized_query
}

/// Every position a field name can appear in rewrites the shadow name to `id`.
#[test]
fn a_shadow_reference_is_rewritten_wherever_a_field_name_can_appear() {
    let temp = TempDir::new().unwrap();
    let store = store_with_shadow(&temp, "files");

    for (query, expected) in [
        ("sha1:d1 AND title:rust", "id:d1 AND title:rust"),
        ("title:rust AND sha1:d1", "title:rust AND id:d1"),
        ("+sha1:d1 +title:rust", "+id:d1 +title:rust"),
        (
            "(sha1:d1 OR sha1:d2) AND title:rust",
            "(id:d1 OR id:d2) AND title:rust",
        ),
        ("title:rust NOT sha1:d2", "title:rust NOT id:d2"),
        ("sha1: IN [d1 d2]", "id: IN [d1 d2]"),
        // An escaped colon inside the value is the value's; the name span still rewrites.
        (
            "sha1:dead\\:beef AND title:rust",
            "id:dead\\:beef AND title:rust",
        ),
    ] {
        let normalized = normalized(&store, "files", query);
        assert!(
            normalized.contains(expected),
            "{query:?} should rewrite to contain {expected:?}, got {normalized:?}"
        );
        assert!(
            !normalized.contains("sha1"),
            "{query:?} should leave no shadow reference behind: {normalized:?}"
        );
    }

    // And the rewritten query actually answers, with nothing dropped.
    let outcome = store
        .search_documents("files", "(sha1:d1 OR sha1:d2) AND title:rust", 10, None)
        .unwrap();
    assert_eq!(outcome.total_hits, 1);
    assert!(outcome.discarded.is_empty(), "{:?}", outcome.discarded);
}

/// A shadow name inside a phrase, a range or a set is a value, not a field reference, and must
/// pass through untouched — as must names that merely look like one.
#[test]
fn a_shadow_name_in_a_value_position_is_not_rewritten() {
    let temp = TempDir::new().unwrap();
    let store = store_with_shadow(&temp, "files");

    for query in [
        // Phrases hold text, not field references.
        "title:\"sha1 d1\"",
        "\"sha1:d1\" AND title:rust",
        // A name that is not the shadow field, including the canonical one.
        "title:rust AND nosuch:d1",
        "id:d1 AND title:rust",
    ] {
        let normalized = normalized(&store, "files", query);
        assert_eq!(
            normalized, query,
            "{query:?} has no shadow reference to rewrite"
        );
    }
}

/// The key-value fast path still owns the bare lookup — rewriting is for the query string the
/// parser sees, and a standalone `sha1:VALUE` never reaches the parser.
#[test]
fn a_bare_shadow_lookup_stays_on_the_fast_path() {
    let temp = TempDir::new().unwrap();
    let store = store_with_shadow(&temp, "files");

    let outcome = store.validate_query("files", "sha1:d1").unwrap().unwrap();
    assert_eq!(
        outcome.normalized_query, "sha1:d1",
        "the fast path answers the query as written, with no rewrite to show"
    );
    assert!(outcome.discarded.is_empty());
}

/// A shadow index whose one document is keyed by `id`, so a test can choose an identifier
/// whose spelling is the point — one carrying a colon, say.
fn store_with_shadow_keyed(temp: &TempDir, index: &str, id: &str) -> HybridStore {
    let store = HybridStore::new(config(temp.path().to_path_buf()), 1).unwrap();

    let mut fields = HashMap::new();
    fields.insert("id".into(), field(TantivyFieldType::Text, false));
    fields.insert("title".into(), field(TantivyFieldType::Text, false));
    let mut shadow = field(TantivyFieldType::Text, false);
    shadow.indexed = false;
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
                id: id.to_string(),
                json_blob: Some(serde_json::json!({"id": id, "title": "rust programming"})),
            },
        )
        .unwrap();
    store.commit_index(index).unwrap();
    store
}

/// An identifier carrying a colon reads the same whichever path answers it.
///
/// The key-value fast path answers a bare lookup without parsing, and the search index answers
/// the same clause inside a larger query after parsing. The parser drops the backslash from
/// `\:`, so the fast path has to as well: looking the key up with the escape still in it finds
/// nothing, and the caller sees a lookup that fails while the compound query it is a clause of
/// succeeds — the same identifier, two answers.
#[test]
fn an_escaped_identifier_reads_the_same_bare_and_inside_a_larger_query() {
    let temp = TempDir::new().unwrap();
    let store = store_with_shadow_keyed(&temp, "files", "urn:x:1");

    for query in [
        // Bare, on the key-value path: escaped, and unescaped as the parser also accepts it.
        "sha1:urn\\:x\\:1",
        "id:urn\\:x\\:1",
        "sha1:urn:x:1",
        "id:urn:x:1",
        // The same clause inside a larger query, on the search index.
        "sha1:urn\\:x\\:1 AND title:rust",
        "title:rust AND id:urn\\:x\\:1",
    ] {
        let outcome = store.search_documents("files", query, 10, None).unwrap();
        assert_eq!(
            outcome.total_hits, 1,
            "{query:?} names the one document in the index but matched nothing"
        );
    }
}

/// A boost is syntax, so the query belongs to the parser.
///
/// The key-value store can only look a key up whole. Handed `d1^2` it looks for a key spelled
/// exactly that, finds none, and reports zero hits — a wrong answer with nothing to distinguish
/// it from a right one. Left to the parser, the operator means what it says and the document
/// comes back.
#[test]
fn a_boosted_identifier_clause_is_left_to_the_parser() {
    let temp = TempDir::new().unwrap();
    let store = store_with_shadow(&temp, "files");

    for query in ["sha1:d1^2", "id:d1^2"] {
        let outcome = store.search_documents("files", query, 10, None).unwrap();
        assert!(
            outcome.discarded.is_empty(),
            "{query:?} discarded a clause: {:?}",
            outcome.discarded
        );
        assert_eq!(
            outcome.total_hits, 1,
            "{query:?} should reach the search index, which understands the operator"
        );
    }
}

/// An identifier that genuinely contains an operator is still reachable, by escaping it — and
/// reachable the same way from both paths, which is the whole point of refusing the raw
/// spelling above. The escape is the parser's to resolve, so the escaped form goes to the
/// parser too rather than being looked up with the backslash still in it.
#[test]
fn an_identifier_containing_an_operator_is_reachable_escaped() {
    let temp = TempDir::new().unwrap();
    let store = store_with_shadow_keyed(&temp, "files", "d1^2");

    for query in [
        "id:d1\\^2",
        "sha1:d1\\^2",
        "title:rust AND id:d1\\^2",
        "title:rust AND sha1:d1\\^2",
    ] {
        let outcome = store.search_documents("files", query, 10, None).unwrap();
        assert_eq!(
            outcome.total_hits, 1,
            "{query:?} names the one document in the index but matched nothing"
        );
    }

    // Unescaped, `^` is the boost operator and names a different document, so this must miss
    // rather than resolve by looking the raw spelling up as a whole key.
    let raw = store
        .search_documents("files", "id:d1^2", 10, None)
        .unwrap();
    assert_eq!(raw.total_hits, 0, "unescaped, the `^` is syntax");
}

/// `~` is not an operator against a bare term, so an identifier may contain one and the
/// key-value path can look it up whole. Pinned because it is the boundary of the rule above:
/// only what the parser reads as syntax belongs to the parser.
#[test]
fn a_tilde_in_an_identifier_is_an_ordinary_character() {
    let temp = TempDir::new().unwrap();
    let store = store_with_shadow_keyed(&temp, "files", "d1~1");

    for query in ["id:d1~1", "sha1:d1~1", "title:rust AND sha1:d1~1"] {
        let outcome = store.search_documents("files", query, 10, None).unwrap();
        assert_eq!(outcome.total_hits, 1, "{query:?} matched nothing");
    }
}

/// A prefix is not a key, and neither is a wildcarded one.
///
/// Pinned alongside the operators above because it is the same rule: the fast path answers only
/// what the key-value store can answer whole, and everything else goes to the index that can.
#[test]
fn a_prefixed_identifier_clause_is_left_to_the_parser() {
    let temp = TempDir::new().unwrap();
    let store = store_with_shadow(&temp, "files");

    let outcome = store
        .search_documents("files", "sha1:d*", 10, None)
        .unwrap();
    assert!(outcome.discarded.is_empty(), "{:?}", outcome.discarded);
    assert_eq!(outcome.total_hits, 1, "a prefix over the identifiers");
}

/// The count-only path answers an identifier lookup from the key-value store too, so it has to
/// read the value the same way the retrieving path does.
///
/// `limit = 0` takes its own branch: it never parses, and reports 1 or 0 from whether the key
/// exists. Reading the value differently there would make a count disagree with the page it is
/// meant to describe.
#[test]
fn a_count_only_identifier_lookup_reads_the_value_as_the_search_does() {
    let temp = TempDir::new().unwrap();
    let store = store_with_shadow_keyed(&temp, "files", "urn:x:1");

    for query in ["sha1:urn\\:x\\:1", "id:urn\\:x\\:1", "sha1:urn:x:1"] {
        let counted = store.search_documents("files", query, 0, None).unwrap();
        let fetched = store.search_documents("files", query, 10, None).unwrap();
        assert_eq!(
            counted.total_hits, 1,
            "{query:?} counts the document it retrieves"
        );
        assert_eq!(
            counted.total_hits, fetched.total_hits,
            "{query:?}: the count must agree with the page"
        );
    }

    // An identifier no document has counts zero, rather than counting the escape.
    let missing = store
        .search_documents("files", "sha1:urn\\:x\\:2", 0, None)
        .unwrap();
    assert_eq!(missing.total_hits, 0);
}
