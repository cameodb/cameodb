//! The query path must not panic — or hang — on anything a client can send.
//!
//! Release builds carry `panic = "abort"`, so a panic here is not a 500 — it takes the process
//! down, and a scatter-gather sends the same query to every node holding the index. That makes
//! the query parsers the one place where an unhandled panic is a cluster-wide outage triggered
//! by an HTTP body. A non-terminating parse is worse still: it never panics, so nothing aborts,
//! but it pins a search thread and grows memory without bound until the node is out of it.
//!
//! Query text reaches three surfaces that slice it by byte offset: `field_references` hands
//! back spans for a rewriter to splice over, and `validate_query` and `search_documents` both
//! run the normalizers that rewrite date literals in place. Byte offsets over arbitrary UTF-8
//! are where a slice lands mid-character, so the corpus is built out of multi-byte characters
//! sitting against the delimiters the grammar reads.
//!
//! The non-termination case is concrete: the lenient set parser (`IN [ ... ]`) skips
//! inter-element space with an ASCII-only `multispace`, but treats everything that is not
//! `char::is_whitespace()` as a term character. A character that is whitespace to Rust but not
//! ASCII space — a non-breaking space, an ideographic space, a form feed — is neither, so an
//! unterminated set like `IN[\u{a0}` consumes nothing and loops. The corpus carries those
//! characters against `IN` and `[`, and the hand-written cases pin the shape down by name.

use serde_json::json;
use storage::{
    FieldDef, HybridStore, IndexSchema, StorageConfig, TantivyFieldType, WalOp, field_references,
};
use tempfile::TempDir;

const INDEX: &str = "fuzzed";

fn test_config(path: std::path::PathBuf) -> StorageConfig {
    StorageConfig {
        shard_path: path,
        indexer_memory_budget: 32 * 1024 * 1024,
        indexer_memory_min_mb: 16,
        indexer_memory_max_mb: 256,
        total_memory_limit_bytes: 4 * 1024 * 1024 * 1024,
        memory_pressure_threshold_percent: 80,
        indexer_num_threads: 1,
        merge_num_threads: 1,
        default_batch_size: 1000,
        wal_sync: true,
    }
}

/// A date field, because the date normalizers are the passes that rewrite by byte offset, and
/// they only run for a schema that declares one.
fn store(dir: &TempDir) -> HybridStore {
    let store = HybridStore::new(test_config(dir.path().to_path_buf()), 1).expect("HybridStore");

    let mut schema = IndexSchema::default();
    for (name, ty) in [
        ("id", TantivyFieldType::Text),
        ("title", TantivyFieldType::Text),
        ("created", TantivyFieldType::Date),
        ("count", TantivyFieldType::I64),
    ] {
        schema
            .fields
            .insert(name.to_string(), FieldDef::new(name.to_string(), ty));
    }
    schema.normalize_after_deserialization();
    store.store_schema_and_cache(INDEX, &schema).expect("schema");

    store
        .apply_batch(
            INDEX,
            vec![WalOp::Put {
                id: "seed".to_string(),
                json_blob: Some(json!({
                    "id": "seed",
                    "title": "seed document",
                    "created": "2024-06-15T12:00:00Z",
                    "count": 1,
                })),
            }],
        )
        .expect("seed");
    store.commit_index(INDEX).expect("commit");
    store
}

/// Pieces chosen for where they can land rather than for looking like a query: every grammar
/// delimiter, the field names the normalizers key off, and characters of one to four bytes
/// including two that `split_whitespace` treats as separators and one that it does not.
const PIECES: &[&str] = &[
    // Delimiters the parsers scan for.
    ":", "[", "]", "{", "}", "\"", "(", ")", "\\", "*", "?", "^", "~", "+", "-", ">", "<", "=",
    "/", ",", "|", "!", ".", // Keywords the passes split on.
    " TO ", "TO", " IN ", "IN", "AND", "OR", "NOT", // Names in and out of the schema.
    "created", "title", "id", "count", "missing", // Date-shaped fragments.
    "2024-06-15", "2024", "T12:00:00Z", "12:00:00", "now",
    // Multi-byte characters, 2 to 4 bytes.
    "é", "日", "🎉", "\u{301}", // Whitespace that is not a space: NBSP and ideographic space.
    "\u{a0}", "\u{3000}", // Zero-width space: three bytes, and not whitespace.
    "\u{200b}", // Ordinary separators.
    " ", "\t", "\n",
];

/// xorshift64*, so the corpus is the same on every run and a failure is reproducible from the
/// seed alone.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

/// Every span `field_references` reports must be a slice a rewriter can take. A span off a
/// character boundary is not a wrong answer, it is an abort in the caller that splices it.
fn spans_are_sliceable(query: &str) {
    for reference in field_references(query) {
        assert!(
            query.get(reference.span.clone()).is_some(),
            "span {:?} is not a slice of {query:?}",
            reference.span
        );
    }
}

#[test]
fn no_generated_query_panics_any_parser() {
    let dir = TempDir::new().expect("temp dir");
    let store = store(&dir);

    let mut rng = Rng(0x9e37_79b9_7f4a_7c15);

    for i in 0..20_000 {
        let query: String = (0..rng.below(16))
            .map(|_| PIECES[rng.below(PIECES.len())])
            .collect();

        spans_are_sliceable(&query);
        // `validate_query` runs the date normalizers over every generated query — that is the
        // pass that rewrites by byte offset. Executing the parsed query adds nothing to the
        // question and costs a searcher per call, so a slice of the corpus carries that half.
        let _ = store.validate_query(INDEX, &query);
        if i % 40 == 0 {
            let _ = store.search_documents(INDEX, &query, 10, None);
        }
    }
}

/// The shapes worth writing down: an incomplete range, a delimiter with a multi-byte character
/// pressed against it, and a name split by something that is not a space. Generated coverage
/// reaches these, but only a named case says which one regressed.
#[test]
fn no_hand_written_edge_case_panics_any_parser() {
    let dir = TempDir::new().expect("temp dir");
    let store = store(&dir);

    let cases = [
        "",
        " ",
        "\u{a0}",
        "created:",
        "created:[",
        "created:{",
        "created:[é",
        "created:{日",
        "created:[🎉 TO 🎉]",
        "created:[2024-06-15 TO 🎉}",
        "created:{é TO 日]",
        "created:[é",
        "created:]",
        "created:[ TO ]",
        "created:[TO]",
        "created: IN [é 日 🎉]",
        "created: IN [",
        "created:>é",
        "created:>=🎉",
        "created:<=\u{200b}",
        "title:\"é",
        "title:\"日 TO 🎉\"",
        "é:2024-06-15",
        "🎉created:[2024-06-15 TO 2024-07-01]",
        "created\u{301}:[2024-06-15 TO 2024-07-01]",
        "created:[2024-06-15\u{a0}TO\u{a0}2024-07-01]",
        "(created:[é",
        "created:[é(TO)é]",
        "\\created:[é TO é]",
        "created:[é TO é]created:[日 TO 日]",
        // A range that never closes, after a name the normalizer rewrites.
        "created:[2024-06-15 TO created:[2024-06-15 TO created:[é",
        // An unterminated `IN` set whose only content is a whitespace character the set
        // parser can neither skip nor take as a term: the shape that looped the parser and
        // grew memory until the node was killed. One line per representative space — a
        // no-break space, a form feed, a NEL, an ideographic space — bare and field-qualified,
        // with and without a following term. If the fold in front of the parser regresses,
        // this case hangs instead of returning, and its name says where to look.
        "IN[\u{a0}",
        "IN[\u{c}",
        "IN[\u{85}",
        "IN[\u{3000}",
        "IN[\u{a0}x",
        "IN[\u{a0}]",
        "created: IN [\u{a0}",
        "created: IN [\u{2028}2024-06-15",
        "title:\u{a0}IN[\u{3000}",
    ];

    for query in cases {
        spans_are_sliceable(query);
        let _ = store.validate_query(INDEX, query);
        let _ = store.search_documents(INDEX, query, 10, None);
    }

    // Length is its own axis: the normalizers walk the string with a cursor, and a long run of
    // the same shape is what would expose an offset that drifts.
    let long = "created:[é TO 日] ".repeat(4_000);
    spans_are_sliceable(&long);
    let _ = store.validate_query(INDEX, &long);
    let _ = store.search_documents(INDEX, &long, 10, None);
}
