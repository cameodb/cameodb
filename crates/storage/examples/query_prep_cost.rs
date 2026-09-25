//! How much a search costs before it searches, and how that scales with the schema.
//!
//! `prepare_query_parser` runs per search and does work that depends on the schema rather than
//! on the query's selectivity: a whitespace fold, a shadow rewrite, date and facet
//! normalisation, a prefix pass, and then a walk of every indexed field to decide which are
//! default search fields. On the fan-out every shard repeats all of it against the same query
//! string, so whatever this costs is multiplied by the shard count.
//!
//! This measures it from outside the crate, the only way an example can: hold the corpus and
//! the query fixed, vary the number of indexed fields, and read the slope. A corpus this small
//! makes the matching work negligible, so what is left is per-search overhead.
//!
//! Two queries, and the difference between them is the whole point. `alpha` is unqualified, so
//! the parser expands it across every default search field — at 200 fields that is a 200-clause
//! disjunction which genuinely executes, and its slope is *not* all avoidable. `f0:alpha` names
//! its field, so exactly one term query runs however wide the schema is, while preparation
//! still walks every field. The qualified column is therefore the honest measure of what
//! M0-f could remove; the unqualified one shows what a wide schema costs regardless.
//!
//! ```text
//! cargo run -p storage --release --example query_prep_cost
//! ```
//!
//! Relative figures only — the slope against field count is the measurement, not the absolutes.

use serde_json::{Map, Value, json};
use std::time::Instant;
use storage::{FieldDef, HybridStore, IndexSchema, StorageConfig, TantivyFieldType, WalOp};
use tempfile::TempDir;

const DOCS: usize = 500;

/// Searches per cell, and the schema widths to sweep. Both are overridable from the environment
/// — `SEARCHES=20000 FIELDS=200` narrows the run to one cell with ten times the samples, which
/// is what it takes to separate a one-microsecond difference from this machine's noise.
fn searches() -> usize {
    std::env::var("SEARCHES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2000)
}

fn widths() -> Vec<usize> {
    match std::env::var("FIELDS") {
        Ok(raw) => raw
            .split(',')
            .filter_map(|v| v.trim().parse().ok())
            .collect(),
        Err(_) => vec![5, 25, 100, 200],
    }
}

fn config(shard_path: &std::path::Path) -> StorageConfig {
    StorageConfig {
        max_open_indexes: 0,
        shard_path: shard_path.to_path_buf(),
        indexer_memory_budget: 64 * 1024 * 1024,
        indexer_memory_min_mb: 32,
        indexer_memory_max_mb: 256,
        total_memory_limit_bytes: 4 * 1024 * 1024 * 1024,
        memory_pressure_threshold_percent: 80,
        indexer_num_threads: 1,
        merge_num_threads: 1,
        default_batch_size: 100_000,
        wal_sync: false,
        commit_interval_ms: 0,
        query: Default::default(),
    }
}

fn schema_with(fields: usize) -> IndexSchema {
    let mut schema = IndexSchema::default();
    schema.fields.insert(
        "id".to_string(),
        FieldDef::new("id".to_string(), TantivyFieldType::Text),
    );
    for i in 0..fields {
        let name = format!("f{i}");
        schema
            .fields
            .insert(name.clone(), FieldDef::new(name, TantivyFieldType::Text));
    }
    schema.normalize_after_deserialization();
    schema
}

/// Every document is the same width whatever the schema declares.
///
/// This is the whole trick, and the first version of this harness got it wrong: if documents
/// carry a value for each declared field, then a search returning ten hits deserialises ten
/// documents of `fields` values each, and *that* dominates the measurement rather than anything
/// to do with preparing the query. Holding the document width fixed leaves schema width as the
/// only variable.
fn document(_fields: usize, seq: usize) -> Value {
    let mut obj = Map::new();
    obj.insert("f0".to_string(), json!(format!("alpha bravo {seq}")));
    obj.insert("f1".to_string(), json!("filler"));
    Value::Object(obj)
}

/// Microseconds per search, on an index declaring `fields` indexed text fields.
fn per_search_us(fields: usize, query: &str, limit: usize) -> f64 {
    let dir = TempDir::new().unwrap();
    let store = HybridStore::new(config(dir.path()), 1).expect("store");
    let index = "probe";
    store
        .store_schema_and_cache(index, &schema_with(fields))
        .expect("schema");

    let ops: Vec<WalOp> = (0..DOCS)
        .map(|i| WalOp::Put {
            id: format!("doc-{i}"),
            json_blob: Some(document(fields, i)),
        })
        .collect();
    store.apply_batch(index, ops).expect("seed");
    store.commit_index(index).expect("commit");

    let searches = searches();

    // Warm the reader, the schema cache and the field cache.
    for _ in 0..50 {
        store
            .search_documents(index, query, limit, None)
            .expect("warm");
    }

    let start = Instant::now();
    for _ in 0..searches {
        store
            .search_documents(index, query, limit, None)
            .expect("search");
    }
    start.elapsed().as_secs_f64() * 1_000_000.0 / searches as f64
}

fn main() {
    println!(
        "\n  {DOCS} documents of fixed width, {} searches, µs per search\n",
        searches()
    );
    println!(
        "  {:>14} {:>18} {:>18} {:>18}",
        "indexed fields", "`alpha` top10", "`f0:alpha` top10", "`f0:alpha` count"
    );
    let (mut b_any, mut b_one, mut b_cnt) = (0.0, 0.0, 0.0);
    for (row, fields) in widths().into_iter().enumerate() {
        // Best of three: the first pass pays for cold caches.
        let (mut any, mut one, mut cnt) = (f64::MAX, f64::MAX, f64::MAX);
        for _ in 0..3 {
            any = any.min(per_search_us(fields, "alpha", 10));
            one = one.min(per_search_us(fields, "f0:alpha", 10));
            cnt = cnt.min(per_search_us(fields, "f0:alpha", 0));
        }
        if row == 0 {
            b_any = any;
            b_one = one;
            b_cnt = cnt;
        }
        println!(
            "  {:>14} {:>11.1} {:>5.2}x {:>11.1} {:>5.2}x {:>11.1} {:>5.2}x",
            fields,
            any,
            any / b_any,
            one,
            one / b_one,
            cnt,
            cnt / b_cnt
        );
    }
    println!();
}
