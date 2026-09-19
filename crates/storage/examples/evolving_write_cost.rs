//! How long N schema-evolving writes take, and how long N non-evolving ones take beside them.
//!
//! Not a unit test and not an assertion — a timing harness, run by hand against two builds to
//! compare them. It exists because `cameodb-bench` declares its schema up front, deliberately,
//! so nothing in the normal load path ever exercises the write that teaches an index a new
//! field. That is the write M0-i changed: it used to commit twice, the data transaction and
//! then a second `Durability::Immediate` transaction for the schema row.
//!
//! An example rather than a test, deliberately: it asserts nothing, it takes tens of seconds,
//! and `scripts/validate/unit.sh` is a gate rather than a stopwatch. `cargo test` still compiles
//! it, so it cannot rot silently.
//!
//! ```text
//! cargo run -p storage --release --example evolving_write_cost
//! ```
//!
//! The number that matters is the ratio between the two columns, not either absolute. Run it
//! against two builds to compare them; absolute figures are the machine's, not the engine's.

use serde_json::json;
use std::time::Instant;
use storage::{FieldDef, HybridStore, IndexSchema, StorageConfig, TantivyFieldType, WalOp};
use tempfile::TempDir;

const N: usize = 400;

fn config(shard_path: &std::path::Path, wal_sync: bool) -> StorageConfig {
    StorageConfig {
        shard_path: shard_path.to_path_buf(),
        indexer_memory_budget: 64 * 1024 * 1024,
        indexer_memory_min_mb: 32,
        indexer_memory_max_mb: 256,
        total_memory_limit_bytes: 4 * 1024 * 1024 * 1024,
        memory_pressure_threshold_percent: 80,
        indexer_num_threads: 1,
        merge_num_threads: 1,
        default_batch_size: 100_000,
        wal_sync,
    }
}

fn base_schema() -> IndexSchema {
    let mut schema = IndexSchema::default();
    for name in ["id", "title"] {
        schema.fields.insert(
            name.to_string(),
            FieldDef::new(name.to_string(), TantivyFieldType::Text),
        );
    }
    schema.normalize_after_deserialization();
    schema
}

/// `evolving`: every document carries a field never seen before, so every write persists a
/// schema. `!evolving`: the same documents without the new field, so none does.
fn time_writes(wal_sync: bool, evolving: bool) -> f64 {
    let dir = TempDir::new().unwrap();
    let store = HybridStore::new(config(dir.path(), wal_sync), 1).expect("store");
    let index = "stream";
    store
        .store_schema_and_cache(index, &base_schema())
        .expect("schema");

    // One write outside the timer: the first one opens the index and builds the writer.
    store
        .apply_write(
            index,
            WalOp::Put {
                id: "warm".to_string(),
                json_blob: Some(json!({ "title": "warm" })),
            },
        )
        .expect("warm");

    let start = Instant::now();
    for i in 0..N {
        let doc = if evolving {
            json!({ "title": "t", format!("f{i}"): i as i64 })
        } else {
            json!({ "title": "t" })
        };
        store
            .apply_write(
                index,
                WalOp::Put {
                    id: format!("doc-{i}"),
                    json_blob: Some(doc),
                },
            )
            .expect("write");
    }
    start.elapsed().as_secs_f64() * 1000.0
}

fn main() {
    println!("\n  N = {N} writes per cell, times in ms (lower is better)\n");
    println!(
        "  {:<12} {:>14} {:>14} {:>10}",
        "wal_sync", "evolving", "plain", "ratio"
    );
    for wal_sync in [true, false] {
        // Two rounds, best of each: the first touches cold page cache.
        let mut evolving = f64::MAX;
        let mut plain = f64::MAX;
        for _ in 0..3 {
            evolving = evolving.min(time_writes(wal_sync, true));
            plain = plain.min(time_writes(wal_sync, false));
        }
        println!(
            "  {:<12} {:>14.1} {:>14.1} {:>10.2}",
            wal_sync,
            evolving,
            plain,
            evolving / plain
        );
    }
    println!();
}
