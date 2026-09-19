//! What the per-index read cache is worth, against the two caches underneath it.
//!
//! `read_cache` holds up to 1024 document bodies per index, mirroring rows of `data_<index>`.
//! Below it sit redb's own page cache — sized by `calculate_cache_size`, 32 MB per shard at the
//! floor and tiered up by database size — and below that the operating system's page cache. A
//! third layer earns its keep only if it removes work the other two still do, and what it
//! removes here is one B-tree descent: the body is returned by `Vec::clone` either way, which is
//! the same memcpy redb's `to_vec` performs from a page it already holds.
//!
//! It is not free. It costs 1024 bodies per index of resident memory, unbounded in index count;
//! a generation protocol that exists purely to stop it serving a body a write has superseded;
//! and an eviction policy. `get_batch_by_keys` — the path a search takes to fetch its hits —
//! opens the redb read transaction and the table *before* consulting it, so even a total hit
//! pays for the transaction it was meant to avoid.
//!
//! ```text
//! cargo run -p storage --release --example read_cache_value
//! ```
//!
//! Three workloads, because a cache is only as good as the access pattern:
//!
//! - **hot** — 100 keys, re-read forever. Everything hits. This is the cache's best case.
//! - **uniform** — every key in the corpus in turn, so a 1024-entry cache never holds what is
//!   asked for next. This is the case where the cache is pure overhead: a miss, an insert and
//!   an eviction on every read.
//! - **search** — `search_documents` top 10, which fetches its bodies through
//!   `get_batch_by_keys`.
//!
//! Run it against a build with the cache and one without to compare; the absolutes are the
//! machine's.

use serde_json::{Value, json};
use std::time::Instant;
use storage::{FieldDef, HybridStore, IndexSchema, StorageConfig, TantivyFieldType, WalOp};
use tempfile::TempDir;

const DOCS: usize = 20_000;
const HOT_KEYS: usize = 100;

fn ops() -> usize {
    std::env::var("OPS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(50_000)
}

fn config(shard_path: &std::path::Path) -> StorageConfig {
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
        wal_sync: false,
    }
}

fn schema() -> IndexSchema {
    let mut schema = IndexSchema::default();
    for name in ["id", "title", "body"] {
        schema.fields.insert(
            name.to_string(),
            FieldDef::new(name.to_string(), TantivyFieldType::Text),
        );
    }
    schema.normalize_after_deserialization();
    schema
}

fn document(seq: usize) -> Value {
    json!({
        "title": format!("alpha bravo {seq}"),
        // A few hundred bytes, so the body copy is not free and the corpus outgrows a
        // 1024-entry cache in bytes as well as in count.
        "body": format!("{} {}", "lorem ipsum dolor sit amet ".repeat(12), seq),
    })
}

fn loaded_store(dir: &TempDir) -> HybridStore {
    let store = HybridStore::new(config(dir.path()), 1).expect("store");
    store
        .store_schema_and_cache("docs", &schema())
        .expect("schema");
    let batch: Vec<WalOp> = (0..DOCS)
        .map(|i| WalOp::Put {
            id: format!("doc-{i}"),
            json_blob: Some(document(i)),
        })
        .collect();
    store.apply_batch("docs", batch).expect("seed");
    store.commit_index("docs").expect("commit");
    store
}

fn main() {
    let ops = ops();
    let dir = TempDir::new().unwrap();
    let store = loaded_store(&dir);

    // Warm every layer that is allowed to be warm: redb's page cache, the OS page cache, and
    // whatever `read_cache` decides to keep.
    for i in 0..DOCS {
        let _ = store.get_by_key("docs", &format!("doc-{i}")).expect("warm");
    }
    for _ in 0..200 {
        store
            .search_documents("docs", "alpha", 10, None)
            .expect("warm");
    }

    let hot = {
        let start = Instant::now();
        for n in 0..ops {
            let key = format!("doc-{}", n % HOT_KEYS);
            let _ = store.get_by_key("docs", &key).expect("get");
        }
        start.elapsed().as_secs_f64() * 1_000_000.0 / ops as f64
    };

    let uniform = {
        let start = Instant::now();
        for n in 0..ops {
            let key = format!("doc-{}", n % DOCS);
            let _ = store.get_by_key("docs", &key).expect("get");
        }
        start.elapsed().as_secs_f64() * 1_000_000.0 / ops as f64
    };

    let searches = ops / 10;
    let search = {
        let start = Instant::now();
        for _ in 0..searches {
            store
                .search_documents("docs", "alpha", 10, None)
                .expect("search");
        }
        start.elapsed().as_secs_f64() * 1_000_000.0 / searches as f64
    };

    println!("\n  {DOCS} documents, {ops} reads per workload, µs per operation\n");
    println!("  {:<28} {:>12}", "workload", "µs");
    println!("  {:<28} {:>12.3}", "hot (100 keys)", hot);
    println!("  {:<28} {:>12.3}", "uniform (all keys)", uniform);
    println!("  {:<28} {:>12.3}", "search top 10", search);
    println!();
}
