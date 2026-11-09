//! Writes the 0.3.9 compatibility fixture: one shard holding one index, with committed
//! documents and an uncommitted WAL tail, left the way a killed process leaves it.
//!
//! Built and run at the 0.3.9 release commit (3e36143), never at HEAD: the fixture is evidence
//! of what that build wrote. See `crates/storage/tests/fixtures/compat/README.md`.
//!
//!     cargo run -p storage --example compat_fixture -- <empty output dir>

use serde_json::json;
use storage::{HybridStore, IndexSchema, StorageConfig, WalOp};

const INDEX: &str = "compat";

fn main() {
    let out = std::env::args().nth(1).expect("usage: compat_fixture <empty dir>");
    let shard_path = std::path::Path::new(&out).join("shard");
    std::fs::create_dir_all(&shard_path).expect("shard dir");

    let config = StorageConfig {
        shard_path,
        max_open_indexes: 0,
        indexer_memory_budget: 16 * 1024 * 1024,
        indexer_memory_min_mb: 16,
        indexer_memory_max_mb: 64,
        total_memory_limit_bytes: 1024 * 1024 * 1024,
        memory_pressure_threshold_percent: 80,
        indexer_num_threads: 1,
        merge_num_threads: 1,
        default_batch_size: 100_000,
        wal_sync: true,
        quick_repair: true,
        commit_interval_ms: 0,
        query: Default::default(),
    };
    let store = HybridStore::new(config, 1).expect("store");

    let mut schema: IndexSchema = serde_json::from_value(json!({
        "description": "Written by 0.3.9 as a compatibility fixture.",
        "default_fields": ["title", "body"],
        "fields": {
            "title": {"type": "text", "description": "A title."},
            "body": {"type": "text"},
            "tags": {"type": "string"},
            "count": {"type": "i64", "fast": true},
            "size": {"type": "u64"},
            "score": {"type": "f64", "fast": true},
            "seen": {"type": "date", "fast": true},
            "addr": {"type": "ip"},
            "active": {"type": "boolean"},
            "meta": {"type": "json"},
            "lang": {"type": "string"},
            "body_en": {"type": "text", "tokenizer": "en_stem",
                        "ghost": {"source": "body", "when": {"field": "lang", "in": ["en"]}}}
        }
    }))
    .expect("schema");
    schema.normalize_after_deserialization();
    schema.validate_ghosts().expect("ghosts");
    store
        .store_schema_and_cache(INDEX, &schema)
        .expect("store schema");

    let doc = |n: i64, title: &str| {
        json!({
            "title": title,
            "body": format!("running documents number {n}"),
            "tags": ["alpha", format!("tag-{n}")],
            "count": n,
            "size": n as u64 * 1000,
            "score": n as f64 / 4.0,
            "seen": format!("2026-11-0{n}T12:00:00Z"),
            "addr": format!("10.0.0.{n}"),
            "active": n % 2 == 0,
            "meta": {"n": n, "kind": "fixture"},
            "lang": if n % 2 == 0 { "en" } else { "hr" },
        })
    };
    let put = |id: &str, body: serde_json::Value| WalOp::Put {
        id: id.to_string(),
        json_blob: Some(body),
    };
    let delete = |id: &str| WalOp::Delete { id: id.to_string() };

    // Committed: d1–d6, then d3 deleted.
    for n in 1..=6 {
        store
            .apply_write(INDEX, put(&format!("d{n}"), doc(n, &format!("committed {n}"))))
            .expect("write");
    }
    store.apply_write(INDEX, delete("d3")).expect("delete");
    store.commit_index(INDEX).expect("commit");

    // The tail, durable in redb and never committed to Tantivy: two new documents, d2
    // replaced, d4 deleted.
    store
        .apply_write(INDEX, put("t1", doc(7, "tail seven")))
        .expect("tail write");
    store
        .apply_write(INDEX, put("t2", doc(8, "tail eight")))
        .expect("tail write");
    store
        .apply_write(INDEX, put("d2", doc(2, "replaced in the tail")))
        .expect("tail replace");
    store.apply_write(INDEX, delete("d4")).expect("tail delete");

    // As a killed process: no commit, no close, no destructor.
    std::process::exit(0);
}
