//! Data written by released builds opens in this one (ROADMAP E4).
//!
//! Each fixture under `tests/fixtures/compat/` is a shard written by the release build it is
//! named for, by the generator kept beside it, and left the way a killed process leaves it:
//! documents committed, one of them deleted, and a WAL tail durable in redb that Tantivy never
//! got — two new documents, one replaced, one deleted. Opening it here has to replay that tail
//! and answer as if the tail had been committed.
//!
//! - **0.3.0** is the legacy shape: its index declares `_seq` and its WAL entries are whole
//!   `WalOp` JSON values. Nothing at HEAD can write either any more, which is why the fixture is
//!   the only end-to-end test of both compatibility paths.
//! - **0.3.9** is the shape 0.3.x ends on: an id-only WAL and the checkpoint in the commit
//!   payload. It is what 0.4.0 has to keep opening.
//!
//! The fixtures are never rewritten by a test: each run opens a copy.

use serde_json::Value as JsonValue;
use std::collections::BTreeSet;
use std::path::Path;
use storage::{HybridStore, MetricValue, MetricsSpec, SearchOptions, StorageConfig, WalOp};
use tempfile::TempDir;

const INDEX: &str = "compat";

/// The ids alive after the tail replays: d1–d6 committed less d3, then t1 and t2 added and d4
/// deleted by the tail. d2 is alive and replaced.
const ALIVE: [&str; 6] = ["d1", "d2", "d5", "d6", "t1", "t2"];

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).expect("create dir");
    for entry in std::fs::read_dir(from).expect("read fixture dir") {
        let entry = entry.expect("entry");
        let target = to.join(entry.file_name());
        if entry.file_type().expect("file type").is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), &target).expect("copy fixture file");
        }
    }
}

/// A private copy of the fixture's shard.
fn shard_copy(version: &str) -> TempDir {
    let temp = TempDir::new().expect("temp dir");
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/compat")
        .join(version)
        .join("shard");
    copy_dir(&fixture, temp.path());
    temp
}

/// Opened as a node starts a shard: the store, then phase 1 of startup, which replays each WAL
/// tail into its writer, and the commit the shard makes for each index phase 1 recovered.
fn open(shard: &Path) -> (HybridStore, Vec<String>) {
    let store = HybridStore::new(
        StorageConfig {
            shard_path: shard.to_path_buf(),
            default_batch_size: 100_000,
            ..StorageConfig::default()
        },
        1,
    )
    .expect("a shard written by a released build opens");
    let plan = store.recover_indices().expect("its WAL tail replays");
    assert!(
        plan.failed.is_empty(),
        "recovery failed for {:?}",
        plan.failed
    );
    for index in &plan.recovered {
        store
            .commit_index(index)
            .expect("the replayed tail commits");
    }
    (store, plan.recovered)
}

fn ids(store: &HybridStore, query: &str) -> BTreeSet<String> {
    let outcome = store
        .search_documents(INDEX, query, 100, None)
        .unwrap_or_else(|e| panic!("search {query:?}: {e}"));
    outcome
        .hits
        .iter()
        .map(|(_, doc)| doc["id"].as_str().expect("hit id").to_string())
        .collect()
}

fn set(ids: &[&str]) -> BTreeSet<String> {
    ids.iter().map(|id| id.to_string()).collect()
}

fn stored(store: &HybridStore, id: &str) -> Option<JsonValue> {
    store
        .get_by_key(INDEX, id)
        .expect("get by key")
        .map(|bytes| {
            let stored: JsonValue =
                serde_json::from_slice(&bytes).expect("stored document is JSON");
            stored["json_blob"].clone()
        })
}

fn opens_and_answers(version: &str) {
    let shard = shard_copy(version);
    let (store, recovered) = open(shard.path());
    assert_eq!(
        recovered,
        [INDEX],
        "{version}: the tail was there to replay"
    );

    // The document store: every write the tail made is there, the deletes are gone.
    for id in ALIVE {
        assert!(stored(&store, id).is_some(), "{version}: {id} is stored");
    }
    for id in ["d3", "d4"] {
        assert!(stored(&store, id).is_none(), "{version}: {id} was deleted");
    }
    let replaced = stored(&store, "d2").expect("d2");
    assert_eq!(
        replaced["title"], "replaced in the tail",
        "{version}: {replaced}"
    );

    // The search index: the tail replayed, so search answers what redb holds.
    assert_eq!(
        ids(&store, "*"),
        set(&ALIVE),
        "{version}: every live document"
    );
    assert_eq!(
        ids(&store, "title:tail"),
        set(&["d2", "t1", "t2"]),
        "{version}: the tail's titles, the replaced one included"
    );
    assert_eq!(
        ids(&store, "title:committed"),
        set(&["d1", "d5", "d6"]),
        "{version}: d2's old title is gone with the replacement"
    );
    assert_eq!(ids(&store, "tags:tag-7"), set(&["t1"]), "{version}: string");
    assert_eq!(
        ids(&store, "count:[5 TO 7]"),
        set(&["d5", "d6", "t1"]),
        "{version}: i64"
    );
    assert_eq!(ids(&store, "addr:10.0.0.8"), set(&["t2"]), "{version}: ip");
    assert_eq!(
        ids(&store, "active:true"),
        set(&["d2", "d6", "t2"]),
        "{version}: bool"
    );

    // A fast column, read whole: min and max over what is alive.
    let spec = MetricsSpec {
        min: vec!["count".to_string()],
        max: vec!["count".to_string(), "seen".to_string()],
    };
    let outcome = store
        .search_keys(
            INDEX,
            "*",
            10,
            None,
            &SearchOptions {
                metrics: Some(&spec),
                ..SearchOptions::default()
            },
        )
        .expect("metrics");
    let metrics = outcome.metrics.expect("metrics asked for");
    assert_eq!(metrics.min["count"], Some(MetricValue::I64(1)), "{version}");
    assert_eq!(metrics.max["count"], Some(MetricValue::I64(8)), "{version}");
    assert!(
        matches!(metrics.max["seen"], Some(MetricValue::Date(_))),
        "{version}: {:?}",
        metrics.max["seen"]
    );

    // Still writable, and what is written now survives another open.
    store
        .apply_write(
            INDEX,
            WalOp::Put {
                id: "n1".to_string(),
                json_blob: Some(serde_json::json!({"title": "written by this build", "count": 9})),
            },
        )
        .expect("write after open");
    store.commit_index(INDEX).expect("commit after open");
    store.shutdown().expect("shutdown");
    drop(store);

    let (store, recovered) = open(shard.path());
    assert!(recovered.is_empty(), "{version}: nothing left to replay");
    let mut alive = set(&ALIVE);
    alive.insert("n1".to_string());
    assert_eq!(ids(&store, "*"), alive, "{version}: reopened after a write");
}

#[test]
fn a_shard_written_by_0_3_0_opens() {
    opens_and_answers("v0.3.0");
}

#[test]
fn a_shard_written_by_0_3_9_opens() {
    opens_and_answers("v0.3.9");

    // 0.3.9 declared a ghost field: `body_en` indexes `body` with English stemming, for the
    // documents whose `lang` is `en` — the even ones.
    let shard = shard_copy("v0.3.9");
    let (store, _) = open(shard.path());
    assert_eq!(ids(&store, "body_en:run"), set(&["d2", "d6", "t2"]));
}
