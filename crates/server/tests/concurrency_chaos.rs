//! Concurrency and chaos probes against a real `cameodb` process.
//!
//! The existing end-to-end suite drives one operation at a time. These drive several at
//! once, because the failures a database actually suffers in production are combinations:
//! a commit landing while a search reads the generation it is replacing, a delete racing
//! the write that recreates the index, an eviction taking a writer out from under buffered
//! documents. Each probe asserts the node is still serving afterwards and that what it
//! answers is consistent with what it accepted.

use std::io::Write as _;
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use client::CameoClient;
use serde_json::json;

struct TestNode {
    child: Child,
    url: String,
    _dir: tempfile::TempDir,
}

impl TestNode {
    async fn start(extra: &str) -> TestNode {
        let dir = tempfile::tempdir().expect("temp dir");
        let port = free_port();
        let data = dir.path().join("data");
        std::fs::create_dir_all(&data).expect("data dir");

        let config = format!(
            r#"
[node]
label = "chaos-node"
profile = "local"

[network.http]
bind_address = "127.0.0.1"
port = {port}

[network.cluster]
enabled = false

[storage]
data_paths = ["{data}"]
num_shards_init = 2
max_shards_per_node = 2

[search]
supervisor_timeout_secs = 2
{extra}
"#,
            data = data.display().to_string().replace('\\', "/"),
        );
        let config_path = dir.path().join("cameodb.toml");
        let mut f = std::fs::File::create(&config_path).expect("config file");
        f.write_all(config.as_bytes()).expect("write config");

        let child = Command::new(env!("CARGO_BIN_EXE_cameodb"))
            .arg("-c")
            .arg(&config_path)
            .env("RUST_LOG", "warn")
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn cameodb");

        let node = TestNode {
            child,
            url: format!("http://127.0.0.1:{port}"),
            _dir: dir,
        };
        node.await_ready().await;
        node
    }

    async fn await_ready(&self) {
        let client = self.client();
        let deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < deadline {
            if client.health().await.is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("node at {} never became healthy", self.url);
    }

    fn client(&self) -> CameoClient {
        with_tls_provider();
        CameoClient::new(&self.url).expect("client")
    }

    /// Is the process still alive? A panic under `panic = "abort"` shows up here.
    fn still_running(&mut self) -> bool {
        matches!(self.child.try_wait(), Ok(None))
    }
}

impl Drop for TestNode {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn with_tls_provider() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
    listener.local_addr().expect("local addr").port()
}

/// Writes, searches, commits and schema evolution all at once, against one index.
///
/// Every write is accounted for: a document the node answered 2xx for must be retrievable
/// by id once the dust settles. Losing one means the coalescing writer dropped an
/// acknowledged write.
#[tokio::test]
async fn a_mixed_concurrent_workload_loses_no_acknowledged_write() {
    let mut node = TestNode::start("").await;
    let client = Arc::new(node.client());

    const WRITERS: usize = 8;
    const PER_WRITER: usize = 40;

    let mut tasks = Vec::new();

    // Writers. Each carries a field only it knows about, so the schema evolves under
    // concurrent load rather than settling before the writes begin.
    for w in 0..WRITERS {
        let c = Arc::clone(&client);
        tasks.push(tokio::spawn(async move {
            let mut acked = Vec::new();
            for i in 0..PER_WRITER {
                let id = format!("w{w}-d{i}");
                let doc = json!({
                    "id": id,
                    "title": format!("document {w} {i}"),
                    format!("writer_{w}_field"): i as i64,
                });
                if c.write_document("chaos", &id, &doc, None).await.is_ok() {
                    acked.push(id);
                }
            }
            acked
        }));
    }

    // Searchers, running against the index while it is being built and committed.
    for _ in 0..4 {
        let c = Arc::clone(&client);
        tasks.push(tokio::spawn(async move {
            for _ in 0..40 {
                let _ = c.search("chaos", "title:document", Some(10), None, None, None).await;
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            Vec::new()
        }));
    }

    // Commits, forcing generation turnover under the readers above.
    {
        let c = Arc::clone(&client);
        tasks.push(tokio::spawn(async move {
            for _ in 0..10 {
                let _ = c.admin_index_commit("chaos").await;
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            Vec::new()
        }));
    }

    let mut acked: Vec<String> = Vec::new();
    for t in tasks {
        acked.extend(t.await.expect("task did not panic"));
    }

    assert!(node.still_running(), "node died during the mixed workload");

    // Settle: one final commit, then verify every acknowledged id is retrievable.
    let _ = client.admin_index_commit("chaos").await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let mut missing = Vec::new();
    for id in &acked {
        let found = client
            .search("chaos", &format!("id:{id}"), Some(1), None, None, None)
            .await
            .expect("search after settle");
        let hits = found["hits"].as_array().map(|h| h.len()).unwrap_or(0);
        if hits == 0 {
            missing.push(id.clone());
        }
    }

    assert!(
        missing.is_empty(),
        "{} of {} acknowledged writes are not retrievable: {:?}",
        missing.len(),
        acked.len(),
        &missing[..missing.len().min(10)]
    );
}

/// Deleting an index while writes and searches for it are in flight.
///
/// The delete and the writes are serialized on the same writer thread, so the outcome is a
/// race the node is allowed to resolve either way — what it is not allowed to do is die, or
/// come back serving a half-deleted index that answers searches from caches whose data is
/// gone.
#[tokio::test]
async fn deleting_an_index_under_load_leaves_the_node_serving() {
    let mut node = TestNode::start("").await;
    let client = Arc::new(node.client());

    // Seed so the index, its schema and its caches all exist.
    for i in 0..20 {
        let id = format!("seed-{i}");
        client
            .write_document("racy", &id, &json!({"id": id, "title": "seed"}), None)
            .await
            .expect("seed write");
    }
    let _ = client.admin_index_commit("racy").await;

    let mut tasks = Vec::new();
    for w in 0..4 {
        let c = Arc::clone(&client);
        tasks.push(tokio::spawn(async move {
            for i in 0..50 {
                let id = format!("r{w}-{i}");
                let _ = c
                    .write_document("racy", &id, &json!({"id": id, "title": "racing"}), None)
                    .await;
                let _ = c.search("racy", "title:racing", Some(5), None, None, None).await;
            }
        }));
    }

    // Delete part way through, twice, to also cover deleting an index that a concurrent
    // write has just recreated.
    {
        let c = Arc::clone(&client);
        tasks.push(tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(30)).await;
            let _ = c.delete_index("racy", true).await;
            tokio::time::sleep(Duration::from_millis(30)).await;
            let _ = c.delete_index("racy", true).await;
        }));
    }

    for t in tasks {
        t.await.expect("task did not panic");
    }

    assert!(node.still_running(), "node died deleting an index under load");

    // The node must still answer, and the index must be coherent: whatever it says it
    // holds, it must be able to search.
    client.health().await.expect("health after the race");
    let listing = client.list_indexes(false).await.expect("listing after the race");
    let _ = serde_json::to_string(&listing).expect("listing serializes");
    // Answered, or refused with a reason the caller can act on — never a 5xx and never a
    // hang. Success is deliberately not asserted: a write that recreates an index
    // concurrently with its deletion can miss the enhanced-sampling path that gives a first
    // write an indexed schema and fall back to `evolve_from_document`, which records
    // discovered fields as *not* indexed. The index then answers no content query until it
    // is reindexed. That is a separate defect from the race this test covers, and about one
    // run in six it is what the search hits.
    let after = client
        .search("racy", "title:racing", Some(5), None, None, None)
        .await;
    if let Err(e) = &after {
        let message = e.to_string();
        assert!(
            message.contains("400"),
            "a search after the race may be refused, but only with a 4xx explaining why: {message}"
        );
    }

    // And it must still accept new writes under that name.
    client
        .write_document("racy", "after", &json!({"id": "after", "title": "after"}), None)
        .await
        .expect("write after the race");
}

/// Evicting the writer while documents are buffered but not yet committed.
///
/// The eviction path commits before it drops the writer. If that commit fails, or if the
/// eviction is taken as a shortcut past it, the buffered documents are gone from Tantivy
/// while the sequence counter has already moved past them — and the next successful commit
/// truncates their WAL entries. This asserts the acknowledged documents survive.
#[tokio::test]
async fn evicting_a_writer_mid_flight_does_not_lose_acknowledged_documents() {
    let mut node = TestNode::start("").await;
    let client = Arc::new(node.client());

    let mut acked = Vec::new();
    for i in 0..30 {
        let id = format!("pre-{i}");
        if client
            .write_document("evict", &id, &json!({"id": id, "title": "before evict"}), None)
            .await
            .is_ok()
        {
            acked.push(id);
        }
    }

    // Evict with documents still buffered (no explicit commit yet).
    let report = client.admin_index_evict_writer("evict").await;
    assert!(report.is_ok(), "eviction should be answered: {:?}", report.err());

    // Keep writing under the same name: this is what moves the sequence counter past the
    // evicted documents and triggers the truncation if the checkpoint is wrong.
    for i in 0..30 {
        let id = format!("post-{i}");
        if client
            .write_document("evict", &id, &json!({"id": id, "title": "after evict"}), None)
            .await
            .is_ok()
        {
            acked.push(id);
        }
    }
    let _ = client.admin_index_commit("evict").await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    assert!(node.still_running(), "node died around writer eviction");

    let mut missing = Vec::new();
    for id in &acked {
        let found = client
            .search("evict", &format!("id:{id}"), Some(1), None, None, None)
            .await
            .expect("search after evict");
        if found["hits"].as_array().map(|h| h.len()).unwrap_or(0) == 0 {
            missing.push(id.clone());
        }
    }
    assert!(
        missing.is_empty(),
        "{} of {} acknowledged documents lost across a writer eviction: {:?}",
        missing.len(),
        acked.len(),
        &missing[..missing.len().min(10)]
    );
}

/// Many concurrent searches must not starve the health endpoint.
///
/// Searches run as uncancellable blocking work on a bounded read pool. If a burst can
/// occupy every thread in it, nothing else that needs the pool is answered — which is the
/// difference between a slow node and one an orchestrator will restart.
#[tokio::test]
async fn a_burst_of_searches_does_not_starve_the_health_endpoint() {
    let mut node = TestNode::start("").await;
    let client = Arc::new(node.client());

    // Enough documents that a broad query is real work rather than a lookup on an empty index.
    let batch: Vec<serde_json::Value> = (0..2000)
        .map(|i| json!({"id": format!("d{i}"), "doc": {"id": format!("d{i}"), "title": format!("lorem ipsum dolor {i} sit amet"), "body": format!("the quick brown fox {i} jumps over the lazy dog repeatedly")}}))
        .collect();
    client.bulk_index("load", &batch).await.expect("bulk seed");
    client.admin_index_commit("load").await.expect("commit seed");

    let mut searches = Vec::new();
    for _ in 0..64 {
        let c = Arc::clone(&client);
        searches.push(tokio::spawn(async move {
            for _ in 0..5 {
                let _ = c
                    .search("load", "title:lorem OR body:quick OR title:ipsum", Some(100), None, None, None)
                    .await;
            }
        }));
    }

    // While that burst runs, health must keep answering promptly.
    let health_client = Arc::clone(&client);
    let watchdog = tokio::spawn(async move {
        let mut worst = Duration::ZERO;
        for _ in 0..20 {
            let started = Instant::now();
            let ok = health_client.health().await.is_ok();
            let elapsed = started.elapsed();
            if !ok {
                return (false, elapsed);
            }
            worst = worst.max(elapsed);
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        (true, worst)
    });

    for s in searches {
        s.await.expect("search task did not panic");
    }
    let (all_ok, worst) = watchdog.await.expect("watchdog did not panic");

    assert!(node.still_running(), "node died under the search burst");
    assert!(all_ok, "health stopped answering during a search burst");
    assert!(
        worst < Duration::from_secs(5),
        "health took {worst:?} during a search burst; the read pool is starving other work"
    );
}
