//! Deleting an index while writes to it are in flight.
//!
//! A write is resolved against the index's schema well before it reaches the shard's writer
//! thread. If the index is deleted in between, the write must not be the thing that brings it
//! back: the schema it was validated against is gone, and rebuilding one from whatever the
//! document happens to carry gives every discovered field a non-indexed column that only a
//! reindex can change. An index in that state accepts writes and reports its fields while
//! refusing every content query against them.

use std::io::Write as _;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use client::CameoClient;
use serde_json::json;

mod common;

struct TestNode {
    child: Child,
    url: String,
    _dir: tempfile::TempDir,
}

impl TestNode {
    /// One shard, so every document in a round lands in the index under test and the race is
    /// between the writer thread and the deletion routed onto it.
    async fn start() -> TestNode {
        let dir = tempfile::tempdir().expect("temp dir");
        let port = common::reserve_port();
        let data = dir.path().join("data");
        std::fs::create_dir_all(&data).expect("data dir");

        let config = format!(
            r#"
[node]
label = "race-node"
profile = "local"

[network.http]
bind_address = "127.0.0.1"
port = {port}

[network.cluster]
enabled = false

[storage]
data_paths = ["{data}"]
num_shards_init = 1
max_shards_per_node = 1

[search]
supervisor_timeout_secs = 3600
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
            .stderr(Stdio::inherit())
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

/// Seed an index so its schema exists with `title` indexed, then delete it part way through a
/// write storm. Returns the client and the index name.
async fn race_round(client: &Arc<CameoClient>, index: &str) {
    for i in 0..5 {
        let id = format!("seed-{i}");
        let _ = client
            .write_document(index, &id, &json!({"id": id, "title": "seed"}), None)
            .await;
    }
    let _ = client.admin_index_commit(index).await;

    let mut tasks = Vec::new();
    for w in 0..4 {
        let c = Arc::clone(client);
        let ix = index.to_string();
        tasks.push(tokio::spawn(async move {
            for i in 0..40 {
                let id = format!("r{w}-{i}");
                let _ = c
                    .write_document(&ix, &id, &json!({"id": id, "title": "racing"}), None)
                    .await;
            }
        }));
    }
    {
        let c = Arc::clone(client);
        let ix = index.to_string();
        tasks.push(tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            let _ = c.delete_index(&ix, true).await;
        }));
    }
    for t in tasks {
        t.await.expect("task did not panic");
    }
}

/// An index that survives the race must still be able to answer for the field it holds.
///
/// The failure this guards is not an error but a shape: `title` present, `indexed: false`, and
/// every query naming it refused. Ten rounds, because the defect it replaced reproduced on the
/// first one.
#[tokio::test]
async fn an_index_surviving_the_race_can_still_answer_for_its_fields() {
    let node = TestNode::start().await;
    let client = Arc::new(node.client());

    let mut degraded = Vec::new();
    for round in 0..10 {
        let index = format!("racy{round}");
        race_round(&client, &index).await;

        if let Err(e) = client
            .search(&index, "title:racing", Some(5), None, None, None)
            .await
            && e.to_string().contains("not indexed")
        {
            degraded.push(index);
        }
    }

    assert!(
        degraded.is_empty(),
        "{} of 10 rounds left an index that reports a field it cannot search: {:?}",
        degraded.len(),
        degraded
    );
}

/// Writing under the name again after the race produces an ordinary, searchable index.
///
/// Whether the racing writes recreated the index or were refused, what is left has to be
/// something the next write can build on rather than a name pinned to a schema nobody chose.
#[tokio::test]
async fn the_name_is_usable_again_after_the_race() {
    let node = TestNode::start().await;
    let client = Arc::new(node.client());

    race_round(&client, "reused").await;

    for i in 0..10 {
        let id = format!("after-{i}");
        client
            .write_document(
                "reused",
                &id,
                &json!({"id": id, "title": "after the race"}),
                None,
            )
            .await
            .expect("write after the race");
    }
    client
        .admin_index_commit("reused")
        .await
        .expect("commit after the race");

    let hits = client
        .search("reused", "title:after", Some(50), None, None, None)
        .await
        .expect("content search after the race");
    assert_eq!(
        hits["hits"].as_array().map(|h| h.len()).unwrap_or(0),
        10,
        "documents written after the race should be searchable by content"
    );

    let config = client
        .get_index_config("reused")
        .await
        .expect("index config");
    let title_indexed = serde_json::to_value(&config)
        .ok()
        .and_then(|v| {
            v["fields"]
                .as_array()?
                .iter()
                .find(|f| f["name"] == "title")
                .map(|f| f["indexed"] == json!(true))
        })
        .unwrap_or(false);
    assert!(
        title_indexed,
        "title should be indexed in the schema the rebuilt index settled on: {}",
        serde_json::to_string_pretty(&config).unwrap_or_default()
    );
}

/// After an index is dropped with its schema, the next write mints it once — not once per write.
///
/// The drop leaves a record in place of the schema, so a write still carrying the old one cannot
/// reinstall it. A write that then mints the index by sampling its documents added the fields
/// itself, so nothing "evolved", and the node went on caching the record: every later batch read
/// "no schema" and minted again from its own documents. Each mint overwrote the stored types
/// while the Tantivy index kept the first one's, until a value the built column could not hold
/// reached the writer and killed it — loading `booksummaries.tsv` after `delete --delete-schema`
/// lost 15,000 of 16,559 documents that way.
///
/// So `when`, which the first batch types as a date, must stay a date when a second batch whose
/// sample would type it as an integer arrives. Measured on the old rule: the re-mint typed it
/// `I64`, every shard's writer died on `Expected a Date for field "when"`, the next batches were
/// refused, and the commit failed — no document written after the drop was searchable.
#[tokio::test]
async fn a_dropped_index_is_minted_once_by_the_writes_that_recreate_it() {
    let node = TestNode::start().await;
    let client = node.client();
    let batch = |prefix: &str, when: serde_json::Value| -> Vec<serde_json::Value> {
        (0..20)
            .map(|i| {
                json!({"id": format!("{prefix}-{i}"),
                       "doc": {"title": format!("{prefix} title {i}"), "when": when}})
            })
            .collect()
    };

    client
        .bulk_index("remade", &batch("old", json!("2019-03-04")))
        .await
        .expect("seed the index that will be dropped");
    let dropped_at = client
        .get_index_config("remade")
        .await
        .expect("config before the drop")
        .version;
    client
        .delete_index("remade", true)
        .await
        .expect("drop the index and its schema");
    assert!(
        client.get_index_config("remade").await.is_err(),
        "a dropped index has no schema to report; answering with the drop's record told a loader \
         the index already had one"
    );

    let batches = [
        ("first", json!("2020-01-05")),
        // Epoch seconds: a date field holds them, and a sample of this batch alone reads `I64`.
        ("second", json!(1_600_000_000)),
        ("third", json!("2021-06-07")),
    ];
    for (name, when) in batches {
        let answer = client
            .bulk_index("remade", &batch(name, when))
            .await
            .expect("bulk write");
        assert_eq!(
            answer["items_written"],
            json!(20),
            "the {name} batch should land whole; a writer killed by a re-mint refuses it: {answer}"
        );
    }

    let config = client
        .get_index_config("remade")
        .await
        .expect("config after the writes");
    let when_type = config
        .fields
        .iter()
        .find(|f| f["name"] == "when")
        .map(|f| f["type"].clone());
    assert_eq!(
        when_type,
        Some(json!("date")),
        "the first mint typed `when`, and no later batch may retype it"
    );
    // The drop's record sits one above the dropped schema; the index minted over it sits above
    // the record, so neither the record nor a write still carrying the dropped schema can be
    // installed over it.
    assert!(
        config.version > dropped_at + 1,
        "the recreated index is versioned above the drop's record ({} after {dropped_at})",
        config.version
    );

    client
        .admin_index_commit("remade")
        .await
        .expect("the commit succeeds: no writer died on a value its column cannot hold");
    let hits = client
        .search("remade", "title:title", Some(100), None, None, None)
        .await
        .expect("content search");
    assert_eq!(
        hits["total_hits"],
        json!(60),
        "every document written after the drop is searchable: {hits}"
    );
}
