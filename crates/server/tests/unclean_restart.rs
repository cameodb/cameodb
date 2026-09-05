//! What a node serves after an unclean shutdown.
//!
//! A write is durable in redb's WAL long before it reaches a Tantivy segment. Startup
//! phase 1 replays that tail and commits it, so an index is searchable for everything it
//! acknowledged without waiting for further writes to trigger a flush.

use std::io::Write as _;
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use client::CameoClient;
use serde_json::json;

/// A node that can be killed and restarted against the same data directory.
struct Node {
    child: Option<Child>,
    url: String,
    config_path: std::path::PathBuf,
    _dir: tempfile::TempDir,
}

impl Node {
    async fn start() -> Node {
        let dir = tempfile::tempdir().expect("temp dir");
        let port = free_port();
        let data = dir.path().join("data");
        std::fs::create_dir_all(&data).expect("data dir");

        // One shard, so every document lands in the index this test reasons about. The
        // commit threshold and the supervisor timeout are both set far above the test's
        // lifetime, so nothing commits unless the test asks for it — which is exactly the
        // state a crash between writes leaves behind.
        let config = format!(
            r#"
[node]
label = "restart-node"
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
default_batch_size = 100000
wal_sync = true

[search]
supervisor_timeout_secs = 3600
"#,
            data = data.display().to_string().replace('\\', "/"),
        );
        let config_path = dir.path().join("cameodb.toml");
        let mut f = std::fs::File::create(&config_path).expect("config file");
        f.write_all(config.as_bytes()).expect("write config");

        let mut node = Node {
            child: None,
            url: format!("http://127.0.0.1:{port}"),
            config_path,
            _dir: dir,
        };
        node.spawn().await;
        node
    }

    async fn spawn(&mut self) {
        let child = Command::new(env!("CARGO_BIN_EXE_cameodb"))
            .arg("-c")
            .arg(&self.config_path)
            .env("RUST_LOG", "warn")
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn cameodb");
        self.child = Some(child);
        self.await_ready().await;
    }

    /// SIGKILL: no graceful shutdown, no final commit — a crash.
    async fn kill_hard(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        // Let the OS release the port before the restart tries to bind it.
        tokio::time::sleep(Duration::from_millis(300)).await;
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

impl Drop for Node {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
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

/// Documents written before a crash are searchable after the restart that replays them.
///
/// `id:` lookups are answered from redb and find them either way, so the assertion that
/// matters is the content query: it is the one that needs the tail in a committed segment.
#[tokio::test]
async fn documents_written_before_a_crash_are_searchable_after_the_restart() {
    let mut node = Node::start().await;

    {
        let client = node.client();
        for i in 0..25 {
            let id = format!("doc-{i}");
            client
                .write_document(
                    "archive",
                    &id,
                    &json!({"id": id, "title": format!("archived record {i}")}),
                    None,
                )
                .await
                .expect("write before the crash");
        }
    }

    // Crash with everything still in the writer's buffer and the WAL.
    node.kill_hard().await;
    node.spawn().await;

    let client = node.client();

    // The documents are durable: redb answers this without consulting Tantivy.
    let by_id = client
        .search("archive", "id:doc-7", Some(1), None, None, None)
        .await
        .expect("id lookup after restart");
    assert_eq!(
        by_id["hits"].as_array().map(|h| h.len()).unwrap_or(0),
        1,
        "the document store should still hold doc-7 after the crash"
    );

    // Give startup recovery room to finish before judging the search index.
    tokio::time::sleep(Duration::from_secs(2)).await;

    let by_content = client
        .search("archive", "title:archived", Some(100), None, None, None)
        .await
        .expect("content search after restart");
    let hits = by_content["hits"].as_array().map(|h| h.len()).unwrap_or(0);

    assert_eq!(
        hits, 25,
        "every document recovered from the WAL should be searchable by content after a \
         restart, got {hits} of 25"
    );
}

/// The recovery commit truncates the WAL, so a second restart has nothing left to replay and
/// answers from committed segments rather than from another round of recovery.
#[tokio::test]
async fn a_second_restart_finds_the_documents_without_replaying_them_again() {
    let mut node = Node::start().await;

    {
        let client = node.client();
        for i in 0..15 {
            let id = format!("doc-{i}");
            client
                .write_document(
                    "archive",
                    &id,
                    &json!({"id": id, "title": format!("archived record {i}")}),
                    None,
                )
                .await
                .expect("write before the crash");
        }
    }

    // Crash, restart, let phase 1 and its commit finish.
    node.kill_hard().await;
    node.spawn().await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Restart again — this time cleanly, and with no writes in between.
    node.kill_hard().await;
    node.spawn().await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    let hits = node
        .client()
        .search("archive", "title:archived", Some(100), None, None, None)
        .await
        .expect("content search after the second restart");
    assert_eq!(
        hits["hits"].as_array().map(|h| h.len()).unwrap_or(0),
        15,
        "the documents should survive a second restart on their own committed segments"
    );
}

/// The recovered tail and writes that arrive after the restart both end up searchable.
///
/// Recovery posts its commits onto the writer channel before the writer thread starts
/// draining it, so this is the case where they interleave with live traffic.
#[tokio::test]
async fn writes_after_a_crash_join_the_recovered_tail() {
    let mut node = Node::start().await;

    {
        let client = node.client();
        for i in 0..10 {
            let id = format!("before-{i}");
            client
                .write_document(
                    "archive",
                    &id,
                    &json!({"id": id, "title": format!("archived record {i}")}),
                    None,
                )
                .await
                .expect("write before the crash");
        }
    }

    node.kill_hard().await;
    node.spawn().await;

    // Write immediately, racing the post-recovery commit.
    let client = node.client();
    for i in 0..10 {
        let id = format!("after-{i}");
        client
            .write_document(
                "archive",
                &id,
                &json!({"id": id, "title": format!("archived record {i}")}),
                None,
            )
            .await
            .expect("write after the restart");
    }
    client
        .admin_index_commit("archive")
        .await
        .expect("commit the new writes");
    tokio::time::sleep(Duration::from_millis(500)).await;

    let hits = client
        .search("archive", "title:archived", Some(100), None, None, None)
        .await
        .expect("content search");
    assert_eq!(
        hits["hits"].as_array().map(|h| h.len()).unwrap_or(0),
        20,
        "both the recovered tail and the writes that followed it should be searchable"
    );
}
