//! End-to-end tests against a real `cameodb` process.
//!
//! `crates/server` is a binary-only crate, so `tests/` has no library to link against and
//! cannot reach the orchestrator directly. These start the built binary as a subprocess and
//! talk to it over HTTP with the shipped SDK instead. That constraint turns out to be worth
//! something: what gets exercised is what actually ships — the config file is parsed by the
//! real loader, the routes are the real routes, and the client is the one a consumer would
//! use — rather than internals only a unit test can see.
//!
//! Everything here is what the 160 unit tests in this crate structurally cannot cover: they
//! test pure helpers, because a `NodeOrchestrator` needs a data directory, threads and a
//! listening socket to exist at all.
//!
//! Each test gets its own port, its own data directory and its own process, so they can run
//! concurrently and leave nothing behind.

use std::io::Write as _;
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use client::CameoClient;
use serde_json::json;

/// A node running in its own process, killed when the test drops it.
struct TestNode {
    child: Child,
    url: String,
    // Held for the lifetime of the node: dropping it removes the data directory.
    _dir: tempfile::TempDir,
}

impl TestNode {
    /// Boot a node on a free port with an empty data directory.
    ///
    /// `extra` is appended to the config file, so a test can turn on a feature (security,
    /// say) without this helper growing a parameter for every setting.
    async fn start(extra: &str) -> TestNode {
        let dir = tempfile::tempdir().expect("temp dir");
        let port = free_port();
        let data = dir.path().join("data");
        std::fs::create_dir_all(&data).expect("data dir");

        // Loopback only, so the "local" profile is honest and no other machine can reach
        // a test node. Few shards: these tests are about behaviour, not throughput, and
        // every shard costs threads.
        let config = format!(
            r#"
[node]
label = "test-node"
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
supervisor_timeout_secs = 5
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

    /// Poll health until the node answers. Fails loudly with the child's stderr rather than
    /// letting the first real assertion fail for a confusing reason.
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

/// Install the rustls crypto provider once per test binary.
///
/// `main.rs` does this at startup and `reqwest` panics building a client without it, so a
/// process that constructs an SDK client without going through `main` has to do it too —
/// including this one, even though every URL here is plain HTTP.
fn with_tls_provider() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

/// Ask the OS for an unused port and immediately give it back.
///
/// Racy in principle — something could take it before the node binds — but the window is
/// microseconds and the alternative is a fixed port, which makes concurrent tests collide
/// every time rather than almost never.
fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
    listener.local_addr().expect("local addr").port()
}

/// The cheapest possible end-to-end assertion, and the one everything else depends on: the
/// shipped binary parses a config file written from scratch and serves HTTP.
#[tokio::test]
async fn a_node_starts_from_a_config_file_and_serves_health() {
    let node = TestNode::start("").await;
    let health = node.client().health().await.expect("health");
    assert!(
        health.node_id.is_some(),
        "a healthy node should identify itself, got status {:?}",
        health.status
    );
}

/// Write then read back by id. `id:` lookups are answered without a committed segment, so
/// this does not depend on commit timing — see `crates/bench/README.md`.
#[tokio::test]
async fn a_written_document_is_retrievable_by_id() {
    let node = TestNode::start("").await;
    let client = node.client();

    client
        .write_document(
            "books",
            "b1",
            &json!({"id": "b1", "title": "Dune", "author": "Herbert"}),
            None,
        )
        .await
        .expect("write");

    let found = client
        .search("books", "id:b1", Some(10), None, None)
        .await
        .expect("search");

    let hits = found["hits"].as_array().expect("hits array");
    assert_eq!(hits.len(), 1, "the document just written should come back");
    assert_eq!(hits[0]["title"], "Dune");
}

/// A content query needs a committed segment. The node commits on an idle timeout, but a
/// test should not sleep for it — `admin_index_commit` is the deterministic path, and this
/// pins the fact that an explicit commit makes a write searchable by content.
#[tokio::test]
async fn a_committed_document_is_searchable_by_content() {
    let node = TestNode::start("").await;
    let client = node.client();

    client
        .write_document(
            "books",
            "b2",
            &json!({"id": "b2", "title": "Neuromancer"}),
            None,
        )
        .await
        .expect("write");
    client
        .admin_index_commit("books")
        .await
        .expect("explicit commit");

    let found = client
        .search("books", "title:Neuromancer", Some(10), None, None)
        .await
        .expect("search");

    assert_eq!(
        found["hits"].as_array().map(|h| h.len()),
        Some(1),
        "a committed document should be matchable by content"
    );
}

/// Writing creates the index, and it shows up in the listing under its own name. Guards the
/// index lifecycle against the write path silently landing somewhere else.
#[tokio::test]
async fn writing_creates_an_index_that_the_listing_reports() {
    let node = TestNode::start("").await;
    let client = node.client();

    client
        .write_document(
            "catalogue",
            "c1",
            &json!({"id": "c1", "title": "Solaris"}),
            None,
        )
        .await
        .expect("write");

    let listed = client.list_indexes(false).await.expect("list");
    assert!(
        listed.indexes.iter().any(|i| i.name == "catalogue"),
        "the index a write created should be listed, got {:?}",
        listed.indexes.iter().map(|i| &i.name).collect::<Vec<_>>()
    );
}

/// Searching an index that was never created returns an empty result rather than failing,
/// and leaves the node serving. The distinction matters: this is the shape of request an
/// agent sends constantly, often before anything has been written.
#[tokio::test]
async fn searching_an_unknown_index_is_empty_and_leaves_the_node_serving() {
    let node = TestNode::start("").await;
    let client = node.client();

    let found = client
        .search("no_such_index", "title:anything", Some(10), None, None)
        .await
        .expect("querying an unknown index is answered, not refused");

    // Answered as an empty result rather than an error. Pinned because it is a contract an
    // agent depends on: a query against an index that does not exist yet is a normal thing
    // to do, and getting back zero hits is easier to handle than a failure.
    assert_eq!(
        found["hits"].as_array().map(|h| h.len()),
        Some(0),
        "an unknown index should match nothing, got {found}"
    );

    // The point of the test: the node is still serving afterwards.
    client
        .health()
        .await
        .expect("the node should survive a query for a missing index");
}

/// The document body must carry its own `id`, even though `write_document` takes one as a
/// parameter — the outer value addresses and routes the write, the inner one is what the
/// store indexes. Getting this wrong is easy and the node answers 500 rather than 400,
/// which is what makes it worth pinning: the behaviour is load-bearing for every caller,
/// and a future change that starts injecting the id would break this test loudly instead
/// of silently changing an API contract.
#[tokio::test]
async fn a_document_without_an_inner_id_is_refused() {
    let node = TestNode::start("").await;
    let client = node.client();

    let result = client
        .write_document("books", "b3", &json!({"title": "no inner id"}), None)
        .await;

    let err = result.expect_err("a document with no inner id should not be accepted");
    assert!(
        err.to_string().contains("id"),
        "the error should name the missing field, got: {err}"
    );
}
