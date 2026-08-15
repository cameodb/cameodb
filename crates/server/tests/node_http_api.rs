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

/// `PATCH /api/{index}/_schema` on an index that has been written to.
///
/// This is the case that answered `500` for every index that had ever seen a write: the update
/// was applied by replaying `CreateConfig`, which re-creates the Tantivy index and could not take
/// a lock the open writer still held. A node with two shards, one index and one document is the
/// whole reproduction.
#[tokio::test]
async fn a_schema_flag_can_be_changed_on_an_index_that_is_open() {
    let node = TestNode::start("").await;
    let client = node.client();

    client
        .write_document(
            "papers",
            "p1",
            &json!({"id": "p1", "title": "On Lockfiles", "author": "hoare"}),
            None,
        )
        .await
        .expect("write");

    let response = patch_schema(&node, "papers", &json!({"title": false})).await;

    assert_eq!(
        response.0, 200,
        "patching an open index should succeed, got {}: {}",
        response.0, response.1
    );
    assert_eq!(response.1["acknowledged"], json!(true));
    assert_eq!(response.1["updated_fields"], json!(["title"]));
}

/// Marking a late-discovered field indexed is applied and flagged, not refused.
///
/// The Tantivy schema was fixed when the index was built, so the field has no column yet and the
/// flag takes effect at the next rebuild. That makes the edit the first step of
/// declare-then-reingest rather than a mistake — so the endpoint saves it and says what remains
/// to be done, instead of blocking the only route to a searchable field.
#[tokio::test]
async fn promoting_a_late_discovered_field_is_applied_and_flagged() {
    let node = TestNode::start("").await;
    let client = node.client();

    // Creates the index, and with it the Tantivy schema: `title` and nothing else.
    client
        .write_document(
            "papers",
            "p1",
            &json!({"id": "p1", "title": "On Lockfiles"}),
            None,
        )
        .await
        .expect("first write");

    // `author` arrives afterwards, so it is recorded non-indexed.
    client
        .write_document(
            "papers",
            "p2",
            &json!({"id": "p2", "title": "On Lockfiles II", "author": "hoare"}),
            None,
        )
        .await
        .expect("second write");

    let (status, body) = patch_schema(&node, "papers", &json!({"author": true})).await;

    assert_eq!(status, 200, "the declaration should be accepted: {body}");
    assert_eq!(body["acknowledged"], json!(true));
    assert_eq!(body["updated_fields"], json!(["author"]));
    assert_eq!(
        body["pending_reindex_fields"],
        json!(["author"]),
        "and it should say the field is not searchable yet: {body}"
    );

    let note = body["note"].as_str().unwrap_or_default();
    assert!(
        note.contains("re-ingest"),
        "the note should say what makes it searchable, got: {note}"
    );
}

/// Changing a flag must not disturb anything else the schema carries. The endpoint used to read
/// the schema out through the `GetConfig` response and write it back, so every property that
/// response omits — `routing_field_name` among them, which decides which shard a document lands
/// on — was reset by an unrelated edit.
#[tokio::test]
async fn changing_a_flag_leaves_the_rest_of_the_schema_alone() {
    let node = TestNode::start("").await;
    let client = node.client();

    client
        .write_document(
            "papers",
            "p1",
            &json!({"id": "p1", "title": "On Lockfiles", "summary": "a note"}),
            None,
        )
        .await
        .expect("write");

    let before = client
        .get_index_config("papers")
        .await
        .expect("config before");

    let (status, _) = patch_schema(&node, "papers", &json!({"title": false})).await;
    assert_eq!(status, 200);

    let after = client
        .get_index_config("papers")
        .await
        .expect("config after");

    assert_eq!(
        before.fields.len(),
        after.fields.len(),
        "the field set should be unchanged by a flag edit"
    );

    // The document still routes and reads back, which is the observable half of the routing
    // field surviving the edit.
    let hits = client
        .search("papers", "id:p1", Some(10), None, None)
        .await
        .expect("search");
    assert_eq!(
        hits["hits"].as_array().map(|h| h.len()),
        Some(1),
        "the document should still be reachable after a schema edit"
    );
}

/// An empty request is a client error, not a no-op that reports success.
#[tokio::test]
async fn an_empty_schema_patch_is_refused() {
    let node = TestNode::start("").await;
    node.client()
        .write_document("papers", "p1", &json!({"id": "p1", "title": "t"}), None)
        .await
        .expect("write");

    let (status, _) = patch_schema(&node, "papers", &json!({})).await;
    assert_eq!(status, 400, "an empty patch should be a 400");
}

/// `PATCH /api/{index}/_schema` over raw HTTP, returning the status and decoded body.
///
/// The SDK has no method for this endpoint, and half of what these tests assert is the status
/// code rather than the payload, so they speak HTTP directly.
async fn patch_schema(
    node: &TestNode,
    index: &str,
    field_updates: &serde_json::Value,
) -> (u16, serde_json::Value) {
    with_tls_provider();
    let response = reqwest::Client::new()
        .patch(format!("{}/api/{index}/_schema", node.url))
        .json(&json!({ "field_updates": field_updates }))
        .send()
        .await
        .expect("patch request");

    let status = response.status().as_u16();
    let body = response.json().await.unwrap_or(serde_json::Value::Null);
    (status, body)
}

/// The listing describes each index in full: identity, statistics and every field with its type.
///
/// Before this, `/_indexes` gave field *names* only, so every caller — the bundled client, the
/// MCP tools — fetched `/api/{index}/_config` again per index just to learn the types. That is
/// what made the listing cost 1 + N requests instead of one.
#[tokio::test]
async fn the_listing_describes_every_field_without_a_second_request() {
    let node = TestNode::start("").await;
    let client = node.client();

    client
        .write_document(
            "papers",
            "p1",
            &json!({"id": "p1", "title": "On Shapes", "year": 2020}),
            None,
        )
        .await
        .expect("write");

    // Raw HTTP rather than the SDK: the typed `IndexInfo` does not model the new keys, so
    // deserializing through it would silently drop exactly what this asserts.
    let raw = get_json(&node, "/_indexes").await;
    let entry = raw["indexes"]
        .as_array()
        .and_then(|a| a.iter().find(|e| e["name"] == "papers"))
        .expect("papers in the listing");

    let fields = entry["fields"].as_array().expect("fields array");
    assert!(
        !fields.is_empty(),
        "the listing should carry fields: {entry}"
    );
    assert_eq!(
        entry["field_count"].as_u64(),
        Some(fields.len() as u64),
        "field_count should match the array it counts"
    );

    let title = fields
        .iter()
        .find(|f| f["name"] == "title")
        .expect("title described");

    // The one-word type an agent keys on, lowercase and matching the syntax reference.
    assert_eq!(title["type"], json!("text"));
    assert_eq!(title["indexed"], json!(true));
    assert_eq!(
        title["searchable"],
        json!(true),
        "declared and built, so a query reaches it: {title}"
    );

    // Every field carries the same keys — no property is present on one and absent on another.
    for field in fields {
        for key in [
            "name",
            "type",
            "indexed",
            "stored",
            "fast",
            "shadow",
            "searchable",
        ] {
            assert!(
                field.get(key).is_some(),
                "every field needs `{key}`, missing on {field}"
            );
        }
    }

    assert!(
        fields.iter().all(|f| f["name"] != "_seq"),
        "`_seq` is WAL bookkeeping and must not be offered as a queryable field: {fields:?}"
    );
}

/// `/api/{index}/_config` describes a field exactly as the listing does.
///
/// The two used to disagree on every property name — the schema keyed fields by map key with
/// `field_type` and `is_shadow`, the listing offered names only — so a caller reading both had to
/// translate between them.
#[tokio::test]
async fn the_schema_endpoint_and_the_listing_describe_a_field_identically() {
    let node = TestNode::start("").await;
    let client = node.client();

    client
        .write_document(
            "papers",
            "p1",
            &json!({"id": "p1", "title": "On Shapes"}),
            None,
        )
        .await
        .expect("write");

    let listing = get_json(&node, "/_indexes").await;
    let from_listing = listing["indexes"]
        .as_array()
        .and_then(|a| a.iter().find(|e| e["name"] == "papers"))
        .and_then(|e| e["fields"].as_array())
        .and_then(|f| f.iter().find(|f| f["name"] == "title"))
        .cloned()
        .expect("title from listing");

    let config = get_json(&node, "/api/papers/_config").await;

    let from_config = config["fields"]
        .as_array()
        .and_then(|f| f.iter().find(|f| f["name"] == "title"))
        .cloned()
        .expect("title from config");

    assert_eq!(
        from_listing, from_config,
        "one index, one description of its fields"
    );
}

/// A GET against the node, decoded as raw JSON.
async fn get_json(node: &TestNode, path: &str) -> serde_json::Value {
    with_tls_provider();
    reqwest::Client::new()
        .get(format!("{}{path}", node.url))
        .send()
        .await
        .expect("request")
        .json()
        .await
        .expect("json")
}
