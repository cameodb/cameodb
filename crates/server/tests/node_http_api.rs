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
        .search("books", "id:b1", Some(10), None, None, None)
        .await
        .expect("search");

    let hits = found["hits"].as_array().expect("hits array");
    assert_eq!(hits.len(), 1, "the document just written should come back");
    assert_eq!(hits[0]["title"], "Dune");
}

/// A deleted document is gone from the key-value path at once, and from the index at the commit.
///
/// Both halves matter and they are not the same moment. An `id:VALUE` query is answered from redb
/// without consulting Tantivy, so the row's removal is immediately visible; a content query is
/// answered by Tantivy, where `delete_term` only takes effect when a commit publishes it. The
/// second assertion is what the read cache defect would have broken — the cached body outlived
/// the row and the document kept coming back.
#[tokio::test]
async fn a_deleted_document_is_gone_by_id_at_once_and_by_content_at_the_commit() {
    let node = TestNode::start("").await;
    let client = node.client();

    client
        .write_document("books", "b1", &json!({"id": "b1", "title": "Dune"}), None)
        .await
        .expect("write");
    client.admin_index_commit("books").await.expect("commit");

    // Read it first, which is what puts the body in the cache that used to outlive it.
    let found = client
        .search("books", "id:b1", Some(10), None, None, None)
        .await
        .expect("search");
    assert_eq!(found["hits"].as_array().map(|h| h.len()), Some(1));

    let removed = client
        .delete_document("books", "b1", None)
        .await
        .expect("delete");
    assert_eq!(removed["result"], "deleted");
    assert_eq!(removed["id"], "b1");
    assert!(
        removed["version"].as_u64().unwrap_or(0) > 0,
        "a delete takes a sequence number like any other write: {removed}"
    );

    let by_id = client
        .search("books", "id:b1", Some(10), None, None, None)
        .await
        .expect("search by id");
    assert_eq!(
        by_id["hits"].as_array().map(|h| h.len()),
        Some(0),
        "the row is gone from redb, so the key lookup must miss immediately: {by_id}"
    );

    client
        .admin_index_commit("books")
        .await
        .expect("commit the delete");
    let by_content = client
        .search("books", "title:Dune", Some(10), None, None, None)
        .await
        .expect("search by content");
    assert_eq!(
        by_content["total_hits"].as_u64(),
        Some(0),
        "once committed, the document is out of the index too: {by_content}"
    );
}

/// Deleting is idempotent, and deleting from an index that does not exist is a 404.
///
/// The asymmetry is deliberate. An id the index does not hold is answered as deleted, exactly as
/// writing over an existing document is answered as created — reporting per-record existence
/// would mean threading an outcome back through the writer thread's reply splitting. A missing
/// *index* is different: it is a name that was never created, and answering "deleted" would hide
/// a typo. It must also not bring the index into existence on its way to answering.
#[tokio::test]
async fn deleting_is_idempotent_and_an_unknown_index_is_refused() {
    let node = TestNode::start("").await;
    let client = node.client();

    client
        .write_document("books", "b1", &json!({"id": "b1", "title": "Dune"}), None)
        .await
        .expect("write");

    for round in 1..=2 {
        let removed = client
            .delete_document("books", "b1", None)
            .await
            .unwrap_or_else(|e| panic!("delete round {round} failed: {e}"));
        assert_eq!(removed["result"], "deleted", "round {round}");
    }
    let absent = client
        .delete_document("books", "never-written", None)
        .await
        .expect("deleting an id the index does not hold is not an error");
    assert_eq!(absent["result"], "deleted");

    let refused = client
        .delete_document("no-such-index", "b1", None)
        .await
        .expect_err("an unknown index must be refused");
    assert!(
        refused.to_string().contains("404"),
        "an unknown index is a 404, not a success or a 500: {refused}"
    );

    // ...and the refusal created nothing: the listing still knows only `books`.
    let listing = client.list_indexes(false).await.expect("listing");
    let names: Vec<&str> = listing
        .indexes
        .iter()
        .map(|index| index.name.as_str())
        .collect();
    assert_eq!(
        names,
        vec!["books"],
        "a refused delete must not create the index it names"
    );
}

/// A bulk delete removes what it names, takes both entry shapes, and reports what it could not do.
///
/// The per-id error rather than a failed batch is the property worth pinning: a batch may span
/// tenants, so one id that cannot be routed says nothing about the rest of them.
#[tokio::test]
async fn a_bulk_delete_removes_many_and_reports_what_it_could_not() {
    let node = TestNode::start("").await;
    let client = node.client();

    let batch: Vec<serde_json::Value> = (1..=4)
        .map(|n| json!({"id": format!("b{n}"), "doc": {"id": format!("b{n}"), "title": "Dune"}}))
        .collect();
    client.bulk_index("books", &batch).await.expect("seed");
    client.admin_index_commit("books").await.expect("commit");

    // Both entry shapes in one call: a bare id, and one that spells out its routing key.
    let removed = client
        .delete_documents(
            "books",
            &[
                json!("b1"),
                json!({"id": "b2"}),
                json!({"id": "b3", "routing_key": "b3"}),
                json!("never-written"),
            ],
        )
        .await
        .expect("bulk delete");

    assert_eq!(removed["items_received"], 4);
    assert_eq!(
        removed["items_deleted"], 4,
        "every id is applied, including one the index does not hold: {removed}"
    );
    assert_eq!(
        removed["errors"].as_array().map(|e| e.len()),
        Some(0),
        "nothing here is unroutable: {removed}"
    );

    for id in ["b1", "b2", "b3"] {
        let found = client
            .search("books", &format!("id:{id}"), Some(10), None, None, None)
            .await
            .expect("search");
        assert_eq!(
            found["hits"].as_array().map(|h| h.len()),
            Some(0),
            "{id} should be gone: {found}"
        );
    }

    // The one that was not named is untouched.
    let survivor = client
        .search("books", "id:b4", Some(10), None, None, None)
        .await
        .expect("search");
    assert_eq!(
        survivor["hits"].as_array().map(|h| h.len()),
        Some(1),
        "a document the batch did not name must survive it: {survivor}"
    );

    // An empty body is a bad request rather than a no-op success.
    let refused = client
        .delete_documents("books", &[])
        .await
        .expect_err("an empty batch is refused");
    assert!(
        refused.to_string().contains("400"),
        "an empty batch is a 400: {refused}"
    );
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
        .search("books", "title:Neuromancer", Some(10), None, None, None)
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
        .search(
            "no_such_index",
            "title:anything",
            Some(10),
            None,
            None,
            None,
        )
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

/// A body does not have to repeat the id it was written under, and may not contradict it.
///
/// The key travels beside the body — `{"id": ..., "doc": {...}}`, the shape the API reference
/// documents and the shape every bulk client sends — and that is the copy the store keeps: the
/// redb key, the term tantivy indexes, and the value reconstruction writes back into every hit
/// all come from it, which is why the stored blob deliberately holds no second copy. Validation
/// demanded `id` inside `doc` anyway, so a caller following the documentation had every document
/// refused: a 400 on a single write, and — once a bulk write stopped storing what failed
/// validation — `items_written: 0` with one error per row on a bulk one.
///
/// What is refused instead is a body whose `id` disagrees with the key it arrived under. That
/// document cannot be stored as written: the blob keeps its own `id` and reconstruction prefers
/// it, so the document would answer to one identifier and report another to whoever found it.
#[tokio::test]
async fn a_body_need_not_repeat_its_id_but_may_not_contradict_it() {
    let node = TestNode::start("").await;
    let client = node.client();

    // A single write in the documented shape: the id beside the body, not in it.
    client
        .write_document("books", "b3", &json!({"title": "no inner id"}), None)
        .await
        .expect("a document whose key travels beside it should be accepted");

    // And a bulk write of the same shape.
    let bulk = client
        .bulk_index(
            "books",
            &[json!({"id": "b4", "doc": {"title": "bulk, no inner id"}})],
        )
        .await
        .expect("bulk write");
    assert_eq!(
        (
            bulk["items_written"].as_u64(),
            bulk["errors"].as_array().map(|e| e.len())
        ),
        (Some(1), Some(0)),
        "a bulk write must accept the shape its own documentation shows: {bulk}"
    );

    // Both are stored under the key they were written with, and read back carrying it.
    for (id, title) in [("b3", "no inner id"), ("b4", "bulk, no inner id")] {
        let found = client
            .search("books", &format!("id:{id}"), Some(10), None, None, None)
            .await
            .expect("search");
        assert_eq!(
            found["total_hits"].as_u64(),
            Some(1),
            "{id} should be retrievable by the key it was written under: {found}"
        );
        let hit = &found["hits"][0];
        assert_eq!(
            (hit["id"].as_str(), hit["title"].as_str()),
            (Some(id), Some(title)),
            "the hit carries the key supplied from the envelope: {hit}"
        );
    }

    // A body that names a different identifier is refused, and the refusal names both.
    let refused = client
        .write_document(
            "books",
            "b5",
            &json!({"id": "b6", "title": "two identities"}),
            None,
        )
        .await;
    let err = refused.expect_err("a body contradicting its key should not be accepted");
    let message = err.to_string();
    assert!(
        message.contains("b5") && message.contains("b6"),
        "the refusal has to name both identifiers, or the caller cannot tell which was wrong: \
         {message}"
    );
    // A malformed document is the caller's fault. It used to answer 500 Internal server error,
    // which is both wrong about whose fault it is and an instruction to retry a request that
    // cannot succeed.
    assert!(
        message.contains("400"),
        "a refused document should be a 400, got: {message}"
    );

    // The same document through the bulk path, which validates elsewhere.
    let bulk = client
        .bulk_index(
            "books",
            &[json!({"id": "b7", "doc": {"id": "b8", "title": "two identities"}})],
        )
        .await
        .expect("a bulk write reports per-document outcomes rather than failing");
    assert_eq!(
        bulk["items_written"].as_u64(),
        Some(0),
        "a bulk write must be held to the same rule as a single one: {bulk}"
    );
    assert!(
        bulk["errors"].to_string().contains("b8"),
        "and must say why the document it received was not written: {bulk}"
    );
}

/// A batch past the fast validator's size boundary keeps the same contract.
///
/// Two validators judge a written document — an inline one for small batches and mature
/// schemas, and a slower one that also reports fields the schema has yet to learn — and which
/// one runs is decided by the batch size alone, at 1000 documents. Both refused a body without
/// `id`, but only the slow one was reachable by a loader sending batches of twelve hundred, so
/// the break showed up as "every document rejected" on a payload no smaller test sent.
///
/// The payload is the shape that found it: a batch across that boundary, the key beside each
/// body rather than in it, and a shadow field carrying that key under the source's own name.
#[tokio::test]
async fn a_batch_past_the_fast_validator_boundary_needs_no_id_in_its_bodies() {
    let node = TestNode::start("").await;
    let client = node.client();

    let (status, body) = put_config(
        &node,
        "files",
        &json!({
            "fields": {
                "id": {"name": "id", "field_type": "text", "indexed": true},
                "sha256": {"name": "sha256", "field_type": "text", "indexed": false, "is_shadow": true},
                "title": {"name": "title", "field_type": "text", "indexed": true}
            }
        }),
    )
    .await;
    assert_eq!(status, 200, "creating the config should succeed: {body}");

    const DOCS: usize = 1200;
    let key = |n: usize| format!("f{n:04}");
    let batch: Vec<serde_json::Value> = (0..DOCS)
        .map(|n| json!({"id": key(n), "doc": {"sha256": key(n), "title": "a record"}}))
        .collect();

    let bulk = client
        .bulk_index("files", &batch)
        .await
        .expect("bulk write");
    assert_eq!(
        (
            bulk["items_written"].as_u64(),
            bulk["errors"].as_array().map(|e| e.len())
        ),
        (Some(DOCS as u64), Some(0)),
        "every document should have been written: {bulk}"
    );

    client.admin_index_commit("files").await.expect("commit");
    let found = client
        .search("files", "title:record", Some(1), None, None, None)
        .await
        .expect("search");
    assert_eq!(
        found["total_hits"].as_u64(),
        Some(DOCS as u64),
        "and be searchable by content afterwards: {found}"
    );

    // And each one is reachable under the name its source uses for the key.
    let last = key(DOCS - 1);
    let found = client
        .search(
            "files",
            &format!("sha256:{last}"),
            Some(1),
            None,
            None,
            None,
        )
        .await
        .expect("search");
    assert_eq!(
        found["hits"][0]["sha256"].as_str(),
        Some(last.as_str()),
        "the last document of the batch should be there under its shadow name: {found}"
    );
}

/// One document, one verdict — whatever size batch it arrives in.
///
/// Two validators used to judge a written document, and which one ran was decided by the batch
/// size alone: a "fast" one for single writes and anything under a thousand documents, a slower
/// one above that. They did not agree. The fast one required a value's type to equal the
/// declared type exactly, while the slow one asked the question that matters — can the engine
/// store this? A text field takes any value, serializing what is not already a string, so a
/// number under a text field was refused below a thousand documents and written above it.
///
/// For a size-driven loader that is not a corner case: batch size follows traffic, so the same
/// record is accepted at midday and rejected at midnight. Both directions are pinned here,
/// because unifying on the strict rule would have been just as wrong: a document the engine can
/// hold must be accepted at every size, and one it cannot must be refused at every size.
#[tokio::test]
async fn a_document_is_judged_the_same_whatever_size_batch_it_arrives_in() {
    let node = TestNode::start("").await;
    let client = node.client();

    let (status, body) = put_config(
        &node,
        "sized",
        &json!({
            "fields": {
                "id": {"name": "id", "field_type": "text", "indexed": true},
                "title": {"name": "title", "field_type": "text", "indexed": true},
                "n": {"name": "n", "field_type": "i64", "indexed": true, "fast": true}
            }
        }),
    )
    .await;
    assert_eq!(status, 200, "creating the config should succeed: {body}");

    // Storable: a text field serializes whatever it is given and indexes that.
    let storable = |id: String| json!({"id": id, "doc": {"title": 42}});
    // Not storable: an i64 field has nowhere to put a word, so the write path would skip the
    // value and leave the document with the field silently unindexed.
    let refused = |id: String| json!({"id": id, "doc": {"n": "twelve"}});

    for (label, count) in [("one", 1usize), ("many", 1200)] {
        let batch: Vec<serde_json::Value> = (0..count)
            .map(|n| storable(format!("ok-{label}-{n}")))
            .collect();
        let bulk = client
            .bulk_index("sized", &batch)
            .await
            .expect("bulk write");
        assert_eq!(
            (
                bulk["items_written"].as_u64(),
                bulk["errors"].as_array().map(|e| e.len())
            ),
            (Some(count as u64), Some(0)),
            "a storable document must be written in a batch of {count}: {bulk}"
        );

        let batch: Vec<serde_json::Value> = (0..count)
            .map(|n| refused(format!("bad-{label}-{n}")))
            .collect();
        let bulk = client
            .bulk_index("sized", &batch)
            .await
            .expect("bulk write");
        assert_eq!(
            (
                bulk["items_written"].as_u64(),
                bulk["errors"].as_array().map(|e| e.len())
            ),
            (Some(0), Some(count)),
            "an unstorable document must be refused in a batch of {count}: {bulk}"
        );
        assert!(
            bulk["errors"][0].as_str().unwrap_or_default().contains("n"),
            "and the reason must name the field: {bulk}"
        );
    }

    // The single-write path is the third caller, and it validated as the small batches did.
    client
        .write_document("sized", "ok-single", &json!({"title": 42}), None)
        .await
        .expect("a storable document should be accepted as a single write too");
    let err = client
        .write_document("sized", "bad-single", &json!({"n": "twelve"}), None)
        .await
        .expect_err("an unstorable document should be refused as a single write too");
    assert!(
        err.to_string().contains("400") && err.to_string().contains('n'),
        "as a bad request naming the field: {err}"
    );

    // Storable means what it says: the value is in the index, not merely accepted.
    client.admin_index_commit("sized").await.expect("commit");
    let found = client
        .search("sized", "title:42", Some(1), None, None, None)
        .await
        .expect("search");
    assert_eq!(
        found["total_hits"].as_u64(),
        Some(1202),
        "every accepted document should be searchable by the value it carried: {found}"
    );
}

/// A list is several values of the field it arrives under, not a value the field cannot hold.
///
/// Every tantivy field is multivalued. Two `add_i64` calls under one field store two values of
/// it, the fast column reports `Cardinality::Multivalued`, and a range or term query matches the
/// document if any one of its values matches — verified against tantivy 0.26 directly in
/// `crates/storage/tests/tantivy_multivalue.rs`. So a source reporting two analyses of one
/// sample belongs in the numeric field the schema declares, and both numbers belong in the
/// index.
///
/// Reading such a value with `as_i64` returned nothing and skipped the field. The write
/// succeeded, the list was kept in the stored document, and the column was left empty — so the
/// value read back correctly while no range query over it ever matched, on every document that
/// carried more than one value. Validation then hardened the same mistake into a refusal.
///
/// Null and the empty list are pinned alongside, because they are the same question: both are
/// the absence of a value, both index nothing, and neither is a reason to refuse a document
/// that could have omitted the key to identical effect.
#[tokio::test]
async fn a_list_is_several_values_of_the_field_it_arrives_under() {
    let node = TestNode::start("").await;
    let client = node.client();

    let (status, body) = put_config(
        &node,
        "multi",
        &json!({
            "fields": {
                "id": {"name": "id", "field_type": "text", "indexed": true},
                "n": {"name": "n", "field_type": "i64", "indexed": true, "fast": true},
                "d": {"name": "d", "field_type": "date", "indexed": true, "fast": true},
                "b": {"name": "b", "field_type": "boolean", "indexed": true},
                "title": {"name": "title", "field_type": "text", "indexed": true}
            }
        }),
    )
    .await;
    assert_eq!(status, 200, "creating the config should succeed: {body}");

    let bulk = client
        .bulk_index(
            "multi",
            &[
                json!({"id": "m1", "doc": {
                    "title": "two analyses",
                    "n": [9, 12],
                    "d": ["2023-08-19T10:24:56Z", "2023-08-19T10:26:30Z"],
                    "b": [false, true]
                }}),
                // The absences: an explicit null, and a list with nothing in it.
                json!({"id": "m2", "doc": {"title": "nothing to index", "n": null, "d": null}}),
                json!({"id": "m3", "doc": {"title": "nothing to index", "n": [], "b": []}}),
            ],
        )
        .await
        .expect("bulk write");
    assert_eq!(
        (
            bulk["items_written"].as_u64(),
            bulk["errors"].as_array().map(|e| e.len())
        ),
        (Some(3), Some(0)),
        "every one of the three should be written: {bulk}"
    );

    client.admin_index_commit("multi").await.expect("commit");

    // Either value of the multivalued document answers, exactly and by range.
    for query in [
        "n:9",
        "n:12",
        "n:[10 TO 20]",
        "n:[0 TO 9]",
        "b:true",
        "b:false",
        "d:[2023-08-19T10:24:00Z TO 2023-08-19T10:25:00Z]",
        "d:[2023-08-19T10:26:00Z TO 2023-08-19T10:27:00Z]",
    ] {
        let found = client
            .search("multi", query, Some(10), None, None, None)
            .await
            .expect("search");
        assert_eq!(
            found["total_hits"].as_u64(),
            Some(1),
            "`{query}` should match the document carrying that value: {found}"
        );
        assert_eq!(
            found["hits"][0]["id"].as_str(),
            Some("m1"),
            "and it should be the multivalued one: {found}"
        );
    }

    // The stored document is unchanged by any of this: the lists read back as written.
    let found = client
        .search("multi", "id:m1", Some(1), None, None, None)
        .await
        .expect("search");
    assert_eq!(
        (
            found["hits"][0]["n"].clone(),
            found["hits"][0]["d"].clone(),
            found["hits"][0]["b"].clone()
        ),
        (
            json!([9, 12]),
            json!(["2023-08-19T10:24:56Z", "2023-08-19T10:26:30Z"]),
            json!([false, true])
        ),
        "the document reads back exactly as written: {found}"
    );

    // An absence indexes nothing, which is the whole of what it should do.
    let found = client
        .search("multi", "title:nothing", Some(10), None, None, None)
        .await
        .expect("search");
    assert_eq!(
        found["total_hits"].as_u64(),
        Some(2),
        "both documents whose fields were absent are stored and searchable: {found}"
    );
    let found = client
        .search("multi", "n:[-1000 TO 1000] limit 0", None, None, None, None)
        .await
        .expect("search");
    assert_eq!(
        found["total_hits"].as_u64(),
        Some(1),
        "and neither is in the numeric column: {found}"
    );
}

/// A field the schema has never seen reaches it from a small write, not only a large one.
///
/// The other half of the split: the fast validator never reported an unknown field, so
/// `needs_evolution` was permanently false on the path whose caller reads it to decide whether
/// the schema has to grow. A batch of twelve hundred taught the index a new field; the same
/// document written singly, or in a batch of two, did not — the value was stored in the document
/// body and the field stayed absent from every description of the index.
///
/// A field discovered after the index exists is deliberately not searchable: tantivy fixes its
/// schema at creation, so there is no column to write into. It is recorded so that the two views
/// of a document agree and so `PATCH /_schema` has something to promote.
#[tokio::test]
async fn a_field_the_schema_never_saw_is_recorded_from_a_small_write() {
    let node = TestNode::start("").await;
    let client = node.client();

    // Created explicitly, so nothing here is initial-creation sampling.
    let (status, body) = put_config(
        &node,
        "growing",
        &json!({
            "fields": {
                "id": {"name": "id", "field_type": "text", "indexed": true},
                "title": {"name": "title", "field_type": "text", "indexed": true}
            }
        }),
    )
    .await;
    assert_eq!(status, 200, "creating the config should succeed: {body}");

    client
        .write_document(
            "growing",
            "g1",
            &json!({"title": "single", "from_single_write": "v"}),
            None,
        )
        .await
        .expect("write");

    client
        .bulk_index(
            "growing",
            &[json!({"id": "g2", "doc": {"title": "bulk", "from_small_batch": "v"}})],
        )
        .await
        .expect("bulk write");

    let config = get_json(&node, "/api/growing/_config").await;
    for name in ["from_single_write", "from_small_batch"] {
        let field = config["fields"]
            .as_array()
            .and_then(|f| f.iter().find(|f| f["name"] == name))
            .unwrap_or_else(|| panic!("the schema should have learned {name}: {config}"));
        assert_eq!(
            (field["type"].as_str(), field["indexed"].as_bool()),
            (Some("text"), Some(false)),
            "a late-discovered field is recorded, and not searchable until promoted: {field}"
        );
    }
}

/// A document whose value the declared type cannot hold is refused as a bad request.
///
/// The type mismatch is caught by the orchestrator, and the storage layer has its own guard
/// underneath for the one type whose constructor panics on a bad value — see
/// `a_value_that_is_not_a_facet_path_is_refused_rather_than_fatal`. Either way the caller gets a
/// 400 naming the field rather than a 500, and the node stays up.
#[tokio::test]
async fn a_value_the_declared_type_cannot_hold_is_a_bad_request() {
    let node = TestNode::start("").await;
    let client = node.client();

    let (status, body) = put_config(
        &node,
        "typed",
        &json!({
            "fields": {
                "id": {"field_type": "text", "indexed": true},
                "count": {"field_type": "i64", "indexed": true},
                "cat": {"field_type": "facet", "indexed": true}
            }
        }),
    )
    .await;
    assert_eq!(status, 200, "creating the config should succeed: {body}");

    // The reason matters as much as the refusal. A facet is refused for the *value* not being a
    // path, never for the type: every facet path is a string, so refusing the type refuses the
    // whole field, which is what made facets unwritable.
    for (field, value, expected) in [
        ("count", json!("not a number"), "expected I64"),
        ("cat", json!("no-slash"), "is not a facet path"),
        ("cat", json!(7), "expected Facet"),
    ] {
        let refused = client
            .write_document(
                "typed",
                "t1",
                &json!({"id": "t1", field: value.clone()}),
                None,
            )
            .await
            .expect_err(&format!("{field}={value} should be refused"));
        let message = refused.to_string();
        assert!(
            message.contains("400"),
            "{field}={value} is the caller's fault and should say so: {message}"
        );
        assert!(
            message.contains(field),
            "and should name the field: {message}"
        );
        assert!(
            message.contains(expected),
            "{field}={value} should be refused for {expected:?}: {message}"
        );
    }

    // The node is still serving, which is the half of this that a panic would break.
    let health = get_json(&node, "/_cluster/health").await;
    assert!(
        health.get("status").is_some(),
        "the node should still answer"
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
        .search("papers", "id:p1", Some(10), None, None, None)
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

/// Creating an index must not answer with `_seq`.
///
/// `PUT /api/{index}/_config` is the one listing that does not go through `describe_fields`,
/// which is where every other endpoint filters the engine's internal WAL sequence field out.
/// It also normalizes the submitted schema first, and normalization *inserts* `_seq` — so the
/// response advertised a field the caller never declared, cannot query, and is told about
/// nowhere else.
#[tokio::test]
async fn creating_an_index_does_not_report_the_internal_seq_field() {
    let node = TestNode::start("").await;

    let (status, body) = put_config(
        &node,
        "declared",
        &json!({
            "fields": {
                "id": {"name": "id", "field_type": "text", "indexed": true},
                "title": {"name": "title", "field_type": "text", "indexed": true}
            }
        }),
    )
    .await;

    assert_eq!(status, 200, "creating a config should succeed: {body}");

    let field_names = body["field_names"]
        .as_array()
        .unwrap_or_else(|| panic!("field_names should be an array: {body}"));

    assert!(
        field_names.iter().all(|name| name != "_seq"),
        "`_seq` is WAL bookkeeping and must not be reported as a field: {field_names:?}"
    );
    assert!(
        field_names.iter().any(|name| name == "title"),
        "the fields the caller actually declared must still be reported: {field_names:?}"
    );
}

async fn put_config(
    node: &TestNode,
    index: &str,
    body: &serde_json::Value,
) -> (u16, serde_json::Value) {
    with_tls_provider();
    let response = reqwest::Client::new()
        .put(format!("{}/api/{index}/_config", node.url))
        .json(body)
        .send()
        .await
        .expect("put config request");

    let status = response.status().as_u16();
    let body = response.json().await.unwrap_or(serde_json::Value::Null);
    (status, body)
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

/// A sort the index cannot answer is refused, not answered with an empty page.
///
/// Every shard fails the same way on a sort naming a column that is not there, and a
/// scatter-gather reports that as a partial failure: `200`, `hits: []`, `total_hits: 0`, and the
/// reason only in per-shard `errors`. A caller reading the hits — which is every caller — sees
/// "nothing matched" for a request that was never run. The refusal is now decided before any
/// shard is asked, so it arrives as a `400` naming the field.
///
/// `_seq` and `_score` are covered alongside an ordinary unknown name because they are the ones
/// that look plausible: the first was a real column until it was retired, the second is a key
/// every hit carries in the response. Neither is a column that can be ordered on.
///
/// `flag` covers the other half of the refusal, and the half a check for the *name* misses: the
/// field is in the schema, so a guard asking whether a column of that name exists waves it
/// through, and the engine then refuses it in every shard for having no fast column to order by
/// — the same empty page, from a request that looked valid.
#[tokio::test]
async fn a_sort_on_a_field_the_index_cannot_order_by_is_refused() {
    let node = TestNode::start("").await;
    seed_ordered(&node, "sorted", 5).await;

    // A boolean is inferred without a fast column, which is what makes it unsortable.
    let client = node.client();
    client
        .bulk_index(
            "sorted",
            &[json!({"id": "d005", "doc": {"id": "d005", "rank": 5, "body": "page", "flag": true}})],
        )
        .await
        .expect("bulk write");
    client.admin_index_commit("sorted").await.expect("commit");

    for field in ["no_such_field", "_seq", "_score", "flag"] {
        let (status, body) = post_json(
            &node,
            "/api/sorted/search",
            json!({"query": "body:page", "sort": {"field": field, "order": "asc"}}),
        )
        .await;

        assert_eq!(
            status, 400,
            "sorting by '{field}' should be refused: {body}"
        );
        let detail = body["details"].as_str().unwrap_or_default();
        assert!(
            detail.contains(field),
            "the refusal should name the field it refused: {detail}"
        );
    }

    // The guard refuses what cannot be ordered, and nothing else: a fast column sorts exactly,
    // and a text field without one still sorts approximately rather than being refused.
    for field in ["rank", "body", "id"] {
        let (status, body) = post_json(
            &node,
            "/api/sorted/search",
            json!({"query": "body:page", "sort": {"field": field, "order": "asc"}}),
        )
        .await;
        assert_eq!(status, 200, "sorting by '{field}' must still work: {body}");
        assert_eq!(
            body["hits"].as_array().map(|h| h.len()),
            Some(6),
            "an accepted sort must still return the hits: {body}"
        );
    }
}

/// A shadow field sorts, and sorts identically to `id`.
///
/// A shadow field is the document key under the source's own name — the query path already maps
/// it to `id`, and a sort maps the same way, so the two names order by the same values. Both are
/// asserted, because the equivalence is the contract: a caller that only ever says `doi` should
/// never have to learn that the engine calls it something else.
///
/// The order is checked across shards. A shadow index answers with the shadow name *instead of*
/// `id`, so a merge that looked for `id` on the hits would find nothing to order by and hand
/// back one shard's block after another — the right documents in the wrong order, which no
/// status code would reveal.
#[tokio::test]
async fn a_shadow_field_sorts_by_the_key_it_stands_for() {
    let node = TestNode::start("").await;
    let client = node.client();

    let (status, body) = put_config(
        &node,
        "papers",
        &json!({
            "fields": {
                "id": {"name": "id", "field_type": "text", "indexed": true},
                "doi": {"name": "doi", "field_type": "text", "indexed": false, "is_shadow": true},
                "title": {"name": "title", "field_type": "text", "indexed": true}
            }
        }),
    )
    .await;
    assert_eq!(status, 200, "creating the config should succeed: {body}");

    // Written in an order that is neither the ascending nor the descending one, so a sort that
    // did nothing at all could not pass by accident.
    let ids = ["p04", "p01", "p08", "p02", "p06", "p03", "p07", "p05"];
    let docs: Vec<serde_json::Value> = ids
        .iter()
        .map(|id| json!({"id": id, "doc": {"id": id, "doi": id, "title": "a paper"}}))
        .collect();
    client
        .bulk_index("papers", &docs)
        .await
        .expect("bulk write");
    client.admin_index_commit("papers").await.expect("commit");

    let mut expected: Vec<String> = ids.iter().map(|id| id.to_string()).collect();
    expected.sort();

    // The hits carry the shadow name in place of `id`, which is what a caller reads them by.
    let keys = |body: &serde_json::Value| -> Vec<String> {
        body["hits"]
            .as_array()
            .expect("hits array")
            .iter()
            .map(|hit| {
                hit["doi"]
                    .as_str()
                    .unwrap_or_else(|| panic!("a hit should carry its shadow key: {hit}"))
                    .to_string()
            })
            .collect()
    };

    async fn sorted(node: &TestNode, field: &str, order: &str) -> serde_json::Value {
        let (status, body) = post_json(
            node,
            "/api/papers/search",
            json!({
                "query": "title:paper",
                "limit": 8,
                "sort": {"field": field, "order": order}
            }),
        )
        .await;
        assert_eq!(status, 200, "sorting by '{field}' should work: {body}");
        body
    }

    let by_shadow = sorted(&node, "doi", "asc").await;
    assert_eq!(
        keys(&by_shadow),
        expected,
        "a sort on the shadow field should order by the key it stands for: {by_shadow}"
    );

    // The same order under the engine's own name for the field.
    let by_id = sorted(&node, "id", "asc").await;
    assert_eq!(
        keys(&by_id),
        expected,
        "sorting by 'id' must give the same order as sorting by the shadow field: {by_id}"
    );

    // And descending is the reverse rather than the same list, so the order is being applied
    // rather than the documents merely arriving in key order.
    let descending = sorted(&node, "doi", "desc").await;
    let mut reversed = expected.clone();
    reversed.reverse();
    assert_eq!(
        keys(&descending),
        reversed,
        "descending should be the reverse order: {descending}"
    );

    // The field has no fast column, so the order is reported as approximate — under the name
    // the caller asked for, not the one the engine ordered on.
    assert_eq!(
        by_shadow["_approximate_sort"].as_str(),
        Some("doi"),
        "the approximate-order note should name the caller's field: {by_shadow}"
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

/// A POST against the node with a raw JSON body, returning the status and the decoded body.
///
/// The SDK is the right tool for a supported request; this is for the ones a client should
/// never send, where what is being tested is the refusal itself.
async fn post_json(
    node: &TestNode,
    path: &str,
    body: serde_json::Value,
) -> (reqwest::StatusCode, serde_json::Value) {
    with_tls_provider();
    let resp = reqwest::Client::new()
        .post(format!("{}{path}", node.url))
        .json(&body)
        .send()
        .await
        .expect("request");
    let status = resp.status();
    (status, resp.json().await.unwrap_or(json!(null)))
}

/// Write `count` documents that sort unambiguously, and commit so they are searchable.
///
/// `rank` is a fast i64, so an ordered page is a page of a *total* order and this test can
/// assert exact membership rather than "some documents came back". `d000`, `d001`, … keep the
/// id in the same order as the rank.
async fn seed_ordered(node: &TestNode, index: &str, count: usize) {
    let client = node.client();
    let docs: Vec<serde_json::Value> = (0..count)
        .map(|n| {
            let id = format!("d{n:03}");
            json!({"id": id, "doc": {"id": id, "rank": n as i64, "body": "page"}})
        })
        .collect();
    client.bulk_index(index, &docs).await.expect("bulk write");
    client.admin_index_commit(index).await.expect("commit");
}

/// The ids of a search response's hits, in the order they came back.
fn hit_ids(found: &serde_json::Value) -> Vec<String> {
    found["hits"]
        .as_array()
        .expect("hits array")
        .iter()
        .map(|hit| hit["id"].as_str().unwrap_or_default().to_string())
        .collect()
}

fn rank_sort() -> storage::SortSpec {
    storage::SortSpec {
        field: "rank".to_string(),
        order: storage::SortOrder::Asc,
    }
}

/// Paging returns consecutive, non-overlapping slices of one order.
///
/// The property that makes paging worth having, and the one no unit test can reach: these
/// hits are gathered from two shards and merged, so a page is only meaningful if the skip is
/// applied once to the merged order rather than inside each shard. Asserted as exact
/// membership against a sort with no ties.
#[tokio::test]
async fn paging_walks_the_result_without_repeating_or_skipping_a_document() {
    let node = TestNode::start("").await;
    let client = node.client();
    seed_ordered(&node, "paged", 25).await;

    let mut seen: Vec<String> = Vec::new();
    for page in 0..5 {
        let found = client
            .search(
                "paged",
                "body:page",
                Some(5),
                Some(page * 5),
                None,
                Some(rank_sort()),
            )
            .await
            .expect("search");

        assert_eq!(found["offset"], json!(page * 5), "page {page}");
        assert_eq!(found["limit"], json!(5), "page {page}");
        assert_eq!(found["total_hits"], json!(25), "page {page}");
        seen.extend(hit_ids(&found));
    }

    let expected: Vec<String> = (0..25).map(|n| format!("d{n:03}")).collect();
    assert_eq!(
        seen, expected,
        "five pages of five should be the whole result, in order, each document once"
    );
}

/// One page equals the same slice of one large request.
///
/// Paging is only a way of reading a result if it does not *change* the result. Compared
/// against the unpaged answer rather than against a hand-written expectation, so the two can
/// only agree by actually agreeing.
#[tokio::test]
async fn a_page_is_the_same_slice_an_unpaged_search_would_have_returned() {
    let node = TestNode::start("").await;
    let client = node.client();
    seed_ordered(&node, "slices", 20).await;

    let whole = client
        .search(
            "slices",
            "body:page",
            Some(20),
            None,
            None,
            Some(rank_sort()),
        )
        .await
        .expect("unpaged search");
    let all = hit_ids(&whole);
    assert_eq!(all.len(), 20, "the unpaged search should return everything");

    for (offset, limit) in [(0, 20), (0, 3), (7, 4), (17, 3), (19, 1)] {
        let page = client
            .search(
                "slices",
                "body:page",
                Some(limit),
                Some(offset),
                None,
                Some(rank_sort()),
            )
            .await
            .expect("paged search");
        assert_eq!(
            hit_ids(&page),
            all[offset..offset + limit],
            "offset {offset} limit {limit}"
        );
    }
}

/// A page past the end is an empty page, not an error and not a wrong answer.
///
/// `total_hits` still reports the whole result, so a caller can tell it paged too far rather
/// than that the query stopped matching.
#[tokio::test]
async fn a_page_past_the_end_is_empty_and_still_reports_the_total() {
    let node = TestNode::start("").await;
    let client = node.client();
    seed_ordered(&node, "short", 3).await;

    let found = client
        .search(
            "short",
            "body:page",
            Some(10),
            Some(50),
            None,
            Some(rank_sort()),
        )
        .await
        .expect("a page past the end is answered, not refused");

    assert_eq!(hit_ids(&found).len(), 0, "nothing lives at offset 50");
    assert_eq!(
        found["total_hits"],
        json!(3),
        "the query still matched everything it matched, got {found}"
    );
    assert_eq!(found["offset"], json!(50));
}

/// The node's ceiling applies to `offset + limit`, on the HTTP surface as well as MCP.
///
/// The window is what gets fetched, so a deep page costs what a large limit costs — and this
/// route enforced neither before. Both refusals are `400` with the numbers in the message,
/// since a caller can only fix this by choosing different ones.
#[tokio::test]
async fn the_http_search_bounds_the_window_it_is_asked_for() {
    let node = TestNode::start(
        r#"
[security.limits]
max_search_limit = 100
"#,
    )
    .await;
    seed_ordered(&node, "bounded", 5).await;

    // Within the bound: accepted.
    let (status, _) = post_json(
        &node,
        "/api/bounded/search",
        json!({"query": "body:page", "limit": 50, "offset": 50}),
    )
    .await;
    assert_eq!(
        status, 200,
        "offset + limit exactly at the bound is allowed"
    );

    // The sum is over the bound, though neither number is on its own.
    let (status, body) = post_json(
        &node,
        "/api/bounded/search",
        json!({"query": "body:page", "limit": 51, "offset": 50}),
    )
    .await;
    assert_eq!(status, 400, "offset + limit past the bound is refused");
    let detail = body["details"].as_str().unwrap_or_default();
    assert!(
        detail.contains("101"),
        "the message should name the window it refused: {detail}"
    );
    assert!(
        detail.contains("100"),
        "and the bound it exceeded: {detail}"
    );

    // A limit past the bound on its own, which this route also never checked.
    let (status, _) = post_json(
        &node,
        "/api/bounded/search",
        json!({"query": "body:page", "limit": 5_000}),
    )
    .await;
    assert_eq!(status, 400, "a limit past the bound is refused");

    // An offset alone still counts the default limit against the bound, so the ceiling the
    // node advertises is the one it enforces.
    let (status, _) = post_json(
        &node,
        "/api/bounded/search",
        json!({"query": "body:page", "offset": 100}),
    )
    .await;
    assert_eq!(
        status, 400,
        "offset at the ceiling plus the default limit is over it"
    );
}

/// The streaming route refuses an offset rather than ignoring one.
///
/// A stream carries the whole result as it is produced, so there is no page to skip to. The
/// failure this prevents is silent: the same payload type serves both routes, so an offset
/// that reached here would have been dropped and page 2 would have been page 1.
#[tokio::test]
async fn the_streaming_search_refuses_an_offset_instead_of_dropping_it() {
    let node = TestNode::start("").await;
    seed_ordered(&node, "streamed", 5).await;

    let (status, body) = post_json(
        &node,
        "/api/streamed/search/stream",
        json!({"query": "body:page", "limit": 2, "offset": 2}),
    )
    .await;
    assert_eq!(status, 400, "an offset on a stream is refused, got {body}");
    let detail = body["details"].as_str().unwrap_or_default();
    assert!(
        detail.contains("/api/streamed/search"),
        "the refusal should name the route that does page, and name it correctly: {detail}"
    );

    // `offset: 0` asks for nothing, so it is not an error — a client that always sends the
    // field is not forced to special-case this route.
    let (status, _) = post_json(
        &node,
        "/api/streamed/search/stream",
        json!({"query": "body:page", "limit": 2, "offset": 0}),
    )
    .await;
    assert_eq!(status, 200, "offset 0 is the absence of paging, not paging");
}

/// `offset` written into the query reaches the same place the argument does.
///
/// The client and the REPL express a search entirely through this grammar, so this is the
/// only form of paging available to them.
#[tokio::test]
async fn an_inline_offset_modifier_pages_like_the_argument() {
    let node = TestNode::start("").await;
    let client = node.client();
    seed_ordered(&node, "inline", 12).await;

    let by_argument = client
        .search(
            "inline",
            "body:page",
            Some(4),
            Some(4),
            None,
            Some(rank_sort()),
        )
        .await
        .expect("search by argument");

    let by_modifier = client
        .search(
            "inline",
            "body:page limit 4 offset 4 sort rank:asc",
            None,
            None,
            None,
            None,
        )
        .await
        .expect("search by inline modifier");

    assert_eq!(
        hit_ids(&by_modifier),
        hit_ids(&by_argument),
        "the same page, however it was asked for"
    );
    assert_eq!(by_modifier["offset"], json!(4));
    assert_eq!(by_modifier["limit"], json!(4));
}

/// A field is `sortable` when the built index can order on it exactly, which is not what
/// `fast` says.
///
/// `fast` is the declaration; the column is written when the index is built. They agree here
/// — the schema was declared before any data — and the point of the flag is that a caller can
/// read one number rather than reasoning about when the index was created.
#[tokio::test]
async fn the_schema_reports_which_fields_can_be_sorted_exactly() {
    let node = TestNode::start("").await;
    let client = node.client();

    client
        .put_index_config(
            "sorted",
            &json!({
                "fields": {
                    "id": {"field_type": "text", "indexed": true, "stored": true},
                    "title": {"field_type": "text", "indexed": true, "stored": true, "fast": true},
                    "body": {"field_type": "text", "indexed": true, "stored": true},
                    "rank": {"field_type": "i64", "indexed": true, "stored": true, "fast": true}
                }
            }),
        )
        .await
        .expect("put schema");
    seed_ordered(&node, "sorted", 3).await;

    let config = get_json(&node, "/api/sorted/_config").await;
    let field = |name: &str| {
        config["fields"]
            .as_array()
            .and_then(|f| f.iter().find(|f| f["name"] == name))
            .cloned()
            .unwrap_or_else(|| panic!("field {name} in {config}"))
    };

    assert_eq!(
        field("rank")["sortable"],
        json!(true),
        "a fast numeric field sorts exactly"
    );
    assert_eq!(
        field("title")["sortable"],
        json!(true),
        "a text field declared fast has the column an exact sort needs"
    );
    assert_eq!(
        field("body")["sortable"],
        json!(false),
        "a text field without the declaration has no column, so its sort is approximate"
    );
}

/// A numeric field can decline the fast column it would otherwise get, and the config says so.
///
/// This is the symptom OB1 was filed for: a `PUT` declaring an i64 field `"fast": false` read
/// back from `GET .../_config` as `"fast": true`. The declaration was overwritten by
/// `normalize_after_deserialization`, which runs on the write path before the schema is stored —
/// so the config and the index agreed with each other and disagreed with the caller, which no
/// response could reveal.
///
/// Both directions are asserted from the same index, because the fix has to leave the default
/// alone: a numeric field that says nothing still gets a column, and only a caller who said
/// `false` gets no column.
#[tokio::test]
async fn a_numeric_field_can_decline_the_fast_column_it_gets_by_default() {
    let node = TestNode::start("").await;
    let client = node.client();

    client
        .put_index_config(
            "declined",
            &json!({
                "fields": {
                    "id": {"field_type": "text", "indexed": true, "stored": true},
                    "body": {"field_type": "text", "indexed": true, "stored": true},
                    "rank": {"field_type": "i64", "indexed": true, "fast": false},
                    "score": {"field_type": "i64", "indexed": true}
                }
            }),
        )
        .await
        .expect("put schema");
    seed_ordered(&node, "declined", 3).await;

    let config = get_json(&node, "/api/declined/_config").await;
    let field = |name: &str| {
        config["fields"]
            .as_array()
            .and_then(|f| f.iter().find(|f| f["name"] == name))
            .cloned()
            .unwrap_or_else(|| panic!("field {name} in {config}"))
    };

    assert_eq!(
        field("rank")["fast"],
        json!(false),
        "the config reports the declaration the caller made"
    );
    assert_eq!(
        field("rank")["sortable"],
        json!(false),
        "and the index was built to match it, so there is no column to sort on"
    );
    assert_eq!(
        field("score")["fast"],
        json!(true),
        "a numeric field that declared nothing still gets a column by default"
    );
    assert_eq!(field("score")["sortable"], json!(true));

    // What declining costs is the sort, and nothing else: a range on the field is answered from
    // the inverted index, which never needed a column.
    let (status, body) =
        post_json(&node, "/api/declined/search", json!({"query": "rank:>=1"})).await;
    assert_eq!(status, 200, "a range on a field with no column: {body}");
    // Sorted here, not asserted in the order it arrived: the query names no sort, so the shards
    // merge in whatever order they answer in and the sequence is not part of the contract.
    let mut matched = hit_ids(&body);
    matched.sort();
    assert_eq!(
        matched,
        vec!["d001".to_string(), "d002".to_string()],
        "the range is answered in full: {body}"
    );

    let (status, body) = post_json(
        &node,
        "/api/declined/search",
        json!({"query": "body:page", "sort": {"field": "rank", "order": "asc"}}),
    )
    .await;
    assert_eq!(
        status, 400,
        "a sort on a field the caller declared unsortable is refused: {body}"
    );
    assert!(
        body["details"]
            .as_str()
            .unwrap_or_default()
            .contains("rank"),
        "the refusal names the field: {body}"
    );
}

/// Declaring `fast` on a type that can carry no column is refused as a sort, not answered empty.
///
/// The index builder adds a boolean, bytes, ip, json or facet field without reading `fast` at all,
/// so a declared `true` on one had no column behind it — and the guard that refuses an unsortable
/// sort reads the declaration, so it waved these through. Measured before the fix: `200`,
/// `hits: []`, `total_hits: 0`, and the reason only in per-shard `errors` — the exact failure that
/// guard exists to prevent, reached through a declaration the build path ignores.
///
/// Both halves are asserted here, because either one alone leaves the caller misinformed: the
/// config has to stop reporting a column that was never built, and the sort has to be refused with
/// a reason the caller can act on rather than one telling them to declare a flag that changes
/// nothing.
#[tokio::test]
async fn a_type_that_can_carry_no_column_is_refused_rather_than_answered_empty() {
    let node = TestNode::start("").await;
    let client = node.client();

    client
        .put_index_config(
            "unsortable",
            &json!({
                "fields": {
                    "id": {"field_type": "text", "indexed": true, "stored": true},
                    "body": {"field_type": "text", "indexed": true, "stored": true},
                    "flag": {"field_type": "boolean", "indexed": true, "fast": true},
                    "addr": {"field_type": "ip", "indexed": true, "fast": true}
                }
            }),
        )
        .await
        .expect("put schema");
    client
        .bulk_index(
            "unsortable",
            &[json!({"id": "d1", "doc": {
                "id": "d1", "body": "page", "flag": true, "addr": "10.0.0.1"}})],
        )
        .await
        .expect("write");
    client
        .admin_index_commit("unsortable")
        .await
        .expect("commit");

    let config = get_json(&node, "/api/unsortable/_config").await;
    let field = |name: &str| {
        config["fields"]
            .as_array()
            .and_then(|f| f.iter().find(|f| f["name"] == name))
            .cloned()
            .unwrap_or_else(|| panic!("field {name} in {config}"))
    };

    for name in ["flag", "addr"] {
        assert_eq!(
            field(name)["fast"],
            json!(false),
            "the config must not report a column the index never builds for {name}"
        );
        assert_eq!(field(name)["sortable"], json!(false));

        let (status, body) = post_json(
            &node,
            "/api/unsortable/search",
            json!({"query": "body:page", "sort": {"field": name, "order": "asc"}}),
        )
        .await;
        assert_eq!(
            status, 400,
            "sorting by '{name}' must be refused before any shard runs: {body}"
        );
        let detail = body["details"].as_str().unwrap_or_default();
        assert!(
            detail.contains(name),
            "the refusal names the field: {detail}"
        );
        assert!(
            detail.contains("cannot give it one"),
            "and says the declaration cannot fix it, rather than asking for one: {detail}"
        );
    }
}

/// A sort on a column the index was never built with is refused, not answered with an empty page.
///
/// This is the one refusal `unsortable_sort_field` cannot make. `fast` is a declaration and the
/// column is written from it when the index is built, so a field declared `fast` onto an index
/// that already holds data has a declaration and no column — and only the built index knows.
/// Every shard then fails the same way, which a scatter-gather used to report as a partial
/// outage: `200`, `hits: []`, `total_hits: 0`, with `errors` naming the field. A caller reading
/// the hits — which is every caller — saw "nothing matched" for a query that ran nowhere.
///
/// The schema listing already told the truth about it, and still does: `fast: true` beside
/// `sortable: false` is exactly the gap those two flags exist to expose.
#[tokio::test]
async fn a_sort_on_a_column_the_index_was_not_built_with_is_refused() {
    let node = TestNode::start("").await;
    let client = node.client();

    client
        .put_index_config(
            "lagging",
            &json!({"fields": {
                "id": {"field_type": "text", "indexed": true, "stored": true},
                "body": {"field_type": "text", "indexed": true, "stored": true}
            }}),
        )
        .await
        .expect("put schema");
    seed_ordered(&node, "lagging", 3).await;

    // Declared onto an index that already has data, so the column cannot exist for it.
    let (status, body) = put_config(
        &node,
        "lagging",
        &json!({"fields": {
            "id": {"field_type": "text", "indexed": true, "stored": true},
            "body": {"field_type": "text", "indexed": true, "stored": true},
            "added": {"field_type": "i64", "indexed": true, "fast": true}
        }}),
    )
    .await;
    assert_eq!(status, 200, "declaring the field must be accepted: {body}");

    let config = get_json(&node, "/api/lagging/_config").await;
    let added = config["fields"]
        .as_array()
        .and_then(|f| f.iter().find(|f| f["name"] == "added"))
        .cloned()
        .expect("the declared field");
    assert_eq!(added["fast"], json!(true), "the declaration is honoured");
    assert_eq!(
        added["sortable"],
        json!(false),
        "and the built index has no column for it, which is the gap under test: {added}"
    );

    let (status, body) = post_json(
        &node,
        "/api/lagging/search",
        json!({"query": "body:page", "sort": {"field": "added", "order": "asc"}}),
    )
    .await;
    assert_eq!(
        status, 400,
        "a query no shard could run must be refused, not answered empty: {body}"
    );
    let detail = body["details"].as_str().unwrap_or_default();
    assert!(
        detail.contains("added"),
        "the refusal names the field: {detail}"
    );

    // The same index still answers everything it can, so the refusal is about this query rather
    // than about the index having a declaration it cannot honour.
    let (status, body) =
        post_json(&node, "/api/lagging/search", json!({"query": "body:page"})).await;
    assert_eq!(status, 200, "an unsorted search is unaffected: {body}");
    assert_eq!(body["hits"].as_array().map(|h| h.len()), Some(3));
}

/// Sorting on a text field with no fast column says so in the response.
///
/// The hits are real and look exactly like an exact answer, so nothing about them reveals
/// that the order is over a sample. It has to be stated, and stated where the caller reading
/// the result will see it rather than in the node's log.
#[tokio::test]
async fn an_approximate_sort_is_reported_on_the_response_that_carries_it() {
    let node = TestNode::start("").await;
    let client = node.client();
    seed_ordered(&node, "approx", 6).await;

    let found = client
        .search(
            "approx",
            "body:page",
            Some(3),
            None,
            None,
            Some(storage::SortSpec {
                field: "body".to_string(),
                order: storage::SortOrder::Asc,
            }),
        )
        .await
        .expect("search");

    assert_eq!(
        found["_approximate_sort"],
        json!("body"),
        "a sort on a field with no fast column is approximate, got {found}"
    );

    // The exact sort on the same data says nothing, so the flag's presence carries meaning.
    let exact = client
        .search(
            "approx",
            "body:page",
            Some(3),
            None,
            None,
            Some(rank_sort()),
        )
        .await
        .expect("search");
    assert!(
        exact.get("_approximate_sort").is_none(),
        "an exact sort should not be flagged, got {exact}"
    );
}

/// A shadow field that disagrees with the identifier is refused, not silently emptied.
///
/// A shadow field is a name for the key rather than a field of its own: the write path drops it
/// from the stored document and the read path writes the key back under it. A document that
/// says something else under that name therefore has that something else discarded — with no
/// error, no warning, and a later read reporting the identifier in its place, so the loss is
/// invisible from both ends. The write is refused instead, and the refusal names both values so
/// the caller can see which of the two it meant.
///
/// Both write paths are checked: a single write validates inline, a bulk write through the
/// staged path, and the rule belongs to the index rather than to whichever endpoint was used.
#[tokio::test]
async fn a_shadow_field_disagreeing_with_the_identifier_is_refused() {
    let node = TestNode::start("").await;
    let client = node.client();

    let (status, body) = put_config(
        &node,
        "files",
        &json!({
            "fields": {
                "id": {"name": "id", "field_type": "text", "indexed": true},
                "sha1": {"name": "sha1", "field_type": "text", "indexed": false, "is_shadow": true},
                "title": {"name": "title", "field_type": "text", "indexed": true}
            }
        }),
    )
    .await;
    assert_eq!(status, 200, "creating the config should succeed: {body}");

    // A single write whose shadow field carries something the index cannot keep.
    let refused = client
        .write_document(
            "files",
            "AAA",
            &json!({"id": "AAA", "sha1": "BBB", "title": "a record"}),
            None,
        )
        .await;
    let message = format!("{:?}", refused.expect_err("the write must be refused"));
    assert!(
        message.contains("sha1") && message.contains("BBB") && message.contains("AAA"),
        "the refusal has to name the field and both values, or the caller cannot tell which of \
         them was wrong: {message}"
    );

    // And the same document through the bulk path, which validates elsewhere.
    let bulk = client
        .bulk_index(
            "files",
            &[json!({"id": "AAA", "doc": {"id": "AAA", "sha1": "BBB", "title": "a record"}})],
        )
        .await;
    let bulk_body = bulk.expect("a bulk write reports per-document outcomes rather than failing");
    assert_eq!(
        bulk_body["items_written"].as_u64(),
        Some(0),
        "a bulk write must be held to the same rule as a single one: {bulk_body}"
    );
    assert!(
        bulk_body["errors"].to_string().contains("sha1"),
        "and must say why the document it received was not written: {bulk_body}"
    );

    // Nothing was stored under either route.
    client.admin_index_commit("files").await.expect("commit");
    let found = client
        .search("files", "title:record", Some(10), None, None, None)
        .await
        .expect("search");
    assert_eq!(
        found["total_hits"].as_u64(),
        Some(0),
        "a refused write must leave nothing behind: {found}"
    );
}

/// The agreeing cases, which have to keep working: the rule is about disagreement alone.
///
/// A document may carry the shadow field with the identifier's value, or omit it entirely and
/// let reconstruction supply it on the way out. Several shadow names are legal too — that is
/// what makes them names for one key rather than fields — so long as they all agree.
#[tokio::test]
async fn a_shadow_field_agreeing_with_the_identifier_is_accepted() {
    let node = TestNode::start("").await;
    let client = node.client();

    let (status, body) = put_config(
        &node,
        "files",
        &json!({
            "fields": {
                "id": {"name": "id", "field_type": "text", "indexed": true},
                "sha1": {"name": "sha1", "field_type": "text", "indexed": false, "is_shadow": true},
                "md5": {"name": "md5", "field_type": "text", "indexed": false, "is_shadow": true},
                "title": {"name": "title", "field_type": "text", "indexed": true}
            }
        }),
    )
    .await;
    assert_eq!(status, 200, "creating the config should succeed: {body}");

    client
        .bulk_index(
            "files",
            &[
                // Every shadow name spelled out, all agreeing.
                json!({"id": "AAA", "doc": {"id": "AAA", "sha1": "AAA", "md5": "AAA", "title": "a record"}}),
                // The shadow names omitted, which is the ordinary shape of a rewritten document.
                json!({"id": "BBB", "doc": {"id": "BBB", "title": "a record"}}),
            ],
        )
        .await
        .expect("both documents should be accepted");
    client.admin_index_commit("files").await.expect("commit");

    let found = client
        .search("files", "title:record", Some(10), None, None, None)
        .await
        .expect("search");
    assert_eq!(
        found["total_hits"].as_u64(),
        Some(2),
        "both documents should have been stored: {found}"
    );

    // Both come back carrying the key under every shadow name and none under `id`, whether or
    // not the write spelled them out.
    for hit in found["hits"].as_array().expect("hits") {
        let key = hit["sha1"].as_str().expect("a hit carries the shadow name");
        assert_eq!(
            hit["md5"].as_str(),
            Some(key),
            "both names are the key: {hit}"
        );
        assert!(hit.get("id").is_none(), "no hit carries `id`: {hit}");
    }
}

/// A shadow field is held to the key even when the body never names it.
///
/// The check read the identifier out of the body, so on the documented shape — the id beside the
/// body rather than in it — there was nothing to compare against and every document passed. That
/// is the shape a bulk loader sends, so the one case the check exists for was the case it did not
/// cover: the shadow value is stripped from the stored blob and the key is written back under its
/// name, so a document disagreeing there loses that value with nothing said.
#[tokio::test]
async fn a_shadow_field_is_checked_against_the_key_when_the_body_omits_id() {
    let node = TestNode::start("").await;
    let client = node.client();

    let (status, body) = put_config(
        &node,
        "files",
        &json!({
            "fields": {
                "id": {"name": "id", "field_type": "text", "indexed": true},
                "sha1": {"name": "sha1", "field_type": "text", "indexed": false, "is_shadow": true},
                "title": {"name": "title", "field_type": "text", "indexed": true}
            }
        }),
    )
    .await;
    assert_eq!(status, 200, "creating the config should succeed: {body}");

    // Neither document repeats `id` in its body. The first disagrees with the key under the
    // shadow name and cannot be stored; the second agrees and can.
    let bulk = client
        .bulk_index(
            "files",
            &[
                json!({"id": "AAA", "doc": {"sha1": "BBB", "title": "a record"}}),
                json!({"id": "CCC", "doc": {"sha1": "CCC", "title": "a record"}}),
            ],
        )
        .await
        .expect("a bulk write reports per-document outcomes rather than failing");
    assert_eq!(
        bulk["items_written"].as_u64(),
        Some(1),
        "the disagreeing document should be the only one dropped: {bulk}"
    );
    let errors = bulk["errors"].to_string();
    assert!(
        errors.contains("sha1") && errors.contains("BBB") && errors.contains("AAA"),
        "and the reason has to name the field and both values: {bulk}"
    );

    client.admin_index_commit("files").await.expect("commit");
    let found = client
        .search("files", "title:record", Some(10), None, None, None)
        .await
        .expect("search");
    assert_eq!(
        found["total_hits"].as_u64(),
        Some(1),
        "only the storable document should be there: {found}"
    );
    assert_eq!(
        found["hits"][0]["sha1"].as_str(),
        Some("CCC"),
        "and it comes back with the key under the shadow name: {found}"
    );
}

/// One bad document in a batch costs that document, not the batch.
///
/// A bulk write reports partial success — `items_received` against `items_written`, with a
/// reason for each shortfall — so a single unstorable row in an import is no reason to discard
/// the rows around it. What it may not do is write the bad row anyway.
#[tokio::test]
async fn a_rejected_document_does_not_cost_the_rest_of_its_batch() {
    let node = TestNode::start("").await;
    let client = node.client();

    let (status, body) = put_config(
        &node,
        "files",
        &json!({
            "fields": {
                "id": {"name": "id", "field_type": "text", "indexed": true},
                "sha1": {"name": "sha1", "field_type": "text", "indexed": false, "is_shadow": true},
                "title": {"name": "title", "field_type": "text", "indexed": true}
            }
        }),
    )
    .await;
    assert_eq!(status, 200, "creating the config should succeed: {body}");

    let written = client
        .bulk_index(
            "files",
            &[
                json!({"id": "AAA", "doc": {"id": "AAA", "sha1": "AAA", "title": "a record"}}),
                json!({"id": "BBB", "doc": {"id": "BBB", "sha1": "WRONG", "title": "a record"}}),
                json!({"id": "CCC", "doc": {"id": "CCC", "sha1": "CCC", "title": "a record"}}),
            ],
        )
        .await
        .expect("the batch as a whole should be accepted");

    assert_eq!(
        written["items_received"].as_u64(),
        Some(3),
        "every document received is accounted for: {written}"
    );
    assert_eq!(
        written["items_written"].as_u64(),
        Some(2),
        "the two storable documents should be stored: {written}"
    );
    let errors = written["errors"].to_string();
    assert!(
        errors.contains("sha1") && errors.contains("WRONG"),
        "the one that was not stored needs its reason: {written}"
    );

    client.admin_index_commit("files").await.expect("commit");
    let found = client
        .search("files", "title:record", Some(10), None, None, None)
        .await
        .expect("search");
    assert_eq!(
        found["total_hits"].as_u64(),
        Some(2),
        "the rejected document must not have been written: {found}"
    );
}

/// The key is the same field however a schema declares it.
///
/// `id` is the one field the index builder creates itself: raw-tokenized, stored and never
/// fast, whatever the schema says. A declared type is fiction, and the engine believed it in
/// three places — `_config` reported it, the slow write validation compared the `Text` it
/// infers for a key against the declaration, and a sort merge keyed the field by its declared
/// type. So an index declaring `id` as `i64` refused every document of a batch large enough to
/// take the slow path, and one declaring it `date` returned an arbitrary order from a sort that
/// reported no error. Both are pinned here against the declaration that provoked them.
#[tokio::test]
async fn a_declared_id_type_does_not_contradict_the_key_the_index_builds() {
    let node = TestNode::start("").await;
    let client = node.client();

    for declared in ["text", "i64", "date"] {
        let index = format!("keyed_{declared}");
        let (status, body) = put_config(
            &node,
            &index,
            &json!({
                "fields": {
                    "id": {"name": "id", "field_type": declared, "indexed": true},
                    "title": {"name": "title", "field_type": "text", "indexed": true}
                }
            }),
        )
        .await;
        assert_eq!(
            status, 200,
            "declaring id as {declared} should be accepted: {body}"
        );

        // Reported as what the index actually carries, not as what was asked for.
        let described =
            serde_json::to_value(client.get_index_config(&index).await.expect("config")).unwrap();
        let id_field = described["fields"]
            .as_array()
            .expect("fields")
            .iter()
            .find(|f| f["name"] == "id")
            .unwrap_or_else(|| panic!("no id field: {described}"));
        assert_eq!(
            id_field["type"].as_str(),
            Some("text"),
            "the key is a raw string whatever was declared: {described}"
        );
        assert_eq!(
            id_field["fast"].as_bool(),
            Some(false),
            "the key never gets a fast column, so it must not claim one: {described}"
        );

        // Enough documents to take the slow validation path, which infers `Text` for the key.
        let docs: Vec<serde_json::Value> = (0..1_100)
            .map(|n| {
                let id = format!("k{n:05}");
                json!({"id": id, "doc": {"id": id, "title": "a record"}})
            })
            .collect();
        let written = client.bulk_index(&index, &docs).await.expect("bulk write");
        assert_eq!(
            written["items_written"].as_u64(),
            Some(1_100),
            "a declared id type must not refuse the documents it keys: {written}"
        );
        client.admin_index_commit(&index).await.expect("commit");

        // And a sort on the key orders by the key, rather than by a type it does not have.
        let sorted = client
            .search(
                &index,
                "title:record",
                Some(3),
                None,
                None,
                Some(storage::SortSpec {
                    field: "id".into(),
                    order: storage::SortOrder::Asc,
                }),
            )
            .await
            .expect("sorted search");
        let keys: Vec<&str> = sorted["hits"]
            .as_array()
            .expect("hits")
            .iter()
            .map(|hit| hit["id"].as_str().unwrap_or("<missing>"))
            .collect();
        assert_eq!(
            keys,
            ["k00000", "k00001", "k00002"],
            "declared as {declared}, the sort must still order by the identifier: {sorted}"
        );
    }
}

/// Declaring `id` explicitly does not displace the shadow name as the key documents answer by.
///
/// The shadow tests elsewhere let the config endpoint insert `id` itself. A schema that names it
/// takes a different route — enrichment runs the key's branch over a caller-supplied definition,
/// tokenizer and all — and the question is whether anything downstream then reads `id` as an
/// ordinary field. It must not: the identifier still travels under the shadow name, and both
/// spellings of a query, a projection and a sort still mean the key.
#[tokio::test]
async fn a_declared_id_beside_a_shadow_field_is_still_the_shadow_name() {
    let node = TestNode::start("").await;
    let client = node.client();

    let (status, body) = put_config(
        &node,
        "files",
        &json!({
            "fields": {
                "id":     {"name": "id",     "field_type": "text", "indexed": true,  "stored": true,  "tokenizer": "raw"},
                "sha256": {"name": "sha256", "field_type": "text", "indexed": false, "stored": false, "is_shadow": true, "tokenizer": "raw"},
                "title":  {"name": "title",  "field_type": "text", "indexed": true}
            }
        }),
    )
    .await;
    assert_eq!(status, 200, "creating the config should succeed: {body}");

    let ids = ["aa11", "bb22", "cc33"];
    let docs: Vec<serde_json::Value> = ids
        .iter()
        .map(|k| json!({"id": k, "doc": {"id": k, "sha256": k, "title": "a record"}}))
        .collect();
    client.bulk_index("files", &docs).await.expect("bulk write");
    client.admin_index_commit("files").await.expect("commit");

    // The description relates the two names, exactly as it does when `id` is inserted for the
    // caller rather than declared by it.
    let described =
        serde_json::to_value(client.get_index_config("files").await.expect("config")).unwrap();
    let field = |name: &str| -> serde_json::Value {
        described["fields"]
            .as_array()
            .expect("fields")
            .iter()
            .find(|f| f["name"] == name)
            .unwrap_or_else(|| panic!("no {name}: {described}"))
            .clone()
    };
    assert_eq!(
        field("id")["returned_as"].as_str(),
        Some("sha256"),
        "a declared `id` still names what the hits carry: {described}"
    );
    assert_eq!(
        field("sha256")["shadow"].as_bool(),
        Some(true),
        "{described}"
    );

    // Both spellings of the key answer, and every hit comes back under the shadow name.
    for query in [
        "sha256:bb22",
        "id:bb22",
        "sha256:bb22 AND title:record",
        "title:record AND id:bb22",
    ] {
        let found = client
            .search("files", query, Some(10), None, None, None)
            .await
            .expect("search");
        assert_eq!(
            found["total_hits"].as_u64(),
            Some(1),
            "{query:?} should find the document: {found}"
        );
        let hit = &found["hits"][0];
        assert_eq!(hit["sha256"].as_str(), Some("bb22"), "{hit}");
        assert!(hit.get("id").is_none(), "no hit carries `id`: {hit}");
    }

    // A projection naming either one, and a sort by either one, both mean the key.
    for asked in ["id", "sha256"] {
        let projected = client
            .search(
                "files",
                "sha256:bb22",
                Some(1),
                None,
                Some(vec![asked.to_string()]),
                None,
            )
            .await
            .expect("projection");
        assert_eq!(
            projected["hits"][0]["sha256"].as_str(),
            Some("bb22"),
            "projecting {asked:?} returns the identifier: {projected}"
        );

        let sorted = client
            .search(
                "files",
                "title:record",
                Some(3),
                None,
                None,
                Some(storage::SortSpec {
                    field: asked.into(),
                    order: storage::SortOrder::Asc,
                }),
            )
            .await
            .expect("sorted search");
        assert_eq!(
            sorted["_approximate_sort"].as_str(),
            Some("sha256"),
            "the order is reported under the name the hits carry: {sorted}"
        );
        let keys: Vec<&str> = sorted["hits"]
            .as_array()
            .expect("hits")
            .iter()
            .map(|hit| hit["sha256"].as_str().unwrap_or("<missing>"))
            .collect();
        assert_eq!(
            keys, ids,
            "sorting by {asked:?} orders by the identifier: {sorted}"
        );
    }
}

/// An index is identified by its name, and by nothing else.
///
/// A reverse lookup keyed by a hash of the field names used to sit in front of the schema
/// cache, and it answered with whichever index of that shape had been cached last. Two indexes
/// of the same shape are ordinary — a monthly partition, a per-tenant index, a re-import beside
/// the original — so a write to one was judged by the other's schema.
///
/// The trigger was any schema that carried a non-zero fingerprint: one the caller supplied
/// through `PUT /_config` (which the bundled importer did on every import), or one a
/// `PATCH /_schema` computed. Each of the three cases below is a symptom that was reproducible
/// against a real node.
#[tokio::test]
async fn a_fresh_index_is_not_judged_by_another_index_of_the_same_shape() {
    let node = TestNode::start("").await;

    // `alpha` declares n as i64, and the config carries a fingerprint of its own field names —
    // the shape a document with keys {id, n} hashes to.
    let (status, body) = put_config(
        &node,
        "alpha",
        &json!({
            "fingerprint": 4190626629970713083u64,
            "fields": {
                "id": {"field_type": "text", "indexed": true},
                "n": {"field_type": "i64", "indexed": true}
            }
        }),
    )
    .await;
    assert_eq!(status, 200, "declaring alpha should succeed: {body}");

    let client = node.client();
    client
        .write_document("alpha", "a1", &json!({"id": "a1", "n": 5}), None)
        .await
        .expect("alpha takes an i64");

    // `beta` has no schema at all, so n is whatever the first document says it is.
    client
        .write_document("beta", "b1", &json!({"id": "b1", "n": "a word"}), None)
        .await
        .expect("a fresh index types n from the document, not from alpha");

    let beta = get_json(&node, "/api/beta/_config").await;
    let n_type = beta["fields"]
        .as_array()
        .and_then(|fields| fields.iter().find(|f| f["name"] == "n"))
        .and_then(|f| f["type"].as_str())
        .map(str::to_string);
    assert_eq!(
        n_type.as_deref(),
        Some("text"),
        "beta types n from its own document: {beta}"
    );
}

/// A shadow field belongs to the index that declares it.
///
/// `sha256` naming the document key in one index says nothing about `sha256` being an ordinary
/// column in another. Through the reverse lookup it did: a fresh index writing a file hash as
/// data was refused for disagreeing with an identifier it had never heard of.
#[tokio::test]
async fn a_shadow_field_does_not_reach_an_index_that_never_declared_it() {
    let node = TestNode::start("").await;

    let (status, body) = put_config(
        &node,
        "hashed",
        &json!({
            "fingerprint": 12540683531433093633u64,
            "fields": {
                "id": {"field_type": "text", "indexed": true},
                "sha256": {"field_type": "text", "indexed": true, "is_shadow": true}
            }
        }),
    )
    .await;
    assert_eq!(status, 200, "declaring hashed should succeed: {body}");

    let client = node.client();
    client
        .write_document("hashed", "h1", &json!({"id": "h1", "sha256": "h1"}), None)
        .await
        .expect("a shadow field carrying the key is fine");

    client
        .write_document(
            "files",
            "f1",
            &json!({"id": "f1", "sha256": "deadbeefcafe"}),
            None,
        )
        .await
        .expect("sha256 is ordinary data on an index that never declared it a shadow");
}

/// A bulk write records its fields in the schema of the index it was written to.
///
/// The quiet symptom, and the worst: where the other index's schema happened to *accept* the
/// document, nothing was refused. Validation ran against a schema that already knew the field,
/// so `needs_evolution` stayed false, and the index the document was actually written to never
/// learned it. The write answered `items_written: 1, errors: []` and the field was unqueryable.
#[tokio::test]
async fn a_bulk_write_teaches_its_own_index_the_fields_it_carries() {
    let node = TestNode::start("").await;

    let (status, body) = put_config(
        &node,
        "declared",
        &json!({
            "fingerprint": 4190626629970713083u64,
            "fields": {
                "id": {"field_type": "text", "indexed": true},
                "n": {"field_type": "i64", "indexed": true}
            }
        }),
    )
    .await;
    assert_eq!(
        status, 200,
        "declaring the first index should succeed: {body}"
    );

    let (status, body) = post_json(
        &node,
        "/api/derived/_bulk",
        json!([{"id": "g1", "doc": {"id": "g1", "n": 7}}]),
    )
    .await;
    assert_eq!(status, 200, "the bulk write should succeed: {body}");
    assert_eq!(
        body["items_written"], 1,
        "and should write its document: {body}"
    );

    let derived = get_json(&node, "/api/derived/_config").await;
    let names: Vec<&str> = derived["fields"]
        .as_array()
        .map(|fields| fields.iter().filter_map(|f| f["name"].as_str()).collect())
        .unwrap_or_default();
    assert!(
        names.contains(&"n"),
        "a written field belongs in the schema of the index that took it: {derived}"
    );
}

/// A declared facet field takes a facet path, and the query side finds it.
///
/// Everything under the write was already built for this — `add_json_value_to_doc` has a facet
/// arm, `normalize_facet_query` quotes a path so the parser resolves it to a facet term, and the
/// type is declarable as `facet`, `category` or `tag`. The validator refused every value before
/// any of it ran: a facet path is a string, `infer_field_type` reads it as text, and text was
/// not a type a facet field would accept. So the whole type was declarable and unusable.
#[tokio::test]
async fn a_facet_field_takes_a_path_and_a_list_of_them() {
    let node = TestNode::start("").await;
    let client = node.client();

    let (status, body) = put_config(
        &node,
        "catalogue",
        &json!({
            "fields": {
                "id": {"field_type": "text", "indexed": true},
                "cat": {"field_type": "facet", "indexed": true}
            }
        }),
    )
    .await;
    assert_eq!(
        status, 200,
        "declaring a facet field should succeed: {body}"
    );

    client
        .write_document(
            "catalogue",
            "p1",
            &json!({"id": "p1", "cat": "/electronics/phones"}),
            None,
        )
        .await
        .expect("a valid facet path is storable");

    // A list is several values of the field, exactly as it is for a number: the writer flattens
    // it into one `add_facet` per element, so either path finds the document.
    client
        .write_document(
            "catalogue",
            "p2",
            &json!({"id": "p2", "cat": ["/electronics/laptops", "/clearance"]}),
            None,
        )
        .await
        .expect("a list of facet paths is several values of the field");

    client
        .admin_index_commit("catalogue")
        .await
        .expect("explicit commit");

    for (query, expected) in [
        ("cat:/electronics/phones", vec!["p1"]),
        ("cat:/electronics/laptops", vec!["p2"]),
        ("cat:/clearance", vec!["p2"]),
        // A facet matches its own path and everything beneath it.
        ("cat:/electronics", vec!["p1", "p2"]),
    ] {
        let found = client
            .search("catalogue", query, Some(10), None, None, None)
            .await
            .unwrap_or_else(|e| panic!("{query} should run: {e}"));
        let mut ids: Vec<String> = found["hits"]
            .as_array()
            .map(|hits| {
                hits.iter()
                    .filter_map(|h| h["id"].as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        ids.sort();
        assert_eq!(ids, expected, "{query} should match {expected:?}: {found}");
    }
}

/// One bad element refuses the document, wherever it sits in the list.
///
/// The element decides, not the shape: a list of facets is judged the same way a list of numbers
/// is, so a path that will not parse is as unstorable in a list of three as it is on its own.
#[tokio::test]
async fn one_unparseable_path_refuses_a_list_of_facets() {
    let node = TestNode::start("").await;
    let client = node.client();

    let (status, body) = put_config(
        &node,
        "mixed",
        &json!({
            "fields": {
                "id": {"field_type": "text", "indexed": true},
                "cat": {"field_type": "facet", "indexed": true}
            }
        }),
    )
    .await;
    assert_eq!(status, 200, "declaring the config should succeed: {body}");

    for value in [
        json!(["/good/path", "no-slash"]),
        json!(["no-slash", "/good/path"]),
    ] {
        let refused = client
            .write_document("mixed", "m1", &json!({"id": "m1", "cat": value}), None)
            .await
            .expect_err(&format!("{value} should be refused"));
        let message = refused.to_string();
        assert!(
            message.contains("400") && message.contains("is not a facet path"),
            "{value} should be refused as a bad path, not as a bad type: {message}"
        );
    }

    // And the refusal is per document rather than per batch: a bulk write reports which one.
    let (status, body) = post_json(
        &node,
        "/api/mixed/_bulk",
        json!([
            {"id": "ok", "doc": {"cat": "/fine"}},
            {"id": "bad", "doc": {"cat": ["/fine", "no-slash"]}}
        ]),
    )
    .await;
    assert_eq!(status, 200, "a bulk write reports partial success: {body}");
    assert_eq!(
        body["items_written"], 1,
        "the good document is written: {body}"
    );
    let errors = body["errors"].as_array().expect("errors array");
    assert_eq!(errors.len(), 1, "and only the bad one is refused: {body}");
    assert!(
        errors[0]
            .as_str()
            .is_some_and(|e| e.starts_with("document 1:") && e.contains("is not a facet path")),
        "naming its position and its reason: {body}"
    );
}

/// A bytes field holds bytes, and says so when it is handed something else.
///
/// The validator checked only that the value was an array, and the writer then read it with
/// `filter_map(as_u64).map(|n| n as u8)` — so a list of words was accepted and indexed as
/// nothing, and a list of numbers over 255 was accepted and truncated. `[300, 70000]` stored as
/// bytes 44 and 112: not a value the caller sent, and no error to say so.
///
/// A list means something different under this type than under any other. `[1, 2]` in an `i64`
/// field is two values of the field and matches a query for either; under `bytes` it is one
/// two-byte value. So an element that does not fit refuses the value rather than itself.
#[tokio::test]
async fn every_element_of_a_bytes_field_is_a_byte() {
    let node = TestNode::start("").await;
    let client = node.client();

    let (status, body) = put_config(
        &node,
        "blobs",
        &json!({
            "fields": {
                "id": {"field_type": "text", "indexed": true},
                "blob": {"field_type": "bytes", "indexed": true}
            }
        }),
    )
    .await;
    assert_eq!(
        status, 200,
        "declaring a bytes field should succeed: {body}"
    );

    for (name, value) in [
        ("a word", json!(["not", "bytes"])),
        ("over 255", json!([300, 70000])),
        ("negative", json!([-1])),
        ("fractional", json!([1.5])),
        ("one bad element among good ones", json!([104, 105, 300])),
    ] {
        let refused = client
            .write_document(
                "blobs",
                "b1",
                &json!({"id": "b1", "blob": value.clone()}),
                None,
            )
            .await
            .expect_err(&format!("{name} should be refused"));
        let message = refused.to_string();
        assert!(
            message.contains("400") && message.contains("blob"),
            "{name} is the caller's fault and should name the field: {message}"
        );
        assert!(
            message.contains("is not a byte"),
            "{name} should be refused for the element, not the shape: {message}"
        );
    }

    // A scalar is still the wrong shape entirely, and says so differently.
    let refused = client
        .write_document("blobs", "b1", &json!({"id": "b1", "blob": 5}), None)
        .await
        .expect_err("a scalar is not a byte array");
    assert!(
        refused.to_string().contains("expected Bytes"),
        "a scalar is a type mismatch: {refused}"
    );

    // What a bytes field is for, and the empty case, which is the absence of a value.
    for (id, value) in [("ok", json!([104, 105, 0, 255])), ("empty", json!([]))] {
        client
            .write_document("blobs", id, &json!({"id": id, "blob": value.clone()}), None)
            .await
            .unwrap_or_else(|e| panic!("{value} should be storable: {e}"));
    }

    let found = client
        .search("blobs", "id:ok", Some(10), None, None, None)
        .await
        .expect("the document reads back");
    let hits = found["hits"].as_array().expect("hits array");
    assert_eq!(hits.len(), 1, "the document just written should come back");
    assert_eq!(
        hits[0]["blob"],
        json!([104, 105, 0, 255]),
        "and reads back exactly as written: {found}"
    );
}

/// A bulk write accounts for every item it received.
///
/// `items_written` plus one reason per document that was not written equals `items_received`,
/// and each reason names the document by the position the caller used. Held as arithmetic
/// rather than as a description because the paths that break it break it quietly: a document
/// that routes nowhere, a shard that will not take its batch, a peer that refuses half of what
/// it was sent were each logged and dropped, leaving a 200 with a shortfall and nothing to
/// explain it. A loader reading `errors` to decide what to retry saw nothing to retry.
#[tokio::test]
async fn a_bulk_write_accounts_for_every_item_it_received() {
    let node = TestNode::start("").await;

    let (status, body) = put_config(
        &node,
        "ledger",
        &json!({
            "fields": {
                "id": {"field_type": "text", "indexed": true},
                "n": {"field_type": "i64", "indexed": true}
            }
        }),
    )
    .await;
    assert_eq!(status, 200, "declaring the config should succeed: {body}");

    let (status, body) = post_json(
        &node,
        "/api/ledger/_bulk",
        json!([
            {"id": "a", "doc": {"n": 1}},
            {"id": "b", "doc": {"n": "twelve"}},
            {"id": "",  "doc": {"n": 3}},
            {"id": "d", "doc": {"n": 4}},
            {"id": "e", "doc": {"n": [5, "six"]}}
        ]),
    )
    .await;
    assert_eq!(status, 200, "a bulk write reports partial success: {body}");

    let received = body["items_received"].as_u64().expect("items_received");
    let written = body["items_written"].as_u64().expect("items_written");
    let errors = body["errors"].as_array().expect("errors array");

    assert_eq!(received, 5, "every item is counted as received: {body}");
    assert_eq!(written, 2, "the two storable documents are written: {body}");
    assert_eq!(
        written + errors.len() as u64,
        received,
        "and each of the rest has exactly one reason: {body}"
    );

    // Every reason names its document by the position in the batch as sent, which is what makes
    // one addressable once the batch is gone.
    let mut positions: Vec<u64> = errors
        .iter()
        .filter_map(|e| e.as_str())
        .filter_map(|e| e.strip_prefix("document "))
        .filter_map(|rest| rest.split_once(": "))
        .filter_map(|(position, _)| position.parse().ok())
        .collect();
    positions.sort_unstable();
    assert_eq!(
        positions,
        vec![1, 2, 4],
        "the refused documents are named by position: {body}"
    );
}

/// An NDJSON import survives a line it cannot use, and names the line by its place in the file.
///
/// Micro-batches commit as the body is read, so aborting on a bad line left documents written
/// that the response never reported — a 400, no counts, and nothing to resume from. And the
/// reasons that did come back were numbered against the micro-batch, so `document 3` meant the
/// fourth document of some five hundred and pointed at nothing an operator could open.
#[tokio::test]
async fn a_write_stream_reports_a_bad_line_and_loads_the_rest() {
    // A batch size of two, so the failures land in different micro-batches and the numbering has
    // something to get wrong.
    let node = TestNode::start("stream_batch_size = 2").await;

    let (status, body) = put_config(
        &node,
        "feed",
        &json!({
            "fields": {
                "id": {"field_type": "text", "indexed": true},
                "n": {"field_type": "i64", "indexed": true}
            }
        }),
    )
    .await;
    assert_eq!(status, 200, "declaring the config should succeed: {body}");

    // Line 3 is blank, line 5 will not parse, line 7 parses but the schema refuses it.
    let ndjson = concat!(
        "{\"id\":\"a\",\"doc\":{\"n\":1}}\n",
        "{\"id\":\"b\",\"doc\":{\"n\":2}}\n",
        "\n",
        "{\"id\":\"c\",\"doc\":{\"n\":3}}\n",
        "{\"id\":\"d\",\"doc\":{\"n\":\n",
        "{\"id\":\"e\",\"doc\":{\"n\":5}}\n",
        "{\"id\":\"f\",\"doc\":{\"n\":\"six\"}}\n",
        "{\"id\":\"g\",\"doc\":{\"n\":7}}\n",
    );

    let (status, body) = post_ndjson(&node, "/api/feed/document/stream", ndjson).await;
    assert_eq!(status, 200, "a bad line does not fail the import: {body}");
    assert_eq!(body["status"], "partial", "and it is reported: {body}");

    let received = body["lines_received"].as_u64().expect("lines_received");
    let written = body["items_written"].as_u64().expect("items_written");
    let errors = body["errors"].as_array().expect("errors array");

    assert_eq!(received, 7, "the blank line is not a document: {body}");
    assert_eq!(written, 5, "every usable document is loaded: {body}");
    assert_eq!(
        written + errors.len() as u64,
        received,
        "and each of the rest has exactly one reason: {body}"
    );

    // Physical lines, blank one included, so `line 5` is what `sed -n 5p` prints.
    let mut lines: Vec<u64> = errors
        .iter()
        .filter_map(|e| e.as_str())
        .filter_map(|e| e.strip_prefix("line "))
        .filter_map(|rest| rest.split_once(": "))
        .filter_map(|(line, _)| line.parse().ok())
        .collect();
    lines.sort_unstable();
    assert_eq!(
        lines,
        vec![5, 7],
        "the unusable lines are named where they sit in the file: {body}"
    );

    // The documents really are there, not merely counted.
    let client = node.client();
    client.admin_index_commit("feed").await.expect("commit");
    let found = client
        .search("feed", "n:[1 TO 7]", Some(10), None, None, None)
        .await
        .expect("search");
    assert_eq!(
        found["hits"].as_array().map(|h| h.len()),
        Some(5),
        "the loaded documents are searchable: {found}"
    );
}

/// A line larger than one record may be is skipped, not fatal, and the file keeps loading.
#[tokio::test]
async fn a_write_stream_skips_an_oversized_line() {
    let node = TestNode::start("").await;

    // One line past the default 64 MB record ceiling, between two ordinary ones.
    let big = "x".repeat(70 * 1024 * 1024);
    let ndjson = format!(
        "{{\"id\":\"a\",\"doc\":{{\"t\":\"one\"}}}}\n\
         {{\"id\":\"big\",\"doc\":{{\"t\":\"{big}\"}}}}\n\
         {{\"id\":\"c\",\"doc\":{{\"t\":\"three\"}}}}\n"
    );

    let (status, body) = post_ndjson(&node, "/api/huge/document/stream", &ndjson).await;
    assert_eq!(
        status, 200,
        "an oversized line does not fail the import: {body}"
    );
    assert_eq!(body["items_written"], 2, "the ordinary lines load: {body}");

    let errors = body["errors"].as_array().expect("errors array");
    assert_eq!(errors.len(), 1, "and only the big one is refused: {body}");
    assert!(
        errors[0]
            .as_str()
            .is_some_and(|e| e.starts_with("line 2:") && e.contains("single-record limit")),
        "named by its line and its reason: {body}"
    );
}

/// A body that is not NDJSON at all is the wrong request, not a partial success.
#[tokio::test]
async fn a_body_that_is_not_ndjson_is_refused_outright() {
    let node = TestNode::start("").await;

    let (status, body) = post_ndjson(
        &node,
        "/api/notndjson/document/stream",
        "[{\"id\":\"a\"},\n{\"id\":\"b\"}]\n",
    )
    .await;
    assert_eq!(
        status, 400,
        "nothing parsed and nothing was written, so the body was the wrong shape: {body}"
    );
}

/// POST a raw NDJSON body, returning the status and the decoded response.
async fn post_ndjson(node: &TestNode, path: &str, body: &str) -> (u16, serde_json::Value) {
    with_tls_provider();
    let resp = reqwest::Client::new()
        .post(format!("{}{path}", node.url))
        .header("content-type", "application/x-ndjson")
        .body(body.to_string())
        .send()
        .await
        .expect("request");
    let status = resp.status().as_u16();
    (status, resp.json().await.unwrap_or(json!(null)))
}

/// A date field takes a timestamp counted as well as one written.
///
/// `infer_field_type` only knows the written form — it reads a number as an integer — so a
/// declared date field refused every epoch timestamp, which is the shape most exporters emit.
/// Seconds, never milliseconds: guessing the unit by magnitude turns a date in 2033 into one in
/// 1970 with nothing to show for it.
#[tokio::test]
async fn a_date_field_takes_epoch_seconds() {
    let node = TestNode::start("").await;
    let client = node.client();

    let (status, body) = put_config(
        &node,
        "events",
        &json!({
            "fields": {
                "id": {"field_type": "text", "indexed": true},
                "at": {"field_type": "date", "indexed": true}
            }
        }),
    )
    .await;
    assert_eq!(status, 200, "declaring a date field should succeed: {body}");

    // The same instant, counted and written.
    client
        .write_document(
            "events",
            "counted",
            &json!({"id": "counted", "at": 1767225600}),
            None,
        )
        .await
        .expect("epoch seconds are a date");
    client
        .write_document(
            "events",
            "written",
            &json!({"id": "written", "at": "2026-01-01T00:00:00Z"}),
            None,
        )
        .await
        .expect("a formatted timestamp is a date");

    client.admin_index_commit("events").await.expect("commit");

    let found = client
        .search(
            "events",
            "at:[2025-12-31T00:00:00Z TO 2026-01-02T00:00:00Z]",
            Some(10),
            None,
            None,
            None,
        )
        .await
        .expect("search");
    assert_eq!(
        found["hits"].as_array().map(|h| h.len()),
        Some(2),
        "both spellings land on the same day and are found by a range: {found}"
    );

    // A word is still not a date, and neither is a fraction of a second.
    for value in [json!("yesterday"), json!(1.5)] {
        client
            .write_document(
                "events",
                "bad",
                &json!({"id": "bad", "at": value.clone()}),
                None,
            )
            .await
            .expect_err(&format!("{value} is not a date"));
    }
}

/// A field first seen holding a list is typed by what the list holds.
///
/// Every tantivy field is multivalued, so a list of numbers is a numeric field with several
/// values in it — the reading the rest of the write path already takes. Typing the list itself
/// as text made the two disagree on the case that matters: a numeric list arriving at a field
/// nobody had declared produced a text field, and no range query ever matched it again.
#[tokio::test]
async fn a_new_field_holding_a_list_is_typed_by_its_elements() {
    let node = TestNode::start("").await;
    let client = node.client();

    client
        .write_document(
            "scores",
            "s1",
            &json!({
                "id": "s1",
                "risk": [9, 12],
                "flags": [true, false],
                "mixed": [1, "two"],
                "nested": [[1, 2]],
                "sparse": [9, null, 12]
            }),
            None,
        )
        .await
        .expect("write");

    let schema = get_json(&node, "/api/scores/_config").await;
    let typed = |name: &str| -> Option<String> {
        schema["fields"]
            .as_array()?
            .iter()
            .find(|f| f["name"] == name)?["type"]
            .as_str()
            .map(str::to_string)
    };

    assert_eq!(typed("risk").as_deref(), Some("i64"), "{schema}");
    assert_eq!(typed("flags").as_deref(), Some("boolean"), "{schema}");
    assert_eq!(
        typed("sparse").as_deref(),
        Some("i64"),
        "a null stores nothing, so it is not evidence against the type: {schema}"
    );
    assert_eq!(
        typed("mixed").as_deref(),
        Some("text"),
        "elements that do not agree leave text, the only type holding both: {schema}"
    );
    assert_eq!(
        typed("nested").as_deref(),
        Some("text"),
        "the writer flattens one level, so a list of lists has no element type: {schema}"
    );

    // The point of all this: the numeric list is range-queryable.
    client.admin_index_commit("scores").await.expect("commit");
    let found = client
        .search("scores", "risk:[10 TO 20]", Some(10), None, None, None)
        .await
        .expect("search");
    assert_eq!(
        found["hits"].as_array().map(|h| h.len()),
        Some(1),
        "a range matches the document on its second value: {found}"
    );
}
