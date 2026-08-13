//! What a federated MCP search actually returns, over the real endpoint.
//!
//! The merge in `search_indexes` orders hits by the internal `_sort_key` the engine stamps on
//! a field-sorted search. Whether that key is still present when the merge runs is a property
//! of the routing path, which no unit test on the comparator can see — so this drives
//! `search_across_indexes` against a live node holding two indexes that share a sortable date
//! field, and asserts on the order of what comes back.
//!
//! Interleaving is the assertion that matters. A merge that ordered each index's block
//! correctly and then concatenated the blocks passes a naive monotonicity check on a single
//! index, so the documents below are laid out to make per-index blocking visible: the correct
//! answer alternates between the two indexes on every hit.

use std::io::Write as _;
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use serde_json::{Value, json};

struct TestNode {
    child: Child,
    url: String,
    _dir: tempfile::TempDir,
}

impl TestNode {
    async fn start() -> TestNode {
        TestNode::start_with("").await
    }

    async fn start_with(extra: &str) -> TestNode {
        let dir = tempfile::tempdir().expect("temp dir");
        let port = free_port();
        let data = dir.path().join("data");
        std::fs::create_dir_all(&data).expect("data dir");

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
num_shards_init = 1
max_shards_per_node = 1
{extra}
"#,
            data = data.display().to_string().replace('\\', "/"),
        );
        let config_path = dir.path().join("cameodb.toml");
        std::fs::File::create(&config_path)
            .expect("config file")
            .write_all(config.as_bytes())
            .expect("write config");

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
        let client = http();
        let deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < deadline {
            if let Ok(resp) = client
                .get(format!("{}/_cluster/health", self.url))
                .send()
                .await
                && resp.status().is_success()
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("node at {} never became healthy", self.url);
    }

    /// An index whose `created` field is a date the engine can sort on.
    ///
    /// `fast` is set explicitly: it defaults to false over the wire, and a sort on a date field
    /// that is not FAST is refused rather than ordered — which would make this test pass for
    /// the wrong reason.
    async fn create_index(&self, index: &str) {
        let status = http()
            .put(format!("{}/api/{index}/_config", self.url))
            .json(&json!({
                "fields": {
                    "title": {"field_type": "text", "indexed": true},
                    "created": {"field_type": "date", "indexed": true, "fast": true},
                }
            }))
            .send()
            .await
            .expect("create config")
            .status();
        assert!(status.is_success(), "creating '{index}' failed: {status}");
    }

    /// Write `documents` as `(id, created)` pairs, then wait until they are searchable.
    async fn seed(&self, index: &str, documents: &[(&str, &str)]) {
        for (id, created) in documents {
            let status = http()
                .put(format!("{}/api/{index}/document", self.url))
                .json(&json!({
                    "id": id,
                    "doc": {"id": id, "title": "quarterly record", "created": created}
                }))
                .send()
                .await
                .expect("write")
                .status();
            assert!(status.is_success(), "seeding {id} failed: {status}");
        }

        // The write path schedules a commit rather than performing one, so poll for visibility
        // instead of sleeping.
        let deadline = Instant::now() + Duration::from_secs(20);
        while Instant::now() < deadline {
            let resp = http()
                .post(format!("{}/api/{index}/search", self.url))
                .json(&json!({"query": "title:record", "limit": 10}))
                .send()
                .await
                .expect("search");
            let body: Value = resp.json().await.expect("search json");
            if body["total_hits"].as_u64() == Some(documents.len() as u64) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("seeded documents never became searchable in '{index}'");
    }

    /// One `tools/call`, returning `(isError, parsed result text)`.
    async fn call_tool(&self, tool: &str, arguments: Value) -> (bool, Value) {
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": tool, "arguments": arguments},
        });
        let resp = http()
            .post(format!("{}/mcp", self.url))
            .header("content-type", "application/json")
            .header("accept", "application/json, text/event-stream")
            .json(&body)
            .send()
            .await
            .expect("mcp post");
        let value: Value = resp.json().await.expect("mcp json");
        let result = &value["result"];
        let is_error = result["isError"].as_bool().unwrap_or(false);
        let text = result["content"][0]["text"].as_str().unwrap_or("");
        let parsed = serde_json::from_str(text).unwrap_or(Value::String(text.to_string()));
        (is_error, parsed)
    }
}

impl Drop for TestNode {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn http() -> reqwest::Client {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
    reqwest::Client::new()
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral")
        .local_addr()
        .expect("local addr")
        .port()
}

/// Two indexes whose documents alternate in the sort field, so any per-index blocking shows up.
const ALPHA: &[(&str, &str)] = &[
    ("a1", "2024-01-01T00:00:00Z"),
    ("a2", "2024-03-01T00:00:00Z"),
    ("a3", "2024-05-01T00:00:00Z"),
];
const BETA: &[(&str, &str)] = &[
    ("b1", "2024-02-01T00:00:00Z"),
    ("b2", "2024-04-01T00:00:00Z"),
    ("b3", "2024-06-01T00:00:00Z"),
];

async fn two_seeded_indexes() -> TestNode {
    let node = TestNode::start().await;
    node.create_index("alpha").await;
    node.create_index("beta").await;
    node.seed("alpha", ALPHA).await;
    node.seed("beta", BETA).await;
    node
}

/// The order the merge promises. Descending by `created` across both indexes means the answer
/// alternates beta, alpha, beta, alpha… — and every hit still names the index it came from.
#[tokio::test]
async fn a_federated_sort_orders_across_indexes_rather_than_within_them() {
    let node = two_seeded_indexes().await;

    let (is_error, result) = node
        .call_tool(
            "search_indexes",
            json!({
                "indexes": [
                    {"index": "alpha", "sort": {"field": "created", "order": "desc"}},
                    {"index": "beta", "sort": {"field": "created", "order": "desc"}},
                ],
                "query": "title:record",
                "limit": 10,
            }),
        )
        .await;
    assert!(!is_error, "federated search failed: {result}");

    let hits = result["hits"].as_array().expect("hits array");
    assert_eq!(hits.len(), 6, "expected every seeded document: {result}");

    let observed: Vec<(String, String)> = hits
        .iter()
        .map(|hit| {
            (
                hit["_index_source"]
                    .as_str()
                    .unwrap_or("<missing>")
                    .to_string(),
                hit["created"].as_str().unwrap_or("<missing>").to_string(),
            )
        })
        .collect();

    let expected = [
        ("beta", "2024-06-01T00:00:00Z"),
        ("alpha", "2024-05-01T00:00:00Z"),
        ("beta", "2024-04-01T00:00:00Z"),
        ("alpha", "2024-03-01T00:00:00Z"),
        ("beta", "2024-02-01T00:00:00Z"),
        ("alpha", "2024-01-01T00:00:00Z"),
    ];
    for (position, (want_index, want_created)) in expected.iter().enumerate() {
        let (got_index, got_created) = &observed[position];
        assert_eq!(
            (got_index.as_str(), got_created.as_str()),
            (*want_index, *want_created),
            "hit {position} is out of order; whole sequence: {observed:?}"
        );
    }
}

/// Ascending is the other direction of the same wire, and the default when no order is given.
#[tokio::test]
async fn a_federated_sort_honours_ascending_order() {
    let node = two_seeded_indexes().await;

    let (is_error, result) = node
        .call_tool(
            "search_indexes",
            json!({
                "indexes": [
                    {"index": "alpha", "sort": {"field": "created"}},
                    {"index": "beta", "sort": {"field": "created"}},
                ],
                "query": "title:record",
                "limit": 10,
            }),
        )
        .await;
    assert!(!is_error, "federated search failed: {result}");

    let created: Vec<String> = result["hits"]
        .as_array()
        .expect("hits array")
        .iter()
        .map(|hit| hit["created"].as_str().unwrap_or("<missing>").to_string())
        .collect();

    let mut sorted = created.clone();
    sorted.sort();
    assert_eq!(
        created, sorted,
        "ascending federated sort returned an unordered sequence"
    );
}

/// `_sort_key` is internal. It has to survive the routing path far enough to drive the merge,
/// and it must not reach the caller.
#[tokio::test]
async fn the_internal_sort_key_does_not_reach_the_caller() {
    let node = two_seeded_indexes().await;

    let (_, result) = node
        .call_tool(
            "search_indexes",
            json!({
                "indexes": [
                    {"index": "alpha", "sort": {"field": "created", "order": "desc"}},
                    {"index": "beta", "sort": {"field": "created", "order": "desc"}},
                ],
                "query": "title:record",
                "limit": 10,
            }),
        )
        .await;

    for hit in result["hits"].as_array().expect("hits array") {
        assert!(
            hit.get("_sort_key").is_none(),
            "a hit carried the internal sort key to the caller: {hit}"
        );
    }
}

/// Enough documents on each side that the node's default limit of 10 is not the answer.
const WIDE: &[(&str, &str)] = &[
    ("w1", "2024-01-02T00:00:00Z"),
    ("w2", "2024-01-04T00:00:00Z"),
    ("w3", "2024-01-06T00:00:00Z"),
    ("w4", "2024-01-08T00:00:00Z"),
    ("w5", "2024-01-10T00:00:00Z"),
    ("w6", "2024-01-12T00:00:00Z"),
    ("w7", "2024-01-14T00:00:00Z"),
    ("w8", "2024-01-16T00:00:00Z"),
];

/// An inline `limit` has to bound the merge, not just the per-index searches feeding it.
///
/// Sixteen documents and a request for all sixteen, with no `limit` argument — so the only
/// limit in play is the inline one. The merge derived its truncation point separately from the
/// value it asked each index for, and only the latter saw the inline clause.
#[tokio::test]
async fn an_inline_limit_bounds_the_federated_merge() {
    let node = TestNode::start().await;
    node.create_index("wide-alpha").await;
    node.create_index("wide-beta").await;
    node.seed("wide-alpha", WIDE).await;
    node.seed("wide-beta", WIDE).await;

    let (is_error, result) = node
        .call_tool(
            "search_indexes",
            json!({
                "indexes": [{"index": "wide-alpha"}, {"index": "wide-beta"}],
                "query": "title:record limit 16",
            }),
        )
        .await;
    assert!(!is_error, "federated search failed: {result}");

    assert_eq!(
        result["hits"].as_array().map(Vec::len),
        Some(16),
        "an inline limit of 16 was not applied to the merge: {result}"
    );
    // The number reported back has to be the number that was honoured, or an agent paging
    // through results is reasoning from a limit the server did not use.
    assert_eq!(
        result["limit"].as_u64(),
        Some(16),
        "the response reported a limit it did not apply: {result}"
    );
}

/// With no limit anywhere, the merge falls back to the node's configured default rather than to
/// a number of its own.
///
/// Configured to 3 rather than left at 10, because a test that agrees with the hard-coded value
/// cannot tell the two apart — the single-index path passes `None` down and gets the configured
/// number, while the merge used a literal.
#[tokio::test]
async fn the_federated_merge_falls_back_to_the_configured_default() {
    let node = TestNode::start_with("\n[search]\ndefault_search_limit = 3\n").await;
    node.create_index("wide-alpha").await;
    node.seed("wide-alpha", WIDE).await;

    let (is_error, result) = node
        .call_tool(
            "search_indexes",
            json!({
                "indexes": [{"index": "wide-alpha"}],
                "query": "title:record",
            }),
        )
        .await;
    assert!(!is_error, "federated search failed: {result}");

    assert_eq!(
        result["hits"].as_array().map(Vec::len),
        Some(3),
        "the merge ignored the node's configured default: {result}"
    );
    assert_eq!(result["limit"].as_u64(), Some(3));

    // The single-index tool reads the same configured number, which is the agreement that was
    // missing: one tool honoured the operator's setting and the other did not.
    let (_, single) = node
        .call_tool(
            "search_index",
            json!({"index": "wide-alpha", "query": "title:record"}),
        )
        .await;
    assert_eq!(
        single["hits"].as_array().map(Vec::len),
        Some(3),
        "single-index search disagreed with the configured default: {single}"
    );
}

/// A single-index search must be unaffected by the federated path keeping the key.
#[tokio::test]
async fn a_single_index_search_still_strips_the_sort_key() {
    let node = two_seeded_indexes().await;

    let (is_error, result) = node
        .call_tool(
            "search_index",
            json!({"index": "alpha", "query": "title:record sort created:desc", "limit": 10}),
        )
        .await;
    assert!(!is_error, "single-index search failed: {result}");

    for hit in result["hits"].as_array().expect("hits array") {
        assert!(
            hit.get("_sort_key").is_none(),
            "a single-index hit carried the internal sort key: {hit}"
        );
    }
}
