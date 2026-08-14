//! What each caller does with a dropped clause, over the real endpoints.
//!
//! `discarded_clauses_test.rs` covers what the engine detects. This covers the wiring that
//! carries it out — shard reply, per-node merge, response JSON — and the two contracts built on
//! it: MCP returns a tool execution error, while the HTTP API returns the hits with
//! `_discarded_clauses` attached.

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

    /// Two documents, of which exactly one has `tag:active`, so a widened query is visible as
    /// a hit count of two.
    async fn seed(&self, index: &str) {
        for (id, tag) in [("d1", "active"), ("d2", "archived")] {
            let status = http()
                .put(format!("{}/api/{index}/document", self.url))
                .json(&json!({
                    "id": id,
                    "doc": {"id": id, "title": "rust programming", "tag": tag}
                }))
                .send()
                .await
                .expect("write")
                .status();
            assert!(status.is_success(), "seeding {id} failed: {status}");
        }
        // The write path schedules a commit rather than performing one, so poll for
        // visibility instead of sleeping.
        let deadline = Instant::now() + Duration::from_secs(20);
        while Instant::now() < deadline {
            let (_, body) = self.http_search(index, "tag:active").await;
            if body["total_hits"].as_u64() == Some(1) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("seeded documents never became searchable in '{index}'");
    }

    async fn http_search(&self, index: &str, query: &str) -> (u16, Value) {
        let resp = http()
            .post(format!("{}/api/{index}/search", self.url))
            .json(&json!({"query": query, "limit": 10}))
            .send()
            .await
            .expect("search");
        let status = resp.status().as_u16();
        let body: Value = resp.json().await.expect("search json");
        (status, body)
    }

    /// One `tools/call`, returning `(isError, text)`.
    async fn call_tool(&self, tool: &str, arguments: Value) -> (bool, String) {
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
        // A successful result arrives as `structuredContent`; only a failure has a text block,
        // because only a failure is a message rather than data.
        let text = if is_error {
            result["content"][0]["text"]
                .as_str()
                .unwrap_or("")
                .to_string()
        } else {
            serde_json::to_string(&result["structuredContent"]).unwrap_or_default()
        };
        (is_error, text)
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

/// A dropped clause reaches the MCP caller as an error rather than as hits.
#[tokio::test]
async fn mcp_refuses_a_search_whose_clause_was_discarded() {
    let node = TestNode::start().await;
    node.seed("docs").await;

    // Control: the same query without the bad clause must succeed.
    let (is_error, text) = node
        .call_tool(
            "search_index",
            json!({"index": "docs", "query": "tag:active"}),
        )
        .await;
    assert!(!is_error, "the control query must succeed: {text}");

    for (label, query) in [
        ("unknown field", "tag:active AND nosuch:x"),
        ("field-presence test", "tag:active AND title:*"),
        ("dropped negation", "title:rust NOT nosuch:x"),
    ] {
        let (is_error, text) = node
            .call_tool("search_index", json!({"index": "docs", "query": query}))
            .await;
        assert!(
            is_error,
            "{label}: MCP returned results for a query that lost a clause: {text}"
        );
        assert!(
            text.contains("dropped"),
            "{label}: the refusal must say a clause was dropped, got: {text}"
        );
        assert!(
            text.contains("validate_query"),
            "{label}: the refusal must point at the tool that lists valid fields, got: {text}"
        );
    }
}

/// Federated search applies one query string to every index, so a dropped clause affects the
/// whole merge.
#[tokio::test]
async fn federated_search_refuses_rather_than_merging_widened_results() {
    let node = TestNode::start().await;
    node.seed("docs").await;
    node.seed("more").await;

    let (is_error, text) = node
        .call_tool(
            "search_across_indexes",
            json!({
                "indexes": [{"index": "docs"}, {"index": "more"}],
                "query": "tag:active AND nosuch:x"
            }),
        )
        .await;
    assert!(
        is_error,
        "federated search merged hits from a query that lost a clause: {text}"
    );
    assert!(
        text.contains("index '"),
        "the refusal should name which index reported it, got: {text}"
    );
}

/// The HTTP API returns the hits and attaches the list rather than failing the request.
#[tokio::test]
async fn the_http_api_reports_discarded_clauses_instead_of_refusing() {
    let node = TestNode::start().await;
    node.seed("docs").await;

    let (status, body) = node.http_search("docs", "tag:active AND nosuch:x").await;
    assert_eq!(status, 200, "HTTP search must not start failing: {body}");

    let discarded = body["_discarded_clauses"]
        .as_array()
        .unwrap_or_else(|| panic!("expected _discarded_clauses in the response: {body}"));
    assert!(!discarded.is_empty(), "the list must not be empty: {body}");
    let joined = discarded
        .iter()
        .filter_map(|note| note.as_str())
        .collect::<Vec<_>>()
        .join(" | ");
    assert!(
        joined.contains("nosuch"),
        "the note must name the offending field, got: {joined}"
    );

    // A clean parse leaves the key absent rather than present and empty, so presence alone is
    // a usable test.
    let (status, body) = node.http_search("docs", "tag:active").await;
    assert_eq!(status, 200);
    assert!(
        body.get("_discarded_clauses").is_none(),
        "a clean parse must not attach the key at all: {body}"
    );
}
