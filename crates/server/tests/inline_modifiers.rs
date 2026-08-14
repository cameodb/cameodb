//! Inline `return`, `limit` and `sort` clauses over the real endpoints.
//!
//! The parser's own rules are unit-tested next to it. This covers the half it cannot decide: a
//! keyword whose argument reads as a field name is only distinguishable from prose against the
//! schema, so it is reported from the search path — an error for MCP, a note for HTTP.

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

    /// One document with a `title` and a `tag`, so `forms` is a field the index does not have.
    async fn seed(&self, index: &str) {
        let status = http()
            .put(format!("{}/api/{index}/document", self.url))
            .json(&json!({
                "id": "d1",
                "doc": {"id": "d1", "title": "rust programming", "tag": "active"}
            }))
            .send()
            .await
            .expect("write")
            .status();
        assert!(status.is_success(), "seeding failed: {status}");

        // The write path schedules a commit rather than performing one, so poll for visibility.
        let deadline = Instant::now() + Duration::from_secs(20);
        while Instant::now() < deadline {
            let (_, body) = self.http_search(index, "tag:active").await;
            if body["total_hits"].as_u64() == Some(1) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("seeded document never became searchable in '{index}'");
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

/// `find tax return forms` parses as a projection of `forms`, which the schema then contradicts.
#[tokio::test]
async fn mcp_refuses_a_modifier_naming_a_field_the_index_does_not_have() {
    let node = TestNode::start().await;
    node.seed("docs").await;

    // Control: a projection of real fields must succeed.
    let (is_error, text) = node
        .call_tool(
            "search_index",
            json!({"index": "docs", "query": "title:rust return title,tag"}),
        )
        .await;
    assert!(!is_error, "the control projection must succeed: {text}");

    for (label, query) in [
        ("projection", "title:rust return forms"),
        ("sort", "title:rust sort forms"),
    ] {
        let (is_error, text) = node
            .call_tool("search_index", json!({"index": "docs", "query": query}))
            .await;
        assert!(
            is_error,
            "{label}: MCP answered a query whose modifier named a missing field: {text}"
        );
        assert!(
            text.contains("forms"),
            "{label}: the refusal must name the field, got: {text}"
        );
    }
}

/// The HTTP contract is unchanged by this check: hits plus a note, never a failure.
#[tokio::test]
async fn the_http_api_reports_a_modifier_naming_a_missing_field() {
    let node = TestNode::start().await;
    node.seed("docs").await;

    let (status, body) = node.http_search("docs", "title:rust return forms").await;
    assert_eq!(status, 200, "HTTP search must not start failing: {body}");
    assert_eq!(body["total_hits"].as_u64(), Some(1), "{body}");

    let notes = body["_discarded_clauses"]
        .as_array()
        .unwrap_or_else(|| panic!("expected _discarded_clauses in the response: {body}"));
    assert!(
        notes.iter().any(|note| {
            note.as_str()
                .is_some_and(|text| text.contains("return forms"))
        }),
        "the note must quote the clause, got: {notes:?}"
    );
}

/// A keyword that opens no complete run is query text, and query text is not reported.
#[tokio::test]
async fn a_keyword_used_as_a_word_is_searched_for() {
    let node = TestNode::start().await;
    node.seed("docs").await;

    for query in ["rust sort by date", "how to limit costs"] {
        let (status, body) = node.http_search("docs", query).await;
        assert_eq!(status, 200, "{query:?}: {body}");
        assert!(
            body.get("_discarded_clauses").is_none(),
            "{query:?} reported a clause it should have searched for: {body}"
        );
    }
}

/// A projection of real fields still returns those fields, and only those.
#[tokio::test]
async fn a_projection_of_real_fields_still_narrows_the_hits() {
    let node = TestNode::start().await;
    node.seed("docs").await;

    let (status, body) = node.http_search("docs", "title:rust return title").await;
    assert_eq!(status, 200, "{body}");
    assert!(body.get("_discarded_clauses").is_none(), "{body}");

    let hit = &body["hits"][0];
    assert_eq!(hit["title"].as_str(), Some("rust programming"), "{body}");
    assert!(
        hit.get("tag").is_none(),
        "the projection should have dropped `tag`: {body}"
    );
}
