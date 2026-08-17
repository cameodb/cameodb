//! What each caller does with a dropped clause, over the real endpoints.
//!
//! `discarded_clauses_test.rs` covers what the engine detects. This covers the wiring that
//! carries it out — shard reply, per-node merge, response JSON — and the two contracts built on
//! it: MCP returns a tool execution error, while the HTTP API returns the hits with
//! `_discarded_clauses` attached.
//!
//! Also covers `validate_query`, which answers the same question before a search rather than
//! after one — including the case it previously could not see at all, a query whose quotes and
//! parentheses balance and which still does not parse.

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
            .header("mcp-protocol-version", "2025-06-18")
            .json(&body)
            .send()
            .await
            .expect("mcp post");
        let value: Value = resp.json().await.expect("mcp json");
        let result = &value["result"];
        let is_error = result["isError"].as_bool().unwrap_or(false);
        // Every result travels in the text block, whether it is data or a message.
        let text = result["content"][0]["text"]
            .as_str()
            .unwrap_or("")
            .to_string();
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

/// The case a structural check cannot reach: `title:` balances perfectly and does not parse.
///
/// The tool used to answer this with a structural pass that had already succeeded, which is
/// exactly the situation its own description sends an agent into after a search fails.
#[tokio::test]
async fn validate_query_catches_a_query_that_balances_but_does_not_parse() {
    let node = TestNode::start().await;
    node.seed("docs").await;

    let query = "title:";
    assert_eq!(
        query.chars().filter(|c| *c == '"').count() % 2,
        0,
        "the point of this case is that a structural check passes it"
    );

    let (is_error, text) = node
        .call_tool("validate_query", json!({"index": "docs", "query": query}))
        .await;
    assert!(!is_error, "validation itself should succeed: {text}");

    let body: Value = serde_json::from_str(&text).expect("structured result");
    let analysis = &body["query_analysis"];

    assert_eq!(
        analysis["parses"],
        json!(false),
        "`{query}` does not parse, and validation should say so: {analysis}"
    );
    assert!(
        analysis["syntax_errors"]
            .as_array()
            .is_some_and(|errors| !errors.is_empty()),
        "a malformed query should carry the parser's errors: {analysis}"
    );
}

/// A well-formed query is reported as parsing, and carries the form the engine will run.
#[tokio::test]
async fn validate_query_reports_the_query_the_engine_will_run() {
    let node = TestNode::start().await;
    node.seed("docs").await;

    let (is_error, text) = node
        .call_tool(
            "validate_query",
            json!({"index": "docs", "query": "title:rust"}),
        )
        .await;
    assert!(!is_error, "{text}");

    let body: Value = serde_json::from_str(&text).expect("structured result");
    let analysis = &body["query_analysis"];

    assert_eq!(analysis["parses"], json!(true), "{analysis}");
    assert_eq!(
        analysis["normalized_query"], "title:rust",
        "an unrewritten query normalizes to itself: {analysis}"
    );
}

/// What validation predicts is what the search does.
///
/// A clause naming a field the index does not have is refused by `search_index`; validation has
/// to name the same clause beforehand, or an agent that checks first learns nothing.
#[tokio::test]
async fn validate_query_names_the_clause_the_search_would_refuse() {
    let node = TestNode::start().await;
    node.seed("docs").await;

    let query = "nosuchfield:rust";

    let (validation_failed, validation_text) = node
        .call_tool("validate_query", json!({"index": "docs", "query": query}))
        .await;
    assert!(!validation_failed, "{validation_text}");

    let body: Value = serde_json::from_str(&validation_text).expect("structured result");
    let analysis = &body["query_analysis"];

    assert_eq!(
        analysis["parses"],
        json!(true),
        "this query parses; what is wrong with it is semantic: {analysis}"
    );
    let discarded = analysis["discarded_clauses"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert!(
        discarded
            .iter()
            .any(|note| note.as_str().is_some_and(|n| n.contains("nosuchfield"))),
        "validation should name the clause that cannot match: {analysis}"
    );

    // And the search it was predicting does refuse.
    let (search_failed, _) = node
        .call_tool("search_index", json!({"index": "docs", "query": query}))
        .await;
    assert!(
        search_failed,
        "the search validation warned about should be the one that fails"
    );
}

/// A declared but empty index still validates, because `PUT /_config` builds the Tantivy index
/// rather than waiting for the first write.
///
/// That is what lets an agent check a query before there is anything to find with it. The
/// unbuilt case — a schema with no index behind it — is reachable below the HTTP layer and is
/// covered in `crates/storage/tests/query_validation_test.rs`; what matters here is that the
/// ordinary path does not fall into it and answer `null` for every new index.
#[tokio::test]
async fn validate_query_works_on_a_declared_index_with_no_documents() {
    let node = TestNode::start().await;
    node.seed("docs").await;

    let status = http()
        .put(format!("{}/api/empty/_config", node.url))
        .json(&json!({
            "fields": {
                "title": {"name": "title", "field_type": "text", "indexed": true}
            }
        }))
        .send()
        .await
        .expect("create config")
        .status();
    assert!(
        status.is_success(),
        "creating the empty index failed: {status}"
    );

    // Malformed, against an index holding nothing: the verdict comes from the schema, not from
    // the documents, so it is available immediately.
    let (is_error, text) = node
        .call_tool(
            "validate_query",
            json!({"index": "empty", "query": "title:"}),
        )
        .await;
    assert!(!is_error, "{text}");

    let body: Value = serde_json::from_str(&text).expect("structured result");
    let analysis = &body["query_analysis"];
    assert_eq!(
        analysis["parses"],
        json!(false),
        "an empty index can still tell a malformed query from a good one: {analysis}"
    );

    // And the well-formed counterpart passes, so the above is not just a blanket refusal.
    let (_, text) = node
        .call_tool(
            "validate_query",
            json!({"index": "empty", "query": "title:rust"}),
        )
        .await;
    let body: Value = serde_json::from_str(&text).expect("structured result");
    assert_eq!(body["query_analysis"]["parses"], json!(true));
}
