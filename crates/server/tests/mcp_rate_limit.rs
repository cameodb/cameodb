//! `[security.limits]` enforced through the real MCP endpoint (Phase 14, C1).
//!
//! The unit tests in `ratelimit.rs` cover the bucket arithmetic. What they cannot show is
//! that the bucket is actually *consulted* — that the config reaches `AppState`, that the
//! dispatcher asks before running a tool, and that a refusal comes back in the shape an MCP
//! client understands. Every one of those is a wire between components, and a wire is
//! exactly what a unit test cannot see.
//!
//! MCP is JSON-RPC over POST with no SDK method, so these speak it directly.

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
    async fn start(extra: &str) -> TestNode {
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

    /// One `tools/call`, returning the tool result content as text.
    ///
    /// Rate limiting refuses inside a *successful* JSON-RPC response with `isError: true`,
    /// which is how MCP reports a tool that would not run — the transport succeeded, the
    /// tool did not. A JSON-RPC error would say the request was malformed, which it is not.
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

/// The default must be inert end to end, not just in the bucket. A node that shipped this
/// code and started refusing an agent's calls without being configured to would be a
/// regression indistinguishable from a fault.
#[tokio::test]
async fn tool_calls_are_unlimited_by_default() {
    let node = TestNode::start("").await;
    for i in 0..25 {
        let (is_error, text) = node.call_tool("list_indexes", json!({})).await;
        assert!(
            !text.contains("Rate limit"),
            "call {i} was rate limited on a node with no limits configured: {text}"
        );
        assert!(!is_error, "call {i} failed: {text}");
    }
}

/// The whole wire, exercised: config file -> `SecurityConfig::limits` -> `AppState` ->
/// the `tools/call` arm -> a refusal an MCP client can act on.
#[tokio::test]
async fn a_configured_burst_is_spendable_and_then_refused() {
    let node = TestNode::start(
        r#"
[security]
enabled = false

[security.limits]
tool_calls_per_minute = 60
tool_call_burst = 3
"#,
    )
    .await;

    for i in 0..3 {
        let (_, text) = node.call_tool("list_indexes", json!({})).await;
        assert!(
            !text.contains("Rate limit"),
            "call {i} is inside the configured burst of 3, but was refused: {text}"
        );
    }

    let (is_error, text) = node.call_tool("list_indexes", json!({})).await;
    assert!(
        is_error && text.contains("Rate limit exceeded"),
        "the 4th call should be refused past a burst of 3, got isError={is_error} {text}"
    );
    assert!(
        text.contains("Retry after"),
        "a refusal must tell the caller how long to wait, got: {text}"
    );
}

/// Refusal happens before the tool runs, so it applies to every tool rather than to the one
/// that happened to be metered. Spending the budget on one tool must refuse a different one.
#[tokio::test]
async fn the_budget_is_shared_across_tools() {
    let node = TestNode::start(
        r#"
[security]
enabled = false

[security.limits]
tool_calls_per_minute = 60
tool_call_burst = 2
"#,
    )
    .await;

    for _ in 0..2 {
        node.call_tool("list_indexes", json!({})).await;
    }

    let (is_error, text) = node
        .call_tool("validate_query", json!({"query": "title:x"}))
        .await;
    assert!(
        is_error && text.contains("Rate limit exceeded"),
        "a different tool should draw on the same budget, got isError={is_error} {text}"
    );
}

/// A federated search is charged for every index it names, not once for the call.
///
/// One authorized call otherwise buys as many concurrent index searches as the caller cares to
/// name, which makes a per-key budget a count of requests rather than of work — and work is
/// what the limiter exists to bound. The indexes need not exist: the charge is taken from the
/// raw arguments before the tool runs, which is the same ordering that keeps a refused call
/// costing a hash lookup rather than a search.
#[tokio::test]
async fn a_federated_search_is_charged_for_its_fan_out() {
    let node = TestNode::start(
        r#"
[security]
enabled = false

[security.limits]
tool_calls_per_minute = 60
tool_call_burst = 3
"#,
    )
    .await;

    // Three indexes named, three tokens spent — the whole burst on one call.
    node.call_tool(
        "search_indexes",
        json!({
            "indexes": [{"index": "a"}, {"index": "b"}, {"index": "c"}],
            "query": "x",
        }),
    )
    .await;

    let (is_error, text) = node.call_tool("list_indexes", json!({})).await;
    assert!(
        is_error && text.contains("Rate limit exceeded"),
        "a three-index fan-out should have spent a burst of three, got isError={is_error} {text}"
    );
}
