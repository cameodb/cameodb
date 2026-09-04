//! `[mcp]` enforced through the real transport: session lifetime, and which routes are mounted.
//!
//! The unit tests in `session.rs` cover the registry's own rules — what the sweeper keeps, what
//! eviction drops. What they cannot show is that an operator's `[mcp]` section reaches those
//! rules: that the seconds in a config file become the timeout a paused client is measured
//! against, that a listening stream held open by a real HTTP client counts as proof of life,
//! and that turning a transport off actually unmounts it. Every one of those is a wire between
//! components, and a wire is exactly what a unit test cannot see.
//!
//! The timeouts here are deliberately tiny. A test that waited out the shipped default would
//! take half an hour, so what these assert is that the *configured* number is the one in force
//! — which is the property that matters, and the one a hard-coded constant would fail.

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

    /// `initialize`, returning the session id the server minted for it.
    async fn initialize(&self) -> String {
        let resp = self
            .post_mcp(
                None,
                json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "initialize",
                    "params": {
                        "protocolVersion": "2025-06-18",
                        "capabilities": {},
                        "clientInfo": {"name": "session-test", "version": "0"},
                    },
                }),
            )
            .await;
        assert_eq!(resp.status(), 200, "initialize was refused");
        resp.headers()
            .get("mcp-session-id")
            .expect("initialize returned no session id")
            .to_str()
            .expect("session id is not text")
            .to_string()
    }

    /// One `tools/list` on `session`, returning only the status: whether the server still
    /// holds the session is the whole question here, and that is what the status answers.
    async fn tools_list_status(&self, session: &str) -> u16 {
        self.post_mcp(
            Some(session),
            json!({"jsonrpc": "2.0", "id": 2, "method": "tools/list"}),
        )
        .await
        .status()
        .as_u16()
    }

    async fn post_mcp(&self, session: Option<&str>, body: Value) -> reqwest::Response {
        let mut request = http()
            .post(format!("{}/mcp", self.url))
            .header("content-type", "application/json")
            .header("accept", "application/json, text/event-stream")
            .header("mcp-protocol-version", "2025-06-18");
        if let Some(session) = session {
            request = request.header("mcp-session-id", session);
        }
        request.json(&body).send().await.expect("mcp post")
    }

    /// Open the listening `GET /mcp` stream for `session` and hold it open.
    ///
    /// The returned handle keeps reading the stream — which is what a client establishing a
    /// listening channel does — until it is aborted. Aborting drops the response, which closes
    /// the connection the way a client going away would.
    async fn hold_listening_stream(&self, session: &str) -> tokio::task::JoinHandle<()> {
        let mut resp = http()
            .get(format!("{}/mcp", self.url))
            .header("accept", "text/event-stream")
            .header("mcp-protocol-version", "2025-06-18")
            .header("mcp-session-id", session)
            .send()
            .await
            .expect("listening stream");
        assert_eq!(resp.status(), 200, "the listening stream was refused");

        tokio::spawn(async move {
            // Reading rather than merely holding the response, so this is the same shape as a
            // client that is actually listening. Ends when the connection does.
            while let Ok(Some(_)) = resp.chunk().await {}
        })
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

/// A short timeout an operator configured is the timeout that is enforced.
///
/// The pause is comfortably inside it and then comfortably past it, so what this shows is that
/// the number came from the config file: against the old fixed five minutes the second half
/// would still answer 200.
#[tokio::test]
async fn the_configured_idle_timeout_is_the_one_enforced() {
    let node = TestNode::start(
        r#"
[mcp]
session_idle_timeout_secs = 3
sse_keepalive_secs = 1
"#,
    )
    .await;
    let session = node.initialize().await;

    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        node.tools_list_status(&session).await,
        200,
        "a session was forgotten well inside its idle timeout"
    );

    // Past the timeout, plus the sweep interval it is checked on.
    tokio::time::sleep(Duration::from_secs(6)).await;
    assert_eq!(
        node.tools_list_status(&session).await,
        404,
        "a session outlived the idle timeout it was configured with"
    );
}

/// A client holding its listening stream open is not idle, however long it pauses.
///
/// This is the case that answered a paused agent `404`: the client was holding the stream the
/// server was writing keep-alives to, and the sweeper — which could only see the time since the
/// last POST — took the session anyway. The second half is the other half of the rule: closing
/// the stream returns the session to the ordinary timeout rather than ending it outright, which
/// is the difference from the legacy transport.
#[tokio::test]
async fn a_listening_stream_holds_its_session_open_past_the_idle_timeout() {
    let node = TestNode::start(
        r#"
[mcp]
session_idle_timeout_secs = 3
sse_keepalive_secs = 1
"#,
    )
    .await;
    let session = node.initialize().await;
    let listening = node.hold_listening_stream(&session).await;

    // Twice the idle timeout with no request on the session at all.
    tokio::time::sleep(Duration::from_secs(6)).await;
    assert_eq!(
        node.tools_list_status(&session).await,
        200,
        "a session was swept while its listening stream was open"
    );

    // And the stream closing does not take the session with it: it is resumable until the
    // idle timeout runs out, unlike a legacy SSE session.
    listening.abort();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        node.tools_list_status(&session).await,
        200,
        "closing the listening stream ended the session instead of releasing it"
    );

    tokio::time::sleep(Duration::from_secs(6)).await;
    assert_eq!(
        node.tools_list_status(&session).await,
        404,
        "a session with no stream and no traffic outlived its idle timeout"
    );
}

/// The legacy transport is mounted by default and absent when turned off.
///
/// Absent rather than refusing: a route that is not there has no behaviour to get wrong. What
/// distinguishes the two is that the default answers the SSE stream at all — a test that only
/// checked the disabled case would pass against a transport that had never worked.
#[tokio::test]
async fn the_legacy_transport_is_mounted_unless_it_is_turned_off() {
    let default = TestNode::start("").await;
    let resp = http()
        .get(format!("{}/mcp/sse", default.url))
        .header("accept", "text/event-stream")
        .send()
        .await
        .expect("legacy sse get");
    assert_eq!(
        resp.status(),
        200,
        "the legacy transport is not mounted by default"
    );
    assert!(
        resp.headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("text/event-stream")),
        "the legacy endpoint answered something that is not an SSE stream"
    );
    drop(resp);

    let off = TestNode::start(
        r#"
[mcp]
legacy_sse_enabled = false
"#,
    )
    .await;
    for path in ["/mcp/sse", "/mcp/messages?session_id=whatever"] {
        let status = http()
            .get(format!("{}{path}", off.url))
            .header("accept", "text/event-stream")
            .send()
            .await
            .expect("legacy request")
            .status();
        assert_eq!(status.as_u16(), 404, "{path} is still mounted");
    }

    // And the current transport is untouched by turning the old one off.
    let session = off.initialize().await;
    assert_eq!(off.tools_list_status(&session).await, 200);
}

/// `enabled = false` withholds the whole endpoint, and nothing else.
#[tokio::test]
async fn mcp_can_be_unmounted_without_touching_the_http_api() {
    let node = TestNode::start(
        r#"
[mcp]
enabled = false
"#,
    )
    .await;

    let status = node
        .post_mcp(
            None,
            json!({"jsonrpc": "2.0", "id": 1, "method": "initialize"}),
        )
        .await
        .status();
    assert_eq!(status.as_u16(), 404, "/mcp is still mounted");

    let status = http()
        .get(format!("{}/_indexes", node.url))
        .send()
        .await
        .expect("list indexes")
        .status();
    assert!(
        status.is_success(),
        "unmounting MCP took the HTTP API with it: {status}"
    );
}
