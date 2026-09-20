//! `[security.limits] write_documents_per_minute` enforced through the real write routes.
//!
//! The unit tests in `ratelimit.rs` cover the bucket arithmetic and which bucket a caller is
//! metered in. What they cannot show is that the bucket is *consulted* on the way into a write:
//! that the config reaches `AppState`, that each of the five write routes asks before doing any
//! of the work, that the charge is the number of documents rather than one per request, and that
//! a refusal comes back as a 429 a client can act on. Every one of those is a wire between
//! components, and a wire is exactly what a unit test cannot see.
//!
//! These speak HTTP directly rather than through the SDK, because the thing under test is the
//! status code and the `Retry-After` header — the parts an SDK is built to hide.

use std::io::Write as _;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use serde_json::{Value, json};

mod common;

struct TestNode {
    child: Child,
    url: String,
    _dir: tempfile::TempDir,
}

impl TestNode {
    async fn start(extra: &str) -> TestNode {
        let dir = tempfile::tempdir().expect("temp dir");
        let port = common::reserve_port();
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
            .stderr(Stdio::inherit())
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
        let deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < deadline {
            if let Ok(resp) = http()
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

    /// One document through `PUT /api/{index}/document`.
    async fn write_one(&self, index: &str, id: &str) -> Answer {
        self.send(
            reqwest::Method::PUT,
            &format!("/api/{index}/document"),
            Some(json!({"id": id, "doc": {"id": id, "title": "t"}})),
        )
        .await
    }

    /// `count` documents through `POST /api/{index}/_bulk`, in one request.
    async fn write_bulk(&self, index: &str, count: usize) -> Answer {
        let docs: Vec<Value> = (0..count)
            .map(|i| json!({"id": format!("bulk-{i}"), "doc": {"id": format!("bulk-{i}")}}))
            .collect();
        self.send(
            reqwest::Method::POST,
            &format!("/api/{index}/_bulk"),
            Some(Value::Array(docs)),
        )
        .await
    }

    /// `count` documents as an NDJSON body through `POST /api/{index}/document/stream`.
    async fn write_stream(&self, index: &str, count: usize) -> Answer {
        let body: String = (0..count)
            .map(|i| format!("{{\"id\":\"s-{i}\",\"doc\":{{\"id\":\"s-{i}\"}}}}\n"))
            .collect();
        let resp = http()
            .post(format!("{}/api/{index}/document/stream", self.url))
            .header("content-type", "application/x-ndjson")
            .body(body)
            .send()
            .await
            .expect("stream request");
        Answer::of(resp).await
    }

    async fn search(&self, index: &str) -> Answer {
        self.send(
            reqwest::Method::POST,
            &format!("/api/{index}/search"),
            Some(json!({"query": "*", "limit": 1})),
        )
        .await
    }

    async fn delete_one(&self, index: &str, id: &str) -> Answer {
        self.send(
            reqwest::Method::DELETE,
            &format!("/api/{index}/document?id={id}"),
            None,
        )
        .await
    }

    async fn send(&self, method: reqwest::Method, path: &str, body: Option<Value>) -> Answer {
        let mut request = http().request(method, format!("{}{path}", self.url));
        if let Some(body) = body {
            request = request.json(&body);
        }
        Answer::of(request.send().await.expect("request")).await
    }
}

/// What came back: the parts a caller acts on.
struct Answer {
    status: u16,
    retry_after: Option<String>,
    body: Value,
}

impl Answer {
    async fn of(resp: reqwest::Response) -> Answer {
        let status = resp.status().as_u16();
        let retry_after = resp
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        Answer {
            status,
            retry_after,
            body: resp.json().await.unwrap_or(json!(null)),
        }
    }

    /// Everything a failed assertion should print: a bare status tells nobody which limit bit.
    fn describe(&self) -> String {
        format!("status {} body {}", self.status, self.body)
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

/// A rate that refills slowly enough for a test to outrun it.
///
/// Six documents a minute is a token every ten seconds, so a burst spent by one request is still
/// spent when the next one lands — the assertion is about the limiter, not about how fast the
/// machine running it happens to be.
const SLOW_WRITE_RATE: &str = r#"
[security.limits]
write_documents_per_minute = 6
write_burst = 10
"#;

/// The default must be inert end to end. A node that shipped this code and started refusing
/// ingest without being configured to would be a regression indistinguishable from a fault.
#[tokio::test]
async fn writes_are_unmetered_by_default() {
    let node = TestNode::start("").await;
    for i in 0..5 {
        let answer = node.write_bulk("docs", 200).await;
        assert_eq!(
            answer.status,
            200,
            "bulk {i} of 200 documents was refused on a node with no write limit: {}",
            answer.describe()
        );
    }
}

/// The charge is documents, not requests.
///
/// This is the whole reason the write surface has a meter of its own. One token per request
/// would let `_bulk` carry a thousand documents for the price of a single write, which is a
/// budget that bounds nothing in the direction that costs the most and grows the disk.
#[tokio::test]
async fn a_bulk_write_is_charged_for_every_document_it_carries() {
    let node = TestNode::start(SLOW_WRITE_RATE).await;

    let answer = node.write_bulk("docs", 10).await;
    assert_eq!(
        answer.status,
        200,
        "ten documents should fit a ten-document burst: {}",
        answer.describe()
    );

    let answer = node.write_one("docs", "one-too-many").await;
    assert_eq!(
        answer.status,
        429,
        "the burst was spent by the bulk request, so the next write should be refused: {}",
        answer.describe()
    );
    assert_eq!(
        answer.retry_after.as_deref(),
        // A token every ten seconds at six a minute, and one token is what this request wants.
        Some("10"),
        "a refusal must carry the wait in a header a client library obeys: {}",
        answer.describe()
    );
    let message = answer.body["details"].as_str().unwrap_or_default();
    assert!(
        message.contains("write_documents_per_minute"),
        "the refusal should name the setting that caused it, got: {message}"
    );
}

/// A removal is a document through the writer, a commit and a merge. Cheaper to send than a
/// write, not cheaper to serve — so it draws on the same allowance.
#[tokio::test]
async fn a_delete_is_charged_like_a_write() {
    let node = TestNode::start(SLOW_WRITE_RATE).await;

    let answer = node.write_bulk("docs", 10).await;
    assert_eq!(answer.status, 200, "{}", answer.describe());

    let answer = node.delete_one("docs", "bulk-0").await;
    assert_eq!(
        answer.status,
        429,
        "a delete on a spent allowance should be refused: {}",
        answer.describe()
    );
}

/// The one route whose total is not knowable up front stops when the allowance runs out, and
/// reports what it wrote before it did.
///
/// A bare 429 would leave the caller unable to tell which half of its file is in the index,
/// which on a partially-consumed stream is the one thing it cannot work out for itself — the
/// same bargain the decompressed-size refusal makes beside it.
#[tokio::test]
async fn a_write_stream_stops_when_the_allowance_runs_out() {
    let node = TestNode::start(&format!(
        r#"
[search]
stream_batch_size = 10
{SLOW_WRITE_RATE}"#
    ))
    .await;

    let answer = node.write_stream("docs", 50).await;
    assert_eq!(
        answer.status,
        429,
        "fifty documents against a ten-document burst should be cut short: {}",
        answer.describe()
    );
    assert_eq!(answer.body["status"], "refused", "{}", answer.describe());
    assert_eq!(
        answer.body["items_written"],
        10,
        "the first micro-batch fit the burst and should be reported as written: {}",
        answer.describe()
    );
    assert!(
        answer.body["retry_after_secs"].as_u64().unwrap_or(0) >= 1,
        "the summary should say when the caller may resume: {}",
        answer.describe()
    );
    assert!(
        answer.retry_after.is_some(),
        "and say it in the header too: {}",
        answer.describe()
    );
}

/// Reads and writes are separate budgets. An agent that has spent its ingest allowance can
/// still search, which is what makes the two settings independently useful.
#[tokio::test]
async fn a_spent_write_allowance_does_not_refuse_a_search() {
    let node = TestNode::start(SLOW_WRITE_RATE).await;

    node.write_bulk("docs", 10).await;
    let refused = node.write_one("docs", "extra").await;
    assert_eq!(refused.status, 429, "{}", refused.describe());

    let answer = node.search("docs").await;
    assert_eq!(
        answer.status,
        200,
        "searching must not draw on the write budget: {}",
        answer.describe()
    );
}

/// Metering tool calls must not silently start metering ingest.
///
/// An operator who set `tool_calls_per_minute` chose a number for tool calls. Charging their
/// bulk imports against it on upgrade would be a release that broke ingest for everyone who had
/// taken the earlier advice — so the write meter is off until it is asked for, by name.
#[tokio::test]
async fn a_tool_rate_alone_leaves_the_write_surface_open() {
    let node = TestNode::start(
        r#"
[security.limits]
tool_calls_per_minute = 60
tool_call_burst = 1
"#,
    )
    .await;

    let answer = node.write_bulk("docs", 500).await;
    assert_eq!(
        answer.status,
        200,
        "a tool-call rate must not meter writes: {}",
        answer.describe()
    );
    let answer = node.write_one("docs", "again").await;
    assert_eq!(answer.status, 200, "{}", answer.describe());

    // And the tool-call side of the same config is doing its job, so the assertion above is
    // about the separation rather than about a limiter that was never switched on.
    assert_eq!(node.search("docs").await.status, 200);
    assert_eq!(
        node.search("docs").await.status,
        429,
        "a burst of one should refuse the second search"
    );
}
