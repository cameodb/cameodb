//! `[security.audit]` through the real binary (Phase 14, C2).
//!
//! The unit tests in `audit.rs` cover the parts: the rollup arithmetic, the ring, the file.
//! What they cannot show is that any of it is *reached* — that the middleware asks, that the
//! identity it asks with is the one that authenticated, that a refusal takes a different
//! path from a success, and that the trail says who read what. Every one of those is a wire
//! between components, and a wire is what a unit test cannot see.
//!
//! These speak HTTP directly rather than through the SDK, because half of what is asserted
//! is about headers the SDK exists to hide: an anonymous request, a wrong-scope key, and an
//! admin key reading the trail are three different callers against one node.

use std::io::Write as _;
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use serde_json::{Value, json};

/// A node with three keys and its audit trail turned on.
struct TestNode {
    child: Child,
    url: String,
    /// admin, reader (scoped to `docs`), writer — the tokens themselves, for `Authorization`.
    admin_key: String,
    reader_key: String,
    writer_key: String,
    audit_file: std::path::PathBuf,
    _dir: tempfile::TempDir,
}

impl TestNode {
    async fn start(audit_extra: &str) -> TestNode {
        let dir = tempfile::tempdir().expect("temp dir");
        let port = free_port();
        let data = dir.path().join("data");
        std::fs::create_dir_all(&data).expect("data dir");
        let audit_file = dir.path().join("audit.jsonl");

        // Minted by the shipped `keygen` rather than hashed here. It is the path an operator
        // takes, so a change to the key format breaks this test instead of silently making
        // the fixture wrong.
        let (admin_key, admin_hash) = keygen(dir.path(), "admin", "ops", None);
        let (reader_key, reader_hash) = keygen(dir.path(), "reader", "analyst", Some("docs"));
        let (writer_key, writer_hash) = keygen(dir.path(), "writer", "ingest", None);

        let config = format!(
            r#"
[node]
label = "audit-test"
profile = "local"

[network.http]
bind_address = "127.0.0.1"
port = {port}
admin_enabled = true

[network.cluster]
enabled = false

[storage]
data_paths = ["{data}"]
num_shards_init = 1
max_shards_per_node = 1

[security]
enabled = true

[[security.api_keys]]
key_hash_file = "{admin_hash}"
role = "admin"
label = "ops"

[[security.api_keys]]
key_hash_file = "{reader_hash}"
role = "reader"
label = "analyst"
allowed_indexes = ["docs"]

[[security.api_keys]]
key_hash_file = "{writer_hash}"
role = "writer"
label = "ingest"

[security.audit]
enabled = true
file = "{audit}"
rollup_secs = 1
{audit_extra}
"#,
            data = posix(&data),
            audit = posix(&audit_file),
            admin_hash = posix(&admin_hash),
            reader_hash = posix(&reader_hash),
            writer_hash = posix(&writer_hash),
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
            admin_key,
            reader_key,
            writer_key,
            audit_file,
            _dir: dir,
        };
        node.await_ready().await;
        node
    }

    async fn await_ready(&self) {
        let deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < deadline {
            // `/_cluster/health` is the one public route, so this needs no key.
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

    /// `GET path` as `key`, or anonymously when `key` is `None`.
    async fn get(&self, path: &str, key: Option<&str>) -> (u16, Value) {
        let mut request = http().get(format!("{}{path}", self.url));
        if let Some(key) = key {
            request = request.bearer_auth(key);
        }
        let resp = request.send().await.expect("get");
        let status = resp.status().as_u16();
        (status, resp.json().await.unwrap_or(Value::Null))
    }

    async fn search(&self, index: &str, query: &str, key: &str) -> u16 {
        http()
            .post(format!("{}/api/{index}/search", self.url))
            .bearer_auth(key)
            .json(&json!({"query": query, "limit": 5}))
            .send()
            .await
            .expect("search")
            .status()
            .as_u16()
    }

    async fn write(&self, index: &str, id: &str) -> u16 {
        http()
            .put(format!("{}/api/{index}/document", self.url))
            .bearer_auth(&self.writer_key)
            .json(&json!({"id": id, "doc": {"id": id, "title": "t"}}))
            .send()
            .await
            .expect("write")
            .status()
            .as_u16()
    }

    /// The trail as the admin endpoint reports it, newest first.
    async fn trail(&self) -> Vec<Value> {
        let (status, body) = self
            .get("/_admin/audit?limit=1000", Some(&self.admin_key))
            .await;
        assert_eq!(
            status, 200,
            "admin should be able to read the trail: {body}"
        );
        body["records"].as_array().cloned().unwrap_or_default()
    }

    /// The trail, re-read until `want` holds, or until the deadline — in which case the last
    /// trail seen is returned so the caller's own assertion produces the message.
    ///
    /// A record is written *after* the response it describes has been built, so any single read
    /// races the writer. A fixed sleep only widens that window rather than closing it: each test
    /// here runs a real server as a child process, and a `cargo test --workspace` run keeps
    /// enough of them in flight that reads came back with an empty trail. Polling waits exactly
    /// as long as it has to, so the checks are neither flaky nor padded with a constant chosen
    /// for the slowest machine anyone might run them on.
    async fn trail_until(&self, want: impl Fn(&[Value]) -> bool) -> Vec<Value> {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let trail = self.trail().await;
            if want(&trail) || Instant::now() >= deadline {
                return trail;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// The trail as it was written to disk.
    fn trail_file(&self) -> String {
        std::fs::read_to_string(&self.audit_file).unwrap_or_default()
    }

    /// The trail file, re-read until `want` holds or the deadline passes. Same reasoning as
    /// [`Self::trail_until`]; the file sink is flushed on the same path as the ring.
    async fn trail_file_until(&self, want: impl Fn(&str) -> bool) -> String {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let contents = self.trail_file();
            if want(&contents) || Instant::now() >= deadline {
                return contents;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

impl Drop for TestNode {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Mint a key with the shipped `keygen`, returning the token and the path to its digest.
fn keygen(
    dir: &std::path::Path,
    role: &str,
    label: &str,
    allowed_indexes: Option<&str>,
) -> (String, std::path::PathBuf) {
    let key_path = dir.join(format!("{label}.key"));
    let hash_path = dir.join(format!("{label}.hash"));
    let mut command = Command::new(env!("CARGO_BIN_EXE_cameodb"));
    command
        .arg("keygen")
        .arg("--role")
        .arg(role)
        .arg("--label")
        .arg(label)
        .arg("--key-out")
        .arg(&key_path)
        .arg("--hash-out")
        .arg(&hash_path);
    if let Some(indexes) = allowed_indexes {
        command.arg("--allowed-indexes").arg(indexes);
    }
    let output = command.output().expect("run keygen");
    assert!(
        output.status.success(),
        "keygen failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let key = std::fs::read_to_string(&key_path)
        .expect("key file")
        .trim()
        .to_string();
    (key, hash_path)
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

/// Paths go into a TOML string, where a Windows backslash would be an escape.
fn posix(path: &std::path::Path) -> String {
    path.display().to_string().replace('\\', "/")
}

fn records_of<'a>(trail: &'a [Value], event: &str) -> Vec<&'a Value> {
    trail.iter().filter(|r| r["event"] == event).collect()
}

/// The gap C2 exists to close: a node that can say *who read what*, attributed to a key an
/// operator issued, and named with the label they gave it.
#[tokio::test]
async fn a_read_is_attributed_to_the_key_that_made_it() {
    let node = TestNode::start("").await;

    let (status, _) = node.get("/_indexes", Some(&node.reader_key)).await;
    assert_eq!(status, 200);

    let trail = node
        .trail_until(|t| {
            records_of(t, "http")
                .iter()
                .any(|r| r["path"] == "/_indexes")
        })
        .await;
    let read = records_of(&trail, "http")
        .into_iter()
        .find(|r| r["path"] == "/_indexes")
        .unwrap_or_else(|| panic!("the read should be in the trail, got {trail:#?}"));

    assert_eq!(read["outcome"], "allowed");
    assert_eq!(read["role"], "reader");
    assert_eq!(
        read["label"], "analyst",
        "the operator's own name for the key"
    );
    assert!(
        read["key_id"].as_str().is_some_and(|id| !id.is_empty()),
        "a read must be attributable to a key: {read}"
    );
    assert_eq!(read["status"], 200);
}

/// The one property that must hold whatever else does: a trail written to answer "who read
/// what" must never itself become somewhere to read a credential.
#[tokio::test]
async fn no_key_ever_appears_in_the_trail() {
    let node = TestNode::start("").await;

    node.get("/_indexes", Some(&node.reader_key)).await;
    node.write("docs", "d1").await;
    // A rejected key is the tempting case: the natural way to log "this token failed" is to
    // log the token.
    node.get(
        "/_indexes",
        Some("cameo_v1_notarealkeynotarealkeynotarealkey000"),
    )
    .await;

    // Absence is the assertion, so waiting for the records to exist is what keeps this from
    // passing against an empty trail.
    let file = node
        .trail_file_until(|c| c.contains("/_indexes") && c.contains("auth_denied"))
        .await;
    let trail = node
        .trail_until(|t| {
            records_of(t, "http")
                .iter()
                .any(|r| r["path"] == "/_indexes")
                && !records_of(t, "auth_denied_stats").is_empty()
        })
        .await;
    let endpoint = serde_json::to_string(&trail).expect("serialize trail");
    for (name, key) in [
        ("admin", &node.admin_key),
        ("reader", &node.reader_key),
        ("writer", &node.writer_key),
    ] {
        assert!(
            !file.contains(key.as_str()),
            "the {name} key appears in the audit file"
        );
        assert!(
            !endpoint.contains(key.as_str()),
            "the {name} key appears in the /_admin/audit response"
        );
    }
    assert!(
        !file.contains("notarealkey"),
        "a rejected token must not be echoed into the trail either"
    );
}

/// The premise the whole design rests on, end to end: a knowledge base ingests far more than
/// it retrieves, so writes are counted and reads are listed. Sixty writes must cost one line.
#[tokio::test]
async fn writes_are_counted_while_reads_are_listed() {
    let node = TestNode::start("").await;

    for i in 0..60 {
        let status = node.write("docs", &format!("d{i}")).await;
        assert!(status < 400, "write {i} failed with {status}");
    }
    node.search("docs", "id:d1", &node.reader_key).await;

    // `rollup_secs = 1`, so the counts appear once a window closes. Wait for all sixty to be
    // accounted for rather than for a duration, and sum across lines: on a loaded machine sixty
    // writes can take longer than a window, which splits them across two — a scheduling detail,
    // not a change in what the rollup does.
    let trail = node
        .trail_until(|t| {
            records_of(t, "write_stats")
                .iter()
                .filter_map(|r| r["ops"].as_u64())
                .sum::<u64>()
                == 60
        })
        .await;

    let per_write: Vec<&Value> = records_of(&trail, "http")
        .into_iter()
        .filter(|r| r["method"] == "PUT")
        .collect();
    assert!(
        per_write.is_empty(),
        "writes must not be listed one by one, found {}",
        per_write.len()
    );

    // Counted, not listed: what matters is that every write is accounted for and that sixty of
    // them did not become sixty lines. Pinning it to exactly one line asserted that the writes
    // fit inside one rollup window, which is a property of the machine rather than of the code.
    let stats = records_of(&trail, "write_stats");
    let counted: u64 = stats.iter().filter_map(|r| r["ops"].as_u64()).sum();
    assert_eq!(
        counted, 60,
        "every write must be counted exactly once, got {stats:#?}"
    );
    assert!(
        stats.len() < 60,
        "sixty writes to one index by one key must not become sixty lines, got {}",
        stats.len()
    );
    for line in &stats {
        assert_eq!(line["index"], "docs");
        assert_eq!(line["label"], "ingest");
    }

    let searches: Vec<&Value> = records_of(&trail, "http")
        .into_iter()
        .filter(|r| r["method"] == "POST" && r["path"] == "/api/docs/search")
        .collect();
    assert_eq!(
        searches.len(),
        1,
        "a read keeps its own line, got {searches:#?}"
    );
}

/// The asymmetry that keeps the trail readable *and* keeps it from being a DoS lever: a
/// refusal of a valid key is bounded by the credentials in circulation and gets a line; an
/// anonymous refusal is bounded by whoever can reach the port and gets counted.
#[tokio::test]
async fn a_refused_key_is_named_but_an_anonymous_flood_is_only_counted() {
    let node = TestNode::start("").await;

    // The reader is scoped to `docs`, so this is a 403 by a caller who authenticated.
    let refused = node.search("payroll", "salary:*", &node.reader_key).await;
    assert_eq!(refused, 403, "the reader is not scoped to payroll");

    for _ in 0..5 {
        let (status, _) = node.get("/_indexes", None).await;
        assert_eq!(status, 401, "no key, no answer");
    }
    let trail = node
        .trail_until(|t| {
            records_of(t, "http")
                .iter()
                .any(|r| r["outcome"] == "denied")
                && records_of(t, "auth_denied_stats")
                    .iter()
                    .any(|r| r["ops"] == 5)
        })
        .await;

    let denial = records_of(&trail, "http")
        .into_iter()
        .find(|r| r["outcome"] == "denied")
        .unwrap_or_else(|| panic!("a 403 must be listed, got {trail:#?}"));
    assert_eq!(denial["index"], "payroll", "which index was reached for");
    assert_eq!(denial["label"], "analyst", "and by whom");
    assert_eq!(denial["status"], 403);
    assert!(
        denial["reason"]
            .as_str()
            .is_some_and(|r| r.contains("not permitted")),
        "the refusal must say why: {denial}"
    );

    let anonymous = records_of(&trail, "auth_denied_stats");
    assert_eq!(
        anonymous.len(),
        1,
        "five anonymous refusals are one counted line, got {anonymous:#?}"
    );
    assert_eq!(anonymous[0]["ops"], 5);
    assert!(
        anonymous[0]["key_id"].is_null(),
        "there is no key to attribute an anonymous refusal to: {}",
        anonymous[0]
    );
}

/// Reading the trail is the thing a compromised credential would most want to do, so it
/// takes node-admin — and doing it leaves a record like everything else.
#[tokio::test]
async fn reading_the_trail_needs_node_admin_and_is_itself_recorded() {
    let node = TestNode::start("").await;

    let (status, _) = node.get("/_admin/audit", Some(&node.reader_key)).await;
    assert_eq!(status, 403, "a reader must not be able to read the trail");

    // The admin's first read cannot appear in its own response — the record is written after
    // the response is built — so it takes a later call to see it.
    let trail = node
        .trail_until(|t| {
            let http = records_of(t, "http");
            http.iter()
                .any(|r| r["path"] == "/_admin/audit" && r["outcome"] == "allowed")
                && http
                    .iter()
                    .any(|r| r["path"] == "/_admin/audit" && r["outcome"] == "denied")
        })
        .await;
    assert!(
        records_of(&trail, "http")
            .iter()
            .any(|r| r["path"] == "/_admin/audit" && r["outcome"] == "allowed"),
        "reading the audit log must itself be audited, got {trail:#?}"
    );
    assert!(
        records_of(&trail, "http")
            .iter()
            .any(|r| r["path"] == "/_admin/audit" && r["outcome"] == "denied"),
        "and so must a refused attempt to read it"
    );
}

/// A search for a person's name is a record of that name. Off by default, and the flag has
/// to actually change what is kept — a setting that silently does nothing is worse than one
/// that does not exist, because the operator believes they have the detail.
#[tokio::test]
async fn query_text_is_kept_only_when_configured() {
    let secret = "patient_zero_identifier";

    let quiet = TestNode::start("").await;
    quiet.search("docs", secret, &quiet.reader_key).await;
    // Wait for the search's own record, so the absence of the query text is evidence rather
    // than a trail that had not been written yet.
    quiet
        .trail_file_until(|c| c.contains("/api/docs/search"))
        .await;
    assert!(
        !quiet.trail_file().contains(secret),
        "query text must not be recorded by default:\n{}",
        quiet.trail_file()
    );

    let loud = TestNode::start("record_query_text = true").await;
    loud.search("docs", secret, &loud.reader_key).await;
    let recorded = loud
        .trail_until(|t| t.iter().any(|r| r["query"].as_str() == Some(secret)))
        .await;
    assert!(
        recorded.iter().any(|r| r["query"].as_str() == Some(secret)),
        "record_query_text = true must keep the query, got {recorded:#?}"
    );
}

/// What the HTTP gate structurally cannot see. From outside, every agent call is
/// `POST /mcp`; which tool ran and which index it touched exist only inside the dispatcher,
/// which is the whole reason `record_tool_call` is a hook on the backend rather than another
/// middleware.
#[tokio::test]
async fn an_mcp_tool_call_records_the_tool_and_the_index() {
    let node = TestNode::start("").await;

    let resp = http()
        .post(format!("{}/mcp", node.url))
        .bearer_auth(&node.reader_key)
        .header("accept", "application/json, text/event-stream")
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": "search_index", "arguments": {"index": "docs", "query": "id:d1"}},
        }))
        .send()
        .await
        .expect("mcp post");
    assert!(
        resp.status().is_success(),
        "mcp call failed: {:?}",
        resp.status()
    );

    let trail = node
        .trail_until(|t| !records_of(t, "mcp_tool").is_empty())
        .await;
    let call = records_of(&trail, "mcp_tool")
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("the tool call should be in the trail, got {trail:#?}"));

    assert_eq!(call["tool"], "search_index");
    assert_eq!(call["index"], "docs", "the HTTP path alone cannot say this");
    assert_eq!(call["outcome"], "allowed");
    assert_eq!(call["label"], "analyst");
    assert!(
        call["query"].is_null(),
        "query text is off by default here too: {call}"
    );

    // The transport request is recorded as well, and is exactly as uninformative as the
    // argument for this hook says it is.
    assert!(
        records_of(&trail, "http")
            .iter()
            .any(|r| r["path"] == "/mcp"),
        "the POST itself is still audited"
    );
}

/// An index outside the key's scope is refused inside the dispatcher, not at the door — so
/// this is the one place that refusal can be recorded at all.
#[tokio::test]
async fn an_mcp_tool_refused_for_scope_is_recorded_with_its_reason() {
    let node = TestNode::start("").await;

    http()
        .post(format!("{}/mcp", node.url))
        .bearer_auth(&node.reader_key)
        .header("accept", "application/json, text/event-stream")
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": "search_index", "arguments": {"index": "payroll", "query": "*"}},
        }))
        .send()
        .await
        .expect("mcp post");

    let trail = node
        .trail_until(|t| !records_of(t, "mcp_tool").is_empty())
        .await;
    let call = records_of(&trail, "mcp_tool")
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("the refused call should be in the trail, got {trail:#?}"));

    assert_eq!(call["outcome"], "denied");
    assert_eq!(call["index"], "payroll");
    assert!(
        call["reason"]
            .as_str()
            .is_some_and(|r| r.contains("not permitted")),
        "the record must say why it was refused: {call}"
    );
}

/// The file is the half that survives a restart, and it has to be machine-readable: one
/// JSON object per line, no partial writes, no framing of its own.
#[tokio::test]
async fn the_file_sink_is_valid_json_lines() {
    let node = TestNode::start("").await;
    node.get("/_indexes", Some(&node.reader_key)).await;
    node.write("docs", "d1").await;

    let contents = node
        .trail_file_until(|c| c.contains("/_indexes") && c.contains("write"))
        .await;
    assert!(!contents.is_empty(), "the file sink wrote nothing");
    for line in contents.lines() {
        let parsed: Value =
            serde_json::from_str(line).unwrap_or_else(|e| panic!("not JSON: {line}\n{e}"));
        assert!(
            parsed["ts"].as_str().is_some_and(|t| t.ends_with('Z')),
            "every record is timestamped in UTC: {line}"
        );
        assert!(
            parsed["event"].as_str().is_some(),
            "every record says what kind it is: {line}"
        );
    }
}
