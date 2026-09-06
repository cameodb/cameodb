//! The panic hardening, proven end to end against the built binary.
//!
//! Every in-process test unwinds, because `cargo test` builds the dev profile — so none of them
//! can show that the *release* profile unwinds rather than aborts. This one can: it builds
//! `cameodb` with the `fault-injection` feature (release by default), boots it, and drives a real
//! panic into each hardened surface over HTTP. If the profile still aborted, the first trigger
//! would take the process down and the very next request would fail.
//!
//! The `fault-injection` feature is off in every shipped build, so the panic seams these probes
//! reach exist only here. The test is `#[ignore]` because it compiles a second binary; run it
//! with `cargo test -p server --test panic_isolation -- --ignored`, and set
//! `PANIC_SMOKE_PROFILE=dev` to trade the release proof for a faster build while iterating.

use std::io::Write as _;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// Trap inputs — the same literals the feature-gated seams in `node_orchestrator` and `routes`
/// match. Kept in step by hand because the binary crate exposes no library to import them from.
const READ_TRAP_QUERY: &str = "__fault_panic_read__";
const WRITE_OP_TRAP_INDEX: &str = "__fault_panic_write_op__";
const WRITER_THREAD_TRAP_INDEX: &str = "__fault_kill_writer__";
const HANDLER_TRAP_PATH: &str = "/__fault/panic";

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("workspace root two levels above the crate")
        .to_path_buf()
}

/// Build `cameodb` with the fault-injection seams and return the binary path. Release by default
/// — the profile whose `panic = "unwind"` this test exists to exercise.
fn build_fault_binary() -> PathBuf {
    let release = std::env::var("PANIC_SMOKE_PROFILE").as_deref() != Ok("dev");

    let mut cmd = Command::new(env!("CARGO"));
    cmd.current_dir(workspace_root())
        .args(["build", "--bin", "cameodb", "--features", "fault-injection"]);
    if release {
        cmd.arg("--release");
    }
    let status = cmd.status().expect("run cargo build");
    assert!(status.success(), "building the fault-injection binary failed");

    let target = std::env::var("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| workspace_root().join("target"));
    let binary = target
        .join(if release { "release" } else { "debug" })
        .join("cameodb");
    assert!(binary.exists(), "built binary not found at {}", binary.display());
    binary
}

/// reqwest builds a rustls client even for plain HTTP, and rustls needs a crypto provider
/// installed process-wide first. The SDK does this itself; here the test speaks reqwest directly.
fn install_crypto_provider() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral")
        .local_addr()
        .expect("local addr")
        .port()
}

/// The built fault-injection binary, running against a temporary data directory.
struct Node {
    child: Option<Child>,
    base: String,
    _dir: tempfile::TempDir,
}

impl Node {
    async fn start(binary: &Path) -> Node {
        let dir = tempfile::tempdir().expect("temp dir");
        let port = free_port();
        let data = dir.path().join("data");
        std::fs::create_dir_all(&data).expect("data dir");

        let config = format!(
            r#"
[node]
label = "panic-smoke"
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
            .and_then(|mut f| f.write_all(config.as_bytes()))
            .expect("write config");

        let child = Command::new(binary)
            .arg("-c")
            .arg(&config_path)
            .env("RUST_LOG", "error")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn cameodb");

        let node = Node {
            child: Some(child),
            base: format!("http://127.0.0.1:{port}"),
            _dir: dir,
        };
        node.await_ready().await;
        node
    }

    async fn await_ready(&self) {
        let client = reqwest::Client::new();
        let deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < deadline {
            if let Ok(resp) = client.get(self.url("/_cluster/health")).send().await
                && resp.status().is_success()
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("node never became healthy");
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base)
    }

    fn is_running(&mut self) -> bool {
        match self.child.as_mut().map(|c| c.try_wait()) {
            Some(Ok(None)) => true,      // still running
            _ => false,                  // exited, errored, or already taken
        }
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

async fn health_status(client: &reqwest::Client, node: &Node) -> String {
    let body: serde_json::Value = client
        .get(node.url("/_cluster/health"))
        .send()
        .await
        .expect("health request")
        .json()
        .await
        .expect("health json");
    body["status"].as_str().expect("status field").to_string()
}

/// Every panic surface is contained: the process survives each trigger, and only a genuinely
/// dead writer turns health red.
#[tokio::test]
#[ignore = "builds a second binary; run explicitly with --ignored"]
async fn a_panic_at_every_surface_is_contained_and_a_dead_writer_shows_in_health() {
    install_crypto_provider();
    let binary = build_fault_binary();
    let mut node = Node::start(&binary).await;
    let client = reqwest::Client::new();

    // A search runs on the read pool; a panic there comes back a 500 and the node serves on.
    let status = client
        .post(node.url("/api/probe/search"))
        .json(&serde_json::json!({ "query": READ_TRAP_QUERY, "limit": 5 }))
        .send()
        .await
        .expect("read trap request")
        .status();
    assert_eq!(status, 500, "a panicking read must answer 500, not abort");
    assert!(node.is_running(), "the node must survive a panicking read");

    // A handler panic is caught by the layer and answered 500.
    let status = client
        .get(node.url(HANDLER_TRAP_PATH))
        .send()
        .await
        .expect("handler trap request")
        .status();
    assert_eq!(status, 500, "a panicking handler must answer 500");
    assert!(node.is_running(), "the node must survive a panicking handler");

    // A write whose per-command guard catches the panic is retriable (503), the writer is
    // rebuilt, and a following write to another index still lands — the thread kept serving.
    let status = client
        .put(node.url(&format!("/api/{WRITE_OP_TRAP_INDEX}/document")))
        .json(&serde_json::json!({ "id": "x", "doc": { "title": "boom" } }))
        .send()
        .await
        .expect("guarded write trap request")
        .status();
    assert_eq!(status, 503, "a guarded write panic must be retriable, not fatal");

    let status = client
        .put(node.url("/api/healthy/document"))
        .json(&serde_json::json!({ "id": "ok", "doc": { "title": "fine" } }))
        .send()
        .await
        .expect("normal write request")
        .status();
    assert_eq!(status, 200, "the writer thread must still serve after a guarded panic");

    // Health is still green: nothing has actually died yet.
    assert_eq!(
        health_status(&client, &node).await,
        "green",
        "a contained panic must not turn health red"
    );

    // Now kill the writer thread outright — a panic past its guard. The write's reply is lost,
    // the shard can take no more writes, and this must show: health goes red while the process
    // stays up and still answers reads.
    let _ = client
        .put(node.url(&format!("/api/{WRITER_THREAD_TRAP_INDEX}/document")))
        .json(&serde_json::json!({ "id": "x", "doc": { "title": "kill" } }))
        .send()
        .await;

    // Give the writer thread a moment to unwind and mark itself down.
    tokio::time::sleep(Duration::from_millis(500)).await;

    assert!(node.is_running(), "a dead writer must not take the process down");
    assert_eq!(
        health_status(&client, &node).await,
        "red",
        "a dead writer thread must show as red in health"
    );

    // Reads are unaffected by the writer's death.
    let status = client
        .post(node.url("/api/healthy/search"))
        .json(&serde_json::json!({ "query": "fine", "limit": 5 }))
        .send()
        .await
        .expect("read after writer death")
        .status();
    assert_eq!(status, 200, "reads must still be served after a writer dies");
}
