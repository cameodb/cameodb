//! The panic hardening, proven end to end against the built binary.
//!
//! Every in-process test unwinds, because `cargo test` builds the dev profile — so none of them
//! can show that the *release* profile unwinds rather than aborts. This one can: it builds
//! `cameodb` with the `fault-injection` feature (release by default), boots it, and drives a real
//! panic into each hardened surface over HTTP. If the profile still aborted, the first trigger
//! would take the process down and the very next request would fail.
//!
//! The `fault-injection` feature is off in every shipped build, so the panic seams these probes
//! reach exist only here. It compiles a second binary — into `target/panic-smoke/`, never over
//! the release artifact the validation and release scripts read; see [`build_fault_binary`]. Set
//! `PANIC_SMOKE_PROFILE=dev` while iterating to trade the release proof for a faster build.

use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

mod common;

/// Trap inputs — the same literals the feature-gated seams in `node_orchestrator` and `routes`
/// match. Kept in step by hand because the binary crate exposes no library to import them from.
const READ_TRAP_QUERY: &str = "__fault_panic_read__";
const WRITE_OP_TRAP_INDEX: &str = "fault_panic_write_op__";
const WRITER_THREAD_TRAP_INDEX: &str = "fault_kill_writer__";
const HANDLER_TRAP_PATH: &str = "/__fault/panic";

/// How long the node's writer monitor is held before it rebuilds a crashed writer. The respawn is
/// immediate in a normal build, which leaves the writerless window far too short to poll over
/// HTTP; holding it open is what lets this test assert the red a dead writer must report, and then
/// the green its replacement restores. Long enough to poll comfortably, short enough not to drag
/// the run out.
const RESPAWN_HOLD: Duration = Duration::from_secs(3);

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("workspace root two levels above the crate")
        .to_path_buf()
}

/// Build `cameodb` with the fault-injection seams and return the binary path. Release by default
/// — the profile whose `panic = "unwind"` this test exists to exercise.
///
/// Built into its **own target directory**, which is not a detail. The ordinary one would put
/// this binary at `target/release/cameodb` — the exact path `scripts/validate/lib.sh` picks up
/// with no rebuild of its own, and the one `scripts/release/build.sh` copies into `dist/`. A
/// `cargo test` between the release build and the validation run would then leave the suite
/// probing a binary with an unauthenticated `/__fault/panic` route compiled in, reporting a pass
/// for something nobody was going to ship. Isolating the two also stops them invalidating each
/// other's cache: the feature flag differs, so sharing a directory means every alternation
/// between `cargo test` and `cargo build --release` rebuilds the server crate.
fn build_fault_binary() -> PathBuf {
    let release = std::env::var("PANIC_SMOKE_PROFILE").as_deref() != Ok("dev");

    // Under whichever target root is in force, so `CARGO_TARGET_DIR` is still respected.
    let target = std::env::var("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| workspace_root().join("target"))
        .join("panic-smoke");

    let mut cmd = Command::new(env!("CARGO"));
    cmd.current_dir(workspace_root())
        .args([
            "build",
            "--bin",
            "cameodb",
            "--features",
            "fault-injection",
            "--target-dir",
        ])
        .arg(&target);
    if release {
        cmd.arg("--release");
    }
    let status = cmd.status().expect("run cargo build");
    assert!(
        status.success(),
        "building the fault-injection binary failed"
    );

    let binary = target
        .join(if release { "release" } else { "debug" })
        .join("cameodb");
    assert!(
        binary.exists(),
        "built binary not found at {}",
        binary.display()
    );
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

/// The built fault-injection binary, running against a temporary data directory.
struct Node {
    child: Option<Child>,
    base: String,
    _dir: tempfile::TempDir,
}

impl Node {
    async fn start(binary: &Path) -> Node {
        let dir = tempfile::tempdir().expect("temp dir");
        let port = common::reserve_port();
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
            .env(
                "CAMEODB_FAULT_RESPAWN_DELAY_MS",
                RESPAWN_HOLD.as_millis().to_string(),
            )
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
            Some(Ok(None)) => true, // still running
            _ => false,             // exited, errored, or already taken
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

/// A fault-injection build declares itself in `--version`, and an ordinary one does not.
///
/// `scripts/validate/all.sh` refuses a binary carrying these seams by reading exactly this, so
/// that refusal is only as good as the marker still being printed. Asserting it here is what
/// keeps the two in step: drop the marker and this fails, rather than the release process quietly
/// losing a guard it is relying on. Both directions, because a marker that is always present
/// would block every release just as surely as an absent one lets a fault build through.
#[tokio::test]
async fn a_fault_injection_build_says_so_in_its_version() {
    let fault = build_fault_binary();
    let reported = String::from_utf8(
        Command::new(&fault)
            .arg("--version")
            .output()
            .expect("run --version on the fault-injection binary")
            .stdout,
    )
    .expect("--version output is text");
    assert!(
        reported.contains("+fault-injection"),
        "a fault-injection build must say so in --version, or the validation suite cannot \
         refuse it; it reported {reported:?}"
    );

    // The ordinary build of this very workspace, which is what ships. Skipped rather than
    // failed when it has not been built: `cargo test` alone does not produce one, and this test
    // exists to pin the marker, not to require a release build.
    let ordinary = workspace_root().join("target/release/cameodb");
    if ordinary.exists() {
        let reported = String::from_utf8(
            Command::new(&ordinary)
                .arg("--version")
                .output()
                .expect("run --version on the release binary")
                .stdout,
        )
        .expect("--version output is text");
        assert!(
            !reported.contains("fault-injection"),
            "an ordinary build must not claim the fault-injection marker, or every release is \
             refused; {} reported {reported:?}",
            ordinary.display()
        );
    }
}

/// Every panic surface is contained: the process survives each trigger, a dead writer turns
/// health red, and its monitor respawns it so the shard heals back to green without a restart.
#[tokio::test]
async fn a_panic_at_every_surface_is_contained_and_a_dead_writer_is_respawned() {
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
    assert!(
        node.is_running(),
        "the node must survive a panicking handler"
    );

    // A write whose per-command guard catches the panic is retriable (503), the writer is
    // rebuilt, and a following write to another index still lands — the thread kept serving.
    let status = client
        .put(node.url(&format!("/api/{WRITE_OP_TRAP_INDEX}/document")))
        .json(&serde_json::json!({ "id": "x", "doc": { "title": "boom" } }))
        .send()
        .await
        .expect("guarded write trap request")
        .status();
    assert_eq!(
        status, 503,
        "a guarded write panic must be retriable, not fatal"
    );

    let status = client
        .put(node.url("/api/healthy/document"))
        .json(&serde_json::json!({ "id": "ok", "doc": { "title": "fine" } }))
        .send()
        .await
        .expect("normal write request")
        .status();
    assert_eq!(
        status, 200,
        "the writer thread must still serve after a guarded panic"
    );

    // Health is still green: nothing has actually died yet.
    assert_eq!(
        health_status(&client, &node).await,
        "green",
        "a contained panic must not turn health red"
    );

    // Now kill the writer thread outright — a panic past its guard ends the thread. The write's
    // reply is lost, and the shard can take no more writes until its monitor rebuilds one. Both
    // halves of that must show: health goes red while the writer is gone, and back to green once
    // the replacement is serving, with no process restart. The monitor is held off for
    // `RESPAWN_HOLD`, so the red window is wide enough to observe rather than race.
    let _ = client
        .put(node.url(&format!("/api/{WRITER_THREAD_TRAP_INDEX}/document")))
        .json(&serde_json::json!({ "id": "x", "doc": { "title": "kill" } }))
        .send()
        .await;

    assert!(
        node.is_running(),
        "a dead writer must not take the process down"
    );

    // Health was green one assertion ago, so a red here is the writer's death and nothing else.
    let deadline = Instant::now() + RESPAWN_HOLD;
    loop {
        if health_status(&client, &node).await == "red" {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "a dead writer thread must show as red in health"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Reads are never affected by the writer's death.
    let status = client
        .post(node.url("/api/healthy/search"))
        .json(&serde_json::json!({ "query": "fine", "limit": 5 }))
        .send()
        .await
        .expect("read after writer death")
        .status();
    assert_eq!(
        status, 200,
        "reads must still be served after a writer dies"
    );

    // Once the hold expires the monitor respawns the writer, and health returns to green on its
    // own — the recovery the down-count exists to be cleared by.
    let deadline = Instant::now() + RESPAWN_HOLD + Duration::from_secs(10);
    loop {
        if health_status(&client, &node).await == "green" {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "a respawned writer must return health to green without a restart"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // And the replacement writer serves: a fresh write to a healthy index on the same shard lands.
    let status = client
        .put(node.url("/api/healthy/document"))
        .json(&serde_json::json!({ "id": "ok2", "doc": { "title": "back" } }))
        .send()
        .await
        .expect("write after respawn")
        .status();
    assert_eq!(status, 200, "the respawned writer must accept writes again");
}
