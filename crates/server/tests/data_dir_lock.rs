//! One process per data directory: a node claims every directory in `storage.data_paths`
//! before reading anything in it, and a second process — another node, `cameodb compact` — is
//! refused, naming the first.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

use client::CameoClient;

mod common;

fn with_tls_provider() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

/// A config on `data` (and `extra`, as a second data path) listening on a port of its own.
fn config(dir: &Path, name: &str, data: &Path, extra: Option<&Path>) -> (PathBuf, u16) {
    let port = common::reserve_port();
    let path_of = |p: &Path| format!("\"{}\"", p.display().to_string().replace('\\', "/"));
    let paths = std::iter::once(data)
        .chain(extra)
        .map(path_of)
        .collect::<Vec<_>>()
        .join(", ");
    let text = format!(
        r#"
[node]
label = "{name}"
profile = "local"

[network.http]
bind_address = "127.0.0.1"
port = {port}

[network.cluster]
enabled = false

[storage]
data_paths = [{paths}]
num_shards_init = 1
max_shards_per_node = 1
"#
    );
    let path = dir.join(format!("{name}.toml"));
    std::fs::write(&path, text).expect("config");
    (path, port)
}

fn spawn(config: &Path) -> Child {
    Command::new(env!("CARGO_BIN_EXE_cameodb"))
        .arg("-c")
        .arg(config)
        .env("RUST_LOG", "warn")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn cameodb")
}

async fn await_ready(port: u16) {
    with_tls_provider();
    let client = CameoClient::new(&format!("http://127.0.0.1:{port}")).expect("client");
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if client.health().await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("node on {port} never became healthy");
}

/// Runs `cameodb` to completion, within a minute.
async fn run(args: &[&std::ffi::OsStr]) -> Output {
    tokio::time::timeout(
        Duration::from_secs(60),
        tokio::process::Command::new(env!("CARGO_BIN_EXE_cameodb"))
            .args(args)
            .env("RUST_LOG", "warn")
            .output(),
    )
    .await
    .expect("the command must end, not run")
    .expect("run cameodb")
}

fn text(output: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

/// SIGTERM and a clean shutdown where there is one.
fn stop(mut child: Child) -> String {
    #[cfg(unix)]
    {
        let _ = Command::new("kill")
            .arg("-TERM")
            .arg(child.id().to_string())
            .status();
        let deadline = Instant::now() + Duration::from_secs(60);
        while child.try_wait().expect("wait").is_none() {
            assert!(
                Instant::now() < deadline,
                "the node did not stop on SIGTERM"
            );
            std::thread::sleep(Duration::from_millis(100));
        }
    }
    #[cfg(not(unix))]
    {
        let _ = child.kill();
    }
    text(&child.wait_with_output().expect("node output"))
}

fn kill_hard(mut child: Child) {
    let _ = child.kill();
    let _ = child.wait();
}

fn owner(data: &Path) -> Option<serde_json::Value> {
    std::fs::read(data.join("cameodb.owner"))
        .ok()
        .map(|b| serde_json::from_slice(&b).expect("an owner record"))
}

/// A second node on a held directory is refused before it opens a store, naming the first;
/// `compact` and `unlock` are refused while the node runs; every data path is claimed.
#[tokio::test]
async fn a_second_process_on_a_held_data_directory_is_refused() {
    let dir = tempfile::tempdir().expect("temp dir");
    let data = dir.path().join("data");
    let second_path = dir.path().join("data-2");
    let (first_config, first_port) = config(dir.path(), "first", &data, Some(&second_path));
    let first = spawn(&first_config);
    await_ready(first_port).await;

    for path in [&data, &second_path] {
        let record = owner(path).expect("an owner record in every data path");
        assert_eq!(record["pid"], first.id(), "{record}");
        assert_eq!(record["command"], "node", "{record}");
        assert!(
            record["node"].as_str().is_some_and(|n| !n.is_empty()),
            "{record}"
        );
    }
    let node = owner(&data).unwrap()["node"].as_str().unwrap().to_string();

    // Another node, on another port, on the same directory: refused by the claim, not the bind.
    let (second_config, _) = config(dir.path(), "second", &data, None);
    let refused = run(&["-c".as_ref(), second_config.as_os_str()]).await;
    let said = text(&refused);
    assert!(!refused.status.success(), "{said}");
    assert!(
        said.contains("in use by another CameoDB process on this machine"),
        "{said}"
    );
    assert!(said.contains(&node), "{said}");
    // The second data path alone is held too.
    let (third_config, _) = config(dir.path(), "third", &second_path, None);
    let refused = run(&["-c".as_ref(), third_config.as_os_str()]).await;
    assert!(!refused.status.success(), "{}", text(&refused));

    let compact = run(&["compact".as_ref(), "-c".as_ref(), first_config.as_os_str()]).await;
    let said = text(&compact);
    assert!(!compact.status.success(), "{said}");
    assert!(said.contains(&node), "{said}");

    let unlock = run(&["unlock".as_ref(), data.as_os_str()]).await;
    let said = text(&unlock);
    assert!(!unlock.status.success(), "{said}");
    assert!(said.contains("running process on this machine"), "{said}");

    // The first node is untouched by all of it, and its record goes with a clean stop.
    await_ready(first_port).await;
    let log = stop(first);
    assert!(!log.contains("panicked at"), "{log}");
    assert!(owner(&data).is_none(), "removed on a clean stop");
    assert!(owner(&second_path).is_none(), "removed on a clean stop");

    // With the node stopped, compact runs.
    let compact = run(&["compact".as_ref(), "-c".as_ref(), first_config.as_os_str()]).await;
    assert!(compact.status.success(), "{}", text(&compact));
    assert!(owner(&data).is_none(), "compact removes its record");
}

/// A node killed outright leaves its record, and the next start on this host takes it over. A
/// record naming another host — a container, say — stops the node until `unlock` removes it.
#[tokio::test]
async fn a_stale_record_is_taken_over_on_this_host_and_refused_from_another() {
    let dir = tempfile::tempdir().expect("temp dir");
    let data = dir.path().join("data");
    let (config_path, port) = config(dir.path(), "node", &data, None);

    let node = spawn(&config_path);
    await_ready(port).await;
    let killed_pid = node.id();
    kill_hard(node);
    let left = owner(&data).expect("a killed node leaves its record");
    assert_eq!(left["pid"], killed_pid);

    let node = spawn(&config_path);
    await_ready(port).await;
    let taken = owner(&data).expect("record");
    assert_eq!(taken["pid"], node.id(), "{taken}");
    let log = stop(node);
    assert!(log.contains("taking over a data directory"), "{log}");

    // A record from another host.
    let mut foreign = left.clone();
    foreign["host"] = "3f9c0a7e21b4".into();
    std::fs::write(
        data.join("cameodb.owner"),
        serde_json::to_vec_pretty(&foreign).unwrap(),
    )
    .unwrap();
    let refused = run(&["-c".as_ref(), config_path.as_os_str()]).await;
    let said = text(&refused);
    assert!(!refused.status.success(), "{said}");
    assert!(said.contains("host 3f9c0a7e21b4"), "{said}");
    assert!(said.contains("cameodb unlock"), "{said}");
    assert_eq!(owner(&data), Some(foreign), "the record is left alone");

    let unlock = run(&["unlock".as_ref(), data.as_os_str()]).await;
    let said = text(&unlock);
    assert!(unlock.status.success(), "{said}");
    assert!(said.contains("3f9c0a7e21b4"), "{said}");
    assert!(owner(&data).is_none());

    let node = spawn(&config_path);
    await_ready(port).await;
    stop(node);
}
