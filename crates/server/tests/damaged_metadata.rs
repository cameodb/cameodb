//! What a node does at startup with a damaged `metadata.redb`.
//!
//! The file holds cluster bookkeeping — configuration, shard assignments, the node registry and
//! the ring snapshot — not documents. A single node sets a damaged one aside and rebuilds it from
//! its shards; a cluster member refuses to start and says why. Neither panics.
//!
//! Unix only: the damage is laid over a file a clean shutdown wrote, and the node is stopped
//! cleanly with SIGTERM. Windows has no signal this test can send, and a killed node never
//! writes the page holding redb's allocator state that the damage is copied from.
#![cfg(unix)]

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use client::CameoClient;
use serde_json::json;

mod common;

fn with_tls_provider() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

fn write_config(dir: &Path, data: &Path, port: u16, clustered: bool) -> PathBuf {
    let cluster = if clustered {
        format!(
            "enabled = true\nbind_address = \"127.0.0.1\"\ncluster_port = {}\n",
            common::reserve_port()
        )
    } else {
        "enabled = false\n".to_string()
    };
    let config = format!(
        r#"
[node]
label = "metadata-node"
profile = "local"

[network.http]
bind_address = "127.0.0.1"
port = {port}

[network.cluster]
{cluster}
[storage]
data_paths = ["{data}"]
num_shards_init = 1
max_shards_per_node = 1
"#,
        data = data.display().to_string().replace('\\', "/"),
    );
    let path = dir.join(if clustered {
        "clustered.toml"
    } else {
        "single.toml"
    });
    std::fs::write(&path, config).expect("config");
    path
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

async fn await_ready(client: &CameoClient) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if client.health().await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("node never became healthy");
}

/// Stops the node the way an operator does, and returns what it printed: SIGTERM and a clean
/// shutdown, which is what preceded the damage found on 2026-11-03.
fn stop(mut child: Child) -> String {
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
    // Both streams: the log is on stdout, a panic's report on stderr.
    let output = child.wait_with_output().expect("node output");
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

/// The damage found on 2026-11-03: every page that holds the table names — the user tables'
/// root — overwritten with the page holding redb's own system tree. Pages are found by content,
/// not by redb's commit-slot layout; only the page size is read from the header.
fn damage_table_root(path: &Path) {
    let mut bytes = std::fs::read(path).expect("metadata.redb");
    // redb's magic number (9 bytes), its god byte, two of padding, then the page size.
    let page_size = u32::from_le_bytes(bytes[12..16].try_into().unwrap()) as usize;
    assert!(
        page_size.is_power_of_two() && page_size >= 512,
        "{page_size}"
    );
    let holds = |bytes: &[u8], page: usize, needle: &[u8]| {
        bytes[page * page_size..(page + 1) * page_size]
            .windows(needle.len())
            .any(|w| w == needle)
    };
    let pages = bytes.len() / page_size;
    let system = (1..pages)
        .find(|&p| holds(&bytes, p, b"allocator_state"))
        .expect("a page holding redb's system tree");
    let donor = bytes[system * page_size..(system + 1) * page_size].to_vec();
    let mut damaged = 0;
    for page in 1..pages {
        if page != system && holds(&bytes, page, b"shard_assignments") {
            bytes[page * page_size..(page + 1) * page_size].copy_from_slice(&donor);
            damaged += 1;
        }
    }
    assert!(damaged > 0, "no page held the table names");
    std::fs::write(path, bytes).expect("write damaged file");
}

fn set_aside(data: &Path) -> Vec<PathBuf> {
    std::fs::read_dir(data)
        .expect("data dir")
        .map(|e| e.expect("entry").path())
        .filter(|p| {
            p.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("metadata.redb.damaged-")
        })
        .collect()
}

/// One node writes documents and stops cleanly; its `metadata.redb` is then damaged. A cluster
/// member on that directory refuses to start, naming the file and the remedy, and leaves it in
/// place. A single node sets it aside, rebuilds it, and serves every document.
#[tokio::test]
async fn a_damaged_metadata_redb_is_refused_in_a_cluster_and_rebuilt_on_a_single_node() {
    with_tls_provider();
    let dir = tempfile::tempdir().expect("temp dir");
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).expect("data dir");
    let port = common::reserve_port();
    let single = write_config(dir.path(), &data, port, false);
    let client = CameoClient::new(&format!("http://127.0.0.1:{port}")).expect("client");

    let node = spawn(&single);
    await_ready(&client).await;
    for i in 0..30 {
        let id = format!("doc-{i}");
        client
            .write_document(
                "ledger",
                &id,
                &json!({"id": id, "title": format!("ledger entry {i}")}),
                None,
            )
            .await
            .expect("write");
    }
    let log = stop(node);
    assert!(!log.contains("panicked at"), "{log}");

    let metadata = data.join("metadata.redb");
    damage_table_root(&metadata);
    let damaged = std::fs::read(&metadata).expect("read");

    // A cluster member refuses, before it binds anything.
    let clustered = write_config(dir.path(), &data, port, true);
    let output = tokio::time::timeout(
        Duration::from_secs(60),
        tokio::process::Command::new(env!("CARGO_BIN_EXE_cameodb"))
            .arg("-c")
            .arg(&clustered)
            .env("RUST_LOG", "warn")
            .output(),
    )
    .await
    .expect("the node must stop, not run")
    .expect("run cameodb");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success(), "{stderr}");
    assert!(stderr.contains("metadata.redb is damaged"), "{stderr}");
    assert!(stderr.contains("rebuild the node"), "{stderr}");
    assert!(
        set_aside(&data).is_empty(),
        "a cluster member sets nothing aside"
    );
    // Left in place for the operator, every page as it was. The header is not compared: redb
    // marks a file it opened for writing as needing recovery, and clears the mark only on a
    // clean close, which a failed integrity check does not reach.
    let now = std::fs::read(&metadata).expect("read");
    let differing: Vec<usize> = (0..now.len().max(damaged.len()))
        .filter(|&i| now.get(i) != damaged.get(i))
        .collect();
    assert!(
        differing.iter().all(|&i| i < 512),
        "bytes past redb's header changed: {:?}",
        &differing[..differing.len().min(20)]
    );

    // A single node sets it aside and serves every document.
    let node = spawn(&single);
    await_ready(&client).await;
    let aside = set_aside(&data);
    assert_eq!(aside.len(), 1, "{aside:?}");
    assert!(metadata.is_file(), "a new metadata.redb is created");

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let found = client
            .search("ledger", "ledger", Some(100), None, None, None)
            .await
            .ok()
            .and_then(|r| r["hits"].as_array().map(Vec::len))
            .unwrap_or(0);
        if found == 30 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "{found} of 30 documents served after the rebuild"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let by_id = client
        .search("ledger", "id:doc-17", Some(1), None, None, None)
        .await
        .expect("id lookup");
    assert_eq!(by_id["hits"].as_array().map(Vec::len), Some(1), "{by_id}");

    let log = stop(node);
    assert!(!log.contains("panicked at"), "{log}");
    assert!(log.contains("set aside"), "{log}");

    // And the next start reads the rebuilt file cleanly, setting nothing more aside.
    let node = spawn(&single);
    await_ready(&client).await;
    let log = stop(node);
    assert!(!log.contains("panicked at"), "{log}");
    assert_eq!(set_aside(&data).len(), 1);
}
