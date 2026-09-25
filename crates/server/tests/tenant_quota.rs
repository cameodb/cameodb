//! `[security.tenants.*]` through the real binary (M4).
//!
//! The unit tests in `node/quota.rs` cover the decisions. What they cannot show is that the
//! decisions are *reached*: that the tenant a key carries becomes the owner stamped at the mint,
//! that both mint paths count, that a write lands in the owner's usage and not the writer's, and
//! that a reading taken off the write path eventually refuses. Each of those is a wire between
//! the HTTP layer, the orchestrator and storage.

use std::io::Write as _;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use serde_json::{Value, json};

mod common;

/// A node with four tenanted keys and one operator key.
///
/// `acme` may own two indexes; `tiny` may occupy one byte, so its first reading refuses it;
/// `globex` has no `[security.tenants]` entry and so no ceiling at all.
struct TestNode {
    child: Child,
    url: String,
    /// Admin with no tenant: the operator.
    operator: String,
    /// Admin carrying `acme`, for the explicit mint and the drop.
    acme_admin: String,
    acme: String,
    globex: String,
    tiny: String,
    _dir: tempfile::TempDir,
}

impl TestNode {
    async fn start() -> TestNode {
        let dir = tempfile::tempdir().expect("temp dir");
        let port = common::reserve_port();
        let data = dir.path().join("data");
        std::fs::create_dir_all(&data).expect("data dir");

        let (operator, operator_hash) = keygen(dir.path(), "admin", "operator");
        let (acme_admin, acme_admin_hash) = keygen(dir.path(), "admin", "acme-admin");
        let (acme, acme_hash) = keygen(dir.path(), "writer", "acme");
        let (globex, globex_hash) = keygen(dir.path(), "writer", "globex");
        let (tiny, tiny_hash) = keygen(dir.path(), "writer", "tiny");

        let config = format!(
            r#"
[node]
label = "quota-test"
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

[security]
enabled = true

[[security.api_keys]]
key_hash_file = "{operator_hash}"
role = "admin"
label = "operator"

[[security.api_keys]]
key_hash_file = "{acme_admin_hash}"
role = "admin"
label = "acme-admin"
tenant = "acme"

[[security.api_keys]]
key_hash_file = "{acme_hash}"
role = "writer"
label = "acme"
tenant = "acme"

[[security.api_keys]]
key_hash_file = "{globex_hash}"
role = "writer"
label = "globex"
tenant = "globex"

[[security.api_keys]]
key_hash_file = "{tiny_hash}"
role = "writer"
label = "tiny"
tenant = "tiny"

[security.tenants.acme]
max_indexes = 2

[security.tenants.tiny]
max_bytes = 1
"#,
            data = posix(&data),
            operator_hash = posix(&operator_hash),
            acme_admin_hash = posix(&acme_admin_hash),
            acme_hash = posix(&acme_hash),
            globex_hash = posix(&globex_hash),
            tiny_hash = posix(&tiny_hash),
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
            operator,
            acme_admin,
            acme,
            globex,
            tiny,
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

    async fn write(&self, key: &str, index: &str, id: &str) -> (u16, Value) {
        let resp = http()
            .put(format!("{}/api/{index}/document", self.url))
            .bearer_auth(key)
            .json(&json!({"id": id, "doc": {"id": id, "title": "a title with some words in it"}}))
            .send()
            .await
            .expect("write");
        let status = resp.status().as_u16();
        (status, resp.json().await.unwrap_or(Value::Null))
    }

    async fn declare(&self, key: &str, index: &str) -> (u16, Value) {
        let resp = http()
            .put(format!("{}/api/{index}/_config", self.url))
            .bearer_auth(key)
            .json(&json!({
                "fields": {
                    "id": {"name": "id", "field_type": "text", "indexed": true},
                    "title": {"name": "title", "field_type": "text", "indexed": true},
                }
            }))
            .send()
            .await
            .expect("declare");
        let status = resp.status().as_u16();
        (status, resp.json().await.unwrap_or(Value::Null))
    }

    async fn drop_index(&self, key: &str, index: &str, delete_schema: bool) -> u16 {
        http()
            .delete(format!(
                "{}/api/{index}?delete_schema={delete_schema}",
                self.url
            ))
            .bearer_auth(key)
            .send()
            .await
            .expect("drop")
            .status()
            .as_u16()
    }
}

impl Drop for TestNode {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Mint a key with the shipped `keygen`, returning the token and the path to its digest.
fn keygen(dir: &std::path::Path, role: &str, label: &str) -> (String, std::path::PathBuf) {
    let key_path = dir.join(format!("{label}.key"));
    let hash_path = dir.join(format!("{label}.hash"));
    let output = Command::new(env!("CARGO_BIN_EXE_cameodb"))
        .arg("keygen")
        .arg("--role")
        .arg(role)
        .arg("--label")
        .arg(label)
        .arg("--key-out")
        .arg(&key_path)
        .arg("--hash-out")
        .arg(&hash_path)
        .output()
        .expect("run keygen");
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

/// Paths go into a TOML string, where a Windows backslash would be an escape.
fn posix(path: &std::path::Path) -> String {
    path.display().to_string().replace('\\', "/")
}

fn detail(body: &Value) -> String {
    body["details"]
        .as_str()
        .or_else(|| body["error"].as_str())
        .unwrap_or_default()
        .to_string()
}

/// A tenant at `max_indexes` cannot mint another, by either path — and nobody else is touched.
///
/// Both mints are covered because they are two call sites: the implicit one inside the write
/// path and the explicit `PUT /_config`. Checking only one would leave the other as the way
/// round the ceiling. Dropping an index with its schema hands the slot back, since the count is
/// taken from live schemas rather than kept as a running total that could drift.
#[tokio::test]
async fn a_tenant_is_refused_a_mint_past_its_index_ceiling() {
    let node = TestNode::start().await;

    for index in ["acme-1", "acme-2"] {
        let (status, body) = node.write(&node.acme, index, "d1").await;
        assert_eq!(
            status, 200,
            "acme's mint of {index} is within its ceiling: {body}"
        );
    }

    let (status, body) = node.write(&node.acme, "acme-3", "d1").await;
    assert_eq!(status, 403, "a third mint must be refused: {body}");
    let message = detail(&body);
    assert!(
        message.contains("acme") && message.contains("max_indexes"),
        "the refusal must name the tenant and the ceiling: {body}"
    );

    // The explicit mint is counted too.
    let (status, body) = node.declare(&node.acme_admin, "acme-4").await;
    assert_eq!(
        status, 403,
        "PUT /_config must not be a way round the ceiling: {body}"
    );

    // A ceiling on minting is not a ceiling on writing: the indexes acme owns still take data.
    let (status, body) = node.write(&node.acme, "acme-1", "d2").await;
    assert_eq!(
        status, 200,
        "a write to an owned index is not a mint: {body}"
    );

    // Another tenant, with no ceiling configured, is untouched.
    for index in ["globex-1", "globex-2", "globex-3"] {
        let (status, body) = node.write(&node.globex, index, "d1").await;
        assert_eq!(status, 200, "globex has no ceiling: {body}");
    }

    // Nor is the operator, whose indexes belong to no tenant.
    for index in ["ops-1", "ops-2", "ops-3"] {
        let (status, body) = node.write(&node.operator, index, "d1").await;
        assert_eq!(
            status, 200,
            "an unowned index counts against no one: {body}"
        );
    }

    // Clearing an index's data keeps the index — its schema, and so its owner — so the slot
    // stays taken ...
    assert_eq!(
        node.drop_index(&node.acme_admin, "acme-2", false).await,
        200
    );
    let (status, body) = node.write(&node.acme, "acme-3", "d1").await;
    assert_eq!(status, 403, "an emptied index is still an index: {body}");

    // ... and dropping the schema too is what removes it and hands the slot back.
    assert_eq!(node.drop_index(&node.acme_admin, "acme-2", true).await, 200);
    let (status, body) = node.write(&node.acme, "acme-3", "d1").await;
    assert_eq!(
        status, 200,
        "the dropped index's slot is free again: {body}"
    );
}

/// A tenant at `max_bytes` is refused writes once the usage reading catches up with them.
///
/// The first write is allowed — there is no reading yet, which is the opening of the overshoot
/// window the docs state — and starts one. A later write, once a reading exists that has the
/// tenant at or past its single permitted byte, is refused. The deadline covers one refresh
/// interval plus slack, which is exactly the window the design admits.
#[tokio::test]
async fn a_tenant_over_its_byte_ceiling_is_refused_once_measured() {
    let node = TestNode::start().await;

    let (status, body) = node.write(&node.tiny, "tiny-1", "d0").await;
    assert_eq!(
        status, 200,
        "the first write has no reading to refuse on: {body}"
    );

    let deadline = Instant::now() + Duration::from_secs(30);
    let mut n = 1;
    let refusal = loop {
        let (status, body) = node.write(&node.tiny, "tiny-1", &format!("d{n}")).await;
        if status == 403 {
            break body;
        }
        assert_eq!(
            status, 200,
            "a write is either served or refused on quota: {body}"
        );
        assert!(
            Instant::now() < deadline,
            "tiny was never refused although it holds data past a one-byte ceiling"
        );
        n += 1;
        tokio::time::sleep(Duration::from_millis(250)).await;
    };
    let message = detail(&refusal);
    assert!(
        message.contains("tiny") && message.contains("max_bytes"),
        "the refusal must name the tenant and the ceiling: {refusal}"
    );

    // A new index is no way round it: the bytes of the index a write would mint are the
    // minting tenant's.
    let (status, body) = node.write(&node.tiny, "tiny-2", "d1").await;
    assert_eq!(
        status, 403,
        "minting a fresh index must not escape the byte ceiling: {body}"
    );

    // Bytes are charged to the index's owner, not the writer. The operator's write into tiny's
    // index is refused for the same reason tiny's would be ...
    let (status, body) = node.write(&node.operator, "tiny-1", "op").await;
    assert_eq!(
        status, 403,
        "the index is still tiny's, whoever writes to it: {body}"
    );

    // ... and other tenants are untouched.
    let (status, body) = node.write(&node.globex, "globex-1", "d1").await;
    assert_eq!(status, 200, "globex has no ceiling: {body}");
}
