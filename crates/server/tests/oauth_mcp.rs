//! OAuth for MCP clients through the real binary (ROADMAP T1).
//!
//! The unit tests in `oauth.rs` and `authz.rs` cover the rules. What they cannot show is the
//! flow a client actually runs, across the routes, the gate, CORS and the MCP transport: a `401`
//! that points to the metadata, the metadata that points to the endpoints, a registration, the
//! approval page and its key, the code, the token, and a tool call made with it.

use std::io::Write as _;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use base64::Engine;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

mod common;

const REDIRECT: &str = "http://127.0.0.1:5555/callback";
const VERIFIER: &str = "a-pkce-verifier-that-is-long-enough-for-rfc-7636-0123456789";

struct TestNode {
    child: Child,
    url: String,
    reader_key: String,
    _dir: tempfile::TempDir,
}

impl TestNode {
    async fn start() -> TestNode {
        let dir = tempfile::tempdir().expect("temp dir");
        let port = common::reserve_port();
        let data = dir.path().join("data");
        std::fs::create_dir_all(&data).expect("data dir");

        let (admin_key, admin_hash) = keygen(dir.path(), "admin", "ops");
        let (reader_key, reader_hash) = keygen(dir.path(), "reader", "user_1");
        let _ = admin_key;
        let secret = dir.path().join("oauth.secret");
        let output = Command::new(env!("CARGO_BIN_EXE_cameodb"))
            .arg("keygen")
            .arg("--oauth-secret-out")
            .arg(&secret)
            .output()
            .expect("run keygen");
        assert!(
            output.status.success(),
            "keygen --oauth-secret-out failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );

        let url = format!("http://127.0.0.1:{port}");
        let config = format!(
            r#"
[node]
label = "oauth-test"
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
key_hash_file = "{admin_hash}"
role = "admin"
label = "ops"

[[security.api_keys]]
key_hash_file = "{reader_hash}"
role = "reader"
label = "user_1"

[security.oauth]
enabled = true
issuer = "{url}"
signing_key_file = "{secret}"
"#,
            data = posix(&data),
            admin_hash = posix(&admin_hash),
            reader_hash = posix(&reader_hash),
            secret = posix(&secret),
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
            url,
            reader_key,
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

    async fn post_mcp(
        &self,
        token: Option<&str>,
        session: Option<&str>,
        body: Value,
    ) -> reqwest::Response {
        let mut request = http()
            .post(format!("{}/mcp", self.url))
            .header("content-type", "application/json")
            .header("accept", "application/json, text/event-stream")
            .header("mcp-protocol-version", "2025-06-18");
        if let Some(token) = token {
            request = request.bearer_auth(token);
        }
        if let Some(session) = session {
            request = request.header("mcp-session-id", session);
        }
        request.json(&body).send().await.expect("mcp post")
    }

    async fn post_form(&self, path: &str, pairs: &[(&str, &str)]) -> reqwest::Response {
        http()
            .post(format!("{}{path}", self.url))
            .header("content-type", "application/x-www-form-urlencoded")
            .body(form(pairs))
            .send()
            .await
            .expect("form post")
    }

    /// Register, approve with `key`, and return the code and the client id.
    async fn code(&self, key: &str) -> (String, String) {
        let registered: Value = http()
            .post(format!("{}/oauth/register", self.url))
            .json(&json!({
                "redirect_uris": [REDIRECT],
                "client_name": "Test <client>",
            }))
            .send()
            .await
            .expect("register")
            .json()
            .await
            .expect("registration json");
        let client_id = registered["client_id"]
            .as_str()
            .expect("client_id")
            .to_string();

        let challenge = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(Sha256::digest(VERIFIER.as_bytes()));
        let query = form(&[
            ("response_type", "code"),
            ("client_id", &client_id),
            ("redirect_uri", REDIRECT),
            ("code_challenge", &challenge),
            ("code_challenge_method", "S256"),
            ("state", "st4te"),
            ("resource", &format!("{}/mcp", self.url)),
        ]);
        let page = http()
            .get(format!("{}/oauth/authorize?{query}", self.url))
            .send()
            .await
            .expect("authorize page");
        assert_eq!(page.status(), 200);
        assert!(
            page.headers()["content-security-policy"]
                .to_str()
                .unwrap()
                .contains("frame-ancestors 'none'")
        );
        let html = page.text().await.unwrap();
        // The client's own name, escaped, and where the browser will be sent.
        assert!(html.contains("Test &lt;client&gt;"), "{html}");
        assert!(html.contains("127.0.0.1:5555"), "{html}");
        let request = hidden_request(&html);

        let wrong = self
            .post_form(
                "/oauth/authorize",
                &[
                    ("request", &request),
                    ("key", "cameo_v1_nope"),
                    ("decision", "approve"),
                ],
            )
            .await;
        assert_eq!(wrong.status(), 200);
        assert!(wrong.text().await.unwrap().contains("not recognised"));

        let approved = self
            .post_form(
                "/oauth/authorize",
                &[("request", &request), ("key", key), ("decision", "approve")],
            )
            .await;
        assert_eq!(approved.status(), 303);
        let location = approved.headers()["location"].to_str().unwrap().to_string();
        assert!(location.starts_with(&format!("{REDIRECT}?")), "{location}");
        let params = query_of(&location);
        assert_eq!(params["state"], "st4te");
        assert_eq!(params["iss"], self.url);
        (params["code"].clone(), client_id)
    }

    async fn token(&self, pairs: &[(&str, &str)]) -> (u16, Value) {
        let resp = self.post_form("/oauth/token", pairs).await;
        let status = resp.status().as_u16();
        (status, resp.json().await.expect("token json"))
    }
}

impl Drop for TestNode {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[tokio::test]
async fn an_mcp_client_signs_in_with_a_key_and_calls_a_tool_with_the_token() {
    let node = TestNode::start().await;

    // 1. No credential: the 401 says where to learn how to get one.
    let resp = node
        .post_mcp(
            None,
            None,
            json!({"jsonrpc": "2.0", "id": 1, "method": "tools/list"}),
        )
        .await;
    assert_eq!(resp.status(), 401);
    let challenge = resp.headers()["www-authenticate"]
        .to_str()
        .unwrap()
        .to_string();
    let metadata_url = format!("{}/.well-known/oauth-protected-resource/mcp", node.url);
    assert!(
        challenge.contains(&format!("resource_metadata=\"{metadata_url}\"")),
        "{challenge}"
    );

    // 2. Discovery, as a client follows it.
    let resource: Value = http()
        .get(&metadata_url)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(resource["resource"], format!("{}/mcp", node.url));
    assert_eq!(resource["authorization_servers"][0], node.url);
    let server: Value = http()
        .get(format!(
            "{}/.well-known/oauth-authorization-server",
            node.url
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(server["issuer"], node.url);
    assert_eq!(server["code_challenge_methods_supported"][0], "S256");
    assert_eq!(
        server["registration_endpoint"],
        format!("{}/oauth/register", node.url)
    );

    // 3. Register, approve with the key, exchange the code.
    let (code, client_id) = node.code(&node.reader_key).await;
    let (status, tokens) = node
        .token(&[
            ("grant_type", "authorization_code"),
            ("code", &code),
            ("redirect_uri", REDIRECT),
            ("code_verifier", VERIFIER),
            ("client_id", &client_id),
            ("resource", &format!("{}/mcp", node.url)),
        ])
        .await;
    assert_eq!(status, 200, "{tokens}");
    assert_eq!(tokens["token_type"], "Bearer");
    assert_eq!(tokens["scope"], "read");
    let access = tokens["access_token"].as_str().unwrap().to_string();
    let refresh = tokens["refresh_token"].as_str().unwrap().to_string();

    // A code is used once.
    let (status, again) = node
        .token(&[
            ("grant_type", "authorization_code"),
            ("code", &code),
            ("redirect_uri", REDIRECT),
            ("code_verifier", VERIFIER),
            ("client_id", &client_id),
        ])
        .await;
    assert_eq!(status, 400);
    assert_eq!(again["error"], "invalid_grant");

    // 4. The token opens /mcp, and a tool answers as the key.
    let resp = node
        .post_mcp(
            Some(&access),
            None,
            json!({
                "jsonrpc": "2.0", "id": 1, "method": "initialize",
                "params": {
                    "protocolVersion": "2025-06-18",
                    "capabilities": {},
                    "clientInfo": {"name": "oauth-test", "version": "0"},
                },
            }),
        )
        .await;
    assert_eq!(resp.status(), 200);
    let session = resp.headers()["mcp-session-id"]
        .to_str()
        .unwrap()
        .to_string();
    let resp = node
        .post_mcp(
            Some(&access),
            Some(&session),
            json!({
                "jsonrpc": "2.0", "id": 2, "method": "tools/call",
                "params": {"name": "list_indexes", "arguments": {}},
            }),
        )
        .await;
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert!(!body.contains("\"error\""), "{body}");

    // 5. REST takes keys, not tokens.
    let resp = http()
        .get(format!("{}/_indexes", node.url))
        .bearer_auth(&access)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);

    // 6. A refresh gives a pair that works the same way.
    let (status, refreshed) = node
        .token(&[
            ("grant_type", "refresh_token"),
            ("refresh_token", &refresh),
            ("client_id", &client_id),
        ])
        .await;
    assert_eq!(status, 200, "{refreshed}");
    let resp = node
        .post_mcp(
            refreshed["access_token"].as_str(),
            Some(&session),
            json!({"jsonrpc": "2.0", "id": 3, "method": "tools/list"}),
        )
        .await;
    assert_eq!(resp.status(), 200);

    // An altered token is refused with the error that tells a client to refresh.
    let mut altered = access.clone();
    altered.push('x');
    let resp = node
        .post_mcp(
            Some(&altered),
            Some(&session),
            json!({"jsonrpc": "2.0", "id": 4, "method": "tools/list"}),
        )
        .await;
    assert_eq!(resp.status(), 401);
    assert!(
        resp.headers()["www-authenticate"]
            .to_str()
            .unwrap()
            .contains("error=\"invalid_token\"")
    );
}

#[tokio::test]
async fn a_wrong_verifier_and_a_denial_get_no_token() {
    let node = TestNode::start().await;

    let (code, client_id) = node.code(&node.reader_key).await;
    let (status, refused) = node
        .token(&[
            ("grant_type", "authorization_code"),
            ("code", &code),
            ("redirect_uri", REDIRECT),
            ("code_verifier", &"z".repeat(50)),
            ("client_id", &client_id),
        ])
        .await;
    assert_eq!(status, 400);
    assert_eq!(refused["error"], "invalid_grant");

    // An unregistered redirect is shown, never followed.
    let page = http()
        .get(format!(
            "{}/oauth/authorize?{}",
            node.url,
            form(&[
                ("response_type", "code"),
                ("client_id", &client_id),
                ("redirect_uri", "https://attacker.example/cb"),
                (
                    "code_challenge",
                    "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"
                ),
                ("code_challenge_method", "S256"),
            ])
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(page.status(), 400);
    assert!(page.headers().get("location").is_none());

    // A registration naming a redirect the node will not send a browser to is refused.
    let resp = http()
        .post(format!("{}/oauth/register", node.url))
        .json(&json!({"redirect_uris": ["http://attacker.example/cb"]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["error"], "invalid_redirect_uri");
}

#[tokio::test]
async fn a_browser_client_may_call_the_token_endpoint_from_any_origin() {
    let node = TestNode::start().await;
    let resp = http()
        .request(
            reqwest::Method::OPTIONS,
            format!("{}/oauth/token", node.url),
        )
        .header("origin", "https://app.example")
        .header("access-control-request-method", "POST")
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.headers()
            .get("access-control-allow-origin")
            .map(|v| v.to_str().unwrap()),
        Some("https://app.example")
    );
    assert!(
        resp.headers()
            .get("access-control-allow-credentials")
            .is_none()
    );

    // Not the data routes: the origin list still governs them.
    let resp = http()
        .request(reqwest::Method::OPTIONS, format!("{}/_indexes", node.url))
        .header("origin", "https://app.example")
        .header("access-control-request-method", "GET")
        .send()
        .await
        .unwrap();
    assert!(resp.headers().get("access-control-allow-origin").is_none());
}

/// Mint a key with the shipped `keygen`, returning the token and the path to its digest.
fn keygen(dir: &std::path::Path, role: &str, label: &str) -> (String, std::path::PathBuf) {
    let key_path = dir.join(format!("{label}.key"));
    let hash_path = dir.join(format!("{label}.hash"));
    let output = Command::new(env!("CARGO_BIN_EXE_cameodb"))
        .args(["keygen", "--role", role, "--label", label, "--key-out"])
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
        .expect("read key")
        .trim()
        .to_string();
    (key, hash_path)
}

fn http() -> reqwest::Client {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
    // A client's browser follows the redirect; the test reads it.
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("http client")
}

fn form(pairs: &[(&str, &str)]) -> String {
    let mut serializer = form_urlencoded::Serializer::new(String::new());
    for (name, value) in pairs {
        serializer.append_pair(name, value);
    }
    serializer.finish()
}

fn query_of(location: &str) -> std::collections::HashMap<String, String> {
    let query = location.split_once('?').map(|(_, q)| q).unwrap_or_default();
    form_urlencoded::parse(query.as_bytes())
        .into_owned()
        .collect()
}

/// The sealed request the approval page carries in its form.
fn hidden_request(html: &str) -> String {
    let marker = r#"name="request" value=""#;
    let start = html.find(marker).expect("the page carries a request") + marker.len();
    let end = start + html[start..].find('"').expect("closing quote");
    html[start..end].to_string()
}

/// Paths go into a TOML string, where a Windows backslash would be an escape.
fn posix(path: &std::path::Path) -> String {
    path.display().to_string().replace('\\', "/")
}
