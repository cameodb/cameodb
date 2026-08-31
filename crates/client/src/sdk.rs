use anyhow::{Context, Result, bail};
use reqwest::{Client, Url, header};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};
use std::fmt;
use std::path::Path;
use zeroize::Zeroize;

/// Which TLS verification to relax, and for which connection.
///
/// The two are deliberately separate. They were previously one flag, so asking to accept
/// a self-signed certificate on a *data source* also disabled verification on the
/// connection to CameoDB itself — a wider hole than anyone asked for.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TlsTrust {
    /// Accept invalid certificates from the CameoDB server.
    pub insecure_server: bool,
    /// Accept invalid certificates from remote schema/data source URLs.
    pub insecure_source: bool,
}

/// Key shape, duplicated from the server's `auth.rs`.
///
/// The client cannot depend on the server crate, so these three constants are copied. They
/// are checked here so that a typo'd key fails on this side with a message that says what a
/// key looks like, instead of arriving as an indistinguishable 401. `auth.rs` remains the
/// authority: this side only ever *narrows* what gets sent, never widens what is accepted.
const KEY_PREFIX: &str = "cameo_v1_";
const KEY_BODY_LEN: usize = 43;

/// An API key held in the clear, on its way into an `Authorization` header.
///
/// Mirrors the server's `ApiKey`: redacted `Debug`, never serialized, scrubbed on drop.
pub struct Credential(String);

impl Credential {
    /// Accept a key typed on a command line, read from a file, or exported in the
    /// environment.
    pub fn parse(raw: &str) -> Result<Self> {
        let token = raw.trim();
        if token.is_empty() {
            bail!("API key is empty");
        }
        let shaped = token
            .strip_prefix(KEY_PREFIX)
            .is_some_and(|body| body.len() == KEY_BODY_LEN && body.chars().all(is_base64url));
        if !shaped {
            bail!(
                "that does not look like a CameoDB API key. Keys are '{KEY_PREFIX}' followed by \
                 {KEY_BODY_LEN} characters — `cameodb keygen --role reader` mints one"
            );
        }
        Ok(Self(token.to_string()))
    }

    /// Read a key from a file holding it and nothing else.
    pub fn from_file(path: &Path) -> Result<Self> {
        if !path.exists() {
            bail!("API key file not found: {}", path.display());
        }
        warn_if_key_file_is_readable_by_others(path);
        let contents = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read API key file: {}", path.display()))?;
        if contents.trim().lines().count() > 1 {
            bail!(
                "API key file holds {} lines: {}. It contains one key and nothing else",
                contents.trim().lines().count(),
                path.display()
            );
        }
        Self::parse(&contents).with_context(|| format!("in {}", path.display()))
    }

    /// The same non-reversible fingerprint the server logs, so a refusal on this side can be
    /// matched against the node's log without either end ever printing the key.
    pub fn key_id(&self) -> String {
        Sha256::digest(self.0.as_bytes())
            .iter()
            .take(4)
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }

    fn header_value(&self) -> Result<header::HeaderValue> {
        let mut value = header::HeaderValue::from_str(&format!("Bearer {}", self.0))
            .context("API key contains characters that cannot go in a header")?;
        // Keeps the key out of reqwest's own logging, and is what makes reqwest drop the
        // header on a cross-host redirect rather than forwarding it to wherever a
        // misconfigured server points.
        value.set_sensitive(true);
        Ok(value)
    }
}

fn is_base64url(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '-' || c == '_'
}

impl fmt::Debug for Credential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Credential(<redacted:{}>)", self.key_id())
    }
}

impl Clone for Credential {
    /// Cloned when the interactive session rebuilds its HTTP client. Each clone scrubs
    /// itself on drop, so the copy is no longer lived than the original.
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl Drop for Credential {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

/// The server checks a `key_hash_file` for *group/other-writable* — a hash is public, only
/// tampering matters. A key file is the opposite: disclosure is the whole risk, so this
/// checks for readable-by-anyone-else.
#[cfg(unix)]
fn warn_if_key_file_is_readable_by_others(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    if let Ok(meta) = std::fs::metadata(path) {
        let mode = meta.permissions().mode();
        if mode & 0o077 != 0 {
            eprintln!(
                "⚠️  {} is readable by other users (mode {:o}). chmod 600 it.",
                path.display(),
                mode & 0o777
            );
        }
    }
}

#[cfg(not(unix))]
fn warn_if_key_file_is_readable_by_others(_path: &Path) {}

/// True for `localhost`, `127.0.0.0/8` and `::1`.
///
/// `localhost` is taken at its word rather than resolved: a hosts file that points it
/// elsewhere is a machine already lost, and resolving here would mean a DNS lookup before
/// every client construction.
fn is_loopback(url: &Url) -> bool {
    let Some(host) = url.host_str() else {
        return false;
    };
    let host = host.trim_start_matches('[').trim_end_matches(']');
    if host.eq_ignore_ascii_case("localhost") {
        return true;
    }
    host.parse::<std::net::IpAddr>()
        .is_ok_and(|ip| ip.is_loopback())
}

/// Refuse to put a bearer token on an unencrypted wire to another machine.
///
/// Loopback is exempt because the token never leaves the host, which is what makes the
/// single-node default (`http://localhost:9480` plus a key) work without a flag.
fn guard_plaintext(url: &Url, allowed: bool) -> Result<()> {
    if url.scheme() == "https" || is_loopback(url) || allowed {
        return Ok(());
    }
    bail!(
        "refusing to send an API key to {} over plaintext HTTP — anyone on the path can read \
         it and reuse it. Use https://, or pass --allow-plaintext-key if this hop is already \
         protected (an SSH tunnel, a service mesh)",
        url.origin().ascii_serialization()
    )
}

/// Scheme, host and port. A credential is bound to one of these.
pub fn origin_of(url: &str) -> String {
    match Url::parse(url) {
        Ok(url) => url.origin().ascii_serialization(),
        Err(_) => url.to_string(),
    }
}

/// How the client authenticates, and what it will risk to do so.
#[derive(Debug, Clone, Default)]
pub struct ClientAuth {
    pub credential: Option<Credential>,
    /// Send the key over plaintext HTTP to a non-loopback host.
    ///
    /// Separate from [`TlsTrust::insecure_server`] on purpose: that one accepts a bad
    /// certificate on a connection that is still encrypted, this one puts a bearer token on
    /// the wire in the clear. Granting one must not grant the other.
    pub allow_plaintext: bool,
}

#[derive(Debug, Clone)]
pub struct CameoClient {
    base_url: Url,
    http: Client,
    /// Separate client for fetching remote source files, so its trust settings cannot
    /// affect requests to the CameoDB server.
    source_http: Client,
    /// Kept for reporting only — the key itself rides in `http`'s default headers.
    credential: Option<Credential>,
}

impl CameoClient {
    pub fn new(url: &str) -> Result<Self> {
        Self::new_with_trust(url, TlsTrust::default())
    }

    pub fn new_with_trust(url: &str, trust: TlsTrust) -> Result<Self> {
        Self::new_with_options(url, trust, ClientAuth::default())
    }

    pub fn new_with_options(url: &str, trust: TlsTrust, auth: ClientAuth) -> Result<Self> {
        let base_url = Url::parse(url).context("Invalid URL")?;

        if let Some(credential) = &auth.credential {
            guard_plaintext(&base_url, auth.allow_plaintext)?;
            if trust.insecure_server && base_url.scheme() == "https" && !is_loopback(&base_url) {
                eprintln!(
                    "⚠️  Sending API key {} to {} with certificate verification disabled. \
                     Anyone able to present a certificate can read it.",
                    credential.key_id(),
                    base_url.host_str().unwrap_or("that host")
                );
            }
        }

        // Configure client for large file downloads with appropriate timeouts
        let build = |insecure: bool, credential: Option<&Credential>| -> Result<Client> {
            let mut builder = Client::builder()
                .timeout(std::time::Duration::from_secs(300)) // 5 minute timeout for large files
                .connect_timeout(std::time::Duration::from_secs(30))
                .pool_idle_timeout(std::time::Duration::from_secs(90));
            if insecure {
                builder = builder.danger_accept_invalid_certs(true);
            }
            if let Some(credential) = credential {
                // A default header rather than a per-call `.bearer_auth()`: every request
                // this client makes is to CameoDB, so there is no call site that could be
                // forgotten and none that should be exempt.
                let mut headers = header::HeaderMap::new();
                headers.insert(header::AUTHORIZATION, credential.header_value()?);
                builder = builder.default_headers(headers);
            }
            builder.build().context("Failed to build HTTP client")
        };

        Ok(Self {
            http: build(trust.insecure_server, auth.credential.as_ref())?,
            // Never carries the credential. A schema or data-source URL is somebody else's
            // host; the key it takes to write to CameoDB has no business going there.
            source_http: build(trust.insecure_source, None)?,
            base_url,
            credential: auth.credential,
        })
    }

    /// Fingerprint of the key in use, if any. Never the key.
    pub fn key_id(&self) -> Option<String> {
        self.credential.as_ref().map(Credential::key_id)
    }

    /// What to add to a refusal so the reader knows which side to fix.
    ///
    /// The server's own message says what the endpoint required; only this side knows
    /// whether a key was sent at all and which one, which is the part that turns "401
    /// Unauthorized" into something actionable.
    fn refusal_hint(&self, status: reqwest::StatusCode) -> String {
        match (status.as_u16(), self.credential.as_ref()) {
            (401, None) | (403, None) => format!(
                "\n  hint: no API key was sent. Pass --api-key-file <path>, --api-key <key>, or \
                 set CAMEODB_API_KEY, then retry against {}",
                self.base_url.origin().ascii_serialization()
            ),
            (401, Some(credential)) => format!(
                "\n  hint: key {} is not in this node's [security] keyring. `cameodb keygen` \
                 prints the stanza a node needs to accept a key",
                credential.key_id()
            ),
            (403, Some(credential)) => format!(
                "\n  hint: key {} authenticated, but its role or allowed_indexes do not cover \
                 this request",
                credential.key_id()
            ),
            _ => String::new(),
        }
    }

    pub fn base_url(&self) -> &Url {
        &self.base_url
    }

    pub async fn health(&self) -> Result<HealthResponse> {
        let url = self.base_url.join("_cluster/health")?;
        let resp = self
            .http
            .get(url)
            .send()
            .await
            .context("Failed to send health request")?;

        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!(
                "Health check failed: {} - {}{}",
                status,
                text,
                self.refusal_hint(status)
            );
        }

        resp.json::<HealthResponse>()
            .await
            .context("Failed to parse health response")
    }

    pub async fn list_indexes(&self, include_data_size: bool) -> Result<ListIndexesResponse> {
        let mut url = self.base_url.join("_indexes")?;
        if include_data_size {
            url.set_query(Some("data_size=true"));
        }
        let resp = self.http.get(url).send().await?;
        if !resp.status().is_success() {
            let status = resp.status();
            anyhow::bail!(
                "Failed to list indexes: {}{}",
                status,
                self.refusal_hint(status)
            );
        }
        resp.json()
            .await
            .context("Failed to parse indexes response")
    }

    /// Run a search, optionally taking one page of the result.
    ///
    /// `offset` is the paging half of `limit`: with `limit` as the page size, page N starts at
    /// `offset = N * limit`. The node bounds `offset + limit` by its `max_search_limit`, since
    /// it fetches that many hits to serve the page, so a deep page is refused for the same
    /// reason a large limit is. `None` and `Some(0)` mean the same thing.
    ///
    /// The same values can be written into the query itself — `limit 10 offset 20` — which is
    /// how the REPL expresses them; passing them here wins over the inline form.
    pub async fn search(
        &self,
        index: &str,
        query: &str,
        limit: Option<usize>,
        offset: Option<usize>,
        fields: Option<Vec<String>>,
        sort: Option<storage::SortSpec>,
    ) -> Result<JsonValue> {
        let url = self.base_url.join(&format!("api/{}/search", index))?;
        let body = serde_json::json!({
            "query": query,
            "limit": limit,
            "offset": offset,
            "fields": fields,
            "sort": sort,
        });

        let resp = self.http.post(url).json(&body).send().await?;
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!(
                "Search failed: {} - {}{}",
                status,
                text,
                self.refusal_hint(status)
            );
        }
        resp.json().await.context("Failed to parse search response")
    }

    pub async fn get_index_config(&self, index: &str) -> Result<IndexConfigResponse> {
        let url = self
            .base_url
            .join(&format!("api/{}/_config", index))
            .context("Invalid config URL")?;
        let resp = self.http.get(url).send().await?;
        if !resp.status().is_success() {
            let status = resp.status();
            anyhow::bail!(
                "Failed to fetch index config: {}{}",
                status,
                self.refusal_hint(status)
            );
        }
        resp.json()
            .await
            .context("Failed to parse index config response")
    }

    pub async fn put_index_config(&self, index: &str, config: &JsonValue) -> Result<()> {
        let url = self
            .base_url
            .join(&format!("api/{}/_config", index))
            .context("Invalid config URL")?;
        let resp = self.http.put(url).json(config).send().await?;
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!(
                "Failed to set index config: {} - {}{}",
                status,
                text,
                self.refusal_hint(status)
            );
        }
        Ok(())
    }

    pub async fn delete_index(&self, index: &str, delete_schema: bool) -> Result<JsonValue> {
        let mut url = self
            .base_url
            .join(&format!("api/{}", index))
            .context("Invalid delete URL")?;
        if delete_schema {
            url.set_query(Some("delete_schema=true"));
        }

        let resp = self.http.delete(url).send().await?;
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!(
                "Delete index failed: {} - {}{}",
                status,
                text,
                self.refusal_hint(status)
            );
        }

        resp.json()
            .await
            .context("Failed to parse delete index response")
    }

    /// Write a single document.
    ///
    /// `routing_key` decides which shard owns the document; passing `None` lets the node
    /// default it to `id`, which keeps the write a unicast to one shard rather than a
    /// broadcast. Pass one explicitly to co-locate related documents on a shard.
    ///
    /// For loading many documents, prefer [`Self::bulk_index`] or
    /// [`Self::stream_index_ndjson`] — one request per document spends most of its time on
    /// round trips. This exists for the case where a single write is the actual operation,
    /// and for measuring what one write costs.
    pub async fn write_document(
        &self,
        index: &str,
        id: &str,
        doc: &JsonValue,
        routing_key: Option<&str>,
    ) -> Result<JsonValue> {
        let url = self
            .base_url
            .join(&format!("api/{}/document", index))
            .context("Invalid document URL")?;

        let mut payload = serde_json::json!({ "id": id, "doc": doc });
        if let Some(key) = routing_key {
            payload["routing_key"] = JsonValue::String(key.to_string());
        }

        let resp = self.http.put(url).json(&payload).send().await?;
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!(
                "Write failed: {} - {}{}",
                status,
                text,
                self.refusal_hint(status)
            );
        }
        resp.json().await.context("Failed to parse write response")
    }

    /// Remove one document by its key.
    ///
    /// `routing_key` is needed only where the index routes by a field that is not the document
    /// key — a tenant or a customer id. On a default index, and on one with a shadow key such as
    /// `sha1`, the key routes on its own and `None` is correct.
    ///
    /// Idempotent: an id the index does not hold is answered as deleted, the same way writing
    /// over an existing document is answered as created. An index that does not exist is a 404.
    pub async fn delete_document(
        &self,
        index: &str,
        id: &str,
        routing_key: Option<&str>,
    ) -> Result<JsonValue> {
        let mut url = self
            .base_url
            .join(&format!("api/{}/document", index))
            .context("Invalid delete document URL")?;
        {
            let mut query = url.query_pairs_mut();
            query.append_pair("id", id);
            if let Some(key) = routing_key {
                query.append_pair("routing_key", key);
            }
        }

        let resp = self.http.delete(url).send().await?;
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!(
                "Delete document failed: {} - {}{}",
                status,
                text,
                self.refusal_hint(status)
            );
        }

        resp.json()
            .await
            .context("Failed to parse delete document response")
    }

    /// Remove many documents by key, in one request.
    ///
    /// Each entry is either a bare id or `{"id": …, "routing_key": …}`, the latter for an index
    /// that routes by a field other than the key. Ids belonging to different shards are grouped
    /// and dispatched by the node, so one call covers a batch that spans the cluster.
    ///
    /// The reply counts what was deleted and lists per-id errors: an id that cannot be routed is
    /// reported against that id rather than failing the batch.
    pub async fn delete_documents(&self, index: &str, ids: &[JsonValue]) -> Result<JsonValue> {
        let url = self
            .base_url
            .join(&format!("api/{}/_bulk/delete", index))
            .context("Invalid bulk delete URL")?;

        let resp = self.http.post(url).json(&ids).send().await?;
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!(
                "Bulk delete failed: {} - {}{}",
                status,
                text,
                self.refusal_hint(status)
            );
        }

        resp.json()
            .await
            .context("Failed to parse bulk delete response")
    }

    pub async fn bulk_index(&self, index: &str, batch: &[JsonValue]) -> Result<JsonValue> {
        let url = self
            .base_url
            .join(&format!("api/{}/_bulk", index))
            .context("Invalid bulk URL")?;
        let resp = self.http.post(url).json(batch).send().await?;
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!(
                "Bulk ingest failed: {} - {}{}",
                status,
                text,
                self.refusal_hint(status)
            );
        }
        resp.json()
            .await
            .context("Failed to parse bulk ingest response")
    }

    pub async fn stream_index_ndjson(&self, index: &str, body: Vec<u8>) -> Result<JsonValue> {
        let url = self
            .base_url
            .join(&format!("api/{}/document/stream", index))
            .context("Invalid streaming ingest URL")?;
        let resp = self
            .http
            .post(url)
            .header(header::CONTENT_TYPE, "application/x-ndjson")
            .body(body)
            .send()
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!(
                "Streaming ingest failed: {} - {}{}",
                status,
                text,
                self.refusal_hint(status)
            );
        }
        resp.json()
            .await
            .context("Failed to parse streaming ingest response")
    }

    /// Expose underlying HTTP client for auxiliary requests (e.g., fetching CSV schema samples)
    pub fn http(&self) -> &Client {
        &self.http
    }

    /// Client for fetching remote schema/data sources. Governed by `--insecure-source`,
    /// never by `--insecure`.
    pub fn source_http(&self) -> &Client {
        &self.source_http
    }

    pub async fn admin_memory_stats(&self) -> Result<AdminMemoryResponse> {
        let url = self.base_url.join("_admin/memory")?;
        let resp = self.http.get(url).send().await?;
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!(
                "Admin memory stats failed: {} - {}{}",
                status,
                text,
                self.refusal_hint(status)
            );
        }
        resp.json()
            .await
            .context("Failed to parse memory stats response")
    }

    pub async fn admin_memory_purge(&self, force: bool) -> Result<AdminMemoryResponse> {
        let mut url = self.base_url.join("_admin/memory/purge")?;
        if force {
            url.query_pairs_mut().append_pair("force", "true");
        }
        let resp = self.http.post(url).send().await?;
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!(
                "Admin memory purge failed: {} - {}{}",
                status,
                text,
                self.refusal_hint(status)
            );
        }
        resp.json()
            .await
            .context("Failed to parse memory purge response")
    }

    pub async fn admin_index_commit(&self, index: &str) -> Result<AdminIndexCommitResponse> {
        let url = self
            .base_url
            .join(&format!("_admin/index/{}/commit", index))?;
        let resp = self.http.post(url).send().await?;
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!(
                "Admin index commit failed: {} - {}{}",
                status,
                text,
                self.refusal_hint(status)
            );
        }
        resp.json()
            .await
            .context("Failed to parse index commit response")
    }

    pub async fn admin_index_evict_writer(
        &self,
        index: &str,
    ) -> Result<AdminIndexEvictWriterResponse> {
        let url = self
            .base_url
            // Hyphen, not underscore: the route is `/_admin/index/{index}/evict-writer`.
            .join(&format!("_admin/index/{}/evict-writer", index))?;
        let resp = self.http.post(url).send().await?;
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!(
                "Admin index evict-writer failed: {} - {}{}",
                status,
                text,
                self.refusal_hint(status)
            );
        }
        resp.json()
            .await
            .context("Failed to parse index evict-writer response")
    }

    pub async fn admin_worker_stats(&self) -> Result<AdminWorkersResponse> {
        let url = self.base_url.join("_admin/workers")?;
        let resp = self.http.get(url).send().await?;
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!(
                "Admin workers stats failed: {} - {}{}",
                status,
                text,
                self.refusal_hint(status)
            );
        }
        resp.json()
            .await
            .context("Failed to parse workers stats response")
    }
}

/// Everything but `status` is absent for an unauthenticated caller against a node with
/// `[security]` enabled — the health endpoint stays public so a load balancer can probe it,
/// but node identity and cluster shape need a key. Hence every field but `status` is
/// optional: an anonymous 200 must parse, not blow up on a missing `node_id`.
#[derive(Debug, Serialize, Deserialize)]
pub struct HealthResponse {
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_shards: Option<usize>,
    pub cluster_name: Option<String>,
    pub cluster_enabled: Option<bool>,
    pub total_nodes: Option<usize>,
    pub connected_nodes: Option<usize>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ListIndexesResponse {
    pub indexes: Vec<IndexInfo>,
    pub total_indexes: usize,
    pub total_shards: usize,
    pub node_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct IndexInfo {
    pub name: String,
    /// What the operator wrote down about the dataset, if anything.
    #[serde(default)]
    pub description: Option<String>,
    pub document_count: u64,
    /// Sizes arrive in bytes and are rendered in megabytes here.
    ///
    /// They used to arrive already rounded to whole megabytes, which the cluster listing then
    /// summed across nodes — losing up to a megabyte per node before anyone saw the number.
    #[serde(default)]
    pub index_size_bytes: Option<u64>,
    #[serde(default)]
    pub memory_bytes: Option<u64>,
    #[serde(default)]
    pub data_size_bytes: Option<u64>,
    #[serde(default)]
    pub total_size_bytes: Option<u64>,
    pub shard_count: usize,
    #[serde(default)]
    pub warm_shards: Option<usize>,
    #[serde(default)]
    pub field_count: Option<usize>,
    /// Every field, described. The listing used to carry names alone, which is why showing an
    /// index cost a second request per index just to learn the types.
    #[serde(default)]
    pub fields: Vec<JsonValue>,
}

impl IndexInfo {
    /// Whole megabytes, rounded once, at the point of display.
    pub fn megabytes(bytes: Option<u64>) -> Option<u64> {
        bytes.map(|value| value / (1024 * 1024))
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct IndexConfigResponse {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub field_count: usize,
    #[serde(default)]
    pub fields: Vec<JsonValue>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AdminMemoryResponse {
    pub process: ProcessMemoryStats,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub process_after_purge: Option<ProcessMemoryStats>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub jemalloc: Option<JemallocStats>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub purge_result: Option<i32>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProcessMemoryStats {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vm_size_kb: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vm_rss_kb: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rss_anon_kb: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rss_file_kb: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rss_shmem_kb: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vm_data_kb: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vm_swap_kb: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub threads: Option<u64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct JemallocStats {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allocated: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resident: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retained: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardError {
    pub shard_id: String,
    pub error: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminIndexCommitResponse {
    pub index: String,
    pub shards_total: usize,
    pub shards_committed: usize,
    pub errors: Vec<ShardError>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminIndexEvictWriterResponse {
    pub index: String,
    pub shards_total: usize,
    pub writers_evicted: usize,
    pub writers_missing: usize,
    pub errors: Vec<ShardError>,
}

/// Every field is `#[serde(default)]` on purpose. This type is deserialized from a *node*,
/// which may be a different version than the client — a released client has to keep working
/// against a node that has added or renamed a field, and a missing counter is better
/// reported as zero than as a parse failure that hides the whole report.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct AdminWorkersResponse {
    /// Pinned worker threads were requested and the platform could enumerate cores.
    pub pinning_requested: bool,
    /// Workers whose pin actually took. Zero alongside `pinning_requested` means every
    /// request was refused — macOS, or a cpuset excluding the target cores.
    pub pinned_workers: usize,
    /// `worker_count` was aligned to the core budget so a worker and the writer for the
    /// shard with the matching ordinal share a core.
    pub core_aligned: bool,
    pub worker_count: usize,
    pub workers: Vec<WorkerStatsResponse>,
    pub shards: Vec<ShardPlacementResponse>,
    pub dispatch: DispatchStatsResponse,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct WorkerStatsResponse {
    pub id: usize,
    /// Core this worker was asked to pin to.
    pub target_core_id: Option<usize>,
    /// Core it is actually pinned to; absent when the pin was refused or never requested.
    pub core_id: Option<usize>,
    /// Jobs queued for this worker but not started.
    pub queue_depth: usize,
    pub queue_capacity: usize,
    /// Operations started and not yet answered. A deep `queue_depth` beside a low
    /// `in_flight` means the worker's concurrency limit is the constraint; the reverse means
    /// the shards are.
    pub in_flight: usize,
    pub in_flight_capacity: usize,
    pub jobs_completed: u64,
}

/// Where one shard sits in the worker pool.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ShardPlacementResponse {
    pub shard_id: String,
    /// `ordinal % worker_count` is the worker handling this shard's writes.
    pub ordinal: usize,
    /// False means the shard holds an ordinal but never started.
    pub serving: bool,
    pub target_core_id: Option<usize>,
    pub core_id: Option<usize>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct DispatchStatsResponse {
    pub affine_sends: u64,
    pub affine_full_fallbacks: u64,
    pub round_robin_sends: u64,
    pub actor_mailbox_fallbacks: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The node's `/_admin/workers` payload, as the server serializes it today. Pinned here
    /// because the client and the node version independently: renaming a field on the server
    /// silently broke `cameodb admin workers` once already, and nothing noticed because the
    /// validation suite reads that endpoint with `curl`, not with this type.
    #[test]
    fn the_worker_report_parses_as_the_node_sends_it() {
        let body = serde_json::json!({
            "pinning_requested": true,
            "pinned_workers": 8,
            "core_aligned": true,
            "worker_count": 8,
            "workers": [
                {"id": 0, "target_core_id": 0, "core_id": 0, "queue_depth": 1,
                 "queue_capacity": 512, "jobs_completed": 42}
            ],
            "shards": [
                {"shard_id": "0d43c3c7-ef3f-4334-aef6-88a98375483a", "ordinal": 0,
                 "serving": true, "target_core_id": 0, "core_id": 0}
            ],
            "dispatch": {"affine_sends": 40, "affine_full_fallbacks": 0,
                         "round_robin_sends": 24, "actor_mailbox_fallbacks": 0}
        });

        let report: AdminWorkersResponse = serde_json::from_value(body).unwrap();
        assert!(report.pinning_requested);
        assert_eq!(report.pinned_workers, 8);
        assert_eq!(report.workers[0].core_id, Some(0));
        assert_eq!(report.shards[0].ordinal, 0);
        assert!(report.shards[0].serving);
        assert_eq!(report.dispatch.affine_sends, 40);
    }

    /// A node that has not pinned anything omits `core_id` entirely, and an older or newer
    /// node may omit fields this client knows about. Neither is a parse error.
    #[test]
    fn a_worker_report_from_a_different_node_version_still_parses() {
        let body = serde_json::json!({
            "worker_count": 2,
            "workers": [{"id": 0, "queue_depth": 0, "queue_capacity": 512,
                         "jobs_completed": 7}],
            "dispatch": {"round_robin_sends": 7}
        });

        let report: AdminWorkersResponse = serde_json::from_value(body).unwrap();
        assert_eq!(report.worker_count, 2);
        assert!(!report.pinning_requested, "absent means not requested");
        assert_eq!(report.workers[0].core_id, None, "absent means not pinned");
        assert!(report.shards.is_empty());
        assert_eq!(report.dispatch.round_robin_sends, 7);
    }

    /// 43 base64url characters, the shape `cameodb keygen` produces.
    const GOOD: &str = "cameo_v1_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

    /// `main.rs` does this once at startup; reqwest panics on `Client::build` without it,
    /// so any test that constructs a client has to do the same.
    fn with_tls_provider() {
        static ONCE: std::sync::Once = std::sync::Once::new();
        ONCE.call_once(|| {
            let _ = rustls::crypto::ring::default_provider().install_default();
        });
    }

    fn keyed(url: &str) -> Result<CameoClient> {
        with_tls_provider();
        CameoClient::new_with_options(
            url,
            TlsTrust::default(),
            ClientAuth {
                credential: Some(Credential::parse(GOOD).unwrap()),
                allow_plaintext: false,
            },
        )
    }

    #[test]
    fn the_key_body_is_exactly_a_base64url_encoded_256_bit_value() {
        // If the server ever changes its key length, this is the assertion that catches
        // the client having been left behind.
        assert_eq!(KEY_BODY_LEN, 32usize.div_ceil(3) * 4 - 1);
        assert_eq!(GOOD.len(), KEY_PREFIX.len() + KEY_BODY_LEN);
    }

    #[test]
    fn a_well_formed_key_parses() {
        let credential = Credential::parse(GOOD).unwrap();
        assert_eq!(credential.key_id().len(), 8);
    }

    #[test]
    fn surrounding_whitespace_is_not_an_error() {
        // A key file written with a trailing newline, or pasted with one, is still a key.
        let from_file = Credential::parse(&format!("  {GOOD}\n")).unwrap();
        assert_eq!(
            from_file.key_id(),
            Credential::parse(GOOD).unwrap().key_id()
        );
    }

    #[test]
    fn anything_that_is_not_a_key_is_refused_before_it_is_sent() {
        for bad in [
            "",
            "   ",
            "hunter2",
            "cameo_v1_",
            "cameo_v1_tooshort",
            "cameo_v2_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
            // 43 characters, but '+' and '/' are base64, not base64url.
            "cameo_v1_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA+/",
            "3f8a1c2b-0000-4000-8000-000000000000",
        ] {
            assert!(
                Credential::parse(bad).is_err(),
                "should have been refused: {bad:?}"
            );
        }
    }

    #[test]
    fn the_message_for_a_malformed_key_says_what_a_key_looks_like() {
        let err = format!("{:#}", Credential::parse("hunter2").unwrap_err());
        assert!(err.contains("cameo_v1_"), "{err}");
        assert!(err.contains("keygen"), "{err}");
    }

    #[test]
    fn a_credential_never_prints_itself() {
        let credential = Credential::parse(GOOD).unwrap();
        let rendered = format!("{credential:?}");
        assert!(!rendered.contains(GOOD));
        assert!(!rendered.contains(&GOOD[KEY_PREFIX.len()..KEY_PREFIX.len() + 8]));
        assert!(rendered.contains(&credential.key_id()));
    }

    #[test]
    fn key_id_matches_the_servers_fingerprint_of_the_same_key() {
        // sha256("cameo_v1_AAA…A"), first four bytes — the same bytes the node logs as
        // key_id when it accepts this key. Fixed here so the two can never drift apart
        // silently.
        assert_eq!(Credential::parse(GOOD).unwrap().key_id().len(), 8);
        assert_eq!(
            Credential::parse(GOOD).unwrap().key_id(),
            Credential::parse(GOOD).unwrap().key_id()
        );
        assert_ne!(
            Credential::parse(GOOD).unwrap().key_id(),
            Credential::parse(&format!("{}B", &GOOD[..GOOD.len() - 1]))
                .unwrap()
                .key_id()
        );
    }

    #[test]
    fn loopback_is_recognised_in_every_form_it_is_written() {
        for url in [
            "http://localhost:9480",
            "http://LOCALHOST:9480",
            "http://127.0.0.1:9480",
            "http://127.9.9.9:9480",
            "http://[::1]:9480",
        ] {
            assert!(is_loopback(&Url::parse(url).unwrap()), "{url}");
        }
        for url in [
            "http://10.0.0.4:9480",
            "http://example.com:9480",
            "http://localhost.evil.example:9480",
        ] {
            assert!(!is_loopback(&Url::parse(url).unwrap()), "{url}");
        }
    }

    #[test]
    fn a_key_is_refused_over_plaintext_to_another_machine() {
        let err = CameoClient::new_with_options(
            "http://db.internal:9480",
            TlsTrust::default(),
            ClientAuth {
                credential: Some(Credential::parse(GOOD).unwrap()),
                allow_plaintext: false,
            },
        )
        .unwrap_err();
        let rendered = format!("{err:#}");
        assert!(rendered.contains("plaintext"), "{rendered}");
        assert!(rendered.contains("--allow-plaintext-key"), "{rendered}");
    }

    #[test]
    fn the_plaintext_refusal_lifts_for_loopback_https_and_explicit_consent() {
        assert!(keyed("http://localhost:9480").is_ok());
        assert!(keyed("https://db.internal:9480").is_ok());
        with_tls_provider();
        assert!(
            CameoClient::new_with_options(
                "http://db.internal:9480",
                TlsTrust::default(),
                ClientAuth {
                    credential: Some(Credential::parse(GOOD).unwrap()),
                    allow_plaintext: true,
                }
            )
            .is_ok()
        );
    }

    #[test]
    fn plaintext_without_a_key_is_not_this_guards_business() {
        // The refusal is about disclosing a credential, not about plaintext as such.
        with_tls_provider();
        assert!(
            CameoClient::new_with_options(
                "http://db.internal:9480",
                TlsTrust::default(),
                ClientAuth::default()
            )
            .is_ok()
        );
    }

    #[test]
    fn a_client_reports_its_key_by_fingerprint_only() {
        let client = keyed("http://localhost:9480").unwrap();
        assert_eq!(
            client.key_id(),
            Some(Credential::parse(GOOD).unwrap().key_id())
        );
        assert!(!format!("{client:?}").contains(GOOD));
    }

    #[test]
    fn the_hint_tells_each_side_which_end_to_fix() {
        with_tls_provider();
        let anonymous = CameoClient::new("http://localhost:9480").unwrap();
        let hint = anonymous.refusal_hint(reqwest::StatusCode::UNAUTHORIZED);
        assert!(hint.contains("--api-key-file"), "{hint}");

        let keyed = keyed("http://localhost:9480").unwrap();
        assert!(
            keyed
                .refusal_hint(reqwest::StatusCode::UNAUTHORIZED)
                .contains("keyring")
        );
        assert!(
            keyed
                .refusal_hint(reqwest::StatusCode::FORBIDDEN)
                .contains("allowed_indexes")
        );
        // A 500 is not an authentication problem and must not be dressed up as one.
        assert!(
            keyed
                .refusal_hint(reqwest::StatusCode::INTERNAL_SERVER_ERROR)
                .is_empty()
        );
    }

    #[test]
    fn an_origin_is_scheme_host_and_port() {
        assert_eq!(
            origin_of("http://localhost:9480/x"),
            "http://localhost:9480"
        );
        assert_ne!(
            origin_of("http://localhost:9480"),
            origin_of("http://localhost:9481")
        );
        assert_ne!(
            origin_of("http://localhost:9480"),
            origin_of("https://localhost:9480")
        );
    }

    #[test]
    fn an_anonymous_health_response_still_parses() {
        // What a node with [security] enabled returns to a caller with no key: the status
        // and nothing else. The client must render that, not fail on the missing fields.
        let bare: HealthResponse = serde_json::from_str(r#"{"status":"green"}"#).unwrap();
        assert_eq!(bare.status, "green");
        assert!(bare.node_id.is_none());
        assert!(bare.active_shards.is_none());
        let round_tripped = serde_json::to_string(&bare).unwrap();
        assert!(!round_tripped.contains("node_id"), "{round_tripped}");
        assert!(!round_tripped.contains("active_shards"), "{round_tripped}");

        let full: HealthResponse = serde_json::from_str(
            r#"{"status":"green","node_id":"n1","active_shards":4,"cluster_enabled":false}"#,
        )
        .unwrap();
        assert_eq!(full.node_id.as_deref(), Some("n1"));
        assert_eq!(full.active_shards, Some(4));
    }

    #[test]
    fn a_key_file_is_read_and_a_multi_line_one_is_refused() {
        let dir = std::env::temp_dir().join(format!("cameodb-key-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();

        let good = dir.join("good.key");
        std::fs::write(&good, format!("{GOOD}\n")).unwrap();
        assert_eq!(
            Credential::from_file(&good).unwrap().key_id(),
            Credential::parse(GOOD).unwrap().key_id()
        );

        let two = dir.join("two.key");
        std::fs::write(&two, format!("{GOOD}\n{GOOD}\n")).unwrap();
        let err = format!("{:#}", Credential::from_file(&two).unwrap_err());
        assert!(err.contains("one key and nothing else"), "{err}");

        let missing = dir.join("absent.key");
        assert!(Credential::from_file(&missing).is_err());

        std::fs::remove_dir_all(&dir).ok();
    }
}

#[cfg(test)]
mod key_id_digest_tests {
    use super::Credential;

    /// A `key_id` is the first four bytes of SHA-256 over the key, in hex, and a server stores
    /// the whole digest in its configuration as `sha256:<hex>`. Both are on-disk contracts: if
    /// what the hash produces ever changes, every deployed key stops matching its stored digest,
    /// and no round-trip test can see it because both sides move together. Pinned against a
    /// value computed outside this codebase:
    ///
    /// ```text
    /// printf 'cameo_v1_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA' | shasum -a 256
    /// 373ce68d26ee121548841ac21adf2706dbe4cb3e28e65b1574b4653a1ac9982a
    /// ```
    #[test]
    fn a_key_id_is_the_sha256_prefix_and_does_not_move_with_the_hash_crate() {
        let credential = Credential::parse(&format!("cameo_v1_{}", "A".repeat(43)))
            .expect("a well-formed key parses");
        assert_eq!(credential.key_id(), "373ce68d");
    }
}
