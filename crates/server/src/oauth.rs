//! OAuth 2.1 for MCP clients: the node as its own authorization server (ROADMAP T1).
//!
//! A web or desktop MCP client cannot be handed a header; it follows the MCP authorization spec
//! instead — discovery, dynamic client registration, an authorization code with PKCE, a bearer
//! token. This module is the model of that flow and [`crate::http_server::oauth`] its endpoints.
//!
//! Two decisions shape it.
//!
//! **A token is a key, narrowed.** The user signs in with the `cameo_v1_…` key they already hold,
//! and what they get back carries that key's full digest — never the 32-bit `key_id`, which is a
//! label, not an identity. Every request looks the key up in the live ring, so revoking the key
//! revokes every token minted from it and a changed scope applies to the next call. The token
//! grants `read` whatever the key's role: MCP is the only surface that accepts one, and its
//! tools read.
//!
//! **Nothing is stored.** The client id, the authorization request, the code and both tokens are
//! signed payloads under one secret every node holds, so a code issued by one node is redeemed on
//! another with nothing replicated, and a restart forgets nothing that matters. The prefix of each
//! artifact is part of what is signed: a code cannot be presented as a token, nor a client id as
//! a code. The one piece of state is the set of codes already redeemed, kept per node for a
//! code's sixty seconds — a code replayed on another node inside that window still needs the
//! PKCE verifier only the client holds.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use base64::Engine;
use hmac::{Hmac, KeyInit, Mac};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use zeroize::Zeroize;

use crate::auth::{Capability, KeyDigest, KeyEntry, KeyRing};

const B64: base64::engine::general_purpose::GeneralPurpose =
    base64::engine::general_purpose::URL_SAFE_NO_PAD;

/// The prefix of an access token. Public because the gate dispatches on it: a `cameo_v1_`
/// bearer is a key, a `cameo_at1_` bearer is one of these.
pub const ACCESS_PREFIX: &str = "cameo_at1_";
const REFRESH_PREFIX: &str = "cameo_rt1_";
const CODE_PREFIX: &str = "cameo_code1_";
const CLIENT_PREFIX: &str = "cameo_client1_";
const REQUEST_PREFIX: &str = "cameo_req1_";
/// Not an artifact: the domain a client secret is derived under.
const CLIENT_SECRET_DOMAIN: &str = "cameo_secret1_";
/// The signing secret's own format, as `keygen --oauth-secret-out` writes it.
const SECRET_PREFIX: &str = "cameo_oauth_v1_";
const SECRET_BYTES: usize = 32;

/// The one scope a token holds.
pub const SCOPE: &str = "read";

/// How long a code may wait to be redeemed. Long enough for a client's callback and its
/// back-channel request; short enough that a leaked one is useless.
const CODE_TTL_SECS: u64 = 60;
/// How long the approval page may sit open before its form goes stale.
const REQUEST_TTL_SECS: u64 = 600;

/// The longest artifact [`Signer::open`] will decode. A client id carries its redirect URIs, so
/// it is the long one; this bounds what an unauthenticated caller can make the node parse.
const MAX_ARTIFACT_LEN: usize = 8 * 1024;
/// At most this many redirect URIs per client, each at most this long.
const MAX_REDIRECT_URIS: usize = 8;
const MAX_REDIRECT_URI_LEN: usize = 512;
const MAX_CLIENT_NAME_LEN: usize = 100;
/// A PKCE verifier's length bounds (RFC 7636 §4.1).
const VERIFIER_LEN: std::ops::RangeInclusive<usize> = 43..=128;

/// `[security.oauth]` — OAuth for MCP clients.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct OAuthConfig {
    /// Serve the OAuth endpoints and accept their tokens on `/mcp` (default: false).
    pub enabled: bool,

    /// The node's external URL, as a client reaches it: `https://db.example.com`. Every URL in
    /// the metadata, and the audience of every token, is built from it. A node behind a proxy
    /// cannot learn it from a request, which is why it is written down.
    pub issuer: Option<String>,

    /// The file holding the signing secret, as `cameodb keygen --oauth-secret-out` writes it.
    /// The same file on every node of a cluster; replacing it signs every client out.
    pub signing_key_file: Option<PathBuf>,

    /// How long an access token lives (default: 3600).
    pub access_token_ttl_secs: u64,

    /// How long a refresh token lives (default: 30 days). The user signs in again with their key
    /// only after this much time without the client using it.
    pub refresh_token_ttl_secs: u64,
}

impl Default for OAuthConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            issuer: None,
            signing_key_file: None,
            access_token_ttl_secs: 3600,
            refresh_token_ttl_secs: 30 * 24 * 3600,
        }
    }
}

impl OAuthConfig {
    /// The checks that need no file: everything [`OAuth::load`] would refuse that a config
    /// validation can name.
    pub fn validate(&self, security_enabled: bool, mcp_enabled: bool) -> Result<(), String> {
        if !self.enabled {
            return Ok(());
        }
        if !security_enabled {
            return Err(
                "security.oauth.enabled needs [security] enabled = true: a token is \
                        issued against a key, and with authentication off there are none"
                    .to_string(),
            );
        }
        if !mcp_enabled {
            return Err(
                "security.oauth.enabled needs [mcp] enabled = true: tokens are for /mcp alone"
                    .to_string(),
            );
        }
        let issuer = self.issuer.as_deref().ok_or(
            "security.oauth.issuer is required: the node's external URL, such as \
             \"https://db.example.com\"",
        )?;
        normalize_issuer(issuer)?;
        if self.signing_key_file.is_none() {
            return Err(
                "security.oauth.signing_key_file is required: make one with \
                        `cameodb keygen --oauth-secret-out <PATH>`"
                    .to_string(),
            );
        }
        if self.access_token_ttl_secs == 0 || self.refresh_token_ttl_secs == 0 {
            return Err("security.oauth token lifetimes must be above 0".to_string());
        }
        if self.refresh_token_ttl_secs < self.access_token_ttl_secs {
            return Err("security.oauth.refresh_token_ttl_secs is shorter than \
                        access_token_ttl_secs, so a token could never be refreshed"
                .to_string());
        }
        Ok(())
    }
}

/// The issuer with no trailing slash, or why it cannot be one.
///
/// An origin and nothing else: a path would move every well-known URL (RFC 8414 §3), and a
/// query or fragment is never part of an issuer. `https` unless the host is loopback, where a
/// local test has no certificate and nothing off the machine can reach it.
fn normalize_issuer(issuer: &str) -> Result<String, String> {
    let trimmed = issuer.trim().trim_end_matches('/');
    let (scheme, rest) = trimmed
        .split_once("://")
        .ok_or_else(|| format!("security.oauth.issuer {issuer:?} is not a URL"))?;
    if rest.is_empty() || rest.contains(['/', '?', '#', '@']) {
        return Err(format!(
            "security.oauth.issuer {issuer:?} must be scheme and host only, such as \
             \"https://db.example.com\""
        ));
    }
    match scheme {
        "https" => {}
        "http" if is_loopback_host(rest) => {}
        _ => {
            return Err(format!(
                "security.oauth.issuer {issuer:?} must be https; plain http is accepted only \
                 for a loopback host"
            ));
        }
    }
    Ok(trimmed.to_string())
}

/// Whether `authority` (`host[:port]`) names this machine.
fn is_loopback_host(authority: &str) -> bool {
    let host = match authority.strip_prefix('[') {
        Some(v6) => v6.split(']').next().unwrap_or_default(),
        None => authority.split(':').next().unwrap_or_default(),
    };
    matches!(host, "localhost" | "127.0.0.1" | "::1")
}

/// Seconds since the epoch. A clock before 1970 is a broken host, read as 0 so every
/// artifact looks expired rather than eternal.
fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// A fresh random value for an artifact that must not repeat.
fn nonce() -> String {
    let mut bytes = [0u8; 12];
    // An OS without entropy cannot have minted the signing secret either; a fixed nonce is
    // then the lesser problem, and every other check still holds.
    let _ = getrandom::fill(&mut bytes);
    B64.encode(bytes)
}

/// The signing secret. `Debug` prints nothing of it.
struct SigningKey([u8; SECRET_BYTES]);

impl Drop for SigningKey {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

impl SigningKey {
    fn generate() -> Result<Self> {
        let mut bytes = [0u8; SECRET_BYTES];
        getrandom::fill(&mut bytes)
            .context("failed to read entropy from the operating system for the OAuth secret")?;
        Ok(Self(bytes))
    }

    fn parse(text: &str) -> Result<Self> {
        let body = text.trim().strip_prefix(SECRET_PREFIX).context(
            "not an OAuth signing secret: make one with `cameodb keygen --oauth-secret-out`",
        )?;
        let mut decoded = B64
            .decode(body)
            .context("the OAuth signing secret is not valid base64url")?;
        if decoded.len() != SECRET_BYTES {
            decoded.zeroize();
            bail!("the OAuth signing secret is not {SECRET_BYTES} bytes");
        }
        let mut bytes = [0u8; SECRET_BYTES];
        bytes.copy_from_slice(&decoded);
        decoded.zeroize();
        Ok(Self(bytes))
    }

    fn to_file_value(&self) -> String {
        format!("{SECRET_PREFIX}{}", B64.encode(self.0))
    }

    fn mac(&self) -> Hmac<Sha256> {
        <Hmac<Sha256> as KeyInit>::new_from_slice(&self.0).expect("HMAC takes a key of any length")
    }
}

/// A new signing secret, in the form the config file reads. For `keygen`.
pub fn generate_secret() -> Result<String> {
    Ok(SigningKey::generate()?.to_file_value())
}

/// Signs and opens artifacts: `<prefix><base64url(json)>.<base64url(hmac)>`.
struct Signer(SigningKey);

impl Signer {
    fn tag(&self, prefix: &str, body: &str) -> Hmac<Sha256> {
        let mut mac = self.0.mac();
        mac.update(prefix.as_bytes());
        mac.update(b".");
        mac.update(body.as_bytes());
        mac
    }

    fn seal<T: Serialize>(&self, prefix: &str, payload: &T) -> String {
        let json = serde_json::to_vec(payload).expect("an artifact payload always serializes");
        let body = B64.encode(json);
        let tag = self.tag(prefix, &body).finalize().into_bytes();
        format!("{prefix}{body}.{}", B64.encode(tag))
    }

    /// The payload, if `artifact` carries `prefix` and this secret signed it. Expiry is the
    /// caller's to check: it is a field of the payload, not of the envelope.
    fn open<T: DeserializeOwned>(&self, prefix: &str, artifact: &str) -> Option<T> {
        if artifact.len() > MAX_ARTIFACT_LEN {
            return None;
        }
        let (body, tag) = artifact.strip_prefix(prefix)?.split_once('.')?;
        let tag = B64.decode(tag).ok()?;
        // Constant time: how long a refusal takes says nothing about how much of a forged tag
        // was right.
        self.tag(prefix, body).verify_slice(&tag).ok()?;
        serde_json::from_slice(&B64.decode(body).ok()?).ok()
    }

    /// A value derived from `input` under `domain`, for what must be reproducible but not
    /// guessable: a client's secret.
    fn derive(&self, domain: &str, input: &str) -> String {
        B64.encode(self.tag(domain, input).finalize().into_bytes())
    }
}

/// How a client authenticates at the token endpoint (RFC 7591 §2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClientAuth {
    /// A public client: PKCE is the proof.
    None,
    ClientSecretPost,
    ClientSecretBasic,
}

impl ClientAuth {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "none" => Some(Self::None),
            "client_secret_post" => Some(Self::ClientSecretPost),
            "client_secret_basic" => Some(Self::ClientSecretBasic),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::ClientSecretPost => "client_secret_post",
            Self::ClientSecretBasic => "client_secret_basic",
        }
    }
}

/// A registered client, as its id carries it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Client {
    #[serde(rename = "r")]
    pub redirect_uris: Vec<String>,
    /// What the client calls itself. Unverified: anyone can register any name, which is why the
    /// approval page shows where the browser will be sent as well.
    #[serde(rename = "n", default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(rename = "a")]
    pub auth: ClientAuth,
    #[serde(rename = "t")]
    pub issued_at: u64,
}

/// What a client id stands for, opened.
pub struct RegisteredClient {
    pub id: String,
    pub client: Client,
}

impl RegisteredClient {
    /// The redirect URI a request names, if this client registered it — or the one it
    /// registered, when it registered exactly one and the request names none (RFC 6749 §3.1.2.3).
    pub fn redirect_uri<'a>(&'a self, requested: Option<&'a str>) -> Option<&'a str> {
        match requested {
            Some(uri) => self
                .client
                .redirect_uris
                .iter()
                .any(|registered| redirect_matches(registered, uri))
                .then_some(uri),
            None if self.client.redirect_uris.len() == 1 => {
                Some(self.client.redirect_uris[0].as_str())
            }
            None => None,
        }
    }
}

/// Exact match, except that a loopback redirect may name any port (RFC 8252 §7.3): a native
/// client picks a free port each time it listens.
fn redirect_matches(registered: &str, requested: &str) -> bool {
    if registered == requested {
        return true;
    }
    match (loopback_parts(registered), loopback_parts(requested)) {
        (Some((host_a, path_a)), Some((host_b, path_b))) => host_a == host_b && path_a == path_b,
        _ => false,
    }
}

/// `(host, path)` of an `http` loopback URI, without its port.
fn loopback_parts(uri: &str) -> Option<(&str, &str)> {
    let rest = uri.strip_prefix("http://")?;
    let (authority, path) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, ""),
    };
    if !is_loopback_host(authority) {
        return None;
    }
    let host = if authority.starts_with('[') {
        authority.split(']').next()?
    } else {
        authority.split(':').next()?
    };
    Some((host, path))
}

/// Why a redirect URI cannot be registered, if it cannot.
///
/// `https`; `http` to loopback; or a private-use scheme, which RFC 8252 §7.1 asks to be a
/// reversed domain name — the dot is what keeps out `javascript:` and `data:`. No fragment, as
/// RFC 6749 §3.1.2 requires.
fn check_redirect_uri(uri: &str) -> Result<(), String> {
    if uri.len() > MAX_REDIRECT_URI_LEN {
        return Err(format!(
            "a redirect URI is longer than {MAX_REDIRECT_URI_LEN} characters"
        ));
    }
    if uri.contains('#') || uri.chars().any(|c| c.is_whitespace() || c.is_control()) {
        return Err(format!(
            "{uri:?} is not a redirect URI: no fragment and no whitespace"
        ));
    }
    let Some((scheme, rest)) = uri.split_once(':') else {
        return Err(format!("{uri:?} is not a URI"));
    };
    let authority = rest
        .strip_prefix("//")
        .map(|r| r.split(['/', '?']).next().unwrap_or_default());
    let ok = match scheme {
        "https" => authority.is_some_and(|a| !a.is_empty() && !a.contains('@')),
        "http" => authority.is_some_and(is_loopback_host),
        other => {
            other.contains('.')
                && other
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '+' | '-'))
        }
    };
    if ok {
        Ok(())
    } else {
        Err(format!(
            "{uri:?} is not a redirect URI this node accepts: https, http to localhost, or a \
             private-use scheme such as com.example.app:/callback"
        ))
    }
}

/// An authorization request, carried signed in the approval form between the page and its
/// submission, so the node keeps no session for it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthorizeRequest {
    #[serde(rename = "c")]
    pub client_id: String,
    #[serde(rename = "u")]
    pub redirect_uri: String,
    #[serde(rename = "p")]
    pub code_challenge: String,
    #[serde(rename = "s", default, skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,
    exp: u64,
}

#[derive(Serialize, Deserialize)]
struct CodeGrant {
    /// The client, as a hash of its id: the id itself can run to a kilobyte.
    #[serde(rename = "c")]
    client: String,
    #[serde(rename = "u")]
    redirect_uri: String,
    #[serde(rename = "p")]
    code_challenge: String,
    /// The key's digest, `sha256:<hex>`.
    #[serde(rename = "k")]
    key: String,
    exp: u64,
    #[serde(rename = "j")]
    nonce: String,
}

#[derive(Serialize, Deserialize)]
struct TokenClaims {
    #[serde(rename = "k")]
    key: String,
    #[serde(rename = "c")]
    client: String,
    aud: String,
    exp: u64,
    #[serde(rename = "j")]
    nonce: String,
}

/// A refusal, in the vocabulary of RFC 6749 §5.2 and §4.1.2.1.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OAuthError {
    pub error: &'static str,
    pub description: String,
}

impl OAuthError {
    fn new(error: &'static str, description: impl Into<String>) -> Self {
        Self {
            error,
            description: description.into(),
        }
    }

    pub fn invalid_request(description: impl Into<String>) -> Self {
        Self::new("invalid_request", description)
    }

    fn invalid_grant(description: impl Into<String>) -> Self {
        Self::new("invalid_grant", description)
    }

    pub fn invalid_client(description: impl Into<String>) -> Self {
        Self::new("invalid_client", description)
    }
}

/// What a client sent to identify itself at the token endpoint.
pub struct ClientCredentials<'a> {
    pub client_id: Option<&'a str>,
    pub client_secret: Option<&'a str>,
    /// Whether they came in an `Authorization: Basic` header rather than the form.
    pub basic: bool,
}

/// An access and a refresh token, as the token endpoint returns them.
pub struct TokenPair {
    pub access_token: String,
    pub refresh_token: String,
    pub expires_in: u64,
}

/// What a valid access token resolves to.
pub struct Grant {
    pub entry: Arc<KeyEntry>,
    /// The hash the token carries for its client — for the audit line, not a secret.
    pub client: String,
}

/// Why an access token was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenRefusal {
    /// Not signed by this node's secret, altered, or not an access token.
    Invalid,
    Expired,
    /// Issued for another resource.
    WrongAudience,
    /// The key it was minted from is gone.
    KeyRevoked,
}

impl TokenRefusal {
    pub fn message(self) -> &'static str {
        match self {
            Self::Invalid => "the OAuth token presented is not valid",
            Self::Expired => "the OAuth token has expired",
            Self::WrongAudience => "the OAuth token was not issued for this resource",
            Self::KeyRevoked => "the key this OAuth token was issued for has been revoked",
        }
    }
}

/// The node's authorization server: its URLs, its secret, and the codes it has seen redeemed.
pub struct OAuth {
    issuer: String,
    resource: String,
    signer: Signer,
    access_ttl: u64,
    refresh_ttl: u64,
    /// Redeemed codes and when each expires; the per-node half of single use.
    redeemed: Mutex<HashMap<String, u64>>,
}

impl std::fmt::Debug for OAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OAuth")
            .field("issuer", &self.issuer)
            .finish_non_exhaustive()
    }
}

impl OAuth {
    /// The server `[security.oauth]` describes, or `None` when it is off. Reads the secret.
    pub fn load(config: &OAuthConfig) -> Result<Option<Arc<Self>>> {
        if !config.enabled {
            return Ok(None);
        }
        let issuer = normalize_issuer(config.issuer.as_deref().unwrap_or_default())
            .map_err(|e| anyhow::anyhow!(e))?;
        let path = config
            .signing_key_file
            .as_deref()
            .context("security.oauth.signing_key_file is required")?;
        let key = read_secret(path)?;
        Ok(Some(Arc::new(Self::with_key(
            issuer,
            key,
            config.access_token_ttl_secs,
            config.refresh_token_ttl_secs,
        ))))
    }

    fn with_key(issuer: String, key: SigningKey, access_ttl: u64, refresh_ttl: u64) -> Self {
        Self {
            resource: format!("{issuer}/mcp"),
            issuer,
            signer: Signer(key),
            access_ttl,
            refresh_ttl,
            redeemed: Mutex::new(HashMap::new()),
        }
    }

    pub fn issuer(&self) -> &str {
        &self.issuer
    }

    /// The protected resource: this node's `/mcp`, and the audience of every token.
    pub fn resource(&self) -> &str {
        &self.resource
    }

    /// Where the `401` from `/mcp` points a client (RFC 9728 §5.1).
    pub fn resource_metadata_url(&self) -> String {
        format!("{}/.well-known/oauth-protected-resource/mcp", self.issuer)
    }

    /// Whether a `resource` parameter (RFC 8707) names this node's `/mcp`. Absent is accepted:
    /// the token's audience is this resource either way.
    pub fn accepts_resource(&self, resource: Option<&str>) -> bool {
        match resource {
            None => true,
            Some(r) => {
                let r = r.trim_end_matches('/');
                r == self.resource || r == self.issuer
            }
        }
    }

    // --- Registration -------------------------------------------------------------------------

    /// Register a client: its id, and its secret when it asked for one.
    pub fn register(
        &self,
        redirect_uris: Vec<String>,
        name: Option<String>,
        auth: ClientAuth,
    ) -> Result<(String, Option<String>, Client), OAuthError> {
        if redirect_uris.is_empty() {
            return Err(OAuthError::new(
                "invalid_redirect_uri",
                "redirect_uris must name at least one URI",
            ));
        }
        if redirect_uris.len() > MAX_REDIRECT_URIS {
            return Err(OAuthError::new(
                "invalid_redirect_uri",
                format!("at most {MAX_REDIRECT_URIS} redirect URIs"),
            ));
        }
        for uri in &redirect_uris {
            check_redirect_uri(uri).map_err(|e| OAuthError::new("invalid_redirect_uri", e))?;
        }
        let name = name
            .map(|n| n.chars().filter(|c| !c.is_control()).collect::<String>())
            .map(|n| {
                n.trim()
                    .chars()
                    .take(MAX_CLIENT_NAME_LEN)
                    .collect::<String>()
            })
            .filter(|n| !n.is_empty());
        let client = Client {
            redirect_uris,
            name,
            auth,
            issued_at: now(),
        };
        let id = self.signer.seal(CLIENT_PREFIX, &client);
        let secret = (auth != ClientAuth::None).then(|| self.client_secret(&id));
        Ok((id, secret, client))
    }

    fn client_secret(&self, client_id: &str) -> String {
        self.signer.derive(CLIENT_SECRET_DOMAIN, client_id)
    }

    /// The client an id stands for, if this node issued it.
    pub fn client(&self, client_id: &str) -> Option<RegisteredClient> {
        let client = self.signer.open::<Client>(CLIENT_PREFIX, client_id)?;
        Some(RegisteredClient {
            id: client_id.to_string(),
            client,
        })
    }

    /// The client the token endpoint is talking to, authenticated as it registered.
    fn authenticate_client(
        &self,
        credentials: &ClientCredentials<'_>,
    ) -> Result<RegisteredClient, OAuthError> {
        let id = credentials
            .client_id
            .ok_or_else(|| OAuthError::invalid_client("client_id is required"))?;
        let registered = self
            .client(id)
            .ok_or_else(|| OAuthError::invalid_client("unknown client_id"))?;
        match registered.client.auth {
            ClientAuth::None => {}
            ClientAuth::ClientSecretPost | ClientAuth::ClientSecretBasic => {
                let presented = credentials
                    .client_secret
                    .ok_or_else(|| OAuthError::invalid_client("client_secret is required"))?;
                let expected = self.client_secret(id);
                let equal: bool =
                    subtle::ConstantTimeEq::ct_eq(presented.as_bytes(), expected.as_bytes()).into();
                if !equal {
                    return Err(OAuthError::invalid_client("client authentication failed"));
                }
                // Which of the two methods carried it is not checked: the secret is the proof,
                // and a client that registered one and sends the other has proved the same thing.
                let _ = credentials.basic;
            }
        }
        Ok(registered)
    }

    // --- Authorization ------------------------------------------------------------------------

    /// Seal what the approval page has to carry to its own submission.
    pub fn seal_request(
        &self,
        client_id: &str,
        redirect_uri: &str,
        code_challenge: &str,
        state: Option<&str>,
    ) -> String {
        self.signer.seal(
            REQUEST_PREFIX,
            &AuthorizeRequest {
                client_id: client_id.to_string(),
                redirect_uri: redirect_uri.to_string(),
                code_challenge: code_challenge.to_string(),
                state: state.map(str::to_string),
                exp: now() + REQUEST_TTL_SECS,
            },
        )
    }

    /// The request the form carried, if it is ours and not stale.
    pub fn open_request(&self, sealed: &str) -> Option<AuthorizeRequest> {
        self.signer
            .open::<AuthorizeRequest>(REQUEST_PREFIX, sealed)
            .filter(|r| r.exp >= now())
    }

    /// Issue the code for an approved request, bound to the key that approved it.
    pub fn issue_code(&self, request: &AuthorizeRequest, entry: &KeyEntry) -> String {
        self.signer.seal(
            CODE_PREFIX,
            &CodeGrant {
                client: client_hash(&request.client_id),
                redirect_uri: request.redirect_uri.clone(),
                code_challenge: request.code_challenge.clone(),
                key: entry.digest().to_config_value(),
                exp: now() + CODE_TTL_SECS,
                nonce: nonce(),
            },
        )
    }

    // --- Token --------------------------------------------------------------------------------

    /// `grant_type=authorization_code`.
    pub fn exchange_code(
        &self,
        ring: &KeyRing,
        credentials: &ClientCredentials<'_>,
        code: &str,
        redirect_uri: Option<&str>,
        code_verifier: Option<&str>,
        resource: Option<&str>,
    ) -> Result<TokenPair, OAuthError> {
        let client = self.authenticate_client(credentials)?;
        let grant = self
            .signer
            .open::<CodeGrant>(CODE_PREFIX, code)
            .ok_or_else(|| OAuthError::invalid_grant("the authorization code is not valid"))?;
        if grant.exp < now() {
            return Err(OAuthError::invalid_grant(
                "the authorization code has expired",
            ));
        }
        if grant.client != client_hash(&client.id) {
            return Err(OAuthError::invalid_grant(
                "the authorization code was issued to another client",
            ));
        }
        // Required whenever the request carried one, which ours always does (RFC 6749 §4.1.3).
        if redirect_uri != Some(grant.redirect_uri.as_str()) {
            return Err(OAuthError::invalid_grant(
                "redirect_uri does not match the authorization request",
            ));
        }
        let verifier = code_verifier
            .ok_or_else(|| OAuthError::invalid_request("code_verifier is required"))?;
        if !pkce_matches(verifier, &grant.code_challenge) {
            return Err(OAuthError::invalid_grant(
                "the PKCE code_verifier does not match",
            ));
        }
        if !self.accepts_resource(resource) {
            return Err(OAuthError::new(
                "invalid_target",
                format!("this server issues tokens for {} only", self.resource),
            ));
        }
        // Last, so a code refused for any reason above can still be redeemed by its rightful
        // client with the right verifier.
        self.redeem_once(code, grant.exp)?;
        let entry = self.live_key(ring, &grant.key)?;
        Ok(self.issue_tokens(&entry, &grant.client))
    }

    /// `grant_type=refresh_token`. Rotates: the answer carries a new refresh token. The old one
    /// stays valid until it expires, since nothing is stored to retire it; revoking the key is
    /// what ends them all.
    pub fn refresh(
        &self,
        ring: &KeyRing,
        credentials: &ClientCredentials<'_>,
        refresh_token: &str,
        resource: Option<&str>,
    ) -> Result<TokenPair, OAuthError> {
        let client = self.authenticate_client(credentials)?;
        let claims = self
            .signer
            .open::<TokenClaims>(REFRESH_PREFIX, refresh_token)
            .ok_or_else(|| OAuthError::invalid_grant("the refresh token is not valid"))?;
        if claims.exp < now() {
            return Err(OAuthError::invalid_grant("the refresh token has expired"));
        }
        if claims.client != client_hash(&client.id) || claims.aud != self.resource {
            return Err(OAuthError::invalid_grant(
                "the refresh token was issued to another client",
            ));
        }
        if !self.accepts_resource(resource) {
            return Err(OAuthError::new(
                "invalid_target",
                format!("this server issues tokens for {} only", self.resource),
            ));
        }
        let entry = self.live_key(ring, &claims.key)?;
        Ok(self.issue_tokens(&entry, &claims.client))
    }

    fn redeem_once(&self, code: &str, exp: u64) -> Result<(), OAuthError> {
        // The tag identifies the code; the body around it adds nothing to the key of the map.
        let id = code.rsplit('.').next().unwrap_or(code).to_string();
        let mut redeemed = self.redeemed.lock().unwrap_or_else(|p| p.into_inner());
        let now = now();
        // Pruned as it goes: an entry outlives its code by nothing, so the map holds about a
        // minute of redemptions.
        redeemed.retain(|_, until| *until >= now);
        if redeemed.insert(id, exp).is_some() {
            return Err(OAuthError::invalid_grant(
                "the authorization code has already been used",
            ));
        }
        Ok(())
    }

    /// The key a grant was minted from, if it is still in the ring and can still read.
    fn live_key(&self, ring: &KeyRing, digest: &str) -> Result<Arc<KeyEntry>, OAuthError> {
        let digest = KeyDigest::parse(digest)
            .map_err(|_| OAuthError::invalid_grant("the grant names no key"))?;
        ring.by_digest(&digest)
            .filter(|entry| entry.has(Capability::Read))
            .ok_or_else(|| {
                OAuthError::invalid_grant("the key this grant was issued for has been revoked")
            })
    }

    fn issue_tokens(&self, entry: &KeyEntry, client: &str) -> TokenPair {
        let key = entry.digest().to_config_value();
        let now = now();
        let access_token = self.signer.seal(
            ACCESS_PREFIX,
            &TokenClaims {
                key: key.clone(),
                client: client.to_string(),
                aud: self.resource.clone(),
                exp: now + self.access_ttl,
                nonce: nonce(),
            },
        );
        let refresh_token = self.signer.seal(
            REFRESH_PREFIX,
            &TokenClaims {
                key,
                client: client.to_string(),
                aud: self.resource.clone(),
                exp: now + self.refresh_ttl,
                nonce: nonce(),
            },
        );
        TokenPair {
            access_token,
            refresh_token,
            expires_in: self.access_ttl,
        }
    }

    /// A server with a fixed secret, for tests outside this module.
    #[cfg(test)]
    pub fn for_test(issuer: &str) -> Self {
        Self::with_key(issuer.to_string(), SigningKey([7; 32]), 3600, 86_400)
    }

    /// An access token for `entry`, as the token endpoint would issue it.
    #[cfg(test)]
    pub fn access_token_for_test(&self, entry: &KeyEntry) -> String {
        self.issue_tokens(entry, "test-client").access_token
    }

    // --- The gate -----------------------------------------------------------------------------

    /// Resolve an access token to the key it stands for.
    pub fn verify_access(&self, ring: &KeyRing, token: &str) -> Result<Grant, TokenRefusal> {
        let claims = self
            .signer
            .open::<TokenClaims>(ACCESS_PREFIX, token)
            .ok_or(TokenRefusal::Invalid)?;
        if claims.exp < now() {
            return Err(TokenRefusal::Expired);
        }
        if claims.aud != self.resource {
            return Err(TokenRefusal::WrongAudience);
        }
        let digest = KeyDigest::parse(&claims.key).map_err(|_| TokenRefusal::Invalid)?;
        let entry = ring.by_digest(&digest).ok_or(TokenRefusal::KeyRevoked)?;
        Ok(Grant {
            entry,
            client: claims.client,
        })
    }
}

/// A short, stable name for a client id: what codes and tokens carry instead of the id itself.
fn client_hash(client_id: &str) -> String {
    B64.encode(&Sha256::digest(client_id.as_bytes())[..16])
}

/// PKCE S256 (RFC 7636 §4.6): `BASE64URL(SHA256(verifier)) == challenge`.
fn pkce_matches(verifier: &str, challenge: &str) -> bool {
    if !VERIFIER_LEN.contains(&verifier.len())
        || !verifier
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~'))
    {
        return false;
    }
    let computed = B64.encode(Sha256::digest(verifier.as_bytes()));
    subtle::ConstantTimeEq::ct_eq(computed.as_bytes(), challenge.as_bytes()).into()
}

/// Whether `challenge` is shaped like an S256 challenge: 43 base64url characters.
pub fn is_s256_challenge(challenge: &str) -> bool {
    challenge.len() == 43
        && challenge
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
}

fn read_secret(path: &Path) -> Result<SigningKey> {
    let mut text = std::fs::read_to_string(path).with_context(|| {
        format!(
            "cannot read security.oauth.signing_key_file {}",
            path.display()
        )
    })?;
    let key = SigningKey::parse(&text)
        .with_context(|| format!("security.oauth.signing_key_file {}", path.display()));
    text.zeroize();
    key
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::{ApiKey, ApiKeyConfig, Role, SecurityConfig};

    const ISSUER: &str = "https://db.example.com";

    fn oauth() -> OAuth {
        OAuth::with_key(ISSUER.to_string(), SigningKey([7; 32]), 3600, 86_400)
    }

    /// A ring with one key of `role`, and that key.
    fn ring(role: Role) -> (KeyRing, ApiKey) {
        let key = ApiKey::generate().unwrap();
        let security = SecurityConfig {
            enabled: true,
            api_keys: vec![ApiKeyConfig {
                key_hash: Some(key.digest().to_config_value()),
                role: Some(role),
                label: Some("user_1".to_string()),
                ..Default::default()
            }],
            ..Default::default()
        };
        (security.load_keyring().unwrap(), key)
    }

    fn verifier() -> String {
        "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk".to_string()
    }

    fn challenge(verifier: &str) -> String {
        B64.encode(Sha256::digest(verifier.as_bytes()))
    }

    fn public_client(o: &OAuth) -> String {
        o.register(
            vec!["https://client.example/callback".to_string()],
            Some("Client".to_string()),
            ClientAuth::None,
        )
        .unwrap()
        .0
    }

    fn creds(client_id: &str) -> ClientCredentials<'_> {
        ClientCredentials {
            client_id: Some(client_id),
            client_secret: None,
            basic: false,
        }
    }

    /// Run the flow up to a code, for a key in `ring`.
    fn code_for(o: &OAuth, ring: &KeyRing, key: &ApiKey, client_id: &str) -> String {
        let sealed = o.seal_request(
            client_id,
            "https://client.example/callback",
            &challenge(&verifier()),
            Some("xyz"),
        );
        let request = o.open_request(&sealed).unwrap();
        let entry = ring.authenticate(key.expose()).unwrap();
        o.issue_code(&request, &entry)
    }

    #[test]
    fn the_flow_ends_in_a_token_that_resolves_to_the_key() {
        let o = oauth();
        let (ring, key) = ring(Role::Reader);
        let client = public_client(&o);
        let code = code_for(&o, &ring, &key, &client);
        let pair = o
            .exchange_code(
                &ring,
                &creds(&client),
                &code,
                Some("https://client.example/callback"),
                Some(&verifier()),
                Some("https://db.example.com/mcp"),
            )
            .unwrap();
        assert!(pair.access_token.starts_with(ACCESS_PREFIX));
        let grant = o.verify_access(&ring, &pair.access_token).unwrap();
        assert_eq!(grant.entry.key_id(), key.digest().key_id());

        // A refresh gives a new pair that resolves the same way.
        let again = o
            .refresh(&ring, &creds(&client), &pair.refresh_token, None)
            .unwrap();
        assert_ne!(again.access_token, pair.access_token);
        assert!(o.verify_access(&ring, &again.access_token).is_ok());
    }

    #[test]
    fn a_code_is_refused_for_a_wrong_verifier_a_second_use_or_another_client() {
        let o = oauth();
        let (ring, key) = ring(Role::Reader);
        let client = public_client(&o);
        let redirect = Some("https://client.example/callback");

        let code = code_for(&o, &ring, &key, &client);
        let wrong = "x".repeat(43);
        let err = o
            .exchange_code(&ring, &creds(&client), &code, redirect, Some(&wrong), None)
            .err()
            .unwrap();
        assert_eq!(err.error, "invalid_grant");
        // Refused above for the verifier, so it is not spent: the rightful client still can.
        assert!(
            o.exchange_code(
                &ring,
                &creds(&client),
                &code,
                redirect,
                Some(&verifier()),
                None
            )
            .is_ok()
        );
        let err = o
            .exchange_code(
                &ring,
                &creds(&client),
                &code,
                redirect,
                Some(&verifier()),
                None,
            )
            .err()
            .unwrap();
        assert!(err.description.contains("already been used"));

        // Another client: the same metadata in the same second would be the same id, which is
        // right — a client id is what it registered, not when.
        let (other, _, _) = o
            .register(
                vec!["https://client.example/callback".to_string()],
                Some("Other".to_string()),
                ClientAuth::None,
            )
            .unwrap();
        let code = code_for(&o, &ring, &key, &client);
        let err = o
            .exchange_code(
                &ring,
                &creds(&other),
                &code,
                redirect,
                Some(&verifier()),
                None,
            )
            .err()
            .unwrap();
        assert!(err.description.contains("another client"));

        let err = o
            .exchange_code(
                &ring,
                &creds(&client),
                &code,
                Some("https://elsewhere.example/cb"),
                Some(&verifier()),
                None,
            )
            .err()
            .unwrap();
        assert!(err.description.contains("redirect_uri"));

        let err = o
            .exchange_code(
                &ring,
                &creds(&client),
                &code,
                redirect,
                Some(&verifier()),
                Some("https://other.example/mcp"),
            )
            .err()
            .unwrap();
        assert_eq!(err.error, "invalid_target");
    }

    #[test]
    fn a_token_is_refused_when_altered_foreign_or_its_key_is_gone() {
        let o = oauth();
        let (ring, key) = ring(Role::Reader);
        let client = public_client(&o);
        let code = code_for(&o, &ring, &key, &client);
        let pair = o
            .exchange_code(
                &ring,
                &creds(&client),
                &code,
                Some("https://client.example/callback"),
                Some(&verifier()),
                None,
            )
            .unwrap();

        // One character of the body changed.
        let mut altered = pair.access_token.clone().into_bytes();
        let i = ACCESS_PREFIX.len() + 3;
        altered[i] = if altered[i] == b'A' { b'B' } else { b'A' };
        let altered = String::from_utf8(altered).unwrap();
        assert_eq!(
            o.verify_access(&ring, &altered).err(),
            Some(TokenRefusal::Invalid)
        );

        // Signed by another secret.
        let other = OAuth::with_key(ISSUER.to_string(), SigningKey([8; 32]), 3600, 86_400);
        assert_eq!(
            other.verify_access(&ring, &pair.access_token).err(),
            Some(TokenRefusal::Invalid)
        );

        // Another issuer with the same secret: the audience differs.
        let elsewhere = OAuth::with_key(
            "https://other.example.com".to_string(),
            SigningKey([7; 32]),
            3600,
            86_400,
        );
        assert_eq!(
            elsewhere.verify_access(&ring, &pair.access_token).err(),
            Some(TokenRefusal::WrongAudience)
        );

        // A refresh token is not an access token, nor a code.
        assert_eq!(
            o.verify_access(&ring, &pair.refresh_token).err(),
            Some(TokenRefusal::Invalid)
        );

        // The key revoked: an empty ring.
        let (other_ring, _) = self::ring(Role::Reader);
        assert_eq!(
            o.verify_access(&other_ring, &pair.access_token).err(),
            Some(TokenRefusal::KeyRevoked)
        );
        assert!(
            o.refresh(&other_ring, &creds(&client), &pair.refresh_token, None)
                .is_err()
        );
    }

    #[test]
    fn an_expired_token_is_refused() {
        let o = oauth();
        let (ring, key) = ring(Role::Reader);
        let entry = ring.authenticate(key.expose()).unwrap();
        let token = o.signer.seal(
            ACCESS_PREFIX,
            &TokenClaims {
                key: entry.digest().to_config_value(),
                client: "c".to_string(),
                aud: o.resource.clone(),
                exp: now() - 1,
                nonce: nonce(),
            },
        );
        assert_eq!(
            o.verify_access(&ring, &token).err(),
            Some(TokenRefusal::Expired)
        );
    }

    #[test]
    fn a_confidential_client_must_present_its_secret() {
        let o = oauth();
        let (ring, key) = ring(Role::Reader);
        let (client, secret, _) = o
            .register(
                vec!["https://client.example/callback".to_string()],
                None,
                ClientAuth::ClientSecretPost,
            )
            .unwrap();
        let secret = secret.unwrap();
        let redirect = Some("https://client.example/callback");

        let code = code_for(&o, &ring, &key, &client);
        let err = o
            .exchange_code(
                &ring,
                &creds(&client),
                &code,
                redirect,
                Some(&verifier()),
                None,
            )
            .err()
            .unwrap();
        assert_eq!(err.error, "invalid_client");

        let wrong = ClientCredentials {
            client_id: Some(&client),
            client_secret: Some("nope"),
            basic: false,
        };
        assert!(
            o.exchange_code(&ring, &wrong, &code, redirect, Some(&verifier()), None)
                .is_err()
        );

        let right = ClientCredentials {
            client_id: Some(&client),
            client_secret: Some(&secret),
            basic: true,
        };
        assert!(
            o.exchange_code(&ring, &right, &code, redirect, Some(&verifier()), None)
                .is_ok()
        );
    }

    #[test]
    fn a_key_that_cannot_read_gets_no_token() {
        let o = oauth();
        let (ring, key) = ring(Role::Provisioner);
        let client = public_client(&o);
        let code = code_for(&o, &ring, &key, &client);
        let err = o
            .exchange_code(
                &ring,
                &creds(&client),
                &code,
                Some("https://client.example/callback"),
                Some(&verifier()),
                None,
            )
            .err()
            .unwrap();
        assert_eq!(err.error, "invalid_grant");
    }

    #[test]
    fn redirect_uris_are_checked_at_registration_and_loopback_takes_any_port() {
        for ok in [
            "https://claude.ai/api/mcp/auth_callback",
            "http://localhost:3000/callback",
            "http://127.0.0.1/cb",
            "http://[::1]:8080/cb",
            "com.example.app:/oauth",
        ] {
            assert!(check_redirect_uri(ok).is_ok(), "{ok}");
        }
        for bad in [
            "http://example.com/cb",
            "https://a.example/cb#frag",
            "javascript:alert(1)",
            "data:text/html,x",
            "https://user@evil.example/cb",
            "https:///cb",
            "not a uri",
        ] {
            assert!(check_redirect_uri(bad).is_err(), "{bad}");
        }

        let o = oauth();
        let (id, _, _) = o
            .register(
                vec!["http://127.0.0.1/callback".to_string()],
                None,
                ClientAuth::None,
            )
            .unwrap();
        let client = o.client(&id).unwrap();
        assert!(
            client
                .redirect_uri(Some("http://127.0.0.1:53124/callback"))
                .is_some()
        );
        assert!(
            client
                .redirect_uri(Some("http://127.0.0.1:53124/other"))
                .is_none()
        );
        assert_eq!(client.redirect_uri(None), Some("http://127.0.0.1/callback"));
    }

    #[test]
    fn a_client_id_cannot_be_forged_or_swapped_for_another_artifact() {
        let o = oauth();
        let id = public_client(&o);
        assert!(o.client(&id).is_some());
        assert!(
            o.client(&id.replacen(CLIENT_PREFIX, REQUEST_PREFIX, 1))
                .is_none()
        );
        assert!(o.client("cameo_client1_e30.AAAA").is_none());
        assert!(o.client(&"x".repeat(MAX_ARTIFACT_LEN + 1)).is_none());
    }

    #[test]
    fn issuer_rules() {
        assert_eq!(
            normalize_issuer("https://db.example.com/").unwrap(),
            "https://db.example.com"
        );
        assert!(normalize_issuer("http://localhost:9480").is_ok());
        assert!(normalize_issuer("http://db.example.com").is_err());
        assert!(normalize_issuer("https://db.example.com/mcp").is_err());
        assert!(normalize_issuer("db.example.com").is_err());
    }

    #[test]
    fn the_config_names_what_it_is_missing() {
        let on = OAuthConfig {
            enabled: true,
            issuer: Some(ISSUER.to_string()),
            signing_key_file: Some(PathBuf::from("/etc/cameodb/keys/oauth")),
            ..Default::default()
        };
        assert!(on.validate(true, true).is_ok());
        assert!(OAuthConfig::default().validate(false, false).is_ok());
        assert!(
            on.validate(false, true)
                .unwrap_err()
                .contains("[security] enabled")
        );
        assert!(
            on.validate(true, false)
                .unwrap_err()
                .contains("[mcp] enabled")
        );
        let no_issuer = OAuthConfig {
            issuer: None,
            ..on.clone()
        };
        assert!(
            no_issuer
                .validate(true, true)
                .unwrap_err()
                .contains("issuer")
        );
        let no_file = OAuthConfig {
            signing_key_file: None,
            ..on.clone()
        };
        assert!(no_file.validate(true, true).unwrap_err().contains("keygen"));
        let short_refresh = OAuthConfig {
            refresh_token_ttl_secs: 60,
            ..on
        };
        assert!(short_refresh.validate(true, true).is_err());
    }

    #[test]
    fn the_secret_round_trips_through_its_file_form() {
        let value = generate_secret().unwrap();
        assert!(value.starts_with(SECRET_PREFIX));
        let parsed = SigningKey::parse(&format!("{value}\n")).unwrap();
        assert_eq!(parsed.to_file_value(), value);
        assert!(SigningKey::parse("cameo_v1_abc").is_err());
    }

    #[test]
    fn pkce_follows_the_rfc_example() {
        // RFC 7636 appendix B.
        assert!(pkce_matches(
            "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk",
            "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"
        ));
        assert!(!pkce_matches(
            "short",
            "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"
        ));
        assert!(is_s256_challenge(
            "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"
        ));
    }
}
