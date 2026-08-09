//! API key authentication: the credential model.
//!
//! One constraint shapes this whole file: **the configuration never holds a usable
//! credential.** Only a SHA-256 digest of a key is stored, and [`run_keygen`] is the only
//! code that ever sees a key. An operator who leaks a config file, a config dump, or a
//! backup of `/etc/cameodb` leaks nothing that can authenticate.
//!
//! An unsalted, unstretched SHA-256 is normally the wrong way to store a credential. It is
//! the right way here because a key is not a password: [`ApiKey::generate`] is the only
//! source of keys, every key is 256 bits of OS entropy, and [`KeyRing::authenticate`]
//! refuses anything that is not shaped like one *before* hashing it. There is no guessable
//! input to protect, so a KDF would only add latency to every request. The format gate is
//! what makes that argument hold — without it, someone could paste `sha256(<passphrase>)`
//! into the config and reintroduce exactly the problem a KDF exists to solve.
//!
//! This module is the model. [`crate::authz`] is the enforcement: it holds the route table
//! and the middleware that consults a [`KeyRing`] in front of the router. What is still
//! missing is per-tool authorization inside MCP and index filtering on the list endpoints
//! (ROADMAP Phase 14 Stage B1 steps 3–4); until those land, an index-scoped key is refused
//! at `/mcp` outright rather than allowed to escape its scope.

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use base64::Engine;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use tracing::warn;
use zeroize::Zeroize;

/// One thing a caller is allowed to do.
///
/// Routes require capabilities, never roles. Keeping the route table role-agnostic is what
/// lets per-index overrides (Stage C3) subtract a capability from one key without every
/// route having to learn about roles.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Capability {
    /// Search, streaming search, read index config, list indexes.
    Read,
    /// Document write, streaming ingest, bulk.
    Write,
    /// Create index, evolve schema, delete index.
    IndexAdmin,
    /// `/_admin/*` — memory, purge, workers, commit, evict-writer.
    NodeAdmin,
}

impl Capability {
    /// The name used in refusal messages and log lines.
    pub fn as_str(self) -> &'static str {
        match self {
            Capability::Read => "read",
            Capability::Write => "write",
            Capability::IndexAdmin => "index-admin",
            Capability::NodeAdmin => "node-admin",
        }
    }
}

/// A named bundle of capabilities.
///
/// Three roles rather than a free-form capability list per key: a key's authority has to be
/// legible at a glance in a config file and in an audit line, and the three answers below
/// are the ones deployments actually need. A key that needs something in between gets a
/// per-index override (Stage C3), not a fourth role.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    /// Everything, including node administration.
    Admin,
    /// Read and write documents, but not index or node administration.
    Writer,
    /// Read only.
    Reader,
}

impl Role {
    /// Every role, most privileged first. Iterated wherever roles are reported, so the
    /// order a summary appears in does not depend on the order keys were configured.
    pub const ALL: [Role; 3] = [Role::Admin, Role::Writer, Role::Reader];

    pub fn as_str(self) -> &'static str {
        match self {
            Role::Admin => "admin",
            Role::Writer => "writer",
            Role::Reader => "reader",
        }
    }

    pub fn parse(s: &str) -> Result<Self, String> {
        match s.trim().to_ascii_lowercase().as_str() {
            "admin" => Ok(Role::Admin),
            "writer" => Ok(Role::Writer),
            "reader" => Ok(Role::Reader),
            other => Err(format!(
                "unknown role '{}' (expected one of: admin, writer, reader)",
                other
            )),
        }
    }

    /// The capabilities this role bundles.
    pub fn capabilities(self) -> &'static [Capability] {
        match self {
            Role::Admin => &[
                Capability::Read,
                Capability::Write,
                Capability::IndexAdmin,
                Capability::NodeAdmin,
            ],
            Role::Writer => &[Capability::Read, Capability::Write],
            Role::Reader => &[Capability::Read],
        }
    }

    pub fn has(self, capability: Capability) -> bool {
        self.capabilities().contains(&capability)
    }
}

impl fmt::Display for Role {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Prefix every minted key carries.
///
/// It makes a leaked string recognisable as a CameoDB credential — secret scanners key off
/// prefixes like this — and it versions the format, so a future scheme can be told apart
/// from this one instead of being guessed at by length.
const KEY_PREFIX: &str = "cameo_v1_";

/// 32 bytes, base64url, unpadded.
const KEY_BODY_LEN: usize = 43;

/// Entropy per key. 256 bits is what makes the unsalted digest and the absence of any
/// lockout on failed authentication (a non-goal, deliberately) defensible.
const KEY_BYTES: usize = 32;

const BASE64: base64::engine::general_purpose::GeneralPurpose =
    base64::engine::general_purpose::URL_SAFE_NO_PAD;

/// A key in the clear.
///
/// Follows the [`crate::config::ClusterPsk`] precedent: redacted `Debug`, never serialized,
/// scrubbed on drop. Only [`run_keygen`] ever constructs one, and only long enough to print
/// it — the server never holds a key, only digests.
pub struct ApiKey(String);

impl ApiKey {
    /// Mint a key from OS entropy.
    ///
    /// `getrandom` rather than a seeded generator: this runs once per key on an operator's
    /// terminal, so there is nothing to amortise, and a key is the one place where "the
    /// entropy source is definitely the OS" is worth more than convenience.
    pub fn generate() -> Result<Self> {
        let mut bytes = [0u8; KEY_BYTES];
        getrandom::fill(&mut bytes)
            .context("failed to read entropy from the operating system for a new API key")?;
        let body = BASE64.encode(bytes);
        bytes.zeroize();
        debug_assert_eq!(body.len(), KEY_BODY_LEN);
        Ok(Self(format!("{KEY_PREFIX}{body}")))
    }

    /// The key itself. Named so that every call site reads as a deliberate decision.
    pub fn expose(&self) -> &str {
        &self.0
    }

    pub fn digest(&self) -> KeyDigest {
        KeyDigest::of_token(&self.0)
    }
}

impl fmt::Debug for ApiKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ApiKey(<redacted:{}>)", self.digest().key_id())
    }
}

impl Drop for ApiKey {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

/// True when `token` has the exact shape [`ApiKey::generate`] produces.
///
/// This is the gate the security argument rests on: a passphrase, a UUID, or an empty
/// string can never authenticate no matter whose digest sits in the config, so the only
/// credentials this server will ever accept are 256-bit random ones. Checked before
/// hashing, which also means a flood of junk tokens costs a length check rather than a
/// SHA-256 each.
fn has_key_shape(token: &str) -> bool {
    let Some(body) = token.strip_prefix(KEY_PREFIX) else {
        return false;
    };
    body.len() == KEY_BODY_LEN
        && body
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

const DIGEST_PREFIX: &str = "sha256:";

/// SHA-256 of a key token — what the configuration stores.
///
/// Not a secret. Against 256 bits of entropy a digest is not something to work backwards
/// from, which is why it is safe to keep in a config file, print from `keygen`, and log the
/// first bytes of as a `key_id`. It is still compared in constant time: an authenticator
/// that returns early on the first differing byte is not a habit worth keeping, even where
/// the timing is unexploitable.
#[derive(Clone)]
pub struct KeyDigest([u8; 32]);

impl KeyDigest {
    fn of_token(token: &str) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(token.as_bytes());
        Self(hasher.finalize().into())
    }

    /// Parse the `sha256:<64 hex>` form used in the configuration.
    ///
    /// The algorithm prefix is required rather than inferred from the length: the day this
    /// grows a second digest, an old config must not be silently reinterpreted.
    pub fn parse(raw: &str) -> Result<Self, String> {
        let trimmed = raw.trim();
        let hex_part = trimmed.strip_prefix(DIGEST_PREFIX).ok_or_else(|| {
            format!(
                "key hash must start with '{DIGEST_PREFIX}' — mint one with `cameodb keygen`, \
                 which prints the stanza to paste"
            )
        })?;
        if hex_part.len() != 64 {
            return Err(format!(
                "key hash must be '{DIGEST_PREFIX}' followed by 64 hex characters; found {} \
                 character(s) after the prefix",
                hex_part.len()
            ));
        }
        let bytes = hex::decode(hex_part)
            .map_err(|_| format!("key hash after '{DIGEST_PREFIX}' is not hexadecimal"))?;
        let mut digest = [0u8; 32];
        digest.copy_from_slice(&bytes);
        Ok(Self(digest))
    }

    /// The `sha256:<hex>` form to paste into a config file.
    pub fn to_config_value(&self) -> String {
        format!("{DIGEST_PREFIX}{}", hex::encode(self.0))
    }

    /// Short, stable, non-secret identity for logs and audit records.
    ///
    /// Derived from the digest rather than assigned, so the same key has the same id on
    /// every node in a cluster without anything having to be distributed.
    pub fn key_id(&self) -> String {
        hex::encode(&self.0[..4])
    }
}

impl PartialEq for KeyDigest {
    fn eq(&self, other: &Self) -> bool {
        self.0.ct_eq(&other.0).into()
    }
}

impl Eq for KeyDigest {}

impl fmt::Debug for KeyDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "KeyDigest({})", self.key_id())
    }
}

/// `[security]` — authentication for the HTTP and MCP surface.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct SecurityConfig {
    /// Require a key on every request that is not explicitly public (default: false).
    ///
    /// Off by default so an upgrade cannot lock an existing deployment out of its own data.
    /// Whether that default is *acceptable* is the posture system's decision, not this
    /// field's: `external` refuses it, `internal` warns, `local` accepts it.
    pub enabled: bool,

    /// `[[security.api_keys]]` entries.
    pub api_keys: Vec<ApiKeyConfig>,

    /// The single key assembled from `--api-key-hash` and `--api-key-role`.
    ///
    /// Not part of the file format, which is why it is skipped rather than folded into
    /// `api_keys`: a container can be handed one key without mounting a config file, and an
    /// override stays distinguishable from a file entry when either is reported in an error.
    #[serde(skip)]
    pub override_key: Option<ApiKeyConfig>,

    /// `[security.limits]` — what a caller may spend on MCP tool calls.
    ///
    /// Under `[security]` rather than a section of its own because it is enforced against an
    /// authenticated identity: the thing being metered is a *key*, and a key is this
    /// section's subject. Inert by default.
    pub limits: crate::ratelimit::McpLimitsConfig,

    /// `[security.audit]` — what the node keeps about who called it.
    ///
    /// Here for the same reason as `limits`: the record it writes is *about a key*, so it
    /// belongs to the section that defines keys. Off by default.
    pub audit: crate::audit::AuditConfig,
}

/// One `[[security.api_keys]]` entry, exactly as written.
///
/// Every field is optional so that a missing one produces this module's own error naming
/// the offending entry, rather than a serde message pointing at a line number in a file the
/// operator may not have written.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ApiKeyConfig {
    /// `sha256:<64 hex>`. Takes precedence over `key_hash_file`.
    pub key_hash: Option<String>,

    /// Path to a file holding nothing but the `sha256:<64 hex>` line.
    pub key_hash_file: Option<PathBuf>,

    /// `admin`, `writer`, or `reader`.
    pub role: Option<Role>,

    /// Audit identity — a team or service name. Not a secret and not a credential; it is
    /// what makes a `key_id` in a log line mean something to a human.
    pub label: Option<String>,

    /// Indexes this key may touch. Omitted means all of them.
    ///
    /// Honored for every role, not just readers: an ingest key for one tenant has no
    /// business writing to another tenant's index.
    pub allowed_indexes: Option<Vec<String>>,
}

impl SecurityConfig {
    /// The flag/environment key entry, created on first use.
    ///
    /// `--api-key-hash` and `--api-key-role` are separate overrides that have to land in one
    /// entry; this is where they meet.
    pub fn override_key_mut(&mut self) -> &mut ApiKeyConfig {
        self.override_key.get_or_insert_with(ApiKeyConfig::default)
    }

    /// Resolve and validate every entry into a usable key ring.
    ///
    /// The single place the `[security]` rules live — the same relationship
    /// [`crate::config::ClusterConfig::load_psk`] has to the cluster PSK, and for the same
    /// reason: a config that validates has to be one the server can actually authenticate
    /// against, which cannot be true if the format rules exist in two places.
    ///
    /// Entries are resolved even when `enabled = false`. A broken key file should be found
    /// by whoever writes it, not by whoever later flips the switch.
    pub fn load_keyring(&self) -> Result<KeyRing> {
        let file_entries = self
            .api_keys
            .iter()
            .enumerate()
            .map(|(i, entry)| (format!("[[security.api_keys]] entry {}", i + 1), entry));
        let override_entry = self
            .override_key
            .iter()
            .map(|entry| ("--api-key-hash / CAMEODB_API_KEY_HASH".to_string(), entry));

        let mut resolved: Vec<Arc<KeyEntry>> = Vec::new();
        for (origin, entry) in file_entries.chain(override_entry) {
            // Name the entry the way the operator wrote it: an error that says "label
            // 'team-a'" is actionable, an error that says "index 2" sends them counting.
            let origin = match &entry.label {
                Some(label) if !label.trim().is_empty() => format!("{origin} (label '{label}')"),
                _ => origin,
            };

            let raw_hash = match (&entry.key_hash, &entry.key_hash_file) {
                (Some(hash), _) => hash.clone(),
                (None, Some(path)) => read_key_hash_file(path)
                    .with_context(|| format!("{origin}: key_hash_file is unusable"))?,
                (None, None) => bail!(
                    "{origin}: needs key_hash or key_hash_file. `cameodb keygen --role \
                     <role>` mints a key and prints both forms"
                ),
            };

            let digest =
                KeyDigest::parse(&raw_hash).map_err(|e| anyhow::anyhow!("{origin}: {e}"))?;

            let role = entry.role.ok_or_else(|| {
                anyhow::anyhow!("{origin}: needs a role (admin, writer, or reader)")
            })?;

            let allowed_indexes = match &entry.allowed_indexes {
                None => None,
                Some(indexes) => {
                    let cleaned: Vec<String> = indexes
                        .iter()
                        .map(|index| index.trim().to_string())
                        .filter(|index| !index.is_empty())
                        .collect();
                    if cleaned.is_empty() {
                        // An empty allow-list reads as "no restriction" and means "nothing
                        // permitted". Refuse rather than pick one of those meanings.
                        bail!(
                            "{origin}: allowed_indexes is empty, which would permit no index at \
                             all. Remove the field to allow every index, or name the indexes"
                        );
                    }
                    Some(cleaned)
                }
            };

            // Two entries with one digest is a config that cannot mean what it says: the
            // same key would map to two roles, decided by ordering.
            if let Some(existing) = resolved.iter().find(|k| k.digest == digest) {
                bail!(
                    "{origin}: this key hash is already configured as '{}' (key_id {}). One key \
                     cannot hold two roles",
                    existing.label,
                    existing.key_id()
                );
            }

            let label = entry
                .label
                .as_deref()
                .map(str::trim)
                .filter(|label| !label.is_empty())
                .map(str::to_string)
                .unwrap_or_else(|| format!("key-{}", digest.key_id()));

            resolved.push(Arc::new(KeyEntry {
                digest,
                role,
                label,
                allowed_indexes,
            }));
        }

        Ok(KeyRing {
            enabled: self.enabled,
            entries: resolved,
        })
    }
}

/// Read a `key_hash_file`, warning if anyone but the owner can write it.
///
/// Deliberately *not* the same rule as [`crate::config::ClusterConfig::load_psk`] applies to
/// `psk_file`, which warns when the file is merely readable. A digest is not a secret, so a
/// readable hash file is not a leak — but a *writable* one is a way to install your own key
/// and grant yourself a role, which is worse than either. Warn rather than refuse, for the
/// same reason the PSK check does: refusing over a permission bit would strand deployments
/// whose secrets are managed by an orchestrator.
/// Write `contents` to a file that must not already exist, readable only by its owner.
///
/// `create_new` rather than a check-then-write: it is atomic, and it means the answer to
/// "what if the file is already there" is decided by the kernel rather than by a race. The
/// mode is set at creation, so there is no window in which the file exists with a wider one.
fn write_new_secret_file(path: &Path, contents: &str) -> Result<()> {
    use std::io::Write;

    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }

    let mut file = options.open(path).map_err(|err| match err.kind() {
        std::io::ErrorKind::AlreadyExists => anyhow::anyhow!(
            "{} already exists. Refusing to overwrite it — remove it deliberately, or write \
             to a new path",
            path.display()
        ),
        _ => anyhow::anyhow!("cannot create {}: {err}", path.display()),
    })?;
    writeln!(file, "{contents}").with_context(|| format!("cannot write {}", path.display()))?;

    #[cfg(not(unix))]
    eprintln!(
        "⚠️  {} was created without restricting its permissions — this platform has no mode \
         to set. Restrict it yourself.",
        path.display()
    );

    Ok(())
}

fn read_key_hash_file(path: &Path) -> Result<String> {
    if !path.exists() {
        bail!("file not found: {}", path.display());
    }
    warn_if_key_hash_file_is_writable_by_others(path);
    let contents = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read {}", path.display()))?;
    let hash = contents.trim();
    if hash.is_empty() {
        bail!("file is empty: {}", path.display());
    }
    if hash.lines().count() > 1 {
        bail!(
            "file holds {} lines: {}. A key_hash_file contains one hash and nothing else — \
             one file per key",
            hash.lines().count(),
            path.display()
        );
    }
    Ok(hash.to_string())
}

#[cfg(unix)]
fn warn_if_key_hash_file_is_writable_by_others(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    if let Ok(meta) = std::fs::metadata(path) {
        let mode = meta.permissions().mode();
        if mode & 0o022 != 0 {
            warn!(
                path = %path.display(),
                mode = format!("{:o}", mode & 0o777),
                "key_hash_file is writable by group or others; anyone who can write it can \
                 grant themselves this key's role. chmod 644 it or tighter"
            );
        }
    }
}

#[cfg(not(unix))]
fn warn_if_key_hash_file_is_writable_by_others(_path: &Path) {}

/// One resolved, usable key: what a request will be authenticated against.
#[derive(Debug)]
pub struct KeyEntry {
    digest: KeyDigest,
    role: Role,
    label: String,
    allowed_indexes: Option<Vec<String>>,
}

impl KeyEntry {
    pub fn key_id(&self) -> String {
        self.digest.key_id()
    }

    pub fn label(&self) -> &str {
        &self.label
    }

    pub fn role(&self) -> Role {
        self.role
    }

    pub fn has(&self, capability: Capability) -> bool {
        self.role.has(capability)
    }

    /// True when this key is restricted to a named set of indexes.
    pub fn is_index_scoped(&self) -> bool {
        self.allowed_indexes.is_some()
    }

    /// Whether this key may touch `index`.
    ///
    /// `index` is the raw path segment, not percent-decoded. A scoped key therefore has to
    /// name indexes that need no encoding — and an encoded request for one of them is
    /// refused rather than allowed, which is the direction to fail in. Decoding here instead
    /// would mean comparing against a different string than the router hands the handler.
    pub fn allows_index(&self, index: &str) -> bool {
        match &self.allowed_indexes {
            None => true,
            Some(allowed) => allowed.iter().any(|permitted| permitted == index),
        }
    }

    /// This key's index scope, rendered for a log line.
    pub fn scope_summary(&self) -> String {
        match &self.allowed_indexes {
            None => "all indexes".to_string(),
            Some(indexes) => indexes.join(", "),
        }
    }
}

/// Every key this node will accept.
///
/// `Debug` is derived and safe to print: a [`KeyEntry`] holds only a digest, and
/// [`KeyDigest`]'s own `Debug` shows nothing but the `key_id`.
#[derive(Debug)]
pub struct KeyRing {
    enabled: bool,
    /// Behind `Arc` so an authenticated request can carry its identity as a cheap clone
    /// through the middleware, the handlers, and eventually an audit record, without the
    /// key ring having to be borrowed for the life of the request.
    entries: Vec<Arc<KeyEntry>>,
}

impl KeyRing {
    pub fn enabled(&self) -> bool {
        self.enabled
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn entries(&self) -> &[Arc<KeyEntry>] {
        &self.entries
    }

    /// True when at least one key holds `capability`, i.e. someone can actually use it.
    pub fn holds(&self, capability: Capability) -> bool {
        self.entries.iter().any(|entry| entry.has(capability))
    }

    /// `3 keys (1 admin, 2 reader)` — for the posture matrix and the startup banner.
    pub fn summary(&self) -> String {
        let counts: Vec<String> = Role::ALL
            .iter()
            .filter_map(|role| {
                let n = self.entries.iter().filter(|e| e.role == *role).count();
                (n > 0).then(|| format!("{n} {role}"))
            })
            .collect();
        match self.entries.len() {
            0 => "no keys".to_string(),
            1 => format!("1 key ({})", counts.join(", ")),
            n => format!("{n} keys ({})", counts.join(", ")),
        }
    }

    /// Resolve a presented token to the key that minted it, or `None`.
    ///
    /// Two properties matter here. The shape check runs first, so nothing but a
    /// [`ApiKey::generate`]-shaped token is ever hashed. And the loop does not exit early,
    /// so how long a rejection takes does not depend on which key nearly matched.
    pub fn authenticate(&self, presented: &str) -> Option<Arc<KeyEntry>> {
        let token = presented.trim();
        if !has_key_shape(token) {
            return None;
        }
        let digest = KeyDigest::of_token(token);
        let mut matched: Option<&Arc<KeyEntry>> = None;
        for entry in &self.entries {
            if entry.digest == digest && matched.is_none() {
                matched = Some(entry);
            }
        }
        matched.cloned()
    }
}

/// `cameodb keygen` — mint a key, print it once, print the configuration that accepts it.
///
/// The key goes to stdout and everything else to stderr, so `cameodb keygen --role reader >
/// key.txt` captures exactly the key and nothing to strip.
pub fn run_keygen<I>(args: I) -> Result<()>
where
    I: IntoIterator<Item = String>,
{
    const USAGE: &str = "\
cameodb keygen — mint an API key

Usage:
  cameodb keygen --role <admin|writer|reader> [--label <NAME>] [--allowed-indexes <A,B>]
                 [--key-out <PATH>] [--hash-out <PATH>]

Options:
  --role <ROLE>             admin (everything), writer (read and write), reader (read only)
  --label <NAME>            Audit identity for logs — a team or service name, not a secret
  --allowed-indexes <A,B>   Restrict this key to these indexes (default: every index)
  --key-out <PATH>          Write the key to PATH (mode 0600) instead of stdout.
                            For `client --api-key-file`.
  --hash-out <PATH>         Write the digest to PATH (mode 0600), for `key_hash_file`.
  -h, --help                Show this message

Without --key-out the key is printed to stdout and everything else to stderr, so redirecting
stdout captures just the key. Either way it is stored nowhere else: only its SHA-256 digest
belongs in the config, and a lost key is replaced rather than recovered.

Neither --key-out nor --hash-out will overwrite an existing file — replacing a key in place
is how a working node stops working.";

    let mut role: Option<Role> = None;
    let mut label: Option<String> = None;
    let mut allowed_indexes: Option<Vec<String>> = None;
    let mut key_out: Option<PathBuf> = None;
    let mut hash_out: Option<PathBuf> = None;

    let mut args = args.into_iter().peekable();
    while let Some(arg) = args.next() {
        let (name, inline) = match arg.split_once('=') {
            Some((name, value)) => (name.to_string(), Some(value.to_string())),
            None => (arg.clone(), None),
        };
        let mut value = |flag: &str| -> Result<String> {
            match inline.clone() {
                Some(value) => Ok(value),
                None => args
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("{flag} requires a value")),
            }
        };

        match name.as_str() {
            "-h" | "--help" => {
                eprintln!("{USAGE}");
                return Ok(());
            }
            "--role" => {
                role = Some(Role::parse(&value("--role")?).map_err(|e| anyhow::anyhow!(e))?)
            }
            "--label" => label = Some(value("--label")?),
            "--allowed-indexes" | "--allowed-index" => {
                let raw = value("--allowed-indexes")?;
                let parsed: Vec<String> = raw
                    .split([',', ';'])
                    .map(str::trim)
                    .filter(|index| !index.is_empty())
                    .map(str::to_string)
                    .collect();
                if parsed.is_empty() {
                    bail!("--allowed-indexes named no index; omit it to allow every index");
                }
                allowed_indexes = Some(parsed);
            }
            "--key-out" => key_out = Some(PathBuf::from(value("--key-out")?)),
            "--hash-out" => hash_out = Some(PathBuf::from(value("--hash-out")?)),
            other => bail!("unknown option: {other}\n\n{USAGE}"),
        }
    }

    let role = role
        .ok_or_else(|| anyhow::anyhow!("keygen needs --role <admin|writer|reader>\n\n{USAGE}"))?;

    let key = ApiKey::generate()?;
    let digest = key.digest();

    // Prove the stanza about to be printed actually accepts the key about to be printed.
    // Cheap here, once per key, and the alternative is an operator discovering a format or
    // hashing bug by being locked out of their own node.
    let ring = KeyRing {
        enabled: true,
        entries: vec![Arc::new(KeyEntry {
            digest: digest.clone(),
            role,
            label: label.clone().unwrap_or_else(|| "keygen".to_string()),
            allowed_indexes: allowed_indexes.clone(),
        })],
    };
    if ring.authenticate(key.expose()).is_none() {
        bail!("internal error: a freshly minted key does not verify against its own digest");
    }

    // Files first. A key printed and then not written is a key the operator has to notice
    // was never saved; a key written and then printed is at worst printed twice.
    if let Some(path) = &hash_out {
        write_new_secret_file(path, &digest.to_config_value())
            .with_context(|| format!("--hash-out {}", path.display()))?;
    }
    match &key_out {
        Some(path) => write_new_secret_file(path, key.expose())
            .with_context(|| format!("--key-out {}", path.display()))?,
        None => println!("{}", key.expose()),
    }

    let mut stanza = String::new();
    stanza.push_str("  [[security.api_keys]]\n");
    stanza.push_str(&format!("  key_hash = \"{}\"\n", digest.to_config_value()));
    stanza.push_str(&format!("  role = \"{}\"\n", role));
    if let Some(label) = &label {
        stanza.push_str(&format!("  label = \"{}\"\n", label));
    }
    if let Some(indexes) = &allowed_indexes {
        let list: Vec<String> = indexes.iter().map(|i| format!("\"{i}\"")).collect();
        stanza.push_str(&format!("  allowed_indexes = [{}]\n", list.join(", ")));
    }

    let whereabouts = match &key_out {
        Some(path) => format!("The key was written to {} (mode 0600).", path.display()),
        None => "The key above was printed to stdout and is not stored anywhere. Copy it now."
            .to_string(),
    };

    // What to put in the config: the file that was just written, if there is one, or the
    // literal hash and the command to put it in a file.
    let config_advice = match &hash_out {
        Some(path) => format!(
            "  [[security.api_keys]]\n  \
             key_hash_file = \"{}\"\n  \
             role = \"{role}\"\n\n\
             If the node runs as another user, chown that file to it — the server reads it at \
             startup.\n",
            path.display(),
            role = role,
        ),
        None => format!(
            "{stanza}\n\
             Or keep the hash out of the config file:\n\n  \
             cameodb keygen --role {role} --hash-out /etc/cameodb/keys/{file}\n\n  \
             [[security.api_keys]]\n  \
             key_hash_file = \"/etc/cameodb/keys/{file}\"\n  \
             role = \"{role}\"\n",
            stanza = stanza,
            role = role,
            file = label.as_deref().unwrap_or("cameodb"),
        ),
    };

    eprintln!(
        "\n{whereabouts}\n\n\
         Add to cameodb.toml:\n\n  \
         [security]\n  \
         enabled = true\n\n\
         {config_advice}\n\
         `cameodb check-config` reports what the node will enforce with this key in place.",
        whereabouts = whereabouts,
        config_advice = config_advice,
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(hash: &str, role: Role) -> ApiKeyConfig {
        ApiKeyConfig {
            key_hash: Some(hash.to_string()),
            role: Some(role),
            ..Default::default()
        }
    }

    #[test]
    fn minted_keys_authenticate_and_nothing_else_does() {
        let key = ApiKey::generate().unwrap();
        let config = SecurityConfig {
            enabled: true,
            api_keys: vec![entry(&key.digest().to_config_value(), Role::Writer)],
            override_key: None,
            ..Default::default()
        };
        let ring = config.load_keyring().unwrap();

        let matched = ring.authenticate(key.expose()).expect("key authenticates");
        assert_eq!(matched.role(), Role::Writer);
        // Surrounding whitespace survives a copy-paste through too many terminals to be
        // treated as a different credential.
        assert!(
            ring.authenticate(&format!("  {}  ", key.expose()))
                .is_some()
        );

        assert!(ring.authenticate("").is_none());
        assert!(
            ring.authenticate(key.expose().trim_start_matches("cameo_v1_"))
                .is_none()
        );
        assert!(ring.authenticate(&format!("{}x", key.expose())).is_none());
        let other = ApiKey::generate().unwrap();
        assert!(ring.authenticate(other.expose()).is_none());
    }

    #[test]
    fn a_hashed_passphrase_can_never_authenticate() {
        // The point of the shape gate: an operator who bypasses `keygen` and pastes the
        // digest of something guessable does not get a working credential out of it.
        let passphrase = "correct horse battery staple";
        let digest = KeyDigest::of_token(passphrase);
        let config = SecurityConfig {
            enabled: true,
            api_keys: vec![entry(&digest.to_config_value(), Role::Admin)],
            override_key: None,
            ..Default::default()
        };
        let ring = config.load_keyring().unwrap();
        assert!(ring.authenticate(passphrase).is_none());
        assert!(!has_key_shape(passphrase));
    }

    #[test]
    fn generated_keys_have_the_documented_shape() {
        let key = ApiKey::generate().unwrap();
        assert!(key.expose().starts_with(KEY_PREFIX));
        assert_eq!(key.expose().len(), KEY_PREFIX.len() + KEY_BODY_LEN);
        assert!(has_key_shape(key.expose()));
        // Two keys in a row must not be related; a fixed key would pass every other test
        // in this file.
        assert_ne!(key.expose(), ApiKey::generate().unwrap().expose());
    }

    #[test]
    fn a_key_never_prints_itself() {
        let key = ApiKey::generate().unwrap();
        let debug = format!("{:?}", key);
        assert!(!debug.contains(key.expose()), "{debug}");
        assert!(debug.contains(&key.digest().key_id()), "{debug}");
    }

    #[test]
    fn digest_round_trips_through_the_config_form() {
        let key = ApiKey::generate().unwrap();
        let digest = key.digest();
        let parsed = KeyDigest::parse(&digest.to_config_value()).unwrap();
        assert_eq!(parsed, digest);
        assert_eq!(parsed.key_id().len(), 8);
    }

    #[test]
    fn digest_rejects_a_hash_without_its_algorithm() {
        let hex = hex::encode([0u8; 32]);
        assert!(KeyDigest::parse(&hex).unwrap_err().contains("sha256:"));
        assert!(
            KeyDigest::parse("sha256:abc")
                .unwrap_err()
                .contains("64 hex")
        );
        assert!(
            KeyDigest::parse(&format!("sha256:{}", "z".repeat(64)))
                .unwrap_err()
                .contains("hexadecimal")
        );
    }

    #[test]
    fn an_entry_without_a_role_or_a_hash_is_refused() {
        let no_role = SecurityConfig {
            enabled: true,
            api_keys: vec![ApiKeyConfig {
                key_hash: Some(KeyDigest::of_token("x").to_config_value()),
                ..Default::default()
            }],
            override_key: None,
            ..Default::default()
        };
        assert!(
            no_role
                .load_keyring()
                .unwrap_err()
                .to_string()
                .contains("needs a role")
        );

        let no_hash = SecurityConfig {
            enabled: true,
            api_keys: vec![ApiKeyConfig {
                role: Some(Role::Reader),
                label: Some("team-a".to_string()),
                ..Default::default()
            }],
            override_key: None,
            ..Default::default()
        };
        let err = no_hash.load_keyring().unwrap_err().to_string();
        assert!(err.contains("key_hash"), "{err}");
        // The error has to name the entry the operator wrote, not an internal index.
        assert!(err.contains("team-a"), "{err}");
    }

    #[test]
    fn one_key_cannot_hold_two_roles() {
        let hash = KeyDigest::of_token("shared").to_config_value();
        let config = SecurityConfig {
            enabled: true,
            api_keys: vec![entry(&hash, Role::Reader), entry(&hash, Role::Admin)],
            override_key: None,
            ..Default::default()
        };
        let err = config.load_keyring().unwrap_err().to_string();
        assert!(err.contains("already configured"), "{err}");
    }

    #[test]
    fn an_empty_index_scope_is_refused_rather_than_guessed_at() {
        let config = SecurityConfig {
            enabled: true,
            api_keys: vec![ApiKeyConfig {
                allowed_indexes: Some(vec![]),
                ..entry(&KeyDigest::of_token("x").to_config_value(), Role::Reader)
            }],
            override_key: None,
            ..Default::default()
        };
        let err = config.load_keyring().unwrap_err().to_string();
        assert!(err.contains("allowed_indexes"), "{err}");
    }

    #[test]
    fn keys_are_validated_even_when_auth_is_disabled() {
        // Otherwise a broken key file is found by whoever flips `enabled`, months later.
        let config = SecurityConfig {
            enabled: false,
            api_keys: vec![entry("not-a-hash", Role::Admin)],
            override_key: None,
            ..Default::default()
        };
        assert!(config.load_keyring().is_err());
    }

    #[test]
    fn roles_bundle_the_capabilities_the_route_table_expects() {
        assert!(Role::Admin.has(Capability::NodeAdmin));
        assert!(Role::Writer.has(Capability::Write));
        assert!(!Role::Writer.has(Capability::IndexAdmin));
        assert!(!Role::Writer.has(Capability::NodeAdmin));
        assert!(Role::Reader.has(Capability::Read));
        assert!(!Role::Reader.has(Capability::Write));
        // Every role can read; a key that cannot read anything has no use.
        assert!(Role::ALL.iter().all(|r| r.has(Capability::Read)));
    }

    #[test]
    fn summary_counts_roles_in_a_stable_order() {
        let keys: Vec<ApiKeyConfig> = [Role::Reader, Role::Admin, Role::Reader]
            .into_iter()
            .enumerate()
            .map(|(i, role)| entry(&KeyDigest::of_token(&i.to_string()).to_config_value(), role))
            .collect();
        let ring = SecurityConfig {
            enabled: true,
            api_keys: keys,
            override_key: None,
            ..Default::default()
        }
        .load_keyring()
        .unwrap();
        assert_eq!(ring.summary(), "3 keys (1 admin, 2 reader)");
        assert!(ring.holds(Capability::NodeAdmin));
        assert!(!ring.holds(Capability::Write) || ring.holds(Capability::NodeAdmin));
    }

    #[test]
    fn a_key_ring_without_write_capability_is_visible_as_such() {
        let ring = SecurityConfig {
            enabled: true,
            api_keys: vec![entry(
                &KeyDigest::of_token("r").to_config_value(),
                Role::Reader,
            )],
            override_key: None,
            ..Default::default()
        }
        .load_keyring()
        .unwrap();
        assert!(!ring.holds(Capability::Write));
        assert!(!ring.holds(Capability::IndexAdmin));
        assert_eq!(ring.summary(), "1 key (1 reader)");
    }

    #[test]
    fn an_unlabelled_key_gets_an_identity_derived_from_its_digest() {
        let digest = KeyDigest::of_token("anonymous");
        let ring = SecurityConfig {
            enabled: true,
            api_keys: vec![entry(&digest.to_config_value(), Role::Reader)],
            override_key: None,
            ..Default::default()
        }
        .load_keyring()
        .unwrap();
        assert_eq!(
            ring.entries()[0].label(),
            format!("key-{}", digest.key_id())
        );
        assert_eq!(ring.entries()[0].scope_summary(), "all indexes");
    }

    #[test]
    fn a_hash_file_is_read_and_a_bad_one_is_named() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ops");
        let digest = KeyDigest::of_token("filed");
        std::fs::write(&path, format!("{}\n", digest.to_config_value())).unwrap();

        let config = SecurityConfig {
            enabled: true,
            api_keys: vec![ApiKeyConfig {
                key_hash_file: Some(path.clone()),
                role: Some(Role::Admin),
                ..Default::default()
            }],
            override_key: None,
            ..Default::default()
        };
        let ring = config.load_keyring().unwrap();
        assert_eq!(ring.entries()[0].role(), Role::Admin);

        // A file holding a whole config, or a key, or two hashes, is a mistake worth
        // naming rather than parsing the first line of.
        std::fs::write(&path, "sha256:aa\nsha256:bb\n").unwrap();
        let err = format!("{:#}", config.load_keyring().unwrap_err());
        assert!(err.contains("one hash"), "{err}");

        std::fs::write(&path, "   \n").unwrap();
        let err = format!("{:#}", config.load_keyring().unwrap_err());
        assert!(err.contains("empty"), "{err}");
    }

    #[test]
    fn a_missing_hash_file_names_the_path() {
        let config = SecurityConfig {
            enabled: true,
            api_keys: vec![ApiKeyConfig {
                key_hash_file: Some(PathBuf::from("/nonexistent/cameodb/key")),
                role: Some(Role::Reader),
                ..Default::default()
            }],
            override_key: None,
            ..Default::default()
        };
        let err = format!("{:#}", config.load_keyring().unwrap_err());
        assert!(err.contains("/nonexistent/cameodb/key"), "{err}");
    }

    #[test]
    fn an_inline_hash_wins_over_a_file_the_way_the_psk_does() {
        let inline = KeyDigest::of_token("inline");
        let config = SecurityConfig {
            enabled: true,
            api_keys: vec![ApiKeyConfig {
                key_hash: Some(inline.to_config_value()),
                key_hash_file: Some(PathBuf::from("/nonexistent/never-read")),
                role: Some(Role::Reader),
                ..Default::default()
            }],
            override_key: None,
            ..Default::default()
        };
        let ring = config.load_keyring().unwrap();
        assert_eq!(ring.entries()[0].key_id(), inline.key_id());
    }

    #[test]
    fn the_flag_provided_key_joins_the_ring_and_is_named_in_errors() {
        let mut config = SecurityConfig {
            enabled: true,
            ..Default::default()
        };
        config.override_key_mut().key_hash = Some(KeyDigest::of_token("flag").to_config_value());
        // Half-configured: a hash with no role is the mistake this pair invites.
        let err = config.load_keyring().unwrap_err().to_string();
        assert!(err.contains("--api-key-hash"), "{err}");

        config.override_key_mut().role = Some(Role::Admin);
        let ring = config.load_keyring().unwrap();
        assert_eq!(ring.entries().len(), 1);
        assert_eq!(ring.entries()[0].role(), Role::Admin);
    }
}
