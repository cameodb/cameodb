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
//! and the middleware that consults a [`KeyRing`] in front of the router. Per-tool
//! authorization inside MCP and index filtering on the list endpoints are part of that
//! enforcement today — the MCP dispatcher checks each tool's capability and the index it
//! names, and the catalogue handlers filter what a scoped key may enumerate. What is still
//! missing is per-index role overrides — a key with one role granted less on a named index —
//! which is Phase 14 Stage C3 in the roadmap.

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use base64::Engine;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use tracing::{info, warn};
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

/// What one tenant's data may add up to on this node.
///
/// Both ceilings are `0` — unlimited — by default, for the reason every limit in this file is:
/// *a patch release must not stop a working deployment over a value nobody wrote.* An operator
/// opts into a ceiling; an upgrade changes nothing until they do.
///
/// The two bound different things and neither implies the other. `max_indexes` bounds the
/// *count*, which is what costs resident memory — an open index is a writer arena and three OS
/// threads whatever it holds. `max_bytes` bounds the *size*, which is what costs disk. A tenant
/// with one enormous index and a tenant with a thousand empty ones are both problems, and they
/// are different problems.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TenantQuota {
    /// Most indexes this tenant may own. `0` (the default) is unlimited.
    #[serde(default)]
    pub max_indexes: usize,

    /// Most bytes this tenant's indexes may occupy in total. `0` (the default) is unlimited.
    ///
    /// Measured from a cached reading rather than a fresh walk of every index directory, so a
    /// tenant can overshoot by up to one refresh interval's worth of ingest. That is the honest
    /// trade and it is stated in the docs: the alternative is a directory walk on the write
    /// path, which is the cost `commit_index` had removed from it earlier in this cycle.
    #[serde(default)]
    pub max_bytes: u64,
}

/// `[security]` — authentication for the HTTP and MCP surface.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SecurityConfig {
    /// Require a key on every request that is not explicitly public (default: false).
    ///
    /// Off by default so an upgrade cannot lock an existing deployment out of its own data.
    /// Whether that default is *acceptable* is the posture system's decision, not this
    /// field's: `external` refuses it, `internal` warns, `local` accepts it.
    pub enabled: bool,

    /// Whether a write to an index that does not exist may create it (default: true).
    ///
    /// Implicit creation is what makes semi-structured input work: the first document an
    /// index sees becomes its schema. On by default so an upgrade does not start refusing
    /// writes it used to serve. Set `false` where minting indexes must be an explicit
    /// decision — a write to an index with no schema anywhere is then refused, and the index
    /// has to be created with `PUT /api/{index}/_config`, which needs the `IndexAdmin`
    /// capability. Applies to every write whoever sends it: a capability grants the write,
    /// and this decides what the write may cause.
    pub implicit_index_creation: bool,

    /// `[[security.api_keys]]` entries.
    pub api_keys: Vec<ApiKeyConfig>,

    /// `[security.tenants.<name>]` — what one tenant's indexes may add up to.
    ///
    /// Keyed by the name keys carry in [`ApiKeyConfig::tenant`]. A tenant with no entry here
    /// has no ceiling, and so does every tenant on a node whose operator has not written one:
    /// this bounds what a tenant accumulates only once someone decides what the bound is.
    #[serde(default)]
    pub tenants: std::collections::HashMap<String, TenantQuota>,

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

impl Default for SecurityConfig {
    /// Written by hand because the derived one would set `implicit_index_creation: false`,
    /// and a default that refuses what an upgrade used to serve is the trap `enabled`'s
    /// own default exists to avoid.
    fn default() -> Self {
        Self {
            enabled: false,
            implicit_index_creation: true,
            api_keys: Vec::new(),
            tenants: std::collections::HashMap::new(),
            override_key: None,
            limits: crate::ratelimit::McpLimitsConfig::default(),
            audit: crate::audit::AuditConfig::default(),
        }
    }
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

    /// Per-index role **subtraction**: on a named index this key holds the listed role
    /// instead of its own.
    ///
    /// The case this exists for is a `writer` that must be read-only on one sensitive index
    /// while keeping write elsewhere. Written as a role rather than a capability list because
    /// a key's authority has to stay legible at a glance, and because an override that can
    /// only name an existing role cannot invent an authority the role vocabulary does not
    /// have.
    ///
    /// **Subtraction only.** An override that grants more than the key's own role is refused
    /// at load: this is a mechanism for reducing a key's reach on one index, never for
    /// widening it, and a config that reads as if it widens one is a mistake worth stopping
    /// at startup rather than honoring.
    ///
    /// ```toml
    /// role = "writer"
    /// index_overrides = { audit = "reader" }
    /// ```
    #[serde(default)]
    pub index_overrides: Option<std::collections::HashMap<String, Role>>,

    /// Which tenant's budget this key spends against.
    ///
    /// Quotas are per *tenant* rather than per key because a tenant is rarely one key: an
    /// importer, a read-only dashboard key and a rotation spare are three credentials and one
    /// customer. Metering them separately would let a tenant multiply their allowance by
    /// issuing keys, and would reset their usage every time one was rotated.
    ///
    /// Omitted means this key belongs to no tenant and spends against no budget — which is the
    /// default, and what every key on an upgraded node reads as.
    pub tenant: Option<String>,
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

            // Per-index overrides, validated hard because a mistake here is a silent grant.
            let mut index_overrides = std::collections::HashMap::new();
            for (index, override_role) in entry.index_overrides.iter().flatten() {
                let index = index.trim();
                if index.is_empty() {
                    bail!("{origin}: index_overrides has an entry with an empty index name");
                }

                // Subtraction only. An override that holds a capability the key's own role
                // does not is an escalation, and the whole point of this mechanism is that it
                // cannot be one. Refused at startup rather than honored or silently clamped:
                // an operator who wrote it meant something, and neither of the other two
                // outcomes tells them it was impossible.
                let widened: Vec<&str> = override_role
                    .capabilities()
                    .iter()
                    .filter(|capability| !role.has(**capability))
                    .map(|capability| capability.as_str())
                    .collect();
                if !widened.is_empty() {
                    bail!(
                        "{origin}: index_overrides for '{index}' grants '{}', which holds {} \
                         that the key's own role '{}' does not. An override may only subtract \
                         from a key's authority, never add to it",
                        override_role.as_str(),
                        widened.join(" and "),
                        role.as_str()
                    );
                }

                // An override for an index the key cannot reach at all is dead config. It
                // reads as protection and provides none, which is worse than absent.
                if let Some(allowed) = &allowed_indexes
                    && !allowed.iter().any(|permitted| permitted == index)
                {
                    bail!(
                        "{origin}: index_overrides names '{index}', which is not in \
                         allowed_indexes. The key cannot reach that index at all, so the \
                         override protects nothing — add it to allowed_indexes, or remove it"
                    );
                }

                if index_overrides
                    .insert(index.to_string(), *override_role)
                    .is_some()
                {
                    bail!("{origin}: index_overrides names '{index}' twice");
                }
            }

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

            let tenant = match entry.tenant.as_deref().map(str::trim) {
                None => None,
                Some("") => bail!(
                    "{origin}: tenant is empty. Name the tenant, or remove the field so this \
                     key spends against no budget"
                ),
                Some(tenant) => Some(tenant.to_string()),
            };

            resolved.push(Arc::new(KeyEntry {
                digest,
                role,
                label,
                allowed_indexes,
                index_overrides,
                tenant,
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
    /// Per-index role subtraction; see [`ApiKeyConfig::index_overrides`]. Empty for the
    /// overwhelming majority of keys, so the lookup below is skipped entirely for them.
    index_overrides: std::collections::HashMap<String, Role>,
    /// The tenant whose quota this key spends; see [`ApiKeyConfig::tenant`].
    tenant: Option<String>,
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

    /// Whether this key holds `capability` **on `index`**.
    ///
    /// This is the question every index-naming route must ask, and [`KeyEntry::has`] is the
    /// question everything else asks. Asking `has` where an index is in play is the bug this
    /// method exists to prevent: it answers about the key's own role and cannot see a
    /// subtraction, so a `writer` restricted to read-only on one index would still write to it.
    ///
    /// Scope is a separate question — [`KeyEntry::allows_index`] — and is checked alongside
    /// this one rather than folded into it, so the two refusals stay distinguishable to the
    /// caller and in the log.
    pub fn has_on(&self, capability: Capability, index: &str) -> bool {
        match self.index_overrides.get(index) {
            Some(role) => role.has(capability),
            None => self.role.has(capability),
        }
    }

    /// The tenant this key spends against, if it was given one.
    pub fn tenant(&self) -> Option<&str> {
        self.tenant.as_deref()
    }

    /// The role this key holds on `index`, which is its own unless an override subtracts.
    pub fn role_on(&self, index: &str) -> Role {
        self.index_overrides
            .get(index)
            .copied()
            .unwrap_or(self.role)
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

/// The live key ring, and what re-resolving it takes.
///
/// One type rather than a swap cell for the gate plus reload inputs somewhere else, because
/// neither half is useful alone: the cell is what a reload swaps, and the inputs are what it
/// swaps in. `POST /_admin/keys/reload` and SIGHUP both land on [`Self::reload`].
pub struct KeyReloader {
    /// Read per request by the gate; written only by [`Self::reload`]. `ArcSwap` rather than
    /// a `RwLock` because the read side is the hot one: a request takes a snapshot and never
    /// waits on a reload in progress, and a reload never waits on a request.
    ring: arc_swap::ArcSwap<KeyRing>,
    /// The overrides startup parsed, re-applied on every reload — which is how an
    /// `--api-key-hash`/`CAMEODB_API_KEY_HASH` key survives the file being re-read.
    cli: crate::config::CliOverrides,
    /// The file startup read, resolved once at boot and pinned. See
    /// [`crate::config::CameoDbConfig::resolve_config_path`] for why the implicit search
    /// list is never re-walked.
    source: Option<PathBuf>,
    /// The `[security]` section as it stood at startup, so a reload can report the fields it
    /// cannot move — `tenants`, `limits`, `audit` and `implicit_index_creation` are bound
    /// into the node, the rate limiter and the audit sink before any request is served.
    startup: SecurityConfig,
}

impl KeyReloader {
    /// Build the handle startup authenticates against.
    ///
    /// `security` is the already-resolved `[security]` section; `load_keyring` cannot fail
    /// for a node that passed `validate()`, and a failure here propagates the same way
    /// startup's does.
    pub fn new(cli: crate::config::CliOverrides, security: SecurityConfig) -> Result<Self> {
        let source = crate::config::CameoDbConfig::resolve_config_path(cli.config_path.as_deref());
        let ring = security.load_keyring()?;
        Ok(Self {
            ring: arc_swap::ArcSwap::from_pointee(ring),
            cli,
            source,
            startup: security,
        })
    }

    /// The ring to decide a request against: a consistent snapshot, cheap to take.
    pub fn current(&self) -> Arc<KeyRing> {
        self.ring.load_full()
    }

    /// Re-resolve the configuration and swap the ring.
    ///
    /// Everything startup validates is re-validated — the same file, the same overrides, the
    /// same [`crate::config::CameoDbConfig::validate`] — so a config this node would refuse
    /// to boot with is refused here, with the previous ring still deciding requests. The one
    /// refusal startup cannot know about: a new ring that is `enabled` but grants
    /// `node-admin` to nobody. This endpoint and SIGHUP are the only ways back, and both
    /// need that capability — a reload that removed the last admin key could never be undone
    /// short of a restart.
    pub fn reload(&self) -> Result<KeyReloadReport> {
        let security = crate::config::CameoDbConfig::reload(&self.cli, self.source.as_deref())
            .context("key reload refused — the previous key ring is still in effect")?
            .security;
        let ring = security
            .load_keyring()
            .context("key reload refused — the previous key ring is still in effect")?;

        if ring.enabled() && !ring.holds(Capability::NodeAdmin) {
            bail!(
                "key reload refused: the new configuration grants node-admin to no key, which \
                 would leave /_admin/* unreachable until a restart. Add an admin key and \
                 reload again; the previous key ring is still in effect"
            );
        }

        // What the swap does not move. These are bound into the node, the rate limiter and
        // the audit sink at startup, so a changed value in the file changes nothing until a
        // restart — reported rather than left to surprise whoever changed it.
        let mut not_applied = Vec::new();
        if security.implicit_index_creation != self.startup.implicit_index_creation {
            not_applied.push("security.implicit_index_creation");
        }
        if security.tenants != self.startup.tenants {
            not_applied.push("security.tenants");
        }
        if security.limits != self.startup.limits {
            not_applied.push("security.limits");
        }
        if security.audit != self.startup.audit {
            not_applied.push("security.audit");
        }

        let was_enabled = self.ring.load().enabled();
        self.ring.store(Arc::new(ring));
        let ring = self.ring.load_full();

        // The same per-key line startup logs, so a reload is as auditable in the log as a
        // boot is.
        for entry in ring.entries() {
            info!(
                key_id = %entry.key_id(),
                label = %entry.label(),
                role = %entry.role(),
                indexes = %entry.scope_summary(),
                "🔑 API key loaded by reload"
            );
        }
        if ring.enabled() != was_enabled {
            warn!(
                enabled = ring.enabled(),
                "key reload changed whether requests are authenticated"
            );
        }
        if !not_applied.is_empty() {
            warn!(
                fields = ?not_applied,
                "key reload: [security] settings bound at startup changed on disk; they still \
                 run with their old values until a restart"
            );
        }

        Ok(KeyReloadReport {
            enabled: ring.enabled(),
            source: self.source.clone(),
            keys: ring
                .entries()
                .iter()
                .map(|entry| ReloadedKey {
                    key_id: entry.key_id(),
                    label: entry.label().to_string(),
                    role: entry.role().as_str(),
                    indexes: entry.scope_summary(),
                })
                .collect(),
            summary: ring.summary(),
            not_applied,
        })
    }

    /// A handle over a ready-made ring, for tests that have no config file to reload from.
    /// Calling `reload` on it resolves environment and flags over defaults — what a node
    /// that booted without a file would adopt.
    #[cfg(test)]
    pub fn for_test(ring: KeyRing) -> Arc<Self> {
        Arc::new(Self {
            ring: arc_swap::ArcSwap::from_pointee(ring),
            cli: crate::config::CliOverrides::default(),
            source: None,
            startup: SecurityConfig::default(),
        })
    }

    /// Swap the ring without re-resolving anything — the test hook `for_test` answers to.
    #[cfg(test)]
    pub fn swap_for_test(&self, ring: KeyRing) {
        self.ring.store(Arc::new(ring));
    }
}

/// What a key reload did — the body `POST /_admin/keys/reload` answers with.
#[derive(Debug, Serialize)]
pub struct KeyReloadReport {
    /// `security.enabled` as now enforced.
    pub enabled: bool,
    /// The file the ring was re-resolved from; `null` on a node configured by environment
    /// and flags alone.
    pub source: Option<PathBuf>,
    /// Every key now accepted — the same facts the startup banner logs.
    pub keys: Vec<ReloadedKey>,
    /// `3 keys (1 admin, 2 reader)`.
    pub summary: String,
    /// `[security]` fields the file changed but the running node did not adopt: they are
    /// bound at startup and take effect on restart.
    pub not_applied: Vec<&'static str>,
}

/// One accepted key, as the reload report renders it.
#[derive(Debug, Serialize)]
pub struct ReloadedKey {
    pub key_id: String,
    pub label: String,
    pub role: &'static str,
    /// `all indexes` or the allow-list, exactly as the startup banner renders it.
    pub indexes: String,
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
            // `keygen` mints a key, it does not configure one. Overrides and tenancy are
            // written by hand into the stanza afterwards, so the round-trip check below proves
            // the key, not a policy it does not yet carry.
            index_overrides: std::collections::HashMap::new(),
            tenant: None,
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

    /// A key carries its tenant through to the entry that quotas are charged against.
    ///
    /// Keys are the only place a tenant is named, so a tenant that does not survive
    /// `load_keyring` is a quota charged to nobody — which reads as "unlimited" rather than as
    /// a misconfiguration, and is the failure mode worth a test of its own.
    #[test]
    fn a_key_carries_its_tenant_and_a_blank_one_is_refused() {
        let with_tenant = ApiKeyConfig {
            key_hash: Some(KeyDigest::of_token("t").to_config_value()),
            role: Some(Role::Writer),
            label: Some("acme-writer".to_string()),
            tenant: Some("  acme  ".to_string()),
            ..blank_key()
        };
        let ring = SecurityConfig {
            enabled: true,
            api_keys: vec![with_tenant],
            ..Default::default()
        }
        .load_keyring()
        .expect("a tenanted key loads");
        assert_eq!(
            ring.entries()[0].tenant(),
            Some("acme"),
            "the tenant is trimmed, so trailing whitespace in a config cannot split one \
             tenant's budget in two"
        );

        // A key with no tenant spends against no budget, which is every key on an upgraded node.
        let untenanted = ApiKeyConfig {
            key_hash: Some(KeyDigest::of_token("u").to_config_value()),
            role: Some(Role::Writer),
            ..blank_key()
        };
        let ring = SecurityConfig {
            enabled: true,
            api_keys: vec![untenanted],
            ..Default::default()
        }
        .load_keyring()
        .expect("an untenanted key loads");
        assert_eq!(ring.entries()[0].tenant(), None);

        // An empty one is neither, so it is refused rather than read as either.
        let blank = ApiKeyConfig {
            key_hash: Some(KeyDigest::of_token("b").to_config_value()),
            role: Some(Role::Writer),
            tenant: Some("   ".to_string()),
            ..blank_key()
        };
        let err = SecurityConfig {
            enabled: true,
            api_keys: vec![blank],
            ..Default::default()
        }
        .load_keyring()
        .expect_err("a blank tenant must not load")
        .to_string();
        assert!(err.contains("tenant"), "{err}");
    }

    // ---- Live reload -------------------------------------------------------

    /// A config file with `[security] enabled` set and one `[[security.api_keys]]` stanza
    /// per (key, role). `profile = "local"` declares what the loopback bind would have
    /// inferred anyway, so a posture rule can never make the test about something else.
    fn security_toml(enabled: bool, keys: &[(&ApiKey, &str)]) -> String {
        let mut toml = format!("[node]\nprofile = \"local\"\n\n[security]\nenabled = {enabled}\n");
        for (key, role) in keys {
            toml.push_str(&format!(
                "\n[[security.api_keys]]\nkey_hash = \"{}\"\nrole = \"{role}\"\n",
                key.digest().to_config_value()
            ));
        }
        toml
    }

    /// Write `toml` as `cameodb.toml` in a fresh tempdir and return it with the overrides
    /// `--config` would have parsed into. The dir must outlive the reloader — the file is
    /// re-read on every reload.
    fn config_file(toml: &str) -> (tempfile::TempDir, crate::config::CliOverrides) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cameodb.toml");
        std::fs::write(&path, toml).unwrap();
        (dir, cli_for(&path, &[]))
    }

    fn rewrite_config(dir: &tempfile::TempDir, toml: &str) {
        std::fs::write(dir.path().join("cameodb.toml"), toml).unwrap();
    }

    fn cli_for(path: &std::path::Path, extra: &[&str]) -> crate::config::CliOverrides {
        let mut args = vec!["--config".to_string(), path.to_string_lossy().to_string()];
        args.extend(extra.iter().map(|arg| arg.to_string()));
        crate::config::CliOverrides::parse(args).unwrap()
    }

    /// Build the reloader the way `main` does: load through the overrides, then hand the
    /// resolved `[security]` section over.
    fn reloader_for(cli: &crate::config::CliOverrides) -> KeyReloader {
        let config = crate::config::CameoDbConfig::load_with_cli(cli).unwrap();
        KeyReloader::new(cli.clone(), config.security).unwrap()
    }

    /// The whole point of the endpoint: keys can be added and revoked while the node runs.
    #[test]
    fn a_reload_adopts_added_and_removed_keys() {
        let key_a = ApiKey::generate().unwrap();
        let key_b = ApiKey::generate().unwrap();
        let (dir, cli) = config_file(&security_toml(true, &[(&key_a, "admin")]));
        let keys = reloader_for(&cli);

        assert!(keys.current().authenticate(key_a.expose()).is_some());
        assert!(keys.current().authenticate(key_b.expose()).is_none());

        // A second key joins without a restart...
        rewrite_config(
            &dir,
            &security_toml(true, &[(&key_a, "admin"), (&key_b, "admin")]),
        );
        let report = keys.reload().unwrap();
        assert_eq!(report.keys.len(), 2);
        assert!(keys.current().authenticate(key_b.expose()).is_some());

        // ...and removing one revokes it: the next request it makes is refused.
        rewrite_config(&dir, &security_toml(true, &[(&key_b, "admin")]));
        let report = keys.reload().unwrap();
        assert_eq!(report.summary, "1 key (1 admin)");
        assert!(keys.current().authenticate(key_a.expose()).is_none());
        assert!(keys.current().authenticate(key_b.expose()).is_some());
    }

    /// A bad edit must not take the working ring down with it.
    #[test]
    fn a_reload_the_node_would_not_boot_is_refused_and_the_old_ring_stays() {
        let key_a = ApiKey::generate().unwrap();
        let (dir, cli) = config_file(&security_toml(true, &[(&key_a, "admin")]));
        let keys = reloader_for(&cli);

        // An entry without a role is a config `load_keyring` refuses — the same refusal a
        // boot would hit, surfaced at reload instead.
        rewrite_config(
            &dir,
            "[node]\nprofile = \"local\"\n\n[security]\nenabled = true\n\n\
             [[security.api_keys]]\nkey_hash = \"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"\n",
        );
        let err = keys.reload().unwrap_err().to_string();
        assert!(err.contains("key reload refused"), "{err}");
        assert!(keys.current().authenticate(key_a.expose()).is_some());

        // And the file being gone entirely is an error, not a slide onto defaults — that
        // would be a silent disabling of authentication.
        std::fs::remove_file(dir.path().join("cameodb.toml")).unwrap();
        let err = keys.reload().unwrap_err().to_string();
        assert!(err.contains("key reload refused"), "{err}");
        assert!(keys.current().authenticate(key_a.expose()).is_some());
    }

    /// The one refusal startup cannot make: `/_admin/keys/reload` needs a node-admin key to
    /// call, so a ring with none could never be fixed from the API — only a restart could.
    #[test]
    fn a_reload_that_drops_the_last_admin_key_is_refused() {
        let key_a = ApiKey::generate().unwrap();
        let key_b = ApiKey::generate().unwrap();
        let (dir, cli) = config_file(&security_toml(true, &[(&key_a, "admin")]));
        let keys = reloader_for(&cli);

        rewrite_config(&dir, &security_toml(true, &[(&key_b, "reader")]));
        let err = keys.reload().unwrap_err().to_string();
        assert!(err.contains("node-admin"), "{err}");
        assert!(keys.current().authenticate(key_a.expose()).is_some());
        assert!(keys.current().authenticate(key_b.expose()).is_none());
    }

    /// `--api-key-hash`/`CAMEODB_API_KEY_HASH` did not come from the file, so re-reading the
    /// file must not lose it.
    #[test]
    fn an_override_key_survives_a_reload() {
        let file_key = ApiKey::generate().unwrap();
        let override_key = ApiKey::generate().unwrap();
        let (dir, _cli) = config_file(&security_toml(true, &[(&file_key, "admin")]));
        let cli = cli_for(
            &dir.path().join("cameodb.toml"),
            &[
                "--api-key-hash",
                &override_key.digest().to_config_value(),
                "--api-key-role",
                "admin",
            ],
        );
        let keys = reloader_for(&cli);
        assert!(
            keys.current().authenticate(override_key.expose()).is_some(),
            "the override key authenticates at boot"
        );

        // Rewrite the file without it — nothing about the file can carry an override.
        rewrite_config(&dir, &security_toml(true, &[(&file_key, "admin")]));
        keys.reload().unwrap();
        assert!(
            keys.current().authenticate(override_key.expose()).is_some(),
            "the override key must survive a reload of the file"
        );
    }

    /// Sections the swap cannot move are named in the report rather than left to surprise
    /// the operator who changed them.
    #[test]
    fn startup_bound_security_fields_changed_on_disk_are_reported() {
        let key_a = ApiKey::generate().unwrap();
        let (dir, cli) = config_file(&security_toml(true, &[(&key_a, "admin")]));
        let keys = reloader_for(&cli);

        let mut toml = String::from(
            "[node]\nprofile = \"local\"\n\n[security]\nenabled = true\n\n\
             [security.limits]\nmax_search_limit = 500\n",
        );
        toml.push_str(&format!(
            "\n[[security.api_keys]]\nkey_hash = \"{}\"\nrole = \"admin\"\n",
            key_a.digest().to_config_value()
        ));
        rewrite_config(&dir, &toml);

        let report = keys.reload().unwrap();
        assert_eq!(report.not_applied, vec!["security.limits"]);
        // ...and the key material itself still moved.
        assert!(keys.current().authenticate(key_a.expose()).is_some());
    }

    /// `enabled` lives inside the ring, so flipping it is part of what a reload can do —
    /// including turning authentication on for a node that booted without it.
    #[test]
    fn enabled_moves_with_the_ring() {
        let key_a = ApiKey::generate().unwrap();
        let (dir, cli) = config_file(&security_toml(false, &[]));
        let keys = reloader_for(&cli);
        assert!(!keys.current().enabled());

        rewrite_config(&dir, &security_toml(true, &[(&key_a, "admin")]));
        let report = keys.reload().unwrap();
        assert!(report.enabled);
        assert!(keys.current().enabled());
        assert!(keys.current().authenticate(key_a.expose()).is_some());
    }

    /// Every optional field unset, so a test names only what it is about.
    fn blank_key() -> ApiKeyConfig {
        ApiKeyConfig {
            key_hash: None,
            key_hash_file: None,
            role: None,
            label: None,
            allowed_indexes: None,
            index_overrides: None,
            tenant: None,
        }
    }
}
