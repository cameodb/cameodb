//! Keys and tenants managed through the node: the files behind `/_keys` and `/_tenants`.
//!
//! The node manages two kinds of file, both where `[security.managed_files]` says. The `keys`
//! file holds keys with no tenant — users and agents scoped by their own `allowed_indexes`.
//! A tenant's file holds its `[security.tenants.<name>]` table and its keys. Each carries a
//! `[provisioning]` stamp that says the node wrote it.
//!
//! The file is the record: every change reads it, changes it, writes it to a temp name in the
//! same folder and renames it into place, then reloads the key ring through the same loader
//! startup uses. A change the loader refuses — a pattern it does not take, a scope overlapping
//! another tenant's, a key reaching past its tenant — is undone, file and ring both, and
//! answered with the loader's reason. So nothing the API writes can be a config the node would
//! refuse to boot with.
//!
//! Changes are serialised by one lock: a change is a read–change–write of a file, and two at
//! once would lose one. A file the node did not write, and a tenant defined anywhere else, are
//! the operator's and are never rewritten.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::auth::{
    ApiKey, ApiKeyConfig, KeyDigest, KeyReloader, ProvisioningStamp, Role, SecurityConfig,
    TenantQuota, valid_tenant_name,
};

/// Why a change was refused. Each maps to one HTTP status in the routes.
#[derive(Debug, thiserror::Error)]
pub enum TenantError {
    #[error(
        "{0} management is not configured on this node: name its file in \
         [security.managed_files], with an include that reads it"
    )]
    NotConfigured(&'static str),
    #[error(
        "'{0}' is not a tenant name the node can create: letters, digits, '.', '_' and '-', \
         starting with a letter or digit, at most 64 characters"
    )]
    InvalidName(String),
    #[error("no tenant '{0}'")]
    NotFound(String),
    #[error("no key '{key_id}' {place}")]
    KeyNotFound { place: String, key_id: String },
    #[error("tenant '{tenant}' is defined in {file}, which the node does not manage")]
    NotManaged { tenant: String, file: PathBuf },
    #[error("{0} exists and was not written by the node for this; it is never overwritten")]
    ForeignFile(PathBuf),
    #[error("{place} is at revision {current}, not {expected}")]
    RevisionMismatch {
        place: String,
        current: u64,
        expected: u64,
    },
    #[error("{0}")]
    Forbidden(String),
    #[error("refused: {0}")]
    Refused(String),
    #[error("{0:#}")]
    Io(anyhow::Error),
}

/// A tenant's settings as a caller writes them. On create, an omitted `allowed_indexes` is
/// `["{tenant}-*"]`; on update, an omitted field keeps its value.
#[derive(Debug, Default, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TenantSpec {
    pub max_indexes: Option<usize>,
    pub max_bytes: Option<u64>,
    pub allowed_indexes: Option<Vec<String>>,
}

/// A key as a caller asks for one. The tenant, if any, is the route's; the key itself is the
/// node's to generate.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KeySpec {
    pub role: Role,
    pub label: Option<String>,
    pub allowed_indexes: Option<Vec<String>>,
    pub index_overrides: Option<HashMap<String, Role>>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct TenantView {
    pub tenant: String,
    /// Whether the node manages this tenant's file. An unmanaged tenant is listed, never
    /// changed.
    pub managed: bool,
    /// `None` for an unmanaged tenant.
    pub revision: Option<u64>,
    pub max_indexes: usize,
    pub max_bytes: u64,
    pub allowed_indexes: Option<Vec<String>>,
    pub keys: Vec<KeyView>,
}

/// The keys file: every key the node manages with no tenant.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct KeysView {
    /// `0` before the first key is written.
    pub revision: u64,
    pub keys: Vec<KeyView>,
}

/// What a listing says about a key: never its digest, never the key.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct KeyView {
    pub key_id: String,
    pub label: String,
    pub role: Role,
    pub indexes: String,
}

/// The one response that carries a key.
#[derive(Debug, Serialize)]
pub struct MintedKey {
    pub key: String,
    pub key_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tenant: Option<String>,
    pub revision: u64,
}

/// A managed file as the node writes it: the keys file has no tenant table, a tenant's file
/// exactly one.
#[derive(Debug, Serialize, Deserialize)]
struct ManagedFile {
    provisioning: ProvisioningStamp,
    security: FileSecurity,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct FileSecurity {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    tenants: BTreeMap<String, TenantQuota>,
    #[serde(default)]
    api_keys: Vec<ApiKeyConfig>,
}

/// The same file read leniently, to tell the node's own file from someone else's before
/// trusting its shape.
#[derive(Deserialize)]
struct AnyFile {
    provisioning: Option<ProvisioningStamp>,
    #[serde(default)]
    security: FileSecurity,
}

/// Which managed file a change is to.
#[derive(Clone, Copy)]
enum Slot<'a> {
    Keys,
    Tenant(&'a str),
}

impl Slot<'_> {
    fn describe(self) -> String {
        match self {
            Slot::Keys => "the keys file".to_string(),
            Slot::Tenant(tenant) => format!("tenant '{tenant}'"),
        }
    }
}

pub struct TenantStore {
    keys: Arc<KeyReloader>,
    node_id: String,
    lock: tokio::sync::Mutex<()>,
}

impl TenantStore {
    pub fn new(keys: Arc<KeyReloader>, node_id: String) -> Self {
        Self {
            keys,
            node_id,
            lock: tokio::sync::Mutex::new(()),
        }
    }

    // ---- Keys with no tenant: /_keys

    /// The keys file's keys and revision.
    pub fn list_keys(&self) -> Result<KeysView, TenantError> {
        let (_, file) = self.open(Slot::Keys)?;
        Ok(self.keys_view(file.as_ref()))
    }

    /// Mint a key with no tenant, scoped by its own `allowed_indexes`. At most `writer`.
    pub async fn create_key(
        &self,
        spec: KeySpec,
        if_match: Option<u64>,
    ) -> Result<MintedKey, TenantError> {
        self.mint(Slot::Keys, spec, if_match).await
    }

    /// Revoke a key the keys file holds.
    pub async fn revoke_key(
        &self,
        key_id: &str,
        if_match: Option<u64>,
    ) -> Result<KeysView, TenantError> {
        let _guard = self.lock.lock().await;
        let (path, file) = self.open(Slot::Keys)?;
        let mut file = file.ok_or_else(|| TenantError::KeyNotFound {
            place: "in the keys file".to_string(),
            key_id: key_id.to_string(),
        })?;
        check_revision(Slot::Keys, Some(&file), if_match)?;
        remove_key(&mut file, key_id, "in the keys file")?;
        let revision = self.commit(&path, Slot::Keys, file)?;
        let (_, file) = self.open(Slot::Keys)?;
        let mut view = self.keys_view(file.as_ref());
        view.revision = revision;
        Ok(view)
    }

    // ---- Tenants: /_tenants

    /// Every tenant on the node that `visible` admits, managed or not.
    pub fn list(&self, visible: impl Fn(&str) -> bool) -> Vec<TenantView> {
        let security = self.keys.security();
        let mut names: Vec<&String> = security.tenants.keys().filter(|t| visible(t)).collect();
        names.sort();
        names
            .into_iter()
            .map(|tenant| self.view(&security, tenant))
            .collect()
    }

    pub fn get(&self, tenant: &str) -> Result<TenantView, TenantError> {
        let security = self.keys.security();
        if !security.tenants.contains_key(tenant) {
            return Err(TenantError::NotFound(tenant.to_string()));
        }
        Ok(self.view(&security, tenant))
    }

    /// Create a tenant, or change its settings.
    pub async fn put_tenant(
        &self,
        tenant: &str,
        spec: TenantSpec,
        if_match: Option<u64>,
    ) -> Result<TenantView, TenantError> {
        let _guard = self.lock.lock().await;
        let slot = Slot::Tenant(tenant);
        let (path, existing) = self.open(slot)?;
        check_revision(slot, existing.as_ref(), if_match)?;

        let mut file = existing.unwrap_or_else(|| ManagedFile {
            provisioning: self.stamp(0),
            security: FileSecurity {
                tenants: [(
                    tenant.to_string(),
                    TenantQuota {
                        allowed_indexes: Some(vec![format!("{tenant}-*")]),
                        ..Default::default()
                    },
                )]
                .into(),
                api_keys: Vec::new(),
            },
        });
        let quota = file
            .security
            .tenants
            .get_mut(tenant)
            .expect("an opened tenant file holds its tenant");
        if let Some(max) = spec.max_indexes {
            quota.max_indexes = max;
        }
        if let Some(max) = spec.max_bytes {
            quota.max_bytes = max;
        }
        if let Some(indexes) = spec.allowed_indexes {
            quota.allowed_indexes = Some(indexes);
        }
        self.commit(&path, slot, file)?;
        self.get(tenant)
    }

    /// Remove a tenant and every key it has. Whether it still owns indexes is the caller's
    /// question to ask first: the store knows files, not indexes.
    pub async fn delete_tenant(
        &self,
        tenant: &str,
        if_match: Option<u64>,
    ) -> Result<(), TenantError> {
        let _guard = self.lock.lock().await;
        let slot = Slot::Tenant(tenant);
        let (path, existing) = self.open(slot)?;
        if existing.is_none() {
            return Err(TenantError::NotFound(tenant.to_string()));
        }
        check_revision(slot, existing.as_ref(), if_match)?;

        let previous = std::fs::read_to_string(&path).map_err(|e| TenantError::Io(e.into()))?;
        std::fs::remove_file(&path).map_err(|e| TenantError::Io(e.into()))?;
        if let Err(err) = self.keys.reload() {
            write_atomically(&path, &previous).map_err(TenantError::Io)?;
            let _ = self.keys.reload();
            return Err(TenantError::Refused(format!("{err:#}")));
        }
        Ok(())
    }

    /// Mint a key for a tenant that exists. At most `writer`.
    pub async fn create_tenant_key(
        &self,
        tenant: &str,
        spec: KeySpec,
        if_match: Option<u64>,
    ) -> Result<MintedKey, TenantError> {
        self.mint(Slot::Tenant(tenant), spec, if_match).await
    }

    /// Revoke one of a tenant's keys. It fails from the next request on.
    pub async fn revoke_tenant_key(
        &self,
        tenant: &str,
        key_id: &str,
        if_match: Option<u64>,
    ) -> Result<TenantView, TenantError> {
        let _guard = self.lock.lock().await;
        let slot = Slot::Tenant(tenant);
        let (path, existing) = self.open(slot)?;
        let Some(mut file) = existing else {
            return Err(TenantError::NotFound(tenant.to_string()));
        };
        check_revision(slot, Some(&file), if_match)?;
        remove_key(&mut file, key_id, &format!("in tenant '{tenant}'"))?;
        self.commit(&path, slot, file)?;
        self.get(tenant)
    }

    // ---- Shared

    /// Mint a key into `slot`: the keys file, created on the first key, or a tenant's file,
    /// which must exist. At most `writer`: a key the API mints can never manage keys or the
    /// node.
    async fn mint(
        &self,
        slot: Slot<'_>,
        spec: KeySpec,
        if_match: Option<u64>,
    ) -> Result<MintedKey, TenantError> {
        if !matches!(spec.role, Role::Writer | Role::Reader) {
            return Err(TenantError::Forbidden(format!(
                "the API mints writer and reader keys only, not '{}'",
                spec.role
            )));
        }
        // Checked before anything is written, so the caller hears the pattern's own fault
        // rather than the loader's account of a file it then had to undo.
        if let Some(list) = &spec.allowed_indexes {
            crate::auth::IndexScope::parse(list).map_err(TenantError::Refused)?;
        }
        let _guard = self.lock.lock().await;
        let (path, existing) = self.open(slot)?;
        check_revision(slot, existing.as_ref(), if_match)?;
        let tenant = match slot {
            Slot::Keys => None,
            Slot::Tenant(tenant) => Some(tenant.to_string()),
        };
        let mut file = match (existing, &tenant) {
            (Some(file), _) => file,
            (None, None) => ManagedFile {
                provisioning: self.stamp(0),
                security: FileSecurity::default(),
            },
            (None, Some(tenant)) => return Err(TenantError::NotFound(tenant.clone())),
        };

        let key = ApiKey::generate().map_err(TenantError::Io)?;
        let digest = key.digest();
        file.security.api_keys.push(ApiKeyConfig {
            key_hash: Some(digest.to_config_value()),
            role: Some(spec.role),
            label: spec.label,
            allowed_indexes: spec.allowed_indexes,
            index_overrides: spec.index_overrides,
            tenant: tenant.clone(),
            ..Default::default()
        });
        let revision = self.commit(&path, slot, file)?;
        Ok(MintedKey {
            key: key.expose().to_string(),
            key_id: digest.key_id(),
            tenant,
            revision,
        })
    }

    /// The path of `slot`'s file, and the file if the node has written one. Refuses a slot
    /// that is not configured, a tenant name the node cannot create, a tenant the operator
    /// defined, and a file at the path that is not the node's for this slot.
    fn open(&self, slot: Slot<'_>) -> Result<(PathBuf, Option<ManagedFile>), TenantError> {
        let security = self.keys.security();
        let files = security.managed_files.as_ref();
        let path = match slot {
            Slot::Keys => files
                .and_then(|files| files.keys_path())
                .ok_or(TenantError::NotConfigured("key"))?,
            Slot::Tenant(tenant) => {
                let path = files
                    .and_then(|files| files.tenant_path(tenant))
                    .ok_or(TenantError::NotConfigured("tenant"))?;
                if !valid_tenant_name(tenant) {
                    return Err(TenantError::InvalidName(tenant.to_string()));
                }
                if let Some(source) = security.tenant_sources.get(tenant)
                    && source.provisioning.is_none()
                {
                    return Err(TenantError::NotManaged {
                        tenant: tenant.to_string(),
                        file: source.file.clone(),
                    });
                }
                // `{tenant}.toml` beside `keys.toml` would make the tenant `keys` the keys file.
                if files.and_then(|files| files.keys_path()).as_deref() == Some(path.as_path()) {
                    return Err(TenantError::ForeignFile(path));
                }
                path
            }
        };

        let content = match std::fs::read_to_string(&path) {
            Ok(content) => content,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok((path, None)),
            Err(err) => return Err(TenantError::Io(err.into())),
        };
        let any: AnyFile =
            toml::from_str(&content).map_err(|_| TenantError::ForeignFile(path.clone()))?;
        let ours = any.provisioning.is_some()
            && match slot {
                Slot::Keys => {
                    any.security.tenants.is_empty()
                        && any.security.api_keys.iter().all(|key| key.tenant.is_none())
                }
                Slot::Tenant(tenant) => {
                    any.security.tenants.len() == 1 && any.security.tenants.contains_key(tenant)
                }
            };
        if !ours {
            return Err(TenantError::ForeignFile(path));
        }
        let file = ManagedFile {
            provisioning: any.provisioning.expect("checked above"),
            security: any.security,
        };
        Ok((path, Some(file)))
    }

    /// Write `file` as `slot`'s next revision and reload; undo both if the loader refuses it.
    /// Returns the revision written.
    fn commit(
        &self,
        path: &Path,
        slot: Slot<'_>,
        mut file: ManagedFile,
    ) -> Result<u64, TenantError> {
        let previous = match std::fs::read_to_string(path) {
            Ok(content) => Some(content),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
            Err(err) => return Err(TenantError::Io(err.into())),
        };

        file.provisioning = self.stamp(file.provisioning.revision + 1);
        let revision = file.provisioning.revision;
        let body = toml::to_string(&file).map_err(|e| TenantError::Io(e.into()))?;
        let route = match slot {
            Slot::Keys => "/_keys".to_string(),
            Slot::Tenant(tenant) => format!("/_tenants/{tenant}"),
        };
        let content = format!(
            "# Written by CameoDB: {}. Change it through {route}: the node rewrites\n\
             # this file on every change, and a hand edit lasts until the next one.\n\n{body}",
            slot.describe()
        );
        write_atomically(path, &content).map_err(TenantError::Io)?;

        if let Err(err) = self.keys.reload() {
            let restored = match &previous {
                Some(previous) => write_atomically(path, previous),
                None => std::fs::remove_file(path).map_err(Into::into),
            };
            if let Err(restore) = restored {
                tracing::error!(
                    path = %path.display(),
                    "a refused key change could not be undone: {restore:#}"
                );
            }
            let _ = self.keys.reload();
            return Err(TenantError::Refused(format!("{err:#}")));
        }
        Ok(revision)
    }

    fn stamp(&self, revision: u64) -> ProvisioningStamp {
        ProvisioningStamp {
            revision,
            written_by: self.node_id.clone(),
            written_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        }
    }

    /// The keys of a file as the loaded ring holds them, in the file's order.
    fn key_views(&self, file: Option<&ManagedFile>) -> Vec<KeyView> {
        let ids: Vec<String> = file
            .iter()
            .flat_map(|file| &file.security.api_keys)
            .filter_map(|entry| entry.key_hash.as_deref())
            .filter_map(|hash| KeyDigest::parse(hash).ok())
            .map(|digest| digest.key_id())
            .collect();
        let wanted: HashSet<&String> = ids.iter().collect();
        let ring = self.keys.current();
        let mut views: Vec<KeyView> = ring
            .entries()
            .iter()
            .filter(|entry| wanted.contains(&entry.key_id()))
            .map(|entry| key_view(entry))
            .collect();
        views.sort_by_key(|view| ids.iter().position(|id| *id == view.key_id));
        views
    }

    fn keys_view(&self, file: Option<&ManagedFile>) -> KeysView {
        KeysView {
            revision: file.map_or(0, |file| file.provisioning.revision),
            keys: self.key_views(file),
        }
    }

    fn view(&self, security: &SecurityConfig, tenant: &str) -> TenantView {
        let quota = security.tenants.get(tenant).cloned().unwrap_or_default();
        let source = security.tenant_sources.get(tenant);
        let revision = source
            .and_then(|source| source.provisioning.as_ref())
            .map(|stamp| stamp.revision);
        let keys = self
            .keys
            .current()
            .entries()
            .iter()
            .filter(|entry| entry.tenant() == Some(tenant))
            .map(|entry| key_view(entry))
            .collect();
        TenantView {
            tenant: tenant.to_string(),
            managed: revision.is_some(),
            revision,
            max_indexes: quota.max_indexes,
            max_bytes: quota.max_bytes,
            allowed_indexes: quota.allowed_indexes,
            keys,
        }
    }
}

fn key_view(entry: &crate::auth::KeyEntry) -> KeyView {
    KeyView {
        key_id: entry.key_id(),
        label: entry.label().to_string(),
        role: entry.role(),
        indexes: entry.scope_summary(),
    }
}

/// Drop the key with `key_id` from `file`, or say it was not there.
fn remove_key(file: &mut ManagedFile, key_id: &str, place: &str) -> Result<(), TenantError> {
    let before = file.security.api_keys.len();
    file.security.api_keys.retain(|entry| {
        entry
            .key_hash
            .as_deref()
            .and_then(|hash| KeyDigest::parse(hash).ok())
            .is_none_or(|digest| digest.key_id() != key_id)
    });
    if file.security.api_keys.len() == before {
        return Err(TenantError::KeyNotFound {
            place: place.to_string(),
            key_id: key_id.to_string(),
        });
    }
    Ok(())
}

fn check_revision(
    slot: Slot<'_>,
    file: Option<&ManagedFile>,
    if_match: Option<u64>,
) -> Result<(), TenantError> {
    let Some(expected) = if_match else {
        return Ok(());
    };
    let current = file.map_or(0, |file| file.provisioning.revision);
    if current != expected {
        return Err(TenantError::RevisionMismatch {
            place: slot.describe(),
            current,
            expected,
        });
    }
    Ok(())
}

/// Write `content` to a dot-named temp file beside `path`, flush it, and rename it over `path`,
/// so a reader — the loader, the key watcher — sees the old file or the new one, never half of
/// one. The folder is flushed too where the platform allows, so the rename survives a crash.
fn write_atomically(path: &Path, content: &str) -> anyhow::Result<()> {
    use anyhow::Context;
    use std::io::Write;

    let dir = path.parent().context("a key file has a folder")?;
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .context("a key file has a name")?;
    let temp = dir.join(format!(".{name}.tmp"));
    // Owner-only, like the key files `keygen` writes: the digests are not secrets, but nothing
    // outside the service has a reason to read them.
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    let mut file = options
        .open(&temp)
        .with_context(|| format!("cannot write {}", temp.display()))?;
    // `mode` above applies only to a file it creates; a temp left by a crashed write keeps its own.
    #[cfg(unix)]
    file.set_permissions(std::os::unix::fs::PermissionsExt::from_mode(0o600))?;
    file.write_all(content.as_bytes())?;
    file.sync_all()?;
    drop(file);
    std::fs::rename(&temp, path)
        .with_context(|| format!("cannot move {} into place", temp.display()))?;
    #[cfg(unix)]
    if let Ok(dir) = std::fs::File::open(dir) {
        let _ = dir.sync_all();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn a_written_file_is_owner_only() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("keys.toml");
        // A leftover temp file from a crashed write, with looser permissions.
        let leftover = dir.path().join(".keys.toml.tmp");
        std::fs::write(&leftover, "x").unwrap();
        std::fs::set_permissions(&leftover, std::fs::Permissions::from_mode(0o644)).unwrap();
        write_atomically(&path, "a = 1\n").unwrap();
        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
    }

    struct Node {
        _dir: tempfile::TempDir,
        root: PathBuf,
        keys: Arc<KeyReloader>,
        store: TenantStore,
        admin: ApiKey,
    }

    /// A node configured for tenant files in `keys.d`, named `name`, with one admin key in the
    /// main file and `extra` appended to it.
    fn node(name: &str, extra: &str) -> Node {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().canonicalize().unwrap();
        std::fs::create_dir(root.join("keys.d")).unwrap();
        let admin = ApiKey::generate().unwrap();
        let config = root.join("cameodb.toml");
        std::fs::write(
            &config,
            format!(
                "include = [\"keys.d/*.toml\"]\n[node]\nprofile = \"local\"\n\
                 [security]\nenabled = true\n\
                 [security.managed_files]\ndir = \"keys.d\"\nkeys = \"keys.toml\"\n\
                 tenant = \"{name}\"\n\
                 [[security.api_keys]]\nkey_hash = \"{}\"\nrole = \"admin\"\n{extra}",
                admin.digest().to_config_value()
            ),
        )
        .unwrap();
        let cli = crate::config::CliOverrides::parse([
            "--config".to_string(),
            config.to_string_lossy().to_string(),
        ])
        .unwrap();
        let security = crate::config::CameoDbConfig::load_with_cli(&cli)
            .unwrap()
            .security;
        let keys = Arc::new(KeyReloader::new(cli, security).unwrap());
        let store = TenantStore::new(Arc::clone(&keys), "node-1".to_string());
        Node {
            _dir: dir,
            root,
            keys,
            store,
            admin,
        }
    }

    /// The first provisioning setup: no tenants anywhere, only the keys file.
    fn keys_only_node() -> Node {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().canonicalize().unwrap();
        std::fs::create_dir(root.join("keys.d")).unwrap();
        let admin = ApiKey::generate().unwrap();
        std::fs::write(
            root.join("cameodb.toml"),
            format!(
                "include = [\"keys.d/*.toml\"]\n[node]\nprofile = \"local\"\n\
                 [security]\nenabled = true\n\
                 [security.managed_files]\ndir = \"keys.d\"\nkeys = \"keys.toml\"\n\
                 [[security.api_keys]]\nkey_hash = \"{}\"\nrole = \"admin\"\n",
                admin.digest().to_config_value()
            ),
        )
        .unwrap();
        let keys = Arc::new(reloader(&root));
        let store = TenantStore::new(Arc::clone(&keys), "node-1".to_string());
        Node {
            _dir: dir,
            root,
            keys,
            store,
            admin,
        }
    }

    /// A key reloader over `root/cameodb.toml`, as a node starting there builds it.
    fn reloader(root: &Path) -> KeyReloader {
        let cli = crate::config::CliOverrides::parse([
            "--config".to_string(),
            root.join("cameodb.toml").to_string_lossy().to_string(),
        ])
        .unwrap();
        let security = crate::config::CameoDbConfig::load_with_cli(&cli)
            .unwrap()
            .security;
        KeyReloader::new(cli, security).unwrap()
    }

    fn user(label: &str, role: Role, indexes: Option<&[&str]>) -> KeySpec {
        KeySpec {
            role,
            label: Some(label.to_string()),
            allowed_indexes: indexes.map(|list| list.iter().map(|i| i.to_string()).collect()),
            index_overrides: None,
        }
    }

    /// The portal's whole first job: users and agents, each a key scoped by its own list, with
    /// no tenant in the files or in the keys.
    #[tokio::test]
    async fn without_tenants_users_and_agents_get_keys_scoped_by_their_own_lists() {
        let node = keys_only_node();
        let jane = node
            .store
            .create_key(
                user("jane@company.com", Role::Writer, Some(&["docs-*"])),
                Some(0),
            )
            .await
            .unwrap();
        let agent = node
            .store
            .create_key(
                user("agent:search", Role::Reader, Some(&["docs-public", "wiki"])),
                Some(1),
            )
            .await
            .unwrap();
        assert_eq!((jane.revision, agent.revision), (1, 2));
        assert_eq!(jane.tenant, None);

        let ring = node.keys.current();
        let jane_entry = ring.authenticate(&jane.key).unwrap();
        assert_eq!(jane_entry.tenant(), None);
        assert_eq!(jane_entry.role(), Role::Writer);
        assert!(jane_entry.allows_index("docs-2026"));
        assert!(!jane_entry.allows_index("wiki"));
        let agent_entry = ring.authenticate(&agent.key).unwrap();
        assert!(agent_entry.allows_index("docs-public"));
        assert!(agent_entry.allows_index("wiki"));
        assert!(!agent_entry.allows_index("docs-private"));
        assert!(!agent_entry.has(crate::auth::Capability::Write));

        // No tenant anywhere: none in the config, none to meter.
        assert!(node.keys.security().tenants.is_empty());
        assert!(node.keys.tenant_limits().load().is_empty());
        assert!(node.store.list(|_| true).is_empty());

        let view = node.store.list_keys().unwrap();
        assert_eq!(view.revision, 2);
        let labels: Vec<&str> = view.keys.iter().map(|k| k.label.as_str()).collect();
        assert_eq!(labels, ["jane@company.com", "agent:search"]);
        assert_eq!(view.keys[0].indexes, "docs-*");

        // The file holds digests, labels and scopes: no tenant table, no key.
        let file = std::fs::read_to_string(node.root.join("keys.d/keys.toml")).unwrap();
        assert!(!file.contains("tenant"), "{file}");
        assert!(!file.contains(&jane.key) && !file.contains(&agent.key));
        assert!(file.contains("label = \"jane@company.com\""), "{file}");
    }

    /// A key with no list reaches every index, as an operator's key with no list always has.
    #[tokio::test]
    async fn without_tenants_a_key_with_no_list_reaches_every_index() {
        let node = keys_only_node();
        let minted = node
            .store
            .create_key(user("ops-bot", Role::Reader, None), None)
            .await
            .unwrap();
        let entry = node.keys.current().authenticate(&minted.key).unwrap();
        assert!(entry.allows_index("anything"));
        assert!(!entry.is_index_scoped());
    }

    /// The keys file is the record: a node started on it later accepts the same keys.
    #[tokio::test]
    async fn keys_minted_without_tenants_survive_a_restart() {
        let node = keys_only_node();
        let minted = node
            .store
            .create_key(
                user("jane@company.com", Role::Writer, Some(&["docs-*"])),
                None,
            )
            .await
            .unwrap();
        let restarted = reloader(&node.root);
        let entry = restarted.current().authenticate(&minted.key).unwrap();
        assert_eq!(entry.label(), "jane@company.com");
        assert!(entry.allows_index("docs-1"));
        assert!(
            restarted
                .current()
                .authenticate(node.admin.expose())
                .is_some()
        );
    }

    #[tokio::test]
    async fn without_tenants_a_revoked_key_fails_and_the_rest_stay() {
        let node = keys_only_node();
        let jane = node
            .store
            .create_key(user("jane@company.com", Role::Writer, None), None)
            .await
            .unwrap();
        let bob = node
            .store
            .create_key(user("bob@company.com", Role::Reader, None), None)
            .await
            .unwrap();
        let view = node.store.revoke_key(&jane.key_id, Some(2)).await.unwrap();
        assert_eq!(view.revision, 3);
        assert_eq!(view.keys.len(), 1);
        assert!(node.keys.current().authenticate(&jane.key).is_none());
        assert!(node.keys.current().authenticate(&bob.key).is_some());

        let err = node.store.revoke_key(&jane.key_id, None).await.unwrap_err();
        assert!(matches!(err, TenantError::KeyNotFound { .. }), "{err}");
    }

    #[tokio::test]
    async fn without_tenants_the_api_refuses_what_the_loader_would_and_undoes_it() {
        let node = keys_only_node();
        // The first key, with a pattern the loader refuses: no file is left behind.
        let err = node
            .store
            .create_key(user("x", Role::Reader, Some(&["*"])), None)
            .await
            .unwrap_err();
        assert!(
            matches!(err, TenantError::Refused(ref m) if m.starts_with("'*' alone")),
            "{err}"
        );
        assert!(!node.root.join("keys.d/keys.toml").exists());

        // A later one: the file is as it was.
        node.store
            .create_key(user("jane", Role::Reader, Some(&["docs"])), None)
            .await
            .unwrap();
        let before = std::fs::read_to_string(node.root.join("keys.d/keys.toml")).unwrap();
        let err = node
            .store
            .create_key(user("x", Role::Reader, Some(&["do*cs"])), None)
            .await
            .unwrap_err();
        assert!(matches!(err, TenantError::Refused(_)), "{err}");
        assert_eq!(
            std::fs::read_to_string(node.root.join("keys.d/keys.toml")).unwrap(),
            before
        );
        assert_eq!(node.store.list_keys().unwrap().keys.len(), 1);

        for role in [Role::Admin, Role::Provisioner] {
            let err = node
                .store
                .create_key(user("x", role, None), None)
                .await
                .unwrap_err();
            assert!(matches!(err, TenantError::Forbidden(_)), "{err}");
        }
        let err = node
            .store
            .create_key(user("x", Role::Reader, None), Some(5))
            .await
            .unwrap_err();
        assert!(
            matches!(err, TenantError::RevisionMismatch { current: 1, .. }),
            "{err}"
        );
    }

    /// A keys file an operator wrote is theirs, as a tenant they defined is.
    #[tokio::test]
    async fn a_hand_written_keys_file_is_never_rewritten() {
        let node = keys_only_node();
        let path = node.root.join("keys.d/keys.toml");
        let hand = format!(
            "[[security.api_keys]]\nkey_hash = \"{}\"\nrole = \"reader\"\n",
            ApiKey::generate().unwrap().digest().to_config_value()
        );
        std::fs::write(&path, &hand).unwrap();
        node.keys.reload().unwrap();
        let err = node
            .store
            .create_key(user("x", Role::Reader, None), None)
            .await
            .unwrap_err();
        assert!(matches!(err, TenantError::ForeignFile(_)), "{err}");
        assert_eq!(std::fs::read_to_string(&path).unwrap(), hand);
    }

    /// With `{tenant}.toml` beside `keys.toml`, a tenant called `keys` would be the keys file.
    #[tokio::test]
    async fn a_tenant_cannot_take_the_keys_files_name() {
        let node = node("{tenant}.toml", "");
        let err = node
            .store
            .put_tenant("keys", TenantSpec::default(), None)
            .await
            .unwrap_err();
        assert!(matches!(err, TenantError::ForeignFile(_)), "{err}");
    }

    fn scoped_tenant(indexes: &[&str]) -> TenantSpec {
        TenantSpec {
            allowed_indexes: Some(indexes.iter().map(|i| i.to_string()).collect()),
            ..Default::default()
        }
    }

    /// One node serving a company as a tenant and individuals with no tenant. The company's
    /// indexes are listed in its tenant; an individual reaches one of them only when the
    /// portal shares it by naming it in that individual's list.
    #[tokio::test]
    async fn a_company_tenant_and_individuals_share_a_node() {
        let node = node("{tenant}.toml", "");
        node.store
            .put_tenant("acme", scoped_tenant(&["acme-docs", "acme-shared"]), None)
            .await
            .unwrap();
        let acme_staff = node
            .store
            .create_tenant_key("acme", user("ana@acme.com", Role::Writer, None), None)
            .await
            .unwrap();
        let ivan = node
            .store
            .create_key(user("ivan@mail.com", Role::Writer, Some(&["ivan-*"])), None)
            .await
            .unwrap();
        let eve = node
            .store
            .create_key(
                user(
                    "eve@mail.com",
                    Role::Reader,
                    Some(&["eve-notes", "acme-shared"]),
                ),
                None,
            )
            .await
            .unwrap();

        let ring = node.keys.current();
        let ana = ring.authenticate(&acme_staff.key).unwrap();
        assert_eq!(ana.tenant(), Some("acme"));
        assert!(ana.allows_index("acme-docs") && ana.allows_index("acme-shared"));
        assert!(!ana.allows_index("ivan-notes") && !ana.allows_index("eve-notes"));

        let ivan = ring.authenticate(&ivan.key).unwrap();
        assert_eq!(ivan.tenant(), None);
        assert!(ivan.allows_index("ivan-notes"));
        assert!(!ivan.allows_index("acme-docs") && !ivan.allows_index("eve-notes"));

        // Shared: Eve reads the one acme index the portal gave her, and no other.
        let eve = ring.authenticate(&eve.key).unwrap();
        assert!(eve.allows_index("acme-shared") && eve.allows_index("eve-notes"));
        assert!(!eve.allows_index("acme-docs"));

        // Each listing holds its own: the tenant its staff, the keys file its individuals.
        let tenant = node.store.get("acme").unwrap();
        assert_eq!(tenant.keys.len(), 1);
        assert_eq!(tenant.keys[0].label, "ana@acme.com");
        let individuals = node.store.list_keys().unwrap();
        let labels: Vec<&str> = individuals.keys.iter().map(|k| k.label.as_str()).collect();
        assert_eq!(labels, ["ivan@mail.com", "eve@mail.com"]);

        // Two files, two revisions, two quotas' worth of state: only acme has a ceiling entry.
        assert_eq!((tenant.revision, individuals.revision), (Some(2), 2));
        assert_eq!(node.keys.tenant_limits().load().len(), 1);
    }

    /// Changing one side leaves the other alone, in the ring and on disk.
    #[tokio::test]
    async fn tenant_and_individual_changes_do_not_touch_each_other() {
        let node = node("{tenant}.toml", "");
        node.store
            .put_tenant("acme", scoped_tenant(&["acme-*"]), None)
            .await
            .unwrap();
        let ana = node
            .store
            .create_tenant_key("acme", user("ana@acme.com", Role::Writer, None), None)
            .await
            .unwrap();
        let ivan = node
            .store
            .create_key(user("ivan@mail.com", Role::Writer, Some(&["ivan-*"])), None)
            .await
            .unwrap();
        let tenant_file = node.root.join("keys.d/acme.toml");
        let acme_before = std::fs::read_to_string(&tenant_file).unwrap();

        node.store.revoke_key(&ivan.key_id, None).await.unwrap();
        assert_eq!(std::fs::read_to_string(&tenant_file).unwrap(), acme_before);
        assert!(node.keys.current().authenticate(&ana.key).is_some());

        let eve = node
            .store
            .create_key(user("eve@mail.com", Role::Reader, Some(&["eve-*"])), None)
            .await
            .unwrap();
        let keys_before = std::fs::read_to_string(node.root.join("keys.d/keys.toml")).unwrap();
        node.store
            .revoke_tenant_key("acme", &ana.key_id, None)
            .await
            .unwrap();
        node.store
            .put_tenant(
                "acme",
                TenantSpec {
                    max_indexes: Some(4),
                    ..Default::default()
                },
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            std::fs::read_to_string(node.root.join("keys.d/keys.toml")).unwrap(),
            keys_before
        );
        assert!(node.keys.current().authenticate(&eve.key).is_some());
        assert!(node.keys.current().authenticate(&ana.key).is_none());

        // And a node started on the same files later has exactly this.
        let restarted = reloader(&node.root);
        assert!(restarted.current().authenticate(&eve.key).is_some());
        assert!(restarted.current().authenticate(&ana.key).is_none());
        assert_eq!(restarted.tenant_limits().load()["acme"].max_indexes, 4);
    }

    /// What the node leaves to the portal, pinned so a change to it is a decision: a tenant's
    /// scope bounds the tenant's keys and reserves nothing from anyone else. An individual with
    /// no list reaches a tenant's indexes, one listing them is a share, and a tenant may be
    /// scoped over names an individual uses. Keeping them apart is the portal's to do.
    #[tokio::test]
    async fn tenant_scopes_bound_tenant_keys_and_reserve_nothing_from_individuals() {
        let node = node("{tenant}.toml", "");
        node.store
            .put_tenant("acme", scoped_tenant(&["acme-*"]), None)
            .await
            .unwrap();
        let unlisted = node
            .store
            .create_key(user("ops-bot", Role::Reader, None), None)
            .await
            .unwrap();
        let sharing = node
            .store
            .create_key(user("eve@mail.com", Role::Reader, Some(&["acme-*"])), None)
            .await
            .unwrap();
        let ring = node.keys.current();
        assert!(
            ring.authenticate(&unlisted.key)
                .unwrap()
                .allows_index("acme-docs")
        );
        assert!(
            ring.authenticate(&sharing.key)
                .unwrap()
                .allows_index("acme-docs")
        );

        node.store
            .put_tenant("beta", scoped_tenant(&["eve-*"]), None)
            .await
            .unwrap();
        let beta = node
            .store
            .create_tenant_key("beta", user("bo@beta.com", Role::Reader, None), None)
            .await
            .unwrap();
        assert!(
            node.keys
                .current()
                .authenticate(&beta.key)
                .unwrap()
                .allows_index("eve-notes")
        );
    }

    fn writer() -> KeySpec {
        KeySpec {
            role: Role::Writer,
            label: Some("jane@acme.com".to_string()),
            allowed_indexes: None,
            index_overrides: None,
        }
    }

    #[tokio::test]
    async fn a_tenant_and_its_key_are_written_loaded_and_revoked() {
        let node = node("{tenant}.toml", "");
        let view = node
            .store
            .put_tenant(
                "acme",
                TenantSpec {
                    max_indexes: Some(5),
                    ..Default::default()
                },
                None,
            )
            .await
            .unwrap();
        assert_eq!(view.revision, Some(1));
        assert!(view.managed);
        assert_eq!(view.allowed_indexes, Some(vec!["acme-*".to_string()]));
        assert_eq!(node.keys.tenant_limits().load()["acme"].max_indexes, 5);

        let minted = node
            .store
            .create_tenant_key("acme", writer(), Some(1))
            .await
            .unwrap();
        assert_eq!(minted.revision, 2);
        let entry = node.keys.current().authenticate(&minted.key).unwrap();
        assert_eq!(entry.tenant(), Some("acme"));
        assert!(entry.allows_index("acme-docs"));
        assert!(!entry.allows_index("beta-docs"));

        // The file holds the digest and the stamp, never the key.
        let file = std::fs::read_to_string(node.root.join("keys.d/acme.toml")).unwrap();
        assert!(!file.contains(&minted.key));
        assert!(file.contains("revision = 2"));
        assert!(file.contains("written_by = \"node-1\""));

        let view = node.store.get("acme").unwrap();
        assert_eq!(view.keys.len(), 1);
        assert_eq!(view.keys[0].key_id, minted.key_id);
        assert_eq!(view.keys[0].label, "jane@acme.com");

        node.store
            .revoke_tenant_key("acme", &minted.key_id, None)
            .await
            .unwrap();
        assert!(node.keys.current().authenticate(&minted.key).is_none());
        assert!(
            node.keys
                .current()
                .authenticate(node.admin.expose())
                .is_some()
        );

        node.store.delete_tenant("acme", Some(3)).await.unwrap();
        assert!(!node.root.join("keys.d/acme.toml").exists());
        assert!(node.store.get("acme").is_err());
    }

    #[tokio::test]
    async fn the_configured_name_is_the_file_written() {
        let node = node("tenant-{tenant}.toml", "");
        node.store
            .put_tenant("acme", TenantSpec::default(), None)
            .await
            .unwrap();
        assert!(node.root.join("keys.d/tenant-acme.toml").exists());
    }

    #[tokio::test]
    async fn a_stale_revision_is_refused_and_changes_nothing() {
        let node = node("{tenant}.toml", "");
        node.store
            .put_tenant("acme", TenantSpec::default(), None)
            .await
            .unwrap();
        let err = node
            .store
            .create_tenant_key("acme", writer(), Some(7))
            .await
            .unwrap_err();
        assert!(
            matches!(
                err,
                TenantError::RevisionMismatch {
                    current: 1,
                    expected: 7,
                    ..
                }
            ),
            "{err}"
        );
        assert!(node.store.get("acme").unwrap().keys.is_empty());
    }

    #[tokio::test]
    async fn the_api_mints_no_key_above_writer() {
        let node = node("{tenant}.toml", "");
        node.store
            .put_tenant("acme", TenantSpec::default(), None)
            .await
            .unwrap();
        for role in [Role::Admin, Role::Provisioner] {
            let spec = KeySpec { role, ..writer() };
            let err = node
                .store
                .create_tenant_key("acme", spec, None)
                .await
                .unwrap_err();
            assert!(matches!(err, TenantError::Forbidden(_)), "{err}");
        }
    }

    /// The loader decides, and a change it refuses is undone on disk and in the ring.
    #[tokio::test]
    async fn a_change_the_loader_refuses_is_undone() {
        let node = node("{tenant}.toml", "");
        node.store
            .put_tenant("acme", TenantSpec::default(), None)
            .await
            .unwrap();

        // A scope overlapping acme's: refused, and beta's file is gone again.
        let overlapping = TenantSpec {
            allowed_indexes: Some(vec!["acme-eu-*".to_string()]),
            ..Default::default()
        };
        let err = node
            .store
            .put_tenant("beta", overlapping, None)
            .await
            .unwrap_err();
        assert!(
            matches!(err, TenantError::Refused(ref m) if m.contains("overlaps")),
            "{err}"
        );
        assert!(!node.root.join("keys.d/beta.toml").exists());

        // A key reaching past its tenant: refused, and acme is as it was.
        let spec = KeySpec {
            allowed_indexes: Some(vec!["beta-docs".to_string()]),
            ..writer()
        };
        let err = node
            .store
            .create_tenant_key("acme", spec, None)
            .await
            .unwrap_err();
        assert!(
            matches!(err, TenantError::Refused(ref m) if m.contains("outside tenant")),
            "{err}"
        );
        let view = node.store.get("acme").unwrap();
        assert_eq!(view.revision, Some(1));
        assert!(view.keys.is_empty());
    }

    #[tokio::test]
    async fn the_operators_tenants_and_files_are_never_rewritten() {
        let node = node("{tenant}.toml", "[security.tenants.ops]\nmax_indexes = 1\n");
        let err = node
            .store
            .put_tenant("ops", TenantSpec::default(), None)
            .await
            .unwrap_err();
        assert!(matches!(err, TenantError::NotManaged { .. }), "{err}");

        // A file at the path that the node did not write, for a tenant that does not exist yet.
        std::fs::write(
            node.root.join("keys.d/beta.toml"),
            "# the operator's notes\n",
        )
        .unwrap();
        let err = node
            .store
            .put_tenant("beta", TenantSpec::default(), None)
            .await
            .unwrap_err();
        assert!(matches!(err, TenantError::ForeignFile(_)), "{err}");
        assert_eq!(
            std::fs::read_to_string(node.root.join("keys.d/beta.toml")).unwrap(),
            "# the operator's notes\n"
        );
    }

    #[tokio::test]
    async fn a_name_that_could_be_a_path_is_refused() {
        let node = node("{tenant}.toml", "");
        for name in ["../etc", "a/b", ".hidden", "", "a..b", &"x".repeat(65)] {
            let err = node
                .store
                .put_tenant(name, TenantSpec::default(), None)
                .await
                .unwrap_err();
            assert!(matches!(err, TenantError::InvalidName(_)), "{name}: {err}");
        }
    }

    #[tokio::test]
    async fn without_managed_files_nothing_is_managed() {
        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join("cameodb.toml");
        std::fs::write(&config, "[node]\nprofile = \"local\"\n").unwrap();
        let cli = crate::config::CliOverrides::parse([
            "--config".to_string(),
            config.to_string_lossy().to_string(),
        ])
        .unwrap();
        let security = crate::config::CameoDbConfig::load_with_cli(&cli)
            .unwrap()
            .security;
        let store = TenantStore::new(
            Arc::new(KeyReloader::new(cli, security).unwrap()),
            "n".into(),
        );
        let err = store
            .put_tenant("acme", TenantSpec::default(), None)
            .await
            .unwrap_err();
        assert!(matches!(err, TenantError::NotConfigured("tenant")), "{err}");
        let err = store.create_key(writer(), None).await.unwrap_err();
        assert!(matches!(err, TenantError::NotConfigured("key")), "{err}");
    }
}
