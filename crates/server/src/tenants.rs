//! Tenants and their keys, managed through the node: the files behind `/_tenants`.
//!
//! A managed tenant is one file — where `[security.tenant_files]` says — holding the tenant's
//! `[security.tenants.<name>]` table, its keys, and a `[provisioning]` stamp that says the node
//! wrote it. The file is the record: every change reads it, changes it, writes it to a temp
//! name in the same folder and renames it into place, then reloads the key ring through the
//! same loader startup uses. A change the loader refuses — a scope overlapping another tenant's,
//! a key reaching past its tenant — is undone, file and ring both, and answered with the
//! loader's reason. So nothing the API writes can be a config the node would refuse to boot
//! with.
//!
//! Changes are serialised by one lock: a change is a read–change–write of a file, and two at
//! once would lose one. A tenant defined anywhere but a file the node wrote is the operator's,
//! and is never rewritten.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::auth::{
    ApiKey, ApiKeyConfig, KeyDigest, KeyReloader, ProvisioningStamp, Role, SecurityConfig,
    TenantFilesConfig, TenantQuota, valid_tenant_name,
};

/// Why a tenant change was refused. Each maps to one HTTP status in the routes.
#[derive(Debug, thiserror::Error)]
pub enum TenantError {
    #[error(
        "tenant management is not configured on this node: add [security.tenant_files] and an \
         include that reads its files"
    )]
    NotConfigured,
    #[error(
        "'{0}' is not a tenant name the node can create: letters, digits, '.', '_' and '-', \
         starting with a letter or digit, at most 64 characters"
    )]
    InvalidName(String),
    #[error("no tenant '{0}'")]
    NotFound(String),
    #[error("no key '{key_id}' in tenant '{tenant}'")]
    KeyNotFound { tenant: String, key_id: String },
    #[error("tenant '{tenant}' is defined in {file}, which the node does not manage")]
    NotManaged { tenant: String, file: PathBuf },
    #[error("{0} exists and was not written by the node for this tenant; it is never overwritten")]
    ForeignFile(PathBuf),
    #[error("tenant '{tenant}' is at revision {current}, not {expected}")]
    RevisionMismatch {
        tenant: String,
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

/// A key as a caller asks for one. The tenant is the route's; the key itself is the node's to
/// generate.
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
    pub tenant: String,
    pub revision: u64,
}

/// A tenant file as the node writes it.
#[derive(Debug, Serialize, Deserialize)]
struct TenantFile {
    provisioning: ProvisioningStamp,
    security: FileSecurity,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct FileSecurity {
    #[serde(default)]
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
        let (files, existing) = self.open(tenant)?;
        check_revision(tenant, existing.as_ref(), if_match)?;

        let mut file = existing.unwrap_or_else(|| TenantFile {
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
        self.commit(&files, tenant, file).await?;
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
        let (files, existing) = self.open(tenant)?;
        if existing.is_none() {
            return Err(TenantError::NotFound(tenant.to_string()));
        }
        check_revision(tenant, existing.as_ref(), if_match)?;

        let path = files.path_for(tenant);
        let previous = std::fs::read_to_string(&path).map_err(|e| TenantError::Io(e.into()))?;
        std::fs::remove_file(&path).map_err(|e| TenantError::Io(e.into()))?;
        if let Err(err) = self.keys.reload() {
            write_atomically(&path, &previous).map_err(TenantError::Io)?;
            let _ = self.keys.reload();
            return Err(TenantError::Refused(format!("{err:#}")));
        }
        Ok(())
    }

    /// Mint a key for a tenant that exists. At most `writer`: a key the API mints can never
    /// manage keys or the node.
    pub async fn create_key(
        &self,
        tenant: &str,
        spec: KeySpec,
        if_match: Option<u64>,
    ) -> Result<MintedKey, TenantError> {
        if !matches!(spec.role, Role::Writer | Role::Reader) {
            return Err(TenantError::Forbidden(format!(
                "the tenant API mints writer and reader keys only, not '{}'",
                spec.role
            )));
        }
        let _guard = self.lock.lock().await;
        let (files, existing) = self.open(tenant)?;
        let Some(mut file) = existing else {
            return Err(TenantError::NotFound(tenant.to_string()));
        };
        check_revision(tenant, Some(&file), if_match)?;

        let key = ApiKey::generate().map_err(TenantError::Io)?;
        let digest = key.digest();
        file.security.api_keys.push(ApiKeyConfig {
            key_hash: Some(digest.to_config_value()),
            role: Some(spec.role),
            label: spec.label,
            allowed_indexes: spec.allowed_indexes,
            index_overrides: spec.index_overrides,
            tenant: Some(tenant.to_string()),
            ..Default::default()
        });
        let revision = self.commit(&files, tenant, file).await?;
        Ok(MintedKey {
            key: key.expose().to_string(),
            key_id: digest.key_id(),
            tenant: tenant.to_string(),
            revision,
        })
    }

    /// Revoke one key. It fails from the next request on.
    pub async fn revoke_key(
        &self,
        tenant: &str,
        key_id: &str,
        if_match: Option<u64>,
    ) -> Result<TenantView, TenantError> {
        let _guard = self.lock.lock().await;
        let (files, existing) = self.open(tenant)?;
        let Some(mut file) = existing else {
            return Err(TenantError::NotFound(tenant.to_string()));
        };
        check_revision(tenant, Some(&file), if_match)?;

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
                tenant: tenant.to_string(),
                key_id: key_id.to_string(),
            });
        }
        self.commit(&files, tenant, file).await?;
        self.get(tenant)
    }

    /// The layout, and the tenant's file if the node has written one. Refuses a tenant name the
    /// node cannot create, a tenant the operator defined, and a file at the path that is not
    /// this tenant's.
    fn open(&self, tenant: &str) -> Result<(TenantFilesConfig, Option<TenantFile>), TenantError> {
        let security = self.keys.security();
        let files = security
            .tenant_files
            .clone()
            .ok_or(TenantError::NotConfigured)?;
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

        let path = files.path_for(tenant);
        let content = match std::fs::read_to_string(&path) {
            Ok(content) => content,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok((files, None)),
            Err(err) => return Err(TenantError::Io(err.into())),
        };
        let any: AnyFile =
            toml::from_str(&content).map_err(|_| TenantError::ForeignFile(path.clone()))?;
        let ours = any.provisioning.is_some()
            && any.security.tenants.len() == 1
            && any.security.tenants.contains_key(tenant);
        if !ours {
            return Err(TenantError::ForeignFile(path));
        }
        let file = TenantFile {
            provisioning: any.provisioning.expect("checked above"),
            security: any.security,
        };
        Ok((files, Some(file)))
    }

    /// Write `file` as the tenant's next revision and reload; undo both if the loader refuses
    /// it. Returns the revision written.
    async fn commit(
        &self,
        files: &TenantFilesConfig,
        tenant: &str,
        mut file: TenantFile,
    ) -> Result<u64, TenantError> {
        let path = files.path_for(tenant);
        let previous = match std::fs::read_to_string(&path) {
            Ok(content) => Some(content),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
            Err(err) => return Err(TenantError::Io(err.into())),
        };

        file.provisioning = self.stamp(file.provisioning.revision + 1);
        let revision = file.provisioning.revision;
        let body = toml::to_string(&file).map_err(|e| TenantError::Io(e.into()))?;
        let content = format!(
            "# Written by CameoDB for tenant '{tenant}'. Change it through /_tenants/{tenant}: the\n\
             # node rewrites this file on every change, and a hand edit lasts until the next one.\n\n\
             {body}"
        );
        write_atomically(&path, &content).map_err(TenantError::Io)?;

        if let Err(err) = self.keys.reload() {
            let restored = match &previous {
                Some(previous) => write_atomically(&path, previous),
                None => std::fs::remove_file(&path).map_err(Into::into),
            };
            if let Err(restore) = restored {
                tracing::error!(
                    path = %path.display(),
                    "a refused tenant change could not be undone: {restore:#}"
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
            .map(|entry| KeyView {
                key_id: entry.key_id(),
                label: entry.label().to_string(),
                role: entry.role(),
                indexes: entry.scope_summary(),
            })
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

fn check_revision(
    tenant: &str,
    file: Option<&TenantFile>,
    if_match: Option<u64>,
) -> Result<(), TenantError> {
    let Some(expected) = if_match else {
        return Ok(());
    };
    let current = file.map_or(0, |file| file.provisioning.revision);
    if current != expected {
        return Err(TenantError::RevisionMismatch {
            tenant: tenant.to_string(),
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

    let dir = path.parent().context("a tenant file has a folder")?;
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .context("a tenant file has a name")?;
    let temp = dir.join(format!(".{name}.tmp"));
    let mut file =
        std::fs::File::create(&temp).with_context(|| format!("cannot write {}", temp.display()))?;
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
                 [security.tenant_files]\ndir = \"keys.d\"\nname = \"{name}\"\n\
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
            .create_key("acme", writer(), Some(1))
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
            .revoke_key("acme", &minted.key_id, None)
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
            .create_key("acme", writer(), Some(7))
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
            let err = node.store.create_key("acme", spec, None).await.unwrap_err();
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
        let err = node.store.create_key("acme", spec, None).await.unwrap_err();
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
    async fn without_tenant_files_nothing_is_managed() {
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
        assert!(matches!(err, TenantError::NotConfigured), "{err}");
    }
}
