//! `include` — key and tenant files beside the main config.
//!
//! The main file is the operator's. Keys and tenants change far more often than anything else
//! in it, and are written by whoever provisions them — an operator splitting one file per
//! tenant, or the node itself on behalf of a provisioning service — so they can live in files of
//! their own, named from the main file:
//!
//! ```toml
//! include = ["keys.d/*.toml"]
//! ```
//!
//! An included file **adds**: `[[security.api_keys]]` entries, `[security.tenants.<name>]`
//! tables, and the `[provisioning]` stamp of a file the node wrote. Nothing else is accepted,
//! so an included file cannot move a port, a profile or `[security] enabled`, and nothing in it
//! overrides what another file says — a tenant or a digest defined twice is refused, naming both
//! files. Merging by addition only is what keeps "why does this setting have this value" a
//! question with one file for an answer.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::Deserialize;

use super::CameoDbConfig;
use crate::auth::{ApiKeyConfig, ProvisioningStamp, TenantQuota, TenantSource};

/// One included file, exactly as written.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
struct IncludedFile {
    provisioning: Option<ProvisioningStamp>,
    security: IncludedSecurity,
}

/// The part of `[security]` an included file may carry.
///
/// Key entries are taken as raw tables and checked field by field before they are parsed:
/// [`ApiKeyConfig`] does not deny unknown fields — in the main file a typo is reported as a
/// warning by `unrecognized_keys` — and in a file a program writes, a field nobody reads is a
/// bug worth stopping at.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
struct IncludedSecurity {
    api_keys: Vec<toml::Table>,
    tenants: HashMap<String, TenantQuota>,
}

impl CameoDbConfig {
    /// Read every file `include` names, relative to `config_path`'s folder, and add what each
    /// holds to `[security]`.
    ///
    /// Tenants the main file defines are recorded as its own first, so a later duplicate names
    /// both files whichever of them it was.
    pub(super) fn apply_includes(&mut self, config_path: &Path) -> Result<()> {
        for tenant in self.security.tenants.keys() {
            self.security.tenant_sources.insert(
                tenant.clone(),
                TenantSource {
                    file: config_path.to_path_buf(),
                    provisioning: None,
                },
            );
        }
        if self.include.is_empty() {
            return Ok(());
        }

        let base = config_path
            .parent()
            .filter(|dir| !dir.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let mut files = Vec::new();
        for pattern in &self.include {
            files.extend(
                resolve_pattern(base, pattern)
                    .with_context(|| format!("{}: include '{pattern}'", config_path.display()))?,
            );
        }

        let mut seen = std::collections::HashSet::new();
        for file in files {
            // Two patterns naming one file would add its keys twice, refused as duplicates
            // with a message pointing at the same file twice. Read it once instead.
            if seen.insert(file.clone()) {
                self.merge_included(&file)?;
            }
        }
        Ok(())
    }

    /// Parse one included file and add its keys and tenants.
    fn merge_included(&mut self, file: &Path) -> Result<()> {
        let content = std::fs::read_to_string(file)
            .with_context(|| format!("cannot read included file {}", file.display()))?;
        let included: IncludedFile = toml::from_str(&content)
            .with_context(|| format!("cannot parse included file {}", file.display()))?;
        tracing::info!("📄 Including keys and tenants from: {}", file.display());

        let known_fields = api_key_fields();
        let mut keys = Vec::with_capacity(included.security.api_keys.len());
        for (i, table) in included.security.api_keys.into_iter().enumerate() {
            let origin = format!("{}: [[security.api_keys]] entry {}", file.display(), i + 1);
            if let Some(unknown) = table
                .keys()
                .find(|key| !known_fields.contains(key.as_str()))
            {
                bail!("{origin}: unknown field '{unknown}'");
            }
            let mut entry: ApiKeyConfig = toml::Value::Table(table)
                .try_into()
                .with_context(|| origin.clone())?;
            entry.source = Some(file.to_path_buf());
            keys.push(entry);
        }

        // A file the node wrote holds one tenant and that tenant's keys, which is what lets
        // the stamp speak for the tenant. Checked here rather than when the node writes, so a
        // hand edit that breaks it is caught too.
        if included.provisioning.is_some() {
            let mut tenants = included.security.tenants.keys();
            let (Some(tenant), None) = (tenants.next(), tenants.next()) else {
                bail!(
                    "{}: a file with [provisioning] holds exactly one [security.tenants.<name>] \
                     table; this one holds {}",
                    file.display(),
                    included.security.tenants.len()
                );
            };
            if let Some(stray) = keys
                .iter()
                .find(|key| key.tenant.as_deref() != Some(tenant))
            {
                bail!(
                    "{}: a file with [provisioning] holds only keys of its tenant '{tenant}'; \
                     the key labelled '{}' names {}",
                    file.display(),
                    stray.label.as_deref().unwrap_or("(none)"),
                    stray
                        .tenant
                        .as_deref()
                        .map_or("no tenant".to_string(), |t| format!("'{t}'"))
                );
            }
        }

        for (tenant, quota) in included.security.tenants {
            if let Some(existing) = self.security.tenant_sources.get(&tenant) {
                bail!(
                    "{}: tenant '{tenant}' is already defined in {}. A tenant is defined once",
                    file.display(),
                    existing.file.display()
                );
            }
            self.security.tenant_sources.insert(
                tenant.clone(),
                TenantSource {
                    file: file.to_path_buf(),
                    provisioning: included.provisioning.clone(),
                },
            );
            self.security.tenants.insert(tenant, quota);
        }
        self.security.api_keys.extend(keys);
        Ok(())
    }
}

/// The field names an `[[security.api_keys]]` entry has, from the struct itself: the serialized
/// default carries every field, `None` as `null`, so the list cannot drift from the type.
fn api_key_fields() -> std::collections::HashSet<String> {
    match serde_json::to_value(ApiKeyConfig::default()) {
        Ok(serde_json::Value::Object(fields)) => fields.into_iter().map(|(name, _)| name).collect(),
        _ => Default::default(),
    }
}

/// The files one `include` entry names, in name order.
///
/// A literal path must exist. A wildcard — `*`, in the file name only — may match nothing,
/// which is an empty `keys.d` before the first tenant is written; its folder must exist. Files
/// whose name starts with a dot are never matched by a wildcard, so a file being written as
/// `.acme.toml.tmp` and renamed into place is never read half-done whatever the pattern.
fn resolve_pattern(base: &Path, pattern: &str) -> Result<Vec<PathBuf>> {
    let path = base.join(pattern);
    let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
        bail!("names no file");
    };
    let dir = path.parent().unwrap_or(base);

    if dir.to_string_lossy().contains('*') {
        bail!("a wildcard is accepted in the file name only, not in a folder");
    }
    if name.contains(['?', '[', ']', '{', '}']) {
        bail!("only `*` is accepted as a wildcard");
    }
    if !name.contains('*') {
        if !path.is_file() {
            bail!("file not found: {}", path.display());
        }
        return Ok(vec![path]);
    }

    let entries = std::fs::read_dir(dir)
        .with_context(|| format!("cannot read the folder {}", dir.display()))?;
    let mut files = Vec::new();
    for entry in entries {
        let entry = entry.with_context(|| format!("cannot read the folder {}", dir.display()))?;
        let Ok(file_name) = entry.file_name().into_string() else {
            continue;
        };
        if file_name.starts_with('.') || !wildcard_matches(name, &file_name) {
            continue;
        }
        if entry
            .file_type()
            .map(|kind| kind.is_file())
            .unwrap_or(false)
            || entry.path().is_file()
        {
            files.push(entry.path());
        }
    }
    files.sort();
    Ok(files)
}

/// Whether `name` matches `pattern`, where `*` stands for any run of characters.
fn wildcard_matches(pattern: &str, name: &str) -> bool {
    let mut parts = pattern.split('*');
    let first = parts.next().unwrap_or("");
    let Some(mut rest) = name.strip_prefix(first) else {
        return false;
    };
    let mut parts: Vec<&str> = parts.collect();
    let Some(last) = parts.pop() else {
        // No `*` at all: the prefix had to be the whole name.
        return rest.is_empty();
    };
    for part in parts {
        match rest.find(part) {
            Some(at) => rest = &rest[at + part.len()..],
            None => return false,
        }
    }
    rest.len() >= last.len() && rest.ends_with(last)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::ApiKey;

    fn digest() -> String {
        ApiKey::generate().unwrap().digest().to_config_value()
    }

    /// A config folder holding `cameodb.toml` and the given files, each `(relative path,
    /// contents)`; folders are created as needed.
    fn folder(main: &str, files: &[(&str, String)]) -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        for (name, contents) in files {
            let path = dir.path().join(name);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, contents).unwrap();
        }
        let main_path = dir.path().join("cameodb.toml");
        std::fs::write(&main_path, main).unwrap();
        (dir, main_path)
    }

    fn load(path: &Path) -> Result<CameoDbConfig> {
        CameoDbConfig::load_config_file(&path.to_path_buf())
    }

    fn key(label: &str, tenant: &str) -> String {
        format!(
            "[[security.api_keys]]\nkey_hash = \"{}\"\nrole = \"writer\"\nlabel = \"{label}\"\n\
             tenant = \"{tenant}\"\n",
            digest()
        )
    }

    #[test]
    fn included_keys_and_tenants_are_added_to_the_main_files() {
        let main = format!(
            "include = [\"keys.d/*.toml\"]\n[security]\nenabled = true\n\
             [[security.api_keys]]\nkey_hash = \"{}\"\nrole = \"admin\"\nlabel = \"ops\"\n",
            digest()
        );
        let acme = format!(
            "[security.tenants.acme]\nmax_indexes = 5\n{}{}",
            key("jane@acme.com", "acme"),
            key("agent@acme.com", "acme")
        );
        let beta = format!("[security.tenants.beta]\n{}", key("bob@beta.com", "beta"));
        let (_dir, main_path) = folder(
            &main,
            &[("keys.d/acme.toml", acme), ("keys.d/beta.toml", beta)],
        );

        let config = load(&main_path).unwrap();
        let labels: Vec<_> = config
            .security
            .api_keys
            .iter()
            .map(|key| key.label.clone().unwrap())
            .collect();
        // Main file first, then the included files in name order.
        assert_eq!(
            labels,
            ["ops", "jane@acme.com", "agent@acme.com", "bob@beta.com"]
        );
        assert_eq!(config.security.tenants["acme"].max_indexes, 5);
        assert!(config.security.tenants.contains_key("beta"));
        assert_eq!(config.security.api_keys[0].source, None);
        assert!(
            config.security.api_keys[1]
                .source
                .as_ref()
                .unwrap()
                .ends_with("keys.d/acme.toml")
        );
        assert!(
            config.security.tenant_sources["beta"]
                .file
                .ends_with("keys.d/beta.toml")
        );
        assert_eq!(config.security.load_keyring().unwrap().entries().len(), 4);
    }

    #[test]
    fn a_relative_include_is_read_from_the_config_files_folder_not_the_working_directory() {
        let (_dir, main_path) = folder(
            "include = [\"tenants/acme.toml\"]\n",
            &[("tenants/acme.toml", "[security.tenants.acme]\n".to_string())],
        );
        // The test runs from the crate's folder, where no `tenants/` exists.
        let config = load(&main_path).unwrap();
        assert!(config.security.tenants.contains_key("acme"));
    }

    #[test]
    fn a_wildcard_skips_dotfiles_and_temp_files_and_may_match_nothing() {
        let (_dir, main_path) = folder(
            "include = [\"keys.d/*.toml\"]\n",
            &[
                (".placeholder", String::new()),
                ("keys.d/.acme.toml", "this is not toml".to_string()),
                ("keys.d/acme.toml.tmp", "nor is this".to_string()),
            ],
        );
        let config = load(&main_path).unwrap();
        assert!(config.security.api_keys.is_empty());
        assert!(config.security.tenants.is_empty());
    }

    #[test]
    fn a_tenant_defined_twice_is_refused_naming_both_files() {
        let (_dir, main_path) = folder(
            "include = [\"keys.d/*.toml\"]\n[security.tenants.acme]\nmax_indexes = 1\n",
            &[("keys.d/acme.toml", "[security.tenants.acme]\n".to_string())],
        );
        let err = format!("{:#}", load(&main_path).unwrap_err());
        assert!(err.contains("keys.d/acme.toml"), "{err}");
        assert!(err.contains("cameodb.toml"), "{err}");
        assert!(err.contains("'acme'"), "{err}");
    }

    #[test]
    fn a_digest_in_two_files_is_refused_naming_both() {
        let hash = digest();
        let entry = |label: &str| {
            format!(
                "[[security.api_keys]]\nkey_hash = \"{hash}\"\nrole = \"reader\"\nlabel = \"{label}\"\n"
            )
        };
        let (_dir, main_path) = folder(
            "include = [\"keys.d/*.toml\"]\n",
            &[
                ("keys.d/a.toml", entry("first")),
                ("keys.d/b.toml", entry("second")),
            ],
        );
        let config = load(&main_path).unwrap();
        let err = format!("{:#}", config.security.load_keyring().unwrap_err());
        assert!(err.contains("keys.d/a.toml"), "{err}");
        assert!(err.contains("keys.d/b.toml"), "{err}");
    }

    #[test]
    fn an_included_file_may_not_carry_any_other_setting() {
        for stray in [
            "[security]\nenabled = false\n",
            "[network.http]\nport = 1\n",
            "include = [\"other.toml\"]\n",
        ] {
            let (_dir, main_path) = folder(
                "include = [\"keys.d/*.toml\"]\n",
                &[("keys.d/x.toml", stray.to_string())],
            );
            let err = format!("{:#}", load(&main_path).unwrap_err());
            assert!(err.contains("keys.d/x.toml"), "{stray}: {err}");
        }
    }

    #[test]
    fn an_unknown_field_on_an_included_key_is_refused() {
        let file = format!(
            "[[security.api_keys]]\nkey_hash = \"{}\"\nrole = \"reader\"\nallowed_index = [\"a\"]\n",
            digest()
        );
        let (_dir, main_path) = folder(
            "include = [\"keys.d/*.toml\"]\n",
            &[("keys.d/x.toml", file)],
        );
        let err = format!("{:#}", load(&main_path).unwrap_err());
        assert!(err.contains("allowed_index"), "{err}");
        assert!(err.contains("entry 1"), "{err}");
    }

    #[test]
    fn a_missing_literal_include_or_folder_is_refused_and_unsupported_patterns_too() {
        for pattern in [
            "absent.toml",
            "absent.d/*.toml",
            "*/acme.toml",
            "keys.d/acme?.toml",
        ] {
            let (_dir, main_path) = folder(&format!("include = [\"{pattern}\"]\n"), &[]);
            let err = format!("{:#}", load(&main_path).unwrap_err());
            assert!(err.contains(pattern), "{pattern}: {err}");
        }
    }

    #[test]
    fn a_provisioned_file_holds_one_tenant_and_only_its_keys() {
        let stamp = "[provisioning]\nrevision = 3\nwritten_by = \"node-1\"\nwritten_at = \"2026-11-01T10:00:00Z\"\n";
        let good = format!(
            "{stamp}[security.tenants.acme]\n{}",
            key("jane@acme.com", "acme")
        );
        let (_dir, main_path) = folder(
            "include = [\"keys.d/*.toml\"]\n",
            &[("keys.d/acme.toml", good)],
        );
        let config = load(&main_path).unwrap();
        let source = &config.security.tenant_sources["acme"];
        assert_eq!(source.provisioning.as_ref().unwrap().revision, 3);

        let foreign_key = format!(
            "{stamp}[security.tenants.acme]\n{}",
            key("bob@beta.com", "beta")
        );
        let two_tenants = format!("{stamp}[security.tenants.acme]\n[security.tenants.beta]\n");
        for bad in [foreign_key, two_tenants] {
            let (_dir, main_path) = folder(
                "include = [\"keys.d/*.toml\"]\n",
                &[("keys.d/acme.toml", bad)],
            );
            let err = format!("{:#}", load(&main_path).unwrap_err());
            assert!(err.contains("[provisioning]"), "{err}");
        }
    }

    #[test]
    fn a_file_named_by_two_patterns_is_read_once() {
        let (_dir, main_path) = folder(
            "include = [\"keys.d/*.toml\", \"keys.d/acme.toml\"]\n",
            &[("keys.d/acme.toml", key("jane@acme.com", "acme"))],
        );
        assert_eq!(load(&main_path).unwrap().security.api_keys.len(), 1);
    }

    #[test]
    fn wildcards_match_as_a_shell_would() {
        assert!(wildcard_matches("*.toml", "acme.toml"));
        assert!(!wildcard_matches("*.toml", "acme.toml.tmp"));
        assert!(wildcard_matches("tenant-*.toml", "tenant-acme.toml"));
        assert!(!wildcard_matches("tenant-*.toml", "acme.toml"));
        assert!(wildcard_matches("a*b*c", "aXbYc"));
        assert!(!wildcard_matches("a*b*c", "aXcYb"));
        // The suffix may not reuse characters the prefix consumed.
        assert!(!wildcard_matches("ab*ba", "aba"));
        assert!(wildcard_matches("keys", "keys"));
        assert!(!wildcard_matches("keys", "keys2"));
    }
}
