//! The command-line and environment override layer: the flag table, `CliOverrides`,
//! unknown-key reporting and the moved-key adoption protocol.

use super::*;
use anyhow::Result;
use tracing::warn;

/// Whether a flag carries a value or is a bare switch.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum FlagKind {
    /// `--http-port 9480` or `--http-port=9480`.
    Value,
    /// `--cluster-enabled` (means `true`), or an explicit `--cluster-enabled=false`.
    Switch,
}

/// One setting that can be overridden after the config file is read.
///
/// Each entry ties a flag and an environment variable to a single setter, which is the point:
/// the two layers are declared together, so neither can be added, renamed or given different
/// parsing rules without the other. [`CameoDbConfig::apply_overrides`] walks this table, and
/// [`cli_help`] renders it into `--help`, so the flag list cannot go stale either.
pub(crate) struct Override {
    /// Long flag, including the leading dashes.
    pub(crate) flag: &'static str,
    /// Environment variable with the same effect.
    pub(crate) env: &'static str,
    pub(crate) kind: FlagKind,
    /// Value placeholder shown in `--help` (empty for switches).
    pub(crate) placeholder: &'static str,
    pub(crate) help: &'static str,
    /// Applies a raw string — from either layer — to the config.
    pub(crate) apply: fn(&mut CameoDbConfig, &str) -> Result<()>,
}

/// Parse the boolean spellings the environment layer has always accepted; anything else is
/// false. Kept lenient, and identical for flags, so `FOO=yes` and `--foo=yes` agree.
fn parse_bool(raw: &str) -> bool {
    matches!(
        raw.trim().to_ascii_lowercase().as_str(),
        "true" | "1" | "yes"
    )
}

/// Split a comma- or semicolon-separated list, dropping empty entries.
fn parse_list(raw: &str) -> Vec<String> {
    raw.split([',', ';'])
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(str::to_string)
        .collect()
}

/// Every setting overridable from the command line or the environment.
#[rustfmt::skip]
pub(crate) const OVERRIDES: &[Override] = &[
    Override {
        flag: "--http-port", env: "CAMEODB_HTTP_PORT", kind: FlagKind::Value,
        placeholder: "<PORT>", help: "HTTP API listen port",
        apply: |c, v| { c.network.http.port = v.parse()?; Ok(()) },
    },
    Override {
        flag: "--http-bind-address", env: "CAMEODB_HTTP_BIND_ADDRESS", kind: FlagKind::Value,
        placeholder: "<ADDR>", help: "HTTP API bind address",
        apply: |c, v| { c.network.http.bind_address = v.to_string(); Ok(()) },
    },
    Override {
        flag: "--max-record-size-mb", env: "CAMEODB_MAX_RECORD_SIZE_MB", kind: FlagKind::Value,
        placeholder: "<MB>", help: "Largest accepted record; derives body and message limits",
        apply: |c, v| { c.limits.max_record_size_mb = v.parse()?; Ok(()) },
    },
    Override {
        flag: "--max-body-size-mb", env: "CAMEODB_MAX_BODY_SIZE_MB", kind: FlagKind::Value,
        placeholder: "<MB>", help: "HTTP body limit (defaults to derived from record size)",
        apply: |c, v| { c.limits.max_body_size_mb = v.parse()?; Ok(()) },
    },
    Override {
        flag: "--max-concurrent-requests", env: "CAMEODB_MAX_CONCURRENT_REQUESTS", kind: FlagKind::Value,
        placeholder: "<N>", help: "Max concurrent in-flight HTTP requests (default: 128)",
        apply: |c, v| { c.network.http.max_concurrent_requests = v.parse()?; Ok(()) },
    },
    Override {
        flag: "--request-timeout-secs", env: "CAMEODB_REQUEST_TIMEOUT_SECS", kind: FlagKind::Value,
        placeholder: "<SECS>", help: "HTTP request timeout (defaults to derived from record size)",
        apply: |c, v| { c.network.http.request_timeout_secs = Some(v.parse()?); Ok(()) },
    },
    Override {
        flag: "--data-paths", env: "CAMEODB_DATA_PATHS", kind: FlagKind::Value,
        placeholder: "<PATHS>", help: "Colon-separated storage directories",
        apply: |c, v| { c.storage.data_paths = v.split(':').map(PathBuf::from).collect(); Ok(()) },
    },
    Override {
        flag: "--storage-wal-sync", env: "CAMEODB_STORAGE_WAL_SYNC", kind: FlagKind::Switch,
        placeholder: "", help: "fsync the WAL on every write",
        apply: |c, v| { c.storage.wal_sync = parse_bool(v); Ok(()) },
    },
    Override {
        flag: "--storage-default-batch-size", env: "CAMEODB_STORAGE_DEFAULT_BATCH_SIZE", kind: FlagKind::Value,
        placeholder: "<N>", help: "Documents per write batch before an automatic commit",
        apply: |c, v| { c.storage.default_batch_size = v.parse()?; Ok(()) },
    },
    Override {
        flag: "--indexer-memory-min-mb", env: "CAMEODB_INDEXER_MEMORY_MIN_MB", kind: FlagKind::Value,
        placeholder: "<MB>", help: "Lower bound on per-index writer memory",
        apply: |c, v| { c.search.indexer_memory_min_mb = v.parse()?; Ok(()) },
    },
    Override {
        flag: "--indexer-memory-max-mb", env: "CAMEODB_INDEXER_MEMORY_MAX_MB", kind: FlagKind::Value,
        placeholder: "<MB>", help: "Upper bound on per-index writer memory",
        apply: |c, v| { c.search.indexer_memory_max_mb = v.parse()?; Ok(()) },
    },
    Override {
        flag: "--total-memory-limit-mb", env: "CAMEODB_TOTAL_MEMORY_LIMIT_MB", kind: FlagKind::Value,
        placeholder: "<MB>", help: "Memory budget shared by all indices on this node",
        apply: |c, v| { c.limits.total_memory_limit_mb = v.parse()?; Ok(()) },
    },
    Override {
        flag: "--memory-pressure-threshold-percent", env: "CAMEODB_MEMORY_PRESSURE_THRESHOLD_PERCENT", kind: FlagKind::Value,
        placeholder: "<PCT>", help: "Percent of the budget that counts as memory pressure",
        apply: |c, v| { c.search.memory_pressure_threshold_percent = v.parse()?; Ok(()) },
    },
    Override {
        flag: "--default-search-limit", env: "CAMEODB_DEFAULT_SEARCH_LIMIT", kind: FlagKind::Value,
        placeholder: "<N>", help: "Hits returned when a query names no limit",
        apply: |c, v| { c.search.default_search_limit = v.parse()?; Ok(()) },
    },
    Override {
        flag: "--supervisor-timeout-secs", env: "CAMEODB_SUPERVISOR_TIMEOUT_SECS", kind: FlagKind::Value,
        placeholder: "<SECS>", help: "Shard supervisor timeout",
        apply: |c, v| { c.search.supervisor_timeout_secs = v.parse()?; Ok(()) },
    },
    Override {
        flag: "--node-label", env: "CAMEODB_NODE_LABEL", kind: FlagKind::Value,
        placeholder: "<NAME>", help: "Human-readable name for this node",
        apply: |c, v| { c.node.label = Some(v.to_string()); Ok(()) },
    },
    Override {
        flag: "--node-zone", env: "CAMEODB_NODE_ZONE", kind: FlagKind::Value,
        placeholder: "<ZONE>", help: "Availability zone this node reports",
        apply: |c, v| { c.node.zone = v.to_string(); Ok(()) },
    },
    Override {
        flag: "--profile", env: "CAMEODB_PROFILE", kind: FlagKind::Value,
        placeholder: "<NAME>", help: "Security posture: local, internal, or external",
        apply: |c, v| {
            c.node.profile = Some(crate::posture::Profile::parse(v).map_err(|e| ConfigError::NetworkConfig { message: e })?);
            Ok(())
        },
    },
    Override {
        flag: "--security-enabled", env: "CAMEODB_SECURITY_ENABLED", kind: FlagKind::Switch,
        placeholder: "", help: "Require an API key on HTTP and MCP requests",
        apply: |c, v| { c.security.enabled = parse_bool(v); Ok(()) },
    },
    // A *hash*, never a key: the server has no use for a key and nothing that holds one can
    // leak it. This is also why there is no `CAMEODB_API_KEY` here — that name belongs to
    // the client, and the two would collide the first time both ran in one compose file.
    Override {
        flag: "--api-key-hash", env: "CAMEODB_API_KEY_HASH", kind: FlagKind::Value,
        placeholder: "<SHA256>", help: "Single API key digest, 'sha256:<hex>' from `cameodb keygen`",
        apply: |c, v| { c.security.override_key_mut().key_hash = Some(v.to_string()); Ok(()) },
    },
    Override {
        flag: "--api-key-role", env: "CAMEODB_API_KEY_ROLE", kind: FlagKind::Value,
        placeholder: "<ROLE>", help: "Role for --api-key-hash: admin, writer, or reader",
        apply: |c, v| {
            c.security.override_key_mut().role = Some(crate::auth::Role::parse(v).map_err(|e| ConfigError::SecurityConfig { message: e })?);
            Ok(())
        },
    },
    Override {
        flag: "--cluster-enabled", env: "CAMEODB_CLUSTER_ENABLED", kind: FlagKind::Switch,
        placeholder: "", help: "Join a cluster instead of running single-node",
        apply: |c, v| { c.network.cluster.enabled = parse_bool(v); Ok(()) },
    },
    Override {
        flag: "--cluster-bind-address", env: "CAMEODB_CLUSTER_BIND_ADDRESS", kind: FlagKind::Value,
        placeholder: "<ADDR>", help: "Cluster transport bind address",
        apply: |c, v| { c.network.cluster.bind_address = v.to_string(); Ok(()) },
    },
    Override {
        flag: "--cluster-port", env: "CAMEODB_CLUSTER_PORT", kind: FlagKind::Value,
        placeholder: "<PORT>", help: "Cluster transport port",
        apply: |c, v| { c.network.cluster.cluster_port = v.parse()?; Ok(()) },
    },
    Override {
        flag: "--cluster-name", env: "CAMEODB_CLUSTER_NAME", kind: FlagKind::Value,
        placeholder: "<NAME>", help: "Cluster this node belongs to (ignored if empty)",
        apply: |c, v| {
            if !v.trim().is_empty() { c.network.cluster.cluster_name = v.to_string(); }
            Ok(())
        },
    },
    Override {
        flag: "--seed-nodes", env: "CAMEODB_SEED_NODES", kind: FlagKind::Value,
        placeholder: "<ADDRS>", help: "Comma-separated seed node addresses (ignored if empty)",
        apply: |c, v| {
            let parsed = parse_list(v);
            if !parsed.is_empty() { c.network.cluster.seed_nodes = parsed; }
            Ok(())
        },
    },
    Override {
        flag: "--cluster-nodes", env: "CAMEODB_CLUSTER_NODES", kind: FlagKind::Value,
        placeholder: "<ADDRS>", help: "Comma-separated static cluster members (ignored if empty)",
        apply: |c, v| {
            let parsed = parse_list(v);
            if !parsed.is_empty() { c.network.cluster.cluster_nodes = parsed; }
            Ok(())
        },
    },
    Override {
        flag: "--cluster-psk", env: "CAMEODB_CLUSTER_PSK", kind: FlagKind::Value,
        placeholder: "<HEX>", help: "Inline hex-encoded 32-byte cluster pre-shared key",
        apply: |c, v| { c.network.cluster.psk = Some(v.to_string()); Ok(()) },
    },
    Override {
        flag: "--cluster-psk-file", env: "CAMEODB_CLUSTER_PSK_FILE", kind: FlagKind::Value,
        placeholder: "<PATH>", help: "Path to file containing hex-encoded 32-byte cluster PSK",
        apply: |c, v| { c.network.cluster.psk_file = Some(PathBuf::from(v)); Ok(()) },
    },
];

/// Configuration overrides collected from the command line.
///
/// Values are kept as raw strings and interpreted by [`OVERRIDES`] during
/// [`CameoDbConfig::load_with_cli`], so parsing a command line never depends on config state
/// and can be unit-tested on its own.
#[derive(Debug, Default, Clone)]
pub struct CliOverrides {
    /// `--config <path>`, if given.
    pub config_path: Option<String>,
    /// `(flag, raw value)` in the order the flags appeared; a repeated flag keeps the last.
    values: Vec<(&'static str, String)>,
}

impl CliOverrides {
    /// Parse server flags from `args`, which must not include the program name.
    ///
    /// Unknown flags and missing values are hard errors. Silently ignoring them is what let
    /// `cameodb --config foo.toml` start on a completely different configuration than asked.
    pub fn parse<I>(args: I) -> Result<Self>
    where
        I: IntoIterator<Item = String>,
    {
        let mut parsed = Self::default();
        let mut args = args.into_iter().peekable();

        while let Some(arg) = args.next() {
            let (name, inline_value) = match arg.split_once('=') {
                Some((name, value)) => (name.to_string(), Some(value.to_string())),
                None => (arg.clone(), None),
            };

            if name == "--config" || name == "-c" {
                let path = match inline_value {
                    Some(value) => value,
                    None => args.next().ok_or_else(|| ConfigError::CommandLine {
                        message: format!("{name} requires a path"),
                    })?,
                };
                parsed.config_path = Some(path);
                continue;
            }

            let Some(entry) = OVERRIDES.iter().find(|entry| entry.flag == name) else {
                return Err(ConfigError::CommandLine {
                    message: format!("Unknown option: {arg}"),
                }
                .into());
            };

            let value = match (inline_value, entry.kind) {
                (Some(value), _) => value,
                // A bare switch means "true"; it must not swallow the next argument.
                (None, FlagKind::Switch) => "true".to_string(),
                (None, FlagKind::Value) => args.next().ok_or_else(|| ConfigError::CommandLine {
                    message: format!("{} requires a value {}", entry.flag, entry.placeholder),
                })?,
            };

            parsed.values.retain(|(flag, _)| *flag != entry.flag);
            parsed.values.push((entry.flag, value));
        }

        Ok(parsed)
    }

    /// Whether the operator named a config file on the command line.
    pub(crate) fn has_explicit_config_path(&self) -> bool {
        self.config_path.is_some() || std::env::var_os("CAMEODB_CONFIG").is_some()
    }

    /// The raw value given for `flag`, if any.
    pub(crate) fn value_for(&self, flag: &str) -> Option<&str> {
        self.values
            .iter()
            .find(|(name, _)| *name == flag)
            .map(|(_, value)| value.as_str())
    }
}

/// Dotted paths in `content` that no configuration field claims, deepest name last
/// (`storrage`, `network.http.prot`).
///
/// TOML only: it is the documented format, the one `generate-config` emits, and the one a
/// generic value tree is cheap to build for. A YAML file simply gets no report.
pub(crate) fn unrecognized_keys(content: &str) -> Vec<String> {
    let Ok(parsed) = toml::from_str::<toml::Value>(content) else {
        return Vec::new();
    };
    // The schema is the serialized default config: exactly the set of keys that mean
    // something, derived from the structs themselves rather than a hand-maintained list.
    //
    // Serialized as JSON rather than TOML because `None` becomes `null` and therefore still
    // *appears*. A TOML schema silently drops every optional setting that happens to default
    // to unset, so `node.profile`, `tls.cert_file`, `tls.key_file` and `cluster.psk_file`
    // were each reported as a typo to anyone who set them — the opposite of this function's
    // job.
    let Ok(schema) = serde_json::to_value(CameoDbConfig::default()) else {
        return Vec::new();
    };

    let mut unknown = Vec::new();
    collect_unrecognized(&parsed, &schema, "", &mut unknown);
    unknown.retain(|key| !NEVER_SERIALIZED_SETTINGS.contains(&key.as_str()));
    unknown
}

/// Report where a setting moved to, and whether the value there was taken from it.
fn moved(file: &str, old: &str, new: &str, applied: bool, value: &dyn std::fmt::Display) {
    if applied {
        warn!(
            "{file}: {old} has moved to limits.{new}; applying {value}. \
             Move it before 0.4.0, when the old spelling goes."
        );
    } else {
        warn!("{file}: {old} has moved to limits.{new}, which is already set; old key ignored.");
    }
}

/// The one take/adopt/warn step `adopt_moved_settings` repeats per moved key: if the old
/// spelling was written at all, adopt it into `slot` when the slot is still at its
/// default — `[limits]` wins where it says anything — and warn either way.
pub(crate) fn adopt_moved<T, S>(
    path: &str,
    old_key: &str,
    new_key: &str,
    old: Option<T>,
    slot: &mut S,
    default: &S,
) where
    S: PartialEq + From<T>,
    T: std::fmt::Display,
{
    let Some(value) = old else { return };
    let free = *slot == *default;
    moved(path, old_key, new_key, free, &value);
    if free {
        *slot = S::from(value);
    }
}

/// Settings a config file may set that no serialization can contain, and which therefore
/// cannot appear in the schema above.
///
/// One entry, and it earns it: the cluster PSK is `skip_serializing` precisely so that no
/// config dump can leak it, which also means the schema cannot see it.
const NEVER_SERIALIZED_SETTINGS: &[&str] = &["network.cluster.psk"];

/// Walk `parsed` against `schema`, recording paths absent from the schema.
///
/// Only tables are recursed into. Arrays of tables — `[[security.api_keys]]` — are checked
/// for existence but not for the keys inside them, which is why every field of an entry is
/// optional and validated by name in [`crate::auth::SecurityConfig::load_keyring`].
fn collect_unrecognized(
    parsed: &toml::Value,
    schema: &serde_json::Value,
    prefix: &str,
    unknown: &mut Vec<String>,
) {
    let (Some(parsed), Some(schema)) = (parsed.as_table(), schema.as_object()) else {
        return;
    };

    for (key, value) in parsed {
        let path = if prefix.is_empty() {
            key.clone()
        } else {
            format!("{prefix}.{key}")
        };

        match schema.get(key) {
            // Recurse only into nested tables; a table where the schema wants a value (or the
            // reverse) is a type error the parse above would already have rejected.
            Some(known) => collect_unrecognized(value, known, &path, unknown),
            None => unknown.push(path),
        }
    }
}

/// Render the server options for `--help`, straight from [`OVERRIDES`].
pub fn cli_help() -> String {
    let mut lines = vec![format!(
        "  {:<44}{}",
        "-c, --config <PATH>", "Configuration file to load (TOML or YAML)"
    )];

    for entry in OVERRIDES {
        let flag = if entry.placeholder.is_empty() {
            entry.flag.to_string()
        } else {
            format!("{} {}", entry.flag, entry.placeholder)
        };
        lines.push(format!("  {:<44}{} [{}]", flag, entry.help, entry.env));
    }

    lines.join("\n")
}
