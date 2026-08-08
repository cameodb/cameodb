//! Security posture presets.
//!
//! A deployment declares *where it sits on the network* — `local`, `internal`, or
//! `external` — and the server enforces the invariants that go with that answer. The
//! alternative is a checklist of a dozen unrelated settings that has to be re-verified by
//! hand on every deployment; here the posture is one word in the config file and the
//! binary refuses to start if the rest of the file contradicts it.
//!
//! **Presets assert, they never rewrite.** A preset does not quietly substitute values —
//! a config that looks permissive but starts anyway would be worse than no preset at all.
//! Every value stays exactly as written; the preset decides which combinations are
//! allowed. Deviating is done by declaring a lower profile, not by overriding one rule,
//! so the posture stays readable as a single word.

use std::fmt;
use std::net::IpAddr;

use serde::{Deserialize, Serialize};

use crate::auth::Capability;
use crate::config::CameoDbConfig;

/// Where this node sits on the network.
///
/// The three values are one axis — how far the node can be reached — and the rules get
/// stricter as the reach widens. Naming them after the reach rather than after an
/// environment (`dev`, `staging`) is deliberate: every rule keys off the bind address, so a
/// lifecycle name would invite an operator to choose by what the box is *for* instead of by
/// who can talk to it, and then be rejected at startup for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Profile {
    /// Reachable only from this machine. Permissive, because nothing off-box can connect.
    Local,
    /// Reachable on a trusted network, but not from the internet.
    Internal,
    /// Reachable from an untrusted network.
    External,
}

impl Profile {
    pub fn as_str(self) -> &'static str {
        match self {
            Profile::Local => "local",
            Profile::Internal => "internal",
            Profile::External => "external",
        }
    }

    pub fn parse(s: &str) -> Result<Self, String> {
        match s.trim().to_ascii_lowercase().as_str() {
            "local" => Ok(Profile::Local),
            "internal" => Ok(Profile::Internal),
            "external" => Ok(Profile::External),
            other => Err(format!(
                "unknown profile '{}' (expected one of: local, internal, external)",
                other
            )),
        }
    }
}

impl fmt::Display for Profile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Result of one posture rule.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Pass(String),
    /// Allowed, but the operator should know. Never blocks startup.
    Warn(String),
    /// Blocks startup.
    Fail(String),
}

impl Outcome {
    pub fn is_fail(&self) -> bool {
        matches!(self, Outcome::Fail(_))
    }

    pub fn marker(&self) -> &'static str {
        match self {
            Outcome::Pass(_) => "PASS",
            Outcome::Warn(_) => "WARN",
            Outcome::Fail(_) => "FAIL",
        }
    }

    pub fn message(&self) -> &str {
        match self {
            Outcome::Pass(m) | Outcome::Warn(m) | Outcome::Fail(m) => m,
        }
    }
}

/// One named rule and how this config fared against it.
#[derive(Debug, Clone)]
pub struct Check {
    pub rule: &'static str,
    pub outcome: Outcome,
}

/// The full posture assessment for a config.
#[derive(Debug, Clone)]
pub struct Posture {
    pub profile: Profile,
    /// True when the profile was inferred from the bind address rather than declared.
    pub inferred: bool,
    pub checks: Vec<Check>,
}

impl Posture {
    pub fn failures(&self) -> impl Iterator<Item = &Check> {
        self.checks.iter().filter(|c| c.outcome.is_fail())
    }

    pub fn warnings(&self) -> impl Iterator<Item = &Check> {
        self.checks
            .iter()
            .filter(|c| matches!(c.outcome, Outcome::Warn(_)))
    }

    /// Human-readable matrix, used by `cameodb check-config` and the startup banner.
    pub fn render(&self) -> String {
        let mut out = String::new();
        let source = if self.inferred {
            " (inferred from a loopback bind address; declare it with `profile` under [node])"
        } else {
            ""
        };
        out.push_str(&format!("Security profile: {}{}\n", self.profile, source));
        for check in &self.checks {
            out.push_str(&format!(
                "  [{}] {:<16} {}\n",
                check.outcome.marker(),
                check.rule,
                check.outcome.message()
            ));
        }
        out
    }
}

/// True when the address can only be reached from this machine.
///
/// A hostname other than `localhost` is treated as non-loopback: resolving it here would
/// make the answer depend on DNS at startup, and a posture rule that can change meaning
/// between restarts is not a rule.
fn is_loopback_bind(addr: &str) -> bool {
    let trimmed = addr.trim();
    if trimmed.eq_ignore_ascii_case("localhost") {
        return true;
    }
    match trimmed.parse::<IpAddr>() {
        Ok(ip) => ip.is_loopback(),
        Err(_) => false,
    }
}

/// Resolve the profile: declared wins, otherwise infer `local` for a loopback bind.
///
/// A node reachable from other hosts must say what it is. Guessing `internal` for it
/// would hand out the weaker ruleset precisely when the stronger one matters.
fn resolve_profile(config: &CameoDbConfig) -> Result<(Profile, bool), String> {
    if let Some(declared) = config.node.profile {
        return Ok((declared, false));
    }
    if is_loopback_bind(&config.network.http.bind_address) {
        Ok((Profile::Local, true))
    } else {
        Err(format!(
            "no security profile declared and bind_address '{}' is reachable from other hosts.\n\
             Set `profile` under [node] to one of:\n  \
             local    — reachable only from this machine, permissive\n  \
             internal — trusted network: explicit CORS origins, cluster PSK required\n  \
             external — untrusted network: TLS required, admin endpoints off, auth required",
            config.network.http.bind_address
        ))
    }
}

/// Evaluate every posture rule for this config.
pub fn evaluate(config: &CameoDbConfig) -> Result<Posture, String> {
    let (profile, inferred) = resolve_profile(config)?;
    let http = &config.network.http;
    let cluster = &config.network.cluster;
    let mut checks = Vec::new();

    let mut push = |rule: &'static str, outcome: Outcome| checks.push(Check { rule, outcome });

    // --- Bind scope -------------------------------------------------------------
    let loopback = is_loopback_bind(&http.bind_address);
    push(
        "bind",
        match (profile, loopback) {
            (Profile::Local, false) => Outcome::Fail(format!(
                "profile 'local' requires a loopback bind_address, found '{}'. Use 127.0.0.1, \
                 or declare profile 'internal'/'external'",
                http.bind_address
            )),
            (_, true) => Outcome::Pass(format!("{} (loopback only)", http.bind_address)),
            (_, false) => Outcome::Pass(format!("{} (reachable off-box)", http.bind_address)),
        },
    );

    // --- TLS --------------------------------------------------------------------
    push(
        "tls",
        match (profile, http.tls.enabled) {
            (_, true) => Outcome::Pass("HTTPS enabled".to_string()),
            (Profile::External, false) => Outcome::Fail(
                "profile 'external' requires TLS. Set [network.http.tls] enabled = true with \
                 cert_file and key_file"
                    .to_string(),
            ),
            (Profile::Internal, false) if config.security.enabled => Outcome::Warn(
                "plaintext HTTP on a network-reachable bind; every API key travels in the clear \
                 and can be replayed by anyone on the path"
                    .to_string(),
            ),
            (Profile::Internal, false) => Outcome::Warn(
                "plaintext HTTP on a network-reachable bind; traffic travels in the clear"
                    .to_string(),
            ),
            (Profile::Local, false) => Outcome::Pass("plaintext (loopback only)".to_string()),
        },
    );

    // --- CORS -------------------------------------------------------------------
    // CORS only governs browsers, so an empty list is the safe default rather than a
    // broken one: non-browser clients are unaffected.
    let wildcard = http.cors_allowed_origins.iter().any(|o| o == "*");
    push(
        "cors",
        match (profile, wildcard, http.cors_allowed_origins.is_empty()) {
            (Profile::Local, true, _) => Outcome::Warn(
                "any origin allowed (\"*\"); any web page you visit can call this node".to_string(),
            ),
            (_, true, _) => Outcome::Fail(format!(
                "profile '{}' does not allow cors_allowed_origins = [\"*\"]. List the origins \
                 that need browser access, or use [] to refuse cross-origin requests entirely",
                profile
            )),
            (_, _, true) => Outcome::Pass("no cross-origin browser access".to_string()),
            (_, _, false) => Outcome::Pass(format!(
                "{} explicit origin(s)",
                http.cors_allowed_origins.len()
            )),
        },
    );

    // Resolved once, read by two rules. Doing the file reads here, rather than leaving them
    // to startup, is what makes `check-config` the one place an operator learns everything
    // wrong with their `[security]` section: reporting "3 keys" while a `key_hash_file` is
    // unreadable would be worse than being slow.
    let keyring = config.security.load_keyring();
    let admin_key_exists = keyring
        .as_ref()
        .is_ok_and(|ring| ring.enabled() && ring.holds(Capability::NodeAdmin));

    // --- Admin endpoints --------------------------------------------------------
    push(
        "admin_api",
        match (profile, http.admin_enabled) {
            (Profile::External, true) => Outcome::Fail(
                "profile 'external' requires admin endpoints to be disabled: they allow memory \
                 purges and writer eviction. Set [network.http] admin_enabled = false"
                    .to_string(),
            ),
            (_, true) if !loopback && admin_key_exists => Outcome::Pass(
                "/_admin/* reachable off-box, gated on a key holding node-admin".to_string(),
            ),
            (_, true) if !loopback => {
                Outcome::Warn("/_admin/* is reachable off-box and unauthenticated".to_string())
            }
            (_, true) => Outcome::Pass("/_admin/* enabled (loopback only)".to_string()),
            (_, false) => Outcome::Pass("/_admin/* disabled".to_string()),
        },
    );

    // --- Cluster membership -----------------------------------------------------
    let has_psk = cluster.psk.is_some() || cluster.psk_file.is_some();
    push(
        "cluster_psk",
        match (profile, cluster.enabled, has_psk) {
            (_, false, _) => Outcome::Pass("single node; cluster disabled".to_string()),
            (_, true, true) => Outcome::Pass("pre-shared key configured".to_string()),
            (Profile::Local, true, false) => {
                Outcome::Warn("cluster enabled without a PSK; any libp2p node can join".to_string())
            }
            (_, true, false) => Outcome::Fail(format!(
                "profile '{}' requires a cluster pre-shared key when the cluster is enabled. \
                 Set [network.cluster] psk_file (preferred) or psk",
                profile
            )),
        },
    );

    // --- Authentication ---------------------------------------------------------
    push(
        "auth",
        match &keyring {
            Err(e) => Outcome::Fail(format!("[security] is unusable: {:#}", e)),
            Ok(ring) if !ring.enabled() => match profile {
                Profile::External => Outcome::Fail(
                    "profile 'external' requires authentication. Set [security] enabled = true \
                     and configure at least one key with `cameodb keygen --role admin`"
                        .to_string(),
                ),
                Profile::Internal => Outcome::Warn(
                    "all HTTP and MCP endpoints are unauthenticated; anyone who can reach the \
                     port can read, write, and delete"
                        .to_string(),
                ),
                // Pass, not Warn: this mirrors how `tls` passes plaintext on loopback. A
                // profile that warns on every boot only teaches operators to ignore warnings.
                Profile::Local => Outcome::Pass("unauthenticated (loopback only)".to_string()),
            },
            Ok(ring) if ring.is_empty() => Outcome::Fail(
                "[security] enabled = true but no keys are configured, so nothing could ever \
                 authenticate and every request would be refused. Mint one with `cameodb keygen \
                 --role admin`, or set enabled = false"
                    .to_string(),
            ),
            Ok(ring) => {
                // Two things a working configuration can still be wrong about. Both are
                // reported as clauses on one line rather than as separate rules, because
                // they describe the same subject — the keys this node holds.
                let mut notes = Vec::new();
                if !ring.holds(Capability::Write) && !ring.holds(Capability::IndexAdmin) {
                    notes.push("no key holds write or index-admin, so this node is read-only");
                }
                let scoped = ring.index_scoped_count();
                let scoped_note;
                if scoped > 0 {
                    scoped_note = format!(
                        "{} index-scoped key(s) are refused at /mcp until per-tool scoping \
                         lands (ROADMAP Phase 14 Stage B1 step 4)",
                        scoped
                    );
                    notes.push(&scoped_note);
                }

                let message = if notes.is_empty() {
                    format!("{} enforced on every route", ring.summary())
                } else {
                    format!("{} enforced; {}", ring.summary(), notes.join("; "))
                };
                if notes.is_empty() {
                    Outcome::Pass(message)
                } else {
                    Outcome::Warn(message)
                }
            }
        },
    );

    // --- Resource ceilings ------------------------------------------------------
    // A DoS ceiling is concurrency × body size; either alone says little.
    let worst_case_mb = config
        .effective_max_body_size_mb()
        .saturating_mul(http.max_concurrent_requests);
    let ceiling_mb = match profile {
        Profile::Local => usize::MAX,
        Profile::Internal => 64 * 1024,
        Profile::External => 16 * 1024,
    };
    push(
        "limits",
        if worst_case_mb > ceiling_mb {
            Outcome::Fail(format!(
                "max_concurrent_requests ({}) × body limit ({} MB) allows {} MB of in-flight \
                 request data, over the {} MB ceiling for profile '{}'. Lower \
                 max_concurrent_requests or max_record_size_mb",
                http.max_concurrent_requests,
                config.effective_max_body_size_mb(),
                worst_case_mb,
                ceiling_mb,
                profile
            ))
        } else {
            Outcome::Pass(format!(
                "{} concurrent × {} MB body = {} MB worst case in flight",
                http.max_concurrent_requests,
                config.effective_max_body_size_mb(),
                worst_case_mb
            ))
        },
    );

    Ok(Posture {
        profile,
        inferred,
        checks,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config_for(profile: Option<Profile>, bind: &str) -> CameoDbConfig {
        let mut c = CameoDbConfig::default();
        c.node.profile = profile;
        c.network.http.bind_address = bind.to_string();
        c
    }

    #[test]
    fn local_is_inferred_only_for_loopback() {
        let posture = evaluate(&config_for(None, "127.0.0.1")).unwrap();
        assert_eq!(posture.profile, Profile::Local);
        assert!(posture.inferred);

        // The important half: a reachable node must declare its posture rather than
        // silently receiving the most permissive one.
        let err = evaluate(&config_for(None, "0.0.0.0")).unwrap_err();
        assert!(err.contains("no security profile declared"), "{}", err);
    }

    #[test]
    fn local_rejects_non_loopback_bind() {
        let posture = evaluate(&config_for(Some(Profile::Local), "0.0.0.0")).unwrap();
        let bind = posture.checks.iter().find(|c| c.rule == "bind").unwrap();
        assert!(bind.outcome.is_fail(), "{:?}", bind);
    }

    #[test]
    fn wildcard_cors_allowed_only_when_local() {
        let mut local = config_for(Some(Profile::Local), "127.0.0.1");
        local.network.http.cors_allowed_origins = vec!["*".to_string()];
        let cors = |c: &CameoDbConfig| {
            evaluate(c)
                .unwrap()
                .checks
                .into_iter()
                .find(|k| k.rule == "cors")
                .unwrap()
                .outcome
        };
        assert!(matches!(cors(&local), Outcome::Warn(_)));

        let mut internal = config_for(Some(Profile::Internal), "0.0.0.0");
        internal.network.http.cors_allowed_origins = vec!["*".to_string()];
        assert!(cors(&internal).is_fail());
    }

    #[test]
    fn empty_cors_is_a_pass_not_an_error() {
        let mut c = config_for(Some(Profile::Internal), "0.0.0.0");
        c.network.http.cors_allowed_origins = vec![];
        let cors = evaluate(&c)
            .unwrap()
            .checks
            .into_iter()
            .find(|k| k.rule == "cors")
            .unwrap();
        assert!(matches!(cors.outcome, Outcome::Pass(_)), "{:?}", cors);
    }

    #[test]
    fn internal_requires_psk_when_clustered() {
        let mut c = config_for(Some(Profile::Internal), "0.0.0.0");
        c.network.cluster.enabled = true;
        let psk = |c: &CameoDbConfig| {
            evaluate(c)
                .unwrap()
                .checks
                .into_iter()
                .find(|k| k.rule == "cluster_psk")
                .unwrap()
                .outcome
        };
        assert!(psk(&c).is_fail());

        c.network.cluster.psk = Some("a".repeat(64));
        assert!(matches!(psk(&c), Outcome::Pass(_)));
    }

    #[test]
    fn external_requires_tls_admin_off_and_blocks_on_missing_auth() {
        let mut c = config_for(Some(Profile::External), "0.0.0.0");
        c.network.http.cors_allowed_origins = vec![];
        let posture = evaluate(&c).unwrap();
        let failed: Vec<&str> = posture.failures().map(|c| c.rule).collect();
        assert!(failed.contains(&"tls"), "{:?}", failed);
        assert!(failed.contains(&"admin_api"), "{:?}", failed);
        assert!(failed.contains(&"auth"), "{:?}", failed);
    }

    /// The `auth` outcome for a config, by rule name.
    fn auth_outcome(config: &CameoDbConfig) -> Outcome {
        evaluate(config)
            .unwrap()
            .checks
            .into_iter()
            .find(|c| c.rule == "auth")
            .unwrap()
            .outcome
    }

    fn with_key(config: &mut CameoDbConfig, role: crate::auth::Role) {
        config.security.enabled = true;
        config.security.api_keys.push(crate::auth::ApiKeyConfig {
            key_hash: Some(format!("sha256:{}", "ab".repeat(32))),
            role: Some(role),
            label: Some(format!("{role}-key")),
            ..Default::default()
        });
    }

    #[test]
    fn unauthenticated_loopback_passes_but_a_reachable_node_does_not() {
        // Local passes rather than warns, deliberately: a profile that warns on every boot
        // trains operators to stop reading warnings.
        assert!(matches!(
            auth_outcome(&config_for(Some(Profile::Local), "127.0.0.1")),
            Outcome::Pass(_)
        ));

        let mut internal = config_for(Some(Profile::Internal), "0.0.0.0");
        internal.network.http.cors_allowed_origins = vec![];
        assert!(matches!(auth_outcome(&internal), Outcome::Warn(_)));

        let mut external = config_for(Some(Profile::External), "0.0.0.0");
        external.network.http.cors_allowed_origins = vec![];
        assert!(auth_outcome(&external).is_fail());
    }

    #[test]
    fn auth_enabled_without_a_key_refuses_to_start() {
        // Every request would be rejected. Failing here is louder than failing per request.
        let mut c = config_for(Some(Profile::Local), "127.0.0.1");
        c.security.enabled = true;
        let outcome = auth_outcome(&c);
        assert!(outcome.is_fail(), "{:?}", outcome);
        assert!(outcome.message().contains("no keys"), "{:?}", outcome);
    }

    #[test]
    fn a_broken_security_section_fails_the_rule_rather_than_being_reported_as_keys() {
        let mut c = config_for(Some(Profile::Local), "127.0.0.1");
        c.security.enabled = true;
        c.security.api_keys.push(crate::auth::ApiKeyConfig {
            key_hash: Some("not-a-digest".to_string()),
            role: Some(crate::auth::Role::Admin),
            ..Default::default()
        });
        let outcome = auth_outcome(&c);
        assert!(outcome.is_fail(), "{:?}", outcome);
        assert!(outcome.message().contains("unusable"), "{:?}", outcome);
    }

    #[test]
    fn configured_keys_pass_and_unblock_the_external_profile() {
        let mut c = config_for(Some(Profile::Local), "127.0.0.1");
        with_key(&mut c, crate::auth::Role::Admin);
        let outcome = auth_outcome(&c);
        assert!(matches!(outcome, Outcome::Pass(_)), "{:?}", outcome);
        assert!(
            outcome.message().contains("1 key (1 admin)"),
            "{:?}",
            outcome
        );

        // `external` was blocked on this rule from the day the profile existed. With keys,
        // TLS and the admin API off, nothing is left to block it.
        let mut external = config_for(Some(Profile::External), "0.0.0.0");
        external.network.http.cors_allowed_origins = vec![];
        external.network.http.admin_enabled = false;
        external.network.http.tls.enabled = true;
        with_key(&mut external, crate::auth::Role::Admin);
        let posture = evaluate(&external).unwrap();
        assert_eq!(posture.failures().count(), 0, "{}", posture.render());
    }

    #[test]
    fn a_read_only_key_ring_is_called_out() {
        let mut c = config_for(Some(Profile::Local), "127.0.0.1");
        with_key(&mut c, crate::auth::Role::Reader);
        let outcome = auth_outcome(&c);
        assert!(matches!(outcome, Outcome::Warn(_)), "{:?}", outcome);
        assert!(outcome.message().contains("read-only"), "{:?}", outcome);
    }

    #[test]
    fn an_index_scoped_key_is_reported_because_mcp_refuses_it() {
        // Surprising behaviour has to be discoverable before an agent is handed the key,
        // not after its first MCP call is refused.
        let mut c = config_for(Some(Profile::Local), "127.0.0.1");
        c.security.enabled = true;
        c.security.api_keys.push(crate::auth::ApiKeyConfig {
            key_hash: Some(format!("sha256:{}", "cd".repeat(32))),
            role: Some(crate::auth::Role::Admin),
            allowed_indexes: Some(vec!["docs".to_string()]),
            ..Default::default()
        });
        let outcome = auth_outcome(&c);
        assert!(matches!(outcome, Outcome::Warn(_)), "{:?}", outcome);
        assert!(outcome.message().contains("/mcp"), "{:?}", outcome);
    }

    #[test]
    fn an_admin_key_settles_the_off_box_admin_api_warning() {
        let admin_api = |c: &CameoDbConfig| {
            evaluate(c)
                .unwrap()
                .checks
                .into_iter()
                .find(|k| k.rule == "admin_api")
                .unwrap()
                .outcome
        };
        let mut c = config_for(Some(Profile::Internal), "0.0.0.0");
        c.network.http.cors_allowed_origins = vec![];
        c.network.http.admin_enabled = true;
        assert!(matches!(admin_api(&c), Outcome::Warn(_)));

        with_key(&mut c, crate::auth::Role::Admin);
        assert!(
            matches!(admin_api(&c), Outcome::Pass(_)),
            "{:?}",
            admin_api(&c)
        );
    }

    #[test]
    fn plaintext_warning_names_the_keys_once_there_are_any() {
        let tls = |c: &CameoDbConfig| {
            evaluate(c)
                .unwrap()
                .checks
                .into_iter()
                .find(|k| k.rule == "tls")
                .unwrap()
                .outcome
        };
        let mut c = config_for(Some(Profile::Internal), "0.0.0.0");
        c.network.http.cors_allowed_origins = vec![];
        assert!(!tls(&c).message().contains("API key"));

        with_key(&mut c, crate::auth::Role::Writer);
        assert!(tls(&c).message().contains("API key"), "{:?}", tls(&c));
    }

    #[test]
    fn limits_ceiling_catches_concurrency_times_body_size() {
        let mut c = config_for(Some(Profile::Internal), "0.0.0.0");
        c.network.http.cors_allowed_origins = vec![];
        // 128 × (512 + 64) MB = 73 728 MB, past the 64 GB internal ceiling.
        c.max_record_size_mb = 512;
        c.network.http.max_concurrent_requests = 128;
        let limits = evaluate(&c)
            .unwrap()
            .checks
            .into_iter()
            .find(|k| k.rule == "limits")
            .unwrap();
        assert!(limits.outcome.is_fail(), "{:?}", limits);
    }

    #[test]
    fn loopback_detection_does_not_resolve_dns() {
        assert!(is_loopback_bind("127.0.0.1"));
        assert!(is_loopback_bind("::1"));
        assert!(is_loopback_bind("localhost"));
        assert!(!is_loopback_bind("0.0.0.0"));
        assert!(!is_loopback_bind("::"));
        assert!(!is_loopback_bind("10.0.0.5"));
        // Would resolve to loopback on many machines; still treated as reachable so the
        // posture cannot change meaning between restarts.
        assert!(!is_loopback_bind("my-host.local"));
    }

    #[test]
    fn default_config_starts_clean_on_loopback() {
        let mut c = CameoDbConfig::default();
        c.network.http.bind_address = "127.0.0.1".to_string();
        let posture = evaluate(&c).unwrap();
        assert_eq!(posture.failures().count(), 0, "{}", posture.render());
    }
}
