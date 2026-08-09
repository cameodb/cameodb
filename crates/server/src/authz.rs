//! Authorization at the HTTP ingress: who may call which route.
//!
//! The split from [`crate::auth`] is deliberate. That module answers *who you are* — key
//! format, digests, roles, the key ring. This one answers *what you may do*, and it is the
//! only place that answer is computed: one table, one middleware, in front of the router.
//!
//! **Deny by default.** [`classify`] maps a (method, path) pair to a requirement, and a path
//! it does not recognise requires a key like everything else. A handler cannot forget to
//! check, because no handler checks — a request that reaches one has already been cleared.
//!
//! Two consequences of running before routing, both accepted on purpose:
//!
//! - An unknown path answers **401 without a key and 404 with one**. Path-existence probing
//!   therefore tells an anonymous caller nothing, which is worth more than the tidiness of a
//!   404 for everyone.
//! - A rejected request is refused before its body is read, so hyper drops the connection
//!   rather than reusing it. The alternative is buffering the body of requests that were
//!   never going to be served, which is the thing an unauthenticated flood wants.

use std::sync::Arc;

use axum::{
    Json,
    extract::{Request, State},
    http::{HeaderMap, StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
};
use tracing::{debug, warn};

use cameodb_mcp::server::{McpAuthz, McpAuthzRef, McpCapability};

use crate::auth::{Capability, KeyEntry, KeyRing};

/// What a route requires of its caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Access {
    /// Answerable without a key. Presenting one is still honored — `/_cluster/health`
    /// returns more to a caller it can identify.
    Public,
    /// Needs this capability. The pattern's `{index}` segment, if it has one, is checked
    /// against the caller's index scope.
    Needs(Capability),
    /// The MCP endpoint: `Read` at the door, and per-tool checks inside it.
    ///
    /// A single JSON-RPC path cannot be classified per operation from the outside, so the
    /// capability a tool needs and the index it names are decided in the dispatcher, from
    /// the identity this middleware attaches. Everything below the door is
    /// [`cameodb_mcp::server::tool_capability`] and [`McpAuthz`].
    Mcp,
}

/// One row of the route table.
struct RouteRule {
    method: &'static str,
    /// Path pattern. `{index}` matches exactly one segment and marks the route as
    /// index-scoped — there is no separate flag to fall out of step with the pattern.
    pattern: &'static str,
    access: Access,
}

const fn rule(method: &'static str, pattern: &'static str, access: Access) -> RouteRule {
    RouteRule {
        method,
        pattern,
        access,
    }
}

/// Every route this server serves, and what it requires.
///
/// Transcribed from `create_router` and guarded by `every_mounted_route_is_classified`
/// below, which reads the router's own source. A hand-maintained matrix cannot promise that
/// a new route arrives classified; a test that fails the build can.
#[rustfmt::skip]
const ROUTES: &[RouteRule] = &[
    // Liveness. Public so a load balancer needs no credential, but the body it gets back is
    // the minimal one — see `health_handler`.
    rule("GET",    "/_cluster/health",                  Access::Public),

    // Read
    rule("POST",   "/api/{index}/search",               Access::Needs(Capability::Read)),
    rule("POST",   "/api/{index}/search/stream",        Access::Needs(Capability::Read)),
    rule("GET",    "/api/{index}/_config",              Access::Needs(Capability::Read)),
    rule("GET",    "/_indexes",                         Access::Needs(Capability::Read)),
    rule("GET",    "/_cluster/_indexes",                Access::Needs(Capability::Read)),

    // Write
    rule("PUT",    "/api/{index}/document",             Access::Needs(Capability::Write)),
    rule("POST",   "/api/{index}/document/stream",      Access::Needs(Capability::Write)),
    rule("POST",   "/api/{index}/_bulk",                Access::Needs(Capability::Write)),

    // Index administration
    rule("PUT",    "/api/{index}/_config",              Access::Needs(Capability::IndexAdmin)),
    rule("PATCH",  "/api/{index}/_schema",              Access::Needs(Capability::IndexAdmin)),
    rule("DELETE", "/api/{index}",                      Access::Needs(Capability::IndexAdmin)),

    // Node administration. Mounted only when `admin_enabled`; classified either way, so
    // turning the API back on cannot turn it on unguarded.
    rule("GET",    "/_admin/memory",                    Access::Needs(Capability::NodeAdmin)),
    rule("POST",   "/_admin/memory/purge",              Access::Needs(Capability::NodeAdmin)),
    rule("GET",    "/_admin/workers",                   Access::Needs(Capability::NodeAdmin)),
    rule("POST",   "/_admin/index/{index}/commit",      Access::Needs(Capability::NodeAdmin)),
    rule("POST",   "/_admin/index/{index}/evict-writer",Access::Needs(Capability::NodeAdmin)),

    // MCP. Streamable HTTP uses one path for all three verbs; the GET (SSE stream) and
    // DELETE (end session) are gated too, not only the POST that carries the payload.
    rule("POST",   "/mcp",                              Access::Mcp),
    rule("GET",    "/mcp",                              Access::Mcp),
    rule("DELETE", "/mcp",                              Access::Mcp),
    rule("GET",    "/mcp/sse",                          Access::Mcp),
    rule("POST",   "/mcp/sse",                          Access::Mcp),
    rule("POST",   "/mcp/messages",                     Access::Mcp),
];

/// A classified request: what it needs, and which index it names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Classified<'a> {
    pub access: Access,
    /// The `{index}` segment, when the matched pattern has one.
    pub index: Option<&'a str>,
}

/// Trailing slashes are stripped before matching, so `/mcp/` and `/mcp` classify alike.
/// Normalising can only make a path match a rule it otherwise would not, never the reverse,
/// so it cannot open a hole — an unroutable variant simply gets its 404 after authenticating
/// rather than before.
fn normalize(path: &str) -> &str {
    let trimmed = path.trim_end_matches('/');
    if trimmed.is_empty() { "/" } else { trimmed }
}

/// Match a request against the route table.
///
/// `None` means no route claims it, which is a deny, not a pass.
pub fn classify<'a>(method: &str, path: &'a str) -> Option<Classified<'a>> {
    let path = normalize(path);
    for rule in ROUTES {
        if rule.method != method {
            continue;
        }
        if let Some(index) = match_pattern(rule.pattern, path) {
            return Some(Classified {
                access: rule.access,
                index,
            });
        }
    }
    None
}

/// `Some(index)` when `path` matches `pattern`; the inner `Option` is the `{index}` segment.
fn match_pattern<'a>(pattern: &str, path: &'a str) -> Option<Option<&'a str>> {
    let mut pattern_segments = pattern.split('/');
    let mut path_segments = path.split('/');
    let mut index = None;

    loop {
        match (pattern_segments.next(), path_segments.next()) {
            (None, None) => return Some(index),
            (Some(expected), Some(actual)) => {
                if expected == "{index}" {
                    if actual.is_empty() {
                        return None;
                    }
                    index = Some(actual);
                } else if expected != actual {
                    return None;
                }
            }
            _ => return None,
        }
    }
}

/// Who the caller is, as decided by the middleware and read by handlers.
///
/// Carried as a request extension rather than looked up again, so there is exactly one
/// place a request's identity is established.
#[derive(Debug, Clone)]
pub enum Authz {
    /// `[security] enabled = false`. No identity exists and nothing was checked.
    Disabled,
    /// Reached a public route without presenting a key.
    Anonymous,
    /// Authenticated as this key.
    Key(Arc<KeyEntry>),
}

impl Authz {
    /// True when the caller is either identified or on a node that does not identify anyone.
    ///
    /// The question handlers actually ask: "may this response say more than the minimum?"
    pub fn is_identified(&self) -> bool {
        !matches!(self, Authz::Anonymous)
    }

    /// The `key_id` to attribute an action to, if there is one.
    pub fn key_id(&self) -> Option<String> {
        match self {
            Authz::Key(entry) => Some(entry.key_id()),
            _ => None,
        }
    }
}

/// The same identity, in the vocabulary the mcp crate understands.
///
/// `/mcp` is one path, so the middleware can only check `Read` at the door; which tool and
/// which index are visible solely to the dispatcher. This is the one place the two
/// capability vocabularies meet, which is why the mapping is exhaustive rather than a
/// catch-all — a capability added to either side has to be mapped here to compile.
impl McpAuthz for Authz {
    fn key_id(&self) -> Option<String> {
        Authz::key_id(self)
    }

    fn allows_index(&self, index: &str) -> bool {
        match self {
            Authz::Disabled => true,
            // The MCP door requires `Read`, so an anonymous caller never gets here. Denying
            // anyway costs nothing and means the guarantee does not depend on that argument
            // staying true.
            Authz::Anonymous => false,
            Authz::Key(entry) => entry.allows_index(index),
        }
    }

    fn has(&self, capability: McpCapability) -> bool {
        let capability = match capability {
            McpCapability::Read => Capability::Read,
            McpCapability::Write => Capability::Write,
            McpCapability::IndexAdmin => Capability::IndexAdmin,
            McpCapability::NodeAdmin => Capability::NodeAdmin,
        };
        match self {
            // Auth off: nothing to enforce.
            Authz::Disabled => true,
            Authz::Anonymous => false,
            Authz::Key(entry) => entry.has(capability),
        }
    }
}

/// Remove indexes the caller may not see from a listing response, in place.
///
/// Named access is refused by [`decide`] before a handler runs; this is the other half —
/// enumeration. Without it a key scoped to one index still learns every index name, and the
/// names alone (`payroll`, `incidents-2026`) are worth having.
///
/// The two listing shapes differ: `/_indexes` names an entry `name`, the MCP catalog names
/// it `index`. Both are accepted, and an entry with **neither** is dropped — a listing whose
/// shape has changed underneath this function must lose entries, not leak them.
pub fn filter_index_listing(value: &mut serde_json::Value, authz: &Authz) {
    match authz {
        // Nothing to enforce: no key means no scope.
        Authz::Disabled => {}
        // Deny, not "unrestricted". No listing route is `Public`, so this is unreachable
        // today — which is exactly why it must not be the permissive branch. The day one is
        // made public, an anonymous caller gets an empty list rather than the catalogue.
        Authz::Anonymous => retain_indexes(value, &|_| false),
        Authz::Key(entry) if !entry.is_index_scoped() => {}
        Authz::Key(entry) => retain_indexes(value, &|index| entry.allows_index(index)),
    }
}

/// The same filtering for an MCP caller.
///
/// Two entry points rather than one because the two sides carry identity in different types:
/// HTTP handlers hold an [`Authz`], the MCP backend holds the trait object the mcp crate
/// gave it. Both narrow to the same predicate over the same response shapes.
pub fn retain_visible_indexes(value: &mut serde_json::Value, authz: &dyn McpAuthz) {
    retain_indexes(value, &|index| authz.allows_index(index));
}

fn retain_indexes(value: &mut serde_json::Value, allowed: &dyn Fn(&str) -> bool) {
    let Some(object) = value.as_object_mut() else {
        return;
    };

    if let Some(indexes) = object.get_mut("indexes").and_then(|v| v.as_array_mut()) {
        indexes.retain(|item| entry_index_name(item).is_some_and(allowed));
        let remaining = indexes.len();
        // The count has to follow the list. A response saying "4 indexes" above a list of
        // one is both wrong and a disclosure of the number withheld.
        if object.contains_key("total_indexes") {
            object.insert("total_indexes".to_string(), remaining.into());
        }
    }

    // The cluster listing repeats the names under each node, with its own count.
    if let Some(nodes) = object.get_mut("nodes").and_then(|v| v.as_array_mut()) {
        for node in nodes.iter_mut() {
            retain_indexes(node, allowed);
        }
    }
}

fn entry_index_name(item: &serde_json::Value) -> Option<&str> {
    item.get("name")
        .or_else(|| item.get("index"))
        .and_then(|value| value.as_str())
}

/// A refusal, rendered the way the rest of the API renders errors.
#[derive(Debug)]
struct Refusal {
    status: StatusCode,
    error: &'static str,
    message: String,
}

impl Refusal {
    fn unauthorized(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            error: "Unauthorized",
            message: message.into(),
        }
    }

    fn forbidden(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::FORBIDDEN,
            error: "Forbidden",
            message: message.into(),
        }
    }
}

impl IntoResponse for Refusal {
    fn into_response(self) -> Response {
        let body = Json(serde_json::json!({
            "error": self.error,
            "message": self.message,
        }));
        if self.status == StatusCode::UNAUTHORIZED {
            // Names the scheme, so a client knows what to send rather than guessing.
            (
                self.status,
                [(header::WWW_AUTHENTICATE, "Bearer realm=\"cameodb\"")],
                body,
            )
                .into_response()
        } else {
            (self.status, body).into_response()
        }
    }
}

/// Extract the token from `Authorization: Bearer <key>`.
///
/// Header only. A key in a query parameter lands in access logs, browser history, and
/// `Referer` headers on every outbound link, so there is deliberately no way to send one.
fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    let value = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
    let (scheme, token) = value.split_once(' ')?;
    scheme
        .eq_ignore_ascii_case("bearer")
        .then(|| token.trim())
        .filter(|token| !token.is_empty())
}

/// Decide a single request. Split out from the layer so it is testable without a server.
/// How many unauthenticated requests have been refused since startup.
///
/// Process-wide rather than threaded through the middleware state: there is one ingress per
/// process, and the thing being bounded is log volume, which is also process-wide.
static UNAUTHENTICATED_REFUSALS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Whether the `n`th refusal gets a line of its own.
///
/// An unauthenticated request is cheap to send and, until now, wrote a `warn!` each time —
/// so anyone who could reach the port could fill the disk with a loop. Logging the first few,
/// then powers of two, then every hundred thousand keeps the signal (something is probing)
/// and drops the volume: a million attempts produce about thirty lines instead of a million.
/// The periodic floor exists because powers of two alone go silent for ever-longer stretches
/// — at a billion the next one is a billion away.
///
/// Only the *unauthenticated* refusals are thinned. A 403 needs a valid key first, so its
/// volume is bounded by someone who already holds credentials — which is precisely the event
/// worth keeping one line per.
fn should_log_refusal(n: u64) -> bool {
    n <= 3 || n.is_power_of_two() || n.is_multiple_of(100_000)
}

/// Count a refusal and report the running total when this one is worth a line.
fn note_unauthenticated_refusal() -> Option<u64> {
    let count = UNAUTHENTICATED_REFUSALS.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
    should_log_refusal(count).then_some(count)
}

fn decide(
    keyring: &KeyRing,
    method: &str,
    path: &str,
    headers: &HeaderMap,
) -> Result<Authz, Refusal> {
    if !keyring.enabled() {
        return Ok(Authz::Disabled);
    }

    let classified = classify(method, path);

    let Some(token) = bearer_token(headers) else {
        // Nothing presented. Public routes are answerable; everything else — including
        // paths that do not exist — is refused without revealing which is which.
        return match classified.as_ref().map(|c| c.access) {
            Some(Access::Public) => Ok(Authz::Anonymous),
            _ => {
                if let Some(total) = note_unauthenticated_refusal() {
                    warn!(
                        %method, %path, refused_so_far = total,
                        "auth: refused a request carrying no API key"
                    );
                }
                Err(Refusal::unauthorized(
                    "this endpoint requires an API key: Authorization: Bearer <key>",
                ))
            }
        };
    };

    let Some(entry) = keyring.authenticate(token) else {
        if let Some(total) = note_unauthenticated_refusal() {
            warn!(
                %method, %path, refused_so_far = total,
                "auth: rejected an unrecognised API key"
            );
        }
        return Err(Refusal::unauthorized(
            "the API key presented is not recognised",
        ));
    };

    // Authenticated. An unclassified path now falls through to the router, which answers
    // 404 — the caller has earned an honest answer about what exists.
    let Some(classified) = classified else {
        return Ok(Authz::Key(entry));
    };

    let required = match classified.access {
        Access::Public => return Ok(Authz::Key(entry)),
        Access::Needs(capability) => capability,
        Access::Mcp => Capability::Read,
    };

    if !entry.has(required) {
        warn!(
            key_id = %entry.key_id(), label = %entry.label(), role = %entry.role(),
            %method, %path, required = required.as_str(),
            "auth: refused, role does not hold the required capability"
        );
        return Err(Refusal::forbidden(format!(
            "role '{}' does not hold the '{}' capability this endpoint requires",
            entry.role(),
            required.as_str()
        )));
    }

    if let Some(index) = classified.index
        && !entry.allows_index(index)
    {
        // Named access gets an honest answer. Enumeration does not — `/_indexes` filters
        // rather than refusing, so a scoped key cannot map what it is not allowed to read.
        warn!(
            key_id = %entry.key_id(), label = %entry.label(),
            %method, %path, %index,
            "auth: refused, index is outside this key's scope"
        );
        return Err(Refusal::forbidden(format!(
            "this key is not permitted on index '{}'",
            index
        )));
    }

    Ok(Authz::Key(entry))
}

/// The authentication and authorization layer.
///
/// Placed inside CORS and outside the timeout, the concurrency guard, and both body limits.
/// Inside CORS so a browser preflight — which never carries `Authorization` — still gets its
/// headers; outside the rest so a flood of unauthenticated requests takes no concurrency
/// permit and has no body buffered on its behalf.
pub async fn authorize(
    State(keyring): State<Arc<KeyRing>>,
    mut req: Request,
    next: Next,
) -> Response {
    let method = req.method().as_str().to_string();
    let path = req.uri().path().to_string();
    match decide(&keyring, &method, &path, req.headers()) {
        Ok(authz) => {
            // Off at the default level, and the first thing worth turning on when an
            // operator asks who called what. The `key_id` is the whole point of minting one:
            // it ties a request to a key without the key appearing anywhere.
            debug!(key_id = ?authz.key_id(), %method, %path, "auth: authorized");
            // Every request that reaches a handler carries its identity, including the
            // anonymous and auth-disabled cases. A handler asking "who is this?" always gets
            // an answer, so none of them has to guess what a missing extension means.
            //
            // MCP needs the same identity behind a trait the mcp crate owns, since it cannot
            // see this one. Allocated only for the routes that can use it.
            if matches!(
                classify(&method, &path).map(|c| c.access),
                Some(Access::Mcp)
            ) {
                let handle: McpAuthzRef = Arc::new(authz.clone());
                req.extensions_mut().insert(handle);
            }
            req.extensions_mut().insert(authz);
            next.run(req).await
        }
        Err(refusal) => refusal.into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::{ApiKey, ApiKeyConfig, Role, SecurityConfig};

    /// Every `.route("…")` literal in `source`, in the order they are mounted.
    fn mounted_routes(source: &str) -> Vec<String> {
        let mut found = Vec::new();
        for fragment in source.split(".route(").skip(1) {
            let trimmed = fragment.trim_start();
            // `.route(HEALTH_PATH, …)` names a constant rather than a literal; it is
            // asserted separately below so this stays a check and not a guess.
            let Some(rest) = trimmed.strip_prefix('"') else {
                continue;
            };
            if let Some(end) = rest.find('"') {
                found.push(rest[..end].to_string());
            }
        }
        found
    }

    fn is_classified(pattern: &str) -> bool {
        ROUTES.iter().any(|rule| rule.pattern == pattern)
    }

    #[test]
    fn every_mounted_route_is_classified() {
        // The guarantee the route table cannot make for itself: a route added to the router
        // without a row here fails the build instead of shipping open.
        let router_source = include_str!("http_server.rs");
        let unclassified: Vec<String> = mounted_routes(router_source)
            .into_iter()
            .filter(|pattern| !is_classified(pattern))
            .collect();
        assert!(
            unclassified.is_empty(),
            "these routes are mounted but have no row in ROUTES: {:?}",
            unclassified
        );

        // The one route mounted through a constant.
        assert!(router_source.contains(".route(HEALTH_PATH"));
        assert!(is_classified(crate::http_server::HEALTH_PATH));

        // ...and every literal plus that constant accounts for every `.route(` call, so a
        // second constant-named route cannot slip past the filter above.
        let literal_count = mounted_routes(router_source).len();
        let call_count = router_source.matches(".route(").count();
        assert_eq!(
            call_count,
            literal_count + 1,
            "a route is mounted through a name this test does not know about"
        );
    }

    #[test]
    fn every_mcp_route_is_classified_under_its_mount_point() {
        // MCP routes live in another crate and are nested, which is exactly why they are the
        // ones a reviewer forgets. Assert the prefix here rather than assuming it.
        assert!(include_str!("http_server.rs").contains(".nest(\"/mcp\""));
        for pattern in mounted_routes(include_str!("../../mcp/src/server.rs")) {
            let mounted = format!("/mcp{}", pattern.trim_end_matches('/'));
            assert!(
                is_classified(&mounted),
                "MCP route {mounted} has no row in ROUTES"
            );
        }
    }

    #[test]
    fn no_row_classifies_a_route_that_is_not_mounted() {
        // The reverse direction: a stale row is a rule nothing enforces, and a reader would
        // take it for coverage.
        let mut mounted: Vec<String> = mounted_routes(include_str!("http_server.rs"));
        mounted.push(crate::http_server::HEALTH_PATH.to_string());
        mounted.extend(
            mounted_routes(include_str!("../../mcp/src/server.rs"))
                .into_iter()
                .map(|p| format!("/mcp{}", p.trim_end_matches('/'))),
        );
        for rule in ROUTES {
            assert!(
                mounted.iter().any(|m| m == rule.pattern),
                "ROUTES classifies {} {}, which no router mounts",
                rule.method,
                rule.pattern
            );
        }
    }

    fn ring(entries: Vec<ApiKeyConfig>) -> KeyRing {
        SecurityConfig {
            enabled: true,
            api_keys: entries,
            override_key: None,
            ..Default::default()
        }
        .load_keyring()
        .unwrap()
    }

    fn key_for(role: Role, indexes: Option<Vec<&str>>) -> (ApiKey, ApiKeyConfig) {
        let key = ApiKey::generate().unwrap();
        let config = ApiKeyConfig {
            key_hash: Some(key.digest().to_config_value()),
            role: Some(role),
            label: Some(format!("{role}-key")),
            allowed_indexes: indexes
                .map(|list| list.into_iter().map(str::to_string).collect::<Vec<_>>()),
            key_hash_file: None,
        };
        (key, config)
    }

    fn headers_with(key: Option<&ApiKey>) -> HeaderMap {
        let mut headers = HeaderMap::new();
        if let Some(key) = key {
            headers.insert(
                header::AUTHORIZATION,
                format!("Bearer {}", key.expose()).parse().unwrap(),
            );
        }
        headers
    }

    fn status_of(result: &Result<Authz, Refusal>) -> Option<StatusCode> {
        result.as_ref().err().map(|r| r.status)
    }

    #[test]
    fn a_disabled_key_ring_authorizes_everything() {
        let ring = SecurityConfig::default().load_keyring().unwrap();
        let decision = decide(&ring, "DELETE", "/api/anything", &HeaderMap::new());
        assert!(matches!(decision, Ok(Authz::Disabled)));
    }

    #[test]
    fn every_classified_route_refuses_a_bare_request() {
        let ring = ring(vec![key_for(Role::Admin, None).1]);
        for rule in ROUTES {
            let path = rule.pattern.replace("{index}", "probe");
            let decision = decide(&ring, rule.method, &path, &HeaderMap::new());
            match rule.access {
                Access::Public => assert!(
                    matches!(decision, Ok(Authz::Anonymous)),
                    "{} {} should be answerable without a key",
                    rule.method,
                    rule.pattern
                ),
                _ => assert_eq!(
                    status_of(&decision),
                    Some(StatusCode::UNAUTHORIZED),
                    "{} {} answered a bare request",
                    rule.method,
                    rule.pattern
                ),
            }
        }
    }

    #[test]
    fn an_unknown_path_is_401_without_a_key_and_falls_through_with_one() {
        let (key, config) = key_for(Role::Reader, None);
        let ring = ring(vec![config]);

        // Anonymous probing learns nothing about what exists...
        assert_eq!(
            status_of(&decide(
                &ring,
                "GET",
                "/api/secret/_internal",
                &HeaderMap::new()
            )),
            Some(StatusCode::UNAUTHORIZED)
        );
        // ...while an authenticated caller is handed to the router, which 404s honestly.
        assert!(matches!(
            decide(
                &ring,
                "GET",
                "/api/secret/_internal",
                &headers_with(Some(&key))
            ),
            Ok(Authz::Key(_))
        ));
    }

    #[test]
    fn an_unrecognised_key_is_401_not_403() {
        let ring = ring(vec![key_for(Role::Admin, None).1]);
        let stranger = ApiKey::generate().unwrap();
        assert_eq!(
            status_of(&decide(
                &ring,
                "GET",
                "/_indexes",
                &headers_with(Some(&stranger))
            )),
            Some(StatusCode::UNAUTHORIZED)
        );
    }

    #[test]
    fn each_role_reaches_exactly_the_capabilities_it_holds() {
        let (reader, reader_config) = key_for(Role::Reader, None);
        let (writer, writer_config) = key_for(Role::Writer, None);
        let (admin, admin_config) = key_for(Role::Admin, None);
        let ring = ring(vec![reader_config, writer_config, admin_config]);

        let allowed = |key: &ApiKey, method: &str, path: &str| {
            decide(&ring, method, path, &headers_with(Some(key))).is_ok()
        };
        let refused = |key: &ApiKey, method: &str, path: &str| {
            status_of(&decide(&ring, method, path, &headers_with(Some(key))))
                == Some(StatusCode::FORBIDDEN)
        };

        // One route per capability class, checked from every side.
        assert!(allowed(&reader, "POST", "/api/docs/search"));
        assert!(refused(&reader, "PUT", "/api/docs/document"));
        assert!(refused(&reader, "DELETE", "/api/docs"));
        assert!(refused(&reader, "GET", "/_admin/memory"));

        assert!(allowed(&writer, "PUT", "/api/docs/document"));
        assert!(allowed(&writer, "POST", "/api/docs/_bulk"));
        assert!(refused(&writer, "PATCH", "/api/docs/_schema"));
        assert!(refused(&writer, "POST", "/_admin/memory/purge"));

        assert!(allowed(&admin, "DELETE", "/api/docs"));
        assert!(allowed(&admin, "GET", "/_admin/workers"));
        assert!(allowed(&admin, "POST", "/_admin/index/docs/evict-writer"));
    }

    #[test]
    fn an_index_scope_holds_on_every_route_that_names_an_index() {
        let (key, config) = key_for(Role::Admin, Some(vec!["docs", "wiki"]));
        let ring = ring(vec![config]);

        for rule in ROUTES {
            if !rule.pattern.contains("{index}") {
                continue;
            }
            let permitted = rule.pattern.replace("{index}", "docs");
            let denied = rule.pattern.replace("{index}", "payroll");
            assert!(
                decide(&ring, rule.method, &permitted, &headers_with(Some(&key))).is_ok(),
                "{} {} refused an in-scope index",
                rule.method,
                rule.pattern
            );
            assert_eq!(
                status_of(&decide(
                    &ring,
                    rule.method,
                    &denied,
                    &headers_with(Some(&key))
                )),
                Some(StatusCode::FORBIDDEN),
                "{} {} allowed an out-of-scope index",
                rule.method,
                rule.pattern
            );
        }
    }

    #[test]
    fn an_index_scoped_key_reaches_mcp_and_arrives_carrying_its_scope() {
        // The door only asks for `Read`. The scope is enforced per tool and per index by the
        // dispatcher, from exactly this identity — so what matters here is that a scoped key
        // is let through *and* that what it hands over still knows what it is scoped to.
        let (scoped, scoped_config) = key_for(Role::Reader, Some(vec!["docs"]));
        let (open, open_config) = key_for(Role::Reader, None);
        let ring = ring(vec![scoped_config, open_config]);

        let authz = decide(&ring, "POST", "/mcp", &headers_with(Some(&scoped))).unwrap();
        assert!(McpAuthz::allows_index(&authz, "docs"));
        assert!(!McpAuthz::allows_index(&authz, "payroll"));
        assert!(authz.has(McpCapability::Read));
        assert!(!authz.has(McpCapability::Write));
        assert_eq!(McpAuthz::key_id(&authz), Authz::key_id(&authz));

        let open = decide(&ring, "POST", "/mcp", &headers_with(Some(&open))).unwrap();
        assert!(McpAuthz::allows_index(&open, "payroll"));
    }

    #[test]
    fn the_mcp_session_verbs_are_gated_and_not_only_the_payload_post() {
        let ring = ring(vec![key_for(Role::Admin, None).1]);
        for method in ["POST", "GET", "DELETE"] {
            assert_eq!(
                status_of(&decide(&ring, method, "/mcp", &HeaderMap::new())),
                Some(StatusCode::UNAUTHORIZED),
                "{method} /mcp answered a bare request"
            );
        }
    }

    #[test]
    fn health_is_public_but_identifies_a_caller_that_presents_a_key() {
        let (key, config) = key_for(Role::Reader, None);
        let ring = ring(vec![config]);
        let anonymous = decide(&ring, "GET", "/_cluster/health", &HeaderMap::new()).unwrap();
        assert!(!anonymous.is_identified());

        let identified =
            decide(&ring, "GET", "/_cluster/health", &headers_with(Some(&key))).unwrap();
        assert!(identified.is_identified());
        assert!(identified.key_id().is_some());
    }

    #[test]
    fn the_authorization_header_is_the_only_way_to_present_a_key() {
        let (key, config) = key_for(Role::Reader, None);
        let ring = ring(vec![config]);

        // A key in the query string is not a credential here, however tempting.
        let path = format!("/_indexes?api_key={}", key.expose());
        assert_eq!(
            status_of(&decide(&ring, "GET", &path, &HeaderMap::new())),
            Some(StatusCode::UNAUTHORIZED)
        );

        // Nor is a scheme other than Bearer.
        let mut basic = HeaderMap::new();
        basic.insert(
            header::AUTHORIZATION,
            format!("Basic {}", key.expose()).parse().unwrap(),
        );
        assert_eq!(
            status_of(&decide(&ring, "GET", "/_indexes", &basic)),
            Some(StatusCode::UNAUTHORIZED)
        );

        // Case in the scheme is not the client's problem, though.
        let mut lowercase = HeaderMap::new();
        lowercase.insert(
            header::AUTHORIZATION,
            format!("bearer {}", key.expose()).parse().unwrap(),
        );
        assert!(decide(&ring, "GET", "/_indexes", &lowercase).is_ok());
    }

    #[test]
    fn a_refusal_never_repeats_the_key_back() {
        // An error body and a log line are both places a credential ends up in a ticket.
        let (key, config) = key_for(Role::Reader, None);
        let ring = ring(vec![config]);
        let refusal = decide(&ring, "DELETE", "/api/docs", &headers_with(Some(&key))).unwrap_err();
        assert!(
            !refusal.message.contains(key.expose()),
            "{}",
            refusal.message
        );

        let stranger = ApiKey::generate().unwrap();
        let refusal =
            decide(&ring, "GET", "/_indexes", &headers_with(Some(&stranger))).unwrap_err();
        assert!(
            !refusal.message.contains(stranger.expose()),
            "{}",
            refusal.message
        );
    }

    #[test]
    fn trailing_slashes_do_not_change_what_a_route_requires() {
        assert_eq!(
            classify("POST", "/mcp/").map(|c| c.access),
            Some(Access::Mcp)
        );
        assert_eq!(
            classify("GET", "/_admin/memory/").map(|c| c.access),
            Some(Access::Needs(Capability::NodeAdmin))
        );
    }

    #[test]
    fn a_pattern_segment_never_swallows_more_than_one_path_segment() {
        // `/api/{index}` is index deletion; `/api/a/b` is not, and must not inherit its rule.
        assert!(classify("DELETE", "/api/docs").is_some());
        assert!(classify("DELETE", "/api/docs/extra").is_none());
        assert!(classify("DELETE", "/api/").is_none());
        assert_eq!(
            classify("POST", "/api/docs/search").and_then(|c| c.index),
            Some("docs")
        );
    }

    #[test]
    fn method_is_part_of_the_classification() {
        // The same path can mean read or index administration depending on the verb, and
        // getting that wrong grants schema writes to every reader.
        assert_eq!(
            classify("GET", "/api/docs/_config").map(|c| c.access),
            Some(Access::Needs(Capability::Read))
        );
        assert_eq!(
            classify("PUT", "/api/docs/_config").map(|c| c.access),
            Some(Access::Needs(Capability::IndexAdmin))
        );
        assert!(classify("PATCH", "/api/docs/_config").is_none());
    }

    /// An [`Authz`] holding a key scoped to `indexes`.
    fn scoped_as(indexes: Option<Vec<&str>>) -> Authz {
        let (key, config) = key_for(Role::Reader, indexes);
        let keyring = ring(vec![config]);
        Authz::Key(keyring.authenticate(key.expose()).unwrap())
    }

    #[test]
    fn a_scoped_key_sees_only_its_own_indexes_in_a_listing() {
        let mut listing = serde_json::json!({
            "indexes": [
                {"name": "docs", "document_count": 3},
                {"name": "payroll", "document_count": 9},
                {"name": "incidents", "document_count": 1},
            ],
            "total_indexes": 3,
            "node_id": "n1",
        });
        filter_index_listing(&mut listing, &scoped_as(Some(vec!["docs"])));

        let names: Vec<&str> = listing["indexes"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|item| item["name"].as_str())
            .collect();
        assert_eq!(names, ["docs"]);
        // The count follows the list: "3" over one row would disclose how many were withheld.
        assert_eq!(listing["total_indexes"], 1);
        // Everything that is not an index name is left alone.
        assert_eq!(listing["node_id"], "n1");
    }

    #[test]
    fn the_mcp_catalog_shape_is_filtered_too() {
        // `/_indexes` calls an entry `name`; the MCP catalog calls it `index`.
        let mut listing = serde_json::json!({
            "indexes": [
                {"index": "docs", "stats": {}},
                {"index": "payroll", "stats": {}},
            ],
            "total_indexes": 2,
        });
        filter_index_listing(&mut listing, &scoped_as(Some(vec!["docs"])));
        assert_eq!(listing["indexes"].as_array().unwrap().len(), 1);
        assert_eq!(listing["indexes"][0]["index"], "docs");
    }

    #[test]
    fn the_cluster_listing_is_filtered_under_every_node_as_well() {
        // The names appear twice: once merged at the top, once per node that answered.
        let mut listing = serde_json::json!({
            "indexes": [{"name": "docs"}, {"name": "payroll"}],
            "total_indexes": 2,
            "nodes_contacted": 2,
            "nodes": [
                {"node_id": "n1", "indexes": [{"name": "docs"}, {"name": "payroll"}], "total_indexes": 2, "total_shards": 4},
                {"node_id": "n2", "indexes": [{"name": "payroll"}], "total_indexes": 1, "total_shards": 2},
            ],
        });
        filter_index_listing(&mut listing, &scoped_as(Some(vec!["docs"])));

        assert_eq!(listing["total_indexes"], 1);
        assert_eq!(listing["nodes"][0]["indexes"].as_array().unwrap().len(), 1);
        assert_eq!(listing["nodes"][0]["total_indexes"], 1);
        assert_eq!(listing["nodes"][1]["indexes"].as_array().unwrap().len(), 0);
        assert_eq!(listing["nodes"][1]["total_indexes"], 0);
        assert!(!serde_json::to_string(&listing).unwrap().contains("payroll"));
    }

    #[test]
    fn an_unscoped_caller_sees_the_listing_untouched() {
        let full = serde_json::json!({
            "indexes": [{"name": "docs"}, {"name": "payroll"}],
            "total_indexes": 2,
        });
        for authz in [Authz::Disabled, scoped_as(None)] {
            let mut listing = full.clone();
            filter_index_listing(&mut listing, &authz);
            assert_eq!(listing, full, "{authz:?} should see everything");
        }
    }

    #[test]
    fn the_refusal_log_thins_out_instead_of_growing_without_bound() {
        // A loop of unauthenticated requests must not be a way to fill the disk. What has to
        // survive the thinning is the fact that probing is happening, not every instance.
        let logged = (1..=1_000_000u64)
            .filter(|n| should_log_refusal(*n))
            .count();
        assert!(logged < 40, "{logged} lines for a million attempts");

        // The first few always, so a misconfigured client is diagnosable immediately.
        for n in 1..=3 {
            assert!(should_log_refusal(n), "{n}");
        }
        assert!(should_log_refusal(4));
        assert!(!should_log_refusal(5));
        // And it never goes silent: there is always another line coming.
        assert!((1_000_001..=1_100_000u64).any(should_log_refusal));
    }

    #[test]
    fn an_anonymous_caller_is_denied_rather_than_left_unrestricted() {
        // Unreachable today: no listing route and no MCP route is `Public`. The point is that
        // the guarantee should not rest on that — a route reclassified later must not turn
        // this into the catalogue for anyone who can reach the port.
        let mut listing = serde_json::json!({
            "indexes": [{"name": "docs"}, {"name": "payroll"}],
            "total_indexes": 2,
        });
        filter_index_listing(&mut listing, &Authz::Anonymous);
        assert_eq!(listing["indexes"].as_array().unwrap().len(), 0);
        assert_eq!(listing["total_indexes"], 0);

        assert!(!McpAuthz::allows_index(&Authz::Anonymous, "docs"));
        assert!(!Authz::Anonymous.has(McpCapability::Read));
        // Auth off is the one case that is genuinely unrestricted.
        assert!(McpAuthz::allows_index(&Authz::Disabled, "docs"));
        assert!(Authz::Disabled.has(McpCapability::NodeAdmin));
    }

    #[test]
    fn an_entry_whose_shape_is_unrecognised_is_dropped_not_kept() {
        // If the listing shape changes underneath this function, the failure has to be
        // "an index vanished from a list", not "a scoped key saw everything".
        let mut listing = serde_json::json!({
            "indexes": [{"name": "docs"}, {"identifier": "payroll"}, "docs", 7],
            "total_indexes": 4,
        });
        filter_index_listing(&mut listing, &scoped_as(Some(vec!["docs"])));
        assert_eq!(listing["indexes"].as_array().unwrap().len(), 1);
        assert_eq!(listing["total_indexes"], 1);
    }
}
