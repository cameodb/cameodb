//! The HTTP API: routes, the middleware stack, and one module per operation.
//!
//! [`routes`] mounts everything and owns the layer order; the operation modules hold the handlers
//! and the request types they deserialize, and are private so that nothing but `routes` can name
//! a handler. The MCP tools are not here — they answer on the same state but a different protocol,
//! and live in `crate::mcp`.

mod admin;
mod catalogue;
mod error;
mod health;
mod routes;
mod search;
mod shed;
mod write;

use crate::ratelimit::Caller;

pub(crate) use catalogue::validate_index_name;
pub(crate) use health::HEALTH_PATH;
pub(crate) use routes::{RouterConfig, create_router};

/// Who the rate limiter charges for this request.
///
/// The authorization gate attaches a [`Caller`] to everything it admits, deciding once — from
/// the key and the socket together — what a handler would otherwise have to work out
/// identically on every route. `None` means the handler was reached without passing the gate,
/// which happens only where a test mounts one directly; such a request is metered as
/// unattributable rather than exempt, because "no subject" must never read as "no limit".
fn caller_of(caller: Option<axum::Extension<Caller>>) -> Caller {
    caller.map_or(Caller::Unattributed, |axum::Extension(caller)| caller)
}

/// Whose budget this request spends, if the caller's key names a tenant.
///
/// Read from the same [`Authz`](crate::authz::Authz) the gate already attaches, rather than a
/// second extension, so there is one answer to "who is this" and the quota cannot end up
/// disagreeing with the audit trail about it. `None` — an anonymous caller, a node with
/// authentication off, or a key with no tenant — spends against no budget and is never refused
/// by one.
fn tenant_of(authz: Option<axum::Extension<crate::authz::Authz>>) -> Option<String> {
    authz.and_then(|axum::Extension(authz)| authz.tenant().map(str::to_string))
}
