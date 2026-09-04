//! The context every request runs against.
//!
//! Its own module because both surfaces need it: the HTTP handlers take it as axum state, and
//! `McpBackend` is implemented on it. Putting it under either one would make the other depend on
//! a module it has nothing else to do with.

use kameo::actor::ActorRef;
use std::sync::Arc;

use crate::cluster_coordinator::ClusterCoordinator;
use crate::node_orchestrator::RouterActor;
use crate::ratelimit::ToolRateLimiter;

/// Application state shared across handlers
#[derive(Clone)]
pub struct AppState {
    pub router: RouterActor,
    pub coordinator: ActorRef<ClusterCoordinator>,
    /// Number of documents per micro-batch for NDJSON write-stream ingestion
    pub stream_batch_size: usize,
    /// Largest accepted single record, in bytes (from `max_record_size_mb`).
    ///
    /// The NDJSON stream handler enforces this per line. The wire-level body limit bounds
    /// the request as a whole, but one unterminated line could still buffer the entire
    /// allowance in memory, so the per-record cap is what keeps peak memory bounded.
    pub max_record_size_bytes: usize,
    /// Per-key budget for MCP tool calls. Shared across every request, because a rate limit
    /// that reset per connection would not be one.
    pub tool_limiter: Arc<ToolRateLimiter>,
    /// Largest `limit` an MCP search may ask for, from `[security.limits]`.
    ///
    /// Both advertised and enforced: the tool schemas render it as their `maximum`, so a
    /// client is shown the same number a call is measured against.
    pub max_search_limit: usize,
    /// Most indexes one MCP federated search may name, from `[security.limits]`.
    ///
    /// Advertised and enforced the same way `max_search_limit` is, and for the same reason: a
    /// caller must not be refused for exceeding a bound the catalogue did not show it.
    pub max_federated_indexes: usize,
    /// Largest MCP search response in bytes, from `[security.limits]`.
    ///
    /// Hits past it are left out and the response says so. The limit bounds how many hits are
    /// returned; this bounds how large they may be, which no count can.
    pub max_response_bytes: usize,
    /// Where the audit trail goes. Inert unless `[security.audit]` turned it on.
    pub audit: Arc<crate::audit::AuditSink>,
}
