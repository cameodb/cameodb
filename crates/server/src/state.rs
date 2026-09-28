//! The context every request runs against.
//!
//! Its own module because both surfaces need it: the HTTP handlers take it as axum state, and
//! `McpBackend` is implemented on it. Putting it under either one would make the other depend on
//! a module it has nothing else to do with.

use kameo::actor::ActorRef;
use std::sync::Arc;
use std::time::Duration;

use crate::cluster_coordinator::ClusterCoordinator;
use crate::node::{QueueLoad, ReadPoolHealth, RouterActor, WriterLiveness};
use crate::ratelimit::RateLimiter;

/// Application state shared across handlers
#[derive(Clone)]
pub struct AppState {
    pub router: RouterActor,
    /// The health body's index counts, kept between probes — see
    /// [`IndexCounts`](crate::http_server::IndexCounts).
    pub index_counts: Arc<crate::http_server::IndexCounts>,
    /// Who this node is, for the health body. Fixed for the life of the process, so health
    /// reads it here rather than asking the orchestrator — an ask that queued behind writes
    /// and ran out of health's budget on every probe under write overload (ROADMAP M6,
    /// session 4).
    pub node_id: uuid::Uuid,
    pub node_name: String,
    pub coordinator: ActorRef<ClusterCoordinator>,
    /// Number of documents per micro-batch for NDJSON write-stream ingestion
    pub stream_batch_size: usize,
    /// Largest accepted single record, in bytes (from `max_record_size_mb`).
    ///
    /// The NDJSON stream handler enforces this per line. The wire-level body limit bounds
    /// the request as a whole, but one unterminated line could still buffer the entire
    /// allowance in memory, so the per-record cap is what keeps peak memory bounded.
    pub max_record_size_bytes: usize,
    /// Largest accepted request body *after decompression*, in bytes (from
    /// `max_body_size_mb`).
    ///
    /// Everywhere else this setting already means decompressed bytes: `DefaultBodyLimit` sits
    /// inside `RequestDecompressionLayer`, so a `Json` or `Bytes` extractor measures a gzip bomb
    /// expanded. The NDJSON stream handler takes a raw `Body`, which no extractor limit
    /// reaches, and the only guard that did reach it — `RequestBodyLimitLayer` — counts bytes
    /// off the socket, before they are inflated. So a compressed stream under the wire limit
    /// could expand without bound, and compressing a request bought a caller more allowance
    /// than sending it plain. The handler counts what it drains against this, so the setting
    /// means one thing on every route.
    pub max_body_size_bytes: usize,
    /// Per-caller budgets for tool calls, searches and writes. Shared across every request,
    /// because a rate limit that reset per connection would not be one.
    pub rate_limiter: Arc<RateLimiter>,
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
    /// The keys requests are decided against, shared with the auth middleware's
    /// `GateState`, plus the means to re-resolve them: `POST /_admin/keys/reload` and
    /// SIGHUP both land on its `reload`, which swaps the ring underneath every
    /// in-flight reader.
    pub keyring: Arc<crate::auth::KeyReloader>,
    /// This node's writer-thread liveness. The health endpoint reads it with one atomic load so
    /// a shard whose writer thread has died stops the node reporting green.
    pub writer_liveness: Arc<WriterLiveness>,
    /// This node's read-pool health. The health endpoint reads it the same non-blocking way for
    /// the saturation gauge and to turn the node red when the pool has wedged.
    pub read_pool_health: Arc<ReadPoolHealth>,
    /// This node's estimate of how long a request arriving now would wait before a worker
    /// started it. The admission guard refuses against it before reading a body, and health
    /// reports it so an operator sees the number the refusals are being made on.
    ///
    /// `None` when there is no worker pool to predict — nothing to estimate, and the guard
    /// stays out of the way.
    pub queue_load: Option<Arc<QueueLoad>>,
    /// The request timeout this node resolved, as `TimeoutLayer` enforces it.
    ///
    /// Carried here so a handler can size its own internal waits against the budget it is
    /// actually running under. A guard longer than that budget never fires: the request is
    /// abandoned as a 408 first, which is the failure the guard existed to prevent. The
    /// health endpoint derives its actor budget from this — see `health_actor_budget`.
    pub request_timeout: Duration,
}
