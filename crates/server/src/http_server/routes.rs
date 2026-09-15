//! Every route this node exposes, and the middleware stack in front of them.
//!
//! One file on purpose. The layer order below is a security property — the body limit is applied
//! after decompression so a compression bomb is measured expanded, the auth gate sits inside CORS,
//! and the concurrency guard exempts the liveness path — and an order spread across files is an
//! order nobody can read. `crate::authz` also parses this file to prove that every mounted route
//! has a row in its authorization table, which is why mounting happens here and nowhere else.

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{
    Json, Router,
    extract::DefaultBodyLimit,
    http::{HeaderName, HeaderValue, StatusCode, header},
    middleware::{Next, from_fn},
    response::IntoResponse,
    routing::{delete, get, patch, post, put},
};
use cameodb_mcp::{MCP_SESSION_ID_HEADER, McpShutdownHandle, mcp_router};
use tokio::sync::Semaphore;
use tower_http::{
    catch_panic::CatchPanicLayer, compression::CompressionLayer, cors::CorsLayer,
    decompression::DecompressionLayer, limit::RequestBodyLimitLayer, timeout::TimeoutLayer,
    trace::TraceLayer,
};
use tracing::{info, warn};

use crate::http_server::HEALTH_PATH;
use crate::http_server::admin::{
    admin_audit_handler, admin_index_commit_handler, admin_index_evict_writer_handler,
    admin_memory_handler, admin_memory_purge_handler, admin_workers_handler,
};
use crate::http_server::catalogue::{
    create_config_handler, delete_index_handler, get_config_handler, list_cluster_indexes_handler,
    list_indexes_handler, update_schema_handler,
};
use crate::http_server::health::health_handler;
use crate::http_server::search::{search_handler, search_stream_handler};
use crate::http_server::write::{
    bulk_delete_handler, bulk_write_handler, delete_document_handler, write_handler,
    write_stream_handler,
};
use crate::node_orchestrator::REQUEST_STARTED_AT;
use crate::state::AppState;

/// What the surface in front of the handlers is configured with.
///
/// Named rather than passed as a run of positional arguments: five of these are numbers or
/// booleans, two of them are derived from other settings rather than read straight out of the
/// file, and a transposition among them is the kind that compiles. `max_body_size_mb` next to
/// `max_concurrent_requests` is the pair that matters most — swapped, the node would accept
/// enormous bodies four at a time.
pub struct RouterConfig<'a> {
    /// Maximum request body size in MB. Derived from `limits.max_record_size_mb` unless pinned.
    pub max_body_size_mb: usize,
    /// Allowed CORS origins, from `[network.http]`.
    pub cors_allowed_origins: &'a [String],
    /// Maximum concurrent in-flight HTTP requests.
    pub max_concurrent_requests: usize,
    /// How long one request may take before it is answered `408`. Also derived.
    pub request_timeout_secs: u64,
    /// Whether `/_admin/*` is mounted at all.
    pub admin_enabled: bool,
    /// The `[mcp]` section: which of the MCP transport is mounted, and session lifetime.
    pub mcp: &'a crate::config::McpConfig,
}

/// Creates the main HTTP router with all endpoints and middleware
///
/// # Arguments
/// * `state` - Application state with actor references
/// * `keyring` - The keys the authorization gate decides against
/// * `config` - See [`RouterConfig`]
pub fn create_router(
    state: AppState,
    keyring: Arc<crate::auth::KeyRing>,
    config: &RouterConfig<'_>,
) -> (Router, McpShutdownHandle) {
    let &RouterConfig {
        max_body_size_mb,
        cors_allowed_origins,
        max_concurrent_requests,
        request_timeout_secs,
        admin_enabled,
        mcp,
    } = config;

    let body_limit_bytes = max_body_size_mb * 1024 * 1024;
    let (mcp_routes, mcp_handle) = mcp_router::<AppState>(mcp.transport());
    // The gate writes to the same sink the handlers do, so a request produces one record
    // whichever layer had the last word about it.
    let audit = Arc::clone(&state.audit);

    // Concurrency limiter: rejects with 503 when too many requests are in flight.
    //
    // The liveness endpoint is exempt. Sharing the semaphore with it meant a node under
    // load answered its own health check with 503, so a load balancer would evict a node
    // that was merely busy — turning local overload into a cluster-wide outage.
    let semaphore = Arc::new(Semaphore::new(max_concurrent_requests));
    let concurrency_guard = from_fn(move |req: axum::extract::Request, next: Next| {
        let sem = semaphore.clone();
        async move {
            // Health checks may arrive with or without a trailing slash; exempt them all so a
            // load balancer's probe is never starved out by ordinary traffic.
            if req.uri().path().trim_end_matches('/') == HEALTH_PATH {
                return next.run(req).await;
            }
            match sem.try_acquire() {
                Ok(_permit) => next.run(req).await,
                Err(_) => (
                    StatusCode::SERVICE_UNAVAILABLE,
                    // Without this, clients retry immediately and deepen the overload.
                    [(header::RETRY_AFTER, "1")],
                    "Too many concurrent requests",
                )
                    .into_response(),
            }
        }
    });

    // Build the CORS layer from config. `CameoDbConfig::validate` has already rejected "*"
    // mixed with specific origins, and origins that are not valid header values, so the parse
    // below cannot silently drop an entry and turn a configured allow-list into deny-all.
    //
    // An empty list is not rejected there — it is the default, and deny-all is what it means.
    // CORS governs browsers only, so no API or MCP client is affected.
    let cors_layer = if cors_allowed_origins.iter().any(|o| o == "*") {
        warn!("CORS: allowing any origin (cors_allowed_origins = [\"*\"])");
        CorsLayer::permissive()
    } else {
        let origins: Vec<HeaderValue> = cors_allowed_origins
            .iter()
            .filter_map(|o| o.parse::<HeaderValue>().ok())
            .collect();
        if origins.is_empty() {
            info!("CORS: no origins configured; browsers get no cross-origin access");
        } else {
            info!(origins = ?cors_allowed_origins, "CORS: restricting to configured origins");
        }
        CorsLayer::new()
            .allow_origin(origins)
            .allow_methods([
                axum::http::Method::GET,
                axum::http::Method::POST,
                axum::http::Method::PUT,
                axum::http::Method::PATCH,
                axum::http::Method::DELETE,
            ])
            // `mcp-session-id` is required by the MCP Streamable HTTP transport: the
            // client sends it on every follow-up request and must be able to read it off
            // the initialize response, so it has to be both allowed and exposed. Without
            // that, restricting origins silently breaks every browser-based MCP client.
            .allow_headers([
                header::CONTENT_TYPE,
                header::AUTHORIZATION,
                header::ACCEPT,
                HeaderName::from_static(MCP_SESSION_ID_HEADER),
            ])
            .expose_headers([HeaderName::from_static(MCP_SESSION_ID_HEADER)])
    };

    // `/mcp` is mounted unless an operator turned it off, in which case the routes are absent
    // rather than refusing — the same reasoning as `admin_enabled` below. The handle is still
    // returned so shutdown has one code path either way; it drains an empty registry.
    let router = Router::new();
    let router = if mcp.enabled {
        router.nest("/mcp", mcp_routes)
    } else {
        info!("MCP disabled: /mcp routes are not mounted");
        router
    };
    let router = router
        // API routes
        .route("/api/{index}/search", post(search_handler))
        .route("/api/{index}/search/stream", post(search_stream_handler))
        .route("/api/{index}/document", put(write_handler))
        .route("/api/{index}/document", delete(delete_document_handler))
        .route("/api/{index}/document/stream", post(write_stream_handler))
        .route("/api/{index}/_bulk", post(bulk_write_handler))
        .route("/api/{index}/_bulk/delete", post(bulk_delete_handler))
        .route("/api/{index}/_config", put(create_config_handler))
        .route("/api/{index}/_config", get(get_config_handler))
        // Schema maintenance
        .route("/api/{index}/_schema", patch(update_schema_handler))
        // Index management
        .route("/api/{index}", delete(delete_index_handler))
        .route("/_indexes", get(list_indexes_handler))
        .route("/_cluster/_indexes", get(list_cluster_indexes_handler))
        // Health check
        .route(HEALTH_PATH, get(health_handler))
        .fallback(fallback_handler);

    // Admin endpoints are mounted only when enabled, so a disabled admin API is absent
    // rather than merely refusing — nothing to probe, nothing to accidentally re-enable
    // with a misconfigured guard.
    let router = if admin_enabled {
        router
            .route("/_admin/memory", get(admin_memory_handler))
            .route("/_admin/memory/purge", post(admin_memory_purge_handler))
            .route("/_admin/workers", get(admin_workers_handler))
            .route("/_admin/audit", get(admin_audit_handler))
            .route(
                "/_admin/index/{index}/commit",
                post(admin_index_commit_handler),
            )
            .route(
                "/_admin/index/{index}/evict-writer",
                post(admin_index_evict_writer_handler),
            )
    } else {
        info!("Admin API disabled: /_admin/* routes are not mounted");
        router
    };

    // A route that panics on demand, so the panic-isolation smoke test can prove the catch layer
    // turns a handler panic into a 500 against the built binary. Absent unless the feature is on.
    #[cfg(feature = "fault-injection")]
    let router = router.route("/__fault/panic", get(fault_panic_handler));

    let router = router
        .with_state(state)
        // Response compression (outermost for responses)
        .layer(CompressionLayer::new())
        // Decompressed-size limit for body *extractors* (Json/Bytes/String). Applied
        // after DecompressionLayer so a compression bomb is measured expanded, not
        // compressed. Note this is an extractor-level limit only — it does not count
        // bytes off the socket, which is what RequestBodyLimitLayer below does.
        .layer(DefaultBodyLimit::max(body_limit_bytes))
        // Allow compressed requests — decompresses before the body limit above
        .layer(DecompressionLayer::new())
        // Wire-level limit: counts bytes as they arrive and returns 413 once the cap is
        // passed, regardless of how the handler consumes the body. This is the only guard
        // that covers handlers taking a raw `Body` (the streaming ingest path), which
        // `DefaultBodyLimit` never applied to.
        .layer(RequestBodyLimitLayer::new(body_limit_bytes))
        // Concurrency guard — reject excess requests with 503
        .layer(concurrency_guard)
        // Stamp request arrival for the deadline checks downstream — the dispatch path and
        // the worker dequeue check both measure spent budget against this instant. Just
        // inside the timeout layer, so the stamp and the client's deadline start together —
        // a request that spent its budget being received (a max-size record at the derived
        // timeout) is refused as stale work rather than given a fresh budget.
        .layer(from_fn(
            |req: axum::extract::Request, next: Next| async move {
                REQUEST_STARTED_AT
                    .scope(Instant::now(), next.run(req))
                    .await
            },
        ))
        // Bound how long a single request may occupy a concurrency permit. Without this,
        // `max_concurrent_requests` trickle-fed connections hold every permit forever and
        // the limit meant to prevent a DoS becomes the mechanism for one.
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            Duration::from_secs(request_timeout_secs),
        ))
        // Authentication and authorization. Inside CORS, so a browser preflight — which
        // never carries `Authorization` — still gets its headers; outside everything below,
        // so a flood of unauthenticated requests neither takes a concurrency permit nor has
        // a body buffered on its behalf.
        .layer(axum::middleware::from_fn_with_state(
            crate::authz::GateState { keyring, audit },
            crate::authz::authorize,
        ))
        .layer(cors_layer)
        // Convert a panic in a handler or any inner layer into a 500 rather than a dropped
        // connection. Placed just inside the trace layer, so a caught panic is logged as a
        // normal 500 response and outside every other layer, so a panic in auth, the
        // concurrency guard or a body limit is caught too. A search panic is already contained
        // at the read pool; this covers the async handlers that never reach it. It relies on the
        // release profile unwinding — under the old `panic = "abort"` there was nothing to catch.
        .layer(CatchPanicLayer::custom(handle_panic))
        .layer(TraceLayer::new_for_http());

    (router, mcp_handle)
}

/// Panic on request, so the smoke test can prove a handler panic is caught. Feature-gated, so a
/// shipped binary never mounts it.
#[cfg(feature = "fault-injection")]
async fn fault_panic_handler() -> axum::response::Response {
    panic!("fault-injection: handler panic");
}

/// Turn a caught handler panic into the same masked 500 an unclassified error answers with.
///
/// The panic payload is logged for an operator and withheld from the caller — a panic message can
/// carry internals, and the caller can act on none of it. A JSON `{"error":"Internal server
/// error"}` matches what [`crate::http_server::error::AppError`] returns for a server fault, so a
/// panicked request is indistinguishable on the wire from any other 500.
fn handle_panic(panic: Box<dyn std::any::Any + Send + 'static>) -> axum::response::Response {
    let detail = panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| panic.downcast_ref::<&str>().copied())
        .unwrap_or("<non-string panic payload>");
    tracing::error!(panic = %detail, "request handler panicked; answering 500");

    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(serde_json::json!({ "error": "Internal server error" })),
    )
        .into_response()
}

/// Fallback handler for 404/405 to return JSON error shape
///
/// Echoes the path only, not the query string: the audit layer strips the query from its
/// records, and the trace layer logs this body in full — so echoing the raw URI here would
/// put the query string (which may carry tokens or other secrets) into both.
async fn fallback_handler(uri: axum::http::Uri) -> impl IntoResponse {
    (
        StatusCode::NOT_FOUND,
        Json(serde_json::json!({
            "error": "Not Found",
            "path": uri.path()
        })),
    )
}

#[cfg(test)]
mod tests {
    use super::handle_panic;
    use axum::http::StatusCode;

    /// A caught panic answers the masked 500, and the panic text never reaches the caller.
    #[tokio::test]
    async fn a_panic_becomes_a_masked_500() {
        // The payload a `panic!("...")` produces.
        let response = handle_panic(Box::new("handler blew up on a document".to_string()));
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read body");
        let json: serde_json::Value = serde_json::from_slice(&body).expect("json body");
        assert_eq!(
            json,
            serde_json::json!({ "error": "Internal server error" })
        );
        assert!(
            !String::from_utf8_lossy(&body).contains("blew up"),
            "the panic message must not reach the caller"
        );
    }

    /// A panic whose payload is not a string is still a 500, not a second panic.
    #[test]
    fn a_non_string_panic_is_still_a_500() {
        let response = handle_panic(Box::new(42u32));
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }
}
