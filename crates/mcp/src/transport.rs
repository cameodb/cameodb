//! The HTTP surface: routes, the two transports, and the header handling each requires.

use std::{
    convert::Infallible,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use axum::{
    Json, Router,
    extract::{Extension, Query, State},
    http::{HeaderMap, HeaderName, HeaderValue, StatusCode},
    response::{
        IntoResponse, Response, Sse,
        sse::{Event, KeepAlive},
    },
    routing::{get, post},
};
use futures::{Stream, StreamExt};
use serde::Deserialize;
use serde_json::{Value as JsonValue, json};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tracing::{debug, info};

use crate::{
    authz::{McpAuthzRef, unrestricted},
    backend::McpBackend,
    protocol::{MCP_PROTOCOL_VERSION_HEADER, MCP_SESSION_ID_HEADER, SUPPORTED_PROTOCOL_VERSIONS},
    rpc::{error_response, handle_rpc_request, method_of, parse_json_rpc_request},
    session::{McpShutdownHandle, McpTransportState, SessionAccess, spawn_cleanup_task},
};

/// The caller a request carries, or the unrestricted one if the host inserted none.
fn caller(authz: Option<Extension<McpAuthzRef>>) -> McpAuthzRef {
    authz.map_or_else(unrestricted, |Extension(authz)| authz)
}

#[derive(Debug, Deserialize)]
struct MessageQuery {
    session_id: String,
}

pub fn mcp_router<S>() -> (Router<S>, McpShutdownHandle)
where
    S: McpBackend,
{
    let transport_state = McpTransportState::default();

    // Start global session cleanup task (respects cancellation token)
    spawn_cleanup_task(transport_state.clone());

    let handle = McpShutdownHandle::new(transport_state.clone());

    let router = Router::new()
        // Streamable HTTP transport (MCP spec 2025-03-26+): a single MCP endpoint
        // that supports POST (send messages), GET (open a listening SSE stream),
        // and DELETE (terminate a session).
        .route(
            "/",
            post(
                |State(app_state): State<S>,
                 Extension(state): Extension<McpTransportState>,
                 authz: Option<Extension<McpAuthzRef>>,
                 headers: HeaderMap,
                 Json(payload): Json<JsonValue>| async move {
                    process_streamable_http(app_state, state, caller(authz), headers, payload).await
                },
            )
            .get(
                |Extension(state): Extension<McpTransportState>,
                 authz: Option<Extension<McpAuthzRef>>,
                 headers: HeaderMap| async move {
                    streamable_listen_handler(state, caller(authz), headers).await
                },
            )
            .delete(
                |Extension(state): Extension<McpTransportState>,
                 authz: Option<Extension<McpAuthzRef>>,
                 headers: HeaderMap| async move {
                    streamable_delete_handler(state, caller(authz), headers).await
                },
            ),
        )
        // Legacy HTTP+SSE transport (MCP spec 2024-11-05): kept for backwards
        // compatibility with already-configured clients.
        .route(
            "/sse",
            get(
                |Extension(state): Extension<McpTransportState>,
                 authz: Option<Extension<McpAuthzRef>>| async move {
                    mcp_sse_handler(state, caller(authz)).await
                },
            )
            .post(
                |State(app_state): State<S>,
                 authz: Option<Extension<McpAuthzRef>>,
                 Json(payload): Json<JsonValue>| async move {
                    process_mcp_http_message(app_state, caller(authz), payload).await
                },
            ),
        )
        .route(
            "/messages",
            post(
                |State(app_state): State<S>,
                 Query(query): Query<MessageQuery>,
                 Extension(state): Extension<McpTransportState>,
                 authz: Option<Extension<McpAuthzRef>>,
                 Json(payload): Json<JsonValue>| async move {
                    process_mcp_message(app_state, query, state, caller(authz), payload).await
                },
            ),
        )
        .layer(Extension(transport_state));

    (router, handle)
}

async fn mcp_sse_handler(
    state: McpTransportState,
    authz: McpAuthzRef,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let (session_id, rx) = {
        let (session_id, tx, rx) = state.create_sse_session(authz.key_id()).await;

        // Emit "endpoint" event per MCP spec
        let endpoint_url = format!("/mcp/messages?session_id={}", session_id);
        let endpoint_event = Event::default().event("endpoint").data(endpoint_url);

        let _ = tx.send(endpoint_event);
        (session_id, rx)
    };

    info!(session_id = %session_id, "MCP SSE session opened");

    let inner_stream = UnboundedReceiverStream::new(rx).map(Ok);

    // Wrap stream with a guard that removes the session when SSE connection drops
    let stream = SessionStream {
        inner: inner_stream,
        guard: SessionDropGuard { session_id, state },
    };

    Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keepalive"),
    )
}

/// Guard that removes the session from the registry when the SSE stream is dropped.
struct SessionDropGuard {
    session_id: String,
    state: McpTransportState,
}

impl Drop for SessionDropGuard {
    fn drop(&mut self) {
        let session_id = self.session_id.clone();
        let state = self.state.clone();
        tokio::spawn(async move {
            if state.forget_session(&session_id).await {
                info!(session_id = %session_id, "MCP SSE session closed");
            }
        });
    }
}

/// Stream wrapper that holds a [`SessionDropGuard`] for automatic cleanup.
struct SessionStream<S> {
    inner: S,
    #[allow(dead_code)] // Held for its Drop impl
    guard: SessionDropGuard,
}

impl<S: Stream + Unpin> Stream for SessionStream<S> {
    type Item = S::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }
}

async fn process_mcp_message<B: McpBackend>(
    app_state: B,
    query: MessageQuery,
    state: McpTransportState,
    authz: McpAuthzRef,
    payload: JsonValue,
) -> impl IntoResponse {
    let key_id = authz.key_id();
    let session = match state
        .claim_session(&query.session_id, key_id.as_deref())
        .await
    {
        SessionAccess::Granted => state.session_of(&query.session_id).await,
        SessionAccess::WrongKey => return session_refusal(&query.session_id).into_response(),
        SessionAccess::Unknown => None,
    };

    match session {
        Some(mcp_session) => {
            let sender = mcp_session.sender.clone();
            let session_id = query.session_id.clone();

            // Spawn background task to process message asynchronously
            tokio::spawn(async move {
                let maybe_response = match parse_json_rpc_request(payload) {
                    Ok(request) => handle_rpc_request(app_state, request, &authz).await,
                    Err(err) => Some(error_response(
                        None,
                        -32600,
                        format!("Invalid JSON-RPC request: {err}"),
                    )),
                };

                if let Some(envelope) = maybe_response {
                    let event = Event::default().event("message").data(envelope.to_string());
                    match sender {
                        Some(sender) if sender.send(event).is_ok() => {}
                        Some(_) => {
                            debug!(session_id = %session_id, "MCP session receiver dropped during async processing");
                        }
                        None => {
                            debug!(session_id = %session_id, "MCP session has no SSE push channel; response dropped");
                        }
                    }
                }
            });

            // Return 202 Accepted immediately per MCP spec
            axum::http::StatusCode::ACCEPTED.into_response()
        }
        None => (
            axum::http::StatusCode::NOT_FOUND,
            Json(json!({
                "error": "Unknown MCP session",
                "session_id": query.session_id,
            })),
        )
            .into_response(),
    }
}

async fn process_mcp_http_message<B: McpBackend>(
    app_state: B,
    authz: McpAuthzRef,
    payload: JsonValue,
) -> impl IntoResponse {
    match parse_json_rpc_request(payload) {
        Ok(request) => match handle_rpc_request(app_state, request, &authz).await {
            Some(response) => (StatusCode::OK, Json(response)).into_response(),
            None => StatusCode::NO_CONTENT.into_response(),
        },
        Err(err) => (
            StatusCode::BAD_REQUEST,
            Json(error_response(
                None,
                -32600,
                format!("Invalid JSON-RPC request: {err}"),
            )),
        )
            .into_response(),
    }
}

/// Streamable HTTP transport POST handler (MCP spec 2025-03-26+).
///
/// Processes a single JSON-RPC message and returns the response inline as
/// `application/json`. On `initialize`, a new session id is generated and
/// returned in the `MCP-Session-Id` response header. Notifications and
/// responses (which produce no reply) return `202 Accepted` per the spec.
///
/// If a client sends an `MCP-Protocol-Version` header, it is validated against
/// the supported set and rejected with `400 Bad Request` if unsupported.
async fn process_streamable_http<B: McpBackend>(
    app_state: B,
    state: McpTransportState,
    authz: McpAuthzRef,
    headers: HeaderMap,
    payload: JsonValue,
) -> Response {
    // A session id names server-side state. Continuing someone else's conversation needs
    // more than knowing its id.
    let key_id = authz.key_id();
    if let Some(session_id) = session_id_of(&headers)
        && state.claim_session(session_id, key_id.as_deref()).await == SessionAccess::WrongKey
    {
        return session_refusal(session_id).into_response();
    }

    // Validate the protocol version header if present (spec: 400 if unsupported).
    if let Some(version) = headers
        .get(MCP_PROTOCOL_VERSION_HEADER)
        .and_then(|value| value.to_str().ok())
        && !SUPPORTED_PROTOCOL_VERSIONS.contains(&version)
    {
        return (
            StatusCode::BAD_REQUEST,
            Json(error_response(
                None,
                -32600,
                format!("Unsupported MCP-Protocol-Version: {version}"),
            )),
        )
            .into_response();
    }

    let is_initialize = method_of(&payload) == Some("initialize");

    match parse_json_rpc_request(payload) {
        Ok(request) => match handle_rpc_request(app_state, request, &authz).await {
            Some(response) => {
                if is_initialize {
                    // Establish a session and advertise it via the MCP-Session-Id header.
                    let session_id = state.create_session(key_id).await;
                    let mut response_headers = HeaderMap::new();
                    if let Ok(value) = HeaderValue::from_str(&session_id) {
                        response_headers
                            .insert(HeaderName::from_static(MCP_SESSION_ID_HEADER), value);
                    }
                    (StatusCode::OK, response_headers, Json(response)).into_response()
                } else {
                    (StatusCode::OK, Json(response)).into_response()
                }
            }
            // No response body => JSON-RPC notification or response: 202 Accepted.
            None => StatusCode::ACCEPTED.into_response(),
        },
        Err(err) => (
            StatusCode::BAD_REQUEST,
            Json(error_response(
                None,
                -32600,
                format!("Invalid JSON-RPC request: {err}"),
            )),
        )
            .into_response(),
    }
}

/// Streamable HTTP transport GET handler (MCP spec 2025-03-26+).
///
/// Opens a server-to-client SSE stream. CameoDB does not currently initiate
/// server-side requests, so this stream only emits keep-alive comments to hold
/// the connection open, satisfying clients that establish a listening channel.
async fn streamable_listen_handler(
    state: McpTransportState,
    authz: McpAuthzRef,
    headers: HeaderMap,
) -> Response {
    if let Some(session_id) = session_id_of(&headers) {
        let key_id = authz.key_id();
        if state.claim_session(session_id, key_id.as_deref()).await == SessionAccess::WrongKey {
            return session_refusal(session_id).into_response();
        }
    }

    let stream = futures::stream::pending::<Result<Event, Infallible>>();
    Sse::new(stream)
        .keep_alive(
            KeepAlive::new()
                .interval(Duration::from_secs(15))
                .text("keepalive"),
        )
        .into_response()
}

fn session_id_of(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(MCP_SESSION_ID_HEADER)
        .and_then(|value| value.to_str().ok())
}

/// A session that exists but belongs to someone else.
///
/// 403 rather than 404: the caller is authenticated and the session is real, so pretending
/// it does not exist would send a well-behaved client into a reconnect loop.
fn session_refusal(session_id: &str) -> impl IntoResponse {
    debug!(session_id = %session_id, "MCP session presented by a different key");
    (
        StatusCode::FORBIDDEN,
        Json(json!({
            "error": "Forbidden",
            "message": "this MCP session belongs to a different key",
        })),
    )
}

/// Streamable HTTP transport DELETE handler (MCP spec 2025-03-26+).
///
/// Explicitly terminates the session identified by the `MCP-Session-Id` header.
/// Returns `200 OK` if removed, `404 Not Found` if unknown, `400 Bad Request`
/// if the header is missing.
async fn streamable_delete_handler(
    state: McpTransportState,
    authz: McpAuthzRef,
    headers: HeaderMap,
) -> Response {
    let Some(session_id) = session_id_of(&headers) else {
        return StatusCode::BAD_REQUEST.into_response();
    };

    let key_id = authz.key_id();
    match state.remove_session(session_id, key_id.as_deref()).await {
        SessionAccess::Granted => {
            info!(session_id = %session_id, "MCP Streamable HTTP session terminated");
            StatusCode::OK.into_response()
        }
        SessionAccess::Unknown => StatusCode::NOT_FOUND.into_response(),
        // Ending someone else's session is a denial of service, not a courtesy.
        SessionAccess::WrongKey => session_refusal(session_id).into_response(),
    }
}
