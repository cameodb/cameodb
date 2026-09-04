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
    session::{
        McpShutdownHandle, McpTransportState, SessionAccess, SessionClaim, SessionLimits,
        spawn_cleanup_task,
    },
};

/// What the host decides about the MCP transport, as opposed to what the protocol decides.
///
/// Every field here is a number or a switch an operator can be wrong about in a way only their
/// deployment can judge: how long a paused agent should keep its session, how many sessions a
/// node will hold, how often a stream must be written to for the proxies in front of it to
/// consider it alive, and whether the superseded transport is reachable at all. None of them
/// changes what the server *says* — a client sees the same protocol either way.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct McpTransportConfig {
    /// How long a session may sit idle before it is swept.
    ///
    /// Idle means nothing arrived and no connection is open on it. A client holding its
    /// listening stream open is never idle, however long it pauses, so this bounds the
    /// disconnected case: how much of a gap a client may leave and still find its session.
    pub session_idle_timeout: Duration,

    /// The most sessions the registry will hold, evicting the idlest at the cap.
    pub max_sessions: usize,

    /// How often an idle SSE stream is written to, on both transports.
    ///
    /// The number that decides whether an intermediary calls the connection dead. It has to be
    /// below the shortest idle-read timeout between this node and its clients, and that is a
    /// property of someone else's load balancer.
    pub sse_keepalive: Duration,

    /// Whether the superseded HTTP+SSE transport (`/sse` and `/messages`) is mounted.
    ///
    /// Off unmounts the routes rather than refusing them, the same way the admin API is
    /// withheld: there is nothing left to probe, and nothing a misconfigured guard can
    /// re-enable. Left on by default because turning it off strands any client still
    /// configured for it.
    pub legacy_sse_enabled: bool,
}

/// The transport a host that configures nothing gets.
impl Default for McpTransportConfig {
    fn default() -> Self {
        Self {
            session_idle_timeout: Duration::from_secs(1800),
            max_sessions: 1024,
            sse_keepalive: Duration::from_secs(15),
            legacy_sse_enabled: true,
        }
    }
}

/// The caller a request carries, or the unrestricted one if the host inserted none.
fn caller(authz: Option<Extension<McpAuthzRef>>) -> McpAuthzRef {
    authz.map_or_else(unrestricted, |Extension(authz)| authz)
}

#[derive(Debug, Deserialize)]
struct MessageQuery {
    session_id: String,
}

pub fn mcp_router<S>(config: McpTransportConfig) -> (Router<S>, McpShutdownHandle)
where
    S: McpBackend,
{
    let transport_state = McpTransportState::new(SessionLimits {
        idle_timeout: config.session_idle_timeout,
        max_sessions: config.max_sessions,
    });

    // Start global session cleanup task (respects cancellation token)
    spawn_cleanup_task(transport_state.clone());

    let handle = McpShutdownHandle::new(transport_state.clone());

    let keepalive = config.sse_keepalive;

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
                move |Extension(state): Extension<McpTransportState>,
                      authz: Option<Extension<McpAuthzRef>>,
                      headers: HeaderMap| async move {
                    streamable_listen_handler(state, caller(authz), headers, keepalive).await
                },
            )
            .delete(
                |Extension(state): Extension<McpTransportState>,
                 authz: Option<Extension<McpAuthzRef>>,
                 headers: HeaderMap| async move {
                    streamable_delete_handler(state, caller(authz), headers).await
                },
            ),
        );

    // Legacy HTTP+SSE transport (MCP spec 2024-11-05): kept for backwards compatibility with
    // already-configured clients, and unmounted rather than refused when an operator turns it
    // off — a route that is absent has no behaviour to get wrong.
    let router = if config.legacy_sse_enabled {
        router
            .route(
                "/sse",
                get(
                    move |Extension(state): Extension<McpTransportState>,
                          authz: Option<Extension<McpAuthzRef>>| async move {
                        mcp_sse_handler(state, caller(authz), keepalive).await
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
    } else {
        info!("MCP legacy HTTP+SSE transport disabled: /mcp/sse and /mcp/messages are not mounted");
        router
    };

    let router = router.layer(Extension(transport_state));

    (router, handle)
}

async fn mcp_sse_handler(
    state: McpTransportState,
    authz: McpAuthzRef,
    keepalive: Duration,
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

    Sse::new(stream).keep_alive(KeepAlive::new().interval(keepalive).text("keepalive"))
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
    match state
        .claim_session(&query.session_id, key_id.as_deref())
        .await
    {
        SessionClaim::Granted(mcp_session) => {
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
            StatusCode::ACCEPTED.into_response()
        }
        SessionClaim::WrongKey => session_refusal(&query.session_id).into_response(),
        SessionClaim::Unknown => unknown_session_refusal(&query.session_id).into_response(),
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
    let is_initialize = method_of(&payload) == Some("initialize");

    // A session id names server-side state. Continuing someone else's conversation needs
    // more than knowing its id — and continuing one the server no longer holds is refused
    // with 404, which is what tells a client to start over with `initialize`. An `initialize`
    // carrying a stale id is already that fresh start, so it proceeds.
    let key_id = authz.key_id();
    if let Some(session_id) = session_id_of(&headers) {
        match state.claim_session(session_id, key_id.as_deref()).await {
            SessionClaim::Granted(_) => {}
            SessionClaim::WrongKey => return session_refusal(session_id).into_response(),
            SessionClaim::Unknown if is_initialize => {}
            SessionClaim::Unknown => return unknown_session_refusal(session_id).into_response(),
        }
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
///
/// While the stream is open the session is registered as connected, which is what keeps the
/// sweeper from reading a client that is present but quiet as one that has gone. The keep-alives
/// are the reason that is sound: a client that disappears without closing the connection is
/// noticed the next time one of them fails to write, and the guard below then releases the
/// session to the ordinary idle timeout.
async fn streamable_listen_handler(
    state: McpTransportState,
    authz: McpAuthzRef,
    headers: HeaderMap,
    keepalive: Duration,
) -> Response {
    // `Some` only for a stream opened on a session, which is what there is to hold open. A
    // client may open one before `initialize`, and there is nothing to register for that.
    let mut listening = None;
    if let Some(session_id) = session_id_of(&headers) {
        let key_id = authz.key_id();
        match state.claim_session(session_id, key_id.as_deref()).await {
            SessionClaim::Granted(_) => {}
            SessionClaim::WrongKey => return session_refusal(session_id).into_response(),
            SessionClaim::Unknown => return unknown_session_refusal(session_id).into_response(),
        }
        if state.open_listener(session_id).await {
            listening = Some(ListenerGuard {
                session_id: session_id.to_string(),
                state: state.clone(),
            });
        }
    }

    let stream = ListeningStream {
        inner: futures::stream::pending::<Result<Event, Infallible>>(),
        guard: listening,
    };
    Sse::new(stream)
        .keep_alive(KeepAlive::new().interval(keepalive).text("keepalive"))
        .into_response()
}

/// Releases a session's listening-stream registration when the stream is dropped.
///
/// Unlike [`SessionDropGuard`] this does not forget the session. A Streamable HTTP session
/// outlives any one connection by design — that is the whole difference between the two
/// transports — so a closed stream returns the session to the idle timeout rather than ending
/// it.
struct ListenerGuard {
    session_id: String,
    state: McpTransportState,
}

impl Drop for ListenerGuard {
    fn drop(&mut self) {
        let session_id = std::mem::take(&mut self.session_id);
        let state = self.state.clone();
        tokio::spawn(async move {
            state.close_listener(&session_id).await;
            debug!(session_id = %session_id, "MCP listening stream closed");
        });
    }
}

/// A listening stream that holds its [`ListenerGuard`] for as long as the client reads it.
struct ListeningStream<S> {
    inner: S,
    #[allow(dead_code)] // Held for its Drop impl
    guard: Option<ListenerGuard>,
}

impl<S: Stream + Unpin> Stream for ListeningStream<S> {
    type Item = S::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }
}

fn session_id_of(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(MCP_SESSION_ID_HEADER)
        .and_then(|value| value.to_str().ok())
}

/// A session id that names nothing — expired, evicted, or never created.
///
/// 404 is what the Streamable HTTP spec requires here, and it is also the only signal a client
/// gets that its session is gone: a request that were answered anyway would leave the client
/// holding a dead id forever, never learning it should `initialize` again.
fn unknown_session_refusal(session_id: &str) -> impl IntoResponse {
    debug!(session_id = %session_id, "MCP request presented an unknown session id");
    (
        StatusCode::NOT_FOUND,
        Json(json!({
            "error": "Unknown MCP session",
            "message": "this session is not known to the server; start a new one with `initialize`",
        })),
    )
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
