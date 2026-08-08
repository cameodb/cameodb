use std::{
    collections::HashMap,
    convert::Infallible,
    pin::Pin,
    sync::Arc,
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
use futures::{Stream, StreamExt, future::BoxFuture};
use serde::Deserialize;
use serde_json::{Value as JsonValue, json};
use tokio::sync::{Mutex, mpsc};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};
use uuid::Uuid;

/// MCP protocol versions this server supports, newest first.
/// Used for version negotiation during `initialize` and for validating the
/// `MCP-Protocol-Version` HTTP header on the Streamable HTTP transport.
const SUPPORTED_PROTOCOL_VERSIONS: &[&str] = &["2025-06-18", "2025-03-26", "2024-11-05"];

/// Latest protocol version supported (returned when the client requests an
/// unknown version or omits one).
const LATEST_PROTOCOL_VERSION: &str = SUPPORTED_PROTOCOL_VERSIONS[0];

/// HTTP header carrying the session identifier on the Streamable HTTP transport.
///
/// Public because the host application's CORS policy has to both allow and expose it —
/// a browser client cannot use the transport otherwise.
pub const MCP_SESSION_ID_HEADER: &str = "mcp-session-id";

/// HTTP header carrying the negotiated protocol version on subsequent requests.
const MCP_PROTOCOL_VERSION_HEADER: &str = "mcp-protocol-version";

/// Negotiate the protocol version: echo the client's requested version if we
/// support it, otherwise fall back to our latest supported version (per MCP spec).
fn negotiate_protocol_version(requested: Option<&str>) -> &'static str {
    match requested {
        Some(req) => SUPPORTED_PROTOCOL_VERSIONS
            .iter()
            .find(|version| **version == req)
            .copied()
            .unwrap_or(LATEST_PROTOCOL_VERSION),
        None => LATEST_PROTOCOL_VERSION,
    }
}

#[derive(Clone)]
struct McpTransportState {
    inner: Arc<Mutex<McpTransportInner>>,
    cancel: CancellationToken,
}

impl Default for McpTransportState {
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(McpTransportInner::default())),
            cancel: CancellationToken::new(),
        }
    }
}

impl McpTransportState {
    /// Gracefully shut down all MCP sessions.
    /// Drops every sender so SSE streams terminate, then clears the session map.
    async fn shutdown(&self) {
        self.cancel.cancel();
        let mut inner = self.inner.lock().await;
        let count = inner.sessions.len();
        inner.sessions.clear();
        if count > 0 {
            info!(
                sessions = count,
                "MCP transport: all sessions closed on shutdown"
            );
        }
    }

    /// Create a new Streamable HTTP session (no SSE push channel) and return its id.
    /// The id is a cryptographically random UUID per the MCP spec recommendation.
    async fn create_session(&self, key_id: Option<String>) -> String {
        let session_id = Uuid::new_v4().to_string();
        let mut inner = self.inner.lock().await;
        inner.sessions.insert(
            session_id.clone(),
            McpSession {
                sender: None,
                last_activity: std::time::Instant::now(),
                key_id,
            },
        );
        session_id
    }

    /// Remove a session by id, if `key_id` is the key that created it.
    async fn remove_session(&self, session_id: &str, key_id: Option<&str>) -> SessionAccess {
        let mut inner = self.inner.lock().await;
        match inner.sessions.get(session_id) {
            None => SessionAccess::Unknown,
            Some(session) if session.owned_by(key_id) => {
                inner.sessions.remove(session_id);
                SessionAccess::Granted
            }
            Some(_) => SessionAccess::WrongKey,
        }
    }

    /// Check that `key_id` may act on `session_id`, refreshing its activity clock if so.
    async fn claim_session(&self, session_id: &str, key_id: Option<&str>) -> SessionAccess {
        let mut inner = self.inner.lock().await;
        match inner.sessions.get_mut(session_id) {
            None => SessionAccess::Unknown,
            Some(session) if session.owned_by(key_id) => {
                session.last_activity = std::time::Instant::now();
                SessionAccess::Granted
            }
            Some(_) => SessionAccess::WrongKey,
        }
    }
}

/// The outcome of presenting a session id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionAccess {
    Granted,
    /// No such session. Not an authorization failure — a session may simply have expired,
    /// and each transport already has its own answer for that.
    Unknown,
    /// The session exists and belongs to a different key.
    WrongKey,
}

impl McpSession {
    /// A session created by an identified caller may only be continued by that same caller.
    /// One created without identity (auth off) is not bound to anyone.
    fn owned_by(&self, key_id: Option<&str>) -> bool {
        match &self.key_id {
            None => true,
            Some(owner) => key_id == Some(owner.as_str()),
        }
    }
}

/// Opaque handle returned by [`mcp_router`] to trigger graceful MCP shutdown.
#[derive(Clone)]
pub struct McpShutdownHandle {
    state: McpTransportState,
}

impl McpShutdownHandle {
    /// Gracefully shut down the MCP transport.
    /// Cancels the cleanup task and drops all active SSE session senders.
    pub async fn shutdown(&self) {
        info!("MCP shutdown: draining sessions");
        self.state.shutdown().await;
    }
}

#[derive(Default)]
struct McpTransportInner {
    next_session_id: u64,
    sessions: HashMap<String, McpSession>,
}

#[derive(Clone)]
struct McpSession {
    /// SSE push channel. `Some` for legacy SSE sessions (server pushes responses
    /// over the stream); `None` for Streamable HTTP sessions where responses are
    /// returned inline on the POST request.
    sender: Option<mpsc::UnboundedSender<Event>>,
    last_activity: std::time::Instant,
    /// The key that created this session, if the host identified one.
    ///
    /// A session id travels in a header and names a conversation the server keeps state
    /// for. Without this, learning someone else's session id would be enough to continue
    /// their conversation.
    key_id: Option<String>,
}

/// What an MCP operation requires of its caller.
///
/// Mirrors the host's capability set one for one. The mcp crate keeps its own copy because
/// it must not depend on the server crate — the host maps between them in its [`McpAuthz`]
/// implementation, which is the single place the two vocabularies meet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpCapability {
    Read,
    Write,
    IndexAdmin,
    NodeAdmin,
}

impl McpCapability {
    pub fn as_str(self) -> &'static str {
        match self {
            McpCapability::Read => "read",
            McpCapability::Write => "write",
            McpCapability::IndexAdmin => "index-admin",
            McpCapability::NodeAdmin => "node-admin",
        }
    }
}

/// The caller, as much of them as this crate needs to know.
///
/// Implemented by the host so identity reaches the tool dispatcher without this crate
/// learning any of the host's types. `/mcp` is a single JSON-RPC path, so path-level
/// middleware cannot see which tool or index is in play — everything below the transport
/// has to ask.
pub trait McpAuthz: Send + Sync + 'static {
    /// Non-reversible fingerprint of the key, for session binding and log lines. `None`
    /// when the host does not identify callers.
    fn key_id(&self) -> Option<String>;

    /// Whether this caller may touch `index`.
    fn allows_index(&self, index: &str) -> bool;

    /// Whether this caller holds `capability`.
    fn has(&self, capability: McpCapability) -> bool;
}

/// How identity is carried through the transport. Cheap to clone into a spawned task.
pub type McpAuthzRef = Arc<dyn McpAuthz>;

/// The caller when the host has no authorization layer in front of this router.
///
/// A permissive default is correct only because the host decides whether it is used: with
/// `[security]` off there is no identity to enforce, and with it on the middleware always
/// supplies a real one.
#[derive(Debug, Clone, Copy)]
pub struct McpUnrestricted;

impl McpAuthz for McpUnrestricted {
    fn key_id(&self) -> Option<String> {
        None
    }

    fn allows_index(&self, _index: &str) -> bool {
        true
    }

    fn has(&self, _capability: McpCapability) -> bool {
        true
    }
}

/// What a tool requires, or `None` if it is not a tool this server knows.
///
/// **Deny by default.** A tool added to [`mcp_tools`] without a row here cannot be called at
/// all, which is the failure that gets noticed; inheriting `Read` from its neighbours is the
/// failure that does not. `every_advertised_tool_has_a_capability` keeps the two in step.
pub fn tool_capability(name: &str) -> Option<McpCapability> {
    match name {
        "search_index" | "search_indexes" | "get_index" | "list_indexes" | "validate_query"
        | "get_index_stats" => Some(McpCapability::Read),
        _ => None,
    }
}

/// The caller for a request the host did not identify.
fn unrestricted() -> McpAuthzRef {
    Arc::new(McpUnrestricted)
}

/// The caller a request carries, or the unrestricted one if the host inserted none.
fn caller(authz: Option<Extension<McpAuthzRef>>) -> McpAuthzRef {
    authz.map_or_else(unrestricted, |Extension(authz)| authz)
}

#[derive(Debug, Deserialize)]
struct MessageQuery {
    session_id: String,
}

/// Sort specification for search results
#[derive(Debug, Clone, Deserialize)]
pub struct SortSpec {
    /// Field name to sort by
    pub field: String,
    /// Sort order (default: Asc)
    #[serde(default)]
    pub order: SortOrder,
}

/// Sort order direction
#[derive(Debug, Clone, Copy, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum SortOrder {
    #[default]
    Asc,
    Desc,
}

#[derive(Debug, Clone, Deserialize)]
pub struct McpIndexSearchRequest {
    pub index: String,
    #[serde(default)]
    pub fields: Option<Vec<String>>,
    #[serde(default)]
    pub sort: Option<SortSpec>,
}

/// The operations MCP exposes, implemented by the host.
///
/// Methods that **name** their index take no caller: [`call_tool`] has the name in hand and
/// refuses a disallowed one before dispatching, so the check happens once rather than in
/// every implementation. Methods that **enumerate** indexes, or that resolve a name from a
/// URI, take an [`McpAuthzRef`] — only the implementation knows which part of its response
/// is a list of index names.
pub trait McpBackend: Clone + Send + Sync + 'static {
    fn search_index(
        &self,
        index: McpIndexSearchRequest,
        query: String,
        limit: Option<usize>,
    ) -> BoxFuture<'_, Result<JsonValue, String>>;

    fn search_indexes(
        &self,
        indexes: Vec<McpIndexSearchRequest>,
        query: String,
        limit: Option<usize>,
    ) -> BoxFuture<'_, Result<JsonValue, String>>;

    fn get_index(&self, index: String) -> BoxFuture<'_, Result<JsonValue, String>>;

    fn list_indexes(&self, authz: McpAuthzRef) -> BoxFuture<'_, Result<JsonValue, String>>;

    fn validate_query(
        &self,
        index: Option<String>,
        partial_field: Option<String>,
        query: Option<String>,
    ) -> BoxFuture<'_, Result<JsonValue, String>>;

    fn get_index_stats(
        &self,
        index: Option<String>,
        authz: McpAuthzRef,
    ) -> BoxFuture<'_, Result<JsonValue, String>>;

    fn list_resources(&self, authz: McpAuthzRef) -> BoxFuture<'_, Result<JsonValue, String>>;

    fn read_resource(
        &self,
        uri: String,
        authz: McpAuthzRef,
    ) -> BoxFuture<'_, Result<JsonValue, String>>;
}

#[derive(Debug, Deserialize)]
struct JsonRpcRequest {
    id: Option<JsonValue>,
    method: String,
    #[serde(default)]
    params: JsonValue,
}

#[derive(Debug, Deserialize)]
struct ToolCallParams {
    name: String,
    #[serde(default)]
    arguments: JsonValue,
}

#[derive(Debug, Deserialize)]
struct SearchIndexArgs {
    index: String,
    query: String,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    fields: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
struct SearchIndexesArgs {
    indexes: Vec<McpIndexSearchRequest>,
    query: String,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct GetIndexArgs {
    index: String,
}

#[derive(Debug, Deserialize)]
struct ValidateQueryArgs {
    #[serde(default)]
    index: Option<String>,
    #[serde(default)]
    partial_field: Option<String>,
    #[serde(default)]
    query: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GetIndexStatsArgs {
    #[serde(default)]
    index: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ReadResourceArgs {
    uri: String,
}

pub fn mcp_router<S>() -> (Router<S>, McpShutdownHandle)
where
    S: McpBackend,
{
    let transport_state = McpTransportState::default();

    // Start global session cleanup task (respects cancellation token)
    let cleanup_state = transport_state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        loop {
            tokio::select! {
                _ = cleanup_state.cancel.cancelled() => {
                    debug!("MCP cleanup task: shutdown signal received");
                    break;
                }
                _ = interval.tick() => {
                    let mut inner = cleanup_state.inner.lock().await;
                    let now = std::time::Instant::now();
                    let timeout = Duration::from_secs(300); // 5 minutes timeout

                    // Remove sessions: only clean up if SSE connection is closed AND inactive
                    inner.sessions.retain(|session_id, session| {
                        // Legacy SSE sessions with a live push channel are kept regardless
                        // of last POST activity. Streamable HTTP sessions (sender = None)
                        // and disconnected SSE sessions fall through to the inactivity check.
                        if let Some(sender) = &session.sender
                            && !sender.is_closed()
                        {
                            return true;
                        }
                        let is_active = now.duration_since(session.last_activity) < timeout;
                        if !is_active {
                            info!(session_id = %session_id, "Cleaning up inactive MCP session");
                        }
                        is_active
                    });
                }
            }
        }
        debug!("MCP cleanup task: exited");
    });

    let handle = McpShutdownHandle {
        state: transport_state.clone(),
    };

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
        let mut inner = state.inner.lock().await;
        inner.next_session_id += 1;
        let session_id = format!("mcp-session-{}", inner.next_session_id);
        let (tx, rx) = mpsc::unbounded_channel();
        let now = std::time::Instant::now();

        // Legacy SSE session ids are sequential, so the next one is guessable. Binding the
        // session to its creator is what stops that from being useful.
        let session = McpSession {
            sender: Some(tx.clone()),
            last_activity: now,
            key_id: authz.key_id(),
        };

        inner.sessions.insert(session_id.clone(), session);

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
            let mut inner = state.inner.lock().await;
            if inner.sessions.remove(&session_id).is_some() {
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
        SessionAccess::Granted => {
            let inner = state.inner.lock().await;
            inner.sessions.get(&query.session_id).cloned()
        }
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

    let is_initialize =
        payload.get("method").and_then(|method| method.as_str()) == Some("initialize");

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

fn parse_json_rpc_request(payload: JsonValue) -> Result<JsonRpcRequest, serde_json::Error> {
    serde_json::from_value::<JsonRpcRequest>(payload)
}

fn success_response(id: Option<JsonValue>, result: JsonValue) -> JsonValue {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": result,
    })
}

fn json_to_pretty_string(value: &JsonValue) -> String {
    serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string())
}

fn error_response(id: Option<JsonValue>, code: i64, message: String) -> JsonValue {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {
            "code": code,
            "message": message,
        }
    })
}

async fn handle_rpc_request<S>(
    backend: S,
    request: JsonRpcRequest,
    authz: &McpAuthzRef,
) -> Option<JsonValue>
where
    S: McpBackend,
{
    match request.method.as_str() {
        // --- Lifecycle ---
        "initialize" => {
            let client_version = request
                .params
                .get("protocolVersion")
                .and_then(|v| v.as_str());
            let negotiated = negotiate_protocol_version(client_version);
            Some(success_response(
                request.id,
                json!({
                    "protocolVersion": negotiated,
                    "capabilities": {
                        "tools": {},
                        "resources": {},
                        "prompts": {}
                    },
                    "serverInfo": {
                        "name": "cameodb-mcp",
                        "version": env!("CARGO_PKG_VERSION")
                    }
                }),
            ))
        }
        "ping" => Some(success_response(request.id, json!({}))),

        // --- Notifications (no response per JSON-RPC spec) ---
        "notifications/initialized" | "notifications/cancelled" => {
            debug!(method = %request.method, "MCP notification received");
            // Don't send response for notifications per JSON-RPC spec
            None
        }

        // --- Resources ---
        "resources/list" => Some(match backend.list_resources(authz.clone()).await {
            Ok(resources) => success_response(request.id, json!({ "resources": resources })),
            Err(err) => error_response(request.id, -32603, err),
        }),
        "resources/read" => Some(
            match serde_json::from_value::<ReadResourceArgs>(request.params) {
                Ok(params) => match backend
                    .read_resource(params.uri.clone(), authz.clone())
                    .await
                {
                    Ok(content) => success_response(
                        request.id,
                        json!({
                            "contents": [{
                                "uri": params.uri,
                                "mimeType": "application/json",
                                "text": json_to_pretty_string(&content),
                            }]
                        }),
                    ),
                    Err(err) => error_response(request.id, -32603, err),
                },
                Err(err) => error_response(
                    request.id,
                    -32602,
                    format!("Invalid resources/read params: {err}"),
                ),
            },
        ),

        // --- Prompts ---
        "prompts/list" => Some(success_response(
            request.id,
            json!({
                "prompts": [{
                    "name": "cameodb-orchestrator",
                    "description": "Universal Data Retrieval & Orchestration Skill for CameoDB.",
                    "arguments": []
                }]
            }),
        )),
        "prompts/get" => {
            let name = request
                .params
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if name == "cameodb-orchestrator" {
                Some(success_response(
                    request.id,
                    json!({
                        "description": "Universal Data Retrieval & Orchestration Skill for CameoDB.",
                        "messages": [{
                            "role": "user",
                            "content": {
                                "type": "text",
                                "text": ORCHESTRATOR_SKILL
                            }
                        }]
                    }),
                ))
            } else {
                Some(error_response(
                    request.id,
                    -32602,
                    format!("Unknown prompt: {name}"),
                ))
            }
        }

        // --- Tools ---
        "tools/list" => Some(success_response(
            request.id,
            json!({ "tools": visible_tools(authz) }),
        )),
        "tools/call" => Some(
            match serde_json::from_value::<ToolCallParams>(request.params) {
                Ok(params) => match call_tool(backend, params, authz).await {
                    Ok(result) => success_response(
                        request.id,
                        json!({
                            "content": [{
                                "type": "text",
                                "text": json_to_pretty_string(&result),
                            }],
                            "isError": false,
                        }),
                    ),
                    Err(err) => success_response(
                        request.id,
                        json!({
                            "content": [{
                                "type": "text",
                                "text": err,
                            }],
                            "isError": true,
                        }),
                    ),
                },
                Err(err) => error_response(
                    request.id,
                    -32602,
                    format!("Invalid tools/call params: {err}"),
                ),
            },
        ),

        other => Some(error_response(
            request.id,
            -32601,
            format!("Unsupported MCP method: {other}"),
        )),
    }
}

async fn call_tool<S>(
    backend: S,
    params: ToolCallParams,
    authz: &McpAuthzRef,
) -> Result<JsonValue, String>
where
    S: McpBackend,
{
    // Capability before arguments: an unknown tool and a forbidden one both stop here, so a
    // tool that was never classified cannot be reached by naming it.
    let Some(required) = tool_capability(&params.name) else {
        return Err(format!("Unsupported MCP tool: {}", params.name));
    };
    if !authz.has(required) {
        return Err(format!(
            "tool '{}' requires the '{}' capability, which this key does not hold",
            params.name,
            required.as_str()
        ));
    }

    match params.name.as_str() {
        "search_index" => {
            let args: SearchIndexArgs = serde_json::from_value(params.arguments)
                .map_err(|err| format!("Invalid search_index arguments: {err}"))?;
            check_index(authz, &args.index)?;
            backend
                .search_index(
                    McpIndexSearchRequest {
                        index: args.index,
                        fields: args.fields,
                        sort: None,
                    },
                    args.query,
                    args.limit,
                )
                .await
        }
        "search_indexes" => {
            let args: SearchIndexesArgs = serde_json::from_value(params.arguments)
                .map_err(|err| format!("Invalid search_indexes arguments: {err}"))?;
            // Refuse the whole call rather than quietly dropping the indexes this key may
            // not read: partial results that look complete are worse than an error.
            for request in &args.indexes {
                check_index(authz, &request.index)?;
            }
            backend
                .search_indexes(args.indexes, args.query, args.limit)
                .await
        }
        "get_index" => {
            let args: GetIndexArgs = serde_json::from_value(params.arguments)
                .map_err(|err| format!("Invalid get_index arguments: {err}"))?;
            check_index(authz, &args.index)?;
            backend.get_index(args.index).await
        }
        "list_indexes" => backend.list_indexes(authz.clone()).await,
        "validate_query" => {
            let args: ValidateQueryArgs = serde_json::from_value(params.arguments)
                .map_err(|err| format!("Invalid validate_query arguments: {err}"))?;
            // Validation reports an index's field names, so it is a read of that index.
            if let Some(index) = &args.index {
                check_index(authz, index)?;
            }
            backend
                .validate_query(args.index, args.partial_field, args.query)
                .await
        }
        "get_index_stats" => {
            let args: GetIndexStatsArgs = serde_json::from_value(params.arguments)
                .map_err(|err| format!("Invalid get_index_stats arguments: {err}"))?;
            // With no index named this aggregates across the catalogue, which the backend
            // filters to the caller's scope.
            if let Some(index) = &args.index {
                check_index(authz, index)?;
            }
            backend.get_index_stats(args.index, authz.clone()).await
        }
        // Unreachable: `tool_capability` above rejects anything not in this match.
        other => Err(format!("Unsupported MCP tool: {other}")),
    }
}

/// The tools this caller could actually call.
///
/// Advertising a tool that [`call_tool`] will refuse invites an agent to plan around it and
/// then fail mid-task. A tool with no row in the capability table is not advertised either —
/// the deny default applies to the catalogue as much as to the call.
fn visible_tools(authz: &McpAuthzRef) -> Vec<JsonValue> {
    mcp_tools()
        .into_iter()
        .filter(|tool| {
            tool.get("name")
                .and_then(|name| name.as_str())
                .and_then(tool_capability)
                .is_some_and(|capability| authz.has(capability))
        })
        .collect()
}

/// Refuse a tool call that names an index outside the caller's scope.
fn check_index(authz: &McpAuthzRef, index: &str) -> Result<(), String> {
    if authz.allows_index(index) {
        Ok(())
    } else {
        Err(format!("this key is not permitted on index '{index}'"))
    }
}

const ORCHESTRATOR_SKILL: &str = r#"# CameoDB Agent Skill: Universal Data Retrieval & Orchestration

## Role and Purpose
You are an expert Data Retrieval Analyst powered by CameoDB, a high-performance, fully-indexed knowledge base. Your sole objective is to extract precise information from CameoDB indexes through optimized queries. Data ingestion is handled externally — you never write data. You retrieve, synthesize, and present answers based **only** on the returned facts.

## Core Directives & Anti-Hallucination Rules
1. **Zero Hallucination:** You MUST use ONLY the exact data returned by the tools. NEVER invent, guess, or inject prior knowledge into database results.
2. **Acknowledge Gaps:** If the database returns partial or no results, state exactly what was found and nothing more.
3. **Schema First:** Never guess field names. If you are unsure of the index structure, you must use `get_index` or `list_indexes` before searching.
4. **Read-Only:** You do not write, ingest, or modify data. All data is loaded by external processes. Your job is retrieval only.

## The Orchestration Workflow
When a user asks a question, you must follow this deterministic loop:

### Step 1: Domain & Schema Discovery
* **Action:** If you do not know which index contains the answer, use `list_indexes`. Read the descriptions to find the right dataset.
* **Action:** Once an index is identified, use `get_index` to read the descriptive field names.
* *Logic:* Use the field names to understand the context. (e.g., If you see `customer_id` and `cart_total`, the domain is E-commerce. If you see `process.pid` and `file_hash`, the domain is Security).

### Step 2: Query Formulation & Validation
* **Action:** Construct your query using CameoDB's Tantivy syntax.
* **Rule:** Map the user's intent to the specific data types found in Step 1.
    * *Text fields:* Use phrases (`title:"exact phrase"`), prefix (`name:john*`), or slop (`body:"near this"~2`).
    * *Numeric/Date fields:* Use ranges (`price:[10.0 TO 100.0]`, `created_at:>2024-01-01`).
    * *Exact ID lookup:* When the user's question provides an exact document `id` or any field with `shadow: true` property, query it directly (e.g., `id:ABC123`). This is the fastest retrieval path — CameoDB bypasses the search index and reads directly from the KV store.
* **Action:** If the query is highly complex or you are unsure of syntax compatibility, use the `validate_query` tool to check your structure before executing.

### Step 3: Precision Execution & Field Projection
* **Action:** Execute the query using `search_index` (for a single index) or `search_indexes` (for federated searches across domains).
* **Rule:** Optimize your queries. Use boosting (`title:rust^3 OR body:rust`) to ensure the most relevant documents are returned first. Use `limit N` to prevent overflowing your context window.
* **Field Projection Strategy (`return` clause):** Always request **only the fields needed** to answer the user's goal. However, include additional fields when they provide **business-domain context** or enable **pivoting** to related records.
    * *Minimal set:* Request exact fields required for the answer (e.g., `return name, price` for a price lookup).
    * *Context set:* Add fields that reveal relationships or enable follow-up analysis (e.g., `return customer_id, order_id, status, total` — `customer_id` enables pivoting to customer history).
    * *Domain expertise:* Use your understanding of the business domain to infer which fields are identifiers, timestamps, or foreign keys that unlock deeper investigation.
    * *Ordering:* Fields are returned in the exact order specified in the `return` clause or `fields` parameter. Metadata fields (like `_score`) are always included automatically.
* **Sorting Strategy (`sort` clause):** When results need to be ordered by a specific field (e.g., newest first, highest price first), use inline `sort field:asc` or `sort field:desc` in the query string, or the `sort` parameter on `search_indexes`.
    * *Supported types:* u64, i64, f64, date (FAST fields), and text/string (alphabetic sort).
    * *Default order:* Ascending (`asc`) when not specified.
    * *Example:* `title:rust sort year:desc limit 10` returns the 10 most recent results matching "rust" in title.
    * *Federated sort:* When using `search_indexes` with a per-index `sort` spec, results are merged by the sort field across all indexes, preserving global ordering.

### Step 4: Iteration and Pivoting
* **Action:** Analyze the results. If a document contains a unique identifier (like a `session_id`, `user_id`, or `transaction_hash`), and the user's question requires more context, **automatically pivot**.
* *Logic:* Formulate a new `search_index` query using that identifier to pull all related records and build a complete timeline or picture.
* *Field-driven pivoting:* When the initial `return` clause included contextual fields (e.g., `category_id`, `parent_order_id`), use those to expand the investigation without re-querying the original record.

## Advanced Querying: Any Field, Any Type
CameoDB indexes every field. There are no "unqueryable" fields. Use the full Tantivy syntax against any indexed field:
- **Existence queries:** `field:*` matches documents where the field is present.
- **Negation:** `-status:deleted` excludes deleted records.
- **Boolean logic:** `(urgent:true OR priority:>5) AND assignee:john`
- **Nested access:** Use dot notation for nested JSON fields (e.g., `metadata.source:api`).

## Output Formatting
When presenting your final answer to the user:
1. Cite the index(es) where the data was found.
2. Present structured data (like timelines or aggregations) in Markdown tables.
3. Explicitly state the query logic and `return` field selection you used so the user understands how the answer was derived.
4. Note any pivot queries executed and why they were necessary."#;

fn mcp_tools() -> Vec<JsonValue> {
    vec![
        json!({
            "name": "search_index",
            "title": "Search Index",
            "description": "Execute full-text search on a single CameoDB index. Query syntax supports field:value targeting, phrase queries (field:\"words\"), boolean operators (AND, OR, NOT), grouping with parentheses, range queries (field:[low TO high]), and date comparisons (field:>2024-01-01). \n\nCRITICAL ANTI-HALLUCINATION RULE FOR AGENTS:\nWhen answering questions based on CameoDB results, you MUST use ONLY the exact data returned by this tool. Do NOT combine database results with your own prior knowledge. If the index returns partial or incomplete information, state exactly what was found and nothing more. NEVER invent or hallucinate fields or values not explicitly present in the query results.\n\nPRO TIPS FOR AGENTS:\n1. Use Tantivy boosting to improve relevance (e.g., 'title:rust^3 OR body:rust').\n2. The query string supports inline modifiers: 'return field1,field2' for field projection, 'limit N' for result count, and 'sort field:asc' or 'sort field:desc' for sorting.\n3. Field projection: when using the 'fields' parameter or inline 'return', fields appear in the response in the exact order specified. Metadata fields (like '_score') are always included automatically.\n4. Sorting: use inline 'sort field:asc' or 'sort field:desc' in the query string, or the 'sort' parameter on search_indexes. Supported sort field types: u64, i64, f64, date (FAST fields), and text/string (alphabetic post-fetch sort). Default order is ascending.\n5. If you receive a field error or do not know the available fields, run the 'get_index' tool first to view the schema.\n\nQUERY SYNTAX QUICK REFERENCE:\n- Terms: rust database (AND by default)\n- Field targeting: title:rust (only applies to next term)\n- Term prefix: title:quick* (matches quickwit, quickstart)\n- Phrases: title:\"rust programming\"\n- Phrase slop (proximity): body:\"small bike\"~2\n- Phrase prefix: \"big bad wo\"* (matches 'big bad wolf')\n- Boolean: title:rust AND author:doe | OR | NOT (UPPERCASE required)\n- Must/must-not: +title:rust -author:smith\n- Grouping: (title:rust OR title:go) AND year:[2020 TO 2024]\n- Range (inclusive []): year:[2020 TO 2024]\n- Range (exclusive {}): score:{0 TO 100}\n- Range (comparison): age:>=18 or score:<100\n- Unbounded range: price:[10.0 TO *] or age:[* TO 30]\n- Set operator: status: IN [active pending review]\n- Boosting: title:rust^3 OR body:rust\n- Exists: author:* (matches docs where author field is set)\n- All docs: *\n- Date: created_at:>2024-01-01, created_at:[2024-01-01 TO 2024-12-31]\n- Escape specials: k8s\\.component\\.name:value (reserved: + ^ ` : { } \" [ ] ( ) ~ ! \\\\ * SPACE)\n- Field names: If a field name literally contains a dot, escape it (k8s\\.node) to avoid JSON nested access.\n\nFIELD TYPE IMPACT ON OPERATORS:\n- text: all operators (phrases, slop, prefix, IN set, boost, range, exists)\n- string/exact: exact match, prefix, IN set, exists (no phrases/slop)\n- numeric (i64/u64/f64): exact, comparisons (>, <), range [], {}, boost, exists (no phrases/IN)\n- date: exact, comparisons (>/</>=/<=), range, exists (no phrases/IN)\n- boolean: true/false only, exists (no range/boost)\n- ip: exact, range, exists (no phrases)\n- json: dot notation field.sub:value, nested exists field.sub:* (escape dots with \\\\)\n- facet: path /category/sub, exists\n\nORCHESTRATION TIP: If your query involves an exact document ID or a field marked with `shadow: true`, query it directly (e.g., `id:123`). This bypasses the search index for ultra-fast KV retrieval.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "index": {
                        "type": "string",
                        "description": "Name of the CameoDB index to search."
                    },
                    "query": {
                        "type": "string",
                        "description": "Search query string. Supports field:value, phrases, AND/OR/NOT, ranges, and inline 'return'/'limit'/'sort' modifiers. Use 'limit 0' for count-only queries. Inline sort: 'sort field:asc' or 'sort field:desc'."
                    },
                    "limit": {
                        "type": "integer",
                        "minimum": 0,
                        "description": "Maximum number of results to return. Pass 0 for count-only mode (returns total_hits without document data). If omitted, defaults to 10."
                    },
                    "fields": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Field names to include in results (field projection). Fields are returned in the exact order specified. Metadata fields (like '_score') are always included automatically."
                    }
                },
                "required": ["index", "query"]
            },
            "annotations": {
                "readOnlyHint": true,
                "openWorldHint": false
            }
        }),
        json!({
            "name": "search_indexes",
            "title": "Federated Search",
            "description": "Execute federated search across multiple CameoDB indexes with optional per-index field projection. Results are merged by relevance score (or by the sort field if specified). Each hit includes an '_index_source' field indicating its origin index. Searches execute concurrently across all specified indexes.\n\nCRITICAL ANTI-HALLUCINATION RULE FOR AGENTS:\nWhen answering questions based on CameoDB results, you MUST use ONLY the exact data returned by this tool. Do NOT combine database results with your own prior knowledge. If the index returns partial or incomplete information, state exactly what was found and nothing more. NEVER invent or hallucinate fields or values not explicitly present in the query results.\n\nUses the same query syntax as search_index (field:value, phrases, boolean operators, ranges, boosting, set IN, slop ~, prefix *, must +/-, grouping). The query string also supports inline 'return field1,field2' for projection, 'limit N' for result count, and 'sort field:asc'/'sort field:desc' for sorting. Per-index 'fields' and 'sort' parameters take precedence over inline modifiers. If indexes have different schemas, the query applies to matching fields in each index.\n\nORCHESTRATION TIP: When federating across indexes, pay close attention to the `_index_source` field in your results. Use this to focus subsequent, deeper queries on a single index.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "indexes": {
                        "type": "array",
                        "description": "List of indexes to search, each with optional field projection.",
                        "items": {
                            "type": "object",
                            "properties": {
                                "index": {
                                    "type": "string",
                                    "description": "Name of the CameoDB index."
                                },
                                "fields": {
                                    "type": "array",
                                    "items": { "type": "string" },
                                    "description": "Fields to include from this index. Fields are returned in the exact order specified. Metadata fields (like '_score') are always included automatically."
                                },
                                "sort": {
                                    "type": "object",
                                    "description": "Sort results by a field within this index. Supported types: u64, i64, f64, date (FAST fields), and text/string (alphabetic sort).",
                                    "properties": {
                                        "field": {
                                            "type": "string",
                                            "description": "Field name to sort by. Supports u64, i64, f64, date, and text/string fields."
                                        },
                                        "order": {
                                            "type": "string",
                                            "enum": ["asc", "desc"],
                                            "description": "Sort order. Defaults to asc."
                                        }
                                    },
                                    "required": ["field"]
                                }
                            },
                            "required": ["index"]
                        }
                    },
                    "query": {
                        "type": "string",
                        "description": "Search query applied to all specified indexes."
                    },
                    "limit": {
                        "type": "integer",
                        "minimum": 0,
                        "description": "Maximum total results across all indexes. Pass 0 for count-only mode (returns total_hits without document data). If omitted, defaults to 10."
                    }
                },
                "required": ["indexes", "query"]
            },
            "annotations": {
                "readOnlyHint": true,
                "openWorldHint": false
            }
        }),
        json!({
            "name": "get_index",
            "title": "Get Index",
            "description": "Retrieve schema and statistics for a single CameoDB index. Returns field definitions with types and a 'queryable_fields' array containing per-field 'query_hint' showing exactly which operators (phrases, ranges, IN set, boost, slop, etc.) work with each field's data type. Use this to understand an index's structure before constructing queries.\n\nORCHESTRATION TIP: Review the returned schema to identify potential pivot fields (like foreign keys, user IDs, or hashes) before running your search.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "index": {
                        "type": "string",
                        "description": "Name of the CameoDB index."
                    }
                },
                "required": ["index"]
            },
            "annotations": {
                "readOnlyHint": true,
                "openWorldHint": false
            }
        }),
        json!({
            "name": "list_indexes",
            "title": "List Indexes",
            "description": "List all available CameoDB indexes with their schemas and metadata. Each index includes a 'queryable_fields' array with per-field type and 'query_hint' showing supported operators. Use this as the first discovery step — new indexes are automatically available here with full schema details.",
            "inputSchema": {
                "type": "object",
                "properties": {}
            },
            "annotations": {
                "readOnlyHint": true,
                "openWorldHint": false
            }
        }),
        json!({
            "name": "validate_query",
            "title": "Validate Query",
            "description": "Validate and get guidance on CameoDB search query syntax. Provides field-type-aware suggestions, detects unknown or non-indexed fields, checks query structure (unbalanced quotes/parens, inline modifiers), and returns the full CameoDB query syntax reference. Supply an index name for schema-aware validation.\n\nPRO TIPS FOR AGENTS:\n1. Call with no arguments to get the complete query syntax reference and operator-by-field-type compatibility matrix.\n2. Supply an index name to get schema-aware field validation with type-specific operator hints per field.\n3. Supply a partial_field to get autocomplete suggestions matching available fields.\n4. Supply a query to get structural validation, field recognition, typo detection ('did you mean?'), and per-field operator guidance.\n\nORCHESTRATION TIP: Use this tool immediately if `search_index` returns a syntax error, before attempting to guess the correct format.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "index": {
                        "type": "string",
                        "description": "Index name for schema-aware field validation. Optional."
                    },
                    "partial_field": {
                        "type": "string",
                        "description": "Partial field name for autocomplete suggestions."
                    },
                    "query": {
                        "type": "string",
                        "description": "Query string to validate and analyze."
                    }
                }
            },
            "annotations": {
                "readOnlyHint": true,
                "openWorldHint": false
            }
        }),
        json!({
            "name": "get_index_stats",
            "title": "Get Index Statistics",
            "description": "Return statistics for a single CameoDB index or aggregated statistics across all indexes.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "index": {
                        "type": "string",
                        "description": "Index name. If omitted, returns aggregated statistics for all indexes."
                    }
                }
            },
            "annotations": {
                "readOnlyHint": true,
                "openWorldHint": false
            }
        }),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A caller scoped to one index, holding only `Read`.
    struct Scoped(&'static str);

    impl McpAuthz for Scoped {
        fn key_id(&self) -> Option<String> {
            Some("aabbccdd".to_string())
        }

        fn allows_index(&self, index: &str) -> bool {
            index == self.0
        }

        fn has(&self, capability: McpCapability) -> bool {
            capability == McpCapability::Read
        }
    }

    fn advertised_tool_names() -> Vec<String> {
        mcp_tools()
            .iter()
            .filter_map(|tool| tool.get("name").and_then(|name| name.as_str()))
            .map(str::to_string)
            .collect()
    }

    #[test]
    fn every_advertised_tool_has_a_capability() {
        // The guarantee the table cannot make for itself. A tool added to `mcp_tools`
        // without a row fails here rather than becoming uncallable at runtime — and, more
        // importantly, a *write* tool added later cannot quietly inherit `Read`.
        let unclassified: Vec<String> = advertised_tool_names()
            .into_iter()
            .filter(|name| tool_capability(name).is_none())
            .collect();
        assert!(
            unclassified.is_empty(),
            "advertised but unclassified tools: {unclassified:?}"
        );
    }

    #[test]
    fn an_unknown_tool_is_denied_rather_than_defaulted() {
        assert_eq!(tool_capability("drop_everything"), None);
        assert_eq!(tool_capability(""), None);
        // Case matters: a lookup that normalised would let `Search_Index` through a table
        // written in lower case.
        assert_eq!(tool_capability("SEARCH_INDEX"), None);
    }

    #[test]
    fn the_current_tools_are_all_reads() {
        for name in advertised_tool_names() {
            assert_eq!(
                tool_capability(&name),
                Some(McpCapability::Read),
                "{name} is not a read; it needs its own row and a look at what else changed"
            );
        }
    }

    #[test]
    fn an_unrestricted_caller_holds_everything() {
        let authz = McpUnrestricted;
        assert!(authz.allows_index("payroll"));
        for capability in [
            McpCapability::Read,
            McpCapability::Write,
            McpCapability::IndexAdmin,
            McpCapability::NodeAdmin,
        ] {
            assert!(authz.has(capability));
        }
        assert_eq!(authz.key_id(), None);
    }

    #[test]
    fn a_named_index_outside_the_scope_is_refused() {
        let authz: McpAuthzRef = Arc::new(Scoped("docs"));
        assert!(check_index(&authz, "docs").is_ok());
        let err = check_index(&authz, "payroll").unwrap_err();
        assert!(err.contains("payroll"), "{err}");
    }

    #[tokio::test]
    async fn a_session_belongs_to_the_key_that_created_it() {
        let state = McpTransportState::default();
        let session = state.create_session(Some("aabbccdd".to_string())).await;

        assert_eq!(
            state.claim_session(&session, Some("aabbccdd")).await,
            SessionAccess::Granted
        );
        assert_eq!(
            state.claim_session(&session, Some("11223344")).await,
            SessionAccess::WrongKey
        );
        // No key at all is not a way around the binding.
        assert_eq!(
            state.claim_session(&session, None).await,
            SessionAccess::WrongKey
        );
        assert_eq!(
            state.claim_session("never-existed", Some("aabbccdd")).await,
            SessionAccess::Unknown
        );
    }

    #[tokio::test]
    async fn another_key_cannot_end_someone_elses_session() {
        let state = McpTransportState::default();
        let session = state.create_session(Some("aabbccdd".to_string())).await;

        assert_eq!(
            state.remove_session(&session, Some("11223344")).await,
            SessionAccess::WrongKey
        );
        // Still there: a refused delete must not have deleted anything.
        assert_eq!(
            state.claim_session(&session, Some("aabbccdd")).await,
            SessionAccess::Granted
        );
        assert_eq!(
            state.remove_session(&session, Some("aabbccdd")).await,
            SessionAccess::Granted
        );
        assert_eq!(
            state.claim_session(&session, Some("aabbccdd")).await,
            SessionAccess::Unknown
        );
    }

    #[tokio::test]
    async fn a_session_created_without_identity_is_bound_to_nobody() {
        // Auth off: there is no key to bind to, and binding to "no key" would lock out the
        // caller that created the session.
        let state = McpTransportState::default();
        let session = state.create_session(None).await;
        assert_eq!(
            state.claim_session(&session, None).await,
            SessionAccess::Granted
        );
        assert_eq!(
            state.claim_session(&session, Some("aabbccdd")).await,
            SessionAccess::Granted
        );
    }

    /// A caller holding nothing at all.
    struct NoCapabilities;

    impl McpAuthz for NoCapabilities {
        fn key_id(&self) -> Option<String> {
            None
        }

        fn allows_index(&self, _index: &str) -> bool {
            false
        }

        fn has(&self, _capability: McpCapability) -> bool {
            false
        }
    }

    #[test]
    fn the_catalogue_only_advertises_tools_the_caller_could_call() {
        let reader: McpAuthzRef = Arc::new(Scoped("docs"));
        assert_eq!(visible_tools(&reader).len(), mcp_tools().len());

        // Nothing held, nothing offered. Advertising a tool that the dispatcher will refuse
        // invites an agent to plan around it and fail mid-task.
        let nobody: McpAuthzRef = Arc::new(NoCapabilities);
        assert!(visible_tools(&nobody).is_empty());
    }
}
