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
const MCP_SESSION_ID_HEADER: &str = "mcp-session-id";

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
    async fn create_session(&self) -> String {
        let session_id = Uuid::new_v4().to_string();
        let mut inner = self.inner.lock().await;
        inner.sessions.insert(
            session_id.clone(),
            McpSession {
                sender: None,
                last_activity: std::time::Instant::now(),
            },
        );
        session_id
    }

    /// Remove a session by id. Returns `true` if a session was removed.
    async fn remove_session(&self, session_id: &str) -> bool {
        let mut inner = self.inner.lock().await;
        inner.sessions.remove(session_id).is_some()
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

    fn list_indexes(&self) -> BoxFuture<'_, Result<JsonValue, String>>;

    fn validate_query(
        &self,
        index: Option<String>,
        partial_field: Option<String>,
        query: Option<String>,
    ) -> BoxFuture<'_, Result<JsonValue, String>>;

    fn get_index_stats(&self, index: Option<String>) -> BoxFuture<'_, Result<JsonValue, String>>;

    fn list_resources(&self) -> BoxFuture<'_, Result<JsonValue, String>>;

    fn read_resource(&self, uri: String) -> BoxFuture<'_, Result<JsonValue, String>>;
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
                 headers: HeaderMap,
                 Json(payload): Json<JsonValue>| async move {
                    process_streamable_http(app_state, state, headers, payload).await
                },
            )
            .get(streamable_listen_handler)
            .delete(
                |Extension(state): Extension<McpTransportState>, headers: HeaderMap| async move {
                    streamable_delete_handler(state, headers).await
                },
            ),
        )
        // Legacy HTTP+SSE transport (MCP spec 2024-11-05): kept for backwards
        // compatibility with already-configured clients.
        .route(
            "/sse",
            get(mcp_sse_handler).post(
                |State(app_state): State<S>, Json(payload): Json<JsonValue>| async move {
                    process_mcp_http_message(app_state, payload).await
                },
            ),
        )
        .route(
            "/messages",
            post(
                |State(app_state): State<S>,
                 Query(query): Query<MessageQuery>,
                 Extension(state): Extension<McpTransportState>,
                 Json(payload): Json<JsonValue>| async move {
                    process_mcp_message(app_state, query, state, payload).await
                },
            ),
        )
        .layer(Extension(transport_state));

    (router, handle)
}

async fn mcp_sse_handler(
    Extension(state): Extension<McpTransportState>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let (session_id, rx) = {
        let mut inner = state.inner.lock().await;
        inner.next_session_id += 1;
        let session_id = format!("mcp-session-{}", inner.next_session_id);
        let (tx, rx) = mpsc::unbounded_channel();
        let now = std::time::Instant::now();

        let session = McpSession {
            sender: Some(tx.clone()),
            last_activity: now,
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
    payload: JsonValue,
) -> impl IntoResponse {
    let session = {
        let mut inner = state.inner.lock().await;
        if let Some(session) = inner.sessions.get_mut(&query.session_id) {
            session.last_activity = std::time::Instant::now();
            Some(session.clone())
        } else {
            None
        }
    };

    match session {
        Some(mcp_session) => {
            let sender = mcp_session.sender.clone();
            let session_id = query.session_id.clone();

            // Spawn background task to process message asynchronously
            tokio::spawn(async move {
                let maybe_response = match parse_json_rpc_request(payload) {
                    Ok(request) => handle_rpc_request(app_state, request).await,
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
    payload: JsonValue,
) -> impl IntoResponse {
    match parse_json_rpc_request(payload) {
        Ok(request) => match handle_rpc_request(app_state, request).await {
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
    headers: HeaderMap,
    payload: JsonValue,
) -> Response {
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
        Ok(request) => match handle_rpc_request(app_state, request).await {
            Some(response) => {
                if is_initialize {
                    // Establish a session and advertise it via the MCP-Session-Id header.
                    let session_id = state.create_session().await;
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
async fn streamable_listen_handler() -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let stream = futures::stream::pending::<Result<Event, Infallible>>();
    Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keepalive"),
    )
}

/// Streamable HTTP transport DELETE handler (MCP spec 2025-03-26+).
///
/// Explicitly terminates the session identified by the `MCP-Session-Id` header.
/// Returns `200 OK` if removed, `404 Not Found` if unknown, `400 Bad Request`
/// if the header is missing.
async fn streamable_delete_handler(state: McpTransportState, headers: HeaderMap) -> StatusCode {
    match headers
        .get(MCP_SESSION_ID_HEADER)
        .and_then(|value| value.to_str().ok())
    {
        Some(session_id) => {
            if state.remove_session(session_id).await {
                info!(session_id = %session_id, "MCP Streamable HTTP session terminated");
                StatusCode::OK
            } else {
                StatusCode::NOT_FOUND
            }
        }
        None => StatusCode::BAD_REQUEST,
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

async fn handle_rpc_request<S>(backend: S, request: JsonRpcRequest) -> Option<JsonValue>
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
        "resources/list" => Some(match backend.list_resources().await {
            Ok(resources) => success_response(request.id, json!({ "resources": resources })),
            Err(err) => error_response(request.id, -32603, err),
        }),
        "resources/read" => Some(
            match serde_json::from_value::<ReadResourceArgs>(request.params) {
                Ok(params) => match backend.read_resource(params.uri.clone()).await {
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
            json!({ "tools": mcp_tools() }),
        )),
        "tools/call" => Some(
            match serde_json::from_value::<ToolCallParams>(request.params) {
                Ok(params) => match call_tool(backend, params).await {
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

async fn call_tool<S>(backend: S, params: ToolCallParams) -> Result<JsonValue, String>
where
    S: McpBackend,
{
    match params.name.as_str() {
        "search_index" => {
            let args: SearchIndexArgs = serde_json::from_value(params.arguments)
                .map_err(|err| format!("Invalid search_index arguments: {err}"))?;
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
            backend
                .search_indexes(args.indexes, args.query, args.limit)
                .await
        }
        "get_index" => {
            let args: GetIndexArgs = serde_json::from_value(params.arguments)
                .map_err(|err| format!("Invalid get_index arguments: {err}"))?;
            backend.get_index(args.index).await
        }
        "list_indexes" => backend.list_indexes().await,
        "validate_query" => {
            let args: ValidateQueryArgs = serde_json::from_value(params.arguments)
                .map_err(|err| format!("Invalid validate_query arguments: {err}"))?;
            backend
                .validate_query(args.index, args.partial_field, args.query)
                .await
        }
        "get_index_stats" => {
            let args: GetIndexStatsArgs = serde_json::from_value(params.arguments)
                .map_err(|err| format!("Invalid get_index_stats arguments: {err}"))?;
            backend.get_index_stats(args.index).await
        }
        other => Err(format!("Unsupported MCP tool: {other}")),
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
            "description": "Execute full-text search on a single CameoDB index. Query syntax supports field:value targeting, phrase queries (field:\"words\"), boolean operators (AND, OR, NOT), grouping with parentheses, range queries (field:[low TO high]), and date comparisons (field:>2024-01-01). \n\nCRITICAL ANTI-HALLUCINATION RULE FOR AGENTS:\nWhen answering questions based on CameoDB results, you MUST use ONLY the exact data returned by this tool. Do NOT combine database results with your own prior knowledge. If the index returns partial or incomplete information, state exactly what was found and nothing more. NEVER invent or hallucinate fields or values not explicitly present in the query results.\n\nPRO TIPS FOR AGENTS:\n1. Use Tantivy boosting to improve relevance (e.g., 'title:rust^3 OR body:rust').\n2. The query string supports inline 'return field1,field2' for field projection, and 'limit N' for result count.\n3. If you receive a field error or do not know the available fields, run the 'get_index' tool first to view the schema.\n\nQUERY SYNTAX QUICK REFERENCE:\n- Terms: rust database (AND by default)\n- Field targeting: title:rust (only applies to next term)\n- Term prefix: title:quick* (matches quickwit, quickstart)\n- Phrases: title:\"rust programming\"\n- Phrase slop (proximity): body:\"small bike\"~2\n- Phrase prefix: \"big bad wo\"* (matches 'big bad wolf')\n- Boolean: title:rust AND author:doe | OR | NOT (UPPERCASE required)\n- Must/must-not: +title:rust -author:smith\n- Grouping: (title:rust OR title:go) AND year:[2020 TO 2024]\n- Range (inclusive []): year:[2020 TO 2024]\n- Range (exclusive {}): score:{0 TO 100}\n- Range (comparison): age:>=18 or score:<100\n- Unbounded range: price:[10.0 TO *] or age:[* TO 30]\n- Set operator: status: IN [active pending review]\n- Boosting: title:rust^3 OR body:rust\n- Exists: author:* (matches docs where author field is set)\n- All docs: *\n- Date: created_at:>2024-01-01, created_at:[2024-01-01 TO 2024-12-31]\n- Escape specials: k8s\\.component\\.name:value (reserved: + ^ ` : { } \" [ ] ( ) ~ ! \\\\ * SPACE)\n- Field names: If a field name literally contains a dot, escape it (k8s\\.node) to avoid JSON nested access.\n\nFIELD TYPE IMPACT ON OPERATORS:\n- text: all operators (phrases, slop, prefix, IN set, boost, range, exists)\n- string/exact: exact match, prefix, IN set, exists (no phrases/slop)\n- numeric (i64/u64/f64): exact, comparisons (>, <), range [], {}, boost, exists (no phrases/IN)\n- date: exact, comparisons (>/</>=/<=), range, exists (no phrases/IN)\n- boolean: true/false only, exists (no range/boost)\n- ip: exact, range, exists (no phrases)\n- json: dot notation field.sub:value, nested exists field.sub:* (escape dots with \\\\)\n- facet: path /category/sub, exists\n\nORCHESTRATION TIP: If your query involves an exact document ID or a field marked with `shadow: true`, query it directly (e.g., `id:123`). This bypasses the search index for ultra-fast KV retrieval.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "index": {
                        "type": "string",
                        "description": "Name of the CameoDB index to search."
                    },
                    "query": {
                        "type": "string",
                        "description": "Search query string. Supports field:value, phrases, AND/OR/NOT, ranges, and inline 'return'/'limit' modifiers. Use 'limit 0' for count-only queries."
                    },
                    "limit": {
                        "type": "integer",
                        "minimum": 0,
                        "description": "Maximum number of results to return. Pass 0 for count-only mode (returns total_hits without document data). If omitted, defaults to 10."
                    },
                    "fields": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Field names to include in results (field projection)."
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
            "description": "Execute federated search across multiple CameoDB indexes with optional per-index field projection. Results are merged by relevance score. Each hit includes an '_index_source' field indicating its origin index.\n\nCRITICAL ANTI-HALLUCINATION RULE FOR AGENTS:\nWhen answering questions based on CameoDB results, you MUST use ONLY the exact data returned by this tool. Do NOT combine database results with your own prior knowledge. If the index returns partial or incomplete information, state exactly what was found and nothing more. NEVER invent or hallucinate fields or values not explicitly present in the query results.\n\nUses the same query syntax as search_index (field:value, phrases, boolean operators, ranges, boosting, set IN, slop ~, prefix *, must +/-, grouping). If indexes have different schemas, the query applies to matching fields in each index.\n\nORCHESTRATION TIP: When federating across indexes, pay close attention to the `_index_source` field in your results. Use this to focus subsequent, deeper queries on a single index.",
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
                                    "description": "Fields to include from this index."
                                },
                                "sort": {
                                    "type": "object",
                                    "description": "Sort results by a FAST field within this index.",
                                    "properties": {
                                        "field": {
                                            "type": "string",
                                            "description": "FAST field name to sort by."
                                        },
                                        "order": {
                                            "type": "string",
                                            "enum": ["asc", "desc"],
                                            "description": "Sort order. Defaults to desc."
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
