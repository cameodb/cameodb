use std::{collections::HashMap, convert::Infallible, sync::Arc, time::Duration};

use axum::{
    Json, Router,
    extract::{Extension, Query, State},
    response::{
        IntoResponse, Sse,
        sse::{Event, KeepAlive},
    },
    routing::{get, post},
};
use futures::{Stream, StreamExt, future::BoxFuture};
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};
use tokio::sync::{Mutex, mpsc};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tracing::{debug, warn};

#[derive(Clone, Default)]
struct McpTransportState {
    inner: Arc<Mutex<McpTransportInner>>,
}

#[derive(Default)]
struct McpTransportInner {
    next_session_id: u64,
    sessions: HashMap<String, mpsc::UnboundedSender<String>>,
}

#[derive(Debug, Deserialize)]
struct MessageQuery {
    session_id: String,
}

#[derive(Debug, Serialize)]
struct MessageAck {
    ok: bool,
    session_id: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct McpIndexSearchRequest {
    pub index: String,
    #[serde(default)]
    pub fields: Option<Vec<String>>,
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
    #[allow(dead_code)]
    jsonrpc: Option<String>,
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

pub fn mcp_router<S>() -> Router<S>
where
    S: McpBackend,
{
    let transport_state = McpTransportState::default();

    Router::new()
        .route("/sse", get(mcp_sse_handler))
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
        .layer(Extension(transport_state))
}

async fn mcp_sse_handler(
    Extension(state): Extension<McpTransportState>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let (session_id, rx) = {
        let mut inner = state.inner.lock().await;
        inner.next_session_id += 1;
        let session_id = format!("mcp-session-{}", inner.next_session_id);
        let (tx, rx) = mpsc::unbounded_channel();
        inner.sessions.insert(session_id.clone(), tx.clone());

        let ready = json!({
            "session_id": session_id,
            "message_endpoint": "/mcp/messages",
        })
        .to_string();

        let _ = tx.send(ready);
        (session_id, rx)
    };

    debug!(session_id = %session_id, "MCP SSE session opened");

    let stream = UnboundedReceiverStream::new(rx)
        .map(|payload| Ok(Event::default().event("message").data(payload)));

    Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keepalive"),
    )
}

async fn process_mcp_message<B: McpBackend>(
    app_state: B,
    query: MessageQuery,
    state: McpTransportState,
    payload: JsonValue,
) -> impl IntoResponse {
    let sender = {
        let inner = state.inner.lock().await;
        inner.sessions.get(&query.session_id).cloned()
    };

    match sender {
        Some(tx) => {
            let maybe_response = match serde_json::from_value::<JsonRpcRequest>(payload) {
                Ok(request) => handle_rpc_request(app_state, request).await,
                Err(err) => Some(error_response(
                    None,
                    -32600,
                    format!("Invalid JSON-RPC request: {err}"),
                )),
            };

            if let Some(envelope) = maybe_response
                && tx.send(envelope.to_string()).is_err()
            {
                warn!(session_id = %query.session_id, "MCP session receiver dropped before message delivery");
                let mut inner = state.inner.lock().await;
                inner.sessions.remove(&query.session_id);
                return (
                    axum::http::StatusCode::GONE,
                    Json(json!({
                        "error": "MCP session is no longer active",
                        "session_id": query.session_id,
                    })),
                )
                    .into_response();
            }

            (
                axum::http::StatusCode::ACCEPTED,
                Json(json!(MessageAck {
                    ok: true,
                    session_id: query.session_id,
                })),
            )
                .into_response()
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
        "initialize" => Some(success_response(
            request.id,
            json!({
                "protocolVersion": "2024-11-05",
                "capabilities": {
                    "tools": {},
                    "resources": {}
                },
                "serverInfo": {
                    "name": "cameodb-mcp",
                    "version": env!("CARGO_PKG_VERSION")
                }
            }),
        )),
        "ping" => Some(success_response(request.id, json!({}))),

        // --- Notifications (no response per JSON-RPC spec) ---
        "notifications/initialized" | "notifications/cancelled" => {
            debug!(method = %request.method, "MCP notification received");
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

fn mcp_tools() -> Vec<JsonValue> {
    vec![
        json!({
            "name": "search_index",
            "title": "Search Index",
            "description": "Execute full-text search on a single CameoDB index. Query syntax supports field:value targeting, phrase queries (field:\"words\"), boolean operators (AND, OR, NOT), grouping with parentheses, range queries (field:[low TO high]), and date comparisons (field:>2024-01-01). The query string also supports inline 'return field1,field2' for field projection and 'limit N' for result count.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "index": {
                        "type": "string",
                        "description": "Name of the CameoDB index to search."
                    },
                    "query": {
                        "type": "string",
                        "description": "Search query string. Supports field:value, phrases, AND/OR/NOT, ranges, and inline 'return'/'limit' modifiers."
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of results to return."
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
            "description": "Execute federated search across multiple CameoDB indexes with optional per-index field projection. Results are merged by relevance score. Each hit includes an '_index_source' field indicating its origin index.",
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
                        "description": "Maximum total results across all indexes."
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
            "description": "Retrieve schema and statistics for a single CameoDB index.",
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
            "description": "List all available CameoDB indexes with their schemas and metadata.",
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
            "description": "Validate and get guidance on CameoDB search query syntax. Provides field-type-aware suggestions, detects unknown or non-indexed fields, checks query structure (unbalanced quotes/parens, inline modifiers), and returns the full CameoDB query syntax reference. Supply an index name for schema-aware validation.",
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
