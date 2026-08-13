//! The JSON-RPC envelope and the method dispatcher.
//!
//! Deliberately knows nothing about HTTP: it takes a parsed request and returns a response
//! value, or `None` where the protocol says there is no reply. That is what lets the transport
//! decide status codes and headers, and what keeps era detection out of here.

use std::sync::Arc;

use serde::Deserialize;
use serde_json::{Value as JsonValue, json};
use tracing::debug;

use crate::{
    authz::McpAuthzRef,
    backend::{McpBackend, RateLimitVerdict, ToolCall},
    guidance::ORCHESTRATOR_SKILL,
    protocol::negotiate_protocol_version,
    tools::{ToolCallParams, call_tool, tool_subject, visible_tools},
};

#[derive(Debug, Deserialize)]
pub(crate) struct JsonRpcRequest {
    id: Option<JsonValue>,
    method: String,
    #[serde(default)]
    params: JsonValue,
}

#[derive(Debug, Deserialize)]
struct ReadResourceArgs {
    uri: String,
}

pub(crate) fn parse_json_rpc_request(
    payload: JsonValue,
) -> Result<JsonRpcRequest, serde_json::Error> {
    serde_json::from_value::<JsonRpcRequest>(payload)
}

pub(crate) fn method_of(payload: &JsonValue) -> Option<&str> {
    payload.get("method").and_then(|method| method.as_str())
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

pub(crate) fn error_response(id: Option<JsonValue>, code: i64, message: String) -> JsonValue {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {
            "code": code,
            "message": message,
        }
    })
}

pub(crate) async fn handle_rpc_request<S>(
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
                // Checked here rather than inside `call_tool` so the refusal precedes both
                // the capability check and the tool body: a caller being rate limited should
                // not learn, from the shape of the refusal, which tools it would otherwise
                // be allowed to run.
                Ok(params) => {
                    // Read off the raw arguments rather than threaded out of `call_tool`,
                    // which has a differently-shaped struct per tool. Every tool that names
                    // an index calls the field `index` (or `indexes`), so one reader covers
                    // all of them and a tool added later is described without being edited
                    // into this match.
                    let subject = tool_subject(&params.arguments);
                    // Owned, because `params` moves into `call_tool` below and the record is
                    // written after it returns.
                    let query = params
                        .arguments
                        .get("query")
                        .and_then(JsonValue::as_str)
                        .map(str::to_string);
                    match backend.check_tool_rate(Arc::clone(authz), &params.name) {
                        RateLimitVerdict::Deny { retry_after_secs } => {
                            backend.record_tool_call(
                                Arc::clone(authz),
                                ToolCall {
                                    tool: &params.name,
                                    index: subject.as_deref(),
                                    query: query.as_deref(),
                                    error: Some("rate limit exceeded"),
                                },
                            );
                            success_response(
                                request.id,
                                json!({
                                    "content": [{
                                        "type": "text",
                                        "text": format!(
                                            "Rate limit exceeded for tool '{}'. Retry after {retry_after_secs}s.",
                                            params.name
                                        ),
                                    }],
                                    "isError": true,
                                }),
                            )
                        }
                        RateLimitVerdict::Allow => {
                            let tool = params.name.clone();
                            let outcome = call_tool(&backend, params, authz).await;
                            backend.record_tool_call(
                                Arc::clone(authz),
                                ToolCall {
                                    tool: &tool,
                                    index: subject.as_deref(),
                                    query: query.as_deref(),
                                    error: outcome.as_ref().err().map(String::as_str),
                                },
                            );
                            match outcome {
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
                            }
                        }
                    }
                }
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
