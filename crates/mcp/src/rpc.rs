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
    guidance::{INSTRUCTIONS, ORCHESTRATOR_SKILL},
    protocol::negotiate_protocol_version,
    tools::{ToolCallParams, call_tool, tool_cost, tool_subject, visible_tools},
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

/// A tool's result as the text block, compactly.
///
/// Not indented. The result now travels as `structuredContent` too, so the text block is the
/// backwards-compatible copy rather than the thing a human reads — and indentation would be a
/// quarter of the message spent on whitespace, twice over.
fn json_to_text(value: &JsonValue) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| value.to_string())
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
    // A message with no id is a notification by definition, and JSON-RPC 2.0 forbids replying
    // to one. Checked on the shape rather than per-method, so a notification this server has
    // never heard of is also answered with silence.
    if request.id.is_none() {
        debug!(method = %request.method, "MCP notification received");
        return None;
    }

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
                    },
                    // The only channel that reaches every client without being asked for.
                    "instructions": INSTRUCTIONS,
                }),
            ))
        }
        "ping" => Some(success_response(request.id, json!({}))),

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
                                "text": json_to_text(&content),
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
            json!({ "tools": visible_tools(authz, backend.max_search_limit()) }),
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
                    // Read the same way and for the same reason: what the call costs has to
                    // be known before the rate check, which precedes decoding.
                    let cost = tool_cost(&params.arguments);
                    // Owned, because `params` moves into `call_tool` below and the record is
                    // written after it returns.
                    let query = params
                        .arguments
                        .get("query")
                        .and_then(JsonValue::as_str)
                        .map(str::to_string);
                    match backend.check_tool_rate(Arc::clone(authz), &params.name, cost) {
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
                                // The result travels once, as `structuredContent`. The spec also
                                // allows a text copy for clients predating structured results,
                                // and it is declined: it is the same JSON escaped into a string,
                                // so it doubles every response to say nothing new. An agent's
                                // context is the scarce resource here, and these tools are
                                // described by `instructions` and `outputSchema` — a client that
                                // needs to know the shape of a result is told, rather than shown
                                // twice.
                                //
                                // `content` stays present and empty rather than being omitted.
                                // It is a required array in the revisions this server negotiates,
                                // and a client validating the envelope should find the field it
                                // expects rather than an error.
                                Ok(result) => success_response(
                                    request.id,
                                    json!({
                                        "content": [],
                                        "structuredContent": result,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{authz::McpUnrestricted, backend::testing::StubBackend};

    fn caller() -> McpAuthzRef {
        Arc::new(McpUnrestricted)
    }

    fn message(body: JsonValue) -> JsonRpcRequest {
        parse_json_rpc_request(body).expect("a well-formed JSON-RPC message")
    }

    #[tokio::test]
    async fn a_message_without_an_id_never_gets_a_response() {
        // JSON-RPC 2.0 forbids replying to a notification, and notification-ness is a property
        // of the message rather than of its method name — so a notification this server has
        // never heard of must be answered with silence too, as must a `ping` sent without an id.
        for method in [
            "notifications/initialized",
            "notifications/cancelled",
            "notifications/progress",
            "notifications/roots/list_changed",
            "ping",
            "tools/list",
            "no/such/method",
        ] {
            let response = handle_rpc_request(
                StubBackend::default(),
                message(json!({ "jsonrpc": "2.0", "method": method })),
                &caller(),
            )
            .await;
            assert!(
                response.is_none(),
                "{method} arrived without an id and was answered with {response:?}"
            );
        }
    }

    #[tokio::test]
    async fn initialize_carries_the_session_instructions() {
        // The one channel a client reads without being asked to. Guidance that lives only in
        // `prompts/get` reaches the clients that call it by hand, which is almost none.
        let response = handle_rpc_request(
            StubBackend::default(),
            message(json!({ "jsonrpc": "2.0", "id": 1, "method": "initialize" })),
            &caller(),
        )
        .await
        .expect("initialize is answered");

        let instructions = response["result"]["instructions"]
            .as_str()
            .expect("initialize must carry instructions");
        assert!(
            instructions.contains("list_indexes") && instructions.contains("validate_query"),
            "instructions must point at the tools they describe: {instructions}"
        );
    }

    #[tokio::test]
    async fn an_unknown_method_carrying_an_id_gets_method_not_found() {
        // The guard above must not over-correct into swallowing real requests.
        let response = handle_rpc_request(
            StubBackend::default(),
            message(json!({ "jsonrpc": "2.0", "id": 7, "method": "no/such/method" })),
            &caller(),
        )
        .await
        .expect("a request carrying an id must be answered");

        assert_eq!(response["id"], json!(7));
        assert_eq!(response["error"]["code"], json!(-32601));
    }
}
