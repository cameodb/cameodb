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
    tools::{
        ToolCallParams, call_tool, schema::ToolLimits, tool_cost, tool_subject, visible_tools,
    },
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
/// Not indented. The text block is the only channel a tool result travels on, so whatever is
/// spent here is spent on every response — and indentation would be a quarter of the message
/// spent on whitespace.
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

/// Answer one JSON-RPC message, or `None` where the protocol says there is no reply.
///
/// The answer does not depend on the caller's negotiated revision. Tool results travel in
/// `content` as a text block, which is valid in every revision this server speaks — see
/// [`handle_tool_call`] for why that is the only channel used.
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
            json!({ "tools": visible_tools(authz, ToolLimits::of(&backend)) }),
        )),
        "tools/call" => Some(handle_tool_call(&backend, request.id, request.params, authz).await),

        other => Some(error_response(
            request.id,
            -32601,
            format!("Unsupported MCP method: {other}"),
        )),
    }
}

/// One `tools/call`: the rate check, the dispatch, and the record of what happened.
///
/// Split out of [`handle_rpc_request`] because it is the one method with real control flow —
/// three outcomes, each recorded — and every outcome added inside a match arm is an outcome
/// that can miss its `record_tool_call`. Here the two records sit next to the refusal and the
/// dispatch they describe.
async fn handle_tool_call<S>(
    backend: &S,
    id: Option<JsonValue>,
    params: JsonValue,
    authz: &McpAuthzRef,
) -> JsonValue
where
    S: McpBackend,
{
    let params = match serde_json::from_value::<ToolCallParams>(params) {
        Ok(params) => params,
        Err(err) => {
            return error_response(id, -32602, format!("Invalid tools/call params: {err}"));
        }
    };

    // Read off the raw arguments rather than threaded out of `call_tool`, which has a
    // differently-shaped struct per tool. Every tool that names an index calls the field
    // `index` (or `indexes`), so one reader covers all of them and a tool added later is
    // described without being edited here.
    let subject = tool_subject(&params.arguments);
    // Read the same way and for the same reason: what the call costs has to be known before
    // the rate check, which precedes decoding.
    let cost = tool_cost(&params.arguments, backend.max_federated_indexes());
    // Owned, because `params` moves into `call_tool` below and the record is written after
    // it returns.
    let query = params
        .arguments
        .get("query")
        .and_then(JsonValue::as_str)
        .map(str::to_string);

    // Checked before `call_tool` so the refusal precedes both the capability check and the
    // tool body: a caller being rate limited should not learn, from the shape of the refusal,
    // which tools it would otherwise be allowed to run.
    if let RateLimitVerdict::Deny { retry_after_secs } =
        backend.check_tool_rate(Arc::clone(authz), &params.name, cost)
    {
        backend.record_tool_call(
            Arc::clone(authz),
            ToolCall {
                tool: &params.name,
                index: subject.as_deref(),
                query: query.as_deref(),
                error: Some("rate limit exceeded"),
            },
        );
        return success_response(
            id,
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
        );
    }

    let tool = params.name.clone();
    let outcome = call_tool(backend, params, authz).await;
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
        // One shape, for every client on every revision: the serialized result in the text
        // block, and no `structuredContent`.
        //
        // Structured content is an optional feature and this server opts out of it. The
        // alternative is not free: the spec asks a tool returning structured content to *also*
        // serialize it into a text block, so opting in means sending every result twice —
        // measured at 1038 bytes against 520 for a two-hit search — and an agent's context is
        // the scarce resource.
        //
        // Sending only `structuredContent` is what this server used to do for clients that
        // negotiated 2025-06-18, and it is the thing that broke: a client's revision says which
        // spec it speaks, not which channel it reads, and several hosts negotiate 2025-06-18
        // while rendering `content` alone. They saw an empty array and reported no results,
        // which is indistinguishable from a query that matched nothing. There is no client
        // capability for "I read structuredContent" — capabilities are `roots`, `sampling`,
        // `elicitation` and `experimental` — so no version test can tell the two apart, and
        // guessing wrong fails silently.
        //
        // `content` is a required array in every revision, so the one channel that is always
        // read is the one always populated. `outputSchema` is not advertised either: it binds
        // the server to returning conforming structured results, and a schema advertised
        // without them is a promise to a client that cannot be kept.
        Ok(result) => success_response(
            id,
            json!({
                "content": [{ "type": "text", "text": json_to_text(&result) }],
                "isError": false,
            }),
        ),
        Err(err) => success_response(
            id,
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

    /// A tool result arrives as the serialized result in the text block, and nowhere else.
    ///
    /// The empty `content` this replaced is the bug it was reported for: a client that renders
    /// `content` alone showed nothing, and nothing is what a query matching no documents looks
    /// like too. So the assertion worth keeping is that the array is populated — a result no
    /// client can read is worse than a verbose one.
    #[tokio::test]
    async fn a_tool_result_carries_the_serialized_result_in_the_text_block() {
        let response = handle_rpc_request(
            StubBackend::default(),
            message(json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {"name": "list_indexes", "arguments": {}},
            })),
            &caller(),
        )
        .await
        .expect("a tool call is answered");

        let result = &response["result"];
        assert_eq!(result["isError"], json!(false), "{response}");

        let content = result["content"].as_array().expect("content is required");
        assert_eq!(
            content.len(),
            1,
            "a successful result is one text block: {result}"
        );
        assert_eq!(content[0]["type"], json!("text"), "{result}");
        let text = content[0]["text"].as_str().unwrap_or_default();
        assert!(
            serde_json::from_str::<JsonValue>(text).is_ok_and(|value| value.is_object()),
            "the text block does not carry the serialized result: {result}"
        );

        // Advertising no `outputSchema` and returning no `structuredContent` are one decision;
        // this is the half of it a client can observe.
        assert!(
            result.get("structuredContent").is_none(),
            "a result carried structuredContent for a server that advertises no outputSchema: \
             {result}"
        );
    }
}
