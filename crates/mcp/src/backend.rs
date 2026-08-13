//! The operations MCP exposes, and the records it hands back to the host.

use futures::future::BoxFuture;
use serde::Deserialize;
use serde_json::Value as JsonValue;

use crate::authz::McpAuthzRef;

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
/// Methods that **name** their index take no caller: [`crate::tools::call_tool`] has the name in
/// hand and refuses a disallowed one before dispatching, so the check happens once rather than in
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

    /// May this caller invoke another tool right now?
    ///
    /// Asked once per `tools/call`, before the tool runs and before its arguments are
    /// interpreted, so a refused call costs a hash lookup rather than a search.
    ///
    /// The policy lives in the host for the same reason authorization does: this crate
    /// speaks MCP and must not acquire opinions about how a deployment is configured. It
    /// asks the question; the host answers it. The default allows everything, so a host that
    /// configures no limits — and every existing implementation of this trait — is unchanged.
    fn check_tool_rate(&self, _authz: McpAuthzRef, _tool: &str) -> RateLimitVerdict {
        RateLimitVerdict::Allow
    }

    /// A tool call happened. Keep a record of it, or do not.
    ///
    /// Called once per `tools/call` after the outcome is known, including the calls that
    /// were refused — a refusal is the record most worth having.
    ///
    /// This exists because the HTTP layer cannot see through `POST /mcp`: from outside, a
    /// thousand searches across a hundred indexes are a thousand identical lines. Which tool
    /// and which index are visible only here, which is the same reason [`crate::McpAuthz`] and
    /// [`check_tool_rate`](McpBackend::check_tool_rate) are here. What is *kept* — whether
    /// query text is retained at all, where the record goes — stays entirely with the host.
    /// The default keeps nothing, so every existing implementation is unchanged.
    fn record_tool_call(&self, _authz: McpAuthzRef, _call: ToolCall<'_>) {}
}

/// What a tool call did, for [`McpBackend::record_tool_call`].
///
/// Borrowed rather than owned: on a host that keeps no record this must cost nothing, and a
/// struct of `String`s would allocate on every call to build something immediately dropped.
#[derive(Debug, Clone, Copy)]
pub struct ToolCall<'a> {
    pub tool: &'a str,
    /// The index the call named, when it named one. `search_indexes` names several and they
    /// arrive comma-joined — the record is for a human reading a trail, not for a parser.
    pub index: Option<&'a str>,
    /// The query text as sent. Passed regardless of whether the host will keep it; deciding
    /// that is the host's business, and this crate holds no policy about PII.
    pub query: Option<&'a str>,
    /// `None` when the tool ran. Otherwise why it did not — a refusal or a failure.
    pub error: Option<&'a str>,
}

/// The host's answer to [`McpBackend::check_tool_rate`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RateLimitVerdict {
    Allow,
    /// Refused. `retry_after_secs` is what the caller is told to wait — an agent that gets a
    /// number can back off correctly, where one that gets "too many requests" will usually
    /// retry immediately and make it worse.
    Deny {
        retry_after_secs: u64,
    },
}

/// A backend for the tests in this crate.
#[cfg(test)]
pub(crate) mod testing {
    use futures::future::BoxFuture;
    use serde_json::{Value as JsonValue, json};

    use super::{McpBackend, McpIndexSearchRequest};
    use crate::authz::McpAuthzRef;

    /// Answers every operation with an empty object.
    ///
    /// The dispatcher tests are about the JSON-RPC envelope — which messages get a reply, what
    /// an error carries — so what a tool *returns* is deliberately uninteresting here.
    #[derive(Clone)]
    pub(crate) struct StubBackend;

    fn empty() -> BoxFuture<'static, Result<JsonValue, String>> {
        Box::pin(async { Ok(json!({})) })
    }

    impl McpBackend for StubBackend {
        fn search_index(
            &self,
            _index: McpIndexSearchRequest,
            _query: String,
            _limit: Option<usize>,
        ) -> BoxFuture<'_, Result<JsonValue, String>> {
            empty()
        }

        fn search_indexes(
            &self,
            _indexes: Vec<McpIndexSearchRequest>,
            _query: String,
            _limit: Option<usize>,
        ) -> BoxFuture<'_, Result<JsonValue, String>> {
            empty()
        }

        fn get_index(&self, _index: String) -> BoxFuture<'_, Result<JsonValue, String>> {
            empty()
        }

        fn list_indexes(&self, _authz: McpAuthzRef) -> BoxFuture<'_, Result<JsonValue, String>> {
            empty()
        }

        fn validate_query(
            &self,
            _index: Option<String>,
            _partial_field: Option<String>,
            _query: Option<String>,
        ) -> BoxFuture<'_, Result<JsonValue, String>> {
            empty()
        }

        fn get_index_stats(
            &self,
            _index: Option<String>,
            _authz: McpAuthzRef,
        ) -> BoxFuture<'_, Result<JsonValue, String>> {
            empty()
        }

        fn list_resources(&self, _authz: McpAuthzRef) -> BoxFuture<'_, Result<JsonValue, String>> {
            empty()
        }

        fn read_resource(
            &self,
            _uri: String,
            _authz: McpAuthzRef,
        ) -> BoxFuture<'_, Result<JsonValue, String>> {
            empty()
        }
    }
}
