//! The MCP tools: what CameoDB answers when an agent calls one.
//!
//! The mcp crate owns the protocol and the catalogue; this owns the answers. A trait impl cannot
//! be split across modules, so [`McpBackend`] here is ten delegating methods and the work lives in
//! a module per group of operations — which also makes each one callable, and testable, without
//! going through the trait.

mod diagnostics;
mod discovery;
mod governance;
mod resources;
mod schema;
mod search;

use cameodb_mcp::{McpAuthzRef, McpBackend, McpIndexSearchRequest, RateLimitVerdict, ToolCall};
use futures::future::BoxFuture;
use serde_json::Value as JsonValue;

use crate::state::AppState;

/// Every MCP tool, implemented against the same router the HTTP API uses.
///
/// The mcp crate owns the protocol and the catalogue; this owns what the tools actually answer.
/// The difference between the two surfaces lives here rather than in the engine: the HTTP API
/// answers a client that can read a status code and an empty body, while a tool answers an agent
/// that acts on the text — so a search naming an index the node does not have is refused here and
/// still returns 200 with no hits over HTTP, and a dropped clause fails a tool call rather than
/// returning results for a query nobody wrote.
impl McpBackend for AppState {
    fn search_index(
        &self,
        index: McpIndexSearchRequest,
        query: String,
        limit: Option<usize>,
    ) -> BoxFuture<'_, Result<JsonValue, String>> {
        search::search_index(self.clone(), index, query, limit)
    }

    fn search_across_indexes(
        &self,
        indexes: Vec<McpIndexSearchRequest>,
        query: String,
        limit: Option<usize>,
    ) -> BoxFuture<'_, Result<JsonValue, String>> {
        search::search_across_indexes(self.clone(), indexes, query, limit)
    }

    fn describe_index(&self, index: String) -> BoxFuture<'_, Result<JsonValue, String>> {
        discovery::describe_index(self.clone(), index)
    }

    fn list_indexes(&self, authz: McpAuthzRef) -> BoxFuture<'_, Result<JsonValue, String>> {
        discovery::list_indexes(self.clone(), authz)
    }

    fn validate_query(
        &self,
        index: Option<String>,
        partial_field: Option<String>,
        query: Option<String>,
    ) -> BoxFuture<'_, Result<JsonValue, String>> {
        discovery::validate_query(self.clone(), index, partial_field, query)
    }

    fn get_catalog_stats(&self, authz: McpAuthzRef) -> BoxFuture<'_, Result<JsonValue, String>> {
        discovery::index_stats(self.clone(), None, authz)
    }

    fn list_resources(&self, authz: McpAuthzRef) -> BoxFuture<'_, Result<JsonValue, String>> {
        resources::list_resources(self.clone(), authz)
    }

    fn read_resource(
        &self,
        uri: String,
        authz: McpAuthzRef,
    ) -> BoxFuture<'_, Result<JsonValue, String>> {
        resources::read_resource(self.clone(), uri, authz)
    }

    fn max_search_limit(&self) -> usize {
        self.max_search_limit
    }

    fn check_tool_rate(&self, authz: McpAuthzRef, tool: &str, cost: u32) -> RateLimitVerdict {
        governance::check_tool_rate(self, authz, tool, cost)
    }

    fn record_tool_call(&self, authz: McpAuthzRef, call: ToolCall<'_>) {
        governance::record_tool_call(self, authz, call)
    }
}
