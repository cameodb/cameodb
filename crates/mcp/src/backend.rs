//! The operations MCP exposes, and the records it hands back to the host.

use futures::future::BoxFuture;
use serde::Deserialize;
use serde_json::Value as JsonValue;

use crate::authz::McpAuthzRef;

/// Sort specification for search results
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
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

/// One index named in a federated search, with what to return from it and how to order it.
///
/// Accepts either the name on its own or an object naming it, because most entries in a
/// federated search want nothing but the name and `{"index": "docs"}` is three times the
/// characters to say so.
///
/// Arrives from a client, so the object form refuses fields it does not know: the schema
/// advertising it says `additionalProperties: false`, and a struct that quietly ignored the rest
/// would make that advertisement false. A misspelled `feilds` is then a named error rather than
/// a projection silently not applied.
#[derive(Debug, Clone)]
pub struct McpIndexSearchRequest {
    pub index: String,
    pub fields: Option<Vec<String>>,
    pub sort: Option<SortSpec>,
}

/// Written by hand rather than derived from an untagged enum.
///
/// An untagged enum buffers the input and reports only that it matched no variant, which would
/// turn "unknown field `feilds`" into "data did not match any variant" and lose the one thing
/// that makes a misspelling correctable. Dispatching on what actually arrived — a string, or a
/// map — keeps the object form's own errors intact.
impl<'de> Deserialize<'de> for McpIndexSearchRequest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Named {
            index: String,
            #[serde(default)]
            fields: Option<Vec<String>>,
            #[serde(default)]
            sort: Option<SortSpec>,
        }

        struct EitherForm;

        impl<'de> serde::de::Visitor<'de> for EitherForm {
            type Value = McpIndexSearchRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("an index name, or an object naming one")
            }

            fn visit_str<E>(self, index: &str) -> Result<Self::Value, E> {
                Ok(McpIndexSearchRequest {
                    index: index.to_string(),
                    fields: None,
                    sort: None,
                })
            }

            fn visit_map<A>(self, map: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let named = Named::deserialize(serde::de::value::MapAccessDeserializer::new(map))?;
                Ok(McpIndexSearchRequest {
                    index: named.index,
                    fields: named.fields,
                    sort: named.sort,
                })
            }
        }

        deserializer.deserialize_any(EitherForm)
    }
}

/// The operations MCP exposes, implemented by the host.
///
/// Methods that **name** their index take no caller: the tool dispatcher has the name in
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
        offset: Option<usize>,
    ) -> BoxFuture<'_, Result<JsonValue, String>>;

    fn search_across_indexes(
        &self,
        indexes: Vec<McpIndexSearchRequest>,
        query: String,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> BoxFuture<'_, Result<JsonValue, String>>;

    fn describe_index(&self, index: String) -> BoxFuture<'_, Result<JsonValue, String>>;

    fn list_indexes(&self, authz: McpAuthzRef) -> BoxFuture<'_, Result<JsonValue, String>>;

    fn validate_query(
        &self,
        index: Option<String>,
        partial_field: Option<String>,
        query: Option<String>,
    ) -> BoxFuture<'_, Result<JsonValue, String>>;

    /// Totals across the whole catalogue, scoped to what this caller may see.
    ///
    /// Takes no index. One index's statistics are part of describing it, and answering the same
    /// question from two tools is how the two come to disagree.
    fn get_catalog_stats(&self, authz: McpAuthzRef) -> BoxFuture<'_, Result<JsonValue, String>>;

    fn list_resources(&self, authz: McpAuthzRef) -> BoxFuture<'_, Result<JsonValue, String>>;

    fn read_resource(
        &self,
        uri: String,
        authz: McpAuthzRef,
    ) -> BoxFuture<'_, Result<JsonValue, String>>;

    /// The largest `limit` a search tool may be asked for.
    ///
    /// A deployment question rather than a protocol one — it is the node that has to build,
    /// merge and serialize that many hits — so the host owns the number and this crate only
    /// carries it. Read once per `tools/list` to render the `maximum` each search schema
    /// advertises, and again per call to enforce it, so a client is never refused for
    /// exceeding a bound it was not shown.
    ///
    /// The default is [`DEFAULT_MAX_SEARCH_LIMIT`](crate::DEFAULT_MAX_SEARCH_LIMIT), which
    /// leaves a host that configures nothing bounded rather than unbounded.
    fn max_search_limit(&self) -> usize {
        crate::tools::schema::DEFAULT_MAX_SEARCH_LIMIT
    }

    /// May this caller invoke another tool right now, at this cost?
    ///
    /// Asked once per `tools/call`, before the tool runs and before its arguments are
    /// interpreted, so a refused call costs a hash lookup rather than a search.
    ///
    /// `cost` is how many units of work the call is asking for: the number of indexes a
    /// federated search names, and one for everything else. A budget charged per call rather
    /// than per unit counts requests instead of work, which prices a twenty-index fan-out the
    /// same as a single lookup.
    ///
    /// The policy lives in the host for the same reason authorization does: this crate
    /// speaks MCP and must not acquire opinions about how a deployment is configured. It
    /// asks the question; the host answers it. The default allows everything, so a host that
    /// configures no limits — and every existing implementation of this trait — is unchanged.
    fn check_tool_rate(&self, _authz: McpAuthzRef, _tool: &str, _cost: u32) -> RateLimitVerdict {
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
    /// The index the call named, when it named one. `search_across_indexes` names several and they
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
    ///
    /// The search ceiling is settable because it is the one thing a host supplies that this
    /// crate both advertises and enforces: a stub fixed at the default could not tell a bound
    /// that follows the host from a constant compiled in here.
    #[derive(Clone, Default)]
    pub(crate) struct StubBackend {
        max_search_limit: Option<usize>,
    }

    impl StubBackend {
        /// A host whose ceiling is not the default one.
        pub(crate) fn capped(max_search_limit: usize) -> Self {
            Self {
                max_search_limit: Some(max_search_limit),
            }
        }
    }

    fn empty() -> BoxFuture<'static, Result<JsonValue, String>> {
        Box::pin(async { Ok(json!({})) })
    }

    impl McpBackend for StubBackend {
        fn max_search_limit(&self) -> usize {
            self.max_search_limit
                .unwrap_or(crate::tools::schema::DEFAULT_MAX_SEARCH_LIMIT)
        }

        fn search_index(
            &self,
            _index: McpIndexSearchRequest,
            _query: String,
            _limit: Option<usize>,
            _offset: Option<usize>,
        ) -> BoxFuture<'_, Result<JsonValue, String>> {
            empty()
        }

        fn search_across_indexes(
            &self,
            _indexes: Vec<McpIndexSearchRequest>,
            _query: String,
            _limit: Option<usize>,
            _offset: Option<usize>,
        ) -> BoxFuture<'_, Result<JsonValue, String>> {
            empty()
        }

        fn describe_index(&self, _index: String) -> BoxFuture<'_, Result<JsonValue, String>> {
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

        fn get_catalog_stats(
            &self,
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
