//! The tool catalogue and the dispatcher that runs a call against the backend.

pub(crate) mod schema;

use serde::{Deserialize, de::DeserializeOwned};
use serde_json::{Value as JsonValue, json};

use crate::{
    authz::{McpAuthzRef, tool_capability},
    backend::{McpBackend, McpIndexSearchRequest},
    tools::schema::{
        GetIndexArgs, GetIndexStatsArgs, ListIndexesArgs, MAX_FEDERATED_INDEXES, SearchIndexArgs,
        SearchIndexesArgs, ValidateQueryArgs, get_index_input_schema, get_index_stats_input_schema,
        list_indexes_input_schema, search_index_input_schema, search_index_output_schema,
        search_indexes_input_schema, search_indexes_output_schema, validate_query_input_schema,
    },
};

#[derive(Debug, Deserialize)]
pub(crate) struct ToolCallParams {
    pub(crate) name: String,
    #[serde(default)]
    pub(crate) arguments: JsonValue,
}

/// A tool's arguments, as the struct that tool accepts.
///
/// An absent `arguments` and an explicit `null` both mean the call carried none, which for a
/// tool whose parameters are all optional is a call in its own right: `validate_query` with
/// nothing supplied is how an agent asks for the query reference.
fn decode_args<T: DeserializeOwned>(tool: &str, arguments: JsonValue) -> Result<T, String> {
    let arguments = if arguments.is_null() {
        json!({})
    } else {
        arguments
    };
    serde_json::from_value(arguments).map_err(|err| format!("Invalid {tool} arguments: {err}"))
}

/// Refuse a `limit` past what the schema advertises.
///
/// Both numbers come from the same place — the host, through
/// [`McpBackend::max_search_limit`] — so a caller cannot be refused for exceeding a bound the
/// catalogue did not show it. Checked here as well as by the host because the schema is this
/// crate's promise about what a call may carry, and a promise nothing enforces describes
/// nothing.
fn check_limit(limit: Option<usize>, max_search_limit: usize) -> Result<(), String> {
    match limit {
        Some(limit) if limit > max_search_limit => Err(format!(
            "limit {limit} is above the maximum of {max_search_limit}; ask for at most that many \
             and narrow the query to reach the rest"
        )),
        _ => Ok(()),
    }
}

/// Refuse an index list that is empty, too long, or names an index twice.
///
/// Each of the three would otherwise be answered rather than refused, and the answer would
/// read as a result. An empty list returns no hits and no errors, which is indistinguishable
/// from a query that matched nothing; a repeated name is searched once per mention and its
/// documents counted once per mention, so `total_hits` comes back larger than the index.
fn check_index_list(indexes: &[McpIndexSearchRequest]) -> Result<(), String> {
    if indexes.is_empty() {
        return Err(
            "no index was named; `indexes` needs at least one entry, or an empty result would \
             read as a query that matched nothing"
                .to_string(),
        );
    }
    if indexes.len() > MAX_FEDERATED_INDEXES {
        return Err(format!(
            "{} indexes named; at most {MAX_FEDERATED_INDEXES} may be searched at once, and \
             `list_indexes` describes the whole catalogue in one call",
            indexes.len()
        ));
    }
    for (position, request) in indexes.iter().enumerate() {
        if let Some(earlier) = indexes[..position]
            .iter()
            .find(|seen| seen.index == request.index)
        {
            return Err(format!(
                "index '{}' is named twice; each mention is searched and counted separately, so \
                 the totals would exceed what the index holds",
                earlier.index
            ));
        }
    }
    Ok(())
}

pub(crate) async fn call_tool<S>(
    backend: &S,
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
            let args: SearchIndexArgs = decode_args("search_index", params.arguments)?;
            check_limit(args.limit, backend.max_search_limit())?;
            check_index(authz, &args.index)?;
            backend
                .search_index(
                    McpIndexSearchRequest {
                        index: args.index,
                        fields: args.fields,
                        sort: args.sort,
                    },
                    args.query,
                    args.limit,
                )
                .await
        }
        "search_indexes" => {
            let args: SearchIndexesArgs = decode_args("search_indexes", params.arguments)?;
            check_limit(args.limit, backend.max_search_limit())?;
            check_index_list(&args.indexes)?;
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
            let args: GetIndexArgs = decode_args("get_index", params.arguments)?;
            check_index(authz, &args.index)?;
            backend.get_index(args.index).await
        }
        "list_indexes" => {
            let ListIndexesArgs {} = decode_args("list_indexes", params.arguments)?;
            backend.list_indexes(authz.clone()).await
        }
        "validate_query" => {
            let args: ValidateQueryArgs = decode_args("validate_query", params.arguments)?;
            // Validation reports an index's field names, so it is a read of that index.
            if let Some(index) = &args.index {
                check_index(authz, index)?;
            }
            backend
                .validate_query(args.index, args.partial_field, args.query)
                .await
        }
        "get_index_stats" => {
            let args: GetIndexStatsArgs = decode_args("get_index_stats", params.arguments)?;
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
pub(crate) fn visible_tools(authz: &McpAuthzRef, max_search_limit: usize) -> Vec<JsonValue> {
    mcp_tools(max_search_limit)
        .into_iter()
        .filter(|tool| {
            tool.get("name")
                .and_then(|name| name.as_str())
                .and_then(tool_capability)
                .is_some_and(|capability| authz.has(capability))
        })
        .collect()
}

/// Which index or indexes a call is about, read from its raw arguments.
///
/// One reader for every tool rather than a match over the per-tool argument structs, because
/// the field name is a convention the tool schemas already keep: `index` for the tools that
/// name one, `indexes` for the federated search that names several. A tool added later is
/// described by this without anyone remembering to edit it — and if it followed neither
/// convention the answer is `None`, which is the honest result rather than a wrong one.
pub(crate) fn tool_subject(arguments: &JsonValue) -> Option<String> {
    if let Some(index) = arguments.get("index").and_then(JsonValue::as_str) {
        return Some(index.to_string());
    }
    let entries = arguments.get("indexes")?.as_array()?;
    let names: Vec<&str> = entries
        .iter()
        .filter_map(|entry| match entry {
            // `search_indexes` accepts both a bare name and an object naming one.
            JsonValue::String(name) => Some(name.as_str()),
            other => other.get("index").and_then(JsonValue::as_str),
        })
        .collect();
    (!names.is_empty()).then(|| names.join(","))
}

/// How much work this call is asking for, read from its raw arguments.
///
/// The rate limiter charges one unit per call, which prices a federated search over twenty
/// indexes the same as a single lookup — so a per-key budget counts calls rather than work,
/// and one authorized call buys as many searches as the caller cares to name.
///
/// Read from the raw arguments for the same reason [`tool_subject`] is: this is asked before
/// the arguments are decoded, so that a refusal costs a hash lookup rather than a search, and
/// so that a rate-limited caller learns nothing from the shape of the refusal about which
/// tools it would otherwise be allowed.
///
/// Capped at [`MAX_FEDERATED_INDEXES`], because a longer list is refused when it is decoded:
/// charging for fan-out that cannot happen would let a malformed call empty a caller's budget.
pub(crate) fn tool_cost(arguments: &JsonValue) -> u32 {
    let Some(entries) = arguments.get("indexes").and_then(JsonValue::as_array) else {
        return 1;
    };
    entries.len().clamp(1, MAX_FEDERATED_INDEXES) as u32
}

/// Refuse a tool call that names an index outside the caller's scope.
fn check_index(authz: &McpAuthzRef, index: &str) -> Result<(), String> {
    if authz.allows_index(index) {
        Ok(())
    } else {
        Err(format!("this key is not permitted on index '{index}'"))
    }
}

/// What `search_index` does, plus the query reference rendered from [`crate::syntax`].
///
/// Rendered rather than written out so this cannot drift from the reference `validate_query`
/// returns or from the per-field hints on a schema. Kept to the shortest form that is still
/// actionable, because a tool description sits in the caller's context for the whole session.
fn search_index_description() -> String {
    format!(
        "Full-text search over one CameoDB index.\n\n         Results carry `_score` and, when a projection was requested, only the named fields in \
         the order given. A query the engine cannot fully interpret fails rather than returning \
         partial results, and the error names the clause it could not use.\n\n         Call `get_index` for an index's fields and their types before constructing a query \
         against unfamiliar data.\n\n{}",
        crate::syntax::compact_reference()
    )
}

/// What `search_indexes` adds over `search_index`. The syntax is identical, so it is not repeated.
fn search_indexes_description() -> String {
    "Full-text search over several CameoDB indexes at once, executed concurrently and merged.\n\n     Each hit carries `_index_source` naming the index it came from. Per-index `fields` and \
     `sort` parameters override the equivalent inline modifiers, as they do on `search_index`. \
     One query string is applied to every index, so a field that exists in only some of them \
     will not match in the rest.\n\n     Query syntax is the same as `search_index`; see that tool's description, or call \
     `validate_query` with no arguments for the full reference."
        .to_string()
}

pub(crate) fn mcp_tools(max_search_limit: usize) -> Vec<JsonValue> {
    vec![
        json!({
            "name": "search_index",
            "title": "Search Index",
            "description": search_index_description(),
            "inputSchema": search_index_input_schema(max_search_limit),
            "outputSchema": search_index_output_schema(),
            "annotations": {
                "readOnlyHint": true,
                "openWorldHint": false
            }
        }),
        json!({
            "name": "search_indexes",
            "title": "Federated Search",
            "description": search_indexes_description(),
            "inputSchema": search_indexes_input_schema(max_search_limit),
            "outputSchema": search_indexes_output_schema(),
            "annotations": {
                "readOnlyHint": true,
                "openWorldHint": false
            }
        }),
        json!({
            "name": "get_index",
            "title": "Get Index",
            "description": "Retrieve schema and statistics for a single CameoDB index. Returns field definitions with types and a 'queryable_fields' array containing per-field 'query_hint' showing exactly which operators (phrases, ranges, IN set, boost, slop, etc.) work with each field's data type. Use this to understand an index's structure before constructing queries.\n\nORCHESTRATION TIP: Review the returned schema to identify potential pivot fields (like foreign keys, user IDs, or hashes) before running your search.",
            "inputSchema": get_index_input_schema(),
            "annotations": {
                "readOnlyHint": true,
                "openWorldHint": false
            }
        }),
        json!({
            "name": "list_indexes",
            "title": "List Indexes",
            "description": "List all available CameoDB indexes with their schemas and metadata. Each index includes a 'queryable_fields' array with per-field type and 'query_hint' showing supported operators. Use this as the first discovery step — new indexes are automatically available here with full schema details.",
            "inputSchema": list_indexes_input_schema(),
            "annotations": {
                "readOnlyHint": true,
                "openWorldHint": false
            }
        }),
        json!({
            "name": "validate_query",
            "title": "Validate Query",
            "description": "Validate and get guidance on CameoDB search query syntax. Provides field-type-aware suggestions, detects unknown or non-indexed fields, checks query structure (unbalanced quotes/parens, inline modifiers), and returns the full CameoDB query syntax reference. Supply an index name for schema-aware validation.\n\nPRO TIPS FOR AGENTS:\n1. Call with no arguments to get the complete query syntax reference and operator-by-field-type compatibility matrix.\n2. Supply an index name to get schema-aware field validation with type-specific operator hints per field.\n3. Supply a partial_field to get autocomplete suggestions matching available fields.\n4. Supply a query to get structural validation, field recognition, typo detection ('did you mean?'), and per-field operator guidance.\n\nORCHESTRATION TIP: Use this tool immediately if `search_index` returns a syntax error, before attempting to guess the correct format.",
            "inputSchema": validate_query_input_schema(),
            "annotations": {
                "readOnlyHint": true,
                "openWorldHint": false
            }
        }),
        json!({
            "name": "get_index_stats",
            "title": "Get Index Statistics",
            "description": "Return statistics for a single CameoDB index or aggregated statistics across all indexes.",
            "inputSchema": get_index_stats_input_schema(),
            "annotations": {
                "readOnlyHint": true,
                "openWorldHint": false
            }
        }),
    ]
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::authz::{
        McpCapability,
        testing::{NoCapabilities, Scoped},
    };
    use crate::backend::testing::StubBackend;
    use crate::tools::schema::DEFAULT_MAX_SEARCH_LIMIT;

    fn advertised_tool_names() -> Vec<String> {
        mcp_tools(DEFAULT_MAX_SEARCH_LIMIT)
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
    fn a_named_index_outside_the_scope_is_refused() {
        let authz: McpAuthzRef = Arc::new(Scoped("docs"));
        assert!(check_index(&authz, "docs").is_ok());
        let err = check_index(&authz, "payroll").unwrap_err();
        assert!(err.contains("payroll"), "{err}");
    }

    /// A tool whose parameters are all optional is callable with none of them.
    ///
    /// `validate_query`'s own description tells an agent to do exactly this for the syntax
    /// reference, and a client that omits `arguments` sends no key at all rather than an empty
    /// object — so both spellings of "nothing" have to arrive as a call.
    #[tokio::test]
    async fn a_tool_that_needs_no_arguments_is_callable_without_them() {
        let authz: McpAuthzRef = Arc::new(Scoped("docs"));
        for tool in ["list_indexes", "validate_query", "get_index_stats"] {
            for arguments in [JsonValue::Null, json!({})] {
                let params = ToolCallParams {
                    name: tool.to_string(),
                    arguments: arguments.clone(),
                };
                let outcome = call_tool(&StubBackend::default(), params, &authz).await;
                assert!(outcome.is_ok(), "{tool} with {arguments}: {outcome:?}");
            }
        }
    }

    /// An argument the tool does not know is an error, not a silence.
    ///
    /// Every argument these tools take changes what comes back, so ignoring one that was
    /// misspelled answers a different question than the one asked — and answers it without
    /// saying so.
    #[tokio::test]
    async fn an_argument_the_tool_does_not_take_is_refused_by_name() {
        let authz: McpAuthzRef = Arc::new(Scoped("docs"));
        for (tool, arguments) in [
            (
                "search_index",
                json!({"index": "docs", "query": "a", "limt": 5}),
            ),
            (
                "search_indexes",
                json!({"indexes": [{"index": "docs", "feilds": ["title"]}], "query": "a"}),
            ),
            ("get_index", json!({"index": "docs", "verbose": true})),
            ("list_indexes", json!({"index": "docs"})),
            ("validate_query", json!({"quer": "a"})),
            ("get_index_stats", json!({"indexes": ["docs"]})),
        ] {
            let params = ToolCallParams {
                name: tool.to_string(),
                arguments,
            };
            let err = call_tool(&StubBackend::default(), params, &authz)
                .await
                .expect_err("{tool} accepted an argument it does not take");
            assert!(
                err.contains("unknown field"),
                "{tool} did not name the field it refused: {err}"
            );
        }
    }

    /// A limit past the advertised maximum is refused, by whichever door it arrives at.
    #[tokio::test]
    async fn a_limit_above_the_maximum_is_refused() {
        let authz: McpAuthzRef = Arc::new(Scoped("docs"));
        let over = DEFAULT_MAX_SEARCH_LIMIT + 1;
        for (tool, arguments) in [
            (
                "search_index",
                json!({"index": "docs", "query": "a", "limit": over}),
            ),
            (
                "search_indexes",
                json!({"indexes": [{"index": "docs"}], "query": "a", "limit": over}),
            ),
        ] {
            let params = ToolCallParams {
                name: tool.to_string(),
                arguments,
            };
            let err = call_tool(&StubBackend::default(), params, &authz)
                .await
                .expect_err("an over-large limit was accepted");
            assert!(
                err.contains(&DEFAULT_MAX_SEARCH_LIMIT.to_string()),
                "{tool}: the refusal does not say what the maximum is: {err}"
            );
        }

        // And the largest permitted value is permitted, so the bound is not off by one.
        let params = ToolCallParams {
            name: "search_index".to_string(),
            arguments: json!({"index": "docs", "query": "a", "limit": DEFAULT_MAX_SEARCH_LIMIT}),
        };
        assert!(
            call_tool(&StubBackend::default(), params, &authz)
                .await
                .is_ok()
        );
    }

    /// A host that lowers the ceiling lowers both halves of it.
    ///
    /// The number a client reads in `tools/list` and the number a call is measured against come
    /// from the same place, so a caller cannot be refused for exceeding a bound the catalogue
    /// did not show it — which is the whole reason the host supplies it rather than this crate
    /// holding a constant.
    #[tokio::test]
    async fn a_host_that_lowers_the_ceiling_lowers_what_is_advertised_too() {
        let authz: McpAuthzRef = Arc::new(Scoped("docs"));
        let backend = StubBackend::capped(25);

        for tool in visible_tools(&authz, backend.max_search_limit()) {
            let name = tool["name"].as_str().unwrap_or("?");
            if !name.starts_with("search") {
                continue;
            }
            assert_eq!(
                tool["inputSchema"]["properties"]["limit"]["maximum"],
                json!(25),
                "{name} advertises a ceiling the host did not set"
            );
        }

        let params = ToolCallParams {
            name: "search_index".to_string(),
            arguments: json!({"index": "docs", "query": "a", "limit": 26}),
        };
        let err = call_tool(&backend, params, &authz)
            .await
            .expect_err("the host's lowered ceiling was not enforced");
        assert!(
            err.contains("25"),
            "the refusal quotes another number: {err}"
        );

        // And the default is not silently in force underneath it.
        let params = ToolCallParams {
            name: "search_index".to_string(),
            arguments: json!({"index": "docs", "query": "a", "limit": DEFAULT_MAX_SEARCH_LIMIT}),
        };
        assert!(
            call_tool(&backend, params, &authz).await.is_err(),
            "the crate default was applied instead of the host's ceiling"
        );
    }

    /// An index list that cannot be answered coherently is refused rather than answered.
    ///
    /// Each of these was previously a result: nothing for an empty list, and doubled totals for
    /// a repeated name — both of which read as facts about the data.
    #[tokio::test]
    async fn an_index_list_that_makes_no_sense_is_refused() {
        let authz: McpAuthzRef = Arc::new(Scoped("docs"));
        let too_many: Vec<JsonValue> = (0..=MAX_FEDERATED_INDEXES)
            .map(|n| json!({"index": format!("docs{n}")}))
            .collect();

        for (case, indexes, expected) in [
            ("empty", json!([]), "at least one"),
            (
                "duplicate",
                json!([{"index": "docs"}, {"index": "docs"}]),
                "twice",
            ),
            (
                "too many",
                JsonValue::Array(too_many),
                &*MAX_FEDERATED_INDEXES.to_string(),
            ),
        ] {
            let params = ToolCallParams {
                name: "search_indexes".to_string(),
                arguments: json!({"indexes": indexes, "query": "a"}),
            };
            match call_tool(&StubBackend::default(), params, &authz).await {
                Ok(result) => panic!("{case} was accepted, returning {result}"),
                Err(err) => assert!(
                    err.contains(expected),
                    "{case}: the refusal does not say why: {err}"
                ),
            }
        }
    }

    /// A federated search is charged for every index it names, and nothing is charged for less
    /// than one call.
    #[test]
    fn a_call_costs_what_it_fans_out_to() {
        assert_eq!(tool_cost(&json!({"index": "docs", "query": "a"})), 1);
        assert_eq!(tool_cost(&json!({})), 1);
        assert_eq!(
            tool_cost(&json!({"indexes": [{"index": "a"}, {"index": "b"}, {"index": "c"}]})),
            3
        );
        // A bare name is the same fan-out as an object naming one.
        assert_eq!(tool_cost(&json!({"indexes": ["a", "b"]})), 2);
        // An empty list is refused when decoded; it is still one call to refuse.
        assert_eq!(tool_cost(&json!({"indexes": []})), 1);
        // Charging for fan-out that the dispatcher will refuse would let a malformed call
        // empty a caller's budget.
        let absurd: Vec<JsonValue> = (0..10_000).map(|_| json!({"index": "a"})).collect();
        assert_eq!(
            tool_cost(&json!({"indexes": absurd})),
            MAX_FEDERATED_INDEXES as u32
        );
    }

    #[test]
    fn the_catalogue_only_advertises_tools_the_caller_could_call() {
        let reader: McpAuthzRef = Arc::new(Scoped("docs"));
        assert_eq!(
            visible_tools(&reader, DEFAULT_MAX_SEARCH_LIMIT).len(),
            mcp_tools(DEFAULT_MAX_SEARCH_LIMIT).len()
        );

        // Nothing held, nothing offered. Advertising a tool that the dispatcher will refuse
        // invites an agent to plan around it and fail mid-task.
        let nobody: McpAuthzRef = Arc::new(NoCapabilities);
        assert!(visible_tools(&nobody, DEFAULT_MAX_SEARCH_LIMIT).is_empty());
    }
}
