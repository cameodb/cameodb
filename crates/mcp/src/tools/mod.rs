//! The tool catalogue and the dispatcher that runs a call against the backend.

pub(crate) mod schema;

use serde::{Deserialize, de::DeserializeOwned};
use serde_json::{Value as JsonValue, json};

use crate::{
    authz::{McpAuthzRef, tool_capability},
    backend::{McpBackend, McpIndexSearchRequest},
    tools::schema::{
        DescribeIndexArgs, GetCatalogStatsArgs, ListIndexesArgs, MAX_FEDERATED_INDEXES,
        SearchAcrossIndexesArgs, SearchIndexArgs, ValidateQueryArgs, describe_index_input_schema,
        get_catalog_stats_input_schema, list_indexes_input_schema,
        search_across_indexes_input_schema, search_across_indexes_output_schema,
        search_index_input_schema, search_index_output_schema, validate_query_input_schema,
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

/// Refuse an offset whose window — `offset + limit` — exceeds the advertised maximum.
///
/// The engine fetches `offset + limit` hits to apply the skip after merging, so the sum is
/// what the bound applies to. Checked here for the same reason [`check_limit`] is: the schema
/// advertises the bound, and a promise nothing enforces describes nothing.
fn check_offset_window(
    limit: Option<usize>,
    offset: Option<usize>,
    max_search_limit: usize,
) -> Result<(), String> {
    let limit = limit.unwrap_or(0);
    let offset = offset.unwrap_or(0);
    let window = offset.saturating_add(limit);
    if window > max_search_limit {
        return Err(format!(
            "offset {offset} + limit {limit} = {window} is above the maximum of {max_search_limit}; \
             narrow the query or reduce the offset to stay within the bound"
        ));
    }
    Ok(())
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
            check_offset_window(args.limit, args.offset, backend.max_search_limit())?;
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
                    args.offset,
                )
                .await
        }
        "search_across_indexes" => {
            let args: SearchAcrossIndexesArgs =
                decode_args("search_across_indexes", params.arguments)?;
            check_limit(args.limit, backend.max_search_limit())?;
            check_offset_window(args.limit, args.offset, backend.max_search_limit())?;
            check_index_list(&args.indexes)?;
            // Refuse the whole call rather than quietly dropping the indexes this key may
            // not read: partial results that look complete are worse than an error.
            for request in &args.indexes {
                check_index(authz, &request.index)?;
            }
            backend
                .search_across_indexes(args.indexes, args.query, args.limit, args.offset)
                .await
        }
        "describe_index" => {
            let args: DescribeIndexArgs = decode_args("describe_index", params.arguments)?;
            check_index(authz, &args.index)?;
            backend.describe_index(args.index).await
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
        "get_catalog_stats" => {
            let GetCatalogStatsArgs {} = decode_args("get_catalog_stats", params.arguments)?;
            // Aggregates across the catalogue, which the backend filters to the caller's scope.
            backend.get_catalog_stats(authz.clone()).await
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
            // `search_across_indexes` accepts both a bare name and an object naming one.
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
         partial results, and the error names the clause it could not use.\n\n         Call `describe_index` for an index's fields and their types before constructing a query \
         against unfamiliar data.\n\n{}",
        crate::syntax::compact_reference()
    )
}

/// What `search_across_indexes` adds over `search_index`. The syntax is identical, so it is not repeated.
fn search_across_indexes_description() -> String {
    "Full-text search over several CameoDB indexes at once, executed concurrently and merged.\n\n     Each hit carries `_index_source` naming the index it came from. Per-index `fields` and \
     `sort` parameters override the equivalent inline modifiers, as they do on `search_index`. \
     One query string is applied to every index, so a field that exists in only some of them \
     will not match in the rest.\n\n     Query syntax is the same as `search_index`; see that tool's description, or call \
     `validate_query` with no arguments for the full reference."
        .to_string()
}

/// The descriptions that are prose rather than rendered from the syntax tables.
///
/// Constants so that the catalogue below reads as a list of tools rather than as a wall
/// of text with six tools hidden in it.
const DESCRIBE_INDEX_DESCRIPTION: &str = "Describe one CameoDB index: its schema, its statistics, and how to query it. Returns a 'fields' array giving each field's type and its 'indexed', 'fast' and 'shadow' flags, and a 'query_hints' array naming the operators (phrases, ranges, IN set, boost, slop) each type present supports. Call this before constructing a query against unfamiliar data.\n\nORCHESTRATION TIP: Review the returned fields to identify potential pivot fields (like foreign keys, user IDs, or hashes) before running your search.";

const LIST_INDEXES_DESCRIPTION: &str = "List every CameoDB index this key can see. Each entry carries the index name, its 'description' where an operator wrote one, its document count, and its 'field_names' — enough to choose which index holds the answer. Field types, the 'indexed'/'fast'/'shadow' flags and the per-type query hints come from `describe_index` on the one you pick. Use this as the first discovery step: a new index appears here with no configuration.";

const VALIDATE_QUERY_DESCRIPTION: &str = "Validate and get guidance on CameoDB search query syntax. With both an index and a query, the query is parsed by the same parser a search uses, so the answer is what that search would actually do: whether it parses, where it fails if not, the rewritten form the engine will run, and which clauses match nothing. Also detects unknown or non-indexed fields, offers 'did you mean' suggestions, and returns the full CameoDB query syntax reference.\n\nREADING THE RESULT: `query_analysis.parses` is false when the query is malformed — `syntax_errors` then says where. It is null when the index has no documents yet, meaning the query could not be checked rather than that it passed. `query_analysis.discarded_clauses` lists clauses that parse but can never match, which is how a search silently answers a narrower question than it was asked. `normalized_query` is what the engine actually runs after rewriting.\n\nPRO TIPS FOR AGENTS:\n1. Call with no arguments to get the complete query syntax reference and operator-by-field-type compatibility matrix.\n2. Supply an index name to get schema-aware field validation with type-specific operator hints per field.\n3. Supply a partial_field to get autocomplete suggestions matching available fields.\n4. Supply BOTH index and query for the real parse. A query alone gets only a structural check, which passes things like `title:` and `title:[2020 TO` that do not parse.\n\nORCHESTRATION TIP: Use this tool immediately if `search_index` returns a syntax error, before attempting to guess the correct format.";

const CATALOG_STATS_DESCRIPTION: &str = "Totals across every CameoDB index this key can see: how many indexes, documents, fields, and bytes. For one index, call `describe_index`, which reports its statistics alongside its schema.";

/// One entry in the catalogue: a read, described, with its schemas.
///
/// The display name is given once and lands twice. A client on the revision that added
/// `annotations.title` looks there; the top-level `title` came later, and a client on that looks
/// there. Writing the string in one place is what keeps the two from drifting into different
/// names for the same tool.
///
/// The hints are the same for every tool here because every tool here is a read. `readOnlyHint`
/// says so, and `openWorldHint: false` says the domain is closed: these tools reach nothing but
/// this node's own indexes. That the documents inside them arrive from elsewhere does not open
/// the world the tool interacts with, any more than it does for a memory tool — the spec's own
/// example of a closed one.
///
/// `destructiveHint` and `idempotentHint` are deliberately absent. The spec scopes both to tools
/// whose `readOnlyHint` is false, so on a read they are bytes in every catalogue listing that no
/// client should act on. The test below requires them the moment a tool stops being a read.
fn read_only_tool(
    name: &str,
    title: &str,
    description: JsonValue,
    input_schema: JsonValue,
    output_schema: Option<JsonValue>,
) -> JsonValue {
    let mut tool = json!({
        "name": name,
        "title": title,
        "description": description,
        "inputSchema": input_schema,
        "annotations": {
            "title": title,
            "readOnlyHint": true,
            "openWorldHint": false
        }
    });
    if let Some(schema) = output_schema
        && let Some(object) = tool.as_object_mut()
    {
        object.insert("outputSchema".to_string(), schema);
    }
    tool
}

pub(crate) fn mcp_tools(max_search_limit: usize) -> Vec<JsonValue> {
    vec![
        read_only_tool(
            "search_index",
            "Search Index",
            search_index_description().into(),
            search_index_input_schema(max_search_limit),
            Some(search_index_output_schema()),
        ),
        read_only_tool(
            "search_across_indexes",
            "Federated Search",
            search_across_indexes_description().into(),
            search_across_indexes_input_schema(max_search_limit),
            Some(search_across_indexes_output_schema()),
        ),
        read_only_tool(
            "describe_index",
            "Describe Index",
            DESCRIBE_INDEX_DESCRIPTION.into(),
            describe_index_input_schema(),
            None,
        ),
        read_only_tool(
            "list_indexes",
            "List Indexes",
            LIST_INDEXES_DESCRIPTION.into(),
            list_indexes_input_schema(),
            None,
        ),
        read_only_tool(
            "validate_query",
            "Validate Query",
            VALIDATE_QUERY_DESCRIPTION.into(),
            validate_query_input_schema(),
            None,
        ),
        read_only_tool(
            "get_catalog_stats",
            "Catalog Statistics",
            CATALOG_STATS_DESCRIPTION.into(),
            get_catalog_stats_input_schema(),
            None,
        ),
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

    /// Every tool carries its display name in both places a client looks for one.
    ///
    /// `annotations.title` is where the revision that introduced it put the name; the top-level
    /// `title` came later. A client on either finds a name, and neither finds a different one.
    #[test]
    fn a_tool_names_itself_the_same_way_twice() {
        for tool in mcp_tools(DEFAULT_MAX_SEARCH_LIMIT) {
            let name = tool["name"].as_str().unwrap_or("?");
            let title = tool["title"].as_str().unwrap_or_default();
            assert!(!title.is_empty(), "{name} has no display name");
            assert_eq!(
                tool["annotations"]["title"].as_str(),
                Some(title),
                "{name} gives two different display names"
            );
        }
    }

    /// The hints a read does not need are absent, and become required if a tool stops being one.
    ///
    /// The spec scopes `destructiveHint` and `idempotentHint` to tools whose `readOnlyHint` is
    /// false, so on a read they are bytes in every catalogue listing that no client should act
    /// on. Emitting them anyway would also be the easy way to get them wrong later: a write tool
    /// inheriting `destructiveHint: false` from a template is worse than one that has to state
    /// it. This is the other half of the deny default in the capability table.
    #[test]
    fn a_tool_that_is_not_a_read_must_say_what_it_does() {
        for tool in mcp_tools(DEFAULT_MAX_SEARCH_LIMIT) {
            let name = tool["name"].as_str().unwrap_or("?");
            let annotations = &tool["annotations"];
            // The domain is this node's own indexes, whatever is ingested into them.
            assert_eq!(
                annotations["openWorldHint"],
                json!(false),
                "{name} claims an open world; these tools reach nothing but local indexes"
            );

            if annotations["readOnlyHint"] == json!(true) {
                for hint in ["destructiveHint", "idempotentHint"] {
                    assert!(
                        annotations.get(hint).is_none(),
                        "{name} is a read and carries '{hint}', which the spec reads only on \
                         tools that are not"
                    );
                }
                continue;
            }

            for hint in ["destructiveHint", "idempotentHint"] {
                assert!(
                    annotations
                        .get(hint)
                        .is_some_and(|value| value.is_boolean()),
                    "{name} is not a read, so it has to state '{hint}' rather than leave a \
                     client to assume the default"
                );
            }
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
        for tool in ["list_indexes", "validate_query", "get_catalog_stats"] {
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
                "search_across_indexes",
                json!({"indexes": [{"index": "docs", "feilds": ["title"]}], "query": "a"}),
            ),
            ("describe_index", json!({"index": "docs", "verbose": true})),
            ("list_indexes", json!({"index": "docs"})),
            ("validate_query", json!({"quer": "a"})),
            ("get_catalog_stats", json!({"indexes": ["docs"]})),
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
                "search_across_indexes",
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
                name: "search_across_indexes".to_string(),
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

    /// An entry may be the name on its own, and a misspelling inside the object form is still
    /// refused by name.
    ///
    /// The bare form is what most entries want — a federated search usually names indexes and
    /// nothing else. What it must not cost is the object form's error quality: "data did not
    /// match any variant" tells a caller nothing it can correct.
    #[tokio::test]
    async fn an_index_entry_may_be_a_bare_name() {
        let authz: McpAuthzRef = Arc::new(Scoped("docs"));

        let params = ToolCallParams {
            name: "search_across_indexes".to_string(),
            arguments: json!({"indexes": ["docs"], "query": "a"}),
        };
        assert!(
            call_tool(&StubBackend::default(), params, &authz)
                .await
                .is_ok(),
            "a bare index name was refused"
        );

        // Mixed with the object form, and the scope check still sees both names.
        let params = ToolCallParams {
            name: "search_across_indexes".to_string(),
            arguments: json!({"indexes": ["docs", {"index": "payroll"}], "query": "a"}),
        };
        let err = call_tool(&StubBackend::default(), params, &authz)
            .await
            .expect_err("an index outside the scope was accepted");
        assert!(err.contains("payroll"), "{err}");

        // Duplicate detection reads through both forms, since one name twice is one name twice
        // however it was written.
        let params = ToolCallParams {
            name: "search_across_indexes".to_string(),
            arguments: json!({"indexes": ["docs", {"index": "docs"}], "query": "a"}),
        };
        let err = call_tool(&StubBackend::default(), params, &authz)
            .await
            .expect_err("the same index named twice in two forms was accepted");
        assert!(err.contains("twice"), "{err}");

        // And the object form still names what it refused.
        let params = ToolCallParams {
            name: "search_across_indexes".to_string(),
            arguments: json!({"indexes": [{"index": "docs", "feilds": ["title"]}], "query": "a"}),
        };
        let err = call_tool(&StubBackend::default(), params, &authz)
            .await
            .expect_err("a misspelled projection was accepted");
        assert!(
            err.contains("feilds"),
            "the bare-name form cost the object form its error: {err}"
        );
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

    #[test]
    fn check_limit_refuses_past_the_maximum() {
        assert!(check_limit(Some(101), 100).is_err());
        assert!(check_limit(Some(100), 100).is_ok());
        assert!(check_limit(None, 100).is_ok());
    }

    #[test]
    fn check_offset_window_refuses_when_offset_plus_limit_exceeds_the_maximum() {
        // offset + limit within the bound is accepted.
        assert!(check_offset_window(Some(50), Some(50), 100).is_ok());
        assert!(check_offset_window(Some(100), Some(0), 100).is_ok());
        assert!(check_offset_window(None, Some(100), 100).is_ok());

        // offset + limit past the bound is refused.
        assert!(check_offset_window(Some(50), Some(51), 100).is_err());
        assert!(check_offset_window(Some(1), Some(100), 100).is_err());

        // Defaults: no limit and no offset is a zero window, always accepted.
        assert!(check_offset_window(None, None, 100).is_ok());
    }
}
