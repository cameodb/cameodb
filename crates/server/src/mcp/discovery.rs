//! Describing the catalogue: what indexes exist, what is in them, and whether a query fits.

use futures::future::BoxFuture;
use serde_json::Value as JsonValue;

use cameodb_mcp::McpAuthzRef;

use crate::authz::retain_visible_indexes;
use crate::mcp::diagnostics::{analyze_query, cameodb_syntax_reference};
use crate::mcp::schema::{
    absent_index_reason, catalogue_entry, enrich_index_entry, enrich_index_entry_owned,
    extract_field_info, extract_field_names, field_query_hint, index_schema,
};
use crate::node_orchestrator::ClientOp;
use crate::state::AppState;

pub(super) fn describe_index(
    state: AppState,
    index: String,
) -> BoxFuture<'static, Result<JsonValue, String>> {
    Box::pin(async move {
        let listing = state
            .router
            .handle_client_op(ClientOp::ListIndexes {
                include_data_size: false,
            })
            .await
            .map_err(|err| err.to_string())?;

        // The listing entry *is* the description. It used to be half of one — statistics here,
        // field definitions from a second `GetConfig`, stitched together by this function in a
        // shape neither endpoint used — which is how the tools came to describe every index as
        // having no fields at all while the bundled client, stitching differently, looked right.
        let entry = listing
            .get("indexes")
            .and_then(|value| value.as_array())
            .and_then(|indexes| {
                indexes
                    .iter()
                    .find(|item| item.get("name").and_then(|v| v.as_str()) == Some(index.as_str()))
            })
            .cloned()
            .ok_or_else(|| format!("Index '{}' not found", index))?;

        Ok(enrich_index_entry(entry))
    })
}

/// The MCP index catalogue, filtered to what the caller may see.
///
/// The tool dispatcher refuses a *named* index outside the caller's scope; this is the
/// enumeration half, and it is also what `get_catalog_stats` and `list_resources` are
/// built on, so filtering here covers all three.
pub(super) fn list_indexes(
    state: AppState,
    authz: McpAuthzRef,
) -> BoxFuture<'static, Result<JsonValue, String>> {
    Box::pin(async move {
        let mut listing = state
            .router
            .handle_client_op(ClientOp::ListIndexes {
                include_data_size: false,
            })
            .await
            .map_err(|err| err.to_string())?;
        retain_visible_indexes(&mut listing, authz.as_ref());

        // One request. Each entry already describes its fields, so the schema read this used to
        // make per index — concurrently, but still one round trip each — is gone.
        //
        // What is kept is narrower than what arrives, deliberately. The listing is where an agent
        // starts, and a full description of every index is most of its context spent before it
        // has chosen one. Types, flags and hints are one `describe_index` away, on the index that
        // turned out to matter.
        let entries: Vec<JsonValue> = listing
            .get("indexes")
            .and_then(|value| value.as_array())
            .map(|indexes| indexes.iter().map(catalogue_entry).collect())
            .unwrap_or_default();

        let total_indexes = entries.len();

        Ok(serde_json::json!({
            "indexes": entries,
            "total_indexes": total_indexes,
            "node_id": listing.get("node_id").cloned().unwrap_or(JsonValue::Null),
            "node_name": listing.get("node_name").cloned().unwrap_or(JsonValue::Null),
            "total_shards": listing.get("total_shards").cloned().unwrap_or(JsonValue::Null),
            "took_ms": listing.get("took_ms").cloned().unwrap_or(JsonValue::Null),
        }))
    })
}

pub(super) fn validate_query(
    state: AppState,
    index: Option<String>,
    partial_field: Option<String>,
    query: Option<String>,
) -> BoxFuture<'static, Result<JsonValue, String>> {
    Box::pin(async move {
        // The index's schema, named — not the catalogue. This wants field definitions and
        // nothing else, and reaching them through `describe_index` meant gathering statistics for
        // every index in the deployment and discarding all of them to validate one query.
        let index_details = if let Some(index_name) = index.clone() {
            let schema = index_schema(&state, &index_name).await;
            // A schema that could not be read is the one case where the catalogue's answer
            // mattered: an index that is not there must be refused, in the same words
            // `describe_index` refuses it, rather than described as having no fields.
            if schema.is_null()
                && let Some(reason) = absent_index_reason(&state, &index_name).await
            {
                return Err(reason);
            }
            // Already the one description shape, fields and all — nothing to stitch.
            Some(schema)
        } else {
            None
        };

        let field_infos = index_details
            .as_ref()
            .map(extract_field_info)
            .unwrap_or_default();

        let field_names: Vec<String> = field_infos.iter().map(|info| info.name.clone()).collect();

        // Field suggestions from partial input
        let field_suggestions = partial_field
            .as_ref()
            .map(|partial| {
                let partial_lower = partial.to_lowercase();
                field_infos
                    .iter()
                    .filter(|info| {
                        let name_lower = info.name.to_lowercase();
                        name_lower.starts_with(&partial_lower)
                            || name_lower.contains(&partial_lower)
                    })
                    .map(|info| {
                        serde_json::json!({
                            "name": info.name,
                            "type": info.field_type,
                            "indexed": info.indexed,
                            "fast": info.fast,
                            "sortable": info.sortable,
                            "shadow": info.is_shadow,
                            "searchable": info.searchable,
                            "queryable": info.is_queryable(),
                        })
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        // Field-type-aware schema summary. Shadow fields are listed rather than filtered
        // out: the identifier's own name is the one field answered without the search
        // index, so a field list that omits it hides the cheapest query in the index.
        let fields_with_types: Vec<JsonValue> = field_infos
            .iter()
            .filter(|info| info.name != "_seq")
            .map(|info| {
                // `fast` belongs here as much as the other flags: it is what decides whether a
                // field can be sorted on, and a caller choosing how to query has to know.
                // Leaving it out was one of the spellings that differed between surfaces.
                //
                // `sortable` sits beside it for the reason `searchable` sits beside `indexed`:
                // `fast` is the declaration and `sortable` is whether the built index carries
                // the column. They differ for a field declared after the index was built, and a
                // caller picking a field to sort on needs the second one.
                let mut entry = serde_json::json!({
                    "name": info.name,
                    "type": info.field_type,
                    "indexed": info.indexed,
                    "fast": info.fast,
                    "sortable": info.sortable,
                    "shadow": info.is_shadow,
                    "searchable": info.searchable,
                    "queryable": info.is_queryable(),
                    "query_hint": field_query_hint(info),
                });
                if let Some(text) = &info.description
                    && let Some(obj) = entry.as_object_mut()
                {
                    obj.insert("description".to_string(), JsonValue::String(text.clone()));
                }
                entry
            })
            .collect();

        // Query analysis with structural validation
        let mut query_analysis = query
            .as_ref()
            .map(|query_text| analyze_query(query_text, &field_infos));

        // The parser's own verdict, which is the only thing here that can answer the question the
        // tool is recommended for: a query that balances and still does not parse. It needs an
        // index, because resolving a field name does, so it runs only when one was named — the
        // structural analysis above is what remains when one was not.
        if let (Some(index_name), Some(query_text), Some(analysis)) =
            (index.as_ref(), query.as_ref(), query_analysis.as_mut())
        {
            let parsed = state
                .router
                .handle_client_op(ClientOp::ValidateQuery {
                    index: index_name.clone(),
                    query: query_text.clone(),
                })
                .await
                .map_err(|err| err.to_string())?;

            merge_parser_verdict(analysis, &parsed);
        }

        Ok(serde_json::json!({
            "index": index,
            "field_suggestions": field_suggestions,
            "query_analysis": query_analysis,
            "syntax_reference": cameodb_syntax_reference(),
            "available_fields": fields_with_types,
            "searchable_field_names": field_names,
        }))
    })
}

/// Fold the engine's parse of a query into the structural analysis of it.
///
/// Kept as a merge rather than a replacement because the two see different things. The parser is
/// authoritative on whether the query runs and on what the engine will actually execute; the
/// structural pass is what produces the "did you mean" suggestions, which need the schema's field
/// names and the parser never offers.
///
/// `parses` is the field worth reading first: it is the answer the tool previously could not give
/// at all, and it is `null` rather than `true` when the index has no documents to check against —
/// an unchecked query must not read as a passing one.
fn merge_parser_verdict(analysis: &mut JsonValue, parsed: &JsonValue) {
    let Some(object) = analysis.as_object_mut() else {
        return;
    };

    let syntax_errors = parsed
        .get("syntax_errors")
        .cloned()
        .unwrap_or_else(|| serde_json::json!([]));
    let parses = match parsed.get("valid") {
        // `valid` folds in discarded clauses, which are reported separately here; what this
        // field claims is narrower and answerable on its own.
        Some(JsonValue::Null) | None => JsonValue::Null,
        Some(_) => JsonValue::Bool(
            syntax_errors
                .as_array()
                .is_none_or(|errors| errors.is_empty()),
        ),
    };

    object.insert("parses".to_string(), parses);
    object.insert("syntax_errors".to_string(), syntax_errors.clone());

    if let Some(normalized) = parsed.get("normalized_query") {
        object.insert("normalized_query".to_string(), normalized.clone());
    }
    if let Some(discarded) = parsed.get("discarded") {
        object.insert("discarded_clauses".to_string(), discarded.clone());
    }
    if let Some(note) = parsed.get("note") {
        object.insert("note".to_string(), note.clone());
    }

    // A syntax error is the finding, not a footnote: it is why the search the agent was about to
    // run would have failed, and the structural warnings above it all passed.
    if let Some(JsonValue::Array(warnings)) = object.get_mut("warnings")
        && let Some(errors) = syntax_errors.as_array()
    {
        for error in errors {
            if let Some(text) = error.as_str() {
                warnings.push(JsonValue::String(format!(
                    "Query does not parse: {text}. The clause it names was dropped, so a search \
                     with this query would answer a different question."
                )));
            }
        }
    }
}

/// Statistics for one index, or totals across the catalogue.
///
/// Both cases live here because the `cameodb://indexes/{index}/stats` resource asks for one index
/// while the tool asks for the catalogue. The tool takes no index argument: naming one would be
/// asking `describe_index`'s question through a tool called `get_catalog_stats`.
pub(super) fn index_stats(
    state: AppState,
    index: Option<String>,
    authz: McpAuthzRef,
) -> BoxFuture<'static, Result<JsonValue, String>> {
    Box::pin(async move {
        if let Some(index_name) = index {
            let details = describe_index(state.clone(), index_name.clone()).await?;
            let field_names = extract_field_names(&details);

            return Ok(serde_json::json!({
                "scope": "single_index",
                "index": index_name,
                "field_count": field_names.len(),
                "field_names": field_names,
                "stats": details,
            }));
        }

        // Its own listing rather than `list_indexes` above, for one reason: the sizes.
        // `total_size_bytes` is emitted only when the listing is asked for data sizes, which
        // the shared listing does not do — an aggregate built on it can only ever sum a key
        // that is absent. An explicit statistics request is the one place the redb size
        // computation is worth paying for.
        let mut listing = state
            .router
            .handle_client_op(ClientOp::ListIndexes {
                include_data_size: true,
            })
            .await
            .map_err(|err| err.to_string())?;
        // Already scoped: the aggregate is over the indexes this caller can see, so
        // the totals it reports do not count documents it cannot read.
        retain_visible_indexes(&mut listing, authz.as_ref());

        // Each entry already describes its fields, so the schema read this made per index is
        // gone along with the stitching that combined the two.
        let mut indexes: Vec<JsonValue> = listing
            .get("indexes")
            .and_then(|value| value.as_array())
            .map(|entries| entries.iter().map(enrich_index_entry_owned).collect())
            .unwrap_or_default();

        indexes.sort_by(|left, right| {
            left.get("name")
                .and_then(|v| v.as_str())
                .cmp(&right.get("name").and_then(|v| v.as_str()))
        });

        let mut total_documents = 0u64;
        let mut total_size_bytes = 0u64;
        let mut total_fields = 0usize;

        for item in &indexes {
            total_documents += item
                .get("document_count")
                .and_then(|value| value.as_u64())
                .unwrap_or(0);
            total_size_bytes += item
                .get("total_size_bytes")
                .and_then(|value| value.as_u64())
                .unwrap_or(0);
            total_fields += item
                .get("field_count")
                .and_then(|value| value.as_u64())
                .unwrap_or(0) as usize;
        }

        Ok(serde_json::json!({
            "scope": "all_indexes",
            "total_indexes": indexes.len(),
            "total_documents": total_documents,
            "total_size_bytes": total_size_bytes,
            "total_fields": total_fields,
            "indexes": indexes,
        }))
    })
}
