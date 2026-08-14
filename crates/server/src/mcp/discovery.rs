//! Describing the catalogue: what indexes exist, what is in them, and whether a query fits.

use futures::{StreamExt, future::BoxFuture, stream::FuturesUnordered};
use serde_json::Value as JsonValue;

use cameodb_mcp::McpAuthzRef;

use crate::authz::retain_visible_indexes;
use crate::mcp::diagnostics::{analyze_query, cameodb_syntax_reference};
use crate::mcp::schema::{
    absent_index_reason, catalogue_entry, enrich_index_entry, extract_field_info,
    extract_field_names, field_query_hint, index_schema,
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

        let stats = listing
            .get("indexes")
            .and_then(|value| value.as_array())
            .and_then(|indexes| {
                indexes.iter().find(|item| {
                    item.get("name")
                        .and_then(|value| value.as_str())
                        .is_some_and(|name| name == index)
                })
            })
            .cloned()
            .ok_or_else(|| format!("Index '{}' not found", index))?;

        let entry = serde_json::json!({
            "index": index,
            "stats": stats,
            "schema": index_schema(&state, &index).await,
        });

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

        let indexes = listing
            .get("indexes")
            .and_then(|value| value.as_array())
            .cloned()
            .unwrap_or_default();

        // One schema read per index, run together rather than in series: the listing is the
        // first thing an agent calls, and a catalogue of two hundred indexes should not cost two
        // hundred sequential round trips. The schema is read for the field names and the
        // operator's description; what it says about each field's type belongs to
        // `describe_index`, on whichever index the listing leads to.
        let mut schema_reads = FuturesUnordered::new();
        for stats in indexes {
            let index_name = stats
                .get("name")
                .and_then(|value| value.as_str())
                .ok_or_else(|| "Index entry missing name".to_string())?
                .to_string();
            let state = state.clone();
            schema_reads.push(async move {
                let schema = index_schema(&state, &index_name).await;
                catalogue_entry(&index_name, &stats, &schema)
            });
        }

        let mut enriched = Vec::with_capacity(schema_reads.len());
        while let Some(entry) = schema_reads.next().await {
            enriched.push(entry);
        }
        // Concurrency reorders them; a catalogue an agent reads twice should not change
        // order between reads.
        enriched.sort_by(|left, right| {
            left.get("index")
                .and_then(|v| v.as_str())
                .cmp(&right.get("index").and_then(|v| v.as_str()))
        });

        let total_indexes = enriched.len();

        Ok(serde_json::json!({
            "indexes": enriched,
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
            Some(enrich_index_entry(serde_json::json!({
                "index": index_name,
                "schema": schema,
            })))
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
                            "field": info.name,
                            "type": info.field_type,
                            "indexed": info.indexed,
                            "shadow": info.is_shadow,
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
                let mut entry = serde_json::json!({
                    "field": info.name,
                    "type": info.field_type,
                    "indexed": info.indexed,
                    "shadow": info.is_shadow,
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
        let query_analysis = query
            .as_ref()
            .map(|query_text| analyze_query(query_text, &field_infos));

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
            let stats = details.get("stats").cloned().unwrap_or(JsonValue::Null);
            let field_names = extract_field_names(&details);
            let field_count = field_names.len();

            return Ok(serde_json::json!({
                "scope": "single_index",
                "index": index_name,
                "field_count": field_count,
                "field_names": field_names,
                "stats": stats,
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

        let raw_indexes = listing
            .get("indexes")
            .and_then(|value| value.as_array())
            .cloned()
            .unwrap_or_default();

        let mut schema_reads = FuturesUnordered::new();
        for stats in raw_indexes {
            let index_name = stats
                .get("name")
                .and_then(|value| value.as_str())
                .unwrap_or_default()
                .to_string();
            let state = state.clone();
            schema_reads.push(async move {
                let schema = index_schema(&state, &index_name).await;
                enrich_index_entry(serde_json::json!({
                    "index": index_name,
                    "stats": stats,
                    "schema": schema,
                }))
            });
        }

        let mut indexes = Vec::with_capacity(schema_reads.len());
        while let Some(entry) = schema_reads.next().await {
            indexes.push(entry);
        }
        indexes.sort_by(|left, right| {
            left.get("index")
                .and_then(|v| v.as_str())
                .cmp(&right.get("index").and_then(|v| v.as_str()))
        });

        let mut total_documents = 0u64;
        let mut total_size_bytes = 0u64;
        let mut total_fields = 0usize;

        for item in &indexes {
            if let Some(stats) = item.get("stats") {
                total_documents += stats
                    .get("document_count")
                    .and_then(|value| value.as_u64())
                    .unwrap_or(0);
                total_size_bytes += stats
                    .get("total_size_bytes")
                    .and_then(|value| value.as_u64())
                    .unwrap_or(0);
            }

            total_fields += extract_field_names(item).len();
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
