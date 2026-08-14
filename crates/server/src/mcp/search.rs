//! The two search tools, and the merge that makes a federated one answerable.

use futures::{StreamExt, future::BoxFuture, stream::FuturesUnordered};
use serde_json::Value as JsonValue;

use cameodb_mcp::McpIndexSearchRequest;

use crate::cluster_coordinator::OperationType;
use crate::mcp::diagnostics::{
    names_a_missing_field, refuse_if_clauses_discarded, with_valid_fields, zero_results_advice,
};
use crate::mcp::schema::absent_index_reason;
use crate::node_orchestrator::ClientOp;
use crate::query::parse_query_keywords;
use crate::state::AppState;

pub(super) fn search_index(
    state: AppState,
    index: McpIndexSearchRequest,
    query: String,
    limit: Option<usize>,
) -> BoxFuture<'static, Result<JsonValue, String>> {
    Box::pin(async move {
        let index_name = index.index.clone();

        // Preprocess query to extract return/limit/sort modifiers (same as HTTP server)
        let (cleaned_query, parsed_limit, parsed_fields, parsed_sort) =
            parse_query_keywords(&query);

        // Merge MCP-provided values with parsed values (MCP takes precedence for limit/fields)
        let final_limit = limit.or(parsed_limit);
        let final_fields = index.fields.or(parsed_fields);
        let final_sort = parsed_sort;

        let result = state
            .router
            .route_and_handle(
                ClientOp::Search {
                    index: index.index,
                    query: cleaned_query,
                    limit: final_limit,
                    fields: final_fields,
                    sort: final_sort,
                },
                None,
                OperationType::Read,
            )
            .await;

        match result {
            Ok(mut response) => {
                // Checked before the hits are described, since a dropped clause makes the
                // rest of this response answer a different query.
                refuse_if_clauses_discarded(&response)?;

                let total_hits = response
                    .get("total_hits")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);

                // Nothing matched, which is also what a search on an index that does not
                // exist looks like. Settle which one it was before describing the result.
                if total_hits == 0
                    && let Some(reason) = absent_index_reason(&state, &index_name).await
                {
                    return Err(reason);
                }

                // Add zero-results warning if applicable
                let hits_returned = response
                    .get("hits_returned")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);

                if hits_returned == 0
                    && let Some(advice) = zero_results_advice(&query)
                    && let Some(obj) = response.as_object_mut()
                {
                    obj.insert("_warning".to_string(), JsonValue::String(advice));
                }
                Ok(response)
            }
            Err(err) => {
                let err_str = err.to_string();

                if names_a_missing_field(&err_str)
                    && let Ok(schema_result) = state
                        .router
                        .handle_client_op(ClientOp::GetConfig {
                            index: index_name.clone(),
                        })
                        .await
                    && let Some(fields_obj) =
                        schema_result.get("fields").and_then(|v| v.as_object())
                {
                    let field_names: Vec<String> = fields_obj.keys().cloned().collect();
                    return Err(with_valid_fields(&err_str, &index_name, &field_names));
                }

                Err(err_str)
            }
        }
    })
}

pub(super) fn search_indexes(
    state: AppState,
    indexes: Vec<McpIndexSearchRequest>,
    query: String,
    limit: Option<usize>,
) -> BoxFuture<'static, Result<JsonValue, String>> {
    Box::pin(async move {
        // Preprocess query to extract return/limit/sort modifiers (same as HTTP server)
        let (cleaned_query, parsed_limit, parsed_fields, parsed_sort) =
            parse_query_keywords(&query);

        // One derivation, so that the value asked of each index, the value the merge
        // truncates to, and the value reported back are the same number — including when it
        // comes from an inline `limit` clause or from this node's configured default.
        let final_limit = limit.or(parsed_limit);
        let requested_limit = final_limit.unwrap_or(state.router.default_search_limit());

        // Determine the global sort spec (if any) for the final merge.
        // Per-index sort takes precedence; fall back to query-parsed sort.
        let global_sort: Option<storage::SortSpec> = indexes
            .iter()
            .find_map(|req| req.sort.as_ref())
            .map(|mcp_sort| storage::SortSpec {
                field: mcp_sort.field.clone(),
                order: match mcp_sort.order {
                    cameodb_mcp::SortOrder::Desc => storage::SortOrder::Desc,
                    cameodb_mcp::SortOrder::Asc => storage::SortOrder::Asc,
                },
            })
            .or_else(|| {
                parsed_sort.as_ref().map(|storage_sort| storage::SortSpec {
                    field: storage_sort.field.clone(),
                    order: storage_sort.order,
                })
            });

        // Read before the requests move into the futures below, for the "did anything at
        // all answer" test after the merge.
        let index_count = indexes.len();

        // Launch all index searches concurrently
        let mut search_futures = FuturesUnordered::new();
        for index_request in indexes {
            let McpIndexSearchRequest {
                index,
                fields,
                sort,
            } = index_request;
            let index_name = index.clone();
            let state = state.clone();
            let cleaned_query = cleaned_query.clone();
            let parsed_fields = parsed_fields.clone();
            let parsed_sort = parsed_sort.clone();

            search_futures.push(async move {
                // Merge MCP-provided fields/sort with parsed values
                let final_fields = fields.or(parsed_fields);
                let final_sort = sort.or_else(|| {
                    parsed_sort.map(|storage_sort| cameodb_mcp::SortSpec {
                        field: storage_sort.field,
                        order: match storage_sort.order {
                            storage::SortOrder::Desc => cameodb_mcp::SortOrder::Desc,
                            storage::SortOrder::Asc => cameodb_mcp::SortOrder::Asc,
                        },
                    })
                });

                // Convert MCP SortSpec to storage SortSpec
                let storage_sort = final_sort.map(|mcp_sort| storage::SortSpec {
                    field: mcp_sort.field,
                    order: match mcp_sort.order {
                        cameodb_mcp::SortOrder::Desc => storage::SortOrder::Desc,
                        cameodb_mcp::SortOrder::Asc => storage::SortOrder::Asc,
                    },
                });

                // The merge below orders by `_sort_key`, so this is the one caller that
                // needs it to survive the routing path. The strip after the merge is what
                // keeps it off the response.
                let result = state
                    .router
                    .route_and_handle_keeping_sort_keys(
                        ClientOp::Search {
                            index: index.clone(),
                            query: cleaned_query,
                            limit: final_limit,
                            fields: final_fields,
                            sort: storage_sort,
                        },
                        None,
                        OperationType::Read,
                    )
                    .await;

                (index_name, result)
            });
        }

        let mut merged_hits = Vec::new();
        let mut total_hits = 0u64;
        // What could not be reached, named so the caller can tell which part of its request
        // is missing from the answer. An index that fails does not sink the ones that
        // answered: partial results with an explicit account of the gap are actionable,
        // where a failed call throws away work that succeeded.
        let mut errors: Vec<JsonValue> = Vec::new();

        while let Some((index_name, result)) = search_futures.next().await {
            // Schema-aware error handling
            let result = match result {
                Ok(r) => r,
                Err(err) => {
                    let err_str = err.to_string();

                    let mut message = err_str.clone();
                    if names_a_missing_field(&err_str)
                        && let Ok(schema_result) = state
                            .router
                            .handle_client_op(ClientOp::GetConfig {
                                index: index_name.clone(),
                            })
                            .await
                        && let Some(fields_obj) =
                            schema_result.get("fields").and_then(|v| v.as_object())
                    {
                        let field_names: Vec<String> = fields_obj.keys().cloned().collect();
                        message = with_valid_fields(&err_str, &index_name, &field_names);
                    }

                    errors.push(serde_json::json!({"index": index_name, "error": message}));
                    continue;
                }
            };

            // One query string covers every index, so a dropped clause affects the whole
            // merge rather than this index alone.
            if let Err(refusal) = refuse_if_clauses_discarded(&result) {
                return Err(format!("index '{index_name}': {refusal}"));
            }

            let index_hits = result
                .get("total_hits")
                .and_then(|value| value.as_u64())
                .unwrap_or(0);

            // An index that answered with nothing may not be an index at all.
            if index_hits == 0
                && let Some(reason) = absent_index_reason(&state, &index_name).await
            {
                errors.push(serde_json::json!({"index": index_name, "error": reason}));
                continue;
            }

            total_hits += index_hits;

            if let Some(hits) = result.get("hits").and_then(|value| value.as_array()) {
                for hit in hits {
                    let mut hit_value = hit.clone();
                    if let Some(hit_obj) = hit_value.as_object_mut() {
                        hit_obj.insert(
                            "_index_source".to_string(),
                            JsonValue::String(index_name.clone()),
                        );
                    }
                    merged_hits.push(hit_value);
                }
            }
        }

        // Nothing answered. Reported as a failure rather than as an empty result, because
        // an empty `hits` beside a populated `errors` reads to an agent exactly like a query
        // that legitimately matched nothing — which is the one reading that must not be
        // available. Partial failure is a success; total failure is not.
        if !errors.is_empty() && errors.len() == index_count {
            let detail = errors
                .iter()
                .map(|entry| {
                    format!(
                        "{}: {}",
                        entry["index"].as_str().unwrap_or("?"),
                        entry["error"].as_str().unwrap_or("unknown error")
                    )
                })
                .collect::<Vec<_>>()
                .join("; ");
            return Err(format!("no index in this search could be read — {detail}"));
        }

        // Merge sort: if a global sort spec was determined, order by _sort_key
        // (injected by the engine for field-sorted queries). Otherwise sort by
        // _score descending (the standard relevance merge).
        match &global_sort {
            Some(spec) => {
                merged_hits.sort_by(|a, b| compare_hits_by_sort_key(a, b, spec.order));
            }
            None => {
                merged_hits.sort_by(|a, b| {
                    let left_score = a
                        .get("_score")
                        .and_then(|value| value.as_f64())
                        .unwrap_or(0.0);
                    let right_score = b
                        .get("_score")
                        .and_then(|value| value.as_f64())
                        .unwrap_or(0.0);
                    right_score
                        .partial_cmp(&left_score)
                        .unwrap_or(std::cmp::Ordering::Equal)
                });
            }
        }
        merged_hits.truncate(requested_limit);

        // Strip internal _sort_key from merged hits before returning
        for hit in &mut merged_hits {
            if let Some(o) = hit.as_object_mut() {
                o.remove("_sort_key");
            }
        }
        let hits_returned = merged_hits.len();

        let mut response = serde_json::json!({
            "hits": merged_hits,
            "hits_returned": hits_returned,
            "total_hits": total_hits,
            "limit": requested_limit,
        });

        // Present only when something is missing, so that its presence means something. The
        // engine's own search response uses the same key for shard-level failures.
        if !errors.is_empty()
            && let Some(obj) = response.as_object_mut()
        {
            obj.insert("errors".to_string(), JsonValue::Array(errors));
        }

        // Add zero-results warning if applicable
        if hits_returned == 0
            && let Some(advice) = zero_results_advice(&query)
            && let Some(obj) = response.as_object_mut()
        {
            obj.insert("_warning".to_string(), JsonValue::String(advice));
        }

        Ok(response)
    })
}

/// Compare two hit documents by their internal `_sort_key` field for federated
/// merge ordering. Handles i64, f64, and string keys. Documents missing the
/// key always sort last, regardless of the requested order.
fn compare_hits_by_sort_key(
    a: &JsonValue,
    b: &JsonValue,
    order: storage::SortOrder,
) -> std::cmp::Ordering {
    let av = a.get("_sort_key");
    let bv = b.get("_sort_key");
    let base = match (av, bv) {
        (Some(a_val), Some(b_val)) => {
            // Try i64 first for precise large-integer comparison
            if let Some(ai) = a_val.as_i64()
                && let Some(bi) = b_val.as_i64()
            {
                ai.cmp(&bi)
            } else if let Some(af) = a_val.as_f64()
                && let Some(bf) = b_val.as_f64()
            {
                af.partial_cmp(&bf).unwrap_or(std::cmp::Ordering::Equal)
            } else {
                // Fall back to string comparison
                let as_str = a_val.as_str().unwrap_or("");
                let bs_str = b_val.as_str().unwrap_or("");
                as_str.cmp(bs_str)
            }
        }
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => std::cmp::Ordering::Equal,
    };
    match order {
        storage::SortOrder::Asc => base,
        storage::SortOrder::Desc => base.reverse(),
    }
}
