//! The two search tools, and the merge that makes a federated one answerable.

use futures::{StreamExt, future::BoxFuture, stream};
use serde_json::Value as JsonValue;

use cameodb_mcp::McpIndexSearchRequest;

use crate::cluster_coordinator::OperationType;
use crate::mcp::diagnostics::{
    names_a_missing_field, refuse_if_clauses_discarded, with_valid_fields, zero_results_advice,
};
use crate::mcp::schema::absent_index_reason;
use crate::node_orchestrator::{ClientOp, order_hit_blocks};
use crate::query::parse_query_keywords;
use crate::state::AppState;

/// How many of a federated search's indexes are queried at once.
///
/// The bound on how many may be *named* is the protocol's; this is the node's. Each name is a
/// scatter-gather across that index's shards, so an uncapped fan-out lets one request occupy
/// every shard worker at once and starve the searches already running. Well below the naming
/// bound on purpose: a wide search finishes slightly later and costs the node no more than a
/// narrow one.
const MAX_CONCURRENT_INDEX_SEARCHES: usize = 8;

/// Trim a response down to `max_bytes`, and say so in the response itself.
///
/// A limit bounds how many hits come back, not how large they are: ten thousand hits is within
/// every bound the tools advertise and can still be more bytes than the node is configured to
/// carry in one message. `max_bytes` follows that configured size, so this is a backstop and
/// not a routine event — a deployment that handles large documents raises the message size and
/// this moves with it.
///
/// Trimming is the right answer rather than refusing: the hits that fit answer the question as
/// far as they go. But it is only safe if the caller is told. An agent that knows it saw part of
/// the result narrows its query; one that does not reports what it read as though it were all
/// there was.
///
/// `total_hits` is left alone: it counts what matched, which trimming does not change. What
/// changes is `hits_returned`, and `_omitted_hits` says how many were dropped here — dropped
/// from the end, so what remains is still the front of the same order.
///
/// One hit always survives, even one larger than the whole allowance. A response trimmed to
/// nothing reads exactly like a query that matched nothing, and the two must not look alike.
fn cap_response_bytes(response: &mut JsonValue, max_bytes: usize) {
    let Some(object) = response.as_object_mut() else {
        return;
    };
    let Some(JsonValue::Array(hits)) = object.remove("hits") else {
        return;
    };

    // Measured without the hits, so the room left for them is what is actually left.
    let envelope = serde_json::to_vec(&*object)
        .map(|bytes| bytes.len())
        .unwrap_or(0);
    let mut used = envelope;
    let mut kept = 0usize;
    for hit in &hits {
        let hit_bytes = serde_json::to_vec(hit)
            .map(|bytes| bytes.len())
            .unwrap_or(0);
        // `+ 1` for the comma that joins it to the previous hit.
        if kept > 0 && used + hit_bytes + 1 > max_bytes {
            break;
        }
        used += hit_bytes + 1;
        kept += 1;
    }

    let omitted = hits.len() - kept;
    let mut hits = hits;
    hits.truncate(kept);
    object.insert("hits".to_string(), JsonValue::Array(hits));
    object.insert("hits_returned".to_string(), JsonValue::from(kept));

    if omitted > 0 {
        object.insert("_truncated".to_string(), JsonValue::Bool(true));
        object.insert("_omitted_hits".to_string(), JsonValue::from(omitted));
        // The same key the zero-results advice uses, and they cannot both apply: a response
        // with no hits has nothing to trim.
        object.insert(
            "_warning".to_string(),
            JsonValue::String(format!(
                "{omitted} of the hits for this query were left out: the full response would \
                 have exceeded the largest single message this node sends ({max_bytes} bytes). \
                 The ones returned are the highest ranked, in order. Narrow the query — add a \
                 field, a phrase or an `AND` clause — or ask for fewer fields with `return`, \
                 rather than reading these as the whole result."
            )),
        );
    }
}

/// Refuse a limit above what the tool schemas advertise.
///
/// Checked on the value the search will actually run with rather than on the argument, because
/// the query string is a second door into the same number: `limit 5000000` written inline
/// reaches this after the argument has already been validated and found absent. `None` needs no
/// check — the node's own default fills it in, and config load refuses a default above the
/// ceiling so that the value which arrives here cannot exceed it.
fn check_effective_limit(limit: Option<usize>, max_search_limit: usize) -> Result<(), String> {
    match limit {
        Some(limit) if limit > max_search_limit => Err(format!(
            "limit {limit} is above the maximum of {max_search_limit}; ask for at most that many \
             and narrow the query to reach the rest"
        )),
        _ => Ok(()),
    }
}

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
        check_effective_limit(final_limit, state.max_search_limit)?;
        let final_fields = index.fields.or(parsed_fields);
        // The argument wins over an inline `sort` clause, as `limit` and `fields` do: a caller
        // that passed a structured sort chose it deliberately, where the clause may be part of
        // a query string it copied.
        let final_sort = index
            .sort
            .map(|requested| storage::SortSpec {
                field: requested.field,
                order: match requested.order {
                    cameodb_mcp::SortOrder::Desc => storage::SortOrder::Desc,
                    cameodb_mcp::SortOrder::Asc => storage::SortOrder::Asc,
                },
            })
            .or(parsed_sort);

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
                cap_response_bytes(&mut response, state.max_response_bytes);
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
        check_effective_limit(final_limit, state.max_search_limit)?;
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

        // Each index is searched concurrently with the others, up to
        // `MAX_CONCURRENT_INDEX_SEARCHES` at a time — the rest start as those finish.
        let searches = indexes
            .into_iter()
            .enumerate()
            .map(|(named_at, index_request)| {
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

                async move {
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

                    (named_at, index_name, result)
                }
            });
        let mut search_futures =
            stream::iter(searches).buffer_unordered(MAX_CONCURRENT_INDEX_SEARCHES);

        // One block per index, held against the position the caller named it at. The searches
        // finish in whatever order they finish, and a merge that let that decide a tie would
        // answer one query differently on each run.
        let mut blocks: Vec<(usize, Vec<JsonValue>)> = Vec::new();
        let mut total_hits = 0u64;
        // What could not be reached, named so the caller can tell which part of its request
        // is missing from the answer. An index that fails does not sink the ones that
        // answered: partial results with an explicit account of the gap are actionable,
        // where a failed call throws away work that succeeded.
        let mut errors: Vec<JsonValue> = Vec::new();

        while let Some((named_at, index_name, result)) = search_futures.next().await {
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
                let block: Vec<JsonValue> = hits
                    .iter()
                    .map(|hit| {
                        let mut hit_value = hit.clone();
                        if let Some(hit_obj) = hit_value.as_object_mut() {
                            hit_obj.insert(
                                "_index_source".to_string(),
                                JsonValue::String(index_name.clone()),
                            );
                        }
                        hit_value
                    })
                    .collect();
                blocks.push((named_at, block));
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

        // Ordered by the requested sort when there is one, otherwise by relevance — and, where
        // those tie, by the order the caller named the indexes in. The same merge the routing
        // layer uses across shards and nodes, one level up.
        blocks.sort_by_key(|(named_at, _)| *named_at);
        let mut merged_hits = order_hit_blocks(
            blocks.into_iter().map(|(_, block)| block).collect(),
            global_sort.as_ref(),
            requested_limit,
        );

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

        cap_response_bytes(&mut response, state.max_response_bytes);
        Ok(response)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn response_with(hits: usize) -> JsonValue {
        let hits: Vec<JsonValue> = (0..hits)
            .map(|n| json!({"id": format!("d{n:03}"), "body": "x".repeat(100)}))
            .collect();
        json!({
            "hits": hits,
            "hits_returned": hits.len(),
            "total_hits": 5_000,
            "limit": 100,
        })
    }

    /// A response inside the allowance is left exactly as it was.
    ///
    /// The trim has to be invisible in the ordinary case: a `_truncated` flag on a complete
    /// response would teach an agent to distrust every result it reads.
    #[test]
    fn a_response_that_fits_is_untouched() {
        let mut response = response_with(3);
        let before = response.clone();
        cap_response_bytes(&mut response, 1024 * 1024);
        assert_eq!(response, before);
        assert!(response.get("_truncated").is_none());
        assert!(response.get("_omitted_hits").is_none());
    }

    /// What is dropped is accounted for, and what matched is not restated.
    #[test]
    fn an_oversized_response_is_trimmed_and_says_so() {
        let mut response = response_with(50);
        cap_response_bytes(&mut response, 1_000);

        let kept = response["hits"].as_array().expect("hits").len();
        assert!(kept > 0 && kept < 50, "kept {kept} of 50");
        assert_eq!(response["hits_returned"].as_u64(), Some(kept as u64));
        assert_eq!(response["_truncated"], json!(true));
        assert_eq!(response["_omitted_hits"].as_u64(), Some((50 - kept) as u64));
        // `total_hits` counts what matched, which trimming does not change.
        assert_eq!(response["total_hits"].as_u64(), Some(5_000));
        let warning = response["_warning"].as_str().unwrap_or_default();
        assert!(
            warning.contains("Narrow the query"),
            "the trim should say what to do about it: {response}"
        );
        assert!(
            warning.contains("1000 bytes"),
            "the trim should name the bound it hit, so an operator knows which knob: {warning}"
        );

        // Within the allowance it was given, give or take the closing bracket.
        let size = serde_json::to_vec(&response).expect("serialize").len();
        assert!(
            size < 1_000 + 600,
            "trimmed to {size} bytes against an allowance of 1000"
        );
    }

    /// The hits kept are the front of the order, not a sample of it.
    #[test]
    fn a_trim_keeps_the_highest_ranked_hits() {
        let mut response = response_with(20);
        cap_response_bytes(&mut response, 800);
        let ids: Vec<String> = response["hits"]
            .as_array()
            .expect("hits")
            .iter()
            .filter_map(|hit| hit["id"].as_str().map(str::to_string))
            .collect();
        assert!(ids.len() > 1 && ids.len() < 20, "kept {}", ids.len());
        let expected: Vec<String> = (0..ids.len()).map(|n| format!("d{n:03}")).collect();
        assert_eq!(ids, expected, "the trim did not keep a prefix of the order");
    }

    /// One hit survives an allowance too small for any hit at all.
    ///
    /// A response trimmed to nothing reads exactly like a query that matched nothing, and those
    /// two must not look alike — so the caller gets one hit, the flag, and the count.
    #[test]
    fn a_single_oversized_hit_is_still_returned() {
        let mut response = response_with(5);
        cap_response_bytes(&mut response, 1);
        assert_eq!(response["hits"].as_array().expect("hits").len(), 1);
        assert_eq!(response["_truncated"], json!(true));
        assert_eq!(response["_omitted_hits"].as_u64(), Some(4));
    }

    /// A response with no hits has nothing to trim, which is what keeps the trim's advice and
    /// the zero-results advice from ever contending for `_warning`.
    #[test]
    fn an_empty_response_is_never_reported_as_trimmed() {
        let mut response = response_with(0);
        cap_response_bytes(&mut response, 1);
        assert!(response.get("_truncated").is_none());
        assert_eq!(response["hits_returned"].as_u64(), Some(0));
    }
}
