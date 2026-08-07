//! HTTP Server Implementation for CameoDB
//!
//! Provides REST API endpoints for distributed hybrid-search operations
//! using Axum web framework with streaming support.

use axum::{
    Json, Router,
    body::Body,
    extract::{DefaultBodyLimit, Path, Query, State},
    http::{HeaderName, HeaderValue, StatusCode, header},
    middleware::{Next, from_fn},
    response::{IntoResponse, Response},
    routing::{delete, get, patch, post, put},
};
use bytes::BytesMut;
use cameodb_mcp::{
    MCP_SESSION_ID_HEADER, McpBackend, McpIndexSearchRequest, McpShutdownHandle, mcp_router,
};
use futures::{StreamExt, future::BoxFuture, stream::FuturesUnordered};
use kameo::actor::ActorRef;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tower_http::{
    compression::CompressionLayer, cors::CorsLayer, decompression::DecompressionLayer,
    limit::RequestBodyLimitLayer, timeout::TimeoutLayer, trace::TraceLayer,
};

/// Liveness endpoint path. Exempt from the concurrency guard so that an overloaded node
/// still reports its real state instead of 503-ing its own health check.
const HEALTH_PATH: &str = "/_cluster/health";
use tracing::{error, info, warn};

use crate::cluster_coordinator::{ClusterCoordinator, GetStatus, OperationType};
use crate::node_orchestrator::{
    AdminIndexCommitReport, AdminIndexEvictWriterReport, AdminMemoryReport, ClientOp, DocPayload,
    RouterActor, WorkerPoolReport,
};
use storage::IndexSchema;

/// Application error wrapper for consistent error handling.
///
/// `status` short-circuits the string-sniffing classification below. Handlers
/// that already know the correct HTTP status should set it explicitly rather
/// than relying on the error text.
#[derive(Debug)]
pub struct AppError {
    pub error: anyhow::Error,
    pub status: Option<StatusCode>,
}

impl AppError {
    /// 400 with an explicit, client-safe message.
    pub fn bad_request(msg: impl Into<String>) -> Self {
        Self {
            error: anyhow::anyhow!("{}", msg.into()),
            status: Some(StatusCode::BAD_REQUEST),
        }
    }

    /// 404 with an explicit, client-safe message.
    pub fn not_found(msg: impl Into<String>) -> Self {
        Self {
            error: anyhow::anyhow!("{}", msg.into()),
            status: Some(StatusCode::NOT_FOUND),
        }
    }

    /// 413 with an explicit, client-safe message.
    pub fn payload_too_large(msg: impl Into<String>) -> Self {
        Self {
            error: anyhow::anyhow!("{}", msg.into()),
            status: Some(StatusCode::PAYLOAD_TOO_LARGE),
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let error_msg = self.error.to_string();

        // An explicit status wins; otherwise fall back to classifying the text.
        let (status, message) = if let Some(status) = self.status {
            (status, error_msg.as_str())
        } else if error_msg.contains("NotFound") || error_msg.contains("not found") {
            (StatusCode::NOT_FOUND, "Resource not found")
        } else if error_msg.contains("QueryParserError") || error_msg.contains("parse") {
            (StatusCode::BAD_REQUEST, "Invalid query format")
        } else {
            (StatusCode::INTERNAL_SERVER_ERROR, "Internal server error")
        };

        // Log at appropriate level: DEBUG for 404 (expected), WARN for client errors, ERROR for server errors
        match status {
            StatusCode::NOT_FOUND => {
                tracing::debug!("API: {} -> {}: {}", status, message, error_msg);
            }
            s if s.is_client_error() => {
                warn!("API Client Error: {} -> {}: {}", status, message, error_msg);
            }
            _ => {
                error!("API Server Error: {} -> {}: {}", status, message, error_msg);
            }
        }

        let body = serde_json::json!({
            "error": message,
            "details": error_msg
        });

        (status, Json(body)).into_response()
    }
}

/// Validate an index name for creation.
///
/// Rejects names that could escape the `shard_path/indices/` directory via path
/// traversal (`..`, `/`, `\`), empty names, names exceeding 255 bytes, and names
/// that don't start with an alphanumeric character.
fn validate_index_name(index: &str) -> Result<(), AppError> {
    if index.is_empty() {
        return Err(AppError::bad_request("index name must not be empty"));
    }
    if index.len() > 255 {
        return Err(AppError::bad_request(
            "index name must not exceed 255 characters",
        ));
    }
    if !index.starts_with(|c: char| c.is_ascii_alphanumeric()) {
        return Err(AppError::bad_request(
            "index name must start with an alphanumeric character",
        ));
    }
    if index.contains("..") {
        return Err(AppError::bad_request("index name must not contain '..'"));
    }
    if index.contains('/') || index.contains('\\') {
        return Err(AppError::bad_request(
            "index name must not contain path separators",
        ));
    }
    if !index
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.')
    {
        return Err(AppError::bad_request(
            "index name contains invalid characters (allowed: a-z, A-Z, 0-9, _, -, .)",
        ));
    }
    Ok(())
}

impl<E> From<E> for AppError
where
    E: Into<anyhow::Error>,
{
    fn from(err: E) -> Self {
        Self {
            error: err.into(),
            status: None,
        }
    }
}

/// Search request payload
#[derive(Debug, Deserialize)]
pub struct SearchPayload {
    pub query: String,
    pub limit: Option<usize>,
    /// Optional list of fields to return (field projection)
    pub fields: Option<Vec<String>>,
    /// Optional sort specification
    pub sort: Option<SortSpec>,
}

/// Schema update request payload for maintenance API
#[derive(Debug, Deserialize)]
pub struct SchemaUpdatePayload {
    /// Map of field_name -> indexed (true/false)
    pub field_updates: std::collections::HashMap<String, bool>,
}

// Re-export SortSpec and SortOrder from storage crate
pub use storage::{SortOrder, SortSpec};

/// Query parameters for the list indexes endpoint
#[derive(Debug, Deserialize, Default)]
pub struct ListIndexesQuery {
    /// Whether to include data size information (default: false)
    #[serde(default)]
    pub data_size: Option<bool>,
}

impl ListIndexesQuery {
    /// Helper to get the data_size flag with a default of false
    pub fn include_data_size(&self) -> bool {
        self.data_size.unwrap_or(false)
    }
}

/// Health check response
#[derive(Debug, Serialize, Deserialize)]
pub struct HealthResponse {
    pub status: String,
    pub node_id: String,
    pub node_name: String,

    // Cluster-wide status
    pub cluster_name: Option<String>,
    pub cluster_enabled: Option<bool>,
    pub total_nodes: Option<usize>,
    pub connected_nodes: Option<usize>,
    pub cluster_total_shards: Option<usize>,

    // Local node info
    pub active_shards: usize,
    pub total_indexes: usize,
    pub indexes_with_data: usize,

    // Performance/Debug metrics
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dial_failures: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bootstrap_successes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub routing_updates: Option<u64>,
}

/// Application state shared across handlers
#[derive(Clone)]
pub struct AppState {
    pub router: RouterActor,
    pub coordinator: ActorRef<ClusterCoordinator>,
    /// Number of documents per micro-batch for NDJSON write-stream ingestion
    pub stream_batch_size: usize,
    /// Largest accepted single record, in bytes (from `max_record_size_mb`).
    ///
    /// The NDJSON stream handler enforces this per line. The wire-level body limit bounds
    /// the request as a whole, but one unterminated line could still buffer the entire
    /// allowance in memory, so the per-record cap is what keeps peak memory bounded.
    pub max_record_size_bytes: usize,
}

impl McpBackend for AppState {
    fn search_index(
        &self,
        index: McpIndexSearchRequest,
        query: String,
        limit: Option<usize>,
    ) -> BoxFuture<'_, Result<JsonValue, String>> {
        let state = self.clone();
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
                    // Add zero-results warning if applicable
                    let hits_returned = response
                        .get("hits_returned")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0);

                    if hits_returned == 0
                        && (query.contains('\"') || query.contains("AND"))
                        && let Some(obj) = response.as_object_mut()
                    {
                        obj.insert(
                            "_warning".to_string(),
                            JsonValue::String(
                                "No exact matches found. Consider removing exact phrase quotes or using broader boolean OR logic.".to_string()
                            ),
                        );
                    }
                    Ok(response)
                }
                Err(err) => {
                    let err_str = err.to_string();

                    // Schema-aware error interceptor for field errors
                    if (err_str.contains("does not exist")
                        || err_str.contains("FieldDoesNotExist")
                        || err_str.contains("field")
                        || err_str.contains("unknown field"))
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
                        return Err(format!(
                            "Query failed: The query references a field that does not exist in the '{}' index. Valid fields are: [{}]. Please correct your query and try again.",
                            index_name,
                            field_names.join(", ")
                        ));
                    }

                    Err(err_str)
                }
            }
        })
    }

    fn search_indexes(
        &self,
        indexes: Vec<McpIndexSearchRequest>,
        query: String,
        limit: Option<usize>,
    ) -> BoxFuture<'_, Result<JsonValue, String>> {
        let state = self.clone();
        Box::pin(async move {
            let requested_limit = limit.unwrap_or(10);

            // Preprocess query to extract return/limit/sort modifiers (same as HTTP server)
            let (cleaned_query, parsed_limit, parsed_fields, parsed_sort) =
                parse_query_keywords(&query);

            // Merge MCP-provided limit with parsed limit
            let final_limit = limit.or(parsed_limit);

            // Determine the global sort spec (if any) for the final merge.
            // Per-index sort takes precedence; fall back to query-parsed sort.
            let global_sort: Option<storage::SortSpec> = indexes
                .iter()
                .find_map(|req| req.sort.as_ref())
                .map(|mcp_sort| storage::SortSpec {
                    field: mcp_sort.field.clone(),
                    order: match mcp_sort.order {
                        cameodb_mcp::server::SortOrder::Desc => storage::SortOrder::Desc,
                        cameodb_mcp::server::SortOrder::Asc => storage::SortOrder::Asc,
                    },
                })
                .or_else(|| {
                    parsed_sort.as_ref().map(|storage_sort| storage::SortSpec {
                        field: storage_sort.field.clone(),
                        order: storage_sort.order,
                    })
                });

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
                        parsed_sort.map(|storage_sort| cameodb_mcp::server::SortSpec {
                            field: storage_sort.field,
                            order: match storage_sort.order {
                                storage::SortOrder::Desc => cameodb_mcp::server::SortOrder::Desc,
                                storage::SortOrder::Asc => cameodb_mcp::server::SortOrder::Asc,
                            },
                        })
                    });

                    // Convert MCP SortSpec to storage SortSpec
                    let storage_sort = final_sort.map(|mcp_sort| storage::SortSpec {
                        field: mcp_sort.field,
                        order: match mcp_sort.order {
                            cameodb_mcp::server::SortOrder::Desc => storage::SortOrder::Desc,
                            cameodb_mcp::server::SortOrder::Asc => storage::SortOrder::Asc,
                        },
                    });

                    let result = state
                        .router
                        .route_and_handle(
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

            while let Some((index_name, result)) = search_futures.next().await {
                // Schema-aware error handling
                let result = match result {
                    Ok(r) => r,
                    Err(err) => {
                        let err_str = err.to_string();

                        if (err_str.contains("does not exist")
                            || err_str.contains("FieldDoesNotExist")
                            || err_str.contains("field")
                            || err_str.contains("unknown field"))
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
                            return Err(format!(
                                "Query failed in index '{}': The query references a field that does not exist. Valid fields are: [{}]. Please correct your query and try again.",
                                index_name,
                                field_names.join(", ")
                            ));
                        }

                        return Err(err_str);
                    }
                };

                total_hits += result
                    .get("total_hits")
                    .and_then(|value| value.as_u64())
                    .unwrap_or(0);

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

            // Add zero-results warning if applicable
            if hits_returned == 0
                && (query.contains('\"') || query.contains("AND"))
                && let Some(obj) = response.as_object_mut()
            {
                obj.insert(
                    "_warning".to_string(),
                    JsonValue::String(
                        "No exact matches found. Consider removing exact phrase quotes or using broader boolean OR logic.".to_string()
                    ),
                );
            }

            Ok(response)
        })
    }

    fn get_index(&self, index: String) -> BoxFuture<'_, Result<JsonValue, String>> {
        let state = self.clone();
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
            });

            Ok(enrich_index_entry(entry))
        })
    }

    fn list_indexes(&self) -> BoxFuture<'_, Result<JsonValue, String>> {
        let state = self.clone();
        Box::pin(async move {
            let listing = state
                .router
                .handle_client_op(ClientOp::ListIndexes {
                    include_data_size: false,
                })
                .await
                .map_err(|err| err.to_string())?;

            let indexes = listing
                .get("indexes")
                .and_then(|value| value.as_array())
                .cloned()
                .unwrap_or_default();

            let mut enriched = Vec::with_capacity(indexes.len());
            for stats in indexes {
                let index_name = stats
                    .get("name")
                    .and_then(|value| value.as_str())
                    .ok_or_else(|| "Index entry missing name".to_string())?
                    .to_string();

                let entry = serde_json::json!({
                    "index": index_name,
                    "stats": stats,
                });
                enriched.push(enrich_index_entry(entry));
            }

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

    fn validate_query(
        &self,
        index: Option<String>,
        partial_field: Option<String>,
        query: Option<String>,
    ) -> BoxFuture<'_, Result<JsonValue, String>> {
        let state = self.clone();
        Box::pin(async move {
            let index_details = if let Some(index_name) = index.clone() {
                Some(state.get_index(index_name).await?)
            } else {
                None
            };

            let field_infos = index_details
                .as_ref()
                .map(extract_field_info)
                .unwrap_or_default();

            let field_names: Vec<String> =
                field_infos.iter().map(|info| info.name.clone()).collect();

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
                            })
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();

            // Field-type-aware schema summary
            let fields_with_types: Vec<JsonValue> = field_infos
                .iter()
                .filter(|info| !info.is_shadow && info.name != "_seq")
                .map(|info| {
                    let hint = field_type_query_hint(&info.field_type);
                    serde_json::json!({
                        "field": info.name,
                        "type": info.field_type,
                        "indexed": info.indexed,
                        "queryable": info.indexed && !info.is_shadow,
                        "query_hint": hint,
                    })
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

    fn get_index_stats(&self, index: Option<String>) -> BoxFuture<'_, Result<JsonValue, String>> {
        let state = self.clone();
        Box::pin(async move {
            if let Some(index_name) = index {
                let details = state.get_index(index_name.clone()).await?;
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

            let listing = state.list_indexes().await?;
            let indexes = listing
                .get("indexes")
                .and_then(|value| value.as_array())
                .cloned()
                .unwrap_or_default();

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

    fn list_resources(&self) -> BoxFuture<'_, Result<JsonValue, String>> {
        let state = self.clone();
        Box::pin(async move {
            let listing = state.list_indexes().await?;
            let indexes = listing
                .get("indexes")
                .and_then(|value| value.as_array())
                .cloned()
                .unwrap_or_default();

            let mut resources = vec![resource_descriptor(
                "cameodb://indexes".to_string(),
                "CameoDB Index Catalog".to_string(),
                "All available CameoDB indexes with schema and metadata.".to_string(),
            )];

            for item in indexes {
                let index_name = item
                    .get("index")
                    .and_then(|value| value.as_str())
                    .ok_or_else(|| "Index entry missing index name".to_string())?
                    .to_string();

                resources.push(resource_descriptor(
                    format!("cameodb://indexes/{index_name}"),
                    format!("Index {index_name}"),
                    format!("Metadata resource for CameoDB index '{index_name}'."),
                ));
                resources.push(resource_descriptor(
                    format!("cameodb://indexes/{index_name}/schema"),
                    format!("Index {index_name} Schema"),
                    format!("Schema resource for CameoDB index '{index_name}'."),
                ));
                resources.push(resource_descriptor(
                    format!("cameodb://indexes/{index_name}/stats"),
                    format!("Index {index_name} Statistics"),
                    format!("Statistics resource for CameoDB index '{index_name}'."),
                ));
            }

            Ok(JsonValue::Array(resources))
        })
    }

    fn read_resource(&self, uri: String) -> BoxFuture<'_, Result<JsonValue, String>> {
        let state = self.clone();
        Box::pin(async move {
            if uri == "cameodb://indexes" {
                return state.list_indexes().await;
            }

            let resource = uri
                .strip_prefix("cameodb://indexes/")
                .ok_or_else(|| format!("Unsupported resource URI: {uri}"))?;

            if let Some(index_name) = resource.strip_suffix("/schema") {
                let details = state.get_index(index_name.to_string()).await?;
                return Ok(details.get("schema").cloned().unwrap_or(JsonValue::Null));
            }

            if let Some(index_name) = resource.strip_suffix("/stats") {
                return state.get_index_stats(Some(index_name.to_string())).await;
            }

            state.get_index(resource.to_string()).await
        })
    }
}

fn resource_descriptor(uri: String, name: String, description: String) -> JsonValue {
    serde_json::json!({
        "uri": uri,
        "name": name,
        "description": description,
        "mimeType": "application/json",
    })
}

#[derive(Debug, Clone)]
struct FieldInfo {
    name: String,
    field_type: String,
    indexed: bool,
    fast: bool,
    is_shadow: bool,
}

fn extract_field_info(value: &JsonValue) -> Vec<FieldInfo> {
    let fields_obj = value
        .get("schema")
        .and_then(|schema| schema.get("fields"))
        .and_then(|fields| fields.as_object())
        .or_else(|| value.get("fields").and_then(|fields| fields.as_object()));

    let Some(fields_obj) = fields_obj else {
        return Vec::new();
    };

    let mut infos: Vec<FieldInfo> = fields_obj
        .iter()
        .map(|(name, def)| FieldInfo {
            name: name.clone(),
            field_type: def
                .get("field_type")
                .and_then(|v| v.as_str())
                .unwrap_or("text")
                .to_string(),
            indexed: def.get("indexed").and_then(|v| v.as_bool()).unwrap_or(true),
            fast: def.get("fast").and_then(|v| v.as_bool()).unwrap_or(false),
            is_shadow: def
                .get("is_shadow")
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
        })
        .collect();

    infos.sort_by(
        |left, right| match (left.name.as_str(), right.name.as_str()) {
            ("id", "id") => std::cmp::Ordering::Equal,
            ("id", _) => std::cmp::Ordering::Less,
            (_, "id") => std::cmp::Ordering::Greater,
            _ => left.name.cmp(&right.name),
        },
    );

    infos
}

fn extract_field_names(value: &JsonValue) -> Vec<String> {
    extract_field_info(value)
        .into_iter()
        .map(|info| info.name)
        .collect()
}

/// Enrich a raw index entry (with schema) by adding compact field metadata
/// and a top-level `query_hints` section with unique hints per field type.
/// All indexed fields are searchable by default.
fn enrich_index_entry(mut entry: JsonValue) -> JsonValue {
    let field_infos = extract_field_info(&entry);

    // Collect unique field types present in this index
    let mut unique_types: std::collections::HashSet<String> = std::collections::HashSet::new();
    for info in &field_infos {
        if info.name != "_seq" {
            unique_types.insert(info.field_type.clone());
        }
    }

    // Build query hints for each unique field type
    let query_hints: Vec<JsonValue> = unique_types
        .iter()
        .map(|field_type| {
            serde_json::json!({
                "type": field_type,
                "query_hint": field_type_query_hint(field_type),
            })
        })
        .collect();

    // Build compact field list (field, type, indexed, fast)
    let fields: Vec<JsonValue> = field_infos
        .iter()
        .filter(|info| info.name != "_seq")
        .map(|info| {
            serde_json::json!({
                "field": info.name,
                "type": info.field_type,
                "indexed": info.indexed,
                "fast": info.fast,
            })
        })
        .collect();

    if let Some(obj) = entry.as_object_mut() {
        obj.insert("fields".to_string(), JsonValue::Array(fields));
        obj.insert("query_hints".to_string(), JsonValue::Array(query_hints));
    }

    entry
}

fn field_type_query_hint(field_type: &str) -> &'static str {
    match field_type {
        "text" => {
            "Tokenized full-text. Supports: field:term, field:\"phrase\", field:\"phrase\"~N (slop/proximity), \"prefix phr\"* (prefix match), field: IN [a b c] (set), field:term^2.0 (boost), field:[a TO z] (lexicographic range), +field:term (must), -field:term (must-not)."
        }
        "string" | "exact" => {
            "Exact match (no tokenization). Supports: field:exact_value, field: IN [val1 val2] (set), +/- (must/must-not). No phrase or slop queries."
        }
        "i64" | "u64" | "f64" => {
            "Numeric field. Supports: field:value (exact), field:>value, field:<value, field:>=value, field:<=value (comparisons), field:[low TO high] (inclusive range), field:{low TO high} (exclusive range), field:[low TO *] or field:[* TO high] (unbounded), field:value^2.0 (boost), +/- (must/must-not). No phrase or IN set queries."
        }
        "date" => {
            "Date field (YYYY-MM-DD or RFC3339). Supports: field:2024-01-15, field:>2024-01-01, field:<2024-12-31, field:>=date, field:<=date, field:[start TO end] (inclusive), field:{start TO end} (exclusive), +/- (must/must-not). No phrase or IN set queries."
        }
        "boolean" => {
            "Boolean field. Supports: field:true, field:false, +/- (must/must-not). No range, phrase, or boost queries."
        }
        "ip" => {
            "IP address field (IPv4/IPv6). Supports: field:192.168.1.1 (exact), field:[192.168.0.0 TO 192.168.255.255] (range), +/- (must/must-not). No phrase or text queries."
        }
        "json" => {
            "Nested JSON object. Use dot notation: field.subfield:value, field.nested.deep:value. Escape literal dots in keys: field\\.name:value. Supports +/- (must/must-not)."
        }
        "facet" => {
            "Hierarchical category. Use path syntax: field:/path/to/category. Supports +/- (must/must-not). No range or phrase queries."
        }
        _ => "Use field:value syntax. Check field type for supported operators.",
    }
}

fn analyze_query(query_text: &str, field_infos: &[FieldInfo]) -> JsonValue {
    let mut warnings: Vec<String> = Vec::new();
    let mut suggestions: Vec<String> = Vec::new();

    // Structural checks
    let quote_count = query_text.chars().filter(|ch| *ch == '"').count();
    if quote_count % 2 != 0 {
        warnings.push(
            "Unbalanced quotes detected. Phrase queries require matching double quotes."
                .to_string(),
        );
    }

    let open_parens = query_text.chars().filter(|ch| *ch == '(').count();
    let close_parens = query_text.chars().filter(|ch| *ch == ')').count();
    if open_parens != close_parens {
        warnings.push(format!(
            "Unbalanced parentheses: {} opening vs {} closing.",
            open_parens, close_parens
        ));
    }

    // Check for inline modifiers (return/limit)
    let parts: Vec<&str> = query_text.split_whitespace().collect();
    let has_return = parts
        .iter()
        .any(|token| token.eq_ignore_ascii_case("return"));
    let has_limit = parts
        .iter()
        .any(|token| token.eq_ignore_ascii_case("limit"));

    if has_return {
        suggestions.push("Query uses inline 'return' for field projection. You can also pass fields via the tool's 'fields' parameter.".to_string());
    }
    if has_limit {
        suggestions.push(
            "Query uses inline 'limit'. You can also pass limit via the tool's 'limit' parameter."
                .to_string(),
        );
    }

    // Extract field references (handle phrases and parens gracefully)
    let referenced_fields = extract_query_fields(query_text);

    let indexed_names: Vec<&str> = field_infos
        .iter()
        .filter(|info| info.indexed && !info.is_shadow)
        .map(|info| info.name.as_str())
        .collect();

    let all_names: Vec<&str> = field_infos.iter().map(|info| info.name.as_str()).collect();

    let mut recognized = Vec::new();
    let mut unknown = Vec::new();
    let mut not_indexed = Vec::new();
    let mut field_hints = Vec::new();

    for field_name in &referenced_fields {
        if indexed_names.contains(&field_name.as_str()) {
            recognized.push(field_name.clone());
            if let Some(info) = field_infos.iter().find(|i| i.name == *field_name) {
                field_hints.push(serde_json::json!({
                    "field": field_name,
                    "type": info.field_type,
                    "hint": field_type_query_hint(&info.field_type),
                }));
            }
        } else if all_names.contains(&field_name.as_str()) {
            not_indexed.push(field_name.clone());
            warnings.push(format!(
                "Field '{}' exists but is not indexed. Queries against it will not match.",
                field_name
            ));
        } else {
            unknown.push(field_name.clone());
        }
    }

    if !unknown.is_empty() && !all_names.is_empty() {
        for unk in &unknown {
            let unk_lower = unk.to_lowercase();
            let close_matches: Vec<&str> = indexed_names
                .iter()
                .filter(|known| {
                    let known_lower = known.to_lowercase();
                    known_lower.starts_with(&unk_lower)
                        || unk_lower.starts_with(&known_lower)
                        || known_lower.contains(&unk_lower)
                        || unk_lower.contains(&known_lower)
                })
                .copied()
                .collect();
            if !close_matches.is_empty() {
                suggestions.push(format!(
                    "Unknown field '{}'. Did you mean: {}?",
                    unk,
                    close_matches.join(", ")
                ));
            } else {
                warnings.push(format!(
                    "Unknown field '{}'. Available indexed fields: {}.",
                    unk,
                    indexed_names.join(", ")
                ));
            }
        }
    }

    serde_json::json!({
        "query": query_text,
        "recognized_fields": recognized,
        "unknown_fields": unknown,
        "not_indexed_fields": not_indexed,
        "field_hints": field_hints,
        "warnings": warnings,
        "suggestions": suggestions,
    })
}

fn extract_query_fields(query: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let reserved = ["AND", "OR", "NOT", "TO", "return", "limit"];
    let chars: Vec<char> = query.chars().collect();
    let len = chars.len();
    let mut i = 0;

    while i < len {
        match chars[i] {
            '"' => {
                // Skip quoted strings
                i += 1;
                while i < len && chars[i] != '"' {
                    i += 1;
                }
                i += 1;
            }
            '(' | ')' | '[' | ']' => {
                i += 1;
            }
            _ if chars[i].is_alphanumeric() || chars[i] == '_' => {
                let start = i;
                while i < len && (chars[i].is_alphanumeric() || chars[i] == '_' || chars[i] == '.')
                {
                    i += 1;
                }
                let token = &query[start..i];

                // Check if followed by ':' (field reference)
                if i < len && chars[i] == ':' {
                    if !reserved.iter().any(|kw| kw.eq_ignore_ascii_case(token))
                        && !fields.contains(&token.to_string())
                    {
                        fields.push(token.to_string());
                    }
                    i += 1; // skip the colon
                }
            }
            _ => {
                i += 1;
            }
        }
    }

    fields
}

fn cameodb_syntax_reference() -> JsonValue {
    serde_json::json!({
        "basic_search": {
            "description": "Search across all default indexed text fields. Multiple terms are combined with AND by default.",
            "syntax": "<term> [<term> ...]",
            "examples": ["rust database", "machine learning"],
            "note": "Terms are tokenized. 'rust database' matches documents containing both 'rust' AND 'database' in any default text field."
        },
        "field_targeted": {
            "description": "Target a specific field. Only applies to the term immediately following the colon.",
            "syntax": "field:term",
            "examples": ["title:rust", "author:doe", "body:rust programming"],
            "note": "In 'body:rust programming', only 'rust' targets body; 'programming' searches default fields."
        },
        "phrase_query": {
            "description": "Match an exact phrase (terms in order). Requires field to have positional indexing.",
            "syntax": "field:\"term1 term2\"",
            "examples": [
                "title:\"rust programming\"",
                "description:\"machine learning\"",
                "title:\"Barack Obama\""
            ]
        },
        "phrase_slop": {
            "description": "Phrase query with slop (proximity). Allows up to N extra words between phrase terms. Transposition costs 2.",
            "syntax": "field:\"term1 term2\"~N",
            "examples": [
                "body:\"small bike\"~1",
                "body:\"small bike\"~3",
                "title:\"big wolf\"~1"
            ],
            "note": "\"small bike\"~1 matches 'small blue bike'. \"A B\"~1 does NOT match 'B A' (transposition costs 2, use ~2)."
        },
        "phrase_prefix": {
            "description": "Phrase query where the last term is treated as a prefix. Useful for autocomplete-style matching. No slop allowed.",
            "syntax": "\"term1 partial\"*",
            "examples": [
                "\"big bad wo\"*",
                "\"rust prog\"*"
            ],
            "note": "\"big bad wo\"* matches 'big bad wolf'. The * prefix operator only applies to the last term in the phrase."
        },
        "term_prefix": {
            "description": "Matches documents where the targeted field contains a token that starts with the provided value.",
            "syntax": "field:prefix*",
            "examples": [
                "title:quick*",
                "author:smi*"
            ],
            "note": "title:quick* matches 'quickwit' or 'quickstart', but not 'qui'."
        },
        "boolean_operators": {
            "description": "Combine conditions with AND, OR, NOT (must be UPPERCASE). AND takes precedence over OR.",
            "syntax": "expr AND expr | expr OR expr | NOT expr",
            "examples": [
                "title:rust AND author:doe",
                "title:rust OR title:go",
                "title:rust NOT author:smith",
                "a AND b OR c  (parsed as: (a AND b) OR c)"
            ]
        },
        "must_must_not": {
            "description": "Prefix a term with + (required) or - (excluded). Equivalent to boolean operators but more concise.",
            "syntax": "+term (must match) | -term (must not match)",
            "examples": [
                "+rust +database",
                "apple -fruit",
                "+title:rust -author:smith",
                "(+title:rust +year:[2020 TO 2024]) author:doe"
            ],
            "note": "'+x +y' is equivalent to 'x AND y'. '(+x y)' means x is required, y is optional but boosts score."
        },
        "grouping": {
            "description": "Use parentheses to group sub-expressions and control operator precedence.",
            "syntax": "(expr)",
            "examples": [
                "(title:rust OR title:go) AND year:[2020 TO 2024]",
                "(color:red OR color:green) AND size:large",
                "(+title:rust +author:doe) OR title:\"systems programming\""
            ]
        },
        "exists_query": {
            "description": "Matches documents where the specified field is set (has any value).",
            "syntax": "field:*",
            "examples": [
                "author:*",
                "published_date:*"
            ],
            "note": "You must specify a field. '*' alone is the match-all query, not an exists query."
        },
        "range_queries": {
            "description": "Match values in a range or use comparison operators (>, <, >=, <=). [] = inclusive, {} = exclusive. Use * for unbounded side.",
            "syntax": "field:[low TO high] | field:{low TO high} | field:>value",
            "examples": [
                "year:[2020 TO 2024]",
                "price:[10.0 TO *]",
                "age:>=18",
                "score:<100",
                "title:[a TO c}"
            ],
            "note": "[] is inclusive, {} is exclusive. 'title:[a TO c}' matches a,b but not c. Works on numeric, date, and text fields."
        },
        "set_operator": {
            "description": "Match a field against a set of literal values. More CPU-efficient than chaining OR for many terms.",
            "syntax": "field: IN [val1 val2 val3]",
            "examples": [
                "status: IN [active pending review]",
                "color: IN [red green blue]",
                "category: IN [rust go python]"
            ],
            "note": "Must specify field. 'title: IN [a b c]' is more efficient than 'title:a OR title:b OR title:c'."
        },
        "boosting": {
            "description": "Boost a term's relevance weight with ^factor. Higher boost = more influence on ranking. No negative boosts.",
            "syntax": "term^factor | field:term^factor | \"phrase\"^factor",
            "examples": [
                "\"SRE\"^2.0 OR devops^0.4",
                "title:rust^3 OR body:rust",
                "title:\"machine learning\"^2.5 OR description:\"deep learning\""
            ],
            "note": "Default boost is 1.0. Boost only affects ranking, not filtering."
        },
        "all_docs_query": {
            "description": "Match all documents in the index. Useful as a base for filtering or as a wildcard query.",
            "syntax": "*",
            "examples": ["*", "* limit 10"],
            "note": "Returns all documents. Combine with inline modifiers for controlled result sets."
        },
        "date_queries": {
            "description": "Query date fields. Accepts YYYY-MM-DD or full RFC3339 (e.g. 2024-01-15T10:30:00Z). Supports comparisons and ranges.",
            "syntax": "field:YYYY-MM-DD | field:>YYYY-MM-DD | field:[start TO end]",
            "examples": [
                "created_at:2024-01-15",
                "created_at:>2024-01-01",
                "created_at:<2024-12-31",
                "created_at:[2024-01-01 TO 2024-12-31]",
                "timestamp:[2024-01-01T00:00:00Z TO 2024-01-02T00:00:00Z}"
            ],
            "note": "Dates are internally stored as RFC3339. YYYY-MM-DD is auto-normalized. Exclusive bound {} works on dates too."
        },
        "exact_id_lookup": {
            "description": "Direct document lookup by ID field (exact match, no tokenization).",
            "syntax": "id:value",
            "examples": ["id:my-document-id", "id:doc-12345"]
        },
        "escape_characters": {
            "description": "Special characters must be escaped with backslash (\\) when used literally in query terms.",
            "reserved_characters": "+ ^ ` : { } \" [ ] ( ) ~ ! \\ * SPACE",
            "examples": [
                "title:C\\+\\+",
                "name:O\\'Brien",
                "field:hello\\ world"
            ],
            "note": "Backslash escapes a single special character. Inside phrase queries (double quotes), only \\\" needs escaping."
        },
        "field_name_rules": {
            "description": "Rules for valid field names and how to handle special characters in them.",
            "rules": [
                "Must be 1-255 characters long",
                "Cannot start with a dot or digit",
                "Allowed characters: a-z, A-Z, 0-9, ., -, _, /, @, $",
                "Reserved names (_source, _dynamic, _field_presence) cannot be used"
            ],
            "dot_escaping": "If a field name literally contains a dot (e.g., 'k8s.node'), you MUST escape it (k8s\\.node) so it isn't treated as JSON nested object access.",
            "examples": [
                "k8s\\.component\\.name:quickwit",
                "@timestamp:>2024-01-01"
            ]
        },
        "inline_modifiers": {
            "description": "CameoDB-specific query modifiers appended to the query string.",
            "return_fields": {
                "syntax": "return field1,field2",
                "description": "Project only specific fields in results.",
                "example": "title:rust return title,author,year"
            },
            "limit_results": {
                "syntax": "limit N",
                "description": "Limit the number of results returned.",
                "example": "title:rust limit 5"
            },
            "sort_results": {
                "syntax": "sort field:order",
                "description": "Sort results by a FAST field or text/string field. Order is optional (asc or desc, defaults to asc).",
                "examples": [
                    "title:rust sort year:desc",
                    "title:rust sort timestamp:asc",
                    "title:rust sort price"
                ],
                "supported_types": "u64, i64, f64, date (FAST), text, string (post-fetch)",
                "note": "Date fields use timestamp ordering. Numeric FAST fields sort natively. Text/string fields sort alphabetically post-fetch."
            },
            "combined": {
                "example": "title:rust AND author:doe return title,author limit 10 sort year:desc"
            }
        },
        "field_types_and_operators": {
            "description": "Operator compatibility depends on field type. This matrix shows which operators work with which types.",
            "text": {
                "type_description": "Tokenized full-text. Terms are split and lowercased.",
                "supported_operators": ["field:term", "field:prefix*", "field:\"phrase\"", "field:\"phrase\"~N (slop)", "\"phrase\"* (prefix)", "field:* (exists)", "AND/OR/NOT", "+/- (must/must-not)", "field: IN [a b c]", "field:term^boost", "field:[a TO z] (lexicographic range)"],
                "not_supported": ["Numeric comparisons (>, <)"]
            },
            "string_exact": {
                "type_description": "Raw exact match, no tokenization. Value must match exactly as stored.",
                "supported_operators": ["field:exact_value", "field:prefix*", "field:* (exists)", "field: IN [val1 val2]", "AND/OR/NOT", "+/-"],
                "not_supported": ["Phrase queries (no tokenization)", "Slop (~)"]
            },
            "numeric_i64_u64_f64": {
                "type_description": "Numeric values. Stored as 64-bit integers or floats.",
                "supported_operators": ["field:value (exact)", "field:>value (comparisons)", "field:[low TO high] (range, inclusive)", "field:{low TO high} (range, exclusive)", "field:[low TO *] (unbounded)", "field:* (exists)", "AND/OR/NOT", "+/-", "field:value^boost"],
                "not_supported": ["Phrase queries", "Slop (~)", "IN set operator"]
            },
            "date": {
                "type_description": "Date/datetime. Accepts YYYY-MM-DD or RFC3339. Auto-normalized internally.",
                "supported_operators": ["field:2024-01-15 (exact date)", "field:>2024-01-01 (after)", "field:<2024-12-31 (before)", "field:>=2024-01-01", "field:<=2024-12-31", "field:[2024-01-01 TO 2024-12-31] (inclusive range)", "field:{2024-01-01 TO 2024-12-31} (exclusive range)", "field:* (exists)", "AND/OR/NOT", "+/-"],
                "not_supported": ["Phrase queries", "IN set operator", "Slop (~)"]
            },
            "boolean": {
                "type_description": "Boolean true/false values.",
                "supported_operators": ["field:true", "field:false", "field:* (exists)", "AND/OR/NOT", "+/-"],
                "not_supported": ["Range queries", "Phrase queries", "Boosting"]
            },
            "ip": {
                "type_description": "IPv4 or IPv6 address. Use same format as indexed.",
                "supported_operators": ["field:192.168.1.1 (exact)", "field:[192.168.0.0 TO 192.168.255.255] (range)", "field:* (exists)", "AND/OR/NOT", "+/-"],
                "not_supported": ["Phrase queries", "Slop (~)", "Text search", "CIDR notation (use ranges instead)"]
            },
            "json": {
                "type_description": "Nested JSON object. Access subfields with dot notation.",
                "supported_operators": ["field.subfield:value", "field.nested.deep:value", "field.nested:* (exists)", "AND/OR/NOT", "+/-"],
                "note": "If keys contain dots, escape them with backslash to avoid ambiguity: k8s\\.component\\.name:value"
            },
            "facet": {
                "type_description": "Hierarchical categories with path-based structure.",
                "supported_operators": ["field:/path/to/category", "AND/OR/NOT", "+/-"],
                "not_supported": ["Range queries", "Phrase queries"]
            }
        }
    })
}

/// Creates the main HTTP router with all endpoints and middleware
///
/// # Arguments
/// * `state` - Application state with actor references
/// * `max_body_size_mb` - Maximum request body size in MB (from config)
/// * `cors_allowed_origins` - List of allowed CORS origins (from config)
/// * `max_concurrent_requests` - Maximum concurrent in-flight HTTP requests
pub fn create_router(
    state: AppState,
    max_body_size_mb: usize,
    cors_allowed_origins: &[String],
    max_concurrent_requests: usize,
    request_timeout_secs: u64,
    admin_enabled: bool,
) -> (Router, McpShutdownHandle) {
    let body_limit_bytes = max_body_size_mb * 1024 * 1024;
    let (mcp_routes, mcp_handle) = mcp_router::<AppState>();

    // Concurrency limiter: rejects with 503 when too many requests are in flight.
    //
    // The liveness endpoint is exempt. Sharing the semaphore with it meant a node under
    // load answered its own health check with 503, so a load balancer would evict a node
    // that was merely busy — turning local overload into a cluster-wide outage.
    let semaphore = Arc::new(Semaphore::new(max_concurrent_requests));
    let concurrency_guard = from_fn(move |req: axum::extract::Request, next: Next| {
        let sem = semaphore.clone();
        async move {
            if req.uri().path() == HEALTH_PATH {
                return next.run(req).await;
            }
            match sem.try_acquire() {
                Ok(_permit) => next.run(req).await,
                Err(_) => (
                    StatusCode::SERVICE_UNAVAILABLE,
                    // Without this, clients retry immediately and deepen the overload.
                    [(header::RETRY_AFTER, "1")],
                    "Too many concurrent requests",
                )
                    .into_response(),
            }
        }
    });

    // Build the CORS layer from config. `CameoDbConfig::validate` has already
    // rejected empty lists, "*" mixed with specific origins, and origins that
    // are not valid header values, so the parse below cannot silently drop an
    // entry and leave a deny-all policy behind.
    let cors_layer = if cors_allowed_origins.iter().any(|o| o == "*") {
        warn!("CORS: allowing any origin (cors_allowed_origins = [\"*\"])");
        CorsLayer::permissive()
    } else {
        let origins: Vec<HeaderValue> = cors_allowed_origins
            .iter()
            .filter_map(|o| o.parse::<HeaderValue>().ok())
            .collect();
        info!(origins = ?cors_allowed_origins, "CORS: restricting to configured origins");
        CorsLayer::new()
            .allow_origin(origins)
            .allow_methods([
                axum::http::Method::GET,
                axum::http::Method::POST,
                axum::http::Method::PUT,
                axum::http::Method::PATCH,
                axum::http::Method::DELETE,
            ])
            // `mcp-session-id` is required by the MCP Streamable HTTP transport: the
            // client sends it on every follow-up request and must be able to read it off
            // the initialize response, so it has to be both allowed and exposed. Without
            // that, restricting origins silently breaks every browser-based MCP client.
            .allow_headers([
                header::CONTENT_TYPE,
                header::AUTHORIZATION,
                header::ACCEPT,
                HeaderName::from_static(MCP_SESSION_ID_HEADER),
            ])
            .expose_headers([HeaderName::from_static(MCP_SESSION_ID_HEADER)])
    };

    let router = Router::new()
        .nest("/mcp", mcp_routes)
        // API routes
        .route("/api/{index}/search", post(search_handler))
        .route("/api/{index}/search/stream", post(search_stream_handler))
        .route("/api/{index}/document", put(write_handler))
        .route("/api/{index}/document/stream", post(write_stream_handler))
        .route("/api/{index}/_bulk", post(bulk_write_handler))
        .route("/api/{index}/_config", put(create_config_handler))
        .route("/api/{index}/_config", get(get_config_handler))
        // Schema maintenance
        .route("/api/{index}/_schema", patch(update_schema_handler))
        // Index management
        .route("/api/{index}", delete(delete_index_handler))
        .route("/_indexes", get(list_indexes_handler))
        .route("/_cluster/_indexes", get(list_cluster_indexes_handler))
        // Health check
        .route(HEALTH_PATH, get(health_handler))
        .fallback(fallback_handler);

    // Admin endpoints are mounted only when enabled, so a disabled admin API is absent
    // rather than merely refusing — nothing to probe, nothing to accidentally re-enable
    // with a misconfigured guard.
    let router = if admin_enabled {
        router
            .route("/_admin/memory", get(admin_memory_handler))
            .route("/_admin/memory/purge", post(admin_memory_purge_handler))
            .route("/_admin/workers", get(admin_workers_handler))
            .route(
                "/_admin/index/{index}/commit",
                post(admin_index_commit_handler),
            )
            .route(
                "/_admin/index/{index}/evict-writer",
                post(admin_index_evict_writer_handler),
            )
    } else {
        info!("Admin API disabled: /_admin/* routes are not mounted");
        router
    };

    let router = router
        .with_state(state)
        // Response compression (outermost for responses)
        .layer(CompressionLayer::new())
        // Decompressed-size limit for body *extractors* (Json/Bytes/String). Applied
        // after DecompressionLayer so a compression bomb is measured expanded, not
        // compressed. Note this is an extractor-level limit only — it does not count
        // bytes off the socket, which is what RequestBodyLimitLayer below does.
        .layer(DefaultBodyLimit::max(body_limit_bytes))
        // Allow compressed requests — decompresses before the body limit above
        .layer(DecompressionLayer::new())
        // Wire-level limit: counts bytes as they arrive and returns 413 once the cap is
        // passed, regardless of how the handler consumes the body. This is the only guard
        // that covers handlers taking a raw `Body` (the streaming ingest path), which
        // `DefaultBodyLimit` never applied to.
        .layer(RequestBodyLimitLayer::new(body_limit_bytes))
        // Concurrency guard — reject excess requests with 503
        .layer(concurrency_guard)
        // Bound how long a single request may occupy a concurrency permit. Without this,
        // `max_concurrent_requests` trickle-fed connections hold every permit forever and
        // the limit meant to prevent a DoS becomes the mechanism for one.
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            Duration::from_secs(request_timeout_secs),
        ))
        .layer(cors_layer)
        .layer(TraceLayer::new_for_http());

    (router, mcp_handle)
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

/// Parse query string for 'limit <n>', 'return <field1,field2,...>', and 'sort field:order' keywords.
/// Returns (cleaned_query, extracted_limit, extracted_fields, extracted_sort).
///
/// OPTIMIZED: Single-pass parsing with minimal allocations.
fn parse_query_keywords(
    query: &str,
) -> (String, Option<usize>, Option<Vec<String>>, Option<SortSpec>) {
    // Early return for empty queries
    if query.is_empty() {
        return (String::new(), None, None, None);
    }

    let mut limit = None;
    let mut fields = None;
    let mut sort = None;

    // Track keyword positions and parse state in a single pass
    let mut return_idx = None;
    let mut limit_idx = None;
    let mut sort_idx = None;
    let mut return_parsed = false;
    let mut limit_parsed = false;
    let mut sort_parsed = false;

    // Single pass to find all keywords and count parts
    let mut part_count = 0;
    for (idx, part) in query.split_whitespace().enumerate() {
        part_count = idx + 1;
        match part {
            "return" if return_idx.is_none() => return_idx = Some(idx),
            "limit" if limit_idx.is_none() => limit_idx = Some(idx),
            "sort" if sort_idx.is_none() => sort_idx = Some(idx),
            _ => {}
        }
    }

    // If no keywords found, return original query
    if return_idx.is_none() && limit_idx.is_none() && sort_idx.is_none() {
        return (query.to_string(), None, None, None);
    }

    // Parse 'return' keyword
    if let Some(ret_idx) = return_idx {
        // Determine where field list ends
        let field_end_idx = [limit_idx, sort_idx]
            .iter()
            .filter_map(|&idx| idx.filter(|&i| i > ret_idx))
            .min()
            .unwrap_or(part_count);

        // Extract field tokens without intermediate join
        let field_tokens: Vec<&str> = query
            .split_whitespace()
            .skip(ret_idx + 1)
            .take(field_end_idx - ret_idx - 1)
            .collect();

        if !field_tokens.is_empty() {
            // Join and parse comma-separated fields
            let field_str = field_tokens.join(" ");
            let field_list: Vec<String> = field_str
                .split(',')
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
                .collect();

            if !field_list.is_empty() {
                fields = Some(field_list);
                return_parsed = true;
            }
        }
    }

    // Parse 'limit' keyword
    if let Some(lim_idx) = limit_idx
        && let Some(limit_str) = query.split_whitespace().nth(lim_idx + 1)
        && let Ok(n) = limit_str.parse::<usize>()
    {
        limit = Some(n);
        limit_parsed = true;
    }

    // Parse 'sort' keyword
    if let Some(s_idx) = sort_idx
        && let Some(sort_spec) = query.split_whitespace().nth(s_idx + 1)
    {
        if let Some((field, order_str)) = sort_spec.split_once(':') {
            let order = match order_str.to_lowercase().as_str() {
                "desc" => SortOrder::Desc,
                _ => SortOrder::Asc,
            };
            sort = Some(SortSpec {
                field: field.to_string(),
                order,
            });
            sort_parsed = true;
        } else {
            // No order specified, default to asc
            sort = Some(SortSpec {
                field: sort_spec.to_string(),
                order: SortOrder::Asc,
            });
            sort_parsed = true;
        }
    }

    // Determine where the query ends (first successfully parsed keyword)
    let query_end_idx = [
        return_idx.filter(|_| return_parsed),
        limit_idx.filter(|_| limit_parsed),
        sort_idx.filter(|_| sort_parsed),
    ]
    .iter()
    .filter_map(|&idx| idx)
    .min()
    .unwrap_or(part_count);

    // Extract cleaned query using iterator (avoid collecting full Vec)
    let cleaned_query = query
        .split_whitespace()
        .take(query_end_idx)
        .collect::<Vec<_>>()
        .join(" ");

    (cleaned_query, limit, fields, sort)
}

/// Handler for standard search operations
async fn search_handler(
    Path(index): Path<String>,
    State(state): State<AppState>,
    Json(payload): Json<SearchPayload>,
) -> Result<Json<JsonValue>, AppError> {
    // Parse query string for embedded limit/return/sort keywords
    let (cleaned_query, parsed_limit, parsed_fields, parsed_sort) =
        parse_query_keywords(&payload.query);

    // Explicit payload fields override parsed values
    let final_limit = payload.limit.or(parsed_limit);
    let final_fields = payload.fields.or(parsed_fields);
    let final_sort = payload.sort.or(parsed_sort);

    info!(
        "Search request - index: {}, query: {}, limit: {:?}, fields: {:?}",
        index, cleaned_query, final_limit, final_fields
    );

    let client_op = ClientOp::Search {
        index,
        query: cleaned_query,
        limit: final_limit,
        fields: final_fields,
        sort: final_sort,
    };

    let result = state
        .router
        .route_and_handle(client_op, None, OperationType::Read)
        .await?;
    Ok(Json(result))
}

/// Handler for listing all indexes across the cluster
async fn list_cluster_indexes_handler(
    State(state): State<AppState>,
    Query(params): Query<ListIndexesQuery>,
) -> Result<Json<JsonValue>, AppError> {
    info!("List cluster indexes request");

    let client_op = ClientOp::ListClusterIndexes {
        include_data_size: params.include_data_size(),
    };

    let result = state
        .router
        .route_and_handle(client_op, None, OperationType::Read)
        .await?;
    Ok(Json(result))
}

/// Handler for streaming search operations
///
/// Uses `route_and_handle_stream` to obtain a bounded `mpsc::Receiver` that
/// yields individual NDJSON lines (one per hit, plus a `_footer` metadata line).
/// The receiver is wrapped into an axum `Body` stream so the HTTP response
/// starts as soon as the first hit is ready, and each subsequent hit is flushed
/// incrementally. This avoids buffering the entire result set in memory.
async fn search_stream_handler(
    Path(index): Path<String>,
    State(state): State<AppState>,
    Json(payload): Json<SearchPayload>,
) -> Result<Response, AppError> {
    // Parse query string for embedded limit/return/sort keywords
    let (cleaned_query, parsed_limit, parsed_fields, parsed_sort) =
        parse_query_keywords(&payload.query);

    // Explicit payload fields override parsed values
    let final_limit = payload.limit.or(parsed_limit);
    let final_fields = payload.fields.or(parsed_fields);
    let final_sort = payload.sort.or(parsed_sort);

    info!(
        "Stream request - index: {}, query: {}, limit: {:?}, fields: {:?}",
        index, cleaned_query, final_limit, final_fields
    );

    let client_op = ClientOp::Stream {
        index,
        query: cleaned_query,
        limit: final_limit,
        fields: final_fields,
        sort: final_sort,
    };

    // Obtain a streaming channel — the search runs in a background task
    let rx = state
        .router
        .route_and_handle_stream(client_op, None, OperationType::Read);

    // Wrap the receiver into a Stream that axum can serve as a response body
    let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
    let body = Body::from_stream(stream);

    let mut resp = Response::new(body);
    resp.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/x-ndjson"),
    );
    Ok(resp)
}

/// Handler for document write operations
async fn write_handler(
    Path(index): Path<String>,
    State(state): State<AppState>,
    Json(payload): Json<DocPayload>,
) -> Result<Json<JsonValue>, AppError> {
    info!("Write request - index: {}, doc_id: {}", index, payload.id);

    let DocPayload {
        id,
        routing_key,
        doc,
    } = payload;

    // Optimization: Default routing_key to doc_id if not present to ensure
    // Unicast routing instead of Broadcast (Scatter-Gather).
    let effective_routing_key = routing_key.or_else(|| Some(id.clone()));

    let client_op = ClientOp::Write {
        index,
        id,
        routing_key: effective_routing_key.clone(),
        doc,
    };

    let result = state
        .router
        .route_and_handle(client_op, effective_routing_key, OperationType::Write)
        .await?;
    Ok(Json(result))
}

/// Handler for bulk write operations
async fn bulk_write_handler(
    Path(index): Path<String>,
    State(state): State<AppState>,
    Json(docs): Json<Vec<DocPayload>>,
) -> Result<Json<JsonValue>, AppError> {
    info!(
        "Bulk write request - index: {}, docs: {}",
        index,
        docs.len()
    );

    // Derive a routing hint from the first document to avoid cluster-wide broadcast:
    // prefer explicit routing_key, then id, then a deterministic hash of the document.
    let routing_hint = docs.first().and_then(|doc| {
        doc.routing_key.clone().or_else(|| {
            if !doc.id.is_empty() {
                Some(doc.id.clone())
            } else {
                // Fallback: hash the document bytes to keep routing stable
                serde_json::to_vec(&doc.doc)
                    .ok()
                    .map(|bytes| format!("{:016x}", xxhash_rust::xxh3::xxh3_64(&bytes)))
            }
        })
    });

    let client_op = ClientOp::BulkWrite { index, docs };

    let result = state
        .router
        .route_and_handle(client_op, routing_hint, OperationType::Write)
        .await?;

    // Debug: Log response size to identify potential serialization issues
    let response_str = serde_json::to_string(&result).unwrap_or_default();
    let response_size = response_str.len();
    info!(
        "Bulk write completed - response size: {} bytes, keys: {}",
        response_size,
        result.as_object().map(|o| o.keys().count()).unwrap_or(0)
    );

    // If response is very large, this could cause HTTP/2 issues
    if response_size > 1_000_000 {
        warn!(
            "Large bulk response ({} bytes) may cause HTTP/2 issues",
            response_size
        );
    }

    Ok(Json(result))
}

/// Handler for streaming document write operations (NDJSON input)
///
/// Reads the request body incrementally, splitting on newlines to decode
/// `DocPayload` items one at a time. Documents are accumulated into
/// micro-batches of `stream_batch_size` and each batch is dispatched
/// through the normal routing path as a `BulkWrite`. This keeps peak
/// memory bounded regardless of total import size.
async fn write_stream_handler(
    Path(index): Path<String>,
    State(state): State<AppState>,
    body: Body,
) -> Result<Response, AppError> {
    info!("Write stream request - index: {}", index);

    let batch_size = state.stream_batch_size.max(1);

    // Aggregate counters across all micro-batches
    let mut total_items_written: u64 = 0;
    let mut total_errors: Vec<String> = Vec::new();
    let mut total_line_count: usize = 0;
    let mut batches_dispatched: usize = 0;

    // Buffer for incomplete trailing line across body chunks
    let mut buf = BytesMut::new();
    // Current micro-batch accumulator
    let mut batch: Vec<DocPayload> = Vec::with_capacity(batch_size);

    let mut body_stream = body.into_data_stream();
    let max_record_size_bytes = state.max_record_size_bytes;

    while let Some(chunk_result) = body_stream.next().await {
        let chunk = chunk_result.map_err(|e| {
            AppError::bad_request(format!("Failed to read request body chunk: {}", e))
        })?;

        buf.extend_from_slice(&chunk);

        // A line only leaves `buf` when its newline arrives, so an unterminated line would
        // otherwise buffer the entire request allowance before any limit was consulted.
        // Rejecting here keeps peak memory bounded by the record size, not the body size.
        if buf.len() > max_record_size_bytes {
            return Err(AppError::payload_too_large(format!(
                "document on line {} exceeds the {} MB single-record limit",
                total_line_count + 1,
                max_record_size_bytes / (1024 * 1024)
            )));
        }

        // Process all complete lines in the buffer
        while let Some(newline_pos) = buf.iter().position(|&b| b == b'\n') {
            let line = buf.split_to(newline_pos + 1);
            let line = &line[..line.len() - 1]; // trim trailing newline
            if line.is_empty() {
                continue;
            }

            total_line_count += 1;
            let doc_payload: DocPayload = serde_json::from_slice(line).map_err(|e| {
                AppError::bad_request(format!(
                    "Failed to parse document on line {}: {}",
                    total_line_count, e
                ))
            })?;
            batch.push(doc_payload);

            // Flush the micro-batch when it reaches the configured size
            if batch.len() >= batch_size {
                let flush_result = flush_write_batch(
                    &state,
                    &index,
                    std::mem::replace(&mut batch, Vec::with_capacity(batch_size)),
                )
                .await;
                batches_dispatched += 1;
                accumulate_batch_result(flush_result, &mut total_items_written, &mut total_errors);
            }
        }
    }

    // Handle any remaining data after the last newline (trailing line without \n)
    if !buf.is_empty() {
        let line = buf.freeze();
        if !line.is_empty() {
            total_line_count += 1;
            let doc_payload: DocPayload = serde_json::from_slice(&line).map_err(|e| {
                AppError::bad_request(format!(
                    "Failed to parse document on line {}: {}",
                    total_line_count, e
                ))
            })?;
            batch.push(doc_payload);
        }
    }

    // Flush any remaining documents in the final micro-batch
    if !batch.is_empty() {
        let flush_result = flush_write_batch(&state, &index, batch).await;
        batches_dispatched += 1;
        accumulate_batch_result(flush_result, &mut total_items_written, &mut total_errors);
    }

    if total_line_count == 0 {
        return Err(AppError::bad_request("No documents found in request body"));
    }

    info!(
        "Write stream completed - index: {}, lines: {}, batches: {}, written: {}, errors: {}",
        index,
        total_line_count,
        batches_dispatched,
        total_items_written,
        total_errors.len()
    );

    let result = serde_json::json!({
        "status": if total_errors.is_empty() { "ok" } else { "partial" },
        "items_written": total_items_written,
        "lines_received": total_line_count,
        "batches": batches_dispatched,
        "errors": total_errors,
    });

    let bytes = serde_json::to_vec(&result).map_err(|e| {
        AppError::from(anyhow::anyhow!(
            "Failed to serialize write stream result: {}",
            e
        ))
    })?;

    let mut resp = Response::new(Body::from(bytes));
    resp.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    Ok(resp)
}

/// Derive a routing hint from the first document in a batch.
fn derive_routing_hint(docs: &[DocPayload]) -> Option<String> {
    docs.first().and_then(|doc| {
        doc.routing_key.clone().or_else(|| {
            if !doc.id.is_empty() {
                Some(doc.id.clone())
            } else {
                serde_json::to_vec(&doc.doc)
                    .ok()
                    .map(|bytes| format!("{:016x}", xxhash_rust::xxh3::xxh3_64(&bytes)))
            }
        })
    })
}

/// Dispatch a single micro-batch of documents through the routing layer.
async fn flush_write_batch(
    state: &AppState,
    index: &str,
    docs: Vec<DocPayload>,
) -> Result<JsonValue, crate::node_orchestrator::OrchestratorError> {
    let routing_hint = derive_routing_hint(&docs);
    let client_op = ClientOp::BulkWrite {
        index: index.to_string(),
        docs,
    };
    state
        .router
        .route_and_handle(client_op, routing_hint, OperationType::Write)
        .await
}

/// Accumulate items_written and errors from a micro-batch result into totals.
fn accumulate_batch_result(
    result: Result<JsonValue, crate::node_orchestrator::OrchestratorError>,
    total_items_written: &mut u64,
    total_errors: &mut Vec<String>,
) {
    match result {
        Ok(val) => {
            if let Some(written) = val.get("items_written").and_then(|v| v.as_u64()) {
                *total_items_written += written;
            }
            if let Some(errs) = val.get("errors").and_then(|v| v.as_array()) {
                for err in errs {
                    if let Some(msg) = err.as_str() {
                        total_errors.push(msg.to_string());
                    }
                }
            }
        }
        Err(e) => {
            total_errors.push(format!("Batch dispatch failed: {}", e));
        }
    }
}

/// Handler for creating/updating index configuration/schema
async fn create_config_handler(
    Path(index): Path<String>,
    State(state): State<AppState>,
    Json(schema): Json<IndexSchema>,
) -> Result<Json<JsonValue>, AppError> {
    validate_index_name(&index)?;
    info!("Create config request - index: {}", index);

    let client_op = ClientOp::CreateConfig { index, schema };

    let result = state
        .router
        .route_and_handle(client_op, None, OperationType::Write)
        .await?;
    Ok(Json(result))
}

/// Handler for retrieving index configuration/schema
async fn get_config_handler(
    Path(index): Path<String>,
    State(state): State<AppState>,
) -> Result<Json<JsonValue>, AppError> {
    info!("Get config request - index: {}", index);

    let client_op = ClientOp::GetConfig { index };

    let result = state
        .router
        .route_and_handle(client_op, None, OperationType::Read)
        .await?;
    Ok(Json(result))
}

/// Handler for listing all available indexes
async fn list_indexes_handler(
    State(state): State<AppState>,
    Query(params): Query<ListIndexesQuery>,
) -> Result<Json<JsonValue>, AppError> {
    info!("List indexes request");

    let client_op = ClientOp::ListIndexes {
        include_data_size: params.include_data_size(),
    };

    let result = state
        .router
        .route_and_handle(client_op, None, OperationType::Read)
        .await?;
    Ok(Json(result))
}

async fn admin_memory_handler(
    State(state): State<AppState>,
) -> Result<Json<AdminMemoryReport>, AppError> {
    Ok(Json(state.router.admin_memory().await?))
}

async fn admin_memory_purge_handler(
    State(state): State<AppState>,
    Query(params): Query<AdminPurgeParams>,
) -> Result<Json<AdminMemoryReport>, AppError> {
    Ok(Json(state.router.admin_purge_memory(params.force).await?))
}

async fn admin_index_commit_handler(
    Path(index): Path<String>,
    State(state): State<AppState>,
) -> Result<Json<AdminIndexCommitReport>, AppError> {
    Ok(Json(state.router.admin_commit_index(index).await?))
}

async fn admin_index_evict_writer_handler(
    Path(index): Path<String>,
    State(state): State<AppState>,
) -> Result<Json<AdminIndexEvictWriterReport>, AppError> {
    Ok(Json(state.router.admin_evict_index_writer(index).await?))
}

async fn admin_workers_handler(
    State(state): State<AppState>,
) -> Result<Json<WorkerPoolReport>, AppError> {
    Ok(Json(state.router.admin_worker_stats()?))
}

/// Handler for cluster health check
async fn health_handler(State(state): State<AppState>) -> Result<Json<HealthResponse>, AppError> {
    // Query cluster status from coordinator
    let cluster_status = match state.coordinator.ask(GetStatus).await {
        Ok(status) => Some(status),
        Err(err) => {
            error!(error = ?err, "Failed to get cluster status from coordinator");
            None
        }
    };

    // Get basic shard count and node info from orchestrator
    let shard_count = state.router.shard_count().await;
    let (node_id, node_name) = match state.router.handle_client_op(ClientOp::GetIdentity).await {
        Ok(result) => {
            let node_id = result
                .get("node_id")
                .and_then(|v| v.as_str())
                .unwrap_or("local")
                .to_string();
            let node_name = result
                .get("node_name")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                .to_string();
            (node_id, node_name)
        }
        Err(_) => ("local".to_string(), "unknown".to_string()),
    };

    // Get index statistics for health check
    let (total_indexes, indexes_with_data) = match state
        .router
        .handle_client_op(ClientOp::ListIndexes {
            include_data_size: false,
        })
        .await
    {
        Ok(result) => {
            let total = result
                .get("total_indexes")
                .and_then(|v| v.as_u64())
                .unwrap_or(0) as usize;
            let empty_vec = vec![];
            let indexes_array = result
                .get("indexes")
                .and_then(|arr| arr.as_array())
                .unwrap_or(&empty_vec);
            let with_data = indexes_array
                .iter()
                .filter(|idx| {
                    idx.get("document_count")
                        .and_then(|c| c.as_u64())
                        .unwrap_or(0)
                        > 0
                })
                .count();
            (total, with_data)
        }
        Err(_) => (0, 0), // Fallback to 0 if index listing fails
    };

    let response = HealthResponse {
        status: cluster_status
            .as_ref()
            .map(|s| s.health.clone())
            .unwrap_or_else(|| "green".to_string()),
        node_id,
        node_name,
        cluster_name: cluster_status.as_ref().map(|s| s.cluster_name.clone()),
        cluster_enabled: cluster_status.as_ref().map(|s| s.cluster_enabled),
        total_nodes: cluster_status.as_ref().map(|s| s.total_nodes),
        connected_nodes: cluster_status.as_ref().map(|s| s.connected_nodes),
        cluster_total_shards: cluster_status.as_ref().map(|s| s.total_shards),
        active_shards: shard_count,
        total_indexes,
        indexes_with_data,
        dial_failures: cluster_status.as_ref().map(|s| s.dial_failures),
        bootstrap_successes: cluster_status.as_ref().map(|s| s.bootstrap_successes),
        routing_updates: cluster_status.as_ref().map(|s| s.routing_updates),
    };

    Ok(Json(response))
}

/// Handler for schema updates (maintenance API)
async fn update_schema_handler(
    Path(index): Path<String>,
    State(state): State<AppState>,
    Json(payload): Json<SchemaUpdatePayload>,
) -> Result<Json<JsonValue>, AppError> {
    info!(
        index = %index,
        field_count = payload.field_updates.len(),
        "Schema update request"
    );

    // Get current schema
    let current_schema_result = state
        .router
        .route_and_handle(
            ClientOp::GetConfig {
                index: index.clone(),
            },
            None,
            OperationType::Read,
        )
        .await?;

    // The stored schema is server-side data, so a decode failure here is a 500,
    // not a client error.
    let mut schema: IndexSchema = serde_json::from_value(current_schema_result)
        .map_err(|e| AppError::from(anyhow::anyhow!("Failed to decode stored schema: {}", e)))?;

    // Update indexed flags for specified fields
    let mut updated_fields = Vec::new();
    let mut missing_fields = Vec::new();

    for (field_name, indexed) in payload.field_updates {
        if let Some(field_def) = schema.fields.get_mut(&field_name) {
            field_def.indexed = indexed;
            updated_fields.push(field_name);
        } else {
            missing_fields.push(field_name);
        }
    }

    if !missing_fields.is_empty() {
        return Err(AppError::bad_request(format!(
            "Fields not found in schema: {}",
            missing_fields.join(", ")
        )));
    }

    // Store updated schema
    state
        .router
        .route_and_handle(
            ClientOp::CreateConfig {
                index: index.clone(),
                schema: schema.clone(),
            },
            None,
            OperationType::Write,
        )
        .await?;

    info!(
        index = %index,
        updated_fields = ?updated_fields,
        "Schema updated successfully"
    );

    Ok(Json(serde_json::json!({
        "success": true,
        "index": index,
        "updated_fields": updated_fields,
        "message": "Schema updated successfully. New writes will respect updated indexed flags."
    })))
}

/// Handler for deleting an index and all its data
async fn delete_index_handler(
    Path(index): Path<String>,
    State(state): State<AppState>,
    Query(params): Query<DeleteIndexParams>,
) -> Result<Json<JsonValue>, AppError> {
    info!(
        "Delete index request - index: {}, delete_schema: {:?}",
        index, params.delete_schema
    );

    // Require the index to exist before deleting. A name that was never created
    // cannot have passed `validate_index_name`, so this also rejects traversal
    // attempts. Distinguish "absent" from "lookup failed" so that an actor
    // timeout is not reported to the client as a missing index.
    if let Err(e) = state
        .router
        .handle_client_op(ClientOp::GetConfig {
            index: index.clone(),
        })
        .await
    {
        let msg = e.to_string();
        return Err(if msg.contains("NotFound") || msg.contains("not found") {
            AppError::not_found(format!("index '{}' not found", index))
        } else {
            AppError::from(anyhow::anyhow!(
                "Failed to look up index '{}': {}",
                index,
                msg
            ))
        });
    }

    // Use cluster coordinator for proper cluster-wide index deletion
    let delete_msg = crate::cluster_coordinator::DeleteIndexCluster {
        index: index.clone(),
        delete_schema: params.delete_schema.unwrap_or(false),
    };

    let result = state.coordinator.ask(delete_msg).await.map_err(|e| {
        AppError::from(anyhow::anyhow!(
            "Failed to delete index across cluster: {}",
            e
        ))
    })?;

    Ok(Json(result))
}

#[derive(Deserialize, Default)]
struct DeleteIndexParams {
    delete_schema: Option<bool>,
}

#[derive(Deserialize, Default)]
struct AdminPurgeParams {
    force: bool,
}

/// Fallback handler for 404/405 to return JSON error shape
async fn fallback_handler(uri: axum::http::Uri) -> impl IntoResponse {
    (
        StatusCode::NOT_FOUND,
        Json(serde_json::json!({
            "error": "Not Found",
            "path": uri.to_string()
        })),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_query_keywords_no_keywords() {
        let query = "title:rust";
        let (cleaned, limit, fields, parsed_sort) = parse_query_keywords(query);
        assert_eq!(cleaned, "title:rust");
        assert_eq!(limit, None);
        assert_eq!(fields, None);
        assert_eq!(parsed_sort, None);
    }

    #[test]
    fn test_parse_query_keywords_limit_only() {
        let query = "title:rust limit 10";
        let (cleaned, limit, fields, parsed_sort) = parse_query_keywords(query);
        assert_eq!(cleaned, "title:rust");
        assert_eq!(limit, Some(10));
        assert_eq!(fields, None);
        assert_eq!(parsed_sort, None);
    }

    #[test]
    fn test_parse_query_keywords_limit_zero() {
        let query = "title:rust limit 0";
        let (cleaned, limit, fields, parsed_sort) = parse_query_keywords(query);
        assert_eq!(cleaned, "title:rust");
        assert_eq!(limit, Some(0));
        assert_eq!(fields, None);
        assert_eq!(parsed_sort, None);
    }

    #[test]
    fn test_parse_query_keywords_return_only() {
        let query = "title:rust return title,author,year";
        let (cleaned, limit, fields, parsed_sort) = parse_query_keywords(query);
        assert_eq!(cleaned, "title:rust");
        assert_eq!(limit, None);
        assert_eq!(
            fields,
            Some(vec![
                "title".to_string(),
                "author".to_string(),
                "year".to_string()
            ])
        );
        assert_eq!(parsed_sort, None);
    }

    #[test]
    fn test_parse_query_keywords_both() {
        let query = "title:rust limit 5 return title,author";
        let (cleaned, limit, fields, parsed_sort) = parse_query_keywords(query);
        assert_eq!(cleaned, "title:rust");
        assert_eq!(limit, Some(5));
        assert_eq!(
            fields,
            Some(vec!["title".to_string(), "author".to_string()])
        );
        assert_eq!(parsed_sort, None);
    }

    #[test]
    fn test_parse_query_keywords_reverse_order() {
        let query = "title:rust return title,author limit 5";
        let (cleaned, limit, fields, parsed_sort) = parse_query_keywords(query);
        assert_eq!(cleaned, "title:rust");
        assert_eq!(limit, Some(5));
        assert_eq!(
            fields,
            Some(vec!["title".to_string(), "author".to_string()])
        );
        assert_eq!(parsed_sort, None);
    }

    #[test]
    fn test_parse_query_keywords_single_field() {
        let query = "title:rust return title";
        let (cleaned, limit, fields, parsed_sort) = parse_query_keywords(query);
        assert_eq!(cleaned, "title:rust");
        assert_eq!(limit, None);
        assert_eq!(fields, Some(vec!["title".to_string()]));
        assert_eq!(parsed_sort, None);
    }

    #[test]
    fn test_parse_query_keywords_with_spaces() {
        // Test space-separated field list: "return title, author, year"
        let query = "title:rust return title, author, year";
        let (cleaned, limit, fields, parsed_sort) = parse_query_keywords(query);
        assert_eq!(cleaned, "title:rust");
        assert_eq!(limit, None);
        assert_eq!(
            fields,
            Some(vec![
                "title".to_string(),
                "author".to_string(),
                "year".to_string()
            ])
        );
        assert_eq!(parsed_sort, None);
    }

    #[test]
    fn test_parse_query_keywords_complex_query() {
        let query = "title:rust AND author:smith limit 20 return title,author,year,isbn";
        let (cleaned, limit, fields, parsed_sort) = parse_query_keywords(query);
        assert_eq!(cleaned, "title:rust AND author:smith");
        assert_eq!(limit, Some(20));
        assert_eq!(
            fields,
            Some(vec![
                "title".to_string(),
                "author".to_string(),
                "year".to_string(),
                "isbn".to_string()
            ])
        );
        assert_eq!(parsed_sort, None);
    }

    #[test]
    fn test_parse_query_keywords_invalid_limit() {
        let query = "title:rust limit abc";
        let (cleaned, limit, fields, parsed_sort) = parse_query_keywords(query);
        // Invalid limit should not be parsed, query remains unchanged
        assert_eq!(cleaned, "title:rust limit abc");
        assert_eq!(limit, None);
        assert_eq!(fields, None);
        assert_eq!(parsed_sort, None);
    }

    #[test]
    fn test_parse_query_keywords_empty_field_list() {
        let query = "title:rust return ";
        let (cleaned, limit, fields, parsed_sort) = parse_query_keywords(query);
        // Empty field list should not be parsed
        assert_eq!(cleaned, "title:rust return");
        assert_eq!(limit, None);
        assert_eq!(fields, None);
        assert_eq!(parsed_sort, None);
    }

    #[test]
    fn test_parse_query_keywords_trailing_comma() {
        let query = "title:rust return title,author,";
        let (cleaned, limit, fields, parsed_sort) = parse_query_keywords(query);
        assert_eq!(cleaned, "title:rust");
        assert_eq!(limit, None);
        // Trailing comma should be filtered out
        assert_eq!(
            fields,
            Some(vec!["title".to_string(), "author".to_string()])
        );
        assert_eq!(parsed_sort, None);
    }

    #[test]
    fn test_parse_query_keywords_sort_desc() {
        let query = "title:rust sort year:desc";
        let (cleaned, limit, fields, parsed_sort) = parse_query_keywords(query);
        assert_eq!(cleaned, "title:rust");
        assert_eq!(limit, None);
        assert_eq!(fields, None);
        assert_eq!(
            parsed_sort,
            Some(SortSpec {
                field: "year".to_string(),
                order: SortOrder::Desc,
            })
        );
    }

    #[test]
    fn test_parse_query_keywords_sort_asc() {
        let query = "title:rust sort year:asc";
        let (cleaned, limit, fields, parsed_sort) = parse_query_keywords(query);
        assert_eq!(cleaned, "title:rust");
        assert_eq!(limit, None);
        assert_eq!(fields, None);
        assert_eq!(
            parsed_sort,
            Some(SortSpec {
                field: "year".to_string(),
                order: SortOrder::Asc,
            })
        );
    }

    #[test]
    fn test_parse_query_keywords_sort_default_order() {
        let query = "title:rust sort year";
        let (cleaned, limit, fields, parsed_sort) = parse_query_keywords(query);
        assert_eq!(cleaned, "title:rust");
        assert_eq!(limit, None);
        assert_eq!(fields, None);
        assert_eq!(
            parsed_sort,
            Some(SortSpec {
                field: "year".to_string(),
                order: SortOrder::Asc,
            })
        );
    }

    #[test]
    fn test_parse_query_keywords_all_three() {
        let query = "title:rust return title,author limit 10 sort year:desc";
        let (cleaned, limit, fields, parsed_sort) = parse_query_keywords(query);
        assert_eq!(cleaned, "title:rust");
        assert_eq!(limit, Some(10));
        assert_eq!(
            fields,
            Some(vec!["title".to_string(), "author".to_string()])
        );
        assert_eq!(
            parsed_sort,
            Some(SortSpec {
                field: "year".to_string(),
                order: SortOrder::Desc,
            })
        );
    }

    #[test]
    fn test_parse_query_keywords_sort_before_return() {
        let query = "title:rust sort year:asc return title,author";
        let (cleaned, limit, fields, parsed_sort) = parse_query_keywords(query);
        assert_eq!(cleaned, "title:rust");
        assert_eq!(limit, None);
        assert_eq!(
            fields,
            Some(vec!["title".to_string(), "author".to_string()])
        );
        assert_eq!(
            parsed_sort,
            Some(SortSpec {
                field: "year".to_string(),
                order: SortOrder::Asc,
            })
        );
    }

    #[test]
    fn test_parse_query_keywords_sort_between_limit_and_return() {
        let query = "title:rust limit 5 sort timestamp:desc return title";
        let (cleaned, limit, fields, parsed_sort) = parse_query_keywords(query);
        assert_eq!(cleaned, "title:rust");
        assert_eq!(limit, Some(5));
        assert_eq!(fields, Some(vec!["title".to_string()]));
        assert_eq!(
            parsed_sort,
            Some(SortSpec {
                field: "timestamp".to_string(),
                order: SortOrder::Desc,
            })
        );
    }

    #[test]
    fn test_parse_query_keywords_empty_query() {
        let query = "";
        let (cleaned, limit, fields, parsed_sort) = parse_query_keywords(query);
        assert_eq!(cleaned, "");
        assert_eq!(limit, None);
        assert_eq!(fields, None);
        assert_eq!(parsed_sort, None);
    }
}

#[cfg(test)]
mod index_name_validation_tests {
    use super::validate_index_name;

    #[test]
    fn valid_names() {
        assert!(validate_index_name("my-index").is_ok());
        assert!(validate_index_name("index_123").is_ok());
        assert!(validate_index_name("a").is_ok());
        assert!(validate_index_name("camelCase").is_ok());
        assert!(validate_index_name("dots.are.ok").is_ok());
    }

    #[test]
    fn rejects_empty() {
        assert!(validate_index_name("").is_err());
    }

    #[test]
    fn rejects_too_long() {
        let long = "a".repeat(256);
        assert!(validate_index_name(&long).is_err());
    }

    #[test]
    fn rejects_non_alphanumeric_start() {
        assert!(validate_index_name("_bad").is_err());
        assert!(validate_index_name("-bad").is_err());
        assert!(validate_index_name(".bad").is_err());
    }

    #[test]
    fn rejects_path_traversal() {
        assert!(validate_index_name("..").is_err());
        assert!(validate_index_name("../etc").is_err());
        assert!(validate_index_name("a..b").is_err());
        assert!(validate_index_name("..%2f..%2fetc").is_err());
    }

    #[test]
    fn rejects_path_separators() {
        assert!(validate_index_name("a/b").is_err());
        assert!(validate_index_name("a\\b").is_err());
        assert!(validate_index_name("/etc").is_err());
    }

    #[test]
    fn rejects_special_chars() {
        assert!(validate_index_name("a b").is_err());
        assert!(validate_index_name("a;b").is_err());
        assert!(validate_index_name("a&b").is_err());
        assert!(validate_index_name("a|b").is_err());
    }
}
