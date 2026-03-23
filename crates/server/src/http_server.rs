//! HTTP Server Implementation for CameoDB
//!
//! Provides REST API endpoints for distributed hybrid-search operations
//! using Axum web framework with streaming support.

use axum::{
    Json, Router,
    body::Body,
    extract::{DefaultBodyLimit, Path, Query, State},
    http::{HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{delete, get, patch, post, put},
};
use bytes::BytesMut;
use cameodb_mcp::{McpBackend, McpIndexSearchRequest, McpShutdownHandle, mcp_router};
use futures::{StreamExt, future::BoxFuture};
use kameo::actor::ActorRef;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use tower_http::{
    compression::CompressionLayer, cors::CorsLayer, decompression::DecompressionLayer,
    trace::TraceLayer,
};
use tracing::{error, info, warn};

use crate::cluster_coordinator::{ClusterCoordinator, GetStatus, OperationType};
use crate::node_orchestrator::{ClientOp, DocPayload, RouterActor};
use storage::IndexSchema;

/// Application error wrapper for consistent error handling
#[derive(Debug)]
pub struct AppError(pub anyhow::Error);

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let error_msg = self.0.to_string();

        // Error classification logic
        let (status, message) = if error_msg.contains("NotFound") || error_msg.contains("not found")
        {
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

impl<E> From<E> for AppError
where
    E: Into<anyhow::Error>,
{
    fn from(err: E) -> Self {
        Self(err.into())
    }
}

/// Search request payload
#[derive(Debug, Deserialize)]
pub struct SearchPayload {
    pub query: String,
    pub limit: Option<usize>,
    /// Optional list of fields to return (field projection)
    pub fields: Option<Vec<String>>,
}

/// Schema update request payload for maintenance API
#[derive(Debug, Deserialize)]
pub struct SchemaUpdatePayload {
    /// Map of field_name -> indexed (true/false)
    pub field_updates: std::collections::HashMap<String, bool>,
}

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
            let result = state
                .router
                .route_and_handle(
                    ClientOp::Search {
                        index: index.index,
                        query: query.clone(),
                        limit,
                        fields: index.fields,
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
            let mut merged_hits = Vec::new();
            let mut per_index = Vec::new();
            let mut total_hits = 0u64;

            for index_request in indexes {
                let McpIndexSearchRequest { index, fields } = index_request;
                let index_name = index.clone();
                let result = state
                    .router
                    .route_and_handle(
                        ClientOp::Search {
                            index: index.clone(),
                            query: query.clone(),
                            limit,
                            fields,
                        },
                        None,
                        OperationType::Read,
                    )
                    .await;

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
                                    index: index.clone(),
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

                per_index.push(serde_json::json!({
                    "index": index_name,
                    "result": result,
                }));
            }

            merged_hits.sort_by(|left, right| {
                let left_score = left
                    .get("score")
                    .and_then(|value| value.as_f64())
                    .unwrap_or(0.0);
                let right_score = right
                    .get("score")
                    .and_then(|value| value.as_f64())
                    .unwrap_or(0.0);
                right_score
                    .partial_cmp(&left_score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            merged_hits.truncate(requested_limit);
            let hits_returned = merged_hits.len();

            let mut response = serde_json::json!({
                "hits": merged_hits,
                "hits_returned": hits_returned,
                "total_hits": total_hits,
                "limit": requested_limit,
                "results_by_index": per_index,
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
            let schema = state
                .router
                .handle_client_op(ClientOp::GetConfig {
                    index: index.clone(),
                })
                .await
                .map_err(|err| err.to_string())?;

            let listing = state
                .router
                .handle_client_op(ClientOp::ListIndexes {
                    include_data_size: true,
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

            Ok(serde_json::json!({
                "index": index,
                "stats": stats,
                "schema": schema,
            }))
        })
    }

    fn list_indexes(&self) -> BoxFuture<'_, Result<JsonValue, String>> {
        let state = self.clone();
        Box::pin(async move {
            let listing = state
                .router
                .handle_client_op(ClientOp::ListIndexes {
                    include_data_size: true,
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

                let schema = state
                    .router
                    .handle_client_op(ClientOp::GetConfig {
                        index: index_name.clone(),
                    })
                    .await
                    .map_err(|err| err.to_string())?;

                enriched.push(serde_json::json!({
                    "index": index_name,
                    "stats": stats,
                    "schema": schema,
                }));
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

fn field_type_query_hint(field_type: &str) -> &'static str {
    match field_type {
        "text" => "Full-text search with tokenization. Use field:value or field:\"phrase query\".",
        "string" | "exact" => "Exact match only (no tokenization). Use field:exact_value.",
        "i64" | "u64" | "f64" => {
            "Numeric field. Use field:value or range field:[low TO high]. Supports * for open bounds."
        }
        "date" => {
            "Date field. Use field:2024-01-15, field:>2024-01-01, or field:[2024-01-01 TO 2024-12-31]. Accepts YYYY-MM-DD or RFC3339."
        }
        "boolean" => "Boolean field. Use field:true or field:false.",
        "ip" => "IP address field. Use field:192.168.1.1.",
        "json" => "JSON object field. Use field.subfield:value for nested access.",
        "facet" => "Facet/category field. Use field:/path/to/category.",
        _ => "Use field:value syntax.",
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
            "description": "Search across all indexed text fields",
            "examples": ["rust database", "machine learning"]
        },
        "field_targeted": {
            "description": "Search a specific field",
            "examples": ["title:rust", "author:doe"]
        },
        "phrase_query": {
            "description": "Match an exact phrase (in order)",
            "examples": ["title:\"rust programming\"", "description:\"machine learning\""]
        },
        "boolean_operators": {
            "description": "Combine conditions with AND, OR, NOT (must be uppercase)",
            "examples": [
                "title:rust AND author:doe",
                "title:rust OR title:go",
                "title:rust NOT author:smith",
                "(title:rust OR title:go) AND year:[2020 TO 2024]"
            ]
        },
        "range_queries": {
            "description": "Match values in a range. Use * for open bounds",
            "examples": [
                "year:[2020 TO 2024]",
                "price:[10.0 TO *]",
                "age:[* TO 30]"
            ]
        },
        "date_queries": {
            "description": "Query date fields with YYYY-MM-DD or RFC3339 format. Supports comparisons and ranges",
            "examples": [
                "created_at:2024-01-15",
                "created_at:>2024-01-01",
                "created_at:<2024-12-31",
                "created_at:[2024-01-01 TO 2024-12-31]"
            ]
        },
        "exact_id_lookup": {
            "description": "Direct document lookup by ID (bypasses full-text search for speed)",
            "examples": ["id:my-document-id"]
        },
        "inline_modifiers": {
            "description": "CameoDB-specific query modifiers appended to the query string",
            "return_fields": {
                "syntax": "return field1,field2",
                "description": "Project only specific fields in results",
                "example": "title:rust return title,author,year"
            },
            "limit_results": {
                "syntax": "limit N",
                "description": "Limit the number of results returned",
                "example": "title:rust limit 5"
            },
            "combined": {
                "example": "title:rust AND author:doe return title,author limit 10"
            }
        },
        "field_types": {
            "text": "Tokenized full-text search. Supports phrases and boolean queries.",
            "string": "Exact match only (raw tokenizer, no splitting).",
            "exact": "Exact match with multi-value support (raw tokenizer).",
            "i64": "Signed 64-bit integer. Supports range queries.",
            "u64": "Unsigned 64-bit integer. Supports range queries.",
            "f64": "64-bit floating point. Supports range queries.",
            "date": "Date/datetime. Auto-normalizes common formats to RFC3339.",
            "boolean": "Boolean true/false.",
            "ip": "IP address.",
            "json": "Nested JSON object. Query with dot notation (field.subfield:value).",
            "facet": "Hierarchical category. Query with /path/syntax."
        }
    })
}

/// Creates the main HTTP router with all endpoints and middleware
///
/// # Arguments
/// * `state` - Application state with actor references
/// * `max_body_size_mb` - Maximum request body size in MB (from config)
pub fn create_router(state: AppState, max_body_size_mb: usize) -> (Router, McpShutdownHandle) {
    let body_limit_bytes = max_body_size_mb * 1024 * 1024;
    let (mcp_routes, mcp_handle) = mcp_router::<AppState>();
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
        .route("/_cluster/health", get(health_handler))
        .fallback(fallback_handler)
        .with_state(state)
        // Response compression first (outermost)
        .layer(CompressionLayer::new())
        // Allow compressed requests
        .layer(DecompressionLayer::new())
        .layer(DefaultBodyLimit::max(body_limit_bytes))
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http());

    (router, mcp_handle)
}

/// Parse query string for 'limit <n>' and 'return <field1,field2,...>' keywords.
/// Returns (cleaned_query, extracted_limit, extracted_fields).
fn parse_query_keywords(query: &str) -> (String, Option<usize>, Option<Vec<String>>) {
    let parts: Vec<&str> = query.split_whitespace().collect();
    let mut limit = None;
    let mut fields = None;
    let mut query_end_idx = parts.len();

    // Find positions of both keywords
    let return_idx = parts.iter().position(|&p| p == "return");
    let limit_idx = parts.iter().position(|&p| p == "limit");

    // Track which keywords were successfully parsed
    let mut return_parsed = false;
    let mut limit_parsed = false;

    // Parse 'return' keyword - collects all tokens after it until 'limit' or end
    if let Some(return_idx) = return_idx
        && return_idx + 1 < parts.len()
    {
        // Determine where the field list ends (either at 'limit' or end of parts)
        let field_end_idx = limit_idx.filter(|&l| l > return_idx).unwrap_or(parts.len());

        // Collect tokens between 'return' and the end position
        let field_tokens = &parts[return_idx + 1..field_end_idx];
        let field_str = field_tokens.join(" ");

        // Parse comma-separated field list
        let field_list: Vec<String> = field_str
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        if !field_list.is_empty() {
            fields = Some(field_list);
            return_parsed = true;
        }
    }

    // Parse 'limit' keyword
    if let Some(limit_idx) = limit_idx {
        // Check if there's a token after 'limit'
        let limit_value_idx = limit_idx + 1;
        if limit_value_idx < parts.len() {
            // Parse the limit value regardless of position relative to 'return'
            if let Ok(n) = parts[limit_value_idx].parse::<usize>() {
                limit = Some(n);
                limit_parsed = true;
            }
        }
    }

    // Determine where the query ends based on successfully parsed keywords
    if return_parsed && limit_parsed {
        // Both parsed - query ends at the first keyword
        query_end_idx = return_idx.unwrap().min(limit_idx.unwrap());
    } else if return_parsed {
        // Only return parsed
        query_end_idx = return_idx.unwrap();
    } else if limit_parsed {
        // Only limit parsed
        query_end_idx = limit_idx.unwrap();
    }
    // If neither parsed, query_end_idx remains at parts.len()

    let cleaned_query = parts[..query_end_idx].join(" ");
    (cleaned_query, limit, fields)
}

/// Handler for standard search operations
async fn search_handler(
    Path(index): Path<String>,
    State(state): State<AppState>,
    Json(payload): Json<SearchPayload>,
) -> Result<Json<JsonValue>, AppError> {
    // Parse query string for embedded limit/return keywords
    let (cleaned_query, parsed_limit, parsed_fields) = parse_query_keywords(&payload.query);

    // Explicit payload fields override parsed values
    let final_limit = payload.limit.or(parsed_limit);
    let final_fields = payload.fields.or(parsed_fields);

    info!(
        "Search request - index: {}, query: {}, limit: {:?}, fields: {:?}",
        index, cleaned_query, final_limit, final_fields
    );

    let client_op = ClientOp::Search {
        index,
        query: cleaned_query,
        limit: final_limit,
        fields: final_fields,
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
    // Parse query string for embedded limit/return keywords
    let (cleaned_query, parsed_limit, parsed_fields) = parse_query_keywords(&payload.query);

    // Explicit payload fields override parsed values
    let final_limit = payload.limit.or(parsed_limit);
    let final_fields = payload.fields.or(parsed_fields);

    info!(
        "Stream request - index: {}, query: {}, limit: {:?}, fields: {:?}",
        index, cleaned_query, final_limit, final_fields
    );

    let client_op = ClientOp::Stream {
        index,
        query: cleaned_query,
        limit: final_limit,
        fields: final_fields,
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

    while let Some(chunk_result) = body_stream.next().await {
        let chunk = chunk_result
            .map_err(|e| AppError(anyhow::anyhow!("Failed to read request body chunk: {}", e)))?;

        buf.extend_from_slice(&chunk);

        // Process all complete lines in the buffer
        while let Some(newline_pos) = buf.iter().position(|&b| b == b'\n') {
            let line = buf.split_to(newline_pos + 1);
            let line = &line[..line.len() - 1]; // trim trailing newline
            if line.is_empty() {
                continue;
            }

            total_line_count += 1;
            let doc_payload: DocPayload = serde_json::from_slice(line).map_err(|e| {
                AppError(anyhow::anyhow!(
                    "Failed to parse document on line {}: {}",
                    total_line_count,
                    e
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
                AppError(anyhow::anyhow!(
                    "Failed to parse document on line {}: {}",
                    total_line_count,
                    e
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
        return Err(AppError(anyhow::anyhow!(
            "No documents found in request body"
        )));
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
        AppError(anyhow::anyhow!(
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

    let mut schema: IndexSchema = serde_json::from_value(current_schema_result)
        .map_err(|e| AppError(anyhow::anyhow!("Failed to parse schema: {}", e)))?;

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
        return Err(AppError(anyhow::anyhow!(
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

    // Use cluster coordinator for proper cluster-wide index deletion
    let delete_msg = crate::cluster_coordinator::DeleteIndexCluster {
        index: index.clone(),
        delete_schema: params.delete_schema.unwrap_or(false),
    };

    let result = state.coordinator.ask(delete_msg).await.map_err(|e| {
        AppError(anyhow::anyhow!(
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
        let (cleaned, limit, fields) = parse_query_keywords(query);
        assert_eq!(cleaned, "title:rust");
        assert_eq!(limit, None);
        assert_eq!(fields, None);
    }

    #[test]
    fn test_parse_query_keywords_limit_only() {
        let query = "title:rust limit 10";
        let (cleaned, limit, fields) = parse_query_keywords(query);
        assert_eq!(cleaned, "title:rust");
        assert_eq!(limit, Some(10));
        assert_eq!(fields, None);
    }

    #[test]
    fn test_parse_query_keywords_return_only() {
        let query = "title:rust return title,author,year";
        let (cleaned, limit, fields) = parse_query_keywords(query);
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
    }

    #[test]
    fn test_parse_query_keywords_both() {
        let query = "title:rust limit 5 return title,author";
        let (cleaned, limit, fields) = parse_query_keywords(query);
        assert_eq!(cleaned, "title:rust");
        assert_eq!(limit, Some(5));
        assert_eq!(
            fields,
            Some(vec!["title".to_string(), "author".to_string()])
        );
    }

    #[test]
    fn test_parse_query_keywords_reverse_order() {
        let query = "title:rust return title,author limit 5";
        let (cleaned, limit, fields) = parse_query_keywords(query);
        assert_eq!(cleaned, "title:rust");
        assert_eq!(limit, Some(5));
        assert_eq!(
            fields,
            Some(vec!["title".to_string(), "author".to_string()])
        );
    }

    #[test]
    fn test_parse_query_keywords_single_field() {
        let query = "title:rust return title";
        let (cleaned, limit, fields) = parse_query_keywords(query);
        assert_eq!(cleaned, "title:rust");
        assert_eq!(limit, None);
        assert_eq!(fields, Some(vec!["title".to_string()]));
    }

    #[test]
    fn test_parse_query_keywords_with_spaces() {
        // Test space-separated field list: "return title, author, year"
        let query = "title:rust return title, author, year";
        let (cleaned, limit, fields) = parse_query_keywords(query);
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
    }

    #[test]
    fn test_parse_query_keywords_complex_query() {
        let query = "title:rust AND author:smith limit 20 return title,author,year,isbn";
        let (cleaned, limit, fields) = parse_query_keywords(query);
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
    }

    #[test]
    fn test_parse_query_keywords_invalid_limit() {
        let query = "title:rust limit abc";
        let (cleaned, limit, fields) = parse_query_keywords(query);
        // Invalid limit should not be parsed, query remains unchanged
        assert_eq!(cleaned, "title:rust limit abc");
        assert_eq!(limit, None);
        assert_eq!(fields, None);
    }

    #[test]
    fn test_parse_query_keywords_empty_field_list() {
        let query = "title:rust return ";
        let (cleaned, limit, fields) = parse_query_keywords(query);
        // Empty field list should not be parsed
        assert_eq!(cleaned, "title:rust return");
        assert_eq!(limit, None);
        assert_eq!(fields, None);
    }

    #[test]
    fn test_parse_query_keywords_trailing_comma() {
        let query = "title:rust return title,author,";
        let (cleaned, limit, fields) = parse_query_keywords(query);
        assert_eq!(cleaned, "title:rust");
        assert_eq!(limit, None);
        // Trailing comma should be filtered out
        assert_eq!(
            fields,
            Some(vec!["title".to_string(), "author".to_string()])
        );
    }
}
