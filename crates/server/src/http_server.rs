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
use bytes::Bytes;
use futures::{StreamExt, stream};
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
}

/// Creates the main HTTP router with all endpoints and middleware
///
/// # Arguments
/// * `state` - Application state with actor references
/// * `max_body_size_mb` - Maximum request body size in MB (from config)
pub fn create_router(state: AppState, max_body_size_mb: usize) -> Router {
    let body_limit_bytes = max_body_size_mb * 1024 * 1024;
    Router::new()
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
        .layer(TraceLayer::new_for_http())
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

    // Use streaming search with our new streaming infrastructure
    let client_op = ClientOp::Stream {
        index,
        query: cleaned_query,
        limit: final_limit,
        fields: final_fields,
    };

    let result = state
        .router
        .route_and_handle(client_op, None, OperationType::Read)
        .await?;

    // Stream hits as NDJSON if present; otherwise stream the full JSON once.
    if let Some(hits) = result.get("hits").and_then(|v| v.as_array()).cloned() {
        let stream = stream::iter(hits.into_iter().map(|hit| match serde_json::to_vec(&hit) {
            Ok(mut bytes) => {
                bytes.push(b'\n');
                Ok(Bytes::from(bytes))
            }
            Err(e) => Err(std::io::Error::other(e)),
        }))
        .map(|res| res.map_err(std::io::Error::other));

        let body = Body::from_stream(stream);
        let mut resp = Response::new(body);
        resp.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/x-ndjson"),
        );
        Ok(resp)
    } else {
        let bytes = serde_json::to_vec(&result)
            .map_err(|e| AppError(anyhow::anyhow!("Failed to serialize stream result: {}", e)))?;
        let stream = stream::iter([Ok::<Bytes, std::io::Error>(Bytes::from(bytes))]);
        let body = Body::from_stream(stream);
        let mut resp = Response::new(body);
        resp.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        Ok(resp)
    }
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
async fn write_stream_handler(
    Path(index): Path<String>,
    State(state): State<AppState>,
    body: Bytes,
) -> Result<Response, AppError> {
    info!("Write stream request - index: {}", index);

    // Parse NDJSON body into individual documents
    let mut docs = Vec::new();
    let mut line_count = 0;

    for line in body.split(|&b| b == b'\n') {
        if line.is_empty() {
            continue;
        }

        line_count += 1;
        let doc_payload: DocPayload = serde_json::from_slice(line).map_err(|e| {
            AppError(anyhow::anyhow!(
                "Failed to parse document on line {}: {}",
                line_count,
                e
            ))
        })?;
        docs.push(doc_payload);
    }

    if docs.is_empty() {
        return Err(AppError(anyhow::anyhow!(
            "No documents found in request body"
        )));
    }

    info!(
        "Write stream processing - index: {}, docs: {}",
        index,
        docs.len()
    );

    // Derive a routing hint from the first document to avoid cluster-wide broadcast
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

    // Return result as JSON
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
