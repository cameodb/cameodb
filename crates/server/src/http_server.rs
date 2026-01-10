//! HTTP Server Implementation for CameoDB
//!
//! Provides REST API endpoints for distributed hybrid-search operations
//! using Axum web framework with streaming support.

use axum::{
    Json, Router,
    body::Body,
    extract::{DefaultBodyLimit, Path, State},
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
use tracing::{error, info};

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

        error!("API Error: {} -> {}: {}", status, message, error_msg);

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
}

/// Schema update request payload for maintenance API
#[derive(Debug, Deserialize)]
pub struct SchemaUpdatePayload {
    /// Map of field_name -> indexed (true/false)
    pub field_updates: std::collections::HashMap<String, bool>,
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
        .route("/api/{index}/stream", post(stream_handler))
        .route("/api/{index}/document", put(write_handler))
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

/// Handler for standard search operations
async fn search_handler(
    Path(index): Path<String>,
    State(state): State<AppState>,
    Json(payload): Json<SearchPayload>,
) -> Result<Json<JsonValue>, AppError> {
    info!(
        "Search request - index: {}, query: {}, limit: {:?}",
        index, payload.query, payload.limit
    );

    let client_op = ClientOp::Search {
        index,
        query: payload.query,
        limit: payload.limit,
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
) -> Result<Json<JsonValue>, AppError> {
    info!("List cluster indexes request");

    let client_op = ClientOp::ListClusterIndexes;

    let result = state
        .router
        .route_and_handle(client_op, None, OperationType::Read)
        .await?;
    Ok(Json(result))
}

/// Handler for streaming search operations
async fn stream_handler(
    Path(index): Path<String>,
    State(state): State<AppState>,
    Json(payload): Json<SearchPayload>,
) -> Result<Response, AppError> {
    info!(
        "Stream request - index: {}, query: {}, limit: {:?}",
        index, payload.query, payload.limit
    );

    // Use streaming search with our new streaming infrastructure
    let client_op = ClientOp::Stream {
        index,
        query: payload.query,
        limit: payload.limit,
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
    Ok(Json(result))
}

/// Handler for creating/updating index configuration/schema
async fn create_config_handler(
    Path(index): Path<String>,
    State(state): State<AppState>,
    Json(schema): Json<IndexSchema>,
) -> Result<Json<JsonValue>, AppError> {
    info!(
        "Create config request - index: {}, shard_count: {}",
        index, schema.shard_count
    );

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
async fn list_indexes_handler(State(state): State<AppState>) -> Result<Json<JsonValue>, AppError> {
    info!("List indexes request");

    let client_op = ClientOp::ListIndexes;

    let result = state
        .router
        .route_and_handle(client_op, None, OperationType::Read)
        .await?;
    Ok(Json(result))
}

/// Handler for cluster health check
async fn health_handler(State(state): State<AppState>) -> Result<Json<HealthResponse>, AppError> {
    let identity = state
        .router
        .get_identity()
        .await
        .map_err(|e| AppError(anyhow::anyhow!(e)))?;
    let shard_count = state
        .router
        .get_shard_count()
        .await
        .map_err(|e| AppError(anyhow::anyhow!(e)))?;

    // Query cluster status from coordinator
    let cluster_status = match state.coordinator.ask(GetStatus).await {
        Ok(status) => Some(status),
        Err(err) => {
            error!(error = ?err, "Failed to get cluster status from coordinator");
            None
        }
    };

    let response = HealthResponse {
        status: cluster_status
            .as_ref()
            .map(|s| s.health.clone())
            .unwrap_or_else(|| "green".to_string()),
        node_id: identity.uuid.to_string(),
        node_name: identity.name.clone(),
        cluster_name: cluster_status.as_ref().map(|s| s.cluster_name.clone()),
        cluster_enabled: cluster_status.as_ref().map(|s| s.cluster_enabled),
        total_nodes: cluster_status.as_ref().map(|s| s.total_nodes),
        connected_nodes: cluster_status.as_ref().map(|s| s.connected_nodes),
        cluster_total_shards: cluster_status.as_ref().map(|s| s.total_shards),
        active_shards: shard_count,
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
) -> Result<Json<JsonValue>, AppError> {
    info!("Delete index request - index: {}", index);

    let client_op = ClientOp::DeleteIndex { index };

    // Use Broadcast to delete from all nodes in cluster
    let result = state
        .router
        .route_and_handle(client_op, None, OperationType::Write)
        .await?;
    Ok(Json(result))
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

// TODO: Add HTTP endpoint tests
