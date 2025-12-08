//! HTTP Server Implementation for CameoDB
//!
//! Provides REST API endpoints for distributed hybrid-search operations
//! using Axum web framework with streaming support.

use std::sync::Arc;

use axum::{
    Json, Router,
    body::Body,
    extract::{Path, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post, put},
};
use futures::stream::{StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use tokio::sync::RwLock;
use tower_http::{cors::CorsLayer, limit::RequestBodyLimitLayer, trace::TraceLayer};
use tracing::{error, info};

use crate::node_orchestrator::{ClientOp, DocPayload, NodeOrchestrator, RouterActor};
use storage::IndexSchema;

/// API Error wrapper for consistent error handling
#[derive(Debug)]
pub struct ApiError(pub anyhow::Error);

impl IntoResponse for ApiError {
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

impl<E> From<E> for ApiError
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

/// Health check response
#[derive(Debug, Serialize, Deserialize)]
pub struct HealthResponse {
    pub status: String,
    pub node_id: String,
    pub active_shards: usize,
}

/// Application state shared across handlers
#[derive(Clone)]
pub struct AppState {
    pub router: RouterActor,
    pub orchestrator: Arc<RwLock<NodeOrchestrator>>,
}

/// Creates the main HTTP router with all endpoints and middleware
pub fn create_router(state: AppState) -> Router {
    Router::new()
        // API routes
        .route("/api/:index/search", post(search_handler))
        .route("/api/:index/stream", post(stream_handler))
        .route("/api/:index/document", put(write_handler))
        .route("/api/:index/_bulk", post(bulk_write_handler))
        .route("/api/:index/_config", put(create_config_handler))
        .route("/api/:index/_config", get(get_config_handler))
        // Index management
        .route("/_indexes", get(list_indexes_handler))
        // Health check
        .route("/_cluster/health", get(health_handler))
        .with_state(state)
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .layer(RequestBodyLimitLayer::new(20 * 1024 * 1024)) // 20MB limit
}

/// Handler for standard search operations
async fn search_handler(
    Path(index): Path<String>,
    State(state): State<AppState>,
    Json(payload): Json<SearchPayload>,
) -> Result<Json<JsonValue>, ApiError> {
    info!(
        "Search request - index: {}, query: {}, limit: {:?}",
        index, payload.query, payload.limit
    );

    let client_op = ClientOp::Search {
        index,
        query: payload.query,
        limit: payload.limit,
    };

    let result = state.router.handle_client_op(client_op).await?;
    Ok(Json(result))
}

/// Handler for streaming search operations
async fn stream_handler(
    Path(index): Path<String>,
    State(state): State<AppState>,
    Json(payload): Json<SearchPayload>,
) -> Result<Response, ApiError> {
    info!(
        "Stream request - index: {}, query: {}",
        index, payload.query
    );

    // Create the stream using the router
    let stream = state
        .router
        .handle_client_stream(index.clone(), payload.query.clone())
        .await?;

    // Convert the stream to NDJSON format
    let ndjson_stream = stream
        .map(|chunk| {
            // Convert each chunk to newline-delimited JSON
            let json_lines: Vec<String> = chunk
                .into_iter()
                .map(|(score, mut doc)| {
                    // Add score to document
                    if let JsonValue::Object(ref mut obj) = doc {
                        obj.insert(
                            "_score".to_string(),
                            JsonValue::Number(
                                serde_json::Number::from_f64(score as f64)
                                    .unwrap_or_else(|| serde_json::Number::from(0)),
                            ),
                        );
                    }
                    serde_json::to_string(&doc).unwrap_or_else(|_| "{}".to_string())
                })
                .collect();

            Ok::<_, std::convert::Infallible>(json_lines.join("\n") + "\n")
        })
        .map_ok(|data| data.into_bytes());

    let body = Body::from_stream(ndjson_stream);

    let response = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/x-ndjson")
        .header(header::TRANSFER_ENCODING, "chunked")
        .body(body)
        .map_err(|e| ApiError(anyhow::anyhow!("Failed to create response: {}", e)))?;

    Ok(response)
}

/// Handler for document write operations
async fn write_handler(
    Path(index): Path<String>,
    State(state): State<AppState>,
    Json(payload): Json<DocPayload>,
) -> Result<Json<JsonValue>, ApiError> {
    info!("Write request - index: {}, doc_id: {}", index, payload.id);

    let DocPayload {
        id,
        routing_key,
        doc,
    } = payload;

    let client_op = ClientOp::Write {
        index,
        id,
        routing_key,
        doc,
    };

    let result = state.router.handle_client_op(client_op).await?;
    Ok(Json(result))
}

/// Handler for bulk write operations
async fn bulk_write_handler(
    Path(index): Path<String>,
    State(state): State<AppState>,
    Json(docs): Json<Vec<DocPayload>>,
) -> Result<Json<JsonValue>, ApiError> {
    info!(
        "Bulk write request - index: {}, docs: {}",
        index,
        docs.len()
    );

    let client_op = ClientOp::BulkWrite { index, docs };

    let result = state.router.handle_client_op(client_op).await?;
    Ok(Json(result))
}

/// Handler for creating/updating index configuration/schema
async fn create_config_handler(
    Path(index): Path<String>,
    State(state): State<AppState>,
    Json(schema): Json<IndexSchema>,
) -> Result<Json<JsonValue>, ApiError> {
    info!(
        "Create config request - index: {}, shard_count: {}",
        index, schema.shard_count
    );

    let client_op = ClientOp::CreateConfig { index, schema };

    let result = state.router.handle_client_op(client_op).await?;
    Ok(Json(result))
}

/// Handler for retrieving index configuration/schema
async fn get_config_handler(
    Path(index): Path<String>,
    State(state): State<AppState>,
) -> Result<Json<JsonValue>, ApiError> {
    info!("Get config request - index: {}", index);

    let client_op = ClientOp::GetConfig { index };

    let result = state.router.handle_client_op(client_op).await?;
    Ok(Json(result))
}

/// Handler for listing all available indexes
async fn list_indexes_handler(State(state): State<AppState>) -> Result<Json<JsonValue>, ApiError> {
    info!("List indexes request");

    let client_op = ClientOp::ListIndexes;

    let result = state.router.handle_client_op(client_op).await?;
    Ok(Json(result))
}

/// Handler for cluster health check
async fn health_handler(State(state): State<AppState>) -> Result<Json<HealthResponse>, ApiError> {
    let orchestrator = state.orchestrator.read().await;

    let response = HealthResponse {
        status: "green".to_string(),
        node_id: orchestrator.identity().uuid.to_string(),
        active_shards: orchestrator.shard_count(),
    };

    Ok(Json(response))
}

// TODO: Add HTTP endpoint tests
// Tests removed temporarily due to dependency issues
