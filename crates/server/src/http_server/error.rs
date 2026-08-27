//! The error type every handler returns, and how it becomes a response.

use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use tracing::{error, warn};

use crate::node_orchestrator::OrchestratorError;

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

    /// Classify an error the routing layer returned.
    ///
    /// Some of what routing refuses is the caller's fault rather than the node's — a sort on a
    /// field the index does not have, decided before any shard is asked. Those carry a status
    /// here instead of falling through to the text classification below, which would read
    /// "field" and answer 500, or read "not found" and answer 404 for a request that was
    /// simply malformed.
    ///
    /// `InvalidInput` is the same verdict reached on another node: a peer that refused the
    /// request keeps that kind across the wire, so a routed search answers the caller the same
    /// way whether it ran here or there.
    pub fn from_route(err: OrchestratorError) -> Self {
        let is_bad_request = match &err {
            OrchestratorError::UnsortableField { .. }
            | OrchestratorError::UnrunnableQuery { .. } => true,
            // `InvalidData` as well as `InvalidInput`: every producer of the former is a
            // document the caller sent that the schema refuses — a missing inner `id`, a type
            // that does not match a declared field. Those answered `500 Internal server error`,
            // which is both wrong about whose fault it is and an instruction to retry something
            // that cannot succeed.
            OrchestratorError::Io(io) => matches!(
                io.kind(),
                std::io::ErrorKind::InvalidInput | std::io::ErrorKind::InvalidData
            ),
            // A value the field's type cannot hold is the document's fault too, and the text
            // classification below would read "invalid value for field" as neither a 404 nor a
            // parse error and answer 500.
            OrchestratorError::Storage(storage::StoreError::InvalidFieldValue { .. }) => true,
            _ => false,
        };

        if is_bad_request {
            Self::bad_request(err.to_string())
        } else {
            Self::from(err)
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
