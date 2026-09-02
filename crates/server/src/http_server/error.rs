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

    /// 503 with an explicit, client-safe message, for a state the caller should retry.
    ///
    /// The status is set explicitly so the message survives: an error that falls through with
    /// no status answers `500` with the text masked, which is right for an internal fault and
    /// wrong for a condition the caller is meant to understand and retry.
    pub fn service_unavailable(msg: impl Into<String>) -> Self {
        Self {
            error: anyhow::anyhow!("{}", msg.into()),
            status: Some(StatusCode::SERVICE_UNAVAILABLE),
        }
    }

    /// 404 with an explicit, client-safe message.
    pub fn not_found(msg: impl Into<String>) -> Self {
        Self {
            error: anyhow::anyhow!("{}", msg.into()),
            status: Some(StatusCode::NOT_FOUND),
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
    ///
    /// Every classification here is on the error's type. It used to fall through to reading the
    /// message text, which cannot tell a caller's mistake from the node's: any error whose text
    /// contained "not found" answered 404, so a write that failed because a *shard* was missing
    /// told the caller their index did not exist, and any text containing "parse" answered 400
    /// "Invalid query format" whatever it was really about.
    pub fn from_route(err: OrchestratorError) -> Self {
        // The one thing that is actually absent rather than broken.
        if let OrchestratorError::Storage(storage::StoreError::IndexNotFound(_)) = &err {
            return Self::not_found(err.to_string());
        }

        // Neither the caller's fault nor a fault at all: the cluster is not whole enough to
        // agree on a schema for an index nobody here has seen. `503` says retry, which is the
        // correct instruction — a `500` would mask the reason and invite the caller to treat a
        // recoverable state as a defect.
        if let OrchestratorError::SchemaUnconfirmed { .. } = &err {
            return Self::service_unavailable(err.to_string());
        }

        let is_bad_request = match &err {
            OrchestratorError::UnsortableField { .. }
            | OrchestratorError::UnrunnableQuery { .. } => true,
            // A query no shard could run carries its own verdict: the shards refused what was
            // asked, or they failed. Only the first is the caller's to fix, and the text
            // classification below cannot tell them apart — it would read "field not found: x"
            // as a missing resource and answer 404.
            OrchestratorError::NoShardAnswered { caller_error, .. } => *caller_error,
            // `InvalidData` as well as `InvalidInput`: every producer of the former is a
            // document the caller sent that the schema refuses — a missing inner `id`, a type
            // that does not match a declared field. Those answered `500 Internal server error`,
            // which is both wrong about whose fault it is and an instruction to retry something
            // that cannot succeed.
            OrchestratorError::Io(io) => matches!(
                io.kind(),
                std::io::ErrorKind::InvalidInput | std::io::ErrorKind::InvalidData
            ),
            // A value the field's type cannot hold is the document's fault too, as is a query
            // that will not parse and a name no index may have.
            OrchestratorError::Storage(
                storage::StoreError::InvalidFieldValue { .. }
                | storage::StoreError::QueryParser(_)
                | storage::StoreError::InvalidIndexName(_),
            ) => true,
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

        // A handler that knows whose fault an error is says so; anything else is this node's
        // problem. Guessing from the message text is what this replaced, and it guessed wrong in
        // both directions — see `from_route`, which classifies on the error's type instead.
        let (status, message) = match self.status {
            Some(status) => (status, error_msg.as_str()),
            None => (StatusCode::INTERNAL_SERVER_ERROR, "Internal server error"),
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

        // `details` carries the real message for a client error, where the caller is the one who
        // has to act on it. For a server error it carried this node's internal error text —
        // precisely what `message` is masked to withhold — so the mask was undone by the field
        // printed beside it. A 5xx answers with the mask alone now; the text went to the `error!`
        // above, which is where an operator reads it and a caller does not.
        let body = if status.is_server_error() {
            serde_json::json!({ "error": message })
        } else {
            serde_json::json!({ "error": message, "details": error_msg })
        };

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
