//! The error type every handler returns, and how it becomes a response.

use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use tracing::{error, warn};

use crate::node_orchestrator::{OrchestratorError, RemoteVerdict};

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

    /// Answer an error the routing layer returned, according to its verdict.
    ///
    /// The classification itself lives on [`OrchestratorError::verdict`], not here, because it is
    /// needed in two places: this one, and the wire form that carries a peer's error home. Two
    /// copies would drift, and the drift would be invisible — a routed request answering
    /// differently from a local one for the same reason.
    ///
    /// So a verdict reached on another node arrives intact: a document a peer's schema refuses is
    /// the caller's `400` whether the shard that refused it was local or a hop away, and a
    /// cluster that cannot agree a schema is a `503` either way. Before this, everything a peer
    /// raised arrived as an unclassified `Io` and answered `500`.
    pub fn from_route(err: OrchestratorError) -> Self {
        match err.verdict() {
            RemoteVerdict::NotFound => Self::not_found(err.to_string()),
            RemoteVerdict::BadRequest => Self::bad_request(err.to_string()),
            RemoteVerdict::Unavailable => Self::service_unavailable(err.to_string()),
            // Addressed to the node that forwarded the write, not to a client, and that node
            // resends with the schema rather than passing this on. Reaching here at all means
            // the resend was not possible — an older peer, or an op that carries no document —
            // so it is a `503` for the same reason `Unavailable` is: nothing about the request
            // is wrong and retrying is the right move.
            RemoteVerdict::SchemaRequired => Self::service_unavailable(err.to_string()),
            // No explicit status, so `into_response` masks the text and logs it. The caller
            // learns nothing useful from this node's internals; the operator reads them.
            RemoteVerdict::ServerFault => Self::from(err),
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
