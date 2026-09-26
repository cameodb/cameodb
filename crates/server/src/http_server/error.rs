//! The error type every handler returns, and how it becomes a response.

use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use tracing::{error, warn};

use crate::node::{OrchestratorError, RemoteVerdict};

/// Application error wrapper for consistent error handling.
///
/// `status` short-circuits the string-sniffing classification below. Handlers
/// that already know the correct HTTP status should set it explicitly rather
/// than relying on the error text.
#[derive(Debug)]
pub struct AppError {
    pub error: anyhow::Error,
    pub status: Option<StatusCode>,
    /// Seconds for the `Retry-After` a 503 answers with. `None` means "no better number
    /// than the default": a refusal that knows its own backlog — `Overloaded`, which carries
    /// the predicted wait — sets it so a retrying client is told when the node expects to
    /// serve again rather than a constant that could send it back into the same backlog.
    pub retry_after_secs: Option<u64>,
    /// The node declined work it could not serve in time — admission or the dequeue check —
    /// rather than failing at it. Logged at `DEBUG` rather than `ERROR`: under overload it is
    /// the most frequent answer the node gives, and `ShedLog` summarises it (ROADMAP OB15).
    pub shed: bool,
    /// The answer is "a peer this node already knows is lost": a request for its keys, refused
    /// at once. Logged at `DEBUG` — the loss itself was reported once, as a `WARN` when the peer
    /// was lost and in health's `connected_nodes` and `ping_failures`, and one `ERROR` per
    /// request for it is the per-refusal logging OB15 measured costing half the goodput.
    pub quiet: bool,
}

impl AppError {
    /// 400 with an explicit, client-safe message.
    pub fn bad_request(msg: impl Into<String>) -> Self {
        Self {
            error: anyhow::anyhow!("{}", msg.into()),
            status: Some(StatusCode::BAD_REQUEST),
            retry_after_secs: None,
            shed: false,
            quiet: false,
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
            retry_after_secs: None,
            shed: false,
            quiet: false,
        }
    }

    /// 403 with an explicit, client-safe message.
    pub fn forbidden(msg: impl Into<String>) -> Self {
        Self {
            error: anyhow::anyhow!("{}", msg.into()),
            status: Some(StatusCode::FORBIDDEN),
            retry_after_secs: None,
            shed: false,
            quiet: false,
        }
    }

    /// 404 with an explicit, client-safe message.
    pub fn not_found(msg: impl Into<String>) -> Self {
        Self {
            error: anyhow::anyhow!("{}", msg.into()),
            status: Some(StatusCode::NOT_FOUND),
            retry_after_secs: None,
            shed: false,
            quiet: false,
        }
    }

    /// 429 for a caller that has spent its rate budget, with the wait the limiter computed.
    ///
    /// The wait goes in both the message and the `Retry-After` header. The header is what a
    /// client library obeys without being written to understand this node's prose, and the
    /// limiter's whole contract is that obeying it works — `waiting_the_advertised_time_earns_
    /// another_call` in `ratelimit` is the arithmetic, and it is worth nothing to a caller that
    /// was never told the number in a form it reads.
    ///
    /// There is no variant without a wait: every refusal this node raises comes from a token
    /// bucket, and a bucket always knows when its next token lands.
    pub fn too_many_requests_in(msg: impl Into<String>, retry_after_secs: u64) -> Self {
        let msg = msg.into();
        Self {
            error: anyhow::anyhow!("{msg} Retry after {retry_after_secs}s."),
            status: Some(StatusCode::TOO_MANY_REQUESTS),
            retry_after_secs: Some(retry_after_secs),
            shed: false,
            quiet: false,
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
        // An admission refusal knows the number it refused on: the predicted wait is the
        // honest answer to "when should I come back", and it is what the door's own 503
        // carries as Retry-After.
        let retry_after_secs = match err {
            OrchestratorError::Overloaded {
                predicted_wait_ms, ..
            } => Some(predicted_wait_ms.div_ceil(1000).max(1)),
            _ => None,
        };
        let shed = matches!(
            err,
            OrchestratorError::Overloaded { .. } | OrchestratorError::ReadDeadlineExpired { .. }
        );
        let quiet = matches!(err, OrchestratorError::PeerUnreachable { .. });
        let mut app = match err.verdict() {
            RemoteVerdict::NotFound => Self::not_found(err.to_string()),
            RemoteVerdict::BadRequest => Self::bad_request(err.to_string()),
            RemoteVerdict::Unavailable => Self::service_unavailable(err.to_string()),
            // Addressed to the node that forwarded the write, not to a client, and that node
            // resends with the schema rather than passing this on. Reaching here at all means
            // the resend was not possible — an older peer, or an op that carries no document —
            // so it is a `503` for the same reason `Unavailable` is: nothing about the request
            // is wrong and retrying is the right move.
            RemoteVerdict::SchemaRequired => Self::service_unavailable(err.to_string()),
            // The tenant is at a ceiling. The message names which one and how to make room, so
            // it is shown rather than masked — it is the caller's to act on.
            RemoteVerdict::QuotaExceeded => Self::forbidden(err.to_string()),
            // No explicit status, so `into_response` masks the text and logs it. The caller
            // learns nothing useful from this node's internals; the operator reads them.
            RemoteVerdict::ServerFault => Self::from(err),
        };
        app.retry_after_secs = retry_after_secs;
        app.shed = shed;
        app.quiet = quiet;
        app
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

        // Log at appropriate level: DEBUG for 404 (expected) and for a refusal — shed work
        // and a spent rate allowance, which `ShedLog` summarises rather than logging per
        // request — WARN for client errors, ERROR for server errors.
        match status {
            _ if self.shed => {
                tracing::debug!("API refused: {} -> {}", status, error_msg);
            }
            _ if self.quiet => {
                tracing::debug!("API unavailable: {} -> {}", status, error_msg);
            }
            StatusCode::TOO_MANY_REQUESTS => {
                tracing::debug!("API refused: {} -> {}", status, error_msg);
            }
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

        // Every 503 this node raises means the same thing — busy, come back — and a client
        // that retries immediately deepens the overload it is retrying into. The refusal
        // carries the backlog it was decided on when it has one (`Overloaded` sets
        // `retry_after_secs` from the predicted wait); anything else retries in a second.
        //
        // A 429 is the same bargain made by the rate limiter, which always knows its number:
        // `too_many_requests_in` carries it, and a client that obeys the header is admitted.
        if status == StatusCode::SERVICE_UNAVAILABLE || status == StatusCode::TOO_MANY_REQUESTS {
            let retry_after = self.retry_after_secs.unwrap_or(1).to_string();
            return (
                status,
                [(axum::http::header::RETRY_AFTER, retry_after)],
                Json(body),
            )
                .into_response();
        }

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
            retry_after_secs: None,
            shed: false,
            quiet: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A refusal that knows its backlog advises when it clears: the `Retry-After` is the
    /// predicted wait rounded up, not the one-second default every other 503 carries.
    #[test]
    fn an_overload_refusal_advises_when_the_backlog_clears() {
        let err = AppError::from_route(OrchestratorError::Overloaded {
            predicted_wait_ms: 2500,
            budget_ms: 1000,
        });
        assert_eq!(err.retry_after_secs, Some(3));

        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            response.headers().get(axum::http::header::RETRY_AFTER),
            Some(&axum::http::HeaderValue::from_static("3")),
        );
    }

    /// Admission and the dequeue check are the node declining work, not failing at it, so
    /// they are logged quietly and summarised. A 503 for anything else — a peer that cannot
    /// be reached, a schema the cluster cannot agree — is a condition an operator has to see,
    /// and keeps its `ERROR` line.
    #[test]
    fn only_shed_work_is_marked_as_shed() {
        let overloaded = AppError::from_route(OrchestratorError::Overloaded {
            predicted_wait_ms: 900,
            budget_ms: 1000,
        });
        let abandoned = AppError::from_route(OrchestratorError::ReadDeadlineExpired {
            waited_ms: 1200,
            budget_ms: 1000,
        });
        let unreachable = AppError::from_route(OrchestratorError::PeerUnreachable {
            message: "peer gone".to_string(),
        });
        assert!(overloaded.shed && abandoned.shed);
        assert!(!unreachable.shed);
        assert_eq!(unreachable.status, Some(StatusCode::SERVICE_UNAVAILABLE));
        assert!(!AppError::service_unavailable("schema unavailable").shed);
    }

    /// A request for a lost peer's keys is refused at once and often — every write it owns, for
    /// as long as it is gone — so it is logged quietly, not as an `ERROR` each. Load shedding is
    /// not what happened, so it stays out of `ShedLog`'s summary; nothing else goes quiet.
    #[test]
    fn a_request_for_a_lost_peer_is_answered_quietly_and_is_not_shedding() {
        let unreachable = AppError::from_route(OrchestratorError::PeerUnreachable {
            message: "the node that owns this key is not reachable".to_string(),
        });
        assert!(unreachable.quiet);
        assert!(!unreachable.shed);
        assert_eq!(unreachable.status, Some(StatusCode::SERVICE_UNAVAILABLE));
        let overloaded = AppError::from_route(OrchestratorError::Overloaded {
            predicted_wait_ms: 900,
            budget_ms: 1000,
        });
        assert!(!overloaded.quiet);
        assert!(!AppError::service_unavailable("schema unavailable").quiet);
    }

    /// Every other 503 still advises the fixed second — only a refusal carrying the backlog
    /// it was decided on can say more.
    #[test]
    fn a_plain_503_retries_after_one_second() {
        let err = AppError::service_unavailable("peer unreachable");
        assert_eq!(err.retry_after_secs, None);
        let response = err.into_response();
        assert_eq!(
            response.headers().get(axum::http::header::RETRY_AFTER),
            Some(&axum::http::HeaderValue::from_static("1")),
        );
    }
}
