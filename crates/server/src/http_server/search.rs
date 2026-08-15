//! Reading documents out: one search, and the streaming form of it.

use axum::{
    Json,
    body::Body,
    extract::{Path, State},
    http::{HeaderValue, header},
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use tracing::{debug, info};

use crate::cluster_coordinator::OperationType;
use crate::http_server::error::AppError;
use crate::node_orchestrator::ClientOp;
use crate::query::parse_query_keywords;
use crate::state::AppState;
use storage::SortSpec;

/// Search request payload
#[derive(Debug, Deserialize)]
pub struct SearchPayload {
    pub query: String,
    pub limit: Option<usize>,
    /// How many ordered hits to skip before the first one returned (paging offset).
    pub offset: Option<usize>,
    /// Optional list of fields to return (field projection)
    pub fields: Option<Vec<String>>,
    /// Optional sort specification
    pub sort: Option<SortSpec>,
}

/// Handler for standard search operations
pub(super) async fn search_handler(
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

    // `debug`, not `info`: one formatted line per search at the level operators run, and it
    // puts the caller's query text in the log — the request span already carries the route.
    debug!(
        "Search request - index: {}, query: {}, limit: {:?}, fields: {:?}",
        index, cleaned_query, final_limit, final_fields
    );

    // Only the audit layer wants this, and only when configured to keep it — a clone of
    // every query string on every search would otherwise be pure cost.
    let audited_query = state
        .audit
        .records_query_text()
        .then(|| cleaned_query.clone());

    let client_op = ClientOp::Search {
        index,
        query: cleaned_query,
        limit: final_limit,
        offset: payload.offset,
        fields: final_fields,
        sort: final_sort,
    };

    let result = state
        .router
        .route_and_handle(client_op, None, OperationType::Read)
        .await?;

    // Handed to the auth middleware on the way out. It writes the request's single audit
    // record and cannot read a body — by the time it runs again this handler has consumed
    // it — so the one thing it cannot discover for itself travels on the response.
    let mut response = Json(result).into_response();
    if let Some(query) = audited_query {
        response
            .extensions_mut()
            .insert(crate::authz::AuditedQuery(query));
    }
    Ok(response)
}

/// Handler for streaming search operations
///
/// Uses `route_and_handle_stream` to obtain a bounded `mpsc::Receiver` that
/// yields individual NDJSON lines (one per hit, plus a `_footer` metadata line).
/// The receiver is wrapped into an axum `Body` stream so the HTTP response
/// starts as soon as the first hit is ready, and each subsequent hit is flushed
/// incrementally. This avoids buffering the entire result set in memory.
pub(super) async fn search_stream_handler(
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
