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
use crate::node_orchestrator::{ClientOp, SearchWindow};
use crate::query::parse_query_keywords;
use crate::state::AppState;
use storage::SortSpec;

/// Search request payload
#[derive(Debug, Deserialize)]
pub struct SearchPayload {
    pub query: String,
    pub limit: Option<usize>,
    /// How many ordered hits to skip before the first one returned (paging offset).
    ///
    /// Accepted by `_search` and refused by `_search/stream`, which has no page to take — see
    /// [`search_stream_handler`].
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
    // Parse query string for embedded limit/offset/return/sort keywords
    let inline = parse_query_keywords(&payload.query);
    let cleaned_query = inline.query;

    // Explicit payload fields override parsed values
    let final_limit = payload.limit.or(inline.limit);
    let final_offset = payload.offset.or(inline.offset);
    let final_fields = payload.fields.or(inline.fields);
    let final_sort = payload.sort.or(inline.sort);

    // The ceiling is enforced here, not only on the MCP tools. `max_search_limit` is the node's
    // bound on how much one search may fetch, and the HTTP surface can ask for exactly the same
    // work — a deep `offset` more cheaply than a large `limit`, since it looks like a request
    // for ten documents. An unbounded window is an allocation the caller chooses the size of.
    let window = SearchWindow::checked(
        final_limit,
        final_offset,
        state.router.default_search_limit(),
        state.max_search_limit,
    )
    .map_err(AppError::bad_request)?;

    // `debug`, not `info`: one formatted line per search at the level operators run, and it
    // puts the caller's query text in the log — the request span already carries the route.
    debug!(
        "Search request - index: {}, query: {}, limit: {}, offset: {}, fields: {:?}",
        index, cleaned_query, window.limit, window.offset, final_fields
    );

    // Only the audit layer wants this, and only when configured to keep it — a clone of
    // every query string on every search would otherwise be pure cost.
    let audited_query = state
        .audit
        .records_query_text()
        .then(|| cleaned_query.clone());

    // The resolved window travels, rather than the caller's two `Option`s: the default and the
    // bound have been applied once, here, and every layer below reads the same numbers back.
    let client_op = ClientOp::Search {
        index,
        query: cleaned_query,
        limit: Some(window.limit),
        offset: Some(window.offset),
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
///
/// An `offset` is refused rather than ignored. A stream delivers the whole result as it is
/// produced, so there is no page to take — and a caller that paged over this route would
/// receive the first page every time, with nothing in the response saying so. Refusing costs
/// that caller one error; ignoring costs them a silently wrong answer per request.
pub(super) async fn search_stream_handler(
    Path(index): Path<String>,
    State(state): State<AppState>,
    Json(payload): Json<SearchPayload>,
) -> Result<Response, AppError> {
    // Parse query string for embedded limit/return/sort keywords
    let inline = parse_query_keywords(&payload.query);
    let cleaned_query = inline.query;

    // Explicit payload fields override parsed values
    let final_limit = payload.limit.or(inline.limit);
    let final_fields = payload.fields.or(inline.fields);
    let final_sort = payload.sort.or(inline.sort);

    if let Some(offset) = payload.offset.or(inline.offset).filter(|off| *off > 0) {
        return Err(AppError::bad_request(format!(
            "offset {offset} cannot be used with a streaming search: the stream carries every \
             hit as it is produced, so there is no page to skip to. Use POST \
             /api/{index}/search for a page, or read the stream and skip what you do not want."
        )));
    }

    // The limit still applies, and the same ceiling bounds it.
    let window = SearchWindow::checked(
        final_limit,
        None,
        state.router.default_search_limit(),
        state.max_search_limit,
    )
    .map_err(AppError::bad_request)?;

    info!(
        "Stream request - index: {}, query: {}, limit: {}, fields: {:?}",
        index, cleaned_query, window.limit, final_fields
    );

    let client_op = ClientOp::Stream {
        index,
        query: cleaned_query,
        limit: Some(window.limit),
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
