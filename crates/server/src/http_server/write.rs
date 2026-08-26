//! Getting documents in: one at a time, in bulk, and as an NDJSON stream — and taking one out.

use axum::{
    Json,
    body::Body,
    extract::{Path, Query, State},
    http::{HeaderValue, header},
    response::Response,
};
use bytes::BytesMut;
use futures::StreamExt;
use serde::Deserialize;
use serde_json::Value as JsonValue;
use tracing::{debug, info, warn};

use crate::cluster_coordinator::OperationType;
use crate::http_server::error::AppError;
use crate::node_orchestrator::{ClientOp, DocPayload};
use crate::state::AppState;

/// Handler for document write operations
pub(super) async fn write_handler(
    Path(index): Path<String>,
    State(state): State<AppState>,
    Json(payload): Json<DocPayload>,
) -> Result<Json<JsonValue>, AppError> {
    debug!("Write request - index: {}, doc_id: {}", index, payload.id);

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

/// What a delete names: the document, and optionally the shard.
#[derive(Deserialize)]
pub(super) struct DeleteDocumentParams {
    id: String,
    /// Required only where the index routes by a field that is not the document key; see
    /// `effective_delete_routing_key`, which is where a missing one is refused.
    #[serde(default)]
    routing_key: Option<String>,
}

/// Handler for removing one document by its key.
///
/// The id travels in the query string rather than in a path segment or a body, and both
/// alternatives were considered and rejected. `DELETE /api/{index}/document/{id}` would need a
/// second placeholder in `authz::match_pattern` — the matcher every request is classified by —
/// and an id is an arbitrary string, so authz reading the raw path while this handler reads the
/// decoded one means an id containing `%2F` is two different documents to the two of them. A body
/// on `DELETE` is the part proxies drop. Deleting an index already carries its parameters in the
/// query, so this is the shape the API already has.
pub(super) async fn delete_document_handler(
    Path(index): Path<String>,
    State(state): State<AppState>,
    Query(params): Query<DeleteDocumentParams>,
) -> Result<Json<JsonValue>, AppError> {
    let DeleteDocumentParams { id, routing_key } = params;
    debug!("Delete request - index: {}, doc_id: {}", index, id);

    if id.trim().is_empty() {
        return Err(AppError::bad_request(
            "query parameter 'id' is required and cannot be empty",
        ));
    }

    // As on a write, the key doubles as the routing hint so the operation is unicast rather than
    // broadcast. Where the index routes by something other than the key this hint is wrong, and
    // the schema-aware refusal downstream is what catches that — it is the only place the schema
    // is in hand without paying for an extra lookup on every delete.
    let effective_routing_key = routing_key.clone().or_else(|| Some(id.clone()));

    let client_op = ClientOp::Delete {
        index,
        id,
        routing_key,
    };

    let result = state
        .router
        .route_and_handle(client_op, effective_routing_key, OperationType::Write)
        .await
        .map_err(AppError::from_route)?;
    Ok(Json(result))
}

/// Handler for bulk write operations
pub(super) async fn bulk_write_handler(
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
pub(super) async fn write_stream_handler(
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
    let max_record_size_bytes = state.max_record_size_bytes;

    while let Some(chunk_result) = body_stream.next().await {
        let chunk = chunk_result.map_err(|e| {
            AppError::bad_request(format!("Failed to read request body chunk: {}", e))
        })?;

        buf.extend_from_slice(&chunk);

        // A line only leaves `buf` when its newline arrives, so an unterminated line would
        // otherwise buffer the entire request allowance before any limit was consulted.
        // Rejecting here keeps peak memory bounded by the record size, not the body size.
        if buf.len() > max_record_size_bytes {
            return Err(AppError::payload_too_large(format!(
                "document on line {} exceeds the {} MB single-record limit",
                total_line_count + 1,
                max_record_size_bytes / (1024 * 1024)
            )));
        }

        // Process all complete lines in the buffer
        while let Some(newline_pos) = buf.iter().position(|&b| b == b'\n') {
            let line = buf.split_to(newline_pos + 1);
            let line = &line[..line.len() - 1]; // trim trailing newline
            if line.is_empty() {
                continue;
            }

            total_line_count += 1;
            let doc_payload: DocPayload = serde_json::from_slice(line).map_err(|e| {
                AppError::bad_request(format!(
                    "Failed to parse document on line {}: {}",
                    total_line_count, e
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
                AppError::bad_request(format!(
                    "Failed to parse document on line {}: {}",
                    total_line_count, e
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
        return Err(AppError::bad_request("No documents found in request body"));
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
        AppError::from(anyhow::anyhow!(
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
pub(super) async fn flush_write_batch(
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
