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
use crate::node_orchestrator::{ClientOp, DeletePayload, DocPayload};
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
        // A request off the wire is the first hop by definition.
        forwarded: false,
        // And decides its own schema: nothing was settled upstream to carry.
        schema_body: None,
    };

    let result = state
        .router
        .route_and_handle(client_op, effective_routing_key, OperationType::Write)
        .await
        .map_err(AppError::from_route)?;
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
        forwarded: false,
    };

    let result = state
        .router
        .route_and_handle(client_op, effective_routing_key, OperationType::Write)
        .await
        .map_err(AppError::from_route)?;
    Ok(Json(result))
}

/// Handler for removing many documents in one request.
///
/// `POST` rather than `DELETE` because the ids are a body, and a body on `DELETE` is the part
/// proxies drop. The path keeps the `_bulk` prefix the write side uses, so the two halves of
/// bulk ingest read as a pair.
pub(super) async fn bulk_delete_handler(
    Path(index): Path<String>,
    State(state): State<AppState>,
    Json(docs): Json<Vec<DeletePayload>>,
) -> Result<Json<JsonValue>, AppError> {
    info!(
        "Bulk delete request - index: {}, ids: {}",
        index,
        docs.len()
    );

    if docs.is_empty() {
        return Err(AppError::bad_request(
            "no ids to delete: the body must be a non-empty array",
        ));
    }

    // The first id keeps the request unicast where the whole batch belongs to one shard, which
    // is the common case; anything else is grouped and forwarded by the orchestrator.
    let routing_hint = docs
        .first()
        .map(|first| first.routing_key().unwrap_or(first.id()).to_string());

    let client_op = ClientOp::BulkDelete { index, docs };

    let result = state
        .router
        .route_and_handle(client_op, routing_hint, OperationType::Write)
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

    let client_op = ClientOp::BulkWrite {
        index,
        docs,
        // A request off the wire is the first hop, and decides its own schema.
        forwarded: false,
        schema_body: None,
    };

    let result = state
        .router
        .route_and_handle(client_op, routing_hint, OperationType::Write)
        .await
        .map_err(AppError::from_route)?;

    // The response is measured by what can actually be large in it, rather than by
    // serialising it.
    //
    // This line used to call `serde_json::to_string(&result)` for its length alone — a second
    // full pass over a response axum is about to serialise itself, into a `String` dropped on
    // the next line. On a 5 000-document batch that was the largest allocation anywhere on the
    // write path, spent on a log line, on every bulk write, at `info!`.
    //
    // A bulk response is `items_written` and one reason per item that failed, so its size is
    // its reasons plus a few dozen bytes of framing. Summing the reason lengths costs nothing
    // and is what the HTTP/2 warning below was reading a whole serialisation to learn.
    let errors = result.get("errors").and_then(|v| v.as_array());
    let error_count = errors.map(|e| e.len()).unwrap_or(0);
    let reason_bytes: usize = errors
        .map(|e| {
            e.iter()
                .map(|r| r.as_str().map(str::len).unwrap_or(0))
                .sum()
        })
        .unwrap_or(0);
    info!(
        "Bulk write completed - items written: {}, errors: {}, reason bytes: {}",
        result
            .get("items_written")
            .and_then(|v| v.as_u64())
            .unwrap_or(0),
        error_count,
        reason_bytes
    );

    // If response is very large, this could cause HTTP/2 issues
    if reason_bytes > 1_000_000 {
        warn!(
            "Large bulk response ({} bytes of failure reasons across {} errors) may cause HTTP/2 issues",
            reason_bytes, error_count
        );
    }

    Ok(Json(result))
}

/// Handler for streaming document write operations (NDJSON input)
///
/// Reads the request body incrementally, splitting on newlines to decode `DocPayload` items one
/// at a time. Documents are accumulated into micro-batches of `stream_batch_size` and each batch
/// is dispatched through the normal routing path as a `BulkWrite`. This keeps peak memory bounded
/// regardless of total import size.
///
/// **A bad line is reported, not fatal.** Micro-batches commit as the body is read, so a request
/// that aborts partway has already written documents it will not account for — the caller is left
/// with a `400`, no counts, and no way to know where to resume. So a line that will not parse, or
/// that is larger than one record may be, is answered like any other refusal: a reason naming the
/// line, and the rest of the file still loaded. `_bulk` has always answered this way.
///
/// Lines are counted as they appear in the file, blanks included, so `line 41` is the line an
/// operator finds at `sed -n 41p`. Every reason names one, and `items_written` plus the reasons
/// is the number of documents the body held.
pub(super) async fn write_stream_handler(
    Path(index): Path<String>,
    State(state): State<AppState>,
    body: Body,
) -> Result<Response, AppError> {
    info!("Write stream request - index: {}", index);

    let batch_size = state.stream_batch_size.max(1);
    let max_record_size_bytes = state.max_record_size_bytes;

    let mut written: u64 = 0;
    let mut errors: Vec<String> = Vec::new();
    // Non-blank lines: the documents the body offered, whether or not any of them parsed.
    let mut documents: usize = 0;
    let mut unparseable: usize = 0;
    let mut batches: usize = 0;
    // Physical lines, so a reason names what the caller can go and look at.
    let mut line_number: usize = 0;

    let mut buf = BytesMut::new();
    let mut batch: Vec<(usize, DocPayload)> = Vec::with_capacity(batch_size);
    // Set when a line has outgrown the record limit: its bytes keep arriving after it has been
    // refused, and they are dropped until the newline that ends it.
    let mut discarding = false;

    let mut body_stream = body.into_data_stream();

    while let Some(chunk) = body_stream.next().await {
        // The body itself failed, so there is no answer to give: what was written cannot be
        // reported to a caller whose request did not finish arriving.
        let chunk = chunk.map_err(|e| {
            AppError::bad_request(format!("Failed to read request body chunk: {e}"))
        })?;
        buf.extend_from_slice(&chunk);

        loop {
            let newline = buf.iter().position(|&b| b == b'\n');

            if discarding {
                match newline {
                    Some(at) => {
                        let _ = buf.split_to(at + 1);
                        discarding = false;
                    }
                    None => {
                        buf.clear();
                        break;
                    }
                }
                continue;
            }

            let Some(at) = newline else {
                // What is left is one unterminated line, and it has already outgrown the limit
                // — the rest of it is still arriving. Checked only now, after every complete
                // line has been taken out: several ordinary lines in the buffer at once are not
                // one oversized one, and measuring the buffer before draining it called them
                // that.
                if buf.len() > max_record_size_bytes {
                    line_number += 1;
                    documents += 1;
                    errors.push(oversized_line(line_number, max_record_size_bytes));
                    buf.clear();
                    discarding = true;
                    continue;
                }
                break;
            };

            let line = buf.split_to(at + 1);
            let line = &line[..line.len() - 1];
            line_number += 1;
            if line.is_empty() {
                continue;
            }

            documents += 1;

            // The same limit, applied to a line that arrived whole. The check above only ever
            // sees a line still being received, so without this one the verdict on a given file
            // depended on where the wire happened to split it: an oversized line delivered in
            // one piece, or with its newline already buffered, was parsed and accepted while the
            // identical line delivered in two was refused.
            if line.len() > max_record_size_bytes {
                errors.push(oversized_line(line_number, max_record_size_bytes));
                continue;
            }

            match serde_json::from_slice::<DocPayload>(line) {
                Ok(doc) => batch.push((line_number, doc)),
                Err(e) => {
                    unparseable += 1;
                    errors.push(format!("line {line_number}: {e}"));
                }
            }

            if batch.len() >= batch_size {
                let flushed = std::mem::replace(&mut batch, Vec::with_capacity(batch_size));
                let (batch_written, batch_errors) = flush_lines(&state, &index, flushed).await;
                batches += 1;
                written += batch_written;
                errors.extend(batch_errors);
            }
        }
    }

    // A trailing line with no newline of its own. One that outgrew the limit was already refused
    // above, when the buffer passed it.
    if !discarding && !buf.is_empty() {
        line_number += 1;
        documents += 1;
        let line = buf.freeze();
        match serde_json::from_slice::<DocPayload>(&line) {
            Ok(doc) => batch.push((line_number, doc)),
            Err(e) => {
                unparseable += 1;
                errors.push(format!("line {line_number}: {e}"));
            }
        }
    }

    if !batch.is_empty() {
        let (batch_written, batch_errors) = flush_lines(&state, &index, batch).await;
        batches += 1;
        written += batch_written;
        errors.extend(batch_errors);
    }

    if documents == 0 {
        return Err(AppError::bad_request("No documents found in request body"));
    }

    // Nothing written and nothing that even parsed: this body was not NDJSON. Reporting a line
    // at a time would answer 200 to a request that was the wrong shape entirely, which is the
    // one thing a caller cannot tell from a partial success.
    if written == 0 && unparseable == documents {
        return Err(AppError::bad_request(format!(
            "no line of the body parsed as a document; the first was: {}",
            errors.first().map(String::as_str).unwrap_or("unknown")
        )));
    }

    // Every document the body held is written or explained, the same arithmetic `_bulk` answers
    // with. A path that stops accounting is a test failure here rather than a silent shortfall,
    // and a log line in the release build that would otherwise just serve the bad total.
    debug_assert_eq!(
        written as usize + errors.len(),
        documents,
        "a write stream must account for every document its body held"
    );
    if written as usize + errors.len() != documents {
        tracing::error!(
            index = %index,
            lines_received = documents,
            items_written = written,
            errors = errors.len(),
            "Write stream did not account for every document its body held"
        );
    }

    info!(
        "Write stream completed - index: {}, lines: {}, batches: {}, written: {}, errors: {}",
        index,
        documents,
        batches,
        written,
        errors.len()
    );

    let result = serde_json::json!({
        "status": if errors.is_empty() { "ok" } else { "partial" },
        "items_written": written,
        "lines_received": documents,
        "batches": batches,
        "errors": errors,
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

/// The one wording for a line larger than a single record may be.
///
/// Reported from two places — a line still arriving that has already outgrown the limit, and one
/// that arrived whole — which have to say the same thing about the same line.
fn oversized_line(line_number: usize, max_record_size_bytes: usize) -> String {
    format!(
        "line {line_number}: exceeds the {} MB single-record limit",
        max_record_size_bytes / (1024 * 1024)
    )
}

/// Dispatch one micro-batch and report its answer in the file's own line numbers.
///
/// The engine numbers its reasons against the batch it was handed — `document 3` of five hundred
/// — which says nothing to someone holding the file. Renumbered here, by the same function the
/// bulk path uses to renumber a peer's reasons, so `line 1503` is the line to go and fix.
async fn flush_lines(
    state: &AppState,
    index: &str,
    batch: Vec<(usize, DocPayload)>,
) -> (u64, Vec<String>) {
    let lines: Vec<usize> = batch.iter().map(|(line, _)| *line).collect();
    let docs: Vec<DocPayload> = batch.into_iter().map(|(_, doc)| doc).collect();

    match flush_write_batch(state, index, docs).await {
        Ok(answer) => {
            let written = answer
                .get("items_written")
                .and_then(|v| v.as_u64())
                .unwrap_or(0)
                .min(lines.len() as u64);
            let reasons: Vec<String> = answer
                .get("errors")
                .and_then(|v| v.as_array())
                .map(|errors| {
                    errors
                        .iter()
                        .filter_map(|e| e.as_str().map(str::to_string))
                        .collect()
                })
                .unwrap_or_default();

            let unwritten = lines.len() - written as usize;
            let renumbered = crate::node_orchestrator::renumber_reasons(
                &reasons,
                &lines,
                "line",
                unwritten,
                "batch",
                || "a document in this batch was neither written nor refused".to_string(),
            );
            (written, renumbered)
        }
        // The batch never got an answer, so none of it was written.
        Err(e) => (
            0,
            lines
                .into_iter()
                .map(|line| format!("line {line}: dispatching this batch failed: {e}"))
                .collect(),
        ),
    }
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
        forwarded: false,
        schema_body: None,
    };
    state
        .router
        .route_and_handle(client_op, routing_hint, OperationType::Write)
        .await
}
