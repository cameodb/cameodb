//! The operator endpoints under `/_admin`.
//!
//! The HTTP surface over `crate::admin`, which is a different module: this one is reached as
//! `http_server::admin` and holds handlers, that one holds the work they report on.

use axum::{
    Json,
    extract::{Path, Query, State},
};
use serde::Deserialize;
use serde_json::Value as JsonValue;

use crate::http_server::error::AppError;
use crate::node_orchestrator::{
    AdminIndexCommitReport, AdminIndexEvictWriterReport, AdminMemoryReport, WorkerPoolReport,
};
use crate::state::AppState;

pub(super) async fn admin_memory_handler(
    State(state): State<AppState>,
) -> Result<Json<AdminMemoryReport>, AppError> {
    Ok(Json(state.router.admin_memory().await?))
}

pub(super) async fn admin_memory_purge_handler(
    State(state): State<AppState>,
    Query(params): Query<AdminPurgeParams>,
) -> Result<Json<AdminMemoryReport>, AppError> {
    Ok(Json(state.router.admin_purge_memory(params.force).await?))
}

pub(super) async fn admin_index_commit_handler(
    Path(index): Path<String>,
    State(state): State<AppState>,
) -> Result<Json<AdminIndexCommitReport>, AppError> {
    Ok(Json(state.router.admin_commit_index(index).await?))
}

pub(super) async fn admin_index_evict_writer_handler(
    Path(index): Path<String>,
    State(state): State<AppState>,
) -> Result<Json<AdminIndexEvictWriterReport>, AppError> {
    Ok(Json(state.router.admin_evict_index_writer(index).await?))
}

pub(super) async fn admin_workers_handler(
    State(state): State<AppState>,
) -> Result<Json<WorkerPoolReport>, AppError> {
    Ok(Json(state.router.admin_worker_stats()?))
}

/// `GET /_admin/audit?limit=N` — the most recent audit records, newest first.
///
/// The point of a live view rather than only a file: the question "who has been reading the
/// payroll index" gets an answer from a node that may not have a file sink configured, and
/// gets it without shell access to wherever that file lives.
///
/// `dropped` is reported alongside the records because a trail that lost entries has to say
/// so where it is *read*, not only where it is written — an operator scrolling this endpoint
/// would otherwise have no way to know the window they are looking at is incomplete.
pub(super) async fn admin_audit_handler(
    State(state): State<AppState>,
    Query(params): Query<AuditQueryParams>,
) -> Json<JsonValue> {
    // Bounded regardless of what was asked for: this response is built in memory, and an
    // admin endpoint that will serialize an unbounded slice on request is a way to make a
    // node fall over using a credential that was only meant to inspect it.
    let limit = params.limit.unwrap_or(100).min(1000);
    let records = state.audit.recent(limit);
    Json(serde_json::json!({
        "enabled": state.audit.is_enabled(),
        "dropped": state.audit.dropped(),
        "count": records.len(),
        "records": records,
    }))
}

#[derive(Debug, Deserialize)]
pub(super) struct AuditQueryParams {
    limit: Option<usize>,
}

#[derive(Deserialize, Default)]
pub(super) struct AdminPurgeParams {
    force: bool,
}
