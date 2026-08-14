//! Liveness and readiness.

use axum::{
    Extension, Json,
    extract::State,
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use tracing::error;

use crate::authz::Authz;
use crate::cluster_coordinator::GetStatus;
use crate::http_server::error::AppError;
use crate::node_orchestrator::ClientOp;
use crate::state::AppState;

/// Liveness endpoint path. Exempt from the concurrency guard so that an overloaded node
/// still reports its real state instead of 503-ing its own health check.
pub const HEALTH_PATH: &str = "/_cluster/health";

/// Health check response
#[derive(Debug, Serialize, Deserialize)]
pub struct HealthResponse {
    pub status: String,
    pub node_id: String,
    pub node_name: String,

    // Cluster-wide status
    pub cluster_name: Option<String>,
    pub cluster_enabled: Option<bool>,
    pub total_nodes: Option<usize>,
    pub connected_nodes: Option<usize>,
    pub cluster_total_shards: Option<usize>,

    // Local node info
    pub active_shards: usize,
    pub total_indexes: usize,
    pub indexes_with_data: usize,

    // Performance/Debug metrics
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dial_failures: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bootstrap_successes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub routing_updates: Option<u64>,
}

/// Handler for cluster health check.
///
/// The only public route, and therefore the only one where the response has to depend on who
/// is asking. An anonymous caller gets liveness and nothing else: node identity, cluster
/// size, peer counts and index counts are a free reconnaissance report for anyone who can
/// reach the port, and a load balancer needs none of it. Presenting any valid key — every
/// role holds `Read` — restores the full body.
///
/// A missing [`Authz`] extension means the auth layer is not in the stack, and the minimal
/// body is the right answer to that too.
pub(super) async fn health_handler(
    State(state): State<AppState>,
    authz: Option<Extension<Authz>>,
) -> Result<Response, AppError> {
    let identified = authz.is_some_and(|Extension(authz)| authz.is_identified());

    // Query cluster status from coordinator
    let cluster_status = match state.coordinator.ask(GetStatus).await {
        Ok(status) => Some(status),
        Err(err) => {
            error!(error = ?err, "Failed to get cluster status from coordinator");
            None
        }
    };

    let status = cluster_status
        .as_ref()
        .map(|s| s.health.clone())
        .unwrap_or_else(|| "green".to_string());

    if !identified {
        // Still the *real* status, not a constant: a health check that cannot go yellow is
        // not a health check, and this is what a load balancer reads.
        return Ok(Json(serde_json::json!({ "status": status })).into_response());
    }

    // Get basic shard count and node info from orchestrator
    let shard_count = state.router.shard_count().await;
    let (node_id, node_name) = match state.router.handle_client_op(ClientOp::GetIdentity).await {
        Ok(result) => {
            let node_id = result
                .get("node_id")
                .and_then(|v| v.as_str())
                .unwrap_or("local")
                .to_string();
            let node_name = result
                .get("node_name")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                .to_string();
            (node_id, node_name)
        }
        Err(_) => ("local".to_string(), "unknown".to_string()),
    };

    // Get index statistics for health check
    let (total_indexes, indexes_with_data) = match state
        .router
        .handle_client_op(ClientOp::ListIndexes {
            include_data_size: false,
        })
        .await
    {
        Ok(result) => {
            let total = result
                .get("total_indexes")
                .and_then(|v| v.as_u64())
                .unwrap_or(0) as usize;
            let empty_vec = vec![];
            let indexes_array = result
                .get("indexes")
                .and_then(|arr| arr.as_array())
                .unwrap_or(&empty_vec);
            let with_data = indexes_array
                .iter()
                .filter(|idx| {
                    idx.get("document_count")
                        .and_then(|c| c.as_u64())
                        .unwrap_or(0)
                        > 0
                })
                .count();
            (total, with_data)
        }
        Err(_) => (0, 0), // Fallback to 0 if index listing fails
    };

    let response = HealthResponse {
        status,
        node_id,
        node_name,
        cluster_name: cluster_status.as_ref().map(|s| s.cluster_name.clone()),
        cluster_enabled: cluster_status.as_ref().map(|s| s.cluster_enabled),
        total_nodes: cluster_status.as_ref().map(|s| s.total_nodes),
        connected_nodes: cluster_status.as_ref().map(|s| s.connected_nodes),
        cluster_total_shards: cluster_status.as_ref().map(|s| s.total_shards),
        active_shards: shard_count,
        total_indexes,
        indexes_with_data,
        dial_failures: cluster_status.as_ref().map(|s| s.dial_failures),
        bootstrap_successes: cluster_status.as_ref().map(|s| s.bootstrap_successes),
        routing_updates: cluster_status.as_ref().map(|s| s.routing_updates),
    };

    Ok(Json(response).into_response())
}
