//! Liveness and readiness.

use axum::{
    Extension, Json,
    extract::State,
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::error;

/// How long the health endpoint will wait on actor queries that can queue behind real work.
/// These only affect the expanded body; the liveness status is always fast.
const HEALTH_ACTOR_TIMEOUT: Duration = Duration::from_secs(5);

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

    // Read-pool saturation gauge: reads executing now, and the pool's blocking width. A read
    // approaching the second is a node shedding read load; equal and stuck is what turns it red.
    pub read_pool_in_flight: usize,
    pub read_pool_capacity: usize,

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

    // The two ways a shard's data path stops serving while the request path stays up: a writer
    // that has died or wedged mid-batch (no more writes), and a read pool with every thread stuck
    // and no progress (no more reads). Either should make an orchestrator stop routing here and
    // recycle the node, so either forces red whatever the cluster view says. This is the only part
    // of the anonymous response that touches local node state, and it is a load of a handful of
    // atomics — it never reaches the read pool or a shard's writer, so it cannot itself stall.
    let status = worst_status(
        status,
        state.writer_liveness.unavailable_writers(),
        state.read_pool_health.is_wedged(),
    );

    if !identified {
        // Still the *real* status, not a constant: a health check that cannot go yellow is
        // not a health check, and this is what a load balancer reads.
        return Ok(Json(serde_json::json!({ "status": status })).into_response());
    }

    let (read_pool_in_flight, read_pool_capacity) = state.read_pool_health.gauge();

    // Get basic shard count and node info from orchestrator. These can queue behind real work,
    // so the expanded body uses bounded waits; on timeout we fall back to defaults rather than
    // let a slow node fail its own health probe.
    let shard_count =
        match tokio::time::timeout(HEALTH_ACTOR_TIMEOUT, state.router.shard_count()).await {
            Ok(count) => count,
            Err(_) => {
                error!("health actor timeout: shard_count");
                0
            }
        };
    let (node_id, node_name) = match tokio::time::timeout(
        HEALTH_ACTOR_TIMEOUT,
        state.router.handle_client_op(ClientOp::GetIdentity),
    )
    .await
    {
        Ok(Ok(result)) => {
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
        Ok(Err(_)) | Err(_) => {
            error!("health actor timeout or error: GetIdentity");
            ("local".to_string(), "unknown".to_string())
        }
    };

    // Get index statistics for health check
    let (total_indexes, indexes_with_data) = match tokio::time::timeout(
        HEALTH_ACTOR_TIMEOUT,
        state.router.handle_client_op(ClientOp::ListIndexes {
            include_data_size: false,
        }),
    )
    .await
    {
        Ok(Ok(result)) => {
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
        Ok(Err(_)) | Err(_) => {
            error!("health actor timeout or error: ListIndexes");
            (0, 0) // Fallback to 0 if index listing fails
        }
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
        read_pool_in_flight,
        read_pool_capacity,
        dial_failures: cluster_status.as_ref().map(|s| s.dial_failures),
        bootstrap_successes: cluster_status.as_ref().map(|s| s.bootstrap_successes),
        routing_updates: cluster_status.as_ref().map(|s| s.routing_updates),
    };

    Ok(Json(response).into_response())
}

/// Fold local data-path liveness into the cluster health string.
///
/// A shard whose writer thread has died or wedged mid-batch can accept no more writes, and a read
/// pool with every thread stuck can answer no more reads — either is a reason for an orchestrator
/// to stop routing here and recycle the node, so either forces `red` whatever the cluster view
/// reported. With every writer serving and the read pool making progress, the cluster status
/// stands. Saturation alone is not folded in: a busy-but-draining pool is doing its job.
fn worst_status(
    cluster_status: String,
    writers_unavailable: usize,
    read_pool_wedged: bool,
) -> String {
    if writers_unavailable > 0 || read_pool_wedged {
        "red".to_string()
    } else {
        cluster_status
    }
}

#[cfg(test)]
mod tests {
    use super::worst_status;

    #[test]
    fn an_unavailable_writer_forces_red_over_any_cluster_status() {
        assert_eq!(worst_status("green".to_string(), 1, false), "red");
        assert_eq!(worst_status("yellow".to_string(), 2, false), "red");
        assert_eq!(worst_status("red".to_string(), 1, false), "red");
    }

    #[test]
    fn a_wedged_read_pool_forces_red_over_any_cluster_status() {
        assert_eq!(worst_status("green".to_string(), 0, true), "red");
        assert_eq!(worst_status("yellow".to_string(), 0, true), "red");
    }

    #[test]
    fn a_healthy_data_path_lets_the_cluster_status_stand() {
        assert_eq!(worst_status("green".to_string(), 0, false), "green");
        assert_eq!(worst_status("yellow".to_string(), 0, false), "yellow");
    }
}
