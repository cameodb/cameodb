//! Liveness and readiness.

use axum::{
    Extension, Json,
    extract::State,
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::time::{Instant, timeout_at};
use tracing::error;

/// Ceiling on what the expanded body may spend waiting on actors, and the value used when the
/// node's request timeout is larger than anything worth waiting for.
const HEALTH_ACTOR_BUDGET_MAX: Duration = Duration::from_secs(5);

/// Floor, so a node configured with a very short timeout still gives its actors a chance to
/// answer rather than degrading every probe by arithmetic.
const HEALTH_ACTOR_BUDGET_MIN: Duration = Duration::from_millis(50);

/// What the expanded body may spend on actor round-trips — **in total, not per call**.
///
/// Both halves of that sentence were wrong before, and each on its own was enough to fail the
/// probe. The wait was a fixed 5s while the expanded body makes four sequential actor calls, so
/// the worst case was 20s; and it was compared against nothing, so on a node running
/// `request_timeout_secs = 1` every one of those calls was abandoned by `TimeoutLayer` as a 408
/// — the exact failure the fallbacks exist to prevent — a full second before its own guard
/// expired. A guard longer than the budget it is guarding cannot fire.
///
/// Half the request timeout, so a probe that has to fall back still has the other half to build
/// and serialize its answer in, clamped so neither a 1s node nor a 300s one gets an absurd
/// figure. Measured: under bulk overload this endpoint returned 408 at 1,001ms on twelve of
/// twelve probes (ROADMAP F8).
fn health_actor_budget(request_timeout: Duration) -> Duration {
    (request_timeout / 2).clamp(HEALTH_ACTOR_BUDGET_MIN, HEALTH_ACTOR_BUDGET_MAX)
}

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
    // Reads refused at dequeue since start, because they had waited longer than the request
    // timeout that asked for them. The gauge above cannot show this: refused work never runs,
    // so it occupies no thread and appears in no latency sample. A node at a healthy in-flight
    // count with this number climbing is one that is shedding, not one that is comfortable.
    pub read_pool_abandoned: u64,
    // The backlog the admission guard is refusing against: jobs queued or running across the
    // worker pool, and what the node predicts a request arriving now would wait before it
    // started. Reported because refusals are made on these two numbers, and an operator asking
    // why a node is returning 503 should be able to read the reason rather than infer it.
    // Absent on a node with no worker pool, where there is no backlog to predict.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queue_depth: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub predicted_wait_ms: Option<u64>,

    // Performance/Debug metrics
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dial_failures: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bootstrap_successes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub routing_updates: Option<u64>,

    // The actor-mailbox lane's backlog — bulk writes, config and metadata — and what it
    // predicts a request arriving now would wait. Separate from `queue_depth` above, which is
    // the worker pool's: the two lanes have different widths and service times, and a node can
    // be idle on one while shedding on the other. Absent where the lane has no gate.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mailbox_depth: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mailbox_predicted_wait_ms: Option<u64>,
    /// The lane's measured service p90, in milliseconds — how long the node's own work is
    /// taking, as opposed to how much of it is queued. Absent until a full window has closed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mailbox_service_p90_ms: Option<u64>,

    /// Which parts of this body could not be filled in before the actor budget ran out.
    ///
    /// Absent on a healthy answer. Present, it names the fields whose values below are
    /// fallbacks rather than readings — a node reporting `active_shards: 0` because it is busy
    /// looks identical to one reporting it because it has no shards, and only this tells them
    /// apart. The liveness fields beside it are atomics and are never degraded.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub degraded: Option<Vec<String>>,
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

    // The two ways a shard's data path stops serving while the request path stays up: a writer
    // that has died or wedged mid-batch (no more writes), and a read pool with every thread stuck
    // and no progress (no more reads). Either should make an orchestrator stop routing here and
    // recycle the node, so either forces red whatever the cluster view says. This is the only part
    // of the anonymous response that touches local node state, and it is a load of a handful of
    // atomics — it never reaches the read pool or a shard's writer, so it cannot itself stall.
    let local_status = worst_status(
        "green".to_string(),
        state.writer_liveness.unavailable_writers(),
        state.read_pool_health.is_wedged(),
    );

    if !identified {
        // Still the *real* status, not a constant: a health check that cannot go yellow is
        // not a health check, and this is what a load balancer reads. Anonymous callers need
        // only the local liveness atomics — the coordinator round-trip is for the expanded
        // body below, and a health flood must not become mailbox pressure on it.
        return Ok(Json(serde_json::json!({ "status": local_status })).into_response());
    }

    // One deadline for every actor round-trip below, rather than one each. They run in
    // sequence, so a per-call wait bounds none of them: four calls at the old fixed 5s was a
    // 20s worst case on a node whose whole request budget might be 1s. `degraded` names
    // whichever did not answer inside it, because the fallbacks below are indistinguishable
    // from real values — 0 shards, 0 indexes, an unknown node id — and a body that looks
    // broken is worse to act on than one that says it is incomplete.
    let actor_deadline = Instant::now() + health_actor_budget(state.request_timeout);
    let mut degraded: Vec<&'static str> = Vec::new();

    // Query cluster status from coordinator — only for the expanded body an identified caller
    // receives, so an anonymous health flood never reaches the actor.
    let cluster_status = match timeout_at(actor_deadline, state.coordinator.ask(GetStatus)).await {
        Ok(Ok(status)) => Some(status),
        Ok(Err(err)) => {
            error!(error = ?err, "Failed to get cluster status from coordinator");
            degraded.push("cluster_status");
            None
        }
        Err(_) => {
            error!("health actor budget exhausted: GetStatus");
            degraded.push("cluster_status");
            None
        }
    };

    let status = cluster_status
        .as_ref()
        .map(|s| s.health.clone())
        .unwrap_or_else(|| "green".to_string());

    let status = worst_status(
        status,
        state.writer_liveness.unavailable_writers(),
        state.read_pool_health.is_wedged(),
    );

    let (read_pool_in_flight, read_pool_capacity) = state.read_pool_health.gauge();
    let read_pool_abandoned = state.read_pool_health.abandoned();
    let (queue_depth, predicted_wait_ms) = match state.queue_load.as_ref() {
        Some(load) => (
            Some(load.depth()),
            Some(load.predicted_wait().as_millis() as u64),
        ),
        None => (None, None),
    };
    let (mailbox_depth, mailbox_predicted_wait_ms) = match state.router.mailbox_load() {
        Some(load) => (
            Some(load.depth()),
            Some(load.predicted_wait().as_millis() as u64),
        ),
        None => (None, None),
    };

    // Get basic shard count and node info from orchestrator. These can queue behind real work,
    // so the expanded body uses bounded waits; on timeout we fall back to defaults rather than
    // let a slow node fail its own health probe.
    let shard_count = match timeout_at(actor_deadline, state.router.shard_count()).await {
        Ok(count) => count,
        Err(_) => {
            error!("health actor budget exhausted: shard_count");
            degraded.push("active_shards");
            0
        }
    };
    let (node_id, node_name) = match timeout_at(
        actor_deadline,
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
            error!("health actor budget exhausted or error: GetIdentity");
            degraded.push("node_id");
            ("local".to_string(), "unknown".to_string())
        }
    };

    // Get index statistics for health check
    let (total_indexes, indexes_with_data) = match timeout_at(
        actor_deadline,
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
            error!("health actor budget exhausted or error: ListIndexes");
            degraded.push("total_indexes");
            (0, 0) // Fallback to 0 if index listing fails
        }
    };

    // A node that could not read its own state inside half its request budget is not in a
    // normal state, whatever the cluster view says — so it reports yellow rather than green.
    // It is still serving, which is why this is not red: red is reserved for a data path that
    // has stopped (a dead writer, a wedged pool), and an orchestrator evicting a node that is
    // merely congested would turn a slow node into an outage.
    let status = degrade_status(status, !degraded.is_empty());

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
        read_pool_abandoned,
        queue_depth,
        predicted_wait_ms,
        mailbox_depth,
        mailbox_predicted_wait_ms,
        mailbox_service_p90_ms: state.router.mailbox_service_p90_ms(),
        dial_failures: cluster_status.as_ref().map(|s| s.dial_failures),
        bootstrap_successes: cluster_status.as_ref().map(|s| s.bootstrap_successes),
        routing_updates: cluster_status.as_ref().map(|s| s.routing_updates),
        degraded: (!degraded.is_empty())
            .then(|| degraded.iter().map(|name| name.to_string()).collect()),
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
/// Fold "this body is incomplete" into the reported status.
///
/// Green is a claim that the node is operating normally. Having failed to answer its own
/// metadata inside the budget, it cannot make that claim — but it is still serving requests, so
/// the honest answer is the middle one. A status already worse than green is left alone: this
/// only ever moves green to yellow, never anything down to it.
fn degrade_status(status: String, degraded: bool) -> String {
    if degraded && status == "green" {
        "yellow".to_string()
    } else {
        status
    }
}

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
    use super::{
        HEALTH_ACTOR_BUDGET_MAX, HEALTH_ACTOR_BUDGET_MIN, degrade_status, health_actor_budget,
        worst_status,
    };
    use std::time::Duration;

    /// The property the fixed 5s constant did not have: the wait has to be shorter than the
    /// budget it is taken out of, or `TimeoutLayer` abandons the request as a 408 before the
    /// fallback can run. Pinned across the range of timeouts a node can resolve.
    #[test]
    fn the_actor_budget_always_leaves_room_inside_the_request_timeout() {
        for secs in [1u64, 2, 5, 30, 60, 300] {
            let timeout = Duration::from_secs(secs);
            let budget = health_actor_budget(timeout);
            assert!(
                budget < timeout,
                "actor budget {budget:?} must fit inside request timeout {timeout:?}",
            );
        }
    }

    /// The one-second node is the case F8 measured, and the case the old constant failed
    /// hardest: 5s of permitted waiting inside a 1s budget.
    #[test]
    fn a_one_second_node_gets_half_a_second_rather_than_five() {
        assert_eq!(
            health_actor_budget(Duration::from_secs(1)),
            Duration::from_millis(500)
        );
    }

    /// Green means "operating normally", and a node that could not read its own state inside
    /// its budget is not. Yellow rather than red: it is congested, not stopped.
    #[test]
    fn an_incomplete_body_reports_yellow_rather_than_green() {
        assert_eq!(degrade_status("green".to_string(), true), "yellow");
        assert_eq!(degrade_status("green".to_string(), false), "green");
    }

    /// Only ever moves green up to yellow — never pulls a worse status back toward it.
    #[test]
    fn degrading_never_improves_a_status() {
        assert_eq!(degrade_status("red".to_string(), true), "red");
        assert_eq!(degrade_status("yellow".to_string(), true), "yellow");
    }

    #[test]
    fn the_actor_budget_is_clamped_at_both_ends() {
        assert_eq!(
            health_actor_budget(Duration::from_secs(300)),
            HEALTH_ACTOR_BUDGET_MAX
        );
        assert_eq!(
            health_actor_budget(Duration::from_millis(1)),
            HEALTH_ACTOR_BUDGET_MIN
        );
    }

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
