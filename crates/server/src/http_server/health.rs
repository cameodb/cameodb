//! Liveness and readiness.

use axum::{
    Extension, Json,
    extract::State,
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::time::{Instant, timeout_at};
use tracing::{error, warn};

use crate::node::RouterActor;

/// How old the index counts in the expanded body may be before a probe starts a fresh count.
///
/// The counts are informational — nothing routes or refuses on them — and producing them walks
/// every index on every shard, so a probe serves the last reading and counts again at most this
/// often, in the background. See [`IndexCounts`].
const INDEX_COUNTS_MAX_AGE: Duration = Duration::from_secs(30);

/// One count of this node's indexes, and when it was taken.
#[derive(Clone, Copy, Debug)]
pub struct IndexCountsReading {
    at: Instant,
    total: usize,
    with_data: usize,
}

/// `total_indexes` and `indexes_with_data` for the health body, counted off the request path.
///
/// Counting reads stats for every index on every shard: sizes, document counts, the fields the
/// built index has. Done on every probe, a health check cost more the more indexes the node
/// held — about 0.2 s at 3,000, and past its 5 s budget at 9,000, where it reported the node
/// degraded for being large. A probe now serves the last reading and, when it is older than
/// [`INDEX_COUNTS_MAX_AGE`], starts one count in the background; only the first probe after
/// start waits for one, inside its usual budget. So a probe costs the same at ten indexes or
/// ten thousand, and the count runs at most every 30 s, and only while someone is asking.
pub struct IndexCounts {
    latest: tokio::sync::watch::Sender<Option<IndexCountsReading>>,
    counting: AtomicBool,
}

impl Default for IndexCounts {
    fn default() -> Self {
        Self {
            latest: tokio::sync::watch::Sender::new(None),
            counting: AtomicBool::new(false),
        }
    }
}

impl IndexCounts {
    fn latest(&self) -> Option<IndexCountsReading> {
        *self.latest.borrow()
    }

    /// Start a count in the background unless one is already running.
    fn refresh(self: &Arc<Self>, router: RouterActor) {
        if self.counting.swap(true, Ordering::AcqRel) {
            return;
        }
        let counts = Arc::clone(self);
        tokio::spawn(async move {
            // Cleared however the task ends, a panic included: stuck set, no count would run
            // again and the body would serve one reading for the life of the process.
            struct Done(Arc<IndexCounts>);
            impl Drop for Done {
                fn drop(&mut self) {
                    self.0.counting.store(false, Ordering::Release);
                }
            }
            let done = Done(counts);
            let listing = router
                .handle_client_op(ClientOp::ListIndexes {
                    include_data_size: false,
                })
                .await;
            match listing {
                Ok(listing) => {
                    let (total, with_data) = count_indexes(&listing);
                    done.0.latest.send_replace(Some(IndexCountsReading {
                        at: Instant::now(),
                        total,
                        with_data,
                    }));
                }
                Err(err) => warn!(error = %err, "health: counting this node's indexes failed"),
            }
        });
    }

    /// Wait for the first reading, up to `deadline`.
    async fn first(&self, deadline: Instant) -> Option<IndexCountsReading> {
        let mut rx = self.latest.subscribe();
        match timeout_at(deadline, rx.wait_for(Option::is_some)).await {
            Ok(Ok(reading)) => *reading,
            _ => None,
        }
    }
}

/// `(total, with data)` from an index listing.
fn count_indexes(listing: &serde_json::Value) -> (usize, usize) {
    let total = listing
        .get("total_indexes")
        .and_then(|v| v.as_u64())
        .unwrap_or(0) as usize;
    let with_data = listing
        .get("indexes")
        .and_then(|arr| arr.as_array())
        .map(|indexes| {
            indexes
                .iter()
                .filter(|idx| {
                    idx.get("document_count")
                        .and_then(|c| c.as_u64())
                        .unwrap_or(0)
                        > 0
                })
                .count()
        })
        .unwrap_or(0);
    (total, with_data)
}

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
use crate::node::ClientOp;
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
    /// How many seconds ago the two counts above were taken. They are counted off the request
    /// path, at most every 30 s, so a body can carry a reading that old. Absent when there is
    /// no reading yet — see `degraded`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_counts_age_secs: Option<u64>,

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
    pub ping_failures: Option<u64>,
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
    /// fallbacks rather than readings — a node reporting `total_indexes: 0` because its first
    /// count is not done looks identical to one that holds no indexes, and only this tells them
    /// apart. The liveness fields beside it, the node's identity and its shard count are read
    /// without waiting on anything and are never degraded.
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
    // from real values — no cluster view, 0 indexes — and a body that looks broken is worse to
    // act on than one that says it is incomplete.
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

    // Read, not asked for. They used to come from `GetIdentity`, which a worker answers — so
    // under write overload the ask queued behind the writes, ran out of this budget on nearly
    // every probe, and each such probe logged an `ERROR` and reported `node_id` and
    // `active_shards` as degraded (ROADMAP M6, session 4). The identity is fixed for the life
    // of the process and the shard count is a lock-free snapshot, so neither needs a queue.
    let node_id = state.node_id.to_string();
    let node_name = state.node_name.clone();
    let shard_count = state.router.live_shard_count();

    // Served from the last count rather than counted here — see `IndexCounts`. Only the first
    // probe after start waits, and only inside the same budget as every call above.
    let counts = &state.index_counts;
    let reading = counts.latest();
    if reading.is_none_or(|r| r.at.elapsed() >= INDEX_COUNTS_MAX_AGE) {
        counts.refresh(state.router.clone());
    }
    let reading = match reading {
        Some(reading) => Some(reading),
        None => counts.first(actor_deadline).await,
    };
    let (total_indexes, indexes_with_data, index_counts_age_secs) = match reading {
        Some(r) => (r.total, r.with_data, Some(r.at.elapsed().as_secs())),
        None => {
            // Not a fault: the first count on a node with many indexes can take longer than a
            // probe may wait. It carries on in the background and the next probe reads it.
            warn!("health: this node's indexes are still being counted");
            degraded.push("total_indexes");
            (0, 0, None)
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
        index_counts_age_secs,
        read_pool_in_flight,
        read_pool_capacity,
        read_pool_abandoned,
        queue_depth,
        predicted_wait_ms,
        mailbox_depth,
        mailbox_predicted_wait_ms,
        mailbox_service_p90_ms: state.router.mailbox_service_p90_ms(),
        dial_failures: cluster_status.as_ref().map(|s| s.dial_failures),
        ping_failures: cluster_status.as_ref().map(|s| s.ping_failures),
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
        HEALTH_ACTOR_BUDGET_MAX, HEALTH_ACTOR_BUDGET_MIN, IndexCounts, IndexCountsReading,
        count_indexes, degrade_status, health_actor_budget, worst_status,
    };
    use std::time::Duration;
    use tokio::time::Instant;

    /// The counts come from the listing's total and the indexes in it that hold documents.
    #[test]
    fn index_counts_read_the_total_and_the_indexes_with_documents() {
        let listing = serde_json::json!({
            "total_indexes": 3,
            "indexes": [
                {"name": "a", "document_count": 5},
                {"name": "b", "document_count": 0},
                {"name": "c"},
            ],
        });
        assert_eq!(count_indexes(&listing), (3, 1));
        assert_eq!(count_indexes(&serde_json::json!({})), (0, 0));
    }

    /// Only the first probe waits for a count, and only until its deadline; a reading arriving
    /// while it waits is what it answers with.
    #[tokio::test]
    async fn the_first_probe_waits_for_a_count_until_its_deadline() {
        let counts = IndexCounts::default();
        let soon = Instant::now() + Duration::from_millis(20);
        assert!(counts.first(soon).await.is_none(), "no count yet, so none");

        let reading = IndexCountsReading {
            at: Instant::now(),
            total: 9_000,
            with_data: 8_000,
        };
        let later = Instant::now() + Duration::from_secs(5);
        let (seen, ()) = tokio::join!(counts.first(later), async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            counts.latest.send_replace(Some(reading));
        });
        assert_eq!(seen.map(|r| (r.total, r.with_data)), Some((9_000, 8_000)));
        assert!(
            counts.latest().is_some(),
            "and later probes read it without waiting"
        );
    }

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
