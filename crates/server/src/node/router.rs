//! RouterActor: forwards client operations to NodeOrchestrator via actor messaging.

use super::*;

use futures::stream::StreamExt;
use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering as AtomicOrdering},
};
use std::time::{Duration, Instant};

use anyhow::Result;
use arc_swap::ArcSwap;
use kameo::Actor;
use kameo::actor::ActorRef;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tracing::{debug, warn};
use uuid::Uuid;

use crate::cluster_coordinator::{
    ClusterCoordinator, OperationType, RequestBootstrapRedial, RouteOperation, RoutingDecision,
};
use crate::config::{MessagingConfig, SearchConfig};
use crate::remote_peer_pool::{ConnectionChannel, RemotePeerPool};

// Re-export SortSpec and SortOrder from storage crate
use cluster::ConsistentRing;
use serde_json::Value as JsonValue;

/// Router actor that forwards client operations to NodeOrchestrator via actor messaging.
/// Uses actor messaging instead of Arc<RwLock> - no locks needed.
#[derive(Clone, Actor)]
pub(crate) struct RouterActor {
    pub(super) orchestrator: ActorRef<NodeOrchestrator>,
    pub(super) coordinator: ActorRef<ClusterCoordinator>,
    pub(super) remote_timeout: Duration,
    pub(super) broadcast_timeout: Duration,
    pub(super) broadcast_fanout_limit: usize,
    pub(super) remote_retry_attempts: u8,
    pub(super) default_search_limit: usize,
    pub(super) broadcasts_total: Arc<AtomicU64>,
    pub(super) broadcast_failures: Arc<AtomicU64>,
    // Streaming search configuration
    pub(super) streaming: StreamingSearchConfig,
    /// Worker pool channel for dispatching hot-path ops (Write, Search)
    /// bypassing the actor mailbox for concurrent processing.
    pub(super) worker_tx: Option<OrchestratorWorkerTx>,
    /// Shared pool of cached RemoteActorRef handles for avoiding repeated lookups.
    pub(super) remote_peer_pool: Arc<RemotePeerPool>,
    /// Shard-affine dispatch configuration and shared routing ring.
    pub(super) shard_affine: ShardAffineConfig,
    /// This node's shards, published lock-free. Lets a keyed operation be recognised as
    /// local without asking the coordinator — see `route_and_handle`.
    pub(super) placement: Arc<ArcSwap<ShardPlacement>>,
    /// Whether `[network.cluster] enabled` is on.
    ///
    /// A node with it off is the whole system, so every routing decision is `Local` and the
    /// coordinator has nothing to add. Static configuration, read here rather than asked,
    /// because the ask was the cost — measured at one mailbox round trip per *keyless*
    /// operation, which is every ordinary search, every streaming search and `GET /_indexes`.
    pub(super) clustered: bool,
    /// The backlog gate for the actor-mailbox lane — deferred bulk writes, config and metadata.
    ///
    /// F7 gated the worker pool and left this lane open, which measured as its whole original
    /// failure surviving intact: under a bulk ingest at twice capacity the node served **0 ok/s
    /// and wrote 0 documents**, every request a 408, and `refused_at_admission` and `abandoned`
    /// both at **0** — no gate refused anything, because `BulkWrite` was not worker-eligible
    /// and so never raised the depth the door judges on (ROADMAP F8). Bulk ops are
    /// worker-eligible now, but the lane still needs its gate: a bulk write that has to decide
    /// a schema comes back through `ask_orchestrator`, and that ask must not queue unbounded.
    ///
    /// Its own `QueueLoad` rather than a share of the pool's, for the reason
    /// [`MAILBOX_LANE_WIDTH`] gives. `None` when there is no configured budget to measure
    /// against, which is the same condition that disables the pool's gate.
    pub(super) mailbox_load: Option<Arc<QueueLoad>>,
}

/// Configuration for shard-affine worker dispatch.
#[derive(Clone, Debug)]
pub(crate) struct ShardAffineConfig {
    /// Shared routing ring for shard-affine dispatch (lock-free via ArcSwap).
    /// When `enabled` is true, the router resolves the target shard from the
    /// routing key and routes the job to the affine worker.
    pub(crate) routing_ring: Arc<ArcSwap<ConsistentRing>>,
    /// Enable shard-affine worker dispatch (default: false).
    pub(crate) enabled: bool,
}

/// Configuration for streaming search behavior.
#[derive(Clone, Debug)]
pub(crate) struct StreamingSearchConfig {
    pub(super) enable_streaming_search: bool,

    pub(super) max_concurrent_shard_searches: usize,
    pub(super) max_concurrent_remote_searches: usize,
}

impl StreamingSearchConfig {
    pub(crate) fn from_search_config(sc: &SearchConfig) -> Self {
        Self {
            enable_streaming_search: sc.enable_streaming_search,

            max_concurrent_shard_searches: sc.max_concurrent_shard_searches,
            max_concurrent_remote_searches: sc.max_concurrent_remote_searches,
        }
    }
}

/// A paged search is widened before it is fanned out: every node — this one included — is
/// asked for `offset + limit` hits from the front of its own order, and the skip is applied
/// once, after their blocks have been merged into one order. A node that skipped `offset` of
/// its own hits would drop rows that belong on this page.
///
/// The window is read off the original op and the copies carry no offset, so this cannot be
/// applied twice however many levels the request travels through. Only `Search` widens —
/// the streaming fan-out converts `Stream` to `Search` itself before calling this, and every
/// other op passes through unchanged.
pub(super) fn widen_broadcast_op(op: ClientOp, window: SearchWindow) -> ClientOp {
    match op {
        ClientOp::Search {
            index,
            query,
            fields,
            sort,
            ..
        } => ClientOp::Search {
            index,
            query,
            limit: Some(window.fetch_count()),
            offset: None,
            fields,
            sort,
        },
        other => other,
    }
}

/// A peer's answer to a broadcast: the remote ask's own verdict, or the timeout that fired
/// waiting for it.
pub(super) type PeerAnswer =
    Result<Result<JsonValue, OrchestratorError>, tokio::time::error::Elapsed>;

/// What a broadcast asks every source and gets back, before the merge differs.
///
/// Built once by [`RouterActor::broadcast_fanout`] for both broadcast paths — the counter,
/// the peer ask, the per-peer timeout, the concurrency cap, the dispatch-ordinal tagging and
/// the join with the local future were written twice and had already drifted (ROADMAP L16
/// records what written-twice cost). What stays per-caller is the local future and the
/// merge.
pub(super) struct BroadcastFanout {
    /// The window read off the op before widening — the merge pages through it.
    pub(super) window: SearchWindow,
    /// The op as fanned out — a `Search` carries `fetch_count` and no offset.
    pub(super) op: ClientOp,
    /// This node's answer.
    pub(super) local: Result<JsonValue, OrchestratorError>,
    /// Every asked peer's answer back in dispatch order — not completion order, which a
    /// merge reading ties off source order would leak into the response. The outer `Err`
    /// is the timeout.
    pub(super) remote: Vec<(Uuid, PeerAnswer)>,
    /// When the local + remote join started — the `took_ms` floor when no source says one.
    pub(super) started: Instant,
    /// How many peers the fan-out asked, after `broadcast_fanout_limit`.
    pub(super) peers_asked: usize,
}

/// One block per source, taken whole. Ordering across blocks is `order_hit_blocks`'s
/// business, and it needs to know which source each hit came from to settle a tie the same
/// way twice. The rest of what a broadcast response carries is folded into `stats` here so
/// the merge loop is one call per source.
pub(super) fn push_hits(
    value: &mut JsonValue,
    blocks: &mut Vec<Vec<JsonValue>>,
    stats: &mut BroadcastStats,
) {
    if let Some(hits) = value.get_mut("hits").and_then(|h| h.as_array_mut()) {
        blocks.push(std::mem::take(hits));
    }
    // Extract shard statistics from the response
    if let Some(stats_obj) = value.get("stats").and_then(|s| s.as_object())
        && let Some(shards) = stats_obj.get("shards").and_then(|s| s.as_object())
        && let Some(responded) = shards.get("responded").and_then(|r| r.as_u64())
    {
        stats.total_shards_queried += responded as usize;
        _ = shards.get("total").and_then(|t| t.as_u64()); // Could track total shards attempted
    }
    if let Some(total) = value.get("total_hits").and_then(|t| t.as_u64()) {
        stats.total_hits_sum += total as usize;
    }
    for note in collect_discarded(std::slice::from_ref(value)) {
        if !stats.discarded.contains(&note) {
            stats.discarded.push(note);
        }
    }
    stats.approximate_sort = stats
        .approximate_sort
        .take()
        .or_else(|| collect_approximate_sort(std::slice::from_ref(value)));
    stats.narrowed_default_fields = stats
        .narrowed_default_fields
        .take()
        .or_else(|| collect_narrowed_default_fields(std::slice::from_ref(value)));
    stats.nodes_contacted += 1;
    if let Some(t) = value.get("took_ms").and_then(|v| v.as_u64()) {
        stats.max_took_ms = match stats.max_took_ms {
            Some(cur) => Some(cur.max(t)),
            None => Some(t),
        };
    }
}

impl RouterActor {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn with_config(
        orchestrator: ActorRef<NodeOrchestrator>,
        coordinator: ActorRef<ClusterCoordinator>,
        messaging: &MessagingConfig,
        // Resolved by `CameoDbConfig::effective_remote_timeout_secs`, not read from
        // `messaging`: unset, the remote deadline follows the HTTP one, and reading the raw
        // field here forwarded with 30 s under a 60 s HTTP timeout.
        remote_timeout_secs: u64,
        streaming: StreamingSearchConfig,
        default_search_limit: usize,
        worker_tx: Option<OrchestratorWorkerTx>,
        remote_peer_pool: Arc<RemotePeerPool>,
        shard_affine: ShardAffineConfig,
        placement: Arc<ArcSwap<ShardPlacement>>,
        clustered: bool,
        mailbox_lane: MailboxLane,
    ) -> Self {
        // The lane's budget is the node's request timeout, which the pool already resolved.
        // Without a pool there is no budget to read and no deadline to fail, so the gate stays
        // off — the same condition under which the pool's own gate is off.
        let mailbox_load = worker_tx
            .as_ref()
            .and_then(|tx| tx.load().budget())
            .map(|budget| {
                Arc::new(
                    QueueLoad::new(
                        Arc::clone(&mailbox_lane.0),
                        MAILBOX_LANE_WIDTH,
                        Some(budget),
                    )
                    .tail_aware(),
                )
            });
        Self {
            orchestrator,
            coordinator,
            remote_timeout: Duration::from_secs(remote_timeout_secs),
            broadcast_timeout: Duration::from_secs(messaging.broadcast_timeout_secs),
            broadcast_fanout_limit: messaging.broadcast_fanout_limit,
            remote_retry_attempts: messaging.remote_retry_attempts,
            default_search_limit,
            broadcasts_total: Arc::new(AtomicU64::new(0)),
            broadcast_failures: Arc::new(AtomicU64::new(0)),
            streaming,
            worker_tx,
            remote_peer_pool,
            shard_affine,
            placement,
            clustered,
            mailbox_load,
        }
    }

    /// The mailbox lane's backlog gate, for the endpoints that report what refusals are made on.
    pub(crate) fn mailbox_load(&self) -> Option<&Arc<QueueLoad>> {
        self.mailbox_load.as_ref()
    }

    /// The lane's measured service p90, in milliseconds.
    ///
    /// The node published no service percentile at all before this, so an operator could see how
    /// much work was queued but not how long the node's own work was taking — and could not tell
    /// a node that had got slower from one that had got busier.
    pub(crate) fn mailbox_service_p90_ms(&self) -> Option<u64> {
        self.mailbox_load
            .as_ref()
            .map(|load| load.dispatch_stats.service_hist.estimate_us() / 1_000)
    }

    /// Handles client operations.
    ///
    /// Hot-path ops (Write, Search, Stream) and the bulk fan-outs are dispatched to the
    /// worker pool for concurrent processing, bypassing the actor mailbox. What still goes
    /// through the mailbox is what needs `&mut NodeOrchestrator`: config and metadata ops,
    /// and the ops a worker handed back because they need a schema written or a remote shard
    /// forwarded to.
    pub(crate) async fn handle_client_op(
        &self,
        op: ClientOp,
    ) -> Result<JsonValue, OrchestratorError> {
        // Try worker pool for hot-path ops
        if let Some(tx) = &self.worker_tx {
            let is_worker_eligible = matches!(
                op,
                ClientOp::Write { .. }
                    | ClientOp::Delete { .. }
                    | ClientOp::Search { .. }
                    | ClientOp::Stream { .. }
                    // Bulk ops fan out over the same snapshots the rest of the engine reads.
                    // What they cannot do off the mailbox is *decide* a schema — a bulk write
                    // that needs one written hands itself back as `UseActor`, which is the
                    // fast/slow split the single-write path already uses.
                    | ClientOp::BulkWrite { .. }
                    | ClientOp::BulkDelete { .. }
                    // A metadata read with no actor state behind it. On the mailbox it queued
                    // behind whatever write was there; the pool answers it from an ArcSwap.
                    | ClientOp::GetIdentity
                    // The index listing asks only `&self` questions of the shard map — stats
                    // gathered per shard, one schema per index, an identity that never
                    // changes. `ListClusterIndexes` lands here only as the local half of a
                    // broadcast, which is the same listing (ROADMAP CH12).
                    | ClientOp::ListIndexes { .. }
                    | ClientOp::ListClusterIndexes { .. }
            );
            if is_worker_eligible {
                // Refuse before queueing, not after waiting. The dequeue check below this is
                // the same decision taken at a worker, by which point the request has already
                // paid for its body, its JSON, a concurrency permit and a channel hop — and
                // under overload nearly every request pays that and is refused anyway. The
                // front door (`routes.rs`) consults the same estimate one layer further out;
                // this is the guard for anything that reaches here another way.
                let load = tx.load();
                if let Some(predicted) = load.would_refuse(OpClass::of(&op)) {
                    return Err(load.refuse(predicted));
                }

                // Resolve shard affinity hint when shard-affine dispatch is enabled.
                // For Write ops, the routing_key maps to a shard via the consistent ring.
                // For Search/Stream ops (scatter-gather), no single shard owns the query,
                // so affinity is None and dispatch falls back to round-robin.
                let affinity_shard = if self.shard_affine.enabled {
                    match &op {
                        // A delete's key is the one the engine will route by, so unlike a write
                        // — whose document may name a different one — the worker chosen here is
                        // always the one co-located with the writer that serves it.
                        ClientOp::Write { routing_key, .. }
                        | ClientOp::Delete { routing_key, .. } => {
                            routing_key.as_ref().and_then(|key| {
                                let ring = self.shard_affine.routing_ring.load();
                                ring.get_owner(key)
                            })
                        }
                        _ => None, // Search/Stream → scatter-gather, no affinity
                    }
                } else {
                    None
                };

                let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
                let job = OrchestratorJob::Execute {
                    // Arrival, not enqueue: everything the request already spent — body read,
                    // parse, routing — is budget it no longer has, and the dequeue check must
                    // see that or a max-size write would be granted a whole second budget after
                    // spending the first being received.
                    arrived_at: request_started_at(),
                    op: Box::new(op),
                    affinity_shard,
                    reply: reply_tx,
                };

                let send_result = if self.shard_affine.enabled {
                    tx.try_send_affine(job, affinity_shard)
                } else {
                    tx.try_send(job)
                };

                match send_result {
                    Ok(()) => {
                        // Await worker result
                        return match reply_rx.await {
                            Ok(WorkerOutcome::Done(result)) => result,
                            // The engine declined and handed the op back. Retrying it here
                            // is the whole point of the fast/slow split: the actor owns the
                            // `&mut` state that schema evolution and bulk writes need.
                            Ok(WorkerOutcome::UseActor(op)) => self.ask_orchestrator(*op).await,
                            Err(_) => Err(OrchestratorError::Io(std::io::Error::other(
                                "Worker dropped reply channel",
                            ))),
                        };
                    }
                    Err(err) => match *err {
                        mpsc::error::TrySendError::Full(job) => {
                            // Every worker queue is full, so the pool is as backed up as it can
                            // get. Diverting to the actor here is what this used to do, and it
                            // was F7's own mechanism surviving in the overflow lane: the actor
                            // mailbox holds 64, `ask` *waits* for a slot rather than failing,
                            // the actor serialises everything it takes, and nothing on that
                            // path checks a deadline. A request sent there under overload
                            // queues behind a single task, runs whatever happens, and answers
                            // after its client has gone — which is exactly what the dequeue
                            // check was added to stop, on the one path it does not cover.
                            //
                            // With a budget configured, refuse instead. Without one the node
                            // has no deadline to fail, so the old relief valve is still the
                            // better answer for a burst.
                            let load = tx.load();
                            if load.budget().is_some() {
                                return Err(load.refuse(load.predicted_wait()));
                            }
                            debug!("Worker pool queue full, falling back to actor mailbox");
                            if let OrchestratorJob::Execute { op, .. } = job {
                                return self.ask_orchestrator(*op).await;
                            }
                            return Err(OrchestratorError::Io(std::io::Error::other(
                                "Worker queue full while shutting down",
                            )));
                        }
                        mpsc::error::TrySendError::Closed(job) => {
                            warn!("Worker pool channel closed, falling back to actor mailbox");
                            if let OrchestratorJob::Execute { op, .. } = job {
                                return self.ask_orchestrator(*op).await;
                            }
                            return Err(OrchestratorError::Io(std::io::Error::other(
                                "Worker pool channel closed during shutdown",
                            )));
                        }
                    },
                }
            }
        }

        // Fallback: route through actor mailbox
        self.ask_orchestrator(op).await
    }

    /// Forward an operation through the actor mailbox (serialized path).
    ///
    /// The handler's own error is returned unchanged. It used to be formatted into a string and
    /// wrapped in `io::Error::other`, which flattened the kind to `Other` — so every refusal an
    /// op earned on the actor path arrived at the HTTP layer unclassifiable, and a document the
    /// schema rejected answered `500 Internal server error` instead of a `400` naming what was
    /// wrong with it. Only the delivery failures need a description; the handler already wrote
    /// one for its own.
    pub(super) async fn ask_orchestrator(
        &self,
        op: ClientOp,
    ) -> Result<JsonValue, OrchestratorError> {
        let Some(load) = self.mailbox_load.as_ref() else {
            return self.ask_orchestrator_unguarded(op).await;
        };

        // Refuse before queueing, for the same reason the worker pool's door does: `ask` waits
        // for a mailbox slot rather than failing, the actor serialises what it takes, and
        // nothing downstream checks a deadline — so a request sent into a deep mailbox runs to
        // completion long after its client has gone. Measured before this gate existed: 8,405
        // bulk requests at 2x and 5x capacity, 100% answered 408, zero documents written, and
        // not one refusal recorded anywhere (ROADMAP F8).
        //
        // This is the lane's door and it has no dequeue counterpart. The re-check a worker
        // makes after waiting cannot be made here: the actor handles the message in its own
        // task, where `REQUEST_STARTED_AT` is not in scope, so a job that queued behind a slow
        // one cannot tell how long it waited. Predicting before the wait is what is available,
        // and it is the half F7 measured as the more valuable one.
        let class = OpClass::of(&op);
        if let Some(predicted) = load.would_refuse(class) {
            return Err(load.refuse(predicted));
        }

        // `outstanding` is this lane's own counter, so the depth above is the mailbox's and
        // never the pool's. Held by a guard rather than decremented after the await — see
        // [`MailboxSlot`], which is the difference between this gate working and this gate
        // becoming the failure it prevents.
        // `_slot` lives to the end of this function, so the count is released on the normal
        // return and on cancellation alike.
        let _slot = MailboxSlot::enter(Arc::clone(&load.dispatch_stats));
        self.ask_orchestrator_unguarded(op).await
    }

    /// The mailbox ask itself, without the backlog gate. Split out so the guarded path above
    /// reads as the decision it is, and so a node with no budget keeps exactly its old behaviour.
    pub(super) async fn ask_orchestrator_unguarded(
        &self,
        op: ClientOp,
    ) -> Result<JsonValue, OrchestratorError> {
        match self.orchestrator.ask(op).await {
            Ok(result) => Ok(result),
            Err(kameo::error::SendError::HandlerError(err)) => Err(err),
            Err(e) => Err(OrchestratorError::Io(std::io::Error::other(format!(
                "Actor error: {}",
                e
            )))),
        }
    }

    pub(crate) async fn admin_memory(&self) -> Result<AdminMemoryReport, OrchestratorError> {
        self.orchestrator.ask(GetAdminMemory).await.map_err(|e| {
            OrchestratorError::Io(std::io::Error::other(format!("Actor error: {}", e)))
        })
    }

    pub(crate) async fn admin_purge_memory(
        &self,
        force: bool,
    ) -> Result<AdminMemoryReport, OrchestratorError> {
        self.orchestrator
            .ask(PurgeAdminMemory { force })
            .await
            .map_err(|e| {
                OrchestratorError::Io(std::io::Error::other(format!("Actor error: {}", e)))
            })
    }

    pub(crate) async fn admin_commit_index(
        &self,
        index: String,
    ) -> Result<AdminIndexCommitReport, OrchestratorError> {
        self.orchestrator
            .ask(CommitAdminIndex { index })
            .await
            .map_err(|e| {
                OrchestratorError::Io(std::io::Error::other(format!("Actor error: {}", e)))
            })
    }

    pub(crate) async fn admin_evict_index_writer(
        &self,
        index: String,
    ) -> Result<AdminIndexEvictWriterReport, OrchestratorError> {
        self.orchestrator
            .ask(EvictAdminIndexWriter { index })
            .await
            .map_err(|e| {
                OrchestratorError::Io(std::io::Error::other(format!("Actor error: {}", e)))
            })
    }

    /// Returns a snapshot of the worker pool stats for `/_admin/workers`.
    pub(crate) fn admin_worker_stats(&self) -> Result<WorkerPoolReport, OrchestratorError> {
        match &self.worker_tx {
            Some(tx) => Ok(tx.snapshot()),
            None => Err(OrchestratorError::NotReady(
                "Worker pool not initialized".to_string(),
            )),
        }
    }

    /// Hits returned when a query names no limit, as this node is configured.
    ///
    /// A caller that passes `None` through gets this applied for it, deeper down. It is exposed
    /// for the one caller that has to know the number before the search runs: a federated merge
    /// truncates the combined result itself, and doing that against a different default than
    /// the searches used would report a limit the node did not apply.
    pub(crate) fn default_search_limit(&self) -> usize {
        self.default_search_limit
    }

    /// Answer "this node owns the key" from published state, or `None` to ask the
    /// coordinator.
    ///
    /// Deliberately conservative: it returns `Some` only for a key whose owning shard is on
    /// this node. An unkeyed operation is a scatter-gather whose answer depends on how many
    /// nodes are in the cluster, which is the coordinator's to know, so it is left alone.
    pub(super) fn resolve_local(&self, routing_key: Option<&str>) -> Option<RoutingDecision> {
        // A node with clustering off is the whole system: `decide_route` has one node in
        // `expected_nodes` and can only ever answer `Local`, whether or not a key was given.
        // Taking that arm here is what keeps a keyless operation off the coordinator — and
        // keyless is every ordinary search, since a search has no routing key to resolve.
        if !self.clustered {
            return Some(RoutingDecision::Local);
        }
        let key = routing_key?;
        let shard = self.shard_affine.routing_ring.load().get_owner(key)?;
        self.placement
            .load()
            .is_local(&shard)
            .then_some(RoutingDecision::Local)
    }

    /// Route via ClusterCoordinator then handle locally (remote/broadcast stubbed).
    pub(crate) async fn route_and_handle(
        &self,
        op: ClientOp,
        routing_key: Option<String>,
        operation_type: OperationType,
    ) -> Result<JsonValue, OrchestratorError> {
        self.route_and_handle_inner(op, routing_key, operation_type, true)
            .await
    }

    /// [`route_and_handle`](Self::route_and_handle) for a caller that merges the response itself.
    ///
    /// The internal `SORT_KEY_FIELD` survives, so a merge across several of these calls can
    /// order by the sort field even where a projection dropped it. **The caller owes the
    /// strip**: the key is metadata and must not reach a client. Re-deriving the key from the
    /// hit is not an alternative — the sort field may have been projected away, which is the
    /// reason the key exists at all.
    pub(crate) async fn route_and_handle_keeping_sort_keys(
        &self,
        op: ClientOp,
        routing_key: Option<String>,
        operation_type: OperationType,
    ) -> Result<JsonValue, OrchestratorError> {
        self.route_and_handle_inner(op, routing_key, operation_type, false)
            .await
    }

    pub(super) async fn route_and_handle_inner(
        &self,
        op: ClientOp,
        routing_key: Option<String>,
        operation_type: OperationType,
        strip_sort_keys_on_exit: bool,
    ) -> Result<JsonValue, OrchestratorError> {
        // Metadata operations (schema/config) always execute locally - no need to broadcast
        if matches!(
            op,
            ClientOp::GetConfig { .. }
                | ClientOp::GetRawSchema { .. }
                // Local in the sense that matters here: it fans out to peers itself, from the
                // node that received it, so routing it anywhere would only add a hop.
                | ClientOp::FindSchemaInCluster { .. }
                | ClientOp::CreateConfig { .. }
                | ClientOp::UpdateSchema { .. }
        ) {
            return self.handle_client_op(op).await;
        }

        // Search/Stream responses carry an internal `SORT_KEY_FIELD` on each hit so that
        // merges can order by the sort field even when it is projected away. This is the
        // single client-facing boundary for every routing decision (local, broadcast,
        // remote, streaming-buffered), so strip that metadata here before returning — unless
        // the caller is itself a merge and asked to keep it.
        let is_search = strip_sort_keys_on_exit
            && matches!(op, ClientOp::Search { .. } | ClientOp::Stream { .. });

        // Resolve locally before asking anyone. The ring and the shard placement are both
        // published lock-free and already in hand, and between them they answer the only
        // question `decide_route` asks for a keyed operation: which shard owns this key, and
        // is that shard mine? Asking the coordinator instead costs a mailbox round trip to a
        // single actor on every write — a cross-core wakeup and a serialisation point in
        // front of a worker pool built to avoid exactly that.
        //
        // Only a definite local answer is taken here. Anything else — an empty ring, a key
        // no shard claims, a shard on another node — still goes to the coordinator, which
        // knows about peers and addresses and is the only thing that can decide those.
        let decision = match self.resolve_local(routing_key.as_deref()) {
            Some(local) => Ok(local),
            None => {
                self.coordinator
                    .ask(RouteOperation {
                        routing_key,
                        operation_type,
                    })
                    .await
            }
        };

        let mut result = match decision {
            Ok(RoutingDecision::Local) => self.handle_client_op(op).await,
            Ok(RoutingDecision::Broadcast) => {
                // CRITICAL: Never broadcast write operations - this causes data duplication
                // and inconsistency. Writes must be routed to a specific shard.
                if is_write_operation(&op) {
                    return Err(OrchestratorError::Missing(
                        "Write operation cannot be broadcast - routing failed".to_string(),
                    ));
                }

                // Use streaming for search operations if enabled
                if self.streaming.enable_streaming_search
                    && matches!(op, ClientOp::Search { .. } | ClientOp::Stream { .. })
                {
                    self.handle_broadcast_streaming(op).await
                } else {
                    self.handle_broadcast(op).await
                }
            }
            Ok(RoutingDecision::Remote { node_id, peer_addr }) => {
                self.handle_remote(op, node_id, peer_addr).await
            }
            Err(err) => {
                let reason = format!("routing failed: {}", err);
                let _ = self
                    .coordinator
                    .ask(RequestBootstrapRedial {
                        reason: reason.clone(),
                    })
                    .await;
                Err(OrchestratorError::Io(std::io::Error::other(reason)))
            }
        };

        if is_search && let Ok(response) = result.as_mut() {
            strip_sort_keys(response);
        }

        result
    }

    /// Streaming variant of `route_and_handle` for NDJSON search responses.
    ///
    /// Returns a bounded `mpsc::Receiver` that yields `Result<Bytes, io::Error>` items.
    /// Each item is a single NDJSON line: individual hit objects followed by a
    /// `_footer` metadata line. A background task performs the actual search and
    /// streams results into the channel, providing:
    /// - Incremental flushing (each hit serialized and sent individually)
    /// - Bounded backpressure via channel capacity
    /// - Early client disconnect detection
    pub(crate) fn route_and_handle_stream(
        &self,
        op: ClientOp,
        routing_key: Option<String>,
        operation_type: OperationType,
    ) -> mpsc::Receiver<Result<bytes::Bytes, std::io::Error>> {
        pub(super) const STREAM_CHANNEL_CAPACITY: usize = 64;

        let (tx, rx) =
            mpsc::channel::<Result<bytes::Bytes, std::io::Error>>(STREAM_CHANNEL_CAPACITY);
        let router = self.clone();

        tokio::spawn(async move {
            let result = router
                .route_and_handle(op, routing_key, operation_type)
                .await;

            match result {
                Ok(val) => {
                    Self::stream_search_result_as_ndjson(&tx, val).await;
                }
                Err(e) => {
                    let error_line = serde_json::json!({
                        "_error": true,
                        "message": e.to_string(),
                    });
                    if let Ok(mut bytes) = serde_json::to_vec(&error_line) {
                        bytes.push(b'\n');
                        let _ = tx.send(Ok(bytes::Bytes::from(bytes))).await;
                    }
                }
            }
            // tx dropped here → channel closes → stream ends
        });

        rx
    }

    /// Serialize a search result as incremental NDJSON lines into a channel.
    ///
    /// Sends each hit as a separate NDJSON line, followed by a footer line
    /// containing aggregated metadata (total_hits, took_ms, stats, errors).
    pub(super) async fn stream_search_result_as_ndjson(
        tx: &mpsc::Sender<Result<bytes::Bytes, std::io::Error>>,
        mut val: JsonValue,
    ) {
        // Extract the hits array, leaving metadata fields in `val`
        let hits = val
            .as_object_mut()
            .and_then(|o| o.remove("hits"))
            .and_then(|v| match v {
                JsonValue::Array(arr) => Some(arr),
                _ => None,
            })
            .unwrap_or_default();

        // Stream each hit as an individual NDJSON line
        for hit in &hits {
            let mut bytes = match serde_json::to_vec(hit) {
                Ok(b) => b,
                Err(_) => continue,
            };
            bytes.push(b'\n');
            if tx.send(Ok(bytes::Bytes::from(bytes))).await.is_err() {
                return; // Client disconnected
            }
        }

        // Build and send the footer line with metadata
        if let Some(obj) = val.as_object_mut() {
            obj.insert("_footer".to_string(), JsonValue::Bool(true));
            // Preserve hits_returned count even though we removed the array
            if !obj.contains_key("hits_returned") {
                obj.insert(
                    "hits_returned".to_string(),
                    JsonValue::Number(serde_json::Number::from(hits.len())),
                );
            }
        }
        if let Ok(mut footer_bytes) = serde_json::to_vec(&val) {
            footer_bytes.push(b'\n');
            let _ = tx.send(Ok(bytes::Bytes::from(footer_bytes))).await;
        }
    }

    /// The fan-out both broadcast paths share.
    ///
    /// The broadcasts counter, the `GetKnownPeers` ask, the per-peer timeout, the
    /// remote-concurrency cap, the dispatch-ordinal tagging and the join with the local
    /// future were all written twice — once here and once in `handle_broadcast_streaming` —
    /// and had already drifted: the streaming half answered a paged search with page 1
    /// (ROADMAP OB8) and dropped a source when it thought it could stop early. What stays
    /// per-caller is the local future — `handle_broadcast` runs the op through
    /// `handle_client_op`'s worker-pool dispatch, the streaming path asks the orchestrator
    /// directly — and the merge.
    ///
    /// `op` arrives already widened by [`widen_broadcast_op`]; the window it was widened
    /// from is reported back on [`BroadcastFanout::window`] so the merge can page with it.
    async fn broadcast_fanout<F, Fut>(
        &self,
        op: ClientOp,
        window: SearchWindow,
        local: F,
    ) -> BroadcastFanout
    where
        F: FnOnce(ClientOp) -> Fut + Send,
        Fut: Future<Output = Result<JsonValue, OrchestratorError>> + Send,
    {
        use crate::cluster_coordinator::{GetKnownPeers, KnownPeer};

        self.broadcasts_total.fetch_add(1, AtomicOrdering::Relaxed);

        // Get known peers for remote fan-out
        let peers: Vec<KnownPeer> = self
            .coordinator
            .ask(GetKnownPeers)
            .await
            .unwrap_or_default();

        debug!(
            "🔍 Broadcast operation: got {} known peers from coordinator",
            peers.len()
        );
        for peer in &peers {
            debug!("  📍 Peer: {} at {}", peer.node_id, peer.address);
        }

        let peer_count = peers.len().min(self.broadcast_fanout_limit);
        debug!(
            timeout_ms = self.broadcast_timeout.as_millis(),
            fanout_limit = self.broadcast_fanout_limit,
            local_shard_concurrency_limit = self.streaming.max_concurrent_shard_searches,
            remote_concurrency_limit = self.streaming.max_concurrent_remote_searches.max(1),
            known_peers = peers.len(),
            target_peers = peer_count,
            "RouterActor: broadcast routing with remote fan-out"
        );

        // Start local operation
        let local_future = local(op.clone());

        // Fan out to remote peers (up to fanout_limit)
        let remote_limit = self.streaming.max_concurrent_remote_searches.max(1);
        let remote_timeout = self.broadcast_timeout;
        let remote_router = self.clone();
        let remote_op = op.clone();
        // Each peer carries the ordinal it was dispatched at. `buffer_unordered` yields in
        // completion order, and a merge that breaks ties by which peer answered first is a merge
        // that answers one query two ways.
        let remote_results_future = futures::stream::iter(
            peers
                .into_iter()
                .take(self.broadcast_fanout_limit)
                .enumerate()
                .map(move |(dispatch_ordinal, peer)| {
                    let op_clone = remote_op.clone();
                    let remote_router = remote_router.clone();
                    let node_id = peer.node_id;
                    let peer_addr = peer.address;
                    async move {
                        let outcome = timeout(
                            remote_timeout,
                            remote_router.try_remote(&op_clone, node_id, &peer_addr),
                        )
                        .await;
                        (dispatch_ordinal, node_id, outcome)
                    }
                }),
        )
        .buffer_unordered(remote_limit)
        .collect::<Vec<_>>();

        // Execute local + remote concurrently
        let started = Instant::now();
        let (local_result, mut remote_results) = tokio::join!(local_future, remote_results_future);

        // Back into dispatch order before merging, rather than the order they finished in.
        remote_results.sort_by_key(|(dispatch_ordinal, ..)| *dispatch_ordinal);

        BroadcastFanout {
            window,
            op,
            local: local_result,
            remote: remote_results
                .into_iter()
                .map(|(_, node_id, outcome)| (node_id, outcome))
                .collect(),
            started,
            peers_asked: peer_count,
        }
    }

    pub(super) async fn handle_broadcast(
        &self,
        op: ClientOp,
    ) -> Result<JsonValue, OrchestratorError> {
        let window = search_window_for(&op, self.default_search_limit);
        let op = widen_broadcast_op(op, window);
        let fanout = self
            .broadcast_fanout(op, window, |op| self.handle_client_op(op))
            .await;
        let BroadcastFanout {
            window,
            op,
            local: local_result,
            remote: remote_results,
            started: t_start,
            peers_asked: peer_count,
        } = fanout;

        // If this is a search, prefer fastest/local results and stop after hitting the limit.
        if let ClientOp::Search { sort, .. } = &op {
            // `window`, not the op's own limit: the op was widened above to `fetch_count` so
            // that every node returned enough for this merge to page through.
            let sort = sort.clone();
            let mut error_count = 0u64;
            let mut stats = BroadcastStats {
                total_shards_queried: 0,
                nodes_contacted: 0,
                max_took_ms: None,
                total_hits_sum: 0,
                discarded: Vec::new(),
                approximate_sort: None,
                narrowed_default_fields: None,
            };

            // The local node is rank 0, then each peer in the order it was dispatched to.
            let mut blocks: Vec<Vec<JsonValue>> = Vec::new();

            match local_result {
                Ok(mut val) => push_hits(&mut val, &mut blocks, &mut stats),
                Err(e) => {
                    error_count += 1;
                    warn!(error = %e, "Broadcast: local search failed");
                }
            }

            for (_, result) in remote_results {
                match result {
                    Ok(Ok(mut val)) => push_hits(&mut val, &mut blocks, &mut stats),
                    Ok(Err(e)) => {
                        error_count += 1;
                        warn!(error = %e, "Broadcast: remote search failed");
                    }
                    Err(elapsed) => {
                        error_count += 1;
                        warn!(error = %elapsed, "Broadcast: remote search timed out");
                    }
                }
            }

            // Track failures
            if error_count > 0 {
                self.broadcast_failures
                    .fetch_add(error_count, AtomicOrdering::Relaxed);
            }

            let merged_hits = order_hit_blocks(blocks, sort.as_ref(), window);

            let mut response = serde_json::json!({
                "hits": merged_hits,
                "hits_returned": merged_hits.len(),
                "total_hits": stats.total_hits_sum,
                "limit": window.limit,
                "offset": window.offset,
                "took_ms": stats.max_took_ms.unwrap_or_else(|| t_start.elapsed().as_millis() as u64),
                "stats": {
                    "shards": {
                        "total": stats.total_shards_queried,
                        "responded": stats.total_shards_queried.saturating_sub(error_count as usize),
                        "failed": error_count as usize
                    },
                    "nodes": {
                        "contacted": stats.nodes_contacted
                    }
                }
            });
            attach_discarded(&mut response, stats.discarded);
            attach_approximate_sort(&mut response, stats.approximate_sort);
            attach_narrowed_default_fields(&mut response, stats.narrowed_default_fields);
            return Ok(response);
        }

        // Aggregate results: for search, merge hits; for writes, report success/failure counts
        let mut all_results: Vec<JsonValue> = Vec::new();
        let mut error_count = 0u64;

        // Process local result
        match local_result {
            Ok(val) => all_results.push(val),
            Err(e) => {
                error_count += 1;
                warn!(error = %e, "Broadcast: local operation failed");
            }
        }

        // Already back in dispatch order — the fan-out sorted them — so the merge below
        // ranks the nodes the same way on every run rather than by which answered first.
        for (_, result) in remote_results {
            match result {
                Ok(Ok(val)) => all_results.push(val),
                Ok(Err(e)) => {
                    error_count += 1;
                    warn!(error = %e, "Broadcast: remote operation failed");
                }
                Err(elapsed) => {
                    error_count += 1;
                    warn!(error = %elapsed, "Broadcast: remote operation timed out");
                }
            }
        }

        if error_count > 0 {
            self.broadcast_failures
                .fetch_add(error_count, AtomicOrdering::Relaxed);
        }

        // Merge results based on operation type
        match &op {
            ClientOp::Search { sort, .. } => {
                // Unreachable for a search today — the branch above returns for every
                // `ClientOp::Search`. Kept in step with `window` regardless, so that it cannot
                // come back to life paging incorrectly.
                let limit = window.limit;
                let nodes_contacted = all_results.len();

                // For search operations, if we only have local results (no remote peers),
                // return the local response directly to preserve shard-level details
                if all_results.len() == 1 && peer_count == 0 {
                    return Ok(all_results[0].clone());
                }

                // One block per node, in the order they were dispatched to.
                let mut blocks: Vec<Vec<JsonValue>> = Vec::new();
                let mut total_shards_queried = 0usize;
                let mut total_hits_sum = 0usize;
                // Read before the loop below consumes `all_results`.
                let discarded = collect_discarded(&all_results);
                let approximate_sort = collect_approximate_sort(&all_results);
                let narrowed_default_fields = collect_narrowed_default_fields(&all_results);

                for mut result in all_results {
                    if let Some(hits) = result.get_mut("hits").and_then(|h| h.as_array_mut()) {
                        blocks.push(std::mem::take(hits));
                    }
                    if let Some(stats) = result.get("stats").and_then(|s| s.as_object())
                        && let Some(shards) = stats.get("shards").and_then(|s| s.as_object())
                        && let Some(responded) = shards.get("responded").and_then(|r| r.as_u64())
                    {
                        total_shards_queried += responded as usize;
                    } else if let Some(shards) =
                        result.get("shards_responded").and_then(|s| s.as_u64())
                    {
                        total_shards_queried += shards as usize;
                    }
                    if let Some(total) = result.get("total_hits").and_then(|t| t.as_u64()) {
                        total_hits_sum += total as usize;
                    }
                }

                // Ordered by the requested sort when there is one. This branch previously
                // merged by score whatever was asked for, so a sorted search that reached more
                // than one node came back ranked by relevance instead.
                let merged_hits = order_hit_blocks(blocks, sort.as_ref(), window);

                let mut response = serde_json::json!({
                    "hits": merged_hits,
                    "hits_returned": merged_hits.len(),
                    "total_hits": total_hits_sum,
                    "limit": limit,
                    "offset": window.offset,
                    "stats": {
                        "shards": {
                            "total": total_shards_queried,
                            "responded": total_shards_queried.saturating_sub(error_count as usize),
                            "failed": error_count as usize
                        },
                        "nodes": {
                            "contacted": nodes_contacted
                        }
                    }
                });
                attach_discarded(&mut response, discarded);
                attach_approximate_sort(&mut response, approximate_sort);
                attach_narrowed_default_fields(&mut response, narrowed_default_fields);
                Ok(response)
            }
            ClientOp::Write { .. } | ClientOp::BulkWrite { .. } => {
                // For writes, return aggregate success info
                let total_nodes = all_results.len();

                // Aggregate items_written and errors from all node responses
                let mut items_written = 0u64;
                let mut errors = Vec::new();

                for result in &all_results {
                    if let Some(n) = result.get("items_written").and_then(|v| v.as_u64()) {
                        items_written += n;
                    }
                    if let Some(errs) = result.get("errors").and_then(|v| v.as_array()) {
                        errors.extend(errs.clone());
                    }
                }

                Ok(serde_json::json!({
                    "success": error_count == 0 && errors.is_empty(),
                    "nodes_contacted": total_nodes + error_count as usize,
                    "nodes_succeeded": total_nodes,
                    "nodes_failed": error_count,
                    "items_written": items_written,
                    "errors": errors
                }))
            }
            ClientOp::ListClusterIndexes { include_data_size } => {
                // Merge index statistics from all nodes
                let mut index_map: HashMap<String, IndexStats> = HashMap::new();
                let mut node_details: Vec<JsonValue> = Vec::new();

                for result in &all_results {
                    // Extract node_id and node_name from each response
                    let node_id = result
                        .get("node_id")
                        .and_then(|v| v.as_str())
                        .unwrap_or("unknown")
                        .to_string();

                    let node_name = result
                        .get("node_name")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string());

                    // Collect per-node details with node_name immediately after node_id
                    let mut node_detail_map = serde_json::Map::new();
                    node_detail_map.insert("node_id".to_string(), serde_json::json!(node_id));
                    if let Some(name) = node_name {
                        node_detail_map.insert("node_name".to_string(), serde_json::json!(name));
                    }
                    node_detail_map.insert(
                        "indexes".to_string(),
                        result
                            .get("indexes")
                            .cloned()
                            .unwrap_or(serde_json::json!([])),
                    );
                    node_detail_map.insert(
                        "total_indexes".to_string(),
                        serde_json::json!(
                            result
                                .get("total_indexes")
                                .and_then(|v| v.as_u64())
                                .unwrap_or(0)
                        ),
                    );
                    node_detail_map.insert(
                        "total_shards".to_string(),
                        serde_json::json!(
                            result
                                .get("total_shards")
                                .and_then(|v| v.as_u64())
                                .unwrap_or(0)
                        ),
                    );

                    node_details.push(serde_json::Value::Object(node_detail_map));

                    // Aggregate index stats across nodes
                    if let Some(indexes) = result.get("indexes").and_then(|v| v.as_array()) {
                        for idx in indexes {
                            let name = idx
                                .get("name")
                                .and_then(|v| v.as_str())
                                .unwrap_or("")
                                .to_string();
                            if name.is_empty() {
                                continue;
                            }

                            let entry = index_map.entry(name.clone()).or_insert(IndexStats {
                                name: name.clone(),
                                description: None,
                                document_count: 0,
                                index_size_bytes: 0,
                                memory_bytes: 0,
                                data_size_bytes: 0,
                                total_size_bytes: 0,
                                shard_count: 0,
                                warm_shards: 0,
                                fields: BTreeMap::new(),
                            });

                            let sum =
                                |key: &str| idx.get(key).and_then(|v| v.as_u64()).unwrap_or(0);
                            entry.document_count += sum("document_count");
                            entry.index_size_bytes += sum("index_size_bytes");
                            entry.memory_bytes += sum("memory_bytes");
                            entry.data_size_bytes += sum("data_size_bytes");
                            entry.total_size_bytes += sum("total_size_bytes");
                            entry.shard_count += sum("shard_count") as usize;
                            entry.warm_shards += sum("warm_shards") as usize;

                            if entry.description.is_none()
                                && let Some(text) = idx.get("description").and_then(|v| v.as_str())
                            {
                                entry.description = Some(text.to_string());
                            }

                            // Merged by name. `searchable` and `sortable` are both OR-ed: one node
                            // holding the column is enough, since the search reaches all of them
                            // and each answers from what it has.
                            if let Some(fields) = idx.get("fields").and_then(|v| v.as_array()) {
                                for field in fields {
                                    let Some(field_name) =
                                        field.get("name").and_then(|v| v.as_str())
                                    else {
                                        continue;
                                    };
                                    match entry.fields.get_mut(field_name) {
                                        Some(existing) => {
                                            for flag in ["searchable", "sortable"] {
                                                let set_here = field
                                                    .get(flag)
                                                    .and_then(|v| v.as_bool())
                                                    .unwrap_or(false);
                                                if set_here
                                                    && let Some(obj) = existing.as_object_mut()
                                                {
                                                    obj.insert(
                                                        flag.to_string(),
                                                        JsonValue::Bool(true),
                                                    );
                                                }
                                            }
                                        }
                                        None => {
                                            entry
                                                .fields
                                                .insert(field_name.to_string(), field.clone());
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                // Convert to the same per-index shape a single node returns, so an index
                // described by the cluster and by one of its nodes reads identically. The
                // previous merge dropped `memory_*` and `warm_shards` entirely, so the same
                // index had two shapes inside one response.
                let mut cluster_indexes: Vec<(String, JsonValue)> = index_map
                    .into_values()
                    .map(|stats| {
                        let name = stats.name.clone();
                        let mut json_obj = serde_json::Map::new();
                        json_obj.insert("name".to_string(), serde_json::json!(stats.name));
                        if let Some(description) = stats.description {
                            json_obj
                                .insert("description".to_string(), serde_json::json!(description));
                        }
                        json_obj.insert(
                            "document_count".to_string(),
                            serde_json::json!(stats.document_count),
                        );
                        json_obj.insert(
                            "index_size_bytes".to_string(),
                            serde_json::json!(stats.index_size_bytes),
                        );
                        json_obj.insert(
                            "memory_bytes".to_string(),
                            serde_json::json!(stats.memory_bytes),
                        );
                        if *include_data_size {
                            json_obj.insert(
                                "data_size_bytes".to_string(),
                                serde_json::json!(stats.data_size_bytes),
                            );
                            json_obj.insert(
                                "total_size_bytes".to_string(),
                                serde_json::json!(stats.total_size_bytes),
                            );
                        }
                        json_obj.insert(
                            "shard_count".to_string(),
                            serde_json::json!(stats.shard_count),
                        );
                        json_obj.insert(
                            "warm_shards".to_string(),
                            serde_json::json!(stats.warm_shards),
                        );

                        // `id` first, then alphabetical — the order a single node uses.
                        let mut fields: Vec<JsonValue> = stats.fields.into_values().collect();
                        fields.sort_by(|a, b| {
                            let key = |v: &JsonValue| {
                                v.get("name")
                                    .and_then(|n| n.as_str())
                                    .unwrap_or_default()
                                    .to_string()
                            };
                            match (key(a).as_str(), key(b).as_str()) {
                                ("id", "id") => std::cmp::Ordering::Equal,
                                ("id", _) => std::cmp::Ordering::Less,
                                (_, "id") => std::cmp::Ordering::Greater,
                                _ => key(a).cmp(&key(b)),
                            }
                        });
                        json_obj.insert("field_count".to_string(), serde_json::json!(fields.len()));
                        json_obj.insert("fields".to_string(), JsonValue::Array(fields));

                        (name, serde_json::Value::Object(json_obj))
                    })
                    .collect();
                cluster_indexes.sort_by(|a, b| a.0.cmp(&b.0));
                let cluster_indexes: Vec<JsonValue> =
                    cluster_indexes.into_iter().map(|(_, json)| json).collect();

                Ok(serde_json::json!({
                    "indexes": cluster_indexes,
                    "total_indexes": cluster_indexes.len(),
                    "nodes_contacted": all_results.len(),
                    "nodes_failed": error_count,
                    "nodes": node_details,
                }))
            }
            _ => {
                // For other operations, return first successful result or error
                if let Some(first) = all_results.first() {
                    Ok(first.clone())
                } else {
                    self.broadcast_failures
                        .fetch_add(1, AtomicOrdering::Relaxed);
                    Err(OrchestratorError::Io(std::io::Error::other(
                        "broadcast failed: no successful responses",
                    )))
                }
            }
        }
    }

    /// Streaming version of handle_broadcast for improved search performance.
    ///
    /// The fan-out is [`broadcast_fanout`](Self::broadcast_fanout) — the same one
    /// [`handle_broadcast`](Self::handle_broadcast) uses. What is streaming about this path is
    /// the local future (a direct ask on the orchestrator rather than `handle_client_op`'s
    /// worker-pool dispatch) and the merge below, which keeps each source's block keyed by
    /// the identity it arrived under.
    ///
    /// The page is read before the arm below destructures the op — `Search` and `Stream`
    /// share the arm and only one of them can be paged, so the distinction is drawn in
    /// `search_window_for` rather than by the pattern.
    ///
    /// It used to be drawn by discarding the offset for both (`offset: _`), so a paged search
    /// on this path silently answered page 1 — hits and count, at every offset, with a 200,
    /// for every unkeyed search on a clustered node with `enable_streaming_search` on.
    /// Tracked as ROADMAP OB8. Honouring it took three things and only the third was here:
    /// the offset had to survive to this point, every source had to be asked for
    /// `offset + limit` rather than `limit` so the merge has enough to page through, and the
    /// merge had to be handed the real window. `order_hit_blocks` already applies whatever
    /// window it is given; the other two are in the shared fan-out.
    ///
    /// It could not have been fixed while this merge still terminated early either: a page
    /// assembled from whichever sources answered first is not the page that was asked for,
    /// wherever the skip is applied. Every source now always answers (2d13f4c).
    pub(super) async fn handle_broadcast_streaming(
        &self,
        op: ClientOp,
    ) -> Result<JsonValue, OrchestratorError> {
        tracing::debug!(
            max_concurrent_shard_searches = self.streaming.max_concurrent_shard_searches,
            max_concurrent_remote_searches = self.streaming.max_concurrent_remote_searches.max(1),
            "🚀 Using STREAMING search for improved performance"
        );

        // Handle search operations with streaming
        match op {
            ClientOp::Search { .. } | ClientOp::Stream { .. } => {
                // A `Stream` is fanned out as the `Search` it names: both asks are the same
                // window from the front of every source's order. `widen_broadcast_op` then
                // gives it the fetch count — inside the shared fan-out, as the non-streaming
                // path's own widening also is.
                let op = match op {
                    ClientOp::Stream {
                        index,
                        query,
                        limit,
                        fields,
                        sort,
                    } => ClientOp::Search {
                        index,
                        query,
                        limit,
                        offset: None,
                        fields,
                        sort,
                    },
                    other => other,
                };
                let window = search_window_for(&op, self.default_search_limit);
                let op = widen_broadcast_op(op, window);
                let ClientOp::Search { sort, .. } = &op else {
                    unreachable!("a stream was rewritten to a search above")
                };
                let sort = sort.clone();

                let fanout = self
                    .broadcast_fanout(op, window, |op| self.ask_orchestrator_unguarded(op))
                    .await;
                let BroadcastFanout {
                    window,
                    op: _,
                    local,
                    remote,
                    started,
                    peers_asked: _,
                } = fanout;

                // One block per source, keyed by that source's identity: this node's shards
                // ahead of its peers, each ordered by id. Sources report in the fan-out's
                // dispatch order; the key is what puts a tie back the same way every time.
                let mut blocks: Vec<((u8, Uuid), Vec<JsonValue>)> = Vec::new();
                let mut total_hits_sum = 0usize;
                let mut nodes_contacted = 0usize;
                // The local block carries no shard identity — one nil stands in.
                let mut unique_shard_ids = std::collections::HashSet::new();
                let mut errors = Vec::new();

                match local {
                    Ok(mut val) => {
                        // Taken whole, the way the remote arm takes its blocks — an earlier
                        // form of this path kept only hits that carried a `_score`, so a
                        // sorted search's local block could shed hits its peers kept.
                        if let Some(hits) = val.get_mut("hits").and_then(|h| h.as_array_mut()) {
                            blocks.push(((0, Uuid::nil()), std::mem::take(hits)));
                        }
                        // Counted from the result, not from its hits — a shard that matched
                        // nothing still answered, and reading the id back out of each
                        // document missed that as well as costing a copy of it per hit.
                        if let Some(total) = val.get("total_hits").and_then(|t| t.as_u64()) {
                            total_hits_sum += total as usize;
                        }
                        unique_shard_ids.insert(Uuid::nil());
                        nodes_contacted += 1;
                    }
                    Err(_) => {
                        // A failed local answer still holds this node's place in the merge —
                        // an empty block, no error note. That was the streaming merge's
                        // answer before the fan-out was shared, and it stands.
                        unique_shard_ids.insert(Uuid::nil());
                        blocks.push(((0, Uuid::nil()), Vec::new()));
                        nodes_contacted += 1;
                    }
                }

                // This node's nil sentinel counts for one; each peer's `responded` adds its
                // own below.
                let mut shards_queried = unique_shard_ids.len();

                for (node_id, result) in remote {
                    nodes_contacted += 1;
                    match result {
                        Ok(Ok(mut val)) => {
                            // Take mutable reference to array to move items
                            if let Some(hits) = val.get_mut("hits").and_then(|h| h.as_array_mut()) {
                                let block: Vec<JsonValue> = std::mem::take(hits);
                                blocks.push(((1, node_id), block));
                            }
                            if let Some(total) = val.get("total_hits").and_then(|t| t.as_u64()) {
                                total_hits_sum += total as usize;
                            }
                            // Extract shard statistics from the response
                            if let Some(stats) = val.get("stats").and_then(|s| s.as_object())
                                && let Some(shards) =
                                    stats.get("shards").and_then(|s| s.as_object())
                                && let Some(responded) =
                                    shards.get("responded").and_then(|r| r.as_u64())
                            {
                                shards_queried += responded as usize;
                            }
                        }
                        Ok(Err(e)) => {
                            errors.push(format!("Remote node {} search failed: {}", node_id, e));
                        }
                        Err(_) => {
                            errors.push(format!(
                                "Remote node {} search failed: {}",
                                node_id,
                                OrchestratorError::Io(std::io::Error::new(
                                    std::io::ErrorKind::TimedOut,
                                    "Remote operation timed out",
                                ))
                            ));
                        }
                    }
                }

                // Ranked by source identity — this node's shards, then each peer — rather than
                // by which of them streamed in first, so that a tie between two hits is settled
                // the same way on every run, and so that a page is a function of the query
                // rather than of the network. Every source contributes: the caveat that used to
                // stand here, that early termination could change *which* of them did, went
                // with the early exit itself.
                blocks.sort_by_key(|(source, _)| *source);
                let all_hits = order_hit_blocks(
                    blocks.into_iter().map(|(_, block)| block).collect(),
                    sort.as_ref(),
                    window,
                );

                let mut response = serde_json::json!({
                    "hits": all_hits,
                    "hits_returned": all_hits.len(),
                    "total_hits": total_hits_sum,
                    "limit": window.limit,
                    // Reported for the same reason the non-streaming path reports it: a caller
                    // that cannot see which page it was given cannot tell a correct answer from
                    // page 1 returned twice, which is how the bug above stayed invisible.
                    "offset": window.offset,
                    "took_ms": started.elapsed().as_millis(),
                    "stats": {
                        "shards": {
                            "total": shards_queried,
                            "responded": shards_queried.saturating_sub(errors.len()),
                            "failed": errors.len()
                        },
                        "nodes": {
                            "contacted": nodes_contacted
                        }
                    },
                });
                attach_shard_errors(&mut response, errors);
                Ok(response)
            }
            _ => {
                // For non-search operations, fall back to broadcast request handling
                self.handle_broadcast_request(op).await
            }
        }
    }

    /// Broadcast request method for non-search operations
    pub(super) async fn handle_broadcast_request(
        &self,
        op: ClientOp,
    ) -> Result<JsonValue, OrchestratorError> {
        // Implementation for non-search operations (write, bulk_write, etc.)
        // This is the existing handle_broadcast logic
        self.handle_broadcast(op).await
    }

    /// Send an operation to the node that owns it, retrying only what is worth retrying.
    ///
    /// **A peer that answered is not a peer that failed.** Both used to end up here as an
    /// unclassified `Io`, so a deterministic refusal — a document whose type the schema will
    /// never accept — was sent twice more, wrapped in "remote routing failed after N attempts",
    /// answered `500`, and reported to the coordinator as a reason to redial the cluster. A
    /// malformed document nudged cluster membership.
    ///
    /// So an answer is returned as it arrived: not retried, because the second attempt reaches
    /// the same schema and the same verdict; not wrapped, because the wrapper described a
    /// transport that worked perfectly; and not reported as a dial failure, because it says
    /// nothing about connectivity. Only an attempt that failed to *reach* the peer is retried.
    pub(super) async fn handle_remote(
        &self,
        op: ClientOp,
        node_id: Uuid,
        peer_addr: String,
    ) -> Result<JsonValue, OrchestratorError> {
        let max_attempts = std::cmp::max(1, self.remote_retry_attempts as usize);
        let mut last_err = None;

        for attempt in 1..=max_attempts {
            match timeout(
                self.remote_timeout,
                self.try_remote(&op, node_id, &peer_addr),
            )
            .await
            {
                Ok(Ok(value)) => return Ok(value),
                // The peer handled the message and its answer was an error. That is the answer.
                Ok(Err(err @ OrchestratorError::Remote { .. })) => {
                    warn!(
                        %node_id,
                        %peer_addr,
                        error = %err,
                        verdict = ?err.verdict(),
                        "RouterActor: remote node refused the operation"
                    );
                    return Err(err);
                }
                Ok(Err(err)) => {
                    warn!(
                        %node_id,
                        %peer_addr,
                        attempt,
                        max_attempts,
                        error = %err,
                        "RouterActor: remote attempt failed"
                    );
                    last_err = Some(err);
                }
                Err(elapsed) => {
                    warn!(
                        %node_id,
                        %peer_addr,
                        attempt,
                        max_attempts,
                        timeout_ms = self.remote_timeout.as_millis(),
                        error = %elapsed,
                        "RouterActor: remote attempt timed out"
                    );
                    last_err = Some(OrchestratorError::Io(std::io::Error::other(
                        elapsed.to_string(),
                    )));
                }
            }
        }

        let reason = last_err
            .map(|e| {
                format!(
                    "remote routing failed after {} attempts: {}",
                    max_attempts, e
                )
            })
            .unwrap_or_else(|| "remote routing failed".to_string());

        let _ = self
            .coordinator
            .ask(RequestBootstrapRedial {
                reason: reason.clone(),
            })
            .await;

        // Every attempt failed to reach the peer. That is a transport problem, so the redial
        // above is right — but the caller's answer is "not now", not "this node is broken".
        Err(OrchestratorError::PeerUnreachable { message: reason })
    }
}

impl RouterActor {
    /// Attempt a remote call to a microshard on another node.
    /// Uses the cached RemotePeerPool to avoid repeated swarm registry lookups.
    ///
    /// **Borrows the operation.** `ask` serialises from a reference, so nothing here ever needed
    /// to own one — and taking it by value obliged every caller to hand over a copy it could not
    /// get back. The retry loop in [`handle_remote`](Self::handle_remote) paid that on every
    /// attempt including the first, so an ordinary cross-node write deep-cloned its whole
    /// document for a call that succeeded. A fan-out still clones once per peer, which is real:
    /// those futures run concurrently and each needs its own.
    pub(super) async fn try_remote(
        &self,
        op: &ClientOp,
        node_id: Uuid,
        peer_addr: &str,
    ) -> Result<JsonValue, OrchestratorError> {
        debug!(
            "🔎 Attempting remote call: node_id={}, addr={}",
            node_id, peer_addr
        );

        let remote = self
            .remote_peer_pool
            .get_orchestrator(node_id, ConnectionChannel::Operations)
            .await
            .map_err(|e| {
                warn!("❌ Remote actor lookup error: {}", e);
                OrchestratorError::Io(std::io::Error::other(e.to_string()))
            })?
            .ok_or_else(|| {
                warn!("❌ Remote orchestrator not found: node_id={}", node_id);
                OrchestratorError::PeerUnreachable {
                    message: format!("remote orchestrator for node {} not found", node_id),
                }
            })?;

        remote_answer(remote.ask(op).await)
    }
}
