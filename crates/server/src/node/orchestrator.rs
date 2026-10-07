//! The dispatch core: NodeOrchestrator, the worker engine and pool, and the write-path
//! machinery — grouping, forwarding and the borrowed-ctx bodies the two lanes share.
//!
//! Two things a reader might expect here are deliberately elsewhere, each because it is a
//! subsystem with invariants of its own rather than a step of dispatch: admission and load
//! accounting in [`super::admission`], and the routing-key ladder in [`super::routing`].

use super::*;

use futures::future::join_all;
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs;
use std::path::PathBuf;
use std::sync::{
    Arc,
    atomic::{AtomicI64, AtomicUsize, Ordering as AtomicOrdering},
};
use std::time::{Duration, Instant};

use anyhow::Result;
use arc_swap::ArcSwap;
use kameo::actor::ActorRef;
use kameo::message::{Context, Message};
use kameo::reply::DelegatedReply;
use kameo::{Actor, RemoteActor, remote_message};
use tokio::sync::mpsc;
use tokio::time::timeout;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::cluster_coordinator::{
    ClusterCoordinator, GetKnownPeers, GetShardAssignments, RegisterLocalShards, ShardMetadata,
};
use crate::query::parse_query_keywords;
use crate::remote_peer_pool::{ConnectionChannel, RemotePeerPool};

// Re-export SortSpec and SortOrder from storage crate
use cluster::{ConsistentRing, NodeIdentity, generate_tokens};
use serde_json::{Map as JsonMap, Value as JsonValue};
pub use storage::SortSpec;
use storage::{
    FieldDef, HybridStore, IndexSchema, SchemaFieldUpdate, ShardStatsTimings, StorageConfig,
    StoreError, TantivyFieldType,
};

/// A document on its way to a shard, still carrying where it sat in the batch that arrived.
///
/// The position is what lets every reason name the document it is about. It used to be dropped
/// the moment validation's rejects were filtered out, so nothing past that point could say
/// `document N:` — a routing failure or a failed shard batch went to the log and left the
/// response reporting fewer documents written than it received with nothing to explain the
/// difference.
#[derive(Debug, Clone)]
pub(super) struct Placed {
    pub(super) position: usize,
    pub(super) doc: DocPayload,
    pub(super) routing_key: Option<String>,
}

/// Where one document is going, or why it is going nowhere.
pub(super) type RoutingResult = Result<(Placed, Uuid), (usize, String)>;

/// Everything a bulk fan-out reads, borrowed off whichever side of the orchestrator/engine
/// split is running it.
///
/// The actor builds it from its own fields; a worker builds the identical view from the
/// engine's `ArcSwap` snapshots — the actor publishes every topology change to those
/// snapshots, so both lanes read the same shard map and ring. The bulk bodies are written
/// once against this so the two lanes cannot drift: same routing ladder, same grouping, same
/// one-hop forwarding bound, same per-item accounting. What is *not* here is schema
/// authority — that stays on the actor, which is why the engine's bulk write defers the
/// moment a batch needs one written.
pub(super) struct BulkCtx<'a> {
    shards: &'a HashMap<Uuid, MicroshardActor>,
    ring: &'a ConsistentRing,
    coordinator: Option<&'a ActorRef<ClusterCoordinator>>,
    remote_peer_pool: Option<&'a RemotePeerPool>,
}

impl BulkCtx<'_> {
    pub(super) fn first_shard_id(&self) -> Option<Uuid> {
        self.shards.keys().copied().next()
    }

    /// Route, group and serve a bulk write whose schema questions are already settled:
    /// local batches to their shards in parallel, remote batches to their owning nodes
    /// bounded to one hop, and a response that accounts for every document it was given.
    ///
    /// `pending` carries only the documents validation passed and `rejections` the reasons
    /// for the rest — one per position, which is what makes the accounting anchor derivable:
    /// the batch arrived as `pending + rejections` and every path below either writes a
    /// document or adds a reason.
    pub(super) async fn apply_bulk_write(
        &self,
        index: &str,
        pending: Vec<Placed>,
        mut rejections: Vec<String>,
        schema: &IndexSchema,
        forwarded: bool,
        start: Instant,
    ) -> Result<JsonValue, OrchestratorError> {
        let items_received = pending.len() + rejections.len();
        // First, route all documents to determine local vs remote
        let mut local_docs = Vec::new();
        let mut remote_docs = Vec::new();

        // Clone routing ring for parallel access
        let routing_ring = self.ring.clone();
        let first_shard_id = self.first_shard_id();

        // Schema-based routing: use routing field from schema instead of per-document routing_key
        let routing_field = schema.get_routing_field().to_string();

        // Route documents in parallel
        let routing_results: Vec<RoutingResult> = tokio::task::spawn_blocking(move || {
            pending
                .into_par_iter()
                .map(|mut placed| {
                    // The same ladder every other write path climbs, resolved against the
                    // routing field this batch already looked up once.
                    placed.routing_key = routing_key_for(
                        &routing_field,
                        placed.doc.routing_key.clone(),
                        &placed.doc.id,
                        &placed.doc.doc,
                    );

                    // Route to shard using consistent hash ring
                    let Some(key) = placed.routing_key.as_ref() else {
                        return Err((
                            placed.position,
                            "no routing key could be derived for this document".to_string(),
                        ));
                    };
                    let Some(target_shard) = routing_ring.get_owner(key).or(first_shard_id) else {
                        return Err((
                            placed.position,
                            "no shard is available to route this document to".to_string(),
                        ));
                    };

                    Ok((placed, target_shard))
                })
                .collect::<Vec<RoutingResult>>()
        })
        .await
        .map_err(|e| OrchestratorError::Io(std::io::Error::other(e.to_string())))?;

        // Separate local and remote documents. A document that routes nowhere is refused rather
        // than logged and forgotten: it was received, it will not be written, and the response
        // has to say so or the counts stop adding up.
        for result in routing_results {
            match result {
                Ok((placed, target_shard)) => {
                    if self.shards.contains_key(&target_shard) {
                        local_docs.push((placed, target_shard));
                    } else {
                        remote_docs.push((placed, target_shard));
                    }
                }
                Err((position, reason)) => {
                    tracing::warn!(position, reason, "Routing error");
                    rejections.push(format!("document {position}: {reason}"));
                }
            }
        }

        // Group local documents by shard
        let batches = NodeOrchestrator::group_local_documents(local_docs);
        let unique_shards = batches.len();

        tracing::debug!(
            items_received = items_received,
            unique_shards = unique_shards,
            remote_docs = remote_docs.len(),
            "BulkWrite grouped items by shard"
        );

        // Who owns a shard, and where that node is — asked only if some document actually
        // routed off this node.
        //
        // A batch that was itself forwarded here goes no further, for the reason
        // `forward_op_to_owner` gives for a single write: two nodes whose views of the ring
        // disagree would otherwise pass it between them, carrying every document each time,
        // until something timed out. This op has carried `forwarded` since OB12 and only the
        // schema decision ever read it.
        //
        // Ahead of the shard-assignment lookup below, so a batch that is going to be refused
        // does not ask the coordinator who owns what first.
        if forwarded && !remote_docs.is_empty() {
            rejections.extend(std::mem::take(&mut remote_docs).into_iter().map(
                |(placed, target_shard)| {
                    format!(
                        "document {}: forwarded here, but shard {target_shard} is not local \
                         either. This node and the one that forwarded disagree about who owns \
                         it; retry once the cluster has settled",
                        placed.position
                    )
                },
            ));
        }

        // Both maps are read in the remote branch below and nowhere else: the local/remote split
        // above uses `self.shards`, which this node already holds. Fetching them up front cost
        // two coordinator mailbox round trips on **every** bulk write, including every bulk write
        // on a single-node deployment, where the answer is always "everything is local". The
        // delete path next door already asked for its peers lazily; this is the same shape.
        let (shard_assignments, peer_addrs) = if remote_docs.is_empty() {
            (HashMap::new(), HashMap::new())
        } else if let Some(coord) = self.coordinator {
            let assignments = coord.ask(GetShardAssignments).await.unwrap_or_default();
            let addrs: HashMap<Uuid, String> = coord
                .ask(GetKnownPeers)
                .await
                .unwrap_or_default()
                .into_iter()
                .map(|p| (p.node_id, p.address))
                .collect();
            (assignments, addrs)
        } else {
            (HashMap::new(), HashMap::new())
        };

        // Separate local and remote batches for parallel processing
        let mut local_batches = HashMap::new();
        let mut remote_batches = Vec::new();
        let mut written = 0usize;
        // Seeded with the documents validation refused, so the response accounts for every
        // item it received: `items_written` counts what was stored, and each of the rest has a
        // reason here.
        let mut errors = rejections;

        // Process local batches from parallel routing
        for (shard_id, batch) in batches {
            local_batches.insert(shard_id, batch);
        }

        // Group remote documents by owning node
        let mut remote_by_node: HashMap<Uuid, Vec<Placed>> = HashMap::new();
        for (placed, target_shard) in remote_docs {
            match shard_assignments.get(&target_shard) {
                Some(shard_meta) => remote_by_node
                    .entry(shard_meta.node_id)
                    .or_default()
                    .push(placed),
                None => errors.push(format!(
                    "document {}: shard {target_shard} owns this document and no node claims \
                     that shard",
                    placed.position
                )),
            }
        }

        // Convert remote batches to the expected format
        for (node_id, batch) in remote_by_node {
            match peer_addrs.get(&node_id) {
                Some(addr) => {
                    tracing::debug!(
                        node = %node_id,
                        count = batch.len(),
                        "Forwarding bulk write batch to remote node"
                    );
                    remote_batches.push((node_id, addr.clone(), batch));
                }
                None => errors.extend(batch.iter().map(|placed| {
                    format!(
                        "document {}: node {node_id} owns this document and has no known address",
                        placed.position
                    )
                })),
            }
        }

        // Phase 3.1: Parallel Local Shard Processing
        let (local_written, local_errors) = self.local_shard_writes(index, local_batches).await?;
        written += local_written;
        errors.extend(local_errors);

        // Phase 3.2: Parallel Remote Forwarding
        if !remote_batches.is_empty() {
            use futures::future::join_all;

            // Borrowed once, outside: a shared reference is `Copy`, so each `async move`
            // below takes the reference and not the schema.
            let established: &IndexSchema = schema;
            let remote_futures: Vec<_> = remote_batches
                .into_iter()
                .map(|(node_id, addr, batch)| async move {
                    // Kept so a call that never reached the node can still name what it carried.
                    let positions: Vec<usize> =
                        batch.iter().map(|placed| placed.position).collect();
                    let outcome = self
                        .forward_write(node_id, &addr, index, batch, established)
                        .await;
                    (node_id, positions, outcome)
                })
                .collect();

            let remote_results = join_all(remote_futures).await;

            for (node_id, positions, outcome) in remote_results {
                match outcome {
                    Ok((items, reasons)) => {
                        written += items;
                        errors.extend(reasons);
                    }
                    // The batch never got an answer, so none of it was written.
                    Err(e) => errors.extend(positions.into_iter().map(|position| {
                        format!("document {position}: forwarding to node {node_id} failed: {e}")
                    })),
                }
            }
        }

        // Every item is either written or explained. Each path above accounts for what it
        // loses, so this is a check on that rather than a repair — a batch that reaches here
        // unbalanced has a path that stopped saying what it dropped, which is the defect this
        // arithmetic exists to catch.
        //
        // Asserted in a debug build and logged in a release one. The assertion is what makes an
        // unaccounting path a test failure; the log is what makes it findable in the build that
        // actually serves the caller the unbalanced answer.
        debug_assert_eq!(
            written + errors.len(),
            items_received,
            "a bulk write must account for every item it received"
        );
        if written + errors.len() != items_received {
            error!(
                index = %index,
                items_received = items_received,
                items_written = written,
                errors = errors.len(),
                "BulkWrite did not account for every item it received"
            );
        }

        let duration = start.elapsed();
        info!(
            index = %index,
            items_received = items_received,
            items_written = written,
            errors = errors.len(),
            duration_ms = duration.as_millis(),
            "BulkWrite completed"
        );

        if !errors.is_empty() {
            warn!(
                index = %index,
                error_count = errors.len(),
                "BulkWrite had some errors"
            );
        }

        Ok(serde_json::json!({
            "items_written": written,
            "items_received": items_received,
            "errors": errors,
            "duration_ms": duration.as_millis()
        }))
    }

    /// Serve a bulk delete: ids grouped onto local shards in parallel, ids whose shard lives
    /// elsewhere forwarded to the owning node bounded to one hop, and a response that
    /// accounts for every id it was given.
    ///
    /// A delete can never need a schema written — it carries no document that could present
    /// a field the schema does not know — so unlike [`apply_bulk_write`](Self::apply_bulk_write)
    /// there is no slow path behind this.
    pub(super) async fn apply_bulk_delete(
        &self,
        index: &str,
        docs: Vec<DeletePayload>,
        schema: &IndexSchema,
        forwarded: bool,
        start: Instant,
    ) -> Result<JsonValue, OrchestratorError> {
        let items_received = docs.len();

        let mut errors: Vec<String> = Vec::new();
        // Local work is a plain list of ids per shard: the routing key has already done its job
        // by the time a shard is chosen, and the storage layer deletes by key.
        let mut local_by_shard: HashMap<Uuid, Vec<String>> = HashMap::new();
        // Ids whose shard this node does not hold, kept with the shard that owns them and
        // resolved to nodes below — once, and only if there are any. Asking who owns a shard is
        // a coordinator mailbox round trip, and a single-node deployment never has an answer to
        // use: every shard it routes to is one it holds.
        let mut off_node: Vec<(Uuid, DeletePayload)> = Vec::new();

        for payload in docs {
            let id = payload.id().to_string();
            if id.trim().is_empty() {
                errors.push("an entry carried an empty id".to_string());
                continue;
            }
            let routing_key = payload.routing_key().map(str::to_string);

            let key = match effective_delete_routing_key(schema, &id, routing_key) {
                Ok(key) => key,
                Err(err) => {
                    errors.push(format!("{id}: {err}"));
                    continue;
                }
            };

            let Some(target) = self.ring.get_owner(&key).or_else(|| self.first_shard_id()) else {
                errors.push(format!("{id}: no shard available for routing"));
                continue;
            };

            if self.shards.contains_key(&target) {
                local_by_shard.entry(target).or_default().push(id);
            } else {
                off_node.push((target, payload));
            }
        }

        let mut remote_by_node: HashMap<Uuid, Vec<DeletePayload>> = HashMap::new();
        if !off_node.is_empty() {
            let shard_assignments = if let Some(coord) = self.coordinator {
                coord.ask(GetShardAssignments).await.unwrap_or_default()
            } else {
                HashMap::new()
            };
            for (target, payload) in off_node {
                match shard_assignments.get(&target) {
                    Some(shard_meta) => remote_by_node
                        .entry(shard_meta.node_id)
                        .or_default()
                        .push(payload),
                    None => errors.push(format!(
                        "{}: no shard assignment for shard {target}, so it was not deleted",
                        payload.id()
                    )),
                }
            }
        }

        let mut deleted = 0usize;

        // Local shards in parallel, serial within each: every shard has its own writer thread,
        // and one batch per shard is what makes this one transaction per shard.
        let local_futures: Vec<_> = local_by_shard
            .into_iter()
            .map(|(shard_id, ids)| {
                let shard = self.shards.get(&shard_id).cloned();
                let index_name = index.to_string();
                async move {
                    // Kept so a failure can name every id it took down with it, as the bulk
                    // write path keeps its positions for the same reason. One reason for a whole
                    // shard batch left the answer short by however many ids were in it.
                    let attributable = ids.clone();
                    let outcome = async {
                        let shard = shard.ok_or_else(|| {
                            OrchestratorError::Missing(format!("Local shard {shard_id} not found"))
                        })?;
                        shard.handle_batch_delete(index_name, ids).await
                    }
                    .await;
                    (shard_id, attributable, outcome)
                }
            })
            .collect();

        for (shard_id, ids, outcome) in futures::future::join_all(local_futures).await {
            match outcome {
                Ok(count) => deleted += count,
                Err(err) => errors.extend(ids.into_iter().map(|id| {
                    format!("{id}: shard {shard_id} did not take the batch this id was in: {err}")
                })),
            }
        }

        // A batch that was itself forwarded here goes no further. Both ends decide ownership
        // from their own view of the ring, and while membership changes those views disagree —
        // two nodes each certain the other owns the shard would otherwise pass the batch back
        // and forth, a full remote ask carrying every document each time, until something timed
        // out. Stating the disagreement per id is what `forward_op_to_owner` does for a single
        // delete; this is the same rule on the path OB3 left open.
        if forwarded && !remote_by_node.is_empty() {
            for (node_id, payloads) in std::mem::take(&mut remote_by_node) {
                errors.extend(payloads.iter().map(|doc| {
                    format!(
                        "{}: forwarded here, but this node does not own its shard either. This node \
                         and node {node_id} disagree about who does; retry once the \
                         cluster has settled",
                        doc.id()
                    )
                }));
            }
        }

        // Peers that own the rest.
        if !remote_by_node.is_empty() {
            let peer_addrs: HashMap<Uuid, String> = if let Some(coord) = self.coordinator {
                coord
                    .ask(GetKnownPeers)
                    .await
                    .unwrap_or_default()
                    .into_iter()
                    .map(|peer| (peer.node_id, peer.address))
                    .collect()
            } else {
                HashMap::new()
            };

            let mut remote_futures = Vec::new();
            for (node_id, payloads) in remote_by_node {
                match peer_addrs.get(&node_id) {
                    Some(addr) => {
                        let addr = addr.clone();
                        remote_futures.push(async move {
                            // Kept so a call that never reached the node can still name what it
                            // carried.
                            let ids: Vec<String> =
                                payloads.iter().map(|doc| doc.id().to_string()).collect();
                            let outcome =
                                self.forward_delete(node_id, &addr, index, payloads).await;
                            (node_id, ids, outcome)
                        });
                    }
                    // One reason per id rather than one for the group: the group is not what the
                    // caller sent or counts in.
                    None => errors.extend(payloads.iter().map(|doc| {
                        format!(
                            "{}: no address known for node {node_id}, which owns its shard",
                            doc.id()
                        )
                    })),
                }
            }

            for (node_id, ids, outcome) in futures::future::join_all(remote_futures).await {
                match outcome {
                    Ok((count, reasons)) => {
                        deleted += count;
                        errors.extend(reasons);
                    }
                    // The batch never got an answer, so none of it was deleted.
                    Err(err) => errors.extend(
                        ids.into_iter()
                            .map(|id| format!("{id}: forwarding to node {node_id} failed: {err}")),
                    ),
                }
            }
        }

        // Every id is either deleted or explained, as on the bulk write path. Each path above
        // accounts for what it loses, so this is a check on that rather than a repair — a batch
        // that reaches here unbalanced has a path that stopped saying what it dropped, which is
        // the defect this arithmetic exists to catch.
        //
        // Asserted in a debug build and logged in a release one: the assertion makes an
        // unaccounting path a test failure, the log makes it findable in the build that actually
        // serves the caller the unbalanced answer.
        debug_assert_eq!(
            deleted + errors.len(),
            items_received,
            "a bulk delete must account for every id it received"
        );
        if deleted + errors.len() != items_received {
            error!(
                index = %index,
                items_received = items_received,
                items_deleted = deleted,
                errors = errors.len(),
                "BulkDelete did not account for every id it received"
            );
        }

        let duration = start.elapsed();
        info!(
            index = %index,
            items_received = items_received,
            items_deleted = deleted,
            errors = errors.len(),
            duration_ms = duration.as_millis(),
            "BulkDelete completed"
        );

        Ok(serde_json::json!({
            "items_received": items_received,
            "items_deleted": deleted,
            "errors": errors,
            "duration_ms": duration.as_millis(),
        }))
    }

    /// Process one batch per local shard, all of them in parallel.
    pub(super) async fn local_shard_writes(
        &self,
        index: &str,
        local_batches: HashMap<Uuid, Vec<Placed>>,
    ) -> Result<(usize, Vec<String>), OrchestratorError> {
        if local_batches.is_empty() {
            return Ok((0, Vec::new()));
        }

        let total_docs: usize = local_batches.values().map(|v| v.len()).sum();
        let shard_count = local_batches.len();

        tracing::debug!(
            local_shard_count = shard_count,
            total_docs = total_docs,
            "Starting local shard processing"
        );

        let mut total_written = 0usize;
        let mut all_errors = Vec::new();

        // Process shards in parallel, but ensure serial access per shard
        // Each shard has its own Tantivy/Redb instance, so cross-shard parallelism is safe
        let mut local_futures = Vec::with_capacity(local_batches.len());
        for (shard_id, batch) in local_batches {
            let shard = self.shards.get(&shard_id).cloned();
            let index_name = index.to_string();

            local_futures.push(async move {
                tracing::debug!(
                    shard_id = %shard_id,
                    count = batch.len(),
                    "Processing bulk write batch for local shard"
                );

                // Kept so a failure can name every document it took down with it.
                let positions: Vec<usize> = batch.iter().map(|placed| placed.position).collect();

                let outcome = async {
                    let shard = shard.ok_or_else(|| {
                        OrchestratorError::Missing(format!("Local shard {} not found", shard_id))
                    })?;

                    let docs: Vec<DocPayload> = batch
                        .into_iter()
                        .map(|placed| DocPayload {
                            id: placed.doc.id,
                            routing_key: placed.routing_key,
                            doc: placed.doc.doc,
                        })
                        .collect();

                    // Each shard handles its own writes serially via its dedicated writer thread
                    // This prevents IndexWriter lock contention within the same shard
                    shard
                        .handle_batch_write(BatchWriteRequest {
                            index: index_name,
                            docs,
                        })
                        .await
                }
                .await;

                (shard_id, positions, outcome)
            });
        }

        let local_results = futures::future::join_all(local_futures).await;
        for (shard_id, positions, outcome) in local_results {
            match outcome {
                Ok(seq_ids) => {
                    tracing::debug!(
                        shard_id = %shard_id,
                        written_count = seq_ids.len(),
                        "Local shard batch completed successfully"
                    );
                    total_written += seq_ids.len();
                }
                Err(e) => {
                    tracing::warn!(
                        shard_id = %shard_id,
                        count = positions.len(),
                        error = %e,
                        "Local shard batch processing failed"
                    );
                    all_errors.extend(positions.into_iter().map(|position| {
                        format!("document {position}: shard {shard_id} did not take the batch this document was in: {e}")
                    }));
                }
            }
        }

        tracing::debug!(
            "Local shard processing completed - total_written: {}, errors: {}",
            total_written,
            all_errors.len()
        );

        // Commit strategy: rely on the two existing commit mechanisms:
        //   1. The writer thread's commit after each drain, once the interval since the oldest
        //      uncommitted write has passed (or the count backstop is reached).
        //   2. Supervisor idle-timeout commit (signal_supervisor called by handle_batch_write)
        //      — fires after the batch completes and no more writes arrive.
        //
        // No explicit commit here: it would be redundant with #1 (if threshold fired)
        // or premature (if the caller is about to send more batches). The supervisor
        // guarantees data is committed within the idle timeout window.

        Ok((total_written, all_errors))
    }

    /// Forward a bulk batch to a remote node's orchestrator.
    ///
    /// Returns what the peer wrote and a reason for every document it did not. The peer's own
    /// reasons used to be read off the response and thrown away — only `items_written` was kept
    /// — so a node refusing half a batch contributed nothing to `errors`, and the coordinating
    /// node answered 200 with a shortfall it could not explain.
    ///
    /// Uses the cached RemotePeerPool to avoid repeated swarm registry lookups.
    pub(super) async fn forward_write(
        &self,
        node_id: Uuid,
        peer_addr: &str,
        index: &str,
        batch: Vec<Placed>,
        established: &IndexSchema,
    ) -> Result<(usize, Vec<String>), OrchestratorError> {
        info!(
            "🔎 Forwarding bulk batch to remote: node_id={}, addr={}, docs={}",
            node_id,
            peer_addr,
            batch.len()
        );

        let positions: Vec<usize> = batch.iter().map(|placed| placed.position).collect();
        let docs: Vec<DocPayload> = batch
            .into_iter()
            .map(|placed| DocPayload {
                id: placed.doc.id,
                routing_key: placed.routing_key,
                doc: placed.doc.doc,
            })
            .collect();

        let pool = self.remote_peer_pool.ok_or_else(|| {
            OrchestratorError::NotReady("Remote peer pool not initialized".to_string())
        })?;

        // One bit, and no schema. `forwarded` tells the owner this share is someone else's
        // decision, so it neither samples nor canvasses; if it holds nothing to run the share
        // against it says so, and the resend below carries the body. Nothing about the schema
        // travels on a forward that does not need it.
        let op = ClientOp::BulkWrite {
            index: index.to_string(),
            docs,
            forwarded: true,
            schema_body: None,
            // A forwarded share never decides a schema, so it never stamps one. The stamp was
            // written by whichever node minted the index.
            tenant: None,
        };

        let res: serde_json::Value = pool
            .converse(node_id, async {
                let remote = lookup_peer_orchestrator(pool, node_id).await?;
                match remote_answer(remote.ask(&op).await) {
                    Err(err) if matches!(err.verdict(), RemoteVerdict::SchemaRequired) => {
                        let Some(resend) = with_schema_body(&op, established) else {
                            return Err(err);
                        };
                        debug!(
                            %node_id,
                            "Peer holds no schema for this index; resending the batch with the schema"
                        );
                        remote_answer(remote.ask(&resend).await)
                    }
                    other => other,
                }
            })
            .await?;

        let Some(items_written) = res.get("items_written").and_then(|v| v.as_u64()) else {
            return Err(OrchestratorError::Io(std::io::Error::other(
                "Invalid response from remote bulk write",
            )));
        };
        let written = (items_written as usize).min(positions.len());

        let reasons: Vec<String> = res
            .get("errors")
            .and_then(|v| v.as_array())
            .map(|errors| {
                errors
                    .iter()
                    .filter_map(|e| e.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();

        Ok((
            written,
            remote_rejections(node_id, &positions, written, &reasons),
        ))
    }

    /// Hand a peer the part of a bulk delete its shards own.
    pub(super) async fn forward_delete(
        &self,
        node_id: Uuid,
        peer_addr: &str,
        index: &str,
        docs: Vec<DeletePayload>,
    ) -> Result<(usize, Vec<String>), OrchestratorError> {
        debug!(
            %node_id,
            %peer_addr,
            count = docs.len(),
            "Forwarding bulk delete batch to remote node"
        );

        let pool = self.remote_peer_pool.ok_or_else(|| {
            OrchestratorError::NotReady("Remote peer pool not initialized".to_string())
        })?;

        // Kept so the peer's answer can be balanced against what it was actually given.
        let ids: Vec<String> = docs.iter().map(|doc| doc.id().to_string()).collect();

        let op = ClientOp::BulkDelete {
            index: index.to_string(),
            docs,
            // One hop. If the peer cannot place these either, it says so per id rather than
            // handing them on again.
            forwarded: true,
        };

        let answer: JsonValue = pool
            .converse(node_id, async {
                let remote = lookup_peer_orchestrator(pool, node_id).await?;
                remote_answer(remote.ask(&op).await)
            })
            .await?;

        let deleted = (answer
            .get("items_deleted")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as usize)
            .min(ids.len());

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

        if !reasons.is_empty() {
            warn!(
                %node_id,
                error_count = reasons.len(),
                "Remote node reported errors deleting its share of the batch"
            );
        }

        // Its reasons are the caller's reasons too. They used to be logged here and dropped,
        // so a peer that refused half its share was reported as a shortfall in `items_deleted`
        // with nothing to explain it — the caller could see that fewer ids were deleted than it
        // sent and could not learn which, or why. ROADMAP OB9.
        Ok((
            deleted,
            remote_delete_rejections(node_id, &ids, deleted, &reasons),
        ))
    }
}

/// The schema cache — one `ArcSwap` map behind a newtype, so how a schema is read, written
/// and ordered by version has a single home. The engine and the actor each hold one
/// `Arc<SchemaCache>`; a hit is the cached `Arc`, so no caller pays a deep clone of the field
/// map per request. Callers that mutate the schema clone out of the `Arc` themselves.
#[derive(Debug)]
pub(super) struct SchemaCache {
    map: ArcSwap<HashMap<String, Arc<IndexSchema>>>,
}

impl SchemaCache {
    pub(super) fn new() -> Self {
        SchemaCache {
            map: ArcSwap::from_pointee(HashMap::new()),
        }
    }

    /// The cached schema for `index`, or `None` on a miss. Shared, not owned.
    pub(super) fn get(&self, index: &str) -> Option<Arc<IndexSchema>> {
        self.map.load().get(index).cloned()
    }

    /// Insert a schema in the cache, unless the cache already holds a newer one.
    ///
    /// Version decides, not arrival order, so a write that resolved against a schema before
    /// it was dropped cannot put that schema back over the record of the deletion. Ordering
    /// by version costs a comparison and needs nothing coordinated between concurrent
    /// writers.
    pub(super) fn put(&self, index: &str, schema: &IndexSchema) {
        self.put_arc(index, Arc::new(schema.clone()));
    }

    /// [`put`](Self::put) for a caller that already holds the `Arc` — the store's own read of
    /// a schema is shared rather than cloned a second time.
    pub(super) fn put_arc(&self, index: &str, schema: Arc<IndexSchema>) {
        let index_str = index.to_string();

        self.map.rcu(|old| {
            if let Some(current) = old.get(&index_str)
                && current.version > schema.version
            {
                return Arc::clone(old);
            }
            let mut new = (**old).clone();
            new.insert(index_str.clone(), Arc::clone(&schema));
            Arc::new(new)
        });
    }

    /// Cache the schema a write's validation settled on, if it is not already the cached one.
    ///
    /// Validation changes a schema only through `Arc::make_mut`, so a handle that is no longer
    /// the cached `Arc` is a schema that changed — a mint, a peer's schema adopted, an
    /// evolution — and one that still is changed nothing. The rule used to be "an evolution, or
    /// nothing cached", which missed a mint over a dropped index's record: sampling adds the
    /// fields itself, so nothing evolved and the record stayed cached. Every later write then
    /// read "no schema" and minted again from its own documents, each overwriting the stored
    /// types while the Tantivy index kept the first mint's, until a shard's writer died on a
    /// value its column could not hold.
    pub(super) fn keep_settled(&self, index: &str, settled: &Arc<IndexSchema>) {
        if self
            .get(index)
            .is_none_or(|cached| !Arc::ptr_eq(&cached, settled))
        {
            // `put_arc`, not `put`: the handle is already an `Arc`, and `put` would deep-copy
            // the field map to build one.
            self.put_arc(index, Arc::clone(settled));
        }
    }

    /// Drop the cached entry outright. Deletion uses this before caching the record of the
    /// drop — an empty entry gives [`put`](Self::put) nothing to compare against, so a write
    /// in flight that resolved against the old schema would install it again.
    pub(super) fn remove(&self, index: &str) {
        let index = index.to_string();
        self.map.rcu(|old| {
            let mut new = (**old).clone();
            new.remove(&index);
            new
        });
    }

    /// The schema this node holds for `index`, or `None` if it holds none: cache, then the
    /// first shard's store.
    ///
    /// **Not the same question as [`get`](Self::get).** The cache is filled lazily by the
    /// first operation to touch an index, so a node that has just booted answers "nothing"
    /// for every index it holds on disk until something asks. Where the answer only decides
    /// whether to re-read, that is a miss. Where it is reported to another node it is a lie —
    /// [`NodeOrchestrator::peer_schema_for`] reads exactly that answer to decide whether a
    /// schema may be invented: a cold holder saying "none" licenses the divergence the gate
    /// exists to prevent. `get_schema_cached` rather than `get_schema` because it is what the
    /// write path resolves against — a schema derived from Tantivy and merged with the stored
    /// metadata, so validation and writing agree on the types.
    pub(super) async fn durable(
        &self,
        shards: &HashMap<Uuid, MicroshardActor>,
        index: &str,
    ) -> Result<Option<Arc<IndexSchema>>, OrchestratorError> {
        if let Some(cached) = self.get(index) {
            return Ok(Some(cached));
        }
        if let Some(schema) = schema_from_shards(shards, index).await? {
            self.put_arc(index, Arc::clone(&schema));
            return Ok(Some(schema));
        }
        Ok(None)
    }

    /// [`durable`](Self::durable) plus the empty-schema answer a miss means on the write
    /// path: an index this node has never seen starts with no declared fields.
    pub(super) async fn schema_for(
        &self,
        shards: &HashMap<Uuid, MicroshardActor>,
        index: &str,
    ) -> Result<Arc<IndexSchema>, OrchestratorError> {
        Ok(self
            .durable(shards, index)
            .await?
            .unwrap_or_else(|| Arc::new(IndexSchema::default())))
    }
}

/// The schema the first shard's store holds for `index`, or `None` when no shard has a store
/// or none holds one — the read every load-from-store path used to spell out for itself.
pub(super) async fn schema_from_shards(
    shards: &HashMap<Uuid, MicroshardActor>,
    index: &str,
) -> Result<Option<Arc<IndexSchema>>, OrchestratorError> {
    let Some(shard) = shards.values().next() else {
        return Ok(None);
    };
    let Some(store) = &shard.store else {
        return Ok(None);
    };
    schema_from_store(store, index).await
}

/// One store's answer for `index`, off the async runtime — `get_schema_cached` is a blocking
/// read. The double `map_err` is the join failure and the store error, collapsed into the
/// same `Io` verdict every caller used to spell out twice.
pub(super) async fn schema_from_store(
    store: &Arc<HybridStore>,
    index: &str,
) -> Result<Option<Arc<IndexSchema>>, OrchestratorError> {
    let sc = Arc::clone(store);
    let idx = index.to_string();
    tokio::task::spawn_blocking(move || sc.get_schema_cached(&idx))
        .await
        .map_err(|e| OrchestratorError::Io(std::io::Error::other(e.to_string())))?
        .map_err(|e| OrchestratorError::Io(std::io::Error::other(e.to_string())))
}

/// What the write gate settled. `Routed` carries what dispatch needs; `Grow` means the schema
/// is empty or must evolve — only the actor can do that, under the mailbox's serialisation.
pub(super) enum WriteGate {
    Routed {
        target: Uuid,
        effective_routing_key: Option<String>,
    },
    Grow,
}

/// A dispatched write, or its parts back when the ring's target is not on this node. What
/// `Elsewhere` means is the lane's call: the actor forwards to the owning node, a worker hands
/// the op back for the mailbox to retry.
pub(super) enum WriteDispatch {
    Done(JsonValue),
    Elsewhere {
        id: String,
        effective_routing_key: Option<String>,
        doc: JsonValue,
    },
}

/// The borrowed view a single write or delete routes and dispatches against — shard map, ring,
/// schema cache — identical on both lanes: the actor reads its own fields, a worker reads the
/// engine's `ArcSwap` snapshots. The shape `BulkCtx` established for the bulk fan-out.
pub(super) struct WriteCtx<'a> {
    shards: &'a HashMap<Uuid, MicroshardActor>,
    ring: &'a ConsistentRing,
    schema_cache: &'a SchemaCache,
}

impl WriteCtx<'_> {
    /// Route a write or delete to the shard the ring gives its key.
    ///
    /// The ring decides the shard, always — never the dispatch hint. The hint is
    /// `owner(routing_key.or(id))`, computed before the schema was in hand, while the
    /// effective key prefers the document's own routing field; on an index whose routing field
    /// is a real, non-key field those two disagree, and taking the hint used to put the
    /// document on a shard the ring does not believe owns it. Nothing looked wrong — searches
    /// are scatter-gather and found it anyway — until the same id was written again through a
    /// path with no hint and landed on a second shard.
    ///
    /// What the hint is for is choosing the worker, and it still does that in
    /// `try_send_affine`: same dense shard ordinal, same co-located writer thread, same saved
    /// cross-core wakeup. This lookup is an xxh3 and a `BTreeMap` range descent, which is not
    /// a saving worth a class of divergence in front of a redb transaction.
    pub(super) fn route_write(
        &self,
        routing_key: &Option<String>,
    ) -> Result<Uuid, OrchestratorError> {
        let key = routing_key.as_ref().ok_or_else(|| {
            OrchestratorError::Validation("Missing routing key for write".to_string())
        })?;

        let target = self
            .ring
            .get_owner(key)
            .or_else(|| self.shards.keys().copied().next());

        target.ok_or_else(|| OrchestratorError::NotReady("No shard selected".to_string()))
    }

    /// Validate `doc` against `schema` and, when the schema already covers every field, cache
    /// the schema and route the write. Anything else — an empty schema, a field the schema
    /// does not describe — is [`WriteGate::Grow`], the actor's to settle.
    pub(super) fn gate(
        &self,
        index: &str,
        id: &str,
        routing_key: &Option<String>,
        doc: &JsonValue,
        schema: &IndexSchema,
    ) -> Result<WriteGate, OrchestratorError> {
        if schema.fields.is_empty() {
            return Ok(WriteGate::Grow);
        }

        let result = validate_document(id, doc, schema);
        if let Some(err) = result.validation_error {
            return Err(OrchestratorError::Validation(err));
        }
        if result.needs_evolution {
            return Ok(WriteGate::Grow);
        }

        // Schema is stable — populate the cache if it does not hold this index yet.
        if self.schema_cache.get(index).is_none() {
            self.schema_cache.put(index, schema);
        }

        let effective_routing_key = effective_routing_key(schema, id, routing_key.clone(), doc);
        let target = self.route_write(&effective_routing_key)?;
        Ok(WriteGate::Routed {
            target,
            effective_routing_key,
        })
    }

    /// Dispatch a routed write to its shard. `Elsewhere` hands the parts back — the ring's
    /// target is not on this node, and what that means is the lane's call.
    pub(super) async fn dispatch(
        &self,
        target: Uuid,
        index: &str,
        id: String,
        effective_routing_key: Option<String>,
        doc: JsonValue,
    ) -> Result<WriteDispatch, OrchestratorError> {
        let Some(shard) = self.shards.get(&target) else {
            return Ok(WriteDispatch::Elsewhere {
                id,
                effective_routing_key,
                doc,
            });
        };

        let req = WriteRequest {
            index: index.to_string(),
            id: id.clone(),
            routing_key: effective_routing_key.unwrap_or_default(),
            doc,
        };

        let seq = shard.handle_write(req).await?;
        Ok(WriteDispatch::Done(serde_json::json!({
            "id": id, "result": "created", "version": seq,
            "shard_id": target.to_string()
        })))
    }

    /// Same shape for a delete: the shard answers, or `None` says the target is not local and
    /// the lane decides between forwarding and deferring.
    pub(super) async fn dispatch_delete(
        &self,
        target: Uuid,
        index: &str,
        id: &str,
    ) -> Result<Option<JsonValue>, OrchestratorError> {
        let Some(shard) = self.shards.get(&target) else {
            return Ok(None);
        };

        let sequence = shard
            .handle_delete(index.to_string(), id.to_string())
            .await?;
        Ok(Some(serde_json::json!({
            "id": id,
            "result": "deleted",
            "version": sequence,
            "shard_id": target.to_string(),
        })))
    }
}

/// Accumulator for broadcast search statistics across local and remote results.
pub(super) struct BroadcastStats {
    pub(super) total_shards_queried: usize,
    pub(super) nodes_contacted: usize,
    pub(super) max_took_ms: Option<u64>,
    pub(super) total_hits_sum: usize,
    /// Distinct dropped clauses across the nodes that answered.
    pub(super) discarded: Vec<String>,
    /// The approximated sort field, if any node reported one; see [`APPROXIMATE_SORT_FIELD`].
    pub(super) approximate_sort: Option<String>,
    /// The narrowed default fields, if any node reported them; see [`NARROWED_DEFAULT_FIELDS`].
    pub(super) narrowed_default_fields: Option<storage::NarrowedDefaultFields>,
}

/// Whether a shard's failure is the request's fault rather than this node's.
///
/// Everything crossing a shard actor arrives as an `io::Error` carrying a kind and a message —
/// `handle_search` maps the engine's error into one — so the kind is the whole classification.
/// `InvalidInput` and `InvalidData` are what the engine raises for a query or a field the index
/// cannot answer; anything else is this node failing to read data it owns, which says nothing
/// about what was asked.
pub(super) fn is_caller_error(err: &OrchestratorError) -> bool {
    match err {
        // `Validation` is the dedicated form; the `Io` arm stays because a genuine
        // `io::Error` can still arrive with the same kind from the `#[from]` conversion.
        OrchestratorError::Validation(_) => true,
        OrchestratorError::Io(io) => matches!(
            io.kind(),
            std::io::ErrorKind::InvalidInput | std::io::ErrorKind::InvalidData
        ),
        OrchestratorError::UnsortableField { .. }
        | OrchestratorError::UnrunnableQuery { .. }
        | OrchestratorError::NoShardAnswered { .. } => true,
        OrchestratorError::Storage(storage::StoreError::InvalidFieldValue { .. }) => true,
        _ => false,
    }
}

/// Who this node is, and how many shards it holds.
///
/// A free function because both the actor and the worker engine answer it, and the answer is
/// two fields and a length — the one metadata read with nothing behind it worth serialising
/// against a write. See [`OrchestratorEngine::execute`].
pub(super) fn identity_json(identity: &NodeIdentity, total_shards: usize) -> JsonValue {
    serde_json::json!({
        "node_id": identity.uuid.to_string(),
        "node_name": identity.name.clone(),
        "total_shards": total_shards,
    })
}

/// Helper function to detect if an operation is a write operation
pub(super) fn is_write_operation(op: &ClientOp) -> bool {
    matches!(
        op,
        ClientOp::Write { .. }
            | ClientOp::BulkWrite { .. }
            | ClientOp::Delete { .. }
            | ClientOp::BulkDelete { .. }
            | ClientOp::DeleteIndex { .. }
    )
}

// ============================================================================
// Enhanced Schema Sampling for Initial Creation
// ============================================================================

/// Type alias for shard hydration task results
pub(super) type ShardTaskResult = Result<(Uuid, Option<MicroshardActor>), OrchestratorError>;

/// Append names not already present, keeping the list sorted and free of duplicates.
///
/// Shard verdicts on a schema update overlap almost entirely — they are reading copies of the
/// same schema — so merging them is a union, not a concatenation.
pub(super) fn merge_names(into: &mut Vec<String>, from: Vec<String>) {
    for name in from {
        if !into.contains(&name) {
            into.push(name);
        }
    }
    into.sort();
}

/// Why a schema update was refused, phrased for whoever has to act on it.
///
/// Only an unknown field gets here. A field whose flag cannot take effect until the index is
/// rebuilt is applied and reported through `pending_reindex`, not refused.
pub(super) fn describe_schema_refusal(outcome: &SchemaFieldUpdate) -> String {
    format!(
        "Schema update refused, nothing was changed: no such field in this schema: {}",
        outcome.unknown.join(", ")
    )
}

/// What a caller has to do about a flag that cannot take effect yet.
pub(super) fn describe_pending_reindex(outcome: &SchemaFieldUpdate) -> String {
    format!(
        "Marked indexed and saved, but not searchable yet: {}. The index was built before these \
         fields were declared, so it has no column for them. Rebuilding the index data from the \
         schema is what makes them searchable — delete the index data without deleting the \
         schema, then re-ingest. Until then a query naming them matches nothing, and says so \
         rather than returning a narrower answer as though it were complete.",
        outcome.pending_reindex.join(", ")
    )
}

/// How long to wait for one peer to report its schema for an index.
///
/// Bounded because this sits on the write path: a peer that has stopped answering must not hold
/// a write open indefinitely. It is deliberately not the broadcast timeout — that belongs to the
/// router and this runs on the orchestrator — and a constant rather than configuration until
/// there is a reason to tune it, which a metadata read of a few hundred bytes is unlikely to
/// give. A peer that misses this window counts as unreachable, which refuses the write rather
/// than letting it invent a schema.
/// A peer's orchestrator from the pool, for use inside [`RemotePeerPool::converse`].
async fn lookup_peer_orchestrator(
    pool: &RemotePeerPool,
    node_id: Uuid,
) -> Result<kameo::actor::RemoteActorRef<NodeOrchestrator>, OrchestratorError> {
    pool.get_orchestrator(node_id, ConnectionChannel::Operations)
        .await
        .map_err(|e| OrchestratorError::Io(std::io::Error::other(e.to_string())))?
        .ok_or_else(|| OrchestratorError::PeerUnreachable {
            message: format!("Remote orchestrator for node {node_id} not found"),
        })
}

/// What a peer's answer to a schema canvass says it holds: a schema, none, or nothing readable.
///
/// **A dropped index's record is none.** The record exists so a write still carrying the dropped
/// schema cannot reinstall it, not to describe an index — its fields are gone. Read as a schema,
/// it was adopted: the index then counted as existing, so every field of the write that asked
/// became an addition, recorded but not searchable, which is the failure the record was kept to
/// prevent. A schema with no fields is none for the same reason `schema_to_carry` never sends
/// one. A peer on an older build still answers with the record, so this side judges it too.
///
/// A peer that answered with something unreadable is not a peer that answered "no schema": the
/// error counts it as unreachable, so it cannot license sampling.
pub(super) fn held_schema(answer: JsonValue) -> Result<Option<IndexSchema>, String> {
    if answer.is_null() {
        return Ok(None);
    }
    let mut schema = serde_json::from_value::<IndexSchema>(answer)
        .map_err(|e| format!("unreadable schema from a peer: {e}"))?;
    if schema.state == storage::SchemaState::Dropped || schema.fields.is_empty() {
        return Ok(None);
    }
    schema.normalize_after_deserialization();
    Ok(Some(schema))
}

pub(super) const PEER_SCHEMA_LOOKUP_TIMEOUT: Duration = Duration::from_secs(5);

/// How long a prepared schema change holds its index on a node when neither its apply nor its
/// release arrives — the node coordinating it stopped between the two. Long enough to cover
/// both phases' peer round trips, short enough that a retry is not shut out for long.
pub(super) const SCHEMA_CHANGE_RESERVATION: Duration = Duration::from_secs(60);

/// The schema body to send after a receiver asked for it, or `None` if there is none to send.
///
/// **An empty schema is never sent.** A receiver adopts what it is handed and then treats the
/// index as one that already exists, so every field of the document arriving with it would
/// become an *addition* to a known schema — recorded but not searchable, pending a rebuild.
pub(super) fn schema_to_carry(schema: &IndexSchema) -> Option<Box<IndexSchema>> {
    (!schema.fields.is_empty()).then(|| Box::new(schema.clone()))
}

/// The same write again, with the schema body attached, for a peer that asked for it.
///
/// The clone here is the one place a forward copies a schema, and it is on the cold path only:
/// a peer asks once per index, the first time a document for it lands there. Every other forward
/// carries **nothing** — see [`ClientOp::BulkWrite::forwarded`].
pub(super) fn with_schema_body(op: &ClientOp, schema: &IndexSchema) -> Option<ClientOp> {
    let body = schema_to_carry(schema)?;
    let mut resend = op.clone();
    match &mut resend {
        ClientOp::Write { schema_body, .. } | ClientOp::BulkWrite { schema_body, .. } => {
            *schema_body = Some(body);
        }
        // Nothing else validates against a schema, so nothing else can be asked for one.
        _ => return None,
    }
    Some(resend)
}

/// What the rest of the cluster holds for an index this node has no schema for.
///
/// See [`OrchestratorEngine::peer_schema_for`]. `NoneHeld` and `Unreachable` are kept apart
/// deliberately: only the first licenses this node to build a schema from the documents.
pub(super) enum PeerSchemaLookup {
    /// A peer holds a schema for this index, and this is the one to adopt.
    Found(Box<IndexSchema>),
    /// Every peer answered and none holds a schema, so this index is genuinely new.
    ///
    /// `dropped_at` is the highest version of a dropped index's record any of them holds, `0`
    /// when none does. A mint goes above it, so a peer still holding the record adopts the new
    /// index as newer rather than keeping the record over it.
    NoneHeld { dropped_at: u64 },
    /// The cluster could not be asked, so nothing can be concluded from the silence.
    Unreachable { reason: String },
    /// No peer holds a schema, and these peers are minting one for the same index right now.
    ///
    /// A race, and one that must end with a single schema: the node that asked and each of
    /// these know of one another, so every one of them settles it the same way — the lowest
    /// node id mints and the rest adopt its schema. See `MintAfterCanvass`.
    Contested { rivals: Vec<Uuid> },
}

/// Everything a canvass of the peers for a schema needs, and nothing that ties it to the actor.
///
/// Cheap to clone: a flag and two handles. The canvass waits on peers — up to
/// [`PEER_SCHEMA_LOOKUP_TIMEOUT`] each — and each peer answers through its own orchestrator
/// mailbox. Run from inside this node's mailbox, that wait is what made two nodes canvassing at
/// once wait on each other until the timeout; held here instead, it can run on a worker or a
/// spawned task, and the mailbox stays free to answer the peers' canvasses of this node.
#[derive(Clone)]
pub(super) struct SchemaCanvass {
    /// `[network.cluster] enabled`. A standalone node has nobody to ask.
    pub(super) clustered: bool,
    pub(super) coordinator: Option<ActorRef<ClusterCoordinator>>,
    pub(super) pool: Option<Arc<RemotePeerPool>>,
}

impl SchemaCanvass {
    /// What the cluster already knows about an index this node has no schema for.
    ///
    /// Three outcomes, and the difference between the last two is the whole point: "nobody has
    /// one" licenses this node to build a schema by sampling, while "I could not ask everybody"
    /// does not, and they are indistinguishable if unreachable peers are counted as silent.
    ///
    /// `minting_by` is this node's id when the canvass is for a mint of its own, so that a peer
    /// minting the same index learns of the race too; `None` for a lookup that creates nothing.
    pub(super) async fn peer_schema_for(
        &self,
        index: &str,
        minting_by: Option<Uuid>,
    ) -> PeerSchemaLookup {
        use crate::cluster_coordinator::{GetKnownPeers, GetStatus, KnownPeer};

        // The standalone arm, taken before anything is asked of anyone. A node with clustering
        // off is the whole system: there is nobody to disagree with, and sampling a schema from
        // the documents is the feature that makes semi-structured input work. `clustered` is
        // static configuration, so this costs no coordinator round trip — the previous form read
        // the same fact out of `GetStatus`, which meant a mailbox hop on the first write to
        // every new index on a node that has no peers by construction.
        if !self.clustered {
            return PeerSchemaLookup::NoneHeld { dropped_at: 0 };
        }

        let Some(coordinator) = self.coordinator.as_ref() else {
            return PeerSchemaLookup::NoneHeld { dropped_at: 0 };
        };
        let Ok(status): Result<crate::distributed::ClusterStatus, _> =
            coordinator.ask(GetStatus).await
        else {
            return PeerSchemaLookup::Unreachable {
                reason: "the cluster coordinator did not answer".to_string(),
            };
        };

        // Every configured member has to be reachable, not merely every member currently known.
        // A node that boots alone while its peers are down knows only itself, so "ask all known
        // peers" would be satisfied by asking nobody — which is exactly the case that produced
        // three schemas for one index. `total_nodes` is the configured member count, so this
        // compares against what the operator said the cluster is.
        if status.connected_nodes < status.total_nodes {
            return PeerSchemaLookup::Unreachable {
                // States the fact and leaves the consequence to the caller: this answer now
                // reaches a `DELETE` deciding whether an index exists as well as a write
                // deciding whether it may invent a schema, and "a schema created now" is
                // nonsense in the first case.
                reason: format!(
                    "only {} of {} cluster nodes are connected, so no answer covers the whole \
                     cluster",
                    status.connected_nodes, status.total_nodes
                ),
            };
        }

        let peers: Vec<KnownPeer> = match self.coordinator.as_ref() {
            Some(coordinator) => coordinator.ask(GetKnownPeers).await.unwrap_or_default(),
            None => Vec::new(),
        };
        if peers.is_empty() {
            return PeerSchemaLookup::NoneHeld { dropped_at: 0 };
        }

        let Some(pool) = self.pool.clone() else {
            return PeerSchemaLookup::Unreachable {
                reason: "no remote peer pool on this node, so no peer can be asked".to_string(),
            };
        };

        let op = ClientOp::GetRawSchema {
            index: index.to_string(),
            minting_by,
        };
        let answers = futures::future::join_all(peers.into_iter().map(|peer| {
            let op = op.clone();
            let pool = pool.clone();
            async move {
                let node = peer.node_id;
                let ask = async {
                    let remote = pool
                        .get_orchestrator(node, ConnectionChannel::Operations)
                        .await
                        .map_err(|e| PeerAnswer::Failed(format!("node {node} lookup failed: {e}")))?
                        .ok_or_else(|| {
                            PeerAnswer::Failed(format!("node {node} has no reachable orchestrator"))
                        })?;
                    // The verdict survives the hop, so a rival is told apart from a failure by
                    // type. The peer is the one asked, so its id needs no parsing out of text.
                    match remote_answer(remote.ask(&op).await) {
                        Ok(value) => Ok(value),
                        Err(err) if err.verdict() == RemoteVerdict::Minting => {
                            Err(PeerAnswer::Minting(node))
                        }
                        Err(err) => Err(PeerAnswer::Failed(format!("node {node}: {err}"))),
                    }
                };
                timeout(PEER_SCHEMA_LOOKUP_TIMEOUT, ask)
                    .await
                    .unwrap_or_else(|_| Err(PeerAnswer::Failed(format!("node {node} timed out"))))
            }
        }))
        .await;

        let mut best: Option<IndexSchema> = None;
        let mut unreachable = Vec::new();
        let mut rivals = Vec::new();
        let mut dropped_at = 0u64;
        for answer in answers {
            match answer {
                Err(PeerAnswer::Failed(why)) => unreachable.push(why),
                Err(PeerAnswer::Minting(node)) => rivals.push(node),
                Ok(value) => match held_schema(value.clone()) {
                    Ok(Some(schema)) => {
                        best = Some(match best.take() {
                            None => schema,
                            Some(current) => NodeOrchestrator::preferred_schema(current, schema),
                        });
                    }
                    Ok(None) => dropped_at = dropped_at.max(dropped_version(&value)),
                    Err(why) => unreachable.push(why),
                },
            }
        }

        // A schema found from any peer settles it even if another peer was unreachable: the
        // declaration exists, and adopting it is strictly better than inventing a second one.
        if let Some(schema) = best {
            return PeerSchemaLookup::Found(Box::new(schema));
        }
        if !unreachable.is_empty() {
            return PeerSchemaLookup::Unreachable {
                reason: unreachable.join("; "),
            };
        }
        if !rivals.is_empty() {
            return PeerSchemaLookup::Contested { rivals };
        }
        PeerSchemaLookup::NoneHeld { dropped_at }
    }
}

/// The version of a dropped index's record a peer answered with, `0` for any other answer.
pub(super) fn dropped_version(answer: &JsonValue) -> u64 {
    let dropped = answer.get("state").and_then(JsonValue::as_str) == Some("dropped");
    if !dropped {
        return 0;
    }
    answer
        .get("version")
        .and_then(JsonValue::as_u64)
        .unwrap_or(0)
}

/// One peer's answer to a canvass, when it is not a schema or `null`.
enum PeerAnswer {
    /// The peer is minting this index right now.
    Minting(Uuid),
    /// The peer could not be asked, or did not answer in time.
    Failed(String),
}

/// The answer to [`ClientOp::FindSchemaInCluster`]: this node's own schema if it holds one,
/// otherwise what a canvass of the peers finds.
///
/// Shared by the worker, which normally serves it, and the actor, which serves it when no
/// worker can. Either way the canvass holds no mailbox.
pub(super) async fn find_schema_in_cluster(
    held: Option<Arc<IndexSchema>>,
    canvass: &SchemaCanvass,
    index: String,
) -> Result<JsonValue, OrchestratorError> {
    let to_json = |schema: &IndexSchema| {
        serde_json::to_value(schema).map_err(|e| OrchestratorError::Io(std::io::Error::other(e)))
    };
    // This node first. A holder answers from its own store without asking anyone, which covers
    // every standalone node and the ordinary clustered case.
    if let Some(schema) = held {
        return to_json(&schema);
    }
    match canvass.peer_schema_for(&index, None).await {
        PeerSchemaLookup::Found(schema) => to_json(&schema),
        PeerSchemaLookup::NoneHeld { .. } => Ok(JsonValue::Null),
        // Being created, so not absent — and not yet there to report either.
        PeerSchemaLookup::Contested { rivals } => Err(OrchestratorError::SchemaUnconfirmed {
            index,
            reason: format!("node(s) {rivals:?} are creating this index right now"),
        }),
        // Not `null`: nobody said the index is absent, only that the cluster could not be
        // canvassed. A caller that reads a partial view as "absent" reports a missing index
        // while a node that holds it is merely unreachable.
        PeerSchemaLookup::Unreachable { reason } => {
            Err(OrchestratorError::SchemaUnconfirmed { index, reason })
        }
    }
}

/// `GET /_config` for an index this node holds no schema for: the schema its peers hold, marked
/// `held_here: false`. Its fields are reported as declared — searchable when indexed, sortable
/// when fast — since this node has built nothing to read that from. Not found only when every
/// peer answered and none holds one.
pub(super) async fn peer_config_response(
    shards: &HashMap<Uuid, MicroshardActor>,
    canvass: &SchemaCanvass,
    index: String,
) -> Result<JsonValue, OrchestratorError> {
    let schema = match canvass.peer_schema_for(&index, None).await {
        PeerSchemaLookup::Found(schema) => schema,
        PeerSchemaLookup::NoneHeld { .. } => {
            return Err(OrchestratorError::Storage(StoreError::IndexNotFound(index)));
        }
        PeerSchemaLookup::Contested { rivals } => {
            return Err(OrchestratorError::SchemaUnconfirmed {
                index,
                reason: format!("node(s) {rivals:?} are creating this index right now"),
            });
        }
        PeerSchemaLookup::Unreachable { reason } => {
            return Err(OrchestratorError::SchemaUnconfirmed { index, reason });
        }
    };
    let searchable: HashSet<String> = schema
        .fields
        .iter()
        .filter(|(_, field)| field.indexed && !field.is_shadow)
        .map(|(name, _)| name.clone())
        .collect();
    let sortable: HashSet<String> = schema
        .fields
        .iter()
        .filter(|(_, field)| field.indexed && !field.is_shadow && field.is_fast())
        .map(|(name, _)| name.clone())
        .collect();
    let max_default_fields = shards
        .values()
        .find_map(|shard| shard.store.as_ref())
        .map_or_else(
            || storage::QueryPolicy::default().max_default_fields,
            |store| store.query_policy().max_default_fields,
        );
    let mut response = NodeOrchestrator::schema_response(
        &index,
        &schema,
        &searchable,
        &sortable,
        max_default_fields,
    );
    if let Some(map) = response.as_object_mut() {
        map.insert("held_here".to_string(), JsonValue::Bool(false));
    }
    Ok(response)
}

/// What a mailbox op produced: its answer, or the rest of the work, to finish off the mailbox.
///
/// `Later` is how an op that must wait on a peer gives the mailbox back first. The actor does
/// what needs `&mut self` — deciding and saving a schema — and hands back a future that owns
/// everything else it needs; the handler runs it in a task and answers through the delegated
/// reply. Waiting inside the mailbox instead is what let two nodes wait on each other: each
/// forwarding to the other's mailbox from inside its own, until the peer timeout (60 s).
#[derive(kameo::Reply)]
pub(super) enum Answer {
    Now(Result<JsonValue, OrchestratorError>),
    Later(futures::future::BoxFuture<'static, Result<JsonValue, OrchestratorError>>),
}

impl Answer {
    /// The answer, waiting for it here if it was deferred. For callers that are not the mailbox.
    pub(super) async fn resolve(self) -> Result<JsonValue, OrchestratorError> {
        match self {
            Self::Now(result) => result,
            Self::Later(rest) => rest.await,
        }
    }
}

impl From<Result<Answer, OrchestratorError>> for Answer {
    fn from(result: Result<Answer, OrchestratorError>) -> Self {
        result.unwrap_or_else(|err| Self::Now(Err(err)))
    }
}

/// [`BulkCtx`], owned: snapshots and handles a task can carry away from the actor.
///
/// The shard map is the engine's published snapshot and the ring the shared one — the actor
/// publishes every topology change to both, which is what the worker lane already relies on —
/// so a fan-out run from here reads exactly what the actor would have.
pub(super) struct OwnedBulkView {
    shards: Arc<HashMap<Uuid, MicroshardActor>>,
    ring: Arc<ConsistentRing>,
    coordinator: Option<ActorRef<ClusterCoordinator>>,
    pool: Option<Arc<RemotePeerPool>>,
}

impl OwnedBulkView {
    fn ctx(&self) -> BulkCtx<'_> {
        BulkCtx {
            shards: &self.shards,
            ring: &self.ring,
            coordinator: self.coordinator.as_ref(),
            remote_peer_pool: self.pool.as_deref(),
        }
    }
}

/// Whether the worker lane serves this op — on a worker for this node's own requests, on the
/// peer lane for a peer's.
///
/// What is left out needs `&mut NodeOrchestrator` (config and schema edits, index deletes) or
/// is answered from actor state alone. An op in the list that turns out to need the actor after
/// all — a write that must grow a schema, a share with none to run against — is handed back as
/// [`WorkerOutcome::UseActor`].
pub(crate) fn worker_eligible(op: &ClientOp) -> bool {
    matches!(
        op,
        ClientOp::Write { .. }
            | ClientOp::Delete { .. }
            | ClientOp::Search { .. }
            | ClientOp::Stream { .. }
            // Bulk ops fan out over the same snapshots the rest of the engine reads. What they
            // cannot do off the mailbox is *decide* a schema — a bulk write that needs one written
            // hands itself back as `UseActor`, which is the fast/slow split the single-write path
            // already uses.
            | ClientOp::BulkWrite { .. }
            | ClientOp::BulkDelete { .. }
            // A metadata read with no actor state behind it. On the mailbox it queued behind
            // whatever write was there; the pool answers it from an ArcSwap.
            | ClientOp::GetIdentity
            // The index listing asks only `&self` questions of the shard map — stats gathered per
            // shard, one schema per index, an identity that never changes. `ListClusterIndexes`
            // lands here only as the local half of a broadcast, which is the same listing
            // (ROADMAP CH12).
            | ClientOp::ListIndexes { .. }
            | ClientOp::ListClusterIndexes { .. }
            // Waits on peers, so it must not wait on this node's mailbox: see the engine's arm
            // for it in `execute`.
            | ClientOp::FindSchemaInCluster { .. }
            // Asks the peers when this node holds no schema, for the same reason; one it holds
            // goes on to the actor as before.
            | ClientOp::GetConfig { .. }
    )
}

/// What a worker did with an op.
///
/// The engine cannot serve every op — schema evolution and bulk writes need `&mut
/// NodeOrchestrator`. Rather than signalling that with a sentinel error, which leaves the
/// caller holding nothing to retry with, [`WorkerOutcome::UseActor`] sends the op itself
/// home. The caller moved the op into the job, so this is the only way it can get it back
/// without cloning every document on the way in.
pub(super) enum WorkerOutcome {
    /// The engine handled it. Success or failure, this is the client's answer.
    Done(Result<JsonValue, OrchestratorError>),
    /// The engine declined; retry this op on the actor mailbox.
    UseActor(Box<ClientOp>),
}

/// The engine's verdict on a single write, before it becomes a [`WorkerOutcome`].
pub(super) enum WriteOutcome {
    Done(JsonValue),
    /// The schema has to grow, or the shard the write routes to is on another node. Carries back
    /// the parts of `ClientOp::Write` that `engine_write` consumed, so `execute` can rebuild the
    /// op — `index` it still owns.
    NeedsActor {
        id: String,
        routing_key: Option<String>,
        doc: JsonValue,
    },
}

/// The engine's verdict on a single delete, before it becomes a [`WorkerOutcome`].
pub(super) enum DeleteOutcome {
    Done(JsonValue),
    /// The shard the delete routes to is on another node. Carries back the parts of
    /// `ClientOp::Delete` that `engine_delete` consumed, so `execute` can rebuild the op.
    NeedsActor {
        id: String,
        routing_key: Option<String>,
    },
}

/// The engine's verdict on a bulk write, before it becomes a [`WorkerOutcome`].
pub(super) enum BulkOutcome {
    Done(JsonValue),
    /// The schema has to be written — the index is still unsettled, or a document carries a
    /// field it does not know. Writing a schema is the actor's, serially: two bulks evolving
    /// the same index at once is exactly what the mailbox keeps from happening. The batch
    /// travels back whole so `execute` can rebuild the op without a clone.
    NeedsActor {
        docs: Vec<DocPayload>,
    },
}

/// A job dispatched to the orchestrator worker pool.
/// Workers execute the operation on shared state and send the result
/// back via the oneshot channel, bypassing the actor mailbox.
pub(super) enum OrchestratorJob {
    Execute {
        /// When the request that produced this job reached the node.
        ///
        /// Stamped at the HTTP boundary (`REQUEST_STARTED_AT`), not at dispatch — a request
        /// that spent its budget being received arrives here already expired, and the dequeue
        /// check below reads this against the node's request timeout to refuse work rather
        /// than run it for a client that has gone. This channel is where the backlog forms
        /// under overload — the measurement is in ROADMAP F7 — and nothing downstream of it
        /// is cancellable.
        arrived_at: Instant,
        op: Box<ClientOp>,
        /// Shard affinity hint for dispatch. When Some, the job was routed to
        /// a worker determined by `xxh3(shard_id) % worker_count`. Passed to
        /// `engine.execute()` so `engine_write` can skip the redundant ring lookup.
        affinity_shard: Option<Uuid>,
        reply: tokio::sync::oneshot::Sender<WorkerOutcome>,
    },
    Shutdown,
}

/// The cores this process may actually use, resolved once at startup.
///
/// Two sources disagree, and both matter. `core_affinity::get_core_ids()` enumerates the
/// cores a thread can be pinned to; `available_parallelism()` respects a cgroup CPU quota
/// that pinning cannot see. Under `docker --cpus=4` on a 32-core host the first reports 32
/// and the second 4 — and the worker pool used to size itself from one while writer pinning
/// indexed into the other, so the co-location the whole design exists for quietly stopped
/// holding. Resolving both here once means every placement decision counts the same cores.
#[derive(Clone, Debug)]
pub(super) struct CoreLayout {
    /// How many cores this process may use. Sizes the worker pool, and is meaningful even
    /// where pinning is unsupported.
    pub(super) budget: usize,
    /// Cores that can actually be pinned to, capped to `budget`. Empty when the platform
    /// cannot enumerate them, in which case every pinning path degrades to unpinned.
    pub(super) cores: Vec<core_affinity::CoreId>,
}

impl CoreLayout {
    pub(super) fn detect() -> Self {
        let budget = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
            .max(1);
        let mut cores = core_affinity::get_core_ids().unwrap_or_default();
        // A quota-limited process is told it has fewer cores than it can see. Honour the
        // smaller number: pinning threads across cores the scheduler will not give us time
        // on spreads the work without spreading the CPU.
        cores.truncate(budget);
        Self { budget, cores }
    }

    /// Cores available for sizing decisions.
    pub(super) fn budget(&self) -> usize {
        self.budget
    }

    /// The core an ordinal maps to, or `None` when pinning is unavailable here.
    pub(super) fn core_for(&self, ordinal: usize) -> Option<core_affinity::CoreId> {
        if self.cores.is_empty() {
            None
        } else {
            Some(self.cores[ordinal % self.cores.len()])
        }
    }

    pub(super) fn pinning_available(&self) -> bool {
        !self.cores.is_empty()
    }
}

/// Dense, stable placement of this node's shards.
///
/// A shard is given an ordinal the first time it appears and keeps it for the life of the
/// process. That ordinal — not `xxh3(shard_id)` — chooses both the worker that handles the
/// shard's writes and the core its writer thread is pinned to, which is what makes the two
/// agree.
///
/// Hashing was the original scheme and it collides: the hash domain is the shard set, which
/// is smaller than the core count. Measured with the shipped defaults — 4 shards, 8 cores —
/// 40 affine writes reached 3 of 8 workers, five of them idle by construction. Ordinals
/// reach exactly `min(shards, workers)`, which is the real ceiling anyway: each shard has
/// one writer thread that serialises its writes, so a shard cannot use more than one worker
/// no matter how the mapping is drawn.
///
/// Ordinals are assigned in the order shards appear rather than by sorting the set, because
/// a writer thread pins itself when its shard starts. Re-sorting on every membership change
/// would leave already-pinned writers on cores that no longer match their worker.
#[derive(Clone, Debug, Default)]
/// `pub(crate)` for the same reason as [`OrchestratorWorkerTx`]: it crosses the boundary as
/// the return of [`NodeOrchestrator::shard_placement`], not as a name anyone writes.
pub(crate) struct ShardPlacement {
    pub(super) slots: HashMap<Uuid, ShardSlot>,
    /// Shards actually serving on this node. A superset relationship with `slots` is the
    /// point: an ordinal is handed out before a shard starts, because its writer thread pins
    /// itself as it spawns, but the shard only becomes routable once it is in the shard map.
    /// A shard that fails to hydrate, or that the `max_shards` cap turns away, keeps its
    /// ordinal and never becomes live — claiming it locally would route writes to a shard
    /// this node cannot serve.
    pub(super) live: HashSet<Uuid>,
    pub(super) next: usize,
}

/// A shard's place in the pool, and where its writer thread actually ended up.
#[derive(Clone, Debug)]
pub(super) struct ShardSlot {
    pub(super) ordinal: usize,
    /// Core the writer thread was asked to take. `None` when pinning is off or the platform
    /// cannot enumerate cores.
    pub(super) target_core: Option<usize>,
    /// Core the writer thread reports it is running on, or [`UNPINNED`] if the request was
    /// refused. Shared with the thread, which writes it once at startup — a request is not
    /// an outcome, and on macOS every request is refused.
    pub(super) pinned_core: Arc<AtomicI64>,
}

/// `pinned_core` sentinel: this thread is not pinned to anything.
pub(super) const UNPINNED: i64 = -1;

/// What a shard's writer thread should pin to, and where it reports what happened.
#[derive(Clone, Debug)]
pub(super) struct WriterPin {
    pub(super) target: Option<core_affinity::CoreId>,
    pub(super) outcome: Arc<AtomicI64>,
}

impl WriterPin {
    /// Pin the calling thread, and record where it actually landed.
    pub(super) fn apply(&self, shard_id: Uuid) {
        let Some(target) = self.target else {
            return;
        };
        if core_affinity::set_for_current(target) {
            self.outcome
                .store(target.id as i64, AtomicOrdering::Relaxed);
            info!(
                shard_id = %shard_id,
                core_id = target.id,
                "Writer thread pinned to CPU core"
            );
        } else if cfg!(target_os = "macos") {
            // `set_for_current` is a no-op on macOS; not a fault worth warning on.
            info!(
                shard_id = %shard_id,
                core_id = target.id,
                "CPU pinning not supported on macOS; writer thread continuing unpinned"
            );
        } else {
            warn!(
                shard_id = %shard_id,
                core_id = target.id,
                "Failed to pin writer thread to CPU core (continuing unpinned)"
            );
        }
    }
}

impl ShardPlacement {
    /// Give `shard` its slot, or return the one it already has.
    ///
    /// Idempotent on purpose: a shard that starts twice keeps the core its writer already
    /// pinned to, and keeps reporting through the same cell.
    pub(super) fn assign(
        &mut self,
        shard: Uuid,
        layout: &CoreLayout,
        pin_writers: bool,
    ) -> ShardSlot {
        if let Some(slot) = self.slots.get(&shard) {
            return slot.clone();
        }
        let ordinal = self.next;
        self.next += 1;
        let target_core = pin_writers.then(|| layout.core_for(ordinal)).flatten();
        let slot = ShardSlot {
            ordinal,
            target_core: target_core.map(|core| core.id),
            pinned_core: Arc::new(AtomicI64::new(UNPINNED)),
        };
        self.slots.insert(shard, slot.clone());
        slot
    }

    /// Mark a shard as serving. Called once it is in the shard map and can take work.
    pub(super) fn activate(&mut self, shard: Uuid) {
        self.live.insert(shard);
    }

    pub(super) fn ordinal(&self, shard: &Uuid) -> Option<usize> {
        self.slots.get(shard).map(|slot| slot.ordinal)
    }

    /// Whether this node serves the shard. The routing ring names a shard for a key; this
    /// answers whether that shard lives here, which is the whole of a local routing
    /// decision.
    pub(super) fn is_local(&self, shard: &Uuid) -> bool {
        self.live.contains(shard)
    }

    /// Per-shard placement for `/_admin/workers`, ordered by ordinal so the report reads as
    /// the pool is laid out.
    pub(super) fn report(&self) -> Vec<ShardPlacementStats> {
        let mut shards: Vec<ShardPlacementStats> = self
            .slots
            .iter()
            .map(|(shard_id, slot)| ShardPlacementStats {
                shard_id: shard_id.to_string(),
                ordinal: slot.ordinal,
                serving: self.live.contains(shard_id),
                target_core_id: slot.target_core,
                core_id: match slot.pinned_core.load(AtomicOrdering::Relaxed) {
                    UNPINNED => None,
                    core => Some(core as usize),
                },
            })
            .collect();
        shards.sort_by_key(|shard| shard.ordinal);
        shards
    }
}

/// A worker carries several operations at once, up to `max_in_flight`.
///
/// It used to await `execute` inline, which made `worker_count` the node's operation
/// concurrency — and an operation is mostly spent *awaiting* the shard writer rather than
/// burning CPU, so the pool sat idle while requests queued. Worth +65-70% write throughput
/// and −64% on p90 at concurrency 64 on an 8-core node (ROADMAP "Worker concurrency,
/// measured").
///
/// The win is a saturation fix and nothing more: where `worker_count` already covers what
/// the client has outstanding, the same sweep is flat. It was also expected to redeem
/// shard-affine dispatch, whose regression had been blamed on this loop halving the node's
/// concurrency — it did not, and that flag stays off for a different reason.
///
/// The permit is acquired **before** `recv`, so a worker only pulls a job it has capacity to
/// start. That keeps the mpsc channel as the backpressure signal it already was: a saturated
/// worker stops draining, its queue fills, and `try_send_affine` falls through to a neighbour
/// exactly as before. Spawning first and bounding later would drain the queue instantly and
/// turn a bounded channel into an unbounded task pile.
///
/// Operations for one shard can now overlap inside a worker. Nothing regresses: they still
/// serialise at that shard's single writer thread, and concurrent requests never had a
/// cross-request ordering guarantee — round-robin dispatch already spread one shard's writes
/// across every worker in the pool.
///
/// `run_op` is how an accepted job becomes an answer — in production a call into
/// [`OrchestratorEngine::execute`]. It is a parameter rather than the engine itself so the
/// properties above can be tested against an operation whose timing the test controls;
/// nothing here depends on what the operation does, only on how many may run at once.
/// How far past its request budget an admitted operation may run before the pool takes its slot
/// back, and the absolute ceiling on that however large the budget is.
///
/// **This is a liveness backstop, not a deadline.** [F7] deliberately lets admitted work finish:
/// a cap tight enough to act as a deadline would shed work that was about to succeed, which is
/// the failure F7 was written to remove. The multiple is therefore generous — an operation this
/// far past its budget is not slow, it is stuck — and the point is only that one such operation
/// costs one slot instead of the whole pool. [OB14] is the case in hand: four parked writes took
/// the width, and with the width gone the node could not serve a search or answer its own health
/// probe.
///
/// Firing this is a defect report. It logs at error and increments `jobs_dropped`; neither is a
/// normal shedding path, and a node showing either has a bug to find rather than a knob to turn.
///
/// [F7]: the request timeout shedding the client rather than the work.
/// [OB14]: the shard-writer deadlock found by the first M6 arm, 2026-09-25.
pub(super) const WORKER_LIVENESS_MULTIPLE: u32 = 20;
const WORKER_LIVENESS_CEILING: Duration = Duration::from_secs(300);

/// Everything an admitted job holds in the pool, released on drop however the job ends.
///
/// The two gauges and the permit used to be released by statements at the tail of the spawned
/// task, so a future that parked, was dropped or panicked kept all three — and a pool that has
/// lost its width serves nothing at all, reads included. [OB14] reached that state one slot per
/// timed-out request. Tying the release to a scope instead of to the task reaching its last line
/// makes the accounting true by construction rather than by the happy path being taken.
///
/// `jobs_completed` counts operations that ran to their own conclusion; one that leaves without
/// finishing — dropped, panicked, or stopped by the liveness cap — increments `jobs_dropped`
/// instead. That is a defect report and is expected to read zero. Note the two are about the
/// *work*, not about the caller: a capped job still answers, with an error, so nobody is left
/// waiting on a channel that will never be written.
///
/// [OB14]: the shard-writer deadlock found by the first M6 arm, 2026-09-25.
struct PoolSlot {
    counters: Option<Arc<WorkerCounters>>,
    stats: Option<Arc<DispatchCounters>>,
    finished: bool,
    /// Held for the lifetime of the guard; released by its destructor.
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl PoolSlot {
    fn enter(
        counters: Option<Arc<WorkerCounters>>,
        stats: Option<Arc<DispatchCounters>>,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> Self {
        if let Some(c) = &counters {
            c.in_flight.fetch_add(1, AtomicOrdering::Relaxed);
        }
        Self {
            counters,
            stats,
            finished: false,
            _permit: permit,
        }
    }

    /// Record that the operation ran to its own conclusion, so the drop counts it as completed
    /// rather than dropped.
    fn finished(&mut self) {
        self.finished = true;
    }
}

impl Drop for PoolSlot {
    fn drop(&mut self) {
        if let Some(c) = &self.counters {
            c.in_flight.fetch_sub(1, AtomicOrdering::Relaxed);
            if self.finished {
                c.jobs_completed.fetch_add(1, AtomicOrdering::Relaxed);
            }
        }
        if let Some(s) = &self.stats {
            s.job_left_pool();
            if !self.finished {
                s.jobs_dropped.fetch_add(1, AtomicOrdering::Relaxed);
            }
        }
    }
}

/// Refuse a job that has outlived its request, and account for it leaving the pool.
///
/// Split out because the decision is taken twice — once before waiting for a permit and once
/// after one is in hand — and the two must agree on what they count and what they answer.
fn shed_stale_job(
    dispatch_stats: &Option<Arc<DispatchCounters>>,
    reply: tokio::sync::oneshot::Sender<WorkerOutcome>,
    waited: Duration,
    budget: Duration,
) {
    if let Some(stats) = dispatch_stats {
        stats.abandoned.fetch_add(1, AtomicOrdering::Relaxed);
        stats.job_left_pool();
    }
    let _ = reply.send(WorkerOutcome::Done(Err(
        OrchestratorError::ReadDeadlineExpired {
            waited_ms: waited.as_millis() as u64,
            budget_ms: budget.as_millis() as u64,
        },
    )));
}

pub(super) async fn orchestrator_worker_loop<F, Fut>(
    mut rx: mpsc::Receiver<OrchestratorJob>,
    run_op: F,
    worker_id: usize,
    counters: Option<Arc<WorkerCounters>>,
    max_in_flight: usize,
    // How long a job may wait in the queue before it is refused rather than run, and where to
    // count the refusals. `None` runs every job however stale — the behaviour before F7.
    budget: Option<Duration>,
    dispatch_stats: Option<Arc<DispatchCounters>>,
) where
    F: Fn(Box<ClientOp>, Option<Uuid>) -> Fut + Clone + Send + 'static,
    Fut: std::future::Future<Output = WorkerOutcome> + Send + 'static,
{
    let width = max_in_flight.max(1);
    let in_flight = Arc::new(tokio::sync::Semaphore::new(width));
    loop {
        match rx.recv().await {
            Some(OrchestratorJob::Execute {
                arrived_at,
                op,
                affinity_shard,
                reply,
            }) => {
                let run_op = run_op.clone();
                let counters = counters.clone();
                if let Some(c) = &counters {
                    c.queue_depth.fetch_sub(1, AtomicOrdering::Relaxed);
                }

                // The F7 check, and it has to be here rather than deeper. Everything past this
                // point is uncancellable: the operation runs, the search reaches the read pool
                // as a blocking closure tokio will never interrupt, and the answer is produced
                // whether or not anyone is still waiting for it. Under sustained overload that
                // was *every* request — the node stayed fully busy and delivered nothing.
                //
                // Refusing here costs one comparison and gives the permit straight back, so the
                // queue drains at the rate work is offered rather than the rate it can be
                // served, and the node returns to serving whatever it can actually keep up with.
                //
                // `arrived_at` is the request's arrival at the node, not its enqueue: the body
                // read, parse and routing it already paid for are spent budget too, and a
                // request that arrived expired — a max-size record at the derived timeout is
                // the standing example — is refused here rather than run for nobody.
                let service_class = OpClass::of(&op);

                // How much budget a job of this class needs left over to be worth starting.
                // Not `waited > budget`: admitting a job with barely any budget left is a
                // slower way of wasting the work — the queue settles exactly on the deadline,
                // every job that passes spends its remaining microseconds being searched for,
                // and the answer still lands after the client has gone. Measured: 11,656 jobs
                // shed and goodput still zero, because the 13,363 that passed all finished late.
                let reserve_for = |budget: Duration| {
                    dispatch_stats
                        .as_ref()
                        .map(|s| s.service_reserve_for(service_class, budget))
                        .unwrap_or_default()
                };

                // **Before** waiting for capacity, not after. The loop used to take a permit and
                // then receive, which meant that once parked jobs held the width no worker ever
                // reached this comparison — the one guard that sheds stale work was unreachable
                // in precisely the state it exists for. OB14 measured that: `abandoned` stayed 0
                // through arms where every job waited seconds against a 1s budget. Checking here
                // also means an already-dead job never occupies a slot a live one could use.
                if let Some(budget) = budget {
                    let waited = arrived_at.elapsed();
                    if waited + reserve_for(budget) > budget {
                        shed_stale_job(&dispatch_stats, reply, waited, budget);
                        continue;
                    }
                }

                // Capacity, waited for only by a job that still has budget to spend.
                // Never closed while the loop runs, so this only fails if the semaphore is dropped.
                let Ok(permit) = Arc::clone(&in_flight).acquire_owned().await else {
                    break;
                };

                // The wait for a permit is itself spent budget, and under saturation it is most
                // of it. A job that was worth starting a moment ago may not be now.
                if let Some(budget) = budget {
                    let waited = arrived_at.elapsed();
                    if waited + reserve_for(budget) > budget {
                        drop(permit);
                        shed_stale_job(&dispatch_stats, reply, waited, budget);
                        continue;
                    }
                }

                // On the pinned path this spawns onto the worker's own current_thread
                // runtime, so the operation stays on that core and pinning still means what
                // it says. On the default path it spawns onto the shared multi-threaded
                // runtime, where a worker is an admission-control unit rather than a place.
                let service_stats = dispatch_stats.clone();
                // No budget means the pre-F7 contract — every job runs, however long it takes —
                // and that is left reachable rather than quietly capped.
                let liveness_cap =
                    budget.map(|b| (b * WORKER_LIVENESS_MULTIPLE).min(WORKER_LIVENESS_CEILING));
                // Run the op inside the request's arrival scope: `request_started_at` is
                // inherited by `spawn`, so deadline checks reached from inside the op — the
                // read pool's, or a re-dispatch's — measure against the same clock this
                // dequeue check did rather than a fresh one.
                tokio::spawn(REQUEST_STARTED_AT.scope(arrived_at, async move {
                    // Everything this job holds — both gauges, the completion tally and the
                    // permit — is released by this guard, so it is released however the job
                    // ends. The releases used to be statements at the tail of this task, which
                    // meant a future that parked or was dropped kept all four forever: OB14 lost
                    // one slot per timed-out request that way, until the width was gone and with
                    // it every worker-eligible operation on the node.
                    let mut slot = PoolSlot::enter(counters, service_stats.clone(), permit);

                    let started = Instant::now();
                    // `finished` is whether the operation ran to its own conclusion, which is a
                    // different question from whether the caller got an answer. A capped job
                    // answers — with an error, so nobody is left hanging — but it did not finish,
                    // so it is counted as dropped and its duration is not folded into the service
                    // estimate. A stuck operation's elapsed time is not a service time, and
                    // admitting the next job against it would spread the damage.
                    let (result, finished) = match liveness_cap {
                        Some(cap) => match tokio::time::timeout(cap, run_op(op, affinity_shard))
                            .await
                        {
                            Ok(result) => (result, true),
                            Err(_) => {
                                // The operation is abandoned, not cancelled: anything it handed
                                // to a blocking pool or a writer thread runs on and replies into
                                // a dropped receiver. What is reclaimed here is the pool slot,
                                // which is the resource whose loss takes the node down.
                                error!(
                                    worker_id = worker_id,
                                    class = ?service_class,
                                    cap_ms = cap.as_millis() as u64,
                                    "Worker operation passed the liveness cap and was abandoned; \
                                     its pool slot has been reclaimed so the node keeps serving. \
                                     This is a defect — an operation should not reach this."
                                );
                                (
                                    WorkerOutcome::Done(Err(OrchestratorError::Io(
                                        std::io::Error::other(
                                            "operation exceeded the worker liveness cap",
                                        ),
                                    ))),
                                    false,
                                )
                            }
                        },
                        None => (run_op(op, affinity_shard).await, true),
                    };

                    // What this job actually cost once admitted, which is what the next job's
                    // admission decision is measured against.
                    if finished {
                        if let Some(stats) = &service_stats {
                            stats.record_service(service_class, started.elapsed());
                        }
                        slot.finished();
                    }
                    // Ignore the error: the caller may have given up and dropped the receiver.
                    let _ = reply.send(result);
                }));
            }
            Some(OrchestratorJob::Shutdown) => {
                debug!(
                    worker_id = worker_id,
                    "Orchestrator worker received shutdown signal"
                );
                break;
            }
            None => {
                debug!(worker_id = worker_id, "Orchestrator worker exiting");
                break;
            }
        }
    }

    // Drain before returning. Operations run as spawned tasks now, and on the pinned path
    // this function is the argument to `block_on` — returning drops the worker's
    // current_thread runtime, which cancels every task still on it. That would abandon
    // accepted writes at shutdown and hand their callers a dropped oneshot instead of an
    // answer. Reacquiring the full width waits for exactly the in-flight set, because a
    // permit comes back only when its task has replied.
    let _ = in_flight.acquire_many(width as u32).await;
    debug!(
        worker_id = worker_id,
        "Orchestrator worker drained in-flight operations"
    );
}

#[derive(Clone, Debug)]
/// `pub(crate)`: returned by [`NodeOrchestrator::worker_tx`], which `main.rs` calls when it
/// wires the router. Named nowhere outside `node/` — the callers bind it by inference.
pub(crate) struct OrchestratorWorkerTx {
    pub(super) workers: Arc<Vec<mpsc::Sender<OrchestratorJob>>>,
    pub(super) next_worker: Arc<AtomicUsize>,
    /// Per-worker atomic counters for observability.
    pub(super) worker_stats: Arc<Vec<Arc<WorkerCounters>>>,
    /// Dispatch-level counters across all workers.
    pub(super) dispatch_stats: Arc<DispatchCounters>,
    /// The backlog estimate the send path refuses against, shared with the HTTP front door so
    /// both refuse on one number.
    pub(super) queue_load: Arc<QueueLoad>,
    /// Per-worker channel capacity (same for all workers).
    pub(super) per_worker_queue_capacity: usize,
    /// Whether pinned worker threads were requested and the platform could enumerate
    /// cores. Whether they took is per-thread, and lives in `WorkerCounters::pinned_core`.
    pub(super) pinning_requested: bool,
    /// Whether worker_count was aligned to the core budget for writer co-location.
    pub(super) core_aligned: bool,
    /// Core layout used for pinning and for reporting which core a worker sits on.
    pub(super) core_layout: CoreLayout,
    /// Shard ordinals — the map from a shard to the worker that owns its writes.
    pub(super) placement: Arc<ArcSwap<ShardPlacement>>,
}

impl OrchestratorWorkerTx {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new_with_stats(
        workers: Vec<mpsc::Sender<OrchestratorJob>>,
        worker_stats: Arc<Vec<Arc<WorkerCounters>>>,
        dispatch_stats: Arc<DispatchCounters>,
        per_worker_queue_capacity: usize,
        pinning_requested: bool,
        core_aligned: bool,
        core_layout: CoreLayout,
        placement: Arc<ArcSwap<ShardPlacement>>,
        budget: Option<Duration>,
    ) -> Self {
        let queue_load = Arc::new(
            QueueLoad::new(
                Arc::clone(&dispatch_stats),
                workers.len() * ORCHESTRATOR_WORKER_MAX_IN_FLIGHT,
                budget,
            )
            .tail_aware(),
        );
        Self {
            workers: Arc::new(workers),
            next_worker: Arc::new(AtomicUsize::new(0)),
            worker_stats,
            dispatch_stats,
            queue_load,
            per_worker_queue_capacity,
            pinning_requested,
            core_aligned,
            core_layout,
            placement,
        }
    }

    pub(super) fn len(&self) -> usize {
        self.workers.len()
    }

    /// The backlog estimate this pool refuses against. Borrowed, because the dispatch path
    /// consults it on every request and an `Arc` bump per request is a cost with no purpose.
    pub(super) fn load(&self) -> &QueueLoad {
        &self.queue_load
    }

    /// The same estimate as a handle, for the HTTP front door to hold. Called once at startup.
    pub(super) fn queue_load(&self) -> Arc<QueueLoad> {
        Arc::clone(&self.queue_load)
    }

    pub(super) fn try_send(
        &self,
        mut job: OrchestratorJob,
    ) -> Result<(), Box<mpsc::error::TrySendError<OrchestratorJob>>> {
        if self.workers.is_empty() {
            return Err(Box::new(mpsc::error::TrySendError::Closed(job)));
        }

        self.dispatch_stats
            .round_robin_sends
            .fetch_add(1, AtomicOrdering::Relaxed);
        let start = self.next_worker.fetch_add(1, AtomicOrdering::Relaxed);
        let mut saw_full = false;

        for offset in 0..self.workers.len() {
            let idx = (start + offset) % self.workers.len();
            match self.workers[idx].try_send(job) {
                Ok(()) => {
                    self.worker_stats[idx]
                        .queue_depth
                        .fetch_add(1, AtomicOrdering::Relaxed);
                    self.dispatch_stats
                        .outstanding
                        .fetch_add(1, AtomicOrdering::Relaxed);
                    return Ok(());
                }
                Err(mpsc::error::TrySendError::Full(returned_job)) => {
                    saw_full = true;
                    job = returned_job;
                }
                Err(mpsc::error::TrySendError::Closed(returned_job)) => {
                    job = returned_job;
                }
            }
        }

        if saw_full {
            self.dispatch_stats
                .actor_mailbox_fallbacks
                .fetch_add(1, AtomicOrdering::Relaxed);
            Err(Box::new(mpsc::error::TrySendError::Full(job)))
        } else {
            Err(Box::new(mpsc::error::TrySendError::Closed(job)))
        }
    }

    /// Shard-affine dispatch: route the job to the worker that "owns" the given
    /// shard, falling through to neighboring workers on `Full` to preserve
    /// throughput. When `shard_id` is `None`, falls back to round-robin.
    pub(super) fn try_send_affine(
        &self,
        mut job: OrchestratorJob,
        shard_id: Option<Uuid>,
    ) -> Result<(), Box<mpsc::error::TrySendError<OrchestratorJob>>> {
        if self.workers.is_empty() {
            return Err(Box::new(mpsc::error::TrySendError::Closed(job)));
        }

        // Deterministic worker selection: same shard → same worker, via the shard's dense
        // ordinal. A shard with no ordinal is one this node does not own — nothing to be
        // affine to — so it round-robins like an unkeyed job.
        let affine_start = shard_id
            .and_then(|sid| self.placement.load().ordinal(&sid))
            .map(|ordinal| ordinal % self.workers.len());
        let is_affine = affine_start.is_some();
        let start = match affine_start {
            Some(start) => start,
            None => self.next_worker.fetch_add(1, AtomicOrdering::Relaxed),
        };

        let mut saw_full = false;
        let mut fell_back = false;

        for offset in 0..self.workers.len() {
            let idx = (start + offset) % self.workers.len();
            match self.workers[idx].try_send(job) {
                Ok(()) => {
                    self.worker_stats[idx]
                        .queue_depth
                        .fetch_add(1, AtomicOrdering::Relaxed);
                    self.dispatch_stats
                        .outstanding
                        .fetch_add(1, AtomicOrdering::Relaxed);
                    if is_affine {
                        if offset == 0 {
                            self.dispatch_stats
                                .affine_sends
                                .fetch_add(1, AtomicOrdering::Relaxed);
                        } else {
                            self.dispatch_stats
                                .affine_full_fallbacks
                                .fetch_add(1, AtomicOrdering::Relaxed);
                        }
                    } else {
                        self.dispatch_stats
                            .round_robin_sends
                            .fetch_add(1, AtomicOrdering::Relaxed);
                    }
                    return Ok(());
                }
                Err(mpsc::error::TrySendError::Full(returned_job)) => {
                    saw_full = true;
                    fell_back = true;
                    job = returned_job;
                }
                Err(mpsc::error::TrySendError::Closed(returned_job)) => {
                    job = returned_job;
                }
            }
        }

        if saw_full {
            if is_affine && fell_back {
                self.dispatch_stats
                    .affine_full_fallbacks
                    .fetch_add(1, AtomicOrdering::Relaxed);
            }
            self.dispatch_stats
                .actor_mailbox_fallbacks
                .fetch_add(1, AtomicOrdering::Relaxed);
            Err(Box::new(mpsc::error::TrySendError::Full(job)))
        } else {
            Err(Box::new(mpsc::error::TrySendError::Closed(job)))
        }
    }

    pub(super) async fn send_shutdown(&self) {
        for worker in self.workers.iter() {
            if worker.send(OrchestratorJob::Shutdown).await.is_err() {
                break;
            }
        }
    }

    /// Produce a snapshot of all worker and dispatch counters for `/_admin/workers`.
    pub(super) fn snapshot(&self) -> WorkerPoolReport {
        let workers: Vec<WorkerStats> = self
            .worker_stats
            .iter()
            .enumerate()
            .map(|(id, counters)| {
                // What was asked for, and what the thread reports it got. They differ
                // wherever `set_for_current` is refused, which is every call on macOS.
                let target_core_id = self
                    .pinning_requested
                    .then(|| self.core_layout.core_for(id).map(|core| core.id))
                    .flatten();
                let core_id = match counters.pinned_core.load(AtomicOrdering::Relaxed) {
                    UNPINNED => None,
                    core => Some(core as usize),
                };
                WorkerStats {
                    id,
                    target_core_id,
                    core_id,
                    queue_depth: counters.queue_depth.load(AtomicOrdering::Relaxed),
                    queue_capacity: self.per_worker_queue_capacity,
                    in_flight: counters.in_flight.load(AtomicOrdering::Relaxed),
                    in_flight_capacity: ORCHESTRATOR_WORKER_MAX_IN_FLIGHT,
                    jobs_completed: counters.jobs_completed.load(AtomicOrdering::Relaxed),
                }
            })
            .collect();
        let pinned_workers = workers
            .iter()
            .filter(|worker| worker.core_id.is_some())
            .count();

        let dispatch = DispatchStats {
            affine_sends: self
                .dispatch_stats
                .affine_sends
                .load(AtomicOrdering::Relaxed),
            affine_full_fallbacks: self
                .dispatch_stats
                .affine_full_fallbacks
                .load(AtomicOrdering::Relaxed),
            round_robin_sends: self
                .dispatch_stats
                .round_robin_sends
                .load(AtomicOrdering::Relaxed),
            actor_mailbox_fallbacks: self
                .dispatch_stats
                .actor_mailbox_fallbacks
                .load(AtomicOrdering::Relaxed),
            abandoned: self.dispatch_stats.abandoned.load(AtomicOrdering::Relaxed),
            refused_at_admission: self
                .dispatch_stats
                .refused_at_admission
                .load(AtomicOrdering::Relaxed),
            jobs_dropped: self
                .dispatch_stats
                .jobs_dropped
                .load(AtomicOrdering::Relaxed),
        };

        WorkerPoolReport {
            pinning_requested: self.pinning_requested,
            pinned_workers,
            core_aligned: self.core_aligned,
            worker_count: self.workers.len(),
            workers,
            shards: self.placement.load().report(),
            dispatch,
        }
    }
}

/// Shared state for the orchestrator worker pool.
///
/// This struct is wrapped in `Arc` and shared across all worker tasks.
/// All fields are either immutable after construction, lock-free (`ArcSwap`),
/// or inherently thread-safe (`ActorRef`, `mpsc::Sender`).
///
/// The `shards` and `routing_ring` fields use `ArcSwap` so that topology
/// updates from the actor can be published without locking, and workers
/// always read the latest snapshot.
pub(super) struct OrchestratorEngine {
    /// Shard map — updated atomically on topology changes via ArcSwap.
    pub(super) shards: ArcSwap<HashMap<Uuid, MicroshardActor>>,
    /// Consistent hash ring — single shared instance across the engine and
    /// the `RouterActor` (shard-affine dispatch). Updated atomically via
    /// `ArcSwap::store` on topology changes; readers always see the latest snapshot.
    pub(super) routing_ring: Arc<ArcSwap<ConsistentRing>>,
    /// Per-index schema cache (lock-free via ArcSwap).
    ///
    /// Keyed by index name, which is the only thing that identifies an index. A reverse
    /// lookup keyed by a hash of the field names used to sit in front of this and answered
    /// with whichever index of that shape was cached last — see `IndexSchema::calculate_fingerprint`.
    pub(super) schema_cache: Arc<SchemaCache>,
    /// Tenant ceilings — the byte check runs on this lane's writes too.
    pub(super) quotas: Arc<TenantQuotas>,
    /// Coordinator actor reference for shard assignments and peer lookups.
    pub(super) coordinator: Option<ActorRef<ClusterCoordinator>>,
    /// Node identity for response metadata, and the answer to `GetIdentity`.
    pub(super) identity: NodeIdentity,
    /// Default search result limit.
    pub(super) default_search_limit: usize,
    pub(super) max_concurrent_shard_searches: usize,
    /// Shared pool of cached RemoteActorRef handles for avoiding repeated lookups.
    pub(super) remote_peer_pool: Arc<RemotePeerPool>,
    /// What a canvass of the peers needs, so a worker can run one — see
    /// [`ClientOp::FindSchemaInCluster`] in `execute`.
    pub(super) canvass: SchemaCanvass,
}

impl std::fmt::Debug for OrchestratorEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OrchestratorEngine")
            .field("default_search_limit", &self.default_search_limit)
            .finish_non_exhaustive()
    }
}

impl OrchestratorEngine {
    /// Load schema from first shard's storage — [`SchemaCache::schema_for`] against the
    /// engine's snapshot of the shard map.
    pub(super) async fn load_schema(
        &self,
        index: &str,
    ) -> Result<Arc<IndexSchema>, OrchestratorError> {
        self.schema_cache
            .schema_for(&self.shards.load(), index)
            .await
    }

    /// Execute a ClientOp on the shared engine state.
    ///
    /// The engine holds `ArcSwap` snapshots, so it can read a schema but never evolve one —
    /// that needs `&mut NodeOrchestrator` for `staged_schema_validation`. Anything it cannot
    /// serve comes back as [`WorkerOutcome::UseActor`] **carrying the op**, so the caller can
    /// retry it on the actor. Handing the op back rather than signalling with an error is
    /// what keeps the fast path free of a defensive clone: on the path that succeeds, the
    /// document moves into the `WriteRequest`; on the path that defers, it moves into the
    /// reconstructed op. Neither copies.
    ///
    /// The shard-affine hint deliberately does not reach here. It picked the *worker* in
    /// `try_send_affine`, which is what it is for; letting it also pick the *shard* let a write
    /// land somewhere the ring disagreed with. See the routing comment in `engine_write`.
    pub(super) async fn execute(&self, op: ClientOp) -> WorkerOutcome {
        match op {
            ClientOp::Write {
                index,
                id,
                routing_key,
                doc,
                forwarded,
                schema_body,
                tenant,
            } => match self.engine_write(&index, id, routing_key, doc).await {
                Ok(WriteOutcome::Done(value)) => WorkerOutcome::Done(Ok(value)),
                Ok(WriteOutcome::NeedsActor {
                    id,
                    routing_key,
                    doc,
                }) => WorkerOutcome::UseActor(Box::new(ClientOp::Write {
                    index,
                    id,
                    routing_key,
                    doc,
                    // Whichever hop this is, handing the op to the actor is not another one.
                    forwarded,
                    // Nor does it change what the forwarding node had settled.
                    schema_body,
                    // The actor is where the mint happens, so the stamp has to survive the
                    // hand-off: dropping it here is how a tenanted write lands unstamped.
                    tenant,
                })),
                Err(err) => WorkerOutcome::Done(Err(err)),
            },
            ClientOp::Search {
                index,
                query,
                limit,
                offset,
                fields,
                sort,
            } => WorkerOutcome::Done(
                self.engine_search(
                    &index,
                    &query,
                    SearchWindow {
                        offset: offset.unwrap_or(0),
                        limit: limit.unwrap_or(self.default_search_limit),
                    },
                    fields.as_deref(),
                    sort.as_ref(),
                )
                .await,
            ),
            // A stream carries the whole result to the caller as it is produced, so there is no
            // page to ask for and no offset on this op.
            ClientOp::Stream {
                index,
                query,
                limit,
                fields,
                sort,
            } => {
                let search_limit = limit.unwrap_or(self.default_search_limit);
                WorkerOutcome::Done(
                    self.engine_search(
                        &index,
                        &query,
                        SearchWindow::first(search_limit),
                        fields.as_deref(),
                        sort.as_ref(),
                    )
                    .await,
                )
            }
            ClientOp::Delete {
                index,
                id,
                routing_key,
                forwarded,
            } => match self.engine_delete(&index, id, routing_key).await {
                Ok(DeleteOutcome::Done(value)) => WorkerOutcome::Done(Ok(value)),
                Ok(DeleteOutcome::NeedsActor { id, routing_key }) => {
                    WorkerOutcome::UseActor(Box::new(ClientOp::Delete {
                        index,
                        id,
                        routing_key,
                        forwarded,
                    }))
                }
                Err(err) => WorkerOutcome::Done(Err(err)),
            },
            ClientOp::BulkWrite {
                index,
                docs,
                forwarded,
                schema_body,
                tenant,
            } => match self.engine_bulk_write(&index, docs, forwarded).await {
                Ok(BulkOutcome::Done(value)) => WorkerOutcome::Done(Ok(value)),
                Ok(BulkOutcome::NeedsActor { docs }) => {
                    WorkerOutcome::UseActor(Box::new(ClientOp::BulkWrite {
                        index,
                        docs,
                        // Whichever hop this is, handing the op to the actor is not another one —
                        // and the actor re-decides the schema on the terms it arrived with.
                        forwarded,
                        schema_body,
                        // The actor mints, so the stamp travels with the hand-off.
                        tenant,
                    }))
                }
                Err(err) => WorkerOutcome::Done(Err(err)),
            },
            // A delete carries no document, so no delete can ever need a schema written —
            // there is no slow path to defer to, and this always answers.
            ClientOp::BulkDelete {
                index,
                docs,
                forwarded,
            } => WorkerOutcome::Done(self.engine_bulk_delete(&index, docs, forwarded).await),
            // Served here rather than on the actor so that "who is this node" cannot queue
            // behind a bulk write holding the mailbox for its whole duration (ROADMAP CH12).
            // It reads an identity that never changes and a shard count from an ArcSwap, so
            // there is nothing for the actor's `&mut` to protect.
            ClientOp::GetIdentity => {
                WorkerOutcome::Done(Ok(identity_json(&self.identity, self.shards.load().len())))
            }
            // Same reason as GetIdentity — the listing is a metadata read: shard stats,
            // one schema per index, an immutable identity. The shards' own actor state is
            // consulted through `handle_get_stats` on a snapshot clone, so nothing here
            // needs the orchestrator's `&mut`. `ListClusterIndexes` shares the arm because
            // the local half of its broadcast is the same listing (ROADMAP CH12).
            ClientOp::ListIndexes { include_data_size }
            | ClientOp::ListClusterIndexes { include_data_size } => WorkerOutcome::Done(
                list_indexes(
                    &self.shards.load(),
                    &self.schema_cache,
                    &self.identity,
                    include_data_size,
                )
                .await,
            ),
            // A read, and one that waits on peers: the canvass asks each of them through its
            // orchestrator mailbox. On this node's mailbox it waited while holding it, so a peer
            // canvassing this node at the same moment — a first write to a new index, minting —
            // waited on it in turn until both timed out: index deletes answered 503 whenever
            // new indexes were being created elsewhere.
            ClientOp::FindSchemaInCluster { index } => {
                let held = match self.schema_cache.durable(&self.shards.load(), &index).await {
                    Ok(held) => held,
                    Err(err) => return WorkerOutcome::Done(Err(err)),
                };
                WorkerOutcome::Done(find_schema_in_cluster(held, &self.canvass, index).await)
            }
            // A node holds an index's schema only once a document of it has reached one of its
            // shards, or a declaration was sent to it. Answered from this node alone, a load
            // through a node that held none heard "no schema" and declared its own, at version
            // 1, beside the one its peers hold.
            ClientOp::GetConfig { index } => {
                let shards = self.shards.load();
                match self.schema_cache.durable(&shards, &index).await {
                    Ok(Some(held)) if held.state != storage::SchemaState::Dropped => {
                        WorkerOutcome::UseActor(Box::new(ClientOp::GetConfig { index }))
                    }
                    Ok(_) => WorkerOutcome::Done(
                        peer_config_response(&shards, &self.canvass, index).await,
                    ),
                    Err(err) => WorkerOutcome::Done(Err(err)),
                }
            }
            other => WorkerOutcome::UseActor(Box::new(other)),
        }
    }

    /// Fast-path single document write.
    ///
    /// Handles the case where the schema already covers the document: validates it, routes
    /// it to the correct shard and dispatches the write. When the schema has to grow —
    /// a new index, or a document carrying a field the schema does not know — returns
    /// [`WriteOutcome::NeedsActor`] holding the parts of the op back, since evolution needs
    /// `staged_schema_validation` and therefore `&mut NodeOrchestrator`.
    pub(super) async fn engine_write(
        &self,
        index: &str,
        id: String,
        routing_key: Option<String>,
        doc: JsonValue,
    ) -> Result<WriteOutcome, OrchestratorError> {
        let shards = self.shards.load();
        if shards.is_empty() {
            return Err(OrchestratorError::NotReady("No shards".to_string()));
        }

        // Lock-free schema lookup, by the one thing that identifies an index: its name.
        let schema = self.load_schema(index).await?;
        // An index with no schema yet has no owner here; the actor's mint decides one and
        // checks it there.
        self.quotas.check_write(schema.tenant.as_deref(), &shards)?;

        let ring = self.routing_ring.load_full();
        let ctx = WriteCtx {
            shards: &shards,
            ring: &ring,
            schema_cache: &self.schema_cache,
        };

        match ctx.gate(index, &id, &routing_key, &doc, &schema)? {
            // The schema is empty (a new index) or the document carries fields it does not
            // describe. Either way this write has to grow the schema, which only the actor
            // can do — give the caller back everything it needs to retry there.
            WriteGate::Grow => Ok(WriteOutcome::NeedsActor {
                id,
                routing_key,
                doc,
            }),
            WriteGate::Routed {
                target,
                effective_routing_key,
            } => match ctx
                .dispatch(target, index, id, effective_routing_key, doc)
                .await?
            {
                WriteDispatch::Done(response) => Ok(WriteOutcome::Done(response)),
                // The shard is on another node. The engine cannot forward — it holds
                // snapshots, not the peer pool — so hand the op back and let the actor
                // forward it. The effective key travels in place of the caller's hint: it is
                // the value the actor re-derives anyway.
                WriteDispatch::Elsewhere {
                    id,
                    effective_routing_key,
                    doc,
                } => Ok(WriteOutcome::NeedsActor {
                    id,
                    routing_key: effective_routing_key,
                    doc,
                }),
            },
        }
    }

    /// Remove one document by key.
    ///
    /// The only operation with no slow path. A delete carries no document, so it cannot present a
    /// field the schema does not know and can never need `staged_schema_validation` — which is
    /// why this returns the answer rather than a [`WorkerOutcome`] that might hand the op back.
    ///
    /// The schema is still needed, to decide what the id says about routing. It is read from the
    /// engine's snapshot caches and only loaded through the shard as a last resort, exactly as
    /// `engine_write` does.
    pub(super) async fn engine_delete(
        &self,
        index: &str,
        id: String,
        routing_key: Option<String>,
    ) -> Result<DeleteOutcome, OrchestratorError> {
        let shards = self.shards.load();
        if shards.is_empty() {
            return Err(OrchestratorError::NotReady("No shards".to_string()));
        }

        let schema = self.load_schema(index).await?;

        let effective = effective_delete_routing_key(&schema, &id, routing_key.clone())?;

        let ring = self.routing_ring.load_full();
        let ctx = WriteCtx {
            shards: &shards,
            ring: &ring,
            schema_cache: &self.schema_cache,
        };

        // The ring owns the placement decision here as it does for a write, and a delete cannot
        // disagree with the hint the router dispatched on: both are this same key.
        let target = ctx.route_write(&Some(effective))?;

        match ctx.dispatch_delete(target, index, &id).await? {
            Some(response) => Ok(DeleteOutcome::Done(response)),
            // The shard is on another node. The engine cannot forward — it holds snapshots, not
            // the peer pool — so hand the op back and let the actor forward it.
            None => Ok(DeleteOutcome::NeedsActor { id, routing_key }),
        }
    }

    /// Fast-path bulk write: validate against the schema as it stands, then fan out.
    ///
    /// What the engine cannot do is *decide* a schema — `staged_schema_validation` evolves or
    /// creates one under the actor's serial mailbox, and two bulks growing one index at once
    /// is exactly what that serialisation is for. A batch that needs it — an unsettled index,
    /// or a document carrying a field the schema does not know — goes back whole on
    /// [`BulkOutcome::NeedsActor`]. Validation is still run first rather than scanned for, so
    /// the deferral carries the same cost a single write's does: the actor re-validates either
    /// way.
    ///
    /// Everything past validation — routing, grouping, the one-hop forwarding bound, per-item
    /// accounting — is [`BulkCtx::apply_bulk_write`], the body the actor runs on the same
    /// terms, so the mailbox path and this one cannot drift.
    pub(super) async fn engine_bulk_write(
        &self,
        index: &str,
        docs: Vec<DocPayload>,
        forwarded: bool,
    ) -> Result<BulkOutcome, OrchestratorError> {
        let start = std::time::Instant::now();
        let shards = self.shards.load_full();
        if shards.is_empty() {
            return Err(OrchestratorError::NotReady("No shards".to_string()));
        }

        let schema = self.load_schema(index).await?;
        self.quotas.check_write(schema.tenant.as_deref(), &shards)?;
        // An empty or dropped schema is an index whose shape is still being settled — the
        // actor's staged validation samples and canvasses to decide it, which is a schema
        // write and therefore not this lane's.
        if schema.fields.is_empty() || schema.state == storage::SchemaState::Dropped {
            return Ok(BulkOutcome::NeedsActor { docs });
        }

        let (results, docs) = NodeOrchestrator::parallel_validate_schema(docs, &schema).await?;
        if results.iter().any(|result| result.needs_evolution) {
            return Ok(BulkOutcome::NeedsActor { docs });
        }

        // The same refusal assembly the actor's head builds: validation failures are reasons
        // against their positions, the rest carry their position on into routing.
        let mut rejections: Vec<String> = Vec::new();
        let mut refused = HashSet::new();
        for (position, result) in results.iter().enumerate() {
            if let Some(err) = &result.validation_error {
                rejections.push(format!("document {position}: {err}"));
                refused.insert(position);
            }
        }
        if !rejections.is_empty() {
            tracing::warn!(
                index = %index,
                error_count = refused.len(),
                total_docs = docs.len(),
                "Some documents failed schema validation and were not written"
            );
        }

        let pending: Vec<Placed> = docs
            .into_iter()
            .enumerate()
            .filter(|(position, _)| !refused.contains(position))
            .map(|(position, doc)| Placed {
                position,
                doc,
                routing_key: None,
            })
            .collect();

        let ring = self.routing_ring.load_full();
        let ctx = BulkCtx {
            shards: &shards,
            ring: &ring,
            coordinator: self.coordinator.as_ref(),
            remote_peer_pool: Some(&self.remote_peer_pool),
        };
        ctx.apply_bulk_write(index, pending, rejections, &schema, forwarded, start)
            .await
            .map(BulkOutcome::Done)
    }

    /// Bulk delete. Nothing on this path can change a schema — a delete carries no document
    /// that could present a field the schema does not know — so unlike the bulk write there
    /// is no slow path behind it at all: the same shape `engine_delete` already has.
    pub(super) async fn engine_bulk_delete(
        &self,
        index: &str,
        docs: Vec<DeletePayload>,
        forwarded: bool,
    ) -> Result<JsonValue, OrchestratorError> {
        let start = std::time::Instant::now();
        let shards = self.shards.load_full();
        if shards.is_empty() {
            return Err(OrchestratorError::NotReady("No shards".to_string()));
        }

        let schema = self.load_schema(index).await?;
        let ring = self.routing_ring.load_full();
        let ctx = BulkCtx {
            shards: &shards,
            ring: &ring,
            coordinator: self.coordinator.as_ref(),
            remote_peer_pool: Some(&self.remote_peer_pool),
        };
        ctx.apply_bulk_delete(index, docs, &schema, forwarded, start)
            .await
    }

    /// Parallel scatter-gather search across all local shards.
    pub(super) async fn engine_search(
        &self,
        index: &str,
        query: &str,
        window: SearchWindow,
        fields: Option<&[String]>,
        sort: Option<&SortSpec>,
    ) -> Result<JsonValue, OrchestratorError> {
        let shards = self.shards.load();
        if shards.is_empty() {
            return Ok(
                serde_json::json!({"hits": [], "hits_returned": 0, "total_hits": 0, "took_ms": 0}),
            );
        }

        // Get the schema for shadow field transformation. Read through to the store on a miss
        // rather than treating "not cached yet" as "no schema": with an empty schema the
        // projection rewrite in the gather is a no-op, so the first search after a boot dropped
        // any field a shadow name refers to. One disk read per index per process.
        let schema = self.load_schema(index).await?;

        ScatterCtx {
            shards: shards.as_ref(),
            schema: &schema,
            max_concurrent_shard_searches: self.max_concurrent_shard_searches,
        }
        .gather(index, query, window, fields, sort)
        .await
    }
}

#[derive(Debug, Actor, RemoteActor)]
pub(crate) struct NodeOrchestrator {
    /// The mailbox lane's service estimate, shared with the [`RouterActor`] that gates it.
    ///
    /// Written here rather than at the caller because this actor handles one op at a time, so
    /// the time around `handle` is service with no queue in it. See [`MailboxLane`].
    pub(super) mailbox_lane: MailboxLane,
    /// Indexes this node is minting a schema for right now, and how many writes are doing it.
    ///
    /// Set when a first write's canvass leaves the mailbox, cleared when its
    /// `MintAfterCanvass` has run. While an index is here and no schema for it is saved yet,
    /// a peer asking for it is told so rather than told there is none — see `GetRawSchema` in
    /// `handle_client_op`.
    pub(super) minting: HashMap<String, usize>,
    /// Permits for peers' ops served off the mailbox — see `Message<ClientOp>`. Sized like the
    /// worker pool's in-flight capacity, and set with it; `None` until the pool exists.
    pub(super) peer_lane: Option<Arc<tokio::sync::Semaphore>>,
    /// Peers that asked about an index while this node was minting it, and were minting it too.
    ///
    /// Read when this node decides whether it mints or yields (`MintAfterCanvass`), and cleared
    /// with `minting` once no write here is minting the index any more.
    pub(super) mint_rivals: HashMap<String, BTreeSet<Uuid>>,
    /// The schema change each index is reserved for here, from its `PrepareSchema` until its
    /// `ApplySchema` or `ReleaseSchemaChange` — or [`SCHEMA_CHANGE_RESERVATION`], should the
    /// node coordinating it stop before either.
    pub(super) schema_changes: HashMap<String, (Uuid, Instant)>,
    /// Rounds agreeing across the cluster on fields this node learned from writes — see
    /// [`crate::cluster_coordinator::SchemaReconciler`].
    pub(super) schema_reconciler: Arc<crate::cluster_coordinator::SchemaReconciler>,
    /// Map of shard UUIDs to their microshard actors.
    ///
    /// `pub(crate)` rather than `pub(super)`, and the only field of this actor that is: the
    /// admin-memory messages are implemented for `NodeOrchestrator` in `crate::admin::memory`,
    /// outside `node/`, and walk the shard map to size and evict writers. A named widening of
    /// one field, not of the actor.
    pub(crate) shards: HashMap<Uuid, MicroshardActor>,
    /// Node-wide writer-thread liveness, shared with every shard's writer thread and read by
    /// the health endpoint so a dead writer stops the node reporting green.
    pub(super) writer_liveness: Arc<WriterLiveness>,
    /// This node's identity (UUID, name, virtual tokens)
    pub(super) identity: NodeIdentity,
    /// Node configuration  
    pub(super) config: NodeConfig,
    /// Consistent hash ring for routing writes based on routing keys
    pub(super) routing_ring: ConsistentRing,
    /// Optional coordinator reference for shard registration
    pub(super) coordinator: Option<ActorRef<ClusterCoordinator>>,
    /// Shared routing ring snapshot (lock-free via ArcSwap).
    /// Wrapped in Arc so it can be shared with the OrchestratorEngine worker pool
    /// and the RouterActor for shard-affine dispatch.
    pub(super) shared_routing_ring: Arc<ArcSwap<ConsistentRing>>,
    /// Cores this process may use, resolved once. Sizes the worker pool and places both
    /// workers and writer threads, so all of them count the same cores.
    pub(super) core_layout: CoreLayout,
    /// Shard ordinals, published lock-free. Read by the dispatcher to pick a shard's worker
    /// and by the router to answer "is this shard mine?" without a coordinator round trip.
    pub(super) placement: Arc<ArcSwap<ShardPlacement>>,
    /// Per-index schema cache to avoid repeated metadata reads (lock-free via ArcSwap).
    /// Wrapped in Arc so it can be shared with the OrchestratorEngine worker pool.
    pub(super) schema_cache: Arc<SchemaCache>,
    /// Tenant ceilings and the usage reading they are checked against, shared with the engine
    /// so a reading taken on either write lane serves both.
    pub(super) quotas: Arc<TenantQuotas>,
    /// Default search result limit when not specified in request
    pub(super) default_search_limit: usize,
    pub(super) max_concurrent_shard_searches: usize,
    /// Shared engine state for the worker pool (Arc-wrapped, lock-free).
    /// Workers operate on this concurrently without going through the actor mailbox.
    pub(super) engine: Option<Arc<OrchestratorEngine>>,
    /// Channel sender for dispatching jobs to the worker pool.
    /// Workers pull jobs from the receiver and execute on the shared engine.
    pub(super) worker_tx: Option<OrchestratorWorkerTx>,
    /// Number of worker tasks spawned in the pool.
    /// Used to signal explicit worker shutdown.
    pub(super) worker_count: usize,
    /// Handles for pinned worker OS threads (Stage 2e). Empty when running in the
    /// default unpinned tokio-task mode. Joined during shutdown to ensure clean
    /// teardown of per-worker `current_thread` runtimes.
    pub(super) worker_threads: Vec<std::thread::JoinHandle<()>>,
    /// Dedicated tokio runtime for read operations (search, stats).
    /// Isolates read I/O from the writer threads and tokio's generic blocking pool.
    /// Arc-wrapped so the runtime outlives shard clones that hold its Handle.
    pub(super) read_runtime: Option<Arc<tokio::runtime::Runtime>>,
    /// Node-wide read-pool health, its capacity the read runtime's blocking width. Handed to every
    /// shard so each read brackets it, and to the health endpoint so it can see the pool saturate
    /// or wedge.
    pub(super) read_pool_health: Arc<ReadPoolHealth>,
    /// How long a read may wait for a pool thread before it is refused instead of run. The
    /// node's request timeout, handed to every shard. `None` when no timeout is configured.
    pub(super) read_budget: Option<Duration>,
    /// Shared pool of cached RemoteActorRef handles for avoiding repeated lookups.
    pub(super) remote_peer_pool: Option<Arc<RemotePeerPool>>,
}

impl NodeOrchestrator {
    /// The mailbox lane this orchestrator folds its service times into.
    ///
    /// Taken before the actor is spawned and handed to [`RouterActor::with_config`], so the end
    /// that predicts and the end that measures share one instance.
    pub(crate) fn mailbox_lane(&self) -> MailboxLane {
        self.mailbox_lane.clone()
    }

    pub(super) fn storage_path_candidates(&self) -> Cow<'_, [PathBuf]> {
        if self.config.storage_paths.is_empty() {
            Cow::Owned(vec![self.config.storage_path.clone()])
        } else {
            Cow::Borrowed(&self.config.storage_paths)
        }
    }

    pub(super) fn deterministic_shard_directory(&self, shard_id: Uuid) -> PathBuf {
        let paths_cow = self.storage_path_candidates();

        // Ensure paths are sorted for deterministic distribution
        let mut sorted_paths: Vec<PathBuf> = paths_cow.as_ref().to_vec();
        sorted_paths.sort();

        // Use the UUID bytes directly for stable distribution
        // Convert first 8 bytes of UUID to u64 for modulo operation
        let uuid_bytes = shard_id.as_bytes();
        let hash_value = u64::from_be_bytes(
            uuid_bytes[..8]
                .try_into()
                .expect("UUID has at least 8 bytes"),
        );

        // Round-robin distribution based on UUID hash
        let path_index = (hash_value as usize) % sorted_paths.len();
        let base = &sorted_paths[path_index];

        base.join(format!("shard-{}", shard_id))
    }

    /// Generates a balanced shard ID using UUID mining for uniform distribution.
    ///
    /// This method "mines" a UUID that will map to the least-loaded data directory,
    /// ensuring uniform distribution across all available storage paths while
    /// maintaining deterministic placement (same UUID always maps to same path).
    ///
    /// Algorithm:
    /// 1. Calculate current distribution by hashing existing shard UUIDs
    /// 2. Identify the directory with the minimum shard count
    /// 3. Generate random UUIDs until one hashes to the target directory
    ///
    /// Performance: Average iterations = number of directories (e.g., 6 attempts for 6 dirs)
    pub(crate) fn generate_balanced_shard_id(&self) -> Uuid {
        let paths_cow = self.storage_path_candidates();
        let mut sorted_paths: Vec<PathBuf> = paths_cow.as_ref().to_vec();
        sorted_paths.sort();

        let dir_count = sorted_paths.len();
        if dir_count == 0 {
            return Uuid::new_v4();
        }

        // Calculate current distribution across directories
        let mut distribution = vec![0usize; dir_count];
        for existing_id in self.shards.keys() {
            let bytes = existing_id.as_bytes();
            let hash =
                u64::from_be_bytes(bytes[..8].try_into().expect("UUID has at least 8 bytes"));
            let idx = (hash as usize) % dir_count;
            distribution[idx] += 1;
        }

        // Find target directory (least loaded, bias towards lower indices on ties)
        let target_idx = distribution
            .iter()
            .enumerate()
            .min_by_key(|(_idx, count)| *count)
            .map(|(idx, _)| idx)
            .unwrap_or(0);

        info!(
            "Balancing shards: targeting dir index {} (current distribution: {:?})",
            target_idx, distribution
        );

        // Mine a UUID that hashes to the target directory
        loop {
            let candidate = Uuid::new_v4();
            let bytes = candidate.as_bytes();
            let hash =
                u64::from_be_bytes(bytes[..8].try_into().expect("UUID has at least 8 bytes"));

            if (hash as usize) % dir_count == target_idx {
                return candidate;
            }
        }
    }

    /// What the cluster already knows about an index this node has no schema for — see
    /// [`SchemaCanvass::peer_schema_for`].
    pub(super) async fn peer_schema_for(&self, index: &str) -> PeerSchemaLookup {
        self.schema_canvass()
            .peer_schema_for(index, Some(self.identity.uuid))
            .await
    }

    /// What a canvass of the peers needs from this actor, detached from it, so the canvass can
    /// run somewhere that does not hold the mailbox.
    pub(super) fn schema_canvass(&self) -> SchemaCanvass {
        SchemaCanvass {
            clustered: self.config.clustered,
            coordinator: self.coordinator.clone(),
            pool: self.remote_peer_pool.clone(),
        }
    }

    /// Which of two schemas for the same index the cluster should settle on.
    ///
    /// Newer version wins; a tie goes to the lower thumbprint. The tie-break needs no
    /// communication and is a pure function of the two candidates, so every node reaches the
    /// same verdict without anyone deciding it — which is what lets schemas converge with no
    /// leader and no consensus round.
    pub(crate) fn preferred_schema(a: IndexSchema, b: IndexSchema) -> IndexSchema {
        match a.version.cmp(&b.version) {
            std::cmp::Ordering::Greater => a,
            std::cmp::Ordering::Less => b,
            std::cmp::Ordering::Equal => {
                if b.calculate_fingerprint() < a.calculate_fingerprint() {
                    b
                } else {
                    a
                }
            }
        }
    }

    /// Validates schema for documents in parallel, then evolves schema sequentially.
    ///
    /// This method uses a two-stage approach:
    /// Stage 1: Parallel validation (read-only, CPU-bound)
    /// Stage 2: Sequential schema evolution (write operations only when needed)
    ///
    /// `forwarded` says this call is serving a share of a decision another node already made, so
    /// it must neither sample nor canvass — see [`ClientOp::BulkWrite::forwarded`]. `schema_body`
    /// is that decision, and arrives only on a resend after this node asked for it.
    /// Validate a batch against the index's schema, growing the schema first where the batch
    /// needs it. The batch travels in and back out again — see `parallel_validate_schema` for
    /// why owning it is what lets the fan-out avoid copying it.
    // The terms the write arrived on, as `orch_write` explains for its own list.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn staged_schema_validation(
        &self,
        index: &str,
        docs: Vec<DocPayload>,
        schema_cache: &mut Arc<IndexSchema>,
        forwarded: bool,
        schema_body: Option<&IndexSchema>,
        // Stamped onto the schema only if this call is the index's mint; see
        // `ClientOp::Write::tenant`.
        tenant: Option<&str>,
        // The canvass of the peers for this index, already run outside the mailbox — see
        // `MintAfterCanvass`. `None` canvasses here, which only a standalone node (whose canvass
        // asks nobody) or a caller that found a schema a moment ago should reach.
        settled: Option<PeerSchemaLookup>,
    ) -> Result<(SchemaValidationSummary, Vec<DocPayload>), OrchestratorError> {
        if docs.is_empty() {
            return Ok((
                SchemaValidationSummary {
                    total_docs: 0,
                    valid_docs: 0,
                    evolution_needed: false,
                    all_new_fields: std::collections::HashSet::new(),
                    errors: Vec::new(),
                },
                docs,
            ));
        }

        // Enhanced sampling for initial schema creation.
        //
        // Sampling types an index from its first documents, which is the feature that makes
        // semi-structured input work — and, in a cluster, the mechanism by which three nodes came
        // to hold three different schemas for one index. Whichever node received `PUT /_config`
        // kept the declaration; every other node typed the same index from the first document
        // that reached it, and a tantivy column is built once, so the types diverged for good.
        //
        // So ask before inventing. `is_initial_creation` is what licenses sampling and it now
        // means "no schema for this index exists anywhere I can see", not merely "none here".
        // A record of a deletion counts as no schema: its fields are gone, so there is nothing
        // to evolve and this write is the one creating the index.
        // The highest dropped record a canvass found on a peer, which a mint goes above.
        let mut peer_dropped_at = 0u64;
        let mut is_initial_creation =
            schema_cache.fields.is_empty() || schema_cache.state == storage::SchemaState::Dropped;

        if is_initial_creation
            && let Some(body) = schema_body
            && !body.fields.is_empty()
        {
            // The forwarding node settled this already, so there is nothing to ask and nothing
            // to invent. Adopted exactly as a peer's answer is below, for the same reason: this
            // node is applying an agreed schema rather than making a change, so the version
            // travels with it and `mark_modified` must not advance it.
            *schema_cache = Arc::new(body.clone());
            Self::persist_schema_to_stores(index, schema_cache, &self.shards).await?;
            tracing::info!(
                index = %index,
                version = schema_cache.version,
                fields = schema_cache.fields.len(),
                "Adopted the schema carried by the forwarded write"
            );
            is_initial_creation = false;
        } else if is_initial_creation && forwarded {
            // Someone else's decision, and this node has nothing to run it against. Ask the
            // forwarding node rather than canvassing peers: this runs inside the orchestrator's
            // message handler, and those peers are using that same mailbox to run the writes
            // this fan-out just sent them. Answering is the only move that cannot wait on
            // something waiting on us.
            //
            // **This node must not sample either.** A share is a subset of the batch, and
            // `infer_type_from_value` reads a subset holding no negative values differently from
            // the whole — so two nodes sampling their own shares can build two different
            // tantivy indexes from one request, with no bug anywhere. One decision per index,
            // made where the batch was whole.
            return Err(OrchestratorError::SchemaBodyRequired {
                index: index.to_string(),
            });
        } else if is_initial_creation {
            let lookup = match settled {
                Some(lookup) => lookup,
                None => self.peer_schema_for(index).await,
            };
            match lookup {
                // Nobody holds one, so this index really is new: the batch types it, minted above
                // any dropped record a peer still keeps.
                PeerSchemaLookup::NoneHeld { dropped_at } => peer_dropped_at = dropped_at,
                PeerSchemaLookup::Found(declared) => {
                    // Adopted verbatim, version included: this node is applying a declaration
                    // that already exists, not making a change, so `mark_modified` must not run
                    // and advance a version the rest of the cluster has agreed on.
                    *schema_cache = Arc::new(*declared);
                    Self::persist_schema_to_stores(index, schema_cache, &self.shards).await?;
                    tracing::info!(
                        index = %index,
                        version = schema_cache.version,
                        fields = schema_cache.fields.len(),
                        "Adopted a peer's schema instead of sampling one from the documents"
                    );
                    // No longer a creation: the schema came from the cluster, so a field these
                    // documents add is an addition to an existing schema and takes that path —
                    // recorded non-indexed, pending a rebuild — rather than being marked
                    // searchable as a first write's fields are.
                    is_initial_creation = false;
                }
                // `MintAfterCanvass` settles a race before it gets here and hands on `NoneHeld` to
                // the winner, so this is a canvass that ran in place — no way to wait for the
                // winner from inside the mailbox, so refuse and let the retry adopt its schema.
                PeerSchemaLookup::Contested { rivals } => {
                    return Err(OrchestratorError::SchemaUnconfirmed {
                        index: index.to_string(),
                        reason: format!("node(s) {rivals:?} are creating this index right now"),
                    });
                }
                PeerSchemaLookup::Unreachable { reason } => {
                    tracing::warn!(
                        index = %index,
                        reason = %reason,
                        "Refusing to sample a schema: the cluster cannot be asked whether one exists"
                    );
                    return Err(OrchestratorError::SchemaUnconfirmed {
                        index: index.to_string(),
                        reason,
                    });
                }
            }
        }

        if is_initial_creation {
            // Reaching here means no schema exists anywhere this node can see — a peer's
            // declaration or a carried body would have settled `is_initial_creation` above —
            // so this write is the index's mint: the sampled schema creates a tantivy index
            // that is built once. Where minting is an explicit decision rather than a side
            // effect of a write, refuse: the caller's remedy is `PUT /api/{index}/_config`,
            // which is the `IndexAdmin` capability this setting exists to hand the decision
            // back to.
            if !self.config.implicit_index_creation {
                return Err(OrchestratorError::Validation(format!(
                    "index '{index}' does not exist and this node does not create indexes \
                     implicitly; create it with PUT /api/{index}/_config before writing"
                )));
            }
            // A mint is the one moment a tenant's index count grows, and this mailbox is the
            // only place one happens, so a count taken here cannot race another.
            if let Some(tenant) = tenant
                && self.quotas.max_indexes(tenant).is_some()
            {
                let owned = owned_index_count(&self.shards, tenant).await?;
                self.quotas.check_mint(tenant, owned)?;
            }
            // Whatever this settles on describes a live index. Over a dropped index's record
            // this is a new index, not the old one resumed: it takes a version above the
            // record's — so neither a write still carrying the dropped schema nor the record
            // itself can be installed over it — and it starts its own clock.
            let minted = Arc::make_mut(schema_cache);
            let local_dropped_at = if minted.state == storage::SchemaState::Dropped {
                minted.version
            } else {
                0
            };
            let dropped_at = local_dropped_at.max(peer_dropped_at);
            if dropped_at > 0 {
                minted.version = dropped_at.saturating_add(1);
                let now = chrono::Utc::now().timestamp();
                minted.created_at = now;
                minted.updated_at = now;
            }
            minted.state = storage::SchemaState::Active;
            // The key, as a declaration records it, so a minted schema and a declared one
            // describe `id` alike and a write carrying it finds nothing new to learn.
            minted
                .fields
                .entry("id".to_string())
                .or_insert_with(|| FieldDef::new("id".to_string(), TantivyFieldType::Text));

            // The index's mint is the one moment ownership is decided, so it is the one place
            // the stamp is written. Not refreshed on later writes: whoever created the index
            // owns it, and a stamp that followed the last writer would let a tenant shed their
            // own usage by having another key write once.
            if let Some(tenant) = tenant {
                minted.tenant = Some(tenant.to_string());
            }
            // Its fields come from the whole batch below: validation reports every field of
            // every document, and evolution types each by the join of all its values and
            // persists them indexed before any write reaches a shard — the tantivy schema is
            // fixed when the first write creates the index.
        }

        // Stage 1: Parallel validation (read-only). The batch goes in by value and comes back
        // beside the verdicts; nothing downstream reads it in between.
        let total_docs = docs.len();
        let (validation_results, docs) = Self::parallel_validate_schema(docs, schema_cache).await?;

        // Stage 2: Aggregate results and identify evolution needs
        let mut summary = SchemaValidationSummary {
            total_docs,
            valid_docs: 0,
            evolution_needed: false,
            all_new_fields: std::collections::HashSet::new(),
            errors: Vec::new(),
        };

        // `parallel_validate_schema` answers in the order it was asked, so the position of a
        // result is the position of the document it judged.
        for (position, result) in validation_results.into_iter().enumerate() {
            if let Some(err) = result.validation_error {
                summary.errors.push((position, err));
            } else {
                summary.valid_docs += 1;
                if result.needs_evolution {
                    summary.evolution_needed = true;
                    for new_field in result.new_fields {
                        summary.all_new_fields.insert(new_field);
                    }
                }
            }
        }

        // Stage 3: Sequential schema evolution (only if needed).
        //
        // `is_initial_creation` is passed down rather than recomputed there: sampling above
        // has already put fields into `schema_cache`, so `fields.is_empty()` no longer
        // answers the question. Without this, a field that first appears past the sampling
        // limit would be treated as a later addition and left non-indexed — two classes of
        // field out of one load.
        if summary.evolution_needed && !summary.all_new_fields.is_empty() {
            self.evolve_schema_sequential(
                index,
                Arc::make_mut(schema_cache),
                &summary.all_new_fields,
                &self.shards,
                is_initial_creation,
            )
            .await?;
        } else if is_initial_creation {
            // A mint whose documents carry nothing but their key: the index exists all the same.
            Self::persist_schema_to_stores(index, schema_cache, &self.shards).await?;
        }

        tracing::debug!(
            total_docs = summary.total_docs,
            valid_docs = summary.valid_docs,
            evolution_needed = summary.evolution_needed,
            new_fields_count = summary.all_new_fields.len(),
            errors_count = summary.errors.len(),
            "Staged schema validation completed"
        );

        Ok((summary, docs))
    }

    /// Validate a batch, inline or fanned out, according to how big it is.
    ///
    /// Size decides *where* the work runs and nothing else: every document is judged by
    /// `validate_document`, so a batch of one and a batch of ten thousand reach the same
    /// verdict about the same document. That was not true while a second validator existed for
    /// small batches — see `validate_document` for what the two disagreed about.
    ///
    /// The batch is taken by value and handed back with the verdicts. `spawn_blocking` needs
    /// `'static`, and the only way to give it that while borrowing was to deep-clone every
    /// document — a second copy of the whole request body, allocated and dropped per bulk
    /// write, to run read-only checks over it. Moving it in and out costs a `Vec` of pointers
    /// either way and copies nothing.
    pub(super) async fn parallel_validate_schema(
        docs: Vec<DocPayload>,
        schema: &Arc<IndexSchema>,
    ) -> Result<(Vec<SchemaValidationResult>, Vec<DocPayload>), OrchestratorError> {
        tracing::debug!(
            "Using parallel Rayon validation for {} documents",
            docs.len()
        );

        // Small batches validate inline. Offloading them costs two thread hops (onto this
        // worker's blocking pool, then a rayon fan-out onto the global rayon pool, which is
        // unpinned and competes with the writer threads) — all to run a handful of cheap
        // per-document checks. Both hops are pure overhead below this size.
        pub(super) const INLINE_VALIDATION_MAX_DOCS: usize = 64;
        if docs.len() <= INLINE_VALIDATION_MAX_DOCS {
            let results = docs
                .iter()
                .map(|doc_payload| validate_document(&doc_payload.id, &doc_payload.doc, schema))
                .collect();
            return Ok((results, docs));
        }

        // `spawn_blocking` needs a `'static` handle, which used to mean deep-cloning the whole
        // field map out of the caller's `Arc` on every batch above the inline threshold. Both
        // callers hold that `Arc` already — the bulk fast lane straight out of `load_schema` —
        // so a refcount bump is the whole cost now. The comment this replaces argued the clone
        // was bounded by field count and so not worth removing; that is true and beside the
        // point, since it was never necessary in the first place.
        let schema = Arc::clone(schema);

        let (results, docs) = tokio::task::spawn_blocking(move || {
            let results = docs
                .par_iter()
                .map(|doc_payload| validate_document(&doc_payload.id, &doc_payload.doc, &schema))
                .collect::<Vec<SchemaValidationResult>>();
            (results, docs)
        })
        .await
        .map_err(|e| OrchestratorError::Io(std::io::Error::other(e)))?;

        Ok((results, docs))
    }

    /// Whether one document can be written, and what the schema would have to learn first.
    ///
    /// The only validator, and it was two: a "fast" one for single writes, small batches and
    /// anything under a thousand documents, and this one past that. They disagreed about the
    /// same document twice over. The fast one required a value's type to equal the declared
    /// type exactly, so a number under a text field — a shape the engine stores and indexes
    /// without complaint — was refused below a thousand documents and written above it; batch
    /// size decided whether a document was valid. It also never reported a field the schema
    /// does not have, which left `needs_evolution` permanently false on the path whose caller
    /// reads it to decide whether the schema has to grow, so a single write carrying a new
    /// field never grew it while a large batch did.
    ///
    /// Add the fields validation discovered to the schema.
    ///
    /// `is_initial_creation` decides whether they are searchable. A tantivy schema is fixed when
    /// the index is created, so the batch that mints an index is the one chance to give its
    /// fields columns: they are added indexed (stored only for `id`; hits are rebuilt from
    /// redb). A field first seen once the index exists has no column to write into, so it is
    /// added learned and non-indexed, to keep the redb and tantivy views of a document
    /// consistent until a schema edit promotes it. Either way each field takes the join of every
    /// value's type ([`TantivyFieldType::widened`]), so no value in the batch is refused for one
    /// that came before it.
    pub(super) async fn evolve_schema_sequential(
        &self,
        index: &str,
        schema_cache: &mut IndexSchema,
        new_fields: &std::collections::HashSet<(String, TantivyFieldType)>,
        shards: &HashMap<Uuid, MicroshardActor>,
        is_initial_creation: bool,
    ) -> Result<(), OrchestratorError> {
        // One type per name: every value's type joined, so the batch's order does not decide it.
        let mut wanted: BTreeMap<&str, TantivyFieldType> = BTreeMap::new();
        for (name, field_type) in new_fields {
            wanted
                .entry(name.as_str())
                .and_modify(|joined| *joined = joined.widened(field_type))
                .or_insert_with(|| field_type.clone());
        }

        let mut added = 0usize;
        let mut widened = 0usize;
        for (name, field_type) in wanted {
            match schema_cache.fields.get_mut(name) {
                None => {
                    // `FieldDef::new` already applies the storage rule — only `id` is stored in
                    // tantivy, everything else is reconstructed from redb.
                    let field = if is_initial_creation {
                        FieldDef::new(name.to_string(), field_type)
                    } else {
                        FieldDef::new_learned(name.to_string(), field_type)
                    };
                    schema_cache.fields.insert(name.to_string(), field);
                    added += 1;
                }
                // Validation asks for a wider type only for a learned field without a column.
                Some(field) if field.learned && !field.indexed => {
                    let joined = field.field_type.widened(&field_type);
                    if joined != field.field_type {
                        field.retype_learned(joined);
                        widened += 1;
                    }
                }
                Some(_) => {}
            }
        }

        if added == 0 && widened == 0 {
            return Ok(());
        }

        Self::persist_schema_to_stores(index, schema_cache, shards).await?;
        tracing::info!(
            index = %index,
            added,
            widened,
            is_initial_creation,
            total_fields = schema_cache.fields.len(),
            "Schema evolved from written documents"
        );
        // What this node learned alone, the cluster is asked to agree on: the same field may
        // have reached another node first with other values.
        if !is_initial_creation {
            self.request_schema_reconcile(index);
        }
        Ok(())
    }

    /// Write a schema to every local shard's store, and its cache with it.
    ///
    /// The storage layer derives its own schema from documents as they are written, using
    /// non-indexed fields — correct for a field arriving at a live index, wrong for the
    /// first write, which is what creates the tantivy index. Persisting here first means
    /// storage finds the fields already described and evolves types against them instead of
    /// inventing its own definitions.
    pub(super) async fn persist_schema_to_stores(
        index: &str,
        schema: &IndexSchema,
        shards: &HashMap<Uuid, MicroshardActor>,
    ) -> Result<(), OrchestratorError> {
        let stores: Vec<Arc<HybridStore>> = shards
            .values()
            .filter_map(|shard| shard.store.as_ref().map(Arc::clone))
            .collect();

        if stores.is_empty() {
            return Err(OrchestratorError::Missing(
                "No local stores available to persist schema".to_string(),
            ));
        }

        let index_name = index.to_string();
        let handles: Vec<_> = stores
            .into_iter()
            .map(|store| {
                let idx = index_name.clone();
                let sch = schema.clone();
                tokio::task::spawn_blocking(move || store.store_schema_and_cache(&idx, &sch))
            })
            .collect();

        for handle in handles {
            handle
                .await
                .map_err(|e| {
                    OrchestratorError::Io(std::io::Error::other(format!(
                        "Failed to spawn schema update task: {}",
                        e
                    )))
                })?
                .map_err(|e| {
                    OrchestratorError::Io(std::io::Error::other(format!(
                        "Failed to store schema: {}",
                        e
                    )))
                })?;
        }

        Ok(())
    }

    /// Produce sorted field names with "id" first (if present), others alphabetical.
    ///
    /// `_seq` is excluded for the same reason `describe_fields` excludes it: it is the engine's
    /// internal WAL sequence, not something a caller declared or can use. It has to be filtered
    /// *here* as well because this is the one listing that does not go through `describe_fields`
    /// — and because the schema it is handed has just been through
    /// `normalize_after_deserialization`, which inserts `_seq`. Without this, creating an index
    /// answers with a field that every other endpoint hides.
    pub(super) fn sorted_field_names(schema: &IndexSchema) -> Vec<String> {
        let mut names: Vec<String> = schema
            .fields
            .keys()
            .filter(|name| name.as_str() != "_seq")
            .cloned()
            .collect();
        names.sort_by(|a, b| match (a.as_str(), b.as_str()) {
            ("id", "id") => std::cmp::Ordering::Equal,
            ("id", _) => std::cmp::Ordering::Less,
            (_, "id") => std::cmp::Ordering::Greater,
            _ => a.cmp(b),
        });
        names
    }

    /// Describe every field of an index, in the one shape every caller renders.
    ///
    /// This is the whole point of the consolidation: the client, the MCP tools and the HTTP
    /// listing each used to compose their own answer to "what is in this index" out of a
    /// statistics call and a schema call, and each composed it differently — `field` against
    /// `name`, `type` against `field_type`, `shadow` against `is_shadow` — so the same index had
    /// as many descriptions as it had readers.
    ///
    /// Identity is always `name`, here and for the index itself. Ordering puts `id` first and the
    /// rest alphabetically, so a description read twice reads the same way.
    ///
    /// `searchable` is the field no caller could compute. `indexed` is a *declaration*, and the
    /// Tantivy index is built from that declaration — so a field declared after the index was
    /// built is `indexed` and yet matches nothing until the data is rebuilt. Only the engine can
    /// see the difference, and an agent that cannot see it writes a query that silently returns
    /// nothing. A shadow field is searchable regardless: it names the identifier, which is
    /// answered from redb without the search index at all.
    ///
    /// `sortable` is the same distinction one property along, and it exists for the same reason.
    /// `fast` is a *declaration*; the column a sort orders on is written at index time from that
    /// declaration, so a field can be `fast: true` with no column behind it — after which a
    /// numeric sort on it errors and a text sort on it silently returns the alphabetical order of
    /// a sample. Only the engine can see which, so a caller choosing a field to sort on reads
    /// `sortable`, not `fast`.
    ///
    /// `returned_as` appears on `id` alone, and only on an index with a shadow field, naming
    /// what the hits carry in its place. The key is the only field whose two names can differ,
    /// and on such an index they always do — so without it the description lists two searchable
    /// text fields with nothing relating them, and an agent that picks `id` gets hits carrying
    /// no `id` and no account of why.
    ///
    /// Omitting `id` there is the other way to remove the ambiguity and it is worse: `id:VALUE`
    /// still answers on such an index, so a description without `id` would make `validate_query`
    /// report the working form as an unknown field.
    ///
    /// `_seq` is omitted everywhere. It is WAL bookkeeping, and offering it as a queryable field
    /// invites a query that cannot mean anything. Filtering it in one place also settles an
    /// inconsistency where one response reported two different field counts.
    /// The fields an unqualified term searches on this index, and whether the cap cut them short.
    ///
    /// The same selection the query path makes ([`storage::select_default_fields`]), fed from the
    /// schema and the built index's searchable set rather than from a tantivy schema, so what a
    /// caller is told a bare term searches is what it searches. `id` is never among them: it is
    /// answered by exact lookup, not by the default fields.
    pub(super) fn searched_by_default(
        schema: &IndexSchema,
        searchable: &HashSet<String>,
        max_default_fields: usize,
    ) -> (Vec<String>, bool) {
        let candidates = searchable
            .iter()
            .filter(|name| name.as_str() != "id")
            .filter(|name| {
                schema.fields.get(*name).is_some_and(|field| {
                    field.field_type.is_default_searchable() && !field.is_shadow
                })
            })
            .cloned();
        storage::select_default_fields(
            candidates,
            schema.default_fields.as_deref(),
            max_default_fields,
        )
    }

    /// Index-level keys describing default search: the declared list (under the name `PUT
    /// /_config` accepts, so a read-modify-write keeps it), what a bare term actually searches,
    /// and whether the cap narrowed it.
    pub(super) fn insert_default_search(
        map: &mut JsonMap<String, JsonValue>,
        schema: &IndexSchema,
        searched: &[String],
        truncated: bool,
    ) {
        if let Some(declared) = &schema.default_fields {
            map.insert("default_fields".to_string(), serde_json::json!(declared));
        }
        map.insert(
            "searched_by_default".to_string(),
            serde_json::json!(searched),
        );
        if truncated {
            map.insert(
                "default_fields_truncated".to_string(),
                JsonValue::Bool(true),
            );
        }
    }

    pub(super) fn describe_fields(
        schema: &IndexSchema,
        searchable: &HashSet<String>,
        sortable: &HashSet<String>,
        searched: &[String],
    ) -> Vec<JsonValue> {
        let document_key = storage::document_key_field(schema);
        Self::sorted_field_names(schema)
            .into_iter()
            .filter(|name| name != "_seq")
            .filter_map(|name| {
                let field = schema.fields.get(&name)?;
                let mut entry = JsonMap::new();
                entry.insert("name".to_string(), JsonValue::String(name.clone()));
                entry.insert(
                    "type".to_string(),
                    JsonValue::String(field.field_type.to_string().to_string()),
                );
                entry.insert("indexed".to_string(), JsonValue::Bool(field.indexed));
                entry.insert("stored".to_string(), JsonValue::Bool(field.stored));
                entry.insert("fast".to_string(), JsonValue::Bool(field.is_fast()));
                entry.insert("shadow".to_string(), JsonValue::Bool(field.is_shadow));
                entry.insert(
                    "searchable".to_string(),
                    JsonValue::Bool(field.is_shadow || searchable.contains(&name)),
                );
                entry.insert(
                    "sortable".to_string(),
                    JsonValue::Bool(sortable.contains(&name)),
                );
                // Whether a term with no field in front of it reaches this one.
                entry.insert(
                    "default_search".to_string(),
                    JsonValue::Bool(searched.contains(&name)),
                );
                // The key under a name that is not its own, which happens on a shadow index
                // and nowhere else.
                if name == "id" && document_key != "id" {
                    entry.insert(
                        "returned_as".to_string(),
                        JsonValue::String(document_key.clone()),
                    );
                }
                if let Some(description) = &field.description {
                    entry.insert(
                        "description".to_string(),
                        JsonValue::String(description.clone()),
                    );
                }
                if let Some(tokenizer) = &field.tokenizer {
                    entry.insert(
                        "tokenizer".to_string(),
                        JsonValue::String(tokenizer.clone()),
                    );
                }
                Some(JsonValue::Object(entry))
            })
            .collect()
    }

    /// The schema as a caller reads it back.
    ///
    /// Everything the schema carries belongs here, not only its fields. This response is not just
    /// read: `PATCH /_schema` decodes it, edits what it was asked to change and writes the whole
    /// thing back, so a property omitted here is a property erased by an unrelated edit.
    pub(super) fn schema_response(
        index: &str,
        schema: &IndexSchema,
        searchable: &HashSet<String>,
        sortable: &HashSet<String>,
        max_default_fields: usize,
    ) -> JsonValue {
        let mut map = JsonMap::new();
        map.insert("name".to_string(), JsonValue::String(index.to_string()));
        // The two values the cluster settles a schema disagreement with, reported here because
        // this is where an operator reads a schema. `version` orders two schemas for one index
        // and `thumbprint` says whether they are the same schema at all — so comparing this
        // response across nodes answers "have these diverged", which otherwise cannot be asked
        // from outside the process at all. Hex, because a thumbprint is compared by eye.
        map.insert("version".to_string(), JsonValue::from(schema.version));
        map.insert(
            "thumbprint".to_string(),
            JsonValue::String(format!("{:016x}", schema.calculate_fingerprint())),
        );
        if let Some(description) = &schema.description {
            map.insert(
                "description".to_string(),
                JsonValue::String(description.clone()),
            );
        }
        // How the loader made this index's ids, so a later load makes them the same way.
        if !schema.id_fields.is_empty() {
            map.insert(
                "id_fields".to_string(),
                JsonValue::from(schema.id_fields.clone()),
            );
        }
        let (searched, truncated) =
            Self::searched_by_default(schema, searchable, max_default_fields);
        Self::insert_default_search(&mut map, schema, &searched, truncated);
        let fields = Self::describe_fields(schema, searchable, sortable, &searched);
        map.insert("field_count".to_string(), JsonValue::from(fields.len()));
        map.insert("fields".to_string(), JsonValue::Array(fields));
        JsonValue::Object(map)
    }

    /// Every field the built index can search, and every field it can sort exactly, across every
    /// shard holding this index.
    ///
    /// Gathered together because they come from the same open of the same index and are reported
    /// side by side on the same field entry — two passes over the shards to answer one question
    /// about each field would double the cost of describing an index for nothing.
    ///
    /// A union in both cases: a shard that has not built this index yet reports neither set, and
    /// describing the field as unsearchable because one shard is empty would be a worse answer
    /// than the one every populated shard gives.
    pub(super) async fn field_capabilities_across_shards(
        &self,
        index: &str,
    ) -> (HashSet<String>, HashSet<String>) {
        let stores: Vec<Arc<HybridStore>> = self
            .shards
            .values()
            .filter_map(|shard| shard.store.as_ref().map(Arc::clone))
            .collect();

        let mut searchable = HashSet::new();
        let mut sortable = HashSet::new();
        for store in stores {
            let idx = index.to_string();
            if let Ok((found, sorted)) = tokio::task::spawn_blocking(move || {
                (store.searchable_fields(&idx), store.sortable_fields(&idx))
            })
            .await
            {
                searchable.extend(found);
                sortable.extend(sorted);
            }
        }
        (searchable, sortable)
    }

    /// Creates a new NodeOrchestrator with the given configuration and identity.
    pub(crate) async fn new(
        config: NodeConfig,
        identity: NodeIdentity,
        default_search_limit: usize,
        max_concurrent_shard_searches: usize,
    ) -> Result<Self, OrchestratorError> {
        // Ensure storage directory exists
        fs::create_dir_all(&config.storage_path)?;

        info!("Node identity: {} ({})", identity.name, identity.uuid);

        // Create dedicated read thread pool for isolated search/stats operations.
        // Use configured search_threads if > 0, otherwise default to max(2, cpu_cores / 2).
        let cpu_cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        let read_threads = if config.search_threads > 0 {
            config.search_threads
        } else {
            std::cmp::max(2, cpu_cores / 2)
        };
        // Every read that reaches this pool arrives through `Handle::spawn_blocking`
        // (see MicroshardActor::spawn_on_read_pool), which dispatches to the runtime's
        // *blocking* pool — not its async worker threads. Two consequences drive this
        // configuration:
        //
        // - The async workers never run search work, so one is enough to host the runtime.
        //   Sizing them by `search_threads` just created idle threads.
        // - `max_blocking_threads` is what actually bounds search parallelism. Left at
        //   tokio's 512 default, `search_threads` named a limit it did not enforce and a
        //   burst of queries could put hundreds of concurrent tantivy searches on the CPU,
        //   thrashing against the pinned writer threads. Bounding it here turns excess
        //   load into queueing, which is the behaviour the config already advertises.
        let read_runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(read_threads)
            .thread_name("cameodb-read")
            .enable_all()
            .build()
            .map_err(|e| OrchestratorError::Io(std::io::Error::other(e)))?;
        let read_runtime = Arc::new(read_runtime);

        info!(
            max_concurrent_reads = read_threads,
            search_threads_config = config.search_threads,
            cpu_cores = cpu_cores,
            "Dedicated read thread pool created"
        );

        // Read before `config` is moved into the struct below.
        let read_budget = (config.request_timeout_secs > 0)
            .then(|| Duration::from_secs(config.request_timeout_secs));

        let quotas = Arc::new(TenantQuotas::new(config.tenant_quotas.clone()));
        let mut orchestrator = Self {
            mailbox_lane: MailboxLane::new(),
            minting: HashMap::new(),
            peer_lane: None,
            mint_rivals: HashMap::new(),
            schema_changes: HashMap::new(),
            schema_reconciler: Arc::default(),
            shards: HashMap::new(),
            writer_liveness: Arc::new(WriterLiveness::default()),
            identity,
            config,
            routing_ring: ConsistentRing::new(),
            coordinator: None,
            shared_routing_ring: Arc::new(ArcSwap::from_pointee(ConsistentRing::new())),
            core_layout: CoreLayout::detect(),
            placement: Arc::new(ArcSwap::from_pointee(ShardPlacement::default())),
            schema_cache: Arc::new(SchemaCache::new()),
            quotas,
            default_search_limit,
            max_concurrent_shard_searches,
            engine: None,
            worker_tx: None,
            worker_count: 0,
            worker_threads: Vec::new(),
            read_runtime: Some(read_runtime),
            read_pool_health: Arc::new(ReadPoolHealth::new(read_threads)),
            read_budget,
            remote_peer_pool: None,
        };

        // Discover and hydrate existing shards
        orchestrator.hydrate_existing_shards().await?;

        Ok(orchestrator)
    }

    /// Set the coordinator ActorRef after it is spawned (used for shard registration).
    pub(crate) fn set_coordinator(&mut self, coordinator: ActorRef<ClusterCoordinator>) {
        self.coordinator = Some(coordinator);
    }

    /// Set the shared remote peer pool for cached actor ref lookups.
    pub(crate) fn set_remote_peer_pool(&mut self, pool: Arc<RemotePeerPool>) {
        self.remote_peer_pool = Some(pool);
    }

    /// Returns a clone of the worker pool sender, if the pool has been spawned.
    pub(crate) fn worker_tx(&self) -> Option<OrchestratorWorkerTx> {
        self.worker_tx.clone()
    }

    /// Returns a clone of the shared routing ring for shard-affine dispatch.
    pub(crate) fn shared_routing_ring(&self) -> Arc<ArcSwap<ConsistentRing>> {
        Arc::clone(&self.shared_routing_ring)
    }

    /// Returns a clone of the published shard placement, for dispatch and local routing.
    pub(crate) fn shard_placement(&self) -> Arc<ArcSwap<ShardPlacement>> {
        Arc::clone(&self.placement)
    }

    /// Build the shared `OrchestratorEngine` and spawn the worker pool.
    ///
    /// Must be called **after** `hydrate_existing_shards` and `set_coordinator`
    /// so that shards, routing ring, and coordinator are fully initialized.
    ///
    /// Worker count formula: `min(local_shards * 2, cpu_cores * 2)`, minimum 1.
    pub(crate) fn spawn_worker_pool(&mut self) {
        // Share the same ArcSwap instances so cache writes from the actor
        // are immediately visible to workers (no duplication).
        // Ensure remote_peer_pool is set before spawning workers.
        // If not explicitly set, create a default empty pool.
        let pool = self
            .remote_peer_pool
            .clone()
            .unwrap_or_else(|| Arc::new(RemotePeerPool::new()));

        // Seed the shared ring with the current canonical state before sharing.
        self.shared_routing_ring
            .store(Arc::new(self.routing_ring.clone()));

        let engine = Arc::new(OrchestratorEngine {
            shards: ArcSwap::from_pointee(self.shards.clone()),
            routing_ring: Arc::clone(&self.shared_routing_ring),
            schema_cache: Arc::clone(&self.schema_cache),
            quotas: Arc::clone(&self.quotas),
            coordinator: self.coordinator.clone(),
            identity: self.identity.clone(),
            default_search_limit: self.default_search_limit,
            max_concurrent_shard_searches: self.max_concurrent_shard_searches,
            remote_peer_pool: pool,
            canvass: self.schema_canvass(),
        });

        // Worker count: min(local_shards * 2, cpu_cores * 2), minimum 1.
        //
        // Note this is not capped at `local_shards`. Only *writes* are shard-affine; searches
        // dispatch round-robin across the whole pool, so workers past the shard count are
        // far from idle. Writes cannot use more workers than there are shards in any case —
        // each shard has one writer thread that serialises them.
        //
        // When shard-affine dispatch and writer pinning are both on, `worker_count` is
        // forced to the core budget so that worker `i` and the writer for the shard with
        // ordinal `i` land on the same core.
        //
        // That alignment is measurably a loss, which is why `shard_affine_dispatch` defaults
        // off — see the flag's documentation in `config.rs`. Halving the pool here is the
        // cost, and it is *this line*, not thread placement, that the measurement blames.
        //
        // It was first blamed on the serial worker loop, on the theory that halving
        // `worker_count` halved the node's operation concurrency. That theory has since been
        // tested and is wrong: a worker now carries `ORCHESTRATOR_WORKER_MAX_IN_FLIGHT`
        // operations, so the affine pool holds 8 x 8 = 64 in flight against the round-robin
        // pool's 128, and affine dispatch still cost 24% of write throughput at concurrency
        // 64 (ROADMAP "Worker concurrency, measured"). What remains is the constraint itself:
        // a job for shard S may only run on worker `S % worker_count`, so an instantaneous
        // skew across shards leaves some workers idle while others queue. Round-robin has no
        // such constraint and needs no luck.
        let cpu_cores = self.core_layout.budget();
        let local_shards = self.shards.len();
        let aligned =
            self.config.shard_affine_dispatch && self.config.writer_core_affinity && cpu_cores > 0;
        let worker_count = if aligned {
            cpu_cores
        } else {
            std::cmp::max(1, std::cmp::min(local_shards * 2, cpu_cores * 2))
        };

        let per_worker_queue_capacity =
            std::cmp::max(1, ORCHESTRATOR_WORKER_QUEUE_CAPACITY / worker_count);
        let mut worker_txs = Vec::with_capacity(worker_count);

        info!(
            worker_count = worker_count,
            local_shards = local_shards,
            cpu_cores = cpu_cores,
            core_aligned = aligned,
            queue_capacity = ORCHESTRATOR_WORKER_QUEUE_CAPACITY,
            per_worker_queue_capacity = per_worker_queue_capacity,
            "Spawning orchestrator worker pool"
        );

        // True OS-thread pinning gate: all three affinity flags on, and a platform that can
        // enumerate cores. Falls back to plain tokio::spawn otherwise.
        let pin_workers =
            aligned && self.config.worker_core_affinity && self.core_layout.pinning_available();
        let worker_stats: Arc<Vec<Arc<WorkerCounters>>> = Arc::new(
            (0..worker_count)
                .map(|_| Arc::new(WorkerCounters::default()))
                .collect::<Vec<_>>(),
        );
        // Built here rather than inside `OrchestratorWorkerTx` so the worker loops, which are
        // spawned below, count their dequeue refusals into the same counters the send path uses.
        let dispatch_stats = Arc::new(DispatchCounters::default());
        let job_budget = self.read_budget;
        let mut worker_threads: Vec<std::thread::JoinHandle<()>> = Vec::new();

        for worker_id in 0..worker_count {
            let (tx, rx) = mpsc::channel::<OrchestratorJob>(per_worker_queue_capacity);
            worker_txs.push(tx);
            let engine = Arc::clone(&engine);
            let counters = Arc::clone(&worker_stats[worker_id]);
            let worker_dispatch_stats = Arc::clone(&dispatch_stats);
            // What the worker does with a job it has admitted. Cloned per operation, so it
            // holds an `Arc` rather than borrowing the engine.
            let run_op = move |op: Box<ClientOp>, _affinity_shard: Option<Uuid>| {
                let engine = Arc::clone(&engine);
                // The hint chose this worker. It has no say in which shard the op reaches.
                async move { engine.execute(*op).await }
            };

            if let Some(target_core) = pin_workers
                .then(|| self.core_layout.core_for(worker_id))
                .flatten()
            {
                // Pinned path: dedicated OS thread + current_thread runtime
                let handle = std::thread::Builder::new()
                    .name(format!("orch-worker-{}", worker_id))
                    .spawn(move || {
                        // Pin this OS thread to the target core (best-effort) and record
                        // what happened — only this thread can find out, and asking for a
                        // core is not the same as getting it.
                        if core_affinity::set_for_current(target_core) {
                            counters
                                .pinned_core
                                .store(target_core.id as i64, AtomicOrdering::Relaxed);
                            info!(
                                worker_id = worker_id,
                                core_id = target_core.id,
                                "Orchestrator worker thread pinned to CPU core"
                            );
                        } else if cfg!(target_os = "macos") {
                            info!(
                                worker_id = worker_id,
                                core_id = target_core.id,
                                "CPU pinning not supported on macOS; worker continuing unpinned"
                            );
                        } else {
                            warn!(
                                worker_id = worker_id,
                                core_id = target_core.id,
                                "Failed to pin orchestrator worker thread to CPU core"
                            );
                        }

                        // Per-worker current_thread runtime. `max_blocking_threads(4)`
                        // is plenty: hot-path search delegates to the shared
                        // `read_runtime` and writes use the pinned writer thread, so
                        // very little work hits this local blocking pool.
                        let rt = tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .max_blocking_threads(4)
                            .thread_name(format!("orch-worker-{}-bg", worker_id))
                            .build()
                            .expect("Failed to build orchestrator worker runtime");
                        rt.block_on(orchestrator_worker_loop(
                            rx,
                            run_op,
                            worker_id,
                            Some(counters),
                            ORCHESTRATOR_WORKER_MAX_IN_FLIGHT,
                            job_budget,
                            Some(worker_dispatch_stats),
                        ));
                    })
                    .expect("Failed to spawn orchestrator worker thread");
                worker_threads.push(handle);
            } else {
                // Default path: tokio task on main multi-threaded runtime.
                tokio::spawn(orchestrator_worker_loop(
                    rx,
                    run_op,
                    worker_id,
                    Some(counters),
                    ORCHESTRATOR_WORKER_MAX_IN_FLIGHT,
                    job_budget,
                    Some(worker_dispatch_stats),
                ));
            }
        }

        let tx = OrchestratorWorkerTx::new_with_stats(
            worker_txs,
            worker_stats,
            Arc::clone(&dispatch_stats),
            per_worker_queue_capacity,
            pin_workers,
            aligned,
            self.core_layout.clone(),
            Arc::clone(&self.placement),
            job_budget,
        );
        self.engine = Some(engine);
        self.worker_count = tx.len();
        self.peer_lane = Some(Arc::new(tokio::sync::Semaphore::new(
            self.worker_count.max(1) * ORCHESTRATOR_WORKER_MAX_IN_FLIGHT,
        )));
        self.worker_tx = Some(tx);
        self.worker_threads = worker_threads;

        info!(
            worker_count = self.worker_count,
            pinned = !self.worker_threads.is_empty(),
            "Orchestrator worker pool started"
        );
    }

    /// Explicitly signal all worker tasks to exit.
    /// Uses one shutdown message per worker for deterministic teardown.
    /// For Stage 2e pinned workers, also joins their OS threads so the per-worker
    /// `current_thread` runtimes are dropped before this returns.
    pub(super) async fn shutdown_worker_pool(&mut self) {
        let Some(tx) = &self.worker_tx else {
            return;
        };

        if self.worker_count == 0 {
            return;
        }

        tracing::info!(
            worker_count = self.worker_count,
            pinned_threads = self.worker_threads.len(),
            "Shutting down orchestrator worker pool"
        );
        tx.send_shutdown().await;

        // For pinned workers, join the OS threads so their runtimes drop cleanly.
        // `join` is blocking, so it must run on the blocking pool.
        if !self.worker_threads.is_empty() {
            let handles = std::mem::take(&mut self.worker_threads);
            let _ = tokio::task::spawn_blocking(move || {
                for handle in handles {
                    if let Err(panic) = handle.join() {
                        tracing::warn!(?panic, "Orchestrator worker thread panicked during join");
                    }
                }
            })
            .await;
        }
    }

    /// Publish updated shard map and routing ring to the engine's ArcSwap fields.
    /// Called after topology changes (new shards, topology updates) so workers
    /// see the latest state without restart.
    pub(super) fn publish_engine_state(&self) {
        // Single source of truth: `shared_routing_ring` is the same Arc held by
        // both the engine and the RouterActor, so one store fans out to everyone.
        self.shared_routing_ring
            .store(Arc::new(self.routing_ring.clone()));
        if let Some(engine) = &self.engine {
            engine.shards.store(Arc::new(self.shards.clone()));
        }
    }

    /// Give a shard its ordinal and publish the result.
    ///
    /// Called as each shard starts, before it can receive work. Ordinals only ever grow, so
    /// this is safe to call again for a shard that already has one — which matters because a
    /// writer thread pins itself using the ordinal it was given and cannot be re-pinned.
    ///
    /// Returns where the shard's writer thread should pin, and the cell it reports back
    /// through.
    pub(super) fn place_shard(&mut self, shard_id: Uuid) -> WriterPin {
        let mut placement = (**self.placement.load()).clone();
        let slot = placement.assign(
            shard_id,
            &self.core_layout,
            self.config.writer_core_affinity,
        );
        self.placement.store(Arc::new(placement));

        WriterPin {
            target: slot.target_core.map(|id| core_affinity::CoreId { id }),
            outcome: slot.pinned_core,
        }
    }

    /// Publish a shard as serving, once it is in the shard map.
    ///
    /// Separate from [`Self::place_shard`] because the two happen at different moments: the
    /// ordinal is needed before the shard starts, to pin its writer thread, but routing must
    /// not claim the shard until it can actually take work.
    pub(super) fn activate_shard(&mut self, shard_id: Uuid) {
        let mut placement = (**self.placement.load()).clone();
        placement.activate(shard_id);
        self.placement.store(Arc::new(placement));
    }

    /// Scans the storage directory for existing shard folders and hydrates them with
    /// bounded concurrency. The bottleneck is redb::Builder::create() which does heavy
    /// disk I/O (WAL replay, compaction). Running all shards simultaneously causes I/O
    /// contention that makes each open 10-100× slower. A semaphore limits how many shards
    /// open their redb databases concurrently.
    pub(super) async fn hydrate_existing_shards(&mut self) -> Result<(), OrchestratorError> {
        let mut existing_shards = self.discover_existing_shards()?;
        // Sorted so ordinals — and therefore worker and core placement — come out the same
        // on every restart. Directory order does not promise that, and a benchmark that
        // cannot reproduce its own thread placement is hard to read.
        existing_shards.sort();
        info!("Found {} existing shards", existing_shards.len());

        // Limit concurrent shard initialization to reduce disk I/O contention.
        // redb::Builder::create() is the bottleneck — too many concurrent opens
        // cause I/O thrashing. Scale with available cores (NVMe can handle more
        // concurrency than spinning disks): min(max(2, cpus/4), shard_count).
        let cpu_cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        let max_concurrent =
            std::cmp::min(std::cmp::max(2, cpu_cores / 4), existing_shards.len()).max(1);
        let semaphore = Arc::new(tokio::sync::Semaphore::new(max_concurrent));

        info!(
            max_concurrent = max_concurrent,
            cpu_cores = cpu_cores,
            shard_count = existing_shards.len(),
            "Hydrating shards with bounded concurrency"
        );

        let hydrate_start = Instant::now();
        let mut shard_tasks: Vec<tokio::task::JoinHandle<ShardTaskResult>> = Vec::new();

        // Create tasks for all shards — semaphore gates actual execution
        let total_shards = existing_shards.len();
        let writer_shutdown_timeout_secs = self.config.writer_shutdown_timeout_secs;
        let supervisor_timeout_secs = self.config.supervisor_timeout_secs;
        for &shard_id in &existing_shards {
            let shard_path = self.deterministic_shard_directory(shard_id);
            let storage_config = self.create_shard_storage_config(shard_id, shard_path);
            let default_search_limit = self.default_search_limit;
            let read_handle = self.read_runtime.as_ref().map(|rt| rt.handle().clone());
            let read_pool_health = Some(Arc::clone(&self.read_pool_health));
            let read_budget = self.read_budget;
            let sem = Arc::clone(&semaphore);
            let writer_liveness = Arc::clone(&self.writer_liveness);
            // Placed here rather than inside the task: hydration runs concurrently, and an
            // ordinal handed out in completion order would not survive a restart.
            let writer_core = self.place_shard(shard_id);

            let task = tokio::spawn(async move {
                // Acquire semaphore permit before starting heavy I/O
                let _permit = sem.acquire().await.map_err(|e| {
                    OrchestratorError::Io(std::io::Error::other(format!("Semaphore closed: {}", e)))
                })?;

                let mut microshard = MicroshardActor::new(
                    shard_id,
                    storage_config,
                    ShardRuntime {
                        default_search_limit,
                        read_pool_handle: read_handle,
                        read_pool_health,
                        read_budget,
                        total_shards,
                        writer_shutdown_timeout_secs,
                        supervisor_timeout_secs,
                        writer_pin: writer_core,
                        writer_liveness,
                    },
                );

                match microshard.start().await {
                    Ok(()) => {
                        info!("Hydrated shard {}", shard_id);
                        Ok((shard_id, Some(microshard)))
                    }
                    Err(e) => {
                        error!("Failed to hydrate shard {}: {}", shard_id, e);
                        Ok((shard_id, None))
                    }
                }
                // _permit dropped here, allowing next shard to start
            });
            shard_tasks.push(task);
        }

        // Wait for all shard tasks to complete
        for task in shard_tasks {
            match task.await {
                Ok(Ok((shard_id, Some(microshard)))) => {
                    if self.shards.len() < self.config.max_shards {
                        self.shards.insert(shard_id, microshard);
                        self.register_shard_for_routing(shard_id);
                        self.activate_shard(shard_id);
                    }
                }
                Ok(Ok((_, None))) => {
                    // Shard failed to hydrate, already logged above
                }
                Ok(Err(e)) => {
                    error!("Shard hydration task error: {}", e);
                }
                Err(e) => {
                    error!("Shard hydration task panicked: {}", e);
                }
            }
        }

        let hydrate_elapsed = hydrate_start.elapsed();
        info!(
            elapsed_ms = hydrate_elapsed.as_millis(),
            "All shard hydration tasks completed"
        );

        info!(
            "NodeOrchestrator startup complete with {} active shards",
            self.shards.len()
        );

        // Reader warmup is not spawned here. Each shard warms its own readers on its own
        // warmup thread (phase 2 in `start_shard`), which walks that shard's indices smallest
        // first and sequentially. A second, node-wide warmup fanned out over every
        // shard × index in parallel — as this function used to do with a `SELECT *` per pair —
        // duplicates that work and turns startup IO into a seek storm.
        //
        // The statistics cache is a different cache, and still worth priming: it is what the
        // admin and index-listing endpoints read, and computing data sizes walks the index
        // directories.
        let stats_stores: Vec<(Uuid, Arc<storage::HybridStore>)> = self
            .shards
            .iter()
            .filter_map(|(id, shard)| shard.store.clone().map(|store| (*id, store)))
            .collect();

        if !stats_stores.is_empty() {
            tokio::spawn(async move {
                let started = std::time::Instant::now();
                let shard_count = stats_stores.len();

                for (shard_id, store) in stats_stores {
                    // One shard at a time, for the same reason phase 2 warms one index at a
                    // time: these all hit the same disk.
                    match tokio::task::spawn_blocking(move || store.gather_index_stats(true)).await
                    {
                        Ok(Ok(_)) => debug!(shard_id = %shard_id, "Stats cache primed"),
                        Ok(Err(e)) => {
                            warn!(shard_id = %shard_id, error = %e, "Stats cache priming failed")
                        }
                        Err(e) => {
                            warn!(shard_id = %shard_id, error = %e, "Stats cache task panicked")
                        }
                    }
                }

                info!(
                    shards = shard_count,
                    elapsed_ms = started.elapsed().as_millis(),
                    "Index statistics cache primed for all shards"
                );
            });
        }

        Ok(())
    }

    /// Scans all configured storage directories for existing shard folders.
    pub(super) fn discover_existing_shards(&self) -> Result<Vec<Uuid>, OrchestratorError> {
        let mut shard_ids = Vec::new();
        let mut seen = HashSet::new();
        let paths_cow = self.storage_path_candidates();

        for base_path in paths_cow.as_ref() {
            if !base_path.exists() {
                continue;
            }

            for entry in fs::read_dir(base_path)? {
                let entry = entry?;
                let path = entry.path();

                if path.is_dir()
                    && let Some(dir_name) = path.file_name().and_then(|n| n.to_str())
                    && let Some(uuid_str) = dir_name.strip_prefix("shard-")
                    && let Ok(shard_id) = Uuid::parse_str(uuid_str)
                    && seen.insert(shard_id)
                {
                    info!(
                        %shard_id,
                        base = %base_path.display(),
                        "Discovered existing shard"
                    );
                    shard_ids.push(shard_id);
                }
            }
        }

        Ok(shard_ids)
    }

    /// Creates a storage configuration for a specific shard.
    pub(super) fn create_shard_storage_config(
        &self,
        _shard_id: Uuid,
        shard_path: PathBuf,
    ) -> StorageConfig {
        // Start at the minimum writer memory; storage will scale between min/max as the index grows.
        let indexer_memory_mb = self.config.indexer_memory_min_mb;

        StorageConfig {
            // The node-wide figure; `HybridStore::new` takes this shard's share of it.
            max_open_indexes: self.config.max_open_indexes,
            shard_path,

            // Memory Budget Configuration
            indexer_memory_budget: indexer_memory_mb * 1024 * 1024,
            indexer_memory_min_mb: self.config.indexer_memory_min_mb,
            indexer_memory_max_mb: self.config.indexer_memory_max_mb,
            total_memory_limit_bytes: (self.config.total_memory_limit_mb as u64) * 1024 * 1024,
            memory_pressure_threshold_percent: self.config.memory_pressure_threshold_percent,

            // Thread Configuration
            indexer_num_threads: self.config.indexer_num_threads,
            merge_num_threads: self.config.merge_num_threads,

            // Other Configuration
            default_batch_size: self.config.default_batch_size,
            wal_sync: self.config.wal_sync,
            commit_interval_ms: self.config.commit_interval_ms,
            query: self.config.query_policy.clone(),
        }
    }

    /// Handles a ProposeShard message to create a new shard.
    pub(crate) async fn handle_propose_shard(
        &mut self,
        msg: ProposeShard,
    ) -> Result<Uuid, OrchestratorError> {
        let shard_id = msg.shard_id;

        info!("Received ProposeShard request for {}", shard_id);

        // Check if shard already exists
        if self.shards.contains_key(&shard_id) {
            return Err(OrchestratorError::ShardAlreadyExists { shard_id });
        }

        // Check shard limit
        if self.shards.len() >= self.config.max_shards {
            return Err(OrchestratorError::ShardLimitExceeded {
                current: self.shards.len(),
                max: self.config.max_shards,
            });
        }

        // Create shard directory using deterministic placement
        let shard_path = self.deterministic_shard_directory(shard_id);
        fs::create_dir_all(&shard_path)?;
        info!("Created shard directory: {:?}", shard_path);

        // Create and start microshard actor
        let storage_config = self.create_shard_storage_config(shard_id, shard_path.clone());
        let read_handle = self.read_runtime.as_ref().map(|rt| rt.handle().clone());
        let total_shards = self.shards.len() + 1; // Current + new shard
        let writer_core = self.place_shard(shard_id);
        let mut microshard = MicroshardActor::new(
            shard_id,
            storage_config,
            ShardRuntime {
                default_search_limit: self.default_search_limit,
                read_pool_handle: read_handle,
                read_pool_health: Some(Arc::clone(&self.read_pool_health)),
                read_budget: self.read_budget,
                total_shards,
                writer_shutdown_timeout_secs: self.config.writer_shutdown_timeout_secs,
                supervisor_timeout_secs: self.config.supervisor_timeout_secs,
                writer_pin: writer_core,
                writer_liveness: Arc::clone(&self.writer_liveness),
            },
        );
        microshard.start().await?;

        // Add to shards map
        self.shards.insert(shard_id, microshard);
        self.register_shard_for_routing(shard_id);
        self.activate_shard(shard_id);
        if let Err(err) = self.register_shard_with_coordinator(shard_id).await {
            warn!(%shard_id, error = %err, "Failed to register new shard with coordinator");
        }

        info!(
            "Successfully created shard {} ({}/{})",
            shard_id,
            self.shard_count(),
            self.config.max_shards
        );
        Ok(shard_id)
    }

    /// Gets the node identity.
    pub(crate) fn identity(&self) -> &NodeIdentity {
        &self.identity
    }

    /// Builds ShardMetadata for a given shard id (storage stats currently stubbed).
    pub(super) fn shard_metadata(&self, shard_id: Uuid) -> ShardMetadata {
        ShardMetadata {
            shard_id,
            node_id: self.identity.uuid,
            vnode_tokens: generate_tokens(shard_id),
            storage_bytes: 0,
            document_count: 0,
        }
    }

    /// Registers a single shard with the coordinator if available.
    pub(super) async fn register_shard_with_coordinator(
        &self,
        shard_id: Uuid,
    ) -> Result<(), OrchestratorError> {
        if let Some(coordinator) = &self.coordinator {
            let metadata = self.shard_metadata(shard_id);
            coordinator
                .ask(RegisterLocalShards {
                    node_id: self.identity.uuid,
                    shards: vec![metadata],
                })
                .await
                .map_err(|e| OrchestratorError::Io(std::io::Error::other(e)))?;
        } else {
            warn!(%shard_id, "Coordinator not set; skipping shard registration");
        }
        Ok(())
    }

    /// Registers all known shards with the coordinator (called on startup after coordinator set).
    pub(crate) async fn register_all_shards_with_coordinator(
        &self,
    ) -> Result<(), OrchestratorError> {
        if let Some(coordinator) = &self.coordinator {
            let shards: Vec<ShardMetadata> = self
                .shards
                .keys()
                .copied()
                .map(|shard_id| self.shard_metadata(shard_id))
                .collect();
            if !shards.is_empty() {
                let _: () = coordinator
                    .ask(RegisterLocalShards {
                        node_id: self.identity.uuid,
                        shards,
                    })
                    .await
                    .map_err(|e| OrchestratorError::Io(std::io::Error::other(e)))?;
            }
        } else {
            warn!("Coordinator not set; skipping bulk shard registration");
        }
        Ok(())
    }

    /// Shutdown all shards gracefully, committing pending writes and releasing resources.
    ///
    /// Shutdown sequence per shard (order is critical for data integrity):
    /// 1. Stop the dedicated writer thread — drains queued commands and completes
    ///    in-flight writes before returning.
    /// 2. Take exclusive ownership of the store Arc (no other references remain
    ///    after the writer thread has exited).
    /// 3. Call `store.shutdown()` in a blocking task — commits pending tantivy
    ///    writers and flushes redb WAL.
    /// 4. Explicitly `drop(store)` inside the blocking task so redb file handles
    ///    are released deterministically before the task completes.
    pub(super) async fn shutdown_all_shards(&mut self) -> Result<(), OrchestratorError> {
        tracing::info!("NodeOrchestrator: Shutting down all shards");

        self.shutdown_worker_pool().await;

        let mut errors = Vec::new();

        // Stop writer threads concurrently so they release their IndexWriter locks before
        // storage shutdown. Each wait is bounded by `writer_shutdown_timeout_secs`; running
        // them in parallel caps this phase at one timeout rather than the sum across shards.
        join_all(self.shards.iter_mut().map(|(shard_id, shard)| {
            tracing::debug!(shard_id = %shard_id, "Shutting down shard writer thread");
            shard.shutdown_writer()
        }))
        .await;

        // Take every store out first, then republish the engine snapshot before shutting any
        // of them down. The worker pool routes through a *clone* of each shard actor
        // (`OrchestratorEngine.shards`), and a cloned actor carries its own
        // `Arc<HybridStore>` — so leaving the snapshot in place keeps every shard's index
        // mmaps, tantivy writer lock and redb database alive past the shutdown that reports
        // releasing them. Nothing routes through it at this point: HTTP drained a phase ago.
        let mut taken: Vec<(Uuid, Arc<HybridStore>)> = Vec::new();
        for (shard_id, shard) in self.shards.iter_mut() {
            match shard.store.take() {
                Some(store) => taken.push((*shard_id, store)),
                None => {
                    tracing::warn!(shard_id = %shard_id, "Shard store already taken, skipping shutdown")
                }
            }
        }
        self.publish_engine_state();

        // Parallel storage shutdown with per-shard 30s timeout
        let mut shard_ids = Vec::new();
        let mut shutdown_futures = Vec::new();
        for (shard_id, store) in taken {
            let shard_id_clone = shard_id;

            let future = tokio::time::timeout(
                Duration::from_secs(30),
                tokio::task::spawn_blocking(move || {
                    tracing::info!(shard_id = %shard_id_clone, "Calling storage shutdown");
                    if let Err(e) = store.shutdown() {
                        tracing::error!(shard_id = %shard_id_clone, error = %e, "Storage shutdown failed");
                        return Err(e);
                    }
                    // Dropping this handle releases the index mmaps and the tantivy
                    // writer lock only if it is the last one. The writer and warmup
                    // threads hold clones of their own, so a surviving reference means
                    // those file handles outlive the shutdown that reports releasing them.
                    match Arc::try_unwrap(store) {
                        Ok(store) => {
                            // Dropped inside the blocking task, so the release happens
                            // here rather than on whichever thread runs the last clone.
                            drop(store);
                            tracing::debug!(shard_id = %shard_id_clone, "Storage dropped, file handles released");
                        }
                        Err(store) => {
                            tracing::warn!(
                                shard_id = %shard_id_clone,
                                strong_count = Arc::strong_count(&store),
                                "Storage still referenced after shutdown; file handles stay open until the last holder drops"
                            );
                        }
                    }
                    Ok(())
                }),
            );
            shard_ids.push(shard_id_clone);
            shutdown_futures.push(future);
        }

        let results = join_all(shutdown_futures).await;
        for (shard_id, result) in shard_ids.iter().zip(results.iter()) {
            match result {
                Ok(Ok(Ok(()))) => {
                    tracing::debug!(shard_id = %shard_id, "Shard storage shutdown successful");
                }
                Ok(Ok(Err(e))) => {
                    let msg = format!("Shard {} storage shutdown error: {}", shard_id, e);
                    tracing::error!(shard_id = %shard_id, error = %e, "{}", msg);
                    errors.push(msg);
                }
                Ok(Err(e)) => {
                    let msg = format!("Shard {} shutdown task failed: {}", shard_id, e);
                    tracing::error!(shard_id = %shard_id, error = %e, "{}", msg);
                    errors.push(msg);
                }
                Err(_) => {
                    let msg = format!("Shard {} storage shutdown timed out after 30s", shard_id);
                    tracing::error!(shard_id = %shard_id, "{}", msg);
                    errors.push(msg);
                }
            }
        }

        if errors.is_empty() {
            tracing::info!("NodeOrchestrator: All shards shut down successfully");
            Ok(())
        } else {
            tracing::warn!(
                error_count = errors.len(),
                "NodeOrchestrator: Some shards failed to shutdown"
            );
            Err(OrchestratorError::Io(std::io::Error::other(format!(
                "Shutdown errors: {}",
                errors.join("; ")
            ))))
        }
    }

    /// Registers a shard with the routing ring for consistent hashing.
    pub(super) fn register_shard_for_routing(&mut self, shard_id: Uuid) {
        let simple = shard_id.simple().to_string();
        let name: String = simple.chars().take(3).collect();
        let identity = NodeIdentity {
            uuid: shard_id,
            name,
            vnode_tokens: generate_tokens(shard_id),
            keypair: None,
        };
        self.routing_ring.add_node(&identity);
        self.publish_engine_state();
    }

    /// Gets the number of active shards.
    pub(crate) fn shard_count(&self) -> usize {
        self.shards.len()
    }

    /// A handle to this node's writer-thread liveness, for the health endpoint to read. Taken
    /// before the orchestrator is moved into its actor, so health can probe it without a message.
    pub(crate) fn writer_liveness(&self) -> Arc<WriterLiveness> {
        Arc::clone(&self.writer_liveness)
    }

    /// A handle to this node's read-pool health, for the health endpoint to read its saturation
    /// gauge and wedge state. Taken before the orchestrator is moved into its actor, like
    /// [`writer_liveness`](Self::writer_liveness), so health can probe it without a message.
    pub(crate) fn read_pool_health(&self) -> Arc<ReadPoolHealth> {
        Arc::clone(&self.read_pool_health)
    }

    /// A handle to this node's backlog estimate, for the HTTP admission guard to refuse against
    /// and for health to report. Taken the same way the two handles above are.
    ///
    /// `None` before the worker pool exists — a node running everything through the actor
    /// mailbox has no worker queue to predict, and the guard stays out of the way.
    pub(crate) fn queue_load(&self) -> Option<Arc<QueueLoad>> {
        self.worker_tx.as_ref().map(|tx| tx.queue_load())
    }

    // ========================================================================
    // Client Operation Handling (for actor-based access - no locks needed)
    // ========================================================================

    /// Handles client operations. Called from Message<ClientOp> handler.
    ///
    /// Writes and deletes may answer [`Answer::Later`]: the part that waits on a peer, for the
    /// handler to run off the mailbox. Everything else is answered here and now.
    pub(super) async fn handle_client_op(&mut self, op: ClientOp) -> Answer {
        Answer::Now(match op {
            ClientOp::Search {
                index,
                query,
                limit,
                offset,
                fields,
                sort,
            } => {
                self.orch_search(
                    &index,
                    &query,
                    SearchWindow {
                        offset: offset.unwrap_or(0),
                        limit: limit.unwrap_or(self.default_search_limit),
                    },
                    fields.as_deref(),
                    sort.as_ref(),
                )
                .await
            }
            ClientOp::Stream {
                index,
                query,
                limit,
                fields,
                sort,
            } => {
                // Use streaming search with the same logic as Search but optimized for HTTP streaming
                let search_limit = limit.unwrap_or(self.default_search_limit);
                self.orch_search(
                    &index,
                    &query,
                    SearchWindow::first(search_limit),
                    fields.as_deref(),
                    sort.as_ref(),
                )
                .await
            }
            ClientOp::Write {
                index,
                id,
                routing_key,
                doc,
                forwarded,
                schema_body,
                tenant,
            } => {
                return self
                    .orch_write(
                        &index,
                        id,
                        routing_key,
                        doc,
                        forwarded,
                        schema_body,
                        tenant.as_deref(),
                        None,
                    )
                    .await
                    .into();
            }
            ClientOp::BulkWrite {
                index,
                docs,
                forwarded,
                schema_body,
                tenant,
            } => {
                return self
                    .orch_bulk_write(
                        &index,
                        docs,
                        forwarded,
                        schema_body,
                        tenant.as_deref(),
                        None,
                    )
                    .await
                    .into();
            }
            ClientOp::Delete {
                index,
                id,
                routing_key,
                forwarded,
            } => {
                return self
                    .orch_delete(&index, id, routing_key, forwarded)
                    .await
                    .into();
            }
            ClientOp::BulkDelete {
                index,
                docs,
                forwarded,
            } => return self.orch_bulk_delete(&index, docs, forwarded).await.into(),
            ClientOp::PrepareSchema {
                index,
                schema,
                tenant,
                change,
            } => {
                let now = Instant::now();
                let busy = self
                    .schema_changes
                    .get(&index)
                    .is_some_and(|(held, until)| *held != change && *until > now);
                if !busy {
                    self.schema_changes
                        .insert(index.clone(), (change, now + SCHEMA_CHANGE_RESERVATION));
                }
                self.orch_prepare_schema(&index, schema, tenant, busy).await
            }
            ClientOp::ApplySchema {
                index,
                schema,
                check_quota,
                change,
            } => {
                let applied = self.orch_apply_schema(&index, schema, check_quota).await;
                self.release_schema_change(&index, change);
                applied
            }
            ClientOp::ReleaseSchemaChange { index, change } => {
                self.release_schema_change(&index, change);
                Ok(JsonValue::Null)
            }
            ClientOp::ReconcileSchema { index } => {
                self.request_schema_reconcile(&index);
                Ok(JsonValue::Null)
            }
            ClientOp::GetConfig { index } => self.orch_get_config(&index).await,
            ClientOp::GetRawSchema { index, minting_by } => {
                self.raw_schema_for_peer(index, minting_by).await
            }
            // Normally a worker's — see `OrchestratorEngine::execute`. Here only when no worker
            // took it, and still without holding this mailbox across the canvass for longer than
            // the peers take to answer; the answer is the same either way.
            ClientOp::FindSchemaInCluster { index } => match self.durable_schema(&index).await {
                Ok(held) => find_schema_in_cluster(held, &self.schema_canvass(), index).await,
                Err(err) => Err(err),
            },
            ClientOp::ValidateQuery { index, query } => {
                self.orch_validate_query(&index, &query).await
            }
            ClientOp::ListIndexes { include_data_size } => {
                self.orch_list_indexes(include_data_size).await
            }
            ClientOp::ListClusterIndexes { include_data_size } => {
                self.orch_list_indexes(include_data_size).await
            }
            ClientOp::GetIdentity => self.orch_get_identity().await,
            ClientOp::DeleteIndex {
                index,
                delete_schema,
            } => self.orch_delete_index(&index, delete_schema).await,
        })
    }

    /// Delete an index and all its data from all local shards (parallel)
    pub(super) async fn orch_delete_index(
        &self,
        index: &str,
        delete_schema: bool,
    ) -> Result<JsonValue, OrchestratorError> {
        if self.shards.is_empty() {
            return Err(OrchestratorError::NotReady(
                "No shards available".to_string(),
            ));
        }

        // Delete index data from all local shards in parallel
        let delete_futures: Vec<_> = self
            .shards
            .iter()
            .map(|(shard_id, shard)| {
                let shard_id = *shard_id;
                let index = index.to_string();
                async move {
                    let result = shard.delete_index(&index, delete_schema).await;
                    (shard_id, result)
                }
            })
            .collect();

        let results = futures::future::join_all(delete_futures).await;

        let mut deleted_from_shards = 0;
        let mut errors = Vec::new();

        for (shard_id, result) in results {
            match result {
                Ok(_) => {
                    deleted_from_shards += 1;
                    tracing::info!(
                        shard_id = %shard_id,
                        index = %index,
                        delete_schema = delete_schema,
                        "Deleted index data from shard"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        shard_id = %shard_id,
                        index = %index,
                        error = %e,
                        "Failed to delete index from shard (may not exist)"
                    );
                    errors.push(format!("shard {}: {}", shard_id, e));
                }
            }
        }

        // Drop the entry, then cache what the shards now hold in its place.
        //
        // An empty entry gives `SchemaCache::put` nothing to compare against, so a write that
        // resolved against the old schema and is only now finishing validation would install
        // it again. The record of the deletion carries a version above anything a write in
        // flight holds, which is what makes that comparison refuse.
        self.schema_cache.remove(index);
        if delete_schema
            && let Some(shard) = self.shards.values().next()
            && let Some(store) = &shard.store
        {
            let sc = Arc::clone(store);
            let idx = index.to_string();
            if let Ok(Ok(Some(dropped))) =
                tokio::task::spawn_blocking(move || sc.get_schema(&idx)).await
            {
                self.schema_cache.put(index, &dropped);
            }
        }

        Ok(serde_json::json!({
            "success": true,
            "index": index,
            "deleted_from_shards": deleted_from_shards,
            "total_shards": self.shards.len(),
            "errors": errors
        }))
    }

    // Nine parameters, and each one is a decision the caller has already made: which index,
    // which document, where it routes, whether this is a forward, what schema came with it,
    // whose quota it stamps, and what the peers said when canvassed outside the mailbox. Bundling them into a struct would move the argument list rather
    // than shorten it, and the op they are destructured from is that struct.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn orch_write(
        &self,
        index: &str,
        id: String,
        routing_key: Option<String>,
        doc: JsonValue,
        forwarded: bool,
        schema_body: Option<Box<IndexSchema>>,
        tenant: Option<&str>,
        settled: Option<PeerSchemaLookup>,
    ) -> Result<Answer, OrchestratorError> {
        if self.shards.is_empty() {
            return Err(OrchestratorError::NotReady("No shards".to_string()));
        }

        // Lock-free schema lookup, by the one thing that identifies an index: its name.
        let schema = self.load_schema(index).await?;
        self.quotas
            .check_write(owner_of(&schema, tenant), &self.shards)?;

        let ctx = WriteCtx {
            shards: &self.shards,
            ring: &self.routing_ring,
            schema_cache: &self.schema_cache,
        };

        // Fast path: mature schema — the gate validates, caches and routes; the dispatch is
        // the same one the worker lane runs, so the two paths cannot drift on where a
        // document lands.
        let (id, routing_key, doc) = match ctx.gate(index, &id, &routing_key, &doc, &schema)? {
            // needs_evolution, or an empty schema: fall through to the slow path below.
            WriteGate::Grow => (id, routing_key, doc),
            WriteGate::Routed {
                target,
                effective_routing_key,
            } => {
                return match ctx
                    .dispatch(target, index, id, effective_routing_key, doc)
                    .await?
                {
                    WriteDispatch::Done(response) => Ok(Answer::Now(Ok(response))),
                    WriteDispatch::Elsewhere {
                        id,
                        effective_routing_key,
                        doc,
                    } => Ok(self.forward_later(
                        target,
                        forwarded,
                        ClientOp::Write {
                            index: index.to_string(),
                            id,
                            routing_key: effective_routing_key,
                            doc,
                            forwarded: true,
                            // This node reached the fast path, which means it holds a
                            // schema and this document needs nothing added to it. The
                            // owner may hold none, and `forwarded` above is all it
                            // needs to know not to invent one: it asks, and the resend
                            // below carries the body. Nothing speculative on the wire.
                            schema_body: None,
                            // Forwarded, so it mints nothing and stamps nothing.
                            tenant: None,
                        },
                        Some(Arc::clone(&schema)),
                    )),
                };
            }
        };

        // Slow path: initial schema creation or schema evolution needed
        // Must use full staged_schema_validation with DocPayload wrapping
        let doc_payload = DocPayload {
            id: id.clone(),
            routing_key: routing_key.clone(),
            doc: doc.clone(),
        };
        // A refcount bump, not a copy. Copy-on-write inside staged validation means a single
        // write that turns out to need no schema change pays nothing for the possibility.
        let mut schema_mut = Arc::clone(&schema);

        let (validation_summary, _docs) = self
            .staged_schema_validation(
                index,
                vec![doc_payload],
                &mut schema_mut,
                forwarded,
                schema_body.as_deref(),
                tenant,
                settled,
            )
            .await?;

        self.schema_cache.keep_settled(index, &schema_mut);

        if !validation_summary.errors.is_empty() {
            // One document, so its position adds nothing to the message.
            return Err(OrchestratorError::Validation(
                validation_summary
                    .errors
                    .into_iter()
                    .map(|(_, reason)| reason)
                    .collect::<Vec<_>>()
                    .join("; "),
            ));
        }

        // Schema-based routing
        let effective_routing_key = effective_routing_key(&schema_mut, &id, routing_key, &doc);

        let target = ctx.route_write(&effective_routing_key)?;

        match ctx
            .dispatch(target, index, id, effective_routing_key, doc)
            .await?
        {
            WriteDispatch::Done(response) => Ok(Answer::Now(Ok(response))),
            WriteDispatch::Elsewhere {
                id,
                effective_routing_key,
                doc,
            } => Ok(self.forward_later(
                target,
                forwarded,
                ClientOp::Write {
                    index: index.to_string(),
                    id,
                    routing_key: effective_routing_key,
                    doc,
                    // Nothing speculative: `forwarded` is the signal, and the owner
                    // asks for the body if it needs one.
                    schema_body: None,
                    forwarded: true,
                    // Forwarded, so it mints nothing and stamps nothing.
                    tenant: None,
                },
                Some(Arc::clone(&schema_mut)),
            )),
        }
    }

    /// [`engine_delete`](OrchestratorEngine::engine_delete) on the actor.
    ///
    /// Reached two ways: a peer forwarding the op over `cameo.orchestrator.client_op`, and the
    /// mailbox fallback when a worker queue is full. Neither is the hot path, and both have to
    /// route a delete the same way the engine does or the same id would resolve to two shards
    /// depending on how busy the node was.
    pub(super) async fn orch_delete(
        &self,
        index: &str,
        id: String,
        routing_key: Option<String>,
        forwarded: bool,
    ) -> Result<Answer, OrchestratorError> {
        if self.shards.is_empty() {
            return Err(OrchestratorError::NotReady("No shards".to_string()));
        }

        let schema = self.load_schema(index).await?;

        let effective = effective_delete_routing_key(&schema, &id, routing_key.clone())?;

        let ctx = WriteCtx {
            shards: &self.shards,
            ring: &self.routing_ring,
            schema_cache: &self.schema_cache,
        };
        let target = ctx.route_write(&Some(effective.clone()))?;

        match ctx.dispatch_delete(target, index, &id).await? {
            Some(response) => Ok(Answer::Now(Ok(response))),
            // The ring placed this delete on a peer. Forward it; the peer re-derives the key
            // from the same schema and routes to the shard it actually hosts.
            None => Ok(self.forward_later(
                target,
                forwarded,
                ClientOp::Delete {
                    index: index.to_string(),
                    id,
                    routing_key: Some(effective),
                    forwarded: true,
                },
                // A delete carries no document, so it can never need a schema.
                None,
            )),
        }
    }

    /// Remove many documents in one request.
    ///
    /// The schema is read once — for routing — and never written, because a delete carries
    /// nothing that could grow it. That is what lets the same body run on a worker: this head
    /// loads the schema and builds the borrowed view, and [`BulkCtx::apply_bulk_delete`] is
    /// the routing, grouping, forwarding and accounting the worker lane runs unchanged.
    pub(super) async fn orch_bulk_delete(
        &self,
        index: &str,
        docs: Vec<DeletePayload>,
        forwarded: bool,
    ) -> Result<Answer, OrchestratorError> {
        let start = std::time::Instant::now();
        if self.shards.is_empty() {
            return Err(OrchestratorError::NotReady("No shards".to_string()));
        }

        let schema = self.load_schema(index).await?;
        // A share forwarded here goes no further, so it never waits on a peer and is served in
        // place. A first hop forwards its remote shares, so it goes off the mailbox — see
        // `Answer`.
        if forwarded {
            let ctx = BulkCtx {
                shards: &self.shards,
                ring: &self.routing_ring,
                coordinator: self.coordinator.as_ref(),
                remote_peer_pool: self.remote_peer_pool.as_deref(),
            };
            return Ok(Answer::Now(
                ctx.apply_bulk_delete(index, docs, &schema, forwarded, start)
                    .await,
            ));
        }
        let view = self.owned_bulk_view();
        let index = index.to_string();
        Ok(Answer::Later(Box::pin(async move {
            view.ctx()
                .apply_bulk_delete(&index, docs, &schema, forwarded, start)
                .await
        })))
    }

    /// Forward a single write or delete to the node that owns `target` — as a future that owns
    /// what it needs, so the mailbox is not held while the peer answers.
    ///
    /// Reached when the ring places a write or delete on a shard this node does not host. The
    /// remote node re-derives the routing key from its own schema, so the op travels the way the
    /// caller sent it and routes identically on the other side.
    ///
    /// **One hop, and only one.** The op that arrives carries `forwarded`, and `already_forwarded`
    /// is that flag: a node that was itself forwarded to refuses rather than forwarding on. Both
    /// ends decide from their own view of the ring and their own copy of the shard assignments,
    /// and those views disagree while membership is changing — two nodes each certain the other
    /// owns the shard would otherwise pass one write between them until something timed out,
    /// once per write. Refusing states the disagreement instead, and the caller's retry lands
    /// after the views have converged.
    pub(super) fn forward_later(
        &self,
        target: Uuid,
        already_forwarded: bool,
        op: ClientOp,
        established: Option<Arc<IndexSchema>>,
    ) -> Answer {
        if already_forwarded {
            return Answer::Now(Err(OrchestratorError::Io(std::io::Error::other(format!(
                "shard {target} was forwarded here and is not local either: this node and the \
                 one that forwarded disagree about who owns it. Retry once the cluster has \
                 settled"
            )))));
        }
        let coordinator = self.coordinator.clone();
        let pool = self.remote_peer_pool.clone();
        Answer::Later(Box::pin(async move {
            let node_id = if let Some(coord) = &coordinator {
                coord
                    .ask(GetShardAssignments)
                    .await
                    .unwrap_or_default()
                    .get(&target)
                    .map(|meta| meta.node_id)
            } else {
                None
            };

            let Some(node_id) = node_id else {
                return Err(OrchestratorError::Missing(format!(
                    "shard {target} is not local and no node owns it"
                )));
            };

            let pool = pool.ok_or_else(|| {
                OrchestratorError::NotReady("Remote peer pool not initialized".to_string())
            })?;

            // One retry, and only for the one answer a retry can change. The peer holds no
            // schema for this index and said so rather than canvassing anyone, so the resend
            // carries the body — see [`CarriedSchema`]. Once per node per index; every other
            // forward, and every other failure, goes through here untouched.
            pool.converse(node_id, async {
                let remote = lookup_peer_orchestrator(&pool, node_id).await?;
                match remote_answer(remote.ask(&op).await) {
                    Err(err) if matches!(err.verdict(), RemoteVerdict::SchemaRequired) => {
                        let Some(schema) = established.as_deref() else {
                            return Err(err);
                        };
                        let Some(resend) = with_schema_body(&op, schema) else {
                            return Err(err);
                        };
                        debug!(
                            %node_id,
                            "Peer holds no schema for this index; resending the write with the schema"
                        );
                        remote_answer(remote.ask(&resend).await)
                    }
                    other => other,
                }
            })
            .await
        }))
    }

    /// The borrowed bulk view's owned twin, for a fan-out that runs off the mailbox.
    pub(super) fn owned_bulk_view(&self) -> OwnedBulkView {
        OwnedBulkView {
            shards: self
                .engine
                .as_ref()
                .map(|engine| engine.shards.load_full())
                .unwrap_or_else(|| Arc::new(self.shards.clone())),
            ring: self.shared_routing_ring.load_full(),
            coordinator: self.coordinator.clone(),
            pool: self.remote_peer_pool.clone(),
        }
    }

    /// Write many documents in one request — the slow half of the bulk-write path.
    ///
    /// Reached on the mailbox when the batch has a schema question to answer: the engine
    /// serves the settled case itself and hands back `NeedsActor` the moment a batch needs a
    /// schema written, because writing one is serialised here — two bulks evolving one index
    /// at once is what the mailbox keeps from happening. It is also where a peer's forwarded
    /// share arrives, since `forwarded`/`schema_body` are the terms [`staged_schema_validation`]
    /// decides on.
    ///
    /// Once validation is settled the fan-out is [`BulkCtx::apply_bulk_write`], the same body
    /// the worker lane runs.
    pub(super) async fn orch_bulk_write(
        &self,
        index: &str,
        docs: Vec<DocPayload>,
        forwarded: bool,
        schema_body: Option<Box<IndexSchema>>,
        tenant: Option<&str>,
        settled: Option<PeerSchemaLookup>,
    ) -> Result<Answer, OrchestratorError> {
        let start = std::time::Instant::now();
        if self.shards.is_empty() {
            return Err(OrchestratorError::NotReady("No shards".to_string()));
        }

        // `load_schema` answers from the cache when it can, and reads a shard when it cannot.
        // The handle is shared, not owned: staged validation takes `&mut Arc` and reaches for
        // `Arc::make_mut`, so the field map is deep-copied only if this write actually changes
        // it — which most writes do not. It used to be unconditionally unwrapped or cloned here.
        let mut schema_mut = self.load_schema(index).await?;
        self.quotas
            .check_write(owner_of(&schema_mut, tenant), &self.shards)?;

        // Use staged schema validation: parallel validation + sequential evolution. The batch
        // is handed over and handed back so the fan-out never has to copy it.
        let (validation_summary, docs) = self
            .staged_schema_validation(
                index,
                docs,
                &mut schema_mut,
                forwarded,
                schema_body.as_deref(),
                tenant,
                settled,
            )
            .await?;

        self.schema_cache.keep_settled(index, &schema_mut);

        // Documents that failed validation are dropped, and their reasons travel to the caller
        // in the response's `errors`. Rejecting the whole batch is the other defensible policy
        // and not this one's: a bulk write already reports partial success, and one bad row in
        // an import is no reason to discard the rest.
        let mut rejections: Vec<String> = Vec::new();
        let refused: HashSet<usize> = if validation_summary.errors.is_empty() {
            HashSet::new()
        } else {
            tracing::warn!(
                index = %index,
                error_count = validation_summary.errors.len(),
                total_docs = validation_summary.total_docs,
                "Some documents failed schema validation and were not written"
            );
            rejections.extend(
                validation_summary
                    .errors
                    .iter()
                    .map(|(position, reason)| format!("document {position}: {reason}")),
            );
            validation_summary
                .errors
                .iter()
                .map(|(position, _)| *position)
                .collect()
        };

        // The position travels with the document from here on. Everything downstream that can
        // lose one — routing, a shard that will not take its batch, a peer that refuses part of
        // what it was sent — reports it by the number the caller used.
        let pending: Vec<Placed> = docs
            .into_iter()
            .enumerate()
            .filter(|(position, _)| !refused.contains(position))
            .map(|(position, doc)| Placed {
                position,
                doc,
                routing_key: None,
            })
            .collect();

        // From here the body is the one the worker lane runs too — same grouping, same one-hop
        // bound, same accounting — so it is written once, against the view `BulkCtx` names.
        //
        // A share forwarded here goes no further and is served in place. A first hop forwards
        // its remote shares to their owners, so everything past the schema — the part that
        // needed this actor — goes off the mailbox: see `Answer`.
        if forwarded {
            let ctx = BulkCtx {
                shards: &self.shards,
                ring: &self.routing_ring,
                coordinator: self.coordinator.as_ref(),
                remote_peer_pool: self.remote_peer_pool.as_deref(),
            };
            return Ok(Answer::Now(
                ctx.apply_bulk_write(index, pending, rejections, &schema_mut, forwarded, start)
                    .await,
            ));
        }
        let view = self.owned_bulk_view();
        let index = index.to_string();
        Ok(Answer::Later(Box::pin(async move {
            view.ctx()
                .apply_bulk_write(&index, pending, rejections, &schema_mut, forwarded, start)
                .await
        })))
    }

    /// Helper method to group local documents by shard
    pub(super) fn group_local_documents(
        local_docs: Vec<(Placed, Uuid)>,
    ) -> HashMap<Uuid, Vec<Placed>> {
        let mut batches: HashMap<Uuid, Vec<Placed>> = HashMap::new();

        for (placed, shard_id) in local_docs {
            batches.entry(shard_id).or_default().push(placed);
        }

        batches
    }

    pub(super) async fn orch_search(
        &self,
        index: &str,
        query: &str,
        window: SearchWindow,
        fields: Option<&[String]>,
        sort: Option<&SortSpec>,
    ) -> Result<JsonValue, OrchestratorError> {
        if self.shards.is_empty() {
            return Ok(
                serde_json::json!({"hits": [], "hits_returned": 0, "total_hits": 0, "took_ms": 0}),
            );
        }

        // Get the schema for shadow field transformation. Read through to the store on a miss
        // rather than treating "not cached yet" as "no schema": with an empty schema the
        // projection rewrite in the gather is a no-op, so the first search after a boot dropped
        // any field a shadow name refers to. One disk read per index per process.
        let schema = self.load_schema(index).await?;

        ScatterCtx {
            shards: &self.shards,
            schema: &schema,
            max_concurrent_shard_searches: self.max_concurrent_shard_searches,
        }
        .gather(index, query, window, fields, sort)
        .await
    }

    /// Ask the cluster to agree on the fields this node learned for `index`. A standalone node
    /// has nobody to agree with: what it learned is the index's schema.
    pub(super) fn request_schema_reconcile(&self, index: &str) {
        if self.config.clustered
            && let Some(coordinator) = self.coordinator.clone()
        {
            self.schema_reconciler.request(index, coordinator);
        }
    }

    /// End `change`'s reservation of `index`, if it still holds it.
    fn release_schema_change(&mut self, index: &str, change: Uuid) {
        if self
            .schema_changes
            .get(index)
            .is_some_and(|(held, _)| *held == change)
        {
            self.schema_changes.remove(index);
        }
    }

    /// A declaration as every node stores it: normalized, with an explicit `id`, and refused
    /// with a `400` naming the field when it lists a default field it lacks or a tokenizer this
    /// node cannot build. Each node checks with its own tokenizers, so a node on an older build
    /// refuses in phase one rather than failing every commit after phase two.
    fn checked_declaration(mut schema: IndexSchema) -> Result<IndexSchema, OrchestratorError> {
        schema.normalize_after_deserialization();
        // A declared default-field list names fields of this schema, or it is refused: a typo
        // left to be filtered out at query time would search fewer fields than the caller wrote,
        // and nothing would say so. Its length is not checked against the node's cap — the cap
        // is node config and can change after the list is written, so it is applied where the
        // query runs, to the list's first entries.
        schema
            .validate_default_fields()
            .map_err(OrchestratorError::Validation)?;
        // A tokenizer this node cannot build would store, take writes, and then fail every
        // commit. Refused here so the caller hears it as a 400 naming the field; the store
        // refuses it too, for schemas that arrive by other routes.
        schema
            .validate_tokenizers()
            .map_err(OrchestratorError::Validation)?;
        // Ensure 'id' field is explicitly in the schema for visibility
        if !schema.fields.contains_key("id") {
            schema.fields.insert(
                "id".to_string(),
                FieldDef::new("id".to_string(), TantivyFieldType::Text),
            );
        }
        Ok(schema)
    }

    /// This node's documents of an index, across its shards.
    async fn local_document_count(&self, index: &str) -> Result<u64, OrchestratorError> {
        let mut documents = 0u64;
        for store in self
            .shards
            .values()
            .filter_map(|shard| shard.store.as_ref())
        {
            let store = Arc::clone(store);
            let idx = index.to_string();
            documents += tokio::task::spawn_blocking(move || store.document_count(&idx))
                .await
                .map_err(|e| OrchestratorError::Io(std::io::Error::other(e.to_string())))??;
        }
        Ok(documents)
    }

    /// Phase one of `PUT /_config` on this node: see [`ClientOp::PrepareSchema`]. Writes
    /// nothing.
    ///
    /// A built index keeps the columns it was built with, so a change to one needs the index
    /// built again — free while it holds no documents. The coordinating node adds up what every
    /// node reports here and refuses a change the built index would act against while the
    /// cluster holds any document of it: a retype, a tokenizer, another id.
    pub(super) async fn orch_prepare_schema(
        &self,
        index: &str,
        schema: IndexSchema,
        tenant: Option<String>,
        busy: bool,
    ) -> Result<JsonValue, OrchestratorError> {
        let schema = Self::checked_declaration(schema)?;
        // Read from durable state rather than the cache, which is filled lazily: its version is
        // what the coordinating node counts the next one from.
        let current = self.durable_schema(index).await?;
        let held = current
            .as_deref()
            .filter(|current| current.state != storage::SchemaState::Dropped);
        let changes = held
            .map(|current| current.rebuild_changes(&schema))
            .unwrap_or_default();
        let documents = if changes.is_empty() {
            0
        } else {
            self.local_document_count(index).await?
        };
        // A new index here counts against the caller's quota. Whether the index is new to the
        // cluster is the coordinating node's to say, so this only reports.
        let quota_exceeded = match tenant.as_deref() {
            Some(tenant) if held.is_none() && self.quotas.max_indexes(tenant).is_some() => {
                let owned = owned_index_count(&self.shards, tenant).await?;
                self.quotas
                    .check_mint(tenant, owned)
                    .err()
                    .map(|e| e.to_string())
            }
            _ => None,
        };
        let unbuilt = match held {
            Some(held) => self.unbuilt_promotions(index, held, &schema).await?,
            None => Vec::new(),
        };
        let (conflicts, pending): (Vec<_>, Vec<_>) =
            changes.into_iter().partition(|change| change.conflicts);
        let readiness = SchemaReadiness {
            current: current.map(|current| (*current).clone()),
            documents,
            conflicts: conflicts.into_iter().map(|c| c.what).collect(),
            pending: pending.into_iter().map(|c| c.what).collect(),
            unbuilt,
            quota_exceeded,
            busy,
        };
        serde_json::to_value(readiness).map_err(|e| OrchestratorError::Io(std::io::Error::other(e)))
    }

    /// The fields `next` marks indexed that `held` does not, and that some shard's built index
    /// has no column for — asked of the shards, since only the built index knows. A field whose
    /// column was built and later switched off is searchable again as soon as it is switched on.
    async fn unbuilt_promotions(
        &self,
        index: &str,
        held: &IndexSchema,
        next: &IndexSchema,
    ) -> Result<Vec<String>, OrchestratorError> {
        let promoted: BTreeMap<String, bool> = next
            .fields
            .iter()
            .filter(|(name, field)| {
                field.indexed && !held.fields.get(*name).is_some_and(|was| was.indexed)
            })
            .map(|(name, _)| (name.clone(), true))
            .collect();
        if promoted.is_empty() {
            return Ok(Vec::new());
        }
        let stores: Vec<Arc<HybridStore>> = self
            .shards
            .values()
            .filter_map(|shard| shard.store.as_ref().map(Arc::clone))
            .collect();
        Ok(self
            .fan_out_schema_update(&stores, index, &promoted)
            .await?
            .pending_reindex)
    }

    /// Phase two of `PUT /_config` on this node: see [`ClientOp::ApplySchema`].
    ///
    /// `schema` arrives with its version and owner decided by the coordinating node, and both
    /// are kept. A schema this node holds and the cluster prefers — newer, or winning the tie at
    /// the same version (`preferred_schema`) — is kept instead, and the answer says so.
    ///
    /// A change to a built column rebuilds this node's shards when they hold none of the
    /// index's documents; otherwise the declaration is stored over the built index, which is
    /// right only for a column declared ahead of it — phase one refused the rest. Each shard
    /// checks again and rebuilds in one step on its writer thread, so a write landing after
    /// phase one is counted there rather than dropped with the data; a conflicting change it
    /// meets is undone on the shards it reached and refused.
    pub(super) async fn orch_apply_schema(
        &self,
        index: &str,
        schema: IndexSchema,
        check_quota: bool,
    ) -> Result<JsonValue, OrchestratorError> {
        let mut schema = Self::checked_declaration(schema)?;
        let answer = |outcome: SchemaApplied| {
            serde_json::to_value(outcome)
                .map_err(|e| OrchestratorError::Io(std::io::Error::other(e)))
        };
        let current = self.durable_schema(index).await?;
        // A field a write taught this node after phase one read it is kept, not overwritten by a
        // schema that never heard of it; the cluster is then asked to agree on it. Merged before
        // the checks below, so a change already applied here still reads as the same one.
        if let Some(held) = current
            .as_deref()
            .filter(|current| current.state != storage::SchemaState::Dropped)
            && crate::cluster_coordinator::merge_learned(&mut schema, held)
        {
            self.request_schema_reconcile(index);
        }
        if let Some(current) = current.as_deref() {
            let same = current.version == schema.version
                && current.calculate_fingerprint() == schema.calculate_fingerprint();
            if same {
                // A retry of a change already applied here.
                return answer(SchemaApplied {
                    applied: true,
                    field_names: Self::sorted_field_names(current),
                    ..Default::default()
                });
            }
            let preferred = Self::preferred_schema(current.clone(), schema.clone());
            if preferred.calculate_fingerprint() == current.calculate_fingerprint()
                && preferred.version == current.version
            {
                return answer(SchemaApplied {
                    superseded: true,
                    ..Default::default()
                });
            }
        }
        let held = current
            .as_deref()
            .filter(|current| current.state != storage::SchemaState::Dropped);
        // The explicit mint counts against `max_indexes` exactly as the implicit one does. Both
        // run on this mailbox, so the count cannot race.
        if check_quota
            && held.is_none()
            && let Some(tenant) = schema.tenant.as_deref()
            && self.quotas.max_indexes(tenant).is_some()
        {
            let owned = owned_index_count(&self.shards, tenant).await?;
            self.quotas.check_mint(tenant, owned)?;
        }

        let stores: Vec<Arc<HybridStore>> = self
            .shards
            .values()
            .filter_map(|shard| shard.store.as_ref().map(Arc::clone))
            .collect();
        if stores.is_empty() {
            return Err(OrchestratorError::Missing(
                "No local stores available to persist schema".to_string(),
            ));
        }

        let changes = held
            .map(|current| current.rebuild_changes(&schema))
            .unwrap_or_default();
        if !changes.is_empty() {
            let conflicts: Vec<String> = changes
                .iter()
                .filter(|c| c.conflicts)
                .map(|c| c.what.clone())
                .collect();
            let documents = self.local_document_count(index).await?;
            if documents > 0 && !conflicts.is_empty() {
                return answer(SchemaApplied {
                    arrived: documents,
                    conflicts,
                    ..Default::default()
                });
            }
            if documents == 0 {
                tracing::info!(
                    index = %index,
                    changes = ?changes,
                    "Schema changes a built column of an empty index; rebuilding it"
                );
                let found = self.rebuild_shards_if_empty(index, &schema).await?;
                let arrived: u64 = found.iter().map(|(_, documents)| documents).sum();
                if arrived == 0 {
                    self.schema_cache.put(index, &schema);
                    return answer(SchemaApplied {
                        applied: true,
                        field_names: Self::sorted_field_names(&schema),
                        ..Default::default()
                    });
                }
                if !conflicts.is_empty() {
                    let rebuilt: Vec<Uuid> = found
                        .iter()
                        .filter(|(_, documents)| *documents == 0)
                        .map(|(shard_id, _)| *shard_id)
                        .collect();
                    if let Some(previous) = held {
                        self.rebuild_shards_back(index, previous, &rebuilt).await;
                    }
                    return answer(SchemaApplied {
                        arrived,
                        conflicts,
                        ..Default::default()
                    });
                }
                // Only columns declared ahead of the index: the shards holding documents take
                // the schema as a populated index does, below, and the rebuilt ones take it
                // again unchanged.
            }
        }

        // Persist schema AND pre-create Tantivy index to all stores concurrently
        // This prevents race conditions where bulk writes start before the index exists
        tracing::info!(
            index = %index,
            version = schema.version,
            num_shards = stores.len(),
            num_fields = schema.fields.len(),
            "Storing schema and pre-creating Tantivy indexes on all shards"
        );
        let handles: Vec<_> = stores
            .into_iter()
            .map(|store| {
                let idx = index.to_string();
                let sch = schema.clone();
                tokio::task::spawn_blocking(move || {
                    store.store_schema_and_cache(&idx, &sch)?;
                    drop(store.get_or_create_index(&idx)?);
                    Ok::<_, storage::StoreError>(())
                })
            })
            .collect();
        for handle in handles {
            handle
                .await
                .map_err(|e| OrchestratorError::Io(std::io::Error::other(e.to_string())))?
                .map_err(|e| OrchestratorError::Io(std::io::Error::other(e.to_string())))?;
        }
        self.schema_cache.put(index, &schema);

        answer(SchemaApplied {
            applied: true,
            field_names: Self::sorted_field_names(&schema),
            ..Default::default()
        })
    }

    /// [`MicroshardActor::rebuild_if_empty`] on every local shard at once: each shard's id and
    /// the documents it found, `0` where it rebuilt.
    async fn rebuild_shards_if_empty(
        &self,
        index: &str,
        schema: &IndexSchema,
    ) -> Result<Vec<(Uuid, u64)>, OrchestratorError> {
        let rebuilds = self
            .shards
            .iter()
            .filter(|(_, shard)| shard.store.is_some())
            .map(|(shard_id, shard)| async move {
                shard
                    .rebuild_if_empty(index, schema)
                    .await
                    .map(|documents| (*shard_id, documents))
            });
        futures::future::join_all(rebuilds)
            .await
            .into_iter()
            .collect()
    }

    /// Put `previous` back on the shards a refused change already rebuilt. A shard that has
    /// taken documents since holds them under the refused schema's columns, which nothing here
    /// can undo, so it is logged for an operator to rebuild.
    async fn rebuild_shards_back(&self, index: &str, previous: &IndexSchema, shard_ids: &[Uuid]) {
        let rollbacks = shard_ids
            .iter()
            .filter_map(|shard_id| self.shards.get(shard_id).map(|shard| (shard_id, shard)))
            .map(|(shard_id, shard)| async move {
                (*shard_id, shard.rebuild_if_empty(index, previous).await)
            });
        for (shard_id, result) in futures::future::join_all(rollbacks).await {
            match result {
                Ok(0) => {}
                Ok(documents) => tracing::error!(
                    index = %index,
                    shard_id = %shard_id,
                    documents,
                    "A refused schema change could not be undone on this shard: documents \
                     reached it under the new columns. Delete the index's documents and load \
                     again"
                ),
                Err(e) => tracing::error!(
                    index = %index,
                    shard_id = %shard_id,
                    error = %e,
                    "A refused schema change could not be undone on this shard"
                ),
            }
        }
    }

    /// What setting `field_updates` would do on every shard, merged; nothing is written.
    pub(super) async fn fan_out_schema_update(
        &self,
        stores: &[Arc<HybridStore>],
        index: &str,
        field_updates: &BTreeMap<String, bool>,
    ) -> Result<SchemaFieldUpdate, OrchestratorError> {
        let handles: Vec<_> = stores
            .iter()
            .map(|store| {
                let store = Arc::clone(store);
                let idx = index.to_string();
                let updates = field_updates.clone();
                tokio::task::spawn_blocking(move || store.plan_field_indexing(&idx, &updates))
            })
            .collect();

        let mut merged = SchemaFieldUpdate::default();
        let mut shard_count = 0usize;
        let mut unknown_counts: HashMap<String, usize> = HashMap::new();

        for handle in handles {
            let outcome = handle
                .await
                .map_err(|e| OrchestratorError::Io(std::io::Error::other(e.to_string())))?
                .map_err(OrchestratorError::Storage)?;

            shard_count += 1;
            for name in outcome.unknown {
                *unknown_counts.entry(name).or_default() += 1;
            }
            merge_names(&mut merged.applied, outcome.applied);
            merge_names(&mut merged.unchanged, outcome.unchanged);
            merge_names(&mut merged.pending_reindex, outcome.pending_reindex);
        }

        // A name is unknown only when *every* shard says so.
        //
        // Shards normally agree, and the two paths that create a schema both make sure of it: a
        // schema declared through `PUT /_config` is fanned out by `orch_create_config`, and one
        // inferred from a bulk load is typed from the whole minting batch and
        // persisted to every shard before the first write lands. Uniform input therefore gives
        // every shard the same schema.
        //
        // Divergence comes from per-document writes over semi-structured input, where a field
        // only some documents carry exists only on the shards those documents reached. That case
        // is legitimate — those shards genuinely cannot answer a query on that field — so one
        // shard's "unknown" must not refuse an edit the others can apply.
        merged.unknown = unknown_counts
            .into_iter()
            .filter(|(_, seen)| *seen == shard_count)
            .map(|(name, _)| name)
            .collect();
        merged.unknown.sort();

        // A field one shard applied and another reported unchanged is applied overall; saying
        // both would read as a contradiction.
        merged
            .unchanged
            .retain(|name| !merged.applied.contains(name));

        Ok(merged)
    }

    /// The body describing a schema update, whether it was accepted or refused.
    pub(crate) fn schema_update_response(index: &str, outcome: &SchemaFieldUpdate) -> JsonValue {
        let mut body = serde_json::json!({
            "acknowledged": !outcome.is_rejected(),
            "index": index,
            "updated_fields": outcome.applied,
            "unchanged_fields": outcome.unchanged,
        });

        if outcome.is_rejected() {
            body["unknown_fields"] = serde_json::json!(outcome.unknown);
            body["reason"] = serde_json::json!(describe_schema_refusal(outcome));
        } else if !outcome.pending_reindex.is_empty() {
            // Applied, saved, and not yet searchable. Reported rather than refused: declaring the
            // field is the first step of the rebuild that makes it searchable.
            body["pending_reindex_fields"] = serde_json::json!(outcome.pending_reindex);
            body["note"] = serde_json::json!(describe_pending_reindex(outcome));
        }

        body
    }

    pub(super) async fn orch_get_config(
        &self,
        index: &str,
    ) -> Result<JsonValue, OrchestratorError> {
        // IMPORTANT: Always get fresh schema from storage layer
        // The storage layer maintains the authoritative schema derived from Tantivy
        // This prevents orchestrator cache staleness issues

        // If no shards are initialized yet, return a helpful error
        if self.shards.is_empty() {
            return Err(OrchestratorError::NotReady(format!(
                "No shards initialized on this node. Schema for index '{}' may exist but cannot be retrieved until shards are created.",
                index
            )));
        }

        tracing::debug!(
            index = %index,
            num_shards = self.shards.len(),
            "Attempting to retrieve schema from shards"
        );

        for (shard_id, shard) in &self.shards {
            if let Some(store) = &shard.store {
                // A dropped index's record is not a schema. It is kept only so a write still
                // carrying the dropped schema cannot reinstall it; answered here, it told a
                // loader the index already had a schema, so the loader applied none and the
                // index was typed by guesswork from its documents instead.
                let schema = schema_from_store(store, index)
                    .await?
                    .filter(|s| s.state != storage::SchemaState::Dropped);
                tracing::debug!(
                    index = %index,
                    shard_id = %shard_id,
                    found = schema.is_some(),
                    "Schema retrieval attempt"
                );

                if let Some(s) = schema {
                    tracing::debug!(
                        index = %index,
                        shard_id = %shard_id,
                        num_fields = s.fields.len(),
                        "Schema found in shard"
                    );
                    let (searchable, sortable) = self.field_capabilities_across_shards(index).await;
                    return Ok(Self::schema_response(
                        index,
                        &s,
                        &searchable,
                        &sortable,
                        store.query_policy().max_default_fields,
                    ));
                }
            }
        }

        tracing::warn!(
            index = %index,
            num_shards = self.shards.len(),
            "Schema not found in any shard"
        );

        // Typed rather than an `Io` of kind `NotFound`, which is the kind this node also raises
        // for its own missing pieces — a shard it cannot find, a peer it cannot reach. The HTTP
        // layer has to answer 404 for the first and 500 for the second, and it cannot tell them
        // apart from an error kind shared by both.
        Err(OrchestratorError::Storage(StoreError::IndexNotFound(
            index.to_string(),
        )))
    }

    /// Parse a query against an index on whichever local shard can answer.
    ///
    /// Resolving a field name needs a built Tantivy index, and a shard that holds the schema but
    /// has never been written to has none — so this asks each shard in turn and takes the first
    /// real verdict. Shards share a schema, so the first answer is every shard's answer.
    pub(super) async fn orch_validate_query(
        &self,
        index: &str,
        query: &str,
    ) -> Result<JsonValue, OrchestratorError> {
        // A search strips inline modifiers before the engine sees the query (HTTP and MCP both
        // do), so validation parses what a search would run rather than treating `limit 5` as
        // two more terms. The verdict then covers the modifier-free query, which is also the
        // only form in which an exact id lookup is recognizable.
        let engine_query = parse_query_keywords(query).query;

        for shard in self.shards.values() {
            let Some(store) = &shard.store else { continue };
            let store = Arc::clone(store);
            let idx = index.to_string();
            let q = engine_query.clone();

            let outcome = tokio::task::spawn_blocking(move || store.validate_query(&idx, &q))
                .await
                .map_err(|e| OrchestratorError::Io(std::io::Error::other(e.to_string())))?
                .map_err(OrchestratorError::Storage)?;

            if let Some(outcome) = outcome {
                return Ok(serde_json::json!({
                    "index": index,
                    "query": query,
                    "valid": outcome.is_valid(),
                    "normalized_query": outcome.normalized_query,
                    "syntax_errors": outcome.syntax_errors,
                    "discarded": outcome.discarded,
                }));
            }
        }

        // Every shard holds the schema and none has a built index, or the index is unknown here.
        // Either way there is nothing to resolve field names against, and saying so beats
        // returning a verdict that was never checked.
        Ok(serde_json::json!({
            "index": index,
            "query": query,
            "valid": JsonValue::Null,
            "normalized_query": engine_query,
            "syntax_errors": [],
            "discarded": [],
            "note": "This index has no documents yet, so the query could not be checked against \
                     it. Field names are resolved against a built index, which does not exist \
                     until the first write.",
        }))
    }

    /// List all available indexes — the mailbox entry for [`list_indexes`]. On the actor it
    /// is the fallback for callers that never met the worker pool; the answer itself is
    /// computed from `&self` state, so the engine answers the same question from a snapshot
    /// without queueing behind whatever the mailbox is holding.
    pub(super) async fn orch_list_indexes(
        &self,
        include_data_size: bool,
    ) -> Result<JsonValue, OrchestratorError> {
        list_indexes(
            &self.shards,
            &self.schema_cache,
            &self.identity,
            include_data_size,
        )
        .await
    }
}

/// Local index listing: stats per index across shards, then each index's schema read once
/// for its fields. Cluster mode is handled at the RouterActor level — this is the local
/// aggregation logic its broadcast merges on top of.
///
/// Shared by `NodeOrchestrator::orch_list_indexes` and `OrchestratorEngine` — every input
/// is a borrow (`&HashMap` of shards, the cache, the identity), so a worker holding the
/// `ArcSwap` snapshots computes the identical answer.
pub(super) async fn list_indexes(
    shards: &HashMap<Uuid, MicroshardActor>,
    schema_cache: &SchemaCache,
    identity: &NodeIdentity,
    include_data_size: bool,
) -> Result<JsonValue, OrchestratorError> {
    if shards.is_empty() {
        return Ok(serde_json::json!({
            "indexes": [],
            "total_indexes": 0,
            "node_id": identity.uuid.to_string(),
            "node_name": identity.name.clone(),
            "total_shards": 0
        }));
    }

    /// Per-index totals accumulated across this node's shards.
    #[derive(Default)]
    struct IndexTotals {
        pub(super) document_count: u64,
        pub(super) redb_bytes: u64,
        pub(super) tantivy_bytes: u64,
        /// Shards that hold data for this index.
        pub(super) shard_count: usize,
        /// Of those, how many have finished warming their reader.
        pub(super) warm_shards: usize,
        /// Union of the fields the built index can actually search, across shards.
        ///
        /// A union rather than an intersection: a shard that has the column can answer a
        /// query on that field, and a scatter-gather asks every shard. Reporting the
        /// intersection would call a field unsearchable because one empty shard lacks it.
        pub(super) searchable: HashSet<String>,
        /// Union of the fields the built index can sort exactly, across shards, on the same
        /// reasoning.
        pub(super) sortable: HashSet<String>,
    }

    let mut all: HashMap<String, IndexTotals> = HashMap::new();

    // Every shard runs the node's one policy; read it once for the default-field report.
    let max_default_fields = shards
        .values()
        .find_map(|shard| shard.store.as_ref())
        .map_or(0, |store| store.query_policy().max_default_fields);

    // Create GetShardStats message
    let msg = GetShardStats { include_data_size };

    // Collect futures for all shard stats requests using actor message pattern
    let mut shard_futures = Vec::new();
    for (shard_id, shard) in shards {
        let shard_id = *shard_id;
        let shard_clone = shard.clone();
        let msg_clone = msg.clone();

        // Call handle_get_stats on each shard actor asynchronously
        let future = async move {
            let result = shard_clone.handle_get_stats(msg_clone).await;
            (shard_id, result)
        };
        shard_futures.push(future);
    }

    // Await all futures in parallel using join_all
    let results = join_all(shard_futures).await;

    let mut shard_timings: Vec<(Uuid, ShardStatsTimings)> = Vec::new();
    for (shard_id, result) in results {
        match result {
            Ok(snapshot) => {
                shard_timings.push((shard_id, snapshot.timings.clone()));

                for (index_name, stats) in snapshot.per_index {
                    let entry = all.entry(index_name).or_default();
                    entry.document_count += stats.document_count;
                    entry.redb_bytes += stats.redb_bytes;
                    entry.tantivy_bytes += stats.tantivy_bytes;

                    if stats.document_count > 0
                        || stats.redb_bytes > 0
                        || stats.tantivy_bytes > 0
                        || stats.tantivy_index_exists
                    {
                        entry.shard_count += 1;
                        if stats.warmup_state == storage::IndexWarmupState::Warm {
                            entry.warm_shards += 1;
                        }
                    }
                    for field in stats.searchable_fields {
                        entry.searchable.insert(field);
                    }
                    for field in stats.sortable_fields {
                        entry.sortable.insert(field);
                    }
                }
            }
            Err(e) => return Err(e),
        }
    }

    let mut total_redb_ms: u128 = 0;
    let mut total_tantivy_ms: u128 = 0;
    for (shard_id, timings) in shard_timings {
        debug!(
            shard = %shard_id,
            redb_ms = timings.redb_ms,
            tantivy_ms = timings.tantivy_ms,
            total_ms = timings.total_ms,
            "Collected shard index statistics"
        );

        total_redb_ms = total_redb_ms.max(timings.redb_ms);
        total_tantivy_ms = total_tantivy_ms.max(timings.tantivy_ms);
    }

    let total_ms = total_redb_ms + total_tantivy_ms;

    let mut indexes: Vec<(String, JsonValue)> = Vec::new();
    for (name, totals) in all {
        let IndexTotals {
            document_count,
            redb_bytes,
            tantivy_bytes,
            shard_count,
            warm_shards,
            searchable,
            sortable,
        } = totals;
        let mut json_obj = JsonMap::new();
        json_obj.insert("name".to_string(), JsonValue::String(name.clone()));
        json_obj.insert(
            "document_count".to_string(),
            JsonValue::from(document_count),
        );

        // Bytes rather than megabytes, everywhere. The cluster listing sums these across
        // nodes, and summing values already rounded to whole megabytes lost up to a megabyte
        // per node. A renderer that wants megabytes divides once, at the end.
        json_obj.insert(
            "index_size_bytes".to_string(),
            JsonValue::from(tantivy_bytes),
        );
        json_obj.insert(
            "memory_bytes".to_string(),
            JsonValue::from(redb_bytes + tantivy_bytes),
        );

        // The redb half is only measured when it was asked for — walking it is the expensive
        // part of the statistics call — so these are absent rather than reported as zero.
        if include_data_size {
            json_obj.insert("data_size_bytes".to_string(), JsonValue::from(redb_bytes));
            json_obj.insert(
                "total_size_bytes".to_string(),
                JsonValue::from(tantivy_bytes + redb_bytes),
            );
        }

        json_obj.insert("shard_count".to_string(), JsonValue::from(shard_count));
        // Warmup coverage on this node: how many of the shards holding this index are
        // already serving from warm readers. Below shard_count means the first query
        // routed to a cold shard still pays the open-and-fault cost.
        json_obj.insert("warm_shards".to_string(), JsonValue::from(warm_shards));
        // The schema is read here, once, and rendered in full. Every caller used to fetch it
        // again per index to learn field types — the client sequentially, the MCP tools
        // concurrently — because the listing offered names alone.
        match schema_cache.schema_for(shards, &name).await {
            Ok(schema) => {
                if let Some(description) = &schema.description {
                    json_obj.insert(
                        "description".to_string(),
                        JsonValue::String(description.clone()),
                    );
                }
                let (searched, truncated) =
                    NodeOrchestrator::searched_by_default(&schema, &searchable, max_default_fields);
                NodeOrchestrator::insert_default_search(
                    &mut json_obj,
                    &schema,
                    &searched,
                    truncated,
                );
                let fields =
                    NodeOrchestrator::describe_fields(&schema, &searchable, &sortable, &searched);
                json_obj.insert("field_count".to_string(), JsonValue::from(fields.len()));
                json_obj.insert("fields".to_string(), JsonValue::Array(fields));
            }
            Err(_) => {
                // An unreadable schema is reported as no fields rather than as a missing key,
                // so a consumer never has to distinguish "absent" from "empty".
                json_obj.insert("field_count".to_string(), JsonValue::from(0));
                json_obj.insert("fields".to_string(), JsonValue::Array(Vec::new()));
            }
        }

        indexes.push((name, JsonValue::Object(json_obj)));
    }

    indexes.sort_by(|a, b| a.0.cmp(&b.0));
    let indexes: Vec<JsonValue> = indexes.into_iter().map(|(_, json)| json).collect();

    Ok(serde_json::json!({
        "indexes": indexes,
        "total_indexes": indexes.len(),
        "node_id": identity.uuid.to_string(),
        "node_name": identity.name.clone(),
        "total_shards": shards.len(),
        "took_ms": total_ms,
    }))
}

impl NodeOrchestrator {
    /// Get node identity information
    pub(super) async fn orch_get_identity(&self) -> Result<JsonValue, OrchestratorError> {
        Ok(identity_json(&self.identity, self.shards.len()))
    }

    /// The schema this node holds for `index`, or `None` if it holds none —
    /// [`SchemaCache::durable`] against the actor's own shard map.
    pub(super) async fn durable_schema(
        &self,
        index: &str,
    ) -> Result<Option<Arc<IndexSchema>>, OrchestratorError> {
        self.schema_cache.durable(&self.shards, index).await
    }

    /// This node's own schema for `index`, for a peer's canvass — see [`ClientOp::GetRawSchema`].
    ///
    /// Read from durable state, not from the lazily-filled cache: this answer is what a peer
    /// uses to decide whether it may invent a schema, so "I have not looked yet" must not be
    /// reported as "there is none". See `durable_schema`.
    async fn raw_schema_for_peer(
        &mut self,
        index: String,
        minting_by: Option<Uuid>,
    ) -> Result<JsonValue, OrchestratorError> {
        // A dropped index's record is sent as it is, and the asker reads it as no schema (see
        // `held_schema`) — but learns its version, which a mint has to go above. Minting here
        // answers first: a record is not a reason to let a second node mint.
        let durable = self.durable_schema(&index).await?;
        let dropped = durable
            .as_ref()
            .filter(|schema| schema.state == storage::SchemaState::Dropped)
            .cloned();
        let held = durable.filter(|schema| schema.state != storage::SchemaState::Dropped);
        match held {
            Some(schema) => serde_json::to_value(&*schema)
                .map_err(|e| OrchestratorError::Io(std::io::Error::other(e))),
            // Minting it now, and not saved yet. "None" would be false in the way that
            // matters: the asker is canvassing to decide whether it may sample a schema of
            // its own, and two nodes that each heard "none" mint two schemas for one index.
            //
            // An asker minting the same index is recorded before answering, so that this
            // node's own decision — still to come, in `MintAfterCanvass` — knows of it, just as
            // the asker learns of this node from the answer. Both then settle it the same way.
            None if self.minting.contains_key(&index) => {
                if let Some(rival) = minting_by {
                    self.mint_rivals
                        .entry(index.clone())
                        .or_default()
                        .insert(rival);
                }
                Err(OrchestratorError::SchemaBeingMinted {
                    index,
                    node: self.identity.uuid,
                })
            }
            None => match dropped {
                Some(record) => serde_json::to_value(&*record)
                    .map_err(|e| OrchestratorError::Io(std::io::Error::other(e))),
                None => Ok(JsonValue::Null),
            },
        }
    }

    /// The index a write would have to canvass the peers for, if it would.
    ///
    /// A clustered first hop — not forwarded, carrying no schema — to an index this node holds
    /// no live schema for: exactly the writes whose staged validation would ask every peer
    /// before deciding. A forwarded share never canvasses (it asks its sender instead), and a
    /// standalone node's canvass asks nobody, so neither is worth a trip out of the mailbox.
    pub(super) async fn mint_canvass_needed(&self, op: &ClientOp) -> Option<String> {
        if !self.config.clustered {
            return None;
        }
        let index = match op {
            ClientOp::Write {
                index,
                forwarded: false,
                schema_body: None,
                ..
            }
            | ClientOp::BulkWrite {
                index,
                forwarded: false,
                schema_body: None,
                ..
            } => index,
            _ => return None,
        };
        let schema = self.load_schema(index).await.ok()?;
        (schema.fields.is_empty() || schema.state == storage::SchemaState::Dropped)
            .then(|| index.clone())
    }

    /// Helper: Load schema from first shard, empty when this node holds none —
    /// [`SchemaCache::schema_for`] against the actor's own shard map.
    pub(super) async fn load_schema(
        &self,
        index: &str,
    ) -> Result<Arc<IndexSchema>, OrchestratorError> {
        self.schema_cache.schema_for(&self.shards, index).await
    }
}

#[remote_message("cameo.orchestrator.client_op")]
impl Message<ClientOp> for NodeOrchestrator {
    /// Delegated so that an op whose answer waits — on peers, or on the peer lane — can release
    /// the mailbox while it waits. The value on the wire is the same
    /// `Result<JsonValue, OrchestratorError>` either way, so peers of any version read it.
    type Reply = DelegatedReply<Result<JsonValue, OrchestratorError>>;

    /// An op from a peer: a forwarded share, a write the router sent to its owner, the local
    /// half of a peer's search. This node's own requests arrive as [`OnActor`] instead.
    ///
    /// What the worker lane can serve runs on the peer lane, off the mailbox and several at a
    /// time. The mailbox served every one of them in turn, so a node receiving shares from two
    /// peers wrote them one after the other however many shards and cores it had; and a share
    /// that needs a schema written still comes back here, as [`OnActor`], because only the actor
    /// may write one. The peer lane is its own, separate from the worker pool: a first hop on a
    /// worker waits on a peer's lane, and the lane's work waits on no peer, so it always drains
    /// — two nodes whose workers were all waiting on each other could not otherwise serve the
    /// shares both were waiting for.
    async fn handle(&mut self, msg: ClientOp, ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        if worker_eligible(&msg)
            && let (Some(engine), Some(lane)) = (self.engine.clone(), self.peer_lane.clone())
        {
            // Full means the lane is as busy as the worker pool can be. The op is served here
            // instead, one at a time — which is what pushes back on the peers sending it, as the
            // mailbox always did — rather than parked in an unbounded pile of waiting tasks.
            if let Ok(permit) = lane.try_acquire_owned() {
                let (delegated, reply) = ctx.reply_sender();
                let orchestrator = ctx.actor_ref().downgrade();
                tokio::spawn(async move {
                    let result = match engine.execute(msg).await {
                        WorkerOutcome::Done(result) => result,
                        WorkerOutcome::UseActor(op) => {
                            drop(permit);
                            on_actor(&orchestrator, *op).await
                        }
                    };
                    if let Some(reply) = reply {
                        reply.send(result);
                    }
                });
                return delegated;
            }
            debug!("peer lane full; serving a peer's op on the mailbox");
        }
        self.run_on_actor(msg, ctx).await
    }
}

/// Run an op on the actor itself: this node's own requests that a worker handed back or could
/// not take, and anything the peer lane does not serve.
///
/// Its own message rather than a [`ClientOp`], so that an op a worker declined is never offered
/// to a worker again — `ClientOp` is the peer entry, and it sends what a worker can serve to the
/// peer lane.
pub(crate) struct OnActor(pub(crate) ClientOp);

impl Message<OnActor> for NodeOrchestrator {
    type Reply = DelegatedReply<Result<JsonValue, OrchestratorError>>;

    async fn handle(&mut self, msg: OnActor, ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        self.run_on_actor(msg.0, ctx).await
    }
}

/// Send an op back to the actor from a task, and wait for its answer.
async fn on_actor(
    orchestrator: &kameo::actor::WeakActorRef<NodeOrchestrator>,
    op: ClientOp,
) -> Result<JsonValue, OrchestratorError> {
    let Some(orchestrator) = orchestrator.upgrade() else {
        return Err(OrchestratorError::NotReady(
            "the orchestrator stopped before the op could run".to_string(),
        ));
    };
    match orchestrator.ask(OnActor(op)).await {
        Ok(result) => Ok(result),
        Err(kameo::error::SendError::HandlerError(err)) => Err(err),
        Err(other) => Err(OrchestratorError::NotReady(format!(
            "the orchestrator stopped before the op could run: {other}"
        ))),
    }
}

impl NodeOrchestrator {
    /// The actor's own handling of an op — see [`OnActor`].
    ///
    /// A first write that must canvass, and any op whose remaining work waits on a peer, is
    /// answered through the delegated reply from a task, so the mailbox moves on; everything
    /// else is answered before this returns.
    async fn run_on_actor(
        &mut self,
        msg: ClientOp,
        ctx: &mut Context<Self, DelegatedReply<Result<JsonValue, OrchestratorError>>>,
    ) -> DelegatedReply<Result<JsonValue, OrchestratorError>> {
        // Dequeue-to-answer for the mailbox lane: this actor is serialised, so nothing else is
        // running inside this span and it holds no queue wait. The gate in `RouterActor`
        // predicts against what is folded here.
        let class = OpClass::of(&msg);
        let started = Instant::now();

        if let Some(index) = self.mint_canvass_needed(&msg).await {
            let (delegated, reply) = ctx.reply_sender();
            *self.minting.entry(index.clone()).or_default() += 1;
            let canvass = self.schema_canvass();
            let me = self.identity.uuid;
            let orchestrator = ctx.actor_ref().downgrade();
            tokio::spawn(async move {
                let lookup = canvass.peer_schema_for(&index, Some(me)).await;
                let answer = match orchestrator.upgrade() {
                    Some(orchestrator) => orchestrator
                        .ask(MintAfterCanvass {
                            index,
                            op: msg,
                            lookup,
                            counted: true,
                        })
                        .await
                        .map_err(|err| {
                            OrchestratorError::NotReady(format!(
                                "the orchestrator stopped before the write could run: {err}"
                            ))
                        }),
                    None => Err(OrchestratorError::NotReady(
                        "the orchestrator stopped before the write could run".to_string(),
                    )),
                };
                // The mint's own forwards, if it had any, run here too — still off the mailbox.
                let result = match answer {
                    Ok(answer) => answer.resolve().await,
                    Err(err) => Err(err),
                };
                if let Some(reply) = reply {
                    reply.send(result);
                }
            });
            self.mailbox_lane.record_service(class, started.elapsed());
            return delegated;
        }

        let answer = match msg {
            ClientOp::DeleteIndex {
                index,
                delete_schema,
            } => Answer::Now(self.orch_delete_index(&index, delete_schema).await),
            other => self.handle_client_op(other).await,
        };
        self.mailbox_lane.record_service(class, started.elapsed());
        match answer {
            Answer::Now(result) => ctx.reply(result),
            // The rest waits on a peer, so it runs in a task and this mailbox moves on.
            Answer::Later(rest) => {
                let (delegated, reply) = ctx.reply_sender();
                tokio::spawn(async move {
                    let result = rest.await;
                    if let Some(reply) = reply {
                        reply.send(result);
                    }
                });
                delegated
            }
        }
    }
}

/// A first write to a new index, back in the mailbox with its canvass answered.
///
/// The canvass asks every peer, and each peer answers through its own orchestrator mailbox.
/// Run inside this mailbox it held it for as long as the peers took, and a peer canvassing this
/// node at the same moment — minting another index — waited on it in turn, until both timed
/// out and refused: new indexes created through every node at once were measured at 0.1/s,
/// most refused. So the canvass runs in a task, and only deciding and saving the schema — the
/// part that needs this actor — comes back here, in order with everything else.
///
/// Local only: sent by this node to itself, never over the wire.
pub(super) struct MintAfterCanvass {
    index: String,
    op: ClientOp,
    lookup: PeerSchemaLookup,
    /// Whether this write holds a mark in `minting`, to be released when it has run. A write
    /// that yielded and comes back to adopt the winner's schema holds none.
    counted: bool,
}

/// How long a node that lost a race to mint an index waits for the winner's schema to appear.
///
/// The winner is mid-canvass or about to save, which takes milliseconds; the bound is for a
/// winner that fails instead — its canvass could not reach a peer, say — after which the write
/// is refused with a retryable `503` and its retry mints or adopts afresh.
const MINT_YIELD_WAIT: Duration = Duration::from_secs(5);

/// The rival this node yields to, if it yields at all: the lowest node id among the rivals,
/// when that is lower than this node's own.
///
/// Every node in a race runs this over the same set — each knows of the others, from their
/// answers to its canvass or from their questions during it — so all of them name the same
/// winner, and that winner names none. Nothing to exchange and nobody to ask: the verdict is a
/// pure function of the ids.
pub(super) fn mint_winner(me: Uuid, rivals: impl IntoIterator<Item = Uuid>) -> Option<Uuid> {
    rivals.into_iter().filter(|rival| *rival < me).min()
}

/// A write that lost a race to mint its index: wait, off the mailbox, for the winner's schema,
/// then run the write against it.
async fn adopt_when_minted(
    canvass: SchemaCanvass,
    orchestrator: kameo::actor::WeakActorRef<NodeOrchestrator>,
    index: String,
    op: ClientOp,
    winner: Uuid,
) -> Result<JsonValue, OrchestratorError> {
    let deadline = Instant::now() + MINT_YIELD_WAIT;
    let mut pause = Duration::from_millis(25);
    loop {
        tokio::time::sleep(pause).await;
        pause = (pause * 2).min(Duration::from_millis(400));
        // Asked as a lookup, not a mint: this node no longer competes, and the winner answers
        // "minting" until it has saved and then answers with the schema.
        if let PeerSchemaLookup::Found(schema) = canvass.peer_schema_for(&index, None).await {
            let orchestrator = orchestrator.upgrade().ok_or_else(|| {
                OrchestratorError::NotReady(
                    "the orchestrator stopped before the write could run".to_string(),
                )
            })?;
            let answer = orchestrator
                .ask(MintAfterCanvass {
                    index,
                    op,
                    lookup: PeerSchemaLookup::Found(schema),
                    counted: false,
                })
                .await
                .map_err(|err| {
                    OrchestratorError::NotReady(format!(
                        "the orchestrator stopped before the write could run: {err}"
                    ))
                })?;
            return answer.resolve().await;
        }
        if Instant::now() >= deadline {
            return Err(OrchestratorError::SchemaUnconfirmed {
                index,
                reason: format!(
                    "node {winner} was creating this index and had not saved it after {}s",
                    MINT_YIELD_WAIT.as_secs()
                ),
            });
        }
    }
}

impl Message<MintAfterCanvass> for NodeOrchestrator {
    /// The write's answer, or its forwards still to run — which the task that sent this runs,
    /// so they do not hold the mailbox either.
    type Reply = Answer;

    async fn handle(
        &mut self,
        msg: MintAfterCanvass,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let MintAfterCanvass {
            index: minted,
            op,
            lookup,
            counted,
        } = msg;
        let class = OpClass::of(&op);
        let started = Instant::now();

        // The schema may have been saved while the canvass ran — by another write to the same
        // index here, or adopted from a forwarded share. Staged validation reads it again and
        // then treats this write as an addition rather than a mint, and the canvass goes unused.
        let held = self
            .load_schema(&minted)
            .await
            .map(|schema| {
                !schema.fields.is_empty() && schema.state != storage::SchemaState::Dropped
            })
            .unwrap_or(false);

        // Settle a race. The rivals are the peers that answered this canvass "minting" and the
        // peers that asked this node while it was minting; each of them knows of this node the
        // same way, so all of them reach the same verdict. Deciding and saving happen within
        // this one message, so no peer can ask in between and hear anything but "minting" or
        // the saved schema.
        let lookup = match lookup {
            PeerSchemaLookup::NoneHeld { .. } | PeerSchemaLookup::Contested { .. }
                if counted && !held =>
            {
                let dropped_at = match &lookup {
                    PeerSchemaLookup::NoneHeld { dropped_at } => *dropped_at,
                    _ => 0,
                };
                let heard = match &lookup {
                    PeerSchemaLookup::Contested { rivals } => rivals.clone(),
                    _ => Vec::new(),
                };
                let asked = self.mint_rivals.get(&minted).cloned().unwrap_or_default();
                match mint_winner(self.identity.uuid, heard.into_iter().chain(asked)) {
                    None => PeerSchemaLookup::NoneHeld { dropped_at },
                    Some(winner) => {
                        info!(
                            index = %minted,
                            %winner,
                            "Another node is creating this index; adopting its schema instead"
                        );
                        let rest = adopt_when_minted(
                            self.schema_canvass(),
                            ctx.actor_ref().downgrade(),
                            minted.clone(),
                            op,
                            winner,
                        );
                        self.release_mint(&minted, counted);
                        self.mailbox_lane.record_service(class, started.elapsed());
                        return Answer::Later(Box::pin(rest));
                    }
                }
            }
            other => other,
        };

        let result = match op {
            ClientOp::Write {
                index,
                id,
                routing_key,
                doc,
                forwarded,
                schema_body,
                tenant,
            } => self
                .orch_write(
                    &index,
                    id,
                    routing_key,
                    doc,
                    forwarded,
                    schema_body,
                    tenant.as_deref(),
                    Some(lookup),
                )
                .await
                .into(),
            ClientOp::BulkWrite {
                index,
                docs,
                forwarded,
                schema_body,
                tenant,
            } => self
                .orch_bulk_write(
                    &index,
                    docs,
                    forwarded,
                    schema_body,
                    tenant.as_deref(),
                    Some(lookup),
                )
                .await
                .into(),
            // `mint_canvass_needed` sends only the two ops above; anything else runs as usual.
            other => self.handle_client_op(other).await,
        };
        self.release_mint(&minted, counted);
        self.mailbox_lane.record_service(class, started.elapsed());
        result
    }
}

impl NodeOrchestrator {
    /// Release one write's mark on an index it was minting, and forget the index's rivals once
    /// no write here is minting it any more.
    fn release_mint(&mut self, index: &str, counted: bool) {
        if !counted {
            return;
        }
        if let Some(count) = self.minting.get_mut(index) {
            *count -= 1;
            if *count == 0 {
                self.minting.remove(index);
                self.mint_rivals.remove(index);
            }
        }
    }
}

impl Message<UpdateTopology> for NodeOrchestrator {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: UpdateTopology,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        // `len()` is the number of vnode tokens, not of nodes — each node contributes many.
        // It was reported as `ring_nodes`, which reads as a cluster size and is not one.
        info!(
            ring_tokens = msg.ring.len(),
            "NodeOrchestrator: received global topology update"
        );
        self.routing_ring = msg.ring;
        self.publish_engine_state();
    }
}

impl Message<ShutdownAllShards> for NodeOrchestrator {
    type Reply = Result<(), OrchestratorError>;

    async fn handle(
        &mut self,
        _msg: ShutdownAllShards,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.shutdown_all_shards().await
    }
}

impl Message<ShutdownReadRuntime> for NodeOrchestrator {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: ShutdownReadRuntime,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let Some(read_runtime) = self.read_runtime.take() else {
            return;
        };

        // Only this actor holds the `Arc` — the shards and the worker pool were given
        // `Handle` clones, which do not own the runtime. A surviving reference therefore
        // means something outlived the shards, and there is nobody left to wait on it.
        let runtime = match Arc::try_unwrap(read_runtime) {
            Ok(runtime) => runtime,
            Err(arc) => {
                warn!(
                    strong_count = Arc::strong_count(&arc),
                    "Read runtime still referenced; abandoning its threads instead of waiting"
                );
                return;
            }
        };

        info!("NodeOrchestrator: Shutting down dedicated read runtime");
        let started = std::time::Instant::now();

        // `shutdown_timeout` parks the calling thread until the reads finish, which is not
        // allowed on a runtime worker — hence `spawn_blocking`. Its own timeout is the bound;
        // the join below only reports what it did.
        let timeout = msg.timeout;
        let joined = tokio::task::spawn_blocking(move || runtime.shutdown_timeout(timeout)).await;

        match joined {
            Ok(()) => info!(
                elapsed_ms = started.elapsed().as_millis(),
                "Read runtime shut down"
            ),
            Err(e) => warn!(error = %e, "Read runtime shutdown task failed"),
        }
    }
}

// Fallback for the paths that never send `ShutdownReadRuntime` — a panic, or an emergency
// exit. A graceful shutdown takes it first, leaving nothing here to do.
//
// `Drop` cannot wait: it may run on a runtime worker, where blocking is not allowed, so this
// can only detach the threads. That is why the waiting version is a message.
impl Drop for NodeOrchestrator {
    fn drop(&mut self) {
        if let Some(read_runtime) = self.read_runtime.take() {
            tracing::info!("NodeOrchestrator: Shutting down dedicated read runtime (unwaited)");
            // Try to unwrap the Arc to get exclusive ownership.
            // If there are other Arc clones (held by shards), we can't force shutdown.
            // In that case, the runtime will be cleaned up when the last Arc is dropped.
            match Arc::try_unwrap(read_runtime) {
                Ok(runtime) => {
                    // We have exclusive ownership - shut down the runtime
                    runtime.shutdown_background();
                    tracing::debug!("NodeOrchestrator: Read runtime shutdown initiated");
                }
                Err(arc) => {
                    // Other references exist (shards still hold handles)
                    let strong_count = Arc::strong_count(&arc);
                    tracing::warn!(
                        strong_count = strong_count,
                        "NodeOrchestrator: Cannot shutdown read runtime - {} other references exist",
                        strong_count
                    );
                    // The runtime will be cleaned up when the last Arc is dropped
                }
            }
        }
    }
}
