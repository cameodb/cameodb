//! MicroshardActor and everything its storage is served by: the writer thread and its
//! monitor, warmup, writer liveness and the read pool.

use super::*;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering as AtomicOrdering},
};
use std::time::{Duration, Instant};

use anyhow::Result;
use arc_swap::ArcSwapOption;
use kameo::message::{Context, Message};
use kameo::{Actor, RemoteActor, remote_message};
use tokio::sync::{RwLock as AsyncRwLock, mpsc};
use tracing::{debug, info, warn};
use uuid::Uuid;

// Re-export SortSpec and SortOrder from storage crate
use serde_json::Value as JsonValue;
use storage::{HybridStore, IndexSchema, StorageConfig, StoreError, WalOp};

/// Type alias for single write commands enqueued in the writer thread
pub(super) type WriteCommand = (WalOp, tokio::sync::oneshot::Sender<Result<u64, StoreError>>);

/// Type alias for batch write commands enqueued in the writer thread
pub(super) type BatchCommand = (
    Vec<WalOp>,
    tokio::sync::oneshot::Sender<Result<Vec<u64>, StoreError>>,
);

/// Where one caller's slice of a merged write goes back to.
///
/// A single write and a batch write to the same index are applied as one transaction, so the
/// replies have to be split by shape as well as by position: a single write is owed its one
/// sequence id, a batch is owed its own run of them.
pub(super) enum MergedWriteReply {
    Single(tokio::sync::oneshot::Sender<Result<u64, StoreError>>),
    Batch(tokio::sync::oneshot::Sender<Result<Vec<u64>, StoreError>>),
}

/// The contiguous run of sequence ids each caller in a merged write owns.
///
/// Separated out because it is the part that fails silently: a mis-walked offset hands one
/// caller another caller's sequence ids and nothing anywhere would notice. `None` when the
/// storage layer did not return one id per op, which the caller turns into an error for
/// everyone in the merge rather than an index out of bounds on the writer thread.
pub(super) fn merged_reply_ranges(
    op_counts: &[usize],
    seq_id_count: usize,
) -> Option<Vec<std::ops::Range<usize>>> {
    let mut ranges = Vec::with_capacity(op_counts.len());
    let mut offset = 0usize;
    for count in op_counts {
        let end = offset.checked_add(*count)?;
        ranges.push(offset..end);
        offset = end;
    }
    (offset == seq_id_count).then_some(ranges)
}

/// Type alias for index deletions enqueued in the writer thread: (index, delete_schema, reply)
pub(super) type DeleteCommand = (
    String,
    bool,
    tokio::sync::oneshot::Sender<Result<(), StoreError>>,
);

/// Type alias for rebuilds enqueued in the writer thread: (index, schema, reply)
pub(super) type RebuildCommand = (
    String,
    Box<IndexSchema>,
    tokio::sync::oneshot::Sender<Result<u64, StoreError>>,
);

/// Type alias for tracking reply slices when coalescing batch writes
pub(super) type BatchReplySegment = (
    usize,
    tokio::sync::oneshot::Sender<Result<Vec<u64>, StoreError>>,
);

/// Helper struct for aggregating index statistics across cluster nodes.
#[derive(Debug, Clone)]
pub(super) struct IndexStats {
    pub(super) name: String,
    pub(super) description: Option<String>,
    pub(super) document_count: u64,
    pub(super) index_size_bytes: u64,
    pub(super) memory_bytes: u64,
    pub(super) data_size_bytes: u64,
    pub(super) total_size_bytes: u64,
    pub(super) shard_count: usize,
    pub(super) warm_shards: usize,
    /// Field descriptions merged by name across nodes.
    ///
    /// A field is `searchable` in the cluster when any node can search it, for the same reason
    /// the per-node union is a union: a scatter-gather asks every node, so one node holding the
    /// column is enough to answer.
    pub(super) fields: BTreeMap<String, JsonValue>,
}

/// Microshard actor that manages a single shard's storage and search operations.
#[derive(Clone, Actor, RemoteActor)]
/// `pub(crate)`: `crate::admin::memory` implements the admin-memory messages for
/// `NodeOrchestrator` and walks its shard map, so the element type crosses with the field.
pub(crate) struct MicroshardActor {
    pub(super) shard_id: Uuid,
    pub(super) store: Option<Arc<HybridStore>>,
    /// Shared slot holding the current writer thread's command sender. A shared slot rather than a
    /// plain field so a monitor can swap in a replacement writer's channel after a crash and every
    /// clone of this actor — the engine holds cloned snapshots — sees it at once.
    pub(super) writer_tx: Arc<ArcSwapOption<mpsc::Sender<StorageCommand>>>,
    /// The writer monitor thread's handle. The monitor owns the writer thread's own handle and
    /// respawns it on a crash; shutdown joins the monitor.
    pub(super) writer_monitor_handle: Arc<std::sync::Mutex<Option<std::thread::JoinHandle<()>>>>,
    /// Set before a requested shutdown so the monitor stops instead of respawning the writer.
    pub(super) shutting_down: Arc<AtomicBool>,
    pub(super) storage_config: StorageConfig,
    pub(super) default_search_limit: usize,
    /// Active supervision tasks per index (idle-timeout commits).
    pub(super) supervisors: Arc<AsyncRwLock<HashMap<String, mpsc::Sender<()>>>>,
    /// Notified when writer thread has stopped.
    pub(super) shutdown_notify: Arc<tokio::sync::Notify>,
    /// Read thread pool handle for isolated search/stats operations.
    pub(super) read_pool_handle: Option<tokio::runtime::Handle>,
    /// Node-wide read-pool health each read on this shard brackets, so health sees saturation and
    /// a wedge. `None` when there is no dedicated pool (tests, and the generic-pool fallback).
    pub(super) read_pool_health: Option<Arc<ReadPoolHealth>>,
    /// How long a read may sit in the pool queue before it is refused instead of run. `None`
    /// disables the check. See [`dispatch_read_pool`].
    pub(super) read_budget: Option<Duration>,
    /// Total shards on this node (for per-shard memory budgeting).
    pub(super) total_shards: usize,
    /// Writer thread shutdown timeout in seconds.
    pub(super) writer_shutdown_timeout_secs: u64,
    /// Seconds of write inactivity before this shard's supervisor commits an index.
    pub(super) supervisor_timeout_secs: u64,
    /// Where this shard's writer thread should pin, resolved from the shard's ordinal by the
    /// orchestrator, plus the cell the thread reports its actual core through. Resolving the
    /// target upstream is what keeps a writer on the same core as the worker that feeds it:
    /// both come from one ordinal and one layout.
    pub(super) writer_pin: WriterPin,
    /// Node-wide writer liveness this shard's writer thread marks if it stops serving.
    pub(super) writer_liveness: Arc<WriterLiveness>,
}

impl std::fmt::Debug for MicroshardActor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MicroshardActor")
            .field("shard_id", &self.shard_id)
            .field("store_initialized", &self.store.is_some())
            .field("writer_initialized", &self.writer_tx.load().is_some())
            .field("storage_config", &self.storage_config)
            .finish()
    }
}

/// What a shard needs to know about the node it is part of.
///
/// Grouped rather than passed as five more positional arguments: they are all decided by the
/// orchestrator at spawn time, they travel together, and at the call site
/// `ShardRuntime { supervisor_timeout_secs, .. }` says what it is where a bare `5` would not.
#[derive(Clone, Debug)]
pub(super) struct ShardRuntime {
    /// Hits returned when a query names no limit.
    pub(super) default_search_limit: usize,
    /// The shared read pool. `None` falls back to tokio's generic blocking pool.
    pub(super) read_pool_handle: Option<tokio::runtime::Handle>,
    /// Node-wide read-pool health each read brackets. Paired with `read_pool_handle`: `Some` for
    /// the dedicated pool, `None` for the generic-pool fallback.
    pub(super) read_pool_health: Option<Arc<ReadPoolHealth>>,
    /// How long a read may wait for a pool thread before it is refused rather than run — the
    /// node's request timeout. `None` runs every queued read however stale.
    pub(super) read_budget: Option<Duration>,
    /// Shards on this node, for per-shard memory budgeting.
    pub(super) total_shards: usize,
    /// How long to let the writer thread drain on shutdown.
    pub(super) writer_shutdown_timeout_secs: u64,
    /// Write inactivity before an index is committed anyway.
    pub(super) supervisor_timeout_secs: u64,
    /// Where the writer thread pins, and where it reports what happened.
    pub(super) writer_pin: WriterPin,
    /// Node-wide writer liveness the writer thread marks if it stops serving.
    pub(super) writer_liveness: Arc<WriterLiveness>,
}

/// A writer that has been mid-batch longer than this is treated as wedged. A single coalesced
/// write batch that genuinely runs this long is already pathological, so the bound is loose
/// enough that a busy-but-progressing writer never trips it, and tight enough that a truly stuck
/// shard surfaces within a health check or two rather than never.
pub(super) const WRITER_STALL_THRESHOLD: Duration = Duration::from_secs(60);

/// Node-wide writer health, read by the anonymous health branch as a bounded, non-blocking probe.
///
/// A shard's writer thread is the sole path for every write to that shard, and two failure shapes
/// leave it unable to serve writes while nothing on the request path notices:
///
/// - It *exits* abnormally — a panic past the per-command guard, or its channel closing
///   unexpectedly. `down` is the notice: the thread bumps it once as it stops.
/// - It is *alive but wedged* — blocked inside a redb or tantivy call and no longer draining its
///   channel. `down` can never catch this, because the thread never returns. Each writer instead
///   publishes a heartbeat: the tick at which it began its current batch, or 0 while it idles on
///   `blocking_recv`. A heartbeat non-zero for longer than [`WRITER_STALL_THRESHOLD`] is a writer
///   stuck mid-op.
///
/// Health folds both into one count via [`unavailable_writers`](Self::unavailable_writers): a
/// single atomic load plus a scan of a handful more, off the work path, so a node whose data path
/// has stalled or died stops reporting green. Ticks are milliseconds from a monotonic origin
/// shared by every writer and the reader, so an NTP step cannot fake or mask a stall.
#[derive(Debug)]
pub(crate) struct WriterLiveness {
    pub(super) down: AtomicUsize,
    pub(super) heartbeats: Arc<std::sync::Mutex<Vec<Option<Arc<AtomicU64>>>>>,
    pub(super) origin: Instant,
}

/// A heartbeat entry returned by [`WriterLiveness::register_writer`]. The guard derefs to the
/// underlying [`AtomicU64`] so the writer thread can stamp it, and removes the entry from the
/// registry when it is dropped — so a crashed or cleanly exited writer does not leave a dead
/// heartbeat behind for future health scans.
#[derive(Debug)]
pub(super) struct WriterHeartbeat {
    pub(super) index: usize,
    pub(super) registry: Arc<std::sync::Mutex<Vec<Option<Arc<AtomicU64>>>>>,
    pub(super) heartbeat: Arc<AtomicU64>,
}

impl std::ops::Deref for WriterHeartbeat {
    type Target = AtomicU64;

    fn deref(&self) -> &Self::Target {
        &self.heartbeat
    }
}

impl Drop for WriterHeartbeat {
    fn drop(&mut self) {
        let mut guard = self.registry.lock().unwrap_or_else(|p| p.into_inner());
        if self.index < guard.len() {
            guard[self.index] = None;
        }
    }
}

impl Default for WriterLiveness {
    fn default() -> Self {
        Self {
            down: AtomicUsize::new(0),
            heartbeats: Arc::new(std::sync::Mutex::new(Vec::new())),
            origin: Instant::now(),
        }
    }
}

impl WriterLiveness {
    /// Milliseconds since this liveness was created. The clock every heartbeat is stamped with and
    /// compared against — monotonic, so it never runs backwards under a wall-clock adjustment.
    pub(super) fn now_ticks(&self) -> u64 {
        self.origin.elapsed().as_millis() as u64
    }

    /// Register a writer thread and hand back its heartbeat. The thread stamps it with
    /// [`now_ticks`](Self::now_ticks) as it starts a batch and resets it to 0 when it goes back to
    /// waiting; a stalled writer is one whose stamp stops advancing while non-zero.
    pub(super) fn register_writer(&self) -> WriterHeartbeat {
        let heartbeat = Arc::new(AtomicU64::new(0));
        let mut guard = self.heartbeats.lock().unwrap_or_else(|p| p.into_inner());
        let index = guard.len();
        guard.push(Some(Arc::clone(&heartbeat)));
        WriterHeartbeat {
            index,
            registry: Arc::clone(&self.heartbeats),
            heartbeat,
        }
    }

    /// Record that a writer thread has stopped serving. Called once, as the thread exits.
    pub(super) fn mark_writer_down(&self) {
        self.down.fetch_add(1, AtomicOrdering::Relaxed);
    }

    /// Record that a replacement writer thread is serving again, undoing one earlier
    /// `mark_writer_down`. Called once, after a monitor relaunches a crashed writer. Saturating at
    /// zero so it can never wrap the count below the number of writers actually down.
    pub(super) fn mark_writer_up(&self) {
        let _ = self
            .down
            .fetch_update(AtomicOrdering::Relaxed, AtomicOrdering::Relaxed, |n| {
                Some(n.saturating_sub(1))
            });
    }

    /// How many registered writers have been mid-batch longer than `threshold_ms` as of `now_ms`.
    /// A writer that idles on `blocking_recv` holds a 0 stamp and is never counted.
    pub(super) fn stalled_count(&self, now_ms: u64, threshold_ms: u64) -> usize {
        self.heartbeats
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .flatten()
            .filter(|heartbeat| {
                let since = heartbeat.load(AtomicOrdering::Relaxed);
                since != 0 && now_ms.saturating_sub(since) >= threshold_ms
            })
            .count()
    }

    /// Writers that cannot currently take writes: exited abnormally, or wedged mid-batch past
    /// [`WRITER_STALL_THRESHOLD`]. Self-contained — reads its own clock and threshold — so the
    /// health branch folds the whole data-path verdict from one call.
    pub(crate) fn unavailable_writers(&self) -> usize {
        self.unavailable_at(self.now_ticks())
    }

    /// The verdict [`unavailable_writers`](Self::unavailable_writers) computes, against a supplied
    /// clock so a test can place a stall without waiting out the real threshold.
    pub(super) fn unavailable_at(&self, now_ms: u64) -> usize {
        self.down.load(AtomicOrdering::Relaxed)
            + self.stalled_count(now_ms, WRITER_STALL_THRESHOLD.as_millis() as u64)
    }
}

/// Apply one writer operation, turning a panic inside it into an error for this index's callers
/// instead of an unwind that ends the writer thread.
///
/// A search can be retried as it was; a panicked write cannot trust what it touched. A panic in
/// tantivy or redb leaves the index's `IndexWriter` in an unknown state and poisons its mutex,
/// so the only safe response is to drop it — the next write rebuilds it, exactly as an eviction
/// does — and fail this one operation. The writer thread goes on serving every other index, so a
/// document that panics the parser costs one request rather than the shard.
/// Compile-time panic seams, absent unless the `fault-injection` feature is on — a shipped binary
/// carries none of them. Each lets the panic-isolation smoke test drive a real panic into one
/// hardened surface, over HTTP against the built binary, to prove the release profile unwinds and
/// the boundary holds where in-process tests (which always unwind) cannot reach the profile.
#[cfg(feature = "fault-injection")]
mod fault_injection {
    /// A search whose query is exactly this panics on the read pool.
    pub(super) const READ_TRAP_QUERY: &str = "__fault_panic_read__";
    /// A write to this index panics inside the per-command guard: caught, the writer rebuilt.
    pub(super) const WRITE_OP_TRAP_INDEX: &str = "fault_panic_write_op__";
    /// A write to this index panics past the guard, so the writer thread itself dies.
    pub(super) const WRITER_THREAD_TRAP_INDEX: &str = "fault_kill_writer__";

    pub(super) fn panic_if_read_trap(query: &str) {
        assert!(
            query != READ_TRAP_QUERY,
            "fault-injection: read on the read pool"
        );
    }

    pub(super) fn panic_if_write_op_trap(index: &str) {
        assert!(
            index != WRITE_OP_TRAP_INDEX,
            "fault-injection: write inside the per-command guard"
        );
    }

    pub(super) fn panic_if_writer_thread_trap(index: &str) {
        assert!(
            index != WRITER_THREAD_TRAP_INDEX,
            "fault-injection: writer thread past its guard"
        );
    }

    /// Hold a crashed writer's monitor before it rebuilds, for the milliseconds named by
    /// `CAMEODB_FAULT_RESPAWN_DELAY_MS`. The respawn is otherwise immediate, so the window in
    /// which a shard has no writer — the one `mark_writer_down` exists to make visible — closes
    /// faster than a health poll can see it. Widening it here lets the smoke test assert the red
    /// that a dead writer must report, instead of racing it. Unset (the default) holds not at all,
    /// so every other run keeps the immediate respawn.
    pub(super) fn hold_before_respawn() {
        let held = std::env::var("CAMEODB_FAULT_RESPAWN_DELAY_MS")
            .ok()
            .and_then(|ms| ms.parse::<u64>().ok())
            .unwrap_or(0);
        if held > 0 {
            std::thread::sleep(std::time::Duration::from_millis(held));
        }
    }
}

pub(super) fn guard_writer_op<T>(
    store: &HybridStore,
    index: &str,
    op: impl FnOnce() -> Result<T, StoreError>,
) -> Result<T, StoreError> {
    let guarded = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        #[cfg(feature = "fault-injection")]
        fault_injection::panic_if_write_op_trap(index);
        op()
    }));
    match guarded {
        Ok(result) => result,
        Err(_) => {
            tracing::error!(
                index = %index,
                "writer operation panicked; resetting the index writer so the next write rebuilds it"
            );
            store.force_remove_writer(index);
            Err(StoreError::WriterPanicked(index.to_string()))
        }
    }
}

/// A read pool that has filled every blocking thread and completed nothing for longer than this
/// is treated as wedged — threads stuck in a tantivy or redb call that will not return, which no
/// per-read panic guard can catch because the threads never unwind. The bound sits well past any
/// healthy search, so a pool merely busy-but-draining — completions still landing — never trips
/// it; only a pool that has stopped making progress does.
pub(super) const READ_POOL_WEDGE_THRESHOLD: Duration = Duration::from_secs(60);

/// Node-wide read-pool health, read by the health endpoint the same bounded, non-blocking way as
/// [`WriterLiveness`].
///
/// Every read runs a blocking closure on the shared pool through [`dispatch_read_pool`], which
/// brackets it: `in_flight` counts closures currently executing — bounded by the pool's blocking
/// width — and `last_progress` is the tick of the most recent bracket edge, a read starting or
/// finishing. Health reads the pair. Two shapes it distinguishes:
///
/// - *Saturation* — `in_flight` at `capacity` — is load, not a fault: reported as a gauge, it
///   never colours the status, because a pool draining a burst is doing its job.
/// - *Wedge* — saturation whose `last_progress` has not advanced for
///   [`READ_POOL_WEDGE_THRESHOLD`] — is every thread stuck with nothing starting or ending, and
///   turns the node red because it can no longer answer reads.
///
/// **Both edges are stamped, and the start edge is load-bearing.** Stamping only completions
/// looks stricter and is in fact wrong: `last_progress` would then be the age of the last
/// *finished* read, which on a quiet node is however long ago that was. A burst arriving after a
/// minute of no reads would saturate the pool and be read as wedged on the spot — a healthy node
/// turned red by its first traffic in a while, for as long as it took the first read to land.
/// `read_threads` is `max(2, cores / 2)`, so on a small node one federated search fanning out is
/// enough to saturate it.
///
/// The start edge cannot hide a real wedge, which is the reason it is safe to trust. [`track`]
/// runs *inside* the closure, on the pool thread, once the closure has begun executing — and
/// `capacity` is the pool's `max_blocking_threads`. So a start edge can only land while a thread
/// is free, and a pool whose every thread is stuck starts nothing: no edge, `last_progress`
/// frozen, red at the threshold. Away from that, a start edge also implies an earlier completion,
/// since a thread only becomes free by finishing — which is why this matters at exactly one point
/// in a pool's life, the first reads after it has been idle.
///
/// [`track`]: ReadPoolHealth::track
#[derive(Debug)]
pub(crate) struct ReadPoolHealth {
    pub(super) in_flight: AtomicUsize,
    pub(super) last_progress: AtomicU64,
    pub(super) capacity: usize,
    pub(super) origin: Instant,
    /// Reads dropped at dequeue because they had outlived the request that asked for them.
    ///
    /// Counted because the shed is otherwise invisible: the work never runs, so it appears in
    /// no latency sample and no worker tally, and a node shedding hard would look identical to
    /// one that is merely idle. A rising count is the node declining work it could not have
    /// delivered — the signal that load exceeds capacity, and the number to read before
    /// concluding a node is healthy because its latencies look fine.
    pub(super) abandoned: AtomicU64,
}

impl ReadPoolHealth {
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            in_flight: AtomicUsize::new(0),
            last_progress: AtomicU64::new(0),
            capacity: capacity.max(1),
            origin: Instant::now(),
            abandoned: AtomicU64::new(0),
        }
    }

    /// Record a read refused at dequeue. See [`Self::abandoned`].
    pub(super) fn record_abandoned(&self) {
        self.abandoned.fetch_add(1, AtomicOrdering::Relaxed);
    }

    /// Reads refused at dequeue since this node started.
    pub(crate) fn abandoned(&self) -> u64 {
        self.abandoned.load(AtomicOrdering::Relaxed)
    }

    pub(super) fn now_ticks(&self) -> u64 {
        self.origin.elapsed().as_millis() as u64
    }

    /// Mark a read as it begins executing on a pool thread; the returned guard marks it done when
    /// dropped — including as a panicking read unwinds — so both bracket edges always land.
    pub(super) fn track(self: &Arc<Self>) -> ReadInFlight {
        self.track_at(self.now_ticks())
    }

    /// [`track`](Self::track) against a supplied clock, so a test can place a read's start
    /// somewhere other than a few microseconds after the pool was built — the same split as
    /// [`is_wedged`](Self::is_wedged) and `is_wedged_at`, and for the same reason.
    pub(super) fn track_at(self: &Arc<Self>, now_ms: u64) -> ReadInFlight {
        self.in_flight.fetch_add(1, AtomicOrdering::Relaxed);
        // A read beginning is progress. See the type's docs for why this edge is safe: it can
        // only land while a pool thread is free, so a wedged pool never produces one.
        self.last_progress
            .store(now_ms.max(1), AtomicOrdering::Relaxed);
        ReadInFlight {
            pool: Arc::clone(self),
        }
    }

    /// In-flight reads and the pool's blocking width — the saturation gauge for the health body.
    pub(crate) fn gauge(&self) -> (usize, usize) {
        (self.in_flight.load(AtomicOrdering::Relaxed), self.capacity)
    }

    /// Whether every pool thread is busy and no read has started, finished or unwound for longer
    /// than [`READ_POOL_WEDGE_THRESHOLD`] — a stuck pool, not merely a loaded one.
    pub(crate) fn is_wedged(&self) -> bool {
        self.is_wedged_at(self.now_ticks())
    }

    pub(super) fn is_wedged_at(&self, now_ms: u64) -> bool {
        if self.in_flight.load(AtomicOrdering::Relaxed) < self.capacity {
            return false;
        }
        let last = self.last_progress.load(AtomicOrdering::Relaxed);
        now_ms.saturating_sub(last) >= READ_POOL_WEDGE_THRESHOLD.as_millis() as u64
    }
}

/// Drop guard bracketing one read: decrements the in-flight count and stamps the progress tick as
/// the read leaves the pool, whether it returned or unwound.
pub(super) struct ReadInFlight {
    pub(super) pool: Arc<ReadPoolHealth>,
}

impl Drop for ReadInFlight {
    fn drop(&mut self) {
        self.pool
            .last_progress
            .store(self.pool.now_ticks().max(1), AtomicOrdering::Relaxed);
        self.pool.in_flight.fetch_sub(1, AtomicOrdering::Relaxed);
    }
}

/// Run a blocking read on the shared read pool — or tokio's generic blocking pool when a shard
/// has none — and turn a panic in the closure into an error instead of a process-ending abort.
///
/// A search executes tantivy and redb inside `f`, either of which can panic on a shape a
/// validator did not anticipate. With `panic = "unwind"` in the release profile, tokio unwinds
/// that panic into the task's `JoinHandle`, which resolves to a `JoinError`; mapping it here
/// leaves the caller with one failed request while the pool — and every other shard on the node
/// — keeps serving. This is the seam finding 01 turns on: under the old `panic = "abort"` the
/// same panic took the whole process down.
///
/// When a [`ReadPoolHealth`] is supplied, the closure is bracketed on the pool thread so health
/// can see saturation and, past a threshold of no progress, a wedge.
///
/// Split out from [`MicroshardActor::spawn_on_read_pool`] so the isolation itself is testable
/// without standing up a shard: it depends on nothing but the pool handle.
pub(super) async fn dispatch_read_pool<F, R>(
    handle: Option<&tokio::runtime::Handle>,
    health: Option<Arc<ReadPoolHealth>>,
    budget: Option<Duration>,
    f: F,
) -> Result<R, OrchestratorError>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    // Stamped at request arrival when the caller runs inside the request's scope — the
    // worker's op task re-enters it with the job's `arrived_at` — and read after the closure
    // is dequeued, so the elapsed span is everything the request has already spent: wire,
    // body, orchestrator queue and this pool's wait. This queue is the one the request
    // timeout cannot reach: `spawn_blocking` work is uncancellable, so a timed-out request
    // leaves its search sitting here, and the pool works through a backlog of searches whose
    // clients have all gone. See [`OrchestratorError::ReadDeadlineExpired`].
    //
    // On a path with no arrival stamp — an internal actor call, a read forwarded from a peer —
    // the helper falls back to `Instant::now()` and the span is the pool wait alone.
    let queued_at = request_started_at();
    let tracked = move || {
        if let Some(budget) = budget {
            let waited = queued_at.elapsed();
            if waited > budget {
                if let Some(health) = health.as_ref() {
                    health.record_abandoned();
                }
                return Err(OrchestratorError::ReadDeadlineExpired {
                    waited_ms: waited.as_millis() as u64,
                    budget_ms: budget.as_millis() as u64,
                });
            }
        }
        // Held across `f` on the pool thread, so in-flight reflects work actually running and the
        // guard's drop records completion even if `f` unwinds. Taken after the deadline check so
        // a refused read is never counted as in flight.
        let _in_flight = health.as_ref().map(|h| h.track());
        Ok(f())
    };
    let joined = match handle {
        Some(handle) => handle.spawn_blocking(tracked).await,
        None => tokio::task::spawn_blocking(tracked).await,
    };
    match joined {
        Ok(outcome) => outcome,
        Err(e) => Err(OrchestratorError::Io(std::io::Error::other(e))),
    }
}

/// Why a writer thread left its loop. The monitor rebuilds the writer on `Crashed` and stops on
/// `Clean`; a panic that somehow escaped even the writer's own outer catch surfaces as a join
/// error, which the monitor also treats as `Crashed`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum WriterExit {
    /// A requested shutdown, or the command channel closing at teardown.
    Clean,
    /// A panic escaped past every per-command guard and ended the thread.
    Crashed,
}

/// Everything the writer, warmup and monitor threads share for one shard. Bundled so the monitor
/// can relaunch the writer without a `&self` it does not hold: every field is an `Arc` or a cheap
/// clone, and `writer_tx` is the same shared slot the send path reads, so a relaunched writer's
/// channel becomes visible everywhere at once.
#[derive(Clone)]
pub(super) struct WriterRuntime {
    pub(super) shard_id: Uuid,
    pub(super) store: Arc<HybridStore>,
    pub(super) writer_pin: WriterPin,
    pub(super) writer_liveness: Arc<WriterLiveness>,
    pub(super) writer_tx: Arc<ArcSwapOption<mpsc::Sender<StorageCommand>>>,
    pub(super) shutdown_notify: Arc<tokio::sync::Notify>,
    pub(super) shutting_down: Arc<AtomicBool>,
}

/// Spawn a shard's warmup thread: it warms the indices named in `pending_warmup` once (empty on a
/// respawn), then serves post-commit re-warm requests until the writer drops its sender. A failure
/// to spawn is not fatal — every index still warms itself on its first query.
pub(super) fn spawn_warmup_thread(
    rt: &WriterRuntime,
    warm_rx: std::sync::mpsc::Receiver<String>,
    pending_warmup: Vec<String>,
) {
    let warmup_store = Arc::clone(&rt.store);
    let shard_id = rt.shard_id;
    let spawned = std::thread::Builder::new()
        .name(format!("warmup-shard-{shard_id}"))
        .spawn(move || {
            if !pending_warmup.is_empty() {
                let requested = pending_warmup.len();
                // Warming is a latency optimisation — a query warms its index on demand
                // regardless — so a panic here must not take the re-warm loop below down
                // with it and leave every later commit unwarmed.
                let warmed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    warmup_store.warm_indices(&pending_warmup)
                }));
                match warmed {
                    Ok(warmed) => info!(
                        shard_id = %shard_id,
                        warmed = warmed,
                        requested = requested,
                        "Phase 2 complete - index readers warmed"
                    ),
                    Err(_) => warn!(
                        shard_id = %shard_id,
                        "startup warmup panicked; indices will warm on demand"
                    ),
                }
            }

            // Serve re-warm requests until the writer thread drops its sender, which
            // happens when the shard shuts down. `warm_index` skips a searcher
            // generation it has already warmed, so bursts of commits on one index
            // collapse into a single warm. Each warm is caught: a panic warming one
            // index costs that index its pre-warming, not every later index its re-warm.
            while let Ok(index) = warm_rx.recv() {
                let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    warmup_store.warm_index(&index)
                }));
                match outcome {
                    Ok(Ok(_)) => {}
                    Ok(Err(e)) => debug!(
                        shard_id = %shard_id,
                        index = %index,
                        error = %e,
                        "Post-commit warm failed; queries will warm this index on demand"
                    ),
                    Err(_) => warn!(
                        shard_id = %shard_id,
                        index = %index,
                        "warming panicked; this index will warm on demand"
                    ),
                }
            }

            debug!(shard_id = %shard_id, "Warmup thread stopped");
        });

    if let Err(e) = spawned {
        warn!(
            shard_id = %shard_id,
            error = %e,
            "Could not spawn warmup thread; indices will warm on first query"
        );
    }
}

/// Spawn a shard's writer thread — the sole serialized path for its writes. It returns a
/// [`WriterExit`] so the monitor can tell a clean shutdown from a crash. Extracted from
/// `start` so the monitor can spawn a replacement over the same store.
pub(super) fn spawn_writer_thread(
    rt: &WriterRuntime,
    mut rx: mpsc::Receiver<StorageCommand>,
    warm_tx: std::sync::mpsc::SyncSender<String>,
) -> std::io::Result<std::thread::JoinHandle<WriterExit>> {
    let writer_store = Arc::clone(&rt.store);
    let writer_shard_id = rt.shard_id;
    let writer_pin = rt.writer_pin.clone();
    let writer_liveness = Arc::clone(&rt.writer_liveness);
    let shutdown = Arc::clone(&rt.shutdown_notify);

    std::thread::Builder::new()
        .name(format!("writer-shard-{}", writer_shard_id))
        .spawn(move || -> WriterExit {
                // Pin to the core the orchestrator picked from this shard's ordinal — the
                // same ordinal that chooses the worker feeding this thread, so the two land
                // together. Improves cache locality for the redb and tantivy structures this
                // thread owns, and removes a cross-core wakeup per write. Reports back
                // whether it took, so `/_admin/workers` can show the outcome.
                writer_pin.apply(writer_shard_id);

                info!(shard_id = %writer_shard_id, "Writer thread started (write coalescing enabled)");

                // Each index's writes go through `guard_writer_op`, so a panic applying one
                // index becomes an error for that index and rebuilds its writer while the thread
                // keeps serving. This outer boundary is the last resort: a panic anywhere else in
                // the loop ends the thread rather than unwinding into the process (which the
                // release profile now lets unwind), and marks the writer down so health sees a
                // shard that can no longer take writes. A clean exit — a shutdown command, or the
                // channel closing as the node tears down — is not a fault and marks nothing.
                // Published so health can tell a writer wedged mid-batch from one idle on the
                // channel: stamped as each batch starts, cleared to 0 once it drains.
                let writer_heartbeat = writer_liveness.register_writer();

                let loop_outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    // Reusable buffers to avoid per-iteration allocations. Every one is
                    // cleared where the iteration used to allocate it; `HashMap::clear` and
                    // `Vec::clear` keep the backing storage, so a busy writer pays the growth
                    // once rather than once per drained batch.
                    let mut pending_cmds: Vec<StorageCommand> = Vec::with_capacity(256);
                    let mut write_groups: HashMap<String, Vec<WriteCommand>> = HashMap::new();
                    let mut batch_groups: HashMap<String, Vec<BatchCommand>> = HashMap::new();
                    let mut commits: Vec<(String, tokio::sync::oneshot::Sender<Result<(), StoreError>>)> = Vec::new();
                    let mut evictions: Vec<(String, tokio::sync::oneshot::Sender<bool>)> = Vec::new();
                    let mut deletions: Vec<DeleteCommand> = Vec::new();
                    let mut rebuilds: Vec<RebuildCommand> = Vec::new();
                    let mut committed_indices: HashSet<String> = HashSet::new();
                    let mut written_indices: HashSet<String> = HashSet::new();
                    let mut mixed: Vec<String> = Vec::new();

                    while let Some(first_cmd) = rx.blocking_recv() {
                    // Mark the writer busy for the whole batch it is about to drain and apply;
                    // `max(1)` keeps the stamp distinct from the 0 that means idle.
                    writer_heartbeat.store(writer_liveness.now_ticks().max(1), AtomicOrdering::Relaxed);
                    // Phase 1: Drain all pending commands from the channel.
                    // The first command blocks until available; subsequent commands
                    // are non-blocking to coalesce as many writes as possible.
                    // Limit drain to prevent starvation - max 256 additional commands per iteration.
                    pub(super) const MAX_DRAIN_PER_ITERATION: usize = 256;
                    pending_cmds.clear();
                    pending_cmds.push(first_cmd);
                    let mut drained = 0;
                    while drained < MAX_DRAIN_PER_ITERATION {
                        match rx.try_recv() {
                            Ok(cmd) => {
                                pending_cmds.push(cmd);
                                drained += 1;
                            }
                            Err(_) => break, // Channel empty or disconnected
                        }
                    }

                    // Phase 2: Group commands by type and index for coalescing.
                    // Both single Write and BatchWrite commands for the same index
                    // are merged to reduce redb transactions and fsyncs.
                    write_groups.clear();
                    batch_groups.clear();
                    commits.clear();
                    evictions.clear();
                    deletions.clear();
                    rebuilds.clear();
                    let mut should_shutdown = false;
                    // Indices whose Tantivy commit published a new segment this iteration.
                    // Collected rather than posted inline so a burst that commits the same
                    // index several times results in one re-warm request.
                    committed_indices.clear();
                    // Indices this drain wrote to, whose commit is decided once every reply
                    // has been sent — see Phase 4b.
                    written_indices.clear();

                    for cmd in pending_cmds.drain(..) {
                        match cmd {
                            StorageCommand::Write { index, op, reply } => {
                                write_groups.entry(index).or_default().push((op, reply));
                            }
                            StorageCommand::BatchWrite { index, ops, reply } => {
                                batch_groups.entry(index).or_default().push((ops, reply));
                            }
                            StorageCommand::Commit { index, reply } => {
                                commits.push((index, reply));
                            }
                            StorageCommand::EvictWriter { index, reply } => {
                                evictions.push((index, reply));
                            }
                            StorageCommand::DeleteIndex { index, delete_schema, reply } => {
                                deletions.push((index, delete_schema, reply));
                            }
                            StorageCommand::RebuildIfEmpty { index, schema, reply } => {
                                rebuilds.push((index, schema, reply));
                            }
                            StorageCommand::Shutdown => {
                                should_shutdown = true;
                            }
                        }
                    }

                    // Phase 3a: indexes that received both single writes and batch writes in
                    // this same drain.
                    //
                    // Applied by the two phases below they are two
                    // `apply_batch` calls — two redb transactions, and with
                    // `wal_sync` on two fsyncs — for work one transaction covers. The comment
                    // above has claimed the two are merged since before they were; this is
                    // where it becomes true. Singles go ahead of batches, which is the order
                    // the phases below would have applied them in, so which write wins a
                    // duplicated id does not change.
                    mixed.clear();
                    mixed.extend(
                        write_groups
                            .keys()
                            .filter(|index| batch_groups.contains_key(*index))
                            .cloned(),
                    );

                    for index in mixed.drain(..) {
                        #[cfg(feature = "fault-injection")]
                        fault_injection::panic_if_writer_thread_trap(&index);

                        let writes = write_groups.remove(&index).unwrap_or_default();
                        let batches = batch_groups.remove(&index).unwrap_or_default();

                        let mut merged_ops: Vec<WalOp> = Vec::new();
                        let mut segments: Vec<MergedWriteReply> = Vec::new();
                        let mut op_counts: Vec<usize> = Vec::new();
                        let mut batch_segments = 0usize;
                        for (op, reply) in writes {
                            merged_ops.push(op);
                            op_counts.push(1);
                            segments.push(MergedWriteReply::Single(reply));
                        }
                        for (ops, reply) in batches {
                            let count = ops.len();
                            batch_segments += 1;
                            merged_ops.extend(ops);
                            op_counts.push(count);
                            segments.push(MergedWriteReply::Batch(reply));
                        }

                        let total_ops = merged_ops.len();
                        let res = guard_writer_op(&writer_store, &index, || {
                            writer_store.apply_batch(&index, merged_ops)
                        });

                        let fail_all = |segments: Vec<MergedWriteReply>, error: &StoreError| {
                            for segment in segments {
                                let err = Err(error.duplicate());
                                match segment {
                                    MergedWriteReply::Single(reply) => {
                                        let _ = reply.send(err.map(|_: Vec<u64>| 0));
                                    }
                                    MergedWriteReply::Batch(reply) => {
                                        let _ = reply.send(err);
                                    }
                                }
                            }
                        };

                        match res {
                            Ok((seq_ids, _new_docs)) => {
                                // One sequence per op is the storage layer's contract. Checked
                                // rather than indexed on faith: this runs on the writer thread,
                                // where a panic takes every shard's writes down with it.
                                let Some(ranges) = merged_reply_ranges(&op_counts, seq_ids.len())
                                else {
                                    tracing::error!(
                                        index = %index,
                                        expected = total_ops,
                                        got = seq_ids.len(),
                                        "Writer: merged write returned the wrong number of sequence ids"
                                    );
                                    fail_all(
                                        segments,
                                        &StoreError::Serialization(
                                            "merged write returned the wrong number of sequence ids"
                                                .to_string(),
                                        ),
                                    );
                                    continue;
                                };
                                written_indices.insert(index.clone());
                                tracing::debug!(
                                    index = %index,
                                    total_ops,
                                    batch_segments,
                                    "Writer: merged single and batch writes into one transaction"
                                );

                                for (segment, range) in segments.into_iter().zip(ranges) {
                                    match segment {
                                        MergedWriteReply::Single(reply) => {
                                            let _ = reply.send(Ok(seq_ids[range.start]));
                                        }
                                        MergedWriteReply::Batch(reply) => {
                                            let _ = reply.send(Ok(seq_ids[range].to_vec()));
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::error!(
                                    index = %index,
                                    error = %e,
                                    "Writer: merged single and batch write failed"
                                );
                                fail_all(segments, &e);
                            }
                        }
                    }

                    // Phase 3: Process coalesced single writes (biggest optimization).
                    // Multiple single writes to the same index become one apply_batch call
                    // with a single redb transaction instead of N separate transactions.
                    for (index, writes) in &mut write_groups {
                        // Outside guard_writer_op on purpose: this panic escapes the per-command
                        // guard, so it reaches the loop's outer catch and takes the writer thread
                        // down — the path that must mark the writer down and turn health red.
                        #[cfg(feature = "fault-injection")]
                        fault_injection::panic_if_writer_thread_trap(index);
                        if writes.len() == 1 {
                            // Single write — no coalescing overhead needed
                            let (op, reply) = writes.pop().unwrap();
                            let res = guard_writer_op(&writer_store, index, || {
                                writer_store.apply_write(index, op)
                            });
                            match &res {
                                Ok(_) => {
                                    written_indices.insert(index.clone());
                                }
                                Err(e) => tracing::error!(index = %index, error = %e, "Writer: write failed"),
                            }
                            let _ = reply.send(res);
                        } else {
                            // Coalesced writes — merge N single writes into one batch
                            let coalesced_count = writes.len();
                            let (ops, replies): (Vec<WalOp>, Vec<_>) = writes.drain(..).unzip();

                            let res = guard_writer_op(&writer_store, index, || {
                                writer_store.apply_batch(index, ops)
                            });
                            match res {
                                Ok((seq_ids, _new_docs)) => {
                                    written_indices.insert(index.clone());
                                    tracing::debug!(
                                        index = %index,
                                        coalesced = coalesced_count,
                                        "Writer: coalesced {} single writes into one batch",
                                        coalesced_count
                                    );
                                    // Distribute individual seq_ids back to each caller
                                    for (reply, seq_id) in replies.into_iter().zip(seq_ids) {
                                        let _ = reply.send(Ok(seq_id));
                                    }
                                }
                                Err(e) => {
                                    // Broadcast error to all callers in this coalesced group
                                    let err_msg = e.to_string();
                                    tracing::error!(
                                        index = %index,
                                        coalesced = coalesced_count,
                                        error = %err_msg,
                                        "Writer: coalesced batch write failed"
                                    );
                                    for reply in replies {
                                        let _ = reply.send(Err(e.duplicate()));
                                    }
                                }
                            }
                        }
                    }

                    // Phase 4: Process coalesced batch writes.
                    // Multiple BatchWrite commands for the same index are merged into
                    // a single apply_batch call, then results are split back to callers.
                    for (index, batches) in batch_groups.drain() {
                        if batches.len() == 1 {
                            // Single batch — no coalescing overhead needed
                            let (ops, reply) = batches.into_iter().next().unwrap();
                            let res = guard_writer_op(&writer_store, &index, || {
                                writer_store.apply_batch(&index, ops)
                            });
                            match &res {
                                Ok(_) => {
                                    written_indices.insert(index.clone());
                                }
                                Err(e) => tracing::error!(index = %index, error = %e, "Writer: batch write failed"),
                            }
                            let _ = reply.send(res.map(|(seq_ids, _)| seq_ids));
                        } else {
                            // Coalesced batches — merge N batch writes into one
                            let coalesced_count = batches.len();
                            let mut merged_ops: Vec<WalOp> = Vec::new();
                            let mut reply_segments: Vec<BatchReplySegment> = Vec::new();

                            for (ops, reply) in batches {
                                let op_count = ops.len();
                                merged_ops.extend(ops);
                                reply_segments.push((op_count, reply));
                            }

                            let total_ops = merged_ops.len();
                            let res = guard_writer_op(&writer_store, &index, || {
                                writer_store.apply_batch(&index, merged_ops)
                            });
                            match res {
                                Ok((seq_ids, _new_docs)) => {
                                    written_indices.insert(index.clone());
                                    tracing::debug!(
                                        index = %index,
                                        coalesced_batches = coalesced_count,
                                        total_ops = total_ops,
                                        "Writer: coalesced {} batch writes ({} ops) into one transaction",
                                        coalesced_count, total_ops
                                    );

                                    // Split merged seq_ids back to each caller by their original op count
                                    let mut offset = 0usize;
                                    for (op_count, reply) in reply_segments {
                                        let segment: Vec<u64> =
                                            seq_ids[offset..offset + op_count].to_vec();
                                        let _ = reply.send(Ok(segment));
                                        offset += op_count;
                                    }
                                }
                                Err(e) => {
                                    let err_msg = e.to_string();
                                    tracing::error!(
                                        index = %index,
                                        coalesced_batches = coalesced_count,
                                        error = %err_msg,
                                        "Writer: coalesced batch write failed"
                                    );
                                    for (_op_count, reply) in reply_segments {
                                        let _ = reply.send(Err(e.duplicate()));
                                    }
                                }
                            }
                        }
                    }

                    // Phase 4b: the commits this drain's writes made due, decided only now that
                    // every one of their callers has been answered.
                    //
                    // A write is durable once its redb transaction commits, which happened above
                    // and before its reply. The Tantivy commit decided here is about visibility to
                    // search, and it is the expensive half: the indexer flushes its segment and
                    // every file of it is synced. Deciding it inside each apply made the callers of
                    // that drain wait for it, and told them their write had failed when only the
                    // commit had — the write itself was durable, and the WAL still held it for the
                    // next commit or a replay. Once per index here, however many groups wrote to it.
                    for index in written_indices.drain() {
                        match guard_writer_op(&writer_store, &index, || {
                            writer_store.maybe_commit_writer(&index)
                        }) {
                            Ok(true) => {
                                tracing::debug!(index = %index, "Writer: commit after this drain's writes");
                                committed_indices.insert(index);
                            }
                            Ok(false) => {}
                            Err(e) => tracing::error!(
                                index = %index,
                                error = %e,
                                "Writer: commit failed; the WAL keeps these writes for the next commit or a replay"
                            ),
                        }
                    }

                    // Phase 5: Process commits after all writes are applied
                    for (index, reply) in commits.drain(..) {
                        let res = guard_writer_op(&writer_store, &index, || {
                            writer_store.commit_index(&index)
                        });
                        if res.is_ok() {
                            committed_indices.insert(index.clone());
                        }
                        let _ = reply.send(res);
                    }

                    // Phase 5b: Process writer evictions (commit then drop from cache)
                    for (index, reply) in evictions.drain(..) {
                        if let Err(e) = guard_writer_op(&writer_store, &index, || {
                            writer_store.commit_index(&index)
                        }) {
                            tracing::warn!(index = %index, error = %e, "Evict: commit failed, evicting anyway");
                        }
                        let removed = writer_store.force_remove_writer(&index);
                        tracing::info!(index = %index, removed, "Writer evicted from cache");
                        let _ = reply.send(removed);
                    }

                    // Phase 5c: Process index deletions last, so any writes that were
                    // batched alongside the delete are applied before their tables go away.
                    for (index, delete_schema, reply) in deletions.drain(..) {
                        let res = guard_writer_op(&writer_store, &index, || {
                            writer_store.delete_index_data(&index, delete_schema)
                        });
                        match &res {
                            Ok(()) => info!(index = %index, delete_schema, "Index data deleted"),
                            Err(e) => warn!(index = %index, error = %e, "Index deletion failed"),
                        }
                        let _ = reply.send(res);
                    }

                    // Phase 5c': Rebuilds after deletions, for the same reason — a write
                    // batched alongside is applied first, and is then counted.
                    for (index, schema, reply) in rebuilds.drain(..) {
                        let res = guard_writer_op(&writer_store, &index, || {
                            let documents = writer_store.document_count(&index)?;
                            if documents > 0 {
                                return Ok(documents);
                            }
                            writer_store.delete_index_data(&index, false)?;
                            writer_store.store_schema_and_cache(&index, &schema)?;
                            drop(writer_store.get_or_create_index(&index)?);
                            Ok(0)
                        });
                        match &res {
                            Ok(0) => info!(index = %index, "Index rebuilt under a new schema"),
                            Ok(documents) => info!(
                                index = %index,
                                documents,
                                "Index not rebuilt: it holds documents"
                            ),
                            Err(e) => warn!(index = %index, error = %e, "Index rebuild failed"),
                        }
                        let _ = reply.send(res);
                    }

                    // Phase 5d: Ask the warmup thread to re-warm the indices we just
                    // committed. A commit publishes a new segment and replaces the searcher
                    // generation, which discards the per-field caches the previous generation
                    // had warmed. `try_send` keeps this strictly best-effort: if the warmup
                    // thread is behind, the request is dropped rather than stalling writes,
                    // and those indices simply warm on their next commit or first query.
                    for index in committed_indices.drain() {
                        if warm_tx.try_send(index).is_err() {
                            tracing::trace!("Warm request dropped; warmup thread busy or stopped");
                        }
                    }

                    // Batch drained and applied; the thread is about to wait again, so it is no
                    // longer mid-op and must not read as stalled while it idles.
                    writer_heartbeat.store(0, AtomicOrdering::Relaxed);

                    // Phase 6: Handle shutdown after draining all pending work
                    if should_shutdown {
                        info!(shard_id = %writer_shard_id, "Writer thread shutting down");
                        break;
                    }
                    }
                }));

                // The thread is exiting, cleanly or by panic; a panic leaves the heartbeat frozen
                // at its last batch stamp, so clear it here to keep a dead writer from also being
                // read as stalled.
                writer_heartbeat.store(0, AtomicOrdering::Relaxed);

                let exit = if loop_outcome.is_err() {
                    // A panic reached past every per-command guard, so the thread is done. Mark it
                    // down so health goes red, and report `Crashed` so the monitor rebuilds it over
                    // this same store — only the thread died, the data is intact.
                    writer_liveness.mark_writer_down();
                    tracing::error!(
                        shard_id = %writer_shard_id,
                        "writer thread crashed; the monitor will respawn it"
                    );
                    WriterExit::Crashed
                } else {
                    info!(shard_id = %writer_shard_id, "Writer thread stopped");
                    WriterExit::Clean
                };
                shutdown.notify_one();
                exit
        })
}

/// Rebuild a shard's writer after a crash: a fresh command channel and warm channel over the same
/// store, the new sender published into the shared slot before the writer starts draining so a
/// racing write finds the live channel. No recovery and no startup warmup — see [`relaunch_writer`]
/// callers and `WriterExit::Crashed`.
pub(super) fn relaunch_writer(
    rt: &WriterRuntime,
) -> std::io::Result<std::thread::JoinHandle<WriterExit>> {
    let (tx, rx) = mpsc::channel::<StorageCommand>(SHARD_WRITER_CHANNEL_CAPACITY);
    let (warm_tx, warm_rx) = std::sync::mpsc::sync_channel::<String>(WARM_REQUEST_CAPACITY);
    rt.writer_tx.store(Some(Arc::new(tx)));
    spawn_warmup_thread(rt, warm_rx, Vec::new());
    spawn_writer_thread(rt, rx, warm_tx)
}

/// Supervise one shard's writer thread, replacing it if it dies. Runs on its own thread, parked in
/// `join` at no cost until the writer exits: a clean exit (shutdown) stops the monitor, a crash
/// makes it relaunch the writer over the same store and keep supervising. It owns the writer's
/// handle, so a genuinely wedged writer that never exits simply keeps the monitor parked rather
/// than being force-killed — Rust has no safe way to terminate a running thread, and that case is
/// already surfaced red by the stall heartbeat.
pub(super) fn writer_monitor(rt: WriterRuntime, mut handle: std::thread::JoinHandle<WriterExit>) {
    loop {
        let exit = match handle.join() {
            Ok(exit) => exit,
            Err(payload) => {
                let message = payload
                    .downcast_ref::<String>()
                    .map(|s| s.as_str())
                    .or_else(|| payload.downcast_ref::<&str>().copied())
                    .unwrap_or("panic payload is not a string");
                tracing::error!(
                    shard_id = %rt.shard_id,
                    panic = %message,
                    "writer thread panicked; the monitor will respawn it"
                );
                WriterExit::Crashed
            }
        };
        if rt.shutting_down.load(AtomicOrdering::Relaxed) || exit == WriterExit::Clean {
            debug!(shard_id = %rt.shard_id, "Writer monitor stopping");
            break;
        }
        // Only ever a no-op outside the fault-injection build: the smoke test uses it to hold the
        // shard writerless long enough to observe the red that a crash must report.
        #[cfg(feature = "fault-injection")]
        fault_injection::hold_before_respawn();

        match relaunch_writer(&rt) {
            Ok(new_handle) => {
                handle = new_handle;
                rt.writer_liveness.mark_writer_up();
                info!(
                    shard_id = %rt.shard_id,
                    "Writer thread respawned; the shard is accepting writes again"
                );
            }
            Err(e) => {
                tracing::error!(
                    shard_id = %rt.shard_id,
                    error = %e,
                    "Could not respawn the writer thread; the shard stays down until restart"
                );
                break;
            }
        }
    }
}

impl MicroshardActor {
    pub(super) fn new(
        shard_id: Uuid,
        storage_config: StorageConfig,
        runtime: ShardRuntime,
    ) -> Self {
        let ShardRuntime {
            default_search_limit,
            read_pool_handle,
            read_pool_health,
            read_budget,
            total_shards,
            writer_shutdown_timeout_secs,
            supervisor_timeout_secs,
            writer_pin,
            writer_liveness,
        } = runtime;

        Self {
            shard_id,
            store: None,
            writer_tx: Arc::new(ArcSwapOption::empty()),
            writer_monitor_handle: Arc::new(std::sync::Mutex::new(None)),
            shutting_down: Arc::new(AtomicBool::new(false)),
            read_budget,
            storage_config,
            default_search_limit,
            supervisors: Arc::new(AsyncRwLock::new(HashMap::new())),
            shutdown_notify: Arc::new(tokio::sync::Notify::new()),
            read_pool_handle,
            read_pool_health,
            total_shards,
            writer_shutdown_timeout_secs,
            supervisor_timeout_secs,
            writer_pin,
            writer_liveness,
        }
    }

    pub(super) async fn start(&mut self) -> Result<(), OrchestratorError> {
        info!(
            shard_id = %self.shard_id,
            path = %self.storage_config.shard_path.display(),
            "MicroshardActor starting"
        );

        // Initialize HybridStore with spawn_blocking to avoid blocking async runtime
        let config = self.storage_config.clone();
        let total_shards = self.total_shards;
        let store = tokio::task::spawn_blocking(move || HybridStore::new(config, total_shards))
            .await
            .map_err(|e| OrchestratorError::Io(std::io::Error::other(e)))?
            .map_err(|e: StoreError| match e {
                StoreError::Io(io_err) => OrchestratorError::Io(io_err),
                _ => OrchestratorError::Io(std::io::Error::other(e.to_string())),
            })?;

        let store_arc = Arc::new(store);
        self.store = Some(store_arc.clone());

        // Startup runs in two phases, both off the async runtime.
        //
        // Phase 1 (recovery) is a correctness requirement: an index whose WAL tail was never
        // committed answers searches without its most recent writes, so it must be replayed
        // and committed. Only indices whose persisted checkpoint falls short of their WAL
        // are touched.
        //
        // Phase 2 (warmup) is purely latency: it opens and caches the *reader* for each
        // index and faults in its segment structures, so the first query from an agent or
        // client does not pay for opening the index. It runs on its own thread and never
        // gates serving — a request arriving first just warms that index on demand.
        //
        // Neither phase blocks `start()`. Requests are served throughout via lazy
        // initialization; the phases only determine whether that work has already been done.
        //
        // The writer channel is created here rather than beside the writer thread below,
        // because phase 1 needs a sender: the commit that finishes a replay belongs on the
        // writer thread like every other commit.
        // The runtime the writer, warmup and monitor threads all share. Cloned into the monitor
        // so it can relaunch the writer over this same store — the durable state a crash leaves
        // intact — without a `&self` it cannot hold.
        let rt = WriterRuntime {
            shard_id: self.shard_id,
            store: Arc::clone(&store_arc),
            writer_pin: self.writer_pin.clone(),
            writer_liveness: Arc::clone(&self.writer_liveness),
            writer_tx: Arc::clone(&self.writer_tx),
            shutdown_notify: Arc::clone(&self.shutdown_notify),
            shutting_down: Arc::clone(&self.shutting_down),
        };

        let (tx, rx) = mpsc::channel::<StorageCommand>(SHARD_WRITER_CHANNEL_CAPACITY);
        let recovery_writer_tx = tx.clone();
        // Published into the shared slot the send path reads, so a write reaches the writer as
        // soon as the channel exists — and reaches its replacement after a respawn.
        rt.writer_tx.store(Some(Arc::new(tx)));

        // After startup the warmup thread keeps serving re-warm requests: the writer thread
        // posts an index name here after each commit, so the segment a commit just published
        // gets warmed without doing that work on the write hot path. The channel is bounded
        // and posted to with `try_send`, making re-warming strictly best-effort — a full
        // channel drops the request rather than ever stalling a write.
        let (warm_tx, warm_rx) = std::sync::mpsc::sync_channel::<String>(WARM_REQUEST_CAPACITY);
        let warmup_store = Arc::clone(&store_arc);
        let shard_id = self.shard_id;
        let rt_recovery = rt.clone();
        tokio::task::spawn_blocking(move || {
            let plan = match warmup_store.recover_indices() {
                Ok(plan) => plan,
                Err(e) => {
                    warn!(
                        shard_id = %shard_id,
                        error = %e,
                        "Index recovery failed; indices will recover on first access"
                    );
                    return;
                }
            };

            // `get_or_create_index` leaves a replayed tail in the writer's buffer rather
            // than committing inline: on a large index that is segment merging and an fsync
            // inside a call that only meant to open the index. Phase 1 commits it here, so
            // the phase ends in a state that stands on its own — the ordinary flush triggers
            // are both write-driven, and a recovered index may take no further writes.
            //
            // The commits go through the writer thread like every other, serializing against
            // any write already arriving, and each truncates the WAL range it covers.
            // Awaiting one before sending the next costs nothing — that thread runs them one
            // at a time — and surfaces a failure instead of dropping the reply.
            let mut committed = 0usize;
            for index in &plan.recovered {
                let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
                if recovery_writer_tx
                    .blocking_send(StorageCommand::Commit {
                        index: index.clone(),
                        reply: reply_tx,
                    })
                    .is_err()
                {
                    warn!(
                        shard_id = %shard_id,
                        index = %index,
                        "Writer thread is gone; the recovered tail stays in the WAL for the next boot"
                    );
                    break;
                }
                match reply_rx.blocking_recv() {
                    Ok(Ok(())) => committed += 1,
                    Ok(Err(e)) => warn!(
                        shard_id = %shard_id,
                        index = %index,
                        error = %e,
                        "Could not commit the recovered tail; it stays in the WAL for the next boot"
                    ),
                    Err(_) => warn!(
                        shard_id = %shard_id,
                        index = %index,
                        "Writer thread dropped the reply to the recovery commit"
                    ),
                }
            }

            // Nothing below sends to the writer thread; releasing the sender keeps this task
            // out of the set that decides when the channel closes.
            drop(recovery_writer_tx);

            // Logged after the commits: the shard is queryable for a replayed tail only once
            // they land.
            info!(
                shard_id = %shard_id,
                recovered = plan.recovered.len(),
                committed = committed,
                failed = plan.failed.len(),
                pending_warmup = plan.pending_warmup.len(),
                "Phase 1 complete - shard is queryable"
            );

            // One warmup thread per shard: startup warmup for the tail recovery just replayed,
            // then post-commit re-warms. Spawned through the shared helper so a respawn gets the
            // same warmup thread back, with nothing to warm at startup.
            spawn_warmup_thread(&rt_recovery, warm_rx, plan.pending_warmup);
        });

        // Spawn the dedicated writer thread and the monitor that will relaunch it if it crashes.
        // The writer's channel was created above, before phase 1, so recovery could post its
        // commits onto it.
        let writer_handle = spawn_writer_thread(&rt, rx, warm_tx).map_err(OrchestratorError::Io)?;

        // The monitor owns the writer's handle and joins it: a clean exit stops the monitor, a
        // crash makes it rebuild the writer over the same store and keep supervising. Shutdown
        // joins the monitor, not the writer.
        let monitor = std::thread::Builder::new()
            .name(format!("writer-monitor-shard-{}", self.shard_id))
            .spawn(move || writer_monitor(rt, writer_handle))
            .map_err(OrchestratorError::Io)?;
        *self
            .writer_monitor_handle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(monitor);

        info!(shard_id = %self.shard_id, "MicroshardActor initialized with dedicated writer thread");
        Ok(())
    }

    /// Send a command to the dedicated writer thread. Loads the current sender from the shared
    /// slot, so a command sent just after a crash reaches the monitor's replacement writer once it
    /// has published its channel. A send that lands in the brief window before the replacement is
    /// up fails as "Writer thread closed" and is retriable, exactly as it was before this slot.
    pub(super) async fn send_write_command(
        &self,
        cmd: StorageCommand,
    ) -> Result<(), OrchestratorError> {
        let tx = self.writer_tx.load_full().ok_or_else(|| {
            OrchestratorError::NotReady("Writer channel not initialized".to_string())
        })?;
        tx.send(cmd)
            .await
            .map_err(|_| OrchestratorError::Io(std::io::Error::other("Writer thread closed")))
    }

    /// Write a single document via the dedicated writer thread.
    pub(super) async fn handle_write_via_channel(
        &self,
        index: String,
        op: WalOp,
    ) -> Result<u64, OrchestratorError> {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        self.send_write_command(StorageCommand::Write {
            index,
            op,
            reply: reply_tx,
        })
        .await?;
        reply_rx
            .await
            .map_err(|_| OrchestratorError::Io(std::io::Error::other("Writer dropped reply")))?
            .map_err(OrchestratorError::Storage)
    }

    /// Write a batch of documents via the dedicated writer thread.
    pub(super) async fn handle_batch_write_via_channel(
        &self,
        index: String,
        ops: Vec<WalOp>,
    ) -> Result<Vec<u64>, OrchestratorError> {
        let index_for_log = index.clone();
        tracing::debug!(
            shard_id = %self.shard_id,
            index = %index_for_log,
            ops_count = ops.len(),
            "MicroshardActor: Sending batch write to writer thread"
        );
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        self.send_write_command(StorageCommand::BatchWrite {
            index,
            ops,
            reply: reply_tx,
        })
        .await?;
        tracing::debug!(
            shard_id = %self.shard_id,
            index = %index_for_log,
            "MicroshardActor: Waiting for writer thread reply"
        );
        let result = reply_rx
            .await
            .map_err(|_| OrchestratorError::Io(std::io::Error::other("Writer dropped reply")))?
            .map_err(OrchestratorError::Storage)?;
        tracing::debug!(
            shard_id = %self.shard_id,
            index = %index_for_log,
            seq_count = result.len(),
            "MicroshardActor: Batch write completed successfully"
        );
        Ok(result)
    }

    pub(crate) async fn admin_commit_via_channel(
        &self,
        index: String,
    ) -> Result<(), OrchestratorError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.send_write_command(StorageCommand::Commit { index, reply: tx })
            .await?;
        rx.await
            .map_err(|_| OrchestratorError::Io(std::io::Error::other("Writer dropped reply")))?
            .map_err(OrchestratorError::Storage)
    }

    /// Indexes this shard holds open, and its share of the node-wide cap (`0` when uncapped).
    ///
    /// Synchronous on purpose: both are lock-free reads off the store, so this does not join
    /// the writer thread's queue to answer a question about how full that queue's shard is.
    pub(crate) fn open_index_counts(&self) -> (usize, usize) {
        // A shard whose store is not attached yet holds nothing open, which is the honest
        // answer rather than a missing one.
        self.store
            .as_ref()
            .map_or((0, 0), |s| (s.open_index_count(), s.open_index_cap()))
    }

    pub(crate) async fn admin_evict_writer_via_channel(
        &self,
        index: String,
    ) -> Result<bool, OrchestratorError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.send_write_command(StorageCommand::EvictWriter { index, reply: tx })
            .await?;
        rx.await
            .map_err(|_| OrchestratorError::Io(std::io::Error::other("Writer dropped reply")))
    }

    /// Gracefully stop the writer thread with timeout.
    /// Clears supervisors, sends shutdown command, and waits for completion.
    /// If timeout expires, abandons the thread (OS cleanup on process exit).
    pub(super) async fn shutdown_writer(&mut self) {
        use tokio::time::{Duration, timeout};

        // Clear supervisors (they hold cloned writer_tx)
        {
            let mut supervisors = self.supervisors.write().await;
            let count = supervisors.len();
            supervisors.clear();
            if count > 0 {
                tracing::debug!(shard_id = %self.shard_id, count, "Cleared supervisor tasks");
            }
        }

        // Tell the monitor this is a requested stop, so it treats the coming exit as clean and
        // does not respawn the writer. Set before the Shutdown command so it is visible however
        // the writer races to exit.
        self.shutting_down.store(true, AtomicOrdering::Relaxed);

        // Send shutdown signal and wait for thread exit. Taking the sender out of the shared slot
        // also closes the channel to any lingering supervisor clone.
        if let Some(tx) = self.writer_tx.swap(None) {
            if tx.send(StorageCommand::Shutdown).await.is_ok() {
                let timeout_secs = self.writer_shutdown_timeout_secs;
                match timeout(
                    Duration::from_secs(timeout_secs),
                    self.shutdown_notify.notified(),
                )
                .await
                {
                    Ok(()) => {
                        tracing::info!(shard_id = %self.shard_id, "Writer thread shutdown complete");
                        // Join the monitor; it has seen the clean exit and stopped, and it owns
                        // the writer thread's handle, so this joins the whole writer stack.
                        if let Some(handle) = self
                            .writer_monitor_handle
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner())
                            .take()
                            && let Err(e) = handle.join()
                        {
                            tracing::warn!(shard_id = %self.shard_id, error = ?e, "Writer monitor panicked");
                        }
                    }
                    Err(_) => {
                        tracing::error!(
                            shard_id = %self.shard_id,
                            timeout_secs = timeout_secs,
                            "Writer thread shutdown timed out after {}s - abandoning",
                            timeout_secs
                        );
                        // Abandon the monitor thread - OS will clean up on process exit
                        *self
                            .writer_monitor_handle
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
                    }
                }
            } else {
                tracing::warn!(shard_id = %self.shard_id, "Writer thread already closed");
            }
        }
    }

    /// Dispatch a blocking closure to the dedicated read pool if available,
    /// falling back to tokio's generic blocking pool otherwise.
    pub(super) async fn spawn_on_read_pool<F, R>(&self, f: F) -> Result<R, OrchestratorError>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        dispatch_read_pool(
            self.read_pool_handle.as_ref(),
            self.read_pool_health.clone(),
            self.read_budget,
            f,
        )
        .await
    }

    /// Handles search requests on the dedicated read thread pool.
    pub(super) async fn handle_search(
        &self,
        request: SearchRequest,
    ) -> Result<SearchReply, OrchestratorError> {
        let store = self.store.as_ref().ok_or_else(|| {
            OrchestratorError::NotReady("HybridStore not initialized".to_string())
        })?;

        let store = Arc::clone(store);
        let query = request.query;
        let limit = request.limit.unwrap_or(self.default_search_limit);
        let index = request.index.clone();
        let sort = request.sort;

        let outcome = self
            .spawn_on_read_pool(move || {
                #[cfg(feature = "fault-injection")]
                fault_injection::panic_if_read_trap(&query);
                store.search_documents(&index, &query, limit, sort.as_ref())
            })
            .await?
            .map_err(|e: StoreError| match e {
                StoreError::Io(io_err) => OrchestratorError::Io(io_err),
                // A field this index does not carry, or a query it cannot parse, is the
                // request's fault rather than the node's — and the distinction has to survive
                // the crossing, because an error leaves this actor as a string and everything
                // downstream reads its verdict to decide who is at fault. Flattened to
                // `other`, a sort naming a column the index never built read as an internal
                // failure, which is how it came back as a partial outage inside a `200`.
                StoreError::FieldNotFound(_) | StoreError::QueryParser(_) => {
                    OrchestratorError::Validation(e.to_string())
                }
                _ => OrchestratorError::Io(std::io::Error::other(e.to_string())),
            })?;

        let search_hits: Vec<SearchHit> = outcome
            .hits
            .into_iter()
            .map(|(score, doc)| SearchHit { score, doc })
            .collect();

        Ok(SearchReply {
            hits: search_hits,
            total_hits: outcome.total_hits,
            discarded: outcome.discarded,
            approximate_sort: outcome.approximate_sort,
            narrowed_default_fields: outcome.narrowed_default_fields,
            emptied: outcome.emptied,
        })
    }

    /// Handles shard statistics requests on the dedicated read thread pool.
    pub(super) async fn handle_get_stats(
        &self,
        msg: GetShardStats,
    ) -> Result<storage::ShardStatsSnapshot, OrchestratorError> {
        let store = self.store.as_ref().ok_or_else(|| {
            OrchestratorError::NotReady("HybridStore not initialized".to_string())
        })?;

        let store = Arc::clone(store);
        let include_data_size = msg.include_data_size;

        self.spawn_on_read_pool(move || store.gather_index_stats(include_data_size))
            .await?
            .map_err(|e: StoreError| match e {
                StoreError::Io(io_err) => OrchestratorError::Io(io_err),
                _ => OrchestratorError::Io(std::io::Error::other(e.to_string())),
            })
    }

    /// Signal the supervisor for a specific index that a write has occurred.
    /// Spawns a new supervisor if one doesn't exist.
    /// The supervisor's role is idle-timeout commit: if no writes arrive for N seconds,
    /// it sends a Commit to the writer thread to flush any remaining uncommitted data.
    pub(super) async fn signal_supervisor(&self, index: String) {
        // Snapshot the current sender for this supervisor's idle commits. If the writer is later
        // replaced, this snapshot's channel closes; the supervisor's send then fails and it exits,
        // and the next write re-arms a fresh supervisor with the new sender.
        let writer_tx = match self.writer_tx.load_full() {
            Some(tx) => tx,
            None => return,
        };

        // Fast path: the supervisor for this index already exists, which is the case for
        // every write after the first. Taking the write lock here — as this used to — made
        // all concurrent writes to a shard serialize on the supervisor map and gave the
        // scheduler a reason to park the task, on the hot path, purely to send a timer reset.
        {
            let supervisors = self.supervisors.read().await;
            if let Some(tx) = supervisors.get(&index) {
                let _ = tx.try_send(());
                return;
            }
        }

        let mut supervisors = self.supervisors.write().await;
        // Re-check: another writer may have created the supervisor while we waited for the
        // write lock.
        if let Some(tx) = supervisors.get(&index) {
            // Signal existing supervisor to reset its timer
            let _ = tx.try_send(());
        } else {
            // Spawn new supervisor task
            // Larger buffer to avoid dropping reset signals during bursts
            let (tx, mut rx) = mpsc::channel(64);
            let index_clone = index.clone();
            // From the node config. This used to read `CAMEODB_SUPERVISOR_TIMEOUT_SECS`
            // directly, which meant `[search] supervisor_timeout_secs` in a config file and
            // `--supervisor-timeout-secs` on the command line were both silently ignored —
            // the environment variable worked only because it bypassed the config system
            // entirely. The config layer still maps that variable onto this field, so the
            // env var keeps working; the file and the flag now work too.
            let timeout_dur = Duration::from_secs(self.supervisor_timeout_secs);
            let supervisors_arc = self.supervisors.clone();

            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        result = rx.recv() => {
                            match result {
                                Some(()) => {
                                    // Signal received, timer implicitly resets by continuing loop
                                    continue;
                                }
                                None => {
                                    // Channel closed (shutdown or supervisor map cleared).
                                    // Exit cleanly without attempting commit.
                                    tracing::debug!(index = %index_clone, "Supervisor channel closed, exiting");
                                    break;
                                }
                            }
                        }
                        _ = tokio::time::sleep(timeout_dur) => {
                            // Timer expired without a signal, trigger commit via writer thread
                            let index_inner = index_clone.clone();
                            let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
                            let send_ok = writer_tx.send(StorageCommand::Commit {
                                index: index_inner.clone(),
                                reply: reply_tx,
                            }).await.is_ok();

                            if !send_ok {
                                // Writer thread channel closed (shutdown in progress).
                                // Exit cleanly — no point retrying.
                                tracing::debug!(index = %index_inner, "Supervisor: writer channel closed, exiting");
                                break;
                            }

                            match reply_rx.await {
                                Ok(Ok(())) => {
                                    tracing::debug!(index = %index_inner, "Supervisor committed index via writer thread after idle timeout");
                                    // Leave the map only if nothing arrived while the commit ran.
                                    // A write in that window found this supervisor's sender and
                                    // nudged it rather than arming a new one, and may have landed
                                    // after the commit — exiting would drop its nudge and leave it
                                    // unsearchable until some later write. Every nudge is sent
                                    // under the map's read lock, so under the write lock the
                                    // channel is final: empty means no write is waiting on us, and
                                    // the next one will find no entry and arm a fresh supervisor.
                                    let mut supervisors = supervisors_arc.write().await;
                                    if !rx.is_empty() {
                                        continue;
                                    }
                                    supervisors.remove(&index_clone);
                                    break;
                                }
                                Ok(Err(e)) => {
                                    tracing::error!(index = %index_inner, error = %e, "Supervisor commit failed via writer thread");
                                    // Keep supervisor alive; next signal resets timer, next timeout retries
                                    continue;
                                }
                                Err(_) => {
                                    // Reply channel dropped — writer thread shut down mid-commit.
                                    tracing::debug!(index = %index_inner, "Supervisor: reply dropped (writer shutdown), exiting");
                                    break;
                                }
                            }
                        }
                    }
                }
            });

            supervisors.insert(index, tx);
        }
    }

    /// Handles write requests via the dedicated writer thread.
    ///
    /// The key is `request.id` — the one the caller wrote under and the one the response
    /// reports — rather than anything read out of the body. Reading it from the body meant a
    /// document written in the documented shape, which leaves `id` out of `doc` because the key
    /// is already beside it, had no key here at all and was refused. The body is still read as a
    /// fallback, for a request from a peer whose build predates `WriteRequest::id`.
    pub(super) async fn handle_write(
        &self,
        request: WriteRequest,
    ) -> Result<u64, OrchestratorError> {
        // OPTIMIZATION: Take ownership of doc from request immediately
        let doc = request.doc;

        let id = if !request.id.is_empty() {
            request.id
        } else {
            doc.get("id")
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    OrchestratorError::Validation(
                        "Document must be written with a non-empty 'id' beside its body"
                            .to_string(),
                    )
                })?
                .to_string()
        };

        // OPTIMIZATION: Move doc into Option, no clone
        let json_blob = Some(doc);

        let op = WalOp::Put { id, json_blob };

        // Dispatch to dedicated writer thread via channel
        let seq_id = self
            .handle_write_via_channel(request.index.clone(), op)
            .await?;

        // Signal supervisor for this index
        self.signal_supervisor(request.index).await;

        Ok(seq_id)
    }

    /// Removes one document, through the same writer thread a write goes through.
    ///
    /// Deliberately not its own storage path: the command is a `StorageCommand::Write` carrying a
    /// `WalOp::Delete`, so the delete is coalesced into the same `apply_batch` as the puts that
    /// arrived with it, takes the same single redb transaction, and counts toward the same commit
    /// threshold. A put and a delete of one id in the same batch resolve whichever order they
    /// arrived in: redb keeps the last row, and `apply_batch` coalesces to the last operation
    /// per id and issues the `delete_term` from whether the id had a row *before the batch* —
    /// not from what each individual `insert` displaced, which a delete earlier in the same
    /// batch had already emptied.
    ///
    /// The supervisor signal is what bounds visibility. A delete removes the redb row at once, so
    /// an `id:VALUE` lookup stops finding it immediately, but the Tantivy `delete_term` only takes
    /// effect at the next commit — without this signal a single delete on an otherwise idle index
    /// would wait for unrelated traffic to trigger one.
    pub(super) async fn handle_delete(
        &self,
        index: String,
        id: String,
    ) -> Result<u64, OrchestratorError> {
        let sequence = self
            .handle_write_via_channel(index.clone(), WalOp::Delete { id })
            .await?;

        self.signal_supervisor(index).await;

        Ok(sequence)
    }

    /// Removes many documents in one transaction, through the same writer thread.
    ///
    /// One `StorageCommand::BatchWrite` of `WalOp::Delete`s, so the whole batch is one redb
    /// transaction and one set of Tantivy term deletes — the same path a bulk write takes, which
    /// is why nothing here is specific to deletion beyond the ops it carries.
    pub(super) async fn handle_batch_delete(
        &self,
        index: String,
        ids: Vec<String>,
    ) -> Result<usize, OrchestratorError> {
        if ids.is_empty() {
            return Ok(0);
        }

        let ops: Vec<WalOp> = ids.into_iter().map(|id| WalOp::Delete { id }).collect();
        let expected = ops.len();

        let sequences = self
            .handle_batch_write_via_channel(index.clone(), ops)
            .await?;

        self.signal_supervisor(index).await;

        // One sequence per op, so the count is the storage layer's own answer rather than an
        // echo of what was asked for.
        debug_assert_eq!(sequences.len(), expected);
        Ok(sequences.len())
    }

    /// Handles batch write requests via the dedicated writer thread.
    pub(super) async fn handle_batch_write(
        &self,
        request: BatchWriteRequest,
    ) -> Result<Vec<u64>, OrchestratorError> {
        tracing::debug!(
            shard_id = %self.shard_id,
            docs_count = request.docs.len(),
            "MicroshardActor: Starting batch write"
        );

        let docs = request.docs;
        let index_name = request.index;

        // Group operations by index
        let mut ops_by_index: HashMap<String, Vec<WalOp>> = HashMap::new();

        for doc_payload in docs {
            let wal_op = WalOp::Put {
                id: doc_payload.id,
                json_blob: Some(doc_payload.doc),
            };

            ops_by_index
                .entry(index_name.clone())
                .or_default()
                .push(wal_op);
        }

        tracing::debug!(
            shard_id = %self.shard_id,
            unique_indices = ops_by_index.len(),
            "MicroshardActor: Dispatching batch write to writer thread"
        );

        // Dispatch each index batch to the dedicated writer thread
        let mut all_seq_ids = Vec::new();
        let mut written_indices = Vec::new();
        for (index, wal_ops) in ops_by_index {
            tracing::debug!(
                shard_id = %self.shard_id,
                index = %index,
                ops_count = wal_ops.len(),
                "MicroshardActor: Sending batch to writer thread"
            );

            let seq_ids = self
                .handle_batch_write_via_channel(index.clone(), wal_ops)
                .await?;
            all_seq_ids.extend(seq_ids);
            written_indices.push(index);
        }

        // Signal supervisor AFTER batch completes on the writer thread.
        // At this point counters are already incremented and the writer thread
        // may have already committed via maybe_commit_writer. The supervisor
        // starts its idle-timeout timer from here.
        for index in written_indices {
            self.signal_supervisor(index).await;
        }

        tracing::debug!(
            shard_id = %self.shard_id,
            seq_count = all_seq_ids.len(),
            "MicroshardActor: Batch write fully completed"
        );

        Ok(all_seq_ids)
    }

    /// Deletes all data for an index from this shard's storage.
    ///
    /// Dispatched to the shard's writer thread so it is serialized against writes to the
    /// same index — deletion tears down the writer, sequence counter and redb tables that
    /// an in-flight write is using.
    pub(super) async fn delete_index(
        &self,
        index: &str,
        delete_schema: bool,
    ) -> Result<(), OrchestratorError> {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        self.send_write_command(StorageCommand::DeleteIndex {
            index: index.to_string(),
            delete_schema,
            reply: reply_tx,
        })
        .await?;

        reply_rx
            .await
            .map_err(|_| OrchestratorError::Io(std::io::Error::other("Writer dropped reply")))?
            .map_err(|e: StoreError| match e {
                StoreError::Io(io_err) => OrchestratorError::Io(io_err),
                _ => OrchestratorError::Io(std::io::Error::other(e.to_string())),
            })
    }

    /// Rebuild an index under `schema` if this shard holds none of its documents, on the
    /// writer thread. The documents found: `0` when it rebuilt. See
    /// [`StorageCommand::RebuildIfEmpty`].
    pub(super) async fn rebuild_if_empty(
        &self,
        index: &str,
        schema: &IndexSchema,
    ) -> Result<u64, OrchestratorError> {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        self.send_write_command(StorageCommand::RebuildIfEmpty {
            index: index.to_string(),
            schema: Box::new(schema.clone()),
            reply: reply_tx,
        })
        .await?;

        reply_rx
            .await
            .map_err(|_| OrchestratorError::Io(std::io::Error::other("Writer dropped reply")))?
            .map_err(|e: StoreError| match e {
                StoreError::Io(io_err) => OrchestratorError::Io(io_err),
                _ => OrchestratorError::Io(std::io::Error::other(e.to_string())),
            })
    }
}

// ============================================================================
// Remote Message Implementations for Distributed Actors
// ============================================================================

/// Message implementation for MicroshardActor search operations
#[remote_message("cameo.microshard.search")]
impl Message<SearchRequest> for MicroshardActor {
    type Reply = Result<SearchReply, RemoteError>;

    async fn handle(
        &mut self,
        msg: SearchRequest,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.handle_search(msg)
            .await
            .map(|result| SearchReply {
                hits: result.hits,
                total_hits: result.total_hits,
                discarded: result.discarded,
                approximate_sort: result.approximate_sort,
                narrowed_default_fields: result.narrowed_default_fields,
                emptied: result.emptied,
            })
            .map_err(RemoteError::from)
    }
}

/// Message implementation for MicroshardActor write operations
#[remote_message("cameo.microshard.write")]
impl Message<WriteRequest> for MicroshardActor {
    type Reply = Result<WriteReply, RemoteError>;

    async fn handle(
        &mut self,
        msg: WriteRequest,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.handle_write(msg)
            .await
            .map(|sequence_id| WriteReply {
                sequence: sequence_id,
            })
            .map_err(RemoteError::from)
    }
}

/// Message implementation for MicroshardActor batch write operations
#[remote_message("cameo.microshard.batch_write")]
impl Message<BatchWriteRequest> for MicroshardActor {
    type Reply = Result<BatchWriteReply, RemoteError>;

    async fn handle(
        &mut self,
        msg: BatchWriteRequest,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.handle_batch_write(msg)
            .await
            .map(|sequence_ids| BatchWriteReply {
                items_written: sequence_ids.len() as u64,
                errors: vec![],
            })
            .map_err(RemoteError::from)
    }
}

/// Message implementation for MicroshardActor shard statistics operations
#[remote_message("cameo.microshard.get_stats")]
impl Message<GetShardStats> for MicroshardActor {
    type Reply = Result<storage::ShardStatsSnapshot, RemoteError>;

    async fn handle(
        &mut self,
        msg: GetShardStats,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.handle_get_stats(msg).await.map_err(|e| e.into())
    }
}

/// Message implementation for MicroshardActor shutdown operations
impl Message<ShutdownShard> for MicroshardActor {
    type Reply = Result<(), RemoteError>;

    async fn handle(
        &mut self,
        _msg: ShutdownShard,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        tracing::info!(shard_id = %self.shard_id, "MicroshardActor: Shutting down shard");

        // Step 1: Stop the dedicated writer thread (drains queued commands first)
        self.shutdown_writer().await;

        // Step 2: Shutdown storage (commit pending tantivy writers, etc.)
        if let Some(store) = self.store.as_ref() {
            let store_clone = store.clone();
            tokio::task::spawn_blocking(move || {
                if let Err(e) = store_clone.shutdown() {
                    tracing::error!(error = %e, "Failed to shutdown storage");
                }
            })
            .await
            .map_err(|e| RemoteError::Other(format!("Shutdown task failed: {}", e)))?;
        }

        // Step 3: Explicitly drop store reference to ensure database file is closed
        // This is critical for clean shutdown - ensures the redb Database is dropped
        // and file handles released before the actor returns.
        self.store = None;
        tracing::info!(shard_id = %self.shard_id, "MicroshardActor: Store dropped, database closed");

        tracing::info!(shard_id = %self.shard_id, "MicroshardActor: Shutdown completed");
        Ok(())
    }
}
