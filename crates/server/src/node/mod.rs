//! # NodeOrchestrator - Distributed Node Management Actor
//!
//! The NodeOrchestrator is the central actor responsible for managing microshard actors
//! within a single CameoDB node. It handles shard lifecycle, discovery, and routing.
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────┐
//! │           NodeOrchestrator              │
//! ├─────────────────────────────────────────┤
//! │ - identity: NodeIdentity                │
//! │ - shards: HashMap<Uuid, MicroshardActor> │
//! │ - config: NodeConfig                    │
//! └─────────────────────────────────────────┘
//! ```
//!
//! # Thread topology
//!
//! Local shards are held by value, not as `ActorRef`s, so calls into them are plain async
//! method calls — no mailbox hop. The threads a request actually crosses:
//!
//! - **main runtime** — axum, coordinator, libp2p swarm.
//! - **`orch-worker-N`** — one dedicated OS thread per worker (`cpu_cores` when hash-space
//!   aligned), each running a `current_thread` runtime. Requests hop here from the main
//!   runtime via a per-worker mpsc queue.
//! - **`writer-shard-<id>`** — one per shard, pinned to `xxh3(shard_id) % cores`. All writes
//!   and commits for that shard are serialized here. Writes hop worker → writer → back.
//! - **`cameodb-read`** — shared blocking pool sized by `search_threads`. Reads hop
//!   worker → read pool → back. Deliberately unpinned: reads leave the writer's core so
//!   they cannot compete with it, which is why the hash-space alignment between worker and
//!   writer cores applies to the write path only.
//! - **`warmup-shard-<id>`** — one per shard. Runs startup warmup, then serves re-warm
//!   requests posted by the writer thread after each commit. Never on a request path.
//! - **tantivy, per open index** — `indexer_num_threads` + `merge_num_threads` per
//!   `IndexWriter`. Thread count therefore scales with the number of *open* indices, not
//!   just shards. Nothing else is per-index: readers use `ReloadPolicy::Manual` (no
//!   `thread-tantivy-meta-file-watcher` polling meta.json every 500ms per index) and register
//!   no `Warmer` (no GC thread per index). Reloading and warming are both driven explicitly —
//!   reloads from `commit_index`, warming from the shard's warmup thread.
//!
//! Note that `core_affinity::set_for_current` is a no-op on macOS, so every pinning path
//! here degrades to unpinned threads on that platform and only takes effect on Linux.
//!
//! # The modules
//!
//! `node_orchestrator.rs` was one file of 13,669 lines before L11 split it. What the split is
//! *for* is that each of these can be read, and changed, without the other five in view:
//!
//! - [`orchestrator`] — the dispatch core: [`NodeOrchestrator`], [`OrchestratorEngine`], the
//!   worker loop and the `Message` impls, plus the write-path machinery the two lanes share.
//! - [`admission`] — what the node lets in and the counters it decides on: [`OpClass`],
//!   [`ServiceHistogram`], [`QueueLoad`], [`MailboxLane`]/[`MailboxSlot`], and the
//!   `/_admin/workers` report types. One subsystem, because its invariants are properties of
//!   the whole set rather than of any one type.
//! - [`routing`] — the routing-key ladder: the one rule that decides which shard a document
//!   lands on, in one place so it cannot disagree with itself.
//! - [`router`] — [`RouterActor`], the front door that decides local against remote.
//! - [`search`] — the scatter/gather across shards and peers, and hit ordering.
//! - [`shard`] — [`MicroshardActor`] and the writer-thread boundary.

use rayon::prelude::*;
use std::collections::HashSet;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::Result;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// Re-export SortSpec and SortOrder from storage crate
use cluster::{ConsistentRing, IdentityError};
use serde_json::Value as JsonValue;
pub use storage::SortSpec;
use storage::{IndexSchema, StoreError, TantivyFieldType, WalOp};

mod admission;
mod orchestrator;
mod quota;
mod router;
mod routing;
mod search;
mod shard;

// Inside `node/`, every submodule reaches its siblings through `use super::*`, so these globs
// stay — but scoped to `node` and no wider. That is what makes the list below the whole
// boundary: an item the rest of the crate may use has to be named there on purpose, and a new
// `pub(super)` item cannot escape by being swept up in a glob. Before L11's split this file was
// one module and the question did not arise; after it, `pub use <submodule>::*` re-exported 129
// submodule items crate-wide, of which 15 are ever used outside `node/` (O1).
pub(in crate::node) use admission::*;
pub(in crate::node) use orchestrator::*;
pub(in crate::node) use quota::*;
pub(in crate::node) use routing::*;
pub(in crate::node) use search::*;
pub(in crate::node) use shard::*;

// Everything outside `node/` may use from `node/`'s submodules. Adding a line here is the
// deliberate act of widening a boundary; nothing else in `node/` is reachable as
// `crate::node::*` unless it is declared in this file.
pub(crate) use admission::{OpClass, QueueLoad, WorkerPoolReport};
pub(crate) use orchestrator::{NodeOrchestrator, lookup_peer_orchestrator};
pub(crate) use quota::TenantQuotas;
pub(crate) use router::{RouterActor, ShardAffineConfig, StreamingSearchConfig};
pub(crate) use search::{
    APPROXIMATE_SORT_FIELD, DISCARDED_CLAUSES_FIELD, NARROWED_DEFAULT_FIELDS, SearchWindow,
    order_hit_blocks, renumber_reasons,
};
pub(crate) use shard::{ReadPoolHealth, WriterLiveness};

/// Channel capacity for per-shard dedicated writer threads.
/// Each MicroshardActor sends StorageCommands through this bounded channel.
pub(crate) const SHARD_WRITER_CHANNEL_CAPACITY: usize = 1024;

/// Capacity of the writer → warmup-thread re-warm request channel.
///
/// Small on purpose. Requests are posted with `try_send` and dropped when full, and a dropped
/// request costs nothing: the index warms on its next commit or on its first query. A deep
/// queue would only accumulate stale requests for generations that have already been replaced.
pub(crate) const WARM_REQUEST_CAPACITY: usize = 64;

/// Channel capacity for the orchestrator worker pool job queue.
/// Quadruple the shard writer capacity to allow more buffering of incoming requests
/// while workers are dispatching to shard writer threads.
pub(crate) const ORCHESTRATOR_WORKER_QUEUE_CAPACITY: usize = SHARD_WRITER_CHANNEL_CAPACITY * 4;

/// Operations one worker may have in flight at once. Total in-flight is this times
/// `worker_count`.
///
/// A worker used to run exactly one, which made `worker_count` the node's whole operation
/// concurrency — far below what the machine could carry, because an operation is mostly spent
/// awaiting a shard writer rather than on CPU. This is the width of that pipeline.
///
/// **Eight because eight measured best**, not because it is a round number. Swept 1/2/4/8/16
/// on an 8-core Linux node with 8 shards at concurrency 64, three repeats each (ROADMAP
/// "Worker concurrency, measured"): throughput climbs 4 178 → 7 118 ok/s from 1 to 8, then
/// *falls* to 6 444 at 16, and every width-8 repeat beat every width-16 repeat. Past the
/// point where every shard writer already has work queued, more in-flight operations only
/// move the queue from the channel into memory — latency and resident bytes, no throughput.
///
/// It is a constant rather than config because the useful value follows the shape of the
/// pipeline — one writer thread per shard, serialising — rather than anything a deployment
/// knows about itself. The number to watch instead is `in_flight` against
/// `in_flight_capacity` on `/_admin/workers`.
pub(crate) const ORCHESTRATOR_WORKER_MAX_IN_FLIGHT: usize = 8;

/// How much of the actor-mailbox lane runs at once: one, because the actor serialises
/// everything it takes.
///
/// The divisor in Little's law for that lane, and the reason it needs its own gate rather than
/// a share of the worker pool's. The pool's width is `workers x in-flight`; this is 1, and a
/// bulk write's service time is three orders of magnitude above a point search's — folding the
/// two into one counter would mis-predict both. See [`RouterActor::mailbox_load`].
pub(crate) const MAILBOX_LANE_WIDTH: usize = 1;

tokio::task_local! {
    /// When the request being served reached this node, scoped to the task running its
    /// handler.
    ///
    /// Stamped by a layer just inside `TimeoutLayer` (`routes.rs`), so the clock it starts
    /// is the same one the client's deadline runs on — auth has already happened, and
    /// everything after it (body read, parse, dispatch, queue wait) counts against the same
    /// budget. The deadline checks F7 put at admission and at dequeue read it through
    /// [`request_started_at`]; stamping it at dispatch instead would grant every job a fresh
    /// budget after whatever the body cost, which for a max-size record is the whole timeout.
    ///
    /// A task-local rather than a field on the request or the op: the value is read at job
    /// build inside `handle_client_op`, sixteen call sites upstream of it, and none of them
    /// has any use for it themselves. `tokio::spawn` does *not* inherit the scope, so a path
    /// that hands the request to a new task re-enters it by hand — streaming search dispatch
    /// with the stamp it read before spawning, the worker's op task scoped to the job's
    /// `arrived_at` — or its deadline checks start from zero. Calls that arrive
    /// another way — a peer's forwarded op over libp2p, an internal ask, a test — run outside
    /// any scope and get `Instant::now()` from the helper, which is the dispatch-time
    /// behaviour this replaced.
    pub(crate) static REQUEST_STARTED_AT: Instant;
}

/// When the request being served reached this node, or `Instant::now()` for work that did
/// not arrive through the HTTP middleware (a peer's forwarded op, an internal call, a test).
/// See [`REQUEST_STARTED_AT`].
pub(crate) fn request_started_at() -> Instant {
    REQUEST_STARTED_AT
        .try_with(|started| *started)
        .unwrap_or_else(|_| Instant::now())
}

// ============================================================================
// Remote Actor Naming Constants
// ============================================================================

/// Generate the remote actor name for a NodeOrchestrator.
/// A reply from a remote peer, with the verdict of a handler error kept.
///
/// The distinction every caller needs and none of them used to make: `HandlerError` means the
/// peer received the message, ran it, and answered with an error — its answer, already carrying
/// the verdict that decides the caller's status. Every other variant is the message not getting
/// there.
///
/// Stringifying the whole `SendError` collapsed the two, so a document whose type a peer's schema
/// will never accept was indistinguishable from a peer that had gone away: retried, wrapped as a
/// routing failure, answered `500`, and reported to the coordinator as grounds to redial the
/// cluster. Four call sites each wrote that same `map_err` by hand, which is why they all had the
/// same bug and why this is one function now.
pub(crate) fn remote_answer<T>(
    reply: Result<T, kameo::error::RemoteSendError<OrchestratorError>>,
) -> Result<T, OrchestratorError> {
    reply.map_err(|err| match err {
        kameo::error::RemoteSendError::HandlerError(answered) => answered,
        never_arrived => OrchestratorError::Io(std::io::Error::other(never_arrived.to_string())),
    })
}

pub fn orchestrator_remote_name(node_id: &Uuid) -> String {
    format!("orchestrator-{}", node_id)
}

// ============================================================================
// Date Parsing Helper Functions
// ============================================================================

// ============================================================================
// Schema Validation Types
// ============================================================================

/// Result of validating a single document
#[derive(Debug, Clone)]
pub struct SchemaValidationResult {
    pub needs_evolution: bool,
    pub new_fields: Vec<(String, TantivyFieldType)>,
    pub validation_error: Option<String>,
}

/// Summary of validation results for a batch of documents
#[derive(Debug, Clone)]
pub struct SchemaValidationSummary {
    pub total_docs: usize,
    pub valid_docs: usize,
    pub evolution_needed: bool,
    pub all_new_fields: HashSet<(String, TantivyFieldType)>,
    /// Each document that failed validation, by its position in the batch, with the reason.
    ///
    /// The position is what lets a bulk write keep the documents that validated and drop the
    /// ones that did not; a reason on its own can only be logged.
    pub errors: Vec<(usize, String)>,
}

/// Configuration for a CameoDB node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    /// Base path for all node data storage
    pub storage_path: PathBuf,
    /// Sorted list of candidate data paths for shard placement
    pub storage_paths: Vec<PathBuf>,
    /// Maximum number of shards this node can host
    pub max_shards: usize,
    /// Tantivy indexer memory configuration (per shard)
    pub indexer_memory_min_mb: usize,
    pub indexer_memory_max_mb: usize,
    /// Total memory limit (in MB) for coordinating per-shard cache sizing
    pub total_memory_limit_mb: usize,
    /// Largest number of indexes held open at once across the node; `0` means no cap.
    pub max_open_indexes: usize,
    /// Memory pressure threshold used for deriving usable cache capacity
    pub memory_pressure_threshold_percent: u8,
    /// Number of threads for the dedicated read (search/stats) runtime
    pub search_threads: usize,
    /// The node's resolved HTTP request timeout, in seconds.
    ///
    /// Not used to time anything out — it is the budget a queued read is measured against, so
    /// a read that has already outlived the request that asked for it is refused rather than
    /// run. `0` disables the check. See [`dispatch_read_pool`].
    pub request_timeout_secs: u64,
    /// Enable WAL fsync for durability
    pub wal_sync: bool,
    /// Default batch size for smart commit calculations
    pub default_batch_size: usize,
    /// Number of indexing worker threads per tantivy IndexWriter (default: 1)
    pub indexer_num_threads: usize,
    /// Number of background merge (compaction) threads per IndexWriter (default: 2)
    pub merge_num_threads: usize,
    /// Timeout in seconds for writer thread to drain pending commands during shutdown
    /// Increased from 10s to 30s to handle large coalesced batches
    pub writer_shutdown_timeout_secs: u64,
    /// Seconds of write inactivity on an index before its supervisor commits it.
    ///
    /// The idle commit: the writes that end a burst have nothing after them to trigger the
    /// interval check, and would otherwise sit unsearchable until the next write.
    pub supervisor_timeout_secs: u64,
    /// Longest an index's oldest uncommitted write waits for a commit while writes keep
    /// arriving, in milliseconds; `0` commits by operation count alone. Passed to each shard's
    /// store as `StorageConfig::commit_interval_ms`.
    pub commit_interval_ms: u64,
    /// Pin per-shard writer threads to the core given by the shard's dense ordinal.
    /// Improves cache locality and reduces cross-core wakeups under heavy write load.
    /// Default: true — measured neutral on throughput and latency, and a no-op where
    /// pinning is unsupported (macOS). The measured regressions were the *dispatch*
    /// flags below, which stay off.
    pub writer_core_affinity: bool,
    /// Enable shard-affine worker dispatch (default: false).
    /// When enabled, operations targeting the same shard are routed to the same
    /// orchestrator worker via the shard's ordinal, reducing cross-core wakeups when
    /// writer pinning is also enabled.
    pub shard_affine_dispatch: bool,

    /// Pin orchestrator worker tasks to CPU cores as dedicated OS threads (Stage 2e).
    /// Requires `shard_affine_dispatch` AND `writer_core_affinity` to take effect.
    /// Default: false.
    pub worker_core_affinity: bool,

    /// Whether this node is part of a cluster (`network.cluster.enabled`).
    ///
    /// Static for the life of the process, which is the point: it lets the paths that only
    /// exist to agree with other nodes take a standalone arm without asking the coordinator
    /// whether there are any. A single node is the whole system, so its own answer is the
    /// cluster's answer and there is nothing to wait for.
    pub clustered: bool,

    /// Whether a write to an index with no schema anywhere may create it
    /// (`security.implicit_index_creation`).
    ///
    /// Carried here for the same reason `clustered` is: the write path decides on it inside
    /// the orchestrator, which never sees the file-level config. When `false`, the sampling
    /// that types an index from its first documents is refused and the index has to be
    /// created explicitly.
    pub implicit_index_creation: bool,

    /// What each tenant's indexes may add up to (`security.tenants`).
    ///
    /// Carried here for `implicit_index_creation`'s reason: both ceilings are decided inside the
    /// orchestrator — the index count at the mint, bytes on the write — and the orchestrator
    /// never sees the file-level config. Empty by default, which is no ceiling for anyone.
    #[serde(default)]
    pub tenant_quotas: std::collections::HashMap<String, crate::auth::TenantQuota>,

    /// What one query may cost (`[security.limits]`: `min_prefix_length`,
    /// `expand_unqualified_prefix`).
    ///
    /// Carried to every shard's `StorageConfig`, since the rewrites that apply it run in storage.
    #[serde(default = "crate::ratelimit::default_query_policy")]
    pub query_policy: storage::QueryPolicy,
}

impl Default for NodeConfig {
    fn default() -> Self {
        let default_path = PathBuf::from("./data/cameodb");
        Self {
            storage_path: default_path.clone(),
            storage_paths: vec![default_path],
            max_shards: 8,
            request_timeout_secs: 0,
            indexer_memory_min_mb: 16,
            indexer_memory_max_mb: 256,
            total_memory_limit_mb: 2048,
            max_open_indexes: 0,
            memory_pressure_threshold_percent: 80,
            search_threads: 8,
            wal_sync: true,
            default_batch_size: 1000,
            indexer_num_threads: 1,
            merge_num_threads: 2,
            writer_shutdown_timeout_secs: 30,
            supervisor_timeout_secs: 3,
            commit_interval_ms: 2000,
            writer_core_affinity: true,
            shard_affine_dispatch: false,
            worker_core_affinity: false,
            // Standalone by default, matching `network.cluster.enabled`.
            clustered: false,
            implicit_index_creation: true,
            tenant_quotas: std::collections::HashMap::new(),
            query_policy: crate::ratelimit::default_query_policy(),
        }
    }
}

/// Errors that can occur during node orchestration operations.
#[derive(Debug, thiserror::Error)]
pub enum OrchestratorError {
    #[error("identity error: {0}")]
    Identity(#[from] IdentityError),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// The request was refused before it ran — a document the schema rejects, a routing
    /// key the index needs that was not sent, an id the body did not carry.
    ///
    /// `BadRequest` wherever it is answered. `InvalidInput`/`InvalidData` carried this
    /// across the actor boundary as an `io::Error` kind before, and the kind match was
    /// done twice over to learn it — once on the way into `RemoteError`, once in
    /// `verdict`.
    #[error("{0}")]
    Validation(String),

    /// A component this node needs for the operation is not up — the shard map is
    /// empty, a writer channel or store handle is not initialized, a pool is absent.
    ///
    /// `Unavailable`, not `ServerFault`: the node is not broken, it is not ready, and a
    /// retry may succeed once it is — the same "not now" [`Self::PeerUnreachable`]
    /// carries for a peer. These were `NotFound`-kind `io::Error`s before, which read
    /// as `500`: the node reported itself at fault for still starting up.
    #[error("{0}")]
    NotReady(String),

    /// A thing this node was asked to use is not there — a shard the ring selected that
    /// the map does not hold, a store to write a schema into, an owner for a routed
    /// shard.
    ///
    /// `ServerFault` rather than `NotFound`: the caller did not name the missing thing —
    /// this is the node's internals disagreeing with themselves, not an absent index,
    /// and a `404` would say otherwise. Also `NotFound`-kind `io::Error`s before, which
    /// the verdict read as `500` anyway; the variant names what the kind only implied.
    #[error("{0}")]
    Missing(String),

    #[error("storage error: {0}")]
    Storage(#[from] StoreError),

    #[error("shard limit exceeded: {current}/{max}")]
    ShardLimitExceeded { current: usize, max: usize },

    #[error("shard already exists: {shard_id}")]
    ShardAlreadyExists { shard_id: Uuid },

    /// The caller asked to sort by a field the index cannot order on.
    ///
    /// Deliberately its own variant rather than the [`StoreError::FieldNotFound`] the engine
    /// would raise: this is decided before the shards are asked, and the HTTP surface has to
    /// answer 400 for it. Routed through `Io` — as every error crossing an actor boundary is —
    /// it would have arrived as a per-shard failure string inside a 200, which is how this was
    /// reported in the first place.
    #[error("cannot sort by '{field}': {reason}")]
    UnsortableField { field: String, reason: String },

    /// Every clause was discarded, so the query that reached the engine was empty.
    ///
    /// A 400 for the reason [`Self::UnsortableField`] is one: the engine cannot run what was
    /// asked. Answered as a 200 it is a zero indistinguishable from a search that ran and
    /// matched nothing, which is the reading that makes it dangerous.
    #[error("no clause of this query can run against this index: {notes}")]
    UnrunnableQuery { notes: String },

    /// The read waited longer in the pool queue than the request that asked for it was given,
    /// so it was dropped at dequeue instead of run.
    ///
    /// Tokio never cancels a blocking closure: dropping its `JoinHandle` neither stops work
    /// that has started nor dequeues work that has not. So when the HTTP timeout fires, the
    /// search behind it still runs, still occupies a read thread, and still produces an answer
    /// — for a client that is already gone. Under sustained overload that is *every* search,
    /// which is why goodput went to zero rather than degrading (ROADMAP F7, measured on two
    /// machines). The work cannot be cancelled, so it is refused before it starts.
    ///
    /// `Unavailable` rather than a fault: nothing about the request is wrong, the node was
    /// simply behind, and a retry is the right move. That also makes it the same `503` the
    /// admission guard already answers, so a client under overload sees one behaviour.
    ///
    /// Named for the read pool, where it was first raised, but the worker pool's dequeue check
    /// raises it for every op it sheds — a single write included — so the message names the
    /// request rather than claiming every one was a read.
    #[error(
        "request abandoned: spent {waited_ms}ms of a {budget_ms}ms budget before a worker could start it"
    )]
    ReadDeadlineExpired { waited_ms: u64, budget_ms: u64 },

    /// The backlog already in front of this request could not clear inside its budget, so it
    /// was refused before being queued rather than after waiting out the wait.
    ///
    /// The same decision as [`Self::ReadDeadlineExpired`], taken at the door instead of at a
    /// worker. That one is the backstop for when this one's prediction was wrong; this one is
    /// what makes the refusal cheap, because everything between the two — the body, the JSON,
    /// the permit, the channel hop — is work spent on a request that was never going to be
    /// answered in time.
    ///
    /// `Unavailable` for the same reason, and answered as the same `503`: nothing about the
    /// request is wrong, the node is behind, and a retry is the right move.
    #[error("overloaded: a {predicted_wait_ms}ms backlog against a {budget_ms}ms request")]
    Overloaded {
        predicted_wait_ms: u64,
        budget_ms: u64,
    },

    /// No shard could run the query, so the empty result it produced is not an answer.
    ///
    /// A scatter-gather reports a failed shard as a partial outage — the hits it did get, plus
    /// `errors` naming what it did not — which is right when *some* shard answered. When none
    /// did, the same shape is a `200` with an empty `hits` array and the reason in a key most
    /// callers never read, indistinguishable from a search that ran everywhere and matched
    /// nothing.
    ///
    /// `caller_error` decides the status, and the distinction is real: a sort naming a column the
    /// index never built is a request to fix, while a shard that could not open its index is this
    /// node's problem and says nothing about the request.
    #[error("no shard could run this query against '{index}': {reasons}")]
    NoShardAnswered {
        index: String,
        reasons: String,
        caller_error: bool,
    },

    /// This node cannot establish what the cluster's schema for an index is, so it will not
    /// invent one.
    ///
    /// A write to an index this node has never seen builds the schema by sampling the documents.
    /// On one node that is the feature that makes semi-structured input work. In a cluster it is
    /// how three nodes came to hold three different schemas for one index: whichever node
    /// received `PUT /_config` kept the declaration, and every other node typed the same index
    /// from the first document that reached it. The types then diverge permanently, because a
    /// tantivy column is built once.
    ///
    /// So sampling is allowed only when this node can be sure no peer already holds a
    /// declaration — which means being able to ask all of them. When it cannot, refusing is the
    /// only answer that is not a guess, and it is retryable: the peers are expected back.
    ///
    /// Answered as `503`, not `500`. Nothing is wrong with the request and nothing is broken
    /// here; the cluster is not currently whole enough to agree on a new schema.
    #[error("cannot establish the cluster schema for '{index}': {reason}")]
    SchemaUnconfirmed { index: String, reason: String },

    /// A forwarded write arrived with a schema *stamp* for an index this node holds no schema
    /// for, so there is nothing to match the stamp against.
    ///
    /// Answered rather than asked, which is the whole point: the receiver cannot canvass its
    /// peers from inside a write without waiting on mailboxes those peers are using to run the
    /// forwarded writes. Reporting back costs the sender one retry carrying the body, once per
    /// node per index, instead of putting the schema on every forward for ever.
    ///
    /// Never reaches a client: the forwarding node recognises it by verdict and retries. If it
    /// somehow does, it is a `503` — nothing is wrong with the request.
    #[error("no schema for '{index}' on this node; resend the write carrying the schema body")]
    SchemaBodyRequired { index: String },

    /// This node is creating the schema for `index` right now and has not saved it yet — the
    /// answer to a peer's [`ClientOp::GetRawSchema`] canvass in that window.
    ///
    /// Never "none": the asker is deciding whether it may sample a schema of its own, and two
    /// nodes that each heard "none" mint two schemas for one index. A new peer reads it as a
    /// rival for the same index and settles it by node id (see `MintAfterCanvass`); an older one
    /// reads an unknown verdict as a fault and refuses its write, which is also safe.
    ///
    /// Never reaches a client: the canvass consumes it. If it somehow does, it is a `503`.
    #[error("node {node} is creating the schema for '{index}' right now")]
    SchemaBeingMinted { index: String, node: Uuid },

    /// A tenant's quota refuses what this request would add — another index past
    /// `max_indexes`, or more data once their indexes occupy `max_bytes`.
    ///
    /// Its own verdict and a `403`, not a `400`: nothing about the request is malformed, and the
    /// same request succeeds once the tenant frees room or the operator raises the ceiling. Not a
    /// `503` either, because retrying unchanged will not help — that is what separates it from
    /// every "not now" above.
    #[error("quota exceeded for tenant '{tenant}': {detail}")]
    QuotaExceeded { tenant: String, detail: String },

    /// The node that owns this operation could not be reached.
    ///
    /// Not a fault here and not the caller's mistake: a peer is down or has not finished
    /// joining, and it is expected back. Reported as `500` before, which says this node is
    /// broken — it is not, and the advice a `500` carries is wrong, because the one thing that
    /// will fix this is the retry a `500` discourages.
    #[error("{message}")]
    PeerUnreachable { message: String },

    /// A verdict another node reached, carried across the boundary intact.
    ///
    /// See [`RemoteVerdict`]. The variant a peer actually raised cannot be rebuilt here — the
    /// errors it wraps do not implement serde and there would be nothing to do with them if they
    /// did — but the verdict is the part that matters, and answering the caller the same way
    /// whether their request ran here or one hop away is the whole point of keeping it.
    #[error("{message}")]
    Remote {
        verdict: RemoteVerdict,
        message: String,
    },
}

/// How an error should be answered, once it is the answer to somebody's request.
///
/// One classification, used twice: by the HTTP surface to pick a status, and by
/// [`OrchestratorError`]'s wire form so a peer's verdict survives the hop. They were two, and
/// two would drift — the wire form would keep saying "server fault" for a refusal the HTTP layer
/// had learned to call a bad request, and nobody would notice until a routed write answered
/// differently from a local one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoteVerdict {
    /// The caller sent something this node will not accept. Nothing to retry.
    BadRequest,
    /// What the caller named is not here.
    NotFound,
    /// Nothing is wrong with the request and nothing is broken; the node cannot serve it *now*.
    /// Worth retrying, and the one verdict that says so.
    Unavailable,
    /// A fault on the node that answered. The caller learns nothing useful from the detail, so
    /// the HTTP surface withholds it and the operator reads it in the log.
    ServerFault,
    /// The peer needs the schema body before it can run this write — see
    /// [`OrchestratorError::SchemaBodyRequired`]. Retryable, and only by a caller that resends
    /// with the body attached, which is why it is not `Unavailable`.
    SchemaRequired,
    /// A tenant quota refuses the request — see [`OrchestratorError::QuotaExceeded`]. Not the
    /// caller's mistake and not retryable as sent; room has to be made first.
    QuotaExceeded,
    /// The peer is creating this index's schema right now — see
    /// [`OrchestratorError::SchemaBeingMinted`]. Its own verdict because a canvassing node acts
    /// on it: the peer is a rival for the same index, not a peer that failed to answer.
    Minting,
}

impl RemoteVerdict {
    /// The tag this verdict travels under. Short, and stable: it is a wire value.
    pub(crate) fn tag(self) -> &'static str {
        match self {
            RemoteVerdict::BadRequest => "bad-request",
            RemoteVerdict::NotFound => "not-found",
            RemoteVerdict::Unavailable => "unavailable",
            RemoteVerdict::ServerFault => "server-fault",
            RemoteVerdict::SchemaRequired => "schema-required",
            RemoteVerdict::QuotaExceeded => "quota-exceeded",
            RemoteVerdict::Minting => "minting",
        }
    }

    pub(crate) fn from_tag(tag: &str) -> Option<Self> {
        match tag {
            "bad-request" => Some(RemoteVerdict::BadRequest),
            "not-found" => Some(RemoteVerdict::NotFound),
            "unavailable" => Some(RemoteVerdict::Unavailable),
            "server-fault" => Some(RemoteVerdict::ServerFault),
            "schema-required" => Some(RemoteVerdict::SchemaRequired),
            "quota-exceeded" => Some(RemoteVerdict::QuotaExceeded),
            "minting" => Some(RemoteVerdict::Minting),
            _ => None,
        }
    }
}

impl OrchestratorError {
    /// How this error should be answered, wherever it is answered.
    ///
    /// Every judgement is on the error's *type*. Reading the message text is what this replaced,
    /// and it could not tell a caller's mistake from the node's: any text containing "not found"
    /// answered 404, so a write that failed because a *shard* was missing told the caller their
    /// index did not exist.
    pub fn verdict(&self) -> RemoteVerdict {
        match self {
            // The one thing that is actually absent rather than broken.
            Self::Storage(StoreError::IndexNotFound(_)) => RemoteVerdict::NotFound,

            // Neither the caller's fault nor a fault at all — see `SchemaUnconfirmed` and
            // `PeerUnreachable`. Both are "not now", and both are worth retrying. A writer that
            // panicked and was reset is the same shape: the document was not applied, the writer
            // is rebuilt on the next write, and the caller should retry.
            Self::SchemaUnconfirmed { .. }
            | Self::PeerUnreachable { .. }
            | Self::ReadDeadlineExpired { .. }
            | Self::Overloaded { .. }
            | Self::NotReady(_)
            | Self::Storage(StoreError::WriterPanicked(_))
            | Self::Storage(StoreError::WriterClosed(_)) => RemoteVerdict::Unavailable,

            Self::Validation(_) => RemoteVerdict::BadRequest,

            Self::QuotaExceeded { .. } => RemoteVerdict::QuotaExceeded,

            // Its own verdict because the forwarding node has to act on it and must not confuse
            // it with any other "not now": the retry that answers it carries something extra,
            // and an ordinary `Unavailable` retry would repeat the same insufficient message.
            Self::SchemaBodyRequired { .. } => RemoteVerdict::SchemaRequired,

            Self::SchemaBeingMinted { .. } => RemoteVerdict::Minting,

            Self::UnsortableField { .. } | Self::UnrunnableQuery { .. } => {
                RemoteVerdict::BadRequest
            }

            // A query no shard could run carries its own verdict: the shards refused what was
            // asked, or they failed. Only the first is the caller's to fix.
            Self::NoShardAnswered { caller_error, .. } => {
                if *caller_error {
                    RemoteVerdict::BadRequest
                } else {
                    RemoteVerdict::ServerFault
                }
            }

            // `InvalidData` as well as `InvalidInput`: every producer of the former is a document
            // the caller sent that the schema refuses — a missing inner `id`, a type that does
            // not match a declared field.
            Self::Io(io) => match io.kind() {
                std::io::ErrorKind::InvalidInput | std::io::ErrorKind::InvalidData => {
                    RemoteVerdict::BadRequest
                }
                _ => RemoteVerdict::ServerFault,
            },

            // A value the field's type cannot hold is the document's fault too, as is a query
            // that will not parse and a name no index may have.
            Self::Storage(
                StoreError::InvalidFieldValue { .. }
                | StoreError::QueryParser(_)
                | StoreError::InvalidIndexName(_),
            ) => RemoteVerdict::BadRequest,

            // Already judged, one hop away. Kept rather than re-derived.
            Self::Remote { verdict, .. } => *verdict,

            _ => RemoteVerdict::ServerFault,
        }
    }
}

/// Separates the verdict tag from the message on the wire.
///
/// ASCII unit separator: a control character, so no error message this node produces can contain
/// one and be mistaken for a tagged form.
pub(crate) const VERDICT_SEPARATOR: char = '\u{1f}';

// A display string still, because the errors this wraps — `IdentityError`, `StoreError` — do not
// implement serde and rebuilding them on the far side would serve nothing. What is added is the
// verdict, prefixed: it decides the status the caller sees, and without it every error a peer
// raised arrived as an unclassified `Io` and answered `500`. A type mismatch in a document was
// reported to the caller as this node's fault, and a schema that could not be agreed as a defect
// rather than as something to retry.
//
// Mixed versions degrade to the old behaviour rather than breaking: a node that predates this
// sends an untagged string, which is read below as a server fault, and reads a tagged one as an
// opaque message — the same `500` it would have answered anyway.
impl Serialize for OrchestratorError {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&format!(
            "{}{VERDICT_SEPARATOR}{}",
            self.verdict().tag(),
            self
        ))
    }
}

impl<'de> Deserialize<'de> for OrchestratorError {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        match s.split_once(VERDICT_SEPARATOR) {
            Some((tag, message)) => match RemoteVerdict::from_tag(tag) {
                Some(verdict) => Ok(OrchestratorError::Remote {
                    verdict,
                    message: message.to_string(),
                }),
                // A tag this build does not know is a newer peer naming a verdict that did not
                // exist here yet. The message is still the answer; only the classification is
                // unavailable, so it takes the cautious one.
                None => Ok(OrchestratorError::Remote {
                    verdict: RemoteVerdict::ServerFault,
                    message: message.to_string(),
                }),
            },
            None => Ok(OrchestratorError::Remote {
                verdict: RemoteVerdict::ServerFault,
                message: s,
            }),
        }
    }
}

/// Document payload for write operations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocPayload {
    pub id: String,
    #[serde(default)]
    pub routing_key: Option<String>,
    pub doc: JsonValue,
}

/// One document to remove, as a bulk delete names it.
///
/// A bare id is the whole of it on an index that routes by the key. `routing_key` carries the
/// same value the write used where the index routes by something else, and is per item because a
/// batch may span tenants — see `effective_delete_routing_key`.
///
/// Deserialized from either shape: `"b1"` or `{"id": "b1", "routing_key": "acme"}`. A list of
/// bare ids is what almost every caller has, and making them wrap each one in an object to say
/// nothing extra is a worse API than accepting both.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DeletePayload {
    Id(String),
    Keyed {
        id: String,
        #[serde(default)]
        routing_key: Option<String>,
    },
}

impl DeletePayload {
    pub fn id(&self) -> &str {
        match self {
            DeletePayload::Id(id) => id,
            DeletePayload::Keyed { id, .. } => id,
        }
    }

    pub fn routing_key(&self) -> Option<&str> {
        match self {
            DeletePayload::Id(_) => None,
            DeletePayload::Keyed { routing_key, .. } => routing_key.as_deref(),
        }
    }
}

/// Write request message for MicroshardActor.
///
/// `id` is the key the document is stored under. It travels beside the body because the body
/// need not carry it — see `unusable_document_identity` — and it is `#[serde(default)]` because
/// this is a remote message: a peer running a build that predates the field sends a request
/// without it, and such a request still means "take the id from the body", which is what
/// `handle_write` falls back to.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteRequest {
    pub index: String,
    #[serde(default)]
    pub id: String,
    pub routing_key: String,
    pub doc: JsonValue,
}

/// Response containing write result from MicroshardActor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteReply {
    pub sequence: u64,
}

/// Batch write request message for MicroshardActor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchWriteRequest {
    pub index: String,
    pub docs: Vec<DocPayload>,
}

/// Response containing batch write result from MicroshardActor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchWriteReply {
    pub items_written: u64,
    pub errors: Vec<String>,
}

/// Search request message for MicroshardActor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchRequest {
    pub index: String,
    pub query: String,
    pub limit: Option<usize>,
    pub sort: Option<SortSpec>,
}

/// Message to delete an index and all its data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShutdownShard;

/// Message to request shard statistics from a MicroshardActor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetShardStats {
    pub include_data_size: bool,
}

// Admin types re-exported from the dedicated admin module.
pub use crate::admin::memory::{
    AdminIndexCommitReport, AdminIndexEvictWriterReport, AdminMemoryReport, CommitAdminIndex,
    EvictAdminIndexWriter, GetAdminMemory, PurgeAdminMemory,
};

/// Remote-friendly error type for cross-node microshard calls.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RemoteError {
    Io(String),
    Identity(String),
    NotFound(String),
    InvalidInput(String),
    Other(String),
}

impl From<OrchestratorError> for RemoteError {
    fn from(err: OrchestratorError) -> Self {
        match err {
            OrchestratorError::Identity(e) => RemoteError::Identity(e.to_string()),
            // The dedicated variants map onto the kinds `RemoteError` already has —
            // what used to be re-derived from an `io::Error`'s kind. A real `Io` still
            // carries its kind for the same mapping, because a genuine filesystem
            // `NotFound` means the same thing.
            OrchestratorError::Validation(s) => RemoteError::InvalidInput(s),
            OrchestratorError::Missing(s) => RemoteError::NotFound(s),
            // `RemoteError` is the microshard path and has no "not now" kind — a shard
            // that is not ready reads as a fault on the far side, as it did before.
            OrchestratorError::NotReady(s) => RemoteError::Io(s),
            OrchestratorError::Io(e) => {
                if e.kind() == std::io::ErrorKind::NotFound {
                    RemoteError::NotFound(e.to_string())
                } else if e.kind() == std::io::ErrorKind::InvalidInput {
                    RemoteError::InvalidInput(e.to_string())
                } else {
                    RemoteError::Io(e.to_string())
                }
            }
            OrchestratorError::Storage(e) => RemoteError::Other(e.to_string()),
            OrchestratorError::ShardLimitExceeded { current, max } => {
                RemoteError::InvalidInput(format!("shard limit exceeded {current}/{max}"))
            }
            OrchestratorError::ShardAlreadyExists { shard_id } => {
                RemoteError::InvalidInput(format!("shard already exists: {shard_id}"))
            }
            OrchestratorError::UnsortableField { field, reason } => {
                RemoteError::InvalidInput(format!("cannot sort by '{field}': {reason}"))
            }
            OrchestratorError::UnrunnableQuery { notes } => RemoteError::InvalidInput(format!(
                "no clause of this query can run against this index: {notes}"
            )),
            // A peer whose shards all refused the request passes that on as a refusal; one whose
            // shards all failed passes on a failure. Collapsing both into `Io` would make a
            // requesting node answer 500 for a query the caller could fix.
            OrchestratorError::NoShardAnswered {
                index,
                reasons,
                caller_error,
            } => {
                let text = format!("no shard could run this query against '{index}': {reasons}");
                if caller_error {
                    RemoteError::InvalidInput(text)
                } else {
                    RemoteError::Io(text)
                }
            }
            // Not the caller's mistake, so not `InvalidInput`: a requesting node must not turn
            // this into a 400 telling the caller to fix a request that is already correct.
            OrchestratorError::SchemaUnconfirmed { index, reason } => RemoteError::Io(format!(
                "cannot establish the cluster schema for '{index}': {reason}"
            )),
            // A microshard never asks for a schema — it is handed one — so this cannot arise on
            // that path. Carried as `Io` because `RemoteError` has no kind for it and inventing
            // one would suggest a shard could produce it.
            OrchestratorError::SchemaBodyRequired { index } => RemoteError::Io(format!(
                "no schema for '{index}' on this node; resend the write carrying the schema body"
            )),
            // Orchestrator to orchestrator only; a microshard never canvasses.
            err @ OrchestratorError::SchemaBeingMinted { .. } => RemoteError::Io(err.to_string()),
            // A verdict from a further hop, mapped onto the kinds this type carries. `RemoteError`
            // is the microshard path and has no retryable kind of its own, so `Unavailable`
            // travels as `Io` here — a shard call does not produce one.
            OrchestratorError::PeerUnreachable { message } => RemoteError::Io(message),
            // Carried like `PeerUnreachable`: a peer that shed the read did not answer, and the
            // gather reports it as the partial outage it is.
            OrchestratorError::ReadDeadlineExpired {
                waited_ms,
                budget_ms,
            } => RemoteError::Io(format!(
                "request abandoned: spent {waited_ms}ms of a {budget_ms}ms budget before a \
                 worker could start it"
            )),
            // Travels like the dequeue refusal above, and for the same reason: a peer that
            // refused the read at its door did not answer, and the gather reports the shard as
            // the partial outage it is.
            OrchestratorError::Overloaded {
                predicted_wait_ms,
                budget_ms,
            } => RemoteError::Io(format!(
                "overloaded: a {predicted_wait_ms}ms backlog against a {budget_ms}ms request"
            )),
            // Decided by the orchestrator, never a microshard, so this path does not produce it.
            // If it ever crosses here, a refusal is the kind that keeps it out of the 500s.
            err @ OrchestratorError::QuotaExceeded { .. } => {
                RemoteError::InvalidInput(err.to_string())
            }
            OrchestratorError::Remote { verdict, message } => match verdict {
                RemoteVerdict::BadRequest | RemoteVerdict::QuotaExceeded => {
                    RemoteError::InvalidInput(message)
                }
                RemoteVerdict::NotFound => RemoteError::NotFound(message),
                RemoteVerdict::Unavailable
                | RemoteVerdict::ServerFault
                | RemoteVerdict::SchemaRequired
                | RemoteVerdict::Minting => RemoteError::Io(message),
            },
        }
    }
}

impl From<RemoteError> for OrchestratorError {
    fn from(err: RemoteError) -> Self {
        match err {
            // The kind is kept for this one: a peer refusing the request — an unsortable sort
            // field, say — has to stay distinguishable from a peer that failed, because the
            // HTTP surface answers 400 for the first and 500 for the second. It was rebuilt
            // as an `InvalidInput`-kind `Io` before; `Validation` carries it directly, which
            // also keeps a peer's `InvalidData` a 400 instead of flattening it to a 500.
            RemoteError::InvalidInput(s) => OrchestratorError::Validation(s),
            // An absence a peer reported stays an absence — `Missing` holds the same
            // `ServerFault` verdict the flattened `Io` carried.
            RemoteError::NotFound(s) => OrchestratorError::Missing(s),
            RemoteError::Io(s) | RemoteError::Identity(s) | RemoteError::Other(s) => {
                OrchestratorError::Io(std::io::Error::other(s))
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchHit {
    pub score: f32,
    pub doc: JsonValue,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchReply {
    pub hits: Vec<SearchHit>,
    pub total_hits: usize,
    /// Clauses the query parser dropped; see [`storage::SearchOutcome::discarded`].
    ///
    /// Defaulted because this type crosses the cluster wire and a peer may not send the field.
    #[serde(default)]
    pub discarded: Vec<String>,
    /// The field whose order is approximate, if the sort could not be exact; see
    /// [`storage::SearchOutcome::approximate_sort`].
    ///
    /// Defaulted for the same reason as `discarded`. A peer that does not send it is read as
    /// "exact", which is the safe direction only because the field is advisory — the hits are
    /// the same either way.
    #[serde(default)]
    pub approximate_sort: Option<String>,
    /// The default fields an unqualified term searched, when the node's cap narrowed them; see
    /// [`storage::SearchOutcome::narrowed_default_fields`].
    ///
    /// Defaulted for the same reason as `approximate_sort`, and advisory in the same way: a peer
    /// that does not send it reads as "not narrowed", and the hits are the same either way.
    #[serde(default)]
    pub narrowed_default_fields: Option<storage::NarrowedDefaultFields>,
    /// Nothing survived the parse on this shard; see [`storage::SearchOutcome::emptied`].
    ///
    /// Defaulted for the same reason as `discarded`. A peer that does not send it reads as
    /// "the query ran", which keeps an older peer answering rather than having its hits
    /// refused on a claim it never made.
    #[serde(default)]
    pub emptied: bool,
}

/// Client operation messages for RouterActor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClientOp {
    /// Search operation across shards of an index
    Search {
        index: String,
        query: String,
        limit: Option<usize>,
        /// How many ordered hits to skip before the first one returned — the paging half of
        /// `limit`. `None` and `Some(0)` mean the same thing and cost the same.
        ///
        /// Widened away before this op is forwarded to another node: a peer is asked for
        /// `offset + limit` hits from the front, and the node that received the request applies
        /// the skip once, after merging. See `SearchWindow::fetch_count`.
        offset: Option<usize>,
        /// Optional field projection (return only specified fields)
        fields: Option<Vec<String>>,
        /// Optional sort specification
        sort: Option<SortSpec>,
    },
    /// Streaming search operation across shards of an index
    Stream {
        index: String,
        query: String,
        limit: Option<usize>,
        /// Optional field projection (return only specified fields)
        fields: Option<Vec<String>>,
        /// Optional sort specification
        sort: Option<SortSpec>,
    },
    /// Write operation to insert/update a document
    Write {
        index: String,
        id: String,
        routing_key: Option<String>,
        doc: JsonValue,
        /// Set by the node that forwarded this op to the shard's owner, so the owner does not
        /// forward it again. See [`NodeOrchestrator::forward_op_to_owner`].
        ///
        /// Defaulted, so an older peer that does not send it reads as a first hop — which is
        /// what every op was before this existed.
        #[serde(default)]
        forwarded: bool,
        /// The schema, sent **only** in answer to [`OrchestratorError::SchemaBodyRequired`].
        ///
        /// See [`ClientOp::BulkWrite::schema_body`].
        #[serde(default)]
        schema_body: Option<Box<IndexSchema>>,
        /// The tenant to stamp on this index if the write creates it, from the key that sent
        /// it.
        ///
        /// Carried on the op rather than read from ambient state because the decision is made
        /// deep inside the orchestrator — implicit creation samples the first documents into a
        /// schema there — while the only place the tenant is known is the HTTP gate. A
        /// task-local would not survive the actor mailbox, and an unstamped index counts
        /// against nobody's quota, which is the one outcome that makes the quota pointless.
        ///
        /// Read **only** when the write creates the index. It never rewrites an existing
        /// stamp: ownership is a fact about who made the index, not who last wrote to it.
        ///
        /// Defaulted, so an op from an older peer stamps nothing.
        #[serde(default)]
        tenant: Option<String>,
    },
    /// Bulk write operation to insert/update multiple documents
    BulkWrite {
        index: String,
        docs: Vec<DocPayload>,
        /// Set by the node that split this batch and forwarded this share, so the owner knows
        /// it is running someone else's decision rather than making its own.
        ///
        /// **A forwarded write must never decide a schema.** The gate runs inside the
        /// orchestrator's message handler, and a fan-out that asks its peers for a schema asks
        /// through the very mailbox those peers are using to run the forwarded writes — each of
        /// which was asking back. Three nodes waited on each other until
        /// [`PEER_SCHEMA_LOOKUP_TIMEOUT`] broke the circle, and a first bulk write into a new
        /// index lost most of its documents on a healthy cluster: measured at 15 of 46 written,
        /// 31 refused, in 5.02s, where the same 46 under one routing key wrote in 0.03s.
        ///
        /// This one bit is the whole signal, and it replaces everything an earlier cut carried.
        /// A share with it set and no local schema does not sample and does not canvass: it
        /// answers [`OrchestratorError::SchemaBodyRequired`] and the forwarding node resends
        /// with [`schema_body`](Self::schema_body). So an ordinary forward carries **no schema
        /// information at all** — where the first cut sent the whole schema (602 bytes for three
        /// fields, 3,478 for twenty, against a 49-byte document) and the second a 61-byte stamp.
        /// [`ClientOp::Write`] already carried this bit, so a single write now costs nothing
        /// extra whatsoever.
        ///
        /// Defaulted, so a share from an older peer reads as a first hop and takes the peer
        /// lookup, which is what every forward did before this existed.
        #[serde(default)]
        forwarded: bool,
        /// The schema, sent **only** in answer to [`OrchestratorError::SchemaBodyRequired`].
        ///
        /// Never on a first attempt: the point of `forwarded` is that the receiver can ask, so
        /// nothing has to be sent speculatively. An empty schema is never sent — see
        /// [`schema_to_carry`].
        #[serde(default)]
        schema_body: Option<Box<IndexSchema>>,
        /// The tenant to stamp on this index if this batch creates it. See
        /// [`ClientOp::Write::tenant`].
        #[serde(default)]
        tenant: Option<String>,
    },
    /// Remove many documents by key, in one request.
    ///
    /// Routed and grouped exactly as [`ClientOp::BulkWrite`] is, so each shard takes one batch in
    /// one redb transaction. A document that cannot be routed is reported as an error against
    /// that id rather than failing the batch, since a batch may span tenants and one unroutable
    /// id says nothing about the rest.
    BulkDelete {
        index: String,
        docs: Vec<DeletePayload>,
        /// True on a batch this node received from a peer that could not place it.
        ///
        /// The hop limit [OB3](ROADMAP) put on single writes and deletes, on the bulk path it
        /// named as still open. Both ends decide from their own view of the ring, those views
        /// disagree while membership changes, and without this a batch neither node believes it
        /// owns is passed back and forth — a full remote ask with every document in it, per
        /// round trip, until something times out.
        ///
        /// `serde(default)` so a batch from a peer built before this field reads as a first
        /// hop, which is what it is.
        #[serde(default)]
        forwarded: bool,
    },
    /// Remove one document by its key.
    ///
    /// A write with no document, which is what makes it the one operation the engine can always
    /// serve: nothing here can grow a schema, so there is no `UseActor` path to fall back to.
    ///
    /// `routing_key` matters more than it does on a write. A write derives its key from the
    /// document's own routing field; a delete has no document, so on an index that routes by a
    /// field other than the key this is the only way to name the shard that holds the row. See
    /// `effective_delete_routing_key`.
    Delete {
        index: String,
        id: String,
        routing_key: Option<String>,
        /// Set by the node that forwarded this op to the shard's owner, so the owner does not
        /// forward it again. See [`NodeOrchestrator::forward_op_to_owner`].
        #[serde(default)]
        forwarded: bool,
    },
    /// Phase one of `PUT /_config`: what storing `schema` here would ask of this node, without
    /// storing it — answered as a [`SchemaReadiness`].
    ///
    /// Internal to the cluster: the node that received the `PUT` asks every node, itself
    /// included, and decides once from all the answers — one version, one owner, and whether
    /// the change may go ahead at all. Decided per node, a change reached only the node that
    /// received it, and each node judged a retype by its own documents alone: one holding none
    /// accepted a change that another, holding the data, refused.
    PrepareSchema {
        index: String,
        schema: IndexSchema,
        /// This change, reserving the index on this node until it is applied or released. A
        /// second change prepared meanwhile is answered `busy` — see [`SchemaReadiness::busy`].
        #[serde(default)]
        change: Uuid,
    },
    /// Phase two of `PUT /_config`: store `schema` here, at the version and with the owner the
    /// coordinating node decided — answered as a [`SchemaApplied`].
    ///
    /// A node holding a newer schema, or one the cluster prefers at the same version, keeps it
    /// and says so: two changes applied at once then settle on one schema on every node,
    /// whichever reached each node first.
    ApplySchema {
        index: String,
        schema: IndexSchema,
        /// Count a new index against its tenant's quota here. Set for the coordinating node
        /// only: the quota is checked in phase one everywhere, and this repeats it on the node
        /// whose mailbox serialises it against other mints.
        #[serde(default)]
        check_quota: bool,
        /// The change being applied; its reservation here ends with it.
        #[serde(default)]
        change: Uuid,
    },
    /// End `change`'s reservation of `index` on this node without applying anything: sent by
    /// the coordinating node when it refuses the change after phase one.
    ReleaseSchemaChange { index: String, change: Uuid },
    /// Ask the cluster to agree on this node's schema for `index` — sent to this node by its
    /// router when a search found the nodes answering it holding different schemas.
    ReconcileSchema { index: String },
    /// Finish on this node a drop it missed: sent by a node that found this one holding the
    /// index at or below a drop another node recorded at `dropped_at`. Drops the index here,
    /// data and schema, only if it still holds it at or below that version; answers whether
    /// it did.
    FinishDrop { index: String, dropped_at: u64 },
    /// What this node holds for every index, dropped indexes' records included — see
    /// [`storage::SchemaRecord`]. Nodes compare them when they connect.
    SchemaRecords,
    /// Get index configuration/schema
    GetConfig { index: String },
    /// This node's stored schema for an index, serialised whole, or `null` if it holds none.
    ///
    /// Internal to the cluster: no HTTP route reaches it, and it exists because `GetConfig` —
    /// the only other way to read a schema — answers with a *response shape*, not a schema.
    /// Adopting a peer's schema through that shape is the erasure this project already fixed
    /// once: reading a schema out as JSON and writing it back drops every property the shape
    /// does not carry, `routing_field_name` among them, which silently changes which shard a
    /// document routes to. This carries the `IndexSchema` itself so a node can adopt a peer's
    /// declaration without reconstructing it.
    GetRawSchema {
        index: String,
        /// Set by a node canvassing because it is about to mint this index, to its own id.
        ///
        /// A receiver minting the same index records the asker as a rival, which is how both
        /// sides of a race come to know of each other and settle it the same way — see
        /// `MintAfterCanvass`. Unset for a lookup that creates nothing (an index delete's).
        /// Defaulted, so an older peer's ask records nothing.
        #[serde(default)]
        minting_by: Option<Uuid>,
    },
    /// The schema for an index from anywhere in the cluster, or `null` if no node holds one.
    ///
    /// This node's own store first, then its peers — so the common case, a node that holds the
    /// index, answers from disk and fans out to nobody, and a standalone node never leaves the
    /// process. It errors rather than answering `null` when the cluster cannot be canvassed in
    /// full, because "no node has this" and "I could not ask every node" are different facts
    /// and only the first one means the index does not exist.
    ///
    /// The point of asking it this way is that it asks the question directly. The alternative
    /// on hand was the cluster catalogue, which answers "does this name exist" by collecting
    /// per-shard statistics for *every* index on every node and reading each of their schemas —
    /// work proportional to the whole catalogue to look up one name.
    FindSchemaInCluster { index: String },
    /// Parse a query against an index without running it.
    ///
    /// Metadata rather than a search: it touches no documents and returns no hits, only what the
    /// parser made of the query. Local-only for the same reason `GetConfig` is — every shard
    /// resolves field names against the same schema, so the first that can answer does.
    ValidateQuery { index: String, query: String },
    /// List all available indexes with statistics (optimized for _indexes endpoint)
    ListIndexes { include_data_size: bool },
    /// Get node identity information
    GetIdentity,
    /// List all indexes across the cluster (broadcast)
    ListClusterIndexes { include_data_size: bool },
    /// Delete an index and all its data
    DeleteIndex { index: String, delete_schema: bool },
}

/// Message to update the global routing topology (consistent ring).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateTopology {
    pub ring: ConsistentRing,
}

/// Message to shutdown all shards gracefully.
#[derive(Debug, Clone)]
pub struct ShutdownAllShards;

/// Message to shut down the dedicated read thread pool and wait for it.
///
/// Separate from [`ShutdownAllShards`] because the pool has to outlive the shards: their
/// shutdown runs on it. Send this once that has returned.
#[derive(Debug, Clone)]
pub struct ShutdownReadRuntime {
    /// How long to wait for in-flight reads before abandoning the threads.
    pub timeout: Duration,
}

/// One node's answer to [`ClientOp::PrepareSchema`].
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SchemaReadiness {
    /// The schema this node holds for the index, a dropped index's record included — its
    /// version still counts, so a re-declaration is newer than the drop.
    pub current: Option<IndexSchema>,
    /// This node's documents of the index; counted only when the change touches a built column.
    pub documents: u64,
    /// Changes this node's built index would act against: see `SchemaChange::conflicts`.
    pub conflicts: Vec<String>,
    /// Fields the change marks indexed that this node's built index has no column for: they
    /// become searchable only when the index is rebuilt, which happens here now only if this node
    /// holds none of its documents.
    #[serde(default)]
    pub unbuilt: Vec<String>,
    /// Why this node would refuse the index as a new one for the calling tenant, if it would.
    pub quota_exceeded: Option<String>,
    /// The declaration would leave this node's schema as it is: the same thumbprint once the
    /// fields this node learned from writes are merged in, as phase two merges them.
    #[serde(default)]
    pub unchanged: bool,
    /// Another change to this index was prepared here and has not been applied or released.
    ///
    /// Two changes prepared at once would each pick the same next version, and both be told
    /// they were applied: every node settles on the one the cluster prefers, so the other was
    /// overwritten while its caller heard success. Refused before anything is written instead.
    #[serde(default)]
    pub busy: bool,
}

/// One node's answer to [`ClientOp::ApplySchema`].
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SchemaApplied {
    /// The schema is stored here.
    pub applied: bool,
    /// Not stored: this node holds a schema the cluster prefers — a newer one, or one that
    /// wins the tie at the same version.
    pub superseded: bool,
    /// Not stored: documents reached this node while it was rebuilding for a change they
    /// conflict with, so the rebuild was undone here.
    pub arrived: u64,
    /// The conflicting changes behind a refusal.
    pub conflicts: Vec<String>,
    /// The stored schema's field names, sorted, for the caller's answer.
    pub field_names: Vec<String>,
}

/// Commands sent to the dedicated writer thread via `tokio::sync::mpsc` channel.
/// The writer thread calls blocking storage methods (`apply_write`, `apply_batch`)
/// and sends results back via oneshot reply channels.
pub enum StorageCommand {
    Write {
        index: String,
        op: WalOp,
        reply: tokio::sync::oneshot::Sender<Result<u64, StoreError>>,
    },
    BatchWrite {
        index: String,
        ops: Vec<WalOp>,
        reply: tokio::sync::oneshot::Sender<Result<Vec<u64>, StoreError>>,
    },
    Commit {
        index: String,
        reply: tokio::sync::oneshot::Sender<Result<(), StoreError>>,
    },
    EvictWriter {
        index: String,
        reply: tokio::sync::oneshot::Sender<bool>,
    },
    /// Drop an index's tables, caches and Tantivy directory.
    ///
    /// Routed through the writer thread rather than run on a blocking-pool thread so it is
    /// serialized against writes to the same index: deletion tears down the writer, the
    /// sequence counter and the redb tables that an in-flight `apply_write` is actively
    /// using, and running the two concurrently let a write recreate what deletion had just
    /// removed.
    DeleteIndex {
        index: String,
        delete_schema: bool,
        reply: tokio::sync::oneshot::Sender<Result<(), StoreError>>,
    },
    /// Build an index again from `schema`, if this shard holds none of its documents: drop its
    /// data, store the schema, and open the index under it. Answers the documents found — `0`
    /// when it rebuilt, otherwise how many stopped it, untouched.
    ///
    /// One writer step, so the check and the rebuild see the same index: a write queued ahead
    /// of it is applied first and counted, and none can land between the two. Run apart, a
    /// write landing after an empty count was dropped with the data, though it had been
    /// acknowledged.
    RebuildIfEmpty {
        index: String,
        schema: Box<IndexSchema>,
        reply: tokio::sync::oneshot::Sender<Result<u64, StoreError>>,
    },
    Shutdown,
}

#[cfg(test)]
mod tests;
