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

use futures::future::join_all;
use futures::stream::{FuturesUnordered, StreamExt};
use rayon::prelude::*;
use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{
    Arc,
    atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering as AtomicOrdering},
};
use std::time::{Duration, Instant};

use anyhow::Result;
use arc_swap::ArcSwap;
use kameo::actor::ActorRef;
use kameo::message::{Context, Message};
use kameo::{Actor, RemoteActor, remote_message};
use serde::{Deserialize, Serialize};
use tokio::sync::{RwLock as AsyncRwLock, mpsc};
use tokio::time::timeout;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::cluster_coordinator::{
    ClusterCoordinator, GetKnownPeers, GetShardAssignments, OperationType, RegisterLocalShards,
    RequestBootstrapRedial, RouteOperation, RoutingDecision, ShardMetadata,
};
use crate::config::{MessagingConfig, SearchConfig};
use crate::query::parse_query_keywords;
use crate::remote_peer_pool::{ConnectionChannel, RemotePeerPool};

// Re-export SortSpec and SortOrder from storage crate
use chrono::{NaiveDate, NaiveDateTime};
use cluster::{ConsistentRing, IdentityError, NodeIdentity, generate_tokens};
use serde_json::{Map as JsonMap, Value as JsonValue};
use storage::{
    FieldDef, HybridStore, IndexSchema, SchemaFieldUpdate, ShardStatsTimings, StorageConfig,
    StoreError, TantivyFieldType, WalOp,
};
pub use storage::{SortOrder, SortSpec};

/// Sample limit for enhanced schema detection during initial creation
const SCHEMA_SAMPLE_LIMIT: usize = 200;

/// Channel capacity for per-shard dedicated writer threads.
/// Each MicroshardActor sends StorageCommands through this bounded channel.
const SHARD_WRITER_CHANNEL_CAPACITY: usize = 1024;

/// Capacity of the writer → warmup-thread re-warm request channel.
///
/// Small on purpose. Requests are posted with `try_send` and dropped when full, and a dropped
/// request costs nothing: the index warms on its next commit or on its first query. A deep
/// queue would only accumulate stale requests for generations that have already been replaced.
const WARM_REQUEST_CAPACITY: usize = 64;

/// Channel capacity for the orchestrator worker pool job queue.
/// Quadruple the shard writer capacity to allow more buffering of incoming requests
/// while workers are dispatching to shard writer threads.
const ORCHESTRATOR_WORKER_QUEUE_CAPACITY: usize = SHARD_WRITER_CHANNEL_CAPACITY * 4;

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
const ORCHESTRATOR_WORKER_MAX_IN_FLIGHT: usize = 8;

/// The prefix a per-document reason carries: its place in the batch it was sent in.
const DOCUMENT_PREFIX: &str = "document ";

/// Split `document 7: reason` into its position and its reason, if that is what it is.
fn split_document_reason(error: &str) -> Option<(usize, &str)> {
    let rest = error.strip_prefix(DOCUMENT_PREFIX)?;
    let (position, reason) = rest.split_once(": ")?;
    Some((position.parse().ok()?, reason))
}

/// Restate reasons numbered against one batch in the numbering its caller used.
///
/// Whoever answered a batch numbered its reasons against the batch *it* received, which is not
/// the numbering the caller counts in: a peer answers about the slice forwarded to it, and a
/// micro-batch of an NDJSON stream answers about five hundred documents out of a file. A
/// position from the wrong numbering is worse than none, because it names a real item that is
/// fine.
///
/// Exactly `unwritten` reasons come back, which is what makes the answer addable — written plus
/// reasons is what was received. Reasons that name no item are attached to items they cost
/// rather than reported beside them, and a shortfall nobody explained is stated by
/// `unattributed` rather than left for the caller to find by subtracting.
pub(crate) fn renumber_reasons(
    reasons: &[String],
    positions: &[usize],
    label: &str,
    unwritten: usize,
    attribution: &str,
    unaccounted: impl Fn() -> String,
) -> Vec<String> {
    let mut renumbered = Vec::with_capacity(unwritten);

    for reason in reasons {
        if renumbered.len() == unwritten {
            break;
        }
        match split_document_reason(reason) {
            Some((theirs, why)) if theirs < positions.len() => {
                renumbered.push(format!("{label} {}: {why}", positions[theirs]));
            }
            // Not about one item, or numbered against a batch this is not: kept as what was
            // said, against an item it cost.
            _ => renumbered.push(format!("{attribution}: {reason}")),
        }
    }

    while renumbered.len() < unwritten {
        renumbered.push(unaccounted());
    }

    renumbered
}

/// What a peer's answer means for the documents this node forwarded to it.
///
/// One reason per document the peer did not write, which is what keeps the coordinating node's
/// own answer addable: `items_written` plus `errors` is `items_received`, on one node or five.
///
/// A peer numbers its reasons against the batch *it* received, so each is renumbered to the
/// position the caller used — a position from someone else's batch is worse than none, because
/// it names a real document that is fine. A reason in any other shape is about the request
/// rather than one document, so it is attached to the documents it cost rather than reported
/// beside them.
///
/// The last case is a peer whose numbers do not add up: fewer reasons than documents it failed
/// to write. It cannot happen between two nodes running this code, and it is stated rather than
/// left as a silent shortfall when it does, because a caller cannot act on a gap it has to infer
/// by subtracting.
fn remote_rejections(
    node_id: Uuid,
    positions: &[usize],
    written: usize,
    reasons: &[String],
) -> Vec<String> {
    renumber_reasons(
        reasons,
        positions,
        "document",
        positions.len().saturating_sub(written),
        &format!("node {node_id}"),
        || {
            format!(
                "a document forwarded to node {node_id} was neither written nor refused: it \
                 reported {written} written of {} with {} reasons",
                positions.len(),
                reasons.len()
            )
        },
    )
}

/// A document on its way to a shard, still carrying where it sat in the batch that arrived.
///
/// The position is what lets every reason name the document it is about. It used to be dropped
/// the moment validation's rejects were filtered out, so nothing past that point could say
/// `document N:` — a routing failure or a failed shard batch went to the log and left the
/// response reporting fewer documents written than it received with nothing to explain the
/// difference.
#[derive(Debug, Clone)]
struct Placed {
    position: usize,
    doc: DocPayload,
    routing_key: Option<String>,
}

/// Where one document is going, or why it is going nowhere.
type RoutingResult = Result<(Placed, Uuid), (usize, String)>;

/// Type alias for single write commands enqueued in the writer thread
type WriteCommand = (WalOp, tokio::sync::oneshot::Sender<Result<u64, StoreError>>);

/// Type alias for batch write commands enqueued in the writer thread
type BatchCommand = (
    Vec<WalOp>,
    tokio::sync::oneshot::Sender<Result<(Vec<u64>, usize), StoreError>>,
);

/// Type alias for index deletions enqueued in the writer thread: (index, delete_schema, reply)
type DeleteCommand = (
    String,
    bool,
    tokio::sync::oneshot::Sender<Result<(), StoreError>>,
);

/// Type alias for tracking reply slices when coalescing batch writes
type BatchReplySegment = (
    usize,
    tokio::sync::oneshot::Sender<Result<(Vec<u64>, usize), StoreError>>,
);

/// Extract routing key value from JSON document using field name
pub fn extract_routing_value(doc: &JsonValue, field_name: &str) -> Option<String> {
    let obj = doc.as_object()?;
    match obj.get(field_name)? {
        JsonValue::String(s) => Some(s.clone()),
        JsonValue::Number(n) => Some(n.to_string()),
        JsonValue::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

/// Apply field projection to a JSON document, keeping only specified fields.
/// Always preserves metadata fields (_score, _sort_key, etc.) that start with underscore.
///
/// User-specified fields are inserted first in the exact order given by the projection
/// list, so the response field order matches the user's `return` clause. Metadata fields
/// are appended afterwards. This guarantees a consistent field order whether or not a
/// sort is active — the internal `_sort_key` (if present) simply appears at the end and
/// is stripped by `strip_sort_keys` at the client boundary.
fn apply_field_projection(doc: JsonValue, fields: &[String]) -> JsonValue {
    if let JsonValue::Object(mut map) = doc {
        let mut filtered = serde_json::Map::new();

        // Add requested fields first, in user-specified projection order
        for field in fields {
            if let Some(value) = map.remove(field) {
                filtered.insert(field.clone(), value);
            }
        }

        // Then append metadata fields (those starting with _)
        for (key, value) in map.iter() {
            if key.starts_with('_') {
                filtered.insert(key.clone(), value.clone());
            }
        }

        JsonValue::Object(filtered)
    } else {
        doc
    }
}

fn hit_score(hit: &JsonValue) -> f64 {
    hit.get("_score").and_then(|s| s.as_f64()).unwrap_or(0.0)
}

/// Accumulator for broadcast search statistics across local and remote results.
struct BroadcastStats {
    total_shards_queried: usize,
    nodes_contacted: usize,
    max_took_ms: Option<u64>,
    total_hits_sum: usize,
    /// Distinct dropped clauses across the nodes that answered.
    discarded: Vec<String>,
    /// The approximated sort field, if any node reported one; see [`APPROXIMATE_SORT_FIELD`].
    approximate_sort: Option<String>,
}

/// Metadata field carrying the normalized sort value of a hit.
///
/// Injected by the shard-gather search paths (`engine_search` / `orch_search`) before
/// field projection runs, and consumed by every merge layer. Because it is `_`-prefixed
/// it survives `apply_field_projection` automatically, so cross-node merges can order
/// results even when the user's `return` projection excludes the sort field itself. It
/// is stripped from every hit at the client boundary (`route_and_handle`).
const SORT_KEY_FIELD: &str = "_sort_key";

/// Whether a shard's failure is the request's fault rather than this node's.
///
/// Everything crossing a shard actor arrives as an `io::Error` carrying a kind and a message —
/// `handle_search` maps the engine's error into one — so the kind is the whole classification.
/// `InvalidInput` and `InvalidData` are what the engine raises for a query or a field the index
/// cannot answer; anything else is this node failing to read data it owns, which says nothing
/// about what was asked.
fn is_caller_error(err: &OrchestratorError) -> bool {
    match err {
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

/// The refusal a scatter-gather owes its caller when not one shard could run the query.
///
/// A failed shard alongside a successful one is a partial answer, and reporting it as hits plus
/// `errors` is right: some of the data was read. With no successful shard there is no answer to
/// qualify — only a `200` whose empty `hits` array is indistinguishable from a search that ran
/// everywhere and matched nothing, with the reason in a key most callers never read.
///
/// The reasons are deduplicated and the shard ids dropped: every shard runs the same query
/// against the same schema, so they fail the same way, and repeating one reason per shard reads
/// as several different problems.
fn no_shard_answered(
    index: &str,
    shard_success: usize,
    failures: &[(Uuid, OrchestratorError)],
) -> Option<OrchestratorError> {
    if shard_success > 0 || failures.is_empty() {
        return None;
    }

    let mut reasons: Vec<String> = Vec::new();
    for (_, err) in failures {
        let reason = match err {
            OrchestratorError::Io(io) => io.to_string(),
            other => other.to_string(),
        };
        if !reasons.contains(&reason) {
            reasons.push(reason);
        }
    }

    Some(OrchestratorError::NoShardAnswered {
        index: index.to_string(),
        reasons: reasons.join("; "),
        caller_error: failures.iter().all(|(_, err)| is_caller_error(err)),
    })
}

/// The per-shard failures as a response reports them, one line each, naming the shard.
fn shard_error_notes(failures: &[(Uuid, OrchestratorError)]) -> Vec<String> {
    failures
        .iter()
        .map(|(shard_id, err)| format!("Shard {}: {}", shard_id, err))
        .collect()
}

/// Report the shards that could not be read, and only then.
///
/// Absent means every shard answered, which is what makes its presence worth reading — the same
/// rule the federated search follows for the indexes it could not reach. An `errors: []` on every
/// successful search teaches a caller to skip the key, which is precisely the habit that hides
/// the one response where it matters.
fn attach_shard_errors(response: &mut JsonValue, errors: Vec<String>) {
    if errors.is_empty() {
        return;
    }
    if let Some(object) = response.as_object_mut() {
        object.insert(
            "errors".to_string(),
            JsonValue::Array(errors.into_iter().map(JsonValue::String).collect()),
        );
    }
}

/// Response key listing the clauses the query parser dropped.
///
/// Absent on a clean parse rather than present and empty, so a caller can test for presence.
pub const DISCARDED_CLAUSES_FIELD: &str = "_discarded_clauses";

/// Attach `DISCARDED_CLAUSES_FIELD` to a search response, if anything was discarded.
fn attach_discarded(response: &mut JsonValue, discarded: Vec<String>) {
    if discarded.is_empty() {
        return;
    }
    if let Some(obj) = response.as_object_mut() {
        obj.insert(
            DISCARDED_CLAUSES_FIELD.to_string(),
            JsonValue::Array(discarded.into_iter().map(JsonValue::String).collect()),
        );
    }
}

/// Notes for projected fields the index does not have.
///
/// A projection drops an unknown field without complaint, so a keyword lifted out of prose —
/// `find tax return forms` — would otherwise answer with documents carrying no fields. Metadata
/// names and `shard_id` are added by the response itself and so are always projectable.
///
/// A sort is not noted here, because an unknown sort field is refused outright — see
/// [`unsortable_sort_field`]. A dropped projection still leaves an answer worth returning; a
/// dropped sort leaves the hits in an order the caller did not ask for.
///
/// Skipped entirely for an index whose schema is not yet known, where every field would look
/// unknown.
fn unknown_projection_fields(schema: &IndexSchema, fields: Option<&[String]>) -> Vec<String> {
    if schema.fields.is_empty() {
        return Vec::new();
    }

    let known = |name: &str| name.starts_with('_') || schema.fields.contains_key(name);
    let note = |clause: &str, field: &str| {
        format!(
            "'{clause} {field}' names a field this index does not have, so the clause had no \
             effect; if it was meant as query text, quote it or drop the keyword"
        )
    };

    fields
        .unwrap_or_default()
        .iter()
        .filter(|field| !known(field))
        .map(|field| note("return", field))
        .collect()
}

/// The projection as the documents can answer it.
///
/// On an index with a shadow field the identifier travels under the shadow name and no hit
/// carries `id`, so a projection naming `id` is rewritten to the name the hits have. Everywhere
/// else the rewrite is identity: `document_key_field` is `id` on a plain index. Done once,
/// before the list is checked or applied, so `return id` on a shadow index finds the field
/// rather than reporting it missing and returning a document with nothing in it.
fn normalize_projection_fields(schema: &IndexSchema, fields: &[String]) -> Vec<String> {
    let key = storage::document_key_field(schema);
    if key == "id" {
        return fields.to_vec();
    }
    fields
        .iter()
        .map(|field| {
            if field == "id" {
                key.clone()
            } else {
                field.clone()
            }
        })
        .collect()
}

/// Whether a value written under a name for the document key is that key.
///
/// The identifier itself is always a string — `DocPayload.id`, the redb key — while a body may
/// carry it as a number: a numeric primary key, an id read out of a column that was integral at
/// the source. So the identifier is read as a number too before those two are called a
/// disagreement, which is what keeps a body saying `"id": 42` beside an envelope saying `"42"`
/// from being refused. Any other JSON type is not an identifier and cannot name one.
fn names_identifier(value: &JsonValue, id: &str) -> bool {
    match value {
        JsonValue::String(text) => text == id,
        JsonValue::Number(number) => id
            .parse::<serde_json::Number>()
            .is_ok_and(|written_as| &written_as == number),
        _ => false,
    }
}

/// Why a document cannot be stored under the identifier it arrived with, if it cannot.
///
/// The key travels beside the body, not inside it: `DocPayload.id` is the redb key, the term
/// tantivy indexes, and the value `reconstruct_shadow_fields_owned` writes back into every hit
/// on the way out. Keeping it in the stored blob as well would hold a second copy of the same
/// string, which is why the documented bulk shape — `{"id": "...", "doc": {...}}` — leaves it
/// out of `doc`, and why the read path treats the key as authoritative when the blob has no
/// `id` of its own. Validation reads the envelope for that reason: demanding `id` inside the
/// body refused every document written in the shape the API documents.
///
/// A body that does carry `id` is the older shape and still valid, but only while it agrees
/// with the envelope. Disagreement is refused for the same reason a disagreeing shadow field
/// is: the blob keeps its own `id` and reconstruction prefers it, so the document would answer
/// to the key it was stored under and report a different one to whoever found it.
fn unusable_document_identity(id: &str, doc: &JsonValue) -> Option<String> {
    let obj = match doc.as_object() {
        Some(obj) => obj,
        None => return Some("Document body must be a JSON object".to_string()),
    };

    if id.is_empty() {
        return Some(
            "Document must be written with a non-empty 'id' beside its body: the identifier is \
             the key it is stored, updated and looked up under. The body itself does not have to \
             repeat it."
                .to_string(),
        );
    }

    let body_id = obj.get("id")?;
    (!names_identifier(body_id, id)).then(|| {
        format!(
            "document is written under id={id} but its body says id={body_id}. The identifier \
             beside the body is the key the document is stored under, so the two cannot differ: \
             a later read would find this document as {id} and report {body_id}. Leave 'id' out \
             of the body — it is supplied from the key — or correct one of the two."
        )
    })
}

/// Why a document's shadow fields cannot be stored as written, if they cannot.
///
/// A shadow field is the document key under the source's own name, and nothing holds a second
/// copy of the value: the write path strips the field out of the stored blob
/// (`filter_shadow_fields_owned`) and the read path writes the key back under it
/// (`reconstruct_shadow_fields_owned`). That round trip returns what was written only while the
/// two agree, so a document saying otherwise has that value silently and unrecoverably
/// discarded — it is refused here rather than accepted and destroyed.
///
/// The rest of the design already rests on this: `document_key_field` picks any one shadow name
/// to read the key back under and reconstruction writes the key under every one, both defensible
/// only if all of them mean the key. The bundled importer checks it before promoting a column to
/// shadow; this is the same rule for a write arriving by any other route.
///
/// Absence is not disagreement. A document may omit a shadow field entirely — the ordinary case
/// for a rewritten document — and reconstruction supplies it on the way out.
///
/// `id` is the identifier the write arrived with rather than anything read out of the body,
/// because a body need not carry `id` at all. Reading it from the body skipped this check
/// entirely on exactly the documents the documented bulk shape sends.
fn disagreeing_shadow_field(doc: &JsonValue, schema: &IndexSchema, id: &str) -> Option<String> {
    if schema.shadow_fields.is_empty() {
        return None;
    }
    let obj = doc.as_object()?;

    // Sorted, so a document disagreeing under two names names the same one every time.
    let mut names: Vec<&String> = schema.shadow_fields.iter().collect();
    names.sort_unstable();

    names.into_iter().find_map(|name| {
        let value = obj.get(name)?;
        (!names_identifier(value, id)).then(|| {
            format!(
                "field '{name}' is a shadow of 'id' and must carry the same value, but this \
                 document has {name}={value} and id={id}. A shadow field is a name for the \
                 identifier rather than a field of its own, so nothing would store {value} and \
                 a later read would report {id} under '{name}'. Write the value under a \
                 different field name, or correct the identifier."
            )
        })
    })
}

/// The field type a value would be given if the schema had never seen the field.
///
/// One reading of a JSON value, shared by every write path. It is deliberately not applied to
/// `id`: the document key is text whatever it looks like, and inferring `i64` from a numeric
/// identifier is how an index came to declare `id` as a type the key it builds does not use.
///
/// A string is examined before it is called text, because the shapes that follow are what the
/// engine can index as something better: an RFC3339 or naive timestamp is a date, and an
/// address is an ip. An array reads as text — the write path serializes it into a text field
/// and tokenizes that — and an object as json. Null reads as text because null is the absence
/// of a value, and text is the only type that can hold the absence of one.
fn infer_field_type(value: &JsonValue) -> TantivyFieldType {
    match value {
        JsonValue::String(s) => {
            if chrono::DateTime::parse_from_rfc3339(s).is_ok()
                || is_naive_datetime(s)
                || is_naive_date(s)
            {
                TantivyFieldType::Date
            } else if s.parse::<std::net::IpAddr>().is_ok() {
                TantivyFieldType::Ip
            } else {
                TantivyFieldType::Text
            }
        }
        JsonValue::Number(n) => {
            if n.is_i64() {
                TantivyFieldType::I64
            } else if n.is_u64() {
                TantivyFieldType::U64
            } else {
                TantivyFieldType::F64
            }
        }
        JsonValue::Bool(_) => TantivyFieldType::Boolean,
        JsonValue::Array(_) => TantivyFieldType::Text,
        JsonValue::Object(_) => TantivyFieldType::Json,
        JsonValue::Null => TantivyFieldType::Text,
    }
}

/// Whether a field declared as one type can hold a single value that reads as another.
///
/// `String` is `Text` under an older name, and a numeric field widens to a float, because `4`
/// and `4.5` in one field is a source being loose about a value rather than a document that
/// cannot be stored. Everything else has to match: a declared `i64` receiving a word has
/// nowhere to put it, and the write path would skip the value and leave the document stored
/// with the field silently unindexed.
///
/// Values whose *shape* rather than type decides the answer — a list, a null, a text or json
/// field that takes anything, a facet path — are settled by `unstorable_value` before this is asked.
fn scalar_type_is_storable(declared: &TantivyFieldType, inferred: &TantivyFieldType) -> bool {
    if declared == inferred {
        return true;
    }
    matches!(
        (declared, inferred),
        (TantivyFieldType::String, TantivyFieldType::Text)
            | (
                TantivyFieldType::I64 | TantivyFieldType::U64,
                TantivyFieldType::F64
            )
    )
}

/// Why the field cannot hold this value, if it cannot.
///
/// The question is what the write path can store, which is a wider question than whether two
/// type names match — see `storage::add_json_value_to_doc`, which is the other half of this
/// answer and has to agree with it exactly. Where they disagree the cost is silent: a value the
/// validator waves through and the writer skips leaves a document stored with that field
/// unindexed, and a query over the field simply never matches it.
///
/// **A list is several values of the field.** Every tantivy field is multivalued — two `add_i64`
/// calls under one field store two values of it, the fast column reports
/// `Cardinality::Multivalued`, and a range query matches the document if any one value falls
/// inside. So `{"risk_score": [9, 12]}` belongs in an `i64` field, and each element is checked
/// on its own. One level is flattened and no more, which is exactly what the writer does with
/// it: a list inside a list is a value the element type has to accept by itself.
///
/// **A text or json field takes anything**, because the writer serializes whatever it is given
/// into text. A list under one of those is indexed as its own JSON text rather than split, so
/// it is not looked into here either. **Bytes** wants a list of byte values and nothing else.
///
/// **Null is the absence of a value**, and any field may be absent. The writer adds nothing, no
/// query matches it, and the document reads back exactly as written — so refusing a document
/// for carrying an explicit null where it could have omitted the key would be a distinction
/// without a difference to anything downstream.
fn unstorable_value(field: &str, declared: &TantivyFieldType, value: &JsonValue) -> Option<String> {
    if value.is_null() {
        return None;
    }

    match declared {
        // Takes the value whole, whatever shape it has.
        TantivyFieldType::Text | TantivyFieldType::Json => None,
        // Takes a list of byte values, and only that. Not routed through the element-wise
        // branch below, because a list means something different here: `[1, 2]` under an `i64`
        // is two values of the field, while under `bytes` it is one two-byte value. So every
        // element has to fit, and one that does not refuses the value rather than itself.
        TantivyFieldType::Bytes => match value.as_array() {
            Some(items) => items
                .iter()
                .find_map(storage::byte_value_error)
                .map(|why| format!("field '{field}': {why}")),
            None => Some(type_mismatch(field, declared, value)),
        },
        _ => match value.as_array() {
            // Several values of the field, each held to the declared type on its own.
            Some(items) => items
                .iter()
                .find_map(|item| unstorable_scalar(field, declared, item)),
            None => unstorable_scalar(field, declared, value),
        },
    }
}

/// Why the field cannot hold one value of it, if it cannot.
///
/// Split out so that a list and a scalar are judged identically: the writer flattens a list into
/// one `add_*` call per element, so an element the field cannot hold is exactly as unstorable in
/// a list of three as it is on its own.
///
/// A facet is the one type whose *value* rather than type decides the answer — `"/a/b"` and
/// `"a/b"` are both strings and only one is a path — so it is asked of the parser the writer
/// uses, `storage::facet_path_error`, rather than of `infer_field_type`, which reads every
/// facet path as text and would refuse the whole type.
fn unstorable_scalar(
    field: &str,
    declared: &TantivyFieldType,
    value: &JsonValue,
) -> Option<String> {
    if value.is_null() {
        return None;
    }

    if matches!(declared, TantivyFieldType::Facet) {
        return match value.as_str() {
            Some(path) => {
                storage::facet_path_error(path).map(|why| format!("field '{field}': {why}"))
            }
            None => Some(type_mismatch(field, declared, value)),
        };
    }

    let inferred = infer_field_type(value);
    (!scalar_type_is_storable(declared, &inferred)).then(|| type_mismatch(field, declared, value))
}

/// The one wording for a value whose type the field cannot hold.
fn type_mismatch(field: &str, declared: &TantivyFieldType, value: &JsonValue) -> String {
    format!(
        "Type mismatch for field '{field}': expected {declared:?}, got {:?}",
        infer_field_type(value)
    )
}

/// The sort field a search cannot be answered with, if the caller named one.
///
/// Why the engine will refuse to order by this field, if it will.
///
/// A sort fails in every shard at once or in none of them, and scatter-gather reports the first
/// as a partial outage: 200, an empty `hits` array, and the reason buried in per-shard `errors`,
/// which reads as "nothing matched". So a refusal the engine is certain to issue is issued here,
/// before the fan-out, where it can be an error about the request.
///
/// The question asked is the engine's own — "can I order by this column?" — not the narrower
/// "does a column of this name exist". Both refusals reach the caller identically, so checking
/// only for the name lets the other kind through.
///
/// What may be sorted on:
/// - a field with a fast column, which the collector orders by.
/// - a text or string field without one, which is ordered after the fetch. Approximate rather
///   than refused, and [`APPROXIMATE_SORT_FIELD`] says so.
/// - `id`, which every Tantivy schema carries and no `IndexSchema` lists.
/// - a shadow field, which is the caller's name for `id`. The engine sorts by the key's column
///   exactly as it queries by it, so both names order by the same values.
///
/// What may not: a name absent from the schema, `_seq`, or a non-text field without a fast
/// column. `_`-prefixed names are not waved through here the way [`unknown_projection_fields`]
/// waves them through: a projection asking for response metadata is meaningful, a sort on it is
/// not.
///
/// Decided from the declared schema, which is what a router holds without opening an index. That
/// leaves one refusal undecidable here: `fast` is a declaration and the column is written from it
/// at index time, so a numeric field declared `fast` after its index was built has no column to
/// order by and only the built index knows. Every other refusal the engine can reach is reachable
/// from the declaration.
///
/// Skipped entirely for an index whose schema is not known yet, where every field would look
/// unknown.
fn unsortable_sort_field(
    schema: &IndexSchema,
    sort: Option<&SortSpec>,
) -> Option<OrchestratorError> {
    let field = &sort?.field;
    if schema.fields.is_empty() {
        return None;
    }

    // The document key, under its own name or under a shadow name that stands for it.
    if field == "id" || schema.is_shadow_field(field) {
        return None;
    }

    let refuse = |reason: String| {
        Some(OrchestratorError::UnsortableField {
            field: field.clone(),
            reason,
        })
    };

    // Retired, and so present only in the schema record of an index built before it was retired
    // — which is why its absence from the record cannot be what refuses it. Every field listing
    // hides it, and the engine refuses it in each shard.
    if field == "_seq" {
        return refuse("the field is internal and no index reports it".to_string());
    }

    let Some(def) = schema.fields.get(field) else {
        return refuse("the index has no column of that name".to_string());
    };

    if def.is_fast()
        || matches!(
            def.field_type,
            TantivyFieldType::Text | TantivyFieldType::String
        )
    {
        return None;
    }

    // Two different refusals, because a caller can act on one of them and not the other. A
    // numeric or date field is one schema edit and a rebuild away from sorting; a boolean, bytes,
    // ip, json or facet field is never getting a column, since the index builder adds those types
    // without reading `fast` at all. Telling the second kind to "declare it fast" sends the caller
    // to make an edit that changes nothing — and one that used to *look* like it worked, because
    // the declaration was reported back as `true` while the guard waved the sort through to fail
    // in every shard.
    if !FieldDef::can_be_fast(&def.field_type) {
        return refuse(format!(
            "a {} field has no column to sort on, and declaring it fast cannot give it one",
            def.field_type.to_string()
        ));
    }

    refuse(format!(
        "a {} field must be declared fast to sort, and this one is not",
        def.field_type.to_string()
    ))
}

/// Collect the distinct dropped clauses from per-node responses.
///
/// Cross-node merges see [`DISCARDED_CLAUSES_FIELD`] as JSON rather than as a typed reply.
fn collect_discarded(responses: &[JsonValue]) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for response in responses {
        let Some(notes) = response
            .get(DISCARDED_CLAUSES_FIELD)
            .and_then(|value| value.as_array())
        else {
            continue;
        };
        for note in notes.iter().filter_map(|note| note.as_str()) {
            if !out.iter().any(|existing| existing == note) {
                out.push(note.to_string());
            }
        }
    }
    out
}

/// Response key naming the field whose sort order is an approximation.
///
/// Absent when the order is exact, which is the common case — present means the hits are in the
/// alphabetical order of a *sample* of the matches rather than of all of them. See
/// [`storage::SearchOutcome::approximate_sort`].
///
/// Carries the field name rather than `true`, because the caller's next move is to look that
/// field up in the schema and see that it has no fast column.
pub const APPROXIMATE_SORT_FIELD: &str = "_approximate_sort";

/// Attach [`APPROXIMATE_SORT_FIELD`] to a search response, if the order returned is approximate.
fn attach_approximate_sort(response: &mut JsonValue, field: Option<String>) {
    let Some(field) = field else {
        return;
    };
    if let Some(obj) = response.as_object_mut() {
        obj.insert(APPROXIMATE_SORT_FIELD.to_string(), JsonValue::String(field));
    }
}

/// The approximated field from per-node responses, if any node reported one.
///
/// One field, not a list: every node ran the same sort on the same field, so either that field
/// has a fast column everywhere or it has one nowhere. A node whose shards are all empty reports
/// nothing at all, which is why the first answer wins rather than requiring agreement.
fn collect_approximate_sort(responses: &[JsonValue]) -> Option<String> {
    responses.iter().find_map(|response| {
        response
            .get(APPROXIMATE_SORT_FIELD)
            .and_then(|value| value.as_str())
            .map(str::to_string)
    })
}

/// Produce a comparable sort key for a hit's raw field value.
///
/// Date fields are parsed to epoch seconds so that cross-node merges order them
/// chronologically (matching each shard's FAST-field ordering) rather than by
/// lexicographic string comparison, which breaks across mixed date formats/offsets.
/// Every other value passes through unchanged — the merge comparator handles the
/// numeric-vs-string distinction. Returns `None` when the value cannot be keyed
/// (e.g. an unparseable date string), in which case the hit sorts last.
fn normalize_sort_key(value: &JsonValue, field_def: Option<&FieldDef>) -> Option<JsonValue> {
    if let Some(def) = field_def
        && matches!(def.field_type, TantivyFieldType::Date)
    {
        return value
            .as_str()
            .and_then(storage::parse_date_to_timestamp_secs)
            .map(|ts| JsonValue::Number(ts.into()));
    }
    Some(value.clone())
}

/// Attach the `SORT_KEY_FIELD` metadata value to each gathered hit, in place, so that
/// downstream merges (local multi-shard and cross-node) have a projection-independent
/// key to order by. No-op for hits lacking the sort field or an unparseable date.
///
/// The key is read under the name the *document* carries, which is not always the name the
/// caller sorted by. A sort on the document key is the case: an index with shadow fields
/// answers with the shadow name in place of `id`, so `sort=id` and `sort=<shadow>` both have to
/// look for whichever of the two is on the hit. Reading only the caller's name leaves every hit
/// unstamped, and an unstamped merge keeps each shard's block whole — a per-shard order
/// presented as a global one.
fn stamp_sort_keys(hits: &mut [(Uuid, f32, JsonValue)], spec: &SortSpec, schema: &IndexSchema) {
    let field_def = schema.fields.get(&spec.field);
    let sorts_by_document_key = spec.field == "id" || schema.is_shadow_field(&spec.field);
    let fallback = sorts_by_document_key.then(|| storage::document_key_field(schema));

    for (_, _, doc) in hits.iter_mut() {
        if let JsonValue::Object(o) = doc
            && let Some(raw) = o
                .get(&spec.field)
                .or_else(|| fallback.as_deref().and_then(|name| o.get(name)))
                .or_else(|| sorts_by_document_key.then(|| o.get("id")).flatten())
            && let Some(key) = normalize_sort_key(raw, field_def)
        {
            o.insert(SORT_KEY_FIELD.to_string(), key);
        }
    }
}

/// Remove the internal `SORT_KEY_FIELD` from every hit in a search response, in place.
/// Called once at the client boundary so the key never leaks to callers.
fn strip_sort_keys(response: &mut JsonValue) {
    if let Some(hits) = response.get_mut("hits").and_then(|h| h.as_array_mut()) {
        for hit in hits.iter_mut() {
            if let Some(o) = hit.as_object_mut() {
                o.remove(SORT_KEY_FIELD);
            }
        }
    }
}

/// Compare two hit documents by a named field for field-sorted search merges.
///
/// Integer values are compared as `i64` first (so keys beyond f64's exact-integer
/// range, e.g. large ids or nanosecond timestamps, order precisely); otherwise values
/// are compared as `f64`, then fall back to string comparison. Documents missing the
/// field always sort last, regardless of the requested order.
fn compare_hits_by_field(
    a: &JsonValue,
    b: &JsonValue,
    field: &str,
    order: SortOrder,
) -> std::cmp::Ordering {
    use std::cmp::Ordering;

    match (a.get(field), b.get(field)) {
        (Some(x), Some(y)) => {
            let base = match (x.as_i64(), y.as_i64()) {
                (Some(nx), Some(ny)) => nx.cmp(&ny),
                _ => match (x.as_f64(), y.as_f64()) {
                    (Some(nx), Some(ny)) => nx.partial_cmp(&ny).unwrap_or(Ordering::Equal),
                    _ => match (x.as_str(), y.as_str()) {
                        (Some(sx), Some(sy)) => sx.cmp(sy),
                        _ => Ordering::Equal,
                    },
                },
            };
            match order {
                SortOrder::Asc => base,
                SortOrder::Desc => base.reverse(),
            }
        }
        // Present values sort before missing ones, independent of order.
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}

/// Compare two hits by what the caller asked to order on.
///
/// Relevance score descending by default — the engine's own ranking, and the order in which it
/// hands hits back — or the injected `SORT_KEY_FIELD` when a sort was requested. Keyed on that
/// metadata field rather than on the sort field itself, because projection may have removed
/// the latter from the hit.
pub(crate) fn compare_hits_primary(
    a: &JsonValue,
    b: &JsonValue,
    sort: Option<&SortSpec>,
) -> std::cmp::Ordering {
    match sort {
        Some(spec) => compare_hits_by_field(a, b, SORT_KEY_FIELD, spec.order),
        None => hit_score(b)
            .partial_cmp(&hit_score(a))
            .unwrap_or(std::cmp::Ordering::Equal),
    }
}

/// Order one node's shard hits, deterministically.
///
/// The tuple form of [`order_hit_blocks`], for the scatter paths that hold a shard id and a
/// score beside each document rather than a finished hit. Shards are polled concurrently and
/// answer in whatever order they finish, so a tie falls back to the shard's id — fixed when the
/// shard was created — and then to the hit's place in that shard's own ordering, which Tantivy
/// has already made total. `results` holds each shard's hits contiguously, so a comparison of
/// positions within one shard is a comparison within its block.
fn order_shard_hits(results: &mut Vec<(Uuid, f32, JsonValue)>, sort: Option<&SortSpec>) {
    let mut ranked: Vec<(usize, (Uuid, f32, JsonValue))> =
        std::mem::take(results).into_iter().enumerate().collect();

    ranked.sort_by(|(left_position, left), (right_position, right)| {
        let primary = match sort {
            Some(spec) => compare_hits_by_field(&left.2, &right.2, SORT_KEY_FIELD, spec.order),
            None => right
                .1
                .partial_cmp(&left.1)
                .unwrap_or(std::cmp::Ordering::Equal),
        };
        primary
            .then_with(|| left.0.cmp(&right.0))
            .then_with(|| left_position.cmp(right_position))
    });

    *results = ranked.into_iter().map(|(_, tuple)| tuple).collect();
}

/// The slice of an ordered result a caller asked for.
///
/// Kept as one value rather than two loose numbers because the two are not independent, and the
/// relationship between them is the whole of paging: see [`SearchWindow::fetch_count`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SearchWindow {
    /// How many ordered hits to discard before the first one returned.
    pub offset: usize,
    /// How many to return after that.
    pub limit: usize,
}

impl SearchWindow {
    /// The first `limit` hits — what every caller that does not page asks for.
    pub fn first(limit: usize) -> Self {
        SearchWindow { offset: 0, limit }
    }

    /// Resolve a request's `limit` and `offset` into a window, or say why it cannot be served.
    ///
    /// Every request surface goes through here, so that all of them apply the node's default and
    /// its ceiling the same way. Two things it settles that a per-surface check kept getting
    /// wrong:
    ///
    /// An absent `limit` means the node's default, not zero. Bounding `offset + 0` lets
    /// `offset = max_search_limit` past a check the engine then exceeds by the default, so the
    /// advertised ceiling was not the real one.
    ///
    /// The ceiling applies to `offset + limit` rather than to `limit`, because that sum is what
    /// gets fetched: every source is asked for the whole window from the front (see
    /// [`Self::fetch_count`]), and Tantivy's collector allocates against the number it is given
    /// before it has matched anything. So a deep page is exactly as expensive as a large limit,
    /// and `max_search_limit` has to bound both or it bounds neither.
    pub fn checked(
        limit: Option<usize>,
        offset: Option<usize>,
        default_limit: usize,
        max_search_limit: usize,
    ) -> Result<Self, String> {
        let window = SearchWindow {
            offset: offset.unwrap_or(0),
            limit: limit.unwrap_or(default_limit),
        };

        if window.limit > max_search_limit {
            return Err(format!(
                "limit {} is above the maximum of {max_search_limit}; ask for at most that many \
                 hits, or narrow the query",
                window.limit
            ));
        }

        if window.fetch_count() > max_search_limit {
            return Err(format!(
                "offset {} + limit {} = {} is above the maximum of {max_search_limit}; the \
                 engine fetches offset + limit hits, so a page this deep costs what a limit that \
                 large costs. Narrow the query, or sort on a field that lets you resume from the \
                 last hit instead of paging.",
                window.offset,
                window.limit,
                window.fetch_count()
            ));
        }

        Ok(window)
    }

    /// How many hits each source must return for this window to be servable from them.
    ///
    /// `offset + limit`, not `limit`. The skip happens once, after every source's hits have been
    /// merged into one order, so a source that returned only `limit` would leave the window short
    /// as soon as `offset` was non-zero.
    ///
    /// The tempting alternative — telling each source to skip `offset` itself — is wrong rather
    /// than merely different. Every hit in the window may come from a single source, so a source
    /// that skipped `offset` of *its own* hits would drop rows that belong in the answer and
    /// promote rows that do not. This is why Tantivy's own `and_offset` is not used here: it is
    /// the right tool for one segment and the wrong one for a scatter-gather.
    pub fn fetch_count(&self) -> usize {
        self.offset.saturating_add(self.limit)
    }

    /// Take this window out of a sequence that is already in its final order.
    pub fn apply<T>(&self, ordered: Vec<T>) -> Vec<T> {
        ordered
            .into_iter()
            .skip(self.offset)
            .take(self.limit)
            .collect()
    }
}

/// Order hits gathered from several sources and return the requested window of them.
///
/// `blocks` arrive in the order the sources were **dispatched**, never the order they answered,
/// and that is what makes this deterministic. Neither key a caller can order on is a total
/// order: every document matching one term scores identically, and a sort field repeats as
/// readily as any other value. Where a tie is settled by whichever source replied first, two
/// runs of one query return different documents — measured, not theorised — and a page of such
/// results is a page of nothing.
///
/// So a tie falls back to the source's rank, then to the hit's place within that source's own
/// ordering. Both are fixed before any result arrives, and each source has already ordered its
/// own hits totally — a shard through Tantivy, which breaks its own ties on document address;
/// a node through this same function. The composition is therefore one order, identical on
/// every run.
///
/// Every hit is held before sorting rather than kept in a running top-K. The bound is the same
/// either way, `window.fetch_count()` per source, and a running top-K cannot be made to agree
/// with this: it must decide what to discard while later sources are still unheard, so its answer
/// depends on the order they answer in — exactly what this function exists to remove.
///
/// The window is taken *after* the merge, which is what makes page *k* mean the same thing here
/// as it would on a single source — see [`SearchWindow::fetch_count`].
pub(crate) fn order_hit_blocks(
    blocks: Vec<Vec<JsonValue>>,
    sort: Option<&SortSpec>,
    window: SearchWindow,
) -> Vec<JsonValue> {
    let mut ranked: Vec<(usize, usize, JsonValue)> = blocks
        .into_iter()
        .enumerate()
        .flat_map(|(rank, hits)| {
            hits.into_iter()
                .enumerate()
                .map(move |(position, hit)| (rank, position, hit))
        })
        .collect();

    ranked.sort_by(
        |(left_rank, left_position, left), (right_rank, right_position, right)| {
            compare_hits_primary(left, right, sort)
                .then_with(|| left_rank.cmp(right_rank))
                .then_with(|| left_position.cmp(right_position))
        },
    );

    window
        .apply(ranked)
        .into_iter()
        .map(|(_, _, hit)| hit)
        .collect()
}

/// The key a write is routed by, in the order of precedence that decides it.
///
/// Written out identically in three places before this existed — the engine fast path and both
/// halves of the actor path — which is a rule that has to agree with itself to be a rule at all.
/// Whatever routes a document has to reach the same shard every time, or the same id lands in
/// two places and a search returns it twice.
///
/// 1. **The document's own routing field.** The schema names it, and it is the authority: a
///    shadow index routes by the source's name for the key, a tenant index by the tenant.
/// 2. **What the caller asked for.** Honoured only where the document does not answer, so a
///    caller cannot move a document off the shard its schema puts it on.
/// 3. **The id**, which is what the routing field resolves to on a default index anyway.
/// 4. **A hash of the document**, so an unkeyed write is still deterministic rather than random.
fn effective_routing_key(
    schema: &IndexSchema,
    id: &str,
    routing_key: Option<String>,
    doc: &JsonValue,
) -> Option<String> {
    extract_routing_value(doc, schema.get_routing_field())
        .or(routing_key)
        .or_else(|| (!id.is_empty()).then(|| id.to_string()))
        .or_else(|| derive_routing_key_from_doc(doc))
}

/// The key a delete is routed by, or the reason it cannot be routed.
///
/// A write reads its routing key out of the document, which outranks everything the caller sent.
/// A delete has no document, so only two of those four rungs are left — and which of them applies
/// is decided entirely by the schema:
///
/// - **The routing field is the key.** `id` by default, or a shadow field, whose value *is* the
///   document key by definition. The id routes, exactly as the original write did.
/// - **The routing field is some other field**, a tenant or a customer. The id says nothing about
///   which shard holds the row, so the caller has to supply the same key the write used.
///
/// The refusal in that second case is deliberate and recorded as a non-goal: fanning a keyless
/// delete out to every shard on every node is *correct*, since a shard without the id removes
/// nothing, but it costs `shards × nodes` writer transactions to remove one row and the code
/// already refuses to broadcast a write. The error names the field so the caller knows what to
/// send, which it can read off the document with one search.
fn effective_delete_routing_key(
    schema: &IndexSchema,
    id: &str,
    routing_key: Option<String>,
) -> Result<String, OrchestratorError> {
    if let Some(key) = routing_key {
        return Ok(key);
    }

    let routing_field = schema.get_routing_field();
    if routing_field == "id" || schema.is_shadow_field(routing_field) {
        return Ok(id.to_string());
    }

    Err(OrchestratorError::Io(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        format!(
            "index routes by '{routing_field}', which is not the document key, so a delete must              carry the same routing_key the write used — read it off the document with a search              for id:{id}"
        ),
    )))
}

/// Helper function to detect if an operation is a write operation
/// Helper function to detect if an operation is a write operation
fn is_write_operation(op: &ClientOp) -> bool {
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
// Streaming Search Results
// ============================================================================

/// Represents a single search result from a shard or remote node
#[derive(Debug)]
pub enum StreamingSearchResult {
    /// Result from a local microshard
    Local {
        #[allow(dead_code)] // Constructed for completeness; matched with wildcard
        shard_id: Uuid,
        hits: Vec<(f32, serde_json::Value)>,
        total_hits: usize,
        #[allow(dead_code)] // Constructed for completeness; matched with wildcard
        took_ms: u64,
    },
    /// Result from a remote node
    Remote {
        node_id: Uuid,
        result: Result<serde_json::Value, OrchestratorError>,
    },
}

// ============================================================================
// Remote Actor Naming Constants
// ============================================================================

/// Generate the remote actor name for a NodeOrchestrator.
pub fn orchestrator_remote_name(node_id: &Uuid) -> String {
    format!("orchestrator-{}", node_id)
}

// ============================================================================
// Enhanced Schema Sampling for Initial Creation
// ============================================================================

/// Enhanced schema sampling for initial schema creation
///
/// This function implements Proposal 2: Improve Sampling Strategy by using multiple documents
/// to improve type detection accuracy during initial schema creation.
///
/// Key Benefits:
/// - Reduces false positives/negatives in type detection
/// - Handles edge cases where first document has unusual data
/// - Provides confidence scoring through majority voting
/// - Matches client crate behavior for consistency
///
/// Algorithm:
/// 1. Sample up to SCHEMA_SAMPLE_LIMIT documents (200, same as client)
/// 2. Evolve schema incrementally using storage layer's evolve_from_document
/// 3. Storage layer handles type compatibility and evolution rules
/// 4. Mark what came out of it indexed — see [`mark_initial_fields_indexed`]
///
/// Usage:
/// - Only used during initial schema creation (empty schema)
/// - Existing schema evolution continues to use current logic
fn enhanced_schema_sampling(docs: &[DocPayload], sample_limit: usize) -> IndexSchema {
    let mut schema = IndexSchema::default();
    let mut sampled = 0usize;

    // Sample documents for better type detection
    for doc_payload in docs.iter() {
        if sampled >= sample_limit {
            break;
        }

        // Evolve schema based on this document
        schema.evolve_from_document(&doc_payload.doc);
        sampled += 1;
    }

    // `evolve_from_document` builds fields through `FieldDef::new_non_indexed`, which is the
    // right default for its usual caller but not here: this schema is about to *create* the
    // tantivy index, so it is the one moment fields can still be made searchable.
    mark_initial_fields_indexed(&mut schema);

    tracing::info!(
        sampled_docs = sampled,
        total_docs = docs.len(),
        sample_limit = sample_limit,
        "Enhanced schema sampling completed for initial schema creation"
    );

    schema
}

/// Make every non-shadow field of a not-yet-created index searchable.
///
/// The split between creation and evolution is forced by tantivy, not chosen: a tantivy
/// `Schema` is fixed at `Index::create_in_dir`, and the storage layer builds it from
/// whatever `IndexSchema` is persisted at that moment. A field marked `indexed` after the
/// index exists gets no field handle, so the write path skips it and the field is searchable
/// only by rebuilding the index. Fields discovered *later* are therefore deliberately
/// non-indexed: they exist to keep the redb and tantivy views of a document consistent.
///
/// Fields discovered *now* have no such excuse — this is the last moment they can be indexed
/// at all, so they are. `stored` stays off for everything but `id`: hits are reconstructed
/// from redb, and storing field values in tantivy as well would duplicate the corpus.
///
/// This is the same rule the bundled client applies before it PUTs a detected schema, which
/// is why `cameodb data load` produces searchable indexes and a plain HTTP write did not.
fn mark_initial_fields_indexed(schema: &mut IndexSchema) {
    for (name, field_def) in schema.fields.iter_mut() {
        // Shadow fields preserve an original field name for query mapping and are never
        // indexed or stored — leave them exactly as they are.
        if field_def.is_shadow {
            continue;
        }
        field_def.indexed = true;
        field_def.stored = name == "id";
    }
}

// ============================================================================
// Date Parsing Helper Functions
// ============================================================================

/// Check common naive datetime formats (no timezone) such as
/// - 2024-05-01 12:30:00
/// - 2024-05-01 12:30
/// - 2024-05-01T12:30:00
/// - 2024-05-01T12:30:00.123
fn is_naive_datetime(s: &str) -> bool {
    const NAIVE_DATETIME_FORMATS: &[&str] = &[
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M",
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S%.f",
    ];

    NAIVE_DATETIME_FORMATS
        .iter()
        .any(|fmt| NaiveDateTime::parse_from_str(s, fmt).is_ok())
}

/// Check common date-only formats such as
/// - 2024-05-01
/// - 2024/05/01
/// - 20240501
fn is_naive_date(s: &str) -> bool {
    const NAIVE_DATE_FORMATS: &[&str] = &["%Y-%m-%d", "%Y/%m/%d", "%Y%m%d", "%Y-%m", "%Y"];

    NAIVE_DATE_FORMATS
        .iter()
        .any(|fmt| NaiveDate::parse_from_str(s, fmt).is_ok())
}

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

/// Type alias for shard hydration task results
type ShardTaskResult = Result<(Uuid, Option<MicroshardActor>), OrchestratorError>;

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
    /// Memory pressure threshold used for deriving usable cache capacity
    pub memory_pressure_threshold_percent: u8,
    /// Number of threads for the dedicated read (search/stats) runtime
    pub search_threads: usize,
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
    /// The safety net under the operation-count threshold: writes that stop short of the
    /// threshold would otherwise sit uncommitted and unsearchable until the next write.
    pub supervisor_timeout_secs: u64,
    /// Pin per-shard writer threads to the core given by the shard's dense ordinal.
    /// Improves cache locality and reduces cross-core wakeups under heavy write load.
    /// Default: false (no pinning, OS scheduler decides).
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
}

impl Default for NodeConfig {
    fn default() -> Self {
        let default_path = PathBuf::from("./data/cameodb");
        Self {
            storage_path: default_path.clone(),
            storage_paths: vec![default_path],
            max_shards: 8,
            indexer_memory_min_mb: 16,
            indexer_memory_max_mb: 256,
            total_memory_limit_mb: 2048,
            memory_pressure_threshold_percent: 80,
            search_threads: 8,
            wal_sync: true,
            default_batch_size: 1000,
            indexer_num_threads: 1,
            merge_num_threads: 2,
            writer_shutdown_timeout_secs: 30,
            supervisor_timeout_secs: 5,
            writer_core_affinity: true,
            shard_affine_dispatch: false,
            worker_core_affinity: false,
        }
    }
}

/// Errors that can occur during node orchestration operations.
#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, thiserror::Error)]
pub enum OrchestratorError {
    #[error("identity error: {0}")]
    Identity(#[from] IdentityError),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

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
}

// Serialize/Deserialize via display string to satisfy remote message bounds without
// requiring downstream error types to implement serde traits.
impl Serialize for OrchestratorError {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for OrchestratorError {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(OrchestratorError::Io(std::io::Error::other(s)))
    }
}

/// Append names not already present, keeping the list sorted and free of duplicates.
///
/// Shard verdicts on a schema update overlap almost entirely — they are reading copies of the
/// same schema — so merging them is a union, not a concatenation.
fn merge_names(into: &mut Vec<String>, from: Vec<String>) {
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
fn describe_schema_refusal(outcome: &SchemaFieldUpdate) -> String {
    format!(
        "Schema update refused, nothing was changed: no such field in this schema: {}",
        outcome.unknown.join(", ")
    )
}

/// What a caller has to do about a flag that cannot take effect yet.
fn describe_pending_reindex(outcome: &SchemaFieldUpdate) -> String {
    format!(
        "Marked indexed and saved, but not searchable yet: {}. The index was built before these \
         fields were declared, so it has no column for them. Rebuilding the index data from the \
         schema is what makes them searchable — delete the index data without deleting the \
         schema, then re-ingest. Until then a query naming them matches nothing, and says so \
         rather than returning a narrower answer as though it were complete.",
        outcome.pending_reindex.join(", ")
    )
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

/// Message to get the current shard count.
#[derive(Debug, Clone)]
pub struct GetShardCount;

/// Message to propose creating a new shard on this node.
#[derive(Debug, Clone)]
pub struct ProposeShard {
    pub shard_id: Uuid,
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
        }
    }
}

impl From<RemoteError> for OrchestratorError {
    fn from(err: RemoteError) -> Self {
        match err {
            // The kind is kept for this one: a peer refusing the request — an unsortable sort
            // field, say — has to stay distinguishable from a peer that failed, because the
            // HTTP surface answers 400 for the first and 500 for the second.
            RemoteError::InvalidInput(s) => {
                OrchestratorError::Io(std::io::Error::new(std::io::ErrorKind::InvalidInput, s))
            }
            RemoteError::Io(s)
            | RemoteError::Identity(s)
            | RemoteError::NotFound(s)
            | RemoteError::Other(s) => OrchestratorError::Io(std::io::Error::other(s)),
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
    /// Nothing survived the parse on this shard; see [`storage::SearchOutcome::emptied`].
    ///
    /// Defaulted for the same reason as `discarded`. A peer that does not send it reads as
    /// "the query ran", which keeps an older peer answering rather than having its hits
    /// refused on a claim it never made.
    #[serde(default)]
    pub emptied: bool,
}

/// Client operation messages for RouterActor.
#[cfg_attr(not(test), allow(dead_code))]
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
    },
    /// Bulk write operation to insert/update multiple documents
    BulkWrite {
        index: String,
        docs: Vec<DocPayload>,
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
    },
    /// Create or update index configuration/schema
    CreateConfig { index: String, schema: IndexSchema },
    /// Set the `indexed` flag on named fields of an existing schema.
    ///
    /// Distinct from `CreateConfig` because it must *not* re-create the Tantivy index: the
    /// index is already open, and replaying a create against it fails on the writer lockfile.
    /// It edits the stored schema in place, so no property is erased by the edit.
    UpdateSchema {
        index: String,
        field_updates: BTreeMap<String, bool>,
    },
    /// Get index configuration/schema
    GetConfig { index: String },
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
        reply: tokio::sync::oneshot::Sender<Result<(Vec<u64>, usize), StoreError>>,
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
    Shutdown,
}

/// What a worker did with an op.
///
/// The engine cannot serve every op — schema evolution and bulk writes need `&mut
/// NodeOrchestrator`. Rather than signalling that with a sentinel error, which leaves the
/// caller holding nothing to retry with, [`WorkerOutcome::UseActor`] sends the op itself
/// home. The caller moved the op into the job, so this is the only way it can get it back
/// without cloning every document on the way in.
pub enum WorkerOutcome {
    /// The engine handled it. Success or failure, this is the client's answer.
    Done(Result<JsonValue, OrchestratorError>),
    /// The engine declined; retry this op on the actor mailbox.
    UseActor(Box<ClientOp>),
}

/// The engine's verdict on a single write, before it becomes a [`WorkerOutcome`].
enum WriteOutcome {
    Done(JsonValue),
    /// The schema has to grow first. Carries back the parts of `ClientOp::Write` that
    /// `engine_write` consumed, so `execute` can rebuild the op — `index` it still owns.
    NeedsActor {
        id: String,
        routing_key: Option<String>,
        doc: JsonValue,
    },
}

/// A job dispatched to the orchestrator worker pool.
/// Workers execute the operation on shared state and send the result
/// back via the oneshot channel, bypassing the actor mailbox.
pub enum OrchestratorJob {
    Execute {
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
pub struct CoreLayout {
    /// How many cores this process may use. Sizes the worker pool, and is meaningful even
    /// where pinning is unsupported.
    budget: usize,
    /// Cores that can actually be pinned to, capped to `budget`. Empty when the platform
    /// cannot enumerate them, in which case every pinning path degrades to unpinned.
    cores: Vec<core_affinity::CoreId>,
}

impl CoreLayout {
    fn detect() -> Self {
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
    fn budget(&self) -> usize {
        self.budget
    }

    /// The core an ordinal maps to, or `None` when pinning is unavailable here.
    fn core_for(&self, ordinal: usize) -> Option<core_affinity::CoreId> {
        if self.cores.is_empty() {
            None
        } else {
            Some(self.cores[ordinal % self.cores.len()])
        }
    }

    fn pinning_available(&self) -> bool {
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
pub struct ShardPlacement {
    slots: HashMap<Uuid, ShardSlot>,
    /// Shards actually serving on this node. A superset relationship with `slots` is the
    /// point: an ordinal is handed out before a shard starts, because its writer thread pins
    /// itself as it spawns, but the shard only becomes routable once it is in the shard map.
    /// A shard that fails to hydrate, or that the `max_shards` cap turns away, keeps its
    /// ordinal and never becomes live — claiming it locally would route writes to a shard
    /// this node cannot serve.
    live: HashSet<Uuid>,
    next: usize,
}

/// A shard's place in the pool, and where its writer thread actually ended up.
#[derive(Clone, Debug)]
struct ShardSlot {
    ordinal: usize,
    /// Core the writer thread was asked to take. `None` when pinning is off or the platform
    /// cannot enumerate cores.
    target_core: Option<usize>,
    /// Core the writer thread reports it is running on, or [`UNPINNED`] if the request was
    /// refused. Shared with the thread, which writes it once at startup — a request is not
    /// an outcome, and on macOS every request is refused.
    pinned_core: Arc<AtomicI64>,
}

/// `pinned_core` sentinel: this thread is not pinned to anything.
const UNPINNED: i64 = -1;

/// What a shard's writer thread should pin to, and where it reports what happened.
#[derive(Clone, Debug)]
pub struct WriterPin {
    target: Option<core_affinity::CoreId>,
    outcome: Arc<AtomicI64>,
}

impl WriterPin {
    /// Pin the calling thread, and record where it actually landed.
    fn apply(&self, shard_id: Uuid) {
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
    fn assign(&mut self, shard: Uuid, layout: &CoreLayout, pin_writers: bool) -> ShardSlot {
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
    fn activate(&mut self, shard: Uuid) {
        self.live.insert(shard);
    }

    fn ordinal(&self, shard: &Uuid) -> Option<usize> {
        self.slots.get(shard).map(|slot| slot.ordinal)
    }

    /// Whether this node serves the shard. The routing ring names a shard for a key; this
    /// answers whether that shard lives here, which is the whole of a local routing
    /// decision.
    fn is_local(&self, shard: &Uuid) -> bool {
        self.live.contains(shard)
    }

    /// Per-shard placement for `/_admin/workers`, ordered by ordinal so the report reads as
    /// the pool is laid out.
    fn report(&self) -> Vec<ShardPlacementStats> {
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
async fn orchestrator_worker_loop<F, Fut>(
    mut rx: mpsc::Receiver<OrchestratorJob>,
    run_op: F,
    worker_id: usize,
    counters: Option<Arc<WorkerCounters>>,
    max_in_flight: usize,
) where
    F: Fn(Box<ClientOp>, Option<Uuid>) -> Fut + Clone + Send + 'static,
    Fut: std::future::Future<Output = WorkerOutcome> + Send + 'static,
{
    let width = max_in_flight.max(1);
    let in_flight = Arc::new(tokio::sync::Semaphore::new(width));
    loop {
        // Never closed while the loop runs, so this only fails if the semaphore is dropped.
        let Ok(permit) = Arc::clone(&in_flight).acquire_owned().await else {
            break;
        };

        match rx.recv().await {
            Some(OrchestratorJob::Execute {
                op,
                affinity_shard,
                reply,
            }) => {
                let run_op = run_op.clone();
                let counters = counters.clone();
                if let Some(c) = &counters {
                    c.queue_depth.fetch_sub(1, AtomicOrdering::Relaxed);
                    c.in_flight.fetch_add(1, AtomicOrdering::Relaxed);
                }
                // On the pinned path this spawns onto the worker's own current_thread
                // runtime, so the operation stays on that core and pinning still means what
                // it says. On the default path it spawns onto the shared multi-threaded
                // runtime, where a worker is an admission-control unit rather than a place.
                tokio::spawn(async move {
                    let result = run_op(op, affinity_shard).await;
                    // Ignore the error: the caller may have given up and dropped the receiver.
                    let _ = reply.send(result);
                    if let Some(c) = &counters {
                        c.in_flight.fetch_sub(1, AtomicOrdering::Relaxed);
                        c.jobs_completed.fetch_add(1, AtomicOrdering::Relaxed);
                    }
                    drop(permit);
                });
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

/// Per-worker atomic counters — updated on the send and receive hot paths.
#[derive(Debug)]
struct WorkerCounters {
    /// Jobs sitting in this worker's mpsc channel, not yet started. Incremented on send,
    /// decremented when the worker picks the job up — so it is queueing against
    /// `queue_capacity`, and `in_flight` is the work actually running.
    queue_depth: AtomicUsize,
    /// Operations this worker has started and not yet answered, bounded by the worker's
    /// in-flight limit. The pair (`queue_depth`, `in_flight`) separates "waiting for a
    /// worker" from "waiting on a shard" — a deep queue beside a low `in_flight` means the
    /// limit is too tight, the reverse means the shards are the constraint.
    in_flight: AtomicUsize,
    /// Total jobs completed by this worker since startup.
    jobs_completed: AtomicU64,
    /// Core this worker is actually pinned to, or [`UNPINNED`]. Written once by the worker
    /// thread itself, because only it can find out whether the pin was accepted.
    pinned_core: AtomicI64,
}

impl Default for WorkerCounters {
    fn default() -> Self {
        Self {
            queue_depth: AtomicUsize::new(0),
            in_flight: AtomicUsize::new(0),
            jobs_completed: AtomicU64::new(0),
            pinned_core: AtomicI64::new(UNPINNED),
        }
    }
}

/// Dispatch-level counters across the entire worker pool.
#[derive(Debug, Default)]
struct DispatchCounters {
    /// Jobs sent directly to the affinity-assigned worker.
    affine_sends: AtomicU64,
    /// Jobs where the affinity-assigned worker was full and fell through to a neighbor.
    affine_full_fallbacks: AtomicU64,
    /// Jobs sent via round-robin (no affinity hint or affine dispatch disabled).
    round_robin_sends: AtomicU64,
    /// Jobs that fell all the way back to the actor mailbox (all workers full/closed).
    actor_mailbox_fallbacks: AtomicU64,
}

/// Snapshot of a single worker's stats for the `/_admin/workers` endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerStats {
    pub id: usize,
    /// Core this worker was asked to pin to.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_core_id: Option<usize>,
    /// Core it is actually pinned to. Absent when the pin was refused or never requested —
    /// the two are not the same thing, and only this one is evidence.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub core_id: Option<usize>,
    pub queue_depth: usize,
    pub queue_capacity: usize,
    /// Operations started and not yet answered. Sits against `in_flight_capacity`.
    #[serde(default)]
    pub in_flight: usize,
    #[serde(default)]
    pub in_flight_capacity: usize,
    pub jobs_completed: u64,
}

/// Where one shard sits in the pool, for the `/_admin/workers` endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardPlacementStats {
    pub shard_id: String,
    /// Dense ordinal. `ordinal % worker_count` is the worker that handles this shard's
    /// writes, which is what makes worker and writer land together.
    pub ordinal: usize,
    /// Whether the shard started and is taking work. False means it holds an ordinal but
    /// failed to hydrate.
    pub serving: bool,
    /// Core this shard's writer thread was asked to pin to.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_core_id: Option<usize>,
    /// Core the writer thread is actually pinned to.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub core_id: Option<usize>,
}

/// Snapshot of the dispatch counters for the `/_admin/workers` endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DispatchStats {
    pub affine_sends: u64,
    pub affine_full_fallbacks: u64,
    pub round_robin_sends: u64,
    pub actor_mailbox_fallbacks: u64,
}

/// Full worker pool report returned by `GET /_admin/workers`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerPoolReport {
    /// Config asked for pinned worker threads and the platform could enumerate cores.
    pub pinning_requested: bool,
    /// Workers whose pin actually took. Zero alongside `pinning_requested` means the
    /// platform refused every one — macOS, or a cpuset that excludes the target cores.
    /// This field, not `pinning_requested`, is the evidence that pinning is in effect.
    pub pinned_workers: usize,
    /// `worker_count` was aligned to the core budget so worker `i` and the writer for the
    /// shard with ordinal `i` share a core.
    pub core_aligned: bool,
    pub worker_count: usize,
    pub workers: Vec<WorkerStats>,
    /// Per-shard placement: ordinal, requested core, and the core actually taken.
    pub shards: Vec<ShardPlacementStats>,
    pub dispatch: DispatchStats,
}

/// Message to retrieve worker pool stats from the RouterActor.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct GetWorkerStats;

#[derive(Clone, Debug)]
pub struct OrchestratorWorkerTx {
    workers: Arc<Vec<mpsc::Sender<OrchestratorJob>>>,
    next_worker: Arc<AtomicUsize>,
    /// Per-worker atomic counters for observability.
    worker_stats: Arc<Vec<Arc<WorkerCounters>>>,
    /// Dispatch-level counters across all workers.
    dispatch_stats: Arc<DispatchCounters>,
    /// Per-worker channel capacity (same for all workers).
    per_worker_queue_capacity: usize,
    /// Whether pinned worker threads were requested and the platform could enumerate
    /// cores. Whether they took is per-thread, and lives in `WorkerCounters::pinned_core`.
    pinning_requested: bool,
    /// Whether worker_count was aligned to the core budget for writer co-location.
    core_aligned: bool,
    /// Core layout used for pinning and for reporting which core a worker sits on.
    core_layout: CoreLayout,
    /// Shard ordinals — the map from a shard to the worker that owns its writes.
    placement: Arc<ArcSwap<ShardPlacement>>,
}

impl OrchestratorWorkerTx {
    fn new_with_stats(
        workers: Vec<mpsc::Sender<OrchestratorJob>>,
        worker_stats: Arc<Vec<Arc<WorkerCounters>>>,
        per_worker_queue_capacity: usize,
        pinning_requested: bool,
        core_aligned: bool,
        core_layout: CoreLayout,
        placement: Arc<ArcSwap<ShardPlacement>>,
    ) -> Self {
        Self {
            workers: Arc::new(workers),
            next_worker: Arc::new(AtomicUsize::new(0)),
            worker_stats,
            dispatch_stats: Arc::new(DispatchCounters::default()),
            per_worker_queue_capacity,
            pinning_requested,
            core_aligned,
            core_layout,
            placement,
        }
    }

    fn len(&self) -> usize {
        self.workers.len()
    }

    fn try_send(
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
    fn try_send_affine(
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

    async fn send_shutdown(&self) {
        for worker in self.workers.iter() {
            if worker.send(OrchestratorJob::Shutdown).await.is_err() {
                break;
            }
        }
    }

    /// Produce a snapshot of all worker and dispatch counters for `/_admin/workers`.
    pub fn snapshot(&self) -> WorkerPoolReport {
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
pub struct OrchestratorEngine {
    /// Shard map — updated atomically on topology changes via ArcSwap.
    pub shards: ArcSwap<HashMap<Uuid, MicroshardActor>>,
    /// Consistent hash ring — single shared instance across the engine and
    /// the `RouterActor` (shard-affine dispatch). Updated atomically via
    /// `ArcSwap::store` on topology changes; readers always see the latest snapshot.
    pub routing_ring: Arc<ArcSwap<ConsistentRing>>,
    /// Per-index schema cache (lock-free via ArcSwap).
    ///
    /// Keyed by index name, which is the only thing that identifies an index. A reverse
    /// lookup keyed by a hash of the field names used to sit in front of this and answered
    /// with whichever index of that shape was cached last — see `IndexSchema::calculate_fingerprint`.
    pub schema_cache: Arc<ArcSwap<HashMap<String, Arc<IndexSchema>>>>,
    /// Coordinator actor reference for shard assignments and peer lookups.
    #[allow(dead_code)] // Used when bulk write is moved to engine
    pub coordinator: Option<ActorRef<ClusterCoordinator>>,
    /// Node identity for response metadata.
    #[allow(dead_code)] // Used when bulk write is moved to engine
    pub identity: NodeIdentity,
    /// Default search result limit.
    pub default_search_limit: usize,
    pub max_concurrent_shard_searches: usize,
    /// Shared pool of cached RemoteActorRef handles for avoiding repeated lookups.
    #[allow(dead_code)] // Used when bulk write is moved to engine
    pub remote_peer_pool: Arc<RemotePeerPool>,
}

impl std::fmt::Debug for OrchestratorEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OrchestratorEngine")
            .field("default_search_limit", &self.default_search_limit)
            .finish_non_exhaustive()
    }
}

impl OrchestratorEngine {
    /// Fetch a schema from cache if present (lock-free).
    fn get_cached_schema(&self, index: &str) -> Option<Arc<IndexSchema>> {
        let map = self.schema_cache.load();
        map.get(index).cloned()
    }

    /// Insert or replace a schema in the cache (copy-on-write).
    fn put_cached_schema(&self, index: &str, schema: &IndexSchema) {
        let schema_arc = Arc::new(schema.clone());
        let index_str = index.to_string();

        self.schema_cache.rcu(|old| {
            let mut new = (**old).clone();
            new.insert(index_str.clone(), schema_arc.clone());
            new
        });
    }

    /// Load schema from first shard's storage.
    async fn load_schema(&self, index: &str) -> Result<IndexSchema, OrchestratorError> {
        if let Some(cached) = self.get_cached_schema(index) {
            return Ok((*cached).clone());
        }

        let shards = self.shards.load();
        if let Some(shard) = shards.values().next()
            && let Some(store) = &shard.store
        {
            let sc = Arc::clone(store);
            let idx = index.to_string();
            let schema = tokio::task::spawn_blocking(move || sc.get_schema_cached(&idx))
                .await
                .map_err(|e| OrchestratorError::Io(std::io::Error::other(e.to_string())))?
                .map_err(|e| OrchestratorError::Io(std::io::Error::other(e.to_string())))?;
            if let Some(schema_arc) = schema {
                let schema = (*schema_arc).clone();
                self.put_cached_schema(index, &schema);
                return Ok(schema);
            }
        }
        Ok(IndexSchema::default())
    }

    /// Route write to shard using deterministic key (no round-robin).
    fn route_write(&self, routing_key: &Option<String>) -> Result<Uuid, OrchestratorError> {
        let key = routing_key.as_ref().ok_or_else(|| {
            OrchestratorError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Missing routing key for write",
            ))
        })?;

        let ring = self.routing_ring.load();
        let target = ring.get_owner(key).or_else(|| self.first_shard_id());

        target.ok_or_else(|| {
            OrchestratorError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "No shard selected",
            ))
        })
    }

    /// Returns the first shard id if any exist (fallback for empty ring).
    fn first_shard_id(&self) -> Option<Uuid> {
        let shards = self.shards.load();
        shards.keys().copied().next()
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
    pub async fn execute(&self, op: ClientOp) -> WorkerOutcome {
        match op {
            ClientOp::Write {
                index,
                id,
                routing_key,
                doc,
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
            } => WorkerOutcome::Done(self.engine_delete(&index, id, routing_key).await),
            // Bulk writes need `staged_schema_validation`, parallel routing and remote
            // forwarding; config and metadata ops are lightweight and rare. Both belong on
            // the actor, which owns the state they touch.
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
    async fn engine_write(
        &self,
        index: &str,
        id: String,
        routing_key: Option<String>,
        doc: JsonValue,
    ) -> Result<WriteOutcome, OrchestratorError> {
        let shards = self.shards.load();
        if shards.is_empty() {
            return Err(OrchestratorError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "No shards",
            )));
        }

        // Lock-free schema lookup, by the one thing that identifies an index: its name.
        let schema = if let Some(cached) = self.get_cached_schema(index) {
            cached
        } else {
            Arc::new(self.load_schema(index).await?)
        };

        // Fast path: mature schema — validate inline
        if !schema.fields.is_empty() {
            let result = NodeOrchestrator::validate_document(&id, &doc, &schema);
            if let Some(err) = result.validation_error {
                return Err(OrchestratorError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    err,
                )));
            }

            if !result.needs_evolution {
                // Schema is stable — populate cache if not yet present
                if self.get_cached_schema(index).is_none() {
                    self.put_cached_schema(index, &schema);
                }

                // Schema-based routing
                let effective_routing_key = effective_routing_key(&schema, &id, routing_key, &doc);

                // The ring decides the shard, always — never the dispatch hint.
                //
                // The hint is `owner(routing_key.or(id))`, computed by the router before the
                // schema was in hand, while the key above prefers the document's own routing
                // field. On an index whose routing field is a real, non-key field those two
                // disagree, and taking the hint used to put the document on a shard the ring
                // does not believe owns it. Nothing looked wrong — searches are scatter-gather
                // and found it anyway — until the same id was written again through a path with
                // no hint (the actor-mailbox fallback when a worker queue is full), which routed
                // by the ring, landed elsewhere, and left the id on two shards at once.
                //
                // What the hint is for is choosing the worker, and it still does that in
                // `try_send_affine`: same dense shard ordinal, same co-located writer thread,
                // same saved cross-core wakeup. This lookup is an xxh3 and a `BTreeMap` range
                // descent, which is not a saving worth a class of divergence in front of a redb
                // transaction.
                let target = self.route_write(&effective_routing_key)?;
                let shard = shards.get(&target).ok_or_else(|| {
                    OrchestratorError::Io(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        "Shard not found",
                    ))
                })?;
                let req = WriteRequest {
                    index: index.to_string(),
                    id: id.clone(),
                    routing_key: effective_routing_key.unwrap_or_default(),
                    doc,
                };

                return match shard.handle_write(req).await {
                    Ok(seq) => Ok(WriteOutcome::Done(serde_json::json!({
                        "id": id, "result": "created", "version": seq,
                        "shard_id": target.to_string()
                    }))),
                    Err(e) => Err(e),
                };
            }
            // needs_evolution == true: fall through and hand the op back.
        }

        // The schema is empty (a new index) or the document carries fields it does not
        // describe. Either way this write has to grow the schema, which only the actor can
        // do — give the caller back everything it needs to retry there.
        Ok(WriteOutcome::NeedsActor {
            id,
            routing_key,
            doc,
        })
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
    async fn engine_delete(
        &self,
        index: &str,
        id: String,
        routing_key: Option<String>,
    ) -> Result<JsonValue, OrchestratorError> {
        let shards = self.shards.load();
        if shards.is_empty() {
            return Err(OrchestratorError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "No shards",
            )));
        }

        let schema = if let Some(cached) = self.get_cached_schema(index) {
            cached
        } else {
            Arc::new(self.load_schema(index).await?)
        };

        let routing_key = effective_delete_routing_key(&schema, &id, routing_key)?;

        // The ring owns the placement decision here as it does for a write, and a delete cannot
        // disagree with the hint the router dispatched on: both are this same key.
        let target = self.route_write(&Some(routing_key.clone()))?;
        let shard = shards.get(&target).ok_or_else(|| {
            OrchestratorError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Shard not found",
            ))
        })?;

        let sequence = shard.handle_delete(index.to_string(), id.clone()).await?;
        Ok(serde_json::json!({
            "id": id,
            "result": "deleted",
            "version": sequence,
            "shard_id": target.to_string(),
        }))
    }

    /// Parallel scatter-gather search across all local shards.
    async fn engine_search(
        &self,
        index: &str,
        query: &str,
        window: SearchWindow,
        fields: Option<&[String]>,
        sort: Option<&SortSpec>,
    ) -> Result<JsonValue, OrchestratorError> {
        let shards = self.shards.load();
        let start = std::time::Instant::now();
        if shards.is_empty() {
            return Ok(
                serde_json::json!({"hits": [], "hits_returned": 0, "total_hits": 0, "took_ms": 0}),
            );
        }

        // Get schema for shadow field transformation (lock-free)
        let schema = self
            .get_cached_schema(index)
            .map(|arc| (*arc).clone())
            .unwrap_or_default();

        // The identifier travels under the shadow name on the way out, so the projection is
        // rewritten before it is checked or applied.
        let fields = fields.map(|list| normalize_projection_fields(&schema, list));
        let fields = fields.as_deref();

        // Refuse a sort the index cannot answer before asking any shard: every shard would
        // fail the same way, and a scatter-gather reports that as a partial failure inside a
        // 200 rather than as the bad request it is.
        if let Some(refusal) = unsortable_sort_field(&schema, sort) {
            return Err(refusal);
        }

        let shard_targets: Vec<(Uuid, MicroshardActor)> = shards
            .iter()
            .map(|(&shard_id, shard)| (shard_id, shard.clone()))
            .collect();
        let shard_results: Vec<_> =
            futures::stream::iter(shard_targets.into_iter().map(|(shard_id, shard)| {
                // Every shard is asked for the whole window from the front, because any of
                // them may hold all of it. The skip is applied once, below.
                let req = SearchRequest {
                    index: index.to_string(),
                    query: query.to_string(),
                    limit: Some(window.fetch_count()),
                    sort: sort.cloned(),
                };
                async move { (shard_id, shard.handle_search(req).await) }
            }))
            .buffer_unordered(self.max_concurrent_shard_searches.max(1))
            .collect()
            .await;

        let mut results: Vec<(Uuid, f32, JsonValue)> = Vec::new();
        // Kept as values rather than formatted here: whether nothing ran at all, and whose fault
        // that was, is decided from the errors themselves once the gather is complete.
        let mut failures: Vec<(Uuid, OrchestratorError)> = Vec::new();
        let mut shard_success = 0usize;
        let mut total_hits_sum = 0usize;
        // Every shard parses the same query string, so collect the distinct set.
        let mut discarded: Vec<String> = Vec::new();
        // Every shard runs the same sort against the same schema, so one shard reporting an
        // approximate order describes the whole answer. A shard with no built index reports
        // nothing, hence first-wins rather than agreement.
        let mut approximate_sort: Option<String> = None;
        // One shard is enough. Shards can hold different schemas for the same index, and a
        // query that one of them could not run at all is not answered by the ones that could.
        let mut emptied = false;
        for (shard_id, result) in shard_results {
            match result {
                Ok(r) => {
                    emptied |= r.emptied;
                    total_hits_sum += r.total_hits;
                    for hit in r.hits {
                        results.push((shard_id, hit.score, hit.doc));
                    }
                    for note in r.discarded {
                        if !discarded.contains(&note) {
                            discarded.push(note);
                        }
                    }
                    approximate_sort = approximate_sort.or(r.approximate_sort);
                    shard_success += 1;
                }
                Err(err) => {
                    warn!(%shard_id, error = %err, "Engine scatter search shard failed");
                    failures.push((shard_id, err));
                }
            }
        }

        // Nothing ran, so there is nothing to qualify: refuse rather than answer with the empty
        // page a partial outage would produce. This is where the one refusal the sort guard above
        // cannot make lands — `fast` is a declaration and the column is written from it when the
        // index is built, so a field declared fast after the fact has no column to order by, and
        // only the built index knows that.
        if let Some(refusal) = no_shard_answered(index, shard_success, &failures) {
            return Err(refusal);
        }
        let errors = shard_error_notes(&failures);

        // Order merged results: by the requested sort field when provided, otherwise by
        // score descending. Each shard already returned field-sorted results, so a global
        // re-sort here is required to interleave them correctly across shards.
        //
        // When sorting, stamp each hit with the normalized `SORT_KEY_FIELD` first (while
        // the full doc is still present) and key the sort on it. The metadata field
        // survives the field projection below and lets a downstream cross-node merge
        // re-order these results even if the sort field is not among the returned fields.
        if let Some(spec) = sort {
            stamp_sort_keys(&mut results, spec, &schema);
        }
        order_shard_hits(&mut results, sort);
        let results = window.apply(results);
        let total_shards = shards.len();
        let hits: Vec<JsonValue> = results
            .into_iter()
            .map(|(_shard_id, score, mut doc)| {
                // Add metadata fields
                if let JsonValue::Object(ref mut o) = doc {
                    o.insert(
                        "_score".to_string(),
                        serde_json::Number::from_f64(score as f64)
                            .map(JsonValue::Number)
                            .unwrap_or(JsonValue::Null),
                    );
                }

                // Apply field projection if specified
                if let Some(field_list) = fields {
                    apply_field_projection(doc, field_list)
                } else {
                    doc
                }
            })
            .collect();
        let mut response = serde_json::json!({
            "hits": hits,
            "hits_returned": hits.len(),
            "total_hits": total_hits_sum,
            "limit": window.limit,
            "offset": window.offset,
            "took_ms": start.elapsed().as_millis(),
            "stats": {
                "shards": {
                    "total": total_shards,
                    "responded": shard_success,
                    "failed": errors.len()
                }
            },
        });
        attach_shard_errors(&mut response, errors);
        // Refuse instead of answering. An emptied query ran as nothing, so the zero it
        // produces is not a negative result — reported as a 200 it cannot be told apart from
        // "no document matches", which is the same confusion an unrunnable sort caused.
        if emptied {
            return Err(OrchestratorError::UnrunnableQuery {
                notes: discarded.join("; "),
            });
        }

        discarded.extend(unknown_projection_fields(&schema, fields));
        attach_discarded(&mut response, discarded);
        attach_approximate_sort(&mut response, approximate_sort);
        Ok(response)
    }
}

/// Helper struct for aggregating index statistics across cluster nodes.
#[derive(Debug, Clone)]
struct IndexStats {
    name: String,
    description: Option<String>,
    document_count: u64,
    index_size_bytes: u64,
    memory_bytes: u64,
    data_size_bytes: u64,
    total_size_bytes: u64,
    shard_count: usize,
    warm_shards: usize,
    /// Field descriptions merged by name across nodes.
    ///
    /// A field is `searchable` in the cluster when any node can search it, for the same reason
    /// the per-node union is a union: a scatter-gather asks every node, so one node holding the
    /// column is enough to answer.
    fields: BTreeMap<String, JsonValue>,
}

/// Microshard actor that manages a single shard's storage and search operations.
#[derive(Clone, Actor, RemoteActor)]
pub struct MicroshardActor {
    shard_id: Uuid,
    store: Option<Arc<HybridStore>>,
    /// Channel sender for dispatching write commands to the writer thread.
    writer_tx: Option<mpsc::Sender<StorageCommand>>,
    /// Writer thread handle for forceful termination on shutdown timeout.
    writer_thread_handle: Arc<std::sync::Mutex<Option<std::thread::JoinHandle<()>>>>,
    storage_config: StorageConfig,
    default_search_limit: usize,
    /// Active supervision tasks per index (idle-timeout commits).
    supervisors: Arc<AsyncRwLock<HashMap<String, mpsc::Sender<()>>>>,
    /// Notified when writer thread has stopped.
    shutdown_notify: Arc<tokio::sync::Notify>,
    /// Read thread pool handle for isolated search/stats operations.
    read_pool_handle: Option<tokio::runtime::Handle>,
    /// Total shards on this node (for per-shard memory budgeting).
    total_shards: usize,
    /// Writer thread shutdown timeout in seconds.
    writer_shutdown_timeout_secs: u64,
    /// Seconds of write inactivity before this shard's supervisor commits an index.
    supervisor_timeout_secs: u64,
    /// Where this shard's writer thread should pin, resolved from the shard's ordinal by the
    /// orchestrator, plus the cell the thread reports its actual core through. Resolving the
    /// target upstream is what keeps a writer on the same core as the worker that feeds it:
    /// both come from one ordinal and one layout.
    writer_pin: WriterPin,
}

impl std::fmt::Debug for MicroshardActor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MicroshardActor")
            .field("shard_id", &self.shard_id)
            .field("store_initialized", &self.store.is_some())
            .field("writer_initialized", &self.writer_tx.is_some())
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
pub struct ShardRuntime {
    /// Hits returned when a query names no limit.
    pub default_search_limit: usize,
    /// The shared read pool. `None` falls back to tokio's generic blocking pool.
    pub read_pool_handle: Option<tokio::runtime::Handle>,
    /// Shards on this node, for per-shard memory budgeting.
    pub total_shards: usize,
    /// How long to let the writer thread drain on shutdown.
    pub writer_shutdown_timeout_secs: u64,
    /// Write inactivity before an index is committed anyway.
    pub supervisor_timeout_secs: u64,
    /// Where the writer thread pins, and where it reports what happened.
    pub writer_pin: WriterPin,
}

impl MicroshardActor {
    pub fn new(shard_id: Uuid, storage_config: StorageConfig, runtime: ShardRuntime) -> Self {
        let ShardRuntime {
            default_search_limit,
            read_pool_handle,
            total_shards,
            writer_shutdown_timeout_secs,
            supervisor_timeout_secs,
            writer_pin,
        } = runtime;

        Self {
            shard_id,
            store: None,
            writer_tx: None,
            writer_thread_handle: Arc::new(std::sync::Mutex::new(None)),
            storage_config,
            default_search_limit,
            supervisors: Arc::new(AsyncRwLock::new(HashMap::new())),
            shutdown_notify: Arc::new(tokio::sync::Notify::new()),
            read_pool_handle,
            total_shards,
            writer_shutdown_timeout_secs,
            supervisor_timeout_secs,
            writer_pin,
        }
    }

    pub async fn start(&mut self) -> Result<(), OrchestratorError> {
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
        // committed answers searches without its most recent writes, so it must be replayed.
        // Only indices whose persisted checkpoint falls short of their WAL are touched.
        //
        // Phase 2 (warmup) is purely latency: it opens and caches the *reader* for each
        // index and faults in its segment structures, so the first query from an agent or
        // client does not pay for opening the index. It runs on its own thread and never
        // gates serving — a request arriving first just warms that index on demand.
        //
        // Neither phase blocks `start()`. Requests are served throughout via lazy
        // initialization; the phases only determine whether that work has already been done.
        //
        // After startup the same thread keeps serving re-warm requests: the writer thread
        // posts an index name here after each commit, so the segment a commit just published
        // gets warmed without doing that work on the write hot path. The channel is bounded
        // and posted to with `try_send`, making re-warming strictly best-effort — a full
        // channel drops the request rather than ever stalling a write.
        let (warm_tx, warm_rx) = std::sync::mpsc::sync_channel::<String>(WARM_REQUEST_CAPACITY);
        let warmup_store = Arc::clone(&store_arc);
        let shard_id = self.shard_id;
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

            info!(
                shard_id = %shard_id,
                recovered = plan.recovered.len(),
                failed = plan.failed.len(),
                pending_warmup = plan.pending_warmup.len(),
                "Phase 1 complete - shard is queryable"
            );

            // One warmup thread per shard, for startup warmup and then for post-commit
            // re-warms. With N shards that is already N-way parallelism against the same
            // disk, so warming a shard's indices sequentially keeps the IO pattern sane
            // instead of turning startup into a seek storm.
            let spawned = std::thread::Builder::new()
                .name(format!("warmup-shard-{shard_id}"))
                .spawn(move || {
                    if !plan.pending_warmup.is_empty() {
                        let requested = plan.pending_warmup.len();
                        let warmed = warmup_store.warm_indices(&plan.pending_warmup);
                        info!(
                            shard_id = %shard_id,
                            warmed = warmed,
                            requested = requested,
                            "Phase 2 complete - index readers warmed"
                        );
                    }

                    // Serve re-warm requests until the writer thread drops its sender, which
                    // happens when the shard shuts down. `warm_index` skips a searcher
                    // generation it has already warmed, so bursts of commits on one index
                    // collapse into a single warm.
                    while let Ok(index) = warm_rx.recv() {
                        if let Err(e) = warmup_store.warm_index(&index) {
                            debug!(
                                shard_id = %shard_id,
                                index = %index,
                                error = %e,
                                "Post-commit warm failed; queries will warm this index on demand"
                            );
                        }
                    }

                    debug!(shard_id = %shard_id, "Warmup thread stopped");
                });

            if let Err(e) = spawned {
                // Not fatal: every index still warms itself on its first query.
                warn!(
                    shard_id = %shard_id,
                    error = %e,
                    "Could not spawn warmup thread; indices will warm on first query"
                );
            }
        });

        // Spawn dedicated writer thread for serialized I/O
        let (tx, mut rx) = mpsc::channel::<StorageCommand>(SHARD_WRITER_CHANNEL_CAPACITY);
        self.writer_tx = Some(tx);

        let writer_store = store_arc;
        let shutdown = self.shutdown_notify.clone();
        let writer_shard_id = self.shard_id;
        let writer_pin = self.writer_pin.clone();

        let handle = std::thread::Builder::new()
            .name(format!("writer-shard-{}", writer_shard_id))
            .spawn(move || {
                // Pin to the core the orchestrator picked from this shard's ordinal — the
                // same ordinal that chooses the worker feeding this thread, so the two land
                // together. Improves cache locality for the redb and tantivy structures this
                // thread owns, and removes a cross-core wakeup per write. Reports back
                // whether it took, so `/_admin/workers` can show the outcome.
                writer_pin.apply(writer_shard_id);

                info!(shard_id = %writer_shard_id, "Writer thread started (write coalescing enabled)");

                // Reusable buffers to avoid per-iteration allocations
                let mut pending_cmds: Vec<StorageCommand> = Vec::with_capacity(256);

                while let Some(first_cmd) = rx.blocking_recv() {
                    // Phase 1: Drain all pending commands from the channel.
                    // The first command blocks until available; subsequent commands
                    // are non-blocking to coalesce as many writes as possible.
                    // Limit drain to prevent starvation - max 256 additional commands per iteration.
                    const MAX_DRAIN_PER_ITERATION: usize = 256;
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
                    let mut write_groups: HashMap<String, Vec<WriteCommand>> = HashMap::new();
                    let mut batch_groups: HashMap<String, Vec<BatchCommand>> = HashMap::new();
                    let mut commits: Vec<(String, tokio::sync::oneshot::Sender<Result<(), StoreError>>)> = Vec::new();
                    let mut evictions: Vec<(String, tokio::sync::oneshot::Sender<bool>)> = Vec::new();
                    let mut deletions: Vec<DeleteCommand> = Vec::new();
                    let mut should_shutdown = false;
                    // Indices whose Tantivy commit published a new segment this iteration.
                    // Collected rather than posted inline so a burst that commits the same
                    // index several times results in one re-warm request.
                    let mut committed_indices: HashSet<String> = HashSet::new();

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
                            StorageCommand::Shutdown => {
                                should_shutdown = true;
                            }
                        }
                    }

                    // Phase 3: Process coalesced single writes (biggest optimization).
                    // Multiple single writes to the same index become one apply_batch call
                    // with a single redb transaction instead of N separate transactions.
                    for (index, writes) in &mut write_groups {
                        if writes.len() == 1 {
                            // Single write — no coalescing overhead needed
                            let (op, reply) = writes.pop().unwrap();
                            let res = writer_store.apply_write_and_maybe_commit(index, op);
                            match &res {
                                Ok((_, true)) => {
                                    tracing::info!(index = %index, "Writer: threshold commit after write");
                                    committed_indices.insert(index.clone());
                                }
                                Ok((_, false)) => {}
                                Err(e) => tracing::error!(index = %index, error = %e, "Writer: write failed"),
                            }
                            let _ = reply.send(res.map(|(seq_id, _)| seq_id));
                        } else {
                            // Coalesced writes — merge N single writes into one batch
                            let coalesced_count = writes.len();
                            let (ops, replies): (Vec<WalOp>, Vec<_>) = writes.drain(..).unzip();

                            let res = writer_store.apply_batch_and_maybe_commit(index, ops);
                            match res {
                                Ok(((seq_ids, _new_docs), committed)) => {
                                    if committed {
                                        tracing::info!(
                                            index = %index,
                                            coalesced = coalesced_count,
                                            "Writer: threshold commit after coalesced writes"
                                        );
                                        committed_indices.insert(index.clone());
                                    }
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
                                        let _ = reply.send(Err(StoreError::Serialization(err_msg.clone())));
                                    }
                                }
                            }
                        }
                    }

                    // Phase 4: Process coalesced batch writes.
                    // Multiple BatchWrite commands for the same index are merged into
                    // a single apply_batch call, then results are split back to callers.
                    for (index, batches) in batch_groups {
                        if batches.len() == 1 {
                            // Single batch — no coalescing overhead needed
                            let (ops, reply) = batches.into_iter().next().unwrap();
                            let res = writer_store.apply_batch_and_maybe_commit(&index, ops);
                            match &res {
                                Ok((_, true)) => {
                                    tracing::info!(index = %index, "Writer: threshold commit after batch write");
                                    committed_indices.insert(index.clone());
                                }
                                Ok((_, false)) => {}
                                Err(e) => tracing::error!(index = %index, error = %e, "Writer: batch write failed"),
                            }
                            let _ = reply.send(res.map(|(result, _)| result));
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
                            let res = writer_store.apply_batch_and_maybe_commit(&index, merged_ops);
                            match res {
                                Ok(((seq_ids, new_docs), committed)) => {
                                    if committed {
                                        tracing::info!(
                                            index = %index,
                                            coalesced_batches = coalesced_count,
                                            total_ops = total_ops,
                                            "Writer: threshold commit after coalesced batch writes"
                                        );
                                        committed_indices.insert(index.clone());
                                    }
                                    tracing::debug!(
                                        index = %index,
                                        coalesced_batches = coalesced_count,
                                        total_ops = total_ops,
                                        "Writer: coalesced {} batch writes ({} ops) into one transaction",
                                        coalesced_count, total_ops
                                    );

                                    // Split merged seq_ids back to each caller by their original op count
                                    let mut offset = 0usize;
                                    let mut remaining_new_docs = new_docs;
                                    let total_segments = reply_segments.len();
                                    for (idx, (op_count, reply)) in reply_segments.into_iter().enumerate() {
                                        let segment: Vec<u64> = seq_ids[offset..offset + op_count].to_vec();
                                        // Use integer arithmetic with remainder distribution to avoid rounding errors
                                        // Each segment gets: (new_docs * op_count) / total_ops
                                        // Last segment gets any remainder to ensure exact sum
                                        let segment_new_docs = if idx == total_segments - 1 {
                                            // Last segment gets all remaining to ensure exact total
                                            remaining_new_docs
                                        } else {
                                            // Integer division with proper distribution
                                            let proportional = new_docs
                                                .checked_mul(op_count)
                                                .and_then(|product| product.checked_div(total_ops))
                                                .unwrap_or(0);
                                            proportional.min(remaining_new_docs)
                                        };
                                        remaining_new_docs = remaining_new_docs.saturating_sub(segment_new_docs);
                                        let _ = reply.send(Ok((segment, segment_new_docs)));
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
                                        let _ = reply.send(Err(StoreError::Serialization(err_msg.clone())));
                                    }
                                }
                            }
                        }
                    }

                    // Phase 5: Process commits after all writes are applied
                    for (index, reply) in commits {
                        let res = writer_store.commit_index(&index);
                        if res.is_ok() {
                            committed_indices.insert(index.clone());
                        }
                        let _ = reply.send(res);
                    }

                    // Phase 5b: Process writer evictions (commit then drop from cache)
                    for (index, reply) in evictions {
                        if let Err(e) = writer_store.commit_index(&index) {
                            tracing::warn!(index = %index, error = %e, "Evict: commit failed, evicting anyway");
                        }
                        let removed = writer_store.force_remove_writer(&index);
                        tracing::info!(index = %index, removed, "Writer evicted from cache");
                        let _ = reply.send(removed);
                    }

                    // Phase 5c: Process index deletions last, so any writes that were
                    // batched alongside the delete are applied before their tables go away.
                    for (index, delete_schema, reply) in deletions {
                        let res = writer_store.delete_index_data(&index, delete_schema);
                        match &res {
                            Ok(()) => info!(index = %index, delete_schema, "Index data deleted"),
                            Err(e) => warn!(index = %index, error = %e, "Index deletion failed"),
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

                    // Phase 6: Handle shutdown after draining all pending work
                    if should_shutdown {
                        info!(shard_id = %writer_shard_id, "Writer thread shutting down");
                        break;
                    }
                }
                shutdown.notify_one();
                info!(shard_id = %writer_shard_id, "Writer thread stopped");
            })
            .map_err(OrchestratorError::Io)?;

        // Store the thread handle for forceful termination if needed during shutdown
        *self.writer_thread_handle.lock().unwrap() = Some(handle);

        info!(shard_id = %self.shard_id, "MicroshardActor initialized with dedicated writer thread");
        Ok(())
    }

    /// Send a command to the dedicated writer thread.
    async fn send_write_command(&self, cmd: StorageCommand) -> Result<(), OrchestratorError> {
        self.writer_tx
            .as_ref()
            .ok_or_else(|| {
                OrchestratorError::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "Writer channel not initialized",
                ))
            })?
            .send(cmd)
            .await
            .map_err(|_| OrchestratorError::Io(std::io::Error::other("Writer thread closed")))
    }

    /// Write a single document via the dedicated writer thread.
    pub async fn handle_write_via_channel(
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
    pub async fn handle_batch_write_via_channel(
        &self,
        index: String,
        ops: Vec<WalOp>,
    ) -> Result<(Vec<u64>, usize), OrchestratorError> {
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
            seq_count = result.0.len(),
            "MicroshardActor: Batch write completed successfully"
        );
        Ok(result)
    }

    pub async fn admin_commit_via_channel(&self, index: String) -> Result<(), OrchestratorError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.send_write_command(StorageCommand::Commit { index, reply: tx })
            .await?;
        rx.await
            .map_err(|_| OrchestratorError::Io(std::io::Error::other("Writer dropped reply")))?
            .map_err(OrchestratorError::Storage)
    }

    pub async fn admin_evict_writer_via_channel(
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
    async fn shutdown_writer(&mut self) {
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

        // Send shutdown signal and wait for thread exit
        if let Some(tx) = self.writer_tx.take() {
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
                        // Join thread cleanly
                        if let Some(handle) = self.writer_thread_handle.lock().unwrap().take()
                            && let Err(e) = handle.join()
                        {
                            tracing::warn!(shard_id = %self.shard_id, error = ?e, "Writer thread panicked");
                        }
                    }
                    Err(_) => {
                        tracing::error!(
                            shard_id = %self.shard_id,
                            timeout_secs = timeout_secs,
                            "Writer thread shutdown timed out after {}s - abandoning",
                            timeout_secs
                        );
                        // Abandon thread - OS will clean up on process exit
                        *self.writer_thread_handle.lock().unwrap() = None;
                    }
                }
            } else {
                tracing::warn!(shard_id = %self.shard_id, "Writer thread already closed");
            }
        }
    }

    /// Dispatch a blocking closure to the dedicated read pool if available,
    /// falling back to tokio's generic blocking pool otherwise.
    async fn spawn_on_read_pool<F, R>(&self, f: F) -> Result<R, OrchestratorError>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        if let Some(handle) = &self.read_pool_handle {
            handle
                .spawn_blocking(f)
                .await
                .map_err(|e| OrchestratorError::Io(std::io::Error::other(e)))
        } else {
            tokio::task::spawn_blocking(f)
                .await
                .map_err(|e| OrchestratorError::Io(std::io::Error::other(e)))
        }
    }

    /// Handles search requests on the dedicated read thread pool.
    pub async fn handle_search(
        &self,
        request: SearchRequest,
    ) -> Result<SearchReply, OrchestratorError> {
        let store = self.store.as_ref().ok_or_else(|| {
            OrchestratorError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "HybridStore not initialized",
            ))
        })?;

        let store = Arc::clone(store);
        let query = request.query;
        let limit = request.limit.unwrap_or(self.default_search_limit);
        let index = request.index.clone();
        let sort = request.sort;

        let outcome = self
            .spawn_on_read_pool(move || {
                store.search_documents(&index, &query, limit, sort.as_ref())
            })
            .await?
            .map_err(|e: StoreError| match e {
                StoreError::Io(io_err) => OrchestratorError::Io(io_err),
                // A field this index does not carry, or a query it cannot parse, is the
                // request's fault rather than the node's — and the distinction has to survive
                // the crossing, because an error leaves this actor as a string and everything
                // downstream reads its `ErrorKind` to decide who is at fault. Flattened to
                // `other`, a sort naming a column the index never built read as an internal
                // failure, which is how it came back as a partial outage inside a `200`.
                StoreError::FieldNotFound(_) | StoreError::QueryParser(_) => OrchestratorError::Io(
                    std::io::Error::new(std::io::ErrorKind::InvalidInput, e.to_string()),
                ),
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
            emptied: outcome.emptied,
        })
    }

    /// Handles shard statistics requests on the dedicated read thread pool.
    pub async fn handle_get_stats(
        &self,
        msg: GetShardStats,
    ) -> Result<storage::ShardStatsSnapshot, OrchestratorError> {
        let store = self.store.as_ref().ok_or_else(|| {
            OrchestratorError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "HybridStore not initialized",
            ))
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
    async fn signal_supervisor(&self, index: String) {
        let writer_tx = match self.writer_tx.as_ref() {
            Some(tx) => tx.clone(),
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
                                    tracing::info!(index = %index_inner, "Supervisor committed index via writer thread after idle timeout");
                                    // Self-cleanup from the supervisors map
                                    let mut supervisors = supervisors_arc.write().await;
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
    pub async fn handle_write(&self, request: WriteRequest) -> Result<u64, OrchestratorError> {
        // OPTIMIZATION: Take ownership of doc from request immediately
        let doc = request.doc;

        let id = if !request.id.is_empty() {
            request.id
        } else {
            doc.get("id")
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    OrchestratorError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Document must be written with a non-empty 'id' beside its body",
                    ))
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
    /// threshold. Ordering inside that batch is already correct — Tantivy applies `delete_term` to
    /// documents added earlier in the same commit, and the redb `insert` then `remove` leaves
    /// nothing behind — so a put and a delete of one id in the same batch resolve whichever order
    /// they arrived in.
    ///
    /// The supervisor signal is what bounds visibility. A delete removes the redb row at once, so
    /// an `id:VALUE` lookup stops finding it immediately, but the Tantivy `delete_term` only takes
    /// effect at the next commit — without this signal a single delete on an otherwise idle index
    /// would wait for unrelated traffic to trigger one.
    pub async fn handle_delete(&self, index: String, id: String) -> Result<u64, OrchestratorError> {
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
    pub async fn handle_batch_delete(
        &self,
        index: String,
        ids: Vec<String>,
    ) -> Result<usize, OrchestratorError> {
        if ids.is_empty() {
            return Ok(0);
        }

        let ops: Vec<WalOp> = ids.into_iter().map(|id| WalOp::Delete { id }).collect();
        let expected = ops.len();

        let (sequences, _new_docs) = self
            .handle_batch_write_via_channel(index.clone(), ops)
            .await?;

        self.signal_supervisor(index).await;

        // One sequence per op, so the count is the storage layer's own answer rather than an
        // echo of what was asked for.
        debug_assert_eq!(sequences.len(), expected);
        Ok(sequences.len())
    }

    /// Handles batch write requests via the dedicated writer thread.
    pub async fn handle_batch_write(
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

            let (seq_ids, _new_docs) = self
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

        tracing::info!(
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
    pub async fn delete_index(
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

/// Router actor that forwards client operations to NodeOrchestrator via actor messaging.
/// Uses actor messaging instead of Arc<RwLock> - no locks needed.
#[cfg_attr(not(test), allow(dead_code))]
#[derive(Clone, Actor)]
pub struct RouterActor {
    orchestrator: ActorRef<NodeOrchestrator>,
    coordinator: ActorRef<ClusterCoordinator>,
    remote_timeout: Duration,
    broadcast_timeout: Duration,
    broadcast_fanout_limit: usize,
    remote_retry_attempts: u8,
    default_search_limit: usize,
    broadcasts_total: Arc<AtomicU64>,
    broadcast_failures: Arc<AtomicU64>,
    // Streaming search configuration
    streaming: StreamingSearchConfig,
    /// Worker pool channel for dispatching hot-path ops (Write, Search)
    /// bypassing the actor mailbox for concurrent processing.
    worker_tx: Option<OrchestratorWorkerTx>,
    /// Shared pool of cached RemoteActorRef handles for avoiding repeated lookups.
    remote_peer_pool: Arc<RemotePeerPool>,
    /// Shard-affine dispatch configuration and shared routing ring.
    shard_affine: ShardAffineConfig,
    /// This node's shards, published lock-free. Lets a keyed operation be recognised as
    /// local without asking the coordinator — see `route_and_handle`.
    placement: Arc<ArcSwap<ShardPlacement>>,
}

/// Configuration for shard-affine worker dispatch.
#[derive(Clone, Debug)]
pub struct ShardAffineConfig {
    /// Shared routing ring for shard-affine dispatch (lock-free via ArcSwap).
    /// When `enabled` is true, the router resolves the target shard from the
    /// routing key and routes the job to the affine worker.
    pub routing_ring: Arc<ArcSwap<ConsistentRing>>,
    /// Enable shard-affine worker dispatch (default: false).
    pub enabled: bool,
}

/// Configuration for streaming search behavior.
#[derive(Clone, Debug)]
pub struct StreamingSearchConfig {
    pub enable_streaming_search: bool,
    pub enable_early_termination: bool,
    pub max_concurrent_shard_searches: usize,
    pub max_concurrent_remote_searches: usize,
}

impl StreamingSearchConfig {
    pub fn from_search_config(sc: &SearchConfig) -> Self {
        Self {
            enable_streaming_search: sc.enable_streaming_search,
            enable_early_termination: sc.enable_early_termination,
            max_concurrent_shard_searches: sc.max_concurrent_shard_searches,
            max_concurrent_remote_searches: sc.max_concurrent_remote_searches,
        }
    }
}

impl RouterActor {
    #[allow(clippy::too_many_arguments)]
    pub fn with_config(
        orchestrator: ActorRef<NodeOrchestrator>,
        coordinator: ActorRef<ClusterCoordinator>,
        messaging: &MessagingConfig,
        streaming: StreamingSearchConfig,
        default_search_limit: usize,
        worker_tx: Option<OrchestratorWorkerTx>,
        remote_peer_pool: Arc<RemotePeerPool>,
        shard_affine: ShardAffineConfig,
        placement: Arc<ArcSwap<ShardPlacement>>,
    ) -> Self {
        Self {
            orchestrator,
            coordinator,
            remote_timeout: Duration::from_secs(messaging.request_timeout_secs),
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
        }
    }

    /// Handles client operations.
    ///
    /// Hot-path ops (Write, Search, Stream) are dispatched to the worker pool
    /// for concurrent processing, bypassing the actor mailbox.
    /// Other ops (BulkWrite, config, metadata) go through the actor mailbox.
    #[cfg_attr(not(test), allow(dead_code))]
    pub async fn handle_client_op(&self, op: ClientOp) -> Result<JsonValue, OrchestratorError> {
        // Try worker pool for hot-path ops
        if let Some(tx) = &self.worker_tx {
            let is_worker_eligible = matches!(
                op,
                ClientOp::Write { .. }
                    | ClientOp::Delete { .. }
                    | ClientOp::Search { .. }
                    | ClientOp::Stream { .. }
            );
            if is_worker_eligible {
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
                            // Queue full — fall through to actor mailbox
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
    async fn ask_orchestrator(&self, op: ClientOp) -> Result<JsonValue, OrchestratorError> {
        match self.orchestrator.ask(op).await {
            Ok(result) => Ok(result),
            Err(kameo::error::SendError::HandlerError(err)) => Err(err),
            Err(e) => Err(OrchestratorError::Io(std::io::Error::other(format!(
                "Actor error: {}",
                e
            )))),
        }
    }

    pub async fn admin_memory(&self) -> Result<AdminMemoryReport, OrchestratorError> {
        self.orchestrator.ask(GetAdminMemory).await.map_err(|e| {
            OrchestratorError::Io(std::io::Error::other(format!("Actor error: {}", e)))
        })
    }

    pub async fn admin_purge_memory(
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

    pub async fn admin_commit_index(
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

    pub async fn admin_evict_index_writer(
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
    pub fn admin_worker_stats(&self) -> Result<WorkerPoolReport, OrchestratorError> {
        match &self.worker_tx {
            Some(tx) => Ok(tx.snapshot()),
            None => Err(OrchestratorError::Io(std::io::Error::other(
                "Worker pool not initialized",
            ))),
        }
    }

    /// Hits returned when a query names no limit, as this node is configured.
    ///
    /// A caller that passes `None` through gets this applied for it, deeper down. It is exposed
    /// for the one caller that has to know the number before the search runs: a federated merge
    /// truncates the combined result itself, and doing that against a different default than
    /// the searches used would report a limit the node did not apply.
    pub fn default_search_limit(&self) -> usize {
        self.default_search_limit
    }

    /// Answer "this node owns the key" from published state, or `None` to ask the
    /// coordinator.
    ///
    /// Deliberately conservative: it returns `Some` only for a key whose owning shard is on
    /// this node. An unkeyed operation is a scatter-gather whose answer depends on how many
    /// nodes are in the cluster, which is the coordinator's to know, so it is left alone.
    fn resolve_local(&self, routing_key: Option<&str>) -> Option<RoutingDecision> {
        let key = routing_key?;
        let shard = self.shard_affine.routing_ring.load().get_owner(key)?;
        self.placement
            .load()
            .is_local(&shard)
            .then_some(RoutingDecision::Local)
    }

    /// Route via ClusterCoordinator then handle locally (remote/broadcast stubbed).
    #[cfg_attr(not(test), allow(dead_code))]
    pub async fn route_and_handle(
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
    pub async fn route_and_handle_keeping_sort_keys(
        &self,
        op: ClientOp,
        routing_key: Option<String>,
        operation_type: OperationType,
    ) -> Result<JsonValue, OrchestratorError> {
        self.route_and_handle_inner(op, routing_key, operation_type, false)
            .await
    }

    async fn route_and_handle_inner(
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
                    return Err(OrchestratorError::Io(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        "Write operation cannot be broadcast - routing failed",
                    )));
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
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn route_and_handle_stream(
        &self,
        op: ClientOp,
        routing_key: Option<String>,
        operation_type: OperationType,
    ) -> mpsc::Receiver<Result<bytes::Bytes, std::io::Error>> {
        const STREAM_CHANNEL_CAPACITY: usize = 64;

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
    async fn stream_search_result_as_ndjson(
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

    /// Get the number of active shards (for health check).
    pub async fn shard_count(&self) -> usize {
        // Forward to orchestrator actor
        (self
            .orchestrator
            .ask(crate::node_orchestrator::GetShardCount)
            .await)
            .unwrap_or_default()
    }

    async fn handle_broadcast(&self, op: ClientOp) -> Result<JsonValue, OrchestratorError> {
        use crate::cluster_coordinator::{GetKnownPeers, KnownPeer};

        self.broadcasts_total.fetch_add(1, AtomicOrdering::Relaxed);

        // A paged search is widened before it is fanned out: every node — this one included —
        // is asked for `offset + limit` hits from the front of its own order, and the skip is
        // applied here, once, after their blocks have been merged into one order. A node that
        // skipped `offset` of its own hits would drop rows that belong on this page.
        //
        // The window is read off the original op and the copies carry no offset, so this cannot
        // be applied twice however many levels the request travels through.
        let window = match &op {
            ClientOp::Search { limit, offset, .. } => SearchWindow {
                offset: offset.unwrap_or(0),
                limit: limit.unwrap_or(self.default_search_limit),
            },
            _ => SearchWindow::first(self.default_search_limit),
        };
        let op = match op {
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
        };

        // Get known peers for remote fan-out
        let peers: Vec<KnownPeer> = self
            .coordinator
            .ask(GetKnownPeers)
            .await
            .unwrap_or_default();

        info!(
            "🔍 Broadcast operation: got {} known peers from coordinator",
            peers.len()
        );
        for peer in &peers {
            info!("  📍 Peer: {} at {}", peer.node_id, peer.address);
        }

        let peer_count = peers.len().min(self.broadcast_fanout_limit);
        info!(
            timeout_ms = self.broadcast_timeout.as_millis(),
            fanout_limit = self.broadcast_fanout_limit,
            local_shard_concurrency_limit = self.streaming.max_concurrent_shard_searches,
            remote_concurrency_limit = self.streaming.max_concurrent_remote_searches.max(1),
            known_peers = peers.len(),
            target_peers = peer_count,
            "RouterActor: broadcast routing with remote fan-out"
        );

        // Start local operation
        let local_op = op.clone();
        let local_future = self.handle_client_op(local_op);

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
                            remote_router.try_remote(op_clone, node_id, &peer_addr),
                        )
                        .await;
                        (dispatch_ordinal, outcome)
                    }
                }),
        )
        .buffer_unordered(remote_limit)
        .collect::<Vec<_>>();

        // Execute local + remote concurrently
        let t_start = Instant::now();
        let (local_result, remote_results) = tokio::join!(local_future, remote_results_future);

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
            };

            // One block per source, taken whole. Ordering across blocks is
            // `order_hit_blocks`'s business, and it needs to know which source each hit came
            // from to settle a tie the same way twice.
            fn push_hits(
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
                stats.nodes_contacted += 1;
                if let Some(t) = value.get("took_ms").and_then(|v| v.as_u64()) {
                    stats.max_took_ms = match stats.max_took_ms {
                        Some(cur) => Some(cur.max(t)),
                        None => Some(t),
                    };
                }
            }

            // The local node is rank 0, then each peer in the order it was dispatched to.
            let mut blocks: Vec<Vec<JsonValue>> = Vec::new();

            match local_result {
                Ok(mut val) => push_hits(&mut val, &mut blocks, &mut stats),
                Err(e) => {
                    error_count += 1;
                    warn!(error = %e, "Broadcast: local search failed");
                }
            }

            // Back into dispatch order before merging, rather than the order they finished in.
            let mut remote_results = remote_results;
            remote_results.sort_by_key(|(dispatch_ordinal, _)| *dispatch_ordinal);
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

        // Back into dispatch order, so that the merge below ranks the nodes the same way on
        // every run rather than by which of them answered first.
        let mut remote_results = remote_results;
        remote_results.sort_by_key(|(dispatch_ordinal, _)| *dispatch_ordinal);
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

    /// Streaming version of handle_broadcast for improved search performance
    async fn handle_broadcast_streaming(
        &self,
        op: ClientOp,
    ) -> Result<JsonValue, OrchestratorError> {
        tracing::info!(
            max_concurrent_shard_searches = self.streaming.max_concurrent_shard_searches,
            max_concurrent_remote_searches = self.streaming.max_concurrent_remote_searches.max(1),
            "🚀 Using STREAMING search for improved performance"
        );

        use crate::cluster_coordinator::{GetKnownPeers, KnownPeer};

        self.broadcasts_total.fetch_add(1, AtomicOrdering::Relaxed);

        // Get known peers for remote fan-out
        let peers: Vec<KnownPeer> = self
            .coordinator
            .ask(GetKnownPeers)
            .await
            .unwrap_or_default();

        let start_time = std::time::Instant::now();

        // Handle search operations with streaming
        match op {
            // `offset` is ignored rather than bound: this is the streaming path, which hands
            // the caller the whole result as it is produced, so there is no page to take.
            ClientOp::Search {
                index,
                query,
                limit,
                offset: _,
                fields,
                sort,
            }
            | ClientOp::Stream {
                index,
                query,
                limit,
                fields,
                sort,
            } => {
                let limit = limit.unwrap_or(self.default_search_limit);

                // Create local search stream using improved concurrent approach
                let local_future = async {
                    // Bind to an explicit `Result` type: the reply flows through a nested
                    // `async` block feeding a `Pin<Box<dyn Future>>`, which defeats
                    // rust-analyzer's inference of the `ask().await` output and makes it
                    // flag the `Ok`/`Err` match as non-exhaustive (E0004). The annotation
                    // resolves the type without changing behavior (rustc already accepts it).
                    let local_result: Result<JsonValue, _> = self
                        .orchestrator
                        .ask(ClientOp::Search {
                            index: index.clone(),
                            query: query.clone(),
                            limit: Some(limit),
                            offset: None,
                            fields: fields.clone(),
                            sort: sort.clone(),
                        })
                        .await;
                    match local_result {
                        Ok(result) => StreamingSearchResult::Local {
                            shard_id: Uuid::nil(), // Individual shard IDs are in the documents
                            hits: result
                                .get("hits")
                                .and_then(|h| h.as_array())
                                .map(|arr| arr.to_vec())
                                .unwrap_or_default()
                                .iter()
                                .filter_map(|hit| {
                                    hit.get("_score")
                                        .and_then(|s| s.as_f64())
                                        .map(|score| (score as f32, hit.clone()))
                                })
                                .collect(),
                            total_hits: result
                                .get("total_hits")
                                .and_then(|t| t.as_u64())
                                .unwrap_or(0) as usize,
                            took_ms: 0,
                        },
                        Err(_) => StreamingSearchResult::Local {
                            shard_id: Uuid::nil(),
                            hits: Vec::new(),
                            total_hits: 0,
                            took_ms: 0,
                        },
                    }
                };

                let remote_limit = self.streaming.max_concurrent_remote_searches.max(1);
                let remote_timeout = self.broadcast_timeout;
                let mut peer_iter = peers.into_iter().take(self.broadcast_fanout_limit);
                let remote_router = self.clone();

                let mut search_futures = FuturesUnordered::new();
                search_futures.push(Box::pin(local_future)
                    as Pin<Box<dyn Future<Output = StreamingSearchResult> + Send>>);

                let push_remote_future = |peer: KnownPeer,
                                          search_futures: &mut FuturesUnordered<
                    Pin<Box<dyn Future<Output = StreamingSearchResult> + Send>>,
                >| {
                    let remote_router = remote_router.clone();
                    let index = index.clone();
                    let query = query.clone();
                    let fields = fields.clone();
                    let sort = sort.clone();
                    let node_id = peer.node_id;
                    let peer_addr = peer.address;
                    search_futures.push(Box::pin(async move {
                        let result = timeout(
                            remote_timeout,
                            remote_router.try_remote(
                                ClientOp::Search {
                                    index,
                                    query,
                                    limit: Some(limit),
                                    offset: None,
                                    fields,
                                    sort,
                                },
                                node_id,
                                &peer_addr,
                            ),
                        )
                        .await
                        .unwrap_or(Err(OrchestratorError::Io(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            "Remote operation timed out",
                        ))));
                        StreamingSearchResult::Remote { node_id, result }
                    })
                        as Pin<Box<dyn Future<Output = StreamingSearchResult> + Send>>);
                };

                for _ in 0..remote_limit {
                    if let Some(peer) = peer_iter.next() {
                        push_remote_future(peer, &mut search_futures);
                    }
                }

                // Process results as they arrive with early termination
                // One block per source, keyed by that source's identity: this node's shards
                // ahead of its peers, each ordered by id. Streaming means they arrive in
                // whatever order they finish, and the key is what puts them back.
                let mut blocks: Vec<((u8, Uuid), Vec<JsonValue>)> = Vec::new();
                // Counted rather than measured off `blocks`, which the early-termination check
                // below consults on every iteration.
                let mut hits_collected = 0usize;
                let mut total_hits_sum = 0usize;
                let mut shards_queried = 0usize;
                let mut nodes_contacted = 0usize;
                let mut unique_shard_ids = std::collections::HashSet::new();
                let mut errors = Vec::new();

                while let Some(search_result) = search_futures.next().await {
                    let refill_remote =
                        matches!(&search_result, StreamingSearchResult::Remote { .. });
                    if refill_remote && let Some(peer) = peer_iter.next() {
                        push_remote_future(peer, &mut search_futures);
                    }

                    // Early termination if limit reached and enabled
                    if self.streaming.enable_early_termination
                        && hits_collected >= limit
                        && search_futures.is_empty()
                        && peer_iter.size_hint().0 == 0
                    {
                        break;
                    }

                    match search_result {
                        StreamingSearchResult::Local {
                            shard_id,
                            hits,
                            total_hits,
                            took_ms: _,
                        } => {
                            // Process streaming local search results
                            let mut block: Vec<JsonValue> = Vec::with_capacity(hits.len());
                            for (score, doc) in hits {
                                let mut hit_doc = doc;
                                if let JsonValue::Object(ref mut o) = hit_doc {
                                    o.insert(
                                        "_score".to_string(),
                                        JsonValue::Number(
                                            serde_json::Number::from_f64(score as f64)
                                                .unwrap_or(serde_json::Number::from(0)),
                                        ),
                                    );
                                }
                                block.push(hit_doc);
                            }
                            hits_collected += block.len();
                            // Counted from the result, not from its hits — a shard that matched
                            // nothing still answered, and reading the id back out of each
                            // document missed that as well as costing a copy of it per hit.
                            unique_shard_ids.insert(shard_id);
                            blocks.push(((0, shard_id), block));
                            total_hits_sum += total_hits;
                            shards_queried = unique_shard_ids.len();
                            nodes_contacted += 1;
                        }
                        StreamingSearchResult::Remote { node_id, result } => {
                            nodes_contacted += 1;
                            match result {
                                Ok(mut val) => {
                                    // Note: mut val
                                    // OPTIMIZATION: Take mutable reference to array to move items
                                    if let Some(hits) =
                                        val.get_mut("hits").and_then(|h| h.as_array_mut())
                                    {
                                        let block: Vec<JsonValue> = std::mem::take(hits);
                                        hits_collected += block.len();
                                        blocks.push(((1, node_id), block));
                                    }
                                    if let Some(total) =
                                        val.get("total_hits").and_then(|t| t.as_u64())
                                    {
                                        total_hits_sum += total as usize;
                                    }
                                    // Extract shard statistics from the response
                                    if let Some(stats) =
                                        val.get("stats").and_then(|s| s.as_object())
                                        && let Some(shards) =
                                            stats.get("shards").and_then(|s| s.as_object())
                                        && let Some(responded) =
                                            shards.get("responded").and_then(|r| r.as_u64())
                                    {
                                        shards_queried += responded as usize;
                                    }
                                }
                                Err(e) => {
                                    errors.push(format!(
                                        "Remote node {} search failed: {}",
                                        node_id, e
                                    ));
                                }
                            }
                        }
                    }
                }

                // Ranked by source identity — this node's shards, then each peer — rather
                // than by which of them streamed in first, so that a tie between two hits is
                // settled the same way on every run. Early termination can still change *which*
                // sources contribute to a page; preferring whoever answers first is what this
                // path is for, and only the ordering of what did arrive is fixed here.
                blocks.sort_by_key(|(source, _)| *source);
                let all_hits = order_hit_blocks(
                    blocks.into_iter().map(|(_, block)| block).collect(),
                    sort.as_ref(),
                    SearchWindow::first(limit),
                );

                let mut response = serde_json::json!({
                    "hits": all_hits,
                    "hits_returned": all_hits.len(),
                    "total_hits": total_hits_sum,
                    "limit": limit,
                    "took_ms": start_time.elapsed().as_millis(),
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
    async fn handle_broadcast_request(&self, op: ClientOp) -> Result<JsonValue, OrchestratorError> {
        // Implementation for non-search operations (write, bulk_write, etc.)
        // This is the existing handle_broadcast logic
        self.handle_broadcast(op).await
    }

    async fn handle_remote(
        &self,
        op: ClientOp,
        node_id: Uuid,
        peer_addr: String,
    ) -> Result<JsonValue, OrchestratorError> {
        let max_attempts = std::cmp::max(1, self.remote_retry_attempts as usize);
        let mut last_err = None;

        for attempt in 1..=max_attempts {
            let op_clone = op.clone();
            match timeout(
                self.remote_timeout,
                self.try_remote(op_clone, node_id, &peer_addr),
            )
            .await
            {
                Ok(Ok(value)) => return Ok(value),
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

        Err(OrchestratorError::Io(std::io::Error::other(reason)))
    }
}

impl RouterActor {
    /// Attempt a remote call to a microshard on another node.
    /// Uses the cached RemotePeerPool to avoid repeated swarm registry lookups.
    async fn try_remote(
        &self,
        _op: ClientOp,
        node_id: Uuid,
        peer_addr: &str,
    ) -> Result<JsonValue, OrchestratorError> {
        info!(
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
                OrchestratorError::Io(std::io::Error::other(format!(
                    "remote orchestrator for node {} not found",
                    node_id
                )))
            })?;

        let res = remote
            .ask(&_op)
            .await
            .map_err(|e| OrchestratorError::Io(std::io::Error::other(e.to_string())))?;
        Ok(res)
    }
}

#[derive(Debug, Actor, RemoteActor)]
pub struct NodeOrchestrator {
    /// Map of shard UUIDs to their microshard actors
    pub(crate) shards: HashMap<Uuid, MicroshardActor>,
    /// This node's identity (UUID, name, virtual tokens)
    identity: NodeIdentity,
    /// Node configuration  
    config: NodeConfig,
    /// Consistent hash ring for routing writes based on routing keys
    routing_ring: ConsistentRing,
    /// Optional coordinator reference for shard registration
    coordinator: Option<ActorRef<ClusterCoordinator>>,
    /// Shared routing ring snapshot (lock-free via ArcSwap).
    /// Wrapped in Arc so it can be shared with the OrchestratorEngine worker pool
    /// and the RouterActor for shard-affine dispatch.
    shared_routing_ring: Arc<ArcSwap<ConsistentRing>>,
    /// Cores this process may use, resolved once. Sizes the worker pool and places both
    /// workers and writer threads, so all of them count the same cores.
    core_layout: CoreLayout,
    /// Shard ordinals, published lock-free. Read by the dispatcher to pick a shard's worker
    /// and by the router to answer "is this shard mine?" without a coordinator round trip.
    placement: Arc<ArcSwap<ShardPlacement>>,
    /// Per-index schema cache to avoid repeated metadata reads (lock-free via ArcSwap).
    /// Wrapped in Arc so it can be shared with the OrchestratorEngine worker pool.
    schema_cache: Arc<ArcSwap<HashMap<String, Arc<IndexSchema>>>>,
    /// Default search result limit when not specified in request
    default_search_limit: usize,
    max_concurrent_shard_searches: usize,
    /// Shared engine state for the worker pool (Arc-wrapped, lock-free).
    /// Workers operate on this concurrently without going through the actor mailbox.
    engine: Option<Arc<OrchestratorEngine>>,
    /// Channel sender for dispatching jobs to the worker pool.
    /// Workers pull jobs from the receiver and execute on the shared engine.
    worker_tx: Option<OrchestratorWorkerTx>,
    /// Number of worker tasks spawned in the pool.
    /// Used to signal explicit worker shutdown.
    worker_count: usize,
    /// Handles for pinned worker OS threads (Stage 2e). Empty when running in the
    /// default unpinned tokio-task mode. Joined during shutdown to ensure clean
    /// teardown of per-worker `current_thread` runtimes.
    worker_threads: Vec<std::thread::JoinHandle<()>>,
    /// Dedicated tokio runtime for read operations (search, stats).
    /// Isolates read I/O from the writer threads and tokio's generic blocking pool.
    /// Arc-wrapped so the runtime outlives shard clones that hold its Handle.
    read_runtime: Option<Arc<tokio::runtime::Runtime>>,
    /// Shared pool of cached RemoteActorRef handles for avoiding repeated lookups.
    remote_peer_pool: Option<Arc<RemotePeerPool>>,
}

impl NodeOrchestrator {
    fn storage_path_candidates(&self) -> Cow<'_, [PathBuf]> {
        if self.config.storage_paths.is_empty() {
            Cow::Owned(vec![self.config.storage_path.clone()])
        } else {
            Cow::Borrowed(&self.config.storage_paths)
        }
    }

    fn deterministic_shard_directory(&self, shard_id: Uuid) -> PathBuf {
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
    pub fn generate_balanced_shard_id(&self) -> Uuid {
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

    /// Validates schema for documents in parallel, then evolves schema sequentially.
    ///
    /// This method uses a two-stage approach:
    /// Stage 1: Parallel validation (read-only, CPU-bound)
    /// Stage 2: Sequential schema evolution (write operations only when needed)
    async fn staged_schema_validation(
        &self,
        index: &str,
        docs: &[DocPayload],
        schema_cache: &mut IndexSchema,
    ) -> Result<SchemaValidationSummary, OrchestratorError> {
        if docs.is_empty() {
            return Ok(SchemaValidationSummary {
                total_docs: 0,
                valid_docs: 0,
                evolution_needed: false,
                all_new_fields: std::collections::HashSet::new(),
                errors: Vec::new(),
            });
        }

        // Enhanced sampling for initial schema creation
        let is_initial_creation = schema_cache.fields.is_empty();
        if is_initial_creation {
            let sampled_schema = enhanced_schema_sampling(docs, SCHEMA_SAMPLE_LIMIT);
            let sampled_field_count = sampled_schema.fields.len();

            // Merge sampled schema into cache for better type detection
            for (field_name, field_def) in &sampled_schema.fields {
                if !schema_cache.fields.contains_key(field_name) {
                    schema_cache
                        .fields
                        .insert(field_name.clone(), field_def.clone());
                }
            }

            // Persist before the write reaches a shard. Sampling has already put these
            // fields into `schema_cache`, so validation below finds nothing new and the
            // evolution stage — which is the only other thing that persists — never runs.
            // Left in memory, this schema would die here and the storage layer would derive
            // its own from the document, as non-indexed fields, permanently: the tantivy
            // schema is fixed when the first write creates the index.
            if sampled_field_count > 0 {
                Self::persist_schema_to_stores(index, schema_cache, &self.shards).await?;
            }

            tracing::info!(
                index = %index,
                sampled_fields = sampled_field_count,
                "Enhanced sampling merged and persisted for initial schema creation"
            );
        }

        // Stage 1: Parallel validation (read-only)
        let validation_results = self
            .parallel_validate_schema(index, docs, schema_cache)
            .await?;

        // Stage 2: Aggregate results and identify evolution needs
        let mut summary = SchemaValidationSummary {
            total_docs: docs.len(),
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
                schema_cache,
                &summary.all_new_fields,
                &self.shards,
                is_initial_creation,
            )
            .await?;
        }

        tracing::debug!(
            total_docs = summary.total_docs,
            valid_docs = summary.valid_docs,
            evolution_needed = summary.evolution_needed,
            new_fields_count = summary.all_new_fields.len(),
            errors_count = summary.errors.len(),
            "Staged schema validation completed"
        );

        Ok(summary)
    }

    /// Validate a batch, inline or fanned out, according to how big it is.
    ///
    /// Size decides *where* the work runs and nothing else: every document is judged by
    /// `validate_document`, so a batch of one and a batch of ten thousand reach the same
    /// verdict about the same document. That was not true while a second validator existed for
    /// small batches — see `validate_document` for what the two disagreed about.
    async fn parallel_validate_schema(
        &self,
        _index: &str,
        docs: &[DocPayload],
        schema_cache: &IndexSchema,
    ) -> Result<Vec<SchemaValidationResult>, OrchestratorError> {
        tracing::debug!(
            "Using parallel Rayon validation for {} documents",
            docs.len()
        );

        // Small batches validate inline. Offloading them costs two thread hops (onto this
        // worker's blocking pool, then a rayon fan-out onto the global rayon pool, which is
        // unpinned and competes with the writer threads) plus a full clone of the documents
        // and schema — all to run a handful of cheap per-document checks. Both hops are pure
        // overhead below this size.
        const INLINE_VALIDATION_MAX_DOCS: usize = 64;
        if docs.len() <= INLINE_VALIDATION_MAX_DOCS {
            return Ok(docs
                .iter()
                .map(|doc_payload| {
                    Self::validate_document(&doc_payload.id, &doc_payload.doc, schema_cache)
                })
                .collect());
        }

        // Clone data so it is Send + 'static inside spawn_blocking
        let docs_owned: Vec<DocPayload> = docs.to_vec();
        let schema_clone = schema_cache.clone();

        let results = tokio::task::spawn_blocking(move || {
            docs_owned
                .par_iter()
                .map(|doc_payload| {
                    Self::validate_document(&doc_payload.id, &doc_payload.doc, &schema_clone)
                })
                .collect::<Vec<SchemaValidationResult>>()
        })
        .await
        .map_err(|e| OrchestratorError::Io(std::io::Error::other(e)))?;

        Ok(results)
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
    /// Nothing about the checks needed to be split. The two hot paths differ in *where* the
    /// work runs — inline, or fanned out over rayon — which is `parallel_validate_schema`'s
    /// decision to make, and the per-document work is a handful of hash lookups either way.
    ///
    /// `id` is the identifier the write arrived with, beside the body rather than in it — see
    /// `unusable_document_identity` for why that is the authoritative one.
    fn validate_document(
        id: &str,
        doc: &JsonValue,
        schema_cache: &IndexSchema,
    ) -> SchemaValidationResult {
        // Check 1: the document has a usable identifier, and does not contradict it.
        if let Some(err) = unusable_document_identity(id, doc) {
            return SchemaValidationResult {
                needs_evolution: false,
                new_fields: Vec::new(),
                validation_error: Some(err),
            };
        }

        // Check 2: A shadow field is a name for the identifier, so it has to carry it.
        if let Some(err) = disagreeing_shadow_field(doc, schema_cache, id) {
            return SchemaValidationResult {
                needs_evolution: false,
                new_fields: Vec::new(),
                validation_error: Some(err),
            };
        }

        // Check 3: every field either fits what the schema declares, or is one the schema has
        // yet to hear about.
        let mut needs_evolution = false;
        let mut new_fields = Vec::new();

        if let Some(obj) = doc.as_object() {
            for (key, value) in obj {
                // The key is text whatever it looks like, and the index builds it itself.
                if key == "id" {
                    continue;
                }

                match schema_cache.fields.get(key) {
                    Some(existing_field) => {
                        if let Some(why) = unstorable_value(key, &existing_field.field_type, value)
                        {
                            return SchemaValidationResult {
                                needs_evolution: false,
                                new_fields: Vec::new(),
                                validation_error: Some(why),
                            };
                        }
                    }
                    None => {
                        // A field the schema has never seen is typed by the value as a whole,
                        // so a list makes a text field rather than a field of its element type:
                        // the next document may carry a list of something else, and text is the
                        // only type that can hold both.
                        needs_evolution = true;
                        new_fields.push((key.clone(), infer_field_type(value)));
                    }
                }
            }
        }

        SchemaValidationResult {
            needs_evolution,
            new_fields,
            validation_error: None,
        }
    }

    /// Add the fields validation discovered to the schema.
    ///
    /// `is_initial_creation` decides whether they are searchable, and comes from the caller
    /// rather than from `schema_cache.fields.is_empty()` — by the time this runs, sampling
    /// may already have populated the cache. See [`mark_initial_fields_indexed`] for why the
    /// two cases differ.
    async fn evolve_schema_sequential(
        &self,
        index: &str,
        schema_cache: &mut IndexSchema,
        new_fields: &std::collections::HashSet<(String, TantivyFieldType)>,
        shards: &HashMap<Uuid, MicroshardActor>,
        is_initial_creation: bool,
    ) -> Result<(), OrchestratorError> {
        // Only fields the schema does not already describe.
        let fields_to_add: Vec<_> = new_fields
            .iter()
            .filter(|(field_name, _)| !schema_cache.fields.contains_key(field_name))
            .collect();

        if fields_to_add.is_empty() {
            return Ok(());
        }

        tracing::debug!(
            index = %index,
            fields_count = fields_to_add.len(),
            is_initial_creation = is_initial_creation,
            "Batch adding new fields to schema"
        );

        for (field_name, field_type) in &fields_to_add {
            // `FieldDef::new` already applies the storage rule — only `id` is stored in
            // tantivy, everything else is reconstructed from redb.
            let mut new_field = FieldDef::new(field_name.clone(), field_type.clone());
            new_field.indexed = is_initial_creation;
            schema_cache.fields.insert(field_name.clone(), new_field);
        }

        tracing::info!(
            index = %index,
            fields_count = fields_to_add.len(),
            is_initial_creation = is_initial_creation,
            "Schema evolution completed - batch added fields"
        );

        // Persist updated schema to storage if changed
        if !new_fields.is_empty() {
            Self::persist_schema_to_stores(index, schema_cache, shards).await?;

            if is_initial_creation {
                tracing::info!(
                    index = %index,
                    field_count = schema_cache.fields.len(),
                    "Initial schema created with all fields indexed=true"
                );
            } else {
                tracing::info!(
                    index = %index,
                    total_fields = schema_cache.fields.len(),
                    new_fields_count = new_fields.len(),
                    "Schema evolved with new fields"
                );
            }
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
    async fn persist_schema_to_stores(
        index: &str,
        schema: &IndexSchema,
        shards: &HashMap<Uuid, MicroshardActor>,
    ) -> Result<(), OrchestratorError> {
        let stores: Vec<Arc<HybridStore>> = shards
            .values()
            .filter_map(|shard| shard.store.as_ref().map(Arc::clone))
            .collect();

        if stores.is_empty() {
            return Err(OrchestratorError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "No local stores available to persist schema",
            )));
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

    /// Process local shard batches sequentially, relying on actor message queues for proper isolation.
    ///
    /// Each shard actor processes its messages sequentially from its own queue,
    /// preventing concurrent access to shared storage resources.
    /// Returns what was written and one reason for every document that was not.
    ///
    /// A shard batch succeeds or fails whole — the writer thread applies it in one transaction —
    /// so a failure loses every document in it. Reported as one error per document rather than
    /// one per batch: a caller reading `errors` to decide what to retry cannot act on a single
    /// line standing for five hundred rows, and the count no longer matches what was lost.
    async fn parallel_local_shard_processing(
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
                        OrchestratorError::Io(std::io::Error::new(
                            std::io::ErrorKind::NotFound,
                            format!("Local shard {} not found", shard_id),
                        ))
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
                    tracing::info!(
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

        tracing::info!(
            "Local shard processing completed - total_written: {}, errors: {}",
            total_written,
            all_errors.len()
        );

        // Commit strategy: rely on the two existing commit mechanisms:
        //   1. Threshold-based commit inside apply_batch_and_maybe_commit (writer thread)
        //      — fires during the batch if enough ops accumulate.
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
    async fn forward_bulk_to_remote(
        &self,
        node_id: Uuid,
        peer_addr: &str,
        index: &str,
        batch: Vec<Placed>,
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

        let pool = self.remote_peer_pool.as_ref().ok_or_else(|| {
            OrchestratorError::Io(std::io::Error::other("Remote peer pool not initialized"))
        })?;

        let remote = pool
            .get_orchestrator(node_id, ConnectionChannel::Operations)
            .await
            .map_err(|e| {
                warn!("❌ Remote actor lookup error: {}", e);
                OrchestratorError::Io(std::io::Error::other(e.to_string()))
            })?
            .ok_or_else(|| {
                OrchestratorError::Io(std::io::Error::other(format!(
                    "Remote orchestrator for node {} not found",
                    node_id
                )))
            })?;

        // Send bulk write operation to remote node
        let op = ClientOp::BulkWrite {
            index: index.to_string(),
            docs,
        };

        let res: serde_json::Value = remote
            .ask(&op)
            .await
            .map_err(|e| OrchestratorError::Io(std::io::Error::other(e.to_string())))?;

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

    /// Fetch a schema from cache if present (lock-free).
    fn get_cached_schema(&self, index: &str) -> Option<Arc<IndexSchema>> {
        let map = self.schema_cache.load();
        map.get(index).cloned()
    }

    /// Insert or replace a schema in the cache (copy-on-write).
    fn put_cached_schema(&self, index: &str, schema: &IndexSchema) {
        let schema_arc = Arc::new(schema.clone());
        let index_str = index.to_string();

        self.schema_cache.rcu(|old| {
            let mut new = (**old).clone();
            new.insert(index_str.clone(), schema_arc.clone());
            new
        });
    }

    /// Produce sorted field names with "id" first (if present), others alphabetical.
    ///
    /// `_seq` is excluded for the same reason `describe_fields` excludes it: it is the engine's
    /// internal WAL sequence, not something a caller declared or can use. It has to be filtered
    /// *here* as well because this is the one listing that does not go through `describe_fields`
    /// — and because the schema it is handed has just been through
    /// `normalize_after_deserialization`, which inserts `_seq`. Without this, creating an index
    /// answers with a field that every other endpoint hides.
    fn sorted_field_names(schema: &IndexSchema) -> Vec<String> {
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
    fn describe_fields(
        schema: &IndexSchema,
        searchable: &HashSet<String>,
        sortable: &HashSet<String>,
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
    fn schema_response(
        index: &str,
        schema: &IndexSchema,
        searchable: &HashSet<String>,
        sortable: &HashSet<String>,
    ) -> JsonValue {
        let mut map = JsonMap::new();
        map.insert("name".to_string(), JsonValue::String(index.to_string()));
        if let Some(description) = &schema.description {
            map.insert(
                "description".to_string(),
                JsonValue::String(description.clone()),
            );
        }
        let fields = Self::describe_fields(schema, searchable, sortable);
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
    async fn field_capabilities_across_shards(
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
    pub async fn new(
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

        let mut orchestrator = Self {
            shards: HashMap::new(),
            identity,
            config,
            routing_ring: ConsistentRing::new(),
            coordinator: None,
            shared_routing_ring: Arc::new(ArcSwap::from_pointee(ConsistentRing::new())),
            core_layout: CoreLayout::detect(),
            placement: Arc::new(ArcSwap::from_pointee(ShardPlacement::default())),
            schema_cache: Arc::new(ArcSwap::from_pointee(HashMap::new())),
            default_search_limit,
            max_concurrent_shard_searches,
            engine: None,
            worker_tx: None,
            worker_count: 0,
            worker_threads: Vec::new(),
            read_runtime: Some(read_runtime),
            remote_peer_pool: None,
        };

        // Discover and hydrate existing shards
        orchestrator.hydrate_existing_shards().await?;

        Ok(orchestrator)
    }

    /// Set the coordinator ActorRef after it is spawned (used for shard registration).
    pub fn set_coordinator(&mut self, coordinator: ActorRef<ClusterCoordinator>) {
        self.coordinator = Some(coordinator);
    }

    /// Set the shared remote peer pool for cached actor ref lookups.
    pub fn set_remote_peer_pool(&mut self, pool: Arc<RemotePeerPool>) {
        self.remote_peer_pool = Some(pool);
    }

    /// Returns a clone of the worker pool sender, if the pool has been spawned.
    pub fn worker_tx(&self) -> Option<OrchestratorWorkerTx> {
        self.worker_tx.clone()
    }

    /// Returns a clone of the shared routing ring for shard-affine dispatch.
    pub fn shared_routing_ring(&self) -> Arc<ArcSwap<ConsistentRing>> {
        Arc::clone(&self.shared_routing_ring)
    }

    /// Returns a clone of the published shard placement, for dispatch and local routing.
    pub fn shard_placement(&self) -> Arc<ArcSwap<ShardPlacement>> {
        Arc::clone(&self.placement)
    }

    /// Build the shared `OrchestratorEngine` and spawn the worker pool.
    ///
    /// Must be called **after** `hydrate_existing_shards` and `set_coordinator`
    /// so that shards, routing ring, and coordinator are fully initialized.
    ///
    /// Worker count formula: `min(local_shards * 2, cpu_cores * 2)`, minimum 1.
    pub fn spawn_worker_pool(&mut self) {
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
            coordinator: self.coordinator.clone(),
            identity: self.identity.clone(),
            default_search_limit: self.default_search_limit,
            max_concurrent_shard_searches: self.max_concurrent_shard_searches,
            remote_peer_pool: pool,
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
        let mut worker_threads: Vec<std::thread::JoinHandle<()>> = Vec::new();

        for worker_id in 0..worker_count {
            let (tx, rx) = mpsc::channel::<OrchestratorJob>(per_worker_queue_capacity);
            worker_txs.push(tx);
            let engine = Arc::clone(&engine);
            let counters = Arc::clone(&worker_stats[worker_id]);
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
                ));
            }
        }

        let tx = OrchestratorWorkerTx::new_with_stats(
            worker_txs,
            worker_stats,
            per_worker_queue_capacity,
            pin_workers,
            aligned,
            self.core_layout.clone(),
            Arc::clone(&self.placement),
        );
        self.engine = Some(engine);
        self.worker_count = tx.len();
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
    async fn shutdown_worker_pool(&mut self) {
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
    fn publish_engine_state(&self) {
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
    fn place_shard(&mut self, shard_id: Uuid) -> WriterPin {
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
    fn activate_shard(&mut self, shard_id: Uuid) {
        let mut placement = (**self.placement.load()).clone();
        placement.activate(shard_id);
        self.placement.store(Arc::new(placement));
    }

    /// Scans the storage directory for existing shard folders and hydrates them with
    /// bounded concurrency. The bottleneck is redb::Builder::create() which does heavy
    /// disk I/O (WAL replay, compaction). Running all shards simultaneously causes I/O
    /// contention that makes each open 10-100× slower. A semaphore limits how many shards
    /// open their redb databases concurrently.
    async fn hydrate_existing_shards(&mut self) -> Result<(), OrchestratorError> {
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
            let sem = Arc::clone(&semaphore);
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
                        total_shards,
                        writer_shutdown_timeout_secs,
                        supervisor_timeout_secs,
                        writer_pin: writer_core,
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
    fn discover_existing_shards(&self) -> Result<Vec<Uuid>, OrchestratorError> {
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
    fn create_shard_storage_config(&self, _shard_id: Uuid, shard_path: PathBuf) -> StorageConfig {
        // Start at the minimum writer memory; storage will scale between min/max as the index grows.
        let indexer_memory_mb = self.config.indexer_memory_min_mb;

        StorageConfig {
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
        }
    }

    /// Handles a ProposeShard message to create a new shard.
    pub async fn handle_propose_shard(
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
                total_shards,
                writer_shutdown_timeout_secs: self.config.writer_shutdown_timeout_secs,
                supervisor_timeout_secs: self.config.supervisor_timeout_secs,
                writer_pin: writer_core,
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
    pub fn identity(&self) -> &NodeIdentity {
        &self.identity
    }

    /// Builds ShardMetadata for a given shard id (storage stats currently stubbed).
    fn shard_metadata(&self, shard_id: Uuid) -> ShardMetadata {
        ShardMetadata {
            shard_id,
            node_id: self.identity.uuid,
            vnode_tokens: generate_tokens(shard_id),
            storage_bytes: 0,
            document_count: 0,
        }
    }

    /// Registers a single shard with the coordinator if available.
    async fn register_shard_with_coordinator(
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
    pub async fn register_all_shards_with_coordinator(&self) -> Result<(), OrchestratorError> {
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
    pub async fn shutdown_all_shards(&mut self) -> Result<(), OrchestratorError> {
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
    fn register_shard_for_routing(&mut self, shard_id: Uuid) {
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

    /// Determines the shard that should handle a routing key.
    fn select_shard_for_key(&self, key: &str) -> Option<Uuid> {
        self.routing_ring.get_owner(key)
    }

    /// Returns the first shard id if any exist (fallback for empty ring).
    fn first_shard_id(&self) -> Option<Uuid> {
        self.shards.keys().copied().next()
    }

    /// Gets the number of active shards.
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    // ========================================================================
    // Client Operation Handling (for actor-based access - no locks needed)
    // ========================================================================

    /// Handles client operations. Called from Message<ClientOp> handler.
    pub async fn handle_client_op(&mut self, op: ClientOp) -> Result<JsonValue, OrchestratorError> {
        match op {
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
            } => self.orch_write(&index, id, routing_key, doc).await,
            ClientOp::BulkWrite { index, docs } => self.orch_bulk_write(&index, docs).await,
            ClientOp::Delete {
                index,
                id,
                routing_key,
            } => self.orch_delete(&index, id, routing_key).await,
            ClientOp::BulkDelete { index, docs } => self.orch_bulk_delete(&index, docs).await,
            ClientOp::CreateConfig { index, schema } => {
                self.orch_create_config(&index, schema).await
            }
            ClientOp::UpdateSchema {
                index,
                field_updates,
            } => self.orch_update_schema(&index, &field_updates).await,
            ClientOp::GetConfig { index } => self.orch_get_config(&index).await,
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
        }
    }

    /// Delete an index and all its data from all local shards (parallel)
    async fn orch_delete_index(
        &self,
        index: &str,
        delete_schema: bool,
    ) -> Result<JsonValue, OrchestratorError> {
        if self.shards.is_empty() {
            return Err(OrchestratorError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "No shards available",
            )));
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

        // Clear this index's cached schema (lock-free).
        {
            let idx = index.to_string();
            self.schema_cache.rcu(|old| {
                let mut new = (**old).clone();
                new.remove(&idx);
                new
            });
        }

        Ok(serde_json::json!({
            "success": true,
            "index": index,
            "deleted_from_shards": deleted_from_shards,
            "total_shards": self.shards.len(),
            "errors": errors
        }))
    }

    async fn orch_write(
        &self,
        index: &str,
        id: String,
        routing_key: Option<String>,
        doc: JsonValue,
    ) -> Result<JsonValue, OrchestratorError> {
        if self.shards.is_empty() {
            return Err(OrchestratorError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "No shards",
            )));
        }

        // Lock-free schema lookup, by the one thing that identifies an index: its name.
        let schema = if let Some(cached) = self.get_cached_schema(index) {
            cached
        } else {
            Arc::new(self.load_schema(index).await?)
        };

        // Fast path: mature schema — validate inline without spawn_blocking/Rayon
        if !schema.fields.is_empty() {
            let result = Self::validate_document(&id, &doc, &schema);
            if let Some(err) = result.validation_error {
                return Err(OrchestratorError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    err,
                )));
            }

            // If new fields detected, fall through to full validation for schema evolution
            if !result.needs_evolution {
                // Schema is stable — populate cache if not yet present
                if self.get_cached_schema(index).is_none() {
                    self.put_cached_schema(index, &schema);
                }

                // Schema-based routing
                let effective_routing_key = effective_routing_key(&schema, &id, routing_key, &doc);

                let target = self.route_write(&effective_routing_key)?;
                let shard = self.shards.get(&target).ok_or_else(|| {
                    OrchestratorError::Io(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        "Shard not found",
                    ))
                })?;
                let req = WriteRequest {
                    index: index.to_string(),
                    id: id.clone(),
                    routing_key: effective_routing_key.unwrap_or_default(),
                    doc,
                };

                return match shard.handle_write(req).await {
                    Ok(seq) => Ok(
                        serde_json::json!({"id": id, "result": "created", "version": seq, "shard_id": target.to_string()}),
                    ),
                    Err(e) => Err(e),
                };
            }
            // needs_evolution == true: fall through to slow path below
        }

        // Slow path: initial schema creation or schema evolution needed
        // Must use full staged_schema_validation with DocPayload wrapping
        let doc_payload = DocPayload {
            id: id.clone(),
            routing_key: routing_key.clone(),
            doc: doc.clone(),
        };
        let docs_slice = [doc_payload];

        let mut schema_mut = (*schema).clone();

        let validation_summary = self
            .staged_schema_validation(index, &docs_slice, &mut schema_mut)
            .await?;

        if validation_summary.evolution_needed || self.get_cached_schema(index).is_none() {
            self.put_cached_schema(index, &schema_mut);
        }

        if !validation_summary.errors.is_empty() {
            // One document, so its position adds nothing to the message.
            return Err(OrchestratorError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                validation_summary
                    .errors
                    .into_iter()
                    .map(|(_, reason)| reason)
                    .collect::<Vec<_>>()
                    .join("; "),
            )));
        }

        // Schema-based routing
        let effective_routing_key = effective_routing_key(&schema_mut, &id, routing_key, &doc);

        let target = self.route_write(&effective_routing_key)?;
        let shard = self.shards.get(&target).ok_or_else(|| {
            OrchestratorError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Shard not found",
            ))
        })?;
        let req = WriteRequest {
            index: index.to_string(),
            id: id.clone(),
            routing_key: effective_routing_key.unwrap_or_default(),
            doc,
        };

        match shard.handle_write(req).await {
            Ok(seq) => Ok(
                serde_json::json!({"id": id, "result": "created", "version": seq, "shard_id": target.to_string()}),
            ),
            Err(e) => Err(e),
        }
    }

    /// [`engine_delete`](OrchestratorEngine::engine_delete) on the actor.
    ///
    /// Reached two ways: a peer forwarding the op over `cameo.orchestrator.client_op`, and the
    /// mailbox fallback when a worker queue is full. Neither is the hot path, and both have to
    /// route a delete the same way the engine does or the same id would resolve to two shards
    /// depending on how busy the node was.
    async fn orch_delete(
        &self,
        index: &str,
        id: String,
        routing_key: Option<String>,
    ) -> Result<JsonValue, OrchestratorError> {
        if self.shards.is_empty() {
            return Err(OrchestratorError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "No shards",
            )));
        }

        let schema = if let Some(cached) = self.get_cached_schema(index) {
            cached
        } else {
            Arc::new(self.load_schema(index).await?)
        };

        let routing_key = effective_delete_routing_key(&schema, &id, routing_key)?;

        let target = self.route_write(&Some(routing_key))?;
        let shard = self.shards.get(&target).ok_or_else(|| {
            OrchestratorError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Shard not found",
            ))
        })?;

        let sequence = shard.handle_delete(index.to_string(), id.clone()).await?;
        Ok(serde_json::json!({
            "id": id,
            "result": "deleted",
            "version": sequence,
            "shard_id": target.to_string(),
        }))
    }

    /// Remove many documents in one request.
    ///
    /// The same shape as [`orch_bulk_write`](Self::orch_bulk_write) with the document work taken
    /// out: route each id, group by shard, hand each local shard one batch and each owning peer
    /// one forwarded op. What is absent is the point — no schema validation and no evolution,
    /// because a delete carries nothing that could grow a schema — so the schema is read once,
    /// for routing, and never written.
    ///
    /// An id that cannot be routed is an error against that id, not a failed batch: a batch may
    /// span tenants, and one id missing its routing key says nothing about the others.
    async fn orch_bulk_delete(
        &self,
        index: &str,
        docs: Vec<DeletePayload>,
    ) -> Result<JsonValue, OrchestratorError> {
        let start = std::time::Instant::now();
        if self.shards.is_empty() {
            return Err(OrchestratorError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "No shards",
            )));
        }

        let items_received = docs.len();
        let schema = if let Some(cached) = self.get_cached_schema(index) {
            cached
        } else {
            Arc::new(self.load_schema(index).await?)
        };

        let shard_assignments = if let Some(coord) = &self.coordinator {
            coord.ask(GetShardAssignments).await.unwrap_or_default()
        } else {
            HashMap::new()
        };

        let mut errors: Vec<String> = Vec::new();
        // Local work is a plain list of ids per shard: the routing key has already done its job
        // by the time a shard is chosen, and the storage layer deletes by key.
        let mut local_by_shard: HashMap<Uuid, Vec<String>> = HashMap::new();
        let mut remote_by_node: HashMap<Uuid, Vec<DeletePayload>> = HashMap::new();

        for payload in docs {
            let id = payload.id().to_string();
            if id.trim().is_empty() {
                errors.push("an entry carried an empty id".to_string());
                continue;
            }
            let routing_key = payload.routing_key().map(str::to_string);

            let key = match effective_delete_routing_key(&schema, &id, routing_key) {
                Ok(key) => key,
                Err(err) => {
                    errors.push(format!("{id}: {err}"));
                    continue;
                }
            };

            let Some(target) = self
                .select_shard_for_key(&key)
                .or_else(|| self.first_shard_id())
            else {
                errors.push(format!("{id}: no shard available for routing"));
                continue;
            };

            if self.shards.contains_key(&target) {
                local_by_shard.entry(target).or_default().push(id);
            } else if let Some(shard_meta) = shard_assignments.get(&target) {
                remote_by_node
                    .entry(shard_meta.node_id)
                    .or_default()
                    .push(payload);
            } else {
                errors.push(format!(
                    "{id}: no shard assignment for shard {target}, so it was not deleted"
                ));
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
                    let shard = shard.ok_or_else(|| {
                        OrchestratorError::Io(std::io::Error::new(
                            std::io::ErrorKind::NotFound,
                            format!("Local shard {shard_id} not found"),
                        ))
                    })?;
                    shard.handle_batch_delete(index_name, ids).await
                }
            })
            .collect();

        for result in futures::future::join_all(local_futures).await {
            match result {
                Ok(count) => deleted += count,
                Err(err) => errors.push(format!("local shard delete failed: {err}")),
            }
        }

        // Peers that own the rest.
        if !remote_by_node.is_empty() {
            let peer_addrs: HashMap<Uuid, String> = if let Some(coord) = &self.coordinator {
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
                            self.forward_bulk_delete_to_remote(node_id, &addr, index, payloads)
                                .await
                        });
                    }
                    None => errors.push(format!("no peer address for node {node_id}")),
                }
            }

            for result in futures::future::join_all(remote_futures).await {
                match result {
                    Ok(count) => deleted += count,
                    Err(err) => errors.push(format!("remote forwarding failed: {err}")),
                }
            }
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

    /// Hand a peer the part of a bulk delete its shards own.
    async fn forward_bulk_delete_to_remote(
        &self,
        node_id: Uuid,
        peer_addr: &str,
        index: &str,
        docs: Vec<DeletePayload>,
    ) -> Result<usize, OrchestratorError> {
        debug!(
            %node_id,
            %peer_addr,
            count = docs.len(),
            "Forwarding bulk delete batch to remote node"
        );

        let pool = self.remote_peer_pool.as_ref().ok_or_else(|| {
            OrchestratorError::Io(std::io::Error::other("Remote peer pool not initialized"))
        })?;

        let remote = pool
            .get_orchestrator(node_id, ConnectionChannel::Operations)
            .await
            .map_err(|e| OrchestratorError::Io(std::io::Error::other(e.to_string())))?
            .ok_or_else(|| {
                OrchestratorError::Io(std::io::Error::other(format!(
                    "Remote orchestrator for node {node_id} not found"
                )))
            })?;

        let op = ClientOp::BulkDelete {
            index: index.to_string(),
            docs,
        };

        let answer: JsonValue = remote
            .ask(&op)
            .await
            .map_err(|e| OrchestratorError::Io(std::io::Error::other(e.to_string())))?;

        // Its errors are the caller's errors too, so surface them rather than counting only
        // what succeeded.
        if let Some(remote_errors) = answer.get("errors").and_then(|e| e.as_array())
            && !remote_errors.is_empty()
        {
            warn!(
                %node_id,
                error_count = remote_errors.len(),
                "Remote node reported errors deleting its share of the batch"
            );
        }

        Ok(answer
            .get("items_deleted")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as usize)
    }

    async fn orch_bulk_write(
        &self,
        index: &str,
        docs: Vec<DocPayload>,
    ) -> Result<JsonValue, OrchestratorError> {
        let start = std::time::Instant::now();
        if self.shards.is_empty() {
            return Err(OrchestratorError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "No shards",
            )));
        }

        // `load_schema` answers from the cache when it can, and reads a shard when it cannot.
        let mut schema_cache = self.load_schema(index).await?;

        // Use staged schema validation: parallel validation + sequential evolution
        let validation_summary = self
            .staged_schema_validation(index, &docs, &mut schema_cache)
            .await?;

        if validation_summary.evolution_needed || self.get_cached_schema(index).is_none() {
            self.put_cached_schema(index, &schema_cache);
        }

        // Group documents by target shard using parallel routing for better performance
        let items_received = docs.len();

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

        // First, route all documents to determine local vs remote
        let mut local_docs = Vec::new();
        let mut remote_docs = Vec::new();

        // Clone routing ring for parallel access
        let routing_ring = self.routing_ring.clone();
        let first_shard_id = self.first_shard_id();

        // Get shard assignments to determine ownership (used later for remote routing)
        let shard_assignments = if let Some(coord) = &self.coordinator {
            coord.ask(GetShardAssignments).await.unwrap_or_default()
        } else {
            HashMap::new()
        };

        // Schema-based routing: use routing field from schema instead of per-document routing_key
        let routing_field = schema_cache.get_routing_field().to_string();

        // Route documents in parallel
        let routing_results: Vec<RoutingResult> = tokio::task::spawn_blocking(move || {
            pending
                .into_par_iter()
                .map(|mut placed| {
                    // Calculate effective routing key using schema's routing field
                    placed.routing_key = extract_routing_value(&placed.doc.doc, &routing_field)
                        .or_else(|| placed.doc.routing_key.clone())
                        .or_else(|| (!placed.doc.id.is_empty()).then(|| placed.doc.id.clone()))
                        .or_else(|| derive_routing_key_from_doc(&placed.doc.doc));

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
        let batches = Self::group_local_documents(local_docs);
        let unique_shards = batches.len();

        tracing::debug!(
            items_received = items_received,
            unique_shards = unique_shards,
            remote_docs = remote_docs.len(),
            "BulkWrite grouped items by shard"
        );

        // Fetch shard ownership and peer addresses to forward remote batches.
        let mut peer_addrs = HashMap::new();
        if let Some(coord) = &self.coordinator {
            peer_addrs = coord
                .ask(GetKnownPeers)
                .await
                .unwrap_or_default()
                .into_iter()
                .map(|p| (p.node_id, p.address))
                .collect();
        }

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
        let (local_written, local_errors) = self
            .parallel_local_shard_processing(index, local_batches)
            .await?;
        written += local_written;
        errors.extend(local_errors);

        // Phase 3.2: Parallel Remote Forwarding
        if !remote_batches.is_empty() {
            use futures::future::join_all;

            let remote_futures: Vec<_> = remote_batches
                .into_iter()
                .map(|(node_id, addr, batch)| async move {
                    // Kept so a call that never reached the node can still name what it carried.
                    let positions: Vec<usize> =
                        batch.iter().map(|placed| placed.position).collect();
                    let outcome = self
                        .forward_bulk_to_remote(node_id, &addr, index, batch)
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
        debug_assert_eq!(
            written + errors.len(),
            items_received,
            "a bulk write must account for every item it received"
        );

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

    /// Helper method to group local documents by shard
    fn group_local_documents(local_docs: Vec<(Placed, Uuid)>) -> HashMap<Uuid, Vec<Placed>> {
        let mut batches: HashMap<Uuid, Vec<Placed>> = HashMap::new();

        for (placed, shard_id) in local_docs {
            batches.entry(shard_id).or_default().push(placed);
        }

        batches
    }

    async fn orch_search(
        &self,
        index: &str,
        query: &str,
        window: SearchWindow,
        fields: Option<&[String]>,
        sort: Option<&SortSpec>,
    ) -> Result<JsonValue, OrchestratorError> {
        let start = std::time::Instant::now();
        if self.shards.is_empty() {
            return Ok(
                serde_json::json!({"hits": [], "hits_returned": 0, "total_hits": 0, "took_ms": 0}),
            );
        }

        // Get schema for shadow field transformation (lock-free)
        let schema = self
            .get_cached_schema(index)
            .map(|arc| (*arc).clone())
            .unwrap_or_default();

        // The identifier travels under the shadow name on the way out, so the projection is
        // rewritten before it is checked or applied.
        let fields = fields.map(|list| normalize_projection_fields(&schema, list));
        let fields = fields.as_deref();

        // Refuse a sort the index cannot answer before asking any shard: every shard would
        // fail the same way, and a scatter-gather reports that as a partial failure inside a
        // 200 rather than as the bad request it is.
        if let Some(refusal) = unsortable_sort_field(&schema, sort) {
            return Err(refusal);
        }

        let shard_targets: Vec<(Uuid, MicroshardActor)> = self
            .shards
            .iter()
            .map(|(&shard_id, shard)| (shard_id, shard.clone()))
            .collect();
        let shard_searches = shard_targets.into_iter().map(|(shard_id, shard)| {
            // The whole window from the front of each shard, for the reason given on
            // `SearchWindow::fetch_count` — the skip cannot be pushed down here either.
            let req = SearchRequest {
                index: index.to_string(),
                query: query.to_string(),
                limit: Some(window.fetch_count()),
                sort: sort.cloned(),
            };
            async move { (shard_id, shard.handle_search(req).await) }
        });
        let shard_results: Vec<_> = futures::stream::iter(shard_searches)
            .buffer_unordered(self.max_concurrent_shard_searches.max(1))
            .collect::<Vec<_>>()
            .await;

        let mut results: Vec<(Uuid, f32, JsonValue)> = Vec::new();
        // Kept as values rather than formatted here: whether nothing ran at all, and whose fault
        // that was, is decided from the errors themselves once the gather is complete.
        let mut failures: Vec<(Uuid, OrchestratorError)> = Vec::new();
        let mut shard_success = 0usize;
        let mut total_hits_sum = 0usize;
        // Every shard parses the same query string, so collect the distinct set.
        let mut discarded: Vec<String> = Vec::new();
        // One shard reporting an approximate order describes the whole answer; see the same
        // gather in `engine_search`.
        let mut approximate_sort: Option<String> = None;
        // One shard is enough. Shards can hold different schemas for the same index, and a
        // query that one of them could not run at all is not answered by the ones that could.
        let mut emptied = false;
        for (shard_id, result) in shard_results {
            match result {
                Ok(r) => {
                    emptied |= r.emptied;
                    total_hits_sum += r.total_hits;
                    for hit in r.hits {
                        results.push((shard_id, hit.score, hit.doc));
                    }
                    for note in r.discarded {
                        if !discarded.contains(&note) {
                            discarded.push(note);
                        }
                    }
                    approximate_sort = approximate_sort.or(r.approximate_sort);
                    shard_success += 1;
                }
                Err(err) => {
                    warn!(%shard_id, error = %err, "Scatter search shard failed");
                    failures.push((shard_id, err));
                }
            }
        }

        // Nothing ran, so there is nothing to qualify: refuse rather than answer with the empty
        // page a partial outage would produce. This is where the one refusal the sort guard above
        // cannot make lands — `fast` is a declaration and the column is written from it when the
        // index is built, so a field declared fast after the fact has no column to order by, and
        // only the built index knows that.
        if let Some(refusal) = no_shard_answered(index, shard_success, &failures) {
            return Err(refusal);
        }
        let errors = shard_error_notes(&failures);

        // Order merged results: by the requested sort field when provided, otherwise by
        // score descending. Each shard already returned field-sorted results, so a global
        // re-sort here interleaves them correctly across this node's shards. When sorting,
        // stamp the normalized `SORT_KEY_FIELD` first (while the full doc is present) so it
        // survives projection and lets the requesting node's cross-node merge re-order
        // these hits even when the sort field is not among the returned fields.
        if let Some(spec) = sort {
            stamp_sort_keys(&mut results, spec, &schema);
        }
        order_shard_hits(&mut results, sort);
        let results: Vec<(Uuid, f32, JsonValue)> = window.apply(results);
        let hits: Vec<JsonValue> = results
            .into_iter()
            .map(|(_shard_id, score, mut doc)| {
                // Add metadata fields
                if let JsonValue::Object(ref mut o) = doc {
                    o.insert(
                        "_score".to_string(),
                        serde_json::Number::from_f64(score as f64)
                            .map(JsonValue::Number)
                            .unwrap_or(JsonValue::Null),
                    );
                }

                // Apply field projection if specified
                if let Some(field_list) = fields {
                    apply_field_projection(doc, field_list)
                } else {
                    doc
                }
            })
            .collect();
        let mut response = serde_json::json!({
            "hits": hits,
            "hits_returned": hits.len(),
            "total_hits": total_hits_sum,
            "limit": window.limit,
            "offset": window.offset,
            "took_ms": start.elapsed().as_millis(),
            "stats": {
                "shards": {
                    "total": self.shards.len(),
                    "responded": shard_success,
                    "failed": errors.len()
                }
            },
        });
        attach_shard_errors(&mut response, errors);
        // Refuse instead of answering. An emptied query ran as nothing, so the zero it
        // produces is not a negative result — reported as a 200 it cannot be told apart from
        // "no document matches", which is the same confusion an unrunnable sort caused.
        if emptied {
            return Err(OrchestratorError::UnrunnableQuery {
                notes: discarded.join("; "),
            });
        }

        discarded.extend(unknown_projection_fields(&schema, fields));
        attach_discarded(&mut response, discarded);
        attach_approximate_sort(&mut response, approximate_sort);
        Ok(response)
    }

    async fn orch_create_config(
        &self,
        index: &str,
        mut schema: IndexSchema,
    ) -> Result<JsonValue, OrchestratorError> {
        // Normalize schema from external sources (populate field names from map keys, etc.)
        schema.normalize_after_deserialization();

        // Ensure 'id' field is explicitly in the schema for visibility
        if !schema.fields.contains_key("id") {
            schema.fields.insert(
                "id".to_string(),
                FieldDef::new("id".to_string(), TantivyFieldType::Text),
            );
        }

        let stores: Vec<Arc<HybridStore>> = self
            .shards
            .values()
            .filter_map(|shard| shard.store.as_ref().map(Arc::clone))
            .collect();

        if stores.is_empty() {
            return Err(OrchestratorError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "No local stores available to persist schema",
            )));
        }

        let index_name = index.to_string();
        let schema_clone = schema.clone();

        // Persist schema AND pre-create Tantivy index to all stores concurrently
        // This prevents race conditions where bulk writes start before the index exists
        tracing::info!(
            index = %index,
            num_shards = stores.len(),
            num_fields = schema.fields.len(),
            "Creating schema and pre-creating Tantivy indexes on all shards"
        );

        let handles: Vec<_> = stores
            .into_iter()
            .map(|store| {
                let idx = index_name.clone();
                let sch = schema_clone.clone();
                tokio::task::spawn_blocking(move || {
                    // First store the schema
                    store.store_schema_and_cache(&idx, &sch)?;
                    tracing::debug!(index = %idx, "Schema stored and cached");

                    // Then pre-create the Tantivy index with the full schema
                    // This ensures the index exists before any writes occur
                    drop(store.get_or_create_index(&idx)?);
                    tracing::debug!(index = %idx, "Tantivy index created");

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

        self.put_cached_schema(index, &schema);

        tracing::info!(
            index = %index,
            "Schema creation completed successfully"
        );

        Ok(serde_json::json!({
            "acknowledged": true,
            "index": index,
            "field_names": Self::sorted_field_names(&schema)
        }))
    }

    /// Set `indexed` flags on an existing schema, across every shard that holds it.
    ///
    /// Two properties this owes the caller, neither of which `CreateConfig` could provide.
    ///
    /// It does not re-create the Tantivy index, so it works on an index that is open — which
    /// is every index that has ever been written to, and the reason the previous
    /// implementation answered `500` for all of them.
    ///
    /// And it is all-or-nothing across shards: every shard is asked whether it would accept
    /// the edit before any shard writes. A shard can legitimately disagree — one that has not
    /// materialised the index yet accepts a promotion the others refuse — so a single shard's
    /// refusal has to refuse the whole request, or the schema diverges between shards.
    ///
    /// The gap between asking and writing is not locked, so a schema change racing this one can
    /// still leave a shard refusing during the apply pass. Each shard re-validates before it
    /// writes, so the outcome of that race is a reported refusal and an edit applied to some
    /// shards — never a shard that wrote something it had already judged impossible. Schema
    /// edits are rare administrative operations against a rare competing writer, which is why
    /// this is a documented bound rather than a lock.
    async fn orch_update_schema(
        &self,
        index: &str,
        field_updates: &BTreeMap<String, bool>,
    ) -> Result<JsonValue, OrchestratorError> {
        let stores: Vec<Arc<HybridStore>> = self
            .shards
            .values()
            .filter_map(|shard| shard.store.as_ref().map(Arc::clone))
            .collect();

        if stores.is_empty() {
            return Err(OrchestratorError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "No local stores available to update schema",
            )));
        }

        let plan = self
            .fan_out_schema_update(&stores, index, field_updates, true)
            .await?;

        if plan.is_rejected() {
            return Ok(Self::schema_update_response(index, &plan));
        }

        let applied = self
            .fan_out_schema_update(&stores, index, field_updates, false)
            .await?;

        // Every shard now agrees on the stored schema, so refresh the orchestrator's own copy
        // from one of them rather than reconstructing what it should be.
        if let Some(store) = stores.first()
            && let Ok(Some(schema)) = store.get_schema(index)
        {
            self.put_cached_schema(index, &schema);
        }

        tracing::info!(
            index = %index,
            num_shards = stores.len(),
            applied = ?applied.applied,
            "Schema field flags updated across shards"
        );

        Ok(Self::schema_update_response(index, &applied))
    }

    /// Run the plan or apply half of a schema update on every shard and merge the verdicts.
    async fn fan_out_schema_update(
        &self,
        stores: &[Arc<HybridStore>],
        index: &str,
        field_updates: &BTreeMap<String, bool>,
        plan_only: bool,
    ) -> Result<SchemaFieldUpdate, OrchestratorError> {
        let handles: Vec<_> = stores
            .iter()
            .map(|store| {
                let store = Arc::clone(store);
                let idx = index.to_string();
                let updates = field_updates.clone();
                tokio::task::spawn_blocking(move || {
                    if plan_only {
                        store.plan_field_indexing(&idx, &updates)
                    } else {
                        store.update_field_indexing(&idx, &updates)
                    }
                })
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
        // inferred from a bulk load is sampled from up to `SCHEMA_SAMPLE_LIMIT` documents and
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
    fn schema_update_response(index: &str, outcome: &SchemaFieldUpdate) -> JsonValue {
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

    async fn orch_get_config(&self, index: &str) -> Result<JsonValue, OrchestratorError> {
        // IMPORTANT: Always get fresh schema from storage layer
        // The storage layer maintains the authoritative schema derived from Tantivy
        // This prevents orchestrator cache staleness issues

        // If no shards are initialized yet, return a helpful error
        if self.shards.is_empty() {
            return Err(OrchestratorError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!(
                    "No shards initialized on this node. Schema for index '{}' may exist but cannot be retrieved until shards are created.",
                    index
                ),
            )));
        }

        tracing::debug!(
            index = %index,
            num_shards = self.shards.len(),
            "Attempting to retrieve schema from shards"
        );

        for (shard_id, shard) in &self.shards {
            if let Some(store) = &shard.store {
                let sc = Arc::clone(store);
                let idx = index.to_string();
                let sid = *shard_id;

                // Use spawn_blocking to safely call blocking storage function
                let schema = tokio::task::spawn_blocking(move || {
                    let result = sc.get_schema_cached(&idx);
                    tracing::debug!(
                        index = %idx,
                        shard_id = %sid,
                        found = result.as_ref().ok().and_then(|s| s.as_ref()).is_some(),
                        "Schema retrieval attempt"
                    );
                    result
                })
                .await
                .map_err(|e| OrchestratorError::Io(std::io::Error::other(e.to_string())))?
                .map_err(|e| OrchestratorError::Io(std::io::Error::other(e.to_string())))?;

                if let Some(s) = schema {
                    tracing::debug!(
                        index = %index,
                        shard_id = %shard_id,
                        num_fields = s.fields.len(),
                        "Schema found in shard"
                    );
                    let (searchable, sortable) = self.field_capabilities_across_shards(index).await;
                    return Ok(Self::schema_response(index, &s, &searchable, &sortable));
                }
            }
        }

        tracing::warn!(
            index = %index,
            num_shards = self.shards.len(),
            "Schema not found in any shard"
        );

        Err(OrchestratorError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("Schema for index '{}' not found", index),
        )))
    }

    /// Parse a query against an index on whichever local shard can answer.
    ///
    /// Resolving a field name needs a built Tantivy index, and a shard that holds the schema but
    /// has never been written to has none — so this asks each shard in turn and takes the first
    /// real verdict. Shards share a schema, so the first answer is every shard's answer.
    async fn orch_validate_query(
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

    /// List all available indexes (unified function for both local and cluster operations)
    async fn orch_list_indexes(
        &self,
        include_data_size: bool,
    ) -> Result<JsonValue, OrchestratorError> {
        // For now, cluster mode is handled at the RouterActor level
        // This function handles the local aggregation logic
        if self.shards.is_empty() {
            return Ok(serde_json::json!({
                "indexes": [],
                "total_indexes": 0,
                "node_id": self.identity.uuid.to_string(),
                "node_name": self.identity.name.clone(),
                "total_shards": 0
            }));
        }

        /// Per-index totals accumulated across this node's shards.
        #[derive(Default)]
        struct IndexTotals {
            document_count: u64,
            redb_bytes: u64,
            tantivy_bytes: u64,
            /// Shards that hold data for this index.
            shard_count: usize,
            /// Of those, how many have finished warming their reader.
            warm_shards: usize,
            /// Union of the fields the built index can actually search, across shards.
            ///
            /// A union rather than an intersection: a shard that has the column can answer a
            /// query on that field, and a scatter-gather asks every shard. Reporting the
            /// intersection would call a field unsearchable because one empty shard lacks it.
            searchable: HashSet<String>,
            /// Union of the fields the built index can sort exactly, across shards, on the same
            /// reasoning.
            sortable: HashSet<String>,
        }

        let mut all: HashMap<String, IndexTotals> = HashMap::new();

        // Create GetShardStats message
        let msg = GetShardStats { include_data_size };

        // Collect futures for all shard stats requests using actor message pattern
        let mut shard_futures = Vec::new();
        for (shard_id, shard) in &self.shards {
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
            match self.load_schema(&name).await {
                Ok(schema) => {
                    if let Some(description) = &schema.description {
                        json_obj.insert(
                            "description".to_string(),
                            JsonValue::String(description.clone()),
                        );
                    }
                    let fields = Self::describe_fields(&schema, &searchable, &sortable);
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
            "node_id": self.identity.uuid.to_string(),
            "node_name": self.identity.name.clone(),
            "total_shards": self.shards.len(),
            "took_ms": total_ms,
        }))
    }

    /// Get node identity information
    async fn orch_get_identity(&self) -> Result<JsonValue, OrchestratorError> {
        Ok(serde_json::json!({
            "node_id": self.identity.uuid.to_string(),
            "node_name": self.identity.name.clone(),
            "total_shards": self.shards.len()
        }))
    }

    /// Helper: Load schema from first shard
    async fn load_schema(&self, index: &str) -> Result<IndexSchema, OrchestratorError> {
        if let Some(cached) = self.get_cached_schema(index) {
            return Ok((*cached).clone());
        }

        if let Some(shard) = self.shards.values().next()
            && let Some(store) = &shard.store
        {
            let sc = Arc::clone(store);
            let idx = index.to_string();
            // IMPORTANT: Use get_schema_cached() instead of get_schema() to match
            // what the storage layer uses during writes. This ensures validation
            // uses the same Tantivy-derived schema as actual write operations.
            let schema = tokio::task::spawn_blocking(move || sc.get_schema_cached(&idx))
                .await
                .map_err(|e| OrchestratorError::Io(std::io::Error::other(e.to_string())))?
                .map_err(|e| OrchestratorError::Io(std::io::Error::other(e.to_string())))?;
            if let Some(schema_arc) = schema {
                let schema = (*schema_arc).clone();
                self.put_cached_schema(index, &schema);
                return Ok(schema);
            }
        }
        Ok(IndexSchema::default())
    }

    /// Helper: Route write to shard using deterministic key (no round-robin).
    fn route_write(&self, routing_key: &Option<String>) -> Result<Uuid, OrchestratorError> {
        let key = routing_key.as_ref().ok_or_else(|| {
            OrchestratorError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Missing routing key for write",
            ))
        })?;

        let target = self
            .select_shard_for_key(key)
            .or_else(|| self.first_shard_id());

        target.ok_or_else(|| {
            OrchestratorError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "No shard selected",
            ))
        })
    }
}

/// Derive a deterministic routing key from document content.
///
/// Preference order:
/// 1. If the document has an "id" field (string), use that directly.
/// 2. Otherwise, serialize the document to JSON bytes, take a prefix,
///    and hex-encode it to produce a stable routing key string.
fn derive_routing_key_from_doc(doc: &JsonValue) -> Option<String> {
    // Fallback: derive from JSON bytes (deterministic for same document)
    let mut bytes = serde_json::to_vec(doc).ok()?;
    if bytes.is_empty() {
        // Use a fixed token to remain deterministic for empty objects
        return Some("empty-doc".to_string());
    }

    // Limit the number of bytes used to keep the key reasonably sized
    const MAX_PREFIX_LEN: usize = 64;
    if bytes.len() > MAX_PREFIX_LEN {
        bytes.truncate(MAX_PREFIX_LEN);
    }

    // Hex-encode the prefix to a string key; ConsistentRing will hash it again
    let mut key = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut key, "{:02x}", b);
    }
    Some(key)
}

/// Message handler for GetShardCount
impl Message<GetShardCount> for NodeOrchestrator {
    type Reply = usize;

    async fn handle(
        &mut self,
        _msg: GetShardCount,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.shards.len()
    }
}

#[remote_message("cameo.orchestrator.client_op")]
impl Message<ClientOp> for NodeOrchestrator {
    type Reply = Result<JsonValue, OrchestratorError>;

    async fn handle(
        &mut self,
        msg: ClientOp,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        match msg {
            ClientOp::DeleteIndex {
                index,
                delete_schema,
            } => self.orch_delete_index(&index, delete_schema).await,
            _ => self.handle_client_op(msg).await,
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
        info!(
            ring_nodes = msg.ring.len(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// An engine with no shards and no coordinator. Enough to exercise `execute`'s dispatch
    /// table, which decides what the worker pool will and will not serve before any shard,
    /// schema or store is touched.
    fn bare_engine() -> OrchestratorEngine {
        OrchestratorEngine {
            shards: ArcSwap::from_pointee(HashMap::new()),
            routing_ring: Arc::new(ArcSwap::from_pointee(ConsistentRing::new())),
            schema_cache: Arc::new(ArcSwap::from_pointee(HashMap::new())),
            coordinator: None,
            identity: NodeIdentity::new(),
            default_search_limit: 10,
            max_concurrent_shard_searches: 4,
            remote_peer_pool: Arc::new(RemotePeerPool::new()),
        }
    }

    /// What a write is routed by, and in which order.
    ///
    /// The document's routing field outranks the caller's `routing_key`, and that precedence is
    /// the whole reason the shard-affine hint may not choose the shard: the hint is computed from
    /// `routing_key.or(id)` before any schema is in hand, so on an index that routes by a
    /// non-key field the two disagree. Taking the hint used to put such a document on a shard the
    /// ring did not own, and a later write of the same id through a hintless path put a second
    /// copy on the shard that did. The hint now picks only the worker; this pins the rule it used
    /// to be able to overrule.
    #[test]
    fn the_routing_key_comes_from_the_document_before_the_caller() {
        let mut schema = IndexSchema::default();
        schema.fields.insert(
            "tenant_id".to_string(),
            FieldDef::new("tenant_id".to_string(), TantivyFieldType::String),
        );
        schema
            .set_routing_field("tenant_id".to_string())
            .expect("the field exists");

        let doc = json!({"id": "d1", "tenant_id": "acme", "title": "Dune"});

        assert_eq!(
            effective_routing_key(&schema, "d1", Some("d1".to_string()), &doc).as_deref(),
            Some("acme"),
            "the schema's routing field decides, even against an explicit routing_key"
        );
        assert_eq!(
            effective_routing_key(&schema, "d1", None, &doc).as_deref(),
            Some("acme"),
            "and with no routing_key at all"
        );

        // A document that does not carry the routing field falls through the rest of the order.
        let bare = json!({"id": "d1", "title": "Dune"});
        assert_eq!(
            effective_routing_key(&schema, "d1", Some("caller".to_string()), &bare).as_deref(),
            Some("caller"),
            "the caller's key is honoured only where the document is silent"
        );
        assert_eq!(
            effective_routing_key(&schema, "d1", None, &bare).as_deref(),
            Some("d1"),
            "then the id"
        );
        assert!(
            effective_routing_key(&schema, "", None, &bare).is_some(),
            "and finally a hash of the document, so an unkeyed write is still deterministic"
        );

        // The default index routes by the key, which is what makes a delete-by-id unicast.
        let default_schema = IndexSchema::default();
        assert_eq!(
            effective_routing_key(&default_schema, "d1", None, &bare).as_deref(),
            Some("d1"),
            "routing_field defaults to `id`"
        );
    }

    /// A bulk delete entry is either a bare id or an object naming its routing key.
    ///
    /// Untagged enums resolve by trying each variant in order, which is exactly the kind of
    /// deserialization that works until someone reorders the variants. Both shapes and both
    /// accessors are pinned here, along with the rejection of an entry that is neither.
    #[test]
    fn a_delete_entry_reads_as_a_bare_id_or_as_an_object() {
        let parsed: Vec<DeletePayload> = serde_json::from_value(json!([
            "b1",
            {"id": "b2"},
            {"id": "b3", "routing_key": "acme"},
        ]))
        .expect("both shapes deserialize");

        let seen: Vec<(&str, Option<&str>)> = parsed
            .iter()
            .map(|entry| (entry.id(), entry.routing_key()))
            .collect();
        assert_eq!(
            seen,
            vec![("b1", None), ("b2", None), ("b3", Some("acme")),]
        );

        // A bare id round-trips as a bare id, so a forwarded batch reaches a peer in the shape
        // the caller sent.
        assert_eq!(
            serde_json::to_value(&parsed[0]).expect("serialize"),
            json!("b1")
        );

        assert!(
            serde_json::from_value::<Vec<DeletePayload>>(json!([{"document": "b1"}])).is_err(),
            "an entry that names no id is refused rather than read as something else"
        );
    }

    /// What a delete is routed by, and when it cannot be routed at all.
    /// What a delete is routed by, and when it cannot be routed at all.
    ///
    /// A write reads its key out of the document, which is the authority. A delete has no
    /// document, so the schema decides whether the id is enough: it is where the routing field is
    /// the key — `id`, or a shadow field whose value *is* the key — and it is not where the index
    /// routes by a tenant or a customer. The refusal in that case is deliberate; fanning out to
    /// every shard is correct and costs shards × nodes transactions to remove one row.
    #[test]
    fn a_delete_routes_by_the_id_unless_the_index_routes_by_something_else() {
        // The default: the routing field is the key, so the id is the key.
        let default_schema = IndexSchema::default();
        assert_eq!(
            effective_delete_routing_key(&default_schema, "d1", None).expect("routable"),
            "d1"
        );

        // A shadow index: the routing field is `sha1`, whose value is the document key, so the
        // id routes exactly as the original write did.
        let mut shadow = IndexSchema::default();
        shadow.add_shadow_field("sha1".to_string(), TantivyFieldType::String);
        shadow
            .set_routing_field("sha1".to_string())
            .expect("the field exists");
        assert!(shadow.is_shadow_field("sha1"));
        assert_eq!(
            effective_delete_routing_key(&shadow, "abc123", None).expect("routable"),
            "abc123"
        );

        // A tenant index: the id says nothing about which shard holds the row.
        let mut tenanted = IndexSchema::default();
        tenanted.fields.insert(
            "tenant_id".to_string(),
            FieldDef::new("tenant_id".to_string(), TantivyFieldType::String),
        );
        tenanted
            .set_routing_field("tenant_id".to_string())
            .expect("the field exists");

        let refused = effective_delete_routing_key(&tenanted, "d1", None)
            .expect_err("a keyless delete on a tenant index cannot be routed");
        let message = refused.to_string();
        assert!(
            message.contains("tenant_id"),
            "the refusal must name the field to supply: {message}"
        );
        assert!(
            message.contains("id:d1"),
            "and how to find its value: {message}"
        );
        assert!(
            matches!(
                &refused,
                OrchestratorError::Io(io) if io.kind() == std::io::ErrorKind::InvalidInput
            ),
            "InvalidInput is what `AppError::from_route` turns into a 400 rather than a 500"
        );

        // ...and with the key supplied it routes by that, on any index.
        assert_eq!(
            effective_delete_routing_key(&tenanted, "d1", Some("acme".to_string()))
                .expect("routable"),
            "acme"
        );
        assert_eq!(
            effective_delete_routing_key(&default_schema, "d1", Some("acme".to_string()))
                .expect("routable"),
            "acme"
        );
    }

    /// The engine cannot evolve a schema — it holds snapshots, not `&mut NodeOrchestrator`.
    /// What matters is that declining returns the *op*, not a sentinel error: the caller
    /// moved the op into the job and has nothing left to retry with otherwise. A write to an
    /// index with no schema used to reach the client as a 500.
    #[tokio::test]
    async fn an_op_the_engine_declines_comes_back_whole() {
        let engine = bare_engine();

        let outcome = engine
            .execute(ClientOp::BulkWrite {
                index: "books".to_string(),
                docs: vec![DocPayload {
                    id: "b1".to_string(),
                    routing_key: None,
                    doc: json!({"title": "Dune"}),
                }],
            })
            .await;

        match outcome {
            WorkerOutcome::UseActor(op) => match *op {
                ClientOp::BulkWrite { index, docs } => {
                    assert_eq!(index, "books");
                    assert_eq!(
                        docs.len(),
                        1,
                        "the documents have to survive the round trip"
                    );
                    assert_eq!(docs[0].doc, json!({"title": "Dune"}));
                }
                other => panic!("the op came back as a different op: {other:?}"),
            },
            WorkerOutcome::Done(result) => {
                panic!("bulk write should defer to the actor, got {result:?}")
            }
        }
    }

    /// A worker operation whose future is boxed so the runner closures below can be named in
    /// a return type. The loop never inspects the op, only how many are running.
    type TestOp = std::pin::Pin<Box<dyn std::future::Future<Output = WorkerOutcome> + Send>>;

    fn placeholder_op() -> Box<ClientOp> {
        Box::new(ClientOp::Write {
            index: "bench".to_string(),
            id: "d1".to_string(),
            routing_key: None,
            doc: json!({"title": "Dune"}),
        })
    }

    /// A runner that holds each operation for `hold` and records the high-water mark of how
    /// many were running at once. That mark is the whole subject of per-worker concurrency
    /// and is not observable from outside the loop any other way.
    fn recording_runner(
        hold: Duration,
        live: Arc<AtomicUsize>,
        peak: Arc<AtomicUsize>,
    ) -> impl Fn(Box<ClientOp>, Option<Uuid>) -> TestOp + Clone {
        move |_op, _shard| {
            let live = Arc::clone(&live);
            let peak = Arc::clone(&peak);
            Box::pin(async move {
                let now = live.fetch_add(1, AtomicOrdering::Relaxed) + 1;
                peak.fetch_max(now, AtomicOrdering::Relaxed);
                tokio::time::sleep(hold).await;
                live.fetch_sub(1, AtomicOrdering::Relaxed);
                WorkerOutcome::Done(Ok(json!({"ok": true})))
            })
        }
    }

    async fn submit(
        tx: &mpsc::Sender<OrchestratorJob>,
    ) -> tokio::sync::oneshot::Receiver<WorkerOutcome> {
        let (reply, answer) = tokio::sync::oneshot::channel();
        tx.send(OrchestratorJob::Execute {
            op: placeholder_op(),
            affinity_shard: None,
            reply,
        })
        .await
        .expect("the worker channel is open");
        answer
    }

    /// The point of the whole change: a worker carries several operations at once. The loop
    /// used to await `execute` inline, which pinned this peak at 1 however many jobs were
    /// queued — and that, not thread placement, is what made shard-affine dispatch a
    /// measured 13-20% write regression, because enabling it halves `worker_count`.
    #[tokio::test]
    async fn a_worker_runs_several_operations_at_once() {
        let (tx, rx) = mpsc::channel(16);
        let live = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        tokio::spawn(orchestrator_worker_loop(
            rx,
            recording_runner(
                Duration::from_millis(50),
                Arc::clone(&live),
                Arc::clone(&peak),
            ),
            0,
            None,
            4,
        ));

        let mut answers = Vec::new();
        for _ in 0..4 {
            answers.push(submit(&tx).await);
        }
        for answer in answers {
            answer.await.expect("every operation is answered");
        }

        assert_eq!(
            peak.load(AtomicOrdering::Relaxed),
            4,
            "four jobs and a width of four should have overlapped; a peak of 1 means the \
             loop went back to awaiting each operation inline"
        );
    }

    /// The other half of the contract. Width is an admission limit, not a suggestion: past
    /// the point where every shard writer already has work queued, more in-flight operations
    /// only move the queue from the channel into memory.
    #[tokio::test]
    async fn a_worker_never_exceeds_its_width() {
        let (tx, rx) = mpsc::channel(16);
        let live = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        tokio::spawn(orchestrator_worker_loop(
            rx,
            recording_runner(
                Duration::from_millis(20),
                Arc::clone(&live),
                Arc::clone(&peak),
            ),
            0,
            None,
            2,
        ));

        let mut answers = Vec::new();
        for _ in 0..8 {
            answers.push(submit(&tx).await);
        }
        for answer in answers {
            answer.await.expect("every operation is answered");
        }

        assert_eq!(
            peak.load(AtomicOrdering::Relaxed),
            2,
            "a width of two must never run three at once"
        );
    }

    /// Shutdown must answer what it already accepted.
    ///
    /// This mirrors the pinned path deliberately: there the loop is the argument to
    /// `block_on` on the worker's own `current_thread` runtime, so *returning* from it drops
    /// that runtime and cancels every task still on it. Operations run as spawned tasks now,
    /// so without the drain a shutdown mid-flight abandons accepted writes and hands their
    /// callers a dropped channel instead of an answer. A plain `#[tokio::test]` would not
    /// catch it — the test runtime outlives the loop and the tasks would finish anyway.
    #[test]
    fn shutdown_answers_operations_it_already_accepted() {
        let (tx, rx) = mpsc::channel::<OrchestratorJob>(16);

        // The first operation is quick and the rest are slow, so the loop is guaranteed to
        // read `Shutdown` — which needs a freed permit — while three are still running. With
        // one uniform duration the whole batch finishes together and the test proves nothing.
        let seq = Arc::new(AtomicUsize::new(0));
        let runner = move |_op: Box<ClientOp>, _shard: Option<Uuid>| -> TestOp {
            let nth = seq.fetch_add(1, AtomicOrdering::Relaxed);
            Box::pin(async move {
                let hold = if nth == 0 { 10 } else { 200 };
                tokio::time::sleep(Duration::from_millis(hold)).await;
                WorkerOutcome::Done(Ok(json!({"nth": nth})))
            })
        };

        let worker = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("worker runtime");
            rt.block_on(orchestrator_worker_loop(rx, runner, 0, None, 4));
        });

        let mut answers = Vec::new();
        for _ in 0..4 {
            let (reply, answer) = tokio::sync::oneshot::channel();
            tx.blocking_send(OrchestratorJob::Execute {
                op: placeholder_op(),
                affinity_shard: None,
                reply,
            })
            .expect("the worker channel is open");
            answers.push(answer);
        }
        tx.blocking_send(OrchestratorJob::Shutdown)
            .expect("the worker channel is open");
        drop(tx);

        worker.join().expect("the worker thread exits cleanly");

        for (nth, answer) in answers.into_iter().enumerate() {
            assert!(
                answer.blocking_recv().is_ok(),
                "operation {nth} was accepted and then abandoned at shutdown"
            );
        }
    }

    /// The defect that made shard-affine dispatch worth avoiding: `xxh3(shard) % workers`
    /// draws from a domain smaller than the pool, so most workers never see a write. With
    /// the shipped defaults — 4 shards, 8 workers — a measured run reached 3 of 8. Ordinals
    /// reach every worker up to the shard count, which is the real ceiling: one writer
    /// thread per shard serialises that shard's writes regardless.
    #[test]
    fn every_shard_gets_its_own_worker_until_the_pool_runs_out() {
        let mut placement = ShardPlacement::default();
        let layout = CoreLayout::detect();
        let shards: Vec<Uuid> = (0..4).map(|_| Uuid::new_v4()).collect();
        for &shard in &shards {
            placement.assign(shard, &layout, false);
        }

        let workers = 8;
        let assigned: HashSet<usize> = shards
            .iter()
            .map(|shard| placement.ordinal(shard).unwrap() % workers)
            .collect();

        assert_eq!(
            assigned.len(),
            shards.len(),
            "four shards must reach four distinct workers, not collide onto fewer"
        );
    }

    /// More shards than workers is the normal steady state; they have to wrap evenly rather
    /// than pile onto one worker.
    #[test]
    fn shards_past_the_worker_count_wrap_evenly() {
        let mut placement = ShardPlacement::default();
        let layout = CoreLayout::detect();
        let shards: Vec<Uuid> = (0..16).map(|_| Uuid::new_v4()).collect();
        for &shard in &shards {
            placement.assign(shard, &layout, false);
        }

        let workers = 8;
        let mut per_worker = vec![0usize; workers];
        for shard in &shards {
            per_worker[placement.ordinal(shard).unwrap() % workers] += 1;
        }

        assert!(
            per_worker.iter().all(|count| *count == 2),
            "16 shards over 8 workers should be 2 each, got {per_worker:?}"
        );
    }

    /// A writer thread pins itself using the ordinal it was given at startup and cannot be
    /// re-pinned afterwards. Assigning ordinals from a set that gets re-sorted on every
    /// membership change would strand those threads, so an ordinal is fixed for the process.
    #[test]
    fn an_ordinal_survives_later_shards_arriving() {
        let mut placement = ShardPlacement::default();
        let layout = CoreLayout::detect();
        let first = Uuid::new_v4();
        let ordinal = placement.assign(first, &layout, false).ordinal;

        for _ in 0..5 {
            placement.assign(Uuid::new_v4(), &layout, false);
        }
        // A shard that starts twice — a restarted actor, a repeated registration — keeps the
        // core its writer already pinned to.
        assert_eq!(placement.assign(first, &layout, false).ordinal, ordinal);
        assert_eq!(placement.ordinal(&first), Some(ordinal));
    }

    /// Local routing reads this to skip the coordinator, so a shard that is not ours must
    /// never look like one that is — including a shard that got an ordinal on its way to
    /// starting and then failed to hydrate. Routing writes at it would send them to a shard
    /// this node cannot serve, where the coordinator would have found the real owner.
    #[test]
    fn placement_claims_only_shards_that_actually_started() {
        let mut placement = ShardPlacement::default();
        let layout = CoreLayout::detect();
        let serving = Uuid::new_v4();
        let failed_to_hydrate = Uuid::new_v4();

        placement.assign(serving, &layout, false);
        placement.activate(serving);
        placement.assign(failed_to_hydrate, &layout, false);

        assert!(placement.is_local(&serving));
        assert!(
            !placement.is_local(&failed_to_hydrate),
            "an ordinal is not a claim; only a started shard is"
        );
        assert!(!placement.is_local(&Uuid::new_v4()));
        assert!(
            placement.ordinal(&failed_to_hydrate).is_some(),
            "the ordinal is still spent — reusing it would move a live writer's core"
        );
    }

    /// Asking for a core is not the same as getting one — `set_for_current` is a no-op on
    /// macOS and can be refused by a cpuset on Linux. The report used to show the request as
    /// though it were the result, which made it useless as evidence for exactly the thing it
    /// exists to show.
    #[test]
    fn a_requested_core_is_not_reported_as_a_taken_one() {
        let mut placement = ShardPlacement::default();
        let layout = CoreLayout::detect();
        let shard = Uuid::new_v4();

        let slot = placement.assign(shard, &layout, true);
        placement.activate(shard);

        let before = &placement.report()[0];
        assert_eq!(
            before.core_id, None,
            "nothing has pinned yet, so no core is taken"
        );
        if layout.pinning_available() {
            assert!(
                before.target_core_id.is_some(),
                "but one was requested, and that has to be visible too"
            );
        }

        // Stand in for the writer thread reporting success.
        slot.pinned_core.store(3, AtomicOrdering::Relaxed);

        let after = &placement.report()[0];
        assert_eq!(after.core_id, Some(3));
        assert!(after.serving);
    }

    /// A shard that never started still holds its ordinal, and the report has to say so
    /// rather than quietly omitting it — an unexplained gap in the ordinals is exactly the
    /// kind of thing an operator needs to see.
    #[test]
    fn the_report_shows_a_shard_that_holds_an_ordinal_without_serving() {
        let mut placement = ShardPlacement::default();
        let layout = CoreLayout::detect();
        let serving = Uuid::new_v4();
        let stalled = Uuid::new_v4();

        placement.assign(serving, &layout, true);
        placement.activate(serving);
        placement.assign(stalled, &layout, true);

        let report = placement.report();
        assert_eq!(report.len(), 2, "both shards appear");
        assert_eq!(report[0].ordinal, 0, "report is ordered by ordinal");
        assert_eq!(report[1].ordinal, 1);
        assert!(report[0].serving);
        assert!(!report[1].serving);
    }

    /// Pinning off means no core was requested, so nothing should imply one was.
    #[test]
    fn no_core_is_requested_when_pinning_is_off() {
        let mut placement = ShardPlacement::default();
        let shard = Uuid::new_v4();
        placement.assign(shard, &CoreLayout::detect(), false);

        let report = placement.report();
        assert_eq!(report[0].target_core_id, None);
        assert_eq!(report[0].core_id, None);
    }

    /// A quota-limited process sees more cores than it may use. Placing threads across cores
    /// the scheduler will not schedule spreads the work without spreading the CPU, and it is
    /// how worker and writer placement stopped agreeing in the first place.
    #[test]
    fn the_core_layout_never_exceeds_the_cpu_budget() {
        let layout = CoreLayout::detect();

        assert!(layout.budget() >= 1);
        assert!(
            layout.cores.len() <= layout.budget(),
            "pinnable cores ({}) must not exceed the budget ({})",
            layout.cores.len(),
            layout.budget()
        );
        if layout.pinning_available() {
            // Ordinals past the end wrap rather than falling off it.
            assert!(layout.core_for(layout.cores.len() * 3 + 1).is_some());
        } else {
            assert!(layout.core_for(0).is_none());
        }
    }

    fn payload(id: &str, doc: JsonValue) -> DocPayload {
        DocPayload {
            id: id.to_string(),
            routing_key: None,
            doc,
        }
    }

    /// A tantivy schema is fixed when the index is created, so initial creation is the only
    /// chance to make a field searchable. Sampling used to leave everything non-indexed,
    /// which produced write-only indexes: documents went in, and nothing but `id` could
    /// find them again.
    #[test]
    fn fields_inferred_when_the_index_is_created_are_searchable() {
        let schema = enhanced_schema_sampling(
            &[payload(
                "d1",
                json!({"id": "d1", "title": "hello", "year": 2024}),
            )],
            SCHEMA_SAMPLE_LIMIT,
        );

        for name in ["title", "year"] {
            let field = schema
                .fields
                .get(name)
                .unwrap_or_else(|| panic!("{name} should have been inferred"));
            assert!(field.indexed, "{name} has to be searchable");
            assert!(!field.stored, "only id belongs in tantivy's stored fields");
        }
    }

    /// Hits are rebuilt from redb, so storing values in tantivy too would keep a second copy
    /// of the corpus. Nothing inferred is stored — and `id` is not inferred at all:
    /// `evolve_field` refuses to touch it, and the storage layer seeds the canonical
    /// definition when it creates the index.
    #[test]
    fn nothing_inferred_is_stored_in_tantivy() {
        let schema = enhanced_schema_sampling(
            &[payload("d1", json!({"id": "d1", "title": "hello"}))],
            SCHEMA_SAMPLE_LIMIT,
        );

        assert!(
            !schema.fields.contains_key("id"),
            "id is seeded by the storage layer, not inferred"
        );
        assert!(
            schema.fields.values().all(|field| !field.stored),
            "an inferred field is indexed, never stored"
        );
    }

    /// Shadow fields exist to map a query written against the original field name onto the
    /// canonical `id`. Indexing one would put a second copy of the ids in the index.
    #[test]
    fn a_shadow_field_survives_initial_creation_untouched() {
        let mut schema = IndexSchema::default();
        schema.add_shadow_field("sha1".to_string(), TantivyFieldType::Text);
        schema.fields.insert(
            "title".to_string(),
            FieldDef::new_non_indexed("title".to_string(), &json!("hello")),
        );

        mark_initial_fields_indexed(&mut schema);

        let shadow = &schema.fields["sha1"];
        assert!(!shadow.indexed, "a shadow field is never indexed");
        assert!(!shadow.stored, "a shadow field is never stored");
        assert!(schema.fields["title"].indexed, "ordinary fields still are");
    }

    /// The identifier travels under the shadow name on the way out, so a projection naming `id`
    /// has to be rewritten to that name before it is applied. The rewrite is the only crossing
    /// point between the two names on read, and it has to hold for both directions of the
    /// mapping: `id` becomes the shadow name, the shadow name is left alone, and unrelated
    /// fields pass through untouched. On a plain index the rewrite is identity, so the helper
    /// is a no-op there.
    #[test]
    fn normalize_projection_fields_rewrites_id_to_the_shadow_name() {
        let mut shadow = IndexSchema::default();
        shadow.add_shadow_field("sha1".to_string(), TantivyFieldType::String);
        shadow.fields.insert(
            "title".to_string(),
            FieldDef::new("title".to_string(), TantivyFieldType::String),
        );

        assert_eq!(
            normalize_projection_fields(&shadow, &["id".to_string(), "title".to_string()]),
            vec!["sha1".to_string(), "title".to_string()],
            "`id` is rewritten to the shadow name; other fields are left alone"
        );
        assert_eq!(
            normalize_projection_fields(&shadow, &["sha1".to_string()]),
            vec!["sha1".to_string()],
            "the shadow name is already the name hits carry, so it is a no-op"
        );
        assert_eq!(
            normalize_projection_fields(&shadow, &[]),
            Vec::<String>::new(),
            "an empty projection stays empty"
        );

        let plain = IndexSchema::default();
        assert_eq!(
            normalize_projection_fields(&plain, &["id".to_string(), "title".to_string()]),
            vec!["id".to_string(), "title".to_string()],
            "on a plain index the key is `id`, so the rewrite is identity"
        );
    }

    /// Metadata ops are never dispatched to the pool today, but if one ever is, it must be
    /// handed to the actor rather than answered with an error — the same contract.
    #[tokio::test]
    async fn a_metadata_op_defers_rather_than_failing() {
        let engine = bare_engine();

        let outcome = engine
            .execute(ClientOp::GetConfig {
                index: "books".to_string(),
            })
            .await;

        assert!(
            matches!(outcome, WorkerOutcome::UseActor(op) if matches!(*op, ClientOp::GetConfig { .. })),
            "a metadata op must be deferred to the actor, carrying its own op"
        );
    }

    #[test]
    fn test_apply_field_projection_single_field() {
        let doc = json!({
            "title": "Rust Programming",
            "author": "John Doe",
            "year": 2024,
            "_score": 0.95,
            "_id": "abc123"
        });

        let fields = vec!["title".to_string()];
        let result = apply_field_projection(doc, &fields);

        // Should only have title and metadata fields (those starting with _)
        assert_eq!(result.get("title").unwrap(), "Rust Programming");
        assert_eq!(result.get("_score").unwrap(), 0.95);
        assert_eq!(result.get("_id").unwrap(), "abc123");
        assert!(result.get("author").is_none());
        assert!(result.get("year").is_none());
    }

    #[test]
    fn test_apply_field_projection_multiple_fields() {
        let doc = json!({
            "title": "Rust Programming",
            "author": "John Doe",
            "year": 2024,
            "isbn": "123-456",
            "_score": 0.95
        });

        let fields = vec!["title".to_string(), "author".to_string()];
        let result = apply_field_projection(doc, &fields);

        assert_eq!(result.get("title").unwrap(), "Rust Programming");
        assert_eq!(result.get("author").unwrap(), "John Doe");
        assert_eq!(result.get("_score").unwrap(), 0.95);
        assert!(result.get("year").is_none());
        assert!(result.get("isbn").is_none());
    }

    #[test]
    fn test_apply_field_projection_preserves_all_metadata() {
        let doc = json!({
            "title": "Rust Programming",
            "author": "John Doe",
            "_score": 0.95,
            "_id": "doc123",
            "_timestamp": 1234567890,
            "_shard_id": "abc123"
        });

        let fields = vec!["title".to_string()];
        let result = apply_field_projection(doc, &fields);

        // All metadata fields (starting with _) should be preserved
        assert_eq!(result.get("title").unwrap(), "Rust Programming");
        assert_eq!(result.get("_score").unwrap(), 0.95);
        assert_eq!(result.get("_id").unwrap(), "doc123");
        assert_eq!(result.get("_timestamp").unwrap(), 1234567890);
        assert_eq!(result.get("_shard_id").unwrap(), "abc123");
        assert!(result.get("author").is_none());
    }

    #[test]
    fn test_apply_field_projection_nonexistent_field() {
        let doc = json!({
            "title": "Rust Programming",
            "author": "John Doe",
            "_score": 0.95
        });

        let fields = vec!["title".to_string(), "nonexistent".to_string()];
        let result = apply_field_projection(doc, &fields);

        // Should have title and metadata, but not nonexistent field
        assert_eq!(result.get("title").unwrap(), "Rust Programming");
        assert_eq!(result.get("_score").unwrap(), 0.95);
        assert!(result.get("nonexistent").is_none());
        assert!(result.get("author").is_none());
    }

    #[test]
    fn test_apply_field_projection_empty_fields() {
        let doc = json!({
            "title": "Rust Programming",
            "author": "John Doe",
            "_score": 0.95
        });

        let fields: Vec<String> = vec![];
        let result = apply_field_projection(doc, &fields);

        // Should only have metadata fields
        assert_eq!(result.get("_score").unwrap(), 0.95);
        assert!(result.get("title").is_none());
        assert!(result.get("author").is_none());
    }

    #[test]
    fn test_apply_field_projection_non_object() {
        let doc = json!("not an object");
        let fields = vec!["title".to_string()];
        let result = apply_field_projection(doc, &fields);

        // Should return the original value unchanged
        assert_eq!(result, json!("not an object"));
    }

    #[test]
    fn test_apply_field_projection_nested_fields() {
        let doc = json!({
            "title": "Rust Programming",
            "author": {
                "name": "John Doe",
                "email": "john@example.com"
            },
            "_score": 0.95
        });

        let fields = vec!["author".to_string()];
        let result = apply_field_projection(doc, &fields);

        // Should preserve the entire nested object
        assert_eq!(
            result.get("author").unwrap().get("name").unwrap(),
            "John Doe"
        );
        assert_eq!(
            result.get("author").unwrap().get("email").unwrap(),
            "john@example.com"
        );
        assert_eq!(result.get("_score").unwrap(), 0.95);
        assert!(result.get("title").is_none());
    }

    #[test]
    fn test_apply_field_projection_preserves_user_order() {
        let doc = json!({
            "id": "doc1",
            "title": "Rust Programming",
            "author": "John Doe",
            "year": 2024,
            "_score": 0.95
        });

        let fields = vec![
            "year".to_string(),
            "title".to_string(),
            "author".to_string(),
        ];
        let result = apply_field_projection(doc, &fields);

        // User fields should appear first, in projection order
        let keys: Vec<&str> = result
            .as_object()
            .unwrap()
            .keys()
            .map(|k| k.as_str())
            .collect();
        assert_eq!(keys, vec!["year", "title", "author", "_score"]);
    }

    #[test]
    fn test_apply_field_projection_order_with_sort_key() {
        // Simulate a document after stamp_sort_keys + _score insertion
        let doc = json!({
            "id": "doc1",
            "title": "Rust Programming",
            "author": "John Doe",
            "year": 2024,
            "_sort_key": 2024,
            "_score": 1.0,
            "shard_id": "abc-123"
        });

        let fields = vec![
            "year".to_string(),
            "title".to_string(),
            "author".to_string(),
        ];
        let mut result = apply_field_projection(doc, &fields);

        // Strip _sort_key as route_and_handle would
        if let Some(o) = result.as_object_mut() {
            o.remove(SORT_KEY_FIELD);
        }

        // After stripping, order should be: user fields (in projection order), then _score
        let keys: Vec<&str> = result
            .as_object()
            .unwrap()
            .keys()
            .map(|k| k.as_str())
            .collect();
        assert_eq!(keys, vec!["year", "title", "author", "_score"]);
    }

    #[test]
    fn test_apply_field_projection_order_without_sort_key() {
        // Same document but without _sort_key (no sort applied)
        let doc = json!({
            "id": "doc1",
            "title": "Rust Programming",
            "author": "John Doe",
            "year": 2024,
            "_score": 1.0,
            "shard_id": "abc-123"
        });

        let fields = vec![
            "year".to_string(),
            "title".to_string(),
            "author".to_string(),
        ];
        let result = apply_field_projection(doc, &fields);

        // Order should be: user fields (in projection order), then _score
        let keys: Vec<&str> = result
            .as_object()
            .unwrap()
            .keys()
            .map(|k| k.as_str())
            .collect();
        assert_eq!(keys, vec!["year", "title", "author", "_score"]);
    }

    // ---- Field-sort merge helpers (`_sort_key`) ----

    fn titles(hits: &[JsonValue]) -> Vec<String> {
        hits.iter()
            .map(|h| h["title"].as_str().unwrap_or_default().to_string())
            .collect()
    }

    /// Even when the sort field itself is projected away, the `_sort_key` metadata lets a
    /// cross-node merge interleave per-node blocks correctly.
    #[test]
    fn a_merge_interleaves_nodes_by_sort_key_without_the_sort_field() {
        let spec = SortSpec {
            field: "year".to_string(),
            order: SortOrder::Desc,
        };
        // Two nodes, each already field-sorted and projected (no `year`).
        let hits = order_hit_blocks(
            vec![
                vec![
                    json!({"title": "a", "_sort_key": 2020}),
                    json!({"title": "c", "_sort_key": 2018}),
                ],
                vec![
                    json!({"title": "d", "_sort_key": 2024}),
                    json!({"title": "b", "_sort_key": 2022}),
                ],
            ],
            Some(&spec),
            SearchWindow::first(10),
        );
        assert_eq!(titles(&hits), vec!["d", "b", "a", "c"]);
    }

    #[test]
    fn a_merge_sorts_ascending_and_puts_a_missing_key_last() {
        let spec = SortSpec {
            field: "year".to_string(),
            order: SortOrder::Asc,
        };
        let hits = order_hit_blocks(
            vec![
                vec![
                    json!({"title": "b", "_sort_key": 2022}),
                    json!({"title": "missing"}), // no `_sort_key` → sorts last
                ],
                vec![json!({"title": "a", "_sort_key": 2018})],
            ],
            Some(&spec),
            SearchWindow::first(10),
        );
        assert_eq!(titles(&hits), vec!["a", "b", "missing"]);
    }

    /// Ties fall back to the source's rank and then to the hit's place within that source.
    ///
    /// Sources are polled concurrently and answer in whatever order they finish, so without
    /// this a tie is settled by whoever replied first and one query has two answers. Every
    /// hit here has the same sort key, which is the ordinary case rather than a contrived
    /// one: every document matching a single term scores identically.
    #[test]
    fn a_tie_falls_back_to_source_rank_and_then_to_position() {
        let spec = SortSpec {
            field: "year".to_string(),
            order: SortOrder::Desc,
        };
        let blocks = vec![
            vec![
                json!({"title": "a1", "_sort_key": 2020}),
                json!({"title": "a2", "_sort_key": 2020}),
            ],
            vec![
                json!({"title": "b1", "_sort_key": 2020}),
                json!({"title": "b2", "_sort_key": 2020}),
            ],
        ];
        let hits = order_hit_blocks(blocks.clone(), Some(&spec), SearchWindow::first(10));
        assert_eq!(titles(&hits), vec!["a1", "a2", "b1", "b2"]);

        // The same blocks in the other dispatch order give the other answer, and that is the
        // point: the order is the caller's, not the network's.
        let swapped = vec![blocks[1].clone(), blocks[0].clone()];
        let hits = order_hit_blocks(swapped, Some(&spec), SearchWindow::first(10));
        assert_eq!(titles(&hits), vec!["b1", "b2", "a1", "a2"]);
    }

    /// A truncated page is the prefix of an untruncated one.
    ///
    /// The property paging will rest on, and the one a running top-K could not provide: it had
    /// to decide what to discard while later sources were still unheard, so which of a tied run
    /// it kept depended on arrival order.
    #[test]
    fn a_limited_merge_is_a_prefix_of_the_unlimited_one() {
        let blocks = vec![
            vec![
                json!({"title": "a1", "_score": 1.0}),
                json!({"title": "a2", "_score": 1.0}),
            ],
            vec![
                json!({"title": "b1", "_score": 1.0}),
                json!({"title": "b2", "_score": 2.0}),
            ],
        ];
        let full = titles(&order_hit_blocks(
            blocks.clone(),
            None,
            SearchWindow::first(10),
        ));
        for limit in 1..=full.len() {
            let page = titles(&order_hit_blocks(
                blocks.clone(),
                None,
                SearchWindow::first(limit),
            ));
            assert_eq!(page, full[..limit], "limit {limit} is not a prefix");
        }
    }

    /// `SearchWindow::fetch_count` is `offset + limit` — the number each source must return
    /// for the window to be servable after a merge.
    #[test]
    fn search_window_fetch_count_is_offset_plus_limit() {
        assert_eq!(SearchWindow::first(10).fetch_count(), 10);
        assert_eq!(
            SearchWindow {
                offset: 30,
                limit: 10
            }
            .fetch_count(),
            40
        );
        // Saturating: a huge offset does not overflow.
        assert_eq!(
            SearchWindow {
                offset: usize::MAX,
                limit: 1
            }
            .fetch_count(),
            usize::MAX
        );
    }

    /// `SearchWindow::apply` takes the slice `[offset, offset+limit)` from an ordered vec.
    #[test]
    fn search_window_apply_takes_the_right_slice() {
        let hits: Vec<usize> = (0..100).collect();
        let page = SearchWindow {
            offset: 30,
            limit: 10,
        }
        .apply(hits);
        assert_eq!(page, (30..40usize).collect::<Vec<_>>());
    }

    /// `apply` clamps to what is available rather than panicking.
    #[test]
    fn search_window_apply_clamps_to_available() {
        let hits: Vec<usize> = vec![1, 2, 3];
        assert_eq!(
            SearchWindow {
                offset: 0,
                limit: 10
            }
            .apply(hits.clone()),
            vec![1usize, 2, 3]
        );
        assert_eq!(
            SearchWindow {
                offset: 2,
                limit: 10
            }
            .apply(hits.clone()),
            vec![3usize]
        );
        assert_eq!(
            SearchWindow {
                offset: 5,
                limit: 10
            }
            .apply(hits),
            Vec::<usize>::new()
        );
    }

    /// A paged merge is the middle of an untruncated one, not a re-run of it.
    ///
    /// `order_hit_blocks` with a window of `offset=2, limit=2` returns the third and fourth
    /// hits of the full order, which is what a caller paging through results expects.
    #[test]
    fn a_paged_merge_returns_the_right_slice_of_the_full_order() {
        let blocks = vec![
            vec![
                json!({"title": "a1", "_score": 1.0}),
                json!({"title": "a2", "_score": 1.0}),
            ],
            vec![
                json!({"title": "b1", "_score": 1.0}),
                json!({"title": "b2", "_score": 2.0}),
            ],
        ];
        let full = titles(&order_hit_blocks(
            blocks.clone(),
            None,
            SearchWindow::first(10),
        ));
        let page = titles(&order_hit_blocks(
            blocks,
            None,
            SearchWindow {
                offset: 2,
                limit: 2,
            },
        ));
        assert_eq!(page, full[2..4].to_vec());
    }

    /// Finding #2: i64 keys beyond f64's exact-integer range must order precisely.
    #[test]
    fn compare_hits_by_field_distinguishes_large_i64_keys() {
        use std::cmp::Ordering;
        let big = 9_007_199_254_740_992i64; // 2^53
        let bigger = big + 1; // not representable distinctly as f64
        let a = json!({ "_sort_key": bigger });
        let b = json!({ "_sort_key": big });
        assert_eq!(
            compare_hits_by_field(&a, &b, "_sort_key", SortOrder::Asc),
            Ordering::Greater
        );
    }

    /// Finding #3: date sort keys are normalized to epoch seconds so ordering is
    /// chronological rather than lexicographic.
    #[test]
    fn normalize_sort_key_converts_dates_to_epoch_seconds() {
        let date_def = FieldDef::new("published".to_string(), TantivyFieldType::Date);

        let early = normalize_sort_key(&json!("2018-11-30"), Some(&date_def)).unwrap();
        let late = normalize_sort_key(&json!("2024-03-10T00:00:00Z"), Some(&date_def)).unwrap();

        assert!(early.is_i64(), "date key should be numeric, got {early:?}");
        assert!(
            early.as_i64().unwrap() < late.as_i64().unwrap(),
            "chronological order must hold numerically"
        );

        // Unparseable date → no key (hit will sort last).
        assert!(normalize_sort_key(&json!("not-a-date"), Some(&date_def)).is_none());
    }

    #[test]
    fn normalize_sort_key_passes_through_non_date_values() {
        let numeric_def = FieldDef::new("year".to_string(), TantivyFieldType::I64);
        assert_eq!(
            normalize_sort_key(&json!(2020), Some(&numeric_def)).unwrap(),
            json!(2020)
        );
        // No schema entry → passthrough.
        assert_eq!(
            normalize_sort_key(&json!("hello"), None).unwrap(),
            json!("hello")
        );
    }

    #[test]
    fn stamp_sort_keys_injects_normalized_date_key() {
        let mut schema = IndexSchema::default();
        schema.fields.insert(
            "published".to_string(),
            FieldDef::new("published".to_string(), TantivyFieldType::Date),
        );
        let spec = SortSpec {
            field: "published".to_string(),
            order: SortOrder::Asc,
        };
        let mut hits = vec![(
            Uuid::nil(),
            1.0f32,
            json!({"title": "x", "published": "2020-06-01"}),
        )];
        stamp_sort_keys(&mut hits, &spec, &schema);
        let key = hits[0].2.get(SORT_KEY_FIELD).expect("sort key stamped");
        assert!(key.is_i64());
    }

    #[test]
    fn strip_sort_keys_removes_only_the_metadata_key() {
        let mut response = json!({
            "hits": [
                {"title": "a", "_score": 1.0, "_sort_key": 2020},
                {"title": "b", "_score": 0.9, "_sort_key": 2018},
            ],
            "hits_returned": 2
        });
        strip_sort_keys(&mut response);
        let hits = response["hits"].as_array().unwrap();
        for hit in hits {
            assert!(
                hit.get(SORT_KEY_FIELD).is_none(),
                "_sort_key must be stripped"
            );
            assert!(hit.get("_score").is_some(), "other metadata preserved");
            assert!(hit.get("title").is_some(), "content preserved");
        }
    }

    /// A schema record that still declares `_seq` does not make it sortable.
    ///
    /// Only reachable as a unit test: the field is retired, so no index created now records it,
    /// and every index created before the retirement still does. The engine refuses it in each
    /// shard either way — so a guard that decides by looking the name up in the schema passes it
    /// through on exactly the indexes that have the field, which is every index that predates
    /// the change and none of the ones a test creates.
    #[test]
    fn a_retired_seq_column_in_the_schema_record_does_not_make_it_sortable() {
        let mut schema = IndexSchema::default();
        for (name, field_type) in [
            ("rank", TantivyFieldType::U64),
            ("title", TantivyFieldType::Text),
            ("flag", TantivyFieldType::Boolean),
            ("_seq", TantivyFieldType::U64),
        ] {
            schema.fields.insert(
                name.to_string(),
                FieldDef::new(name.to_string(), field_type),
            );
        }
        schema.fields.insert("doi".to_string(), shadow_field("doi"));
        schema.rebuild_shadow_fields_cache();

        let refused = |field: &str| {
            unsortable_sort_field(
                &schema,
                Some(&SortSpec {
                    field: field.to_string(),
                    order: SortOrder::Asc,
                }),
            )
        };

        // `_seq` is a fast u64 sitting in the record, so nothing about the declaration itself
        // distinguishes it from `rank`. It is refused by name.
        assert!(
            refused("_seq").is_some(),
            "a legacy _seq column must not be sortable"
        );
        // In the schema, no fast column, not text: the engine refuses this one too.
        assert!(
            refused("flag").is_some(),
            "a non-text field with no fast column must not be sortable"
        );
        assert!(
            refused("no_such_field").is_some(),
            "a name absent from the schema must not be sortable"
        );

        for field in ["rank", "title", "id", "doi"] {
            assert!(
                refused(field).is_none(),
                "'{field}' must still sort: a fast column, a text field, the key, and the key's \
                 shadow name"
            );
        }
    }

    /// A declaration cannot make a column exist, and the refusal says which case it is.
    ///
    /// The guard reads the declaration, and for these five types the index builder never reads it
    /// back — `add_bool_field`, `add_bytes_field`, `add_ip_addr_field`, `add_json_field` and
    /// `add_facet_field` take no `fast`. So a declared `fast: true` passed the guard and was then
    /// refused by every shard, which a scatter-gather reports as `200` with an empty page. Refused
    /// here instead, with a reason that does not send the caller off to declare a flag that
    /// changes nothing.
    #[test]
    fn a_declared_fast_on_a_type_with_no_column_is_still_refused() {
        let mut schema = IndexSchema::default();
        for (name, field_type) in [
            ("flag", TantivyFieldType::Boolean),
            ("addr", TantivyFieldType::Ip),
            ("blob", TantivyFieldType::Json),
            ("category", TantivyFieldType::Facet),
            ("raw", TantivyFieldType::Bytes),
            ("rank", TantivyFieldType::U64),
        ] {
            let mut def = FieldDef::new(name.to_string(), field_type);
            def.fast = Some(true);
            schema.fields.insert(name.to_string(), def);
        }

        let refusal = |field: &str| {
            unsortable_sort_field(
                &schema,
                Some(&SortSpec {
                    field: field.to_string(),
                    order: SortOrder::Asc,
                }),
            )
        };

        for field in ["flag", "addr", "blob", "category", "raw"] {
            let Some(OrchestratorError::UnsortableField { reason, .. }) = refusal(field) else {
                panic!("'{field}' declares a column its type cannot carry and must be refused");
            };
            assert!(
                reason.contains("cannot give it one"),
                "the reason must not ask for a declaration that already exists: {reason}"
            );
        }

        assert!(
            refusal("rank").is_none(),
            "a u64 field declared fast does carry a column and must still sort"
        );
    }

    /// A gather with no successful shard refuses, and carries whose fault it was.
    ///
    /// The verdict decides the status — 400 for a request the caller can fix, 500 for a node that
    /// could not read its own data — and it cannot be recovered from the message text, which is
    /// how "field not found: added" used to be classified as a missing resource and answered 404.
    #[test]
    fn a_gather_that_answered_nowhere_refuses_and_says_whose_fault_it_was() {
        let shard = Uuid::nil();
        let refused = |kind, text: &str| {
            (
                shard,
                OrchestratorError::Io(std::io::Error::new(kind, text.to_string())),
            )
        };

        // One shard answering makes the rest a partial outage, which is reported as hits plus
        // `errors` rather than as a refusal.
        assert!(
            no_shard_answered(
                "papers",
                1,
                &[refused(
                    std::io::ErrorKind::InvalidInput,
                    "field not found: x"
                )]
            )
            .is_none(),
            "a partial answer is still an answer"
        );
        assert!(
            no_shard_answered("papers", 0, &[]).is_none(),
            "no shards and no failures is an empty index, not a refusal"
        );

        // Every shard runs the same query against the same schema, so they fail identically —
        // and one reason repeated per shard would read as several distinct problems.
        let Some(OrchestratorError::NoShardAnswered {
            index,
            reasons,
            caller_error,
        }) = no_shard_answered(
            "papers",
            0,
            &[
                refused(std::io::ErrorKind::InvalidInput, "field not found: added"),
                refused(std::io::ErrorKind::InvalidInput, "field not found: added"),
            ],
        )
        else {
            panic!("a gather no shard could answer must refuse");
        };
        assert_eq!(index, "papers");
        assert_eq!(reasons, "field not found: added");
        assert!(
            caller_error,
            "a field the index has no column for is the request's to fix"
        );

        // One node-level failure is enough to stop calling it the caller's fault: the request may
        // have been perfectly good and unreadable data is not an answer about it.
        let Some(OrchestratorError::NoShardAnswered { caller_error, .. }) = no_shard_answered(
            "papers",
            0,
            &[
                refused(std::io::ErrorKind::InvalidInput, "field not found: added"),
                refused(std::io::ErrorKind::PermissionDenied, "cannot open index"),
            ],
        ) else {
            panic!("still a refusal");
        };
        assert!(
            !caller_error,
            "a shard that could not read its data is this node's problem"
        );
    }

    /// A shadow field as the schema records one: the caller's name for the key, carrying no
    /// column of its own.
    fn shadow_field(name: &str) -> FieldDef {
        let mut def = FieldDef::new(name.to_string(), TantivyFieldType::Text);
        def.indexed = false;
        def.is_shadow = true;
        def
    }

    /// A peer numbers its reasons against the batch it received, not the one the caller sent.
    #[test]
    fn a_peers_reasons_are_renumbered_into_this_batch() {
        let node = Uuid::nil();
        // Positions 4 and 9 of the caller's batch were the ones forwarded.
        let reasons = vec!["document 1: bad field".to_string()];
        assert_eq!(
            remote_rejections(node, &[4, 9], 1, &reasons),
            vec!["document 9: bad field".to_string()],
            "the peer's document 1 is the caller's document 9"
        );
    }

    /// A reason that is not about one document still stands for a document that was not written.
    #[test]
    fn a_peers_batch_level_reason_is_attributed_rather_than_dropped() {
        let node = Uuid::nil();
        let reasons = vec!["index is read-only".to_string()];
        assert_eq!(
            remote_rejections(node, &[0, 1], 1, &reasons),
            vec![format!("node {node}: index is read-only")]
        );
    }

    /// A position from someone else's batch is worse than none: it names a document that is fine.
    #[test]
    fn a_position_outside_this_batch_is_not_taken_as_one() {
        let node = Uuid::nil();
        let reasons = vec!["document 7: bad field".to_string()];
        let rejections = remote_rejections(node, &[0, 1], 1, &reasons);
        assert_eq!(rejections.len(), 1);
        assert!(
            rejections[0].starts_with(&format!("node {node}:")),
            "it is kept as what the node said, not renumbered onto a document: {rejections:?}"
        );
    }

    /// A peer whose numbers do not add up leaves a shortfall, and the shortfall is stated.
    #[test]
    fn a_shortfall_a_peer_did_not_explain_is_still_reported() {
        let node = Uuid::nil();
        // Three forwarded, one written, and the node gave only one reason for the other two.
        let reasons = vec!["document 0: bad field".to_string()];
        let rejections = remote_rejections(node, &[10, 11, 12], 1, &reasons);
        assert_eq!(
            rejections.len(),
            2,
            "one reason per document not written: {rejections:?}"
        );
        assert_eq!(rejections[0], "document 10: bad field");
        assert!(
            rejections[1].contains("neither written nor refused"),
            "and the one it did not explain says so: {rejections:?}"
        );
    }

    /// A peer that wrote everything leaves nothing to report, whatever it said.
    #[test]
    fn nothing_is_reported_for_a_batch_the_peer_wrote_whole() {
        let node = Uuid::nil();
        assert!(remote_rejections(node, &[0, 1, 2], 3, &[]).is_empty());
        // More reasons than documents unwritten cannot inflate the count either.
        assert!(
            remote_rejections(node, &[0], 1, &["document 0: stale".to_string()]).is_empty(),
            "a reason for a document the node also says it wrote is not an extra rejection"
        );
    }

    #[test]
    fn a_document_reason_is_split_only_when_that_is_what_it_is() {
        assert_eq!(
            split_document_reason("document 12: because"),
            Some((12, "because"))
        );
        assert_eq!(split_document_reason("documents 12: because"), None);
        assert_eq!(split_document_reason("document twelve: because"), None);
        assert_eq!(split_document_reason("index is read-only"), None);
    }
}
