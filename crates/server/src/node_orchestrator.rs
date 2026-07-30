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
use std::collections::{HashMap, HashSet};
use std::fs;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{
    Arc,
    atomic::{AtomicU64, AtomicUsize, Ordering as AtomicOrdering},
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
use crate::remote_peer_pool::{ConnectionChannel, RemotePeerPool};

// Re-export SortSpec and SortOrder from storage crate
use chrono::{NaiveDate, NaiveDateTime};
use cluster::{ConsistentRing, IdentityError, NodeIdentity, generate_tokens};
use serde_json::{Map as JsonMap, Value as JsonValue};
use storage::{
    FieldDef, HybridStore, IndexSchema, ShardStatsTimings, StorageConfig, StoreError,
    TantivyFieldType, WalOp,
};
pub use storage::{SortOrder, SortSpec};
use xxhash_rust::xxh3::xxh3_64;

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

/// Type alias for routing results to reduce complexity
type RoutingResult = Result<(DocPayload, Option<String>, Option<Uuid>), OrchestratorError>;

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

/// Extract unique field names from a batch of documents (lightweight operation)
fn extract_field_names(docs: &[DocPayload]) -> HashSet<String> {
    let mut field_names = HashSet::new();
    for doc in docs {
        if let Some(obj) = doc.doc.as_object() {
            field_names.extend(obj.keys().cloned());
        }
    }
    field_names
}

/// Calculate fingerprint from sorted field names using xxh3_64 one-shot API
fn calculate_batch_fingerprint(field_names: &HashSet<String>) -> u64 {
    let mut sorted_names: Vec<&String> = field_names.iter().collect();
    sorted_names.sort();
    let mut combined = Vec::new();
    for name in &sorted_names {
        combined.extend_from_slice(name.as_bytes());
    }
    xxh3_64(&combined)
}

/// Calculate fingerprint directly from a JSON document's keys.
/// Avoids the intermediate `HashSet<String>` allocation used by
/// `extract_field_names` + `calculate_batch_fingerprint`.
fn calculate_doc_fingerprint(doc: &JsonValue) -> u64 {
    let obj = match doc.as_object() {
        Some(o) => o,
        None => return 0,
    };
    let mut keys: Vec<&str> = obj.keys().map(|k| k.as_str()).collect();
    keys.sort_unstable();
    let mut combined = Vec::with_capacity(keys.len() * 8);
    for key in keys {
        combined.extend_from_slice(key.as_bytes());
    }
    xxh3_64(&combined)
}

/// Transform search query to map shadow fields to canonical "id" field
///
/// This function replaces shadow field references in queries with "id" field references
/// to enable searching by original field names while using the efficient canonical ID index.
///
/// Example transformations:
/// - {"term": {"book_id": "book_123"}} → {"term": {"id": "book_123"}}
/// - {"term": {"sha256": "abc123"}} → {"term": {"id": "abc123"}}
///
/// The transformation preserves the query structure while ensuring all shadow field searches
/// use the optimized "id" field index in Tantivy.
fn transform_shadow_query(query: &str, schema: &IndexSchema) -> String {
    let shadow_mapping = schema.get_shadow_mapping();

    if shadow_mapping.is_empty() {
        // No shadow fields, return original query
        return query.to_string();
    }

    // Parse the query as JSON to transform field names
    if let Ok(mut query_json) = serde_json::from_str::<JsonValue>(query) {
        transform_shadow_fields_recursive(&mut query_json, &shadow_mapping);
        serde_json::to_string(&query_json).unwrap_or_else(|_| query.to_string())
    } else {
        // If parsing fails, return original query
        query.to_string()
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
}

fn push_hit_into_top_k(top_hits: &mut Vec<JsonValue>, hit: JsonValue, limit: usize) {
    if limit == 0 {
        return;
    }

    let new_score = hit_score(&hit);
    if top_hits.len() < limit {
        top_hits.push(hit);
        top_hits.sort_by(|a, b| {
            hit_score(b)
                .partial_cmp(&hit_score(a))
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        return;
    }

    let worst_score = top_hits.last().map(hit_score).unwrap_or(f64::NEG_INFINITY);
    if new_score <= worst_score {
        return;
    }

    top_hits.push(hit);
    top_hits.sort_by(|a, b| {
        hit_score(b)
            .partial_cmp(&hit_score(a))
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    top_hits.truncate(limit);
}

/// Metadata field carrying the normalized sort value of a hit.
///
/// Injected by the shard-gather search paths (`engine_search` / `orch_search`) before
/// field projection runs, and consumed by every merge layer. Because it is `_`-prefixed
/// it survives `apply_field_projection` automatically, so cross-node merges can order
/// results even when the user's `return` projection excludes the sort field itself. It
/// is stripped from every hit at the client boundary (`route_and_handle`).
const SORT_KEY_FIELD: &str = "_sort_key";

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
fn stamp_sort_keys(hits: &mut [(Uuid, f32, JsonValue)], spec: &SortSpec, schema: &IndexSchema) {
    let field_def = schema.fields.get(&spec.field);
    for (_, _, doc) in hits.iter_mut() {
        if let JsonValue::Object(o) = doc
            && let Some(raw) = o.get(&spec.field)
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

/// Order merged search hits in place: by the injected `SORT_KEY_FIELD` when an explicit
/// sort is requested, otherwise by relevance score (descending). The sort field itself
/// may have been projected away, so the merge always keys on the metadata field, which
/// is preserved through projection.
fn order_merged_hits(hits: &mut [JsonValue], sort: Option<&SortSpec>) {
    match sort {
        Some(spec) => {
            hits.sort_by(|a, b| compare_hits_by_field(a, b, SORT_KEY_FIELD, spec.order));
        }
        None => {
            hits.sort_by(|a, b| {
                hit_score(b)
                    .partial_cmp(&hit_score(a))
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
        }
    }
}

/// Recursively transform shadow field names in JSON query structure
fn transform_shadow_fields_recursive(
    value: &mut JsonValue,
    shadow_mapping: &std::collections::HashMap<String, String>,
) {
    match value {
        JsonValue::Object(map) => {
            // Check if this is a field reference that needs transformation
            if let Some((field_name, field_value)) = map.iter_mut().next() {
                // Handle common query patterns: {"term": {"field": "value"}}
                if (field_name == "term" || field_name == "terms" || field_name == "exists")
                    && let JsonValue::Object(field_map) = field_value
                {
                    for shadow_field in shadow_mapping.keys() {
                        if let Some(shadow_value) = field_map.remove(shadow_field) {
                            // Replace shadow field with canonical "id" field
                            field_map.insert("id".to_string(), shadow_value);
                            break; // Only one field per term query
                        }
                    }
                }

                // Recursively process nested objects
                transform_shadow_fields_recursive(field_value, shadow_mapping);
            }

            // Process all key-value pairs in the object
            for (_, v) in map.iter_mut() {
                transform_shadow_fields_recursive(v, shadow_mapping);
            }
        }
        JsonValue::Array(arr) => {
            // Process array elements
            for item in arr.iter_mut() {
                transform_shadow_fields_recursive(item, shadow_mapping);
            }
        }
        _ => {
            // Primitive values, no transformation needed
        }
    }
}

/// Helper function to detect if an operation is a write operation
fn is_write_operation(op: &ClientOp) -> bool {
    matches!(
        op,
        ClientOp::Write { .. } | ClientOp::BulkWrite { .. } | ClientOp::DeleteIndex { .. }
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
/// 4. Returns merged schema with best-guess field types
///
/// Usage:
/// - Only used during initial schema creation (empty schema)
/// - Existing schema evolution continues to use current logic
/// - Maintains backward compatibility with existing behavior
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

    tracing::info!(
        sampled_docs = sampled,
        total_docs = docs.len(),
        sample_limit = sample_limit,
        "Enhanced schema sampling completed for initial schema creation"
    );

    schema
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
    pub errors: Vec<String>,
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
    /// Number of background merge (compaction) threads per IndexWriter (default: 1)
    pub merge_num_threads: usize,
    /// Timeout in seconds for writer thread to drain pending commands during shutdown
    /// Increased from 10s to 30s to handle large coalesced batches
    pub writer_shutdown_timeout_secs: u64,
    /// Pin per-shard writer threads to a CPU core derived from `xxh3(shard_id) % num_cores`.
    /// Improves cache locality and reduces cross-core wakeups under heavy write load.
    /// Default: false (no pinning, OS scheduler decides).
    pub writer_core_affinity: bool,
    /// Enable shard-affine worker dispatch (default: false).
    /// When enabled, operations targeting the same shard are routed to the same
    /// orchestrator worker via `xxh3(shard_id) % worker_count`, reducing cross-core
    /// wakeups when writer pinning is also enabled.
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

/// Document payload for write operations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocPayload {
    pub id: String,
    #[serde(default)]
    pub routing_key: Option<String>,
    pub doc: JsonValue,
}

/// Write request message for MicroshardActor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteRequest {
    pub index: String,
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
        }
    }
}

impl From<RemoteError> for OrchestratorError {
    fn from(err: RemoteError) -> Self {
        match err {
            RemoteError::Io(s)
            | RemoteError::Identity(s)
            | RemoteError::NotFound(s)
            | RemoteError::InvalidInput(s)
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
    /// Create or update index configuration/schema
    CreateConfig { index: String, schema: IndexSchema },
    /// Get index configuration/schema
    GetConfig { index: String },
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
        reply: tokio::sync::oneshot::Sender<Result<JsonValue, OrchestratorError>>,
    },
    Shutdown,
}

/// Shared worker loop body used by both the default tokio-task spawn and the
/// pinned OS-thread spawn (Stage 2e). Exits cleanly when receiving `Shutdown`
/// or when the channel is closed.
async fn orchestrator_worker_loop(
    mut rx: mpsc::Receiver<OrchestratorJob>,
    engine: Arc<OrchestratorEngine>,
    worker_id: usize,
    counters: Option<Arc<WorkerCounters>>,
) {
    loop {
        match rx.recv().await {
            Some(OrchestratorJob::Execute {
                op,
                affinity_shard,
                reply,
            }) => {
                let result = engine.execute(*op, affinity_shard).await;
                // Send result back; ignore error if caller dropped the receiver
                let _ = reply.send(result);
                if let Some(c) = &counters {
                    c.queue_depth.fetch_sub(1, AtomicOrdering::Relaxed);
                    c.jobs_completed.fetch_add(1, AtomicOrdering::Relaxed);
                }
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
}

/// Per-worker atomic counters — updated on the send and receive hot paths.
#[derive(Debug, Default)]
struct WorkerCounters {
    /// Current number of jobs waiting in this worker's mpsc channel.
    queue_depth: AtomicUsize,
    /// Total jobs completed by this worker since startup.
    jobs_completed: AtomicU64,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub core_id: Option<usize>,
    pub queue_depth: usize,
    pub queue_capacity: usize,
    pub jobs_completed: u64,
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
    pub pinned: bool,
    pub hash_aligned: bool,
    pub worker_count: usize,
    pub workers: Vec<WorkerStats>,
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
    /// Whether workers are pinned to dedicated OS threads.
    pinned: bool,
    /// Whether worker_count was aligned to num_cores for hash co-location.
    hash_aligned: bool,
    /// Core IDs used for pinning (empty if not pinned).
    core_ids: Vec<core_affinity::CoreId>,
}

impl OrchestratorWorkerTx {
    fn new_with_stats(
        workers: Vec<mpsc::Sender<OrchestratorJob>>,
        worker_stats: Arc<Vec<Arc<WorkerCounters>>>,
        per_worker_queue_capacity: usize,
        pinned: bool,
        hash_aligned: bool,
        core_ids: Vec<core_affinity::CoreId>,
    ) -> Self {
        Self {
            workers: Arc::new(workers),
            next_worker: Arc::new(AtomicUsize::new(0)),
            worker_stats,
            dispatch_stats: Arc::new(DispatchCounters::default()),
            per_worker_queue_capacity,
            pinned,
            hash_aligned,
            core_ids,
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

        let is_affine = shard_id.is_some();
        let start = match shard_id {
            Some(sid) => {
                // Deterministic worker selection: same shard → same worker
                (xxh3_64(sid.as_bytes()) as usize) % self.workers.len()
            }
            None => {
                // No affinity hint — fall back to round-robin
                self.next_worker.fetch_add(1, AtomicOrdering::Relaxed)
            }
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
        let core_ids_slice: &[core_affinity::CoreId] = &self.core_ids;
        let workers = self
            .worker_stats
            .iter()
            .enumerate()
            .map(|(id, counters)| {
                let core_id = if core_ids_slice.is_empty() {
                    None
                } else {
                    Some(core_ids_slice[id % core_ids_slice.len()].id)
                };
                WorkerStats {
                    id,
                    core_id,
                    queue_depth: counters.queue_depth.load(AtomicOrdering::Relaxed),
                    queue_capacity: self.per_worker_queue_capacity,
                    jobs_completed: counters.jobs_completed.load(AtomicOrdering::Relaxed),
                }
            })
            .collect();

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
            pinned: self.pinned,
            hash_aligned: self.hash_aligned,
            worker_count: self.workers.len(),
            workers,
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
    pub schema_cache: Arc<ArcSwap<HashMap<String, Arc<IndexSchema>>>>,
    /// Fingerprint → index_name reverse lookup (lock-free via ArcSwap).
    pub fingerprint_index: Arc<ArcSwap<HashMap<u64, String>>>,
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
        let fingerprint = schema.fingerprint;

        self.schema_cache.rcu(|old| {
            let mut new = (**old).clone();
            new.insert(index_str.clone(), schema_arc.clone());
            new
        });

        if fingerprint != 0 {
            let idx = index_str;
            self.fingerprint_index.rcu(|old| {
                let mut new = (**old).clone();
                new.insert(fingerprint, idx.clone());
                new
            });
        }
    }

    /// Get schema by fingerprint (lock-free instant cache hit).
    fn get_schema_by_fingerprint(&self, fingerprint: u64) -> Option<Arc<IndexSchema>> {
        let fp_map = self.fingerprint_index.load();
        if let Some(index_name) = fp_map.get(&fingerprint) {
            let cache = self.schema_cache.load();
            return cache.get(index_name).cloned();
        }
        None
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
    /// Returns `Ok(result)` for ops handled by the engine, or an error
    /// with `ErrorKind::Unsupported` for ops that must go through the actor mailbox.
    ///
    /// `affinity_shard` is a pre-resolved shard hint from shard-affine dispatch.
    /// When `Some`, `engine_write` skips the redundant ring lookup.
    pub async fn execute(
        &self,
        op: ClientOp,
        affinity_shard: Option<Uuid>,
    ) -> Result<JsonValue, OrchestratorError> {
        match op {
            ClientOp::Write {
                index,
                id,
                routing_key,
                doc,
            } => {
                self.engine_write(&index, id, routing_key, doc, affinity_shard)
                    .await
            }
            ClientOp::BulkWrite { index, docs } => self.engine_bulk_write(&index, docs).await,
            ClientOp::Search {
                index,
                query,
                limit,
                fields,
                sort,
            } => {
                self.engine_search(
                    &index,
                    &query,
                    limit.unwrap_or(self.default_search_limit),
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
                let search_limit = limit.unwrap_or(self.default_search_limit);
                self.engine_search(
                    &index,
                    &query,
                    search_limit,
                    fields.as_deref(),
                    sort.as_ref(),
                )
                .await
            }
            // Config/metadata ops are lightweight — route through actor mailbox
            _ => Err(OrchestratorError::Io(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "Operation not supported by worker pool; use actor mailbox",
            ))),
        }
    }

    /// Fast-path single document write.
    ///
    /// Handles the common case where the schema is mature (no evolution needed):
    /// validates the document, routes to the correct shard, and dispatches the write.
    /// For schema evolution (rare), returns `ErrorKind::Unsupported` so the caller
    /// can fall back to the actor mailbox which has access to `staged_schema_validation`.
    async fn engine_write(
        &self,
        index: &str,
        id: String,
        routing_key: Option<String>,
        doc: JsonValue,
        affinity_shard: Option<Uuid>,
    ) -> Result<JsonValue, OrchestratorError> {
        let shards = self.shards.load();
        if shards.is_empty() {
            return Err(OrchestratorError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "No shards",
            )));
        }

        // Inline fingerprint from doc keys
        let doc_fingerprint = calculate_doc_fingerprint(&doc);

        // Lock-free schema lookup by fingerprint, then by index name
        let schema = if let Some(cached) = self.get_schema_by_fingerprint(doc_fingerprint) {
            cached
        } else if let Some(cached) = self.get_cached_schema(index) {
            cached
        } else {
            Arc::new(self.load_schema(index).await?)
        };

        // Fast path: mature schema — validate inline
        if !schema.fields.is_empty() {
            let result =
                NodeOrchestrator::validate_single_document_readonly_fast(&doc, &schema, false);
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
                let routing_field = schema.get_routing_field().to_string();
                let effective_routing_key = extract_routing_value(&doc, &routing_field)
                    .or(routing_key)
                    .or_else(|| (!id.is_empty()).then(|| id.clone()))
                    .or_else(|| derive_routing_key_from_doc(&doc));

                // Use pre-resolved shard from shard-affine dispatch when available,
                // skipping the redundant ring lookup. Fall back to route_write when
                // affinity is None (round-robin dispatch) or the hinted shard is gone.
                let target = if let Some(hint) = affinity_shard {
                    if shards.contains_key(&hint) {
                        hint
                    } else {
                        // Shard was removed or reassigned — re-resolve via ring
                        self.route_write(&effective_routing_key)?
                    }
                } else {
                    self.route_write(&effective_routing_key)?
                };
                let shard = shards.get(&target).ok_or_else(|| {
                    OrchestratorError::Io(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        "Shard not found",
                    ))
                })?;
                let req = WriteRequest {
                    index: index.to_string(),
                    routing_key: effective_routing_key.unwrap_or_default(),
                    doc,
                };

                return match shard.handle_write(req).await {
                    Ok(seq) => Ok(serde_json::json!({
                        "id": id, "result": "created", "version": seq,
                        "shard_id": target.to_string()
                    })),
                    Err(e) => Err(e),
                };
            }
            // needs_evolution == true: fall back to actor mailbox
        }

        // Slow path: schema evolution needed — signal caller to use actor mailbox
        Err(OrchestratorError::Io(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "SCHEMA_EVOLUTION_NEEDED",
        )))
    }

    /// Bulk write via the engine.
    ///
    /// Bulk writes involve complex schema validation, parallel routing, remote forwarding,
    /// and parallel shard processing. These are delegated to the actor mailbox which has
    /// access to the full `NodeOrchestrator` state.
    async fn engine_bulk_write(
        &self,
        _index: &str,
        _docs: Vec<DocPayload>,
    ) -> Result<JsonValue, OrchestratorError> {
        // Bulk writes require staged_schema_validation, parallel_local_shard_processing,
        // and remote forwarding — all of which need full NodeOrchestrator access.
        // Signal caller to route through actor mailbox.
        Err(OrchestratorError::Io(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "BULK_WRITE_USE_ACTOR",
        )))
    }

    /// Parallel scatter-gather search across all local shards.
    async fn engine_search(
        &self,
        index: &str,
        query: &str,
        limit: usize,
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

        // Transform query to map shadow fields to canonical "id" field
        let transformed_query = transform_shadow_query(query, &schema);

        let shard_targets: Vec<(Uuid, MicroshardActor)> = shards
            .iter()
            .map(|(&shard_id, shard)| (shard_id, shard.clone()))
            .collect();
        let shard_results: Vec<_> =
            futures::stream::iter(shard_targets.into_iter().map(|(shard_id, shard)| {
                let req = SearchRequest {
                    index: index.to_string(),
                    query: transformed_query.clone(),
                    limit: Some(limit),
                    sort: sort.cloned(),
                };
                async move { (shard_id, shard.handle_search(req).await) }
            }))
            .buffer_unordered(self.max_concurrent_shard_searches.max(1))
            .collect()
            .await;

        let mut results: Vec<(Uuid, f32, JsonValue)> = Vec::new();
        let mut errors = Vec::new();
        let mut shard_success = 0usize;
        let mut total_hits_sum = 0usize;
        for (shard_id, result) in shard_results {
            match result {
                Ok(r) => {
                    total_hits_sum += r.total_hits;
                    for hit in r.hits {
                        results.push((shard_id, hit.score, hit.doc));
                    }
                    shard_success += 1;
                }
                Err(err) => {
                    warn!(%shard_id, error = %err, "Engine scatter search shard failed");
                    errors.push(format!("Shard {}: {}", shard_id, err));
                }
            }
        }

        // Order merged results: by the requested sort field when provided, otherwise by
        // score descending. Each shard already returned field-sorted results, so a global
        // re-sort here is required to interleave them correctly across shards.
        //
        // When sorting, stamp each hit with the normalized `SORT_KEY_FIELD` first (while
        // the full doc is still present) and key the sort on it. The metadata field
        // survives the field projection below and lets a downstream cross-node merge
        // re-order these results even if the sort field is not among the returned fields.
        match sort {
            Some(spec) => {
                stamp_sort_keys(&mut results, spec, &schema);
                results
                    .sort_by(|a, b| compare_hits_by_field(&a.2, &b.2, SORT_KEY_FIELD, spec.order))
            }
            None => {
                results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal))
            }
        }
        results.truncate(limit);
        let total_shards = shards.len();
        let hits: Vec<JsonValue> = results
            .into_iter()
            .map(|(shard_id, score, mut doc)| {
                // Add metadata fields
                if let JsonValue::Object(ref mut o) = doc {
                    o.insert(
                        "_score".to_string(),
                        serde_json::Number::from_f64(score as f64)
                            .map(JsonValue::Number)
                            .unwrap_or(JsonValue::Null),
                    );
                    o.insert(
                        "shard_id".to_string(),
                        JsonValue::String(shard_id.to_string()),
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
        Ok(serde_json::json!({
            "hits": hits,
            "hits_returned": hits.len(),
            "total_hits": total_hits_sum,
            "limit": limit,
            "took_ms": start.elapsed().as_millis(),
            "stats": {
                "shards": {
                    "total": total_shards,
                    "responded": shard_success,
                    "failed": errors.len()
                }
            },
            "errors": errors
        }))
    }
}

/// Helper struct for aggregating index statistics across cluster nodes.
#[derive(Debug, Clone)]
struct IndexStats {
    name: String,
    document_count: u64,
    total_size_bytes: u64,
    index_size_mb: u64,
    data_size_mb: u64,
    shard_count: usize,
    field_names: Vec<String>,
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
    /// Pin the per-shard writer thread to a deterministic CPU core for cache locality.
    writer_core_affinity: bool,
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

impl MicroshardActor {
    pub fn new(
        shard_id: Uuid,
        storage_config: StorageConfig,
        default_search_limit: usize,
        read_pool_handle: Option<tokio::runtime::Handle>,
        total_shards: usize,
        writer_shutdown_timeout_secs: u64,
        writer_core_affinity: bool,
    ) -> Self {
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
            writer_core_affinity,
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
        let pin_core = self.writer_core_affinity;

        let handle = std::thread::Builder::new()
            .name(format!("writer-shard-{}", writer_shard_id))
            .spawn(move || {
                // Optionally pin this writer thread to a deterministic CPU core
                // derived from `xxh3(shard_id) % num_cores`. This improves cache
                // locality for redb/tantivy data structures and reduces cross-core
                // wakeups on the write hot path.
                if pin_core {
                    if let Some(core_ids) = core_affinity::get_core_ids()
                        && !core_ids.is_empty()
                    {
                        let hash = xxh3_64(writer_shard_id.as_bytes()) as usize;
                        let core_idx = hash % core_ids.len();
                        let target = core_ids[core_idx];
                        if core_affinity::set_for_current(target) {
                            info!(
                                shard_id = %writer_shard_id,
                                core_id = target.id,
                                num_cores = core_ids.len(),
                                "Writer thread pinned to CPU core"
                            );
                        } else {
                            // CPU pinning is not supported on macOS, so log as info instead of warn
                            if cfg!(target_os = "macos") {
                                info!(
                                    shard_id = %writer_shard_id,
                                    core_id = target.id,
                                    "CPU pinning not supported on macOS; writer thread continuing unpinned"
                                );
                            } else {
                                warn!(
                                    shard_id = %writer_shard_id,
                                    core_id = target.id,
                                    "Failed to pin writer thread to CPU core (continuing unpinned)"
                                );
                            }
                        }
                    } else {
                        // CPU pinning is not supported on macOS, so log as info instead of warn
                        if cfg!(target_os = "macos") {
                            info!(
                                shard_id = %writer_shard_id,
                                "CPU pinning not supported on macOS; writer thread continuing unpinned"
                            );
                        } else {
                            warn!(
                                shard_id = %writer_shard_id,
                                "core_affinity::get_core_ids() returned None/empty; writer thread unpinned"
                            );
                        }
                    }
                }

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

        let (results, total_hits) = self
            .spawn_on_read_pool(move || {
                store.search_documents(&index, &query, limit, sort.as_ref())
            })
            .await?
            .map_err(|e: StoreError| match e {
                StoreError::Io(io_err) => OrchestratorError::Io(io_err),
                _ => OrchestratorError::Io(std::io::Error::other(e.to_string())),
            })?;

        let search_hits: Vec<SearchHit> = results
            .into_iter()
            .map(|(score, doc)| SearchHit { score, doc })
            .collect();

        Ok(SearchReply {
            hits: search_hits,
            total_hits,
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
            // Read supervisor timeout from environment variable or use default
            let supervisor_timeout_secs = std::env::var("CAMEODB_SUPERVISOR_TIMEOUT_SECS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(5); // Default to 5 seconds
            let timeout_dur = Duration::from_secs(supervisor_timeout_secs); // Configurable timeout to allow batch processing to complete
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
    pub async fn handle_write(&self, request: WriteRequest) -> Result<u64, OrchestratorError> {
        // OPTIMIZATION: Take ownership of doc from request immediately
        let doc = request.doc;

        // Extract ID (borrowing from doc before move)
        let id = doc
            .get("id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                OrchestratorError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Document must contain an 'id' field",
                ))
            })?
            .to_string();

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
                ClientOp::Write { .. } | ClientOp::Search { .. } | ClientOp::Stream { .. }
            );
            if is_worker_eligible {
                // Resolve shard affinity hint when shard-affine dispatch is enabled.
                // For Write ops, the routing_key maps to a shard via the consistent ring.
                // For Search/Stream ops (scatter-gather), no single shard owns the query,
                // so affinity is None and dispatch falls back to round-robin.
                let affinity_shard = if self.shard_affine.enabled {
                    match &op {
                        ClientOp::Write { routing_key, .. } => {
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
                            Ok(result) => {
                                // Check if engine signaled fallback to actor
                                if let Err(OrchestratorError::Io(io_err)) = &result
                                    && io_err.kind() == std::io::ErrorKind::Unsupported
                                {
                                    let msg = io_err.to_string();
                                    if msg == "SCHEMA_EVOLUTION_NEEDED"
                                        || msg == "BULK_WRITE_USE_ACTOR"
                                    {
                                        return Err(OrchestratorError::Io(std::io::Error::other(
                                            "Schema evolution required; retry via actor",
                                        )));
                                    }
                                }
                                result
                            }
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
    async fn ask_orchestrator(&self, op: ClientOp) -> Result<JsonValue, OrchestratorError> {
        match self.orchestrator.ask(op).await {
            Ok(result) => Ok(result),
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

    /// Route via ClusterCoordinator then handle locally (remote/broadcast stubbed).
    #[cfg_attr(not(test), allow(dead_code))]
    pub async fn route_and_handle(
        &self,
        op: ClientOp,
        routing_key: Option<String>,
        operation_type: OperationType,
    ) -> Result<JsonValue, OrchestratorError> {
        // Metadata operations (schema/config) always execute locally - no need to broadcast
        if matches!(
            op,
            ClientOp::GetConfig { .. } | ClientOp::CreateConfig { .. }
        ) {
            return self.handle_client_op(op).await;
        }

        // Search/Stream responses carry an internal `SORT_KEY_FIELD` on each hit so that
        // merges can order by the sort field even when it is projected away. This is the
        // single client-facing boundary for every routing decision (local, broadcast,
        // remote, streaming-buffered), so strip that metadata here before returning.
        let is_search = matches!(op, ClientOp::Search { .. } | ClientOp::Stream { .. });

        let decision = self
            .coordinator
            .ask(RouteOperation {
                routing_key,
                operation_type,
            })
            .await;

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
        let remote_results_future =
            futures::stream::iter(peers.into_iter().take(self.broadcast_fanout_limit).map(
                move |peer| {
                    let op_clone = remote_op.clone();
                    let remote_router = remote_router.clone();
                    let node_id = peer.node_id;
                    let peer_addr = peer.address;
                    async move {
                        timeout(
                            remote_timeout,
                            remote_router.try_remote(op_clone, node_id, &peer_addr),
                        )
                        .await
                    }
                },
            ))
            .buffer_unordered(remote_limit)
            .collect::<Vec<_>>();

        // Execute local + remote concurrently
        let t_start = Instant::now();
        let (local_result, remote_results) = tokio::join!(local_future, remote_results_future);

        // If this is a search, prefer fastest/local results and stop after hitting the limit.
        if let ClientOp::Search { limit, sort, .. } = &op {
            let limit = limit.unwrap_or(self.default_search_limit);
            let sort = sort.clone();
            let mut merged_hits: Vec<JsonValue> = Vec::with_capacity(limit);
            let mut error_count = 0u64;
            let mut stats = BroadcastStats {
                total_shards_queried: 0,
                nodes_contacted: 0,
                max_took_ms: None,
                total_hits_sum: 0,
            };

            // Helper to push hits from a result up to the remaining limit.
            // For field-sorted queries we must collect all hits (bounded by fanout*limit)
            // and order them globally afterwards; the score-based top-K heap would drop
            // the wrong hits when ranking is by a document field rather than by score.
            fn push_hits(
                value: &mut JsonValue,
                merged_hits: &mut Vec<JsonValue>,
                limit: usize,
                is_field_sorted: bool,
                stats: &mut BroadcastStats,
            ) {
                if let Some(hits) = value.get_mut("hits").and_then(|h| h.as_array_mut()) {
                    for hit in hits.drain(..) {
                        if is_field_sorted {
                            merged_hits.push(hit);
                        } else {
                            push_hit_into_top_k(merged_hits, hit, limit);
                        }
                    }
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
                stats.nodes_contacted += 1;
                if let Some(t) = value.get("took_ms").and_then(|v| v.as_u64()) {
                    stats.max_took_ms = match stats.max_took_ms {
                        Some(cur) => Some(cur.max(t)),
                        None => Some(t),
                    };
                }
            }

            let is_field_sorted = sort.is_some();

            // Process local result first
            match local_result {
                Ok(mut val) => push_hits(
                    &mut val,
                    &mut merged_hits,
                    limit,
                    is_field_sorted,
                    &mut stats,
                ),
                Err(e) => {
                    error_count += 1;
                    warn!(error = %e, "Broadcast: local search failed");
                }
            }

            // Then process remote results in completion order until limit is reached
            for result in remote_results {
                match result {
                    Ok(Ok(mut val)) => push_hits(
                        &mut val,
                        &mut merged_hits,
                        limit,
                        is_field_sorted,
                        &mut stats,
                    ),
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

            // Order the collected set: by the requested sort field when provided,
            // otherwise by relevance score (descending).
            order_merged_hits(&mut merged_hits, sort.as_ref());
            merged_hits.truncate(limit);

            return Ok(serde_json::json!({
                "hits": merged_hits,
                "hits_returned": merged_hits.len(),
                "total_hits": stats.total_hits_sum,
                "limit": limit,
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
            }));
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

        // Process remote results
        for result in remote_results {
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
            ClientOp::Search { limit, .. } => {
                // Enforce a global limit across merged results to avoid returning
                // (limit * nodes) hits when broadcasting.
                let limit = limit.unwrap_or(self.default_search_limit);
                let nodes_contacted = all_results.len();

                // For search operations, if we only have local results (no remote peers),
                // return the local response directly to preserve shard-level details
                if all_results.len() == 1 && peer_count == 0 {
                    return Ok(all_results[0].clone());
                }

                // Merge search results from multiple nodes: combine all hits arrays
                let mut merged_hits: Vec<JsonValue> = Vec::new();
                let mut total_shards_queried = 0usize;
                let mut total_hits_sum = 0usize;

                for mut result in all_results {
                    if let Some(hits) = result.get_mut("hits").and_then(|h| h.as_array_mut()) {
                        for hit in hits.drain(..) {
                            push_hit_into_top_k(&mut merged_hits, hit, limit);
                        }
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

                // Sort by score descending and deduplicate by _id if present
                merged_hits.sort_by(|a, b| {
                    hit_score(b)
                        .partial_cmp(&hit_score(a))
                        .unwrap_or(std::cmp::Ordering::Equal)
                });
                merged_hits.truncate(limit);

                Ok(serde_json::json!({
                    "hits": merged_hits,
                    "hits_returned": merged_hits.len(),
                    "total_hits": total_hits_sum,
                    "limit": limit,
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
                }))
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
                                document_count: 0,
                                total_size_bytes: 0,
                                index_size_mb: 0,
                                data_size_mb: 0,
                                shard_count: 0,
                                field_names: Vec::new(),
                            });

                            entry.document_count += idx
                                .get("document_count")
                                .and_then(|v| v.as_u64())
                                .unwrap_or(0);
                            entry.total_size_bytes += idx
                                .get("total_size_bytes")
                                .and_then(|v| v.as_u64())
                                .unwrap_or(0);
                            entry.index_size_mb += idx
                                .get("index_size_mb")
                                .and_then(|v| v.as_u64())
                                .unwrap_or(0);
                            if let Some(data_size) =
                                idx.get("data_size_mb").and_then(|v| v.as_u64())
                            {
                                entry.data_size_mb += data_size;
                            }
                            entry.shard_count +=
                                idx.get("shard_count").and_then(|v| v.as_u64()).unwrap_or(0)
                                    as usize;

                            if let Some(fields) = idx.get("field_names").and_then(|v| v.as_array())
                            {
                                for field in fields {
                                    if let Some(field_str) = field.as_str()
                                        && !entry.field_names.contains(&field_str.to_string())
                                    {
                                        entry.field_names.push(field_str.to_string());
                                    }
                                }
                            }
                        }
                    }
                }

                for stats in index_map.values_mut() {
                    stats
                        .field_names
                        .sort_by(|a, b| match (a.as_str(), b.as_str()) {
                            ("id", "id") => std::cmp::Ordering::Equal,
                            ("id", _) => std::cmp::Ordering::Less,
                            (_, "id") => std::cmp::Ordering::Greater,
                            _ => a.cmp(b),
                        });
                }

                // Convert to JSON array with new format
                let mut cluster_indexes: Vec<(String, JsonValue)> = index_map
                    .into_values()
                    .map(|stats| {
                        let name = stats.name.clone();
                        let mut json_obj = serde_json::Map::new();
                        json_obj.insert("name".to_string(), serde_json::json!(stats.name));
                        json_obj.insert(
                            "document_count".to_string(),
                            serde_json::json!(stats.document_count),
                        );

                        // Only include size fields when data size is requested
                        if *include_data_size {
                            json_obj.insert(
                                "total_size_bytes".to_string(),
                                serde_json::json!(stats.total_size_bytes),
                            );
                        }

                        json_obj.insert(
                            "index_size_mb".to_string(),
                            serde_json::json!(stats.index_size_mb),
                        );

                        if *include_data_size {
                            json_obj.insert(
                                "data_size_mb".to_string(),
                                serde_json::json!(stats.data_size_mb),
                            );
                        }
                        json_obj.insert(
                            "shard_count".to_string(),
                            serde_json::json!(stats.shard_count),
                        );
                        json_obj.insert(
                            "field_names".to_string(),
                            serde_json::json!(stats.field_names),
                        );
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
            ClientOp::Search {
                index,
                query,
                limit,
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
                let is_field_sorted = sort.is_some();

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
                let mut all_hits = Vec::new();
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
                        && all_hits.len() >= limit
                        && search_futures.is_empty()
                        && peer_iter.size_hint().0 == 0
                    {
                        break;
                    }

                    match search_result {
                        StreamingSearchResult::Local {
                            shard_id: _,
                            hits,
                            total_hits,
                            took_ms: _,
                        } => {
                            // Process streaming local search results
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
                                    // Track unique shard IDs from individual documents
                                    if let Some(shard_id) =
                                        hit_doc.get("shard_id").and_then(|s| s.as_str())
                                        && let Ok(uuid) = Uuid::parse_str(shard_id)
                                    {
                                        unique_shard_ids.insert(uuid);
                                    }
                                }
                                if is_field_sorted {
                                    all_hits.push(hit_doc);
                                } else {
                                    push_hit_into_top_k(&mut all_hits, hit_doc, limit);
                                }
                            }
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
                                        for hit in hits.drain(..) {
                                            if is_field_sorted {
                                                all_hits.push(hit);
                                            } else {
                                                push_hit_into_top_k(&mut all_hits, hit, limit);
                                            }
                                        }
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

                // Order the collected set: by the requested sort field when provided,
                // otherwise by relevance score (descending), then apply the limit.
                order_merged_hits(&mut all_hits, sort.as_ref());
                all_hits.truncate(limit);

                Ok(serde_json::json!({
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
                    "errors": errors
                }))
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
    /// Per-index schema cache to avoid repeated metadata reads (lock-free via ArcSwap).
    /// Wrapped in Arc so it can be shared with the OrchestratorEngine worker pool.
    schema_cache: Arc<ArcSwap<HashMap<String, Arc<IndexSchema>>>>,
    /// Fingerprint → index_name reverse lookup for instant cache hits (lock-free via ArcSwap).
    /// Wrapped in Arc so it can be shared with the OrchestratorEngine worker pool.
    fingerprint_index: Arc<ArcSwap<HashMap<u64, String>>>,
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

            tracing::info!(
                index = %index,
                sampled_fields = sampled_field_count,
                "Enhanced sampling merged for initial schema creation"
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

        for result in validation_results {
            if let Some(err) = result.validation_error {
                summary.errors.push(err);
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

        // Stage 3: Sequential schema evolution (only if needed)
        if summary.evolution_needed && !summary.all_new_fields.is_empty() {
            self.evolve_schema_sequential(
                index,
                schema_cache,
                &summary.all_new_fields,
                &self.shards,
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

    /// Parallel schema validation (read-only, no mutations)
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

        let is_initial_creation = schema_cache.fields.is_empty();

        // Small batches validate inline. Offloading them costs two thread hops (onto this
        // worker's blocking pool, then a rayon fan-out onto the global rayon pool, which is
        // unpinned and competes with the writer threads) plus a full clone of the documents
        // and schema — all to run a handful of cheap per-document checks. Both hops are pure
        // overhead below this size.
        const INLINE_VALIDATION_MAX_DOCS: usize = 64;
        if !is_initial_creation && docs.len() <= INLINE_VALIDATION_MAX_DOCS {
            return Ok(docs
                .iter()
                .map(|doc_payload| {
                    Self::validate_single_document_readonly_fast(
                        &doc_payload.doc,
                        schema_cache,
                        is_initial_creation,
                    )
                })
                .collect());
        }

        // Fast path: if schema is mature and batch is small, skip expensive clone
        let use_fast_path = !is_initial_creation && docs.len() < 1000;

        // Clone data so it is Send + 'static inside spawn_blocking
        let docs_owned: Vec<DocPayload> = docs.to_vec();
        let schema_clone = schema_cache.clone();

        if use_fast_path {
            tracing::debug!("Using fast path validation for {} documents", docs.len());
            let results = tokio::task::spawn_blocking(move || {
                docs_owned
                    .par_iter()
                    .map(|doc_payload| {
                        Self::validate_single_document_readonly_fast(
                            &doc_payload.doc,
                            &schema_clone,
                            is_initial_creation,
                        )
                    })
                    .collect::<Vec<SchemaValidationResult>>()
            })
            .await
            .map_err(|e| OrchestratorError::Io(std::io::Error::other(e)))?;

            return Ok(results);
        }

        let results = tokio::task::spawn_blocking(move || {
            docs_owned
                .par_iter()
                .map(|doc_payload| {
                    Self::validate_single_document_readonly(
                        &doc_payload.doc,
                        &schema_clone,
                        is_initial_creation,
                    )
                })
                .collect::<Vec<SchemaValidationResult>>()
        })
        .await
        .map_err(|e| OrchestratorError::Io(std::io::Error::other(e)))?;

        Ok(results)
    }

    /// Read-only validation for a single document (no mutations) - fast path
    fn validate_single_document_readonly_fast(
        doc: &JsonValue,
        schema_cache: &IndexSchema, // Pass by reference, no clone needed
        _is_initial_creation: bool,
    ) -> SchemaValidationResult {
        // Check 1: Ensure doc["id"] exists
        if !doc.is_object() || !doc.as_object().unwrap().contains_key("id") {
            return SchemaValidationResult {
                needs_evolution: false,
                new_fields: Vec::new(),
                validation_error: Some("Document missing required 'id' field".to_string()),
            };
        }

        // Check 2: Validate against existing schema (no evolution in fast path)
        if let Some(obj) = doc.as_object() {
            for (key, value) in obj {
                if key == "id" {
                    continue; // Skip ID field
                }

                // Only check if field exists in schema, don't add new fields
                if !schema_cache.fields.contains_key(key) {
                    // In fast path, we don't track new fields for schema evolution
                    // This is a performance optimization for mature schemas
                    continue;
                }

                // Type validation against existing schema
                if let Some(field_def) = schema_cache.fields.get(key) {
                    let inferred_type = if key == "id" {
                        TantivyFieldType::Text
                    } else {
                        match value {
                            JsonValue::String(s) => {
                                // Try to infer date from string
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
                            JsonValue::Array(_) => TantivyFieldType::Text, // Arrays as text
                            JsonValue::Object(_) => TantivyFieldType::Json, // Objects as JSON
                            JsonValue::Null => TantivyFieldType::Text,
                        }
                    };

                    if inferred_type != field_def.field_type {
                        return SchemaValidationResult {
                            needs_evolution: false,
                            new_fields: Vec::new(),
                            validation_error: Some(format!(
                                "Type mismatch for field '{}': expected {:?}, got {:?}",
                                key, field_def.field_type, inferred_type
                            )),
                        };
                    }
                }
            }
        }

        SchemaValidationResult {
            needs_evolution: false, // Fast path never needs evolution
            new_fields: Vec::new(), // No new fields tracked in fast path
            validation_error: None,
        }
    }

    /// Read-only validation for a single document (no mutations)
    fn validate_single_document_readonly(
        doc: &JsonValue,
        schema_cache: &IndexSchema,
        _is_initial_creation: bool,
    ) -> SchemaValidationResult {
        // Check 1: Ensure doc["id"] exists
        if !doc.is_object() || !doc.as_object().unwrap().contains_key("id") {
            return SchemaValidationResult {
                needs_evolution: false,
                new_fields: Vec::new(),
                validation_error: Some("Document must contain an 'id' field".to_string()),
            };
        }

        let mut needs_evolution = false;
        let mut new_fields = Vec::new();

        // Check 2: Validate fields and identify new ones
        if let Some(obj) = doc.as_object() {
            for (key, value) in obj {
                let inferred_type = if key == "id" {
                    TantivyFieldType::Text
                } else {
                    match value {
                        JsonValue::String(s) => {
                            // Try to infer date from string
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
                        JsonValue::Array(_) => TantivyFieldType::Text, // Arrays as text
                        JsonValue::Object(_) => TantivyFieldType::Json, // Objects as JSON
                        JsonValue::Null => TantivyFieldType::Text,
                    }
                };

                if let Some(existing_field) = schema_cache.fields.get(key) {
                    // Check type compatibility (read-only)
                    let mut is_compatible = existing_field.field_type == inferred_type;

                    // Allow Text to match String (backward compatibility)
                    if !is_compatible
                        && inferred_type == TantivyFieldType::Text
                        && existing_field.field_type == TantivyFieldType::String
                    {
                        is_compatible = true;
                    }

                    // Allow Text to evolve to more specific types
                    if !is_compatible && existing_field.field_type == TantivyFieldType::Text {
                        match inferred_type {
                            TantivyFieldType::Date
                            | TantivyFieldType::Ip
                            | TantivyFieldType::I64
                            | TantivyFieldType::U64
                            | TantivyFieldType::F64
                            | TantivyFieldType::Boolean
                            | TantivyFieldType::Json => {
                                is_compatible = true;
                            }
                            _ => {}
                        }
                    }

                    // Allow numeric upgrades
                    if !is_compatible {
                        match (&existing_field.field_type, inferred_type.clone()) {
                            (TantivyFieldType::I64, TantivyFieldType::F64)
                            | (TantivyFieldType::U64, TantivyFieldType::F64) => {
                                is_compatible = true;
                            }
                            _ => {}
                        }
                    }

                    if !is_compatible {
                        return SchemaValidationResult {
                            needs_evolution: false,
                            new_fields: Vec::new(),
                            validation_error: Some(format!(
                                "Type mismatch for field '{}': expected {:?}, got {:?}",
                                key, existing_field.field_type, inferred_type
                            )),
                        };
                    }
                } else {
                    // New field detected
                    needs_evolution = true;
                    new_fields.push((key.clone(), inferred_type));
                }
            }
        }

        SchemaValidationResult {
            needs_evolution,
            new_fields,
            validation_error: None,
        }
    }

    /// Optimized schema evolution for batch processing
    async fn evolve_schema_sequential(
        &self,
        index: &str,
        schema_cache: &mut IndexSchema,
        new_fields: &std::collections::HashSet<(String, TantivyFieldType)>,
        shards: &HashMap<Uuid, MicroshardActor>,
    ) -> Result<(), OrchestratorError> {
        let is_initial_creation = schema_cache.fields.is_empty();

        // For initial creation with many fields, do optimized batch processing
        if is_initial_creation && new_fields.len() > 10 {
            tracing::debug!(
                index = %index,
                fields_count = new_fields.len(),
                "Optimized initial schema creation with batch field addition"
            );

            // Add all fields at once for better performance
            let indexed = true; // All fields indexed in initial creation
            for (field_name, field_type) in new_fields {
                if !schema_cache.fields.contains_key(field_name) {
                    let mut new_field = FieldDef::new(field_name.clone(), field_type.clone());
                    new_field.indexed = indexed;
                    schema_cache.fields.insert(field_name.clone(), new_field);
                }
            }

            tracing::info!(
                index = %index,
                total_fields = schema_cache.fields.len(),
                "Initial schema created with batch optimization"
            );
        } else {
            // Filter only truly new fields to avoid redundant work
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

            // Batch add all fields at once for better performance
            let indexed = is_initial_creation; // All fields indexed in initial creation
            for (field_name, field_type) in &fields_to_add {
                let mut new_field = FieldDef::new(field_name.clone(), field_type.clone());
                new_field.indexed = indexed;
                schema_cache.fields.insert(field_name.clone(), new_field);
            }

            tracing::info!(
                index = %index,
                fields_count = fields_to_add.len(),
                "Schema evolution completed - batch added fields"
            );
        }

        // Persist updated schema to storage if changed
        if !new_fields.is_empty() {
            let index_name = index.to_string();
            let schema_clone = schema_cache.clone();

            // Collect all stores from local shards
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

            // Persist to all stores concurrently
            let handles: Vec<_> = stores
                .into_iter()
                .map(|store| {
                    let idx = index_name.clone();
                    let sch = schema_clone.clone();
                    tokio::task::spawn_blocking(move || store.store_schema_and_cache(&idx, &sch))
                })
                .collect();

            // Await all results
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

    /// Process local shard batches sequentially, relying on actor message queues for proper isolation.
    ///
    /// Each shard actor processes its messages sequentially from its own queue,
    /// preventing concurrent access to shared storage resources.
    async fn parallel_local_shard_processing(
        &self,
        index: &str,
        local_batches: HashMap<Uuid, Vec<(DocPayload, Option<String>)>>,
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

                let shard = shard.ok_or_else(|| {
                    OrchestratorError::Io(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        format!("Local shard {} not found", shard_id),
                    ))
                })?;

                let docs: Vec<DocPayload> = batch
                    .into_iter()
                    .map(|(d, effective_routing_key)| DocPayload {
                        id: d.id,
                        routing_key: effective_routing_key,
                        doc: d.doc,
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
                    .map(|seq_ids| (shard_id, seq_ids))
            });
        }

        let local_results = futures::future::join_all(local_futures).await;
        for result in local_results {
            match result {
                Ok((shard_id, seq_ids)) => {
                    tracing::info!(
                        shard_id = %shard_id,
                        written_count = seq_ids.len(),
                        "Local shard batch completed successfully"
                    );
                    total_written += seq_ids.len();
                }
                Err(e) => {
                    let error_msg = format!("{}", e);
                    tracing::warn!(error = %error_msg, "Local shard batch processing failed");
                    all_errors.push(error_msg);
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
    /// Uses the cached RemotePeerPool to avoid repeated swarm registry lookups.
    async fn forward_bulk_to_remote(
        &self,
        node_id: Uuid,
        peer_addr: &str,
        index: &str,
        docs: Vec<DocPayload>,
    ) -> Result<usize, OrchestratorError> {
        info!(
            "🔎 Forwarding bulk batch to remote: node_id={}, addr={}, docs={}",
            node_id,
            peer_addr,
            docs.len()
        );

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

        // Extract the number of items written from the response
        if let Some(items_written) = res.get("items_written").and_then(|v| v.as_u64()) {
            Ok(items_written as usize)
        } else {
            Err(OrchestratorError::Io(std::io::Error::other(
                "Invalid response from remote bulk write",
            )))
        }
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
        let fingerprint = schema.fingerprint;

        self.schema_cache.rcu(|old| {
            let mut new = (**old).clone();
            new.insert(index_str.clone(), schema_arc.clone());
            new
        });

        // Maintain fingerprint reverse lookup
        if fingerprint != 0 {
            let idx = index_str;
            self.fingerprint_index.rcu(|old| {
                let mut new = (**old).clone();
                new.insert(fingerprint, idx.clone());
                new
            });
        }
    }

    /// Get schema by fingerprint (lock-free instant cache hit).
    fn get_schema_by_fingerprint(&self, fingerprint: u64) -> Option<Arc<IndexSchema>> {
        let fp_map = self.fingerprint_index.load();
        if let Some(index_name) = fp_map.get(&fingerprint) {
            let cache = self.schema_cache.load();
            return cache.get(index_name).cloned();
        }
        None
    }

    /// Produce sorted field names with "id" first (if present), others alphabetical.
    fn sorted_field_names(schema: &IndexSchema) -> Vec<String> {
        let mut names: Vec<String> = schema.fields.keys().cloned().collect();
        names.sort_by(|a, b| match (a.as_str(), b.as_str()) {
            ("id", "id") => std::cmp::Ordering::Equal,
            ("id", _) => std::cmp::Ordering::Less,
            (_, "id") => std::cmp::Ordering::Greater,
            _ => a.cmp(b),
        });
        names
    }

    /// Produce a JSON object of fields, ordered with "id" first (if present).
    fn sorted_fields_map(schema: &IndexSchema) -> JsonMap<String, JsonValue> {
        let mut entries: Vec<_> = schema.fields.iter().collect();
        entries.sort_by(|(a, _), (b, _)| match (a.as_str(), b.as_str()) {
            ("id", "id") => std::cmp::Ordering::Equal,
            ("id", _) => std::cmp::Ordering::Less,
            (_, "id") => std::cmp::Ordering::Greater,
            _ => a.cmp(b),
        });

        let mut map = JsonMap::new();
        for (k, v) in entries {
            let value = serde_json::to_value(v).unwrap_or(JsonValue::Null);
            map.insert(k.clone(), value);
        }
        map
    }

    fn schema_response(fields: JsonMap<String, JsonValue>) -> JsonValue {
        let mut map = JsonMap::new();
        map.insert("fields".to_string(), JsonValue::Object(fields));
        JsonValue::Object(map)
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
            schema_cache: Arc::new(ArcSwap::from_pointee(HashMap::new())),
            fingerprint_index: Arc::new(ArcSwap::from_pointee(HashMap::new())),
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
            fingerprint_index: Arc::clone(&self.fingerprint_index),
            coordinator: self.coordinator.clone(),
            identity: self.identity.clone(),
            default_search_limit: self.default_search_limit,
            max_concurrent_shard_searches: self.max_concurrent_shard_searches,
            remote_peer_pool: pool,
        });

        // Worker count: min(local_shards * 2, cpu_cores * 2), minimum 1.
        //
        // Stage 2c — hash-space alignment: when shard-affine dispatch AND writer
        // core pinning are both enabled, force `worker_count == cpu_cores` so that
        // `xxh3(shard_id) % worker_count` (dispatch) and `xxh3(shard_id) % cpu_cores`
        // (writer pinning) produce the SAME bucket. Then for any shard S, the worker
        // handling S dispatches into the writer pinned on the matching core, giving
        // the OS scheduler the best chance to keep the worker task near that core.
        let cpu_cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
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
            hash_aligned = aligned,
            queue_capacity = ORCHESTRATOR_WORKER_QUEUE_CAPACITY,
            per_worker_queue_capacity = per_worker_queue_capacity,
            "Spawning orchestrator worker pool"
        );

        // Stage 2e — true OS-thread pinning gate. Only active when all three
        // affinity flags are on AND we successfully retrieved core_ids. Falls
        // back to plain tokio::spawn otherwise.
        let pin_workers = aligned && self.config.worker_core_affinity;
        let core_ids: Option<Vec<core_affinity::CoreId>> = if pin_workers {
            let ids = core_affinity::get_core_ids().unwrap_or_default();
            if ids.is_empty() { None } else { Some(ids) }
        } else {
            None
        };
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

            if let Some(ids) = core_ids.as_ref() {
                // Pinned path: dedicated OS thread + current_thread runtime
                let target_core = ids[worker_id % ids.len()];
                let handle = std::thread::Builder::new()
                    .name(format!("orch-worker-{}", worker_id))
                    .spawn(move || {
                        // Pin this OS thread to the target core (best-effort).
                        if core_affinity::set_for_current(target_core) {
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
                            engine,
                            worker_id,
                            Some(counters),
                        ));
                    })
                    .expect("Failed to spawn orchestrator worker thread");
                worker_threads.push(handle);
            } else {
                // Default path: tokio task on main multi-threaded runtime.
                tokio::spawn(orchestrator_worker_loop(
                    rx,
                    engine,
                    worker_id,
                    Some(counters),
                ));
            }
        }

        let tx = OrchestratorWorkerTx::new_with_stats(
            worker_txs,
            worker_stats,
            per_worker_queue_capacity,
            pin_workers,
            aligned,
            core_ids.unwrap_or_default(),
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

    /// Scans the storage directory for existing shard folders and hydrates them with
    /// bounded concurrency. The bottleneck is redb::Builder::create() which does heavy
    /// disk I/O (WAL replay, compaction). Running all shards simultaneously causes I/O
    /// contention that makes each open 10-100× slower. A semaphore limits how many shards
    /// open their redb databases concurrently.
    async fn hydrate_existing_shards(&mut self) -> Result<(), OrchestratorError> {
        let existing_shards = self.discover_existing_shards()?;
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
        let writer_core_affinity = self.config.writer_core_affinity;
        for &shard_id in &existing_shards {
            let shard_path = self.deterministic_shard_directory(shard_id);
            let storage_config = self.create_shard_storage_config(shard_id, shard_path);
            let default_search_limit = self.default_search_limit;
            let read_handle = self.read_runtime.as_ref().map(|rt| rt.handle().clone());
            let sem = Arc::clone(&semaphore);

            let task = tokio::spawn(async move {
                // Acquire semaphore permit before starting heavy I/O
                let _permit = sem.acquire().await.map_err(|e| {
                    OrchestratorError::Io(std::io::Error::other(format!("Semaphore closed: {}", e)))
                })?;

                let mut microshard = MicroshardActor::new(
                    shard_id,
                    storage_config,
                    default_search_limit,
                    read_handle,
                    total_shards,
                    writer_shutdown_timeout_secs,
                    writer_core_affinity,
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
        let mut microshard = MicroshardActor::new(
            shard_id,
            storage_config,
            self.default_search_limit,
            read_handle,
            total_shards,
            self.config.writer_shutdown_timeout_secs,
            self.config.writer_core_affinity,
        );
        microshard.start().await?;

        // Add to shards map
        self.shards.insert(shard_id, microshard);
        self.register_shard_for_routing(shard_id);
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

        // Writer threads must exit before storage shutdown to release IndexWriter locks
        for (shard_id, shard) in self.shards.iter_mut() {
            tracing::debug!(shard_id = %shard_id, "Shutting down shard writer thread");
            shard.shutdown_writer().await;
        }

        // Parallel storage shutdown with per-shard 30s timeout
        let mut shard_ids = Vec::new();
        let mut shutdown_futures = Vec::new();
        for (shard_id, shard) in self.shards.iter_mut() {
            if let Some(store) = shard.store.take() {
                let shard_id_clone = *shard_id;

                let future = tokio::time::timeout(
                    Duration::from_secs(30),
                    tokio::task::spawn_blocking(move || {
                        tracing::info!(shard_id = %shard_id_clone, "Calling storage shutdown");
                        if let Err(e) = store.shutdown() {
                            tracing::error!(shard_id = %shard_id_clone, error = %e, "Storage shutdown failed");
                            return Err(e);
                        }
                        // Drop inside blocking task ensures file handles are released deterministically
                        drop(store);
                        tracing::debug!(shard_id = %shard_id_clone, "Storage dropped successfully");
                        Ok(())
                    }),
                );
                shard_ids.push(shard_id_clone);
                shutdown_futures.push(future);
            } else {
                tracing::warn!(shard_id = %shard_id, "Shard store already taken, skipping shutdown");
            }
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
                fields,
                sort,
            } => {
                self.orch_search(
                    &index,
                    &query,
                    limit.unwrap_or(self.default_search_limit),
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
                    search_limit,
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
            ClientOp::CreateConfig { index, schema } => {
                self.orch_create_config(&index, schema).await
            }
            ClientOp::GetConfig { index } => self.orch_get_config(&index).await,
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

        // Clear schema cache and fingerprint index for this index (lock-free)
        {
            let old_cache = self.schema_cache.load();
            if let Some(schema) = old_cache.get(index) {
                let fingerprint = schema.fingerprint;
                let idx = index.to_string();
                self.schema_cache.rcu(|old| {
                    let mut new = (**old).clone();
                    new.remove(&idx);
                    new
                });
                if fingerprint != 0 {
                    self.fingerprint_index.rcu(|old| {
                        let mut new = (**old).clone();
                        new.remove(&fingerprint);
                        new
                    });
                }
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

        // Inline fingerprint from doc keys — no HashSet<String> allocation
        let doc_fingerprint = calculate_doc_fingerprint(&doc);

        // Lock-free schema lookup by fingerprint, then by index name
        let schema = if let Some(cached) = self.get_schema_by_fingerprint(doc_fingerprint) {
            cached
        } else if let Some(cached) = self.get_cached_schema(index) {
            cached
        } else {
            Arc::new(self.load_schema(index).await?)
        };

        // Fast path: mature schema — validate inline without spawn_blocking/Rayon
        if !schema.fields.is_empty() {
            let result = Self::validate_single_document_readonly_fast(&doc, &schema, false);
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
                let routing_field = schema.get_routing_field().to_string();
                let effective_routing_key = extract_routing_value(&doc, &routing_field)
                    .or(routing_key)
                    .or_else(|| (!id.is_empty()).then(|| id.clone()))
                    .or_else(|| derive_routing_key_from_doc(&doc));

                let target = self.route_write(&effective_routing_key)?;
                let shard = self.shards.get(&target).ok_or_else(|| {
                    OrchestratorError::Io(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        "Shard not found",
                    ))
                })?;
                let req = WriteRequest {
                    index: index.to_string(),
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
            return Err(OrchestratorError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                validation_summary.errors.join("; "),
            )));
        }

        // Schema-based routing
        let routing_field = schema_mut.get_routing_field().to_string();
        let effective_routing_key = extract_routing_value(&doc, &routing_field)
            .or(routing_key)
            .or_else(|| (!id.is_empty()).then(|| id.clone()))
            .or_else(|| derive_routing_key_from_doc(&doc));

        let target = self.route_write(&effective_routing_key)?;
        let shard = self.shards.get(&target).ok_or_else(|| {
            OrchestratorError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Shard not found",
            ))
        })?;
        let req = WriteRequest {
            index: index.to_string(),
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

        // Fingerprint-based schema lookup: check cache before loading from shard
        let batch_field_names = extract_field_names(&docs);
        let batch_fingerprint = calculate_batch_fingerprint(&batch_field_names);

        let mut schema_cache =
            if let Some(cached) = self.get_schema_by_fingerprint(batch_fingerprint) {
                (*cached).clone()
            } else {
                self.load_schema(index).await?
            };

        // Use staged schema validation: parallel validation + sequential evolution
        let validation_summary = self
            .staged_schema_validation(index, &docs, &mut schema_cache)
            .await?;

        // Update cache (also populates fingerprint_index for future lookups)
        if validation_summary.evolution_needed || self.get_cached_schema(index).is_none() {
            self.put_cached_schema(index, &schema_cache);
        }

        // Check for validation errors
        if !validation_summary.errors.is_empty() {
            tracing::warn!(
                error_count = validation_summary.errors.len(),
                total_docs = validation_summary.total_docs,
                "Some documents failed schema validation"
            );
            // Continue processing valid documents, errors are tracked separately
        }

        // Group documents by target shard using parallel routing for better performance
        let items_received = docs.len();

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
            docs.into_par_iter()
                .map(|doc| {
                    // Calculate effective routing key using schema's routing field
                    let effective_routing_key = extract_routing_value(&doc.doc, &routing_field)
                        .or_else(|| doc.routing_key.clone())
                        .or_else(|| (!doc.id.is_empty()).then(|| doc.id.clone()))
                        .or_else(|| derive_routing_key_from_doc(&doc.doc));

                    // Route to shard using consistent hash ring
                    let target_shard =
                        match effective_routing_key.as_ref() {
                            Some(key) => routing_ring
                                .get_owner(key)
                                .or(first_shard_id)
                                .ok_or_else(|| {
                                    OrchestratorError::Io(std::io::Error::new(
                                        std::io::ErrorKind::NotFound,
                                        "No shard available for routing",
                                    ))
                                })?,
                            None => {
                                return Err(OrchestratorError::Io(std::io::Error::new(
                                    std::io::ErrorKind::InvalidInput,
                                    "Missing routing key for document",
                                )));
                            }
                        };

                    Ok((doc, effective_routing_key, Some(target_shard)))
                })
                .collect::<Vec<RoutingResult>>()
        })
        .await
        .map_err(|e| OrchestratorError::Io(std::io::Error::other(e.to_string())))?;

        // Separate local and remote documents
        for result in routing_results {
            match result {
                Ok((doc, routing_key, Some(target_shard))) => {
                    // Check if this shard is local
                    if self.shards.contains_key(&target_shard) {
                        local_docs.push((doc, routing_key, target_shard));
                    } else {
                        remote_docs.push((doc, routing_key, target_shard));
                    }
                }
                Ok((doc, _, None)) => {
                    // No target shard - this shouldn't happen but handle gracefully
                    tracing::warn!("Document routed to no shard: {}", doc.id);
                }
                Err(e) => {
                    tracing::warn!("Routing error: {}", e);
                }
            }
        }

        // Group local documents by shard
        let batches = self.group_local_documents(local_docs).await?;
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
        let mut errors = Vec::new();

        // Process local batches from parallel routing
        for (shard_id, batch) in batches {
            local_batches.insert(shard_id, batch);
        }

        // Group remote documents by owning node
        let mut remote_by_node: HashMap<Uuid, Vec<DocPayload>> = HashMap::new();
        for (doc, routing_key, target_shard) in remote_docs {
            if let Some(shard_meta) = shard_assignments.get(&target_shard) {
                let owner_node = shard_meta.node_id;
                let doc_payload = DocPayload {
                    id: doc.id.clone(),
                    routing_key,
                    doc: doc.doc,
                };
                remote_by_node
                    .entry(owner_node)
                    .or_default()
                    .push(doc_payload);
            } else {
                errors.push(format!(
                    "No shard assignment for shard {}; dropping document",
                    target_shard
                ));
            }
        }

        // Convert remote batches to the expected format
        for (node_id, docs) in remote_by_node {
            if let Some(addr) = peer_addrs.get(&node_id) {
                tracing::debug!(
                    node = %node_id,
                    count = docs.len(),
                    "Forwarding bulk write batch to remote node"
                );
                remote_batches.push((node_id, addr.clone(), docs));
            } else {
                errors.push(format!("No peer address for node {}", node_id));
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
                .map(|(node_id, addr, docs_for_remote)| async move {
                    self.forward_bulk_to_remote(node_id, &addr, index, docs_for_remote)
                        .await
                        .map(|items| (node_id, items))
                })
                .collect();

            let remote_results = join_all(remote_futures).await;

            for result in remote_results {
                match result {
                    Ok((_, items)) => {
                        written += items;
                    }
                    Err(e) => {
                        errors.push(format!("Remote forwarding failed: {}", e));
                    }
                }
            }
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

    /// Helper method to group local documents by shard
    async fn group_local_documents(
        &self,
        local_docs: Vec<(DocPayload, Option<String>, Uuid)>,
    ) -> Result<HashMap<Uuid, Vec<(DocPayload, Option<String>)>>, OrchestratorError> {
        let mut batches: HashMap<Uuid, Vec<(DocPayload, Option<String>)>> = HashMap::new();

        for (doc, routing_key, shard_id) in local_docs {
            batches
                .entry(shard_id)
                .or_default()
                .push((doc, routing_key));
        }

        Ok(batches)
    }

    async fn orch_search(
        &self,
        index: &str,
        query: &str,
        limit: usize,
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

        // Transform query to map shadow fields to canonical "id" field
        let transformed_query = transform_shadow_query(query, &schema);

        let shard_targets: Vec<(Uuid, MicroshardActor)> = self
            .shards
            .iter()
            .map(|(&shard_id, shard)| (shard_id, shard.clone()))
            .collect();
        let shard_searches = shard_targets.into_iter().map(|(shard_id, shard)| {
            let req = SearchRequest {
                index: index.to_string(),
                query: transformed_query.clone(),
                limit: Some(limit),
                sort: sort.cloned(),
            };
            async move { (shard_id, shard.handle_search(req).await) }
        });
        let shard_results: Vec<_> = futures::stream::iter(shard_searches)
            .buffer_unordered(self.max_concurrent_shard_searches.max(1))
            .collect::<Vec<_>>()
            .await;

        let mut results: Vec<(Uuid, f32, JsonValue)> = Vec::new();
        let mut errors = Vec::new();
        let mut shard_success = 0usize;
        let mut total_hits_sum = 0usize;
        for (shard_id, result) in shard_results {
            match result {
                Ok(r) => {
                    total_hits_sum += r.total_hits;
                    for hit in r.hits {
                        results.push((shard_id, hit.score, hit.doc));
                    }
                    shard_success += 1;
                }
                Err(err) => {
                    warn!(%shard_id, error = %err, "Scatter search shard failed");
                    errors.push(format!("Shard {}: {}", shard_id, err));
                }
            }
        }

        // Order merged results: by the requested sort field when provided, otherwise by
        // score descending. Each shard already returned field-sorted results, so a global
        // re-sort here interleaves them correctly across this node's shards. When sorting,
        // stamp the normalized `SORT_KEY_FIELD` first (while the full doc is present) so it
        // survives projection and lets the requesting node's cross-node merge re-order
        // these hits even when the sort field is not among the returned fields.
        match sort {
            Some(spec) => {
                stamp_sort_keys(&mut results, spec, &schema);
                results
                    .sort_by(|a, b| compare_hits_by_field(&a.2, &b.2, SORT_KEY_FIELD, spec.order))
            }
            None => {
                results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal))
            }
        }
        results.truncate(limit);
        let hits: Vec<JsonValue> = results
            .into_iter()
            .map(|(shard_id, score, mut doc)| {
                // Add metadata fields
                if let JsonValue::Object(ref mut o) = doc {
                    o.insert(
                        "_score".to_string(),
                        serde_json::Number::from_f64(score as f64)
                            .map(JsonValue::Number)
                            .unwrap_or(JsonValue::Null),
                    );
                    o.insert(
                        "shard_id".to_string(),
                        JsonValue::String(shard_id.to_string()),
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
        Ok(serde_json::json!({
            "hits": hits,
            "hits_returned": hits.len(),
            "total_hits": total_hits_sum,
            "limit": limit,
            "took_ms": start.elapsed().as_millis(),
            "stats": {
                "shards": {
                    "total": self.shards.len(),
                    "responded": shard_success,
                    "failed": errors.len()
                }
            },
            "errors": errors
        }))
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
                    let fields = Self::sorted_fields_map(&s);
                    return Ok(Self::schema_response(fields));
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
        }

        let mut all: HashMap<String, IndexTotals> = HashMap::new();
        let mut field_cache: HashMap<String, Vec<String>> = HashMap::new();

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
            } = totals;
            let total_size_bytes = tantivy_bytes + if include_data_size { redb_bytes } else { 0 };
            let index_size_mb = tantivy_bytes / (1024 * 1024);
            let memory_mb = (redb_bytes + tantivy_bytes) / (1024 * 1024);

            let mut json_obj = JsonMap::new();
            json_obj.insert("name".to_string(), JsonValue::String(name.clone()));
            json_obj.insert(
                "document_count".to_string(),
                JsonValue::from(document_count),
            );

            // Only include size fields when data size is requested
            if include_data_size {
                json_obj.insert(
                    "total_size_bytes".to_string(),
                    JsonValue::from(total_size_bytes),
                );
            }

            json_obj.insert("index_size_mb".to_string(), JsonValue::from(index_size_mb));
            json_obj.insert("memory_mb".to_string(), JsonValue::from(memory_mb));

            if include_data_size {
                json_obj.insert(
                    "data_size_mb".to_string(),
                    JsonValue::from(redb_bytes / (1024 * 1024)),
                );
            }
            json_obj.insert("shard_count".to_string(), JsonValue::from(shard_count));
            // Warmup coverage on this node: how many of the shards holding this index are
            // already serving from warm readers. Below shard_count means the first query
            // routed to a cold shard still pays the open-and-fault cost.
            json_obj.insert("warm_shards".to_string(), JsonValue::from(warm_shards));
            let fields = if let Some(cached) = field_cache.get(&name) {
                cached.clone()
            } else {
                match self.load_schema(&name).await {
                    Ok(schema) => {
                        let sorted = Self::sorted_field_names(&schema);
                        field_cache.insert(name.clone(), sorted.clone());
                        sorted
                    }
                    Err(_) => Vec::new(),
                }
            };
            json_obj.insert(
                "field_names".to_string(),
                JsonValue::Array(fields.into_iter().map(JsonValue::String).collect()),
            );

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

// Implement Drop to ensure read_runtime is properly shut down when NodeOrchestrator is destroyed.
// This prevents resource leaks by cleaning up the dedicated read thread pool.
impl Drop for NodeOrchestrator {
    fn drop(&mut self) {
        if let Some(read_runtime) = self.read_runtime.take() {
            tracing::info!("NodeOrchestrator: Shutting down dedicated read runtime");
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

    /// The core of finding #1: even when the sort field itself is projected away, the
    /// `_sort_key` metadata lets a cross-node merge interleave per-node blocks correctly.
    #[test]
    fn order_merged_hits_interleaves_nodes_by_sort_key_without_sort_field() {
        let spec = SortSpec {
            field: "year".to_string(),
            order: SortOrder::Desc,
        };
        // Two nodes, each already field-sorted + projected (no `year`), concatenated as
        // the merge would receive them: all of node A, then all of node B.
        let mut hits = vec![
            json!({"title": "a", "_sort_key": 2020}),
            json!({"title": "c", "_sort_key": 2018}),
            json!({"title": "d", "_sort_key": 2024}),
            json!({"title": "b", "_sort_key": 2022}),
        ];
        order_merged_hits(&mut hits, Some(&spec));
        assert_eq!(titles(&hits), vec!["d", "b", "a", "c"]);
    }

    #[test]
    fn order_merged_hits_ascending_and_missing_key_sorts_last() {
        let spec = SortSpec {
            field: "year".to_string(),
            order: SortOrder::Asc,
        };
        let mut hits = vec![
            json!({"title": "b", "_sort_key": 2022}),
            json!({"title": "missing"}), // no `_sort_key` → sorts last
            json!({"title": "a", "_sort_key": 2018}),
        ];
        order_merged_hits(&mut hits, Some(&spec));
        assert_eq!(titles(&hits), vec!["a", "b", "missing"]);
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
}
