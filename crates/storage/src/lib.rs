//! # Multi-Tenant Hybrid Storage Engine - CameoDB
//!
//! This crate provides a production-grade multi-tenant hybrid storage engine that combines:
//! - **redb**: ACID-compliant shared key-value storage for durability and consistency
//! - **tantivy**: Per-index full-text search indexing for query capabilities
//!
//! ## Multi-Tenant Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────┐
//! │           HybridStore                   │
//! ├─────────────────────────────────────────┤
//! │ Shared redb Database                    │
//! │ ├── data_index1 table                   │
//! │ ├── wal_index1 table                    │
//! │ ├── data_index2 table                   │
//! │ ├── wal_index2 table                    │
//! │ └── schema table (shared)               │
//! │                                         │
//! │ Per-Index Tantivy Indices               │
//! │ ├── indices/index1/                     │
//! │ └── indices/index2/                     │
//! └─────────────────────────────────────────┘
//! ```

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use tantivy::doc;
use tantivy::query::QueryParserError;
use thiserror::Error;

mod analysis;
mod query;
mod schema;
mod search;
mod store;

#[cfg(test)]
mod tests;

// The crate's public API, named. Before these lists, `lib.rs` re-exported `query`, `schema` and
// `store` whole, so any `pub` added in those modules widened the crate's API with no review step
// (O4). Now widening it is an edit here: an item a caller outside the crate needs goes in the
// list, on purpose, and everything else stays the crate's own business.
pub use query::{FieldReference, field_references};
pub use schema::{
    DropsCounted, FieldDef, IndexSchema, SchemaChange, SchemaFieldUpdate, SchemaFields,
    SchemaRecord, SchemaState, SchemaVersion, TantivyFieldType, WalOp, byte_value_error,
    count_drops, date_sort_secs, document_key_field, facet_path_error, is_date_value,
    parse_date_to_timestamp_secs, select_default_fields,
};
pub use store::HybridStore;
// `SchemaFields` is in the list because `HybridStore::get_or_create_index` returns it, not because
// a caller reads it: its fields are crate-private, so outside the crate it is a handle to hold and
// drop. Narrowing the type makes that signature a `private_interfaces` warning, which the
// `-D warnings` clippy gate turns into a failure — so this line is held in place.

// Inside the crate every module reaches its siblings through `crate::*`, so these stay globs —
// scoped to the crate and no wider, which is what makes the lists above the whole public surface.
pub(crate) use analysis::*;
pub(crate) use query::*;
pub(crate) use schema::*;
pub(crate) use search::*;
pub(crate) use store::*;

/// Sort specification for search results
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SortSpec {
    /// Field name to sort by
    pub field: String,
    /// Sort order (default: Asc)
    #[serde(default)]
    pub order: SortOrder,
}

/// Sort order direction
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SortOrder {
    #[default]
    Asc,
    Desc,
}

/// Configuration for the multi-tenant hybrid storage engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// The root folder for this shard's data files.
    pub shard_path: PathBuf,

    /// Largest number of indexes this **node** may hold open at once; `0` means no cap.
    ///
    /// Divided by the shard count in `HybridStore::new`, the same way
    /// `total_memory_limit_bytes` is, because the number that matters to an operator is the
    /// node's and the number that matters to a shard is its share.
    ///
    /// A count rather than a byte budget, and that is the whole point. An open index costs an
    /// indexing arena *and* `indexer_num_threads + merge_num_threads` OS threads — three, at
    /// the defaults. A cap expressed in megabytes would bound the first and leave the second
    /// growing with however many index names the workload touches, which on a node whose
    /// tenants choose their own names is not a number this process gets to pick. Capping the
    /// count bounds both.
    pub max_open_indexes: usize,

    // Memory Budget Configuration
    /// Default memory budget for each tantivy IndexWriter in bytes.
    pub indexer_memory_budget: usize,
    /// Minimum memory budget for IndexWriter in MB.
    pub indexer_memory_min_mb: usize,
    /// Maximum memory budget for IndexWriter in MB.
    pub indexer_memory_max_mb: usize,

    /// Total memory limit available to this node (across all shards) in bytes.
    /// Used to derive per-shard cache sizing without probing the host OS each time.
    pub total_memory_limit_bytes: u64,
    /// Percentage of the total memory limit considered safe for cache allocations (0-100).
    pub memory_pressure_threshold_percent: u8,

    // Thread Configuration
    /// Number of indexing worker threads per tantivy IndexWriter.
    /// Each worker creates one segment per commit. Default: 1 (optimal for
    /// CameoDB's single-writer-thread-per-shard architecture).
    pub indexer_num_threads: usize,
    /// Number of background merge (compaction) threads per tantivy IndexWriter.
    /// Controls how many segment merges run concurrently. Tantivy default is 4,
    /// but on memory-constrained nodes with many indices this causes mmap storms.
    /// Default: 1. Scale up on nodes with ample RAM and high write throughput.
    pub merge_num_threads: usize,

    // Other Configuration
    /// Default batch size for smart commit calculations.
    pub default_batch_size: usize,
    /// Whether to call fsync() on every redb commit.
    pub wal_sync: bool,
    /// How long, in milliseconds, an index's oldest uncommitted write may wait for the commit
    /// that makes it searchable, while writes keep arriving; `0` commits by count alone.
    ///
    /// A commit is what makes a write visible to search, and it is the expensive half of a
    /// write: Tantivy waits for its indexer to flush a segment and syncs every file of it. By
    /// count alone — the threshold derived from `default_batch_size` — a bulk load crosses the
    /// threshold on nearly every drain and pays that on every one, while a steady trickle that
    /// never reaches it is not searchable until it does. Measured by time, the first costs one
    /// commit per interval however fast documents arrive, and the second is bounded too.
    ///
    /// With an interval the count still applies, twenty times higher, as a backstop bounding
    /// the WAL tail a restart replays. The idle commit after the last write is the node's, not
    /// this crate's: see `supervisor_timeout_secs`. `0` here is the count-only policy the
    /// library's own tests are written against; the node sets `[search] commit_interval_ms`.
    #[serde(default)]
    pub commit_interval_ms: u64,

    /// How much one query may ask of the index: the node's query-cost policy, grouped so a new
    /// bound is one field here rather than one more line in every place a config is built.
    #[serde(default)]
    pub query: QueryPolicy,
}

/// What a query may cost, decided by the node and applied where the query is rewritten.
///
/// The library default imposes nothing — every bound `0`, every expansion off — because policy is
/// the node's to set; `[security.limits]` supplies the defaults a deployment actually runs with.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct QueryPolicy {
    /// Shortest prefix a `field:pre*` clause is expanded for, in characters; `0` expands any.
    ///
    /// A prefix is rewritten into a range over the term dictionary, and the range's cost is the
    /// number of distinct terms it covers, with no ceiling: one character on a field of hashes
    /// covers a sixteenth of every term the field holds. A shorter prefix is not expanded — it
    /// matches the term as written, with a note saying so.
    pub min_prefix_length: usize,

    /// Whether a prefix naming no field — a bare `pre*` — is expanded across the default fields.
    ///
    /// Off, tantivy drops the `*` and matches `pre` as a term, and the caller is told. On, it
    /// becomes one prefix range per text default field, OR'd — the same fields an unqualified term
    /// searches, so its cost is a prefix's cost times their number.
    pub expand_unqualified_prefix: bool,

    /// Most fields an unqualified term searches; `0` searches every one.
    ///
    /// Every indexed text, string and JSON field is a default field, and an unqualified term is
    /// one clause per default field — so its cost grows with the schema's width, which the
    /// tenant chooses. Measured (ROADMAP M8), worse than linearly: when the fields share a
    /// vocabulary, doubling them multiplies the cost by about four. Past the cap the first
    /// `max_default_fields` are searched, by name, or by the index's declared `default_fields`
    /// order; see [`select_default_fields`].
    pub max_default_fields: usize,
}

/// The results of a search, and the clauses that did not survive parsing.
///
/// Tantivy parses leniently: a clause it cannot interpret is dropped and the rest of the query
/// executes. In a conjunction that widens the result set; in a negation it disables the
/// exclusion. Neither is visible in the hits, so a caller that needs the query to have meant
/// what it said checks [`discarded`](Self::discarded) before trusting the rows.
#[derive(Debug, Clone, Default)]
pub struct SearchOutcome {
    /// Matching documents, ordered by relevance or by the requested sort field.
    pub hits: Vec<(f32, JsonValue)>,
    /// Total matches, which exceeds `hits.len()` when a limit applied.
    pub total_hits: usize,
    /// One entry per dropped clause, phrased for the caller. Empty on a clean parse.
    pub discarded: Vec<String>,
    /// The sorted field, when the order returned is an approximation of the one asked for.
    ///
    /// Set only for a text or string sort on a field with no fast column: those candidates are
    /// taken by relevance and alphabetised afterwards, so the answer is the alphabetical order
    /// of the top-scoring `limit * 2` rather than of everything that matched. Separate from
    /// [`Self::discarded`] because the two mean different things and a caller acts on them
    /// differently — a discarded clause means the query that ran is not the one written, while
    /// this means the query ran as written and the *order* is a sample.
    ///
    /// `None` whenever the order is exact, which is every other sort and every unsorted search.
    pub approximate_sort: Option<String>,
    /// The default fields a term with no field in front of it searched, when the node's
    /// `max_default_fields` cut them short.
    ///
    /// Set only when both hold: the cap narrowed this index's default fields, and the query
    /// actually had an unqualified clause — a query naming every field it searches reached them
    /// all, and saying otherwise would be noise. Separate from [`Self::discarded`] for the
    /// reason [`Self::approximate_sort`] is: nothing was dropped, the query ran as the node's
    /// policy defines it, and a caller that reads a discarded clause as "this is not the query I
    /// wrote" — as the MCP tools do, refusing the search — would be wrong to here.
    pub narrowed_default_fields: Option<NarrowedDefaultFields>,
    /// Every clause was discarded, so the query that ran was empty and matched nothing.
    ///
    /// Distinct from a `discarded` list that still left something to run: those hits answer a
    /// different question, while these are not an answer at all. The zero reported alongside
    /// this says nothing about the data — read as "no document matches", it is a claim about a
    /// query that never ran.
    pub emptied: bool,
}

impl SearchOutcome {
    /// No hits and nothing dropped: an index with no committed segments.
    fn empty() -> Self {
        Self::default()
    }

    /// A count with no documents attached, for `limit = 0`.
    ///
    /// Never approximate: a count is over every match, and no order was produced to approximate.
    fn counted(total_hits: usize, discarded: Vec<String>, emptied: bool) -> Self {
        Self {
            hits: Vec::new(),
            total_hits,
            discarded,
            approximate_sort: None,
            narrowed_default_fields: None,
            emptied,
        }
    }
}

/// Which default fields an unqualified term searched, out of how many it could have.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NarrowedDefaultFields {
    /// The fields searched, in the order they were chosen: the index's declared `default_fields`
    /// order, or by name.
    pub searched: Vec<String>,
    /// How many default fields the index has — or, with a declared list, how many it lists —
    /// before the cap.
    pub available: usize,
    /// Whether the fields came from the index's declared `default_fields` rather than by name.
    pub declared: bool,
}

/// What parsing a query against an index found, without running it.
///
/// The two lists are separate because they fail differently. A syntax error means the query is
/// malformed and the parser recovered by dropping something; a discarded clause parsed fine and
/// simply cannot match. An agent can fix the first from the message alone, while the second
/// usually means looking at the schema.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueryValidation {
    /// The query as the engine parses it, after date, facet and prefix normalization.
    ///
    /// Worth returning even when nothing is wrong: a query is rewritten before it runs, and the
    /// rewrite is where a surprising result usually comes from.
    pub normalized_query: String,
    /// Malformed syntax, in the parser's own words, including the position it reached.
    ///
    /// This is the case a structural check cannot reach — a query whose quotes and parentheses
    /// balance and which still does not parse.
    pub syntax_errors: Vec<String>,
    /// Clauses that parse but cannot match: unknown fields, fields that are not indexed,
    /// constructs the parser does not support. The same notes a search reports as discarded.
    pub discarded: Vec<String>,
}

impl QueryValidation {
    /// Whether the query runs, and every clause in it can match something.
    pub fn is_valid(&self) -> bool {
        self.syntax_errors.is_empty() && self.discarded.is_empty()
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        const DEFAULT_TOTAL_MEMORY_LIMIT_MB: u64 = 1024;
        const DEFAULT_MEMORY_PRESSURE_THRESHOLD_PERCENT: u8 = 80;
        Self {
            shard_path: PathBuf::from("/var/tmp/cameodb"),
            max_open_indexes: 0,

            // Memory Budget Configuration
            indexer_memory_budget: 64 * 1024 * 1024,
            indexer_memory_min_mb: 32,
            indexer_memory_max_mb: 512,
            total_memory_limit_bytes: DEFAULT_TOTAL_MEMORY_LIMIT_MB * 1024 * 1024,
            memory_pressure_threshold_percent: DEFAULT_MEMORY_PRESSURE_THRESHOLD_PERCENT,

            // Thread Configuration
            indexer_num_threads: 1,
            merge_num_threads: 2,

            // Other Configuration
            default_batch_size: 1000,
            wal_sync: true,
            commit_interval_ms: 0,
            query: QueryPolicy::default(),
        }
    }
}

impl StorageConfig {
    /// Calculate optimal memory budget based on index size with consistent linear scaling.
    ///
    /// Scaling algorithm:
    /// - 0-100MB index    → 32MB (min)
    /// - 101-500MB index  → 64MB (default)
    /// - 501-2000MB index → 128MB (4x min)
    /// - 2001-8000MB index → 256MB (8x min)
    /// - >8000MB index    → 512MB (max)
    ///
    /// Field-count awareness: Schemas with many indexed fields require more memory
    /// for segment building (each field has its own postings writer and fast-field writer).
    /// If field_count is provided, scales budget by 1.25x for >50 fields, 1.5x for >100 fields.
    pub fn get_optimal_memory_budget(
        &self,
        index_path: &Path,
        field_count: Option<usize>,
    ) -> usize {
        let min_budget_bytes = self.indexer_memory_min_mb * 1024 * 1024;
        let max_budget_bytes = self.indexer_memory_max_mb * 1024 * 1024;
        let default_budget_bytes = self.indexer_memory_budget;

        // Check index size and adjust budget dynamically within configurable range
        let size_based_budget = if let Some(index_bytes) = index_size_bytes(index_path) {
            let size_mb = index_bytes / (1024 * 1024);
            let optimal_budget = match size_mb {
                0..=100 => min_budget_bytes,         // 32MB - very small
                101..=500 => default_budget_bytes,   // 64MB - small
                501..=2000 => min_budget_bytes * 4,  // 128MB - medium
                2001..=8000 => min_budget_bytes * 8, // 256MB - large
                _ => max_budget_bytes,               // 512MB - very large
            };

            // Ensure result is within configured bounds
            optimal_budget.max(min_budget_bytes).min(max_budget_bytes)
        } else {
            // New index, use minimum budget (starting point will scale as data is written)
            min_budget_bytes
        };

        // Apply field-count scaling if provided
        if let Some(fc) = field_count {
            let field_multiplier = if fc > 100 {
                1.5
            } else if fc > 50 {
                1.25
            } else {
                1.0
            };
            let field_adjusted = (size_based_budget as f64 * field_multiplier) as usize;
            field_adjusted.min(max_budget_bytes)
        } else {
            size_based_budget
        }
    }
}

/// Statistics for an index.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexStats {
    pub document_count: u64,
    pub total_size_bytes: u64,
    pub tantivy_index_exists: bool,
}

/// Per-index statistics gathered from a single shard.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct IndexShardStats {
    pub document_count: u64,
    pub redb_bytes: u64,
    pub tantivy_bytes: u64,
    pub tantivy_index_exists: bool,
    pub tantivy_scan_ms: u128,
    /// Where this index sits in the startup warmup lifecycle. `Cold` means the first query
    /// will pay the open-and-fault cost; `Warm` means it is served from warm buffers.
    pub warmup_state: IndexWarmupState,
    /// Field names the built Tantivy index actually has a column for, on this shard.
    ///
    /// Distinct from the schema's `indexed` flag, which is a *declaration*. A field declared
    /// after the index was built has no column until the index data is rebuilt from the schema,
    /// so it is `indexed` and yet matches nothing — the state `PATCH /_schema` reports as
    /// `pending_reindex`. Nothing above the engine can tell the two apart, which is why this is
    /// gathered here rather than inferred by a caller.
    ///
    /// Empty when the index has not been built on this shard.
    pub searchable_fields: HashSet<String>,
    /// Field names the built Tantivy index has a *fast column* for, on this shard.
    ///
    /// The same distinction as `searchable_fields`, applied to sorting: `fast` in the schema is a
    /// declaration, and the column it asks for is written at index time. See
    /// [`HybridStore::sortable_fields`].
    ///
    /// Defaulted because this type crosses the cluster wire: a peer running an older build sends
    /// no such field, and reporting nothing sortable there is better than failing to decode its
    /// statistics at all.
    #[serde(default)]
    pub sortable_fields: HashSet<String>,
}

/// Timing metadata for shard-level statistics gathering.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ShardStatsTimings {
    pub redb_ms: u128,
    pub tantivy_ms: u128,
    pub total_ms: u128,
}

/// Snapshot of all index stats within a shard along with timing info.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ShardStatsSnapshot {
    pub per_index: HashMap<String, IndexShardStats>,
    pub timings: ShardStatsTimings,
}

/// Comprehensive error types for storage engine operations.
#[derive(Debug, Error)]
pub enum StoreError {
    #[error("redb error: {0}")]
    Redb(#[from] redb::Error),

    #[error("redb database error: {0}")]
    Database(#[from] redb::DatabaseError),

    #[error("redb transaction error: {0}")]
    Transaction(#[from] redb::TransactionError),

    #[error("redb table error: {0}")]
    Table(#[from] redb::TableError),

    #[error("redb storage error: {0}")]
    Storage(#[from] redb::StorageError),

    #[error("redb commit error: {0}")]
    Commit(#[from] redb::CommitError),

    #[error("redb durability error: {0}")]
    Durability(#[from] redb::SetDurabilityError),

    #[error("tantivy error: {0}")]
    Tantivy(#[from] tantivy::TantivyError),

    /// An index whose contents contradict the engine's own bookkeeping — e.g. a document the
    /// `_seq` fast field orders but whose stored fields cannot read the value. Not the
    /// caller's fault and not fixable by retrying: a checkpoint reconstructed from nothing
    /// seeds a replay window from a number nobody can trust, so the read that finds this
    /// fails rather than inventing one.
    #[error("corrupt index state: {0}")]
    CorruptIndex(String),

    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("field not found: {0}")]
    FieldNotFound(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("query parser error: {0}")]
    QueryParser(#[from] QueryParserError),

    #[error("index not found: {0}")]
    IndexNotFound(String),

    /// A write whose index writer panicked and was reset. The document was not applied and the
    /// writer has been dropped so the next write rebuilds it, so the operation is safe to retry.
    /// Its own variant so the writer thread can reply it in place of an unwind that would end the
    /// thread, and so the HTTP layer can answer a retriable failure rather than a bad request.
    #[error("index writer for '{0}' panicked and was reset; retry the write")]
    WriterPanicked(String),

    /// A write whose index was closed by eviction every time it was reopened for it. Nothing
    /// was applied — the writer is confirmed live before a sequence is reserved — so the
    /// operation is safe to retry. Needs the open-index cap to evict this index repeatedly
    /// within one write, so it is a busy shard's answer rather than a fault.
    #[error("index '{0}' was closed while a write was opening it; retry the write")]
    WriterClosed(String),

    #[error("invalid index name: {0}")]
    InvalidIndexName(String),

    /// A document value the field's type cannot hold. The caller's fault, not the node's, which
    /// is why it is its own variant rather than an `Io` — the HTTP layer answers `400` on the
    /// `InvalidInput` kind and would otherwise call a bad document an internal error.
    #[error("invalid value for field '{field}': {reason}")]
    InvalidFieldValue { field: String, reason: String },
}

/// Where an index sits in the startup warmup lifecycle.
///
/// Queries are served in every state — a cold index just pays the open-and-fault cost on
/// the first query instead of having paid it in the background.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IndexWarmupState {
    /// Not yet touched. The first query opens and faults in everything it needs.
    Cold,
    /// Replaying WAL entries that were not committed to Tantivy before the last shutdown.
    /// Searches against this index can miss the uncommitted tail until replay finishes.
    Recovering,
    /// Reader is being opened and its segment structures faulted in.
    Warming,
    /// Reader cached and every segment warmed. Queries hit warm buffers.
    Warm,
    /// Warmup failed; the index still works, the first query just pays the cold cost.
    Failed,
}

/// Result of the blocking recovery phase, and the work handed to the background phase.
#[derive(Debug, Clone, Default)]
pub struct WarmupPlan {
    /// Indices that had uncommitted WAL entries and were replayed synchronously.
    pub recovered: Vec<String>,
    /// Indices whose recovery failed. They are excluded from warmup and will retry on
    /// first access.
    pub failed: Vec<String>,
    /// Indices to warm in the background, ordered smallest first.
    pub pending_warmup: Vec<String>,
}

/// What a single index's warmup accomplished.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexWarmupStats {
    pub index: String,
    /// Segments in the searcher that was warmed.
    pub segments: usize,
    /// Segments actually warmed by this call. Zero means the searcher generation was already
    /// warm and the call was a no-op.
    pub segments_warmed: usize,
    /// Searcher generation this call observed. Tantivy mints a new one on every
    /// `IndexReader::reload()`, and readers here reload only from `commit_index`, so this
    /// changes exactly once per commit.
    pub generation: u64,
    pub num_docs: u64,
    pub elapsed_ms: u128,
}
