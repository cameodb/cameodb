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

use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, LazyLock, Mutex};
use std::time::{Duration, Instant};

use chrono::{NaiveDate, NaiveDateTime, SecondsFormat, TimeZone, Utc};
use dashmap::DashMap;
use redb::{
    Database, Durability, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition,
};
use serde::{Deserialize, Serialize, de::Error as DeserializeError};
use serde_json::Map as JsonMap;
use serde_json::Value as JsonValue;
use tantivy::collector::TopDocs;
use tantivy::query::{AllQuery, QueryParserError};
use tantivy::schema::{
    Document, FAST, Facet, Field, INDEXED, STORED, STRING, Schema, TEXT, Value as TantivyValue,
};
use tantivy::{DateTime, Index, IndexReader, IndexWriter, Order, doc};
use thiserror::Error;
use tracing::{debug, trace, warn};
use walkdir::WalkDir;
use xxhash_rust::xxh3::xxh3_64;

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

/// Wrapper to handle both sorted (u64) and unsorted (f32) search results
enum SearchResult {
    Unsorted(Vec<(f32, tantivy::DocAddress)>),
    Sorted(Vec<(Option<u64>, tantivy::DocAddress)>),
}

const TANTIVY_DATA_FILE_EXTENSIONS: &[&str] = &["store", "fast", "idx", "doc", "pos", "term"];

/// Tantivy DateTime safe range limits (to avoid i64 overflow during nanosecond conversion)
/// DateTime::from_timestamp_secs() multiplies by 1_000_000_000, so safe range is:
/// i64::MIN / 1_000_000_000 to i64::MAX / 1_000_000_000
const TANTIVY_MIN_TIMESTAMP_SECS: i64 = -9_223_372_036; // 1677-09-21 00:12:44 UTC
const TANTIVY_MAX_TIMESTAMP_SECS: i64 = 9_223_372_036; // 2262-04-11 23:47:16 UTC

/// Common naive datetime and date formats used for inference and normalization
const NAIVE_DATETIME_FORMATS: &[&str] = &[
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M",
    "%Y-%m-%d %H:%M:%S%.f",
    "%Y-%m-%dT%H:%M:%S%.f",
    // Slash separator for date part
    "%Y/%m/%d %H:%M:%S",
    "%Y/%m/%d %H:%M",
    "%Y/%m/%dT%H:%M:%S",
    "%Y/%m/%dT%H:%M",
    // Dot separator for date part
    "%Y.%m.%d %H:%M:%S",
    "%Y.%m.%d %H:%M",
    "%Y.%m.%dT%H:%M:%S",
    "%Y.%m.%dT%H:%M",
];

const NAIVE_DATE_FORMATS: &[&str] = &["%Y-%m-%d", "%Y/%m/%d", "%Y.%m.%d", "%Y%m%d", "%Y-%m", "%Y"];

/// Schema metadata table: maps index names to their schema definitions.
const TABLE_SCHEMA: TableDefinition<&str, &[u8]> = TableDefinition::new("schema");

/// Recovery metadata table: maps index names to their last committed Tantivy sequence.
///
/// Written in the same transaction that truncates the WAL after a successful Tantivy commit.
/// Since the checkpoint moved into Tantivy's own commit payload this is a fallback rather
/// than the primary record: it is what tells recovery where an index stands when that index
/// was last committed by a build that predates the payload stamp. Nothing on the boot path
/// reads it for an index whose WAL is empty.
const TABLE_RECOVERY_META: TableDefinition<&str, u64> = TableDefinition::new("_recovery_meta");

/// Tag byte introducing a WAL entry that records only the document id.
///
/// The WAL exists to say *which documents* Tantivy may be behind on, and it is written in the
/// same redb transaction as the `data_<index>` row for that document. The row is therefore
/// always there to be read at replay time, and storing the document a second time in the WAL
/// bought nothing while doubling the bytes every write serialises and fsyncs.
///
/// Recovery reads the id, looks it up in `data_<index>`, and lets the answer decide the
/// operation: a row means the document should be indexed as it now stands, no row means it was
/// deleted. That is not a weaker record than the old one — replay converges on the committed
/// state of each id rather than re-enacting a log, so a put later overwritten, or deleted, in
/// the same tail resolves in one step instead of several.
///
/// `0x01` cannot begin a legacy entry: those are JSON objects, so they begin with `{`.
const WAL_ENTRY_ID_ONLY: u8 = 0x01;

/// Encode a WAL entry: the tag byte followed by the document id.
fn encode_wal_entry(id: &str) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(id.len() + 1);
    encoded.push(WAL_ENTRY_ID_ONLY);
    encoded.extend_from_slice(id.as_bytes());
    encoded
}

/// Read the document id out of a WAL entry, in either format.
///
/// Entries written before the id-only format are whole `WalOp` JSON values. They still decode
/// here — only their id is taken, and the document body they carry is ignored in favour of the
/// `data_<index>` row, so one replay path serves both formats and an upgrade needs no migration
/// of a WAL tail left behind by the previous build.
fn decode_wal_entry(bytes: &[u8]) -> Result<String, StoreError> {
    if let Some((&WAL_ENTRY_ID_ONLY, id_bytes)) = bytes.split_first() {
        return std::str::from_utf8(id_bytes)
            .map(str::to_string)
            .map_err(|e| {
                StoreError::Serialization(format!("WAL entry id is not valid UTF-8: {e}"))
            });
    }

    let legacy: WalOp =
        serde_json::from_slice(bytes).map_err(|e| StoreError::Serialization(e.to_string()))?;
    Ok(match legacy {
        WalOp::Put { id, .. } => id,
        WalOp::Delete { id } => id,
    })
}

/// Prefix of the string cameodb stamps into every Tantivy commit payload. The number after
/// it is the `wal_<index>` sequence that commit covers.
///
/// Tantivy writes the payload into `meta.json` as part of the commit itself, which is the
/// whole reason the checkpoint lives there. A checkpoint kept anywhere else is a second
/// write that can be interrupted, and the two orderings fail differently: recorded too early
/// it claims documents Tantivy never got, recorded too late it forces a replay of a tail
/// Tantivy already has. Inside the commit there is no window at all — if the segments are on
/// disk, so is the sequence that describes them.
const CHECKPOINT_PAYLOAD_PREFIX: &str = "cameodb:wal_seq=";

fn encode_checkpoint_payload(seq: u64) -> String {
    format!("{CHECKPOINT_PAYLOAD_PREFIX}{seq}")
}

fn decode_checkpoint_payload(payload: &str) -> Option<u64> {
    payload
        .strip_prefix(CHECKPOINT_PAYLOAD_PREFIX)?
        .parse()
        .ok()
}

/// Commit `writer`, recording `seq` as the WAL sequence the commit covers.
///
/// Every Tantivy commit in the process goes through here. One that skips the stamp leaves
/// the checkpoint behind on an index that is in fact up to date, and the next boot pays for
/// it by replaying a tail that is already indexed.
fn commit_writer_at(writer: &mut IndexWriter, seq: u64) -> Result<(), StoreError> {
    let mut prepared = writer.prepare_commit()?;
    prepared.set_payload(&encode_checkpoint_payload(seq));
    prepared.commit()?;
    Ok(())
}

/// The WAL sequence recorded in `tantivy_index`'s last commit.
///
/// Reads `meta.json` and nothing else, so the cost does not move with the size of the index.
/// `None` means the last commit predates the stamp; the caller resolves those from redb.
fn tantivy_checkpoint_seq(tantivy_index: &Index) -> Option<u64> {
    tantivy_index
        .load_metas()
        .ok()?
        .payload
        .as_deref()
        .and_then(decode_checkpoint_payload)
}

/// How long startup warmup may spend faulting in segment structures before it gives up on
/// the indices it has not reached. Indices are warmed smallest-first, so the budget buys the
/// largest number of warm indices it can and leaves the rest to warm on demand.
const WARMUP_BUDGET: Duration = Duration::from_secs(60);

/// Counting semaphore bounding how many indices replay their WAL tail at once, across every
/// shard in the process.
///
/// Replay needs an `IndexWriter`, and an `IndexWriter` is worker threads plus an indexing
/// arena that reaches hundreds of megabytes on a large index. Recovery is driven per shard
/// and every shard on a node starts at the same moment, so a per-shard limit silently
/// multiplies by the shard count — the 16-shard node that was just killed for using too much
/// memory would answer by allocating 16 × cores arenas to recover from it. The cap is global
/// for that reason.
struct RecoveryGate {
    permits: Mutex<usize>,
    released: Condvar,
}

impl RecoveryGate {
    fn acquire(&self) -> RecoveryPermit<'_> {
        let mut permits = self
            .permits
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while *permits == 0 {
            permits = self
                .released
                .wait(permits)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
        *permits -= 1;
        RecoveryPermit { gate: self }
    }
}

struct RecoveryPermit<'a> {
    gate: &'a RecoveryGate,
}

impl Drop for RecoveryPermit<'_> {
    fn drop(&mut self) {
        let mut permits = self
            .gate
            .permits
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *permits += 1;
        self.gate.released.notify_one();
    }
}

/// Four concurrent replays is enough to keep the disk busy without letting the arenas add up
/// to something the node cannot hold. It bounds a path that only runs at boot, and only for
/// the handful of indices that were mid-write when the process stopped.
static RECOVERY_GATE: LazyLock<RecoveryGate> = LazyLock::new(|| RecoveryGate {
    permits: Mutex::new(
        std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(4)
            .clamp(1, 4),
    ),
    released: Condvar::new(),
});

/// Configuration for the multi-tenant hybrid storage engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// The root folder for this shard's data files.
    pub shard_path: PathBuf,

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
}

/// Convert a date literal into RFC3339 Z string using the same rules as indexing.
/// Returns None if the literal cannot be parsed as a date.
fn normalize_date_literal(lit: &str) -> Option<String> {
    if lit == "*" {
        return None;
    }

    // Strip surrounding quotes (e.g. ""2026.07.01"" -> "2026.07.01")
    let stripped = lit.trim_matches('"');
    let (_, _, clamped) = parse_date_str_to_tantivy(stripped)?;
    let dt = Utc.timestamp_opt(clamped, 0).single()?;
    Some(dt.to_rfc3339_opts(SecondsFormat::Secs, true))
}

/// Rewrite both bounds of a date range to RFC3339.
///
/// Accepts either delimiter on either side — `[a TO b]`, `{a TO b}`, and the mixed pairs — and
/// preserves them, since they carry the inclusive/exclusive meaning.
fn normalize_date_ranges(input: &str, field: &str) -> String {
    let prefix = format!("{}:", field);
    let mut out = String::with_capacity(input.len());
    let mut idx = 0usize;

    while let Some(rel) = input[idx..].find(&prefix) {
        let start = idx + rel;
        out.push_str(&input[idx..start]);
        let after_colon = start + prefix.len();

        // The opening delimiter decides whether this is a range at all.
        let open = input[after_colon..].chars().next();
        let Some(open) = open.filter(|ch| *ch == '[' || *ch == '{') else {
            out.push_str(&input[start..after_colon]);
            idx = after_colon;
            continue;
        };

        let inner_start = after_colon + open.len_utf8();
        // Either closing form may terminate the range, so take whichever comes first.
        let close_rel = input[inner_start..]
            .find([']', '}'])
            .map(|rel| (rel, input[inner_start + rel..].chars().next().unwrap()));

        if let Some((end_rel, close)) = close_rel {
            let end = inner_start + end_rel;
            let inner = &input[inner_start..end];

            if let Some((lower, upper)) = inner.split_once(" TO ") {
                let lower_norm = normalize_date_literal(lower).unwrap_or_else(|| lower.to_string());
                let upper_norm = normalize_date_literal(upper).unwrap_or_else(|| upper.to_string());
                out.push_str(&format!(
                    "{}:{}{} TO {}{}",
                    field, open, lower_norm, upper_norm, close
                ));
                idx = end + close.len_utf8();
                continue;
            }
        }

        // No closing delimiter, or no ` TO ` inside it: not a range we can rewrite. Copy the
        // field prefix and the opening delimiter and carry on from there.
        out.push_str(&input[start..inner_start]);
        idx = inner_start;
    }

    out.push_str(&input[idx..]);
    out
}

/// Rewrite every element of a date set query — `field: IN [a b c]` — to RFC3339.
///
/// Whitespace is allowed around `IN` and after the colon, so this shape is not reachable by the
/// single-literal pass, which reads a value up to the next whitespace.
fn normalize_date_in_sets(input: &str, field: &str) -> String {
    let prefix = format!("{}:", field);
    let mut out = String::with_capacity(input.len());
    let mut idx = 0usize;

    /// Bytes of leading whitespace at `from`, so the cursor can step over it.
    fn space_at(input: &str, from: usize) -> usize {
        input[from..].len() - input[from..].trim_start().len()
    }

    while let Some(rel) = input[idx..].find(&prefix) {
        let start = idx + rel;
        out.push_str(&input[idx..start]);
        let after_colon = start + prefix.len();

        // Walk forward from the colon with one cursor: optional space, `IN`, optional space,
        // `[`, elements, `]`. Anything else is not a set query and is copied through.
        let mut cursor = after_colon + space_at(input, after_colon);
        if !input[cursor..].starts_with("IN") {
            out.push_str(&input[start..after_colon]);
            idx = after_colon;
            continue;
        }
        cursor += "IN".len();
        cursor += space_at(input, cursor);
        if !input[cursor..].starts_with('[') {
            out.push_str(&input[start..after_colon]);
            idx = after_colon;
            continue;
        }
        cursor += '['.len_utf8();

        let Some(close_rel) = input[cursor..].find(']') else {
            out.push_str(&input[start..after_colon]);
            idx = after_colon;
            continue;
        };

        // Quoted for the same reason as a bare literal: RFC3339 carries colons, which the
        // grammar would otherwise read as a field separator inside the set.
        let normalized: Vec<String> = input[cursor..cursor + close_rel]
            .split_whitespace()
            .map(|element| match normalize_date_literal(element) {
                Some(norm) => format!("\"{norm}\""),
                None => element.to_string(),
            })
            .collect();
        out.push_str(&format!("{}: IN [{}]", field, normalized.join(" ")));
        idx = cursor + close_rel + ']'.len_utf8();
    }

    out.push_str(&input[idx..]);
    out
}

fn normalize_date_comparisons(input: &str, field: &str) -> String {
    let prefix = format!("{}:", field);
    let mut out = String::with_capacity(input.len());
    let mut idx = 0usize;

    while let Some(rel) = input[idx..].find(&prefix) {
        let start = idx + rel;
        out.push_str(&input[idx..start]);

        let op_idx = start + prefix.len();
        let rest = &input[op_idx..];
        let mut chars = rest.chars();
        if let Some(op) = chars.next()
            && (op == '<' || op == '>')
        {
            // Check for compound operators >= and <=
            let (full_op, op_len) = if chars.next() == Some('=') {
                (format!("{}=", op), op.len_utf8() + 1)
            } else {
                (op.to_string(), op.len_utf8())
            };
            let value_start = op_idx + op_len;
            // If the value is quoted, find the closing quote as the boundary.
            // Otherwise, use whitespace as the boundary.
            let value_end = if input[value_start..].starts_with('"') {
                input[value_start + 1..]
                    .find('"')
                    .map(|r| value_start + 1 + r + 1)
                    .unwrap_or(input.len())
            } else {
                input[value_start..]
                    .find(char::is_whitespace)
                    .map(|r| value_start + r)
                    .unwrap_or(input.len())
            };
            let value = &input[value_start..value_end];
            let norm = normalize_date_literal(value).unwrap_or_else(|| value.to_string());
            out.push_str(&format!("{}{}{}", prefix, full_op, norm));
            idx = value_end;
            continue;
        }

        // Not a comparison; copy current char and advance
        out.push_str(&input[start..start + prefix.len()]);
        idx = start + prefix.len();
    }

    out.push_str(&input[idx..]);
    out
}

fn normalize_date_literals(input: &str, field: &str) -> String {
    let prefix = format!("{}:", field);
    let mut out = String::with_capacity(input.len());
    let mut idx = 0usize;

    while let Some(rel) = input[idx..].find(&prefix) {
        let start = idx + rel;
        out.push_str(&input[idx..start]);

        let value_start = start + prefix.len();
        // If the value is quoted, find the closing quote as the boundary.
        // Otherwise, use whitespace as the boundary.
        let value_end = if input[value_start..].starts_with('"') {
            input[value_start + 1..]
                .find('"')
                .map(|r| value_start + 1 + r + 1)
                .unwrap_or(input.len())
        } else {
            input[value_start..]
                .find(char::is_whitespace)
                .map(|r| value_start + r)
                .unwrap_or(input.len())
        };
        let value = &input[value_start..value_end];

        // Leave the shapes the range, comparison and `IN` passes own; an empty value is a
        // bare `field:` with nothing after it.
        if value.starts_with(['[', '{', '<', '>']) || value.is_empty() {
            out.push_str(&input[start..value_end]);
            idx = value_end;
            continue;
        }

        // Quoted, because RFC3339 contains colons and the grammar would otherwise read
        // `2024-06-15T00` as a field name. Only on success: a failed normalisation returns
        // `value` verbatim, which may already carry quotes.
        let rendered = match normalize_date_literal(value) {
            Some(norm) => format!("\"{norm}\""),
            None => value.to_string(),
        };
        out.push_str(&format!("{}{}", prefix, rendered));
        idx = value_end;
    }

    out.push_str(&input[idx..]);
    out
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
            emptied,
        }
    }
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

/// Normalize a query the way a search does, and build the parser a search would use.
///
/// Shared by the search path, the count-only path and validation, so that what validation
/// reports is what a search would actually do. A validator that built its parser differently —
/// a different default field set, a different normalization — would be worse than none: it would
/// disagree with the search it exists to predict.
fn prepare_query_parser(
    tantivy_index: &Index,
    fields: &SchemaFields,
    schema: &IndexSchema,
    query: &str,
) -> (String, Vec<String>, tantivy::query::QueryParser) {
    // Shadow names first, so every later rewriter — and the parser — sees only fields the
    // Tantivy schema actually carries.
    let query = rewrite_shadow_fields(query, schema);

    // Normalize date literals against the schema so naive inputs match indexed Date fields,
    // then facets, then rewrite single-term prefixes into ranges.
    let (normalized_query, prefix_notes) = normalize_prefix_query(
        &normalize_facet_query(&normalize_date_query(&query, schema), schema),
        tantivy_index,
    );

    // Only text and JSON fields are default search fields, so an unqualified term is never
    // attempted against a numeric or date field — which the parser reports as a type error
    // rather than as a non-match.
    let tantivy_schema = tantivy_index.schema();
    let default_query_fields: Vec<Field> = fields
        .indexed_fields
        .values()
        .filter(|field| {
            matches!(
                tantivy_schema.get_field_entry(**field).field_type(),
                tantivy::schema::FieldType::Str(_) | tantivy::schema::FieldType::JsonObject(_)
            )
        })
        .cloned()
        .collect();

    let parser = tantivy::query::QueryParser::for_index(tantivy_index, default_query_fields);
    (normalized_query, prefix_notes, parser)
}

/// Whether the parser resolved this ambiguity and ran the clause anyway.
///
/// The grammar reads `field:value` whose value contains a colon as a field name first, then
/// re-reads it as a term. It reports that as an error but the clause still executes, so it does
/// not belong in [`SearchOutcome::discarded`].
fn is_recovered_ambiguity(err: &tantivy::query::QueryParserError) -> bool {
    matches!(err, tantivy::query::QueryParserError::SyntaxError(detail)
        if detail.contains("parsed possible invalid field as term"))
}

/// Describe a dropped clause: what was lost from the query, and what to use instead where
/// there is an alternative.
///
/// Replaces Tantivy's own wording, which names parser internals rather than the effect on the
/// query — an exists leaf reports "Range query need to target a specific field", and a
/// non-indexed field reports being "not declared as indexed".
/// Note for a field the schema does not have.
///
/// Shared with [`unresolvable_fields`] so that when both the parser and the schema check see the
/// same field, the two notes are one string and collapse in [`describe_discarded_all`].
fn unknown_field_note(field: &str) -> String {
    format!(
        "unknown field '{field}' — the clause naming it was dropped, so this result set does \
         not match what the query asked for"
    )
}

/// Note for a field present in the schema but not indexed, and so not queryable.
fn non_indexed_field_note(field: &str) -> String {
    format!(
        "field '{field}' exists but is not indexed, so the clause naming it was dropped and \
         this result set does not match what the query asked for"
    )
}

/// Whether the lenient parse left nothing to run.
///
/// Tantivy trims discarded clauses out of the AST and returns `EmptyQuery` when that removes
/// every one of them. It matches no documents, which makes this the difference between having
/// answered a different question and having asked nothing at all.
fn nothing_survived(parsed: &dyn tantivy::query::Query) -> bool {
    parsed.is::<tantivy::query::EmptyQuery>()
}

fn describe_discarded(err: &tantivy::query::QueryParserError) -> String {
    use tantivy::query::QueryParserError as E;
    match err {
        E::FieldDoesNotExist(field) => unknown_field_note(field),
        E::FieldNotIndexed(field) => non_indexed_field_note(field),
        E::UnsupportedQuery(detail) => {
            // The parser refuses every exists leaf with this text, whatever the field type.
            if detail.contains("Range query need to target a specific field") {
                "field-presence tests (`field:*`) are not supported; the clause was dropped. \
                 Use a bounded range or an explicit value instead"
                    .to_string()
            } else {
                format!("unsupported clause was dropped: {detail}")
            }
        }
        E::FieldDoesNotHavePositionsIndexed(field) => format!(
            "field '{field}' has no positions indexed, so the phrase clause against it was \
             dropped; phrase queries need a text field"
        ),
        E::ExpectedInt(_) | E::ExpectedFloat(_) | E::ExpectedBool(_) | E::ExpectedBase64(_) => {
            format!("a value did not match its field's type, so the clause was dropped: {err}")
        }
        other => format!("clause was dropped: {other}"),
    }
}

/// Describe every clause the query lost, from both sources that can tell: the parser's error
/// list, and a schema check covering the field names the parser resolved to something
/// ineffective.
///
/// One note per distinct problem. An unfielded term is attempted against every default field,
/// so a single mistake arrives from the parser once per field. And where both sources name the
/// same field the schema's verdict wins, since it distinguishes a field that is absent from one
/// that is present but not indexed — the parser sees only that it is missing from the Tantivy
/// schema and reports both as unknown.
fn describe_discarded_all(
    errors: &[tantivy::query::QueryParserError],
    query: &str,
    schema: &IndexSchema,
) -> Vec<String> {
    let from_schema = unresolvable_fields(query, schema);
    let claimed = |field: &str| {
        let field = field.replace('\\', "");
        from_schema.iter().any(|(name, _)| *name == field)
    };

    let mut out: Vec<String> = Vec::new();
    for err in errors {
        use tantivy::query::QueryParserError as E;
        if is_recovered_ambiguity(err) {
            continue;
        }
        if matches!(err, E::FieldDoesNotExist(field) | E::FieldNotIndexed(field) if claimed(field))
        {
            continue;
        }
        let described = describe_discarded(err);
        if !out.contains(&described) {
            out.push(described);
        }
    }
    for (field, issue) in from_schema {
        let note = match issue {
            FieldIssue::Unknown => unknown_field_note(&field),
            FieldIssue::NotIndexed => non_indexed_field_note(&field),
        };
        if !out.contains(&note) {
            out.push(note);
        }
    }
    out
}

/// Byte offset of the first `:` not preceded by a backslash.
fn first_unescaped_colon(token: &str) -> Option<usize> {
    let mut escaped = false;
    for (idx, ch) in token.char_indices() {
        match ch {
            _ if escaped => escaped = false,
            '\\' => escaped = true,
            ':' => return Some(idx),
            _ => {}
        }
    }
    None
}

/// One field name a query references, and where it sits in the query string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldReference<'a> {
    /// Byte range of the name as written, escapes included.
    ///
    /// What a rewriter splices over. Kept separate from `name` because the two differ whenever
    /// the query escaped something: `k8s\.node` occupies ten bytes and names a nine-byte field.
    pub span: std::ops::Range<usize>,
    /// The name to look the schema up by, with the query's escapes resolved.
    pub name: Cow<'a, str>,
}

/// Every field name a query references, in the order they appear.
///
/// Shared so the readers of this question cannot disagree about what a query says: the shadow
/// rewriter splices over the span, [`unresolvable_fields`] classifies the name against the
/// schema, and the MCP layer lists the names for an agent.
///
/// A reference is the text before the first unescaped colon of a segment, after any leading
/// `+`, `-` or `!`, and a segment yields at most one. The rules that are not obvious:
///
/// - **Only the first colon splits.** A colon occurs inside values too, so taking every
///   name-then-colon run would read `2024-06-15T00` out of `created:2024-06-15T00:00:00Z` and
///   `https` out of `url:https://x`.
/// - **A segment is a whitespace token split again at `(` and `)`,** since a parenthesis ends
///   one clause and begins another without needing a space: `AND(sha1:x)` references `sha1`.
/// - **Only *leading* occurrence operators are stripped** — `content-type` is a name a `-` sits
///   inside.
/// - **Phrases, ranges and sets hold values, so nothing inside one is read.** Depth is tracked
///   for `[`/`{` only; parentheses group clauses and deliberately do not count.
///
/// The result borrows from `query`, and `name` allocates only for a name that was escaped.
pub fn field_references(query: &str) -> Vec<FieldReference<'_>> {
    let mut found = Vec::new();
    let mut inside_phrase = false;
    // Depth of `[ ]` and `{ }` only. Parentheses group clauses and do contain field references.
    let mut value_depth = 0i32;

    for (token_start, token) in whitespace_tokens(query) {
        // Read before the token's own delimiters are counted, so a token that opens a range
        // still offers the field name in front of it: `created:[2024-01-01` names `created`.
        let readable_position = !inside_phrase && value_depth == 0;

        for (offset, segment) in clause_segments(token) {
            if readable_position
                && let Some(reference) = leading_field_reference(segment, token_start + offset)
            {
                found.push(reference);
            }
        }

        // Then track what this token opened or closed for the tokens after it.
        let mut escaped = false;
        for ch in token.chars() {
            match ch {
                _ if escaped => escaped = false,
                '\\' => escaped = true,
                '"' => inside_phrase = !inside_phrase,
                '[' | '{' => value_depth += 1,
                ']' | '}' => value_depth = (value_depth - 1).max(0),
                _ => {}
            }
        }
    }

    found
}

/// Whitespace-separated tokens with their byte offsets, which `split_whitespace` drops.
fn whitespace_tokens(query: &str) -> impl Iterator<Item = (usize, &str)> {
    query.split_whitespace().scan(0usize, |cursor, token| {
        // The gap between tokens is whitespace alone, so the token's first occurrence at or
        // after the cursor is its position.
        let start = *cursor
            + query[*cursor..]
                .find(token)
                .expect("tokens come from this string");
        *cursor = start + token.len();
        Some((start, token))
    })
}

/// A token split at its parentheses, each piece with its offset within the token.
///
/// A parenthesis ends one clause and begins another without needing a space, so the pieces
/// either side of it are separate candidates for a field reference.
fn clause_segments(token: &str) -> impl Iterator<Item = (usize, &str)> {
    let mut offset = 0;
    token
        .split_inclusive(['(', ')'])
        .map(move |piece| {
            let start = offset;
            offset += piece.len();
            (start, piece.trim_end_matches(['(', ')']))
        })
        .filter(|(_, piece)| !piece.is_empty())
}

/// The field reference a segment opens with, if it opens with one.
///
/// `at` is the segment's byte offset in the whole query, so the returned span is absolute.
fn leading_field_reference(segment: &str, at: usize) -> Option<FieldReference<'_>> {
    let sigils = segment.len() - segment.trim_start_matches(['+', '-', '!']).len();
    let segment = &segment[sigils..];

    // A field reference never opens a phrase, a range or a set.
    if segment.starts_with(['"', '[', '{']) {
        return None;
    }
    let colon = first_unescaped_colon(segment)?;
    let name = &segment[..colon];
    if name.is_empty() {
        return None;
    }

    let start = at + sigils;
    Some(FieldReference {
        span: start..start + name.len(),
        // Escapes are the query's, not the field's: `k8s\.node` names the field `k8s.node`.
        name: if name.contains('\\') {
            Cow::Owned(name.replace('\\', ""))
        } else {
            Cow::Borrowed(name)
        },
    })
}

/// Why a field name a query references cannot answer it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FieldIssue {
    /// No such field in the schema.
    Unknown,
    /// Present in the schema but not indexed, so not queryable.
    NotIndexed,
}

/// Field names a query references that the schema cannot resolve, each with its reason.
///
/// Covers what the parser does not report. When an index has a JSON field, that field is a
/// default query field, so Tantivy resolves an unrecognised `name:` prefix as a path *inside*
/// it: the clause parses cleanly, matches nothing, and produces no error. A clause lost that
/// way is as ineffective as a dropped one, and in a negation it disables the exclusion.
///
/// Which names a query references is [`field_references`]'s question; this one only says what
/// the schema makes of each. Nothing is claimed when the schema is empty, or for a dotted name
/// whose root is a real field, since the parser judges the path itself.
fn unresolvable_fields(query: &str, schema: &IndexSchema) -> Vec<(String, FieldIssue)> {
    // An index with no stored schema yields an empty one; everything would look unresolvable.
    if schema.fields.is_empty() {
        return Vec::new();
    }

    let mut found: Vec<(String, FieldIssue)> = Vec::new();

    for reference in field_references(query) {
        let name = reference.name;

        // `id` and `_seq` are added to every Tantivy schema, not to `IndexSchema::fields`.
        if name == "id" || name == "_seq" {
            continue;
        }

        let issue = match schema.fields.get(name.as_ref()) {
            // Shadow fields are rewritten to `id` before a query reaches the engine.
            Some(def) if def.indexed || def.is_shadow => continue,
            Some(_) => FieldIssue::NotIndexed,
            // A dotted name whose root is a real field is a path expression; whether the path
            // is valid for that field's type is the parser's judgement, not ours.
            None if name
                .split_once('.')
                .is_some_and(|(root, _)| schema.fields.contains_key(root)) =>
            {
                continue;
            }
            None => FieldIssue::Unknown,
        };

        if !found.iter().any(|(seen, _)| *seen == name) {
            found.push((name.into_owned(), issue));
        }
    }

    found
}

/// Characters that make a value the parser's business rather than the key-value store's.
///
/// Each one is syntax — a space, quote or parenthesis ends the value, `*` is the prefix operator
/// and `^` the boost — and the key-value store can only look a key up whole, so a value carrying
/// one falls through to the search index instead.
///
/// Matched *before* escapes are removed, so an escaped operator goes to the parser too. That is
/// what keeps an identifier genuinely containing one reachable: the parser resolves `id:d1\^2`
/// to the literal `d1^2`, bare and inside a larger query alike.
///
/// `~` is deliberately absent. Tantivy reads it as slop only after a quoted phrase; against a
/// bare term it is an ordinary character an identifier may contain.
const QUERY_SYNTAX_IN_VALUE: &[char] = &[' ', '"', '(', ')', '*', '^'];

/// The identifier a whole-query `id:VALUE` or `shadowfield:VALUE` lookup names, or `None` when
/// the query is not that shape.
///
/// This is the one path that answers without the search index, so it has to read the query the
/// way the parser would — otherwise a bare lookup and the same clause inside a larger query
/// disagree about which document was named. Two things keep them aligned:
///
/// - The field name ends at the first *unescaped* colon, the same position
///   [`rewrite_shadow_fields`] and [`unresolvable_fields`] read it at.
/// - Escapes in the value are removed, because the parser removes them: `id:urn\:x\:1` names
///   the key `urn:x:1`.
///
/// A value carrying anything in [`QUERY_SYNTAX_IN_VALUE`] is not a whole key and is left to the
/// parser.
fn parse_exact_id_query(query: &str, schema: &IndexSchema) -> Option<(String, bool)> {
    let query = query.trim();

    let colon = first_unescaped_colon(query)?;
    let field_part = query[..colon].trim();
    let value_part = query[colon + 1..].trim();

    if value_part.contains(QUERY_SYNTAX_IN_VALUE) {
        return None;
    }

    // The document key under its own name, or under a shadow name that stands for it.
    if field_part != "id" && !schema.is_shadow_field(field_part) {
        return None;
    }

    Some((unescape_query_value(value_part), true))
}

/// A query value with the parser's escapes removed: `\x` is the literal `x`.
///
/// Tantivy's grammar reads a backslash as "the next character is data, not syntax", and drops
/// the backslash when it builds the term. Anything comparing a value against stored data has to
/// do the same, or the two see different strings. A trailing lone backslash is kept, since there
/// is no character after it for it to have been escaping.
fn unescape_query_value(value: &str) -> String {
    if !value.contains('\\') {
        return value.to_string();
    }
    let mut out = String::with_capacity(value.len());
    let mut chars = value.chars();
    while let Some(ch) = chars.next() {
        match ch {
            '\\' => match chars.next() {
                Some(escaped) => out.push(escaped),
                None => out.push('\\'),
            },
            _ => out.push(ch),
        }
    }
    out
}

/// Rewrite each shadow field reference in a query string to the canonical `id` field it stands
/// for.
///
/// A shadow field is the identifier under its source name: the value lives in `id`, which every
/// Tantivy schema carries indexed, so the name can appear anywhere a field name can — alone,
/// where [`parse_exact_id_query`] answers from the key-value store without parsing, or inside a
/// larger query, where it is rewritten here and runs against the search index like any other
/// clause.
///
/// Only the name in field-reference position is replaced, as [`field_references`] finds it, so
/// a shadow name inside a phrase or a range stays the value it is. The replacement is spliced
/// by byte range, leaving the rest of the query — whitespace, quoting, escapes — untouched.
fn rewrite_shadow_fields(query: &str, schema: &IndexSchema) -> String {
    if schema.shadow_fields.is_empty() {
        return query.to_string();
    }

    let spans: Vec<std::ops::Range<usize>> = field_references(query)
        .into_iter()
        .filter(|reference| schema.is_shadow_field(&reference.name))
        .map(|reference| reference.span)
        .collect();
    if spans.is_empty() {
        return query.to_string();
    }

    let mut rewritten = String::with_capacity(query.len());
    let mut cursor = 0;
    for span in spans {
        rewritten.push_str(&query[cursor..span.start]);
        rewritten.push_str("id");
        cursor = span.end;
    }
    rewritten.push_str(&query[cursor..]);
    rewritten
}

/// Longest token, in bytes, that `default` and `en_stem` keep.
///
/// Tantivy's own `default` and `en_stem` cap tokens at 40 bytes, which silently drops the long
/// atoms this engine is routinely asked to match on — hex digests, base64 blobs, opaque keys.
/// A dropped token is invisible: the document indexes, the field reports itself as indexed, and
/// the term simply does not exist to be searched. Both are re-registered under their original
/// names with this cap.
///
/// Deliberately a constant rather than a [`StorageConfig`] knob. The cap decides which terms
/// exist on disk, so two shards holding the same data under different caps would answer the
/// same query differently, and nothing in a response would say why.
const MAX_INDEXED_TOKEN_LEN: usize = 128;

/// Registers this engine's tokenizer overrides on an index.
///
/// A `TokenizerManager` is per-[`Index`]-instance and in-memory — nothing about it is persisted
/// with the index — so every instance handed out must pass through here. Both constructors
/// ([`open_tantivy_index`] and [`create_tantivy_index`]) do, and they are the only two in the
/// workspace; an `Index` built any other way silently falls back to the 40-byte builtins and
/// writes terms that disagree with the rest of the shard.
fn register_tokenizers(index: &Index) {
    use tantivy::tokenizer::{
        Language, LowerCaser, RemoveLongFilter, SimpleTokenizer, Stemmer, TextAnalyzer,
    };

    // `RemoveLongFilter` keeps tokens strictly shorter than its limit, so the limit is one past
    // the longest token to keep. Off by one here and a digest of exactly the cap disappears.
    let remove_long = || RemoveLongFilter::limit(MAX_INDEXED_TOKEN_LEN + 1);

    // Filter order matches tantivy's own construction of these two analyzers. Term bytes are
    // whatever the last filter emits, so a reordering here would not raise anything — it would
    // just stop matching the terms already on disk.
    index.tokenizers().register(
        "default",
        TextAnalyzer::builder(SimpleTokenizer::default())
            .filter(remove_long())
            .filter(LowerCaser)
            .build(),
    );
    index.tokenizers().register(
        "en_stem",
        TextAnalyzer::builder(SimpleTokenizer::default())
            .filter(remove_long())
            .filter(LowerCaser) // The stemmer does not lowercase.
            .filter(Stemmer::new(Language::English))
            .build(),
    );
}

/// Opens the Tantivy index at `path` with this engine's tokenizers registered.
fn open_tantivy_index(path: &Path) -> Result<Index, StoreError> {
    let index = Index::open_in_dir(path)?;
    register_tokenizers(&index);
    Ok(index)
}

/// Creates a Tantivy index at `path` with this engine's tokenizers registered.
fn create_tantivy_index(path: &Path, schema: Schema) -> Result<Index, StoreError> {
    let index = Index::create_in_dir(path, schema)?;
    register_tokenizers(&index);
    Ok(index)
}

/// The single token an analyzer produces from `text`, or None if it produces any other number.
fn single_token(analyzer: &mut tantivy::tokenizer::TextAnalyzer, text: &str) -> Option<String> {
    use tantivy::tokenizer::TokenStream;

    let mut tokens: Vec<String> = Vec::new();
    let mut stream = analyzer.token_stream(text);
    stream.process(&mut |token| tokens.push(token.text.clone()));
    match tokens.len() {
        1 => tokens.pop(),
        _ => None,
    }
}

/// The exclusive upper bound of a lexicographic prefix range: `term` with its final scalar raised
/// to the next value the analyzer leaves unchanged.
///
/// Tantivy tokenizes a range bound and takes a single token only, so a candidate the analyzer
/// rewrites or discards cannot serve as one. Ascending order keeps the bound tight: a scalar the
/// analyzer discards cannot appear in an indexed term either, so stepping over it admits nothing.
///
/// None when no candidate qualifies, leaving the clause for the caller to report.
fn prefix_upper_bound(
    term: &str,
    analyzer: &mut tantivy::tokenizer::TextAnalyzer,
) -> Option<String> {
    /// Enough to cross the punctuation runs between the digits, the ASCII letters and the
    /// alphanumeric scalars above them.
    const MAX_CANDIDATES: u32 = 64;

    let mut scalars: Vec<char> = term.chars().collect();
    let last = scalars.pop()?;
    let head: String = scalars.into_iter().collect();

    let mut code = last as u32;
    for _ in 0..MAX_CANDIDATES {
        code += 1;
        // Surrogates are not scalar values, so the successor of U+D7FF is U+E000.
        if code == 0xD800 {
            code = 0xE000;
        }
        let candidate = format!("{head}{}", char::from_u32(code)?);
        if single_token(analyzer, &candidate).as_deref() == Some(candidate.as_str()) {
            return Some(candidate);
        }
    }
    None
}

/// Split a single-term prefix clause into its term and any trailing boost.
///
/// None for the values this rewrite does not claim: a quoted value is a phrase prefix, a bare `*`
/// a presence test, and a range or group is not a term.
fn single_term_prefix(value: &str) -> Option<(&str, &str)> {
    let (head, boost) = match value.find('^') {
        Some(at) => value.split_at(at),
        None => (value, ""),
    };
    let term = head.strip_suffix('*')?;
    if term.contains('*') || matches!(term.chars().next()?, '"' | '[' | '{' | '(') {
        return None;
    }
    Some((term, boost))
}

/// Note for a prefix clause left as the bare term because no bound was available.
fn unrewritable_prefix_note(field: &str, value: &str) -> String {
    format!(
        "'{field}:{value}' could not be rewritten as a prefix range, so it matched the term \
         '{}' exactly; write the range you want instead",
        value.trim_end_matches('*')
    )
}

/// Rewrite a single-term prefix — `field:pre*` — into the equivalent lexicographic range, on text
/// and string fields.
///
/// The grammar has no prefix operator: it drops the `*` and matches `pre` as a whole term without
/// raising an error. Tantivy tokenizes the bounds, so the prefix may be written in any case.
///
/// Returns the query with a note per prefix clause left unrewritten.
fn normalize_prefix_query(query: &str, tantivy_index: &Index) -> (String, Vec<String>) {
    use tantivy::schema::FieldType;

    if !query.contains('*') {
        return (query.to_string(), Vec::new());
    }

    let tantivy_schema = tantivy_index.schema();
    let mut normalized = query.to_string();
    let mut notes = Vec::new();

    for (_, entry) in tantivy_schema.fields() {
        let FieldType::Str(ref options) = *entry.field_type() else {
            continue;
        };
        let Some(indexing) = options.get_indexing_options() else {
            continue;
        };
        let name = entry.name();
        let prefix = format!("{name}:");
        if !normalized.contains(&prefix) {
            continue;
        }
        let Some(mut analyzer) = tantivy_index.tokenizers().get(indexing.tokenizer()) else {
            continue;
        };

        let mut out = String::with_capacity(normalized.len());
        let mut idx = 0usize;
        while let Some(rel) = normalized[idx..].find(&prefix) {
            let start = idx + rel;
            out.push_str(&normalized[idx..start]);

            // The value runs to the next whitespace or to a closing paren from a group.
            let value_start = start + prefix.len();
            let value_end = normalized[value_start..]
                .find(|ch: char| ch.is_whitespace() || ch == ')')
                .map(|r| value_start + r)
                .unwrap_or(normalized.len());
            let value = &normalized[value_start..value_end];

            let range = single_term_prefix(value).map(|(term, boost)| {
                single_token(&mut analyzer, term)
                    .and_then(|lower| {
                        let upper = prefix_upper_bound(&lower, &mut analyzer)?;
                        Some(format!("{name}:[{lower} TO {upper}}}{boost}"))
                    })
                    .ok_or(value)
            });

            match range {
                Some(Ok(rewritten)) => out.push_str(&rewritten),
                Some(Err(unrewritable)) => {
                    notes.push(unrewritable_prefix_note(name, unrewritable));
                    out.push_str(&normalized[start..value_end]);
                }
                None => out.push_str(&normalized[start..value_end]),
            }
            idx = value_end;
        }
        out.push_str(&normalized[idx..]);
        normalized = out;
    }

    (normalized, notes)
}

/// Quote facet path values so the parser resolves them to facet terms.
///
/// The parser matches a facet term only against a quoted value, so `category:/electronics/phones`
/// alone matches nothing. Only unquoted values beginning with `/` are touched; a quoted value is
/// already in the form the parser wants. Matching is hierarchical, so a parent path matches its
/// descendants.
fn normalize_facet_query(query: &str, schema: &IndexSchema) -> String {
    let facet_fields: Vec<&str> = schema
        .fields
        .iter()
        .filter(|(_, def)| matches!(def.field_type, TantivyFieldType::Facet))
        .map(|(name, _)| name.as_str())
        .collect();

    if facet_fields.is_empty() {
        return query.to_string();
    }

    let mut normalized = query.to_string();
    for field in facet_fields {
        let prefix = format!("{}:/", field);
        if !normalized.contains(&prefix) {
            continue;
        }

        let mut out = String::with_capacity(normalized.len() + 2);
        let mut idx = 0usize;
        while let Some(rel) = normalized[idx..].find(&prefix) {
            let start = idx + rel;
            out.push_str(&normalized[idx..start]);

            // The path runs to the next whitespace or to a closing paren from a group.
            let value_start = start + field.len() + ':'.len_utf8();
            let value_end = normalized[value_start..]
                .find(|ch: char| ch.is_whitespace() || ch == ')')
                .map(|r| value_start + r)
                .unwrap_or(normalized.len());

            out.push_str(&format!(
                "{}:\"{}\"",
                field,
                &normalized[value_start..value_end]
            ));
            idx = value_end;
        }
        out.push_str(&normalized[idx..]);
        normalized = out;
    }

    normalized
}

/// Rewrite every date literal in a query to RFC3339, the only form Tantivy's date parser
/// accepts. Covers:
///
/// - `field:value`
/// - `field:>value`, `field:<value`, `field:>=value`, `field:<=value`
/// - `field:[lower TO upper]`, `field:{lower TO upper}`, and the mixed pairs
/// - `field: IN [a b c]`
///
/// A shape no pass recognises reaches the parser unrewritten, which drops the clause rather
/// than raising an error — so a gap here surfaces as a query that matches nothing.
fn normalize_date_query(query: &str, schema: &IndexSchema) -> String {
    let date_fields: HashSet<&str> = schema
        .fields
        .iter()
        .filter(|(_, def)| matches!(def.field_type, TantivyFieldType::Date))
        .map(|(name, _)| name.as_str())
        .collect();

    if date_fields.is_empty() {
        return query.to_string();
    }

    let mut normalized = query.to_string();
    for field in &date_fields {
        // Each pass claims one shape; the single-literal pass takes whatever is left, so it
        // runs last or it would rewrite a range bound as a whole value.
        normalized = normalize_date_ranges(&normalized, field);
        normalized = normalize_date_in_sets(&normalized, field);
        normalized = normalize_date_comparisons(&normalized, field);
        normalized = normalize_date_literals(&normalized, field);
    }

    normalized
}

impl Default for StorageConfig {
    fn default() -> Self {
        const DEFAULT_TOTAL_MEMORY_LIMIT_MB: u64 = 1024;
        const DEFAULT_MEMORY_PRESSURE_THRESHOLD_PERCENT: u8 = 80;
        Self {
            shard_path: PathBuf::from("/var/tmp/cameodb"),

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
        }
    }
}

/// Measure the on-disk size of a Tantivy index.
///
/// Callers pass the index directory (`<shard>/indices/<name>`). `fs::metadata(dir).len()`
/// reports the size of the directory entry itself — a couple of KB regardless of contents —
/// so summing the files inside is the only way to size an index. Tantivy lays its segment
/// files out flat, so one non-recursive `read_dir` suffices: a `getdents` plus a `stat`
/// per file.
///
/// A plain file path is measured directly, so the function is meaningful for any path a
/// caller might reasonably hand it.
///
/// Returns `None` when the path does not exist yet (a brand-new index).
fn index_size_bytes(index_path: &Path) -> Option<u64> {
    let metadata = fs::metadata(index_path).ok()?;
    if metadata.is_file() {
        return Some(metadata.len());
    }

    let mut total = 0u64;
    for entry in fs::read_dir(index_path).ok()?.flatten() {
        if let Ok(entry_meta) = entry.metadata()
            && entry_meta.is_file()
        {
            total = total.saturating_add(entry_meta.len());
        }
    }
    Some(total)
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

    /// Calculate memory budget for bulk operations with size-based scaling.
    ///
    /// Increases budget for large bulk operations to reduce segment flushing:
    /// - batch_size > 5000: 2x base budget
    /// - batch_size > 1000: 1.5x base budget
    /// - otherwise: base budget
    pub fn get_bulk_operation_budget(&self, index_path: &Path, batch_size: usize) -> usize {
        let base_budget = self.get_optimal_memory_budget(index_path, None);

        // Scale budget based on batch size to optimize indexing throughput
        let scaled_budget = match batch_size {
            0..=1000 => base_budget,
            1001..=5000 => base_budget * 3 / 2, // 1.5x for medium batches
            _ => base_budget * 2,               // 2x for large batches (>5000)
        };

        // Cap at maximum budget
        let max_budget = self.indexer_memory_max_mb * 1024 * 1024;
        scaled_budget.min(max_budget)
    }
}

/// Native Tantivy field types with proper enum for type safety.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub enum TantivyFieldType {
    /// Tokenized text for full-text search
    #[default]
    Text,
    /// Untokenized string (exact match)
    String,
    /// 64-bit signed integer
    I64,
    /// 64-bit unsigned integer
    U64,
    /// 64-bit floating point
    F64,
    /// Date/Time (stored as timestamp)
    Date,
    /// Boolean (stored as "true"/"false")
    Boolean,
    /// Binary data
    Bytes,
    /// IP address (IPv4/IPv6)
    Ip,
    /// Nested JSON object
    Json,
    /// Categorical/facet field
    Facet,
}

/// Serialized as the same lowercase name every other surface uses.
///
/// The derived implementation emitted the variant name — `Date`, `Boolean` — while
/// [`TantivyFieldType::to_string`] returns `date` and `boolean`, and that is the name the query
/// syntax reference, the per-field hints and the deserializer's own canonical list are all keyed
/// on. So a schema described one type and every instruction for querying it named another.
///
/// Delegating to `to_string` rather than renaming the variants means the JSON name and the name
/// an agent is told to use are one function, and a new variant cannot introduce a third spelling.
///
/// Safe to change: deserialization lowercases before matching, so schemas already persisted with
/// the capitalized form still load.
impl Serialize for TantivyFieldType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.to_string())
    }
}

impl TantivyFieldType {
    /// Convert to string representation (for serialization)
    pub fn to_string(&self) -> &'static str {
        match self {
            TantivyFieldType::Text => "text",
            TantivyFieldType::String => "string",
            TantivyFieldType::I64 => "i64",
            TantivyFieldType::U64 => "u64",
            TantivyFieldType::F64 => "f64",
            TantivyFieldType::Date => "date",
            TantivyFieldType::Boolean => "boolean",
            TantivyFieldType::Bytes => "bytes",
            TantivyFieldType::Ip => "ip",
            TantivyFieldType::Json => "json",
            TantivyFieldType::Facet => "facet",
        }
    }
}

// Custom deserialization to support common type aliases from Python and other languages
impl<'de> Deserialize<'de> for TantivyFieldType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let normalized = s.to_lowercase();

        match normalized.as_str() {
            // Primary canonical names
            "text" => Ok(TantivyFieldType::Text),
            "string" => Ok(TantivyFieldType::String),
            "i64" => Ok(TantivyFieldType::I64),
            "u64" => Ok(TantivyFieldType::U64),
            "f64" => Ok(TantivyFieldType::F64),
            "date" => Ok(TantivyFieldType::Date),
            "boolean" => Ok(TantivyFieldType::Boolean),
            "bytes" => Ok(TantivyFieldType::Bytes),
            "ip" => Ok(TantivyFieldType::Ip),
            "json" => Ok(TantivyFieldType::Json),
            "facet" => Ok(TantivyFieldType::Facet),

            // Common aliases for Python/JavaScript/SQL compatibility
            "float" | "double" | "decimal" => Ok(TantivyFieldType::F64),
            "integer" | "int" | "number" | "signed" => Ok(TantivyFieldType::I64),
            "unsigned" | "uint" => Ok(TantivyFieldType::U64),
            "bool" => Ok(TantivyFieldType::Boolean),
            "datetime" | "timestamp" => Ok(TantivyFieldType::Date),
            "binary" | "blob" => Ok(TantivyFieldType::Bytes),
            "object" | "document" => Ok(TantivyFieldType::Json),
            "category" | "tag" => Ok(TantivyFieldType::Facet),

            // Fallback with helpful error
            _ => Err(D::Error::custom(format!(
                "Unknown field type: '{}'. Supported types: text, string, i64, u64, f64, date, boolean, bytes, ip, json, facet. Aliases: float, double, integer, int, number, bool, datetime, timestamp, binary, blob, object, document, category, tag",
                s
            ))),
        }
    }
}

fn default_true() -> bool {
    true
}

fn default_version() -> u64 {
    1
}

fn default_timestamp() -> i64 {
    chrono::Utc::now().timestamp()
}

fn default_routing_field() -> String {
    "id".to_string()
}

/// Longest description an index may carry, in characters.
///
/// A description is read by a caller choosing between datasets, and a catalogue listing returns
/// one per index, so the whole node's worth of them is resident in that caller's context at once.
/// A paragraph is enough to say what a dataset is; a page of it is a document, and belongs
/// somewhere that can be fetched on purpose.
pub const MAX_INDEX_DESCRIPTION_CHARS: usize = 512;

/// Longest description a single field may carry, in characters.
///
/// Tighter than the index limit because it is paid per field on every schema read: one line
/// saying what the field means, not the history of how it came to exist.
pub const MAX_FIELD_DESCRIPTION_CHARS: usize = 200;

/// Blank is the same as unset, so an operator clearing a description gets `None` rather than a
/// key with nothing in it.
fn normalize_description(description: &mut Option<String>) {
    if let Some(text) = description {
        let trimmed = text.trim();
        if trimmed.is_empty() {
            *description = None;
        } else if trimmed.len() != text.len() {
            *text = trimmed.to_string();
        }
    }
}

/// Field definition for schema evolution and validation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FieldDef {
    /// Field name — populated from the map key if not present in JSON
    #[serde(default)]
    pub name: String,
    pub field_type: TantivyFieldType,
    /// Whether this field is indexed in Tantivy (default: true for user-defined schemas)
    #[serde(default = "default_true")]
    pub indexed: bool,
    #[serde(default)]
    pub stored: bool,
    /// Whether this field gets a Tantivy *fast column* — the columnar copy a sort orders on.
    ///
    /// Three-state on purpose. `None` means the caller said nothing and the default for the type
    /// applies; `Some(false)` means a caller said no. A plain `bool` with a serde default cannot
    /// tell those apart — an absent key and an explicit `false` both arrive as `false` — which is
    /// how a declared `"fast": false` on a numeric field came to be overwritten every time the
    /// schema was read.
    ///
    /// Read it through [`FieldDef::is_fast`] rather than directly, which resolves the default;
    /// [`IndexSchema::normalize_after_deserialization`] materializes it into `Some(..)` so a
    /// stored schema always names a concrete boolean and every reader of the serialised form
    /// sees the same shape it saw before.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fast: Option<bool>,
    /// Shadow field flag: true if this field preserves original field name when ID is copied to canonical "id" field
    /// Shadow fields are NOT indexed and NOT stored in Tantivy, but preserved in schema for query mapping
    /// Default is false for backward compatibility with existing schemas
    #[serde(default)]
    pub is_shadow: bool,
    /// What this field means, in the operator's words.
    ///
    /// Nothing infers it: a field name says what a value is called and a type says how it is
    /// queried, but neither says what it records. Absent unless someone wrote one, and omitted
    /// from the serialised schema when absent so an undescribed index costs nothing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    // Additional options for Text fields
    pub tokenizer: Option<String>,
    pub index_record_option: Option<String>, // "Basic", "WithFreqs", "WithFreqsAndPositions"
}

impl FieldDef {
    /// Create a new field definition with sensible defaults
    pub fn new(name: String, field_type: TantivyFieldType) -> Self {
        // Only ID field should be stored in Tantivy
        // All other fields are indexed-only, complete data comes from redb
        let stored = name == "id";
        let fast = Some(Self::fast_by_default(&field_type));

        Self {
            name,
            field_type,
            indexed: true,
            stored,
            fast,
            is_shadow: false,          // Default: not a shadow field
            description: None,         // Nothing infers a description; an operator writes it
            tokenizer: None,           // Will be set when creating from actual Tantivy schema
            index_record_option: None, // Will be set when creating from actual Tantivy schema
        }
    }

    /// Whether a field of this type gets a fast column when nothing says either way.
    ///
    /// Numeric and date fields do: a range or a sort on them is the ordinary reason to declare
    /// one, and the column is what a sort orders on. Everything else does not — a text field
    /// pays for a full copy of every value, so it gets a column only when asked.
    pub fn fast_by_default(field_type: &TantivyFieldType) -> bool {
        matches!(
            field_type,
            TantivyFieldType::I64
                | TantivyFieldType::U64
                | TantivyFieldType::F64
                | TantivyFieldType::Date
        )
    }

    /// Whether a field of this type can carry a fast column at all.
    ///
    /// Five types cannot, and the reason is in the index builder rather than in Tantivy: a
    /// boolean, bytes, ip, json or facet field is added with `add_bool_field`, `add_bytes_field`,
    /// `add_ip_addr_field`, `add_json_field` and `add_facet_field`, none of which consults `fast`
    /// — so a schema asking for a column on one gets no column, and a `fast: true` that reads back
    /// as `true` is a claim the index cannot honour. Text and string can: `set_fast` on them builds
    /// the string column an exact alphabetical sort orders on.
    ///
    /// A declaration this returns `false` for is not refused at the door — a schema is a
    /// description of intent and a caller may well be declaring a field for a rebuild — it is
    /// resolved to `false`, so what the config reports and what the index does are the same thing.
    pub fn can_be_fast(field_type: &TantivyFieldType) -> bool {
        matches!(
            field_type,
            TantivyFieldType::Text
                | TantivyFieldType::String
                | TantivyFieldType::I64
                | TantivyFieldType::U64
                | TantivyFieldType::F64
                | TantivyFieldType::Date
        )
    }

    /// Resolved `fast`: what the caller declared, or the default for the type when they declared
    /// nothing.
    ///
    /// This is the only correct way to read `fast`, because [`FieldDef::fast`] is three-state and
    /// `None` does not mean `false`. Two kinds of field are never fast whatever they declare, and
    /// for the same reason — there is no column behind the declaration. A shadow field is not added
    /// to the Tantivy index at all, and a type [`FieldDef::can_be_fast`] rejects is added without
    /// its `fast` ever being read.
    pub fn is_fast(&self) -> bool {
        if self.is_shadow || !Self::can_be_fast(&self.field_type) {
            return false;
        }
        self.fast
            .unwrap_or_else(|| Self::fast_by_default(&self.field_type))
    }

    /// Infer field type from JSON value for schema evolution
    pub fn infer_from_value(name: String, value: &JsonValue) -> Self {
        let field_type = Self::infer_type_from_value(value);
        Self::new(name, field_type)
    }

    /// Create a non-indexed field definition for background schema evolution
    /// New fields discovered during writes are marked as non-indexed to avoid
    /// requiring Tantivy schema rebuilds. They can be stored in redb and later
    /// promoted to indexed fields through explicit schema updates.
    pub fn new_non_indexed(name: String, value: &JsonValue) -> Self {
        let field_type = Self::infer_type_from_value(value);
        // Only ID field should be stored in Tantivy
        let stored = name == "id";
        let fast = Some(Self::fast_by_default(&field_type));

        Self {
            name,
            field_type,
            indexed: false, // Non-indexed by default for background evolution
            stored,
            fast,
            is_shadow: false, // Default: not a shadow field
            description: None,
            tokenizer: None,
            index_record_option: None,
        }
    }

    /// Create a shadow field definition for preserving original field names
    /// Shadow fields are NOT indexed and NOT stored in Tantivy, but preserved in schema
    pub fn new_shadow(name: String, field_type: TantivyFieldType) -> Self {
        Self {
            name,
            field_type,
            indexed: false,    // Shadow fields are never indexed
            stored: false,     // Shadow fields are never stored
            fast: Some(false), // Shadow fields don't need fast access
            is_shadow: true,   // This is a shadow field
            description: None,
            tokenizer: None,
            index_record_option: None,
        }
    }

    /// Infer Tantivy field type from JSON value
    pub fn infer_type_from_value(value: &JsonValue) -> TantivyFieldType {
        match value {
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
            JsonValue::String(s) => {
                // 1) RFC3339 (full timestamp with offset)
                if chrono::DateTime::parse_from_rfc3339(s).is_ok()
                    // 2) Naive datetime with common formats
                    || Self::is_naive_datetime(s)
                    // 3) Date-only formats
                    || Self::is_naive_date(s)
                {
                    TantivyFieldType::Date
                // 4) IP detection
                } else if s.parse::<std::net::IpAddr>().is_ok() {
                    TantivyFieldType::Ip
                } else {
                    TantivyFieldType::Text
                }
            }
            JsonValue::Array(_) => TantivyFieldType::Text, // Arrays as text for compatibility
            JsonValue::Object(_) => TantivyFieldType::Json, // Nested objects as JSON
            JsonValue::Null => TantivyFieldType::Text,
        }
    }

    /// Check common naive datetime formats (no timezone) such as
    /// - 2024-05-01 12:30:00
    /// - 2024-05-01 12:30
    /// - 2024-05-01T12:30:00
    /// - 2024-05-01T12:30:00.123
    /// - 2024/05/01 12:30:00
    /// - 2024.05.01T12:30:00
    fn is_naive_datetime(s: &str) -> bool {
        NAIVE_DATETIME_FORMATS
            .iter()
            .any(|fmt| NaiveDateTime::parse_from_str(s, fmt).is_ok())
    }

    /// Check common date-only formats such as
    /// - 2024-05-01
    /// - 2024/05/01
    /// - 2024.05.01
    /// - 20240501
    fn is_naive_date(s: &str) -> bool {
        NAIVE_DATE_FORMATS
            .iter()
            .any(|fmt| NaiveDate::parse_from_str(s, fmt).is_ok())
    }
}

/// Parse a date string (RFC3339, naive datetime, date-only, compact datetime,
/// unix epoch seconds, year-month, or year-only) into the epoch-second timestamp
/// that the Date FAST field is sorted on.
///
/// Returns the value **clamped to Tantivy's supported range**, matching exactly what
/// `parse_date_str_to_tantivy` feeds into the index. Callers that need a comparable
/// numeric sort key for a date value (e.g. cross-node merge ordering) should use this
/// so the merge order agrees with each shard's local FAST-field ordering. Returns
/// `None` when the string is not a recognized date format.
pub fn parse_date_to_timestamp_secs(s: &str) -> Option<i64> {
    parse_date_str_to_tantivy(s).map(|(_, _, clamped)| clamped)
}

/// Parse a date string (RFC3339, naive datetime, date-only, year-month, or year-only) into Tantivy DateTime
/// What a facet value the type cannot hold should cost.
///
/// Ingest refuses it, because the caller is right there and can be told which field and which
/// value. Replay cannot: the value is already committed to redb, so refusing it would fail the
/// index open rather than the write that accepted it, and an index that will not open serves
/// nothing.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum BadFacet {
    Refuse,
    SkipAndWarn,
}

/// Add one JSON value to a tantivy document, under the type its field declares.
///
/// One place, called by both write paths and by WAL replay. It was three copies of the same
/// match, and they had already drifted: only replay logged a clamped date, and only replay
/// survived a bad facet — so a document could index differently on recovery than it did on
/// the write, which is the one thing replay must never do.
///
/// **A list is several values of the field, not a value the field cannot hold.** Every tantivy
/// field is multivalued: `add_i64` twice under one field stores two values of it, the fast
/// column reports `Cardinality::Multivalued`, and a range or term query matches the document if
/// any one of its values matches. So `{"risk_score": [9, 12]}` is indexed as both numbers and
/// found by either, which is what a source that reports several analyses of one sample means.
/// Reading such a value with `as_i64` skipped it, leaving the field unindexed on every document
/// carrying more than one value, with nothing said and no error to see — the write succeeded
/// and a range query over the field simply never matched.
///
/// Text and json are deliberately not treated that way: both take the whole value serialized,
/// so a list under them is already indexed as its own JSON text and splitting it would change
/// what those fields have always held. Bytes is a list by definition. One level is flattened
/// and no more, so a list inside a list is a value the inner type has to accept on its own —
/// which mirrors what this function then does with it.
///
/// Sorting is the one place where several values are not simply more: `order_by_fast_field`
/// reads one value per document, and for a multivalued column that is the first one written —
/// insertion order, not the largest or the latest. A caller who needs a particular one to order
/// by has to send that one, in a field of its own.
pub(crate) fn add_json_value_to_doc(
    tantivy_doc: &mut tantivy::TantivyDocument,
    tantivy_field: Field,
    field_name: &str,
    field_type: &TantivyFieldType,
    field_value: &JsonValue,
    bad_facet: BadFacet,
) -> Result<(), StoreError> {
    // The types that take the value whole, whatever shape it arrived in.
    match field_type {
        TantivyFieldType::Text => {
            if let Some(s) = field_value.as_str() {
                tantivy_doc.add_text(tantivy_field, s);
            } else {
                let field_str = serde_json::to_string(field_value)
                    .map_err(|e| StoreError::Serialization(e.to_string()))?;
                tantivy_doc.add_text(tantivy_field, &field_str);
            }
            return Ok(());
        }
        TantivyFieldType::Json => {
            let json_str = serde_json::to_string(field_value)
                .map_err(|e| StoreError::Serialization(e.to_string()))?;
            tantivy_doc.add_text(tantivy_field, &json_str);
            return Ok(());
        }
        TantivyFieldType::Bytes => {
            if let Some(arr) = field_value.as_array() {
                let bytes: Vec<u8> = arr
                    .iter()
                    .filter_map(|item| item.as_u64())
                    .map(|n| n as u8)
                    .collect();
                if !bytes.is_empty() {
                    tantivy_doc.add_bytes(tantivy_field, bytes.as_slice());
                }
            }
            return Ok(());
        }
        _ => {}
    }

    // Everything else holds one value per entry, and a list is several entries.
    let values: &[JsonValue] = match field_value.as_array() {
        Some(items) => items.as_slice(),
        None => std::slice::from_ref(field_value),
    };

    for value in values {
        match field_type {
            TantivyFieldType::String => {
                if let Some(s) = value.as_str() {
                    tantivy_doc.add_text(tantivy_field, s);
                }
            }
            TantivyFieldType::F64 => {
                if let Some(n) = value.as_f64() {
                    tantivy_doc.add_f64(tantivy_field, n);
                }
            }
            TantivyFieldType::I64 => {
                if let Some(n) = value.as_i64() {
                    tantivy_doc.add_i64(tantivy_field, n);
                }
            }
            TantivyFieldType::U64 => {
                if let Some(n) = value.as_u64() {
                    tantivy_doc.add_u64(tantivy_field, n);
                }
            }
            TantivyFieldType::Date => {
                if let Some(s) = value.as_str()
                    && let Some((tantivy_dt, ts, clamped)) = parse_date_str_to_tantivy(s)
                {
                    if ts != clamped {
                        tracing::debug!(
                            field = %field_name,
                            input = %s,
                            original_ts = %ts,
                            clamped_ts = %clamped,
                            "Date clamped to Tantivy safe range"
                        );
                    }
                    tantivy_doc.add_date(tantivy_field, tantivy_dt);
                }
            }
            TantivyFieldType::Boolean => {
                if let Some(b) = value.as_bool() {
                    tantivy_doc.add_bool(tantivy_field, b);
                }
            }
            TantivyFieldType::Ip => {
                if let Some(s) = value.as_str()
                    && let Ok(ip) = s.parse::<std::net::IpAddr>()
                {
                    let ipv6 = match ip {
                        std::net::IpAddr::V4(ipv4) => ipv4.to_ipv6_mapped(),
                        std::net::IpAddr::V6(ipv6) => ipv6,
                    };
                    tantivy_doc.add_ip_addr(tantivy_field, ipv6);
                }
            }
            TantivyFieldType::Facet => {
                if let Some(s) = value.as_str() {
                    match facet_value(field_name, s) {
                        Ok(facet) => tantivy_doc.add_facet(tantivy_field, facet),
                        Err(err) => match bad_facet {
                            BadFacet::Refuse => return Err(err),
                            BadFacet::SkipAndWarn => tracing::warn!(
                                field = %field_name,
                                error = %err,
                                "Skipping a value that is not a valid facet path during replay"
                            ),
                        },
                    }
                }
            }
            // Handled above, before this loop.
            TantivyFieldType::Text | TantivyFieldType::Json | TantivyFieldType::Bytes => {}
        }
    }

    Ok(())
}

fn parse_date_str_to_tantivy(s: &str) -> Option<(DateTime, i64, i64)> {
    // RFC3339 with offset
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        let ts = dt.timestamp();
        let clamped = ts.clamp(TANTIVY_MIN_TIMESTAMP_SECS, TANTIVY_MAX_TIMESTAMP_SECS);
        let tantivy_dt = DateTime::from_timestamp_secs(clamped);
        return Some((tantivy_dt, ts, clamped));
    }

    // Naive datetime (no timezone) -> assume UTC
    if let Some(ndt) = NAIVE_DATETIME_FORMATS
        .iter()
        .find_map(|fmt| NaiveDateTime::parse_from_str(s, fmt).ok())
    {
        let ts = Utc.from_utc_datetime(&ndt).timestamp();
        let clamped = ts.clamp(TANTIVY_MIN_TIMESTAMP_SECS, TANTIVY_MAX_TIMESTAMP_SECS);
        let tantivy_dt = DateTime::from_timestamp_secs(clamped);
        return Some((tantivy_dt, ts, clamped));
    }

    // Date-only formats that NaiveDate can parse directly (YYYY-MM-DD, YYYY/MM/DD, YYYY.MM.DD, YYYYMMDD)
    for fmt in &["%Y-%m-%d", "%Y/%m/%d", "%Y.%m.%d", "%Y%m%d"] {
        if let Ok(nd) = NaiveDate::parse_from_str(s, fmt)
            && let Some(ndt) = nd.and_hms_opt(0, 0, 0)
        {
            let ts = Utc.from_utc_datetime(&ndt).timestamp();
            let clamped = ts.clamp(TANTIVY_MIN_TIMESTAMP_SECS, TANTIVY_MAX_TIMESTAMP_SECS);
            let tantivy_dt = DateTime::from_timestamp_secs(clamped);
            return Some((tantivy_dt, ts, clamped));
        }
    }

    // Compact datetime: YYYYMMDDHHMMSS or YYYYMMDDHHMM (no separators)
    if s.len() == 14
        && s.chars().all(|c| c.is_ascii_digit())
        && let (Ok(year), Ok(month), Ok(day), Ok(hour), Ok(min), Ok(sec)) = (
            s[0..4].parse::<i32>(),
            s[4..6].parse::<u32>(),
            s[6..8].parse::<u32>(),
            s[8..10].parse::<u32>(),
            s[10..12].parse::<u32>(),
            s[12..14].parse::<u32>(),
        )
        && let Some(nd) = NaiveDate::from_ymd_opt(year, month, day)
        && let Some(ndt) = nd.and_hms_opt(hour, min, sec)
    {
        let ts = Utc.from_utc_datetime(&ndt).timestamp();
        let clamped = ts.clamp(TANTIVY_MIN_TIMESTAMP_SECS, TANTIVY_MAX_TIMESTAMP_SECS);
        let tantivy_dt = DateTime::from_timestamp_secs(clamped);
        return Some((tantivy_dt, ts, clamped));
    }
    if s.len() == 12
        && s.chars().all(|c| c.is_ascii_digit())
        && let (Ok(year), Ok(month), Ok(day), Ok(hour), Ok(min)) = (
            s[0..4].parse::<i32>(),
            s[4..6].parse::<u32>(),
            s[6..8].parse::<u32>(),
            s[8..10].parse::<u32>(),
            s[10..12].parse::<u32>(),
        )
        && let Some(nd) = NaiveDate::from_ymd_opt(year, month, day)
        && let Some(ndt) = nd.and_hms_opt(hour, min, 0)
    {
        let ts = Utc.from_utc_datetime(&ndt).timestamp();
        let clamped = ts.clamp(TANTIVY_MIN_TIMESTAMP_SECS, TANTIVY_MAX_TIMESTAMP_SECS);
        let tantivy_dt = DateTime::from_timestamp_secs(clamped);
        return Some((tantivy_dt, ts, clamped));
    }

    // Unix epoch seconds (pure integer, not a date format)
    // Only attempt this for values that look like reasonable timestamps (10-11 digits for
    // contemporary dates, or smaller for historical). This avoids misinterpreting 4-digit
    // years (already handled above) or 8-digit YYYYMMDD dates (already handled above).
    if (s.len() == 10 || s.len() == 11)
        && s.chars().all(|c| c.is_ascii_digit())
        && let Ok(secs) = s.parse::<i64>()
        && (946_684_800..=10_000_000_000).contains(&secs)
    {
        let clamped = secs.clamp(TANTIVY_MIN_TIMESTAMP_SECS, TANTIVY_MAX_TIMESTAMP_SECS);
        let tantivy_dt = DateTime::from_timestamp_secs(clamped);
        return Some((tantivy_dt, secs, clamped));
    }

    // Year-month format (YYYY-MM) -> first day of month, midnight UTC
    // NaiveDate::parse_from_str cannot parse incomplete dates, so we handle this manually
    if s.len() == 7
        && s.chars().nth(4) == Some('-')
        && let (Ok(year), Ok(month)) = (s[0..4].parse::<i32>(), s[5..7].parse::<u32>())
        && let Some(nd) = NaiveDate::from_ymd_opt(year, month, 1)
        && let Some(ndt) = nd.and_hms_opt(0, 0, 0)
    {
        let ts = Utc.from_utc_datetime(&ndt).timestamp();
        let clamped = ts.clamp(TANTIVY_MIN_TIMESTAMP_SECS, TANTIVY_MAX_TIMESTAMP_SECS);
        let tantivy_dt = DateTime::from_timestamp_secs(clamped);
        return Some((tantivy_dt, ts, clamped));
    }

    // Year-only format (YYYY) -> Jan 1, midnight UTC
    // NaiveDate::parse_from_str cannot parse year-only, so we handle this manually
    if s.len() == 4
        && s.chars().all(|c| c.is_ascii_digit())
        && let Ok(year) = s.parse::<i32>()
        && let Some(nd) = NaiveDate::from_ymd_opt(year, 1, 1)
        && let Some(ndt) = nd.and_hms_opt(0, 0, 0)
    {
        let ts = Utc.from_utc_datetime(&ndt).timestamp();
        let clamped = ts.clamp(TANTIVY_MIN_TIMESTAMP_SECS, TANTIVY_MAX_TIMESTAMP_SECS);
        let tantivy_dt = DateTime::from_timestamp_secs(clamped);
        return Some((tantivy_dt, ts, clamped));
    }

    None
}

/// What happened when `indexed` flags were applied to a stored schema.
///
/// A rejected update applies nothing at all — the lists below then say why, and `applied` is
/// empty. Partially applying a schema edit would leave the caller unable to tell which half
/// took effect.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaFieldUpdate {
    /// Fields whose `indexed` flag changed.
    pub applied: Vec<String>,
    /// Fields already in the requested state, so nothing was written for them.
    pub unchanged: Vec<String>,
    /// Fields the schema does not have. The only reason a request is refused.
    pub unknown: Vec<String>,
    /// Fields marked indexed that the built index has no column for, so the flag takes effect at
    /// the next rebuild rather than now.
    ///
    /// A subset of `applied`: the edit was made. Until the index data is rebuilt from the schema
    /// these fields match nothing, which the query path reports rather than hides.
    pub pending_reindex: Vec<String>,
}

impl SchemaFieldUpdate {
    /// Whether the request was refused, in which case nothing was written.
    ///
    /// Only an unknown field refuses. A field whose flag cannot take effect until the index is
    /// rebuilt is applied and reported, not refused — declaring it is the first step of the
    /// rebuild, so refusing it would block the very workflow that makes it searchable.
    pub fn is_rejected(&self) -> bool {
        !self.unknown.is_empty()
    }
}

/// Index schema definition for validation and evolution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexSchema {
    pub fields: HashMap<String, FieldDef>,
    #[serde(default = "default_version")]
    pub version: u64,
    #[serde(default = "default_timestamp")]
    pub created_at: i64,
    #[serde(default = "default_timestamp")]
    pub updated_at: i64,
    /// What this index holds, in the operator's words.
    ///
    /// The one thing a caller cannot work out from the schema: field names and types describe the
    /// shape of the data, not which dataset it is. Absent unless someone wrote one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Field name to use for routing/sharding (default: "id")
    #[serde(default = "default_routing_field")]
    pub routing_field_name: String,
    /// Pre-computed set of shadow field names for O(1) lookup.
    /// Rebuilt from fields on deserialization via rebuild_shadow_fields_cache().
    #[serde(skip)]
    pub shadow_fields: HashSet<String>,
}

impl Default for IndexSchema {
    fn default() -> Self {
        let now = chrono::Utc::now().timestamp();
        Self {
            fields: HashMap::new(),
            version: 1,
            created_at: now,
            updated_at: now,
            description: None,
            routing_field_name: "id".to_string(),
            shadow_fields: HashSet::new(),
        }
    }
}

impl IndexSchema {
    /// Normalize schema after deserialization from external sources (e.g. Python scripts).
    /// - Populates field `name` from the map key if empty
    /// - Enriches indexed fields with proper Tantivy defaults (tokenizer, fast, etc.)
    /// - Rebuilds shadow fields cache
    pub fn normalize_after_deserialization(&mut self) {
        normalize_description(&mut self.description);
        for (key, field_def) in &mut self.fields {
            // Populate name from map key if not provided in JSON
            if field_def.name.is_empty() {
                field_def.name = key.clone();
            }
            normalize_description(&mut field_def.description);

            // `fast` is three-state on the wire, and this is where it stops being. `None` — the
            // caller said nothing — becomes the default for the type; a value the caller did
            // declare, `true` or `false`, is left exactly as it arrived. Resolved before the
            // arms below so no arm can reach an unresolved value, and before the `id` and
            // shadow shortcuts so every field in a normalized schema names a concrete boolean.
            //
            // The numeric arm used to end with an unconditional `field_def.fast = true;`. It
            // read as a default and behaved as an assignment, so a declared `false` was
            // overwritten on every deserialization — the schema said one thing and the index
            // did another.
            let resolved_fast = field_def.is_fast();
            field_def.fast = Some(resolved_fast);

            // The 'id' field has fixed Tantivy attributes regardless of user input, and the
            // type is one of them. The index builder skips `id` entirely and creates the key
            // itself: raw-tokenized, stored, never fast, whatever the schema declared. A
            // declared type is therefore fiction the rest of the engine goes on believing —
            // `describe_index` reports it, the slow write validation infers `Text` for the key
            // and refuses every document against an `i64` declaration, and a sort merge asked
            // to key a `date` field parses identifiers as dates, fails, and returns an
            // arbitrary order. Pinning both here is what keeps the schema and the index the
            // same shape, as `can_be_fast` does for the types that carry no column.
            if key == "id" {
                field_def.field_type = TantivyFieldType::Text;
                field_def.fast = Some(false);
                field_def.indexed = true;
                field_def.stored = true;
                field_def.tokenizer = Some("raw".to_string());
                field_def.index_record_option = Some("Basic".to_string());
                continue;
            }

            // Skip enrichment for shadow fields and non-indexed fields
            if field_def.is_shadow || !field_def.indexed {
                continue;
            }

            // Enrich with Tantivy defaults based on field type
            match field_def.field_type {
                TantivyFieldType::Text => {
                    // Set default tokenizer if not specified
                    if field_def.tokenizer.is_none() {
                        field_def.tokenizer = Some("default".to_string());
                    }
                    // Set default index record option if not specified
                    if field_def.index_record_option.is_none() {
                        field_def.index_record_option = Some("WithFreqsAndPositions".to_string());
                    }
                }
                TantivyFieldType::String => {
                    // STRING uses raw tokenizer with Basic index option
                    if field_def.tokenizer.is_none() {
                        field_def.tokenizer = Some("raw".to_string());
                    }
                    if field_def.index_record_option.is_none() {
                        field_def.index_record_option = Some("Basic".to_string());
                    }
                }
                // Numeric, date and the remaining types need no enrichment here. Their one
                // default — the fast column — is resolved above, from `fast_by_default`.
                _ => {}
            }
        }
        // `_seq` is deliberately not inserted. It used to be forced into every schema so the
        // Tantivy index would carry a column for the checkpoint scan to order on; the commit
        // payload made that scan a fallback, so new indices no longer declare the field. A
        // schema loaded from an index that already has it keeps it — it arrives in `fields`
        // from disk and nothing here removes it.

        self.rebuild_shadow_fields_cache();
    }

    /// A hash of this schema's field names, for comparing two schemas cheaply.
    ///
    /// Computed on demand rather than stored, which is what keeps it honest: a value carried
    /// in the struct has to be recomputed everywhere `fields` changes, and it was not — the
    /// orchestrator's own evolution path never touched it, so a schema's fingerprint routinely
    /// described a shape it no longer had. At 724ns for a twenty-field schema there is nothing
    /// to save by caching it.
    ///
    /// This answers "are these the same fields?", never "which schema is this?". An index is
    /// identified by its name, which every caller already holds; a hash used as a lookup key
    /// instead collides across indexes of the same shape, and a reverse lookup that did exactly
    /// that handed one index's schema to another. Names are separated by a byte no field name
    /// may contain, so `{"ab", "c"}` and `{"a", "bc"}` no longer hash alike.
    pub fn calculate_fingerprint(&self) -> u64 {
        let mut sorted_names: Vec<&String> = self.fields.keys().collect();
        sorted_names.sort();
        let mut combined = Vec::new();
        for name in sorted_names {
            combined.extend_from_slice(name.as_bytes());
            combined.push(0);
        }
        xxh3_64(&combined)
    }

    /// Check operator-supplied descriptions against their limits.
    ///
    /// Rejected rather than truncated: a description cut off mid-sentence still reads as the
    /// whole statement, and the caller who wrote it is the only one who can say what to drop.
    ///
    /// Counted in characters rather than bytes, so a description in a non-ASCII script gets the
    /// same allowance as one in English.
    pub fn validate_descriptions(&self) -> Result<(), String> {
        if let Some(text) = &self.description {
            let length = text.chars().count();
            if length > MAX_INDEX_DESCRIPTION_CHARS {
                return Err(format!(
                    "index description is {length} characters; the limit is \
                     {MAX_INDEX_DESCRIPTION_CHARS}"
                ));
            }
        }

        let mut named: Vec<&String> = self
            .fields
            .iter()
            .filter(|(_, def)| {
                def.description
                    .as_ref()
                    .is_some_and(|text| text.chars().count() > MAX_FIELD_DESCRIPTION_CHARS)
            })
            .map(|(name, _)| name)
            .collect();
        // A HashMap iterates in an arbitrary order, and an error naming a different field on
        // each attempt is one an operator cannot work through.
        named.sort();

        if let Some(name) = named.first() {
            let length = self.fields[*name]
                .description
                .as_ref()
                .map_or(0, |text| text.chars().count());
            let rest = named.len() - 1;
            let and_others = match rest {
                0 => String::new(),
                1 => " (and 1 other field)".to_string(),
                n => format!(" (and {n} other fields)"),
            };
            return Err(format!(
                "description for field '{name}' is {length} characters; the limit is \
                 {MAX_FIELD_DESCRIPTION_CHARS}{and_others}"
            ));
        }

        Ok(())
    }

    /// Rebuild shadow_fields cache from fields HashMap.
    /// Must be called after deserialization (shadow_fields is #[serde(skip)]).
    pub fn rebuild_shadow_fields_cache(&mut self) {
        self.shadow_fields = self
            .fields
            .iter()
            .filter(|(_, def)| def.is_shadow)
            .map(|(name, _)| name.clone())
            .collect();
    }

    /// Check if there are any shadow fields — zero-cost early exit
    pub fn has_shadow_fields(&self) -> bool {
        !self.shadow_fields.is_empty()
    }

    /// Get the routing field name (defaults to "id")
    pub fn get_routing_field(&self) -> &str {
        if self.routing_field_name.is_empty() {
            "id"
        } else {
            &self.routing_field_name
        }
    }

    /// Set the routing field (validates field exists in schema)
    pub fn set_routing_field(&mut self, field_name: String) -> Result<(), String> {
        if !self.fields.contains_key(&field_name) {
            return Err(format!("Field '{}' does not exist in schema", field_name));
        }
        self.routing_field_name = field_name;
        Ok(())
    }

    /// Auto-detect and set routing field using priority algorithm
    /// Priority: id → hash fields (sha256/sha1/md5) → *_id suffix → *id* substring → first sorted field
    pub fn auto_detect_routing_field(&mut self) {
        if self.fields.contains_key("id") {
            self.routing_field_name = "id".to_string();
            return;
        }
        for hash in &["sha256", "sha1", "md5"] {
            if self.fields.contains_key(*hash) {
                self.routing_field_name = hash.to_string();
                return;
            }
        }
        for name in self.fields.keys() {
            let lower = name.to_lowercase();
            if lower.ends_with("id") || lower.ends_with("_id") {
                self.routing_field_name = name.clone();
                return;
            }
        }
        for name in self.fields.keys() {
            if name.to_lowercase().contains("id") {
                self.routing_field_name = name.clone();
                return;
            }
        }
        let mut sorted: Vec<&String> = self.fields.keys().collect();
        sorted.sort();
        self.routing_field_name = sorted
            .first()
            .map(|s| s.to_string())
            .unwrap_or_else(|| "id".to_string());
    }

    /// Add or evolve a field based on JSON value (schema evolution)
    /// New fields are added as non-indexed to avoid Tantivy schema rebuilds.
    /// Existing fields can have their types evolved if compatible.
    pub fn evolve_field(&mut self, name: String, value: &JsonValue) -> bool {
        use std::collections::hash_map::Entry;

        // CRITICAL: Never evolve the mandatory 'id' field
        if name == "id" {
            return false; // id field is mandatory and should never evolve
        }

        // CRITICAL: Never evolve shadow fields - they preserve their special status
        if let Some(field_def) = self.fields.get(&name)
            && field_def.is_shadow
        {
            return false; // shadow fields should never evolve
        }

        let inferred_type = FieldDef::infer_type_from_value(value);

        let changed = match self.fields.entry(name.clone()) {
            Entry::Vacant(entry) => {
                // New field - create as non-indexed for background evolution
                // This allows the field to be stored in redb without requiring
                // Tantivy schema changes. Fields can be promoted to indexed later.
                let field_def = FieldDef::new_non_indexed(name, value);
                entry.insert(field_def);
                true
            }
            Entry::Occupied(mut entry) => {
                // Existing field - check if type evolution is needed
                let current_def = entry.get();

                // Only evolve if the inferred type is "more specific" or compatible
                if Self::should_evolve_field_static(current_def, inferred_type.clone()) {
                    let mut new_def = current_def.clone();
                    new_def.field_type = inferred_type;
                    entry.insert(new_def);
                    true
                } else {
                    false
                }
            }
        };

        if changed {
            self.updated_at = chrono::Utc::now().timestamp();
        }

        changed
    }

    /// Add a shadow field to the schema
    /// Shadow fields preserve original field names when ID is copied to canonical "id" field
    /// They are NOT indexed and NOT stored in Tantivy
    pub fn add_shadow_field(&mut self, name: String, field_type: TantivyFieldType) -> bool {
        // Don't add shadow field if it already exists
        if self.fields.contains_key(&name) {
            return false;
        }

        let field_def = FieldDef::new_shadow(name.clone(), field_type);
        self.shadow_fields.insert(name.clone());
        self.fields.insert(name, field_def);
        true
    }

    /// Check if a field is a shadow field — O(1) via pre-computed set
    pub fn is_shadow_field(&self, field_name: &str) -> bool {
        self.shadow_fields.contains(field_name)
    }

    /// Determine if a field should evolve to a new type (static version to avoid borrowing issues)
    fn should_evolve_field_static(current: &FieldDef, new_type: TantivyFieldType) -> bool {
        // Don't evolve if types are the same
        if current.field_type == new_type {
            return false;
        }

        // Evolution rules - only allow certain upgrades
        match (&current.field_type, new_type) {
            // Text can be refined to more specific types
            (TantivyFieldType::Text, TantivyFieldType::Date) => true,
            (TantivyFieldType::Text, TantivyFieldType::Ip) => true,
            (TantivyFieldType::Text, TantivyFieldType::I64) => true,
            (TantivyFieldType::Text, TantivyFieldType::U64) => true,
            (TantivyFieldType::Text, TantivyFieldType::F64) => true,
            (TantivyFieldType::Text, TantivyFieldType::Boolean) => true,
            (TantivyFieldType::Text, TantivyFieldType::Json) => true,

            // Numeric types can be upgraded to more general types
            (TantivyFieldType::I64, TantivyFieldType::F64) => true,
            (TantivyFieldType::U64, TantivyFieldType::F64) => true,

            // String can be upgraded to Text (for tokenization)
            (TantivyFieldType::String, TantivyFieldType::Text) => true,

            _ => false, // Prevent downgrades or incompatible changes
        }
    }

    /// Evolve schema based on a JSON document
    pub fn evolve_from_document(&mut self, json_blob: &JsonValue) -> Vec<String> {
        let mut evolved_fields = Vec::new();

        if let Some(obj) = json_blob.as_object() {
            for (field_name, field_value) in obj {
                if self.evolve_field(field_name.clone(), field_value) {
                    evolved_fields.push(field_name.clone());
                }
            }
        }

        evolved_fields
    }

    /// Promote a field from non-indexed to indexed status
    /// This requires a Tantivy schema rebuild and should be done explicitly.
    /// Returns true if the field was promoted, false if it was already indexed or doesn't exist.
    pub fn promote_field_to_indexed(&mut self, field_name: &str) -> bool {
        if let Some(field_def) = self.fields.get_mut(field_name)
            && !field_def.indexed
        {
            field_def.indexed = true;
            tracing::info!(
                field = %field_name,
                field_type = ?field_def.field_type,
                "Promoted field to indexed status - requires Tantivy schema rebuild"
            );
            return true;
        }
        false
    }

    /// Get all non-indexed fields in the schema
    /// Useful for identifying fields that can be promoted to indexed status.
    pub fn get_non_indexed_fields(&self) -> Vec<String> {
        self.fields
            .iter()
            .filter(|(_, field_def)| !field_def.indexed)
            .map(|(name, _)| name.clone())
            .collect()
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

    #[error("invalid index name: {0}")]
    InvalidIndexName(String),

    /// A document value the field's type cannot hold. The caller's fault, not the node's, which
    /// is why it is its own variant rather than an `Io` — the HTTP layer answers `400` on the
    /// `InvalidInput` kind and would otherwise call a bad document an internal error.
    #[error("invalid value for field '{field}': {reason}")]
    InvalidFieldValue { field: String, reason: String },
}

/// Resolve an index name to its on-disk directory under `indices_base`.
///
/// The index name must be exactly one normal path component. This is a purely
/// lexical check — it needs no filesystem access, so it holds for indexes that
/// do not exist yet (the case where a traversal attempt would otherwise create
/// a directory outside the shard). Rejects `..`, `.`, absolute paths, path
/// separators, Windows prefixes, and the empty string.
fn resolve_index_dir(indices_base: &Path, index: &str) -> Result<PathBuf, StoreError> {
    let mut components = Path::new(index).components();
    let is_single_normal_component = matches!(
        (components.next(), components.next()),
        (Some(std::path::Component::Normal(_)), None)
    );
    if !is_single_normal_component {
        return Err(StoreError::InvalidIndexName(format!(
            "'{}' must be a single path component without separators or '..'",
            index
        )));
    }
    Ok(indices_base.join(index))
}

/// A facet path Tantivy will accept, or the reason it will not.
///
/// **The panic this exists to prevent is not hypothetical machinery.** `Facet: From<&str>` is
/// `Facet::from_text(path).unwrap()`, and `from_text` refuses a value that is empty or does not
/// begin with `/`. So `add_facet(field, "electronics/phones")` panics — on the shard's writer
/// thread, from a document body, with `panic = "abort"` in the release profile, which takes the
/// process down rather than the request.
///
/// Nothing reaches it today: the orchestrator's schema validation infers `Text` from every JSON
/// string and refuses it against a declared `Facet` field, which is why facet fields cannot be
/// written to at all (ROADMAP OB2). That refusal is load-bearing by accident, and it is the wrong
/// thing to be relying on — the value is checked here, at the point it enters the index, so that
/// making facets writable is a change to what is accepted rather than a new way to abort a node.
///
/// Delegates to `from_text` rather than checking the shape by hand: escaping (`\/` for a literal
/// slash inside a segment) is its rule to define, and a second implementation of it here would be
/// a second implementation to drift.
fn facet_value(field: &str, value: &str) -> Result<Facet, StoreError> {
    Facet::from_text(value).map_err(|_| StoreError::InvalidFieldValue {
        field: field.to_string(),
        reason: format!(
            "'{value}' is not a facet path; a facet path begins with '/' and names its levels in \
             order, as in '/electronics/phones'. Escape a literal slash inside a level as '\\/'"
        ),
    })
}

/// Write-Ahead Log operations for atomic dual-write.
/// Write-Ahead Log operations for atomic dual-write.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalOp {
    Put {
        id: String,
        json_blob: Option<JsonValue>,
    },
    Delete {
        id: String,
    },
}

/// Helper struct for zero-copy serialization of stored documents
#[derive(Serialize)]
struct StoredDoc<'a> {
    json_blob: Option<&'a JsonValue>,
}

/// Owned version for deserialization from redb
#[derive(Serialize, Deserialize)]
struct StoredDocOwned {
    json_blob: Option<JsonValue>,
}

/// Reconstruct shadow fields in JSON blob for document retrieval
///
/// This function reconstructs shadow fields by copying the canonical "id" value
/// to shadow field names, restoring the original document structure.
///
/// Performance Note: This takes a reference (&JsonValue) and creates a new map
/// to guarantee field ordering (id → shadow → rest). For zero-copy performance,
/// consider an owned version if ordering requirements are flexible.
///
/// Example:
/// Input: {"id": "123", "title": "Book"} with shadow mapping {"book_id": "id"}
/// Output: {"id": "123", "book_id": "123", "title": "Book"}  // book_id reconstructed
/// The name a returned document carries its key under.
///
/// `id` normally, but an index with shadow fields answers with the shadow name *instead of*
/// `id` — that is what a shadow field is for. Anything that reads the key back off a hit has to
/// ask for it by this name: a post-fetch sort, a cross-shard merge, a projection.
///
/// Several shadow fields all stand for the same key and reconstruction writes every one of
/// them, so any would do; the first in sorted order is chosen to keep one shard's answer the
/// same as another's.
pub fn document_key_field(schema: &IndexSchema) -> String {
    schema
        .shadow_fields
        .iter()
        .min()
        .cloned()
        .unwrap_or_else(|| "id".to_string())
}

/// Reconstruct shadow fields by consuming the input (Ownership Transfer).
///
/// The `doc_id` parameter provides the canonical document identifier from the redb key
/// or tantivy stored field. This is used as the authoritative ID source when the blob
/// does not contain an "id" field (avoiding redundant storage of the key inside the body).
///
/// Behavior:
/// 1. If NO shadow fields exist: Ensures 'id' is the first field in the JSON object.
/// 2. If shadow fields EXIST: Replaces 'id' with the shadow field(s) (e.g., returns 'book_id' instead of 'id').
///
/// This avoids cloning the bulk of the document (original fields) by using `append`.
pub fn reconstruct_shadow_fields_owned(
    json_blob: JsonValue,
    schema: &IndexSchema,
    doc_id: &str,
) -> JsonValue {
    // Fast fail if not an object
    let mut obj = match json_blob {
        JsonValue::Object(map) => map,
        _ => return json_blob,
    };

    // CASE 1: No Shadow Fields -> Strict ID Ordering
    if schema.shadow_fields.is_empty() {
        // Fast Path: Check if 'id' is already first (O(1) check)
        if let Some(first_key) = obj.keys().next()
            && first_key == "id"
        {
            return JsonValue::Object(obj);
        }

        // Reorder: move existing "id" to front, or inject from doc_id
        let id_val = obj
            .remove("id")
            .unwrap_or_else(|| serde_json::Value::String(doc_id.to_string()));
        let mut out = JsonMap::with_capacity(obj.len() + 1);
        out.insert("id".to_string(), id_val);
        out.append(&mut obj); // Moves pointers only
        return JsonValue::Object(out);
    }

    // CASE 2: Shadow Fields Exist -> Replace ID with Shadow Field(s)

    // Resolve the canonical ID: prefer blob's "id", fall back to doc_id (redb key)
    let id_val = obj
        .remove("id")
        .unwrap_or_else(|| serde_json::Value::String(doc_id.to_string()));

    // Sorted, so a hit's field order does not depend on set iteration order.
    let mut shadow_names: Vec<&String> = schema.shadow_fields.iter().collect();
    shadow_names.sort_unstable();

    let mut out = JsonMap::with_capacity(obj.len() + shadow_names.len());
    for name in shadow_names {
        out.insert(name.clone(), id_val.clone());
    }
    // Note: We deliberately SKIP inserting "id" here.
    // The shadow field replaces it in the presentation layer.

    // Move remaining original fields (bulk data)
    out.append(&mut obj);

    JsonValue::Object(out)
}

/// Optimized: Filter shadow fields in-place using retain.
///
/// This avoids allocating a new map and cloning keys/values.
fn filter_shadow_fields_owned(mut json_blob: JsonValue, schema: &IndexSchema) -> JsonValue {
    if let JsonValue::Object(ref mut map) = json_blob {
        // retain is O(n) scan but O(0) allocation
        map.retain(|key, _| !schema.is_shadow_field(key));
    }
    json_blob
}

/// Internal schema field mappings for Tantivy.
#[derive(Debug, Clone)]
pub struct SchemaFields {
    /// Tantivy field for the document identifier
    id: Field,
    /// Tantivy field for the WAL sequence number, on indices that were built with one.
    ///
    /// `None` on anything built after the field stopped being declared. It exists only to let
    /// `get_highest_indexed_seq` locate a checkpoint by scanning, and the commit payload does
    /// that in O(1) now — so it is written when present, purely so an index built by an older
    /// build keeps the column its own last-resort scan would read.
    seq: Option<Field>,
    /// Map of schema field name -> Tantivy field (only indexed fields are present)
    indexed_fields: HashMap<String, Field>,
}

/// Unified cache entry for index sizes (both Tantivy directory and Redb table) with timestamp
#[derive(Clone)]
struct IndexSizeCache {
    tantivy_bytes: u64,
    redb_bytes: u64,
    document_count: u64,
    timestamp: Instant,
}

/// One index's cached document bodies, and the generation that says whether they are current.
///
/// The bodies mirror rows in `data_<index>`, so any write to that table makes the entries for
/// the ids it touched wrong. Removing those entries is not enough on its own: a reader that
/// began its redb transaction before the write commits legitimately sees the pre-write row, and
/// if it caches that body *after* the write has invalidated it, the stale value is back and
/// nothing will remove it again.
///
/// `generation` closes that window. A reader reads it before opening its transaction and passes
/// it back to `insert_into_cache`, which declines to cache anything if a write has bumped it
/// since. Reader and writer both hold the same `DashMap` entry guard while they touch this
/// struct, so the check and the insert cannot interleave with a bump and a removal — whichever
/// side takes the guard first, the outcome is a cache without a stale body in it.
#[derive(Default)]
struct IndexReadCache {
    entries: HashMap<String, Vec<u8>>,
    generation: u64,
}

/// Result of batch index size measurement
struct IndexSizes {
    tantivy_bytes: u64,
    redb_bytes: u64,
    document_count: u64,
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

/// Build the per-field `InvertedIndexReader`s (term dictionaries) a query needs, so the first
/// real query pays for neither opening them nor the page faults behind them.
///
/// Only inverted indexes are warmed, because in tantivy 0.26 they are the only thing a
/// `SegmentReader` keeps: `inverted_index()` memoizes into `inv_idx_reader_cache`, so every
/// later query on this generation reuses the work. The two obvious neighbours do not memoize
/// and were dropped from this function:
///
/// - fast fields — `FastFieldReaders::u64()` and friends go through `read_columns()`, which
///   re-reads the columnar on every call; nothing is cached to hand the next query.
/// - the doc store — `SegmentReader::get_store_reader()` returns a *fresh* `StoreReader` with
///   its own block cache, dropped the moment warming returns, and the searcher builds its own
///   store readers regardless.
///
/// Warming those two bought page-cache residency and nothing else — which a segment this
/// process just wrote already has. What remains here is per *searcher generation*, not per
/// segment: tantivy rebuilds every `SegmentReader` on reload, so the cache dies with the
/// generation that owned it.
fn warm_segment(index: &str, segment_reader: &tantivy::SegmentReader) {
    for (field, field_entry) in segment_reader.schema().fields() {
        if !field_entry.is_indexed() {
            continue;
        }
        // Builds and caches this field's InvertedIndexReader (term dictionary).
        if let Err(e) = segment_reader.inverted_index(field) {
            trace!(
                index = %index,
                field = field_entry.name(),
                error = %e,
                "Warmup: could not open inverted index for field"
            );
        }
    }
}

/// Multi-tenant hybrid storage engine combining redb and tantivy.
pub struct HybridStore {
    /// Shared redb database across all indices
    kv: Database,
    /// Cache of IndexWriters keyed by index name
    writers: Arc<DashMap<String, Arc<Mutex<IndexWriter>>>>,
    /// Cache of IndexReaders keyed by index name
    readers: Arc<DashMap<String, IndexReader>>,
    /// Atomic counters for WAL sequence IDs per index
    current_seq: Arc<DashMap<String, AtomicU64>>,
    /// Operation counters for smart commits per index
    operations_counter: Arc<DashMap<String, AtomicU64>>,
    /// Simple per-index read cache for frequently accessed documents
    read_cache: Arc<DashMap<String, IndexReadCache>>,
    /// Cache of optimal memory budgets per index to avoid frequent syscalls
    budget_cache: Arc<DashMap<String, usize>>,
    /// Cache of schemas per index to avoid repeated redb reads
    schema_cache: Arc<DashMap<String, Arc<IndexSchema>>>,
    /// Cache of Tantivy field mappings per index
    fields_cache: Arc<DashMap<String, SchemaFields>>,
    /// Per-index initialization locks, serializing concurrent `get_or_create_index` calls.
    /// Tantivy's `INDEX_WRITER_LOCK` is a non-blocking flock on `.tantivy-writer.lock`, so
    /// two threads opening a writer for the same index race and one fails with `LockBusy`.
    index_init_locks: Arc<DashMap<String, Arc<Mutex<()>>>>,
    /// Searcher generation last warmed, per index. A searcher whose generation is unchanged
    /// holds the same `SegmentReader`s with the same filled caches, so re-warming it is
    /// pointless — this makes repeated warm requests for an idle index free.
    warmed_generations: Arc<DashMap<String, u64>>,
    /// Per-index warmup lifecycle state, for observability.
    warmup_states: Arc<DashMap<String, IndexWarmupState>>,
    /// Unified cache for index sizes (Tantivy + Redb) with expiration to avoid repeated expensive calculations
    index_size_cache: Arc<Mutex<HashMap<String, IndexSizeCache>>>,
    /// Cache expiration duration for index sizes (1 hour)
    index_cache_expiry: Duration,
    /// Storage configuration
    config: StorageConfig,
}

impl HybridStore {
    /// Calculate tiered cache sizes based on database file size, system memory, and shard count.
    /// Returns the normal cache size in bytes.
    ///
    /// Memory is divided by max_shards to ensure we don't exceed system limits
    /// when multiple shards are initialized on the same node.
    fn calculate_cache_size(
        config: &StorageConfig,
        db_file_size_bytes: u64,
        total_shards: usize,
    ) -> usize {
        use sysinfo::{MemoryRefreshKind, System};

        const MIN_CACHE_BYTES: u64 = 32 * 1024 * 1024; // 32MB safety floor per shard

        let shard_count = total_shards.max(1) as u64;

        // Use configured total limit when provided, otherwise fall back to host memory stats
        let (total_memory_bytes, available_memory_bytes) = if config.total_memory_limit_bytes > 0 {
            let pressure = config.memory_pressure_threshold_percent.clamp(1, 100) as u64;
            let total = config.total_memory_limit_bytes;
            let available = total.saturating_mul(pressure) / 100;
            (total, available.max(MIN_CACHE_BYTES * shard_count))
        } else {
            let mut system = System::new();
            system.refresh_memory_specifics(MemoryRefreshKind::everything());
            let total = system.total_memory();
            let available = if system.available_memory() > 0 {
                system.available_memory()
            } else {
                total / 4
            };
            (total, available)
        };

        let cache_pool_bytes = (available_memory_bytes / 4).max(MIN_CACHE_BYTES * shard_count);
        let total_pool_bytes = (total_memory_bytes / 2).max(MIN_CACHE_BYTES * shard_count);

        let per_shard_available = cache_pool_bytes / shard_count;
        let per_shard_total = total_pool_bytes / shard_count;

        // Base standard cache sizes by database tier (before per-shard limits)
        let base_standard_cache = if db_file_size_bytes < 1024 * 1024 {
            32 * 1024 * 1024
        } else if db_file_size_bytes < 100 * 1024 * 1024 {
            64 * 1024 * 1024
        } else if db_file_size_bytes < 1024 * 1024 * 1024 {
            128 * 1024 * 1024
        } else {
            256 * 1024 * 1024
        };

        // Apply per-shard memory caps
        let standard_cache = (base_standard_cache as u64)
            .min(per_shard_available)
            .min(per_shard_total) as usize;

        tracing::info!(
            file_size_mb = db_file_size_bytes / (1024 * 1024),
            available_memory_mb = available_memory_bytes / (1024 * 1024),
            total_memory_mb = total_memory_bytes / (1024 * 1024),
            max_shards = shard_count,
            per_shard_available_mb = per_shard_available / (1024 * 1024),
            standard_cache_mb = standard_cache / (1024 * 1024),
            "HybridStore: calculated cache size (per-shard)"
        );

        standard_cache
    }

    /// Creates a new multi-tenant HybridStore with per-shard cache sizing.
    /// Cache size is divided by total_shards to prevent OOM when multiple
    /// shards are initialized on the same node.
    ///
    /// # Arguments
    /// * `config` - Storage configuration
    /// * `total_shards` - Total number of shards on this node (for per-shard memory budgeting)
    pub fn new(config: StorageConfig, total_shards: usize) -> Result<Self, StoreError> {
        let init_start = Instant::now();
        tracing::info!(
            shard_path = %config.shard_path.display(),
            "HybridStore: initializing shard storage with tiered cache"
        );

        // Create directory structure
        let dir_start = Instant::now();
        fs::create_dir_all(&config.shard_path)?;
        let kv_path = config.shard_path.join("store.redb");
        let indices_path = config.shard_path.join("indices");
        fs::create_dir_all(&indices_path)?;
        let dir_elapsed = dir_start.elapsed();
        tracing::debug!(
            shard_path = %config.shard_path.display(),
            indices_path = %indices_path.display(),
            elapsed_ms = dir_elapsed.as_millis(),
            "HybridStore: ensured directory structure"
        );

        let db_file_exists = kv_path.exists();
        let db_file_size = if db_file_exists {
            fs::metadata(&kv_path)?.len()
        } else {
            0
        };

        // Calculate tiered cache sizes based on file size and shard count
        let normal_cache_size = Self::calculate_cache_size(&config, db_file_size, total_shards);

        let kv = if db_file_exists {
            // EXISTING DATABASE: Open directly with normal cache.
            // The init boost cache was removed — recovery now uses persisted
            // committed seq from TABLE_RECOVERY_META, so no large cache is
            // needed for metadata loading during startup.
            tracing::info!(
                db_path = %kv_path.display(),
                normal_cache_mb = normal_cache_size / (1024 * 1024),
                "HybridStore: Opening existing database"
            );

            let mut builder = redb::Builder::new();
            builder.set_cache_size(normal_cache_size);
            builder.open(&kv_path)?
        } else {
            // NEW DATABASE: Just create with normal cache
            tracing::info!(
                db_path = %kv_path.display(),
                cache_mb = normal_cache_size / (1024 * 1024),
                "HybridStore: Creating new database with standard cache"
            );

            let mut builder = redb::Builder::new();
            builder.set_cache_size(normal_cache_size);
            builder.create(&kv_path)?
        };

        let total_elapsed = init_start.elapsed();
        tracing::info!(
            shard_path = %config.shard_path.display(),
            db_path = %kv_path.display(),
            existed = db_file_exists,
            file_size_mb = db_file_size / (1024 * 1024),
            normal_cache_mb = normal_cache_size / (1024 * 1024),
            elapsed_ms = total_elapsed.as_millis(),
            "HybridStore: initialization complete"
        );

        Ok(HybridStore {
            kv,
            writers: Arc::new(DashMap::new()),
            readers: Arc::new(DashMap::new()),
            current_seq: Arc::new(DashMap::new()),
            operations_counter: Arc::new(DashMap::new()),
            read_cache: Arc::new(DashMap::new()),
            budget_cache: Arc::new(DashMap::new()),
            schema_cache: Arc::new(DashMap::new()),
            fields_cache: Arc::new(DashMap::new()),
            index_init_locks: Arc::new(DashMap::new()),
            warmed_generations: Arc::new(DashMap::new()),
            warmup_states: Arc::new(DashMap::new()),
            index_size_cache: Arc::new(Mutex::new(HashMap::new())),
            index_cache_expiry: Duration::from_secs(3600), // 1 hour
            config: config.clone(),
        })
    }

    /// Gracefully shutdown the HybridStore, releasing all locks and resources
    pub fn shutdown(&self) -> Result<(), StoreError> {
        tracing::info!("HybridStore: Starting graceful shutdown");

        // Check which indices have pending operations
        let indices_with_pending_ops: Vec<String> = self
            .operations_counter
            .iter()
            .filter(|entry| entry.value().load(Ordering::SeqCst) > 0)
            .map(|entry| entry.key().clone())
            .collect();

        if indices_with_pending_ops.is_empty() {
            tracing::info!("No pending operations, skipping commits during shutdown");
        } else {
            tracing::info!(
                indices_count = indices_with_pending_ops.len(),
                indices = ?indices_with_pending_ops,
                "Committing indices with pending operations during shutdown"
            );
        }

        // Commit only writers with pending operations
        for entry in self.writers.iter() {
            let index = entry.key();
            let writer_arc = entry.value();
            if indices_with_pending_ops.contains(index) {
                // Capture the sequence before committing — see commit_index for why the
                // checkpoint must never claim a sequence allocated after the commit started.
                let committed_seq = self
                    .current_seq
                    .get(index)
                    .map(|counter| counter.load(Ordering::SeqCst));

                // Retry with 5s timeout to handle slow writer thread lock release
                let writer = {
                    let start = std::time::Instant::now();
                    let timeout = std::time::Duration::from_secs(5);
                    loop {
                        match writer_arc.try_lock() {
                            Ok(guard) => break Some(guard),
                            Err(_) if start.elapsed() < timeout => {
                                std::thread::sleep(std::time::Duration::from_millis(10));
                            }
                            Err(_) => {
                                tracing::error!(index = %index, "Writer lock timeout during shutdown, skipping commit — data may be lost");
                                break None;
                            }
                        }
                    }
                };
                if let Some(mut w) = writer {
                    tracing::debug!(index = %index, "Committing index during shutdown");
                    let outcome = match committed_seq {
                        Some(seq) => commit_writer_at(&mut w, seq).map(|()| 0),
                        None => w.commit().map_err(StoreError::from),
                    };
                    match outcome {
                        Ok(_) => {
                            // Release the writer lock before touching redb.
                            drop(w);
                            // Checkpoint what we just made durable. Without this the next
                            // startup sees a stale recovery sequence and replays the entire
                            // tail of the WAL even though it is all already in Tantivy.
                            if let Some(seq) = committed_seq
                                && let Err(e) = self.checkpoint_committed(index, seq)
                            {
                                tracing::warn!(
                                    index = %index,
                                    error = %e,
                                    "Failed to checkpoint on shutdown; next startup will replay the WAL tail"
                                );
                            }
                        }
                        Err(e) => {
                            tracing::warn!(index = %index, error = %e, "Failed to commit index during shutdown");
                        }
                    }
                }
            } else {
                tracing::debug!(index = %index, "No pending operations, skipping commit during shutdown");
            }
        }

        // Release what holds file handles, rather than leaving it to whenever the last `Arc`
        // to this store happens to drop: `readers` holds every open index's mmaps and
        // `writers` holds tantivy's `.tantivy-writer.lock` flock. Dropping an `IndexWriter`
        // joins its merge threads, so that cost lands here — inside the caller's shutdown
        // timeout — instead of on whichever thread outlives it.
        self.writers.clear();
        self.readers.clear();

        // Clear all caches
        self.schema_cache.clear();
        self.budget_cache.clear();
        self.operations_counter.clear();
        self.current_seq.clear();
        self.index_size_cache.lock().unwrap().clear();

        // Force a final redb fsync/flush to reduce WAL replay on startup
        let redb_start = std::time::Instant::now();
        match self.kv.begin_write() {
            Ok(mut txn) => {
                if let Err(e) = txn.set_durability(Durability::Immediate) {
                    tracing::warn!(error = %e, "Failed to set durability on shutdown flush");
                } else if let Err(e) = txn.commit() {
                    tracing::warn!(error = %e, "Failed to commit shutdown flush transaction");
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "Failed to open shutdown flush transaction");
            }
        }
        let redb_elapsed = redb_start.elapsed();
        if redb_elapsed > std::time::Duration::from_secs(10) {
            tracing::warn!(elapsed = ?redb_elapsed, "Redb shutdown flush exceeded 10s");
        } else {
            tracing::debug!(elapsed = ?redb_elapsed, "Redb shutdown flush completed");
        }

        tracing::info!("HybridStore: Graceful shutdown completed");
        Ok(())
    }

    /// Whether an `IndexWriter` is currently open for `index`.
    ///
    /// A writer is the expensive resource in this engine — worker threads plus an indexing
    /// arena — so this answers "did that operation have to open one", which is the difference
    /// between a boot that scales with in-flight writes and one that scales with stored data.
    pub fn has_open_writer(&self, index: &str) -> bool {
        self.writers.contains_key(index)
    }

    /// Forcefully remove a writer from cache, even if locked.
    /// Last-resort operation for stuck writers. WARNING: May cause data loss.
    /// Returns true if writer was removed, false if not found.
    pub fn force_remove_writer(&self, index: &str) -> bool {
        if let Some((_, writer_arc)) = self.writers.remove(index) {
            if writer_arc.try_lock().is_ok() {
                tracing::warn!(index = %index, "Force-removing writer (lock available)");
            } else {
                tracing::error!(index = %index, "Force-removing LOCKED writer - data loss possible");
            }
            drop(writer_arc);
            true
        } else {
            tracing::debug!(index = %index, "No writer to force-remove");
            false
        }
    }

    /// Highest `_seq` present in the index, found by ordering on the `_seq` fast field.
    ///
    /// Last resort only. Despite returning a single document this reads the `_seq` column of
    /// every segment, so it costs O(segments × docs) — on a multi-terabyte index, minutes.
    /// It is reachable for one index at most once: an index that still has a WAL tail and
    /// whose last commit carries neither a payload stamp nor a `_recovery_meta` row. The
    /// commit that ends that replay stamps a payload, and the path is dead for that index
    /// from then on.
    fn get_highest_indexed_seq(&self, tantivy_index: &Index) -> Result<u64, StoreError> {
        let reader: IndexReader = tantivy_index
            .reader_builder()
            .reload_policy(tantivy::ReloadPolicy::Manual)
            .try_into()?;
        let searcher = reader.searcher();
        let schema = searcher.index().schema();
        let doc_count = searcher.num_docs();

        // Get the _seq field from schema
        let seq_field = schema
            .get_field("_seq")
            .map_err(|_| StoreError::FieldNotFound("_seq field missing".to_string()))?;

        // Get one document sorted by _seq descending to find the highest value
        let top_collector = TopDocs::with_limit(1).order_by_u64_field("_seq", Order::Desc);
        let top_docs = searcher.search(&AllQuery, &top_collector)?;

        tracing::debug!(
            doc_count = doc_count,
            top_docs_len = top_docs.len(),
            "get_highest_indexed_seq: Tantivy search completed"
        );

        // CRITICAL: The sort key returned by order_by_u64_field is u64::MAX - actual_value
        // We must retrieve the actual _seq value from the document's stored fields
        if let Some((inverted_sort_key, doc_address)) = top_docs.first() {
            let doc: tantivy::TantivyDocument = searcher.doc(*doc_address)?;
            if let Some(value) = doc.get_first(seq_field)
                && let Some(seq) = value.as_u64()
            {
                tracing::debug!(
                    inverted_sort_key = ?inverted_sort_key,
                    actual_seq = seq,
                    doc_address = ?doc_address,
                    "get_highest_indexed_seq: Retrieved actual _seq from stored fields"
                );
                return Ok(seq);
            } else {
                tracing::error!(
                    inverted_sort_key = ?inverted_sort_key,
                    doc_address = ?doc_address,
                    "_seq field not found in stored document - this should never happen! Returning inverted_sort_key which is WRONG"
                );
                // CRITICAL BUG: We should NOT return the inverted sort key here
                // Return u64::MAX - inverted_sort_key to get the actual value
                return Ok(u64::MAX - inverted_sort_key.unwrap_or(0));
            }
        } else {
            tracing::debug!("get_highest_indexed_seq: No documents found in index");
        }

        // No documents found, return 0
        Ok(0)
    }

    /// The WAL sequence Tantivy has durably indexed for `index`.
    ///
    /// Three sources, in descending order of trust:
    ///
    /// 1. The Tantivy commit payload, written inside the commit, so it cannot describe a
    ///    segment set that is not on disk.
    /// 2. `_recovery_meta`, written in the transaction that truncates the WAL right after a
    ///    successful commit. It can lag the payload by one commit but never lead it, because
    ///    nothing writes it until the commit it describes has returned.
    /// 3. The `_seq` fast field, scanned. Correct but expensive, available only on an index
    ///    old enough to carry the field, and needed only when the two above have no answer.
    ///
    /// Taking the maximum is safe precisely because none of the three can report a sequence
    /// Tantivy does not have, and it keeps a build transition from replaying a tail twice.
    fn checkpoint_seq(&self, index: &str, tantivy_index: &Index) -> Result<u64, StoreError> {
        let stamped = tantivy_checkpoint_seq(tantivy_index);
        let persisted = self.get_persisted_committed_seq(index)?;

        if let Some(seq) = stamped.into_iter().chain(persisted).max() {
            tracing::debug!(
                index = %index,
                stamped = ?stamped,
                persisted = ?persisted,
                checkpoint_seq = seq,
                "Resolved Tantivy checkpoint without scanning the index"
            );
            return Ok(seq);
        }

        // No `_seq` column, so there is nothing to scan and nothing to find. An index built
        // without the field is one built after commits started carrying a payload, so the only
        // way to reach here is an index that has never committed — which has nothing indexed,
        // and whose checkpoint is therefore 0.
        if tantivy_index.schema().get_field("_seq").is_err() {
            tracing::debug!(
                index = %index,
                "No checkpoint recorded and no _seq column; index has never been committed"
            );
            return Ok(0);
        }

        // Neither cheap source exists. Either this index was last committed by a build
        // predating both, or it has never been committed at all — a freshly created index
        // looks identical here, and the scan is what distinguishes them.
        let scan_start = Instant::now();
        let scanned = self.get_highest_indexed_seq(tantivy_index)?;

        if scanned == 0 {
            // Nothing indexed, so there is no checkpoint to remember and the scan was free.
            // This is the ordinary path for an index that was just created.
            tracing::debug!(index = %index, "Index has no indexed documents; checkpoint is 0");
            return Ok(0);
        }

        // Record what the scan found, so it is a one-time cost for this index rather than
        // something every open repeats: the stamped payload only appears on the *next*
        // commit, and an index that is read but never written again would otherwise pay the
        // scan forever.
        tracing::warn!(
            index = %index,
            checkpoint_seq = scanned,
            elapsed_ms = scan_start.elapsed().as_millis(),
            "No commit payload and no recovery metadata; scanned the _seq field to locate the \
             checkpoint and backfilled it. This index will not scan again."
        );

        if let Err(e) = self.persist_committed_seq(index, scanned) {
            tracing::warn!(
                index = %index,
                error = %e,
                "Could not backfill the recovery checkpoint; the next open will scan again"
            );
        }

        Ok(scanned)
    }

    /// Replay into Tantivy the writes redb has committed but Tantivy has not.
    ///
    /// redb is the ACID source of truth and the Tantivy index is derived from it, so the only
    /// state that can be stale after an unclean stop is Tantivy's, and only by the writes redb
    /// made durable after Tantivy's last commit. That difference is exactly the `wal_<index>`
    /// entries above [`Self::checkpoint_seq`], and it is all this touches — both engines have
    /// already finished their own recovery by the time it runs, and no part of the corpus is
    /// read to work out where to start.
    ///
    /// The tail is small by construction: [`Self::checkpoint_committed`] deletes every WAL
    /// entry a commit covers, so what remains is one commit interval's worth of writes at
    /// most, whatever the size of the index underneath.
    ///
    /// Idempotent. Every replayed put deletes its `id` term before adding the document, so
    /// replaying a range Tantivy already has changes nothing, which is what lets recovery
    /// resume from a checkpoint that is behind rather than needing one that is exact.
    ///
    /// Returns `(replayed_count, max_wal_seq, checkpoint_seq)` so the caller can seed the
    /// sequence counter without repeating the lookups.
    fn recover_index(
        &self,
        index: &str,
        writer: &mut IndexWriter,
        tantivy_index: &Index,
    ) -> Result<(usize, u64, u64), StoreError> {
        let max_wal_seq = self.get_max_wal_id_for_index(index)?;

        if max_wal_seq == 0 {
            // Nothing is waiting. Read the checkpoint anyway, and fail the open if it cannot
            // be read: it is the durable high-water mark of the sequence counter, and
            // defaulting it to zero would hand out sequence numbers this index has already
            // used — which a later crash reads as a tail already covered and never replays.
            let checkpoint = self.checkpoint_seq(index, tantivy_index)?;
            tracing::debug!(
                index = %index,
                checkpoint_seq = checkpoint,
                "WAL tail is empty; index is in sync with redb"
            );
            return Ok((0, 0, checkpoint));
        }

        let last_committed_seq = self.checkpoint_seq(index, tantivy_index)?;

        // If all sequences are committed, nothing to recover
        if last_committed_seq >= max_wal_seq {
            tracing::info!(
                index = %index,
                checkpoint_seq = last_committed_seq,
                max_wal_seq = max_wal_seq,
                "WAL tail is already indexed; truncating it and skipping replay"
            );

            // Finish the truncation the commit that indexed these entries did not get to.
            // A crash between the Tantivy commit and `checkpoint_committed` leaves entries
            // behind that the checkpoint already covers, and without this they stay: the
            // partition in `recover_indices` reads a non-empty WAL as "needs recovery", so
            // the index would open a writer to discover there is nothing to do on *every*
            // subsequent boot, and would only stop once it happened to take another write.
            // The checkpoint proves these entries are in Tantivy, which is exactly the
            // precondition `checkpoint_committed` asks of its caller.
            if let Err(e) = self.checkpoint_committed(index, last_committed_seq) {
                tracing::warn!(
                    index = %index,
                    error = %e,
                    "Could not truncate the already-indexed WAL tail; recovery will look at it again next boot"
                );
            }

            return Ok((0, max_wal_seq, last_committed_seq));
        }

        // Start recovery from the first missing sequence
        let range_start = last_committed_seq + 1;

        tracing::info!(
            index = %index,
            range_start = range_start,
            max_wal_seq = max_wal_seq,
            pending = max_wal_seq - last_committed_seq,
            "Replaying the WAL tail redb committed after Tantivy's last commit"
        );

        // Start a read transaction on Redb
        let read_txn = self.kv.begin_read()?;
        let wal_table_name = format!("wal_{}", index);
        let wal_table_def = TableDefinition::<u64, &[u8]>::new(&wal_table_name);

        let wal_table = match read_txn.open_table(wal_table_def) {
            Ok(table) => table,
            Err(_) => {
                tracing::debug!(index = %index, "No WAL table found");
                return Ok((0, max_wal_seq, last_committed_seq));
            }
        };

        // The document bodies, read from the same snapshot as the WAL so the two cannot
        // disagree about what was committed.
        let data_table_name = format!("data_{}", index);
        let data_table_def = TableDefinition::<&str, &[u8]>::new(&data_table_name);
        let data_table = read_txn.open_table(data_table_def).ok();

        // Get schema for building documents
        let index_schema = self
            .get_schema_cached(index)?
            .unwrap_or_else(|| Arc::new(IndexSchema::default()));

        let schema = tantivy_index.schema();
        let id_field = schema
            .get_field("id")
            .map_err(|_| StoreError::FieldNotFound("id".to_string()))?;
        // Absent on any index built without it. See `SchemaFields::seq`.
        let seq_field = schema.get_field("_seq").ok();

        // Build indexed fields map
        let mut indexed_fields = HashMap::new();
        for (field, field_entry) in schema.fields() {
            let name = field_entry.name();
            if name != "id" && name != "_seq" {
                indexed_fields.insert(name.to_string(), field);
            }
        }

        // Replay commits are checkpoints, not throughput throttles: size them well above the
        // steady-state write threshold so a large WAL produces a handful of big segments
        // rather than one small segment per batch.
        let recovery_commit_threshold = self.recovery_commit_threshold();

        // Zero-copy WAL replay: iterate range directly, process each entry in-place.
        // AccessGuard::value() returns &[u8] pointing directly into redb's mmap'd pages,
        // avoiding the need to allocate a Vec<u8> for every entry.
        //
        // Each id is applied once. A tail that touched the same document repeatedly — the
        // shape a bursty updater produces, and the one most likely to be long — collapses to
        // one Tantivy operation per distinct id, because every entry for that id resolves to
        // the same committed row. Applying at the first occurrence rather than the last is
        // what keeps the mid-replay checkpoints meaningful: everything below the stamped
        // sequence really has been applied.
        let mut replayed_count = 0;
        let mut replayed_since_commit = 0u64;
        let mut skipped_duplicates = 0usize;
        let mut applied_ids: HashSet<String> = HashSet::new();

        for result in wal_table.range(range_start..=max_wal_seq)? {
            let (seq_guard, wal_data_guard) = result?;
            let seq_id = seq_guard.value();
            let id = decode_wal_entry(wal_data_guard.value())?;

            if !applied_ids.insert(id.clone()) {
                skipped_duplicates += 1;
                replayed_count += 1;
                replayed_since_commit += 1;
                continue;
            }

            // The committed state of this id decides the operation. A row means the document
            // stands as written; no row means it was deleted. Nothing else can be true — a put
            // always writes the row and a delete always removes it, in the same transaction
            // that appended the WAL entry being read here.
            let stored = match data_table.as_ref() {
                Some(table) => table.get(id.as_str())?,
                None => None,
            };

            match stored {
                Some(doc_guard) => {
                    let stored_doc: StoredDocOwned = serde_json::from_slice(doc_guard.value())
                        .map_err(|e| StoreError::Serialization(e.to_string()))?;

                    let mut tantivy_doc = tantivy::TantivyDocument::default();
                    tantivy_doc.add_text(id_field, &id);
                    if let Some(seq_field) = seq_field {
                        tantivy_doc.add_u64(seq_field, seq_id);
                    }

                    if let Some(json_obj) =
                        stored_doc.json_blob.as_ref().and_then(|v| v.as_object())
                    {
                        for (field_name, field_def) in &index_schema.fields {
                            if !field_def.indexed || field_def.is_shadow || field_name == "id" {
                                continue;
                            }

                            if let Some(tantivy_field) = indexed_fields.get(field_name)
                                && let Some(field_value) = json_obj.get(field_name)
                            {
                                add_json_value_to_doc(
                                    &mut tantivy_doc,
                                    *tantivy_field,
                                    field_name,
                                    &field_def.field_type,
                                    field_value,
                                    BadFacet::SkipAndWarn,
                                )?;
                            }
                        }
                    }

                    let term = tantivy::Term::from_field_text(id_field, &id);
                    writer.delete_term(term);
                    writer.add_document(tantivy_doc)?;
                }
                None => {
                    let term = tantivy::Term::from_field_text(id_field, &id);
                    writer.delete_term(term);
                }
            }

            replayed_count += 1;
            replayed_since_commit += 1;

            // Periodic commit during replay, on a much coarser threshold than steady-state
            // writes: each commit seals a segment and fsyncs, so replaying a large WAL at the
            // normal batch threshold would produce hundreds of tiny segments and the merge
            // storm that follows. Stamping the sequence into each one is what makes them
            // worth taking — a crash mid-recovery resumes from the last stamp instead of
            // replaying the tail from the start. Nothing is written to redb here; the WAL is
            // being iterated inside an open read transaction, and the stamp already records
            // the progress that a redb write would have.
            if replayed_since_commit >= recovery_commit_threshold {
                tracing::info!(
                    index = %index,
                    replayed = replayed_count,
                    replayed_since_commit = replayed_since_commit,
                    checkpoint_seq = seq_id,
                    "Recovery: threshold commit during WAL replay"
                );
                commit_writer_at(writer, seq_id)?;
                replayed_since_commit = 0;
            }

            // Log progress every 1000 documents
            if replayed_count % 1000 == 0 {
                tracing::info!(
                    index = %index,
                    replayed = replayed_count,
                    range_start = range_start,
                    max_wal_seq = max_wal_seq,
                    "Recovery progress"
                );
            }
        }

        // Documents replayed since the last commit are in the writer's buffer but not yet
        // durable. Seed the operations counter with them so the normal commit path (threshold
        // or supervisor idle timeout) flushes them; otherwise the no-op guard in commit_index
        // would leave them buffered until unrelated traffic arrives for this index.
        if replayed_since_commit > 0 {
            self.operations_counter
                .entry(index.to_string())
                .or_insert_with(|| AtomicU64::new(0))
                .value()
                .fetch_add(replayed_since_commit, Ordering::SeqCst);
        }

        tracing::info!(
            index = %index,
            replayed_count = replayed_count,
            distinct_documents = applied_ids.len(),
            skipped_duplicates = skipped_duplicates,
            uncommitted = replayed_since_commit,
            "WAL recovery completed - replayed missing operations"
        );

        Ok((replayed_count, max_wal_seq, last_committed_seq))
    }

    /// Get a value from the read cache if present.
    fn get_from_cache(&self, index: &str, key: &str) -> Option<Vec<u8>> {
        self.read_cache.get(index)?.entries.get(key).cloned()
    }

    /// The generation a reader must quote back to `insert_into_cache`.
    ///
    /// Read *before* the redb transaction the body will come out of, so that a write landing in
    /// between is detectable. An index with no cache yet reads as 0, which the first write to it
    /// bumps like any other — see [`IndexReadCache`].
    fn cache_generation(&self, index: &str) -> u64 {
        self.read_cache
            .get(index)
            .map(|cache| cache.generation)
            .unwrap_or(0)
    }

    /// Insert a value into the read cache with a simple per-index size bound.
    ///
    /// `seen_generation` is what [`cache_generation`](Self::cache_generation) returned before the
    /// read that produced `value`. A mismatch means a write committed in between, so `value` is a
    /// pre-write body and caching it would reinstate exactly the staleness the write removed.
    /// Dropping it costs one cache miss; keeping it costs a wrong answer until the FIFO evicts it.
    fn insert_into_cache(&self, index: &str, key: &str, value: Vec<u8>, seen_generation: u64) {
        const MAX_CACHE_ENTRIES_PER_INDEX: usize = 1024;

        let mut index_cache = self.read_cache.entry(index.to_string()).or_default();

        if index_cache.generation != seen_generation {
            return;
        }

        if index_cache.entries.len() >= MAX_CACHE_ENTRIES_PER_INDEX
            && let Some(first_key) = index_cache.entries.keys().next().cloned()
        {
            index_cache.entries.remove(&first_key);
        }

        index_cache.entries.insert(key.to_string(), value);
    }

    /// Drop the cached bodies for ids a write has just changed, and bump the generation.
    ///
    /// **Call after the redb transaction commits, never before.** Invalidating first leaves a
    /// window in which a reader still sees the pre-write row and can cache it again; the
    /// generation bump is what makes such a reader decline to.
    ///
    /// The generation is bumped even when nothing was cached, because a reader that found no
    /// cache read generation 0 and would otherwise be free to install a body this write has
    /// already superseded.
    fn invalidate_read_cache<'a, I>(&self, index: &str, ids: I)
    where
        I: IntoIterator<Item = &'a str>,
    {
        let mut index_cache = self.read_cache.entry(index.to_string()).or_default();
        index_cache.generation = index_cache.generation.wrapping_add(1);
        for id in ids {
            index_cache.entries.remove(id);
        }
    }

    /// [`invalidate_read_cache`](Self::invalidate_read_cache) for a change that touched every id.
    fn invalidate_read_cache_all(&self, index: &str) {
        let mut index_cache = self.read_cache.entry(index.to_string()).or_default();
        index_cache.generation = index_cache.generation.wrapping_add(1);
        index_cache.entries.clear();
    }

    /// Build Tantivy schema and field map from index schema definition using native Tantivy types.
    fn create_schema_from_definition(index_schema: &IndexSchema) -> (Schema, SchemaFields) {
        use tantivy::schema::{IndexRecordOption, TextFieldIndexing, TextOptions};

        let mut schema_builder = Schema::builder();

        // ID field is always present - untokenized string for exact matching
        let id_field = schema_builder.add_text_field("id", STRING | STORED);

        // No `_seq` field. It cost 8 stored bytes plus a fast column per document, and its
        // only reader was the checkpoint scan that `order_by_u64_field` needs — which the
        // commit payload replaced. Indices that already have the column keep it and keep
        // being written to; nothing new grows one.

        let mut indexed_fields = HashMap::new();

        for (name, field_def) in &index_schema.fields {
            // Skip reserved fields (id, _seq), non-indexed fields, and shadow fields
            if name == "id" || name == "_seq" || !field_def.indexed || field_def.is_shadow {
                continue;
            }

            let field = match field_def.field_type {
                TantivyFieldType::Text => {
                    let mut options = TextOptions::default().set_indexing_options(
                        TextFieldIndexing::default()
                            .set_tokenizer(field_def.tokenizer.as_deref().unwrap_or("default"))
                            .set_index_option(match field_def.index_record_option.as_deref() {
                                Some("Basic") => IndexRecordOption::Basic,
                                Some("WithFreqs") => IndexRecordOption::WithFreqs,
                                _ => IndexRecordOption::WithFreqsAndPositions,
                            }),
                    );
                    if field_def.stored {
                        options = options.set_stored();
                    }
                    // `fast` on a text field builds the string fast column that a sort needs.
                    // `None` rather than a tokenizer name on purpose: the column then holds the
                    // whole untokenized value, which is what an alphabetical sort orders on. A
                    // tokenized fast column would sort by whichever token came first.
                    //
                    // Without this the field has no column, and `search_documents` has to pick
                    // sort candidates by relevance and reorder them afterwards — an ordering that
                    // is only approximately right and cannot be paged through. See the sort
                    // branch there.
                    if field_def.is_fast() {
                        options = options.set_fast(None);
                    }
                    schema_builder.add_text_field(name, options)
                }
                TantivyFieldType::String => {
                    if field_def.is_fast() {
                        schema_builder.add_text_field(name, STRING | FAST)
                    } else {
                        schema_builder.add_text_field(name, STRING)
                    }
                }
                TantivyFieldType::I64 => {
                    if field_def.is_fast() {
                        schema_builder.add_i64_field(name, INDEXED | FAST)
                    } else {
                        schema_builder.add_i64_field(name, INDEXED)
                    }
                }
                TantivyFieldType::U64 => {
                    if field_def.is_fast() {
                        schema_builder.add_u64_field(name, INDEXED | FAST)
                    } else {
                        schema_builder.add_u64_field(name, INDEXED)
                    }
                }
                TantivyFieldType::F64 => {
                    if field_def.is_fast() {
                        schema_builder.add_f64_field(name, INDEXED | FAST)
                    } else {
                        schema_builder.add_f64_field(name, INDEXED)
                    }
                }
                TantivyFieldType::Date => {
                    if field_def.is_fast() {
                        schema_builder.add_date_field(name, INDEXED | FAST)
                    } else {
                        schema_builder.add_date_field(name, INDEXED)
                    }
                }
                TantivyFieldType::Boolean => schema_builder.add_bool_field(name, INDEXED),
                TantivyFieldType::Bytes => schema_builder.add_bytes_field(name, INDEXED),
                TantivyFieldType::Ip => schema_builder.add_ip_addr_field(name, INDEXED),
                TantivyFieldType::Json => schema_builder.add_json_field(name, TEXT),
                TantivyFieldType::Facet => schema_builder.add_facet_field(name, INDEXED),
            };

            indexed_fields.insert(name.clone(), field);
        }

        let schema = schema_builder.build();
        let fields = SchemaFields {
            id: id_field,
            seq: None,
            indexed_fields,
        };

        (schema, fields)
    }

    /// Derive Tantivy field mapping from an existing index schema on disk.
    fn load_fields_from_existing_index(tantivy_index: &Index) -> Result<SchemaFields, StoreError> {
        let schema = tantivy_index.schema();

        let id = schema
            .get_field("id")
            .map_err(|_| StoreError::FieldNotFound("id".to_string()))?;

        // Absent on any index built without it; that is not an error, it is the new normal.
        // This runs on every open of an existing index, so treating it as required here is
        // what would make an index built either way unopenable by the other.
        let seq = schema.get_field("_seq").ok();

        let mut indexed_fields = HashMap::new();
        for (field, field_entry) in schema.fields() {
            let name = field_entry.name();
            if name == "id" || name == "_seq" {
                continue;
            }
            indexed_fields.insert(name.to_string(), field);
        }

        Ok(SchemaFields {
            id,
            seq,
            indexed_fields,
        })
    }

    /// Derive IndexSchema from a Tantivy index's schema.
    /// This reads back the actual persisted schema from Tantivy and converts it
    /// to our IndexSchema format, ensuring we're in sync with what Tantivy has.
    /// NOTE: Excludes the mandatory 'id' field since it's implicit in Tantivy
    fn derive_index_schema_from_tantivy(tantivy_index: &Index) -> IndexSchema {
        use tantivy::schema::FieldType;

        let schema = tantivy_index.schema();
        let mut fields = HashMap::new();

        for (_field, field_entry) in schema.fields() {
            let name = field_entry.name();
            if name == "id" {
                continue; // Skip the mandatory id field - it's implicit in Tantivy
            }
            if name == "_seq" {
                // Present only on an index built before the field was retired. Carrying it
                // into a derived schema would put it back into a document nobody declared it
                // in, and every listing filters it out again downstream anyway.
                continue;
            }

            let field_type = match field_entry.field_type() {
                FieldType::Str(_) => {
                    // Check if it's indexed with STRING flag (untokenized) or default TEXT
                    // For simplicity, we'll check if it's stored but not indexed as a heuristic
                    let is_indexed = field_entry.is_indexed();
                    let is_stored = field_entry.is_stored();
                    if is_stored && !is_indexed {
                        TantivyFieldType::String
                    } else {
                        TantivyFieldType::Text
                    }
                }
                FieldType::U64(_) => TantivyFieldType::U64,
                FieldType::I64(_) => TantivyFieldType::I64,
                FieldType::F64(_) => TantivyFieldType::F64,
                FieldType::Bool(_) => TantivyFieldType::Boolean,
                FieldType::Date(_) => TantivyFieldType::Date,
                FieldType::Bytes(_) => TantivyFieldType::Bytes,
                FieldType::JsonObject(_) => TantivyFieldType::Json,
                FieldType::IpAddr(_) => TantivyFieldType::Ip,
                FieldType::Facet(_) => TantivyFieldType::Facet,
            };

            // Determine field options from Tantivy's field entry
            let indexed = field_entry.is_indexed();
            let stored = field_entry.is_stored();
            let fast = field_entry.is_fast();

            // Capture additional options for Text fields
            let (tokenizer, index_record_option) = if let FieldType::Str(text_options) =
                field_entry.field_type()
            {
                // Extract the actual tokenizer and index options from Tantivy
                let tokenizer_name = match text_options.get_indexing_options() {
                    Some(opts) => {
                        let token_name = opts.tokenizer().to_string();
                        tracing::trace!(field_name = %name, tokenizer = %token_name, "Extracted tokenizer from Tantivy");
                        Some(token_name)
                    }
                    None => {
                        tracing::trace!(field_name = %name, "No indexing options found, using default tokenizer");
                        Some("default".to_string())
                    }
                };

                let index_option = match text_options.get_indexing_options() {
                    Some(opts) => {
                        let opt_str = match opts.index_option() {
                            tantivy::schema::IndexRecordOption::Basic => "Basic".to_string(),
                            tantivy::schema::IndexRecordOption::WithFreqs => {
                                "WithFreqs".to_string()
                            }
                            tantivy::schema::IndexRecordOption::WithFreqsAndPositions => {
                                "WithFreqsAndPositions".to_string()
                            }
                        };
                        tracing::trace!(field_name = %name, index_option = %opt_str, "Extracted index option from Tantivy");
                        Some(opt_str)
                    }
                    None => {
                        tracing::trace!(field_name = %name, "No indexing options found, using default index option");
                        Some("WithFreqsAndPositions".to_string())
                    }
                };
                (tokenizer_name, index_option)
            } else {
                tracing::trace!(field_name = %name, field_type = ?field_entry.field_type(), "Non-text field, no tokenizer options");
                (None, None)
            };

            fields.insert(
                name.to_string(),
                FieldDef {
                    name: name.to_string(),
                    field_type,
                    indexed,
                    stored,
                    // Derived from the built index, so this is what the index actually has
                    // rather than what a schema asked for: concrete, never unresolved.
                    fast: Some(fast),
                    is_shadow: false, // Fields derived from Tantivy schema are not shadow fields
                    // Tantivy stores no description; one lives only in the schema record.
                    description: None,
                    tokenizer,
                    index_record_option,
                },
            );
        }

        let text_count = fields
            .values()
            .filter(|f| matches!(f.field_type, TantivyFieldType::Text))
            .count();
        let fast_count = fields.values().filter(|f| f.is_fast()).count();
        tracing::debug!(
            total_fields = fields.len(),
            text_fields = text_count,
            fast_fields = fast_count,
            "Derived index schema from Tantivy"
        );

        let now = chrono::Utc::now().timestamp();
        IndexSchema {
            fields,
            version: 1,
            created_at: now,
            updated_at: now,
            description: None,
            routing_field_name: "id".to_string(),
            shadow_fields: HashSet::new(),
        }
    }

    /// On-disk Tantivy directory for `index`, validated to stay inside this
    /// shard's `indices/` directory.
    ///
    /// Every path derived from an index name goes through here rather than joining
    /// directly, so that a name containing `..` or a path separator can never escape the
    /// shard. That includes names read back from redb or the filesystem, which are
    /// already trusted — one construction site is what keeps the guarantee checkable.
    pub fn index_dir(&self, index: &str) -> Result<PathBuf, StoreError> {
        resolve_index_dir(&self.config.shard_path.join("indices"), index)
    }

    /// Helper method: get_or_create_index
    /// Made public to allow pre-creating indexes when schema is created
    pub fn get_or_create_index(
        &self,
        index: &str,
    ) -> Result<(Arc<Mutex<IndexWriter>>, SchemaFields), StoreError> {
        // Fast path: Check writers cache first
        if let Some(writer) = self.writers.get(index)
            && let Some(fields) = self.fields_cache.get(index)
        {
            return Ok((Arc::clone(writer.value()), fields.value().clone()));
        }

        // Slow path: serialize initialization per index. `get_or_create_index` is reachable
        // concurrently from the shard writer thread, the read pool (stats) and the startup
        // warmup threads. Without this guard two of them can both miss the fast path and
        // both call `writer_with_options`, where tantivy's non-blocking `.tantivy-writer.lock`
        // makes the loser fail with `LockError::LockBusy`.
        let init_lock = {
            let entry = self
                .index_init_locks
                .entry(index.to_string())
                .or_insert_with(|| Arc::new(Mutex::new(())));
            Arc::clone(entry.value())
        };
        let _init_guard = init_lock.lock().unwrap_or_else(|poisoned| {
            tracing::error!(index = %index, "Index init mutex was poisoned, recovering");
            poisoned.into_inner()
        });

        // Re-check under the init lock: another thread may have finished initialization
        // while we were waiting for it.
        if let Some(writer) = self.writers.get(index)
            && let Some(fields) = self.fields_cache.get(index)
        {
            return Ok((Arc::clone(writer.value()), fields.value().clone()));
        }

        // A writer is cached but its field handles are not. Persisting a schema evicts the
        // field cache without touching the writer — `store_schema_and_cache` and
        // `invalidate_schema_cache` both do — so a live index can arrive here with half of the
        // pair the fast path needs. Rebuilding the handles from the writer's own index is the
        // only correct response: the slow path below would open a *second* `IndexWriter`
        // against the lockfile this one still holds and fail with `LockBusy`, which is what
        // made every schema write against an open index a 500.
        //
        // Deriving them from the cached writer is also what keeps the two in step by
        // construction — the handles resolve against exactly the index the writer is writing
        // to, rather than against a schema read from somewhere else.
        if let Some(writer) = self.writers.get(index) {
            let writer_arc = Arc::clone(writer.value());
            // Release the map's shard guard before blocking on the writer mutex.
            drop(writer);

            let fields = {
                let guard = writer_arc.lock().unwrap_or_else(|poisoned| {
                    tracing::error!(index = %index, "Writer mutex was poisoned, recovering");
                    poisoned.into_inner()
                });
                Self::load_fields_from_existing_index(guard.index())?
            };

            self.fields_cache.insert(index.to_string(), fields.clone());
            tracing::debug!(
                index = %index,
                "Rebuilt field handles for a live index from its cached writer"
            );
            return Ok((writer_arc, fields));
        }

        // Create index directory and Tantivy index if it doesn't exist.
        // `index_dir` rejects any name that is not a single path component, so a
        // traversal attempt cannot reach the `create_dir_all` below.
        let index_path = self.index_dir(index)?;
        let init_start = Instant::now();

        // Determine schema for this index
        let index_schema = self
            .get_schema_cached(index)?
            .unwrap_or_else(|| Arc::new(IndexSchema::default()));

        let (schema, _) = Self::create_schema_from_definition(&index_schema);

        // Create or open tantivy index, and get the correct field handles
        let open_start = Instant::now();
        let (tantivy_index, fields, sync_schema) = if index_path.join("meta.json").exists() {
            // Opening existing index: must use Field handles from the opened index's schema
            let opened_index = open_tantivy_index(&index_path)?;
            let fields = Self::load_fields_from_existing_index(&opened_index)?;
            (opened_index, fields, false)
        } else {
            // Creating new index: use the schema and fields we just built
            fs::create_dir_all(&index_path)?;
            let new_index = create_tantivy_index(&index_path, schema)?;

            // After creating the index, read back the actual Tantivy schema and sync it.
            // This ensures our cached schema matches exactly what Tantivy persisted.
            let fields = Self::load_fields_from_existing_index(&new_index)?;
            (new_index, fields, true)
        };

        // IMPORTANT: Only sync schema when we actually created a new index
        // This ensures we don't overwrite persisted schema when index was deleted
        if sync_schema {
            // Use the original schema as the source of truth
            // The index_schema contains the complete field definitions including indexed=false fields
            let mut tantivy_schema = (*index_schema).clone();

            // Ensure 'id' field exists with correct Tantivy-derived attributes
            // This handles cases where the original schema didn't specify 'id' field
            tantivy_schema
                .fields
                .entry("id".to_string())
                .or_insert_with(|| {
                    FieldDef {
                        name: "id".to_string(),
                        field_type: TantivyFieldType::Text,
                        indexed: true,
                        stored: true,
                        fast: Some(false),
                        is_shadow: false, // The canonical 'id' field is not a shadow field
                        description: None,
                        tokenizer: Some("raw".to_string()),
                        index_record_option: Some("Basic".to_string()),
                    }
                });

            // IMPORTANT: Cache should reflect merged schema (Tantivy + stored metadata)
            self.schema_cache
                .insert(index.to_string(), Arc::new(tantivy_schema.clone()));

            // Persist the merged schema to redb for future reference
            self.store_schema(index, &tantivy_schema)?;

            // CRITICAL: Clear reader cache to ensure search sees latest commits
            // This prevents searches from using stale readers that don't see newly written documents
            self.readers.remove(index);

            tracing::debug!(index = %index, "Schema synced: Tantivy schema merged with stored metadata, cached and persisted");
        }

        let open_elapsed = open_start.elapsed();

        // Create writer with dynamic memory budget based on index size and field count
        let writer_start = Instant::now();
        let field_count = Some(fields.indexed_fields.len());
        let optimal_budget = self
            .config
            .get_optimal_memory_budget(&index_path, field_count);

        // Cache the budget
        self.budget_cache.insert(index.to_string(), optimal_budget);

        let num_worker_threads = self.config.indexer_num_threads.max(1);
        let num_merge_threads = self.config.merge_num_threads.max(1);
        let memory_per_thread = optimal_budget / num_worker_threads;

        let writer_options = tantivy::indexer::IndexWriterOptions::builder()
            .num_worker_threads(num_worker_threads)
            .memory_budget_per_thread(memory_per_thread)
            .num_merge_threads(num_merge_threads)
            .build();
        let mut writer = tantivy_index.writer_with_options(writer_options)?;

        tracing::info!(
            index = %index,
            worker_threads = num_worker_threads,
            merge_threads = num_merge_threads,
            budget_mb = optimal_budget / (1024 * 1024),
            "IndexWriter created with explicit thread configuration"
        );

        let writer_elapsed = writer_start.elapsed();

        // Bring Tantivy up to what redb has already made durable. Normally this reads two
        // numbers and stops. No reader is opened for it: the checkpoint comes from the commit
        // payload in `meta.json`, so nothing here has to touch a searcher, and on a large
        // index building one is real work that the query path would only have to redo.
        let recovery_start = Instant::now();
        let (replayed_count, max_wal_seq, last_committed_seq) =
            self.recover_index(index, &mut writer, &tantivy_index)?;

        if replayed_count > 0 {
            tracing::info!(
                index = %index,
                count = replayed_count,
                "Recovered {} operations from WAL for index {}",
                replayed_count,
                index
            );

            // CRITICAL FIX: Do NOT commit immediately after recovery.
            // The blocking commit() call can take a very long time (segment merging, fsync, etc.)
            // which blocks the writer thread and causes HTTP requests to timeout.
            // Instead, rely on the normal commit flow:
            //   1. Operations are already in Tantivy's in-memory buffer
            //   2. The next normal commit (via maybe_commit_writer) will persist them
            //   3. If the process crashes before that commit, WAL recovery will replay again
            // This is safe because:
            //   - WAL entries are still present until after the next commit
            //   - The sequence counter is set to max(max_wal_seq, last_committed_seq)
            //   - Recovery is idempotent - replaying again is safe
            tracing::info!(
                index = %index,
                "Recovery complete - {} operations in Tantivy buffer, will persist on next commit",
                replayed_count
            );

            // Replay may have made threshold commits, and a query arriving during recovery
            // can already have cached a reader for this directory — the shard answers
            // searches throughout startup. Readers reload only when told to, and the next
            // `commit_index` may be far off on an index that stops taking writes here, so
            // tell them now rather than serving the pre-recovery segment set until then.
            if let Err(e) = self.smart_refresh_reader(index) {
                tracing::warn!(
                    index = %index,
                    error = %e,
                    "Could not refresh reader after recovery; it will refresh on the next commit"
                );
            }
        }

        let recovery_elapsed = recovery_start.elapsed();
        let total_elapsed = init_start.elapsed();

        tracing::info!(
            index = %index,
            open_ms = open_elapsed.as_millis(),
            writer_ms = writer_elapsed.as_millis(),
            recovery_ms = recovery_elapsed.as_millis(),
            total_ms = total_elapsed.as_millis(),
            replayed = replayed_count,
            budget_mb = optimal_budget / (1024 * 1024),
            "Index initialization complete"
        );

        let writer_arc = Arc::new(Mutex::new(writer));

        // Store in cache
        self.writers
            .insert(index.to_string(), Arc::clone(&writer_arc));
        self.fields_cache.insert(index.to_string(), fields.clone());

        // Seed the sequence counter from what `recover_index` already read.
        //
        // Both terms matter. The WAL tail is empty on every clean restart, because the last
        // commit truncated it, so `max_wal_seq` alone would restart numbering from zero and
        // hand out sequences this index has already used — and the next crash would then find
        // a checkpoint far *above* the reissued tail and skip replaying it, dropping those
        // documents from the search index while redb still held them. The checkpoint is the
        // durable high-water mark that keeps numbering monotonic across restarts.
        self.current_seq
            .entry(index.to_string())
            .or_insert_with(|| {
                let max_seq = max_wal_seq.max(last_committed_seq);
                // Guard against u64::MAX which indicates corruption or uninitialized state.
                // u64::MAX is not a valid sequence number (would overflow on first write).
                let max_seq = if max_seq == u64::MAX {
                    tracing::warn!(
                        index = %index,
                        max_wal_seq = max_wal_seq,
                        last_committed_seq = last_committed_seq,
                        "Sequence counter detected u64::MAX (corrupted state), resetting to 0"
                    );
                    0
                } else {
                    max_seq
                };
                tracing::debug!(
                    index = %index,
                    max_wal_seq = max_wal_seq,
                    last_committed_seq = last_committed_seq,
                    initialized_seq = max_seq,
                    "Initialized sequence counter"
                );
                AtomicU64::new(max_seq)
            });

        Ok((writer_arc, fields))
    }

    /// ACID-compliant commit threshold with optimized batching to reduce transaction overhead.
    /// Larger batches mean fewer commits and less fsync overhead while maintaining Durability::Immediate.
    fn should_commit_writer(&self, index: &str, operations_since_commit: u64) -> bool {
        // Get dynamic memory budget for this specific index
        // Use cached budget if available to avoid syscalls on every write
        let budget = if let Some(b) = self.budget_cache.get(index) {
            *b.value()
        } else {
            // Fallback: calculate and cache. Measurement only, but the path is still
            // built by `index_dir` so no caller-supplied name is ever joined by hand.
            let b = match self.index_dir(index) {
                Ok(index_path) => self.config.get_optimal_memory_budget(&index_path, None),
                Err(_) => self.config.indexer_memory_budget,
            };
            self.budget_cache.insert(index.to_string(), b);
            b
        };

        // ACID-safe optimization: Scale commit frequency with memory budget
        // More memory = larger batches = fewer commits = less transaction overhead
        let min_budget = self.config.indexer_memory_min_mb * 1024 * 1024;
        let max_budget = self.config.indexer_memory_max_mb * 1024 * 1024;

        // Enhanced adaptive threshold for ACID-compliant commit optimization
        let budget_ratio = (budget - min_budget) as f64 / (max_budget - min_budget) as f64;
        let default_batch = self.config.default_batch_size as f64;

        // Optimized scaling: 1x default (min) to 20x default (max)
        // e.g., default=1000: 1000 ops (32MB) -> 20000 ops (512MB)
        // This reduces commit frequency while maintaining ACID via Durability::Immediate
        let base_ops = (default_batch * (1.0 + budget_ratio * 19.0)) as u64;

        // Additional optimization: larger thresholds for indices with high operation counts
        // This detects bulk operation patterns and adjusts accordingly
        let threshold = if operations_since_commit > default_batch as u64 * 5 {
            // For very large batches, allow up to 50% more accumulation
            // This reduces fsync overhead during bulk imports
            (base_ops as f64 * 1.5) as u64
        } else {
            base_ops
        };

        operations_since_commit >= threshold
    }

    /// Number of replayed WAL entries between commits during recovery.
    ///
    /// Recovery has different economics from steady-state writes. There is no client waiting
    /// on durability, so the only reasons to commit mid-replay are bounding the writer's
    /// in-memory buffer and checkpointing progress. Each commit costs an fsync and seals a
    /// segment, so committing at the steady-state threshold turns a large WAL into hundreds
    /// of tiny segments and a long merge tail. Scaled off the configured batch size, with a
    /// floor that keeps a small `default_batch_size` from checkpointing constantly.
    fn recovery_commit_threshold(&self) -> u64 {
        const RECOVERY_THRESHOLD_MULTIPLIER: u64 = 10;
        const MIN_RECOVERY_COMMIT_OPS: u64 = 25_000;

        let steady_state = self.config.default_batch_size.max(1) as u64;
        steady_state
            .saturating_mul(RECOVERY_THRESHOLD_MULTIPLIER)
            .max(MIN_RECOVERY_COMMIT_OPS)
    }

    /// Get operation count for an index since last commit
    pub fn get_operations_count(&self, index: &str) -> u64 {
        self.operations_counter
            .get(index)
            .map(|counter| counter.value().load(Ordering::SeqCst))
            .unwrap_or(0)
    }

    /// Increment operation count and return new count
    fn increment_operations(&self, index: &str) -> u64 {
        self.operations_counter
            .entry(index.to_string())
            .or_insert_with(|| AtomicU64::new(0))
            .value()
            .fetch_add(1, Ordering::SeqCst)
            + 1
    }

    /// Reset operation counter after commit
    pub fn reset_operations_counter(&self, index: &str) {
        if let Some(counter) = self.operations_counter.get(index) {
            counter.value().store(0, Ordering::SeqCst);
        }
    }

    /// Reset operation counter to a specific value (for intermediate commits)
    /// This allows the supervisor to continue working while resetting the counter
    pub fn reset_operations_counter_to(&self, index: &str, value: u64) {
        if let Some(counter) = self.operations_counter.get(index) {
            counter.value().store(value, Ordering::SeqCst);
        }
    }

    /// Smart refresh strategy for reader cache
    /// Tries fast reload first, falls back to remove + recreate if reload fails
    /// This preserves cache when possible while ensuring data freshness
    fn smart_refresh_reader(&self, index: &str) -> Result<(), StoreError> {
        // Fast path: Try to reload existing reader
        if let Some(reader_ref) = self.readers.get(index) {
            match reader_ref.value().reload() {
                Ok(_) => {
                    tracing::debug!(index = %index, "Reader reloaded successfully (fast path)");
                    return Ok(());
                }
                Err(e) => {
                    tracing::warn!(index = %index, error = %e, "Reader reload failed, falling back to recreation");
                }
            }
        }

        // Fallback: Remove and recreate (reliable path)
        self.readers.remove(index);
        tracing::debug!(index = %index, "Reader cache cleared, will recreate on next search (reliable path)");
        Ok(())
    }

    /// Force a commit for a specific index.
    /// Skips the commit if there are no pending operations (no-op guard).
    /// After commit, checkpoints the durable sequence and truncates the WAL entries
    /// that are now safely persisted in Tantivy.
    pub fn commit_index(&self, index: &str) -> Result<(), StoreError> {
        // No-op guard: skip commit if no operations pending since last commit.
        let ops_pending = self.get_operations_count(index);
        if ops_pending == 0 {
            tracing::debug!(index = %index, "commit_index: skipping, no pending operations");
            return Ok(());
        }

        let Some(writer_arc) = self.writers.get(index) else {
            // Pending operations with no writer: the buffered documents were dropped
            // together with the writer (admin eviction, forced removal). They are NOT in
            // Tantivy, so the WAL must be kept and the recovery checkpoint must not move —
            // the next open replays them.
            tracing::error!(
                index = %index,
                ops_pending = ops_pending,
                "commit_index: writer missing with pending operations; keeping WAL for replay"
            );
            return Err(StoreError::IndexNotFound(format!(
                "no writer for index {index} with {ops_pending} pending operations"
            )));
        };

        // Capture the sequence to checkpoint BEFORE committing. Anything allocated after
        // this point may not be included in the commit below, so claiming it durable would
        // truncate a WAL entry whose document never reached Tantivy.
        let committed_seq = self
            .current_seq
            .get(index)
            .map(|counter| counter.load(Ordering::SeqCst));

        // CRITICAL: Minimize lock hold time to prevent deadlocks
        // The writer lock must be dropped IMMEDIATELY after commit
        {
            let mut writer = writer_arc.value().lock().unwrap_or_else(|poisoned| {
                tracing::error!(index = %index, "Writer mutex was poisoned during commit, recovering");
                poisoned.into_inner()
            });
            // Stamp the sequence into the commit itself, so the checkpoint lands atomically
            // with the segments rather than in a second write that a crash can separate them
            // from. Writes that arrive between the capture above and this call may ride along
            // in the commit without being covered by the stamp; that direction is safe,
            // costing at most one redundant replay of an idempotent operation.
            match committed_seq {
                Some(seq) => commit_writer_at(&mut writer, seq)?,
                None => {
                    writer.commit()?;
                }
            }
            // Explicit drop to release lock before any other operations
            drop(writer);
        }
        drop(writer_arc);

        // All post-commit operations happen WITHOUT holding the writer lock
        self.reset_operations_counter(index);

        tracing::debug!(index = %index, ops_committed = ops_pending, "commit_index: committed");

        // CRITICAL: Smart refresh reader cache after commit to ensure search sees latest data
        self.smart_refresh_reader(index)?;

        // Refresh budget cache after commit since index size likely changed
        let index_path = self.index_dir(index)?;
        let new_budget = self.config.get_optimal_memory_budget(&index_path, None);
        self.budget_cache.insert(index.to_string(), new_budget);

        // AFTER the Tantivy commit succeeds: record the durable sequence and drop the WAL
        // entries it covers. Both happen in one redb transaction so a crash can never leave
        // the checkpoint ahead of the WAL.
        if let Some(seq) = committed_seq {
            self.checkpoint_committed(index, seq)?;
        }

        Ok(())
    }

    /// Record `committed_seq` as durable in Tantivy and drop the WAL entries it covers.
    ///
    /// Both writes share a single `Durability::Immediate` transaction: one fsync instead of
    /// two, and no window where the checkpoint has advanced but the WAL entries it claims
    /// are still present (or, worse, the reverse).
    ///
    /// The caller must have completed a successful `IndexWriter::commit()` covering every
    /// sequence up to and including `committed_seq`.
    fn checkpoint_committed(&self, index: &str, committed_seq: u64) -> Result<(), StoreError> {
        if committed_seq == 0 {
            return Ok(());
        }

        let wal_table_name = format!("wal_{}", index);
        let wal_table_def = TableDefinition::<u64, &[u8]>::new(&wal_table_name);

        let mut write_txn = self.kv.begin_write()?;
        {
            write_txn.set_durability(Durability::Immediate)?;

            let mut deleted_count = 0usize;
            {
                let mut wal_table = write_txn.open_table(wal_table_def)?;
                // retain_in deletes in-place over the range — no Vec of keys to materialize.
                wal_table.retain_in(0..=committed_seq, |_, _| {
                    deleted_count += 1;
                    false
                })?;
            }

            let mut meta_table = write_txn.open_table(TABLE_RECOVERY_META)?;
            meta_table.insert(index, committed_seq)?;

            if deleted_count > 0 {
                tracing::debug!(
                    index = %index,
                    deleted = deleted_count,
                    up_to_seq = committed_seq,
                    "Checkpointed committed sequence and truncated WAL"
                );
            }
        }
        write_txn.commit()?;

        Ok(())
    }

    /// Force commit writer for an index (for testing)
    pub fn commit_writer(&self, index: &str) -> Result<(), StoreError> {
        self.commit_index(index)
    }

    /// Perform smart commit based on operation count.
    /// Returns Ok(true) if a commit was performed, Ok(false) if threshold not yet reached.
    pub fn maybe_commit_writer(&self, index: &str) -> Result<bool, StoreError> {
        let ops_count = self.get_operations_count(index);

        if self.should_commit_writer(index, ops_count) {
            self.commit_index(index)?;
            return Ok(true);
        }
        Ok(false)
    }

    /// Apply a single write and commit if the cumulative operations threshold is met.
    /// Returns (seq_id, committed) where committed indicates if a Tantivy commit was performed.
    pub fn apply_write_and_maybe_commit(
        &self,
        index: &str,
        op: WalOp,
    ) -> Result<(u64, bool), StoreError> {
        let seq_id = self.apply_write(index, op)?;
        let committed = self.maybe_commit_writer(index)?;
        Ok((seq_id, committed))
    }

    /// Apply a batch of writes and commit if the cumulative operations threshold is met.
    /// Returns ((seq_ids, new_docs_count), committed) where committed indicates
    /// if a Tantivy commit was performed.
    pub fn apply_batch_and_maybe_commit(
        &self,
        index: &str,
        ops: Vec<WalOp>,
    ) -> Result<((Vec<u64>, usize), bool), StoreError> {
        let result = self.apply_batch(index, ops)?;
        let committed = self.maybe_commit_writer(index)?;
        Ok((result, committed))
    }

    /// Whether this shard knows `index` at all, without opening or creating anything.
    ///
    /// The schema table is the registry — `get_index_names` enumerates exactly it — so a row
    /// there means the index was created, whether or not a document has ever been written to it.
    /// The writer cache is checked first because an index taking writes is the common case and
    /// answering from it costs no transaction.
    pub fn index_exists(&self, index: &str) -> bool {
        if self.writers.contains_key(index) || self.schema_cache.contains_key(index) {
            return true;
        }
        let Ok(read_txn) = self.kv.begin_read() else {
            return false;
        };
        match read_txn.open_table(TABLE_SCHEMA) {
            Ok(schema_table) => matches!(schema_table.get(index), Ok(Some(_))),
            // No schema table yet, so no index has ever been created here.
            Err(_) => false,
        }
    }

    /// Multi-tenant apply_write method
    pub fn apply_write(&self, index: &str, op: WalOp) -> Result<u64, StoreError> {
        // A delete must not bring an index into existence. `get_or_create_index` below creates
        // one when it is absent, which is what a put wants and the opposite of what removing a
        // document that cannot be there wants — an empty index and a Tantivy directory would be
        // the trace left by deleting nothing. A put is unaffected: it is the caller that
        // legitimately creates.
        if matches!(op, WalOp::Delete { .. }) && !self.index_exists(index) {
            return Err(StoreError::IndexNotFound(index.to_string()));
        }

        // Get or create the index
        let (writer_arc, fields) = self.get_or_create_index(index)?;

        // Get sequence ID for this index
        let seq_id = {
            let counter = self.current_seq.get(index).ok_or_else(|| {
                StoreError::IndexNotFound(format!(
                    "Sequence counter not found for index: {}",
                    index
                ))
            })?;
            counter.fetch_add(1, Ordering::SeqCst) + 1
        };

        // Create dynamic table definitions
        let data_table_name = format!("data_{}", index);
        let wal_table_name = format!("wal_{}", index);
        let data_table_def = TableDefinition::<&str, &[u8]>::new(&data_table_name);
        let wal_table_def = TableDefinition::<u64, &[u8]>::new(&wal_table_name);

        // The WAL records which document changed, not what it changed to: the `data_<index>`
        // row written in this same transaction is the document, and recovery reads it there.
        let wal_data = encode_wal_entry(match &op {
            WalOp::Put { id, .. } => id,
            WalOp::Delete { id } => id,
        });

        // Evolve schema if new fields are present (declare outside transaction scope)
        let mut evolved_schema = None;
        // The id this write changed, moved out of the op by whichever arm ran, so the read cache
        // can be invalidated once the transaction below has committed.
        let touched_id: String;

        let mut write_txn = self.kv.begin_write()?;
        {
            // Set durability based on config (wal_sync flag is an intentional configuration decision)
            let durability = if self.config.wal_sync {
                Durability::Immediate
            } else {
                Durability::None
            };
            write_txn.set_durability(durability)?;
            tracing::trace!(index = %index, durability = ?durability, "Data transaction durability set (user data)");

            let mut wal_table = write_txn.open_table(wal_table_def)?;
            wal_table.insert(seq_id, wal_data.as_slice())?;

            // Apply to data table
            match op {
                WalOp::Put { id, json_blob } => {
                    // Step 1: Get cached schema for field filtering and evolution
                    // If not in cache, load from persisted metadata
                    let schema = if let Some(schema) = self.get_schema_cached(index)? {
                        schema
                    } else {
                        // Load from metadata if not in cache
                        tracing::debug!(index = %index, "Loading schema from metadata store");
                        self.get_schema(index)?
                            .map(Arc::new)
                            .unwrap_or_else(|| Arc::new(IndexSchema::default()))
                    };

                    if let Some(json_blob) = &json_blob {
                        let mut schema_mut = (*schema).clone();
                        let evolved_fields = schema_mut.evolve_from_document(json_blob);
                        if !evolved_fields.is_empty() {
                            tracing::debug!(
                                index = %index,
                                evolved_fields = ?evolved_fields,
                                "Evolved schema with new non-indexed fields (will persist in separate transaction)"
                            );
                            // Store evolved schema for persistence after data transaction
                            evolved_schema = Some(schema_mut.clone());

                            // Update cache immediately for subsequent reads
                            let schema_arc = Arc::new(schema_mut);
                            self.schema_cache.insert(index.to_string(), schema_arc);
                            // Note: No need to invalidate fields cache since new fields are non-indexed
                            // and won't affect Tantivy schema
                        }
                    }

                    // OPTIMIZATION: Skip shadow filtering when no shadow fields exist (common case)
                    let filtered_json_blob = if schema.has_shadow_fields() {
                        json_blob
                            .clone()
                            .map(|blob| filter_shadow_fields_owned(blob, &schema))
                    } else {
                        json_blob.clone()
                    };
                    let doc_data = StoredDoc {
                        json_blob: filtered_json_blob.as_ref(),
                    };
                    let doc_bytes = serde_json::to_vec(&doc_data)
                        .map_err(|e| StoreError::Serialization(e.to_string()))?;

                    let mut data_table = write_txn.open_table(data_table_def)?;

                    // Check if document is new or updated by examining insert return value
                    let old_value = data_table.insert(id.as_str(), doc_bytes.as_slice())?;
                    let is_new_document = old_value.is_none();

                    // Step 3: Build tantivy document with ONLY indexed fields
                    let mut tantivy_doc = doc!(fields.id => id.as_str());
                    if let Some(seq_field) = fields.seq {
                        tantivy_doc.add_u64(seq_field, seq_id);
                    }

                    // Step 4: Single-pass JSON traversal — skip shadows + extract Tantivy fields
                    if let Some(json_obj) = json_blob.as_ref().and_then(|v| v.as_object()) {
                        for (field_name, field_value) in json_obj {
                            // O(1) shadow field skip via pre-computed HashSet
                            if schema.shadow_fields.contains(field_name) {
                                continue;
                            }

                            // Look up schema field def + Tantivy field in one go
                            let field_def = match schema.fields.get(field_name) {
                                Some(fd) if fd.indexed => fd,
                                _ => continue,
                            };
                            let tantivy_field = match fields.indexed_fields.get(field_name) {
                                Some(tf) => tf,
                                None => continue,
                            };

                            add_json_value_to_doc(
                                &mut tantivy_doc,
                                *tantivy_field,
                                field_name,
                                &field_def.field_type,
                                field_value,
                                BadFacet::Refuse,
                            )?;
                        }
                    }

                    let writer = writer_arc.lock().unwrap_or_else(|poisoned| {
                        tracing::error!(index = %index, "Writer mutex was poisoned, recovering");
                        poisoned.into_inner()
                    });

                    // Optimized Tantivy operations: delete only if document was updated
                    if !is_new_document {
                        // Document was updated - delete old version first
                        let term = tantivy::Term::from_field_text(fields.id, &id);
                        writer.delete_term(term);
                    }
                    // Add the document (new or updated)
                    writer.add_document(tantivy_doc)?;
                    touched_id = id;
                }
                WalOp::Delete { id } => {
                    let mut data_table = write_txn.open_table(data_table_def)?;
                    data_table.remove(id.as_str())?;

                    // Delete from tantivy index
                    let term = tantivy::Term::from_field_text(fields.id, &id);
                    let writer = writer_arc.lock().unwrap_or_else(|poisoned| {
                        tracing::error!(index = %index, "Writer mutex was poisoned, recovering");
                        poisoned.into_inner()
                    });
                    writer.delete_term(term);
                    touched_id = id;
                }
            }
        }

        write_txn.commit()?;

        // The cached body for this id is now the previous one. Removing it after the commit
        // rather than before is what makes the removal stick — see `invalidate_read_cache`.
        self.invalidate_read_cache(index, [touched_id.as_str()]);

        // Persist schema evolution in separate transaction with Immediate durability
        if let Some(evolved) = evolved_schema {
            // Note: Schema persistence failure is critical but doesn't affect data consistency
            // The data has already been committed successfully, but schema evolution failed
            match self.persist_schema_evolution(index, &evolved) {
                Ok(()) => {
                    // Update cache after successful persistence
                    self.schema_cache
                        .insert(index.to_string(), Arc::new(evolved));
                    tracing::info!(index = %index, "Schema evolution persisted successfully");
                }
                Err(e) => {
                    tracing::error!(
                        index = %index,
                        error = %e,
                        "CRITICAL: Schema evolution failed after data commit. Data was saved but schema may be inconsistent."
                    );
                    // Return error to signal the issue, but note that data was already committed
                    return Err(StoreError::Serialization(format!(
                        "Schema evolution failed for index {}: {}. Data was committed but schema may be inconsistent.",
                        index, e
                    )));
                }
            }
        }

        // Increment operation counter for threshold tracking.
        // The actual commit decision is made by the writer thread loop
        // after this function returns, via maybe_commit_writer().
        self.increment_operations(index);

        Ok(seq_id)
    }

    /// Delete all data for an index using redb's efficient delete_table() function
    /// If delete_schema is true, also removes schema metadata from TABLE_SCHEMA
    pub fn delete_index_data(&self, index: &str, delete_schema: bool) -> Result<(), StoreError> {
        // Resolve (and validate) the directory before mutating any state, so an
        // invalid name cannot drop caches or redb tables on its way to failing.
        let index_path = self.index_dir(index)?;

        // Remove from caches first
        self.writers.remove(index);
        self.readers.remove(index);
        self.current_seq.remove(index);
        // Cleared rather than removed: a reader mid-flight read this index's generation and
        // must be refused its insert, which a fresh entry starting from zero would allow.
        self.invalidate_read_cache_all(index);
        self.schema_cache.remove(index);
        self.fields_cache.remove(index);
        self.budget_cache.remove(index);
        // The operations counter tracks documents buffered in the writer we just dropped.
        // Leaving it non-zero makes the next commit_index for this name believe there is
        // unflushed data.
        self.operations_counter.remove(index);
        // Note: index_init_locks is deliberately not cleared. A concurrent
        // get_or_create_index may be holding the lock, and replacing it here would let a
        // later caller initialize the same index in parallel with that holder.

        // Invalidate size cache entries for this index
        {
            let mut size_cache = self.index_size_cache.lock().unwrap();
            size_cache.retain(|key, _| !key.contains(&format!(":{}", index)));
        }

        // Delete redb tables completely using delete_table() for efficiency
        let mut write_txn = self.kv.begin_write()?;
        {
            // Index deletion always uses Immediate durability for critical metadata operations
            write_txn.set_durability(Durability::Immediate)?;
            tracing::trace!(index = %index, durability = "Immediate", "Index deletion durability set");

            let data_table_name = format!("data_{}", index);
            let wal_table_name = format!("wal_{}", index);
            let data_table_def = TableDefinition::<&str, &[u8]>::new(&data_table_name);
            let wal_table_def = TableDefinition::<u64, &[u8]>::new(&wal_table_name);

            // Delete tables using redb's delete_table function (more efficient than manual clearing)
            // Note: delete_table returns bool indicating if table existed, we ignore the result
            let _ = write_txn.delete_table(data_table_def)?;
            let _ = write_txn.delete_table(wal_table_def)?;

            // Drop the recovery checkpoint together with the data it describes. A stale
            // checkpoint outlives the deleted index and, once the name is recreated, reads
            // as "already synced" far beyond the new WAL — which would skip recovery for an
            // index that genuinely needs it.
            {
                let mut meta_table = write_txn.open_table(TABLE_RECOVERY_META)?;
                let _ = meta_table.remove(index)?;
            }

            // Conditionally delete schema metadata if requested
            if delete_schema {
                tracing::debug!(index = %index, "Deleting schema metadata from TABLE_SCHEMA");
                let mut schema_table = write_txn.open_table(TABLE_SCHEMA)?;
                let _ = schema_table.remove(index)?;
            } else {
                tracing::debug!(index = %index, "Keeping schema metadata in TABLE_SCHEMA");
            }
        }
        write_txn.commit()?;

        // Remove tantivy directory
        if index_path.exists() {
            fs::remove_dir_all(&index_path)?;
        }

        Ok(())
    }

    /// Get document by key from specific index
    pub fn get_by_key(&self, index: &str, key: &str) -> Result<Option<Vec<u8>>, StoreError> {
        if let Some(cached) = self.get_from_cache(index, key) {
            return Ok(Some(cached));
        }

        let data_table_name = format!("data_{}", index);
        let data_table_def = TableDefinition::<&str, &[u8]>::new(&data_table_name);

        // Read before the transaction opens: the snapshot this returns may predate a write that
        // is committing right now, and the generation is how `insert_into_cache` finds out.
        let seen_generation = self.cache_generation(index);
        let read_txn = self.kv.begin_read()?;

        match read_txn.open_table(data_table_def) {
            Ok(data_table) => match data_table.get(key)? {
                Some(value) => {
                    let bytes = value.value().to_vec();
                    self.insert_into_cache(index, key, bytes.clone(), seen_generation);
                    Ok(Some(bytes))
                }
                None => Ok(None),
            },
            Err(_) => Ok(None), // Table doesn't exist (index was deleted)
        }
    }

    /// Batch retrieve documents by keys from specific index
    /// More efficient than multiple get_by_key calls - uses single transaction
    pub fn get_batch_by_keys(
        &self,
        index: &str,
        keys: &[String],
    ) -> Result<Vec<(String, Vec<u8>)>, StoreError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let data_table_name = format!("data_{}", index);
        let data_table_def = TableDefinition::<&str, &[u8]>::new(&data_table_name);

        // Single read transaction for all keys. The generation is read first, for the reason
        // given in `get_by_key`.
        let seen_generation = self.cache_generation(index);
        let read_txn = self.kv.begin_read()?;
        let data_table = match read_txn.open_table(data_table_def) {
            Ok(table) => table,
            Err(_) => return Ok(Vec::new()), // Table doesn't exist
        };

        let mut results = Vec::with_capacity(keys.len());

        for key in keys {
            // Check cache first
            if let Some(cached) = self.get_from_cache(index, key) {
                results.push((key.clone(), cached));
                continue;
            }

            // Fetch from redb
            if let Some(value) = data_table.get(key.as_str())? {
                let bytes = value.value().to_vec();
                self.insert_into_cache(index, key, bytes.clone(), seen_generation);
                results.push((key.clone(), bytes));
            }
            // Skip keys that don't exist (document may have been deleted)
        }

        Ok(results)
    }

    /// Store schema for an index
    pub fn store_schema(&self, index_name: &str, schema: &IndexSchema) -> Result<(), StoreError> {
        let schema_bytes =
            serde_json::to_vec(schema).map_err(|e| StoreError::Serialization(e.to_string()))?;

        let mut write_txn = self.kv.begin_write()?;
        {
            // Schema changes always use Immediate durability for critical metadata
            write_txn.set_durability(Durability::Immediate)?;
            tracing::trace!(index = %index_name, durability = "Immediate", "Schema persistence durability set");

            let mut schema_table = write_txn.open_table(TABLE_SCHEMA)?;
            schema_table.insert(index_name, schema_bytes.as_slice())?;
        }
        write_txn.commit()?;

        Ok(())
    }

    /// Persist schema evolution with Immediate durability (critical metadata)
    fn persist_schema_evolution(
        &self,
        index_name: &str,
        schema: &IndexSchema,
    ) -> Result<(), StoreError> {
        let schema_bytes =
            serde_json::to_vec(schema).map_err(|e| StoreError::Serialization(e.to_string()))?;

        let mut write_txn = self.kv.begin_write()?;
        {
            // Schema evolution always uses Immediate durability for critical metadata
            write_txn.set_durability(Durability::Immediate)?;
            tracing::trace!(index = %index_name, durability = "Immediate", "Schema evolution persistence durability set");

            let mut schema_table = write_txn.open_table(TABLE_SCHEMA)?;
            schema_table.insert(index_name, schema_bytes.as_slice())?;
        }
        write_txn.commit()?;

        tracing::debug!(index = %index_name, "Schema evolution persisted with Immediate durability");
        Ok(())
    }

    /// Get schema for an index
    pub fn get_schema(&self, index_name: &str) -> Result<Option<IndexSchema>, StoreError> {
        let read_txn = self.kv.begin_read()?;

        match read_txn.open_table(TABLE_SCHEMA) {
            Ok(schema_table) => match schema_table.get(index_name)? {
                Some(value) => {
                    let mut schema: IndexSchema = serde_json::from_slice(value.value())
                        .map_err(|e| StoreError::Serialization(e.to_string()))?;
                    schema.rebuild_shadow_fields_cache();
                    Ok(Some(schema))
                }
                None => Ok(None),
            },
            Err(_) => Ok(None), // Table doesn't exist yet
        }
    }

    /// Get schema from cache, or load from Tantivy and cache it
    /// IMPORTANT: Always prefers Tantivy schema (source of truth) over stored schema
    pub fn get_schema_cached(&self, index: &str) -> Result<Option<Arc<IndexSchema>>, StoreError> {
        // Fast path: check cache first
        if let Some(schema) = self.schema_cache.get(index) {
            return Ok(Some(Arc::clone(schema.value())));
        }

        // Slow path: load from Tantivy (source of truth), not from stored schema
        // Get the index path and open the Tantivy index directly
        let index_path = self.index_dir(index)?;

        // Always load stored schema first (may contain non-indexed fields)
        let stored_schema = self.get_schema(index)?;

        if index_path.exists() {
            let tantivy_index = open_tantivy_index(&index_path)?;

            // Derive schema from Tantivy (indexed fields only, excludes 'id')
            let tantivy_schema = Self::derive_index_schema_from_tantivy(&tantivy_index);

            // If Tantivy has no indexed fields (empty or only 'id'), prefer stored schema
            if tantivy_schema.fields.is_empty()
                && let Some(stored) = stored_schema
            {
                self.schema_cache
                    .insert(index.to_string(), Arc::new(stored.clone()));
                tracing::debug!(index = %index, "Using stored schema (Tantivy has no indexed fields yet)");
                return Ok(Some(Arc::new(stored)));
            }

            // Use stored schema as base, then add indexed fields from Tantivy
            // This preserves all field definitions including non-indexed ones
            let mut merged_schema = stored_schema.unwrap_or_default();

            // Add indexed fields from Tantivy that might be missing
            for (name, field_def) in tantivy_schema.fields {
                merged_schema.fields.entry(name).or_insert(field_def);
            }

            // Cache the merged schema
            self.schema_cache
                .insert(index.to_string(), Arc::new(merged_schema.clone()));

            tracing::debug!(index = %index, "Loaded and cached merged schema (Tantivy + stored metadata)");
            Ok(Some(Arc::new(merged_schema)))
        } else {
            // Fallback: try to load from stored schema (metadata only)
            if let Some(stored) = stored_schema {
                self.schema_cache
                    .insert(index.to_string(), Arc::new(stored.clone()));
                tracing::debug!(index = %index, "Using stored schema as fallback (Tantivy not available)");
                Ok(Some(Arc::new(stored)))
            } else {
                Ok(None)
            }
        }
    }

    /// Invalidate cache entry when schema is updated
    pub fn invalidate_schema_cache(&self, index: &str) {
        self.schema_cache.remove(index);
        self.fields_cache.remove(index);
        tracing::debug!(index = %index, "Invalidated schema and fields cache");
    }

    /// Evolve schema from a JSON document and invalidate caches if changed
    pub fn evolve_schema_from_document(
        &self,
        index: &str,
        json_blob: &JsonValue,
    ) -> Result<Vec<String>, StoreError> {
        // Get current schema
        let mut schema = self
            .get_schema_cached(index)?
            .unwrap_or_else(|| Arc::new(IndexSchema::default()));

        // Make it mutable for evolution
        let evolved_fields = Arc::make_mut(&mut schema).evolve_from_document(json_blob);

        if !evolved_fields.is_empty() {
            tracing::info!(
                index = %index,
                evolved_fields = ?evolved_fields,
                "Schema evolved with new fields"
            );

            // Store the evolved schema
            self.store_schema_and_cache(index, &schema)?;

            // Invalidate caches to force rebuild with new schema
            self.invalidate_schema_cache(index);
        }

        Ok(evolved_fields)
    }

    /// Update both redb and cache atomically
    pub fn store_schema_and_cache(
        &self,
        index: &str,
        schema: &IndexSchema,
    ) -> Result<(), StoreError> {
        // Persist to redb first
        self.store_schema(index, schema)?;

        // Update cache
        let schema_arc = Arc::new(schema.clone());
        self.schema_cache.insert(index.to_string(), schema_arc);

        // Invalidate fields cache so it rebuilds on next access
        self.fields_cache.remove(index);

        Ok(())
    }

    /// Set the `indexed` flag on named fields of a stored schema.
    ///
    /// This is the engine half of `PATCH /api/{index}/_schema`, and it exists because the two
    /// things the operation has to get right are both only knowable here.
    ///
    /// The first is that the schema must be edited in place rather than round-tripped through
    /// a response shape. Reading the schema out as JSON, mutating it and writing it back
    /// erases every property that shape does not carry — `routing_field_name` among them,
    /// which silently changes which shard a document routes to.
    ///
    /// The second is that the stored schema is a *declaration*, and the Tantivy index is built
    /// from it — at creation, and again whenever the index data is rebuilt. So marking a field
    /// indexed is meaningful even when the current Tantivy index has no column for it: it is the
    /// first step of declare-then-reingest, which is how a discovered field is made searchable
    /// today (`delete_index_data` with `delete_schema = false`, then write again).
    ///
    /// Such a field is reported in `pending_reindex` rather than refused. Until the rebuild it
    /// simply does not match, and that is not silent — the query path reports the clause as
    /// discarded and the MCP layer refuses the search outright, so nothing reads a narrower
    /// answer as a complete one.
    pub fn update_field_indexing(
        &self,
        index: &str,
        updates: &BTreeMap<String, bool>,
    ) -> Result<SchemaFieldUpdate, StoreError> {
        let (mut schema, outcome) = self.plan_field_indexing_inner(index, updates)?;

        // Applies what this shard knows and reports what it does not, rather than refusing the
        // whole request. Shards usually hold the same schema, but semi-structured input written a
        // document at a time can leave a field on only the shards that received it — so "unknown
        // here" is not by itself a bad request. Whether a name is unknown *everywhere*, and so
        // worth refusing, is a question only the caller spanning the shards can answer.
        if outcome.applied.is_empty() {
            return Ok(outcome);
        }

        for field_name in &outcome.applied {
            if let Some(field_def) = schema.fields.get_mut(field_name) {
                field_def.indexed = updates[field_name];
            }
        }
        schema.updated_at = chrono::Utc::now().timestamp();

        // Persist without re-creating the index. A field in `pending_reindex` deliberately does
        // not get a Tantivy column here: building one would mean recreating the index and
        // discarding every document in it, which is the caller's decision to make, not this
        // function's.
        self.persist_schema_evolution(index, &schema)?;
        self.schema_cache
            .insert(index.to_string(), Arc::new(schema));

        tracing::info!(
            index = %index,
            applied = ?outcome.applied,
            "Field indexing flags updated"
        );

        Ok(outcome)
    }

    /// What [`HybridStore::update_field_indexing`] would do, without doing it.
    ///
    /// A schema spans every shard that holds the index, so a caller that wants the edit to be
    /// all-or-nothing across them has to learn whether each one would accept it before any of
    /// them writes.
    pub fn plan_field_indexing(
        &self,
        index: &str,
        updates: &BTreeMap<String, bool>,
    ) -> Result<SchemaFieldUpdate, StoreError> {
        self.plan_field_indexing_inner(index, updates)
            .map(|(_, outcome)| outcome)
    }

    /// The classification both the plan and the apply path share, with the schema it read.
    fn plan_field_indexing_inner(
        &self,
        index: &str,
        updates: &BTreeMap<String, bool>,
    ) -> Result<(IndexSchema, SchemaFieldUpdate), StoreError> {
        let schema = self
            .get_schema_cached(index)?
            .map(|arc| (*arc).clone())
            .ok_or_else(|| StoreError::IndexNotFound(index.to_string()))?;

        // Whether the built index has a column for a field decides only whether the edit takes
        // effect *now* or at the next rebuild — not whether it is allowed. Opening the index
        // reads `meta.json`; it does not touch the writer lockfile, so this is safe against a
        // live writer.
        let index_path = self.index_dir(index)?;
        let tantivy_schema = if index_path.join("meta.json").exists() {
            Some(open_tantivy_index(&index_path)?.schema())
        } else {
            None
        };

        let mut outcome = SchemaFieldUpdate::default();

        for (field_name, want_indexed) in updates {
            let Some(field_def) = schema.fields.get(field_name) else {
                outcome.unknown.push(field_name.clone());
                continue;
            };

            if field_def.indexed == *want_indexed {
                outcome.unchanged.push(field_name.clone());
                continue;
            }

            let is_promotion = *want_indexed;
            let missing_from_tantivy = tantivy_schema
                .as_ref()
                .is_some_and(|schema| schema.get_field(field_name).is_err());

            // Applied either way. The flag is a declaration, and the index is built from the
            // declaration — so this takes effect at the next rebuild rather than immediately.
            outcome.applied.push(field_name.clone());
            if is_promotion && missing_from_tantivy {
                outcome.pending_reindex.push(field_name.clone());
            }
        }

        Ok((schema, outcome))
    }

    /// How many WAL entries are waiting for Tantivy to catch up on this index.
    ///
    /// This is the quantity that decides recovery cost: it is what a replay would have to
    /// read, and zero is what lets startup skip the index without opening Tantivy at all. In
    /// steady state it returns to zero at every commit, so a value that keeps climbing means
    /// commits are not keeping up with writes.
    pub fn pending_wal_entries(&self, index: &str) -> Result<u64, StoreError> {
        let wal_table_name = format!("wal_{}", index);
        let wal_table_def = TableDefinition::<u64, &[u8]>::new(&wal_table_name);

        let read_txn = self.kv.begin_read()?;
        match read_txn.open_table(wal_table_def) {
            Ok(table) => Ok(table.len()?),
            // No table at all: the index has never been written to.
            Err(_) => Ok(0),
        }
    }

    /// Get max WAL ID for a specific index
    /// Uses B-tree last() for O(log n) access instead of O(n) full table scan
    fn get_max_wal_id_for_index(&self, index: &str) -> Result<u64, StoreError> {
        let wal_table_name = format!("wal_{}", index);
        let wal_table_def = TableDefinition::<u64, &[u8]>::new(&wal_table_name);

        let read_txn = self.kv.begin_read()?;

        match read_txn.open_table(wal_table_def) {
            Ok(wal_table) => {
                // redb B-tree stores keys in sorted order; last() is O(log n)
                if let Some(result) = wal_table.last()? {
                    let max_id = result.0.value();
                    tracing::debug!(
                        index = %index,
                        max_wal_id = max_id,
                        "Retrieved max WAL ID from redb (B-tree last)"
                    );
                    Ok(max_id)
                } else {
                    tracing::debug!(index = %index, "WAL table is empty, returning 0");
                    Ok(0)
                }
            }
            Err(_) => {
                tracing::debug!(index = %index, "WAL table does not exist, returning 0");
                Ok(0) // Table doesn't exist yet
            }
        }
    }

    /// Record `seq` in the recovery metadata table on its own.
    ///
    /// Only the backfill in [`Self::checkpoint_seq`] uses this. The steady-state path writes
    /// the same table through [`Self::checkpoint_committed`], together with the WAL truncation
    /// the checkpoint authorises, so that the two can never be separated by a crash.
    fn persist_committed_seq(&self, index: &str, seq: u64) -> Result<(), StoreError> {
        let mut write_txn = self.kv.begin_write()?;
        {
            write_txn.set_durability(Durability::Immediate)?;
            let mut meta_table = write_txn.open_table(TABLE_RECOVERY_META)?;
            meta_table.insert(index, seq)?;
        }
        write_txn.commit()?;
        Ok(())
    }

    /// Read the persisted last committed sequence for an index from the recovery metadata table.
    ///
    /// The checkpoint now travels inside Tantivy's commit payload, so this is the fallback
    /// [`Self::checkpoint_seq`] consults for an index whose last commit predates the stamp.
    /// Returns `None` when no row exists.
    fn get_persisted_committed_seq(&self, index: &str) -> Result<Option<u64>, StoreError> {
        let read_txn = self.kv.begin_read()?;
        match read_txn.open_table(TABLE_RECOVERY_META) {
            Ok(meta_table) => match meta_table.get(index)? {
                Some(guard) => Ok(Some(guard.value())),
                None => Ok(None),
            },
            Err(_) => Ok(None), // Table doesn't exist yet
        }
    }

    /// Helper: Get fields cache (Lock-Free Read)
    fn get_fields_for_index(
        &self,
        index: &str,
        tantivy_index: &Index,
    ) -> Result<SchemaFields, StoreError> {
        // Fast path: fields already cached
        if let Some(fields) = self.fields_cache.get(index) {
            return Ok(fields.value().clone());
        }

        // Derive fields from the opened Tantivy index (Field handles must match the index)
        let fields = Self::load_fields_from_existing_index(tantivy_index)?;
        self.fields_cache.insert(index.to_string(), fields.clone());
        Ok(fields)
    }

    /// Get or create a cached IndexReader for the given index.
    ///
    /// Lock-free fast path via DashMap. Readers reload only when `commit_index` tells them to.
    fn get_reader(&self, index: &str) -> Result<Option<(IndexReader, SchemaFields)>, StoreError> {
        // Fast path: Zero-lock retrieval from cache
        if let Some(reader_ref) = self.readers.get(index) {
            let reader = reader_ref.value();
            // No reload() here: `commit_index` reloads through `smart_refresh_reader` as part
            // of the commit, so a cached reader is already current.

            // Get fields (fast lookup)
            let tantivy_index = reader.searcher().index().clone();
            let fields = self.get_fields_for_index(index, &tantivy_index)?;

            return Ok(Some((reader.clone(), fields)));
        }

        // Slow path: Index not cached, need to open and cache it
        let index_path = self.index_dir(index)?;
        if !index_path.exists() || !index_path.join("meta.json").exists() {
            return Ok(None);
        }

        // Use DashMap entry API for concurrent-safe creation
        let reader = self
            .readers
            .entry(index.to_string())
            .or_try_insert_with(|| {
                let tantivy_index = open_tantivy_index(&index_path)?;

                // `ReloadPolicy::Manual`, deliberately. Every commit in this process goes
                // through `commit_index`, which reloads the reader itself, so the alternative
                // (`OnCommitWithDelay`) would only ever reload a *second* time for a commit
                // already reflected here. That redundant reload is not free:
                //
                // - it makes tantivy spawn a `thread-tantivy-meta-file-watcher` per open
                //   index, each waking every 500ms forever to read and checksum meta.json,
                // - and because reload() rebuilds every SegmentReader, it discards the caches
                //   `warm_index` had just filled for the generation the first reload made.
                //
                // What manual reloading gives up: segments published by a background merge
                // become visible at the next commit rather than within 500ms. Nothing reads
                // stale data — the live searcher keeps its own (pre-merge) segments
                // referenced, and merges are triggered by commits anyway.
                //
                // Also deliberately no tantivy `Warmer`: warmers run synchronously inside
                // reload(), which happens on the shard writer thread, so one would put
                // warming on the write hot path — and cost another thread per open index.
                // Warming is driven explicitly by `warm_index` instead.
                let reader = tantivy_index
                    .reader_builder()
                    .reload_policy(tantivy::ReloadPolicy::Manual)
                    .try_into()?;

                Ok::<IndexReader, StoreError>(reader)
            })?;

        // Warm up fields cache
        let tantivy_index = reader.value().searcher().index().clone();
        let fields = self.get_fields_for_index(index, &tantivy_index)?;

        Ok(Some((reader.value().clone(), fields)))
    }

    /// Warm one index so the first query does not pay cold-start costs.
    ///
    /// Opens and caches the `IndexReader` (which is what the search path uses — the *writer*
    /// cache is irrelevant to queries), populates the schema and field caches, and builds each
    /// segment's term dictionaries. See [`warm_segment`] for why that is the whole list.
    ///
    /// Best-effort by design. Warming is driven explicitly — at startup and after commits —
    /// rather than by a registered tantivy `Warmer`, which would run on the writer thread
    /// inside `reload()` and cost a background thread per open index. The gap that buys:
    /// segments published by a background merge are not warmed until the next commit on that
    /// index. That is a cheap gap, because a merge writes its output segment through this
    /// process, so the merged data is already in page cache — the part warming cannot
    /// reconstruct for free — and merges are themselves triggered by commits, so an active
    /// index warms its merged segments on the following commit.
    ///
    /// Safe to call concurrently and repeatedly: an already-warm searcher generation is a
    /// no-op. Returns `None` when the index has no Tantivy directory yet (nothing to warm).
    pub fn warm_index(&self, index: &str) -> Result<Option<IndexWarmupStats>, StoreError> {
        let start = Instant::now();
        self.warmup_states
            .insert(index.to_string(), IndexWarmupState::Warming);

        // Loading the schema here keeps the first query off the redb metadata path.
        if let Err(e) = self.get_schema_cached(index) {
            self.warmup_states
                .insert(index.to_string(), IndexWarmupState::Failed);
            return Err(e);
        }

        let reader = match self.get_reader(index) {
            Ok(Some((reader, _fields))) => reader,
            Ok(None) => {
                // No Tantivy directory: an index that has a schema but has never been
                // written to. Nothing to warm, and nothing wrong.
                self.warmup_states
                    .insert(index.to_string(), IndexWarmupState::Warm);
                return Ok(None);
            }
            Err(e) => {
                self.warmup_states
                    .insert(index.to_string(), IndexWarmupState::Failed);
                return Err(e);
            }
        };

        let searcher = reader.searcher();
        let generation = searcher.generation().generation_id();
        let segments = searcher.segment_readers().len();

        // Skip if this exact generation was already warmed. `reader.searcher()` hands out the
        // same searcher until a reload replaces it, so an unchanged generation means the same
        // SegmentReaders with the same filled caches.
        let already_warm = self
            .warmed_generations
            .get(index)
            .is_some_and(|warmed| *warmed.value() == generation);

        let segments_warmed = if already_warm {
            0
        } else {
            for segment_reader in searcher.segment_readers() {
                warm_segment(index, segment_reader);
            }
            self.warmed_generations
                .insert(index.to_string(), generation);
            segments
        };

        let stats = IndexWarmupStats {
            index: index.to_string(),
            segments,
            segments_warmed,
            generation,
            num_docs: searcher.num_docs(),
            elapsed_ms: start.elapsed().as_millis(),
        };

        self.warmup_states
            .insert(index.to_string(), IndexWarmupState::Warm);

        if segments_warmed > 0 {
            tracing::debug!(
                index = %index,
                segments = stats.segments,
                generation = generation,
                num_docs = stats.num_docs,
                elapsed_ms = stats.elapsed_ms,
                "Index warmed"
            );
        }

        Ok(Some(stats))
    }

    /// Current warmup state for every index this store knows about.
    pub fn warmup_states(&self) -> HashMap<String, IndexWarmupState> {
        self.warmup_states
            .iter()
            .map(|entry| (entry.key().clone(), *entry.value()))
            .collect()
    }

    /// Whether an index has completed warmup and will answer from warm buffers.
    pub fn is_index_warm(&self, index: &str) -> bool {
        self.warmup_states
            .get(index)
            .is_some_and(|state| *state.value() == IndexWarmupState::Warm)
    }

    /// Parse a query against an index without running it, and report what the parser found.
    ///
    /// This exists because the only way to learn whether a query parses used to be to run it and
    /// read what a search discarded. Checking that quotes and parentheses balance is not the same
    /// question: the interesting failure is a query that balances fine and still does not parse,
    /// and resolving a field name needs the index, so nothing above the engine can answer it.
    ///
    /// Parses through exactly the path a search takes — same normalization, same default field
    /// set, same lenient parser — so a query this accepts is one a search will run, and the
    /// clauses it reports as discarded are the clauses that search would drop.
    ///
    /// `Ok(None)` when the index has no Tantivy directory yet: an index that has a schema but has
    /// never been written to has nothing to resolve field names against. That is not an error,
    /// and it is the same distinction [`HybridStore::warm_index`] draws.
    pub fn validate_query(
        &self,
        index: &str,
        query: &str,
    ) -> Result<Option<QueryValidation>, StoreError> {
        let Some((reader, fields)) = self.get_reader(index)? else {
            return Ok(None);
        };

        let searcher = reader.searcher();
        let tantivy_index = searcher.index();
        let schema = self
            .get_schema_cached(index)?
            .unwrap_or_else(|| Arc::new(IndexSchema::default()));

        // An exact id or shadow-field lookup never reaches the parser on the search path —
        // `parse_exact_id_query` answers it from the key-value store — so validating it must
        // not report the identifier's field as a discarded clause.
        if parse_exact_id_query(query, &schema).is_some() {
            return Ok(Some(QueryValidation {
                normalized_query: query.to_string(),
                syntax_errors: Vec::new(),
                discarded: Vec::new(),
            }));
        }

        let (normalized_query, prefix_notes, query_parser) =
            prepare_query_parser(tantivy_index, &fields, &schema, query);

        // The query itself is discarded: what is wanted is the error list, which is the half a
        // search throws away after deciding it can still run.
        let (_parsed_query, parse_errors) = query_parser.parse_query_lenient(&normalized_query);

        let syntax_errors: Vec<String> = parse_errors
            .iter()
            .filter(|err| !is_recovered_ambiguity(err))
            .filter_map(|err| match err {
                tantivy::query::QueryParserError::SyntaxError(detail) => Some(detail.clone()),
                _ => None,
            })
            .collect();

        // Syntax errors are reported above with the parser's own wording and position, so they
        // are kept out of the discarded list rather than appearing twice in weaker words.
        let semantic_errors: Vec<tantivy::query::QueryParserError> = parse_errors
            .into_iter()
            .filter(|err| !matches!(err, tantivy::query::QueryParserError::SyntaxError(_)))
            .collect();

        let mut discarded = describe_discarded_all(&semantic_errors, query, &schema);
        discarded.extend(prefix_notes);

        Ok(Some(QueryValidation {
            normalized_query,
            syntax_errors,
            discarded,
        }))
    }

    /// Search documents in a specific index
    /// Uses tantivy for search, then batch-retrieves complete documents from redb
    /// Returns (results, total_hits) where total_hits is the total number of matching documents
    pub fn search_documents(
        &self,
        index: &str,
        query: &str,
        limit: usize,
        _sort: Option<&SortSpec>,
    ) -> Result<SearchOutcome, StoreError> {
        // Get reader and field mapping from cache or disk
        let (reader, fields) = match self.get_reader(index)? {
            Some(r) => r,
            None => {
                // Normal for an index with no commits yet, and emitted once per shard per
                // search — four lines for one query against an empty index at `warn`.
                debug!(index = %index, "No tantivy reader found for index");
                return Ok(SearchOutcome::empty());
            }
        };

        let searcher = reader.searcher();
        let tantivy_index = searcher.index();

        // Get cached schema to determine which fields are indexed
        let schema = self
            .get_schema_cached(index)?
            .unwrap_or_else(|| Arc::new(IndexSchema::default()));

        // Count-only mode: limit=0 means return just total_hits without document data.
        // Runs only the Count collector (cheaper than TopDocs) and skips all redb lookups.
        if limit == 0 {
            // For exact ID queries, we can short-circuit: total_hits is 0 or 1
            if let Some((id_value, _)) = parse_exact_id_query(query, &schema) {
                let exists = self.get_batch_by_keys(index, &[id_value])?.len();
                let total_hits = if exists > 0 { 1 } else { 0 };
                // No parse happens on this path, so nothing can be discarded.
                return Ok(SearchOutcome::counted(total_hits, Vec::new(), false));
            }

            let (normalized_query, prefix_notes, query_parser) =
                prepare_query_parser(tantivy_index, &fields, &schema, query);
            let (parsed_query, parse_errors) = query_parser.parse_query_lenient(&normalized_query);
            let mut discarded = describe_discarded_all(&parse_errors, query, &schema);
            discarded.extend(prefix_notes);

            let emptied = !discarded.is_empty() && nothing_survived(parsed_query.as_ref());

            if !discarded.is_empty() {
                if emptied {
                    warn!(
                        index = %index,
                        query = %normalized_query,
                        discarded = ?discarded,
                        "Count-only: every clause was discarded; nothing was left to run and the count is zero"
                    );
                } else {
                    warn!(
                        index = %index,
                        query = %normalized_query,
                        discarded = ?discarded,
                        "Count-only: query parser discarded clauses; the count does not answer the query as written"
                    );
                }
            }

            let count_collector = tantivy::collector::Count;
            let total_hits = searcher.search(&parsed_query, &count_collector)?;

            debug!(
                index = %index,
                total_hits = total_hits,
                "Count-only search completed (limit=0)"
            );

            return Ok(SearchOutcome::counted(total_hits, discarded, emptied));
        }

        // Check if this is an exact ID lookup (id:field or shadow field) that can bypass Tantivy
        if let Some((id_value, _is_exact_id_query)) = parse_exact_id_query(query, &schema) {
            debug!(
                index = %index,
                id_value = %id_value,
                "Exact ID query detected, bypassing Tantivy search"
            );

            // Simulate Tantivy result with score 1.0
            let doc_ids_with_scores = vec![(1.0, id_value)];

            // Skip to Step 2: Batch retrieve from redb (reuse existing logic)
            let doc_ids: Vec<String> = doc_ids_with_scores
                .iter()
                .map(|(_, id)| id.clone())
                .collect();

            let redb_docs = self.get_batch_by_keys(index, &doc_ids)?;

            debug!(
                index = %index,
                requested_ids = doc_ids.len(),
                retrieved_docs = redb_docs.len(),
                "Retrieved documents from redb (direct ID lookup)"
            );

            // Create lookup map for O(1) access
            let doc_map: std::collections::HashMap<String, Vec<u8>> =
                redb_docs.into_iter().collect();

            // Step 3: Combine scores with complete documents (reuse existing logic)
            let mut results = Vec::new();
            for (score, doc_id) in doc_ids_with_scores {
                if let Some(doc_bytes) = doc_map.get(&doc_id) {
                    // Deserialize complete document from redb
                    let stored_doc: StoredDocOwned = serde_json::from_slice(doc_bytes)
                        .map_err(|e| StoreError::Serialization(e.to_string()))?;

                    // Get schema for shadow field reconstruction
                    let schema = if let Some(schema) = self.get_schema_cached(index)? {
                        schema
                    } else {
                        self.get_schema(index)?
                            .map(Arc::new)
                            .unwrap_or_else(|| Arc::new(IndexSchema::default()))
                    };

                    // OPTIMIZATION: Pass ownership to avoid cloning all fields
                    let final_doc = if let Some(json_blob) = stored_doc.json_blob {
                        reconstruct_shadow_fields_owned(json_blob, &schema, &doc_id)
                    } else {
                        // Fallback if blob was empty
                        serde_json::json!({ "id": doc_id })
                    };

                    results.push((score, final_doc));
                } else {
                    trace!(index = %index, doc_id = %doc_id, "Document not found in redb lookup map");
                }
            }

            let total_hits = if results.is_empty() { 0 } else { 1 };
            // No parse happens on this path, and no sort: an id lookup returns the one document
            // it names. So nothing can be discarded and no order can be approximate.
            return Ok(SearchOutcome {
                hits: results,
                total_hits,
                discarded: Vec::new(),
                approximate_sort: None,
                emptied: false,
            });
        }

        let (normalized_query, prefix_notes, query_parser) =
            prepare_query_parser(tantivy_index, &fields, &schema, query);

        // Lenient, so one bad clause does not fail the whole query; what it drops is reported
        // through `SearchOutcome::discarded` rather than swallowed.
        let (parsed_query, parse_errors) = query_parser.parse_query_lenient(&normalized_query);
        let mut discarded = describe_discarded_all(&parse_errors, query, &schema);
        discarded.extend(prefix_notes);

        let emptied = !discarded.is_empty() && nothing_survived(parsed_query.as_ref());

        if !discarded.is_empty() {
            // A dropped clause does not move the result set one way: it widens a conjunction,
            // narrows a disjunction, and empties a query that had nothing else to run. Only the
            // last is knowable from here, and it is the one worth separating — the zero an
            // emptied query reports is not a negative answer, it is no answer at all.
            if emptied {
                warn!(
                    index = %index,
                    query = %normalized_query,
                    discarded = ?discarded,
                    "Every clause was discarded; nothing was left to run and the result set is empty"
                );
            } else {
                warn!(
                    index = %index,
                    query = %normalized_query,
                    discarded = ?discarded,
                    "Query parser discarded clauses; the results do not answer the query as written"
                );
            }
        }

        // Flag set when sorting by a string field (post-fetch alphabetic sort).
        // The field name and order are captured here and used after redb retrieval.
        let mut string_sort: Option<(String, SortOrder)> = None;

        let (top_docs, total_hits) = if let Some(sort_spec) = _sort {
            // A sort names one value under two names, and on an index with a shadow field they
            // are not the same name.
            //
            // A shadow field *is* the document key under the source's own name: the query path
            // maps it to `id` (`rewrite_shadow_fields`), so a sort maps the same way or the two
            // disagree about what the caller's name means. That gives the column to order on.
            // But the value the caller reads back is not under that name — shadow
            // reconstruction *replaces* `id` with the shadow field on the way out (see
            // `reconstruct_shadow_fields_owned`), so the post-fetch sort below, and every merge
            // above this one, has to look for it under the name the document actually carries.
            let sorts_by_document_key =
                sort_spec.field == "id" || schema.is_shadow_field(&sort_spec.field);
            let column_name: &str = if sorts_by_document_key {
                "id"
            } else {
                &sort_spec.field
            };
            let document_name: String = if sorts_by_document_key {
                document_key_field(&schema)
            } else {
                sort_spec.field.clone()
            };

            // Get field from schema to check type and FAST flag
            let schema = tantivy_index.schema();

            // `_seq` is FAST, so it would otherwise satisfy every check below and sort — but
            // only within one shard. Document bodies are served from redb, which has no `_seq`
            // key, so nothing is stamped for a scatter-gather merge to order by: the caller
            // gets a partial ordering and no error. Every field listing already hides `_seq`,
            // so refusing it here is what makes the engine agree with what `sortable_fields`
            // advertises.
            if sort_spec.field == "_seq" {
                return Err(StoreError::FieldNotFound(sort_spec.field.clone()));
            }

            let field = schema
                .get_field(column_name)
                .map_err(|_| StoreError::FieldNotFound(sort_spec.field.clone()))?;

            let field_entry = schema.get_field_entry(field);

            // Text/String fields don't require the FAST flag — they use a post-fetch sort.
            let is_str_field =
                matches!(field_entry.field_type(), tantivy::schema::FieldType::Str(_));

            if !field_entry.is_fast() && !is_str_field {
                return Err(StoreError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "Field '{}' is not marked as FAST. Only FAST fields support sorting.",
                        sort_spec.field
                    ),
                )));
            }

            let order = match sort_spec.order {
                SortOrder::Asc => tantivy::Order::Asc,
                SortOrder::Desc => tantivy::Order::Desc,
            };

            // Run a TopDocs sort ordered by a FAST field of the given type. The generic
            // type MUST match the field's actual type — `order_by_fast_field::<u64>` on an
            // i64/f64/date field returns a Tantivy SchemaError at collection time.
            // The sort key itself is discarded downstream (ordering is encoded in the Vec
            // sequence), so all branches normalize to `(None, doc_address)`.
            macro_rules! collect_sorted {
                ($t:ty) => {{
                    let top_docs_collector = tantivy::collector::TopDocs::with_limit(limit)
                        .order_by_fast_field::<$t>(column_name, order);
                    let count_collector = tantivy::collector::Count;

                    // Use MultiCollector to get both results and count in single query execution
                    let mut multi_collector = tantivy::collector::MultiCollector::new();
                    let top_docs_handle = multi_collector.add_collector(top_docs_collector);
                    let count_handle = multi_collector.add_collector(count_collector);

                    let mut multi_fruit = searcher.search(&parsed_query, &multi_collector)?;
                    let sorted: Vec<(Option<$t>, tantivy::DocAddress)> =
                        top_docs_handle.extract(&mut multi_fruit);
                    let total_hits = count_handle.extract(&mut multi_fruit);

                    let docs: Vec<(Option<u64>, tantivy::DocAddress)> =
                        sorted.into_iter().map(|(_, addr)| (None, addr)).collect();
                    (SearchResult::Sorted(docs), total_hits)
                }};
            }

            // u64, i64, f64 and Date sort on their FAST column; text sorts on its own when it
            // has one, and falls back to a post-fetch sort of scored candidates when it does not.
            match field_entry.field_type() {
                tantivy::schema::FieldType::U64(_) => collect_sorted!(u64),
                tantivy::schema::FieldType::I64(_) => collect_sorted!(i64),
                tantivy::schema::FieldType::F64(_) => collect_sorted!(f64),
                tantivy::schema::FieldType::Date(_) => collect_sorted!(tantivy::DateTime),
                // A text field with a fast column is sorted by the column, in the collector,
                // like any other sortable type. Tantivy keys this on the term ordinal, which
                // its term dictionary holds in lexicographic order, so the result is a true
                // alphabetical total order over *every* match rather than over a sample —
                // which is what makes a deep page of a text sort mean anything.
                tantivy::schema::FieldType::Str(_) if field_entry.is_fast() => {
                    let top_docs_collector = tantivy::collector::TopDocs::with_limit(limit)
                        .order_by_string_fast_field(column_name, order);
                    let count_collector = tantivy::collector::Count;
                    let mut multi_collector = tantivy::collector::MultiCollector::new();
                    let top_docs_handle = multi_collector.add_collector(top_docs_collector);
                    let count_handle = multi_collector.add_collector(count_collector);

                    let mut multi_fruit = searcher.search(&parsed_query, &multi_collector)?;
                    let sorted: Vec<(Option<String>, tantivy::DocAddress)> =
                        top_docs_handle.extract(&mut multi_fruit);
                    let total_hits = count_handle.extract(&mut multi_fruit);

                    // Ordering is carried by the sequence from here on, as it is for the
                    // numeric branches, so the key itself is dropped.
                    let docs: Vec<(Option<u64>, tantivy::DocAddress)> =
                        sorted.into_iter().map(|(_, addr)| (None, addr)).collect();
                    (SearchResult::Sorted(docs), total_hits)
                }
                tantivy::schema::FieldType::Str(_) => {
                    // No fast column on this field, so there is nothing to order on in the
                    // collector: candidates are taken by relevance and sorted alphabetically
                    // after the redb fetch. The result is the alphabetical order *of the
                    // highest-scoring `limit * 2`*, not of everything that matched — the
                    // alphabetically first document does not score its way in unless the query
                    // happens to favour it.
                    //
                    // Declaring the field `fast` takes the branch above instead and removes the
                    // approximation. The column is written at index time, so that declaration
                    // has to be in place before the data is: on an index that already exists
                    // there is no way to add one, and no reindex to add it with (ROADMAP
                    // Phase 15). `sortable` on the field's description reports which case an
                    // index is in.
                    //
                    // `debug`, not `warn`: this fires once per shard per search, and the caller
                    // is told in the response itself through `approximate_sort` — which is where
                    // it can be acted on. A log line per query would be noise in front of an
                    // operator who cannot fix it from there anyway.
                    debug!(
                        index = %index,
                        field = %sort_spec.field,
                        "sorting on a text field without a fast column; the order returned is \
                         the alphabetical order of the top-scoring candidates, not of all matches"
                    );
                    let budget = limit.saturating_mul(2);
                    string_sort = Some((document_name, sort_spec.order));

                    let top_docs_collector =
                        tantivy::collector::TopDocs::with_limit(budget).order_by_score();
                    let count_collector = tantivy::collector::Count;
                    let mut multi_collector = tantivy::collector::MultiCollector::new();
                    let top_docs_handle = multi_collector.add_collector(top_docs_collector);
                    let count_handle = multi_collector.add_collector(count_collector);

                    let mut multi_fruit = searcher.search(&parsed_query, &multi_collector)?;
                    let top_docs: Vec<(f32, tantivy::DocAddress)> =
                        top_docs_handle.extract(&mut multi_fruit);
                    let total_hits = count_handle.extract(&mut multi_fruit);
                    (SearchResult::Unsorted(top_docs), total_hits)
                }
                _ => {
                    return Err(StoreError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!(
                            "Field '{}' type {:?} is not sortable. Supported types: u64, i64, f64, date (FAST), text, string.",
                            sort_spec.field,
                            field_entry.field_type()
                        ),
                    )));
                }
            }
        } else {
            // Default: sort by relevance score using MultiCollector
            let top_docs_collector =
                tantivy::collector::TopDocs::with_limit(limit).order_by_score();
            let count_collector = tantivy::collector::Count;
            let mut multi_collector = tantivy::collector::MultiCollector::new();
            let top_docs_handle = multi_collector.add_collector(top_docs_collector);
            let count_handle = multi_collector.add_collector(count_collector);

            let mut multi_fruit = searcher.search(&parsed_query, &multi_collector)?;
            let top_docs: Vec<(f32, tantivy::DocAddress)> =
                top_docs_handle.extract(&mut multi_fruit);
            let total_hits = count_handle.extract(&mut multi_fruit);

            (SearchResult::Unsorted(top_docs), total_hits)
        };

        debug!(
            index = %index,
            hits_returned = match &top_docs {
                SearchResult::Sorted(docs) => docs.len(),
                SearchResult::Unsorted(docs) => docs.len(),
            },
            total_hits = total_hits,
            "Tantivy search completed"
        );

        let is_empty = match &top_docs {
            SearchResult::Sorted(docs) => docs.is_empty(),
            SearchResult::Unsorted(docs) => docs.is_empty(),
        };

        if is_empty {
            return Ok(SearchOutcome::counted(total_hits, discarded, emptied));
        }

        // Step 1: Extract document IDs from Tantivy results using direct stored-field access
        let capacity = match &top_docs {
            SearchResult::Sorted(docs) => docs.len(),
            SearchResult::Unsorted(docs) => docs.len(),
        };
        let mut doc_ids_with_scores = Vec::with_capacity(capacity);

        match &top_docs {
            SearchResult::Sorted(docs) => {
                for (_sort_key, doc_address) in docs {
                    let doc: tantivy::TantivyDocument = searcher.doc(*doc_address)?;

                    if let Some(value) = doc.get_first(fields.id)
                        && let Some(id_str) = value.as_str()
                    {
                        debug!(
                            index = %index,
                            doc_id = %id_str,
                            doc_addr = ?doc_address,
                            "Tantivy document matched"
                        );
                        // For sorted results, use 1.0 as placeholder score (sort order is what matters)
                        doc_ids_with_scores.push((1.0, id_str.to_string()));
                    } else {
                        let tantivy_doc = doc.to_json(&tantivy_index.schema());
                        warn!(
                            index = %index,
                            doc_addr = ?doc_address,
                            tantivy_doc = %tantivy_doc,
                            "Tantivy document missing or invalid 'id' field"
                        );
                    }
                }
            }
            SearchResult::Unsorted(docs) => {
                for (score, doc_address) in docs {
                    let doc: tantivy::TantivyDocument = searcher.doc(*doc_address)?;

                    if let Some(value) = doc.get_first(fields.id)
                        && let Some(id_str) = value.as_str()
                    {
                        debug!(
                            index = %index,
                            doc_id = %id_str,
                            doc_addr = ?doc_address,
                            "Tantivy document matched"
                        );
                        doc_ids_with_scores.push((*score, id_str.to_string()));
                    } else {
                        let tantivy_doc = doc.to_json(&tantivy_index.schema());
                        warn!(
                            index = %index,
                            doc_addr = ?doc_address,
                            tantivy_doc = %tantivy_doc,
                            "Tantivy document missing or invalid 'id' field"
                        );
                    }
                }
            }
        }

        debug!(
            index = %index,
            ids_extracted = doc_ids_with_scores.len(),
            "Extracted document IDs from tantivy results"
        );

        // Step 2: Batch retrieve complete documents from redb (single transaction)
        let doc_ids: Vec<String> = doc_ids_with_scores
            .iter()
            .map(|(_, id)| id.clone())
            .collect();

        let redb_docs = self.get_batch_by_keys(index, &doc_ids)?;

        debug!(
            index = %index,
            requested_ids = doc_ids.len(),
            retrieved_docs = redb_docs.len(),
            "Retrieved documents from redb"
        );

        // Create lookup map for O(1) access
        let doc_map: std::collections::HashMap<String, Vec<u8>> = redb_docs.into_iter().collect();

        // Step 3: Combine scores with complete documents
        let mut results = Vec::new();
        for (score, doc_id) in doc_ids_with_scores {
            if let Some(doc_bytes) = doc_map.get(&doc_id) {
                // Deserialize complete document from redb
                let stored_doc: StoredDocOwned = serde_json::from_slice(doc_bytes)
                    .map_err(|e| StoreError::Serialization(e.to_string()))?;

                // Get schema for shadow field reconstruction
                let schema = if let Some(schema) = self.get_schema_cached(index)? {
                    schema
                } else {
                    self.get_schema(index)?
                        .map(Arc::new)
                        .unwrap_or_else(|| Arc::new(IndexSchema::default()))
                };

                // OPTIMIZATION: Pass ownership to avoid cloning all fields
                let final_doc = if let Some(json_blob) = stored_doc.json_blob {
                    reconstruct_shadow_fields_owned(json_blob, &schema, &doc_id)
                } else {
                    // Fallback if blob was empty
                    serde_json::json!({ "id": doc_id })
                };

                results.push((score, final_doc));
            } else {
                trace!(index = %index, doc_id = %doc_id, "Document not found in redb lookup map");
            }
        }

        // Post-fetch alphabetic sort for string fields.
        // Candidates were collected with budget = limit*2; sort and truncate to limit.
        //
        // Documents arrive in Tantivy's order, which is total — it breaks its own ties on
        // document address — so a value repeated across documents falls back to that order
        // rather than to whatever the comparison happened to leave in place. Stated as a
        // comparison on the original position instead of relying on the sort being stable: a
        // later switch to `sort_unstable_by` would otherwise make this shard's answer vary
        // between runs, and every merge above it inherits that.
        // Read before the sort below consumes it: an approximate order is a property of the
        // answer, and the caller has to be able to see it on the answer.
        //
        // Named as the *documents* name it, not as the request did. The two differ only when
        // the sort is on the document key of a shadow index, where a caller may say `id` and
        // every hit comes back carrying the shadow name instead — reporting `id` there names a
        // field absent from every hit in the same response, which is the one thing a caller
        // cannot check the order against.
        let approximate_sort = string_sort.as_ref().map(|(name, _)| name.clone());

        if let Some((field, order)) = string_sort {
            let mut ranked: Vec<(usize, _)> = std::mem::take(&mut results)
                .into_iter()
                .enumerate()
                .collect();
            ranked.sort_by(|(left_position, a), (right_position, b)| {
                let av = a.1.get(&field).and_then(|v| v.as_str());
                let bv = b.1.get(&field).and_then(|v| v.as_str());
                let base = match (av, bv) {
                    (Some(ax), Some(bx)) => ax.cmp(bx),
                    (Some(_), None) => std::cmp::Ordering::Less,
                    (None, Some(_)) => std::cmp::Ordering::Greater,
                    (None, None) => std::cmp::Ordering::Equal,
                };
                let base = match order {
                    SortOrder::Asc => base,
                    SortOrder::Desc => base.reverse(),
                };
                base.then_with(|| left_position.cmp(right_position))
            });
            results = ranked.into_iter().map(|(_, hit)| hit).collect();
            results.truncate(limit);
        }

        Ok(SearchOutcome {
            hits: results,
            total_hits,
            discarded,
            approximate_sort,
            emptied,
        })
    }

    /// Apply multiple write operations atomically to a specific index
    ///
    /// This function provides guaranteed batch write with supervised smart commits:
    /// 1. Single atomic redb transaction for all data operations
    /// 2. Single atomic tantivy writer commit for all index operations  
    /// 3. Predictable smart commit logic based on operation thresholds
    /// 4. Guaranteed document searchability after successful commit
    ///
    /// Returns (sequence_ids, new_documents_count)
    pub fn apply_batch(
        &self,
        index: &str,
        ops: Vec<WalOp>,
    ) -> Result<(Vec<u64>, usize), StoreError> {
        let ops_len = ops.len();
        if ops.is_empty() {
            return Ok((Vec::new(), 0));
        }

        tracing::debug!(
            index = %index,
            ops_count = ops.len(),
            "HybridStore: Starting apply_batch"
        );

        // See the guard in `apply_write`. A batch carrying even one put may create the index,
        // because that put is a caller asking for it; a batch of nothing but deletes may not.
        if ops.iter().all(|op| matches!(op, WalOp::Delete { .. })) && !self.index_exists(index) {
            return Err(StoreError::IndexNotFound(index.to_string()));
        }

        // Get or create the index
        let (writer_arc, fields) = self.get_or_create_index(index)?;

        // Get schema for shadow field filtering
        let schema = if let Some(schema) = self.get_schema_cached(index)? {
            schema
        } else {
            self.get_schema(index)?
                .map(Arc::new)
                .unwrap_or_else(|| Arc::new(IndexSchema::default()))
        };

        // Reserve a contiguous block of sequence IDs atomically.
        // fetch_add returns the previous value; +1 gives the first usable seq.
        let start_seq = {
            let counter = self.current_seq.get(index).ok_or_else(|| {
                StoreError::IndexNotFound(format!(
                    "Sequence counter not found for index: {}",
                    index
                ))
            })?;
            counter.fetch_add(ops_len as u64, Ordering::SeqCst) + 1
        };
        let seq_ids_iter = (0..ops_len).map(move |i| start_seq + i as u64);

        let data_table_name = format!("data_{}", index);
        let wal_table_name = format!("wal_{}", index);
        let data_table_def = TableDefinition::<&str, &[u8]>::new(&data_table_name);
        let wal_table_def = TableDefinition::<u64, &[u8]>::new(&wal_table_name);

        #[derive(Debug)]
        enum PreparedKind {
            Put { json_blob: Option<JsonValue> },
            Delete,
        }

        #[derive(Debug)]
        struct PreparedOp {
            wal_bytes: Vec<u8>,
            doc_bytes: Option<Vec<u8>>,
            id: String,
            kind: PreparedKind,
        }

        let has_shadow_fields = schema.has_shadow_fields();

        // Step 1: Serialize outside of lock (CPU work)
        let mut prepared_ops = Vec::with_capacity(ops_len);
        for op in ops {
            match op {
                WalOp::Put { id, json_blob } => {
                    let filtered_json_blob = if has_shadow_fields {
                        json_blob.map(|blob| filter_shadow_fields_owned(blob, &schema))
                    } else {
                        json_blob
                    };

                    // Id only: the document goes to `data_<index>` in the same transaction,
                    // which is where recovery reads it from.
                    let wal_bytes = encode_wal_entry(&id);

                    let doc_bytes = serde_json::to_vec(&StoredDoc {
                        json_blob: filtered_json_blob.as_ref(),
                    })
                    .map_err(|e| StoreError::Serialization(e.to_string()))?;

                    prepared_ops.push(PreparedOp {
                        wal_bytes,
                        doc_bytes: Some(doc_bytes),
                        id,
                        kind: PreparedKind::Put {
                            json_blob: filtered_json_blob,
                        },
                    });
                }
                WalOp::Delete { id } => {
                    let wal_bytes = encode_wal_entry(&id);

                    prepared_ops.push(PreparedOp {
                        wal_bytes,
                        doc_bytes: None,
                        id,
                        kind: PreparedKind::Delete,
                    });
                }
            }
        }

        // Single transaction for all operations
        let mut write_txn = self.kv.begin_write()?;
        let mut tantivy_ops = Vec::new();
        let batch_size = ops_len as u64;

        // Collect sequence IDs during processing
        let mut seq_ids = Vec::with_capacity(ops_len);
        let mut new_documents_count = 0usize;
        let mut updated_document_ids = Vec::new(); // Track updated documents for selective Tantivy deletes

        {
            // Set durability based on config for bulk operations
            let durability = if self.config.wal_sync {
                Durability::Immediate
            } else {
                Durability::None
            };
            write_txn.set_durability(durability)?;
            tracing::trace!(index = %index, batch_size = batch_size, durability = ?durability, "Bulk data transaction durability set (user data)");

            let mut wal_table = write_txn.open_table(wal_table_def)?;
            let mut data_table = write_txn.open_table(data_table_def)?;

            // Process operations and collect sequence IDs
            for (prepared, seq_id) in prepared_ops.into_iter().zip(seq_ids_iter) {
                let PreparedOp {
                    wal_bytes,
                    doc_bytes,
                    id,
                    kind,
                } = prepared;

                // Write to WAL
                wal_table.insert(seq_id, wal_bytes.as_slice())?;

                // Collect sequence ID for final result
                seq_ids.push(seq_id);

                match kind {
                    PreparedKind::Put { json_blob } => {
                        if let Some(doc_bytes) = doc_bytes {
                            // Check if document is new or updated by examining insert return value
                            let old_value = data_table.insert(id.as_str(), doc_bytes.as_slice())?;
                            let is_new_document = old_value.is_none();

                            if is_new_document {
                                new_documents_count += 1;
                            } else {
                                // Track updated documents for selective Tantivy deletes
                                updated_document_ids.push(id.clone());
                            }
                        }

                        // Step 3: Build tantivy document with ONLY indexed fields
                        let mut tantivy_doc = doc!(fields.id => id.as_str());
                        if let Some(seq_field) = fields.seq {
                            tantivy_doc.add_u64(seq_field, seq_id);
                        }

                        // Step 4: Single-pass JSON traversal — skip shadows + extract Tantivy fields
                        if let Some(json_obj) = json_blob.as_ref().and_then(|v| v.as_object()) {
                            for (field_name, field_value) in json_obj {
                                // O(1) shadow field skip via pre-computed HashSet
                                if has_shadow_fields && schema.shadow_fields.contains(field_name) {
                                    continue;
                                }

                                // Look up schema field def + Tantivy field in one go
                                let field_def = match schema.fields.get(field_name) {
                                    Some(fd) if fd.indexed => fd,
                                    _ => continue,
                                };
                                let tantivy_field = match fields.indexed_fields.get(field_name) {
                                    Some(tf) => tf,
                                    None => continue,
                                };

                                add_json_value_to_doc(
                                    &mut tantivy_doc,
                                    *tantivy_field,
                                    field_name,
                                    &field_def.field_type,
                                    field_value,
                                    BadFacet::Refuse,
                                )?;
                            }
                        }

                        tantivy_ops.push(("add", tantivy_doc, id));
                    }
                    PreparedKind::Delete => {
                        data_table.remove(id.as_str())?;
                        tantivy_ops.push(("delete", doc!(), id));
                    }
                }
            }
        }

        write_txn.commit()?;

        // Every id in this batch had its row written or removed, so any cached body for it is
        // the previous one. Done after the commit and before the Tantivy work, with the ids
        // still borrowed out of `tantivy_ops` rather than collected into a second vector.
        self.invalidate_read_cache(index, tantivy_ops.iter().map(|(_, _, id)| id.as_str()));

        // Apply all tantivy operations with optimized selective deletes
        {
            let writer = writer_arc.lock().unwrap_or_else(|poisoned| {
                tracing::error!(index = %index, "Writer mutex was poisoned, recovering");
                poisoned.into_inner()
            });

            // Step 1: Delete only updated documents (selective optimization)
            if !updated_document_ids.is_empty() {
                tracing::debug!(
                    updated_count = updated_document_ids.len(),
                    "Selective Tantivy deletes for updated documents"
                );
                for updated_id in &updated_document_ids {
                    let term = tantivy::Term::from_field_text(fields.id, updated_id);
                    writer.delete_term(term);
                }
            }

            // Step 2: Add all documents (new + updated)
            // Note: Updated documents already deleted in Step 1, new documents don't need deletion
            for (op_type, tantivy_doc, _id) in tantivy_ops {
                match op_type {
                    "add" => {
                        writer.add_document(tantivy_doc)?;
                    }
                    "delete" => {
                        // Handle explicit delete operations (not from updates)
                        let term = tantivy::Term::from_field_text(fields.id, &_id);
                        writer.delete_term(term);
                    }
                    _ => unreachable!(),
                }
            }

            // Increment operations counter by batch size for threshold tracking.
            // The actual commit decision is made by apply_batch_and_maybe_commit()
            // which calls maybe_commit_writer() after this function returns.
            self.operations_counter
                .entry(index.to_string())
                .or_insert_with(|| AtomicU64::new(0))
                .value()
                .fetch_add(batch_size, Ordering::SeqCst);

            tracing::debug!(
                index = %index,
                batch_size = batch_size,
                new_docs = new_documents_count,
                updated_docs = updated_document_ids.len(),
                "Bulk write completed"
            );

            // Explicit drop of writer to ensure lock release before leaving scope
            drop(writer);
        }

        // Invalidate size cache for this index to ensure fresh stats on next query.
        //
        // Unconditional, where this used to ask for a new or updated document first: a batch of
        // pure deletes satisfies neither test and still changes every figure in there. An empty
        // batch cannot reach this point — `ops.is_empty()` returned at the top.
        {
            let mut size_cache = self.index_size_cache.lock().unwrap();
            size_cache.retain(|key, _| !key.contains(&format!(":{}", index)));
        }

        tracing::debug!(
            index = %index,
            seq_count = seq_ids.len(),
            new_docs = new_documents_count,
            "HybridStore: apply_batch completed successfully"
        );

        Ok((seq_ids, new_documents_count))
    }

    /// Get adaptive sample count based on table size.
    /// Larger tables get more samples for better statistical accuracy,
    /// while maintaining O(1) fixed cost (not O(N)).
    fn get_adaptive_sample_count(table_count: u64) -> u64 {
        match table_count {
            0..=200 => table_count,     // Exact for tiny tables
            201..=10_000 => 200,        // 200 samples for small tables
            10_001..=100_000 => 300,    // 300 samples for medium tables
            100_001..=1_000_000 => 400, // 400 samples for large tables
            _ => 500,                   // 500 samples for huge tables (millions+)
        }
    }

    /// Calculate table size using Hybrid Exact/Sampling Estimation algorithm
    ///
    /// Uses adaptive sampling: larger tables get more samples for better accuracy.
    /// - Tiny tables (≤200): Exact calculation by iterating all records
    /// - Small tables (≤10K): 200 samples
    /// - Medium tables (≤100K): 300 samples
    /// - Large tables (≤1M): 400 samples
    /// - Huge tables (>1M): 500 samples
    ///
    /// Returns (raw_size, is_estimated) where raw_size is the calculated/estimated size
    /// and is_estimated indicates whether sampling was used
    fn calculate_table_size_estimated(
        &self,
        table: &redb::ReadOnlyTable<&str, &[u8]>,
    ) -> Result<(u64, bool), StoreError> {
        let count = table.len()?;
        let sample_count = Self::get_adaptive_sample_count(count);

        if count <= sample_count {
            // Exact calculation for small tables
            let mut total_size = 0u64;
            for result in table.iter()? {
                let (key, value): (redb::AccessGuard<&str>, redb::AccessGuard<&[u8]>) = result?;
                total_size += key.value().len() as u64 + value.value().len() as u64;
            }
            Ok((total_size, false))
        } else {
            // Sample-based estimation for large tables
            let mut sample_size = 0u64;
            let mut actual_samples = 0u64;

            for result in table.iter()?.take(sample_count as usize) {
                let (key, value): (redb::AccessGuard<&str>, redb::AccessGuard<&[u8]>) = result?;
                sample_size += key.value().len() as u64 + value.value().len() as u64;
                actual_samples += 1;
            }

            let average_row_size = if actual_samples > 0 {
                sample_size as f64 / actual_samples as f64
            } else {
                0.0
            };

            let estimated_raw_size = (average_row_size * count as f64) as u64;

            tracing::trace!(
                table_count = count,
                sample_count = actual_samples,
                avg_row_size = average_row_size as u64,
                estimated_size = estimated_raw_size,
                "Adaptive sampling used for table size estimation"
            );

            Ok((estimated_raw_size, true))
        }
    }

    /// Gather per-index statistics and timing information for this shard.
    ///
    /// PERFORMANCE: This function now uses batch measurement to avoid N² complexity.
    /// All indexes are measured once in a single pass, reusing a single transaction.
    pub fn gather_index_stats(
        &self,
        include_data_size: bool,
    ) -> Result<ShardStatsSnapshot, StoreError> {
        let mut per_index = HashMap::new();

        let mut index_names: HashSet<String> = HashSet::new();
        let redb_phase_start = Instant::now();
        let read_txn = self.kv.begin_read()?;

        if let Ok(schema_table) = read_txn.open_table(TABLE_SCHEMA) {
            for result in schema_table.iter()? {
                let (index_name, _) = result?;
                index_names.insert(index_name.value().to_string());
            }
        }

        let indices_dir = self.config.shard_path.join("indices");
        if indices_dir.exists() {
            for entry in fs::read_dir(&indices_dir)? {
                let entry = entry?;
                if entry.file_type()?.is_dir() {
                    index_names.insert(entry.file_name().to_string_lossy().to_string());
                }
            }
        }

        // Batch measure all indexes once (eliminates N² pattern)
        let all_sizes =
            self.batch_measure_all_indexes(&index_names, &read_txn, include_data_size)?;

        // Build result from batch measurements
        for index_name in &index_names {
            if let Some(sizes) = all_sizes.get(index_name) {
                // Check if Tantivy index directory exists (not just if it has size)
                // This ensures empty indexes (after schema creation) are counted
                let tantivy_index_exists = self
                    .index_dir(index_name)
                    .map(|p| p.join("meta.json").exists())
                    .unwrap_or(false);

                per_index.insert(
                    index_name.clone(),
                    IndexShardStats {
                        document_count: sizes.document_count,
                        redb_bytes: sizes.redb_bytes,
                        tantivy_bytes: sizes.tantivy_bytes,
                        tantivy_index_exists,
                        tantivy_scan_ms: 0,
                        warmup_state: self
                            .warmup_states
                            .get(index_name)
                            .map(|state| *state.value())
                            .unwrap_or(IndexWarmupState::Cold),
                        searchable_fields: self.searchable_fields(index_name),
                        sortable_fields: self.sortable_fields(index_name),
                    },
                );
            }
        }
        let redb_duration = redb_phase_start.elapsed();
        drop(read_txn);

        Ok(ShardStatsSnapshot {
            per_index,
            timings: ShardStatsTimings {
                redb_ms: redb_duration.as_millis(),
                tantivy_ms: 0, // Included in redb calculation now
                total_ms: redb_duration.as_millis(),
            },
        })
    }

    /// Field names the built Tantivy index has a column for.
    ///
    /// Answers "can a query reach this field *now*", which the schema alone cannot: `indexed`
    /// there is a declaration, and a field declared after the index was built has no column until
    /// the data is rebuilt. See [`IndexShardStats::searchable_fields`].
    ///
    /// Free when the index has been touched — the field handles are already cached — and one
    /// `meta.json` read when it has not. Returns empty rather than erroring for an index with no
    /// built directory, since that is a normal state and not a failure to report.
    /// Both paths report the same set: every column except `_seq`, which is WAL bookkeeping and
    /// not something a caller queries. `id` *is* included — `id:value` is answerable, and is in
    /// fact the one lookup served without touching the search index at all — even though
    /// [`SchemaFields::indexed_fields`] omits it, since that map exists to drive document
    /// building where `id` is handled separately.
    pub fn searchable_fields(&self, index: &str) -> HashSet<String> {
        if let Some(fields) = self.fields_cache.get(index) {
            let mut names: HashSet<String> = fields.indexed_fields.keys().cloned().collect();
            names.insert("id".to_string());
            return names;
        }

        let Ok(index_path) = self.index_dir(index) else {
            return HashSet::new();
        };
        if !index_path.join("meta.json").exists() {
            return HashSet::new();
        }

        // Opening reads `meta.json` for the schema; it does not take the writer lockfile, so this
        // is safe against a live writer.
        match open_tantivy_index(&index_path) {
            Ok(opened) => opened
                .schema()
                .fields()
                .map(|(_, entry)| entry.name().to_string())
                .filter(|name| name != "_seq")
                .collect(),
            Err(err) => {
                tracing::debug!(index = %index, error = %err, "Could not read searchable fields");
                HashSet::new()
            }
        }
    }

    /// Field names the built Tantivy index has a *fast column* for, and can therefore sort
    /// exactly.
    ///
    /// The same distinction [`Self::searchable_fields`] draws, one property along: `fast` in the
    /// schema is a declaration, the column is written at index time from that declaration, and
    /// the two part company for any field declared after the index was built. A caller that
    /// reads the declaration and sorts on it gets an answer — an approximate one for a text
    /// field, an error for a numeric one — so the declaration alone cannot answer "will a sort
    /// on this field be exact".
    ///
    /// Read from the built index rather than from the cached field handles, because
    /// [`SchemaFields`] records which fields exist and not which carry a column. Empty for an
    /// index with no built directory, as above.
    pub fn sortable_fields(&self, index: &str) -> HashSet<String> {
        let Ok(index_path) = self.index_dir(index) else {
            return HashSet::new();
        };
        if !index_path.join("meta.json").exists() {
            return HashSet::new();
        }

        match open_tantivy_index(&index_path) {
            Ok(opened) => opened
                .schema()
                .fields()
                .filter(|(_, entry)| entry.is_fast())
                .map(|(_, entry)| entry.name().to_string())
                .filter(|name| name != "_seq")
                .collect(),
            Err(err) => {
                tracing::debug!(index = %index, error = %err, "Could not read sortable fields");
                HashSet::new()
            }
        }
    }

    /// Get list of index names from redb schema table only
    pub fn get_index_names(&self) -> Result<Vec<String>, StoreError> {
        let mut index_names = Vec::new();

        let read_txn = self.kv.begin_read()?;

        // Only check redb schema table - no filesystem access, no Tantivy loading
        match read_txn.open_table(TABLE_SCHEMA) {
            Ok(schema_table) => {
                for result in schema_table.iter()? {
                    let (index_name, _) = result?;
                    index_names.push(index_name.value().to_string());
                }
            }
            Err(_) => {
                // Schema table doesn't exist yet - return empty list
            }
        }

        Ok(index_names)
    }

    /// Phase 1 of startup: replay the WAL tail of every index that has one.
    ///
    /// redb and Tantivy have each finished their own recovery before this runs — redb by
    /// pointing back at its last commit root, Tantivy by opening the segments its last commit
    /// published. Neither costs time proportional to the data it holds. All that is left is
    /// the gap between them, and this closes it.
    ///
    /// Finding the indices in that gap costs one redb read transaction for the whole shard.
    /// A commit deletes the WAL entries it covers, so a non-empty `wal_<index>` is exactly
    /// the set of writes Tantivy may be missing, and an empty one means the index is in sync:
    /// no Tantivy open, no metadata lookup, no searcher. That is what keeps boot bounded by
    /// how much was in flight when the process stopped rather than by how much data the node
    /// holds — an idle 30 TB index is one B-tree descent, the same as an empty one.
    ///
    /// Returns the plan for phase 2. Recovery and warmup are deliberately split: recovery
    /// is a correctness requirement and blocks, warmup is a latency optimization and does
    /// not. See [`HybridStore::warm_index`] for phase 2.
    pub fn recover_indices(&self) -> Result<WarmupPlan, StoreError> {
        let start = Instant::now();
        let index_names = self.get_index_names()?;

        if index_names.is_empty() {
            tracing::debug!("No indices to recover");
            return Ok(WarmupPlan::default());
        }

        // One read transaction for the whole partition. Per-index transactions were a long
        // tail of small redb operations at high index counts, and there is nothing to gain
        // from them: this is a point-in-time question, and a single snapshot answers it for
        // every index at once.
        let mut needs_recovery = Vec::new();
        {
            let read_txn = self.kv.begin_read()?;
            for index_name in &index_names {
                let wal_table_name = format!("wal_{}", index_name);
                let wal_table_def = TableDefinition::<u64, &[u8]>::new(&wal_table_name);
                let has_tail = match read_txn.open_table(wal_table_def) {
                    // `last()` descends to the rightmost leaf; it does not scan the table.
                    Ok(table) => table.last()?.is_some(),
                    // No table at all: the index has never been written to.
                    Err(_) => false,
                };

                if has_tail {
                    self.warmup_states
                        .insert(index_name.clone(), IndexWarmupState::Recovering);
                    needs_recovery.push(index_name.clone());
                } else {
                    self.warmup_states
                        .insert(index_name.clone(), IndexWarmupState::Cold);
                }
            }
        }

        tracing::info!(
            total = index_names.len(),
            needs_recovery = needs_recovery.len(),
            partition_ms = start.elapsed().as_millis(),
            "Phase 1: replaying the WAL tail of indices redb committed past Tantivy"
        );

        let mut recovered = Vec::new();
        let mut failed = Vec::new();

        if !needs_recovery.is_empty() {
            let results = std::sync::Mutex::new(Vec::new());

            // Every index that needs replay gets a thread, and `RECOVERY_GATE` decides how
            // many of them hold an `IndexWriter` at once. The threads are the cheap part; the
            // arenas are what has to be rationed, and rationing them here rather than by
            // chunking means a slow replay does not hold back the rest of its chunk.
            std::thread::scope(|scope| {
                let handles: Vec<_> = needs_recovery
                    .iter()
                    .map(|index_name| {
                        let results = &results;
                        scope.spawn(move || {
                            let _permit = RECOVERY_GATE.acquire();
                            // get_or_create_index runs recover_index as a side effect.
                            let outcome = self.get_or_create_index(index_name);
                            results
                                .lock()
                                .unwrap()
                                .push((index_name.clone(), outcome.is_ok()));
                            if let Err(e) = outcome {
                                tracing::warn!(
                                    index = %index_name,
                                    error = %e,
                                    "Recovery failed, index will retry on first access"
                                );
                            }
                        })
                    })
                    .collect();

                for handle in handles {
                    let _ = handle.join();
                }
            });

            for (index_name, ok) in results.into_inner().unwrap() {
                if ok {
                    // Recovered, but the reader is still cold — phase 2 warms it.
                    self.warmup_states
                        .insert(index_name.clone(), IndexWarmupState::Cold);
                    recovered.push(index_name);
                } else {
                    self.warmup_states
                        .insert(index_name.clone(), IndexWarmupState::Failed);
                    failed.push(index_name);
                }
            }
        }

        // Phase 2 covers every index, recovered or not: recovery populates the *writer*
        // cache, which queries never touch. Order smallest-first so the greatest number of
        // indices become warm soonest — a large index left for last still answers queries,
        // it just pays its own cold cost once.
        let mut pending_warmup: Vec<String> = index_names
            .iter()
            .filter(|name| !failed.contains(name))
            .cloned()
            .collect();
        pending_warmup.sort_by_key(|name| {
            self.index_dir(name)
                .ok()
                .and_then(|path| index_size_bytes(&path))
                .unwrap_or(0)
        });

        let plan = WarmupPlan {
            recovered,
            failed,
            pending_warmup,
        };

        tracing::info!(
            total = index_names.len(),
            recovered = plan.recovered.len(),
            failed = plan.failed.len(),
            pending_warmup = plan.pending_warmup.len(),
            elapsed_ms = start.elapsed().as_millis(),
            "Phase 1 complete: all indices are queryable"
        );

        Ok(plan)
    }

    /// Phase 2 of startup: warm the readers for `indices`, in the order given.
    ///
    /// Runs on the calling thread — callers put it on a background thread so it never delays
    /// serving. Individual failures are logged and skipped: a failed warmup costs latency on
    /// the first query, not correctness.
    ///
    /// Returns the number of indices warmed.
    pub fn warm_indices(&self, indices: &[String]) -> usize {
        if indices.is_empty() {
            return 0;
        }

        let start = Instant::now();
        let mut warmed = 0usize;
        let mut total_segments = 0usize;
        let mut total_docs = 0u64;
        let mut skipped = 0usize;

        for (position, index) in indices.iter().enumerate() {
            // Warming faults term dictionaries in from mmap, so on a multi-terabyte shard it
            // is sustained random IO — and every shard on the node is doing it at the same
            // time. Past this budget the storm costs the queries that are already arriving
            // more than it saves the ones that have not. Whatever is left warms on first
            // access through the same path, which is where an index nobody queries should
            // have been paying anyway.
            if start.elapsed() >= WARMUP_BUDGET {
                skipped = indices.len() - position;
                break;
            }

            match self.warm_index(index) {
                Ok(Some(stats)) => {
                    warmed += 1;
                    total_segments += stats.segments;
                    total_docs += stats.num_docs;
                }
                Ok(None) => {
                    // Schema exists but nothing was ever written; nothing to warm.
                    warmed += 1;
                }
                Err(e) => {
                    tracing::warn!(
                        index = %index,
                        error = %e,
                        "Warmup failed; first query for this index will pay the cold cost"
                    );
                }
            }
        }

        if skipped > 0 {
            tracing::warn!(
                requested = indices.len(),
                warmed = warmed,
                skipped = skipped,
                budget_secs = WARMUP_BUDGET.as_secs(),
                "Phase 2 hit its time budget; the remaining indices will warm on first query"
            );
        }

        tracing::info!(
            requested = indices.len(),
            warmed = warmed,
            skipped = skipped,
            segments = total_segments,
            documents = total_docs,
            elapsed_ms = start.elapsed().as_millis(),
            "Phase 2 complete: index readers warmed"
        );

        warmed
    }

    fn measure_tantivy_bytes(&self, index_name: &str) -> Result<u64, StoreError> {
        let index_dir = self.index_dir(index_name)?;
        if !index_dir.exists() {
            return Ok(0);
        }

        let mut total_size = 0u64;
        for entry in WalkDir::new(&index_dir)
            .follow_links(false)
            .into_iter()
            .filter_map(Result::ok)
        {
            if let Ok(metadata) = entry.metadata()
                && metadata.is_file()
                && Self::is_tantivy_data_file(entry.path())
            {
                total_size += metadata.len();
            }
        }

        Ok(total_size)
    }

    /// Measure redb stats using an existing transaction (avoids opening new transaction).
    /// This is more efficient when measuring multiple indexes.
    fn measure_redb_stats_with_txn(
        &self,
        index_name: &str,
        read_txn: &redb::ReadTransaction,
    ) -> Result<(u64, u64), StoreError> {
        let data_table_name = format!("data_{}", index_name);
        let data_table_def = TableDefinition::<&str, &[u8]>::new(&data_table_name);

        let (doc_count, raw_bytes) = match read_txn.open_table(data_table_def) {
            Ok(data_table) => {
                let doc_count = data_table.len().unwrap_or(0);
                let (raw_size, _) = self.calculate_table_size_estimated(&data_table)?;
                (doc_count, raw_size)
            }
            Err(_) => (0, 0),
        };

        Ok((doc_count, raw_bytes))
    }

    /// Get document count from Tantivy index (O(1) operation).
    /// This is faster than querying redb when we don't need size calculation.
    fn get_document_count_from_tantivy(&self, index_name: &str) -> Result<u64, StoreError> {
        // get_reader owns the reader cache: it serves the cached reader when there is one and
        // otherwise opens the index and caches it. The previous implementation fell back to
        // get_or_create_index, which only populates the *writer* cache, so the follow-up
        // reader lookup always missed and this reported 0 documents for any index that had
        // not been searched yet.
        match self.get_reader(index_name) {
            Ok(Some((reader, _fields))) => Ok(reader.searcher().num_docs()),
            Ok(None) => Ok(0), // Index has no Tantivy directory yet
            Err(e) => {
                tracing::debug!(index = %index_name, error = %e, "Failed to open reader for document count");
                Ok(0)
            }
        }
    }

    /// Batch measure all indexes in a single pass with shared transaction.
    /// This eliminates the N² complexity of the old approach where get_index_sizes_cached
    /// was called once per index, and each call measured ALL indexes.
    ///
    /// Returns a HashMap of index_name -> IndexSizes for all indexes.
    fn batch_measure_all_indexes(
        &self,
        index_names: &HashSet<String>,
        read_txn: &redb::ReadTransaction,
        include_data_size: bool,
    ) -> Result<HashMap<String, IndexSizes>, StoreError> {
        let mut results = HashMap::new();

        // Check cache first for all indexes
        let cache_suffix = if include_data_size { "full" } else { "fast" };
        let cache_key_prefix = format!("{}:{}:", self.config.shard_path.display(), cache_suffix);

        {
            let cache = self.index_size_cache.lock().unwrap();
            for index_name in index_names {
                let cache_key = format!("{}{}", cache_key_prefix, index_name);
                if let Some(entry) = cache.get(&cache_key)
                    && entry.timestamp.elapsed() < self.index_cache_expiry
                {
                    results.insert(
                        index_name.clone(),
                        IndexSizes {
                            tantivy_bytes: entry.tantivy_bytes,
                            redb_bytes: entry.redb_bytes,
                            document_count: entry.document_count,
                        },
                    );
                }
            }
        }

        // If all cached, return early
        if results.len() == index_names.len() {
            tracing::debug!(
                shard = %self.config.shard_path.display(),
                cached_count = results.len(),
                "All index sizes retrieved from cache"
            );
            return Ok(results);
        }

        // Measure uncached indexes
        let mut per_index_stats = Vec::new();
        let mut total_raw_redb_size = 0u64;

        for idx_name in index_names {
            if results.contains_key(idx_name) {
                continue; // Skip cached
            }

            let tantivy_bytes = self.measure_tantivy_bytes(idx_name)?;

            let (doc_count, raw_redb_bytes) = if include_data_size {
                // When calculating data size, get count from redb for consistency
                self.measure_redb_stats_with_txn(idx_name, read_txn)?
            } else {
                // When skipping data size, use Tantivy count (faster, no redb access)
                let doc_count = self.get_document_count_from_tantivy(idx_name)?;
                (doc_count, 0)
            };

            per_index_stats.push((idx_name.clone(), tantivy_bytes, doc_count, raw_redb_bytes));
            total_raw_redb_size = total_raw_redb_size.saturating_add(raw_redb_bytes);
        }

        tracing::debug!(
            shard = %self.config.shard_path.display(),
            uncached_count = per_index_stats.len(),
            cached_count = results.len(),
            "Measured uncached indexes"
        );

        // Calculate correction factor (only when include_data_size is true)
        let correction_factor = if include_data_size && total_raw_redb_size > 0 {
            let physical_db_size =
                match std::fs::metadata(self.config.shard_path.join("store.redb")) {
                    Ok(metadata) => metadata.len(),
                    Err(e) => {
                        tracing::warn!(
                            error = %e,
                            "Failed to get database file size, using raw estimation"
                        );
                        total_raw_redb_size
                    }
                };
            physical_db_size as f64 / total_raw_redb_size as f64
        } else {
            1.0
        };

        // Cache and build results for uncached indexes
        // OPTIMIZATION: Populate BOTH fast and full cache entries to enable cache sharing
        {
            let mut cache = self.index_size_cache.lock().unwrap();
            let shard_path = self.config.shard_path.display().to_string();

            for (idx_name, tantivy_bytes, doc_count, raw_redb_bytes) in per_index_stats {
                let corrected_redb_bytes = if include_data_size {
                    (raw_redb_bytes as f64 * correction_factor) as u64
                } else {
                    0
                };

                // Always cache the "fast" entry (tantivy bytes + doc count, no redb size)
                let fast_key = format!("{}:fast:{}", shard_path, idx_name);
                cache.insert(
                    fast_key,
                    IndexSizeCache {
                        tantivy_bytes,
                        redb_bytes: 0,
                        document_count: doc_count,
                        timestamp: Instant::now(),
                    },
                );

                // When we have redb data, also cache the "full" entry
                if include_data_size {
                    let full_key = format!("{}:full:{}", shard_path, idx_name);
                    cache.insert(
                        full_key,
                        IndexSizeCache {
                            tantivy_bytes,
                            redb_bytes: corrected_redb_bytes,
                            document_count: doc_count,
                            timestamp: Instant::now(),
                        },
                    );
                }

                results.insert(
                    idx_name.clone(),
                    IndexSizes {
                        tantivy_bytes,
                        redb_bytes: corrected_redb_bytes,
                        document_count: doc_count,
                    },
                );
            }
        }

        Ok(results)
    }

    fn is_tantivy_data_file(path: &std::path::Path) -> bool {
        path.extension()
            .and_then(|ext| ext.to_str())
            .map(|ext| TANTIVY_DATA_FILE_EXTENSIONS.contains(&ext))
            .unwrap_or(false)
    }

    /// Get field names from actual documents in an index by sampling
    pub fn get_index_field_names(&self, index: &str) -> Result<Vec<String>, StoreError> {
        let data_table_name = format!("data_{}", index);
        let data_table_def = TableDefinition::<&str, &[u8]>::new(&data_table_name);

        let read_txn = self.kv.begin_read()?;
        let mut field_names = std::collections::HashSet::new();

        match read_txn.open_table(data_table_def) {
            Ok(data_table) => {
                const MAX_SAMPLES: usize = 100; // Sample up to 100 documents

                for (sample_count, result) in data_table.iter()?.enumerate() {
                    if sample_count >= MAX_SAMPLES {
                        break;
                    }

                    let (_, value) = result?;

                    // Parse the document JSON to extract field names
                    if let Ok(doc_data) = serde_json::from_slice::<JsonValue>(value.value()) {
                        if let Some(json_blob) = doc_data.get("json_blob")
                            && let Some(json_obj) = json_blob.as_object()
                        {
                            for field_name in json_obj.keys() {
                                field_names.insert(field_name.clone());
                            }
                        }

                        // Also check top-level fields in the document
                        if let Some(doc_obj) = doc_data.as_object() {
                            for field_name in doc_obj.keys() {
                                if field_name != "body" && field_name != "json_blob" {
                                    field_names.insert(field_name.clone());
                                }
                            }
                        }
                    }
                }
            }
            Err(_) => {
                // Table doesn't exist, return empty list
            }
        }

        let mut field_names_vec: Vec<String> = field_names.into_iter().collect();

        // Sort fields with "id" first, then alphabetically
        field_names_vec.sort_by(|a, b| {
            match (a.as_str(), b.as_str()) {
                ("id", "id") => std::cmp::Ordering::Equal,
                ("id", _) => std::cmp::Ordering::Less, // "id" comes first
                (_, "id") => std::cmp::Ordering::Greater, // "id" comes first
                (a, b) => a.cmp(b),                    // alphabetical for others
            }
        });

        Ok(field_names_vec)
    }
}

// Safe because all components are Send+Sync
unsafe impl Send for HybridStore {}
unsafe impl Sync for HybridStore {}

#[cfg(test)]
mod index_dir_tests {
    use super::{StoreError, resolve_index_dir};
    use std::path::Path;

    fn base() -> &'static Path {
        Path::new("/shard/indices")
    }

    #[test]
    fn accepts_plain_names() {
        assert_eq!(
            resolve_index_dir(base(), "docs").unwrap(),
            Path::new("/shard/indices/docs")
        );
        assert_eq!(
            resolve_index_dir(base(), "my-index_2.v1").unwrap(),
            Path::new("/shard/indices/my-index_2.v1")
        );
    }

    /// The guard must hold without touching the filesystem: these paths do not
    /// exist, which is exactly the case where a canonicalize-based check would
    /// silently pass and let `create_dir_all` escape the shard.
    #[test]
    fn rejects_traversal_and_separators() {
        for name in [
            "..",
            ".",
            "../etc",
            "../../etc/passwd",
            "a/b",
            "a/../../b",
            "/etc",
            "/etc/passwd",
            "",
            "./x",
        ] {
            let err =
                resolve_index_dir(base(), name).expect_err(&format!("'{}' must be rejected", name));
            assert!(
                matches!(err, StoreError::InvalidIndexName(_)),
                "'{}' produced the wrong error: {:?}",
                name,
                err
            );
        }
    }

    /// A rejected name must never yield a path outside the base, and an accepted
    /// one must always stay directly beneath it.
    #[test]
    fn accepted_names_stay_within_base() {
        for name in ["docs", "a.b", "x-1"] {
            let path = resolve_index_dir(base(), name).unwrap();
            assert_eq!(path.parent(), Some(base()));
            assert!(path.starts_with(base()));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    /// An id-only WAL entry round-trips, whatever the id looks like.
    ///
    /// The empty id and one beginning with `{` are the two that could collide with the legacy
    /// format if the tag byte were ever dropped in favour of sniffing the payload.
    #[test]
    fn a_wal_entry_round_trips_any_document_id() {
        for id in ["doc-1", "", "{not-json", "ünïcodé", "a b\tc", "\u{1F600}"] {
            let encoded = encode_wal_entry(id);
            assert_eq!(encoded[0], WAL_ENTRY_ID_ONLY, "entry must carry its tag");
            assert_eq!(
                decode_wal_entry(&encoded).expect("decode"),
                id,
                "id must survive the round trip"
            );
        }
    }

    /// A value that is not a facet path is refused, not handed to a constructor that panics.
    ///
    /// `Facet: From<&str>` unwraps `from_text`, so `add_facet(field, "electronics/phones")` panics
    /// — on a shard's writer thread, from a document body, and with `panic = "abort"` in the
    /// release profile that ends the process rather than the request. Nothing reaches it today
    /// because the orchestrator refuses every value against a declared facet field, which makes
    /// that refusal load-bearing by accident; this is the guard that means it does not have to be.
    ///
    /// Driven straight at the write paths, since the orchestrator is what currently stops a facet
    /// value getting this far and the point is what happens when it does.
    #[test]
    fn a_value_that_is_not_a_facet_path_is_refused_rather_than_fatal() {
        let temp_dir = TempDir::new().unwrap();
        let store = HybridStore::new(read_cache_config(&temp_dir), 1).unwrap();

        let mut schema = IndexSchema::default();
        schema.fields.insert(
            "id".to_string(),
            FieldDef::new("id".to_string(), TantivyFieldType::Text),
        );
        schema.fields.insert(
            "cat".to_string(),
            FieldDef::new("cat".to_string(), TantivyFieldType::Facet),
        );
        schema.normalize_after_deserialization();
        store.store_schema_and_cache("shop", &schema).unwrap();

        // Every shape `from_text` rejects: no leading slash, and empty.
        for bad in ["electronics/phones", ""] {
            let refused = store.apply_write(
                "shop",
                WalOp::Put {
                    id: "x".to_string(),
                    json_blob: Some(serde_json::json!({"id": "x", "cat": bad})),
                },
            );
            match refused {
                Err(StoreError::InvalidFieldValue { field, reason }) => {
                    assert_eq!(field, "cat", "the refusal names the field");
                    assert!(
                        reason.contains("facet path") && reason.contains("/electronics/phones"),
                        "and says what one looks like: {reason}"
                    );
                }
                other => panic!("{bad:?} should be refused as a bad value, got {other:?}"),
            }

            // The batch path builds its documents separately and needs its own guard.
            assert!(
                matches!(
                    store.apply_batch(
                        "shop",
                        vec![WalOp::Put {
                            id: "y".to_string(),
                            json_blob: Some(serde_json::json!({"id": "y", "cat": bad})),
                        }]
                    ),
                    Err(StoreError::InvalidFieldValue { .. })
                ),
                "the batch path must refuse {bad:?} too"
            );
        }

        // A real path is accepted on both, and the ancestors Tantivy indexes are what make a
        // parent match its descendants.
        store
            .apply_write(
                "shop",
                WalOp::Put {
                    id: "a".to_string(),
                    json_blob: Some(serde_json::json!({"id": "a", "cat": "/electronics/phones"})),
                },
            )
            .expect("a facet path is accepted");
        store
            .apply_batch(
                "shop",
                vec![WalOp::Put {
                    id: "b".to_string(),
                    json_blob: Some(
                        serde_json::json!({"id": "b", "cat": "/electronics/phones/cases"}),
                    ),
                }],
            )
            .expect("and in a batch");
        store.commit_index("shop").expect("commit");

        let parent = store
            .search_documents("shop", "cat:/electronics", 10, None)
            .expect("search the parent path");
        assert_eq!(
            parent.hits.len(),
            2,
            "a parent path matches both descendants: {:?}",
            parent.discarded
        );
    }

    /// A delete must not bring an index into existence.    /// A delete must not bring an index into existence.
    ///
    /// The write path opens through `get_or_create_index`, which creates an index when it is
    /// absent — right for a put, which is a caller asking for the index, and wrong for a delete,
    /// where an empty index and a Tantivy directory on disk would be the trace left by removing
    /// nothing. Asserted on both write paths, and on the batch rule that a put in the same batch
    /// still creates.
    #[test]
    fn deleting_from_an_unknown_index_creates_nothing() {
        let temp_dir = TempDir::new().unwrap();
        let store = HybridStore::new(read_cache_config(&temp_dir), 1).unwrap();
        let indices = temp_dir.path().join("indices");

        for op in [
            WalOp::Delete {
                id: "d1".to_string(),
            },
            WalOp::Delete {
                id: "d2".to_string(),
            },
        ] {
            let refused = store.apply_write("ghost", op);
            assert!(
                matches!(refused, Err(StoreError::IndexNotFound(_))),
                "a delete against an unknown index must be refused: {refused:?}"
            );
        }
        assert!(
            matches!(
                store.apply_batch(
                    "ghost",
                    vec![WalOp::Delete {
                        id: "d1".to_string()
                    }]
                ),
                Err(StoreError::IndexNotFound(_))
            ),
            "and so must a batch of nothing but deletes"
        );
        assert!(
            !indices.join("ghost").exists(),
            "nothing may be created on disk for an index that was never created"
        );
        assert!(!store.index_exists("ghost"));

        // A created index accepts a delete for a document it does not hold: the id is absent,
        // which is not the same as the index being absent, and deletion is idempotent.
        store
            .store_schema_and_cache("real", &IndexSchema::default())
            .unwrap();
        assert!(store.index_exists("real"));
        store
            .apply_write(
                "real",
                WalOp::Delete {
                    id: "never-written".to_string(),
                },
            )
            .expect("a delete of an absent id is not an error");

        // A batch carrying a put still creates, because that put asked for the index.
        store
            .apply_batch(
                "fresh",
                vec![
                    WalOp::Put {
                        id: "d1".to_string(),
                        json_blob: Some(serde_json::json!({"id": "d1"})),
                    },
                    WalOp::Delete {
                        id: "d1".to_string(),
                    },
                ],
            )
            .expect("a batch with a put creates the index");
        assert!(indices.join("fresh").exists());
    }

    /// The read cache must not outlive the row it mirrors.
    ///
    /// Its only writer is a body hydration on the read path, and until 2026-08-26 its only
    /// invalidation was dropping an entire index. So a document read once and then updated kept
    /// serving its previous body, and a document read once and then deleted kept being served at
    /// all — which is fatal for deletion, since an `id:VALUE` lookup is answered from redb and
    /// never consults Tantivy. Both halves are asserted here on the single-write path and on the
    /// batch path, because each does its own invalidation.
    #[test]
    fn a_changed_row_is_not_served_from_the_read_cache() {
        let temp_dir = TempDir::new().unwrap();
        let store = HybridStore::new(read_cache_config(&temp_dir), 1).unwrap();

        let body = |index: &str, id: &str| -> Option<String> {
            store
                .get_by_key(index, id)
                .expect("read")
                .map(|bytes| String::from_utf8(bytes).expect("utf-8"))
        };

        // --- single-write path
        let single = "cache_single";
        store
            .apply_write(
                single,
                WalOp::Put {
                    id: "d1".to_string(),
                    json_blob: Some(serde_json::json!({"id": "d1", "title": "v1"})),
                },
            )
            .unwrap();
        assert!(
            body(single, "d1").expect("v1 present").contains("v1"),
            "the first read populates the cache"
        );

        store
            .apply_write(
                single,
                WalOp::Put {
                    id: "d1".to_string(),
                    json_blob: Some(serde_json::json!({"id": "d1", "title": "v2"})),
                },
            )
            .unwrap();
        let updated = body(single, "d1").expect("v2 present");
        assert!(
            updated.contains("v2") && !updated.contains("v1"),
            "an update must be visible, not shadowed by the cached body: {updated}"
        );

        store
            .apply_write(
                single,
                WalOp::Delete {
                    id: "d1".to_string(),
                },
            )
            .unwrap();
        assert_eq!(
            body(single, "d1"),
            None,
            "a deleted document must not be served from the cache"
        );
        assert!(
            store
                .get_batch_by_keys(single, &["d1".to_string()])
                .unwrap()
                .is_empty(),
            "the batch path hydrates search hits and must agree"
        );

        // --- batch path
        let batch = "cache_batch";
        store
            .apply_batch(
                batch,
                vec![
                    WalOp::Put {
                        id: "d1".to_string(),
                        json_blob: Some(serde_json::json!({"id": "d1", "title": "v1"})),
                    },
                    WalOp::Put {
                        id: "d2".to_string(),
                        json_blob: Some(serde_json::json!({"id": "d2", "title": "keep"})),
                    },
                ],
            )
            .unwrap();
        assert!(body(batch, "d1").is_some() && body(batch, "d2").is_some());

        store
            .apply_batch(
                batch,
                vec![
                    WalOp::Put {
                        id: "d1".to_string(),
                        json_blob: Some(serde_json::json!({"id": "d1", "title": "v2"})),
                    },
                    WalOp::Delete {
                        id: "d2".to_string(),
                    },
                ],
            )
            .unwrap();
        let updated = body(batch, "d1").expect("v2 present");
        assert!(
            updated.contains("v2") && !updated.contains("v1"),
            "a batched update must be visible: {updated}"
        );
        assert_eq!(body(batch, "d2"), None, "a batched delete must be visible");
    }

    /// A reader whose snapshot predates a write must not install that snapshot's body.
    ///
    /// Removing the entry is not sufficient on its own: the removal happens after the redb
    /// commit, and a reader that opened its transaction earlier legitimately still sees the
    /// pre-write row. If it caches that body after the removal, the staleness is back and
    /// nothing will remove it a second time. The generation is what makes such a reader decline,
    /// and the interleaving that needs it cannot be produced from a single thread — so this
    /// drives the two halves directly, in the order the race would put them.
    #[test]
    fn a_body_read_before_a_write_is_refused_by_the_cache() {
        let temp_dir = TempDir::new().unwrap();
        let store = HybridStore::new(read_cache_config(&temp_dir), 1).unwrap();
        let index = "cache_generation";

        store
            .apply_write(
                index,
                WalOp::Put {
                    id: "d1".to_string(),
                    json_blob: Some(serde_json::json!({"id": "d1", "title": "v1"})),
                },
            )
            .unwrap();

        // What a reader would have captured before opening its transaction.
        let seen_generation = store.cache_generation(index);

        // The write it is about to race, committed and invalidated.
        store
            .apply_write(
                index,
                WalOp::Delete {
                    id: "d1".to_string(),
                },
            )
            .unwrap();

        // The reader, arriving late with a body that was true when it looked.
        store.insert_into_cache(index, "d1", b"stale".to_vec(), seen_generation);

        assert_eq!(
            store.get_from_cache(index, "d1"),
            None,
            "a body read before the write must be refused, not installed"
        );

        // A reader that saw the current generation is still served by the cache, or the guard
        // would have turned the cache off rather than made it correct.
        let current = store.cache_generation(index);
        store.insert_into_cache(index, "d1", b"fresh".to_vec(), current);
        assert_eq!(
            store.get_from_cache(index, "d1").as_deref(),
            Some(&b"fresh"[..]),
            "a body read after the write must still be cacheable"
        );
    }

    fn read_cache_config(temp_dir: &TempDir) -> StorageConfig {
        StorageConfig {
            shard_path: temp_dir.path().to_path_buf(),
            indexer_memory_budget: 32 * 1024 * 1024,
            indexer_memory_min_mb: 16,
            indexer_memory_max_mb: 256,
            total_memory_limit_bytes: 2048 * 1024 * 1024,
            memory_pressure_threshold_percent: 80,
            indexer_num_threads: 1,
            merge_num_threads: 1,
            default_batch_size: 100_000,
            wal_sync: true,
        }
    }

    /// A WAL entry written by the previous build still decodes.
    ///
    /// Those entries are whole `WalOp` JSON values, and an upgrade can find a tail of them left
    /// behind by the process that died. Only the id is taken — the body they carry is ignored in
    /// favour of the `data_<index>` row — so one replay path serves both formats and no
    /// migration of the tail is needed.
    #[test]
    fn a_legacy_wal_entry_still_yields_its_document_id() {
        let legacy_put = serde_json::to_vec(&WalOp::Put {
            id: "doc-7".to_string(),
            json_blob: Some(serde_json::json!({ "title": "written by the old build" })),
        })
        .expect("serialize legacy put");
        assert_eq!(
            decode_wal_entry(&legacy_put).expect("decode legacy put"),
            "doc-7"
        );

        let legacy_delete = serde_json::to_vec(&WalOp::Delete {
            id: "doc-8".to_string(),
        })
        .expect("serialize legacy delete");
        assert_eq!(
            decode_wal_entry(&legacy_delete).expect("decode legacy delete"),
            "doc-8"
        );

        // Legacy entries are JSON objects, so they cannot be mistaken for the tagged format.
        assert_ne!(legacy_put[0], WAL_ENTRY_ID_ONLY);
        assert_ne!(legacy_delete[0], WAL_ENTRY_ID_ONLY);
    }

    /// An id-only entry is a fraction of the bytes the old format wrote.
    ///
    /// This is the point of the change: the document was already being written to
    /// `data_<index>` in the same transaction, so the copy in the WAL doubled what every write
    /// serialised and fsynced for nothing.
    #[test]
    fn a_wal_entry_no_longer_carries_the_document() {
        let body = serde_json::json!({
            "title": "a representative document",
            "body": "x".repeat(1024),
            "tags": ["alpha", "beta", "gamma"],
        });
        let legacy = serde_json::to_vec(&WalOp::Put {
            id: "doc-1".to_string(),
            json_blob: Some(body),
        })
        .expect("serialize legacy put");
        let current = encode_wal_entry("doc-1");

        assert_eq!(current.len(), 6, "one tag byte plus the id");
        assert!(
            current.len() * 50 < legacy.len(),
            "an id-only entry should be orders of magnitude smaller: {} vs {}",
            current.len(),
            legacy.len()
        );
    }

    /// A field type serializes under the name every other surface calls it.
    ///
    /// The derived implementation emitted the variant name, so a schema said `Date` while the
    /// syntax reference, the per-field query hints and the deserializer's own canonical list all
    /// said `date`. An agent reading the schema and then reading how to query it was given two
    /// spellings of one type.
    #[test]
    fn a_field_type_serializes_as_the_name_everything_else_uses() {
        for field_type in [
            TantivyFieldType::Text,
            TantivyFieldType::String,
            TantivyFieldType::I64,
            TantivyFieldType::U64,
            TantivyFieldType::F64,
            TantivyFieldType::Date,
            TantivyFieldType::Boolean,
            TantivyFieldType::Bytes,
            TantivyFieldType::Ip,
            TantivyFieldType::Json,
            TantivyFieldType::Facet,
        ] {
            let serialized = serde_json::to_value(&field_type).unwrap();
            assert_eq!(
                serialized,
                JsonValue::String(field_type.to_string().to_string()),
                "{field_type:?} should serialize as its canonical lowercase name"
            );

            let round_tripped: TantivyFieldType = serde_json::from_value(serialized).unwrap();
            assert_eq!(round_tripped, field_type, "{field_type:?} must round-trip");
        }
    }

    /// Schemas persisted before the change above still load.
    ///
    /// This is what makes it safe to change at all: deserialization lowercases before matching,
    /// so a redb table full of `"Date"` is read exactly as one full of `"date"`.
    #[test]
    fn a_schema_written_with_the_old_capitalized_names_still_loads() {
        for (stored, expected) in [
            ("\"Date\"", TantivyFieldType::Date),
            ("\"Text\"", TantivyFieldType::Text),
            ("\"Boolean\"", TantivyFieldType::Boolean),
            ("\"I64\"", TantivyFieldType::I64),
        ] {
            let parsed: TantivyFieldType = serde_json::from_str(stored).unwrap();
            assert_eq!(parsed, expected, "{stored} should still deserialize");
        }
    }

    /// Warming must actually fill the per-field caches, and must skip a generation it has
    /// already warmed.
    ///
    /// Both halves are invisible to black-box tests: a `warm_index` that silently did nothing
    /// would leave every query correct but cold, and a missing generation guard would re-warm
    /// an idle index on every request. This inspects the store's own bookkeeping and tantivy's
    /// per-segment cache to pin down both.
    #[test]
    fn warm_index_fills_caches_and_skips_warm_generations() {
        let temp_dir = TempDir::new().unwrap();
        let config = StorageConfig {
            shard_path: temp_dir.path().to_path_buf(),
            indexer_memory_budget: 32 * 1024 * 1024,
            indexer_memory_min_mb: 16,
            indexer_memory_max_mb: 256,
            total_memory_limit_bytes: 2048 * 1024 * 1024,
            memory_pressure_threshold_percent: 80,
            indexer_num_threads: 1,
            merge_num_threads: 1,
            default_batch_size: 100_000,
            wal_sync: true,
        };

        let store = HybridStore::new(config, 1).unwrap();
        let index = "warm_wiring";
        store
            .store_schema_and_cache(index, &IndexSchema::default())
            .unwrap();

        store
            .apply_write(
                index,
                WalOp::Put {
                    id: "doc-1".to_string(),
                    json_blob: Some(serde_json::json!({ "title": "first" })),
                },
            )
            .unwrap();
        store.commit_index(index).unwrap();

        let first = store.warm_index(index).unwrap().expect("stats");
        assert!(first.segments > 0, "committed data must produce a segment");
        assert_eq!(
            first.segments_warmed, first.segments,
            "the first warm must warm every segment"
        );

        // Nothing reloads a reader except `commit_index`, so the generation cannot move
        // under us here and a second warm must do no work.
        let second = store.warm_index(index).unwrap().expect("stats");
        assert_eq!(
            second.generation, first.generation,
            "no commit means no reload, so no new generation"
        );
        assert_eq!(
            second.segments_warmed, 0,
            "re-warming an unchanged generation must be a no-op"
        );

        // Prove the caches are actually populated rather than merely reported as warm: a
        // warmed segment answers inverted_index() from its cache. Comparing against a
        // freshly opened SegmentReader for the same segment shows the difference.
        let (reader, _) = store.get_reader(index).unwrap().unwrap();
        let searcher = reader.searcher();
        let segment_reader = &searcher.segment_readers()[0];
        let id_field = segment_reader.schema().get_field("id").unwrap();
        assert!(
            segment_reader.inverted_index(id_field).is_ok(),
            "warmed segment should resolve its inverted index"
        );

        // A new commit publishes a new generation, which must be warmed again — the per-field
        // caches live on SegmentReaders that tantivy rebuilds on every reload.
        store
            .apply_write(
                index,
                WalOp::Put {
                    id: "doc-2".to_string(),
                    json_blob: Some(serde_json::json!({ "title": "second" })),
                },
            )
            .unwrap();
        store.commit_index(index).unwrap();

        let third = store.warm_index(index).unwrap().expect("stats");
        assert!(
            third.segments_warmed > 0,
            "a new searcher generation must be warmed, not skipped"
        );
        assert_eq!(third.num_docs, 2, "both documents should be searchable");
    }

    /// Both overridden analyzers must keep a token of exactly `MAX_INDEXED_TOKEN_LEN` bytes and
    /// drop the one byte past it.
    ///
    /// Tantivy's builtins cap these at 40 bytes, so the lower bound fails without the override.
    /// The upper bound is what pins `RemoveLongFilter`'s strictly-less-than limit: written
    /// without the `+ 1`, a token of exactly the cap disappears and this test is the only thing
    /// that says so.
    #[test]
    fn overridden_tokenizers_keep_tokens_up_to_the_cap() {
        let temp_dir = TempDir::new().unwrap();
        let config = StorageConfig {
            shard_path: temp_dir.path().to_path_buf(),
            indexer_memory_budget: 32 * 1024 * 1024,
            indexer_memory_min_mb: 16,
            indexer_memory_max_mb: 256,
            total_memory_limit_bytes: 2048 * 1024 * 1024,
            memory_pressure_threshold_percent: 80,
            indexer_num_threads: 1,
            merge_num_threads: 1,
            default_batch_size: 100_000,
            wal_sync: true,
        };

        let store = HybridStore::new(config, 1).unwrap();
        let index = "long_tokens";
        let mut schema: IndexSchema = serde_json::from_value(serde_json::json!({
            "fields": {
                "title": {"field_type": "text", "indexed": true},
                "body": {"field_type": "text", "indexed": true, "tokenizer": "en_stem"}
            }
        }))
        .unwrap();
        schema.normalize_after_deserialization();
        store.store_schema_and_cache(index, &schema).unwrap();

        let long_token = "a".repeat(MAX_INDEXED_TOKEN_LEN);
        store
            .apply_write(
                index,
                WalOp::Put {
                    id: "doc-1".to_string(),
                    json_blob: Some(serde_json::json!({ "title": long_token })),
                },
            )
            .unwrap();
        store.commit_index(index).unwrap();

        store
            .apply_write(
                index,
                WalOp::Put {
                    id: "doc-2".to_string(),
                    json_blob: Some(serde_json::json!({ "body": long_token })),
                },
            )
            .unwrap();
        store.commit_index(index).unwrap();

        // One byte past the cap, in a third document, to fix where the boundary falls.
        let over_cap = "b".repeat(MAX_INDEXED_TOKEN_LEN + 1);
        store
            .apply_write(
                index,
                WalOp::Put {
                    id: "doc-3".to_string(),
                    json_blob: Some(serde_json::json!({ "title": over_cap })),
                },
            )
            .unwrap();
        store.commit_index(index).unwrap();

        let outcome = store
            .search_documents(index, &format!("title:{long_token}"), 10, None)
            .unwrap();
        assert_eq!(
            outcome.total_hits, 1,
            "a {MAX_INDEXED_TOKEN_LEN}-byte token must be indexed and searchable"
        );

        // Same bound through the stemming tokenizer. Index-time and query-time analysis both
        // resolve `en_stem` from this index, so the two stay symmetric by construction.
        let stemmed = store
            .search_documents(index, &format!("body:{long_token}"), 10, None)
            .unwrap();
        assert_eq!(
            stemmed.total_hits, 1,
            "en_stem must also keep {MAX_INDEXED_TOKEN_LEN}-byte tokens"
        );

        let dropped = store
            .search_documents(index, &format!("title:{over_cap}"), 10, None)
            .unwrap();
        assert_eq!(
            dropped.total_hits, 0,
            "a token past the cap is dropped at index time, so nothing matches it"
        );
    }

    /// A traversal index name must not create or remove anything outside the
    /// shard. This exercises the real write and delete paths rather than the
    /// validator in isolation, because the earlier canonicalize-based guard
    /// passed its unit tests while still allowing `create_dir_all` to escape.
    #[test]
    fn traversal_index_name_cannot_touch_paths_outside_shard() {
        let parent = TempDir::new().unwrap();
        let shard_path = parent.path().join("shard");
        std::fs::create_dir_all(&shard_path).unwrap();

        // A sibling of the shard that must survive untouched.
        let victim = parent.path().join("victim");
        std::fs::create_dir_all(&victim).unwrap();
        std::fs::write(victim.join("keep.txt"), b"precious").unwrap();

        let config = StorageConfig {
            shard_path: shard_path.clone(),
            indexer_memory_budget: 32 * 1024 * 1024,
            indexer_memory_min_mb: 16,
            indexer_memory_max_mb: 256,
            total_memory_limit_bytes: 2048 * 1024 * 1024,
            memory_pressure_threshold_percent: 80,
            indexer_num_threads: 1,
            merge_num_threads: 1,
            default_batch_size: 100,
            wal_sync: true,
        };
        let store = HybridStore::new(config, 1).unwrap();

        for name in ["../victim", "..", "../../etc", "a/b"] {
            assert!(
                matches!(store.index_dir(name), Err(StoreError::InvalidIndexName(_))),
                "index_dir must reject '{}'",
                name
            );

            // The write path creates the index directory, so it must refuse too.
            let write = store.apply_write(
                name,
                WalOp::Put {
                    id: "doc-1".to_string(),
                    json_blob: Some(serde_json::json!({ "title": "x" })),
                },
            );
            assert!(write.is_err(), "apply_write must reject '{}'", name);

            // The delete path removes a directory, so it must refuse too.
            assert!(
                store.delete_index_data(name, true).is_err(),
                "delete_index_data must reject '{}'",
                name
            );
        }

        // Nothing outside the shard was created or removed.
        assert!(victim.join("keep.txt").exists(), "sibling file was deleted");
        assert_eq!(
            std::fs::read(victim.join("keep.txt")).unwrap(),
            b"precious",
            "sibling file was modified"
        );
        assert!(
            !parent.path().join("etc").exists(),
            "a directory was created outside the shard"
        );
    }

    #[test]
    fn test_multi_tenant_storage() {
        let temp_dir = TempDir::new().unwrap();
        let config = StorageConfig {
            shard_path: temp_dir.path().to_path_buf(),

            // Memory Budget Configuration
            indexer_memory_budget: 32 * 1024 * 1024,
            indexer_memory_min_mb: 32,
            indexer_memory_max_mb: 512,
            total_memory_limit_bytes: 2048 * 1024 * 1024,
            memory_pressure_threshold_percent: 80,

            // Thread Configuration
            indexer_num_threads: 1,
            merge_num_threads: 2,

            // Other Configuration
            default_batch_size: 1000,
            wal_sync: true,
        };

        let store = HybridStore::new(config, 1).unwrap();

        // Write to index1
        let op1 = WalOp::Put {
            id: "doc1".to_string(),
            json_blob: None,
        };
        let seq1 = store.apply_write("index1", op1).unwrap();
        assert_eq!(seq1, 1);

        // Write to index2
        let op2 = WalOp::Put {
            id: "doc1".to_string(),
            json_blob: None,
        };
        let seq2 = store.apply_write("index2", op2).unwrap();
        assert_eq!(seq2, 1); // Independent sequence

        // Verify directories exist
        let index1_path = temp_dir.path().join("indices").join("index1");
        let index2_path = temp_dir.path().join("indices").join("index2");
        assert!(index1_path.exists());
        assert!(index2_path.exists());

        // Delete index1 (with schema deletion)
        store.delete_index_data("index1", true).unwrap();

        // Verify index1 is gone but index2 remains
        assert!(!index1_path.exists());
        assert!(index2_path.exists());

        // Verify index2 still works
    }

    #[test]
    fn test_field_type_inference() {
        use crate::{FieldDef, TantivyFieldType};
        use serde_json::json;

        // Test field type inference from JSON values
        let test_cases = vec![
            (json!("hello"), TantivyFieldType::Text),
            (json!("2023-01-01T00:00:00Z"), TantivyFieldType::Date),
            (json!("192.168.1.1"), TantivyFieldType::Ip),
            (json!(42), TantivyFieldType::I64),
            (json!(std::f64::consts::PI), TantivyFieldType::F64),
            (json!(true), TantivyFieldType::Boolean),
            (json!(null), TantivyFieldType::Text),
            (json!([1, 2, 3]), TantivyFieldType::Text),
            (json!({"key": "value"}), TantivyFieldType::Json),
        ];

        for (value, expected_type) in test_cases {
            let inferred_type = FieldDef::infer_type_from_value(&value);
            assert_eq!(
                inferred_type, expected_type,
                "Failed to infer type for value: {:?}",
                value
            );
        }

        println!("✅ Field type inference works correctly!");
    }

    #[test]
    fn test_field_def_creation() {
        use crate::{FieldDef, TantivyFieldType};

        // Test FieldDef creation with different types
        let text_field = FieldDef::new("title".to_string(), TantivyFieldType::Text);
        assert_eq!(text_field.field_type, TantivyFieldType::Text);
        assert!(text_field.indexed);
        assert!(!text_field.stored); // Only "id" field is stored in Tantivy
        assert!(!text_field.is_fast()); // Text fields are not fast by default

        let i64_field = FieldDef::new("count".to_string(), TantivyFieldType::I64);
        assert_eq!(i64_field.field_type, TantivyFieldType::I64);
        assert!(i64_field.indexed);
        assert!(!i64_field.stored); // Only "id" field is stored in Tantivy
        assert!(i64_field.is_fast()); // Numeric fields are fast by default

        // Test the "id" field special case
        let id_field = FieldDef::new("id".to_string(), TantivyFieldType::Text);
        assert_eq!(id_field.field_type, TantivyFieldType::Text);
        assert!(id_field.indexed);
        assert!(id_field.stored); // "id" field is stored in Tantivy
        assert!(!id_field.is_fast()); // Text fields are not fast by default

        let json_field = FieldDef::new("metadata".to_string(), TantivyFieldType::Json);
        assert_eq!(json_field.field_type, TantivyFieldType::Json);
        assert!(json_field.indexed);
        assert!(!json_field.stored); // Only "id" field is stored in Tantivy
        assert!(!json_field.is_fast()); // JSON fields are not fast by default

        println!("✅ FieldDef creation works correctly!");
    }

    #[test]
    fn test_schema_evolution() {
        use crate::{IndexSchema, TantivyFieldType};
        use serde_json::json;

        let mut schema = IndexSchema::default();

        // Add initial fields
        let doc1 = json!({
            "name": "Test",
            "value": 123
        });

        let evolved_fields = schema.evolve_from_document(&doc1);
        assert_eq!(evolved_fields.len(), 2);
        assert_eq!(schema.fields.len(), 2);

        // Verify field types
        assert_eq!(
            schema.fields.get("name").unwrap().field_type,
            TantivyFieldType::Text
        );
        assert_eq!(
            schema.fields.get("value").unwrap().field_type,
            TantivyFieldType::I64
        );

        // Evolve with new document
        let doc2 = json!({
            "name": "Test 2",
            "value": 456.789, // Should evolve to F64
            "created_at": "2023-01-01T00:00:00Z" // New field
        });

        let evolved_fields = schema.evolve_from_document(&doc2);
        assert_eq!(evolved_fields.len(), 2); // value evolved + created_at added
        assert_eq!(schema.fields.len(), 3);

        // Verify evolution
        assert_eq!(
            schema.fields.get("value").unwrap().field_type,
            TantivyFieldType::F64
        );
        assert_eq!(
            schema.fields.get("created_at").unwrap().field_type,
            TantivyFieldType::Date
        );

        println!("✅ Schema evolution works correctly!");
    }

    #[test]
    fn test_tantivy_date_comparison_with_clamping() {
        use tantivy::DateTime;

        // Test that our clamping strategy works correctly
        // 1606-01-01 (Volpone publication - would overflow without clamping)
        let old_ts: i64 = -11_486_668_800;
        let clamped_old_ts = old_ts.clamp(TANTIVY_MIN_TIMESTAMP_SECS, TANTIVY_MAX_TIMESTAMP_SECS);
        let old_tantivy = DateTime::from_timestamp_secs(clamped_old_ts);

        // 2023-05-27 (Query bound)
        let new_ts: i64 = 1_685_145_600; // 2023-05-27T00:00:00Z
        let new_tantivy = DateTime::from_timestamp_secs(new_ts);

        println!(
            "1606-01-01 (clamped to 1677): timestamp={}, tantivy={:?}",
            clamped_old_ts, old_tantivy
        );
        println!(
            "2023-05-27: timestamp={}, tantivy={:?}",
            new_ts, new_tantivy
        );

        // With clamping, 1677 should be LESS than 2023
        assert!(
            old_tantivy < new_tantivy,
            "Clamped 1677 date should be less than 2023 date"
        );
        assert_eq!(
            clamped_old_ts, TANTIVY_MIN_TIMESTAMP_SECS,
            "Pre-1677 date should be clamped to minimum"
        );

        // Test future date clamping
        let future_ts: i64 = 10_000_000_000; // Beyond 2262
        let clamped_future =
            future_ts.clamp(TANTIVY_MIN_TIMESTAMP_SECS, TANTIVY_MAX_TIMESTAMP_SECS);
        assert_eq!(
            clamped_future, TANTIVY_MAX_TIMESTAMP_SECS,
            "Post-2262 date should be clamped to maximum"
        );

        println!("✅ Tantivy DateTime clamping works correctly for out-of-range dates!");
    }

    #[test]
    fn test_background_schema_evolution() {
        use crate::{IndexSchema, TantivyFieldType};
        use serde_json::json;

        let mut schema = IndexSchema::default();

        // Add initial document with new fields
        let doc = json!({
            "title": "Test Document",
            "count": 42,
            "timestamp": "2023-01-01T00:00:00Z"
        });

        let evolved_fields = schema.evolve_from_document(&doc);
        assert_eq!(evolved_fields.len(), 3, "Should discover 3 new fields");

        // Verify all new fields are non-indexed
        let title_field = schema.fields.get("title").unwrap();
        assert_eq!(title_field.field_type, TantivyFieldType::Text);
        assert!(!title_field.indexed, "New fields should be non-indexed");
        assert!(
            !title_field.stored,
            "Only 'id' field should be stored in Tantivy"
        );

        let count_field = schema.fields.get("count").unwrap();
        assert_eq!(count_field.field_type, TantivyFieldType::I64);
        assert!(!count_field.indexed, "New fields should be non-indexed");
        assert!(count_field.is_fast(), "Numeric fields should be fast");

        let timestamp_field = schema.fields.get("timestamp").unwrap();
        assert_eq!(timestamp_field.field_type, TantivyFieldType::Date);
        assert!(!timestamp_field.indexed, "New fields should be non-indexed");

        // Verify we can get non-indexed fields
        let non_indexed = schema.get_non_indexed_fields();
        assert_eq!(non_indexed.len(), 3, "Should have 3 non-indexed fields");
        assert!(non_indexed.contains(&"title".to_string()));
        assert!(non_indexed.contains(&"count".to_string()));
        assert!(non_indexed.contains(&"timestamp".to_string()));

        // Test promoting a field to indexed
        let promoted = schema.promote_field_to_indexed("title");
        assert!(promoted, "Should successfully promote field");
        assert!(
            schema.fields.get("title").unwrap().indexed,
            "Field should now be indexed"
        );

        // Verify non-indexed count decreased
        let non_indexed_after = schema.get_non_indexed_fields();
        assert_eq!(
            non_indexed_after.len(),
            2,
            "Should have 2 non-indexed fields after promotion"
        );
        assert!(
            !non_indexed_after.contains(&"title".to_string()),
            "Promoted field should not be in list"
        );

        // Test promoting already indexed field
        let promoted_again = schema.promote_field_to_indexed("title");
        assert!(!promoted_again, "Should not promote already indexed field");

        println!("✅ Background schema evolution works correctly!");
    }

    #[test]
    fn test_type_aliases_deserialization() {
        use serde_json;

        // Test various type aliases deserialize correctly
        let test_cases = vec![
            ("float", TantivyFieldType::F64),
            ("double", TantivyFieldType::F64),
            ("decimal", TantivyFieldType::F64),
            ("integer", TantivyFieldType::I64),
            ("int", TantivyFieldType::I64),
            ("number", TantivyFieldType::I64),
            ("signed", TantivyFieldType::I64),
            ("unsigned", TantivyFieldType::U64),
            ("uint", TantivyFieldType::U64),
            ("bool", TantivyFieldType::Boolean),
            ("datetime", TantivyFieldType::Date),
            ("timestamp", TantivyFieldType::Date),
            ("binary", TantivyFieldType::Bytes),
            ("blob", TantivyFieldType::Bytes),
            ("object", TantivyFieldType::Json),
            ("document", TantivyFieldType::Json),
            ("category", TantivyFieldType::Facet),
            ("tag", TantivyFieldType::Facet),
            // Test canonical names still work
            ("text", TantivyFieldType::Text),
            ("string", TantivyFieldType::String),
            ("i64", TantivyFieldType::I64),
            ("u64", TantivyFieldType::U64),
            ("f64", TantivyFieldType::F64),
            ("date", TantivyFieldType::Date),
            ("boolean", TantivyFieldType::Boolean),
            ("bytes", TantivyFieldType::Bytes),
            ("ip", TantivyFieldType::Ip),
            ("json", TantivyFieldType::Json),
            ("facet", TantivyFieldType::Facet),
        ];

        for (alias, expected) in test_cases {
            let json = format!(r#"{{"field_type": "{}"}}"#, alias);
            let field_def: FieldDef = serde_json::from_str(&json)
                .unwrap_or_else(|e| panic!("Failed to deserialize '{}': {}", alias, e));

            assert_eq!(
                field_def.field_type, expected,
                "Alias '{}' should map to {:?}, got {:?}",
                alias, expected, field_def.field_type
            );
        }

        // Test case-insensitive
        let json = r#"{"field_type": "FLOAT"}"#;
        let field_def: FieldDef = serde_json::from_str(json).unwrap();
        assert_eq!(field_def.field_type, TantivyFieldType::F64);

        // Test invalid type gives helpful error
        let json = r#"{"field_type": "invalid_type"}"#;
        let result: Result<FieldDef, _> = serde_json::from_str(json);
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("Unknown field type: 'invalid_type'")
        );
        assert!(error.to_string().contains("Supported types:"));

        println!("✅ Type alias deserialization works correctly!");
    }

    #[test]
    fn test_python_schema_compatibility() {
        use serde_json;

        // Test the exact schema from ingest_urls.py
        let python_schema = serde_json::json!({
            "fields": {
                "id": {"field_type": "text", "indexed": true, "stored": true},
                "sha1": {"field_type": "text", "indexed": false, "stored": false, "is_shadow": true},
                "first_analysis": {"field_type": "date", "indexed": true, "stored": false},
                "last_analysis": {"field_type": "date", "indexed": true, "stored": false},
                "platform": {"field_type": "text", "indexed": true, "stored": false},
                "classification": {"field_type": "text", "indexed": true, "stored": false},
                "risk_score": {"field_type": "float", "indexed": true, "stored": false},
                "threat_names": {"field_type": "text", "indexed": true, "stored": false},
                "file_types": {"field_type": "text", "indexed": true, "stored": false},
                "signatures": {"field_type": "text", "indexed": true, "stored": false},
                "urls": {"field_type": "text", "indexed": true, "stored": false}
            }
        });

        let schema: IndexSchema = serde_json::from_value(python_schema).unwrap();

        // Verify the float field was correctly mapped to F64
        let risk_field = schema.fields.get("risk_score").unwrap();
        assert_eq!(risk_field.field_type, TantivyFieldType::F64);
        assert!(risk_field.indexed);
        assert!(!risk_field.stored);

        // Verify other fields are correct
        let id_field = schema.fields.get("id").unwrap();
        assert_eq!(id_field.field_type, TantivyFieldType::Text);
        assert!(id_field.indexed);
        assert!(id_field.stored);

        let date_field = schema.fields.get("first_analysis").unwrap();
        assert_eq!(date_field.field_type, TantivyFieldType::Date);
        assert!(date_field.indexed);
        assert!(!date_field.stored);

        println!("✅ Python schema compatibility works correctly!");
    }

    #[test]
    fn test_ted_schema_compatibility() {
        use serde_json;

        // Test the exact schema from ingest_ted.py with integer type
        let ted_schema = serde_json::json!({
            "fields": {
                "id": {"field_type": "text", "indexed": true, "stored": true},
                "video_id": {"field_type": "text", "indexed": false, "stored": false, "is_shadow": true},
                "title": {"field_type": "text", "indexed": true, "stored": false},
                "speaker": {"field_type": "text", "indexed": true, "stored": false},
                "channel": {"field_type": "text", "indexed": true, "stored": false},
                "description": {"field_type": "text", "indexed": true, "stored": false},
                "tags": {"field_type": "text", "indexed": true, "stored": false},
                "topic_categories": {"field_type": "text", "indexed": true, "stored": false},
                "category_id": {"field_type": "integer", "indexed": true, "stored": false},
                "category_label": {"field_type": "text", "indexed": true, "stored": false},
                "view_count": {"field_type": "integer", "indexed": true, "stored": false},
                "like_count": {"field_type": "integer", "indexed": true, "stored": false},
                "comment_count": {"field_type": "integer", "indexed": true, "stored": false},
                "caption": {"field_type": "boolean", "indexed": true, "stored": false},
                "published_at": {"field_type": "date", "indexed": true, "stored": false},
                "duration_seconds": {"field_type": "integer", "indexed": true, "stored": false}
            }
        });

        let schema: IndexSchema = serde_json::from_value(ted_schema).unwrap();

        // Verify the integer fields were correctly mapped to I64
        for field_name in [
            "category_id",
            "view_count",
            "like_count",
            "comment_count",
            "duration_seconds",
        ] {
            let field = schema.fields.get(field_name).unwrap();
            assert_eq!(field.field_type, TantivyFieldType::I64);
            assert!(field.indexed);
            assert!(!field.stored);
        }

        // Verify boolean field
        let caption_field = schema.fields.get("caption").unwrap();
        assert_eq!(caption_field.field_type, TantivyFieldType::Boolean);
        assert!(caption_field.indexed);
        assert!(!caption_field.stored);

        println!("✅ TED schema compatibility works correctly!");
    }

    #[test]
    fn test_schema_enrichment_preserves_explicit_values() {
        use serde_json;

        // Schema with a mix of minimal and explicit field definitions
        let schema_json = serde_json::json!({
            "fields": {
                "id": {"field_type": "text"},
                "title": {"field_type": "text", "indexed": true},
                "body": {"field_type": "text", "indexed": true, "tokenizer": "en_stem", "index_record_option": "Basic"},
                "score": {"field_type": "float", "indexed": true},
                "created_at": {"field_type": "date"},
                "tag": {"field_type": "string", "indexed": true, "tokenizer": "raw"},
                "sha1": {"field_type": "text", "is_shadow": true},
                "notes": {"field_type": "text", "indexed": false}
            }
        });

        let mut schema: IndexSchema = serde_json::from_value(schema_json).unwrap();
        schema.normalize_after_deserialization();

        // --- id field: always forced to specific Tantivy attributes ---
        let id = schema.fields.get("id").unwrap();
        assert_eq!(id.name, "id");
        assert!(id.indexed, "id must always be indexed");
        assert!(id.stored, "id must always be stored");
        assert_eq!(
            id.tokenizer.as_deref(),
            Some("raw"),
            "id must use raw tokenizer"
        );
        assert_eq!(
            id.index_record_option.as_deref(),
            Some("Basic"),
            "id must use Basic index option"
        );

        // --- title: minimal Text field gets enriched with defaults ---
        let title = schema.fields.get("title").unwrap();
        assert_eq!(title.name, "title");
        assert!(title.indexed);
        assert_eq!(
            title.tokenizer.as_deref(),
            Some("default"),
            "Text field should get default tokenizer"
        );
        assert_eq!(
            title.index_record_option.as_deref(),
            Some("WithFreqsAndPositions"),
            "Text field should get WithFreqsAndPositions"
        );

        // --- body: explicit tokenizer and index_record_option are PRESERVED ---
        let body = schema.fields.get("body").unwrap();
        assert_eq!(body.name, "body");
        assert!(body.indexed);
        assert_eq!(
            body.tokenizer.as_deref(),
            Some("en_stem"),
            "Explicit tokenizer must be preserved"
        );
        assert_eq!(
            body.index_record_option.as_deref(),
            Some("Basic"),
            "Explicit index_record_option must be preserved"
        );

        // --- score: F64 gets fast=true enrichment ---
        let score = schema.fields.get("score").unwrap();
        assert_eq!(score.name, "score");
        assert_eq!(score.field_type, TantivyFieldType::F64);
        assert!(score.indexed);
        assert!(
            score.is_fast(),
            "Numeric fields should be enriched with fast=true"
        );
        assert!(
            score.tokenizer.is_none(),
            "Numeric fields should not have tokenizer"
        );

        // --- created_at: Date gets fast=true, indexed defaults to true ---
        let created = schema.fields.get("created_at").unwrap();
        assert_eq!(created.name, "created_at");
        assert_eq!(created.field_type, TantivyFieldType::Date);
        assert!(created.indexed, "indexed defaults to true");
        assert!(
            created.is_fast(),
            "Date fields should be enriched with fast=true"
        );

        // --- tag: String field with explicit tokenizer preserved ---
        let tag = schema.fields.get("tag").unwrap();
        assert_eq!(tag.name, "tag");
        assert_eq!(tag.field_type, TantivyFieldType::String);
        assert_eq!(
            tag.tokenizer.as_deref(),
            Some("raw"),
            "Explicit tokenizer preserved for String"
        );
        assert_eq!(
            tag.index_record_option.as_deref(),
            Some("Basic"),
            "String gets Basic index option"
        );

        // --- sha1: shadow field is NOT enriched ---
        let sha1 = schema.fields.get("sha1").unwrap();
        assert!(sha1.is_shadow);
        assert!(
            sha1.tokenizer.is_none(),
            "Shadow fields should not be enriched"
        );

        // --- notes: non-indexed field is NOT enriched ---
        let notes = schema.fields.get("notes").unwrap();
        assert!(!notes.indexed);
        assert!(
            notes.tokenizer.is_none(),
            "Non-indexed fields should not be enriched"
        );

        // --- _seq: no longer injected ---
        // Normalization used to force this field into every schema so the built index would
        // carry a column for the checkpoint scan to order on. The commit payload answers that
        // question in O(1) now, so a schema declares only what its author declared. Indices
        // built while the field was injected still have the column and are still written to;
        // nothing new grows one.
        assert!(
            !schema.fields.contains_key("_seq"),
            "normalization must not invent a field the caller never declared"
        );

        println!("✅ Schema enrichment correctly preserves explicit values and fills defaults!");
    }

    /// A description is the one part of a schema nothing can infer, so it has to survive
    /// everything the schema does on its own.
    #[test]
    fn descriptions_round_trip_and_survive_field_evolution() {
        let mut schema: IndexSchema = serde_json::from_value(serde_json::json!({
            "description": "  Quarterly filings, one document per filing.  ",
            "fields": {
                "title": {"field_type": "text", "description": "Filing headline"},
                "year": {"field_type": "i64", "description": "   "},
                "notes": {"field_type": "text"},
            }
        }))
        .expect("schema with descriptions");
        schema.normalize_after_deserialization();

        assert_eq!(
            schema.description.as_deref(),
            Some("Quarterly filings, one document per filing."),
            "surrounding whitespace is not part of what someone wrote"
        );
        assert_eq!(
            schema.fields["year"].description, None,
            "blank is the same as unset, or every reader has to check for it"
        );
        assert_eq!(schema.fields["notes"].description, None);

        // Evolution rewrites a field's type; it must not rewrite what the field means.
        let before = schema.calculate_fingerprint();
        assert!(schema.evolve_field("notes".to_string(), &serde_json::json!(7)));
        assert_eq!(
            schema.fields["title"].description.as_deref(),
            Some("Filing headline")
        );
        assert_eq!(
            schema.calculate_fingerprint(),
            before,
            "the fingerprint is over field names, so nothing here should have moved it"
        );

        // And a round trip through the stored form keeps both, while an index that describes
        // nothing serialises no description keys at all.
        let encoded = serde_json::to_value(&schema).expect("serialise");
        let decoded: IndexSchema = serde_json::from_value(encoded).expect("deserialise");
        assert_eq!(decoded.description, schema.description);
        assert_eq!(
            decoded.fields["title"].description.as_deref(),
            Some("Filing headline")
        );

        let bare = serde_json::to_value(IndexSchema::default()).expect("serialise default");
        assert!(
            bare.get("description").is_none(),
            "an undescribed index must not pay for the field: {bare}"
        );
    }

    /// The limits exist because a catalogue listing carries every index's description at once.
    #[test]
    fn an_over_long_description_is_refused_by_name() {
        let mut schema = IndexSchema {
            description: Some("x".repeat(MAX_INDEX_DESCRIPTION_CHARS + 1)),
            ..Default::default()
        };
        let err = schema.validate_descriptions().expect_err("must be refused");
        assert!(
            err.contains("index description")
                && err.contains(&MAX_INDEX_DESCRIPTION_CHARS.to_string()),
            "the refusal must say what was too long and by what measure: {err}"
        );

        schema.description = Some("x".repeat(MAX_INDEX_DESCRIPTION_CHARS));
        assert!(
            schema.validate_descriptions().is_ok(),
            "the limit is inclusive"
        );

        for name in ["zebra", "apple"] {
            let mut def = FieldDef::new(name.to_string(), TantivyFieldType::Text);
            def.description = Some("x".repeat(MAX_FIELD_DESCRIPTION_CHARS + 1));
            schema.fields.insert(name.to_string(), def);
        }
        let err = schema.validate_descriptions().expect_err("must be refused");
        assert!(
            err.starts_with("description for field 'apple'"),
            "fields live in a HashMap, so the one named has to be chosen in a stable order or \
             the same schema is refused differently each time: {err}"
        );
        assert!(
            err.contains("1 other field"),
            "an operator fixing one at a time needs to know there are more: {err}"
        );

        // A multi-byte description gets the same allowance as an ASCII one.
        let schema = IndexSchema {
            description: Some("é".repeat(MAX_INDEX_DESCRIPTION_CHARS)),
            ..Default::default()
        };
        assert!(
            schema.validate_descriptions().is_ok(),
            "the limit counts characters, not bytes"
        );
    }

    #[test]
    fn test_normalize_date_comparisons() {
        // Single-char operators should normalize the date
        assert_eq!(
            normalize_date_comparisons("created:>2026-01-14", "created"),
            "created:>2026-01-14T00:00:00Z"
        );
        assert_eq!(
            normalize_date_comparisons("created:<2026-01-14", "created"),
            "created:<2026-01-14T00:00:00Z"
        );

        // Compound operators >= and <= must also normalize the date
        assert_eq!(
            normalize_date_comparisons("created:>=2026-01-14", "created"),
            "created:>=2026-01-14T00:00:00Z"
        );
        assert_eq!(
            normalize_date_comparisons("created:<=2026-01-14", "created"),
            "created:<=2026-01-14T00:00:00Z"
        );

        // Already RFC3339 should pass through unchanged
        assert_eq!(
            normalize_date_comparisons("created:>2026-01-14T00:00:00Z", "created"),
            "created:>2026-01-14T00:00:00Z"
        );
        assert_eq!(
            normalize_date_comparisons("created:>=2026-01-14T00:00:00Z", "created"),
            "created:>=2026-01-14T00:00:00Z"
        );

        // Non-date field should not be touched
        assert_eq!(
            normalize_date_comparisons("count:>20", "created"),
            "count:>20"
        );

        // Mixed query with date comparison and other terms
        assert_eq!(
            normalize_date_comparisons("created:>=2026-01-14 AND status:active", "created"),
            "created:>=2026-01-14T00:00:00Z AND status:active"
        );
    }
}
