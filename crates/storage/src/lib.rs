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
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
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
    Document, FAST, Field, INDEXED, STORED, STRING, Schema, TEXT, Value as TantivyValue,
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
    /// Sort order (default: Desc)
    #[serde(default)]
    pub order: SortOrder,
}

/// Sort order direction
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SortOrder {
    #[default]
    Desc,
    Asc,
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
];

const NAIVE_DATE_FORMATS: &[&str] = &["%Y-%m-%d", "%Y/%m/%d", "%Y%m%d", "%Y-%m", "%Y"];

/// Schema metadata table: maps index names to their schema definitions.
const TABLE_SCHEMA: TableDefinition<&str, &[u8]> = TableDefinition::new("schema");

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

    let (_, _, clamped) = parse_date_str_to_tantivy(lit)?;
    let dt = Utc.timestamp_opt(clamped, 0).single()?;
    Some(dt.to_rfc3339_opts(SecondsFormat::Secs, true))
}

fn normalize_date_ranges(input: &str, field: &str) -> String {
    let prefix = format!("{}:[", field);
    let mut out = String::with_capacity(input.len());
    let mut idx = 0usize;

    while let Some(rel) = input[idx..].find(&prefix) {
        let start = idx + rel;
        out.push_str(&input[idx..start]);

        let inner_start = start + prefix.len();
        if let Some(end_rel) = input[inner_start..].find(']') {
            let end = inner_start + end_rel;
            let inner = &input[inner_start..end];

            if let Some((lower, upper)) = inner.split_once(" TO ") {
                let lower_norm = normalize_date_literal(lower).unwrap_or_else(|| lower.to_string());
                let upper_norm = normalize_date_literal(upper).unwrap_or_else(|| upper.to_string());
                out.push_str(&format!("{}:[{} TO {}]", field, lower_norm, upper_norm));
                idx = end + 1;
                continue;
            }
        }

        // Fallback: no closing bracket or malformed, copy the rest of the prefix char and move on
        out.push_str(&input[start..start + prefix.len()]);
        idx = start + prefix.len();
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
            let value_end = input[value_start..]
                .find(char::is_whitespace)
                .map(|r| value_start + r)
                .unwrap_or(input.len());
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
        let value_end = input[value_start..]
            .find(char::is_whitespace)
            .map(|r| value_start + r)
            .unwrap_or(input.len());
        let value = &input[value_start..value_end];

        // Skip if this looks like a range or comparison already handled
        if value.starts_with('[') || value.starts_with('<') || value.starts_with('>') {
            out.push_str(&input[start..value_end]);
            idx = value_end;
            continue;
        }

        let norm = normalize_date_literal(value).unwrap_or_else(|| value.to_string());
        out.push_str(&format!("{}{}", prefix, norm));
        idx = value_end;
    }

    out.push_str(&input[idx..]);
    out
}

/// Parse exact ID queries (id:value or shadow_field:value) that can bypass Tantivy.
/// Returns Some((id_value, true)) for exact ID queries, None otherwise.
fn parse_exact_id_query(query: &str, schema: &IndexSchema) -> Option<(String, bool)> {
    let query = query.trim();

    // Check for simple id:value pattern (no AND/OR operators)
    if let Some(colon_pos) = query.find(':') {
        let field_part = &query[..colon_pos].trim();
        let value_part = &query[colon_pos + 1..].trim();

        // Must be a simple query with no operators
        if value_part.contains(&[' ', '"', '(', ')'][..]) {
            return None;
        }

        // Check if it's the id field or a shadow field
        if *field_part == "id" {
            return Some((value_part.to_string(), true));
        }

        // Check shadow fields
        if let Some(field_def) = schema.fields.get(*field_part)
            && field_def.is_shadow
        {
            return Some((value_part.to_string(), true));
        }
    }

    None
}

/// Normalize date literals in a Tantivy query string based on schema field types.
/// Supports common forms:
/// - field:value (single literal)
/// - field:<value / field:>value
/// - field:[lower TO upper]
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
        normalized = normalize_date_ranges(&normalized, field);
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
        index_path: &PathBuf,
        field_count: Option<usize>,
    ) -> usize {
        let min_budget_bytes = self.indexer_memory_min_mb * 1024 * 1024;
        let max_budget_bytes = self.indexer_memory_max_mb * 1024 * 1024;
        let default_budget_bytes = self.indexer_memory_budget;

        // Check index size and adjust budget dynamically within configurable range
        let size_based_budget = if let Ok(metadata) = std::fs::metadata(index_path) {
            let size_mb = metadata.len() / (1024 * 1024);
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
    pub fn get_bulk_operation_budget(&self, index_path: &PathBuf, batch_size: usize) -> usize {
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
#[derive(Debug, Clone, Serialize, PartialEq, Eq, Hash, Default)]
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
    #[serde(default)]
    pub fast: bool,
    /// Shadow field flag: true if this field preserves original field name when ID is copied to canonical "id" field
    /// Shadow fields are NOT indexed and NOT stored in Tantivy, but preserved in schema for query mapping
    /// Default is false for backward compatibility with existing schemas
    #[serde(default)]
    pub is_shadow: bool,
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
        let fast = matches!(
            field_type,
            TantivyFieldType::I64
                | TantivyFieldType::U64
                | TantivyFieldType::F64
                | TantivyFieldType::Date
        );

        Self {
            name,
            field_type,
            indexed: true,
            stored,
            fast,
            is_shadow: false,          // Default: not a shadow field
            tokenizer: None,           // Will be set when creating from actual Tantivy schema
            index_record_option: None, // Will be set when creating from actual Tantivy schema
        }
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
        let fast = matches!(
            field_type,
            TantivyFieldType::I64
                | TantivyFieldType::U64
                | TantivyFieldType::F64
                | TantivyFieldType::Date
        );

        Self {
            name,
            field_type,
            indexed: false, // Non-indexed by default for background evolution
            stored,
            fast,
            is_shadow: false, // Default: not a shadow field
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
            indexed: false,  // Shadow fields are never indexed
            stored: false,   // Shadow fields are never stored
            fast: false,     // Shadow fields don't need fast access
            is_shadow: true, // This is a shadow field
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
    fn is_naive_datetime(s: &str) -> bool {
        NAIVE_DATETIME_FORMATS
            .iter()
            .any(|fmt| NaiveDateTime::parse_from_str(s, fmt).is_ok())
    }

    /// Check common date-only formats such as
    /// - 2024-05-01
    /// - 2024/05/01
    /// - 20240501
    fn is_naive_date(s: &str) -> bool {
        NAIVE_DATE_FORMATS
            .iter()
            .any(|fmt| NaiveDate::parse_from_str(s, fmt).is_ok())
    }
}

/// Parse a date string (RFC3339, naive datetime, date-only, year-month, or year-only)
/// into the epoch-second timestamp that the Date FAST field is sorted on.
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

    // Date-only formats that NaiveDate can parse directly (YYYY-MM-DD, YYYY/MM/DD, YYYYMMDD)
    for fmt in &["%Y-%m-%d", "%Y/%m/%d", "%Y%m%d"] {
        if let Ok(nd) = NaiveDate::parse_from_str(s, fmt)
            && let Some(ndt) = nd.and_hms_opt(0, 0, 0)
        {
            let ts = Utc.from_utc_datetime(&ndt).timestamp();
            let clamped = ts.clamp(TANTIVY_MIN_TIMESTAMP_SECS, TANTIVY_MAX_TIMESTAMP_SECS);
            let tantivy_dt = DateTime::from_timestamp_secs(clamped);
            return Some((tantivy_dt, ts, clamped));
        }
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

/// Index schema definition for validation and evolution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexSchema {
    pub fields: HashMap<String, FieldDef>,
    #[serde(default = "default_version")]
    pub version: u64,
    #[serde(default)]
    pub fingerprint: u64,
    #[serde(default = "default_timestamp")]
    pub created_at: i64,
    #[serde(default = "default_timestamp")]
    pub updated_at: i64,
    /// Field name to use for routing/sharding (default: "id")
    #[serde(default = "default_routing_field")]
    pub routing_field_name: String,
    /// Pre-computed set of shadow field names for O(1) lookup.
    /// Eliminates per-document HashMap scan in get_shadow_mapping().
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
            fingerprint: 0,
            created_at: now,
            updated_at: now,
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
        for (key, field_def) in &mut self.fields {
            // Populate name from map key if not provided in JSON
            if field_def.name.is_empty() {
                field_def.name = key.clone();
            }

            // The 'id' field has fixed Tantivy attributes regardless of user input
            if key == "id" {
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
                TantivyFieldType::I64
                | TantivyFieldType::U64
                | TantivyFieldType::F64
                | TantivyFieldType::Date => {
                    // Numeric and date types should be fast by default for range queries
                    field_def.fast = true;
                }
                _ => {}
            }
        }
        // Ensure reserved '_seq' field is always present (WAL sequence tracking)
        self.fields
            .entry("_seq".to_string())
            .or_insert_with(|| FieldDef {
                name: "_seq".to_string(),
                field_type: TantivyFieldType::U64,
                indexed: false,
                stored: true,
                fast: true,
                is_shadow: false,
                tokenizer: None,
                index_record_option: None,
            });

        self.rebuild_shadow_fields_cache();
    }

    /// Calculate deterministic fingerprint from sorted field names
    pub fn calculate_fingerprint(&self) -> u64 {
        let mut sorted_names: Vec<&String> = self.fields.keys().collect();
        sorted_names.sort();
        let mut combined = Vec::new();
        for name in sorted_names {
            combined.extend_from_slice(name.as_bytes());
        }
        xxh3_64(&combined)
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
            self.fingerprint = self.calculate_fingerprint();
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

    /// Get mapping of shadow field names to canonical "id" field
    /// All shadow fields map to "id" for query transformation
    pub fn get_shadow_mapping(&self) -> HashMap<String, String> {
        self.fields
            .iter()
            .filter(|(_, def)| def.is_shadow)
            .map(|(name, _)| (name.clone(), "id".to_string()))
            .collect()
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
}

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
pub fn reconstruct_shadow_fields_in_json(json_blob: &JsonValue, schema: &IndexSchema) -> JsonValue {
    if let Some(obj) = json_blob.as_object() {
        let shadow_mapping = schema.get_shadow_mapping();

        // Capacity optimization: pre-allocate for shadow + original fields
        let mut out = JsonMap::with_capacity(obj.len() + shadow_mapping.len());

        // OPTIMIZATION 1: Use get() instead of get().cloned() to avoid temporary allocation
        if let Some(id_val) = obj.get("id") {
            // Insert canonical id first (Enforces Order)
            out.insert("id".to_string(), id_val.clone());

            // Then shadow fields
            for (shadow_field, canonical_field) in shadow_mapping.iter() {
                // Check if mapping targets "id" and field doesn't already exist
                if canonical_field == "id" && !obj.contains_key(shadow_field) {
                    out.insert(shadow_field.clone(), id_val.clone());
                }
            }
        }

        // Then remaining original fields (skip id since we added it)
        for (k, v) in obj {
            if k != "id" {
                out.insert(k.clone(), v.clone());
            }
        }

        JsonValue::Object(out)
    } else {
        json_blob.clone()
    }
}

/// Optimized: Reconstruct shadow fields by consuming the input (Ownership Transfer).
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

    let shadow_mapping = schema.get_shadow_mapping();

    // CASE 1: No Shadow Fields -> Strict ID Ordering
    if shadow_mapping.is_empty() {
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
    let mut out = JsonMap::with_capacity(obj.len() + shadow_mapping.len());

    // Resolve the canonical ID: prefer blob's "id", fall back to doc_id (redb key)
    let id_val = obj
        .remove("id")
        .unwrap_or_else(|| serde_json::Value::String(doc_id.to_string()));

    // Insert Shadow Fields FIRST
    for (shadow_field, canonical_field) in shadow_mapping {
        if canonical_field == "id" {
            out.insert(shadow_field, id_val.clone());
        }
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
    /// Tantivy field for the WAL sequence number
    seq: Field,
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

/// Result of batch index size measurement
struct IndexSizes {
    tantivy_bytes: u64,
    redb_bytes: u64,
    document_count: u64,
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
    read_cache: Arc<DashMap<String, HashMap<String, Vec<u8>>>>,
    /// Cache of optimal memory budgets per index to avoid frequent syscalls
    budget_cache: Arc<DashMap<String, usize>>,
    /// Cache of schemas per index to avoid repeated redb reads
    schema_cache: Arc<DashMap<String, Arc<IndexSchema>>>,
    /// Cache of Tantivy field mappings per index
    fields_cache: Arc<DashMap<String, SchemaFields>>,
    /// Unified cache for index sizes (Tantivy + Redb) with expiration to avoid repeated expensive calculations
    index_size_cache: Arc<Mutex<HashMap<String, IndexSizeCache>>>,
    /// Cache expiration duration for index sizes (1 hour)
    index_cache_expiry: Duration,
    /// Storage configuration
    config: StorageConfig,
}

impl HybridStore {
    /// Calculate tiered cache sizes based on database file size, system memory, and shard count.
    /// Returns (init_cache_size, normal_cache_size) in bytes.
    ///
    /// Memory is divided by max_shards to ensure we don't exceed system limits
    /// when multiple shards are initialized on the same node.
    fn calculate_cache_sizes(
        config: &StorageConfig,
        db_file_size_bytes: u64,
        total_shards: usize,
    ) -> (usize, usize) {
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

        // Calculate INIT BOOST cache
        let boost_multiplier = if db_file_size_bytes < 1024 * 1024 {
            1
        } else if db_file_size_bytes < 100 * 1024 * 1024 {
            2
        } else if db_file_size_bytes < 1024 * 1024 * 1024 {
            4
        } else {
            8
        };

        let init_cache = if boost_multiplier == 1 {
            standard_cache
        } else {
            // Cap init boost at per_shard_available to prevent excessive memory usage
            // across many shards. Previously used fixed caps (128MB/512MB/2GB) which could
            // lead to OOM with many large shards.
            let max_boost = per_shard_available;
            let boosted = (base_standard_cache * boost_multiplier).min(max_boost as usize);
            (boosted as u64)
                .min(per_shard_available)
                .min(per_shard_total) as usize
        };

        tracing::info!(
            file_size_mb = db_file_size_bytes / (1024 * 1024),
            available_memory_mb = available_memory_bytes / (1024 * 1024),
            total_memory_mb = total_memory_bytes / (1024 * 1024),
            max_shards = shard_count,
            per_shard_available_mb = per_shard_available / (1024 * 1024),
            standard_cache_mb = standard_cache / (1024 * 1024),
            init_cache_mb = init_cache / (1024 * 1024),
            "HybridStore: calculated tiered cache sizes (per-shard)"
        );

        (init_cache, standard_cache)
    }

    /// Creates a new multi-tenant HybridStore with tiered cache sizing.
    /// Uses a larger "init boost" cache for fast recovery on existing databases,
    /// then reopens with normal cache size for standard operations.
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
        let (init_cache_size, normal_cache_size) =
            Self::calculate_cache_sizes(&config, db_file_size, total_shards);

        let kv = if db_file_exists {
            // EXISTING DATABASE: Two-phase open for fast recovery
            // Phase 1: Open with init boost cache for fast recovery/metadata loading
            let init_start_time = Instant::now();
            tracing::info!(
                db_path = %kv_path.display(),
                init_cache_mb = init_cache_size / (1024 * 1024),
                "HybridStore: Phase 1/2 - Opening with init boost cache"
            );

            let mut builder = redb::Builder::new();
            builder.set_cache_size(init_cache_size);
            let temp_db = builder.open(&kv_path)?;

            // Phase 2: Close and reopen with normal cache
            tracing::info!(
                elapsed_ms = init_start_time.elapsed().as_millis(),
                normal_cache_mb = normal_cache_size / (1024 * 1024),
                "HybridStore: Phase 2/2 - Reopening with normal cache"
            );

            // Explicitly drop to close before reopening
            drop(temp_db);

            // Reopen with normal cache for standard operations
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
            init_cache_mb = if db_file_exists { init_cache_size / (1024 * 1024) } else { 0 },
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
                    if let Err(e) = w.commit() {
                        tracing::warn!(index = %index, error = %e, "Failed to commit index during shutdown");
                    }
                }
            } else {
                tracing::debug!(index = %index, "No pending operations, skipping commit during shutdown");
            }
        }

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

    /// Get the highest indexed sequence number using FAST field ordering.
    /// Leverages columnar FAST field storage for O(1) access instead of O(n) scanning.
    fn get_highest_indexed_seq(&self, reader: &IndexReader) -> Result<u64, StoreError> {
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

    /// Recover missing WAL operations.
    /// 1. Get max seq from redb WAL table
    /// 2. Get highest committed seq from Tantivy _seq FAST field
    /// 3. Replay missing operations from (last_committed_seq+1) to max_wal_seq
    ///
    /// Returns (replayed_count, max_wal_seq, last_committed_seq) so callers
    /// can reuse these values without redundant lookups.
    fn recover_index(
        &self,
        index: &str,
        writer: &mut IndexWriter,
        reader: &IndexReader,
    ) -> Result<(usize, u64, u64), StoreError> {
        let max_wal_seq = self.get_max_wal_id_for_index(index)?;

        if max_wal_seq == 0 {
            tracing::debug!(index = %index, "No WAL entries found, skipping recovery");
            return Ok((0, 0, 0));
        }

        let last_committed_seq = self.get_highest_indexed_seq(reader)?;

        tracing::info!(
            index = %index,
            max_wal_seq = max_wal_seq,
            last_committed_seq = last_committed_seq,
            "WAL recovery check"
        );

        // If all sequences are committed, nothing to recover
        if last_committed_seq >= max_wal_seq {
            tracing::info!(index = %index, "All WAL sequences already committed to Tantivy");
            return Ok((0, max_wal_seq, last_committed_seq));
        }

        // Start recovery from the first missing sequence
        let range_start = last_committed_seq + 1;

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

        // Get schema for building documents
        let index_schema = self
            .get_schema_cached(index)?
            .unwrap_or_else(|| Arc::new(IndexSchema::default()));

        let schema = reader.searcher().index().schema();
        let id_field = schema
            .get_field("id")
            .map_err(|_| StoreError::FieldNotFound("id".to_string()))?;
        let seq_field = schema
            .get_field("_seq")
            .map_err(|_| StoreError::FieldNotFound("_seq".to_string()))?;

        // Build indexed fields map
        let mut indexed_fields = HashMap::new();
        for (field, field_entry) in schema.fields() {
            let name = field_entry.name();
            if name != "id" && name != "_seq" {
                indexed_fields.insert(name.to_string(), field);
            }
        }

        // Collect all WAL entries first for better cache locality during processing
        let wal_entries: Vec<(u64, Vec<u8>)> = wal_table
            .range(range_start..)?
            .map(|result| {
                let (seq_guard, wal_data_guard) = result?;
                Ok((seq_guard.value(), wal_data_guard.value().to_vec()))
            })
            .collect::<Result<Vec<_>, redb::Error>>()?;

        let total_entries = wal_entries.len();
        tracing::info!(
            index = %index,
            total_entries = total_entries,
            "Collected WAL entries for recovery"
        );

        // Replay missing operations with pre-collected data for better cache locality
        let mut replayed_count = 0;
        for (seq_id, wal_data) in wal_entries {
            let wal_op: WalOp = serde_json::from_slice(&wal_data)
                .map_err(|e| StoreError::Serialization(e.to_string()))?;

            match wal_op {
                WalOp::Put { id, json_blob } => {
                    let mut tantivy_doc = tantivy::TantivyDocument::default();
                    tantivy_doc.add_text(id_field, &id);
                    tantivy_doc.add_u64(seq_field, seq_id);

                    if let Some(json_obj) = json_blob.as_ref().and_then(|v| v.as_object()) {
                        for (field_name, field_def) in &index_schema.fields {
                            if !field_def.indexed || field_def.is_shadow || field_name == "id" {
                                continue;
                            }

                            if let Some(tantivy_field) = indexed_fields.get(field_name)
                                && let Some(field_value) = json_obj.get(field_name)
                            {
                                self.add_field_to_tantivy_doc(
                                    &mut tantivy_doc,
                                    *tantivy_field,
                                    field_def,
                                    field_value,
                                )?;
                            }
                        }
                    }

                    let term = tantivy::Term::from_field_text(id_field, &id);
                    writer.delete_term(term);
                    writer.add_document(tantivy_doc)?;
                }
                WalOp::Delete { id } => {
                    let term = tantivy::Term::from_field_text(id_field, &id);
                    writer.delete_term(term);
                }
            }

            replayed_count += 1;

            // Log progress every 1000 documents
            if replayed_count % 1000 == 0 {
                tracing::info!(
                    index = %index,
                    replayed = replayed_count,
                    total = total_entries,
                    "Recovery progress"
                );
            }
        }

        tracing::info!(
            index = %index,
            replayed_count = replayed_count,
            "WAL recovery completed - replayed missing operations"
        );

        Ok((replayed_count, max_wal_seq, last_committed_seq))
    }

    /// Helper method to add a field to a Tantivy document based on its type.
    /// Used during WAL recovery to rebuild documents from JSON.
    fn add_field_to_tantivy_doc(
        &self,
        tantivy_doc: &mut tantivy::TantivyDocument,
        tantivy_field: Field,
        field_def: &FieldDef,
        field_value: &JsonValue,
    ) -> Result<(), StoreError> {
        match field_def.field_type {
            TantivyFieldType::Text => {
                if let Some(s) = field_value.as_str() {
                    tantivy_doc.add_text(tantivy_field, s);
                } else {
                    let field_str = serde_json::to_string(field_value)
                        .map_err(|e| StoreError::Serialization(e.to_string()))?;
                    tantivy_doc.add_text(tantivy_field, &field_str);
                }
            }
            TantivyFieldType::String => {
                if let Some(s) = field_value.as_str() {
                    tantivy_doc.add_text(tantivy_field, s);
                } else if let Some(arr) = field_value.as_array() {
                    for item in arr {
                        if let Some(s) = item.as_str() {
                            tantivy_doc.add_text(tantivy_field, s);
                        }
                    }
                }
            }
            TantivyFieldType::F64 => {
                if let Some(n) = field_value.as_f64() {
                    tantivy_doc.add_f64(tantivy_field, n);
                }
            }
            TantivyFieldType::I64 => {
                if let Some(n) = field_value.as_i64() {
                    tantivy_doc.add_i64(tantivy_field, n);
                }
            }
            TantivyFieldType::U64 => {
                if let Some(n) = field_value.as_u64() {
                    tantivy_doc.add_u64(tantivy_field, n);
                }
            }
            TantivyFieldType::Date => {
                if let Some(s) = field_value.as_str()
                    && let Some((tantivy_dt, _, _)) = parse_date_str_to_tantivy(s)
                {
                    tantivy_doc.add_date(tantivy_field, tantivy_dt);
                }
            }
            TantivyFieldType::Boolean => {
                if let Some(b) = field_value.as_bool() {
                    tantivy_doc.add_bool(tantivy_field, b);
                }
            }
            TantivyFieldType::Bytes => {
                if let Some(arr) = field_value.as_array() {
                    let mut bytes = Vec::new();
                    for item in arr {
                        if let Some(n) = item.as_u64() {
                            bytes.push(n as u8);
                        }
                    }
                    if !bytes.is_empty() {
                        tantivy_doc.add_bytes(tantivy_field, &bytes);
                    }
                }
            }
            TantivyFieldType::Ip => {
                if let Some(s) = field_value.as_str()
                    && let Ok(ip) = s.parse::<std::net::IpAddr>()
                {
                    let ipv6 = match ip {
                        std::net::IpAddr::V4(ipv4) => ipv4.to_ipv6_mapped(),
                        std::net::IpAddr::V6(ipv6) => ipv6,
                    };
                    tantivy_doc.add_ip_addr(tantivy_field, ipv6);
                }
            }
            TantivyFieldType::Json => {
                let json_str = serde_json::to_string(field_value)
                    .map_err(|e| StoreError::Serialization(e.to_string()))?;
                tantivy_doc.add_text(tantivy_field, &json_str);
            }
            TantivyFieldType::Facet => {
                if let Some(s) = field_value.as_str() {
                    tantivy_doc.add_facet(tantivy_field, s);
                }
            }
        }
        Ok(())
    }

    /// Get a value from the read cache if present.
    fn get_from_cache(&self, index: &str, key: &str) -> Option<Vec<u8>> {
        self.read_cache.get(index)?.get(key).cloned()
    }

    /// Insert a value into the read cache with a simple per-index size bound.
    fn insert_into_cache(&self, index: &str, key: &str, value: Vec<u8>) {
        const MAX_CACHE_ENTRIES_PER_INDEX: usize = 1024;

        let mut index_cache = self.read_cache.entry(index.to_string()).or_default();

        if index_cache.len() >= MAX_CACHE_ENTRIES_PER_INDEX
            && let Some(first_key) = index_cache.keys().next().cloned()
        {
            index_cache.remove(&first_key);
        }

        index_cache.insert(key.to_string(), value);
    }

    /// Build Tantivy schema and field map from index schema definition using native Tantivy types.
    fn create_schema_from_definition(index_schema: &IndexSchema) -> (Schema, SchemaFields) {
        use tantivy::schema::{IndexRecordOption, TextFieldIndexing, TextOptions};

        let mut schema_builder = Schema::builder();

        // ID field is always present - untokenized string for exact matching
        let id_field = schema_builder.add_text_field("id", STRING | STORED);

        // Sequence field for WAL ordering - reserved field
        // FAST is required for order_by_u64_field sorting
        let seq_field = schema_builder.add_u64_field("_seq", STORED | FAST);

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
                    schema_builder.add_text_field(name, options)
                }
                TantivyFieldType::String => schema_builder.add_text_field(name, STRING),
                TantivyFieldType::I64 => {
                    if field_def.fast {
                        schema_builder.add_i64_field(name, INDEXED | FAST)
                    } else {
                        schema_builder.add_i64_field(name, INDEXED)
                    }
                }
                TantivyFieldType::U64 => {
                    if field_def.fast {
                        schema_builder.add_u64_field(name, INDEXED | FAST)
                    } else {
                        schema_builder.add_u64_field(name, INDEXED)
                    }
                }
                TantivyFieldType::F64 => {
                    if field_def.fast {
                        schema_builder.add_f64_field(name, INDEXED | FAST)
                    } else {
                        schema_builder.add_f64_field(name, INDEXED)
                    }
                }
                TantivyFieldType::Date => {
                    if field_def.fast {
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
            seq: seq_field,
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

        let seq = schema
            .get_field("_seq")
            .map_err(|_| StoreError::FieldNotFound("_seq".to_string()))?;

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
                    fast,
                    is_shadow: false, // Fields derived from Tantivy schema are not shadow fields
                    tokenizer,
                    index_record_option,
                },
            );
        }

        let text_count = fields
            .values()
            .filter(|f| matches!(f.field_type, TantivyFieldType::Text))
            .count();
        let fast_count = fields.values().filter(|f| f.fast).count();
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
            fingerprint: 0,
            created_at: now,
            updated_at: now,
            routing_field_name: "id".to_string(),
            shadow_fields: HashSet::new(),
        }
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

        // Create index directory and Tantivy index if it doesn't exist
        let index_path = self.config.shard_path.join("indices").join(index);
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
            let opened_index = Index::open_in_dir(&index_path)?;
            let fields = Self::load_fields_from_existing_index(&opened_index)?;
            (opened_index, fields, false)
        } else {
            // Creating new index: use the schema and fields we just built
            fs::create_dir_all(&index_path)?;
            let new_index = Index::create_in_dir(&index_path, schema)?;

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
                        fast: false,
                        is_shadow: false, // The canonical 'id' field is not a shadow field
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

        // WAL Recovery: Check if there are any operations in the WAL that need to be replayed
        // This happens when the index was opened after a crash or restart
        let recovery_start = Instant::now();
        let reader = tantivy_index
            .reader_builder()
            .reload_policy(tantivy::ReloadPolicy::OnCommitWithDelay)
            .try_into()?;

        let (replayed_count, max_wal_seq, last_committed_seq) =
            self.recover_index(index, &mut writer, &reader)?;

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

        // Initialize sequence counter for this index using values already computed
        // by recover_index — avoids redundant lookups.
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
            // Fallback: calculate and cache
            let index_path = self.config.shard_path.join("indices").join(index);
            let b = self.config.get_optimal_memory_budget(&index_path, None);
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
    /// After commit, truncates WAL entries that are now safely persisted in Tantivy.
    pub fn commit_index(&self, index: &str) -> Result<(), StoreError> {
        // No-op guard: skip commit if no operations pending since last commit.
        let ops_pending = self.get_operations_count(index);
        if ops_pending == 0 {
            tracing::debug!(index = %index, "commit_index: skipping, no pending operations");
            return Ok(());
        }

        if let Some(writer_arc) = self.writers.get(index) {
            // CRITICAL: Minimize lock hold time to prevent deadlocks
            // The writer lock must be dropped IMMEDIATELY after commit
            {
                let mut writer = writer_arc.value().lock().unwrap_or_else(|poisoned| {
                    tracing::error!(index = %index, "Writer mutex was poisoned during commit, recovering");
                    poisoned.into_inner()
                });
                writer.commit()?;
                // Explicit drop to release lock before any other operations
                drop(writer);
            }

            // All post-commit operations happen WITHOUT holding the writer lock
            self.reset_operations_counter(index);

            tracing::debug!(index = %index, ops_committed = ops_pending, "commit_index: committed");

            // CRITICAL: Smart refresh reader cache after commit to ensure search sees latest data
            self.smart_refresh_reader(index)?;

            // Refresh budget cache after commit since index size likely changed
            let index_path = self.config.shard_path.join("indices").join(index);
            let new_budget = self.config.get_optimal_memory_budget(&index_path, None);
            self.budget_cache.insert(index.to_string(), new_budget);
        }

        // AFTER Tantivy commit succeeds: Truncate WAL entries that are now safely persisted.
        // This prevents the WAL from growing indefinitely and ensures that on restart,
        // we only replay operations that were NOT committed to Tantivy.
        self.truncate_wal_up_to_committed(index)?;

        Ok(())
    }

    /// Truncate WAL entries up to the current sequence counter.
    /// Called after a successful Tantivy commit — all WAL entries up to current_seq
    /// are now safely persisted in Tantivy and can be removed from redb.
    fn truncate_wal_up_to_committed(&self, index: &str) -> Result<(), StoreError> {
        let last_committed_seq = match self.current_seq.get(index) {
            Some(counter) => counter.load(Ordering::SeqCst),
            None => return Ok(()),
        };

        if last_committed_seq == 0 {
            return Ok(());
        }

        let wal_table_name = format!("wal_{}", index);
        let wal_table_def = TableDefinition::<u64, &[u8]>::new(&wal_table_name);

        let mut write_txn = self.kv.begin_write()?;
        {
            write_txn.set_durability(Durability::Immediate)?;
            let mut wal_table = write_txn.open_table(wal_table_def)?;

            let keys_to_delete: Vec<u64> = wal_table
                .range(0..=last_committed_seq)?
                .map(|result| result.map(|(k, _)| k.value()))
                .collect::<Result<Vec<_>, _>>()?;

            let deleted_count = keys_to_delete.len();
            for key in keys_to_delete {
                wal_table.remove(key)?;
            }

            if deleted_count > 0 {
                tracing::debug!(
                    index = %index,
                    deleted = deleted_count,
                    up_to_seq = last_committed_seq,
                    "Truncated committed WAL entries"
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

    /// Multi-tenant apply_write method
    pub fn apply_write(&self, index: &str, op: WalOp) -> Result<u64, StoreError> {
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

        // Write to WAL first
        let wal_data =
            serde_json::to_vec(&op).map_err(|e| StoreError::Serialization(e.to_string()))?;

        // Evolve schema if new fields are present (declare outside transaction scope)
        let mut evolved_schema = None;

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
                    let mut tantivy_doc = doc!(
                        fields.id => id.as_str(),
                        fields.seq => seq_id // Inject the WAL sequence
                    );

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

                            match field_def.field_type {
                                TantivyFieldType::Text => {
                                    if let Some(s) = field_value.as_str() {
                                        tantivy_doc.add_text(*tantivy_field, s);
                                    } else {
                                        let field_str = serde_json::to_string(field_value)
                                            .map_err(|e| {
                                                StoreError::Serialization(e.to_string())
                                            })?;
                                        tantivy_doc.add_text(*tantivy_field, &field_str);
                                    }
                                }
                                TantivyFieldType::String => {
                                    if let Some(s) = field_value.as_str() {
                                        tantivy_doc.add_text(*tantivy_field, s);
                                    } else if let Some(arr) = field_value.as_array() {
                                        for item in arr {
                                            if let Some(s) = item.as_str() {
                                                tantivy_doc.add_text(*tantivy_field, s);
                                            }
                                        }
                                    }
                                }
                                TantivyFieldType::F64 => {
                                    if let Some(n) = field_value.as_f64() {
                                        tantivy_doc.add_f64(*tantivy_field, n);
                                    }
                                }
                                TantivyFieldType::I64 => {
                                    if let Some(n) = field_value.as_i64() {
                                        tantivy_doc.add_i64(*tantivy_field, n);
                                    }
                                }
                                TantivyFieldType::U64 => {
                                    if let Some(n) = field_value.as_u64() {
                                        tantivy_doc.add_u64(*tantivy_field, n);
                                    }
                                }
                                TantivyFieldType::Date => {
                                    if let Some(s) = field_value.as_str()
                                        && let Some((tantivy_dt, ts, clamped)) =
                                            parse_date_str_to_tantivy(s)
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
                                        tantivy_doc.add_date(*tantivy_field, tantivy_dt);
                                    }
                                }
                                TantivyFieldType::Boolean => {
                                    if let Some(b) = field_value.as_bool() {
                                        tantivy_doc.add_bool(*tantivy_field, b);
                                    }
                                }
                                TantivyFieldType::Bytes => {
                                    if let Some(arr) = field_value.as_array() {
                                        let mut bytes = Vec::new();
                                        for item in arr {
                                            if let Some(n) = item.as_u64() {
                                                bytes.push(n as u8);
                                            }
                                        }
                                        if !bytes.is_empty() {
                                            tantivy_doc.add_bytes(*tantivy_field, bytes.as_slice());
                                        }
                                    }
                                }
                                TantivyFieldType::Ip => {
                                    if let Some(s) = field_value.as_str()
                                        && let Ok(ip) = s.parse::<std::net::IpAddr>()
                                    {
                                        let ipv6 = match ip {
                                            std::net::IpAddr::V4(ipv4) => ipv4.to_ipv6_mapped(),
                                            std::net::IpAddr::V6(ipv6) => ipv6,
                                        };
                                        tantivy_doc.add_ip_addr(*tantivy_field, ipv6);
                                    }
                                }
                                TantivyFieldType::Json => {
                                    let json_str = serde_json::to_string(field_value)
                                        .map_err(|e| StoreError::Serialization(e.to_string()))?;
                                    tantivy_doc.add_text(*tantivy_field, &json_str);
                                }
                                TantivyFieldType::Facet => {
                                    if let Some(s) = field_value.as_str() {
                                        tantivy_doc.add_facet(*tantivy_field, &s);
                                    }
                                }
                            }
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
                }
            }
        }

        write_txn.commit()?;

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
        // Remove from caches first
        self.writers.remove(index);
        self.readers.remove(index);
        self.current_seq.remove(index);
        self.read_cache.remove(index);
        self.schema_cache.remove(index);
        self.fields_cache.remove(index);
        self.budget_cache.remove(index);

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
        let index_path = self.config.shard_path.join("indices").join(index);
        if index_path.exists() {
            fs::remove_dir_all(index_path)?;
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

        let read_txn = self.kv.begin_read()?;

        match read_txn.open_table(data_table_def) {
            Ok(data_table) => match data_table.get(key)? {
                Some(value) => {
                    let bytes = value.value().to_vec();
                    self.insert_into_cache(index, key, bytes.clone());
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

        // Single read transaction for all keys
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
                self.insert_into_cache(index, key, bytes.clone());
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
        let index_path = self.config.shard_path.join("indices").join(index);

        // Always load stored schema first (may contain non-indexed fields)
        let stored_schema = self.get_schema(index)?;

        if index_path.exists() {
            let tantivy_index = Index::open_in_dir(&index_path)?;

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

    /// Get or create a cached IndexReader for the given index
    /// Uses lock-free fast path with DashMap and ReloadPolicy::OnCommitWithDelay for automatic background updates
    fn get_reader(&self, index: &str) -> Result<Option<(IndexReader, SchemaFields)>, StoreError> {
        // Fast path: Zero-lock retrieval from cache
        if let Some(reader_ref) = self.readers.get(index) {
            let reader = reader_ref.value();
            // Note: Manual reload() removed. Reader configured with ReloadPolicy::OnCommitWithDelay
            // will automatically reload within milliseconds after commits.

            // Get fields (fast lookup)
            let tantivy_index = reader.searcher().index().clone();
            let fields = self.get_fields_for_index(index, &tantivy_index)?;

            return Ok(Some((reader.clone(), fields)));
        }

        // Slow path: Index not cached, need to open and cache it
        let index_path = self.config.shard_path.join("indices").join(index);
        if !index_path.exists() || !index_path.join("meta.json").exists() {
            return Ok(None);
        }

        // Use DashMap entry API for concurrent-safe creation
        let reader = self
            .readers
            .entry(index.to_string())
            .or_try_insert_with(|| {
                let tantivy_index = Index::open_in_dir(&index_path)?;

                // Configure reader with ReloadPolicy::OnCommitWithDelay for automatic background reloading
                // This watches meta.json and reloads within milliseconds after commits
                let reader = tantivy_index
                    .reader_builder()
                    .reload_policy(tantivy::ReloadPolicy::OnCommitWithDelay)
                    .try_into()?;

                Ok::<IndexReader, StoreError>(reader)
            })?;

        // Warm up fields cache
        let tantivy_index = reader.value().searcher().index().clone();
        let fields = self.get_fields_for_index(index, &tantivy_index)?;

        Ok(Some((reader.value().clone(), fields)))
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
    ) -> Result<(Vec<(f32, JsonValue)>, usize), StoreError> {
        // Get reader and field mapping from cache or disk
        let (reader, fields) = match self.get_reader(index)? {
            Some(r) => r,
            None => {
                warn!(index = %index, "No tantivy reader found for index");
                return Ok((Vec::new(), 0));
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
                return Ok((Vec::new(), total_hits));
            }

            let normalized_query = normalize_date_query(query, &schema);
            let tantivy_schema = tantivy_index.schema();
            let default_query_fields: Vec<Field> = fields
                .indexed_fields
                .values()
                .filter(|field| {
                    let field_entry = tantivy_schema.get_field_entry(**field);
                    matches!(
                        field_entry.field_type(),
                        tantivy::schema::FieldType::Str(_)
                            | tantivy::schema::FieldType::JsonObject(_)
                    )
                })
                .cloned()
                .collect();
            let query_parser =
                tantivy::query::QueryParser::for_index(tantivy_index, default_query_fields);
            let (parsed_query, parse_errors) = query_parser.parse_query_lenient(&normalized_query);

            if !parse_errors.is_empty() {
                debug!(
                    index = %index,
                    query = %normalized_query,
                    errors = ?parse_errors,
                    "Count-only: lenient query parse produced non-fatal errors"
                );
            }

            let count_collector = tantivy::collector::Count;
            let total_hits = searcher.search(&parsed_query, &count_collector)?;

            debug!(
                index = %index,
                total_hits = total_hits,
                "Count-only search completed (limit=0)"
            );

            return Ok((Vec::new(), total_hits));
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
            return Ok((results, total_hits));
        }

        // Normalize date literals based on schema so naive inputs match indexed Date fields
        let normalized_query = normalize_date_query(query, &schema);

        // Create query parser and execute search
        // Only text/string fields are used as default search fields (for unqualified queries).
        // Numeric/date fields require explicit field:value syntax.
        // This prevents parse errors when a generic text search tries to match against numeric fields.
        let tantivy_schema = tantivy_index.schema();
        let default_query_fields: Vec<Field> = fields
            .indexed_fields
            .values()
            .filter(|field| {
                let field_entry = tantivy_schema.get_field_entry(**field);
                matches!(
                    field_entry.field_type(),
                    tantivy::schema::FieldType::Str(_) | tantivy::schema::FieldType::JsonObject(_)
                )
            })
            .cloned()
            .collect();
        let query_parser =
            tantivy::query::QueryParser::for_index(tantivy_index, default_query_fields);

        // Use lenient parsing to gracefully handle type mismatches
        // (e.g. field:hello on a numeric field skips that field instead of failing the entire query)
        let (parsed_query, parse_errors) = query_parser.parse_query_lenient(&normalized_query);

        if !parse_errors.is_empty() {
            debug!(
                index = %index,
                query = %normalized_query,
                errors = ?parse_errors,
                "Lenient query parse produced non-fatal errors"
            );
        }

        // Execute search with sorting if specified, otherwise use MultiCollector
        // OPTIMIZATION: Use MultiCollector for both sorted and unsorted to get count in single pass
        let (top_docs, total_hits) = if let Some(sort_spec) = _sort {
            // Get field from schema to check type and FAST flag
            let schema = tantivy_index.schema();
            let field = schema
                .get_field(&sort_spec.field)
                .map_err(|_| StoreError::FieldNotFound(sort_spec.field.clone()))?;

            let field_entry = schema.get_field_entry(field);

            if !field_entry.is_fast() {
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
                        .order_by_fast_field::<$t>(&sort_spec.field, order);
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

            // Support u64, i64, f64, and Date fields for sorting via FAST fields
            match field_entry.field_type() {
                tantivy::schema::FieldType::U64(_) => collect_sorted!(u64),
                tantivy::schema::FieldType::I64(_) => collect_sorted!(i64),
                tantivy::schema::FieldType::F64(_) => collect_sorted!(f64),
                tantivy::schema::FieldType::Date(_) => collect_sorted!(tantivy::DateTime),
                _ => {
                    return Err(StoreError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!(
                            "Field '{}' type {:?} is not sortable. Supported sortable FAST field types: u64, i64, f64, date.",
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
            return Ok((Vec::new(), total_hits));
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

        Ok((results, total_hits))
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

                    let wal_bytes = serde_json::to_vec(&WalOp::Put {
                        id: id.clone(),
                        json_blob: filtered_json_blob.clone(),
                    })
                    .map_err(|e| StoreError::Serialization(e.to_string()))?;

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
                    let wal_bytes = serde_json::to_vec(&WalOp::Delete { id: id.clone() })
                        .map_err(|e| StoreError::Serialization(e.to_string()))?;

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
                        let mut tantivy_doc = doc!(
                            fields.id => id.as_str(),
                            fields.seq => seq_id // Inject the WAL sequence
                        );

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

                                match field_def.field_type {
                                    TantivyFieldType::Text => {
                                        if let Some(s) = field_value.as_str() {
                                            tantivy_doc.add_text(*tantivy_field, s);
                                        } else {
                                            let field_str = serde_json::to_string(field_value)
                                                .map_err(|e| {
                                                    StoreError::Serialization(e.to_string())
                                                })?;
                                            tantivy_doc.add_text(*tantivy_field, &field_str);
                                        }
                                    }
                                    TantivyFieldType::String => {
                                        if let Some(s) = field_value.as_str() {
                                            tantivy_doc.add_text(*tantivy_field, s);
                                        } else if let Some(arr) = field_value.as_array() {
                                            for item in arr {
                                                if let Some(s) = item.as_str() {
                                                    tantivy_doc.add_text(*tantivy_field, s);
                                                }
                                            }
                                        }
                                    }
                                    TantivyFieldType::F64 => {
                                        if let Some(n) = field_value.as_f64() {
                                            tantivy_doc.add_f64(*tantivy_field, n);
                                        }
                                    }
                                    TantivyFieldType::I64 => {
                                        if let Some(n) = field_value.as_i64() {
                                            tantivy_doc.add_i64(*tantivy_field, n);
                                        }
                                    }
                                    TantivyFieldType::U64 => {
                                        if let Some(n) = field_value.as_u64() {
                                            tantivy_doc.add_u64(*tantivy_field, n);
                                        }
                                    }
                                    TantivyFieldType::Date => {
                                        if let Some(s) = field_value.as_str()
                                            && let Some((tantivy_dt, ts, clamped)) =
                                                parse_date_str_to_tantivy(s)
                                        {
                                            if ts != clamped {
                                                tracing::debug!(
                                                    field = %field_name,
                                                    input = %s,
                                                    original_ts = %ts,
                                                    clamped_ts = %clamped,
                                                    "Date clamped to Tantivy safe range (batch)"
                                                );
                                            }
                                            tantivy_doc.add_date(*tantivy_field, tantivy_dt);
                                        }
                                    }
                                    TantivyFieldType::Boolean => {
                                        if let Some(b) = field_value.as_bool() {
                                            tantivy_doc.add_bool(*tantivy_field, b);
                                        }
                                    }
                                    TantivyFieldType::Bytes => {
                                        if let Some(arr) = field_value.as_array() {
                                            let mut bytes = Vec::new();
                                            for item in arr {
                                                if let Some(n) = item.as_u64() {
                                                    bytes.push(n as u8);
                                                }
                                            }
                                            if !bytes.is_empty() {
                                                tantivy_doc
                                                    .add_bytes(*tantivy_field, bytes.as_slice());
                                            }
                                        }
                                    }
                                    TantivyFieldType::Ip => {
                                        if let Some(s) = field_value.as_str()
                                            && let Ok(ip) = s.parse::<std::net::IpAddr>()
                                        {
                                            let ipv6 = match ip {
                                                std::net::IpAddr::V4(ipv4) => ipv4.to_ipv6_mapped(),
                                                std::net::IpAddr::V6(ipv6) => ipv6,
                                            };
                                            tantivy_doc.add_ip_addr(*tantivy_field, ipv6);
                                        }
                                    }
                                    TantivyFieldType::Json => {
                                        let json_str =
                                            serde_json::to_string(field_value).map_err(|e| {
                                                StoreError::Serialization(e.to_string())
                                            })?;
                                        tantivy_doc.add_text(*tantivy_field, &json_str);
                                    }
                                    TantivyFieldType::Facet => {
                                        if let Some(s) = field_value.as_str() {
                                            tantivy_doc.add_facet(*tantivy_field, &s);
                                        }
                                    }
                                }
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

        // Invalidate size cache for this index to ensure fresh stats on next query
        if new_documents_count > 0 || !updated_document_ids.is_empty() {
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
                let index_path = self.config.shard_path.join("indices").join(index_name);
                let tantivy_index_exists = index_path.join("meta.json").exists();

                per_index.insert(
                    index_name.clone(),
                    IndexShardStats {
                        document_count: sizes.document_count,
                        redb_bytes: sizes.redb_bytes,
                        tantivy_bytes: sizes.tantivy_bytes,
                        tantivy_index_exists,
                        tantivy_scan_ms: 0,
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

    /// Warm up all existing indices by opening them, which triggers WAL recovery.
    /// This should be called during startup to ensure all indices are recovered
    /// and ready for use, rather than waiting for first access.
    ///
    /// Uses parallel threads to warm up indices concurrently for faster startup.
    /// Returns the total number of operations recovered across all indices.
    pub fn warmup_indices(&self) -> Result<usize, StoreError> {
        let warmup_start = Instant::now();
        let index_names = self.get_index_names()?;

        if index_names.is_empty() {
            tracing::debug!("No indices to warm up");
            return Ok(0);
        }

        tracing::info!(
            count = index_names.len(),
            "Starting parallel index warmup and WAL recovery for {} indices",
            index_names.len()
        );

        // Use scoped threads to warm up indices in parallel.
        // get_or_create_index uses DashMap internally so concurrent access is safe.
        // Cap parallelism to avoid excessive memory from many simultaneous Tantivy writers.
        let max_parallel = std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(4)
            .min(index_names.len())
            .max(1);
        let errors = std::sync::Mutex::new(Vec::new());

        std::thread::scope(|s| {
            // Process indices in chunks to limit parallelism
            for chunk in index_names.chunks(max_parallel) {
                let handles: Vec<_> = chunk
                    .iter()
                    .map(|index_name| {
                        let errors = &errors;
                        s.spawn(move || {
                            match self.get_or_create_index(index_name) {
                                Ok(_) => {
                                    tracing::debug!(index = %index_name, "Index warmed up successfully");
                                }
                                Err(e) => {
                                    tracing::warn!(
                                        index = %index_name,
                                        error = %e,
                                        "Failed to warm up index, will retry on first access"
                                    );
                                    errors.lock().unwrap().push(index_name.clone());
                                }
                            }
                        })
                    })
                    .collect();

                // Wait for this chunk to complete before starting next
                for handle in handles {
                    let _ = handle.join();
                }
            }
        });

        let failed = errors.into_inner().unwrap();
        let warmup_elapsed = warmup_start.elapsed();
        tracing::info!(
            indices_count = index_names.len(),
            failed_count = failed.len(),
            parallel_threads = max_parallel,
            elapsed_ms = warmup_elapsed.as_millis(),
            "Index warmup completed in {}ms",
            warmup_elapsed.as_millis()
        );

        Ok(0)
    }

    fn measure_tantivy_bytes(&self, index_name: &str) -> Result<u64, StoreError> {
        let index_dir = self.config.shard_path.join("indices").join(index_name);
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
        // Try to get reader from cache first
        if let Some(reader) = self.readers.get(index_name) {
            let searcher = reader.searcher();
            return Ok(searcher.num_docs());
        }

        // If reader not cached, try to create index (which will cache the reader)
        match self.get_or_create_index(index_name) {
            Ok(_) => {
                // Now reader should be cached, try again
                if let Some(reader) = self.readers.get(index_name) {
                    let searcher = reader.searcher();
                    Ok(searcher.num_docs())
                } else {
                    Ok(0) // Shouldn't happen, but handle gracefully
                }
            }
            Err(_) => Ok(0), // Index doesn't exist or not yet created
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
mod tests {
    use super::*;
    use tempfile::TempDir;

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
        assert!(!text_field.fast); // Text fields are not fast by default

        let i64_field = FieldDef::new("count".to_string(), TantivyFieldType::I64);
        assert_eq!(i64_field.field_type, TantivyFieldType::I64);
        assert!(i64_field.indexed);
        assert!(!i64_field.stored); // Only "id" field is stored in Tantivy
        assert!(i64_field.fast); // Numeric fields are fast by default

        // Test the "id" field special case
        let id_field = FieldDef::new("id".to_string(), TantivyFieldType::Text);
        assert_eq!(id_field.field_type, TantivyFieldType::Text);
        assert!(id_field.indexed);
        assert!(id_field.stored); // "id" field is stored in Tantivy
        assert!(!id_field.fast); // Text fields are not fast by default

        let json_field = FieldDef::new("metadata".to_string(), TantivyFieldType::Json);
        assert_eq!(json_field.field_type, TantivyFieldType::Json);
        assert!(json_field.indexed);
        assert!(!json_field.stored); // Only "id" field is stored in Tantivy
        assert!(!json_field.fast); // JSON fields are not fast by default

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
        assert!(count_field.fast, "Numeric fields should be fast");

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
            score.fast,
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
            created.fast,
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

        // --- _seq: reserved WAL sequence field is always injected ---
        let seq = schema.fields.get("_seq").unwrap();
        assert_eq!(seq.name, "_seq");
        assert_eq!(seq.field_type, TantivyFieldType::U64);
        assert!(!seq.indexed, "_seq must not be indexed");
        assert!(seq.stored, "_seq must be stored");
        assert!(seq.fast, "_seq must be fast");
        assert!(!seq.is_shadow);
        assert!(seq.tokenizer.is_none());
        assert!(seq.index_record_option.is_none());

        println!("✅ Schema enrichment correctly preserves explicit values and fills defaults!");
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
