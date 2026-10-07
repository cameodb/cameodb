//! The data model: field and index schemas, document building, WAL record
//! types, date typing and shadow-field handling.
use crate::*;
use std::collections::{HashMap, HashSet};

use chrono::{Datelike, NaiveDate, NaiveDateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize, de::Error as DeserializeError};
use serde_json::Map as JsonMap;
use serde_json::Value as JsonValue;
use tantivy::schema::{Facet, Field, IndexRecordOption};
use tantivy::{DateTime, doc};
use xxhash_rust::xxh3::xxh3_64;

/// Tantivy DateTime safe range limits (to avoid i64 overflow during nanosecond conversion)
/// DateTime::from_timestamp_secs() multiplies by 1_000_000_000, so safe range is:
/// i64::MIN / 1_000_000_000 to i64::MAX / 1_000_000_000
pub(crate) const TANTIVY_MIN_TIMESTAMP_SECS: i64 = -9_223_372_036; // 1677-09-21 00:12:44 UTC
pub(crate) const TANTIVY_MAX_TIMESTAMP_SECS: i64 = 9_223_372_036; // 2262-04-11 23:47:16 UTC

/// Common naive datetime and date formats used for inference and normalization
pub(crate) const NAIVE_DATETIME_FORMATS: &[&str] = &[
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

pub(crate) const NAIVE_DATE_FORMATS: &[&str] = &["%Y-%m-%d", "%Y/%m/%d", "%Y.%m.%d", "%Y%m%d"];

/// Dates written with the year last: slashes month first, dots day first, and named months.
///
/// Each separator has one reading, always: a slash date is American, `03/04/2024` is March 4th;
/// a dotted date is European, `03.04.2024` is the 3rd of April. Deciding per value would read
/// one file's dates two ways — `03/04/2024` month first beside `15/03/2024` day first — with
/// nothing to say which rows were which. So a value only the other order can read (`15/03/2024`,
/// `03.15.2024`) is refused. A source written the other way round is the loader's to recognise:
/// it decides a column's order from its sample and sends such dates as ISO.
///
/// A year is four digits here as in every naive form — see [`parse_naive_datetime`].
pub(crate) const YEAR_LAST_FORMATS: &[&str] = &[
    "%m/%d/%Y",
    "%m/%d/%Y %H:%M:%S",
    "%m/%d/%Y %H:%M",
    "%d.%m.%Y",
    "%d.%m.%Y %H:%M:%S",
    "%d.%m.%Y %H:%M",
    "%b %d, %Y",
    "%B %d, %Y",
    "%b %d %Y",
    "%B %d %Y",
    "%d %b %Y",
    "%d %B %Y",
];

/// A date or datetime written without an offset, read as UTC.
///
/// The one list both inference and the writer read, so a column inferred as a date is one the
/// writer can index — two lists had already drifted once, and a field typed `date` whose values
/// the writer then skipped is a field that silently never matches.
pub(crate) fn parse_naive_datetime(s: &str) -> Option<NaiveDateTime> {
    let midnight = |date: NaiveDate| date.and_hms_opt(0, 0, 0);
    let parse = |fmt: &&str| {
        NaiveDateTime::parse_from_str(s, fmt)
            .ok()
            .or_else(|| NaiveDate::parse_from_str(s, fmt).ok().and_then(midnight))
    };
    // chrono reads `%Y` from any number of digits, so `03/15/24` parsed as the year 24 and
    // `15.03.24` — through `%Y.%m.%d` — as the 24th of March in the year 15: dates that sort
    // before every real one and that nobody wrote. The year has to be written out, as four
    // digits, which still admits a real early year such as `0476-09-04`.
    let year_written_out = |parsed: &NaiveDateTime| s.contains(&format!("{:04}", parsed.year()));
    NAIVE_DATETIME_FORMATS
        .iter()
        .chain(NAIVE_DATE_FORMATS)
        .chain(YEAR_LAST_FORMATS)
        .filter_map(parse)
        .find(year_written_out)
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

impl TantivyFieldType {
    /// The narrowest type that holds the values of both: the type itself when they agree, `f64`
    /// for two numeric types, and `text`, which holds any value, otherwise.
    ///
    /// The join a [learned](FieldDef::learned) field's type is reached by. It depends only on the
    /// two types, never on which came first, so nodes that saw the same values in any order —
    /// or different values, joined afterwards — settle on the same type.
    pub fn widened(&self, other: &TantivyFieldType) -> TantivyFieldType {
        use TantivyFieldType::{F64, I64, U64};
        match (self, other) {
            (a, b) if a == b => a.clone(),
            (I64 | U64 | F64, I64 | U64 | F64) => F64,
            _ => TantivyFieldType::Text,
        }
    }
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
    /// Whether an unqualified term can be searched against a field of this type — the types
    /// that make up the default search fields.
    pub fn is_default_searchable(&self) -> bool {
        matches!(
            self,
            TantivyFieldType::Text | TantivyFieldType::String | TantivyFieldType::Json
        )
    }

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

pub(crate) fn default_true() -> bool {
    true
}

pub(crate) fn default_version() -> u64 {
    1
}

pub(crate) fn default_routing_field() -> String {
    "id".to_string()
}

/// Longest description an index may carry, in characters.
///
/// A description is read by a caller choosing between datasets, and a catalogue listing returns
/// one per index, so the whole node's worth of them is resident in that caller's context at once.
/// A paragraph is enough to say what a dataset is; a page of it is a document, and belongs
/// somewhere that can be fetched on purpose.
pub(crate) const MAX_INDEX_DESCRIPTION_CHARS: usize = 512;

/// Longest description a single field may carry, in characters.
///
/// Tighter than the index limit because it is paid per field on every schema read: one line
/// saying what the field means, not the history of how it came to exist.
pub(crate) const MAX_FIELD_DESCRIPTION_CHARS: usize = 200;

/// Blank is the same as unset, so an operator clearing a description gets `None` rather than a
/// key with nothing in it.
pub(crate) fn normalize_description(description: &mut Option<String>) {
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
    /// How a text field is analysed; omitted when unset, as on every field that is not text.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tokenizer: Option<String>,
    /// What a text field's postings record: "Basic", "WithFreqs" or "WithFreqsAndPositions";
    /// omitted when unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_record_option: Option<String>,
    /// Added by a write rather than declared. While it is not indexed it has no column, so its
    /// type is only what its values have been — widened to hold each new one
    /// ([`TantivyFieldType::widened`]) rather than refusing it. A declared field keeps the type it
    /// was given. In the fingerprint when set, since it changes which values a write may carry.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub learned: bool,
}

impl FieldDef {
    /// Create a new field definition with sensible defaults
    pub fn new(name: String, field_type: TantivyFieldType) -> Self {
        // Only ID field should be stored in Tantivy
        // All other fields are indexed-only, complete data comes from redb
        let stored = name == "id";
        // `id` is the exception to the type's default too: the builder always gives the key a
        // fast column, whatever the declaration says — normalization pins the same flags.
        let fast = Some(name == "id" || Self::fast_by_default(&field_type));

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
            learned: false,
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

    /// A field a write added, typed by the values it has seen: non-indexed, so no column is
    /// built for it until it is promoted.
    pub fn new_learned(name: String, field_type: TantivyFieldType) -> Self {
        let mut field = Self::new(name, field_type);
        field.indexed = false;
        field.learned = true;
        field
    }

    /// Give a [learned](Self::learned) field a wider type, with the `fast` default that type has.
    pub fn retype_learned(&mut self, field_type: TantivyFieldType) {
        let fresh = Self::new(self.name.clone(), field_type);
        self.field_type = fresh.field_type;
        self.fast = fresh.fast;
    }

    /// A field a write discovered, typed by its first value: [learned](Self::learned), so later
    /// values widen it, and non-indexed, since the index's columns were fixed when it was built.
    /// It can be promoted to indexed through a schema edit.
    pub fn new_non_indexed(name: String, value: &JsonValue) -> Self {
        Self::new_learned(name, Self::infer_type_from_value(value))
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
            learned: false,
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
                // 1) A timestamp with an offset: RFC 3339, or RFC 2822 as mail and HTTP write it
                if chrono::DateTime::parse_from_rfc3339(s).is_ok()
                    || chrono::DateTime::parse_from_rfc2822(s).is_ok()
                    // 2) A date or datetime without one
                    || parse_naive_datetime(s).is_some()
                {
                    TantivyFieldType::Date
                // 4) IP detection
                } else if s.parse::<std::net::IpAddr>().is_ok() {
                    TantivyFieldType::Ip
                } else {
                    TantivyFieldType::Text
                }
            }
            JsonValue::Array(items) => {
                Self::infer_element_type(items).unwrap_or(TantivyFieldType::Text)
            }
            JsonValue::Object(_) => TantivyFieldType::Json, // Nested objects as JSON
            JsonValue::Null => TantivyFieldType::Text,
        }
    }

    /// The one type every element of a list can be held as, if there is one.
    ///
    /// Every tantivy field is multivalued, so a list of numbers is a numeric field with several
    /// values in it — the reading the write path already takes, since it adds one value per
    /// element. Typing the list itself as text made the two disagree on the case that matters:
    /// `{"risk_score": [9, 12]}` arriving at a field nobody had declared produced a text field,
    /// and no range query ever matched it again.
    ///
    /// Nulls are passed over rather than counted against it: the writer stores nothing for one,
    /// so `[9, null, 12]` is two values of a numeric field. A list of only nulls, an empty one,
    /// one whose elements disagree, or one holding lists or objects has no element type — the
    /// writer flattens exactly one level, and text is the only type that holds both of anything.
    pub(crate) fn infer_element_type(items: &[JsonValue]) -> Option<TantivyFieldType> {
        let mut agreed: Option<TantivyFieldType> = None;

        for item in items {
            if item.is_null() {
                continue;
            }
            if matches!(item, JsonValue::Array(_) | JsonValue::Object(_)) {
                return None;
            }
            let inferred = Self::infer_type_from_value(item);
            agreed = match agreed {
                None => Some(inferred),
                Some(current) => Some(current.widened(&inferred)),
            };
        }

        agreed
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
/// What a value the field's type cannot hold should cost.
///
/// Ingest refuses it, because the caller is right there and can be told which field and which
/// value. Replay cannot: the value is already committed to redb, so refusing it would fail the
/// index open rather than the write that accepted it, and an index that will not open serves
/// nothing.
///
/// Two types need this, and for the same reason: a facet path and a byte value are both checked
/// by their *content* rather than their JSON type, so a document written by a build that did not
/// check can still be sitting in redb.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum BadValue {
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
/// Text is treated that way too: each element of a list is a value of its own, a string as it
/// is and anything else as its JSON. Serializing the whole list into one value, as text once did,
/// indexed `["ok", "warning"]` as the text `["ok","warning"]` — so a phrase matched across two
/// elements, and under the raw tokenizer no element could be matched at all, only the whole
/// list's spelling. Json alone takes the whole value serialized: a json field holds a document,
/// not several values. Bytes is a list by definition. One level is flattened and no more, so a
/// list inside a list is a value the inner type has to accept on its own — which mirrors what
/// this function then does with it.
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
    bad_value: BadValue,
) -> Result<(), StoreError> {
    // Text, which takes any value as text and a list as several; json and bytes, which take the
    // value whole.
    match field_type {
        TantivyFieldType::Text => {
            let values: &[JsonValue] = match field_value.as_array() {
                Some(items) => items.as_slice(),
                None => std::slice::from_ref(field_value),
            };
            for value in values {
                match value {
                    JsonValue::String(s) => tantivy_doc.add_text(tantivy_field, s),
                    JsonValue::Null => {}
                    other => {
                        let text = serde_json::to_string(other)
                            .map_err(|e| StoreError::Serialization(e.to_string()))?;
                        tantivy_doc.add_text(tantivy_field, &text);
                    }
                }
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
            let Some(arr) = field_value.as_array() else {
                return Ok(());
            };

            // Every element or none. A byte array is one value, not several — dropping the
            // element that would not fit rewrites the value rather than losing one of a set,
            // which is why replay skips the whole field here where it skips a single facet.
            let mut bytes = Vec::with_capacity(arr.len());
            for item in arr {
                match item.as_u64().and_then(|n| u8::try_from(n).ok()) {
                    Some(byte) => bytes.push(byte),
                    None => {
                        let err = StoreError::InvalidFieldValue {
                            field: field_name.to_string(),
                            reason: not_a_byte(item),
                        };
                        match bad_value {
                            BadValue::Refuse => return Err(err),
                            BadValue::SkipAndWarn => {
                                tracing::warn!(
                                    field = %field_name,
                                    error = %err,
                                    "Skipping a byte array with a value outside 0-255 during replay"
                                );
                                return Ok(());
                            }
                        }
                    }
                }
            }

            if !bytes.is_empty() {
                tantivy_doc.add_bytes(tantivy_field, bytes.as_slice());
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
                // A date arrives written or counted: a formatted string, or a whole number of
                // seconds since the epoch, which is the shape most exporters emit and the unit
                // every timestamp in this file is already in.
                let parsed = match value {
                    JsonValue::String(s) => parse_date_str_to_tantivy(s),
                    _ => value.as_i64().map(epoch_seconds_to_tantivy),
                };
                if let Some((tantivy_dt, ts, clamped)) = parsed {
                    if ts != clamped {
                        tracing::debug!(
                            field = %field_name,
                            input = %value,
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
                        Err(err) => match bad_value {
                            BadValue::Refuse => return Err(err),
                            BadValue::SkipAndWarn => tracing::warn!(
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

/// A whole number of seconds since the epoch, as tantivy holds it.
///
/// Seconds, never milliseconds. Guessing the unit from the magnitude is how a timestamp in 2033
/// becomes one in 1970 and nobody notices, and there is no value that says which was meant — a
/// caller with milliseconds sends a string, or divides.
pub(crate) fn epoch_seconds_to_tantivy(secs: i64) -> (DateTime, i64, i64) {
    let clamped = secs.clamp(TANTIVY_MIN_TIMESTAMP_SECS, TANTIVY_MAX_TIMESTAMP_SECS);
    (DateTime::from_timestamp_secs(clamped), secs, clamped)
}

/// Whether a value is one a date field can hold: a parseable string, or epoch seconds.
///
/// Public so the write path's validator asks the same question the writer answers, rather than
/// asking `infer_field_type`, which reads a number as an integer and would refuse it.
pub fn is_date_value(value: &JsonValue) -> bool {
    match value {
        JsonValue::String(s) => parse_date_str_to_tantivy(s).is_some(),
        _ => value.as_i64().is_some(),
    }
}

/// Whether a date field indexes `value` as another instant than it names: a date before
/// 1677-09-21 or after 2262-04-11, which the index holds at the nearest of those — the range its
/// nanosecond timestamps reach. The document keeps the value as written; searches and sorts see
/// the nearest date. A list is out of range when any of its values is.
pub fn date_out_of_range(value: &JsonValue) -> bool {
    match value {
        JsonValue::String(s) => {
            parse_date_str_to_tantivy(s).is_some_and(|(_, secs, clamped)| secs != clamped)
        }
        JsonValue::Array(items) => items.iter().any(date_out_of_range),
        _ => value.as_i64().is_some_and(|secs| {
            let (_, secs, clamped) = epoch_seconds_to_tantivy(secs);
            secs != clamped
        }),
    }
}

/// The epoch second a date field's fast column holds for this value, as the writer indexed it.
///
/// For ordering a merge the way each shard's column is ordered, so it reads every shape the
/// writer indexes: a string, whole seconds, and a list, whose *first* value is the one the column
/// sorts by. Reading strings alone left a date sent as epoch seconds without a key, and a hit
/// without a key sorts with the ones that have no date at all.
pub fn date_sort_secs(value: &JsonValue) -> Option<i64> {
    match value {
        JsonValue::String(s) => parse_date_to_timestamp_secs(s),
        JsonValue::Array(items) => items.first().and_then(date_sort_secs),
        _ => value.as_i64().map(|secs| epoch_seconds_to_tantivy(secs).2),
    }
}

pub(crate) fn parse_date_str_to_tantivy(s: &str) -> Option<(DateTime, i64, i64)> {
    // A timestamp with an offset: RFC 3339, or RFC 2822 (`Fri, 15 Mar 2024 16:13:13 +0000`)
    if let Ok(dt) =
        chrono::DateTime::parse_from_rfc3339(s).or_else(|_| chrono::DateTime::parse_from_rfc2822(s))
    {
        let ts = dt.timestamp();
        let clamped = ts.clamp(TANTIVY_MIN_TIMESTAMP_SECS, TANTIVY_MAX_TIMESTAMP_SECS);
        let tantivy_dt = DateTime::from_timestamp_secs(clamped);
        return Some((tantivy_dt, ts, clamped));
    }

    // A date or datetime without one, read as UTC: year first with `-`, `/` or `.`, year last
    // with slashes month first, dots day first or a named month (see `YEAR_LAST_FORMATS`), and
    // `YYYYMMDD`.
    if let Some(ndt) = parse_naive_datetime(s) {
        let ts = Utc.from_utc_datetime(&ndt).timestamp();
        let clamped = ts.clamp(TANTIVY_MIN_TIMESTAMP_SECS, TANTIVY_MAX_TIMESTAMP_SECS);
        let tantivy_dt = DateTime::from_timestamp_secs(clamped);
        return Some((tantivy_dt, ts, clamped));
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

/// Whether a stored schema row records a deletion rather than describing a live index.
///
/// A row that will not decode counts as live, so an index whose metadata cannot be read stays
/// in the listings rather than dropping out of them.
pub(crate) fn schema_records_a_deletion(bytes: &[u8]) -> bool {
    serde_json::from_slice::<IndexSchema>(bytes)
        .map(|schema| schema.state == SchemaState::Dropped)
        .unwrap_or(false)
}

/// One node's record of one index's schema, as drops are judged: its version, and whether it
/// records a drop. A schema and the summary [`SchemaRecord`] nodes trade are both judged by the
/// same rule, [`SchemaVersion::dropped_by`].
pub trait SchemaVersion {
    fn version(&self) -> u64;
    fn records_drop(&self) -> bool;

    /// Whether this is the index a drop recorded at version `dropped_at` removed: a live schema
    /// at or below it. A drop is recorded one above the schema it dropped, and an index created
    /// again over it is minted above the record, so nothing newer can be at or below it — and a
    /// node still holding such a schema missed the drop.
    fn dropped_by(&self, dropped_at: u64) -> bool {
        !self.records_drop() && self.version() <= dropped_at
    }
}

/// Several nodes' records of one index, with drops counted: see [`count_drops`].
#[derive(Debug)]
pub struct DropsCounted<N, S> {
    /// The highest drop recorded, `0` when none is.
    pub dropped_at: u64,
    /// Nodes holding a schema that drop removed: they were down for it, and have to finish it
    /// before anything is concluded from what they hold.
    pub missed: Vec<N>,
    /// Every other record, beside its node: live schemas the drops left standing, and the drops'
    /// own records.
    pub standing: Vec<(N, S)>,
}

/// Count the drops among nodes' records of one index. `dropped_at` is a drop known from
/// elsewhere — the asking node's own record — and `0` when there is none.
pub fn count_drops<N, S: SchemaVersion>(
    records: impl IntoIterator<Item = (N, S)>,
    dropped_at: u64,
) -> DropsCounted<N, S> {
    let records: Vec<(N, S)> = records.into_iter().collect();
    let dropped_at = records
        .iter()
        .filter(|(_, record)| record.records_drop())
        .map(|(_, record)| record.version())
        .fold(dropped_at, u64::max);
    let (missed, standing): (Vec<_>, Vec<_>) = records
        .into_iter()
        .partition(|(_, record)| record.dropped_by(dropped_at));
    DropsCounted {
        dropped_at,
        missed: missed.into_iter().map(|(node, _)| node).collect(),
        standing,
    }
}

/// What a node holds for one index, as nodes compare their records when they connect.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaRecord {
    pub index: String,
    pub version: u64,
    /// It records a drop.
    pub dropped: bool,
    pub thumbprint: u64,
}

impl SchemaRecord {
    pub fn of(index: String, schema: &IndexSchema) -> Self {
        Self {
            index,
            version: schema.version,
            dropped: schema.records_drop(),
            thumbprint: schema.calculate_fingerprint(),
        }
    }
}

impl SchemaVersion for SchemaRecord {
    fn version(&self) -> u64 {
        self.version
    }
    fn records_drop(&self) -> bool {
        self.dropped
    }
}

impl SchemaVersion for IndexSchema {
    fn version(&self) -> u64 {
        self.version
    }
    fn records_drop(&self) -> bool {
        self.state == SchemaState::Dropped
    }
}

impl<S: SchemaVersion> SchemaVersion for &S {
    fn version(&self) -> u64 {
        (**self).version()
    }
    fn records_drop(&self) -> bool {
        (**self).records_drop()
    }
}

/// Whether a schema describes a live index or records that one was dropped.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SchemaState {
    /// The index exists and answers.
    #[default]
    Active,
    /// The index and its schema were dropped.
    ///
    /// The row is kept so a write arriving after the drop reads that the index is gone,
    /// instead of finding nothing and typing the index from its own document — which would
    /// leave every field it discovers non-indexed, in a Tantivy column only a reindex can
    /// change.
    ///
    /// The fields are cleared, so nothing is left to inherit and the next write samples
    /// afresh. `version` is above the dropped schema's, so a write still carrying that schema
    /// cannot install it over this row.
    Dropped,
}

/// One way a schema change differs from the index already built. See
/// [`IndexSchema::rebuild_changes`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaChange {
    /// The change, as a person reads it: `score: i64 → f64`.
    pub what: String,
    /// The built index would act against the schema until rebuilt, rather than merely lack a
    /// column the schema declares.
    pub conflicts: bool,
}

impl SchemaChange {
    fn conflicting(what: String) -> Self {
        Self {
            what,
            conflicts: true,
        }
    }

    fn pending(what: String) -> Self {
        Self {
            what,
            conflicts: false,
        }
    }
}

/// The postings a text column keeps, by the name a schema declares: `Basic`, `WithFreqs`, and
/// anything else — absent or misspelled — the full `WithFreqsAndPositions`.
pub(crate) fn index_record_option(declared: Option<&str>) -> IndexRecordOption {
    match declared {
        Some("Basic") => IndexRecordOption::Basic,
        Some("WithFreqs") => IndexRecordOption::WithFreqs,
        _ => IndexRecordOption::WithFreqsAndPositions,
    }
}

/// How a text-like field's column analyses its values: the tokenizer and the postings it keeps.
/// A `string` field builds Tantivy's `STRING` — the raw tokenizer with `Basic` postings — whatever
/// it declares, so a `text` field declaring those two builds the same column. `None` for a field
/// that builds no text column.
fn text_analysis(field: &FieldDef) -> Option<(&str, IndexRecordOption)> {
    match field.field_type {
        TantivyFieldType::String => Some(("raw", IndexRecordOption::Basic)),
        TantivyFieldType::Text => Some((
            field.tokenizer.as_deref().unwrap_or("default"),
            index_record_option(field.index_record_option.as_deref()),
        )),
        _ => None,
    }
}

/// Index schema definition for validation and evolution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexSchema {
    pub fields: HashMap<String, FieldDef>,
    /// Whether this schema describes a live index or records that one was dropped.
    ///
    /// Defaulted, so a schema stored before this field existed reads back as `Active`.
    #[serde(default)]
    pub state: SchemaState,
    #[serde(default = "default_version")]
    pub version: u64,
    /// What this index holds, in the operator's words.
    ///
    /// The one thing a caller cannot work out from the schema: field names and types describe the
    /// shape of the data, not which dataset it is. Absent unless someone wrote one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Which tenant owns this index, for quota accounting.
    ///
    /// Stamped from the creating key's `tenant` when the index is created, and never rewritten
    /// afterwards: ownership is a fact about who made the index, not about who last wrote to
    /// it, and a field that moved with the last writer would let a tenant shed their own usage
    /// by having someone else write once.
    ///
    /// `None` on every index created before this field existed, and on every index created by a
    /// key with no tenant. Those count against nobody's budget, which is what makes this
    /// upgrade-safe: a node that gains the field does not suddenly find its existing indexes
    /// over a ceiling.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant: Option<String>,
    /// The fields an unqualified term searches, in priority order. `None` searches every
    /// indexed text, string and JSON field.
    ///
    /// Query-time only: the tantivy index is built the same whichever fields are listed, so
    /// declaring or changing this needs no reindex and takes effect on the next search. The
    /// node's `max_default_fields` still applies — a list longer than the cap is cut to its
    /// first entries — and so does the list's order when it is. See [`select_default_fields`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_fields: Option<Vec<String>>,
    /// Field name to use for routing/sharding (default: "id")
    #[serde(default = "default_routing_field")]
    pub routing_field_name: String,
    /// The source fields whose values, joined with `|` in this order, make a document's id.
    ///
    /// Recorded by the loader so that every load into the index keys its documents the same way:
    /// a load that forgot how the first one did keyed the rest by another field, and each of its
    /// rows overwrote whichever document happened to share that field's value. Empty on a schema
    /// that does not say, which is every schema written before this field existed.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub id_fields: Vec<String>,
}

impl Default for IndexSchema {
    fn default() -> Self {
        Self {
            fields: HashMap::new(),
            state: SchemaState::Active,
            version: 1,
            description: None,
            tenant: None,
            default_fields: None,
            routing_field_name: "id".to_string(),
            id_fields: Vec::new(),
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
            // itself: raw-tokenized, stored, fast-columned, whatever the schema declared. A
            // declared type is therefore fiction the rest of the engine goes on believing —
            // `describe_index` reports it, the slow write validation infers `Text` for the key
            // and refuses every document against an `i64` declaration, and a sort merge asked
            // to key a `date` field parses identifiers as dates, fails, and returns an
            // arbitrary order. Pinning both here is what keeps the schema and the index the
            // same shape, as `can_be_fast` does for the types that carry no column.
            if key == "id" {
                field_def.field_type = TantivyFieldType::Text;
                field_def.fast = Some(true);
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
    }

    /// Record that this schema just changed: advance the version.
    ///
    /// A cluster comparing `(version, fingerprint)` needs the version to actually advance on a
    /// local edit, or a node holding newer content cannot say so.
    ///
    /// Monotonic, not a count of operations: a change touching three fields may advance it three
    /// times, and nothing downstream depends on the step size — only on later being greater.
    ///
    /// Not called when *applying* a schema that arrived already agreed. That path stores the
    /// version it was given, because the whole point of an agreed version is that every node
    /// records the same one.
    pub(crate) fn mark_modified(&mut self) {
        self.version = self.version.saturating_add(1);
    }

    /// A hash of everything in this schema that decides how the index behaves.
    ///
    /// Computed on demand rather than stored, which is what keeps it honest: a value carried
    /// in the struct has to be recomputed everywhere `fields` changes, and it was not — the
    /// orchestrator's own evolution path never touched it, so a schema's fingerprint routinely
    /// described a shape it no longer had. At 724ns for a twenty-field schema there is nothing
    /// to save by caching it.
    ///
    /// This answers "are these the same schema?". It used to hash field *names* alone, which
    /// answered the much narrower "are these the same fields?" — and that is blind to the one
    /// disagreement a cluster most needs to see. Two nodes that had independently typed the same
    /// index, one holding `{amount: f64, label: text}` and the other `{amount: i64, label: i64}`,
    /// hashed identically, so any check built on this would have polled them and concluded they
    /// agreed. Types, flags and the routing field are hashed for that reason: a thumbprint that
    /// cannot see a divergence cannot be used to detect one.
    ///
    /// An index is still identified by its *name*, never by this hash. A hash used as a lookup
    /// key collides across indexes of the same shape — a monthly partition, a per-tenant index —
    /// and a reverse lookup that did exactly that handed one index's schema to another.
    ///
    /// **What is deliberately left out.** `version` is not hashed: the two are compared together
    /// as a pair, so a node whose content matches at a different version has to be able to
    /// recognise that, which it cannot do if the version is baked into the hash.
    ///
    /// **Why lengths rather than a separator.** The previous form separated names with NUL, on
    /// the grounds that no field name may contain one. Descriptions and tokenizer names are freer
    /// than field names, so every variable-length part is length-prefixed instead: unambiguous
    /// whatever bytes it holds, which a separator cannot promise once free text is in the hash.
    pub fn calculate_fingerprint(&self) -> u64 {
        // Hash the *effective* schema, not the declaration as written.
        //
        // Two nodes can hold the same schema and write it down differently. A node that has
        // only the declaration keeps `tokenizer: None` on a field; a node that has built the
        // Tantivy index reads back the tokenizer the engine actually chose, because
        // `get_schema_cached` merges the derived schema into the stored one. Same schema, two
        // spellings — and hashing them raw reported the pair as divergent forever, which is
        // exactly backwards for a value whose entire job is to answer "are these the same".
        // Observed between two nodes of a live cluster where one held a declared index with no
        // documents: `id` was `tokenizer: None` there and `"raw"` on the node that had written
        // to it.
        //
        // `normalize_after_deserialization` is the one definition of what a declaration
        // resolves to — the same one the index builder honours, `id`'s fixed attributes
        // included — so the fingerprint borrows it rather than restating the defaults and
        // drifting from them. This is the same reasoning `is_fast()` already applied to the
        // three-state `fast`, extended to every property that has a default.
        let mut effective = self.clone();
        effective.normalize_after_deserialization();
        effective.fingerprint_of_effective()
    }

    /// [`calculate_fingerprint`](Self::calculate_fingerprint) over an already-normalized schema.
    pub(crate) fn fingerprint_of_effective(&self) -> u64 {
        fn push_str(buf: &mut Vec<u8>, value: &str) {
            buf.extend_from_slice(&(value.len() as u64).to_le_bytes());
            buf.extend_from_slice(value.as_bytes());
        }
        fn push_opt(buf: &mut Vec<u8>, value: Option<&str>) {
            match value {
                None => buf.push(0),
                Some(text) => {
                    buf.push(1);
                    push_str(buf, text);
                }
            }
        }
        fn push_bool(buf: &mut Vec<u8>, value: bool) {
            buf.push(u8::from(value));
        }

        let mut sorted_names: Vec<&String> = self.fields.keys().collect();
        sorted_names.sort();

        let mut combined = Vec::new();
        for name in sorted_names {
            let Some(field) = self.fields.get(name) else {
                continue;
            };
            push_str(&mut combined, name);
            // The canonical lowercase name, not the enum discriminant: this hash is compared
            // between nodes that may be running different builds, and `to_string` is the spelling
            // the wire format and the query syntax are both keyed on. A discriminant would shift
            // under a variant reordering and report a false divergence across a rolling upgrade.
            push_str(&mut combined, field.field_type.to_string());
            push_bool(&mut combined, field.indexed);
            push_bool(&mut combined, field.stored);
            // The resolved answer, not the three-state declaration. `None` and an explicit
            // `Some(true)` mean the same thing on a numeric field, and hashing the raw option
            // would call two nodes divergent for having written the same intent differently.
            push_bool(&mut combined, field.is_fast());
            push_bool(&mut combined, field.is_shadow);
            push_opt(&mut combined, field.description.as_deref());
            push_opt(&mut combined, field.tokenizer.as_deref());
            push_opt(&mut combined, field.index_record_option.as_deref());
            // Only when set, so every schema without one keeps the thumbprint it had. A learned
            // field widens to take a value a declared one refuses, so two nodes differing here
            // differ in what they accept.
            if field.learned {
                combined.push(0x4C);
            }
        }

        // Index-level properties. The routing field earns its place here more than any type
        // does: two nodes that disagree about it route the same document to different shards.
        push_str(&mut combined, &self.routing_field_name);
        push_opt(&mut combined, self.description.as_deref());
        // Hashed only when declared, so every schema written before the field existed keeps the
        // fingerprint it had: an absent list adding a byte would read as a divergence between
        // an upgraded node and one that has not been, for a schema neither of them changed.
        if let Some(default_fields) = &self.default_fields {
            combined.push(0xD5);
            combined.extend_from_slice(&(default_fields.len() as u64).to_le_bytes());
            for name in default_fields {
                push_str(&mut combined, name);
            }
        }
        // Likewise only when recorded: two nodes keying the same index differently would write
        // one record under two ids.
        if !self.id_fields.is_empty() {
            combined.push(0x1D);
            combined.extend_from_slice(&(self.id_fields.len() as u64).to_le_bytes());
            for name in &self.id_fields {
                push_str(&mut combined, name);
            }
        }

        xxh3_64(&combined)
    }

    /// What changing to `next` would ask of a Tantivy index already built from this schema: each
    /// column added, dropped, retyped, re-tokenized, or made fast or not, and a different id.
    ///
    /// A built index keeps the columns it was built with, so every change listed here needs a
    /// rebuild to take effect — free while the index holds no documents. They differ in what the
    /// index does meanwhile. A [conflicting](SchemaChange::conflicts) one makes it act against
    /// the schema: a field retyped `i64` to `f64` refused every range query with a decimal in it
    /// and silently skipped every decimal written, a re-tokenized field analysed queries one way
    /// and documents another, a new id sat each record beside its old copy. The rest declare a
    /// column the index does not have yet, which the schema listing already reports as not
    /// searchable or sortable until a rebuild. Empty when the built index serves `next` as it is:
    /// a description, the default search fields, a type renamed over the same column. Both
    /// schemas are compared as they build, not as they are spelled: `tokenizer: None` and
    /// `"default"` on a text field are one declaration, and so are `string` and a `text` field
    /// with the raw tokenizer and `Basic` postings.
    pub fn rebuild_changes(&self, next: &IndexSchema) -> Vec<SchemaChange> {
        let mut current = self.clone();
        current.normalize_after_deserialization();
        let mut next = next.clone();
        next.normalize_after_deserialization();

        let built = |schema: &IndexSchema, name: &str| -> Option<FieldDef> {
            schema
                .fields
                .get(name)
                .filter(|f| f.indexed && !f.is_shadow)
                .cloned()
        };
        let mut names: Vec<&String> = current.fields.keys().chain(next.fields.keys()).collect();
        names.sort();
        names.dedup();

        let mut changes = Vec::new();
        let mut conflict = |what: String| changes.push(SchemaChange::conflicting(what));
        let mut pending = Vec::new();
        for name in names.into_iter().filter(|n| *n != "id") {
            match (built(&current, name), built(&next, name)) {
                (None, None) => {}
                (Some(_), None) => pending.push(format!("{name}: no longer indexed")),
                (None, Some(field)) => pending.push(format!(
                    "{name}: indexed as {}",
                    field.field_type.to_string()
                )),
                (Some(was), Some(now)) => {
                    let retyped = || {
                        format!(
                            "{name}: {} → {}",
                            was.field_type.to_string(),
                            now.field_type.to_string()
                        )
                    };
                    match (text_analysis(&was), text_analysis(&now)) {
                        // Both build a text column, and the column is the analysis: `string` and a
                        // raw, `Basic` `text` are one column under two names, so renaming it
                        // changes nothing the index holds.
                        (Some(a), Some(b)) if a == b => {}
                        (Some(_), Some(_)) if was.field_type != now.field_type => {
                            conflict(retyped())
                        }
                        (Some((was_tok, was_rec)), Some((now_tok, now_rec))) => {
                            if was_tok != now_tok {
                                conflict(format!("{name}: tokenizer {was_tok} → {now_tok}"));
                            }
                            if was_rec != now_rec {
                                conflict(format!(
                                    "{name}: index record option {was_rec:?} → {now_rec:?}"
                                ));
                            }
                        }
                        _ if was.field_type != now.field_type => conflict(retyped()),
                        _ => {}
                    }
                    if was.is_fast() != now.is_fast() {
                        pending.push(format!(
                            "{name}: fast {} → {}",
                            was.is_fast(),
                            now.is_fast()
                        ));
                    }
                }
            }
        }
        // An id recorded once and recorded differently now: documents already in the index are
        // keyed the old way, and a record loaded the new way would sit beside its old copy rather
        // than replace it. Recording an id where none was is not a change of key.
        if !current.id_fields.is_empty() && current.id_fields != next.id_fields {
            conflict(format!(
                "id: {} → {}",
                current.id_fields.join(","),
                if next.id_fields.is_empty() {
                    "none".to_string()
                } else {
                    next.id_fields.join(",")
                }
            ));
        }
        changes.extend(pending.into_iter().map(SchemaChange::pending));
        changes
    }

    /// Check a declared `default_fields` list against this schema.
    ///
    /// Every name must be a field this schema indexes as text, string or JSON — the types an
    /// unqualified term can match — and not a shadow. Refused rather than filtered: a list
    /// with a typo in it would otherwise quietly search fewer fields than the caller wrote. An
    /// empty list is refused too, since it reads as "search nothing" and would mean "search
    /// everything"; omitting the key is how to ask for every field.
    pub fn validate_default_fields(&self) -> Result<(), String> {
        let Some(list) = &self.default_fields else {
            return Ok(());
        };
        if list.is_empty() {
            return Err(
                "default_fields is empty; omit it to search every text field, or name at least one"
                    .to_string(),
            );
        }
        let mut seen = HashSet::new();
        for name in list {
            if !seen.insert(name.as_str()) {
                return Err(format!("default_fields names '{name}' twice"));
            }
            let Some(field) = self.fields.get(name) else {
                return Err(format!(
                    "default_fields names '{name}', which is not a field of this index"
                ));
            };
            if field.is_shadow || !field.indexed || !field.field_type.is_default_searchable() {
                return Err(format!(
                    "default_fields names '{name}', which an unqualified term cannot search: \
                     it must be an indexed text, string or json field"
                ));
            }
        }
        Ok(())
    }

    /// Check that every text field names a tokenizer this engine registers.
    ///
    /// Refused where it is declared, because nothing later would say so: an unknown name stores,
    /// its writes land in the WAL and answer success, and then every commit of the index fails
    /// because the writer cannot find the analyzer — the index is stuck, and a search finds
    /// nothing rather than erroring. A typo does this, and so does a name a newer build
    /// introduced, arriving at a node that predates it.
    ///
    /// Only `text` fields are checked, indexed or not: they are the only type the builder hands a
    /// tokenizer to, and a field declared unindexed can be switched on later without its
    /// tokenizer being looked at again.
    pub fn validate_tokenizers(&self) -> Result<(), String> {
        let mut unknown: Vec<(&str, &str)> = self
            .fields
            .iter()
            .filter(|(_, def)| def.field_type == TantivyFieldType::Text)
            .filter_map(|(name, def)| {
                def.tokenizer
                    .as_deref()
                    .filter(|tokenizer| !is_known_tokenizer(tokenizer))
                    .map(|tokenizer| (name.as_str(), tokenizer))
            })
            .collect();
        // Sorted for the same reason as descriptions: an error naming a different field on each
        // attempt is one an operator cannot work through.
        unknown.sort();
        let Some((name, tokenizer)) = unknown.first() else {
            return Ok(());
        };
        let and_others = match unknown.len() - 1 {
            0 => String::new(),
            1 => " (and 1 other field)".to_string(),
            n => format!(" (and {n} other fields)"),
        };
        Err(format!(
            "field '{name}' names tokenizer '{tokenizer}', which this node does not have{and_others}; \
             available: {}",
            TOKENIZERS.join(", ")
        ))
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

    /// Whether any field is a shadow field. Read from the fields themselves: a set kept beside
    /// them had to be rebuilt after every deserialization, and a schema adopted from a peer
    /// skipped that and filtered nothing.
    pub fn has_shadow_fields(&self) -> bool {
        self.fields.values().any(|field| field.is_shadow)
    }

    /// The shadow fields' names, sorted, so anything walking them does so in one order.
    pub fn shadow_names(&self) -> Vec<&String> {
        let mut names: Vec<&String> = self
            .fields
            .iter()
            .filter(|(_, field)| field.is_shadow)
            .map(|(name, _)| name)
            .collect();
        names.sort_unstable();
        names
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

    /// Record what a written value says about its field, by the same rules the node's write
    /// path follows: a field the schema lacks is added [learned](FieldDef::learned) and
    /// non-indexed; a learned field without a column widens to hold the value
    /// ([`TantivyFieldType::widened`]); a declared field keeps its type, and an indexed one is
    /// pinned to the column built for it. The version is left alone. Whether anything changed.
    ///
    /// The storage-level net: the node persists what a write teaches before the write reaches a
    /// shard, so on a node this adds only what that path did not, and agrees with it.
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

                // An indexed field's type is pinned to the column the index built for it.
                // Evolving it would write the new type against the old column, and Tantivy
                // silently skips a value that does not match the column — the document would be
                // stored in redb and never indexed. A type change goes through a schema edit and
                // a rebuild instead; the inline path never performs one.
                if current_def.indexed {
                    return false;
                }

                // A field a write added has no column and no declared type: it widens to hold
                // the value, and never narrows — `text` refined to `i64` here would refuse the
                // text values that made it text. A declared field keeps the type it was given.
                if !current_def.learned {
                    return false;
                }
                let widened = current_def.field_type.widened(&inferred_type);
                if widened == current_def.field_type {
                    return false;
                }
                let mut new_def = current_def.clone();
                new_def.retype_learned(widened);
                entry.insert(new_def);
                true
            }
        };

        // The version is the cluster's to advance: a field one shard or one node learned is
        // agreed, at one new version, by the round that follows it.
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
        self.fields.insert(name, field_def);
        self.mark_modified();
        true
    }

    /// Check if a field is a shadow field — O(1) via pre-computed set
    pub fn is_shadow_field(&self, field_name: &str) -> bool {
        self.fields
            .get(field_name)
            .is_some_and(|field| field.is_shadow)
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
            self.mark_modified();
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

/// A facet path Tantivy will accept, or the reason it will not.
///
/// **The panic this exists to prevent is not hypothetical machinery.** `Facet: From<&str>` is
/// `Facet::from_text(path).unwrap()`, and `from_text` refuses a value that is empty or does not
/// begin with `/`. So `add_facet(field, "electronics/phones")` panics — on the shard's writer
/// thread, from a document body, with `panic = "abort"` in the release profile, which takes the
/// process down rather than the request.
///
/// The validator now accepts a facet path for a declared `Facet` field (ROADMAP OB2 / J1), so the
/// write path can reach this with a caller-supplied value. That is exactly why the check lives
/// here rather than relying on the orchestrator to refuse the type: the value is judged at the
/// point it enters the index, so a bad path is refused by name instead of panicking the writer
/// thread, whatever the validator accepted.
///
/// Delegates to `from_text` rather than checking the shape by hand: escaping (`\/` for a literal
/// slash inside a segment) is its rule to define, and a second implementation of it here would be
/// a second implementation to drift.
pub(crate) fn facet_value(field: &str, value: &str) -> Result<Facet, StoreError> {
    Facet::from_text(value).map_err(|_| StoreError::InvalidFieldValue {
        field: field.to_string(),
        reason: bad_facet_path(value),
    })
}

/// Why this string is not a facet path, if it is not.
///
/// Public so that the write path's validator can refuse a bad path before any shard is asked,
/// against the same parser that would refuse it here. Two readings of what a facet path is
/// would be one reading too many: the validator waving through what the writer skips is how a
/// document ends up stored with a field silently unindexed.
pub fn facet_path_error(value: &str) -> Option<String> {
    Facet::from_text(value).err().map(|_| bad_facet_path(value))
}

/// Why this element of a byte array is not a byte, if it is not.
///
/// Public for the same reason `facet_path_error` is: the validator refuses it before a shard is
/// asked, against the same rule the writer applies. Reading it with `as u64 as u8` instead
/// truncated silently — 300 was stored as 44 — and dropped anything that was not a number at
/// all, leaving the field unindexed with nothing said.
pub fn byte_value_error(value: &JsonValue) -> Option<String> {
    match value.as_u64().and_then(|n| u8::try_from(n).ok()) {
        Some(_) => None,
        None => Some(not_a_byte(value)),
    }
}

/// The one wording for an element a byte array cannot hold.
pub(crate) fn not_a_byte(value: &JsonValue) -> String {
    format!("{value} is not a byte; every element of a bytes field is a whole number 0-255")
}

/// The one wording for a value that is not a facet path.
pub(crate) fn bad_facet_path(value: &str) -> String {
    format!(
        "'{value}' is not a facet path; a facet path begins with '/' and names its levels in \
         order, as in '/electronics/phones'. Escape a literal slash inside a level as '\\/'"
    )
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
pub(crate) struct StoredDoc<'a> {
    pub(crate) json_blob: Option<&'a JsonValue>,
}

/// Owned version for deserialization from redb
#[derive(Serialize, Deserialize)]
pub(crate) struct StoredDocOwned {
    pub(crate) json_blob: Option<JsonValue>,
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
        .shadow_names()
        .first()
        .map(|name| name.to_string())
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
pub(crate) fn reconstruct_shadow_fields_owned(
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
    if !schema.has_shadow_fields() {
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
    let shadow_names = schema.shadow_names();

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
pub(crate) fn filter_shadow_fields_owned(
    mut json_blob: JsonValue,
    schema: &IndexSchema,
) -> JsonValue {
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
    pub(crate) id: Field,
    /// Tantivy field for the WAL sequence number, on indices that were built with one.
    ///
    /// `None` on anything built after the field stopped being declared. It exists only to let
    /// `get_highest_indexed_seq` locate a checkpoint by scanning, and the commit payload does
    /// that in O(1) now — so it is written when present, purely so an index built by an older
    /// build keeps the column its own last-resort scan would read.
    pub(crate) seq: Option<Field>,
    /// Map of schema field name -> Tantivy field (only indexed fields are present)
    pub(crate) indexed_fields: HashMap<String, Field>,
    /// The kind of value each of those columns was built to take, read from the index itself.
    ///
    /// The stored schema declares a type; this is what the column is. The two can disagree —
    /// an edit waiting for its rebuild, or a schema that should never have been saved — and a
    /// value added under the declaration rather than the column reaches Tantivy's indexing
    /// thread as the wrong type and kills the writer. See [`SchemaFields::write_type`].
    pub(crate) built_types: HashMap<String, tantivy::schema::Type>,
}

impl SchemaFields {
    /// The type a value of `field` is added under: the declaration when the built column takes
    /// that kind of value, and the column's own kind when it does not. See [`writable_type`].
    pub(crate) fn write_type(&self, field: &str, declared: &TantivyFieldType) -> TantivyFieldType {
        match self.built_types.get(field) {
            Some(built) => writable_type(declared, *built),
            None => declared.clone(),
        }
    }

    /// The built kind of every indexed column, from the index's own schema.
    pub(crate) fn built_types_of(
        schema: &tantivy::schema::Schema,
        indexed_fields: &HashMap<String, Field>,
    ) -> HashMap<String, tantivy::schema::Type> {
        indexed_fields
            .iter()
            .map(|(name, field)| {
                (
                    name.clone(),
                    schema.get_field_entry(*field).field_type().value_type(),
                )
            })
            .collect()
    }
}

/// The type to add a value under, given the kind of value its column was built to take.
///
/// **The built column decides, not the declaration.** Tantivy fixes a column's type when the
/// index is built, and its indexing thread refuses a value of any other kind by failing — which
/// kills the `IndexWriter`, so every later write to that index on the shard fails too, and
/// nothing buffered since the last commit is ever committed. A stored schema that disagrees with
/// its column is not supposed to exist, but it has: a schema re-minted from a later batch's
/// sample typed a built date column `I64`, and one integer killed the writer. Adding by the
/// column makes that state harmless. A value the column cannot hold is skipped, the same as any
/// value `add_json_value_to_doc` cannot convert, and the declaration still governs validation.
///
/// The text-like declarations all add text, so any of them fits a text column.
pub(crate) fn writable_type(
    declared: &TantivyFieldType,
    built: tantivy::schema::Type,
) -> TantivyFieldType {
    use tantivy::schema::Type;
    let fits = matches!(
        (declared, built),
        (
            TantivyFieldType::Text | TantivyFieldType::String | TantivyFieldType::Json,
            Type::Str
        ) | (TantivyFieldType::Json, Type::Json)
            | (TantivyFieldType::I64, Type::I64)
            | (TantivyFieldType::U64, Type::U64)
            | (TantivyFieldType::F64, Type::F64)
            | (TantivyFieldType::Date, Type::Date)
            | (TantivyFieldType::Boolean, Type::Bool)
            | (TantivyFieldType::Bytes, Type::Bytes)
            | (TantivyFieldType::Ip, Type::IpAddr)
            | (TantivyFieldType::Facet, Type::Facet)
    );
    if fits {
        return declared.clone();
    }
    match built {
        Type::Str => TantivyFieldType::Text,
        Type::Json => TantivyFieldType::Json,
        Type::I64 => TantivyFieldType::I64,
        Type::U64 => TantivyFieldType::U64,
        Type::F64 => TantivyFieldType::F64,
        Type::Date => TantivyFieldType::Date,
        Type::Bool => TantivyFieldType::Boolean,
        Type::Bytes => TantivyFieldType::Bytes,
        Type::IpAddr => TantivyFieldType::Ip,
        Type::Facet => TantivyFieldType::Facet,
    }
}

/// The fields an unqualified term searches, and whether the cap cut the set short.
///
/// `candidates` are the fields the built index can search this way. With a declared list, it is
/// the list's names that are among them, in the list's order. Without one, it is every candidate
/// **sorted by name**. Then, when `max` is non-zero, the first `max`.
///
/// By name because every shard must pick the same fields: a shard's own field order comes from
/// iterating a hash map when its index was built, so two shards of one index can hold them in
/// different orders, and taking "the first 64" in that order would have each shard search a
/// different 64. Alphabetical is arbitrary, but it is the same arbitrary everywhere and a caller
/// can predict it. A declared list is how to choose instead.
pub fn select_default_fields(
    candidates: impl IntoIterator<Item = String>,
    declared: Option<&[String]>,
    max: usize,
) -> (Vec<String>, bool) {
    let candidates: HashSet<String> = candidates.into_iter().collect();
    let mut selected: Vec<String> = match declared {
        Some(list) => list
            .iter()
            .filter(|name| candidates.contains(*name))
            .cloned()
            .collect(),
        None => {
            let mut all: Vec<String> = candidates.into_iter().collect();
            all.sort();
            all
        }
    };
    let truncated = max > 0 && selected.len() > max;
    if truncated {
        selected.truncate(max);
    }
    (selected, truncated)
}
