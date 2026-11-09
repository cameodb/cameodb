//! The lowest and highest value of a field over the documents a query matches (ROADMAP K1).
//!
//! Read from the field's fast column, segment by segment, with the query's own matching set:
//! the same parse, the same discarded clauses, the same "nothing survived means nothing
//! matched" rule a search applies. Not Tantivy's aggregation API, which answers every metric
//! as an `f64` — an `i64` or `u64` past 2⁵³ comes back wrong, and a date as nanoseconds in a
//! float. The column holds the exact value in its own type, so that is what is kept: an
//! integer extreme is exact at any magnitude, and a date is rendered as RFC 3339.
//!
//! Not `Column::min_value()`/`max_value()` either, which need no scan and answer a different
//! question: what the column ever held, deleted documents and the query both ignored.

use std::cmp::Ordering;
use std::collections::BTreeMap;

use chrono::{SecondsFormat, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as JsonValue};
use tantivy::columnar::Column;
use tantivy::schema::{FieldType, Schema};
use tantivy::{DateTime, DocId, Score, SegmentReader};

use crate::StoreError;

/// Which fields a search measures: `{"min": [...], "max": [...]}`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetricsSpec {
    /// Fields whose lowest value is wanted.
    #[serde(default)]
    pub min: Vec<String>,
    /// Fields whose highest value is wanted.
    #[serde(default)]
    pub max: Vec<String>,
}

impl MetricsSpec {
    /// Every field named, once, in the order first named.
    pub fn fields(&self) -> Vec<&str> {
        let mut fields: Vec<&str> = Vec::new();
        for name in self.min.iter().chain(&self.max) {
            if !fields.contains(&name.as_str()) {
                fields.push(name);
            }
        }
        fields
    }
}

/// One extreme, in the type of the column it was read from.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "lowercase")]
pub enum MetricValue {
    I64(i64),
    U64(u64),
    F64(f64),
    /// Seconds since the epoch, the precision every date is written at.
    Date(i64),
}

impl MetricValue {
    /// The order of two values of one field. Values of one field share a type; across types —
    /// which only a schema that disagrees between shards could produce — they compare as
    /// numbers, exactly while both are integers.
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::I64(a), Self::I64(b)) | (Self::Date(a), Self::Date(b)) => a.cmp(b),
            (Self::U64(a), Self::U64(b)) => a.cmp(b),
            (Self::F64(a), Self::F64(b)) => a.total_cmp(b),
            _ => match (self.as_i128(), other.as_i128()) {
                (Some(a), Some(b)) => a.cmp(&b),
                _ => self.as_f64().total_cmp(&other.as_f64()),
            },
        }
    }

    fn as_i128(&self) -> Option<i128> {
        match *self {
            Self::I64(v) | Self::Date(v) => Some(i128::from(v)),
            Self::U64(v) => Some(i128::from(v)),
            Self::F64(_) => None,
        }
    }

    fn as_f64(&self) -> f64 {
        match *self {
            Self::I64(v) | Self::Date(v) => v as f64,
            Self::U64(v) => v as f64,
            Self::F64(v) => v,
        }
    }

    /// The value as a caller reads it: a JSON number, or an RFC 3339 string for a date.
    pub fn to_json(&self) -> JsonValue {
        match *self {
            Self::I64(v) => JsonValue::from(v),
            Self::U64(v) => JsonValue::from(v),
            Self::F64(v) => serde_json::Number::from_f64(v)
                .map(JsonValue::Number)
                .unwrap_or(JsonValue::Null),
            Self::Date(secs) => Utc
                .timestamp_opt(secs, 0)
                .single()
                .map(|at| JsonValue::String(at.to_rfc3339_opts(SecondsFormat::Secs, true)))
                .unwrap_or(JsonValue::Null),
        }
    }
}

/// The lowest and highest value seen of one field.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct Extremes {
    min: Option<MetricValue>,
    max: Option<MetricValue>,
}

impl Extremes {
    fn offer(&mut self, value: MetricValue) {
        if self.min.is_none_or(|min| value.cmp(&min) == Ordering::Less) {
            self.min = Some(value);
        }
        if self
            .max
            .is_none_or(|max| value.cmp(&max) == Ordering::Greater)
        {
            self.max = Some(value);
        }
    }

    fn merge(&mut self, other: &Extremes) {
        for value in [other.min, other.max].into_iter().flatten() {
            self.offer(value);
        }
    }
}

/// What a search measured: per field asked for, its extreme over the matching documents, or
/// `None` when no matching document holds a value — no minimum, which is not `0`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct FieldMetrics {
    pub min: BTreeMap<String, Option<MetricValue>>,
    pub max: BTreeMap<String, Option<MetricValue>>,
}

impl FieldMetrics {
    /// The answer for documents that hold no value at all: every field asked for, `None`.
    pub fn nothing(spec: &MetricsSpec) -> Self {
        let none = |names: &[String]| names.iter().map(|name| (name.clone(), None)).collect();
        Self {
            min: none(&spec.min),
            max: none(&spec.max),
        }
    }

    /// Take in another shard's answer to the same request: the lowest of the lows, the highest
    /// of the highs, and a shard that matched nothing changing neither.
    pub fn merge(&mut self, other: &FieldMetrics) {
        merge_side(&mut self.min, &other.min, Ordering::Less);
        merge_side(&mut self.max, &other.max, Ordering::Greater);
    }

    /// `{"min": {field: value | null}, "max": {...}}`, each side present only when asked for.
    pub fn to_json(&self) -> JsonValue {
        let side = |values: &BTreeMap<String, Option<MetricValue>>| -> JsonValue {
            JsonValue::Object(
                values
                    .iter()
                    .map(|(field, value)| {
                        (
                            field.clone(),
                            value.map_or(JsonValue::Null, |v| v.to_json()),
                        )
                    })
                    .collect(),
            )
        };
        let mut out = Map::new();
        if !self.min.is_empty() {
            out.insert("min".into(), side(&self.min));
        }
        if !self.max.is_empty() {
            out.insert("max".into(), side(&self.max));
        }
        JsonValue::Object(out)
    }
}

fn merge_side(
    into: &mut BTreeMap<String, Option<MetricValue>>,
    from: &BTreeMap<String, Option<MetricValue>>,
    keep: Ordering,
) {
    for (field, value) in from {
        let slot = into.entry(field.clone()).or_insert(None);
        if let Some(value) = value
            && slot.is_none_or(|held| value.cmp(&held) == keep)
        {
            *slot = Some(*value);
        }
    }
}

/// Merge one node's rendered `metrics` into another's, as the cluster merges node answers.
///
/// Node answers cross the wire as the JSON a caller reads, so the merge reads that: numbers
/// compare exactly while both are integers, dates by the instant their RFC 3339 names, and a
/// `null` — a node that matched nothing — changes nothing.
pub fn merge_metrics_json(into: &mut JsonValue, from: &JsonValue) {
    let (Some(into), Some(from)) = (into.as_object_mut(), from.as_object()) else {
        return;
    };
    for (side, keep) in [("min", Ordering::Less), ("max", Ordering::Greater)] {
        let Some(from_side) = from.get(side).and_then(JsonValue::as_object) else {
            continue;
        };
        let into_side = into
            .entry(side)
            .or_insert_with(|| JsonValue::Object(Map::new()));
        let Some(into_side) = into_side.as_object_mut() else {
            continue;
        };
        for (field, value) in from_side {
            let slot = into_side.entry(field.clone()).or_insert(JsonValue::Null);
            if !value.is_null() && (slot.is_null() || compare_rendered(value, slot) == Some(keep)) {
                *slot = value.clone();
            }
        }
    }
}

/// The order of two rendered extremes of one field; `None` when they cannot be compared.
fn compare_rendered(a: &JsonValue, b: &JsonValue) -> Option<Ordering> {
    match (a, b) {
        (JsonValue::Number(a), JsonValue::Number(b)) => {
            let exact = |n: &serde_json::Number| {
                n.as_i64()
                    .map(i128::from)
                    .or_else(|| n.as_u64().map(i128::from))
            };
            match (exact(a), exact(b)) {
                (Some(a), Some(b)) => Some(a.cmp(&b)),
                _ => Some(a.as_f64()?.total_cmp(&b.as_f64()?)),
            }
        }
        (JsonValue::String(a), JsonValue::String(b)) => {
            match (
                chrono::DateTime::parse_from_rfc3339(a),
                chrono::DateTime::parse_from_rfc3339(b),
            ) {
                (Ok(a), Ok(b)) => Some(a.cmp(&b)),
                _ => Some(a.cmp(b)),
            }
        }
        _ => None,
    }
}

/// The column type a metric reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ColumnKind {
    I64,
    U64,
    F64,
    Date,
}

/// Collects the extremes of `fields` over the documents a query matches.
pub(crate) struct MetricsCollector {
    fields: Vec<(String, ColumnKind)>,
    spec: MetricsSpec,
}

impl MetricsCollector {
    /// The collector for `spec` over an index built with `schema`, or the refusal for a field
    /// it cannot measure: one the index does not have, one with no fast column, one whose type
    /// has no order a min or max means anything in.
    pub(crate) fn new(schema: &Schema, spec: &MetricsSpec) -> Result<Self, StoreError> {
        let mut fields = Vec::new();
        for name in spec.fields() {
            let field = schema
                .get_field(name)
                .map_err(|_| StoreError::FieldNotFound(name.to_string()))?;
            let entry = schema.get_field_entry(field);
            let kind = match entry.field_type() {
                FieldType::I64(_) => ColumnKind::I64,
                FieldType::U64(_) => ColumnKind::U64,
                FieldType::F64(_) => ColumnKind::F64,
                FieldType::Date(_) => ColumnKind::Date,
                other => {
                    return Err(invalid(format!(
                        "Field '{name}' is of type {:?}; min and max apply to i64, u64, f64 and \
                         date fields",
                        other.value_type()
                    )));
                }
            };
            if !entry.is_fast() {
                return Err(invalid(format!(
                    "Field '{name}' has no fast column, so it has no min or max: declare it \
                     fast, which takes effect when the index is built"
                )));
            }
            fields.push((name.to_string(), kind));
        }
        Ok(Self {
            fields,
            spec: spec.clone(),
        })
    }

    /// The extremes collected, as the request asked for them.
    pub(crate) fn metrics_from(&self, extremes: Vec<Extremes>) -> FieldMetrics {
        let by_field: BTreeMap<&str, Extremes> = self
            .fields
            .iter()
            .map(|(name, _)| name.as_str())
            .zip(extremes)
            .collect();
        let pick = |names: &[String], side: fn(&Extremes) -> Option<MetricValue>| {
            names
                .iter()
                .map(|name| (name.clone(), by_field.get(name.as_str()).and_then(side)))
                .collect()
        };
        FieldMetrics {
            min: pick(&self.spec.min, |e| e.min),
            max: pick(&self.spec.max, |e| e.max),
        }
    }
}

fn invalid(message: String) -> StoreError {
    StoreError::Io(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        message,
    ))
}

/// One segment's columns of the measured fields.
enum TypedColumn {
    I64(Column<i64>),
    U64(Column<u64>),
    F64(Column<f64>),
    Date(Column<DateTime>),
}

pub(crate) struct SegmentMetrics {
    columns: Vec<TypedColumn>,
    extremes: Vec<Extremes>,
}

impl tantivy::collector::Collector for MetricsCollector {
    type Fruit = Vec<Extremes>;
    type Child = SegmentMetrics;

    fn for_segment(
        &self,
        _segment_local_id: u32,
        segment: &SegmentReader,
    ) -> tantivy::Result<SegmentMetrics> {
        let fast = segment.fast_fields();
        let columns = self
            .fields
            .iter()
            .map(|(name, kind)| {
                Ok(match kind {
                    ColumnKind::I64 => TypedColumn::I64(fast.i64(name)?),
                    ColumnKind::U64 => TypedColumn::U64(fast.u64(name)?),
                    ColumnKind::F64 => TypedColumn::F64(fast.f64(name)?),
                    ColumnKind::Date => TypedColumn::Date(fast.date(name)?),
                })
            })
            .collect::<tantivy::Result<Vec<_>>>()?;
        Ok(SegmentMetrics {
            extremes: vec![Extremes::default(); columns.len()],
            columns,
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(&self, segment_fruits: Vec<Vec<Extremes>>) -> tantivy::Result<Vec<Extremes>> {
        let mut merged = vec![Extremes::default(); self.fields.len()];
        for fruit in &segment_fruits {
            for (into, from) in merged.iter_mut().zip(fruit) {
                into.merge(from);
            }
        }
        Ok(merged)
    }
}

impl tantivy::collector::SegmentCollector for SegmentMetrics {
    type Fruit = Vec<Extremes>;

    fn collect(&mut self, doc: DocId, _score: Score) {
        // Every value of a document counts, so an array field's extreme is over its elements.
        for (column, extremes) in self.columns.iter().zip(self.extremes.iter_mut()) {
            match column {
                TypedColumn::I64(c) => c
                    .values_for_doc(doc)
                    .for_each(|v| extremes.offer(MetricValue::I64(v))),
                TypedColumn::U64(c) => c
                    .values_for_doc(doc)
                    .for_each(|v| extremes.offer(MetricValue::U64(v))),
                // A value JSON cannot carry (NaN, an infinity) is not an extreme anyone can be
                // answered with.
                TypedColumn::F64(c) => c
                    .values_for_doc(doc)
                    .filter(|v| v.is_finite())
                    .for_each(|v| extremes.offer(MetricValue::F64(v))),
                TypedColumn::Date(c) => c
                    .values_for_doc(doc)
                    .for_each(|v| extremes.offer(MetricValue::Date(v.into_timestamp_secs()))),
            }
        }
    }

    fn harvest(self) -> Vec<Extremes> {
        self.extremes
    }
}
