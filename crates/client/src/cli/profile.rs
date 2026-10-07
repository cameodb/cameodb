//! What a scan of a source says about each of its columns: which kinds of value it holds, how
//! many distinct ones, whether its values could identify a row — and from that, the field each
//! column becomes.
//!
//! Every question here is about a column, not a cell, and is answered from all the values the
//! scan saw. Typing a field from one value at a time was how a load went wrong: the schema took
//! whatever the first `true` or `5` suggested, and every later value that did not fit was refused
//! by the node, row by row.

use super::*;
use anyhow::{Result, anyhow};
use serde_json::Map as JsonMap;
use serde_json::Value as JsonValue;
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::time::Duration;
use storage::{FieldDef, TantivyFieldType};

/// Where in the source a value was read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Location {
    /// A file line, as `sed -n <N>p` counts it.
    Line(u64),
    /// The Nth document of a JSON source.
    Document(u64),
    /// A record found by jumping into the file, at about this byte.
    Offset(u64),
}

impl fmt::Display for Location {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Location::Line(n) => write!(f, "line {}", grouped(*n)),
            Location::Document(n) => write!(f, "document {}", grouped(*n)),
            Location::Offset(n) => write!(f, "byte {}", grouped(*n)),
        }
    }
}

/// A count written with thousands separators: `5,168,255`.
pub(crate) fn grouped(n: u64) -> String {
    let digits = n.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (i, c) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(c);
    }
    out
}

/// The kinds of value a cell can hold, as far as typing its field is concerned.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Shape {
    /// Blank, or JSON `null`.
    Empty,
    /// `NA`, `n/a`, `null` and their kind: no value, except in a text column.
    Missing,
    /// `true` or `false` in any case, or a JSON boolean.
    Boolean,
    /// `yes`, `no`, `y`, `n`: a boolean only in a column that also says `true` or `false`.
    BooleanWord,
    Integer,
    /// `007`: a code, not the number 7.
    LeadingZero,
    /// More digits than a double holds exactly.
    LongInteger,
    Decimal,
    /// `8023954622E7`: a number, or an identifier a spreadsheet rewrote as one.
    Exponent,
    Date,
    Ip,
    /// A list written into the value, `['a', 'b']`, or a JSON array.
    List,
    Object,
    Text,
}

const SHAPES: [Shape; 14] = [
    Shape::Empty,
    Shape::Missing,
    Shape::Boolean,
    Shape::BooleanWord,
    Shape::Integer,
    Shape::LeadingZero,
    Shape::LongInteger,
    Shape::Decimal,
    Shape::Exponent,
    Shape::Date,
    Shape::Ip,
    Shape::List,
    Shape::Object,
    Shape::Text,
];

impl Shape {
    fn index(self) -> usize {
        self as usize
    }

    fn label(self) -> &'static str {
        match self {
            Shape::Empty => "empty",
            Shape::Missing => "missing",
            Shape::Boolean => "boolean",
            Shape::BooleanWord => "yes/no",
            Shape::Integer => "integer",
            Shape::LeadingZero => "leading-zero number",
            Shape::LongInteger => "long number",
            Shape::Decimal => "decimal",
            Shape::Exponent => "exponent number",
            Shape::Date => "date",
            Shape::Ip => "ip",
            Shape::List => "list",
            Shape::Object => "object",
            Shape::Text => "text",
        }
    }

    /// Whether the value is one: an empty or missing cell holds nothing.
    fn is_filled(self) -> bool {
        !matches!(self, Shape::Empty | Shape::Missing)
    }

    /// Whether a value of this kind could be a document's key.
    fn is_keyable(self) -> bool {
        matches!(
            self,
            Shape::Integer
                | Shape::LeadingZero
                | Shape::LongInteger
                | Shape::Exponent
                | Shape::Text
                | Shape::Date
                | Shape::Ip
        )
    }
}

/// Integers past this many digits are past what a double holds exactly.
const EXACT_DIGITS: usize = 15;

/// A text value's kind.
pub(crate) fn classify_text(trimmed: &str) -> Shape {
    if trimmed.is_empty() {
        return Shape::Empty;
    }
    if is_missing_marker(trimmed) {
        return Shape::Missing;
    }
    match trimmed.to_ascii_lowercase().as_str() {
        "true" | "false" => return Shape::Boolean,
        "yes" | "no" | "y" | "n" => return Shape::BooleanWord,
        _ => {}
    }
    if trimmed.starts_with('[') && parse_list(trimmed).is_some() {
        return Shape::List;
    }
    if let Some(shape) = number_shape(trimmed) {
        return shape;
    }
    // A date or an address has digits in it; anything without one is text, and cheaply so.
    if trimmed.bytes().any(|b| b.is_ascii_digit()) {
        match FieldDef::infer_type_from_value(&JsonValue::String(trimmed.to_string())) {
            TantivyFieldType::Date => return Shape::Date,
            TantivyFieldType::Ip => return Shape::Ip,
            _ => {}
        }
        // Written the other way round from how the node reads it, still a date.
        let either_way = DateOrders {
            slash: DateOrder::DayFirst,
            dot: DateOrder::MonthFirst,
        };
        if reordered_date(trimmed, &either_way).is_some() {
            return Shape::Date;
        }
    }
    Shape::Text
}

fn number_shape(t: &str) -> Option<Shape> {
    let digits = t.strip_prefix(['+', '-']).unwrap_or(t);
    if digits.is_empty() {
        return None;
    }
    if digits.bytes().all(|b| b.is_ascii_digit()) {
        return Some(if digits.len() > 1 && digits.starts_with('0') {
            Shape::LeadingZero
        } else if digits.len() > EXACT_DIGITS {
            Shape::LongInteger
        } else {
            Shape::Integer
        });
    }
    let numeric_chars = digits
        .bytes()
        .all(|b| b.is_ascii_digit() || matches!(b, b'.' | b'e' | b'E' | b'+' | b'-'));
    if numeric_chars
        && digits
            .bytes()
            .next()
            .is_some_and(|b| b.is_ascii_digit() || b == b'.')
        && t.parse::<f64>().is_ok_and(f64::is_finite)
    {
        return Some(if digits.contains(['e', 'E']) {
            Shape::Exponent
        } else {
            Shape::Decimal
        });
    }
    None
}

/// A JSON value's kind. A string is read the way a CSV cell is, so `"12"` is a number and
/// `"NA"` no value — the loader converts both to fit the field.
pub(crate) fn classify_json(value: &JsonValue) -> Shape {
    match value {
        JsonValue::Null => Shape::Empty,
        JsonValue::Bool(_) => Shape::Boolean,
        JsonValue::Number(n) if n.is_f64() => Shape::Decimal,
        JsonValue::Number(n) => number_shape(&n.to_string()).unwrap_or(Shape::Integer),
        JsonValue::String(s) => classify_text(s.trim()),
        JsonValue::Array(_) => Shape::List,
        JsonValue::Object(_) => Shape::Object,
    }
}

/// A list written into one value: `['a', 'b']` as Python prints one, `["a","b"]` as JSON does,
/// `[1, 2]`, or `[]`. Elements are scalars; anything that is not a list of them is `None`, so a
/// bracketed note such as `[draft]` stays text.
pub(crate) fn parse_list(t: &str) -> Option<Vec<JsonValue>> {
    let inner = t.strip_prefix('[')?.strip_suffix(']')?;
    if let Ok(JsonValue::Array(items)) = serde_json::from_str::<JsonValue>(t) {
        return items
            .iter()
            .all(|item| !item.is_array() && !item.is_object())
            .then_some(items);
    }
    let mut items = Vec::new();
    let mut chars = inner.chars().peekable();
    loop {
        while chars.next_if(|c| c.is_whitespace()).is_some() {}
        let Some(&first) = chars.peek() else {
            break;
        };
        if first == '\'' || first == '"' {
            chars.next();
            let mut value = String::new();
            loop {
                match chars.next()? {
                    '\\' => value.push(chars.next()?),
                    c if c == first => break,
                    c => value.push(c),
                }
            }
            items.push(JsonValue::String(value));
        } else {
            let mut token = String::new();
            while let Some(c) = chars.next_if(|c| *c != ',') {
                token.push(c);
            }
            let token = token.trim();
            items.push(match token {
                "None" | "null" => JsonValue::Null,
                "True" | "true" => JsonValue::Bool(true),
                "False" | "false" => JsonValue::Bool(false),
                _ => serde_json::from_str::<serde_json::Number>(token)
                    .ok()
                    .map(JsonValue::Number)?,
            });
        }
        while chars.next_if(|c| c.is_whitespace()).is_some() {}
        match chars.next() {
            None => break,
            Some(',') => continue,
            Some(_) => return None,
        }
    }
    Some(items)
}

/// A value's text, as distinct counts and keys compare it.
fn value_text(value: &JsonValue) -> String {
    match value {
        JsonValue::String(s) => s.trim().to_string(),
        other => other.to_string(),
    }
}

pub(crate) fn hash_text(text: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    text.hash(&mut hasher);
    hasher.finish()
}

/// Distinct values kept exactly, up to this many; past it a column is not a category.
const DISTINCT_CAP: usize = 1024;
/// A value longer than this is neither a category nor a key.
const KEY_MAX_LEN: usize = 256;
/// A category holds at most this many distinct values…
const CATEGORY_MAX_DISTINCT: usize = 64;
/// …none longer than this…
const CATEGORY_MAX_LEN: usize = 64;
/// …and each, on average, at least this many times: four names in four rows are not categories.
const CATEGORY_MIN_REPEATS: u64 = 20;
/// Hash entries all key trackers together may hold — about 256 MB. Past it, checking stops and
/// the report says from where.
const KEY_ENTRY_BUDGET: usize = 16 << 20;

/// What the scan knows about a column's values as keys.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum KeyState {
    /// Filled and distinct in every row scanned.
    Unique,
    /// A value came back.
    Repeats { at: Location, value: String },
    /// A row without a value.
    NotFilled { at: Location },
    /// A value no key is: a decimal, a boolean, a list.
    NotAKey { shape: Shape, at: Location },
    /// Unique as far as checked; checking stopped when the scan's memory for keys ran out.
    Unchecked { after: Location },
}

#[derive(Debug)]
struct KeyTrack {
    state: KeyState,
    seen: HashSet<u64>,
}

impl KeyTrack {
    fn new() -> Self {
        Self {
            state: KeyState::Unique,
            seen: HashSet::new(),
        }
    }

    fn is_live(&self) -> bool {
        self.state == KeyState::Unique
    }

    fn end(&mut self, state: KeyState) {
        self.state = state;
        self.seen = HashSet::new();
    }

    /// Note one row's value; `None` for a row without one. Returns the entries added.
    fn observe(&mut self, hash: Option<u64>, shape: Shape, text: &str, at: Location) -> usize {
        if !self.is_live() {
            return 0;
        }
        match hash {
            None if !shape.is_filled() => self.end(KeyState::NotFilled { at }),
            None => self.end(KeyState::NotAKey { shape, at }),
            Some(h) => {
                if self.seen.insert(h) {
                    return 1;
                }
                self.end(KeyState::Repeats {
                    at,
                    value: text.chars().take(60).collect(),
                });
            }
        }
        0
    }
}

/// What a scan saw of one column.
#[derive(Debug)]
pub(crate) struct ColumnProfile {
    pub(crate) name: String,
    counts: [u64; SHAPES.len()],
    first: [Option<(Location, String)>; SHAPES.len()],
    pub(crate) max_len: usize,
    has_whitespace: bool,
    /// Each distinct value and its kind, until there are more than [`DISTINCT_CAP`].
    distinct: HashMap<String, Shape>,
    distinct_overflow: bool,
    /// Every integer was 0 or 1.
    only_zero_one: bool,
    negative: bool,
    over_i64: bool,
    /// Which way round its numeric dates were written, where only one reading is a date.
    date_evidence: Vec<(char, DateOrder)>,
    /// It held a numeric date that reads as a date either way round, `03/04/2024`.
    ambiguous_dates: bool,
    /// The values inside its lists.
    elements: Option<Box<ColumnProfile>>,
    key: KeyTrack,
    /// Its values matched the source's `id` column in every row.
    pub(crate) equals_id: bool,
}

impl ColumnProfile {
    fn new(name: String) -> Self {
        Self {
            name,
            counts: [0; SHAPES.len()],
            first: Default::default(),
            max_len: 0,
            has_whitespace: false,
            distinct: HashMap::new(),
            distinct_overflow: false,
            only_zero_one: true,
            negative: false,
            over_i64: false,
            date_evidence: Vec::new(),
            ambiguous_dates: false,
            elements: None,
            key: KeyTrack::new(),
            equals_id: true,
        }
    }

    fn count(&self, shape: Shape) -> u64 {
        self.counts[shape.index()]
    }

    /// Values that hold something.
    pub(crate) fn filled(&self) -> u64 {
        SHAPES
            .iter()
            .filter(|s| s.is_filled())
            .map(|s| self.count(*s))
            .sum()
    }

    fn rows(&self) -> u64 {
        self.counts.iter().sum()
    }

    pub(crate) fn key_state(&self) -> &KeyState {
        &self.key.state
    }

    /// Distinct values, when the scan kept count of them all.
    pub(crate) fn distinct(&self) -> Option<usize> {
        (!self.distinct_overflow).then_some(self.distinct.len())
    }

    /// Note one value: its kind, its text, and — for the key tracker and the pairs — its hash,
    /// returned when it is one a key could be.
    fn observe(&mut self, value: &JsonValue, at: Location, entries: &mut usize) -> Option<u64> {
        let text = value_text(value);
        let shape = match self.distinct.get(&text) {
            Some(shape) if value.is_string() => *shape,
            _ => classify_json(value),
        };
        let slot = shape.index();
        self.counts[slot] += 1;
        if self.first[slot].is_none() {
            self.first[slot] = Some((at, text.chars().take(60).collect()));
        }
        self.max_len = self.max_len.max(text.len());
        if shape.is_filled() && text.contains(char::is_whitespace) {
            self.has_whitespace = true;
        }
        if !self.distinct_overflow && !self.distinct.contains_key(&text) {
            if self.distinct.len() >= DISTINCT_CAP || text.len() > KEY_MAX_LEN {
                self.distinct_overflow = true;
                self.distinct = HashMap::new();
            } else {
                self.distinct.insert(text.clone(), shape);
            }
        }
        match shape {
            Shape::Integer | Shape::LongInteger => {
                let digits = text.trim_start_matches('+');
                self.only_zero_one &= digits == "0" || digits == "1";
                self.negative |= digits.starts_with('-');
                self.over_i64 |= digits.parse::<i64>().is_err();
            }
            Shape::Date => match numeric_date_order(&text) {
                Some(evidence) => {
                    if !self.date_evidence.contains(&evidence) {
                        self.date_evidence.push(evidence);
                    }
                }
                None => self.ambiguous_dates |= is_numeric_date(&text),
            },
            Shape::List => {
                let items = match value {
                    JsonValue::Array(items) => Some(items.clone()),
                    _ => parse_list(&text),
                };
                let name = &self.name;
                let elements = self.elements.get_or_insert_with(|| {
                    // The values inside a list are never its key; nothing to track.
                    let mut elements = ColumnProfile::new(name.clone());
                    elements.key.end(KeyState::NotAKey {
                        shape: Shape::List,
                        at,
                    });
                    Box::new(elements)
                });
                let mut ignored = 0;
                for item in items.unwrap_or_default() {
                    elements.observe(&item, at, &mut ignored);
                }
            }
            _ => {}
        }

        let hash = (shape.is_keyable() && text.len() <= KEY_MAX_LEN).then(|| hash_text(&text));
        let key_shape = if shape.is_keyable() && hash.is_none() {
            Shape::Text
        } else {
            shape
        };
        *entries += self.key.observe(hash, key_shape, &text, at);
        hash
    }

    /// It wrote numeric dates that read either way round and none that say which: the order
    /// is a guess until more of the source is read.
    pub(crate) fn date_order_unsettled(&self) -> bool {
        self.ambiguous_dates && self.date_evidence.is_empty()
    }

    /// The date order this column writes in, or `None` when its sample writes both ways.
    fn date_orders(&self) -> Option<DateOrders> {
        date_orders_from_evidence(&self.date_evidence)
    }

    /// The field this column becomes. Strict: a column takes a type only when every value it
    /// holds fits it, so no row is refused for a value the scan saw. `id_like` keeps a column
    /// whose name says it identifies something from being read as a number when its numbers are
    /// too long, or written with an exponent, to survive as one.
    pub(crate) fn decide(&self, id_like: bool) -> FieldChoice {
        let filled: Vec<Shape> = SHAPES
            .iter()
            .copied()
            .filter(|s| s.is_filled() && self.count(*s) > 0)
            .collect();
        let only = |allowed: &[Shape]| filled.iter().all(|s| allowed.contains(s));
        let has = |shape: Shape| filled.contains(&shape);
        let first_of = |shape: Shape| {
            self.first[shape.index()]
                .as_ref()
                .map(|(at, _)| at.to_string())
                .unwrap_or_default()
        };
        let text = |note: String| FieldChoice::scalar(TantivyFieldType::Text, Some(note));

        let mut choice = if filled.is_empty() {
            text("no values".to_string())
        } else if only(&[Shape::List]) {
            let mut element = match &self.elements {
                Some(elements) if elements.filled() > 0 => elements.decide(id_like),
                _ => FieldChoice::scalar(TantivyFieldType::Text, None),
            };
            element.list = true;
            return element;
        } else if only(&[Shape::Object]) {
            FieldChoice::scalar(TantivyFieldType::Json, None)
        } else if only(&[Shape::Boolean, Shape::BooleanWord, Shape::Integer])
            && has(Shape::Boolean)
            && (!has(Shape::Integer) || self.only_zero_one)
        {
            FieldChoice::scalar(TantivyFieldType::Boolean, None)
        } else if only(&[
            Shape::Integer,
            Shape::LongInteger,
            Shape::Decimal,
            Shape::Exponent,
            Shape::LeadingZero,
        ]) {
            if has(Shape::LeadingZero) {
                text(format!(
                    "leading zeros make it a code, not a number (first at {})",
                    first_of(Shape::LeadingZero)
                ))
            } else if id_like && (has(Shape::LongInteger) || has(Shape::Exponent)) {
                text("an identifier written as a number".to_string())
            } else if has(Shape::Decimal)
                || has(Shape::Exponent)
                || (self.over_i64 && self.negative)
            {
                // Past what i64 holds and negative too: only a double holds them all.
                FieldChoice::scalar(TantivyFieldType::F64, None)
            } else if self.over_i64 {
                FieldChoice::scalar(TantivyFieldType::U64, None)
            } else {
                FieldChoice::scalar(TantivyFieldType::I64, None)
            }
        } else if only(&[Shape::Date]) {
            match self.date_orders() {
                Some(dates) => FieldChoice {
                    dates,
                    ..FieldChoice::scalar(TantivyFieldType::Date, None)
                },
                None => text("dates written both day first and month first".to_string()),
            }
        } else if only(&[Shape::Ip]) {
            FieldChoice::scalar(TantivyFieldType::Ip, None)
        } else if filled.len() > 1 {
            let mut kinds: Vec<Shape> = filled.clone();
            kinds.sort_by_key(|s| std::cmp::Reverse(self.count(*s)));
            let mixed = kinds
                .iter()
                .map(|s| format!("{} from {}", s.label(), first_of(*s)))
                .collect::<Vec<_>>()
                .join(", ");
            text(format!("mixed: {mixed}"))
        } else {
            FieldChoice::scalar(TantivyFieldType::Text, None)
        };

        if choice.field_type == TantivyFieldType::Text {
            if self.is_category() {
                choice.field_type = TantivyFieldType::String;
                choice.category = true;
            } else if self.is_code() {
                choice.tokenizer = Some("raw");
            }
        }
        choice
    }

    /// Codes, identifiers, addresses: values without spaces, nearly one per row. Each is matched
    /// whole, so the raw tokenizer indexes it as the one term it is — split at its punctuation, a
    /// MAC address was six words, and a search for one matched every device sharing a byte.
    fn is_code(&self) -> bool {
        let many = self
            .distinct()
            .is_none_or(|d| d as u64 * 2 >= self.filled());
        many && self.filled() > 0 && self.looks_like_key()
    }

    /// Short values, few of them, each repeated: a status, a class, a vendor.
    fn is_category(&self) -> bool {
        let Some(distinct) = self.distinct() else {
            return false;
        };
        distinct > 0
            && distinct <= CATEGORY_MAX_DISTINCT
            && self.max_len <= CATEGORY_MAX_LEN
            && self.filled() >= CATEGORY_MIN_REPEATS * distinct as u64
    }

    /// Whether the column's values are mostly ones a key could be, ignoring no-values.
    fn count_keyable(&self) -> bool {
        let keyable: u64 = SHAPES
            .iter()
            .filter(|s| s.is_keyable())
            .map(|s| self.count(*s))
            .sum();
        keyable * 2 > self.filled()
    }

    /// Whether every value looks like a key: no spaces, no date, not too long. Asked only of a
    /// column whose name says nothing — a title or a timestamp is often unique across a sample,
    /// and is never what a source meant as its id.
    fn looks_like_key(&self) -> bool {
        !self.has_whitespace
            && self.max_len <= KEY_MAX_LEN
            && self.count(Shape::Date) == 0
            && self.count(Shape::Ip) == 0
    }

    /// The kinds of value seen, most common first, for the report.
    fn shape_summary(&self) -> String {
        let mut kinds: Vec<Shape> = SHAPES
            .iter()
            .copied()
            .filter(|s| self.count(*s) > 0)
            .collect();
        kinds.sort_by_key(|s| std::cmp::Reverse(self.count(*s)));
        kinds
            .iter()
            .map(|s| format!("{} {}", s.label(), grouped(self.count(*s))))
            .collect::<Vec<_>>()
            .join(" · ")
    }
}

/// The date orders a column's evidence settles on; `None` when it holds dates only one order
/// reads and dates only the other order reads, under the same separator.
pub(crate) fn date_orders_from_evidence(evidence: &[(char, DateOrder)]) -> Option<DateOrders> {
    let mut orders = DateOrders::default();
    for separator in ['/', '.'] {
        let day_first = evidence.contains(&(separator, DateOrder::DayFirst));
        let month_first = evidence.contains(&(separator, DateOrder::MonthFirst));
        match (day_first, month_first) {
            (true, true) => return None,
            (true, false) => orders.set(separator, DateOrder::DayFirst),
            (false, true) => orders.set(separator, DateOrder::MonthFirst),
            (false, false) => {}
        }
    }
    Some(orders)
}

/// The field a column becomes.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct FieldChoice {
    pub(crate) field_type: TantivyFieldType,
    /// Each value is a list, loaded as several values of the field.
    pub(crate) list: bool,
    /// A short set of repeated values, kept whole as a `string` for exact matching.
    pub(crate) category: bool,
    pub(crate) dates: DateOrders,
    /// The tokenizer a text field takes when it is not the default: `raw` for codes.
    pub(crate) tokenizer: Option<&'static str>,
    /// Why, where the type is not what the values look like at first sight.
    pub(crate) note: Option<String>,
}

impl FieldChoice {
    fn scalar(field_type: TantivyFieldType, note: Option<String>) -> Self {
        Self {
            field_type,
            list: false,
            category: false,
            dates: DateOrders::default(),
            tokenizer: None,
            note,
        }
    }
}

/// Two columns whose values together might identify a row.
#[derive(Debug)]
struct PairTrack {
    columns: (usize, usize),
    key: KeyTrack,
}

/// How many columns pair candidates are drawn from: the likeliest keys, and what to pair them
/// with.
const PAIR_ANCHORS: usize = 3;
const PAIR_PARTNERS: usize = 3;

/// What a scan saw of every column, row by row.
#[derive(Debug)]
pub(crate) struct Profiler {
    pub(crate) columns: Vec<ColumnProfile>,
    by_name: HashMap<String, usize>,
    pub(crate) rows: u64,
    batch_rows: usize,
    /// The first batch's value hashes and locations, kept until the pairs are chosen from it.
    first_batch: Vec<(Location, Vec<Option<u64>>)>,
    pairs: Option<Vec<PairTrack>>,
    /// The id `--id` named, followed through the scan as the load will compose it.
    named_id: Option<(Vec<String>, KeyTrack)>,
    entries: usize,
    /// Kinds of value seen for the first time, per column, ever — what stability is judged on.
    novelty: usize,
    /// The source's own `id` column.
    id_column: Option<usize>,
}

impl Profiler {
    pub(crate) fn new(names: &[String], batch_rows: usize) -> Self {
        let mut profiler = Self {
            columns: Vec::new(),
            by_name: HashMap::new(),
            rows: 0,
            batch_rows,
            first_batch: Vec::new(),
            pairs: None,
            entries: 0,
            novelty: 0,
            id_column: None,
            named_id: None,
        };
        // A CSV's columns are positional: a repeated header is still a column of its own.
        for name in names {
            profiler
                .by_name
                .entry(name.clone())
                .or_insert(profiler.columns.len());
            if name.eq_ignore_ascii_case("id") && profiler.id_column.is_none() {
                profiler.id_column = Some(profiler.columns.len());
            }
            profiler.columns.push(ColumnProfile::new(name.clone()));
        }
        profiler
    }

    /// The column with this name, added when first seen. A column that appears after the first
    /// row was empty in every row before it.
    fn column(&mut self, name: &str) -> usize {
        if let Some(&idx) = self.by_name.get(name) {
            return idx;
        }
        let mut profile = ColumnProfile::new(name.to_string());
        if self.rows > 0 {
            profile.counts[Shape::Empty.index()] = self.rows;
            profile.first[Shape::Empty.index()] = Some((Location::Document(1), String::new()));
            profile.key.end(KeyState::NotFilled {
                at: Location::Document(1),
            });
            profile.equals_id = false;
        }
        let idx = self.columns.len();
        if name.eq_ignore_ascii_case("id") && self.id_column.is_none() {
            self.id_column = Some(idx);
        }
        self.columns.push(profile);
        self.by_name.insert(name.to_string(), idx);
        self.novelty += 1;
        idx
    }

    pub(crate) fn index_of(&self, name: &str) -> Option<usize> {
        self.by_name.get(name).copied().or_else(|| {
            self.columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(name))
        })
    }

    /// The count of kinds seen for the first time; unchanged across a batch means the batch
    /// taught nothing new about any column's type.
    pub(crate) fn novelty(&self) -> usize {
        self.novelty
            + self
                .columns
                .iter()
                .map(|c| c.first.iter().filter(|f| f.is_some()).count())
                .sum::<usize>()
    }

    pub(crate) fn observe_csv(&mut self, record: &csv::StringRecord, at: Location) {
        let values: Vec<JsonValue> = (0..self.columns.len())
            .map(|idx| JsonValue::String(record.get(idx).unwrap_or_default().to_string()))
            .collect();
        self.observe_row(values, at);
    }

    pub(crate) fn observe_json(&mut self, doc: &JsonMap<String, JsonValue>, at: Location) {
        for name in doc.keys() {
            self.column(name);
        }
        let values: Vec<JsonValue> = self
            .columns
            .iter()
            .map(|c| doc.get(&c.name).cloned().unwrap_or(JsonValue::Null))
            .collect();
        self.observe_row(values, at);
    }

    /// Follow the id `--id` names through the scan, so a repeat is found before the load
    /// overwrites anything.
    pub(crate) fn track_id(&mut self, spec: &IdSpec) {
        let columns = spec.columns().into_iter().map(str::to_string).collect();
        self.named_id = Some((columns, KeyTrack::new()));
    }

    /// What the scan found of the id `--id` named.
    pub(crate) fn named_id_state(&self) -> Option<&KeyState> {
        self.named_id.as_ref().map(|(_, key)| &key.state)
    }

    fn observe_row(&mut self, values: Vec<JsonValue>, at: Location) {
        let id_text = self.id_column.map(|idx| value_text(&values[idx]));
        let named = self.named_id.as_ref().map(|(columns, _)| {
            compose_id(columns.iter().map(|name| {
                self.index_of(name)
                    .and_then(|idx| values.get(idx))
                    .map(value_text)
                    .filter(|text| !text.is_empty() && text != "null")
            }))
        });
        if let (Some((_, key)), Some(id)) = (&mut self.named_id, named) {
            let hash = id.as_deref().map(hash_text);
            self.entries += key.observe(hash, Shape::Empty, id.as_deref().unwrap_or(""), at);
        }
        let mut hashes = Vec::with_capacity(values.len());
        for (idx, value) in values.iter().enumerate() {
            let column = &mut self.columns[idx];
            if self.entries >= KEY_ENTRY_BUDGET && column.key.is_live() {
                column.key.end(KeyState::Unchecked { after: at });
            }
            hashes.push(column.observe(value, at, &mut self.entries));
            if let Some(id_text) = &id_text
                && column.equals_id
                && value_text(value) != *id_text
            {
                column.equals_id = false;
            }
        }
        self.rows += 1;
        match &mut self.pairs {
            None => {
                self.first_batch.push((at, hashes));
                if self.first_batch.len() >= self.batch_rows {
                    self.choose_pairs();
                }
            }
            Some(pairs) => {
                for pair in pairs.iter_mut() {
                    let (a, b) = pair.columns;
                    let hash = combine(hashes[a], hashes[b]);
                    self.entries += pair.key.observe(hash, Shape::Empty, "", at);
                }
            }
        }
    }

    /// Choose which pairs of columns to follow as composite keys, from the first batch: the
    /// likeliest keys, each paired with the others and with the likeliest partners — a date
    /// first, since a reading taken per device per hour is keyed by both.
    fn choose_pairs(&mut self) {
        let keyable = |c: &ColumnProfile| {
            c.filled() == c.rows()
                && SHAPES
                    .iter()
                    .filter(|s| c.count(**s) > 0)
                    .all(|s| s.is_keyable())
        };
        // The likeliest keys are the columns with the most distinct values.
        let distinct = |c: &ColumnProfile| c.distinct().unwrap_or(usize::MAX);
        let mut anchors: Vec<usize> = (0..self.columns.len())
            .filter(|&i| keyable(&self.columns[i]) && distinct(&self.columns[i]) > 1)
            .collect();
        anchors.sort_by_key(|&i| {
            let c = &self.columns[i];
            (
                !c.key.is_live(),
                std::cmp::Reverse(distinct(c)),
                id_name_rank(&c.name),
                i,
            )
        });
        anchors.truncate(PAIR_ANCHORS);
        let mut partners: Vec<usize> = (0..self.columns.len())
            .filter(|&i| keyable(&self.columns[i]) && !anchors.contains(&i))
            .collect();
        partners.sort_by_key(|&i| {
            let c = &self.columns[i];
            (
                c.count(Shape::Date) == 0,
                !is_id_like_name(&c.name),
                std::cmp::Reverse(c.distinct().unwrap_or(DISTINCT_CAP)),
                i,
            )
        });
        partners.truncate(PAIR_PARTNERS);

        let mut pairs = Vec::new();
        for (n, &a) in anchors.iter().enumerate() {
            for &b in anchors[n + 1..].iter().chain(&partners) {
                pairs.push(PairTrack {
                    columns: (a.min(b), a.max(b)),
                    key: KeyTrack::new(),
                });
            }
        }
        for (at, hashes) in std::mem::take(&mut self.first_batch) {
            for pair in pairs.iter_mut() {
                let hash = combine(hashes[pair.columns.0], hashes[pair.columns.1]);
                self.entries += pair.key.observe(hash, Shape::Empty, "", at);
            }
        }
        self.pairs = Some(pairs);
    }

    /// End of scan: a source shorter than one batch still has its pairs chosen.
    pub(crate) fn finish(&mut self) {
        if self.pairs.is_none() {
            self.choose_pairs();
        }
    }

    /// Column pairs that stayed unique, where neither column is unique alone.
    pub(crate) fn unique_pairs(&self) -> Vec<(usize, usize)> {
        self.pairs
            .iter()
            .flatten()
            .filter(|p| matches!(p.key.state, KeyState::Unique | KeyState::Unchecked { .. }))
            .filter(|p| {
                !self.columns[p.columns.0].key.is_live() && !self.columns[p.columns.1].key.is_live()
            })
            .map(|p| p.columns)
            .collect()
    }

    /// Each column's field, in source order.
    pub(crate) fn choices(&self) -> Vec<FieldChoice> {
        self.columns
            .iter()
            .map(|c| c.decide(is_id_like_name(&c.name)))
            .collect()
    }
}

fn combine(a: Option<u64>, b: Option<u64>) -> Option<u64> {
    let (a, b) = (a?, b?);
    let mut hasher = DefaultHasher::new();
    (a, b).hash(&mut hasher);
    Some(hasher.finish())
}

/// Which columns make a document's id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum IdSpec {
    /// One column; a shadow field keeps its name when it is not `id`.
    Column(String),
    /// Several, joined with `|` in the order given. They stay fields of their own.
    Composite(Vec<String>),
}

/// The separator between a composite id's parts.
pub(crate) const ID_SEPARATOR: &str = "|";

impl IdSpec {
    /// The columns `--id` names: one, or several comma-separated.
    pub(crate) fn parse(arg: &str) -> Result<Self> {
        let names: Vec<String> = arg
            .split(',')
            .map(str::trim)
            .filter(|n| !n.is_empty())
            .map(str::to_string)
            .collect();
        match names.len() {
            0 => Err(anyhow!("--id names no column")),
            1 => Ok(IdSpec::Column(names.into_iter().next().expect("one"))),
            _ => Ok(IdSpec::Composite(names)),
        }
    }

    pub(crate) fn columns(&self) -> Vec<&str> {
        match self {
            IdSpec::Column(name) => vec![name.as_str()],
            IdSpec::Composite(names) => names.iter().map(String::as_str).collect(),
        }
    }

    /// The single column, when there is one.
    pub(crate) fn single(&self) -> Option<&str> {
        match self {
            IdSpec::Column(name) => Some(name),
            IdSpec::Composite(_) => None,
        }
    }
}

/// How the id was chosen, for the report and the warnings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum IdReason {
    /// Named with `--id`.
    Explicit,
    /// The id the index already records: its `id_fields`, or its shadow field.
    Existing,
    /// The column named `id`.
    Named,
    /// Unique across the scan, and the best-named such column.
    Unique,
    /// No column was unique; the best name won alone.
    NameOnly,
}

#[derive(Debug, Clone)]
pub(crate) struct IdChoice {
    pub(crate) spec: IdSpec,
    pub(crate) reason: IdReason,
}

impl Profiler {
    /// The id for this source: the columns `--id` names, the index's own shadow field when the
    /// source has it, the column named `id`, and otherwise the best-named column unique across
    /// the scan — failing a named one, the first whose values all look like keys. Only when no
    /// column is unique does the best name win alone, as it always did.
    pub(crate) fn choose_id(
        &self,
        explicit: Option<&IdSpec>,
        recorded: Option<&IdSpec>,
    ) -> Result<IdChoice> {
        let name_of = |idx: usize| self.columns[idx].name.clone();
        // A named id, resolved to the source's spelling of each column.
        let resolve = |spec: &IdSpec, missing: &dyn Fn(&str) -> anyhow::Error| {
            let resolved = spec
                .columns()
                .iter()
                .map(|name| {
                    self.index_of(name)
                        .map(name_of)
                        .ok_or_else(|| missing(name))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok::<_, anyhow::Error>(match spec {
                IdSpec::Column(_) => IdSpec::Column(resolved.into_iter().next().expect("one")),
                IdSpec::Composite(_) => IdSpec::Composite(resolved),
            })
        };
        if let Some(spec) = explicit {
            let spec = resolve(spec, &|name| {
                anyhow!("--id names a column the source does not have: '{name}'")
            })?;
            return Ok(IdChoice {
                spec,
                reason: IdReason::Explicit,
            });
        }
        // The index keys its documents one way already; a load keys the rest the same way, or
        // its rows would sit beside the records they were meant to replace.
        if let Some(spec) = recorded {
            let columns = spec.columns().join(",");
            let spec = resolve(spec, &|name| {
                anyhow!(
                    "The index keys its documents by {columns}, and the source has no '{name}' \
                     column. Name the id with --id."
                )
            })?;
            return Ok(IdChoice {
                spec,
                reason: IdReason::Existing,
            });
        }
        let column = |idx: usize, reason| IdChoice {
            spec: IdSpec::Column(name_of(idx)),
            reason,
        };
        if let Some(idx) = self.id_column {
            return Ok(column(idx, IdReason::Named));
        }
        let unique = |c: &ColumnProfile| {
            c.rows() > 0 && matches!(c.key.state, KeyState::Unique | KeyState::Unchecked { .. })
        };
        let rank = |i: usize| (id_name_rank(&self.columns[i].name), i);
        let named = (0..self.columns.len())
            .filter(|&i| unique(&self.columns[i]) && is_id_like_name(&self.columns[i].name))
            .min_by_key(|&i| rank(i));
        let keyed = || {
            (0..self.columns.len())
                .find(|&i| unique(&self.columns[i]) && self.columns[i].looks_like_key())
        };
        if let Some(idx) = named.or_else(keyed) {
            return Ok(column(idx, IdReason::Unique));
        }
        // Nothing is unique: the column that tells the most rows apart loses the fewest to
        // overwriting, and among those the best name.
        let best = (0..self.columns.len())
            .min_by_key(|&i| {
                let c = &self.columns[i];
                let distinct = c.distinct().map_or(u64::MAX, |d| d as u64);
                (!c.count_keyable(), std::cmp::Reverse(distinct), rank(i))
            })
            .ok_or_else(|| anyhow!("The source has no columns"))?;
        Ok(column(best, IdReason::NameOnly))
    }

    /// The columns worth reporting as keys: every column with a distinct value for at least
    /// every other row — the ones a reader would expect to be unique.
    pub(crate) fn key_candidates(&self) -> Vec<usize> {
        let mut candidates: Vec<usize> = (0..self.columns.len())
            .filter(|&i| {
                let c = &self.columns[i];
                let many = c
                    .distinct()
                    .is_none_or(|d| d as u64 * 2 >= c.rows() && d > 1);
                many && c.count_keyable()
            })
            .collect();
        candidates.sort_by_key(|&i| (id_name_rank(&self.columns[i].name), i));
        candidates
    }

    /// The `--id` that would key every row, when no single column does.
    pub(crate) fn suggested_id(&self) -> Option<String> {
        self.unique_key_candidates().into_iter().next()
    }

    /// Every key the scan found unique, as `--id` would name it: single columns first, best
    /// name first, then pairs of columns neither of which is unique alone. Suggestions only — an
    /// id named with `--id` is used as named.
    pub(crate) fn unique_key_candidates(&self) -> Vec<String> {
        let unique = |c: &ColumnProfile| {
            c.rows() > 0 && matches!(c.key.state, KeyState::Unique | KeyState::Unchecked { .. })
        };
        let mut singles: Vec<usize> = (0..self.columns.len())
            .filter(|&i| unique(&self.columns[i]) && self.columns[i].count_keyable())
            .collect();
        singles.sort_by_key(|&i| (id_name_rank(&self.columns[i].name), i));
        singles
            .into_iter()
            .map(|i| self.columns[i].name.clone())
            .chain(
                self.unique_pairs()
                    .into_iter()
                    .map(|(a, b)| format!("{},{}", self.columns[a].name, self.columns[b].name)),
            )
            .collect()
    }

    /// Say so when the chosen id is not unique across the scan.
    pub(crate) fn warn_about_id(&self, choice: &IdChoice) {
        let name = choice.spec.columns().join(",");
        let hint = match self.suggested_id() {
            Some(id) if id != name => format!(" Detected unique key candidate: --id {id}"),
            _ => String::new(),
        };
        let named = matches!(choice.reason, IdReason::Explicit | IdReason::Existing);
        match (&choice.reason, self.named_id_state()) {
            (IdReason::NameOnly, _) => eprintln!(
                "⚠️  No single column is filled and unique across the {} scanned rows; using \
                 '{name}' as the id, and rows sharing a value overwrite each other.{hint}",
                grouped(self.rows)
            ),
            // Named on purpose: a repeat is a later version of the same record, and replacing
            // the earlier one is what the caller asked for. Said, not warned about.
            (_, Some(KeyState::Repeats { at, value })) if named => eprintln!(
                "ℹ️  The id ({name}) repeats in the scan, first at {at} ('{value}'); a later row \
                 replaces the document an earlier one with its id made."
            ),
            (_, Some(KeyState::NotFilled { at })) if named => {
                eprintln!("⚠️  The id ({name}) has no value at {at}; rows without one are skipped.")
            }
            _ => {}
        }
    }
}

/// How the scan read the source, for the report.
#[derive(Debug, Clone, Default)]
pub(crate) struct ScanSummary {
    pub(crate) format: String,
    /// The source's size on disk, when known.
    pub(crate) source_bytes: Option<u64>,
    pub(crate) head_rows: u64,
    pub(crate) head_last: Option<Location>,
    pub(crate) spread_rows: u64,
    pub(crate) spread_blocks: usize,
    pub(crate) bytes_read: u64,
    pub(crate) elapsed: Duration,
    /// The scan read to the end of the source.
    pub(crate) whole: bool,
    pub(crate) stopped_by: &'static str,
    /// Records in the source: counted when read whole, else estimated.
    pub(crate) rows: Option<u64>,
}

/// Format a byte count the way a person reads one.
pub(crate) fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1000.0 && unit < UNITS.len() - 1 {
        value /= 1000.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

/// The report `schema detect --report` prints: how the source was read, what identifies its
/// rows, and why each column became the field it did.
pub(crate) fn render_report(
    source: &str,
    summary: &ScanSummary,
    profiler: &Profiler,
    id: &IdChoice,
) -> String {
    let mut out = String::new();
    let size = summary
        .source_bytes
        .map(|b| format!(", {}", human_bytes(b)))
        .unwrap_or_default();
    out.push_str(&format!("Source    {source} ({}{size})\n", summary.format));

    let mut scanned = if summary.whole {
        format!("all {} rows", grouped(summary.head_rows))
    } else {
        let head_end = summary
            .head_last
            .map(|at| format!(" (to {at})"))
            .unwrap_or_default();
        format!(
            "{} rows from the head{head_end}",
            grouped(summary.head_rows)
        )
    };
    if summary.spread_blocks > 0 {
        scanned.push_str(&format!(
            ", then {} rows in {} blocks spread over the rest",
            grouped(summary.spread_rows),
            summary.spread_blocks
        ));
    }
    out.push_str(&format!(
        "Scanned   {scanned}\n          {} read in {:.1} s; stopped: {}\n",
        human_bytes(summary.bytes_read),
        summary.elapsed.as_secs_f64(),
        summary.stopped_by
    ));
    match summary.rows {
        Some(rows) if summary.whole => out.push_str(&format!("Rows      {}\n", grouped(rows))),
        Some(rows) => out.push_str(&format!("Rows      ~{} (estimated)\n", grouped(rows))),
        None => {}
    }

    let id_columns = id.spec.columns().join(" + ");
    let source = match id.reason {
        IdReason::Existing => "the id the index records",
        _ => "named with --id",
    };
    let named = match profiler.named_id_state() {
        Some(KeyState::Unique) => format!("{source}; unique in every scanned row"),
        Some(KeyState::Repeats { at, value }) => format!(
            "{source} — repeats at {at} ('{value}'); a later row replaces the earlier document"
        ),
        Some(KeyState::NotFilled { at }) => {
            format!("{source} — no value at {at}; rows without one are skipped")
        }
        Some(KeyState::Unchecked { after }) => {
            format!("{source}; unique through {after}, checked no further")
        }
        _ => source.to_string(),
    };
    let why = match id.reason {
        IdReason::Explicit | IdReason::Existing => named.as_str(),
        IdReason::Named => "the column named id",
        IdReason::Unique => "filled and unique in every scanned row",
        IdReason::NameOnly => {
            "by name only — not unique; rows sharing a value overwrite each other"
        }
    };
    out.push_str(&format!("Id        {id_columns} ({why})\n"));

    // What the scan found of the columns that could identify a row: a suggestion beside the id,
    // never a replacement for one named with --id.
    let candidates = profiler.unique_key_candidates();
    let checked = profiler.key_candidates();
    if !candidates.is_empty() || !checked.is_empty() {
        let heading = match id.reason {
            IdReason::Explicit => "detected; a suggestion — the id is used as named",
            _ => "detected",
        };
        out.push_str(&format!("Keys      {heading}\n"));
        let width = candidates
            .iter()
            .map(|c| c.len())
            .chain(checked.iter().map(|&i| profiler.columns[i].name.len()))
            .max()
            .unwrap_or(0);
        for candidate in &candidates {
            out.push_str(&format!(
                "          {candidate:width$}  unique in every scanned row → --id {candidate}\n"
            ));
        }
        for idx in checked {
            let column = &profiler.columns[idx];
            let name = &column.name;
            let state = match column.key_state() {
                KeyState::Repeats { at, value } => format!("repeats at {at} ('{value}')"),
                KeyState::NotFilled { at } => format!("has no value at {at}"),
                KeyState::NotAKey { shape, at } => {
                    format!("holds a {} at {at}, which no key is", shape.label())
                }
                // Listed above, as a candidate.
                KeyState::Unique | KeyState::Unchecked { .. } => continue,
            };
            out.push_str(&format!("          {name:width$}  {state}\n"));
        }
    }

    out.push_str("Columns\n");
    let width = profiler
        .columns
        .iter()
        .map(|c| c.name.len())
        .max()
        .unwrap_or(4)
        .max(4);
    for (column, choice) in profiler.columns.iter().zip(profiler.choices()) {
        let mut field_type = choice.field_type.to_string().to_string();
        if let Some(tokenizer) = choice.tokenizer {
            field_type.push_str(&format!(" ({tokenizer})"));
        }
        if choice.list {
            field_type.push_str("[]");
        }
        let distinct = column
            .distinct()
            .map(|n| format!("{} distinct", grouped(n as u64)))
            .unwrap_or_else(|| format!(">{} distinct", grouped(DISTINCT_CAP as u64)));
        let mut notes = vec![column.shape_summary(), distinct];
        if id.spec.single() == Some(column.name.as_str()) && !column.name.eq_ignore_ascii_case("id")
        {
            // The schema keeps it as the id's shadow: found by `name:value`, not indexed itself.
            notes.push("the id — kept as a shadow field, not indexed".to_string());
        }
        if choice.category {
            notes.push("category".to_string());
        }
        if let Some(note) = &choice.note {
            notes.push(note.clone());
        }
        out.push_str(&format!(
            "  {:width$}  {:12} {}\n",
            column.name,
            field_type,
            notes.join("; ")
        ));
    }
    out
}
