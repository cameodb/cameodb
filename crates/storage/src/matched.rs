//! Which elements of an array a hit matched — read in the fetch phase, so a cut array can lead
//! with them (ROADMAP P6).
//!
//! The search index sees every element of an array, so a clause matches an element anywhere in
//! it, often far past what the array cap keeps. The hit is right and shows none of what made it
//! one. Nothing tantivy returns says which element it was: the built query hides its ranges and
//! slop, and walks `MustNot` clauses as readily as the rest. The screened query tree does not
//! ([`PreparedQuery::screened_ast`]), so the matcher is built from it, once per fetch batch.
//!
//! **It agrees with the index by asking the index's own questions.** Text is tokenized by the
//! column's analyzer, range bounds and set members by the same analyzer to one token each — as
//! the parser builds them — and typed values are read the way the writer reads them
//! ([`crate::schema::add_json_value_to_doc`]). tantivy leaves a position gap between a field's
//! values, so a term or exact phrase that matched a document matched one element of it.
//!
//! **Positive clauses only, per field.** A `MustNot` subtree names what a hit does not hold, so it
//! is never evidence. A clause is matched on its own field, not the whole query re-evaluated: for
//! `a:x AND b:y` each array reports its own elements.
//!
//! **A ghost's clause is matched on its source.** A ghost column (Q1) is never stored: its values
//! are the source's, analysed by the ghost's tokenizer, for the documents its condition admits.
//! So its matcher scans the source's elements with the ghost's analyzer, for a document the ghost
//! is fed from, and reports them under the source's name.
//!
//! What gives no element evidence is left out: `*`, presence, a regex, json, bytes and facet
//! fields, and a clause on `id`.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, Ipv6Addr};
use std::ops::Bound;

use serde_json::{Map as JsonMap, Value as JsonValue};
use tantivy::query_grammar::{
    Occur, UserInputAst, UserInputBound, UserInputLeaf, UserInputLiteral,
};
use tantivy::tokenizer::TextAnalyzer;

use crate::query::PreparedQuery;
use crate::schema::{
    Ghost, IndexSchema, SchemaFields, TantivyFieldType, epoch_seconds_to_tantivy,
    parse_date_str_to_tantivy,
};

/// The matchers for one query, keyed by the stored field whose elements they test.
pub(crate) struct ElementMatcher {
    fields: HashMap<String, Vec<Probe>>,
}

/// One positive clause, as a test on one element.
struct Probe {
    test: Test,
    /// The ghost the clause named, when it named one: its source is only scanned in a document
    /// the ghost is fed from.
    ghost: Option<Ghost>,
}

enum Test {
    /// A term, a phrase — with its slop — or a phrase whose last word is a prefix.
    Tokens {
        analyzer: TextAnalyzer,
        terms: Vec<String>,
        slop: u32,
        prefix: bool,
        string_only: bool,
    },
    /// A range of terms: some token of the element falls in it.
    TokenRange {
        analyzer: TextAnalyzer,
        lower: Bound<String>,
        upper: Bound<String>,
        string_only: bool,
    },
    /// A set of terms: some token of the element is one of them.
    TokenSet {
        analyzer: TextAnalyzer,
        terms: HashSet<String>,
        string_only: bool,
    },
    /// A typed equality or range: the element's value falls in it.
    Value {
        field_type: TantivyFieldType,
        lower: Bound<Typed>,
        upper: Bound<Typed>,
    },
    /// A typed set: the element's value is one of them.
    ValueSet {
        field_type: TantivyFieldType,
        values: Vec<Typed>,
    },
}

/// A value of a numeric, date, boolean or ip column, as the index holds it.
#[derive(Debug, Clone, PartialEq)]
enum Typed {
    I64(i64),
    U64(u64),
    F64(f64),
    Bool(bool),
    /// Whole seconds since the epoch, clamped as the writer clamps.
    Date(i64),
    /// IPv4 held inside `::ffff:0:0/96`, as the column holds it.
    Ip(Ipv6Addr),
}

impl Typed {
    fn compare(&self, other: &Typed) -> Option<Ordering> {
        match (self, other) {
            (Typed::I64(a), Typed::I64(b)) => Some(a.cmp(b)),
            (Typed::U64(a), Typed::U64(b)) => Some(a.cmp(b)),
            (Typed::F64(a), Typed::F64(b)) => a.partial_cmp(b),
            (Typed::Bool(a), Typed::Bool(b)) => Some(a.cmp(b)),
            (Typed::Date(a), Typed::Date(b)) => Some(a.cmp(b)),
            (Typed::Ip(a), Typed::Ip(b)) => Some(a.cmp(b)),
            _ => None,
        }
    }

    /// A query's literal for a column of `field_type`, read as the parser reads it.
    fn parse(field_type: &TantivyFieldType, text: &str) -> Option<Typed> {
        match field_type {
            TantivyFieldType::I64 => text.parse().ok().map(Typed::I64),
            TantivyFieldType::U64 => text.parse().ok().map(Typed::U64),
            TantivyFieldType::F64 => text.parse().ok().map(Typed::F64),
            TantivyFieldType::Boolean => text.parse().ok().map(Typed::Bool),
            TantivyFieldType::Date => {
                parse_date_str_to_tantivy(text).map(|(_, _, clamped)| Typed::Date(clamped))
            }
            TantivyFieldType::Ip => text.parse::<IpAddr>().ok().map(|ip| Typed::Ip(as_v6(ip))),
            _ => None,
        }
    }

    /// An element of a column of `field_type`, read as the writer reads it.
    fn of_element(field_type: &TantivyFieldType, value: &JsonValue) -> Option<Typed> {
        match field_type {
            TantivyFieldType::I64 => value.as_i64().map(Typed::I64),
            TantivyFieldType::U64 => value.as_u64().map(Typed::U64),
            TantivyFieldType::F64 => value.as_f64().map(Typed::F64),
            TantivyFieldType::Boolean => value.as_bool().map(Typed::Bool),
            TantivyFieldType::Date => match value {
                JsonValue::String(s) => parse_date_str_to_tantivy(s),
                _ => value.as_i64().map(epoch_seconds_to_tantivy),
            }
            .map(|(_, _, clamped)| Typed::Date(clamped)),
            TantivyFieldType::Ip => value
                .as_str()
                .and_then(|s| s.parse::<IpAddr>().ok())
                .map(|ip| Typed::Ip(as_v6(ip))),
            _ => None,
        }
    }
}

fn as_v6(ip: IpAddr) -> Ipv6Addr {
    match ip {
        IpAddr::V4(v4) => v4.to_ipv6_mapped(),
        IpAddr::V6(v6) => v6,
    }
}

fn within<T>(
    value: &T,
    lower: &Bound<T>,
    upper: &Bound<T>,
    cmp: impl Fn(&T, &T) -> Option<Ordering>,
) -> bool {
    let above = match lower {
        Bound::Included(low) => {
            matches!(cmp(value, low), Some(Ordering::Greater | Ordering::Equal))
        }
        Bound::Excluded(low) => matches!(cmp(value, low), Some(Ordering::Greater)),
        Bound::Unbounded => true,
    };
    let below = match upper {
        Bound::Included(high) => matches!(cmp(value, high), Some(Ordering::Less | Ordering::Equal)),
        Bound::Excluded(high) => matches!(cmp(value, high), Some(Ordering::Less)),
        Bound::Unbounded => true,
    };
    above && below
}

/// The tokens of `text` under `analyzer`, each with its position.
fn tokens(analyzer: &mut TextAnalyzer, text: &str) -> Vec<(usize, String)> {
    let mut out = Vec::new();
    analyzer
        .token_stream(text)
        .process(&mut |token| out.push((token.position, token.text.clone())));
    out
}

/// Whether some token of `text` under `analyzer` passes `test`, read as they stream rather than
/// collected: a term, range or set test on an array of a few hundred thousand elements is one
/// allocation-free pass.
fn any_token(analyzer: &mut TextAnalyzer, text: &str, mut test: impl FnMut(&str) -> bool) -> bool {
    let mut found = false;
    analyzer
        .token_stream(text)
        .process(&mut |token| found = found || test(&token.text));
    found
}

/// The one token a range bound or set member analyses to; `None` when it is not one, which the
/// parser refuses too.
fn one_token(analyzer: &mut TextAnalyzer, text: &str) -> Option<String> {
    let mut found = tokens(analyzer, text);
    (found.len() == 1).then(|| found.remove(0).1)
}

/// The text a text or string column indexes for one element, as the writer adds it: a text
/// column takes any value but `null` as text, a string column only a string.
fn element_text(value: &JsonValue, string_only: bool) -> Option<std::borrow::Cow<'_, str>> {
    match value {
        JsonValue::String(s) => Some(std::borrow::Cow::Borrowed(s)),
        JsonValue::Null => None,
        _ if string_only => None,
        other => serde_json::to_string(other)
            .ok()
            .map(std::borrow::Cow::Owned),
    }
}

/// Whether `found` holds `terms` as a phrase: in order, each within `slop` positions of the one
/// before, the last a prefix when `prefix` is set.
fn holds_phrase(found: &[(usize, String)], terms: &[String], slop: u32, prefix: bool) -> bool {
    let last = terms.len() - 1;
    let is = |at: usize, text: &str| {
        if prefix && at == last {
            text.starts_with(terms[at].as_str())
        } else {
            text == terms[at]
        }
    };
    found.iter().enumerate().any(|(start, (position, text))| {
        if !is(0, text) {
            return false;
        }
        let mut previous = *position;
        let mut next = start + 1;
        for at in 1..terms.len() {
            let reach = previous + 1 + slop as usize;
            let Some(offset) = found[next..].iter().position(|(position, text)| {
                *position > previous && *position <= reach && is(at, text)
            }) else {
                return false;
            };
            previous = found[next + offset].0;
            next += offset + 1;
        }
        true
    })
}

impl Test {
    fn matches(&mut self, element: &JsonValue) -> bool {
        match self {
            Test::Tokens {
                analyzer,
                terms,
                slop,
                prefix,
                string_only,
            } => {
                let Some(text) = element_text(element, *string_only) else {
                    return false;
                };
                if let [term] = terms.as_slice() {
                    return any_token(analyzer, &text, |token| {
                        if *prefix {
                            token.starts_with(term.as_str())
                        } else {
                            token == term
                        }
                    });
                }
                // Most elements lack the first word, and finding that out allocates nothing.
                if !any_token(analyzer, &text, |token| token == terms[0]) {
                    return false;
                }
                let found = tokens(analyzer, &text);
                holds_phrase(&found, terms, *slop, *prefix)
            }
            Test::TokenRange {
                analyzer,
                lower,
                upper,
                string_only,
            } => {
                let Some(text) = element_text(element, *string_only) else {
                    return false;
                };
                let lower = lower.as_ref().map(String::as_str);
                let upper = upper.as_ref().map(String::as_str);
                any_token(analyzer, &text, |token| {
                    within(&token, &lower, &upper, |a, b| Some(a.cmp(b)))
                })
            }
            Test::TokenSet {
                analyzer,
                terms,
                string_only,
            } => {
                let Some(text) = element_text(element, *string_only) else {
                    return false;
                };
                any_token(analyzer, &text, |token| terms.contains(token))
            }
            Test::Value {
                field_type,
                lower,
                upper,
            } => Typed::of_element(field_type, element)
                .is_some_and(|value| within(&value, lower, upper, Typed::compare)),
            Test::ValueSet { field_type, values } => Typed::of_element(field_type, element)
                .is_some_and(|value| {
                    values
                        .iter()
                        .any(|v| value.compare(v) == Some(Ordering::Equal))
                }),
        }
    }
}

impl ElementMatcher {
    /// The matcher for `prepared`, or `None` when no clause of it can name an element.
    pub(crate) fn new(
        prepared: &PreparedQuery,
        tantivy_index: &tantivy::Index,
        schema: &IndexSchema,
        fields: &SchemaFields,
    ) -> Option<Self> {
        let mut matcher = ElementMatcher {
            fields: HashMap::new(),
        };
        let context = Context {
            tantivy_index,
            schema,
            fields,
            default_fields: &prepared.default_fields,
        };
        matcher.walk(&prepared.screened_ast(), &context);
        (!matcher.fields.is_empty()).then_some(matcher)
    }

    /// Whether any clause tests elements of `field`.
    pub(crate) fn tests(&self, field: &str) -> bool {
        self.fields.contains_key(field)
    }

    /// The positions in `items` — the array under `field` in `body` — that some clause matched,
    /// in stored order, at most `limit` of them.
    pub(crate) fn positions(
        &mut self,
        body: &JsonMap<String, JsonValue>,
        field: &str,
        items: &[JsonValue],
        limit: usize,
    ) -> Vec<usize> {
        let mut found = Vec::new();
        let Some(probes) = self.fields.get_mut(field) else {
            return found;
        };
        let mut live: Vec<&mut Probe> = probes
            .iter_mut()
            .filter(|probe| probe.ghost.as_ref().is_none_or(|ghost| ghost.feeds(body)))
            .collect();
        if live.is_empty() || limit == 0 {
            return found;
        }
        for (position, item) in items.iter().enumerate() {
            if live.iter_mut().any(|probe| probe.test.matches(item)) {
                found.push(position);
                if found.len() == limit {
                    break;
                }
            }
        }
        found
    }

    fn walk(&mut self, ast: &UserInputAst, context: &Context<'_>) {
        match ast {
            UserInputAst::Clause(clauses) => {
                for (occur, clause) in clauses {
                    if *occur != Some(Occur::MustNot) {
                        self.walk(clause, context);
                    }
                }
            }
            UserInputAst::Boost(inner, _) => self.walk(inner, context),
            UserInputAst::Leaf(leaf) => match leaf.as_ref() {
                UserInputLeaf::Literal(literal) => match &literal.field_name {
                    Some(field) => self.add(field, leaf, context),
                    None => {
                        for field in context.default_fields {
                            let named = UserInputLeaf::Literal(UserInputLiteral {
                                field_name: Some(field.clone()),
                                ..literal.clone()
                            });
                            self.add(field, &named, context);
                        }
                    }
                },
                UserInputLeaf::Range {
                    field: Some(field), ..
                }
                | UserInputLeaf::Set {
                    field: Some(field), ..
                } => self.add(field, leaf, context),
                _ => {}
            },
        }
    }

    fn add(&mut self, name: &str, leaf: &UserInputLeaf, context: &Context<'_>) {
        let Some(def) = context
            .schema
            .fields
            .get(name)
            .filter(|def| def.indexed && !def.is_shadow)
        else {
            return;
        };
        let Some(column) = context.fields.indexed_fields.get(name) else {
            return;
        };
        let field_type = context.fields.write_type(name, &def.field_type);
        let test = match field_type {
            TantivyFieldType::Text | TantivyFieldType::String => {
                let Ok(analyzer) = context.tantivy_index.tokenizer_for_field(*column) else {
                    return;
                };
                token_test(analyzer, leaf, field_type == TantivyFieldType::String)
            }
            TantivyFieldType::I64
            | TantivyFieldType::U64
            | TantivyFieldType::F64
            | TantivyFieldType::Boolean
            | TantivyFieldType::Date
            | TantivyFieldType::Ip => value_test(field_type, leaf),
            TantivyFieldType::Json | TantivyFieldType::Bytes | TantivyFieldType::Facet => None,
        };
        let Some(test) = test else {
            return;
        };
        let (stored, ghost) = match &def.ghost {
            Some(ghost) => (ghost.source.clone(), Some(ghost.clone())),
            None => (name.to_string(), None),
        };
        self.fields
            .entry(stored)
            .or_default()
            .push(Probe { test, ghost });
    }
}

struct Context<'a> {
    tantivy_index: &'a tantivy::Index,
    schema: &'a IndexSchema,
    fields: &'a SchemaFields,
    default_fields: &'a [String],
}

fn token_bound(analyzer: &mut TextAnalyzer, bound: &UserInputBound) -> Option<Bound<String>> {
    Some(match bound {
        UserInputBound::Inclusive(text) => Bound::Included(one_token(analyzer, text)?),
        UserInputBound::Exclusive(text) => Bound::Excluded(one_token(analyzer, text)?),
        UserInputBound::Unbounded => Bound::Unbounded,
    })
}

fn token_test(mut analyzer: TextAnalyzer, leaf: &UserInputLeaf, string_only: bool) -> Option<Test> {
    match leaf {
        UserInputLeaf::Literal(literal) => {
            let terms: Vec<String> = tokens(&mut analyzer, &literal.phrase)
                .into_iter()
                .map(|(_, text)| text)
                .collect();
            (!terms.is_empty()).then_some(Test::Tokens {
                analyzer,
                terms,
                slop: literal.slop,
                prefix: literal.prefix,
                string_only,
            })
        }
        UserInputLeaf::Range { lower, upper, .. } => {
            let lower = token_bound(&mut analyzer, lower)?;
            let upper = token_bound(&mut analyzer, upper)?;
            Some(Test::TokenRange {
                analyzer,
                lower,
                upper,
                string_only,
            })
        }
        UserInputLeaf::Set { elements, .. } => {
            let terms: HashSet<String> = elements
                .iter()
                .filter_map(|element| one_token(&mut analyzer, element))
                .collect();
            (!terms.is_empty()).then_some(Test::TokenSet {
                analyzer,
                terms,
                string_only,
            })
        }
        _ => None,
    }
}

fn value_bound(field_type: &TantivyFieldType, bound: &UserInputBound) -> Option<Bound<Typed>> {
    Some(match bound {
        UserInputBound::Inclusive(text) => Bound::Included(Typed::parse(field_type, text)?),
        UserInputBound::Exclusive(text) => Bound::Excluded(Typed::parse(field_type, text)?),
        UserInputBound::Unbounded => Bound::Unbounded,
    })
}

fn value_test(field_type: TantivyFieldType, leaf: &UserInputLeaf) -> Option<Test> {
    match leaf {
        UserInputLeaf::Literal(literal) if !literal.prefix => {
            let value = Typed::parse(&field_type, literal.phrase.trim())?;
            Some(Test::Value {
                field_type,
                lower: Bound::Included(value.clone()),
                upper: Bound::Included(value),
            })
        }
        UserInputLeaf::Range { lower, upper, .. } => {
            let lower = value_bound(&field_type, lower)?;
            let upper = value_bound(&field_type, upper)?;
            Some(Test::Value {
                field_type,
                lower,
                upper,
            })
        }
        UserInputLeaf::Set { elements, .. } => {
            let values: Vec<Typed> = elements
                .iter()
                .filter_map(|element| Typed::parse(&field_type, element))
                .collect();
            (!values.is_empty()).then_some(Test::ValueSet { field_type, values })
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::holds_phrase;

    fn found(words: &[(usize, &str)]) -> Vec<(usize, String)> {
        words
            .iter()
            .map(|(at, text)| (*at, text.to_string()))
            .collect()
    }

    fn terms(words: &[&str]) -> Vec<String> {
        words.iter().map(|word| word.to_string()).collect()
    }

    #[test]
    fn a_phrase_is_its_words_in_order_within_the_slop() {
        let text = found(&[(0, "the"), (1, "quick"), (2, "brown"), (3, "fox")]);
        assert!(holds_phrase(&text, &terms(&["quick", "brown"]), 0, false));
        assert!(!holds_phrase(&text, &terms(&["quick", "fox"]), 0, false));
        assert!(holds_phrase(&text, &terms(&["quick", "fox"]), 1, false));
        assert!(
            !holds_phrase(&text, &terms(&["fox", "quick"]), 3, false),
            "in order"
        );
        assert!(
            holds_phrase(&text, &terms(&["brown", "fo"]), 0, true),
            "the last word a prefix"
        );
        assert!(
            !holds_phrase(&text, &terms(&["bro", "fox"]), 0, true),
            "only the last"
        );
    }

    /// A stop word the analyzer drops leaves its position behind; an exact phrase across it is
    /// not one.
    #[test]
    fn positions_come_from_the_analyzer() {
        let text = found(&[(0, "state"), (2, "art")]);
        assert!(!holds_phrase(&text, &terms(&["state", "art"]), 0, false));
        assert!(holds_phrase(&text, &terms(&["state", "art"]), 1, false));
    }
}
