//! What a delete by query removes from one shard (ROADMAP S1).
//!
//! A delete by id names its document; a delete by query names a condition, and every shard
//! deletes the matching documents it holds — so no routing key is needed, whatever the routing
//! field. This module finds them: it screens the query, collects every match (not a page of
//! them), resolves their ids, and for a `text` clause checks each candidate against its stored
//! value. The deleting itself goes through the shard's writer, as any delete does.
//!
//! **Exact clauses only.** A search answers a near miss with a lower score; a delete answers it by
//! removing the document. So the clauses allowed are those that match a value exactly — on
//! `string`, numeric, `date`, `ip` and `boolean` fields, and on the document key — and a `text`
//! clause only as a quoted phrase, required, whose candidates are checked against the stored
//! value: analysis lowercases and stems, so the index alone cannot tell `"Acme Corp"` from
//! `"acme corps"`.
//!
//! **A dropped clause refuses the delete.** A search runs what is left of a query and reports
//! the rest; under `AND`, what is left matches more, and a delete would remove it.

use std::collections::HashMap;
use std::sync::Arc;

use serde_json::Value as JsonValue;
use tantivy::collector::DocSetCollector;
use tantivy::query_grammar::{Delimiter, Occur, UserInputAst, UserInputLeaf};
use tantivy::schema::Value as TantivyValue;
use tantivy::{DocAddress, Searcher};

use crate::query::{describe_discarded_all, prepare_query_parser};
use crate::schema::StoredDocOwned;
use crate::search::{deadline_or, with_deadline};
use crate::*;

/// The candidates a `text` clause's stored-value check reads in one redb transaction.
const EXACT_CHECK_BATCH: usize = 1024;

/// What a delete by query found on one shard.
#[derive(Debug, Default, Clone)]
pub struct DeleteMatches {
    /// The ids to delete, in index order.
    pub ids: Vec<String>,
    /// Candidates the index matched that a `text` clause's exact check turned away.
    pub skipped_inexact: usize,
}

/// A required `text` clause: the field's stored value, or one element of it, must equal this.
#[derive(Debug)]
struct ExactText {
    field: String,
    value: String,
}

fn refused(reason: impl std::fmt::Display) -> StoreError {
    StoreError::QueryParser(tantivy::query::QueryParserError::UnsupportedQuery(format!(
        "not a query a delete can run, so nothing was deleted: {reason}"
    )))
}

impl HybridStore {
    /// The documents of `index` on this shard that `query` matches as a delete: screened,
    /// every match collected, ids resolved, `text` clauses checked against the stored value.
    ///
    /// Reads the last committed searcher, so a document written since is not among them. A
    /// shard that holds no part of the index yet answers nothing to delete.
    pub fn delete_query_matches(
        &self,
        index: &str,
        query: &str,
        deadline: Option<&SearchDeadline>,
    ) -> Result<DeleteMatches, StoreError> {
        let Some((reader, fields)) = self.get_reader(index)? else {
            return Ok(DeleteMatches::default());
        };
        let schema = self
            .get_schema_cached(index)?
            .unwrap_or_else(|| Arc::new(IndexSchema::default()));
        let searcher = reader.searcher();
        let tantivy_index = searcher.index();

        // No prefix is expanded: one left as written is a dropped clause, and refused below.
        let policy = QueryPolicy {
            min_prefix_length: usize::MAX,
            expand_unqualified_prefix: false,
            ..self.config.query.clone()
        };
        let prepared = prepare_query_parser(tantivy_index, &fields, &schema, query, &policy)?;
        let exact = screen(&prepared.screened_ast(), &schema)?;

        let (parsed, errors, range_notes) = prepared.parse();
        let mut discarded = describe_discarded_all(&errors, query, &schema);
        discarded.extend(prepared.notes.iter().cloned());
        discarded.extend(range_notes);
        if !discarded.is_empty() {
            return Err(refused(format!(
                "a clause was dropped, and a dropped clause can widen what a delete removes — {}",
                discarded.join("; ")
            )));
        }

        let parsed = with_deadline(parsed, deadline);
        let addresses = deadline_or(searcher.search(&parsed, &DocSetCollector), deadline)?;
        let ids = resolve_ids(&searcher, &fields, addresses, deadline)?;
        if exact.is_empty() {
            return Ok(DeleteMatches {
                ids,
                skipped_inexact: 0,
            });
        }
        self.keep_exact(index, ids, &exact, deadline)
    }

    /// `ids` narrowed to the documents whose stored values equal every required `text` clause.
    fn keep_exact(
        &self,
        index: &str,
        ids: Vec<String>,
        exact: &[ExactText],
        deadline: Option<&SearchDeadline>,
    ) -> Result<DeleteMatches, StoreError> {
        let mut kept = DeleteMatches::default();
        for chunk in ids.chunks(EXACT_CHECK_BATCH) {
            let rows: HashMap<String, Vec<u8>> = self
                .get_batch_by_keys_with(index, chunk, deadline)?
                .into_iter()
                .collect();
            for id in chunk {
                // A row deleted since the commit has nothing left to delete.
                let Some(bytes) = rows.get(id) else {
                    continue;
                };
                let stored: StoredDocOwned = serde_json::from_slice(bytes)
                    .map_err(|e| StoreError::Serialization(e.to_string()))?;
                let body = stored.json_blob.unwrap_or(JsonValue::Null);
                if exact.iter().all(|clause| holds_exactly(&body, clause)) {
                    kept.ids.push(id.clone());
                } else {
                    kept.skipped_inexact += 1;
                }
            }
        }
        Ok(kept)
    }
}

/// Whether `body`'s value for the clause's field, or one element of it, is the clause's text.
fn holds_exactly(body: &JsonValue, clause: &ExactText) -> bool {
    match body.get(&clause.field) {
        Some(JsonValue::String(value)) => *value == clause.value,
        Some(JsonValue::Array(items)) => items
            .iter()
            .any(|item| item.as_str() == Some(clause.value.as_str())),
        _ => false,
    }
}

/// The ids behind `addresses`: from the fast `id` column where the segment has one, from the
/// stored document otherwise.
fn resolve_ids(
    searcher: &Searcher,
    fields: &SchemaFields,
    addresses: std::collections::HashSet<DocAddress>,
    deadline: Option<&SearchDeadline>,
) -> Result<Vec<String>, StoreError> {
    let mut addresses: Vec<DocAddress> = addresses.into_iter().collect();
    addresses.sort_unstable();
    let mut ids = Vec::with_capacity(addresses.len());
    let mut column = None;
    let mut column_segment = None;
    let mut text = String::new();
    for (n, address) in addresses.into_iter().enumerate() {
        if n.is_multiple_of(4096)
            && let Some(deadline) = deadline
        {
            deadline.check()?;
        }
        if column_segment != Some(address.segment_ord) {
            column_segment = Some(address.segment_ord);
            column = searcher.segment_readers()[address.segment_ord as usize]
                .fast_fields()
                .str("id")
                .ok()
                .flatten();
        }
        let from_column = column.as_ref().and_then(|column| {
            let ord = column.term_ords(address.doc_id).next()?;
            text.clear();
            column
                .ord_to_str(ord, &mut text)
                .ok()
                .filter(|found| *found)
                .map(|_| text.clone())
        });
        let id = match from_column {
            Some(id) => Some(id),
            None => {
                let doc: tantivy::TantivyDocument = searcher.doc(address)?;
                doc.get_first(fields.id)
                    .and_then(|value| value.as_str())
                    .map(str::to_string)
            }
        };
        if let Some(id) = id {
            ids.push(id);
        }
    }
    Ok(ids)
}

/// Refuse what a delete cannot run, and return the `text` clauses to check against stored
/// values. See the module documentation for the rules.
fn screen(ast: &UserInputAst, schema: &IndexSchema) -> Result<Vec<ExactText>, StoreError> {
    let mut screen = Screen {
        schema,
        exact: Vec::new(),
        positive: 0,
    };
    // Which clauses are required at the top: a lone leaf, the `AND`ed members of the outer
    // group, or the one member of a group of one.
    match unboost(ast) {
        UserInputAst::Clause(clauses) => {
            let alone = clauses.len() == 1;
            for (occur, clause) in clauses {
                let negated = *occur == Some(Occur::MustNot);
                let required = !negated && (*occur == Some(Occur::Must) || alone);
                screen.node(clause, negated, required)?;
            }
        }
        node => screen.node(node, false, true)?,
    }
    if screen.positive == 0 {
        return Err(refused(
            "it names nothing to match — a delete needs at least one clause that is not negated",
        ));
    }
    Ok(screen.exact)
}

fn unboost(ast: &UserInputAst) -> &UserInputAst {
    match ast {
        UserInputAst::Boost(inner, _) => unboost(inner),
        other => other,
    }
}

struct Screen<'a> {
    schema: &'a IndexSchema,
    exact: Vec<ExactText>,
    positive: usize,
}

/// What kind of field a clause names, as far as a delete is concerned.
enum Kind {
    /// The document key, or a shadow of it.
    Key,
    /// Matched exactly by the index: string, numeric, date, ip, boolean.
    Exact,
    /// Analyzed text: a quoted phrase checked against the stored value.
    Text,
}

impl Screen<'_> {
    fn node(
        &mut self,
        ast: &UserInputAst,
        negated: bool,
        required: bool,
    ) -> Result<(), StoreError> {
        match ast {
            UserInputAst::Clause(clauses) => {
                for (occur, clause) in clauses {
                    let negated = negated || *occur == Some(Occur::MustNot);
                    self.node(clause, negated, false)?;
                }
                Ok(())
            }
            UserInputAst::Boost(inner, _) => self.node(inner, negated, required),
            UserInputAst::Leaf(leaf) => {
                self.leaf(leaf, negated, required)?;
                if !negated {
                    self.positive += 1;
                }
                Ok(())
            }
        }
    }

    fn leaf(
        &mut self,
        leaf: &UserInputLeaf,
        negated: bool,
        required: bool,
    ) -> Result<(), StoreError> {
        match leaf {
            UserInputLeaf::Literal(literal) => {
                let Some(field) = literal.field_name.as_deref() else {
                    return Err(refused(format!(
                        "'{}' names no field; a delete matches named fields only, as field:value",
                        literal.phrase
                    )));
                };
                if literal.prefix || literal.phrase.ends_with('*') {
                    return Err(refused(format!(
                        "{field}:{}* is a prefix; a delete matches whole values only",
                        literal.phrase.trim_end_matches('*')
                    )));
                }
                if literal.slop > 0 {
                    return Err(refused(format!(
                        "{field}:\"{}\"~{} is a phrase with slop; a delete matches whole values only",
                        literal.phrase, literal.slop
                    )));
                }
                match self.kind(field)? {
                    Kind::Key | Kind::Exact => Ok(()),
                    Kind::Text => {
                        if literal.delimiter != Delimiter::DoubleQuotes {
                            return Err(refused(format!(
                                "{field} is a text field, matched in a delete only as a quoted \
                                 phrase equal to the whole value: {field}:\"{}\"",
                                literal.phrase
                            )));
                        }
                        if negated || !required {
                            return Err(refused(format!(
                                "the text clause on {field} must be required — joined to the rest \
                                 with AND, not negated or one alternative of an OR — so each \
                                 match can be checked against the stored value"
                            )));
                        }
                        self.exact.push(ExactText {
                            field: field.to_string(),
                            value: literal.phrase.clone(),
                        });
                        Ok(())
                    }
                }
            }
            UserInputLeaf::Range { field, .. } => {
                let field = field.as_deref().unwrap_or("*");
                match self.kind(field)? {
                    Kind::Exact => Ok(()),
                    Kind::Key => Err(refused(format!(
                        "a range on {field}, the document key, is not allowed; name the ids, as \
                         {field}: IN [a b]"
                    ))),
                    Kind::Text => Err(refused(format!(
                        "a range on {field}, a text field, matches analyzed terms, not values"
                    ))),
                }
            }
            UserInputLeaf::Set { field, .. } => {
                let field = field.as_deref().unwrap_or("*");
                match self.kind(field)? {
                    Kind::Key | Kind::Exact => Ok(()),
                    Kind::Text => Err(refused(format!(
                        "an IN set on {field}, a text field, matches analyzed terms, not values"
                    ))),
                }
            }
            UserInputLeaf::All => Err(refused(
                "'*' matches every document; name what to delete, or drop the index",
            )),
            UserInputLeaf::Exists { field } => Err(refused(format!(
                "a presence test on {field} is not a value to match"
            ))),
            UserInputLeaf::Regex { field, .. } => Err(refused(format!(
                "a regex on {} matches patterns, not values",
                field.as_deref().unwrap_or("a field")
            ))),
        }
    }

    fn kind(&self, field: &str) -> Result<Kind, StoreError> {
        if field == "id" || self.schema.is_shadow_field(field) {
            return Ok(Kind::Key);
        }
        let Some(def) = self.schema.fields.get(field) else {
            return Err(refused(format!("{field} is not a field of this index")));
        };
        if def.is_ghost() {
            return Err(refused(format!(
                "{field} is a ghost field, an analyzed column of another field; match the source"
            )));
        }
        if !def.indexed {
            return Err(refused(format!(
                "{field} is not indexed, so nothing can match it"
            )));
        }
        match def.field_type {
            TantivyFieldType::String
            | TantivyFieldType::I64
            | TantivyFieldType::U64
            | TantivyFieldType::F64
            | TantivyFieldType::Date
            | TantivyFieldType::Ip
            | TantivyFieldType::Boolean => Ok(Kind::Exact),
            TantivyFieldType::Text => Ok(Kind::Text),
            TantivyFieldType::Json | TantivyFieldType::Bytes | TantivyFieldType::Facet => {
                Err(refused(format!(
                    "{field} is a {} field, which a delete does not match",
                    match def.field_type {
                        TantivyFieldType::Json => "json",
                        TantivyFieldType::Bytes => "bytes",
                        _ => "facet",
                    }
                )))
            }
        }
    }
}
