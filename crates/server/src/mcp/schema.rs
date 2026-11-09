//! What an index is, as the discovery tools describe it.
//!
//! One field arrives in two shapes — the schema's map of definitions, and the compact array the
//! tools project it into — and [`FieldInfo`] is the form everything downstream reads, so a caller
//! is described the same way whichever shape it came from.

use serde_json::Value as JsonValue;

use crate::node::{ClientOp, RemoteVerdict};
use crate::state::AppState;

/// An index's field definitions, or `Null` if they cannot be read.
///
/// The catalogue listing reports field *names* and nothing else, so describing an index means
/// composing it with this: types, the `searchable` and `sortable` flags and the hints all come
/// from the schema. The bundled CLI composes the same two calls to render `list indexes`.
///
/// `Null` rather than an error: a schema that cannot be read costs the caller its hints, not
/// its answer, and the statistics the entry does carry are still worth returning.
pub(super) async fn index_schema(state: &AppState, index: &str) -> JsonValue {
    state
        .router
        .handle_client_op(ClientOp::GetConfig {
            index: index.to_string(),
        })
        .await
        .unwrap_or(JsonValue::Null)
}

/// `Some(reason)` when the node has no such index, for the tools that read a schema rather than
/// search: `validate_query` and the discovery tools, whose answer for a missing index would
/// otherwise be a description of no fields.
///
/// A search needs no such check: the engine refuses an index it does not have, and
/// [`tool_error`](super::diagnostics::tool_error) carries that refusal to the caller. `GetConfig`
/// names the index rather than scanning the catalogue, so this is a single-index lookup.
pub(super) async fn absent_index_reason(state: &AppState, index: &str) -> Option<String> {
    match state
        .router
        .handle_client_op(ClientOp::GetConfig {
            index: index.to_string(),
        })
        .await
    {
        Ok(_) => None,
        // Worded as `describe_index` words it, since agreeing with that tool is the point.
        Err(err) if err.verdict() == RemoteVerdict::NotFound => {
            Some(format!("Index '{index}' not found"))
        }
        // Any other failure to read a schema is not evidence that the index is absent, and
        // reporting it as one would trade a silent wrong answer for a confident wrong answer.
        Err(_) => None,
    }
}

/// One field, as the discovery tools describe it.
///
/// Read from the single shape the engine now produces for every surface, so nothing downstream
/// has to know which endpoint the entry came from.
#[derive(Debug, Clone)]
pub(super) struct FieldInfo {
    pub(super) name: String,
    pub(super) field_type: String,
    /// The analyzer a text field declares; `None` for a text field taking `default` and for
    /// every type without one. A `string` field reports `raw`.
    pub(super) tokenizer: Option<String>,
    pub(super) is_shadow: bool,
    /// Whether a query can reach this field *now*, as opposed to `indexed`, which is what the
    /// schema declares. The two differ for a field declared after the index was built: it has no
    /// column until the index data is rebuilt, so it is `indexed` and matches nothing.
    pub(super) searchable: bool,
    /// Whether a sort on this field is exact — the built index has a fast column for it — as
    /// opposed to `fast`, which is what the schema declares. A text field without one is sorted
    /// approximately rather than refused, so this is the flag that decides whether an order can
    /// be trusted or paged through.
    pub(super) sortable: bool,
}

impl FieldInfo {
    /// Whether a query naming this field can match anything.
    ///
    /// This used to be `indexed || is_shadow`, which was the best guess available without the
    /// engine's help: it could not see that a declared-but-unbuilt field matches nothing, so it
    /// reported such a field as queryable and an agent querying it got silence. The engine now
    /// answers directly, and a shadow field remains queryable because it names the identifier,
    /// which is answered from the key-value store rather than the search index.
    pub(super) fn is_queryable(&self) -> bool {
        self.searchable || self.is_shadow
    }

    /// Whether a value must be written exactly as the data writes it — case, spaces and
    /// punctuation — and whole: a `string` field, or a `text` field under `raw`. Decided by how
    /// the field matches, not by its type, so a field declared either way gets the same help.
    /// A shadow field is the identifier, answered by key, and is left out.
    pub(super) fn matches_whole_value_as_written(&self) -> bool {
        !self.is_shadow
            && self.searchable
            && match self.field_type.as_str() {
                "string" | "exact" => true,
                "text" => self.tokenizer.as_deref() == Some("raw"),
                _ => false,
            }
    }
}

pub(super) fn extract_field_info(value: &JsonValue) -> Vec<FieldInfo> {
    // One shape to read. The engine describes every field the same way for the listing, the
    // schema endpoint and everything built on them, so this no longer has to guess whether it
    // was handed a schema map, an enriched entry, or a listing row.
    value
        .get("fields")
        .and_then(|fields| fields.as_array())
        .map(|fields| {
            fields
                .iter()
                .map(|def| FieldInfo {
                    name: def
                        .get("name")
                        .and_then(|v| v.as_str())
                        .unwrap_or_default()
                        .to_string(),
                    field_type: def
                        .get("type")
                        .and_then(|v| v.as_str())
                        .unwrap_or("text")
                        .to_string(),
                    tokenizer: def
                        .get("tokenizer")
                        .and_then(|v| v.as_str())
                        .map(str::to_string),
                    is_shadow: def.get("shadow").and_then(|v| v.as_bool()).unwrap_or(false),
                    searchable: def
                        .get("searchable")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(true),
                    sortable: def
                        .get("sortable")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false),
                })
                .filter(|info| !info.name.is_empty())
                .collect()
        })
        .unwrap_or_default()
}

pub(super) fn extract_field_names(value: &JsonValue) -> Vec<String> {
    extract_field_info(value)
        .into_iter()
        .map(|info| info.name)
        .collect()
}

/// The schema keys a field carries for the node that wrote it and that an agent cannot act on.
///
/// `indexed`, `stored` and `fast` are declarations, and each was read as an answer: `stored:
/// false` as "hits do not return this field", when every hit is read from the document store and
/// carries it; `indexed` and `fast` as "searchable" and "sortable", which the built index decides
/// and `searchable` and `sortable` report. `index_record_option` is how the column keeps
/// postings, which the tokenizer's rule already accounts for. A read of `GET /_config` keeps all
/// four, since that is the shape a schema is written back in.
const DECLARATION_KEYS: &[&str] = &["indexed", "stored", "fast", "index_record_option"];

/// The order a described field's keys come in; any key not named follows, as it came.
const FIELD_KEY_ORDER: &[&str] = &[
    "name",
    "type",
    "tokenizer",
    "description",
    "searchable",
    "sortable",
    "returned",
    "default_search",
    "shadow",
    "ghost",
    "returned_as",
];

/// Turn an index's schema block into the entry `describe_index` returns: the block as
/// `GET /_config` reports it, each field described by what a query can do with it rather than
/// by what its schema declares, with one `query_hint` per field type present and one
/// `tokenizer_hint` per tokenizer.
///
/// Per field: `searchable` and `sortable` from the built index, `returned` — whether a hit
/// carries it, false for a ghost alone — and the declaration keys removed
/// ([`DECLARATION_KEYS`]). `shadow` and `default_search` stay on every field, so a `false` is
/// an answer rather than an omission.
/// [`enrich_index_entry`] for an entry held by reference.
pub(super) fn enrich_index_entry_owned(entry: &JsonValue) -> JsonValue {
    enrich_index_entry(entry.clone())
}

pub(super) fn enrich_index_entry(mut entry: JsonValue) -> JsonValue {
    // Read before the declarations go: `tokenizer` stays, but the reading is done once.
    let field_infos = extract_field_info(&entry);

    if let Some(fields) = entry.get_mut("fields").and_then(JsonValue::as_array_mut) {
        for field in fields.iter_mut() {
            let Some(mut source) = field.as_object().cloned() else {
                continue;
            };
            for key in DECLARATION_KEYS {
                source.remove(*key);
            }
            let returned = !source.contains_key("ghost");
            source.insert("returned".to_string(), JsonValue::Bool(returned));
            // One key order on every field — what it is, what it holds, what a query can do with
            // it — so forty fields read as a table rather than forty differently shuffled objects.
            let mut ordered = serde_json::Map::new();
            for key in FIELD_KEY_ORDER {
                if let Some(value) = source.remove(*key) {
                    ordered.insert(key.to_string(), value);
                }
            }
            ordered.extend(source);
            *field = JsonValue::Object(ordered);
        }
    }

    // One hint per distinct type rather than per field: the hint depends on nothing else, so on
    // a wide index repeating it per field is the same paragraph a dozen times.
    let mut seen: Vec<String> = Vec::new();
    for info in &field_infos {
        if !seen.contains(&info.field_type) {
            seen.push(info.field_type.clone());
        }
    }
    seen.sort();

    let query_hints: Vec<JsonValue> = seen
        .iter()
        .map(|field_type| {
            serde_json::json!({
                "type": field_type,
                "query_hint": field_type_query_hint(field_type),
            })
        })
        .collect();

    // The same economy for tokenizers: the name on each field, the rule once. A string field
    // reports `raw`, so the whole-value rule is here whenever a string field is.
    let mut tokenizers: Vec<(&str, &str)> = Vec::new();
    for info in field_infos.iter().filter(|info| !info.is_shadow) {
        let name = match info.field_type.as_str() {
            "string" | "exact" => "raw",
            _ => info.tokenizer.as_deref().unwrap_or("default"),
        };
        if let Some(rule) =
            cameodb_mcp::syntax::tokenizer_rule(&info.field_type, info.tokenizer.as_deref())
            && !tokenizers.iter().any(|(seen, _)| *seen == name)
        {
            tokenizers.push((name, rule));
        }
    }
    tokenizers.sort();
    let tokenizer_hints: Vec<JsonValue> = tokenizers
        .into_iter()
        .map(|(tokenizer, matching)| serde_json::json!({"tokenizer": tokenizer, "matching": matching}))
        .collect();

    if let Some(obj) = entry.as_object_mut() {
        obj.insert("query_hints".to_string(), JsonValue::Array(query_hints));
        obj.insert(
            "tokenizer_hints".to_string(),
            JsonValue::Array(tokenizer_hints),
        );
    }

    entry
}

/// One index as the catalogue lists it: enough to choose between indexes, and no more.
///
/// The entry that arrives describes every field with its type and flags. Passing that straight
/// through would make the listing a `describe_index` per index delivered at once, which on a
/// catalogue of any size is most of an agent's context spent before it has decided which index to
/// look at — and the prompt tells it to start here.
///
/// What is kept is what choosing needs: the name, the operator's description of the dataset where
/// there is one, how many documents, and the field names. Types, flags and hints are one
/// `describe_index` away, on the index that turned out to matter.
///
/// The names go under `field_names`, not `fields`, because `fields` names objects everywhere
/// else. One key naming two shapes is how a reader comes to look for `fields[0].type` on a string.
pub(super) fn catalogue_entry(entry: &JsonValue) -> JsonValue {
    let field_names: Vec<String> = extract_field_info(entry)
        .into_iter()
        .map(|info| info.name)
        .collect();

    let mut out = serde_json::json!({
        "index": entry.get("name").cloned().unwrap_or(JsonValue::Null),
        "document_count": entry.get("document_count").cloned().unwrap_or(JsonValue::Null),
        "field_count": field_names.len(),
        "field_names": field_names,
    });

    if let Some(description) = entry.get("description")
        && let Some(obj) = out.as_object_mut()
    {
        obj.insert("description".to_string(), description.clone());
    }
    // Worth knowing before choosing: on this index a bare term searches only some of its text
    // fields, and `describe_index` says which.
    if entry.get("default_fields_truncated") == Some(&JsonValue::Bool(true))
        && let Some(obj) = out.as_object_mut()
    {
        obj.insert(
            "default_fields_truncated".to_string(),
            JsonValue::Bool(true),
        );
    }

    out
}

pub(super) fn field_type_query_hint(field_type: &str) -> String {
    cameodb_mcp::syntax::hint_for_type(field_type)
}

/// What this particular field supports.
///
/// A shadow field takes the shadow rule rather than its declared type's operator list, because
/// what it supports is `id`'s repertoire: the identifier is a raw string however the field is
/// declared, so exact matches, prefixes, ranges and sets work and phrases and slop do not. The
/// rule also carries the two things no operator list says — that a bare lookup skips the search
/// index, and that hits come back under this name instead of `id`.
pub(super) fn field_query_hint(info: &FieldInfo) -> String {
    if info.is_shadow {
        cameodb_mcp::syntax::SHADOW_FIELD.to_string()
    } else {
        field_type_query_hint(&info.field_type)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every tokenizer the engine accepts has a matching rule an agent can read. The rules live
    /// in the mcp crate, which cannot see the engine's list, so the two are pinned here.
    #[test]
    fn every_engine_tokenizer_has_a_matching_rule() {
        let rules: Vec<&str> = cameodb_mcp::syntax::TOKENIZER_RULES
            .iter()
            .map(|(name, _)| *name)
            .collect();
        for name in storage::TOKENIZERS {
            assert!(
                rules.contains(name),
                "no matching rule for tokenizer '{name}'"
            );
        }
        assert_eq!(
            rules.len(),
            storage::TOKENIZERS.len(),
            "a rule for no tokenizer: {rules:?}"
        );
    }

    #[test]
    fn a_described_field_says_what_a_query_can_do_with_it() {
        let entry = serde_json::json!({
            "name": "books",
            "fields": [
                {"name": "title", "type": "text", "indexed": true, "stored": false, "fast": false,
                 "shadow": false, "searchable": true, "sortable": false, "default_search": true,
                 "tokenizer": "en_stem", "index_record_option": "WithFreqsAndPositions"},
                {"name": "authors", "type": "string", "indexed": true, "stored": false,
                 "fast": false, "shadow": false, "searchable": true, "sortable": false,
                 "default_search": false, "tokenizer": "raw"},
                {"name": "text_hr", "type": "text", "indexed": true, "stored": false,
                 "fast": false, "shadow": false, "searchable": true, "sortable": false,
                 "default_search": true, "tokenizer": "hr_stem_fold",
                 "ghost": {"source": "title"}},
                {"name": "year", "type": "i64", "indexed": true, "stored": false, "fast": true,
                 "shadow": false, "searchable": true, "sortable": true, "default_search": false},
            ]
        });
        let described = enrich_index_entry(entry);
        let fields = described["fields"].as_array().unwrap();
        let field = |name: &str| fields.iter().find(|f| f["name"] == name).unwrap();

        for f in fields {
            for key in DECLARATION_KEYS {
                assert!(f.get(*key).is_none(), "{f}");
            }
        }
        assert_eq!(field("title")["returned"], true);
        assert_eq!(
            field("text_hr")["returned"],
            false,
            "a ghost is searched, never returned"
        );
        assert_eq!(field("year")["sortable"], true);
        assert_eq!(
            field("authors")["shadow"],
            false,
            "false is an answer, so it stays"
        );
        // Every field's keys in one order.
        let keys = |f: &JsonValue| f.as_object().unwrap().keys().cloned().collect::<Vec<_>>();
        assert_eq!(
            keys(field("year")),
            [
                "name",
                "type",
                "searchable",
                "sortable",
                "returned",
                "default_search",
                "shadow"
            ]
        );
        assert_eq!(
            keys(field("text_hr")),
            [
                "name",
                "type",
                "tokenizer",
                "searchable",
                "sortable",
                "returned",
                "default_search",
                "shadow",
                "ghost"
            ]
        );

        let tokenizers: Vec<&str> = described["tokenizer_hints"]
            .as_array()
            .unwrap()
            .iter()
            .map(|h| h["tokenizer"].as_str().unwrap())
            .collect();
        assert_eq!(tokenizers, ["en_stem", "hr_stem_fold", "raw"]);
        let raw = &described["tokenizer_hints"][2]["matching"];
        assert!(raw.as_str().unwrap().contains("case"), "{raw}");
    }
}
