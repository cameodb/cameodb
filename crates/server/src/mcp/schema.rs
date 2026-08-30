//! What an index is, as the discovery tools describe it.
//!
//! One field arrives in two shapes — the schema's map of definitions, and the compact array the
//! tools project it into — and [`FieldInfo`] is the form everything downstream reads, so a caller
//! is described the same way whichever shape it came from.

use serde_json::Value as JsonValue;

use crate::node_orchestrator::ClientOp;
use crate::state::AppState;

/// An index's field definitions, or `Null` if they cannot be read.
///
/// The catalogue listing reports field *names* and nothing else, so describing an index means
/// composing it with this: types, `indexed` and `fast` flags and per-type query hints all come
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

/// `Some(reason)` when the node has no such index, asked of a search that returned nothing.
///
/// The engine answers a search on an index it does not have with an empty result, which reads
/// to a caller exactly like a query that matched nothing — while `describe_index` on the same name
/// says the index is not there. This is what lets the two MCP tools give the same answer about
/// whether an index exists, in the one place where the difference is invisible.
///
/// Asked only where the result is empty, because that is the one answer in which "no such index"
/// and "nothing matched" are indistinguishable. A search that found something has already proved
/// the index exists and pays nothing for this. `GetConfig` names the index rather than scanning
/// the catalogue, so the check that is paid for is a single-index lookup.
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
        Err(err) if err.to_string().contains("not found") => {
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
    pub(super) indexed: bool,
    pub(super) fast: bool,
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
    /// What the field records, if anyone wrote it down. Never inferred.
    pub(super) description: Option<String>,
    /// The name a hit carries this field's value under, when it is not the field's own name.
    ///
    /// Set on `id` alone, and only on an index with a shadow field, where the identifier
    /// travels under the source's name and no hit carries an `id`. Everywhere else a field
    /// answers under its own name and this is `None`.
    pub(super) returned_as: Option<String>,
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
                    indexed: def.get("indexed").and_then(|v| v.as_bool()).unwrap_or(true),
                    fast: def.get("fast").and_then(|v| v.as_bool()).unwrap_or(false),
                    is_shadow: def.get("shadow").and_then(|v| v.as_bool()).unwrap_or(false),
                    searchable: def
                        .get("searchable")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(true),
                    sortable: def
                        .get("sortable")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false),
                    description: def
                        .get("description")
                        .and_then(|v| v.as_str())
                        .map(str::to_string),
                    returned_as: def
                        .get("returned_as")
                        .and_then(|v| v.as_str())
                        .map(str::to_string),
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

/// Turn `{index, stats, schema}` into the entry the discovery tools return.
///
/// Projects the schema's field map into a compact array — `field`, `type`, `indexed`, `fast`,
/// `shadow`, and `description` where one exists — lifts the index description alongside it, adds
/// one `query_hint` per field type present, and drops the schema it was built from. The
/// projection is the point: the schema map carries tokenizers and record options that no caller
/// writing a query can act on, and on a wide index they are most of the response.
/// [`enrich_index_entry`] for an entry held by reference.
pub(super) fn enrich_index_entry_owned(entry: &JsonValue) -> JsonValue {
    enrich_index_entry(entry.clone())
}

pub(super) fn enrich_index_entry(mut entry: JsonValue) -> JsonValue {
    let field_infos = extract_field_info(&entry);

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

    if let Some(obj) = entry.as_object_mut() {
        obj.insert("query_hints".to_string(), JsonValue::Array(query_hints));
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
