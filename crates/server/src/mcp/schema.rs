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
/// The common form behind both shapes a field arrives in — the schema's map of definitions and
/// the compact array [`enrich_index_entry`] projects it into — so that everything downstream
/// reads fields the same way whichever it was handed.
#[derive(Debug, Clone)]
pub(super) struct FieldInfo {
    pub(super) name: String,
    pub(super) field_type: String,
    pub(super) indexed: bool,
    pub(super) fast: bool,
    pub(super) is_shadow: bool,
    /// What the field records, if anyone wrote it down. Never inferred.
    pub(super) description: Option<String>,
}

impl FieldInfo {
    /// Whether a query may name this field.
    ///
    /// Every field an index is created with is searchable — `indexed` defaults to true, so a
    /// schema defined up front is queryable throughout. Two cases are reported rather than
    /// assumed: a field discovered from a document after creation is added unindexed and stays
    /// that way until a schema update promotes it, and a shadow field is searched through the
    /// identifier it duplicates rather than through the search index. Both are queryable
    /// answers to a caller; only the first is not.
    pub(super) fn is_queryable(&self) -> bool {
        self.indexed || self.is_shadow
    }
}

pub(super) fn extract_field_info(value: &JsonValue) -> Vec<FieldInfo> {
    let fields_obj = value
        .get("schema")
        .and_then(|schema| schema.get("fields"))
        .and_then(|fields| fields.as_object())
        .or_else(|| value.get("fields").and_then(|fields| fields.as_object()));

    let Some(fields_obj) = fields_obj else {
        // An entry that has already been through `enrich_index_entry` carries the compact
        // array rather than the schema map it was built from. Readers downstream of the
        // enrichment — `validate_query` and the statistics counts — see only that form, and
        // this is what lets them read the fields out of it.
        return compact_field_info(value);
    };

    let mut infos: Vec<FieldInfo> = fields_obj
        .iter()
        .map(|(name, def)| FieldInfo {
            name: name.clone(),
            // A schema serialises its type as the variant name — `Date`, `I64` — while every
            // syntax surface keys on the lowercase names in `cameodb_mcp::syntax`. Left as-is,
            // each field's hint reads "Unrecognised field type 'Date'".
            field_type: def
                .get("field_type")
                .and_then(|v| v.as_str())
                .unwrap_or("text")
                .to_ascii_lowercase(),
            indexed: def.get("indexed").and_then(|v| v.as_bool()).unwrap_or(true),
            fast: def.get("fast").and_then(|v| v.as_bool()).unwrap_or(false),
            is_shadow: def
                .get("is_shadow")
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
            description: def
                .get("description")
                .and_then(|v| v.as_str())
                .map(str::to_string),
        })
        .collect();

    infos.sort_by(
        |left, right| match (left.name.as_str(), right.name.as_str()) {
            ("id", "id") => std::cmp::Ordering::Equal,
            ("id", _) => std::cmp::Ordering::Less,
            (_, "id") => std::cmp::Ordering::Greater,
            _ => left.name.cmp(&right.name),
        },
    );

    infos
}

/// Read back the compact `fields` array that [`enrich_index_entry`] writes.
///
/// The inverse of the projection performed there, and deliberately not a second source of
/// truth: it recovers exactly the properties that form carries.
pub(super) fn compact_field_info(value: &JsonValue) -> Vec<FieldInfo> {
    let Some(entries) = value.get("fields").and_then(|fields| fields.as_array()) else {
        return Vec::new();
    };

    entries
        .iter()
        .filter_map(|entry| {
            let name = entry.get("field").and_then(|v| v.as_str())?;
            Some(FieldInfo {
                name: name.to_string(),
                field_type: entry
                    .get("type")
                    .and_then(|v| v.as_str())
                    .unwrap_or("text")
                    .to_ascii_lowercase(),
                indexed: entry
                    .get("indexed")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(true),
                fast: entry.get("fast").and_then(|v| v.as_bool()).unwrap_or(false),
                is_shadow: entry
                    .get("shadow")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false),
                description: entry
                    .get("description")
                    .and_then(|v| v.as_str())
                    .map(str::to_string),
            })
        })
        .collect()
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
pub(super) fn enrich_index_entry(mut entry: JsonValue) -> JsonValue {
    let field_infos = extract_field_info(&entry);

    // Collect unique field types present in this index
    let mut unique_types: std::collections::HashSet<String> = std::collections::HashSet::new();
    for info in &field_infos {
        if info.name != "_seq" {
            unique_types.insert(info.field_type.clone());
        }
    }

    // Build query hints for each unique field type
    let query_hints: Vec<JsonValue> = unique_types
        .iter()
        .map(|field_type| {
            serde_json::json!({
                "type": field_type,
                "query_hint": field_type_query_hint(field_type),
            })
        })
        .collect();

    let fields: Vec<JsonValue> = field_infos
        .iter()
        .filter(|info| info.name != "_seq")
        .map(|info| {
            let mut field = serde_json::json!({
                "field": info.name,
                "type": info.field_type,
                "indexed": info.indexed,
                "fast": info.fast,
                // On every field rather than only where true: a flag that appears by its absence
                // cannot be read as an answer, and `indexed: false` without it describes a field
                // nothing can query — the opposite of what a shadow field is.
                "shadow": info.is_shadow,
            });
            // Unlike the flags, present only when written. A description has no false value to
            // report, and an index nobody has described should not pay for a key per field.
            if let Some(text) = &info.description
                && let Some(obj) = field.as_object_mut()
            {
                obj.insert("description".to_string(), JsonValue::String(text.clone()));
            }
            field
        })
        .collect();

    // Lifted before the schema is dropped below, since that is where it was written.
    let description = entry
        .get("schema")
        .and_then(|schema| schema.get("description"))
        .and_then(|value| value.as_str())
        .map(str::to_string);

    if let Some(obj) = entry.as_object_mut() {
        if let Some(text) = description {
            obj.insert("description".to_string(), JsonValue::String(text));
        }
        obj.insert("fields".to_string(), JsonValue::Array(fields));
        obj.insert("query_hints".to_string(), JsonValue::Array(query_hints));
        // The schema map was the input to the projection above; keeping it as well would send
        // every field twice, which on a wide index is most of the response.
        obj.remove("schema");
    }

    entry
}

/// What a field of this type supports, for attaching to a schema listing.
///
/// Rendered from [`cameodb_mcp::syntax`], the one table every syntax surface reads.
pub(super) fn field_type_query_hint(field_type: &str) -> String {
    cameodb_mcp::syntax::hint_for_type(field_type)
}

/// What this particular field supports.
///
/// A shadow field takes the shadow rule rather than its type's operator list: it is not in the
/// search index, so none of the forms its type would otherwise support reach it.
pub(super) fn field_query_hint(info: &FieldInfo) -> String {
    if info.is_shadow {
        cameodb_mcp::syntax::SHADOW_FIELD.to_string()
    } else {
        field_type_query_hint(&info.field_type)
    }
}
