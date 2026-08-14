//! What each tool accepts, stated twice: once as the JSON Schema a client reads, once as the
//! struct the dispatcher deserializes into. They sit together so the pair can be held to each
//! other rather than drifting in separate files, which the tests below do for every tool: the
//! advertised properties are the accepted fields, and an argument is optional in the schema
//! exactly when it is optional in the struct.
//!
//! Both halves are closed. Each struct refuses a field it does not know and each schema says
//! `additionalProperties: false`, so a client is told in advance what a caller learns from the
//! error. The reason to be strict is that every argument here changes what a search returns:
//! a `limit` that arrives as `limt` would otherwise be dropped, and the caller would read ten
//! results as though they were all of them.

use serde::Deserialize;
use serde_json::{Value as JsonValue, json};

use crate::backend::McpIndexSearchRequest;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SearchIndexArgs {
    pub(crate) index: String,
    pub(crate) query: String,
    #[serde(default)]
    pub(crate) limit: Option<usize>,
    #[serde(default)]
    pub(crate) fields: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SearchIndexesArgs {
    pub(crate) indexes: Vec<McpIndexSearchRequest>,
    pub(crate) query: String,
    #[serde(default)]
    pub(crate) limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GetIndexArgs {
    pub(crate) index: String,
}

/// `list_indexes` takes nothing, and says so by refusing everything.
///
/// A tool with no parameters still decodes its arguments, because the alternative is to ignore
/// them: a call carrying `{"index": "docs"}` would then return the whole catalogue and read as
/// though the filter had been applied.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ListIndexesArgs {}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ValidateQueryArgs {
    #[serde(default)]
    pub(crate) index: Option<String>,
    #[serde(default)]
    pub(crate) partial_field: Option<String>,
    #[serde(default)]
    pub(crate) query: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GetIndexStatsArgs {
    #[serde(default)]
    pub(crate) index: Option<String>,
}

pub(crate) fn search_index_input_schema() -> JsonValue {
    json!({
        "type": "object",
        "properties": {
            "index": {
                "type": "string",
                "description": "Name of the CameoDB index to search."
            },
            "query": {
                "type": "string",
                "description": "Search query string. Supports field:value, phrases, AND/OR/NOT, ranges, and inline 'return'/'limit'/'sort' modifiers. Use 'limit 0' for count-only queries. Inline sort: 'sort field:asc' or 'sort field:desc'."
            },
            "limit": {
                "type": "integer",
                "minimum": 0,
                "description": "Maximum number of results to return. Pass 0 for count-only mode (returns total_hits without document data). If omitted, the node's configured default applies, which is 10 unless an operator changed it."
            },
            "fields": {
                "type": "array",
                "items": { "type": "string" },
                "description": "Field names to include in results (field projection). Fields are returned in the exact order specified. Metadata fields (like '_score') are always included automatically."
            }
        },
        "required": ["index", "query"],
        "additionalProperties": false
    })
}

pub(crate) fn search_indexes_input_schema() -> JsonValue {
    json!({
        "type": "object",
        "properties": {
            "indexes": {
                "type": "array",
                "description": "List of indexes to search, each with optional field projection.",
                "items": {
                    "type": "object",
                    "properties": {
                        "index": {
                            "type": "string",
                            "description": "Name of the CameoDB index."
                        },
                        "fields": {
                            "type": "array",
                            "items": { "type": "string" },
                            "description": "Fields to include from this index. Fields are returned in the exact order specified. Metadata fields (like '_score') are always included automatically."
                        },
                        "sort": {
                            "type": "object",
                            "description": "Sort results by a field within this index. Supported types: u64, i64, f64, date (FAST fields), and text/string (alphabetic sort).",
                            "properties": {
                                "field": {
                                    "type": "string",
                                    "description": "Field name to sort by. Supports u64, i64, f64, date, and text/string fields."
                                },
                                "order": {
                                    "type": "string",
                                    "enum": ["asc", "desc"],
                                    "description": "Sort order. Defaults to asc."
                                }
                            },
                            "required": ["field"],
                            "additionalProperties": false
                        }
                    },
                    "required": ["index"],
                    "additionalProperties": false
                }
            },
            "query": {
                "type": "string",
                "description": "Search query applied to all specified indexes."
            },
            "limit": {
                "type": "integer",
                "minimum": 0,
                "description": "Maximum total results across all indexes. Pass 0 for count-only mode (returns total_hits without document data). If omitted, the node's configured default applies, which is 10 unless an operator changed it."
            }
        },
        "required": ["indexes", "query"],
        "additionalProperties": false
    })
}

pub(crate) fn get_index_input_schema() -> JsonValue {
    json!({
        "type": "object",
        "properties": {
            "index": {
                "type": "string",
                "description": "Name of the CameoDB index."
            }
        },
        "required": ["index"],
        "additionalProperties": false
    })
}

pub(crate) fn list_indexes_input_schema() -> JsonValue {
    json!({
        "type": "object",
        "properties": {},
        "additionalProperties": false
    })
}

pub(crate) fn validate_query_input_schema() -> JsonValue {
    json!({
        "type": "object",
        "properties": {
            "index": {
                "type": "string",
                "description": "Index name for schema-aware field validation. Optional."
            },
            "partial_field": {
                "type": "string",
                "description": "Partial field name for autocomplete suggestions."
            },
            "query": {
                "type": "string",
                "description": "Query string to validate and analyze."
            }
        },
        "additionalProperties": false
    })
}

pub(crate) fn get_index_stats_input_schema() -> JsonValue {
    json!({
        "type": "object",
        "properties": {
            "index": {
                "type": "string",
                "description": "Index name. If omitted, returns aggregated statistics for all indexes."
            }
        },
        "additionalProperties": false
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use serde::de::DeserializeOwned;
    use serde_json::Map as JsonMap;

    use super::*;
    use crate::backend::SortSpec;

    /// A schema and the struct it feeds, paired so the two can be compared.
    ///
    /// The struct is erased behind its field set and a decode function because the tools take
    /// different argument types and a table of them cannot be typed; `contract` below is the
    /// one place the type is still known.
    struct Contract {
        label: &'static str,
        schema: JsonValue,
        accepted: BTreeSet<String>,
        decode: Box<dyn Fn(JsonValue) -> Result<(), String>>,
    }

    fn contract<T: DeserializeOwned + 'static>(label: &'static str, schema: JsonValue) -> Contract {
        Contract {
            label,
            schema,
            accepted: accepted_fields::<T>(),
            decode: Box::new(|value| {
                serde_json::from_value::<T>(value)
                    .map(|_| ())
                    .map_err(|err| err.to_string())
            }),
        }
    }

    /// The field names a struct accepts, read out of the error it raises when offered one it
    /// does not.
    ///
    /// `deny_unknown_fields` makes serde name every field it knows in that message, which is
    /// the only runtime view of a struct's field set — there is nothing to ask instead. If the
    /// wording ever changes, the extraction yields a set that matches no schema and the
    /// comparisons below fail, which is the direction for this to break in.
    fn accepted_fields<T: DeserializeOwned>() -> BTreeSet<String> {
        let refusal = serde_json::from_value::<T>(json!({"__not_a_field__": null}))
            .err()
            .map(|err| err.to_string())
            .unwrap_or_else(|| panic!("an unknown field was accepted; deny_unknown_fields?"));
        // Every name in the message is backticked. The first is the field just offered.
        refusal
            .split('`')
            .skip(3)
            .step_by(2)
            .map(str::to_string)
            .collect()
    }

    fn advertised_properties(schema: &JsonValue) -> BTreeSet<String> {
        schema["properties"]
            .as_object()
            .expect("a schema describes its properties")
            .keys()
            .cloned()
            .collect()
    }

    fn required_properties(schema: &JsonValue) -> Vec<String> {
        schema
            .get("required")
            .and_then(JsonValue::as_array)
            .map(|names| {
                names
                    .iter()
                    .filter_map(JsonValue::as_str)
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default()
    }

    /// The smallest value the schema accepts for a property, built from its declared type.
    ///
    /// Synthesised rather than written out so the samples cannot become a third statement of
    /// what a tool accepts, drifting from the two this file already holds.
    fn sample(schema: &JsonValue) -> JsonValue {
        // A closed set of values is the only thing the struct will take, so take from it.
        if let Some(permitted) = schema.get("enum").and_then(JsonValue::as_array)
            && let Some(first) = permitted.first()
        {
            return first.clone();
        }
        match schema.get("type").and_then(JsonValue::as_str) {
            Some("string") => json!("x"),
            Some("integer") | Some("number") => json!(1),
            Some("boolean") => json!(true),
            Some("array") => json!([]),
            Some("object") => required_only(schema),
            other => panic!("no sample for schema type {other:?}"),
        }
    }

    /// An object carrying every property the schema requires and none that it does not.
    fn required_only(schema: &JsonValue) -> JsonValue {
        let mut object = JsonMap::new();
        for name in required_properties(schema) {
            object.insert(name.clone(), sample(&schema["properties"][&name]));
        }
        JsonValue::Object(object)
    }

    /// Every advertised schema with the struct that decodes calls against it, including the
    /// two nested objects inside the federated search — a client reads those the same way and
    /// they can drift the same way.
    fn contracts() -> Vec<Contract> {
        let federated = search_indexes_input_schema();
        let per_index = federated["properties"]["indexes"]["items"].clone();
        let sort = per_index["properties"]["sort"].clone();

        vec![
            contract::<SearchIndexArgs>("search_index", search_index_input_schema()),
            contract::<SearchIndexesArgs>("search_indexes", federated),
            contract::<McpIndexSearchRequest>("search_indexes.indexes[]", per_index),
            contract::<SortSpec>("search_indexes.indexes[].sort", sort),
            contract::<GetIndexArgs>("get_index", get_index_input_schema()),
            contract::<ListIndexesArgs>("list_indexes", list_indexes_input_schema()),
            contract::<ValidateQueryArgs>("validate_query", validate_query_input_schema()),
            contract::<GetIndexStatsArgs>("get_index_stats", get_index_stats_input_schema()),
        ]
    }

    #[test]
    fn a_schema_advertises_exactly_the_arguments_its_struct_accepts() {
        for Contract {
            label,
            schema,
            accepted,
            ..
        } in contracts()
        {
            assert_eq!(
                advertised_properties(&schema),
                accepted,
                "{label}: the advertised arguments and the accepted ones differ, so a client \
                 either reads about one it cannot send or sends one it was never told about"
            );
        }
    }

    #[test]
    fn a_schema_closes_itself_the_way_its_struct_does() {
        for Contract { label, schema, .. } in contracts() {
            assert_eq!(
                schema.get("additionalProperties"),
                Some(&JsonValue::Bool(false)),
                "{label} does not advertise itself as closed, so a client is not told that a \
                 misspelled argument will be refused rather than ignored"
            );
        }
    }

    #[test]
    fn a_schema_requires_exactly_what_its_struct_cannot_do_without() {
        for Contract {
            label,
            schema,
            decode,
            ..
        } in contracts()
        {
            let minimal = required_only(&schema);
            assert!(
                decode(minimal.clone()).is_ok(),
                "{label}: the schema's required arguments alone do not decode: {:?}",
                decode(minimal.clone())
            );

            for name in required_properties(&schema) {
                let mut without = minimal.clone();
                without.as_object_mut().unwrap().remove(&name);
                assert!(
                    decode(without).is_err(),
                    "{label}: '{name}' is advertised as required but the struct does without it"
                );
            }

            let required = required_properties(&schema);
            for name in advertised_properties(&schema) {
                if required.contains(&name) {
                    continue;
                }
                let mut with = minimal.clone();
                with.as_object_mut()
                    .unwrap()
                    .insert(name.clone(), sample(&schema["properties"][&name]));
                assert!(
                    decode(with).is_ok(),
                    "{label}: '{name}' is advertised as optional, but sending it as the type \
                     the schema declares is refused"
                );
            }
        }
    }
}
