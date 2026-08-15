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

use crate::backend::{McpIndexSearchRequest, SortSpec};

/// The largest `limit` a search tool accepts when the host names no other number.
///
/// A limit is a promise to hold that many hits in memory, merge them and serialize them, so an
/// unbounded one is a caller choosing how much work the node does for a single request. What
/// the ceiling should be is a deployment question — how much the node can afford — so the host
/// answers it through [`McpBackend::max_search_limit`](crate::McpBackend::max_search_limit) and
/// this is only the answer for a host that does not.
///
/// Whatever the number is, both search schemas advertise it as `maximum` and the dispatcher
/// enforces it, so a client is never refused for exceeding a bound it was not shown.
pub const DEFAULT_MAX_SEARCH_LIMIT: usize = 10_000;

/// The `limit` a search runs with when the call names none.
///
/// The number the schema descriptions have always quoted to callers ("If omitted, defaults to
/// 10"). It is a constant here because `offset` is bounded against `offset + limit`, so the
/// dispatcher has to know what an omitted `limit` will become — see
/// [`McpBackend::default_search_limit`](crate::McpBackend::default_search_limit), which a host
/// overrides when its own default differs.
pub const DEFAULT_SEARCH_LIMIT: usize = 10;

/// The most indexes one federated search may name.
///
/// Each name is a full scatter-gather across that index's shards, so the argument is a
/// multiplier on everything the call costs. A caller that wants the whole catalogue is asking
/// a different question — `list_indexes` answers it in one request.
pub const MAX_FEDERATED_INDEXES: usize = 20;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SearchIndexArgs {
    pub(crate) index: String,
    pub(crate) query: String,
    #[serde(default)]
    pub(crate) limit: Option<usize>,
    /// How many ordered hits to skip before the first one returned (paging offset).
    #[serde(default)]
    pub(crate) offset: Option<usize>,
    #[serde(default)]
    pub(crate) fields: Option<Vec<String>>,
    /// The same structured sort the federated tool takes per index.
    ///
    /// One index is the common case, so this is where a sort is most often wanted; the
    /// federated tool having it and this one not is an asymmetry a caller reads as "sorting a
    /// single index is done some other way".
    #[serde(default)]
    pub(crate) sort: Option<SortSpec>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SearchAcrossIndexesArgs {
    pub(crate) indexes: Vec<McpIndexSearchRequest>,
    pub(crate) query: String,
    #[serde(default)]
    pub(crate) limit: Option<usize>,
    /// How many ordered hits to skip before the first one returned (paging offset).
    #[serde(default)]
    pub(crate) offset: Option<usize>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DescribeIndexArgs {
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

/// `get_catalog_stats` takes nothing, for the same reason `list_indexes` does.
///
/// It answers about the catalogue. One index's statistics are part of describing that index, and
/// a tool that answered both would be two tools sharing a name — and eventually disagreeing.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GetCatalogStatsArgs {}

/// The sort object both search tools take.
///
/// Written once because the two tools must describe it identically — they decode it into the
/// same struct, and the drift tests compare each schema with that struct.
fn sort_schema() -> JsonValue {
    json!({
        "type": "object",
        "description": "Sort results by a field. Supported types: u64, i64, f64, date (FAST fields), and text/string (alphabetic sort).",
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
    })
}

pub(crate) fn search_index_input_schema(max_search_limit: usize) -> JsonValue {
    json!({
        "type": "object",
        "properties": {
            "index": {
                "type": "string",
                "description": "Name of the CameoDB index to search."
            },
            "query": {
                "type": "string",
                "description": "Search query string. Supports field:value, phrases, AND/OR/NOT, ranges, and inline 'return'/'limit'/'offset'/'sort' modifiers. Use 'limit 0' for count-only queries. Inline sort: 'sort field:asc' or 'sort field:desc'; inline paging: 'limit 10 offset 20'. An argument wins over the same modifier written inline."
            },
            "limit": {
                "type": "integer",
                "minimum": 0,
                "maximum": max_search_limit,
                "description": format!(
                    "Maximum number of results to return, up to {max_search_limit}. Pass 0 for count-only mode (returns total_hits without document data). If omitted, the node's configured default applies, which is 10 unless an operator changed it."
                )
            },
            "offset": {
                "type": "integer",
                "minimum": 0,
                "description": format!(
                    "How many hits to skip before the first one returned. With `limit` as the page size, page N is `offset = N * limit`. There are more results when `offset + hits_returned < total_hits`, and an offset at or past `total_hits` returns an empty page with a `_warning` saying so rather than an error. The engine fetches offset + limit hits, so `offset + limit` must not exceed {max_search_limit} — a deep page costs what a large limit costs. Sorting is what makes a page mean anything: without a `sort` the order is by relevance, which is stable for one query against unchanged data but not across writes. Defaults to 0."
                )
            },
            "fields": {
                "type": "array",
                "items": { "type": "string" },
                "description": "Field names to include in results (field projection). Fields are returned in the exact order specified. Metadata fields (like '_score') are always included automatically."
            },
            "sort": sort_schema()
        },
        "required": ["index", "query"],
        "additionalProperties": false
    })
}

pub(crate) fn search_across_indexes_input_schema(max_search_limit: usize) -> JsonValue {
    json!({
        "type": "object",
        "properties": {
            "indexes": {
                "type": "array",
                "minItems": 1,
                "maxItems": MAX_FEDERATED_INDEXES,
                "description": format!(
                    "The indexes to search, at least one and at most {MAX_FEDERATED_INDEXES}. An entry is either a bare index name, or an object naming one with a field projection and a sort of its own. Naming the same index twice is refused rather than searched twice."
                ),
                "items": {
                    "oneOf": [
                        {
                            "type": "string",
                            "description": "The name of an index to search with no per-index options."
                        },
                        {
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
                                "sort": sort_schema()
                            },
                            "required": ["index"],
                            "additionalProperties": false
                        }
                    ]
                }
            },
            "query": {
                "type": "string",
                "description": "Search query applied to all specified indexes."
            },
            "limit": {
                "type": "integer",
                "minimum": 0,
                "maximum": max_search_limit,
                "description": format!(
                    "Maximum total results across all indexes, up to {max_search_limit}. Pass 0 for count-only mode (returns total_hits without document data). If omitted, the node's configured default applies, which is 10 unless an operator changed it."
                )
            },
            "offset": {
                "type": "integer",
                "minimum": 0,
                "description": format!(
                    "How many hits to skip before the first one returned, applied once after merging across every index — so page N of a federated search is page N of the combined order, not of each index separately. With `limit` as the page size, page N is `offset = N * limit`. Each index is asked for offset + limit hits, so `offset + limit` must not exceed {max_search_limit}. Defaults to 0."
                )
            }
        },
        "required": ["indexes", "query"],
        "additionalProperties": false
    })
}

/// What every search result carries, whichever tool produced it.
///
/// Declared so the contract stops being prose inside a description string: a client reading the
/// catalogue learns that `total_hits` counts matches while `hits_returned` counts what came
/// back, and that a trimmed response says so — rather than having to be told in a paragraph it
/// may not have been given.
///
/// `additionalProperties` is left open deliberately. A hit is the stored document, whose fields
/// belong to the index rather than to this protocol, and the envelope also carries engine
/// diagnostics that are not a promise to anyone. What is written out here is what a caller may
/// rely on; what is not written out is not thereby forbidden.
fn search_result_properties() -> JsonValue {
    json!({
        "hits": {
            "type": "array",
            "description": "The matching documents, highest ranked first, or in sort order when one was requested. Each carries `_score`; a projection returns the named fields in the order given.",
            "items": { "type": "object" }
        },
        "hits_returned": {
            "type": "integer",
            "description": "How many hits this response carries. Below `total_hits` when a limit applied or the response was trimmed."
        },
        "total_hits": {
            "type": "integer",
            "description": "How many documents matched, which is not how many were returned. Unaffected by the limit or by trimming."
        },
        "limit": {
            "type": "integer",
            "description": "The limit the search ran with, whether it came from the argument, an inline modifier, or the node's default."
        },
        "offset": {
            "type": "integer",
            "description": "The offset the search ran with — how many hits were skipped before the first one returned. There are more results when `offset + hits_returned < total_hits`; the next page is `offset + limit`. Defaults to 0 when no offset was requested."
        },
        "_warning": {
            "type": "string",
            "description": "Present when the response needs explaining: nothing matched and something narrowed the query, the page starts past the end of the result, the order is approximate, or hits were left out. Read it before reporting the result."
        },
        "_approximate_sort": {
            "type": "string",
            "description": "Present when the hits are sorted on a field with no fast column, naming that field. The order is then the alphabetical order of the highest-scoring candidates rather than of everything that matched, so a document that belongs first may be absent and each page re-orders a different sample. Do not report such an order as ranked, and do not page through it. `describe_index` reports `sortable: false` for the field."
        },
        "_truncated": {
            "type": "boolean",
            "description": "Present and true when hits were left out to keep the response within the largest message this node sends. The hits returned are the front of the same order."
        },
        "_omitted_hits": {
            "type": "integer",
            "description": "How many hits were left out by trimming. Narrow the query to see them rather than treating what was returned as the whole result."
        }
    })
}

pub(crate) fn search_index_output_schema() -> JsonValue {
    let mut schema = json!({
        "type": "object",
        "properties": search_result_properties(),
        "required": ["hits", "hits_returned", "total_hits"],
        "additionalProperties": true
    });
    if let Some(properties) = schema["properties"].as_object_mut() {
        properties.insert(
            "errors".to_string(),
            json!({
                "type": "array",
                "description": "Shards that could not be read, absent when every shard answered — so its presence means part of the answer is missing.",
                "items": { "type": "string" }
            }),
        );
    }
    schema
}

pub(crate) fn search_across_indexes_output_schema() -> JsonValue {
    let mut schema = json!({
        "type": "object",
        "properties": search_result_properties(),
        "required": ["hits", "hits_returned", "total_hits"],
        "additionalProperties": true
    });
    if let Some(properties) = schema["properties"].as_object_mut() {
        properties.insert(
            "errors".to_string(),
            json!({
                "type": "array",
                "description": "Indexes that could not be read, each naming the index and why. Absent when every index answered, so its presence means part of the answer is missing. A search where every index failed is an error rather than a result.",
                "items": {
                    "type": "object",
                    "properties": {
                        "index": { "type": "string" },
                        "error": { "type": "string" }
                    }
                }
            }),
        );
        // Only the federated tool can say which index a hit came from.
        if let Some(hits) = properties.get_mut("hits")
            && let Some(hits) = hits.as_object_mut()
        {
            hits.insert(
                "description".to_string(),
                JsonValue::String(
                    "The merged hits, highest ranked first, or in sort order when one was requested. Each carries `_score` and `_index_source` naming the index it came from."
                        .to_string(),
                ),
            );
        }
    }
    schema
}

pub(crate) fn describe_index_input_schema() -> JsonValue {
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

pub(crate) fn get_catalog_stats_input_schema() -> JsonValue {
    json!({
        "type": "object",
        "properties": {},
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
        let federated = search_across_indexes_input_schema(DEFAULT_MAX_SEARCH_LIMIT);
        // The object branch of the entry's `oneOf`. A bare name has no properties to compare;
        // the object form is the half that has to agree with the struct.
        let per_index = federated["properties"]["indexes"]["items"]["oneOf"][1].clone();
        let sort = per_index["properties"]["sort"].clone();

        vec![
            contract::<SearchIndexArgs>(
                "search_index",
                search_index_input_schema(DEFAULT_MAX_SEARCH_LIMIT),
            ),
            contract::<SearchAcrossIndexesArgs>("search_across_indexes", federated),
            contract::<McpIndexSearchRequest>("search_across_indexes.indexes[]", per_index),
            contract::<SortSpec>("search_across_indexes.indexes[].sort", sort),
            contract::<DescribeIndexArgs>("describe_index", describe_index_input_schema()),
            contract::<ListIndexesArgs>("list_indexes", list_indexes_input_schema()),
            contract::<ValidateQueryArgs>("validate_query", validate_query_input_schema()),
            contract::<GetCatalogStatsArgs>("get_catalog_stats", get_catalog_stats_input_schema()),
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

    /// The advertised bounds are the enforced ones.
    ///
    /// A `maximum` a client reads and a maximum the dispatcher applies are the same drift as a
    /// property and a struct field: the schema is where a caller learns what it may ask for, so
    /// a number written into it by hand is a promise nothing keeps. The ceiling is checked
    /// against a value no constant here could supply, since it is the host's to choose.
    #[test]
    fn a_schema_advertises_the_bounds_that_are_enforced() {
        let hosts_own_ceiling = 4_242;
        for (label, schema) in [
            ("search_index", search_index_input_schema(hosts_own_ceiling)),
            (
                "search_across_indexes",
                search_across_indexes_input_schema(hosts_own_ceiling),
            ),
        ] {
            assert_eq!(
                schema["properties"]["limit"]["maximum"],
                json!(hosts_own_ceiling),
                "{label} advertises a limit bound that is not the host's"
            );
            assert!(
                schema["properties"]["limit"]["description"]
                    .as_str()
                    .is_some_and(|text| text.contains(&hosts_own_ceiling.to_string())),
                "{label} describes a limit bound that is not the host's: {schema}"
            );
        }

        let indexes =
            &search_across_indexes_input_schema(DEFAULT_MAX_SEARCH_LIMIT)["properties"]["indexes"];
        assert_eq!(
            indexes["minItems"],
            json!(1),
            "an empty index list must be refused by the schema as well as the dispatcher"
        );
        assert_eq!(
            indexes["maxItems"],
            json!(MAX_FEDERATED_INDEXES),
            "the advertised fan-out bound is not the enforced one"
        );
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
