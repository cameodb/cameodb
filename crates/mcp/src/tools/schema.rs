//! What each tool accepts, stated twice: once as the JSON Schema a client reads, once as the
//! struct the dispatcher deserializes into. They sit together so the pair can be held to each
//! other rather than drifting in separate files.

use serde::Deserialize;
use serde_json::{Value as JsonValue, json};

use crate::backend::McpIndexSearchRequest;

#[derive(Debug, Deserialize)]
pub(crate) struct SearchIndexArgs {
    pub(crate) index: String,
    pub(crate) query: String,
    #[serde(default)]
    pub(crate) limit: Option<usize>,
    #[serde(default)]
    pub(crate) fields: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct SearchIndexesArgs {
    pub(crate) indexes: Vec<McpIndexSearchRequest>,
    pub(crate) query: String,
    #[serde(default)]
    pub(crate) limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct GetIndexArgs {
    pub(crate) index: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ValidateQueryArgs {
    #[serde(default)]
    pub(crate) index: Option<String>,
    #[serde(default)]
    pub(crate) partial_field: Option<String>,
    #[serde(default)]
    pub(crate) query: Option<String>,
}

#[derive(Debug, Deserialize)]
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
        "required": ["index", "query"]
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
                            "required": ["field"]
                        }
                    },
                    "required": ["index"]
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
        "required": ["indexes", "query"]
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
        "required": ["index"]
    })
}

pub(crate) fn list_indexes_input_schema() -> JsonValue {
    json!({
        "type": "object",
        "properties": {}
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
        }
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
        }
    })
}
