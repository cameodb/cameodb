//! The one description of CameoDB's query syntax, and the renderings taken from it.
//!
//! Every surface that tells a caller what a query may contain — the MCP tool descriptions, the
//! `validate_query` reference, the per-field hints attached to a schema, the orchestrator prompt
//! and the crate README — renders from the tables below rather than restating them. A claim is
//! therefore either true everywhere or false everywhere, and a change to engine behaviour is one
//! edit.
//!
//! This module lives in the mcp crate because the server crate depends on it and not the reverse.
//!
//! Entries describe what the engine does, which is not always what a Lucene-shaped query language
//! would do. [`NOT_SUPPORTED`] exists for that reason: the forms listed there are ones a caller
//! will reasonably try, and saying so costs less than a failed query.

use serde_json::{Value as JsonValue, json};

/// Schema type names, as they appear in an index's field definitions.
pub const TYPE_TEXT: &str = "text";
pub const TYPE_STRING: &str = "string";
pub const TYPE_I64: &str = "i64";
pub const TYPE_U64: &str = "u64";
pub const TYPE_F64: &str = "f64";
pub const TYPE_DATE: &str = "date";
pub const TYPE_BOOLEAN: &str = "boolean";
pub const TYPE_IP: &str = "ip";
pub const TYPE_JSON: &str = "json";
pub const TYPE_FACET: &str = "facet";

const ALL_TYPES: &[&str] = &[
    TYPE_TEXT,
    TYPE_STRING,
    TYPE_I64,
    TYPE_U64,
    TYPE_F64,
    TYPE_DATE,
    TYPE_BOOLEAN,
    TYPE_IP,
    TYPE_JSON,
    TYPE_FACET,
];

/// A query form the engine supports, and the field types it applies to.
#[derive(Debug, Clone, Copy)]
pub struct Operator {
    /// The form itself, written as a caller would type it.
    pub syntax: &'static str,
    pub summary: &'static str,
    pub examples: &'static [&'static str],
    /// Schema type names this form works against. Empty means it is not tied to a field.
    pub types: &'static [&'static str],
    /// A constraint that changes the result rather than merely explaining the form.
    pub caveat: Option<&'static str>,
}

/// A query form a caller is likely to try that the engine does not support.
#[derive(Debug, Clone, Copy)]
pub struct NotSupported {
    pub syntax: &'static str,
    /// What happens, and what to use instead.
    pub detail: &'static str,
}

/// Every supported query form.
pub const OPERATORS: &[Operator] = &[
    Operator {
        syntax: "term",
        summary: "Match a term; several terms are ANDed.",
        examples: &["rust database", "machine learning"],
        types: &[],
        caveat: Some(
            "Without a field name only text, string and json fields are searched. Numeric, date, \
             boolean, ip and facet fields must be named explicitly.",
        ),
    },
    Operator {
        syntax: "*",
        summary: "Match every document.",
        examples: &["*", "* limit 10"],
        types: &[],
        caveat: None,
    },
    Operator {
        syntax: "field:value",
        summary: "Match a value in one field.",
        examples: &["title:rust", "year:2024", "flag:true"],
        types: ALL_TYPES,
        caveat: None,
    },
    Operator {
        syntax: "field:\"a b\"",
        summary: "Exact phrase, terms in order. Text fields only.",
        examples: &["title:\"rust programming\""],
        types: &[TYPE_TEXT],
        caveat: Some("Needs positions, which string fields do not index."),
    },
    Operator {
        syntax: "field:\"a b\"~N",
        summary: "Phrase allowing N extra words between terms.",
        examples: &["body:\"small bike\"~2"],
        types: &[TYPE_TEXT],
        caveat: Some("A transposition costs 2, so \"a b\"~1 does not match \"b a\"."),
    },
    Operator {
        syntax: "\"a b pre\"*",
        summary: "Phrase whose last term is a prefix. Two or more terms.",
        examples: &["\"big bad wo\"*"],
        types: &[TYPE_TEXT],
        caveat: None,
    },
    Operator {
        syntax: "field:pre*",
        summary: "Match every term starting with `pre`. One term, and the field is required.",
        examples: &["title:data*", "tag:urn:cve*"],
        types: &[TYPE_TEXT, TYPE_STRING],
        caveat: Some(
            "Runs as a lexicographic range, so a short prefix scans a wide slice of the term \
             dictionary. On a stemmed field the prefix is stemmed too, which can move where it \
             lands.",
        ),
    },
    Operator {
        syntax: "AND / OR / NOT",
        summary: "Combine clauses. Uppercase only.",
        examples: &["title:rust AND year:2024", "title:rust NOT tag:draft"],
        types: &[],
        caveat: Some(
            "Lowercase is not reported: `a and b` is three terms, which widens the query, and \
             `a not b` matches everything.",
        ),
    },
    Operator {
        syntax: "+term / -term",
        summary: "Require or exclude a clause.",
        examples: &["+title:rust -tag:draft"],
        types: &[],
        caveat: None,
    },
    Operator {
        syntax: "(...)",
        summary: "Group clauses to control precedence.",
        examples: &["(title:rust OR title:go) AND year:[2020 TO 2024]"],
        types: &[],
        caveat: None,
    },
    Operator {
        syntax: "field:value^N",
        summary: "Weight a clause's score contribution.",
        examples: &["title:rust^3 OR body:rust"],
        types: ALL_TYPES,
        caveat: Some("Affects ranking only, never which documents match."),
    },
    Operator {
        syntax: "field:[low TO high]",
        summary: "Range, bounds inclusive.",
        examples: &["year:[2020 TO 2024]", "created:[2024-01-01 TO 2024-12-31]"],
        types: &[
            TYPE_TEXT,
            TYPE_STRING,
            TYPE_I64,
            TYPE_U64,
            TYPE_F64,
            TYPE_DATE,
            TYPE_IP,
        ],
        caveat: Some(
            "Text and string ranges compare lexicographically. The `fast` flag is needed for \
             sorting, not for ranges.",
        ),
    },
    Operator {
        syntax: "field:{low TO high}",
        summary: "Range, bounds exclusive.",
        examples: &["score:{0 TO 100}", "created:[2024-01-01 TO 2024-12-31}"],
        types: &[
            TYPE_TEXT,
            TYPE_STRING,
            TYPE_I64,
            TYPE_U64,
            TYPE_F64,
            TYPE_DATE,
            TYPE_IP,
        ],
        caveat: Some("Brackets and braces may be mixed to make one bound inclusive."),
    },
    Operator {
        syntax: "field:[low TO *]",
        summary: "Range with one side unbounded.",
        examples: &["price:[10.0 TO *]", "year:[* TO 2024]"],
        types: &[
            TYPE_TEXT,
            TYPE_STRING,
            TYPE_I64,
            TYPE_U64,
            TYPE_F64,
            TYPE_DATE,
            TYPE_IP,
        ],
        caveat: None,
    },
    Operator {
        syntax: "field:>value",
        summary: "Comparison: `>` `<` `>=` `<=`.",
        examples: &["age:>=18", "score:<100", "created:>2024-01-01"],
        types: &[TYPE_I64, TYPE_U64, TYPE_F64, TYPE_DATE],
        caveat: None,
    },
    Operator {
        syntax: "field: IN [a b c]",
        summary: "Match any of several values.",
        examples: &["status: IN [active pending]", "year: IN [2023 2024]"],
        types: &[
            TYPE_TEXT,
            TYPE_STRING,
            TYPE_I64,
            TYPE_U64,
            TYPE_F64,
            TYPE_DATE,
            TYPE_BOOLEAN,
        ],
        caveat: None,
    },
    Operator {
        syntax: "field:/path/to/value",
        summary: "Match a facet path.",
        examples: &["category:/electronics/phones", "category:/electronics"],
        types: &[TYPE_FACET],
        caveat: Some("Hierarchical: a parent path matches its descendants."),
    },
    Operator {
        syntax: "id:value",
        summary: "Look up one document by id. Fastest retrieval path.",
        examples: &["id:doc-12345"],
        types: &[],
        caveat: Some(
            "Reads the key-value store and skips the search index, but only when the whole query \
             is exactly `id:value`, with no other clause and no space, quote or parenthesis in \
             the value.",
        ),
    },
];

/// Forms a caller is likely to try that do not work, and what to do instead.
pub const NOT_SUPPORTED: &[NotSupported] = &[
    NotSupported {
        syntax: "field:*",
        detail: "Field-presence tests are not supported for any field type. The clause is \
                 dropped and reported. Use a bounded range, or match an explicit value.",
    },
    NotSupported {
        syntax: "pre*",
        detail: "A prefix needs a field name; without one the `*` is dropped and `pre` is matched \
                 as a whole term. Name the field, or OR one clause per field.",
    },
    NotSupported {
        syntax: "field.subfield:value",
        detail: "Paths into a json field are not queryable. A json field is searchable only as \
                 unstructured text, so `field:value` matches any key or value inside it.",
    },
    NotSupported {
        syntax: "field:/regex/",
        detail: "Regular expressions are disabled.",
    },
];

/// Characters the query grammar reserves. A literal one must be escaped with a backslash.
pub const RESERVED_CHARACTERS: &str = "+ ^ ` : { } \" ' [ ] ( ) ! \\ * and space";

/// Facts about querying that belong to no single operator.
pub const RULES: &[&str] = &[
    "A field name containing a dot is written as it is, unescaped: `k8s.node:worker-1`. Escaping \
     the dot makes the lookup miss.",
    "A field that exists in the schema but is not indexed cannot be queried. Fields discovered \
     from a document are added unindexed, and stay that way until a schema update promotes them, \
     so check the `indexed` flag before naming a field.",
    "`_seq` is an internal sequence number used to track write-ahead-log position. It is present \
     in every index and technically queryable, but it carries no meaning for a search and should \
     be ignored.",
    "`AND`, `OR`, `NOT`, `TO` and `IN` are keywords in uppercase only. Lowercase is query text: \
     `to` and `in` break the clause around them and are reported, while `and`, `or` and `not` are \
     searched for as ordinary words and change what the query means without any warning.",
    "A clause the engine cannot interpret is dropped and the rest of the query runs, which widens \
     a conjunction and disables a negation. Every dropped clause is reported: the HTTP API \
     attaches `_discarded_clauses` to the response, and an MCP tool call fails with the reason. \
     Results are never returned as though the query had been understood.",
];

/// How ordering behaves, which the `sort` modifier and parameter share.
pub const SORT_RULES: &[&str] = &[
    "Sorting on a numeric or date field requires the `fast` flag on that field, reported per field \
     by `get_index`.",
    "Sorting on a text or string field is approximate: the top `2 × limit` matches by relevance \
     are collected and then ordered alphabetically, so the result is not the alphabetically first \
     documents in the index.",
    "Under a numeric or date sort every hit carries `_score` of 1.0, because no relevance score is \
     computed. Do not read it as a ranking.",
    "Ascending unless `desc` is given.",
];

/// Inline modifiers CameoDB accepts after the query itself.
pub const INLINE_MODIFIERS: &[(&str, &str, &str)] = &[
    (
        "return f1,f2",
        "Return only these fields, in this order.",
        "title:rust return title,author",
    ),
    (
        "limit N",
        "Cap the number of results.",
        "title:rust limit 5",
    ),
    (
        "sort field:desc",
        "Order by a field. See the sorting rules.",
        "title:rust sort year:desc",
    ),
];

/// How the inline modifiers are told apart from query text.
pub const MODIFIER_RULES: &[&str] = &[
    "A modifier counts only where it opens an unbroken run of clauses reaching the end of the \
     query, with query text left in front of it. Anything else is searched for, so `find tax \
     return forms` is four terms and `* limit 10` is how to ask for a bare limit.",
    "A field list needs a comma between names, a limit needs a number, and a sort order must be \
     exactly `asc` or `desc`. A clause that does not parse stays in the query rather than being \
     applied in part.",
    "A modifier naming a field the index does not have is reported as a dropped clause, since a \
     projection would otherwise return documents with no fields.",
];

/// Schema type names an index may use, with what each is for.
pub const FIELD_TYPES: &[(&str, &str)] = &[
    (
        TYPE_TEXT,
        "Tokenized full text. Terms are split and lowercased.",
    ),
    (TYPE_STRING, "Exact value, not tokenized."),
    (TYPE_I64, "Signed 64-bit integer."),
    (TYPE_U64, "Unsigned 64-bit integer."),
    (TYPE_F64, "64-bit float."),
    (
        TYPE_DATE,
        "Date or datetime. `YYYY-MM-DD` and RFC3339 are both accepted.",
    ),
    (TYPE_BOOLEAN, "`true` or `false`."),
    (TYPE_IP, "IPv4 or IPv6 address."),
    (
        TYPE_JSON,
        "Nested object, searchable only as unstructured text — its keys and values are indexed as \
         a bag of words and paths into it cannot be queried.",
    ),
    (TYPE_FACET, "Hierarchical category path."),
];

/// Markers delimiting the generated syntax table in the crate README.
pub const README_BEGIN: &str = "<!-- BEGIN GENERATED SYNTAX -->";
pub const README_END: &str = "<!-- END GENERATED SYNTAX -->";

/// The forms that work against one schema type.
pub fn operators_for(field_type: &str) -> Vec<&'static Operator> {
    let field_type = normalize_type(field_type);
    OPERATORS
        .iter()
        .filter(|op| op.types.contains(&field_type))
        .collect()
}

/// `exact` is an accepted spelling of `string`; numeric aliases collapse to themselves.
fn normalize_type(field_type: &str) -> &str {
    match field_type {
        "exact" => TYPE_STRING,
        other => other,
    }
}

/// One line naming what a field of this type supports, for attaching to a schema listing.
pub fn hint_for_type(field_type: &str) -> String {
    let normalized = normalize_type(field_type);
    let forms = operators_for(normalized);
    if forms.is_empty() {
        return format!(
            "Unrecognised field type '{field_type}'. Use `field:value` and check the index schema."
        );
    }

    let summary = FIELD_TYPES
        .iter()
        .find(|(name, _)| *name == normalized)
        .map(|(_, summary)| *summary)
        .unwrap_or("");

    let syntaxes: Vec<&str> = forms.iter().map(|op| op.syntax).collect();
    let mut hint = format!("{summary} Supports: {}.", syntaxes.join(", "));

    // Only caveats belonging to a form this type actually supports.
    let caveats: Vec<&str> = forms.iter().filter_map(|op| op.caveat).collect();
    if !caveats.is_empty() {
        hint.push(' ');
        hint.push_str(&caveats.join(" "));
    }
    hint
}

/// The full reference, as `validate_query` returns it.
pub fn reference_json() -> JsonValue {
    let operators: Vec<JsonValue> = OPERATORS
        .iter()
        .map(|op| {
            json!({
                "syntax": op.syntax,
                "summary": op.summary,
                "examples": op.examples,
                "field_types": if op.types.is_empty() { json!("any") } else { json!(op.types) },
                "caveat": op.caveat,
            })
        })
        .collect();

    let not_supported: Vec<JsonValue> = NOT_SUPPORTED
        .iter()
        .map(|form| json!({ "syntax": form.syntax, "detail": form.detail }))
        .collect();

    let field_types: Vec<JsonValue> = FIELD_TYPES
        .iter()
        .map(|(name, summary)| {
            json!({
                "type": name,
                "description": summary,
                "supported": operators_for(name).iter().map(|op| op.syntax).collect::<Vec<_>>(),
            })
        })
        .collect();

    let inline: Vec<JsonValue> = INLINE_MODIFIERS
        .iter()
        .map(|(syntax, summary, example)| {
            json!({ "syntax": syntax, "summary": summary, "example": example })
        })
        .collect();

    json!({
        "operators": operators,
        "not_supported": not_supported,
        "field_types": field_types,
        "inline_modifiers": inline,
        "inline_modifier_rules": MODIFIER_RULES,
        "sorting": SORT_RULES,
        "rules": RULES,
        "reserved_characters": RESERVED_CHARACTERS,
    })
}

/// A short reference for a tool description.
///
/// Summaries only: caveats, examples, per-type tables and the full rule text are omitted, since
/// this text is resident in the caller's context for a whole session while `validate_query` and
/// `get_index` supply the detail on demand. Only the traps that change a result silently — a
/// dropped clause, an unindexed field, an approximate sort — are worth their space here.
pub fn compact_reference() -> String {
    let mut out = String::with_capacity(1536);
    out.push_str("QUERY SYNTAX\n");
    for op in OPERATORS {
        out.push_str(&format!("  {:<22} {}\n", op.syntax, op.summary));
    }
    out.push_str("\nNOT SUPPORTED — these are dropped, not errors in the query\n");
    for form in NOT_SUPPORTED {
        out.push_str(&format!("  {}\n", form.syntax));
    }
    out.push_str("\nINLINE MODIFIERS, in one run at the end of the query\n");
    for (syntax, summary, _) in INLINE_MODIFIERS {
        out.push_str(&format!("  {syntax:<22} {summary}\n"));
    }
    out.push_str(
        "\nTRAPS\n  \
         A clause the engine cannot interpret is dropped, not rejected — the call fails and names \
         it.\n  \
         `AND` `OR` `NOT` `TO` `IN` count only in uppercase; lowercase is searched for as a word, \
         and for `and`, `or` and `not` that happens silently.\n  \
         An unindexed field cannot be queried; check the `indexed` flag from `get_index`.\n  \
         Sorting a text or string field is approximate, and sets every `_score` to 1.0.\n  \
         A modifier is searched for as text unless its whole run parses, so give a field list its \
         commas and a sort an `asc` or `desc`.\n",
    );
    out.push_str(&format!(
        "\nEscape these to match them literally: {RESERVED_CHARACTERS}\n"
    ));
    out.push_str(
        "\nCall `validate_query` with no arguments for the full reference including examples, \
         per-type operators and sorting rules; `get_index` for one index's fields and types.\n",
    );
    out
}

/// The reference as markdown, for the crate README and the agent skill.
pub fn markdown_reference() -> String {
    let mut out = String::with_capacity(4096);
    out.push_str("| Syntax | Meaning | Field types |\n|---|---|---|\n");
    for op in OPERATORS {
        let types = if op.types.is_empty() {
            "any".to_string()
        } else {
            op.types.join(", ")
        };
        out.push_str(&format!(
            "| `{}` | {} | {} |\n",
            op.syntax, op.summary, types
        ));
    }
    out.push_str("\n**Not supported**\n\n");
    for form in NOT_SUPPORTED {
        out.push_str(&format!("- `{}` — {}\n", form.syntax, form.detail));
    }
    out.push_str("\n**Inline modifiers**, in one run at the end of the query\n\n");
    out.push_str("| Syntax | Meaning | Example |\n|---|---|---|\n");
    for (syntax, summary, example) in INLINE_MODIFIERS {
        out.push_str(&format!("| `{syntax}` | {summary} | `{example}` |\n"));
    }
    out.push('\n');
    for rule in MODIFIER_RULES {
        out.push_str(&format!("- {rule}\n"));
    }
    out.push_str("\n**Rules**\n\n");
    for rule in RULES {
        out.push_str(&format!("- {rule}\n"));
    }
    out.push_str("\n**Sorting**\n\n");
    for rule in SORT_RULES {
        out.push_str(&format!("- {rule}\n"));
    }
    out.push_str(&format!(
        "\nReserved characters, which must be escaped with a backslash to be matched \
         literally: {RESERVED_CHARACTERS}\n"
    ));
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A form that names no field type would be missing from every per-field hint, and a type no
    /// operator claims would render a hint with nothing in it.
    #[test]
    fn every_field_type_supports_at_least_one_operator() {
        for (name, _) in FIELD_TYPES {
            assert!(
                !operators_for(name).is_empty(),
                "no operator claims to work on '{name}'"
            );
        }
    }

    #[test]
    fn operators_only_name_declared_field_types() {
        for op in OPERATORS {
            for claimed in op.types {
                assert!(
                    FIELD_TYPES.iter().any(|(name, _)| name == claimed),
                    "operator {:?} names unknown field type '{claimed}'",
                    op.syntax
                );
            }
        }
    }

    /// `exact` is what a schema may call a string field; a hint for it must not be the fallback.
    #[test]
    fn the_string_alias_resolves() {
        assert_eq!(hint_for_type("exact"), hint_for_type(TYPE_STRING));
        assert!(!hint_for_type("exact").contains("Unrecognised"));
    }

    #[test]
    fn an_unknown_type_gets_a_usable_fallback() {
        let hint = hint_for_type("something_else");
        assert!(hint.contains("something_else"));
        assert!(hint.contains("field:value"));
    }

    /// The renderings are what callers read, so each must actually carry the tables rather than
    /// silently coming out empty.
    #[test]
    fn every_rendering_covers_the_tables() {
        let compact = compact_reference();
        let markdown = markdown_reference();
        let reference = reference_json();

        for op in OPERATORS {
            assert!(compact.contains(op.syntax), "compact omits {:?}", op.syntax);
            assert!(
                markdown.contains(op.syntax),
                "markdown omits {:?}",
                op.syntax
            );
        }
        for form in NOT_SUPPORTED {
            assert!(
                compact.contains(form.syntax),
                "compact omits the unsupported form {:?}",
                form.syntax
            );
            assert!(
                markdown.contains(form.syntax),
                "markdown omits the unsupported form {:?}",
                form.syntax
            );
        }
        assert_eq!(
            reference["operators"].as_array().map(Vec::len),
            Some(OPERATORS.len())
        );
        assert_eq!(
            reference["not_supported"].as_array().map(Vec::len),
            Some(NOT_SUPPORTED.len())
        );
        assert_eq!(
            reference["field_types"].as_array().map(Vec::len),
            Some(FIELD_TYPES.len())
        );
    }

    /// Forms verified as broken must stay listed as unsupported: an agent that tries one gets a
    /// dropped clause, so the reference has to say so rather than stay silent.
    #[test]
    fn the_known_broken_forms_are_listed_as_unsupported() {
        for syntax in ["field:*", "pre*", "field.subfield:value"] {
            assert!(
                NOT_SUPPORTED.iter().any(|form| form.syntax == syntax),
                "{syntax} must be listed as unsupported"
            );
        }
        // And must not also appear as a supported operator.
        for op in OPERATORS {
            assert!(
                !NOT_SUPPORTED.iter().any(|form| form.syntax == op.syntax),
                "{:?} is listed as both supported and unsupported",
                op.syntax
            );
        }
    }
}
