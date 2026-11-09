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
    /// Constraints that change the result rather than merely explaining the form, each for the
    /// types it is true of.
    pub caveats: &'static [Caveat],
}

/// A constraint on one operator, and the field types it applies to.
///
/// A caveat used to be one paragraph per operator, appended whole to the hint of every type the
/// operator supports, so an integer field's hint explained how text ranges compare and a string
/// field's how a stemmed prefix moves. Scoped to its types, each type's hint says only what is
/// true of that type.
#[derive(Debug, Clone, Copy)]
pub struct Caveat {
    /// The types it is true of; empty for every type the operator supports.
    pub types: &'static [&'static str],
    pub text: &'static str,
}

impl Caveat {
    const fn all(text: &'static str) -> Self {
        Self { types: &[], text }
    }

    const fn on(types: &'static [&'static str], text: &'static str) -> Self {
        Self { types, text }
    }

    fn applies_to(&self, field_type: &str) -> bool {
        self.types.is_empty() || self.types.contains(&field_type)
    }
}

impl Operator {
    /// Every caveat, joined, for the reference, which lists each operator once.
    pub fn caveat_text(&self) -> Option<String> {
        (!self.caveats.is_empty()).then(|| {
            self.caveats
                .iter()
                .map(|caveat| caveat.text)
                .collect::<Vec<_>>()
                .join(" ")
        })
    }
}

/// A query form a caller is likely to try that the engine does not support.
#[derive(Debug, Clone, Copy)]
pub struct NotSupported {
    pub syntax: &'static str,
    /// What happens, and what to use instead.
    pub detail: &'static str,
}

/// Types a range or comparison orders by value rather than by text.
const ORDERED_VALUE_TYPES: &[&str] = &[TYPE_I64, TYPE_U64, TYPE_F64, TYPE_DATE, TYPE_IP];

/// Every supported query form.
pub const OPERATORS: &[Operator] = &[
    Operator {
        syntax: "term",
        summary: "Match a term; several terms are ORed, so a document matching any one of them \
                  is returned.",
        examples: &["rust database", "machine learning"],
        types: &[],
        caveats: &[Caveat::all(
            "Without a field name only text, string and json fields are searched. Numeric, date, \
             boolean, ip and facet fields must be named explicitly. Adding a term widens the \
             result rather than narrowing it: put `AND` between clauses, or `+` in front of each, \
             to require them all.",
        )],
    },
    Operator {
        syntax: "*",
        summary: "Match every document.",
        examples: &["*", "* limit 10"],
        types: &[],
        caveats: &[],
    },
    Operator {
        syntax: "field:value",
        summary: "Match a value in one field.",
        examples: &["title:rust", "year:2024", "flag:true"],
        types: ALL_TYPES,
        caveats: &[],
    },
    Operator {
        syntax: "field:\"a b\"",
        summary: "Exact phrase, terms in order. Text fields only.",
        examples: &["title:\"rust programming\""],
        types: &[TYPE_TEXT],
        caveats: &[Caveat::all(
            "Needs word positions, which a text field keeps unless its schema declares otherwise.",
        )],
    },
    Operator {
        syntax: "field:\"a b\"~N",
        summary: "Phrase allowing N extra words between terms.",
        examples: &["body:\"small bike\"~2"],
        types: &[TYPE_TEXT],
        caveats: &[Caveat::all(
            "A transposition costs 2, so \"a b\"~1 does not match \"b a\".",
        )],
    },
    Operator {
        syntax: "\"a b pre\"*",
        summary: "Phrase whose last term is a prefix. Two or more terms.",
        examples: &["\"big bad wo\"*"],
        types: &[TYPE_TEXT],
        caveats: &[],
    },
    Operator {
        syntax: "field:pre*",
        summary: "Match every term starting with `pre`. One term, and the field is required.",
        examples: &["title:data*", "tag:urn:cve*"],
        types: &[TYPE_TEXT, TYPE_STRING],
        caveats: &[
            Caveat::all(
                "Runs as a lexicographic range, so a short prefix scans a wide slice of the term \
                 dictionary. A prefix shorter than the node's `min_prefix_length` (2 characters \
                 by default) is not expanded: it is matched as that term exactly, and reported.",
            ),
            Caveat::on(
                &[TYPE_TEXT],
                "The prefix goes through the field's tokenizer: lowercased, and on a stemmed \
                 field stemmed too, which can move where it lands. A prefix the tokenizer cannot \
                 reduce to one term is matched as that term exactly instead, and reported.",
            ),
            Caveat::on(
                &[TYPE_STRING],
                "Case-sensitive, like the value: `Data*` and `data*` find different values.",
            ),
        ],
    },
    Operator {
        syntax: "AND / OR / NOT",
        summary: "Combine clauses. Uppercase only.",
        examples: &[
            "title:rust AND year:2024",
            "title:rust AND NOT tag:draft",
            "title:rust OR NOT tag:draft",
            "NOT tag:draft",
        ],
        types: &[],
        caveats: &[Caveat::all(
            "`a AND NOT b` keeps what matches a and not b, `a OR NOT b` adds everything not \
             matching b, `a NOT b` and `NOT b` alone are exclusions. Lowercase is not reported: \
             `a and b` is three terms, which widens the query, and `a not b` matches everything.",
        )],
    },
    Operator {
        syntax: "+term / -term",
        summary: "Require or exclude a clause.",
        examples: &["+title:rust -tag:draft"],
        types: &[],
        caveats: &[],
    },
    Operator {
        syntax: "(...)",
        summary: "Group clauses to control precedence.",
        examples: &["(title:rust OR title:go) AND year:[2020 TO 2024]"],
        types: &[],
        caveats: &[],
    },
    Operator {
        syntax: "field:value^N",
        summary: "Weight a clause's score contribution.",
        examples: &["title:rust^3 OR body:rust"],
        types: ALL_TYPES,
        caveats: &[Caveat::all(
            "Affects ranking only, never which documents match.",
        )],
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
        caveats: &[
            Caveat::on(
                &[TYPE_TEXT, TYPE_STRING],
                "Ranges and comparisons compare terms lexicographically, as text. A bound \
                 holding a space or punctuation is quoted — `category:[\"Film & Animation\" TO \
                 Music]` — in every range and comparison form.",
            ),
            Caveat::on(
                ORDERED_VALUE_TYPES,
                "Ranges and comparisons need no sort column: they work whether or not the field \
                 is `sortable`.",
            ),
        ],
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
        caveats: &[Caveat::all(
            "Brackets and braces may be mixed to make one bound inclusive.",
        )],
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
        caveats: &[],
    },
    Operator {
        syntax: "field:>value",
        summary: "Comparison: `>` `<` `>=` `<=`.",
        examples: &["age:>=18", "score:<100", "created:>2024-01-01", "id:>9000"],
        types: &[
            TYPE_TEXT,
            TYPE_STRING,
            TYPE_I64,
            TYPE_U64,
            TYPE_F64,
            TYPE_DATE,
        ],
        // Compared as a range is: the range's caveats say how, once for both.
        caveats: &[],
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
        caveats: &[],
    },
    Operator {
        syntax: "field:/path/to/value",
        summary: "Match a facet path.",
        examples: &["category:/electronics/phones", "category:/electronics"],
        types: &[TYPE_FACET],
        caveats: &[Caveat::all(
            "Hierarchical: a parent path matches its descendants. The path ends at the first \
             space, so a path containing one cannot be queried.",
        )],
    },
    Operator {
        syntax: "id:value",
        summary: "Look up one document by id. Fastest retrieval path.",
        examples: &["id:doc-12345"],
        types: &[],
        caveats: &[Caveat::all(
            "Reads the key-value store and skips the search index, but only when the whole query \
             is exactly `id:value`, with no other clause and no space, quote, parenthesis, `*` \
             or `^` in the value — those are syntax, so the query runs on the search index \
             instead, where a trailing `*` is a prefix over the identifiers and `^` is a boost. \
             An escape resolves the same way on both paths, so an identifier containing one of \
             them is reached by escaping it: `id:doc\\^2`. A `shadow` field is the identifier \
             under its source name and works the same way, alone or rewritten to `id` inside a \
             larger query.",
        )],
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
        syntax: "field:[* TO *]",
        detail: "A range open at both ends is the same presence test, dropped and reported. \
                 Bound it on one side from a value no document is below — `year:[0 TO *]`, \
                 `tag:[\"\" TO *]`.",
    },
    NotSupported {
        syntax: "json_field:[a TO b]",
        detail: "A json field takes no range or comparison; the clause is dropped and reported. \
                 Match a value: `json_field:value`.",
    },
    NotSupported {
        syntax: "pre*",
        detail: "A prefix with no field name searches the default fields, unless the node has \
                 turned `expand_unqualified_prefix` off; then the `*` is dropped, `pre` is matched \
                 as a whole term, and reported. Naming the field always works.",
    },
    NotSupported {
        syntax: "field:*suffix",
        detail: "Leading and inner wildcards (`*fix`, `pre*fix`) are not supported. The `*` is \
                 dropped, the rest is matched as written, and reported. Only a trailing `*` on \
                 a field-qualified term is a prefix.",
    },
    NotSupported {
        syntax: "field.subfield:value",
        detail: "Paths into a json field are not queryable: a json field is indexed as the words \
                 of its value, so a clause on a path — value, phrase, set or range — is dropped \
                 and reported. `field:value` matches any key or value inside it.",
    },
    NotSupported {
        syntax: "field:/regex/",
        detail: "Regular expressions are disabled.",
    },
];

/// Characters the query grammar reserves. A literal one must be escaped with a backslash.
pub const RESERVED_CHARACTERS: &str = "+ ^ ` : { } \" ' [ ] ( ) ! \\ * and space";

/// What a field marked `shadow` is, how it is queried, and how it comes back.
///
/// A shadow field is the name the source data used for its identifier. On write that value moves
/// into `id`, the key in both the key-value store and the search index, and is not stored or
/// indexed a second time under the descriptive name — the schema keeps the name, nothing keeps a
/// copy of the data, which is what makes the field a shadow. The name is then applied to `id` in
/// both directions: a query naming it resolves to `id`, and a document returns its identifier
/// under it.
///
/// Its own constant because it is both a rule about querying and the per-field hint a schema
/// listing attaches, and the two must not drift. Deliberately not derived from the field's type:
/// a shadow field is absent from the search index, so none of the operators its type would
/// otherwise support reach it.
pub const SHADOW_FIELD: &str = "A field marked `shadow` is the name the source data used for its identifier. The value lives \
     only in `id`, the key in both the key-value store and the search index, and is never stored \
     or indexed again under the descriptive name — the schema carries the name and nothing carries \
     a second copy of the data, which is what makes the field a shadow. Querying it is therefore \
     querying `id`: `shadowfield:VALUE` on its own is the fastest retrieval CameoDB has, answered \
     from the key-value store without the search index, and named inside a larger query the \
     reference is rewritten to `id` and runs against the search index like any other clause. A \
     trailing `*` is a prefix over the identifiers, which a short one pays for in the term \
     dictionary the way any prefix query does. Results come back the same way round: every \
     hit carries the identifier under the shadow name and has no `id` field, so name the shadow \
     field in `fields` or `return` — asking for `id` there is rewritten to the shadow name and \
     returns the identifier all the same. `describe_index` says which name that is: on such an \
     index the `id` field carries `returned_as`, naming the shadow field the hits use in its \
     place, and it is the only thing relating the two. Sorting by a shadow field \
     works and orders by the identifier, which is the one thing an unindexed field can be used \
     for beyond the key lookup: the order is approximate, as it is for any text field with no \
     fast column, and `_approximate_sort` names the field the hits carry that order under — \
     the shadow name, whichever of the two names you sorted by.";

/// What a ghost field is, for a caller who meets the `ghost` property in a schema.
pub const GHOST_FIELD: &str = "A field with a `ghost` property is a search column filled from \
     another field, its `source`, rather than from a value of its own: from every document, or \
     from those whose `when.field` is (`in`) or is not (`not_in`) one of the listed values — one \
     stemmed column per language beside a text stored once, for instance. Query it like any text \
     field; it is analysed by its own tokenizer. It is never stored, so hits do not carry it: \
     project, sort and pivot on the source, which holds the text. A query naming an unindexed \
     source says which ghosts search its text instead.";

/// Facts about querying that belong to no single operator.
pub const RULES: &[&str] = &[
    "A field name containing a dot is written as it is, unescaped: `k8s.node:worker-1`. Escaping \
     the dot makes the lookup miss.",
    "A field that is not `searchable` cannot be queried, unless it is marked `shadow`. Fields \
     discovered from a document are added unindexed, and stay that way until a schema update \
     promotes them, so check the `searchable` flag before naming a field. Whether a hit carries \
     a field is a separate question: every field is `returned` but a ghost.",
    "Values are read from the data, not guessed from the question. A `string` field matches only \
     its whole value as written, so a value in another case, or a part of one, finds nothing — \
     and an empty result there more often means the value is written differently than that the \
     data is absent. To see how a field's values are written, search for rows that hold it with \
     `limit 5` and the field in `fields`, then copy the value; a prefix in the right case \
     (`field:Ab*`) finds values from their start. A field's `description`, where it has one, \
     often lists its values. For a numeric or date field, `metrics` with `limit 0` gives its \
     lowest and highest value over the matches.",
    SHADOW_FIELD,
    GHOST_FIELD,
    "A text field whose `tokenizer` starts with `cjk` holds Chinese, Japanese or Korean. \
     Write a word as its characters, with no spaces: `text_zh:孔子` matches only where 孔 and 子 \
     stand together, and a single character matches wherever it stands. Spaces between the \
     characters make each a separate clause that matches anywhere in the text. Under `cjk_fold` \
     traditional and simplified Chinese find each other (`學習` finds `学习`), under \
     `cjk_ja_fold` old and new kanji (`清吉` finds `淸吉`), under `cjk_ko_fold` Hanja and Hangul \
     (`가족` finds `家族`); under `cjk` none do.",
    "A term with no field in front of it searches the index's default fields, which may be fewer \
     than all of its text fields: an index can declare them, and a node caps how many are \
     searched. Each field's `default_search` flag says whether a bare term reaches it, and a \
     search the cap narrowed carries `_narrowed_default_fields` naming the fields it reached — \
     name the field to search one that it does not.",
    "Every date literal form works wherever a date value goes: alone, in a range, after a \
     comparison, and in an `IN` set. But a date matched *alone* is an exact instant, so \
     `created:2024-06-15` means midnight precisely and matches nothing unless a document sits on \
     that second. A day, a month or a year is a range: `created:[2024-06-15 TO 2024-06-16}`, \
     `created:[2024 TO 2025}`.",
    "A date literal containing a space must be quoted — `created:\"2024-06-15 12:00:00\"` — \
     because an unquoted value ends at the first space, and the remainder is read as a new \
     clause: `created:2024-06-15 12:00:00` reports `12` as an unknown field. Writing the `T` \
     avoids the question: `created:2024-06-15T12:00:00`.",
    "A date outside the range Tantivy represents is clamped to the nearest bound rather than \
     refused, on the way in and in a query: before 1677-09-21 becomes that instant, after \
     2262-04-11 becomes that one. So a far-future sentinel compares as though it were \
     2262-04-11, and `created:>9999-01-01` matches nothing rather than erroring.",
];

/// How ordering behaves, which the `sort` modifier and parameter share.
pub const SORT_RULES: &[&str] = &[
    "Sorting is exact on a field with a fast column, and `describe_index` reports which those are \
     as `sortable`. A text or string field without one is sorted approximately rather than \
     refused; every other type without one is refused.",
    "A sort the index cannot answer is a refusal, not a degraded result: the request fails with \
     `cannot sort by 'FIELD'` and the reason — the index has no column of that name, or the type \
     needs a fast column it does not have. It is decided before any shard runs, so no partial \
     answer comes back with it. Boolean, bytes, ip, json and facet fields are the ones this \
     catches in practice. Retry on a different field rather than re-sending.",
    "An approximate sort collects the top `2 × limit` matches by relevance and orders those \
     alphabetically, so the result is not the alphabetically first documents in the index and \
     paging through it re-orders a different sample on each page. The response says so: it \
     carries `_approximate_sort` naming the field, and a `_warning`.",
    "A field's fast column is written when the index is built, so `sortable` cannot be turned on \
     for an index that already has data. Declare the field `fast` before writing to it — which \
     works for text, string and the numeric and date types. It does nothing on a boolean, bytes, \
     ip, json or facet field: no column is built for those, so `fast` on one reads back as \
     `false`.",
    "Under a numeric or date sort every hit carries `_score` of 1.0, because no relevance score is \
     computed. Do not read it as a ranking.",
    "Ascending unless `desc` is given.",
];

/// How `metrics` — the min and max of fields over a search's matches — behaves (ROADMAP K).
pub const METRIC_RULES: &[&str] = &[
    "`metrics: {\"min\": [fields], \"max\": [fields]}` on `search_index` answers the lowest and \
     highest value of each field over every document the query matches — not just the page — as \
     `metrics` beside `total_hits`. With `limit: 0` it is the count and the range alone.",
    "It applies to i64, u64, f64 and date fields with a fast column: the numeric and date fields \
     `describe_index` reports `sortable`. Any other field, text and string included, is refused \
     with `cannot take the min or max of 'FIELD'` and the reason, before any shard runs.",
    "An integer comes back exact at any size, and a date as RFC 3339 in UTC. An array field counts \
     every element. A field no matching document holds is `null`, which is not 0.",
    "`search_across_indexes` does not take it: its rows come from different indexes.",
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
        "Cap the number of results. `limit 0` is count-only: `total_hits` and no hits, skipping \
         the key-value store, which is the cheapest way to ask how many documents match.",
        "title:rust limit 5",
    ),
    (
        "offset K",
        "Skip the first K results — the page, where `limit` is the page size. `offset + limit` \
         is bounded by the same maximum `limit` is.",
        "title:rust limit 10 offset 20",
    ),
    (
        "sort field:desc",
        "Order by a field. See the sorting rules.",
        "title:rust sort year:desc",
    ),
    (
        "array N",
        "Keep at most N elements of each array in a hit, the first ones in stored order, at any \
         depth — a cut array leads with the elements the query matched unless `matches off`. A hit that lost elements carries `_truncated_arrays`, naming each shortened array \
         by its path with the length it had; `array 0` is the lengths without the elements. \
         Without it the node's default applies, and N is bounded by a maximum the way `limit` is.",
        "string_ipv4:10.0.0.1 array 50",
    ),
    (
        "matches off",
        "Keep each cut array's head. By default an array that `array` cuts leads with the \
         elements the query matched, in stored order, then the rest of the cap, and the hit names \
         their stored positions in `_matched_elements`; an array that is not cut keeps its order. \
         `matches on` asks for it where the node's default is off.",
        "string_ipv4:10.0.0.1 array 5 matches off",
    ),
];

/// How the inline modifiers are told apart from query text.
pub const MODIFIER_RULES: &[&str] = &[
    "A modifier counts only where it opens an unbroken run of clauses reaching the end of the \
     query, with query text left in front of it. Anything else is searched for, so `find tax \
     return forms` is four terms and `* limit 10` is how to ask for a bare limit.",
    "A field list needs a comma between names, a limit or an array needs a number, `matches` \
     needs `on` or `off`, and a sort order must be exactly `asc` or `desc`. A clause that does not parse stays in the query rather than being \
     applied in part.",
    "A modifier naming a field the index does not have is reported as a dropped clause, since a \
     projection would otherwise return documents with no fields.",
];

/// Schema type names an index may use, with what each is for.
pub const FIELD_TYPES: &[(&str, &str)] = &[
    (
        TYPE_TEXT,
        "Full text, split into terms by the field's `tokenizer`, which also decides case, \
         stemming and diacritics — `describe_index` gives each tokenizer's rule in \
         `tokenizer_hints`.",
    ),
    (
        TYPE_STRING,
        "The whole value is one term, matched exactly: case, spaces and punctuation count, and \
         a word or a part of the value matches nothing. Quote a value holding a space or \
         punctuation — `author:\"Austen, Jane\"` — and copy it as the data writes it.",
    ),
    (TYPE_I64, "Signed 64-bit integer."),
    (TYPE_U64, "Unsigned 64-bit integer."),
    (TYPE_F64, "64-bit float."),
    (
        TYPE_DATE,
        "Date or datetime, given as RFC3339, a datetime with no zone, a date, a month, a year, \
         a compact form such as `20240615` or `20240615143000`, or Unix epoch seconds. `/` and \
         `.` may replace `-` throughout, and a value with no zone is read as UTC.",
    ),
    (TYPE_BOOLEAN, "`true` or `false`."),
    (
        TYPE_IP,
        "IPv4 or IPv6 address. A CIDR block is its range: `src:10.0.0.0/8`, \
         `src:\"2001:db8::/32\"` (quote IPv6). An IPv4 bound keeps an open range among IPv4 \
         addresses. A bare address also searches every ip field. No prefixes: `src:10.1.*` is \
         dropped — write `src:10.1.0.0/16`. Sorting needs the field declared fast.",
    ),
    (
        TYPE_JSON,
        "Nested object, searchable only as unstructured text — its keys and values are indexed as \
         a bag of words and paths into it cannot be queried.",
    ),
    (TYPE_FACET, "Hierarchical category path."),
];

/// How each tokenizer a field may name matches, in the words a query is written in.
///
/// A tokenizer name is a decision about matching that the caller has to act on — whether case
/// counts, whether `running` finds `runs`, whether `cafe` finds `café` — and the name alone says
/// none of it. `describe_index` carries the rule for every tokenizer its fields use.
pub const TOKENIZER_RULES: &[(&str, &str)] = &[
    (
        "raw",
        "The whole value is one term, matched exactly: case, spaces and punctuation count, and a \
         word or a part of the value matches nothing. Quote a value holding a space.",
    ),
    (
        "raw_fold",
        "The whole value is one term, matched in any case and with or without diacritics: \
         `rjesenje` finds `RJEŠENJE`, `\"hrvatski sabor\"` finds `Hrvatski sabor`. Spaces and \
         punctuation still count, and a word or a part of the value matches nothing. Quote a \
         value holding a space.",
    ),
    (
        "whitespace",
        "Split at spaces only; case and punctuation are kept, so `Word,` and `word` are \
         different terms.",
    ),
    (
        "default",
        "Words, in any case; punctuation splits them. No stemming, and diacritics are kept: \
         `run` does not find `running`, `cafe` does not find `café`.",
    ),
    (
        "en_stem",
        "Words in any case, English stemmed: `running` finds `runs`.",
    ),
    (
        "de_stem",
        "Words in any case, German stemmed; `Groesse` does not find `Größe`.",
    ),
    (
        "de_stem_fold",
        "Words in any case, German stemmed, umlauts folded: `Groesse` and `Grosse` find `Größe`.",
    ),
    (
        "es_stem",
        "Words in any case, Spanish stemmed; accents kept, so an unaccented word misses an \
         accented one.",
    ),
    (
        "es_stem_fold",
        "Words in any case, Spanish stemmed, accents folded: `informacion` finds `información`.",
    ),
    (
        "fr_stem",
        "Words in any case, French stemmed; accents kept, so an unaccented word misses an \
         accented one.",
    ),
    (
        "fr_stem_fold",
        "Words in any case, French stemmed, accents folded: `situee` finds `située`.",
    ),
    (
        "it_stem",
        "Words in any case, Italian stemmed; accents kept, so an unaccented word misses an \
         accented one.",
    ),
    (
        "it_stem_fold",
        "Words in any case, Italian stemmed, accents folded: `citta` finds `città`.",
    ),
    (
        "pt_stem",
        "Words in any case, Portuguese stemmed, plurals to the singular; accents kept, so an \
         unaccented word misses an accented one.",
    ),
    (
        "pt_stem_fold",
        "Words in any case, Portuguese stemmed, plurals to the singular, accents folded: \
         `decisoes` finds `decisão`.",
    ),
    (
        "hr_stem",
        "Words in any case, Croatian stemmed, Serbian Cyrillic read as Latin; diacritics kept, \
         so `zupanija` misses `županija`.",
    ),
    (
        "hr_stem_fold",
        "Words in any case, Croatian stemmed, Serbian Cyrillic read as Latin, diacritics folded: \
         `zupanija` finds `županije`.",
    ),
    (
        "cjk",
        "Chinese, Japanese or Korean: each character alone and paired with the next. Write a word \
         as its characters with no spaces; no script folding.",
    ),
    (
        "cjk_fold",
        "Chinese: each character alone and paired with the next, traditional folded to \
         simplified, so `學習` finds `学习`. Write a word with no spaces.",
    ),
    (
        "cjk_ja_fold",
        "Japanese: each character alone and paired with the next, old kanji folded to new, so \
         `清吉` finds `淸吉`. Write a word with no spaces.",
    ),
    (
        "cjk_ko_fold",
        "Korean: each character alone and paired with the next, Hanja read as Hangul, so `가족` \
         finds `家族`. Write a word with no spaces.",
    ),
];

/// How a field of this type and tokenizer matches a query value, or `None` for a type that has
/// no tokenizer. A `string` field is always the raw rule, whatever it declares; a text field
/// declaring no tokenizer is analysed by `default`.
pub fn tokenizer_rule(field_type: &str, tokenizer: Option<&str>) -> Option<&'static str> {
    let tokenizer = match normalize_type(field_type) {
        TYPE_STRING => "raw",
        TYPE_TEXT => tokenizer.unwrap_or("default"),
        _ => return None,
    };
    TOKENIZER_RULES
        .iter()
        .find(|(name, _)| *name == tokenizer)
        .map(|(_, rule)| *rule)
}

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

    // Only the caveats true of this type, each under the form it belongs to: unattributed, a
    // sentence about prefixes read as a sentence about the field.
    for op in &forms {
        let caveats: Vec<&str> = op
            .caveats
            .iter()
            .filter(|caveat| caveat.applies_to(normalized))
            .map(|caveat| caveat.text)
            .collect();
        if !caveats.is_empty() {
            hint.push_str(&format!(" `{}`: {}", op.syntax, caveats.join(" ")));
        }
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
                "caveat": op.caveat_text(),
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
        "metrics": METRIC_RULES,
        "rules": RULES,
        "tokenizers": TOKENIZER_RULES
            .iter()
            .map(|(name, rule)| json!({ "tokenizer": name, "matching": rule }))
            .collect::<Vec<_>>(),
        "reserved_characters": RESERVED_CHARACTERS,
    })
}

/// A short reference for a tool description.
///
/// Summaries only: caveats, examples, per-type tables and the full rule text are omitted, since
/// this text is resident in the caller's context for a whole session while `validate_query` and
/// `describe_index` supply the detail on demand. Only the traps that change a result silently — a
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
         Terms with no operator between them are ORed, so each one added widens the result. \
         `AND` or `+` is what requires them all.\n  \
         A clause the engine cannot interpret is dropped, not rejected — the call fails and names \
         it.\n  \
         `AND` `OR` `NOT` `TO` `IN` count only in uppercase; lowercase is searched for as a word, \
         and for `and`, `or` and `not` that happens silently.\n  \
         A string field matches its whole value, case and all: copy values from a description or \
         a hit, never from the question's wording.\n  \
         A field that is not `searchable` cannot be queried; `describe_index` says which. The \
         exception is a `shadow` field, which is the identifier under another name: `field:VALUE` \
         on its own is the key-value fast path, inside a larger query it is rewritten to `id`, \
         and results carry it in place of `id`.\n  \
         Sorting a text or string field is approximate, and sets every `_score` to 1.0.\n  \
         `metrics` gives the min and max of numeric and date fields over every match, not the \
         page; any other field is refused, and a field no match holds is null, not 0.\n  \
         A modifier is searched for as text unless its whole run parses, so give a field list its \
         commas and a sort an `asc` or `desc`.\n",
    );
    out.push_str(&format!(
        "\nEscape these to match them literally: {RESERVED_CHARACTERS}\n"
    ));
    out.push_str(
        "\nCall `validate_query` with no arguments for the full reference including examples, \
         per-type operators and sorting rules; `describe_index` for one index's fields and types.\n",
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
    out.push_str("\n**Tokenizers**, how a text field's values are matched\n\n");
    out.push_str("| Tokenizer | Matching |\n|---|---|\n");
    for (name, rule) in TOKENIZER_RULES {
        out.push_str(&format!("| `{name}` | {rule} |\n"));
    }
    out.push_str("\n**Sorting**\n\n");
    for rule in SORT_RULES {
        out.push_str(&format!("- {rule}\n"));
    }
    out.push_str("\n**Min and max**\n\n");
    for rule in METRIC_RULES {
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

    /// Nothing here describes a field no index reports.
    ///
    /// `_seq` was documented as "present in every index and technically queryable" until
    /// 2026-08-27, and by then it was neither: Stage 7 stopped declaring it on new indices,
    /// `describe_fields`, `sorted_field_names`, `searchable_fields` and `sortable_fields` all
    /// filter it, and a sort naming it is refused. The rule spent resident context telling an
    /// agent to ignore a field it could not see.
    ///
    /// The reason it survived is that retiring a column swept the engine and not the prose, and
    /// the reason it would come back is an index old enough to still carry the column. This is
    /// the assertion that says the engine's silence about a field is the reference's silence too.
    #[test]
    fn no_rule_describes_a_field_the_engine_hides() {
        for text in RULES {
            assert!(
                !text.contains("_seq"),
                "a rule describes `_seq`, which no index reports and no listing shows: {text}"
            );
        }
        for op in OPERATORS {
            assert!(
                !op.syntax.contains("_seq") && !op.summary.contains("_seq"),
                "an operator names `_seq`: {}",
                op.syntax
            );
        }
        for rule in SORT_RULES {
            assert!(
                !rule.contains("_seq"),
                "a sort rule names `_seq`, which is refused rather than sorted: {rule}"
            );
        }
    }

    /// A form that names no field type would be missing from every per-field hint, and a type no
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

    /// A type's hint says only what is true of that type.
    ///
    /// Every caveat of every operator a type supports used to be appended whole, so an integer's
    /// hint explained how text compares and a string's how a stemmed prefix moves — and a reader
    /// cannot tell which sentence was about its field.
    #[test]
    fn a_hint_carries_only_the_caveats_of_its_type() {
        let string = hint_for_type(TYPE_STRING);
        assert!(string.contains("case"), "{string}");
        assert!(!string.contains("stemmed"), "{string}");
        assert!(!string.contains("sortable"), "{string}");

        let integer = hint_for_type(TYPE_I64);
        assert!(!integer.contains("lexicographically"), "{integer}");
        assert!(!integer.contains("Film & Animation"), "{integer}");

        let text = hint_for_type(TYPE_TEXT);
        assert!(text.contains("stemmed"), "{text}");
        assert!(!text.contains("`Data*` and `data*`"), "{text}");

        // Said once, under the form it belongs to, never twice.
        for hint in [&string, &integer, &text] {
            assert!(hint.matches("lexicographically").count() <= 1, "{hint}");
            assert!(
                hint.contains("`field:value^N`: Affects ranking only"),
                "{hint}"
            );
        }
    }

    #[test]
    fn a_field_s_rule_follows_its_type_and_tokenizer() {
        // A string field builds the raw column whatever it declares.
        assert_eq!(
            tokenizer_rule(TYPE_STRING, Some("en_stem")),
            tokenizer_rule(TYPE_TEXT, Some("raw"))
        );
        assert_eq!(
            tokenizer_rule(TYPE_TEXT, None),
            tokenizer_rule(TYPE_TEXT, Some("default"))
        );
        assert!(tokenizer_rule(TYPE_I64, None).is_none());
        assert!(tokenizer_rule(TYPE_TEXT, Some("klingon")).is_none());
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

    /// Every surface that tells an agent what it may query has to carry the shadow rule, because
    /// the flag it explains contradicts the `indexed` flag reported beside it: without the rule,
    /// `indexed: false` reads as a field nothing can match, and the fastest lookup in the index
    /// goes unused.
    #[test]
    fn every_surface_that_talks_about_queryability_carries_the_shadow_rule() {
        assert!(
            RULES.contains(&SHADOW_FIELD),
            "the shadow rule must be one of the rules, which is what renders it everywhere"
        );
        assert!(
            reference_json().to_string().contains("shadowfield:VALUE"),
            "the full reference omits the shadow rule"
        );
        assert!(
            markdown_reference().contains("shadowfield:VALUE"),
            "the markdown reference omits the shadow rule"
        );
        // The compact reference writes its own traps list rather than rendering `RULES`, so it
        // is the one that can lose the exception while the others keep it.
        assert!(
            compact_reference().contains("shadow"),
            "the tool descriptions warn that an unindexed field cannot be queried without \
             naming the exception"
        );
    }

    /// Forms verified as broken must stay listed as unsupported: an agent that tries one gets a
    /// dropped clause, so the reference has to say so rather than stay silent.
    #[test]
    fn the_known_broken_forms_are_listed_as_unsupported() {
        for syntax in ["field:*", "pre*", "field:*suffix", "field.subfield:value"] {
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
