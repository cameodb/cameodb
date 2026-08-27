//! Telling a caller why a result is not what it expected.
//!
//! Pure functions over a query string, a JSON response and a field list. Nothing here reaches the
//! engine, which is what keeps every test in this module a unit test.

use serde_json::Value as JsonValue;

use crate::mcp::schema::{FieldInfo, field_query_hint};
use crate::query::parse_query_keywords;

/// Whether a query contains a phrase or a conjunction, and so could be usefully relaxed after
/// matching nothing.
///
/// `AND` counts only as a standalone token, the way the parser reads it, so it is not found
/// inside a value such as `status:ANDROID`.
/// Why a search that matched nothing may have asked for less than it meant to, or `None` when
/// the query gives no reason to think so.
///
/// Names only what the query contains, and only the constructs that narrow. Terms are not among
/// them: they are ORed, so bare terms that returned nothing matched none of them and there is no
/// narrowing to undo. Advice about a phrase the caller did not write, or a boolean it did not
/// use, reads as a finding about the data — worse than silence, since zero hits is usually the
/// true answer.
///
/// Inline modifiers are stripped first, so `limit` and `sort` are not read as query terms.
///
/// The behaviour these sentences rest on is stated in [`cameodb_mcp::syntax`]; this is the
/// diagnosis of one query rather than a second copy of the reference, hence the wording as
/// advice rather than as a rule.
pub(super) fn zero_results_advice(query: &str) -> Option<String> {
    let text = parse_query_keywords(query).query;
    let tokens: Vec<&str> = text.split_whitespace().collect();

    let mut reasons: Vec<&str> = Vec::new();

    if text.contains('"') {
        reasons.push(
            "A quoted phrase matches only that exact run of terms, in that order. Try the terms \
             unquoted, or `field:\"a b\"~2` to allow words between them.",
        );
    }

    if tokens.contains(&"AND") {
        reasons.push(
            "Every `AND` clause has to match the same document. Try `OR` between them, or drop \
             the narrowest clause.",
        );
    }

    // `+` makes a clause required where the default is not, so two of them are a conjunction
    // written another way.
    if tokens.iter().any(|token| token.starts_with('+')) {
        reasons.push(
            "A `+clause` is required rather than optional, so every one of them has to match. \
             Drop the `+` from the clauses that are not essential.",
        );
    }

    if tokens
        .iter()
        .any(|token| token.starts_with('-') || *token == "NOT")
    {
        reasons.push(
            "An excluded clause removes every document matching it, however well the rest of the \
             query fits. Check that the exclusion is not taking the answer with it.",
        );
    }

    (!reasons.is_empty()).then(|| reasons.join(" "))
}

/// Why an empty page came back from a query that matched, or `None` when it did not page past
/// the end.
///
/// This is the case [`zero_results_advice`] must not be asked about. A page beyond the last one
/// returns no hits and a `total_hits` in the hundreds, and the query is blameless — advice about
/// a phrase or an `AND` clause there reads as a finding about the data and sends the caller off
/// to rewrite a query that was already correct.
///
/// Names the last offset that holds a hit, because that is what the caller needs to get back to
/// a page with something on it.
pub(super) fn paged_past_the_end(offset: usize, total_hits: usize) -> Option<String> {
    if offset == 0 || total_hits == 0 || offset < total_hits {
        return None;
    }
    Some(format!(
        "This page is empty because it starts past the end of the result: offset {offset} with \
         {total_hits} matching document(s). The last document is at offset {}.",
        total_hits - 1
    ))
}

/// A page holding fewer hits than the count says it should, and why.
///
/// The count and the bodies come from different engines: Tantivy counts what matched, and the
/// documents are fetched from the key-value store by key. A delete removes the row at once and
/// the indexed term only at the next commit, so between the two a match is counted and has no
/// body to return — the count runs ahead of the documents by however many were deleted since.
///
/// Reachable in ordinary operation only since record deletion shipped, which is why nothing
/// explained it before. It matters because the session instructions tell an agent never to
/// present an incomplete result as a whole one, and this is the one shortfall it cannot see: the
/// hits are real, the count is real, and nothing in the response relates them.
///
/// `expected` is what the window should have yielded — `limit`, or what is left after `offset`,
/// whichever is smaller. Silent when the page is full, and silent for a count-only query, which
/// asks for no hits at all.
pub(super) fn short_page_note(
    hits_returned: usize,
    total_hits: usize,
    offset: usize,
    limit: usize,
) -> Option<String> {
    if limit == 0 {
        return None;
    }
    let expected = limit.min(total_hits.saturating_sub(offset));
    if hits_returned >= expected {
        return None;
    }
    let missing = expected - hits_returned;
    Some(format!(
        "This page carries {hits_returned} of the {expected} hit(s) the count implies, so \
         {missing} matching document(s) could not be read back. The count comes from the search \
         index and the documents from the key-value store, and a deletion clears the store first \
         — so a document deleted since the index was last committed is still counted and no \
         longer there. Treat {total_hits} as the count at the last commit, not as the number of \
         documents you can retrieve."
    ))
}

/// What an approximate sort order means for the caller holding it.
/// What an approximate sort order means for the caller holding it.
///
/// Attached whenever the engine reports [`crate::node_orchestrator::APPROXIMATE_SORT_FIELD`],
/// rather than left in the node's log where the caller cannot see it. An agent reading a sorted
/// page has no other way to tell that it is holding the alphabetical order of a sample: the hits
/// look exactly like an exact answer, and every hit in them is real.
pub(super) fn approximate_sort_note(field: &str) -> String {
    format!(
        "These hits are sorted on '{field}', which has no fast column, so the order is the \
         alphabetical order of the highest-scoring candidates rather than of everything that \
         matched — the alphabetically first document may be absent entirely, and paging deeper \
         re-orders a different sample rather than continuing this one. `describe_index` reports \
         `sortable: false` for such a field. An exact order needs the field declared `fast` \
         before the index is built."
    )
}

/// Whether an engine error reports a field the schema does not have.
///
/// Matched against specific signals rather than the word "field", which appears in unrelated
/// errors such as the sort error for a non-FAST field.
pub(super) fn names_a_missing_field(error: &str) -> bool {
    const MISSING_FIELD_SIGNALS: [&str; 5] = [
        "does not exist",
        "FieldDoesNotExist",
        "unknown field",
        "not declared as indexed",
        // A refused sort, which the router decides before any shard is asked.
        "no column of that name",
    ];
    MISSING_FIELD_SIGNALS
        .iter()
        .any(|signal| error.contains(signal))
}

/// Append an index's field list to an engine error, keeping the original message.
pub(super) fn with_valid_fields(error: &str, index: &str, field_names: &[String]) -> String {
    format!(
        "{error}\n\nFields available in '{index}': [{}]",
        field_names.join(", ")
    )
}

/// Turn a search response carrying dropped clauses into a tool execution error.
///
/// The hits are real but do not answer the query as written — wider, narrower, or empty,
/// depending on where the dropped clause sat — and nothing in the payload marks them as such. MCP callers present results as fact, so they get an error naming the
/// clause; the HTTP API keeps the hits and reports the same list as
/// [`DISCARDED_CLAUSES_FIELD`].
pub(super) fn refuse_if_clauses_discarded(response: &JsonValue) -> Result<(), String> {
    let discarded: Vec<&str> = response
        .get(crate::node_orchestrator::DISCARDED_CLAUSES_FIELD)
        .and_then(|value| value.as_array())
        .map(|notes| notes.iter().filter_map(|note| note.as_str()).collect())
        .unwrap_or_default();

    if discarded.is_empty() {
        return Ok(());
    }

    let detail = discarded
        .iter()
        .map(|note| format!("  - {note}"))
        .collect::<Vec<_>>()
        .join("\n");

    Err(format!(
        "Query rejected: part of this query could not be interpreted and was dropped, so the \
         results would not be the ones asked for.\n{detail}\n\nRewrite the query and retry. \
         `validate_query` lists the fields this index actually has, with the operators each \
         field's type supports."
    ))
}

pub(super) fn analyze_query(query_text: &str, field_infos: &[FieldInfo]) -> JsonValue {
    let mut warnings: Vec<String> = Vec::new();
    let mut suggestions: Vec<String> = Vec::new();

    // Structural checks
    let quote_count = query_text.chars().filter(|ch| *ch == '"').count();
    if quote_count % 2 != 0 {
        warnings.push(
            "Unbalanced quotes detected. Phrase queries require matching double quotes."
                .to_string(),
        );
    }

    let open_parens = query_text.chars().filter(|ch| *ch == '(').count();
    let close_parens = query_text.chars().filter(|ch| *ch == ')').count();
    if open_parens != close_parens {
        warnings.push(format!(
            "Unbalanced parentheses: {} opening vs {} closing.",
            open_parens, close_parens
        ));
    }

    // Check for inline modifiers (return/limit)
    let parts: Vec<&str> = query_text.split_whitespace().collect();
    let has_return = parts
        .iter()
        .any(|token| token.eq_ignore_ascii_case("return"));
    let has_limit = parts
        .iter()
        .any(|token| token.eq_ignore_ascii_case("limit"));

    if has_return {
        suggestions.push("Query uses inline 'return' for field projection. You can also pass fields via the tool's 'fields' parameter.".to_string());
    }
    if has_limit {
        suggestions.push(
            "Query uses inline 'limit'. You can also pass limit via the tool's 'limit' parameter."
                .to_string(),
        );
    }

    // Extract field references (handle phrases and parens gracefully)
    let referenced_fields = extract_query_fields(query_text);

    let queryable_names: Vec<&str> = field_infos
        .iter()
        .filter(|info| info.is_queryable())
        .map(|info| info.name.as_str())
        .collect();

    let all_names: Vec<&str> = field_infos.iter().map(|info| info.name.as_str()).collect();

    let mut recognized = Vec::new();
    let mut unknown = Vec::new();
    let mut not_indexed = Vec::new();
    let mut field_hints = Vec::new();

    for field_name in &referenced_fields {
        if queryable_names.contains(&field_name.as_str()) {
            recognized.push(field_name.clone());
            if let Some(info) = field_infos.iter().find(|i| i.name == *field_name) {
                field_hints.push(serde_json::json!({
                    "field": field_name,
                    "type": info.field_type,
                    "hint": field_query_hint(info),
                }));
            }
        } else if all_names.contains(&field_name.as_str()) {
            not_indexed.push(field_name.clone());
            warnings.push(format!(
                "Field '{}' exists but is not indexed. Queries against it will not match.",
                field_name
            ));
        } else {
            unknown.push(field_name.clone());
        }
    }

    if !unknown.is_empty() && !all_names.is_empty() {
        for unk in &unknown {
            let unk_lower = unk.to_lowercase();
            let close_matches: Vec<&str> = queryable_names
                .iter()
                .filter(|known| {
                    let known_lower = known.to_lowercase();
                    known_lower.starts_with(&unk_lower)
                        || unk_lower.starts_with(&known_lower)
                        || known_lower.contains(&unk_lower)
                        || unk_lower.contains(&known_lower)
                })
                .copied()
                .collect();
            if !close_matches.is_empty() {
                suggestions.push(format!(
                    "Unknown field '{}'. Did you mean: {}?",
                    unk,
                    close_matches.join(", ")
                ));
            } else {
                warnings.push(format!(
                    "Unknown field '{}'. Available queryable fields: {}.",
                    unk,
                    queryable_names.join(", ")
                ));
            }
        }
    }

    serde_json::json!({
        "query": query_text,
        "recognized_fields": recognized,
        "unknown_fields": unknown,
        "not_indexed_fields": not_indexed,
        "field_hints": field_hints,
        "warnings": warnings,
        "suggestions": suggestions,
    })
}

pub(super) fn extract_query_fields(query: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let reserved = ["AND", "OR", "NOT", "TO", "return", "limit"];
    let chars: Vec<char> = query.chars().collect();
    let len = chars.len();
    let mut i = 0;

    while i < len {
        match chars[i] {
            '"' => {
                // Skip quoted strings
                i += 1;
                while i < len && chars[i] != '"' {
                    i += 1;
                }
                i += 1;
            }
            '(' | ')' | '[' | ']' => {
                i += 1;
            }
            _ if chars[i].is_alphanumeric() || chars[i] == '_' => {
                let start = i;
                while i < len && (chars[i].is_alphanumeric() || chars[i] == '_' || chars[i] == '.')
                {
                    i += 1;
                }
                let token = &query[start..i];

                // Check if followed by ':' (field reference)
                if i < len && chars[i] == ':' {
                    if !reserved.iter().any(|kw| kw.eq_ignore_ascii_case(token))
                        && !fields.contains(&token.to_string())
                    {
                        fields.push(token.to_string());
                    }
                    i += 1; // skip the colon
                }
            }
            _ => {
                i += 1;
            }
        }
    }

    fields
}

/// The full query syntax reference, as `validate_query` returns it.
///
/// Rendered from [`cameodb_mcp::syntax`] so the reference, the per-field hints, the MCP tool
/// descriptions and the agent skill cannot disagree.
pub(super) fn cameodb_syntax_reference() -> JsonValue {
    cameodb_mcp::syntax::reference_json()
}

/// What the search error path is allowed to say about an engine error.
///
/// The interceptor exists so an agent that named a field wrongly gets the list of real ones.
/// Both ways it can go wrong are silent: reading an unrelated error as a missing field sends
/// the agent to fix a name that was never the problem, and replacing the engine's message with
/// a guess discards the only account of what actually happened. Neither shows up as a failure
/// anywhere else, so they are pinned here.
#[cfg(test)]
mod zero_results_advice_tests {
    use super::{short_page_note, zero_results_advice};

    /// Zero hits is usually the true answer, and a warning attached to it claims the query asked
    /// for less than it meant to. Terms alone never do: they are ORed, so a query of bare terms
    /// that found nothing matched none of them and there is no narrowing to undo. Advice there
    /// would send a caller to loosen a query that is already as loose as it gets.
    #[test]
    fn a_query_with_nothing_narrowing_it_gets_no_warning() {
        for query in [
            "rust",
            "title:rust",
            "quarterly revenue report",
            "title:rust title:go",
            "title:rust limit 5",
            "status: IN [active pending]",
            "year:[2020 TO 2024]",
            "(title:rust OR title:go)",
        ] {
            assert_eq!(
                zero_results_advice(query),
                None,
                "{query:?} was diagnosed as narrowed when nothing in it narrows"
            );
        }
    }

    /// The advice has to describe the query in front of it. Advice about quotes on a query with
    /// no quotes reads as a finding about the data.
    #[test]
    fn the_advice_names_only_what_the_query_contains() {
        let phrase = zero_results_advice(r#"title:"exact phrase""#).expect("a phrase narrows");
        assert!(phrase.contains("quoted phrase"), "{phrase}");
        assert!(
            !phrase.contains("`AND`"),
            "the query has no AND to broaden: {phrase}"
        );

        let conjunction = zero_results_advice("title:rust AND year:2024").expect("AND narrows");
        assert!(conjunction.contains("`AND`"), "{conjunction}");
        assert!(
            !conjunction.contains("quoted"),
            "the query has no quotes to remove: {conjunction}"
        );

        let both =
            zero_results_advice(r#"title:"exact phrase" AND year:2024"#).expect("both narrow");
        assert!(
            both.contains("quoted phrase") && both.contains("`AND`"),
            "both apply and both should be said: {both}"
        );
    }

    /// A page shorter than the count implies is explained; a page that is merely paged is not.
    ///
    /// The distinction is the whole value of the note. `total_hits` above `limit` is the ordinary
    /// case and says nothing is wrong; `hits_returned` below what the window should have yielded
    /// is the case a deletion produces between the redb removal and the Tantivy commit, and it is
    /// invisible in the hits themselves — every one of them is real.
    #[test]
    fn only_a_page_shorter_than_its_window_is_explained() {
        // Full pages, paged or not.
        assert!(short_page_note(10, 10, 0, 10).is_none(), "exactly full");
        assert!(
            short_page_note(10, 500, 0, 10).is_none(),
            "a first page of many is not short"
        );
        assert!(
            short_page_note(10, 500, 100, 10).is_none(),
            "nor is a later one"
        );
        assert!(
            short_page_note(5, 105, 100, 10).is_none(),
            "a last page holds what is left of the count, which is fewer than the limit"
        );
        assert!(
            short_page_note(0, 0, 0, 10).is_none(),
            "nothing matched, which zero_results_advice speaks to instead"
        );
        assert!(
            short_page_note(0, 50, 0, 0).is_none(),
            "a count-only query asks for no hits, so it is never short"
        );

        // The shortfall a deletion leaves behind: five counted, four readable.
        let note = short_page_note(4, 5, 0, 10).expect("four of five is short");
        assert!(
            note.contains("4 of the 5") && note.contains("1 matching document"),
            "the note should say how many are missing: {note}"
        );
        assert!(
            note.contains("deleted") && note.contains("last commit"),
            "and why, and what the count now means: {note}"
        );

        // Short within a later page, where the offset decides what was expected.
        assert!(
            short_page_note(7, 200, 100, 10).is_some(),
            "a mid-result page missing three documents is short too"
        );
    }

    /// The two ways to narrow that are not the word `AND`.    /// The two ways to narrow that are not the word `AND`. With terms ORed, `+` is what a caller
    /// reaches for to require a clause, and it is easy to leave on one that need not be.
    #[test]
    fn required_and_excluded_clauses_are_recognised_as_narrowing() {
        let required = zero_results_advice("+title:rust +year:2024").expect("`+` requires");
        assert!(required.contains("required"), "{required}");

        for query in ["title:rust -tag:draft", "title:rust NOT tag:draft"] {
            let excluded = zero_results_advice(query).expect("an exclusion narrows");
            assert!(
                excluded.contains("excluded"),
                "{query:?} excludes documents and the advice missed it: {excluded}"
            );
        }
    }

    /// Modifiers are query syntax, not query text, so they must not be read as clauses.
    #[test]
    fn inline_modifiers_are_stripped_before_the_query_is_read() {
        assert_eq!(zero_results_advice("rust limit 5"), None);
        assert!(
            zero_results_advice(r#"title:"a b" limit 5"#).is_some(),
            "stripping the modifier must not take the phrase with it"
        );
    }
}

#[cfg(test)]
mod search_error_interception_tests {
    use super::{names_a_missing_field, with_valid_fields};

    #[test]
    fn a_sort_error_is_not_read_as_a_missing_field() {
        // A sort error names a field without being about a missing one. Matching on the bare
        // word "field" would report it as nonexistent — a confident wrong diagnosis of a real
        // problem the caller could otherwise fix.
        assert!(!names_a_missing_field(
            "Field 'year' is not marked as FAST. Only FAST fields support sorting."
        ));
    }

    #[test]
    fn a_missing_field_is_still_recognised_however_the_engine_words_it() {
        for error in [
            "Field 'nosuch' does not exist in schema",
            "FieldDoesNotExist(\"nosuch\")",
            "Query error: unknown field 'nosuch'",
            "Field 'nosuch' is not declared as indexed",
        ] {
            assert!(
                names_a_missing_field(error),
                "the interceptor stopped recognising: {error}"
            );
        }
    }

    #[test]
    fn the_field_list_is_appended_rather_than_replacing_the_error() {
        let original = "Field 'titel' does not exist in schema";
        let enriched =
            with_valid_fields(original, "docs", &["title".to_string(), "body".to_string()]);

        assert!(
            enriched.starts_with(original),
            "the engine's own message must survive: {enriched}"
        );
        assert!(
            enriched.contains("title, body") && enriched.contains("docs"),
            "the caller needs the real fields and the index they belong to: {enriched}"
        );
    }
}
