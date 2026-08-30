//! What counts as a field reference in a query string.
//!
//! One scanner answers this for every reader that needs it — the shadow rewriter splices over
//! the span, the schema check classifies the name, and the MCP layer lists the names for an
//! agent. The rules are pinned here rather than inside any one caller, so the three cannot
//! drift into disagreeing about what a query says.

use storage::field_references;

/// The names a query references, in order.
fn names(query: &str) -> Vec<String> {
    field_references(query)
        .into_iter()
        .map(|reference| reference.name.into_owned())
        .collect()
}

/// The text each span covers, which is what a rewriter would replace.
fn spanned(query: &str) -> Vec<&str> {
    field_references(query)
        .into_iter()
        .map(|reference| &query[reference.span])
        .collect()
}

#[test]
fn a_name_before_a_colon_is_a_reference() {
    assert_eq!(names("title:rust"), ["title"]);
    assert_eq!(names("title:rust AND created:2024"), ["title", "created"]);
    assert_eq!(
        names("title:rust body:fast extra:x"),
        ["title", "body", "extra"]
    );
}

/// A bare term references nothing, and neither does a modifier run.
#[test]
fn a_token_without_a_colon_is_not_a_reference() {
    assert!(names("rust programming").is_empty());
    assert!(names("AND OR NOT TO IN").is_empty());
    assert_eq!(names("title:rust limit 5 return id,title"), ["title"]);
}

/// The occurrence operators sit in front of the name, so they are not part of it. Only the
/// leading ones: a hyphen is an ordinary character inside a name.
#[test]
fn leading_operators_are_not_part_of_the_name() {
    assert_eq!(
        names("+title:a -body:b !other:c"),
        ["title", "body", "other"]
    );
    assert_eq!(names("content-type:json"), ["content-type"]);
    assert_eq!(names("-content-type:json"), ["content-type"]);
    assert_eq!(spanned("-content-type:json"), ["content-type"]);
}

/// The name ends at the first colon of its segment; every colon after it belongs to the value.
/// This is what keeps a timestamp or a URL from reading as a field name.
#[test]
fn only_the_first_colon_of_a_segment_splits() {
    assert_eq!(names("created:2024-06-15T00:00:00Z"), ["created"]);
    assert_eq!(names("url:https://example.com/x"), ["url"]);
    assert_eq!(
        names("title:rust AND created:2024-06-15T12:30:00Z"),
        ["title", "created"]
    );
    assert_eq!(names("id:urn:x:1"), ["id"]);
}

/// A phrase holds text, so nothing inside one is a field reference — including a token that is
/// wholly a quoted phrase and happens to contain a colon.
#[test]
fn nothing_inside_a_phrase_is_a_reference() {
    assert_eq!(names("title:\"a:b\""), ["title"]);
    assert_eq!(names("\"sha1:d1\" AND title:rust"), ["title"]);
    assert_eq!(names("body:\"one two\" AND title:rust"), ["body", "title"]);
    assert!(names("\"only a phrase\"").is_empty());
}

/// A range and a set hold values. Their contents are skipped, and the name in front of them is
/// still read — the reference and the bracket arrive in the same token.
#[test]
fn nothing_inside_a_range_or_a_set_is_a_reference() {
    assert_eq!(
        names("created:[2024-01-01T00:00:00Z TO 2024-12-31T00:00:00Z]"),
        ["created"]
    );
    assert_eq!(names("created:{2024-01-01 TO 2024-12-31}"), ["created"]);
    assert_eq!(names("sha1: IN [d1 d2]"), ["sha1"]);
    assert_eq!(
        names("price:[10.0 TO 100.0] AND title:rust"),
        ["price", "title"]
    );
}

/// Parentheses group clauses rather than holding values, so a clause may begin immediately
/// after one. Reading a whitespace token whole would make `AND(sha1:x)` reference a field
/// called `AND(sha1` — a name no index has, reported in place of the real syntax problem.
#[test]
fn a_parenthesis_begins_a_new_reference_without_a_space() {
    assert_eq!(names("(title:rust OR body:fast)"), ["title", "body"]);
    assert_eq!(names("other:x AND(title:rust)"), ["other", "title"]);
    assert_eq!(names("(title:rust)AND(body:fast)"), ["title", "body"]);
    assert_eq!(names("((title:rust))"), ["title"]);
    assert_eq!(spanned("other:x AND(title:rust)"), ["other", "title"]);
}

/// An escape is the query's, not the field's, so the name resolves it — while the span still
/// covers the name as written, which is what a rewriter has to replace.
#[test]
fn an_escaped_name_resolves_but_its_span_covers_what_was_written() {
    assert_eq!(names("k8s\\.node:worker-1"), ["k8s.node"]);
    assert_eq!(spanned("k8s\\.node:worker-1"), ["k8s\\.node"]);

    // An escaped colon is part of the name, not the split point.
    assert_eq!(names("odd\\:name:value"), ["odd:name"]);
}

/// Spans address the query, not the token, so a rewriter can splice them in order.
#[test]
fn spans_locate_the_name_within_the_whole_query() {
    let query = "title:rust AND created:2024";
    let references = field_references(query);
    assert_eq!(references.len(), 2);
    assert_eq!(references[0].span, 0..5);
    assert_eq!(references[1].span, 15..22);
    assert_eq!(&query[references[1].span.clone()], "created");
}

/// Degenerate shapes yield nothing rather than an empty name or a panic.
#[test]
fn a_colon_with_no_name_in_front_of_it_is_not_a_reference() {
    assert!(names(":value").is_empty());
    assert!(names("-:value").is_empty());
    assert!(names("").is_empty());
    assert!(names("   ").is_empty());
    assert_eq!(names("title:rust :orphan"), ["title"]);
}

/// Multi-byte characters do not shift a span, since both are counted in bytes.
#[test]
fn a_multibyte_query_still_spans_correctly() {
    let query = "titre:café AND auteur:x";
    let references = field_references(query);
    assert_eq!(names(query), ["titre", "auteur"]);
    assert_eq!(&query[references[1].span.clone()], "auteur");
}
