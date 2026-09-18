//! Query machinery: normalization, field scanning, parser preparation and
//! discarded-clause reporting. Pure functions over `IndexSchema` — nothing here
//! touches the store.
use crate::*;
use std::borrow::Cow;
use std::collections::HashSet;

use chrono::{SecondsFormat, TimeZone, Utc};
use tantivy::Index;
use tantivy::schema::Field;

/// Replace every whitespace character the query grammar cannot skip with a plain space.
///
/// Tantivy's lenient set parser (`IN [ ... ]`) advances over inter-element space with nom's
/// `multispace`, which is ASCII only — space, tab, CR, LF. A term character is anything that is
/// not `char::is_whitespace()`. Any character that is whitespace to Rust but not one of those
/// four ASCII spaces therefore satisfies neither: the parser cannot consume it as space and
/// will not take it as a term, so an unterminated set such as `IN[\u{a0}` loops forever,
/// appending an error each turn until the process is out of memory. A single query body reaches
/// this through `parse_query_lenient`, so it is an unauthenticated way to wedge a search thread.
///
/// Folding those characters to an ASCII space closes the gap: the same byte the parser already
/// knows how to step over, in a position where whitespace is what the character meant. Inside a
/// quoted phrase it merges two tokens the default tokenizer already splits on, so a search
/// returns what it did before. Borrow the query unchanged when it holds none of them.
pub(crate) fn fold_untokenizable_whitespace(query: &str) -> Cow<'_, str> {
    let is_trap = |c: char| c.is_whitespace() && !matches!(c, ' ' | '\t' | '\n' | '\r');
    if !query.contains(is_trap) {
        return Cow::Borrowed(query);
    }
    Cow::Owned(
        query
            .chars()
            .map(|c| if is_trap(c) { ' ' } else { c })
            .collect(),
    )
}

/// Convert a date literal into RFC3339 Z string using the same rules as indexing.
/// Returns None if the literal cannot be parsed as a date.
pub(crate) fn normalize_date_literal(lit: &str) -> Option<String> {
    if lit == "*" {
        return None;
    }

    // Strip surrounding quotes (e.g. ""2026.07.01"" -> "2026.07.01")
    let stripped = lit.trim_matches('"');
    let (_, _, clamped) = parse_date_str_to_tantivy(stripped)?;
    let dt = Utc.timestamp_opt(clamped, 0).single()?;
    Some(dt.to_rfc3339_opts(SecondsFormat::Secs, true))
}

/// Rewrite both bounds of a date range to RFC3339.
///
/// Accepts either delimiter on either side — `[a TO b]`, `{a TO b}`, and the mixed pairs — and
/// preserves them, since they carry the inclusive/exclusive meaning.
pub(crate) fn normalize_date_ranges(input: &str, field: &str) -> String {
    let prefix = format!("{}:", field);
    let mut out = String::with_capacity(input.len());
    let mut idx = 0usize;

    while let Some(rel) = input[idx..].find(&prefix) {
        let start = idx + rel;
        out.push_str(&input[idx..start]);
        let after_colon = start + prefix.len();

        // The opening delimiter decides whether this is a range at all.
        let open = input[after_colon..].chars().next();
        let Some(open) = open.filter(|ch| *ch == '[' || *ch == '{') else {
            out.push_str(&input[start..after_colon]);
            idx = after_colon;
            continue;
        };

        let inner_start = after_colon + open.len_utf8();
        // Either closing form may terminate the range, so take whichever comes first.
        let close_rel = input[inner_start..]
            .find([']', '}'])
            .map(|rel| (rel, input[inner_start + rel..].chars().next().unwrap()));

        if let Some((end_rel, close)) = close_rel {
            let end = inner_start + end_rel;
            let inner = &input[inner_start..end];

            if let Some((lower, upper)) = inner.split_once(" TO ") {
                let lower_norm = normalize_date_literal(lower).unwrap_or_else(|| lower.to_string());
                let upper_norm = normalize_date_literal(upper).unwrap_or_else(|| upper.to_string());
                out.push_str(&format!(
                    "{}:{}{} TO {}{}",
                    field, open, lower_norm, upper_norm, close
                ));
                idx = end + close.len_utf8();
                continue;
            }
        }

        // No closing delimiter, or no ` TO ` inside it: not a range we can rewrite. Copy the
        // field prefix and the opening delimiter and carry on from there.
        out.push_str(&input[start..inner_start]);
        idx = inner_start;
    }

    out.push_str(&input[idx..]);
    out
}

/// Rewrite every element of a date set query — `field: IN [a b c]` — to RFC3339.
///
/// Whitespace is allowed around `IN` and after the colon, so this shape is not reachable by the
/// single-literal pass, which reads a value up to the next whitespace.
pub(crate) fn normalize_date_in_sets(input: &str, field: &str) -> String {
    let prefix = format!("{}:", field);
    let mut out = String::with_capacity(input.len());
    let mut idx = 0usize;

    /// Bytes of leading whitespace at `from`, so the cursor can step over it.
    pub(crate) fn space_at(input: &str, from: usize) -> usize {
        input[from..].len() - input[from..].trim_start().len()
    }

    while let Some(rel) = input[idx..].find(&prefix) {
        let start = idx + rel;
        out.push_str(&input[idx..start]);
        let after_colon = start + prefix.len();

        // Walk forward from the colon with one cursor: optional space, `IN`, optional space,
        // `[`, elements, `]`. Anything else is not a set query and is copied through.
        let mut cursor = after_colon + space_at(input, after_colon);
        if !input[cursor..].starts_with("IN") {
            out.push_str(&input[start..after_colon]);
            idx = after_colon;
            continue;
        }
        cursor += "IN".len();
        cursor += space_at(input, cursor);
        if !input[cursor..].starts_with('[') {
            out.push_str(&input[start..after_colon]);
            idx = after_colon;
            continue;
        }
        cursor += '['.len_utf8();

        let Some(close_rel) = input[cursor..].find(']') else {
            out.push_str(&input[start..after_colon]);
            idx = after_colon;
            continue;
        };

        // Quoted for the same reason as a bare literal: RFC3339 carries colons, which the
        // grammar would otherwise read as a field separator inside the set.
        let normalized: Vec<String> = input[cursor..cursor + close_rel]
            .split_whitespace()
            .map(|element| match normalize_date_literal(element) {
                Some(norm) => format!("\"{norm}\""),
                None => element.to_string(),
            })
            .collect();
        out.push_str(&format!("{}: IN [{}]", field, normalized.join(" ")));
        idx = cursor + close_rel + ']'.len_utf8();
    }

    out.push_str(&input[idx..]);
    out
}

pub(crate) fn normalize_date_comparisons(input: &str, field: &str) -> String {
    let prefix = format!("{}:", field);
    let mut out = String::with_capacity(input.len());
    let mut idx = 0usize;

    while let Some(rel) = input[idx..].find(&prefix) {
        let start = idx + rel;
        out.push_str(&input[idx..start]);

        let op_idx = start + prefix.len();
        let rest = &input[op_idx..];
        let mut chars = rest.chars();
        if let Some(op) = chars.next()
            && (op == '<' || op == '>')
        {
            // Check for compound operators >= and <=
            let (full_op, op_len) = if chars.next() == Some('=') {
                (format!("{}=", op), op.len_utf8() + 1)
            } else {
                (op.to_string(), op.len_utf8())
            };
            let value_start = op_idx + op_len;
            // If the value is quoted, find the closing quote as the boundary.
            // Otherwise, use whitespace as the boundary.
            let value_end = if input[value_start..].starts_with('"') {
                input[value_start + 1..]
                    .find('"')
                    .map(|r| value_start + 1 + r + 1)
                    .unwrap_or(input.len())
            } else {
                input[value_start..]
                    .find(char::is_whitespace)
                    .map(|r| value_start + r)
                    .unwrap_or(input.len())
            };
            let value = &input[value_start..value_end];
            let norm = normalize_date_literal(value).unwrap_or_else(|| value.to_string());
            out.push_str(&format!("{}{}{}", prefix, full_op, norm));
            idx = value_end;
            continue;
        }

        // Not a comparison; copy current char and advance
        out.push_str(&input[start..start + prefix.len()]);
        idx = start + prefix.len();
    }

    out.push_str(&input[idx..]);
    out
}

pub(crate) fn normalize_date_literals(input: &str, field: &str) -> String {
    let prefix = format!("{}:", field);
    let mut out = String::with_capacity(input.len());
    let mut idx = 0usize;

    while let Some(rel) = input[idx..].find(&prefix) {
        let start = idx + rel;
        out.push_str(&input[idx..start]);

        let value_start = start + prefix.len();
        // If the value is quoted, find the closing quote as the boundary.
        // Otherwise, use whitespace as the boundary.
        let value_end = if input[value_start..].starts_with('"') {
            input[value_start + 1..]
                .find('"')
                .map(|r| value_start + 1 + r + 1)
                .unwrap_or(input.len())
        } else {
            input[value_start..]
                .find(char::is_whitespace)
                .map(|r| value_start + r)
                .unwrap_or(input.len())
        };
        let value = &input[value_start..value_end];

        // Leave the shapes the range, comparison and `IN` passes own; an empty value is a
        // bare `field:` with nothing after it.
        if value.starts_with(['[', '{', '<', '>']) || value.is_empty() {
            out.push_str(&input[start..value_end]);
            idx = value_end;
            continue;
        }

        // Quoted, because RFC3339 contains colons and the grammar would otherwise read
        // `2024-06-15T00` as a field name. Only on success: a failed normalisation returns
        // `value` verbatim, which may already carry quotes.
        let rendered = match normalize_date_literal(value) {
            Some(norm) => format!("\"{norm}\""),
            None => value.to_string(),
        };
        out.push_str(&format!("{}{}", prefix, rendered));
        idx = value_end;
    }

    out.push_str(&input[idx..]);
    out
}

/// Normalize a query the way a search does, and build the parser a search would use.
///
/// Shared by the search path, the count-only path and validation, so that what validation
/// reports is what a search would actually do. A validator that built its parser differently —
/// a different default field set, a different normalization — would be worse than none: it would
/// disagree with the search it exists to predict.
pub(crate) fn prepare_query_parser(
    tantivy_index: &Index,
    fields: &SchemaFields,
    schema: &IndexSchema,
    query: &str,
) -> (String, Vec<String>, tantivy::query::QueryParser) {
    // Fold whitespace the grammar's set parser cannot skip down to an ASCII space first, so no
    // later pass — and above all `parse_query_lenient` — is handed a character that makes its
    // element loop spin without consuming input.
    let query = fold_untokenizable_whitespace(query);
    let query = query.as_ref();

    // Shadow names first, so every later rewriter — and the parser — sees only fields the
    // Tantivy schema actually carries.
    let query = rewrite_shadow_fields(query, schema);

    // Normalize date literals against the schema so naive inputs match indexed Date fields,
    // then facets, then rewrite single-term prefixes into ranges.
    let (normalized_query, prefix_notes) = normalize_prefix_query(
        &normalize_facet_query(&normalize_date_query(&query, schema), schema),
        tantivy_index,
    );

    // Only text and JSON fields are default search fields, so an unqualified term is never
    // attempted against a numeric or date field — which the parser reports as a type error
    // rather than as a non-match.
    let tantivy_schema = tantivy_index.schema();
    let default_query_fields: Vec<Field> = fields
        .indexed_fields
        .values()
        .filter(|field| {
            matches!(
                tantivy_schema.get_field_entry(**field).field_type(),
                tantivy::schema::FieldType::Str(_) | tantivy::schema::FieldType::JsonObject(_)
            )
        })
        .cloned()
        .collect();

    let parser = tantivy::query::QueryParser::for_index(tantivy_index, default_query_fields);
    (normalized_query, prefix_notes, parser)
}

/// Whether the parser resolved this ambiguity and ran the clause anyway.
///
/// The grammar reads `field:value` whose value contains a colon as a field name first, then
/// re-reads it as a term. It reports that as an error but the clause still executes, so it does
/// not belong in [`SearchOutcome::discarded`].
pub(crate) fn is_recovered_ambiguity(err: &tantivy::query::QueryParserError) -> bool {
    matches!(err, tantivy::query::QueryParserError::SyntaxError(detail)
        if detail.contains("parsed possible invalid field as term"))
}

/// Describe a dropped clause: what was lost from the query, and what to use instead where
/// there is an alternative.
///
/// Replaces Tantivy's own wording, which names parser internals rather than the effect on the
/// query — an exists leaf reports "Range query need to target a specific field", and a
/// non-indexed field reports being "not declared as indexed".
/// Note for a field the schema does not have.
///
/// Shared with [`unresolvable_fields`] so that when both the parser and the schema check see the
/// same field, the two notes are one string and collapse in [`describe_discarded_all`].
pub(crate) fn unknown_field_note(field: &str) -> String {
    format!(
        "unknown field '{field}' — the clause naming it was dropped, so this result set does \
         not match what the query asked for"
    )
}

/// Note for a field present in the schema but not indexed, and so not queryable.
pub(crate) fn non_indexed_field_note(field: &str) -> String {
    format!(
        "field '{field}' exists but is not indexed, so the clause naming it was dropped and \
         this result set does not match what the query asked for"
    )
}

/// Whether the lenient parse left nothing to run.
///
/// Tantivy trims discarded clauses out of the AST and returns `EmptyQuery` when that removes
/// every one of them. It matches no documents, which makes this the difference between having
/// answered a different question and having asked nothing at all.
pub(crate) fn nothing_survived(parsed: &dyn tantivy::query::Query) -> bool {
    parsed.is::<tantivy::query::EmptyQuery>()
}

pub(crate) fn describe_discarded(err: &tantivy::query::QueryParserError) -> String {
    use tantivy::query::QueryParserError as E;
    match err {
        E::FieldDoesNotExist(field) => unknown_field_note(field),
        E::FieldNotIndexed(field) => non_indexed_field_note(field),
        E::UnsupportedQuery(detail) => {
            // The parser refuses every exists leaf with this text, whatever the field type.
            if detail.contains("Range query need to target a specific field") {
                "field-presence tests (`field:*`) are not supported; the clause was dropped. \
                 Use a bounded range or an explicit value instead"
                    .to_string()
            } else {
                format!("unsupported clause was dropped: {detail}")
            }
        }
        E::FieldDoesNotHavePositionsIndexed(field) => format!(
            "field '{field}' has no positions indexed, so the phrase clause against it was \
             dropped; phrase queries need a text field"
        ),
        E::ExpectedInt(_) | E::ExpectedFloat(_) | E::ExpectedBool(_) | E::ExpectedBase64(_) => {
            format!("a value did not match its field's type, so the clause was dropped: {err}")
        }
        other => format!("clause was dropped: {other}"),
    }
}

/// Describe every clause the query lost, from both sources that can tell: the parser's error
/// list, and a schema check covering the field names the parser resolved to something
/// ineffective.
///
/// One note per distinct problem. An unfielded term is attempted against every default field,
/// so a single mistake arrives from the parser once per field. And where both sources name the
/// same field the schema's verdict wins, since it distinguishes a field that is absent from one
/// that is present but not indexed — the parser sees only that it is missing from the Tantivy
/// schema and reports both as unknown.
pub(crate) fn describe_discarded_all(
    errors: &[tantivy::query::QueryParserError],
    query: &str,
    schema: &IndexSchema,
) -> Vec<String> {
    let from_schema = unresolvable_fields(query, schema);
    let claimed = |field: &str| {
        let field = field.replace('\\', "");
        from_schema.iter().any(|(name, _)| *name == field)
    };

    let mut out: Vec<String> = Vec::new();
    for err in errors {
        use tantivy::query::QueryParserError as E;
        if is_recovered_ambiguity(err) {
            continue;
        }
        if matches!(err, E::FieldDoesNotExist(field) | E::FieldNotIndexed(field) if claimed(field))
        {
            continue;
        }
        let described = describe_discarded(err);
        if !out.contains(&described) {
            out.push(described);
        }
    }
    for (field, issue) in from_schema {
        let note = match issue {
            FieldIssue::Unknown => unknown_field_note(&field),
            FieldIssue::NotIndexed => non_indexed_field_note(&field),
        };
        if !out.contains(&note) {
            out.push(note);
        }
    }
    out
}

/// Byte offset of the first `:` not preceded by a backslash.
pub(crate) fn first_unescaped_colon(token: &str) -> Option<usize> {
    let mut escaped = false;
    for (idx, ch) in token.char_indices() {
        match ch {
            _ if escaped => escaped = false,
            '\\' => escaped = true,
            ':' => return Some(idx),
            _ => {}
        }
    }
    None
}

/// One field name a query references, and where it sits in the query string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldReference<'a> {
    /// Byte range of the name as written, escapes included.
    ///
    /// What a rewriter splices over. Kept separate from `name` because the two differ whenever
    /// the query escaped something: `k8s\.node` occupies ten bytes and names a nine-byte field.
    pub span: std::ops::Range<usize>,
    /// The name to look the schema up by, with the query's escapes resolved.
    pub name: Cow<'a, str>,
}

/// Every field name a query references, in the order they appear.
///
/// Shared so the readers of this question cannot disagree about what a query says: the shadow
/// rewriter splices over the span, [`unresolvable_fields`] classifies the name against the
/// schema, and the MCP layer lists the names for an agent.
///
/// A reference is the text before the first unescaped colon of a segment, after any leading
/// `+`, `-` or `!`, and a segment yields at most one. The rules that are not obvious:
///
/// - **Only the first colon splits.** A colon occurs inside values too, so taking every
///   name-then-colon run would read `2024-06-15T00` out of `created:2024-06-15T00:00:00Z` and
///   `https` out of `url:https://x`.
/// - **A segment is a whitespace token split again at `(` and `)`,** since a parenthesis ends
///   one clause and begins another without needing a space: `AND(sha1:x)` references `sha1`.
/// - **Only *leading* occurrence operators are stripped** — `content-type` is a name a `-` sits
///   inside.
/// - **Phrases, ranges and sets hold values, so nothing inside one is read.** Depth is tracked
///   for `[`/`{` only; parentheses group clauses and deliberately do not count.
///
/// The result borrows from `query`, and `name` allocates only for a name that was escaped.
pub fn field_references(query: &str) -> Vec<FieldReference<'_>> {
    let mut found = Vec::new();
    let mut inside_phrase = false;
    // Depth of `[ ]` and `{ }` only. Parentheses group clauses and do contain field references.
    let mut value_depth = 0i32;

    for (token_start, token) in whitespace_tokens(query) {
        // Read before the token's own delimiters are counted, so a token that opens a range
        // still offers the field name in front of it: `created:[2024-01-01` names `created`.
        let readable_position = !inside_phrase && value_depth == 0;

        for (offset, segment) in clause_segments(token) {
            if readable_position
                && let Some(reference) = leading_field_reference(segment, token_start + offset)
            {
                found.push(reference);
            }
        }

        // Then track what this token opened or closed for the tokens after it.
        let mut escaped = false;
        for ch in token.chars() {
            match ch {
                _ if escaped => escaped = false,
                '\\' => escaped = true,
                '"' => inside_phrase = !inside_phrase,
                '[' | '{' => value_depth += 1,
                ']' | '}' => value_depth = (value_depth - 1).max(0),
                _ => {}
            }
        }
    }

    found
}

/// Whitespace-separated tokens with their byte offsets, which `split_whitespace` drops.
pub(crate) fn whitespace_tokens(query: &str) -> impl Iterator<Item = (usize, &str)> {
    query.split_whitespace().scan(0usize, |cursor, token| {
        // The gap between tokens is whitespace alone, so the token's first occurrence at or
        // after the cursor is its position.
        let start = *cursor
            + query[*cursor..]
                .find(token)
                .expect("tokens come from this string");
        *cursor = start + token.len();
        Some((start, token))
    })
}

/// A token split at its parentheses, each piece with its offset within the token.
///
/// A parenthesis ends one clause and begins another without needing a space, so the pieces
/// either side of it are separate candidates for a field reference.
pub(crate) fn clause_segments(token: &str) -> impl Iterator<Item = (usize, &str)> {
    let mut offset = 0;
    token
        .split_inclusive(['(', ')'])
        .map(move |piece| {
            let start = offset;
            offset += piece.len();
            (start, piece.trim_end_matches(['(', ')']))
        })
        .filter(|(_, piece)| !piece.is_empty())
}

/// The field reference a segment opens with, if it opens with one.
///
/// `at` is the segment's byte offset in the whole query, so the returned span is absolute.
pub(crate) fn leading_field_reference(segment: &str, at: usize) -> Option<FieldReference<'_>> {
    let sigils = segment.len() - segment.trim_start_matches(['+', '-', '!']).len();
    let segment = &segment[sigils..];

    // A field reference never opens a phrase, a range or a set.
    if segment.starts_with(['"', '[', '{']) {
        return None;
    }
    let colon = first_unescaped_colon(segment)?;
    let name = &segment[..colon];
    if name.is_empty() {
        return None;
    }

    let start = at + sigils;
    Some(FieldReference {
        span: start..start + name.len(),
        // Escapes are the query's, not the field's: `k8s\.node` names the field `k8s.node`.
        name: if name.contains('\\') {
            Cow::Owned(name.replace('\\', ""))
        } else {
            Cow::Borrowed(name)
        },
    })
}

/// Why a field name a query references cannot answer it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FieldIssue {
    /// No such field in the schema.
    Unknown,
    /// Present in the schema but not indexed, so not queryable.
    NotIndexed,
}

/// Field names a query references that the schema cannot resolve, each with its reason.
///
/// Covers what the parser does not report. When an index has a JSON field, that field is a
/// default query field, so Tantivy resolves an unrecognised `name:` prefix as a path *inside*
/// it: the clause parses cleanly, matches nothing, and produces no error. A clause lost that
/// way is as ineffective as a dropped one, and in a negation it disables the exclusion.
///
/// Which names a query references is [`field_references`]'s question; this one only says what
/// the schema makes of each. Nothing is claimed when the schema is empty, or for a dotted name
/// whose root is a real field, since the parser judges the path itself.
pub(crate) fn unresolvable_fields(query: &str, schema: &IndexSchema) -> Vec<(String, FieldIssue)> {
    // An index with no stored schema yields an empty one; everything would look unresolvable.
    if schema.fields.is_empty() {
        return Vec::new();
    }

    let mut found: Vec<(String, FieldIssue)> = Vec::new();

    for reference in field_references(query) {
        let name = reference.name;

        // `id` and `_seq` are added to every Tantivy schema, not to `IndexSchema::fields`.
        if name == "id" || name == "_seq" {
            continue;
        }

        let issue = match schema.fields.get(name.as_ref()) {
            // Shadow fields are rewritten to `id` before a query reaches the engine.
            Some(def) if def.indexed || def.is_shadow => continue,
            Some(_) => FieldIssue::NotIndexed,
            // A dotted name whose root is a real field is a path expression; whether the path
            // is valid for that field's type is the parser's judgement, not ours.
            None if name
                .split_once('.')
                .is_some_and(|(root, _)| schema.fields.contains_key(root)) =>
            {
                continue;
            }
            None => FieldIssue::Unknown,
        };

        if !found.iter().any(|(seen, _)| *seen == name) {
            found.push((name.into_owned(), issue));
        }
    }

    found
}

/// Characters that make a value the parser's business rather than the key-value store's.
///
/// Each one is syntax — a space, quote or parenthesis ends the value, `*` is the prefix operator
/// and `^` the boost — and the key-value store can only look a key up whole, so a value carrying
/// one falls through to the search index instead.
///
/// Matched *before* escapes are removed, so an escaped operator goes to the parser too. That is
/// what keeps an identifier genuinely containing one reachable: the parser resolves `id:d1\^2`
/// to the literal `d1^2`, bare and inside a larger query alike.
///
/// `~` is deliberately absent. Tantivy reads it as slop only after a quoted phrase; against a
/// bare term it is an ordinary character an identifier may contain.
pub(crate) const QUERY_SYNTAX_IN_VALUE: &[char] = &[' ', '"', '(', ')', '*', '^'];

/// The identifier a whole-query `id:VALUE` or `shadowfield:VALUE` lookup names, or `None` when
/// the query is not that shape.
///
/// This is the one path that answers without the search index, so it has to read the query the
/// way the parser would — otherwise a bare lookup and the same clause inside a larger query
/// disagree about which document was named. Two things keep them aligned:
///
/// - The field name ends at the first *unescaped* colon, the same position
///   [`rewrite_shadow_fields`] and [`unresolvable_fields`] read it at.
/// - Escapes in the value are removed, because the parser removes them: `id:urn\:x\:1` names
///   the key `urn:x:1`.
///
/// A value carrying anything in [`QUERY_SYNTAX_IN_VALUE`] is not a whole key and is left to the
/// parser.
pub(crate) fn parse_exact_id_query(query: &str, schema: &IndexSchema) -> Option<(String, bool)> {
    let query = query.trim();

    let colon = first_unescaped_colon(query)?;
    let field_part = query[..colon].trim();
    let value_part = query[colon + 1..].trim();

    if value_part.contains(QUERY_SYNTAX_IN_VALUE) {
        return None;
    }

    // The document key under its own name, or under a shadow name that stands for it.
    if field_part != "id" && !schema.is_shadow_field(field_part) {
        return None;
    }

    Some((unescape_query_value(value_part), true))
}

/// A query value with the parser's escapes removed: `\x` is the literal `x`.
///
/// Tantivy's grammar reads a backslash as "the next character is data, not syntax", and drops
/// the backslash when it builds the term. Anything comparing a value against stored data has to
/// do the same, or the two see different strings. A trailing lone backslash is kept, since there
/// is no character after it for it to have been escaping.
pub(crate) fn unescape_query_value(value: &str) -> String {
    if !value.contains('\\') {
        return value.to_string();
    }
    let mut out = String::with_capacity(value.len());
    let mut chars = value.chars();
    while let Some(ch) = chars.next() {
        match ch {
            '\\' => match chars.next() {
                Some(escaped) => out.push(escaped),
                None => out.push('\\'),
            },
            _ => out.push(ch),
        }
    }
    out
}

/// Rewrite each shadow field reference in a query string to the canonical `id` field it stands
/// for.
///
/// A shadow field is the identifier under its source name: the value lives in `id`, which every
/// Tantivy schema carries indexed, so the name can appear anywhere a field name can — alone,
/// where [`parse_exact_id_query`] answers from the key-value store without parsing, or inside a
/// larger query, where it is rewritten here and runs against the search index like any other
/// clause.
///
/// Only the name in field-reference position is replaced, as [`field_references`] finds it, so
/// a shadow name inside a phrase or a range stays the value it is. The replacement is spliced
/// by byte range, leaving the rest of the query — whitespace, quoting, escapes — untouched.
pub(crate) fn rewrite_shadow_fields(query: &str, schema: &IndexSchema) -> String {
    if schema.shadow_fields.is_empty() {
        return query.to_string();
    }

    let spans: Vec<std::ops::Range<usize>> = field_references(query)
        .into_iter()
        .filter(|reference| schema.is_shadow_field(&reference.name))
        .map(|reference| reference.span)
        .collect();
    if spans.is_empty() {
        return query.to_string();
    }

    let mut rewritten = String::with_capacity(query.len());
    let mut cursor = 0;
    for span in spans {
        rewritten.push_str(&query[cursor..span.start]);
        rewritten.push_str("id");
        cursor = span.end;
    }
    rewritten.push_str(&query[cursor..]);
    rewritten
}

/// The single token an analyzer produces from `text`, or None if it produces any other number.
pub(crate) fn single_token(
    analyzer: &mut tantivy::tokenizer::TextAnalyzer,
    text: &str,
) -> Option<String> {
    use tantivy::tokenizer::TokenStream;

    let mut tokens: Vec<String> = Vec::new();
    let mut stream = analyzer.token_stream(text);
    stream.process(&mut |token| tokens.push(token.text.clone()));
    match tokens.len() {
        1 => tokens.pop(),
        _ => None,
    }
}

/// The exclusive upper bound of a lexicographic prefix range: `term` with its final scalar raised
/// to the next value the analyzer leaves unchanged.
///
/// Tantivy tokenizes a range bound and takes a single token only, so a candidate the analyzer
/// rewrites or discards cannot serve as one. Ascending order keeps the bound tight: a scalar the
/// analyzer discards cannot appear in an indexed term either, so stepping over it admits nothing.
///
/// None when no candidate qualifies, leaving the clause for the caller to report.
pub(crate) fn prefix_upper_bound(
    term: &str,
    analyzer: &mut tantivy::tokenizer::TextAnalyzer,
) -> Option<String> {
    /// Enough to cross the punctuation runs between the digits, the ASCII letters and the
    /// alphanumeric scalars above them.
    const MAX_CANDIDATES: u32 = 64;

    let mut scalars: Vec<char> = term.chars().collect();
    let last = scalars.pop()?;
    let head: String = scalars.into_iter().collect();

    let mut code = last as u32;
    for _ in 0..MAX_CANDIDATES {
        code += 1;
        // Surrogates are not scalar values, so the successor of U+D7FF is U+E000.
        if code == 0xD800 {
            code = 0xE000;
        }
        let candidate = format!("{head}{}", char::from_u32(code)?);
        if single_token(analyzer, &candidate).as_deref() == Some(candidate.as_str()) {
            return Some(candidate);
        }
    }
    None
}

/// Split a single-term prefix clause into its term and any trailing boost.
///
/// None for the values this rewrite does not claim: a quoted value is a phrase prefix, a bare `*`
/// a presence test, and a range or group is not a term.
pub(crate) fn single_term_prefix(value: &str) -> Option<(&str, &str)> {
    let (head, boost) = match value.find('^') {
        Some(at) => value.split_at(at),
        None => (value, ""),
    };
    let term = head.strip_suffix('*')?;
    if term.contains('*') || matches!(term.chars().next()?, '"' | '[' | '{' | '(') {
        return None;
    }
    Some((term, boost))
}

/// Note for a prefix clause left as the bare term because no bound was available.
pub(crate) fn unrewritable_prefix_note(field: &str, value: &str) -> String {
    format!(
        "'{field}:{value}' could not be rewritten as a prefix range, so it matched the term \
         '{}' exactly; write the range you want instead",
        value.trim_end_matches('*')
    )
}

/// Rewrite a single-term prefix — `field:pre*` — into the equivalent lexicographic range, on text
/// and string fields.
///
/// The grammar has no prefix operator: it drops the `*` and matches `pre` as a whole term without
/// raising an error. Tantivy tokenizes the bounds, so the prefix may be written in any case.
///
/// Returns the query with a note per prefix clause left unrewritten.
pub(crate) fn normalize_prefix_query(query: &str, tantivy_index: &Index) -> (String, Vec<String>) {
    use tantivy::schema::FieldType;

    if !query.contains('*') {
        return (query.to_string(), Vec::new());
    }

    let tantivy_schema = tantivy_index.schema();
    let mut normalized = query.to_string();
    let mut notes = Vec::new();

    for (_, entry) in tantivy_schema.fields() {
        let FieldType::Str(ref options) = *entry.field_type() else {
            continue;
        };
        let Some(indexing) = options.get_indexing_options() else {
            continue;
        };
        let name = entry.name();
        let prefix = format!("{name}:");
        if !normalized.contains(&prefix) {
            continue;
        }
        let Some(mut analyzer) = tantivy_index.tokenizers().get(indexing.tokenizer()) else {
            continue;
        };

        let mut out = String::with_capacity(normalized.len());
        let mut idx = 0usize;
        while let Some(rel) = normalized[idx..].find(&prefix) {
            let start = idx + rel;
            out.push_str(&normalized[idx..start]);

            // The value runs to the next whitespace or to a closing paren from a group.
            let value_start = start + prefix.len();
            let value_end = normalized[value_start..]
                .find(|ch: char| ch.is_whitespace() || ch == ')')
                .map(|r| value_start + r)
                .unwrap_or(normalized.len());
            let value = &normalized[value_start..value_end];

            let range = single_term_prefix(value).map(|(term, boost)| {
                single_token(&mut analyzer, term)
                    .and_then(|lower| {
                        let upper = prefix_upper_bound(&lower, &mut analyzer)?;
                        Some(format!("{name}:[{lower} TO {upper}}}{boost}"))
                    })
                    .ok_or(value)
            });

            match range {
                Some(Ok(rewritten)) => out.push_str(&rewritten),
                Some(Err(unrewritable)) => {
                    notes.push(unrewritable_prefix_note(name, unrewritable));
                    out.push_str(&normalized[start..value_end]);
                }
                None => out.push_str(&normalized[start..value_end]),
            }
            idx = value_end;
        }
        out.push_str(&normalized[idx..]);
        normalized = out;
    }

    (normalized, notes)
}

/// Quote facet path values so the parser resolves them to facet terms.
///
/// The parser matches a facet term only against a quoted value, so `category:/electronics/phones`
/// alone matches nothing. Only unquoted values beginning with `/` are touched; a quoted value is
/// already in the form the parser wants. Matching is hierarchical, so a parent path matches its
/// descendants.
pub(crate) fn normalize_facet_query(query: &str, schema: &IndexSchema) -> String {
    let facet_fields: Vec<&str> = schema
        .fields
        .iter()
        .filter(|(_, def)| matches!(def.field_type, TantivyFieldType::Facet))
        .map(|(name, _)| name.as_str())
        .collect();

    if facet_fields.is_empty() {
        return query.to_string();
    }

    let mut normalized = query.to_string();
    for field in facet_fields {
        let prefix = format!("{}:/", field);
        if !normalized.contains(&prefix) {
            continue;
        }

        let mut out = String::with_capacity(normalized.len() + 2);
        let mut idx = 0usize;
        while let Some(rel) = normalized[idx..].find(&prefix) {
            let start = idx + rel;
            out.push_str(&normalized[idx..start]);

            // The path runs to the next whitespace or to a closing paren from a group.
            let value_start = start + field.len() + ':'.len_utf8();
            let value_end = normalized[value_start..]
                .find(|ch: char| ch.is_whitespace() || ch == ')')
                .map(|r| value_start + r)
                .unwrap_or(normalized.len());

            out.push_str(&format!(
                "{}:\"{}\"",
                field,
                &normalized[value_start..value_end]
            ));
            idx = value_end;
        }
        out.push_str(&normalized[idx..]);
        normalized = out;
    }

    normalized
}

/// Rewrite every date literal in a query to RFC3339, the only form Tantivy's date parser
/// accepts. Covers:
///
/// - `field:value`
/// - `field:>value`, `field:<value`, `field:>=value`, `field:<=value`
/// - `field:[lower TO upper]`, `field:{lower TO upper}`, and the mixed pairs
/// - `field: IN [a b c]`
///
/// A shape no pass recognises reaches the parser unrewritten, which drops the clause rather
/// than raising an error — so a gap here surfaces as a query that matches nothing.
pub(crate) fn normalize_date_query(query: &str, schema: &IndexSchema) -> String {
    let date_fields: HashSet<&str> = schema
        .fields
        .iter()
        .filter(|(_, def)| matches!(def.field_type, TantivyFieldType::Date))
        .map(|(name, _)| name.as_str())
        .collect();

    if date_fields.is_empty() {
        return query.to_string();
    }

    let mut normalized = query.to_string();
    for field in &date_fields {
        // Each pass claims one shape; the single-literal pass takes whatever is left, so it
        // runs last or it would rewrite a range bound as a whole value.
        normalized = normalize_date_ranges(&normalized, field);
        normalized = normalize_date_in_sets(&normalized, field);
        normalized = normalize_date_comparisons(&normalized, field);
        normalized = normalize_date_literals(&normalized, field);
    }

    normalized
}
