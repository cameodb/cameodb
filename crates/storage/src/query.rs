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

/// The clause context a `NOT` keyword sits in, which decides the rewrite.
#[derive(Clone, Copy, PartialEq, Eq)]
enum NotContext {
    /// Nothing a leaf follows precedes it: the start of the query, `(`, or a `+`/`-` sign that
    /// is itself at a leaf boundary — the sign is copied through, so `+NOT b` rewrites like a
    /// bare `NOT b`.
    Bare,
    /// After `AND`.
    And,
    /// After `OR`.
    Or,
    /// After another leaf — `a NOT b`.
    AfterLeaf,
}

/// The context of the `NOT` starting at `not_pos`, or `None` when `NOT` is not an operator
/// there — mid-term (`aNOT b`) or behind a sign that is part of a term (`a-NOT b`).
fn not_context(bytes: &[u8], not_pos: usize) -> Option<NotContext> {
    /// Whether a `+`/`-` at `sign` begins a leaf rather than sitting inside one.
    fn sign_is_occur(bytes: &[u8], sign: usize) -> bool {
        sign == 0 || bytes[sign - 1].is_ascii_whitespace() || matches!(bytes[sign - 1], b'(' | b')')
    }

    if not_pos > 0 {
        match bytes[not_pos - 1] {
            c if c.is_ascii_whitespace() || c == b'(' => {}
            b'+' | b'-' => {
                return sign_is_occur(bytes, not_pos - 1).then_some(NotContext::Bare);
            }
            _ => return None,
        }
    }
    let mut end = not_pos;
    while end > 0 && bytes[end - 1].is_ascii_whitespace() {
        end -= 1;
    }
    if end == 0 {
        return Some(NotContext::Bare);
    }
    match bytes[end - 1] {
        b'(' => Some(NotContext::Bare),
        b'+' | b'-' => Some(if sign_is_occur(bytes, end - 1) {
            NotContext::Bare
        } else {
            // `x- NOT b` — the sign belongs to the leaf before it.
            NotContext::AfterLeaf
        }),
        b')' => Some(NotContext::AfterLeaf),
        _ => {
            let mut start = end;
            while start > 0
                && !bytes[start - 1].is_ascii_whitespace()
                && !matches!(bytes[start - 1], b'(' | b')')
            {
                start -= 1;
            }
            Some(match &bytes[start..end] {
                b"AND" => NotContext::And,
                b"OR" => NotContext::Or,
                _ => NotContext::AfterLeaf,
            })
        }
    }
}

/// Every bracket of the query that opens a leaf's span — `(`, `[` or `{` outside quotes — paired
/// with the bracket that closes it, or with the query's length when none does. Ordered by the
/// opening offset.
///
/// Brackets pair by nesting alone, whatever their kind, which is how [`not_leaf_end`] counts
/// them. Found in one pass, so that a leaf scan steps over a group instead of walking it again:
/// without it `NOT (NOT (NOT ...` scans the rest of the query once per level.
fn bracket_pairs(bytes: &[u8]) -> Vec<(usize, usize)> {
    let n = bytes.len();
    let mut pairs = Vec::new();
    // Indexes into `pairs` of the brackets still open.
    let mut open = Vec::new();
    let mut in_quote = false;
    let mut i = 0usize;
    while i < n {
        let c = bytes[i];
        if c == b'\\' {
            i += 2;
            continue;
        }
        if in_quote {
            in_quote = c != b'"';
            i += 1;
            continue;
        }
        match c {
            b'"' => in_quote = true,
            b'(' | b'[' | b'{' => {
                open.push(pairs.len());
                pairs.push((i, n));
            }
            b')' | b']' | b'}' => {
                if let Some(k) = open.pop() {
                    pairs[k].1 = i;
                }
            }
            _ => {}
        }
        i += 1;
    }
    pairs
}

/// Byte offset one past the leaf a `NOT` negates, `start` pointing at its first byte.
///
/// A leaf is a parenthesised group, a quoted phrase, a `field:value` — where the value may
/// itself be quoted, a bracketed range or `field:( ... )` group, all of which can contain
/// spaces — a `field: IN [ ... ]` set, which whitespace alone would otherwise split after the
/// colon, or a bare term. It ends at the first space or `)` outside all of those. `pairs` is
/// [`bracket_pairs`] of the same query, so a bracket inside the leaf is stepped over in one jump.
fn not_leaf_end(query: &str, start: usize, pairs: &[(usize, usize)]) -> usize {
    let bytes = query.as_bytes();
    let n = bytes.len();
    let mut i = start;
    let mut in_quote = false;
    while i < n {
        let c = bytes[i];
        if c == b'\\' {
            i += 2;
            continue;
        }
        if in_quote {
            in_quote = c != b'"';
            i += 1;
            continue;
        }
        match c {
            b'"' => in_quote = true,
            // Past the bracket that closes it — or to the end, which is where an unclosed one
            // keeps the leaf open to.
            b'(' | b'[' | b'{' => {
                let close = pairs
                    .binary_search_by_key(&i, |&(open, _)| open)
                    .map_or(n, |k| pairs[k].1);
                i = close.saturating_add(1);
                continue;
            }
            b')' => return i,
            c if c.is_ascii_whitespace() => break,
            _ => {}
        }
        i += 1;
    }
    // A trailing `\` steps past the end.
    let i = i.min(n);

    // `field: IN [a b]` — the set sits behind the whitespace after the colon.
    if i > start && bytes[i - 1] == b':' {
        let mut p = i;
        while p < n && bytes[p].is_ascii_whitespace() {
            p += 1;
        }
        if bytes[p..].starts_with(b"IN") {
            let mut q = p + 2;
            while q < n && bytes[q].is_ascii_whitespace() {
                q += 1;
            }
            if q < n && bytes[q] == b'[' {
                let mut set_quote = false;
                while q < n {
                    match bytes[q] {
                        b'\\' => q += 1,
                        b'"' => set_quote = !set_quote,
                        b']' if !set_quote => return q + 1,
                        _ => {}
                    }
                    q += 1;
                }
                return n;
            }
        }
    }
    i
}

/// Rewrite `NOT` clauses into the boolean shapes Tantivy actually evaluates.
///
/// Tantivy parses `NOT leaf` as a negated clause, but repairs an all-negative clause only at
/// the top level: nested under `Must` or `Should` it matches no documents. So `a AND NOT b`
/// parses cleanly and answers zero documents, `a OR NOT b` silently drops the `NOT` arm, and a
/// bare `NOT b` runs correctly yet reports a discarded clause. The rewrite puts a positive
/// clause beside every negation, which is the only shape the evaluator honours:
///
/// - `a AND NOT b` → `a AND -b`: the `-` binds the leaf that follows.
/// - `a OR NOT b` → `a OR (* -b)`: the `*` is the positive the `OR` needs. Writing `-b`
///   directly would make it an exclusion on the whole disjunction — `a OR -b` drops a document
///   matching both.
/// - `NOT b` at the start of the query, after `(`, or after a `+`/`-` sign → `(* -b)`.
/// - `a NOT b` → `a -b`: the exclusion it already evaluates as.
///
/// A run of `NOT`s collapses by parity: `NOT NOT a` is `a`. Inside a `field:( ... )` group the
/// positive form cannot be written — the `*` would take the field's scope, and `field:*` is
/// refused — so only the `-` rewrites apply there, and a `NOT` needing `(* -x)` is left for
/// the parser to report.
///
/// `NOT` is the operator only in uppercase at a leaf boundary, outside quotes and outside
/// `[]`/`{}` ranges and sets, matching where the grammar reads it as one: `notes:x`,
/// `a-NOT b` and `title:"a NOT b"` are all left alone.
pub(crate) fn normalize_not_clauses(query: &str) -> Cow<'_, str> {
    if !query.contains("NOT") {
        return Cow::Borrowed(query);
    }

    let bytes = query.as_bytes();
    let n = bytes.len();
    let mut out = String::with_capacity(query.len() + 8);
    let mut copied = 0usize;
    let mut i = 0usize;
    let mut in_quote = false;
    // Depth of `[` and `{` — inside a range or an `IN` set a `NOT` is a literal element.
    let mut value_depth = 0usize;
    // Per open `(`, whether it opened a `field:(` group, and how many of those are open.
    let mut groups: Vec<bool> = Vec::new();
    let mut field_groups = 0usize;
    // Where each open `(* -` rewrite's `)` belongs. Its leaf is scanned in place, and leaves
    // nest, so the innermost — the nearest end — is on top.
    let mut closes: Vec<usize> = Vec::new();
    // Built at the first `NOT` that needs a leaf's extent.
    let mut pairs: Option<Vec<(usize, usize)>> = None;

    while i < n {
        while let Some(&end) = closes.last()
            && end <= i
        {
            closes.pop();
            out.push_str(&query[copied..end]);
            out.push(')');
            copied = end;
        }
        let c = bytes[i];
        if c == b'\\' {
            i += 2;
            continue;
        }
        if in_quote {
            in_quote = c != b'"';
            i += 1;
            continue;
        }
        let mut advanced = false;
        match c {
            b'"' => in_quote = true,
            b'[' | b'{' => value_depth += 1,
            b']' | b'}' => value_depth = value_depth.saturating_sub(1),
            b'(' if value_depth == 0 => {
                let field_group = i > 0 && bytes[i - 1] == b':';
                field_groups += usize::from(field_group);
                groups.push(field_group);
            }
            b')' if value_depth == 0 => {
                if groups.pop() == Some(true) {
                    field_groups -= 1;
                }
            }
            b'N' if value_depth == 0
                && bytes[i..].starts_with(b"NOT")
                && i + 3 < n
                && bytes[i + 3].is_ascii_whitespace() =>
            {
                if let Some(ctx) = not_context(bytes, i) {
                    // A run of `NOT`s negates by parity; each one must be followed by
                    // whitespace to be the keyword at all.
                    let mut count = 1usize;
                    let mut p = i + 3;
                    while p < n && bytes[p].is_ascii_whitespace() {
                        p += 1;
                    }
                    while bytes[p..].starts_with(b"NOT")
                        && p + 3 < n
                        && bytes[p + 3].is_ascii_whitespace()
                    {
                        count += 1;
                        p += 3;
                        while p < n && bytes[p].is_ascii_whitespace() {
                            p += 1;
                        }
                    }
                    let pairs = pairs.get_or_insert_with(|| bracket_pairs(bytes));
                    let leaf_end = not_leaf_end(query, p, pairs);
                    let leaf = &query[p..leaf_end];
                    let needs_positive = matches!(ctx, NotContext::Bare | NotContext::Or);
                    if leaf.is_empty()
                        || matches!(leaf, "AND" | "OR" | "NOT" | "IN")
                        || leaf.starts_with(['+', '-'])
                    {
                        // Every `NOT` of the run negates this same leaf and is refused for the
                        // same reason, so none is worth recounting the run from.
                        i = p;
                        advanced = true;
                    } else if !(needs_positive && field_groups > 0 && count % 2 == 1) {
                        out.push_str(&query[copied..i]);
                        if count.is_multiple_of(2) {
                            // `NOT NOT` is the clause alone — after a leaf it needs an
                            // operator to keep its conjunction.
                            if ctx == NotContext::AfterLeaf {
                                out.push_str("AND ");
                            }
                        } else {
                            match ctx {
                                NotContext::And | NotContext::AfterLeaf => out.push('-'),
                                _ => {
                                    out.push_str("(* -");
                                    closes.push(leaf_end);
                                }
                            }
                        }
                        // The leaf is scanned where it stands rather than rewritten on its own,
                        // so a group's `NOT`s — `NOT (a AND NOT b)` — rewrite by the same rules
                        // in the scope they sit in, and nesting costs neither stack nor a rescan.
                        copied = p;
                        i = p;
                        advanced = true;
                    }
                }
            }
            _ => {}
        }
        if !advanced {
            i += 1;
        }
    }

    while let Some(end) = closes.pop() {
        out.push_str(&query[copied..end]);
        out.push(')');
        copied = end;
    }

    if copied == 0 {
        return Cow::Borrowed(query);
    }
    out.push_str(&query[copied..]);
    Cow::Owned(out)
}

/// How deep a query may nest before it is refused. Each `(` — a group or a `field:( ... )` —
/// is a level until its `)`, and each `NOT` a level until the leaf it negates ends.
///
/// Tantivy's grammar, its AST rewrite and the query built from it all recurse once per level,
/// on a read-pool thread's 2 MiB stack, and running out of it aborts the node rather than
/// panicking. Through validate, search and count the stack ran out at 186 levels in a debug
/// build and 468 in release, `field:(` groups and `NOT (` nests reaching it first. 64 keeps a
/// wide margin under both and is far deeper than any query written by hand.
pub(crate) const MAX_QUERY_DEPTH: usize = 64;

/// The deepest nesting in `query`, counted as [`MAX_QUERY_DEPTH`] describes.
///
/// A `NOT` stays open until the term after it ends or, when it negates a group, until the
/// group closes: `a AND NOT b AND NOT c` is one level deep and `NOT (NOT (a))` four. Brackets
/// and `NOT` inside quotes, ranges and `IN` sets are text, not nesting.
pub(crate) fn query_depth(query: &str) -> usize {
    let bytes = query.as_bytes();
    let n = bytes.len();
    // Per open group, the levels it added: its own and those of the `NOT`s in front of it.
    let mut groups: Vec<usize> = Vec::new();
    let mut depth = 0usize;
    // `NOT`s still waiting on the leaf they negate.
    let mut pending = 0usize;
    let mut in_term = false;
    let mut in_quote = false;
    let mut value_depth = 0usize;
    let mut deepest = 0usize;
    let mut i = 0usize;
    while i < n {
        let c = bytes[i];
        if c == b'\\' {
            in_term = true;
            i += 2;
            continue;
        }
        if in_quote {
            in_quote = c != b'"';
            i += 1;
            continue;
        }
        match c {
            b'"' => {
                in_quote = true;
                in_term = true;
            }
            b'[' | b'{' => {
                value_depth += 1;
                in_term = true;
            }
            b']' | b'}' => value_depth = value_depth.saturating_sub(1),
            _ if value_depth > 0 => {}
            b'(' => {
                let added = 1 + pending;
                groups.push(added);
                depth += added;
                deepest = deepest.max(depth);
                pending = 0;
                in_term = false;
            }
            b')' => {
                depth -= groups.pop().unwrap_or(0);
                pending = 0;
                in_term = false;
            }
            c if c.is_ascii_whitespace() => {
                if in_term {
                    pending = 0;
                    in_term = false;
                }
            }
            // A sign belongs to the leaf after it, which may be a `NOT`.
            b'+' | b'-' if !in_term => {}
            b'N' if !in_term
                && bytes[i..].starts_with(b"NOT")
                && i + 3 < n
                && bytes[i + 3].is_ascii_whitespace() =>
            {
                pending += 1;
                deepest = deepest.max(depth + pending);
                i += 3;
                continue;
            }
            _ => in_term = true,
        }
        i += 1;
    }
    deepest
}

/// Normalize a query the way a search does, and build the parser a search would use.
///
/// Shared by the search path, the count-only path and validation, so that what validation
/// reports is what a search would actually do. A validator that built its parser differently —
/// a different default field set, a different normalization — would be worse than none: it would
/// disagree with the search it exists to predict.
///
/// Refuses a query nested past [`MAX_QUERY_DEPTH`] before any pass parses it.
pub(crate) fn prepare_query_parser(
    tantivy_index: &Index,
    fields: &SchemaFields,
    schema: &IndexSchema,
    query: &str,
    // `StorageConfig::query`; see `normalize_prefix_query`.
    policy: &QueryPolicy,
) -> Result<PreparedQuery, StoreError> {
    // Fold whitespace the grammar's set parser cannot skip down to an ASCII space first, so no
    // later pass — and above all `parse_query_lenient` — is handed a character that makes its
    // element loop spin without consuming input.
    let query = fold_untokenizable_whitespace(query);
    let query = query.as_ref();

    // Every pass below that parses — and the parser itself — recurses once per level.
    let depth = query_depth(query);
    if depth > MAX_QUERY_DEPTH {
        return Err(StoreError::QueryParser(
            tantivy::query::QueryParserError::SyntaxError(format!(
                "query nests {depth} levels deep, past the limit of {MAX_QUERY_DEPTH}; each \
                 parenthesised group and each NOT is a level"
            )),
        ));
    }

    // `AND NOT`, `OR NOT` and a leading `NOT` all parse and all evaluate wrongly — a negated
    // clause under `Must` or `Should` matches nothing. Rewrite them into the `-` and `(* -x)`
    // forms the evaluator honours while the text still says what the caller wrote.
    let query = normalize_not_clauses(query);
    let query = query.as_ref();

    // Shadow names first, so every later rewriter — and the parser — sees only fields the
    // Tantivy schema actually carries.
    let query = rewrite_shadow_fields(query, schema);

    // Only text and JSON fields are default search fields, so an unqualified term is never
    // attempted against a numeric or date field — which the parser reports as a type error
    // rather than as a non-match. Computed first, because an unqualified prefix is expanded
    // across exactly these fields and must not disagree with where an unqualified term goes.
    let tantivy_schema = tantivy_index.schema();
    let candidates = fields
        .indexed_fields
        .iter()
        .filter(|(_, field)| {
            matches!(
                tantivy_schema.get_field_entry(**field).field_type(),
                tantivy::schema::FieldType::Str(_) | tantivy::schema::FieldType::JsonObject(_)
            )
        })
        .map(|(name, _)| name.clone());
    // Capped by the node and ordered by the index's declared list, if it has one — the one
    // definition the listing also reports, so what a caller is told a bare term searches is what
    // it does search.
    let candidates: Vec<String> = candidates.collect();
    let available = match schema.default_fields.as_deref() {
        Some(declared) => declared.iter().filter(|n| candidates.contains(n)).count(),
        None => candidates.len(),
    };
    let (selected, truncated) = select_default_fields(
        candidates,
        schema.default_fields.as_deref(),
        policy.max_default_fields,
    );
    // Reported only when it made a difference to *this* query. The grammar is consulted only
    // then, so an index under the cap pays nothing for the check.
    let narrowed = (truncated && has_unqualified_clause(&query)).then(|| NarrowedDefaultFields {
        searched: selected.clone(),
        available,
        declared: schema.default_fields.is_some(),
    });
    let default_query_fields: Vec<Field> = selected
        .iter()
        .filter_map(|name| fields.indexed_fields.get(name).copied())
        .collect();

    // Normalize date literals against the schema so naive inputs match indexed Date fields,
    // then facets, then rewrite single-term prefixes into ranges.
    let (normalized_query, prefix_notes) = normalize_prefix_query(
        &normalize_facet_query(&normalize_date_query(&query, schema), schema),
        tantivy_index,
        policy,
        &default_query_fields,
    );

    // Last, so every pass above has seen the ranges it rewrites — a date range's quoted bounds
    // are already bare by now — and only what the grammar cannot carry is held.
    let (text, held) = hold_quoted_ranges(&normalized_query);
    let parser = tantivy::query::QueryParser::for_index(tantivy_index, default_query_fields);
    Ok(PreparedQuery {
        text,
        notes: prefix_notes,
        parser,
        narrowed,
        held,
    })
}

/// A query ready to run: the normalized text, what normalizing it noted, and the parser.
pub(crate) struct PreparedQuery {
    /// The query as normalized, with each range clause holding a quoted bound replaced by a
    /// placeholder — see [`hold_quoted_ranges`]. [`PreparedQuery::shown`] is it as written.
    text: String,
    /// What the normalization passes noted, reported as discarded clauses.
    pub(crate) notes: Vec<String>,
    pub(crate) parser: tantivy::query::QueryParser,
    pub(crate) narrowed: Option<NarrowedDefaultFields>,
    held: Vec<HeldRange>,
}

impl PreparedQuery {
    /// Parse leniently, as `QueryParser::parse_query_lenient` does, with each held range put back
    /// where its placeholder parsed.
    pub(crate) fn parse(
        &self,
    ) -> (
        Box<dyn tantivy::query::Query>,
        Vec<tantivy::query::QueryParserError>,
    ) {
        if self.held.is_empty() {
            return self.parser.parse_query_lenient(&self.text);
        }
        let (ast, grammar_errors) = tantivy::query_grammar::parse_query_lenient(&self.text);
        let mut errors: Vec<tantivy::query::QueryParserError> = grammar_errors
            .into_iter()
            .map(|error| {
                tantivy::query::QueryParserError::SyntaxError(format!(
                    "{} at position {}",
                    error.message, error.pos
                ))
            })
            .collect();
        let (query, mut built_errors) = self
            .parser
            .build_query_from_user_input_ast_lenient(restore_ranges(ast, &self.held));
        errors.append(&mut built_errors);
        (query, errors)
    }

    /// The normalized query as it reads, held ranges written back.
    pub(crate) fn shown(&self) -> String {
        self.held.iter().fold(self.text.clone(), |text, held| {
            text.replacen(&held.placeholder, &held.written, 1)
        })
    }
}

/// A range clause taken out of the query text because a bound of it is quoted.
#[derive(Debug, Clone, PartialEq, Eq)]
struct HeldRange {
    /// The term standing in its place, under the clause's own field.
    placeholder: String,
    /// The range as the query wrote it, after the field's colon.
    written: String,
    lower: tantivy::query_grammar::UserInputBound,
    upper: tantivy::query_grammar::UserInputBound,
}

/// Take each range clause with a quoted bound out of `query`, leaving a placeholder term under
/// its field.
///
/// The grammar reads a range bound as a bare word: a quote is part of it, and a space ends it.
/// So `label:["Film & Animation" TO G]` cannot be written in the text at all, and
/// `label:>"S"` compares against the three characters `"S"` — on an untokenized field that
/// silently matched every value or none. The bound is a plain string once parsed, though, so the
/// range is held here and put back into the parsed query ([`PreparedQuery::parse`]).
fn hold_quoted_ranges(query: &str) -> (String, Vec<HeldRange>) {
    if !query.contains('"') {
        return (query.to_string(), Vec::new());
    }
    let mut held = Vec::new();
    let mut out = String::with_capacity(query.len());
    let mut copied = 0usize;
    for reference in field_references(query) {
        let start = reference.span.end + 1;
        if start <= copied || query.as_bytes().get(reference.span.end) != Some(&b':') {
            continue;
        }
        let Some((lower, upper, len)) = quoted_range_at(&query[start..]) else {
            continue;
        };
        let placeholder = format!("cameoheldrange{}", held.len());
        out.push_str(&query[copied..start]);
        out.push_str(&placeholder);
        copied = start + len;
        held.push(HeldRange {
            placeholder,
            written: query[start..start + len].to_string(),
            lower,
            upper,
        });
    }
    out.push_str(&query[copied..]);
    (out, held)
}

/// The range opening `text` — `[a TO b]`, `{a TO b}` or the mixed pairs, or `>a`, `>=a`, `<a`,
/// `<=a` — with its byte length, when a bound of it is quoted. A bare `*` is unbounded.
fn quoted_range_at(
    text: &str,
) -> Option<(
    tantivy::query_grammar::UserInputBound,
    tantivy::query_grammar::UserInputBound,
    usize,
)> {
    use tantivy::query_grammar::UserInputBound as Bound;
    let mut quoted = false;
    let mut bound = |rest: &str| -> Option<(Option<String>, usize)> {
        let (value, len, was_quoted) = range_bound(rest)?;
        quoted |= was_quoted;
        Some(((was_quoted || value != "*").then_some(value), len))
    };
    let (lower, upper, len) = if let Some(open) = text.strip_prefix(['[', '{']) {
        let inclusive_lower = text.starts_with('[');
        let skip = open.len() - open.trim_start().len();
        let (lower, lower_len) = bound(&open[skip..])?;
        let mut at = 1 + skip + lower_len;
        let rest = &text[at..];
        let gap = rest.len() - rest.trim_start().len();
        let rest = rest.trim_start().strip_prefix("TO")?;
        if !rest.starts_with(char::is_whitespace) {
            return None;
        }
        at += gap + 2;
        let rest = &text[at..];
        let gap = rest.len() - rest.trim_start().len();
        at += gap;
        let (upper, upper_len) = bound(&text[at..])?;
        at += upper_len;
        let rest = &text[at..];
        let gap = rest.len() - rest.trim_start().len();
        at += gap;
        let close = text[at..]
            .chars()
            .next()
            .filter(|c| matches!(c, ']' | '}'))?;
        at += 1;
        let lower = match lower {
            None => Bound::Unbounded,
            Some(v) if inclusive_lower => Bound::Inclusive(v),
            Some(v) => Bound::Exclusive(v),
        };
        let upper = match upper {
            None => Bound::Unbounded,
            Some(v) if close == ']' => Bound::Inclusive(v),
            Some(v) => Bound::Exclusive(v),
        };
        (lower, upper, at)
    } else {
        let (op, rest) = [">=", "<=", ">", "<"]
            .iter()
            .find_map(|op| text.strip_prefix(op).map(|rest| (*op, rest)))?;
        let gap = rest.len() - rest.trim_start().len();
        let (value, value_len) = bound(&rest[gap..])?;
        let value = value?;
        let len = op.len() + gap + value_len;
        match op {
            ">=" => (Bound::Inclusive(value), Bound::Unbounded, len),
            ">" => (Bound::Exclusive(value), Bound::Unbounded, len),
            "<=" => (Bound::Unbounded, Bound::Inclusive(value), len),
            _ => (Bound::Unbounded, Bound::Exclusive(value), len),
        }
    };
    quoted.then_some((lower, upper, len))
}

/// One range bound at the start of `text`: its value, its byte length, and whether it was
/// quoted. A quoted bound ends at its unescaped closing quote, escapes removed; a bare one ends
/// where the grammar's would, at whitespace or a bracket.
fn range_bound(text: &str) -> Option<(String, usize, bool)> {
    if let Some(inner) = text.strip_prefix('"') {
        let mut value = String::new();
        let mut chars = inner.char_indices();
        while let Some((at, ch)) = chars.next() {
            match ch {
                '\\' => value.push(chars.next()?.1),
                '"' => return Some((value, 1 + at + 1, true)),
                _ => value.push(ch),
            }
        }
        return None;
    }
    let len = text
        .find(|c: char| c.is_whitespace() || matches!(c, '[' | ']' | '{' | '}' | '"' | '(' | ')'))
        .unwrap_or(text.len());
    (len > 0).then(|| (text[..len].to_string(), len, false))
}

/// Put each held range back where its placeholder parsed: a term under the clause's field.
fn restore_ranges(
    ast: tantivy::query_grammar::UserInputAst,
    held: &[HeldRange],
) -> tantivy::query_grammar::UserInputAst {
    use tantivy::query_grammar::{UserInputAst, UserInputLeaf};
    match ast {
        UserInputAst::Clause(clauses) => UserInputAst::Clause(
            clauses
                .into_iter()
                .map(|(occur, clause)| (occur, restore_ranges(clause, held)))
                .collect(),
        ),
        UserInputAst::Boost(inner, boost) => {
            UserInputAst::Boost(Box::new(restore_ranges(*inner, held)), boost)
        }
        UserInputAst::Leaf(leaf) => match *leaf {
            UserInputLeaf::Literal(literal) => {
                match held.iter().find(|h| h.placeholder == literal.phrase) {
                    Some(range) => UserInputAst::Leaf(Box::new(UserInputLeaf::Range {
                        field: literal.field_name,
                        lower: range.lower.clone(),
                        upper: range.upper.clone(),
                    })),
                    None => UserInputAst::Leaf(Box::new(UserInputLeaf::Literal(literal))),
                }
            }
            other => UserInputAst::Leaf(Box::new(other)),
        },
    }
}

/// Whether any clause of `query` names no field, and so goes to the default fields.
///
/// Read from tantivy's own parse, so a group's field, a quoted phrase and a range are seen as the
/// parser sees them: `title:(a b)` is qualified throughout, `"a b"` and `[a TO b]` with no field
/// are not.
fn has_unqualified_clause(query: &str) -> bool {
    use tantivy::query_grammar::{UserInputAst, UserInputLeaf};

    fn walk(ast: &UserInputAst) -> bool {
        match ast {
            UserInputAst::Clause(clauses) => clauses.iter().any(|(_, child)| walk(child)),
            UserInputAst::Boost(child, _) => walk(child),
            UserInputAst::Leaf(leaf) => match leaf.as_ref() {
                UserInputLeaf::Literal(literal) => literal.field_name.is_none(),
                UserInputLeaf::Range { field, .. } | UserInputLeaf::Set { field, .. } => {
                    field.is_none()
                }
                _ => false,
            },
        }
    }

    walk(&tantivy::query_grammar::parse_query_lenient(query).0)
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

    // A range is the parser's: `id:>9000` names no key.
    if value_part.contains(QUERY_SYNTAX_IN_VALUE) || value_part.starts_with(['>', '<', '[', '{']) {
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
    if !schema.has_shadow_fields() {
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

/// Note for a prefix clause left as the bare term because it is shorter than the node expands.
pub(crate) fn short_prefix_note(
    field: Option<&str>,
    value: &str,
    min_prefix_length: usize,
) -> String {
    let written = match field {
        Some(field) => format!("{field}:{value}"),
        None => value.to_string(),
    };
    let unit = if min_prefix_length == 1 {
        "character"
    } else {
        "characters"
    };
    format!(
        "'{written}' was not expanded as a prefix: this node expands prefixes of at least \
         {min_prefix_length} {unit}, so it matched the term '{}' exactly; lengthen the prefix",
        value.trim_end_matches('*')
    )
}

/// Note for a `*` the grammar dropped without a word: a prefix that names no field, a leading or
/// inner wildcard, or a prefix inside a field group. Tantivy matches what is left as written.
pub(crate) fn ignored_wildcard_note(
    field: Option<&str>,
    phrase: &str,
    unqualified_expansion: bool,
) -> String {
    let written = match field {
        Some(field) => format!("{field}:{phrase}"),
        None => phrase.to_string(),
    };
    let literal = phrase.replace('*', " ");
    let literal = literal.split_whitespace().collect::<Vec<_>>().join(" ");
    let remedy = match field {
        _ if phrase.trim_end_matches('*').contains('*') => {
            "only a trailing '*' is supported, as field:prefix*".to_string()
        }
        None if unqualified_expansion => format!(
            "it could not be expanded across the default fields; name the field, as \
             field:{phrase}"
        ),
        None => format!(
            "name the field to search a prefix, as field:{phrase}, or enable \
             expand_unqualified_prefix to search the default fields"
        ),
        Some(field) => format!("write it as {field}:{phrase} outside any group"),
    };
    format!(
        "the '*' in '{written}' was ignored, so it matched '{literal}' exactly rather than as a \
         wildcard; {remedy}"
    )
}

/// Whether `analyzer` keeps a `*` in any token it makes from `phrase`.
fn analyzer_keeps_star(analyzer: &mut tantivy::tokenizer::TextAnalyzer, phrase: &str) -> bool {
    use tantivy::tokenizer::TokenStream;

    let mut kept = false;
    analyzer
        .token_stream(phrase)
        .process(&mut |token| kept |= token.text.contains('*'));
    kept
}

/// Every unquoted literal in `query` still carrying a `*` that tantivy will silently drop.
///
/// Read from the grammar's own parse rather than scanned from the text, so a group, a boost or a
/// quoted phrase is seen exactly as the parser will see it. A phrase in quotes is left alone —
/// `"big bad wo"*` is tantivy's phrase prefix and works — as is a literal on a field that is not
/// text, where the parser reports the value itself as an error and a note here would be a second
/// account of one clause. `already` holds the `(field, phrase)` pairs the rewrite has noted.
fn ignored_wildcards(
    query: &str,
    tantivy_index: &Index,
    already: &HashSet<(Option<String>, String)>,
    unqualified_expansion: bool,
) -> Vec<String> {
    use tantivy::query_grammar::{Delimiter, UserInputAst, UserInputLeaf};
    use tantivy::schema::FieldType;

    fn walk<'a>(ast: &'a UserInputAst, out: &mut Vec<(Option<&'a str>, &'a str)>) {
        match ast {
            UserInputAst::Clause(clauses) => clauses.iter().for_each(|(_, child)| walk(child, out)),
            UserInputAst::Boost(child, _) => walk(child, out),
            UserInputAst::Leaf(leaf) => {
                if let UserInputLeaf::Literal(literal) = leaf.as_ref()
                    && literal.delimiter == Delimiter::None
                    && literal.phrase.contains('*')
                {
                    out.push((literal.field_name.as_deref(), literal.phrase.as_str()));
                }
            }
        }
    }

    let (ast, _) = tantivy::query_grammar::parse_query_lenient(query);
    let mut found = Vec::new();
    walk(&ast, &mut found);

    let tantivy_schema = tantivy_index.schema();
    let mut notes = Vec::new();
    for (field, phrase) in found {
        if already.contains(&(field.map(str::to_string), phrase.to_string())) {
            continue;
        }
        let dropped = match field {
            // Unqualified terms reach the text default fields, whose analyzers drop a `*`.
            None => true,
            Some(name) => tantivy_schema
                .find_field(name)
                .and_then(|(field, _)| {
                    let indexing = match tantivy_schema.get_field_entry(field).field_type() {
                        FieldType::Str(options) => options.get_indexing_options(),
                        FieldType::JsonObject(options) => options.get_text_indexing_options(),
                        _ => None,
                    }?;
                    tantivy_index.tokenizers().get(indexing.tokenizer())
                })
                // A `raw` field keeps the `*` inside its one term, so `id:a*b` matches the term
                // `a*b` exactly as written and there is nothing to report.
                .is_some_and(|mut analyzer| !analyzer_keeps_star(&mut analyzer, phrase)),
        };
        if dropped {
            let note = ignored_wildcard_note(field, phrase, unqualified_expansion);
            if !notes.contains(&note) {
                notes.push(note);
            }
        }
    }
    notes
}

/// Rewrite a single-term prefix — `field:pre*` — into the equivalent lexicographic range, on text
/// and string fields.
///
/// The grammar has no prefix operator: it drops the `*` and matches `pre` as a whole term without
/// raising an error. Tantivy tokenizes the bounds, so the prefix may be written in any case.
///
/// **A prefix shorter than `min_prefix_length` characters is not expanded** (`0` expands any).
/// The range this builds walks every term it covers and reads each one's postings, with no ceiling
/// — tantivy caps its own phrase prefix at 50 terms per segment and puts no cap on a range. Measured
/// on 10M single-term documents per shard (ROADMAP M8), one hex character covered 625k terms and
/// cost 165 ms; two cost 11 ms and three under 1 ms. So a short prefix is left as written and
/// matches the term exactly, with a note, rather than refused: the same treatment an unrewritable
/// prefix already gets, and a query with one short clause still runs the rest.
///
/// **Every other `*` the grammar would drop silently is noted too** — a prefix that names no
/// field, a leading or inner wildcard, a prefix inside a field group. Tantivy matches the text
/// with the `*` removed and raises no error, so without a note the caller reads a near-empty result
/// as the answer to a wildcard search they never actually ran.
///
/// Returns the query with a note per prefix clause left unrewritten.
pub(crate) fn normalize_prefix_query(
    query: &str,
    tantivy_index: &Index,
    policy: &QueryPolicy,
    default_fields: &[Field],
) -> (String, Vec<String>) {
    let min_prefix_length = policy.min_prefix_length;
    use tantivy::schema::FieldType;

    if !query.contains('*') {
        return (query.to_string(), Vec::new());
    }

    let tantivy_schema = tantivy_index.schema();
    let mut normalized = query.to_string();
    let mut notes = Vec::new();
    // What the loop below has already accounted for, so the wildcard pass does not note it twice.
    let mut noted: HashSet<(Option<String>, String)> = HashSet::new();

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

            enum Prefix {
                Range(String),
                TooShort(String),
                Unrewritable,
            }
            let prefix_clause = single_term_prefix(value).map(|(term, boost)| {
                noted.insert((Some(name.to_string()), format!("{term}*")));
                match single_token(&mut analyzer, term) {
                    // Counted after analysis, in characters: what the range walks is terms.
                    Some(lower)
                        if min_prefix_length > 0 && lower.chars().count() < min_prefix_length =>
                    {
                        Prefix::TooShort(format!("{term}*"))
                    }
                    Some(lower) => match prefix_upper_bound(&lower, &mut analyzer) {
                        Some(upper) => {
                            Prefix::Range(format!("{name}:[{lower} TO {upper}}}{boost}"))
                        }
                        None => Prefix::Unrewritable,
                    },
                    None => Prefix::Unrewritable,
                }
            });

            match prefix_clause {
                Some(Prefix::Range(rewritten)) => out.push_str(&rewritten),
                Some(Prefix::TooShort(prefix)) => {
                    notes.push(short_prefix_note(
                        Some(name),
                        &prefix,
                        policy.min_prefix_length,
                    ));
                    out.push_str(&normalized[start..value_end]);
                }
                Some(Prefix::Unrewritable) => {
                    notes.push(unrewritable_prefix_note(name, value));
                    out.push_str(&normalized[start..value_end]);
                }
                None => out.push_str(&normalized[start..value_end]),
            }
            idx = value_end;
        }
        out.push_str(&normalized[idx..]);
        normalized = out;
    }

    if policy.expand_unqualified_prefix {
        normalized = expand_unqualified_prefixes(
            &normalized,
            tantivy_index,
            default_fields,
            min_prefix_length,
            &mut notes,
            &mut noted,
        );
    }

    notes.extend(ignored_wildcards(
        &normalized,
        tantivy_index,
        &noted,
        policy.expand_unqualified_prefix,
    ));
    (normalized, notes)
}

/// A bare prefix found in the query text: `pre*`, with its byte span and any boost.
#[derive(Debug)]
struct BarePrefix<'a> {
    /// Covers the term, its `*` and its boost — everything the rewrite replaces. A leading `+`
    /// or `-` is outside it and stays where it is.
    start: usize,
    end: usize,
    term: &'a str,
    /// `^2`, or empty.
    boost: &'a str,
}

/// The unquoted, unqualified single-term prefixes in `query`, in order.
///
/// A prefix is *qualified* by a `field:` in front of it or by an enclosing `field:( … )` group,
/// and both are skipped: the first is the field-qualified rewrite's, the second belongs to the
/// group's field and must not be sent to the default fields. Quotes, range and set brackets, and
/// anything escaped are skipped too. This is a scan of the text, which is what the rewrite needs
/// to know *where* to write; [`expand_unqualified_prefixes`] checks it against the grammar's own
/// parse before trusting it.
fn bare_prefixes(query: &str) -> Vec<BarePrefix<'_>> {
    const STOPS: &[char] = &['(', ')', '[', ']', '{', '}', '"', '\''];

    let mut found = Vec::new();
    // One entry per open parenthesis: whether it opened a `field:( … )` group.
    let mut groups: Vec<bool> = Vec::new();
    let mut brackets = 0usize;
    let mut chars = query.char_indices().peekable();

    while let Some(&(at, ch)) = chars.peek() {
        match ch {
            '"' | '\'' => {
                chars.next();
                let mut escaped = false;
                for (_, inner) in chars.by_ref() {
                    match inner {
                        _ if escaped => escaped = false,
                        '\\' => escaped = true,
                        _ if inner == ch => break,
                        _ => {}
                    }
                }
            }
            '(' => {
                groups.push(false);
                chars.next();
            }
            ')' => {
                groups.pop();
                chars.next();
            }
            '[' | '{' => {
                brackets += 1;
                chars.next();
            }
            ']' | '}' => {
                brackets = brackets.saturating_sub(1);
                chars.next();
            }
            _ if ch.is_whitespace() => {
                chars.next();
            }
            _ => {
                let mut end = at;
                while let Some(&(pos, next)) = chars.peek() {
                    if next.is_whitespace() || STOPS.contains(&next) {
                        break;
                    }
                    end = pos + next.len_utf8();
                    chars.next();
                }
                let token = &query[at..end];

                // `field:(` opens a group whose field every literal inside it belongs to.
                if token.ends_with(':') && query[end..].starts_with('(') {
                    groups.push(true);
                    chars.next();
                    continue;
                }
                if brackets > 0 || groups.contains(&true) {
                    continue;
                }

                let sign = usize::from(token.starts_with(['+', '-']));
                let body = &token[sign..];
                let (core, boost) = match body.find('^') {
                    Some(caret) => body.split_at(caret),
                    None => (body, ""),
                };
                let Some(term) = core.strip_suffix('*') else {
                    continue;
                };
                if term.is_empty()
                    || term.contains(['*', ':', '\\'])
                    || term.starts_with(['!', '~', '^'])
                {
                    continue;
                }
                found.push(BarePrefix {
                    start: at + sign,
                    end,
                    term,
                    boost,
                });
            }
        }
    }
    found
}

/// The unqualified prefixes tantivy's grammar sees in `query`, as the phrases it holds them by.
fn grammar_bare_prefixes(query: &str) -> Vec<String> {
    use tantivy::query_grammar::{Delimiter, UserInputAst, UserInputLeaf};

    fn walk(ast: &UserInputAst, out: &mut Vec<String>) {
        match ast {
            UserInputAst::Clause(clauses) => clauses.iter().for_each(|(_, child)| walk(child, out)),
            UserInputAst::Boost(child, _) => walk(child, out),
            UserInputAst::Leaf(leaf) => {
                if let UserInputLeaf::Literal(literal) = leaf.as_ref()
                    && literal.field_name.is_none()
                    && literal.delimiter == Delimiter::None
                    && literal.phrase.len() > 1
                    && literal.phrase.ends_with('*')
                    && literal.phrase.matches('*').count() == 1
                {
                    out.push(literal.phrase.clone());
                }
            }
        }
    }

    let (ast, _) = tantivy::query_grammar::parse_query_lenient(query);
    let mut found = Vec::new();
    walk(&ast, &mut found);
    found
}

/// Rewrite each bare `pre*` into one prefix range per text default field, OR'd.
///
/// Tantivy's grammar has no unqualified prefix — it drops the `*` and matches `pre` as a term — so
/// this is the only way `pre*` can mean what it looks like. It sends the prefix exactly where an
/// unqualified term goes: the default fields, less those that cannot hold a range (a JSON field
/// supports one only as a fast column). `min_prefix_length` applies per field, after that field's
/// analyzer, as it does to a qualified prefix.
///
/// **Trusted only when the text scan agrees with the grammar.** [`bare_prefixes`] finds where to
/// write; the grammar decides what the query means. If they disagree on which bare prefixes exist
/// — an escape or nesting the scan reads differently — nothing is rewritten, and each prefix is
/// reported by the ignored-wildcard pass instead. A wrong rewrite would change what a query
/// matches silently; declining changes nothing and says so.
fn expand_unqualified_prefixes(
    query: &str,
    tantivy_index: &Index,
    default_fields: &[Field],
    min_prefix_length: usize,
    notes: &mut Vec<String>,
    noted: &mut HashSet<(Option<String>, String)>,
) -> String {
    use tantivy::schema::FieldType;

    let spans = bare_prefixes(query);
    if spans.is_empty() {
        return query.to_string();
    }
    let mut scanned: Vec<String> = spans.iter().map(|span| format!("{}*", span.term)).collect();
    let mut parsed = grammar_bare_prefixes(query);
    scanned.sort();
    parsed.sort();
    if scanned != parsed {
        tracing::debug!(
            query = %query,
            ?scanned,
            ?parsed,
            "Unqualified prefixes left unexpanded: the text scan and the grammar disagree"
        );
        return query.to_string();
    }

    // Sorted by name, so one query always expands to the same text.
    let tantivy_schema = tantivy_index.schema();
    let mut targets: Vec<(String, tantivy::tokenizer::TextAnalyzer)> = default_fields
        .iter()
        .filter_map(|&field| {
            let entry = tantivy_schema.get_field_entry(field);
            let FieldType::Str(options) = entry.field_type() else {
                return None;
            };
            let indexing = options.get_indexing_options()?;
            let analyzer = tantivy_index.tokenizers().get(indexing.tokenizer())?;
            Some((entry.name().to_string(), analyzer))
        })
        .collect();
    targets.sort_by(|a, b| a.0.cmp(&b.0));

    let mut out = String::with_capacity(query.len() * 2);
    let mut cursor = 0;
    for span in spans {
        out.push_str(&query[cursor..span.start]);
        cursor = span.end;

        let mut clauses = Vec::new();
        let mut too_short = false;
        for (name, analyzer) in targets.iter_mut() {
            let Some(lower) = single_token(analyzer, span.term) else {
                continue;
            };
            if min_prefix_length > 0 && lower.chars().count() < min_prefix_length {
                too_short = true;
                continue;
            }
            if let Some(upper) = prefix_upper_bound(&lower, analyzer) {
                clauses.push(format!("{name}:[{lower} TO {upper}}}"));
            }
        }

        if clauses.is_empty() {
            // Left as written. A short prefix is reported here; anything else falls to the
            // ignored-wildcard pass, which says it could not be expanded.
            if too_short {
                let prefix = format!("{}*", span.term);
                notes.push(short_prefix_note(None, &prefix, min_prefix_length));
                noted.insert((None, prefix));
            }
            out.push_str(&query[span.start..span.end]);
        } else {
            out.push('(');
            out.push_str(&clauses.join(" OR "));
            out.push(')');
            out.push_str(span.boost);
        }
    }
    out.push_str(&query[cursor..]);
    out
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

#[cfg(test)]
mod held_range_tests {
    use super::*;
    use tantivy::query_grammar::UserInputBound as Bound;

    /// Only a range with a quoted bound is held, under its own field; the text reads back as
    /// written.
    #[test]
    fn a_range_with_a_quoted_bound_is_held_and_reads_back_as_written() {
        let query = r#"name:rust AND label:["Film & Animation" TO M} -tag:>="a \"b\"" id:[1 TO 2]"#;
        let (text, held) = hold_quoted_ranges(query);
        assert_eq!(
            text,
            "name:rust AND label:cameoheldrange0 -tag:cameoheldrange1 id:[1 TO 2]"
        );
        assert_eq!(
            (held[0].lower.clone(), held[0].upper.clone()),
            (
                Bound::Inclusive("Film & Animation".to_string()),
                Bound::Exclusive("M".to_string())
            )
        );
        assert_eq!(
            (held[1].lower.clone(), held[1].upper.clone()),
            (Bound::Inclusive(r#"a "b""#.to_string()), Bound::Unbounded)
        );
        let prepared_text = held
            .iter()
            .fold(text, |text, h| text.replacen(&h.placeholder, &h.written, 1));
        assert_eq!(prepared_text, query);
    }

    /// What is not a whole range is left to the parser as it was.
    #[test]
    fn what_is_not_a_quoted_range_is_left_alone() {
        for query in [
            r#"title:"a phrase""#,
            r#"label:["unterminated TO b]"#,
            r#"label:[a TO b] "free text""#,
            r#"label:>"unterminated"#,
        ] {
            let (text, held) = hold_quoted_ranges(query);
            assert_eq!((text.as_str(), held.len()), (query, 0), "{query}");
        }
    }

    /// A whole-query range on the id is the parser's, not a lookup of a key that begins `>`.
    #[test]
    fn a_range_on_the_id_is_not_a_key_lookup() {
        let schema = IndexSchema::default();
        for query in ["id:>9000", "id:<=9000", "id:[1 TO 2]", "id:{9000 TO *}"] {
            assert_eq!(parse_exact_id_query(query, &schema), None, "{query}");
        }
        assert!(parse_exact_id_query("id:9000", &schema).is_some());
    }
}

#[cfg(test)]
mod bare_prefix_tests {
    use super::*;

    fn terms(query: &str) -> Vec<&str> {
        bare_prefixes(query).iter().map(|span| span.term).collect()
    }

    /// Bare prefixes in every position a clause can take, with signs and boosts outside the term.
    #[test]
    fn the_scan_finds_bare_prefixes_wherever_a_clause_can_sit() {
        assert_eq!(terms("qui*"), ["qui"]);
        assert_eq!(terms("a qui* b"), ["qui"]);
        assert_eq!(terms("(qui* OR zeb*)"), ["qui", "zeb"]);
        assert_eq!(terms("+qui* -zeb*"), ["qui", "zeb"]);

        let spans = bare_prefixes("x -qui*^2 y");
        assert_eq!(spans.len(), 1);
        assert_eq!(&"x -qui*^2 y"[spans[0].start..spans[0].end], "qui*^2");
        assert_eq!(spans[0].boost, "^2");
    }

    /// Everything that is not an unqualified single-term prefix is left alone.
    #[test]
    fn the_scan_skips_qualified_quoted_bracketed_and_wildcard_forms() {
        for query in [
            "title:qui*",             // qualified
            "title:(qui* zeb*)",      // inside a field group
            "title:(a OR (qui*))",    // nested inside a field group
            "\"big bad wo\"*",        // phrase prefix
            "\"qui* inside quotes\"", // quoted
            "title:[a* TO b]",        // range bound
            "*",                      // match all
            "*uick",                  // leading wildcard
            "q*ck",                   // inner wildcard
            "qu\\*i*",                // escaped
            "title:*",                // presence
        ] {
            assert!(terms(query).is_empty(), "{query:?} -> {:?}", terms(query));
        }
        // A field group closes, and what follows it is bare again.
        assert_eq!(terms("title:(a b) qui*"), ["qui"]);
    }

    /// The scan and the grammar agree on the forms the rewrite is used for — which is what licenses
    /// the rewrite — and where they would not, the rewrite declines.
    #[test]
    fn the_scan_agrees_with_the_grammar() {
        for query in [
            "qui*",
            "(qui* OR zeb*) title:x",
            "+qui* -zeb*^2",
            "title:(a qui*) zeb*",
            "\"big bad wo\"* qui*",
        ] {
            let mut scanned: Vec<String> =
                terms(query).iter().map(|term| format!("{term}*")).collect();
            let mut parsed = grammar_bare_prefixes(query);
            scanned.sort();
            parsed.sort();
            assert_eq!(scanned, parsed, "{query:?}");
        }
    }
}

#[cfg(test)]
mod query_depth_tests {
    use super::*;

    /// Groups nest by their parentheses, a `field:( ... )` group like any other.
    #[test]
    fn a_group_is_a_level_until_it_closes() {
        assert_eq!(query_depth("a b"), 0);
        assert_eq!(query_depth("(a) (b)"), 1);
        assert_eq!(query_depth("((a) OR title:(b c))"), 2);
    }

    /// A `NOT` lasts as long as its leaf: a term ends it, a group carries it to its `)`.
    #[test]
    fn a_not_is_a_level_until_its_leaf_ends() {
        assert_eq!(query_depth("a AND NOT b AND NOT c"), 1);
        assert_eq!(query_depth("NOT NOT a"), 2);
        assert_eq!(query_depth("NOT (NOT (a))"), 4);
        assert_eq!(query_depth("x -NOT title:(a)"), 2);
    }

    /// Inside a phrase, a range or a set, brackets and `NOT` are text.
    #[test]
    fn quoted_and_bracketed_text_does_not_nest() {
        assert_eq!(query_depth("title:\"((NOT a\""), 0);
        assert_eq!(query_depth("f:[( TO (] g: IN [NOT (]"), 0);
        assert_eq!(query_depth("a\\(b"), 0);
    }
}

#[cfg(test)]
mod not_clause_tests {
    use super::*;

    /// A negated group's own `NOT`s rewrite in place, closed or not, each level getting its
    /// `)` where its group ends.
    #[test]
    fn nested_negated_groups_rewrite_in_place() {
        assert_eq!(
            normalize_not_clauses("NOT (NOT (NOT (a)))"),
            "(* -((* -((* -(a))))))"
        );
        assert_eq!(normalize_not_clauses("NOT (NOT (a"), "(* -((* -(a))");
        assert_eq!(
            normalize_not_clauses("NOT (a OR (b AND NOT c)) d"),
            "(* -(a OR (b AND -c))) d"
        );
    }

    /// A run of `NOT`s whose leaf is refused is left as written, and scanning resumes after the
    /// run rather than recounting it from each `NOT` in it.
    #[test]
    fn a_refused_not_run_is_left_as_written() {
        for query in ["NOT NOT NOT", "NOT NOT NOT -x", "a AND NOT NOT AND b"] {
            assert_eq!(normalize_not_clauses(query), query);
        }
        assert_eq!(
            normalize_not_clauses("NOT NOT -x AND NOT b"),
            "NOT NOT -x AND -b"
        );
    }

    /// A trailing `\` escapes past the end of the query, which the leaf scan once indexed
    /// beyond. The query is malformed whatever the rewrite makes of it; what matters is that it
    /// reaches the parser to be reported rather than panicking the search.
    #[test]
    fn a_trailing_escape_ends_the_leaf_at_the_query_end() {
        assert!(normalize_not_clauses("NOT a\\").starts_with("(* -a\\"));
        assert_eq!(normalize_not_clauses("x AND NOT a\\"), "x AND -a\\");
    }

    /// A group negated by a rewrite keeps the scope it sits in: inside `field:( ... )` its own
    /// `NOT` cannot take the `(* -x)` form, and is left for the parser to report.
    #[test]
    fn a_negated_group_keeps_its_field_scope() {
        assert_eq!(
            normalize_not_clauses("title:(x AND NOT (a OR NOT b))"),
            "title:(x AND -(a OR NOT b))"
        );
        assert_eq!(
            normalize_not_clauses("x AND NOT (a OR NOT (c AND NOT d))"),
            "x AND -(a OR (* -(c AND -d)))"
        );
    }
}
