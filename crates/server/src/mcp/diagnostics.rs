//! Telling a caller why a result is not what it expected.
//!
//! Pure functions over a query string, a JSON response and a field list. Nothing here reaches the
//! engine, which is what keeps every test in this module a unit test.

use std::cmp::Ordering;
use std::collections::HashSet;

use serde_json::Value as JsonValue;

use cameodb_mcp::ToolError;

use crate::mcp::schema::{FieldInfo, field_query_hint};
use crate::node::{OrchestratorError, RemoteVerdict};
use crate::query::parse_query_keywords;

/// Turn a routing error into what the tool answers, classified the way HTTP classifies.
///
/// `verdict` is the node's own account of whose fault a failure is, decided where the error
/// was raised rather than guessed from its text here. The mapping is the HTTP surface's
/// masking boundary: a `BadRequest` or `NotFound` message was written for the caller and
/// passes through, while anything the node answers `5xx` for is masked — including
/// `Unavailable`, whose text names the topology the caller cannot act on (which shard is
/// absent, which peer did not answer). The detail is not lost: [`ToolError::detail`] is what
/// the audit record and the log receive.
pub(super) fn tool_error(err: OrchestratorError) -> ToolError {
    match err.verdict() {
        RemoteVerdict::NotFound => ToolError::caller(err.caller_message()),
        RemoteVerdict::BadRequest | RemoteVerdict::QuotaExceeded => {
            ToolError::caller(err.to_string())
        }
        RemoteVerdict::Unavailable
        | RemoteVerdict::SchemaRequired
        | RemoteVerdict::Minting
        | RemoteVerdict::ServerFault => ToolError::internal(err.to_string()),
    }
}

/// Why a search that matched nothing may have asked for less than it meant to, or `None` when
/// the query gives no reason to think so.
///
/// Names only what the query contains, and only the constructs that narrow. Terms are not among
/// them: they are ORed, so bare terms that returned nothing matched none of them and there is no
/// narrowing to undo. Advice about a phrase the caller did not write, or a boolean it did not
/// use, reads as a finding about the data — worse than silence, since zero hits is usually the
/// true answer.
///
/// Inline modifiers are stripped first, so `limit` and `sort` are not read as query terms, and
/// `AND` counts only as a standalone token, the way the parser reads it — so it is not found
/// inside a value such as `status:ANDROID`.
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

/// A value's spellings in the order a retry tries them: as given, lowercase, first letter
/// capitalised, each word capitalised, uppercase — each once.
pub(super) fn spellings(value: &str) -> Vec<String> {
    fn capitalise(word: &str) -> String {
        let mut done = false;
        word.chars()
            .flat_map(|ch| {
                if !done && ch.is_alphabetic() {
                    done = true;
                    ch.to_uppercase().collect::<Vec<_>>()
                } else {
                    vec![ch]
                }
            })
            .collect()
    }
    let lower = value.to_lowercase();
    let first = capitalise(&lower);
    let each = lower
        .split(' ')
        .map(capitalise)
        .collect::<Vec<_>>()
        .join(" ");
    let mut out: Vec<String> = Vec::new();
    for spelling in [value.to_string(), lower, first, each, value.to_uppercase()] {
        if !out.contains(&spelling) {
            out.push(spelling);
        }
    }
    out
}

/// A clause on a field matched as a whole value as written: `field:value`, `field:"a value"`
/// or `field: IN [a "b c"]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct WholeValueClause {
    pub field: String,
    /// The clause exactly as the query writes it: what is counted alone, and what the general
    /// advice reads without its quotes.
    pub text: String,
    /// The value, or the values of the set.
    pub values: Vec<String>,
    pub set: bool,
}

/// The values of an `IN [...]` set as written, quoted or bare, and the byte length of the set
/// from its `[` to its `]`; `None` when the set does not close.
fn set_values(set: &str) -> Option<(Vec<String>, usize)> {
    let mut values = Vec::new();
    let mut chars = set.char_indices().skip(1).peekable();
    while let Some((at, ch)) = chars.next() {
        match ch {
            ']' => return Some((values, at + 1)),
            '"' => {
                let mut value = String::new();
                loop {
                    match chars.next()? {
                        (_, '"') => break,
                        (_, '\\') => value.push(chars.next()?.1),
                        (_, other) => value.push(other),
                    }
                }
                values.push(value);
            }
            _ if ch.is_whitespace() => {}
            _ => {
                let mut value = ch.to_string();
                while let Some(&(_, next)) = chars.peek() {
                    if next.is_whitespace() || next == ']' {
                        break;
                    }
                    value.push(next);
                    chars.next();
                }
                values.push(value);
            }
        }
    }
    None
}

/// The clauses of a query that name a field matched as a whole value as written — a `string`
/// field, or a `text` one under `raw` — with a plain value or an `IN` set: the ones a value in
/// another case, or written another way, leaves empty. A range, a prefix and a boost are left
/// out — each says something about the value a retry would have to keep.
pub(super) fn string_value_clauses(query: &str, fields: &[FieldInfo]) -> Vec<WholeValueClause> {
    let mut found: Vec<WholeValueClause> = Vec::new();
    for reference in storage::field_references(query) {
        if !fields
            .iter()
            .any(|f| f.name == reference.name && f.matches_whole_value_as_written())
        {
            continue;
        }
        let after_name = reference.span.end;
        let Some(rest) = query[after_name..].strip_prefix(':') else {
            continue;
        };
        let (values, len, set) = if let Some(set) = rest
            .trim_start()
            .strip_prefix("IN")
            .map(str::trim_start)
            .filter(|set| set.starts_with('['))
        {
            let Some((values, set_len)) = set_values(set) else {
                continue;
            };
            let offset = rest.len() - set.len();
            (values, offset + set_len, true)
        } else if let Some(quoted) = rest.strip_prefix('"') {
            let Some(end) = quoted.find('"') else {
                continue;
            };
            (vec![quoted[..end].to_string()], end + 2, false)
        } else {
            let end = rest
                .find(|ch: char| ch.is_whitespace() || ch == ')')
                .unwrap_or(rest.len());
            (vec![rest[..end].to_string()], end, false)
        };
        if values.is_empty()
            || values.iter().any(|value| {
                value.is_empty()
                    || (!set
                        && (value.contains(['*', '^', '\\'])
                            || value.starts_with(['[', '{', '<', '>', '(', '='])))
            })
        {
            continue;
        }
        let clause = WholeValueClause {
            field: reference.name.to_string(),
            text: query[reference.span.start..after_name + 1 + len].to_string(),
            values,
            set,
        };
        if !found.iter().any(|seen| seen.text == clause.text) {
            found.push(clause);
        }
    }
    found
}

const READ_THE_VALUES: &str = "Read how the values are written: in the field's description, or \
                               from a few hits with `limit 5` and the field in `fields`.";

/// What to try when a whole-value clause matched nothing on its own, spelled out for that
/// clause: for a value, the one retry over its spellings; for a set, or a value with no other
/// spelling, where the values are written.
///
/// A `string` field matches its whole value in the case the data writes it, so
/// `authors:"austen, jane"` finds nothing where the data holds `Austen, Jane` — and the general
/// zero-result advice (drop an `AND`, unquote a phrase) sends the caller the wrong way. The
/// caller has checked the clause alone matches nothing, so the advice never blames a clause that
/// was right while another one emptied the result.
pub(super) fn string_case_advice(clause: &WholeValueClause) -> String {
    let WholeValueClause {
        field,
        text,
        values,
        set,
    } = clause;
    let opening = format!(
        "`{text}` alone matches nothing. '{field}' matches only a whole value, as the data \
         writes it"
    );
    if *set {
        // The retry already ran, or the caller wrote the set: no spelling is left to try.
        return format!(
            "{opening} — case, diacritics and punctuation included — and no value of this set is \
             written so. {READ_THE_VALUES}"
        );
    }
    let value = &values[0];
    let spellings = spellings(value);
    // A value with no letters has one spelling, so a retry over its spellings would send the
    // same clause again: what is left is a part of a value, or a value written another way.
    if spellings.len() < 2 {
        return format!(
            "{opening}, so a part of one, or one written another way, finds nothing. \
             {READ_THE_VALUES}"
        );
    }
    let set = spellings
        .iter()
        .map(|spelling| format!("\"{spelling}\""))
        .collect::<Vec<_>>()
        .join(" ");
    format!(
        "{opening}, so a value in another case, or a part of one, finds nothing. If \"{value}\" \
         came from the question, retry once with its spellings in one set — `{field}: IN [{set}]` \
         — and if that finds nothing too, read how the values are written: in the field's \
         description, or from a few hits with `limit 5` and the field in `fields`."
    )
}

/// The advice for every whole-value clause that matched nothing on its own: one clause gets
/// [`string_case_advice`]; several share one explanation, then each its retry, so an agent that
/// wrote three values in the wrong case reads why once and fixes all three in one round.
pub(super) fn whole_value_advice(clauses: &[&WholeValueClause]) -> Option<String> {
    match clauses {
        [] => None,
        [clause] => Some(string_case_advice(clause)),
        _ => {
            let named = clauses
                .iter()
                .map(|clause| format!("`{}`", clause.text))
                .collect::<Vec<_>>()
                .join(", ");
            let mut retries = Vec::new();
            let mut spent = Vec::new();
            for clause in clauses {
                let spellings = spellings(&clause.values[0]);
                if clause.set || spellings.len() < 2 {
                    spent.push(format!("`{}`", clause.text));
                } else {
                    let set = spellings
                        .iter()
                        .map(|spelling| format!("\"{spelling}\""))
                        .collect::<Vec<_>>()
                        .join(" ");
                    retries.push(format!("`{}: IN [{set}]`", clause.field));
                }
            }
            let mut advice = format!(
                "Each of {named} alone matches nothing. These fields match only a whole value, \
                 as the data writes it — case, diacritics and punctuation included — so a value \
                 in another case, or a part of one, finds nothing."
            );
            if !retries.is_empty() {
                advice.push_str(&format!(
                    " If the values came from the question, retry once with each one's spellings \
                     in one set: {}.",
                    retries.join(", ")
                ));
            }
            if !spent.is_empty() {
                advice.push_str(&format!(
                    " No other spelling is left to try for {}.",
                    spent.join(", ")
                ));
            }
            advice.push_str(
                " Where that finds nothing, read how the values are written: in each field's \
                 description, or from a few hits with `limit 5` and the fields in `fields`.",
            );
            Some(advice)
        }
    }
}

/// The query as the general zero-result advice should read it: each whole-value clause
/// replaced by `field:value`, since its quotes enclose a value rather than a phrase, and advice
/// about phrases would point at the wrong thing.
pub(super) fn without_string_value_quotes(query: &str, clauses: &[WholeValueClause]) -> String {
    let mut out = query.to_string();
    for clause in clauses {
        out = out.replace(&clause.text, &format!("{}:value", clause.field));
    }
    out
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
///
/// Attached whenever the engine reports [`crate::node::APPROXIMATE_SORT_FIELD`],
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

/// What a narrowed set of default fields means for the caller holding the hits.
///
/// Attached whenever the engine reports [`crate::node::NARROWED_DEFAULT_FIELDS`]. Not a refusal:
/// nothing in the query was dropped, it ran against the fields the node's policy gives an
/// unqualified term. But an agent that searched a bare word and got nothing back would otherwise
/// read that as "the index holds nothing about it", when the word may sit in a field the term
/// never reached — and the remedy, naming the field, is one the agent can take itself.
pub(super) fn narrowed_default_fields_note(
    index: Option<&str>,
    narrowed: &storage::NarrowedDefaultFields,
) -> String {
    let on = index
        .map(|name| format!(" on '{name}'"))
        .unwrap_or_default();
    let why = if narrowed.declared {
        "the index's declared default_fields are more than this node's max_default_fields"
    } else {
        "the index has more text fields than this node's max_default_fields, and declares no \
         default_fields to choose among them, so the first by name were taken"
    };
    format!(
        "Terms with no field in front of them searched {} of {} default fields{on} ({}): {why}. \
         A word found only in another field was not looked for there — name that field \
         (`field:word`) to search it. `describe_index` marks each field's `default_search`.",
        narrowed.searched.len(),
        narrowed.available,
        narrowed.searched.join(", "),
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

/// How many fields a correction may name: enough to cover a family of related names
/// without becoming a dump of the schema — on a wide index the dump is the failure
/// mode this replaces.
const MAX_FIELD_SUGGESTIONS: usize = 10;

/// The similarity a candidate must clear to be offered. Below it the matches are
/// coincidental — a handful of shared trigrams like `_re` across `*_record_timestamp`
/// fields — and offering them reads as a finding rather than a guess.
const MIN_SUGGESTION_SCORE: f64 = 0.25;

/// A field name split into the words a caller compares it by.
///
/// Separators and camelCase boundaries both split, and everything is lowercased, so
/// `sandbox.threatLabels`, `sandbox_threat_labels` and `sandbox-threat-labels` are the
/// same three tokens.
fn name_tokens(name: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    for ch in name.chars() {
        if matches!(ch, '_' | '.' | '-' | ' ' | ':' | '/' | '\\') {
            if !current.is_empty() {
                tokens.push(std::mem::take(&mut current));
            }
        } else if ch.is_uppercase() && !current.is_empty() {
            tokens.push(std::mem::take(&mut current));
            current.push(ch.to_ascii_lowercase());
        } else {
            current.extend(ch.to_lowercase());
        }
    }
    if !current.is_empty() {
        tokens.push(current);
    }
    tokens
}

/// Two tokens are the same word when they are equal or one is a prefix of the other —
/// `name`/`names`, `sha`/`sha256` — with a floor on length so `a` does not match
/// everything starting with it.
fn tokens_match(a: &str, b: &str) -> bool {
    a == b || (a.len() >= 3 && b.len() >= 3 && (a.starts_with(b) || b.starts_with(a)))
}

/// The F1 over the input's tokens against a candidate's: how much of what the caller
/// wrote survives in this field's name, balanced against how much extra the field
/// carries. Catches reordered and partial guesses — `compile_date` against
/// `date_binary_compiled_on` — that no whole-string comparison sees.
fn token_f1(input_tokens: &[String], candidate_tokens: &[String]) -> f64 {
    if input_tokens.is_empty() || candidate_tokens.is_empty() {
        return 0.0;
    }
    let matched = input_tokens
        .iter()
        .filter(|token| candidate_tokens.iter().any(|c| tokens_match(token, c)))
        .count() as f64;
    if matched == 0.0 {
        return 0.0;
    }
    let precision = matched / input_tokens.len() as f64;
    let recall = matched / candidate_tokens.len() as f64;
    2.0 * precision * recall / (precision + recall)
}

/// Character-trigram Dice over the whole name, padded so the ends participate.
///
/// Catches what tokens cannot: transposition typos (`titel` → `title`), names written
/// fused (`threatlabel` → `*_threat_label`) and contained names (`hash` → `fuzzyhash`).
fn trigram_dice(input: &str, candidate: &str) -> f64 {
    let grams = |name: &str| -> HashSet<String> {
        let chars: Vec<char> = format!("_{}_", name.to_lowercase()).chars().collect();
        (0..chars.len().saturating_sub(2))
            .map(|i| chars[i..i + 3].iter().collect())
            .collect()
    };
    let (a, b) = (grams(input), grams(candidate));
    if a.is_empty() || b.is_empty() {
        return 0.0;
    }
    2.0 * a.intersection(&b).count() as f64 / (a.len() + b.len()) as f64
}

/// The queryable fields closest to a name the index does not answer, best first.
///
/// One score is taken of the two that differ in what they see: `token_f1` rewards a
/// name made of the same words, and `trigram_dice` rewards one made of the same
/// characters. Neither alone covers both failure shapes — a reordered guess and a
/// typo — and the `max` keeps whichever explanation is stronger.
fn similar_fields<'a>(unknown: &str, candidates: &[&'a FieldInfo]) -> Vec<&'a FieldInfo> {
    let input_tokens = name_tokens(unknown);
    let mut scored: Vec<(f64, &FieldInfo)> = candidates
        .iter()
        .map(|info| {
            let score = token_f1(&input_tokens, &name_tokens(&info.name))
                .max(trigram_dice(unknown, &info.name));
            (score, *info)
        })
        .filter(|(score, _)| *score >= MIN_SUGGESTION_SCORE)
        .collect();
    scored.sort_by(|(a_score, a), (b_score, b)| {
        b_score
            .partial_cmp(a_score)
            .unwrap_or(Ordering::Equal)
            .then(a.name.len().cmp(&b.name.len()))
            .then(a.name.cmp(&b.name))
    });
    scored
        .into_iter()
        .take(MAX_FIELD_SUGGESTIONS)
        .map(|(_, info)| info)
        .collect()
}

/// `a (b)` — how a suggestion reads: the name, then the type that decides which
/// operators it takes.
fn name_with_type(info: &FieldInfo) -> String {
    format!("{} ({})", info.name, info.field_type)
}

/// The name of the index's identifier field, for steering a caller that matched
/// nothing: the shadow name where one exists — the identifier under its source name —
/// else `id` when the schema carries it. Either is the key-value lookup rather than a
/// search.
fn identifier_field(field_infos: &[FieldInfo]) -> Option<&str> {
    field_infos
        .iter()
        .find(|info| info.is_shadow)
        .or_else(|| field_infos.iter().find(|info| info.name == "id"))
        .map(|info| info.name.as_str())
}

/// The correction for a field name the index does not answer: the closest real fields,
/// each with the type that decides which operators it takes.
fn did_you_mean(unknown: &str, similar: &[&FieldInfo]) -> String {
    format!(
        "Unknown field '{unknown}'. Did you mean: {}?",
        similar
            .iter()
            .map(|info| name_with_type(info))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

/// What is true when nothing is close: the catalogue of fields lives on
/// `describe_index`, and the identifier — the field the dataset is keyed on — is the
/// lookup worth knowing about when every other guess failed.
fn no_match_warning(unknown: &str, identifier: Option<&str>) -> String {
    match identifier {
        Some(name) => format!(
            "Unknown field '{unknown}'. No similar field exists — `describe_index` lists this \
             index's fields. If you meant the record's identifier, `{name}:VALUE` on its own is \
             answered from the key-value store: the fastest lookup this index has."
        ),
        None => format!(
            "Unknown field '{unknown}'. No similar field exists — `describe_index` lists this \
             index's fields."
        ),
    }
}

/// Append what an engine error about a missing field cannot say: which real fields the
/// caller probably meant.
///
/// The names come from the query rather than from the error text, because the engine's
/// wordings differ and the query is the reliable account of what was asked for. A
/// caller-fault error in an agent's hands is a query to rewrite, so the correction
/// belongs in the error rather than in a separate tool's answer.
pub(super) fn with_field_suggestions(
    error: &str,
    index: &str,
    field_infos: &[FieldInfo],
    query: &str,
) -> String {
    let queryable: Vec<&FieldInfo> = field_infos.iter().filter(|i| i.is_queryable()).collect();
    let identifier = identifier_field(field_infos);
    let mut corrections = String::new();
    for name in referenced_field_names(query) {
        if queryable.iter().any(|info| info.name == name) {
            continue;
        }
        let similar = similar_fields(&name, &queryable);
        corrections.push('\n');
        corrections.push_str(&if similar.is_empty() {
            no_match_warning(&name, identifier)
        } else {
            did_you_mean(&name, &similar)
        });
    }
    if corrections.is_empty() {
        format!("{error}\n\n`describe_index` lists the fields '{index}' has.")
    } else {
        format!("{error}\n{corrections}")
    }
}

/// Turn a search response carrying dropped clauses into a tool execution error.
///
/// The hits are real but do not answer the query as written — wider, narrower, or empty,
/// depending on where the dropped clause sat — and nothing in the payload marks them as such. MCP callers present results as fact, so they get an error naming the
/// clause; the HTTP API keeps the hits and reports the same list as
/// [`DISCARDED_CLAUSES_FIELD`].
pub(super) fn refuse_if_clauses_discarded(response: &JsonValue) -> Result<(), String> {
    let discarded: Vec<&str> = response
        .get(crate::node::DISCARDED_CLAUSES_FIELD)
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
         `describe_index` lists the fields this index actually has, with the operators each \
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

    let referenced_fields = referenced_field_names(query_text);

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
    // A field's hint is its type's — the shadow rule for a shadow field, whatever its
    // declared type — so the text is written once per distinct key into `hints` and
    // each entry names the key it resolved to. On a wide index the repeated paragraph
    // is the difference between guidance and a schema dump.
    let mut hints = serde_json::Map::new();

    for field_name in &referenced_fields {
        if queryable_names.contains(&field_name.as_str()) {
            recognized.push(field_name.clone());
            if let Some(info) = field_infos.iter().find(|i| i.name == *field_name) {
                let key = if info.is_shadow {
                    "shadow".to_string()
                } else {
                    info.field_type.clone()
                };
                let mut entry = serde_json::json!({
                    "field": field_name,
                    "type": info.field_type,
                    "hint": key,
                });
                // How the value in the query meets the field's terms — case, stemming,
                // diacritics — which the type's hint cannot say, since it is the tokenizer's.
                // Not on a `string` field: it is always `raw`, and its type's hint opens with
                // that rule already.
                if !info.is_shadow
                    && info.field_type != "string"
                    && let Some(rule) = cameodb_mcp::syntax::tokenizer_rule(
                        &info.field_type,
                        info.tokenizer.as_deref(),
                    )
                {
                    entry["matching"] = JsonValue::String(rule.to_string());
                }
                field_hints.push(entry);
                hints
                    .entry(key)
                    .or_insert_with(|| JsonValue::String(field_query_hint(info)));
            }
        } else if all_names.contains(&field_name.as_str()) {
            not_indexed.push(field_name.clone());
            warnings.push(format!(
                "Field '{}' exists but is not indexed. Queries against it will not match.",
                field_name
            ));
        } else if let Some(json) = field_infos.iter().find(|info| {
            info.field_type == "json"
                && field_name
                    .strip_prefix(info.name.as_str())
                    .is_some_and(|path| path.starts_with('.'))
        }) {
            // A path inside a json field, not an unknown field: the field is there, the path is
            // what cannot be queried, and the discarded clause says so. A "did you mean" here
            // would be a second, contradicting verdict. The json hint says what the field takes.
            field_hints.push(serde_json::json!({
                "field": field_name,
                "type": json.field_type,
                "hint": json.field_type,
            }));
            hints
                .entry(json.field_type.clone())
                .or_insert_with(|| JsonValue::String(field_query_hint(json)));
        } else {
            unknown.push(field_name.clone());
        }
    }

    if !unknown.is_empty() && !all_names.is_empty() {
        let queryable: Vec<&FieldInfo> = field_infos
            .iter()
            .filter(|info| info.is_queryable())
            .collect();
        let identifier = identifier_field(field_infos);
        for unk in &unknown {
            let similar = similar_fields(unk, &queryable);
            if similar.is_empty() {
                warnings.push(no_match_warning(unk, identifier));
            } else {
                suggestions.push(did_you_mean(unk, &similar));
            }
        }
    }

    serde_json::json!({
        "query": query_text,
        "recognized_fields": recognized,
        "unknown_fields": unknown,
        "not_indexed_fields": not_indexed,
        "field_hints": field_hints,
        "hints": hints,
        "warnings": warnings,
        "suggestions": suggestions,
    })
}

/// The distinct field names a query references, in the order they first appear.
///
/// Ordering and dedup over [`storage::field_references`], deliberately the same scanner the
/// engine reads a query with: this tool is where an agent is sent when it doubts a query, so a
/// verdict it reaches by its own reading could tell the agent a working query is wrong.
fn referenced_field_names(query: &str) -> Vec<String> {
    let mut names: Vec<String> = Vec::new();
    for reference in storage::field_references(query) {
        if !names.iter().any(|seen| seen == reference.name.as_ref()) {
            names.push(reference.name.into_owned());
        }
    }
    names
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
    use super::{names_a_missing_field, with_field_suggestions};
    use crate::mcp::schema::FieldInfo;

    fn field(name: &str, field_type: &str) -> FieldInfo {
        FieldInfo {
            name: name.to_string(),
            field_type: field_type.to_string(),
            tokenizer: None,
            is_shadow: false,
            searchable: true,
            sortable: false,
        }
    }

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

    /// The correction is appended, the engine's message survives, and the schema is not.
    #[test]
    fn a_correction_is_appended_rather_than_the_field_list() {
        let original = "Field 'titel' does not exist in schema";
        let fields = [field("title", "text"), field("body", "text")];
        let enriched = with_field_suggestions(original, "docs", &fields, "titel:rust");

        assert!(
            enriched.starts_with(original),
            "the engine's own message must survive: {enriched}"
        );
        assert!(
            enriched.contains("Did you mean: title (text)"),
            "the caller needs the field it meant, with its type: {enriched}"
        );
        assert!(
            !enriched.contains("body"),
            "a field nothing resembles is not a correction: {enriched}"
        );
    }

    /// With nothing close, the error points at the catalogue and at the identifier lookup
    /// rather than reciting the schema — the same answer `validate_query` gives.
    #[test]
    fn with_nothing_close_the_error_points_at_describe_index_and_the_identifier() {
        let fields = [field("id", "text"), field("title", "text")];
        let enriched = with_field_suggestions("unknown field 'zzz'", "docs", &fields, "zzz:rust");
        assert!(enriched.contains("describe_index"), "{enriched}");
        assert!(enriched.contains("`id:VALUE`"), "{enriched}");
        assert!(!enriched.contains("title"), "{enriched}");

        // A name the query scan cannot see still gets the catalogue pointer.
        let enriched = with_field_suggestions("unknown field 'zzz'", "docs", &fields, "rust");
        assert!(
            enriched.contains("describe_index") && enriched.contains("'docs'"),
            "{enriched}"
        );
    }
}

/// Corrections are scored against a synthetic malware-sample catalogue: invented field names
/// shaped like a wide security index, since the matcher's failure modes only show on that shape
/// — a generic word matching a dozen fields that share its prefix, a reordered guess sharing no
/// substring with the field it means, a name written fused, and a dump of every name in place
/// of a correction.
///
/// Invented on purpose. Tests are modelled on a real use case, never copied from one: field
/// names from a deployment's schema describe that deployment's data, and do not belong here.
#[cfg(test)]
mod field_suggestion_tests {
    use super::{MAX_FIELD_SUGGESTIONS, analyze_query, name_tokens, similar_fields, trigram_dice};
    use crate::mcp::schema::FieldInfo;

    const SAMPLE_CATALOGUE_FIELDS: &[(&str, &str)] = &[
        ("id", "text"),
        // The identifier under its source name, made a shadow field where a test needs one.
        ("sample_digest", "text"),
        // A digest family, and longer names that merely end in a digest.
        ("digest_md5", "text"),
        ("digest_sha1", "text"),
        ("digest_sha256", "text"),
        ("digest_sha384", "text"),
        ("digest_sha512", "text"),
        ("fuzzyhash", "text"),
        ("overlay_section_sha1", "text"),
        ("resource_blob_sha256", "text"),
        // Names under which a sample was submitted.
        ("upload_name", "text"),
        ("upload_names", "text"),
        ("upload_count", "i64"),
        ("suggested_upload_name", "text"),
        // Threat labels from several sources, sharing words in different positions.
        ("sandbox_threat_labels", "text"),
        ("intel_threat_label", "text"),
        ("intel_engine_threat_label", "text"),
        ("intel_feed_threat_label", "text"),
        ("intel_threat_severity", "text"),
        ("intel_malware_family", "text"),
        ("intel_malware_class", "text"),
        ("sandbox_malware_configs", "text"),
        // Scores of two numeric types.
        ("sandbox_risk_score", "i64"),
        ("exploit_score_f", "f64"),
        ("sandbox_behavior_rules", "text"),
        ("sandbox_engine_name", "text"),
        ("verdicts", "text"),
        ("intel_verdicts", "text"),
        // Dates, one with its words in an order a caller would not guess.
        ("date_binary_compiled_on", "date"),
        ("doc_created_date", "date"),
        ("doc_title", "text"),
        ("observed_first", "date"),
        ("observed_last", "date"),
        ("ingest_record_timestamp", "date"),
        ("intel_record_timestamp", "date"),
        // One scanner verdict per engine: more fields under one prefix than the cap.
        ("scanner_alpha", "text"),
        ("scanner_bravo", "text"),
        ("scanner_charlie", "text"),
        ("scanner_delta", "text"),
        ("scanner_echo", "text"),
        ("scanner_foxtrot", "text"),
        ("scanner_golf", "text"),
        ("scanner_hotel", "text"),
        ("scanner_india", "text"),
        ("scanner_juliett", "text"),
        ("scanner_kilo", "text"),
        ("scanner_quickcheck", "text"),
    ];

    fn info(name: &str, field_type: &str) -> FieldInfo {
        FieldInfo {
            name: name.to_string(),
            field_type: field_type.to_string(),
            tokenizer: None,
            is_shadow: false,
            searchable: true,
            sortable: false,
        }
    }

    fn schema() -> Vec<FieldInfo> {
        SAMPLE_CATALOGUE_FIELDS
            .iter()
            .map(|(name, field_type)| info(name, field_type))
            .collect()
    }

    fn suggest(unknown: &str) -> Vec<String> {
        let fields = schema();
        let refs: Vec<&FieldInfo> = fields.iter().collect();
        similar_fields(unknown, &refs)
            .into_iter()
            .map(|info| info.name.clone())
            .collect()
    }

    #[test]
    fn a_value_s_spellings_are_each_tried_once() {
        assert_eq!(
            super::spellings("austen, jane"),
            [
                "austen, jane",
                "Austen, jane",
                "Austen, Jane",
                "AUSTEN, JANE"
            ]
        );
        assert_eq!(
            super::spellings("HRVATSKI SABOR"),
            [
                "HRVATSKI SABOR",
                "hrvatski sabor",
                "Hrvatski sabor",
                "Hrvatski Sabor"
            ]
        );
        assert_eq!(
            super::spellings("rješenje"),
            ["rješenje", "Rješenje", "RJEŠENJE"]
        );
    }

    /// Decided by how a field matches, not its type: a `text` field under `raw` is as
    /// case-sensitive as a `string` one; one under any other tokenizer, or a shadow, is not.
    #[test]
    fn a_text_field_under_raw_takes_the_retry_too() {
        let mut code = info("code", "text");
        code.tokenizer = Some("raw".to_string());
        let mut folded = info("label", "text");
        folded.tokenizer = Some("raw_fold".to_string());
        let mut sha = info("sha1", "string");
        sha.is_shadow = true;
        let fields = vec![code, folded, sha];
        let found = super::string_value_clauses("code:ABC AND label:Abc AND sha1:FF", &fields);
        assert_eq!(
            found.iter().map(|c| c.text.as_str()).collect::<Vec<_>>(),
            ["code:ABC"]
        );
    }

    /// The clauses the retry is for: plain values and `IN` sets on string fields, not a text
    /// clause, a range or a prefix.
    #[test]
    fn the_string_clauses_of_a_query_are_found() {
        let fields = vec![
            info("authors", "string"),
            info("unit_type", "string"),
            info("title", "text"),
            info("year", "i64"),
        ];
        let found = super::string_value_clauses(
            r#"authors:"austen, jane" AND title:pride AND unit_type:Book AND unit_type: IN ["a b" C]"#,
            &fields,
        );
        assert_eq!(
            found
                .iter()
                .map(|c| (c.text.as_str(), c.values.clone(), c.set))
                .collect::<Vec<_>>(),
            [
                (
                    r#"authors:"austen, jane""#,
                    vec!["austen, jane".to_string()],
                    false
                ),
                ("unit_type:Book", vec!["Book".to_string()], false),
                (
                    r#"unit_type: IN ["a b" C]"#,
                    vec!["a b".to_string(), "C".to_string()],
                    true
                ),
            ]
        );
        for query in [
            "title:pride",
            "year:1813",
            "authors:Aus*",
            r#"authors:["a" TO "b"]"#,
            r#"authors: IN ["a" "b""#,
        ] {
            assert!(
                super::string_value_clauses(query, &fields).is_empty(),
                "{query}"
            );
        }

        let advice = super::string_case_advice(&found[0]);
        assert!(
            advice.contains(
                r#"`authors: IN ["austen, jane" "Austen, jane" "Austen, Jane" "AUSTEN, JANE"]`"#
            ),
            "{advice}"
        );
        // A set is the retry already made: no spellings are offered again.
        let advice = super::string_case_advice(&found[2]);
        assert!(advice.contains("no value of this set"), "{advice}");
        assert!(!advice.contains("retry"), "{advice}");
        // A value with no letters has no other spelling to retry.
        let fields = vec![info("id", "string")];
        let id = super::string_value_clauses("id:1212_0004", &fields);
        let advice = super::string_case_advice(&id[0]);
        assert!(!advice.contains("IN ["), "{advice}");
        assert!(advice.contains("a part of one"), "{advice}");
        // Several clauses share one explanation, then each its retry or none.
        let advice = super::whole_value_advice(&[&found[0], &found[1], &found[2]]).unwrap();
        assert_eq!(advice.matches("whole value").count(), 1, "{advice}");
        assert!(
            advice.contains(r#"`unit_type: IN ["Book" "book" "BOOK"]`"#),
            "{advice}"
        );
        assert!(
            advice.contains(r#"left to try for `unit_type: IN ["a b" C]`"#),
            "{advice}"
        );
        assert!(super::whole_value_advice(&[]).is_none());
        // The quotes of a string value or set are not a phrase to the general advice.
        let stripped = super::without_string_value_quotes(
            r#"authors:"austen, jane" AND title:pride AND unit_type: IN ["a b" C]"#,
            &found,
        );
        assert!(!stripped.contains('"'), "{stripped}");
    }

    /// A field the query names comes back with how its values are matched: a string field
    /// matches its whole value in its own case, which is the commonest reason a query that
    /// parses finds nothing, and the type's operator list cannot say it.
    #[test]
    fn a_named_field_carries_its_matching_rule() {
        let mut title = info("title", "text");
        title.tokenizer = Some("en_stem".to_string());
        let fields = vec![info("authors", "string"), title, info("year", "i64")];
        let analysis = analyze_query(
            "authors:\"austen, jane\" AND title:pride AND year:1813",
            &fields,
        );
        let hints = analysis["field_hints"].as_array().expect("field_hints");
        let matching = |name: &str| {
            hints
                .iter()
                .find(|h| h["field"] == name)
                .and_then(|h| h["matching"].as_str())
                .map(str::to_string)
        };
        // A string field's rule is its type's hint, said once.
        assert_eq!(matching("authors"), None, "{analysis}");
        assert!(
            analysis["hints"]["string"]
                .as_str()
                .is_some_and(|h| h.contains("case")),
            "{analysis}"
        );
        assert!(
            matching("title").is_some_and(|m| m.contains("English")),
            "{analysis}"
        );
        assert_eq!(
            matching("year"),
            None,
            "a number has no tokenizer: {analysis}"
        );
    }

    /// A path inside a json field is not an unknown field: no "did you mean" for it, and the
    /// json field's hint in its place. Why the path cannot be queried is the discarded clause's.
    #[test]
    fn a_path_inside_a_json_field_is_not_an_unknown_field() {
        let fields = vec![info("cells", "json"), info("year", "i64")];
        let analysis = analyze_query("cells.amount:>3 AND nosuch:x", &fields);
        assert_eq!(
            analysis["unknown_fields"],
            serde_json::json!(["nosuch"]),
            "{analysis}"
        );
        assert!(
            !analysis["suggestions"]
                .as_array()
                .unwrap()
                .iter()
                .any(|s| s.as_str().unwrap().contains("cells.amount")),
            "{analysis}"
        );
        assert_eq!(
            analysis["field_hints"][0],
            serde_json::json!({"field": "cells.amount", "type": "json", "hint": "json"})
        );
        assert!(analysis["hints"]["json"].is_string(), "{analysis}");
    }

    #[test]
    fn names_split_on_separators_and_camel_case() {
        assert_eq!(
            name_tokens("sandbox_threat_labels"),
            ["sandbox", "threat", "labels"]
        );
        assert_eq!(
            name_tokens("sandbox.threatLabels"),
            ["sandbox", "threat", "labels"]
        );
        assert_eq!(name_tokens("k8s-node"), ["k8s", "node"]);
    }

    /// A reordered guess shares words and no substring: the case whole-string matching missed
    /// entirely and answered with every name in the schema.
    #[test]
    fn a_reordered_guess_finds_the_field_by_its_words() {
        assert_eq!(
            suggest("compile_date").first().map(String::as_str),
            Some("date_binary_compiled_on")
        );
        let labels = suggest("threat_label");
        assert!(
            labels.contains(&"intel_threat_label".to_string()),
            "{labels:?}"
        );
        assert!(
            labels.contains(&"sandbox_threat_labels".to_string()),
            "{labels:?}"
        );
    }

    /// Typos and fused names share characters rather than words.
    #[test]
    fn a_typo_or_a_fused_name_finds_the_field_by_its_characters() {
        assert!(trigram_dice("titel", "title") >= super::MIN_SUGGESTION_SCORE);
        assert_eq!(
            suggest("riskscore").first().map(String::as_str),
            Some("sandbox_risk_score")
        );
        assert!(suggest("threatlabel").contains(&"intel_threat_label".to_string()));
        assert!(suggest("hash").contains(&"fuzzyhash".to_string()));
    }

    /// The digest family ranks ahead of the long names that merely end in a digest.
    #[test]
    fn exact_word_matches_rank_ahead_of_partial_ones() {
        let found = suggest("sha");
        let top4: Vec<&str> = found.iter().take(4).map(String::as_str).collect();
        for name in [
            "digest_sha1",
            "digest_sha256",
            "digest_sha384",
            "digest_sha512",
        ] {
            assert!(
                top4.contains(&name),
                "{name} not in the top four: {found:?}"
            );
        }
    }

    /// A generic word matches every field under its prefix; the answer is bounded however wide
    /// the schema.
    #[test]
    fn a_generic_word_is_capped() {
        let found = suggest("scanner");
        assert_eq!(found.len(), MAX_FIELD_SUGGESTIONS, "{found:?}");
        assert!(found.iter().all(|f| f.starts_with("scanner_")), "{found:?}");
    }

    /// Below the threshold the shared trigrams are coincidence, and offering them would read as
    /// a finding.
    #[test]
    fn nothing_resembling_suggests_nothing() {
        assert!(
            suggest("zzz_nonexistent").is_empty(),
            "{:?}",
            suggest("zzz_nonexistent")
        );
    }

    /// The analysis puts a correction in `suggestions` with each field's type, and never lists
    /// the schema when nothing is close — it names the identifier fast path instead.
    #[test]
    fn the_analysis_corrects_with_types_and_never_dumps_the_schema() {
        let mut fields = schema();
        let close = analyze_query("compile_date:[2024 TO 2025}", &fields);
        let suggestion = close["suggestions"].to_string();
        assert!(
            suggestion.contains("date_binary_compiled_on (date)"),
            "{close}"
        );

        let far = analyze_query("zzz_nonexistent:x", &fields);
        let warning = far["warnings"].to_string();
        assert!(warning.contains("describe_index"), "{far}");
        assert!(
            warning.contains("`id:VALUE`"),
            "with no shadow field, `id` is the identifier to point at: {far}"
        );
        assert!(
            !warning.contains("scanner_alpha"),
            "the schema must not be recited: {far}"
        );

        // On an index with a shadow field, the shadow name is the identifier to point at.
        if let Some(digest) = fields.iter_mut().find(|f| f.name == "sample_digest") {
            digest.is_shadow = true;
            digest.searchable = false;
        }
        let far = analyze_query("zzz_nonexistent:x", &fields);
        assert!(
            far["warnings"]
                .to_string()
                .contains("`sample_digest:VALUE`"),
            "{far}"
        );
    }

    /// Each referenced field names its hint by key; the text is written once per key.
    #[test]
    fn hints_are_written_once_per_type() {
        let fields = schema();
        let analysis = analyze_query(
            "upload_name:a AND sandbox_behavior_rules:b AND doc_title:c AND observed_first:>2024",
            &fields,
        );
        let entries = analysis["field_hints"].as_array().expect("field_hints");
        assert_eq!(entries.len(), 4, "{analysis}");
        assert!(entries.iter().all(|e| e["hint"].is_string()), "{analysis}");
        let hints = analysis["hints"].as_object().expect("hints");
        let mut keys: Vec<&str> = hints.keys().map(String::as_str).collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            ["date", "text"],
            "two types referenced, two paragraphs: {analysis}"
        );
        for entry in entries {
            let key = entry["hint"].as_str().unwrap_or_default();
            assert!(
                hints.contains_key(key),
                "{key} has no paragraph: {analysis}"
            );
        }
    }
}

/// The verdict mapping is the whole contract: caller-fault verdicts keep their text, every
/// 5xx verdict is masked. If a new variant lands on the wrong side of that line the leak is
/// silent, so each arm is pinned.
#[cfg(test)]
mod tool_error_tests {
    use super::tool_error;
    use cameodb_mcp::ToolError;
    use storage::StoreError;

    use crate::node::OrchestratorError;

    /// What the caller reads: `Caller` keeps its text, `Internal` the mask.
    fn shown(err: OrchestratorError) -> String {
        tool_error(err).into_response_text()
    }

    #[test]
    fn a_caller_fault_keeps_the_message_written_for_it() {
        let text = shown(OrchestratorError::Validation(
            "Missing routing key for index 'docs'".to_string(),
        ));
        assert_eq!(text, "Missing routing key for index 'docs'");

        let text = shown(OrchestratorError::Storage(StoreError::IndexNotFound(
            "docs".to_string(),
        )));
        assert!(
            text.contains("docs"),
            "a missing index names itself: {text}"
        );
    }

    #[test]
    fn a_server_fault_is_masked_but_not_lost() {
        let err = tool_error(OrchestratorError::Missing(
            "local shard 7 for 'docs' not found".to_string(),
        ));
        assert!(matches!(err, ToolError::Internal(_)));
        // The mask is for the caller; the record of what happened keeps the detail.
        assert!(err.detail().contains("local shard 7"));
    }

    #[test]
    fn an_unavailable_is_masked_too() {
        // Its text names topology the caller cannot act on — which shard, which peer — so it
        // is masked like any other 5xx rather than passed through like a 503 body.
        for err in [
            OrchestratorError::NotReady("no shards for 'docs'".to_string()),
            OrchestratorError::PeerUnreachable {
                message: "peer node-3 did not answer".to_string(),
            },
        ] {
            let err = tool_error(err);
            assert!(matches!(err, ToolError::Internal(_)), "{err}");
        }
    }
}
