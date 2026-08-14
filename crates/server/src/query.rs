//! The inline modifiers a query may carry after its text: `return`, `limit` and `sort`.
//!
//! Shared by both surfaces — an HTTP search and an MCP tool call accept the same query string, so
//! they have to agree on where the query ends and the modifiers begin.

use storage::{SortOrder, SortSpec};

/// The keywords that open an inline modifier clause.
const MODIFIER_KEYWORDS: [&str; 3] = ["return", "limit", "sort"];

/// Whether a token has the shape of a field name, which rules out quoted text, groups and a
/// `field:value` clause.
fn is_field_name(token: &str) -> bool {
    !token.is_empty()
        && token
            .chars()
            .all(|ch| ch.is_alphanumeric() || matches!(ch, '_' | '.' | '-'))
}

/// A `sort` clause's spec. An order suffix must be exactly `asc` or `desc`.
fn parse_sort_spec(token: &str) -> Option<SortSpec> {
    let (field, order) = match token.split_once(':') {
        Some((field, order)) => match order.to_ascii_lowercase().as_str() {
            "asc" => (field, SortOrder::Asc),
            "desc" => (field, SortOrder::Desc),
            _ => return None,
        },
        None => (token, SortOrder::Asc),
    };

    is_field_name(field).then(|| SortSpec {
        field: field.to_string(),
        order,
    })
}

/// The fields of a `return` clause.
///
/// The tokens must form one comma-separated list: adjacent names need a comma between them, so
/// `return name, price` is a projection of two fields while `return tax forms` is not a projection
/// at all.
fn parse_field_list(tokens: &[&str]) -> Option<Vec<String>> {
    if tokens.is_empty()
        || tokens
            .windows(2)
            .any(|pair| !pair[0].ends_with(',') && !pair[1].starts_with(','))
    {
        return None;
    }

    let mut names = Vec::new();
    for name in tokens.iter().flat_map(|token| token.split(',')) {
        let name = name.trim();
        if name.is_empty() {
            continue;
        }
        if !is_field_name(name) {
            return None;
        }
        names.push(name.to_string());
    }

    (!names.is_empty()).then_some(names)
}

/// What a run of inline modifiers set: limit, projected fields, sort.
type Modifiers = (Option<usize>, Option<Vec<String>>, Option<SortSpec>);

/// Parse `tokens` as an unbroken run of modifier clauses, in any order and at most one of each.
///
/// None unless every token belongs to a clause, which is what confines a keyword to a run that
/// reaches the end of the query.
fn parse_modifier_run(tokens: &[&str]) -> Option<Modifiers> {
    let mut limit = None;
    let mut fields = None;
    let mut sort = None;
    let mut idx = 0;

    while idx < tokens.len() {
        match tokens[idx] {
            "limit" if limit.is_none() => {
                limit = Some(tokens.get(idx + 1)?.parse().ok()?);
                idx += 2;
            }
            "sort" if sort.is_none() => {
                sort = Some(parse_sort_spec(tokens.get(idx + 1)?)?);
                idx += 2;
            }
            "return" if fields.is_none() => {
                // The field list runs to the next keyword, since only a keyword can end it.
                let end = tokens[idx + 1..]
                    .iter()
                    .position(|token| MODIFIER_KEYWORDS.contains(token))
                    .map_or(tokens.len(), |rel| idx + 1 + rel);
                fields = Some(parse_field_list(&tokens[idx + 1..end])?);
                idx = end;
            }
            _ => return None,
        }
    }

    Some((limit, fields, sort))
}

/// Split a query from its trailing inline modifiers — `return`, `limit` and `sort`.
///
/// A keyword counts only where it opens a run of clauses that reaches the end of the query and
/// leaves at least one token in front of it. Everything else is query text, so a keyword used as a
/// word — `find tax return forms`, `sort by date` — stays in the query.
///
/// Returns (query, limit, fields, sort).
pub(crate) fn parse_query_keywords(
    query: &str,
) -> (String, Option<usize>, Option<Vec<String>>, Option<SortSpec>) {
    let tokens: Vec<&str> = query.split_whitespace().collect();

    // Earliest start first, so `return a limit 5` is one run rather than a query ending in
    // `return a`.
    for start in 1..tokens.len() {
        if MODIFIER_KEYWORDS.contains(&tokens[start])
            && let Some((limit, fields, sort)) = parse_modifier_run(&tokens[start..])
        {
            return (tokens[..start].join(" "), limit, fields, sort);
        }
    }

    (query.to_string(), None, None, None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_query_keywords_no_keywords() {
        let query = "title:rust";
        let (cleaned, limit, fields, parsed_sort) = parse_query_keywords(query);
        assert_eq!(cleaned, "title:rust");
        assert_eq!(limit, None);
        assert_eq!(fields, None);
        assert_eq!(parsed_sort, None);
    }

    #[test]
    fn test_parse_query_keywords_limit_only() {
        let query = "title:rust limit 10";
        let (cleaned, limit, fields, parsed_sort) = parse_query_keywords(query);
        assert_eq!(cleaned, "title:rust");
        assert_eq!(limit, Some(10));
        assert_eq!(fields, None);
        assert_eq!(parsed_sort, None);
    }

    #[test]
    fn test_parse_query_keywords_limit_zero() {
        let query = "title:rust limit 0";
        let (cleaned, limit, fields, parsed_sort) = parse_query_keywords(query);
        assert_eq!(cleaned, "title:rust");
        assert_eq!(limit, Some(0));
        assert_eq!(fields, None);
        assert_eq!(parsed_sort, None);
    }

    #[test]
    fn test_parse_query_keywords_return_only() {
        let query = "title:rust return title,author,year";
        let (cleaned, limit, fields, parsed_sort) = parse_query_keywords(query);
        assert_eq!(cleaned, "title:rust");
        assert_eq!(limit, None);
        assert_eq!(
            fields,
            Some(vec![
                "title".to_string(),
                "author".to_string(),
                "year".to_string()
            ])
        );
        assert_eq!(parsed_sort, None);
    }

    #[test]
    fn test_parse_query_keywords_both() {
        let query = "title:rust limit 5 return title,author";
        let (cleaned, limit, fields, parsed_sort) = parse_query_keywords(query);
        assert_eq!(cleaned, "title:rust");
        assert_eq!(limit, Some(5));
        assert_eq!(
            fields,
            Some(vec!["title".to_string(), "author".to_string()])
        );
        assert_eq!(parsed_sort, None);
    }

    #[test]
    fn test_parse_query_keywords_reverse_order() {
        let query = "title:rust return title,author limit 5";
        let (cleaned, limit, fields, parsed_sort) = parse_query_keywords(query);
        assert_eq!(cleaned, "title:rust");
        assert_eq!(limit, Some(5));
        assert_eq!(
            fields,
            Some(vec!["title".to_string(), "author".to_string()])
        );
        assert_eq!(parsed_sort, None);
    }

    #[test]
    fn test_parse_query_keywords_single_field() {
        let query = "title:rust return title";
        let (cleaned, limit, fields, parsed_sort) = parse_query_keywords(query);
        assert_eq!(cleaned, "title:rust");
        assert_eq!(limit, None);
        assert_eq!(fields, Some(vec!["title".to_string()]));
        assert_eq!(parsed_sort, None);
    }

    #[test]
    fn test_parse_query_keywords_with_spaces() {
        // Test space-separated field list: "return title, author, year"
        let query = "title:rust return title, author, year";
        let (cleaned, limit, fields, parsed_sort) = parse_query_keywords(query);
        assert_eq!(cleaned, "title:rust");
        assert_eq!(limit, None);
        assert_eq!(
            fields,
            Some(vec![
                "title".to_string(),
                "author".to_string(),
                "year".to_string()
            ])
        );
        assert_eq!(parsed_sort, None);
    }

    #[test]
    fn test_parse_query_keywords_complex_query() {
        let query = "title:rust AND author:smith limit 20 return title,author,year,isbn";
        let (cleaned, limit, fields, parsed_sort) = parse_query_keywords(query);
        assert_eq!(cleaned, "title:rust AND author:smith");
        assert_eq!(limit, Some(20));
        assert_eq!(
            fields,
            Some(vec![
                "title".to_string(),
                "author".to_string(),
                "year".to_string(),
                "isbn".to_string()
            ])
        );
        assert_eq!(parsed_sort, None);
    }

    #[test]
    fn test_parse_query_keywords_invalid_limit() {
        let query = "title:rust limit abc";
        let (cleaned, limit, fields, parsed_sort) = parse_query_keywords(query);
        // Invalid limit should not be parsed, query remains unchanged
        assert_eq!(cleaned, "title:rust limit abc");
        assert_eq!(limit, None);
        assert_eq!(fields, None);
        assert_eq!(parsed_sort, None);
    }

    #[test]
    fn test_parse_query_keywords_empty_field_list() {
        let query = "title:rust return ";
        let (cleaned, limit, fields, parsed_sort) = parse_query_keywords(query);
        // No clause parsed, so the query is passed through as written.
        assert_eq!(cleaned, "title:rust return ");
        assert_eq!(limit, None);
        assert_eq!(fields, None);
        assert_eq!(parsed_sort, None);
    }

    #[test]
    fn test_parse_query_keywords_trailing_comma() {
        let query = "title:rust return title,author,";
        let (cleaned, limit, fields, parsed_sort) = parse_query_keywords(query);
        assert_eq!(cleaned, "title:rust");
        assert_eq!(limit, None);
        // Trailing comma should be filtered out
        assert_eq!(
            fields,
            Some(vec!["title".to_string(), "author".to_string()])
        );
        assert_eq!(parsed_sort, None);
    }

    #[test]
    fn test_parse_query_keywords_sort_desc() {
        let query = "title:rust sort year:desc";
        let (cleaned, limit, fields, parsed_sort) = parse_query_keywords(query);
        assert_eq!(cleaned, "title:rust");
        assert_eq!(limit, None);
        assert_eq!(fields, None);
        assert_eq!(
            parsed_sort,
            Some(SortSpec {
                field: "year".to_string(),
                order: SortOrder::Desc,
            })
        );
    }

    #[test]
    fn test_parse_query_keywords_sort_asc() {
        let query = "title:rust sort year:asc";
        let (cleaned, limit, fields, parsed_sort) = parse_query_keywords(query);
        assert_eq!(cleaned, "title:rust");
        assert_eq!(limit, None);
        assert_eq!(fields, None);
        assert_eq!(
            parsed_sort,
            Some(SortSpec {
                field: "year".to_string(),
                order: SortOrder::Asc,
            })
        );
    }

    #[test]
    fn test_parse_query_keywords_sort_default_order() {
        let query = "title:rust sort year";
        let (cleaned, limit, fields, parsed_sort) = parse_query_keywords(query);
        assert_eq!(cleaned, "title:rust");
        assert_eq!(limit, None);
        assert_eq!(fields, None);
        assert_eq!(
            parsed_sort,
            Some(SortSpec {
                field: "year".to_string(),
                order: SortOrder::Asc,
            })
        );
    }

    #[test]
    fn test_parse_query_keywords_all_three() {
        let query = "title:rust return title,author limit 10 sort year:desc";
        let (cleaned, limit, fields, parsed_sort) = parse_query_keywords(query);
        assert_eq!(cleaned, "title:rust");
        assert_eq!(limit, Some(10));
        assert_eq!(
            fields,
            Some(vec!["title".to_string(), "author".to_string()])
        );
        assert_eq!(
            parsed_sort,
            Some(SortSpec {
                field: "year".to_string(),
                order: SortOrder::Desc,
            })
        );
    }

    #[test]
    fn test_parse_query_keywords_sort_before_return() {
        let query = "title:rust sort year:asc return title,author";
        let (cleaned, limit, fields, parsed_sort) = parse_query_keywords(query);
        assert_eq!(cleaned, "title:rust");
        assert_eq!(limit, None);
        assert_eq!(
            fields,
            Some(vec!["title".to_string(), "author".to_string()])
        );
        assert_eq!(
            parsed_sort,
            Some(SortSpec {
                field: "year".to_string(),
                order: SortOrder::Asc,
            })
        );
    }

    #[test]
    fn test_parse_query_keywords_sort_between_limit_and_return() {
        let query = "title:rust limit 5 sort timestamp:desc return title";
        let (cleaned, limit, fields, parsed_sort) = parse_query_keywords(query);
        assert_eq!(cleaned, "title:rust");
        assert_eq!(limit, Some(5));
        assert_eq!(fields, Some(vec!["title".to_string()]));
        assert_eq!(
            parsed_sort,
            Some(SortSpec {
                field: "timestamp".to_string(),
                order: SortOrder::Desc,
            })
        );
    }

    #[test]
    fn test_parse_query_keywords_empty_query() {
        let query = "";
        let (cleaned, limit, fields, parsed_sort) = parse_query_keywords(query);
        assert_eq!(cleaned, "");
        assert_eq!(limit, None);
        assert_eq!(fields, None);
        assert_eq!(parsed_sort, None);
    }

    /// A query that is left whole, because no run of clauses reaches its end.
    fn assert_no_modifiers(query: &str) {
        let (cleaned, limit, fields, parsed_sort) = parse_query_keywords(query);
        assert_eq!(cleaned, query, "{query:?} lost text to a modifier");
        assert_eq!(limit, None, "{query:?}");
        assert_eq!(fields, None, "{query:?}");
        assert_eq!(parsed_sort, None, "{query:?}");
    }

    #[test]
    fn test_parse_query_keywords_a_keyword_used_as_a_word_stays_in_the_query() {
        for query in [
            "sort by date",
            "how to limit costs",
            "find tax return forms online",
            "the sort order of things",
        ] {
            assert_no_modifiers(query);
        }
    }

    #[test]
    fn test_parse_query_keywords_a_run_must_reach_the_end_of_the_query() {
        for query in [
            "title:rust limit 5 extra",
            "title:rust sort year:desc AND tag:active",
            "body:\"the limit 10 rule\"",
        ] {
            assert_no_modifiers(query);
        }
    }

    #[test]
    fn test_parse_query_keywords_a_run_may_not_consume_the_whole_query() {
        // `* limit 10` is the way to ask for a bare limit; on its own it is two terms.
        assert_no_modifiers("limit 10");

        let (cleaned, limit, _, _) = parse_query_keywords("* limit 10");
        assert_eq!(cleaned, "*");
        assert_eq!(limit, Some(10));
    }

    #[test]
    fn test_parse_query_keywords_a_field_list_needs_its_commas() {
        assert_no_modifiers("title:rust return title author");
        assert_no_modifiers("title:rust return \"title\"");

        // Either side of the gap may carry the comma.
        let (_, _, fields, _) = parse_query_keywords("title:rust return title ,author");
        assert_eq!(
            fields,
            Some(vec!["title".to_string(), "author".to_string()])
        );
    }

    #[test]
    fn test_parse_query_keywords_a_sort_order_must_be_asc_or_desc() {
        assert_no_modifiers("title:rust sort year:descending");
        assert_no_modifiers("title:rust sort year:1");
    }

    #[test]
    fn test_parse_query_keywords_a_limit_must_be_a_number() {
        assert_no_modifiers("title:rust limit many");
        assert_no_modifiers("title:rust limit -5");
    }
}
