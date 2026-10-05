//! The window bound, spelled once.
//!
//! The tool schemas promise a caller that `offset + limit` cannot exceed the advertised
//! maximum, and the node's own surfaces enforce the same ceiling on the window a search
//! actually runs with — which is not always the argument a client sent, since the query
//! string carries `limit`/`offset` clauses of its own. Both doors into a search call
//! [`checked_search_window`], so the arithmetic and the refusal text cannot drift apart the
//! way two per-surface spellings did.

/// Resolve a request's `limit` and `offset` into the window the search will run with, or say
/// why it cannot be served.
///
/// Two things it settles that a per-surface check kept getting wrong:
///
/// An absent `limit` is the host's default, not zero. Bounding `offset + 0` lets
/// `offset = max_search_limit` past a check the engine then exceeds by the default, so the
/// advertised ceiling was not the real one.
///
/// The ceiling applies to `offset + limit` rather than to `limit`, because that sum is what
/// gets fetched: every source is asked for the whole window from the front and the skip is
/// applied after merging, so a deep page is exactly as expensive as a large limit, and
/// `max_search_limit` has to bound both or it bounds neither.
///
/// On success returns `(offset, limit)` — the window, in the order it reads on the page.
pub fn checked_search_window(
    limit: Option<usize>,
    offset: Option<usize>,
    default_limit: usize,
    max_search_limit: usize,
) -> Result<(usize, usize), String> {
    let limit = limit.unwrap_or(default_limit);
    let offset = offset.unwrap_or(0);

    if limit > max_search_limit {
        return Err(format!(
            "limit {limit} is above the maximum of {max_search_limit}; ask for at most that \
             many hits, or narrow the query"
        ));
    }

    let window = offset.saturating_add(limit);
    if window > max_search_limit {
        return Err(format!(
            "offset {offset} + limit {limit} = {window} is above the maximum of \
             {max_search_limit}; the engine fetches offset + limit hits, so a page this deep \
             costs what a limit that large costs. Narrow the query, or sort on a field that \
             lets you resume from the last hit instead of paging."
        ));
    }

    Ok((offset, limit))
}

#[cfg(test)]
mod tests {
    use super::checked_search_window;

    /// The vectors every surface agrees on — the point of the shared spelling is that these
    /// cannot pass on one side of the boundary and fail on the other.
    #[test]
    fn limit_past_the_maximum_is_refused() {
        assert!(checked_search_window(Some(101), None, 10, 100).is_err());
        assert!(checked_search_window(Some(100), None, 10, 100).is_ok());
        assert!(checked_search_window(None, None, 10, 100).is_ok());
    }

    #[test]
    fn refuses_when_offset_plus_limit_exceeds_the_maximum() {
        // offset + limit within the bound is accepted.
        assert!(checked_search_window(Some(50), Some(50), 10, 100).is_ok());
        assert!(checked_search_window(Some(100), Some(0), 10, 100).is_ok());

        // offset + limit past the bound is refused.
        assert!(checked_search_window(Some(50), Some(51), 10, 100).is_err());
        assert!(checked_search_window(Some(1), Some(100), 10, 100).is_err());

        // Neither named: the default limit from offset 0, which is within any usable bound.
        assert!(checked_search_window(None, None, 10, 100).is_ok());
    }

    /// An omitted `limit` is the node's default, and the bound is applied to that.
    ///
    /// The window this rejects — `offset` at the ceiling with no `limit` — is the one an
    /// earlier version accepted by reading the omission as zero, leaving the node to fetch
    /// `max + default` hits for a request the check had already approved.
    #[test]
    fn counts_the_default_limit_when_none_is_given() {
        assert!(checked_search_window(None, Some(100), 10, 100).is_err());
        assert!(checked_search_window(None, Some(90), 10, 100).is_ok());
        assert!(checked_search_window(None, Some(91), 10, 100).is_err());

        // The host's default is what counts, not this crate's: a node configured with a
        // larger one has less room for the offset, and says so at the same ceiling.
        assert!(checked_search_window(None, Some(90), 20, 100).is_err());
    }

    #[test]
    fn returns_the_resolved_window() {
        assert_eq!(
            checked_search_window(Some(5), Some(20), 10, 100),
            Ok((20, 5))
        );
        assert_eq!(checked_search_window(None, None, 10, 100), Ok((0, 10)));
    }

    /// The refusal says the number asked for against the bound advertised — the caller is
    /// owed both, in a text it can hand back to whoever wrote the call.
    #[test]
    fn refusal_names_the_numbers() {
        let err = checked_search_window(Some(200), None, 10, 100).unwrap_err();
        assert!(err.contains("200") && err.contains("100"), "{err}");

        let err = checked_search_window(Some(50), Some(60), 10, 100).unwrap_err();
        assert!(
            err.contains("60") && err.contains("110") && err.contains("100"),
            "{err}"
        );
    }
}
