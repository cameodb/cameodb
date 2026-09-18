//! Unit tests for the CLI.

use super::*;
use rustyline::completion::{Completer, Pair};

// Only import colored on non-Windows platforms

#[cfg(test)]
#[allow(clippy::module_inception)]
mod tests {
    use super::*;
    use rustyline::Context;
    use rustyline::history::MemHistory;
    use std::collections::HashMap;
    use std::sync::{Arc, RwLock};

    /// `delete <index>` still means the index; naming documents is what changes it.
    ///
    /// The dangerous direction is a caller who meant to delete documents and deletes the index
    /// instead, so an ids file that yields nothing is an error rather than a fall-through — and
    /// blank lines and comments are ignored, since an ids file is something a person edits.
    #[test]
    fn delete_names_documents_only_when_it_is_given_some() {
        let dir = tempfile::tempdir().expect("tempdir");

        assert_eq!(
            collect_delete_ids(&[], None).expect("no ids is the index"),
            None
        );
        assert_eq!(
            collect_delete_ids(&["b1".to_string()], None).expect("one id"),
            Some(vec!["b1".to_string()])
        );

        // One `--id` may name several, and the flag still repeats: the two syntaxes are the same
        // request, and they compose.
        assert_eq!(
            collect_delete_ids(&["b1,b2,b3".to_string()], None).expect("comma-separated"),
            Some(vec!["b1".to_string(), "b2".to_string(), "b3".to_string()])
        );
        assert_eq!(
            collect_delete_ids(&["b1,b2".to_string(), "b3".to_string()], None)
                .expect("both syntaxes at once"),
            Some(vec!["b1".to_string(), "b2".to_string(), "b3".to_string()])
        );

        // Whitespace around a comma is the shape a person types, and a stray or trailing comma
        // costs nothing rather than producing an empty id the server would refuse.
        assert_eq!(
            collect_delete_ids(&[" b1 , b2,, b3 ,".to_string()], None).expect("sloppy list"),
            Some(vec!["b1".to_string(), "b2".to_string(), "b3".to_string()])
        );
        // A `--id` that names nothing usable must not fall through to deleting the index. Same
        // refusal as an ids file with no ids in it, for the same reason.
        for empty_value in [",", "", "  ", ",,"] {
            let refused = collect_delete_ids(&[empty_value.to_string()], None).expect_err(
                "--id naming no usable id must be refused, not read as 'delete the index'",
            );
            assert!(
                refused.to_string().contains("refusing"),
                "the refusal must say why, for '{empty_value}': {refused}"
            );
        }

        // A file, with the shapes a hand-edited file actually has.
        let path = dir.path().join("ids.txt");
        std::fs::write(&path, "b2\n\n# a comment\n  b3  \n").expect("write");
        assert_eq!(
            collect_delete_ids(&["b1".to_string()], path.to_str()).expect("flags and file compose"),
            Some(vec!["b1".to_string(), "b2".to_string(), "b3".to_string()])
        );

        // A file line is one id, taken whole. That is what keeps an id containing a comma
        // nameable at all, now that `--id` splits on them.
        let commas = dir.path().join("commas.txt");
        std::fs::write(&commas, "a,b\n").expect("write");
        assert_eq!(
            collect_delete_ids(&[], commas.to_str()).expect("a line is not a list"),
            Some(vec!["a,b".to_string()]),
            "an id containing a comma survives the file path"
        );

        // An empty file must not read as "delete the index".
        let empty = dir.path().join("empty.txt");
        std::fs::write(&empty, "# nothing but a comment\n").expect("write");
        let refused = collect_delete_ids(&[], empty.to_str())
            .expect_err("an ids file with no ids is an error");
        assert!(
            refused.to_string().contains("refusing"),
            "the refusal must say why: {refused}"
        );

        let missing = collect_delete_ids(&[], Some("/no/such/ids/file"))
            .expect_err("an unreadable ids file is an error");
        assert!(missing.to_string().contains("Failed to read ids"));
    }

    fn completer_with_index() -> IndexCompleter {
        let cache = Arc::new(RwLock::new(HashMap::new()));
        {
            let mut guard = cache.write().unwrap();
            guard.insert(
                "myindex".to_string(),
                IndexMetadata {
                    fields: vec![FieldInfo {
                        name: "title".to_string(),
                        field_type: "text".to_string(),
                    }],
                },
            );
        }
        IndexCompleter::new(cache)
    }

    fn ctx(history: &MemHistory) -> Context<'_> {
        Context::new(history)
    }

    fn replacements(pairs: &[Pair]) -> Vec<&str> {
        pairs.iter().map(|p| p.replacement.as_str()).collect()
    }

    #[test]
    fn command_completion_respects_leading_whitespace() {
        let c = completer_with_index();
        let history = MemHistory::new();
        let (start, pairs) = c.complete("  se", 4, &ctx(&history)).unwrap();
        assert_eq!(start, 2);
        assert!(pairs.iter().any(|p| p.replacement == "search "));
    }

    #[test]
    fn command_completion_at_start_of_line() {
        let c = completer_with_index();
        let history = MemHistory::new();
        let (start, pairs) = c.complete("se", 2, &ctx(&history)).unwrap();
        assert_eq!(start, 0);
        assert!(pairs.iter().any(|p| p.replacement == "search "));
    }

    #[test]
    fn index_completion_start_is_beginning_of_token() {
        let c = completer_with_index();
        let history = MemHistory::new();
        let line = "search myindex";
        let (start, pairs) = c.complete(line, line.len(), &ctx(&history)).unwrap();
        assert_eq!(start, 7);
        assert!(pairs.iter().any(|p| p.replacement == "myindex "));
    }

    /// A word arrives with the space that opens the position after it, so the next Tab lands
    /// there.
    #[test]
    fn a_word_the_command_continues_past_completes_with_its_space() {
        let c = completer_with_index();
        let history = MemHistory::new();
        for (line, expected) in [
            ("li", "list "),
            ("list ind", "index "),
            ("delete myind", "myindex "),
            ("admin mem", "memory "),
            ("admin index myind", "myindex "),
            ("ke", "key "),
            ("key fil", "file "),
        ] {
            let (_, pairs) = c.complete(line, line.len(), &ctx(&history)).unwrap();
            assert!(
                pairs.iter().any(|p| p.replacement == expected),
                "{line:?} completed to {:?}, expected {expected:?}",
                replacements(&pairs)
            );
        }
    }

    /// The last word of a command has no position after it to open.
    #[test]
    fn a_word_that_ends_the_command_completes_without_a_space() {
        let c = completer_with_index();
        let history = MemHistory::new();
        for (line, expected) in [
            ("heal", "health"),
            ("exi", "exit"),
            ("admin work", "workers"),
            ("admin memory stat", "stats"),
            ("admin index myindex commi", "commit"),
            ("key sho", "show"),
            ("key clea", "clear"),
            ("admin memory purge --for", "--force"),
        ] {
            let (_, pairs) = c.complete(line, line.len(), &ctx(&history)).unwrap();
            assert!(
                pairs.iter().any(|p| p.replacement == expected),
                "{line:?} completed to {:?}, expected {expected:?}",
                replacements(&pairs)
            );
        }
    }

    /// `key` takes the key itself as well as its subcommands, so a secret typed at the prompt is
    /// left alone rather than completed against them.
    #[test]
    fn the_key_subcommands_complete_and_a_key_itself_does_not() {
        let c = completer_with_index();
        let history = MemHistory::new();

        let line = "key ";
        let (_, pairs) = c.complete(line, line.len(), &ctx(&history)).unwrap();
        assert_eq!(replacements(&pairs), vec!["file ", "show", "clear"]);

        let line = "key cameo_v1_";
        let (_, pairs) = c.complete(line, line.len(), &ctx(&history)).unwrap();
        assert!(pairs.is_empty(), "got: {:?}", replacements(&pairs));
    }

    /// An argument nothing can complete still leaves the command open, and the lookahead cannot
    /// see that on its own.
    #[test]
    fn a_command_taking_a_free_form_argument_completes_with_its_space() {
        let c = completer_with_index();
        let history = MemHistory::new();
        for (line, expected) in [("conne", "connect "), ("con", "conn ")] {
            let (_, pairs) = c.complete(line, line.len(), &ctx(&history)).unwrap();
            assert!(
                pairs.iter().any(|p| p.replacement == expected),
                "{line:?} completed to {:?}, expected {expected:?}",
                replacements(&pairs)
            );
        }
    }

    /// `index` is a target in its own right next to `indexes`, so a Tab on the finished word
    /// closes it instead of splicing back the same five characters.
    #[test]
    fn a_finished_word_wins_over_the_longer_word_that_shares_its_prefix() {
        let c = completer_with_index();
        let history = MemHistory::new();
        let line = "list index";
        let (_, pairs) = c.complete(line, line.len(), &ctx(&history)).unwrap();
        assert_eq!(replacements(&pairs), vec!["index "]);
    }

    /// Until the word is finished both are still in play.
    #[test]
    fn a_partial_word_keeps_every_candidate() {
        let c = completer_with_index();
        let history = MemHistory::new();
        let line = "list ind";
        let (_, pairs) = c.complete(line, line.len(), &ctx(&history)).unwrap();
        assert_eq!(replacements(&pairs), vec!["indexes ", "index "]);
    }

    /// Past the positional arguments only a flag is valid, whatever has been typed of it.
    #[test]
    fn only_flags_follow_the_positional_arguments() {
        let c = completer_with_index();
        let history = MemHistory::new();
        for line in ["list index myindex ", "list indexes ", "delete myindex "] {
            let (_, pairs) = c.complete(line, line.len(), &ctx(&history)).unwrap();
            assert!(
                !pairs.is_empty() && pairs.iter().all(|p| p.replacement.starts_with('-')),
                "{line:?} offered {:?}, expected flags only",
                replacements(&pairs)
            );
        }
    }

    #[test]
    fn a_flag_already_on_the_line_is_not_offered_again() {
        let c = completer_with_index();
        let history = MemHistory::new();
        let line = "list index myindex --data-size ";
        let (_, pairs) = c.complete(line, line.len(), &ctx(&history)).unwrap();
        assert!(
            !pairs.iter().any(|p| p.replacement.contains("--data-size")),
            "expected --data-size to be spent, got: {:?}",
            replacements(&pairs)
        );
    }

    /// A flag is completable from any prefix of it, and its value is a token of its own.
    #[test]
    fn a_flag_and_its_value_complete_as_separate_tokens() {
        let c = completer_with_index();
        let history = MemHistory::new();

        let line = "data load myindex data.csv --de";
        let (_, pairs) = c.complete(line, line.len(), &ctx(&history)).unwrap();
        assert_eq!(replacements(&pairs), vec!["--delimiter "]);

        let line = "data load myindex data.csv --delimiter ";
        let (_, pairs) = c.complete(line, line.len(), &ctx(&history)).unwrap();
        assert_eq!(
            replacements(&pairs),
            vec!["detect ", "comma ", "tab ", "semicolon "]
        );

        // The count that follows completes to nothing, so the flag carries its own space.
        let line = "data load myindex data.csv --ba";
        let (_, pairs) = c.complete(line, line.len(), &ctx(&history)).unwrap();
        assert_eq!(replacements(&pairs), vec!["--batch-size "]);

        let line = "data load myindex data.csv --batch-size ";
        let (_, pairs) = c.complete(line, line.len(), &ctx(&history)).unwrap();
        assert!(pairs.is_empty(), "got: {:?}", replacements(&pairs));
    }

    #[test]
    fn keyword_and_field_completion_after_trailing_space() {
        let c = completer_with_index();
        let history = MemHistory::new();
        let line = "search myindex title:rust ";
        let (start, pairs) = c.complete(line, line.len(), &ctx(&history)).unwrap();
        assert_eq!(start, line.len());
        assert!(pairs.iter().any(|p| p.replacement == "return "));
        assert!(pairs.iter().any(|p| p.replacement == "title:"));
    }

    #[test]
    fn keyword_and_field_completion_after_trailing_tab() {
        let c = completer_with_index();
        let history = MemHistory::new();
        let line = "search myindex title:rust\t";
        let (start, pairs) = c.complete(line, line.len(), &ctx(&history)).unwrap();
        assert_eq!(start, line.len());
        assert!(pairs.iter().any(|p| p.replacement == "return "));
        assert!(pairs.iter().any(|p| p.replacement == "title:"));
    }

    #[test]
    fn partial_keyword_completion_replaces_current_token() {
        let c = completer_with_index();
        let history = MemHistory::new();
        let line = "search myindex title:rust re";
        let (start, pairs) = c.complete(line, line.len(), &ctx(&history)).unwrap();
        assert_eq!(start, 26);
        assert!(pairs.iter().any(|p| p.replacement == "return "));
    }

    /// A run must leave query text in front of it, so a query that has none is not offered one.
    #[test]
    fn no_modifier_is_offered_before_the_query_has_a_term() {
        let c = completer_with_index();
        let history = MemHistory::new();
        for line in [
            "search myindex ",
            "search myindex re",
            "search myindex limit 5 ",
        ] {
            let (_, pairs) = c.complete(line, line.len(), &ctx(&history)).unwrap();
            assert!(
                !pairs
                    .iter()
                    .any(|p| matches!(p.replacement.as_str(), "return " | "limit " | "sort ")),
                "{line:?} offered a modifier with no query in front of it"
            );
        }
    }

    /// Successive completions have to build one comma-separated list, since a list without commas
    /// is query text rather than a projection.
    #[test]
    fn a_return_field_completes_with_its_comma() {
        let c = completer_with_index();
        let history = MemHistory::new();
        let line = "search myindex title:rust return ti";
        let (_, pairs) = c.complete(line, line.len(), &ctx(&history)).unwrap();
        assert!(
            pairs.iter().any(|p| p.replacement == "title,"),
            "expected a comma-terminated field, got: {:?}",
            pairs.iter().map(|p| &p.replacement).collect::<Vec<_>>()
        );
    }

    #[test]
    fn field_completion_replaces_partial_field_token() {
        let c = completer_with_index();
        let history = MemHistory::new();
        let line = "search myindex ti";
        let (start, pairs) = c.complete(line, line.len(), &ctx(&history)).unwrap();
        assert_eq!(start, 15);
        assert!(pairs.iter().any(|p| p.replacement == "title:"));
    }
}
