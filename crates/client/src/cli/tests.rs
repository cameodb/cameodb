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

    /// A refused document is named by where it is in the source, not by its line in one batch.
    ///
    /// Every batch is a request of its own and the node counts lines per request, so a load of
    /// several batches reported "line 1" once per batch. A delimited file maps back to the line
    /// the row was read from; a JSON source, where a document need not be one line, to the
    /// document's number in the source.
    #[test]
    fn a_refusal_names_its_place_in_the_source() {
        let file = SourceLines::File(vec![4001, 4002, 4005]);
        assert_eq!(
            relocate_reason("line 3: Type mismatch for field 'when'", &file),
            "line 4005: Type mismatch for field 'when'"
        );
        let json = SourceLines::Documents(vec![8001, 8002, 8004]);
        assert_eq!(
            relocate_reason("line 2: not an object", &json),
            "document 8002: not an object"
        );
        // Nothing to map: left as the node wrote it rather than guessed at.
        assert_eq!(relocate_reason("line 9: gone", &file), "line 9: gone");
        assert_eq!(
            relocate_reason("document 2: shard did not take the batch", &file),
            "document 2: shard did not take the batch"
        );
    }

    /// The failed count is every document not written, not the reasons the node chose to list.
    ///
    /// The node lists a hundred and counts the rest in `suppressed_errors`; counting only the
    /// list reported `loaded=1030 failed=500` for a load of 16,559 documents.
    #[test]
    fn every_refusal_is_counted_including_those_not_listed() {
        let (mut sent, mut failed) = (0, 0);
        let listed: Vec<String> = (1..=100).map(|n| format!("line {n}: refused")).collect();
        record_ingest_response(
            &serde_json::json!({
                "items_written": 100,
                "errors": listed,
                "suppressed_errors": 3800,
            }),
            &SourceLines::Documents((1..=4000).collect()),
            &mut sent,
            &mut failed,
        );
        assert_eq!((sent, failed), (100, 3900));
        assert_eq!(sent + failed, 4000, "written plus refused is what was sent");
    }

    /// `schema load` takes the index first, as `data load` does, and still the form it was first
    /// written in.
    #[test]
    fn schema_load_names_the_index_first() {
        let args = |a: &[&str]| a.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        let load = SchemaOperation::Load;
        assert_eq!(
            schema_targets(load, args(&["wifi", "s.json"]), None).unwrap(),
            (Some("wifi".to_string()), "s.json".to_string())
        );
        assert_eq!(
            schema_targets(load, args(&["s.json"]), Some("wifi".to_string())).unwrap(),
            (Some("wifi".to_string()), "s.json".to_string())
        );
        assert_eq!(
            schema_targets(load, args(&["wifi", "s.json"]), Some("wifi".to_string())).unwrap(),
            (Some("wifi".to_string()), "s.json".to_string())
        );
        assert!(schema_targets(load, args(&["a", "s.json"]), Some("b".to_string())).is_err());
        assert!(schema_targets(SchemaOperation::Detect, args(&["a", "b"]), None).is_err());
    }

    /// Each batch a JSON source emits knows where in the source each of its documents came
    /// from — a document skipped for having no id included.
    #[test]
    fn json_batches_are_numbered_through_the_source() {
        let plan = JsonLoadPlan {
            id: IdSpec::Column("id".to_string()),
            explicit: false,
            shapes: HashMap::new(),
        };
        let mut pipeline = JsonIngestPipeline::new(2, plan);
        let mut events = Vec::new();
        for n in 0..6 {
            let doc = if n == 2 {
                serde_json::json!({"n": n})
            } else {
                serde_json::json!({"id": format!("d{n}"), "n": n})
            };
            pipeline.push(&doc, &mut events).expect("push");
        }
        pipeline.finish(&mut events);
        let batches: Vec<SourceLines> = events
            .into_iter()
            .map(|JsonIngestEvent::DataBatch { lines, .. }| lines)
            .collect();
        assert_eq!(
            batches,
            vec![
                SourceLines::Documents(vec![1, 2]),
                SourceLines::Documents(vec![4, 5]),
                SourceLines::Documents(vec![6]),
            ]
        );
        assert_eq!(pipeline.ledger.skipped, 1);
    }
}

/// How a cell is read: by the field it lands in, and by its column's date order.
#[cfg(test)]
mod csv_cell_tests {
    use super::*;
    use serde_json::json;
    use storage::TantivyFieldType;

    fn typed(field_type: TantivyFieldType) -> ColumnShape {
        ColumnShape {
            field_type: Some(field_type),
            ..Default::default()
        }
    }

    /// `NA` in a count column is a count nobody reported, and the row loads without it. The ted
    /// example has 134 of them; each refused its row as text in an integer field.
    #[test]
    fn a_missing_marker_is_no_value_in_a_field_that_cannot_hold_text() {
        for field_type in [
            TantivyFieldType::I64,
            TantivyFieldType::F64,
            TantivyFieldType::Date,
            TantivyFieldType::Boolean,
        ] {
            for marker in ["NA", "n/a", "#N/A", "NaN", "null", "None", "-", "", "  "] {
                assert_eq!(
                    csv_cell(marker, &typed(field_type.clone())),
                    JsonValue::Null,
                    "{marker:?} under {field_type:?}"
                );
            }
        }
        // In a text column it may be the value itself — Namibia's country code, for one.
        assert_eq!(csv_cell("NA", &typed(TantivyFieldType::Text)), json!("NA"));
    }

    /// A date cell goes as text whenever the text is a date, so the node's parser reads it as
    /// written. As numbers, `20240315` and `2024` were seconds since 1970.
    #[test]
    fn a_date_cell_is_sent_as_the_date_it_writes() {
        let date = typed(TantivyFieldType::Date);
        assert_eq!(csv_cell("20240315", &date), json!("20240315"));
        assert_eq!(csv_cell("2024", &date), json!("2024"));
        assert_eq!(csv_cell("20240315161313", &date), json!("20240315161313"));
        assert_eq!(csv_cell("1710519193", &date), json!("1710519193"));
        // Seconds the parser does not read as text stay seconds.
        assert_eq!(csv_cell("946684799", &date), json!(946_684_799));
        assert_eq!(csv_cell("-86400", &date), json!(-86_400));
        // Anything else goes as written, for the node to refuse by its reason.
        assert_eq!(csv_cell("soon", &date), json!("soon"));
    }

    #[test]
    fn a_text_cell_keeps_the_text_it_holds() {
        let text = typed(TantivyFieldType::Text);
        assert_eq!(csv_cell(" 007 ", &text), json!("007"));
        assert_eq!(csv_cell("TRUE", &text), json!("TRUE"));
        assert_eq!(csv_cell("8023954622E7", &text), json!("8023954622E7"));
        // A column no field describes is read by its look, as before.
        assert_eq!(csv_cell("007", &ColumnShape::default()), json!(7));
    }

    /// The spellings that cannot mean anything else in a boolean column; a blank is no value,
    /// not `false` — that would be a guess.
    #[test]
    fn a_boolean_cell_takes_the_spellings_that_cannot_mean_anything_else() {
        let flag = typed(TantivyFieldType::Boolean);
        for yes in ["TRUE", "yes", "Y", "1"] {
            assert_eq!(csv_cell(yes, &flag), json!(true), "{yes}");
        }
        for no in ["false", "No", "n", "0"] {
            assert_eq!(csv_cell(no, &flag), json!(false), "{no}");
        }
        assert_eq!(csv_cell("maybe", &flag), json!("maybe"));
        assert_eq!(csv_cell("", &flag), JsonValue::Null);
    }

    /// A list written into a cell is several values of the field, each fitted to it; an empty
    /// list is no value.
    #[test]
    fn a_list_cell_is_several_values() {
        let list = |field_type| ColumnShape {
            list: true,
            ..typed(field_type)
        };
        assert_eq!(
            csv_cell("['FRITZ!Box 6670 CM','x']", &list(TantivyFieldType::Text)),
            json!(["FRITZ!Box 6670 CM", "x"])
        );
        assert_eq!(
            csv_cell("[\"1\", 2, None]", &list(TantivyFieldType::I64)),
            json!([1, 2])
        );
        assert_eq!(
            csv_cell("[]", &list(TantivyFieldType::Text)),
            JsonValue::Null
        );
        // Not a list after all: the value as it is.
        assert_eq!(
            csv_cell("[draft] note", &list(TantivyFieldType::Text)),
            json!("[draft] note")
        );
    }

    #[test]
    fn a_list_is_parsed_as_python_and_json_write_one() {
        assert_eq!(parse_list("['a', 'b']"), Some(vec![json!("a"), json!("b")]));
        assert_eq!(
            parse_list(r#"["a","b"]"#),
            Some(vec![json!("a"), json!("b")])
        );
        assert_eq!(
            parse_list("['it\\'s', 1, True, None]"),
            Some(vec![json!("it's"), json!(1), json!(true), JsonValue::Null])
        );
        assert_eq!(parse_list("[]"), Some(vec![]));
        assert_eq!(parse_list("[draft]"), None);
        assert_eq!(parse_list("['a' 'b']"), None);
        assert_eq!(parse_list("[[1], [2]]"), None);
    }

    /// A JSON source's values are fitted to their fields as a CSV's cells are.
    #[test]
    fn a_json_value_is_fitted_to_its_field() {
        let count = typed(TantivyFieldType::I64);
        assert_eq!(fit_json(json!("12"), &count), json!(12));
        assert_eq!(fit_json(json!("NA"), &count), JsonValue::Null);
        assert_eq!(fit_json(json!(""), &count), JsonValue::Null);
        assert_eq!(fit_json(json!(12), &count), json!(12));
        let flag = typed(TantivyFieldType::Boolean);
        assert_eq!(fit_json(json!("TRUE"), &flag), json!(true));
        assert_eq!(fit_json(json!(""), &flag), JsonValue::Null);
        assert_eq!(fit_json(json!(0), &flag), json!(false));
        assert_eq!(fit_json(json!([true, "no"]), &flag), json!([true, false]));
        let text = typed(TantivyFieldType::Text);
        assert_eq!(
            fit_json(json!(" kept as is "), &text),
            json!(" kept as is ")
        );
        assert_eq!(fit_json(json!(""), &text), json!(""));
        assert_eq!(fit_json(json!(7), &text), json!("7"));
        let doc = typed(TantivyFieldType::Json);
        assert_eq!(fit_json(json!({"a": "1"}), &doc), json!({"a": "1"}));
    }

    /// Each record's line as `sed -n <N>p` counts: with LF or CRLF endings, a quoted field over
    /// two lines, blank lines, and no final line ending. The reader's own position was one line
    /// early on every CRLF record, and short again after each blank line.
    #[test]
    fn a_record_is_located_on_the_line_it_starts() {
        let cases = [
            ("a;b\n1;x\n2;\"multi\nline\"\n3;z\n", vec![2, 3, 5]),
            (
                "a;b\r\n1;x\r\n2;\"multi\r\nline\"\r\n3;z\r\n",
                vec![2, 3, 5],
            ),
            ("a;b\r\n1;x\r\n2;\"multi\nline\"\r\n3;z\r\n", vec![2, 3, 5]),
            ("a;b\n1;x\n\n\n2;y\n3;z\n", vec![2, 5, 6]),
            ("a;b\r\n1;x\r\n\r\n\r\n2;y\r\n3;z\r\n", vec![2, 5, 6]),
            ("a;b\n1;x\n2;y", vec![2, 3]),
            ("a;b\r\n1;x\r\n2;y", vec![2, 3]),
        ];
        for (text, expected) in cases {
            let mut reader = csv::ReaderBuilder::new()
                .delimiter(b';')
                .flexible(true)
                .from_reader(text.as_bytes());
            let header = reader.headers().expect("header").clone();
            let mut lines = RecordLines::after_header(&header, reader.position().line());
            let mut record = csv::StringRecord::new();
            let mut found = Vec::new();
            while reader.read_record(&mut record).expect("record") {
                found.push(lines.locate(&record, reader.position().line()));
            }
            assert_eq!(found, expected, "{text:?}");
        }
    }
}

/// What a scan says about a column, and the field it becomes.
#[cfg(test)]
mod detect_tests {
    use super::*;
    use serde_json::json;
    use std::collections::HashMap;
    use storage::TantivyFieldType;

    fn profile(csv_text: &str) -> (Vec<(String, Option<TantivyFieldType>)>, Profiler) {
        let mut reader = csv::ReaderBuilder::new().from_reader(csv_text.as_bytes());
        let headers: Vec<(String, Option<TantivyFieldType>)> = reader
            .headers()
            .expect("headers")
            .iter()
            .map(parse_header_with_hint)
            .collect();
        let names: Vec<String> = headers.iter().map(|(n, _)| n.clone()).collect();
        let mut profiler = Profiler::new(&names, 8192);
        for (n, record) in reader.records().enumerate() {
            profiler.observe_csv(&record.expect("record"), Location::Line(n as u64 + 2));
        }
        profiler.finish();
        (headers, profiler)
    }

    fn analysis(csv_text: &str, explicit: Option<&IdSpec>) -> SourceAnalysis {
        let (headers, profiler) = profile(csv_text);
        let ids = IdOptions {
            explicit,
            recorded: None,
        };
        SourceAnalysis::new(
            SourceFormat::CsvLike,
            None,
            Some(b','),
            headers,
            (profiler, ScanSummary::default()),
            &ids,
        )
        .expect("analysis")
    }

    fn types(csv_text: &str) -> HashMap<String, TantivyFieldType> {
        let analysis = analysis(csv_text, None);
        analysis
            .profiler
            .columns
            .iter()
            .zip(&analysis.choices)
            .map(|(c, choice)| (c.name.clone(), choice.field_type.clone()))
            .collect()
    }

    /// Strict: a column takes a type only when every value fits it, whatever order they come
    /// in. Evolution typed `abc, 5` and `5, abc` both as integers, and refused the `abc` rows.
    #[test]
    fn a_column_is_typed_by_all_its_values_not_the_first() {
        let t = types(
            "id,a,b,c,d,e\n\
             1,abc,5,1,1.5,2024-01-01\n\
             2,5,abc,2,2,2024-01-02\n\
             3,7,9,3,3,soon\n",
        );
        assert_eq!(t["a"], TantivyFieldType::Text);
        assert_eq!(t["b"], TantivyFieldType::Text);
        assert_eq!(t["c"], TantivyFieldType::I64);
        assert_eq!(t["d"], TantivyFieldType::F64);
        assert_eq!(t["e"], TantivyFieldType::Text);
    }

    /// A boolean column says `true` or `false` somewhere and nothing a flag cannot hold;
    /// blanks and `NA` are no value. `no data` beside `false` makes it text.
    #[test]
    fn a_column_is_boolean_only_when_every_value_can_be() {
        let t = types(
            "id,flag,anomaly,late,numbers\n\
             1,TRUE,false,true,1\n\
             2,,no data,unknown,0\n\
             3,False,ok,false,1\n\
             4,NA,false,TRUE,true\n",
        );
        assert_eq!(t["flag"], TantivyFieldType::Boolean);
        assert_eq!(t["anomaly"], TantivyFieldType::Text);
        assert_eq!(t["late"], TantivyFieldType::Text);
        assert_eq!(t["numbers"], TantivyFieldType::Boolean);
        // Only 1 and 0 say nothing of the kind, and stay a number.
        assert_eq!(types("id,n\n1,1\n2,0\n")["n"], TantivyFieldType::I64);
    }

    /// Numbers that are codes stay text: a leading zero anywhere, and in a column named as an
    /// identifier, a number too long for a double or written with an exponent.
    #[test]
    fn a_number_that_is_a_code_stays_text() {
        let t = types(
            "id,zip,serialNumber,reading,big_serial\n\
             1,01234,8023954622E7,8023954622E7,1234567890123456\n\
             2,10115,802395453830,2.5,1234567890123457\n",
        );
        assert_eq!(t["zip"], TantivyFieldType::Text);
        assert_eq!(t["serialNumber"], TantivyFieldType::Text);
        assert_eq!(t["reading"], TantivyFieldType::F64);
        assert_eq!(t["big_serial"], TantivyFieldType::Text);
    }

    /// Codes — no spaces, nearly one per row — are text indexed whole with the raw tokenizer;
    /// prose and a handful of repeated words are not.
    #[test]
    fn a_column_of_codes_is_raw_text() {
        let mut text = String::from("id,mac,note,word\n");
        for n in 0..100 {
            text.push_str(&format!(
                "{n},00:7A:A4:E5:{n:02X}:28,note number {n},w{}\n",
                n % 10
            ));
        }
        let analysis = analysis(&text, None);
        assert_eq!(analysis.choices[1].field_type, TantivyFieldType::Text);
        assert_eq!(analysis.choices[1].tokenizer, Some("raw"));
        assert_eq!(analysis.choices[2].tokenizer, None);
        assert_eq!(analysis.choices[3].tokenizer, None);
        let schema = schema_from_analysis(&analysis).expect("schema");
        assert_eq!(schema["fields"]["mac"]["tokenizer"], json!("raw"));
        assert_eq!(
            schema["fields"]["mac"]["index_record_option"],
            json!("Basic")
        );
    }

    /// A short set of repeated values is a category, kept whole as a string field.
    #[test]
    fn a_short_repeated_set_is_a_category() {
        let mut text = String::from("id,status,name\n");
        for n in 0..100 {
            text.push_str(&format!(
                "{n},{},name {n}\n",
                ["ok", "no data", "warning"][n % 3]
            ));
        }
        let analysis = analysis(&text, None);
        let status = &analysis.choices[1];
        assert_eq!(status.field_type, TantivyFieldType::String);
        assert!(status.category);
        // Names have spaces: prose, the default tokenizer.
        assert_eq!(analysis.choices[2].field_type, TantivyFieldType::Text);
        assert_eq!(analysis.choices[2].tokenizer, None);
        // Four values in four rows are not a category.
        assert_eq!(
            types("id,s\n1,a\n2,b\n3,a\n4,b\n")["s"],
            TantivyFieldType::Text
        );
    }

    /// A column of written lists is a multi-valued field of what the lists hold.
    #[test]
    fn a_column_of_lists_is_multivalued() {
        let analysis = analysis(
            "id,ssids,counts,mixed\n1,\"['a b','c']\",[1],x\n2,\"['d']\",\"[2, 3]\",\"['y']\"\n",
            None,
        );
        let ssids = &analysis.choices[1];
        assert!(ssids.list);
        assert_eq!(ssids.field_type, TantivyFieldType::Text);
        assert!(analysis.choices[2].list);
        assert_eq!(analysis.choices[2].field_type, TantivyFieldType::I64);
        assert!(!analysis.choices[3].list);
    }

    /// A slash column holding a date only day first can read is day first; one holding both
    /// kinds is no convention at all, and strictness makes it text rather than refuse half.
    #[test]
    fn a_date_column_is_read_in_the_order_it_writes() {
        let analysis = analysis(
            "id,when,dotted,both\n\
             a,03/04/2024,03.04.2024,15/03/2024\n\
             b,15/03/2024,03.15.2024,03/15/2024\n",
            None,
        );
        let when = &analysis.choices[1];
        assert_eq!(when.field_type, TantivyFieldType::Date);
        assert_eq!(when.dates.slash, DateOrder::DayFirst);
        assert_eq!(analysis.choices[2].dates.dot, DateOrder::MonthFirst);
        assert_eq!(analysis.choices[3].field_type, TantivyFieldType::Text);

        let field_types = HashMap::from([
            ("when".to_string(), TantivyFieldType::Date),
            ("dotted".to_string(), TantivyFieldType::Text),
        ]);
        let shapes = analysis.shapes(&field_types);
        assert_eq!(csv_cell("03/04/2024", &shapes[1]), json!("2024-04-03"));
        assert_eq!(
            csv_cell("15/03/2024 16:13", &shapes[1]),
            json!("2024-03-15 16:13")
        );
        // Only a date field is reordered: a text column keeps what it holds.
        assert_eq!(csv_cell("03.15.2024", &shapes[2]), json!("03.15.2024"));
    }

    #[test]
    fn a_field_name_splits_into_its_words() {
        assert_eq!(id_name_rank("fileSHA256"), id_name_rank("sha256"));
        assert_eq!(id_name_rank("MD5Hash"), id_name_rank("md5"));
        assert!(id_name_rank("sha512") < id_name_rank("sha256"));
        assert!(id_name_rank("sha256") < id_name_rank("sha1"));
        assert!(id_name_rank("sha1") < id_name_rank("md5"));
        assert!(id_name_rank("md5") < id_name_rank("file_hash"));
        assert!(id_name_rank("file_hash") < id_name_rank("uuid"));
        assert!(id_name_rank("uuid") < id_name_rank("userID"));
        assert_eq!(id_name_rank("userID"), id_name_rank("user_id"));
        assert_eq!(id_name_rank("videoid"), id_name_rank("user_id"));
        assert!(id_name_rank("user_id") < id_name_rank("id_user"));
        assert!(id_name_rank("record_key") < id_name_rank("seq"));
        assert_eq!(id_name_rank("SequenceNumber"), id_name_rank("seq"));
        assert!(id_name_rank("seq") < id_name_rank("provider"));
        assert_eq!(id_name_rank("title"), UNNAMED_RANK);
    }

    fn chosen(csv_text: &str) -> (IdSpec, IdReason) {
        let analysis = analysis(csv_text, None);
        (analysis.id.spec, analysis.id.reason)
    }

    fn column(name: &str) -> IdSpec {
        IdSpec::Column(name.to_string())
    }

    /// A name says which column to prefer; only one filled and distinct in every scanned row
    /// can be chosen.
    #[test]
    fn the_id_is_the_best_named_column_that_is_unique() {
        assert_eq!(
            chosen("parent_sha1,seq,title\na,1,x\na,2,y\nb,3,z\n").0,
            column("seq")
        );
        assert_eq!(chosen("seq,SHA256\n1,a\n2,b\n").0, column("SHA256"));
        assert_eq!(chosen("user_id,uuid\n1,a\n,b\n3,c\n").0, column("uuid"));
        // Named exactly `id`, it is the id whatever it holds.
        assert_eq!(
            chosen("sha256,ID\na,1\nb,1\n"),
            (column("ID"), IdReason::Named)
        );
        // Failing a named one, the first whose values look like keys.
        assert_eq!(
            chosen("title,created,code\nA b,2024-01-01,X1\nC d,2024-01-02,X2\n").0,
            column("code")
        );
        // None unique: the best name, and the load says so.
        assert_eq!(
            chosen("kind,user_id\na,1\na,1\n"),
            (column("user_id"), IdReason::NameOnly)
        );
    }

    /// The hourly-readings case: no column is unique, a device and its hour are. The pair is
    /// found, suggested, and taken when named with --id.
    #[test]
    fn a_unique_pair_is_found_and_named_with_id() {
        let mut text = String::from("Hr,cmMacAddress,serialNumber,status\n");
        for hour in 0..3 {
            for device in 0..50 {
                text.push_str(&format!(
                    "2026-10-05 0{hour}:00:00,mac{device},sn{device},ok\n"
                ));
            }
        }
        let auto = analysis(&text, None);
        assert_eq!(auto.id.reason, IdReason::NameOnly);
        let pairs: Vec<(String, String)> = auto
            .profiler
            .unique_pairs()
            .into_iter()
            .map(|(a, b)| {
                let name = |i: usize| auto.profiler.columns[i].name.clone();
                (name(a), name(b))
            })
            .collect();
        assert!(
            pairs.contains(&("Hr".to_string(), "cmMacAddress".to_string())),
            "{pairs:?}"
        );
        assert!(auto.profiler.suggested_id().is_some());

        let spec = IdSpec::parse("Hr, cmMacAddress").expect("spec");
        let named = analysis(&text, Some(&spec));
        assert_eq!(named.id.reason, IdReason::Explicit);
        assert_eq!(named.shadow_name(), None);
        let schema = schema_from_analysis(&named).expect("schema");
        let fields = schema["fields"].as_object().expect("fields");
        // The id's columns stay fields of their own, returned apart; no shadow field.
        for name in ["Hr", "cmMacAddress"] {
            assert_eq!(fields[name]["is_shadow"], json!(false), "{name}");
            assert_eq!(fields[name]["indexed"], json!(true), "{name}");
        }
        // A column the source does not have is refused, not guessed at.
        let missing = IdSpec::parse("Hr,nope").expect("spec");
        assert!(auto.profiler.choose_id(Some(&missing), None).is_err());
    }

    /// The id `--id` names is followed through the scan as the load composes it, so a pair that
    /// repeats is reported before anything is overwritten.
    #[test]
    fn a_named_id_is_checked_by_the_scan() {
        let mut text = String::from("Hr,cmMacAddress,serialNumber\n");
        for hour in 0..2 {
            for device in 0..20 {
                // Two devices share a serial: the hour and serial pair repeats, the hour and MAC
                // pair does not.
                let serial = if device == 7 { 6 } else { device };
                text.push_str(&format!("0{hour}:00,mac{device},sn{serial}\n"));
            }
        }
        let track = |id: &str| {
            let spec = IdSpec::parse(id).expect("spec");
            let (headers, _) = profile(&text);
            let names: Vec<String> = headers.iter().map(|(n, _)| n.clone()).collect();
            let mut profiler = Profiler::new(&names, 8192);
            profiler.track_id(&spec);
            let mut reader = csv::ReaderBuilder::new().from_reader(text.as_bytes());
            for (n, record) in reader.records().enumerate() {
                profiler.observe_csv(&record.expect("record"), Location::Line(n as u64 + 2));
            }
            profiler.named_id_state().cloned()
        };
        assert_eq!(track("Hr,cmMacAddress"), Some(KeyState::Unique));

        // A named id is used as named, even when the scan finds a better one, which it offers
        // only as a suggestion.
        let spec = IdSpec::parse("serialNumber,Hr").expect("spec");
        let named = analysis(&text, Some(&spec));
        assert_eq!(named.id.spec, spec);
        assert_eq!(named.id.reason, IdReason::Explicit);
        assert_eq!(
            named.profiler.suggested_id().as_deref(),
            Some("Hr,cmMacAddress")
        );
        assert!(named.report("wifi.csv").contains("--id Hr,cmMacAddress"));
        assert_eq!(
            track("Hr,serialNumber"),
            Some(KeyState::Repeats {
                at: Location::Line(9),
                value: "00:00|sn6".to_string(),
            })
        );
    }

    #[test]
    fn a_composite_id_joins_its_parts_in_order() {
        assert_eq!(
            compose_id([
                Some("2026-10-05 06:00:00".to_string()),
                Some("00:7A".to_string())
            ]),
            Some("2026-10-05 06:00:00|00:7A".to_string())
        );
        assert_eq!(compose_id([Some("a".to_string()), None]), None);

        let mut ingest = CsvIngest::new(Vec::new(), Vec::new(), vec![0, 1], 10);
        let mut ledger_rows = |cells: &[&str], line| {
            let record = csv::StringRecord::from(cells.to_vec());
            let id = compose_id(ingest.id_columns.iter().map(|&i| {
                record
                    .get(i)
                    .map(str::trim)
                    .filter(|c| !c.is_empty())
                    .map(str::to_string)
            }));
            match id {
                Some(id) => {
                    ingest.ledger.admit(&id, Location::Line(line));
                }
                None => ingest.ledger.skip(Location::Line(line)),
            }
        };
        ledger_rows(&["h1", "m1"], 2);
        ledger_rows(&["h1", "m2"], 3);
        ledger_rows(&["h1", "m1"], 4);
        ledger_rows(&["h1", ""], 5);
        assert_eq!((ingest.ledger.repeats, ingest.ledger.skipped), (1, 1));
    }

    /// A load keys rows by the id the index records — its `id_fields`, or the shadow field of an
    /// index written before them — whatever the scan would pick. A source without those columns
    /// is refused rather than keyed another way.
    #[test]
    fn a_load_keeps_the_id_the_index_records() {
        let (_, profiler) = profile("seq,SHA1,Hr\n1,a,x\n2,b,x\n");
        let existing = profiler.choose_id(None, Some(&column("seq"))).expect("id");
        assert_eq!(
            (existing.spec, existing.reason),
            (column("seq"), IdReason::Existing)
        );
        let composite = IdSpec::parse("hr,sha1").expect("spec");
        let recorded = profiler.choose_id(None, Some(&composite)).expect("id");
        assert_eq!(
            recorded.spec,
            IdSpec::Composite(vec!["Hr".to_string(), "SHA1".to_string()])
        );
        assert!(profiler.choose_id(None, Some(&column("md5"))).is_err());
        // --id wins over what the index records.
        let named = profiler
            .choose_id(Some(&column("SHA1")), Some(&column("seq")))
            .expect("id");
        assert_eq!(named.reason, IdReason::Explicit);

        let existing = ExistingSchema {
            id_fields: vec!["Hr".to_string(), "cmMacAddress".to_string()],
            shadow_field: Some("ignored".to_string()),
            ..Default::default()
        };
        assert_eq!(
            existing.recorded_id(),
            IdSpec::parse("Hr,cmMacAddress").ok()
        );
        let legacy = ExistingSchema {
            shadow_field: Some("sha256".to_string()),
            ..Default::default()
        };
        assert_eq!(legacy.recorded_id(), Some(column("sha256")));
        assert!(same_id(
            &IdSpec::parse("hr,CMMACADDRESS").unwrap(),
            &IdSpec::parse("Hr,cmMacAddress").unwrap()
        ));
        assert!(!same_id(
            &IdSpec::parse("cmMacAddress,Hr").unwrap(),
            &IdSpec::parse("Hr,cmMacAddress").unwrap()
        ));
    }

    /// The schema records how its ids are made, in the order --id named the columns; and a
    /// schema rebuilt for another id keeps every field a person declared.
    #[test]
    fn a_schema_records_its_id_and_keeps_declared_fields_across_a_change() {
        let text =
            "Hr,mac,serial,ssids\n06,AA,s1,\"['x']\"\n06,BB,s2,\"['y']\"\n07,AA,s1,\"['x']\"\n";
        let spec = IdSpec::parse("mac,Hr").expect("spec");
        let schema = schema_from_analysis(&analysis(text, Some(&spec))).expect("schema");
        assert_eq!(schema["id_fields"], json!(["mac", "Hr"]));

        let existing = ExistingSchema {
            id_fields: vec!["serial".to_string()],
            shadow_field: Some("serial".to_string()),
            description: Some("hourly wifi".to_string()),
            fields: vec![
                json!({"name": "serial", "type": "text", "shadow": true}),
                json!({"name": "ssids", "type": "text", "tokenizer": "raw", "indexed": true,
                       "description": "SSIDs seen"}),
                json!({"name": "extra", "type": "i64", "indexed": true}),
            ],
            ..Default::default()
        };
        let rebuilt = schema_for_new_id(&analysis(text, Some(&spec)), &existing).expect("schema");
        let fields = &rebuilt["fields"];
        assert_eq!(rebuilt["id_fields"], json!(["mac", "Hr"]));
        // The old id's shadow is an ordinary field again; the edits survive.
        assert_eq!(fields["serial"]["is_shadow"], json!(false));
        assert_eq!(fields["ssids"]["tokenizer"], json!("raw"));
        assert_eq!(fields["ssids"]["description"], json!("SSIDs seen"));
        assert_eq!(fields["extra"]["field_type"], json!("i64"));
        assert_eq!(rebuilt["description"], json!("hourly wifi"));
    }

    /// A JSON source is profiled by its documents' values: `"12"` is a count, `"true"` a flag,
    /// and a field missing from earlier documents was empty in them.
    #[test]
    fn a_json_source_is_typed_by_its_values() {
        let mut profiler = Profiler::new(&[], 8192);
        for n in 0..4u64 {
            let active = ["true", "FALSE", ""][n as usize % 3];
            let mut doc = json!({
                "seq_no": n,
                "count": n.to_string(),
                "active": active,
                "valid": n % 2 == 0,
                "tags": ["a", "b"],
            });
            if n >= 2 {
                doc["late"] = json!("x");
            }
            profiler.observe_json(doc.as_object().unwrap(), Location::Document(n + 1));
        }
        profiler.finish();
        let choices: HashMap<String, FieldChoice> = profiler
            .columns
            .iter()
            .map(|c| c.name.clone())
            .zip(profiler.choices())
            .collect();
        assert_eq!(choices["count"].field_type, TantivyFieldType::I64);
        assert_eq!(choices["active"].field_type, TantivyFieldType::Boolean);
        assert!(choices["tags"].list);
        let id = profiler.choose_id(None, None).expect("id");
        assert_eq!(id.spec, column("seq_no"));
        assert!(matches!(
            profiler.columns[profiler.index_of("late").unwrap()].key_state(),
            KeyState::NotFilled { .. }
        ));
    }

    /// A file larger than the whole-file limit is read from the head until its columns are
    /// stable, then in blocks spread over the rest — which is where a sorted file keeps what its
    /// head does not show.
    #[test]
    fn a_large_file_is_read_across_its_whole_length() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("sorted.csv");
        let mut text = String::from("seq,reading\n");
        for n in 0..40_000 {
            // Whole numbers in the first half, decimals in the second.
            if n < 20_000 {
                text.push_str(&format!("{n:06},{}\n", n % 97));
            } else {
                text.push_str(&format!("{n:06},{}.5\n", n % 97));
            }
        }
        std::fs::write(&path, &text).expect("write");
        let data = SourceData::File {
            path: path.clone(),
            compression: Compression::None,
        };
        let limits = ScanLimits {
            whole_file_bytes: 1024,
            min_rows: 1_000,
            batch_rows: 500,
            ..Default::default()
        };
        let scan = scan_csv(&data, b',', &limits, None).expect("scan");
        assert!(!scan.summary.whole);
        assert!(
            scan.summary.head_rows < 20_000,
            "{}",
            scan.summary.head_rows
        );
        assert_eq!(scan.summary.spread_blocks, limits.blocks);
        assert_eq!(scan.profiler.choices()[1].field_type, TantivyFieldType::F64);
        let estimated = scan.summary.rows.expect("estimate");
        assert!((36_000..44_000).contains(&estimated), "{estimated}");

        // Under the limit, the file is read whole and its rows counted.
        let whole = scan_csv(&data, b',', &ScanLimits::default(), None).expect("scan");
        assert!(whole.summary.whole);
        assert_eq!(whole.summary.rows, Some(40_000));
    }

    /// Loading into an index that has a schema reads only the first batch of rows ahead, where a
    /// new index's scan reads the same file whole; a typed index without a recorded id samples it.
    #[test]
    fn a_load_into_a_typed_index_reads_only_its_first_batch() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("steady.csv");
        let mut text = String::from("seq,reading\n");
        for n in 0..150_000 {
            text.push_str(&format!("{n:06},{}.5\n", n % 97));
        }
        std::fs::write(&path, &text).expect("write");
        let data = SourceData::File {
            path,
            compression: Compression::None,
        };

        let limits = ScanLimits::first_batch();
        let first = scan_csv(&data, b',', &limits, None).expect("scan");
        assert!(!first.summary.whole);
        assert_eq!(first.summary.head_rows, limits.batch_rows);
        assert_eq!(first.summary.spread_blocks, 0);

        let sampled = scan_csv(&data, b',', &ScanLimits::sampled(), None).expect("scan");
        assert!(!sampled.summary.whole);
        assert_eq!(sampled.summary.stopped_by, "column types stable");
        assert!(
            sampled.summary.head_rows < 100_000,
            "{}",
            sampled.summary.head_rows
        );

        let whole = scan_csv(&data, b',', &ScanLimits::default(), None).expect("scan");
        assert!(whole.summary.whole);
        assert_eq!(whole.summary.rows, Some(150_000));
    }
}

#[cfg(test)]
mod parallel_tests {
    use super::super::ingest::{MAX_PARALLEL, check_parallel, parse_parallel_arg};

    #[test]
    fn parallel_is_one_unless_given_and_bounded_by_half_a_nodes_requests() {
        let (parallel, rest) = parse_parallel_arg(&["wifi", "kpi.csv"]).unwrap();
        assert_eq!((parallel, rest), (1, vec!["wifi", "kpi.csv"]));

        let (parallel, rest) = parse_parallel_arg(&["wifi", "--parallel", "4", "kpi.csv"]).unwrap();
        assert_eq!((parallel, rest), (4, vec!["wifi", "kpi.csv"]));

        assert_eq!(check_parallel(MAX_PARALLEL).unwrap(), 16);
        for refused in [0, MAX_PARALLEL + 1] {
            let err = check_parallel(refused).unwrap_err().to_string();
            assert!(err.contains("1 to 16"), "{err}");
        }
        assert!(parse_parallel_arg(&["--parallel", "many"]).is_err());
        assert!(parse_parallel_arg(&["--parallel"]).is_err());
    }
}
