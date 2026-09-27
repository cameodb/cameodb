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
        let json = SourceLines::Documents { first: 8001 };
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
            &SourceLines::Documents { first: 1 },
            &mut sent,
            &mut failed,
        );
        assert_eq!((sent, failed), (100, 3900));
        assert_eq!(sent + failed, 4000, "written plus refused is what was sent");
    }

    /// Each batch a JSON source emits knows the number of its first document in the source.
    #[test]
    fn json_batches_are_numbered_through_the_source() {
        let mut pipeline = JsonIngestPipeline::new(2, true);
        let mut events = Vec::new();
        for n in 0..5 {
            pipeline
                .push(
                    &serde_json::json!({"id": format!("d{n}"), "n": n}),
                    &mut events,
                )
                .expect("push");
        }
        pipeline.finish(&mut events).expect("finish");
        let firsts: Vec<u64> = events
            .iter()
            .filter_map(|event| match event {
                JsonIngestEvent::DataBatch { first_document, .. } => Some(*first_document),
                JsonIngestEvent::CreateSchema(_) => None,
            })
            .collect();
        assert_eq!(firsts, vec![1, 3, 5]);
    }
}

/// How a CSV cell is read: by the field it lands in, and by its column's date order.
#[cfg(test)]
mod csv_cell_tests {
    use super::*;
    use serde_json::json;
    use std::collections::HashMap;
    use storage::TantivyFieldType;

    fn typed(field_type: TantivyFieldType) -> ColumnShape {
        ColumnShape {
            field_type: Some(field_type),
            dates: DateOrders::default(),
        }
    }

    fn dated(slash: DateOrder, dot: DateOrder) -> ColumnShape {
        ColumnShape {
            field_type: Some(TantivyFieldType::Date),
            dates: DateOrders { slash, dot },
        }
    }

    fn rows(csv_text: &str) -> Vec<csv::StringRecord> {
        csv::ReaderBuilder::new()
            .delimiter(b';')
            .has_headers(false)
            .from_reader(csv_text.as_bytes())
            .records()
            .collect::<Result<_, _>>()
            .expect("rows")
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
            for marker in ["NA", "n/a", "#N/A", "NaN", "null", "None", "-"] {
                assert_eq!(
                    csv_cell(marker, &typed(field_type.clone())),
                    JsonValue::Null,
                    "{marker} under {field_type:?}"
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
        // A column no field describes is read by its look, as before.
        assert_eq!(csv_cell("007", &ColumnShape::default()), json!(7));
    }

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
    }

    /// A slash column whose sample has a date only day-first can read, and none only month-first
    /// can, is day first — and its ambiguous dates are read that way too.
    #[test]
    fn a_slash_column_that_writes_day_first_is_read_day_first() {
        let sample = rows("a;03/04/2024;x\nb;15/03/2024;y\nc;01/02/2024 16:13;z\n");
        let orders = date_orders_by_column(&sample, 3);
        assert_eq!(orders[1].slash, DateOrder::DayFirst);
        assert_eq!(orders[0], DateOrders::default());
        assert_eq!(orders[2], DateOrders::default());

        let shape = dated(DateOrder::DayFirst, DateOrder::DayFirst);
        assert_eq!(csv_cell("03/04/2024", &shape), json!("2024-04-03"));
        assert_eq!(csv_cell("15/03/2024", &shape), json!("2024-03-15"));
        assert_eq!(
            csv_cell("01/02/2024 16:13", &shape),
            json!("2024-02-01 16:13")
        );
        // Not a slash date: sent as written, as in any date column.
        assert_eq!(csv_cell("2024-03-15", &shape), json!("2024-03-15"));
        // A dotted date the node reads as meant is sent as written.
        assert_eq!(csv_cell("15.03.2024", &shape), json!("15.03.2024"));
    }

    /// The mirror for dots: the node reads them day first, and a column whose sample has a date
    /// only month-first can read, and none only day-first can, is month first.
    #[test]
    fn a_dotted_column_that_writes_month_first_is_read_month_first() {
        let sample = rows("03.04.2024\n03.15.2024\n");
        let orders = date_orders_by_column(&sample, 1);
        assert_eq!(orders[0].dot, DateOrder::MonthFirst);
        assert_eq!(orders[0].slash, DateOrder::MonthFirst);

        let shape = dated(DateOrder::MonthFirst, DateOrder::MonthFirst);
        assert_eq!(csv_cell("03.04.2024", &shape), json!("2024-03-04"));
        assert_eq!(
            csv_cell("03.15.2024 16:13:13", &shape),
            json!("2024-03-15 16:13:13")
        );
    }

    /// With no evidence, or evidence both ways, a column keeps the node's order for that
    /// separator, and its dates go as written for the node to read.
    #[test]
    fn a_column_keeps_the_nodes_order_unless_its_sample_says_otherwise() {
        for sample in [
            "03/04/2024\n05/06/2024\n",
            "03/15/2024\n03/04/2024\n",
            "15/03/2024\n03/15/2024\n",
            "03.04.2024\n15.03.2024\n",
            "15.03.2024\n03.15.2024\n",
        ] {
            assert_eq!(
                date_orders_by_column(&rows(sample), 1),
                vec![DateOrders::default()],
                "{sample:?}"
            );
        }
        let as_the_node_reads = typed(TantivyFieldType::Date);
        assert_eq!(
            csv_cell("03/04/2024", &as_the_node_reads),
            json!("03/04/2024")
        );
        assert_eq!(
            csv_cell("03.04.2024", &as_the_node_reads),
            json!("03.04.2024")
        );
    }

    /// Each separator is judged on its own evidence.
    #[test]
    fn slashes_and_dots_in_one_column_are_judged_apart() {
        let sample = rows("15/03/2024\n03.15.2024\n");
        assert_eq!(
            date_orders_by_column(&sample, 1),
            vec![DateOrders {
                slash: DateOrder::DayFirst,
                dot: DateOrder::MonthFirst,
            }]
        );
    }

    /// The order is only taken for a date field, so a text column of slash dates keeps them as
    /// written.
    #[test]
    fn only_a_date_column_is_reordered() {
        let headers = vec![("when".to_string(), None), ("note".to_string(), None)];
        let field_types = HashMap::from([
            ("when".to_string(), TantivyFieldType::Date),
            ("note".to_string(), TantivyFieldType::Text),
        ]);
        let day_first = DateOrders {
            slash: DateOrder::DayFirst,
            dot: DateOrder::DayFirst,
        };
        let shapes = column_shapes(&headers, &field_types, &[day_first, day_first]);
        assert_eq!(shapes[0].dates, day_first);
        assert_eq!(shapes[1].dates, DateOrders::default());
    }

    /// With no schema, inference sees a day-first column's dates as dates. `15/03/2024` is text
    /// to the node, so the column would have been typed text and never sorted.
    #[test]
    fn a_day_first_sample_infers_a_date_field() {
        let headers = vec![("id".to_string(), None), ("when".to_string(), None)];
        let detection = detect_id_field(&headers);
        let sample = rows("a;15/03/2024\nb;03/04/2024\nc;NA\n");
        let date_orders = date_orders_by_column(&sample, 2);
        let schema =
            csv_sample_schema(&headers, &detection, &sample, &date_orders).expect("schema");
        assert_eq!(
            schema_field_types(&schema).get("when"),
            Some(&TantivyFieldType::Date)
        );
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
