//! What tantivy does with several values under one field — the assumption the write path rests on.
//!
//! Not a test of this crate. It pins a property of the engine underneath it, because the write
//! path reads a list as several values of a field and would be silently wrong if tantivy did not
//! store them that way: the write would succeed, the column would be short, and only a query
//! that never matches would show it. The belief this replaced — that a numeric or date field
//! holds one value and a list has to be reduced before it can be stored — was wrong, and cost
//! every multivalued field in an index its column before anyone asked tantivy directly.
//!
//! Worth keeping rather than deleting after the answer, since it is an upstream property: a
//! version bump that narrows it should fail here, next to the reason, rather than in whichever
//! query stops matching first.

use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::{FAST, INDEXED, STORED, STRING, Schema};
use tantivy::{DateTime, Index, TantivyDocument, doc};

#[test]
fn a_numeric_or_date_field_takes_several_values() {
    let mut builder = Schema::builder();
    let id = builder.add_text_field("id", STRING | STORED);
    let n = builder.add_i64_field("n", INDEXED | FAST);
    let f = builder.add_f64_field("f", INDEXED | FAST);
    let b = builder.add_bool_field("b", INDEXED);
    let d = builder.add_date_field("d", INDEXED | FAST);
    let schema = builder.build();

    let index = Index::create_in_ram(schema.clone());
    let mut writer = index.writer_with_num_threads(1, 15_000_000).unwrap();

    // One document, several values under each single-valued-looking field.
    let mut multi = doc!(id => "multi");
    multi.add_i64(n, 9);
    multi.add_i64(n, 12);
    multi.add_f64(f, 1.5);
    multi.add_f64(f, 2.5);
    multi.add_bool(b, false);
    multi.add_bool(b, true);
    multi.add_date(d, DateTime::from_timestamp_secs(1_692_440_696)); // 2023-08-19T10:24:56Z
    multi.add_date(d, DateTime::from_timestamp_secs(1_692_440_790)); // 2023-08-19T10:26:30Z
    writer.add_document(multi).unwrap();

    // And a plain single-valued one alongside, to be sure the segment is coherent.
    let mut single = doc!(id => "single");
    single.add_i64(n, 100);
    single.add_date(d, DateTime::from_timestamp_secs(1_700_000_000));
    writer.add_document(single).unwrap();

    writer.commit().unwrap();

    let reader = index.reader().unwrap();
    let searcher = reader.searcher();
    let parser = QueryParser::for_index(&index, vec![]);

    let hits = |q: &str| {
        let query = parser.parse_query(q).unwrap_or_else(|e| panic!("{q}: {e}"));
        searcher
            .search(&query, &TopDocs::with_limit(10).order_by_score())
            .unwrap()
            .into_iter()
            .map(|(_, addr)| {
                let d: TantivyDocument = searcher.doc(addr).unwrap();
                use tantivy::schema::Value as _;
                d.get_first(id).unwrap().as_str().unwrap().to_string()
            })
            .collect::<Vec<_>>()
    };

    // Every value of the multivalued document is queryable, exactly and by range.
    assert_eq!(hits("n:9"), vec!["multi"], "the first i64 value matches");
    assert_eq!(hits("n:12"), vec!["multi"], "the second i64 value matches");
    assert_eq!(
        hits("n:[10 TO 20]"),
        vec!["multi"],
        "a range over the second value matches"
    );
    assert_eq!(hits("f:1.5"), vec!["multi"]);
    assert_eq!(hits("f:2.5"), vec!["multi"]);
    assert_eq!(hits("b:true"), vec!["multi"]);
    assert_eq!(hits("b:false"), vec!["multi"]);
    assert_eq!(
        hits("d:[2023-08-19T10:26:00Z TO 2023-08-19T10:27:00Z]"),
        vec!["multi"],
        "a range over the later of two dates matches"
    );
    assert_eq!(
        hits("d:[2023-08-19T10:24:00Z TO 2023-08-19T10:25:00Z]"),
        vec!["multi"],
        "and so does one over the earlier"
    );

    // The fast column holds both values, and reports a cardinality that says so.
    let segment = searcher.segment_reader(0);
    let column = segment.fast_fields().i64("n").unwrap();
    let all: Vec<i64> = column.values_for_doc(0).collect();
    assert_eq!(all, vec![9, 12], "the fast column keeps every value");
    assert_eq!(
        column.first(0),
        Some(9),
        "and `first` is what a single-value reader sees — insertion order, not the largest"
    );
    println!(
        "cardinality of a multivalued fast column: {:?}",
        column.index.get_cardinality()
    );

    let dates = segment.fast_fields().date("d").unwrap();
    assert_eq!(
        dates.values_for_doc(0).count(),
        2,
        "dates are multivalued too"
    );

    // Sorting is where a single value has to be chosen: TopDocs::order_by_fast_field reads one.
    let by_n = searcher
        .search(
            &parser.parse_query("n:[0 TO 1000]").unwrap(),
            &TopDocs::with_limit(10).order_by_fast_field::<i64>("n", tantivy::Order::Desc),
        )
        .unwrap();
    println!(
        "order_by_fast_field(n desc) sort keys: {:?}",
        by_n.iter().map(|(k, _)| *k).collect::<Vec<_>>()
    );
}
