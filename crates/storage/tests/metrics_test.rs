//! The lowest and highest value of a field over what a query matches (ROADMAP K1): read from the
//! fast column, exact in the column's own type, over the query's matching set and nothing else.

use serde_json::{Value as JsonValue, json};
use storage::{
    FieldMetrics, HybridStore, IndexSchema, MetricValue, MetricsSpec, SearchOptions, StorageConfig,
    StoreError, WalOp,
};
use tempfile::TempDir;

const INDEX: &str = "readings";

fn config(path: &std::path::Path) -> StorageConfig {
    StorageConfig {
        shard_path: path.to_path_buf(),
        default_batch_size: 100_000,
        ..StorageConfig::default()
    }
}

/// Past 2⁵³, where an `f64` can no longer tell it from its neighbour.
const BIG: u64 = 9_007_199_254_740_993;

fn store() -> (TempDir, HybridStore) {
    let temp = TempDir::new().expect("temp dir");
    let store = HybridStore::new(config(temp.path()), 1).expect("store");
    let mut schema: IndexSchema = serde_json::from_value(json!({"fields": {
        "status": {"type": "string"},
        "title": {"type": "text"},
        "delta": {"type": "i64"},
        "count": {"type": "u64"},
        "score": {"type": "f64"},
        "seen": {"type": "date"},
        "slow": {"type": "i64", "fast": false},
        "readings": {"type": "i64"},
    }}))
    .expect("schema");
    schema.normalize_after_deserialization();
    store
        .store_schema_and_cache(INDEX, &schema)
        .expect("store schema");
    let docs = [
        (
            "a",
            json!({"status": "active", "title": "first", "delta": -40, "count": 7,
                   "score": 1.5, "seen": "2026-01-10T08:00:00Z", "slow": 1, "readings": [3, 90]}),
        ),
        (
            "b",
            json!({"status": "active", "title": "second", "delta": 12, "count": BIG,
                   "score": -2.25, "seen": "2026-03-01T00:00:00Z", "slow": 2, "readings": [-8]}),
        ),
        (
            "c",
            json!({"status": "retired", "title": "third", "delta": -900, "count": 1,
                   "score": 99.0, "seen": "2025-06-30T23:59:59Z", "slow": 3}),
        ),
        // Holds none of the measured fields: it matches, and changes no extreme.
        ("d", json!({"status": "active", "title": "fourth"})),
    ];
    let ops = docs
        .iter()
        .map(|(id, body)| WalOp::Put {
            id: id.to_string(),
            json_blob: Some(body.clone()),
        })
        .collect();
    store.apply_batch(INDEX, ops).expect("write");
    store.commit_index(INDEX).expect("commit");
    (temp, store)
}

fn spec(min: &[&str], max: &[&str]) -> MetricsSpec {
    MetricsSpec {
        min: min.iter().map(|s| s.to_string()).collect(),
        max: max.iter().map(|s| s.to_string()).collect(),
    }
}

fn measure(store: &HybridStore, query: &str, limit: usize, spec: &MetricsSpec) -> FieldMetrics {
    store
        .search_keys(
            INDEX,
            query,
            limit,
            None,
            &SearchOptions {
                metrics: Some(spec),
                ..Default::default()
            },
        )
        .expect("search")
        .metrics
        .expect("metrics were asked for")
}

fn refusal(store: &HybridStore, field: &str) -> StoreError {
    store
        .search_keys(
            INDEX,
            "*",
            0,
            None,
            &SearchOptions {
                metrics: Some(&spec(&[field], &[])),
                ..Default::default()
            },
        )
        .expect_err("refused")
}

#[test]
fn every_numeric_type_and_a_date_answer_exactly_over_what_the_query_matched() {
    let (_temp, store) = store();
    let fields = ["delta", "count", "score", "seen"];
    let metrics = measure(&store, "status:active", 0, &spec(&fields, &fields));

    assert_eq!(
        metrics.min["delta"],
        Some(MetricValue::I64(-40)),
        "c is retired"
    );
    assert_eq!(metrics.max["delta"], Some(MetricValue::I64(12)));
    assert_eq!(metrics.min["count"], Some(MetricValue::U64(7)));
    assert_eq!(
        metrics.max["count"],
        Some(MetricValue::U64(BIG)),
        "an integer past 2^53 comes back as itself, not as its f64 neighbour"
    );
    assert_eq!(metrics.min["score"], Some(MetricValue::F64(-2.25)));
    assert_eq!(metrics.max["score"], Some(MetricValue::F64(1.5)));

    let rendered = metrics.to_json();
    assert_eq!(rendered["min"]["seen"], json!("2026-01-10T08:00:00Z"));
    assert_eq!(rendered["max"]["seen"], json!("2026-03-01T00:00:00Z"));
    assert_eq!(rendered["max"]["count"], json!(BIG));
}

#[test]
fn a_page_and_a_count_carry_the_same_metrics_and_none_unless_asked() {
    let (_temp, store) = store();
    let asked = spec(&["delta"], &["delta"]);
    assert_eq!(
        measure(&store, "*", 0, &asked),
        measure(&store, "*", 2, &asked)
    );
    assert_eq!(
        measure(&store, "*", 0, &asked).min["delta"],
        Some(MetricValue::I64(-900))
    );
    let plain = store
        .search_keys(INDEX, "*", 2, None, &SearchOptions::default())
        .expect("search");
    assert!(plain.metrics.is_none());
}

#[test]
fn an_array_counts_every_element_and_a_match_with_no_value_is_null_not_zero() {
    let (_temp, store) = store();
    let metrics = measure(&store, "*", 0, &spec(&["readings"], &["readings"]));
    assert_eq!(metrics.min["readings"], Some(MetricValue::I64(-8)));
    assert_eq!(metrics.max["readings"], Some(MetricValue::I64(90)));

    let none = measure(&store, "title:fourth", 0, &spec(&["delta"], &["score"]));
    assert_eq!(none.min["delta"], None);
    assert_eq!(none.max["score"], None);
    assert_eq!(
        none.to_json(),
        json!({"min": {"delta": null}, "max": {"score": null}})
    );
    let nothing = measure(&store, "status:nobody", 0, &spec(&["delta"], &[]));
    assert_eq!(nothing.min["delta"], None);
}

#[test]
fn a_deleted_document_is_no_longer_an_extreme() {
    let (_temp, store) = store();
    store
        .apply_batch(INDEX, vec![WalOp::Delete { id: "c".into() }])
        .expect("delete");
    store.commit_index(INDEX).expect("commit");
    let metrics = measure(&store, "*", 0, &spec(&["delta"], &["score"]));
    assert_eq!(metrics.min["delta"], Some(MetricValue::I64(-40)));
    assert_eq!(metrics.max["score"], Some(MetricValue::F64(1.5)));
}

#[test]
fn a_field_with_no_order_or_no_column_is_refused_rather_than_answered_null() {
    let (_temp, store) = store();
    for field in ["status", "title"] {
        let message = refusal(&store, field).to_string();
        assert!(
            message.contains("min and max apply to"),
            "{field}: {message}"
        );
    }
    let message = refusal(&store, "slow").to_string();
    assert!(message.contains("no fast column"), "{message}");
    assert!(matches!(
        refusal(&store, "missing"),
        StoreError::FieldNotFound(_)
    ));
}

#[test]
fn shard_answers_merge_into_the_lowest_low_and_the_highest_high() {
    let mut one = FieldMetrics::default();
    one.min.insert("n".into(), Some(MetricValue::I64(5)));
    one.max.insert("n".into(), None);
    let mut two = FieldMetrics::default();
    two.min.insert("n".into(), Some(MetricValue::I64(-3)));
    two.max.insert("n".into(), Some(MetricValue::I64(40)));
    let mut three = FieldMetrics::default();
    three.min.insert("n".into(), None);
    three.max.insert("n".into(), Some(MetricValue::I64(7)));

    one.merge(&two);
    one.merge(&three);
    assert_eq!(one.min["n"], Some(MetricValue::I64(-3)));
    assert_eq!(one.max["n"], Some(MetricValue::I64(40)));
}

#[test]
fn rendered_node_answers_merge_exactly_and_by_instant() {
    let mut into: JsonValue = json!({
        "min": {"count": 9_007_199_254_740_995u64, "seen": "2026-03-01T00:00:00Z", "none": null},
        "max": {"score": 1.5}
    });
    storage::merge_metrics_json(
        &mut into,
        &json!({
            "min": {"count": BIG, "seen": "2025-12-31T23:00:00-05:00", "none": null},
            "max": {"score": 2.0, "other": 4}
        }),
    );
    assert_eq!(
        into,
        json!({
            "min": {"count": BIG, "seen": "2025-12-31T23:00:00-05:00", "none": null},
            "max": {"score": 2.0, "other": 4}
        }),
        "integers past 2^53 compare exactly, and a date by its instant, not its spelling"
    );
}
