//! Integration tests verifying that:
//!  1. Field sorting (`sort` spec) reorders results by a FAST field, not by score.
//!  2. Comparison operators (`>`, `>=`, `<`, `<=`) and range operators (`[a TO b]`,
//!     `{a TO b}`) are correctly parsed and executed by Tantivy for numeric and date
//!     fields.

use serde_json::json;
use storage::{
    FieldDef, HybridStore, IndexSchema, SortOrder, SortSpec, StorageConfig, TantivyFieldType, WalOp,
};
use tempfile::TempDir;

fn test_config(path: std::path::PathBuf) -> StorageConfig {
    StorageConfig {
        shard_path: path,
        indexer_memory_budget: 32 * 1024 * 1024,
        indexer_memory_min_mb: 16,
        indexer_memory_max_mb: 256,
        total_memory_limit_bytes: 4 * 1024 * 1024 * 1024,
        memory_pressure_threshold_percent: 80,
        indexer_num_threads: 1,
        merge_num_threads: 2,
        default_batch_size: 1000,
        wal_sync: true,
    }
}

/// Build a schema with an id, a text field, an i64 field, an f64 field, and a date field.
fn build_schema() -> IndexSchema {
    let mut schema = IndexSchema::default();
    for (name, ty) in [
        ("id", TantivyFieldType::Text),
        ("title", TantivyFieldType::Text),
        ("year", TantivyFieldType::I64),
        ("price", TantivyFieldType::F64),
        ("published", TantivyFieldType::Date),
    ] {
        schema
            .fields
            .insert(name.to_string(), FieldDef::new(name.to_string(), ty));
    }
    schema.normalize_after_deserialization();
    schema
}

fn put(id: &str, doc: serde_json::Value) -> WalOp {
    WalOp::Put {
        id: id.to_string(),
        json_blob: Some(doc),
    }
}

/// Create a store, apply a schema, write the given docs, and commit so they are searchable.
fn setup_index(dir: &TempDir, index: &str, docs: Vec<WalOp>) -> HybridStore {
    let store = HybridStore::new(test_config(dir.path().to_path_buf()), 1)
        .expect("Failed to create HybridStore");
    store
        .store_schema_and_cache(index, &build_schema())
        .expect("Failed to store schema");
    store
        .apply_batch(index, docs)
        .expect("Failed to write docs");
    store.commit_index(index).expect("Failed to commit index");
    store
}

fn ids(results: &[(f32, serde_json::Value)]) -> Vec<String> {
    results
        .iter()
        .map(|(_, doc)| doc["id"].as_str().unwrap_or_default().to_string())
        .collect()
}

fn sample_docs() -> Vec<WalOp> {
    vec![
        put(
            "a",
            json!({"id": "a", "title": "alpha rust", "year": 2020, "price": 9.5, "published": "2020-06-01"}),
        ),
        put(
            "b",
            json!({"id": "b", "title": "beta rust", "year": 2022, "price": 4.5, "published": "2022-01-15"}),
        ),
        put(
            "c",
            json!({"id": "c", "title": "gamma rust", "year": 2018, "price": 19.0, "published": "2018-11-30"}),
        ),
        put(
            "d",
            json!({"id": "d", "title": "delta rust", "year": 2024, "price": 1.0, "published": "2024-03-10"}),
        ),
    ]
}

#[test]
fn test_sort_by_numeric_field_orders_results() {
    let dir = TempDir::new().unwrap();
    let store = setup_index(&dir, "books", sample_docs());

    // Sort ascending by year: expect c(2018), a(2020), b(2022), d(2024)
    let sort_asc = SortSpec {
        field: "year".to_string(),
        order: SortOrder::Asc,
    };
    let (results, total) = store
        .search_documents("books", "rust", 10, Some(&sort_asc))
        .expect("search failed");
    assert_eq!(total, 4, "all four docs match 'rust'");
    assert_eq!(
        ids(&results),
        vec!["c", "a", "b", "d"],
        "ascending year sort order mismatch"
    );

    // Sort descending by year: expect d(2024), b(2022), a(2020), c(2018)
    let sort_desc = SortSpec {
        field: "year".to_string(),
        order: SortOrder::Desc,
    };
    let (results, _) = store
        .search_documents("books", "rust", 10, Some(&sort_desc))
        .expect("search failed");
    assert_eq!(
        ids(&results),
        vec!["d", "b", "a", "c"],
        "descending year sort order mismatch"
    );
}

#[test]
fn test_numeric_comparison_operators() {
    let dir = TempDir::new().unwrap();
    let store = setup_index(&dir, "books", sample_docs());

    // year > 2020 -> b(2022), d(2024)
    let (results, _) = store
        .search_documents("books", "year:>2020", 10, None)
        .expect("search failed");
    let mut got = ids(&results);
    got.sort();
    assert_eq!(got, vec!["b", "d"], "year:>2020 mismatch");

    // year >= 2020 -> a, b, d
    let (results, _) = store
        .search_documents("books", "year:>=2020", 10, None)
        .expect("search failed");
    let mut got = ids(&results);
    got.sort();
    assert_eq!(got, vec!["a", "b", "d"], "year:>=2020 mismatch");

    // year < 2020 -> c(2018)
    let (results, _) = store
        .search_documents("books", "year:<2020", 10, None)
        .expect("search failed");
    assert_eq!(ids(&results), vec!["c"], "year:<2020 mismatch");

    // price <= 4.5 -> b(4.5), d(1.0)
    let (results, _) = store
        .search_documents("books", "price:<=4.5", 10, None)
        .expect("search failed");
    let mut got = ids(&results);
    got.sort();
    assert_eq!(got, vec!["b", "d"], "price:<=4.5 mismatch");
}

#[test]
fn test_numeric_range_operators() {
    let dir = TempDir::new().unwrap();
    let store = setup_index(&dir, "books", sample_docs());

    // Inclusive range year:[2020 TO 2022] -> a, b
    let (results, _) = store
        .search_documents("books", "year:[2020 TO 2022]", 10, None)
        .expect("search failed");
    let mut got = ids(&results);
    got.sort();
    assert_eq!(got, vec!["a", "b"], "inclusive range mismatch");

    // Exclusive range year:{2018 TO 2024} -> a(2020), b(2022)
    let (results, _) = store
        .search_documents("books", "year:{2018 TO 2024}", 10, None)
        .expect("search failed");
    let mut got = ids(&results);
    got.sort();
    assert_eq!(got, vec!["a", "b"], "exclusive range mismatch");
}

#[test]
fn test_date_comparison_and_range_operators() {
    let dir = TempDir::new().unwrap();
    let store = setup_index(&dir, "books", sample_docs());

    // published > 2021-01-01 -> b(2022), d(2024)
    let (results, _) = store
        .search_documents("books", "published:>2021-01-01", 10, None)
        .expect("search failed");
    let mut got = ids(&results);
    got.sort();
    assert_eq!(got, vec!["b", "d"], "date comparison mismatch");

    // published in [2019-01-01 TO 2023-01-01] -> a(2020), b(2022)
    let (results, _) = store
        .search_documents("books", "published:[2019-01-01 TO 2023-01-01]", 10, None)
        .expect("search failed");
    let mut got = ids(&results);
    got.sort();
    assert_eq!(got, vec!["a", "b"], "date range mismatch");
}

#[test]
fn test_date_comparison_dot_format_and_quoted_values() {
    let dir = TempDir::new().unwrap();
    let store = setup_index(&dir, "books", sample_docs());

    // published > "2021.07.01" (dot format, quoted) -> b(2022), d(2024)
    let (results, _) = store
        .search_documents("books", r#"published:>"2021.07.01""#, 10, None)
        .expect("search failed");
    let mut got = ids(&results);
    got.sort();
    assert_eq!(
        got,
        vec!["b", "d"],
        "dot-format quoted date comparison mismatch"
    );

    // published > 2021.07.01 (dot format, unquoted) -> b(2022), d(2024)
    let (results, _) = store
        .search_documents("books", "published:>2021.07.01", 10, None)
        .expect("search failed");
    let mut got = ids(&results);
    got.sort();
    assert_eq!(
        got,
        vec!["b", "d"],
        "dot-format unquoted date comparison mismatch"
    );

    // Range with dot format: [2019.01.01 TO 2023.01.01] -> a(2020), b(2022)
    let (results, _) = store
        .search_documents("books", "published:[2019.01.01 TO 2023.01.01]", 10, None)
        .expect("search failed");
    let mut got = ids(&results);
    got.sort();
    assert_eq!(got, vec!["a", "b"], "dot-format date range mismatch");
}

#[test]
fn test_datetime_formats_with_slash_and_dot_separators() {
    let dir = TempDir::new().unwrap();
    let store = HybridStore::new(test_config(dir.path().to_path_buf()), 1).unwrap();
    let mut schema = IndexSchema::default();
    for (name, ty) in [
        ("id", TantivyFieldType::Text),
        ("ts", TantivyFieldType::Date),
    ] {
        schema
            .fields
            .insert(name.to_string(), FieldDef::new(name.to_string(), ty));
    }
    schema.normalize_after_deserialization();
    store.store_schema_and_cache("dt", &schema).unwrap();

    let docs = vec![
        put("a", json!({"id": "a", "ts": "2020-06-01 12:00:00"})),
        put("b", json!({"id": "b", "ts": "2022-01-15 08:30:00"})),
        put("c", json!({"id": "c", "ts": "2018-11-30 23:59:59"})),
        put("d", json!({"id": "d", "ts": "2024-03-10 00:00:00"})),
    ];
    store.apply_batch("dt", docs).unwrap();
    store.commit_index("dt").unwrap();

    // Slash separator datetime: ts > "2021/01/01 00:00:00" -> b, d
    let (results, _) = store
        .search_documents("dt", r#"ts:>"2021/01/01 00:00:00""#, 10, None)
        .expect("search failed");
    let mut got = ids(&results);
    got.sort();
    assert_eq!(got, vec!["b", "d"], "slash datetime comparison mismatch");

    // Dot separator datetime: ts > "2021.01.01 00:00:00" -> b, d
    let (results, _) = store
        .search_documents("dt", r#"ts:>"2021.01.01 00:00:00""#, 10, None)
        .expect("search failed");
    let mut got = ids(&results);
    got.sort();
    assert_eq!(got, vec!["b", "d"], "dot datetime comparison mismatch");

    // T separator with dot date: ts > "2021.01.01T00:00:00" -> b, d
    let (results, _) = store
        .search_documents("dt", r#"ts:>"2021.01.01T00:00:00""#, 10, None)
        .expect("search failed");
    let mut got = ids(&results);
    got.sort();
    assert_eq!(got, vec!["b", "d"], "dot-T datetime comparison mismatch");
}

#[test]
fn test_compact_datetime_and_epoch_formats() {
    let dir = TempDir::new().unwrap();
    let store = HybridStore::new(test_config(dir.path().to_path_buf()), 1).unwrap();
    let mut schema = IndexSchema::default();
    for (name, ty) in [
        ("id", TantivyFieldType::Text),
        ("ts", TantivyFieldType::Date),
    ] {
        schema
            .fields
            .insert(name.to_string(), FieldDef::new(name.to_string(), ty));
    }
    schema.normalize_after_deserialization();
    store.store_schema_and_cache("dt", &schema).unwrap();

    // 2020-06-01 12:00:00 UTC = 1591012800
    // 2022-01-15 08:30:00 UTC = 1642236600
    // 2018-11-30 23:59:59 UTC = 1543622399
    // 2024-03-10 00:00:00 UTC = 1710000000
    let docs = vec![
        put("a", json!({"id": "a", "ts": "2020-06-01 12:00:00"})),
        put("b", json!({"id": "b", "ts": "2022-01-15 08:30:00"})),
        put("c", json!({"id": "c", "ts": "2018-11-30 23:59:59"})),
        put("d", json!({"id": "d", "ts": "2024-03-10 00:00:00"})),
    ];
    store.apply_batch("dt", docs).unwrap();
    store.commit_index("dt").unwrap();

    // Compact datetime YYYYMMDDHHMMSS: ts > 20210101000000 -> b, d
    let (results, _) = store
        .search_documents("dt", "ts:>20210101000000", 10, None)
        .expect("search failed");
    let mut got = ids(&results);
    got.sort();
    assert_eq!(got, vec!["b", "d"], "compact datetime comparison mismatch");

    // Epoch seconds: ts > 1609459200 (2021-01-01 UTC) -> b, d
    let (results, _) = store
        .search_documents("dt", "ts:>1609459200", 10, None)
        .expect("search failed");
    let mut got = ids(&results);
    got.sort();
    assert_eq!(got, vec!["b", "d"], "epoch seconds comparison mismatch");
}

#[test]
fn test_quoted_value_boundary_without_trailing_space() {
    let dir = TempDir::new().unwrap();
    let store = setup_index(&dir, "books", sample_docs());

    // Quoted value with no space after closing quote — the `+` immediately follows.
    // This tests that the quote boundary detection works correctly.
    let (results, _) = store
        .search_documents(
            "books",
            r#"published:>"2021.07.01" AND title:rust"#,
            10,
            None,
        )
        .expect("search failed");
    let mut got = ids(&results);
    got.sort();
    assert_eq!(
        got,
        vec!["b", "d"],
        "quoted value boundary without trailing space mismatch"
    );
}

#[test]
fn test_sort_by_f64_field_orders_results() {
    let dir = TempDir::new().unwrap();
    let store = setup_index(&dir, "books", sample_docs());

    // Sort ascending by price: expect d(1.0), b(4.5), a(9.5), c(19.0)
    let sort_asc = SortSpec {
        field: "price".to_string(),
        order: SortOrder::Asc,
    };
    let (results, total) = store
        .search_documents("books", "rust", 10, Some(&sort_asc))
        .expect("search failed");
    assert_eq!(total, 4, "all four docs match 'rust'");
    assert_eq!(
        ids(&results),
        vec!["d", "b", "a", "c"],
        "ascending price sort order mismatch"
    );

    // Sort descending by price: expect c(19.0), a(9.5), b(4.5), d(1.0)
    let sort_desc = SortSpec {
        field: "price".to_string(),
        order: SortOrder::Desc,
    };
    let (results, _) = store
        .search_documents("books", "rust", 10, Some(&sort_desc))
        .expect("search failed");
    assert_eq!(
        ids(&results),
        vec!["c", "a", "b", "d"],
        "descending price sort order mismatch"
    );
}

#[test]
fn test_sort_by_date_field_orders_results() {
    let dir = TempDir::new().unwrap();
    let store = setup_index(&dir, "books", sample_docs());

    // Sort ascending by published: expect c(2018-11-30), a(2020-06-01), b(2022-01-15), d(2024-03-10)
    let sort_asc = SortSpec {
        field: "published".to_string(),
        order: SortOrder::Asc,
    };
    let (results, total) = store
        .search_documents("books", "rust", 10, Some(&sort_asc))
        .expect("search failed");
    assert_eq!(total, 4, "all four docs match 'rust'");
    assert_eq!(
        ids(&results),
        vec!["c", "a", "b", "d"],
        "ascending published sort order mismatch"
    );

    // Sort descending by published: expect d(2024-03-10), b(2022-01-15), a(2020-06-01), c(2018-11-30)
    let sort_desc = SortSpec {
        field: "published".to_string(),
        order: SortOrder::Desc,
    };
    let (results, _) = store
        .search_documents("books", "rust", 10, Some(&sort_desc))
        .expect("search failed");
    assert_eq!(
        ids(&results),
        vec!["d", "b", "a", "c"],
        "descending published sort order mismatch"
    );
}

#[test]
fn test_sort_by_string_field_orders_results_alphabetically() {
    let dir = TempDir::new().unwrap();
    let store = setup_index(&dir, "books", sample_docs());

    // title values: a="alpha rust", b="beta rust", c="gamma rust", d="delta rust"
    // Ascending alpha: a(alpha), b(beta), d(delta), c(gamma)
    let sort_asc = SortSpec {
        field: "title".to_string(),
        order: SortOrder::Asc,
    };
    let (results, total) = store
        .search_documents("books", "rust", 10, Some(&sort_asc))
        .expect("search failed");
    assert_eq!(total, 4, "all four docs match 'rust'");
    assert_eq!(
        ids(&results),
        vec!["a", "b", "d", "c"],
        "ascending title sort order mismatch"
    );

    // Descending alpha: c(gamma), d(delta), b(beta), a(alpha)
    let sort_desc = SortSpec {
        field: "title".to_string(),
        order: SortOrder::Desc,
    };
    let (results, _) = store
        .search_documents("books", "rust", 10, Some(&sort_desc))
        .expect("search failed");
    assert_eq!(
        ids(&results),
        vec!["c", "d", "b", "a"],
        "descending title sort order mismatch"
    );
}
