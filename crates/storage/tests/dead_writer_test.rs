//! A Tantivy writer whose indexing thread has died, and the schema disagreement that killed one.
//!
//! Tantivy fixes a column's type when the index is built, and its indexing thread fails on a
//! value of any other kind by exiting. From then on every `add_document` on that writer answers
//! "An index writer was killed" and every commit fails — so one bad value made an index
//! unwritable on the shard until the process restarted, and nothing buffered since the last
//! commit was ever committed. Loading `booksummaries.tsv` after `delete --delete-schema` lost
//! 15,000 of 16,559 documents to exactly this: a re-minted schema typed a built date column
//! `I64`, and the first integer written under it killed the writer.
//!
//! Two properties close it. A value is added by the column the index built, whatever the stored
//! schema declares; and a writer that dies anyway — an I/O error — is retired, so the next use
//! reopens the index and replays from its last commit what died with it.

use serde_json::json;
use storage::{FieldDef, HybridStore, IndexSchema, StorageConfig, TantivyFieldType, WalOp};
use tempfile::TempDir;

fn config(shard_path: &std::path::Path) -> StorageConfig {
    StorageConfig {
        shard_path: shard_path.to_path_buf(),
        max_open_indexes: 16,
        indexer_memory_budget: 16 * 1024 * 1024,
        indexer_memory_min_mb: 16,
        indexer_memory_max_mb: 64,
        total_memory_limit_bytes: 1024 * 1024 * 1024,
        memory_pressure_threshold_percent: 80,
        indexer_num_threads: 1,
        merge_num_threads: 1,
        default_batch_size: 100_000,
        wal_sync: true,
        commit_interval_ms: 0,
        query: Default::default(),
    }
}

/// `title` text and `when` a date, built as declared.
fn dated_schema(when: TantivyFieldType) -> IndexSchema {
    let mut schema = IndexSchema::default();
    for (name, field_type) in [
        ("id", TantivyFieldType::Text),
        ("title", TantivyFieldType::Text),
        ("when", when),
    ] {
        schema.fields.insert(
            name.to_string(),
            FieldDef::new(name.to_string(), field_type),
        );
    }
    schema.normalize_after_deserialization();
    schema
}

fn put(id: &str, when: serde_json::Value) -> WalOp {
    WalOp::Put {
        id: id.to_string(),
        json_blob: Some(json!({ "title": format!("book {id}"), "when": when })),
    }
}

fn searchable(store: &HybridStore, index: &str) -> usize {
    store
        .search_documents(index, "title:book", 1000, None)
        .expect("search")
        .total_hits
}

/// Kill the index's writer the way the re-minted schema did: a value of the wrong kind for a
/// built column, added below the store so no safeguard of its own can intervene.
fn poison(store: &HybridStore, index: &str) {
    let (writer_arc, _) = store.get_or_create_index(index).expect("open");
    let writer = writer_arc.lock().expect("writer");
    let schema = writer.index().schema();
    let id = schema.get_field("id").expect("id field");
    let when = schema.get_field("when").expect("when field");
    let mut doc = tantivy::TantivyDocument::default();
    doc.add_text(id, "poison");
    doc.add_i64(when, 1_600_000_000);
    writer
        .add_document(doc)
        .expect("queued: the failure is the indexing thread's");
}

/// A stored schema that retypes a built column cannot kill the writer: the value goes in by the
/// column. Before, the value was added under the declaration — an integer on a date column —
/// the indexing thread died, and the commit failed with `Expected a Date for field "when"`.
#[test]
fn a_value_is_added_by_the_column_the_index_built() {
    let dir = TempDir::new().expect("tempdir");
    let store = HybridStore::new(config(dir.path()), 1).expect("store");
    store
        .store_schema_and_cache("books", &dated_schema(TantivyFieldType::Date))
        .expect("schema");
    store
        .apply_write("books", put("built", json!("2020-01-05")))
        .expect("the write that builds the index");

    // The disagreement a re-mint left behind: the stored schema says `I64`, the column is a date.
    store
        .store_schema_and_cache("books", &dated_schema(TantivyFieldType::I64))
        .expect("a schema that retypes a built column is saved (a rebuild may be pending)");
    store
        .apply_batch(
            "books",
            (0..20)
                .map(|i| put(&format!("n{i}"), json!(1_600_000_000 + i)))
                .collect(),
        )
        .expect("the batch lands");

    store
        .commit_index("books")
        .expect("the commit succeeds: no writer died on a value its column cannot hold");
    assert_eq!(searchable(&store, "books"), 21);
}

/// A commit that fails on a dead writer retires it, and the next write reopens the index. Before,
/// the dead writer stayed cached: that write answered "An index writer was killed", and so did
/// every write after it until the process restarted.
#[test]
fn a_failed_commit_retires_the_writer_and_the_next_write_reopens_the_index() {
    let dir = TempDir::new().expect("tempdir");
    let store = HybridStore::new(config(dir.path()), 1).expect("store");
    store
        .store_schema_and_cache("books", &dated_schema(TantivyFieldType::Date))
        .expect("schema");
    store
        .apply_write("books", put("before", json!("2020-01-05")))
        .expect("a write the dead writer will hold uncommitted");

    poison(&store, "books");
    assert!(
        store.commit_index("books").is_err(),
        "the commit fails: the indexing thread died on the poisoned document"
    );

    store
        .apply_batch(
            "books",
            (0..20)
                .map(|i| put(&format!("after{i}"), json!("2021-06-07")))
                .collect(),
        )
        .expect("the next write reopens the index instead of hitting the dead writer");
    store.commit_index("books").expect("commit");

    // The reopen replayed the write the dead writer held, from redb, and nothing was lost.
    assert_eq!(searchable(&store, "books"), 21);
}

/// A write that finds the writer already dead is written, not refused: its batch is durable in
/// redb before Tantivy is asked, so reopening the index replays it along with everything else
/// the dead writer held.
#[test]
fn a_write_that_meets_a_dead_writer_is_replayed_by_the_reopened_index() {
    let dir = TempDir::new().expect("tempdir");
    let store = HybridStore::new(config(dir.path()), 1).expect("store");
    store
        .store_schema_and_cache("books", &dated_schema(TantivyFieldType::Date))
        .expect("schema");
    store
        .apply_write("books", put("before", json!("2020-01-05")))
        .expect("a write the dead writer will hold uncommitted");

    poison(&store, "books");
    // Give the indexing thread time to reach the poisoned document and exit, so the next add
    // meets a dead writer rather than queueing behind a dying one.
    std::thread::sleep(std::time::Duration::from_millis(500));

    store
        .apply_batch(
            "books",
            (0..20)
                .map(|i| put(&format!("after{i}"), json!("2021-06-07")))
                .collect(),
        )
        .expect("a batch that meets the dead writer is written");
    store.commit_index("books").expect("commit");
    assert_eq!(searchable(&store, "books"), 21);
}
