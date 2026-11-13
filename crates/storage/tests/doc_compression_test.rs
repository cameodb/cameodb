//! Document rows are stored LZ4-compressed against a dictionary the index's own first documents
//! fill (`storage::doc_codec`). What the store promises about that:
//!
//! - every document reads back as written, whichever state the dictionary was in when its row
//!   was compressed, across a reopen and through WAL replay;
//! - a row written before compression existed still reads, beside compressed ones;
//! - a deleted index takes its dictionary with it, so the name's next documents do not read
//!   against the old one;
//! - writers growing the dictionary and readers decoding against it do not need each other's
//!   permission.

use std::sync::Arc;

use redb::{ReadableDatabase, ReadableTable, TableDefinition};
use serde_json::{Value, json};
use storage::doc_codec::{DICT_CAP, TABLE_DOC_DICT, dictionary_needed};
use storage::{HybridStore, IndexSchema, StorageConfig, WalOp};
use tempfile::TempDir;

const INDEX: &str = "library";

fn config(shard_path: &std::path::Path) -> StorageConfig {
    StorageConfig {
        max_open_indexes: 0,
        shard_path: shard_path.to_path_buf(),
        indexer_num_threads: 1,
        merge_num_threads: 1,
        default_batch_size: 100_000,
        wal_sync: false,
        ..StorageConfig::default()
    }
}

fn open(shard_path: &std::path::Path) -> HybridStore {
    HybridStore::new(config(shard_path), 1).expect("open store")
}

/// A document of about 700 bytes: fixed field names, a little that differs.
fn body(n: u32) -> Value {
    json!({
        "title": format!("Annual report of the harbour board, volume {n}"),
        "category": "harbour-board-reports",
        "summary": format!(
            "Volume {n} records the tonnage landed at the east quay, the dredging of the \
             channel, the state of the breakwater after the winter gales, and the accounts \
             of the board as audited for the year, with the engineer's remarks appended."
        ),
        "keywords": ["harbour", "dredging", "breakwater", "accounts", "tonnage"],
        "pages": 100 + n,
        "available": n.is_multiple_of(2),
    })
}

fn put(n: u32) -> WalOp {
    WalOp::Put {
        id: format!("doc-{n}"),
        json_blob: Some(body(n)),
    }
}

fn stored_body(store: &HybridStore, index: &str, n: u32) -> Option<Value> {
    let bytes = store.get_by_key(index, &format!("doc-{n}")).expect("get")?;
    let row: Value = serde_json::from_slice(&bytes).expect("a stored row is JSON");
    Some(row["json_blob"].clone())
}

/// What the file holds for `index`: each row's stored length and the dictionary length it
/// names, and the dictionary's own length.
fn on_disk(shard_path: &std::path::Path, index: &str) -> (Vec<(usize, usize)>, usize) {
    let db = redb::Database::open(shard_path.join("store.redb")).expect("open the file");
    let read = db.begin_read().expect("read");
    let name = format!("data_{index}");
    let table = read
        .open_table(TableDefinition::<&str, &[u8]>::new(&name))
        .expect("data table");
    let rows = table
        .iter()
        .expect("iter")
        .map(|row| {
            let (_, value) = row.expect("row");
            (value.value().len(), dictionary_needed(value.value()))
        })
        .collect();
    let dict = read
        .open_table(TABLE_DOC_DICT)
        .ok()
        .and_then(|t| t.get(index).expect("dict").map(|d| d.value().len()))
        .unwrap_or(0);
    (rows, dict)
}

#[test]
fn documents_are_stored_compressed_and_read_back_as_written() {
    let dir = TempDir::new().unwrap();
    let store = open(dir.path());
    store
        .store_schema_and_cache(INDEX, &IndexSchema::default())
        .unwrap();

    // One at a time first, so rows are written against every length the dictionary passes
    // through, then in batches against the full one.
    for n in 0..150 {
        store.apply_write(INDEX, put(n)).unwrap();
    }
    for chunk in (150..1_000).collect::<Vec<u32>>().chunks(100) {
        store
            .apply_batch(INDEX, chunk.iter().map(|n| put(*n)).collect())
            .unwrap();
    }

    for n in 0..1_000 {
        assert_eq!(stored_body(&store, INDEX, n), Some(body(n)), "doc-{n}");
    }
    let ids: Vec<String> = (0..1_000).map(|n| format!("doc-{n}")).collect();
    assert_eq!(store.get_batch_by_keys(INDEX, &ids).unwrap().len(), 1_000);
    let fetched = store
        .fetch_documents(INDEX, &ids[..20], None, None, None, None)
        .unwrap();
    assert_eq!(fetched.docs.len(), 20);

    store.commit_index(INDEX).unwrap();
    drop(store);

    let (rows, dict) = on_disk(dir.path(), INDEX);
    assert_eq!(rows.len(), 1_000);
    assert_eq!(dict, DICT_CAP, "the first documents fill the dictionary");
    let raw = serde_json::to_vec(&json!({ "json_blob": body(999) }))
        .unwrap()
        .len();
    let against_full: Vec<usize> = rows
        .iter()
        .filter(|(_, needed)| *needed == DICT_CAP)
        .map(|(len, _)| *len)
        .collect();
    assert!(
        against_full.len() > 800,
        "most rows are written after it fills"
    );
    let mean = against_full.iter().sum::<usize>() / against_full.len();
    assert!(
        mean * 3 < raw,
        "a row is {mean} bytes stored, {raw} as written"
    );
    // Rows from while it was filling name shorter lengths, and were read back above.
    assert!(
        rows.iter()
            .any(|(_, needed)| *needed > 0 && *needed < DICT_CAP)
    );

    // Another process's view: a cold cache, every row again.
    let store = open(dir.path());
    for n in (0..1_000).step_by(7) {
        assert_eq!(
            stored_body(&store, INDEX, n),
            Some(body(n)),
            "doc-{n} after reopen"
        );
    }
}

#[test]
fn replay_reads_compressed_rows() {
    let dir = TempDir::new().unwrap();
    let config = StorageConfig {
        wal_sync: true,
        ..config(dir.path())
    };
    let store = HybridStore::new(config.clone(), 1).unwrap();
    store
        .store_schema_and_cache(INDEX, &IndexSchema::default())
        .unwrap();
    for n in 0..300 {
        store.apply_write(INDEX, put(n)).unwrap();
    }
    // No Tantivy commit: the documents exist only as redb rows and a WAL tail.
    drop(store);

    let store = HybridStore::new(config, 1).unwrap();
    store.get_or_create_index(INDEX).unwrap();
    store.commit_index(INDEX).unwrap();
    let found = store.search_documents(INDEX, "*", 10, None).unwrap();
    assert_eq!(found.hits.len(), 10);
    for (_, doc) in &found.hits {
        assert_eq!(doc["category"], "harbour-board-reports");
    }
    assert_eq!(
        found.total_hits, 300,
        "every compressed row was replayed into the index"
    );
}

#[test]
fn rows_written_before_compression_read_beside_compressed_ones() {
    let dir = TempDir::new().unwrap();
    {
        let store = open(dir.path());
        store
            .store_schema_and_cache(INDEX, &IndexSchema::default())
            .unwrap();
        store.apply_write(INDEX, put(0)).unwrap();
        store.commit_index(INDEX).unwrap();
    }
    // What an older build left behind: the document as JSON, nothing in front of it.
    {
        let db = redb::Database::create(dir.path().join("store.redb")).unwrap();
        let txn = db.begin_write().unwrap();
        {
            let name = format!("data_{INDEX}");
            let mut table = txn
                .open_table(TableDefinition::<&str, &[u8]>::new(&name))
                .unwrap();
            for n in 5_000..5_010u32 {
                let row = serde_json::to_vec(&json!({ "json_blob": body(n) })).unwrap();
                table
                    .insert(format!("doc-{n}").as_str(), row.as_slice())
                    .unwrap();
            }
        }
        txn.commit().unwrap();
    }
    let store = open(dir.path());
    for n in 1..400 {
        store.apply_write(INDEX, put(n)).unwrap();
    }
    for n in (5_000..5_010).chain(0..400) {
        assert_eq!(stored_body(&store, INDEX, n), Some(body(n)), "doc-{n}");
    }
    // Overwriting an old row compresses it.
    store.apply_write(INDEX, put(5_000)).unwrap();
    assert_eq!(stored_body(&store, INDEX, 5_000), Some(body(5_000)));
    drop(store);
    let (rows, _) = on_disk(dir.path(), INDEX);
    assert_eq!(
        rows.iter().filter(|(_, needed)| *needed == 0).count(),
        9 + 1
    );
}

#[test]
fn a_deleted_index_takes_its_dictionary_with_it() {
    let dir = TempDir::new().unwrap();
    let store = open(dir.path());
    store
        .store_schema_and_cache(INDEX, &IndexSchema::default())
        .unwrap();
    for n in 0..300 {
        store.apply_write(INDEX, put(n)).unwrap();
    }
    assert_eq!(stored_body(&store, INDEX, 299), Some(body(299)));

    store.delete_index_data(INDEX, true).unwrap();
    store
        .store_schema_and_cache(INDEX, &IndexSchema::default())
        .unwrap();
    // Different documents under the same name: read against the old dictionary they would
    // come back as something else, or not at all.
    let other = |n: u32| json!({ "note": format!("entry {n} of the second collection, unlike the first"), "n": n });
    for n in 0..300 {
        store
            .apply_write(
                INDEX,
                WalOp::Put {
                    id: format!("doc-{n}"),
                    json_blob: Some(other(n)),
                },
            )
            .unwrap();
    }
    for n in 0..300 {
        assert_eq!(stored_body(&store, INDEX, n), Some(other(n)), "doc-{n}");
    }
    drop(store);
    let (rows, dict) = on_disk(dir.path(), INDEX);
    assert_eq!(rows.len(), 300);
    assert!(
        dict > 0 && dict < DICT_CAP,
        "a new dictionary, still filling: {dict}"
    );
}

#[test]
fn readers_decode_while_writers_fill_the_dictionary() {
    let dir = TempDir::new().unwrap();
    let store = Arc::new(open(dir.path()));
    store
        .store_schema_and_cache(INDEX, &IndexSchema::default())
        .unwrap();
    let written = Arc::new(std::sync::atomic::AtomicU32::new(0));

    std::thread::scope(|scope| {
        // Four writers, interleaved ids, single writes: the dictionary grows under all of them.
        for writer in 0..4u32 {
            let store = store.clone();
            let written = written.clone();
            scope.spawn(move || {
                for i in 0..150u32 {
                    let n = i * 4 + writer;
                    store.apply_write(INDEX, put(n)).unwrap();
                    written.fetch_max(i, std::sync::atomic::Ordering::SeqCst);
                }
            });
        }
        for _ in 0..3 {
            let store = store.clone();
            let written = written.clone();
            scope.spawn(move || {
                let mut seen = 0u32;
                while seen < 149 {
                    seen = written.load(std::sync::atomic::Ordering::SeqCst);
                    for n in 0..seen * 4 {
                        // Present or not yet, never unreadable and never another document.
                        if let Some(found) = stored_body(&store, INDEX, n) {
                            assert_eq!(found, body(n), "doc-{n}");
                        }
                    }
                }
            });
        }
    });

    for n in 0..600 {
        assert_eq!(stored_body(&store, INDEX, n), Some(body(n)), "doc-{n}");
    }
    let table_len = {
        drop(store);
        let (rows, dict) = on_disk(dir.path(), INDEX);
        assert_eq!(dict, DICT_CAP);
        rows.len()
    };
    assert_eq!(table_len, 600);
}
