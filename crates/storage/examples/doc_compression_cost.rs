//! What each document compression method makes of the rows of a real store, and what it costs.
//!
//! ```text
//! cargo run --release -p storage --example doc_compression_cost -- <copy of a store.redb> [rows]
//! ```
//!
//! Give it a **copy** of a shard's `store.redb`: opening a file may repair it. For each data
//! table it fills a dictionary from the table's first documents, as a rewrite does, then
//! compresses an evenly spaced sample of the rows after those (20,000 unless told) with LZ4 and
//! with zstd, and reads each back. The rows that filled the dictionary are left out: against a
//! dictionary that holds them they compress to almost nothing, and on a small table they would
//! be most of the sample. It reports the stored bytes as a share of the documents as written, and the
//! time per row to compress and to decompress, one thread.

use std::time::{Duration, Instant};

use redb::{
    ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition, TableError,
    TableHandle,
};
use storage::doc_codec::{self, DICT_CAP, DocCompression, DocDict, TABLE_DOC_DICT};

fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let path = args
        .next()
        .ok_or_else(|| anyhow::anyhow!("usage: doc_compression_cost <store.redb copy> [rows]"))?;
    let sample: u64 = args.next().map_or(Ok(20_000), |n| n.parse())?;

    let db = redb::Database::open(&path)?;
    let txn = db.begin_read()?;
    println!(
        "{:<28} {:>8} {:>7} | {:>6} {:>8} {:>8} | {:>6} {:>8} {:>8}",
        "table", "rows", "avg B", "lz4", "enc µs", "dec µs", "zstd", "enc µs", "dec µs"
    );
    for handle in txn.list_tables()? {
        let name = handle.name().to_string();
        let Some(index) = name.strip_prefix("data_") else {
            continue;
        };
        let table = match txn.open_table(TableDefinition::<&str, &[u8]>::new(&name)) {
            Ok(table) => table,
            Err(TableError::TableTypeMismatch { .. }) => continue,
            Err(e) => return Err(e.into()),
        };
        let rows = table.len()?;
        if rows == 0 {
            continue;
        }
        let old_dict: Vec<u8> = match txn.open_table(TABLE_DOC_DICT) {
            Ok(dicts) => dicts
                .get(index)?
                .map(|d| d.value().to_vec())
                .unwrap_or_default(),
            Err(TableError::TableDoesNotExist(_)) => Vec::new(),
            Err(e) => return Err(e.into()),
        };

        let mut bytes = Vec::new();
        let mut rows_in_dict = 0u64;
        for row in table.iter()? {
            if bytes.len() >= DICT_CAP {
                break;
            }
            let doc = doc_codec::decode(row?.1.value(), &old_dict)?.into_owned();
            doc_codec::grow(&mut bytes, &doc);
            rows_in_dict += 1;
        }
        let dict = DocDict::new(bytes);
        if rows_in_dict >= rows {
            println!("{index:<28} {rows} rows, all in the dictionary: nothing to measure");
            continue;
        }

        let stride = ((rows - rows_in_dict) / sample).max(1) as usize;
        let mut docs = Vec::new();
        for (i, row) in table.iter()?.skip(rows_in_dict as usize).enumerate() {
            let (_, value) = row?;
            if i % stride == 0 {
                docs.push(doc_codec::decode(value.value(), &old_dict)?.into_owned());
            }
        }
        let raw: usize = docs.iter().map(Vec::len).sum();

        let mut line = format!("{:<28} {:>8} {:>7} |", index, docs.len(), raw / docs.len());
        for method in [DocCompression::Lz4, DocCompression::Zstd] {
            // Once unmeasured: the first zstd row prepares the dictionary.
            let _ = doc_codec::encode(&docs[0], &dict, method);
            let started = Instant::now();
            let stored: Vec<Vec<u8>> = docs
                .iter()
                .map(|doc| doc_codec::encode(doc, &dict, method).unwrap_or_else(|| doc.clone()))
                .collect();
            let encode = started.elapsed();
            let started = Instant::now();
            for (row, doc) in stored.iter().zip(&docs) {
                let back = doc_codec::decode(row, dict.as_bytes())?;
                assert_eq!(back.len(), doc.len());
                std::hint::black_box(&back);
            }
            let decode = started.elapsed();
            let per = |d: Duration| d.as_secs_f64() * 1e6 / docs.len() as f64;
            let size: usize = stored.iter().map(Vec::len).sum();
            line += &format!(
                " {:>5.1}% {:>8.2} {:>8.2} |",
                100.0 * size as f64 / raw as f64,
                per(encode),
                per(decode)
            );
        }
        println!("{}", line.trim_end_matches(" |"));
    }
    Ok(())
}
