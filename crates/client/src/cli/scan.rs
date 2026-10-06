//! Reading enough of a source to judge its columns, and no more.
//!
//! A sample of the first rows was all schema detection used to see, and a file sorted by time —
//! which is how exports are written — shows only its first hour there. Here the head is read in
//! batches until the columns stop teaching anything new; a local file is then also read in short
//! blocks spread over the rest, visited coarse to fine, so even a scan cut short has seen every
//! part of the file. A file up to [`ScanLimits::whole_file_bytes`] is read whole. Compressed and
//! remote sources cannot be read out of order, so they are read from the head only.

use super::*;
use anyhow::{Context, Result};
use serde_json::Value as JsonValue;
use std::fs;
use std::io::{BufRead, BufReader, Cursor, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use storage::TantivyFieldType;

/// How far a scan reads.
#[derive(Debug, Clone)]
pub(crate) struct ScanLimits {
    /// A local file up to this size is read whole.
    pub(crate) whole_file_bytes: u64,
    /// Past it, the scan reads at most the larger of this and a third of the file.
    pub(crate) budget_fraction: u64,
    /// The head is read for at most this long.
    pub(crate) time: Duration,
    /// The head is read for at least this many rows, or to its end.
    pub(crate) min_rows: u64,
    /// Rows per batch; stability is judged between batches.
    pub(crate) batch_rows: u64,
    /// Batches in a row that must add no new kind of value to any column.
    pub(crate) stable_batches: u32,
    /// Spread blocks over the rest of a local file.
    pub(crate) blocks: usize,
}

impl Default for ScanLimits {
    fn default() -> Self {
        Self {
            whole_file_bytes: 1 << 30,
            budget_fraction: 3,
            time: Duration::from_secs(10),
            min_rows: 50_000,
            batch_rows: 8_192,
            stable_batches: 2,
            blocks: 64,
        }
    }
}

impl ScanLimits {
    /// The bytes a scan may read of a source this size.
    fn budget(&self, size: Option<u64>) -> u64 {
        size.map_or(self.whole_file_bytes, |size| {
            self.whole_file_bytes.max(size / self.budget_fraction)
        })
    }

    /// Rows per spread block: enough that all the blocks together hold 5·√N rows. That many rows
    /// drawn across the file show a key whose every value appears twice about a dozen times —
    /// a sample of a few hundred, as detection used, shows it about never.
    fn block_rows(&self, estimated_rows: u64) -> u64 {
        let target = 5.0 * (estimated_rows as f64).sqrt();
        ((target / self.blocks as f64).ceil() as u64).max(256)
    }
}

/// A source's bytes: a local file, or a remote one fetched once and kept for the scan and the
/// load both.
pub(crate) enum SourceData {
    File {
        path: PathBuf,
        compression: Compression,
    },
    Memory(Arc<Vec<u8>>),
}

/// Shared bytes a cursor can read without copying them.
struct SharedBytes(Arc<Vec<u8>>);

impl AsRef<[u8]> for SharedBytes {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl SourceData {
    pub(crate) async fn open(client: &CameoClient, source: &str) -> Result<Self> {
        if is_http_source(source) {
            let bytes = fetch_bytes_source(client, source).await?;
            Ok(SourceData::Memory(Arc::new(bytes)))
        } else {
            Ok(SourceData::File {
                path: PathBuf::from(source),
                compression: detect_compression(source),
            })
        }
    }

    /// The source from its start, decompressed.
    pub(crate) fn reader(&self) -> Result<Box<dyn Read + Send>> {
        match self {
            SourceData::File { path, compression } => open_local_reader(path, *compression),
            SourceData::Memory(bytes) => Ok(Box::new(Cursor::new(SharedBytes(bytes.clone())))),
        }
    }

    /// The first bytes of the source, decompressed.
    pub(crate) fn prefix(&self, max_bytes: usize) -> Result<Vec<u8>> {
        match self {
            SourceData::File { path, .. } => read_local_prefix_bytes(path, max_bytes),
            SourceData::Memory(bytes) => Ok(bytes[..bytes.len().min(max_bytes)].to_vec()),
        }
    }

    /// A local file read as stored, and its length: the one source a scan can jump into.
    fn seekable(&self) -> Option<(&Path, u64)> {
        match self {
            SourceData::File {
                path,
                compression: Compression::None,
            } => fs::metadata(path).ok().map(|m| (path.as_path(), m.len())),
            _ => None,
        }
    }

    /// The size of the decompressed source, when it is known without reading it.
    fn size(&self) -> Option<u64> {
        match self {
            SourceData::File {
                path,
                compression: Compression::None,
            } => fs::metadata(path).ok().map(|m| m.len()),
            SourceData::File { .. } => None,
            SourceData::Memory(bytes) => Some(bytes.len() as u64),
        }
    }

    /// The size the source takes where it is kept, for the report.
    fn stored_size(&self) -> Option<u64> {
        match self {
            SourceData::File { path, .. } => fs::metadata(path).ok().map(|m| m.len()),
            SourceData::Memory(bytes) => Some(bytes.len() as u64),
        }
    }
}

/// The delimiter a CSV's first line uses most: semicolon, tab or comma.
pub(crate) fn sniff_delimiter(prefix: &[u8]) -> u8 {
    let first_line_end = prefix
        .iter()
        .position(|b| *b == b'\n')
        .unwrap_or(prefix.len());
    let first_line = &prefix[..first_line_end];
    let count = |d: u8| first_line.iter().filter(|b| **b == d).count();
    let (tab, comma, semi) = (count(b'\t'), count(b','), count(b';'));
    if semi >= tab && semi >= comma {
        b';'
    } else if tab >= comma {
        b'\t'
    } else {
        b','
    }
}

pub(crate) fn delimiter_byte(delimiter: Delimiter, data: &SourceData) -> Result<u8> {
    Ok(match delimiter {
        Delimiter::Detect => sniff_delimiter(&data.prefix(SOURCE_SNIFF_BYTES)?),
        Delimiter::Comma => b',',
        Delimiter::Tab => b'\t',
        Delimiter::Semicolon => b';',
    })
}

/// A CSV reader over a source. Variable-length records are allowed: a stray delimiter in a
/// remote TSV should not abort the whole read.
pub(crate) fn csv_reader<R: Read>(reader: R, delimiter: u8) -> csv::Reader<R> {
    csv::ReaderBuilder::new()
        .flexible(true)
        .delimiter(delimiter)
        .from_reader(reader)
}

/// Decides, batch by batch, when the head has been read far enough.
struct Stopper {
    limits: ScanLimits,
    start: Instant,
    whole: bool,
    byte_limit: Option<u64>,
    rows: u64,
    last_novelty: usize,
    stable: u32,
}

impl Stopper {
    fn new(limits: &ScanLimits, whole: bool, byte_limit: Option<u64>) -> Self {
        Self {
            limits: limits.clone(),
            start: Instant::now(),
            whole,
            byte_limit,
            rows: 0,
            last_novelty: usize::MAX,
            stable: 0,
        }
    }

    /// After each row: why to stop now, if it is time to.
    fn after_row(&mut self, profiler: &Profiler, bytes: u64) -> Option<&'static str> {
        self.rows += 1;
        if !self.rows.is_multiple_of(self.limits.batch_rows) {
            return None;
        }
        let novelty = profiler.novelty();
        if novelty == self.last_novelty {
            self.stable += 1;
        } else {
            self.stable = 0;
            self.last_novelty = novelty;
        }
        if self.start.elapsed() >= self.limits.time {
            return Some("time limit");
        }
        if self.whole {
            return None;
        }
        if self.byte_limit.is_some_and(|limit| bytes >= limit) {
            return Some("byte budget");
        }
        if self.rows >= self.limits.min_rows && self.stable >= self.limits.stable_batches {
            return Some("column types stable");
        }
        None
    }
}

/// Block indexes in coarse-to-fine order — the middle, the quarters, the eighths — so a scan
/// stopped part way has still seen the whole file evenly.
fn coarse_to_fine(blocks: usize) -> Vec<usize> {
    let bits = blocks.next_power_of_two().trailing_zeros();
    let mut order: Vec<usize> = (0..blocks.next_power_of_two())
        .map(|i| {
            if bits == 0 {
                0
            } else {
                i.reverse_bits() >> (usize::BITS - bits)
            }
        })
        .filter(|i| *i < blocks)
        .collect();
    order.dedup();
    order
}

/// What a CSV scan found.
pub(crate) struct CsvScan {
    pub(crate) headers: Vec<(String, Option<TantivyFieldType>)>,
    pub(crate) profiler: Profiler,
    pub(crate) summary: ScanSummary,
}

pub(crate) fn scan_csv(
    data: &SourceData,
    delimiter: u8,
    limits: &ScanLimits,
    named_id: Option<&IdSpec>,
) -> Result<CsvScan> {
    let mut reader = csv_reader(data.reader()?, delimiter);
    let raw_headers = reader
        .headers()
        .context("CSV file is missing headers")?
        .clone();
    let header_bytes = reader.position().byte();
    let headers: Vec<(String, Option<TantivyFieldType>)> =
        raw_headers.iter().map(parse_header_with_hint).collect();
    let names: Vec<String> = headers.iter().map(|(n, _)| n.clone()).collect();
    let mut profiler = Profiler::new(&names, limits.batch_rows as usize);
    if let Some(spec) = named_id {
        profiler.track_id(spec);
    }

    let seekable = data.seekable();
    let whole = seekable.is_some_and(|(_, len)| len <= limits.whole_file_bytes);
    let byte_limit = match seekable {
        Some(_) if whole => None,
        // Half the budget goes to the head, the other half to the blocks after it.
        Some((_, len)) => Some(limits.budget(Some(len)) / 2),
        None => Some(limits.budget(data.size())),
    };
    let mut stopper = Stopper::new(limits, whole, byte_limit);
    let mut lines = RecordLines::after_header(&raw_headers, reader.position().line());
    let mut record = csv::StringRecord::new();
    let mut summary = ScanSummary {
        format: format!("CSV, delimiter {}", delimiter_name(delimiter)),
        source_bytes: data.stored_size(),
        stopped_by: "end of source",
        ..Default::default()
    };
    let mut at_end = false;
    loop {
        if !reader
            .read_record(&mut record)
            .context("Failed to read CSV record")?
        {
            at_end = true;
            break;
        }
        let line = lines.locate(&record, reader.position().line());
        profiler.observe_csv(&record, Location::Line(line));
        summary.head_rows += 1;
        summary.head_last = Some(Location::Line(line));
        if let Some(reason) = stopper.after_row(&profiler, reader.position().byte()) {
            summary.stopped_by = reason;
            break;
        }
    }
    let head_bytes = reader.position().byte();
    summary.bytes_read = head_bytes;

    if !at_end && let Some((path, len)) = seekable {
        let per_row = (head_bytes - header_bytes) as f64 / summary.head_rows.max(1) as f64;
        let estimated = ((len - header_bytes) as f64 / per_row.max(1.0)) as u64;
        let block_rows = limits.block_rows(estimated);
        let region = len - head_bytes;
        for block in coarse_to_fine(limits.blocks) {
            if stopper.start.elapsed() >= limits.time * 2 {
                break;
            }
            let offset = head_bytes + region * (2 * block as u64 + 1) / (2 * limits.blocks as u64);
            let (rows, bytes) = read_csv_block(
                path,
                offset,
                delimiter,
                headers.len(),
                block_rows,
                &mut profiler,
            )?;
            summary.spread_rows += rows;
            summary.bytes_read += bytes;
            summary.spread_blocks += 1;
        }
    }

    profiler.finish();
    summary.whole = at_end;
    summary.elapsed = stopper.start.elapsed();
    summary.rows = if at_end {
        Some(summary.head_rows)
    } else {
        data.size().map(|size| {
            let rows = summary.head_rows + summary.spread_rows;
            let per_row = (summary.bytes_read - header_bytes) as f64 / rows.max(1) as f64;
            ((size - header_bytes) as f64 / per_row.max(1.0)) as u64
        })
    };
    Ok(CsvScan {
        headers,
        profiler,
        summary,
    })
}

fn delimiter_name(delimiter: u8) -> &'static str {
    match delimiter {
        b',' => "comma",
        b'\t' => "tab",
        b';' => "semicolon",
        _ => "other",
    }
}

/// Read up to `rows` records from a block starting at about `offset`. The reader lands mid-row,
/// so the partial line is dropped and reading starts only once two records in a row have the
/// header's width — one alone might be the tail of a quoted value that spans lines.
fn read_csv_block(
    path: &Path,
    offset: u64,
    delimiter: u8,
    width: usize,
    rows: u64,
    profiler: &mut Profiler,
) -> Result<(u64, u64)> {
    let mut file =
        fs::File::open(path).with_context(|| format!("Failed to open {}", path.display()))?;
    file.seek(SeekFrom::Start(offset))?;
    let mut buffered = BufReader::new(file);
    let mut partial = Vec::new();
    let skipped = buffered.read_until(b'\n', &mut partial)? as u64;
    let start = offset + skipped;
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .delimiter(delimiter)
        .from_reader(buffered);
    let mut record = csv::StringRecord::new();
    let mut pending: Option<(csv::StringRecord, Location)> = None;
    let mut synced = false;
    let mut misses = 0;
    let mut taken = 0;
    while taken < rows {
        match reader.read_record(&mut record) {
            Ok(true) => {}
            // The end of the file, or bytes no record is made of: the block is done.
            Ok(false) | Err(_) => break,
        }
        let at = Location::Offset(start + record.position().map_or(0, |p| p.byte()));
        if record.len() != width {
            if !synced {
                pending = None;
                misses += 1;
                if misses > 16 {
                    break;
                }
            }
            continue;
        }
        if !synced {
            match pending.take() {
                Some((first, first_at)) => {
                    synced = true;
                    profiler.observe_csv(&first, first_at);
                    taken += 1;
                }
                None => {
                    pending = Some((record.clone(), at));
                    continue;
                }
            }
        }
        profiler.observe_csv(&record, at);
        taken += 1;
    }
    Ok((taken, skipped + reader.position().byte()))
}

/// A reader that counts the bytes read through it.
struct Counted<R> {
    inner: R,
    count: Arc<AtomicU64>,
}

impl<R: Read> Read for Counted<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.count.fetch_add(n as u64, Ordering::Relaxed);
        Ok(n)
    }
}

/// What a JSON scan found.
pub(crate) struct JsonScan {
    pub(crate) profiler: Profiler,
    pub(crate) summary: ScanSummary,
}

/// The head of a JSON source, document by document, until it has been read far enough.
pub(crate) struct JsonHeadScan {
    profiler: Profiler,
    stopper: Stopper,
    summary: ScanSummary,
    bytes: Arc<AtomicU64>,
    /// Set when the scan stopped on purpose, so the error that stopped the reader is not one.
    stopped: bool,
}

impl JsonHeadScan {
    pub(crate) fn new(
        format: SourceFormat,
        stored: Option<u64>,
        limits: &ScanLimits,
        whole: bool,
        byte_limit: Option<u64>,
    ) -> Self {
        Self {
            profiler: Profiler::new(&[], limits.batch_rows as usize),
            stopper: Stopper::new(limits, whole, byte_limit),
            summary: ScanSummary {
                format: match format {
                    SourceFormat::JsonLines => "JSON lines",
                    SourceFormat::JsonArray => "JSON array",
                    _ => "JSON document",
                }
                .to_string(),
                source_bytes: stored,
                stopped_by: "end of source",
                ..Default::default()
            },
            bytes: Arc::new(AtomicU64::new(0)),
            stopped: false,
        }
    }

    /// Note one document; an error once the head has been read far enough, which stops the
    /// reader and is then discarded by [`Self::finish`].
    pub(crate) fn push(&mut self, raw_doc: &JsonValue) -> Result<()> {
        let doc = effective_json_document(raw_doc)?;
        let obj = doc
            .as_object()
            .ok_or_else(|| anyhow!("JSON source documents must be objects"))?;
        let at = Location::Document(self.summary.head_rows + 1);
        self.profiler.observe_json(obj, at);
        self.summary.head_rows += 1;
        self.summary.head_last = Some(at);
        let bytes = self.bytes.load(Ordering::Relaxed);
        if let Some(reason) = self.stopper.after_row(&self.profiler, bytes) {
            self.summary.stopped_by = reason;
            self.stopped = true;
            anyhow::bail!("scan complete");
        }
        Ok(())
    }

    pub(crate) fn finish(mut self, read: Result<usize>, size: Option<u64>) -> Result<JsonScan> {
        match read {
            Ok(_) => {}
            Err(_) if self.stopped => {}
            Err(err) => return Err(err),
        }
        if self.summary.head_rows == 0 {
            anyhow::bail!("JSON source does not contain any valid object documents");
        }
        self.profiler.finish();
        let bytes = self.bytes.load(Ordering::Relaxed);
        self.summary.bytes_read = bytes;
        self.summary.whole = !self.stopped;
        self.summary.elapsed = self.stopper.start.elapsed();
        self.summary.rows = if self.summary.whole {
            Some(self.summary.head_rows)
        } else {
            size.map(|size| {
                (size as f64 / (bytes as f64 / self.summary.head_rows as f64).max(1.0)) as u64
            })
        };
        Ok(JsonScan {
            profiler: self.profiler,
            summary: self.summary,
        })
    }
}

/// Scan a JSON source from its start: a local file read as stored up to the whole-file limit is
/// read whole, anything else from the head only — a document in an array cannot be found by
/// jumping into the middle of one.
pub(crate) fn scan_json(
    data: &SourceData,
    format: SourceFormat,
    limits: &ScanLimits,
    named_id: Option<&IdSpec>,
) -> Result<JsonScan> {
    let seekable = data.seekable();
    let whole = seekable.is_some_and(|(_, len)| len <= limits.whole_file_bytes);
    let byte_limit = if whole {
        None
    } else {
        Some(limits.budget(data.size()))
    };
    let mut scan = JsonHeadScan::new(format, data.stored_size(), limits, whole, byte_limit);
    if let Some(spec) = named_id {
        scan.profiler.track_id(spec);
    }
    let reader = Counted {
        inner: data.reader()?,
        count: scan.bytes.clone(),
    };
    let read = for_each_json_document_in_reader(reader, format, |doc| scan.push(&doc));
    scan.finish(read, data.size())
}

/// Scan the head of a remote JSON source as it streams, then let the connection go.
pub(crate) async fn scan_http_json(
    client: &CameoClient,
    source: &str,
    format: SourceFormat,
    limits: &ScanLimits,
    named_id: Option<&IdSpec>,
) -> Result<JsonScan> {
    let mut scan = JsonHeadScan::new(format, None, limits, false, Some(limits.budget(None)));
    if let Some(spec) = named_id {
        scan.profiler.track_id(spec);
    }
    let count = scan.bytes.clone();
    let read = for_each_json_document_in_http_source(client, source, format, |doc| {
        count.fetch_add(
            serde_json::to_vec(&doc).map_or(0, |b| b.len() as u64),
            Ordering::Relaxed,
        );
        scan.push(&doc)
    })
    .await;
    scan.finish(read, None)
}
