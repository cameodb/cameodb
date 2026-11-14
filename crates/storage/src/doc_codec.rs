//! How a document row in `data_<index>` is stored: as written, or compressed against the
//! index's dictionary, by LZ4 or by zstd.
//!
//! A document here is small — a kilobyte or two of JSON — and repeats little inside itself; what
//! it repeats is its neighbours: the field names, the fixed values, the turns of phrase of the
//! collection. LZ4 on one such document alone saves a fifth. Against a dictionary of the
//! collection's own earlier documents it saves a half to three quarters, measured on text
//! indexes, and each row still reads and writes on its own.
//!
//! # The method
//!
//! [`DocCompression`] is what a node compresses a row with when it writes one; it is the node's
//! setting, not the index's and not the file's. A row says in its first byte how it was
//! compressed, so a store reads rows of either kind whatever the node is set to, an index may
//! hold both, and changing the setting re-encodes nothing: it applies to the rows written
//! after it.
//!
//! # The dictionary
//!
//! One per index per store, kept in [`TABLE_DOC_DICT`], at most [`DICT_CAP`] bytes — the
//! distance LZ4 can refer back, so a longer one would buy it nothing; zstd could use more, but
//! the two share the one dictionary, which is what lets an index hold rows of both. It is
//! **append-only**: the first documents written to the index are appended whole until it is
//! full, and after that it never changes. A row records the dictionary length it was compressed
//! against and is decompressed against that prefix, so a row written while the dictionary held
//! 12,000 bytes reads the same once it holds 65,535. There are no versions and nothing is ever
//! re-encoded. To zstd the bytes are a raw-content dictionary: text to refer back to, with no
//! header or tables of its own, which is what a prefix of it can also be.
//!
//! The bytes a batch appends are committed in the batch's own transaction, and a row is only
//! ever compressed against a length that has already been committed, so no committed row can
//! name bytes the table does not hold.
//!
//! # The row
//!
//! ```text
//! as written:   {"json_blob": ...}                      first byte is `{`
//! LZ4:          0x01 | raw len u32 LE | dict len u16 LE | LZ4 block
//! zstd:         0x02 | raw len u32 LE | dict len u16 LE | zstd frame
//! ```
//!
//! The zstd frame is zstd's standard one with its default flags, as Tantivy writes it: magic
//! number and content size, no checksum, and no dictionary id, which a raw-content dictionary
//! does not have. Leaving out what the header already says would save four to six bytes a row,
//! but needs zstd's experimental API and a reader told the frame is not standard.
//!
//! A stored row has always been a JSON object, so every row written before this module existed
//! is a valid row "as written", and a row that would not shrink is still stored that way. The
//! three cannot be confused: `{` is neither tag.

use std::borrow::Cow;
use std::cell::RefCell;
use std::sync::OnceLock;

use redb::TableDefinition;
use serde::{Deserialize, Serialize};
use zstd_safe::{CCtx, CDict, CParameter, DCtx, ResetDirective};

/// The dictionary of each index in this store, by index name.
pub const TABLE_DOC_DICT: TableDefinition<&str, &[u8]> = TableDefinition::new("_doc_dict");

/// The most a dictionary holds: LZ4 refers back at most 65,535 bytes.
pub const DICT_CAP: usize = 65_535;

/// First byte of a row compressed by LZ4.
const TAG_LZ4: u8 = 0x01;
/// First byte of a row compressed by zstd.
const TAG_ZSTD: u8 = 0x02;
/// Tag, raw length, dictionary length.
const HEADER: usize = 1 + 4 + 2;
/// Below this a row is not worth the attempt.
const MIN_COMPRESS: usize = 64;
/// LZ4 cannot expand a block by more than this factor, so a row claiming more is damaged.
const MAX_LZ4_RATIO: usize = 255;
/// Nor zstd by more than this: its densest block is four bytes standing for 128 KB of one byte.
const MAX_ZSTD_RATIO: usize = 1 << 15;
/// The zstd level rows are compressed at, the fastest of its standard levels.
///
/// Measured on held-out rows against a 64 KB dictionary of their table's first documents,
/// level 3 stores 0.2–3 points less than level 1 for 25–35% more time to compress, and its
/// prepared dictionary takes 462 KB against level 1's 110 KB — kept per index per shard, so
/// 185 MB against 44 MB on a node with a hundred indexes on four shards. Level 5 doubles the
/// time for another point.
const ZSTD_LEVEL: i32 = 1;

thread_local! {
    // A zstd context is a few hundred kilobytes of tables; making one per row would cost more
    // than compressing the row. `None` if zstd could not allocate or configure one, and rows
    // are then stored as written, or refused as unreadable.
    static ZSTD_CCTX: RefCell<Option<CCtx<'static>>> = RefCell::new(zstd_cctx());
    static ZSTD_DCTX: RefCell<Option<DCtx<'static>>> = RefCell::new(zstd_dctx());
}

fn zstd_cctx() -> Option<CCtx<'static>> {
    let mut cctx = CCtx::try_create()?;
    cctx.set_parameter(CParameter::CompressionLevel(ZSTD_LEVEL))
        .ok()?;
    Some(cctx)
}

fn zstd_dctx() -> Option<DCtx<'static>> {
    DCtx::try_create()
}

/// What a node compresses a document row with when it writes one. Rows of either kind are read
/// whatever this is.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DocCompression {
    /// The fastest to write and to read.
    #[default]
    Lz4,
    /// zstd at level 1: smaller rows than LZ4, for more CPU on each write and read of a row.
    Zstd,
}

impl DocCompression {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Lz4 => "lz4",
            Self::Zstd => "zstd",
        }
    }
}

impl std::fmt::Display for DocCompression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for DocCompression {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "lz4" => Ok(Self::Lz4),
            "zstd" => Ok(Self::Zstd),
            other => Err(format!(
                "unknown document compression '{other}': expected 'lz4' or 'zstd'"
            )),
        }
    }
}

/// An index's dictionary as committed at some length, with what zstd prepares from it.
///
/// LZ4 and every decompression use the bytes as they are. zstd, to compress against a
/// dictionary, first indexes it; done per row that would cost many times the row. So the
/// indexed form is made once, on the first row zstd compresses against this dictionary, and
/// kept beside the bytes — on a node set to LZ4 it is never made.
pub struct DocDict {
    bytes: Vec<u8>,
    zstd: OnceLock<Option<CDict<'static>>>,
}

impl DocDict {
    pub fn new(bytes: Vec<u8>) -> Self {
        debug_assert!(bytes.len() <= DICT_CAP);
        Self {
            bytes,
            zstd: OnceLock::new(),
        }
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    /// `None` if zstd could not allocate it; rows are then stored as written.
    ///
    /// A buffer that opened with zstd's own dictionary magic would be parsed as a trained
    /// dictionary, not taken as text. This one opens with the `{` of a stored document.
    fn zstd(&self) -> Option<&CDict<'static>> {
        self.zstd
            .get_or_init(|| CDict::try_create(&self.bytes, ZSTD_LEVEL))
            .as_ref()
    }
}

impl Default for DocDict {
    fn default() -> Self {
        Self::new(Vec::new())
    }
}

impl std::fmt::Debug for DocDict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DocDict")
            .field("len", &self.bytes.len())
            .field("zstd_prepared", &self.zstd.get().is_some())
            .finish()
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum CodecError {
    /// The row was compressed against more dictionary than the caller holds. Not damage by
    /// itself: the caller's copy may be behind the table, and should be read again.
    #[error("row needs {needed} bytes of dictionary, {held} held")]
    DictionaryBehind { needed: usize, held: usize },
    #[error("damaged document row: {0}")]
    Damaged(&'static str),
}

/// How much dictionary `stored` was compressed against; zero for a row stored as written.
pub fn dictionary_needed(stored: &[u8]) -> usize {
    match stored {
        [TAG_LZ4 | TAG_ZSTD, _, _, _, _, lo, hi, ..] => u16::from_le_bytes([*lo, *hi]) as usize,
        _ => 0,
    }
}

/// `raw` as it should be stored: compressed by `method` against `dict` when that is smaller,
/// else `None` for "store it as written".
///
/// `dict` is the dictionary as committed, whole; the row records its length.
pub fn encode(raw: &[u8], dict: &DocDict, method: DocCompression) -> Option<Vec<u8>> {
    if raw.len() < MIN_COMPRESS || raw.len() > u32::MAX as usize || dict.len() > DICT_CAP {
        return None;
    }
    let (tag, bound) = match method {
        DocCompression::Lz4 => (TAG_LZ4, lz4_flex::block::get_maximum_output_size(raw.len())),
        DocCompression::Zstd => (TAG_ZSTD, zstd_safe::compress_bound(raw.len())),
    };
    let mut out = Vec::with_capacity(HEADER + bound);
    out.push(tag);
    out.extend_from_slice(&(raw.len() as u32).to_le_bytes());
    out.extend_from_slice(&(dict.len() as u16).to_le_bytes());
    out.resize(HEADER + bound, 0);
    let block = &mut out[HEADER..];
    let written = match method {
        DocCompression::Lz4 if dict.is_empty() => {
            lz4_flex::block::compress_into(raw, block).ok()?
        }
        DocCompression::Lz4 => {
            lz4_flex::block::compress_into_with_dict(raw, block, dict.as_bytes()).ok()?
        }
        DocCompression::Zstd => ZSTD_CCTX.with_borrow_mut(|cctx| {
            let cctx = cctx.as_mut()?;
            // A frame that failed is left open; the dictionary cannot be changed under it.
            cctx.reset(ResetDirective::SessionOnly).ok()?;
            // Set for every row: the context outlives any one dictionary, and must never be
            // left to compress against the last one it was given.
            if dict.is_empty() {
                cctx.disable_dictionary().ok()?;
            } else {
                cctx.ref_cdict(dict.zstd()?).ok()?;
            }
            cctx.compress2(block, raw).ok()
        })?,
    };
    if HEADER + written >= raw.len() {
        return None;
    }
    out.truncate(HEADER + written);
    Some(out)
}

/// The document a stored row holds. Borrowed when the row is stored as written.
///
/// `dict` may be longer than the row was compressed against — it only ever grows — and the
/// row's own prefix of it is used.
pub fn decode<'a>(stored: &'a [u8], dict: &[u8]) -> Result<Cow<'a, [u8]>, CodecError> {
    let tag = match stored.first() {
        Some(&tag @ (TAG_LZ4 | TAG_ZSTD)) => tag,
        // Every row written before compression existed, and every row that would not shrink.
        Some(b'{') => return Ok(Cow::Borrowed(stored)),
        Some(_) => return Err(CodecError::Damaged("unknown row format")),
        None => return Err(CodecError::Damaged("empty row")),
    };
    if stored.len() < HEADER {
        return Err(CodecError::Damaged("row shorter than its header"));
    }
    let raw_len = u32::from_le_bytes([stored[1], stored[2], stored[3], stored[4]]) as usize;
    let needed = dictionary_needed(stored);
    let block = &stored[HEADER..];
    // The length is the allocation below, and it comes from the file: a damaged row must not
    // be able to ask for four gigabytes.
    let max_ratio = if tag == TAG_LZ4 {
        MAX_LZ4_RATIO
    } else {
        MAX_ZSTD_RATIO
    };
    if raw_len > block.len().saturating_mul(max_ratio) {
        return Err(CodecError::Damaged(
            "length no compressed block of this size can hold",
        ));
    }
    if needed > dict.len() {
        return Err(CodecError::DictionaryBehind {
            needed,
            held: dict.len(),
        });
    }
    if tag == TAG_ZSTD {
        // Capacity, not length: zstd writes into it and sets the length to what it wrote, and
        // refuses a frame that holds more.
        let mut out = Vec::with_capacity(raw_len);
        ZSTD_DCTX
            .with_borrow_mut(|dctx| {
                let dctx = dctx.as_mut().ok_or(())?;
                dctx.decompress_using_dict(&mut out, block, &dict[..needed])
                    .map_err(|_| ())
            })
            .map_err(|()| CodecError::Damaged("zstd frame does not decompress"))?;
        if out.len() != raw_len {
            return Err(CodecError::Damaged("zstd frame is not the length recorded"));
        }
        return Ok(Cow::Owned(out));
    }
    let mut out = vec![0u8; raw_len];
    let read = if needed == 0 {
        lz4_flex::block::decompress_into(block, &mut out)
    } else {
        lz4_flex::block::decompress_into_with_dict(block, &mut out, &dict[..needed])
    }
    .map_err(|_| CodecError::Damaged("LZ4 block does not decompress"))?;
    if read != raw_len {
        return Err(CodecError::Damaged("LZ4 block is not the length recorded"));
    }
    Ok(Cow::Owned(out))
}

/// Append `raw` to a dictionary that still has room, as much of it as fits. `true` if it grew.
pub fn grow(dict: &mut Vec<u8>, raw: &[u8]) -> bool {
    let room = DICT_CAP.saturating_sub(dict.len());
    let take = raw.len().min(room);
    dict.extend_from_slice(&raw[..take]);
    take > 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use DocCompression::{Lz4, Zstd};

    fn doc(i: usize) -> Vec<u8> {
        format!(
            r#"{{"json_blob":{{"title":"Report number {i}","category":"quarterly-review","body":"The committee met on day {i} and resolved to continue the programme as planned.","tags":["alpha","beta"]}}}}"#
        )
        .into_bytes()
    }

    #[test]
    fn a_row_reads_back_against_the_dictionary_as_it_stood_and_as_it_grew() {
        for method in [Lz4, Zstd] {
            let mut dict = Vec::new();
            let mut rows = Vec::new();
            for i in 0..2_000 {
                let raw = doc(i);
                let stored = encode(&raw, &DocDict::new(dict.clone()), method);
                rows.push((raw.clone(), stored, dict.len()));
                grow(&mut dict, &raw);
            }
            assert_eq!(dict.len(), DICT_CAP);
            for (raw, stored, dict_len) in &rows {
                let stored = stored.as_deref().unwrap_or(raw);
                assert_eq!(
                    dictionary_needed(stored),
                    if stored == raw { 0 } else { *dict_len }
                );
                // Against the dictionary of the time, and against the full one.
                assert_eq!(decode(stored, &dict[..*dict_len]).unwrap(), raw.as_slice());
                assert_eq!(decode(stored, &dict).unwrap(), raw.as_slice());
            }
            // The dictionary is what makes rows this size worth compressing.
            let last = &rows[rows.len() - 1];
            assert!(
                last.1.as_ref().unwrap().len() * 3 < last.0.len(),
                "{method}"
            );
        }
    }

    #[test]
    fn rows_of_both_methods_read_against_the_one_dictionary() {
        let mut bytes = Vec::new();
        for i in 0..40 {
            grow(&mut bytes, &doc(i));
        }
        let dict = DocDict::new(bytes);
        let raw = doc(77);
        let lz4 = encode(&raw, &dict, Lz4).unwrap();
        let zstd = encode(&raw, &dict, Zstd).unwrap();
        assert_ne!(lz4[0], zstd[0]);
        assert_eq!(decode(&lz4, dict.as_bytes()).unwrap(), raw.as_slice());
        assert_eq!(decode(&zstd, dict.as_bytes()).unwrap(), raw.as_slice());
        // The prepared dictionary is reused, and gives the same row each time.
        assert_eq!(encode(&raw, &dict, Zstd).unwrap(), zstd);
        // The frame is zstd's standard one, opening with its magic number.
        assert_eq!(zstd[HEADER..HEADER + 4], 0xFD2F_B528u32.to_le_bytes());
        // A row with no dictionary, after rows with one, is compressed against none.
        let alone = encode(&raw, &DocDict::default(), Zstd).unwrap();
        assert_eq!(dictionary_needed(&alone), 0);
        assert_eq!(decode(&alone, &[]).unwrap(), raw.as_slice());
    }

    #[test]
    fn the_method_is_named_as_the_config_writes_it() {
        for method in [Lz4, Zstd] {
            assert_eq!(method.as_str().parse::<DocCompression>(), Ok(method));
            assert_eq!(
                serde_json::to_string(&method).unwrap(),
                format!("\"{method}\"")
            );
        }
        assert!("gzip".parse::<DocCompression>().is_err());
        assert_eq!(DocCompression::default(), Lz4);
    }

    #[test]
    fn a_row_written_before_compression_is_read_as_it_is() {
        let raw = doc(1);
        assert!(matches!(decode(&raw, &[]).unwrap(), Cow::Borrowed(b) if b == raw));
        assert_eq!(dictionary_needed(&raw), 0);
    }

    #[test]
    fn what_would_not_shrink_is_stored_as_written() {
        let none = DocDict::default();
        // Long enough to try, nothing in it to find.
        let noise: Vec<u8> = (0..400u32)
            .map(|i| (i.wrapping_mul(2_654_435_761) >> 24) as u8)
            .collect();
        for method in [Lz4, Zstd] {
            assert_eq!(encode(br#"{"json_blob":{"a":1}}"#, &none, method), None);
            assert_eq!(encode(&noise, &none, method), None);
        }
    }

    #[test]
    fn a_dictionary_shorter_than_the_row_needs_is_named_not_guessed_at() {
        let mut bytes = Vec::new();
        grow(&mut bytes, &doc(0));
        let dict = DocDict::new(bytes);
        for method in [Lz4, Zstd] {
            let stored = encode(&doc(1), &dict, method).unwrap();
            assert_eq!(
                decode(&stored, &dict.as_bytes()[..10]),
                Err(CodecError::DictionaryBehind {
                    needed: dict.len(),
                    held: 10
                })
            );
        }
    }

    #[test]
    fn a_damaged_row_is_refused_before_anything_is_allocated_for_it() {
        let none = DocDict::default();
        for method in [Lz4, Zstd] {
            let stored = encode(&doc(1).repeat(4), &none, method).unwrap();
            let mut huge = stored.clone();
            huge[1..5].copy_from_slice(&u32::MAX.to_le_bytes());
            assert!(matches!(decode(&huge, &[]), Err(CodecError::Damaged(_))));
            let mut longer = stored.clone();
            longer[1..5].copy_from_slice(&(doc(1).len() as u32 * 4 + 1).to_le_bytes());
            assert!(matches!(decode(&longer, &[]), Err(CodecError::Damaged(_))));
            let mut shorter = stored.clone();
            shorter[1..5].copy_from_slice(&(doc(1).len() as u32 * 4 - 1).to_le_bytes());
            assert!(matches!(decode(&shorter, &[]), Err(CodecError::Damaged(_))));
            assert!(matches!(
                decode(&stored[..stored.len() - 3], &[]),
                Err(CodecError::Damaged(_))
            ));
            assert!(matches!(
                decode(&stored[..4], &[]),
                Err(CodecError::Damaged(_))
            ));
        }
        assert!(matches!(
            decode(&[0x03, 1, 2, 3], &[]),
            Err(CodecError::Damaged(_))
        ));
        assert!(matches!(decode(&[], &[]), Err(CodecError::Damaged(_))));
    }
}
