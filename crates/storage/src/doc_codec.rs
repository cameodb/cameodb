//! How a document row in `data_<index>` is stored: as written, or LZ4-compressed against the
//! index's dictionary.
//!
//! A document here is small — a kilobyte or two of JSON — and repeats little inside itself; what
//! it repeats is its neighbours: the field names, the fixed values, the turns of phrase of the
//! collection. LZ4 on one such document alone saves a fifth. Against a dictionary of the
//! collection's own earlier documents it saves a half to three quarters, measured on text
//! indexes, and each row still reads and writes on its own.
//!
//! # The dictionary
//!
//! One per index per store, kept in [`TABLE_DOC_DICT`], at most [`DICT_CAP`] bytes — the
//! distance LZ4 can refer back, so a longer one would buy nothing. It is **append-only**: the
//! first documents written to the index are appended whole until it is full, and after that it
//! never changes. A row records the dictionary length it was compressed against and is
//! decompressed against that prefix, so a row written while the dictionary held 12,000 bytes
//! reads the same once it holds 65,535. There are no versions and nothing is ever re-encoded.
//!
//! The bytes a batch appends are committed in the batch's own transaction, and a row is only
//! ever compressed against a length that has already been committed, so no committed row can
//! name bytes the table does not hold.
//!
//! # The row
//!
//! ```text
//! as written:   {"json_blob": ...}                      first byte is `{`
//! compressed:   0x01 | raw len u32 LE | dict len u16 LE | LZ4 block
//! ```
//!
//! A stored row has always been a JSON object, so every row written before this module existed
//! is a valid row "as written", and a row that would not shrink is still stored that way. The
//! two cannot be confused: `{` is not `0x01`.

use std::borrow::Cow;

use redb::TableDefinition;

/// The dictionary of each index in this store, by index name.
pub const TABLE_DOC_DICT: TableDefinition<&str, &[u8]> = TableDefinition::new("_doc_dict");

/// The most a dictionary holds: LZ4 refers back at most 65,535 bytes.
pub const DICT_CAP: usize = 65_535;

/// First byte of a compressed row.
const TAG_LZ4: u8 = 0x01;
/// Tag, raw length, dictionary length.
const HEADER: usize = 1 + 4 + 2;
/// Below this a row is not worth the attempt.
const MIN_COMPRESS: usize = 64;
/// LZ4 cannot expand a block by more than this factor, so a row claiming more is damaged.
const MAX_LZ4_RATIO: usize = 255;

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
        [TAG_LZ4, _, _, _, _, lo, hi, ..] => u16::from_le_bytes([*lo, *hi]) as usize,
        _ => 0,
    }
}

/// `raw` as it should be stored: compressed against `dict` when that is smaller, else `None`
/// for "store it as written".
///
/// `dict` is the dictionary as committed, whole; the row records its length.
pub fn encode(raw: &[u8], dict: &[u8]) -> Option<Vec<u8>> {
    debug_assert!(dict.len() <= DICT_CAP);
    if raw.len() < MIN_COMPRESS || raw.len() > u32::MAX as usize || dict.len() > DICT_CAP {
        return None;
    }
    let mut out = Vec::with_capacity(HEADER + lz4_flex::block::get_maximum_output_size(raw.len()));
    out.push(TAG_LZ4);
    out.extend_from_slice(&(raw.len() as u32).to_le_bytes());
    out.extend_from_slice(&(dict.len() as u16).to_le_bytes());
    out.resize(out.capacity(), 0);
    let written = if dict.is_empty() {
        lz4_flex::block::compress_into(raw, &mut out[HEADER..])
    } else {
        lz4_flex::block::compress_into_with_dict(raw, &mut out[HEADER..], dict)
    }
    .ok()?;
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
    match stored.first() {
        Some(&TAG_LZ4) => {}
        // Every row written before compression existed, and every row that would not shrink.
        Some(b'{') => return Ok(Cow::Borrowed(stored)),
        Some(_) => return Err(CodecError::Damaged("unknown row format")),
        None => return Err(CodecError::Damaged("empty row")),
    }
    if stored.len() < HEADER {
        return Err(CodecError::Damaged("row shorter than its header"));
    }
    let raw_len = u32::from_le_bytes([stored[1], stored[2], stored[3], stored[4]]) as usize;
    let needed = dictionary_needed(stored);
    let block = &stored[HEADER..];
    // The length is the allocation below, and it comes from the file: a damaged row must not
    // be able to ask for four gigabytes.
    if raw_len > block.len().saturating_mul(MAX_LZ4_RATIO) {
        return Err(CodecError::Damaged(
            "length no LZ4 block of this size can hold",
        ));
    }
    if needed > dict.len() {
        return Err(CodecError::DictionaryBehind {
            needed,
            held: dict.len(),
        });
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

    fn doc(i: usize) -> Vec<u8> {
        format!(
            r#"{{"json_blob":{{"title":"Report number {i}","category":"quarterly-review","body":"The committee met on day {i} and resolved to continue the programme as planned.","tags":["alpha","beta"]}}}}"#
        )
        .into_bytes()
    }

    #[test]
    fn a_row_reads_back_against_the_dictionary_as_it_stood_and_as_it_grew() {
        let mut dict = Vec::new();
        let mut rows = Vec::new();
        for i in 0..2_000 {
            let raw = doc(i);
            rows.push((raw.clone(), encode(&raw, &dict), dict.len()));
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
        assert!(last.1.as_ref().unwrap().len() * 3 < last.0.len());
    }

    #[test]
    fn a_row_written_before_compression_is_read_as_it_is() {
        let raw = doc(1);
        assert!(matches!(decode(&raw, &[]).unwrap(), Cow::Borrowed(b) if b == raw));
        assert_eq!(dictionary_needed(&raw), 0);
    }

    #[test]
    fn what_would_not_shrink_is_stored_as_written() {
        assert_eq!(encode(br#"{"json_blob":{"a":1}}"#, &[]), None);
        // Long enough to try, nothing in it to find.
        let noise: Vec<u8> = (0..400u32)
            .map(|i| (i.wrapping_mul(2_654_435_761) >> 24) as u8)
            .collect();
        assert_eq!(encode(&noise, &[]), None);
    }

    #[test]
    fn a_dictionary_shorter_than_the_row_needs_is_named_not_guessed_at() {
        let mut dict = Vec::new();
        grow(&mut dict, &doc(0));
        let stored = encode(&doc(1), &dict).unwrap();
        assert_eq!(
            decode(&stored, &dict[..10]),
            Err(CodecError::DictionaryBehind {
                needed: dict.len(),
                held: 10
            })
        );
    }

    #[test]
    fn a_damaged_row_is_refused_before_anything_is_allocated_for_it() {
        let stored = encode(&doc(1).repeat(4), &[]).unwrap();
        let mut huge = stored.clone();
        huge[1..5].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(matches!(decode(&huge, &[]), Err(CodecError::Damaged(_))));
        let mut longer = stored.clone();
        longer[1..5].copy_from_slice(&(doc(1).len() as u32 * 4 + 1).to_le_bytes());
        assert!(matches!(decode(&longer, &[]), Err(CodecError::Damaged(_))));
        assert!(matches!(
            decode(&stored[..stored.len() - 3], &[]),
            Err(CodecError::Damaged(_))
        ));
        assert!(matches!(
            decode(&stored[..4], &[]),
            Err(CodecError::Damaged(_))
        ));
        assert!(matches!(
            decode(&[0x02, 1, 2, 3], &[]),
            Err(CodecError::Damaged(_))
        ));
        assert!(matches!(decode(&[], &[]), Err(CodecError::Damaged(_))));
    }
}
