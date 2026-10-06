//! What a field's name and a value's spelling say, apart from any one source: which names
//! identify a document, and which spellings are a boolean.

use serde_json::Value as JsonValue;

/// A boolean, in the spellings the loader maps: `true`/`yes`/`y`/`1` and their opposites, in any
/// case. A blank is not one — it is no value, whatever field it lands in.
pub(crate) fn boolean_word(cell: &str) -> Option<bool> {
    match cell.trim().to_ascii_lowercase().as_str() {
        "true" | "yes" | "y" | "1" => Some(true),
        "false" | "no" | "n" | "0" => Some(false),
        _ => None,
    }
}

/// A value bound for a boolean field, as the boolean it spells: a string [`boolean_word`] takes,
/// or the number `1` or `0`. `None` for anything else, which goes as written for the node to
/// refuse by its reason.
pub(crate) fn boolean_value(value: &JsonValue) -> Option<bool> {
    match value {
        JsonValue::Bool(b) => Some(*b),
        JsonValue::String(s) => boolean_word(s),
        JsonValue::Number(n) => match n.as_u64() {
            Some(1) => Some(true),
            Some(0) => Some(false),
            _ => None,
        },
        _ => None,
    }
}

/// Digest names, most bits first: a longer digest spreads further and collides less.
const DIGESTS: &[&str] = &["sha512", "sha384", "sha256", "sha224", "sha1", "md5"];
/// A digest of some kind, unnamed.
const HASH_WORDS: &[&str] = &["hash", "digest", "checksum", "fingerprint"];
const UUID_WORDS: &[&str] = &["uuid", "guid"];
const ID_WORDS: &[&str] = &["id", "uid", "identifier"];
const KEY_WORDS: &[&str] = &["key", "pk"];
/// What a running number is called.
const SEQUENCE_WORDS: &[&str] = &[
    "seq", "sequence", "seqno", "serial", "no", "nr", "num", "number", "rownum",
];

/// The rank of a name that says nothing about identity.
pub(crate) const UNNAMED_RANK: u8 = 13;

/// A field name's words: split at anything not a letter or digit, and where a lowercase letter or
/// digit meets an uppercase one — `fileSHA256` is `file sha256`, `MD5Hash` is `md5 hash`, `userID`
/// is `user id`. Lowercased.
fn name_words(name: &str) -> Vec<String> {
    let mut words = Vec::new();
    let mut current = String::new();
    let mut prev: Option<char> = None;
    for c in name.chars() {
        if !c.is_ascii_alphanumeric() {
            if !current.is_empty() {
                words.push(std::mem::take(&mut current));
            }
            prev = None;
            continue;
        }
        if c.is_ascii_uppercase()
            && prev.is_some_and(|p| p.is_ascii_lowercase() || p.is_ascii_digit())
            && !current.is_empty()
        {
            words.push(std::mem::take(&mut current));
        }
        current.push(c.to_ascii_lowercase());
        prev = Some(c);
    }
    if !current.is_empty() {
        words.push(current);
    }
    words
}

/// How strongly a field's name says it identifies a document; lower is stronger.
///
/// A field named exactly `id`; then a digest by name (`sha256`, `file_sha1`), longest first; a
/// hash of no named kind; a `uuid`/`guid`; anything ending in an id (`user_id`, `videoId`,
/// `userid`); an id, key or `pk` anywhere in the name; a sequence or serial number; and last any
/// name with `id` in it at all. A name says only which candidate to prefer — whether a field is
/// one is for its values to say.
pub(crate) fn id_name_rank(name: &str) -> u8 {
    let lower = name.to_ascii_lowercase();
    if lower == "id" {
        return 0;
    }
    let words = name_words(name);
    let has = |set: &[&str]| words.iter().any(|w| set.contains(&w.as_str()));
    if let Some(pos) = DIGESTS.iter().position(|d| words.iter().any(|w| w == d)) {
        return 1 + pos as u8;
    }
    if has(HASH_WORDS) {
        7
    } else if has(UUID_WORDS) || UUID_WORDS.iter().any(|u| lower.ends_with(u)) {
        8
    } else if words.last().is_some_and(|w| ID_WORDS.contains(&w.as_str())) || lower.ends_with("id")
    {
        9
    } else if has(ID_WORDS) || has(KEY_WORDS) {
        10
    } else if has(SEQUENCE_WORDS) {
        11
    } else if lower.contains("id") {
        12
    } else {
        UNNAMED_RANK
    }
}

/// Whether a field's name says it identifies something — the columns whose numbers are codes.
pub(crate) fn is_id_like_name(name: &str) -> bool {
    id_name_rank(name) < UNNAMED_RANK
}

/// Whether a field's name is one the shadow field keeps lowercased: `ID` and `SHA256` name the
/// same field their spelling hides.
pub(crate) fn is_canonical_id_name(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    lower == "id" || DIGESTS.contains(&lower.as_str())
}
