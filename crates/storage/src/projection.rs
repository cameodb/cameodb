//! Field projection on a returned document, shared by the node-level merge and the
//! per-shard search it is now pushed down into.
//!
//! It lives in storage because a shard is where the full document already exists: applying
//! the projection there keeps 80k hits over 8 shards from materializing on the wire in
//! full when the caller asked for one field. The node still applies it again after the
//! merge — a sort can require fields the projection drops to be carried up — and one
//! function serving both places is what keeps the two projections identical.
//!
//! The array cap sits beside it for the same reason: the shard is where a field of a few
//! hundred thousand elements can be shortened before it costs the wire anything.

use serde_json::Value as JsonValue;

/// Apply field projection to a JSON document, keeping only specified fields.
/// Always preserves metadata fields (_score, _sort_key, etc.) that start with underscore.
///
/// User-specified fields are inserted first in the exact order given by the projection
/// list, so the response field order matches the user's `return` clause. Metadata fields
/// are appended afterwards. This guarantees a consistent field order whether or not a
/// sort is active — the internal `_sort_key` (if present) simply appears at the end and
/// is stripped by `strip_sort_keys` at the client boundary.
pub fn apply_field_projection(doc: JsonValue, fields: &[String]) -> JsonValue {
    if let JsonValue::Object(mut map) = doc {
        let mut filtered = serde_json::Map::new();

        // Add requested fields first, in user-specified projection order
        for field in fields {
            if let Some(value) = map.remove(field) {
                filtered.insert(field.clone(), value);
            }
        }

        // Then append metadata fields (those starting with _)
        for (key, value) in map.iter() {
            if key.starts_with('_') {
                filtered.insert(key.clone(), value.clone());
            }
        }

        JsonValue::Object(filtered)
    } else {
        doc
    }
}

/// The key a hit names its shortened arrays under, each with the length it had.
///
/// `_`-prefixed, so [`apply_field_projection`] carries it through whatever fields a caller
/// asked for — the node projects the merged page again, and the account has to survive it.
pub const TRUNCATED_ARRAYS_FIELD: &str = "_truncated_arrays";

/// Keep at most `max` elements of every array in a document, and say which were cut.
///
/// A search bounds how many hits it returns; this bounds how large one hit can be. A single
/// field of a few hundred thousand network addresses is megabytes per document, and a page of
/// them is more than any client or agent context asked for. The first `max` elements are kept
/// in stored order, at every depth: an array inside an object, and an array of objects —
/// capped at its own level, then each object it kept capped in turn.
///
/// Every array that lost elements is recorded under [`TRUNCATED_ARRAYS_FIELD`] by its path —
/// dotted through objects, `[i]` through array elements — with its original length, so a
/// caller knows what it did not get and can ask again for more. Nothing is recorded when
/// nothing was cut.
///
/// Idempotent, which is what lets the node apply it again after a merge: an array already at
/// most `max` is left alone, and an entry already in the account is kept rather than replaced
/// — it holds the length the array had before the first cut, which a second pass cannot see.
pub fn cap_document_arrays(doc: &mut JsonValue, max: usize) {
    let JsonValue::Object(map) = doc else {
        return;
    };
    let mut truncated: Vec<(String, usize)> = Vec::new();
    for (key, value) in map.iter_mut() {
        if key == TRUNCATED_ARRAYS_FIELD {
            continue;
        }
        cap_value(value, max, key, &mut truncated);
    }
    if truncated.is_empty() {
        return;
    }
    let account = map
        .entry(TRUNCATED_ARRAYS_FIELD)
        .or_insert_with(|| JsonValue::Object(serde_json::Map::new()));
    if let JsonValue::Object(account) = account {
        for (path, length) in truncated {
            account.entry(path).or_insert(JsonValue::from(length));
        }
    }
}

fn cap_value(value: &mut JsonValue, max: usize, path: &str, truncated: &mut Vec<(String, usize)>) {
    match value {
        JsonValue::Array(items) => {
            if items.len() > max {
                truncated.push((path.to_string(), items.len()));
                items.truncate(max);
            }
            for (position, item) in items.iter_mut().enumerate() {
                if matches!(item, JsonValue::Array(_) | JsonValue::Object(_)) {
                    cap_value(item, max, &format!("{path}[{position}]"), truncated);
                }
            }
        }
        JsonValue::Object(map) => {
            for (key, item) in map.iter_mut() {
                if matches!(item, JsonValue::Array(_) | JsonValue::Object(_)) {
                    cap_value(item, max, &format!("{path}.{key}"), truncated);
                }
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn an_array_past_the_cap_keeps_its_head_and_reports_its_length() {
        let mut doc = json!({"id": "d1", "ips": ["a", "b", "c", "d"], "tag": "x"});
        cap_document_arrays(&mut doc, 2);
        assert_eq!(doc["ips"], json!(["a", "b"]));
        assert_eq!(doc[TRUNCATED_ARRAYS_FIELD], json!({"ips": 4}));
        assert_eq!(doc["id"], json!("d1"));
        assert_eq!(doc["tag"], json!("x"));
    }

    #[test]
    fn nothing_cut_adds_no_account() {
        let mut doc = json!({"ips": ["a", "b"], "empty": []});
        cap_document_arrays(&mut doc, 2);
        assert_eq!(doc, json!({"ips": ["a", "b"], "empty": []}));
    }

    /// `array 0` is the sizes without the payload: every non-empty array comes back empty,
    /// and its length is in the account.
    #[test]
    fn a_cap_of_zero_empties_every_array_and_reports_every_length() {
        let mut doc = json!({"ips": ["a", "b", "c"], "empty": [], "n": 1});
        cap_document_arrays(&mut doc, 0);
        assert_eq!(doc["ips"], json!([]));
        assert_eq!(doc["empty"], json!([]));
        assert_eq!(doc[TRUNCATED_ARRAYS_FIELD], json!({"ips": 3}));
    }

    #[test]
    fn nested_arrays_are_capped_and_named_by_path() {
        let mut doc = json!({
            "network": {"ipv4": [1, 2, 3], "port": 80},
            "hosts": [
                {"name": "a", "ports": [1, 2, 3]},
                {"name": "b", "ports": [1]},
                {"name": "c", "ports": [1, 2, 3, 4]},
            ],
            "matrix": [[1, 2, 3], [4]],
        });
        cap_document_arrays(&mut doc, 2);
        assert_eq!(doc["network"]["ipv4"], json!([1, 2]));
        assert_eq!(doc["network"]["port"], json!(80));
        assert_eq!(doc["hosts"].as_array().unwrap().len(), 2);
        assert_eq!(doc["hosts"][0]["ports"], json!([1, 2]));
        assert_eq!(doc["hosts"][1]["ports"], json!([1]));
        assert_eq!(doc["matrix"], json!([[1, 2], [4]]));
        assert_eq!(
            doc[TRUNCATED_ARRAYS_FIELD],
            json!({
                "network.ipv4": 3,
                "hosts": 3,
                "hosts[0].ports": 3,
                "matrix[0]": 3,
            })
        );
    }

    /// The node caps the merged page again; that pass must not shorten what the shard kept,
    /// nor overwrite the original lengths the shard recorded with the shortened ones.
    #[test]
    fn capping_twice_changes_nothing() {
        let mut doc = json!({"ips": ["a", "b", "c", "d"]});
        cap_document_arrays(&mut doc, 2);
        let once = doc.clone();
        cap_document_arrays(&mut doc, 2);
        assert_eq!(doc, once);
    }

    /// A larger cap above a smaller one — an old peer's whole array capped by the node — keeps
    /// the account it already holds and adds what it cut itself.
    #[test]
    fn a_second_cap_adds_to_the_account_without_replacing_it() {
        let mut doc = json!({"ips": ["a", "b", "c", "d"], "dns": ["x", "y"]});
        cap_document_arrays(&mut doc, 2);
        cap_document_arrays(&mut doc, 1);
        assert_eq!(doc["ips"], json!(["a"]));
        assert_eq!(doc["dns"], json!(["x"]));
        assert_eq!(
            doc[TRUNCATED_ARRAYS_FIELD],
            json!({"ips": 4, "dns": 2}),
            "the first cut's original length stands, and the second cut is added"
        );
    }

    /// Metadata the node stamps on a hit is not document data, and the account itself is
    /// never capped — a document with more cut arrays than the cap would otherwise lose them.
    #[test]
    fn the_account_is_not_capped() {
        let mut doc = json!({"a": [1, 2], "b": [1, 2], "c": [1, 2]});
        cap_document_arrays(&mut doc, 0);
        cap_document_arrays(&mut doc, 0);
        assert_eq!(doc[TRUNCATED_ARRAYS_FIELD], json!({"a": 2, "b": 2, "c": 2}));
    }
}
