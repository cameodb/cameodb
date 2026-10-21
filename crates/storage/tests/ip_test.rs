//! Ip fields as a caller uses them: a pasted address finds the field that holds it, CIDR is the
//! range it stands for, an open-ended IPv4 range stays among IPv4 addresses, and a fast ip field
//! sorts by address rather than by its spelling.

use std::collections::HashMap;

use storage::{
    FieldDef, HybridStore, IndexSchema, SortOrder, SortSpec, StorageConfig, TantivyFieldType, WalOp,
};
use tempfile::TempDir;

fn config(path: std::path::PathBuf) -> StorageConfig {
    StorageConfig {
        max_open_indexes: 0,
        shard_path: path,
        indexer_memory_budget: 32 * 1024 * 1024,
        indexer_memory_min_mb: 16,
        indexer_memory_max_mb: 256,
        total_memory_limit_bytes: 2048 * 1024 * 1024,
        memory_pressure_threshold_percent: 80,
        indexer_num_threads: 1,
        merge_num_threads: 1,
        default_batch_size: 100_000,
        wal_sync: true,
        quick_repair: true,
        commit_interval_ms: 0,
        query: Default::default(),
    }
}

fn field(field_type: TantivyFieldType, fast: bool) -> FieldDef {
    let mut def = FieldDef::new("placeholder".to_string(), field_type);
    def.fast = Some(fast);
    def.stored = false;
    def
}

/// `src` and `ips` are plain ip fields, `seen` a fast one; `note` is text, a default field.
fn store(temp: &TempDir) -> HybridStore {
    let store = HybridStore::new(config(temp.path().to_path_buf()), 1).unwrap();
    let mut fields = HashMap::new();
    fields.insert("src".into(), field(TantivyFieldType::Ip, false));
    fields.insert("ips".into(), field(TantivyFieldType::Ip, false));
    fields.insert("seen".into(), field(TantivyFieldType::Ip, true));
    fields.insert("note".into(), field(TantivyFieldType::Text, false));
    let schema = IndexSchema {
        fields,
        ..Default::default()
    };
    store.store_schema_and_cache("net", &schema).unwrap();

    let docs = [
        (
            "d1",
            serde_json::json!({"src": "10.1.2.3", "ips": ["192.168.0.1", "8.8.8.8"], "seen": "10.1.2.3", "note": "router"}),
        ),
        (
            "d2",
            serde_json::json!({"src": "2001:db8::1", "ips": ["::1"], "seen": "2001:db8::1", "note": "v6 host"}),
        ),
        (
            "d3",
            serde_json::json!({"src": "::ffff:10.1.2.4", "seen": "10.1.2.4", "note": "mapped"}),
        ),
        (
            "d4",
            serde_json::json!({"src": "10.200.0.1", "seen": "10.200.0.1", "note": "far"}),
        ),
        ("d5", serde_json::json!({"note": "no address"})),
        (
            "d6",
            serde_json::json!({"src": "9.9.9.9", "seen": "9.9.9.9", "note": "resolver"}),
        ),
    ];
    for (id, doc) in docs {
        store
            .apply_write(
                "net",
                WalOp::Put {
                    id: id.to_string(),
                    json_blob: Some(doc),
                },
            )
            .unwrap();
    }
    store.commit_index("net").unwrap();
    store
}

fn ids(store: &HybridStore, query: &str) -> Vec<String> {
    let outcome = store.search_documents("net", query, 20, None).unwrap();
    assert!(
        outcome.discarded.is_empty(),
        "{query:?}: {:?}",
        outcome.discarded
    );
    let mut ids: Vec<String> = outcome
        .hits
        .iter()
        .map(|(_, hit)| hit["id"].as_str().unwrap().to_string())
        .collect();
    ids.sort();
    ids
}

/// A pasted address, or block, reaches the ip fields as well as the text ones; a word does not
/// change.
#[test]
fn a_bare_address_searches_the_ip_fields() {
    let temp = TempDir::new().unwrap();
    let store = store(&temp);
    assert_eq!(ids(&store, "8.8.8.8"), ["d1"], "held only in the ip list");
    assert_eq!(ids(&store, "\"2001:db8::1\""), ["d2"]);
    assert_eq!(ids(&store, "10.1.2.0/24"), ["d1", "d3"]);
    assert_eq!(ids(&store, "\"2001:db8::/32\""), ["d2"]);
    assert_eq!(ids(&store, "router"), ["d1"]);
    assert_eq!(ids(&store, "8.8.8.8 AND note:router"), ["d1"]);
}

/// CIDR on an ip field is its range, exact for IPv4 and IPv6, host bits below the prefix ignored.
#[test]
fn cidr_on_an_ip_field_is_its_range() {
    let temp = TempDir::new().unwrap();
    let store = store(&temp);
    assert_eq!(ids(&store, "src:10.0.0.0/8"), ["d1", "d3", "d4"]);
    assert_eq!(ids(&store, "src:10.1.2.0/24"), ["d1", "d3"]);
    assert_eq!(ids(&store, "src:10.1.2.3/32"), ["d1"]);
    assert_eq!(ids(&store, "src:10.9.9.9/8"), ["d1", "d3", "d4"]);
    assert_eq!(ids(&store, "src:\"2001:db8::/32\""), ["d2"]);
    assert_eq!(ids(&store, "ips:192.168.0.0/16"), ["d1"]);
    assert_eq!(ids(&store, "src:0.0.0.0/0"), ["d1", "d3", "d4", "d6"]);
}

/// An IPv4 bound keeps an open-ended range among IPv4 addresses; an IPv6 bound does not.
#[test]
fn an_open_range_with_an_ipv4_bound_stays_among_ipv4_addresses() {
    let temp = TempDir::new().unwrap();
    let store = store(&temp);
    assert_eq!(
        ids(&store, "src:>10.1.2.3"),
        ["d3", "d4"],
        "not 2001:db8::1"
    );
    assert_eq!(ids(&store, "src:[10.1.2.4 TO *]"), ["d3", "d4"]);
    assert_eq!(ids(&store, "ips:<10.0.0.0"), ["d1"], "8.8.8.8, not ::1");
    assert_eq!(ids(&store, "src:>\"2001:db8::\""), ["d2"]);
    assert_eq!(ids(&store, "src:[10.1.2.0 TO 10.1.2.255]"), ["d1", "d3"]);
}

/// A prefix on an ip field is dropped and named as the block it stands for; a value that is not an
/// address is reported in terms of what the field takes.
#[test]
fn what_an_ip_field_cannot_match_is_reported_as_what_it_can() {
    let temp = TempDir::new().unwrap();
    let store = store(&temp);
    let prefix = store
        .search_documents("net", "src:10.1.*", 10, None)
        .unwrap();
    assert!(prefix.emptied, "{:?}", prefix.discarded);
    assert!(
        prefix
            .discarded
            .iter()
            .any(|note| note.contains("`src:10.1.0.0/16`")),
        "{:?}",
        prefix.discarded
    );
    let junk = store
        .search_documents("net", "src:router", 10, None)
        .unwrap();
    assert!(
        junk.discarded
            .iter()
            .any(|note| note.contains("not an IPv4 or IPv6 address")),
        "{:?}",
        junk.discarded
    );
}

/// A fast ip field sorts by address — `9.9.9.9` before `10.1.2.3`, IPv4 before IPv6 — and a
/// document without one comes last in either order.
#[test]
fn a_fast_ip_field_sorts_by_address() {
    let temp = TempDir::new().unwrap();
    let store = store(&temp);
    let sorted = |order: SortOrder| -> Vec<String> {
        let sort = SortSpec {
            field: "seen".to_string(),
            order,
        };
        store
            .search_documents("net", "*", 10, Some(&sort))
            .unwrap()
            .hits
            .iter()
            .map(|(_, hit)| hit["id"].as_str().unwrap().to_string())
            .collect()
    };
    assert_eq!(sorted(SortOrder::Asc), ["d6", "d1", "d3", "d4", "d2", "d5"]);
    assert_eq!(
        sorted(SortOrder::Desc),
        ["d2", "d4", "d3", "d1", "d6", "d5"]
    );
}
