//! A cut array leads with the elements the query matched (ROADMAP P6).
//!
//! The array cap keeps the first N elements in stored order, and a clause matches an element
//! anywhere in the array — so a hit used to show none of what made it one. With the query handed
//! to the fetch phase, an array the cap cuts keeps its matching elements first, in stored order,
//! fills the rest of the cap from the others, and names the positions it moved under
//! `_matched_elements`. An array that is not cut is untouched.

use serde_json::{Value as JsonValue, json};
use storage::{
    HybridStore, IndexSchema, MATCHED_ELEMENTS_FIELD, SearchOptions, StorageConfig,
    TRUNCATED_ARRAYS_FIELD, WalOp,
};
use tempfile::TempDir;

const INDEX: &str = "intel";

fn config(path: &std::path::Path) -> StorageConfig {
    StorageConfig {
        max_open_indexes: 0,
        shard_path: path.to_path_buf(),
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

fn declared(body: JsonValue) -> IndexSchema {
    let mut schema: IndexSchema = serde_json::from_value(body).expect("declaration");
    schema.normalize_after_deserialization();
    schema.validate_ghosts().expect("valid ghosts");
    schema
}

/// A store holding `docs` under a schema of every kind of array the matcher reads.
fn store_with(temp: &TempDir, docs: &[(&str, JsonValue)]) -> HybridStore {
    let store = HybridStore::new(config(temp.path()), 1).expect("store");
    let schema = declared(json!({"fields": {
        "title": {"type": "text"},
        "domains": {"type": "string"},
        "notes": {"type": "text"},
        "ports": {"type": "u64"},
        "seen": {"type": "date"},
        "addrs": {"type": "ip"},
        "lang": {"type": "string"},
        "text": {"type": "text", "indexed": false},
        "text_en": {"type": "text", "tokenizer": "en_stem",
                    "ghost": {"source": "text", "when": {"field": "lang", "in": ["en"]}}},
    }}));
    store
        .store_schema_and_cache(INDEX, &schema)
        .expect("schema");
    for (id, body) in docs {
        store
            .apply_write(
                INDEX,
                WalOp::Put {
                    id: id.to_string(),
                    json_blob: Some(body.clone()),
                },
            )
            .expect("write");
    }
    store.commit_index(INDEX).expect("commit");
    store
}

/// The one hit `query` finds, its arrays cut to `max`, leading with matches unless `plain`.
fn hit(store: &HybridStore, query: &str, max: usize) -> JsonValue {
    hit_with(store, query, max, None, true)
}

fn hit_with(
    store: &HybridStore,
    query: &str,
    max: usize,
    fields: Option<&[String]>,
    leading: bool,
) -> JsonValue {
    let opts = SearchOptions {
        fields,
        array_limit: Some(max),
        match_query: leading.then_some(query),
        ..Default::default()
    };
    let outcome = store
        .search_documents_with(INDEX, query, 10, None, &opts)
        .expect("search");
    assert_eq!(outcome.hits.len(), 1, "{query}: {:?}", outcome.discarded);
    outcome.hits[0].1.clone()
}

/// `n` domain names in sorted order, `bad.example.com` at position `at`.
fn domains(n: usize, at: usize) -> Vec<String> {
    (0..n)
        .map(|i| {
            if i == at {
                "bad.example.com".to_string()
            } else {
                format!("a{i:05}.example.com")
            }
        })
        .collect()
}

#[test]
fn a_cut_array_leads_with_its_match_and_fills_from_the_head() {
    let temp = TempDir::new().unwrap();
    let store = store_with(
        &temp,
        &[("d1", json!({"title": "sample", "domains": domains(50, 40)}))],
    );
    let found = hit(&store, r#"domains:"bad.example.com""#, 3);
    assert_eq!(
        found["domains"],
        json!([
            "bad.example.com",
            "a00000.example.com",
            "a00001.example.com"
        ])
    );
    assert_eq!(found[MATCHED_ELEMENTS_FIELD], json!({"domains": [40]}));
    assert_eq!(found[TRUNCATED_ARRAYS_FIELD], json!({"domains": 50}));

    // A match already inside the head moves to the front too, and nothing appears twice.
    let store_head = {
        let temp = TempDir::new().unwrap();
        let store = store_with(
            &temp,
            &[("d1", json!({"title": "sample", "domains": domains(50, 1)}))],
        );
        let found = hit(&store, r#"domains:"bad.example.com""#, 3);
        (temp, found)
    };
    assert_eq!(
        store_head.1["domains"],
        json!([
            "bad.example.com",
            "a00000.example.com",
            "a00002.example.com"
        ])
    );
    assert_eq!(
        store_head.1[MATCHED_ELEMENTS_FIELD],
        json!({"domains": [1]})
    );
}

#[test]
fn an_array_that_is_not_cut_keeps_its_order() {
    let temp = TempDir::new().unwrap();
    let list = domains(5, 4);
    let store = store_with(
        &temp,
        &[("d1", json!({"title": "sample", "domains": list}))],
    );
    let found = hit(&store, r#"domains:"bad.example.com""#, 10);
    assert_eq!(found["domains"], json!(list));
    assert!(found.get(MATCHED_ELEMENTS_FIELD).is_none(), "{found}");
    assert!(found.get(TRUNCATED_ARRAYS_FIELD).is_none(), "{found}");
}

#[test]
fn without_the_query_a_cut_array_keeps_its_head() {
    let temp = TempDir::new().unwrap();
    let store = store_with(
        &temp,
        &[("d1", json!({"title": "sample", "domains": domains(50, 40)}))],
    );
    let found = hit_with(&store, r#"domains:"bad.example.com""#, 3, None, false);
    assert_eq!(
        found["domains"],
        json!([
            "a00000.example.com",
            "a00001.example.com",
            "a00002.example.com"
        ])
    );
    assert!(found.get(MATCHED_ELEMENTS_FIELD).is_none(), "{found}");
}

#[test]
fn matches_past_the_cap_keep_the_first_in_stored_order() {
    let temp = TempDir::new().unwrap();
    let ports: Vec<u64> = (1..=100).collect();
    let store = store_with(&temp, &[("d1", json!({"title": "sample", "ports": ports}))]);
    let found = hit(&store, "ports:[50 TO 60]", 3);
    assert_eq!(found["ports"], json!([50, 51, 52]));
    assert_eq!(
        found[MATCHED_ELEMENTS_FIELD],
        json!({"ports": [49, 50, 51]})
    );
}

#[test]
fn every_kind_of_clause_names_its_element() {
    let temp = TempDir::new().unwrap();
    let mut notes: Vec<String> = (0..20).map(|i| format!("routine entry {i}")).collect();
    notes[12] = "the quick brown fox".to_string();
    notes[15] = "a reticulated python".to_string();
    let mut ports: Vec<u64> = (1000..1020).collect();
    ports[17] = 3389;
    let mut seen: Vec<String> = (1..=20)
        .map(|d| format!("2025-03-{d:02}T00:00:00Z"))
        .collect();
    seen[18] = "2026-07-04T12:00:00Z".to_string();
    let mut addrs: Vec<String> = (0..20).map(|i| format!("192.168.0.{i}")).collect();
    addrs[19] = "10.9.8.7".to_string();
    let store = store_with(
        &temp,
        &[(
            "d1",
            json!({"title": "sample", "notes": notes, "ports": ports, "seen": seen, "addrs": addrs}),
        )],
    );

    let cases: &[(&str, &str, JsonValue)] = &[
        (r#"notes:"quick brown""#, "notes", json!([12])),
        (r#"notes:"quick fox"~1"#, "notes", json!([12])),
        ("notes:reticul*", "notes", json!([15])),
        ("notes:[pythom TO pythoo]", "notes", json!([15])),
        ("notes: IN [fox python]", "notes", json!([12, 15])),
        ("ports:3389", "ports", json!([17])),
        ("ports: IN [22 3389]", "ports", json!([17])),
        (
            "seen:[2026-01-01T00:00:00Z TO 2027-01-01T00:00:00Z]",
            "seen",
            json!([18]),
        ),
        ("addrs:10.9.0.0/16", "addrs", json!([19])),
        ("addrs:10.9.8.7", "addrs", json!([19])),
    ];
    for (query, field, positions) in cases {
        let found = hit(&store, query, 2);
        assert_eq!(
            found[MATCHED_ELEMENTS_FIELD][field], *positions,
            "{query}: {found}"
        );
        let first = positions[0].as_u64().unwrap() as usize;
        let stored = match *field {
            "notes" => json!(notes[first]),
            "ports" => json!(ports[first]),
            "seen" => json!(seen[first]),
            _ => json!(addrs[first]),
        };
        assert_eq!(found[field][0], stored, "{query}");
    }
}

#[test]
fn a_bare_term_matches_through_the_default_fields() {
    let temp = TempDir::new().unwrap();
    let mut notes: Vec<String> = (0..20).map(|i| format!("routine entry {i}")).collect();
    notes[9] = "lateral movement observed".to_string();
    let store = store_with(&temp, &[("d1", json!({"title": "sample", "notes": notes}))]);
    let found = hit(&store, "lateral", 2);
    assert_eq!(found[MATCHED_ELEMENTS_FIELD], json!({"notes": [9]}));
    assert_eq!(found["notes"][0], json!("lateral movement observed"));
}

#[test]
fn an_excluded_clause_is_not_evidence() {
    let temp = TempDir::new().unwrap();
    let store = store_with(
        &temp,
        &[("d1", json!({"title": "sample", "domains": domains(50, 40)}))],
    );
    for query in [
        r#"title:sample -domains:"bad.example.com""#,
        r#"title:sample AND NOT domains:"bad.example.com""#,
    ] {
        let opts = SearchOptions {
            array_limit: Some(3),
            match_query: Some(query),
            ..Default::default()
        };
        let outcome = store
            .search_documents_with(INDEX, query, 10, None, &opts)
            .expect("search");
        assert!(outcome.hits.is_empty(), "{query}: the document holds it");
    }
    let found = hit(&store, r#"title:sample -domains:"good.example.com""#, 3);
    assert!(found.get(MATCHED_ELEMENTS_FIELD).is_none(), "{found}");
    assert_eq!(found["domains"][0], json!("a00000.example.com"));
}

#[test]
fn a_ghost_clause_moves_its_sources_elements_where_the_ghost_is_fed() {
    let temp = TempDir::new().unwrap();
    let mut text: Vec<String> = (0..20).map(|i| format!("plain sentence {i}")).collect();
    text[14] = "the attackers were running scripts".to_string();
    let store = store_with(
        &temp,
        &[
            (
                "en",
                json!({"title": "english", "lang": "en", "text": text}),
            ),
            ("fr", json!({"title": "french", "lang": "fr", "text": text})),
        ],
    );
    // `run` reaches `running` only through the stemmed ghost, and only the English document
    // feeds it.
    let found = hit(&store, "text_en:run", 2);
    assert_eq!(found["id"], json!("en"));
    assert_eq!(found[MATCHED_ELEMENTS_FIELD], json!({"text": [14]}));
    assert_eq!(
        found["text"][0],
        json!("the attackers were running scripts")
    );
    assert!(
        found.get("text_en").is_none(),
        "a ghost is never stored: {found}"
    );

    // The French document has the same text, but the ghost is not fed from it: found through its
    // title, it keeps its head.
    let query = "text_en:run OR title:french";
    let opts = SearchOptions {
        array_limit: Some(2),
        match_query: Some(query),
        ..Default::default()
    };
    let outcome = store
        .search_documents_with(INDEX, query, 10, None, &opts)
        .expect("search");
    let french = outcome
        .hits
        .iter()
        .map(|(_, hit)| hit)
        .find(|hit| hit["id"] == json!("fr"))
        .expect("the French document");
    assert!(french.get(MATCHED_ELEMENTS_FIELD).is_none(), "{french}");
    assert_eq!(french["text"][0], json!("plain sentence 0"));
}

#[test]
fn a_projected_away_array_is_not_scanned() {
    let temp = TempDir::new().unwrap();
    let store = store_with(
        &temp,
        &[("d1", json!({"title": "sample", "domains": domains(50, 40)}))],
    );
    let keep = vec!["title".to_string()];
    let found = hit_with(&store, r#"domains:"bad.example.com""#, 3, Some(&keep), true);
    assert!(found.get("domains").is_none());
    assert!(found.get(MATCHED_ELEMENTS_FIELD).is_none(), "{found}");
}

#[test]
fn array_zero_reports_the_positions_without_the_elements() {
    let temp = TempDir::new().unwrap();
    let store = store_with(
        &temp,
        &[("d1", json!({"title": "sample", "domains": domains(50, 40)}))],
    );
    let found = hit(&store, r#"domains:"bad.example.com""#, 0);
    assert_eq!(found["domains"], json!([]));
    assert_eq!(found[MATCHED_ELEMENTS_FIELD], json!({"domains": [40]}));
    assert_eq!(found[TRUNCATED_ARRAYS_FIELD], json!({"domains": 50}));
}

#[test]
fn a_match_deep_in_a_large_array_leads() {
    let temp = TempDir::new().unwrap();
    let addrs: Vec<String> = (0..300_000u32)
        .map(|n| std::net::Ipv4Addr::from(0x0A00_0000 + n).to_string())
        .collect();
    let wanted = addrs[250_000].clone();
    let store = store_with(&temp, &[("d1", json!({"title": "sample", "addrs": addrs}))]);
    let found = hit(&store, &format!("addrs:{wanted}"), 5);
    assert_eq!(found["addrs"][0], json!(wanted));
    assert_eq!(found[MATCHED_ELEMENTS_FIELD], json!({"addrs": [250_000]}));
    assert_eq!(found[TRUNCATED_ARRAYS_FIELD], json!({"addrs": 300_000}));
}
