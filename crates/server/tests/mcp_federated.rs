//! What a federated MCP search actually returns, over the real endpoint.
//!
//! The merge in `search_indexes` orders hits by the internal `_sort_key` the engine stamps on
//! a field-sorted search. Whether that key is still present when the merge runs is a property
//! of the routing path, which no unit test on the comparator can see — so this drives
//! `search_across_indexes` against a live node holding two indexes that share a sortable date
//! field, and asserts on the order of what comes back.
//!
//! Interleaving is the assertion that matters. A merge that ordered each index's block
//! correctly and then concatenated the blocks passes a naive monotonicity check on a single
//! index, so the documents below are laid out to make per-index blocking visible: the correct
//! answer alternates between the two indexes on every hit.

use std::io::Write as _;
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use serde_json::{Value, json};

struct TestNode {
    child: Child,
    url: String,
    _dir: tempfile::TempDir,
}

impl TestNode {
    async fn start() -> TestNode {
        TestNode::start_with("").await
    }

    async fn start_with(extra: &str) -> TestNode {
        let dir = tempfile::tempdir().expect("temp dir");
        let port = free_port();
        let data = dir.path().join("data");
        std::fs::create_dir_all(&data).expect("data dir");

        let config = format!(
            r#"
[node]
label = "test-node"
profile = "local"

[network.http]
bind_address = "127.0.0.1"
port = {port}

[network.cluster]
enabled = false

[storage]
data_paths = ["{data}"]
num_shards_init = 1
max_shards_per_node = 1
{extra}
"#,
            data = data.display().to_string().replace('\\', "/"),
        );
        let config_path = dir.path().join("cameodb.toml");
        std::fs::File::create(&config_path)
            .expect("config file")
            .write_all(config.as_bytes())
            .expect("write config");

        let child = Command::new(env!("CARGO_BIN_EXE_cameodb"))
            .arg("-c")
            .arg(&config_path)
            .env("RUST_LOG", "warn")
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn cameodb");

        let node = TestNode {
            child,
            url: format!("http://127.0.0.1:{port}"),
            _dir: dir,
        };
        node.await_ready().await;
        node
    }

    async fn await_ready(&self) {
        let client = http();
        let deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < deadline {
            if let Ok(resp) = client
                .get(format!("{}/_cluster/health", self.url))
                .send()
                .await
                && resp.status().is_success()
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("node at {} never became healthy", self.url);
    }

    /// An index whose `created` field is a date the engine can sort on.
    ///
    /// `fast` is set explicitly: it defaults to false over the wire, and a sort on a date field
    /// that is not FAST does not order the results — which would make the sort tests pass for
    /// the wrong reason.
    async fn create_index(&self, index: &str) {
        let status = http()
            .put(format!("{}/api/{index}/_config", self.url))
            .json(&json!({
                "fields": {
                    "title": {"field_type": "text", "indexed": true},
                    "created": {"field_type": "date", "indexed": true, "fast": true},
                }
            }))
            .send()
            .await
            .expect("create config")
            .status();
        assert!(status.is_success(), "creating '{index}' failed: {status}");
    }

    /// Write `documents` as `(id, created)` pairs, then wait until they are searchable.
    async fn seed(&self, index: &str, documents: &[(&str, &str)]) {
        for (id, created) in documents {
            let status = http()
                .put(format!("{}/api/{index}/document", self.url))
                .json(&json!({
                    "id": id,
                    "doc": {"id": id, "title": "quarterly record", "created": created}
                }))
                .send()
                .await
                .expect("write")
                .status();
            assert!(status.is_success(), "seeding {id} failed: {status}");
        }

        // The write path schedules a commit rather than performing one, so poll for visibility
        // instead of sleeping.
        let deadline = Instant::now() + Duration::from_secs(20);
        while Instant::now() < deadline {
            let resp = http()
                .post(format!("{}/api/{index}/search", self.url))
                .json(&json!({"query": "title:record", "limit": 10}))
                .send()
                .await
                .expect("search");
            let body: Value = resp.json().await.expect("search json");
            if body["total_hits"].as_u64() == Some(documents.len() as u64) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("seeded documents never became searchable in '{index}'");
    }

    /// One `tools/call`, returning `(isError, parsed result text)`.
    async fn call_tool(&self, tool: &str, arguments: Value) -> (bool, Value) {
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": tool, "arguments": arguments},
        });
        let resp = http()
            .post(format!("{}/mcp", self.url))
            .header("content-type", "application/json")
            .header("accept", "application/json, text/event-stream")
            .json(&body)
            .send()
            .await
            .expect("mcp post");
        let value: Value = resp.json().await.expect("mcp json");
        let result = &value["result"];
        let is_error = result["isError"].as_bool().unwrap_or(false);
        let text = result["content"][0]["text"].as_str().unwrap_or("");
        let parsed = serde_json::from_str(text).unwrap_or(Value::String(text.to_string()));
        (is_error, parsed)
    }
}

impl Drop for TestNode {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn http() -> reqwest::Client {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
    reqwest::Client::new()
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral")
        .local_addr()
        .expect("local addr")
        .port()
}

/// Two indexes whose documents alternate in the sort field, so any per-index blocking shows up.
const ALPHA: &[(&str, &str)] = &[
    ("a1", "2024-01-01T00:00:00Z"),
    ("a2", "2024-03-01T00:00:00Z"),
    ("a3", "2024-05-01T00:00:00Z"),
];
const BETA: &[(&str, &str)] = &[
    ("b1", "2024-02-01T00:00:00Z"),
    ("b2", "2024-04-01T00:00:00Z"),
    ("b3", "2024-06-01T00:00:00Z"),
];

async fn two_seeded_indexes() -> TestNode {
    let node = TestNode::start().await;
    node.create_index("alpha").await;
    node.create_index("beta").await;
    node.seed("alpha", ALPHA).await;
    node.seed("beta", BETA).await;
    node
}

/// The order the merge promises. Descending by `created` across both indexes means the answer
/// alternates beta, alpha, beta, alpha… — and every hit still names the index it came from.
#[tokio::test]
async fn a_federated_sort_orders_across_indexes_rather_than_within_them() {
    let node = two_seeded_indexes().await;

    let (is_error, result) = node
        .call_tool(
            "search_indexes",
            json!({
                "indexes": [
                    {"index": "alpha", "sort": {"field": "created", "order": "desc"}},
                    {"index": "beta", "sort": {"field": "created", "order": "desc"}},
                ],
                "query": "title:record",
                "limit": 10,
            }),
        )
        .await;
    assert!(!is_error, "federated search failed: {result}");

    let hits = result["hits"].as_array().expect("hits array");
    assert_eq!(hits.len(), 6, "expected every seeded document: {result}");

    let observed: Vec<(String, String)> = hits
        .iter()
        .map(|hit| {
            (
                hit["_index_source"]
                    .as_str()
                    .unwrap_or("<missing>")
                    .to_string(),
                hit["created"].as_str().unwrap_or("<missing>").to_string(),
            )
        })
        .collect();

    let expected = [
        ("beta", "2024-06-01T00:00:00Z"),
        ("alpha", "2024-05-01T00:00:00Z"),
        ("beta", "2024-04-01T00:00:00Z"),
        ("alpha", "2024-03-01T00:00:00Z"),
        ("beta", "2024-02-01T00:00:00Z"),
        ("alpha", "2024-01-01T00:00:00Z"),
    ];
    for (position, (want_index, want_created)) in expected.iter().enumerate() {
        let (got_index, got_created) = &observed[position];
        assert_eq!(
            (got_index.as_str(), got_created.as_str()),
            (*want_index, *want_created),
            "hit {position} is out of order; whole sequence: {observed:?}"
        );
    }
}

/// Ascending is the other direction of the same wire, and the default when no order is given.
#[tokio::test]
async fn a_federated_sort_honours_ascending_order() {
    let node = two_seeded_indexes().await;

    let (is_error, result) = node
        .call_tool(
            "search_indexes",
            json!({
                "indexes": [
                    {"index": "alpha", "sort": {"field": "created"}},
                    {"index": "beta", "sort": {"field": "created"}},
                ],
                "query": "title:record",
                "limit": 10,
            }),
        )
        .await;
    assert!(!is_error, "federated search failed: {result}");

    let created: Vec<String> = result["hits"]
        .as_array()
        .expect("hits array")
        .iter()
        .map(|hit| hit["created"].as_str().unwrap_or("<missing>").to_string())
        .collect();

    let mut sorted = created.clone();
    sorted.sort();
    assert_eq!(
        created, sorted,
        "ascending federated sort returned an unordered sequence"
    );
}

/// `_sort_key` is internal. It has to survive the routing path far enough to drive the merge,
/// and it must not reach the caller.
#[tokio::test]
async fn the_internal_sort_key_does_not_reach_the_caller() {
    let node = two_seeded_indexes().await;

    let (_, result) = node
        .call_tool(
            "search_indexes",
            json!({
                "indexes": [
                    {"index": "alpha", "sort": {"field": "created", "order": "desc"}},
                    {"index": "beta", "sort": {"field": "created", "order": "desc"}},
                ],
                "query": "title:record",
                "limit": 10,
            }),
        )
        .await;

    for hit in result["hits"].as_array().expect("hits array") {
        assert!(
            hit.get("_sort_key").is_none(),
            "a hit carried the internal sort key to the caller: {hit}"
        );
    }
}

/// Enough documents on each side that the node's default limit of 10 is not the answer.
const WIDE: &[(&str, &str)] = &[
    ("w1", "2024-01-02T00:00:00Z"),
    ("w2", "2024-01-04T00:00:00Z"),
    ("w3", "2024-01-06T00:00:00Z"),
    ("w4", "2024-01-08T00:00:00Z"),
    ("w5", "2024-01-10T00:00:00Z"),
    ("w6", "2024-01-12T00:00:00Z"),
    ("w7", "2024-01-14T00:00:00Z"),
    ("w8", "2024-01-16T00:00:00Z"),
];

/// An inline `limit` has to bound the merge, not just the per-index searches feeding it.
///
/// Sixteen documents and a request for all sixteen, with no `limit` argument — so the only
/// limit in play is the inline one. The merge derived its truncation point separately from the
/// value it asked each index for, and only the latter saw the inline clause.
#[tokio::test]
async fn an_inline_limit_bounds_the_federated_merge() {
    let node = TestNode::start().await;
    node.create_index("wide-alpha").await;
    node.create_index("wide-beta").await;
    node.seed("wide-alpha", WIDE).await;
    node.seed("wide-beta", WIDE).await;

    let (is_error, result) = node
        .call_tool(
            "search_indexes",
            json!({
                "indexes": [{"index": "wide-alpha"}, {"index": "wide-beta"}],
                "query": "title:record limit 16",
            }),
        )
        .await;
    assert!(!is_error, "federated search failed: {result}");

    assert_eq!(
        result["hits"].as_array().map(Vec::len),
        Some(16),
        "an inline limit of 16 was not applied to the merge: {result}"
    );
    // The number reported back has to be the number that was honoured, or an agent paging
    // through results is reasoning from a limit the server did not use.
    assert_eq!(
        result["limit"].as_u64(),
        Some(16),
        "the response reported a limit it did not apply: {result}"
    );
}

/// With no limit anywhere, the merge falls back to the node's configured default rather than to
/// a number of its own.
///
/// Configured to 3 rather than left at 10, because a test that agrees with the hard-coded value
/// cannot tell the two apart — the single-index path passes `None` down and gets the configured
/// number, while the merge used a literal.
#[tokio::test]
async fn the_federated_merge_falls_back_to_the_configured_default() {
    let node = TestNode::start_with("\n[search]\ndefault_search_limit = 3\n").await;
    node.create_index("wide-alpha").await;
    node.seed("wide-alpha", WIDE).await;

    let (is_error, result) = node
        .call_tool(
            "search_indexes",
            json!({
                "indexes": [{"index": "wide-alpha"}],
                "query": "title:record",
            }),
        )
        .await;
    assert!(!is_error, "federated search failed: {result}");

    assert_eq!(
        result["hits"].as_array().map(Vec::len),
        Some(3),
        "the merge ignored the node's configured default: {result}"
    );
    assert_eq!(result["limit"].as_u64(), Some(3));

    // The single-index tool reads the same configured number, which is the agreement that was
    // missing: one tool honoured the operator's setting and the other did not.
    let (_, single) = node
        .call_tool(
            "search_index",
            json!({"index": "wide-alpha", "query": "title:record"}),
        )
        .await;
    assert_eq!(
        single["hits"].as_array().map(Vec::len),
        Some(3),
        "single-index search disagreed with the configured default: {single}"
    );
}

/// One unusable index must not sink the indexes that answered.
///
/// The result an agent can act on is "here is what I found, and here is what I could not
/// reach": failing the whole call throws away work that succeeded and says nothing about which
/// part of the request was the problem.
#[tokio::test]
async fn a_failing_index_does_not_sink_the_others() {
    let node = two_seeded_indexes().await;

    let (is_error, result) = node
        .call_tool(
            "search_indexes",
            json!({
                "indexes": [
                    {"index": "alpha"},
                    {"index": "no-such-index"},
                ],
                "query": "title:record",
                "limit": 10,
            }),
        )
        .await;

    assert!(
        !is_error,
        "one index answered, so the call succeeded partially: {result}"
    );
    assert_eq!(
        result["hits"].as_array().map(Vec::len),
        Some(3),
        "the index that answered should still have contributed its hits: {result}"
    );
    assert!(
        result["hits"]
            .as_array()
            .is_some_and(|hits| hits.iter().all(|hit| hit["_index_source"] == "alpha")),
        "only the index that answered should appear in the hits: {result}"
    );

    let errors = result["errors"]
        .as_array()
        .unwrap_or_else(|| panic!("a partial result must account for what is missing: {result}"));
    assert_eq!(errors.len(), 1, "expected exactly one failure: {result}");
    assert_eq!(
        errors[0]["index"].as_str(),
        Some("no-such-index"),
        "the failure must name the index it belongs to: {result}"
    );
    assert!(
        errors[0]["error"].as_str().is_some_and(|e| !e.is_empty()),
        "the failure must say what went wrong: {result}"
    );
}

/// A search where nothing answered is a failed call, not an empty one.
///
/// An empty `hits` beside a populated `errors` reads to an agent exactly like a query that
/// legitimately matched nothing, which is the one reading that must not be available.
#[tokio::test]
async fn a_search_where_every_index_fails_is_an_error() {
    let node = two_seeded_indexes().await;

    let (is_error, result) = node
        .call_tool(
            "search_indexes",
            json!({
                "indexes": [
                    {"index": "no-such-index"},
                    {"index": "also-missing"},
                ],
                "query": "title:record",
                "limit": 10,
            }),
        )
        .await;

    assert!(
        is_error,
        "no index answered, so this is a failure rather than an empty result: {result}"
    );
    let text = result.as_str().unwrap_or_default();
    assert!(
        text.contains("no-such-index") && text.contains("also-missing"),
        "the error must name every index that failed, got: {text}"
    );
}

/// Success with nothing missing carries no `errors` key, so its presence means something.
#[tokio::test]
async fn a_wholly_successful_search_reports_no_errors() {
    let node = two_seeded_indexes().await;

    let (is_error, result) = node
        .call_tool(
            "search_indexes",
            json!({
                "indexes": [{"index": "alpha"}, {"index": "beta"}],
                "query": "title:record",
                "limit": 10,
            }),
        )
        .await;

    assert!(!is_error, "search failed: {result}");
    assert!(
        result.get("errors").is_none(),
        "nothing failed, so nothing should be reported as missing: {result}"
    );
}

/// A single-index search must be unaffected by the federated path keeping the key.
#[tokio::test]
async fn a_single_index_search_still_strips_the_sort_key() {
    let node = two_seeded_indexes().await;

    let (is_error, result) = node
        .call_tool(
            "search_index",
            json!({"index": "alpha", "query": "title:record sort created:desc", "limit": 10}),
        )
        .await;
    assert!(!is_error, "single-index search failed: {result}");

    for hit in result["hits"].as_array().expect("hits array") {
        assert!(
            hit.get("_sort_key").is_none(),
            "a single-index hit carried the internal sort key: {hit}"
        );
    }
}

/// The single-index tool refuses an index that does not exist, rather than answering with an
/// empty result an agent reads as "the data is not there".
///
/// `get_index` already refuses the same name, so this is two MCP tools agreeing about whether
/// an index exists.
#[tokio::test]
async fn a_search_on_an_index_that_does_not_exist_is_refused() {
    let node = two_seeded_indexes().await;

    let (is_error, result) = node
        .call_tool(
            "search_index",
            json!({"index": "no-such-index", "query": "title:record"}),
        )
        .await;
    assert!(
        is_error,
        "a search on a missing index must not look like an empty result: {result}"
    );
    assert!(
        result
            .as_str()
            .unwrap_or_default()
            .contains("no-such-index"),
        "the refusal must name the index: {result}"
    );

    // The same name through the metadata tool, for the agreement this is about.
    let (is_error, _) = node
        .call_tool("get_index", json!({"index": "no-such-index"}))
        .await;
    assert!(is_error, "get_index has always refused a missing index");
}

/// A query that legitimately matches nothing in an index that does exist is still an empty
/// success — the refusal above must not have swallowed the ordinary no-results case.
#[tokio::test]
async fn a_query_matching_nothing_in_a_real_index_is_still_a_success() {
    let node = two_seeded_indexes().await;

    let (is_error, result) = node
        .call_tool(
            "search_index",
            json!({"index": "alpha", "query": "title:nothingmatchesthis"}),
        )
        .await;
    assert!(!is_error, "an honest empty result was refused: {result}");
    assert_eq!(result["total_hits"].as_u64(), Some(0));

    let (is_error, federated) = node
        .call_tool(
            "search_indexes",
            json!({
                "indexes": [{"index": "alpha"}, {"index": "beta"}],
                "query": "title:nothingmatchesthis",
            }),
        )
        .await;
    assert!(
        !is_error,
        "an honest empty federated result was refused: {federated}"
    );
    assert!(
        federated.get("errors").is_none(),
        "both indexes exist, so nothing is missing: {federated}"
    );
}

/// The HTTP search API keeps its contract: a missing index answers 200 with no hits.
///
/// The refusal above is scoped to the MCP tools, where the caller is an agent that cannot tell
/// an empty index from an absent one. An HTTP client that already handles 200-with-no-hits is
/// not broken to give the agent a better answer.
#[tokio::test]
async fn the_http_search_api_still_answers_empty_for_a_missing_index() {
    let node = two_seeded_indexes().await;

    let resp = http()
        .post(format!("{}/api/no-such-index/search", node.url))
        .json(&json!({"query": "title:record", "limit": 10}))
        .send()
        .await
        .expect("http search");
    assert_eq!(resp.status().as_u16(), 200);

    let body: Value = resp.json().await.expect("search json");
    assert_eq!(body["total_hits"].as_u64(), Some(0));
    assert_eq!(body["hits"].as_array().map(Vec::len), Some(0));
}

/// The catalogue aggregate has to be measured, not structurally zero.
///
/// `total_size_bytes` was summed from a key that only appears when the listing is asked for
/// data sizes, and the MCP listing never asked — so the number was always 0 whatever the node
/// held. A zero here is indistinguishable from an empty node, which is the reading an agent
/// deciding whether an index is worth searching would act on.
#[tokio::test]
async fn the_catalogue_aggregate_reports_a_size_it_measured() {
    let node = two_seeded_indexes().await;

    let (is_error, result) = node.call_tool("get_index_stats", json!({})).await;
    assert!(!is_error, "aggregate stats failed: {result}");

    assert_eq!(result["scope"].as_str(), Some("all_indexes"));
    assert_eq!(result["total_indexes"].as_u64(), Some(2));
    assert_eq!(
        result["total_documents"].as_u64(),
        Some(6),
        "six documents were ingested: {result}"
    );
    assert!(
        result["total_size_bytes"].as_u64().is_some_and(|b| b > 0),
        "a node holding six documents does not occupy zero bytes: {result}"
    );
    // Same class of structural zero, same five lines: summed from a key that was never there.
    assert!(
        result["total_fields"].as_u64().is_some_and(|f| f > 0),
        "two indexes with a title and a created field do not have zero fields: {result}"
    );
}

/// The single-index scope is unchanged by the aggregate paying for sizes.
#[tokio::test]
async fn single_index_stats_still_describe_one_index() {
    let node = two_seeded_indexes().await;

    let (is_error, result) = node
        .call_tool("get_index_stats", json!({"index": "alpha"}))
        .await;
    assert!(!is_error, "single-index stats failed: {result}");

    assert_eq!(result["scope"].as_str(), Some("single_index"));
    assert_eq!(result["index"].as_str(), Some("alpha"));
    assert_eq!(result["stats"]["document_count"].as_u64(), Some(3));
    assert!(
        result["field_count"].as_u64().is_some_and(|c| c > 0),
        "field_count was summed from the same absent key: {result}"
    );
    assert!(
        result["field_names"]
            .as_array()
            .is_some_and(|names| names.iter().any(|n| n == "title")),
        "the fields it counted should be the ones the index has: {result}"
    );
}

/// The schema-discovery surface has to describe the index, not an index with no fields.
///
/// The catalogue listing reports field names only, so an entry built from it alone has no
/// types, no `indexed` flags and no query hints. Every tool an agent is told to call before
/// writing a query read from exactly that entry.
#[tokio::test]
async fn the_discovery_tools_describe_the_fields_an_index_has() {
    let node = two_seeded_indexes().await;

    let (is_error, described) = node.call_tool("get_index", json!({"index": "alpha"})).await;
    assert!(!is_error, "get_index failed: {described}");

    let fields = described["fields"].as_array().expect("a fields array");
    let named: Vec<&str> = fields.iter().filter_map(|f| f["field"].as_str()).collect();
    assert!(
        named.contains(&"title") && named.contains(&"created"),
        "the index's own fields are missing: {described}"
    );

    let created = fields
        .iter()
        .find(|f| f["field"] == "created")
        .unwrap_or_else(|| panic!("no entry for 'created': {described}"));
    assert_eq!(
        created["type"].as_str(),
        Some("date"),
        "a field without its type cannot be queried correctly: {described}"
    );
    assert_eq!(created["fast"].as_bool(), Some(true), "{described}");

    // The per-type hints are the mechanism the whole guidance design rests on.
    let hints = described["query_hints"].as_array().expect("query_hints");
    assert!(
        hints.iter().any(|h| h["type"] == "date") && hints.iter().any(|h| h["type"] == "text"),
        "no hint for the types this index actually has: {described}"
    );
    assert!(
        hints.iter().all(|h| !h["query_hint"]
            .as_str()
            .unwrap_or_default()
            .contains("Unrecognised")),
        "a hint that does not recognise its own field type is worse than none: {described}"
    );

    // The catalogue tool the prompt names as the first discovery step says the same.
    let (_, listed) = node.call_tool("list_indexes", json!({})).await;
    let entry = listed["indexes"]
        .as_array()
        .and_then(|entries| entries.iter().find(|e| e["index"] == "alpha"))
        .unwrap_or_else(|| panic!("alpha missing from the catalogue: {listed}"));
    assert!(
        entry["fields"].as_array().is_some_and(|f| !f.is_empty()),
        "the catalogue describes alpha as having no fields: {listed}"
    );
}

/// `validate_query` must not report a real field as unknown.
///
/// It reads the same entry, so it inherited the empty field list and answered that every field
/// named in a working query was one the index does not have — the most misleading answer in the
/// tool set, since an agent is sent here precisely when it doubts a query.
#[tokio::test]
async fn validate_query_recognises_a_field_the_index_has() {
    let node = two_seeded_indexes().await;

    let (is_error, result) = node
        .call_tool(
            "validate_query",
            json!({"index": "alpha", "query": "title:record"}),
        )
        .await;
    assert!(!is_error, "validate_query failed: {result}");

    let analysis = &result["query_analysis"];
    let unknown: Vec<&str> = analysis["unknown_fields"]
        .as_array()
        .map(|f| f.iter().filter_map(|v| v.as_str()).collect())
        .unwrap_or_default();
    assert!(
        !unknown.contains(&"title"),
        "'title' is a real indexed field and the search on it works: {result}"
    );

    let recognized: Vec<&str> = analysis["recognized_fields"]
        .as_array()
        .map(|f| f.iter().filter_map(|v| v.as_str()).collect())
        .unwrap_or_default();
    assert!(
        recognized.contains(&"title"),
        "a field the index has must be recognised: {result}"
    );

    // And a field it really does not have is still reported.
    let (_, missing) = node
        .call_tool(
            "validate_query",
            json!({"index": "alpha", "query": "nosuchfield:x"}),
        )
        .await;
    let unknown: Vec<&str> = missing["query_analysis"]["unknown_fields"]
        .as_array()
        .map(|f| f.iter().filter_map(|v| v.as_str()).collect())
        .unwrap_or_default();
    assert!(
        unknown.contains(&"nosuchfield"),
        "an absent field must still be called out: {missing}"
    );
}
