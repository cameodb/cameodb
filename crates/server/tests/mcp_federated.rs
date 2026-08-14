//! What the MCP tools actually answer, driven over the real endpoint against a live node.
//!
//! Everything here is a claim the tools make to an agent — the order of a federated merge, which
//! fields an index reports, what a shadow field is, what the syntax reference says about the
//! default operator — checked against the engine rather than against another part of the same
//! description. A tool that agrees with itself and disagrees with the engine is the failure these
//! exist to catch, and none of it is visible to a unit test: it is a property of the routing
//! path, the schema composition and the query parser together.
//!
//! Federated ordering carries the most setup, because interleaving is what has to be asserted. A
//! merge that ordered each index's block correctly and then concatenated the blocks passes a
//! naive monotonicity check, so `ALPHA` and `BETA` are laid out to make per-index blocking
//! visible: the correct answer alternates between the two indexes on every hit.

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

    /// An index identified by `sha1` rather than by `id`, the shape an import produces when the
    /// source data names its own identifier.
    ///
    /// `sha1` is a shadow of `id`: unindexed and unstored, kept in the schema so that a query
    /// naming it still resolves to the identifier it duplicates.
    async fn create_shadow_index(&self, index: &str) {
        let status = http()
            .put(format!("{}/api/{index}/_config", self.url))
            .json(&json!({
                "fields": {
                    "title": {"field_type": "text", "indexed": true},
                    "sha1": {
                        "field_type": "text",
                        "indexed": false,
                        "stored": false,
                        "is_shadow": true,
                    },
                }
            }))
            .send()
            .await
            .expect("create shadow config")
            .status();
        assert!(status.is_success(), "creating '{index}' failed: {status}");
    }

    /// Write documents whose `sha1` repeats their `id`, the way an import copies the source's
    /// identifier into the canonical field.
    async fn seed_shadow(&self, index: &str, ids: &[&str]) {
        for id in ids {
            let status = http()
                .put(format!("{}/api/{index}/document", self.url))
                .json(&json!({
                    "id": id,
                    "doc": {"id": id, "sha1": id, "title": "quarterly record"}
                }))
                .send()
                .await
                .expect("write")
                .status();
            assert!(status.is_success(), "seeding {id} failed: {status}");
        }
        self.await_searchable(index, ids.len()).await;
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
        self.await_searchable(index, documents.len()).await;
    }

    /// The write path schedules a commit rather than performing one, so poll for visibility
    /// instead of sleeping.
    async fn await_searchable(&self, index: &str, expected: usize) {
        let deadline = Instant::now() + Duration::from_secs(20);
        while Instant::now() < deadline {
            let resp = http()
                .post(format!("{}/api/{index}/search", self.url))
                .json(&json!({"query": "title:record", "limit": 10}))
                .send()
                .await
                .expect("search");
            let body: Value = resp.json().await.expect("search json");
            if body["total_hits"].as_u64() == Some(expected as u64) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("seeded documents never became searchable in '{index}'");
    }

    /// One `tools/call`, returning `(isError, parsed result text)`.
    async fn call_tool(&self, tool: &str, arguments: Value) -> (bool, Value) {
        self.call_tool_raw(json!({"name": tool, "arguments": arguments}))
            .await
    }

    /// One `tools/call` with the params written out, for the shapes `call_tool` cannot express —
    /// a call that omits `arguments` altogether rather than sending an empty object.
    async fn call_tool_raw(&self, params: Value) -> (bool, Value) {
        let value = self
            .rpc(json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": params,
            }))
            .await;
        let result = &value["result"];
        let is_error = result["isError"].as_bool().unwrap_or(false);
        let text = result["content"][0]["text"].as_str().unwrap_or("");
        let parsed = serde_json::from_str(text).unwrap_or(Value::String(text.to_string()));
        (is_error, parsed)
    }

    /// One JSON-RPC message, returning the whole response envelope.
    async fn rpc(&self, body: Value) -> Value {
        let resp = http()
            .post(format!("{}/mcp", self.url))
            .header("content-type", "application/json")
            .header("accept", "application/json, text/event-stream")
            .json(&body)
            .send()
            .await
            .expect("mcp post");
        resp.json().await.expect("mcp json")
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
/// Sixteen documents and a request for all sixteen, with no `limit` argument, so the inline
/// clause is the only limit in play. It has to reach both the value asked of each index and the
/// point the merge truncates at; a merge deriving that point separately would cut at ten.
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
/// Configured to 3 rather than left at the built-in 10, because a merge falling back to a
/// literal of its own would agree with the default and the test could not tell the two apart.
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
/// `total_size_bytes` is summed from a key the listing emits only when asked for data sizes, so
/// an aggregate built on a listing that does not ask reports 0 whatever the node holds. A zero
/// here is indistinguishable from an empty node, which is what an agent deciding whether an
/// index is worth searching would act on.
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
/// types, no `indexed` flags and no query hints — and every tool an agent is told to call before
/// writing a query reads from that entry.
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
/// It reads the same enriched entry the catalogue does, so an entry without field types reaches
/// it as a claim that every field in a working query is one the index does not have — the most
/// misleading answer in the tool set, since an agent comes here precisely when it doubts a
/// query.
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

/// A shadow field is the identifier under the name the source data gave it — `sha1`, `book_id` —
/// and querying it is querying `id`.
///
/// It is not in the search index, so `indexed: false` is all a discovery surface sees unless the
/// shadow flag travels with it — and an agent reading that alone is told to avoid the one
/// retrieval CameoDB answers from the key-value store without touching the search index at all.
/// Every claim below is checked against the engine first, so the description cannot merely agree
/// with itself.
#[tokio::test]
async fn a_shadow_field_is_described_as_the_queryable_alias_of_id() {
    let node = TestNode::start().await;
    node.create_shadow_index("files").await;
    node.seed_shadow("files", &["deadbeef01", "cafe02"]).await;

    // The claim the discovery tools are about to make, checked against the engine first: this
    // query works, so anything describing it as unqueryable is describing something else.
    let (is_error, found) = node
        .call_tool(
            "search_index",
            json!({"index": "files", "query": "sha1:deadbeef01"}),
        )
        .await;
    assert!(!is_error, "a shadow-field lookup failed: {found}");
    assert_eq!(
        found["total_hits"].as_u64(),
        Some(1),
        "the shadow lookup found nothing, so the rest of this test proves nothing: {found}"
    );

    // Which field that was is only discoverable if the flag is reported.
    let (_, described) = node.call_tool("get_index", json!({"index": "files"})).await;
    let fields = described["fields"].as_array().expect("a fields array");
    let field = |name: &str| {
        fields
            .iter()
            .find(|f| f["field"] == name)
            .unwrap_or_else(|| panic!("no entry for '{name}': {described}"))
            .clone()
    };
    assert_eq!(
        field("sha1")["shadow"].as_bool(),
        Some(true),
        "nothing in the description distinguishes the identifier's own name from a dead field: \
         {described}"
    );
    assert_eq!(
        field("title")["shadow"].as_bool(),
        Some(false),
        "the flag has to be present on every field to be readable as an answer: {described}"
    );

    // And `validate_query`, which is where an agent goes when it doubts a query, must not send
    // it away from one that works.
    let (_, validated) = node
        .call_tool(
            "validate_query",
            json!({"index": "files", "query": "sha1:deadbeef01"}),
        )
        .await;
    let analysis = &validated["query_analysis"];
    let names = |key: &str| -> Vec<String> {
        analysis[key]
            .as_array()
            .map(|f| {
                f.iter()
                    .filter_map(|v| v.as_str())
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default()
    };
    assert!(
        names("recognized_fields").contains(&"sha1".to_string()),
        "the field the working query names must be recognised: {validated}"
    );
    assert!(
        !names("not_indexed_fields").contains(&"sha1".to_string()),
        "unindexed is true of the search index and false of the query: {validated}"
    );
    let warnings = names("warnings").join(" ");
    assert!(
        !warnings.contains("will not match"),
        "the query does match, and this warning is why an agent would not send it: {validated}"
    );

    let listed = validated["available_fields"]
        .as_array()
        .expect("available_fields");
    let sha1 = listed
        .iter()
        .find(|f| f["field"] == "sha1")
        .unwrap_or_else(|| panic!("sha1 missing from available_fields: {validated}"));
    assert_eq!(
        sha1["queryable"].as_bool(),
        Some(true),
        "a field the engine answers on is queryable: {validated}"
    );
    assert_eq!(sha1["shadow"].as_bool(), Some(true), "{validated}");
    assert!(
        sha1["query_hint"]
            .as_str()
            .is_some_and(|hint| hint.contains("id")),
        "a shadow field's hint has to say what it is a shadow of: {validated}"
    );
}

/// The other direction, which is where the name does the surprising work.
///
/// A shadow field stores nothing: the value moved into `id` on write, and on read `id` comes back
/// under the descriptive name instead. So the identifier an agent must project and pivot on is the
/// shadow name, and asking for `id` — the obvious thing to ask for — returns a document with
/// nothing in it and no explanation. The hint has to say so, which means this has to stay true.
#[tokio::test]
async fn a_shadow_index_returns_its_identifier_under_the_shadow_name() {
    let node = TestNode::start().await;
    node.create_shadow_index("files").await;
    node.seed_shadow("files", &["deadbeef01"]).await;

    let (is_error, result) = node
        .call_tool(
            "search_index",
            json!({"index": "files", "query": "title:record"}),
        )
        .await;
    assert!(!is_error, "search failed: {result}");

    let hit = &result["hits"][0];
    assert_eq!(
        hit["sha1"].as_str(),
        Some("deadbeef01"),
        "the identifier must come back under the name the schema gives it: {result}"
    );
    assert!(
        hit.get("id").is_none(),
        "if `id` were also present the shadow name would be a convenience rather than the only \
         way to read the identifier, and the guidance would be overstating the case: {result}"
    );

    // What an agent that trusts `id` gets instead. Pinned because the hint promises this, not
    // because an empty projection is desirable.
    let (_, projected) = node
        .call_tool(
            "search_index",
            json!({"index": "files", "query": "title:record", "fields": ["id"]}),
        )
        .await;
    let hit = &projected["hits"][0];
    assert!(
        hit.get("id").is_none() && hit.get("sha1").is_none(),
        "projecting `id` on a shadow index returns nothing, which is what the hint tells an \
         agent to expect: {projected}"
    );
}

/// The restriction that comes with it, pinned so the guidance stays checkable.
///
/// The key-value bypass recognises `field:VALUE` and nothing else, so the same clause inside a
/// boolean query is dropped by the parser — which is what makes the whole-query form worth
/// naming rather than describing the field as generally searchable.
#[tokio::test]
async fn a_shadow_field_inside_a_larger_query_is_reported_rather_than_answered() {
    let node = TestNode::start().await;
    node.create_shadow_index("files").await;
    node.seed_shadow("files", &["deadbeef01"]).await;

    let (is_error, result) = node
        .call_tool(
            "search_index",
            json!({"index": "files", "query": "sha1:deadbeef01 AND title:record"}),
        )
        .await;
    assert!(
        is_error,
        "the sha1 clause was dropped and the hits came from title alone; answering as though \
         the query had run is the failure mode the dropped-clause report exists to prevent: \
         {result}"
    );
    assert!(
        result.to_string().contains("sha1"),
        "the refusal must name the clause that was dropped: {result}"
    );
}

/// What an index is for is the one thing no schema implies.
///
/// Field names and types describe the shape of the data; they do not say which dataset it is or
/// what a column records. An operator writes that down, and the discovery tools carry it — which
/// is what the first step of the orchestrator prompt, "read the descriptions to find the right
/// dataset", depends on being true.
#[tokio::test]
async fn descriptions_written_into_the_schema_reach_every_discovery_surface() {
    let node = TestNode::start().await;
    let status = http()
        .put(format!("{}/api/filings/_config", node.url))
        .json(&json!({
            "description": "Quarterly regulatory filings, one document per filing.",
            "fields": {
                "title": {
                    "field_type": "text",
                    "indexed": true,
                    "description": "Headline as filed, not normalised.",
                },
                "created": {"field_type": "date", "indexed": true, "fast": true},
            }
        }))
        .send()
        .await
        .expect("create config")
        .status();
    assert!(status.is_success(), "creating 'filings' failed: {status}");
    node.seed("filings", &[("f1", "2024-01-01T00:00:00Z")])
        .await;

    let (is_error, described) = node
        .call_tool("get_index", json!({"index": "filings"}))
        .await;
    assert!(!is_error, "get_index failed: {described}");
    assert_eq!(
        described["description"].as_str(),
        Some("Quarterly regulatory filings, one document per filing."),
        "the index's own description is missing from its description: {described}"
    );

    let fields = described["fields"].as_array().expect("a fields array");
    let field = |name: &str| {
        fields
            .iter()
            .find(|f| f["field"] == name)
            .unwrap_or_else(|| panic!("no entry for '{name}': {described}"))
            .clone()
    };
    assert_eq!(
        field("title")["description"].as_str(),
        Some("Headline as filed, not normalised.")
    );
    assert!(
        field("created").get("description").is_none(),
        "a field nobody described must not carry an empty key, or absent and blank become the \
         same answer: {described}"
    );

    // The catalogue is where the choice between datasets is actually made.
    let (_, listed) = node.call_tool("list_indexes", json!({})).await;
    let entry = listed["indexes"]
        .as_array()
        .and_then(|entries| entries.iter().find(|e| e["index"] == "filings"))
        .unwrap_or_else(|| panic!("filings missing from the catalogue: {listed}"));
    assert!(
        entry["description"]
            .as_str()
            .is_some_and(|text| text.contains("Quarterly")),
        "the catalogue lists the index without saying what it is: {listed}"
    );

    // And the tool an agent reaches for when a query looks wrong.
    let (_, validated) = node
        .call_tool(
            "validate_query",
            json!({"index": "filings", "query": "title:filing"}),
        )
        .await;
    let title = validated["available_fields"]
        .as_array()
        .and_then(|fields| fields.iter().find(|f| f["field"] == "title"))
        .unwrap_or_else(|| panic!("title missing from available_fields: {validated}"));
    assert_eq!(
        title["description"].as_str(),
        Some("Headline as filed, not normalised.")
    );
}

/// The limits are a promise about response size, so they are enforced where the schema is
/// written rather than trimmed on the way out — a description cut off mid-sentence still reads
/// as the whole statement.
#[tokio::test]
async fn an_over_long_description_is_refused_at_the_config_endpoint() {
    let node = TestNode::start().await;

    let response = http()
        .put(format!("{}/api/filings/_config", node.url))
        .json(&json!({
            "description": "x".repeat(600),
            "fields": {"title": {"field_type": "text", "indexed": true}}
        }))
        .send()
        .await
        .expect("create config");
    assert_eq!(
        response.status(),
        400,
        "an over-long description is the caller's mistake, not the server's"
    );
    let body: Value = response.json().await.expect("error json");
    assert!(
        body.to_string().contains("512"),
        "the refusal must say what the limit is: {body}"
    );

    // And the index was not created despite the description being the only thing wrong with it.
    let listed = http()
        .get(format!("{}/_indexes", node.url))
        .send()
        .await
        .expect("list")
        .json::<Value>()
        .await
        .expect("list json");
    assert!(
        !listed.to_string().contains("filings"),
        "a refused config must not leave the index half-created: {listed}"
    );
}

/// The default operator, checked against the engine rather than assumed.
///
/// Every syntax surface is rendered from one table, so a wrong entry there is wrong in the tool
/// descriptions, the reference and the prompt at once — and this entry is the one whose error
/// inverts the advice it leads to. An agent that narrows a result by adding a term widens it
/// instead, and the extra documents arrive looking like data rather than like a mistake.
#[tokio::test]
async fn bare_terms_are_ored_the_way_the_syntax_reference_says() {
    let node = TestNode::start().await;
    node.create_index("docs").await;
    for (id, title) in [("d1", "alpha record"), ("d2", "beta record")] {
        let status = http()
            .put(format!("{}/api/docs/document", node.url))
            .json(&json!({
                "id": id,
                "doc": {"id": id, "title": title, "created": "2024-01-01T00:00:00Z"}
            }))
            .send()
            .await
            .expect("write")
            .status();
        assert!(status.is_success(), "seeding {id} failed: {status}");
    }
    node.await_searchable("docs", 2).await;

    let hits = |result: &Value| -> u64 { result["total_hits"].as_u64().unwrap_or_default() };

    // One term per document, so ANDing them would match nothing and ORing them matches both.
    let (is_error, ored) = node
        .call_tool(
            "search_index",
            json!({"index": "docs", "query": "alpha beta"}),
        )
        .await;
    assert!(!is_error, "search failed: {ored}");
    assert_eq!(
        hits(&ored),
        2,
        "two terms returned fewer documents than either alone, so they are not ORed: {ored}"
    );

    // And the operator that does require both still does.
    let (_, anded) = node
        .call_tool(
            "search_index",
            json!({"index": "docs", "query": "title:alpha AND title:beta"}),
        )
        .await;
    assert_eq!(
        hits(&anded),
        0,
        "no document has both terms, so `AND` must return nothing: {anded}"
    );

    // The zero-results advice follows from that: `AND` narrows and is worth explaining, while
    // terms that matched nothing were never narrowed and get no warning to undo.
    assert!(
        anded["_warning"]
            .as_str()
            .is_some_and(|warning| warning.contains("`AND`")),
        "an AND query that found nothing should say what narrowed it: {anded}"
    );
    let (_, nothing) = node
        .call_tool(
            "search_index",
            json!({"index": "docs", "query": "nonexistent absent missing"}),
        )
        .await;
    assert_eq!(hits(&nothing), 0);
    assert!(
        nothing.get("_warning").is_none(),
        "these terms are ORed, so there is no narrowing to undo and nothing to warn about: \
         {nothing}"
    );
}

/// An argument no tool takes is an error rather than a silence.
///
/// This is what the strictness is for: `limt` is dropped by a lenient decoder, the search then
/// runs under the node's default limit, and the agent reads a truncated answer as a complete
/// one. Nothing in the response would say otherwise, which is why the misspelling has to be
/// refused rather than reported alongside results.
#[tokio::test]
async fn an_argument_no_tool_takes_is_refused_rather_than_ignored() {
    let node = two_seeded_indexes().await;

    let (is_error, refusal) = node
        .call_tool(
            "search_index",
            json!({"index": "alpha", "query": "title:record", "limt": 1}),
        )
        .await;
    assert!(is_error, "a misspelled limit was accepted: {refusal}");
    let text = refusal.as_str().unwrap_or_default();
    assert!(
        text.contains("limt"),
        "the refusal does not name it: {text}"
    );
    assert!(
        refusal.get("hits").is_none(),
        "the search ran anyway, so the limit was silently dropped: {refusal}"
    );

    // Nested arguments are read the same way: a per-index projection is exactly the kind of
    // argument whose absence looks like a document that has no such fields.
    let (is_error, refusal) = node
        .call_tool(
            "search_indexes",
            json!({
                "indexes": [{"index": "alpha", "feilds": ["title"]}],
                "query": "title:record",
            }),
        )
        .await;
    assert!(is_error, "a misspelled projection was accepted: {refusal}");
    assert!(
        refusal.as_str().is_some_and(|text| text.contains("feilds")),
        "the refusal does not name it: {refusal}"
    );
}

/// A tool whose arguments are all optional is callable with none of them, however "none" is spelled.
///
/// `validate_query`'s description tells an agent to call it with no arguments for the syntax
/// reference, and a client that has none to send omits the key rather than sending an empty
/// object. Both have to arrive as a call.
#[tokio::test]
async fn a_tool_call_that_carries_no_arguments_is_still_a_call() {
    let node = TestNode::start().await;

    for params in [
        json!({"name": "validate_query"}),
        json!({"name": "validate_query", "arguments": {}}),
        json!({"name": "validate_query", "arguments": null}),
    ] {
        let (is_error, result) = node.call_tool_raw(params.clone()).await;
        assert!(!is_error, "{params} was refused: {result}");
        assert!(
            result["syntax_reference"].is_object() || result["syntax_reference"].is_array(),
            "{params} did not return the syntax reference: {result}"
        );
    }
}

/// Every advertised schema tells a client that it is closed.
///
/// The dispatcher refuses an unknown argument either way; what this checks is that the client
/// was told in advance, so a schema-driven caller never constructs the call at all.
#[tokio::test]
async fn every_advertised_tool_schema_says_it_is_closed() {
    let node = TestNode::start().await;

    let listing = node
        .rpc(json!({"jsonrpc": "2.0", "id": 1, "method": "tools/list"}))
        .await;
    let tools = listing["result"]["tools"]
        .as_array()
        .expect("tools array")
        .clone();
    assert!(!tools.is_empty(), "no tools advertised: {listing}");

    for tool in tools {
        let name = tool["name"].as_str().unwrap_or("?");
        assert_eq!(
            tool["inputSchema"]["additionalProperties"],
            json!(false),
            "{name} does not advertise itself as closed: {}",
            tool["inputSchema"]
        );
    }
}

/// A limit above the advertised maximum is refused, however it reaches the search.
///
/// The argument is refused by the dispatcher; an inline `limit` modifier in the query string
/// arrives after that check, so the node checks the value the search will actually run with.
/// Both matter: unbounded means the caller decides how many hits the node builds, merges and
/// serializes for one request.
#[tokio::test]
async fn a_limit_above_the_maximum_is_refused_by_either_door() {
    let node = two_seeded_indexes().await;
    let over = cameodb_mcp::DEFAULT_MAX_SEARCH_LIMIT + 1;

    for (door, arguments) in [
        (
            "argument",
            json!({"index": "alpha", "query": "title:record", "limit": over}),
        ),
        (
            "inline modifier",
            json!({"index": "alpha", "query": format!("title:record limit {over}")}),
        ),
    ] {
        let (is_error, refusal) = node.call_tool("search_index", arguments).await;
        assert!(is_error, "the {door} was accepted: {refusal}");
        assert!(
            refusal.as_str().is_some_and(
                |text| text.contains(&cameodb_mcp::DEFAULT_MAX_SEARCH_LIMIT.to_string())
            ),
            "the {door} refusal does not say what the maximum is: {refusal}"
        );
    }

    // The maximum itself is allowed, so the bound is not off by one.
    let (is_error, result) = node
        .call_tool(
            "search_index",
            json!({
                "index": "alpha",
                "query": "title:record",
                "limit": cameodb_mcp::DEFAULT_MAX_SEARCH_LIMIT,
            }),
        )
        .await;
    assert!(!is_error, "the maximum itself was refused: {result}");
}

/// An index list that cannot be answered coherently is refused rather than answered.
///
/// Both cases previously returned something an agent would read as a fact about the data: no
/// hits and no errors for an empty list, and — for an index named twice — a `total_hits`
/// larger than the index holds, because each mention is searched and counted separately.
#[tokio::test]
async fn an_index_list_that_cannot_be_answered_is_refused() {
    let node = two_seeded_indexes().await;

    let (is_error, refusal) = node
        .call_tool(
            "search_indexes",
            json!({"indexes": [], "query": "title:record"}),
        )
        .await;
    assert!(is_error, "an empty index list was answered: {refusal}");

    let (is_error, refusal) = node
        .call_tool(
            "search_indexes",
            json!({"indexes": [{"index": "alpha"}, {"index": "alpha"}], "query": "title:record"}),
        )
        .await;
    assert!(is_error, "a repeated index was searched twice: {refusal}");
    assert!(
        refusal.as_str().is_some_and(|text| text.contains("alpha")),
        "the refusal does not name the repeated index: {refusal}"
    );

    // What the repeated name would have reported, for contrast: naming it once is the truth.
    let (is_error, result) = node
        .call_tool(
            "search_indexes",
            json!({"indexes": [{"index": "alpha"}], "query": "title:record"}),
        )
        .await;
    assert!(!is_error, "{result}");
    assert_eq!(
        result["total_hits"], 3,
        "the index holds three documents: {result}"
    );
}

/// The widest permitted fan-out is answered in full, though it exceeds what runs at once.
///
/// Indexes are searched a bounded number at a time — one name is a scatter-gather across that
/// index's shards, so an uncapped fan-out lets a single request occupy every shard worker and
/// starve the searches already running. What the bound must not do is lose an index: the ones
/// that wait for a slot have to arrive like the ones that did not, which is the property a
/// queue can quietly break.
///
/// The two seeded indexes are named last, so they are the ones that wait, and every other name
/// is one no index answers — which accounts for all twenty either as hits or as errors.
#[tokio::test]
async fn the_widest_permitted_fan_out_answers_from_every_index() {
    let node = two_seeded_indexes().await;

    let absent = cameodb_mcp::MAX_FEDERATED_INDEXES - 2;
    let mut named: Vec<Value> = (0..absent)
        .map(|n| json!({"index": format!("gone{n:02}")}))
        .collect();
    named.push(json!({"index": "alpha"}));
    named.push(json!({"index": "beta"}));

    let (is_error, result) = node
        .call_tool(
            "search_indexes",
            json!({
                "indexes": named,
                "query": "title:record",
                "limit": cameodb_mcp::MAX_FEDERATED_INDEXES,
            }),
        )
        .await;
    assert!(!is_error, "the widest permitted search failed: {result}");

    // Every name is accounted for, so nothing was dropped while waiting for a slot.
    assert_eq!(
        result["errors"].as_array().map(Vec::len),
        Some(absent),
        "an absent index went unreported: {result}"
    );
    assert_eq!(
        result["total_hits"].as_u64(),
        Some(6),
        "the two real indexes hold three documents each: {result}"
    );
    let sources: std::collections::BTreeSet<&str> = result["hits"]
        .as_array()
        .expect("hits")
        .iter()
        .filter_map(|hit| hit["_index_source"].as_str())
        .collect();
    assert_eq!(
        sources,
        ["alpha", "beta"].into_iter().collect(),
        "an index that waited for a slot did not reach the merge: {sources:?}"
    );
}

/// A configured ceiling is the number the tools advertise and the number they enforce.
///
/// The bound is a deployment question — how many hits this node can afford to build, merge and
/// serialize for one request — so an operator sets it, and both halves have to follow. A client
/// reading a `maximum` of one number and being refused at another has been misled by the
/// catalogue it was given.
#[tokio::test]
async fn a_configured_search_ceiling_is_advertised_and_enforced() {
    let node = TestNode::start_with(
        r#"
[search]
default_search_limit = 5

[security.limits]
max_search_limit = 25
"#,
    )
    .await;
    node.create_index("docs").await;
    node.seed("docs", &[("d1", "2024-01-01T00:00:00Z")]).await;

    let listing = node
        .rpc(json!({"jsonrpc": "2.0", "id": 1, "method": "tools/list"}))
        .await;
    for tool in listing["result"]["tools"].as_array().expect("tools") {
        let name = tool["name"].as_str().unwrap_or("?");
        if !name.starts_with("search") {
            continue;
        }
        assert_eq!(
            tool["inputSchema"]["properties"]["limit"]["maximum"],
            json!(25),
            "{name} advertises a ceiling the operator did not configure"
        );
    }

    // Enforced at the configured number, not at the compiled-in default.
    let (is_error, refusal) = node
        .call_tool(
            "search_index",
            json!({"index": "docs", "query": "*", "limit": 26}),
        )
        .await;
    assert!(
        is_error,
        "the configured ceiling was not enforced: {refusal}"
    );

    let (is_error, result) = node
        .call_tool(
            "search_index",
            json!({"index": "docs", "query": "*", "limit": 25}),
        )
        .await;
    assert!(
        !is_error,
        "the configured ceiling itself was refused: {result}"
    );

    // The inline door too, since it bypasses the argument check.
    let (is_error, refusal) = node
        .call_tool(
            "search_index",
            json!({"index": "docs", "query": "* limit 26"}),
        )
        .await;
    assert!(
        is_error,
        "an inline limit above the configured ceiling was accepted: {refusal}"
    );
}

/// The single-index tool takes the same structured sort the federated one does.
///
/// Sorting one index is the common case, so this is where a sort is most often wanted. The
/// federated tool advertising a per-index `sort` while this one silently discarded it read as
/// "sorting a single index is done some other way" — and an argument accepted and ignored is
/// worse than one refused, because the results look sorted enough to believe.
#[tokio::test]
async fn the_single_index_tool_sorts_by_a_structured_argument() {
    let node = two_seeded_indexes().await;

    let order_of = |result: &Value| -> Vec<String> {
        result["hits"]
            .as_array()
            .expect("hits")
            .iter()
            .filter_map(|hit| hit["id"].as_str().map(str::to_string))
            .collect()
    };

    let (is_error, ascending) = node
        .call_tool(
            "search_index",
            json!({
                "index": "alpha",
                "query": "title:record",
                "limit": 10,
                "sort": {"field": "created", "order": "asc"},
            }),
        )
        .await;
    assert!(!is_error, "a sorted search failed: {ascending}");
    assert_eq!(
        order_of(&ascending),
        ["a1", "a2", "a3"],
        "ascending order was not applied: {ascending}"
    );

    let (_, descending) = node
        .call_tool(
            "search_index",
            json!({
                "index": "alpha",
                "query": "title:record",
                "limit": 10,
                "sort": {"field": "created", "order": "desc"},
            }),
        )
        .await;
    assert_eq!(
        order_of(&descending),
        ["a3", "a2", "a1"],
        "descending order was not applied: {descending}"
    );

    // `order` is optional, and its default is ascending.
    let (_, defaulted) = node
        .call_tool(
            "search_index",
            json!({
                "index": "alpha",
                "query": "title:record",
                "limit": 10,
                "sort": {"field": "created"},
            }),
        )
        .await;
    assert_eq!(order_of(&defaulted), order_of(&ascending));

    // The argument wins over an inline clause, as `limit` and `fields` do.
    let (_, argument_wins) = node
        .call_tool(
            "search_index",
            json!({
                "index": "alpha",
                "query": "title:record sort created:asc",
                "limit": 10,
                "sort": {"field": "created", "order": "desc"},
            }),
        )
        .await;
    assert_eq!(
        order_of(&argument_wins),
        ["a3", "a2", "a1"],
        "the inline clause overrode the argument: {argument_wins}"
    );

    // The internal sort key stays internal, as it does on the inline path.
    for hit in argument_wins["hits"].as_array().expect("hits") {
        assert!(
            hit.get("_sort_key").is_none(),
            "a sorted hit carried the internal sort key: {hit}"
        );
    }
}

/// One query, asked many times, gives one answer.
///
/// Neither key a search can order on is a total order: every document matching a single term
/// scores identically, and a sort field repeats as readily as any other value. Indexes and
/// shards are searched concurrently and answer in whatever order they finish, so a merge that
/// let arrival order settle a tie returned different documents on different runs — measured at
/// two distinct answers over twenty-five identical calls, sharing one hit out of four.
///
/// Ties now fall back to the order the caller named its indexes in, then to each index's own
/// ordering. Both are fixed before any result arrives.
#[tokio::test]
async fn one_query_asked_repeatedly_gives_one_answer() {
    let node = TestNode::start().await;
    for index in ["alpha", "beta"] {
        node.create_index(index).await;
    }
    // Every document shares one `created` value, so every sort key ties, and one shared term
    // means every score ties too.
    let tied = "2024-01-01T00:00:00Z";
    node.seed("alpha", &[("a1", tied), ("a2", tied), ("a3", tied)])
        .await;
    node.seed("beta", &[("b1", tied), ("b2", tied), ("b3", tied)])
        .await;

    let ids = |result: &Value| -> Vec<String> {
        result["hits"]
            .as_array()
            .expect("hits")
            .iter()
            .filter_map(|hit| hit["id"].as_str().map(str::to_string))
            .collect()
    };

    // Both merges: the sorted one keys on `_sort_key`, the unsorted one on `_score`.
    for query in ["title:record sort created:asc", "title:record"] {
        let mut answers: std::collections::BTreeSet<Vec<String>> = Default::default();
        for _ in 0..12 {
            let (is_error, result) = node
                .call_tool(
                    "search_indexes",
                    json!({
                        "indexes": [{"index": "alpha"}, {"index": "beta"}],
                        "query": query,
                        "limit": 4,
                    }),
                )
                .await;
            assert!(!is_error, "{result}");
            answers.insert(ids(&result));
        }
        assert_eq!(
            answers.len(),
            1,
            "'{query}' returned {} different answers: {answers:?}",
            answers.len()
        );
    }

    // The tie-break is the caller's own order, so naming the indexes the other way round is a
    // different question with a different — and equally stable — answer.
    for (first, second) in [("alpha", "beta"), ("beta", "alpha")] {
        let (_, result) = node
            .call_tool(
                "search_indexes",
                json!({
                    "indexes": [{"index": first}, {"index": second}],
                    "query": "title:record",
                    "limit": 2,
                }),
            )
            .await;
        let sources: Vec<&str> = result["hits"]
            .as_array()
            .expect("hits")
            .iter()
            .filter_map(|hit| hit["_index_source"].as_str())
            .collect();
        assert_eq!(
            sources,
            [first, first],
            "the index named first should lead a tied merge: {result}"
        );
    }
}
