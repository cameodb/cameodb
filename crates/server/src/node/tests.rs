use crate::remote_peer_pool::RemotePeerPool;
use anyhow::Result;
use arc_swap::ArcSwap;
use cluster::{ConsistentRing, NodeIdentity};
use serde_json::Value as JsonValue;
use std::collections::{HashMap, HashSet};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering as AtomicOrdering},
};
use std::time::{Duration, Instant};
use storage::{FieldDef, HybridStore, IndexSchema, StorageConfig, StoreError, TantivyFieldType};
use tokio::sync::mpsc;
use uuid::Uuid;

use super::routing::routing_key_without_schema;
use super::*;
use serde_json::json;

/// A panic inside a read must cost one request, not the node.
///
/// The whole point of `panic = "unwind"` in the release profile is that a panic raised deep
/// in tantivy or redb — on a query or document shape a validator missed — unwinds into the
/// read task's `JoinHandle` instead of aborting the process. `dispatch_read_pool` is the
/// boundary that catches it, and this pins both halves of what that buys: the panicking read
/// comes back as an error, and the same pool that ran it serves the next read.
///
/// `cargo test` always unwinds, so this exercises the code-level isolation rather than the
/// release profile flag; a release-profile smoke test against the built binary is the
/// separate half of proving finding 01.
#[test]
fn a_panicking_read_is_an_error_and_the_read_pool_keeps_serving() {
    // The pool a shard builds: one worker thread, a bounded blocking pool.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .max_blocking_threads(2)
        .enable_all()
        .build()
        .expect("read runtime");
    let handle = runtime.handle().clone();

    let health = Arc::new(ReadPoolHealth::new(2));

    runtime.block_on(async {
        // The dedicated-pool path a real search takes, tracked so the in-flight bracket is
        // exercised across the panic too.
        let panicked: Result<(), OrchestratorError> =
            dispatch_read_pool(Some(&handle), Some(Arc::clone(&health)), None, || {
                panic!("tantivy panicked on a document")
            })
            .await;
        assert!(
            panicked.is_err(),
            "a panic in a read must surface as an error, not take the process down"
        );
        let after: u32 = dispatch_read_pool(Some(&handle), Some(Arc::clone(&health)), None, || 7)
            .await
            .expect("the dedicated read pool serves the next read after a panic");
        assert_eq!(after, 7);
        // The guard drops on both the panicking and the clean read, so nothing stays counted.
        assert_eq!(
            health.gauge().0,
            0,
            "a read that panicked still left the in-flight count"
        );

        // And the fallback path, for a shard with no dedicated pool.
        let panicked: Result<(), OrchestratorError> =
            dispatch_read_pool(None, None, None, || panic!("redb panicked")).await;
        assert!(panicked.is_err(), "the fallback pool isolates a panic too");
        let after: u32 = dispatch_read_pool(None, None, None, || 9)
            .await
            .expect("the fallback pool serves the next read after a panic");
        assert_eq!(after, 9);
    });
}

/// The F7 fix: a read that has outlived its request is refused at dequeue, not run.
///
/// The pool is one thread wide and the first read holds it, so the second is still queued
/// when its budget expires — which is exactly the shape of the measured failure, where a
/// backlog of searches was worked through for clients that had all timed out. The closure
/// must not run: proving that is the entire point, since the wasted work is uncancellable
/// once it starts.
#[test]
fn a_read_that_outlived_its_request_is_refused_instead_of_run() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .max_blocking_threads(1)
        .enable_all()
        .build()
        .expect("read runtime");
    let handle = runtime.handle().clone();
    let health = Arc::new(ReadPoolHealth::new(1));
    let ran = Arc::new(AtomicBool::new(false));

    runtime.block_on(async {
        let budget = Duration::from_millis(50);

        // Occupy the only pool thread for longer than the second read's budget.
        let blocker = {
            let handle = handle.clone();
            let health = Arc::clone(&health);
            tokio::spawn(async move {
                dispatch_read_pool(Some(&handle), Some(health), None, || {
                    std::thread::sleep(Duration::from_millis(250));
                })
                .await
            })
        };
        // Let the blocker reach the pool thread before the queued read is offered.
        tokio::time::sleep(Duration::from_millis(20)).await;

        let ran_flag = Arc::clone(&ran);
        let queued: Result<(), OrchestratorError> = dispatch_read_pool(
            Some(&handle),
            Some(Arc::clone(&health)),
            Some(budget),
            move || {
                ran_flag.store(true, AtomicOrdering::SeqCst);
            },
        )
        .await;

        match queued {
            Err(OrchestratorError::ReadDeadlineExpired {
                waited_ms,
                budget_ms,
            }) => {
                assert_eq!(budget_ms, 50);
                assert!(
                    waited_ms >= 50,
                    "a refused read reports what it actually waited, got {waited_ms}ms"
                );
            }
            other => panic!("expected the queued read to be refused, got {other:?}"),
        }
        assert!(
            !ran.load(AtomicOrdering::SeqCst),
            "the closure ran anyway — the work this fix exists to avoid is still being done"
        );
        assert_eq!(
            health.abandoned(),
            1,
            "a refused read must be counted, or the shed is invisible"
        );

        blocker.await.expect("blocker joined").expect("blocker ran");
        assert_eq!(
            health.gauge().0,
            0,
            "a refused read must never have been counted as in flight"
        );
    });
}

/// The other half: the check costs a comparison and changes nothing when the node keeps up.
/// A budget is not a deadline on the work itself — a read that starts in time runs to
/// completion however long it takes.
#[test]
fn a_read_within_its_budget_runs_normally_however_slow() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .max_blocking_threads(2)
        .enable_all()
        .build()
        .expect("read runtime");
    let handle = runtime.handle().clone();
    let health = Arc::new(ReadPoolHealth::new(2));

    runtime.block_on(async {
        // Dequeued immediately, so the budget is never in play.
        let served: u32 = dispatch_read_pool(
            Some(&handle),
            Some(Arc::clone(&health)),
            Some(Duration::from_millis(50)),
            || 42,
        )
        .await
        .expect("an unqueued read is served");
        assert_eq!(served, 42);

        // And once running, it is not interrupted by its own budget.
        let slow: u32 = dispatch_read_pool(
            Some(&handle),
            Some(Arc::clone(&health)),
            Some(Duration::from_millis(10)),
            || {
                std::thread::sleep(Duration::from_millis(60));
                7
            },
        )
        .await
        .expect("a read that started in time runs to completion");
        assert_eq!(slow, 7);

        assert_eq!(
            health.abandoned(),
            0,
            "nothing was shed on a pool that kept up"
        );
    });
}

/// `None` is the escape hatch, and it must mean what it says: a shard with no configured
/// timeout runs every queued read however long it waited.
#[test]
fn no_budget_runs_a_stale_read() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .max_blocking_threads(1)
        .enable_all()
        .build()
        .expect("read runtime");
    let handle = runtime.handle().clone();
    let health = Arc::new(ReadPoolHealth::new(1));

    runtime.block_on(async {
        let blocker = {
            let handle = handle.clone();
            let health = Arc::clone(&health);
            tokio::spawn(async move {
                dispatch_read_pool(Some(&handle), Some(health), None, || {
                    std::thread::sleep(Duration::from_millis(120));
                })
                .await
            })
        };
        tokio::time::sleep(Duration::from_millis(20)).await;

        let queued: u32 = dispatch_read_pool(Some(&handle), Some(Arc::clone(&health)), None, || 5)
            .await
            .expect("no budget means no refusal");
        assert_eq!(queued, 5);
        assert_eq!(health.abandoned(), 0);
        blocker.await.expect("blocker joined").expect("blocker ran");
    });
}

/// A shed read is a `503`, the same answer the admission guard already gives, so a client
/// under overload sees one behaviour rather than two.
#[test]
fn a_shed_read_is_unavailable_not_a_fault() {
    let err = OrchestratorError::ReadDeadlineExpired {
        waited_ms: 3_000,
        budget_ms: 1_000,
    };
    assert_eq!(err.verdict(), RemoteVerdict::Unavailable);
}

/// A saturated read pool is load, not a fault; a saturated pool that has stopped making
/// progress is a wedge and turns the node red. Driven against a supplied clock (`is_wedged_at`)
/// so the stall lands without a 60-second wait.
#[test]
fn a_stuck_read_pool_reads_as_wedged_and_a_busy_one_does_not() {
    let pool = Arc::new(ReadPoolHealth::new(2));
    let threshold = READ_POOL_WEDGE_THRESHOLD.as_millis() as u64;

    // Idle: never wedged, however stale the progress tick.
    assert!(!pool.is_wedged_at(10 * threshold));

    // Two reads start (capacity full) and make progress at tick 1_000.
    let g1 = pool.track();
    let g2 = pool.track();
    pool.last_progress.store(1_000, AtomicOrdering::Relaxed);
    assert_eq!(
        pool.gauge(),
        (2, 2),
        "both reads are in flight against a width of two"
    );

    // At capacity but progress happened recently: saturated, not wedged.
    assert!(!pool.is_wedged_at(1_000 + threshold - 1));
    // At capacity with no progress past the threshold: wedged.
    assert!(pool.is_wedged_at(1_000 + threshold));

    // One read finishes: below capacity, so not wedged even long after.
    drop(g1);
    assert!(!pool.is_wedged_at(u64::MAX));
    drop(g2);
    assert_eq!(pool.gauge().0, 0);
}

/// A burst that saturates a pool which has been idle a long while is *not* a wedge.
///
/// This is the case that makes the start edge load-bearing. Track only completions and
/// `last_progress` is the age of the last read to finish, which on a quiet node is however
/// long ago that was — so the first traffic in a minute would saturate the pool and read as
/// wedged immediately, turning a healthy node red until the first read landed. A node idle
/// long enough to hit this is exactly a small one, where `max(2, cores / 2)` threads are
/// saturated by one fanned-out search.
///
/// Both halves are asserted against the same pool, because a check that cannot still catch
/// the genuine stall afterwards has bought the false negative rather than fixed anything.
#[test]
fn a_burst_after_a_long_idle_is_not_a_wedge_but_a_stall_in_it_still_is() {
    let pool = Arc::new(ReadPoolHealth::new(2));
    let threshold = READ_POOL_WEDGE_THRESHOLD.as_millis() as u64;

    // The pool served reads an hour ago and has been quiet since.
    let last_completion = 60 * threshold;
    pool.last_progress
        .store(last_completion, AtomicOrdering::Relaxed);

    // Five minutes later a burst arrives and fills every thread. `track` stamps the start,
    // so the reads that just began count as progress rather than as an hour of silence.
    let started = last_completion + 5 * threshold;
    let _g1 = pool.track_at(started);
    let _g2 = pool.track_at(started);
    assert_eq!(pool.gauge(), (2, 2), "the burst saturated the pool");

    assert!(
        !pool.is_wedged_at(started),
        "a pool saturated by work that just started reads as wedged"
    );
    assert!(
        !pool.is_wedged_at(started + threshold - 1),
        "a saturated pool is a fault before its work has had the threshold to finish"
    );

    // And the stall is still caught: those same reads, now stuck past the threshold.
    assert!(
        pool.is_wedged_at(started + threshold),
        "a pool whose reads have been stuck for the threshold is not reported wedged"
    );
}

/// A writer wedged mid-batch is as unavailable as a dead one, and an idle writer is not.
///
/// `down` catches a writer that *exited*; it can never catch one blocked inside a redb or
/// tantivy call, because that thread never returns to bump it. The heartbeat closes that gap:
/// a stamp that stops advancing while non-zero is a stuck writer, a 0 stamp is one idle on its
/// channel. This drives the stamps by hand — no writer thread — to pin the fold health reads.
#[test]
fn a_wedged_writer_counts_as_unavailable_and_an_idle_one_does_not() {
    let liveness = WriterLiveness::default();
    let heartbeat = liveness.register_writer();
    let threshold = WRITER_STALL_THRESHOLD.as_millis() as u64;

    // Idle on the channel (stamp 0): never stalled, however much time passes.
    assert_eq!(liveness.stalled_count(threshold * 10, threshold), 0);

    // Mid-batch since tick 1_000, but not yet past the threshold: still healthy.
    heartbeat.store(1_000, AtomicOrdering::Relaxed);
    assert_eq!(liveness.stalled_count(1_000 + threshold - 1, threshold), 0);

    // Mid-batch past the threshold: wedged.
    assert_eq!(liveness.stalled_count(1_000 + threshold, threshold), 1);

    // The writer finished its batch and went back to waiting: healthy again.
    heartbeat.store(0, AtomicOrdering::Relaxed);
    assert_eq!(liveness.stalled_count(u64::MAX, threshold), 0);
}

/// `unavailable_writers` is the single number the health branch folds: dead writers plus
/// wedged ones, from the same struct, so one call decides the whole data-path verdict. Driven
/// against a supplied clock (`unavailable_at`) so the stall lands without a 60-second wait.
#[test]
fn unavailable_writers_sums_dead_and_wedged() {
    let liveness = WriterLiveness::default();
    let threshold = WRITER_STALL_THRESHOLD.as_millis() as u64;
    let now = 10 * threshold; // well past any stamp we place below
    assert_eq!(liveness.unavailable_at(now), 0);

    // One writer exits abnormally.
    liveness.mark_writer_down();
    assert_eq!(liveness.unavailable_at(now), 1);

    // A second writer is registered and wedges mid-batch a full threshold ago.
    let heartbeat = liveness.register_writer();
    heartbeat.store(now - threshold, AtomicOrdering::Relaxed);
    assert_eq!(
        liveness.unavailable_at(now),
        2,
        "dead and wedged both count"
    );

    // That writer unblocks and returns to waiting: only the dead one remains.
    heartbeat.store(0, AtomicOrdering::Relaxed);
    assert_eq!(liveness.unavailable_at(now), 1);
}

fn writer_test_config(path: std::path::PathBuf) -> StorageConfig {
    StorageConfig {
        max_open_indexes: 0,
        shard_path: path,
        indexer_memory_budget: 32 * 1024 * 1024,
        indexer_memory_min_mb: 16,
        indexer_memory_max_mb: 256,
        total_memory_limit_bytes: 4 * 1024 * 1024 * 1024,
        memory_pressure_threshold_percent: 80,
        indexer_num_threads: 1,
        merge_num_threads: 1,
        default_batch_size: 1000,
        wal_sync: true,
        commit_interval_ms: 0,
        query: Default::default(),
    }
}

/// A panic applying a write is caught and reported, not left to unwind the writer thread.
///
/// This is the per-command boundary the writer loop wraps every op in: a panic in tantivy or
/// redb becomes a `WriterPanicked` error for that index's caller — retriable, because the
/// writer is dropped and rebuilt on the next write — while the thread goes on. A normal result
/// passes straight through.
#[test]
fn guard_writer_op_turns_a_panic_into_a_retriable_writer_reset() {
    let dir = tempfile::tempdir().expect("temp dir");
    let store = HybridStore::new(writer_test_config(dir.path().to_path_buf()), 1).expect("store");

    let ok: Result<u32, StoreError> = guard_writer_op(&store, "idx", || Ok(7));
    assert_eq!(ok.expect("a normal op passes through"), 7);

    let panicked: Result<u32, StoreError> =
        guard_writer_op(&store, "idx", || panic!("tantivy panicked mid-write"));
    assert!(
        matches!(&panicked, Err(StoreError::WriterPanicked(index)) if index == "idx"),
        "a panicked write is reported as a writer reset, got {panicked:?}"
    );
}

/// Each writer that stops raises the count the health endpoint reads.
#[test]
fn writer_liveness_counts_the_writers_that_have_stopped() {
    let liveness = WriterLiveness::default();
    assert_eq!(liveness.unavailable_writers(), 0);
    liveness.mark_writer_down();
    liveness.mark_writer_down();
    assert_eq!(liveness.unavailable_writers(), 2);
}

/// Respawning a dead writer clears its down-count, so a shard that self-heals reports green
/// again — and the clear can never drive the count below the writers actually down.
#[test]
fn respawning_a_writer_clears_its_down_count() {
    let liveness = WriterLiveness::default();
    liveness.mark_writer_down();
    assert_eq!(liveness.unavailable_writers(), 1, "a dead writer counts");

    liveness.mark_writer_up();
    assert_eq!(
        liveness.unavailable_writers(),
        0,
        "its replacement clears the count"
    );

    // Saturating: an extra up with nothing down cannot wrap the count back up.
    liveness.mark_writer_up();
    assert_eq!(liveness.unavailable_writers(), 0);
}

/// An engine with no shards and no coordinator. Enough to exercise `execute`'s dispatch
/// table, which decides what the worker pool will and will not serve before any shard,
/// schema or store is touched.
fn bare_engine() -> OrchestratorEngine {
    OrchestratorEngine {
        shards: ArcSwap::from_pointee(HashMap::new()),
        routing_ring: Arc::new(ArcSwap::from_pointee(ConsistentRing::new())),
        schema_cache: Arc::new(SchemaCache::new()),
        quotas: Arc::new(TenantQuotas::new(HashMap::new())),
        coordinator: None,
        identity: NodeIdentity::new(),
        default_search_limit: 10,
        max_concurrent_shard_searches: 4,
        remote_peer_pool: Arc::new(RemotePeerPool::new()),
        canvass: SchemaCanvass {
            clustered: false,
            coordinator: None,
            pool: None,
        },
    }
}

/// What a write is routed by, and in which order.
///
/// The document's routing field outranks the caller's `routing_key`, and that precedence is
/// the whole reason the shard-affine hint may not choose the shard: the hint is computed from
/// `routing_key.or(id)` before any schema is in hand, so on an index that routes by a
/// non-key field the two disagree. Taking the hint used to put such a document on a shard the
/// ring did not own, and a later write of the same id through a hintless path put a second
/// copy on the shard that did. The hint now picks only the worker; this pins the rule it used
/// to be able to overrule.
#[test]
fn the_routing_key_comes_from_the_document_before_the_caller() {
    let mut schema = IndexSchema::default();
    schema.fields.insert(
        "tenant_id".to_string(),
        FieldDef::new("tenant_id".to_string(), TantivyFieldType::String),
    );
    schema
        .set_routing_field("tenant_id".to_string())
        .expect("the field exists");

    let doc = json!({"id": "d1", "tenant_id": "acme", "title": "Dune"});

    assert_eq!(
        effective_routing_key(&schema, "d1", Some("d1".to_string()), &doc).as_deref(),
        Some("acme"),
        "the schema's routing field decides, even against an explicit routing_key"
    );
    assert_eq!(
        effective_routing_key(&schema, "d1", None, &doc).as_deref(),
        Some("acme"),
        "and with no routing_key at all"
    );

    // A document that does not carry the routing field falls through the rest of the order.
    let bare = json!({"id": "d1", "title": "Dune"});
    assert_eq!(
        effective_routing_key(&schema, "d1", Some("caller".to_string()), &bare).as_deref(),
        Some("caller"),
        "the caller's key is honoured only where the document is silent"
    );
    assert_eq!(
        effective_routing_key(&schema, "d1", None, &bare).as_deref(),
        Some("d1"),
        "then the id"
    );
    assert!(
        effective_routing_key(&schema, "", None, &bare).is_some(),
        "and finally a hash of the document, so an unkeyed write is still deterministic"
    );

    // The default index routes by the key, which is what makes a delete-by-id unicast.
    let default_schema = IndexSchema::default();
    assert_eq!(
        effective_routing_key(&default_schema, "d1", None, &bare).as_deref(),
        Some("d1"),
        "routing_field defaults to `id`"
    );
}

/// A bulk delete entry is either a bare id or an object naming its routing key.
///
/// Untagged enums resolve by trying each variant in order, which is exactly the kind of
/// deserialization that works until someone reorders the variants. Both shapes and both
/// accessors are pinned here, along with the rejection of an entry that is neither.
#[test]
fn a_delete_entry_reads_as_a_bare_id_or_as_an_object() {
    let parsed: Vec<DeletePayload> = serde_json::from_value(json!([
        "b1",
        {"id": "b2"},
        {"id": "b3", "routing_key": "acme"},
    ]))
    .expect("both shapes deserialize");

    let seen: Vec<(&str, Option<&str>)> = parsed
        .iter()
        .map(|entry| (entry.id(), entry.routing_key()))
        .collect();
    assert_eq!(
        seen,
        vec![("b1", None), ("b2", None), ("b3", Some("acme")),]
    );

    // A bare id round-trips as a bare id, so a forwarded batch reaches a peer in the shape
    // the caller sent.
    assert_eq!(
        serde_json::to_value(&parsed[0]).expect("serialize"),
        json!("b1")
    );

    assert!(
        serde_json::from_value::<Vec<DeletePayload>>(json!([{"document": "b1"}])).is_err(),
        "an entry that names no id is refused rather than read as something else"
    );
}

/// An op from a peer that predates `forwarded` reads as a first hop.
///
/// The flag is what stops two nodes disagreeing about a shard from passing one write between
/// them, and it is a field added to an op that crosses the wire. kameo encodes a remote
/// message with `rmp_serde::to_vec_named`, so fields travel by name and `#[serde(default)]`
/// is what makes an older peer's message — which carries no such field — decode as `false`,
/// the behaviour every op had before this existed. Asserted through `serde_json`, which has
/// the same rule for a missing field; what is pinned here is the `default`, not the codec.
#[test]
fn an_op_without_the_forwarded_flag_reads_as_a_first_hop() {
    let write: ClientOp = serde_json::from_str(
        r#"{"Write":{"index":"i","id":"d1","routing_key":null,"doc":{"t":1}}}"#,
    )
    .expect("a Write from a peer that does not send `forwarded`");
    assert!(
        matches!(
            write,
            ClientOp::Write {
                forwarded: false,
                ..
            }
        ),
        "an absent flag is a first hop, which is what every op was before it existed"
    );

    let delete: ClientOp =
        serde_json::from_str(r#"{"Delete":{"index":"i","id":"d1","routing_key":null}}"#)
            .expect("a Delete from a peer that does not send `forwarded`");
    assert!(matches!(
        delete,
        ClientOp::Delete {
            forwarded: false,
            ..
        }
    ));

    // And a hop that was forwarded says so, so the owner can refuse to forward again.
    let hop: ClientOp = serde_json::from_str(
        r#"{"Delete":{"index":"i","id":"d1","routing_key":"acme","forwarded":true}}"#,
    )
    .expect("a Delete forwarded by a peer running this code");
    assert!(matches!(
        hop,
        ClientOp::Delete {
            forwarded: true,
            ..
        }
    ));
}

/// What a delete is routed by, and when it cannot be routed at all.
///
/// A write reads its key out of the document, which is the authority. A delete has no
/// document, so the schema decides whether the id is enough: it is where the routing field is
/// the key — `id`, or a shadow field whose value *is* the key — and it is not where the index
/// routes by a tenant or a customer. The refusal in that case is deliberate; fanning out to
/// every shard is correct and costs shards × nodes transactions to remove one row.
#[test]
fn a_delete_routes_by_the_id_unless_the_index_routes_by_something_else() {
    // The default: the routing field is the key, so the id is the key.
    let default_schema = IndexSchema::default();
    assert_eq!(
        effective_delete_routing_key(&default_schema, "d1", None).expect("routable"),
        "d1"
    );

    // A shadow index: the routing field is `sha1`, whose value is the document key, so the
    // id routes exactly as the original write did.
    let mut shadow = IndexSchema::default();
    shadow.add_shadow_field("sha1".to_string(), TantivyFieldType::String);
    shadow
        .set_routing_field("sha1".to_string())
        .expect("the field exists");
    assert!(shadow.is_shadow_field("sha1"));
    assert_eq!(
        effective_delete_routing_key(&shadow, "abc123", None).expect("routable"),
        "abc123"
    );

    // A tenant index: the id says nothing about which shard holds the row.
    let mut tenanted = IndexSchema::default();
    tenanted.fields.insert(
        "tenant_id".to_string(),
        FieldDef::new("tenant_id".to_string(), TantivyFieldType::String),
    );
    tenanted
        .set_routing_field("tenant_id".to_string())
        .expect("the field exists");

    let refused = effective_delete_routing_key(&tenanted, "d1", None)
        .expect_err("a keyless delete on a tenant index cannot be routed");
    let message = refused.to_string();
    assert!(
        message.contains("tenant_id"),
        "the refusal must name the field to supply: {message}"
    );
    assert!(
        message.contains("id:d1"),
        "and how to find its value: {message}"
    );
    assert!(
        matches!(&refused, OrchestratorError::Validation(_)),
        "Validation is what `AppError::from_route` turns into a 400 rather than a 500"
    );

    // ...and with the key supplied a tenant index routes by it.
    assert_eq!(
        effective_delete_routing_key(&tenanted, "d1", Some("acme".to_string())).expect("routable"),
        "acme"
    );
    // On a key-routed index the id still wins, so a stray routing_key cannot retarget the
    // delete to a shard that holds no such row and answer "deleted" for nothing.
    assert_eq!(
        effective_delete_routing_key(&default_schema, "d1", Some("acme".to_string()))
            .expect("routable"),
        "d1"
    );
    assert_eq!(
        effective_delete_routing_key(&shadow, "abc123", Some("elsewhere".to_string()))
            .expect("routable"),
        "abc123"
    );

    // An empty key on a tenant index is the same as no key: refused, not routed by "".
    let empty = effective_delete_routing_key(&tenanted, "d1", Some(String::new()))
        .expect_err("an empty routing_key cannot route");
    assert!(
        empty.to_string().contains("tenant_id"),
        "the refusal names the field: {empty}"
    );
}

/// The engine cannot evolve a schema — it holds snapshots, not `&mut NodeOrchestrator`.
/// What matters is that declining returns the *op*, not a sentinel error: the caller
/// moved the op into the job and has nothing left to retry with otherwise. A write to an
/// index with no schema used to reach the client as a 500.
#[tokio::test]
async fn an_op_the_engine_declines_comes_back_whole() {
    let dir = tempfile::tempdir().expect("temp dir");
    let engine = bare_engine();

    // A shard in the map is what takes the op past the "no shards" refusal to the schema
    // question. Never started, so nothing behind it runs: the unsettled index it cannot
    // serve is what defers.
    let shard_id = Uuid::new_v4();
    let shard = MicroshardActor::new(
        shard_id,
        writer_test_config(dir.path().to_path_buf()),
        ShardRuntime {
            default_search_limit: 10,
            read_pool_handle: None,
            read_pool_health: None,
            read_budget: None,
            total_shards: 1,
            writer_shutdown_timeout_secs: 5,
            supervisor_timeout_secs: 5,
            writer_pin: WriterPin {
                target: None,
                outcome: Arc::new(AtomicI64::new(UNPINNED)),
            },
            writer_liveness: Arc::new(WriterLiveness::default()),
        },
    );
    engine
        .shards
        .store(Arc::new(HashMap::from([(shard_id, shard)])));

    let outcome = engine
        .execute(ClientOp::BulkWrite {
            index: "books".to_string(),
            docs: vec![DocPayload {
                id: "b1".to_string(),
                routing_key: None,
                doc: json!({"title": "Dune"}),
            }],
            forwarded: false,
            schema_body: None,
            tenant: None,
        })
        .await;

    match outcome {
        WorkerOutcome::UseActor(op) => match *op {
            ClientOp::BulkWrite { index, docs, .. } => {
                assert_eq!(index, "books");
                assert_eq!(
                    docs.len(),
                    1,
                    "the documents have to survive the round trip"
                );
                assert_eq!(docs[0].doc, json!({"title": "Dune"}));
            }
            other => panic!("the op came back as a different op: {other:?}"),
        },
        WorkerOutcome::Done(result) => {
            panic!("bulk write should defer to the actor, got {result:?}")
        }
    }
}

/// A worker operation whose future is boxed so the runner closures below can be named in
/// a return type. The loop never inspects the op, only how many are running.
type TestOp = std::pin::Pin<Box<dyn std::future::Future<Output = WorkerOutcome> + Send>>;

fn placeholder_op() -> Box<ClientOp> {
    Box::new(ClientOp::Write {
        index: "bench".to_string(),
        id: "d1".to_string(),
        routing_key: None,
        doc: json!({"title": "Dune"}),
        forwarded: false,
        schema_body: None,
        tenant: None,
    })
}

/// Bucketing must never report a service time *below* what was measured: admission that
/// under-reads a cost admits work that cannot finish, which is the failure the whole gate
/// exists to prevent. Checked across the range a real sample can land in.
#[test]
fn a_bucketed_service_time_never_reads_low() {
    for sample_us in [
        0u64, 1, 15, 16, 17, 100, 999, 1_000, 8_500, 54_000, 1_000_000,
    ] {
        let bucket = ServiceHistogram::bucket_of(sample_us);
        let reported = ServiceHistogram::bucket_upper_us(bucket);
        assert!(
            reported >= sample_us,
            "sample {sample_us}µs landed in bucket {bucket} reported as {reported}µs"
        );
    }
}

/// The wait ahead of a new arrival is a sum of service times, so its spread grows as the
/// root of the depth and not with it. Pinned as the property the first cut got wrong: a
/// per-request quantile multiplied by depth grows the margin linearly, which over-predicts
/// a deep queue and admits a shallower one than the budget can carry.
#[test]
fn a_queues_predicted_spread_grows_with_the_root_of_its_depth() {
    let hist = ServiceHistogram::new();
    // A spread-out distribution, so sigma is meaningfully non-zero.
    for _ in 0..50 {
        hist.record(Duration::from_millis(5));
    }
    for _ in 0..50 {
        hist.record(Duration::from_millis(45));
    }
    let (mean_us, sigma_us) = hist
        .moments_of(hist.active.load(AtomicOrdering::Relaxed) & 1)
        .expect("samples recorded");
    assert!(sigma_us > 0, "a two-valued distribution has spread");

    // The margin over the mean at depth 4d must be 2x the margin at depth d, not 4x.
    let margin = |depth: f64| SERVICE_ADMISSION_SIGMAS * sigma_us as f64 * depth.sqrt();
    let ratio = margin(400.0) / margin(100.0);
    assert!(
        (ratio - 2.0).abs() < 1e-9,
        "quadrupling depth must double the margin, got {ratio}x"
    );
    assert!(mean_us > 0);
}

/// A quantile has to actually separate the body of the distribution from its tail, and
/// which quantile does that depends on how heavy the tail is. 90 samples at 8ms and 10 at
/// 200ms: the p90 sits exactly on the boundary and still reads 8ms, while the p95 is in the
/// tail. Pinned because it is the trap in "reserve a percentile instead of the mean" — a
/// percentile chosen at the weight of the tail reports the body.
#[test]
fn a_quantile_reports_the_tail_only_once_it_is_past_it() {
    let hist = ServiceHistogram::new();
    for _ in 0..90 {
        hist.record(Duration::from_millis(8));
    }
    for _ in 0..10 {
        hist.record(Duration::from_millis(200));
    }
    let generation = hist.active.load(AtomicOrdering::Relaxed) & 1;

    let p90 = hist
        .quantile_of(generation, 0.90)
        .expect("samples recorded");
    assert!(p90 < 16_000, "p90 sits on the boundary and reads the body");

    let p95 = hist
        .quantile_of(generation, 0.95)
        .expect("samples recorded");
    assert!(
        p95 >= 200_000,
        "p95 should be inside the 200ms tail, read {p95}µs"
    );
}

/// A window with no samples must not reset the estimate to zero — that would admit
/// everything the moment a node went quiet, which is when its next burst arrives.
#[test]
fn an_empty_window_leaves_the_estimate_alone() {
    let hist = ServiceHistogram::new();
    assert_eq!(
        hist.quantile_of(hist.active.load(AtomicOrdering::Relaxed) & 1, 0.90),
        None,
        "an empty generation has no quantile to report"
    );
    assert_eq!(hist.estimate_us(), 0, "and nothing is cached from it");
}

/// Without a routing field in the schema, the key is exactly the schema-free ladder. That
/// ladder once had a second copy in the HTTP layer's bulk hint, and the two had drifted onto
/// different hashes of different byte ranges; the hint is gone, and the ladder is still one.
#[test]
fn without_a_routing_field_the_key_is_the_schema_free_ladder() {
    // No routing field, so `effective_routing_key` falls straight through.
    let schema = IndexSchema::default();
    let doc = json!({"title": "Dune", "author": "Herbert"});

    // No schema routing field, so `effective_routing_key` falls straight through to the
    // shared ladder and the two must produce the same answer at every rung.
    for (id, caller) in [("d1", Some("caller".to_string())), ("d1", None), ("", None)] {
        assert_eq!(
            effective_routing_key(&schema, id, caller.clone(), &doc),
            routing_key_without_schema(caller.clone(), id, &doc),
            "the key left the schema-free ladder for id={id:?} caller={caller:?}"
        );
    }
}

/// The unkeyed rung is the orchestrator's own derivation, and deliberately not the HTTP
/// layer's former `xxh3_64`. Which shard a document lands on is decided here, so unifying
/// the two spellings had to keep *this* one: a changed hash would move unkeyed documents to
/// different shards across an upgrade.
#[test]
fn the_unkeyed_rung_keeps_the_derivation_that_places_shards() {
    let doc = json!({"title": "Dune"});
    let derived = routing_key_without_schema(None, "", &doc).expect("a key is derived");
    assert_eq!(
        derived,
        derive_routing_key_from_doc(&doc).expect("the same derivation"),
        "the shared ladder's last rung is the orchestrator's derivation"
    );
    // Hex of a JSON prefix, so it is even-length hex and not a 16-char xxh3 digest.
    assert!(derived.len().is_multiple_of(2) && derived.chars().all(|c| c.is_ascii_hexdigit()));
}

/// A bulk delete from a peer built before the hop marker existed must read as a first hop.
///
/// `serde(default)` is the compatibility promise, and it points the safe way for a *mixed*
/// cluster: an older peer's forward reads as `false`, which is the behaviour that node
/// already had, rather than as `true`, which would make this node refuse a batch it can
/// place. The bound closes once both ends are new.
#[test]
fn a_bulk_delete_without_a_hop_marker_reads_as_a_first_hop() {
    let op = ClientOp::BulkDelete {
        index: "books".to_string(),
        docs: vec![],
        forwarded: true,
    };
    let mut wire = serde_json::to_value(&op).expect("serializes");

    // Strip the field, which is exactly what an older peer sends.
    let body = wire
        .get_mut("BulkDelete")
        .and_then(|v| v.as_object_mut())
        .expect("externally tagged");
    assert!(
        body.remove("forwarded").is_some(),
        "the marker is on the wire when this node sends it"
    );

    let parsed: ClientOp = serde_json::from_value(wire).expect("deserializes without it");
    match parsed {
        ClientOp::BulkDelete { forwarded, .. } => assert!(
            !forwarded,
            "a batch with no marker is a first hop, not a forwarded one"
        ),
        other => panic!("round-tripped into {other:?}"),
    }
}

/// A merged write hands each caller a contiguous run of the ids it asked for, and the runs
/// partition the result exactly. A mis-walked offset would give one caller another's
/// sequence ids, which nothing downstream could detect.
#[test]
fn a_merged_write_partitions_its_sequence_ids_exactly() {
    // One single write, a batch of three, another single, a batch of two.
    let ranges = merged_reply_ranges(&[1, 3, 1, 2], 7).expect("counts match the ids");
    assert_eq!(
        ranges,
        vec![0..1, 1..4, 4..5, 5..7],
        "each caller owns the run it contributed, in order"
    );
    // Contiguous, non-overlapping, and covering everything.
    assert_eq!(ranges.last().unwrap().end, 7);
}

/// The storage layer's contract is one sequence id per op. If it is ever not, the merge
/// must refuse the whole group rather than index past the end — this runs on the writer
/// thread, where a panic takes every shard's writes down with it.
#[test]
fn a_merged_write_refuses_a_short_result_rather_than_indexing_past_it() {
    assert!(
        merged_reply_ranges(&[1, 3, 2], 5).is_none(),
        "six ops against five ids must not produce ranges"
    );
    assert!(
        merged_reply_ranges(&[1, 3, 2], 7).is_none(),
        "six ops against seven ids is just as wrong"
    );
    assert!(
        merged_reply_ranges(&[], 0).is_some(),
        "an empty merge is fine"
    );
}

/// A cancelled ask must still give its slot back. `TimeoutLayer` drops the request future
/// when the budget expires — under overload, most of them — and an increment that survives
/// its request makes the lane's depth climb monotonically until the gate refuses everything
/// and no sample can ever correct it. Measured before the guard existed: `mailbox_depth` 63
/// on an idle node, 100% of requests refused (ROADMAP F8).
#[test]
fn a_dropped_mailbox_ask_gives_its_slot_back() {
    let stats = Arc::new(DispatchCounters::default());

    let slot = MailboxSlot::enter(Arc::clone(&stats));
    assert_eq!(stats.outstanding.load(AtomicOrdering::Relaxed), 1);

    // Dropping without awaiting anything is what a cancelled request does.
    drop(slot);
    assert_eq!(
        stats.outstanding.load(AtomicOrdering::Relaxed),
        0,
        "a cancelled ask must not leave its increment behind"
    );

    // And the lane is usable again rather than permanently one deeper.
    let next = MailboxSlot::enter(Arc::clone(&stats));
    drop(next);
    assert_eq!(stats.outstanding.load(AtomicOrdering::Relaxed), 0);
}

/// Overlapping asks each account for themselves, in any release order — the depth the gate
/// reads is the number actually outstanding, not the number that happened to finish in order.
#[test]
fn overlapping_mailbox_asks_each_release_their_own_slot() {
    let stats = Arc::new(DispatchCounters::default());
    let first = MailboxSlot::enter(Arc::clone(&stats));
    let second = MailboxSlot::enter(Arc::clone(&stats));
    assert_eq!(stats.outstanding.load(AtomicOrdering::Relaxed), 2);

    drop(first);
    assert_eq!(stats.outstanding.load(AtomicOrdering::Relaxed), 1);
    drop(second);
    assert_eq!(stats.outstanding.load(AtomicOrdering::Relaxed), 0);
}

/// The estimate is what makes the dequeue check work, so its two properties are pinned:
/// it starts permissive, and it reserves more than one service time.
#[test]
fn the_service_estimate_starts_permissive_and_reserves_a_margin() {
    let budget = Duration::from_secs(10);
    let stats = DispatchCounters::default();
    assert_eq!(
        stats.service_reserve_for(OpClass::Read, budget),
        Duration::ZERO,
        "a node with no evidence must admit everything"
    );

    // The first sample is taken whole rather than averaged against a zero that means
    // "unknown" — otherwise the estimate spends its first jobs climbing out of a value it
    // never measured.
    stats.record_service(OpClass::Read, Duration::from_millis(200));
    assert_eq!(
        stats.service_reserve_for(OpClass::Read, budget),
        Duration::from_millis(400)
    );

    // And it tracks, rather than jumping, once it has a history.
    for _ in 0..40 {
        stats.record_service(OpClass::Read, Duration::from_millis(100));
    }
    let reserve = stats.service_reserve_for(OpClass::Read, budget);
    assert!(
        reserve > Duration::from_millis(190) && reserve < Duration::from_millis(215),
        "the estimate should have converged on ~100ms, reserving ~200ms, got {reserve:?}"
    );
}

/// The cap, which is what keeps this fix from becoming the failure it fixes.
///
/// An estimate above half the budget would make the reserve exceed the budget, refusing
/// every job including one that has waited no time at all. Nothing completes, nothing
/// updates the estimate, and the node refuses everything for good. A freshly arrived job
/// must always be admitted, whatever the estimate says.
#[test]
fn a_service_estimate_larger_than_the_budget_cannot_wedge_the_node() {
    let budget = Duration::from_secs(1);
    let stats = DispatchCounters::default();

    // Far beyond the budget — a node that was very slow, or a budget that was lowered.
    stats.record_service(OpClass::Any, Duration::from_secs(30));

    let reserve = stats.service_reserve_for(OpClass::Read, budget);
    assert_eq!(
        reserve,
        budget / 2,
        "the reserve must be capped at half the budget, got {reserve:?}"
    );
    assert!(
        reserve < budget,
        "a reserve at or above the budget refuses a job that has waited no time at all"
    );
    // Which is the property that matters: a job arriving now still fits.
    assert!(Duration::ZERO + reserve < budget);
}

/// The F7 fix, at the queue the measurement found it in.
///
/// A job that has been queued longer than its budget is refused at dequeue rather than run,
/// and the refusal is a `503` the client can act on rather than work done for nobody.
#[tokio::test]
async fn a_job_that_outlived_its_request_is_refused_at_dequeue() {
    let (tx, rx) = mpsc::channel::<OrchestratorJob>(8);
    let stats = Arc::new(DispatchCounters::default());
    let live = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));

    tokio::spawn(orchestrator_worker_loop(
        rx,
        recording_runner(
            Duration::from_millis(5),
            Arc::clone(&live),
            Arc::clone(&peak),
        ),
        0,
        None,
        4,
        Some(Duration::from_millis(50)),
        Some(Arc::clone(&stats)),
    ));

    // Enqueued as though it had already been waiting longer than its budget.
    let (reply, answer) = tokio::sync::oneshot::channel();
    tx.send(OrchestratorJob::Execute {
        arrived_at: Instant::now() - Duration::from_millis(500),
        op: placeholder_op(),
        affinity_shard: None,
        reply,
    })
    .await
    .expect("the worker channel is open");

    match answer.await.expect("the worker answers") {
        WorkerOutcome::Done(Err(OrchestratorError::ReadDeadlineExpired {
            waited_ms,
            budget_ms,
        })) => {
            assert!(waited_ms >= 500, "got {waited_ms}ms");
            assert_eq!(budget_ms, 50);
        }
        WorkerOutcome::Done(Err(other)) => panic!("expected a refusal, got {other:?}"),
        _ => panic!("expected a refusal, got an answer"),
    }
    assert_eq!(stats.abandoned.load(AtomicOrdering::Relaxed), 1);
    assert_eq!(
        peak.load(AtomicOrdering::Relaxed),
        0,
        "the operation ran anyway — the wasted work is still being done"
    );

    // And a fresh job on the same worker is served: shedding is per job, not a mode the
    // worker latches into.
    let answer = submit(&tx).await;
    assert!(matches!(
        answer.await.expect("the worker answers"),
        WorkerOutcome::Done(Ok(_))
    ));
    assert_eq!(stats.abandoned.load(AtomicOrdering::Relaxed), 1);
}

/// No budget is the pre-F7 behaviour, and it has to stay reachable: a stale job runs.
#[tokio::test]
async fn without_a_budget_a_stale_job_still_runs() {
    let (tx, rx) = mpsc::channel::<OrchestratorJob>(8);
    let live = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));

    tokio::spawn(orchestrator_worker_loop(
        rx,
        recording_runner(
            Duration::from_millis(5),
            Arc::clone(&live),
            Arc::clone(&peak),
        ),
        0,
        None,
        4,
        None,
        None,
    ));

    let (reply, answer) = tokio::sync::oneshot::channel();
    tx.send(OrchestratorJob::Execute {
        arrived_at: Instant::now() - Duration::from_secs(60),
        op: placeholder_op(),
        affinity_shard: None,
        reply,
    })
    .await
    .expect("the worker channel is open");

    assert!(matches!(
        answer.await.expect("the worker answers"),
        WorkerOutcome::Done(Ok(_))
    ));
    assert_eq!(peak.load(AtomicOrdering::Relaxed), 1);
}

// ------------------------------------------------------------------
// F7 fix 2 — refusing at admission rather than at a worker.
// ------------------------------------------------------------------

/// Build a load signal whose pool-wide `outstanding` is set by hand, so the prediction can
/// be checked against a backlog the test decides. The split between `queued` and `running`
/// describes the scenario — the gate reads only their sum.
fn queue_load_with(
    queued: &[usize],
    running: &[usize],
    width: usize,
    budget: Option<Duration>,
) -> (QueueLoad, Arc<DispatchCounters>) {
    let dispatch = Arc::new(DispatchCounters::default());
    dispatch.outstanding.store(
        queued.iter().sum::<usize>() + running.iter().sum::<usize>(),
        AtomicOrdering::Relaxed,
    );
    let load = QueueLoad::new(Arc::clone(&dispatch), width, budget);
    (load, dispatch)
}

/// Little's law over the gauges the pool already keeps: work ahead, divided by how much of
/// it runs at once, times what one job costs.
#[test]
fn the_predicted_wait_is_the_backlog_divided_by_how_much_runs_at_once() {
    // Width 4, so four jobs can be in service at once.
    let (load, dispatch) = queue_load_with(&[0, 0], &[2, 2], 4, Some(Duration::from_secs(1)));
    dispatch.record_service(OpClass::Read, Duration::from_millis(100));

    // Exactly full and nothing queued: an arrival waits for the next slot, not a round.
    assert_eq!(load.depth(), 4);
    assert_eq!(load.predicted_wait(), Duration::ZERO);

    // One full round of work ahead of the arrival.
    let (load, dispatch) = queue_load_with(&[2, 2], &[2, 2], 4, Some(Duration::from_secs(1)));
    dispatch.record_service(OpClass::Read, Duration::from_millis(100));
    assert_eq!(load.depth(), 8);
    assert_eq!(load.predicted_wait(), Duration::from_millis(100));

    // Three rounds ahead. This is the number the semaphore cannot see: the same 16
    // requests are 16 permits either way, but only one of the two says 300ms.
    let (load, dispatch) = queue_load_with(&[6, 6], &[2, 2], 4, Some(Duration::from_secs(1)));
    dispatch.record_service(OpClass::Read, Duration::from_millis(100));
    assert_eq!(load.depth(), 16);
    assert_eq!(load.predicted_wait(), Duration::from_millis(300));
}

/// A partly-filled round still has to finish before the next one starts, so the arithmetic
/// rounds up. Rounding down would predict zero wait for a backlog that plainly has one.
#[test]
fn a_partly_filled_round_still_counts_as_a_round() {
    let (load, dispatch) = queue_load_with(&[1, 0], &[2, 2], 4, Some(Duration::from_secs(1)));
    dispatch.record_service(OpClass::Read, Duration::from_millis(100));
    assert_eq!(load.depth(), 5);
    assert_eq!(load.predicted_wait(), Duration::from_millis(100));
}

/// The invariant that keeps this gate from becoming the failure it prevents.
///
/// The estimate is only ever updated by jobs that complete. A gate that can refuse an
/// arrival into a pool with a free slot can therefore stop every job, stop every sample,
/// and go on refusing forever against a number nothing will correct. F7's own reserve had
/// this exact shape before it was capped; this is the same mistake one layer out, and it
/// is ruled out by construction rather than by the estimate happening to stay small.
#[test]
fn a_pool_with_a_free_slot_admits_whatever_the_estimate_says() {
    let budget = Duration::from_millis(100);
    // Width 8, only 7 jobs anywhere in the pool — one slot free.
    let (load, dispatch) = queue_load_with(&[3, 0], &[2, 2], 8, Some(budget));
    // An estimate hundreds of times the budget, which is what a lowered timeout or a very
    // slow index produces.
    dispatch.record_service(OpClass::Read, Duration::from_secs(30));

    assert!(load.depth() < 8);
    assert_eq!(
        load.would_refuse(OpClass::Any),
        None,
        "a pool with a free slot must admit, or nothing ever completes to correct the estimate"
    );
}

/// The gate itself: a backlog that cannot clear inside the budget is refused before it is
/// queued, and the refusal is counted where an operator can read it.
#[test]
fn a_backlog_that_cannot_clear_in_time_is_refused_at_admission() {
    let budget = Duration::from_secs(1);
    // Width 4 with 16 jobs in the pool — three rounds ahead.
    let (load, dispatch) = queue_load_with(&[6, 6], &[2, 2], 4, Some(budget));
    dispatch.record_service(OpClass::Read, Duration::from_millis(400));

    // 3 rounds x 400ms predicted, plus the 500ms reserve (2x400ms capped at half the
    // budget), against a 1s budget.
    let predicted = load
        .would_refuse(OpClass::Any)
        .expect("this backlog cannot be served in time");
    assert_eq!(predicted, Duration::from_millis(1200));

    assert_eq!(
        dispatch.refused_at_admission.load(AtomicOrdering::Relaxed),
        0
    );
    let err = load.refuse(predicted);
    assert_eq!(
        dispatch.refused_at_admission.load(AtomicOrdering::Relaxed),
        1
    );
    assert!(matches!(
        err,
        OrchestratorError::Overloaded {
            predicted_wait_ms: 1200,
            budget_ms: 1000
        }
    ));

    // And a backlog that *can* clear is admitted: the gate is about the deadline, not
    // about the queue being non-empty.
    let (load, dispatch) = queue_load_with(&[1, 1], &[2, 2], 4, Some(budget));
    dispatch.record_service(OpClass::Read, Duration::from_millis(50));
    assert_eq!(load.would_refuse(OpClass::Any), None);
}

/// A refusal at the door is the same `503` the semaphore already answers, not a `500` and
/// not the `408` the timeout used to produce. Nothing about the request is wrong.
#[test]
fn an_admission_refusal_is_retryable_rather_than_a_fault() {
    let err = OrchestratorError::Overloaded {
        predicted_wait_ms: 1200,
        budget_ms: 1000,
    };
    assert!(matches!(err.verdict(), RemoteVerdict::Unavailable));
    assert!(
        err.to_string().contains("1200ms backlog"),
        "the refusal should say what it predicted: {err}"
    );
}

/// No budget is no deadline to miss, so there is nothing to predict against and the gate
/// stays out of the way — the same shape as the dequeue check without a budget.
#[test]
fn without_a_budget_nothing_is_refused_at_admission() {
    let (load, dispatch) = queue_load_with(&[500, 500], &[2, 2], 4, None);
    dispatch.record_service(OpClass::Read, Duration::from_secs(30));
    assert_eq!(load.would_refuse(OpClass::Any), None);
    assert_eq!(load.budget(), None);
}

/// The gate measures the request's *remaining* budget, not the configured one: time the
/// request already spent being received, parsed and routed is budget it no longer has.
/// A backlog that fits a fresh 1s budget is refused by a request that arrived 800ms ago —
/// the case deadline propagation exists for.
#[tokio::test]
async fn spent_budget_counts_against_the_request_not_just_the_queue_wait() {
    let budget = Duration::from_secs(1);
    // One round of 400ms work ahead: predicted 400ms + 500ms reserve (2x400ms capped at
    // half the remaining budget) — admitted against a full second, refused against the
    // ~200ms a nearly-spent request actually has.
    let (load, dispatch) = queue_load_with(&[2, 2], &[2, 2], 4, Some(budget));
    dispatch.record_service(OpClass::Read, Duration::from_millis(400));

    assert_eq!(
        load.would_refuse(OpClass::Any),
        None,
        "outside a request scope the whole budget remains, and this backlog fits"
    );

    let refused = REQUEST_STARTED_AT
        .scope(Instant::now() - Duration::from_millis(800), async {
            load.would_refuse(OpClass::Any)
        })
        .await;
    assert_eq!(
        refused,
        Some(Duration::from_millis(400)),
        "200ms left of a 1s budget cannot cover a 400ms backlog plus the reserve"
    );
}

/// A class with no samples of its own reserves against the blend, not zero: a node that
/// has only ever served searches still knows what *a* job costs when the first write
/// arrives.
#[test]
fn a_class_with_no_samples_reserves_against_the_blend() {
    let budget = Duration::from_secs(10);
    let stats = DispatchCounters::default();
    stats.record_service(OpClass::Read, Duration::from_millis(100));

    assert_eq!(
        stats.service_reserve_for(OpClass::Write, budget),
        Duration::from_millis(200),
        "no write samples yet, so the write reserve reads the blended estimate"
    );
    // Once writes have their own history, theirs is the one used.
    stats.record_service(OpClass::Write, Duration::from_millis(900));
    let reserve = stats.service_reserve_for(OpClass::Write, budget);
    assert!(
        reserve > Duration::from_millis(200),
        "a write estimate exists now; the reserve must track it, got {reserve:?}"
    );
}

/// A runner that holds each operation for `hold` and records the high-water mark of how
/// many were running at once. That mark is the whole subject of per-worker concurrency
/// and is not observable from outside the loop any other way.
fn recording_runner(
    hold: Duration,
    live: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
) -> impl Fn(Box<ClientOp>, Option<Uuid>) -> TestOp + Clone {
    move |_op, _shard| {
        let live = Arc::clone(&live);
        let peak = Arc::clone(&peak);
        Box::pin(async move {
            let now = live.fetch_add(1, AtomicOrdering::Relaxed) + 1;
            peak.fetch_max(now, AtomicOrdering::Relaxed);
            tokio::time::sleep(hold).await;
            live.fetch_sub(1, AtomicOrdering::Relaxed);
            WorkerOutcome::Done(Ok(json!({"ok": true})))
        })
    }
}

/// A runner whose operation never returns, for the two properties that are only observable
/// when one does not.
fn parking_runner() -> impl Fn(Box<ClientOp>, Option<Uuid>) -> TestOp + Clone {
    move |_op, _shard| Box::pin(async move { std::future::pending::<WorkerOutcome>().await })
}

async fn submit_aged(
    tx: &mpsc::Sender<OrchestratorJob>,
    age: Duration,
) -> tokio::sync::oneshot::Receiver<WorkerOutcome> {
    let (reply, answer) = tokio::sync::oneshot::channel();
    tx.send(OrchestratorJob::Execute {
        arrived_at: Instant::now() - age,
        op: placeholder_op(),
        affinity_shard: None,
        reply,
    })
    .await
    .expect("the worker channel is open");
    answer
}

/// An operation that never returns costs one slot, not the pool.
///
/// This is [OB14]'s shape reduced to its mechanism. There, four writer threads deadlocked and
/// every request behind them parked; each one kept its `in_flight` count, its `outstanding`
/// count and its semaphore permit, because all three were released by statements at the tail of
/// a task that never reached its tail. At the width the pool was gone — searches, metadata reads
/// and the node's own health probe with it.
///
/// Two guarantees are asserted together because either alone is worthless. The gauges must come
/// back, so the admission gate is predicting against a number that means something; and the
/// worker must serve the next job, so the pool is genuinely usable rather than merely
/// well-reported.
///
/// [OB14]: the shard-writer deadlock found by the first M6 arm, 2026-09-25.
#[tokio::test]
async fn an_operation_that_never_returns_costs_one_slot_not_the_pool() {
    let (tx, rx) = mpsc::channel::<OrchestratorJob>(8);
    let stats = Arc::new(DispatchCounters::default());
    let counters = Arc::new(WorkerCounters::default());
    // Small, so twenty budgets is a wall-clock fraction of a second.
    let budget = Duration::from_millis(20);

    tokio::spawn(orchestrator_worker_loop(
        rx,
        parking_runner(),
        0,
        Some(Arc::clone(&counters)),
        2, // width 2, so two parked jobs would have been the whole pool
        Some(budget),
        Some(Arc::clone(&stats)),
    ));

    // Fill the width with operations that will never return.
    stats.outstanding.fetch_add(2, AtomicOrdering::Relaxed);
    let first = submit_aged(&tx, Duration::ZERO).await;
    let second = submit_aged(&tx, Duration::ZERO).await;
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(
        counters.in_flight.load(AtomicOrdering::Relaxed),
        2,
        "both jobs should be running"
    );

    // Past the liveness cap — twenty budgets, so a second here against 50ms — the slots are
    // reclaimed and both callers are answered rather than left hanging.
    tokio::time::sleep(budget * WORKER_LIVENESS_MULTIPLE + Duration::from_millis(50)).await;

    // Awaited against a deadline, not bare: without the cap these never resolve, and a test
    // that hangs reports nothing. The regression has to be a failure, not a stall.
    for answer in [first, second] {
        match tokio::time::timeout(Duration::from_millis(500), answer).await {
            Ok(Ok(WorkerOutcome::Done(Err(_)))) => {}
            Ok(Ok(_)) => panic!("a job past the liveness cap must answer with an error"),
            Ok(Err(_)) => panic!("a job past the liveness cap dropped its reply channel"),
            Err(_) => panic!(
                "a parked job was never answered — its slot is still held and the pool is short one"
            ),
        }
    }
    assert_eq!(
        counters.in_flight.load(AtomicOrdering::Relaxed),
        0,
        "in_flight must return to zero — this is the gauge OB14 left permanently inflated"
    );
    assert_eq!(
        stats.outstanding.load(AtomicOrdering::Relaxed),
        0,
        "outstanding is what the admission gate predicts against, so it must return too"
    );
    assert_eq!(
        stats.jobs_dropped.load(AtomicOrdering::Relaxed),
        2,
        "a job that left without answering has to be counted, or the next leak is silent too"
    );
    assert_eq!(
        counters.jobs_completed.load(AtomicOrdering::Relaxed),
        0,
        "nothing answered, so nothing may be tallied as completed"
    );

    // The pool is usable, which is the claim that matters.
    stats.outstanding.fetch_add(1, AtomicOrdering::Relaxed);
    let (reply, answer) = tokio::sync::oneshot::channel();
    tx.send(OrchestratorJob::Execute {
        arrived_at: Instant::now(),
        op: placeholder_op(),
        affinity_shard: None,
        reply,
    })
    .await
    .expect("the worker channel is open");
    drop(answer);
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(
        counters.in_flight.load(AtomicOrdering::Relaxed),
        1,
        "the worker has to accept new work after reclaiming the parked slots"
    );
}

/// A stale job is shed even when every permit is held.
///
/// The loop used to acquire a permit and *then* receive, so a pool whose width was held could
/// not reach the deadline comparison at all — the guard that sheds stale work was unreachable in
/// exactly the state it exists for. OB14 measured the consequence: `abandoned` stayed at 0
/// through arms where every job waited seconds against a one-second budget, while the queue grew
/// without bound behind permits nothing would return.
///
/// Receiving first and checking before the wait also means a job that is already dead never
/// occupies a slot a live job could have used.
#[tokio::test]
async fn a_stale_job_is_shed_even_when_every_permit_is_held() {
    let (tx, rx) = mpsc::channel::<OrchestratorJob>(8);
    let stats = Arc::new(DispatchCounters::default());
    // Deliberately long enough that the liveness cap — twenty budgets, so four seconds here —
    // cannot fire inside this test. Otherwise the cap would release the parked job's permit and
    // the shed would happen for the wrong reason, which is exactly what an earlier draft of this
    // test measured: it passed against the old ordering because the cap masked it.
    let budget = Duration::from_millis(200);

    tokio::spawn(orchestrator_worker_loop(
        rx,
        parking_runner(),
        0,
        None,
        1, // width 1: one parked job is the whole pool
        Some(budget),
        Some(Arc::clone(&stats)),
    ));

    // Take the only permit with an operation that will not give it back.
    stats.outstanding.fetch_add(1, AtomicOrdering::Relaxed);
    let _parked = submit_aged(&tx, Duration::ZERO).await;
    tokio::time::sleep(Duration::from_millis(5)).await;

    // A job that arrived long before its budget allowed. With the width held, the old order
    // never looked at it.
    stats.outstanding.fetch_add(1, AtomicOrdering::Relaxed);
    let stale = submit_aged(&tx, Duration::from_secs(5)).await;

    match tokio::time::timeout(Duration::from_millis(500), stale).await {
        Ok(Ok(WorkerOutcome::Done(Err(OrchestratorError::ReadDeadlineExpired {
            waited_ms,
            budget_ms,
        })))) => {
            assert!(waited_ms >= 5_000, "got {waited_ms}ms");
            assert_eq!(budget_ms, 200);
        }
        Ok(Ok(_)) => panic!("expected the stale job to be shed, got an answer"),
        Ok(Err(_)) => panic!("the stale job's reply channel was dropped"),
        Err(_) => panic!("the stale job was never answered, so it was never shed"),
    }
    assert_eq!(
        stats.abandoned.load(AtomicOrdering::Relaxed),
        1,
        "the shed has to be counted; a silent one reads as an idle node"
    );
}

async fn submit(
    tx: &mpsc::Sender<OrchestratorJob>,
) -> tokio::sync::oneshot::Receiver<WorkerOutcome> {
    let (reply, answer) = tokio::sync::oneshot::channel();
    tx.send(OrchestratorJob::Execute {
        arrived_at: Instant::now(),
        op: placeholder_op(),
        affinity_shard: None,
        reply,
    })
    .await
    .expect("the worker channel is open");
    answer
}

/// The point of the whole change: a worker carries several operations at once. The loop
/// used to await `execute` inline, which pinned this peak at 1 however many jobs were
/// queued — and that, not thread placement, is what made shard-affine dispatch a
/// measured 13-20% write regression, because enabling it halves `worker_count`.
#[tokio::test]
async fn a_worker_runs_several_operations_at_once() {
    let (tx, rx) = mpsc::channel(16);
    let live = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    tokio::spawn(orchestrator_worker_loop(
        rx,
        recording_runner(
            Duration::from_millis(50),
            Arc::clone(&live),
            Arc::clone(&peak),
        ),
        0,
        None,
        4,
        None,
        None,
    ));

    let mut answers = Vec::new();
    for _ in 0..4 {
        answers.push(submit(&tx).await);
    }
    for answer in answers {
        answer.await.expect("every operation is answered");
    }

    assert_eq!(
        peak.load(AtomicOrdering::Relaxed),
        4,
        "four jobs and a width of four should have overlapped; a peak of 1 means the \
             loop went back to awaiting each operation inline"
    );
}

/// The other half of the contract. Width is an admission limit, not a suggestion: past
/// the point where every shard writer already has work queued, more in-flight operations
/// only move the queue from the channel into memory.
#[tokio::test]
async fn a_worker_never_exceeds_its_width() {
    let (tx, rx) = mpsc::channel(16);
    let live = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    tokio::spawn(orchestrator_worker_loop(
        rx,
        recording_runner(
            Duration::from_millis(20),
            Arc::clone(&live),
            Arc::clone(&peak),
        ),
        0,
        None,
        2,
        None,
        None,
    ));

    let mut answers = Vec::new();
    for _ in 0..8 {
        answers.push(submit(&tx).await);
    }
    for answer in answers {
        answer.await.expect("every operation is answered");
    }

    assert_eq!(
        peak.load(AtomicOrdering::Relaxed),
        2,
        "a width of two must never run three at once"
    );
}

/// Shutdown must answer what it already accepted.
///
/// This mirrors the pinned path deliberately: there the loop is the argument to
/// `block_on` on the worker's own `current_thread` runtime, so *returning* from it drops
/// that runtime and cancels every task still on it. Operations run as spawned tasks now,
/// so without the drain a shutdown mid-flight abandons accepted writes and hands their
/// callers a dropped channel instead of an answer. A plain `#[tokio::test]` would not
/// catch it — the test runtime outlives the loop and the tasks would finish anyway.
#[test]
fn shutdown_answers_operations_it_already_accepted() {
    let (tx, rx) = mpsc::channel::<OrchestratorJob>(16);

    // The first operation is quick and the rest are slow, so the loop is guaranteed to
    // read `Shutdown` — which needs a freed permit — while three are still running. With
    // one uniform duration the whole batch finishes together and the test proves nothing.
    let seq = Arc::new(AtomicUsize::new(0));
    let runner = move |_op: Box<ClientOp>, _shard: Option<Uuid>| -> TestOp {
        let nth = seq.fetch_add(1, AtomicOrdering::Relaxed);
        Box::pin(async move {
            let hold = if nth == 0 { 10 } else { 200 };
            tokio::time::sleep(Duration::from_millis(hold)).await;
            WorkerOutcome::Done(Ok(json!({"nth": nth})))
        })
    };

    let worker = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("worker runtime");
        rt.block_on(orchestrator_worker_loop(rx, runner, 0, None, 4, None, None));
    });

    let mut answers = Vec::new();
    for _ in 0..4 {
        let (reply, answer) = tokio::sync::oneshot::channel();
        tx.blocking_send(OrchestratorJob::Execute {
            arrived_at: Instant::now(),
            op: placeholder_op(),
            affinity_shard: None,
            reply,
        })
        .expect("the worker channel is open");
        answers.push(answer);
    }
    tx.blocking_send(OrchestratorJob::Shutdown)
        .expect("the worker channel is open");
    drop(tx);

    worker.join().expect("the worker thread exits cleanly");

    for (nth, answer) in answers.into_iter().enumerate() {
        assert!(
            answer.blocking_recv().is_ok(),
            "operation {nth} was accepted and then abandoned at shutdown"
        );
    }
}

/// The defect that made shard-affine dispatch worth avoiding: `xxh3(shard) % workers`
/// draws from a domain smaller than the pool, so most workers never see a write. With
/// the shipped defaults — 4 shards, 8 workers — a measured run reached 3 of 8. Ordinals
/// reach every worker up to the shard count, which is the real ceiling: one writer
/// thread per shard serialises that shard's writes regardless.
#[test]
fn every_shard_gets_its_own_worker_until_the_pool_runs_out() {
    let mut placement = ShardPlacement::default();
    let layout = CoreLayout::detect();
    let shards: Vec<Uuid> = (0..4).map(|_| Uuid::new_v4()).collect();
    for &shard in &shards {
        placement.assign(shard, &layout, false);
    }

    let workers = 8;
    let assigned: HashSet<usize> = shards
        .iter()
        .map(|shard| placement.ordinal(shard).unwrap() % workers)
        .collect();

    assert_eq!(
        assigned.len(),
        shards.len(),
        "four shards must reach four distinct workers, not collide onto fewer"
    );
}

/// More shards than workers is the normal steady state; they have to wrap evenly rather
/// than pile onto one worker.
#[test]
fn shards_past_the_worker_count_wrap_evenly() {
    let mut placement = ShardPlacement::default();
    let layout = CoreLayout::detect();
    let shards: Vec<Uuid> = (0..16).map(|_| Uuid::new_v4()).collect();
    for &shard in &shards {
        placement.assign(shard, &layout, false);
    }

    let workers = 8;
    let mut per_worker = vec![0usize; workers];
    for shard in &shards {
        per_worker[placement.ordinal(shard).unwrap() % workers] += 1;
    }

    assert!(
        per_worker.iter().all(|count| *count == 2),
        "16 shards over 8 workers should be 2 each, got {per_worker:?}"
    );
}

/// A writer thread pins itself using the ordinal it was given at startup and cannot be
/// re-pinned afterwards. Assigning ordinals from a set that gets re-sorted on every
/// membership change would strand those threads, so an ordinal is fixed for the process.
#[test]
fn an_ordinal_survives_later_shards_arriving() {
    let mut placement = ShardPlacement::default();
    let layout = CoreLayout::detect();
    let first = Uuid::new_v4();
    let ordinal = placement.assign(first, &layout, false).ordinal;

    for _ in 0..5 {
        placement.assign(Uuid::new_v4(), &layout, false);
    }
    // A shard that starts twice — a restarted actor, a repeated registration — keeps the
    // core its writer already pinned to.
    assert_eq!(placement.assign(first, &layout, false).ordinal, ordinal);
    assert_eq!(placement.ordinal(&first), Some(ordinal));
}

/// Local routing reads this to skip the coordinator, so a shard that is not ours must
/// never look like one that is — including a shard that got an ordinal on its way to
/// starting and then failed to hydrate. Routing writes at it would send them to a shard
/// this node cannot serve, where the coordinator would have found the real owner.
#[test]
fn placement_claims_only_shards_that_actually_started() {
    let mut placement = ShardPlacement::default();
    let layout = CoreLayout::detect();
    let serving = Uuid::new_v4();
    let failed_to_hydrate = Uuid::new_v4();

    placement.assign(serving, &layout, false);
    placement.activate(serving);
    placement.assign(failed_to_hydrate, &layout, false);

    assert!(placement.is_local(&serving));
    assert!(
        !placement.is_local(&failed_to_hydrate),
        "an ordinal is not a claim; only a started shard is"
    );
    assert!(!placement.is_local(&Uuid::new_v4()));
    assert!(
        placement.ordinal(&failed_to_hydrate).is_some(),
        "the ordinal is still spent — reusing it would move a live writer's core"
    );
}

/// Asking for a core is not the same as getting one — `set_for_current` is a no-op on
/// macOS and can be refused by a cpuset on Linux. The report used to show the request as
/// though it were the result, which made it useless as evidence for exactly the thing it
/// exists to show.
#[test]
fn a_requested_core_is_not_reported_as_a_taken_one() {
    let mut placement = ShardPlacement::default();
    let layout = CoreLayout::detect();
    let shard = Uuid::new_v4();

    let slot = placement.assign(shard, &layout, true);
    placement.activate(shard);

    let before = &placement.report()[0];
    assert_eq!(
        before.core_id, None,
        "nothing has pinned yet, so no core is taken"
    );
    if layout.pinning_available() {
        assert!(
            before.target_core_id.is_some(),
            "but one was requested, and that has to be visible too"
        );
    }

    // Stand in for the writer thread reporting success.
    slot.pinned_core.store(3, AtomicOrdering::Relaxed);

    let after = &placement.report()[0];
    assert_eq!(after.core_id, Some(3));
    assert!(after.serving);
}

/// A shard that never started still holds its ordinal, and the report has to say so
/// rather than quietly omitting it — an unexplained gap in the ordinals is exactly the
/// kind of thing an operator needs to see.
#[test]
fn the_report_shows_a_shard_that_holds_an_ordinal_without_serving() {
    let mut placement = ShardPlacement::default();
    let layout = CoreLayout::detect();
    let serving = Uuid::new_v4();
    let stalled = Uuid::new_v4();

    placement.assign(serving, &layout, true);
    placement.activate(serving);
    placement.assign(stalled, &layout, true);

    let report = placement.report();
    assert_eq!(report.len(), 2, "both shards appear");
    assert_eq!(report[0].ordinal, 0, "report is ordered by ordinal");
    assert_eq!(report[1].ordinal, 1);
    assert!(report[0].serving);
    assert!(!report[1].serving);
}

/// Pinning off means no core was requested, so nothing should imply one was.
#[test]
fn no_core_is_requested_when_pinning_is_off() {
    let mut placement = ShardPlacement::default();
    let shard = Uuid::new_v4();
    placement.assign(shard, &CoreLayout::detect(), false);

    let report = placement.report();
    assert_eq!(report[0].target_core_id, None);
    assert_eq!(report[0].core_id, None);
}

/// A quota-limited process sees more cores than it may use. Placing threads across cores
/// the scheduler will not schedule spreads the work without spreading the CPU, and it is
/// how worker and writer placement stopped agreeing in the first place.
#[test]
fn the_core_layout_never_exceeds_the_cpu_budget() {
    let layout = CoreLayout::detect();

    assert!(layout.budget() >= 1);
    assert!(
        layout.cores.len() <= layout.budget(),
        "pinnable cores ({}) must not exceed the budget ({})",
        layout.cores.len(),
        layout.budget()
    );
    if layout.pinning_available() {
        // Ordinals past the end wrap rather than falling off it.
        assert!(layout.core_for(layout.cores.len() * 3 + 1).is_some());
    } else {
        assert!(layout.core_for(0).is_none());
    }
}

fn payload(id: &str, doc: JsonValue) -> DocPayload {
    DocPayload {
        id: id.to_string(),
        routing_key: None,
        doc,
    }
}

/// A tantivy schema is fixed when the index is created, so initial creation is the only
/// chance to make a field searchable. Sampling used to leave everything non-indexed,
/// which produced write-only indexes: documents went in, and nothing but `id` could
/// find them again.
#[test]
fn fields_inferred_when_the_index_is_created_are_searchable() {
    let schema = enhanced_schema_sampling(
        &[payload(
            "d1",
            json!({"id": "d1", "title": "hello", "year": 2024}),
        )],
        SCHEMA_SAMPLE_LIMIT,
    );

    for name in ["title", "year"] {
        let field = schema
            .fields
            .get(name)
            .unwrap_or_else(|| panic!("{name} should have been inferred"));
        assert!(field.indexed, "{name} has to be searchable");
        assert!(!field.stored, "only id belongs in tantivy's stored fields");
    }
}

/// Hits are rebuilt from redb, so storing values in tantivy too would keep a second copy
/// of the corpus. Nothing inferred is stored — and `id` is not inferred at all:
/// `evolve_field` refuses to touch it, and the storage layer seeds the canonical
/// definition when it creates the index.
#[test]
fn nothing_inferred_is_stored_in_tantivy() {
    let schema = enhanced_schema_sampling(
        &[payload("d1", json!({"id": "d1", "title": "hello"}))],
        SCHEMA_SAMPLE_LIMIT,
    );

    assert!(
        !schema.fields.contains_key("id"),
        "id is seeded by the storage layer, not inferred"
    );
    assert!(
        schema.fields.values().all(|field| !field.stored),
        "an inferred field is indexed, never stored"
    );
}

/// Shadow fields exist to map a query written against the original field name onto the
/// canonical `id`. Indexing one would put a second copy of the ids in the index.
#[test]
fn a_shadow_field_survives_initial_creation_untouched() {
    let mut schema = IndexSchema::default();
    schema.add_shadow_field("sha1".to_string(), TantivyFieldType::Text);
    schema.fields.insert(
        "title".to_string(),
        FieldDef::new_non_indexed("title".to_string(), &json!("hello")),
    );

    mark_initial_fields_indexed(&mut schema);

    let shadow = &schema.fields["sha1"];
    assert!(!shadow.indexed, "a shadow field is never indexed");
    assert!(!shadow.stored, "a shadow field is never stored");
    assert!(schema.fields["title"].indexed, "ordinary fields still are");
}

/// The identifier travels under the shadow name on the way out, so a projection naming `id`
/// has to be rewritten to that name before it is applied. The rewrite is the only crossing
/// point between the two names on read, and it has to hold for both directions of the
/// mapping: `id` becomes the shadow name, the shadow name is left alone, and unrelated
/// fields pass through untouched. On a plain index the rewrite is identity, so the helper
/// is a no-op there.
#[test]
fn normalize_projection_fields_rewrites_id_to_the_shadow_name() {
    let mut shadow = IndexSchema::default();
    shadow.add_shadow_field("sha1".to_string(), TantivyFieldType::String);
    shadow.fields.insert(
        "title".to_string(),
        FieldDef::new("title".to_string(), TantivyFieldType::String),
    );

    assert_eq!(
        normalize_projection_fields(&shadow, &["id".to_string(), "title".to_string()]),
        vec!["sha1".to_string(), "title".to_string()],
        "`id` is rewritten to the shadow name; other fields are left alone"
    );
    assert_eq!(
        normalize_projection_fields(&shadow, &["sha1".to_string()]),
        vec!["sha1".to_string()],
        "the shadow name is already the name hits carry, so it is a no-op"
    );
    assert_eq!(
        normalize_projection_fields(&shadow, &[]),
        Vec::<String>::new(),
        "an empty projection stays empty"
    );

    let plain = IndexSchema::default();
    assert_eq!(
        normalize_projection_fields(&plain, &["id".to_string(), "title".to_string()]),
        vec!["id".to_string(), "title".to_string()],
        "on a plain index the key is `id`, so the rewrite is identity"
    );
}

/// A metadata op the pool has no answer for must be handed to the actor rather than
/// answered with an error — the same contract. `GetIdentity` and the index listing are
/// metadata the pool *does* answer; the rest still defer.
#[tokio::test]
async fn a_metadata_op_defers_rather_than_failing() {
    let engine = bare_engine();

    let outcome = engine
        .execute(ClientOp::GetConfig {
            index: "books".to_string(),
        })
        .await;

    assert!(
        matches!(outcome, WorkerOutcome::UseActor(op) if matches!(*op, ClientOp::GetConfig { .. })),
        "a metadata op must be deferred to the actor, carrying its own op"
    );
}

/// The cluster-wide schema lookup is answered by a worker, never handed to the actor.
///
/// It canvasses peers, and each peer answers through its own orchestrator mailbox. On this
/// node's mailbox the canvass held it while waiting, so a peer canvassing this node at the same
/// moment waited too, until both timed out: index deletes answered 503 whenever new indexes
/// were being minted elsewhere. A standalone engine holding nothing answers `null` — the index
/// does not exist — without asking anyone.
#[tokio::test]
async fn the_cluster_schema_lookup_is_answered_by_the_worker() {
    let engine = bare_engine();

    let outcome = engine
        .execute(ClientOp::FindSchemaInCluster {
            index: "books".to_string(),
        })
        .await;

    match outcome {
        WorkerOutcome::Done(Ok(answer)) => assert!(answer.is_null(), "got {answer}"),
        WorkerOutcome::Done(Err(err)) => panic!("expected null, got {err:?}"),
        WorkerOutcome::UseActor(_) => {
            panic!("the cluster schema lookup must not be deferred to the actor")
        }
    }
}

/// The index listing is a metadata read — shard stats asked on a clone, one schema per
/// index, an identity that never changes — so a worker answers it from snapshots and
/// never hands it back. The day it defers, "what indexes exist" queues behind a held
/// mailbox again, which is what moved it off the actor (ROADMAP CH12).
#[tokio::test]
async fn the_index_listing_is_answered_by_the_worker() {
    let engine = bare_engine();

    for op in [
        ClientOp::ListIndexes {
            include_data_size: false,
        },
        ClientOp::ListIndexes {
            include_data_size: true,
        },
        ClientOp::ListClusterIndexes {
            include_data_size: false,
        },
    ] {
        match engine.execute(op).await {
            WorkerOutcome::Done(Ok(json)) => {
                assert_eq!(json["total_indexes"], 0);
                assert_eq!(json["total_shards"], 0);
            }
            WorkerOutcome::UseActor(_) => {
                panic!("the index listing must not be deferred to the actor")
            }
            WorkerOutcome::Done(Err(other)) => panic!("expected a listing, got {other:?}"),
        }
    }
}

/// A node is standalone unless its configuration says otherwise.
///
/// `peer_schema_for` takes its no-peers arm off this flag before asking the coordinator
/// anything, so the default decides what a node with no cluster configuration does on the
/// first write to a new index: sample a schema from the documents, in-process, rather than
/// wait on a canvass of peers that cannot exist.
#[test]
fn a_node_is_standalone_until_configured_otherwise() {
    assert!(
        !NodeConfig::default().clustered,
        "the default configuration must not claim to be part of a cluster"
    );
}

/// An ordinary forward carries no schema information at all, and the body only on request.
///
/// Three cuts of this. The first sent the whole schema on every forward — 602 bytes for
/// three fields against a 49-byte document. The second sent a 61-byte stamp. Both were
/// answering "does the receiver have a schema", and neither needed to: a forwarded write is
/// a share of a decision another node already made, so the receiver can simply *ask* when
/// it has nothing to run the share against. `ClientOp::Write` already carried the bit that
/// says so, so a single write costs nothing extra; `BulkWrite` gained it.
#[test]
fn an_ordinary_forward_carries_no_schema_at_all() {
    let mut settled = IndexSchema::default();
    for i in 0..3 {
        settled.fields.insert(
            format!("f{i}"),
            FieldDef::new(format!("f{i}"), TantivyFieldType::Text),
        );
    }

    let forward = ClientOp::BulkWrite {
        index: "books".to_string(),
        docs: vec![DocPayload {
            id: "b1".to_string(),
            routing_key: None,
            doc: json!({"f0": "Dune"}),
        }],
        forwarded: true,
        schema_body: None,
        tenant: None,
    };
    let wire = serde_json::to_string(&forward).unwrap();
    assert!(
        !wire.contains("field_type") && !wire.contains("thumbprint"),
        "no schema and no stamp may appear on an ordinary forward: {wire}"
    );

    // The body exists, and is what a resend attaches — never a first attempt.
    let body = schema_to_carry(&settled).expect("a schema with fields can be sent");
    assert!(body.fields.contains_key("f0"));
    assert!(
        schema_to_carry(&IndexSchema::default()).is_none(),
        "an empty schema must not travel: the receiver would adopt it and index nothing"
    );

    // What the resend costs, against what it replaces on every forward.
    let resent = with_schema_body(&forward, &settled).expect("a bulk write can carry one");
    let resent_len = serde_json::to_string(&resent).unwrap().len();
    assert!(
        wire.len() * 3 < resent_len,
        "the point of asking is that the body is large: forward {} vs resend {}",
        wire.len(),
        resent_len
    );
}

/// A peer that cannot run a share asks for the body, and the resend attaches it.
#[test]
fn a_resend_attaches_the_body_the_peer_asked_for() {
    let mut settled = IndexSchema::default();
    settled.fields.insert(
        "title".to_string(),
        FieldDef::new("title".to_string(), TantivyFieldType::Text),
    );

    let forwarded = ClientOp::Write {
        index: "books".to_string(),
        id: "b1".to_string(),
        routing_key: None,
        doc: json!({"title": "Dune"}),
        forwarded: true,
        schema_body: None,
        tenant: None,
    };

    match with_schema_body(&forwarded, &settled).expect("a write can carry a schema") {
        ClientOp::Write {
            schema_body: Some(body),
            id,
            forwarded: true,
            ..
        } => {
            assert_eq!(id, "b1", "the write itself has to be the same write");
            assert!(body.fields.contains_key("title"));
        }
        other => panic!("expected a body on the resend, got {other:?}"),
    }

    // The verdict is what the forwarding node matches on, rather than the message text —
    // reading the text is what the classification work removed.
    let asked = OrchestratorError::SchemaBodyRequired {
        index: "books".to_string(),
    };
    assert!(matches!(asked.verdict(), RemoteVerdict::SchemaRequired));
    assert_eq!(
        RemoteVerdict::from_tag(RemoteVerdict::SchemaRequired.tag()),
        Some(RemoteVerdict::SchemaRequired),
        "the verdict has to survive the wire, or the retry never happens"
    );

    // Nothing without a document can be asked for a schema.
    assert!(
        with_schema_body(
            &ClientOp::Delete {
                index: "books".to_string(),
                id: "b1".to_string(),
                routing_key: None,
                forwarded: true,
            },
            &settled
        )
        .is_none()
    );
}

#[test]
fn test_apply_field_projection_single_field() {
    let doc = json!({
        "title": "Rust Programming",
        "author": "John Doe",
        "year": 2024,
        "_score": 0.95,
        "_id": "abc123"
    });

    let fields = vec!["title".to_string()];
    let result = apply_field_projection(doc, &fields);

    // Should only have title and metadata fields (those starting with _)
    assert_eq!(result.get("title").unwrap(), "Rust Programming");
    assert_eq!(result.get("_score").unwrap(), 0.95);
    assert_eq!(result.get("_id").unwrap(), "abc123");
    assert!(result.get("author").is_none());
    assert!(result.get("year").is_none());
}

#[test]
fn test_apply_field_projection_multiple_fields() {
    let doc = json!({
        "title": "Rust Programming",
        "author": "John Doe",
        "year": 2024,
        "isbn": "123-456",
        "_score": 0.95
    });

    let fields = vec!["title".to_string(), "author".to_string()];
    let result = apply_field_projection(doc, &fields);

    assert_eq!(result.get("title").unwrap(), "Rust Programming");
    assert_eq!(result.get("author").unwrap(), "John Doe");
    assert_eq!(result.get("_score").unwrap(), 0.95);
    assert!(result.get("year").is_none());
    assert!(result.get("isbn").is_none());
}

#[test]
fn test_apply_field_projection_preserves_all_metadata() {
    let doc = json!({
        "title": "Rust Programming",
        "author": "John Doe",
        "_score": 0.95,
        "_id": "doc123",
        "_timestamp": 1234567890,
        "_shard_id": "abc123"
    });

    let fields = vec!["title".to_string()];
    let result = apply_field_projection(doc, &fields);

    // All metadata fields (starting with _) should be preserved
    assert_eq!(result.get("title").unwrap(), "Rust Programming");
    assert_eq!(result.get("_score").unwrap(), 0.95);
    assert_eq!(result.get("_id").unwrap(), "doc123");
    assert_eq!(result.get("_timestamp").unwrap(), 1234567890);
    assert_eq!(result.get("_shard_id").unwrap(), "abc123");
    assert!(result.get("author").is_none());
}

#[test]
fn test_apply_field_projection_nonexistent_field() {
    let doc = json!({
        "title": "Rust Programming",
        "author": "John Doe",
        "_score": 0.95
    });

    let fields = vec!["title".to_string(), "nonexistent".to_string()];
    let result = apply_field_projection(doc, &fields);

    // Should have title and metadata, but not nonexistent field
    assert_eq!(result.get("title").unwrap(), "Rust Programming");
    assert_eq!(result.get("_score").unwrap(), 0.95);
    assert!(result.get("nonexistent").is_none());
    assert!(result.get("author").is_none());
}

#[test]
fn test_apply_field_projection_empty_fields() {
    let doc = json!({
        "title": "Rust Programming",
        "author": "John Doe",
        "_score": 0.95
    });

    let fields: Vec<String> = vec![];
    let result = apply_field_projection(doc, &fields);

    // Should only have metadata fields
    assert_eq!(result.get("_score").unwrap(), 0.95);
    assert!(result.get("title").is_none());
    assert!(result.get("author").is_none());
}

#[test]
fn test_apply_field_projection_non_object() {
    let doc = json!("not an object");
    let fields = vec!["title".to_string()];
    let result = apply_field_projection(doc, &fields);

    // Should return the original value unchanged
    assert_eq!(result, json!("not an object"));
}

#[test]
fn test_apply_field_projection_nested_fields() {
    let doc = json!({
        "title": "Rust Programming",
        "author": {
            "name": "John Doe",
            "email": "john@example.com"
        },
        "_score": 0.95
    });

    let fields = vec!["author".to_string()];
    let result = apply_field_projection(doc, &fields);

    // Should preserve the entire nested object
    assert_eq!(
        result.get("author").unwrap().get("name").unwrap(),
        "John Doe"
    );
    assert_eq!(
        result.get("author").unwrap().get("email").unwrap(),
        "john@example.com"
    );
    assert_eq!(result.get("_score").unwrap(), 0.95);
    assert!(result.get("title").is_none());
}

#[test]
fn test_apply_field_projection_preserves_user_order() {
    let doc = json!({
        "id": "doc1",
        "title": "Rust Programming",
        "author": "John Doe",
        "year": 2024,
        "_score": 0.95
    });

    let fields = vec![
        "year".to_string(),
        "title".to_string(),
        "author".to_string(),
    ];
    let result = apply_field_projection(doc, &fields);

    // User fields should appear first, in projection order
    let keys: Vec<&str> = result
        .as_object()
        .unwrap()
        .keys()
        .map(|k| k.as_str())
        .collect();
    assert_eq!(keys, vec!["year", "title", "author", "_score"]);
}

#[test]
fn test_apply_field_projection_order_with_sort_key() {
    // Simulate a document after stamp_sort_keys + _score insertion
    let doc = json!({
        "id": "doc1",
        "title": "Rust Programming",
        "author": "John Doe",
        "year": 2024,
        "_sort_key": 2024,
        "_score": 1.0,
        "shard_id": "abc-123"
    });

    let fields = vec![
        "year".to_string(),
        "title".to_string(),
        "author".to_string(),
    ];
    let mut result = apply_field_projection(doc, &fields);

    // Strip _sort_key as route_and_handle would
    if let Some(o) = result.as_object_mut() {
        o.remove(SORT_KEY_FIELD);
    }

    // After stripping, order should be: user fields (in projection order), then _score
    let keys: Vec<&str> = result
        .as_object()
        .unwrap()
        .keys()
        .map(|k| k.as_str())
        .collect();
    assert_eq!(keys, vec!["year", "title", "author", "_score"]);
}

#[test]
fn test_apply_field_projection_order_without_sort_key() {
    // Same document but without _sort_key (no sort applied)
    let doc = json!({
        "id": "doc1",
        "title": "Rust Programming",
        "author": "John Doe",
        "year": 2024,
        "_score": 1.0,
        "shard_id": "abc-123"
    });

    let fields = vec![
        "year".to_string(),
        "title".to_string(),
        "author".to_string(),
    ];
    let result = apply_field_projection(doc, &fields);

    // Order should be: user fields (in projection order), then _score
    let keys: Vec<&str> = result
        .as_object()
        .unwrap()
        .keys()
        .map(|k| k.as_str())
        .collect();
    assert_eq!(keys, vec!["year", "title", "author", "_score"]);
}

// ---- Field-sort merge helpers (`_sort_key`) ----

fn titles(hits: &[JsonValue]) -> Vec<String> {
    hits.iter()
        .map(|h| h["title"].as_str().unwrap_or_default().to_string())
        .collect()
}

/// Even when the sort field itself is projected away, the `_sort_key` metadata lets a
/// cross-node merge interleave per-node blocks correctly.
#[test]
fn a_merge_interleaves_nodes_by_sort_key_without_the_sort_field() {
    let spec = SortSpec {
        field: "year".to_string(),
        order: SortOrder::Desc,
    };
    // Two nodes, each already field-sorted and projected (no `year`).
    let hits = order_hit_blocks(
        vec![
            vec![
                json!({"title": "a", "_sort_key": 2020}),
                json!({"title": "c", "_sort_key": 2018}),
            ],
            vec![
                json!({"title": "d", "_sort_key": 2024}),
                json!({"title": "b", "_sort_key": 2022}),
            ],
        ],
        Some(&spec),
        SearchWindow::first(10),
    );
    assert_eq!(titles(&hits), vec!["d", "b", "a", "c"]);
}

#[test]
fn a_merge_sorts_ascending_and_puts_a_missing_key_last() {
    let spec = SortSpec {
        field: "year".to_string(),
        order: SortOrder::Asc,
    };
    let hits = order_hit_blocks(
        vec![
            vec![
                json!({"title": "b", "_sort_key": 2022}),
                json!({"title": "missing"}), // no `_sort_key` → sorts last
            ],
            vec![json!({"title": "a", "_sort_key": 2018})],
        ],
        Some(&spec),
        SearchWindow::first(10),
    );
    assert_eq!(titles(&hits), vec!["a", "b", "missing"]);
}

/// Ties fall back to the source's rank and then to the hit's place within that source.
///
/// Sources are polled concurrently and answer in whatever order they finish, so without
/// this a tie is settled by whoever replied first and one query has two answers. Every
/// hit here has the same sort key, which is the ordinary case rather than a contrived
/// one: every document matching a single term scores identically.
#[test]
fn a_tie_falls_back_to_source_rank_and_then_to_position() {
    let spec = SortSpec {
        field: "year".to_string(),
        order: SortOrder::Desc,
    };
    let blocks = vec![
        vec![
            json!({"title": "a1", "_sort_key": 2020}),
            json!({"title": "a2", "_sort_key": 2020}),
        ],
        vec![
            json!({"title": "b1", "_sort_key": 2020}),
            json!({"title": "b2", "_sort_key": 2020}),
        ],
    ];
    let hits = order_hit_blocks(blocks.clone(), Some(&spec), SearchWindow::first(10));
    assert_eq!(titles(&hits), vec!["a1", "a2", "b1", "b2"]);

    // The same blocks in the other dispatch order give the other answer, and that is the
    // point: the order is the caller's, not the network's.
    let swapped = vec![blocks[1].clone(), blocks[0].clone()];
    let hits = order_hit_blocks(swapped, Some(&spec), SearchWindow::first(10));
    assert_eq!(titles(&hits), vec!["b1", "b2", "a1", "a2"]);
}

/// A truncated page is the prefix of an untruncated one.
///
/// The property paging will rest on, and the one a running top-K could not provide: it had
/// to decide what to discard while later sources were still unheard, so which of a tied run
/// it kept depended on arrival order.
#[test]
fn a_limited_merge_is_a_prefix_of_the_unlimited_one() {
    let blocks = vec![
        vec![
            json!({"title": "a1", "_score": 1.0}),
            json!({"title": "a2", "_score": 1.0}),
        ],
        vec![
            json!({"title": "b1", "_score": 1.0}),
            json!({"title": "b2", "_score": 2.0}),
        ],
    ];
    let full = titles(&order_hit_blocks(
        blocks.clone(),
        None,
        SearchWindow::first(10),
    ));
    for limit in 1..=full.len() {
        let page = titles(&order_hit_blocks(
            blocks.clone(),
            None,
            SearchWindow::first(limit),
        ));
        assert_eq!(page, full[..limit], "limit {limit} is not a prefix");
    }
}

/// `SearchWindow::fetch_count` is `offset + limit` — the number each source must return
/// for the window to be servable after a merge.
#[test]
fn search_window_fetch_count_is_offset_plus_limit() {
    assert_eq!(SearchWindow::first(10).fetch_count(), 10);
    assert_eq!(
        SearchWindow {
            offset: 30,
            limit: 10
        }
        .fetch_count(),
        40
    );
    // Saturating: a huge offset does not overflow.
    assert_eq!(
        SearchWindow {
            offset: usize::MAX,
            limit: 1
        }
        .fetch_count(),
        usize::MAX
    );
}

/// `SearchWindow::apply` takes the slice `[offset, offset+limit)` from an ordered vec.
#[test]
fn search_window_apply_takes_the_right_slice() {
    let hits: Vec<usize> = (0..100).collect();
    let page = SearchWindow {
        offset: 30,
        limit: 10,
    }
    .apply(hits);
    assert_eq!(page, (30..40usize).collect::<Vec<_>>());
}

/// `apply` clamps to what is available rather than panicking.
#[test]
fn search_window_apply_clamps_to_available() {
    let hits: Vec<usize> = vec![1, 2, 3];
    assert_eq!(
        SearchWindow {
            offset: 0,
            limit: 10
        }
        .apply(hits.clone()),
        vec![1usize, 2, 3]
    );
    assert_eq!(
        SearchWindow {
            offset: 2,
            limit: 10
        }
        .apply(hits.clone()),
        vec![3usize]
    );
    assert_eq!(
        SearchWindow {
            offset: 5,
            limit: 10
        }
        .apply(hits),
        Vec::<usize>::new()
    );
}

/// A paged merge is the middle of an untruncated one, not a re-run of it.
///
/// `order_hit_blocks` with a window of `offset=2, limit=2` returns the third and fourth
/// hits of the full order, which is what a caller paging through results expects.
#[test]
fn a_paged_merge_returns_the_right_slice_of_the_full_order() {
    let blocks = vec![
        vec![
            json!({"title": "a1", "_score": 1.0}),
            json!({"title": "a2", "_score": 1.0}),
        ],
        vec![
            json!({"title": "b1", "_score": 1.0}),
            json!({"title": "b2", "_score": 2.0}),
        ],
    ];
    let full = titles(&order_hit_blocks(
        blocks.clone(),
        None,
        SearchWindow::first(10),
    ));
    let page = titles(&order_hit_blocks(
        blocks,
        None,
        SearchWindow {
            offset: 2,
            limit: 2,
        },
    ));
    assert_eq!(page, full[2..4].to_vec());
}

/// Every fan-out reads the same page out of the same operation.
///
/// The streaming fan-out used to discard `offset` here, so a paged search answered page 1
/// (ROADMAP OB8), and the two fan-outs disagreed about a `Stream`'s limit. This is the one
/// reading both of them now use.
#[test]
fn a_fan_out_reads_the_page_off_the_operation() {
    let search = |limit: Option<usize>, offset: Option<usize>| ClientOp::Search {
        index: "books".to_string(),
        query: "rust".to_string(),
        limit,
        offset,
        fields: None,
        sort: None,
    };

    assert_eq!(
        search_window_for(&search(Some(10), Some(20)), 7),
        SearchWindow {
            offset: 20,
            limit: 10
        },
        "a paged search keeps both halves of its page"
    );
    assert_eq!(
        search_window_for(&search(None, Some(20)), 7),
        SearchWindow {
            offset: 20,
            limit: 7
        },
        "an absent limit is the node default, and does not take the offset with it"
    );
    assert_eq!(
        search_window_for(&search(Some(10), None), 7),
        SearchWindow::first(10),
        "an unpaged search starts at the front"
    );

    // A stream has no offset to read — the HTTP route refuses one — but its limit is still
    // its own. The non-streaming fan-out used to fall through to the default here.
    assert_eq!(
        search_window_for(
            &ClientOp::Stream {
                index: "books".to_string(),
                query: "rust".to_string(),
                limit: Some(50),
                fields: None,
                sort: None,
            },
            7
        ),
        SearchWindow::first(50),
        "a stream's limit is its own"
    );

    // Nothing else is paged, and the value must not read as a page either way.
    assert_eq!(
        search_window_for(
            &ClientOp::GetConfig {
                index: "books".to_string()
            },
            7
        ),
        SearchWindow::first(7),
        "an unpaged operation starts at the front"
    );
}

/// Finding #2: i64 keys beyond f64's exact-integer range must order precisely.
#[test]
fn compare_hits_by_field_distinguishes_large_i64_keys() {
    use std::cmp::Ordering;
    let big = 9_007_199_254_740_992i64; // 2^53
    let bigger = big + 1; // not representable distinctly as f64
    let a = json!({ "_sort_key": bigger });
    let b = json!({ "_sort_key": big });
    assert_eq!(
        compare_hits_by_field(&a, &b, "_sort_key", SortOrder::Asc),
        Ordering::Greater
    );
}

/// Finding #3: date sort keys are normalized to epoch seconds so ordering is
/// chronological rather than lexicographic.
#[test]
fn normalize_sort_key_converts_dates_to_epoch_seconds() {
    let date_def = FieldDef::new("published".to_string(), TantivyFieldType::Date);

    let early = normalize_sort_key(&json!("2018-11-30"), Some(&date_def)).unwrap();
    let late = normalize_sort_key(&json!("2024-03-10T00:00:00Z"), Some(&date_def)).unwrap();

    assert!(early.is_i64(), "date key should be numeric, got {early:?}");
    assert!(
        early.as_i64().unwrap() < late.as_i64().unwrap(),
        "chronological order must hold numerically"
    );

    // Unparseable date → no key (hit will sort last).
    assert!(normalize_sort_key(&json!("not-a-date"), Some(&date_def)).is_none());
}

#[test]
fn normalize_sort_key_passes_through_non_date_values() {
    let numeric_def = FieldDef::new("year".to_string(), TantivyFieldType::I64);
    assert_eq!(
        normalize_sort_key(&json!(2020), Some(&numeric_def)).unwrap(),
        json!(2020)
    );
    // No schema entry → passthrough.
    assert_eq!(
        normalize_sort_key(&json!("hello"), None).unwrap(),
        json!("hello")
    );
}

#[test]
fn stamp_sort_keys_injects_normalized_date_key() {
    let mut schema = IndexSchema::default();
    schema.fields.insert(
        "published".to_string(),
        FieldDef::new("published".to_string(), TantivyFieldType::Date),
    );
    let spec = SortSpec {
        field: "published".to_string(),
        order: SortOrder::Asc,
    };
    let mut hits = vec![(
        Uuid::nil(),
        1.0f32,
        json!({"title": "x", "published": "2020-06-01"}),
    )];
    stamp_sort_keys(&mut hits, &spec, &schema);
    let key = hits[0].2.get(SORT_KEY_FIELD).expect("sort key stamped");
    assert!(key.is_i64());
}

#[test]
fn strip_sort_keys_removes_only_the_metadata_key() {
    let mut response = json!({
        "hits": [
            {"title": "a", "_score": 1.0, "_sort_key": 2020},
            {"title": "b", "_score": 0.9, "_sort_key": 2018},
        ],
        "hits_returned": 2
    });
    strip_sort_keys(&mut response);
    let hits = response["hits"].as_array().unwrap();
    for hit in hits {
        assert!(
            hit.get(SORT_KEY_FIELD).is_none(),
            "_sort_key must be stripped"
        );
        assert!(hit.get("_score").is_some(), "other metadata preserved");
        assert!(hit.get("title").is_some(), "content preserved");
    }
}

/// A schema record that still declares `_seq` does not make it sortable.
///
/// Only reachable as a unit test: the field is retired, so no index created now records it,
/// and every index created before the retirement still does. The engine refuses it in each
/// shard either way — so a guard that decides by looking the name up in the schema passes it
/// through on exactly the indexes that have the field, which is every index that predates
/// the change and none of the ones a test creates.
#[test]
fn a_retired_seq_column_in_the_schema_record_does_not_make_it_sortable() {
    let mut schema = IndexSchema::default();
    for (name, field_type) in [
        ("rank", TantivyFieldType::U64),
        ("title", TantivyFieldType::Text),
        ("flag", TantivyFieldType::Boolean),
        ("_seq", TantivyFieldType::U64),
    ] {
        schema.fields.insert(
            name.to_string(),
            FieldDef::new(name.to_string(), field_type),
        );
    }
    schema.fields.insert("doi".to_string(), shadow_field("doi"));
    schema.rebuild_shadow_fields_cache();

    let refused = |field: &str| {
        unsortable_sort_field(
            &schema,
            Some(&SortSpec {
                field: field.to_string(),
                order: SortOrder::Asc,
            }),
        )
    };

    // `_seq` is a fast u64 sitting in the record, so nothing about the declaration itself
    // distinguishes it from `rank`. It is refused by name.
    assert!(
        refused("_seq").is_some(),
        "a legacy _seq column must not be sortable"
    );
    // In the schema, no fast column, not text: the engine refuses this one too.
    assert!(
        refused("flag").is_some(),
        "a non-text field with no fast column must not be sortable"
    );
    assert!(
        refused("no_such_field").is_some(),
        "a name absent from the schema must not be sortable"
    );

    for field in ["rank", "title", "id", "doi"] {
        assert!(
            refused(field).is_none(),
            "'{field}' must still sort: a fast column, a text field, the key, and the key's \
                 shadow name"
        );
    }
}

/// A verdict reached on one node has to arrive intact on the next.
///
/// Everything a peer raised used to come back as an unclassified `Io` and answer `500`: a
/// document whose type the schema refuses was reported as this node's fault, and a schema the
/// cluster could not agree on as a defect rather than as something to retry. The classified
/// verdict is what crosses now, so a routed request is answered the same way a local one is.
#[test]
fn a_verdict_survives_the_wire() {
    let round_trip = |err: &OrchestratorError| -> OrchestratorError {
        let encoded = serde_json::to_string(err).expect("serialise");
        serde_json::from_str(&encoded).expect("deserialise")
    };

    let cases = [
        (
            OrchestratorError::SchemaUnconfirmed {
                index: "docs".into(),
                reason: "2 of 3 nodes connected".into(),
            },
            RemoteVerdict::Unavailable,
        ),
        (
            OrchestratorError::Validation(
                "Type mismatch for field 'n': expected I64, got F64".into(),
            ),
            RemoteVerdict::BadRequest,
        ),
        (
            OrchestratorError::NotReady("No shards".into()),
            RemoteVerdict::Unavailable,
        ),
        (
            OrchestratorError::Missing("Local shard 7 not found".into()),
            RemoteVerdict::ServerFault,
        ),
        (
            OrchestratorError::UnsortableField {
                field: "title".into(),
                reason: "no fast column".into(),
            },
            RemoteVerdict::BadRequest,
        ),
        (
            OrchestratorError::Storage(StoreError::IndexNotFound("gone".into())),
            RemoteVerdict::NotFound,
        ),
        (
            OrchestratorError::PeerUnreachable {
                message: "remote orchestrator for node 1 not found".into(),
            },
            RemoteVerdict::Unavailable,
        ),
        (
            OrchestratorError::Io(std::io::Error::other("a disk gave up")),
            RemoteVerdict::ServerFault,
        ),
        // A peer's quota refusal must stay a refusal: read as a fault, a forwarded write to a
        // tenant at its ceiling would answer 500 and invite the retry that cannot succeed.
        (
            OrchestratorError::QuotaExceeded {
                tenant: "acme".into(),
                detail: "tenant already owns 2 of 2 permitted indexes".into(),
            },
            RemoteVerdict::QuotaExceeded,
        ),
        // A peer minting the index must arrive as a rival, not as a peer that failed to answer:
        // the canvass settles the first and refuses on the second.
        (
            OrchestratorError::SchemaBeingMinted {
                index: "books".into(),
                node: Uuid::nil(),
            },
            RemoteVerdict::Minting,
        ),
    ];

    for (err, expected) in cases {
        assert_eq!(err.verdict(), expected, "local verdict for {err}");
        let arrived = round_trip(&err);
        assert_eq!(
            arrived.verdict(),
            expected,
            "verdict changed crossing the wire for {err}"
        );
        assert_eq!(
            arrived.to_string(),
            err.to_string(),
            "the message must arrive unchanged, and without the tag"
        );
    }
}

/// A node that predates the tagged form still gets an answer, and its answers are still read.
///
/// Forward and backward: an untagged string is what an older peer sends, and it has to read
/// as the cautious verdict rather than as a parse failure. An unknown tag is what a *newer*
/// peer sends, naming a verdict this build has not heard of, and takes the same route.
#[test]
fn an_untagged_or_unknown_verdict_is_read_as_a_server_fault() {
    let untagged: OrchestratorError =
        serde_json::from_str("\"something an older node said\"").expect("deserialise");
    assert_eq!(untagged.verdict(), RemoteVerdict::ServerFault);
    assert_eq!(untagged.to_string(), "something an older node said");

    // `\u001f` as JSON escapes it, which is how serde_json writes the separator: a raw
    // control byte is not legal in a JSON string, so the wire form stays valid JSON.
    let unknown: OrchestratorError =
        serde_json::from_str(r#""teapot\u001fa verdict from the future""#).expect("deserialise");
    assert_eq!(unknown.verdict(), RemoteVerdict::ServerFault);
    assert_eq!(unknown.to_string(), "a verdict from the future");
}

/// Two nodes holding different schemas for one index must pick the same winner without
/// talking to each other.
///
/// That is what lets schemas converge with no leader and no consensus round: the choice is a
/// pure function of the two candidates, so every node computes the same answer. The property
/// that matters is symmetry — if the verdict depended on which schema was passed first, two
/// nodes comparing the same pair would disagree and both would think they had won.
#[test]
fn the_schema_tie_break_is_deterministic_and_symmetric() {
    let schema = |version: u64, field: &str, field_type: TantivyFieldType| {
        let mut s = IndexSchema {
            version,
            ..Default::default()
        };
        s.fields
            .insert(field.into(), FieldDef::new(field.into(), field_type));
        s
    };

    // A newer version wins outright, whichever side it is passed on.
    let older = schema(4, "a", TantivyFieldType::Text);
    let newer = schema(9, "b", TantivyFieldType::I64);
    assert_eq!(
        NodeOrchestrator::preferred_schema(older.clone(), newer.clone()).version,
        9
    );
    assert_eq!(
        NodeOrchestrator::preferred_schema(newer.clone(), older.clone()).version,
        9
    );

    // At the same version the lower thumbprint wins, and the answer does not depend on
    // argument order. This is the concurrent-creation case: two nodes each declared version 1
    // for the same index and neither can be called the origin.
    let left = schema(1, "amount", TantivyFieldType::F64);
    let right = schema(1, "amount", TantivyFieldType::I64);
    assert_ne!(
        left.calculate_fingerprint(),
        right.calculate_fingerprint(),
        "the two candidates must be distinguishable or this proves nothing"
    );

    let from_left = NodeOrchestrator::preferred_schema(left.clone(), right.clone());
    let from_right = NodeOrchestrator::preferred_schema(right.clone(), left.clone());
    assert_eq!(
        from_left.calculate_fingerprint(),
        from_right.calculate_fingerprint(),
        "the tie-break disagreed with itself when the arguments were swapped"
    );
    assert_eq!(
        from_left.calculate_fingerprint(),
        left.calculate_fingerprint()
            .min(right.calculate_fingerprint()),
        "the lower thumbprint should have won"
    );

    // Identical candidates are a no-op rather than a coin flip.
    let same = schema(3, "x", TantivyFieldType::Text);
    assert_eq!(
        NodeOrchestrator::preferred_schema(same.clone(), same.clone()).calculate_fingerprint(),
        same.calculate_fingerprint()
    );
}

/// A numeric field holds what the writer can store, which is not "what the name infers".
///
/// `infer_type_from_value` reads every integer that fits in an `i64` as `I64`, so comparing
/// inferred names refused a declared `u64` every ordinary integer — `0` and `2018` included —
/// and accepted only what exceeds `i64::MAX`. A declared `f64` likewise refused every whole
/// number and took only a value written with a fraction. Both refused documents the engine
/// holds perfectly well: `add_json_value_to_doc` would have stored each one through
/// `as_u64()`/`as_f64()`.
#[test]
fn a_numeric_field_takes_every_value_the_writer_can_store() {
    let storable = |declared, value| unstorable_value("n", &declared, &value).is_none();

    // The cases the old name comparison refused.
    assert!(storable(TantivyFieldType::U64, json!(2018)));
    assert!(storable(TantivyFieldType::U64, json!(0)));
    assert!(storable(TantivyFieldType::F64, json!(3)));
    assert!(storable(TantivyFieldType::F64, json!(0)));
    assert!(storable(TantivyFieldType::F64, json!(-5)));

    // The ones it already allowed, still allowed.
    assert!(storable(TantivyFieldType::U64, json!(u64::MAX)));
    assert!(storable(TantivyFieldType::I64, json!(2018)));
    assert!(storable(TantivyFieldType::I64, json!(-5)));
    assert!(storable(TantivyFieldType::F64, json!(3.5)));

    // A list is several values of the field, each judged on its own.
    assert!(storable(TantivyFieldType::U64, json!([1, 2, 3])));
    assert!(storable(TantivyFieldType::F64, json!([1, 2.5])));
}

/// The refusals that prevent silent loss are the ones `as_*` returns `None` for, and they
/// have to survive the widening above — a value the writer would skip must still be refused
/// here, or the document is stored with the field quietly unindexed.
#[test]
fn a_numeric_field_still_refuses_what_the_writer_would_skip() {
    let refused = |declared, value| unstorable_value("n", &declared, &value).is_some();

    assert!(refused(TantivyFieldType::U64, json!(-5))); // as_u64() -> None
    assert!(refused(TantivyFieldType::U64, json!(3.5))); // as_u64() -> None
    assert!(refused(TantivyFieldType::I64, json!(3.5))); // as_i64() -> None
    assert!(refused(TantivyFieldType::I64, json!(u64::MAX))); // past i64::MAX
    assert!(refused(TantivyFieldType::U64, json!("2018"))); // a word, not a number
    assert!(refused(TantivyFieldType::F64, json!(true)));

    // And an element inside a list is held to the same rule.
    assert!(refused(TantivyFieldType::U64, json!([1, -2])));
}

/// A declaration cannot make a column exist, and the refusal says which case it is.
///
/// The guard reads the declaration, and for these five types the index builder never reads it
/// back — `add_bool_field`, `add_bytes_field`, `add_ip_addr_field`, `add_json_field` and
/// `add_facet_field` take no `fast`. So a declared `fast: true` passed the guard and was then
/// refused by every shard, which a scatter-gather reports as `200` with an empty page. Refused
/// here instead, with a reason that does not send the caller off to declare a flag that
/// changes nothing.
#[test]
fn a_declared_fast_on_a_type_with_no_column_is_still_refused() {
    let mut schema = IndexSchema::default();
    for (name, field_type) in [
        ("flag", TantivyFieldType::Boolean),
        ("addr", TantivyFieldType::Ip),
        ("blob", TantivyFieldType::Json),
        ("category", TantivyFieldType::Facet),
        ("raw", TantivyFieldType::Bytes),
        ("rank", TantivyFieldType::U64),
    ] {
        let mut def = FieldDef::new(name.to_string(), field_type);
        def.fast = Some(true);
        schema.fields.insert(name.to_string(), def);
    }

    let refusal = |field: &str| {
        unsortable_sort_field(
            &schema,
            Some(&SortSpec {
                field: field.to_string(),
                order: SortOrder::Asc,
            }),
        )
    };

    for field in ["flag", "addr", "blob", "category", "raw"] {
        let Some(OrchestratorError::UnsortableField { reason, .. }) = refusal(field) else {
            panic!("'{field}' declares a column its type cannot carry and must be refused");
        };
        assert!(
            reason.contains("cannot give it one"),
            "the reason must not ask for a declaration that already exists: {reason}"
        );
    }

    assert!(
        refusal("rank").is_none(),
        "a u64 field declared fast does carry a column and must still sort"
    );
}

/// A gather with no successful shard refuses, and carries whose fault it was.
///
/// The verdict decides the status — 400 for a request the caller can fix, 500 for a node that
/// could not read its own data — and it cannot be recovered from the message text, which is
/// how "field not found: added" used to be classified as a missing resource and answered 404.
#[test]
fn a_gather_that_answered_nowhere_refuses_and_says_whose_fault_it_was() {
    let shard = Uuid::nil();
    let refused = |kind, text: &str| {
        (
            shard,
            OrchestratorError::Io(std::io::Error::new(kind, text.to_string())),
        )
    };

    // One shard answering makes the rest a partial outage, which is reported as hits plus
    // `errors` rather than as a refusal.
    assert!(
        no_shard_answered(
            "papers",
            1,
            &[refused(
                std::io::ErrorKind::InvalidInput,
                "field not found: x"
            )]
        )
        .is_none(),
        "a partial answer is still an answer"
    );
    assert!(
        no_shard_answered("papers", 0, &[]).is_none(),
        "no shards and no failures is an empty index, not a refusal"
    );

    // Every shard runs the same query against the same schema, so they fail identically —
    // and one reason repeated per shard would read as several distinct problems.
    let Some(OrchestratorError::NoShardAnswered {
        index,
        reasons,
        caller_error,
    }) = no_shard_answered(
        "papers",
        0,
        &[
            refused(std::io::ErrorKind::InvalidInput, "field not found: added"),
            refused(std::io::ErrorKind::InvalidInput, "field not found: added"),
        ],
    )
    else {
        panic!("a gather no shard could answer must refuse");
    };
    assert_eq!(index, "papers");
    assert_eq!(reasons, "field not found: added");
    assert!(
        caller_error,
        "a field the index has no column for is the request's to fix"
    );

    // One node-level failure is enough to stop calling it the caller's fault: the request may
    // have been perfectly good and unreadable data is not an answer about it.
    let Some(OrchestratorError::NoShardAnswered { caller_error, .. }) = no_shard_answered(
        "papers",
        0,
        &[
            refused(std::io::ErrorKind::InvalidInput, "field not found: added"),
            refused(std::io::ErrorKind::PermissionDenied, "cannot open index"),
        ],
    ) else {
        panic!("still a refusal");
    };
    assert!(
        !caller_error,
        "a shard that could not read its data is this node's problem"
    );
}

/// A shadow field as the schema records one: the caller's name for the key, carrying no
/// column of its own.
fn shadow_field(name: &str) -> FieldDef {
    let mut def = FieldDef::new(name.to_string(), TantivyFieldType::Text);
    def.indexed = false;
    def.is_shadow = true;
    def
}

/// A delete's reasons name ids, and an id needs no renumbering to mean the same thing.
///
/// The delete counterpart of the write path's renumbering, and the reason it is simpler:
/// a position is only meaningful in the batch it was numbered against, an id is meaningful
/// everywhere. So a peer's reason about an id this node forwarded is the caller's reason
/// already, and passing it through is what makes the answer one flat list keyed by id
/// whichever node handled the id.
#[test]
fn a_peers_delete_reasons_come_back_keyed_by_id() {
    let node = Uuid::nil();
    let ids = vec!["d05".to_string(), "d09".to_string()];

    assert_eq!(
        remote_delete_rejections(node, &ids, 1, &["d09: no shard available".to_string()]),
        vec!["d09: no shard available".to_string()],
        "a reason about a forwarded id is passed through as the peer said it"
    );

    // Not about one id — it cost ids without naming one — so it is attributed to the node
    // that said it and stands for an id that was not deleted.
    assert_eq!(
        remote_delete_rejections(node, &ids, 1, &["index is read-only".to_string()]),
        vec![format!("node {node}: index is read-only")],
        "a batch-level reason is attributed rather than dropped"
    );

    // An id from someone else's batch is not one of ours: naming it would report an id the
    // caller never sent, so it is kept as what the node said.
    let foreign = remote_delete_rejections(node, &ids, 1, &["zz1: gone".to_string()]);
    assert_eq!(foreign, vec![format!("node {node}: zz1: gone")]);
}

/// A bulk delete's answer has to add up: deleted plus reasons is what was sent.
///
/// The defect this closes is the shortfall, not the wording. A peer's reasons were logged
/// and thrown away, so a peer that refused two of the three ids forwarded to it reported one
/// deleted and no reasons — and the caller could see two ids missing without learning which,
/// or why. ROADMAP OB9.
#[test]
fn a_delete_shortfall_is_always_explained() {
    let node = Uuid::nil();
    let ids = vec!["a".to_string(), "b".to_string(), "c".to_string()];

    // Three forwarded, one deleted, and the peer accounted for only one of the other two.
    let short = remote_delete_rejections(node, &ids, 1, &["b: locked".to_string()]);
    assert_eq!(short.len(), 2, "one reason per id not deleted: {short:?}");
    assert_eq!(short[0], "b: locked");
    assert!(
        short[1].contains("neither deleted nor refused"),
        "the one it did not explain says so: {short:?}"
    );

    // A peer that deleted everything leaves nothing to report, whatever else it said.
    assert!(
        remote_delete_rejections(node, &ids, 3, &["b: locked".to_string()]).is_empty(),
        "nothing was lost, so there is nothing to account for"
    );

    // More reasons than ids to say them against: folded onto the last, because the count
    // has to hold and a reason someone sent is the only account of a failure there is.
    let surplus = remote_delete_rejections(
        node,
        &ids,
        2,
        &["a: locked".to_string(), "c: vanished".to_string()],
    );
    assert_eq!(surplus.len(), 1, "one id was not deleted: {surplus:?}");
    assert!(
        surplus[0].contains("a: locked") && surplus[0].contains("c: vanished"),
        "and neither reason is lost: {surplus:?}"
    );
}

/// A peer numbers its reasons against the batch it received, not the one the caller sent.
#[test]
fn a_peers_reasons_are_renumbered_into_this_batch() {
    let node = Uuid::nil();
    // Positions 4 and 9 of the caller's batch were the ones forwarded.
    let reasons = vec!["document 1: bad field".to_string()];
    assert_eq!(
        remote_rejections(node, &[4, 9], 1, &reasons),
        vec!["document 9: bad field".to_string()],
        "the peer's document 1 is the caller's document 9"
    );
}

/// A reason that is not about one document still stands for a document that was not written.
#[test]
fn a_peers_batch_level_reason_is_attributed_rather_than_dropped() {
    let node = Uuid::nil();
    let reasons = vec!["index is read-only".to_string()];
    assert_eq!(
        remote_rejections(node, &[0, 1], 1, &reasons),
        vec![format!("node {node}: index is read-only")]
    );
}

/// A position from someone else's batch is worse than none: it names a document that is fine.
#[test]
fn a_position_outside_this_batch_is_not_taken_as_one() {
    let node = Uuid::nil();
    let reasons = vec!["document 7: bad field".to_string()];
    let rejections = remote_rejections(node, &[0, 1], 1, &reasons);
    assert_eq!(rejections.len(), 1);
    assert!(
        rejections[0].starts_with(&format!("node {node}:")),
        "it is kept as what the node said, not renumbered onto a document: {rejections:?}"
    );
}

/// A peer whose numbers do not add up leaves a shortfall, and the shortfall is stated.
#[test]
fn a_shortfall_a_peer_did_not_explain_is_still_reported() {
    let node = Uuid::nil();
    // Three forwarded, one written, and the node gave only one reason for the other two.
    let reasons = vec!["document 0: bad field".to_string()];
    let rejections = remote_rejections(node, &[10, 11, 12], 1, &reasons);
    assert_eq!(
        rejections.len(),
        2,
        "one reason per document not written: {rejections:?}"
    );
    assert_eq!(rejections[0], "document 10: bad field");
    assert!(
        rejections[1].contains("neither written nor refused"),
        "and the one it did not explain says so: {rejections:?}"
    );
}

/// A peer that wrote everything leaves nothing to report, whatever it said.
#[test]
fn nothing_is_reported_for_a_batch_the_peer_wrote_whole() {
    let node = Uuid::nil();
    assert!(remote_rejections(node, &[0, 1, 2], 3, &[]).is_empty());
    // More reasons than documents unwritten cannot inflate the count either.
    assert!(
        remote_rejections(node, &[0], 1, &["document 0: stale".to_string()]).is_empty(),
        "a reason for a document the node also says it wrote is not an extra rejection"
    );
}

#[test]
fn a_document_reason_is_split_only_when_that_is_what_it_is() {
    assert_eq!(
        split_document_reason("document 12: because"),
        Some((12, "because"))
    );
    assert_eq!(split_document_reason("documents 12: because"), None);
    assert_eq!(split_document_reason("document twelve: because"), None);
    assert_eq!(split_document_reason("index is read-only"), None);
}

/// A streaming search gives up on a client that stops reading, instead of parking — holding the
/// whole result — for as long as the connection stays open. A client that has gone is noticed
/// at once, as before.
#[tokio::test]
async fn a_stream_abandons_a_client_that_stops_reading() {
    let stall = Duration::from_millis(50);
    let (tx, rx) = mpsc::channel::<Result<bytes::Bytes, std::io::Error>>(1);
    let line = || bytes::Bytes::from_static(b"{}\n");

    assert!(
        RouterActor::send_or_abandon(&tx, line(), stall).await,
        "room in the channel: sent"
    );
    let started = Instant::now();
    assert!(
        !RouterActor::send_or_abandon(&tx, line(), stall).await,
        "nobody reading and the channel full: abandoned"
    );
    assert!(
        started.elapsed() >= stall,
        "after waiting out the stall bound, not before"
    );

    drop(rx);
    assert!(
        !RouterActor::send_or_abandon(&tx, line(), Duration::from_secs(30)).await,
        "a client that has gone is noticed without waiting"
    );
}

/// A clustered orchestrator with no shards, in a directory of its own.
async fn clustered_orchestrator(dir: &std::path::Path) -> NodeOrchestrator {
    let config = NodeConfig {
        storage_path: dir.to_path_buf(),
        clustered: true,
        ..NodeConfig::default()
    };
    NodeOrchestrator::new(config, NodeIdentity::new(), 10, 4)
        .await
        .expect("an orchestrator with no shards")
}

fn first_write(index: &str) -> ClientOp {
    ClientOp::Write {
        index: index.to_string(),
        id: "d1".to_string(),
        routing_key: None,
        doc: json!({"title": "Dune"}),
        forwarded: false,
        schema_body: None,
        tenant: None,
    }
}

/// While this node is minting an index's schema, a peer asking for it is told so — never "none".
///
/// The asker is canvassing to decide whether it may sample a schema of its own; answered
/// "none", it would mint a second schema for the same index. It is told who is minting, and an
/// asker minting the same index is recorded as a rival, so the two settle the race the same way.
#[tokio::test]
async fn a_schema_being_minted_is_not_reported_absent() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut orchestrator = clustered_orchestrator(dir.path()).await;
    let me = orchestrator.identity.uuid;
    let rival = Uuid::new_v4();
    let ask = |index: &str, minting_by| ClientOp::GetRawSchema {
        index: index.to_string(),
        minting_by,
    };

    let before = orchestrator
        .handle_client_op(ask("books", Some(rival)))
        .await
        .resolve()
        .await;
    assert!(
        matches!(&before, Ok(JsonValue::Null)),
        "nothing held and nothing minting is plainly absent, got {before:?}"
    );
    assert!(
        orchestrator.mint_rivals.is_empty(),
        "an asker is a rival only while this node is minting the same index"
    );

    orchestrator.minting.insert("books".to_string(), 1);
    let lookup = orchestrator
        .handle_client_op(ask("books", None))
        .await
        .resolve()
        .await;
    assert!(
        matches!(&lookup, Err(OrchestratorError::SchemaBeingMinted { index, node }) if index == "books" && *node == me),
        "an index being minted must not read as absent, got {lookup:?}"
    );
    assert!(
        orchestrator.mint_rivals.is_empty(),
        "a lookup that creates nothing is not a rival"
    );

    let _ = orchestrator
        .handle_client_op(ask("books", Some(rival)))
        .await
        .resolve()
        .await;
    assert_eq!(
        orchestrator
            .mint_rivals
            .get("books")
            .map(|r| r.contains(&rival)),
        Some(true),
        "a peer minting the same index must be recorded as a rival"
    );

    let other = orchestrator
        .handle_client_op(ask("films", Some(rival)))
        .await
        .resolve()
        .await;
    assert!(
        matches!(&other, Ok(JsonValue::Null)),
        "only the index being minted is affected, got {other:?}"
    );
}

/// A race to mint one index is won by the lowest node id, and every contender agrees on it.
///
/// Each contender runs the same function over the others' ids, so the one with the lowest id
/// finds no one to yield to and every other one yields to it — one schema, with nothing
/// exchanged but the ids the canvass already carried.
#[test]
fn the_lowest_node_id_wins_a_race_to_mint() {
    let mut ids: Vec<Uuid> = (0..4).map(|_| Uuid::new_v4()).collect();
    ids.sort();
    let lowest = ids[0];

    for me in &ids {
        let rivals = ids.iter().copied().filter(|id| id != me);
        let verdict = mint_winner(*me, rivals);
        if *me == lowest {
            assert_eq!(verdict, None, "the lowest id mints");
        } else {
            assert_eq!(
                verdict,
                Some(lowest),
                "every other contender yields to the lowest"
            );
        }
    }
    assert_eq!(mint_winner(lowest, []), None, "no rivals, no race");
}

/// Only a clustered first write to an index with no schema leaves the mailbox to canvass.
///
/// A forwarded share asks its sender rather than the cluster, a write carrying a schema body
/// has its answer with it, and a standalone node's canvass asks nobody — none of them waits on
/// a peer, so none is worth the trip out and back.
#[tokio::test]
async fn only_a_clustered_first_write_canvasses_outside_the_mailbox() {
    let dir = tempfile::tempdir().expect("temp dir");
    let orchestrator = clustered_orchestrator(dir.path()).await;

    assert_eq!(
        orchestrator
            .mint_canvass_needed(&first_write("books"))
            .await,
        Some("books".to_string())
    );
    let bulk = ClientOp::BulkWrite {
        index: "books".to_string(),
        docs: Vec::new(),
        forwarded: false,
        schema_body: None,
        tenant: None,
    };
    assert_eq!(
        orchestrator.mint_canvass_needed(&bulk).await,
        Some("books".to_string())
    );

    let mut forwarded = first_write("books");
    if let ClientOp::Write { forwarded: f, .. } = &mut forwarded {
        *f = true;
    }
    assert_eq!(orchestrator.mint_canvass_needed(&forwarded).await, None);

    let mut carried = first_write("books");
    if let ClientOp::Write { schema_body, .. } = &mut carried {
        let mut schema = IndexSchema::default();
        schema.fields.insert(
            "title".to_string(),
            FieldDef::new("title".to_string(), TantivyFieldType::Text),
        );
        *schema_body = Some(Box::new(schema));
    }
    assert_eq!(orchestrator.mint_canvass_needed(&carried).await, None);

    let read = ClientOp::GetRawSchema {
        index: "books".to_string(),
        minting_by: None,
    };
    assert_eq!(orchestrator.mint_canvass_needed(&read).await, None);

    let standalone_dir = tempfile::tempdir().expect("temp dir");
    let standalone = NodeOrchestrator::new(
        NodeConfig {
            storage_path: standalone_dir.path().to_path_buf(),
            ..NodeConfig::default()
        },
        NodeIdentity::new(),
        10,
        4,
    )
    .await
    .expect("an orchestrator with no shards");
    assert_eq!(
        standalone.mint_canvass_needed(&first_write("books")).await,
        None
    );
}

/// A forward to a peer is handed back to run off the mailbox, never awaited inside it.
///
/// Awaited inside, two nodes forwarding to each other at once each waited on the other's
/// mailbox until the 60 s peer timeout — every bulk write sent through all three nodes at once
/// stalled. The one-hop bound is kept, and still answers at once: a node that was itself
/// forwarded to refuses rather than forwarding on.
#[tokio::test]
async fn a_forward_runs_off_the_mailbox() {
    let dir = tempfile::tempdir().expect("temp dir");
    let orchestrator = clustered_orchestrator(dir.path()).await;
    let target = Uuid::new_v4();
    let delete = |forwarded| ClientOp::Delete {
        index: "books".to_string(),
        id: "d1".to_string(),
        routing_key: None,
        forwarded,
    };

    assert!(
        matches!(
            orchestrator.forward_later(target, false, delete(true), None),
            Answer::Later(_)
        ),
        "a first hop's forward must be deferred"
    );
    match orchestrator.forward_later(target, true, delete(true), None) {
        Answer::Now(Err(err)) => assert!(err.to_string().contains("disagree about who owns it")),
        Answer::Now(Ok(v)) => panic!("a second hop must be refused, got {v}"),
        Answer::Later(_) => panic!("a second hop is refused at once, not deferred"),
    }
}

/// A peer's writes, shares and searches are served off the mailbox; what needs the actor is not.
///
/// The worker lane is where the router sends this node's own requests and where a peer's go
/// now, on the peer lane. An op that must write a schema or edit actor state has to stay on the
/// actor: offered to a worker it would only be handed back, and a config edit or index delete
/// run anywhere else would race the actor's own changes.
#[test]
fn only_what_a_worker_can_serve_leaves_the_mailbox() {
    let index = || "books".to_string();
    for op in [
        first_write("books"),
        ClientOp::BulkWrite {
            index: index(),
            docs: Vec::new(),
            forwarded: true,
            schema_body: None,
            tenant: None,
        },
        ClientOp::BulkDelete {
            index: index(),
            docs: Vec::new(),
            forwarded: true,
        },
        ClientOp::Search {
            index: index(),
            query: "*".to_string(),
            limit: None,
            offset: None,
            fields: None,
            sort: None,
        },
    ] {
        assert!(worker_eligible(&op), "a worker must serve {op:?}");
    }
    for op in [
        ClientOp::DeleteIndex {
            index: index(),
            delete_schema: false,
        },
        ClientOp::GetConfig { index: index() },
        ClientOp::GetRawSchema {
            index: index(),
            minting_by: None,
        },
    ] {
        assert!(!worker_eligible(&op), "{op:?} must stay on the actor");
    }
}
