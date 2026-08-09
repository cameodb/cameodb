//! The workloads themselves — all of them ordinary SDK calls.

use crate::args::Args;
use crate::stats::{Samples, Summary};
use anyhow::{Context, Result};
use client::CameoClient;
use client::sdk::AdminWorkersResponse;
use serde_json::json;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Words the generated documents are built from, so searches have predictable selectivity:
/// every document contains `bench`, and the rest vary.
const WORDS: &[&str] = &[
    "alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta",
];

/// A schema, declared rather than inferred.
///
/// Inference would also work — the node builds a schema from the first documents it sees —
/// but a benchmark should not measure schema evolution by accident on its first requests.
pub async fn prepare_index(client: &CameoClient, args: &Args) -> Result<()> {
    let schema = json!({
        "fields": {
            "title": {"name": "title", "field_type": "text", "indexed": true},
            "body":  {"name": "body",  "field_type": "text", "indexed": true},
            "seq":   {"name": "seq",   "field_type": "i64",  "indexed": true, "fast": true}
        }
    });

    client
        .put_index_config(&args.index, &schema)
        .await
        .with_context(|| format!("could not create the schema for '{}'", args.index))?;

    if args.seed_docs == 0 {
        return Ok(());
    }

    // Seed in batches. `bulk_index` exists precisely so that loading data is not measured as
    // one request per document — which is also why the write workload below, which *is*
    // measuring one write, uses `write_document` instead.
    print!("seeding {} documents… ", args.seed_docs);
    let started = Instant::now();
    const BATCH: usize = 500;
    let mut written = 0usize;
    while written < args.seed_docs {
        let batch: Vec<_> = (written..(written + BATCH).min(args.seed_docs))
            .map(|i| {
                let id = format!("seed-{i}");
                json!({"id": id, "doc": document(&id, i)})
            })
            .collect();
        written += batch.len();
        client
            .bulk_index(&args.index, &batch)
            .await
            .context("seeding failed")?;
    }

    // Commit so the seeded documents are searchable before measurement starts; otherwise the
    // search workload would run against an empty searcher for its first seconds.
    let _ = client.admin_index_commit(&args.index).await;
    println!("done in {:.1}s", started.elapsed().as_secs_f64());
    Ok(())
}

fn document(id: &str, seq: usize) -> serde_json::Value {
    let a = WORDS[seq % WORDS.len()];
    let b = WORDS[(seq / WORDS.len()) % WORDS.len()];
    json!({
        "id": id,
        "title": format!("bench {a} {b}"),
        "body": format!("document {seq} covering {a} and {b} for the bench corpus"),
        "seq": seq as i64,
    })
}

pub struct Report {
    pub writes: Samples,
    /// One sample per *request*, not per document — a 500-document request that takes 200ms
    /// is one 200ms sample. `docs` carries the document count so per-document cost can be
    /// derived without pretending 500 documents each took 200ms.
    pub bulk: Samples,
    pub bulk_docs: u64,
    pub searches: Samples,
    /// The node's own `took_ms` for each search, as microseconds so it shares a scale with
    /// the client-side samples.
    pub server_side: Samples,
}

impl Report {
    pub fn print(self, wall: Duration) {
        let Report {
            writes,
            bulk,
            bulk_docs,
            searches,
            server_side,
        } = self;

        let searched = !searches.is_empty();
        let bulked = !bulk.is_empty();

        let mut summaries: Vec<Summary> = Vec::new();
        if !writes.is_empty() {
            summaries.push(writes.summarize("writes (client-observed)", wall));
        }
        if bulked {
            summaries.push(bulk.summarize("bulk requests (client-observed)", wall));
        }
        if searched {
            summaries.push(searches.summarize("searches (client-observed)", wall));
            summaries.push(server_side.summarize("searches (node-reported took_ms)", wall));
        }
        for summary in summaries {
            summary.print();
        }

        if bulked {
            let secs = wall.as_secs_f64();
            let docs_per_sec = if secs > 0.0 {
                bulk_docs as f64 / secs
            } else {
                0.0
            };
            println!(
                "\n  {:>10}  {} documents, {:.0} docs/s",
                "bulk total", bulk_docs, docs_per_sec
            );
            println!(
                "  {:>10}  compare docs/s against the write mode's ok/s — that ratio is what\n\
                 {:>12}batching buys. Per-request latency rises with batch size; per-document\n\
                 {:>12}cost is what falls.",
                "", "", ""
            );
        }

        if searched {
            println!(
                "\nThe gap between client-observed and node-reported search latency is queueing,\n\
                 the worker hop and the network — everything outside the search itself."
            );
        }
    }
}

/// Run both workloads for `duration`, then report.
///
/// Each worker owns its own `Samples` and they are merged at the end, so nothing is shared
/// across tasks on the hot path — a mutex around the sample vector would show up in the very
/// numbers being collected.
pub async fn run(client: Arc<CameoClient>, args: &Args, duration: Duration) -> Result<Report> {
    let deadline = Instant::now() + duration;
    let mut tasks = Vec::new();

    if args.mode.writes() {
        for worker in 0..args.concurrency {
            let client = Arc::clone(&client);
            let index = args.index.clone();
            tasks.push(tokio::spawn(async move {
                write_worker(client, index, worker, deadline).await
            }));
        }
    }

    if args.mode.bulk() {
        for worker in 0..args.concurrency {
            let client = Arc::clone(&client);
            let index = args.index.clone();
            let batch_size = args.batch_size;
            tasks.push(tokio::spawn(async move {
                bulk_worker(client, index, worker, batch_size, deadline).await
            }));
        }
    }

    if args.mode.searches() {
        for worker in 0..args.concurrency {
            let client = Arc::clone(&client);
            let index = args.index.clone();
            tasks.push(tokio::spawn(async move {
                search_worker(client, index, worker, deadline).await
            }));
        }
    }

    let mut report = Report {
        writes: Samples::default(),
        bulk: Samples::default(),
        bulk_docs: 0,
        searches: Samples::default(),
        server_side: Samples::default(),
    };
    for task in tasks {
        let outcome = task.await.context("a workload task panicked")?;
        match outcome {
            WorkerOutput::Writes(samples) => report.writes.merge(samples),
            WorkerOutput::Bulk { samples, docs } => {
                report.bulk.merge(samples);
                report.bulk_docs += docs;
            }
            WorkerOutput::Searches { client, server } => {
                report.searches.merge(client);
                report.server_side.merge(server);
            }
        }
    }
    Ok(report)
}

enum WorkerOutput {
    Writes(Samples),
    Bulk { samples: Samples, docs: u64 },
    Searches { client: Samples, server: Samples },
}

/// Batched writes through `bulk_index`.
///
/// The counterpart to `write_worker`. One request carries `batch_size` documents, so the
/// latency sample is per request and the interesting number is documents per second — a
/// batch amortises the round trip, the redb transaction and the commit-threshold check
/// across every document in it.
async fn bulk_worker(
    client: Arc<CameoClient>,
    index: String,
    worker: usize,
    batch_size: usize,
    deadline: Instant,
) -> WorkerOutput {
    let mut samples = Samples::default();
    let mut docs = 0u64;
    let mut seq = 0usize;
    while Instant::now() < deadline {
        let batch: Vec<_> = (0..batch_size)
            .map(|i| {
                let id = format!("b{worker}-{}", seq + i);
                json!({"id": id, "doc": document(&id, worker * 10_000_000 + seq + i)})
            })
            .collect();
        seq += batch_size;

        let started = Instant::now();
        match client.bulk_index(&index, &batch).await {
            Ok(_) => {
                samples.record(started.elapsed());
                docs += batch_size as u64;
            }
            Err(err) => samples.record_error(err),
        }
    }
    WorkerOutput::Bulk { samples, docs }
}

async fn write_worker(
    client: Arc<CameoClient>,
    index: String,
    worker: usize,
    deadline: Instant,
) -> WorkerOutput {
    let mut samples = Samples::default();
    let mut seq = 0usize;
    while Instant::now() < deadline {
        // Ids are unique per worker so concurrent writers never collide on one document,
        // which would turn the workload into a contended update of a single key.
        let id = format!("w{worker}-{seq}");
        let doc = document(&id, worker * 1_000_000 + seq);
        seq += 1;

        let started = Instant::now();
        match client.write_document(&index, &id, &doc, None).await {
            Ok(_) => samples.record(started.elapsed()),
            Err(err) => samples.record_error(err),
        }
    }
    WorkerOutput::Writes(samples)
}

async fn search_worker(
    client: Arc<CameoClient>,
    index: String,
    worker: usize,
    deadline: Instant,
) -> WorkerOutput {
    let mut observed = Samples::default();
    let mut server = Samples::default();
    let mut round = 0usize;
    while Instant::now() < deadline {
        // Vary the term so every query does not hit the same warmed posting list.
        let query = format!("{} {}", "bench", WORDS[(worker + round) % WORDS.len()]);
        round += 1;

        let started = Instant::now();
        match client.search(&index, &query, Some(10), None, None).await {
            Ok(response) => {
                observed.record(started.elapsed());
                if let Some(took) = response.get("took_ms").and_then(|v| v.as_u64()) {
                    server.record(Duration::from_millis(took));
                }
            }
            Err(err) => observed.record_error(err),
        }
    }
    WorkerOutput::Searches {
        client: observed,
        server,
    }
}

/// Print how work was spread across the worker pool during the run.
///
/// This is the other half of a latency result. An even `jobs` row with a bad p99 is a
/// different problem from a lopsided one, and `affine_sends` versus `round_robin_sends`
/// says whether shard-affine dispatch was in play at all.
pub fn print_worker_delta(before: &AdminWorkersResponse, after: &AdminWorkersResponse) {
    println!("\nworker pool");
    println!("-----------");

    let jobs: Vec<u64> = after
        .workers
        .iter()
        .map(|worker| {
            let was = before
                .workers
                .iter()
                .find(|w| w.id == worker.id)
                .map(|w| w.jobs_completed)
                .unwrap_or(0);
            worker.jobs_completed.saturating_sub(was)
        })
        .collect();
    println!("  jobs per worker: {jobs:?}");

    let cores: Vec<String> = after
        .workers
        .iter()
        .map(|worker| match (worker.core_id, worker.target_core_id) {
            (Some(core), _) => core.to_string(),
            (None, Some(target)) => format!("({target})"),
            (None, None) => "-".to_string(),
        })
        .collect();
    println!(
        "  cores:           {} {}",
        cores.join(" "),
        if after.pinned_workers == 0 && after.pinning_requested {
            "  (requested but refused — pinning is a no-op on macOS)"
        } else {
            ""
        }
    );

    let d = &after.dispatch;
    let b = &before.dispatch;
    println!(
        "  dispatch:        affine {}  affine-full {}  round-robin {}  mailbox {}",
        d.affine_sends.saturating_sub(b.affine_sends),
        d.affine_full_fallbacks
            .saturating_sub(b.affine_full_fallbacks),
        d.round_robin_sends.saturating_sub(b.round_robin_sends),
        d.actor_mailbox_fallbacks
            .saturating_sub(b.actor_mailbox_fallbacks),
    );

    if !after.shards.is_empty() {
        let placement: Vec<String> = after
            .shards
            .iter()
            .map(|shard| match shard.core_id {
                Some(core) => format!("{}→core{}", shard.ordinal, core),
                None => format!("{}→unpinned", shard.ordinal),
            })
            .collect();
        println!("  shard writers:   {}", placement.join("  "));
    }
}
