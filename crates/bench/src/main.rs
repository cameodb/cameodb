//! `cameodb-bench` — a latency harness for CameoDB, and a worked example of the client SDK.
//!
//! Two jobs, and they reinforce each other. As a harness it answers the question the
//! ROADMAP's performance targets are written in terms of and that nothing here could
//! previously measure: what does a write cost at the 99th percentile, and what does that do
//! to searches running beside it. As an example it is a complete, ordinary consumer of
//! [`client::CameoClient`] — connect, authenticate, create a schema, write, search, read the
//! admin endpoints — using nothing a third party could not use.
//!
//! That second job constrains the first: this binary depends on `client` and never on the
//! server crate, and it issues no request the SDK cannot express. When it needed a
//! single-document write, the answer was to add `write_document` to the SDK rather than
//! reach past it with a raw `http()` call.
//!
//! # What it measures, and what it does not
//!
//! Closed-loop: `--concurrency` workers each issue one request, wait, and issue the next.
//! That measures service time at a fixed concurrency. It is not an open-loop generator, so
//! it does not model a fixed arrival rate and its percentiles are not an SLA — a saturated
//! node appears as rising latency rather than an unbounded queue, because the harness stops
//! offering load while it waits. Compare runs at equal concurrency and treat the numbers as
//! relative.
//!
//! Searches also carry the node's own `took_ms`, reported beside the client-observed
//! latency. The gap between them is everything outside the search itself: queueing at the
//! concurrency limiter, the worker hop, and the network.

mod args;
mod stats;
mod workload;

use anyhow::{Context, Result};
use client::{CameoClient, ClientAuth, Credential, TlsTrust};
use std::sync::Arc;
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<()> {
    // reqwest builds a rustls client, and rustls 0.23 refuses to pick a crypto provider on
    // its own when more than one is compiled in. The server binary does this too.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let Some(args) = args::parse()? else {
        return Ok(()); // --help
    };

    let client = Arc::new(connect(&args)?);

    // Fail early and legibly rather than deep inside a worker: a wrong URL, a missing key or
    // a node that is not up should say so before anything is written.
    let health = client
        .health()
        .await
        .context("could not reach the node — check --url, and --api-key if it is secured")?;
    println!(
        "node: {} ({})",
        health.node_id.as_deref().unwrap_or("id withheld"),
        health.status
    );
    if let Some(key_id) = client.key_id() {
        println!("authenticated as key {key_id}");
    }

    println!(
        "plan: mode={:?} concurrency={} warmup={}s duration={}s index={}",
        args.mode,
        args.concurrency,
        args.warmup.as_secs(),
        args.duration.as_secs(),
        args.index
    );

    workload::prepare_index(&client, &args).await?;

    if !args.warmup.is_zero() {
        println!(
            "\nwarming up for {}s (not measured)…",
            args.warmup.as_secs()
        );
        workload::run(Arc::clone(&client), &args, args.warmup).await?;
    }

    // Snapshot *after* warmup, so the worker-pool delta covers the same requests the
    // latencies do. Taken before it, the counts also include seeding and warmup and cannot
    // be reconciled against the measured request totals.
    let before = client.admin_worker_stats().await.ok();

    println!("measuring for {}s…", args.duration.as_secs());
    let started = Instant::now();
    let report = workload::run(Arc::clone(&client), &args, args.duration).await?;
    let wall = started.elapsed();

    report.print(wall);

    let after = client.admin_worker_stats().await.ok();
    if let (Some(before), Some(after)) = (before, after) {
        workload::print_worker_delta(&before, &after);
    } else {
        println!(
            "\n(worker pool stats unavailable — /_admin/* is disabled or needs a node-admin key)"
        );
    }

    if !args.keep_index {
        // Best-effort: a failure to clean up should not mask the results just printed.
        if let Err(err) = client.delete_index(&args.index, true).await {
            eprintln!("warning: could not delete index '{}': {err}", args.index);
        }
    }

    Ok(())
}

/// Build the client. This is the whole of what an SDK consumer has to do.
fn connect(args: &args::Args) -> Result<CameoClient> {
    let trust = TlsTrust {
        insecure_server: args.insecure,
        // The harness fetches no remote schema or data sources, so this stays strict
        // whatever --insecure says: the two are separate trust decisions in the SDK.
        insecure_source: false,
    };

    let auth = ClientAuth {
        credential: args
            .api_key
            .as_deref()
            .map(Credential::parse)
            .transpose()
            .context("--api-key is not a valid CameoDB key")?,
        // A key over plaintext to a non-loopback host is refused by the SDK unless this is
        // set. The harness does not set it: benchmarking is not a reason to leak a key.
        allow_plaintext: false,
    };

    CameoClient::new_with_options(&args.url, trust, auth)
        .with_context(|| format!("could not build a client for {}", args.url))
}
