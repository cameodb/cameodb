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
//! # What it measures
//!
//! Two load models, and the flags pick between them.
//!
//! **Closed-loop (`--concurrency`, the default).** N workers each issue one request, wait,
//! and issue the next. Measures service time at a fixed concurrency. It is not an arrival
//! process and its percentiles are not an SLA — a saturated node appears as rising latency
//! rather than an unbounded queue, because the harness stops offering load while it waits.
//! Compare runs at equal concurrency and treat the numbers as relative. Every performance
//! figure published for this project was taken this way, and this path is unchanged so they
//! stay comparable.
//!
//! **Open-loop (`--rate`).** Requests are offered on a schedule that does not wait for
//! answers, so an overloaded node produces a growing queue instead of a shrinking offered
//! load. Latency is reported from the *intended* send time as well as the actual one; where
//! those diverge, the difference is queueing that the closed-loop model cannot see. See
//! [`openloop`] for the mechanism and [`stats`] for why both clocks are kept.
//!
//! Searches also carry the node's own `took_ms`, reported beside the client-observed
//! latency. The gap between them is everything outside the search itself: queueing at the
//! concurrency limiter, the worker hop, and the network.

mod args;
mod openloop;
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

    print_plan(&args);

    workload::prepare_index(&client, &args).await?;

    // Snapshot around the measured window only. Taken earlier, the counts also include
    // seeding and warmup and cannot be reconciled against the measured request totals. The
    // open-loop path warms up inside its own runner, so each arm takes the snapshot at the
    // point where its warmup is already done.
    let before;
    let after;

    match &args.load {
        args::Load::Closed { concurrency } => {
            if !args.warmup.is_zero() {
                println!(
                    "\nwarming up for {}s (not measured)…",
                    args.warmup.as_secs()
                );
                workload::run(Arc::clone(&client), &args, *concurrency, args.warmup).await?;
            }
            before = client.admin_worker_stats().await.ok();

            println!("measuring for {}s…", args.duration.as_secs());
            let started = Instant::now();
            let report =
                workload::run(Arc::clone(&client), &args, *concurrency, args.duration).await?;
            let wall = started.elapsed();
            report.print(wall);
            after = client.admin_worker_stats().await.ok();
        }
        args::Load::Open(open) => {
            // The open-loop runner does its own warmup, so the snapshot has to sit inside it
            // — taken here it would straddle the warmup. It is taken after instead, which
            // costs the warmup's jobs and is the same trade the closed-loop arm makes.
            let reports = openloop::run(Arc::clone(&client), &args, open).await?;
            before = None;
            openloop::print(reports, open);
            after = client.admin_worker_stats().await.ok();
        }
    }

    match (before, after) {
        (Some(before), Some(after)) => workload::print_worker_delta(&before, &after),
        (None, Some(_)) => {}
        _ => println!(
            "\n(worker pool stats unavailable — /_admin/* is disabled or needs a node-admin key)"
        ),
    }

    if !args.keep_index {
        // Best-effort: a failure to clean up should not mask the results just printed.
        if let Err(err) = client.delete_index(&args.index, true).await {
            eprintln!("warning: could not delete index '{}': {err}", args.index);
        }
    }

    Ok(())
}

/// What this run is about to do, in one place, before it does any of it.
///
/// The open-loop arm says more than the closed-loop one because more of it can go wrong: the
/// generator reserves a core to hit its arrival times, so a run against a node on the same
/// machine is a run where the two compete. That is worth saying before twenty seconds of
/// measurement rather than after.
fn print_plan(args: &args::Args) {
    match &args.load {
        args::Load::Closed { concurrency } => println!(
            "plan: closed-loop mode={:?} concurrency={} warmup={}s duration={}s index={}",
            args.mode,
            concurrency,
            args.warmup.as_secs(),
            args.duration.as_secs(),
            args.index
        ),
        args::Load::Open(open) => {
            let rates: Vec<String> = open
                .steps
                .iter()
                .map(|step| format!("{:.0}/s", step.search.max(step.write)))
                .collect();
            println!(
                "plan: open-loop mode={:?} arrival={} rate={} warmup={}s duration={}s \
                 max-in-flight={} seed={} index={}",
                args.mode,
                open.arrival.name(),
                rates.join(" → "),
                args.warmup.as_secs(),
                open.total_duration().as_secs(),
                open.max_in_flight,
                open.seed,
                args.index
            );
            if client::sdk::origin_of(&args.url).contains("localhost")
                || args.url.contains("127.0.0.1")
                || args.url.contains("[::1]")
            {
                println!(
                    "note: the node is on this machine. The generator spins to hit its arrival \
                     times, so the two compete for cores — read the reported harness lag before \
                     believing any number here."
                );
            }
        }
    }
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
