//! The open-loop generator: arrivals on a schedule, independent of completions.
//!
//! # What makes it open-loop
//!
//! A scheduler computes when each request *should* go out and dispatches it then, whatever
//! is already outstanding. It never waits for an answer. The closed-loop mode next door
//! cannot do this by construction — its next request does not exist until the previous one
//! returns — and that difference is the whole reason this module exists:
//!
//! - **A saturated node shows as a growing queue rather than as rising latency.** Closed-loop,
//!   a node that slows down simply receives less work, so the offered load silently tracks
//!   the service rate and the overload never appears.
//! - **Arrivals can coincide.** Several writes genuinely in flight at one shard is the
//!   precondition for any write-side batching to pay, and a closed-loop client at
//!   concurrency *N* across *S* shards can never produce it below *N/S* per shard. This is
//!   what made the bounded linger untestable rather than merely unmeasured.
//! - **Latency can be charged from the moment the request was wanted**, not from the moment
//!   the harness got round to sending it. See [`crate::stats`] for why that is the only
//!   honest number once the generator is behind.
//!
//! # What it costs
//!
//! Arrival gaps at interesting rates are tens of microseconds, and tokio's timer wheel ticks
//! at about a millisecond. Sleeping per request would quantize every arrival to the tick and
//! inject more scheduling noise than the latencies being measured. So the loop sleeps only
//! while the next arrival is comfortably far off and busy-waits the last stretch, which
//! reserves a core for the generator. That is a real cost and it is why [`StepReport`] reports
//! harness lag beside every latency: if the generator is behind while in-flight is below the
//! ceiling, the run measured this harness.

use crate::args::{Args, Arrival, OpenLoop, Step, Workload};
use crate::stats::{Samples, ms};
use crate::workload::{WORDS, document};
use anyhow::Result;
use client::{CameoClient, failure_status};
use serde_json::{Value as JsonValue, json};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{Semaphore, mpsc, oneshot};

/// How far ahead of an arrival the loop stops sleeping and starts spinning.
///
/// Sized to swallow tokio's timer granularity plus a scheduling slice. Smaller and the sleep
/// overshoots the arrival it was waiting for; larger and the generator burns a core for
/// longer than it needs to.
const SPIN_SLACK: Duration = Duration::from_micros(1_500);

/// Dispatches to make before yielding, while catching up after falling behind.
///
/// Without it a generator that is behind spins out its entire backlog without ever returning
/// to the runtime, starving the very tasks that would drain it.
const CATCHUP_YIELD_EVERY: u32 = 64;

/// How long to wait for requests still outstanding when a step's window closes.
const DRAIN_GRACE: Duration = Duration::from_secs(30);

/// Harness lag above which a result is not a statement about the node.
const LAG_CEILING: Duration = Duration::from_millis(5);

// ============================================================================
// Arrival process
// ============================================================================

/// splitmix64. Enough for inter-arrival gaps and one less dependency in a crate whose other
/// job is to be a readable example.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in (0, 1]. Never zero, so `ln` below is always finite.
    fn next_unit(&mut self) -> f64 {
        ((self.next_u64() >> 11) as f64 + 1.0) / ((1u64 << 53) as f64 + 1.0)
    }
}

/// The gap to the next arrival.
///
/// Exponential for a Poisson process — the memoryless gap, which is what makes the count in
/// any window Poisson-distributed and produces the clustering a real client population has.
/// Uniform is exactly `1/rate`, which removes arrival variance when the question is about the
/// node rather than about the traffic.
fn interarrival(rng: &mut Rng, rate: f64, arrival: Arrival) -> Duration {
    let seconds = match arrival {
        Arrival::Uniform => 1.0 / rate,
        Arrival::Poisson => -rng.next_unit().ln() / rate,
    };
    Duration::from_secs_f64(seconds)
}

// ============================================================================
// Requests
// ============================================================================

/// Everything a request needs that does not change between requests.
struct RequestContext {
    client: Arc<CameoClient>,
    index: String,
    /// One fully-built batch, cloned and re-keyed per bulk request. Building 500 documents
    /// from scratch per arrival is harness CPU that would show up as lag, which is the one
    /// number that has to stay trustworthy.
    bulk_template: Vec<JsonValue>,
}

impl RequestContext {
    fn bulk_batch(&self, base_seq: u64) -> Vec<JsonValue> {
        self.bulk_template
            .iter()
            .enumerate()
            .map(|(offset, template)| {
                let mut item = template.clone();
                let id = format!("b{}", base_seq + offset as u64);
                item["doc"]["id"] = JsonValue::String(id.clone());
                item["id"] = JsonValue::String(id);
                item
            })
            .collect()
    }
}

/// What a request turned into.
enum Answer {
    Ok {
        /// Documents written, for the bulk per-document figure. One elsewhere.
        docs: u64,
        /// The node's own `took_ms`, where it reports one.
        server_micros: Option<u64>,
    },
    Failed {
        status: Option<u16>,
        message: String,
    },
}

struct Completion {
    workload: Workload,
    /// All three as offsets from the start of the step.
    intended: Duration,
    sent: Duration,
    done: Duration,
    answer: Answer,
}

async fn issue(ctx: &RequestContext, workload: Workload, seq: u64, batch_size: usize) -> Answer {
    let outcome = match workload {
        Workload::Search => {
            let query = format!("bench {}", WORDS[(seq as usize) % WORDS.len()]);
            ctx.client
                .search(&ctx.index, &query, Some(10), None, None, None)
                .await
                .map(|response| Answer::Ok {
                    docs: 1,
                    server_micros: response
                        .get("took_ms")
                        .and_then(|v| v.as_u64())
                        .map(|ms| ms * 1_000),
                })
        }
        Workload::Write => {
            let id = format!("w{seq}");
            let doc = document(&id, seq as usize);
            ctx.client
                .write_document(&ctx.index, &id, &doc, None)
                .await
                .map(|_| Answer::Ok {
                    docs: 1,
                    server_micros: None,
                })
        }
        Workload::Bulk => {
            let batch = ctx.bulk_batch(seq * batch_size as u64);
            ctx.client
                .bulk_index(&ctx.index, &batch)
                .await
                .map(|_| Answer::Ok {
                    docs: batch_size as u64,
                    server_micros: None,
                })
        }
    };

    match outcome {
        Ok(answer) => answer,
        Err(err) => Answer::Failed {
            // The status, not the prose. See `client::HttpFailure` for why the node shedding
            // load and the node being unreachable must not land in one bucket.
            status: failure_status(&err),
            message: format!("{err}"),
        },
    }
}

// ============================================================================
// The scheduler
// ============================================================================

/// What the generator itself did, as opposed to what the node did.
#[derive(Debug, Default, Clone, Copy)]
struct SchedulerStats {
    /// Arrivals the process produced inside the window — dispatched or not.
    offered: u64,
    /// Arrivals dropped because `--max-in-flight` was already reached. Any of these
    /// invalidates the step: the harness, not the node, decided how much load there was.
    dropped: u64,
}

#[allow(clippy::too_many_arguments)]
async fn schedule(
    ctx: Arc<RequestContext>,
    workload: Workload,
    rate: f64,
    arrival: Arrival,
    seed: u64,
    batch_size: usize,
    start: Instant,
    window: Duration,
    in_flight: Arc<Semaphore>,
    outstanding: Arc<AtomicU64>,
    completions: mpsc::UnboundedSender<Completion>,
    requests: tokio::runtime::Handle,
) -> SchedulerStats {
    let mut rng = Rng::new(seed);
    let mut stats = SchedulerStats::default();
    let mut next = Duration::ZERO;
    let mut seq = 0u64;
    let mut since_yield = 0u32;

    loop {
        if next >= window {
            break;
        }

        let target = start + next;
        let now = Instant::now();
        if now < target {
            let gap = target - now;
            if gap > SPIN_SLACK {
                tokio::time::sleep(gap - SPIN_SLACK).await;
            } else {
                // Spinning through the runtime rather than on the CPU, so this thread stays
                // usable. Nothing else is scheduled on it — see `run_step` — so the yield
                // returns immediately and the arrival lands within microseconds.
                tokio::task::yield_now().await;
            }
            since_yield = 0;
            continue;
        }

        // Due. Counted as offered whether or not it can be dispatched — a dropped arrival is
        // load the node was never asked for, and hiding it would turn a harness limit into a
        // node result.
        stats.offered += 1;
        let intended = next;
        next += interarrival(&mut rng, rate, arrival);
        let this_seq = seq;
        seq += 1;

        match Arc::clone(&in_flight).try_acquire_owned() {
            Ok(permit) => {
                outstanding.fetch_add(1, Ordering::Relaxed);
                let ctx = Arc::clone(&ctx);
                let completions = completions.clone();
                let outstanding = Arc::clone(&outstanding);
                requests.spawn(async move {
                    let sent = start.elapsed();
                    let answer = issue(&ctx, workload, this_seq, batch_size).await;
                    let done = start.elapsed();
                    outstanding.fetch_sub(1, Ordering::Relaxed);
                    // A closed receiver means the step already reported; the request was
                    // abandoned and is accounted for as such.
                    let _ = completions.send(Completion {
                        workload,
                        intended,
                        sent,
                        done,
                        answer,
                    });
                    drop(permit);
                });
            }
            Err(_) => stats.dropped += 1,
        }

        since_yield += 1;
        if since_yield >= CATCHUP_YIELD_EVERY {
            since_yield = 0;
            tokio::task::yield_now().await;
        }
    }

    stats
}

// ============================================================================
// Running a step
// ============================================================================

/// One workload's result within a step.
#[derive(Debug)]
pub struct WorkloadReport {
    pub workload: Workload,
    pub offered_rate: f64,
    pub scheduler: SchedulerReport,
    pub samples: Samples,
    pub docs: u64,
    /// The node's own `took_ms`, where it reports one.
    pub server_side: Samples,
}

/// The scheduler's own numbers, flattened for reporting.
#[derive(Debug, Default, Clone, Copy)]
pub struct SchedulerReport {
    pub offered: u64,
    pub dropped: u64,
}

#[derive(Debug)]
pub struct StepReport {
    pub workloads: Vec<WorkloadReport>,
    /// Requests still outstanding when the drain grace expired. Their latency is unknown, so
    /// they are reported rather than folded into a percentile.
    pub abandoned: u64,
    pub in_flight_peak: u64,
    pub wall: Duration,
}

/// Whether a step's numbers mean anything, and about what.
#[derive(Debug, PartialEq)]
pub enum Verdict {
    /// The node took everything offered and answered it.
    Sustained,
    /// The node *deliberately* declined part of the load — admission control, the request
    /// timeout, a rate limiter. This is the capacity answer, not a failure.
    NodeShed { percent: f64 },
    /// Requests failed for a reason that is not the node declining work: a 500, a 404, a
    /// connection that never landed. Held apart from `NodeShed` because it is not a capacity
    /// result at all — something is wrong with the run or with the node.
    NodeErrored { percent: f64 },
    /// Nothing failed, but the answers did not keep up with the offers.
    NodeBehind { achieved: f64, offered: f64 },
    /// The generator could not offer the load it was asked for. Says nothing about the node.
    HarnessLimited { reason: String },
}

impl StepReport {
    pub fn offered(&self) -> u64 {
        self.workloads.iter().map(|w| w.scheduler.offered).sum()
    }

    pub fn dropped(&self) -> u64 {
        self.workloads.iter().map(|w| w.scheduler.dropped).sum()
    }

    pub fn completed(&self) -> u64 {
        self.workloads.iter().map(|w| w.samples.outcomes.ok).sum()
    }

    /// Requests the node turned away on purpose. The capacity signal.
    fn refused(&self) -> u64 {
        self.workloads
            .iter()
            .map(|w| {
                let o = &w.samples.outcomes;
                o.shed + o.timed_out + o.rate_limited
            })
            .sum()
    }

    /// Requests that failed some other way. Not a capacity signal, and reported as such: a
    /// node answering 500s is not a node at its limit.
    fn errored(&self) -> u64 {
        self.workloads
            .iter()
            .map(|w| w.samples.outcomes.other_status + w.samples.outcomes.transport)
            .sum()
    }

    fn worst_lag(&self) -> Duration {
        self.workloads
            .iter()
            .filter_map(|w| w.lag_p99())
            .max()
            .unwrap_or_default()
    }

    /// Read the step, in order of what would invalidate it.
    ///
    /// The harness is checked first and hardest. A number produced by a generator that could
    /// not keep to its own schedule is not a slower version of the truth, it is a measurement
    /// of the generator — and it is the failure mode an open-loop harness is most prone to,
    /// because it shares a machine with the node it is testing.
    pub fn verdict(&self) -> Verdict {
        let dropped = self.dropped();
        if dropped > 0 {
            return Verdict::HarnessLimited {
                reason: format!(
                    "{dropped} arrivals dropped at the --max-in-flight ceiling; the offered \
                     rate was not actually offered"
                ),
            };
        }

        let lag = self.worst_lag();
        if lag > LAG_CEILING {
            return Verdict::HarnessLimited {
                reason: format!(
                    "the generator fell {} behind its own schedule at p99; it could not \
                     produce this arrival rate on this machine",
                    ms(lag.as_micros() as u64)
                ),
            };
        }

        let offered = self.offered();
        if offered > 0 {
            // Deliberate refusal first: on an overloaded node it is both the commonest
            // outcome and the one the run is looking for, and it should not be masked by a
            // handful of stray errors alongside it.
            let refused = self.refused();
            if refused > 0 {
                return Verdict::NodeShed {
                    percent: (refused as f64 / offered as f64) * 100.0,
                };
            }
            let errored = self.errored();
            if errored > 0 {
                return Verdict::NodeErrored {
                    percent: (errored as f64 / offered as f64) * 100.0,
                };
            }
        }

        let secs = self.wall.as_secs_f64();
        let achieved = if secs > 0.0 {
            self.completed() as f64 / secs
        } else {
            0.0
        };
        let offered_rate = if secs > 0.0 {
            offered as f64 / secs
        } else {
            0.0
        };
        // 2% of slack absorbs the requests in flight when the window closed, which are real
        // and are also reported as `abandoned`.
        if offered_rate > 0.0 && achieved < offered_rate * 0.98 {
            return Verdict::NodeBehind {
                achieved,
                offered: offered_rate,
            };
        }

        Verdict::Sustained
    }
}

impl WorkloadReport {
    pub fn name(&self) -> &'static str {
        match self.workload {
            Workload::Search => "searches",
            Workload::Write => "writes",
            Workload::Bulk => "bulk requests",
        }
    }

    fn lag_p99(&self) -> Option<Duration> {
        self.samples.lag_p99().map(Duration::from_micros)
    }
}

/// Run every active workload at one step's rates.
async fn run_step(
    client: Arc<CameoClient>,
    args: &Args,
    open: &OpenLoop,
    step: Step,
) -> Result<StepReport> {
    let ctx = Arc::new(RequestContext {
        client,
        index: args.index.clone(),
        bulk_template: (0..args.batch_size)
            .map(|offset| {
                let id = format!("b{offset}");
                json!({ "id": id, "doc": document(&id, offset) })
            })
            .collect(),
    });

    let in_flight = Arc::new(Semaphore::new(open.max_in_flight));
    let outstanding = Arc::new(AtomicU64::new(0));
    let (tx, rx) = mpsc::unbounded_channel::<Completion>();
    let (stop_tx, stop_rx) = oneshot::channel::<()>();

    let collector = tokio::spawn(collect(rx, stop_rx));

    // A high-water mark for outstanding requests, sampled rather than exact. A pool sitting
    // at its ceiling reads very differently from one that never rose above ten.
    let peak = Arc::new(AtomicU64::new(0));
    let sampler = {
        let outstanding = Arc::clone(&outstanding);
        let peak = Arc::clone(&peak);
        tokio::spawn(async move {
            loop {
                let now = outstanding.load(Ordering::Relaxed);
                peak.fetch_max(now, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
    };

    // Arrival timing gets threads of its own, and this is not a tuning detail.
    //
    // Sharing a runtime with the response handlers means that the busier the node's answers
    // make this process, the later the generator dispatches — so the offered rate quietly
    // falls exactly when the node is under the most load. That is the closed-loop coupling
    // this whole module exists to remove, reintroduced through the back door of the
    // scheduler. Measured on a loopback node at 3,000/s it was worth ~50ms of p99 lag, which
    // the verdict correctly refused to call a node result.
    //
    // One thread per active scheduler, and nothing else runs on them; requests are dispatched
    // onto the main runtime through its handle.
    let active = [Workload::Search, Workload::Write, Workload::Bulk]
        .into_iter()
        .filter(|workload| step.rate_for(args.mode, *workload) > 0.0)
        .count()
        .max(1);
    let arrivals = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(active)
        .thread_name("bench-arrival")
        .enable_all()
        .build()
        .map_err(|e| anyhow::anyhow!("could not build the arrival runtime: {e}"))?;
    let requests = tokio::runtime::Handle::current();

    let start = Instant::now();
    let mut schedulers = Vec::new();
    for (ordinal, workload) in [Workload::Search, Workload::Write, Workload::Bulk]
        .into_iter()
        .enumerate()
    {
        let rate = step.rate_for(args.mode, workload);
        if rate <= 0.0 {
            continue;
        }
        // Each workload gets its own stream, derived from the run seed, so that adding a
        // second workload does not change the arrival times of the first.
        let seed = open
            .seed
            .wrapping_add((ordinal as u64 + 1).wrapping_mul(0x9E37_79B9));
        schedulers.push((
            workload,
            rate,
            arrivals.spawn(schedule(
                Arc::clone(&ctx),
                workload,
                rate,
                open.arrival,
                seed,
                args.batch_size,
                start,
                step.duration,
                Arc::clone(&in_flight),
                Arc::clone(&outstanding),
                tx.clone(),
                requests.clone(),
            )),
        ));
    }
    // The schedulers hold the only senders that matter from here.
    drop(tx);

    let mut scheduler_stats = Vec::new();
    for (workload, rate, handle) in schedulers {
        let stats = handle.await.unwrap_or_default();
        scheduler_stats.push((workload, rate, stats));
    }
    let wall = start.elapsed();
    // Dropping a runtime from inside an async context panics, and every scheduler it held has
    // already returned.
    arrivals.shutdown_background();

    // Let what is in flight finish, but not forever: a node deep in overload may hold
    // requests for minutes, and a harness that waits for them is closed-loop again at the
    // worst possible moment.
    let drain_until = Instant::now() + DRAIN_GRACE;
    while outstanding.load(Ordering::Relaxed) > 0 && Instant::now() < drain_until {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let abandoned = outstanding.load(Ordering::Relaxed);

    sampler.abort();
    let _ = stop_tx.send(());
    let mut collected = collector.await.unwrap_or_default();

    let workloads = scheduler_stats
        .into_iter()
        .map(|(workload, rate, stats)| {
            let bucket = collected.take(workload);
            WorkloadReport {
                workload,
                offered_rate: rate,
                scheduler: SchedulerReport {
                    offered: stats.offered,
                    dropped: stats.dropped,
                },
                samples: bucket.samples,
                docs: bucket.docs,
                server_side: bucket.server_side,
            }
        })
        .collect();

    Ok(StepReport {
        workloads,
        abandoned,
        in_flight_peak: peak.load(Ordering::Relaxed),
        wall,
    })
}

#[derive(Default)]
struct Bucket {
    samples: Samples,
    docs: u64,
    server_side: Samples,
}

#[derive(Default)]
struct Collected {
    search: Bucket,
    write: Bucket,
    bulk: Bucket,
}

impl Collected {
    fn bucket(&mut self, workload: Workload) -> &mut Bucket {
        match workload {
            Workload::Search => &mut self.search,
            Workload::Write => &mut self.write,
            Workload::Bulk => &mut self.bulk,
        }
    }

    fn take(&mut self, workload: Workload) -> Bucket {
        std::mem::take(self.bucket(workload))
    }
}

/// Drain completions into per-workload samples.
///
/// One task doing the accumulation, fed by a channel, so nothing on the request path takes a
/// lock — a mutex around the sample vectors would show up in the very numbers being collected.
async fn collect(
    mut rx: mpsc::UnboundedReceiver<Completion>,
    mut stop: oneshot::Receiver<()>,
) -> Collected {
    let mut collected = Collected::default();
    let accumulate = |collected: &mut Collected, completion: Completion| {
        let bucket = collected.bucket(completion.workload);
        match completion.answer {
            Answer::Ok {
                docs,
                server_micros,
            } => {
                bucket
                    .samples
                    .record_open(completion.intended, completion.sent, completion.done);
                bucket.docs += docs;
                if let Some(micros) = server_micros {
                    bucket.server_side.record(Duration::from_micros(micros));
                }
            }
            Answer::Failed { status, message } => {
                bucket.samples.record_failure(status, message);
            }
        }
    };

    loop {
        tokio::select! {
            // Biased so pending completions are drained before the stop is honoured; a
            // completion already in the channel is a measurement, not a straggler.
            biased;
            received = rx.recv() => match received {
                Some(completion) => accumulate(&mut collected, completion),
                None => break,
            },
            _ = &mut stop => {
                while let Ok(completion) = rx.try_recv() {
                    accumulate(&mut collected, completion);
                }
                break;
            }
        }
    }

    collected
}

/// Run the whole open-loop plan: warmup at the first step's rates, then every step measured.
pub async fn run(
    client: Arc<CameoClient>,
    args: &Args,
    open: &OpenLoop,
) -> Result<Vec<StepReport>> {
    if !args.warmup.is_zero() {
        println!(
            "\nwarming up for {}s (not measured)…",
            args.warmup.as_secs()
        );
        let mut warm = open.steps[0];
        warm.duration = args.warmup;
        run_step(Arc::clone(&client), args, open, warm).await?;
    }

    let mut reports = Vec::new();
    for (ordinal, step) in open.steps.iter().enumerate() {
        if open.steps.len() > 1 {
            println!(
                "\nstep {}/{}: offering {:.0}/s for {}s…",
                ordinal + 1,
                open.steps.len(),
                step.search.max(step.write),
                step.duration.as_secs()
            );
        } else {
            println!("measuring for {}s…", step.duration.as_secs());
        }
        reports.push(run_step(Arc::clone(&client), args, open, *step).await?);
    }
    Ok(reports)
}

// ============================================================================
// Reporting
// ============================================================================

impl Verdict {
    fn line(&self) -> String {
        match self {
            Verdict::Sustained => {
                "sustained — the node took everything offered and answered it".to_string()
            }
            Verdict::NodeShed { percent } => format!(
                "the node declined {percent:.1}% of what was offered. That is the capacity \
                 answer — admission control working, not a harness fault. Read the failure \
                 breakdown: 503 is the concurrency guard refusing cleanly, 408 is the request \
                 timeout abandoning a client whose work may still be running"
            ),
            Verdict::NodeErrored { percent } => format!(
                "{percent:.1}% of requests failed for a reason that is not the node declining \
                 work. Not a capacity result — fix the run or the node before reading anything \
                 else here"
            ),
            Verdict::NodeBehind { achieved, offered } => format!(
                "the node answered {achieved:.0}/s against {offered:.0}/s offered and refused \
                 nothing — the backlog absorbed the difference. Read the per-second series: a \
                 rate that decays through the run is a collapse, not a plateau"
            ),
            Verdict::HarnessLimited { reason } => format!(
                "INVALID as a statement about the node — {reason}. Lower the rate, or move the \
                 generator off this machine"
            ),
        }
    }
}

/// Print every step.
///
/// Takes the reports by value because `Samples::summarize` consumes: percentiles need sorted
/// vectors, and consuming is how that destruction is made obvious rather than surprising.
pub fn print(reports: Vec<StepReport>, open: &OpenLoop) {
    let total = reports.len();
    for (ordinal, report) in reports.into_iter().enumerate() {
        let heading = if total > 1 {
            format!("step {}/{total}", ordinal + 1)
        } else {
            "open-loop run".to_string()
        };

        // Read before the workloads are consumed below.
        let verdict = report.verdict().line();
        let (offered, dropped) = (report.offered(), report.dropped());
        let StepReport {
            workloads,
            abandoned,
            in_flight_peak,
            wall,
            ..
        } = report;

        println!("\n{}", "=".repeat(48));
        println!("{heading}");
        println!("{}", "=".repeat(48));

        for workload in workloads {
            print_workload(workload, wall);
        }

        println!("\n  {:>12}  {}", "arrival", open.arrival.name());
        println!(
            "  {:>12}  {offered} arrivals, {dropped} dropped at the ceiling of {}",
            "offered", open.max_in_flight
        );
        println!("  {:>12}  peak {in_flight_peak} outstanding", "in flight");
        if abandoned > 0 {
            println!(
                "  {:>12}  {abandoned} still unanswered when the {}s drain expired — their \
                 latency is unknown and is in none of the percentiles above",
                "abandoned",
                DRAIN_GRACE.as_secs()
            );
        }
        println!("  {:>12}  {verdict}", "verdict");
    }
}

fn print_workload(workload: WorkloadReport, wall: Duration) {
    if workload.samples.is_empty() {
        return;
    }

    let name = workload.name();
    let label = format!("{name} — offered {:.0}/s", workload.offered_rate);
    let per_second = workload.samples.per_second().to_vec();
    let is_bulk = workload.workload == Workload::Bulk;
    let docs = workload.docs;

    workload.samples.summarize(&label, wall).print();

    if !per_second.is_empty() {
        // The final bucket is usually a partial second, and a short bucket reads as a dip
        // that never happened. Dropped unless it is all there is.
        let keep = if per_second.len() > 1 {
            per_second.len() - 1
        } else {
            1
        };
        let series: Vec<String> = per_second[..keep]
            .iter()
            .take(60)
            .map(u64::to_string)
            .collect();
        println!("  {:>10}  {}", "ok per sec", series.join(" "));
    }

    if is_bulk {
        let secs = wall.as_secs_f64();
        let docs_per_sec = if secs > 0.0 { docs as f64 / secs } else { 0.0 };
        println!(
            "  {:>10}  {docs} documents, {docs_per_sec:.0} docs/s",
            "bulk total"
        );
    }

    if !workload.server_side.is_empty() {
        workload
            .server_side
            .summarize(&format!("{name} — node-reported took_ms"), wall)
            .print();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A uniform process is exactly `1/rate`, which is what makes it the control arm.
    #[test]
    fn uniform_arrivals_are_evenly_spaced() {
        let mut rng = Rng::new(1);
        for _ in 0..100 {
            let gap = interarrival(&mut rng, 1_000.0, Arrival::Uniform);
            assert_eq!(gap, Duration::from_micros(1_000));
        }
    }

    /// An exponential distribution has mean `1/rate` and standard deviation equal to its mean.
    /// Both are checked: a generator that produced the right average with the wrong spread
    /// would look correct and would never produce the bursts the mode exists for.
    #[test]
    fn poisson_arrivals_have_the_right_mean_and_spread() {
        let mut rng = Rng::new(42);
        let rate = 1_000.0;
        let samples: Vec<f64> = (0..200_000)
            .map(|_| interarrival(&mut rng, rate, Arrival::Poisson).as_secs_f64())
            .collect();

        let mean = samples.iter().sum::<f64>() / samples.len() as f64;
        let variance =
            samples.iter().map(|s| (s - mean).powi(2)).sum::<f64>() / samples.len() as f64;
        let expected = 1.0 / rate;

        assert!(
            (mean - expected).abs() < expected * 0.02,
            "mean {mean} against {expected}"
        );
        assert!(
            (variance.sqrt() - expected).abs() < expected * 0.05,
            "sd {} against {expected}",
            variance.sqrt()
        );
    }

    /// The seed is what makes two runs offer the same load, which is what makes them
    /// comparable at all.
    #[test]
    fn the_same_seed_produces_the_same_arrival_stream() {
        let gaps = |seed| {
            let mut rng = Rng::new(seed);
            (0..64)
                .map(|_| interarrival(&mut rng, 500.0, Arrival::Poisson))
                .collect::<Vec<_>>()
        };
        assert_eq!(gaps(7), gaps(7));
        assert_ne!(gaps(7), gaps(8));
    }

    fn report(workloads: Vec<WorkloadReport>, abandoned: u64, wall_secs: u64) -> StepReport {
        StepReport {
            workloads,
            abandoned,
            in_flight_peak: 0,
            wall: Duration::from_secs(wall_secs),
        }
    }

    fn workload(
        offered: u64,
        dropped: u64,
        ok: u64,
        failed_503: u64,
        lag: Duration,
    ) -> WorkloadReport {
        let mut samples = Samples::default();
        for i in 0..ok {
            let intended = Duration::from_micros(i);
            samples.record_open(intended, intended + lag, intended + lag);
        }
        for _ in 0..failed_503 {
            samples.record_failure(Some(503), "shed");
        }
        WorkloadReport {
            workload: Workload::Search,
            offered_rate: 1_000.0,
            scheduler: SchedulerReport { offered, dropped },
            samples,
            docs: ok,
            server_side: Samples::default(),
        }
    }

    /// The harness is judged before the node, and a dropped arrival is disqualifying however
    /// good the rest of the numbers look — the load was never offered, so nothing about the
    /// node was established.
    #[test]
    fn dropped_arrivals_invalidate_the_step() {
        let step = report(vec![workload(1_000, 3, 997, 0, Duration::ZERO)], 0, 1);
        match step.verdict() {
            Verdict::HarnessLimited { reason } => assert!(reason.contains("dropped"), "{reason}"),
            other => panic!("expected a harness limit, got {other:?}"),
        }
    }

    /// A generator behind its own schedule is measuring itself, even with nothing dropped and
    /// every request answered.
    #[test]
    fn a_lagging_generator_invalidates_the_step() {
        let step = report(
            vec![workload(1_000, 0, 1_000, 0, Duration::from_millis(50))],
            0,
            1,
        );
        match step.verdict() {
            Verdict::HarnessLimited { reason } => assert!(reason.contains("behind"), "{reason}"),
            other => panic!("expected a harness limit, got {other:?}"),
        }
    }

    /// Shedding is the capacity answer, and it is reported as a share of what was offered
    /// rather than of what was answered — the denominator a caller cares about is what it asked.
    #[test]
    fn a_shedding_node_reports_the_share_it_refused() {
        let step = report(vec![workload(1_000, 0, 900, 100, Duration::ZERO)], 0, 1);
        match step.verdict() {
            Verdict::NodeShed { percent } => assert!((percent - 10.0).abs() < 0.001, "{percent}"),
            other => panic!("expected a shed verdict, got {other:?}"),
        }
    }

    /// A transport blip is not a capacity result, and must not be reported as the node
    /// declining work — that reading would turn an unreachable node into a busy one.
    #[test]
    fn a_failure_that_is_not_a_refusal_is_reported_apart_from_shedding() {
        let mut samples = Samples::default();
        samples.record_open(Duration::ZERO, Duration::ZERO, Duration::ZERO);
        samples.record_failure(None, "connection refused");
        let errored = WorkloadReport {
            workload: Workload::Search,
            offered_rate: 1_000.0,
            scheduler: SchedulerReport {
                offered: 100,
                dropped: 0,
            },
            samples,
            docs: 1,
            server_side: Samples::default(),
        };
        match report(vec![errored], 0, 1).verdict() {
            Verdict::NodeErrored { percent } => assert!((percent - 1.0).abs() < 0.001, "{percent}"),
            other => panic!("expected an error verdict, got {other:?}"),
        }
    }

    /// And where both occur, shedding wins the headline: on an overloaded node it is the
    /// commoner outcome and the one the run is asking about.
    #[test]
    fn shedding_outranks_a_stray_error_alongside_it() {
        let mut samples = Samples::default();
        for _ in 0..50 {
            samples.record_failure(Some(503), "shed");
        }
        samples.record_failure(Some(500), "server");
        let both = WorkloadReport {
            workload: Workload::Search,
            offered_rate: 1_000.0,
            scheduler: SchedulerReport {
                offered: 100,
                dropped: 0,
            },
            samples,
            docs: 0,
            server_side: Samples::default(),
        };
        match report(vec![both], 0, 1).verdict() {
            Verdict::NodeShed { percent } => assert!((percent - 50.0).abs() < 0.001, "{percent}"),
            other => panic!("expected a shed verdict, got {other:?}"),
        }
    }

    /// A node can fall behind without refusing anything — it just answers more slowly than it
    /// is asked, and the backlog grows. That is the metastable case, and it has to be
    /// distinguishable from a clean run.
    #[test]
    fn a_node_that_answers_too_slowly_is_reported_as_behind() {
        let step = report(vec![workload(1_000, 0, 500, 0, Duration::ZERO)], 500, 1);
        match step.verdict() {
            Verdict::NodeBehind { achieved, offered } => {
                assert_eq!(achieved, 500.0);
                assert_eq!(offered, 1_000.0);
            }
            other => panic!("expected a behind verdict, got {other:?}"),
        }
    }

    #[test]
    fn a_clean_step_is_sustained() {
        let step = report(vec![workload(1_000, 0, 1_000, 0, Duration::ZERO)], 0, 1);
        assert_eq!(step.verdict(), Verdict::Sustained);
    }
}
