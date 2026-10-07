//! Admission and load accounting — what the node lets in, and the numbers it decides on.
//!
//! One subsystem, and it is here rather than in the dispatch core because its correctness is a
//! property of the *set*, not of any one type. The depth a lane reports, the reserve the
//! histogram predicts, the budget the send path refuses against and the slot a request holds
//! are four views of one count, and they have to stay consistent with each other across every
//! path that touches them — including the paths that end in a cancellation.
//!
//! F8 is why that is worth a file. The mailbox lane's counter was decremented after an `await`,
//! so a request cancelled mid-flight never gave its slot back; the leak accumulated until an
//! *idle* node read `mailbox_depth 63`, believed itself saturated and refused everything, for
//! good. Nothing about the type was wrong in isolation — the invariant was "every increment has
//! exactly one matching decrement on every exit path", and that invariant had no home. It has
//! one now: [`MailboxSlot`] is the RAII form of it, and it lives beside the counters it guards.
//!
//! The pieces, in the order a request meets them:
//!
//! - [`OpClass`] — which estimate a request is measured against. `Any` is the door's view,
//!   before the body has been read.
//! - [`ServiceHistogram`] — the decaying service-time distribution, per class, that turns
//!   observed work into a predicted wait.
//! - [`QueueLoad`] — the shared backlog estimate the HTTP front door and the send path both
//!   refuse against, so two layers cannot disagree about whether the node is full.
//! - [`MailboxLane`] and [`MailboxSlot`] — the actor mailbox's own depth, and the guard that
//!   makes its accounting exit-path-proof.
//! - [`WorkerCounters`] and [`DispatchCounters`] — per-worker and pool-wide totals.
//! - [`WorkerPoolReport`] and friends — the serialisable face of all of it, for
//!   `GET /_admin/workers`. They are the evidence an operator reads, so they live with the
//!   counters they report rather than with the endpoint that serves them.

use super::*;

use std::sync::{
    Arc,
    atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering as AtomicOrdering},
};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

/// The kind of work a [`ClientOp`] implies, for the admission-time service estimate.
///
/// One EWMA for every op would blend a point search's milliseconds with a bulk write's, and
/// the reserve computed from the blend is wrong for both — over-reserving the cheap op and
/// under-reserving the expensive one. `Any` is the door's view: the admission guard runs
/// before the body is read, so it cannot know which the request is and uses the blended
/// estimate — the honest answer to "what does a request cost" before the request is known.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OpClass {
    /// Op not yet known (the admission guard runs before parsing) or outside the split.
    Any,
    /// `Search`, `Stream` — work that lands on the read pool.
    Read,
    /// `Write`, `Delete` — work that lands on a shard's writer thread.
    Write,
    /// `BulkWrite`, `BulkDelete` — a fan-out over writer threads and peers whose service is
    /// orders above a single write's. Folded into the write estimate it would poison it:
    /// one 300ms bulk sample outweighs a hundred 2ms writes, and single writes would be
    /// reserved — and refused — against a cost they never pay.
    Bulk,
}

impl OpClass {
    pub(super) fn of(op: &ClientOp) -> Self {
        match op {
            ClientOp::Search { .. } => OpClass::Read,
            ClientOp::Write { .. } | ClientOp::Delete { .. } => OpClass::Write,
            ClientOp::BulkWrite { .. } | ClientOp::BulkDelete { .. } => OpClass::Bulk,
            _ => OpClass::Any,
        }
    }
}

/// Per-worker atomic counters — updated on the send and receive hot paths.
#[derive(Debug)]
pub(super) struct WorkerCounters {
    /// Jobs sitting in this worker's mpsc channel, not yet started. Incremented on send,
    /// decremented when the worker picks the job up — so it is queueing against
    /// `queue_capacity`, and `in_flight` is the work actually running.
    pub(super) queue_depth: AtomicUsize,
    /// Operations this worker has started and not yet answered, bounded by the worker's
    /// in-flight limit. The pair (`queue_depth`, `in_flight`) separates "waiting for a
    /// worker" from "waiting on a shard" — a deep queue beside a low `in_flight` means the
    /// limit is too tight, the reverse means the shards are the constraint.
    pub(super) in_flight: AtomicUsize,
    /// Total jobs completed by this worker since startup.
    pub(super) jobs_completed: AtomicU64,
    /// Core this worker is actually pinned to, or [`UNPINNED`]. Written once by the worker
    /// thread itself, because only it can find out whether the pin was accepted.
    pub(super) pinned_core: AtomicI64,
}

impl Default for WorkerCounters {
    fn default() -> Self {
        Self {
            queue_depth: AtomicUsize::new(0),
            in_flight: AtomicUsize::new(0),
            jobs_completed: AtomicU64::new(0),
            pinned_core: AtomicI64::new(UNPINNED),
        }
    }
}

/// Number of buckets: 16 linear (0..15µs) then four per octave up to 2^63µs.
pub(super) const SERVICE_BUCKETS: usize = 256;

/// How long one generation of the histogram collects before it is rotated out.
///
/// The EWMA this sits beside decays continuously; a histogram does not, so without rotation it
/// would answer for every sample the node ever took and could not track load that changed. Two
/// generations: one collecting, one complete and readable.
pub(super) const SERVICE_WINDOW: Duration = Duration::from_secs(2);

/// Quantile reported for monitoring. Not what admission predicts against — see
/// [`SERVICE_ADMISSION_SIGMAS`].
pub(super) const SERVICE_REPORTED_QUANTILE: f64 = 0.90;

/// How many standard deviations of the *queue's* service sum the wait prediction allows for.
///
/// A queue of `d` jobs has a wait whose mean is `d × µ` and whose standard deviation is `σ√d`,
/// because variances add and deviations do not. So the honest bound on what a job joining at
/// depth `d` will wait is `d·µ + k·σ√d`, and `k` is a confidence level rather than a fudge: at
/// 2 it covers about 98% of arrivals on a normal sum, which is the right shape for a deadline.
///
/// The first cut multiplied a per-request p90 by the depth instead. That is the same mistake in
/// reverse — it grows the spread as `d` rather than `√d`, so it over-predicts deep queues and
/// admits a shallower one than the budget can actually carry. It worked (goodput went flat), and
/// it left throughput on the table and a tail of timeouts at the far end; this is the form that
/// has both.
pub(super) const SERVICE_ADMISSION_SIGMAS: f64 = 3.0;

/// A decaying log-bucketed histogram of service times.
///
/// Recording is one `fetch_add` on an atomic counter — cheaper than the `fetch_update` CAS loop
/// the EWMA beside it uses, and it never retries under contention. Reading a quantile means
/// walking the buckets, which is far too much for an admission check that runs on every request,
/// so the quantile is computed once per rotation and cached in a single atomic. The admission
/// path therefore stays one load, which is what F7's door was built to cost.
pub(super) struct ServiceHistogram {
    /// Two generations. `active` selects the one being recorded into; the other is complete and
    /// is what a quantile is computed from.
    pub(super) generations: [Box<[AtomicU64]>; 2],
    pub(super) active: AtomicUsize,
    /// Microseconds since `base`, when the active generation started.
    pub(super) rotated_at_us: AtomicU64,
    pub(super) base: Instant,
    /// Last computed quantile, in microseconds. Zero until a full generation has been seen.
    /// Reported rather than predicted against.
    pub(super) cached_us: AtomicU64,
    /// Mean and standard deviation of the closed generation, in microseconds. The pair the wait
    /// prediction is built from; zero until a full generation has been seen.
    pub(super) cached_mean_us: AtomicU64,
    pub(super) cached_sigma_us: AtomicU64,
}

impl ServiceHistogram {
    pub(super) fn new() -> Self {
        let generation = || {
            (0..SERVICE_BUCKETS)
                .map(|_| AtomicU64::new(0))
                .collect::<Box<[_]>>()
        };
        Self {
            generations: [generation(), generation()],
            active: AtomicUsize::new(0),
            rotated_at_us: AtomicU64::new(0),
            base: Instant::now(),
            cached_us: AtomicU64::new(0),
            cached_mean_us: AtomicU64::new(0),
            cached_sigma_us: AtomicU64::new(0),
        }
    }

    /// Bucket a sample falls in: linear below 16µs, then four buckets per octave, so the
    /// quantile is never more than ~25% above the value it stands for.
    pub(super) fn bucket_of(sample_us: u64) -> usize {
        if sample_us < 16 {
            return sample_us as usize;
        }
        let exponent = 63 - sample_us.leading_zeros() as usize;
        let sub = ((sample_us >> (exponent - 2)) & 0b11) as usize;
        (16 + (exponent - 4) * 4 + sub).min(SERVICE_BUCKETS - 1)
    }

    /// The upper edge of a bucket — what a quantile landing in it reports, so the answer errs
    /// high rather than low. Under-reporting a service time admits work that cannot finish.
    pub(super) fn bucket_upper_us(bucket: usize) -> u64 {
        if bucket < 16 {
            return bucket as u64;
        }
        let exponent = 4 + (bucket - 16) / 4;
        let sub = ((bucket - 16) % 4) as u64;
        (5 + sub) << (exponent - 2)
    }

    pub(super) fn record(&self, sample: Duration) {
        let index = self.active.load(AtomicOrdering::Relaxed) & 1;
        self.generations[index][Self::bucket_of(sample.as_micros() as u64)]
            .fetch_add(1, AtomicOrdering::Relaxed);
        self.maybe_rotate();
    }

    /// Rotate if the window has elapsed, and fold the generation that just closed into the
    /// cached quantile. Exactly one caller wins the swap; the rest return immediately.
    pub(super) fn maybe_rotate(&self) {
        let now_us = self.base.elapsed().as_micros() as u64;
        let started = self.rotated_at_us.load(AtomicOrdering::Relaxed);
        if now_us.saturating_sub(started) < SERVICE_WINDOW.as_micros() as u64 {
            return;
        }
        if self
            .rotated_at_us
            .compare_exchange(
                started,
                now_us,
                AtomicOrdering::Relaxed,
                AtomicOrdering::Relaxed,
            )
            .is_err()
        {
            return;
        }
        let closing = self.active.fetch_xor(1, AtomicOrdering::Relaxed) & 1;
        if let Some(value) = self.quantile_of(closing, SERVICE_REPORTED_QUANTILE) {
            self.cached_us.store(value, AtomicOrdering::Relaxed);
        }
        if let Some((mean_us, sigma_us)) = self.moments_of(closing) {
            self.cached_mean_us.store(mean_us, AtomicOrdering::Relaxed);
            self.cached_sigma_us
                .store(sigma_us, AtomicOrdering::Relaxed);
        }
        // Zero it so it is clean when it becomes active again one window from now.
        for slot in self.generations[closing].iter() {
            slot.store(0, AtomicOrdering::Relaxed);
        }
    }

    /// `None` when the generation holds no samples, which is what keeps a quiet window from
    /// resetting the estimate to zero and admitting everything.
    pub(super) fn quantile_of(&self, generation: usize, quantile: f64) -> Option<u64> {
        let counts = &self.generations[generation];
        let total: u64 = counts.iter().map(|c| c.load(AtomicOrdering::Relaxed)).sum();
        if total == 0 {
            return None;
        }
        let target = ((total as f64) * quantile).ceil() as u64;
        let mut seen = 0u64;
        for (bucket, count) in counts.iter().enumerate() {
            seen += count.load(AtomicOrdering::Relaxed);
            if seen >= target {
                return Some(Self::bucket_upper_us(bucket));
            }
        }
        None
    }

    /// Mean and standard deviation of one generation, in microseconds.
    ///
    /// Accumulated in `f64`: a bucket's upper edge squared overflows `u64` at the top of the
    /// range, and this runs once per rotation rather than per request, so the cost is irrelevant.
    pub(super) fn moments_of(&self, generation: usize) -> Option<(u64, u64)> {
        let counts = &self.generations[generation];
        let (mut n, mut sum, mut sum_sq) = (0f64, 0f64, 0f64);
        for (bucket, count) in counts.iter().enumerate() {
            let c = count.load(AtomicOrdering::Relaxed) as f64;
            if c == 0.0 {
                continue;
            }
            let value = Self::bucket_upper_us(bucket) as f64;
            n += c;
            sum += c * value;
            sum_sq += c * value * value;
        }
        if n == 0.0 {
            return None;
        }
        let mean = sum / n;
        // Clamped at zero: floating-point cancellation can make this very slightly negative
        // when every sample landed in one bucket, and a NaN here would disable the gate.
        let variance = (sum_sq / n - mean * mean).max(0.0);
        Some((mean as u64, variance.sqrt() as u64))
    }

    /// The cached mean and standard deviation, or `None` before a full window has closed.
    pub(super) fn moments(&self) -> Option<(u64, u64)> {
        let mean = self.cached_mean_us.load(AtomicOrdering::Relaxed);
        (mean != 0).then(|| (mean, self.cached_sigma_us.load(AtomicOrdering::Relaxed)))
    }

    /// The cached reported quantile in microseconds, or zero before a full window has closed.
    pub(super) fn estimate_us(&self) -> u64 {
        self.cached_us.load(AtomicOrdering::Relaxed)
    }
}

impl std::fmt::Debug for ServiceHistogram {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServiceHistogram")
            .field("estimate_us", &self.estimate_us())
            .finish()
    }
}

/// Dispatch-level counters across the entire worker pool.
#[derive(Debug)]
pub(super) struct DispatchCounters {
    /// Jobs sent directly to the affinity-assigned worker.
    pub(super) affine_sends: AtomicU64,
    /// Jobs where the affinity-assigned worker was full and fell through to a neighbor.
    pub(super) affine_full_fallbacks: AtomicU64,
    /// Jobs sent via round-robin (no affinity hint or affine dispatch disabled).
    pub(super) round_robin_sends: AtomicU64,
    /// Jobs that fell all the way back to the actor mailbox (all workers full/closed).
    pub(super) actor_mailbox_fallbacks: AtomicU64,
    /// Jobs refused at dequeue because the budget left could not cover the work.
    ///
    /// The shed is otherwise invisible — the work never runs, so it lands in no latency sample
    /// and no `jobs_completed` tally, and a node shedding hard reads as one that is idle.
    pub(super) abandoned: AtomicU64,
    /// Requests refused before they were queued, because the backlog already in front of them
    /// could not clear inside their budget.
    ///
    /// Read beside `abandoned`, which is the same decision taken too late. Once the gate is
    /// working this one carries the refusals and `abandoned` becomes the measure of how often
    /// the prediction was wrong — so the split between them, not either count alone, is what
    /// says whether admission control is doing its job.
    pub(super) refused_at_admission: AtomicU64,
    /// Jobs that left the pool without producing an answer — the future was dropped, or it
    /// panicked, rather than returning.
    ///
    /// Zero is the expected reading, and a non-zero one is a defect report: a job that reaches
    /// a worker either answers or is counted here. It exists because the alternative to
    /// counting these is what [OB14] did, which is to lose a pool slot per occurrence with
    /// nothing anywhere saying so until the node stopped serving. The gauges below it are
    /// restored either way; this is the count that makes the silence audible.
    ///
    /// [OB14]: the shard-writer deadlock found by the first M6 arm, 2026-09-25.
    pub(super) jobs_dropped: AtomicU64,
    /// Jobs anywhere in the pool — queued or running — across all workers.
    ///
    /// This is the depth [`QueueLoad`] predicts wait from. It is one atomic rather than a sum
    /// over the per-worker `queue_depth` + `in_flight` pairs because the admission guard reads
    /// it on every request and the sum is information this counter already carries — the
    /// per-worker gauges stay, but their job is the `/_admin/workers` report, not the gate.
    pub(super) outstanding: AtomicUsize,
    /// Exponentially-weighted mean of how long a job takes once admitted, in microseconds —
    /// every op class folded together.
    ///
    /// Dequeue-to-answer, so it covers everything downstream — the shard hop, the read-pool
    /// queue and the search itself — which is exactly what a job needs to have left when it is
    /// admitted. Measured rather than configured because it moves with index size, shard count
    /// and load, and a number an operator has to keep in step with those is one that will be
    /// wrong.
    ///
    /// This is the blend the admission door refuses against: the guard runs before the body
    /// is read, so the op's class is not yet knowable there. The two per-class estimates
    /// beside it are what a dequeue or dispatch check reserves against, since there the op
    /// is in hand — see [`OpClass`].
    ///
    /// Starts at zero, so a node under no load admits everything and the estimate only becomes
    /// restrictive once there is evidence to be restrictive about. It cannot go stale while
    /// shedding: a queue that drains admits jobs, and those jobs update it.
    pub(super) service_ewma_us: AtomicU64,
    /// Dequeue-to-answer EWMA for reads (`Search`, `Stream`). See `service_ewma_us`.
    pub(super) service_ewma_read_us: AtomicU64,
    /// Dequeue-to-answer EWMA for writes (`Write`, `Delete`). See `service_ewma_us`.
    pub(super) service_ewma_write_us: AtomicU64,
    /// Dequeue-to-answer EWMA for bulk ops (`BulkWrite`, `BulkDelete`). See `OpClass::Bulk`.
    pub(super) service_ewma_bulk_us: AtomicU64,
    /// Dequeue-to-answer distribution, for lanes that admit against a tail rather than a mean.
    /// Fed by the same samples as the EWMAs above; read only where `tail_aware` is set.
    pub(super) service_hist: ServiceHistogram,
}

impl Default for DispatchCounters {
    fn default() -> Self {
        Self {
            affine_sends: AtomicU64::new(0),
            affine_full_fallbacks: AtomicU64::new(0),
            round_robin_sends: AtomicU64::new(0),
            actor_mailbox_fallbacks: AtomicU64::new(0),
            abandoned: AtomicU64::new(0),
            refused_at_admission: AtomicU64::new(0),
            jobs_dropped: AtomicU64::new(0),
            outstanding: AtomicUsize::new(0),
            service_ewma_us: AtomicU64::new(0),
            service_ewma_read_us: AtomicU64::new(0),
            service_ewma_write_us: AtomicU64::new(0),
            service_ewma_bulk_us: AtomicU64::new(0),
            service_hist: ServiceHistogram::new(),
        }
    }
}

impl DispatchCounters {
    /// Fold one dequeue-to-answer sample into the estimates: the blend, and the class the job
    /// belonged to when it has one.
    pub(super) fn record_service(&self, class: OpClass, sample: Duration) {
        let sample_us = sample.as_micros() as u64;
        self.service_hist.record(sample);
        Self::fold_ewma(&self.service_ewma_us, sample_us);
        match class {
            OpClass::Read => Self::fold_ewma(&self.service_ewma_read_us, sample_us),
            OpClass::Write => Self::fold_ewma(&self.service_ewma_write_us, sample_us),
            OpClass::Bulk => Self::fold_ewma(&self.service_ewma_bulk_us, sample_us),
            OpClass::Any => {}
        }
    }

    /// 1/8 weight — slow enough not to chase a single slow query, fast enough to track a node
    /// whose load has changed.
    ///
    /// Read-modify-write under `fetch_update` rather than load-then-store: every worker folds
    /// into these values, and a lost update is a sample the estimate never saw.
    pub(super) fn fold_ewma(slot: &AtomicU64, sample_us: u64) {
        let _ = slot.fetch_update(
            AtomicOrdering::Relaxed,
            AtomicOrdering::Relaxed,
            |previous| {
                Some(if previous == 0 {
                    sample_us
                } else {
                    (previous.saturating_mul(7).saturating_add(sample_us)) / 8
                })
            },
        );
    }

    /// A job left the pool — refused at dequeue, or answered. Saturating rather than
    /// `fetch_sub`: an increment that was somehow missed (a sender that is not `try_send`,
    /// which is what the tests use) must underflow to zero rather than wrap to `usize::MAX`
    /// and wedge the gate into refusing everything.
    pub(super) fn job_left_pool(&self) {
        let _ =
            self.outstanding
                .fetch_update(AtomicOrdering::Relaxed, AtomicOrdering::Relaxed, |v| {
                    Some(v.saturating_sub(1))
                });
    }

    /// The estimate a reserve should be computed from for `class`: the class's own EWMA, or
    /// the blend when the class has no samples yet — a node that has only ever served searches
    /// still knows what *a* job costs, and zero would reserve nothing at all.
    pub(super) fn service_estimate_for(&self, class: OpClass) -> u64 {
        let class_estimate = match class {
            OpClass::Read => self.service_ewma_read_us.load(AtomicOrdering::Relaxed),
            OpClass::Write => self.service_ewma_write_us.load(AtomicOrdering::Relaxed),
            OpClass::Bulk => self.service_ewma_bulk_us.load(AtomicOrdering::Relaxed),
            OpClass::Any => 0,
        };
        if class_estimate != 0 {
            class_estimate
        } else {
            self.service_ewma_us.load(AtomicOrdering::Relaxed)
        }
    }

    /// What a job should still have left to be worth admitting, given the budget it is measured
    /// against.
    ///
    /// Twice the estimate, so the margin survives the spread rather than only the mean. At one
    /// times the estimate the queue settles exactly on the deadline and about half of what is
    /// admitted still misses it — which is the state this whole check exists to leave.
    ///
    /// **Capped at half the budget, and that cap is load-bearing rather than tidy.** Uncapped,
    /// an estimate above half the budget makes the reserve exceed the budget outright, so every
    /// job is refused — including one that has waited no time at all. Nothing then completes,
    /// no sample ever updates the estimate, and the node refuses everything forever: the same
    /// metastable shape F7 is about, reintroduced by its own fix. The cap guarantees a freshly
    /// arrived job is always admitted, which guarantees the estimate keeps being measured.
    pub(super) fn service_reserve_for(&self, class: OpClass, budget: Duration) -> Duration {
        let reserve = Duration::from_micros(self.service_estimate_for(class).saturating_mul(2));
        reserve.min(budget / 2)
    }
}

/// The node's own estimate of how long a request arriving right now would wait before it starts.
///
/// [`OrchestratorError::ReadDeadlineExpired`] refuses work that cannot meet its deadline, but
/// only once a worker has reached it — after the request has already paid for decompression,
/// the body limit, a concurrency permit, JSON parsing, a job allocation and a channel round
/// trip. Under overload nearly every request pays that and is then refused, and it is where
/// most of the gap between the ~555/s the node serves and the ~730/s it is capable of goes
/// (ROADMAP F7).
///
/// Admission counts requests, not the work they imply: `max_concurrent_requests` permits at
/// 3,000 against ~730 searches/s is about four seconds of backlog against a one-second budget.
/// This turns the gauges the pool already keeps into the number the semaphore is missing, so
/// the same refusal can be made at the door for the price of a few atomic loads.
///
/// Nothing new is measured. `outstanding` is maintained on the dispatch path already — one
/// increment where a send lands and one decrement where a job leaves the pool — and the
/// service estimate is [`DispatchCounters::service_ewma_us`].
pub(crate) struct QueueLoad {
    pub(super) dispatch_stats: Arc<DispatchCounters>,
    /// Jobs the whole pool can have running at once — `worker_count × in-flight limit`. The
    /// divisor in Little's law, and the depth below which the gate never refuses.
    pub(super) width: usize,
    /// Whether the wait prediction uses a measured tail rather than the mean. See
    /// [`QueueLoad::tail_aware`].
    pub(super) tail_aware: bool,
    /// The node's request timeout. `None` disables the gate, matching a node whose timeout is
    /// disabled: there is no deadline to predict against.
    pub(super) budget: Option<Duration>,
}

impl QueueLoad {
    pub(super) fn new(
        dispatch_stats: Arc<DispatchCounters>,
        width: usize,
        budget: Option<Duration>,
    ) -> Self {
        Self {
            dispatch_stats,
            width: width.max(1),
            budget,
            tail_aware: false,
        }
    }

    /// Predict against a measured tail of the service distribution rather than its mean.
    ///
    /// Set for the mailbox lane and deliberately **not** for the worker pool. The pool's
    /// behaviour under overload is the measured result F7 recorded, and changing what it admits
    /// on would invalidate those arms without a run of its own — worth doing, separately, with
    /// its own before and after.
    pub(super) fn tail_aware(mut self) -> Self {
        self.tail_aware = true;
        self
    }

    /// The service figure the wait prediction multiplies out.
    ///
    /// A mean says what a typical request costs; admission needs to know whether the *last*
    /// request in the queue it is about to join will still make its deadline, and that is a
    /// question about the slow ones. Falls back to the mean until a full window has closed, so
    /// a node that has just started admits on the same basis it always did.
    pub(super) fn admission_service_us(&self) -> u64 {
        if self.tail_aware {
            let tail = self.dispatch_stats.service_hist.estimate_us();
            if tail != 0 {
                return tail;
            }
        }
        self.dispatch_stats
            .service_ewma_us
            .load(AtomicOrdering::Relaxed)
    }

    /// Jobs queued or running across the pool — everything a new arrival waits behind.
    pub(crate) fn depth(&self) -> usize {
        self.dispatch_stats
            .outstanding
            .load(AtomicOrdering::Relaxed)
    }

    /// Little's law over the gauges above: work ahead, divided by how much of it runs at once,
    /// times what one job costs.
    ///
    /// `div_ceil` because a partly-filled round still has to finish before the next one starts,
    /// and `saturating_sub` because a pool with a free slot imposes no wait at all.
    pub(super) fn predicted_wait_at(&self, depth: usize) -> Duration {
        let rounds = depth.saturating_sub(self.width).div_ceil(self.width) as u64;
        if self.tail_aware
            && let Some((mean_us, sigma_us)) = self.dispatch_stats.service_hist.moments()
        {
            // d·µ + k·σ√d — the wait ahead is a *sum* of service times, so its mean scales
            // with the depth and its spread only with the root of it.
            let rounds_f = rounds as f64;
            let predicted = mean_us as f64 * rounds_f
                + SERVICE_ADMISSION_SIGMAS * sigma_us as f64 * rounds_f.sqrt();
            return Duration::from_micros(predicted as u64);
        }
        Duration::from_micros(self.admission_service_us().saturating_mul(rounds))
    }

    /// What a request arriving now would wait before a worker starts it.
    pub(crate) fn predicted_wait(&self) -> Duration {
        self.predicted_wait_at(self.depth())
    }

    /// The part of the request budget not yet spent — the full timeout minus whatever the
    /// request already cost being received, parsed and routed. `None` when the gate is off
    /// (no configured deadline to measure against).
    ///
    /// The same quantity the dequeue check computes as `budget − waited`: a request admitted
    /// here and judged again at a worker is judged against one deadline either way.
    pub(super) fn remaining_budget(&self) -> Option<Duration> {
        self.budget
            .map(|b| b.saturating_sub(request_started_at().elapsed()))
    }

    /// Whether a request of `class` arriving now should be refused rather than queued, and
    /// the wait that decided it.
    ///
    /// **A pool with a free slot always admits, whatever the estimate says.** That is not an
    /// optimisation, it is the invariant that keeps this from becoming the failure it prevents:
    /// the estimate is only updated by jobs that complete, so a gate that can refuse an arrival
    /// into an empty pool can stop every job, stop every sample, and refuse forever on a number
    /// nothing will ever correct. F7's reserve had exactly this shape before it was capped —
    /// see [`DispatchCounters::service_reserve_for`] — and it is the same mistake one layer out.
    pub(crate) fn would_refuse(&self, class: OpClass) -> Option<Duration> {
        let remaining = self.remaining_budget()?;
        let depth = self.depth();
        if depth < self.width {
            return None;
        }
        let predicted = self.predicted_wait_at(depth);
        // The same margin the dequeue check reserves, and the door has to apply it too. A
        // weaker door was measured — refusing only at `predicted > budget`, on the theory that
        // the door should catch the certainly-doomed and leave anything marginal to the worker
        // — and it was 20% slower (424/s against 521/s at 1,000/s offered), because letting
        // the queue grow past the point the worker will accept just means refusing the same
        // requests later, after they have been queued rather than before.
        let reserve = self.dispatch_stats.service_reserve_for(class, remaining);
        (predicted + reserve > remaining).then_some(predicted)
    }

    /// The budget refusals are measured against, for the error a refusal answers with.
    pub(super) fn budget(&self) -> Option<Duration> {
        self.budget
    }

    /// Seconds until the refused backlog is predicted to clear — the `Retry-After` a refusal
    /// should carry. The number the refusal was made on, so the answer is the advice rather
    /// than a fixed delay that could send a client back into the same backlog.
    pub(crate) fn retry_after_secs(&self, predicted: Duration) -> u64 {
        (predicted.as_millis() as u64).div_ceil(1000).max(1)
    }

    /// Count a request refused at the door. See [`DispatchCounters::refused_at_admission`].
    pub(super) fn record_refused(&self) {
        self.dispatch_stats
            .refused_at_admission
            .fetch_add(1, AtomicOrdering::Relaxed);
    }

    /// Build the error a refused request answers with, and count it.
    ///
    /// `pub(crate)`: the HTTP front door refuses on this same number, so the error text and the
    /// counter have to come from here rather than be spelled out a second time at the edge.
    pub(crate) fn refuse(&self, predicted: Duration) -> OrchestratorError {
        self.record_refused();
        OrchestratorError::Overloaded {
            predicted_wait_ms: predicted.as_millis() as u64,
            budget_ms: self.budget.unwrap_or_default().as_millis() as u64,
        }
    }
}

/// A shared handle to the actor-mailbox lane's counters.
///
/// Both ends need the same instance: [`RouterActor`] predicts and refuses against it, and
/// [`NodeOrchestrator`] folds each op's *true* dequeue-to-answer into it. Measuring at the
/// caller instead was tried and is wrong in a way that only shows up under load — the caller's
/// clock spans queue *and* service, so the only samples that are pure service are the
/// uncontended ones, which are also the fastest. The estimate then sits near the idle p50 (32ms
/// measured, against a p90 of 152ms), the gate admits a queue far deeper than the budget can
/// drain, and the back of it times out. The actor handles one op at a time, so start-to-finish
/// there *is* the service time.
#[derive(Clone, Debug)]
/// `pub(crate)` because [`NodeOrchestrator::mailbox_lane`] hands it to the router through
/// `main.rs`; both ends must hold the same handle, and neither spells the type.
pub(crate) struct MailboxLane(pub(super) Arc<DispatchCounters>);

impl MailboxLane {
    pub(super) fn new() -> Self {
        Self(Arc::new(DispatchCounters::default()))
    }

    /// Fold one op's dequeue-to-answer into the lane's estimate.
    pub(super) fn record_service(&self, class: OpClass, sample: Duration) {
        self.0.record_service(class, sample);
    }
}

impl Default for MailboxLane {
    fn default() -> Self {
        Self::new()
    }
}

/// One in-flight ask on the actor-mailbox lane, counted for as long as this value lives.
///
/// The decrement is in `Drop` rather than after the `await`, and that is load-bearing. A request
/// whose budget expires has its future dropped by `TimeoutLayer` mid-await, which under overload
/// is most of them; a decrement written after the await never runs for any of those, so every
/// abandoned request leaves its increment behind. The depth then only climbs, the gate refuses
/// everything, nothing completes, and no sample ever corrects the estimate — F7's metastable
/// shape rebuilt inside the fix meant to prevent it.
///
/// Measured with the plain decrement, on an **idle** node after one 120/s bulk arm:
/// `mailbox_depth` 63, every subsequent request refused. With the guard: back to 0.
pub(super) struct MailboxSlot {
    pub(super) stats: Arc<DispatchCounters>,
}

impl MailboxSlot {
    pub(super) fn enter(stats: Arc<DispatchCounters>) -> Self {
        stats.outstanding.fetch_add(1, AtomicOrdering::Relaxed);
        Self { stats }
    }
}

impl Drop for MailboxSlot {
    fn drop(&mut self) {
        self.stats.job_left_pool();
    }
}

impl std::fmt::Debug for QueueLoad {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueueLoad")
            .field("depth", &self.depth())
            .field("width", &self.width)
            .field("predicted_wait", &self.predicted_wait())
            .field("budget", &self.budget)
            .finish()
    }
}

/// Snapshot of a single worker's stats for the `/_admin/workers` endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct WorkerStats {
    pub(super) id: usize,
    /// Core this worker was asked to pin to.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) target_core_id: Option<usize>,
    /// Core it is actually pinned to. Absent when the pin was refused or never requested —
    /// the two are not the same thing, and only this one is evidence.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) core_id: Option<usize>,
    pub(super) queue_depth: usize,
    pub(super) queue_capacity: usize,
    /// Operations started and not yet answered. Sits against `in_flight_capacity`.
    #[serde(default)]
    pub(super) in_flight: usize,
    #[serde(default)]
    pub(super) in_flight_capacity: usize,
    pub(super) jobs_completed: u64,
}

/// Where one shard sits in the pool, for the `/_admin/workers` endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ShardPlacementStats {
    pub(super) shard_id: String,
    /// Dense ordinal. `ordinal % worker_count` is the worker that handles this shard's
    /// writes, which is what makes worker and writer land together.
    pub(super) ordinal: usize,
    /// Whether the shard started and is taking work. False means it holds an ordinal but
    /// failed to hydrate.
    pub(super) serving: bool,
    /// Core this shard's writer thread was asked to pin to.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) target_core_id: Option<usize>,
    /// Core the writer thread is actually pinned to.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) core_id: Option<usize>,
}

/// Snapshot of the dispatch counters for the `/_admin/workers` endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DispatchStats {
    pub(super) affine_sends: u64,
    pub(super) affine_full_fallbacks: u64,
    pub(super) round_robin_sends: u64,
    pub(super) actor_mailbox_fallbacks: u64,
    /// Jobs refused at dequeue for having outlived their request. Defaulted so an older peer's
    /// report still deserializes.
    #[serde(default)]
    pub(super) abandoned: u64,
    /// Requests refused before being queued, because the backlog could not clear in time.
    /// Defaulted for the same reason `abandoned` is.
    #[serde(default)]
    pub(super) refused_at_admission: u64,
    /// Jobs that left the pool without answering. Expected to be `0`; anything else is a
    /// defect. Defaulted for the same reason `abandoned` is.
    #[serde(default)]
    pub(super) jobs_dropped: u64,
}

/// Full worker pool report returned by `GET /_admin/workers`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct WorkerPoolReport {
    /// Config asked for pinned worker threads and the platform could enumerate cores.
    pub(super) pinning_requested: bool,
    /// Workers whose pin actually took. Zero alongside `pinning_requested` means the
    /// platform refused every one — macOS, or a cpuset that excludes the target cores.
    /// This field, not `pinning_requested`, is the evidence that pinning is in effect.
    pub(super) pinned_workers: usize,
    /// `worker_count` was aligned to the core budget so worker `i` and the writer for the
    /// shard with ordinal `i` share a core.
    pub(super) core_aligned: bool,
    pub(super) worker_count: usize,
    pub(super) workers: Vec<WorkerStats>,
    /// Per-shard placement: ordinal, requested core, and the core actually taken.
    pub(super) shards: Vec<ShardPlacementStats>,
    pub(super) dispatch: DispatchStats,
}
