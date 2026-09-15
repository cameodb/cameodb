//! Latency sampling and percentile reporting.
//!
//! Percentiles come from a sorted sample vector rather than a histogram. That is exact, and
//! at the sample counts a load run produces (millions at most) the sort is not the expensive
//! part of anything. A histogram would trade that exactness for a bounded memory footprint,
//! which is not a constraint here.
//!
//! # Two clocks, and why both are kept
//!
//! A closed-loop run has one meaningful latency: the request was sent, the answer came back.
//! An open-loop run has two, and reporting only the first is the error the whole mode exists
//! to avoid.
//!
//! - **Service latency** — sent → answered. What the node took once it had the request.
//! - **Total latency** — *intended* send → answered. What a client that wanted to ask at that
//!   moment actually waited, including any time the request spent queued inside this harness
//!   because the previous ones had not gone out yet.
//!
//! When the generator keeps up the two are the same. When it falls behind — because the node
//! is saturated and the harness is holding thousands of requests open — service latency keeps
//! looking healthy while total latency explodes, and only the second is the truth. Reporting
//! service latency alone is *coordinated omission*: the measurement stops offering load at
//! exactly the moment the system stops keeping up, and so never records the queue it built.
//!
//! The difference between them is [`Samples::lag`], recorded separately so the report can say
//! whether the delay belonged to the node or to the harness.

use std::fmt::Display;
use std::time::Duration;

/// What became of a request. Every class is a different fact about the node, and collapsing
/// them into one "errors" count throws away the result of an overload run.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Outcomes {
    /// Answered successfully.
    pub ok: u64,
    /// `503` — the node's concurrency guard refused admission without queueing. Under
    /// overload this is the node working correctly, and the rate at which it starts is the
    /// capacity figure an open-loop run is looking for.
    pub shed: u64,
    /// `408` — the request timeout fired and the node gave up on the client. Distinct from
    /// `shed` because the work behind it is *not* known to have stopped.
    pub timed_out: u64,
    /// `429` — a rate limiter refused. Off by default, so any of these means the run was
    /// measuring configuration rather than capacity.
    pub rate_limited: u64,
    /// Any other non-success status: a bad request, a 500, a 404 for a missing index.
    pub other_status: u64,
    /// Never became an HTTP response — connection refused, TLS failure, a body that would
    /// not parse. Counted apart from `shed` because a dead node must not read as a busy one.
    pub transport: u64,
}

impl Outcomes {
    /// Everything that was not a success.
    pub fn failures(&self) -> u64 {
        self.shed + self.timed_out + self.rate_limited + self.other_status + self.transport
    }

    fn merge(&mut self, other: Outcomes) {
        self.ok += other.ok;
        self.shed += other.shed;
        self.timed_out += other.timed_out;
        self.rate_limited += other.rate_limited;
        self.other_status += other.other_status;
        self.transport += other.transport;
    }

    /// The breakdown, as a line, omitting classes that did not occur. `None` when nothing
    /// failed — there is no point printing six zeroes on a clean run.
    fn describe(&self) -> Option<String> {
        if self.failures() == 0 {
            return None;
        }
        let mut parts = Vec::new();
        let mut push = |label: &str, count: u64| {
            if count > 0 {
                parts.push(format!("{label} {count}"));
            }
        };
        push("shed (503)", self.shed);
        push("timeout (408)", self.timed_out);
        push("rate-limited (429)", self.rate_limited);
        push("other status", self.other_status);
        push("transport", self.transport);
        Some(parts.join("  "))
    }
}

/// Latency samples for one operation type, plus the outcome counts that give them meaning.
///
/// A percentile over successes alone is a half-truth if a tenth of the requests failed, so
/// outcomes are carried alongside and reported next to the numbers they qualify.
#[derive(Debug, Default)]
pub struct Samples {
    /// Sent → answered.
    service: Vec<u64>,
    /// Intended send → answered. Empty in closed-loop mode, where there is no intended time
    /// distinct from the actual one.
    total: Vec<u64>,
    /// Intended send → actually sent: how far behind its own schedule the generator was.
    lag: Vec<u64>,
    /// What happened in each whole second since the run began, so the report can show
    /// whether the achieved rate held or decayed. A node that sustains a rate and one that
    /// collapses into it halfway through produce the same average.
    per_second: Vec<SecondBucket>,
    pub outcomes: Outcomes,
    /// First error seen, kept so a failed run says *why* rather than just how often.
    pub first_error: Option<String>,
}

impl Samples {
    /// A closed-loop success: one clock, so service and total are the same number and only
    /// the former is kept.
    pub fn record(&mut self, elapsed: Duration) {
        self.service.push(elapsed.as_micros() as u64);
        self.outcomes.ok += 1;
    }

    /// An open-loop success.
    ///
    /// `intended` is when the arrival process said this request should go out, `sent` when it
    /// actually did, `done` when the answer arrived — all as offsets from the start of the
    /// measured window, which is also what buckets the per-second series.
    pub fn record_open(&mut self, intended: Duration, sent: Duration, done: Duration) {
        self.service
            .push(done.saturating_sub(sent).as_micros() as u64);
        self.total
            .push(done.saturating_sub(intended).as_micros() as u64);
        self.lag
            .push(sent.saturating_sub(intended).as_micros() as u64);
        self.outcomes.ok += 1;
        self.bucket(intended).ok += 1;
    }

    /// The bucket for the second an arrival was *offered* in, grown as needed.
    ///
    /// Keyed on the intended time rather than on completion: a request the node answers late,
    /// or refuses late, still belongs to the second it was asked for. Bucketing by completion
    /// would make a collapse look like load moving into the future rather than being lost.
    fn bucket(&mut self, intended: Duration) -> &mut SecondBucket {
        let second = intended.as_secs() as usize;
        if self.per_second.len() <= second {
            self.per_second.resize(second + 1, SecondBucket::default());
        }
        &mut self.per_second[second]
    }

    /// Record a failure, classified by the status the node answered with.
    ///
    /// `status` is `None` for anything that never became a response. See [`Outcomes`] for why
    /// the distinction is kept rather than counted as one number.
    pub fn record_failure(&mut self, status: Option<u16>, error: impl Display) {
        self.classify(status, error);
    }

    /// [`record_failure`](Self::record_failure) for an open-loop run, which also knows *when*
    /// the request was offered.
    ///
    /// The per-second series has to carry failures as well as successes or it shows half the
    /// picture. The case this exists for is the crossover: successes falling while timeouts
    /// climb, at a fixed offered rate, is the signature of work continuing after the client
    /// that asked for it has been abandoned — and an average over the run hides it completely.
    pub fn record_failure_open(
        &mut self,
        intended: Duration,
        status: Option<u16>,
        error: impl Display,
    ) {
        self.classify(status, error);
        let bucket = self.bucket(intended);
        match status {
            Some(503) => bucket.shed += 1,
            Some(408) => bucket.timed_out += 1,
            _ => bucket.other += 1,
        }
    }

    fn classify(&mut self, status: Option<u16>, error: impl Display) {
        match status {
            Some(503) => self.outcomes.shed += 1,
            Some(408) => self.outcomes.timed_out += 1,
            Some(429) => self.outcomes.rate_limited += 1,
            Some(_) => self.outcomes.other_status += 1,
            None => self.outcomes.transport += 1,
        }
        if self.first_error.is_none() {
            self.first_error = Some(error.to_string());
        }
    }

    pub fn merge(&mut self, mut other: Samples) {
        self.service.append(&mut other.service);
        self.total.append(&mut other.total);
        self.lag.append(&mut other.lag);
        self.outcomes.merge(other.outcomes);

        if self.per_second.len() < other.per_second.len() {
            self.per_second
                .resize(other.per_second.len(), SecondBucket::default());
        }
        for (slot, bucket) in self.per_second.iter_mut().zip(other.per_second.iter()) {
            slot.ok += bucket.ok;
            slot.shed += bucket.shed;
            slot.timed_out += bucket.timed_out;
            slot.other += bucket.other;
        }

        if self.first_error.is_none() {
            self.first_error = other.first_error;
        }
    }

    pub fn len(&self) -> usize {
        self.service.len()
    }

    /// No samples *and* no failures — a workload that did not run, as opposed to one that ran
    /// and failed. The two print very differently.
    pub fn is_empty(&self) -> bool {
        self.len() == 0 && self.outcomes.failures() == 0
    }

    /// The lag distribution's 99th percentile, in microseconds, without consuming the
    /// samples.
    ///
    /// Read before the report is built, because whether the run says anything about the node
    /// depends on it: a generator that could not keep to its own schedule measured itself.
    pub fn lag_p99(&self) -> Option<u64> {
        if self.lag.is_empty() {
            return None;
        }
        let mut sorted = self.lag.clone();
        sorted.sort_unstable();
        Some(percentile(&sorted, 99.0))
    }

    /// Successful completions per whole second of the run.
    ///
    /// The last bucket is usually short and is dropped by the reader rather than here, since
    /// only the caller knows the measured duration.
    pub fn per_second(&self) -> &[SecondBucket] {
        &self.per_second
    }

    /// Sorts in place, then reports. Consuming self makes it obvious the ordering is
    /// destroyed.
    pub fn summarize(mut self, label: &str, wall: Duration) -> Summary {
        self.service.sort_unstable();
        self.total.sort_unstable();
        self.lag.sort_unstable();
        Summary {
            label: label.to_string(),
            count: self.service.len() as u64,
            outcomes: self.outcomes,
            first_error: self.first_error,
            wall,
            service: Percentiles::of(&self.service),
            total: (!self.total.is_empty()).then(|| Percentiles::of(&self.total)),
            lag: (!self.lag.is_empty()).then(|| Percentiles::of(&self.lag)),
        }
    }
}

/// What one second of a run contained, by outcome.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SecondBucket {
    pub ok: u64,
    pub shed: u64,
    pub timed_out: u64,
    pub other: u64,
}

/// One latency distribution, in microseconds.
#[derive(Debug, Default, Clone, Copy)]
pub struct Percentiles {
    pub mean: u64,
    pub p50: u64,
    pub p90: u64,
    pub p95: u64,
    pub p99: u64,
    pub p999: u64,
    pub max: u64,
}

impl Percentiles {
    /// `sorted` must already be sorted ascending.
    fn of(sorted: &[u64]) -> Self {
        Self {
            mean: mean(sorted),
            p50: percentile(sorted, 50.0),
            p90: percentile(sorted, 90.0),
            p95: percentile(sorted, 95.0),
            p99: percentile(sorted, 99.0),
            p999: percentile(sorted, 99.9),
            max: sorted.last().copied().unwrap_or(0),
        }
    }
}

/// Nearest-rank percentile on an already-sorted slice. Returns microseconds.
fn percentile(sorted: &[u64], pct: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    // Nearest-rank: the smallest value at or below which at least `pct` of samples fall.
    // Interpolating between neighbours would invent a latency no request actually saw.
    let rank = ((pct / 100.0) * sorted.len() as f64).ceil() as usize;
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

fn mean(samples: &[u64]) -> u64 {
    if samples.is_empty() {
        return 0;
    }
    (samples.iter().map(|v| *v as u128).sum::<u128>() / samples.len() as u128) as u64
}

#[derive(Debug)]
pub struct Summary {
    pub label: String,
    pub count: u64,
    pub outcomes: Outcomes,
    pub first_error: Option<String>,
    pub wall: Duration,
    /// Sent → answered. Always present.
    pub service: Percentiles,
    /// Intended send → answered. Open-loop only.
    pub total: Option<Percentiles>,
    /// Intended send → actually sent. Open-loop only.
    pub lag: Option<Percentiles>,
}

impl Summary {
    pub fn throughput(&self) -> f64 {
        let secs = self.wall.as_secs_f64();
        if secs <= 0.0 {
            0.0
        } else {
            self.count as f64 / secs
        }
    }

    pub fn print(&self) {
        println!("\n{}", self.label);
        println!("{}", "-".repeat(self.label.len()));
        if self.count == 0 {
            println!(
                "  no successful requests ({} failed)",
                self.outcomes.failures()
            );
            if let Some(breakdown) = self.outcomes.describe() {
                println!("  {breakdown}");
            }
            if let Some(err) = &self.first_error {
                println!("  first error: {err}");
            }
            return;
        }
        println!(
            "  {:>10}  {:.0} ok/s over {:.1}s",
            self.count,
            self.throughput(),
            self.wall.as_secs_f64()
        );

        // Labelled "service" only when there is a second clock to tell it apart from. A
        // closed-loop run has one latency and naming it invites the reader to look for the
        // other.
        let service_label = if self.total.is_some() {
            "service"
        } else {
            "latency"
        };
        print_percentiles(service_label, &self.service);

        if let Some(total) = &self.total {
            print_percentiles("total", total);
            println!(
                "  {:>10}  measured from the intended send time — this is the number an SLA is \
                 written against",
                ""
            );
        }
        if let Some(lag) = &self.lag {
            println!(
                "  {:>10}  p50 {}  p99 {}  max {}   (generator behind its own schedule)",
                "harness lag",
                ms(lag.p50),
                ms(lag.p99),
                ms(lag.max)
            );
        }

        if let Some(breakdown) = self.outcomes.describe() {
            println!("  {:>10}  {}", "failures", breakdown);
            if let Some(err) = &self.first_error {
                println!("  {:>10}  {}", "first", err);
            }
        }
    }
}

fn print_percentiles(label: &str, p: &Percentiles) {
    println!(
        "  {:>10}  mean {}  p50 {}  p90 {}",
        label,
        ms(p.mean),
        ms(p.p50),
        ms(p.p90)
    );
    println!(
        "  {:>10}  p95 {}  p99 {}  p99.9 {}  max {}",
        "",
        ms(p.p95),
        ms(p.p99),
        ms(p.p999),
        ms(p.max)
    );
}

/// Microseconds, rendered at a precision that suits the magnitude. A p99 of "0.42ms" reads
/// better than "420µs" next to a max of "31.7ms", and worse below 1ms.
pub fn ms(micros: u64) -> String {
    if micros < 1_000 {
        format!("{micros}µs")
    } else {
        format!("{:.2}ms", micros as f64 / 1_000.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentiles_land_on_real_samples() {
        let sorted: Vec<u64> = (1..=100).collect();

        // Nearest-rank, so every answer is a value that actually occurred.
        assert_eq!(percentile(&sorted, 50.0), 50);
        assert_eq!(percentile(&sorted, 99.0), 99);
        assert_eq!(percentile(&sorted, 100.0), 100);
        assert!(sorted.contains(&percentile(&sorted, 99.9)));
    }

    #[test]
    fn a_single_sample_is_every_percentile() {
        let sorted = vec![7];
        assert_eq!(percentile(&sorted, 50.0), 7);
        assert_eq!(percentile(&sorted, 99.9), 7);
    }

    #[test]
    fn empty_samples_do_not_panic() {
        assert_eq!(percentile(&[], 99.0), 0);
        assert_eq!(mean(&[]), 0);

        let summary = Samples::default().summarize("empty", Duration::from_secs(1));
        assert_eq!(summary.count, 0);
        assert_eq!(summary.throughput(), 0.0);
        assert!(summary.total.is_none(), "no open-loop clock was recorded");
    }

    /// Failures have to survive the merge from per-worker samples, or a run where every
    /// request failed would report as a clean run with no samples.
    #[test]
    fn merging_keeps_failures_and_the_first_message() {
        let mut a = Samples::default();
        a.record(Duration::from_micros(10));
        a.record_failure(Some(503), "first failure");

        let mut b = Samples::default();
        b.record(Duration::from_micros(20));
        b.record_failure(None, "second failure");

        a.merge(b);
        assert_eq!(a.len(), 2);
        assert_eq!(a.outcomes.shed, 1);
        assert_eq!(a.outcomes.transport, 1);
        assert_eq!(a.outcomes.failures(), 2);
        assert_eq!(a.first_error.as_deref(), Some("first failure"));
    }

    /// The classification is the point of carrying the status at all: a node shedding load
    /// and a node that cannot be reached must not land in the same bucket.
    #[test]
    fn failures_are_classified_by_status() {
        let mut samples = Samples::default();
        samples.record_failure(Some(503), "shed");
        samples.record_failure(Some(408), "timeout");
        samples.record_failure(Some(429), "limited");
        samples.record_failure(Some(500), "server");
        samples.record_failure(None, "refused");

        let o = samples.outcomes;
        assert_eq!(
            (
                o.shed,
                o.timed_out,
                o.rate_limited,
                o.other_status,
                o.transport
            ),
            (1, 1, 1, 1, 1)
        );
        assert_eq!(o.failures(), 5);
        assert_eq!(o.ok, 0);
    }

    #[test]
    fn throughput_counts_successes_over_wall_clock() {
        let mut samples = Samples::default();
        for _ in 0..50 {
            samples.record(Duration::from_millis(1));
        }
        let summary = samples.summarize("writes", Duration::from_secs(2));
        assert_eq!(summary.throughput(), 25.0);
    }

    /// The correction, stated as arithmetic. A request that should have gone out at 1.000s,
    /// went out at 1.500s because the harness was backed up, and was answered at 1.600s took
    /// 100ms of the node's time and made its caller wait 600ms. Reporting only the first is
    /// the omission this mode exists to remove.
    #[test]
    fn total_latency_includes_time_spent_queued_in_the_harness() {
        let mut samples = Samples::default();
        samples.record_open(
            Duration::from_millis(1_000),
            Duration::from_millis(1_500),
            Duration::from_millis(1_600),
        );

        let summary = samples.summarize("searches", Duration::from_secs(2));
        assert_eq!(summary.service.p50, 100_000, "sent → answered");
        assert_eq!(
            summary.total.expect("open-loop clock").p50,
            600_000,
            "intended → answered"
        );
        assert_eq!(
            summary.lag.expect("open-loop clock").p50,
            500_000,
            "intended → sent"
        );
    }

    /// A generator that keeps up records the two clocks as the same number, which is what
    /// makes a divergence meaningful when it appears.
    #[test]
    fn a_punctual_generator_reports_identical_clocks() {
        let mut samples = Samples::default();
        samples.record_open(
            Duration::from_millis(500),
            Duration::from_millis(500),
            Duration::from_millis(700),
        );
        let summary = samples.summarize("searches", Duration::from_secs(1));
        assert_eq!(summary.service.p50, summary.total.unwrap().p50);
        assert_eq!(summary.lag.unwrap().max, 0);
    }

    /// Bucketed by *intended* second rather than completion second, so a request the node
    /// answers late is still counted against the second it was offered in — otherwise a
    /// collapse would appear to shift load into the future rather than to lose it.
    #[test]
    fn completions_bucket_by_the_second_they_were_offered_in() {
        let mut samples = Samples::default();
        samples.record_open(
            Duration::from_millis(100),
            Duration::from_millis(100),
            Duration::from_millis(150),
        );
        samples.record_open(
            Duration::from_millis(900),
            Duration::from_millis(900),
            Duration::from_millis(2_400),
        );
        samples.record_open(
            Duration::from_millis(2_500),
            Duration::from_millis(2_500),
            Duration::from_millis(2_600),
        );

        let seconds: Vec<u64> = samples.per_second().iter().map(|b| b.ok).collect();
        assert_eq!(seconds, vec![2, 0, 1]);
    }

    /// Failures are bucketed by class as well as by second. The crossover this exists to
    /// show — successes falling while timeouts climb at a fixed offered rate — is invisible in
    /// a series that counts only what succeeded.
    #[test]
    fn failures_bucket_by_second_and_class() {
        let mut samples = Samples::default();
        samples.record_open(
            Duration::from_millis(100),
            Duration::from_millis(100),
            Duration::from_millis(150),
        );
        samples.record_failure_open(Duration::from_millis(200), Some(408), "timeout");
        samples.record_failure_open(Duration::from_millis(300), Some(503), "shed");
        samples.record_failure_open(Duration::from_millis(1_400), Some(408), "timeout");
        samples.record_failure_open(Duration::from_millis(1_500), Some(500), "server");

        let buckets = samples.per_second();
        assert_eq!(buckets.len(), 2);
        assert_eq!(
            (buckets[0].ok, buckets[0].timed_out, buckets[0].shed),
            (1, 1, 1)
        );
        assert_eq!(
            (buckets[1].ok, buckets[1].timed_out, buckets[1].other),
            (0, 1, 1)
        );
        // The totals still agree with the classification the outcome counts made.
        assert_eq!(samples.outcomes.timed_out, 2);
        assert_eq!(samples.outcomes.shed, 1);
        assert_eq!(samples.outcomes.other_status, 1);
    }

    /// Per-second series have to survive the merge too — they are collected per workload and
    /// added together, and a lost bucket would understate exactly the second that mattered.
    #[test]
    fn merging_adds_per_second_buckets_elementwise() {
        let mut a = Samples::default();
        a.record_open(
            Duration::from_millis(0),
            Duration::from_millis(0),
            Duration::from_millis(1),
        );

        let mut b = Samples::default();
        b.record_open(
            Duration::from_millis(0),
            Duration::from_millis(0),
            Duration::from_millis(1),
        );
        b.record_open(
            Duration::from_millis(3_000),
            Duration::from_millis(3_000),
            Duration::from_millis(3_001),
        );

        a.merge(b);
        let seconds: Vec<u64> = a.per_second().iter().map(|b| b.ok).collect();
        assert_eq!(seconds, vec![2, 0, 0, 1]);
    }
}
