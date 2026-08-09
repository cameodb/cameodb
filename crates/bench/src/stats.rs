//! Latency sampling and percentile reporting.
//!
//! Percentiles come from a sorted sample vector rather than a histogram. That is exact, and
//! at the sample counts a load run produces (millions at most) the sort is not the expensive
//! part of anything. A histogram would trade that exactness for a bounded memory footprint,
//! which is not a constraint here.

use std::time::Duration;

/// Latency samples for one operation type, plus the outcome counts that give them meaning.
///
/// A percentile over successes alone is a half-truth if a tenth of the requests failed, so
/// errors are carried alongside and reported next to the numbers they qualify.
#[derive(Debug, Default)]
pub struct Samples {
    micros: Vec<u64>,
    pub errors: u64,
    /// First error seen, kept so a failed run says *why* rather than just how often.
    pub first_error: Option<String>,
}

impl Samples {
    pub fn record(&mut self, elapsed: Duration) {
        self.micros.push(elapsed.as_micros() as u64);
    }

    pub fn record_error(&mut self, error: impl std::fmt::Display) {
        self.errors += 1;
        if self.first_error.is_none() {
            self.first_error = Some(error.to_string());
        }
    }

    pub fn merge(&mut self, mut other: Samples) {
        self.micros.append(&mut other.micros);
        self.errors += other.errors;
        if self.first_error.is_none() {
            self.first_error = other.first_error;
        }
    }

    pub fn len(&self) -> usize {
        self.micros.len()
    }

    /// No samples *and* no errors — a workload that did not run, as opposed to one that ran
    /// and failed. The two print very differently.
    pub fn is_empty(&self) -> bool {
        self.len() == 0 && self.errors == 0
    }

    /// Sorts in place, then reports. Consuming self makes it obvious the ordering is
    /// destroyed, which matters if anyone later wants to add per-second buckets.
    pub fn summarize(mut self, label: &str, wall: Duration) -> Summary {
        self.micros.sort_unstable();
        Summary {
            label: label.to_string(),
            count: self.micros.len() as u64,
            errors: self.errors,
            first_error: self.first_error,
            wall,
            p50: percentile(&self.micros, 50.0),
            p90: percentile(&self.micros, 90.0),
            p95: percentile(&self.micros, 95.0),
            p99: percentile(&self.micros, 99.0),
            p999: percentile(&self.micros, 99.9),
            max: self.micros.last().copied().unwrap_or(0),
            mean: mean(&self.micros),
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
    pub errors: u64,
    pub first_error: Option<String>,
    pub wall: Duration,
    pub mean: u64,
    pub p50: u64,
    pub p90: u64,
    pub p95: u64,
    pub p99: u64,
    pub p999: u64,
    pub max: u64,
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
            println!("  no successful requests ({} errors)", self.errors);
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
        println!(
            "  {:>10}  mean {}  p50 {}  p90 {}",
            "latency",
            ms(self.mean),
            ms(self.p50),
            ms(self.p90)
        );
        println!(
            "  {:>10}  p95 {}  p99 {}  p99.9 {}  max {}",
            "",
            ms(self.p95),
            ms(self.p99),
            ms(self.p999),
            ms(self.max)
        );
        if self.errors > 0 {
            println!("  {:>10}  {}", "errors", self.errors);
            if let Some(err) = &self.first_error {
                println!("  {:>10}  {}", "first", err);
            }
        }
    }
}

/// Microseconds, rendered at a precision that suits the magnitude. A p99 of "0.42ms" reads
/// better than "420µs" next to a max of "31.7ms", and worse below 1ms.
fn ms(micros: u64) -> String {
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
    }

    /// Errors have to survive the merge from per-worker samples, or a run where every
    /// request failed would report as a clean run with no samples.
    #[test]
    fn merging_keeps_errors_and_the_first_message() {
        let mut a = Samples::default();
        a.record(Duration::from_micros(10));
        a.record_error("first failure");

        let mut b = Samples::default();
        b.record(Duration::from_micros(20));
        b.record_error("second failure");

        a.merge(b);
        assert_eq!(a.len(), 2);
        assert_eq!(a.errors, 2);
        assert_eq!(a.first_error.as_deref(), Some("first failure"));
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
}
