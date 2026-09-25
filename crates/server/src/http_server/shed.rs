//! Load shedding is logged as a rate, not as a line per request.
//!
//! A refusal is the node working as designed — admission, the dequeue check, the request
//! timeout and the rate limiter each turn away work it cannot serve in time — and under
//! overload it is also the most frequent response the node gives. Logged one line each, at
//! `ERROR` as `TraceLayer` does by default for every 5xx, a 4,000 writes/s arm at 2× capacity
//! wrote 51,000 lines in 20 seconds, synchronously, on the same runtime that serves writes and
//! health. Measured (ROADMAP OB15): with those lines silenced the same arm served 2,500 ok/s
//! instead of 1,116, the 408s fell from 10,218 to a few hundred, and health stopped failing.
//! On a node exposed to the internet it is also an amplifier: every request a caller can get
//! refused costs a log line, at a level no operator filters out.
//!
//! So refusals are counted here, at the outermost layer, and reported as one `WARN` per
//! [`REPORT_INTERVAL`] naming how many of each there were. The first refusal after a quiet
//! spell is reported at once, so an operator learns an overload has started without waiting
//! out an interval. Refusals after the last report are carried into the next one, so a burst
//! that ends between reports is summarised by the next refusal rather than never. The exact,
//! continuous figures are `refused_at_admission` and `abandoned` on `/_admin/workers`; this is
//! the log's view of them, not a replacement.
//!
//! The summary is a `WARN`, and a node started without `RUST_LOG` filters at `ERROR`
//! (`tracing_subscriber::fmt::init`'s default), so by default it is not printed at all. That
//! is deliberate rather than an oversight to fix by raising the level: shedding is the node
//! behaving as designed, and an `ERROR` is the one level the default lets through — which is
//! exactly how one line per refusal became the flood. An operator who wants the summary runs
//! at `RUST_LOG=warn` or above; the counters are there either way.

use axum::http::{Response, StatusCode};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tower_http::classify::ServerErrorsFailureClass;
use tower_http::trace::{DefaultOnFailure, DefaultOnResponse, OnFailure, OnResponse};
use tracing::Span;

/// The longest the log stays silent about refusals while they are happening.
pub(crate) const REPORT_INTERVAL: Duration = Duration::from_secs(10);

/// No report has been made yet, so the next refusal is reported at once.
const NEVER: u64 = u64::MAX;

/// Refusals counted since the last report, by the status they were answered with.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct ShedSummary {
    /// 503: the admission gate, the dequeue check, the concurrency guard.
    pub unavailable: u64,
    /// 408: the request timeout abandoned the client.
    pub timed_out: u64,
    /// 429: the caller spent its rate allowance.
    pub rate_limited: u64,
    /// How long the counts were gathered over.
    pub window: Duration,
}

impl ShedSummary {
    fn total(&self) -> u64 {
        self.unavailable + self.timed_out + self.rate_limited
    }
}

struct Tally {
    epoch: Instant,
    interval_ms: u64,
    /// Milliseconds after `epoch` of the last report, or [`NEVER`].
    last_report_ms: AtomicU64,
    unavailable: AtomicU64,
    timed_out: AtomicU64,
    rate_limited: AtomicU64,
}

/// The `TraceLayer` hooks that count refusals instead of logging each one.
///
/// Cheap to clone: `TraceLayer` takes `on_response` by value per request.
#[derive(Clone)]
pub(crate) struct ShedLog {
    tally: Arc<Tally>,
}

impl ShedLog {
    pub(crate) fn new(interval: Duration) -> Self {
        Self {
            tally: Arc::new(Tally {
                epoch: Instant::now(),
                interval_ms: interval.as_millis() as u64,
                last_report_ms: AtomicU64::new(NEVER),
                unavailable: AtomicU64::new(0),
                timed_out: AtomicU64::new(0),
                rate_limited: AtomicU64::new(0),
            }),
        }
    }

    /// Count `status` if it is a refusal, and return a summary if one is due.
    ///
    /// One `fetch_add` per refusal and a load to see whether a report is due; the swap-out
    /// happens only for the one caller that wins the compare-exchange, so concurrent refusals
    /// never produce two reports for the same window.
    pub(crate) fn record_at(&self, status: StatusCode, now: Instant) -> Option<ShedSummary> {
        let t = &*self.tally;
        let counter = match status {
            StatusCode::SERVICE_UNAVAILABLE => &t.unavailable,
            StatusCode::REQUEST_TIMEOUT => &t.timed_out,
            StatusCode::TOO_MANY_REQUESTS => &t.rate_limited,
            _ => return None,
        };
        counter.fetch_add(1, Ordering::Relaxed);

        let now_ms = now.saturating_duration_since(t.epoch).as_millis() as u64;
        let last = t.last_report_ms.load(Ordering::Relaxed);
        if last != NEVER && now_ms.saturating_sub(last) < t.interval_ms {
            return None;
        }
        t.last_report_ms
            .compare_exchange(last, now_ms, Ordering::Relaxed, Ordering::Relaxed)
            .ok()?;

        let since = if last == NEVER { 0 } else { last };
        Some(ShedSummary {
            unavailable: t.unavailable.swap(0, Ordering::Relaxed),
            timed_out: t.timed_out.swap(0, Ordering::Relaxed),
            rate_limited: t.rate_limited.swap(0, Ordering::Relaxed),
            window: Duration::from_millis(now_ms - since),
        })
    }

    fn record(&self, status: StatusCode) {
        if let Some(summary) = self.record_at(status, Instant::now()) {
            tracing::warn!(
                unavailable_503 = summary.unavailable,
                timed_out_408 = summary.timed_out,
                rate_limited_429 = summary.rate_limited,
                window_secs = summary.window.as_secs(),
                "shedding load: refused {} request(s) in the last {}s. Exact counts are \
                 refused_at_admission and abandoned on /_admin/workers",
                summary.total(),
                summary.window.as_secs()
            );
        }
    }
}

impl<B> OnResponse<B> for ShedLog {
    fn on_response(self, response: &Response<B>, latency: Duration, span: &Span) {
        self.record(response.status());
        DefaultOnResponse::default().on_response(response, latency, span);
    }
}

impl OnFailure<ServerErrorsFailureClass> for ShedLog {
    /// Every 5xx is a failure to `TraceLayer`, and its default logs each at `ERROR`. A 503 is
    /// a refusal — counted by `on_response`, which sees every response — so it is left to the
    /// summary; anything else is a fault and is logged exactly as before.
    fn on_failure(&mut self, class: ServerErrorsFailureClass, latency: Duration, span: &Span) {
        if matches!(
            class,
            ServerErrorsFailureClass::StatusCode(StatusCode::SERVICE_UNAVAILABLE)
        ) {
            return;
        }
        DefaultOnFailure::default().on_failure(class, latency, span);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn log() -> (ShedLog, Instant) {
        let log = ShedLog::new(REPORT_INTERVAL);
        let epoch = log.tally.epoch;
        (log, epoch)
    }

    /// An operator learns an overload has begun from its first refusal, not an interval later.
    #[test]
    fn the_first_refusal_is_reported_at_once() {
        let (log, epoch) = log();
        let summary = log
            .record_at(
                StatusCode::SERVICE_UNAVAILABLE,
                epoch + Duration::from_secs(3),
            )
            .expect("the first refusal is reported");
        assert_eq!(summary.unavailable, 1);
        assert_eq!(summary.total(), 1);
    }

    /// The point of the type: a flood inside one interval is one line, not one per request,
    /// and the next report carries every refusal the silence swallowed, by status.
    #[test]
    fn refusals_inside_an_interval_are_counted_and_reported_together() {
        let (log, epoch) = log();
        let start = epoch + Duration::from_secs(1);
        assert!(
            log.record_at(StatusCode::SERVICE_UNAVAILABLE, start)
                .is_some()
        );

        for i in 0..1_000u64 {
            let at = start + Duration::from_millis(i * 9);
            let status = match i % 4 {
                0 | 1 => StatusCode::SERVICE_UNAVAILABLE,
                2 => StatusCode::REQUEST_TIMEOUT,
                _ => StatusCode::TOO_MANY_REQUESTS,
            };
            assert_eq!(log.record_at(status, at), None, "refusal {i} was reported");
        }

        let summary = log
            .record_at(StatusCode::REQUEST_TIMEOUT, start + REPORT_INTERVAL)
            .expect("a report is due once the interval has passed");
        assert_eq!(
            summary,
            ShedSummary {
                unavailable: 500,
                timed_out: 251,
                rate_limited: 250,
                window: REPORT_INTERVAL,
            }
        );
    }

    /// Only refusals are counted: a success, a client error and a fault do not start a report
    /// or inflate one.
    #[test]
    fn only_refusals_are_counted() {
        let (log, epoch) = log();
        for status in [
            StatusCode::OK,
            StatusCode::NOT_FOUND,
            StatusCode::BAD_REQUEST,
            StatusCode::INTERNAL_SERVER_ERROR,
        ] {
            assert_eq!(log.record_at(status, epoch + REPORT_INTERVAL * 2), None);
        }
        let summary = log
            .record_at(StatusCode::TOO_MANY_REQUESTS, epoch + REPORT_INTERVAL * 2)
            .expect("the first refusal is reported");
        assert_eq!(summary.total(), 1);
    }
}
