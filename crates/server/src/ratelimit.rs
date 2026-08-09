//! Rate limiting for MCP tool invocations (Phase 14, C1).
//!
//! The threat this exists for is not a hostile stranger — B1's authentication already
//! answers that — but a `reader` key held by an AI agent that decides to call
//! `search_indexes` in a loop. The key is legitimate, every individual call is authorized,
//! and nothing in the capability model has anything to say about *how often*. A search fans
//! out across every shard, so a loop costs the node far more than it costs the agent.
//!
//! A token bucket rather than a fixed window: an agent's traffic is bursty by nature (a
//! plan, then a flurry of lookups, then thinking), and a fixed window either refuses the
//! flurry or is set so loose it never bites. A bucket lets a burst through and then meters
//! the sustained rate, which is the shape the traffic actually has.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Instant;

use serde::{Deserialize, Serialize};

/// What a caller may spend on MCP tool calls.
///
/// Off by default. An existing deployment that upgrades into this code must not start
/// refusing calls it used to serve — the same reasoning that keeps `[security] enabled`
/// off by default.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct McpLimitsConfig {
    /// Sustained tool calls per minute per caller. `0` disables limiting entirely.
    #[serde(default)]
    pub tool_calls_per_minute: u32,

    /// How much of that allowance may be spent at once. `0` means "one minute's worth",
    /// which is the only default that cannot surprise: the bucket starts full and a caller
    /// under the sustained rate never notices the limiter exists.
    #[serde(default)]
    pub tool_call_burst: u32,
}

impl McpLimitsConfig {
    pub fn enabled(&self) -> bool {
        self.tool_calls_per_minute > 0
    }

    /// Bucket capacity in tokens.
    fn capacity(&self) -> f64 {
        if self.tool_call_burst > 0 {
            f64::from(self.tool_call_burst)
        } else {
            f64::from(self.tool_calls_per_minute)
        }
    }

    /// Tokens added per second.
    fn refill_per_sec(&self) -> f64 {
        f64::from(self.tool_calls_per_minute) / 60.0
    }
}

/// One caller's bucket.
#[derive(Debug)]
struct Bucket {
    tokens: f64,
    last: Instant,
}

/// Per-caller token buckets.
///
/// The map is keyed by `key_id`, so its size is bounded by the number of configured API
/// keys — a caller cannot mint new buckets, which is what would otherwise make a limiter
/// its own memory-exhaustion lever. Callers with no identity (security disabled) share one
/// bucket under [`UNIDENTIFIED`]; there is nothing to tell them apart by, and saying so is
/// better than inventing a distinction that does not exist.
#[derive(Debug)]
pub struct ToolRateLimiter {
    config: McpLimitsConfig,
    buckets: Mutex<HashMap<String, Bucket>>,
}

/// The bucket used when no key identified the caller.
const UNIDENTIFIED: &str = "<unidentified>";

/// Outcome of asking to spend one token.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    Allow,
    /// Refused; seconds until one token is available again, rounded up so a caller that
    /// obeys it succeeds rather than arriving a hair early and being refused twice.
    Deny {
        retry_after_secs: u64,
    },
}

impl ToolRateLimiter {
    pub fn new(config: McpLimitsConfig) -> Self {
        Self {
            config,
            buckets: Mutex::new(HashMap::new()),
        }
    }

    /// Spend one token for `key_id`, or report how long to wait.
    pub fn check(&self, key_id: Option<&str>) -> Verdict {
        self.check_at(key_id, Instant::now())
    }

    /// The same decision against a caller-supplied clock, so the refill arithmetic can be
    /// tested without sleeping.
    fn check_at(&self, key_id: Option<&str>, now: Instant) -> Verdict {
        if !self.config.enabled() {
            return Verdict::Allow;
        }
        let capacity = self.config.capacity();
        let refill = self.config.refill_per_sec();
        let subject = key_id.unwrap_or(UNIDENTIFIED);

        // A poisoned lock here must not take the node down, and must not silently stop
        // enforcing either. Recovering the guard keeps the limiter working: the data behind
        // it is a set of counters, and a torn counter costs at most one call's accuracy.
        let mut buckets = self
            .buckets
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        let bucket = buckets.entry(subject.to_string()).or_insert(Bucket {
            tokens: capacity,
            last: now,
        });

        // Refill for elapsed time, capped at capacity — an idle caller gets a full bucket,
        // not an unbounded credit for the time it was away.
        let elapsed = now.saturating_duration_since(bucket.last).as_secs_f64();
        bucket.tokens = (bucket.tokens + elapsed * refill).min(capacity);
        bucket.last = now;

        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            Verdict::Allow
        } else {
            let deficit = 1.0 - bucket.tokens;
            let wait = if refill > 0.0 { deficit / refill } else { 1.0 };
            Verdict::Deny {
                retry_after_secs: wait.ceil().max(1.0) as u64,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn limiter(per_minute: u32, burst: u32) -> ToolRateLimiter {
        ToolRateLimiter::new(McpLimitsConfig {
            tool_calls_per_minute: per_minute,
            tool_call_burst: burst,
        })
    }

    /// The default has to be inert. An upgrade that quietly started refusing an agent's
    /// calls would be indistinguishable, from the agent's side, from the node breaking.
    #[test]
    fn a_limiter_with_no_configured_rate_allows_everything() {
        let limiter = limiter(0, 0);
        for _ in 0..1_000 {
            assert_eq!(limiter.check(Some("k1")), Verdict::Allow);
        }
    }

    /// The burst is spendable at once, and the call after it is refused — that is the whole
    /// contract of a bucket, as opposed to a fixed window that would refuse mid-burst.
    #[test]
    fn a_caller_may_spend_its_burst_and_is_then_refused() {
        let limiter = limiter(60, 5);
        let start = Instant::now();
        for i in 0..5 {
            assert_eq!(
                limiter.check_at(Some("k1"), start),
                Verdict::Allow,
                "call {i} is within the burst"
            );
        }
        assert!(
            matches!(
                limiter.check_at(Some("k1"), start),
                Verdict::Deny { retry_after_secs } if retry_after_secs >= 1
            ),
            "the call past the burst should be refused with a wait"
        );
    }

    /// Waiting the advertised time has to actually work. A `retry_after` a caller obeys and
    /// is still refused for trains agents to ignore it.
    #[test]
    fn waiting_the_advertised_time_earns_another_call() {
        let limiter = limiter(60, 1);
        let start = Instant::now();
        assert_eq!(limiter.check_at(Some("k1"), start), Verdict::Allow);

        let Verdict::Deny { retry_after_secs } = limiter.check_at(Some("k1"), start) else {
            panic!("the second immediate call should be refused");
        };
        let later = start + Duration::from_secs(retry_after_secs);
        assert_eq!(
            limiter.check_at(Some("k1"), later),
            Verdict::Allow,
            "obeying retry_after should be enough"
        );
    }

    /// One key exhausting its allowance must not refuse another. Shared buckets would make
    /// a single noisy agent an outage for every other consumer of the node.
    #[test]
    fn one_callers_exhaustion_does_not_refuse_another() {
        let limiter = limiter(60, 2);
        let start = Instant::now();
        for _ in 0..2 {
            assert_eq!(limiter.check_at(Some("noisy"), start), Verdict::Allow);
        }
        assert!(matches!(
            limiter.check_at(Some("noisy"), start),
            Verdict::Deny { .. }
        ));
        assert_eq!(
            limiter.check_at(Some("quiet"), start),
            Verdict::Allow,
            "a different key has its own bucket"
        );
    }

    /// An idle caller comes back to a full bucket, not to credit for every minute it was
    /// away — otherwise a limiter is only a delay, and a long-idle agent could spend an
    /// unbounded burst in one go.
    #[test]
    fn an_idle_caller_refills_to_capacity_and_no_further() {
        let limiter = limiter(60, 3);
        let start = Instant::now();
        for _ in 0..3 {
            assert_eq!(limiter.check_at(Some("k1"), start), Verdict::Allow);
        }
        // Away for an hour: at 60/minute that is 3 600 tokens of notional credit.
        let much_later = start + Duration::from_secs(3_600);
        for i in 0..3 {
            assert_eq!(
                limiter.check_at(Some("k1"), much_later),
                Verdict::Allow,
                "refilled token {i}"
            );
        }
        assert!(
            matches!(
                limiter.check_at(Some("k1"), much_later),
                Verdict::Deny { .. }
            ),
            "an hour idle should restore the burst, not more than the burst"
        );
    }

    /// Burst defaults to a minute's worth rather than to zero, which would otherwise mean
    /// "configured a rate, refused everything".
    #[test]
    fn an_unset_burst_means_one_minutes_allowance() {
        let limiter = limiter(10, 0);
        let start = Instant::now();
        for i in 0..10 {
            assert_eq!(
                limiter.check_at(Some("k1"), start),
                Verdict::Allow,
                "call {i} inside the implied burst"
            );
        }
        assert!(matches!(
            limiter.check_at(Some("k1"), start),
            Verdict::Deny { .. }
        ));
    }
}
