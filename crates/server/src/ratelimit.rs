//! Rate limiting for MCP tool invocations (Phase 14, C1).
//!
//! The threat this exists for is not a hostile stranger — B1's authentication already
//! answers that — but a `reader` key held by an AI agent that decides to call
//! `search_across_indexes` in a loop. The key is legitimate, every individual call is authorized,
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
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
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

    /// The largest `limit` an MCP search may ask for.
    ///
    /// Unlike the rate above, this one is always in force: a rate of `0` means an agent may
    /// call as often as it likes, but there is no reading of "no ceiling at all" that is a
    /// number, and an absent ceiling is a caller deciding how many hits the node builds for
    /// one request. Raise it if the deployment can afford to; `0` is refused at load rather
    /// than read as unlimited, because a bound whose zero inverts its meaning is a trap.
    #[serde(default = "default_max_search_limit")]
    pub max_search_limit: usize,

    /// Moved to `[limits] max_response_bytes`. Read from here until 0.4.0.
    ///
    /// Kept as a field rather than left to fall through as an unknown key, because this
    /// section refuses unknown keys: an operator upgrading with the old spelling would not
    /// get a warning, they would get a node that will not start.
    #[serde(default)]
    pub max_response_bytes: Option<usize>,
}

/// The ceiling when an operator sets none.
///
/// Ten thousand hits is the point past which one request stops being one request for this
/// architecture: a search fans out across every shard of an index, and each hit is a redb
/// lookup, a merge entry and a serialized document.
fn default_max_search_limit() -> usize {
    cameodb_mcp::DEFAULT_MAX_SEARCH_LIMIT
}

/// Written out rather than derived, so that a config built in code and one parsed from an
/// absent `[security.limits]` are the same config. A derived `Default` would leave
/// `max_search_limit` at zero, which is the one value this section refuses.
impl Default for McpLimitsConfig {
    fn default() -> Self {
        Self {
            tool_calls_per_minute: 0,
            tool_call_burst: 0,
            max_search_limit: default_max_search_limit(),
            max_response_bytes: None,
        }
    }
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

/// Outcome of asking to spend tokens.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    Allow,
    /// Refused; seconds until the asked-for tokens are available again, rounded up so a caller
    /// that obeys it succeeds rather than arriving a hair early and being refused twice.
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

    /// Spend `cost` tokens for `key_id`, or report how long to wait.
    ///
    /// The cost is what the call asks the node to do — a federated search over five indexes is
    /// five searches — so that the budget measures work rather than requests.
    pub fn check(&self, key_id: Option<&str>, cost: u32) -> Verdict {
        self.check_at(key_id, cost, Instant::now())
    }

    /// The same decision against a caller-supplied clock, so the refill arithmetic can be
    /// tested without sleeping.
    fn check_at(&self, key_id: Option<&str>, cost: u32, now: Instant) -> Verdict {
        if !self.config.enabled() {
            return Verdict::Allow;
        }
        let capacity = self.config.capacity();
        let refill = self.config.refill_per_sec();
        // A cost above the bucket's whole capacity would never be affordable, and the caller
        // would be refused forever with a retry time that never comes true. Spending the
        // bucket dry is the honest charge: it is everything the caller has.
        let cost = f64::from(cost.max(1)).min(capacity);
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

        if bucket.tokens >= cost {
            bucket.tokens -= cost;
            Verdict::Allow
        } else {
            let deficit = cost - bucket.tokens;
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
            ..Default::default()
        })
    }

    /// The default has to be inert. An upgrade that quietly started refusing an agent's
    /// calls would be indistinguishable, from the agent's side, from the node breaking.
    #[test]
    fn a_limiter_with_no_configured_rate_allows_everything() {
        let limiter = limiter(0, 0);
        for _ in 0..1_000 {
            assert_eq!(limiter.check(Some("k1"), 1), Verdict::Allow);
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
                limiter.check_at(Some("k1"), 1, start),
                Verdict::Allow,
                "call {i} is within the burst"
            );
        }
        assert!(
            matches!(
                limiter.check_at(Some("k1"), 1, start),
                Verdict::Deny { retry_after_secs } if retry_after_secs >= 1
            ),
            "the call past the burst should be refused with a wait"
        );
    }

    /// A call that costs five spends five, so a budget measures work rather than requests.
    ///
    /// Without this, one authorized federated search buys as many index searches as the caller
    /// cares to name, and the per-key budget bounds nothing that matters.
    #[test]
    fn a_call_spends_what_it_costs() {
        let limiter = limiter(60, 10);
        let start = Instant::now();
        assert_eq!(limiter.check_at(Some("k1"), 5, start), Verdict::Allow);
        assert_eq!(limiter.check_at(Some("k1"), 5, start), Verdict::Allow);
        assert!(
            matches!(
                limiter.check_at(Some("k1"), 1, start),
                Verdict::Deny { retry_after_secs } if retry_after_secs >= 1
            ),
            "ten tokens spent on two calls should leave nothing for a third"
        );
    }

    /// A cost larger than the whole bucket empties it rather than being unaffordable forever.
    ///
    /// The alternative is a caller refused permanently, told each time to wait a number of
    /// seconds that will never be enough — a limiter that cannot be satisfied is a limiter
    /// that lies.
    #[test]
    fn a_cost_above_the_whole_budget_spends_the_budget() {
        let limiter = limiter(60, 3);
        let start = Instant::now();
        assert_eq!(
            limiter.check_at(Some("k1"), 100, start),
            Verdict::Allow,
            "a full bucket should afford a cost it cannot hold"
        );
        assert!(
            matches!(limiter.check_at(Some("k1"), 1, start), Verdict::Deny { .. }),
            "and the bucket should now be empty"
        );
    }

    /// Waiting the advertised time has to actually work. A `retry_after` a caller obeys and
    /// is still refused for trains agents to ignore it.
    #[test]
    fn waiting_the_advertised_time_earns_another_call() {
        let limiter = limiter(60, 1);
        let start = Instant::now();
        assert_eq!(limiter.check_at(Some("k1"), 1, start), Verdict::Allow);

        let Verdict::Deny { retry_after_secs } = limiter.check_at(Some("k1"), 1, start) else {
            panic!("the second immediate call should be refused");
        };
        let later = start + Duration::from_secs(retry_after_secs);
        assert_eq!(
            limiter.check_at(Some("k1"), 1, later),
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
            assert_eq!(limiter.check_at(Some("noisy"), 1, start), Verdict::Allow);
        }
        assert!(matches!(
            limiter.check_at(Some("noisy"), 1, start),
            Verdict::Deny { .. }
        ));
        assert_eq!(
            limiter.check_at(Some("quiet"), 1, start),
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
            assert_eq!(limiter.check_at(Some("k1"), 1, start), Verdict::Allow);
        }
        // Away for an hour: at 60/minute that is 3 600 tokens of notional credit.
        let much_later = start + Duration::from_secs(3_600);
        for i in 0..3 {
            assert_eq!(
                limiter.check_at(Some("k1"), 1, much_later),
                Verdict::Allow,
                "refilled token {i}"
            );
        }
        assert!(
            matches!(
                limiter.check_at(Some("k1"), 1, much_later),
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
                limiter.check_at(Some("k1"), 1, start),
                Verdict::Allow,
                "call {i} inside the implied burst"
            );
        }
        assert!(matches!(
            limiter.check_at(Some("k1"), 1, start),
            Verdict::Deny { .. }
        ));
    }
}
