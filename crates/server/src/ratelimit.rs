//! Rate limiting: what one caller may spend on this node per unit time.
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
//!
//! **Two meters, not one.** Reads and writes are metered separately because they are bounded
//! in different units. A tool call or a search is one request costing one unit; a write is
//! *documents*, and `_bulk` carries five thousand of them in one request that would otherwise
//! cost the same single token as a search returning ten hits. A budget that cannot tell those
//! apart bounds nothing on the expensive side, which is the direction that also grows the disk.
//! So `tool_calls_per_minute` meters calls and `write_documents_per_minute` meters documents,
//! and an operator sets each against what it actually costs this node.
//!
//! **Whose bucket.** Keyed by `key_id` where there is one, and the map is then bounded by the
//! number of keys an operator issued — a caller cannot mint buckets, which is what would
//! otherwise make a limiter its own memory-exhaustion lever. Where there is no key the caller's
//! address is the next best distinction, and it is worth having: on a node with `[security]`
//! off, one shared bucket means the first client to spend it denies every other. That map is
//! bounded too; see [`MAX_ANONYMOUS_BUCKETS`].

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Mutex;
use std::time::Instant;

use serde::{Deserialize, Serialize};

/// What a caller may spend.
///
/// Off by default, both meters. An existing deployment that upgrades into this code must not
/// start refusing calls it used to serve — the same reasoning that keeps `[security] enabled`
/// off by default. In particular the write meter does *not* fall back to `tool_calls_per_minute`:
/// an operator who metered tool calls chose a number for tool calls, and quietly charging their
/// bulk ingest against it would be an upgrade that broke ingest.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct McpLimitsConfig {
    /// Sustained tool calls per minute per caller. `0` disables limiting entirely.
    ///
    /// Also meters `POST /api/{index}/search` and its streaming form, which ask the node for
    /// exactly the work the `search_index` tool does.
    #[serde(default)]
    pub tool_calls_per_minute: u32,

    /// How much of that allowance may be spent at once. `0` means "one minute's worth",
    /// which is the only default that cannot surprise: the bucket starts full and a caller
    /// under the sustained rate never notices the limiter exists.
    #[serde(default)]
    pub tool_call_burst: u32,

    /// Sustained *documents* per minute per caller on the write surface. `0` disables it.
    ///
    /// Documents rather than requests, because one request is not one unit of work here: a
    /// `_bulk` body may carry thousands, and a stream is unbounded until its body limit. Every
    /// write route is charged what it actually asks the node to index or remove — one for a
    /// single write or delete, the batch size for `_bulk` and `_bulk/delete`, and one charge
    /// per micro-batch for the NDJSON stream, which is the only route whose total is not known
    /// until it has been read.
    #[serde(default)]
    pub write_documents_per_minute: u32,

    /// How much of the write allowance may be spent at once. `0` means one minute's worth.
    ///
    /// Worth setting above the sustained rate here more often than on the read side: an import
    /// is one burst of many documents followed by nothing, and a burst equal to a minute's
    /// sustained rate refuses the second half of a file that a burst of twice that accepts
    /// whole.
    #[serde(default)]
    pub write_burst: u32,

    /// The largest `limit` an MCP search may ask for.
    ///
    /// Unlike the rates above, this one is always in force: a rate of `0` means an agent may
    /// call as often as it likes, but there is no reading of "no ceiling at all" that is a
    /// number, and an absent ceiling is a caller deciding how many hits the node builds for
    /// one request. Raise it if the deployment can afford to; `0` is refused at load rather
    /// than read as unlimited, because a bound whose zero inverts its meaning is a trap.
    #[serde(default = "default_max_search_limit")]
    pub max_search_limit: usize,

    /// The most indexes one `search_across_indexes` call may name.
    ///
    /// The other half of what one call may cost, and always in force for the same reason
    /// `max_search_limit` is. Each name is a scatter-gather across that index's shards, so this
    /// is a multiplier on everything the call does — which is why the rate limiter above
    /// charges a federated search per index named rather than per call. `0` is refused at load
    /// rather than read as unlimited.
    #[serde(default = "default_max_federated_indexes")]
    pub max_federated_indexes: usize,

    /// Shortest prefix, in characters, that `field:pre*` is expanded for. `0` expands any.
    ///
    /// A prefix becomes a range over the term dictionary, and a range walks every term it covers
    /// with no ceiling. Measured on 10M documents per shard (ROADMAP M8): on a field of hashes
    /// one character covered 625k terms and cost 165 ms of a read thread per shard, two cost
    /// 11 ms, three under 1 ms — each character divides the cost by the size of the field's
    /// alphabet, and nothing else bounds it. A shorter prefix is not refused; it matches the term
    /// as written and the response says so. Unlike the two ceilings above, `0` is honoured as
    /// unlimited: nothing about "expand every prefix" is incoherent, it is only expensive.
    #[serde(default = "default_min_prefix_length")]
    pub min_prefix_length: usize,

    /// Whether a prefix naming no field — a bare `pre*` — searches the default fields.
    ///
    /// On by default. Tantivy's grammar has no unqualified prefix: it drops the `*` and matches
    /// `pre` as a term. On, the node rewrites it into one prefix range per text field that an
    /// unqualified term would search, OR'd together, with `min_prefix_length` applied to each.
    /// The cost is one prefix per default field, which `max_default_fields` bounds — that bound is
    /// what let this default on. Off, a bare prefix matches the literal term and is reported.
    #[serde(default = "default_expand_unqualified_prefix")]
    pub expand_unqualified_prefix: bool,

    /// Most fields an unqualified term searches. `0` searches every one.
    ///
    /// An unqualified term is one clause per default field — every indexed text, string and
    /// JSON field — and its cost grows worse than linearly with their number. Measured on one
    /// shard (ROADMAP M8), where the fields share a vocabulary: at 1M documents a 5-term query
    /// cost 120 ms across 32 fields and 513 ms across 64; at 200k documents it cost 5.2 s across
    /// 400. A query that names its field is unaffected at any width. Past the cap the index's
    /// declared `default_fields`, in order, or else its fields by name, are searched up to the
    /// cap — narrowed, not refused.
    #[serde(default = "default_max_default_fields")]
    pub max_default_fields: usize,

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

/// The prefix floor when an operator sets none.
///
/// Two, because one character is where the cost stops being a function of the query and starts
/// being a function of the field: 165 ms per shard against 11 ms for two on the measured hash
/// field, and it grows with the shard. Two still serves the short prefixes people type — `en*`,
/// an id's first pair — which three would take away.
pub const DEFAULT_MIN_PREFIX_LENGTH: usize = 2;

fn default_min_prefix_length() -> usize {
    DEFAULT_MIN_PREFIX_LENGTH
}

/// Whether a bare `pre*` searches the default fields when an operator says nothing: yes. Its cost
/// is one prefix per default field, and `max_default_fields` bounds how many there are.
fn default_expand_unqualified_prefix() -> bool {
    true
}

/// The default-field cap when an operator sets none.
///
/// Generous on purpose: a handful of text fields is the ordinary schema and even a wide CSV or
/// log import rarely reaches this many. What it stops is the far end of the curve, where one
/// unqualified query costs seconds per shard.
pub const DEFAULT_MAX_DEFAULT_FIELDS: usize = 64;

fn default_max_default_fields() -> usize {
    DEFAULT_MAX_DEFAULT_FIELDS
}

/// The query policy a node runs with when `[security.limits]` says nothing.
pub(crate) fn default_query_policy() -> storage::QueryPolicy {
    McpLimitsConfig::default().query_policy()
}

/// The fan-out bound when an operator sets none.
///
/// Twenty indexes is where one federated call stops being one call: a caller that wants the
/// whole catalogue is asking a different question, and `list_indexes` answers it in one request.
fn default_max_federated_indexes() -> usize {
    cameodb_mcp::DEFAULT_MAX_FEDERATED_INDEXES
}

/// Written out rather than derived, so that a config built in code and one parsed from an
/// absent `[security.limits]` are the same config. A derived `Default` would leave
/// `max_search_limit` and `max_federated_indexes` at zero, which is the one value each of them
/// refuses.
impl Default for McpLimitsConfig {
    fn default() -> Self {
        Self {
            tool_calls_per_minute: 0,
            tool_call_burst: 0,
            write_documents_per_minute: 0,
            write_burst: 0,
            max_search_limit: default_max_search_limit(),
            max_federated_indexes: default_max_federated_indexes(),
            min_prefix_length: default_min_prefix_length(),
            expand_unqualified_prefix: default_expand_unqualified_prefix(),
            max_default_fields: default_max_default_fields(),
            max_response_bytes: None,
        }
    }
}

impl McpLimitsConfig {
    /// The part of this section that storage applies to every query.
    pub fn query_policy(&self) -> storage::QueryPolicy {
        storage::QueryPolicy {
            min_prefix_length: self.min_prefix_length,
            expand_unqualified_prefix: self.expand_unqualified_prefix,
            max_default_fields: self.max_default_fields,
        }
    }

    /// Whether tool calls and searches are metered.
    pub fn enabled(&self) -> bool {
        self.tool_calls_per_minute > 0
    }

    /// Whether the write surface is metered.
    pub fn writes_metered(&self) -> bool {
        self.write_documents_per_minute > 0
    }

    /// What the tool and search meter is set to.
    fn tool_rate(&self) -> Rate {
        Rate {
            per_minute: self.tool_calls_per_minute,
            burst: self.tool_call_burst,
        }
    }

    /// What the write meter is set to.
    fn write_rate(&self) -> Rate {
        Rate {
            per_minute: self.write_documents_per_minute,
            burst: self.write_burst,
        }
    }
}

/// One allowance: a sustained rate, and how much of it may be spent at once.
#[derive(Debug, Clone, Copy)]
struct Rate {
    per_minute: u32,
    burst: u32,
}

impl Rate {
    fn enabled(self) -> bool {
        self.per_minute > 0
    }

    /// Bucket capacity in tokens.
    fn capacity(self) -> f64 {
        if self.burst > 0 {
            f64::from(self.burst)
        } else {
            f64::from(self.per_minute)
        }
    }

    /// Tokens added per second.
    fn refill_per_sec(self) -> f64 {
        f64::from(self.per_minute) / 60.0
    }
}

/// Who a request is metered as, decided once at the gate.
///
/// The distinction the limiter needs is not "authenticated or not" but "what bounds the number
/// of buckets this kind of caller can create" — one per issued key, one per address group, or
/// the single bucket left for a caller nothing tells apart.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Caller {
    /// Authenticated as this `key_id`.
    Key(String),
    /// No key, but the socket says where from.
    Address(IpAddr),
    /// Neither. Nothing distinguishes this caller from any other.
    Unattributed,
}

impl Caller {
    /// The subject for a request, preferring the key: an address is a fallback, not a second
    /// dimension. Two keys behind one NAT are two tenants, and metering them as one would
    /// make an operator's own key allocation meaningless.
    pub fn of(key_id: Option<String>, peer: Option<IpAddr>) -> Self {
        match (key_id, peer) {
            (Some(key_id), _) => Caller::Key(key_id),
            (None, Some(ip)) => Caller::Address(ip),
            (None, None) => Caller::Unattributed,
        }
    }
}

/// One caller's bucket.
#[derive(Debug)]
struct Bucket {
    tokens: f64,
    last: Instant,
}

impl Bucket {
    /// A caller met for the first time starts with a full allowance rather than an empty one —
    /// otherwise turning the limiter on would refuse everyone's first call.
    fn full(capacity: f64, now: Instant) -> Self {
        Self {
            tokens: capacity,
            last: now,
        }
    }

    /// What this bucket holds at `now`, capped at capacity — an idle caller gets a full
    /// bucket, not an unbounded credit for the time it was away.
    fn refilled(&self, capacity: f64, refill: f64, now: Instant) -> f64 {
        let elapsed = now.saturating_duration_since(self.last).as_secs_f64();
        (self.tokens + elapsed * refill).min(capacity)
    }
}

/// The bucket used when nothing identifies the caller.
const UNIDENTIFIED: &str = "<unidentified>";

/// How many address groups one meter will track at once.
///
/// The per-address bucket exists so that one anonymous client cannot spend everybody's
/// allowance; it must not become a way to spend the node's memory instead. Past this many
/// groups, buckets that have refilled to capacity are dropped — a full bucket and an absent
/// one admit exactly the same next request, so dropping one gives nobody a token they had not
/// already earned — and if every tracked group is still spending, further addresses share the
/// [`UNIDENTIFIED`] bucket. That is the behaviour this node had before there were per-address
/// buckets at all, so the worst case under an address flood is no worse than the old floor.
const MAX_ANONYMOUS_BUCKETS: usize = 4_096;

/// Every bucket one meter holds.
#[derive(Debug, Default)]
struct Buckets {
    /// Keyed by `key_id`, plus [`UNIDENTIFIED`]. Bounded by the configured key ring.
    keyed: HashMap<String, Bucket>,
    /// Keyed by address group. Bounded by [`MAX_ANONYMOUS_BUCKETS`].
    anon: HashMap<String, Bucket>,
}

/// Which map a caller's bucket lives in, once the decision has been made.
///
/// Separated from taking the bucket out so that each arm below borrows one map once. Deciding
/// and borrowing in the same expression is the shape the borrow checker refuses, and working
/// around it with a lookup per arm would hash the same key twice.
enum Slot {
    Keyed(String),
    Anon(String),
}

impl Buckets {
    /// Where this caller's bucket belongs, making room for it if that is possible and falling
    /// back to the shared bucket if it is not.
    fn slot_for(&mut self, caller: &Caller, capacity: f64, refill: f64, now: Instant) -> Slot {
        match caller {
            Caller::Key(key_id) => Slot::Keyed(key_id.clone()),
            Caller::Unattributed => Slot::Keyed(UNIDENTIFIED.to_string()),
            Caller::Address(ip) => {
                let group = address_group(*ip);
                if self.anon.contains_key(&group) {
                    return Slot::Anon(group);
                }
                if self.anon.len() >= MAX_ANONYMOUS_BUCKETS {
                    self.drop_full_anonymous(capacity, refill, now);
                }
                if self.anon.len() >= MAX_ANONYMOUS_BUCKETS {
                    Slot::Keyed(UNIDENTIFIED.to_string())
                } else {
                    Slot::Anon(group)
                }
            }
        }
    }

    /// Forget every anonymous caller whose bucket has refilled to capacity.
    ///
    /// Nothing is given away: the caller this frees space from would have been admitted by its
    /// own full bucket, and is admitted by the fresh one it gets on its next request.
    fn drop_full_anonymous(&mut self, capacity: f64, refill: f64, now: Instant) {
        self.anon
            .retain(|_, bucket| bucket.refilled(capacity, refill, now) < capacity);
    }

    /// The bucket itself, created full if this is the first time the caller has been seen.
    fn bucket(&mut self, slot: Slot, capacity: f64, now: Instant) -> &mut Bucket {
        match slot {
            Slot::Keyed(key) => self
                .keyed
                .entry(key)
                .or_insert_with(|| Bucket::full(capacity, now)),
            Slot::Anon(group) => self
                .anon
                .entry(group)
                .or_insert_with(|| Bucket::full(capacity, now)),
        }
    }
}

/// The unit an anonymous bucket is kept per.
///
/// One address for IPv4. The /64 for IPv6, because a single host is routinely handed a whole
/// one: metering per address there would let one machine mint 2^64 buckets, which is the
/// unbounded-map problem that keying by `key_id` exists to avoid. An IPv4-mapped address folds
/// back to its IPv4 form, so a dual-stack listener does not hand the same caller two buckets.
fn address_group(ip: IpAddr) -> String {
    match ip {
        IpAddr::V4(v4) => v4.to_string(),
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => v4.to_string(),
            None => {
                let s = v6.segments();
                format!("{:x}:{:x}:{:x}:{:x}::/64", s[0], s[1], s[2], s[3])
            }
        },
    }
}

/// One allowance and the buckets that account for it.
#[derive(Debug)]
struct Meter {
    rate: Rate,
    buckets: Mutex<Buckets>,
}

impl Meter {
    fn new(rate: Rate) -> Self {
        Self {
            rate,
            buckets: Mutex::new(Buckets::default()),
        }
    }

    fn check_at(&self, caller: &Caller, cost: u32, now: Instant) -> Verdict {
        if !self.rate.enabled() {
            return Verdict::Allow;
        }
        let capacity = self.rate.capacity();
        let refill = self.rate.refill_per_sec();
        // A cost above the bucket's whole capacity would never be affordable, and the caller
        // would be refused forever with a retry time that never comes true. Spending the
        // bucket dry is the honest charge: it is everything the caller has.
        let cost = f64::from(cost.max(1)).min(capacity);

        // A poisoned lock here must not take the node down, and must not silently stop
        // enforcing either. Recovering the guard keeps the limiter working: the data behind
        // it is a set of counters, and a torn counter costs at most one call's accuracy.
        let mut buckets = self
            .buckets
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        let slot = buckets.slot_for(caller, capacity, refill, now);
        let bucket = buckets.bucket(slot, capacity, now);

        bucket.tokens = bucket.refilled(capacity, refill, now);
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

/// Both meters: what a caller may ask of this node, and what it may write to it.
#[derive(Debug)]
pub struct RateLimiter {
    tools: Meter,
    writes: Meter,
}

impl RateLimiter {
    pub fn new(config: McpLimitsConfig) -> Self {
        Self {
            tools: Meter::new(config.tool_rate()),
            writes: Meter::new(config.write_rate()),
        }
    }

    /// Spend `cost` tool-call tokens, or report how long to wait.
    ///
    /// The cost is what the call asks the node to do — a federated search over five indexes is
    /// five searches — so that the budget measures work rather than requests.
    pub fn check(&self, caller: &Caller, cost: u32) -> Verdict {
        self.tools.check_at(caller, cost, Instant::now())
    }

    /// Spend `documents` write tokens, or report how long to wait.
    ///
    /// Charged against a separate allowance from [`RateLimiter::check`], and in a different
    /// unit: see `write_documents_per_minute`.
    pub fn check_write(&self, caller: &Caller, documents: u32) -> Verdict {
        self.writes.check_at(caller, documents, Instant::now())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, Ipv6Addr};
    use std::time::Duration;

    fn limiter(per_minute: u32, burst: u32) -> RateLimiter {
        RateLimiter::new(McpLimitsConfig {
            tool_calls_per_minute: per_minute,
            tool_call_burst: burst,
            ..Default::default()
        })
    }

    fn write_limiter(per_minute: u32, burst: u32) -> RateLimiter {
        RateLimiter::new(McpLimitsConfig {
            write_documents_per_minute: per_minute,
            write_burst: burst,
            ..Default::default()
        })
    }

    fn key(id: &str) -> Caller {
        Caller::Key(id.to_string())
    }

    fn v4(a: u8, b: u8, c: u8, d: u8) -> Caller {
        Caller::Address(IpAddr::V4(Ipv4Addr::new(a, b, c, d)))
    }

    /// The tool meter, against a caller-supplied clock so refill can be tested without sleeping.
    fn tool_check(limiter: &RateLimiter, caller: &Caller, cost: u32, now: Instant) -> Verdict {
        limiter.tools.check_at(caller, cost, now)
    }

    /// The default has to be inert. An upgrade that quietly started refusing an agent's
    /// calls would be indistinguishable, from the agent's side, from the node breaking.
    #[test]
    fn a_limiter_with_no_configured_rate_allows_everything() {
        let limiter = limiter(0, 0);
        for _ in 0..1_000 {
            assert_eq!(limiter.check(&key("k1"), 1), Verdict::Allow);
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
                tool_check(&limiter, &key("k1"), 1, start),
                Verdict::Allow,
                "call {i} is within the burst"
            );
        }
        assert!(
            matches!(
                tool_check(&limiter, &key("k1"), 1, start),
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
        assert_eq!(tool_check(&limiter, &key("k1"), 5, start), Verdict::Allow);
        assert_eq!(tool_check(&limiter, &key("k1"), 5, start), Verdict::Allow);
        assert!(
            matches!(
                tool_check(&limiter, &key("k1"), 1, start),
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
            tool_check(&limiter, &key("k1"), 100, start),
            Verdict::Allow,
            "a full bucket should afford a cost it cannot hold"
        );
        assert!(
            matches!(
                tool_check(&limiter, &key("k1"), 1, start),
                Verdict::Deny { .. }
            ),
            "and the bucket should now be empty"
        );
    }

    /// Waiting the advertised time has to actually work. A `retry_after` a caller obeys and
    /// is still refused for trains agents to ignore it.
    #[test]
    fn waiting_the_advertised_time_earns_another_call() {
        let limiter = limiter(60, 1);
        let start = Instant::now();
        assert_eq!(tool_check(&limiter, &key("k1"), 1, start), Verdict::Allow);

        let Verdict::Deny { retry_after_secs } = tool_check(&limiter, &key("k1"), 1, start) else {
            panic!("the second immediate call should be refused");
        };
        let later = start + Duration::from_secs(retry_after_secs);
        assert_eq!(
            tool_check(&limiter, &key("k1"), 1, later),
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
            assert_eq!(
                tool_check(&limiter, &key("noisy"), 1, start),
                Verdict::Allow
            );
        }
        assert!(matches!(
            tool_check(&limiter, &key("noisy"), 1, start),
            Verdict::Deny { .. }
        ));
        assert_eq!(
            tool_check(&limiter, &key("quiet"), 1, start),
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
            assert_eq!(tool_check(&limiter, &key("k1"), 1, start), Verdict::Allow);
        }
        // Away for an hour: at 60/minute that is 3 600 tokens of notional credit.
        let much_later = start + Duration::from_secs(3_600);
        for i in 0..3 {
            assert_eq!(
                tool_check(&limiter, &key("k1"), 1, much_later),
                Verdict::Allow,
                "refilled token {i}"
            );
        }
        assert!(
            matches!(
                tool_check(&limiter, &key("k1"), 1, much_later),
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
                tool_check(&limiter, &key("k1"), 1, start),
                Verdict::Allow,
                "call {i} inside the implied burst"
            );
        }
        assert!(matches!(
            tool_check(&limiter, &key("k1"), 1, start),
            Verdict::Deny { .. }
        ));
    }

    /// C8's second half: one anonymous client spending its allowance must not refuse the next.
    ///
    /// With one shared bucket, the node with `[security]` off — which is the default, and the
    /// configuration an exposed node is most likely to be running — hands the whole budget to
    /// whoever asks first.
    #[test]
    fn two_anonymous_callers_do_not_share_a_bucket() {
        let limiter = limiter(60, 2);
        let start = Instant::now();
        for _ in 0..2 {
            assert_eq!(
                tool_check(&limiter, &v4(10, 0, 0, 1), 1, start),
                Verdict::Allow
            );
        }
        assert!(matches!(
            tool_check(&limiter, &v4(10, 0, 0, 1), 1, start),
            Verdict::Deny { .. }
        ));
        assert_eq!(
            tool_check(&limiter, &v4(10, 0, 0, 2), 1, start),
            Verdict::Allow,
            "a different address has its own bucket"
        );
    }

    /// A caller with no address at all still gets metered, and shares the one bucket left.
    #[test]
    fn callers_nothing_tells_apart_share_the_one_bucket() {
        let limiter = limiter(60, 2);
        let start = Instant::now();
        for _ in 0..2 {
            assert_eq!(
                tool_check(&limiter, &Caller::Unattributed, 1, start),
                Verdict::Allow
            );
        }
        assert!(matches!(
            tool_check(&limiter, &Caller::Unattributed, 1, start),
            Verdict::Deny { .. }
        ));
    }

    /// The per-address map must not be a memory lever. Past its bound, buckets that have
    /// refilled are dropped and anything further shares the unidentified bucket — so the
    /// map stays bounded no matter how many addresses arrive.
    #[test]
    fn the_anonymous_bucket_map_is_bounded() {
        let limiter = limiter(60, 2);
        let start = Instant::now();
        // Every one of these spends its whole bucket, so none of them can be dropped as full.
        for i in 0..(MAX_ANONYMOUS_BUCKETS + 5_000) as u32 {
            let caller = Caller::Address(IpAddr::V4(Ipv4Addr::from(i)));
            for _ in 0..2 {
                tool_check(&limiter, &caller, 1, start);
            }
        }
        let held = limiter
            .tools
            .buckets
            .lock()
            .expect("the limiter's own lock")
            .anon
            .len();
        assert!(
            held <= MAX_ANONYMOUS_BUCKETS,
            "the anonymous map held {held} buckets, over its {MAX_ANONYMOUS_BUCKETS} bound"
        );
    }

    /// A full bucket is the one thing safe to forget: the caller it belonged to is admitted
    /// by the fresh bucket it gets next time, exactly as it would have been by the old one.
    #[test]
    fn room_is_made_by_forgetting_callers_that_owe_nothing() {
        let limiter = limiter(60, 2);
        let start = Instant::now();
        for i in 0..MAX_ANONYMOUS_BUCKETS as u32 {
            tool_check(
                &limiter,
                &Caller::Address(IpAddr::V4(Ipv4Addr::from(i))),
                1,
                start,
            );
        }
        // A minute later every one of those has refilled, so the newcomer gets a bucket of its
        // own rather than being pushed into the shared one.
        let later = start + Duration::from_secs(60);
        let newcomer = v4(203, 0, 113, 7);
        for _ in 0..2 {
            assert_eq!(tool_check(&limiter, &newcomer, 1, later), Verdict::Allow);
        }
        assert!(
            matches!(
                tool_check(&limiter, &newcomer, 1, later),
                Verdict::Deny { .. }
            ),
            "the newcomer should be metered in a bucket of its own"
        );
        assert_eq!(
            tool_check(&limiter, &Caller::Unattributed, 1, later),
            Verdict::Allow,
            "and the shared bucket should be untouched by it"
        );
    }

    /// A host is routinely given a whole /64, so the prefix is the tenant — not the address.
    #[test]
    fn one_ipv6_prefix_is_one_bucket() {
        let limiter = limiter(60, 2);
        let start = Instant::now();
        let first = Caller::Address(IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 1, 0, 0, 0, 1)));
        let second = Caller::Address(IpAddr::V6(Ipv6Addr::new(
            0x2001, 0xdb8, 0, 1, 0xdead, 0xbeef, 0, 2,
        )));
        let elsewhere = Caller::Address(IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 2, 0, 0, 0, 1)));

        assert_eq!(tool_check(&limiter, &first, 1, start), Verdict::Allow);
        assert_eq!(tool_check(&limiter, &second, 1, start), Verdict::Allow);
        assert!(
            matches!(
                tool_check(&limiter, &second, 1, start),
                Verdict::Deny { .. }
            ),
            "two addresses in one /64 should share one allowance"
        );
        assert_eq!(
            tool_check(&limiter, &elsewhere, 1, start),
            Verdict::Allow,
            "a different /64 is a different caller"
        );
    }

    /// A dual-stack listener reports an IPv4 client as `::ffff:a.b.c.d`. That must be the same
    /// bucket as the plain form, or a caller gets two allowances by reconnecting.
    #[test]
    fn an_ipv4_mapped_address_is_the_same_caller() {
        let limiter = limiter(60, 1);
        let start = Instant::now();
        let mapped = Caller::Address(IpAddr::V6(Ipv4Addr::new(198, 51, 100, 4).to_ipv6_mapped()));
        assert_eq!(
            tool_check(&limiter, &v4(198, 51, 100, 4), 1, start),
            Verdict::Allow
        );
        assert!(
            matches!(
                tool_check(&limiter, &mapped, 1, start),
                Verdict::Deny { .. }
            ),
            "the mapped form must not buy a second bucket"
        );
    }

    /// The two meters are separate budgets. An agent that has spent its searches must still be
    /// able to write, and an import must not consume the allowance searches are metered by.
    #[test]
    fn writes_and_tool_calls_are_metered_separately() {
        let limiter = RateLimiter::new(McpLimitsConfig {
            tool_calls_per_minute: 60,
            tool_call_burst: 1,
            write_documents_per_minute: 60,
            write_burst: 1,
            ..Default::default()
        });
        assert_eq!(limiter.check(&key("k1"), 1), Verdict::Allow);
        assert!(matches!(limiter.check(&key("k1"), 1), Verdict::Deny { .. }));
        assert_eq!(
            limiter.check_write(&key("k1"), 1),
            Verdict::Allow,
            "a spent search budget must not refuse a write"
        );
    }

    /// The write meter counts documents, so a bulk request costs what it asks the node to index.
    ///
    /// This is the whole reason the write surface has a meter of its own: charging one token per
    /// request would let `_bulk` carry a thousand documents for the price of a single write.
    #[test]
    fn a_bulk_write_is_charged_per_document() {
        let limiter = write_limiter(600, 100);
        assert_eq!(limiter.check_write(&key("k1"), 60), Verdict::Allow);
        assert_eq!(limiter.check_write(&key("k1"), 40), Verdict::Allow);
        assert!(
            matches!(limiter.check_write(&key("k1"), 1), Verdict::Deny { .. }),
            "a hundred documents should spend a hundred-document burst"
        );
    }

    /// Writes are unmetered unless an operator sets a write rate. Falling back to the tool rate
    /// would make an upgrade start refusing ingest for everyone who had metered tool calls.
    #[test]
    fn a_tool_rate_alone_does_not_meter_writes() {
        let limiter = limiter(1, 1);
        assert_eq!(limiter.check(&key("k1"), 1), Verdict::Allow);
        assert!(matches!(limiter.check(&key("k1"), 1), Verdict::Deny { .. }));
        for _ in 0..1_000 {
            assert_eq!(limiter.check_write(&key("k1"), 5_000), Verdict::Allow);
        }
    }
}
