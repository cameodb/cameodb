//! Delete by query across the node and the cluster (ROADMAP S1).
//!
//! A shard deletes the documents it holds that the query matches — see
//! `storage::HybridStore::delete_query_matches` for what may be matched and how — so the query
//! goes to every shard of every node, whatever the index's routing field. The node that received
//! the request runs it twice: a count everywhere, on which the caller's limit is decided once,
//! then the delete. Neither phase is atomic across nodes; the tally names what did not answer.
//!
//! Neither phase holds what it matched: the count keeps a number, and the delete keeps at most
//! [`DELETE_FIND_IDS`] ids per node and counts the rest as `remaining`. The finds share the read
//! pool with searches, and take at most half of it at once — see [`find_width`].
//!
//! The delete shares every shard's writer thread with the index's regular writes, and is cut
//! so as not to hold them: [`DELETE_ROUND`] ids per round across the shards that hold matches,
//! each shard's share one small transaction that the shard awaits before sending its next — so
//! a write that arrived meanwhile is ahead of the next round in its shard's channel. Shards
//! pace themselves independently. No round decides a commit; each shard is committed once at
//! the end — awaited with `refresh`, posted without a wait otherwise — and a delete that would
//! run past the request's budget stops between rounds, posts its commits, and says how many ids
//! remain — the same request sent again continues it.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use storage::{IndexSchema, StoreError};
use uuid::Uuid;

use super::request_started_at;
use super::router::RouterActor;
use super::search::{holds_no_index, holds_no_such_index, search_window_for};
use super::shard::MicroshardActor;
use super::{ClientOp, OrchestratorError, RemoteVerdict};

/// The ids a node deletes in one round, spread over its shards: the size a bulk delete has,
/// and with sixteen shards some six hundred ids per shard per transaction.
pub(super) const DELETE_ROUND: usize = 10_000;

/// The most ids one request's find keeps on a node, split evenly over its shards — some
/// sixty megabytes of ids, whatever the query matches. A delete that matches more deletes
/// what it found, answers the rest as `remaining`, and the request that continues it finds
/// them again. Never under a round per shard.
pub(super) const DELETE_FIND_IDS: usize = 1_000_000;

/// The share of the read pool a delete by query's find may hold at once: half of it, so the
/// searches that share the pool keep the other half however many shards the find walks.
pub(super) fn find_width(read_pool_width: usize) -> usize {
    (read_pool_width / 2).max(1)
}

/// What the work after the last round may still cost before the answer is out: the posted
/// commits and the response path itself. The stop check reserves it, or the answer races
/// the request timeout and loses — which is how the deadline it is for came to read as a
/// `408` while carrying a perfectly good tally.
///
/// A floor, not the figure itself: the tail is mostly scheduler latency on a node whose
/// writer threads and merges are busy, so it scales with the budget —
/// `max(ANSWER_TAIL, budget / 10)`, capped at 3 s. Measured on a loaded 16-shard node, the
/// real tail ran past half a second; the floor alone lost the race to the timeout twice.
const ANSWER_TAIL: Duration = Duration::from_millis(250);
const ANSWER_TAIL_MAX: Duration = Duration::from_secs(3);

/// Which phase of a delete by query a node runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeleteMode {
    /// Find the matches and count them; delete nothing.
    Count,
    /// Find the matches and delete them.
    Delete,
}

/// What a delete by query found and removed — one shard's, one node's, or the cluster's sum.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteTally {
    /// Documents the query matched exactly.
    pub matched: usize,
    /// Documents deleted; `0` for a count.
    pub deleted: usize,
    /// Candidates a `text` clause's stored-value check turned away.
    pub skipped_inexact: usize,
    /// Matches found and not deleted, because the request's budget ran out first. The same
    /// request sent again continues from here.
    #[serde(default)]
    pub remaining: usize,
    /// Shards asked, and how many of them failed.
    pub shards: usize,
    pub shards_failed: usize,
    /// Nodes asked — this one included — and those that did not answer.
    #[serde(default)]
    pub nodes_contacted: usize,
    #[serde(default)]
    pub nodes_failed: Vec<String>,
    /// Why each failed shard or node failed.
    #[serde(default)]
    pub errors: Vec<String>,
}

impl DeleteTally {
    fn add(&mut self, other: DeleteTally) {
        self.matched += other.matched;
        self.deleted += other.deleted;
        self.skipped_inexact += other.skipped_inexact;
        self.remaining += other.remaining;
        self.shards += other.shards;
        self.shards_failed += other.shards_failed;
        self.errors.extend(other.errors);
    }
}

/// A refusal of the query itself: every shard holds the same schema, so one says it for all.
fn is_refusal(err: &OrchestratorError) -> bool {
    matches!(err, OrchestratorError::Validation(_))
        || matches!(err.verdict(), RemoteVerdict::BadRequest)
}

/// Run one phase on every shard of this node.
///
/// Both phases find the matches on every shard; a delete then removes them in rounds — see the
/// module doc — and commits each shard that deleted anything once at the end: awaited when
/// `refresh` asks for it and the delete finished, posted without a wait otherwise.
pub(super) async fn delete_by_query_on_shards(
    shards: &HashMap<Uuid, MicroshardActor>,
    schema: &IndexSchema,
    index: &str,
    query: &str,
    mode: DeleteMode,
    refresh: bool,
    budget: Option<Duration>,
) -> Result<JsonValue, OrchestratorError> {
    if holds_no_index(schema) {
        return Err(OrchestratorError::Storage(StoreError::IndexNotFound(
            index.to_string(),
        )));
    }
    // At most `find_width` shards walk at once: a find holds a read thread for as long as
    // it walks, and one per shard took the whole pool from the searches beside it.
    let width = shards
        .values()
        .next()
        .map_or(1, |shard| find_width(shard.read_pool_width()));
    let cap = (DELETE_FIND_IDS / shards.len().max(1)).max(DELETE_ROUND);
    let finds: Vec<_> = shards
        .values()
        .map(|shard| find_on_shard(shard, index, query, mode, cap))
        .collect();
    let found: Vec<_> = futures::stream::iter(finds)
        .buffer_unordered(width)
        .collect()
        .await;

    let mut tally = DeleteTally {
        shards: shards.len(),
        ..Default::default()
    };
    let mut pending: Vec<(&MicroshardActor, Vec<String>)> = Vec::new();
    for (shard, outcome) in found {
        match outcome {
            Ok(found) => {
                tally.matched += found.matched;
                tally.skipped_inexact += found.skipped_inexact;
                tally.remaining += found.unexamined;
                if !found.ids.is_empty() {
                    pending.push((shard, found.ids));
                }
            }
            Err(err) if is_refusal(&err) => return Err(err),
            Err(err) => {
                tally.shards_failed += 1;
                tally.errors.push(err.to_string());
            }
        }
    }
    if pending.is_empty() {
        return tally_json(tally);
    }

    // A round's share per shard is cut over the shards that hold matches, not the node's
    // count: matches on one shard of sixteen would otherwise go in sixteenth-size rounds.
    let per_shard = DELETE_ROUND.div_ceil(pending.len()).max(1);
    let pace = Pace {
        started: request_started_at(),
        budget,
    };
    let outcomes = futures::future::join_all(pending.into_iter().map(|(shard, ids)| async move {
        (shard, run_rounds(shard, index, ids, per_shard, &pace).await)
    }))
    .await;

    let mut stopped = false;
    let mut to_commit: Vec<&MicroshardActor> = Vec::new();
    for (shard, outcome) in outcomes {
        tally.deleted += outcome.deleted;
        tally.remaining += outcome.remaining;
        stopped |= outcome.stopped;
        if let Some(err) = outcome.failed {
            tally.shards_failed += 1;
            tally.errors.push(err.to_string());
        }
        // A shard that deleted anything has tombstones to commit, however its rounds ended.
        if outcome.deleted > 0 {
            to_commit.push(shard);
        }
    }
    tracing::info!(
        index = %index,
        deleted = tally.deleted,
        remaining = tally.remaining,
        stopped,
        elapsed_ms = pace.started.elapsed().as_millis() as u64,
        "Delete by query: rounds done"
    );

    // Every shard that deleted anything is committed once, here: its rounds deferred the
    // decision, so nothing else will before the supervisor's idle timeout.
    if refresh && !stopped {
        // `refresh` is the caller asking for the deletes to be visible in the answer, so
        // these commits are awaited.
        let commits = futures::future::join_all(
            to_commit
                .iter()
                .map(|shard| shard.admin_commit_via_channel(index.to_string())),
        )
        .await;
        for err in commits.into_iter().filter_map(Result::err) {
            tally
                .errors
                .push(format!("commit after the delete failed: {err}"));
        }
    } else {
        // Posted, not awaited. Without `refresh` the caller did not ask to wait on it. After
        // a budget stop the caller would not get the answer in time if it did: a commit can
        // take seconds on a loaded writer, and the answer must land inside the budget it
        // stopped for. The commit still matters to the request that continues the delete,
        // which then finds the index as this one left it. If the post itself fails, the
        // supervisor's idle commit covers the shard all the same.
        for shard in &to_commit {
            if let Err(err) = shard.post_commit_to_writer(index.to_string()) {
                tally.errors.push(format!(
                    "commit after the delete could not be posted: {err}"
                ));
            }
        }
    }
    tally_json(tally)
}

/// One shard's find, in either phase.
struct ShardFind {
    /// Ids to delete; empty on a count.
    ids: Vec<String>,
    matched: usize,
    skipped_inexact: usize,
    /// Matches past the cap, which this request leaves for the one that continues it.
    unexamined: usize,
}

/// One shard's find for `mode`: a count keeps no id, a delete keeps about `cap`.
async fn find_on_shard<'a>(
    shard: &'a MicroshardActor,
    index: &str,
    query: &str,
    mode: DeleteMode,
    cap: usize,
) -> (&'a MicroshardActor, Result<ShardFind, OrchestratorError>) {
    let outcome = match mode {
        DeleteMode::Count => shard
            .count_delete_matches(index.to_string(), query.to_string())
            .await
            .map(|count| ShardFind {
                ids: Vec::new(),
                matched: count.matched,
                skipped_inexact: count.skipped_inexact,
                unexamined: 0,
            }),
        DeleteMode::Delete => shard
            .find_delete_matches(index.to_string(), query.to_string(), cap)
            .await
            .map(|found| ShardFind {
                // Candidates past the cap are matches this request does not delete.
                matched: found.ids.len() + found.unexamined,
                ids: found.ids,
                skipped_inexact: found.skipped_inexact,
                unexamined: found.unexamined,
            }),
    };
    (shard, outcome)
}

/// The request clock a delete paces its rounds against.
#[derive(Clone, Copy)]
pub(super) struct Pace {
    pub(super) started: Instant,
    pub(super) budget: Option<Duration>,
}

impl Pace {
    /// Whether a round like the longest so far still fits, with the tail the stop owes the
    /// response — see [`ANSWER_TAIL`]. Unbounded without a budget.
    fn allows(&self, longest_round: Duration) -> bool {
        let Some(budget) = self.budget else {
            return true;
        };
        self.started.elapsed() + longest_round + answer_tail(budget) <= budget
    }
}

/// The time a node keeps back from `budget` for its answer — see [`ANSWER_TAIL`].
fn answer_tail(budget: Duration) -> Duration {
    (budget / 10).clamp(ANSWER_TAIL, ANSWER_TAIL_MAX)
}

/// How long the node that received a delete by query waits on each peer's answer.
///
/// A count — a dry run, or the phase a delete is decided on — finds and deletes nothing, so it
/// is waited on for the broadcast timeout, as a search's share is. A delete paces its rounds to
/// the request's budget on every peer and may use most of it, so it is waited on until half the
/// answer tail before the budget ends: after every peer's last round — each stops a whole tail
/// before it — and early enough that the answer, with what a hung peer did not say, still lands
/// inside the request timeout rather than as a `408` with no tally. Never past `remote_timeout`,
/// and without a budget, `remote_timeout` alone.
pub(super) fn peer_wait(
    mode: DeleteMode,
    budget: Option<Duration>,
    started: Instant,
    broadcast_timeout: Duration,
    remote_timeout: Duration,
) -> Duration {
    if mode == DeleteMode::Count {
        return broadcast_timeout;
    }
    let Some(budget) = budget else {
        return remote_timeout;
    };
    budget
        .saturating_sub(answer_tail(budget) / 2)
        .saturating_sub(started.elapsed())
        .min(remote_timeout)
}

/// What a shard's rounds came to.
#[derive(Debug, Default)]
pub(super) struct ShardRounds {
    pub(super) deleted: usize,
    /// Ids found and not deleted — the budget ran out, or a round failed.
    pub(super) remaining: usize,
    pub(super) stopped: bool,
    pub(super) failed: Option<OrchestratorError>,
}

/// A shard as the round loop sees it: something that deletes a round of ids. A trait so the
/// loop can be tested against a stand-in whose timing and failures the test controls.
pub(super) trait DeletesRounds {
    fn delete_round(
        &self,
        index: &str,
        ids: Vec<String>,
    ) -> impl Future<Output = Result<usize, OrchestratorError>> + Send;
}

impl DeletesRounds for MicroshardActor {
    async fn delete_round(
        &self,
        index: &str,
        ids: Vec<String>,
    ) -> Result<usize, OrchestratorError> {
        self.handle_deferred_batch_delete(index.to_string(), ids)
            .await
    }
}

/// One shard's rounds, at its own pace.
///
/// Shards run this concurrently and independently rather than in lockstep: in lockstep the
/// slowest shard gated every round and its time stood in for every shard's, so the budget
/// check stopped the fast ones early. Each shard's first round always runs, so a delete makes
/// progress whatever the budget — the find phases before it may have spent most of it — and
/// each later round is sent only while [`Pace::allows`] one like the longest so far. Rounds are
/// taken off the end of the list, which moves the ids rather than copying them; order is
/// nothing to a delete.
pub(super) async fn run_rounds<S: DeletesRounds>(
    shard: &S,
    index: &str,
    mut ids: Vec<String>,
    per_shard: usize,
    pace: &Pace,
) -> ShardRounds {
    let mut rounds = ShardRounds::default();
    let mut longest_round = Duration::ZERO;
    let mut first = true;
    while !ids.is_empty() {
        if !first && !pace.allows(longest_round) {
            rounds.stopped = true;
            break;
        }
        first = false;
        let round = ids.split_off(ids.len().saturating_sub(per_shard));
        let round_len = round.len();
        let round_started = Instant::now();
        match shard.delete_round(index, round).await {
            Ok(deleted) => rounds.deleted += deleted,
            Err(err) => {
                // The failed round and the shard's rest are left where they are: the request
                // that continues the delete finds them again.
                rounds.failed = Some(err);
                rounds.remaining += round_len;
                break;
            }
        }
        longest_round = longest_round.max(round_started.elapsed());
    }
    rounds.remaining += ids.len();
    rounds
}

fn tally_json(tally: DeleteTally) -> Result<JsonValue, OrchestratorError> {
    serde_json::to_value(tally).map_err(|err| OrchestratorError::Io(std::io::Error::other(err)))
}

impl RouterActor {
    /// Run one phase of a delete by query on this node and every peer, and sum the answers.
    ///
    /// A refusal of the query from any node is the answer; an index no node holds is a `404`;
    /// a node that fails or does not answer is named in the tally and the rest still count.
    pub(crate) async fn delete_by_query(
        &self,
        index: &str,
        query: &str,
        mode: DeleteMode,
        refresh: bool,
    ) -> Result<DeleteTally, OrchestratorError> {
        let op = ClientOp::DeleteByQuery {
            spent_ms: None,
            index: index.to_string(),
            query: query.to_string(),
            mode,
            refresh,
        };
        // Nothing here is paged; the window only rides the fan-out.
        let window = search_window_for(&op, 0);
        let fanout = self
            .broadcast_fanout(op, window, |op| self.handle_client_op(op))
            .await;

        let mut tally = DeleteTally::default();
        let mut absent = 0usize;
        let sources = 1 + fanout.remote.len();
        let answers = std::iter::once(("this node".to_string(), Ok(fanout.local))).chain(
            fanout
                .remote
                .into_iter()
                .map(|(node_id, answer)| (node_id.to_string(), answer)),
        );
        for (node, answer) in answers {
            tally.nodes_contacted += 1;
            let answer = match answer {
                Ok(answer) => answer,
                Err(elapsed) => {
                    tally.nodes_failed.push(node.clone());
                    tally
                        .errors
                        .push(format!("node {node}: no answer in time ({elapsed})"));
                    continue;
                }
            };
            match answer {
                Ok(value) => match serde_json::from_value::<DeleteTally>(value) {
                    Ok(part) => tally.add(part),
                    Err(err) => {
                        tally.nodes_failed.push(node.clone());
                        tally
                            .errors
                            .push(format!("node {node}: unreadable answer ({err})"));
                    }
                },
                Err(err) if is_refusal(&err) => return Err(err),
                Err(err) if holds_no_such_index(&err) => absent += 1,
                Err(err) => {
                    tally.nodes_failed.push(node.clone());
                    tally.errors.push(format!("node {node}: {err}"));
                }
            }
        }
        if absent == sources {
            return Err(OrchestratorError::Storage(StoreError::IndexNotFound(
                index.to_string(),
            )));
        }
        Ok(tally)
    }
}
