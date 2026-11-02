//! Delete by query across the node and the cluster (ROADMAP S1).
//!
//! A shard deletes the documents it holds that the query matches — see
//! `storage::HybridStore::delete_query_matches` for what may be matched and how — so the query
//! goes to every shard of every node, whatever the index's routing field. The node that received
//! the request runs it twice: a count everywhere, on which the caller's limit is decided once,
//! then the delete. Neither phase is atomic across nodes; the tally names what did not answer.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use storage::{IndexSchema, StoreError};
use uuid::Uuid;

use super::router::RouterActor;
use super::search::{holds_no_index, holds_no_such_index, search_window_for};
use super::shard::MicroshardActor;
use super::{ClientOp, OrchestratorError, RemoteVerdict};

/// The ids a shard deletes in one writer transaction.
pub(super) const DELETE_CHUNK: usize = 10_000;

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
pub(super) async fn delete_by_query_on_shards(
    shards: &HashMap<Uuid, MicroshardActor>,
    schema: &IndexSchema,
    index: &str,
    query: &str,
    mode: DeleteMode,
    refresh: bool,
) -> Result<JsonValue, OrchestratorError> {
    if holds_no_index(schema) {
        return Err(OrchestratorError::Storage(StoreError::IndexNotFound(
            index.to_string(),
        )));
    }
    let outcomes = futures::future::join_all(shards.values().map(|shard| {
        shard.handle_delete_by_query(index.to_string(), query.to_string(), mode, refresh)
    }))
    .await;
    let mut tally = DeleteTally {
        shards: shards.len(),
        ..Default::default()
    };
    for outcome in outcomes {
        match outcome {
            Ok(shard) => tally.add(shard),
            Err(err) if is_refusal(&err) => return Err(err),
            Err(err) => {
                tally.shards_failed += 1;
                tally.errors.push(err.to_string());
            }
        }
    }
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
