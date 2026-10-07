//! `PUT /api/{index}/_config` across the cluster: one decision, applied on every node.
//!
//! A schema change used to be applied by the node that received it, alone. Its peers kept the
//! schema and built columns they had, nothing reconciled the two afterwards, and each node
//! judged a retype by its own documents: a node holding none of the index accepted a change that
//! the node holding the data refused, leaving one field typed two ways across the cluster.
//!
//! Now the receiving node asks every node first ([`ClientOp::PrepareSchema`]) and decides once,
//! from all the answers: a version one past the highest any node holds, the owner the cluster
//! already recorded, and whether the change may go ahead — a change the built index would act
//! against is refused while the cluster holds any document of the index. Then every node stores
//! that one schema ([`ClientOp::ApplySchema`]).
//!
//! `PATCH /api/{index}/_schema` takes the same round ([`patch_schema_cluster`]): its flag edits
//! are made to the schema the cluster holds, and the result is declared like any other. Phase
//! one then confirms the cluster still holds what the edit was made to; if another change landed
//! first, the edit is made again on that one.
//!
//! Run from the HTTP task, not the coordinator's mailbox, for the reason given on
//! [`super::delete_index_cluster`]: the coordinator hands over who to reach and this reaches them.

use std::collections::BTreeMap;

use serde_json::Value as JsonValue;
use tracing::{info, warn};
use uuid::Uuid;

use super::DeleteTargets;
use crate::distributed::ClusterStatus;
use crate::node::{ClientOp, NodeOrchestrator, OrchestratorError, SchemaApplied, SchemaReadiness};
use storage::{IndexSchema, SchemaFieldUpdate, SchemaRecord, SchemaVersion, count_drops};

/// How many times a change that finds its index held by another change asks again.
const BUSY_ATTEMPTS: u32 = 5;
/// The least a change waits before asking again, and the spread added at random: changes that
/// stepped back together ask again apart.
const BUSY_BACKOFF_MS: u64 = 50;
const BUSY_JITTER_MS: u64 = 200;
/// How many times a flag edit is made again on a schema that changed under it.
const MOVED_ATTEMPTS: u32 = 5;

/// A node a schema change reaches: this one, or a peer.
#[derive(Clone)]
enum Node {
    Local(kameo::actor::ActorRef<NodeOrchestrator>),
    Peer(Uuid),
}

impl Node {
    fn name(&self) -> String {
        match self {
            Node::Local(_) => "this node".to_string(),
            Node::Peer(id) => format!("node {id}"),
        }
    }
}

/// Every node, this one first, and how to reach the peers.
struct Reach {
    nodes: Vec<Node>,
    pool: Option<std::sync::Arc<crate::remote_peer_pool::RemotePeerPool>>,
}

impl Reach {
    /// Ask one node, bounded by the peer timeout. A peer's own refusal keeps its verdict; a peer
    /// that could not be asked is `PeerUnreachable`.
    async fn ask(&self, node: &Node, op: ClientOp) -> Result<JsonValue, OrchestratorError> {
        match node {
            Node::Local(orchestrator) => orchestrator.ask(op).await.map_err(|e| match e {
                kameo::error::SendError::HandlerError(err) => err,
                other => OrchestratorError::NotReady(format!(
                    "Failed to communicate with local orchestrator: {other}"
                )),
            }),
            Node::Peer(id) => {
                let Some(pool) = self.pool.as_ref() else {
                    return Err(OrchestratorError::PeerUnreachable {
                        message: format!("no remote peer pool on this node to reach node {id}"),
                    });
                };
                let id = *id;
                pool.converse(id, async {
                    let remote = crate::node::lookup_peer_orchestrator(pool, id).await?;
                    crate::node::remote_answer(remote.ask(&op).await)
                })
                .await
            }
        }
    }

    /// End `change`'s reservation of `index` on every node, for a change refused after phase
    /// one. Best effort: a node that does not hear it lets the reservation lapse.
    async fn release(&self, index: &str, change: Uuid) {
        let release = ClientOp::ReleaseSchemaChange {
            index: index.to_string(),
            change,
        };
        for (node, answer) in self.ask_all(&release).await {
            if let Err(err) = answer {
                warn!(index = %index, node = %node.name(), error = %err, "Schema change not released");
            }
        }
    }

    /// Finish on each of `missed` a drop of `index` recorded at `dropped_at` that it was down
    /// for — see [`ClientOp::FinishDrop`]. Every one or an error: a node that has not dropped
    /// keeps the old index's documents, and a schema applied over them would take them back.
    async fn finish_missed_drops(
        &self,
        index: &str,
        dropped_at: u64,
        missed: &[Node],
    ) -> Result<(), OrchestratorError> {
        let op = ClientOp::FinishDrop {
            index: index.to_string(),
            dropped_at,
        };
        let answers =
            futures::future::join_all(missed.iter().map(|node| self.ask(node, op.clone()))).await;
        let failed: Vec<String> = missed
            .iter()
            .zip(answers)
            .filter_map(|(node, answer)| answer.err().map(|err| format!("{}: {err}", node.name())))
            .collect();
        if failed.is_empty() {
            return Ok(());
        }
        Err(OrchestratorError::PeerUnreachable {
            message: format!(
                "index '{index}' was dropped while some nodes were down, and they could not be \
                 told to drop it too; retry once they answer. Not finished: {}",
                failed.join("; ")
            ),
        })
    }

    /// Ask every node at once; each answer beside the node it came from.
    async fn ask_all(&self, op: &ClientOp) -> Vec<(Node, Result<JsonValue, OrchestratorError>)> {
        futures::future::join_all(
            self.nodes
                .iter()
                .map(|node| async move { (node.clone(), self.ask(node, op.clone()).await) }),
        )
        .await
    }
}

fn decode<T: serde::de::DeserializeOwned>(
    node: &Node,
    value: JsonValue,
) -> Result<T, OrchestratorError> {
    serde_json::from_value(value).map_err(|e| OrchestratorError::PeerUnreachable {
        message: format!(
            "{} answered in a form this node cannot read ({e}); is it on an older build?",
            node.name()
        ),
    })
}

/// The answer to a change refused with nothing changed: the handler answers it `409`.
fn refused(index: &str, documents: u64, changes: &[String], reason: String) -> JsonValue {
    serde_json::json!({
        "acknowledged": false,
        "index": index,
        "documents": documents,
        "changes": changes,
        "reason": reason,
    })
}

/// Declare `schema` for `index` on every node of the cluster. See the module documentation.
///
/// `schema.tenant` is the calling key's, and is kept only when the index is new to the cluster.
/// Answers `{"acknowledged": true, …}` when every node stored it, `{"acknowledged": false, …}`
/// for a change refused with nothing changed, and `PeerUnreachable` (`503`) when a node could
/// not be reached — before anything was written, or after some nodes had stored it, which the
/// message says; the same request sent again finishes it.
pub(crate) async fn change_schema_cluster(
    targets: DeleteTargets,
    status: Option<ClusterStatus>,
    index: String,
    schema: IndexSchema,
) -> Result<JsonValue, OrchestratorError> {
    let reach = reach_whole_cluster(&targets, status.as_ref(), &index)?;
    match run_change(&reach, &index, schema, None).await? {
        Step::Applied(done) => Ok(serde_json::json!({
            "acknowledged": true,
            "index": index,
            "version": done.version,
            "nodes": done.nodes,
            "field_names": done.field_names,
        })),
        Step::Unchanged {
            version,
            field_names,
        } => Ok(serde_json::json!({
            "acknowledged": true,
            "index": index,
            "version": version,
            "nodes": reach.nodes.len(),
            "field_names": field_names,
            "unchanged": true,
        })),
        Step::Refused(body) => Ok(body),
        Step::Moved { .. } => unreachable!("only an edit moves"),
    }
}

/// Every node, this one first, when every configured member is connected; `503` otherwise.
fn reach_whole_cluster(
    targets: &DeleteTargets,
    status: Option<&ClusterStatus>,
    index: &str,
) -> Result<Reach, OrchestratorError> {
    let Some(local) = targets.local_orchestrator.clone() else {
        return Err(OrchestratorError::NotReady(
            "Local orchestrator not available".to_string(),
        ));
    };

    // Every configured member, connected, or nothing is changed: a node not asked keeps the old
    // schema, and the cluster is split until someone notices. Counted against the configured
    // membership, not the peers this node happens to know, as the schema canvass does.
    if let Some(status) = status.filter(|status| status.cluster_enabled) {
        let lost: Vec<String> = targets
            .peers
            .iter()
            .filter(|peer| !peer.connected)
            .map(|peer| format!("node {}", peer.node_id))
            .collect();
        if status.connected_nodes < status.total_nodes || !lost.is_empty() {
            return Err(OrchestratorError::PeerUnreachable {
                message: format!(
                    "index '{index}' was not changed: a schema change has to reach every node, \
                     and only {} of {} cluster nodes are connected{}. Retry once the cluster \
                     is whole",
                    status.connected_nodes,
                    status.total_nodes,
                    if lost.is_empty() {
                        String::new()
                    } else {
                        format!(" ({} lost)", lost.join(", "))
                    }
                ),
            });
        }
    }
    Ok(Reach {
        nodes: std::iter::once(Node::Local(local))
            .chain(targets.peers.iter().map(|peer| Node::Peer(peer.node_id)))
            .collect(),
        pool: targets.pool.clone(),
    })
}

/// What one round of a schema change came to.
enum Step {
    /// Stored on every node.
    Applied(Done),
    /// Refused with nothing changed; the body says why, and the handler answers it `409`.
    Refused(JsonValue),
    /// The cluster holds another schema than the edit was made to: the round was released, and
    /// the caller sends `proposal`, the edit made again to `held`.
    Moved {
        held: Box<IndexSchema>,
        proposal: Box<IndexSchema>,
    },
    /// Every node already holds the schema the change asks for, at this version; nothing was
    /// written.
    Unchanged {
        version: u64,
        field_names: Vec<String>,
    },
}

/// A change every node stored.
struct Done {
    version: u64,
    nodes: usize,
    field_names: Vec<String>,
    /// Fields marked indexed that a node holding documents has no column for yet.
    unbuilt: Vec<String>,
}

/// One round of a schema change: phase one on every node, the decision, phase two. With `edit`,
/// `schema` is that edit made to the schema the caller last saw, and phase one confirms it is
/// still the edit made to the schema the cluster holds — [`Step::Moved`] when it is not.
async fn run_change(
    reach: &Reach,
    index: &str,
    mut schema: IndexSchema,
    edit: Option<&FieldEdit>,
) -> Result<Step, OrchestratorError> {
    let index = index.to_string();

    // Phase one: what every node holds, and what the change would ask of it. Each node
    // reserves the index for this change until it is applied or released.
    let change = Uuid::new_v4();
    let tenant = schema.tenant.clone();
    let prepare = ClientOp::PrepareSchema {
        index: index.clone(),
        schema: schema.clone(),
        change,
    };
    let mut attempt = 0;
    let readiness = loop {
        attempt += 1;
        let mut readiness: Vec<(Node, SchemaReadiness)> = Vec::new();
        let mut unreachable = Vec::new();
        for (node, answer) in reach.ask_all(&prepare).await {
            match answer.and_then(|value| decode::<SchemaReadiness>(&node, value)) {
                Ok(ready) => readiness.push((node, ready)),
                // This node's own refusal — a field the declaration lacks, a tokenizer it
                // cannot build — is the caller's answer, verdict intact.
                Err(err) if matches!(node, Node::Local(_)) => {
                    reach.release(&index, change).await;
                    return Err(err);
                }
                // A peer's own refusal is the caller's answer too — a tokenizer an older build
                // cannot build is a 400, which no retry once the cluster is whole would change.
                Err(err @ OrchestratorError::Validation(_)) => {
                    reach.release(&index, change).await;
                    return Err(err);
                }
                Err(OrchestratorError::PeerUnreachable { message }) => unreachable.push(message),
                Err(err) => unreachable.push(format!("{}: {err}", node.name())),
            }
        }
        if !unreachable.is_empty() {
            reach.release(&index, change).await;
            return Err(OrchestratorError::PeerUnreachable {
                message: format!(
                    "index '{index}' was not changed: not every node could be asked. Retry once \
                     the cluster is whole. Not asked: {}",
                    unreachable.join("; ")
                ),
            });
        }
        // A node still holding the index at or below a drop the others recorded was down for
        // that drop. It drops now, and the round is asked again: a change made over its schema
        // would bring the dropped index back.
        let counted = count_drops(
            readiness.iter().filter_map(|(node, ready)| {
                ready
                    .current
                    .as_ref()
                    .map(|current| (node.clone(), current))
            }),
            0,
        );
        if !counted.missed.is_empty() {
            reach.release(&index, change).await;
            reach
                .finish_missed_drops(&index, counted.dropped_at, &counted.missed)
                .await?;
            if attempt >= BUSY_ATTEMPTS {
                return Err(OrchestratorError::PeerUnreachable {
                    message: format!(
                        "index '{index}' was not changed: nodes kept answering with an index \
                         that was dropped. Retry"
                    ),
                });
            }
            continue;
        }
        if !readiness.iter().any(|(_, ready)| ready.busy) {
            break readiness;
        }
        // Another change holds the index on some node. Two changes sent at once each reach
        // their own node first and find the other on the rest, so both step back here; each
        // waits a different while and asks again, and one goes first. The second then applies
        // on top of it, at the next version, and both callers are told the truth.
        reach.release(&index, change).await;
        if attempt >= BUSY_ATTEMPTS {
            return Ok(Step::Refused(refused(
                &index,
                0,
                &[],
                format!(
                    "another change to index '{index}' is being applied right now; nothing \
                     was changed. Read the schema again (GET /api/{index}/_config) and send \
                     this change again if it is still wanted"
                ),
            )));
        }
        let jitter = BUSY_BACKOFF_MS + (Uuid::new_v4().as_u128() % BUSY_JITTER_MS as u128) as u64;
        tokio::time::sleep(std::time::Duration::from_millis(jitter)).await;
    };

    // One version for every node: one past the highest any node holds, a dropped index's record
    // included, so the declaration is newer than everything the cluster has seen.
    let highest = readiness
        .iter()
        .filter_map(|(_, ready)| ready.current.as_ref().map(|current| current.version))
        .max()
        .unwrap_or(0);
    schema.version = highest.saturating_add(1);
    // The schema the cluster holds now, as it settles a disagreement: newest, then the tie-break.
    // No node holds a schema a drop overtook: the loop above finished those drops first.
    let held = readiness
        .iter()
        .filter_map(|(_, ready)| ready.current.clone())
        .filter(|current| current.state != storage::SchemaState::Dropped)
        .reduce(NodeOrchestrator::preferred_schema);
    if let Some(edit) = edit {
        // The edit is made to the schema the cluster holds now. When that is not what it was
        // made to, the round is given up and the caller sends the edit made again.
        let Some(view) = held_view(&readiness) else {
            reach.release(&index, change).await;
            return Err(OrchestratorError::Storage(
                storage::StoreError::IndexNotFound(index.clone()),
            ));
        };
        let edited = match edit.apply(&view) {
            Ok(edited) => edited,
            Err(err) => {
                reach.release(&index, change).await;
                return Err(err);
            }
        };
        let proposal = match edited {
            Edited::Unknown(outcome) => {
                reach.release(&index, change).await;
                return Ok(Step::Refused(NodeOrchestrator::schema_update_response(
                    &index, &outcome,
                )));
            }
            Edited::Proposal(proposal, _) => proposal,
        };
        let fingerprint = proposal.calculate_fingerprint();
        if fingerprint != schema.calculate_fingerprint() {
            reach.release(&index, change).await;
            return Ok(Step::Moved {
                held: Box::new(view),
                proposal,
            });
        }
        // Every node holds it already, at one version: nothing to write, and no version to
        // spend. The same schema at two versions is applied again, so the nodes agree on the
        // version too — a search compares both.
        let holds_view = |current: &IndexSchema| {
            !current.records_drop()
                && current.version == view.version
                && current.calculate_fingerprint() == fingerprint
        };
        if readiness
            .iter()
            .all(|(_, ready)| ready.current.as_ref().is_some_and(holds_view))
        {
            reach.release(&index, change).await;
            return Ok(Step::Unchanged {
                version: view.version,
                field_names: NodeOrchestrator::sorted_field_names(&view),
            });
        }
        // Every node holding the index holds it already, and the rest hold none yet — an index
        // just created, not yet asked for there. Nothing changes; those nodes take it up at its
        // version, which a version past it would announce as a change the index never had.
        if readiness.iter().all(|(_, ready)| {
            ready
                .current
                .as_ref()
                .is_none_or(|current| current.records_drop() || holds_view(current))
        }) {
            schema.version = view.version;
        }
    } else if let Some(held) = held.as_ref()
        && readiness.iter().all(|(_, ready)| {
            ready.unchanged
                && ready
                    .current
                    .as_ref()
                    .is_some_and(|current| current.version == held.version)
        })
    {
        // A declaration every node already holds, at one version — the same schema declared
        // again. Nothing is written, and no version is spent on it.
        reach.release(&index, change).await;
        return Ok(Step::Unchanged {
            version: held.version,
            field_names: NodeOrchestrator::sorted_field_names(held),
        });
    }
    // The owner is recorded once, by whoever created the index. A re-declaration keeps it, so
    // updating with an admin key (which carries no tenant) cannot unstamp an index and hand its
    // owner their quota back.
    let minting = held.is_none();
    if let Some(held) = held.as_ref() {
        schema.tenant = held.tenant.clone();
    } else if let Some(detail) = readiness
        .iter()
        .find_map(|(_, ready)| ready.quota_exceeded.clone())
    {
        reach.release(&index, change).await;
        return Err(OrchestratorError::QuotaExceeded {
            tenant: tenant.unwrap_or_default(),
            detail,
        });
    }

    let documents: u64 = readiness.iter().map(|(_, ready)| ready.documents).sum();
    let mut conflicts: Vec<String> = readiness
        .iter()
        .flat_map(|(_, ready)| ready.conflicts.iter().cloned())
        .collect();
    conflicts.sort();
    conflicts.dedup();
    if documents > 0 && !conflicts.is_empty() {
        let holders: Vec<String> = readiness
            .iter()
            .filter(|(_, ready)| ready.documents > 0)
            .map(|(node, ready)| format!("{} {}", node.name(), ready.documents))
            .collect();
        reach.release(&index, change).await;
        return Ok(Step::Refused(refused(
            &index,
            documents,
            &conflicts,
            format!(
                "index '{index}' holds {documents} documents ({}), and its built index cannot \
                 change under them: {}. Delete its documents (DELETE /api/{index} without \
                 delete_schema keeps the schema), apply this schema, then load again",
                holders.join(", "),
                conflicts.join("; ")
            ),
        )));
    }

    // Phase two: every node stores the one schema. This node first, so a refusal of its own —
    // the quota, re-checked where mints are serialised — stops the change before any peer
    // takes it.
    info!(
        index = %index,
        version = schema.version,
        nodes = reach.nodes.len(),
        minting,
        "Applying a schema change on every node"
    );
    let apply = |check_quota: bool| ClientOp::ApplySchema {
        index: index.clone(),
        schema: schema.clone(),
        check_quota,
        change,
    };
    let local_node = reach.nodes[0].clone();
    let local_answer = match reach
        .ask(&local_node, apply(minting))
        .await
        .and_then(|value| decode::<SchemaApplied>(&local_node, value))
    {
        Ok(answer) => answer,
        Err(err) => {
            reach.release(&index, change).await;
            return Err(err);
        }
    };
    let peers = Reach {
        nodes: reach.nodes[1..].to_vec(),
        pool: reach.pool.clone(),
    };
    let mut outcomes: Vec<(Node, Result<SchemaApplied, OrchestratorError>)> =
        vec![(local_node, Ok(local_answer))];
    outcomes.extend(
        peers
            .ask_all(&apply(false))
            .await
            .into_iter()
            .map(|(node, answer)| {
                let decoded = answer.and_then(|value| decode::<SchemaApplied>(&node, value));
                (node, decoded)
            }),
    );

    let mut applied = Vec::new();
    let mut superseded = Vec::new();
    let mut arrived = 0u64;
    let mut late_conflicts = Vec::new();
    let mut unconfirmed = Vec::new();
    let mut field_names = Vec::new();
    for (node, outcome) in outcomes {
        match outcome {
            Ok(outcome) if outcome.applied => {
                if field_names.is_empty() {
                    field_names = outcome.field_names;
                }
                applied.push(node);
            }
            Ok(outcome) if outcome.superseded => superseded.push(node.name()),
            Ok(outcome) => {
                arrived += outcome.arrived;
                late_conflicts.extend(outcome.conflicts);
            }
            Err(err) => unconfirmed.push(format!("{}: {err}", node.name())),
        }
    }

    // Documents reached the index between the two phases, and the change conflicts with them.
    // The nodes that took it are put back: the schema the cluster held, at a newer version
    // still, so every node agrees on it. They were empty a moment ago, so building them again
    // costs nothing; a node that has taken documents meanwhile refuses, and says so in its log.
    if arrived > 0 {
        late_conflicts.sort();
        late_conflicts.dedup();
        if let Some(mut previous) = held {
            previous.version = schema.version.saturating_add(1);
            let restore = ClientOp::ApplySchema {
                index: index.clone(),
                schema: previous,
                check_quota: false,
                change: Uuid::new_v4(),
            };
            for (node, answer) in reach.ask_all(&restore).await {
                if let Err(err) = answer {
                    warn!(
                        index = %index,
                        node = %node.name(),
                        error = %err,
                        "A refused schema change could not be undone on this node"
                    );
                }
            }
        }
        return Ok(Step::Refused(refused(
            &index,
            arrived,
            &late_conflicts,
            format!(
                "{arrived} documents reached index '{index}' while its schema was changing, and \
                 its built index cannot change under them: {}. The schema is unchanged; delete \
                 its documents (DELETE /api/{index} without delete_schema keeps the schema), \
                 apply this schema, then load again",
                late_conflicts.join("; ")
            ),
        )));
    }

    // Another change to the same index was applied at the same time, and the cluster prefers
    // it. Every node ends on that one — the rule is the same everywhere, whichever change
    // reached a node first — so this one is reported as not taken, rather than half taken.
    if !superseded.is_empty() {
        return Ok(Step::Refused(refused(
            &index,
            0,
            &[],
            format!(
                "another change to index '{index}' was applied at the same time and takes \
                 precedence ({} kept it). Read the schema again (GET /api/{index}/_config) and \
                 send the change again if it is still wanted",
                superseded.join(", ")
            ),
        )));
    }

    if !unconfirmed.is_empty() {
        // A node that is alive but did not confirm still holds the reservation; let it go now
        // rather than at its expiry, so the retry the message asks for is not answered busy.
        reach.release(&index, change).await;
        return Err(OrchestratorError::PeerUnreachable {
            message: format!(
                "index '{index}' schema version {} is stored on {} of {} nodes, and not \
                 confirmed on the rest. Send the same request again once the cluster is whole \
                 to finish it. Unconfirmed: {}",
                schema.version,
                applied.len(),
                reach.nodes.len(),
                unconfirmed.join("; ")
            ),
        });
    }

    let mut unbuilt: Vec<String> = readiness
        .iter()
        .filter(|(_, ready)| ready.documents > 0)
        .flat_map(|(_, ready)| ready.unbuilt.iter().cloned())
        .collect();
    unbuilt.sort();
    unbuilt.dedup();
    Ok(Step::Applied(Done {
        version: schema.version,
        nodes: applied.len(),
        field_names,
        unbuilt,
    }))
}

/// The schema the cluster holds, as an edit is made to it: the one it prefers, with every field
/// some node learned from a write and the preferred one lacks. Made to the preferred schema
/// alone, an edit stored on every node would drop such a field from the nodes that hold it.
fn held_view(readiness: &[(Node, SchemaReadiness)]) -> Option<IndexSchema> {
    merged_view(
        readiness
            .iter()
            .filter_map(|(_, ready)| ready.current.as_ref()),
    )
}

/// [`held_view`] over the schemas themselves, a dropped index's record left out. Drops are
/// counted before this is asked: no schema a drop removed is among them.
fn merged_view<'a>(schemas: impl Iterator<Item = &'a IndexSchema>) -> Option<IndexSchema> {
    let held: Vec<&IndexSchema> = schemas.filter(|current| !current.records_drop()).collect();
    let mut view = held
        .iter()
        .map(|current| (*current).clone())
        .reduce(NodeOrchestrator::preferred_schema)?;
    for current in held {
        merge_learned(&mut view, current);
    }
    Some(view)
}

/// Bring into `into` what `from` learned from writes: a learned field `into` lacks, and a wider
/// type for a learned field both hold without a column. A field `into` declared is left as
/// declared, and a declared field it lacks stays absent. Whether anything changed.
pub(crate) fn merge_learned(into: &mut IndexSchema, from: &IndexSchema) -> bool {
    let mut changed = false;
    for (name, field) in &from.fields {
        match into.fields.get_mut(name) {
            // Only a learned one: a declared field `into` lacks was removed on purpose.
            None if field.learned && !field.indexed => {
                into.fields.insert(name.clone(), field.clone());
                changed = true;
            }
            None => {}
            Some(held) if held.learned && !held.indexed && field.learned && !field.indexed => {
                let joined = held.field_type.widened(&field.field_type);
                if joined != held.field_type {
                    held.retype_learned(joined);
                    changed = true;
                }
            }
            Some(_) => {}
        }
    }
    changed
}

/// A `PATCH /_schema`: `indexed` flags, and the fields an unqualified term searches.
#[derive(Debug, Clone)]
pub(crate) struct FieldEdit {
    pub(crate) field_updates: BTreeMap<String, bool>,
    /// `Some(vec![])` clears the declaration; `None` leaves it as it is.
    pub(crate) default_fields: Option<Vec<String>>,
}

/// An edit made to a schema.
enum Edited {
    /// The schema the edit leaves, and which flags it changed.
    Proposal(Box<IndexSchema>, SchemaFieldUpdate),
    /// It names fields the schema does not have; nothing is changed.
    Unknown(SchemaFieldUpdate),
}

impl FieldEdit {
    /// This edit made to `held`. A default-field list the result would get wrong — a name it
    /// lacks, or a field the same edit stops indexing — is refused `400`.
    fn apply(&self, held: &IndexSchema) -> Result<Edited, OrchestratorError> {
        let mut next = held.clone();
        let mut outcome = SchemaFieldUpdate::default();
        for (name, indexed) in &self.field_updates {
            match next.fields.get_mut(name) {
                None => outcome.unknown.push(name.clone()),
                Some(field) if field.indexed == *indexed => outcome.unchanged.push(name.clone()),
                Some(field) => {
                    field.indexed = *indexed;
                    // An indexed field is pinned to its column: no longer learned, so turning
                    // it off again later does not let it widen over a column that exists.
                    if *indexed {
                        field.learned = false;
                    }
                    outcome.applied.push(name.clone());
                }
            }
        }
        if !outcome.unknown.is_empty() {
            return Ok(Edited::Unknown(outcome));
        }
        if let Some(list) = &self.default_fields {
            next.default_fields = (!list.is_empty()).then(|| list.clone());
        }
        if self.default_fields.is_some() || self.field_updates.values().any(|indexed| !indexed) {
            next.validate_default_fields().map_err(|reason| {
                OrchestratorError::Validation(if self.default_fields.is_some() {
                    reason
                } else {
                    format!(
                        "{reason}; it is listed in default_fields, so remove it from the list \
                         first, or in the same request"
                    )
                })
            })?;
        }
        Ok(Edited::Proposal(Box::new(next), outcome))
    }
}

/// What an edit came to across the cluster.
enum EditOutcome {
    /// Every node holds the edited schema, at `version`, stored by `nodes` of them — or held it
    /// already, in which case nothing was written. `outcome` is what the edit changed.
    Done {
        outcome: SchemaFieldUpdate,
        version: u64,
        nodes: usize,
    },
    /// Refused with nothing changed; the body says why.
    Refused(JsonValue),
}

/// Make `edit` to the schema the cluster holds, starting from `base`, and apply it on every node;
/// made again on whatever landed first, up to [`MOVED_ATTEMPTS`] times.
async fn run_edit(
    reach: &Reach,
    index: &str,
    base: IndexSchema,
    edit: &FieldEdit,
) -> Result<EditOutcome, OrchestratorError> {
    let mut proposal = match edit.apply(&base)? {
        Edited::Proposal(proposal, _) => proposal,
        Edited::Unknown(outcome) => {
            return Ok(EditOutcome::Refused(
                NodeOrchestrator::schema_update_response(index, &outcome),
            ));
        }
    };
    let mut held = base;
    for _ in 0..MOVED_ATTEMPTS {
        // What the edit changes, against the schema it was last made to.
        let Edited::Proposal(_, mut outcome) = edit.apply(&held)? else {
            unreachable!("made to this schema already");
        };
        match run_change(reach, index, (*proposal).clone(), Some(edit)).await? {
            Step::Applied(done) => {
                outcome.pending_reindex = done
                    .unbuilt
                    .into_iter()
                    .filter(|name| outcome.applied.contains(name))
                    .collect();
                return Ok(EditOutcome::Done {
                    outcome,
                    version: done.version,
                    nodes: done.nodes,
                });
            }
            Step::Unchanged { version, .. } => {
                // Already held everywhere: whatever this request asked is already so.
                outcome.unchanged.append(&mut outcome.applied);
                outcome.unchanged.sort();
                return Ok(EditOutcome::Done {
                    outcome,
                    version,
                    nodes: reach.nodes.len(),
                });
            }
            Step::Refused(body) => return Ok(EditOutcome::Refused(body)),
            Step::Moved {
                held: now,
                proposal: next,
            } => {
                held = *now;
                proposal = next;
            }
        }
    }
    Ok(EditOutcome::Refused(refused(
        index,
        0,
        &[],
        format!(
            "index '{index}' kept changing while this edit was being applied; nothing was \
             changed. Read the schema again (GET /api/{index}/_config) and send the edit again \
             if it is still wanted"
        ),
    )))
}

/// Apply `edit` to `index` on every node of the cluster. See the module documentation.
///
/// `base` is the schema the cluster held when the request arrived, which the first round edits.
/// Answers as `PATCH /_schema` always has — `updated_fields`, `unchanged_fields`, and
/// `pending_reindex_fields` with a note for a field marked indexed that a node holding documents
/// has no column for — plus the `version` and the `nodes` that stored it.
pub(crate) async fn patch_schema_cluster(
    targets: DeleteTargets,
    status: Option<ClusterStatus>,
    index: String,
    base: IndexSchema,
    edit: FieldEdit,
) -> Result<JsonValue, OrchestratorError> {
    let reach = reach_whole_cluster(&targets, status.as_ref(), &index)?;
    match run_edit(&reach, &index, base, &edit).await? {
        EditOutcome::Refused(body) => Ok(body),
        EditOutcome::Done {
            outcome,
            version,
            nodes,
        } => {
            let mut response = NodeOrchestrator::schema_update_response(&index, &outcome);
            if let Some(list) = &edit.default_fields {
                response["default_fields"] = if list.is_empty() {
                    JsonValue::Null
                } else {
                    serde_json::json!(list)
                };
            }
            response["version"] = serde_json::json!(version);
            response["nodes"] = serde_json::json!(nodes);
            Ok(response)
        }
    }
}

/// How long a reconcile waits before its first round, so a burst of writes teaching the same
/// fields is agreed in one round rather than one per batch.
const RECONCILE_SETTLE: std::time::Duration = std::time::Duration::from_millis(250);
/// The longest wait between rounds that could not finish, and how many rounds are tried before
/// the next learned field asks again.
const RECONCILE_BACKOFF_MAX: std::time::Duration = std::time::Duration::from_secs(30);
const RECONCILE_ATTEMPTS: u32 = 30;

/// Agreement on what writes taught the nodes, one index at a time.
///
/// A field that first arrives after an index exists is learned by whichever node receives it,
/// typed by the values that node saw. Each node that learns one asks for a round here: the
/// schema every node holds, merged — every learned field, at the type joining what each node
/// saw — is applied on every node as an empty edit, at one new version. Requests for an index
/// already in a round mark it to run once more after, so a burst of writes costs one or two
/// rounds, not one per batch. Run on a spawned task, never in an orchestrator's mailbox: a round
/// asks every orchestrator, this node's included.
#[derive(Debug, Default)]
pub(crate) struct SchemaReconciler {
    /// Indexes with a round running, and whether another was asked for meanwhile.
    pending: std::sync::Mutex<std::collections::HashMap<String, bool>>,
}

impl SchemaReconciler {
    /// Ask for a round for `index`; one is started unless one is running.
    pub(crate) fn request(
        self: &std::sync::Arc<Self>,
        index: &str,
        coordinator: kameo::actor::ActorRef<super::ClusterCoordinator>,
    ) {
        {
            let mut pending = self.pending.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(again) = pending.get_mut(index) {
                *again = true;
                return;
            }
            pending.insert(index.to_string(), false);
        }
        let reconciler = std::sync::Arc::clone(self);
        let index = index.to_string();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(RECONCILE_SETTLE).await;
                reconcile_until_agreed(&coordinator, &index).await;
                let mut pending = reconciler.pending.lock().unwrap_or_else(|p| p.into_inner());
                if pending.get(&index) == Some(&true) {
                    pending.insert(index.clone(), false);
                    continue;
                }
                pending.remove(&index);
                break;
            }
        });
    }
}

/// Rounds until one finishes, waiting longer after each that could not.
async fn reconcile_until_agreed(
    coordinator: &kameo::actor::ActorRef<super::ClusterCoordinator>,
    index: &str,
) {
    let mut wait = std::time::Duration::from_secs(1);
    for attempt in 1..=RECONCILE_ATTEMPTS {
        match reconcile_once(coordinator, index).await {
            Ok(Some(version)) => {
                info!(index = %index, version, "Learned fields agreed across the cluster");
                return;
            }
            Ok(None) => return,
            Err(reason) => {
                warn!(
                    index = %index,
                    attempt,
                    reason = %reason,
                    "Learned fields not agreed yet; trying again"
                );
            }
        }
        tokio::time::sleep(wait).await;
        wait = (wait * 2).min(RECONCILE_BACKOFF_MAX);
    }
    warn!(
        index = %index,
        "Learned fields still not agreed across the cluster; the next one learned asks again"
    );
}

/// One round: `Some(version)` when it stored the merged schema on every node, `None` when there
/// was nothing to do — no index, or every node already agreeing — and why it could not finish
/// otherwise.
async fn reconcile_once(
    coordinator: &kameo::actor::ActorRef<super::ClusterCoordinator>,
    index: &str,
) -> Result<Option<u64>, String> {
    let targets = coordinator
        .ask(super::GetDeleteTargets)
        .await
        .map_err(|e| format!("the cluster coordinator did not answer: {e}"))?;
    let status = coordinator.ask(super::GetStatus).await.ok();
    let reach = reach_whole_cluster(&targets, status.as_ref(), index).map_err(|e| e.to_string())?;
    // Read every node's schema first, holding nothing. When they all agree — the round another
    // node started already ran, or nothing was missing — there is nothing to do. Otherwise the
    // edit starts from the schema they would merge to, so the first round normally finishes.
    let held_op = ClientOp::GetRawSchema {
        index: index.to_string(),
        minting_by: None,
    };
    let mut answers = Vec::new();
    for (node, answer) in reach.ask_all(&held_op).await {
        let value = answer.map_err(|e| e.to_string())?;
        if !value.is_null() {
            let schema = decode::<IndexSchema>(&node, value).map_err(|e| e.to_string())?;
            answers.push((node, schema));
        }
    }
    // A node down for a drop the others recorded drops now; agreeing on its schema instead
    // would bring the dropped index back on every node.
    let counted = count_drops(answers, 0);
    if !counted.missed.is_empty() {
        reach
            .finish_missed_drops(index, counted.dropped_at, &counted.missed)
            .await
            .map_err(|e| e.to_string())?;
    }
    let held: Vec<IndexSchema> = counted
        .standing
        .into_iter()
        .map(|(_, schema)| schema)
        .collect();
    let Some(base) = merged_view(held.iter()) else {
        return Ok(None);
    };
    let fingerprint = base.calculate_fingerprint();
    let agreed = held.len() == reach.nodes.len()
        && held.iter().all(|schema| {
            schema.version == base.version && schema.calculate_fingerprint() == fingerprint
        });
    if agreed {
        return Ok(None);
    }
    let unchanged = FieldEdit {
        field_updates: BTreeMap::new(),
        default_fields: None,
    };
    match run_edit(&reach, index, base, &unchanged)
        .await
        .map_err(|e| e.to_string())?
    {
        EditOutcome::Done { version, .. } => Ok(Some(version)),
        EditOutcome::Refused(body) => Err(body["reason"].as_str().unwrap_or("refused").to_string()),
    }
}

/// What a sweep found this node should set right: drops it missed, as `(index, dropped_at)`,
/// and indexes whose live schema differs from a peer's.
#[derive(Debug, Default, PartialEq, Eq)]
struct SweepVerdict {
    finish: Vec<(String, u64)>,
    reconcile: Vec<String>,
}

/// Compare this node's schema records with its peers'. Only this node's own records are judged:
/// each peer runs the same comparison and sets right its own.
fn sweep_verdict(local: &[SchemaRecord], peers: &[SchemaRecord]) -> SweepVerdict {
    let mut verdict = SweepVerdict::default();
    for mine in local.iter().filter(|record| !record.records_drop()) {
        let theirs = count_drops(
            peers
                .iter()
                .filter(|record| record.index == mine.index)
                .map(|record| ((), record)),
            0,
        );
        if mine.dropped_by(theirs.dropped_at) {
            verdict.finish.push((mine.index.clone(), theirs.dropped_at));
        } else if theirs.standing.iter().any(|(_, record)| {
            !record.records_drop()
                && (record.version, record.thumbprint) != (mine.version, mine.thumbprint)
        }) {
            verdict.reconcile.push(mine.index.clone());
        }
    }
    verdict
}

/// Compare this node's schema records with every connected peer's, and set right this node's:
/// a drop it was down for is finished here, and an index whose live schema differs from a
/// peer's is handed to the reconcile. Run when a peer connects — a node back from being down,
/// or a split healing — and on a timer, for a connection whose sweep found the peer not ready.
pub(crate) async fn sweep_schemas(coordinator: &kameo::actor::ActorRef<super::ClusterCoordinator>) {
    let Ok(targets) = coordinator.ask(super::GetDeleteTargets).await else {
        return;
    };
    let Some(local) = targets.local_orchestrator.clone() else {
        return;
    };
    let reach = Reach {
        nodes: targets
            .peers
            .iter()
            .filter(|peer| peer.connected)
            .map(|peer| Node::Peer(peer.node_id))
            .collect(),
        pool: targets.pool.clone(),
    };
    if reach.nodes.is_empty() {
        return;
    }
    let local_node = Node::Local(local);
    let mine = match reach.ask(&local_node, ClientOp::SchemaRecords).await {
        Ok(value) => decode::<Vec<SchemaRecord>>(&local_node, value),
        Err(err) => Err(err),
    };
    let Ok(mine) = mine else {
        return;
    };
    let mut theirs = Vec::new();
    for (node, answer) in reach.ask_all(&ClientOp::SchemaRecords).await {
        // A peer not ready yet is left to the next sweep.
        if let Ok(records) = answer.and_then(|value| decode::<Vec<SchemaRecord>>(&node, value)) {
            theirs.extend(records);
        }
    }
    let verdict = sweep_verdict(&mine, &theirs);
    for (index, dropped_at) in verdict.finish {
        let op = ClientOp::FinishDrop {
            index: index.clone(),
            dropped_at,
        };
        if let Err(err) = reach.ask(&local_node, op).await {
            warn!(index = %index, error = %err, "Could not finish a drop this node missed");
        }
    }
    for index in verdict.reconcile {
        let _ = reach
            .ask(&local_node, ClientOp::ReconcileSchema { index })
            .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use storage::{FieldDef, TantivyFieldType};

    fn schema(fields: Vec<FieldDef>) -> IndexSchema {
        let mut schema = IndexSchema::default();
        for field in fields {
            schema.fields.insert(field.name.clone(), field);
        }
        schema
    }

    /// What a node learned from writes is carried into a schema that lacks it, widened where
    /// both hold it; a declared field the schema lacks was removed, and stays removed.
    #[test]
    fn only_learned_fields_are_merged_in() {
        let mut into = schema(vec![
            FieldDef::new("title".to_string(), TantivyFieldType::Text),
            FieldDef::new_learned("level".to_string(), TantivyFieldType::I64),
        ]);
        let from = schema(vec![
            FieldDef::new("removed".to_string(), TantivyFieldType::Text),
            FieldDef::new_learned("reading".to_string(), TantivyFieldType::F64),
            FieldDef::new_learned("level".to_string(), TantivyFieldType::Text),
        ]);
        assert!(merge_learned(&mut into, &from));
        assert!(!into.fields.contains_key("removed"));
        assert_eq!(
            into.fields["reading"].field_type,
            TantivyFieldType::F64,
            "a learned field it lacked"
        );
        assert_eq!(into.fields["level"].field_type, TantivyFieldType::Text);
        assert!(!merge_learned(&mut into, &from), "nothing more to bring");
    }

    /// Marking a learned field indexed pins it: it is declared from then on, so switching it off
    /// again does not let it widen over the column built for it.
    #[test]
    fn promoting_a_learned_field_makes_it_declared() {
        let held = schema(vec![FieldDef::new_learned(
            "level".to_string(),
            TantivyFieldType::I64,
        )]);
        let edit = FieldEdit {
            field_updates: BTreeMap::from([("level".to_string(), true)]),
            default_fields: None,
        };
        let Ok(Edited::Proposal(next, outcome)) = edit.apply(&held) else {
            panic!("a known field is edited");
        };
        assert_eq!(outcome.applied, vec!["level".to_string()]);
        assert!(next.fields["level"].indexed);
        assert!(!next.fields["level"].learned);
    }

    /// A sweep judges this node's own records: a live schema at or below a peer's drop is a
    /// drop it missed, a live schema differing from a peer's is reconciled, and a drop here or
    /// an index only a peer holds is left to that peer's own sweep and to the next lookup.
    #[test]
    fn a_sweep_finishes_missed_drops_and_reconciles_differences() {
        let record = |index: &str, version, dropped, thumbprint| SchemaRecord {
            index: index.to_string(),
            version,
            dropped,
            thumbprint,
        };
        let local = vec![
            record("dropped_meanwhile", 2, false, 7),
            record("agreed", 5, false, 1),
            record("differs", 5, false, 1),
            record("minted_above", 6, false, 9),
            record("dropped_here", 4, true, 0),
        ];
        let peers = vec![
            record("dropped_meanwhile", 3, true, 0),
            record("dropped_meanwhile", 3, true, 0),
            record("agreed", 5, false, 1),
            record("differs", 5, false, 2),
            record("minted_above", 5, true, 0),
            record("dropped_here", 3, false, 1),
            record("only_there", 1, false, 1),
        ];
        assert_eq!(
            sweep_verdict(&local, &peers),
            SweepVerdict {
                finish: vec![("dropped_meanwhile".to_string(), 3)],
                reconcile: vec!["differs".to_string()],
            }
        );
    }
}
