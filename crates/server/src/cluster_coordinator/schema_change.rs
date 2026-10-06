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
//! Run from the HTTP task, not the coordinator's mailbox, for the reason given on
//! [`super::delete_index_cluster`]: the coordinator hands over who to reach and this reaches them.

use serde_json::Value as JsonValue;
use tracing::{info, warn};
use uuid::Uuid;

use super::DeleteTargets;
use crate::distributed::ClusterStatus;
use crate::node::{ClientOp, NodeOrchestrator, OrchestratorError, SchemaApplied, SchemaReadiness};
use storage::IndexSchema;

/// How many times a change that finds its index held by another change asks again.
const BUSY_ATTEMPTS: u32 = 5;
/// The least a change waits before asking again, and the spread added at random: changes that
/// stepped back together ask again apart.
const BUSY_BACKOFF_MS: u64 = 50;
const BUSY_JITTER_MS: u64 = 200;

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
                    use crate::remote_peer_pool::ConnectionChannel;
                    let remote = pool
                        .get_orchestrator(id, ConnectionChannel::Operations)
                        .await
                        .map_err(|e| OrchestratorError::PeerUnreachable {
                            message: format!("node {id} lookup failed: {e}"),
                        })?
                        .ok_or_else(|| OrchestratorError::PeerUnreachable {
                            message: format!("node {id} has no reachable orchestrator"),
                        })?;
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
    mut schema: IndexSchema,
) -> Result<JsonValue, OrchestratorError> {
    let Some(local) = targets.local_orchestrator.clone() else {
        return Err(OrchestratorError::NotReady(
            "Local orchestrator not available".to_string(),
        ));
    };

    // Every configured member, connected, or nothing is changed: a node not asked keeps the old
    // schema, and the cluster is split until someone notices. Counted against the configured
    // membership, not the peers this node happens to know, as the schema canvass does.
    if let Some(status) = status.as_ref().filter(|status| status.cluster_enabled) {
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
    let reach = Reach {
        nodes: std::iter::once(Node::Local(local))
            .chain(targets.peers.iter().map(|peer| Node::Peer(peer.node_id)))
            .collect(),
        pool: targets.pool.clone(),
    };

    // Phase one: what every node holds, and what the change would ask of it. Each node
    // reserves the index for this change until it is applied or released.
    let change = Uuid::new_v4();
    let tenant = schema.tenant.clone();
    let prepare = ClientOp::PrepareSchema {
        index: index.clone(),
        schema: schema.clone(),
        tenant: tenant.clone(),
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
        if !readiness.iter().any(|(_, ready)| ready.busy) {
            break readiness;
        }
        // Another change holds the index on some node. Two changes sent at once each reach
        // their own node first and find the other on the rest, so both step back here; each
        // waits a different while and asks again, and one goes first. The second then applies
        // on top of it, at the next version, and both callers are told the truth.
        reach.release(&index, change).await;
        if attempt >= BUSY_ATTEMPTS {
            return Ok(refused(
                &index,
                0,
                &[],
                format!(
                    "another change to index '{index}' is being applied right now; nothing \
                     was changed. Read the schema again (GET /api/{index}/_config) and send \
                     this change again if it is still wanted"
                ),
            ));
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
    let held = readiness
        .iter()
        .filter_map(|(_, ready)| ready.current.clone())
        .filter(|current| current.state != storage::SchemaState::Dropped)
        .reduce(NodeOrchestrator::preferred_schema);
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
        return Ok(refused(
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
        ));
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
        return Ok(refused(
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
        ));
    }

    // Another change to the same index was applied at the same time, and the cluster prefers
    // it. Every node ends on that one — the rule is the same everywhere, whichever change
    // reached a node first — so this one is reported as not taken, rather than half taken.
    if !superseded.is_empty() {
        return Ok(refused(
            &index,
            0,
            &[],
            format!(
                "another change to index '{index}' was applied at the same time and takes \
                 precedence ({} kept it). Read the schema again (GET /api/{index}/_config) and \
                 send the change again if it is still wanted",
                superseded.join(", ")
            ),
        ));
    }

    if !unconfirmed.is_empty() {
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

    Ok(serde_json::json!({
        "acknowledged": true,
        "index": index,
        "version": schema.version,
        "nodes": applied.len(),
        "field_names": field_names,
    }))
}
