//! Unit tests for the cluster coordinator.

use super::*;

use uuid::Uuid;

use crate::distributed::{DistributedCluster, NodeInfo};

#[cfg(test)]
#[allow(clippy::module_inception)]
mod tests {
    use super::*;
    use crate::config::ClusterConfig;
    use crate::distributed::NodeStatus;

    fn make_cluster() -> DistributedCluster {
        let cfg = ClusterConfig::default();
        let path = std::env::temp_dir();
        DistributedCluster::new(
            cfg,
            Uuid::new_v4(),
            "TST".to_string(),
            path,
            64 * 1024 * 1024,
            60,
        )
    }

    #[test]
    fn decide_route_defaults_to_local_when_no_key() {
        let cc = ClusterCoordinator::new(make_cluster());
        let decision = cc.decide_route(None, OperationType::Read);
        assert!(matches!(decision, RoutingDecision::Local));
    }

    /// Every routing decision on a lone node is `Local`, whatever it is asked.
    ///
    /// This is what lets `RouterActor::resolve_local` answer for a *keyless* operation without a
    /// mailbox round trip when `[network.cluster] enabled` is off. Measured on a release build
    /// against a 2,000-document index, that ask was happening once per ordinary search, once per
    /// streaming search and once per `GET /_indexes` — for an answer that could not have been
    /// anything else. Keyed writes already resolved locally from the published ring and
    /// placement; the keyless reads were the gap, and they are the common case.
    ///
    /// Asserted here rather than in the router, because the claim is *this function's*: if it
    /// ever gains an answer other than `Local` for a single-node cluster, the shortcut becomes a
    /// lie and this test is what says so.
    #[test]
    fn a_lone_node_can_only_ever_be_routed_to_locally() {
        // No shards registered, so the ring is empty — the state a node is in before its first
        // write, and the one the shortcut has to be right about too.
        let empty = ClusterCoordinator::new(make_cluster());
        // And with a shard of its own, which is every later operation.
        let cluster = make_cluster();
        let local = cluster.local_node_id;
        let mut owning = ClusterCoordinator::new(cluster);
        let shard_id = Uuid::new_v4();
        owning.shard_assignments.insert(
            shard_id,
            ShardMetadata {
                shard_id,
                node_id: local,
                vnode_tokens: vec![1, 2, 3],
                storage_bytes: 0,
                document_count: 0,
            },
        );
        owning.rebuild_ring();

        for cc in [&empty, &owning] {
            for key in [None, Some("any-key".to_string())] {
                for op in [OperationType::Read, OperationType::Write] {
                    let decision = cc.decide_route(key.clone(), op.clone());
                    assert!(
                        matches!(decision, RoutingDecision::Local),
                        "a node that is the whole cluster has nowhere else to send \
                         {key:?}/{op:?}, got {decision:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn decide_route_returns_local_when_owner_is_self() {
        let cluster = make_cluster();
        let local = cluster.local_node_id;
        let mut cc = ClusterCoordinator::new(cluster);

        let shard_id = Uuid::new_v4();
        cc.shard_assignments.insert(
            shard_id,
            ShardMetadata {
                shard_id,
                node_id: local,
                vnode_tokens: vec![1, 2, 3],
                storage_bytes: 0,
                document_count: 0,
            },
        );
        cc.rebuild_ring();

        let decision = cc.decide_route(Some("key-1".into()), OperationType::Read);
        assert!(matches!(decision, RoutingDecision::Local));
    }

    #[test]
    fn decide_route_returns_remote_when_owner_known_with_addr() {
        let mut cluster = make_cluster();
        let owner = Uuid::new_v4();
        cluster.peer_nodes.insert(
            owner,
            NodeInfo {
                node_id: owner,
                node_name: None,
                address: "127.0.0.1:9000".into(),
                status: NodeStatus::Connected,
                shard_count: 0,
            },
        );
        let mut cc = ClusterCoordinator::new(cluster);

        let shard_id = Uuid::new_v4();
        cc.shard_assignments.insert(
            shard_id,
            ShardMetadata {
                shard_id,
                node_id: owner,
                vnode_tokens: vec![1, 2, 3],
                storage_bytes: 0,
                document_count: 0,
            },
        );
        cc.rebuild_ring();

        let decision = cc.decide_route(Some("key-remote".into()), OperationType::Write);
        match decision {
            RoutingDecision::Remote { node_id, .. } => assert_eq!(node_id, owner),
            other => panic!("expected Remote, got {:?}", other),
        }
    }

    #[test]
    fn decide_route_broadcasts_when_owner_address_missing() {
        let mut cc = ClusterCoordinator::new(make_cluster());
        let shard_id = Uuid::new_v4();
        let owner = Uuid::new_v4(); // not present in peer_nodes
        cc.shard_assignments.insert(
            shard_id,
            ShardMetadata {
                shard_id,
                node_id: owner,
                vnode_tokens: vec![1, 2, 3],
                storage_bytes: 0,
                document_count: 0,
            },
        );
        cc.rebuild_ring();

        let decision = cc.decide_route(Some("key-no-addr".into()), OperationType::Read);
        assert!(matches!(decision, RoutingDecision::Broadcast));
    }

    #[test]
    fn ring_distribution_splits_across_multiple_nodes() {
        use cluster::generate_tokens;

        let mut cluster = make_cluster();
        let n1 = Uuid::new_v4();
        let n2 = Uuid::new_v4();
        cluster.peer_nodes.insert(
            n1,
            NodeInfo {
                node_id: n1,
                node_name: None,
                address: "127.0.0.1:9101".into(),
                status: NodeStatus::Connected,
                shard_count: 0,
            },
        );
        cluster.peer_nodes.insert(
            n2,
            NodeInfo {
                node_id: n2,
                node_name: None,
                address: "127.0.0.1:9102".into(),
                status: NodeStatus::Connected,
                shard_count: 0,
            },
        );
        let mut cc = ClusterCoordinator::new(cluster);

        let s1 = Uuid::new_v4();
        let s2 = Uuid::new_v4();
        // Use realistic token distribution across the u64 hash space
        cc.shard_assignments.insert(
            s1,
            ShardMetadata {
                shard_id: s1,
                node_id: n1,
                vnode_tokens: generate_tokens(s1),
                storage_bytes: 0,
                document_count: 0,
            },
        );
        cc.shard_assignments.insert(
            s2,
            ShardMetadata {
                shard_id: s2,
                node_id: n2,
                vnode_tokens: generate_tokens(s2),
                storage_bytes: 0,
                document_count: 0,
            },
        );
        cc.rebuild_ring();

        let mut counts = std::collections::HashMap::new();
        for i in 0..200 {
            let key = format!("key-{i}");
            match cc.decide_route(Some(key), OperationType::Read) {
                RoutingDecision::Remote { node_id, .. } => {
                    *counts.entry(node_id).or_insert(0usize) += 1;
                }
                RoutingDecision::Local => {
                    *counts.entry(cc.cluster.local_node_id).or_insert(0usize) += 1;
                }
                RoutingDecision::Broadcast => {
                    *counts.entry(Uuid::nil()).or_insert(0usize) += 1;
                }
            }
        }

        assert!(counts.get(&n1).copied().unwrap_or(0) > 0);
        assert!(counts.get(&n2).copied().unwrap_or(0) > 0);
    }
}
