//! Cluster State Machine
//!
//! Defines the state machine for cluster health and readiness tracking.

use serde::{Deserialize, Serialize};

/// Simplified cluster state - derived from active node count
/// State changes only via message events (PeerDiscovered/PeerLost)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ClusterState {
    /// Cluster operational (all expected nodes active)
    Active {
        generation: u64,
        active_nodes: usize,
        total_expected: usize,
    },

    /// One node inactive (Yellow state)
    Degraded {
        active_nodes: usize,
        inactive_nodes: usize,
    },

    /// More than one node inactive (Red state)
    Failed { reason: String },
}

impl ClusterState {
    /// Check if the cluster is healthy (Active state)
    pub fn is_healthy(&self) -> bool {
        matches!(self, ClusterState::Active { .. })
    }
}

impl Default for ClusterState {
    fn default() -> Self {
        Self::Failed {
            reason: "Unknown".to_string(),
        }
    }
}
