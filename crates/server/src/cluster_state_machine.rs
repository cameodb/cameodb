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
    /// Check if the cluster can accept write operations
    #[allow(dead_code)] // Reserved for future operation validation and health checks
    pub fn can_accept_writes(&self) -> bool {
        matches!(
            self,
            ClusterState::Active { .. } | ClusterState::Degraded { .. }
        )
    }

    /// Check if the cluster is healthy (Active state)
    pub fn is_healthy(&self) -> bool {
        matches!(self, ClusterState::Active { .. })
    }

    /// Check if the cluster has failed
    #[allow(dead_code)] // Reserved for future health check endpoints
    pub fn is_failed(&self) -> bool {
        matches!(self, ClusterState::Failed { .. })
    }
}

impl Default for ClusterState {
    fn default() -> Self {
        Self::Failed {
            reason: "Unknown".to_string(),
        }
    }
}
