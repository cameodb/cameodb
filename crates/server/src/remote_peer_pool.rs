//! # RemotePeerPool - Cached Remote Actor Reference Pool
//!
//! Eliminates repeated `RemoteActorRef::lookup()` calls by caching resolved
//! references per node. Lookups go through the Kameo swarm registry/DHT which
//! is the expensive part; the resulting `RemoteActorRef` is lightweight
//! (ActorId + mpsc sender clone) and safe to reuse.
//!
//! ## Invalidation
//!
//! Cached refs are evicted when a peer disconnects (`invalidate_peer`) or on
//! full topology changes (`invalidate_all`). On cache miss the pool falls back
//! to a fresh `RemoteActorRef::lookup()`.
//!
//! ## Replication Readiness
//!
//! The pool separates refs by `ConnectionChannel` so that future replication
//! traffic can use dedicated actor refs with different timeout/priority
//! semantics without restructuring the pool.

use kameo::actor::RemoteActorRef;
use std::collections::HashMap;
use std::sync::RwLock;
use std::time::Instant;
use tracing::debug;
use uuid::Uuid;

use crate::cluster_coordinator::ClusterCoordinator;
use crate::node_orchestrator::{NodeOrchestrator, orchestrator_remote_name};

// ============================================================================
// Connection Channel (replication-ready)
// ============================================================================

/// Logical channel separation for future replication support.
/// Currently only `Operations` is used. When replication is introduced,
/// dedicated refs with different timeout/retry semantics can be cached
/// under the `Replication` channel without changing the pool API.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConnectionChannel {
    /// Standard operations: reads, writes, searches, broadcasts.
    Operations,
    /// Future: dedicated replication stream with different priorities.
    #[allow(dead_code)]
    Replication,
}

// ============================================================================
// Cached Entry
// ============================================================================

#[derive(Clone)]
struct CachedOrchestratorRef {
    remote_ref: RemoteActorRef<NodeOrchestrator>,
    #[allow(dead_code)] // Reserved for TTL-based expiry
    cached_at: Instant,
}

#[derive(Clone)]
struct CachedCoordinatorRef {
    remote_ref: RemoteActorRef<ClusterCoordinator>,
    #[allow(dead_code)] // Reserved for TTL-based expiry
    cached_at: Instant,
}

// ============================================================================
// RemotePeerPool
// ============================================================================

/// Thread-safe pool of cached `RemoteActorRef` handles keyed by node UUID.
///
/// Uses `RwLock<HashMap>` for minimal overhead — writes (invalidation on peer
/// disconnect) are rare; reads (cache hits on every remote operation) are the
/// common case.
pub struct RemotePeerPool {
    orchestrator_refs: RwLock<HashMap<(Uuid, ConnectionChannel), CachedOrchestratorRef>>,
    coordinator_refs: RwLock<HashMap<Uuid, CachedCoordinatorRef>>,
}

impl RemotePeerPool {
    /// Create an empty pool.
    pub fn new() -> Self {
        Self {
            orchestrator_refs: RwLock::new(HashMap::new()),
            coordinator_refs: RwLock::new(HashMap::new()),
        }
    }

    // ========================================================================
    // NodeOrchestrator refs
    // ========================================================================

    /// Get a cached `RemoteActorRef<NodeOrchestrator>` or perform a fresh lookup.
    ///
    /// Returns `None` if the remote actor is not found (peer not registered).
    pub async fn get_orchestrator(
        &self,
        node_id: Uuid,
        channel: ConnectionChannel,
    ) -> Result<Option<RemoteActorRef<NodeOrchestrator>>, RemotePeerPoolError> {
        // Fast path: check cache under read lock
        if let Some(cached) = self.get_cached_orchestrator(node_id, channel) {
            return Ok(Some(cached));
        }

        // Slow path: lookup and cache
        let name = orchestrator_remote_name(&node_id);
        debug!(node_id = %node_id, name = %name, "RemotePeerPool: cache miss, performing lookup");

        let remote_ref = RemoteActorRef::<NodeOrchestrator>::lookup(name)
            .await
            .map_err(|e| RemotePeerPoolError::LookupFailed(e.to_string()))?;

        if let Some(ref r) = remote_ref {
            self.cache_orchestrator(node_id, channel, r.clone());
        }

        Ok(remote_ref)
    }

    /// Get a cached `RemoteActorRef<NodeOrchestrator>`, performing a fresh
    /// lookup on miss. Returns an error if the actor is not found.
    #[allow(dead_code)] // Public API for callers that require a ref
    pub async fn get_orchestrator_required(
        &self,
        node_id: Uuid,
        channel: ConnectionChannel,
    ) -> Result<RemoteActorRef<NodeOrchestrator>, RemotePeerPoolError> {
        self.get_orchestrator(node_id, channel)
            .await?
            .ok_or_else(|| {
                RemotePeerPoolError::NotFound(format!(
                    "remote orchestrator for node {} not found",
                    node_id
                ))
            })
    }

    fn get_cached_orchestrator(
        &self,
        node_id: Uuid,
        channel: ConnectionChannel,
    ) -> Option<RemoteActorRef<NodeOrchestrator>> {
        let map = self.orchestrator_refs.read().ok()?;
        map.get(&(node_id, channel))
            .map(|entry| entry.remote_ref.clone())
    }

    fn cache_orchestrator(
        &self,
        node_id: Uuid,
        channel: ConnectionChannel,
        remote_ref: RemoteActorRef<NodeOrchestrator>,
    ) {
        if let Ok(mut map) = self.orchestrator_refs.write() {
            map.insert(
                (node_id, channel),
                CachedOrchestratorRef {
                    remote_ref,
                    cached_at: Instant::now(),
                },
            );
        }
    }

    // ========================================================================
    // ClusterCoordinator refs
    // ========================================================================

    /// Get a cached `RemoteActorRef<ClusterCoordinator>` or perform a fresh lookup.
    pub async fn get_coordinator(
        &self,
        node_id: Uuid,
    ) -> Result<Option<RemoteActorRef<ClusterCoordinator>>, RemotePeerPoolError> {
        // Fast path: check cache under read lock
        if let Some(cached) = self.get_cached_coordinator(node_id) {
            return Ok(Some(cached));
        }

        // Slow path: lookup and cache
        let name = format!("coordinator-{}", node_id);
        debug!(node_id = %node_id, name = %name, "RemotePeerPool: coordinator cache miss, performing lookup");

        let remote_ref = RemoteActorRef::<ClusterCoordinator>::lookup(name)
            .await
            .map_err(|e| RemotePeerPoolError::LookupFailed(e.to_string()))?;

        if let Some(ref r) = remote_ref {
            self.cache_coordinator(node_id, r.clone());
        }

        Ok(remote_ref)
    }

    /// Get a cached `RemoteActorRef<ClusterCoordinator>`, performing a fresh
    /// lookup on miss. Returns an error if the actor is not found.
    #[allow(dead_code)] // Public API for callers that require a ref
    pub async fn get_coordinator_required(
        &self,
        node_id: Uuid,
    ) -> Result<RemoteActorRef<ClusterCoordinator>, RemotePeerPoolError> {
        self.get_coordinator(node_id).await?.ok_or_else(|| {
            RemotePeerPoolError::NotFound(format!(
                "remote coordinator for node {} not found",
                node_id
            ))
        })
    }

    fn get_cached_coordinator(&self, node_id: Uuid) -> Option<RemoteActorRef<ClusterCoordinator>> {
        let map = self.coordinator_refs.read().ok()?;
        map.get(&node_id).map(|entry| entry.remote_ref.clone())
    }

    fn cache_coordinator(&self, node_id: Uuid, remote_ref: RemoteActorRef<ClusterCoordinator>) {
        if let Ok(mut map) = self.coordinator_refs.write() {
            map.insert(
                node_id,
                CachedCoordinatorRef {
                    remote_ref,
                    cached_at: Instant::now(),
                },
            );
        }
    }

    // ========================================================================
    // Invalidation
    // ========================================================================

    /// Evict all cached refs for a specific peer (called on peer disconnect).
    pub fn invalidate_peer(&self, node_id: Uuid) {
        let mut orch_evicted = 0;
        if let Ok(mut map) = self.orchestrator_refs.write() {
            let before = map.len();
            map.retain(|&(nid, _), _| nid != node_id);
            orch_evicted = before - map.len();
        }
        let mut coord_evicted = 0;
        if let Ok(mut map) = self.coordinator_refs.write()
            && map.remove(&node_id).is_some()
        {
            coord_evicted = 1;
        }
        if orch_evicted > 0 || coord_evicted > 0 {
            debug!(
                node_id = %node_id,
                orchestrator_evicted = orch_evicted,
                coordinator_evicted = coord_evicted,
                "RemotePeerPool: invalidated peer"
            );
        }
    }

    /// Evict all cached refs (called on major topology changes).
    #[allow(dead_code)] // Public API for full topology resets
    pub fn invalidate_all(&self) {
        let mut total = 0;
        if let Ok(mut map) = self.orchestrator_refs.write() {
            total += map.len();
            map.clear();
        }
        if let Ok(mut map) = self.coordinator_refs.write() {
            total += map.len();
            map.clear();
        }
        if total > 0 {
            debug!(
                evicted = total,
                "RemotePeerPool: invalidated all cached refs"
            );
        }
    }

    /// Return the number of currently cached refs (for diagnostics).
    pub fn cached_count(&self) -> usize {
        let orch = self.orchestrator_refs.read().map(|m| m.len()).unwrap_or(0);
        let coord = self.coordinator_refs.read().map(|m| m.len()).unwrap_or(0);
        orch + coord
    }
}

impl std::fmt::Debug for RemotePeerPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemotePeerPool")
            .field("cached_count", &self.cached_count())
            .finish()
    }
}

// ============================================================================
// Error type
// ============================================================================

#[derive(Debug, thiserror::Error)]
pub enum RemotePeerPoolError {
    #[error("remote actor lookup failed: {0}")]
    LookupFailed(String),
    #[error("remote actor not found: {0}")]
    #[allow(dead_code)] // Used by get_*_required methods
    NotFound(String),
}
