//! # RemotePeerPool - Cached Remote Actor Reference Pool
//!
//! Eliminates repeated `RemoteActorRef::lookup()` calls by caching resolved
//! references per node. Lookups go through the Kameo swarm registry/DHT which
//! is the expensive part; the resulting `RemoteActorRef` is lightweight
//! (ActorId + mpsc sender clone) and safe to reuse.
//!
//! ## Invalidation
//!
//! Cached refs are evicted when a peer disconnects (`invalidate_peer`), when a
//! conversation with it fails to reach it ([`RemotePeerPool::converse`]), or on
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
use std::future::Future;
use std::sync::RwLock;
use std::time::{Duration, Instant};
use tracing::debug;
use uuid::Uuid;

use crate::cluster_coordinator::ClusterCoordinator;
use crate::node::{NodeOrchestrator, OrchestratorError, orchestrator_remote_name};

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
    /// The longest one [`converse`](Self::converse) waits on a peer: the node's remote timeout.
    peer_timeout: Duration,
}

/// What [`RemotePeerPool::new`] waits on a peer, for a pool nobody configured: the default
/// request timeout, which is also what the remote timeout follows when unset.
const DEFAULT_PEER_TIMEOUT: Duration = Duration::from_secs(60);

impl RemotePeerPool {
    /// Create an empty pool.
    pub fn new() -> Self {
        Self::with_peer_timeout(DEFAULT_PEER_TIMEOUT)
    }

    /// Create an empty pool whose conversations with a peer give up after `peer_timeout`.
    pub fn with_peer_timeout(peer_timeout: Duration) -> Self {
        Self {
            orchestrator_refs: RwLock::new(HashMap::new()),
            coordinator_refs: RwLock::new(HashMap::new()),
            peer_timeout,
        }
    }

    /// Run one conversation with a peer — its lookup, its ask and any resend — under one
    /// deadline, and forget the peer's cached refs when the conversation did not reach it.
    ///
    /// The orchestrator's forwards run on its mailbox, and each bounded only the transport,
    /// step by step: a peer that stopped answering held that node's whole mailbox for the
    /// remote timeout, twice when the schema resend followed, and the registry lookup before
    /// them had no bound at all. Now the whole conversation gets the remote timeout once.
    ///
    /// A peer that answered — with a result or with its own error, which arrives as
    /// [`OrchestratorError::Remote`] — is returned as it is. Anything else means the message
    /// never got there: the cached ref may be the problem (a restarted peer answers an old ref
    /// with "actor not running" until something notices it left), so it is dropped and the next
    /// call looks the peer up afresh, and the caller hears `PeerUnreachable` — "not now", a
    /// retryable `503`, rather than a failure of this node.
    pub(crate) async fn converse<T>(
        &self,
        node_id: Uuid,
        conversation: impl Future<Output = Result<T, OrchestratorError>>,
    ) -> Result<T, OrchestratorError> {
        match tokio::time::timeout(self.peer_timeout, conversation).await {
            Ok(Ok(answer)) => Ok(answer),
            Ok(Err(answered @ OrchestratorError::Remote { .. })) => Err(answered),
            Ok(Err(never_arrived)) => {
                self.invalidate_peer(node_id);
                Err(OrchestratorError::PeerUnreachable {
                    message: format!("node {node_id} could not be reached: {never_arrived}"),
                })
            }
            Err(_) => {
                self.invalidate_peer(node_id);
                Err(OrchestratorError::PeerUnreachable {
                    message: format!(
                        "node {node_id} did not answer within {}s",
                        self.peer_timeout.as_secs()
                    ),
                })
            }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::RemoteVerdict;

    fn peer() -> Uuid {
        Uuid::from_u128(7)
    }

    /// A peer that ran the request and refused it has answered. Its verdict is the caller's
    /// answer, and nothing about the peer's reachability has been learned.
    #[tokio::test]
    async fn a_peer_s_own_error_comes_back_as_it_was() {
        let pool = RemotePeerPool::new();
        let refused = pool
            .converse(peer(), async {
                Err::<(), _>(OrchestratorError::Remote {
                    verdict: RemoteVerdict::BadRequest,
                    message: "field `n` is not an integer".to_string(),
                })
            })
            .await;
        assert!(matches!(
            refused,
            Err(OrchestratorError::Remote {
                verdict: RemoteVerdict::BadRequest,
                ..
            })
        ));
    }

    /// Anything else is the message not getting there — "not now", which the caller can retry.
    #[tokio::test]
    async fn a_message_that_never_arrived_is_peer_unreachable() {
        let pool = RemotePeerPool::new();
        let lost = pool
            .converse(peer(), async {
                Err::<(), _>(OrchestratorError::Io(std::io::Error::other(
                    "network timeout",
                )))
            })
            .await;
        match lost {
            Err(OrchestratorError::PeerUnreachable { message }) => {
                assert!(message.contains("network timeout"), "{message}");
            }
            other => panic!("expected PeerUnreachable, got {other:?}"),
        }
    }

    /// The deadline covers the whole conversation — lookup, ask and resend — once, so a peer
    /// that stops answering half-way holds the caller no longer than the pool's timeout.
    #[tokio::test]
    async fn a_conversation_that_outlives_the_deadline_is_cut_off() {
        let pool = RemotePeerPool::with_peer_timeout(Duration::from_millis(50));
        let started = Instant::now();
        let stalled = pool
            .converse(peer(), async {
                tokio::time::sleep(Duration::from_secs(30)).await;
                Ok::<(), OrchestratorError>(())
            })
            .await;
        assert!(matches!(
            stalled,
            Err(OrchestratorError::PeerUnreachable { .. })
        ));
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    #[tokio::test]
    async fn an_answer_in_time_is_returned() {
        let pool = RemotePeerPool::new();
        let answer = pool
            .converse(peer(), async { Ok::<_, OrchestratorError>(42) })
            .await;
        assert_eq!(answer.unwrap(), 42);
    }
}
