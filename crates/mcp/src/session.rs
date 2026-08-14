//! The session registry, and the key each session is bound to.
//!
//! Scoped to the `initialize` era: a session id is minted on `initialize` and travels in a
//! header afterwards. Everything the registry knows is reachable only through the methods
//! below, so the lock stays inside this module.

use std::{collections::HashMap, sync::Arc, time::Duration};

use axum::response::sse::Event;
use tokio::sync::{Mutex, mpsc};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};
use uuid::Uuid;

/// How long a session may sit idle before the sweeper removes it.
const SESSION_IDLE_TIMEOUT: Duration = Duration::from_secs(300);

/// How often the sweeper looks for idle sessions.
const SWEEP_INTERVAL: Duration = Duration::from_secs(30);

/// The most sessions the registry will hold.
///
/// `initialize` is reachable before any rate limit and creates a session every time, so without
/// a cap the registry's size is chosen by whoever sends requests fastest. At the cap the
/// longest-idle session is evicted rather than the new one refused: refusing `initialize` hands
/// the flood a way to lock everyone else out, while evicting costs the idlest caller one
/// re-initialize.
const MAX_SESSIONS: usize = 1024;

#[derive(Clone)]
pub(crate) struct McpTransportState {
    inner: Arc<Mutex<McpTransportInner>>,
    cancel: CancellationToken,
}

impl Default for McpTransportState {
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(McpTransportInner::default())),
            cancel: CancellationToken::new(),
        }
    }
}

impl McpTransportState {
    /// Gracefully shut down all MCP sessions.
    /// Drops every sender so SSE streams terminate, then clears the session map.
    async fn shutdown(&self) {
        self.cancel.cancel();
        let mut inner = self.inner.lock().await;
        let count = inner.sessions.len();
        inner.sessions.clear();
        if count > 0 {
            info!(
                sessions = count,
                "MCP transport: all sessions closed on shutdown"
            );
        }
    }

    /// Create a new Streamable HTTP session (no SSE push channel) and return its id.
    /// The id is a cryptographically random UUID per the MCP spec recommendation.
    ///
    /// `protocol_version` is what `initialize` negotiated. It is remembered here because it is
    /// a property of the session, not of each request: a client that omits the version header
    /// on later requests is still owed answers in the shape its revision defines.
    pub(crate) async fn create_session(
        &self,
        key_id: Option<String>,
        protocol_version: Option<String>,
    ) -> String {
        let session_id = Uuid::new_v4().to_string();
        let mut inner = self.inner.lock().await;
        inner.evict_idlest_if_full();
        inner.sessions.insert(
            session_id.clone(),
            McpSession {
                sender: None,
                last_activity: std::time::Instant::now(),
                key_id,
                protocol_version,
            },
        );
        session_id
    }

    /// Create a legacy SSE session with a push channel, returning its id, a sender the caller
    /// can emit the endpoint event on, and the receiver that becomes the stream.
    ///
    /// The id is a random UUID for the same reason the Streamable HTTP one is: a session
    /// created with authorization off is bound to nobody, so a guessable id would be enough to
    /// post into someone else's conversation.
    pub(crate) async fn create_sse_session(
        &self,
        key_id: Option<String>,
    ) -> (
        String,
        mpsc::UnboundedSender<Event>,
        mpsc::UnboundedReceiver<Event>,
    ) {
        let session_id = Uuid::new_v4().to_string();
        let (tx, rx) = mpsc::unbounded_channel();

        let session = McpSession {
            sender: Some(tx.clone()),
            last_activity: std::time::Instant::now(),
            key_id,
            // The legacy HTTP+SSE transport is the 2024-11-05 revision by definition.
            protocol_version: Some("2024-11-05".to_string()),
        };

        let mut inner = self.inner.lock().await;
        inner.evict_idlest_if_full();
        inner.sessions.insert(session_id.clone(), session);
        (session_id, tx, rx)
    }

    /// Forget a session without asking who is asking. For the SSE stream's drop guard, where
    /// the connection closing *is* the authority.
    pub(crate) async fn forget_session(&self, session_id: &str) -> bool {
        let mut inner = self.inner.lock().await;
        inner.sessions.remove(session_id).is_some()
    }

    /// Remove a session by id, if `key_id` is the key that created it.
    pub(crate) async fn remove_session(
        &self,
        session_id: &str,
        key_id: Option<&str>,
    ) -> SessionAccess {
        let mut inner = self.inner.lock().await;
        match inner.sessions.get(session_id) {
            None => SessionAccess::Unknown,
            Some(session) if session.owned_by(key_id) => {
                inner.sessions.remove(session_id);
                SessionAccess::Granted
            }
            Some(_) => SessionAccess::WrongKey,
        }
    }

    /// Check that `key_id` may act on `session_id`, refreshing its activity clock and returning
    /// the session if so.
    ///
    /// Returns the session on a grant rather than leaving the caller to fetch it, so checking
    /// and reading are one lock acquisition and there is no window between them.
    pub(crate) async fn claim_session(
        &self,
        session_id: &str,
        key_id: Option<&str>,
    ) -> SessionClaim {
        let mut inner = self.inner.lock().await;
        match inner.sessions.get_mut(session_id) {
            None => SessionClaim::Unknown,
            Some(session) if session.owned_by(key_id) => {
                session.last_activity = std::time::Instant::now();
                SessionClaim::Granted(session.clone())
            }
            Some(_) => SessionClaim::WrongKey,
        }
    }
}

/// Sweep inactive sessions until the state is cancelled.
pub(crate) fn spawn_cleanup_task(state: McpTransportState) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(SWEEP_INTERVAL);
        loop {
            tokio::select! {
                _ = state.cancel.cancelled() => {
                    debug!("MCP cleanup task: shutdown signal received");
                    break;
                }
                _ = interval.tick() => {
                    let mut inner = state.inner.lock().await;
                    let now = std::time::Instant::now();

                    // Remove sessions: only clean up if SSE connection is closed AND inactive
                    inner.sessions.retain(|session_id, session| {
                        // Legacy SSE sessions with a live push channel are kept regardless
                        // of last POST activity. Streamable HTTP sessions (sender = None)
                        // and disconnected SSE sessions fall through to the inactivity check.
                        if let Some(sender) = &session.sender
                            && !sender.is_closed()
                        {
                            return true;
                        }
                        let is_active = now.duration_since(session.last_activity) < SESSION_IDLE_TIMEOUT;
                        if !is_active {
                            info!(session_id = %session_id, "Cleaning up inactive MCP session");
                        }
                        is_active
                    });
                }
            }
        }
        debug!("MCP cleanup task: exited");
    });
}

/// The outcome of presenting a session id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SessionAccess {
    Granted,
    /// No such session. Not an authorization failure — a session may simply have expired,
    /// and each transport already has its own answer for that.
    Unknown,
    /// The session exists and belongs to a different key.
    WrongKey,
}

/// The outcome of claiming a session, carrying the session on a grant.
pub(crate) enum SessionClaim {
    Granted(McpSession),
    /// No such session — expired, evicted, or never created. Per the Streamable HTTP spec the
    /// transport answers 404 so the client starts a new session with `initialize`.
    Unknown,
    /// The session exists and belongs to a different key.
    WrongKey,
}

/// Opaque handle returned by [`crate::mcp_router`] to trigger graceful MCP shutdown.
#[derive(Clone)]
pub struct McpShutdownHandle {
    state: McpTransportState,
}

impl McpShutdownHandle {
    pub(crate) fn new(state: McpTransportState) -> Self {
        Self { state }
    }

    /// Gracefully shut down the MCP transport.
    /// Cancels the cleanup task and drops all active SSE session senders.
    pub async fn shutdown(&self) {
        info!("MCP shutdown: draining sessions");
        self.state.shutdown().await;
    }
}

#[derive(Default)]
struct McpTransportInner {
    sessions: HashMap<String, McpSession>,
}

impl McpTransportInner {
    /// Make room for one more session by evicting the longest-idle one at the cap.
    ///
    /// Evicting a legacy SSE session drops the registry's copy of its sender, which ends its
    /// stream and lets the drop guard clean up — so an evicted session is gone the same way an
    /// expired one is.
    fn evict_idlest_if_full(&mut self) {
        if self.sessions.len() < MAX_SESSIONS {
            return;
        }
        if let Some(idlest) = self
            .sessions
            .iter()
            .min_by_key(|(_, session)| session.last_activity)
            .map(|(session_id, _)| session_id.clone())
        {
            info!(session_id = %idlest, "MCP session evicted: registry at capacity");
            self.sessions.remove(&idlest);
        }
    }
}

#[derive(Clone)]
pub(crate) struct McpSession {
    /// SSE push channel. `Some` for legacy SSE sessions (server pushes responses
    /// over the stream); `None` for Streamable HTTP sessions where responses are
    /// returned inline on the POST request.
    pub(crate) sender: Option<mpsc::UnboundedSender<Event>>,
    /// The protocol version `initialize` negotiated for this session, if one was recorded.
    ///
    /// What a later request falls back to when it carries no `MCP-Protocol-Version` header:
    /// the shape of a tool result follows the revision the client speaks, and the session is
    /// where that revision was agreed.
    pub(crate) protocol_version: Option<String>,
    last_activity: std::time::Instant,
    /// The key that created this session, if the host identified one.
    ///
    /// A session id travels in a header and names a conversation the server keeps state
    /// for. Without this, learning someone else's session id would be enough to continue
    /// their conversation.
    key_id: Option<String>,
}

impl McpSession {
    /// A session created by an identified caller may only be continued by that same caller.
    /// One created without identity (auth off) is not bound to anyone.
    fn owned_by(&self, key_id: Option<&str>) -> bool {
        match &self.key_id {
            None => true,
            Some(owner) => key_id == Some(owner.as_str()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_session_belongs_to_the_key_that_created_it() {
        let state = McpTransportState::default();
        let session = state
            .create_session(Some("aabbccdd".to_string()), None)
            .await;

        assert!(matches!(
            state.claim_session(&session, Some("aabbccdd")).await,
            SessionClaim::Granted(_)
        ));
        assert!(matches!(
            state.claim_session(&session, Some("11223344")).await,
            SessionClaim::WrongKey
        ));
        // No key at all is not a way around the binding.
        assert!(matches!(
            state.claim_session(&session, None).await,
            SessionClaim::WrongKey
        ));
        assert!(matches!(
            state.claim_session("never-existed", Some("aabbccdd")).await,
            SessionClaim::Unknown
        ));
    }

    #[tokio::test]
    async fn another_key_cannot_end_someone_elses_session() {
        let state = McpTransportState::default();
        let session = state
            .create_session(Some("aabbccdd".to_string()), None)
            .await;

        assert_eq!(
            state.remove_session(&session, Some("11223344")).await,
            SessionAccess::WrongKey
        );
        // Still there: a refused delete must not have deleted anything.
        assert!(matches!(
            state.claim_session(&session, Some("aabbccdd")).await,
            SessionClaim::Granted(_)
        ));
        assert_eq!(
            state.remove_session(&session, Some("aabbccdd")).await,
            SessionAccess::Granted
        );
        assert!(matches!(
            state.claim_session(&session, Some("aabbccdd")).await,
            SessionClaim::Unknown
        ));
    }

    #[tokio::test]
    async fn a_session_created_without_identity_is_bound_to_nobody() {
        // Auth off: there is no key to bind to, and binding to "no key" would lock out the
        // caller that created the session.
        let state = McpTransportState::default();
        let session = state.create_session(None, None).await;
        assert!(matches!(
            state.claim_session(&session, None).await,
            SessionClaim::Granted(_)
        ));
        assert!(matches!(
            state.claim_session(&session, Some("aabbccdd")).await,
            SessionClaim::Granted(_)
        ));
    }

    /// The version `initialize` negotiated comes back with the claimed session.
    ///
    /// It is what the transport falls back to when a request carries no version header: the
    /// shape of a tool result follows the client's revision, and the session is where that
    /// revision was agreed.
    #[tokio::test]
    async fn a_claimed_session_recalls_its_negotiated_version() {
        let state = McpTransportState::default();
        let session = state
            .create_session(None, Some("2025-06-18".to_string()))
            .await;
        match state.claim_session(&session, None).await {
            SessionClaim::Granted(session) => {
                assert_eq!(session.protocol_version.as_deref(), Some("2025-06-18"));
            }
            _ => panic!("the session was not granted to its creator"),
        }
    }

    /// A legacy SSE session id must not be guessable.
    ///
    /// With authorization off a session is bound to nobody, so the id itself is the only thing
    /// standing between a stranger and someone else's conversation. Sequential ids handed that
    /// stranger the next one for free.
    #[tokio::test]
    async fn a_legacy_sse_session_id_is_not_guessable() {
        let state = McpTransportState::default();
        let (first, _tx1, _rx1) = state.create_sse_session(None).await;
        let (second, _tx2, _rx2) = state.create_sse_session(None).await;
        assert!(
            Uuid::parse_str(&first).is_ok() && Uuid::parse_str(&second).is_ok(),
            "legacy SSE ids are not random UUIDs: {first}, {second}"
        );
    }

    /// The registry holds at most [`MAX_SESSIONS`], evicting the idlest rather than refusing
    /// the newest — a flood of `initialize` must not choose the registry's size, and must not
    /// lock a new caller out either.
    #[tokio::test]
    async fn a_full_registry_evicts_rather_than_grows_or_refuses() {
        let state = McpTransportState::default();
        for _ in 0..MAX_SESSIONS {
            state.create_session(None, None).await;
        }
        let newest = state.create_session(None, None).await;

        let inner = state.inner.lock().await;
        assert_eq!(
            inner.sessions.len(),
            MAX_SESSIONS,
            "the registry grew past its cap"
        );
        assert!(
            inner.sessions.contains_key(&newest),
            "the newest session was refused instead of the idlest being evicted"
        );
    }
}
