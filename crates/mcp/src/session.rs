//! The session registry, and the key each session is bound to.
//!
//! Scoped to the `initialize` era: a session id is minted on `initialize` and travels in a
//! header afterwards. Everything the registry knows is reachable only through the methods
//! below, so the lock stays inside this module.

use std::{collections::HashMap, time::Duration};

use axum::response::sse::Event;
use tokio::sync::{Mutex, mpsc};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};
use uuid::Uuid;

use std::sync::Arc;

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
    pub(crate) async fn create_session(&self, key_id: Option<String>) -> String {
        let session_id = Uuid::new_v4().to_string();
        let mut inner = self.inner.lock().await;
        inner.sessions.insert(
            session_id.clone(),
            McpSession {
                sender: None,
                last_activity: std::time::Instant::now(),
                key_id,
            },
        );
        session_id
    }

    /// Create a legacy SSE session with a push channel, returning its id, a sender the caller
    /// can emit the endpoint event on, and the receiver that becomes the stream.
    pub(crate) async fn create_sse_session(
        &self,
        key_id: Option<String>,
    ) -> (
        String,
        mpsc::UnboundedSender<Event>,
        mpsc::UnboundedReceiver<Event>,
    ) {
        let mut inner = self.inner.lock().await;
        inner.next_session_id += 1;
        let session_id = format!("mcp-session-{}", inner.next_session_id);
        let (tx, rx) = mpsc::unbounded_channel();

        // Legacy SSE session ids are sequential, so the next one is guessable. Binding the
        // session to its creator is what stops that from being useful.
        let session = McpSession {
            sender: Some(tx.clone()),
            last_activity: std::time::Instant::now(),
            key_id,
        };

        inner.sessions.insert(session_id.clone(), session);
        (session_id, tx, rx)
    }

    /// The session under `session_id`, if there is one.
    pub(crate) async fn session_of(&self, session_id: &str) -> Option<McpSession> {
        let inner = self.inner.lock().await;
        inner.sessions.get(session_id).cloned()
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

    /// Check that `key_id` may act on `session_id`, refreshing its activity clock if so.
    pub(crate) async fn claim_session(
        &self,
        session_id: &str,
        key_id: Option<&str>,
    ) -> SessionAccess {
        let mut inner = self.inner.lock().await;
        match inner.sessions.get_mut(session_id) {
            None => SessionAccess::Unknown,
            Some(session) if session.owned_by(key_id) => {
                session.last_activity = std::time::Instant::now();
                SessionAccess::Granted
            }
            Some(_) => SessionAccess::WrongKey,
        }
    }
}

/// Sweep inactive sessions until the state is cancelled.
pub(crate) fn spawn_cleanup_task(state: McpTransportState) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        loop {
            tokio::select! {
                _ = state.cancel.cancelled() => {
                    debug!("MCP cleanup task: shutdown signal received");
                    break;
                }
                _ = interval.tick() => {
                    let mut inner = state.inner.lock().await;
                    let now = std::time::Instant::now();
                    let timeout = Duration::from_secs(300); // 5 minutes timeout

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
                        let is_active = now.duration_since(session.last_activity) < timeout;
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
    next_session_id: u64,
    sessions: HashMap<String, McpSession>,
}

#[derive(Clone)]
pub(crate) struct McpSession {
    /// SSE push channel. `Some` for legacy SSE sessions (server pushes responses
    /// over the stream); `None` for Streamable HTTP sessions where responses are
    /// returned inline on the POST request.
    pub(crate) sender: Option<mpsc::UnboundedSender<Event>>,
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
        let session = state.create_session(Some("aabbccdd".to_string())).await;

        assert_eq!(
            state.claim_session(&session, Some("aabbccdd")).await,
            SessionAccess::Granted
        );
        assert_eq!(
            state.claim_session(&session, Some("11223344")).await,
            SessionAccess::WrongKey
        );
        // No key at all is not a way around the binding.
        assert_eq!(
            state.claim_session(&session, None).await,
            SessionAccess::WrongKey
        );
        assert_eq!(
            state.claim_session("never-existed", Some("aabbccdd")).await,
            SessionAccess::Unknown
        );
    }

    #[tokio::test]
    async fn another_key_cannot_end_someone_elses_session() {
        let state = McpTransportState::default();
        let session = state.create_session(Some("aabbccdd".to_string())).await;

        assert_eq!(
            state.remove_session(&session, Some("11223344")).await,
            SessionAccess::WrongKey
        );
        // Still there: a refused delete must not have deleted anything.
        assert_eq!(
            state.claim_session(&session, Some("aabbccdd")).await,
            SessionAccess::Granted
        );
        assert_eq!(
            state.remove_session(&session, Some("aabbccdd")).await,
            SessionAccess::Granted
        );
        assert_eq!(
            state.claim_session(&session, Some("aabbccdd")).await,
            SessionAccess::Unknown
        );
    }

    #[tokio::test]
    async fn a_session_created_without_identity_is_bound_to_nobody() {
        // Auth off: there is no key to bind to, and binding to "no key" would lock out the
        // caller that created the session.
        let state = McpTransportState::default();
        let session = state.create_session(None).await;
        assert_eq!(
            state.claim_session(&session, None).await,
            SessionAccess::Granted
        );
        assert_eq!(
            state.claim_session(&session, Some("aabbccdd")).await,
            SessionAccess::Granted
        );
    }
}
