//! The session registry, and the key each session is bound to.
//!
//! Scoped to the `initialize` era: a session id is minted on `initialize` and travels in a
//! header afterwards. Everything the registry knows is reachable only through the methods
//! below, so the lock stays inside this module.

use std::{collections::HashMap, sync::Arc, time::Duration};

use axum::response::sse::Event;
use serde_json::Value as JsonValue;
use tokio::sync::{Mutex, mpsc};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};
use uuid::Uuid;

/// The most often the sweeper will run, and the least often.
///
/// The interval is derived from the idle timeout rather than configured beside it. Two
/// independent knobs admit a config where the sweep is slower than the timeout, and then the
/// sweep interval silently *becomes* the timeout — a session configured to last five minutes
/// living for an hour, with nothing to read that says why. A tenth of the timeout keeps the
/// overshoot under 10 %, and the bounds keep a very short timeout from spinning and a very long
/// one from taking hours to notice a session it could have swept.
const MIN_SWEEP_INTERVAL: Duration = Duration::from_secs(1);
const MAX_SWEEP_INTERVAL: Duration = Duration::from_secs(60);

/// How long a session may sit idle, and how many the registry will hold.
///
/// Both are deployment questions rather than protocol ones, so the host answers them and this
/// crate only carries the numbers — see [`crate::McpTransportConfig`], which is where an
/// operator's configuration arrives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SessionLimits {
    /// How long a session may sit idle before the sweeper removes it.
    pub(crate) idle_timeout: Duration,
    /// The most sessions the registry will hold.
    ///
    /// `initialize` is reachable before any rate limit and creates a session every time, so
    /// without a cap the registry's size is chosen by whoever sends requests fastest. At the
    /// cap the longest-idle session is evicted rather than the new one refused: refusing
    /// `initialize` hands the flood a way to lock everyone else out, while evicting costs the
    /// idlest caller one re-initialize.
    pub(crate) max_sessions: usize,
}

#[derive(Clone)]
pub(crate) struct McpTransportState {
    inner: Arc<Mutex<McpTransportInner>>,
    cancel: CancellationToken,
    limits: SessionLimits,
}

impl McpTransportState {
    pub(crate) fn new(limits: SessionLimits) -> Self {
        Self {
            inner: Arc::new(Mutex::new(McpTransportInner::default())),
            cancel: CancellationToken::new(),
            limits,
        }
    }

    /// How often the sweeper looks for idle sessions, derived from the idle timeout.
    fn sweep_interval(&self) -> Duration {
        (self.limits.idle_timeout / 10).clamp(MIN_SWEEP_INTERVAL, MAX_SWEEP_INTERVAL)
    }

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
    /// The negotiated protocol version is deliberately not remembered. It decided the shape of
    /// a tool result once, which is exactly the inference that broke: a revision says which
    /// spec a client speaks, not which part of a result it reads. Every response now has one
    /// shape, so nothing downstream has a version to ask about.
    pub(crate) async fn create_session(&self, key_id: Option<String>) -> String {
        let session_id = Uuid::new_v4().to_string();
        let mut inner = self.inner.lock().await;
        inner.evict_idlest_if_full(self.limits.max_sessions);
        inner
            .sessions
            .insert(session_id.clone(), McpSession::new(None, key_id));
        session_id
    }

    /// Create a legacy SSE session with a push channel, returning its id, a sender the caller
    /// can emit the endpoint event on, and the receiver that becomes the stream.
    ///
    /// The id is a random UUID for the same reason the Streamable HTTP one is: a session
    /// created with authorization off is bound to nobody, so a guessable id would be enough to
    /// post into someone else's conversation.
    ///
    /// The channel is bounded so a slow or absent reader cannot grow the server's memory without
    /// bound. Filling the buffer is treated the same as a closed receiver: the event is dropped
    /// rather than stalling the caller.
    pub(crate) async fn create_sse_session(
        &self,
        key_id: Option<String>,
    ) -> (String, mpsc::Sender<Event>, mpsc::Receiver<Event>) {
        let session_id = Uuid::new_v4().to_string();
        let (tx, rx) = mpsc::channel::<Event>(128);

        let session = McpSession::new(Some(tx.clone()), key_id);

        let mut inner = self.inner.lock().await;
        inner.evict_idlest_if_full(self.limits.max_sessions);
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

    /// Record that a listening stream is open on `session_id`, if the session still exists.
    ///
    /// A Streamable HTTP session has no push channel, so the registry could not otherwise tell
    /// a client that is holding its listening stream open from one that has gone away. Without
    /// this the sweeper reads a paused-but-connected client as idle and takes its session, and
    /// the next tool call is answered 404 while the connection the server is writing keep-alives
    /// to is still open.
    ///
    /// Counted rather than flagged: a client may reconnect its stream before the old one's drop
    /// guard has run, and a flag cleared by the outgoing stream would then leave the incoming
    /// one uncounted.
    pub(crate) async fn open_listener(&self, session_id: &str) -> bool {
        let mut inner = self.inner.lock().await;
        match inner.sessions.get_mut(session_id) {
            Some(session) => {
                session.listeners += 1;
                session.last_activity = std::time::Instant::now();
                true
            }
            None => false,
        }
    }

    /// The counterpart to [`Self::open_listener`], for the stream's drop guard.
    ///
    /// The session's activity clock is stamped on the way out, so a session whose stream has
    /// just closed gets the full idle timeout to be resumed in rather than however much of it
    /// was left when the stream opened.
    pub(crate) async fn close_listener(&self, session_id: &str) {
        let mut inner = self.inner.lock().await;
        if let Some(session) = inner.sessions.get_mut(session_id) {
            session.listeners = session.listeners.saturating_sub(1);
            session.last_activity = std::time::Instant::now();
        }
    }
}

/// Sweep inactive sessions until the state is cancelled.
pub(crate) fn spawn_cleanup_task(state: McpTransportState) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(state.sweep_interval());
        loop {
            tokio::select! {
                _ = state.cancel.cancelled() => {
                    debug!("MCP cleanup task: shutdown signal received");
                    break;
                }
                _ = interval.tick() => {
                    state.inner.lock().await.sweep(state.limits.idle_timeout);
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
    /// Drop every session that is neither connected nor recently active.
    ///
    /// A session is swept for being idle, not for being quiet: a connection the server is still
    /// holding open is proof the client is there, whichever transport it arrived on. Only
    /// sessions with no live connection reach the inactivity check.
    fn sweep(&mut self, idle_timeout: Duration) {
        let now = std::time::Instant::now();
        self.sessions.retain(|session_id, session| {
            if session.is_connected() {
                return true;
            }
            let is_active = now.duration_since(session.last_activity) < idle_timeout;
            if !is_active {
                info!(session_id = %session_id, "Cleaning up inactive MCP session");
            }
            is_active
        });
    }

    /// Make room for one more session by evicting the longest-idle one at the cap.
    ///
    /// Evicting a legacy SSE session drops the registry's copy of its sender, which ends its
    /// stream and lets the drop guard clean up — so an evicted session is gone the same way an
    /// expired one is.
    fn evict_idlest_if_full(&mut self, max_sessions: usize) {
        if self.sessions.len() < max_sessions {
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
    pub(crate) sender: Option<mpsc::Sender<Event>>,
    /// How many listening streams are open on this session.
    ///
    /// The Streamable HTTP equivalent of `sender` being live. That transport's `GET` stream
    /// carries nothing from the server but keep-alives, so there is no channel whose closure
    /// reports the disconnect — the count is maintained by the handler and its drop guard
    /// instead.
    listeners: u32,
    last_activity: std::time::Instant,
    /// The key that created this session, if the host identified one.
    ///
    /// A session id travels in a header and names a conversation the server keeps state
    /// for. Without this, learning someone else's session id would be enough to continue
    /// their conversation.
    key_id: Option<String>,
    /// In-flight requests on this session, keyed by JSON-RPC request id. Only legacy SSE
    /// requests are registered here, because they are processed asynchronously after the
    /// server returns 202 Accepted. A `notifications/cancelled` message can cancel one of
    /// these tasks; the last handle to the session going away cancels them all.
    in_flight: Arc<InFlight>,
}

/// The in-flight registry for one session, shared by every clone of it.
///
/// Cancelling on drop is what ties an asynchronous request's lifetime to its session, and it is
/// the only correct place for that: a legacy SSE response is pushed down the *stream*, not
/// returned on the POST that started the work, so the POST's own response is dropped — by
/// hyper, the moment the 202 is written — long before the work is done. Hanging the cancel
/// there stopped every request before it could answer. When the session is swept, terminated,
/// or evicted and the last clone goes, nobody is left who could receive the answer, and only
/// then is stopping the work right.
///
/// A [`CancellationToken`] rather than a task's `AbortHandle`, because a token can be minted
/// *before* the work it cancels exists. That is what lets a request be registered before its
/// task is spawned, which is the only ordering in which the guard below cannot run before the
/// insertion it is meant to undo.
#[derive(Debug, Default)]
struct InFlight(std::sync::Mutex<HashMap<JsonValue, CancellationToken>>);

impl InFlight {
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<JsonValue, CancellationToken>> {
        self.0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl Drop for InFlight {
    fn drop(&mut self) {
        for token in self.lock().values() {
            token.cancel();
        }
    }
}

/// Removes a request from its session's in-flight map when its task finishes or is cancelled, so
/// a request that has answered does not leave an entry behind.
///
/// Holds a `Weak`, deliberately. The task that owns this guard must not keep the registry
/// alive, or [`InFlight::drop`] could never run while anything was in flight — the request
/// would be holding open the very thing whose disappearance is meant to stop it, and "cancelled
/// if the session goes away" would be true only of a session with nothing to cancel.
pub(crate) struct RequestGuard {
    in_flight: std::sync::Weak<InFlight>,
    id: JsonValue,
}

impl Drop for RequestGuard {
    fn drop(&mut self) {
        if let Some(in_flight) = self.in_flight.upgrade() {
            in_flight.lock().remove(&self.id);
        }
    }
}

impl McpSession {
    fn new(sender: Option<mpsc::Sender<Event>>, key_id: Option<String>) -> Self {
        Self {
            sender,
            listeners: 0,
            last_activity: std::time::Instant::now(),
            key_id,
            in_flight: Arc::new(InFlight::default()),
        }
    }

    /// Register a request that will be processed asynchronously, returning the token its task
    /// should stop on and the guard that un-registers it once it has.
    ///
    /// Called *before* the task is spawned, and both halves come back from the one call so that
    /// ordering is not something a caller can get wrong. It is the only safe order: registering
    /// after the spawn leaves a window in which the task finishes first, its guard removes an
    /// entry that does not exist yet, and the insertion that follows is one nothing will ever
    /// take out. An `AbortHandle` cannot be registered this early — it does not exist until the
    /// task does — which is why the map holds a token instead.
    pub(crate) fn start_request(&self, request_id: JsonValue) -> (CancellationToken, RequestGuard) {
        let token = CancellationToken::new();
        self.in_flight
            .lock()
            .insert(request_id.clone(), token.clone());
        (
            token,
            RequestGuard {
                in_flight: Arc::downgrade(&self.in_flight),
                id: request_id,
            },
        )
    }

    /// Cancel an in-flight request by its JSON-RPC id. Returns true if the id was known.
    pub(crate) fn cancel_request(&self, request_id: &JsonValue) -> bool {
        if let Some(token) = self.in_flight.lock().remove(request_id) {
            token.cancel();
            true
        } else {
            false
        }
    }

    /// Is the server still holding a connection open for this session?
    ///
    /// True for a legacy SSE session whose push channel has not closed, and for a Streamable
    /// HTTP session with a listening stream open. Either way the server is writing keep-alives
    /// to a socket the client is reading, which says more about whether the client is there
    /// than the time since its last POST does.
    fn is_connected(&self) -> bool {
        if self.listeners > 0 {
            return true;
        }
        matches!(&self.sender, Some(sender) if !sender.is_closed())
    }

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

    /// A registry with room to spare and a long idle timeout, so a test that is not about
    /// either does not have to name them.
    const TEST_LIMITS: SessionLimits = SessionLimits {
        idle_timeout: Duration::from_secs(1800),
        max_sessions: 1024,
    };

    fn state() -> McpTransportState {
        McpTransportState::new(TEST_LIMITS)
    }

    /// Spawn a request task the way the legacy SSE transport spawns one: registered before it
    /// exists, stopping on the token that registration handed back, and holding the guard that
    /// un-registers it. Resolves to whether the work ran to completion rather than being
    /// cancelled, which is the only thing these tests need to see.
    ///
    /// Built here rather than inline so a test cannot accidentally assert against an ordering
    /// the transport does not use — the ordering is the property under test.
    fn spawn_request(
        session: &McpSession,
        request_id: JsonValue,
        work: impl std::future::Future<Output = ()> + Send + 'static,
    ) -> tokio::task::JoinHandle<bool> {
        let (token, guard) = session.start_request(request_id);
        tokio::spawn(async move {
            let _guard = guard;
            tokio::select! {
                biased;
                () = token.cancelled() => false,
                () = work => true,
            }
        })
    }

    #[tokio::test]
    async fn cancelling_a_registered_request_stops_its_task() {
        let state = state();
        let session_id = state.create_session(None).await;
        let SessionClaim::Granted(session) = state.claim_session(&session_id, None).await else {
            panic!("session should be claimable");
        };

        // Work that would never finish on its own, so completing can only mean it was not
        // cancelled.
        let request_id = JsonValue::from(7);
        let task = spawn_request(&session, request_id.clone(), std::future::pending());

        assert!(session.cancel_request(&request_id));
        assert!(
            !task.await.expect("the request task ran"),
            "the work carried on after its request was cancelled"
        );

        // A second cancellation no longer finds the request.
        assert!(!session.cancel_request(&request_id));
    }

    /// The session is what an asynchronous request's lifetime hangs on, so ending the session
    /// ends the work — and nothing before that does. Both halves matter: the POST's own response
    /// is dropped the moment the 202 is written, and hanging the cancel there is what stopped
    /// every legacy-SSE request before it could answer.
    #[tokio::test]
    async fn a_request_outlives_its_post_and_dies_with_its_session() {
        let state = state();
        let session_id = state.create_session(None).await;
        let SessionClaim::Granted(session) = state.claim_session(&session_id, None).await else {
            panic!("session should be claimable");
        };

        let request_id = JsonValue::from(11);
        let task = spawn_request(&session, request_id, std::future::pending());

        // Dropping the handle the POST held is not the end of the request: the answer goes to
        // the stream, so the work has to survive the request that started it.
        drop(session);
        tokio::task::yield_now().await;
        assert!(
            !task.is_finished(),
            "the request was cancelled when its POST was done with it"
        );

        // Ending the session is the end of it: nobody is left who could receive the answer.
        assert!(state.forget_session(&session_id).await);
        assert!(
            !task.await.expect("the request task ran"),
            "the request outlived the session it belonged to"
        );
    }

    /// A request that has answered leaves no entry behind, however fast it answers.
    ///
    /// The map is keyed by request id and only emptied by the guard, so an entry nothing removes
    /// is one the session carries until it ends. That is what the old ordering could leave:
    /// registering *after* the spawn let a task finish first, its guard remove an entry that did
    /// not exist yet, and the insertion that followed be one nothing would ever take out. Work
    /// that completes immediately is the shape that used to hit it, so that is what this runs —
    /// and it asserts the map is empty rather than merely that cancelling finds nothing, because
    /// the leak is the entry, not what cancelling makes of it.
    #[tokio::test]
    async fn a_finished_request_leaves_no_entry_however_fast_it_finishes() {
        let state = state();
        let session_id = state.create_session(None).await;
        let SessionClaim::Granted(session) = state.claim_session(&session_id, None).await else {
            panic!("session should be claimable");
        };

        for id in 0..8 {
            let request_id = JsonValue::from(id);
            let task = spawn_request(&session, request_id, std::future::ready(()));
            assert!(
                task.await.expect("the request task ran"),
                "work that cannot block was reported cancelled"
            );
        }

        let left_behind = session.in_flight.lock().len();
        assert_eq!(
            left_behind, 0,
            "{left_behind} finished requests are still registered as in flight"
        );
    }

    #[tokio::test]
    async fn a_session_belongs_to_the_key_that_created_it() {
        let state = state();
        let session = state.create_session(Some("aabbccdd".to_string())).await;

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
        let state = state();
        let session = state.create_session(Some("aabbccdd".to_string())).await;

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
        let state = state();
        let session = state.create_session(None).await;
        assert!(matches!(
            state.claim_session(&session, None).await,
            SessionClaim::Granted(_)
        ));
        assert!(matches!(
            state.claim_session(&session, Some("aabbccdd")).await,
            SessionClaim::Granted(_)
        ));
    }

    /// A legacy SSE session id must not be guessable.
    ///
    /// With authorization off a session is bound to nobody, so the id itself is the only thing
    /// standing between a stranger and someone else's conversation. Sequential ids handed that
    /// stranger the next one for free.
    #[tokio::test]
    async fn a_legacy_sse_session_id_is_not_guessable() {
        let state = state();
        let (first, _tx1, _rx1) = state.create_sse_session(None).await;
        let (second, _tx2, _rx2) = state.create_sse_session(None).await;
        assert!(
            Uuid::parse_str(&first).is_ok() && Uuid::parse_str(&second).is_ok(),
            "legacy SSE ids are not random UUIDs: {first}, {second}"
        );
    }

    /// The registry holds at most `max_sessions`, evicting the idlest rather than refusing
    /// the newest — a flood of `initialize` must not choose the registry's size, and must not
    /// lock a new caller out either.
    #[tokio::test]
    async fn a_full_registry_evicts_rather_than_grows_or_refuses() {
        const CAP: usize = 8;
        let state = McpTransportState::new(SessionLimits {
            max_sessions: CAP,
            ..TEST_LIMITS
        });
        for _ in 0..CAP {
            state.create_session(None).await;
        }
        let newest = state.create_session(None).await;

        let inner = state.inner.lock().await;
        assert_eq!(inner.sessions.len(), CAP, "the registry grew past its cap");
        assert!(
            inner.sessions.contains_key(&newest),
            "the newest session was refused instead of the idlest being evicted"
        );
    }

    /// The cap an operator configured is the cap that is enforced.
    ///
    /// The test above would pass against a compiled-in constant that happened to be larger, so
    /// this one asserts the number came from the configuration.
    #[tokio::test]
    async fn the_configured_cap_is_the_one_enforced() {
        let state = McpTransportState::new(SessionLimits {
            max_sessions: 2,
            ..TEST_LIMITS
        });
        for _ in 0..5 {
            state.create_session(None).await;
        }
        assert_eq!(state.inner.lock().await.sessions.len(), 2);
    }

    /// A Streamable HTTP session with a listening stream open is not idle, however long the
    /// client pauses.
    ///
    /// This is the case that sent a paused agent a 404: the client was holding the stream the
    /// server was writing keep-alives to, and the sweeper — which could only see the time since
    /// the last POST — took the session anyway.
    #[tokio::test]
    async fn a_listening_stream_keeps_its_session_off_the_sweeper() {
        let state = state();
        let listening = state.create_session(None).await;
        let quiet = state.create_session(None).await;
        assert!(state.open_listener(&listening).await);

        // Zero, so every session is past its idle timeout the moment it is checked: what
        // survives, survives for being connected rather than for being recent.
        state.inner.lock().await.sweep(Duration::ZERO);

        assert!(matches!(
            state.claim_session(&listening, None).await,
            SessionClaim::Granted(_)
        ));
        assert!(matches!(
            state.claim_session(&quiet, None).await,
            SessionClaim::Unknown
        ));
    }

    /// Closing the stream returns the session to the idle timeout rather than ending it.
    ///
    /// The difference from the legacy transport, which forgets a session the moment its stream
    /// drops: a Streamable HTTP session is meant to outlive any one connection, so a client
    /// whose stream was cut can still resume on its next POST.
    #[tokio::test]
    async fn closing_a_listening_stream_leaves_the_session_resumable() {
        let state = state();
        let session = state.create_session(None).await;
        assert!(state.open_listener(&session).await);
        state.close_listener(&session).await;

        // Still there while inside the timeout,
        state.inner.lock().await.sweep(Duration::from_secs(1800));
        assert!(matches!(
            state.claim_session(&session, None).await,
            SessionClaim::Granted(_)
        ));
        // and swept once past it, like any other disconnected session.
        state.inner.lock().await.sweep(Duration::ZERO);
        assert!(matches!(
            state.claim_session(&session, None).await,
            SessionClaim::Unknown
        ));
    }

    /// Two streams on one session, and the first to close does not un-register the second.
    #[tokio::test]
    async fn a_reconnected_listener_is_counted_separately() {
        let state = state();
        let session = state.create_session(None).await;
        assert!(state.open_listener(&session).await);
        assert!(state.open_listener(&session).await);

        state.close_listener(&session).await;
        state.inner.lock().await.sweep(Duration::ZERO);
        assert!(
            matches!(
                state.claim_session(&session, None).await,
                SessionClaim::Granted(_)
            ),
            "one stream closing dropped a session another stream was still holding"
        );
    }

    /// A stream cannot be registered on a session that is not there, which is what tells the
    /// handler not to install a guard that would decrement someone else's count later.
    #[tokio::test]
    async fn a_listener_cannot_be_opened_on_an_unknown_session() {
        let state = state();
        assert!(!state.open_listener("never-existed").await);
    }

    /// The sweep interval follows the idle timeout, and stays inside its bounds.
    ///
    /// The upper bound is what keeps a long timeout from being overshot by hours; the lower is
    /// what keeps a short one from spinning the sweeper.
    #[test]
    fn the_sweep_interval_is_derived_from_the_idle_timeout() {
        let interval = |secs| {
            McpTransportState::new(SessionLimits {
                idle_timeout: Duration::from_secs(secs),
                ..TEST_LIMITS
            })
            .sweep_interval()
        };
        assert_eq!(interval(300), Duration::from_secs(30));
        // Clamped rather than proportional at the extremes.
        assert_eq!(interval(5), MIN_SWEEP_INTERVAL);
        assert_eq!(interval(86_400), MAX_SWEEP_INTERVAL);
        // Never slower than the timeout it is meant to enforce.
        for secs in [1, 30, 300, 1800, 86_400] {
            assert!(
                interval(secs) <= Duration::from_secs(secs).max(MIN_SWEEP_INTERVAL),
                "a {secs}s timeout is swept less often than it expires"
            );
        }
    }
}
