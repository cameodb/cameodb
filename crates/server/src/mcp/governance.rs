//! What a tool call costs and what it leaves behind: the rate limit, and the audit record.

use cameodb_mcp::{McpAuthzRef, RateLimitVerdict};
use tracing::warn;

use crate::state::AppState;

/// Charge `cost` tokens against the calling key's budget.
///
/// Attributed by `key_id`, not by session: a session id is chosen by the caller's host
/// and a new one is a header away, so metering per session would let an agent reset its
/// own limit by reconnecting. The key is the thing an operator issued and can revoke.
///
/// The cost is the fan-out the protocol layer read off the call, so a search naming ten
/// indexes is charged as ten searches. Which is what it is: ten scatter-gathers across ten
/// indexes' shards, dispatched by one authorized request.
pub(super) fn check_tool_rate(
    state: &AppState,
    authz: McpAuthzRef,
    tool: &str,
    cost: u32,
) -> RateLimitVerdict {
    match state.tool_limiter.check(authz.key_id().as_deref(), cost) {
        crate::ratelimit::Verdict::Allow => RateLimitVerdict::Allow,
        crate::ratelimit::Verdict::Deny { retry_after_secs } => {
            // Worth a line each: this needs a valid key, so its volume is bounded by
            // someone who already holds credentials — the same reasoning that keeps a
            // 403 unthinned in `authz`.
            warn!(
                key_id = authz.key_id().unwrap_or_else(|| "-".to_string()),
                tool = tool,
                retry_after_secs = retry_after_secs,
                "MCP tool call refused: rate limit exceeded"
            );
            RateLimitVerdict::Deny { retry_after_secs }
        }
    }
}

/// Keep one line per tool call.
///
/// Every MCP tool is a read, and reads are the class this trail exists to make visible:
/// the HTTP gate has already recorded that somebody POSTed to `/mcp`, which answers
/// nothing. This is where "which index, which tool, and did it work" is written down.
///
/// Unlike the HTTP path there is no rollup, because there is nothing to roll up — an
/// agent's tool calls are counted in hundreds, not in the hundreds of thousands that
/// make per-event ingest records untenable.
pub(super) fn record_tool_call(
    state: &AppState,
    authz: McpAuthzRef,
    call: cameodb_mcp::ToolCall<'_>,
) {
    if !state.audit.is_enabled() {
        return;
    }
    let record = crate::audit::AuditRecord::mcp_tool(call.tool)
        .with_identity(authz.key_id(), authz.label(), authz.role())
        .with_index(call.index.map(str::to_string))
        // Gated here rather than in the mcp crate: whether a query is data worth keeping
        // is a deployment question, and the protocol layer holds no opinion about it.
        .with_query(
            state
                .audit
                .records_query_text()
                .then(|| call.query.map(str::to_string))
                .flatten(),
        );
    state.audit.record(match call.error {
        None => record.succeeded(),
        Some(error) => record.refused(error),
    });
}
