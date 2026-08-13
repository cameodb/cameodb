//! Protocol versions and the headers that carry them.
//!
//! Separate from both [`crate::transport`] and [`crate::rpc`] because both need it and neither
//! owns it: the transport validates the version header, the dispatcher echoes the negotiated
//! version back on `initialize`.

/// MCP protocol versions this server supports, newest first.
/// Used for version negotiation during `initialize` and for validating the
/// `MCP-Protocol-Version` HTTP header on the Streamable HTTP transport.
pub(crate) const SUPPORTED_PROTOCOL_VERSIONS: &[&str] = &["2025-06-18", "2025-03-26", "2024-11-05"];

/// Latest protocol version supported (returned when the client requests an
/// unknown version or omits one).
const LATEST_PROTOCOL_VERSION: &str = SUPPORTED_PROTOCOL_VERSIONS[0];

/// HTTP header carrying the session identifier on the Streamable HTTP transport.
///
/// Public because the host application's CORS policy has to both allow and expose it —
/// a browser client cannot use the transport otherwise.
pub const MCP_SESSION_ID_HEADER: &str = "mcp-session-id";

/// HTTP header carrying the negotiated protocol version on subsequent requests.
pub(crate) const MCP_PROTOCOL_VERSION_HEADER: &str = "mcp-protocol-version";

/// Negotiate the protocol version: echo the client's requested version if we
/// support it, otherwise fall back to our latest supported version (per MCP spec).
pub(crate) fn negotiate_protocol_version(requested: Option<&str>) -> &'static str {
    match requested {
        Some(req) => SUPPORTED_PROTOCOL_VERSIONS
            .iter()
            .find(|version| **version == req)
            .copied()
            .unwrap_or(LATEST_PROTOCOL_VERSION),
        None => LATEST_PROTOCOL_VERSION,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_supported_version_is_echoed_back() {
        // Echoing is the requirement, not answering with the newest: a client that asked for a
        // version it can speak and is told a different one has to decide whether to continue.
        for version in SUPPORTED_PROTOCOL_VERSIONS {
            assert_eq!(negotiate_protocol_version(Some(version)), *version);
        }
    }

    #[test]
    fn an_unknown_version_falls_back_to_the_latest() {
        // Both directions of unknown: a revision newer than anything here, and one older.
        assert_eq!(
            negotiate_protocol_version(Some("2026-07-28")),
            LATEST_PROTOCOL_VERSION
        );
        assert_eq!(
            negotiate_protocol_version(Some("2024-01-01")),
            LATEST_PROTOCOL_VERSION
        );
        assert_eq!(
            negotiate_protocol_version(Some("")),
            LATEST_PROTOCOL_VERSION
        );
    }

    #[test]
    fn an_absent_version_falls_back_to_the_latest() {
        assert_eq!(negotiate_protocol_version(None), LATEST_PROTOCOL_VERSION);
    }
}
