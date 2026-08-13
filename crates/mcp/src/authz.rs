//! Who the caller is, and what each tool requires of them.
//!
//! The host implements [`McpAuthz`] so identity reaches the tool dispatcher without this crate
//! learning any of the host's types.

use std::sync::Arc;

/// What an MCP operation requires of its caller.
///
/// Mirrors the host's capability set one for one. The mcp crate keeps its own copy because
/// it must not depend on the server crate — the host maps between them in its [`McpAuthz`]
/// implementation, which is the single place the two vocabularies meet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpCapability {
    Read,
    Write,
    IndexAdmin,
    NodeAdmin,
}

impl McpCapability {
    pub fn as_str(self) -> &'static str {
        match self {
            McpCapability::Read => "read",
            McpCapability::Write => "write",
            McpCapability::IndexAdmin => "index-admin",
            McpCapability::NodeAdmin => "node-admin",
        }
    }
}

/// The caller, as much of them as this crate needs to know.
///
/// Implemented by the host so identity reaches the tool dispatcher without this crate
/// learning any of the host's types. `/mcp` is a single JSON-RPC path, so path-level
/// middleware cannot see which tool or index is in play — everything below the transport
/// has to ask.
pub trait McpAuthz: Send + Sync + 'static {
    /// Non-reversible fingerprint of the key, for session binding and log lines. `None`
    /// when the host does not identify callers.
    fn key_id(&self) -> Option<String>;

    /// Whether this caller may touch `index`.
    fn allows_index(&self, index: &str) -> bool;

    /// The human name the operator gave this key, if any.
    ///
    /// Defaulted rather than required: it is used only to make an audit line readable, and a
    /// host that has no such notion should not be forced to invent one.
    fn label(&self) -> Option<String> {
        None
    }

    /// The caller's role, named as the host names it.
    fn role(&self) -> Option<String> {
        None
    }

    /// Whether this caller holds `capability`.
    fn has(&self, capability: McpCapability) -> bool;
}

/// How identity is carried through the transport. Cheap to clone into a spawned task.
pub type McpAuthzRef = Arc<dyn McpAuthz>;

/// The caller when the host has no authorization layer in front of this router.
///
/// A permissive default is correct only because the host decides whether it is used: with
/// `[security]` off there is no identity to enforce, and with it on the middleware always
/// supplies a real one.
#[derive(Debug, Clone, Copy)]
pub struct McpUnrestricted;

impl McpAuthz for McpUnrestricted {
    fn key_id(&self) -> Option<String> {
        None
    }

    fn allows_index(&self, _index: &str) -> bool {
        true
    }

    fn has(&self, _capability: McpCapability) -> bool {
        true
    }
}

/// What a tool requires, or `None` if it is not a tool this server knows.
///
/// **Deny by default.** A tool added to [`crate::tools::mcp_tools`] without a row here cannot be
/// called at all, which is the failure that gets noticed; inheriting `Read` from its neighbours
/// is the failure that does not. `every_advertised_tool_has_a_capability` keeps the two in step.
pub fn tool_capability(name: &str) -> Option<McpCapability> {
    match name {
        "search_index" | "search_indexes" | "get_index" | "list_indexes" | "validate_query"
        | "get_index_stats" => Some(McpCapability::Read),
        _ => None,
    }
}

/// The caller for a request the host did not identify.
pub(crate) fn unrestricted() -> McpAuthzRef {
    Arc::new(McpUnrestricted)
}

/// Callers used by the tests in this crate.
#[cfg(test)]
pub(crate) mod testing {
    use super::{McpAuthz, McpCapability};

    /// A caller scoped to one index, holding only `Read`.
    pub(crate) struct Scoped(pub(crate) &'static str);

    impl McpAuthz for Scoped {
        fn key_id(&self) -> Option<String> {
            Some("aabbccdd".to_string())
        }

        fn allows_index(&self, index: &str) -> bool {
            index == self.0
        }

        fn has(&self, capability: McpCapability) -> bool {
            capability == McpCapability::Read
        }
    }

    /// A caller holding nothing at all.
    pub(crate) struct NoCapabilities;

    impl McpAuthz for NoCapabilities {
        fn key_id(&self) -> Option<String> {
            None
        }

        fn allows_index(&self, _index: &str) -> bool {
            false
        }

        fn has(&self, _capability: McpCapability) -> bool {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_unknown_tool_is_denied_rather_than_defaulted() {
        assert_eq!(tool_capability("drop_everything"), None);
        assert_eq!(tool_capability(""), None);
        // Case matters: a lookup that normalised would let `Search_Index` through a table
        // written in lower case.
        assert_eq!(tool_capability("SEARCH_INDEX"), None);
    }

    #[test]
    fn an_unrestricted_caller_holds_everything() {
        let authz = McpUnrestricted;
        assert!(authz.allows_index("payroll"));
        for capability in [
            McpCapability::Read,
            McpCapability::Write,
            McpCapability::IndexAdmin,
            McpCapability::NodeAdmin,
        ] {
            assert!(authz.has(capability));
        }
        assert_eq!(authz.key_id(), None);
    }
}
