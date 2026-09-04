//! CameoDB's MCP server: the protocol, the tool catalogue and the guidance that travels with it.
//!
//! The crate holds no CameoDB behaviour. A host implements [`McpBackend`] and [`McpAuthz`] and
//! mounts [`mcp_router`]; everything here is protocol and description, which is why it can
//! depend on nothing of the server's.
//!
//! Layered so each module answers to exactly one thing above it:
//!
//! - `transport` owns HTTP — routes, headers, sessions, status codes.
//! - `rpc` owns the JSON-RPC envelope and dispatch, and knows nothing of HTTP.
//! - `tools` owns the catalogue: what each tool accepts, and running one against the backend.
//! - `backend` and `authz` are the host's side of the boundary.
//! - [`syntax`] and `guidance` are the description: query syntax rendered from one table, and
//!   the prose an agent is given.
//!
//! [`syntax`] is public because the server crate renders the same tables into its own responses.
//! Nothing else needs to be.

pub mod syntax;

mod authz;
mod backend;
mod guidance;
mod protocol;
mod rpc;
mod session;
mod tools;
mod transport;

pub use authz::{McpAuthz, McpAuthzRef, McpCapability, McpUnrestricted, tool_capability};
pub use backend::{
    McpBackend, McpIndexSearchRequest, RateLimitVerdict, SortOrder, SortSpec, ToolCall,
};
pub use protocol::MCP_SESSION_ID_HEADER;
pub use session::McpShutdownHandle;
// The bounds the tool schemas advertise. Public because the host enforces the same numbers on
// the value a search runs with, which is not always the argument a client sent — importing
// them is what keeps the advertised bound and the enforced one from becoming two numbers.
pub use tools::schema::{DEFAULT_MAX_FEDERATED_INDEXES, DEFAULT_MAX_SEARCH_LIMIT};
pub use transport::{McpTransportConfig, mcp_router};
