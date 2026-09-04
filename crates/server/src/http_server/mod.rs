//! The HTTP API: routes, the middleware stack, and one module per operation.
//!
//! [`routes`] mounts everything and owns the layer order; the operation modules hold the handlers
//! and the request types they deserialize, and are private so that nothing but `routes` can name
//! a handler. The MCP tools are not here — they answer on the same state but a different protocol,
//! and live in `crate::mcp`.

mod admin;
mod catalogue;
mod error;
mod health;
mod routes;
mod search;
mod write;

pub(crate) use health::HEALTH_PATH;
pub(crate) use routes::{RouterConfig, create_router};
