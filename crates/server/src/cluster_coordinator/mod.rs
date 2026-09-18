//! The cluster coordinator: an actor owning the distributed cluster state, and the
//! messages it answers. `messages.rs` holds the wire types; `coordinator.rs` the actor
//! and its handlers.

mod coordinator;
mod messages;

#[cfg(test)]
mod tests;

pub use coordinator::*;
pub use messages::*;
