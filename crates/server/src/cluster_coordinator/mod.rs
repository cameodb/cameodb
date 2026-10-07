//! The cluster coordinator: an actor owning the distributed cluster state, and the
//! messages it answers. `messages.rs` holds the wire types; `coordinator.rs` the actor
//! and its handlers.

mod coordinator;
mod messages;
mod schema_change;

#[cfg(test)]
mod tests;

pub use coordinator::*;
pub use messages::*;
pub(crate) use schema_change::{
    FieldEdit, SchemaReconciler, change_schema_cluster, merge_learned, patch_schema_cluster,
};
