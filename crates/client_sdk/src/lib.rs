use uuid::Uuid;

/// Represents an identifier for a remote cluster node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeId(pub Uuid);

/// Placeholder client designed for future HTTP interactions.
#[derive(Debug, Default)]
pub struct ClientSdk {}

impl ClientSdk {
    /// Construct a new SDK instance.
    pub fn new() -> Self {
        Self {}
    }
}
