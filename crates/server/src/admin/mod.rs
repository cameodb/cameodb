//! Admin module — observability and operational endpoints.
//!
//! Extracted from `node.rs` for maintainability.
//! Each sub-module owns its types, free functions, and the
//! `Message` implementations that run on `NodeOrchestrator`.

pub mod memory;
