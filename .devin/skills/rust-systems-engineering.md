# General Rust Systems Engineering Skills

This document outlines the general Rust skills, principles, and best practices expected when working on this project.

## 1. Error Handling Strategy
- **Libraries (`crates/cluster`, `crates/storage`)**:
  - Use `thiserror`.
  - Define specific, strong error enums (e.g., `StorageError::WalFull`).
  - Do not panic; always return a `Result`.
- **Application/Binaries (`crates/server`, Actors)**:
  - Use `anyhow::Result`.
  - Context is key: always attach context to errors, e.g., `.context("Failed to initialize shard")`.

## 2. Strict Typing & Data Flow
- Avoid "Stringly Typed" code. Use strong Rust enums and structs.
- Do not pass raw `serde_json::Value` deep into the core logic. Parse JSON into strong Rust structs immediately at the API boundary (e.g., `axum` handlers).

## 3. Coding Style
- Follow standard Rust idioms (strict Clippy compliance).
- Prefer `impl Trait` or Generics over `Box<dyn Trait>` unless dynamic dispatch is strictly required by the architecture.
- Use `tracing::{info, warn, error, debug, trace}` for logging. **Never** use `println!`.
