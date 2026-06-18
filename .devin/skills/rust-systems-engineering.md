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

## 3. Ownership & Cloning
- Prefer borrowing over cloning. Clone only at system boundaries (e.g., cloning `Arc<HybridStore>` before `spawn_blocking`, cloning config/name strings when crossing actor message boundaries).
- Use `Arc` for shared ownership across threads. `Rc` is forbidden — the entire codebase is tokio-multi-threaded.
- Use `ArcSwap` for lock-free atomic updates to shared state (e.g., routing ring, schema cache) — readers always see the latest snapshot without locking.

## 4. Async Discipline
- Mark functions `async fn` **only** when they actually `.await`. The storage crate (`crates/storage`) has zero `async fn` — it is purely synchronous/blocking.
- Never call `redb` or `tantivy` methods from an async context. Always wrap in `tokio::task::spawn_blocking` (see `.devinrules` §1 for the full pattern).

## 5. Module Structure & Visibility
- Keep `mod.rs` files thin — they declare sub-modules and re-export public items. Implementations go in named files.
- Use `pub(crate)` to limit visibility to the crate. Only expose `pub` when the item is part of the crate's public API.

## 6. Coding Style
- Follow standard Rust idioms (strict Clippy compliance).
- Prefer `impl Trait` or Generics over `Box<dyn Trait>` unless dynamic dispatch is strictly required by the architecture.
- Use `tracing::{info, warn, error, debug, trace}` for logging. **Never** use `println!`.

## 7. Testing
- Integration tests go in `tests/` directories per crate (e.g., `crates/cluster/tests/`, `crates/storage/tests/`).
- Storage tests are synchronous (no `#[tokio::test]` needed) since `HybridStore` is entirely blocking.
- Server/actor tests that need the full runtime use `#[tokio::test]` in the `tests/` directory.
