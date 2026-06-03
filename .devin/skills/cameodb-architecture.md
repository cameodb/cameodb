# CameoDB Specific Architecture & Skills

This document outlines the highly specific architectural rules, dependencies, and core system patterns used in the development of CameoDB.

## 1. Core Dependencies & Versions
*Do not hallucinate versions. Use these strictly:*
- **Actor Framework**: `kameo` (0.19) - *Note: It uses Tokio 1.x.*
- **Web Server**: `axum` (0.7)
- **KV Store**: `redb` (3.1)
- **Search**: `tantivy` (0.25)
- **Async Runtime**: `tokio` (1.48)
- **HTTP Client**: `reqwest` (0.12)

---

## 2. Critical Rules

### 2.1 Async/Sync Isolation
The storage engines (`redb` and `tantivy`) are **BLOCKING** and synchronous. The Actor system (`kameo`) and Web Server (`axum`) are **ASYNC** (`tokio`).

- **RULE**: You must NEVER call `redb` or `tantivy` methods directly inside an async `fn` or Actor `handle` method.
- **RULE**: You must ALWAYS wrap these calls inside `tokio::task::spawn_blocking`.
- **RULE**: Ensure your storage structs (e.g., `HybridStore`) implement `Arc + Send + Sync` so they can be cloned into blocking tasks.

**✅ CORRECT PATTERN:**
```rust
async fn handle(&mut self, msg: WriteMsg, _ctx: Context<'_, Self, Self::Mailbox>) -> Result<u64> {
    let store = self.store.clone(); // Clone the Arc
    // Offload to blocking thread pool
    let result = tokio::task::spawn_blocking(move || {
        store.apply_op(msg.op) // Blocking call happens here
    }).await??; 
    Ok(result)
}
```

**❌ WRONG PATTERN:**
```rust
async fn handle(&mut self, msg: WriteMsg, ...) -> Result<u64> {
    // THIS IS FORBIDDEN: It blocks the async runtime thread
    self.store.apply_op(msg.op) 
}
```

### 2.2 Kameo Actor System Implementation Rules
When implementing an Actor using `kameo`, strictly follow this pattern:

1. **Struct Definition**: Use `#[derive(Actor)]` on the struct (or manually implement `Actor` defining `type Args`, `type Error`, and `fn on_start`).
2. **Messages**: Define message structs for the actions the actor can receive.
3. **Traits**: Implement `Message<Msg>` for the Actor, specifying `type Reply = ...`.
4. **Handling**: Handle the core logic exclusively inside the `async fn handle` method of the `Message` trait block. *Do not match generic enums inside a global handle function.*

---

## 3. Architectural Boundaries

- **Shared-Nothing**: There is no central master. State is emergent.
- **Topology**: Use `crates/cluster` for all Consistent Hashing (Ring) and Node Identity logic.
- **Storage**: The atomic unit is a "Shard" (Microshard). A Shard contains BOTH a `redb` file (for data/WAL) and a `tantivy` directory (for search).
- **Routing**:
  - `routing_key` present -> Unicast (Hash Ring Lookup).
  - `routing_key` missing -> Scatter-Gather (Broadcast to all shards).
- **Storage Boundary**: `redb` stores raw bytes (Bincode/JSON); `tantivy` stores indexed fields.
