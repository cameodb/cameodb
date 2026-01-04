# CameoDB Node

The `server` crate hosts the CameoDB node process: a high-performance, actor-based database node that combines local hybrid storage (redb + Tantivy) with distributed coordination, routing, and remote execution over libp2p + Kameo.

This document focuses on the *node-side* architecture and the distributed workflows currently implemented.

---

## 1. Core Responsibilities

- **HTTP API surface**
  - Routes: `/api/{index}/search` (JSON), `/api/{index}/stream` (NDJSON, JSON fallback), `/api/{index}/document` (PUT), `/api/{index}/_bulk` (POST), `/api/{index}/_config` (GET/PUT), `/api/{index}/_schema` (PATCH), `/_indexes`, `/_cluster/_indexes`, `/_cluster/health`.
  - Translates requests into strongly-typed operations (`ClientOp`) and hands them to `RouterActor`.
  - Middleware: compression/decompression, trace, permissive CORS, request body limit; ConnectInfo enabled at serve for client addr extraction.
- **Local orchestration**
  - Manages microshards (`MicroshardActor`) and their storage configuration.
  - Ensures all redb/tantivy I/O is executed via `tokio::task::spawn_blocking`.
- **Cluster coordination**
  - Manages the libp2p swarm and Kademlia DHT.
  - Tracks peer nodes, shard ownership, and consistent-hash routing.
- **Distributed execution**
  - Uses Kameo remote actors over libp2p to forward operations to remote nodes.
  - Implements scatter–gather broadcast for search and multi-node writes.

---

## 2. Main Actors & Data Flow

### 2.1 RouterActor

The `RouterActor` is the primary ingress for database operations on a node.

- Input: `ClientOp`, representing logical client operations:
  - `Search { index, query, limit }`
  - `Stream { index, query }`
  - `Write { index, id, routing_key, doc }`
  - `BulkWrite { index, docs }`
  - `CreateConfig`, `GetConfig` (Schema management)
  - `ListIndexes`, `ListClusterIndexes` (Metadata)
- Responsibilities:
  - Ask the `ClusterCoordinator` for a routing decision:
    - `RoutingDecision::Local`
    - `RoutingDecision::Remote { node_id, peer_addr }`
    - `RoutingDecision::Broadcast`
  - Execute the chosen path:
    - Local → delegate to `NodeOrchestrator` on this node.
    - Remote → call `handle_remote` / `try_remote` with retries + timeouts.
    - Broadcast → fan out to local + remote nodes and aggregate results.
- Telemetry:
  - Tracks broadcast attempts and failures via `AtomicU64` counters.

### 2.2 NodeOrchestrator

`NodeOrchestrator` owns the node’s identity and all local shards.

- Fields:
  - `identity: NodeIdentity` (UUID, name, vnode tokens).
  - `shards: HashMap<Uuid, MicroshardActor>`.
  - `routing_ring: ConsistentRing` for shard placement.
- Startup:
  - Hydrates existing shards from disk.
  - Registers shard assignments with `ClusterCoordinator`.
- Message handling:
  - `Message<ClientOp>` delegates to `handle_client_op`, which:
    - Maps logical operations into local microshard operations.
    - Uses `spawn_blocking` for all redb/tantivy calls.
- Remote capability:
  - `#[derive(Actor, RemoteActor)]`.
  - `#[remote_message("cameo.orchestrator.client_op")] impl Message<ClientOp>` enables remote `ask`.

### 2.3 MicroshardActor

Each `MicroshardActor` manages a single shard’s data and index.

- Storage:
  - redb for durable KV and WAL.
  - Tantivy for full-text search.
- Remote messages:
  - `SearchRequest` → `Result<SearchReply, RemoteError>`.
  - `WriteRequest` → `Result<WriteReply, RemoteError>`.
  - `BatchWriteRequest` → `Result<BatchWriteReply, RemoteError>`.
- All storage operations are executed inside `spawn_blocking` to avoid blocking async executors.

### 2.4 ClusterCoordinator & DistributedCluster

`ClusterCoordinator` owns a `DistributedCluster` and exposes cluster operations via actor messages.

- **Core State**:
  - `peer_nodes: HashMap<Uuid, NodeInfo>` - Remote node id, address, status, shard_count.
  - `shard_assignments: HashMap<Uuid, ShardMetadata>` - Which shards live on which nodes.
  - `ring: ConsistentRing` - Used for consistent hashing.
  - `state: ClusterState` - Current cluster health (`Active`, `Degraded`, `Failed`).
  - `expected_nodes: HashSet<Uuid>` - Nodes expected from persisted snapshot.
  - `expected_shards: HashMap<Uuid, ShardMetadata>` - Shards expected from snapshot for reconciliation.

- **Key Messages**:
  - `InitSwarm` / `ShutdownSwarm` - Swarm lifecycle.
  - `DiscoverPeers`, `GetStatus` - Cluster observability.
  - `RegisterLocalShards` + `rebuild_ring()` - Maintain shard metadata.
  - `RouteOperation` → `RoutingDecision` - Read/write routing.
  - `GetKnownPeers` → `Vec<KnownPeer>` - Broadcast fan-out.
  - `PeerDiscovered` / `PeerLost` - Membership events trigger state transitions.
  - `MergeRemoteShards` - Reconcile remote node shard reports with local expectations.

- **Event-Driven State Management**:
  - No background polling or timeouts.
  - State transitions occur only on membership events (`PeerDiscovered`, `PeerLost`).
  - Cluster metadata persisted inline with state changes to `metadata.redb`.
  - Pure Kameo actor message-driven lifecycle.

---

## 3. Networking & Remote Actors

### 3.1 Libp2p Swarm & Kademlia

The node builds a custom libp2p swarm with:

- TCP (nodelay), QUIC, Noise, Yamux.
- Kademlia DHT for peer discovery and routing metadata.
- Custom `DhtBehaviour`:
  - `kademlia: kad::Behaviour<MemoryStore>`.
  - `kameo: kameo::remote::Behaviour`.

After the swarm is built, the code calls:

```rust
swarm.behaviour_mut().kameo.init_global();
```

This wires Kameo’s remote registry into the swarm so actor lookups and remote messaging flow over libp2p.

### 3.2 Remote Actor Naming & Registration

To make nodes discoverable for remote calls, CameoDB uses stable actor names:

- `orchestrator-{node_id}` for `NodeOrchestrator`.
- `shard-{shard_id}` for `MicroshardActor` (planned for direct shard-to-shard calls).

On startup, after `NodeOrchestrator` is spawned, the node:

1. Computes the name using `orchestrator_remote_name(node_id)`.
2. Calls `orchestrator_ref.register(name).await` to register with the Kameo registry.

### 3.3 Remote Call Path (`RouterActor::try_remote`)

When `ClusterCoordinator` decides that an operation should be routed to a remote node:

1. `RouterActor::handle_remote` applies retries and timeouts.
2. Each attempt calls `RouterActor::try_remote`:
   - Builds the orchestrator name from the target node’s UUID.
   - Uses `RemoteActorRef::<NodeOrchestrator>::lookup(name).await`.
   - On success, forwards the original `ClientOp` with `remote.ask(&op).await`.
3. Errors and timeouts are logged and converted into `OrchestratorError`.

This path reuses the same `ClientOp` semantics on remote nodes as on the local node.

---

## 4. Routing & Distribution Semantics

### 4.1 Single-Key Read/Write Routing

For **writes**, the system derives an effective `routing_key` using the following priority:

1. **Explicit routing_key from the client payload** (if provided).
2. **Document id field**: if the JSON document has an `"id"` field, that value is used.
3. **Derived key from document bytes**: if neither of the above is present, the document is
   serialized to JSON, hashed using XXH3-64, and hex-encoded into a stable key.

This effective key is then used consistently across the cluster:

1. `RouterActor` sends `RouteOperation { routing_key, operation_type }` to `ClusterCoordinator`.
2. `ClusterCoordinator::decide_route`:
   - Uses `ConsistentRing::get_owner(key)` (XXH3 based) to map key → `shard_id`.
   - Uses `shard_owner(shard_id)` to map `shard_id` → `node_id`.
   - Looks up node address in `peer_nodes`.
   - Returns:
     - `RoutingDecision::Local` if the owner node is the local node.
     - `RoutingDecision::Remote { node_id, peer_addr }` if a remote owner is known.
     - `RoutingDecision::Broadcast` if metadata is missing.
3. `RouterActor` executes the routing decision:
   - Local: calls `NodeOrchestrator` directly.
   - Remote: uses Kameo remote actors as described above.
   - Broadcast: falls through to scatter–gather.

This gives you single-owner semantics for keyed **writes** across the cluster while still
providing a deterministic, evenly distributed fallback when clients do not specify a
routing key explicitly.

**Metadata operations** (`GetConfig`, `CreateConfig`, `ListIndexes`) always execute locally on the node handling the HTTP request. They do not broadcast or remote, since schema/config data is available via the local `HybridStore`.

**Cluster-wide Metadata** (`ListClusterIndexes`) is an exception: it broadcasts to all nodes to aggregate index statistics and shard counts across the cluster.

Schema metadata is cached per node inside `NodeOrchestrator` to avoid repeated redb reads on every request. The cache is populated on first read (`_config`), updated on schema evolution or `CreateConfig`, and returned on subsequent requests with fields sorted (`id` first, others alphabetical).

### 4.2 Broadcast / Scatter–Gather

When there is **no routing_key**, or the coordinator chooses `Broadcast` because of missing metadata, the router performs a scatter–gather:

1. Ask `ClusterCoordinator` for known peers via `GetKnownPeers`.
2. Select up to `broadcast_fanout_limit` peers.
3. Execute in parallel:
   - Local `handle_client_op` over local microshards.
   - Remote `try_remote` calls to selected peers, each wrapped in `timeout(broadcast_timeout, ...)`.
4. Aggregate results:
    - **Search**:
      - Collect `hits` arrays from all successful responses.
      - Merge into a single `hits` list capped by the configured `default_search_limit`.
      - Sort by `_score` descending.
      - Report `hits_returned`, `total_hits`, `limit`, plus `total_shards` and `failed_shards`.
   - **Write/BulkWrite**:
     - Report number of nodes contacted, succeeded, and failed.
   - Other operations:
     - Return first successful result or an aggregated failure.
5. Update `broadcasts_total` and `broadcast_failures` telemetry counters.

This implements a distributed search/read path suitable for fan-out queries across nodes while preserving timeouts and backpressure.

---

## 5. Error Handling & Telemetry

- **Error types**:
  - `OrchestratorError` for node-level orchestration failures.
  - `RemoteError` for remote microshard call failures.
  - Conversions are implemented so remote errors can be surfaced as orchestrator errors.
- **Retries & timeouts**:
  - Remote routing uses bounded retries (`remote_retry_attempts`) and per-attempt `remote_timeout`.
  - Broadcast uses `broadcast_timeout` per remote call.
- **Telemetry**:
  - Broadcast paths track:
    - Total broadcast attempts.
    - Broadcast failures (per failed shard/node).

---

## 6. Cluster Metadata Persistence & State Reconciliation

### 6.1 Event-Driven Persistence

Cluster metadata is persisted to `metadata.redb` using a **zero-polling, event-driven** approach:

- **No background tasks** - All persistence triggered inline with state-changing actor messages.
- **No timeouts** - State transitions occur only on actual membership events.
- **Message-driven lifecycle** - Fully aligned with Kameo actor model.

**Persistence Triggers:**
- `PeerDiscovered` → Update node registry, evaluate cluster state, persist snapshot.
- `PeerLost` → Mark node inactive, transition state (e.g., Active → Degraded), persist.
- `MergeRemoteShards` → Reconcile shard metadata, update assignments, persist.

**Persisted Data (`metadata.redb` tables):**
- `cluster_config` - Generation number, expected node count, last stable timestamp.
- `shard_assignments` - Per-shard metadata (node owner, vnode tokens, document count, storage bytes).
- `node_registry` - Node info (UUID, address, shard count, first/last seen timestamps).
- `ring_snapshot` - (Reserved for future ring serialization optimization).

### 6.2 State Reconciliation on Boot

When a node restarts with persisted metadata, it performs **snapshot-vs-reality reconciliation**:

**Boot Sequence:**
1. Load `metadata.redb` → `PersistedClusterTopology`.
2. Extract `expected_nodes` (nodes from last run) → mark as Inactive.
3. Extract `expected_shards` (shard assignments from snapshot) → store for comparison.
4. Set initial state to `Degraded` (only local node active, others expected).
5. Wait for peer discovery via libp2p Kademlia.

**Reconciliation on Peer Join:**
When a remote node sends `PeerDiscovered` and reports its shards via `MergeRemoteShards`:

1. **Compare** actual reported shards vs expected shards from snapshot.
2. **Categorize**:
   - **Matched**: Shard exists, metadata unchanged (document count, storage bytes match).
   - **Changed**: Shard exists but metadata differs (e.g., +200 docs since last shutdown).
   - **Added**: Shard not in snapshot (new shard created on remote node).
   - **Missing**: Expected from snapshot but not reported (possible data loss or migration).
3. **Log** detailed reconciliation results for operational visibility.
4. **Accept** remote node's reported state as source of truth.
5. **Persist** reconciled cluster topology to `metadata.redb`.

**Example Reconciliation Log:**
```
INFO ClusterCoordinator: reconciling node state with snapshot
  node=b2c3d4e5-... matched=1 added=1 changed=1 missing=1

INFO New shards not in snapshot
  node=b2c3d4e5-... shards=[shard-uuid-4]

INFO Shard state changed since snapshot
  node=b2c3d4e5-... shard=shard-uuid-1
  expected_docs=1000 actual_docs=1200
  expected_bytes=5242880 actual_bytes=6291456

WARN Expected shards from snapshot not reported by node
  node=b2c3d4e5-... shards=[shard-uuid-3]
```

### 6.3 Cluster State Machine

Simplified reactive state machine with three states:

- **Active** - All expected nodes present and healthy.
- **Degraded** - Some expected nodes inactive or missing.
- **Failed** - Too few nodes to operate reliably (< 50% quorum).

**State Transitions:**
- `PeerDiscovered` → May transition Degraded → Active if all nodes rejoined.
- `PeerLost` → May transition Active → Degraded or Degraded → Failed.
- No waiting states, no timeouts - purely reactive to membership events.

---

## 7. Current Distributed Feature Coverage

The `server` crate currently supports:

- Local hybrid-search storage with per-shard actors and blocking I/O isolation.
- Clustered routing using consistent hashing and shard assignments.
- Remote execution of logical `ClientOp` operations on other nodes via Kameo + libp2p.
- Scatter–gather broadcast for unkeyed search and multi-node writes.
- **Event-driven cluster metadata persistence** with zero-polling architecture.
- **Snapshot-based state reconciliation** on boot with discrepancy logging.
- **Simplified cluster state machine** (Active/Degraded/Failed) triggered by membership events.
- Basic resilience hooks (retries, timeouts) and telemetry.

Planned future work includes:

- Ring snapshot persistence optimization for faster boot (10-40x) on large clusters.
- Cluster state history tracking for SLA monitoring and failure forensics.
- Health metrics persistence for capacity planning.
- `GetClusterSnapshot` query API for operational visibility.
- Dedicated remote registry/connector module to cache `RemoteActorRef`s.
- Configurable fallback modes (e.g. local-only when remoting is disabled).

---

## 8. Distributed Flows (Sequence Diagrams)

This section illustrates the main distributed workflows implemented by the `server` crate.

### 8.1 Local Read/Write

```mermaid
sequenceDiagram
    autonumber
    participant Client
    participant HTTP as HTTP API
    participant Router as RouterActor
    participant Coord as ClusterCoordinator
    participant Orchestrator as NodeOrchestrator
    participant Shards as MicroshardActors

    Client->>HTTP: HTTP request (Search / Write / BulkWrite)
    HTTP->>Router: ClientOp
    Router->>Coord: RouteOperation { routing_key, op_type }
    Coord-->>Router: RoutingDecision::Local

    Router->>Orchestrator: ClientOp
    Orchestrator->>Shards: per-shard ops (via Actor messages)
    Shards->>Shards: redb + Tantivy via spawn_blocking
    Shards-->>Orchestrator: shard-level replies
    Orchestrator-->>Router: aggregated JSON result
    Router-->>HTTP: JSON result
    HTTP-->>Client: HTTP response
```

### 8.2 Remote Read/Write

```mermaid
sequenceDiagram
    autonumber
    participant Client
    participant HTTP as HTTP API
    participant Router as RouterActor (local)
    participant Coord as ClusterCoordinator (local)
    participant RemoteOrch as NodeOrchestrator (remote node)
    participant RemoteShards as MicroshardActors (remote)

    Client->>HTTP: HTTP request (Search / Write / BulkWrite)
    HTTP->>Router: ClientOp
    Router->>Coord: RouteOperation { routing_key, op_type }
    Coord-->>Router: RoutingDecision::Remote { node_id, peer_addr }

    loop up to remote_retry_attempts
        Router->>Router: handle_remote (timeout + retry)
        Router->>RemoteOrch: Kameo remote ask(ClientOp)\n(via libp2p + kameo::remote::Behaviour)
        RemoteOrch->>RemoteShards: per-shard ops
        RemoteShards->>RemoteShards: redb + Tantivy via spawn_blocking
        RemoteShards-->>RemoteOrch: shard-level replies
        RemoteOrch-->>Router: aggregated JSON result
    end

    Router-->>HTTP: JSON result or OrchestratorError
    HTTP-->>Client: HTTP response
```

### 8.3 Broadcast Search (Scatter–Gather)

```mermaid
sequenceDiagram
    autonumber
    participant Client
    participant HTTP as HTTP API
    participant Router as RouterActor (local)
    participant Coord as ClusterCoordinator
    participant LocalOrch as NodeOrchestrator (local)
    participant LocalShards as MicroshardActors (local)
    participant RemoteOrchN as NodeOrchestrator (remote peers)
    participant RemoteShardsN as MicroshardActors (remote peers)

    Client->>HTTP: HTTP search without routing_key
    HTTP->>Router: ClientOp::Search
    Router->>Coord: RouteOperation { routing_key = None, Read }
    Coord-->>Router: RoutingDecision::Broadcast

    Router->>Coord: GetKnownPeers
    Coord-->>Router: Vec<KnownPeer> (node_id, address)
    Router->>Router: select up to broadcast_fanout_limit peers

    par Local search
        Router->>LocalOrch: ClientOp::Search
        LocalOrch->>LocalShards: shard-level search
        LocalShards->>LocalShards: Tantivy search via spawn_blocking
        LocalShards-->>LocalOrch: hits per shard
        LocalOrch-->>Router: local JSON result { hits, ... }
    and Remote fan-out
        loop for each selected peer
            Router->>RemoteOrchN: Kameo remote ask(ClientOp::Search)\n(timeout = broadcast_timeout)
            RemoteOrchN->>RemoteShardsN: shard-level search
            RemoteShardsN->>RemoteShardsN: Tantivy search via spawn_blocking
            RemoteShardsN-->>RemoteOrchN: hits per shard
            RemoteOrchN-->>Router: remote JSON result { hits, ... } or error
        end
    end

    Router->>Router: merge hits, sort by _score,\ntrack failed_shards
    Router-->>HTTP: aggregated JSON { hits, total_shards, failed_shards }
    HTTP-->>Client: HTTP response
```

