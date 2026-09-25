# CameoDB Configuration Guide

This guide covers comprehensive configuration management for CameoDB, including network settings, storage paths, and Tantivy search engine tuning.

## Table of Contents

- [Quick Start](#quick-start)
- [Configuration Sources](#configuration-sources)
- [Configuration Reference](#configuration-reference)
- [Security and Posture](#security-and-posture)
- [Environment Variables](#environment-variables)
- [Multi-Disk Setup](#multi-disk-setup)
- [Performance Tuning](#performance-tuning)
  - [Write throughput: what actually moves it](#write-throughput-what-actually-moves-it)
- [Production Deployment](#production-deployment)
- [Troubleshooting](#troubleshooting)

## Quick Start

### 1. Generate Default Configuration

```bash
# Generate sample configuration file
cargo run --release --bin cameodb generate-config > cameodb.toml

# Or use the configuration manager
./scripts/setup/config-manager.sh generate
```

### 2. Basic Configuration

Edit `cameodb.toml`:

```toml
[node]
label = "cameo-node-01"
# Omitted only for a loopback bind, which infers "local". Anything reachable from
# another host must state its posture — see Security profiles.
profile = "local"

[network.http]
bind_address = "127.0.0.1"
port = 9480

[storage]
data_paths = ["./data/cameodb"]

[limits]
total_memory_limit_mb = 2048

[search]
indexer_memory_min_mb = 64
indexer_memory_max_mb = 512
default_search_limit = 10
```

### 3. Start CameoDB

```bash
cargo run --release --bin cameodb
```

## Configuration Sources

CameoDB loads configuration from multiple sources with the following precedence (highest to lowest):

1. **Command-line flags** (e.g. `--http-port 9999`)
2. **Environment Variables**
3. **Configuration Files**
4. **Default Values** (lowest priority)

### Configuration File Locations

CameoDB searches for configuration files in this order:

1. `cameodb.toml`, `cameodb.yaml`, `cameodb.yml` (current directory)
2. `config/cameodb.toml`, `config/cameodb.yaml`
3. `/etc/cameodb/cameodb.toml`, `/etc/cameodb/config.toml`

Both TOML and YAML formats are supported. An explicit `--config <path>` (or
`CAMEODB_CONFIG`) bypasses the search entirely.

## Configuration Reference

### Network Configuration

```toml
[network.http]
# Port for HTTP (default: 9480)
port = 9480

# Bind address for HTTP (default: "127.0.0.1" — loopback only).
# A non-loopback address is reachable from other hosts, so it requires an explicit
# `profile` under [node]; see Security profiles.
bind_address = "127.0.0.1"

# Request timeout in seconds. Unset, it is derived from limits.max_record_size_mb; see
# Size and memory limits below. Write it only to override that derivation.
# request_timeout_secs = 60

# CORS allowed origins (default: [] — no cross-origin browser access).
# `["*"]` is permitted only on a `local` profile, and warned about there.
# CORS governs browsers only: no API or MCP client is affected by an empty list.
cors_allowed_origins = []
```

### Size and memory limits

```toml
[limits]
# Largest single record in MB (default: 64). The source of truth for message size.
max_record_size_mb = 64

# HTTP body ceiling in MB. 0 (default) derives it from max_record_size_mb.
max_body_size_mb = 0

# The node's memory budget in MB (default: 2048).
total_memory_limit_mb = 2048

# Largest number of indexes held open at once. 0 (default) derives it from the memory budget.
max_open_indexes = 0

# Largest MCP search response in bytes. Unset, it follows the HTTP body ceiling.
# max_response_bytes = 16777216
```

`max_record_size_mb` is the only one of these an operator usually sets, because the rest derive
from it:

| Derived limit | Formula | At the default 64 MB |
|---|---|---|
| HTTP max body size | `max_record_size_mb + 64` MB | 128 MB |
| Inter-node message max | `max_record_size_mb × 1.25` | 80 MB |
| HTTP request timeout | `max(60, max_record_size_mb / 10)` s | 60 s |
| Largest MCP search response | the HTTP body size | 128 MB |
| Open indexes | `total_memory_limit_mb / indexer_memory_min_mb`, clamped to 8–256 | 32 |

Each derived value can be pinned on its own — `limits.max_body_size_mb`,
`limits.max_response_bytes`, `limits.max_open_indexes`, `network.http.request_timeout_secs` —
and a written value always wins, whatever it says. Because most of them are *not* written in the file, `cameodb
check-config` prints what the node resolved, and says of the timeout which of the two it is:

```
Limits: record 420MB, HTTP body 512MB, remote msg 525MB, MCP response 16MB, timeout 90s (derived), memory budget 96000MB
```

`max_record_size_mb / 10` is also the floor a written timeout is measured against: it is how
long a maximum-size record needs to arrive, so a shorter timeout means the record size
configured here can never be received. The node warns and honours the value — a search-only
node that accepts no large writes is entitled to a short timeout — but on a node that does take
them, the two settings need to agree.

The timeout is the one setting here with a command-line and environment override,
`--request-timeout-secs` / `CAMEODB_REQUEST_TIMEOUT_SECS`, because it is usually changed while
trying something rather than while writing a file.

Inter-node forwarding has its own deadline, `network.cluster.messaging.request_timeout_secs`.
Unset, it follows the HTTP timeout, so that a forwarded request is not abandoned while the
client that triggered it is still waiting; set it shorter only deliberately.

**`limits.max_open_indexes` bounds what the node holds open, not what it stores.** Every open
index costs an indexing arena and `indexer_num_threads + merge_num_threads` OS threads — three
at the defaults — and none of that is proportional to how much data the index holds. On a node
where callers create their own indexes, that makes resident memory a function of how many index
*names* have been touched. Past the cap the least recently used index is committed and closed;
its data is untouched and the next request for it reopens it, paying one reopen. Startup and
`check-config` print what a given cap implies in both currencies:

```text
  limits           PASS  32 indexes open at once = 2048 MB of arenas and 96 writer threads worst case
```

There is no setting for "unbounded" — `0` derives rather than disables. A deployment that
genuinely wants every index resident sets a number past its index count.

**`max_body_size_mb × network.http.max_concurrent_requests` has to fit inside
`total_memory_limit_mb`.** In-flight request bodies are held in memory, so a large body ceiling
and a high concurrency limit multiply into a way to run the node out of memory from outside.
Startup and `check-config` weigh the product and warn:

```
[WARN] limits   max_concurrent_requests (128) × body limit (484 MB) allows 61952 MB of in-flight
                request data, over this node's limits.total_memory_limit_mb (2048 MB)
```

**`max_concurrent_requests` also has to fit inside the request timeout, and that is a separate
check.** The memory arithmetic above bounds how much body the node holds; this one bounds how
much *work* it admits. A permit is a request, not the time the request implies, so admitting
3,000 of them on a node that serves 500 searches a second is six seconds of backlog against
whatever budget the timeout allows. Past that point it is the timeout, not admission, that
decides what gets shed — and a request shed by the timeout has already consumed a full budget's
worth of the node's capacity on a client that has gone. `check-config` reports the rate the
configuration requires:

```
[PASS] overload    128 concurrent / 30s timeout = safe above 5 requests/s
[WARN] overload    max_concurrent_requests (3000) against a 1s request timeout admits more
                   work than the budget covers unless this node serves over 3000 requests/s…
```

The division is arithmetic with no assumption in it: the tool cannot know a node's service
rate, so it hands you the threshold and names both knobs that move it. Measure the node's own
rate from `jobs_completed` on [`/_admin/workers`](API_REFERENCE.md#worker-pool) and compare.

A default node sits three orders of magnitude clear of this, which is why the rule is normally
quiet. The way in is to raise `max_concurrent_requests` because the node is answering `503` —
which is the opposite of the fix. A `503` is the node refusing work it cannot finish in time,
at a cost of one comparison; widening admission does not create capacity, it converts cheap
refusals into expensive timeouts. If a node is shedding, either give it more capacity or
lengthen the budget its callers allow.

`[limits]` is what this node can hold; [`[security.limits]`](#rate-limiting-tool-calls-search-and-writes-securitylimits)
is what one caller may ask of it. Unknown keys inside `[limits]` are refused at startup rather
than ignored, so a typo cannot leave a limit silently at its default.

#### The spellings these replaced

These four were scattered across three tables and the file root before 0.3.3. A file still using
the old names starts, applies them, and says where each one went:

| Old | New |
|---|---|
| `max_record_size_mb` (file root) | `limits.max_record_size_mb` |
| `network.http.max_body_size_mb` | `limits.max_body_size_mb` |
| `search.total_memory_limit_mb` | `limits.total_memory_limit_mb` |
| `security.limits.max_response_bytes` | `limits.max_response_bytes` |

```
WARN cameodb.toml: max_record_size_mb has moved to limits.max_record_size_mb; applying 420.
     Move it before 0.4.0, when the old spelling goes.
```

`[limits]` wins where it names the setting itself. The old names are removed in 0.4.0.

### Node Configuration

```toml
[node]
# Human-readable label for this node (optional)
label = "cameo-node-01"

# Topology zone for rack/datacenter awareness (default: "default")
zone = "default"
```

### Search Configuration

```toml
[search]
# Minimum memory for each indexer thread in MB (default: 64)
indexer_memory_min_mb = 64

# Maximum memory for each indexer thread in MB (default: 512)
indexer_memory_max_mb = 512

# Threshold for memory pressure (percent, default: 80). Measured against
# limits.total_memory_limit_mb.
memory_pressure_threshold_percent = 80

# Maximum searches running concurrently on this node
# (default: 8, fallback to max(2, CPU/2) if set to 0)
# Searches beyond this limit queue instead of spawning more threads.
search_threads = 8

# Default search result limit (default: 10)
# Note: Explicit limit 0 in queries means count-only mode (returns total_hits without documents)
default_search_limit = 10
```

### The MCP endpoint (`[mcp]`)

```toml
[mcp]
enabled = true                     # false unmounts /mcp entirely
session_idle_timeout_secs = 1800   # how long a disconnected client may pause
max_sessions = 1024                # concurrent MCP clients held, idlest evicted at the cap
sse_keepalive_secs = 15            # how often an idle SSE stream is written to
legacy_sse_enabled = true          # the superseded /mcp/sse + /mcp/messages transport
max_in_flight_per_session = 32     # requests one session may hold in flight on /mcp/messages
```

The transport itself, as opposed to what a caller may spend on it — that is
[`[security.limits]`](#rate-limiting-tool-calls-search-and-writes-securitylimits), which meters a *key*.
Nothing here changes what a client is told; the protocol is the same either way.

#### `session_idle_timeout_secs` — how long a paused client keeps its session

The setting to reach for when an agent comes back from a pause to:

```
transport error: failed to send request: session terminated (404). need to re-initialize
```

A session id names server-side state, and a request presenting one the node no longer holds is
answered `404` — which the Streamable HTTP spec requires, and which is the only way a client
learns to start over. This is how long that state survives.

- **Idle means nothing arrived *and* no connection is open.** A client holding its listening
  `GET /mcp` stream open is never idle, however long it pauses. So this bounds the
  disconnected case: the gap a client may leave and still find its session.
- **Defaults to 1800** (thirty minutes). A session holds no conversation state — an id, an
  activity clock and the key that owns it — so holding one longer costs almost nothing, while
  losing one costs an agent a re-initialize mid-task. `max_sessions` is what bounds the memory.
- **Re-initializing is cheap and needs no cleanup.** An `initialize` carrying a stale session id
  is allowed through and answers with a new one, so a client recovers in one round trip
  without clearing anything first.
- **The sweep interval derives from it** — a tenth of the timeout, between 1 s and 60 s — so a
  session outlives its timeout by at most 10 %. It is not separately configurable: two knobs
  admit a config where the sweep is slower than the timeout, and then the sweep interval
  silently becomes the timeout.
- **`0` is refused.** It would expire every session before its next request could arrive.

#### `sse_keepalive_secs` — holding a stream open through the middle

How often an idle SSE stream is written to, on both transports. It has to be below the shortest
idle-read timeout between this node and its clients, and that is a property of someone else's
load balancer — 30 s on nginx by default, 60 s on an AWS ALB. It defaults to **15**.

It also has to be below `session_idle_timeout_secs`, or a stream written to less often than its
session expires cannot keep that session open; the node refuses to start rather than leaving
the contradiction to be discovered as a 404.

#### `max_in_flight_per_session` — the bound on a session's outstanding work

Only the legacy `/mcp/messages` transport needs this number: it answers `202` and runs the
request on a spawned task, so the request's own guards — the concurrency semaphore, the
timeout — end before the work begins. This is the bound on that work, counted per session
because the session is where the requests accumulate.

- **Defaults to 32** — well past what an agent's parallel tool calls reach for. The bound
  exists for the loop that does not stop, not for a client that batches a few calls.
- **Past the bound a request is refused `429`** with a `Retry-After` hint rather than queued.
  One more in-flight request on a saturated session is backlog the node is better off not
  taking on.
- **`0` is refused** — it would refuse every request on an otherwise valid session.

#### `legacy_sse_enabled` — the superseded transport

`/mcp/sse` and `/mcp/messages`, the 2024-11-05 HTTP+SSE transport. Current clients negotiate
Streamable HTTP on `/mcp` and never touch these. `false` **unmounts** them rather than refusing
them, the same way `admin_enabled` withholds the admin API: a route that is absent has nothing
to probe and no guard to misconfigure. On by default, because turning it off strands any client
still configured for it.

Worth knowing if you are considering it as a way to get longer sessions: it does not idle out
at all — a session with a live push channel is kept regardless of how long the client pauses —
but it ties that session to one TCP connection, and the session is forgotten the instant the
connection drops. A laptop sleeping or a proxy recycling the connection ends it with no grace
period, where a Streamable HTTP session survives the reconnect for
`session_idle_timeout_secs`. For a client that pauses and reconnects, that is the worse trade.

#### `enabled` — withholding MCP altogether

`false` unmounts `/mcp` and everything under it. For a node whose callers are all using the
HTTP API, this removes the surface rather than guarding it.

### Storage Configuration

```toml
[storage]
# Data directories for shard storage (default: ["./data/cameodb"])
data_paths = ["./data/cameodb"]

# Disk usage alert threshold in percent (default: 90)
disk_usage_threshold_percent = 90

# Enable fsync after WAL batches for durability (default: true).
# Turning this off can leave the search index ahead of the document store after a crash —
# see "What `wal_sync = false` actually costs" below.
wal_sync = true

# WAL segment size in MB (default: 64)
wal_segment_size_mb = 64

# Default batch size for bulk ingestion; also the base of the smart-commit threshold
# (default: 1000).
#
# This is an internal commit-cadence parameter, NOT the number of documents to put in a
# `_bulk` request. The two are easy to confuse and only the second one moves ingest
# throughput much — see "Write throughput: what actually moves it".
default_batch_size = 1000

# Initial number of shards per index (default: 4).
#
# Four is the measured optimum for write throughput on a single node, and eight is roughly
# a third slower — a bulk request fans out to every shard and waits for the slowest. Raise
# this for data volume and parallel recovery, not for ingest speed.
num_shards_init = 4

# Maximum shards allowed on this node (default: 8)
max_shards_per_node = 8

# Pin each shard's writer thread to a dedicated CPU core (default: true)
writer_core_affinity = true

# Route a shard's operations to one worker (default: false)
shard_affine_dispatch = false

# Pin each orchestrator worker to its own core (default: false)
worker_core_affinity = false
```

#### CPU affinity

Three flags, and only the first is on. All three place threads by a shard's **placement
ordinal** — a dense counter assigned when the shard is first seen, so shard ordinals are
`0, 1, 2, …` and map onto cores without collisions. Placement is not a hash of the shard
UUID; a hash leaves cores empty and doubles up on others.

The core budget is `min(get_core_ids().len(), available_parallelism())`, so a container with
a CPU quota pins within the quota rather than to cores the scheduler will never give it.

| Flag | Default | Effect |
|---|---|---|
| `writer_core_affinity` | `true` | Each shard's writer thread pinned to `core[ordinal]` |
| `shard_affine_dispatch` | `false` | Operations for a shard go to `worker[ordinal]` |
| `worker_core_affinity` | `false` | Each worker is an OS thread pinned to `core[worker_id]` |

The last two compose: `worker_core_affinity` requires the other two and is otherwise a
silent no-op.

**Turning the last two on makes things slower.** Measured with `cameodb-bench` on an
8-core aarch64 Linux node, 8 shards, three repeats per arm, medians:

| Arm | write ok/s @16 | write p90 | search ok/s @16 | search p99 |
|---|---|---|---|---|
| no affinity at all | 3 339 | 6.57ms | — | — |
| `writer_core_affinity` only (default) | 3 375 | 6.63ms | 5 055 | 8.3ms |
| `+ shard_affine_dispatch` | 2 797 | 10.15ms | 4 850 | 8.9ms |
| `+ worker_core_affinity` | 2 815 | 9.94ms | 4 320 | 16.5ms |

Writer pinning is free — neutral against no affinity at all, and it is what gives the other
two something to align to. Shard-affine dispatch costs 13–20% of write throughput at every
concurrency tested (8, 16 and 32) and roughly doubles write p90. Pinning the workers on top
adds nothing to writes and takes a further 15% off search, with p99 roughly doubled.

The cause is not the pinning, and it is worth being precise about what it *is*, because the
first answer turned out to be incomplete.

Enabling affine dispatch forces `worker_count` down from `min(shards × 2, cores × 2)` to
`cores`. At the time of that first run a worker awaited each operation inline, so halving the
pool halved the node's operation concurrency, and that looked like the whole explanation. It
was testable, and it was tested: a worker now carries eight operations at once, and the flags
were re-measured on the same rig (2026-08-10, concurrency 64, where the pool is actually the
constraint).

| Arm | write ok/s @64 | write p99 | search ok/s @16 | search p99 |
|---|---|---|---|---|
| `writer_core_affinity` only (default) | 7 118 | 61.53ms | 6 326 | 5.75ms |
| `+ shard_affine_dispatch` | 5 393 | 141.62ms | 6 298 | 5.78ms |
| `+ worker_core_affinity` | 6 735 | 92.78ms | 5 618 | 8.62ms |

Affine dispatch still costs 24% of write throughput, and the default arm's worst repeat beat
every affinity repeat. What remains is the constraint itself: a job for shard S may only run
on worker `S % worker_count`, so any instantaneous skew across shards leaves workers idle
while their neighbours queue — round-robin cannot be unlucky that way. Searches show the same
thing from the other side: affine dispatch is *neutral* for them, because searches dispatch
round-robin regardless, while confining the driving worker to one core costs 11% and half
again on p99 (searches are CPU-heavy and fan out across every shard).

So leave them off. Two independent measurements now say so, the second designed to overturn
the first.

### Sizing the read pool

`search_threads` bounds how many searches may run at once. It is the mechanism this design
uses instead of partitioning cores between readers and writers, and it is the one knob in
this area that measurably helps.

Keep it **at or below the number of cores the node actually has** — in a container, the
cores the container is given, not the host's. Reads share cores with the pinned shard
writers; allowing more concurrent searches than there are cores does not create throughput,
it moves queueing out of the pool and into the kernel scheduler, where the write path pays
for it too.

Measured on an 8-core node under simultaneous read and write load:

| `search_threads` | write ok/s | write p99 | search ok/s | search p99 |
|---|---|---|---|---|
| 16 (2x cores) | 1 776 | 27.0ms | 3 284 | 15.44ms |
| 8 (= cores, the default) | 1 895 | 22.3ms | 3 329 | 13.46ms |
| 6 | 1 837 | 23.1ms | 3 434 | 12.49ms |

Oversizing was worse on every axis, and it was also far less *predictable*: run-to-run write
throughput spread 1 477-1 853 at 16 against 1 837-1 842 at 6. `docker/cameodb-docker.toml`
shipped 16 and now ships the default 8.

### What mixed read/write load costs

Worth knowing before you size anything from the numbers above them: run searches and writes
at the same time and both drop by roughly half — writes 4 074 -> 1 776 ok/s, searches
5 880 -> 3 284, on a node with one and a half cores still idle. The cause is not core
contention (unpinning the writers changes nothing) but the cost of a durable commit: with
searches running, a WAL fsync competes with tantivy segment reads for IO and page cache, and
the per-commit cost roughly triples. Setting `wal_sync = false` recovers most of the write
throughput, at the durability cost that implies.

Note this is specific to *mixed* load. On write-only bulk ingest the same flag makes no
measurable difference — see "What does not help" under Write throughput — so the trade is worth
considering only when reads and writes are competing.

Plan capacity from a mixed measurement, not from a single-workload one.

Pinning is a no-op on macOS. It is reported as requested-and-refused rather than silently
ignored: `GET /_admin/workers` returns `pinning_requested`, `pinned_workers` and a
`target_core_id`/`core_id` pair per worker, so a config that asked for pinning and did not
get it is visible. Check that endpoint rather than assuming a flag took effect — this is
how the table above was produced.

### Cluster Configuration

```toml
[network.cluster]
# Enable distributed cluster mode (default: false)
enabled = true

# Bind address for cluster communication (default: "0.0.0.0")
bind_address = "0.0.0.0"

# Cluster communication port (default: 9580)
# Note the name: `port` under [network.cluster] is not a recognised key and is ignored.
cluster_port = 9580

# Cluster name for isolation (default: "cameodb-cluster")
cluster_name = "cameodb-cluster"

# Seed nodes for initial discovery
seed_nodes = ["10.0.1.5:9580", "10.0.1.6:9580"]

# Pre-shared key for the private peer-to-peer network (libp2p pnet), on top of the Noise
# encryption every connection already gets. Without it, anyone who can reach the cluster
# port can join the swarm. Required by the internal and external profiles.
# Exactly 64 hex characters: openssl rand -hex 32
psk_file = "/etc/cameodb/cluster.psk"     # or psk = "…" / CAMEODB_CLUSTER_PSK
```

Every node in a cluster must carry the same PSK; there is no rotation path short of stopping
every node. Cluster peers are trusted by this key, which is why API-key index scoping is
enforced at the HTTP/MCP ingress and is **not** a defense against a compromised peer.

## Security and Posture

Two independent settings: `[node] profile` declares how far this node can be reached and is
enforced as a set of assertions, and `[security]` decides who may call it.

### Security profiles (`[node] profile`)

The profile is not a preset that rewrites your config — it is a claim the rest of the config
has to be consistent with. A node whose settings contradict its profile **refuses to start**.

```toml
[node]
profile = "internal"   # local | internal | external
```

| | `local` | `internal` | `external` |
|---|---|---|---|
| Reachable from | this machine only | a trusted network | untrusted networks |
| Bind address | loopback only | any | any |
| TLS | optional | warned if off | **required** |
| CORS `"*"` | allowed (warned) | rejected | rejected |
| `/_admin/*` | allowed | allowed | **must be disabled** |
| Cluster PSK | warned | **required** | **required** |
| Authentication | optional | warned if off; fails `check-config` off a loopback bind | **required** |

Choose by who can reach the bind address, not by what the environment is for — a shared test
box is `internal`, not `local`. Omitting `profile` is valid only for a loopback bind, which
infers `local`; a node reachable from other hosts must state its posture.

Check a config without starting the node:

```bash
cameodb check-config -c /etc/cameodb/cameodb.toml
```

It prints one line per rule (`pass` / `warn` / `fail`) and exits non-zero on any failure, so
it works as a pre-flight step in a deploy script.

**One warning fails the check even though the node starts on it**: an `internal` node with a
non-loopback bind and no authentication. Every other warning is a risk you may reasonably accept
for the profile you declared; this one means every route, `/_admin/*` included, is open to anyone
who can reach the port — and `[security] enabled` defaults to `false`, so a config reaches that
state by *omitting* a setting rather than writing one. `Result: OK (3 warnings)` was the same
answer for a locked-down node and a wide-open one.

Whether the node *boots* stays permissive, deliberately: refusing would stop a deployment that
had been running this way over a value nobody wrote. Whether a config is *fit to deploy* is a
different question, and it is the one a deploy step asks. To accept the exposure, say so on the
command line rather than in the file:

```bash
cameodb check-config -c /etc/cameodb/cameodb.toml --allow-unauthenticated
```

The bind decides, not the label: `internal` over a loopback bind has overstated its reach rather
than exposed anything, and neither warns nor fails the check.

The packaged systemd unit runs the check as `ExecStartPre`, and passes `--allow-unauthenticated`
there for the same reason — without it the unit would convert this failure into the refusal to
boot the design rejected, and the config the packages install to `/etc/cameodb/cameodb.toml` is
exactly the one it fails on. Every other failure still stops the start before the port opens.
Drop the flag from the unit (`sudo systemctl edit cameodb`, override `ExecStartPre`) if you want
this host to refuse to run an unauthenticated node at all.

### Authentication (`[security]`)

Off by default. When enabled, every route except liveness requires
`Authorization: Bearer <key>`; see [API Reference](API_REFERENCE.md#capability-required-per-endpoint) for the
capability each endpoint needs.

```toml
[security]
# Enforce authentication on every route (default: false)
enabled = true

# Whether a write to an index that does not exist may create it (default: true).
# Set false where minting indexes must be an explicit decision — a write to an
# unknown index is then refused, and the index has to be created with
# PUT /api/{index}/_config, which needs the index-admin capability.
implicit_index_creation = true

[[security.api_keys]]
# The SHA-256 digest of the key — never the key itself
key_hash = "sha256:1db44a37dcf74ef70439a8887862839803d9686a41fe7c9d75d8fdfa0c72cdb1"
# admin (everything) | writer (read + write) | reader (read only)
role = "writer"
# Audit identity used in logs. Not a secret, not a credential.
label = "team-a"
# Optional: restrict this key to these indexes, for any role
allowed_indexes = ["docs", "wiki"]
# Optional: hold a reduced role on a named index. This key writes to "wiki" and
# reads "docs", with one key rather than two.
index_overrides = { docs = "reader" }
# Optional: the tenant this key acts for. Indexes it creates are owned by that
# tenant and count against [security.tenants.<name>].
tenant = "team-a"

[[security.api_keys]]
# Or keep the digest out of the config file entirely
key_hash_file = "/etc/cameodb/keys/agent"
role = "reader"
label = "agent"
```

The configs this repo ships — `cameodb.example.toml`, and the one the DEB and RPM install to
`/etc/cameodb/cameodb.toml` — bind `0.0.0.0` with authentication off, so a fresh install runs and
`check-config` tells you it is open. Enable authentication for anything holding data worth
keeping.

Only digests are stored, so a leaked config file contains nothing that can authenticate. A
key that is lost is replaced, not recovered.

Roles bundle four capabilities:

| Role | `read` | `write` | `index-admin` | `node-admin` |
|------|:---:|:---:|:---:|:---:|
| `admin` | ✅ | ✅ | ✅ | ✅ |
| `writer` | ✅ | ✅ | | |
| `reader` | ✅ | | | |

`allowed_indexes` applies on top of the role and holds everywhere a key can reach: naming
another index is refused, and `/_indexes`, the MCP catalog and the MCP resource list return
only the indexes that key may see.

**`index_overrides` reduces a key on one index.** The case it exists for is a `writer` that
must be read-only on a single sensitive index while keeping write everywhere else — one key
instead of two, and no fourth role:

```toml
role = "writer"
index_overrides = { audit = "reader" }
```

A write to `audit` is then refused `403` naming the effective role, while a write anywhere else
succeeds and a read of `audit` still works. The two refusals stay distinguishable on purpose:
*"this key is not permitted on index 'x'"* means the index is outside `allowed_indexes`, while
*"this key is restricted to role 'reader' on index 'x'"* means it may reach the index but not
that way. They send you to different parts of the stanza.

**It can only subtract.** An override naming a role that holds a capability the key's own role
does not is refused at startup, naming the capabilities it would have added — this is a
mechanism for reducing a key's reach, never for widening it, and a config that reads as if it
widens one is a mistake worth stopping rather than honouring. An override naming an index that
is not in `allowed_indexes` is refused too: the key cannot reach that index at all, so the
override would read as protection and provide none.

Enforced on REST and on `/mcp` alike. That is worth stating because the two check in different
places — the HTTP gate classifies a path, while an MCP tool is checked after its arguments are
decoded, since a single JSON-RPC path cannot be classified from the outside.

**`implicit_index_creation`** decides whether a write may mint an index. On by default: a write
to an index that does not exist samples the documents into a schema and creates it, which is
what makes semi-structured input work — and what lets any caller with `write` grow the node's
disk with arbitrarily many indexes. Set it `false` where creating indexes must be an explicit
decision: a write to an index with no schema anywhere is then refused `400` naming the remedy,
and the index has to be created with `PUT /api/{index}/_config` — the `index-admin`
capability's route. The setting decides what a write may cause, so it holds whoever sends the
write, key or no key.

The config is validated even when `enabled = false`, so a key stanza cannot be wrong in a way
you only discover on the day you turn authentication on. These all refuse to start:

- `enabled = true` with no keys — every request would be refused
- a `key_hash` with no `role`
- a hash that is not `sha256:<64 hex>`
- two entries with the same hash — one key cannot hold two roles
- `allowed_indexes = []`, which reads as "no restriction" but means "no index at all"
- an `index_overrides` entry that grants more than the key's own role
- an `index_overrides` entry naming an index outside `allowed_indexes`
- a blank `tenant`

### Tenant quotas (`[security.tenants]`)

A key may carry a `tenant`. The tenant owns every index that key creates, and
`[security.tenants.<name>]` bounds what those indexes may add up to on this node:

```toml
[[security.api_keys]]
key_hash_file = "/etc/cameodb/keys/acme"
role = "writer"
label = "acme-ingest"
tenant = "acme"

[security.tenants.acme]
max_indexes = 20                 # most indexes acme may own (0 = unlimited)
max_bytes = 53687091200          # 50 GiB across all of them (0 = unlimited)
```

Both ceilings default to `0`, unlimited, and a tenant with no `[security.tenants]` entry has no
ceiling at all — so an upgrade, or a key given a tenant before anyone has decided its limits,
changes nothing. Several keys may name one tenant; they share its quota.

**Ownership is decided once, when the index is created,** and stamped on its schema. The stamp
comes from the key, never the request body: a write or `PUT /_config` naming someone else's
tenant, or none, is overwritten with the caller's own. Later writes do not move it — a tenant
cannot shed usage by having another key write into their index — and re-declaring the schema
with an admin key keeps the owner. A key with no tenant creates indexes nobody owns, which no
quota counts.

**`max_indexes` is exact.** It is checked when an index is created, by either path — the
implicit creation a write performs and `PUT /api/{index}/_config` — and both happen on the
orchestrator's single mailbox, so two concurrent creations cannot both slip under the ceiling.
The count is of live indexes: clearing an index's documents (`DELETE /api/{index}`) leaves it,
and its slot, in place; dropping it with `?delete_schema=true` frees the slot.

**`max_bytes` can be overshot by up to ten seconds of ingest.** Usage is the listing's
`total_size_bytes` — tantivy plus the document store — summed over the tenant's indexes, and
it is measured off the write path and refreshed at most every ten seconds rather than
recomputed per write, since an exact figure is a directory walk per index on every request.
Writes are checked against the latest reading, so a tenant writing flat out can land one
refresh interval's worth of data past the ceiling before being refused. Set the ceiling with
that margin in mind. The first write after startup has no reading to check against and starts
one.

Bytes are charged to the index's **owner**, not the key writing: once a tenant is at their
ceiling, a write into one of their indexes is refused whoever sends it, including an admin key.
Raising the ceiling, or deleting data, is the remedy — the refusal names both.

Refusals answer **`403`** and name the tenant and the ceiling:

```json
{"error": "quota exceeded for tenant 'acme': tenant already owns 20 of 20 permitted indexes; delete one or ask the operator to raise max_indexes"}
```

Not a `400`, since nothing about the request is malformed, and not a `503`, since retrying the
same request will not help until room is made. Quotas are per node: in a cluster each node
holds its own share of a tenant's data and checks its own ceiling.

### Rate limiting tool calls, search and writes (`[security.limits]`)

```toml
[security.limits]
tool_calls_per_minute = 120       # 0 (the default) disables limiting entirely
tool_call_burst = 30              # spendable at once; 0 means one minute's worth
write_documents_per_minute = 5000 # the write surface, metered in documents; 0 disables it
write_burst = 20000               # spendable at once; 0 means one minute's worth
max_search_limit = 10000          # largest `limit` an MCP search may ask for
max_federated_indexes = 20        # most indexes one `search_across_indexes` may name
min_prefix_length = 2             # shortest `field:pre*` expanded; 0 expands any
expand_unqualified_prefix = false # let a bare `pre*` search the default fields
max_default_fields = 64           # most fields an unqualified term searches; 0 = all
```

**Two meters, and both are off until you set them.** The first pair meters *calls* — MCP tools
and HTTP search. The second meters *documents* through the write routes. They are separate
settings because they are separate units, and setting one does not set the other.

The companion size ceiling, `max_response_bytes`, lives in [`[limits]`](#size-and-memory-limits)
with the other message sizes it derives from.

Authentication answers *who*, and `allowed_indexes` answers *what*. Neither says anything
about **how often**, and the caller this matters for is not an attacker: it is a legitimate
`reader` key held by an agent that decides to call `search_across_indexes` in a loop. Every one of
those calls is authorized, and a search fans out across every shard, so the loop costs the
node far more than it costs the agent.

**The same bucket meters HTTP search.** `POST /api/{index}/search` and
`/api/{index}/search/stream` each spend one token from the caller's own bucket, refusing with
`429` and a retry delay, because the same work is reachable over either surface and a limit only
one of them honours is not a limit. `max_search_limit` is enforced on both for the same reason —
a deep `offset` asks for the same work as a large `limit` while looking like a request for ten
documents.

A token bucket rather than a fixed window. Agent traffic is bursty by nature — a plan, then
a flurry of lookups, then a pause — and a fixed window either refuses the flurry or is set
so loose it never bites. The bucket lets the burst through and meters the sustained rate.

Points worth knowing:

- **Metered per key**, so one noisy agent cannot refuse another. With `[security]` off there
  is no key to meter, and the caller's **address** is used instead — an IPv4 address, or an
  IPv6 /64, since a single host is routinely given a whole one. So one anonymous client cannot
  spend everybody's allowance on an unauthenticated node either. A key always outranks the
  address it connected from: two keys behind one NAT are two tenants.
- **The address map is bounded.** Past 4096 address groups per meter, buckets that have
  refilled to full are dropped (a full bucket and an absent one admit exactly the same next
  request, so nothing is given away), and if every tracked group is still spending, further
  addresses share one bucket. A limiter must not become the memory-exhaustion lever it exists
  to prevent.
- **Charged before the tool runs**, and before the per-tool capability check — so being rate
  limited never reveals which tools a key would otherwise be allowed to call.
- **The budget is shared across tools.** It bounds what a key costs the node, not how often
  it may call any one thing.
- **Charged by fan-out, not per call.** A federated search over five indexes spends five
  tokens, because it is five scatter-gathers dispatched by one request. Charging per call
  would make the budget a count of requests, and one request can name twenty indexes. A cost
  above the whole bucket empties it rather than being refused forever.
- **A refusal names a wait**: `Rate limit exceeded for tool 'x'. Retry after Ns.`, returned
  as an MCP tool error (`isError: true`) rather than a transport failure, because the
  request was well-formed and the tool simply did not run. Agents that are given a number
  back off correctly; ones told only "too many requests" usually retry immediately.

Off by default: an upgrade must not start refusing calls a deployment used to serve.

#### `write_documents_per_minute` — metering the write surface

```toml
[security.limits]
write_documents_per_minute = 5000
write_burst = 20000
```

Writes are the expensive direction and the one that grows the disk, so a node that meters
search and not writes has metered the wrong half. This is the ceiling on sustained ingest from
one caller; `network.http.max_concurrent_requests` bounds how many requests are in flight at
one instant and says nothing about how many arrive in an hour.

- **Counted in documents, not requests.** A `_bulk` body may carry thousands, and charging it
  the single token a search costs would leave the budget bounding nothing that matters. The
  charge is what the request asks the node to index or remove: **1** for `PUT
  /api/{index}/document` and `DELETE /api/{index}/document`, the array length for
  `POST /api/{index}/_bulk` and `POST /api/{index}/_bulk/delete`, and one charge per
  micro-batch (`search.stream_batch_size`) for `POST /api/{index}/document/stream`.
- **A delete costs what a write costs.** It is a document through the writer, a commit and a
  merge — cheaper to send, not cheaper to serve.
- **Charged before any of the work happens**, so a refused request leaves the index untouched
  and there is nothing for the caller to reconcile. The streaming route is the one exception
  and cannot be otherwise: how many documents the body holds is not known until it has been
  read. When a stream runs out mid-file it stops, and answers `429` with the same summary the
  size limit gives — `items_written`, `lines_received`, `batches` and `retry_after_secs` — so
  the caller knows exactly where to resume.
- **Set the burst above the sustained rate** more readily than on the read side. An import is
  one burst of many documents and then nothing; a burst equal to a minute's sustained rate
  refuses the second half of a file that a burst of twice that accepts whole.
- **Refusals carry `Retry-After`.** Every 429 from either meter does, and obeying it works —
  that is the bucket's contract, and it is pinned by a test.
- **It does not inherit `tool_calls_per_minute`.** An operator who metered tool calls chose a
  number for tool calls; charging their bulk imports against it on upgrade would be a release
  that broke ingest. Writes stay unmetered until this setting says otherwise.

`check-config` reports the state of both meters under the `rate` rule, and warns on a
non-`local` profile when either one is open.

#### `max_search_limit` — how much one search may ask for

The rate above is off by default; this one is not. There is no reading of "no ceiling" that is
a number, and without one the caller decides how many hits the node builds, merges and
serializes for a single request. It defaults to **10000**, which is where one request stops
being one request for this architecture: a search fans out across every shard of an index, and
each hit is a redb lookup, a merge entry and a serialized document.

- **Enforced on `POST /api/{index}/search` too**, though it lives with the MCP limits because
  that is the caller it was written for — an agent choosing its own limit. The HTTP surface can
  ask for exactly the same work, so a ceiling only one door honours is not a ceiling.
- **Advertised as well as enforced.** Both search tools render it as their `inputSchema`
  `maximum`, so a schema-driven client never constructs a call that will be refused — and a
  caller is never refused for exceeding a bound it was not shown.
- **Both doors are checked.** A `limit` argument above the ceiling is refused, and so is an
  inline `limit N` written into the query string, which reaches the search by a different route.
- **`search.default_search_limit` may not exceed it.** A search naming no limit is filled in
  with that default, so the pair would contradict each other; the node refuses to start rather
  than clamping, so the number an operator wrote is the number that runs.
- **`0` is refused**, not read as unlimited. A bound whose zero inverts its meaning is a trap;
  an operator who wants a high ceiling writes a high number.

#### `max_federated_indexes` — how wide one search may fan out

The other half of what one call may cost, and always in force for the same reasons
`max_search_limit` is. It defaults to **20**. Each name in a federated search is a full
scatter-gather across that index's shards, so the argument is a multiplier on everything the
call does — which is why the rate limiter charges a federated search per index named rather
than per call.

- **Advertised as well as enforced**, as the `maxItems` of `search_across_indexes`'s `indexes`
  array and in its description, so a schema-driven client never builds a call that will be
  refused.
- **It also caps what one call may be charged.** A list longer than this is refused when it is
  decoded, so charging for fan-out that cannot happen would let a malformed call empty a
  caller's budget.
- **A caller that wants the whole catalogue is asking a different question.** `list_indexes`
  answers it in one request.
- **`0` is refused**, not read as unlimited.

#### `min_prefix_length` — how short a prefix query may be

`field:pre*` runs as a range over the field's term dictionary: every term starting with `pre` is
visited and its postings read. Tantivy caps its own phrase prefix (`"big bad wo"*`) at 50 terms
per segment, but puts no cap on a range, so the cost of a prefix is the number of distinct terms
it covers — a property of the field, not of the query. It defaults to **2**.

Measured on one shard of 10M documents, one distinct term per document (ROADMAP M8):

| Prefix length | Hash field (16 hex chars) | | Base36 id (10 chars) | | Text (20 words/doc) | |
|---|---:|---:|---:|---:|---:|---:|
| | terms | ms | terms | ms | terms | ms |
| 1 | 625,427 | **164.7** | 277,783 | **57.8** | 44,404 | 31.4 |
| 2 | 38,929 | 10.6 | 7,729 | 1.8 | 1,738 | 1.6 |
| 3 | 2,458 | 0.7 | 216 | 0.1 | 70 | 0.1 |
| 4 | 146 | 0.1 | 4 | 0.06 | 0 | 0.06 |

Each character divides the cost by the size of the field's alphabet, and the whole table scales
with the shard — the same measurement at 1M documents came in at roughly a tenth of these figures
(8–13×). Identifier
fields (hashes, UUIDs, URLs, emails) are where it bites: one character on a hash field holds a read
thread for 165 ms per shard, and a search fans out to every shard. On natural text a one-character
prefix costs about the same as searching its most common word, because the vocabulary is small.

- **A shorter prefix is not refused.** It is matched as the literal term, like any prefix the
  rewrite cannot expand, and reported in `_discarded_clauses`, so the rest of the query still runs.
  The MCP search tools refuse a search with a reported clause, as they do for every such note.
- **`0` expands every prefix**, and is accepted: unlike `max_search_limit`, nothing about "no
  floor" is incoherent, only expensive. Raise it to `3` where a shard's identifier fields hold
  hundreds of millions of values: at 160M hex values per shard, two characters costs what one
  does at 10M.
- **Characters, not bytes**, counted after the field's analyzer has run.

#### `max_default_fields` — how many fields an unqualified term searches

A term with no field in front of it — `alpha`, `"a phrase"` — is searched in every default field,
one clause per field, and every indexed text, string and JSON field is a default field. So the
cost of an unqualified query follows the width of the schema, which is whatever the tenant's
first documents or `PUT /_config` made it. It defaults to **64**; `0` searches every field.

Measured on one shard with the fields sharing a vocabulary — the worst case, as with log records
or a wide CSV of similar columns (ROADMAP M8):

| Default fields | 200k docs, 1 term | 200k docs, 5 terms | 1M docs, 1 term | 1M docs, 5 terms |
|---:|---:|---:|---:|---:|
| 5 | 0.08 ms | 0.8 ms | 0.44 ms | 3.3 ms |
| 32 | 1.5 ms | 24 ms | 6.0 ms | 120 ms |
| 64 | 5.7 ms | 97 ms | 26 ms | 513 ms |
| 100 | 12.6 ms | 232 ms | | |
| 200 | 54 ms | 1,067 ms | | |
| 400 | 229 ms | 5,211 ms | | |

The cost grows faster than the field count — about four times per doubling — and with the
shard. Where each field holds its own vocabulary the other fields are dictionary misses and even
400 cost 0.25 ms; a query naming its field is 0.01 ms at every width.

- **Past the cap a bare term is narrowed, not refused.** It searches the index's declared
  `default_fields` in their order, or else its fields **by name**, up to the cap. By name because
  every shard has to pick the same fields, and each shard's own field order is an accident of how
  its index was built. The index reports what a bare term searches under `searched_by_default`,
  with `default_fields_truncated: true` when the cap cut it; each field carries `default_search`.
  A search it narrowed carries `_narrowed_default_fields` naming the fields reached, and the MCP
  tools add a `_warning` — advisory, as for an approximate sort, never a refusal.
- **Choose the fields with `default_fields`** on the index, in `PUT /api/{index}/_config` or
  `PATCH /api/{index}/_schema` — no reindex; it applies to the next search. See
  [API Reference](API_REFERENCE.md#change-field-indexing-flags).
- **Query-time only.** No index is rebuilt and nothing on disk changes, so the cap applies to
  existing indexes the moment the node starts. Set it the same on every node of a cluster: shards
  on nodes with different caps would search different fields for one query. A declared list
  travels with the schema and avoids the question.
- **The number of terms is not bounded here.** A query of K words is still up to K × 64 clauses.
- **It also bounds `expand_unqualified_prefix`**, which expands a bare prefix across these same
  fields.

#### `expand_unqualified_prefix` — whether `pre*` needs a field

Off by default. Tantivy's grammar has no unqualified prefix: a bare `pre*` has its `*` dropped
and matches the term `pre`, and the response reports that in `_discarded_clauses`. Turn this on
and the node rewrites it into one prefix range per text field an unqualified term searches, OR'd
together — `qui*` becomes `(body:[qui TO quj} OR title:[qui TO quj})` — so it finds what it
looks like it should.

- **It costs one prefix per default field.** Every indexed text field is a default field, so on
  a wide index one bare prefix is that many ranges. `min_prefix_length` applies to each, which
  is what keeps any single one of them cheap; `max_default_fields` bounds how many there are.
- **Only unqualified prefixes.** `title:pre*` is rewritten as before; a prefix inside a field
  group — `title:(pre*)` — belongs to that field and is never sent to the others. It is still
  reported rather than expanded.
- **Text fields only.** A JSON field supports a range only as a fast column, so it is left out of
  the expansion.
- **Declined rather than guessed.** The node rewrites only when its reading of where the bare
  prefixes are agrees with tantivy's own parse of the query. If they ever disagree — an escape or
  nesting read differently — the query is left as written and each prefix is reported, rather
  than rewritten into something that matches differently without saying so.

#### `limits.max_response_bytes` — how large one response may be

Set under [`[limits]`](#size-and-memory-limits) rather than here, because it is a message size
and derives from one; it is documented alongside `max_search_limit` because the two bound the
same request from different directions.

`max_search_limit` bounds how many hits come back; nothing bounds how large they are. A search
well inside it can still be more bytes than this node carries in one message.

**Leave it unset.** It then derives from `limits.max_record_size_mb`, the single source of truth for
message size — the same place the HTTP body limit, the inter-node message size and the request
timeout come from. What a node accepts in one message is what it will send in one: with the
default 64 MB record size that is **128 MB**, and raising the record size for large documents
raises this with it rather than leaving searches over those documents trimmed by a bound nobody
moved. See [the derived limits table](#configuration-reference).

Past the ceiling the hits that do not fit are left out and the response says so: `_truncated:
true`, `_omitted_hits: N`, and a `_warning` naming the byte figure it hit and telling the caller
to narrow the query. The hits returned are the front of the same order, so the answer stays
usable as far as it goes — the point of the flag is that an agent which thinks it read
everything will report it as everything.

- Set it explicitly only to go **below** the message size, which is worth doing when the
  callers are agents whose context is smaller than what the node can send. To go above it,
  raise `limits.max_record_size_mb` and every limit moves together.
- `total_hits` still counts what matched. `hits_returned` counts what came back.
- One hit always survives, even one larger than the whole allowance: a response trimmed to
  nothing is indistinguishable from a query that matched nothing.
- Applies to the MCP search tools, like `max_search_limit`, and for the same reason.
- An explicit `0` is refused — it would report every response as truncated rather than
  refusing any.
- **It bounds one result, not the packet.** The message adds a JSON-RPC envelope around it, so
  it runs slightly over the ceiling. Bounding the answer is the more useful of the two, and the
  difference is a small constant rather than a surprise.

### The audit trail (`[security.audit]`)

```toml
[security.audit]
enabled = false                            # off by default
file = "/var/log/cameodb/audit.jsonl"      # optional; without it the trail is memory-only
buffer_capacity = 2048                     # records kept for /_admin/audit
queue_capacity = 8192                      # hand-off depth to the writer thread
max_file_bytes = 104857600                 # rotate past 100 MiB
max_files = 5                              # audit.jsonl.1 … .5, oldest discarded
record_query_text = false                  # see the warning below
rollup_secs = 10                           # how often counted totals are flushed
```

Authentication decides *who*, `allowed_indexes` decides *what*, `[security.limits]` decides
*how often*. None of them keeps a record. Without this section a node can tell you it turned
somebody away — refusals have always been logged — but not who legitimately read which index,
which is the question an incident actually asks.

**Detail for reads, totals for writes.** A knowledge base ingests far more than it retrieves,
so a record per write would bury the handful of reads worth looking at. Writes are folded
into a per-key, per-index count flushed every `rollup_secs`; reads keep a line each:

```json
{"ts":"2026-08-09T14:22:31.118Z","event":"http","outcome":"allowed","key_id":"k_7f3a",
 "label":"analyst","role":"reader","peer":"10.0.4.19","method":"POST",
 "path":"/api/customers/search","index":"customers","status":200}
{"ts":"2026-08-09T14:22:40.000Z","event":"write_stats","key_id":"k_1c8e","label":"ingest",
 "role":"writer","index":"docs","ops":48213,"errors":2,"window_start":"2026-08-09T14:22:30.000Z"}
```

| `event` | What it is | Detail or total |
|---|---|---|
| `http` | One request through the API | Detail |
| `mcp_tool` | One MCP tool call — which tool, which index | Detail |
| `write_stats` | Writes by one key to one index in a window | Total |
| `public_stats` | Health checks, which are not an access to anyone's data | Total |
| `auth_denied_stats` | Refusals of callers who presented no key | Total |
| `gap` | Records lost to a full queue, and how many | — |

Refusals of a **valid** key always keep their own line: that is bounded by the credentials in
circulation, and "this key reached for an index it does not hold" is the shape of both a
misconfiguration and a compromised credential. Refusals of an *unidentified* caller are
counted instead, because their volume is chosen by whoever can reach the port — listing them
individually would hand a stranger a way to fill the disk.

Points worth knowing:

- **Never on the request path.** Emitting is a timestamp and a non-blocking hand-off; the
  file writing, rotation and serialization happen on a dedicated thread. A slow disk cannot
  become a slow node.
- **Loss is admitted.** If the queue fills, the record is dropped, counted, and a `gap`
  record naming the number lost is written. `/_admin/audit` reports the running total, so an
  operator reading the trail can see the window is incomplete.
- **No key is ever written.** The `key_id` is a digest prefix minted for exactly this: it
  ties a line to a credential without the credential appearing. There is no code path that
  can put a token in a record, and a test asserts it for accepted *and* rejected tokens.
- **`peer` is the socket address**, not `X-Forwarded-For`. That header is written by the
  client, so trusting it would let a caller choose what the trail says about them. Behind a
  proxy this records the proxy, which is at least true.
- **Reading the trail takes `node-admin`** and is itself recorded, refusals included.

> **`record_query_text` keeps data, not just metadata.** A search for a person's name records
> that name, so a trail turned on to answer "who read the customer index" starts accumulating
> the customers who were looked up. Off by default; when you turn it on, treat the audit file
> as sensitive as the index it describes. It covers `POST /api/{index}/search` and MCP tool
> calls.

### Reading it back

```bash
curl -H "Authorization: Bearer $ADMIN_KEY" 'http://localhost:9480/_admin/audit?limit=200'
```

Answers `{enabled, dropped, count, records}` with the newest first, capped at 1000. The
endpoint needs `[network.http] admin_enabled = true`. It reads the in-memory ring, so it
works without a file sink — and dies with the process, which is what the file is for.

```bash
# Who read the payroll index?
jq -c 'select(.index=="payroll" and .outcome=="allowed")' /var/log/cameodb/audit.jsonl

# What did each key ingest?
jq -s 'map(select(.event=="write_stats")) | group_by(.label)
       | map({key: .[0].label, ops: map(.ops) | add})' /var/log/cameodb/audit.jsonl
```

Every record is also emitted as a `tracing` event on the target `cameodb::audit`, so a
deployment already shipping logs to a collector gets the trail without configuring a second
path — and can route or silence it independently of everything else the node says.

### Minting keys

```bash
# Print the key to stdout and the config stanza to stderr
cameodb keygen --role writer --label team-a --allowed-indexes docs,wiki

# Or write both files directly (created 0600, never overwritten)
cameodb keygen --role reader --label agent \
  --key-out ~/.cameodb/agent.key \          # for the client's --api-key-file
  --hash-out /etc/cameodb/keys/agent         # for key_hash_file above
```

Keys are `cameo_v1_` followed by 43 characters — 256 bits from the OS. Anything else is
rejected before it is hashed, so a passphrase or a UUID can never authenticate regardless of
what digest is configured.

If the node runs as its own user, `chown` any `key_hash_file` to it: the file is read at
startup. CameoDB warns if a `key_hash_file` is writable by group or others — a digest is not
secret, but a writable one lets anyone mint themselves a role.

### Rotation

Keys are read once at startup; there is no hot reload. Rotating is therefore:

1. `cameodb keygen` a replacement and add it as a second `[[security.api_keys]]` entry
2. Restart, so the node accepts both
3. Move clients across
4. Remove the old entry and restart again

## Environment Variables

CameoDB supports environment variable overrides for all major settings. Prefix variable names with `CAMEODB_`.

### Node Configuration
- `CAMEODB_NODE_LABEL`: Node label
- `CAMEODB_NODE_ZONE`: Topology zone

### Network Configuration
- `CAMEODB_HTTP_PORT`: HTTP port
- `CAMEODB_HTTP_BIND_ADDRESS`: HTTP bind address
- `CAMEODB_CLUSTER_ENABLED`: Enable/disable cluster (`true`/`false`)
- `CAMEODB_CLUSTER_PORT`: Cluster communication port
- `CAMEODB_CLUSTER_BIND_ADDRESS`: Cluster bind address
- `CAMEODB_CLUSTER_NAME`: Cluster name
- `CAMEODB_SEED_NODES`: Comma-separated list of seed nodes
- `CAMEODB_CLUSTER_PSK`, `CAMEODB_CLUSTER_PSK_FILE`: Cluster pre-shared key, or a file holding it

### Security Configuration
- `CAMEODB_SECURITY_ENABLED`: Enforce authentication (`true`/`false`)
- `CAMEODB_API_KEY_HASH`: A single key digest, for a node configured entirely from the environment
- `CAMEODB_API_KEY_ROLE`: The role for `CAMEODB_API_KEY_HASH` (required with it)
- `CAMEODB_PROFILE`: Security profile (`local`/`internal`/`external`)

There is deliberately no `CAMEODB_API_KEY` on the server: a node never needs a key in the
clear, only digests. That variable is read by the **client**.

### Storage Configuration
- `CAMEODB_DATA_PATHS`: Colon-separated list of data paths

### Limits
- `CAMEODB_MAX_RECORD_SIZE_MB`: Largest single record; the other message sizes derive from it
- `CAMEODB_MAX_BODY_SIZE_MB`: HTTP body ceiling, overriding the derived one
- `CAMEODB_TOTAL_MEMORY_LIMIT_MB`: The node's memory budget

### Search Configuration
- `CAMEODB_INDEXER_MEMORY_MIN_MB`: Minimum indexer memory
- `CAMEODB_INDEXER_MEMORY_MAX_MB`: Maximum indexer memory
- `CAMEODB_MEMORY_PRESSURE_THRESHOLD_PERCENT`: Memory pressure threshold
- `CAMEODB_DEFAULT_SEARCH_LIMIT`: Default search result limit

## Multi-Disk Setup

For high-throughput deployments with multiple storage devices:

### Generate Multi-Disk Configuration

```bash
./scripts/setup/config-manager.sh multi-disk
```

### Manual Configuration

```toml
[node]
label = "cameodb-multi-disk"

[network.http]
port = 9480
# Loopback, so this example needs no `profile` — it is about `data_paths` below.
# For a node reachable from other hosts, see the production example at the end of
# this document: a non-loopback bind needs a declared profile, and the posture
# that goes with it.
bind_address = "127.0.0.1"

[storage]
data_paths = [
  "/mnt/nvme1/cameodb",
  "/mnt/nvme2/cameodb", 
  "/mnt/ssd1/cameodb",
  "/mnt/ssd2/cameodb"
]
disk_usage_threshold_percent = 85
wal_segment_size_mb = 128
max_shards_per_node = 50

[limits]
total_memory_limit_mb = 4096

[search]
indexer_memory_max_mb = 512
# Sized for a host with at least this many cores — see "Sizing the read pool" below.
# Exceeding the core count costs the write path more than it gains the read path.
search_threads = 16
```

### Benefits

- **Parallel I/O**: Distribute shards across multiple disks
- **Fault Tolerance**: Continue operation if one disk fails
- **Performance**: Increased throughput and reduced latency

## Performance Tuning

### High-Performance Configuration

```bash
./scripts/setup/config-manager.sh performance
```

### Key Performance Parameters

#### Memory Configuration

```toml
[limits]
total_memory_limit_mb = 8192

[search]
# Higher memory allocation for better write performance
indexer_memory_min_mb = 64
indexer_memory_max_mb = 1024

# Aggressive memory usage
memory_pressure_threshold_percent = 90
# Assumes >= 16 cores. This is a ceiling on concurrent searches, not a throughput dial:
# past the core count it buys queueing in the kernel instead of queueing in the pool, and
# the write path pays for it. See "Sizing the read pool".
search_threads = 16
default_batch_size = 2000
```

#### Storage Optimization

```toml
[storage]
# Disable fsync for maximum write speed. Only for data you can reload from elsewhere:
# a crash can leave the search index ahead of the document store.
#
# Measure before accepting this trade. On write-only bulk ingest it made no measurable
# difference at saturation (370,932 docs/s with fsync on against 365,382 with it off), so
# the durability is usually free. It earns its keep only under mixed read/write load, where
# a durable commit competes with segment reads for IO.
wal_sync = false

# Large WAL segments reduce overhead
wal_segment_size_mb = 256

# Use more disk space
disk_usage_threshold_percent = 95
```

#### Threading Configuration

```toml
[search]
# Maximize CPU utilization
search_threads = 32
```

### Write throughput: what actually moves it

Measured 2026-09-25 on a 15-core M5 Pro, release build, single node, bulk ingest, harness
co-located. Single runs — read the shapes, not the third digit — but the shapes held across
every sweep, and the reversals below were each reproduced.

**The short version: send few large batches, not many small ones.** That one change is worth
more than every setting in this section combined.

| What you change | Effect on ingest |
|---|---|
| Batch size 500 → 20,000 | **~11x** |
| Concurrency, up to the knee | ~2x |
| Concurrency, past the knee | nothing, and latency grows |
| `num_shards_init` 1 → 4 | ~1.4x |
| `num_shards_init` 4 → 8 | **−36%** |
| `wal_sync = false` on bulk ingest | nothing measurable |
| `indexer_num_threads` 1 → 4 | nothing measurable |

#### Batch size and concurrency are one dial, not two

What the node responds to is **documents in flight** — batch size multiplied by concurrent
requests. The plateau is around 400,000 of them, at roughly 320,000–370,000 documents/second.
Past that, offering more work buys latency and nothing else.

| Concurrency | Batch | Docs in flight | Docs/sec | p50 |
|---|---|---|---|---|
| 16 | 500 | 8k | 25,000 | 324ms |
| 64 | 2,000 | 128k | 158,000 | 725ms |
| 32 | 10,000 | 320k | 265,000 | 1.0s |
| **8** | **50,000** | 400k | **321,000** | **1.1s** |
| **32** | **20,000** | 640k | **324,000** | 1.5s |
| 32 | 50,000 | 1.6M | 234,000 | 4.9s |

Two rows are worth dwelling on. **The last one is slower than the ones above it** and has five
times the latency — over-feeding the node costs throughput, it does not merely fail to add any.
And the `8 x 50,000` row reaches the plateau with **a third of the latency** of `32 x 20,000`.
Given a choice, prefer fewer connections sending larger batches: same ceiling, far better tail.

So the knee moves with batch size, and a single "recommended concurrency" would be wrong at
least half the time:

- at batch 500, more concurrency helps up to about 64
- at batch 20,000, concurrency 32 already beats 64
- at batch 50,000, concurrency 8 beats both

**If you tune one thing, raise the batch size and leave concurrency low.**

#### Shard count: the default of 4 is the right default

Measured at a load point that saturates, not at an idle one — this matters, see the warning
below.

| `num_shards_init` | Docs/sec |
|---|---|
| 1 | 122,000 |
| 2 | 163,000 |
| **4 (default)** | **172,000** |
| 8 | 110,000 |

Sharding pays to about four and then reverses. A bulk request fans out to *every* shard and
waits for the slowest, so each added shard buys parallelism and pays a tail-latency tax on every
request; past four the tax wins. Raise `num_shards_init` for data volume and parallel recovery
if you need to — not for write throughput.

#### What does not help

- **`wal_sync = false` on bulk ingest.** Measured at saturation: 370,932 docs/sec with fsync on
  against 365,382 with it off, and a repeat pair of 357,220 and 358,223 — no difference outside
  run-to-run noise. **You do not need to trade durability for bulk ingest speed.** (This is
  specific to write-only bulk load. Under *mixed* read and write load a durable commit is
  genuinely expensive — see "What mixed read/write load costs" above, where turning it off does
  recover throughput.)
- **`indexer_num_threads`.** 1, 2 and 4 measured within noise of each other on bulk ingest.
- **More cores.** See below.

#### Do not size this node by CPU

**CPU peaked at 3.9 of 15 cores — 26% — at the 324,000 docs/sec plateau**, and sat at 0.6–0.8
cores for every configuration below it. The write path is bound by latency and serialization,
not by processor time: one writer thread per shard, a reply round-trip per slice, coalescing in
between.

The practical consequences:

- A node that looks idle in `top` while ingest feels slow is **not** under-provisioned. Look at
  batch size first.
- Adding cores will not raise ingest throughput. Adding *offered load*, in the shape above, will.
- Capacity-plan ingest from documents/second and documents in flight, not from CPU headroom.

#### Measure before you tune

```bash
# The knee, found in one pass
cameodb-bench --url http://localhost:9480 --index yours \
  --mode bulk --batch-size 20000 --concurrency 32 --duration 30
```

Then read `GET /_admin/workers`. `in_flight` against `in_flight_capacity` says whether the pool
is the bottleneck, `jobs_completed` over the run is this node's real service rate, and
`jobs_dropped` must be `0` — see
[Worker Pool](API_REFERENCE.md#worker-pool) for what each counter means.

> **A warning that cost us a wrong answer.** The shard table above was first measured at
> concurrency 16 / batch 500 and appeared to show that *one shard beats four*. That was an
> artefact of offering too little load: at a point that actually saturates, four beats one by
> 41%. **A scaling comparison taken below the knee measures your load generator, not the node.**
> Find the knee first, then compare configurations at it.

### Performance vs Durability Trade-offs

| Setting | Performance | Durability | Note |
|---------|-------------|------------|------|
| `wal_sync = false` | ⬆️ High | ⬇️ Low | Recent writes can be lost **and** the search index can end up ahead of the document store — see below |
| `indexer_memory_max_mb = 1024` | ⬆️ High | ➡️ Same | Uses more RAM |
| `memory_pressure_threshold_percent = 90` | ⬆️ High | ➡️ Same | Higher memory usage |

#### What `wal_sync = false` actually costs

More than the row above can say in a cell, because it does not just lose data — it can make the
two engines disagree.

Crash recovery rests on redb being the authority: the Tantivy index is derived from it, so
startup replays whatever redb has that Tantivy does not. `wal_sync = false` sets redb's
durability to `None`, which means a committed transaction may never reach disk at all. A
process kill can therefore lose WAL and document rows that Tantivy had *already committed to
its own segments* — leaving the derived index ahead of the source of truth.

Recovery cannot repair that. It only ever replays forward, so a search will return hits whose
documents `GET /api/{index}/{id}` reports as missing, and it will keep doing so until those
documents are rewritten or the index is rebuilt.

Use it for bulk-loading data you can replay from an external source, and turn it back on before
the node holds anything you cannot regenerate.

## Production Deployment

### Recommended Production Configuration

```toml
[node]
label = "cameo-prod-01"
zone = "us-east-1a"
# Required for a non-loopback bind. "external" is the strictest: TLS, authentication and
# admin endpoints off are all enforced, and the node refuses to start without them.
profile = "external"

[network.http]
port = 9480
bind_address = "0.0.0.0"
request_timeout_secs = 60
cors_allowed_origins = []  # "*" is rejected by internal and external
admin_enabled = false      # required off by the external profile

[network.http.tls]
enabled = true
cert_file = "/etc/cameodb/certs/cert.pem"
key_file = "/etc/cameodb/certs/key.pem"

[security]
enabled = true

[[security.api_keys]]
key_hash_file = "/etc/cameodb/keys/ops"   # cameodb keygen --role admin --hash-out …
role = "admin"
label = "ops"

[[security.api_keys]]
key_hash_file = "/etc/cameodb/keys/ingest"
role = "writer"
label = "ingest"

[storage]
data_paths = ["/data/cameodb"]  # Dedicated data volume
disk_usage_threshold_percent = 85
wal_sync = true  # Enable for durability
wal_segment_size_mb = 128
default_batch_size = 1000
max_shards_per_node = 20
writer_core_affinity = true
shard_affine_dispatch = false   # measured a regression; see "CPU affinity" above
worker_core_affinity = false    # ditto

[limits]
max_body_size_mb = 50
total_memory_limit_mb = 2048

[search]
indexer_memory_min_mb = 64
indexer_memory_max_mb = 512
memory_pressure_threshold_percent = 80
search_threads = 8
default_search_limit = 10

[network.cluster]
enabled = true
cluster_port = 9580
cluster_name = "cameodb-production"
seed_nodes = ["10.0.1.5:9580", "10.0.1.6:9580"]
# Required by internal and external whenever the cluster is enabled: without it, anyone who
# can reach the cluster port can join the swarm. Generate with `openssl rand -hex 32`.
psk_file = "/etc/cameodb/cluster.psk"
```

Verify it before the service starts — this config refuses to boot if any of the above is
missing or inconsistent:

```bash
cameodb check-config -c /etc/cameodb/cameodb.toml
```

### System Requirements

| Component | Minimum | Recommended | High-Performance |
|-----------|---------|-------------|------------------|
| **CPU** | 2 cores | 4-8 cores | 16+ cores |
| **RAM** | 2GB | 8GB | 32GB+ |
| **Storage** | 10GB SSD | 100GB NVMe | Multiple NVMe drives |
| **Network** | 100Mbps | 1Gbps | 10Gbps+ |

### Monitoring

Monitor these key metrics:

- **Memory Usage**: Stay below `memory_pressure_threshold_percent`
- **Disk Usage**: Watch `disk_usage_threshold_percent`
- **Search Latency**: Monitor query response times
- **Write Throughput**: Track documents/second ingestion

## Troubleshooting

### Common Issues

#### 1. Memory Errors

**Error**: "Memory pressure threshold exceeded"

**Solution**:
```toml
[limits]
total_memory_limit_mb = 4096  # Increase limit

[search]
memory_pressure_threshold_percent = 90  # Allow higher usage
```

#### 2. Disk Space Issues

**Error**: "Disk usage threshold exceeded"

**Solution**:
```toml
[storage]
disk_usage_threshold_percent = 95  # Allow more disk usage
# Or add more data paths
data_paths = ["/data1/cameodb", "/data2/cameodb"]
```

#### 3. Configuration Validation

```bash
# Validate configuration syntax
./scripts/setup/config-manager.sh validate cameodb.toml

# Test configuration loading
cargo run --release --bin cameodb  # Should start without errors
```

#### 4. Performance Issues

**Slow Writes**:
- Increase `indexer_memory_max_mb`
- Disable `wal_sync` (reduces durability)
- Use faster storage (NVMe)

**Slow Searches**:
- Increase `search_threads` if queries are queueing (concurrency-bound)
- Add more RAM for caching
- Check `warm_shards` vs `shard_count` in `/_indexes` — if it is short, first queries are
  still paying cold-start costs while background warmup catches up

### Debug Configuration Loading

Set environment variable to see configuration details:

```bash
RUST_LOG=debug cargo run --release --bin cameodb
```

## Configuration Templates

### Development

```bash
./scripts/setup/config-manager.sh minimal
```

### Production

```bash
./scripts/setup/config-manager.sh generate
# Edit data_paths, memory limits, and the [security] section — a non-loopback bind needs a
# declared profile, and internal/external both require decisions about TLS and keys
```

### High-Performance

```bash
./scripts/setup/config-manager.sh performance
# Review durability trade-offs
```

### Multi-Disk

```bash
./scripts/setup/config-manager.sh multi-disk
# Customize mount points
```

---

For more configuration examples and advanced scenarios, see the [scripts/setup/config-manager.sh](../scripts/setup/config-manager.sh) tool.
