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

[network.http]
bind_address = "0.0.0.0"
port = 9480

[storage]
data_paths = ["./data/cameodb"]

[search]
indexer_memory_min_mb = 64
indexer_memory_max_mb = 512
total_memory_limit_mb = 2048
default_search_limit = 10
```

### 3. Start CameoDB

```bash
cargo run --release --bin cameodb
```

## Configuration Sources

CameoDB loads configuration from multiple sources with the following precedence (highest to lowest):

1. **Environment Variables** (highest priority)
2. **Configuration Files**
3. **Default Values** (lowest priority)

### Configuration File Locations

CameoDB searches for configuration files in this order:

1. `cameodb.toml` (current directory)
2. `cameodb.yaml` (current directory)
3. `config/cameodb.toml`
4. `config/cameodb.yaml`
5. `/etc/cameodb/config.toml`

Both TOML and YAML formats are supported.

## Configuration Reference

### Network Configuration

```toml
[network.http]
# Port for HTTP (default: 9480)
port = 9480

# Bind address for HTTP (default: "0.0.0.0")
bind_address = "0.0.0.0"

# Request timeout in seconds (default: 30)
request_timeout_secs = 30

# Maximum request body size in MB (default: 200)
max_body_size_mb = 200

# CORS allowed origins (default: ["*"])
cors_allowed_origins = ["*"]
```

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

# Total memory limit for all search operations in MB (default: 2048)
total_memory_limit_mb = 2048

# Threshold for memory pressure (percent, default: 80)
memory_pressure_threshold_percent = 80

# Maximum searches running concurrently on this node
# (default: 8, fallback to max(2, CPU/2) if set to 0)
# Searches beyond this limit queue instead of spawning more threads.
search_threads = 8

# Default search result limit (default: 10)
# Note: Explicit limit 0 in queries means count-only mode (returns total_hits without documents)
default_search_limit = 10
```

### Storage Configuration

```toml
[storage]
# Data directories for shard storage (default: ["./data/cameodb"])
data_paths = ["./data/cameodb"]

# Disk usage alert threshold in percent (default: 90)
disk_usage_threshold_percent = 90

# Enable fsync after WAL batches for durability (default: true)
wal_sync = true

# WAL segment size in MB (default: 64)
wal_segment_size_mb = 64

# Default batch size for bulk ingestion (default: 5000)
default_batch_size = 5000

# Initial number of shards per index (default: 4)
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
| Authentication | optional | warned if off | **required** |

Choose by who can reach the bind address, not by what the environment is for — a shared test
box is `internal`, not `local`. Omitting `profile` is valid only for a loopback bind, which
infers `local`; a node reachable from other hosts must state its posture.

Check a config without starting the node:

```bash
cameodb check-config -c /etc/cameodb/cameodb.toml
```

It prints one line per rule (`pass` / `warn` / `fail`) and exits non-zero on any failure, so
it works as a pre-flight step in a deploy script.

### Authentication (`[security]`)

Off by default. When enabled, every route except liveness requires
`Authorization: Bearer <key>`; see [API Reference](API_REFERENCE.md#capability-required-per-endpoint) for the
capability each endpoint needs.

```toml
[security]
# Enforce authentication on every route (default: false)
enabled = true

[[security.api_keys]]
# The SHA-256 digest of the key — never the key itself
key_hash = "sha256:1db44a37dcf74ef70439a8887862839803d9686a41fe7c9d75d8fdfa0c72cdb1"
# admin (everything) | writer (read + write) | reader (read only)
role = "writer"
# Audit identity used in logs. Not a secret, not a credential.
label = "team-a"
# Optional: restrict this key to these indexes, for any role
allowed_indexes = ["docs", "wiki"]

[[security.api_keys]]
# Or keep the digest out of the config file entirely
key_hash_file = "/etc/cameodb/keys/agent"
role = "reader"
label = "agent"
```

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

The config is validated even when `enabled = false`, so a key stanza cannot be wrong in a way
you only discover on the day you turn authentication on. These all refuse to start:

- `enabled = true` with no keys — every request would be refused
- a `key_hash` with no `role`
- a hash that is not `sha256:<64 hex>`
- two entries with the same hash — one key cannot hold two roles
- `allowed_indexes = []`, which reads as "no restriction" but means "no index at all"

### Rate limiting MCP tool calls (`[security.limits]`)

```toml
[security.limits]
tool_calls_per_minute = 120   # 0 (the default) disables limiting entirely
tool_call_burst = 30          # spendable at once; 0 means one minute's worth
max_search_limit = 10000      # largest `limit` an MCP search may ask for
```

Authentication answers *who*, and `allowed_indexes` answers *what*. Neither says anything
about **how often**, and the caller this matters for is not an attacker: it is a legitimate
`reader` key held by an agent that decides to call `search_indexes` in a loop. Every one of
those calls is authorized, and a search fans out across every shard, so the loop costs the
node far more than it costs the agent.

A token bucket rather than a fixed window. Agent traffic is bursty by nature — a plan, then
a flurry of lookups, then a pause — and a fixed window either refuses the flurry or is set
so loose it never bites. The bucket lets the burst through and meters the sustained rate.

Points worth knowing:

- **Metered per key**, so one noisy agent cannot refuse another. With `[security]` off there
  is no identity to meter, and every caller shares a single bucket.
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

#### `max_search_limit` — how much one search may ask for

The rate above is off by default; this one is not. There is no reading of "no ceiling" that is
a number, and without one the caller decides how many hits the node builds, merges and
serializes for a single request. It defaults to **10000**, which is where one request stops
being one request for this architecture: a search fans out across every shard of an index, and
each hit is a redb lookup, a merge entry and a serialized document.

- **Applies to the MCP tools, not to `POST /api/{index}/search`.** The HTTP API is an
  operator's own client asking a considered question; the ceiling exists for an agent choosing
  its own limit, which is why it sits with the other MCP limits rather than under `[search]`.
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

### Search Configuration
- `CAMEODB_INDEXER_MEMORY_MIN_MB`: Minimum indexer memory
- `CAMEODB_INDEXER_MEMORY_MAX_MB`: Maximum indexer memory
- `CAMEODB_TOTAL_MEMORY_LIMIT_MB`: Total memory limit
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
bind_address = "0.0.0.0"

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

[search]
indexer_memory_max_mb = 512
total_memory_limit_mb = 4096
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
[search]
# Higher memory allocation for better write performance
indexer_memory_min_mb = 64
indexer_memory_max_mb = 1024
total_memory_limit_mb = 8192

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
# Disable fsync for maximum write speed (less durable)
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

### Performance vs Durability Trade-offs

| Setting | Performance | Durability | Note |
|---------|-------------|------------|------|
| `wal_sync = false` | ⬆️ High | ⬇️ Low | Risk of data loss on crash |
| `indexer_memory_max_mb = 1024` | ⬆️ High | ➡️ Same | Uses more RAM |
| `memory_pressure_threshold_percent = 90` | ⬆️ High | ➡️ Same | Higher memory usage |

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
max_body_size_mb = 50
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

[search]
indexer_memory_min_mb = 64
indexer_memory_max_mb = 512
total_memory_limit_mb = 2048
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
[search]
total_memory_limit_mb = 4096  # Increase limit
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
