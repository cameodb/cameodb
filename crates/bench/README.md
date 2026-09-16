# cameodb-bench

A latency harness for CameoDB, and a worked example of the client SDK. It offers load two
ways, and they answer different questions.

```bash
# Closed loop: N requests in flight, each issued when the last is answered.
# Measures service time at a fixed concurrency.
cargo run -p bench -- --url http://localhost:9480 --mode mixed --concurrency 8 --duration 30

# Open loop: requests offered on a schedule that does not wait for answers,
# so the node's queue is allowed to grow and that growth is the measurement.
cargo run --release -p bench -- --url http://localhost:9480 --mode search --rate 3000 --duration 20
```

`--rate` and `--concurrency` are mutually exclusive: one is a statement about how much work is
outstanding, the other about how much arrives, and no run is both.

## Why it exists

The ROADMAP's performance targets are written in percentiles — "write p99 reduced by 20-40%"
— and nothing in the repo could measure one. The previous `scripts/testing/load-test.sh`
forks `curl` per request and times it in bash, so at any real concurrency it measures process
spawn and connection setup rather than CameoDB.

## What it reports

```
writes (client-observed)      count, throughput, mean, p50/p90/p95/p99/p99.9/max
bulk requests                 per *request*, plus total documents and docs/s
searches (client-observed)    the same, for queries
searches (node-reported)      the node's own took_ms for the same requests
worker pool                   jobs per worker, core placement, dispatch counters
open loop only (--rate)       per-second series bucketed by outcome, harness lag, a verdict
```

`--mode bulk` measures per *request*, not per document — a 500-document request taking 200ms
is one 200ms sample, and `docs/s` is the number to compare. Measured on one 4-shard node,
4 workers, 10s:

| Mode | Throughput | Per document | p50 request |
|---|---|---|---|
| `write` (1 doc/request) | 224 docs/s | 4.46ms | 16.15ms |
| `bulk --batch-size 50` | 2 074 docs/s | 0.48ms | 63.68ms |
| `bulk --batch-size 500` | 5 339 docs/s | 0.19ms | 365.05ms |

Batching is worth roughly **9× at 50 and 24× at 500** here. The shape is the point: request
latency rises with batch size while per-document cost falls, because one request amortises
the round trip, the redb transaction and the commit-threshold check over every document in
it. Pick a batch size from the request latency you can tolerate, not from the throughput
chart alone.

The two search rows are the useful pair. Node-reported `took_ms` is the search itself;
client-observed includes queueing at the concurrency limiter, the worker hop and the network.
A large gap between them points at queueing, not at the query.

The worker-pool section is the other half of a latency number: an even `jobs per worker` row
with a bad p99 is a different problem from a lopsided one, and `affine` versus `round-robin`
says whether shard-affine dispatch was in play. Core placement shows `N` for a thread that is
pinned and `(N)` for one that asked and was refused — pinning is a no-op on macOS, so every
run there shows the latter.

## Commits, and why they shape the numbers

A commit is not per document. Two things trigger one, per `(shard, index)`:

1. **An operation-count threshold.** `should_commit_writer` commits once operations since the
   last commit reach `default_batch_size × (1 + budget_ratio × 19)` — 1 000 ops at the
   minimum indexer memory budget, up to 20 000 at the maximum, with a further ×1.5 once a
   burst exceeds 5× `default_batch_size`.
2. **A 5-second idle timeout** (`[search] supervisor_timeout_secs`). The safety net: a
   trickle that never reaches the threshold would otherwise sit uncommitted, and therefore
   unsearchable, until the next write arrived.

Which of the two fires changes what a run means, and at bench-scale traffic it is usually the
second. Measured on a 4-shard node:

| Run | Documents | Threshold commits | Idle commits |
|---|---|---|---|
| `--mode write`, 10s | 2 250 | **0** | 4 (one per shard, after writes stopped) |
| `--mode bulk --batch-size 500`, 10s | 52 000 | **48** | — |

2 250 single writes spread over 4 shards is ~560 per shard, which never reaches the 1 000-op
threshold — so nothing committed during the run, and those documents were not searchable
until 5 seconds after the last write. Two consequences for reading results:

- **Single-write latency mostly excludes commit cost** at this scale. It is WAL plus redb
  plus the tantivy in-memory add. Push a run past the threshold and roughly one write in a
  thousand also pays for a commit, which is where the p99.9 tail comes from.
- **Search freshness lags writes** by up to the idle timeout, but only for queries that go
  through tantivy. An `id:` lookup is answered without a committed segment and is visible
  within milliseconds; a query on an ordinary indexed field waits for the commit. Measured
  with a 5s timeout: `id:i4` visible in 0.05s, `title:zebracrossing` for the same document in
  5.35s. `--mode mixed` therefore searches an index whose most recent writes are not yet
  matchable by content — real behaviour, not an artifact, but do not read it as a
  search-recall measurement.

## What it was first used for

Deciding whether to recommend the CPU affinity flags. The answer was no, and the run is
worth repeating as a template: one arm per config, three repeats each, every arm from an
empty data volume, node in a Linux container because pinning is a no-op on macOS, and
`/_admin/workers` checked afterwards so "pinned" means observed rather than requested.

| Arm | write ok/s @c16 | write p90 | search ok/s @c16 | search p99 |
|---|---|---|---|---|
| `writer_core_affinity` only (default) | 3 375 | 6.63ms | 5 055 | 8.3ms |
| `+ shard_affine_dispatch` | 2 797 | 10.15ms | 4 850 | 8.9ms |
| `+ worker_core_affinity` | 2 815 | 9.94ms | 4 320 | 16.5ms |

The worker-pool section is what turned a regression into an explanation. `jobs per worker`
showed the pool shrinking from 16 workers to 8 when the flags went on, and the container
sitting at ~135% CPU of 800% available said the workers were waiting rather than computing —
so halving them halved what the node had in flight. Neither number is in the latency block;
both were necessary. See `docs/CONFIGURATION.md` for the full write-up.

## Open loop: offering an arrival rate

`--rate N` offers N requests per second on a schedule that does not wait for answers. A
saturated node therefore shows up as a growing queue and then as refusals, which is what
"can this node take 3,000 searches a second" actually asks. Closed loop cannot answer it: it
stops offering load exactly when the node stops keeping up, so a saturated node reads as
rising latency and never as an overload.

```
--rate <PER_SEC>       Requests offered per second, per workload
--search-rate <N>      Override --rate for searches
--write-rate <N>       Override --rate for writes (single or bulk)
--rate-steps <a,b,c>   Ramp through these rates, --duration seconds each
--arrival <PROCESS>    poisson (default) | uniform
--max-in-flight <N>    Harness safety ceiling (default: 50000)
--seed <N>             Arrival-stream seed (default: 1)
```

Arrivals are Poisson by default, because the bursts are the condition a batching optimisation
needs in order to pay and a perfectly spaced stream never produces one. `--arrival uniform`
removes that variance when the question is about the node alone.

### Reading an open-loop run

```
       10002  500 ok/s over 20.0s
     service  mean 755.98ms  p50 787.77ms  p90 855.79ms …
       total  mean 756.08ms  p50 787.88ms  p90 855.96ms …
  harness lag  p50 16µs  p99 1.36ms  max 30.56ms
    failures  shed (503) 50207
       first  Search failed: 503 Service Unavailable - overloaded: a 823ms backlog against a 1000ms request
           ok/s  989 537 302 598 536 418 490 544 403 508 …
     503 shed/s  2069 2394 2735 2355 2442 2593 2510 2418 2581 2450 …
       verdict  the node declined 83.4% of what was offered…
```

Four things carry the meaning, and the summary percentiles are the least of them:

- **`service` against `total`.** `service` is sent-to-answered; `total` runs from the time the
  request was *due* to be sent. `total` is the number an SLA is written against, and where the
  two diverge the gap is queueing ahead of the node that a closed-loop run cannot see at all —
  time a request spent waiting to be issued rather than waiting to be answered.
- **`harness lag`, before anything else.** The generator spins to hit sub-millisecond arrival
  times, so a co-located run competes with the node for cores. Lag climbing while in-flight is
  below the ceiling means the run measured the harness. The verdict says `INVALID` and refuses
  to be read as a statement about the node when that happens, rather than printing a plausible
  number.
- **The per-second series, bucketed by outcome.** `ok/s` beside `503 shed/s` and `408 timeout/s`
  is where a collapse is visible: a node whose successes fall while timeouts climb at a fixed
  offered rate is shedding the client rather than the work, and no summary statistic shows it.
- **The verdict line.** `sustained` means the node took everything offered. A declined
  percentage is the capacity answer, not a harness fault — read the failure breakdown, where
  `503` is a clean refusal the caller can retry and `408` is a client abandoned after paying
  the full timeout.

### A worked run

The overload work in `ROADMAP.md` (F7) was measured with this, and it is the template worth
copying: seed the index once, then hold a fixed rate against a node whose request timeout is
short enough to reach the regime.

```bash
# One node, 4 shards, search_threads = 2, max_concurrent_requests = 3000,
# request_timeout_secs = 1, index seeded once and kept between arms.
cargo run --release -p bench -- --mode search --rate 300  --duration 20 --seed-docs 200000 --keep-index
cargo run --release -p bench -- --mode search --rate 1000 --duration 20 --seed-docs 0 --keep-index
cargo run --release -p bench -- --mode search --rate 3000 --duration 20 --seed-docs 0 --keep-index

# Recovery: hold overload, then drop to well under capacity in one run.
cargo run --release -p bench -- --mode search --rate-steps 3000,300 --duration 15 --seed-docs 0 --keep-index

# The capacity knee in one pass, rather than by re-running by hand.
cargo run --release -p bench -- --mode search --rate-steps 2000,4000,8000,16000 --duration 20
```

Seed with `--seed-docs` on the first arm and `--seed-docs 0 --keep-index` on the rest: seeding
is a bulk load through the node under test, and repeating it per arm both wastes time and
leaves each arm measuring a differently-sized index. Seed *before* narrowing the timeout — a
200k-document load through a node running `request_timeout_secs = 1` is a queue of bulk writes
against a one-second budget, and the node is entitled to refuse it. Load with the timeout at
its default, then restart the node on the short one. Pair the run with `/_cluster/health`
(`queue_depth`, `predicted_wait_ms`, `read_pool_abandoned`) and `/_admin/workers`
(`dispatch.refused_at_admission`, `dispatch.abandoned`), which say *where* a refusal was made —
the harness only sees that one happened.

## What it does not measure

`--concurrency` is closed-loop and does not model an arrival rate, so **compare closed-loop
runs only at equal concurrency, and do not read their percentiles as an SLA.** Use `--rate` for
anything phrased as a rate. Neither mode measures recall or correctness: `--mode mixed`
searches an index whose newest writes are not yet committed, by design.

Run it against a node on another machine when the numbers matter. Sharing a host with the
node means the generator competes for the cores under test — which is exactly the interference
the thread-per-core work is about, and under `--rate` it is also the generator's own accuracy
at stake. Read `harness lag` and the verdict before believing a co-located number.

## As an SDK example

This binary depends on `client` and never on the server crate, and it issues no request the
SDK cannot express — when it needed a single-document write, `write_document` was added to
the SDK rather than reached around with a raw `http()` call. Everything it does is available
to any consumer:

| What | Where |
|---|---|
| Build a client, with TLS trust and a bearer key | `main.rs::connect` |
| Fail early on an unreachable or unauthenticated node | `main.rs`, the `health()` call |
| Create a schema | `workload.rs::prepare_index` |
| Bulk load | `bulk_index`, in `prepare_index` |
| Single-document write | `write_document`, in `write_worker` |
| Search | `search`, in `search_worker` |
| Admin endpoints | `admin_worker_stats`, `admin_index_commit` |

Argument parsing is deliberately hand-rolled rather than `clap`, so a reader following the
example meets `CameoClient` on the second screen instead of a derive macro.

## Not shipped

`publish = false`, and `scripts/build/build-packages.sh` packages the `cameodb` binary by name.
Keeping the crate in the workspace means it has to keep compiling against the SDK, which is
most of its value as an example — an SDK change that breaks a consumer breaks the build here.
