# CameoDB Development & Optimization Plan

Active work is in this file: what is open, partial, planned or deferred. Everything
delivered is in [docs/ROADMAP-ARCHIVE.md](docs/ROADMAP-ARCHIVE.md), kept in full. A finished item leaves a
one-line stub where it stood, linking to it, and the ID it was filed under (F7, OB8, M6, as quoted in
code comments) is a heading in this file or in the archive. The archive is not decoration: the
measurements and the rejected options are what stop a settled question being reopened, and
several entries exist precisely to say *do not build this again*.

**Last reconciled against the code: 2026-09-01, at 0.3.3.** The 2026-08-26 pass is recorded
under [Reconciliation](#reconciliation-2026-08-26); the 2026-09-01 review is filed in place —
C3–C6, CH8–CH12 and OB3–OB9 — and a re-read on the same day is recorded under
[Reconciliation](#reconciliation-2026-09-01). The 2026-09-08 review of the 0.3.3 and 0.3.4
changesets is filed as its own activity group under
[L. Post-0.3.4 review](docs/ROADMAP-ARCHIVE.md#l-post-034-review--preparing-the-next-cycle--done), held until the
release has settled. The 2026-10-07 whole-code review, taken at the 0.3.6 cut, is filed as
[N. Pre-0.3.6 whole-code review](#n-pre-036-whole-code-review--partial). The 2026-10-08 review
of the released 0.3.6, read as a database-systems pass, is filed as
[O. Post-0.3.6 review](#o-post-036-review--the-base-for-the-next-iteration--partial). The
2026-10-15 crash-recovery review, measured on a single node and a three-node cluster, is recorded
under [Reconciliation, 2026-10-15](#reconciliation-2026-10-15): it corrected Phase 16's premise
about redb, closed [E6](docs/ROADMAP-ARCHIVE.md#e6--a-crash-restart-repaired-every-shards-redb-file), and filed OB25 and
OB26. Features agreed for the iterations after the 0.3.7 release are filed as
[P. Post-0.3.7 features](#p-post-037-features--partial), and those after 0.3.8 as
[Q. Post-0.3.8 features](#q-post-038-features--planned).

## How to read this file

Every phase, stage and item carries one of these markers, and only these:

| Marker | Meaning |
|---|---|
| ✅ **Done** | Shipped and present in the tree |
| ◐ **Partial** | Some of it shipped; what remains is named in the same place |
| 📋 **Planned** | Agreed and scoped, not started |
| 💭 **Deferred** | Deliberately not planned — the reason is recorded so it is not re-litigated |
| ❌ **Rejected** | Built or designed and turned down; kept so it is not rebuilt |
| ⏭️ **Skipped** | Overtaken by another change before it was needed |

A phase is ◐ if anything inside it is not ✅. A finished item moves to
[docs/ROADMAP-ARCHIVE.md](docs/ROADMAP-ARCHIVE.md) and leaves a one-line stub in its place; a group whose every
item is ✅ moves whole and is listed under [Delivered work](#delivered-work-in-the-archive). An item that is
✅ but names a remainder it did not close (A4, OB12, OB14) stays here until that is closed or filed on its own.

Two spellings, one meaning: a marker in a heading or in a bullet is plain (`✅ Done`); an
item's opening status line is bold (`📋 **Planned.**`) because it is the sentence, not a tag
on one.

## Status at a glance

| Phase | Status | What is left |
|---|---|---|
| 1–9 — Foundations through advanced architecture | ✅ Done | — |
| 10 — Field projection | ✅ Done | — |
| 11 — Read/write hot-path optimizations | ✅ Done | — |
| 11.5 — Jemalloc memory management | ✅ Done | — |
| 12 — MCP server integration | ◐ Partial | Streaming, semantic routing, the documentation pass, and compliance tests and benchmarks |
| 13 — Thread-per-core & memory operations | ◐ Partial | Stage 2f.2 (CPU arenas) and 2f.3 (per-arena jemalloc stats) — both with the evidence against 2f.2 |
| 14 — Security hardening | ◐ Partial | Stage C3 only (per-index role overrides); complexity caps deferred |
| 15 — HA: reindex, replication, migration | 📋 Planned | All three stages |
| 16 — Boot & OOM recovery at scale | ◐ Partial | Stage 3's deeper warming options, and the measurement on the reporting node — now with `quick_repair` on and off; E5 (a cap on open indexes) done by M1, E6 (redb's crash repair) done 2026-10-15, E1 (Stage 4.2) overtaken by F9 |
| 17 — Record deletion | ✅ Done | — |
| 18 — Field types: Facet and JSON | ◐ Partial | J2 and J3 — a json field behaves exactly like a text one. J1 (facet writable) and OB1 (the `fast` three-state prerequisite) are done. No migration for what remains |
| 19 — Field metrics: min and max | 📋 Planned | All of it — no aggregation of any kind exists today. Min and max on a fast numeric or date field, nothing else |
| 14 — Security hardening (posture items C3–C8) | ✅ Done | C3–C8 all closed; C8 by M3 on 2026-09-20 |
| Code health — reviewed at 0.3.1, extended 2026-09-01 | ◐ Partial | Twelve items; CH1, CH8–CH12 done, CH2's server half absorbed by the split, CH2's storage half closed out by L12 |
| L — Post-0.3.4 review: the refactor cycle | ✅ Done | All twenty closed — four defects, six security remainder items, three decompositions, six simplifications, and the retrospective (L20, run 2026-09-19) |
| M — The 0.3.5 goal set: multi-tenant exposure | ✅ Done | M0 closed; M1, M2, M3, M4, M5 and M7 done — the blocker is cleared, the surface is metered, and tenants are bounded and isolated per index. No feature build remains; M8 closed with the prefix floor and default-field cap, the clause cap deferred. M6 done: the open-loop arms show goodput degrading rather than collapsing on the bulk, single-write and read lanes, after fixing the two defects its arms found — [OB14](#ob14--a-timed-out-request-never-leaves-the-worker-pool-and-the-node-degrades-until-it-is-restarted) and [OB15](docs/ROADMAP-ARCHIVE.md#ob15--every-refused-request-was-an-error-line-and-under-write-overload-the-logging-cost-half-the-goodput), and its confirmation run on 2026-09-27 found OB22 costs a single node nothing measurable. What is left is the release cut |
| N — Pre-0.3.6 whole-code review | ◐ Partial | N1–N10 and N10a done 2026-10-07/08, in 0.3.6: a `HEAD` authorization gap, batched writes that never learn a field, a peer's 400 answered as 503, node identity replaced silently, query rewrites by substring, three client errors, the first shard's memory budget, streaming broadcast, a storage shutdown race, flattened storage errors, a search of a missing index answering 200. Left, planned and not release blockers: N11–N17 — one way to reach a peer, dead surface, `run_change`, typed boundaries, single owners, the orchestrator split |
| O — Post-0.3.6 review: the next-iteration base | ◐ Partial | O1 (the `wal_sync=false` durability barrier) and O2 (deadlines on every remote ask) done 2026-10-15. Left: O3, shard-map merges on a per-owner epoch (the prerequisite N15 does not name, and since 2026-10-18 the one [OB27](#ob27--a-replaced-node-never-leaves-the-cluster)'s `forget` needs), and O4, the request budget on the wire. O5 folds Rust-surface items into N16/N17; O6 reconciles the architecture doc |
| P — Post-0.3.7 features | ◐ Partial | P1, a cap on how many elements of each array a search hit carries — a node default, a node maximum, and `array N` per request — done 2026-10-19. P2, one declaration of an index's default fields — `searched_by_default` dropped, the `--id` reload keeps the list — done 2026-10-20. P3, a schema read is a schema write — `PUT` takes a `GET`, which now reports every property the thumbprint covers — done 2026-10-21. P4, ip fields queried the way addresses are — CIDR, bare addresses, IPv4-bounded ranges, a fast ip sort — done 2026-10-21. Left: P5, native json fields — paths, typed values and ranges, planned; P6, a hit says which array elements matched, planned |

## Reconciliation, 2026-08-26

The file had not been touched since 2026-08-19 and 0.3.2 shipped on 2026-08-20. Checked
item by item against the tree; six corrections, all of them recorded in place below.

- **Phase 12's documentation pass is ◐, not 📋.** `crates/mcp/README.md` already carries
  client configuration for Claude Code, Claude Desktop, Windsurf, Cursor and the MCP
  Inspector, a six-step usage workflow, the paging rules and the full query-syntax
  reference — and `crates/mcp/src/guidance.rs` ships session `instructions` from
  `initialize` plus an orchestrator skill over `prompts/get`, neither of which this file
  ever recorded. What is actually missing is narrower than the item claimed.
- **Phase 12's testing item is ◐, not 📋.** Four MCP suites drive `tools/call` against a real
  node. Conformance against the specification and agent-query latency figures are still
  absent, which is what the item now says.
- **Phase 12 was headed 📋 Planned** while six of its nine steps were complete. Corrected.
- **The 0.3.2 fixes were not recorded at all.** Five commits between 2026-08-19 and
  2026-08-20 — sort refusal, emptied-query refusal, shard release on shutdown, node identity
  persistence, and the validation suite's timeout probe — are now archived under
  [0.3.2 hardening](docs/ROADMAP-ARCHIVE.md#032-hardening-2026-08-1920--done).
- **Two code-health files grew rather than shrank** since the 0.3.1 review:
  `node_orchestrator.rs` 9,300 → 9,683 lines, `storage/src/lib.rs` 7,600 → 8,293. Items CH1
  and CH2 carry the current figures.
- **One defect was observed on 2026-08-13 and never filed here.** `fast: false` on a numeric
  field is not honoured; it is now item OB1.

Everything else verified as the file described it: no `search_after`, no `sched_setaffinity`,
no per-arena `mallctl`, no reindex path, `should_commit_writer` still counts operations only,
`cameodb-bench` still closed-loop (it grew an open-loop mode later — see
[F2](docs/ROADMAP-ARCHIVE.md#f2--an-open-loop-load-generator), 2026-09-15), and the four code-health duplications all
still present.

## Reconciliation, 2026-09-01

A re-read of the 0.3.3 review's own fixes, against the tree rather than against the commits that
claimed them. Five corrections; the pattern in four of them is the same, and worth naming: a fix
that closes a defect on the path it was found on, while the *sibling* path — the other arm of a
match, the other framing of the same input, the second hop of the same forward — keeps the
behaviour the fix was written to remove.

- **[OB5](docs/ROADMAP-ARCHIVE.md#ob5--one-batch-can-index-the-same-id-twice) was ✅ and is only now done.** Coalescing
  the adds was right; the deletes still read "did this id exist" from each `insert` in turn, which
  a delete earlier in the same batch had already emptied. Reproduced, then fixed and pinned.
- **[OB7](docs/ROADMAP-ARCHIVE.md#ob7--the-validator-and-the-writer-disagree-about-floats-in-integer-fields) had a
  sibling.** Element-wise list typing, landed the same day, made a *nested* list read as its
  inner element type — so the disagreement OB7 closed for floats reopened one level down.
- **[OB3](docs/ROADMAP-ARCHIVE.md#ob3--a-single-write-or-delete-can-land-on-the-wrong-shard) made an unbounded pattern
  reachable from every write.** Forwarding had no hop limit before, on the bulk paths only; OB3
  extended it to single writes and deletes. Bounded now, with the bulk paths named as still open.
- **Two new items, both from reading the fixes rather than the code they fixed:**
  [OB10](docs/ROADMAP-ARCHIVE.md#ob10--the-record-limit-meant-different-things-depending-on-how-the-wire-split-the-line)
  and [OB11](docs/ROADMAP-ARCHIVE.md#ob11--reasons-and-totals-that-went-missing-on-the-way-out), plus
  [C7](docs/ROADMAP-ARCHIVE.md#c7--a-500-printed-the-nodes-internal-error-text) from the status-classification work.
- **[CH8](docs/ROADMAP-ARCHIVE.md#ch8--the-single-write-path-clones-the-whole-schema-and-document) is correct, and its
  documentation was not.** The `has_new_field` guard reads as though it disables type evolution
  for existing fields. It does not change behaviour: `validate_document` sets `needs_evolution`
  only for fields the schema has never seen, and a value an existing field cannot hold is
  *refused* rather than widened — so evolution of an existing field is reachable only from
  initial schema sampling, which is unaffected. `evolve_field` and `should_evolve_field_static`
  now say so, because the next reader will ask.

The two file sizes in [CH1](docs/ROADMAP-ARCHIVE.md#ch1--one-scatter-gather-written-twice) and
[CH2](#ch2--the-merge-primitives-deserve-their-own-module) were stale by ~1,300 and ~930 lines and
are current again. Everything else verified as the file described it: CH9's two remaining pieces,
CH10, CH11's two duplicates and two hashes, OB8's dropped offset and OB9's discarded peer errors
are all still exactly as written.

**On the order of work.** The [table](#the-order-of-work) is ordered by what each item costs to
read, and this batch ignored it: everything done since 0.3.3 is an OB or CH item opened on
2026-09-01. Defects before features is the right call and not a reason to rewrite the table — but
the table describes how the *remaining* work should be sequenced, not how work is sequenced when a
review turns up something wrong.

## Reconciliation, 2026-10-15

Opened by a report from the field: a node restarted after an unclean exit is slow to start, and
the suspicion was that recovery replays more of the WAL than it needs to. Measured rather than
read, on a release build of `dc331cf`: one node with 4 shards and ~2.5 GB of redb, killed with
`SIGKILL` mid-ingest; then three clustered nodes, one of them killed the same way.

- **The WAL replay is exact, and was never the delay.** Replay starts one past the checkpoint
  stamped into the Tantivy commit and stops at the WAL's last entry; every commit path stamps it,
  and Tantivy's merges carry the payload forward. The tails measured 1,708–20,713 entries per
  shard, replayed in 22–100 ms, and no acknowledged write was lost on either topology.
- **The delay is redb's own repair, and Phase 16 said it did not exist.** Its analysis records
  that redb's unclean restart "takes ~0 extra seconds"; that has been false since redb 2. Without
  a recorded allocator state, `Database::open` on a file left open by a crash verifies and walks
  the whole file — up to three passes — before the shard can open: 389–665 ms per 1 GB shard with
  the file in the page cache, against 5–8 ms after a clean stop, and minutes per shard read from
  disk at production sizes. Corrected in the archive, and fixed by
  [E6](docs/ROADMAP-ARCHIVE.md#e6--a-crash-restart-repaired-every-shards-redb-file).
- **[E1](#e1--stage-42--a-max-wal-size-commit-trigger) was overtaken.** It was filed as the only
  bound on worst-case replay; [F9](docs/ROADMAP-ARCHIVE.md#f9--commit-on-a-clock-not-a-count)'s commit clock and its
  backstop now bound the tail by time and by count, which is what the measurement above shows.
- **A graceful stop is clean.** Every shard's file opened without repair after `SIGTERM`, so the
  `Arc<HybridStore>` clones held by the writer, warmup and monitor threads do not outlive the
  shutdown — checked because, had they, every restart would have paid for a repair.
- **Two clustered findings, filed as observed:** a node rejoining answers a search from a partial
  ring as if it were whole ([OB25](docs/ROADMAP-ARCHIVE.md#ob25--a-search-during-a-restart-answered-short-without-saying-so)),
  and a restarted node stays in bootstrap mode while any expected peer is down
  ([OB26](#ob26--a-restarted-node-waits-on-every-expected-peer-before-it-leaves-bootstrap)).
  Rejoin itself is fast: the killed node was serving 0.5 s after restart and its peers saw the
  full cluster 0.56 s after it, because a killed process's sockets close at once and Identify
  reintegrates it on the new connection.

The post-0.3.6 commits — the search budget and stage timings (`a8c61f8`), the two-phase
scatter-gather (`7c794eb`), the two MCP fixes and the retention-prune ops script — had reached
[F10](#f10--fast-field-projection-the-pages-fields-answered-from-the-columns)'s background here but
not `CHANGELOG.md`; they are recorded under `[Unreleased]`.

---

# Active work

## The order of work

Ordered by what the work costs rather than by what it is worth, agreed 2026-08-15: each item
is a prerequisite for reading the next one clearly. The *Opened* column is when the item was
first written down here, so the chronology stays visible under the cost ordering.

| # | Item | Phase | Opened | Status |
|---|---|---|---|---|
| [A1](#a1--mcp-streaming) | MCP streaming | 12 | 2026-08-15 | 📋 |
| [A2](#a2--the-documentation-pass) | The documentation pass | 12 | 2026-08-15 | ◐ |
| [A3](#a3--protocol-compliance-tests-and-agent-query-benchmarks) | Protocol-compliance tests and agent-query benchmarks | 12 | 2026-08-15 | ◐ |
| [A4](#a4--what-a-schema-listing-says-about-id-for-projection-and-for-sorting) | What a schema listing says about `id`, for projection and for sorting | 12 | 2026-08-15 | ✅ |
| [A5](#a5--semantic-routing) | Semantic routing | 12 | 2026-08-05 | 📋 |
| [A6](docs/ROADMAP-ARCHIVE.md#a6--the-syntax-reference-has-drifted-from-the-engine) | The syntax reference has drifted from the engine | 12 | 2026-08-27 | ✅ |
| [A7](docs/ROADMAP-ARCHIVE.md#a7--a-short-page-and-a-stale-count-say-nothing-about-why) | A short page and a stale count say nothing about why | 12 | 2026-08-27 | ✅ |
| [B1](#b1--2f2--cpu-arenas-for-write--read--merge) | 2f.2 — CPU arenas for write / read / merge | 13 | 2026-08-08 | 📋 |
| [B2](#b2--2f3--per-arena-jemalloc-stats) | 2f.3 — per-arena jemalloc stats | 13 | 2026-08-08 | 📋 |
| [C1](#c1--per-index-role-overrides) | Per-index role overrides | 14 | 2026-07-30 | 📋 |
| [C2](#c2--query-complexity-caps) | Query complexity caps | 14 | 2026-08-10 | 💭 |
| [C3](docs/ROADMAP-ARCHIVE.md#c3--fail-closed-on-unauthenticated-internal) … [C6](docs/ROADMAP-ARCHIVE.md#c6--redact-the-cluster-psk-in-debug) | Posture hardening: four items from the 2026-09-01 review — C3 and C4 done | 14 | 2026-09-01 | ◐ |
| [C7](docs/ROADMAP-ARCHIVE.md#c7--a-500-printed-the-nodes-internal-error-text) | A `500` printed the node's internal error text | 14 | 2026-09-01 | ✅ |
| [D1](#d1--reindex) | Reindex | 15 | 2026-08-15 | 📋 |
| [D2](#d2--replication) | Replication | 15 | 2026-08-15 | 📋 |
| [D3](#d3--migration) | Migration | 15 | 2026-08-15 | 📋 |
| [E1](#e1--stage-42--a-max-wal-size-commit-trigger) | Stage 4.2 — a max-WAL-size commit trigger — overtaken by F9's commit clock and backstop | 16 | 2026-08-19 | ⏭️ |
| [E2](#e2--stage-3s-deeper-warming-options) | Stage 3's deeper warming options | 16 | 2026-08-19 | 📋 |
| [E3](#e3--measure-recovery-on-the-reporting-node) | Measure recovery on the reporting node — with `quick_repair` on and off | 16 | 2026-08-19 | 📋 |
| [E4](#e4--two-compatibility-paths-with-no-end-to-end-test) | Two compatibility paths with no end-to-end test | 16 | 2026-08-19 | 📋 |
| [E6](docs/ROADMAP-ARCHIVE.md#e6--a-crash-restart-repaired-every-shards-redb-file) | **A crash restart repaired every shard's redb file** — up to three passes over each file before the shard opened; `[storage] quick_repair`, on by default, records the allocator state per commit, and a repair is now logged. 1 GB shard: 2.1–2.5 s → 12–18 ms | 16 | 2026-10-15 | ✅ |
| [F1](#f1--the-cost-of-a-durable-commit-under-read-load) | The cost of a durable commit under read load — deferred: redb has no middle durability level, and building one trades the guarantee this node keeps | — | 2026-08-10 | 💭 |
| [F2](docs/ROADMAP-ARCHIVE.md#f2--an-open-loop-load-generator) | An open-loop load generator | — | 2026-09-15 | ✅ |
| [F3](#f3--take-unkeyed-searches-off-the-coordinator) | Take unkeyed searches off the coordinator — standalone half done, clustered half turned down: the published ring lags the coordinator | — | 2026-08-10 | ◐ |
| [F4](docs/ROADMAP-ARCHIVE.md#f4--the-bulk-paths-asked-the-coordinator-before-they-knew-they-needed-to) | The bulk paths asked the coordinator before they knew they needed to | — | 2026-09-02 | ✅ |
| [F5](#f5--concurrency-sweep-measured-2026-09-02) | Concurrency sweep on the release build — the operating point, and bulk's serialization measured | — | 2026-09-02 | ✅ |
| [F6](#f6--what-fsync-actually-costs-measured-2026-09-02) | What fsync actually costs — and why turning it off is a reallocation, not a speedup | — | 2026-09-02 | ✅ |
| [F7](#f7--the-request-timeout-sheds-the-client-not-the-work) | The request timeout sheds the client, not the work — measured: goodput goes to zero, not down | — | 2026-09-15 | ✅ |
| [F8](docs/ROADMAP-ARCHIVE.md#f8--the-overload-gates-do-not-cover-the-bulk-write-path) | The overload gates do not cover the bulk write path — health fixed, both lanes gated, admission predicting against a measured spread, and bulk folded into the service estimate by `490608b`. All three items closed; the re-measurement is [M6](docs/ROADMAP-ARCHIVE.md#m6--close-and-re-measure-the-bulk-lane) | — | 2026-09-17 | ✅ |
| [CH1](docs/ROADMAP-ARCHIVE.md#ch1--one-scatter-gather-written-twice) … [CH7](#ch7--the-string-fast-collector-repeats-the-macros-body) | Code health, seven items — CH1 done | — | 2026-08-16 | 📋 |
| [CH11](docs/ROADMAP-ARCHIVE.md#ch11--routing-key-derivation-is-written-four-times-with-two-algorithms) | Routing-key derivation, four spellings and two hashes — closed ahead of the split | — | 2026-09-01 | ✅ |
| [CH8](docs/ROADMAP-ARCHIVE.md#ch8--the-single-write-path-clones-the-whole-schema-and-document) … [CH12](docs/ROADMAP-ARCHIVE.md#ch12--write-path-serialization-and-round-trip-waste) | Code health, write-path efficiency, five items — all done | — | 2026-09-01 | ✅ |
| [OB1](docs/ROADMAP-ARCHIVE.md#ob1--fast-false-is-not-honoured-on-a-numeric-field) | `fast: false` is not honoured on a numeric field — landed ahead of [J2](#j2--a-json-field-should-mean-subfield-addressing), whose override it would otherwise have eaten | 18 | 2026-08-13 | ✅ |
| [J1](docs/ROADMAP-ARCHIVE.md#j1--a-facet-field-cannot-be-written-to) | A facet field cannot be written to | 18 | 2026-08-27 | ✅ |
| [J2](#j2--a-json-field-should-mean-subfield-addressing) | A json field should mean subfield addressing | 18 | 2026-08-27 | 📋 |
| [J3](#j3--the-flattening-lane-and-the-reference-that-describes-neither-lane-correctly) | The flattening lane, and the reference that describes neither | 18 | 2026-08-27 | 📋 |
| [OB2](docs/ROADMAP-ARCHIVE.md#ob2--a-facet-field-cannot-be-written-to) | A `facet` field cannot be written to — the evidence behind J1 | 18 | 2026-08-27 | ✅ |
| [OB3](docs/ROADMAP-ARCHIVE.md#ob3--a-single-write-or-delete-can-land-on-the-wrong-shard) … [OB12](#ob12--the-schema-gate-deadlocked-a-fan-out-against-itself) | Correctness, ten items from the 2026-09-01 review, the re-read of its own fixes, and the 0.3.3 release check — OB3–OB12 all done | — | 2026-09-01 | ✅ |
| [OB14](#ob14--a-timed-out-request-never-leaves-the-worker-pool-and-the-node-degrades-until-it-is-restarted) | **A timed-out request never leaves the worker pool** — a `DashMap` self-deadlock in `should_commit_writer` parked every shard writer thread past a 30s TTL. Found by the first [M6](docs/ROADMAP-ARCHIVE.md#m6--close-and-re-measure-the-bulk-lane) arm, fixed and pinned the same day | — | 2026-09-25 | ✅ |
| [OB15](docs/ROADMAP-ARCHIVE.md#ob15--every-refused-request-was-an-error-line-and-under-write-overload-the-logging-cost-half-the-goodput) | **Every refused request was an `ERROR` line** — synchronous on the write path's runtime, it halved single-write goodput under overload and failed health. Refusals are now counted into one periodic summary. Found by the M6 single-write arm, fixed the same day | — | 2026-09-25 | ✅ |
| [OB16](docs/ROADMAP-ARCHIVE.md#ob16--closing-an-index-from-another-thread-lost-the-writes-in-flight-on-it) … [OB19](docs/ROADMAP-ARCHIVE.md#ob19--two-clustered-deadlocks-through-the-coordinators-mailbox) | **The pre-release concurrency audit** — eviction from another thread lost in-flight writes from search (464 of 600 in the test), schema edits and evolution overwrote each other, streaming search ran outside the concurrency limit, and two clustered mailbox deadlocks. All fixed and, where a test can force it, pinned | — | 2026-09-26 | ✅ |
| [OB20](docs/ROADMAP-ARCHIVE.md#ob20--a-fresh-cluster-can-keep-a-partial-ring-and-nothing-repairs-it) | **A fresh cluster can keep a partial ring** — 4 of 5 simultaneous starts left one node without a peer's shards (or alone), and nothing re-synced. Fixed with one connection per peer, a seed redial, and a 10 s shard-map pull; 5 of 5 now converge. Found by the new `cluster` validation suite | — | 2026-09-26 | ✅ |
| [OB21](docs/ROADMAP-ARCHIVE.md#ob21--a-peer-that-stops-answering-detected-refused-at-once-and-bounded-while-it-lasts) | **A peer that stops answering** — orchestrator forwards now share one deadline, stale peer references go on the first failure, and topology can no longer drop the newest ring. Also fixed: a kameo panic at shutdown (5.5), and a frozen peer is detected by ping within ~40 s, after which requests for it are answered at once (5.4) | — | 2026-09-26 | ✅ |
| [OB22](docs/ROADMAP-ARCHIVE.md#ob22--an-orchestrator-waited-on-peers-while-holding-its-mailbox) | **An orchestrator waited on peers while holding its mailbox** — schema canvasses and forwards ran inside it, so two nodes doing either at once waited on each other until a 5 s or 60 s timeout: bulk writes through every node all timed out, and new indexes created through every node ran at 0.1/s, most refused. Fixed; now 510 batches/s and 134 new indexes/s, none failed; several nodes minting one index at once settle it by node id, where 88 of 120 such writes were refused. Found by the cluster suite's new cross-node phase | — | 2026-09-26 | ✅ |
| [OB23](docs/ROADMAP-ARCHIVE.md#ob23--recreating-an-index-after-dropping-its-schema-lost-most-of-what-was-written) | **Recreating an index after dropping its schema lost most of what was written** — the drop's record read as a schema, the node re-minted the index from every batch, and a retyped column killed the Tantivy writer, which stayed dead until restart. 1,030 of 16,559 documents landed; now all do, a dying writer is retired and replayed, and the loader counts every failure by its file line | — | 2026-09-27 | ✅ |
| [OB24](docs/ROADMAP-ARCHIVE.md#ob24--a-and-not-b-and-a-or-not-b-parsed-cleanly-and-answered-wrongly) | **`a AND NOT b` and `a OR NOT b` parsed cleanly and answered wrongly** — a `NOT` arm under `Must`/`Should` matched nothing and nothing reported it; rewritten to `a AND -b` / `a OR (* -b)` before the parser runs | — | 2026-10-05 | ✅ |
| [OB25](docs/ROADMAP-ARCHIVE.md#ob25--a-search-during-a-restart-answered-short-without-saying-so) | **A search during a restart answered short without saying so** — a rejoining node searched 5 of 9 shards and reported `failed: 0`; on one node, HTTP serves before the replayed tail is committed. Both fixed the same day | — | 2026-10-15 | ✅ |
| [OB26](#ob26--a-restarted-node-waits-on-every-expected-peer-before-it-leaves-bootstrap) | **A restarted node waits on every expected peer before it leaves bootstrap** — reproduced 2026-10-18: real, and invisible to clients, because the snapshot and any never-restarted peer carry the map. A day, mostly deleting code; with OB27 | — | 2026-10-15 | 📋 |
| [OB27](#ob27--a-replaced-node-never-leaves-the-cluster) | **A replaced node never leaves the cluster** — a node rebuilt with an empty data directory leaves its old identity's shards on every ring for good: writes to their key ranges answer 500, and the new node cannot learn an existing schema (503). The 500 is independent; `forget` needs O3 | — | 2026-10-18 | 📋 |
| [OB28](docs/ROADMAP-ARCHIVE.md#ob28--two-range-shapes-were-dropped-without-a-word-or-failed-the-search) | **Two range shapes were dropped without a word or failed the search** — `field:[* TO *]` answered zero unreported, and a range on a json field was a 500; both are now reported dropped clauses, refused with 400 when nothing else is left | — | 2026-10-21 | ✅ |
| [OB29](docs/ROADMAP-ARCHIVE.md#ob29--what-verifying-ob28-over-mcp-found) | **What verifying OB28 over MCP found** — a query whose every clause was dropped ran as match-all, a json path matched nothing unreported, and `validate_query` called a path an unknown field | — | 2026-10-21 | ✅ |
| [F9](docs/ROADMAP-ARCHIVE.md#f9--commit-on-a-clock-not-a-count) | **Commit on a clock, not a count** — bulk ingest 1.8–6.6×, a trickle searchable within 2 s, single writes unchanged | — | 2026-09-26 | ✅ |
| [K1](#k1--min-and-max-in-the-engine) | min and max in the engine, refused before any shard runs | 19 | 2026-08-27 | 📋 |
| [K2](#k2--the-merge-across-shards-and-nodes) | The merge across shards and nodes | 19 | 2026-08-27 | 📋 |
| [K3](#k3--the-surface) | The surface: a `metrics` block, the SDK, and the MCP reference | 19 | 2026-08-27 | 📋 |
| [L1](docs/ROADMAP-ARCHIVE.md#l1--size-cache-invalidation-by-substring-evicts-neighbouring-indexes) … [L20](docs/ROADMAP-ARCHIVE.md#l20--the-retrospective-and-the-sequence-into-the-next-cycle) | Post-0.3.4 review group — all twenty closed; the retrospective's output is [M](docs/ROADMAP-ARCHIVE.md#m-the-035-goal-set--multi-tenant-exposure--done) | — | 2026-09-19 | ✅ |
| [M0](docs/ROADMAP-ARCHIVE.md#m0--the-architecture-review-and-the-order-of-work) … [M8](docs/ROADMAP-ARCHIVE.md#m8--re-decide-the-query-complexity-caps) | The 0.3.5 goal set — a node exposed on the internet serving several tenants from one process; M0 closed, the M1 blocker cleared, and M2–M8 done; M6's open-loop arms found and fixed [OB14](#ob14--a-timed-out-request-never-leaves-the-worker-pool-and-the-node-degrades-until-it-is-restarted) and [OB15](docs/ROADMAP-ARCHIVE.md#ob15--every-refused-request-was-an-error-line-and-under-write-overload-the-logging-cost-half-the-goodput), and were confirmed on the cut binary 2026-09-27. The release cut remains | — | 2026-09-27 | ◐ |
| [N1](docs/ROADMAP-ARCHIVE.md#n1--head-requests-skip-authorization) … [N17](#n17--smaller-items-filed-so-they-are-not-lost) | Pre-0.3.6 whole-code review — ten defects (N1–N10) to close before the cut, then the duplication and structure behind them: one way to reach a peer, dead surface, `run_change`, typed boundaries, single owners, the `orchestrator.rs` split | — | 2026-10-07 | 📋 |
| [F10](#f10--fast-field-projection-the-pages-fields-answered-from-the-columns) | Fast-field projection — the page's fields answered from the columns, and the fetch phase skipped when they can be; the last layer of the search-economics work opened by the TS5 outage — deferred: the gain over the two-phase search is small for today's workloads, and the design is filed so it can be picked up | — | 2026-10-09 | 💭 |
| [F11](#f11--the-disk-a-delete-frees-was-never-returned) | The disk a delete frees was never returned — `cameodb compact` and `--rewrite` (redb, offline) and `POST /_admin/index/{index}/merge` (Tantivy, online) done; reclaiming deleted documents automatically, behind a setting, planned | — | 2026-10-16 | ◐ |
| [P1](docs/ROADMAP-ARCHIVE.md#p1--a-hits-arrays-are-capped-the-way-its-hits-are) | A hit's arrays are capped the way its hits are — `[search] default_array_limit`, `[security.limits] max_array_limit`, `array N` per request; found validating 0.3.7 on the threat-intel dataset, where one field carries a few hundred thousand addresses | — | 2026-10-19 | ✅ |
| [P2](docs/ROADMAP-ARCHIVE.md#p2--one-declaration-of-an-indexs-default-fields) | One declaration of an index's default fields — `default_fields` is the only list, each field's `default_search` the effective set; `searched_by_default` dropped, and `data load --id` keeps a declared list | — | 2026-10-20 | ✅ |
| [P3](docs/ROADMAP-ARCHIVE.md#p3--a-schema-read-is-a-schema-write) | A schema read is a schema write — `PUT /_config` takes a `GET /_config` as it is, and the read reports the record option, `learned` and the routing field it lacked, so a saved read restores its index and the thumbprint proves it | — | 2026-10-21 | ✅ |
| [P4](docs/ROADMAP-ARCHIVE.md#p4--ip-fields-queried-the-way-addresses-are) | Ip fields queried the way addresses are — CIDR as its range, a bare address searching the ip fields, an open IPv4 range kept among IPv4, `fast` on ip for a sort by address, write refusals naming the value | — | 2026-10-21 | ✅ |
| [P5](#p5--native-json-fields-paths-typed-values-ranges) | Native json fields — a json field indexes its object beside its text: paths and typed values match, ranges at a path run from the fast column it gets by default, RFC 3339 strings are dates; existing indexes unchanged until rebuilt | — | 2026-10-21 | 📋 |
| [P6](#p6--a-hit-says-which-array-elements-matched) | A hit says which array elements matched — `_matched_elements` beside `_truncated_arrays`, a per-field matcher from the screened AST applied in the fetch phase before the cap, `matches N` per request; priority not yet decided | — | 2026-10-21 | 📋 |
| [Q1](#q1--ghost-fields-index-columns-derived-from-another-field-under-a-condition) | Ghost fields — an index column derived from another field's value, for every document or under an `in` / `not_in` condition on a discriminator field, indexed in Tantivy and never stored in redb; one copy of the text in redb instead of one per analyzer — about 38% less disk on a multilingual sample | — | 2026-10-26 | 📋 |
| [Q2](#q2--the-croatian-analyzers-read-serbian-cyrillic) | The Croatian analyzers read Serbian Cyrillic — Cyrillic and Latin text and queries reach one term, inside `hr_stem` and `hr_stem_fold`; an index already holding Cyrillic text is reloaded | — | 2026-10-26 | 📋 |

---

## A. Phase 12 — MCP Server Integration ◐ Partial

Steps 1–6 of the phase and items 1–4 of the completion track are done; see
[Phase 12 — what landed](docs/ROADMAP-ARCHIVE.md#phase-12--mcp-server-integration-for-ai-agents).
What remains is below.

### A1 — MCP streaming

📋 **Planned.** Large result sets over the MCP streaming protocol. Left until after completion
track items 1–4 deliberately: streaming a result shape that is still changing means building
the transport twice. That shape has now stopped moving, so the reason to wait is spent.

Belongs beside [CH3 (cursor paging)](#ch3--cursor-paging-search_after): both answer "the result
is larger than a page", and building either changes how the other should work.

### A2 — The documentation pass

◐ **Partial**, and further along than this file claimed until 2026-08-26.

**Already shipped**, in `crates/mcp/README.md` unless noted:

- Client configuration for Claude Code, Claude Desktop, Windsurf, Cursor and the MCP Inspector
- A six-step usage workflow from `list_indexes` through a corrected federated search
- The paging rules, the resource URIs, and the full query-syntax reference with the
  per-field-type operator matrix
- Session `instructions` returned from `initialize`, and the orchestrator skill served over
  `prompts/get` — both in `crates/mcp/src/guidance.rs`, with tests holding the instructions
  short and keeping every query form they name in step with `crate::syntax`

**What is actually left:**

- **The syntax reference's home.** `validate_query` called with no arguments still returns the
  static reference, and the tool's own description tells agents to do exactly that. Moving it
  to `instructions` and a `cameodb://syntax` resource is a change to the tool's contract
  rather than a fix to it, which is why it was held for this item — the description, the
  instructions and the README have to change together.

**Done 2026-10-05**, on top of the earlier list:

- **`docs/MCP.md`** now carries the operator's side of the surface — what is mounted,
  enabling and securing it, connecting clients — and links the crate README as the
  authoritative caller reference rather than duplicating it. The root README's documentation
  list reaches it.
- **Index-design guidance for agent context** is the back half of that page: descriptions,
  fields declared up front with `PUT /api/{index}/_config` or carried by the first write
  (the cheap mitigation for [D1](#d1--reindex)), and types chosen for the operators
  `describe_index` will advertise.
- The stale **"Recent Changes"** section in `crates/mcp/README.md` is deleted; `CHANGELOG.md`
  holds the one history.

### A3 — Protocol-compliance tests and agent-query benchmarks

◐ **Partial.** Integration testing is no longer part of this item: `tools/call` is driven end
to end against a real node by `crates/server/tests/mcp_rate_limit.rs`,
`mcp_discarded_clauses.rs`, `mcp_federated.rs` and the MCP cases in `audit_trail.rs`.

Still absent: **conformance against the MCP specification** as a suite rather than as
assertions scattered through feature tests, and **latency figures for agent query patterns**.
The second wants [F2](docs/ROADMAP-ARCHIVE.md#f2--an-open-loop-load-generator), since an agent's arrival process is
not closed-loop.

Also carried from Phase 12 step 9 and not yet started: **example datasets shaped for RAG
workflows**, which is what a benchmark of agent query patterns needs to run against and what
[A2](#a2--the-documentation-pass)'s index-design guidance would demonstrate.

### A4 — What a schema listing says about `id`, for projection and for sorting

✅ **Done** 2026-08-31. Two halves, both about the same field.

**Projection** ✅ **Done** 2026-08-31, and the remedy is the opposite of the one this item
proposed. The plan was that `id` "should stop being offered as something to *project*, while
remaining something to query". Withdrawing it from the listing turned out to be the worse of the
two options: `id:VALUE` still answers on a shadow index, so a description without `id` makes
`validate_query` report the working form as an unknown field — trading a silent empty projection
for a confident wrong warning.

So `id` stays listed, and the projection was made to work instead. A projection naming `id` is
rewritten to the name the hits carry before it is checked or applied, and the `id` entry carries
`returned_as` naming that field, on `describe_index` and on `validate_query`'s `available_fields`
alike. That is the only thing relating the two names on a surface where they otherwise read as
two searchable text fields of the same type. Opened by completion track item 3.

**Sorting** ✅ **Done** 2026-08-27, and smaller than this item first claimed. The audit filed it
as "`sortable: false` is wrong for `id` and its shadow name". Verification against a running node
narrowed it twice, and the correction is worth keeping: `sortable` means *exactly* sortable by
design — `the_schema_reports_which_fields_can_be_sorted_exactly` asserts `sortable: false` for a
text field with no fast column — so the flag was not lying, and the rule added to `SORT_RULES` in
the same session ("a text or string field without one is sorted approximately rather than
refused") already covers `id`, whose declared type is text.

What was genuinely missing was narrower and shadow-specific. A shadow field reads
`indexed: false, fast: false, sortable: false`, and the guidance says an unindexed field matches
nothing — so nothing told an agent that sorting by it works at all, let alone that it orders by
the identifier. Confirmed on a node: `sort=doi` on a shadow index returns the right order and
reports `_approximate_sort: "doi"`. `SHADOW_FIELD` and the orchestrator skill now say so, which is
one edit propagating to the per-field `query_hint`, the README, `validate_query` and the served
prompt.

Amended 2026-08-31: that name is the one the *hits carry*, not the one the request used. The two
differ when a caller sorts a shadow index by `id`, where reporting `id` named the single field
absent from every hit in the same response — leaving nothing to check the order against. Both
spellings now report the shadow name.

No flag changed. Reporting `sortable: true` for an approximate sort would break a contract a test
names, and adding a third state is not worth it while `fast` already answers "how well".

**Still open: the projection half above.** Do it with [A2](#a2--the-documentation-pass), since the
fix and the prose describing field shapes land in the same place.

### A5 — Semantic routing

📋 **Planned.** Auto-select the best index or indexes for a query's intent, so an agent that
does not know the catalogue does not have to enumerate it. Carried from Phase 12 step 5;
nothing depends on it and nothing blocks it.

- ✅ [**A6 — The syntax reference has drifted from the engine**](docs/ROADMAP-ARCHIVE.md#a6--the-syntax-reference-has-drifted-from-the-engine)
- ✅ [**A7 — A short page and a stale count say nothing about why**](docs/ROADMAP-ARCHIVE.md#a7--a-short-page-and-a-stale-count-say-nothing-about-why)

---

## B. Phase 13 — Stage 2f ◐ Partial

Stages 1 and 2a–2e are done, and both affinity flags stay `false` on measured evidence — see
[Phase 13 — what landed](docs/ROADMAP-ARCHIVE.md#phase-13--thread-per-core--memory-operations) and the
two measurement sections in the archive. Stage 2f.1 (Tantivy merge thread control) is done.
2f.2 and 2f.3 remain, and they are no longer blocked behind the worker-width work.

**The evidence against more *pinning* got stronger, not weaker.** Two independent measurements
say it does not pay, so 2f.2 should not be attempted without a specific hypothesis neither of
them covers — the defect it fixes, below, is that hypothesis. Per-arena jemalloc stats are
worth having on their own account.

### B1 — 2f.2 — CPU arenas for write / read / merge

📋 **Planned** — analysed 2026-08-08, not implemented. Verified still absent 2026-08-26:
the server crate has a `CoreLayout` but no `sched_setaffinity` call anywhere.

Design investigation, measured on Linux (aarch64 container, 8 cores, 4 shards × 4 indexes,
all affinity flags on). The observations below come from `Cpus_allowed_list` in
`/proc/<pid>/task/*/status`, not from what the process reports about itself.

**The defect this fixes already exists, and enabling `writer_core_affinity` is what causes
it.** Linux threads inherit their creator's affinity mask, and tantivy spawns its threads
from whichever thread happens to build the `IndexWriter` or drive the commit:

| Index created by | `merge_thread_*` | `segment_updater` | `thrd-tantivy-index*` |
|---|---|---|---|
| `PUT _config` (unpinned thread) | `0-7` | `0-7` | **single core** |
| a write (pinned writer thread) | **single core** | **single core** | **single core** |

So an index created by writing to it — the normal path — gets its two merge threads confined
to the *same single core as the writer they contend with*, making `merge_num_threads = 2` two
threads timesharing one core. Indexer threads are confined in both cases, because
`prepare_commit` calls `add_indexing_worker` on every commit and that runs on our pinned
writer thread. Nothing in CameoDB asks for this; it is inheritance, unnoticed.

**Mechanism available.** Tantivy builds its merge pool via `ThreadPoolBuilder` with no
`start_handler` and exposes no hook to supply one, so those threads cannot be pinned
directly. Inheritance is the lever, and it suffices because we own both creation sites:
`IndexWriter` construction (spawns `segment_updater` + merge pool eagerly) and
`prepare_commit` (respawns indexer threads). Set the creating thread's mask, create, restore.
`core_affinity::set_for_current` takes a single core and cannot express a set, so this needs
`libc::sched_setaffinity` with `CPU_SET` — `libc` is already a direct dependency of the
server crate.

**Proposed layout.** `CoreLayout` splits into two disjoint sets, sized from config with
`0 = auto`: a **write arena** of `clamp(local_shards, 1, cores - max(1, cores/4))` cores (one
core per shard is the ceiling that matters — a shard's writes serialise on one writer
thread), and a **read/merge arena** of the remainder.

| Threads | Arena | How |
|---|---|---|
| `orch-worker-N`, `writer-shard-*` | write, single core each | as today, indexed within the arena |
| `thrd-tantivy-index*` | write arena (all its cores) | widen the writer's mask around `commit()`, restore after |
| `merge_thread_*`, `segment_updater` | read/merge arena | set mask before constructing the `IndexWriter`, restore after |
| `cameodb-read` blocking pool | read arena | tokio `on_thread_start` |
| `warmup-shard-*` | read arena | at thread start |
| global rayon | read arena | build the global pool explicitly with a `start_handler` |

**Oversubscription is the point, not a limitation.** Arenas are affinity *masks*, not
reservations, so `shards × indexes` threads share an arena's cores and the kernel timeshares
them. What an arena guarantees is the negative: nothing in it can preempt a writer core.
Note that arenas bound *where* threads run, not *how many* — thread count is
`shards × open_indexes × (1 + merge_num_threads + indexer_num_threads)`, which was 64 tantivy
threads (98 total) at 4 shards × 4 indexes. The only lever on the count is
`merge_num_threads`; tantivy has no shared merge executor across `IndexWriter`s.

**Expected impact.** Certain: removes the confinement above, restoring the meaning of
`merge_num_threads`. Likely: better write tail latency under merge pressure, since compaction
can no longer preempt a writer mid-commit. Uncertain and deliberately not predicted: whether
disjoint arenas beat free OS scheduling on aggregate throughput — reserving cores leaves some
idle at low load while others queue, which typically helps p99 and can cost p50. That
uncertainty is why the latency harness comes first.

**Bonus for 2f.3.** With `percpu_arena:percpu`, confining thread populations to disjoint core
sets also separates their jemalloc arenas, making per-arena stats attributable to write
versus read work instead of an undifferentiated total.

**The hypothesis this asked for now exists.** `delete_term` reclaims no bytes until a merge
rewrites the segment, so a delete-heavy index is the workload whose throughput is gated by merge
capacity — the one case where two merge threads timesharing the writer's own core is the binding
constraint rather than a curiosity. [Phase 17](docs/ROADMAP-ARCHIVE.md#phase-17--record-deletion--done) shipped the
operation; a delete-heavy arm in the load generator is what would falsify or confirm this.

**Risks.** Linux-only; macOS keeps the current no-op, so the platforms genuinely differ. Mask
save/restore around `IndexWriter` creation needs a drop guard. Below ~4 cores the split
degenerates and should be disabled. It depends on tantivy internals (eager pool construction,
commit-time worker respawn) that are not part of its public contract, so the `/proc` affinity
check should become a Linux validation-suite check rather than a one-off.

### B2 — 2f.3 — per-arena jemalloc stats

📋 **Planned.** Verified still absent 2026-08-26: `admin/memory.rs` calls `mallctl` only
for the two purge control names.

- `read_jemalloc_stats()` currently reads global stats only
- With `percpu_arena:percpu`, expose per-arena stats via `mallctl("arena.i.allocated", ...)` and `mallctl("arena.i.resident", ...)`
- Useful for diagnosing which shard/core is consuming the most memory
- Requires 2f.2's core sets to map arena IDs to shard/core

---

## C. Phase 14 — Security Hardening ◐ Partial

A1–A5, B1–B3, C1 and C2 are done and verified by `scripts/validate/` — see
[Phase 14 — what landed](docs/ROADMAP-ARCHIVE.md#phase-14--security-hardening). C3 is the only stage
still open, and it shrank because B1 absorbed index scoping.

### C1 — Per-index role overrides

📋 **Planned** · Phase 14 Stage C3 · ~2 days (was ~5+) · impact Medium · risk if unfixed:
multi-tenant isolation. The only security stage left, and it matters only for multi-tenant
deployments.

- Most of what this stage originally described is B1's scoping mechanism, which now applies to
  every role. What remains is *overrides*: a key with `role = "writer"` granted read-only on a
  named sensitive index — per-index capability **subtraction** rather than a second allow-list
- Enforced at B1's ingress chokepoint, **not** at the `RouterActor` boundary as first
  sketched — that boundary is also driven by cluster peers over kameo, where no API key exists
  (see the trust boundary note in B1)
- Depends on B1's capability model and route classification table
- Verified 2026-08-26: `auth.rs` carries `allowed_indexes` (B1's allow-list) and nothing that
  subtracts from it

### C2 — Query complexity caps

💭 **Deferred, not planned.** Max boolean clauses, max prefix-expansion terms, and wiring the
existing per-request timeout into the MCP path.

Judged unnecessary at this stage of the auth module: rate limiting already bounds what a key
costs the node per unit time, which is the resource-exhaustion risk C1 was written for, and a
per-request timeout already exists on the HTTP path. A single expensive query is a different
and much narrower problem than a loop of ordinary ones. Revisit if a workload appears where
one query — not a stream of them — is the threat; the two `parse_query_lenient` call sites in
`crates/storage/src/lib.rs` are where a cap would go.

- ✅ [**C3 — Fail closed on unauthenticated `internal`**](docs/ROADMAP-ARCHIVE.md#c3--fail-closed-on-unauthenticated-internal)
- ✅ [**C4 — The admin API reachable off-box unauthenticated**](docs/ROADMAP-ARCHIVE.md#c4--the-admin-api-reachable-off-box-unauthenticated)
- ✅ [**C5 — A decompression cap on the streaming ingest path**](docs/ROADMAP-ARCHIVE.md#c5--a-decompression-cap-on-the-streaming-ingest-path)
- ✅ [**C7 — A `500` printed the node's internal error text**](docs/ROADMAP-ARCHIVE.md#c7--a-500-printed-the-nodes-internal-error-text)
- ✅ [**C6 — Redact the cluster PSK in `Debug`**](docs/ROADMAP-ARCHIVE.md#c6--redact-the-cluster-psk-in-debug)
- ✅ [**C8 — REST has no rate limit, and anonymous MCP callers share one bucket**](docs/ROADMAP-ARCHIVE.md#c8--rest-has-no-rate-limit-and-anonymous-mcp-callers-share-one-bucket)

---

## D. Phase 15 — High Availability: Reindex, Replication & Migration 📋 Planned

**Objective**: The operational features a deployment needs once data outlives the shape it was
written in. Scoped 2026-08-15 as enterprise/HA work, separate from the MCP completion track it
was briefly filed under — nothing here is surfaced by an MCP tool, and none of it blocks one.

**Audience**: enterprise and multi-tenant deployments. A single-node local deployment gains
nothing from D2 and D3, and D1 has a documented workaround — declare fields up front, or
`delete_index_data(delete_schema = false)` and re-ingest — that
[A2](#a2--the-documentation-pass) is responsible for teaching.

Stages are provisional and not yet ordered beyond their dependencies.

### D1 — Reindex

📋 **Planned.** Rebuild an index so a late-discovered field can be queried.

Filed 2026-08-15 out of MCP completion track item 1, which established that this is the *only*
way to make such a field reachable and that no path for it exists —
`create_schema_from_definition` has exactly one caller, the branch that creates an index that
is not there yet. Verified still true 2026-08-26.

The shape of the work: drop the writer, rebuild the Tantivy index from the documents redb
already holds under a schema that declares the field, per shard, with the original left intact
until the replacement is complete.

**Why it is here and not in the MCP track.** It moved out of that track on 2026-08-15: it is
engine work — reindex, alongside replication and migration — rather than MCP work; the MCP
side of the gap is already honest about it (`pending_reindex`, refused searches), and the
cheap mitigation that already exists — declare the fields up front with
`PUT /api/{index}/_config`, or let the first write carry them — belongs in
[A2](#a2--the-documentation-pass)'s index-design guidance.

**A second use, added 2026-10-06: a schema change on an index that holds documents.** `PUT
/_config` now compares the built columns (`rebuild_changes`) and decides once for the whole
cluster. A change the built index would act against — a retype such as `i64` to `f64`, another
tokenizer, another `id_fields` — rebuilds an empty index on every node, but on a populated one it
is refused with `409`, and the way through is to delete the documents and load the source again.
Reindex would apply it in place: rebuild each shard from the documents redb holds under the new
schema, reporting the documents whose values the new types cannot hold, driven through the same
two-phase `PrepareSchema`/`ApplySchema` round so every node rebuilds together.

Until it exists, "the index knows about this field" and "you can search on it" stay permanently
different states for anything discovered after creation. The engine already reports the gap
honestly (`pending_reindex`, discarded-clause refusals), so this stage turns an explained gap
into a closed one.

**Two other gaps close with it**, both reported rather than hidden today:

- A **text field cannot be made `sortable`** after its index holds data — the string fast
  column is written at index time and `PATCH /_schema` edits `indexed` only. Opened by
  completion track item 4.
- A **numeric field declared `fast` after its index was built** has nothing to order by, and
  only the built index knows that. Opened 2026-08-19 by the sort-refusal work.

### D2 — Replication

📋 **Planned.** Documents copied beyond their primary shard placement, so a lost node loses
availability rather than data. Depends on [D1](#d1--reindex) for catch-up: a rebuilt replica is
the same operation as a rebuilt index, from a different source. This is the durability gap the
0.3.3 stability audit records as finding 08 — today the Kademlia record puts use `Quorum::One`
(`swarm/behaviour.rs` 138, 198, 235), so a lost node loses its shards' data outright.

### D3 — Migration

📋 **Planned.** Moving shard ownership between nodes without downtime — placement change,
catch-up replication, cutover. Depends on both stages above; it is replication with an end
state.

---

## E. Phase 16 — Boot & OOM Recovery at Scale ◐ Partial

The bridge between redb and Tantivy was rebuilt on 2026-08-19 rather than patched stage by
stage, which retired four of the six hot points outright — see
[Phase 16 — what landed](docs/ROADMAP-ARCHIVE.md#phase-16--boot--oom-recovery-at-scale) for the analysis,
the outcome table and Stage 7's write-path work. What remains:

### E1 — Stage 4.2 — a max-WAL-size commit trigger

⏭️ **Skipped 2026-10-15 — overtaken by [F9](docs/ROADMAP-ARCHIVE.md#f9--commit-on-a-clock-not-a-count).** Filed so a
bursty writer could not accumulate a large tail before the operation-count threshold fired, when
that threshold was the only bound on replay. F9 added the wall-clock term this entry found
missing: an index commits once its oldest uncommitted write has waited `commit_interval_ms`
(2 s), with 20× the count as a backstop, so the tail is bounded by time and by count. Measured
2026-10-15 after a `SIGKILL` mid-ingest: 1,708–20,713 entries per shard, replayed in 22–100 ms. A
byte-size term would bound the tail for very large documents only, and nothing has asked for it.

The original finding, 2026-08-26: `should_commit_writer` decided purely on
`operations_since_commit` against a memory-budget-scaled threshold, with no byte-size term and no
wall-clock term.

### E2 — Stage 3's deeper warming options

📋 **Planned.** Hot-set and field-scoped warming, if the 60-second warmup budget proves too
blunt at 30 TB. The budget shipped; these are the options it defers, not a replacement for it.

### E3 — Measure recovery on the reporting node

📋 **Planned.** The change is verified by the storage suite and by construction; the
30 TB / 16-shard figures in the archived success metrics are still the target, not a result.
Nothing else in this phase should be called finished before this runs.

**Measured on the development machine, 2026-10-15** (M5 Pro, 4 shards, ~1 GB of redb each, file in
the page cache, `SIGKILL` mid-ingest): the WAL replay is 22–100 ms per shard; redb's repair of the
file was 389–665 ms per shard, and 2.1–2.5 s once the files had grown, against 12–18 ms with
`quick_repair` — see [E6](docs/ROADMAP-ARCHIVE.md#e6--a-crash-restart-repaired-every-shards-redb-file). Run on the
reporting node, the measurement needs both arms: a crash restart with `quick_repair = true` and
with `false`, read from disk rather than the page cache, and the single-write and bulk throughput
each setting gives — the second flush per commit is the cost that decides the default there.

### E4 — Two compatibility paths with no end-to-end test

📋 **Planned.** Both from Stage 7, and both unbuildable in-repo as things stand:

- **An index built by an older build, opened by this one.** The compatibility path is real and
  exercised in the decoder unit tests, but `create_schema_from_definition` no longer declares
  `_seq`, so there is no longer any way to *create* a legacy-shaped index to open. Verifying it
  needs a checked-in fixture index or a build-flag seam.
- **A legacy WAL tail replaying end to end**, for the same reason. `decode_wal_entry` is unit
  tested against both formats, and the replay body above it is format-agnostic by construction.

- ✅ [**E5 — A cap on open index writers**](docs/ROADMAP-ARCHIVE.md#e5--a-cap-on-open-index-writers)
- ✅ [**E6 — A crash restart repaired every shard's redb file**](docs/ROADMAP-ARCHIVE.md#e6--a-crash-restart-repaired-every-shards-redb-file)

---

## F. Engine and performance, not filed under a phase

### F5 — Concurrency sweep, measured 2026-09-02

`cameodb-bench` against a standalone release node on an Apple M5 Pro (5 performance + 10
efficiency cores, 24 GB), 4 shards, 8 workers, no pinning, `wal_sync` on, 5,000 seed documents,
5s warmup and 20s measured per point. **Closed-loop, and the harness shares the machine with the
node**, so the top of the sweep measures the pair and not the server alone.

**Search — saturates at 12, and 16 is worse than free.**

| conc | ok/s | p50 | p90 | p99 | max |
|---|---|---|---|---|---|
| 4 | 13,819 | 284µs | 339µs | 394µs | 1.50ms |
| 8 | 22,388 | 348µs | 423µs | 508µs | 2.07ms |
| 12 | 26,916 | 437µs | 510µs | 617µs | **15.11ms** |
| 16 | 26,920 | 586µs | 661µs | 772µs | 10.17ms |

12 → 16 adds **4 ops/s** and 34% to p50. The scaling efficiency is +62%, +20%, then 0%. Note
`search_threads` defaults to 8 and throughput still improves to 12, so the read pool is not the
only thing absorbing load — but nothing absorbs anything past it. The `max` at c=12 is the
figure to explain: 15ms against a 617µs p99.

**Bulk — throughput is invariant under concurrency, and latency scales exactly with it.**

| conc | req/s | docs/s | p50 | p99 | µs per doc |
|---|---|---|---|---|---|
| 4 | 20 | 12,580 | 151.5ms | 294.5ms | 303 |
| 8 | 20 | 12,363 | 311.5ms | 430.0ms | 623 |
| 12 | 20 | 12,456 | 466.4ms | 671.9ms | 933 |
| 16 | 21 | 12,264 | 647.4ms | 790.9ms | 1,295 |

**This is [CH12](docs/ROADMAP-ARCHIVE.md#ch12--write-path-serialization-and-round-trip-waste)'s fourth bullet, measured.**
Four times the clients moves the same documents per second and multiplies latency by 4.3 — the
signature of a hard serialization point, not of a saturated resource. A saturated resource still
shows *some* gain. The worker-pool report confirms where: `jobs per worker: [0 × 8]` and every
dispatch counter zero, because a `BulkWrite` never reaches the pool and holds the orchestrator
mailbox for its whole duration. **Concurrency is not a tuning knob for bulk on this build; 4 is
as fast as 16 and four times quicker to answer.**

**Single writes — commit-bound, which is [F1](#f1--the-cost-of-a-durable-commit-under-read-load).**

| conc | ok/s | p50 | p99 | max |
|---|---|---|---|---|
| 4 | 289 | 13.04ms | 25.90ms | 143.29ms |
| 8 | 403 | 19.08ms | 33.99ms | 179.02ms |
| 12 | 502 | 23.03ms | 64.31ms | 271.87ms |
| 16 | 640 | 24.48ms | **100.45ms** | 205.28ms |

Each write is its own durable commit across 4 shards. Throughput doubles from 4 to 16 while p99
quadruples. Serially, at concurrency 1, the same write is 4.0ms p50 — so most of what is measured
here is queueing behind other commits, and the growing `max` is commit and merge stalls.

**Batching is worth 19x.** 640 docs/s as single writes at c=16 against 12,264 docs/s in bulk.

**Mixed — 8 is the operating point; past it reads pay and writes do not gain.**

| conc (= 2N in flight) | search ok/s | search p50 | write ok/s | write p50 |
|---|---|---|---|---|
| 4 | 14,299 | 274µs | 261 | 14.90ms |
| 8 | 21,724 | 360µs | 376 | 20.04ms |
| 12 | 21,789 | 546µs | 497 | 24.06ms |
| 16 | 19,895 | 799µs | 622 | 24.96ms |

`--concurrency N` spawns N workers **per workload**, so these rows are 2N requests in flight.
Reads are barely hurt by concurrent writes at c=8 — 21,724 against 22,388 search-only, a 3% loss
— and that is the useful result: the read pool and the shard writers are not fighting at that
level. At c=12 and above search throughput stops improving and then *falls*, while search-only was
still climbing to 26,916.

**The operating point: 8.** Highest search throughput before the tail opens up, 3% read cost under
concurrent writes, and the point at which every extra client is queueing rather than working. For
bulk specifically, lower — concurrency buys nothing there at all.

**Not measured, and it matters:** every figure here is closed-loop service time at a fixed
concurrency. None of it is an arrival-rate SLA.
[F2](docs/ROADMAP-ARCHIVE.md#f2--an-open-loop-load-generator) landed on 2026-09-15 and can produce one; this sweep has
not been re-run against it, and the numbers above stay as they were taken.

**Re-measured 2026-09-06 on the unwind build, and the figures above stand.** Every number in this
sweep was taken on a `panic = "abort"` binary, three days before `232fa9d` set the release profile
to `unwind` — so as written they described a build that no longer ships. Re-running against
today's tree settles what that change cost.

Both profiles were built from the same commit and run **interleaved** — unwind, abort, unwind,
abort — so thermal drift and background load fall on both equally; 3 repeats of the four search
points plus the c=16 write point, same 4 shards, 8 search threads, `wal_sync` on and 5,000 seed
documents as above. Median of 3, with the observed range:

| search conc | unwind ok/s | abort ok/s | delta | ranges |
|---|---|---|---|---|
| 4 | 13,754 [13,562–13,781] | 13,696 [13,546–13,698] | +0.4% | overlapping |
| 8 | 21,790 [21,180–21,898] | 21,097 [20,772–21,447] | +3.3% | overlapping |
| 12 | 26,679 [23,628–27,647] | 24,867 [23,159–26,506] | +7.3% | overlapping |
| 16 | 27,843 [22,972–27,853] | 25,833 [22,019–27,160] | +7.8% | overlapping |
| write, 16 | 620, p50 24.89ms | 624, p50 24.90ms | −0.6% | overlapping |

**No comparison separates, and the direction is the reading.** Every nominal delta favours unwind,
which cannot be a real effect — unwinding tables can only cost. A genuine penalty would show as a
consistent deficit and there is none, so the honest conclusion is *no measurable cost*, not that
unwind is faster. Trust c=4 and c=8, where the spread is ±1.6%; at c=12 and c=16 it opens to ±10%
because the harness shares cores with the node, which is the same caveat this sweep opens with.

The cross-check that makes the rest of it credible: **today's abort build reproduces the
2026-09-02 sweep** — 13,696 against 13,819 at c=4, and the write point at 620–624 ops/s, 24.9ms
p50 and ~100ms p99 against 640, 24.48ms and 100.45ms. Four days and one profile change later, the
harness lands in the same place, so the numbers in this section are reproducible rather than a
single lucky afternoon.

What unwinding actually costs is **4,581,024 bytes** of binary — 24,300,288 against 19,719,264,
measured on the two binaries this comparison used.

### F6 — What fsync actually costs, measured 2026-09-02

The same sweep as [F5](#f5--concurrency-sweep-measured-2026-09-02) with `[storage] wal_sync =
false`, which sets redb's durability to `None` (the banner reports `Durability: Eventual`). Same
node, same binary, same corpus. **Search-only is the control**, because a read never fsyncs:
22,388 → 22,472 ops/s and p50 348µs → 346µs, so the write arms below are not reading machine
noise.

**A single durable write is almost entirely fsync.**

| conc | ok/s on | ok/s off | p50 on | p50 off | p99 on | p99 off |
|---|---|---|---|---|---|---|
| 4 | 289 | **13,267** | 13.0ms | **140µs** | 25.9ms | 252µs |
| 8 | 403 | 14,910 | 19.1ms | 218µs | 34.0ms | 370µs |
| 12 | 502 | 15,926 | 23.0ms | 255µs | 64.3ms | 27.8ms |
| 16 | 640 | 15,699 | 24.5ms | 277µs | 100.5ms | 38.7ms |

46x the throughput and 93x the latency at c=4. Each single write is its own commit, so this is
one APFS fsync per operation and it is essentially the whole cost. The tail does *not* go away —
p99 at c=12/16 is still 28–39ms against a 277µs p50 — and those are Tantivy commit and merge
stalls, which durability has nothing to do with.

**Bulk is only ~30% fsync. The rest is [CH12](docs/ROADMAP-ARCHIVE.md#ch12--write-path-serialization-and-round-trip-waste).**

| conc | docs/s on | docs/s off | gain | p50 on | p50 off |
|---|---|---|---|---|---|
| 4 | 12,580 | 16,106 | +28% | 151.5ms | 114.0ms |
| 8 | 12,363 | 16,236 | +31% | 311.5ms | 238.0ms |
| 12 | 12,456 | 16,041 | +29% | 466.4ms | 359.9ms |
| 16 | 12,264 | 16,086 | +31% | 647.4ms | 486.9ms |

**This is the finding worth acting on.** With the disk taken out of the picture bulk is *still*
flat across concurrency — 16,106 / 16,236 / 16,041 / 16,086 — and its latency still scales
exactly with the client count. So the ceiling on bulk ingestion is not durability; it is the
orchestrator mailbox that a `BulkWrite` holds for its whole duration. Fixing that is worth more
than 30% and costs no safety, where `wal_sync = false` buys 30% and costs the crash-recovery
guarantee.

**And it is not a free speedup — it moves the cost to readers.**

| conc | search on | search off | write on | write off |
|---|---|---|---|---|
| 4 | 14,299 | **4,680** | 261 | 12,143 |
| 8 | 21,724 | **3,872** | 376 | 14,720 |
| 12 | 21,789 | 3,599 | 497 | 15,238 |
| 16 | 19,895 | 3,478 | 622 | 15,589 |

Search throughput in a mixed workload **falls by 82%** and p50 goes 360µs → 1.7ms. Nothing about
reads got slower; the writes got 40x faster, and 14,720 writes/s of index churn is 40x the
Tantivy commits and segment merges competing with the searches. The durable configuration is not
"slow writes" — it is a rate limiter on write-side interference that readers were benefiting
from.

So the honest summary of the flag: it converts a disk cost into a CPU cost and hands the bill to
whoever is reading. For a bulk load with nothing else running, that is exactly the trade you
want, which is what `docs/CONFIGURATION.md` already says. For a node serving queries it is a
regression dressed as a tuning knob.

### F1 — The cost of a durable commit under read load

💭 **Deferred 2026-09-16, deliberately: this node stays durable.** The measurements below stand
and the lever is real; what changed is that the lever this entry names does not exist any more,
and the thing that would replace it trades away the guarantee the project has chosen to keep.

*The middle durability level cannot be selected, only built.* This entry asks for "a durability
level between every commit and none". `wal_sync` maps onto redb's `Durability`
(`storage/src/lib.rs` ~5288), and **redb 4.1 has exactly two variants, `None` and `Immediate`** —
the `Eventual` level that existed in redb 2.x is gone. So there is nothing to select.

*What building it would look like, recorded so it is not re-derived.* redb's own documentation
gives the mechanism: a commit at `None` "will not be persisted to disk unless followed by a
commit with `Durability::Immediate`". So commit `None` normally and have a ticker issue an
`Immediate` commit every N ms — group commit at the fsync layer, with a bounded loss window and
one fsync amortised over everything inside it.

*And why that is not simply `wal_sync = false` with a shorter fuse.* The hazard this project
records against turning the sync off is not lost writes, it is divergence: a crash "can leave the
search index holding documents the document store lost, which recovery cannot repair". A timed
flush narrows that window without closing it, because a tantivy commit fires on its own threshold
or its 5s idle timer and can publish a segment whose documents redb has not yet fsynced. Making
the middle level *safe* therefore means forcing an `Immediate` redb flush before every tantivy
commit, so the store is never behind the index. That ordering is the whole design; without it the
feature is a nicer-sounding way to reach the same corruption.

**Not being built.** A bounded loss window is a different product promise, and the one this node
makes — a commit that returns is on disk — is worth more than the fsync it costs. Revisit only
with a deployment that has asked for the trade by name.

What the rejected linger was meant to paper over, still the reason this entry exists: a commit costs
~12.5ms with searches running against ~4.6ms without, and `wal_sync = false` recovers +86% of
write throughput. The lever is **the fsync itself** — WAL device and placement, or a durability
level between "every commit" and "none" — not how the writer groups writes.

**The +86% is hardware-specific and understates it badly on some machines**, measured
2026-09-02 — see [F6](#f6--what-fsync-actually-costs-measured-2026-09-02). On an M5 Pro over
APFS, turning `wal_sync` off took a single write from 13.0ms to **140µs** at p50 and its
throughput from 289 to 13,267 ops/s: the fsync was ~99% of the operation, not 46% of it. The
figure to quote is the *shape* — fsync dominates a single durable write — and not the
percentage, which belongs to the disk it was taken on.

The reasoning is in [Mixed read/write load, measured](docs/ROADMAP-ARCHIVE.md#mixed-readwrite-load-measured), which
also records why the linger cannot be rebuilt from that reasoning alone.

- ✅ [**F2 — An open-loop load generator**](docs/ROADMAP-ARCHIVE.md#f2--an-open-loop-load-generator)

### F3 — Take unkeyed searches off the coordinator

◐ **The standalone half is done** 2026-09-02. **The clustered half is turned down** 2026-09-16,
on inspection rather than measurement, and the reason is worth more than the hop it would save.

*The node count is genuinely not published.* `ConsistentRing::len()` returns the number of vnode
tokens, not of nodes — each node contributes many — so nothing in the published state answers
"how many nodes are there". (It was being logged as `ring_nodes` on every topology update, which
reads as a cluster size and is not one. Corrected to `ring_tokens`.) Publishing a real count
alongside the ring is a small change.

*What stops it is that the ring lags the authority.* The coordinator owns `expected_nodes` and
publishes the ring through a `SubscribeTopology` channel and a fire-and-forget `tell`
(`main.rs` ~604), so the router's copy is behind the coordinator's own state by a channel hop and
an actor message. Asking the coordinator — what a keyless operation does today — reads the
authority; reading a published count reads a copy that is briefly stale.

For a keyed operation that is harmless: a wrong answer is a forward, and
[OB3](docs/ROADMAP-ARCHIVE.md#ob3--a-single-write-or-delete-can-land-on-the-wrong-shard) bounds it. For a **keyless**
one it is not, and keyless is the case this item exists for. A scatter-gather that believes the
cluster is one node returns only local results — no error, no partial flag, just a short answer —
for the window between a peer becoming live and the ring arriving. That is the failure shape
[OB8](docs/ROADMAP-ARCHIVE.md#ob8--a-paged-search-loses-its-page-on-the-streaming-fan-out) and
[OB9](docs/ROADMAP-ARCHIVE.md#ob9--a-bulk-delete-drops-a-peers-per-id-errors) were opened for.

The saving is one mailbox hop per keyless operation on a clustered node, and
[F3's own measurement](#f3--take-unkeyed-searches-off-the-coordinator) put that at a
single-request tail improvement and nothing at concurrency. **Not worth a window where a search
silently under-reports.** The standalone shortcut stays because `clustered = false` is static
configuration: a peer can never appear, so there is no window to be stale in. Revisit only with a
topology version the router can check cheaply enough to make the shortcut provably current.

`resolve_local` opened with `let key = routing_key?`, so every *keyless* operation asked the
coordinator. Counted on a standalone release build against a 2,000-document index — by grepping
the coordinator's own `RouteOperation` debug line, so this is asks and not inference:

| operation | coordinator asks before | after |
|---|---|---|
| search | **1 per search** | 0 |
| streaming search | **1 per search** | 0 |
| `GET /_indexes` | **1 per call** | 0 |
| single write | 0 | 0 |
| bulk write / delete | 0 route asks (but see [F4](docs/ROADMAP-ARCHIVE.md#f4--the-bulk-paths-asked-the-coordinator-before-they-knew-they-needed-to)) | 0 |

A search has no routing key to resolve, so *every* search took the ask — and the answer could
only ever be `Local`, because `decide_route` has one node in `expected_nodes`. `NodeConfig`
already grew `clustered` for a5ed2ff's schema gate, and it answers the same question statically:
a node with clustering off will never have a peer. `RouterActor` now reads it and takes the local
arm for keyed and keyless alike. Pinned by `a_lone_node_can_only_ever_be_routed_to_locally`, in
the coordinator's own tests, because the claim being relied on is `decide_route`'s and not the
router's.

**What it is worth, measured rather than assumed.** Release build, 2,000-document index,
five repeats after the change to establish the spread:

| | before | after (5 runs) | outside the spread? |
|---|---|---|---|
| search p50, c=1 | 0.358ms | 0.353–0.357ms | marginally |
| search p99, c=1 | 0.504ms | 0.402–0.476ms | **yes** |
| search p50, c=16 | 3.276ms | 3.249–3.300ms | no |
| search p99, c=16 | 5.924ms | 5.806–6.073ms | no |
| search throughput, c=16 | 4,489 ops/s | 4,439–4,519 ops/s | no |

So: a real improvement to the **single-request tail**, and nothing at concurrency, where the read
pool's eight search threads are the constraint and a removed mailbox hop disappears into
queueing. It is not a throughput change and should not be sold as one. What is unambiguous is the
work removed — 150 coordinator asks over 250 operations, down to **zero** — and that a standalone
node no longer consults a cluster it does not have, which is now structural rather than
incidental.

- ✅ [**F4 — The bulk paths asked the coordinator before they knew they needed to**](docs/ROADMAP-ARCHIVE.md#f4--the-bulk-paths-asked-the-coordinator-before-they-knew-they-needed-to)

### F7 — The request timeout sheds the client, not the work

⚠️ **Confirmed by measurement** 2026-09-15 (0.3.3 stability audit, finding 05), and worse than
it was described. The mechanism was reasoned about correctly; what the reasoning understated is
the size of the effect. **Goodput does not degrade under this failure. It goes to zero.**

The mechanism, unchanged: searches run as blocking closures dispatched with `spawn_blocking` onto
the read pool, bounded by `max_blocking_threads` (`node_orchestrator.rs` 8341). Tokio never
cancels a blocking closure — dropping its `JoinHandle` neither stops work that has started nor
dequeues work that has not — so when `TimeoutLayer` (`routes.rs` 241) fires it drops the request
future and releases the concurrency permit while the Tantivy work behind it runs to completion.
Admission is capped (`max_concurrent_requests`); the backlog behind it is not.

**Measured**, with [F2](docs/ROADMAP-ARCHIVE.md#f2--an-open-loop-load-generator)'s open-loop generator, on two machines.
Release node and release harness co-located in both, a 200,000-document index, 4 shards,
`search_threads = 2`, `max_concurrent_requests = 3000`. Harness lag p99 stayed at or below 1.1ms
on every M1 arm and 608µs on every M5 arm, and no arm dropped an arrival or returned an INVALID
verdict, so these are node measurements and not the generator. The two columns are the same node,
the same index and the same offered load; the only thing that differs is the timeout.

**Apple M1** (4+4 cores, 16 GB):

| offered | `request_timeout_secs = 1` | `request_timeout_secs = 300` |
|---|---|---|
| 1,000/s | **1 ok/s**, 14,949 × 408 | **735 ok/s**, 3,930 × 503 |
| 2,000/s | ~0 ok/s, 29,872 × 408 | 733 ok/s, 18,885 × 503 |
| 3,000/s | ~0 ok/s, 44,466 × 408 | 729 ok/s, 34,232 × 503 |
| 4,000/s | ~0 ok/s, 45,000 × 408 | 729 ok/s, 49,277 × 503 |

**Apple M5 Pro** (5 performance + 10 efficiency cores, 24 GB) — the machine
[F5](#f5--concurrency-sweep-measured-2026-09-02)'s sweep was taken on — same protocol, re-run
2026-09-15:

| offered | `request_timeout_secs = 1` | `request_timeout_secs = 300` |
|---|---|---|
| 1,000/s | **233 ok/s**, 15,304 × 408 | **995 ok/s**, 73 × 503 |
| 2,000/s | 1 ok/s, 39,990 × 408 | 993 ok/s, 20,137 × 503 |
| 3,000/s | ~0 ok/s, 59,191 × 408, 1,018 × 503 | 990 ok/s, 40,400 × 503 |
| 4,000/s | ~0 ok/s, 60,000 × 408, 20,210 × 503 | 988 ok/s, 60,452 × 503 |

Real capacity is ~730 searches/s on the M1 and ~990/s on the M5, and each control column delivers
its own capacity at every level of overload — flat, with the excess refused cleanly as 503. That
is textbook graceful degradation. The left column is the same machine doing the same work and
delivering **none of it**, because every search it completes belongs to a client the timeout
abandoned while it queued. The node is not slow and it is not idle. It is fully busy producing
answers nobody is left to receive.

**A third more capacity buys no resistance to it.** The M5 reaches the same zero, one step
later: its 1,000/s arm still clears 233 ok/s where the M1's was already at 1, and by 2,000/s the
two are indistinguishable. What the extra capacity moves is the offered rate at which the collapse
completes, not whether it completes — which follows from the regime condition below being a ratio,
not a rate.

The per-second series added for this measurement shows the crossover directly, on the M5 arm at
1,000/s offered with a 1s timeout. Successes fall as timeouts climb, at a fixed offered rate:

```
         ok/s  1045 1005 1008 1012  589    0    0    0    0    0 …
408 timeout/s     0    0    0    0  397  933 1006 1000 1031  977 …
```

**It recovers, but not promptly.** Dropping from 3,000/s to 300/s — 41% of M1 capacity — left
goodput at zero for a further **12 seconds**, with 408s continuing at the full offered rate,
before it returned:

```
ok/s       0 0 0 0 0 0 0 0 0 0 0 0 51 303 287 314 251 277 292
```

So it is not permanently metastable: the backlog drains at `capacity − offered`, and the dead
period scales with how deep admission lets the backlog get.

**The M5 shortens the dead period in the direction that reading predicts.** The same 3,000/s →
300/s drop left goodput at zero for **4 seconds**, not 12:

```
         ok/s    0    0    0    0  149  284  308  277  317  307 …
408 timeout/s  310  303  337  293  173    0    0    0    0    0 …
```

Admission caps the backlog at 3,000 requests on both machines, so the drain is
`3000 / (capacity − offered)`: 3000/(730−300) ≈ 7s against 3000/(990−300) ≈ 4.3s. The M1 took
longer than its own arithmetic predicts and the M5 landed on it, which puts the dead period in the
region the model describes without making the model exact. The operational point is unchanged
and now has a second data point: recovery time is set by admission depth, so the same knob that
creates the exposure also decides how long a node stays dark after the load relents.

**What decides whether a node is exposed.** The regime is entered when
`max_concurrent_requests / service_rate > request_timeout_secs`. At the 128 default against
~730/s that is 175ms versus an effective **60s** — three orders of magnitude of headroom, which
is why no default deployment has ever seen this and why the audit could only reason about it.

*The timeout a default node actually runs is 60s, not the 30s this document said.*
`effective_request_timeout_secs` (`config.rs` 1067) honours `request_timeout_secs` only when it
**differs** from the default of 30, and otherwise derives `max(60, max_record_size_mb / 10)`. So
the literal value 30 is indistinguishable from leaving the key unset, and both resolve to 60 for
any `max_record_size_mb` below 600 — so, in practice, always. `cameodb check-config` prints the
resolved figure and was the tell. This widens the headroom rather than narrowing it, but the condition above is written in
terms of a number the configuration file cannot currently express, which is worth knowing before
anyone tunes against it.

Both machines confirm the headroom, and the M5 widens it. The M1 with a 5,000-document index and
8 read threads (capacity ~9,300/s) produced **zero 408s across 1.7M requests** at up to 16,000/s
offered, peaking at 9,697 ok/s and still serving 8,852 ok/s under 60% overload. The M5 at the same
settings has a knee at **~26,500/s** — 16,000/s is not yet overload for it, sustained with 16
requests in flight against the admission ceiling of 128 — and produced **zero 408s across
3,658,940 requests** offered up to 50,000/s, holding 24,385 ok/s while refusing 51% as 503.

The danger is reached by *raising* `max_concurrent_requests`, which is exactly what an operator
does when they start seeing 503s.

**Fix 1 — reject at dequeue. ✅ Done 2026-09-15**, and the implementation corrected the entry
twice. Both corrections came from measurement, and both are worth keeping.

*The queue is not where this said it was.* The entry blamed `spawn_blocking`, so the first
attempt stamped a deadline in `dispatch_read_pool`. It shed **nothing** — `read_pool_abandoned`
stayed at 0 through a full collapse. The backlog forms one layer up, in the orchestrator
worker channel (`OrchestratorJob`, 512 per worker), and the read pool is downstream of an
admission gate that keeps it shallow. The check belongs at `rx.recv()` in the worker loop.

*Rejecting **at** the deadline is not enough.* Moved to the worker queue, the check fired
hard — 11,656 jobs refused — and goodput stayed at zero. A job admitted with 10ms of budget
left still needs its whole service time, so the queue simply settled onto the deadline and
every job that passed finished after its client had gone: 13,363 completed, all wasted. The
check has to reserve what the work costs, not merely what it has already spent. So the worker
keeps an EWMA of dequeue-to-answer per class of operation — a bulk write is not held to a
point search's service time — and admits only while `elapsed + 2 × estimate` fits the budget,
where `elapsed` runs from when the request *arrived*, not when it was dispatched: the body
read, decompress, parse and routing a large write already paid for are spent budget too.
Measured rather than configured: it moves with index size, shard count and load.

*The reserve has to be capped, and the cap is load-bearing.* Uncapped, an estimate above half
the budget makes the reserve exceed the budget outright — every job is refused, including one
that has waited no time at all, so nothing completes, nothing updates the estimate, and the node
refuses everything for good. That is F7's own metastable shape, reintroduced by its fix. Capped
at half the budget, a freshly arrived job is always admitted, which is what keeps the estimate
measured. The cap also turned out to be worth 44% of goodput on its own, the uncapped reserve
having been the more conservative of the two.

**Measured after, M1, same node, same 200k index, `request_timeout_secs = 1`:**

| offered | before | after |
|---|---|---|
| 300/s (under capacity) | — | 301 ok/s, p99 10.4ms, **0 shed**, 0 × 408 |
| 1,000/s | **1 ok/s**, 14,949 × 408 | **572 ok/s**, 8,516 × 503, **0 × 408** |
| 3,000/s | ~0 ok/s, 44,466 × 408 | **554 ok/s**, 44,077 × 503, 20 × 408 |

Goodput is flat under 3× overload instead of zero, essentially every refusal is a 503 a client
can act on rather than a 408 after a wasted second, and a node that keeps up is untouched — the
300/s arm sheds nothing and answers in 10ms at p99.

**It does not recover full capacity, and it is no longer perfectly clean.** The control column
delivers ~730/s where this delivers ~555/s, so the reserve still costs about a quarter of what
the node could serve; and at 3,000/s a 0.037% tail (20 of 54,071) still times out, against none
at the more conservative uncapped reserve. Both are the same dial. Whether the factor should be
tuned further, or replaced by a percentile of the service distribution rather than twice its
mean, is open and is the obvious next measurement. *That measurement was taken on the M5 and the
premise did not survive it — the reserve costs ~2% there, not a quarter; see the 2026-09-16 M5
run at the end of this entry.*

**Fix 2 — bound the backlog at admission. ✅ Done 2026-09-15**, and like fix 1 it corrected
the entry that asked for it — this time about why it was worth doing.

Admission counts requests, not the work they imply: `max_concurrent_requests` at 3,000 against
~500 searches/s is seconds of backlog against a one-second budget. `QueueLoad` turns the
accounting the pool already keeps — a single pool-wide `outstanding` counter, maintained where
jobs are handed to and leave the workers, and fix 1's service estimates — into Little's law,
and both the HTTP admission guard (`routes.rs`) and the dispatch path refuse against it before
a body is read. The door does not know the routed class, so it judges on the blended estimate;
the worker rechecks against its own. The estimate is the pool's own; nothing new is measured.

*The premise was wrong.* This entry said fix 2 was "the thing standing between ~555/s and
~730/s". It is not, and the ~730/s control it was measured against does not reproduce. Re-run
2026-09-15 on the same M1, same 200k index, same settings, the 300s-timeout control now serves
**417/s** in steady state where fix 1 alone serves **553/s** — fix 1 is *faster* than the
control, so the gap fix 2 was supposed to close is not there in the form described. What the
earlier column measured was a less-loaded machine, and the two sessions' absolute numbers are
not comparable. Only same-session comparisons below.

*And the difference it does make to throughput is below this machine's noise floor.* The same
binary run back to back at 1,000/s offered gave 533/s and 494/s in steady state — an 8% spread,
which is wider than every difference between fix 1 and fix 2 measured here. Anything at the
few-percent level on this box is not a result.

**Measured, M1, 200k index, `request_timeout_secs = 1`, `max_concurrent_requests = 3000`,
`search_threads = 2`. Steady-state ok/s, not the run mean — the mean is dominated by the
opening second, where an unbounded queue absorbs a burst it will answer far too late.**

| offered | control (300s timeout) | fix 1 | fix 2 |
|---|---|---|---|
| 300/s | — | 301 ok/s, p99 10.4ms, 0 shed | 299 ok/s, p99 14.4ms, **0 shed** |
| 1,000/s | 417/s, p50 **7,106ms** | 553/s, p50 880ms | 521/s (494–536), p50 836ms |
| 3,000/s | 410/s, p50 **7,314ms** | 425/s, p50 850ms | 410/s, p50 782ms |

So on throughput and latency fix 2 changes nothing measurable. What it changes is categorical:

- **Where the refusal happens.** Under fix 1 at 3,000/s the node refused 133,039 jobs at a
  worker and none at the door; under fix 2, 19,112 at the door against 144 at the worker. The
  dequeue check has become the backstop it was meant to be, and a refused request no longer
  pays for its body, its parse, a permit and a channel hop first. On this workload — searches
  with tiny bodies — that saving is not visible. On a write it is the body.
- **What the client is told.** `503 Overloaded: a 832ms backlog against a 1000ms request`
  instead of `read abandoned`, and every `503` this node raises now carries `Retry-After`. The
  admission guard always did; a refusal arriving through `AppError` did not, so the same
  condition advised the caller differently depending on which layer noticed it. Where the
  refusal is made on a predicted wait the header carries that wait, so a retry lands after the
  backlog rather than back inside it; where it is not, a second stands in.
- **A queue with no deadline check, closed.** A full worker queue used to divert to the actor
  mailbox: capacity 64, `ask` *waits* rather than failing, serialised through one task, and
  nothing on that path checks a budget — F7's mechanism intact in the overflow lane. It is
  refused instead. Worth knowing that it was never reached in any arm measured here, before or
  after: `actor_mailbox_fallbacks` stayed at 0 even at 4,000/s against 8,192 permits, because
  fix 1's dequeue refusal drains the worker queues faster than admission can fill them. This is
  a hole closed on the code, not on evidence of it being entered.
- **The node reports the number it is refusing on.** `queue_depth` and `predicted_wait_ms` in
  health, `dispatch.refused_at_admission` in `/_admin/workers`.

*A shallower queue is a slower one, which is why the target is the deadline and not a fixed
lookahead.* Bounding the backlog to one service round served 417/s, two rounds 443/s, and the
depth the deadline rule settles on ~521/s; the 3,000-deep control, 417/s. Throughput is not
monotonic in queue depth — too shallow starves the pool, too deep thrashes — and the deadline
rule lands near the optimum without a tuning constant. A door *weaker* than the worker was also
tried, refusing only at `predicted > budget` and leaving anything marginal to the worker: 424/s,
20% down and well outside the noise band, because letting the queue grow past what the worker
will accept only means refusing the same requests later.

**Recovery was already fixed by fix 1, not by this.** The 3,000/s → 300/s drop that once left
goodput at zero for 12 seconds now returns 310 ok/s in the first second at p50 7ms, and does so
identically with and without fix 2.

**[OB13](docs/ROADMAP-ARCHIVE.md#ob13--the-health-endpoint-fails-under-overload-but-not-for-the-reason-it-looked-like)
re-verified**: 14 of 14 probes `200` under 3,000/s, p50 52.8ms. `/_admin/*` is exempt from the
backlog gate for the same reason health is exempt from the semaphore — a node that stops
answering the endpoint that explains why it is refusing cannot be diagnosed at the one moment it
matters.

Not measured: whether the earlier refusal pays for itself on a large-body write workload, which
is the case it was built for and the one arm here does not cover. *Measured 2026-09-16 and the
answer is that it never gets the chance — the bulk lane reaches neither gate. See
[F8](docs/ROADMAP-ARCHIVE.md#f8--the-overload-gates-do-not-cover-the-bulk-write-path).*

**Fix 3 — warn on the ratio. ✅ Done 2026-09-15.** `cameodb check-config` gained an `overload`
rule, beside the `limits` rule that already weighs `max_concurrent_requests × body limit`
against the memory budget.

*The ratio could not be checked the way this entry asked for it.* It wanted admission divided
by "a plausible service rate" compared against the timeout, but the tool cannot know a node's
service rate, and any constant standing in for one is wrong for somebody — the measurements in
this entry alone span 410/s and 26,500/s on the same code. So the condition is inverted:
`max_concurrent_requests / request_timeout_secs` is the rate *at which the configuration enters
the regime*, which is arithmetic with no assumption in it, and the operator is handed that
number to check against a node they can measure.

The one judgement left is where to warn, and F7's own arms set it: 500/s, just above the ~410/s
slowest steady state measured here (M1, 200k documents, `search_threads = 2`, 3x overload). A
configuration asking the node to beat that is asking for more than this project has measured on
its slowest arm.

```
[PASS] overload    128 concurrent / 30s timeout = safe above 5 requests/s
[WARN] overload    max_concurrent_requests (3000) against a 1s request timeout admits more
                   work than the budget covers unless this node serves over 3000 requests/s...
```

A default node is three orders of magnitude clear of it, which is why the failure was only ever
reached deliberately and why the rule stays quiet by default. The warning names both knobs that
settle it, and a test pins the route an operator actually takes into the regime — raising
`max_concurrent_requests` because the node is answering 503, with nothing else changed.

**F7 is now closed.** All three fixes are in, and what each was for is worth keeping distinct:
fix 1 is the one that recovered goodput and recovery time, fix 2 moved the refusal to the door
and closed a queue that checked no deadline, and fix 3 is what tells an operator they are
walking into it. Two of the three entries were wrong about their own premise until measured.

**Verified against the pre-fix binary, 2026-09-16.** Every arm above was measured as its own
fix landed, each against the binary carrying it. This is the comparison that was missing: a node
built from `6c43c7e` — the commit before fix 1, and the one that made `request_timeout_secs`
honoured at all — run in the same session on the same M1, against the same seeded index and the
same configuration as the current binary. 200,000 documents, 4 shards, `search_threads = 2`,
`max_concurrent_requests = 3000`, `request_timeout_secs = 1`, 20s arms, harness lag p99 at or
below 2.2ms on every arm quoted.

| offered | before (`6c43c7e`) | after (`c105536`) |
|---|---|---|
| 300/s | 299 ok/s, 0 shed, p99 22.9ms | 299 ok/s, 0 shed, p99 16.7ms |
| 1,000/s | **0 ok/s**, 19,956 × 408 | **541 ok/s**, 9,138 × 503, 10 × 408 |
| 3,000/s | **0 ok/s**, 58,942 × 408, 1,267 × 503 | **500 ok/s**, 50,207 × 503, **0 × 408** |
| health under 3,000/s | 408 at 1,001ms, from the third probe on | 200 at ~130ms, 16 of 16 |
| 3,000/s → 300/s | 0 ok/s for the whole 15s step | 310 ok/s in the first second, p50 4ms |

The counters say the same thing about where the refusal is now made: 71,707
`refused_at_admission` against 1,518 `abandoned` at a worker across the three arms, with
`actor_mailbox_fallbacks` at 0 throughout. The two 300/s arms are within noise of each other, so
the gate still costs nothing to a node that keeps up.

*Read these as a before and after, not as capacity.* The absolute figures sit at the low end of
the 494–553/s band recorded above because the machine was carrying an ordinary desktop load
throughout, and every arm is a single 20s run.

The recovery arm came out worse before than the 12s recorded above: the pre-fix node served
nothing for the entire 15s it was offered 300/s after 3,000/s, where the current binary is back
to 310 ok/s within a second of the drop. That is the same relationship, not a new one — the dead
period is admission depth divided by spare capacity, and a busier machine has less to spare.

`cargo test --workspace` passes 797 tests across 43 suites at this commit and `cargo clippy
--workspace --all-targets` is clean. Fix 3 was checked against the benchmark configuration
itself, which is also what confirms the arms ran on a one-second budget: `check-config` reports
`timeout 1s (set)` rather than the derived 60s, and raises the `overload` warning on it.

A second guard remains in `dispatch_read_pool` against the full budget, for a configuration
where the read pool rather than the worker channel is the deep queue. It was kept on reasoning
rather than evidence when the fixes landed, no arm having fired it. The 2026-09-16 run did:
`read_pool_abandoned` reached 53 across the three arms, against 1,518 refused at a worker and
71,707 at the door. It is a backstop that is genuinely reached, three orders of magnitude below
the door, rather than a guard kept on argument alone.

**[OB13](docs/ROADMAP-ARCHIVE.md#ob13--the-health-endpoint-fails-under-overload-but-not-for-the-reason-it-looked-like)
is closed by this on the search path, as predicted and now measured** — and reopens on the write
path, where health queues behind a bulk write rather than behind the backlog
([F8](docs/ROADMAP-ARCHIVE.md#f8--the-overload-gates-do-not-cover-the-bulk-write-path)). Under 3,000/s against the same
node that previously answered health in 1001.8ms with every probe a 408, fourteen consecutive probes
returned **200 in ~120ms**. One fix, both entries. The 2026-09-16 run has it on both binaries at
once: the pre-fix node answered its third probe and everything after it with a 408 at 1,001ms,
the current one 200 in ~130ms on sixteen of sixteen.

**Re-measured on the M5 Pro, 2026-09-16.** The before-and-after above is an M1 result; the M5 had
only a pre-fix column, recorded the day before. This closes that gap on the second machine, and
one of its numbers contradicts the M1 reading. Release build of `525740c`, 15 cores, 24 GB,
macOS 26.6.2, same protocol throughout: 200,000 documents, 4 shards, `search_threads = 2`,
`max_concurrent_requests = 3000`, `request_timeout_secs = 1`, 20s arms after a 5s warmup, Poisson
arrivals, seed 1. Harness lag p99 stayed at or below 1.12ms on every arm, no arm dropped an
arrival and no verdict came back INVALID, so these are node measurements and not the generator.
Seeded once at the default timeout with the node restarted on the one-second budget, which
`check-config` confirmed as `timeout 1s (set)` while raising the `overload` warning on it.

| offered | before (pre-fix, 2026-09-15) | after (`525740c`) |
|---|---|---|
| 300/s | — | 300 ok/s, 0 shed, p99 5.6ms |
| 1,000/s | **233 ok/s**, 15,304 × 408 | **927 ok/s**, 1,433 × 503, **0 × 408** |
| 2,000/s | 1 ok/s, 39,990 × 408 | **923 ok/s**, 21,550 × 503, **0 × 408** |
| 3,000/s | ~0 ok/s, 59,191 × 408, 1,018 × 503 | **919 ok/s**, 41,829 × 503, **0 × 408** |
| 4,000/s | ~0 ok/s, 60,000 × 408, 20,210 × 503 | **920 ok/s**, 61,815 × 503, **0 × 408** |
| 3,000/s → 300/s | 0 ok/s for 4s after the drop | 310 ok/s in the first second, p50 1.95ms, 0 shed |

Steady-state goodput is flat at 879–917 ok/s from 1,000/s through 4,000/s offered, and **not one
408 was raised in the entire session** — every refusal was a 503 carrying `Retry-After` and a
body naming the backlog against the budget, 157,746 of them client-observed across the measured
arms. Health answered 25 of 25 probes `200` on every arm, p50 72ms at 4,000/s. `jobs_completed`
came to ~23,600 per arm whether that arm offered 1,000/s or 4,000/s: the node does a constant
amount of real work and wastes none of it on clients that have already gone.

Where the refusal is made reproduces and sharpens. Counted node-side across the whole arms
session, warmups included: 196,132 `refused_at_admission` against 745 `abandoned` at a worker —
263 to 1 — with `actor_mailbox_fallbacks` at 0 throughout. `read_pool_abandoned` stayed at **0**
here where the M1 run reached 53, so the read-pool backstop is reached on some machines and not
others, which is what a backstop should look like.

***The quarter of capacity the reserve was said to cost does not reproduce.*** A same-session
control — the same node and index restarted on `request_timeout_secs = 300`, so admission is the
only gate and no deadline logic runs — puts the cost at roughly nothing:

| at 3,000/s offered | steady goodput | p50 | peak in flight |
|---|---|---|---|
| control, 300s timeout | 897 ok/s | **3,334ms** | 3,007 |
| current, 1s timeout | 879 ok/s | **873ms** | 836 |

Two percent apart on goodput, which is inside this harness's noise, against a p50 **3.8× lower**
and a queue **3.6× shallower**. So on this machine the deadline reserve is close to free, and what
it buys is latency and a bounded queue rather than throughput — not the 555-against-730 trade the
M1 recorded. That also empties the open question raised under fix 1: there is no quarter of
capacity here to reclaim by tuning the 2× factor or replacing it with a percentile of the service
distribution. Whether the M1 figure was the machine or the older binary is not settled by this run.

*Read as a before and after on one configuration, not as the box's ceiling.* `search_threads = 2`
is deliberately small so the overload regime is reachable at a few thousand requests a second, the
harness was co-located on a machine where core pinning is a no-op, and every arm is a single 20s
run.

Not yet measured: whether a retrying client deepens it (each arm here used a fixed arrival rate
and no retries). **The write path, which has its own queue, was measured 2026-09-16 and does not
hold** — F7's gates are absent on the bulk lane and its original collapse is intact there, with
OB13 reopening on a second mechanism. Filed as
[F8](docs/ROADMAP-ARCHIVE.md#f8--the-overload-gates-do-not-cover-the-bulk-write-path).

- ✅ [**F8 — The overload gates do not cover the bulk write path**](docs/ROADMAP-ARCHIVE.md#f8--the-overload-gates-do-not-cover-the-bulk-write-path)

---

### F11 — The disk a delete frees was never returned

◐ **Partial.** Raised from the field on 2026-10-16: a node of about 500M records had a large share
deleted to free disk, and the files kept their size.

**What was measured** (4 shards, 2.2M documents of about 800 B, 75% deleted):

| | After load | After the delete | Reclaimed |
|---|---|---|---|
| redb `store.redb`, per shard | 1,028 MB, holding 435 MB | 1,028 MB, unchanged | 310 MB with `compact`, 1.5 s |
| Tantivy index, per shard | 160 MB | 176 MB, 62% of it deleted | 65 MB with a merge, 1.3 s |

- **redb never returns space while it runs.** A delete frees pages inside the file and later writes
  reuse them, but only `Database::compact` truncates, and it needs the database to itself. A
  running node shares every shard's store, so this is offline.
- **The redb file was 2.3× its data before any delete.** The WAL tables live in the same file and
  are written and trimmed at every checkpoint, which is the likely cause. Not proven, and worth its
  own look: it means a node that never deletes still carries that much free space.
- **Tantivy keeps a deleted document until its segment is merged.** The default `LogMergePolicy`
  merges similar-sized segments once there are eight, never because of deletes alone
  (`del_docs_ratio_before_merge` is 1.0), and never a segment above ten million live documents.
  On a large node, the deleted documents in its biggest segments may never go.
- **Write speed after the delete was unchanged at this scale,** about 43k docs/s before, after and
  after reclaiming. The site's slowdown was not reproduced; its data is hundreds of times larger.

**✅ Done:**
- `cameodb compact [-c <PATH>] [--dry-run]` (`a648898`), with the node stopped: compacts every
  shard's `store.redb` and `metadata.redb`. `--dry-run` reports what each file would give back.
- `POST /_admin/index/{index}/merge` (`8647baf`), with the node running: commits, then merges each
  shard's segments into one on Tantivy's merge threads. It returns at once, and each shard logs
  `Merge complete`. Writes carried on at full rate during a merge of 850k deleted documents.
- `cameodb compact --rewrite [--keep]` (`9741dc5`): copies each file into a new one and renames it
  over the original, after checking the filesystem has room for the copy. Added after a
  production node of 17 shards of 1.5–1.8 TB, a few percent of its documents deleted, came out
  of 5–8 h of in-place compaction 0–6% smaller per file. The suspicion was in-place
  compaction stopping at a large page with no free run below it. Not reproduced: a dense file
  of mixed 2–72 KB documents with every 20th deleted compacted in place to its allocated size,
  and a rewrite ends there too, never smaller. The deciding number is that node's `--dry-run`
  size against its file size, not yet taken.

**📋 Planned: reclaim deleted documents automatically, behind a setting.** Postponed on
2026-10-16 in favour of the two manual tools above. Off by default, so a node that does not set
it merges exactly as it does today.

- **The shape:** a merge policy wrapping `LogMergePolicy`. It keeps every candidate the default
  proposes and adds a single-segment merge for any segment whose deleted share is above a
  threshold, of any size. Rewriting one segment costs in proportion to that segment, which keeps a
  delete from pulling a whole level into one merge.
- **Not the built-in knob:** `set_del_docs_ratio_before_merge` alone would merge the whole level
  around such a segment, and still skips segments above ten million live documents, which are the
  ones that matter at the site's scale.
- **Where it plugs in:** the writer is built in `HybridStore::get_or_create_index_locked`
  (`store.rs`), beside `merge_num_threads`. The setting would follow the same path as
  `merge_num_threads` and `quick_repair`: `[search]` in the config, the CLI and environment
  overrides, `NodeConfig`, then `StorageConfig`.
- **Open questions:** the default threshold (0.3 is a starting point), and whether a rewrite
  should wait until the node is quiet. Every rewrite needs free disk for the new segment.

---

## G. Code health, reviewed at 0.3.1

Reviewed 2026-08-16, after the paging and MCP work landed; re-checked 2026-08-26, when every
item was still present. Nothing here changes behaviour; each item is a place the code now says
one thing twice, or where the next feature will cost more than it should. Ordered by what each
buys, not by effort.

CH8–CH12 were added by the 2026-09-01 write/read/delete review and are the same kind of item:
write-path allocations and duplication that cost latency but are not defects.

- ✅ [**CH1 — One scatter-gather, written twice**](docs/ROADMAP-ARCHIVE.md#ch1--one-scatter-gather-written-twice)

### CH2 — The merge primitives deserve their own module

📋 `SearchWindow`, `order_hit_blocks`, `order_shard_hits`, `compare_hits_by_field`,
`stamp_sort_keys` and their tests are a coherent, self-contained unit inside
`node_orchestrator.rs` — and they are the unit every paging invariant lives in, imported by the
HTTP and MCP surfaces alike. A `search_merge` module shrinks the file everyone edits and gives
those invariants one place to be read. `storage/src/lib.rs` has the same disease and the same
cure — the sorted-collector logic, query preparation and schema description are separable.

**2026-08-26:** both files grew rather than shrank since the review —
`node_orchestrator.rs` 9,300 → **9,683** lines, `storage/src/lib.rs` 7,600 → **8,293**.

**2026-09-01:** and again, by more than the previous interval — `node_orchestrator.rs` 9,683 →
**11,012**, `storage/src/lib.rs` 8,293 → **9,226**. Both grew *while* being actively corrected,
which is the case for this item rather than against it: every fix in the 0.3.3 review had to be
made inside one of these two files.

**2026-09-17:** the server half is done — [L11](docs/ROADMAP-ARCHIVE.md#l11--node_orchestratorrs-is-13669-lines-and-holds-four-actors)
landed the split, and the merge primitives, sort keys and validation live in
`node/search.rs` as this item always wanted. `storage/src/lib.rs` keeps the same
disease; its cure is [L12](docs/ROADMAP-ARCHIVE.md#l12--storagesrclibrs-is-9961-lines-of-which-one-impl-block-is-4460).

**2026-09-19:** the storage half is done too — L12 split `storage/src/lib.rs` into `query.rs`,
`schema.rs`, `store.rs` and `search.rs`, so the sorted-collector logic lives in
`storage/src/search.rs`, query preparation in `storage/src/query.rs` and schema description in
`storage/src/schema.rs`. Both files this item tracked are now directories.

### CH3 — Cursor paging (`search_after`)

📋 The deep-page refusal already tells callers to "sort on a field that lets you resume from the
last hit" — advice nothing implements. A `search_after` parameter on a sorted search (resume
past the last sort key, tie broken the way the merge already breaks it) makes page *N* cost what
page 1 costs, where offset paging fetches and discards *N−1* pages from every source — the cost
`SearchWindow::checked` exists to refuse. The merge already threads `_sort_key` through every
hit, which is most of the cursor.

Belongs beside [A1 (MCP streaming)](#a1--mcp-streaming): both answer "the result is larger than
a page", and building either changes how the other should work.

- ✅ [**CH4 — The window bound is spelled twice**](docs/ROADMAP-ARCHIVE.md#ch4--the-window-bound-is-spelled-twice)
- ✅ [**CH5 — Sort type conversion, four times inline**](docs/ROADMAP-ARCHIVE.md#ch5--sort-type-conversion-four-times-inline)
- ✅ [**CH6 — The federated merge clones every hit**](docs/ROADMAP-ARCHIVE.md#ch6--the-federated-merge-clones-every-hit)

### CH7 — The string-fast collector repeats the macro's body

📋 `collect_sorted!` in `storage/src/lib.rs` covers the u64/i64/f64/date branches; the
string-fast branch writes the same `MultiCollector` block out by hand because its key type is
`String` rather than a copyable numeric. Fold it in by parameterizing the collector expression,
so the next change to how a sorted search counts its total touches one place.

- ✅ [**CH8 — The single-write path clones the whole schema and document**](docs/ROADMAP-ARCHIVE.md#ch8--the-single-write-path-clones-the-whole-schema-and-document)
- ✅ [**CH9 — Bulk validation clones the batch, and Tantivy docs are built inside the transaction**](docs/ROADMAP-ARCHIVE.md#ch9--bulk-validation-clones-the-batch-and-tantivy-docs-are-built-inside-the-transaction)
- ✅ [**CH10 — `engine_write` and `orch_write` are near-duplicates**](docs/ROADMAP-ARCHIVE.md#ch10--engine_write-and-orch_write-are-near-duplicates)
- ✅ [**CH11 — Routing-key derivation is written four times, with two algorithms**](docs/ROADMAP-ARCHIVE.md#ch11--routing-key-derivation-is-written-four-times-with-two-algorithms)
- ✅ [**CH12 — Write-path serialization and round-trip waste**](docs/ROADMAP-ARCHIVE.md#ch12--write-path-serialization-and-round-trip-waste)

---

## H. Observed

Defects found outside a phase's own work. An entry keeps its evidence here even once it is
scoped — OB1 was Phase 18's first item and OB2 is what J1 fixes — because the observation and
the plan answer different questions, and splitting one across two places loses the reason it was
opened.

- ✅ [**OB1 — `fast: false` is not honoured on a numeric field**](docs/ROADMAP-ARCHIVE.md#ob1--fast-false-is-not-honoured-on-a-numeric-field)
- ✅ [**OB2 — A `facet` field cannot be written to**](docs/ROADMAP-ARCHIVE.md#ob2--a-facet-field-cannot-be-written-to)
- ✅ [**OB3 — A single write or delete can land on the wrong shard**](docs/ROADMAP-ARCHIVE.md#ob3--a-single-write-or-delete-can-land-on-the-wrong-shard)
- ✅ [**OB4 — The single-write path stages Tantivy before redb commits**](docs/ROADMAP-ARCHIVE.md#ob4--the-single-write-path-stages-tantivy-before-redb-commits)
- ✅ [**OB5 — One batch can index the same id twice**](docs/ROADMAP-ARCHIVE.md#ob5--one-batch-can-index-the-same-id-twice)
- ✅ [**OB6 — Evolving an already-indexed field's type silently unindexes its values**](docs/ROADMAP-ARCHIVE.md#ob6--evolving-an-already-indexed-fields-type-silently-unindexes-its-values)
- ✅ [**OB7 — The validator and the writer disagree about floats in integer fields**](docs/ROADMAP-ARCHIVE.md#ob7--the-validator-and-the-writer-disagree-about-floats-in-integer-fields)
- ✅ [**OB8 — A paged search loses its page on the streaming fan-out**](docs/ROADMAP-ARCHIVE.md#ob8--a-paged-search-loses-its-page-on-the-streaming-fan-out)
- ✅ [**OB9 — A bulk delete drops a peer's per-id errors**](docs/ROADMAP-ARCHIVE.md#ob9--a-bulk-delete-drops-a-peers-per-id-errors)
- ✅ [**OB10 — The record limit meant different things depending on how the wire split the line**](docs/ROADMAP-ARCHIVE.md#ob10--the-record-limit-meant-different-things-depending-on-how-the-wire-split-the-line)
- ✅ [**OB11 — Reasons and totals that went missing on the way out**](docs/ROADMAP-ARCHIVE.md#ob11--reasons-and-totals-that-went-missing-on-the-way-out)

### OB12 — The schema gate deadlocked a fan-out against itself

✅ **Done** 2026-09-02, found verifying the 0.3.3 perf work on the compose cluster rather than by
reading anything. **The most serious defect of this cycle**, and it was in unreleased code:
[c6da72a](#the-order-of-work)'s gate, which had been verified on a path that happened not to fan
out.

The gate runs inside `NodeOrchestrator`'s message handler. A bulk write holds that mailbox for its
whole duration — which is [CH12](docs/ROADMAP-ARCHIVE.md#ch12--write-path-serialization-and-round-trip-waste)'s fourth
bullet, filed as an efficiency note — and from inside it the node both forwards shares to its
peers *and* asks those peers for a schema, through the one `RemoteActorRef<NodeOrchestrator>`
mailbox now running the forwarded writes, each of which was asking back.
`ConnectionChannel::{Operations, Replication}` both resolve to that single actor, so there is no
lane a metadata read can take instead.

| Measured on the 3-node compose cluster, `green 3/3` | before | after |
|---|---|---|
| 46 documents, new index, spread across shards | **15 written, 31 refused, 5.02s** | 46 written, 0 refused, 1.72s |
| the same 46 under one `routing_key` (no fan-out) | 46 written, 0.03s | 46 written, 0.13s |
| 46 sequential single writes | 46 ok, 0.15s | 46 ok, 0.68s |
| retrying the failed batch | 18 of 46, then 18 again | n/a |

How many survived was decided by which forwards won the race — 14 to 20 of 46 over six runs —
and every run paid the full five seconds.

Exactly one `PEER_SCHEMA_LOOKUP_TIMEOUT` window, and the refusal came from the *receiving* node's
own handler: `Refusing to sample a schema … reason=node 8da172e0 timed out; node 6fa6557d timed
out`, 5.028s after that same node logged `Attempting remote call` to the first of them.

**A forwarded write now carries what it was validated against**, so the receiver neither asks nor
samples — `ClientOp::{Write, BulkWrite}::carried_schema`, defaulted so a share from an older peer
reads as "nothing carried" and takes the lookup as before. This is what the gate was arguing for:
one schema decision per index, not one per node.

**What travels is one bit, and no schema.** Three cuts, and only the third is right. The first
sent the whole `IndexSchema` on every forward: 602 bytes for three fields, 3,478 for twenty,
against a 49-byte document. The second sent a 61-byte stamp, which is nothing on a batch and more
than the payload on a single write. Neither needed to be sent, because the question was wrong: a
forwarded write is a *share of a decision another node already made*, so the receiver only needs
to know that inventing one is not its job — and can then **ask**. `ClientOp::Write` already
carried `forwarded` for OB3's hop limit, so a single write costs nothing extra; `BulkWrite` gained
it.

The receiver *answers* rather than asking, which is what keeps the deadlock closed: it cannot
canvass peers from inside a write without waiting on the mailbox those peers are using to run the
writes this fan-out just sent them. Recognised by verdict — a new `RemoteVerdict::SchemaRequired`
— rather than by reading the message text, which is what a0cf3da's classification work exists to
avoid.

The sender-side alternative was a cache of "peer P already has index I". Cheaper still, and
rejected: it is an assumption about another node's disk, which is what ADR 005's "a peer's own
report is the truth" rules out. Asking costs one round trip per node per index and cannot be
stale.

Three invariants hold it up:

- **A node that already holds a schema ignores a body it is handed**, because the adopt path is
  only reached when it holds none. So a stale carried schema cannot overwrite a current one.
- **An empty schema is never sent** (`schema_to_carry`). The receiver adopts what it is given and
  then treats the index as already existing, so every field of the arriving document would be
  recorded unsearchable rather than typed.
- **A forwarded share is never sampled.** `infer_type_from_value` reads a subset holding no
  negative values differently from the whole batch, so two nodes sampling their own shares can
  build two different tantivy indexes from one request, with no bug anywhere. One decision per
  index, made where the batch was whole.

Divergence detection is *not* on this path, deliberately. The stamp would have bought it, and the
price was 61 bytes on every forward to notice something that only shows on a node currently being
written to. Once step 5 lands it is free and better: every node compares its own thumbprint against
the cluster-agreed one, at boot and on reconcile, for no wire bytes at all.

Verified: identical `version` and `thumbprint` on all three nodes; a content query finds all 46
and a sorted page returns `[46,45,44,43,42]`, so adoption kept the fields searchable and the fast
column intact; a declaration on one node is still adopted verbatim by the other two with `n`
still `u64`; the degraded arm still answers `503` on a new index, now in 0.01s rather than after
a five-second wait, and the retry once whole writes all of it.

**The class is still open.** This closes the write path by removing the question, not by making
the answer cheap: any *other* peer metadata read issued while that peer is inside a write still
queues behind it — `FindSchemaInCluster` behind a `DELETE` is the reachable one. Taking metadata
reads off the orchestrator mailbox is the general fix, and it is not done.

- ✅ [**OB13 — The health endpoint fails under overload, but not for the reason it looked like**](docs/ROADMAP-ARCHIVE.md#ob13--the-health-endpoint-fails-under-overload-but-not-for-the-reason-it-looked-like)

---

### OB14 — A timed-out request never leaves the worker pool, and the node degrades until it is restarted

✅ **Found and fixed 2026-09-25**, by the first [M6](docs/ROADMAP-ARCHIVE.md#m6--close-and-re-measure-the-bulk-lane) arm, on the
M5 Pro against `f4a20a3`, release build, harness co-located. This is the run F8's re-measurement
was owed, and it did not get as far as an open-loop arm: the closed-loop capacity probe wedged the
node.

**The shape, in one line.** Every request the server answers `408` leaves one slot permanently
occupied in the worker pool's accounting; the slots never come back; reads stay healthy the whole
time; writes stop node-wide long before the counter reaches capacity; and the process will not
shut down on `SIGTERM`.

**Measured, three arms on one node** — 4 shards, `search_threads = 2`,
`max_concurrent_requests = 3000`, `request_timeout_secs = 1`, `wal_sync = true`, the F8 protocol:

| arm | load | goodput | `408` | pool gap after, at rest |
|---|---|---|---|---|
| A | bulk, batch 100, concurrency 1, 10s | 54 ok/s, 5,447 docs/s | 0 | **0** |
| B | bulk, batch 500, concurrency 8, 10s | 19 ok/s, 9,727 docs/s | **16** | **16** |
| probe | bulk, batch 500, concurrency 4, 200k seeded, 20s | 11 ok/s, 5,587 docs/s | **44** | **44** |

"Pool gap" is `dispatch.round_robin_sends` minus the sum of per-worker `jobs_completed` on
`/_admin/workers`, read at rest with no client connected. **It equals the `408` count exactly, in
every arm, and it never decreases.** Arm A is the control: no timeouts, gap 0, `sent == completed`.
`abandoned` and `refused_at_admission` stayed `0` throughout — no gate refused any of this.

**It is not an observability wart.** The same event that leaks the per-worker `in_flight` gauge
also skips `job_left_pool()`, and that decrements `outstanding` — the pool-wide counter
[`QueueLoad`](docs/ROADMAP-ARCHIVE.md#f8--the-overload-gates-do-not-cover-the-bulk-write-path) predicts against and the
one `/_cluster/health` publishes as `queue_depth`. A node that has answered *n* timeouts believes
it has *n* requests in flight forever, and both the front door and the dequeue check are reading
that number.

**The degradation is ordered, and the order is the surprising part.**

- **Reads never stop.** With 19 slots gone, `POST /api/armA/search` answered `200` in **4.8ms**
  with all four shards responding, `/_indexes` in 1.7ms, `/_admin/workers` in 0.5ms.
- **Writes stop node-wide well before the pool is full.** At the same 19, every write answered
  `408` after a full second — to the hammered index, and to a brand-new index name. The gap did
  *not* rise when those writes failed, so they are blocking *before* worker dispatch rather than
  being dispatched and stuck; the schema-defer path onto the actor is the suspect and this is the
  part that is inferred rather than measured.
- **At a gap of 64 — 8 workers × 8 in-flight — everything worker-eligible dies.** `GetIdentity`
  and `ListIndexes` both failed (`health actor budget exhausted or error`), and health degraded to
  `status: red` with `active_shards: 0` and `total_indexes: 0` while `/_admin/workers` still
  reported all four shards `serving: true`. The shards were fine; health could not get an answer.
  That the body degrades to fallbacks instead of hanging is [F8](docs/ROADMAP-ARCHIVE.md#f8--the-overload-gates-do-not-cover-the-bulk-write-path)
  item 2 working as designed — on a node whose real state it can no longer see.
- **`SIGTERM` does not stop the process.** The worker loop's drain reacquires the full semaphore
  width before returning, and the stuck tasks never give their permits back. The port closed, the
  process stayed, and `SIGKILL` was required. A node in this state cannot be restarted by an
  orchestrator that waits for a graceful stop.

**No panic anywhere** in 34,452 lines of debug log, and **CPU at 0.0%** while wedged — the tasks
are parked, not spinning, which is why nothing in the process noticed.

**Where the decrement lives.** `orchestrator_worker_loop` increments `in_flight` before
`tokio::spawn`, and the spawned task calls `record_service`, `job_left_pool`,
`in_flight.fetch_sub`, `jobs_completed.fetch_add` and `drop(permit)` after `run_op` returns. A
task that never reaches that tail leaks all of it together, which matches the 1:1 count. The
comment above the F7 check states the intent — *everything past this point is uncancellable* — and
the measurement says that either the task is cancelled after all, or `run_op` parks forever. Which
of the two, and where, is the open question; it is a source question rather than a harness one and
should be answered before anything is changed.

**Why this outranks the rest of the 0.3.5 list.** The trigger is a server-side timeout, which on an
internet-exposed multi-tenant node is routine traffic rather than an attack — one slow tenant's
timeouts degrade every other tenant on the process, and the only recovery is a hard restart. The
1s timeout used here makes it fast to reproduce; a 60s production timeout makes it slower, not
absent. [M4](docs/ROADMAP-ARCHIVE.md#m4--per-key-resource-quotas) and [M5](docs/ROADMAP-ARCHIVE.md#m5--per-index-capability-subtraction) bound
what a tenant may *ask for* and neither touches this.

**Narrowed 2026-09-25, same session.** Four further arms, and three things are now settled.

*The tasks park; they do not panic.* At 240 timeouts the per-worker `in_flight` gauge pins at
**64 — exactly `8 workers × 8`, the full width** — and stays there while `round_robin_sends`
keeps climbing and `jobs_completed` is frozen. New work is dispatched and never runs, so the
semaphore permits are still held. A panic would have run `permit`'s destructor and freed the
pool; there are **zero panics in any of the four node logs**, and CPU sits at 0.0%. The tasks are
alive and parked on a waker that never fires.

*The gap and the gauge measure different things once the pool is full.* `in_flight` is capped by
the width, so at 240 timeouts the other 176 are jobs sitting in per-worker channels that will
never be dequeued. Below the width the two agree, which is why the first three arms read equal.

*F7's dequeue check is unreachable exactly when it is needed.* `abandoned` stayed **0** through
every arm, including one where jobs waited many seconds against a 1s budget. The loop takes its
permit **before** `rx.recv()`, so once the width is held by parked tasks no worker ever reaches
the deadline comparison that exists to shed stale work. The protection is upstream of the thing
that breaks it.

*Still open: the await itself.* Every runtime thread is idle, and the four `writer-shard-*`
threads are parked on their own command channel rather than inside a write — so the parked tasks
are not blocked behind the writers. Thread sampling cannot see a parked future, and the release
binary carries no symbols, so naming the await needs instrumentation rather than another arm.

**Root cause: `should_commit_writer` deadlocks the shard writer thread against itself.**
Named by symbolising a sample of the wedged process — which needed an unstripped build, because
`[profile.release]` sets `strip = true` and `debug = false`, so every earlier sample resolved to
raw offsets. The stack is unambiguous:

```
spawn_writer_thread (shard.rs)
 → HybridStore::apply_batch_and_maybe_commit
   → HybridStore::maybe_commit_writer        (should_commit_writer, inlined)
     → DashMap<String, BudgetCacheEntry>::insert
       → dashmap RawRwLock::lock_exclusive_slow → pthread_cond_wait, forever
```

The budget read was written as a `match` over `DashMap::get`:

```rust
let budget = match self.budget_cache.get(index) {          // Ref = read guard on the shard
    Some(entry) if !entry.value().is_stale() => entry.value().budget,
    _ => { ...; self.budget_cache.insert(...); b }         // write lock on the SAME shard
};
```

A match scrutinee's temporary lives to the end of the match, so the `_` arm asks the shard for
its write lock while this thread still holds the shard's read lock. dashmap's `RwLock` is not
reentrant — the writer waits for a reader that is itself, on the one thread that serves every
write for that shard.

**Why it survived review, a release, and every test.** Two conditions have to coincide. The arm
is only reachable when the entry is **stale**, and `BUDGET_CACHE_TTL` is **30 seconds** — so an
index younger than that always takes the fresh arm, which is every unit test and every short
bench run. And the entry has to be **present**: a missing one makes `get` return `None`, which
holds no guard, so the insert goes through. *Present-but-stale* is the only state that hangs, and
it is the state every index older than thirty seconds is in. `get_or_create_index` inserts the
entry when the writer opens, so the clock starts there. That is why arm A (10s on a fresh index)
was clean and everything past the half-minute mark wedged.

**The fix** reads the entry through `and_then`, which consumes the `Ref` and drops it before the
decision is made, so the write lock is taken on an unlocked shard. Four lines, and the comment
records the rule rather than the symptom.

**Pinned by `a_stale_budget_entry_does_not_deadlock_the_writer`**, which ages an entry past the
TTL in place and calls `should_commit_writer` on another thread against a deadline — because the
regression is a hang, and an assertion on a return value cannot fail if the call never returns.
It fails on the old code with `Timeout` after 10s and passes on the new.

**Verified end to end.** The arm pair that wedged the node now runs clean, and faster:

| | before | after |
|---|---|---|
| arm 1 | 50 ok/s | 52 ok/s, **26,049 docs/s** |
| arm 2 | 36 ok/s, **32 × 408, gap 32** | 53 ok/s, **26,419 docs/s, 0 × 408, gap 0** |

Four further 15s arms back to back, well past the TTL and on an index grown past a million
documents: **gap 0 and `green` on every one**, 16,959–25,885 docs/s as merges come and go, zero
timeouts. `SIGTERM` stops the node cleanly again. `cargo test -p storage -p server` is green — 36
suites, no failures.

**What this fix does not cover, and why that mattered.** Three weaknesses the run exposed stand on
their own merits, because a node should survive a stuck operation rather than depend on there
never being one: the pool's accounting was not RAII, F7's dequeue check was unreachable once the
width was held, and nothing bounded a worker task. This deadlock was one way to reach all three;
it was not the only one. **All three were closed the same day** — see
[M6](docs/ROADMAP-ARCHIVE.md#m6--close-and-re-measure-the-bulk-lane) for the mechanisms, the two tests that fail against
the code they pin, and the overload run where goodput degrades to 89 ok/s instead of collapsing to
zero while health answers in 1.4ms.

**Reproduction, from a wiped volume**: start a node with the config above; run
`cameodb-bench --mode bulk --batch-size 100 --concurrency 1 --duration 10 --seed-docs 5000
--keep-index` and read the gap on `/_admin/workers` (0); run the same with
`--batch-size 500 --concurrency 8 --seed-docs 0` and read it again. It equals the `408` count and
stays there.

---

- ✅ [**OB15 — Every refused request was an `ERROR` line, and under write overload the logging cost half the goodput**](docs/ROADMAP-ARCHIVE.md#ob15--every-refused-request-was-an-error-line-and-under-write-overload-the-logging-cost-half-the-goodput)
- ✅ [**OB16 — Closing an index from another thread lost the writes in flight on it**](docs/ROADMAP-ARCHIVE.md#ob16--closing-an-index-from-another-thread-lost-the-writes-in-flight-on-it)
- ✅ [**OB17 — A schema edit and an evolving write overwrote each other**](docs/ROADMAP-ARCHIVE.md#ob17--a-schema-edit-and-an-evolving-write-overwrote-each-other)
- ✅ [**OB18 — A streaming search ran outside the concurrency limit, and waited forever on a stalled client**](docs/ROADMAP-ARCHIVE.md#ob18--a-streaming-search-ran-outside-the-concurrency-limit-and-waited-forever-on-a-stalled-client)
- ✅ [**OB19 — Two clustered deadlocks through the coordinator's mailbox**](docs/ROADMAP-ARCHIVE.md#ob19--two-clustered-deadlocks-through-the-coordinators-mailbox)
- ✅ [**OB20 — A fresh cluster can keep a partial ring, and nothing repairs it**](docs/ROADMAP-ARCHIVE.md#ob20--a-fresh-cluster-can-keep-a-partial-ring-and-nothing-repairs-it)
- ✅ [**OB21 — A peer that stops answering: detected, refused at once, and bounded while it lasts**](docs/ROADMAP-ARCHIVE.md#ob21--a-peer-that-stops-answering-detected-refused-at-once-and-bounded-while-it-lasts)
- ✅ [**OB22 — An orchestrator waited on peers while holding its mailbox**](docs/ROADMAP-ARCHIVE.md#ob22--an-orchestrator-waited-on-peers-while-holding-its-mailbox)
- ✅ [**OB23 — Recreating an index after dropping its schema lost most of what was written**](docs/ROADMAP-ARCHIVE.md#ob23--recreating-an-index-after-dropping-its-schema-lost-most-of-what-was-written)
- ✅ [**OB24 — `a AND NOT b` and `a OR NOT b` parsed cleanly and answered wrongly**](docs/ROADMAP-ARCHIVE.md#ob24--a-and-not-b-and-a-or-not-b-parsed-cleanly-and-answered-wrongly)
- ✅ [**OB25 — A search during a restart answered short without saying so**](docs/ROADMAP-ARCHIVE.md#ob25--a-search-during-a-restart-answered-short-without-saying-so)

### OB26 — A restarted node waits on every expected peer before it leaves bootstrap

📋 **Planned, low priority.** Read from the code 2026-10-15; reproduced in Docker 2026-10-18 at
`7696b4a` — the mode is real, and no client sees it. `bootstrap_complete` becomes true in
`evaluate_and_transition_state` only when no expected node is inactive (`coordinator.rs:510–513`) —
every node in the snapshot or the configuration, connected. Until then `PeerDiscovered` skips the
actor push and pull of shard maps (`:1388`) and so does `RegisterLocalShards` (`:789`), leaving the
node to the DHT records and the 10 s `SyncShardMaps` pull. A node restarted while any one peer is
down therefore never leaves that mode, and the event-driven exchange
[N15](#n15--per-index-state-and-node-state-each-with-one-owner) intends does not run on it.
`MarkBootstrapComplete`, the override that would end the mode, is a struct with no handler and no
sender (dead surface, for [N12](#n12--dead-surface-and-the-comments-that-no-longer-match)).

**Measured, three nodes, `cameodb.toml` listing all three:**

| Scenario | Leaves bootstrap | Shard maps | Data |
|---|---|---|---|
| Fresh cluster, node3 never started | Neither node | Each other's shards after 12.7 s and 14.8 s, from the 10 s pull. The DHT step fetched node metadata only. Instant once node3 joined (the stability push) | Not reachable: creating an index needs every configured node to answer the schema canvass (503, *only 2 of 3 cluster nodes are connected*) |
| node3 stopped, node2 restarted | node2 never (`Skipping actor-based shard exchange (discovery phase)`) | Whole at once: the snapshot restored them, and node1, never restarted, pushed and pulled 9 ms after Identify | Whole: node3's ids unavailable and reported (`failed: 1`), none misplaced or duplicated |
| Whole cluster restarted, node3 down | No node; no exchange runs | Whole at once, from the snapshot | Whole, as above |

So the snapshot carries the map across restarts, and any node that never restarted still pushes on
Identify. The only delay is the fresh cluster, where nothing can be written anyway. The fix is still
worth making because it deletes code and makes the exchange the one that actually runs, not because
a client is waiting on it.

The rule this should follow: bootstrap ends at the first peer connected, or on a deadline — a
peer that is down is the steady state the cluster has to serve in, not a phase before it. That
means the actor push and pull run on every `PeerDiscovered` and `RegisterLocalShards`; the
stability push in `evaluate_and_transition_state` then repeats what `PeerDiscovered` does and goes;
`MarkBootstrapComplete` goes. Roughly a day with a `cluster.sh` phase for the restart-with-a-peer-down
case. Today shard sets cannot change during a process's life, so pushing against a partial view
risks only what the 10 s pull already risks ([O3](#o3--shard-maps-merge-on-a-version-not-on-recency)'s
narrow live case); it no longer has to wait for O3. Best done together with
[OB27](#ob27--a-replaced-node-never-leaves-the-cluster), which touches the same count of expected nodes.

### OB27 — A replaced node never leaves the cluster

📋 **Planned.** Found 2026-10-18 while reproducing OB26, at `7696b4a`. node3 of three was stopped, its
data directory emptied, and started again on the same address: a new node identity, and the old
one never returns. From then on, permanently:

- **Every node's ring keeps the old identity's shards** — `cluster_total_shards` 16 for 12 real
  shards — and `total_nodes` reads 4, so health stays yellow and every node restarted afterwards stays
  in bootstrap ([OB26](#ob26--a-restarted-node-waits-on-every-expected-peer-before-it-leaves-bootstrap)).
- **The old shards keep their key ranges.** A write whose id hashes there gets
  `500 Write operation cannot be broadcast - routing failed` (9 of 50 ids) — unavailable, and a 500
  where every other unreachable-owner path answers 503.
- **The new node cannot learn an existing index's schema.** The canvass counts the old identity as
  a node that must answer, so it refuses: `503 cannot establish the cluster schema for 'splitidx':
  only 3 of 4 cluster nodes are connected` (10 of 50 ids). Every write landing on the new node's
  shards fails.

The cause is one rule applied in three places — bootstrap, the schema canvass, health: *every node
ever seen must be connected*. `expected_nodes` grows from the snapshot, the configuration and every
shard map, and nothing removes an entry; there is no admin operation that forgets a node. Rebuilding
one node's disk therefore breaks the cluster until its metadata is edited by hand.

**Change:**
1. **The 500 becomes a 503** naming the unreachable owner, as for a lost peer. Small, independent.
2. **`POST /_admin/cluster/nodes/{node_id}/forget`** (and the CLI): refused while the node is
   connected; removes it from `expected_nodes` and its shards from the map, persists, and
   propagates. Propagation is the hard part: a merge prunes only the *sender's* shards, so any other
   node's push or pull would bring the forgotten shards back within 10 s. It needs
   [O3](#o3--shard-maps-merge-on-a-version-not-on-recency)'s per-owner epochs plus a tombstone — an
   owner entry with a final epoch and no shards, which no older entry can overwrite.
3. **Decide what a node rebuilt on the same address means.** Either the operator forgets the old
   identity (2), or a node may keep its identity file across a rebuilt data directory. Documented in
   DEPLOYMENT either way. The data that lived on the old shards is gone in both — replication is
   [D2](#d2--replication).

- ✅ [**OB28 — Two range shapes were dropped without a word or failed the search**](docs/ROADMAP-ARCHIVE.md#ob28--two-range-shapes-were-dropped-without-a-word-or-failed-the-search)
- ✅ [**OB29 — What verifying OB28 over MCP found**](docs/ROADMAP-ARCHIVE.md#ob29--what-verifying-ob28-over-mcp-found)
- ✅ [**F9 — Commit on a clock, not a count**](docs/ROADMAP-ARCHIVE.md#f9--commit-on-a-clock-not-a-count)

---

## J. Phase 18 — Field types: Facet and JSON ◐ Partial

Scoped 2026-08-27, measured against a running engine rather than read off the source.

Two field types exist in the schema and are advertised in the query reference. `facet` could not
be written to, and is fixed — [J1](docs/ROADMAP-ARCHIVE.md#j1--a-facet-field-cannot-be-written-to) landed 2026-08-31, so a
declared facet field now takes a path and matches a hierarchy. `json` can be written to, and still
behaves exactly like `text` — same terms, same matches, no subfield addressing. That is the
remaining item, and it is not only a fix: it alters what an index means, which is why it is
planned rather than patched.

**The order is fixed, and one item was a prerequisite rather than a preference.**
[OB1](docs/ROADMAP-ARCHIVE.md#ob1--fast-false-is-not-honoured-on-a-numeric-field) went first and **landed 2026-08-27**: J2
defaults a `json` field to `fast` and lets a schema turn it off, and OB1 was the defect that ate
exactly that override — an unconditional assignment over a `bool` that cannot express "unset".
Building J2's default the way the numeric types built theirs would have reproduced it, and the
promised override would silently not have existed. With `fast` three-state on the wire and
resolved in one place, J2 declares its default rather than assigning one. Then J1, which was
independent and small and is now done; then J2; then J3, which describes what the first three did.

    OB1 ✅ the override holds  →  J1 ✅ facet writable  →  J2 json subfields  →  J3 the reference

**Neither needs a migration**, which is what keeps the phase small. A `json` field expresses
nothing a `text` field does not, so there is no behaviour any existing index depends on — see J2's
own section for the measurement and the boundary that follows from it. `text` holding JSON is
untouched throughout, and the facet half shipped without one because no index had ever held a
facet value.

**The shape this phase settles**, and the reason it is worth stating before any of it is built:

| Declared type | What it is for | How it is queried |
|---|---|---|
| `text` holding an object | Search everything in a blob without declaring its shape. Deliberate, and staying. | `blob:red` — keys and values alike, one bag of words |
| `json` | Subfield addressing, with the subfield's own type | `blob.color:red`, `blob.size:>40` |
| `facet` | One hierarchical path per document, matched at any level | `cat:/electronics` matches every descendant |

The first two are not competitors. Declaring `text` is a decision to treat a blob as prose;
declaring `json` is a decision to address inside it. Both remain, and the reference has to say
which is which — today it describes only the second, and describes it wrongly.

- ✅ [**J1 — A facet field cannot be written to**](docs/ROADMAP-ARCHIVE.md#j1--a-facet-field-cannot-be-written-to)

### J2 — A JSON field should mean subfield addressing

📋 **Planned.** The substantive item, and the one with a migration in it.

**Today `json` and `text` are the same field.** Measured on one document carrying
`{"color":"red","size":42,"nested":{"brand":"acme"},"tags":["a","b"]}` in both:

| Query | `json` field | `text` field |
|---|---|---|
| `f:red` | matches | matches |
| `f:color` | matches | matches |
| `f:acme` | matches | matches |
| `f:42` | matches | matches |
| `f.color:red` | **no match, silently** | no match, **reported** |

The cause is one line: the write path serializes the value with `serde_json::to_string` and calls
`add_text` on a field declared with `add_json_field`. A JSON field is handed a string, so it holds
one text blob and no paths. The type costs a different schema declaration and delivers nothing.

**The query half already works.** Tantivy's `Schema::find_field` splits `f.color` into a field and
a JSON path, so a subfield query parses today and reaches the right field — it simply finds
nothing there. `unresolvable_fields` deliberately lets a dotted name through when its root is a
real field ("whether the path is valid for that field's type is the parser's judgement"), so
nothing upstream objects either.

**Which makes the current failure the worst of the three shapes.** On a `text` field a path is
rejected and reported as a discarded clause, so a caller learns. On a `json` field it returns zero
hits with no note — indistinguishable from "nothing matched". An agent reading that reports an
empty answer to a question the index could have answered.

**The change is one arm of the write path**, and only that one. Each declared type has its own
arm; the `Json` arm calls `add_text` with a serialization and becomes `add_object` with the map.
The `Text` arm — which serializes a non-string value and indexes it as prose — is untouched, so a
JSON value in a `text` field behaves in every respect as it does today. Nothing about this change
reaches a field the caller declared `text`.
Each leaf is then indexed under its own path and by its own JSON type — measured on the term
dictionary, `{"color":"red","size":42,"nested":{"brand":"acme"}}` writes `color·s red`,
`nested|brand·s acme` and `size·i 42`, where `|` separates path levels and `·s` / `·i` mark the
leaf's type.

**Equality on a subfield comes with it; a range does not.** `blob.size:42` matches, and
`blob.size:>40` fails outright — *"RangeQuery on JSON is only supported for fast fields
currently"*. So `set_fast` is not the optional extra for sorting it first appeared to be: it is
what a numeric comparison on a subfield requires, and comparisons are a large part of why anyone
addresses a subfield at all. Treat the fast column as part of the feature rather than as a later
stage.

**And a JSON subfield is the exception, not the rule** — measured on a plain `i64` field built
without a column, which is the state a declared `"fast": false` describes:

| | plain numeric field | JSON subfield |
|---|---|---|
| `year:2020` / `blob.size:42` | works | works |
| `year:>2020`, `[a TO b]`, `{a TO b}`, `[a TO *]` | **all work, no column** | **needs the column** |
| sort | needs the column | needs the column |

A range on an ordinary numeric field is answered from the inverted index and needs nothing; the
reference already says so ("the `fast` flag is needed for sorting, not for ranges") and that is
now verified. A range *inside* a JSON field is the case that does not, which is the whole argument
for defaulting the column on here and the reason the reference will need an exception written into
that caveat once J2 lands.

**Decisions this item has to make, none of them forced by the code:**

- **`expand_dots`, and it is narrower than it first looked.** A dotted query path is split on the
  dots *whatever* this option says, so `k8s.node.id:5` already reaches a genuinely nested
  `{"k8s": {"node": {"id": 5}}}` without it. What the option changes is the *document* side: a
  flat key that itself contains dots. Measured on two documents in one field —
  `nested = {"node": {"id": 5}}` and `flatkey = {"node.id": 7}`:

  | Query | `expand_dots` off | `expand_dots` on |
  |---|---|---|
  | `k8s.node.id:5` | nested | nested |
  | `k8s.node.id:7` | — | flatkey |
  | `k8s.node\.id:7` | flatkey | flatkey |
  | `k8s.node\.id:5` | — | nested |

  So off, the two shapes stay distinguishable and a literal dotted key is addressed by escaping;
  on, they collapse — every form finds both. That collapse is the ambiguity Tantivy warns about,
  and whether it is a cost depends entirely on the data: it is a loss only if a caller needs to
  tell a dotted key from nesting, and a gain if they never should have to. **Flat dotted keys are
  what log and telemetry shippers emit**, which is the case that wants it on.
- **A dotted field name shadows a JSON path, and that is the right way round.** Measured on an
  index holding a `json` field `k8s` *and* a text field literally named `k8s.node.id`:
  `k8s.node.id:5` answers from the text field, and the JSON path needs `k8s.node\.id:5`. So the
  rule CameoDB already documents — a dotted field name is written unescaped — keeps working and
  keeps precedence, and no existing index changes meaning. What it costs is that a dotted field
  name prefixed by a JSON field's name makes that part of the JSON unreachable except by escaping,
  which is a schema-design rule to state rather than a defect to fix.
- **Indexing options for the string leaves** — `set_indexing_options` takes the same
  `TextFieldIndexing` a text field does, so tokenizer and position choices apply per JSON field
  rather than per leaf.
- **`set_fast` — decided: on by default, and a caller may turn it off.** A comparison on a
  subfield needs the column, and offering subfield addressing without comparisons would be
  offering half a feature, so the default carries the cost and a schema that does not want it says
  so. Two consequences to keep in view. It is a real cost: a fast JSON field builds columnar
  storage for *every leaf* the documents contain, which for a wide or deep blob is not a rounding
  error against the same data in a text field. And `set_fast` takes an optional tokenizer name for
  the string leaves — `None` keeps the untokenized value, which is what an ordering wants —
  whereas numeric leaves need no such choice.

  **This depended on [OB1](docs/ROADMAP-ARCHIVE.md#ob1--fast-false-is-not-honoured-on-a-numeric-field), which landed
  2026-08-27.** Defaulting `fast` the way the numeric types used to do it is what caused that
  defect: an unconditional assignment in `normalize_after_deserialization`, over a `bool` that
  could not express "unset". Implemented the same way here, a caller's `"fast": false` on a json
  field would have been silently overwritten and the override this item promises would not have
  existed. It is now a two-line change instead: add `Json` to `FieldDef::fast_by_default`, and read
  the value through `FieldDef::is_fast()` like every other type. Nothing in this item resolves
  `fast` itself.

  `sortable` would then have to report per path rather than per field, which is a listing question
  no item currently holds — [A4](#a4--what-a-schema-listing-says-about-id-for-projection-and-for-sorting)
  was the nearest and closed without touching it, since a JSON subfield is not the case it was about.

**The two term spaces are disjoint, and that decides the migration** — measured by writing one
document each way into the same field and querying both:

| Query | Document written as text (today) | Document written as an object (J2) |
|---|---|---|
| `blob:red` | matches | **no match** |
| `blob:color` | matches — the *key* is a token | no match |
| `blob.color:red` | no match | matches |
| `blob.size:42` | no match | matches |

A query with no path reaches only the empty path; a query with one reaches only that path. Nothing
overlaps. So J2 does change what an existing query against a `json` field finds, and there is no
version of it that reads both forms.

**It needs no migration path anyway, and this is the point of the phase.** The remaining type
holds nothing worth preserving. A `json` field is today *indistinguishable from a `text` one* —
same terms, same matches — so nothing is expressible through it that a `text` field does not
already express, and nobody choosing bag-of-words search had a reason to reach for it. (The facet
half shipped with the same property: it became writable only in J1, so no index holds facet data
that predates the fix.) What the phase changes is what the type will mean, not what any existing
index relies on.

So: **a stated boundary, not [reindex](#d1--reindex) first.** An existing `json` field's documents
keep the terms they were written with and stay findable by the query that finds them today; they
gain paths when they are next written. `text` holding JSON is untouched in every respect — same
declaration, same terms, same queries — which is the lane any existing deployment is actually
using. Recording it here so the absence of a migration is a decision with a reason behind it
rather than an omission.

**What does not change**, and it is what keeps the risk down: nothing about what a search
*returns*. Document bodies come from redb, so a hit's `blob` field is the object as written either
way. `add_object` changes only what matches.

### J3 — The flattening lane, and the reference that describes neither lane correctly

📋 **Planned.** Documentation, plus one type-dependent rule.

A JSON value on a `text` field is serialized and tokenized as prose. That is by design and stays:
it is how a caller says "make this blob searchable without declaring its shape", and it costs one
field and no schema work. Its properties are worth writing down rather than leaving to be
discovered — keys are indexed alongside values, so `blob:color` matches; nothing is typed, so
`blob:42` is the string `42`; and there are no paths.

**The reference is wrong about both lanes.** It documents `json` as "searchable only as
unstructured text — its keys and values are indexed as a bag of words and paths into it cannot be
queried", which is an accurate description of *today's json field* and will be an inaccurate one
the moment J2 lands. And it says nothing at all about the text-flattening lane, which is the
pattern actually in use. After J2, `field.subfield:value` stops being a single entry in
`NOT_SUPPORTED` and becomes type-dependent: supported on `json`, reported as an error on `text`.

**One trap belongs here regardless of J1 and J2.** A path-shaped value is read as a *regex* by the
query grammar: `f:/electronics/phones` on a text or string field parses `/electronics/` as a regex
literal, regexes are disabled, and the clause is dropped and reported — while the same syntax on a
facet field works only because `normalize_facet_query` quotes it first. So a caller storing paths
in a text field must write `f:"/electronics/phones"`. That is true today, undocumented today, and
independent of everything else in this phase.

---

## K. Phase 19 — Field metrics: min and max 📋 Planned

Scoped 2026-08-27, measured against tantivy 0.26.1 — the version already pinned — rather than read
off its documentation. Opened because the question "what is the lowest and highest value of this
field" has no answer in the API today, and it is not an exotic one.

**What exists today, and it is not nothing.** A match-all sorted to one hit gives the exact
extreme: `{"query": "*", "sort": {"field": "score", "order": "asc"}, "limit": 1}` for the minimum,
`"desc"` for the maximum. Measured the same day: `*` is a match-all, `*:*` is **not** — its clause
is dropped and reported — and a document missing the field sorts **last in both directions**, so it
never comes back as a false extreme. Two searches, exact values, and the value arrives from redb
rather than from a column, which matters below. What it costs is a full search per extreme, and it
answers only for a field the index can sort on.

**Why an aggregation instead.** Tantivy ships `tantivy::aggregation` in its default feature set, so
this needs no dependency change: `MinAggregation` and `MaxAggregation` are requested as
Elasticsearch-compatible JSON and return `SingleMetricResult { value: Option<f64> }`. It reads the
fast column instead of collecting documents, and — the reason it is worth building rather than
documenting the sort trick — it answers both extremes in one pass over the matching set, honouring
the query, which the sort trick cannot do without two round trips.

**Measured, 2026-08-27**, by running min and max over an in-RAM index of two documents. Everything
in this table is a fact the implementation has to respect, and three of them are surprises:

| Field | Result | What it means for this phase |
|---|---|---|
| `i64` with `FAST` | `{"lo": -5.0, "hi": 42.0}` | works, exact in this range |
| `i64` without `FAST` | `InvalidArgument: 'Field "i64_plain" is not configured as fast field'` | the column is required, exactly as it is for a sort |
| `f64` with `FAST` | `-1.6666666666666667` / `14.0` | works, and the native type is already `f64` |
| `u64` above 2⁵³ | `9007199254740993` → **`9007199254740992.0`** | **lossy.** The result is an `f64`, so an integer past 2⁵³ comes back wrong |
| `date` with `FAST` | `1.767225595e+18` | **nanoseconds since the epoch**, in an `f64` |
| `text` without `FAST` | same not-a-fast-field error | refused, not answered |
| `string` **with** `FAST` | `{"lo": null, "hi": null}` | **silently empty.** No error, no panic — a null that reads as "no data" |

Three consequences follow, and they are the substance of the phase rather than details of it:

- **The `f64` return is not a formality.** For `i64` and `u64` the exact value is in the column and
  is destroyed on the way out. Either the phase documents the 2⁵³ boundary, or a caller who needs
  an exact integer extreme is pointed at the sorted-top-1 path, which reads the value from redb and
  is exact at any magnitude. Recommended: report the metric, document the boundary, and say plainly
  which path is exact — inventing a lossless path through an API that returns `f64` is not
  available.
- **A date must be rendered, not returned.** `1.767225595e+18` is not an answer any caller wants,
  and it is not a form CameoDB accepts back as a query literal. The metric on a date field must
  come back as RFC3339, which is what every other date surface uses. The `f64` step at 2026 is
  ~256 ns, so second and millisecond precision survive and nanosecond precision does not — worth
  one sentence in the reference and no more.
- **A fast text field must be refused by type**, not passed through. It returns `null`, which is
  indistinguishable from "the field is empty" and is the worst of the three outcomes: the caller
  gets a shaped, confident, meaningless answer. No panic risk was found here — checked
  deliberately, because `panic = "abort"` in the release profile makes a thread panic a node
  outage, and [OB2](docs/ROADMAP-ARCHIVE.md#ob2--a-facet-field-cannot-be-written-to) records one such panic reached from a
  document body. Tantivy's own conversion helper *does* panic on a non-numeric column type, so the
  absence of a panic here is a property of how the metric resolves its accessor, not a guarantee to
  rely on: refusing by type keeps it from being tested in production.

### K1 — min and max in the engine

📋 **Planned.** One entry point in `crates/storage`, alongside `search_documents` and sharing its
query path so a metric is over the same matching set a search would return: query parsing, shadow
mapping and clause discarding all behave identically or the two disagree about what the query
meant.

The guard is the interesting part, and it already has a shape to copy. `unsortable_sort_field` in
`node_orchestrator.rs` refuses an unsortable sort **before any shard runs**, so the caller gets a
`400` naming the field instead of an empty page assembled from per-shard failures. A metric on a
field with no fast column is the same class of request and gets the same treatment, with the same
`fast` / `sortable` distinction reported: `fast` is the declaration, the column is what the built
index carries, and only the second one can answer. A metric on a field whose *type* cannot produce
one — text, string, bytes, ip, facet — is refused there too, by type, before the null can be
returned.

Empty is `null`, not `0`: a query matching nothing has no minimum, and `0` is a value.

### K2 — the merge across shards and nodes

📋 **Planned**, and straightforward, which is worth stating explicitly so it is not over-built: the
minimum of the shard minima is the minimum, the maximum of the maxima is the maximum, and `null`
from a shard that matched nothing is skipped rather than treated as zero. Both levels of the
existing fan-out need it — shards within a node, then nodes through the `RouterActor` — and both
already merge search results, so the metric rides the same responses.

Two things to keep in view. A partial failure has to stay visible: a metric merged from three
shards when four answered is wrong in a way that no shape of the response reveals, so it follows
the same `errors` convention the federated search already uses rather than being silently narrowed.
And if this ever grows past min and max, merging final results stops being correct — an average of
averages is not an average — at which point tantivy's own path is
`DistributedAggregationCollector`, which returns `IntermediateAggregationResults` for merging and
`into_final_result()` at the end. Min and max do not need it; anything with a denominator does.

### K3 — the surface

📋 **Planned.** The recommendation is a `metrics` block on the existing search request rather than a
new endpoint, because the query, the routing, the authorization, the scatter-gather and the MCP
search tool all already exist and a separate endpoint would duplicate every one of them:

```json
{"query": "status:active", "limit": 0, "metrics": {"min": ["score"], "max": ["score", "created"]}}
```

`limit: 0` is already count-only — `total_hits` and no hits, skipping the body fetches — so it
composes into "aggregate only" with nothing new invented. The same request with a real limit
returns hits and metrics together, which is the common case for a UI that shows a range alongside a
page.

What the surface work is: the request and response types, the client SDK and one CLI flag, then the
MCP side — the tool schema, and a line in `syntax.rs`, which is the single source rendered into the
`search_index` description, the `validate_query` reference, the per-type `query_hint` and the README
block. An agent that cannot see the metric will not ask for it, so the reference entry is not
optional polish; it is how the feature becomes reachable. `describe_index` already reports `fast`
and `sortable` per field, and a metric is answerable exactly when `sortable` is true and the type is
numeric or a date — so nothing new has to be advertised per field.

**Non-goals, so the phase does not grow into a framework.** No average, sum, count-distinct,
percentiles, histograms or terms aggregation, and no facet counts — the last is already a recorded
non-goal of [J1](docs/ROADMAP-ARCHIVE.md#j1--a-facet-field-cannot-be-written-to). Min and max are worth their surface
because they answer a question about a field's range that nothing else in the API answers; the rest
are a different phase with a different design question, and each one merges differently.

**Rejected, and recorded so it is not proposed again.** `Column::min_value()` and `max_value()` in
`tantivy-columnar` give whole-column extremes with no scan at all, which is tempting and wrong:
they ignore the query and they ignore deleted documents, whose rows are removed from redb at once
and from the index at the next commit. They answer "what did this column ever hold", not "what does
this query match" — a different question, and one that gets more wrong the longer an index has been
written to.

---

## N. Pre-0.3.6 whole-code review ◐ Partial

Reviewed 2026-10-07, at `ae98807`, on the question *what is interconnected, and what in it is
not idiomatic Rust or not a coordinated design*. Five read-only passes ran in parallel — `storage`;
the server's `node/`; the coordinator, swarm, `main.rs` and config; the request-facing layers
(`http_server/`, `auth`, `authz`, `audit`, `posture`, `ratelimit`); and `client`, `mcp` and
`bench` — and the claims that would change behaviour were then re-read by hand. Where an item
says **checked**, the code was read and the described path exists as written; where it says
**reported**, it rests on a pass's reading alone and is to be confirmed before it is fixed. Nothing
was built or run for this review.

**The standard it was measured against, set 2026-10-05:** operations that are *coordinated and
correlated* — one function per thing the system does, each with a reason to exist — and not code
added to carry one scenario (a special-case branch, a side channel, a near-copy of a helper, a
flag threaded through for one caller).

**What the review found, in one paragraph.** Ten defects (seven checked by hand, three
reported), the worst in behaviour that shipped (a security gap, a schema that depends on write batching, a first-boot memory budget),
and one pattern behind most of the rest: *one operation written two or more times, the copies
having drifted*. The single and batch write paths, bulk write and bulk delete, the actor's and
the engine's `orch_*`/`engine_*` pairs, five ways to sync the shard map, four ways to reach a
peer, a REPL that re-parses the CLI grammar — each pair is where a defect sits (N2, N3, N6, N8
are all drift between copies). What is sound: no `unwrap` or `expect` that a request or the
network can reach, no lock held across an `.await`, key handling (constant-time compare,
zeroized keys, only key ids logged), `storage::count_drops` as the one primitive under drops,
the sweep and the reconcile, and a `cluster` and `mcp` crate that are cleanly layered.

**How the crates connect.** `server` depends on `storage`, on `client` (for the CLI) and on `mcp`
(behind the `McpBackend` and `McpAuthz` traits); `client` depends on `storage` for its schema
types and date rules; `bench` on `client` alone; `mcp` on no workspace crate. Inside the server,
`node` and `cluster_coordinator` hold each other's `ActorRef`s, and `schema_change.rs`, which
lives in the coordinator, mostly calls node code while the node owns the coordinator's
`SchemaReconciler`. Every HTTP route runs one stack — trace, panic catch, CORS, authz and audit,
timeout, concurrency, body limits, handler — and then reaches the backend three ways: the
router's `ClientOp`, the `admin_*` methods, or the coordinator directly (the catalogue routes).
No crate holds the wire types: the server writes its answers as `JsonValue`, and the router, the
handlers, the client and `bench` read them back by key name.

**The order, agreed here for the release and after it:**

1. **Before the 0.3.6 cut — N1–N10**, the defects, one commit each (N9 and N10 may share one).
   Done, with N10a, 2026-10-07/08.
2. **After the cut, in this order — N11, N12, N13, N14, N15, N16; N17 as each file is next touched.** N12 and N11 first because they
   delete code and remove the copies N2–N8 grew in; N16 last, because the engine as single owner
   removes most of what the split would otherwise move.

- ✅ [**N1 — `HEAD` requests skip authorization**](docs/ROADMAP-ARCHIVE.md#n1--head-requests-skip-authorization)
- ✅ [**N2 — A batched write never learns a new field**](docs/ROADMAP-ARCHIVE.md#n2--a-batched-write-never-learns-a-new-field)
- ✅ [**N3 — A peer's 400 during a schema change is answered as a 503**](docs/ROADMAP-ARCHIVE.md#n3--a-peers-400-during-a-schema-change-is-answered-as-a-503)
- ✅ [**N4 — Node identity is loaded twice, and a broken file is silently replaced**](docs/ROADMAP-ARCHIVE.md#n4--node-identity-is-loaded-twice-and-a-broken-file-is-silently-replaced)
- ✅ [**N5 — The query rewrites find a field name by substring**](docs/ROADMAP-ARCHIVE.md#n5--the-query-rewrites-find-a-field-name-by-substring)
- ✅ [**N6 — Three client errors that lose or mislabel data**](docs/ROADMAP-ARCHIVE.md#n6--three-client-errors-that-lose-or-mislabel-data)
- ✅ [**N7 — The first shard's memory budget is the whole node's**](docs/ROADMAP-ARCHIVE.md#n7--the-first-shards-memory-budget-is-the-whole-nodes)
- ✅ [**N8 — A streaming search broadcast returns local hits only**](docs/ROADMAP-ARCHIVE.md#n8--a-streaming-search-broadcast-returns-local-hits-only)
- ✅ [**N9 — Storage `shutdown` is a fourth commit path, with the race fixed elsewhere**](docs/ROADMAP-ARCHIVE.md#n9--storage-shutdown-is-a-fourth-commit-path-with-the-race-fixed-elsewhere)
- ✅ [**N10 — Storage errors are flattened, and lose their HTTP status**](docs/ROADMAP-ARCHIVE.md#n10--storage-errors-are-flattened-and-lose-their-http-status)
- ✅ [**N10a — A search of an index that does not exist answers 404**](docs/ROADMAP-ARCHIVE.md#n10a--a-search-of-an-index-that-does-not-exist-answers-404)

### N11 — One way to reach a peer

**Planned.** Peers are reached four ways, with different deadlines and different rules for a lost
peer. `Reach::ask` goes through `pool.converse` (the remote timeout, the lost-peer short circuit,
ref invalidation). `SchemaCanvass::peer_schema_for` (`orchestrator.rs:1474`) uses `get_orchestrator`
with its own 5 s timeout and skips both the lost check and the invalidation. `finish_drop_on`
(`:1557`) re-implements `Reach::ask`. `delete_index_cluster` (`coordinator.rs:1737`) uses
`get_orchestrator` with no deadline at all, plus a `RemoteActorRef::lookup` fallback. "Is the
whole cluster here" is `connected < total || any lost` in `reach_whole_cluster`
(`schema_change.rs:231`) and `connected < total` in the canvass (`orchestrator.rs:1438`). The
`SchemaRequired` resend is written in `forward_write` (`:754`) and in `forward_later` (`:5616`);
the `converse` + lookup + `remote_answer` sequence appears at five sites; and there is
`remote_answer` for a peer's failure but no `local_answer` for the local one, so the same failed
`ask` is Io/500 at `router.rs:481`, NotReady/503 at `orchestrator.rs:6775`, and something else at
`coordinator.rs:1715` and `schema_change.rs:70`.

**Change.** One `PeerReach` in `remote_peer_pool` with `ask`, `ask_all` and `require_whole_cluster`
— the deadline, the lost-peer rule and the invalidation in one place — used by the canvass, the
delete, the schema change, the sweep and the forwards, and `RemotePeerPool::ask_orchestrator(node,
&op, carry)` for the resend. `local_answer()` beside `remote_answer()`. This subsumes
`peer_addr` in the bulk fan-out, which is fetched through `GetKnownPeers` and only logged.

### N12 — Dead surface, and the comments that no longer match

**Planned.** Code that nothing reaches, and the doc comments that moved off their items.

- **Shards.** `MicroshardActor` derives `Actor` and `RemoteActor` and has five `Message` impls,
  four registered as `remote_message` (`shard.rs:105`, `:2081–2190`), and is never spawned: shards
  live in a `HashMap` and are called directly. Dead with it are `RemoteError` and both `From`
  impls (`node/mod.rs:883–1013`), `WriteReply`, `BatchWriteReply` (its `errors` is always empty),
  `ShutdownShard` (a handler that duplicates and diverges from `shutdown_all_shards`),
  `WriteRequest.routing_key` (never read) and its "older peer" `serde(default)` and body-id
  fallback, and a `HashMap` in `handle_batch_write` that holds one key.
- **Coordinator.** `RouteShard` (a deprecated stub), `TrackPushFailure`, `ResetPushFailure` and
  `push_failure_count`, `MarkBootstrapComplete`, `GetClusterSnapshot` and `ClusterSnapshot`
  (`#[allow(dead_code)]`), the empty loop in `sync_expected_nodes`, `decide_route`'s unused
  `operation_type`, `existing_shard_ids` computed for one debug line, `CachedCoordinatorRef.cached_at`.
- **Storage.** No callers: `commit_writer`, `apply_batch_and_maybe_commit`,
  `reset_operations_counter_to`, `get_index_field_names`, `IndexStats`. Tests only:
  `apply_write_and_maybe_commit`, `promote_field_to_indexed`, `get_non_indexed_fields`,
  `set_routing_field`, `pending_wal_entries`, `has_open_writer`, `is_index_open`,
  `invalidate_schema_cache`, `get_by_key`. `tantivy_ms` and `tantivy_scan_ms` are always 0
  (`search.rs:1164`) and the server sums them. `SchemaFields` is public only because
  `get_or_create_index` returns it and both server callers drop the result: add
  `open_index(&str) -> Result<()>` and make the other `pub(crate)`.
- **Client.** `sdk.rs:697` `http()` hands out the client that carries the API key — the one thing
  the separate `source_http` client exists to prevent — and has no caller; `get_index_config` and
  `base_url` are unused.
- **Comments.** Doc comments left on the wrong item: `WARMUP_BUDGET` on `LIVE_WRITER_ATTEMPTS`
  and `delete_index_data` on `document_count` (`store.rs:129`, `:2819`), `handle_panic` on
  `StreamedBody` (`routes.rs:340`), `worst_status` fused into `degrade_status`
  (`health.rs:400`), `decide` on a `static` (`authz.rs:532`), `read_key_hash_file` on
  `write_new_secret_file` (`auth.rs:614`), `publish_lost_peers` (`coordinator.rs:460`). Stale:
  "Stage C3 still missing" (`auth.rs:20`, per-index overrides are done), "O(1) via pre-computed
  set" (the set was removed), the `Drop` and `ShutdownReadRuntime` pair on who holds the read
  runtime (`orchestrator.rs:7133`, `:7181`), a doc linking `forward_op_to_owner`, which no longer
  exists.

**Change.** Delete, shard methods taking `(index, id, doc)` and `(index, Vec<WalOp>)` directly;
move each doc comment to its item; confirm each coordinator message has no sender before it goes
(`grep` for the constructor, not the type).

### N13 — `run_change` is four functions in one

**Planned.** `run_change` is about 400 lines (`schema_change.rs:288–684`), calls `reach.release`
by hand 13 times, threads `edit: Option<&FieldEdit>` through for one caller, applies the edit
three times per round (`:414`, `:817`, `:828`), and the other caller reaches `Step::Moved` through
an `unreachable!` (`:205`). `sweep_schemas` builds a `Reach` of peers only (`:1108`), against
`Reach`'s own rule that this node comes first, and reaches `SchemaReconciler` through a
`ClientOp::ReconcileSchema` round trip to the local orchestrator that hands it back to coordinator
code. Peer schema state is read three ways: `SchemaRecords` with a thumbprint (the sweep),
`GetRawSchema` decoded to `IndexSchema` (the reconcile), and `GetRawSchema` probed as JSON for
`"state" == "dropped"` (`dropped_version`, `held_schema`, `orchestrator.rs:1289`, `:1575`).

**Change.** `prepare(reach) -> Round`, with the release on every non-apply exit in one place;
`decide_declaration` and `decide_edit` separately; `apply(round, schema) -> Step`; `run_edit`
composes prepare, `decide_edit` and apply, and `Moved` leaves `Step`. Decode drop records once
(through `records_drop`); give the sweep the `Arc<SchemaReconciler>` directly; one `schema_cluster`
module that owns the change, the reconciler and the sweep, so the coordinator and the node stop
holding each other's halves.

### N14 — Typed boundaries: the wire, the errors, the index name

**Planned.** Three places where a type is missing and a key name, a string or a copy stands in.

- **The wire.** `route_and_handle` returns `JsonValue`, and callers read `"acknowledged"`,
  `"reason"`, `"errors"`, `"items_written"`, `"total_indexes"`, `"name"` or `"index"` by key
  (`catalogue.rs`, `write.rs:287`, `health.rs:106`, `authz.rs:409`). The field description is a
  `JsonMap` built at `orchestrator.rs:3966` and read back by hand in five places (client
  `ingest.rs:1766`, `:589`, `mod.rs:393`, `shell.rs:130`, server `mcp/schema.rs:116`); the ingest
  reply is built with `json!` three times (`write.rs:528`, `:552`, `:614`) and parsed by hand at
  `ingest.rs:1850`; `SearchPayload`, `DocPayload`, the health and admin responses are defined on
  both sides, and a server-side rename already broke `admin workers` once. `mcp` keeps its own
  `SortSpec`.
- **The name.** `validate_index_name` lives in `http_server/catalogue.rs:26` and returns the
  HTTP `AppError`, which `authz.rs` imports, against the direction of dependency; storage enforces
  a second, looser rule (`store.rs:268`); the MCP tools apply neither.
- **The schema record.** `index_record_option: Option<String>` takes any string; the `id` field is
  defined three times (`schema.rs:1200`, `store.rs:1800`, `:1378`); "default", "raw" and
  "WithFreqsAndPositions" are spelled in five places.

**Change.** A serde-only module or crate without tantivy, used by `server`, `client` and `mcp`,
holding `FieldDescription`, `IngestReport`, `SearchPayload`, `DocPayload`, `IndexInfo`, `Health`,
the admin reports and `SortSpec`; the SDK returns typed reports and `search` takes a struct, not
six positional arguments. `IndexName` in storage, used by the gate, storage and MCP. A serde enum
for the record option and `FieldDef::document_key()`.

Also here, because they come from the same missing types: the router's four error body shapes
(`{error, details}`, `{error, message}`, `{error, path}`, the stream's `{status, error}`) and the
axum extractor rejections that come back as plain text, `Health` status as a `String` where an
ordered enum would replace `degrade_status` and `worst_status`, and the caller's identity carried
as two extensions (`Caller` and `Authz`) where one extractor would do.

### N15 — Per-index state, and node state, each with one owner

**Planned.**

- **Storage.** `HybridStore` keeps 13 maps keyed by index name (`store.rs:324–389`).
  `drop_index_caches` lists them by hand, `shutdown` clears a different subset (it misses
  `fields_cache`, `warmed_generations`, `warmup_states` and `open_indexes`), and
  `index_init_locks` and `schema_locks` are never removed, so they grow with every index name ever
  used — and tenants choose index names. **Change:** one `DashMap<String, IndexState>`.
- **Node.** The actor owns `shards` and `routing_ring` and republishes clones into the engine's
  `ArcSwap`s (`publish_engine_state`, `orchestrator.rs:4468`); the engine duplicates eight fields,
  is `Option` until `spawn_worker_pool`, and falls back to an empty peer pool while the actor's is
  `None` (`:4235`). That is the root of `orch_write`/`engine_write`, `orch_delete`/`engine_delete`,
  `orch_search`/`engine_search` (identical bodies), `orch_bulk_*`/`engine_bulk_*`, and the borrowed
  and owned `WriteCtx`, `BulkCtx` and `OwnedBulkView`. **Change:** `Arc<OrchestratorEngine>` built
  in `new()` as the single owner; the actor keeps `minting`, `mint_rivals`, `schema_changes` and
  the pool handles; its fast paths call the engine and take the slow path on `NeedsActor`.
- **The writer thread.** `spawn_writer_thread` (`shard.rs:707`, 494 lines) has Phase 3 (single
  writes), Phase 3a (mixed) and Phase 4 (batches), which are one algorithm; Phase 4 slices
  `seq_ids` unchecked (`:1042`), the panic 3a's `merged_reply_ranges` was written to prevent.
  **Change:** one `apply_segments(index, Vec<MergedWriteReply>)`.
- **Bulk.** `apply_bulk_write` (`orchestrator.rs:94–371`) and `apply_bulk_delete` (`:372–591`) are
  one pipeline written twice — route, one-hop refusal, assignments, peers, fan-out, the accounting
  assert — and have drifted: delete refuses forwarded items *after* `GetShardAssignments`, write
  before; the refusal keys differ (`document N:` and `{id}:`); `forward_write` fails when
  `items_written` is missing, `forward_delete` defaults it to 0. **Change:** `partition_by_owner`
  and `forward_share`.
- **Shard map sync.** Five mechanisms: the DHT publish at bootstrap, the stability push
  (`coordinator.rs:538–583`), `RegisterLocalShards` into `ExchangeShardsWithPeer` into a query
  and a push and a fallback push (`:806–857`, `:2261–2320`), the `PeerDiscovered` push and pull
  with a hand-rolled 5× backoff (`:1417–1510`), and the periodic `SyncShardMaps` pull (`:2379`).
  **Change:** one exchange, run on the events that change a shard map — a peer connecting
  (`PeerDiscovered`), this node's shards registered or moved, a topology change — each answered
  by one pull-and-push with the peer concerned, retried on that peer's own failure rather than
  on a clock. The four push paths and the 5× backoff fold into it, and every
  `RemoteActorRef::lookup` fallback goes (the pool is always set, `main.rs:458`). The periodic
  `SyncShardMaps` pull, added for OB20 because a partial ring had no event that repaired it, is
  replaced by the event that should have: a node that finds a connected peer's shards missing
  from its ring asks that peer, as the canvass already does for a schema. Kept, if at all, only
  as a rare safety net, not the mechanism. The architecture prefers event-triggered coordination
  to ticking pull or push tasks (2026-10-07); the 30 s schema sweep (`spawn_schema_sweep`, N13)
  is held to the same rule.
- **Smaller owners.** The ring is built from shards twice by forging a `NodeIdentity`
  (`coordinator.rs:222`, `:883`, `orchestrator.rs:5001`): `ConsistentRing::insert(id, &tokens)`;
  the health rules are written three times: `ClusterState::from_counts`; the "settled" predicate
  (`!fields.is_empty() && state != Dropped`) has six copies and the "live" filter eight:
  `IndexSchema::is_settled()` and `is_live()`.

### N16 — The split of `orchestrator.rs`

**Planned**, last in the order, after N15's engine change. `orchestrator.rs` is 7,196 lines and
its `impl NodeOrchestrator` is one block of about 3,100 lines (`:3250–6385`) mixing lifecycle,
schema, catalogue and writes, under one section banner. The rule agreed for L11–L13 holds: few
files, grouped by feature and architectural meaning, no per-function fragmentation. The cut the
pass proposed, by responsibility:

| Module | Holds | ~Lines |
|---|---|---|
| `orchestrator.rs` | the struct, `Answer`, `Message<ClientOp>`, `OnActor`, `run_on_actor`, `handle_client_op`, `UpdateTopology` | 600 |
| `engine.rs` | `OrchestratorEngine`, `execute`, `engine_*`, `worker_eligible` | 550 |
| `worker_pool.rs` | outcomes, the job and slot types, the worker loop, spawn, shutdown, publish | 1,150 |
| `placement.rs` | `CoreLayout`, `ShardPlacement`, `WriterPin` | 250 |
| `write_path.rs` | `Placed`, the write and bulk contexts, forwarding, `orch_write`, `orch_delete`, `orch_bulk_*`, `forward_later` | 1,700, about 1,000 after N15 |
| `schema_cache.rs` | `SchemaCache`, `schema_from_*` | 150 |
| `canvass.rs` | carry and held helpers, `SchemaCanvass`, `find_schema_in_cluster`, the mint race | 900 |
| `schema_authority.rs` | staged validation and evolution, persist, prepare, apply, rebuild, reservations | 1,100 |
| `catalog.rs` | `describe_fields`, `schema_response`, get_config, `validate_query`, `list_indexes` | 700 |
| `lifecycle.rs` | `new`, setters, directories, hydrate, shard creation, registration, shutdown, `Drop` | 1,100 |

The same pass names the functions over 150 lines and where they split: `staged_schema_validation`
(264 lines, into `resolve_origin()` returning `SchemaOrigin { Existing, Adopted, Mint { dropped_at
} }`, then validate, then evolve — which also replaces the mutable `is_initial_creation`),
`handle_broadcast` (483, into `merge_search` and `merge_cluster_indexes`), `list_indexes` (210),
`spawn_worker_pool` (204), `search_documents` in storage (about 650, into `parse_and_report`,
`collect`, `resolve_ids`, `fetch_documents`, `post_sort`), `main()` (about 1,000, into
`build_node`, `start_cluster`, `serve`, `shutdown`), `posture::evaluate` (420, one function per
rule), `write_stream_handler` (290), `dispatch_interactive_command` and `complete_tokens` in the
client REPL (which N17 replaces).

### N17 — Smaller items, filed so they are not lost

**Planned.** None of these blocks a release; each is a place the next change will cost more than
it should, or a defect with a small reach.

- **Shutdown.** The second signal's "force" does nothing: `SHUTDOWN_IN_PROGRESS` is checked only
  inside the first `select!` (`main.rs:877–924`), so "Press Ctrl+C again to force" is false; the
  emergency checks between phases cannot fire, since the phase caps sum to 100 s under a 120 s
  limit; and the post-connect sweep, the periodic sweep, the shard-map timer and
  `RegisterLocalShards` run as plain `task::spawn` outside the `peer_tasks` cancellation token that
  `spawn_peer_task` exists for (`coordinator.rs:911`, `:1366`, `:2339`, `:2363`, `:837`).
  **Change:** a watchdog (second signal or deadline, then `exit`), and leave the cluster before
  Phase 3.
- **Persistence.** `persist_snapshot` (`coordinator.rs:414`) is an unordered `spawn_blocking` per
  call and sets `last_persisted_generation` before success, so an older snapshot can commit after
  a newer one: one persister task on a `watch`, as topology already does. The shard checksum
  (`:293`) uses `DefaultHasher`, whose output is not stable across Rust releases, and is compared
  across nodes, so a mixed-build cluster always sees a difference: use `xxh3`, already a
  dependency. `GetStatus` mutates state and logs at `info!` on every call (`:1241`).
- **Swarm.** The event forwarder is a 160-line closure inside `InitSwarm` over an unbounded
  channel with one `ask` per event (`swarm/mod.rs:597`, `coordinator.rs:998–1160`): extract
  `forward_swarm_events`, use `tell`, bound or coalesce. The yamux and swarm config are copied in
  the PSK and non-PSK branches (`:446–492`).
- **Streaming search** logs the caller's query at `info!` (`search.rs:174`) where the plain search
  deliberately uses `debug!`, and attaches no `AuditedQuery`, so with `record_query_text` on,
  streamed queries are missing from the audit trail; the rate check, keyword parse and merge are
  copied between the two handlers. A request the limiter or quota refused (429, 403) is audited as
  `outcome: "allowed"` (`authz.rs:778`), against `Outcome`'s own doc. `classify` runs up to three
  times per request and the same `AuditRecord` is built three times (`authz.rs:737–802`).
- **Storage.** `gather_index_stats(false)` opens readers through `get_reader` and
  `admit_open_index`, which can close and evict an index, so a plain `ListIndexes` churns the
  open-index cap (`search.rs:250`): use `document_count()`; `searchable_fields` and
  `sortable_fields` open each index from disk on every call (`:1219`, `:1254`): one
  `built_schema(index)` that prefers the cached reader or writer. `derive_index_schema_from_tantivy`
  decides String against Text by `is_stored && !is_indexed` (`store.rs:1537`), which never matches
  the indexed, unstored column the builder makes, so a string field reads back as text — check the
  tokenizer instead (**reported**). `parse_date_str_to_tantivy` (`schema.rs:781–882`) repeats the
  clamp, `from_timestamp_secs` and the tuple six times.
- **Client.** Two complete JSON reading paths — the push chunk parsers (`ingest.rs:1178–1503`)
  and the serde reader (`:1512–1640`) — where a blocking `Read` over the response stream would
  serve both and remove about 400 lines. A compressed remote source is downloaded twice, once to
  sniff it (`:1893`) and again to open it; `detect_schema_from_source` loads a JSON document twice
  (`:2227`); format sniffing runs `from_utf8` on the 64 KiB prefix, so a cut inside a multibyte
  character skips the JSONL check and the file is read as one JSON document (**reported**);
  `SourceAnalysis` allows impossible states (format, `Option<data>`, `Option<delimiter>` vary
  independently, hence the `expect`s in `load_csv`). A column the existing index lacks is read by
  `parse_csv_cell`, which turns `007` into the number 7 while the profiler calls it text, and the
  server then creates an `i64` field (**reported**). The REPL re-parses the CLI grammar and has
  drifted from it: no `--limit` or `--offset` on its `search`, `-e` only there, a literal `1..=16`
  against `MAX_PARALLEL`, and `split_whitespace` collapses spacing inside a quoted phrase. **Change:**
  parse the REPL line with `try_parse_from` on a clap enum that wraps `ClientCommand`, with one
  `execute(&client, cmd)` for `run_cli` and the REPL.
- **Config.** `config.rs` is cohesive in scope but 1,800 lines, and `validate_network` mixes HTTP,
  TLS, CORS and PSK in about 170: split into `config/{model,load,validate,psk}.rs`, as
  `overrides.rs` already is. The `default_search_limit` clamp in `apply_overrides` (`:1077`) changes
  a bad value where `max_search_limit = 0` is refused.
- **Small.** `TantivyFieldType::to_string` returns `&'static str` and shadows `ToString` (rename
  `as_str`, add `Display`, make it `Copy`); `parse_exact_id_query` returns `(String, bool)` with the
  bool always `true`; `sort_by_key` reads a directory on every comparison (`store.rs:3911`,
  `sort_by_cached_key`); `budget - min_budget` can underflow and divide by zero when min equals max
  (`:2020`); the cluster crate's doc says the node UUID is v4 where production derives v5 from the
  `PeerId`; `Retry-After` is built in three places; `spawn_worker_pool` uses `expect`; the
  `forwarded` flag threads through about a dozen functions where a `Hop` enum would say it; the
  swarm's `connected_peers` counts dials started.

**Exit criteria for the 0.3.6 cut.** N1–N10 each closed with a test that fails before the change
and passes after it (N1 by a reader key refused `HEAD /_admin/memory`; N2 by one schema from a
single write, a two-write batch and a `_bulk`; N3 by a 400 from a peer; N4 by a node that refuses
to start on a corrupt identity file; N7 by the first shard of *N* receiving the same budget as the
last); `scripts/validate/all.sh` and the 38-check cluster suite green on the cut binary; the
CHANGELOG entry for each that changes behaviour (N1, N2, N3, N4, N6, N7). N11–N17 are not 0.3.6
work, and the release is not held for them.

## O. Post-0.3.6 review — the base for the next iteration ◐ Partial

Reviewed 2026-10-08, at `73b8fd5` (the 0.3.6 tag), on the question *does the code hold the
architecture's stated invariants, and where does it pay for what it does not hold*. Read as a
database-systems pass — durability ordering, the convergence protocol, overload behaviour —
with N read first, so what is filed below is what N did not already file. `cargo clippy
--workspace --all-targets` is clean; 653 unit tests pass.

**What the review found, in one paragraph.** The components hold their contracts: the write
path's locking discipline, the checkpoint stamped inside the Tantivy commit, worker-pool
accounting released by `Drop`, the shed-before-permit check, writer-thread supervision, and
the `Answer::Later` pattern that keeps peer waits off the mailbox are all sound where read.
What the system lacks is a *convergence protocol* for cluster metadata: the shard map merges
on recency alone, with no versioning, which the periodic 10 s pull currently papers over and
Phase 15 (migration) will expose — O3 is the item the rest of the group's order exists to
serve. Two smaller items close out the operational hazards: remote asks that can wait
forever, and a `wal_sync = false` mode that can leave the search index durable ahead of the
store. The remainder is code shape, and it has the same cause N named: the system has grown
by accretion of fixes, each correct, and the incident history lives in the code comments.

**The order, agreed for the iteration:** O1–O2 first — small, and each closes an operational
hazard. O3 next: it is the prerequisite N15 does not name, because N15's event-driven
exchange only works once a stale map cannot win. N15 then runs as amended by O3. O4 after —
it touches the same wire types. O5 folds into N16 and N17 wherever those items are taken up.
O6 last, reconciling the architecture document after the code it describes has moved.

- ✅ [**O1 — A durability barrier makes `wal_sync = false` safe**](docs/ROADMAP-ARCHIVE.md#o1--a-durability-barrier-makes-wal_sync--false-safe)
- ✅ [**O2 — Every remote conversation runs under a deadline**](docs/ROADMAP-ARCHIVE.md#o2--every-remote-conversation-runs-under-a-deadline)

### O3 — Shard maps merge on a version, not on recency

**Planned.** Re-read 2026-10-18 against `7696b4a` and the OB26 runs; now the prerequisite of
[OB27](#ob27--a-replaced-node-never-leaves-the-cluster)'s `forget`, which makes it the next cluster item
rather than a Phase 15 one.

Three defects share one cause — the cluster's only notion of shard-map currency is "whatever
arrived last":

- `MergeRemoteShards` rewrites every entry it carries, including shards owned by third nodes
  (`coordinator.rs:2088–2116`); only the *sender's own* shards are pruned. Node A's stale view
  of node C overwrites node B's fresher one until the next tick flips it back. A node's shard set
  cannot change during its process's life — `RegisterLocalShards` is sent once, at startup — so
  this is live only when a set shrinks across a restart (a shard that fails to hydrate, a shard
  directory removed): third nodes re-add the missing shard on every pull, the owner removes it on
  its own, and the ring flips every 10 s. **Phase 15 makes it common** — a migrated shard is
  precisely two views of one owner disagreeing — and OB27 cannot remove a node without it.
- *Corrected 2026-10-18:* this bullet said the checksum (`:293–319`), which hashes
  `document_count` and `storage_bytes`, never matches under write load. Both fields are always 0 —
  `NodeOrchestrator::shard_metadata` is the only producer and hard-codes them — so the checksum is
  stable and the push's "identical, skip" does skip. Still three definitions of what changed: the
  checksum, the merge's routing-only test (`:2104`) and `PeerShardDiscovered`'s count test
  (`:1860`), and the first and third start meaning something the day counts are filled in.
- The periodic pull that papers over the first is O(N²) full-map transfers every 10 s, and it
  bypasses the short-circuit: it sends checksum 0, so every pull is a full merge — observed
  2026-10-18, every node merging every peer's whole map each tick.

**Change, one item:** each node owns a monotonic `epoch: u64` for *its own* shard set — bumped
on any change to its shards, persisted in the snapshot; the wire map becomes
`HashMap<NodeId, { epoch, shards }>`; a merge replaces an owner's entry only when
`incoming.epoch > known.epoch` (the per-endpoint version, ~100 lines). The checksum then
hashes the epoch vector — routing data only — in `xxh3` per N17, cached and invalidated on
generation change; `PeerShardDiscovered` stops bumping generation on counts. **This amends
N15:** its "one exchange on events" is safe only once a stale map cannot win, so N15's
shard-map-sync item runs after this; and the residual anti-entropy N15 calls a "rare safety
net" becomes digest-first — exchange epoch vectors, pull only newer owners — at a
minutes-long interval rather than a 10 s full-map pull.

### O4 — The request budget crosses the wire

**Planned.**

F7 shedding measures `REQUEST_STARTED_AT`, a task-local: a forwarded `ClientOp` starts a
fresh clock on the peer, which then runs work for a client the first hop already abandoned —
and the router's retry in `handle_remote` spends the peer's writer time again. **Change:**
carry `remaining_budget_ms` on forwarded ops and let the peer lane shed on it; document in
API_REFERENCE that a remote write answered 503 may have been applied — retry is safe because
puts and deletes are keyed by id.

### O5 — The Rust surface, folded into N16 and N17

**Planned.** Small idiomatic items, each cheap inside work already agreed:

- **Comments carry the incident archive.** `orchestrator.rs`, `shard.rs` and `store.rs` read
  as incident histories ("OB14 measured…", "the mistake M0-j already caught once"). The
  invariants they state are load-bearing; the archaeology is not — it is a third of
  `orchestrator.rs`'s 7,100 lines. With N16's split: each comment keeps the invariant
  ("the writer is held from sequence reservation until the document is added") and cites its
  measurement (`// OB14`), while the narrative moves to this file, which already holds the
  incidents. From here on the rule holds for new comments: the invariant lives in code, the
  evidence lives in ROADMAP.
- `exchange_shards_with_peer` returns `Box<dyn Error + Send + Sync>`; the server crate's rule
  is `anyhow`. Converted as part of O2's edit of the same function.
- `Ordering::SeqCst` on every counter — correct but unreasoned; `Relaxed`/`AcqRel` wherever
  nothing depends on ordering. One audit pass, not per-site.
- `apply_batch_attempt` recurses to retry a detached writer (`store.rs:3046`); a `loop` to
  `LIVE_WRITER_ATTEMPTS` reads as what it is.
- Peer identity is tracked three times — `PeerBook` in the swarm, `peer_meta` in the
  `InitSwarm` forwarder, `cluster.peer_nodes` — and reconciles by convention. One owner, in
  the coordinator, when N17 extracts `forward_swarm_events`.

### O6 — The architecture document says what the code does

**Planned.** ARCHITECTURE.md claims "no background polling or timeouts" while the code runs a
10 s shard-map pull, a 30 s schema sweep and a 5 s seed check; §4.3 serialises hits by
`doc.to_json` where the code hydrates them from redb; §7.2 names the writer mutex where the
writer thread is the real serialisation point; ADR 002's code sample shows a WAL carrying the
document where it now carries only the id. **Change:** reconcile after O3 lands — the sync
section is what O3 rewrites — and add to the docs' own conventions that the claims in this
file are checked against the code each review, as this roadmap's own reconciliations are.

### F10 — Fast-field projection: the page's fields answered from the columns

💭 **Deferred 2026-10-15, deliberately.** After the deadline and two-phase layers the
remaining cost of a large page is the redb read and the JSON parse behind each surviving
document, and for the workloads this node serves today that is a small share of the request;
the layer buys little and adds a second answer path whose values can differ from the stored
ones. Not planned, and the design below is the starting point if a workload changes that —
see **Reopen when**. The fourth layer of the search-economics work opened by the TS5 incident
(2026-10-08): a 310-value `IN` query matching 88M documents asked for one field at
`limit 10,000` and refused the endpoint new sessions for ~25 minutes, because `limit` bounded
the answer but never the work — every shard materialised `offset + limit` full documents and
the merge threw all but the page away.

Three of the layers are shipped. `a8c61f8` put the request's budget inside the walk —
`DeadlineQuery` rides the parsed query and a tripped check surfaces as a typed
`SearchDeadlineExceeded` (408 at the edge, refused whole by the gather) — and pushed the
caller's projection onto `SearchRequest.fields`, so hits leave the shard already trimmed.
`7c794eb` split the shard search into `search_keys` (parse, walk, resolve → `KeyHit`) and
`fetch_documents` (redb → reconstruct → project): a merge now orders keys and fetches only
the documents that survive its window. Measured on a 1.5M-doc 4-shard bench index,
`fetched_docs` for `limit 10,000` fell 40,000 → 10,000 and warm latency 2.5–7× — and `build`
(the JSON parse of each surviving blob) plus `fetch` (the redb reads behind it) are now most
of what is left.

**This item.** When a projection is *column-coverable* — every requested field is `id` or a
shadow name (the value is the document key itself, already in hand) or a schema field that is
`is_fast()` of an eligible type — `search_keys` reads the values in the same per-segment pass
that resolves ids and answers them on `KeyHit.doc`. `fetch_documents` is then asked only for
hits without one — zero when the projection is covered, so the fetch round trip disappears
for exactly the workload that caused the incident. A hit whose column a segment lacks falls
back to the fetch phase per id, as a doc-map miss already does; `fields: None` (the full
document) never qualifies. For a sorted search the sort field's value arrives free — the
collector already read it.

**What a fast projection is allowed to say** — the indexed view, not the stored one; the same
contract Elasticsearch's `fields`/`docvalue_fields` carries. `String` (keyword), `I64`, `U64`
and `F64` round-trip exactly for the values that index. A value that did not index (a `5.5`
under an `i64`) reads as absent rather than projected. A single-element array reads as its
scalar — the column cannot tell `["a"]` from `"a"`. Two decisions open:

- **Dates.** The column holds `DateTime`; the stored shape — RFC 3339 string, epoch seconds,
  RFC 2822 — is unrecoverable from it. Either emit epoch seconds (the shape `SortKey::Date`
  already reports, documented as the canonical rendering) or exclude `Date` in v1 and let a
  date in the field list take the redb path.
- **Text.** An analysed field's fast column holds raw strings — faithful for string-valued
  documents, not for the non-string values `add_json_value_to_doc` indexes as serialized
  JSON. Same choice: in with the contract stated, or out of v1.

**Schema posture, agreed:** `fast` stays a per-field opt-in — a recommendation schema
designers apply to the fields their use case projects, never a requirement; everything
uncovered falls back to the fetch phase unchanged, and `id` stays fast by default.
`describe_index`/`_config` should say which fields qualify, so an architect marking a field
`fast` knows what it buys.

**Where it hooks in** (as the code stands after `7c794eb`):

- `crates/storage/src/search.rs`, `search_keys`, resolve stage (the block that builds `keyed:
  Vec<KeyHit>`): the per-segment pass that already reads the `id` column — one
  `sorted_ords_to_term_cb` walk when a segment contributes ≥ 32 hits packed within 4× their
  count, a stored-document read otherwise. The projected columns are read for the same
  `DocAddress` list in the same loop; `opts.fields` is already on `SearchOptions` but `search_keys`
  ignores it today ("answered by the fetch phase").
- `crates/storage/src/lib.rs`: `KeyHit` gains `doc: Option<JsonValue>` (`#[serde(default,
  skip_serializing_if = "Option::is_none")]` — a mixed-version cluster must still read the old
  shape); `SearchTimings` should report how many hits were answered from columns, so the
  response can say which path a page took.
- `search_documents_with` (the one-shot composition) fetches only the hits whose `doc` is
  `None`; the string-sort candidate fetch inside `search_keys` already projects to one field and
  is the first place a column read could replace it.
- `crates/server/src/node/mod.rs` + `search.rs`: `SearchKeysRequest` has no `fields` today —
  the gather would pass the normalized projection (after `normalize_projection_fields`, so `id`
  is already the shadow name on a shadow index). `group_window_by_shard` builds the phase-two
  requests from the windowed `KeyHit`s and must skip those that carry a `doc`; the assembly step
  already stamps `_score` and `_sort_key` and applies `apply_field_projection`, so a column doc
  goes through the same last step as a fetched one and a cross-node merge sees no difference.

**Coverability is decided from the built index, not the declared schema.** `FieldDef::is_fast()`
is the declaration; the column exists only if the index was built with it, and
`SchemaFields::built_types` already records that the two can disagree (an edit waiting for its
rebuild, an index older than the declaration). So the per-segment lookup (`fast_fields().i64(..)`,
`.u64`, `.f64`, `.str`) is the test, and a segment without the column sends its hits to the fetch
phase rather than answering them empty. Never coverable: `Boolean`, `Bytes`, `Ip`, `Json`, `Facet`
(`can_be_fast` is false — the builder does not consult `fast` for them); fields added by a write
(`new_learned`, non-indexed, so no column until promoted); `_`-prefixed metadata other than what
the gather itself stamps.

**Behaviours the column path changes, to settle before building:**

- **Freshness.** The fetch phase reads redb, the source of truth at the time of the call; a
  column reads the last *committed* segment (`commit_interval_ms`, 2000 by default). A document
  deleted or updated since the commit is dropped or re-read today ("deleted between the phases")
  and would be answered from its old committed values. Either accept near-real-time and document
  it, as Elasticsearch's `docvalue_fields` does, or keep a redb existence check per hit — which
  keeps most of the cost this item exists to remove.
- **Multi-valued fields.** A list indexes as several column entries (`add_json_value_to_doc`);
  the stored array's order and any repeated values are not guaranteed to come back from the
  column. A one-element list reads as its scalar. Verify tantivy's ordering and de-duplication
  for each column type before promising a shape.
- **Shadow indexes.** The identifier replaces `id` with *every* shadow name carrying the same
  value (`reconstruct_shadow_fields_owned`); a projection naming `id` or a shadow name is
  answered from the document key with no column read, and must produce the same keys, in the
  projection's order, as `apply_field_projection` does.
- **Field order.** `apply_field_projection` emits requested fields in the caller's order and then
  the `_` metadata; the column doc is built in that order so the two paths serialise
  identically.
- **Dates** also lose the stored original at index time when clamped to tantivy's safe range
  (`Date clamped to Tantivy safe range`), so the column cannot be more faithful than the index.
- **Text** fast columns are built with `set_fast(None)`: the whole untokenized value, so a
  string-valued document round-trips; a number or object written to a text field was indexed as
  its serialized JSON and would come back as a string.

If built, the lean is to exclude both `Date` and analysed `Text` from v1 and cover `id`, shadow
names, `String`, `I64`, `U64` and `F64` — the types whose column is the value.

**Test shape.** Parity is the property: for every covered projection the column path and the
fetch path return identical documents, so the suite runs each case through both (a switch on
`SearchOptions` forces the fetch path) — scalar and list values, an absent value, a value that
did not index, a shadow index, a sorted search, a segment without the column (a field declared
fast after the data), and a document deleted after the commit. `two_phase_test.rs` and
`projection_test.rs` hold the harness for the first two phases.

**Reopen when** `stats.timings_ms` shows `fetch` + `build` dominating the request for a
workload whose projections are column-coverable — a large `limit` over a few scalar fields, the
TS5 shape — measured on a real index with the fields declared `fast`, not on a synthetic bench.
Until then `fast` stays what it is today: a sort and range column.

## P. Post-0.3.7 features ◐ Partial

Features agreed after the 0.3.7 release, for the iterations that follow it. Each is scoped against
the code as it stands at `1cf4f66`, so the paths and line numbers below are where the work starts,
not a promise they will not move.

- ✅ [**P1 — A hit's arrays are capped the way its hits are**](docs/ROADMAP-ARCHIVE.md#p1--a-hits-arrays-are-capped-the-way-its-hits-are)
- ✅ [**P2 — One declaration of an index's default fields**](docs/ROADMAP-ARCHIVE.md#p2--one-declaration-of-an-indexs-default-fields)
- ✅ [**P3 — A schema read is a schema write**](docs/ROADMAP-ARCHIVE.md#p3--a-schema-read-is-a-schema-write)
- ✅ [**P4 — Ip fields queried the way addresses are**](docs/ROADMAP-ARCHIVE.md#p4--ip-fields-queried-the-way-addresses-are)

### P5 — Native json fields: paths, typed values, ranges

📋 **Planned.** Filed 2026-10-21 from the json analysis behind OB28/OB29.

**Today.** A `json` field is built as tantivy's json field (`add_json_field(name, TEXT)`,
`store.rs`) but written as one string — the value serialized and `add_text` on the field's root
(`schema.rs`). So it is a bag of words: `cells:kn` matches any key or value inside, a path
(`cells.unit:kn`) never matches, a value has no type (`5` is the word "5"), and no range,
comparison or sort can run. Since OB28/OB29 those clauses are reported as dropped rather than
answered with a zero or a 500. The stored document is whole and returned unchanged.

**Measured** with a throwaway tantivy test, one json field fed both the object and today's text:

| Query | object + text | object + text, `fast` |
|---|---|---|
| `cells:kn`, `cells:unit` (bag of words, as today) | ✓ | ✓ |
| `cells.unit:kn` (path) | ✓ | ✓ |
| `cells.amount:5` (typed number) | ✓ | ✓ |
| `cells.amount:>3`, `cells.amount:[1 TO 6]` | tantivy refuses | ✓ |
| `cells.when:[2024-01-01T00:00:00Z TO *]` | tantivy refuses | 0 — an RFC 3339 string stays text |

Feeding both keeps every existing `field:value` query meaning what it does. A value at a path is
matched without a fast column; a range or comparison on a path needs one — `RangeQuery` on json
runs only from the fast column.

**Decided** (2026-10-21):

1. A json field declared or inferred from here on indexes paths (`paths: true`). A stored schema
   without the key reads as `paths: false`: today's text-only build, behaviour and thumbprint, until
   the index opts in and is rebuilt.
2. A string inside a json value that parses as RFC 3339 is written as a date, so date ranges at a
   path work.
3. A json field is `fast: true` by default, so ranges and comparisons at a path work — search
   needs no column, ranges do. The cost is one column per distinct path: cheap for a small regular
   object (a table row), large for a wide or irregular one (a threat-intel record); `fast: false`
   declines it, and `schema detect` reports the path count it profiled so the choice is visible.

**Design.**

- *Schema.* `paths` (and `expand_dots`, tantivy's `expand_dots_enabled`, for keys holding a dot) on
  a json field; `fast` admitted for json in `FieldDef::can_be_fast`. All built-column properties:
  in the thumbprint, reported by `GET /_config` (P3), a change refused with `409` on a populated
  index.
- *Write.* A `paths` field gets the object (`add_field_value`) beside the flattened text; arrays of
  objects index each element under its path; RFC 3339 strings at any depth as dates. The stored
  document is unchanged.
- *Build.* `JsonObjectOptions` from `TEXT`, `set_fast(Some("raw"))` when fast,
  `set_expand_dots_enabled` when asked.
- *Query.* `screen_clauses` keys on the field's mode rather than on "is json": on a `paths` field a
  path takes values, phrases and sets, and ranges and comparisons when fast; a range on the whole
  field stays dropped (no root value to range on); a `paths: false` field keeps today's notes.
- *Discovery.* The client profiler records each json field's paths, their value types and coverage;
  `schema detect` and `--report` print them; `describe_index` lists a field's leading paths (capped)
  and `validate_query` checks a path against them; the MCP json hint and syntax reference say what
  each mode takes; the API reference gets a *JSON fields* section.
- *Sort on a path* (`sort: cells.amount`) reads the path's dynamic column, as the ip sort reads
  its column (P4), the merge key typed by the column — after the rest.

**Cost.** The json field's postings roughly double (paths beside text); fast adds a column per
path; one more walk of each object on write. Existing indexes change only when they opt in and are
rebuilt; a node on an older build reading a `paths` schema builds text-only and its thumbprint
differs, so a cluster is upgraded before such a schema is applied.

**Tests.** A path, a typed value, a date, and ranges at a path on a fast field; the bag of words
unchanged; a `paths: false` field unchanged, notes included; arrays of objects; the thumbprint and
`GET /_config` round trip with the new properties; the profiler's path report.

### P6 — A hit says which array elements matched

📋 **Planned.** Filed 2026-10-21; priority not yet decided. Picks up the second out-of-scope note
of [P1](docs/ROADMAP-ARCHIVE.md#p1--a-hits-arrays-are-capped-the-way-its-hits-are).

**Today.** The P1 cap keeps the first N elements of each array in stored order. A clause such as
`domains:"bad.example.com"` matches an element anywhere in the array, often far past N, so the
hit shows none of what made it a hit. Ordering makes it worse: an array stored sorted returns its
alphabetical head, so the matching element is reliably absent from a capped read. The match is
still found — the search index sees the whole array — but the caller cannot tell which element it
was without fetching the array whole, which past `max_array_limit` it cannot.

**What a hit gains.** Beside `_truncated_arrays`, keyed by the same paths:

```json
"_matched_elements": {"domains": [{"index": 812345, "value": "bad.example.com"}]}
```

- The array itself is untouched: still the first N in stored order. Injecting matches into it
  would break that contract and hide which elements were added.
- Matches inside the head are reported too, so every hit names its evidence.
- `array 0` with matches is the compact form: lengths, and only the elements that matched.
- At most `matches` elements per array, the first found in stored order.

**Where the facts are.**

- *The fetch phase has no query.* `search_keys` parses and walks; `fetch_documents`
  (`storage/src/search.rs`) builds every hit — redb read, shadow reconstruction, projection, cap —
  and on a cluster runs on the owning shard from a `FetchDocsRequest` that carries only ids,
  `fields` and `array_limit` (`server/src/node/mod.rs`).
- *The built tantivy query is not enough.* `BooleanQuery::query_terms` visits `MustNot` clauses,
  `RangeQuery` keeps its bounds private, and `PhraseQuery` has no slop getter. Walking it would
  report `NOT` terms as matches and lose ranges, which is what a CIDR block becomes (P4).
- *The screened AST is.* Inside `PreparedQuery::parse` (`storage/src/query.rs`) the AST after
  `screen_clauses` carries occurrence, field, phrase, slop, prefix, ranges, sets and CIDR ranges.
- *Elements are separate in the index.* tantivy leaves a position gap between the values of a
  field (`POSITION_GAP`), so an exact phrase cannot span two elements: a term or phrase hit always
  has one element that caused it.
- *The coordinator caps again after the merge* (`cap_hit_arrays`, `server/src/node/search.rs`),
  and `cap_document_arrays` skips only `_truncated_arrays` — a new metadata key holding arrays
  would be cut by that second pass.

**The approach.**

1. *A matcher per field, from the screened AST,* built once per fetch batch. Positive leaves only;
   a `MustNot` subtree is skipped. A bare term expands to the default fields the parser used.
   - text and string: the literal and each element tokenized by the field's own analyzer
     (`index.tokenizer_for_field`), so the matcher and the index agree; a term, an exact phrase as
     contiguous tokens, a phrase with slop, a prefix on the last token;
   - numeric, date and ip: typed equality, ranges and sets;
   - `*` and presence: no element evidence, nothing reported.
2. *Applied in `fetch_documents`, after the projection and before the cap,* so it sees each array
   whole and only for fields the hit returns and the query names. A field projected away is not
   scanned.
3. *Cheap on large arrays.* For term and phrase leaves a case-folded substring prefilter
   (`memchr::memmem`) rules out most elements before any tokenizing; the scan stops at the
   per-array match limit. A short-string array of a million elements is a pass of tens of
   milliseconds, not a tokenization of each element.
4. *The query reaches the shard.* `#[serde(default)] match_query: Option<String>` on
   `FetchDocsRequest` and on `SearchOptions`; the owning shard re-parses it, once per batch. A peer
   on an older build ignores the field and reports no matches — the coordinator cannot make up for
   it, since it only ever sees arrays already cut. Clean degradation, documented.
5. *The cap leaves metadata alone.* `cap_document_arrays` skips every `_`-prefixed key, not only
   `_truncated_arrays`.

**The surface,** the same three layers as `array`:

| | Array elements (P1) | Matched elements (this item) |
|---|---|---|
| Node default | `[search] default_array_limit = 1000` | `[search] default_match_limit` — proposed on, small (10) |
| Node maximum | `[security.limits] max_array_limit = 100000` | `[security.limits] max_match_limit` |
| HTTP body | `"array": N` | `"matches": N` |
| Inline keyword | `... array N` | `... matches N` |
| MCP | `array` | `matches`, with `maximum` advertised |

`matches 0` turns it off. Whether it is on by default is the open decision: on gives every hit
its evidence at the cost of one filtered scan per queried array field; off keeps today's
responses byte-identical until a caller asks.

**Limits to document.**

- A phrase with slop can, rarely, match across two adjacent elements; that hit has no single
  element to report.
- Matching is the query's positive clauses per field, not a re-evaluation of the whole query: for
  `A AND B` over two fields, each reports its own elements.
- A json value or an array of objects is matched against the object's text, and the whole object
  is reported.

**Tests.** Storage: the matcher per leaf kind — term, phrase, slop, prefix, set, numeric and date
ranges, a CIDR — `NOT` excluded, default-field expansion, the prefilter's agreement with the
tokenizer, the per-array limit. Server: the HTTP body, inline keyword and MCP parameter, refusal
above the maximum, a document whose 300k-element array matches at position 250,000, `array 0`
with matches, a projected-away field reporting nothing, the coordinator's re-cap leaving
`_matched_elements` whole, and a two-node search answered by the owning shard. **Docs:**
`docs/API_REFERENCE.md`, `docs/CONFIGURATION.md`, the MCP syntax reference
(`crates/mcp/src/syntax.rs`), `cameodb.example.toml`, and a `CHANGELOG.md` entry.

---

## Q. Post-0.3.8 features 📋 Planned

Features agreed after the 0.3.8 release, for the iterations that follow it. Each is scoped against
the code as it stands at `c04c63b`, so the paths below are where the work starts, not a promise
they will not move. Both came out of loading a multilingual full-text corpus as a test (350,798
passage rows, 376 MB of text, in 17 languages; "the sample" below).

### Q1 — Ghost fields: index columns derived from another field, under a condition

📋 **Planned.** Filed 2026-10-26; revised 2026-10-27 against the code — a change to a ghost applies
from the change on, ghosts leave hits at the shadow rewrite, and the read and write paths that had
to know.

**Today.** Every analyzer an index wants on a text costs a copy of the text in redb. A Tantivy
column is fed from the document body, by field name (`SchemaFields::document`,
`storage/src/schema.rs`), and redb keeps that body whole, so to search one text stemmed and exact,
or stemmed by the language of its row, the document carries the text once per field. Measured on
the sample, rows with only a parent key and `lang` beside the text, each variant loaded on its own
fresh node and measured after the merges, allocated size:

| Layout | redb | Tantivy | Total |
|---|---|---|---|
| text in a stemmed language field and in `text_exact` (what a schema needs today) | 1,055 MB | 350 MB | 1,407 MB |
| the stemmed language field only | 524 MB | 180 MB | 705 MB |
| `text_exact` only, no stemming | 524 MB | 190 MB | 715 MB |
| **ghost: the text once, a stemmed column beside the exact one** | **about 524 MB** | **about 330–350 MB** | **about 860–875 MB** |

The last row is derived, not measured: the Tantivy side is the same content as the first row (a
column per analyzer), and the redb side is the second row's. It is 38% below today's layout and 22%
above having no stemming at all.

**The feature.** A field property, `ghost`, says where a column's values come from instead of the
document body:

```json
"text_en": {
  "type": "text", "tokenizer": "en_stem",
  "ghost": {"source": "text_exact", "when": {"field": "lang", "in": ["en"]}}
}
```

Written in prose as `ghost(text_exact, lang, ["en"])`, `ghost(text_exact, lang, not ["en"])` for
the complement, and `ghost(text_exact)` for a ghost with no condition. A ghost is an indexed text column that is never stored and never in a document: for
a document whose body has the source field and which matches the condition (`when.field` equal to
one of the `in` values, or not, for `not_in`), the source's value is added to the ghost's column, under
the ghost's own tokenizer. Otherwise the ghost holds nothing for the document. redb keeps the body
as written, so the text is stored once.

- **A list of values.** `in` is an array, one value or many: Serbian, Slovenian, Bosnian,
  Montenegrin and Croatian text all feed one `hr_stem_fold` column,
  `"in": ["hr", "sr", "sl", "bs", "cnr"]`. A scalar is read as a list of one.
- **The discriminator is a string, integer or boolean field** of the document, and every value in
  the list has its type. Strings compare exactly (case included). An array-valued discriminator
  matches `in` when any element is in the list. A document without the discriminator, or with a
  value of another type, does not match `in`.
- **`not_in` is the complement of `in`, decided 2026-10-27.** `"when": {"field": "lang",
  "not_in": [...]}` matches exactly the documents that `in` with the same list does not: an array
  discriminator none of whose elements is in the list, and a document with no discriminator or one
  of another type. One rule rather than a second set of edge cases, and it makes a pair of ghosts on
  one list cover every row with the source exactly once. A condition has `in` or `not_in`, not both.
  It is the easier form wherever the rest is the open set: "every language without a stemmer" is
  `not_in` the stemmed ones, and a language that first appears next year is covered from its first
  write without a schema change.
- **A source feeds any number of ghosts**, and a document can match several of them; conditions
  need not be disjoint. Kept in step by hand: a value added to a stemmer's `in` but not to the
  catch-all's `not_in` is indexed in both columns — searchable, and scored twice in a default-field
  search, not lost.
- **A ghost without a condition, decided 2026-10-27.** `when` omitted: every document whose body
  has the source feeds the ghost. `"text": {"type": "text", "tokenizer": "default"}` and
  `"text_stem": {"type": "text", "tokenizer": "en_stem", "ghost": {"source": "text"}}` is a
  single-language index searched by exact form and by stem with the text stored once — the first
  row of the table above, at the size of the last, and the case most indexes have, since most hold
  one language. It is also the way to give an unindexed source one column under an analyzer of the
  operator's choosing, and to try an analyzer beside the column in use before switching to it.
- **A catch-all is optional.** With an indexed source, languages without a stemmer need no ghost:
  the source column, with the default tokenizer, is what they would have had. With an unindexed
  source, a `not_in` ghost is what keeps them searchable (below).
- **The source is an array of strings the way a text column is:** each element is indexed, with
  the position gap between elements a text field already has.

**What the schema refuses,** at `PUT /_config` and `PATCH /_schema`, in the way `validate_tokenizers`
and `validate_default_fields` refuse today:

- a `source` that is not declared, is not `text` or `string`, is itself a ghost (no chains), or is
  `id` or a shadow field — the put path strips a shadow name from the body before the document is
  built (`filter_shadow_fields_owned`), so a ghost on one would always be empty;
- a `when.field` that is not declared, is a ghost, or is not a string, integer or boolean field; a
  `when` with both `in` and `not_in`, or neither; an empty list in either (an empty `not_in`
  would mean every document: omit `when` for that); a value of another type than the field;
- a ghost that is `stored`, `fast` or `id`;
- a ghost with no condition on an indexed source, under the source's own analysis (tokenizer and
  record option, compared as `rebuild_changes` compares them): it would build the source's column a
  second time.

**The source may be unindexed, decided 2026-10-27.** A ghost reads its source from the stored
body, not from the source's column, and redb keeps every field of the body whatever its `indexed`
flag says — the put path strips shadow names and nothing else — so the source needs no column of
its own. Its life is its own, and the ghosts it feeds are untouched by it:

- *Declared unindexed.* No column is built for it; its value is in redb, comes back in every hit,
  and feeds the ghosts from the first write.
- *Indexed later* (`PATCH /_schema`, `indexed: true`). `FieldEdit::apply` pins it, clearing
  `learned`, as any promotion does. A shard that holds documents has no column for it, so it is
  `pending_reindex` there (`unbuilt_promotions`) like any field declared ahead of the index.
- *No longer indexed* (`indexed: false`). The builder stops feeding its column (it adds only
  `indexed` fields); the terms already in it stay until a rebuild and are not reachable, since a
  clause naming a non-indexed field is dropped with `non_indexed_field_note`. The ghosts go on
  being fed. `FieldEdit::apply` needs no new check for this.
- *Removed from the declaration* (a `PUT /_config` that leaves it out). Refused while a ghost names
  it, the way `validate_default_fields` refuses a listed field that is no longer indexed: "remove
  the ghost first, or in the same request". `merge_learned` treats a declared field a `PUT` leaves
  out as removed on purpose, and the next write carrying it would add it back as a learned field —
  the ghost would read a field the schema says it does not know. `indexed: false` is the way to
  stop indexing it.
- *A learned source.* A write cannot break the type rule — a learned text field only widens to
  text (`TantivyFieldType::widened`) — but the declaration that names it as a source clears its
  `learned`, as a promotion does, so the source is a declared field from then on.

A query naming an unindexed source has its clause dropped as today, and the note names where the
text is searched instead: "field `text_exact` is not indexed; its text is searched through
`text_en`, `text_hr`, …".

**What an unindexed source asks of the operator.** A row whose discriminator matches no ghost is
not searchable by its text at all. Covering those rows is a catch-all ghost — for an index with a
ghost per stemmer, a `text_other` ghost under the `default` tokenizer with
`"not_in": ["en", "de", "fr", "es", "it", "hr", "sr", "sl", "bs", "cnr"]`, the union of the stemmer
ghosts' lists. A language that shows up later is covered from its first row; one that later gets a
stemmer is moved by adding it to that ghost's `in` and to this `not_in` in one change, which by the
rule below applies to new writes. And the exact word forms of a stemmed language are no longer a
column of their own: only the stemmed one is.

A write whose body names a ghost is refused with `400`, naming the field, rather than dropped:
the value would be neither stored nor indexed, and nothing would say so. The check goes in
`validate_document` (`server/src/node/search.rs`), beside `disagreeing_shadow_field`; and
`evolve_field` (`schema.rs`) leaves a ghost alone, as it leaves a shadow field.

**Where the facts are.**

- *One builder for the write and the replay.* `SchemaFields::document` (`storage/src/schema.rs`) is
  called from exactly two places, the put path (`store.rs`, before the redb transaction opens) and
  the WAL replay (`store.rs`, in recovery), so a document indexes on recovery as it did when
  written. The ghost columns are added there, reading the stored body, and so need nothing the
  recovery does not have. The put path also strips shadow fields from the body before it is stored
  (`filter_shadow_fields_owned`): a precedent for the body and the index differing, in the other
  direction. Two rules in the builder:
  - *The body loop skips a ghost's name.* A body written before its field became a ghost still
    carries the value (below); the column is fed from the source alone, or a replay of such a
    document would index both copies.
  - *The ghosts are read from the `IndexSchema` the builder is handed,* not cached in
    `SchemaFields`: an index opened from disk gets its `SchemaFields` from the Tantivy schema alone
    (`load_fields_from_existing_index`, `store.rs`), which knows nothing of sources or conditions.
    A walk of the ghost list per document is cheap beside tokenizing the text; cache it only if a
    bench says otherwise.
- *The column build already takes any indexed text field.* `create_schema_from_definition`
  (`store.rs`) skips only `id`, `_seq`, non-indexed and shadow fields; a ghost is `indexed`, not
  shadow, and builds as a text column with its tokenizer and record option. `SchemaFields`
  records it in `indexed_fields` and `built_types` like any other.
- *The schema owns the rest.* `FieldDef` gains the property (`schema.rs`, beside `is_shadow`); the
  fingerprint (`fingerprint_of_effective`) includes source, field, operator and values — and a
  marker of its own for a ghost with no condition, so that none hashes like any list — since two nodes must
  agree on which documents a column holds; `normalize_after_deserialization` leaves it alone;
  `rebuild_changes` knows it (below); `GET /_config` reports it, `stored` false.
- *The schema takes a ghost out of every hit, the way it puts a shadow name in.* Every hit is built
  in `fetch_documents` (`storage/src/search.rs`) — both search paths and the `id:VALUE` lookup — and
  its one rewrite of the stored body, `reconstruct_shadow_fields_owned`, is where a ghost's name is
  removed, gated by a `has_ghost_fields()` as the shadow branch is by `has_shadow_fields()`. Before
  the projection and the array cap, as the shadow rewrite is, so a document written before its field
  became a ghost (and still holding the value) and one written after answer alike.
- *A projection naming a ghost says why it is empty.* `unknown_projection_fields`
  (`server/src/node/search.rs`) counts every schema field as known, so `fields: [text_en]` would
  return hits without it and no word. It gets the dropped-clause note other projections get, naming
  the source: "`text_en` is a ghost; its value is in `text_exact`".
- *A sort on a ghost is refused.* `unsortable_sort_field` lets a non-fast `text` field through and
  orders the candidates from the stored body afterwards; a ghost has no value there, so every row
  would sort as missing. It refuses a ghost by name, pointing at the source.
- A ghost can be a default search field (it is an indexed text column), so `default_fields` can
  list the language columns and `text_exact`.

**A change to a ghost is valid from the change on.** The rule, decided 2026-10-27: a schema change
that turns a field into a ghost, or changes the values of a ghost's condition, applies to writes
from that point; documents already written keep what they were indexed with until they are written
again. Nothing is backfilled and nothing is refused for being populated, with one exception the
engine imposes — a built Tantivy index's columns are fixed, so a column that does not exist yet
cannot be written into. `rebuild_changes` then knows three cases:

1. *An indexed field becomes a ghost* (today's `text_en`, fed from `text_exact` on `lang`), or a
   ghost becomes a plain field again. The column, its type and its tokenizer stay, so documents
   written before and after share it and answer the same queries. Old bodies keep the value in
   redb — the saving applies to documents written after the change, until the old ones are written
   again — and the hit rewrite above keeps it out of the answer. Accepted; reported as
   `applies to new writes`, not as a conflict. Today `rebuild_changes` compares type, tokenizer,
   record option and `fast` only, and would see no change here at all: it has to report this one.
2. *A ghost's `source` or `when` changes* (`bs` added to `text_hr`). From the change on, new `bs`
   rows feed the column; old ones do not until rewritten, and a value taken out leaves its old rows
   in the column. Adding a condition to a ghost that had none, or taking it away, is the same kind
   of change. Accepted and reported the same way.
3. *A ghost is added to an index whose shard already holds documents.* That shard has no column for
   it: `pending_reindex`, not searchable there until rebuilt, as for any field declared ahead of the
   index. An empty index, or a shard built fresh, gets the column and works from the first write.

A changed tokenizer or type on a ghost stays a conflict refused with `409`, as on any built column:
a query would be analysed differently from the terms the column holds.

The old copies are disk only. Two ways to take them back: write the documents again, or have the offline `cameodb compact --rewrite` ([F11](#f11--the-disk-a-delete-frees-was-never-returned))
drop ghost names from the bodies it rewrites. Case 3, and any wish to make cases 1 and 2 hold for
old rows too, is [D1](#d1--reindex): the condition reads only what redb holds, so rebuilding a shard
from its documents under the new schema fills the ghosts exactly as the writes would have, and D1
stops needing the source to be re-ingested.

**Migrating an index that writes its text twice** is then one schema change and a change in the
writer, no reload. Take an index that writes every row's text to `text_exact` and again to a field
per language: `text_en`, `text_de`, … become ghosts of `text_exact` on `lang`, `text_hr` with
`"in": ["hr", "sr", "sl", "bs", "cnr"]` (after [Q2](#q2--the-croatian-analyzers-read-serbian-cyrillic),
so a writer that transliterated Serbian before writing need not), a `text_other` field that repeated
`text_exact` under the same `default` tokenizer for the languages without a stemmer leaves
`default_fields`, and the writer stops sending the language fields from the same moment — a write
still naming one is refused once it is a ghost.

The two layouts such an index can choose between, both with the text stored once, in its original
script:

| Layout | Columns | Allocated size, sample |
|---|---|---|
| source indexed: `text_exact` searched for exact forms and for the languages without a stemmer, a ghost per stemmer | exact + stemmed | about 860–875 MB (derived; the table above) |
| source unindexed: a ghost per stemmer, and `text_other` a `not_in` ghost for the rest | one per row | about 705 MB — the columns of the "stemmed language field only" row, measured |

The second builds what the "stemmed language field only" row built, with one difference: there the
writer stored only the language field, so text it had transliterated was stored transliterated;
here redb keeps the original script and the analyzer folds it (Q2).

**On a cluster.** The schema is decided once and stored on every node (`PrepareSchema` and
`ApplySchema`), and no document or wire message changes. A peer on a build that does not know
`ghost` deserializes the schema without it (`FieldDef` has no `deny_unknown_fields`) and builds a
plain text column that is never filled. That is not silent: the peer's stored schema then hashes
differently, and thumbprints are compared at apply and at reconcile (`schema_change.rs`), so the
likely result is a divergence reconciled again and again rather than one quiet empty column — to
be confirmed by tracing the reconcile loop. The `PUT` already needs every node connected, but not
at one version. The lean is a gate at `PrepareSchema`: a proposal with a ghost in it is refused
while any peer is on a build that does not know the property, rather than documenting "upgrade
every node first".

**Tests.** Storage: the document builder per condition kind (string, integer, boolean, array
discriminator, missing discriminator, wrong type, several values, several ghosts on a source,
overlapping conditions, an array source), each for `in` and for `not_in`, and an `in`/`not_in` pair
on one list feeding every document exactly once, and a ghost with no condition feeding every
document with the source and none without it; a ghost document and the same document replayed from the
WAL give equal Tantivy documents; a body that still carries the ghost's name indexes the source's
value only; the stored body holds no ghost; the fingerprint moves with the condition;
`rebuild_changes` for each of the three cases and the `409`; every refusal above. Server:
`PUT /_config` with ghosts, a write refused for naming one, a search by stem on a ghost and an exact
search on the source finding the same row, `GET /_config` reporting the property and a saved read
restoring it (P3); a populated index whose plain field becomes a ghost — old and new rows both
found by stem, neither hit carrying the field, the old row's redb body unchanged; a condition value
added on a populated index reaching new rows only; a ghost added to a populated index reported
`pending_reindex`; `fields: [ghost]` answered with the note, a sort on a ghost refused; an
unindexed source feeding its ghosts and coming back in hits, then indexed (`pending_reindex` on a
populated shard), then no longer indexed with its ghosts still fed, and a `PUT` that leaves it out
refused while a ghost names it; a query on an unindexed source answered with the note naming its
ghosts; an update
that changes the discriminator moving the row between ghosts. Cluster: a three-node schema with ghosts, and a
write through each node. **Measure** the table above on the sample, redb and Tantivy
separately, with and without ghosts. **Docs:** `docs/API_REFERENCE.md` (the field reference and the
tokenizer table), `docs/ARCHITECTURE.md` (the write path), the MCP syntax and schema descriptions
(`crates/mcp/src/syntax.rs`, `guidance.rs`, `server/src/mcp/schema.rs`, `discovery.rs`), and a
`CHANGELOG.md` entry.

**Not in this item:** deriving a value by transformation (a transliteration, a lower-casing)
rather than by analysis — the analyzer does that, and Q2 puts the Serbian one there; a
ghost fed from several sources; a condition on more than one discriminator or on a range.

### Q2 — The Croatian analyzers read Serbian Cyrillic

📋 **Planned.** Filed 2026-10-26. Wanted by Q1: a ghost reads the source text as it was written,
so a transliteration a writer does before sending the text (one way to make Serbian Cyrillic
searchable today) has no place to happen.

**Today.** `hr_stem` is a lower-caser and the Croatian stemmer's plain model; `hr_stem_fold` is a
lower-caser, `AsciiFoldingFilter` and the stemmer's folded model (`storage/src/analysis/mod.rs`,
`analysis/croatian/`). Serbian, Slovenian, Bosnian and
Montenegrin text goes to them because the Croatian stemmer serves them well, but Serbian is also
written in Cyrillic, and Cyrillic words reach the stemmer unchanged: a Latin query for `kamen`
does not find `камен`, and neither is stemmed. A third of the Serbian and Slovenian texts in
Q1's sample are Cyrillic.

**The change.** A token filter, placed after the lower-caser in both chains (so before the ASCII fold
in `hr_stem_fold`), mapping the Serbian Cyrillic alphabet to its Latin letters — the 30 letters, with `љ`, `њ` and `џ` to the
digraphs `lj`, `nj` and `dž`, and `ђ`, `ћ`, `ч`, `ш`, `ж` to `đ`, `ć`, `č`, `š`, `ž` — so the stemmer, and
in `hr_stem_fold` the fold, then see what they see for Latin text. `hr_stem` keeps the diacritics the
mapping produces (`č`, `ć`, `đ`, `š`, `ž`), as it does for Latin text: Cyrillic is another spelling
of the same language, and the mapping is all that separates the two. Cyrillic letters outside the Serbian
alphabet (Russian, Bulgarian) pass through unchanged. The mapping is lossless in this direction,
so the original script stays in the source column, which is why Q1 matters here.

**It goes into `hr_stem` and `hr_stem_fold` themselves, decided 2026-10-26.** No new analyzer name. Today a Cyrillic
word reaches the stemmer unchanged and is left as it is, so folding it can only add matches, and
mixed-script text is unlikely in practice. The one effect to document: an index that already holds
Cyrillic text was built with Cyrillic terms, and a query in Cyrillic is now analyzed to Latin ones,
so such an index finds nothing in Cyrillic until it is reloaded. `rebuild_changes` compares
analyzer names, not behaviour, so nothing refuses or reports this; the CHANGELOG entry and the
tokenizer table say it. (The alternative, filed and not taken: a new name such as
`hr_stem_cyr` and `hr_stem_fold_cyr`, which leaves every existing index as it is and is moved to by a rebuild.)

**Tests.** The mapping per letter and per digraph; Cyrillic and Latin spellings of one word
analyzing to the same terms, for a list of Serbian words in both scripts; Latin text analyzing
exactly as it did before the change (a regression over the existing sample); mixed-script text; the
existing `hr_stem_fold_matches_queries_written_without_diacritics` test unchanged; all of it for
both analyzers, and `hr_stem` keeping `č` and `ć` apart where the folded one joins them. **Measure** the
stemmer's coverage on the Cyrillic texts of the sample, transliterated, against the same text
transliterated by hand. **Docs:** the tokenizer table in `docs/API_REFERENCE.md`,
`docs/ARCHITECTURE.md`, and a `CHANGELOG.md` entry.

---


## Delivered work, in the archive

Delivered work is in [docs/ROADMAP-ARCHIVE.md](docs/ROADMAP-ARCHIVE.md), kept in full. Where an item of a group still open
was delivered, its stub stands above in its place; these groups were delivered whole:

- ✅ [**L. Post-0.3.4 review — preparing the next cycle**](docs/ROADMAP-ARCHIVE.md#l-post-034-review--preparing-the-next-cycle--done) — L1–L20
- ✅ [**M. The 0.3.5 goal set — multi-tenant exposure**](docs/ROADMAP-ARCHIVE.md#m-the-035-goal-set--multi-tenant-exposure--done) — M0–M8

The archive also holds the original write-up of every delivered phase, the measurements, and the settled decisions:
what the affinity flags cost, what the audit trail costs, the non-goals recorded so they are not re-litigated, and the
questions asked and answered.
