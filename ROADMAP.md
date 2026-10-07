# CameoDB Development & Optimization Plan

Active work is at the front of this file; everything delivered is in
[Part II — Archive](#part-ii--archive), kept in full. The archive is not decoration: the
measurements and the rejected options are what stop a settled question being reopened, and
several entries exist precisely to say *do not build this again*.

**Last reconciled against the code: 2026-09-01, at 0.3.3.** The 2026-08-26 pass is recorded
under [Reconciliation](#reconciliation-2026-08-26); the 2026-09-01 review is filed in place —
C3–C6, CH8–CH12 and OB3–OB9 — and a re-read on the same day is recorded under
[Reconciliation](#reconciliation-2026-09-01). The 2026-09-08 review of the 0.3.3 and 0.3.4
changesets is filed as its own activity group under
[L. Post-0.3.4 review](#l-post-034-review--preparing-the-next-cycle--planned), held until the
release has settled. The 2026-10-07 whole-code review, taken at the 0.3.6 cut, is filed as
[N. Pre-0.3.6 whole-code review](#n-pre-036-whole-code-review--planned).

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

A phase is ◐ if anything inside it is not ✅. A phase whose every stage is ✅ moves whole into
Part II.

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
| 16 — Boot & OOM recovery at scale | ◐ Partial | Stage 4.2, Stage 3's deeper warming options, and the measurement on the reporting node; E5 (a cap on open indexes) done by M1 |
| 17 — Record deletion | ✅ Done | — |
| 18 — Field types: Facet and JSON | ◐ Partial | J2 and J3 — a json field behaves exactly like a text one. J1 (facet writable) and OB1 (the `fast` three-state prerequisite) are done. No migration for what remains |
| 19 — Field metrics: min and max | 📋 Planned | All of it — no aggregation of any kind exists today. Min and max on a fast numeric or date field, nothing else |
| 14 — Security hardening (posture items C3–C8) | ✅ Done | C3–C8 all closed; C8 by M3 on 2026-09-20 |
| Code health — reviewed at 0.3.1, extended 2026-09-01 | ◐ Partial | Twelve items; CH1, CH8–CH12 done, CH2's server half absorbed by the split, CH2's storage half closed out by L12 |
| L — Post-0.3.4 review: the refactor cycle | ✅ Done | All twenty closed — four defects, six security remainder items, three decompositions, six simplifications, and the retrospective (L20, run 2026-09-19) |
| M — The 0.3.5 goal set: multi-tenant exposure | ◐ Partial | M0 closed; M1, M2, M3, M4, M5 and M7 done — the blocker is cleared, the surface is metered, and tenants are bounded and isolated per index. No feature build remains; M8 closed with the prefix floor and default-field cap, the clause cap deferred. M6 done: the open-loop arms show goodput degrading rather than collapsing on the bulk, single-write and read lanes, after fixing the two defects its arms found — [OB14](#ob14--a-timed-out-request-never-leaves-the-worker-pool-and-the-node-degrades-until-it-is-restarted) and [OB15](#ob15--every-refused-request-was-an-error-line-and-under-write-overload-the-logging-cost-half-the-goodput), and its confirmation run on 2026-09-27 found OB22 costs a single node nothing measurable. What is left is the release cut |
| N — Pre-0.3.6 whole-code review | 📋 Planned | N1–N10 before the 0.3.6 cut: a `HEAD` authorization gap, batched writes that never learn a field, a peer's 400 answered as 503, node identity replaced silently, query rewrites by substring, three client errors, the first shard's memory budget, streaming broadcast, a storage shutdown race, flattened storage errors. N11–N17 after it: one way to reach a peer, dead surface, `run_change`, typed boundaries, single owners, the orchestrator split |

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
  [0.3.2 hardening](#032-hardening-2026-08-1920--done).
- **Two code-health files grew rather than shrank** since the 0.3.1 review:
  `node_orchestrator.rs` 9,300 → 9,683 lines, `storage/src/lib.rs` 7,600 → 8,293. Items CH1
  and CH2 carry the current figures.
- **One defect was observed on 2026-08-13 and never filed here.** `fast: false` on a numeric
  field is not honoured; it is now item OB1.

Everything else verified as the file described it: no `search_after`, no `sched_setaffinity`,
no per-arena `mallctl`, no reindex path, `should_commit_writer` still counts operations only,
`cameodb-bench` still closed-loop (it grew an open-loop mode later — see
[F2](#f2--an-open-loop-load-generator), 2026-09-15), and the four code-health duplications all
still present.

## Reconciliation, 2026-09-01

A re-read of the 0.3.3 review's own fixes, against the tree rather than against the commits that
claimed them. Five corrections; the pattern in four of them is the same, and worth naming: a fix
that closes a defect on the path it was found on, while the *sibling* path — the other arm of a
match, the other framing of the same input, the second hop of the same forward — keeps the
behaviour the fix was written to remove.

- **[OB5](#ob5--one-batch-can-index-the-same-id-twice) was ✅ and is only now done.** Coalescing
  the adds was right; the deletes still read "did this id exist" from each `insert` in turn, which
  a delete earlier in the same batch had already emptied. Reproduced, then fixed and pinned.
- **[OB7](#ob7--the-validator-and-the-writer-disagree-about-floats-in-integer-fields) had a
  sibling.** Element-wise list typing, landed the same day, made a *nested* list read as its
  inner element type — so the disagreement OB7 closed for floats reopened one level down.
- **[OB3](#ob3--a-single-write-or-delete-can-land-on-the-wrong-shard) made an unbounded pattern
  reachable from every write.** Forwarding had no hop limit before, on the bulk paths only; OB3
  extended it to single writes and deletes. Bounded now, with the bulk paths named as still open.
- **Two new items, both from reading the fixes rather than the code they fixed:**
  [OB10](#ob10--the-record-limit-meant-different-things-depending-on-how-the-wire-split-the-line)
  and [OB11](#ob11--reasons-and-totals-that-went-missing-on-the-way-out), plus
  [C7](#c7--a-500-printed-the-nodes-internal-error-text) from the status-classification work.
- **[CH8](#ch8--the-single-write-path-clones-the-whole-schema-and-document) is correct, and its
  documentation was not.** The `has_new_field` guard reads as though it disables type evolution
  for existing fields. It does not change behaviour: `validate_document` sets `needs_evolution`
  only for fields the schema has never seen, and a value an existing field cannot hold is
  *refused* rather than widened — so evolution of an existing field is reachable only from
  initial schema sampling, which is unaffected. `evolve_field` and `should_evolve_field_static`
  now say so, because the next reader will ask.

The two file sizes in [CH1](#ch1--one-scatter-gather-written-twice) and
[CH2](#ch2--the-merge-primitives-deserve-their-own-module) were stale by ~1,300 and ~930 lines and
are current again. Everything else verified as the file described it: CH9's two remaining pieces,
CH10, CH11's two duplicates and two hashes, OB8's dropped offset and OB9's discarded peer errors
are all still exactly as written.

**On the order of work.** The [table](#the-order-of-work) is ordered by what each item costs to
read, and this batch ignored it: everything done since 0.3.3 is an OB or CH item opened on
2026-09-01. Defects before features is the right call and not a reason to rewrite the table — but
the table describes how the *remaining* work should be sequenced, not how work is sequenced when a
review turns up something wrong.

---

# Part I — Active work

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
| [A6](#a6--the-syntax-reference-has-drifted-from-the-engine) | The syntax reference has drifted from the engine | 12 | 2026-08-27 | ✅ |
| [A7](#a7--a-short-page-and-a-stale-count-say-nothing-about-why) | A short page and a stale count say nothing about why | 12 | 2026-08-27 | ✅ |
| [B1](#b1--2f2--cpu-arenas-for-write--read--merge) | 2f.2 — CPU arenas for write / read / merge | 13 | 2026-08-08 | 📋 |
| [B2](#b2--2f3--per-arena-jemalloc-stats) | 2f.3 — per-arena jemalloc stats | 13 | 2026-08-08 | 📋 |
| [C1](#c1--per-index-role-overrides) | Per-index role overrides | 14 | 2026-07-30 | 📋 |
| [C2](#c2--query-complexity-caps) | Query complexity caps | 14 | 2026-08-10 | 💭 |
| [C3](#c3--fail-closed-on-unauthenticated-internal) … [C6](#c6--redact-the-cluster-psk-in-debug) | Posture hardening: four items from the 2026-09-01 review — C3 and C4 done | 14 | 2026-09-01 | ◐ |
| [C7](#c7--a-500-printed-the-nodes-internal-error-text) | A `500` printed the node's internal error text | 14 | 2026-09-01 | ✅ |
| [D1](#d1--reindex) | Reindex | 15 | 2026-08-15 | 📋 |
| [D2](#d2--replication) | Replication | 15 | 2026-08-15 | 📋 |
| [D3](#d3--migration) | Migration | 15 | 2026-08-15 | 📋 |
| [E1](#e1--stage-42--a-max-wal-size-commit-trigger) | Stage 4.2 — a max-WAL-size commit trigger | 16 | 2026-08-19 | 📋 |
| [E2](#e2--stage-3s-deeper-warming-options) | Stage 3's deeper warming options | 16 | 2026-08-19 | 📋 |
| [E3](#e3--measure-recovery-on-the-reporting-node) | Measure recovery on the reporting node | 16 | 2026-08-19 | 📋 |
| [E4](#e4--two-compatibility-paths-with-no-end-to-end-test) | Two compatibility paths with no end-to-end test | 16 | 2026-08-19 | 📋 |
| [F1](#f1--the-cost-of-a-durable-commit-under-read-load) | The cost of a durable commit under read load — deferred: redb has no middle durability level, and building one trades the guarantee this node keeps | — | 2026-08-10 | 💭 |
| [F2](#f2--an-open-loop-load-generator) | An open-loop load generator | — | 2026-09-15 | ✅ |
| [F3](#f3--take-unkeyed-searches-off-the-coordinator) | Take unkeyed searches off the coordinator — standalone half done, clustered half turned down: the published ring lags the coordinator | — | 2026-08-10 | ◐ |
| [F4](#f4--the-bulk-paths-asked-the-coordinator-before-they-knew-they-needed-to) | The bulk paths asked the coordinator before they knew they needed to | — | 2026-09-02 | ✅ |
| [F5](#f5--concurrency-sweep-measured-2026-09-02) | Concurrency sweep on the release build — the operating point, and bulk's serialization measured | — | 2026-09-02 | ✅ |
| [F6](#f6--what-fsync-actually-costs-measured-2026-09-02) | What fsync actually costs — and why turning it off is a reallocation, not a speedup | — | 2026-09-02 | ✅ |
| [F7](#f7--the-request-timeout-sheds-the-client-not-the-work) | The request timeout sheds the client, not the work — measured: goodput goes to zero, not down | — | 2026-09-15 | ✅ |
| [F8](#f8--the-overload-gates-do-not-cover-the-bulk-write-path) | The overload gates do not cover the bulk write path — health fixed, both lanes gated, admission predicting against a measured spread, and bulk folded into the service estimate by `0836df2`. All three items closed; the re-measurement is [M6](#m6--close-and-re-measure-the-bulk-lane) | — | 2026-09-17 | ✅ |
| [CH1](#ch1--one-scatter-gather-written-twice) … [CH7](#ch7--the-string-fast-collector-repeats-the-macros-body) | Code health, seven items — CH1 done | — | 2026-08-16 | 📋 |
| [CH11](#ch11--routing-key-derivation-is-written-four-times-with-two-algorithms) | Routing-key derivation, four spellings and two hashes — closed ahead of the split | — | 2026-09-01 | ✅ |
| [CH8](#ch8--the-single-write-path-clones-the-whole-schema-and-document) … [CH12](#ch12--write-path-serialization-and-round-trip-waste) | Code health, write-path efficiency, five items — all done | — | 2026-09-01 | ✅ |
| [OB1](#ob1--fast-false-is-not-honoured-on-a-numeric-field) | `fast: false` is not honoured on a numeric field — landed ahead of [J2](#j2--a-json-field-should-mean-subfield-addressing), whose override it would otherwise have eaten | 18 | 2026-08-13 | ✅ |
| [J1](#j1--a-facet-field-cannot-be-written-to) | A facet field cannot be written to | 18 | 2026-08-27 | ✅ |
| [J2](#j2--a-json-field-should-mean-subfield-addressing) | A json field should mean subfield addressing | 18 | 2026-08-27 | 📋 |
| [J3](#j3--the-flattening-lane-and-the-reference-that-describes-neither-lane-correctly) | The flattening lane, and the reference that describes neither | 18 | 2026-08-27 | 📋 |
| [OB2](#ob2--a-facet-field-cannot-be-written-to) | A `facet` field cannot be written to — the evidence behind J1 | 18 | 2026-08-27 | ✅ |
| [OB3](#ob3--a-single-write-or-delete-can-land-on-the-wrong-shard) … [OB12](#ob12--the-schema-gate-deadlocked-a-fan-out-against-itself) | Correctness, ten items from the 2026-09-01 review, the re-read of its own fixes, and the 0.3.3 release check — OB3–OB12 all done | — | 2026-09-01 | ✅ |
| [OB14](#ob14--a-timed-out-request-never-leaves-the-worker-pool-and-the-node-degrades-until-it-is-restarted) | **A timed-out request never leaves the worker pool** — a `DashMap` self-deadlock in `should_commit_writer` parked every shard writer thread past a 30s TTL. Found by the first [M6](#m6--close-and-re-measure-the-bulk-lane) arm, fixed and pinned the same day | — | 2026-09-25 | ✅ |
| [OB15](#ob15--every-refused-request-was-an-error-line-and-under-write-overload-the-logging-cost-half-the-goodput) | **Every refused request was an `ERROR` line** — synchronous on the write path's runtime, it halved single-write goodput under overload and failed health. Refusals are now counted into one periodic summary. Found by the M6 single-write arm, fixed the same day | — | 2026-09-25 | ✅ |
| [OB16](#ob16--closing-an-index-from-another-thread-lost-the-writes-in-flight-on-it) … [OB19](#ob19--two-clustered-deadlocks-through-the-coordinators-mailbox) | **The pre-release concurrency audit** — eviction from another thread lost in-flight writes from search (464 of 600 in the test), schema edits and evolution overwrote each other, streaming search ran outside the concurrency limit, and two clustered mailbox deadlocks. All fixed and, where a test can force it, pinned | — | 2026-09-26 | ✅ |
| [OB20](#ob20--a-fresh-cluster-can-keep-a-partial-ring-and-nothing-repairs-it) | **A fresh cluster can keep a partial ring** — 4 of 5 simultaneous starts left one node without a peer's shards (or alone), and nothing re-synced. Fixed with one connection per peer, a seed redial, and a 10 s shard-map pull; 5 of 5 now converge. Found by the new `cluster` validation suite | — | 2026-09-26 | ✅ |
| [OB21](#ob21--a-peer-that-stops-answering-detected-refused-at-once-and-bounded-while-it-lasts) | **A peer that stops answering** — orchestrator forwards now share one deadline, stale peer references go on the first failure, and topology can no longer drop the newest ring. Also fixed: a kameo panic at shutdown (5.5), and a frozen peer is detected by ping within ~40 s, after which requests for it are answered at once (5.4) | — | 2026-09-26 | ✅ |
| [OB22](#ob22--an-orchestrator-waited-on-peers-while-holding-its-mailbox) | **An orchestrator waited on peers while holding its mailbox** — schema canvasses and forwards ran inside it, so two nodes doing either at once waited on each other until a 5 s or 60 s timeout: bulk writes through every node all timed out, and new indexes created through every node ran at 0.1/s, most refused. Fixed; now 510 batches/s and 134 new indexes/s, none failed; several nodes minting one index at once settle it by node id, where 88 of 120 such writes were refused. Found by the cluster suite's new cross-node phase | — | 2026-09-26 | ✅ |
| [OB23](#ob23--recreating-an-index-after-dropping-its-schema-lost-most-of-what-was-written) | **Recreating an index after dropping its schema lost most of what was written** — the drop's record read as a schema, the node re-minted the index from every batch, and a retyped column killed the Tantivy writer, which stayed dead until restart. 1,030 of 16,559 documents landed; now all do, a dying writer is retired and replayed, and the loader counts every failure by its file line | — | 2026-09-27 | ✅ |
| [OB24](#ob24--a-and-not-b-and-a-or-not-b-parsed-cleanly-and-answered-wrongly) | **`a AND NOT b` and `a OR NOT b` parsed cleanly and answered wrongly** — a `NOT` arm under `Must`/`Should` matched nothing and nothing reported it; rewritten to `a AND -b` / `a OR (* -b)` before the parser runs | — | 2026-10-05 | ✅ |
| [F9](#f9--commit-on-a-clock-not-a-count) | **Commit on a clock, not a count** — bulk ingest 1.8–6.6×, a trickle searchable within 2 s, single writes unchanged | — | 2026-09-26 | ✅ |
| [K1](#k1--min-and-max-in-the-engine) | min and max in the engine, refused before any shard runs | 19 | 2026-08-27 | 📋 |
| [K2](#k2--the-merge-across-shards-and-nodes) | The merge across shards and nodes | 19 | 2026-08-27 | 📋 |
| [K3](#k3--the-surface) | The surface: a `metrics` block, the SDK, and the MCP reference | 19 | 2026-08-27 | 📋 |
| [L1](#l1--size-cache-invalidation-by-substring-evicts-neighbouring-indexes) … [L20](#l20--the-retrospective-and-the-sequence-into-the-next-cycle) | Post-0.3.4 review group — all twenty closed; the retrospective's output is [M](#m-the-035-goal-set--multi-tenant-exposure--planned) | — | 2026-09-19 | ✅ |
| [M0](#m0--the-architecture-review-and-the-order-of-work) … [M8](#m8--re-decide-the-query-complexity-caps) | The 0.3.5 goal set — a node exposed on the internet serving several tenants from one process; M0 closed, the M1 blocker cleared, and M2–M8 done; M6's open-loop arms found and fixed [OB14](#ob14--a-timed-out-request-never-leaves-the-worker-pool-and-the-node-degrades-until-it-is-restarted) and [OB15](#ob15--every-refused-request-was-an-error-line-and-under-write-overload-the-logging-cost-half-the-goodput), and were confirmed on the cut binary 2026-09-27. The release cut remains | — | 2026-09-27 | ◐ |
| [N1](#n1--head-requests-skip-authorization) … [N17](#n17--smaller-items-filed-so-they-are-not-lost) | Pre-0.3.6 whole-code review — ten defects (N1–N10) to close before the cut, then the duplication and structure behind them: one way to reach a peer, dead surface, `run_change`, typed boundaries, single owners, the `orchestrator.rs` split | — | 2026-10-07 | 📋 |

---

## A. Phase 12 — MCP Server Integration ◐ Partial

Steps 1–6 of the phase and items 1–4 of the completion track are done; see
[Phase 12 — what landed](#phase-12--mcp-server-integration-for-ai-agents).
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
The second wants [F2](#f2--an-open-loop-load-generator), since an agent's arrival process is
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

### A6 — The syntax reference has drifted from the engine

✅ **Done** 2026-08-27, audited against the query path and then against a running node — which is
the part worth keeping, because probing corrected two claims that reading the code had produced.

**The machinery is sound and that is the point.** `crates/mcp/src/syntax.rs` is the single source
rendered into four surfaces — the `search_index` description, the reference `validate_query`
returns, the per-field and per-type `query_hint` on `describe_index` and `list_indexes`, and the
README block that `crates/mcp/tests/readme_syntax.rs` holds equal to it. `guidance.rs` is
test-pinned *not* to name query forms so the prose cannot drift either. Every correction below
lands in one file and propagates. What has drifted is content, not structure.

**Two entries are now false.**

- **`_seq`.** The rule says it is "present in every index and technically queryable". Stage 7
  retired it: a new index never declares it, `sort=_seq` is refused by name, and every listing
  filters it — `describe_fields`, `sorted_field_names`, `searchable_fields`, `sortable_fields`.
  The rule spends resident context telling an agent to ignore a field it cannot see. Delete it
  rather than correct it.
- **A refused sort is not described as a refusal.** 0.3.2 made a numeric or date field without a
  fast column a `400` naming the field, decided before any shard is asked. `SORT_RULES` says only
  that such a field "needs one to be sorted at all", which does not tell an agent whether the
  request errors or degrades — and that is the distinction that decides whether it retries with a
  different field or reads the results it got.

**Two entries understate what the engine accepts**, which costs an agent a conversion it did not
need to make, or a query form it avoided for no reason.

- **Dates.** The reference says "`YYYY-MM-DD` and RFC3339 are both accepted".
  `parse_date_str_to_tantivy` also takes naive datetimes (space or `T`, optional fractional
  seconds, dash or slash separators), `YYYY/MM/DD`, `YYYY.MM.DD`, `YYYYMMDD`, compact
  `YYYYMMDDHHMM` and `YYYYMMDDHHMMSS`, Unix epoch seconds at 10–11 digits, `YYYY-MM`, and a bare
  `YYYY`. The query path runs literals through the same parser, so every one of those works in a
  range, a comparison and an `IN` set: `created:2024` and `created:[2024-06 TO 2024-08]` are legal
  and undocumented. A literal outside Tantivy's representable range is silently clamped, which is
  also unstated.
- **Count-only.** `limit 0` returns `total_hits` and skips the key-value store entirely — the
  cheapest answer to "how many?" the engine has. It is documented only in the hand-written
  `search_index` schema string, so it is missing from `INLINE_MODIFIERS` and therefore from the
  README and from `validate_query`'s reference.

**Two caveats are thinner than the behaviour.** A prefix that cannot be rewritten as a range
matches the term exactly instead, and says so through the discarded-clause channel — which fails
an MCP call, so the agent needs to recognise it. And a facet path cannot contain a space: the
normalizer ends the path at whitespace or `)`.

**What the node said that the source did not.** Two of the date claims above were wrong when
first written, and both corrections are now rules in their own right: a bare date literal is an
*exact instant*, so `created:2024-06-15` means midnight and matches nothing unless a document
sits on that second — the most natural date query to write and the least likely to work — and a
literal containing a space must be quoted, because an unquoted value ends at the first space and
`12:00:00` is then read as a new clause reporting `12` as an unknown field. The sort refusal was
checked the same way, which is how the item learned that boolean, ip, json and facet are the
types that actually reach it, numeric ones being unreachable while [OB1](#ob1--fast-false-is-not-honoured-on-a-numeric-field)
stood — fixed 2026-08-27, so a numeric field that declines its column now reaches the refusal too.
`crates/storage/tests/date_query_forms_test.rs` pins all of it.

**One defect found while looking**, and fixed here because it is the same surface: two tool
descriptions shipped runs of nine and five literal spaces after each paragraph break. A `\` at
the end of a line in a Rust string swallows the following indentation; a `\n` written into the
string keeps it. A description sits in the caller's context for a whole session, and this is the
one class of defect there that no reviewer catches, because the source looks right. A test now
walks every tool description and refuses a run of two spaces outside the reference's own padded
table.

**Two more, filed rather than fixed:** [OB1](#ob1--fast-false-is-not-honoured-on-a-numeric-field)
reproduced, and [OB2](#ob2--a-facet-field-cannot-be-written-to) — a `facet` field cannot be
written to at all, which makes `field:/path/to/value` an operator advertised in four MCP surfaces
for a field type no document can carry.

**Verified aligned, recorded so it is not re-audited:** the `id:value` fast-path caveat matches
`parse_exact_id_query` condition for condition; a discarded clause does fail an MCP call;
`offset` and its `offset + limit` bound; the approximate-sort mechanics and `_approximate_sort`;
the shadow-field prose; `deny_unknown_fields` on every tool's arguments; the read-only hints and
the deliberate absence of `outputSchema`. The read-only claims in `INSTRUCTIONS` and the
orchestrator skill survive Phase 17 unchanged — deletion is a write, and no tool here writes.

### A7 — A short page and a stale count say nothing about why

✅ **Done** 2026-08-27. New with [Phase 17](#phase-17--record-deletion--done): deletion made a
transient state ordinary that used to be almost unreachable.

A search counts matches in Tantivy and fetches bodies from redb. A delete removes the redb row at
once and the Tantivy term at the next commit, so for the seconds in between a hit is counted and
has no body — `total_hits` says five and four hits arrive. `annotate_search_response` explains an
empty page, a page past the end and an approximate sort, and says nothing about
`hits_returned < min(limit, total_hits − offset)`, which is exactly this case.

The same divergence reaches the catalogue: `document_count` is Tantivy's `num_docs`, so
`list_indexes`, `describe_index` and `get_catalog_stats` over-report a deleted document until the
commit lands and the measurement cache expires.

It matters because of what the session instructions promise — *"never present an incomplete
result as a whole one"* — which an agent cannot honour on a signal it is not given.

**What landed.** `short_page_note` joins the `_warning` notes, on the one condition paging cannot
explain: fewer hits than `min(limit, total_hits − offset)`. It says how many are missing, why the
two numbers can disagree, and what the count then means. Silent on a full page, on a last page
holding the remainder, on an empty result — which `zero_results_advice` already speaks to — and
on a count-only query, which asks for no hits at all. Verified through `tools/call` across all
four states of one delete: clean, short, count-only, and after the commit.

**The count keeps its number**, which was the other decision and the less obvious one. It is
Tantivy's `num_docs`, so it is the count *as of the last commit* — and that is exactly what
`total_hits` is too. Making it read redb instead would give a number no search agrees with, and
two honest-looking figures that never match is worse than one figure with a stated boundary. So
`describe_index`, `list_indexes` and `get_catalog_stats` now say what their counts are counts of.

---

## B. Phase 13 — Stage 2f ◐ Partial

Stages 1 and 2a–2e are done, and both affinity flags stay `false` on measured evidence — see
[Phase 13 — what landed](#phase-13--thread-per-core--memory-operations) and the
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
constraint rather than a curiosity. [Phase 17](#phase-17--record-deletion--done) shipped the
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
[Phase 14 — what landed](#phase-14--security-hardening). C3 is the only stage
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

### C3 — Fail closed on unauthenticated `internal`

✅ **Done** 2026-09-02, ahead of the public 0.3.3 — and **not** as this item specified, for two
reasons that only became visible once it was built that way.

The item asked for `enabled = false` on `internal` to become a `Fail`, with
`allow_unauthenticated = true` as an escape hatch in `[security]`. Both halves were wrong:

- **A `Fail` breaks an in-place upgrade.** A config with `profile = "internal"`, an off-box bind
  and no `[security]` section was valid in 0.3.2 and would refuse to boot on 0.3.3. Not a parse
  error either — `serde(default)` handles a missing field, so the config loads cleanly and *then*
  the node declines to start. A patch release must not stop a working deployment over a value
  nobody wrote. `crates/server/cameodb.toml` is exactly that config, and it is what the DEB and
  RPM install.
- **The escape hatch says nothing new.** `[node] profile` is already the operator's explicit
  declaration of reach, and it cannot be inferred for a non-loopback bind — a node with neither
  refuses to start rather than guess. So `internal` was already stated deliberately, and a second
  setting re-affirming it is one more thing to get wrong and one more value whose absence can
  stop a boot.

**So the tool fails and the node warns**, which is what the item's own second sentence actually
asked for — "have `check-config` exit non-zero on that warning specifically". `Posture` records
`unauthenticated_off_box` as a fact rather than leaving it to be read out of a message, and
`cameodb check-config` exits 1 on it; `--allow-unauthenticated` accepts it and exits 0. The
acceptance lives on the *invocation*, where the operator is, and not in a file that a node has to
boot from. Whether a node may run and whether a config is fit to deploy are different questions,
and only the second one is a deploy step's business.

The bind still decides, not the label: `internal` over loopback has overstated its reach rather
than exposed anything, and neither warns nor fails.

Verified: the three shipped configs fail the plain check with the exposure named and pass with the
flag, reporting the same three accepted warnings the 0.3.0–0.3.2 records describe; and a node
started on the config that fails the check serves requests, so nothing that ran on 0.3.2 stops
running.

**Corrected 2026-09-03, from a real upgrade.** That last sentence was verified by starting the
binary by hand, which is not how the packages start it. The unit installed by the DEB and the
RPM gates `ExecStart` on `ExecStartPre=cameodb check-config` — added in Phase 15 (see the auth
record below), when every check failure was also a refusal to boot — so on an existing
`internal` node the pre-flight exited 1 and systemd never reached `ExecStart`. The split this
item is built on was undone by a file it never touched: the tool failed *and* the node stopped.
The unit now passes `--allow-unauthenticated` on that line, and `scripts/validate/posture.sh`
runs the unit's own `ExecStartPre` flags against the config the packages install, because
neither file was wrong when read alone.

### C4 — The admin API reachable off-box unauthenticated

✅ **Done** 2026-09-02, and **half of its premise was wrong**. C3's gate closes the
unauthenticated case, as the item expected. The other half — "an operator can enable auth and
still leave the admin API exposed to every key rather than to an admin one" — does not happen:
`authz.rs` (116–123) requires `Capability::NodeAdmin` on every `/_admin/*` route, so a `reader`
or `writer` key is refused there regardless of what else it can do.

What was actually wrong was the *message*. With authentication on but no key holding
`node-admin`, the rule reported `/_admin/*` as "reachable off-box and unauthenticated" — which
described the opposite configuration. Those endpoints are then reachable by *nobody*, which is
safe and still worth a line, because an operator who mounted keys expecting to use the admin API
has to be told which capability is missing. The rule now separates three cases: gated on a
`node-admin` key (pass), gated on a capability no key holds (warn, naming it), and genuinely
ungated (warn, naming memory purge, forced commit and writer eviction — reachable only because
C3's hatch accepted it).

Pinned by `an_admin_api_with_no_admin_key_is_closed_rather_than_open`, which asserts the word
"unauthenticated" does **not** appear for an authenticated node.

### C5 — A decompression cap on the streaming ingest path

✅ **Done 2026-09-20** (finding H3), by
[M2](#m2--cap-decompressed-bytes-on-the-streaming-ingest-path). The cap the finding asked for
is there. What the finding assumed — and what the router's own comments asserted — was that
`DecompressionLayer` inflated request bodies. It decompresses *responses*, so the threat was
unreachable and two neighbouring bugs were not. M2 has the account.

### C7 — A `500` printed the node's internal error text

✅ **Done** 2026-09-01, found reviewing the `from_route` classification. `AppError::into_response`
masks a server error's message to `"Internal server error"` — and then returned the unmasked text
in `details` in the same body, so the mask withheld nothing. A `5xx` now answers with the mask
alone and the text goes to the `error!` log, which is where an operator reads it and a caller does
not. A client error still carries its real message in `details`: that one is the caller's to act
on, and six tests read it.

### C6 — Redact the cluster PSK in `Debug`

✅ **Done 2026-09-07**, in `33399d6` — and carried as 📋 until **2026-09-19**, when
[M7](#m7--redact-the-cluster-psk-in-debug) went to do it and found it already done. The original
finding (H4): `ClusterConfig` derived `Debug` while holding `psk: Option<String>`, so any future
`debug!("{:?}", config)` would have leaked the key.

What is actually there now is more than the entry asked for. `ClusterConfig` has a hand-written
`Debug` that prints `<redacted>` in place of the key; `psk` is `#[serde(default,
skip_serializing)]`, so no config dump can carry it, which is why `overrides.rs` needs a
`NEVER_SERIALIZED_SETTINGS` list for it; `ClusterPsk` — the resolved 32 bytes — has its own
redacting `Debug` that prints a non-reversible fingerprint, and a `Drop` that scrubs the bytes
with `write_volatile`; the swarm logs that fingerprint rather than the key; and `load_psk`'s
refusal reports only the *length* of a malformed value, deliberately, because an error message
is the one place a bad secret would otherwise reach a log.

The stale marker is the lesson, not the code: this sat ✅-in-fact and 📋-on-paper for twelve
days, which is the same bookkeeping failure [L20](#l20--the-retrospective-and-the-sequence-into-the-next-cycle)
corrected seven dates for.

### C8 — REST has no rate limit, and anonymous MCP callers share one bucket

✅ **Done 2026-09-20 by [M3](#m3--meter-the-write-surface-and-give-anonymous-callers-their-own-bucket)**,
in two halves a year and a release apart. [L10](#l10--the-low-findings-in-one-place) metered the
two HTTP search handlers; M3 metered the five write routes and gave an unidentified caller a
bucket of its own. Both recommendations in the original finding were taken rather than the
deployment-docs alternative it offered.

The finding as filed (0.3.3 stability audit, finding 09): the token-bucket limiter was wired into
exactly one place — `tool_limiter.check(...)` in `mcp/governance.rs` (23) — so the REST read and
write API was governed only by `max_concurrent_requests` and body size, which bound instantaneous
concurrency and place no ceiling on sustained request rate from one caller. Separately, within MCP
every unidentified caller shared a single bucket (`ratelimit.rs` 141, 182): one anonymous client
could spend that budget and deny it to the rest, with no per-address dimension to fall back on.

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
[Phase 16 — what landed](#phase-16--boot--oom-recovery-at-scale) for the analysis,
the outcome table and Stage 7's write-path work. What remains:

### E1 — Stage 4.2 — a max-WAL-size commit trigger

📋 **Planned.** So a bursty writer cannot accumulate a large tail before the operation-count
threshold fires. **This is now the only thing that bounds worst-case replay length**, which is
what makes it the first of these four.

Verified 2026-08-26: `should_commit_writer` in `crates/storage/src/lib.rs` still decides purely
on `operations_since_commit` against a memory-budget-scaled threshold. There is no byte-size
term and no wall-clock term.

### E2 — Stage 3's deeper warming options

📋 **Planned.** Hot-set and field-scoped warming, if the 60-second warmup budget proves too
blunt at 30 TB. The budget shipped; these are the options it defers, not a replacement for it.

### E3 — Measure recovery on the reporting node

📋 **Planned.** The change is verified by the storage suite and by construction; the
30 TB / 16-shard figures in the archived success metrics are still the target, not a result.
Nothing else in this phase should be called finished before this runs.

### E4 — Two compatibility paths with no end-to-end test

📋 **Planned.** Both from Stage 7, and both unbuildable in-repo as things stand:

- **An index built by an older build, opened by this one.** The compatibility path is real and
  exercised in the decoder unit tests, but `create_schema_from_definition` no longer declares
  `_seq`, so there is no longer any way to *create* a legacy-shaped index to open. Verifying it
  needs a checked-in fixture index or a build-flag seam.
- **A legacy WAL tail replaying end to end**, for the same reason. `decode_wal_entry` is unit
  tested against both formats, and the replay body above it is format-agnostic by construction.

### E5 — A cap on open index writers

✅ **Done 2026-09-20** by [M1](#m1--bound-resident-memory-against-index-count), which caps
the *index* rather than the writer this entry names — see **M0-a** for why one of ten maps was
the wrong unit. The original finding (07, 0.3.3 stability audit): `writers` is a `DashMap` that grows with the
number of distinct indexes written to (`storage/src/lib.rs` 3410), and every entry is a live
Tantivy `IndexWriter` holding its own indexing arena — `indexer_memory_budget`, default 64 MiB
(1432), scaled up further by the optimal-budget calculation. Nothing evicts by count or by total
budget; the only eviction is the admin endpoint. Resident memory is therefore proportional to how
many index *names* a workload touches, not to how much data it holds — and since writes create
indexes implicitly, that count is caller-controlled, so a tenant-per-index or date-partitioned
pattern reaches an uncomfortable footprint quickly. Bound the set: an LRU or total-budget cap that
commits and drops the coldest writer when a new one would exceed it — the same drop-and-rebuild an
eviction already does.

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

**This is [CH12](#ch12--write-path-serialization-and-round-trip-waste)'s fourth bullet, measured.**
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
[F2](#f2--an-open-loop-load-generator) landed on 2026-09-15 and can produce one; this sweep has
not been re-run against it, and the numbers above stay as they were taken.

**Re-measured 2026-09-06 on the unwind build, and the figures above stand.** Every number in this
sweep was taken on a `panic = "abort"` binary, three days before `2c53e97` set the release profile
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

**Bulk is only ~30% fsync. The rest is [CH12](#ch12--write-path-serialization-and-round-trip-waste).**

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

The reasoning is in [Mixed read/write load, measured](#mixed-readwrite-load-measured), which
also records why the linger cannot be rebuilt from that reasoning alone.

### F2 — An open-loop load generator

✅ **Done** 2026-09-15. `cameodb-bench --rate` offers requests on a schedule that does not
wait for answers. The closed-loop path is untouched and still the default, because every figure
in this section was taken with it and they stay comparable only if a bare invocation keeps
meaning what it meant.

**What it produces that the closed-loop mode could not.**

- **An arrival process.** Exponential inter-arrival gaps (`--arrival poisson`, the default) or
  exact spacing (`uniform`), seeded so two runs offer the same load. A saturated node now shows
  as a growing queue rather than as a shrinking offered rate — closed-loop, a node that slows
  down simply receives less work, which is why the overload never appeared.
- **Latency from the intended send time.** Reported as `total`, beside `service` (sent →
  answered) and the `harness lag` between them. Where they diverge the difference is queueing,
  and reporting only the first is the coordinated omission the mode exists to remove.
- **Outcomes told apart.** `503` (the concurrency guard refusing admission), `408` (the request
  timeout abandoning a client), `429`, any other status, and transport failures are counted
  separately. They were one `errors` number, which is exactly the number an overload run needs
  broken down. The status comes from `client::HttpFailure` rather than from parsing the message.
- **A verdict per step**, which refuses to present a number as an SLA point when it is not one.
  The harness is judged first and hardest: any arrival dropped at the `--max-in-flight` ceiling,
  or a p99 lag over 5ms, reports `INVALID as a statement about the node`. A generator that
  cannot keep its own schedule is measuring itself, and it shares a machine with the node.
- **A per-second series**, so a rate that holds and a rate that decays through the run are
  distinguishable. They have the same average.
- **Ramps.** `--rate-steps 1000,2000,4000` finds the knee in one pass.

**How it hits sub-millisecond arrival times.** Tokio's timer ticks at about a millisecond and
gaps at interesting rates are tens of microseconds, so the loop sleeps only while the next
arrival is far off and spins the last 1.5ms. That reserves a core, which is why harness lag is
reported beside every latency and why the tool says so when the node is on the same machine.

**The arrival schedulers get their own threads, and that was a finding rather than a detail.**
Sharing a runtime with the response handlers means the busier the node's answers make this
process, the later the generator dispatches — so the offered rate quietly falls exactly when the
node is under most load. That is the closed-loop coupling this mode exists to remove,
reintroduced through the scheduler. Measured, on a loopback node at 3,000/s: p99 lag **49.45ms**
sharing the runtime against **2.33ms** with one thread per active scheduler and requests
dispatched onto the main runtime through its handle. The first run was reported `INVALID`; the
second produced a capacity answer. The guard caught its own harness, which is the argument for
having it.

**Smoke-checked** on loopback, release harness against a debug node (so the node's absolute
numbers mean nothing — the harness behaviour is the point):

| offered | arrivals | ok/s | shed (503) | in flight peak | lag p50 / p99 | verdict |
|---|---|---|---|---|---|---|
| 500/s | 3,058 | 510 | 0 | 18 | 30µs / 1.70ms | sustained |
| 3,000/s | 17,968 | 1,881 | 6,681 | 241 | 77µs / 2.33ms | node declined 37.2% |

Both runs offered exactly what the rate asked for and the 3,000/s run reproduced its arrival
count across rebuilds, which is what the seed is for. The in-flight peak of 241 against the
node's admission limit of 128 is the shape an open-loop run is supposed to show: requests
outstanding at the client while the node refuses them, which no closed-loop configuration can
produce.

**What it unblocks, none of it measured yet:**

- [F1](#f1--the-cost-of-a-durable-commit-under-read-load), and the bounded linger, which was
  rejected on arithmetic computed from a closed-loop rate — 0.046 writes arriving at a shard per
  200µs window at c16. Poisson arrivals are the condition it needed and never had.
- ✅ [F7](#f7--the-request-timeout-sheds-the-client-not-the-work), **done the same day**. The
  `408` count broken out from the other outcomes, the per-second series and the co-located-harness
  verdict were between them exactly the instrument it needed: the failure is confirmed, it is
  larger than the audit described, and it surfaced [OB13](#ob13--the-health-endpoint-fails-under-overload-but-not-for-the-reason-it-looked-like)
  alongside. This is the item that paid for F2.
- The audit trail's **read path**, and the trail **under a queue-overrunning workload**, which
  is the case the `gap` record exists for.
- [A3](#a3--protocol-compliance-tests-and-agent-query-benchmarks)'s agent-query latency figures.

**Every number elsewhere in this document is still closed-loop** and is labelled so where it is
tabulated. Nothing here re-measures them.

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
[OB3](#ob3--a-single-write-or-delete-can-land-on-the-wrong-shard) bounds it. For a **keyless**
one it is not, and keyless is the case this item exists for. A scatter-gather that believes the
cluster is one node returns only local results — no error, no partial flag, just a short answer —
for the window between a peer becoming live and the ring arriving. That is the failure shape
[OB8](#ob8--a-paged-search-loses-its-page-on-the-streaming-fan-out) and
[OB9](#ob9--a-bulk-delete-drops-a-peers-per-id-errors) were opened for.

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
| bulk write / delete | 0 route asks (but see [F4](#f4--the-bulk-paths-asked-the-coordinator-before-they-knew-they-needed-to)) | 0 |

A search has no routing key to resolve, so *every* search took the ask — and the answer could
only ever be `Local`, because `decide_route` has one node in `expected_nodes`. `NodeConfig`
already grew `clustered` for 12c3ad9's schema gate, and it answers the same question statically:
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

### F4 — The bulk paths asked the coordinator before they knew they needed to

✅ **Done** 2026-09-02, found auditing the single-node path after the cluster work.

`orch_bulk_write` fetched `GetShardAssignments` **and** `GetKnownPeers` at the top of every bulk
write, and `orch_bulk_delete` fetched `GetShardAssignments` at the top of every bulk delete. All
three maps are read only in the branch that forwards to a peer: the local/remote split uses
`self.shards`, which the node already holds. So a single-node deployment paid two coordinator
mailbox round trips per bulk write and one per bulk delete for maps whose only possible use was
to discover that everything was local — and a clustered node paid them on every batch that
happened to land locally, which a routing hint makes common.

They are fetched lazily now, once, only if some document actually routed off this node. The
delete path already asked for its *peers* lazily (`if !remote_by_node.is_empty()`), which is what
made the pattern obvious: the same function was doing it right for one map and wrong for the
other.

**Not a measurable win, and worth saying so.** A bulk write is commit-bound at ~80 ops/s on this
hardware whatever the concurrency, so two mailbox hops vanish into the fsync: p50 8.03ms before
and 8.02ms after, p99 within run-to-run spread. The reason to do it anyway is that the work was
unconditional and unnecessary, and the `ClusterCoordinator` is one actor that also serves gossip,
shard registration and snapshot persistence — nothing that does not need it should be queuing
behind it, whatever this hardware happens to show.

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

**Measured**, with [F2](#f2--an-open-loop-load-generator)'s open-loop generator, on two machines.
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

**[OB13](#ob13--the-health-endpoint-fails-under-overload-but-not-for-the-reason-it-looked-like)
re-verified**: 14 of 14 probes `200` under 3,000/s, p50 52.8ms. `/_admin/*` is exempt from the
backlog gate for the same reason health is exempt from the semaphore — a node that stops
answering the endpoint that explains why it is refusing cannot be diagnosed at the one moment it
matters.

Not measured: whether the earlier refusal pays for itself on a large-body write workload, which
is the case it was built for and the one arm here does not cover. *Measured 2026-09-16 and the
answer is that it never gets the chance — the bulk lane reaches neither gate. See
[F8](#f8--the-overload-gates-do-not-cover-the-bulk-write-path).*

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
built from `9a56de5` — the commit before fix 1, and the one that made `request_timeout_secs`
honoured at all — run in the same session on the same M1, against the same seeded index and the
same configuration as the current binary. 200,000 documents, 4 shards, `search_threads = 2`,
`max_concurrent_requests = 3000`, `request_timeout_secs = 1`, 20s arms, harness lag p99 at or
below 2.2ms on every arm quoted.

| offered | before (`9a56de5`) | after (`bc1a50e`) |
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

**[OB13](#ob13--the-health-endpoint-fails-under-overload-but-not-for-the-reason-it-looked-like)
is closed by this on the search path, as predicted and now measured** — and reopens on the write
path, where health queues behind a bulk write rather than behind the backlog
([F8](#f8--the-overload-gates-do-not-cover-the-bulk-write-path)). Under 3,000/s against the same
node that previously answered health in 1001.8ms with every probe a 408, fourteen consecutive probes
returned **200 in ~120ms**. One fix, both entries. The 2026-09-16 run has it on both binaries at
once: the pre-fix node answered its third probe and everything after it with a 408 at 1,001ms,
the current one 200 in ~130ms on sixteen of sixteen.

**Re-measured on the M5 Pro, 2026-09-16.** The before-and-after above is an M1 result; the M5 had
only a pre-fix column, recorded the day before. This closes that gap on the second machine, and
one of its numbers contradicts the M1 reading. Release build of `beab920`, 15 cores, 24 GB,
macOS 26.6.2, same protocol throughout: 200,000 documents, 4 shards, `search_threads = 2`,
`max_concurrent_requests = 3000`, `request_timeout_secs = 1`, 20s arms after a 5s warmup, Poisson
arrivals, seed 1. Harness lag p99 stayed at or below 1.12ms on every arm, no arm dropped an
arrival and no verdict came back INVALID, so these are node measurements and not the generator.
Seeded once at the default timeout with the node restarted on the one-second budget, which
`check-config` confirmed as `timeout 1s (set)` while raising the `overload` warning on it.

| offered | before (pre-fix, 2026-09-15) | after (`beab920`) |
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
[F8](#f8--the-overload-gates-do-not-cover-the-bulk-write-path).

### F8 — The overload gates do not cover the bulk write path

✅ **Done — all three items closed, item 3 on 2026-09-17.** Measured before it was
planned, 2026-09-16, on the M5 Pro against `beab920` —
the binary that closed [F7](#f7--the-request-timeout-sheds-the-client-not-the-work), with all
three of its fixes in.

⚠️ **Every table below predates the fix for item 3 and describes a design that no longer
exists.** `0836df2` (2026-09-17) moved `BulkWrite` and `BulkDelete` off the actor mailbox and
onto the worker pool. The diagnosis below — a lane the gates could not see — is what motivated
that move and is kept for it, but the numbers record the node as it was on 2026-09-16 and are
not a description of `main`. Re-measuring is [M6](#m6--close-and-re-measure-the-bulk-lane).

Every F7 arm was a search. Its own closing note said the write path was not covered and has its
own queue. Measured now, the answer is worse than "not covered": **F7's mechanism is absent on
the bulk lane, and F7's original failure is intact there** — goodput does not degrade under bulk
overload, it goes to zero, and no gate refuses anything.

Same node and configuration as the M5 F7 run: 4 shards, `search_threads = 2`,
`max_concurrent_requests = 3000`, `request_timeout_secs = 1`, `--mode bulk --batch-size 500`,
20s arms. Capacity probed first rather than assumed — 56 requests/s closed-loop at concurrency 4
(28,000 docs/s, p50 54ms) — so the offered rates are multiples of a measured number.

| offered | ok/s | 408 | 503 | documents written |
|---|---|---|---|---|
| 30/s (under capacity) | 29 ok/s, `sustained` | 0 | 0 | 292,000 |
| 120/s (≥2×) | **0** | 2,370 (100%) | **0** | **0** |
| 300/s (≥5×) | **0** | 6,035 (100%) | **0** | **0** |

Zero for the whole 20s of both overload arms, at a steady `408 timeout/s` matching the offered
rate — F7's signature exactly. For contrast, on the same node and in the same session a *search*
at 2× overload served 923 ok/s with no 408 at all.

**No gate refused anything, and the counters are unambiguous.** Across both overload arms —
8,405 requests, every one abandoned at its deadline:

```
refused_at_admission 0    abandoned 0    actor_mailbox_fallbacks 0    round_robin_sends 0
```

`round_robin_sends` at 0 is the tell: no bulk work reached the worker pool at all. Two
independent reasons, both read from the code before the run and confirmed by it (line numbers
read 2026-09-16, and they drift — the shapes are the durable part):

- **The dequeue check never runs.** `handle_client_op`'s `is_worker_eligible`
  (`node_orchestrator.rs` ~6241) lists `Write`, `Delete`, `Search`, `Stream`. `BulkWrite` is
  not among them, so it goes to the orchestrator actor mailbox and never meets fix 1's check at
  `rx.recv()`. Fix 2 closed the *overflow* route into that mailbox from a full worker queue; the
  bulk path's *primary* route into it was never gated.
- **The door sees an idle node.** `QueueLoad::would_refuse` returns `None` early at
  `depth < self.width` (~3262), and `depth()` reads the pool-wide `outstanding` counter, which is
  incremented only where a job is sent to a worker. A bulk write never increments it, so however
  deep the mailbox gets, the door reads a depth of 0 and admits.

A third detail compounds it rather than causing it: `OpClass::of` maps anything outside
search/single-write to `Any` (~176), and `record_service` ignores `Any` (~3108) — so bulk service
time never enters any EWMA, and the blended estimate the door judges on is built from searches
and single writes only. *Not* a zero-reserve bug: `service_estimate_for` falls back to the blend
when a class has no samples, which is deliberate and documented at the function.

**[OB13](#ob13--the-health-endpoint-fails-under-overload-but-not-for-the-reason-it-looked-like)
reopens here, on a different mechanism.** Under bulk overload `/_cluster/health` returned **408 at
1,001ms on twelve of twelve probes**, and five of five on a repeat — the same signature the
pre-fix node produced under search overload. `/_admin/workers` was unaffected throughout,
answering **200 in 0.7ms**, and health returns to 0.7ms the moment load stops.

That split is the diagnosis. `/_admin/workers` reads dispatch atomics directly; health is
actor-served and queues behind the bulk write holding the mailbox — which is
[CH12](#ch12--write-path-serialization-and-round-trip-waste)'s third bullet ("a large `BulkWrite`
serializes on the orchestrator actor mailbox for its full duration, blocking other actor-served
operations"), demonstrated rather than reasoned about. F7 exempted health from the *semaphore and
the backlog gate*, which is why the search fix held; nothing exempts it from waiting on an actor.

So a node under bulk-ingest overload writes nothing, makes every client wait a full second to be
told 408, and fails the probe a load balancer evicts on — while `/_admin/workers` reports a node
in perfect health.

***The endpoint's own guards could not fire, and that is the part worth keeping.*** "Actor-served"
is true but too kind: the expanded body already wrapped its actor calls in
`HEALTH_ACTOR_TIMEOUT` precisely so "a slow node [does not] fail its own health probe". Three
things stopped that working, and each alone was enough:

- **The wait was per call, and the calls are sequential.** Four of them — `GetStatus`,
  `shard_count`, `GetIdentity`, `ListIndexes` — at a fixed 5s each is a 20s worst case for a body
  that is supposed to be the fast answer.
- **The guard was longer than the budget it guarded.** 5s against a node running
  `request_timeout_secs = 1`: `TimeoutLayer` abandons the request as a 408 a full second before
  any fallback can run. A guard above the request timeout is not a guard, and nothing related the
  two numbers.
- **`GetStatus` had no guard at all.**

And the fast path that exists for exactly this — the anonymous branch, whose comment says "a
health flood must not become mailbox pressure" — is **unreachable on an unauthenticated node**:
`is_identified()` is `!matches!(self, Authz::Anonymous)`, and `[security] enabled = false` yields
`Authz::Disabled`, which counts as identified. Every probe on a local or dev node takes the
actor path.

✅ **Fixed 2026-09-16**, and measured on the arm that found it. The four actor calls now share one
deadline rather than one each, so the total is bounded; the deadline is derived from the node's
own resolved request timeout (half of it, clamped to 50ms–5s) so it always fires before
`TimeoutLayer` does; `GetStatus` is inside it; and a `degraded` field names whichever lookups did
not answer, because `active_shards: 0` from a busy node is otherwise indistinguishable from a node
that has no shards. Under the same 120/s bulk arm: **200 on twelve of twelve probes at ~505ms**,
against 408 at 1,001ms before, carrying `degraded: ["active_shards", "node_id", "total_indexes"]`
— and *not* `cluster_status`, which localises the blocking actor to the orchestrator rather than
the coordinator. An idle node's body is byte-for-byte what it was.

`Authz::Disabled` was deliberately **not** rerouted to the anonymous fast path. It would have
removed the timeout by removing the body — that path returns `status` alone, dropping
`queue_depth`, `predicted_wait_ms` and `read_pool_abandoned` from precisely the unauthenticated
nodes F7 added them to. Bounding the waits keeps the full body and degrades it honestly instead.

**The instrument is restored, but it still cannot see this lane.** Health now answers under bulk
overload — reporting `green` with `queue_depth: 0`, because bulk work never enters the counter
`queue_depth` reads. That is the same root cause as the gap above, and it is fixed by item 1, not
by anything further here.

**What the fix has to do**, in the order the evidence supports:

1. ◐ **Make the bulk lane visible to the gates.** Partly done 2026-09-16. The mailbox lane got
   its own `QueueLoad` — width 1, because the actor serialises what it takes, and its own
   counters, because a bulk write's service time is three orders of magnitude above a point
   search's and one counter would mis-predict both. `ask_orchestrator` refuses against it before
   queueing. Routing `BulkWrite` through the worker pool was the alternative and was not taken:
   [OB12](#ob12--the-schema-gate-deadlocked-a-fan-out-against-itself)'s invariants are written in
   terms of what holds that mailbox, and this lane can be gated without disturbing them.

   | offered | before | after |
   |---|---|---|
   | 30/s (under capacity) | 29 ok/s, sustained | 29 ok/s, sustained — untouched |
   | 120/s | **0 ok/s**, 0 documents, 100% × 408 | **6 ok/s**, 55,500 documents, 1,333 × 503, 926 × 408 |
   | 300/s | **0 ok/s**, 0 documents, 100% × 408 | 0 ok/s, 3,000 documents, 4,848 × 503, 1,181 × 408 |

   *Those two rows were taken on an index that had grown to 815 MB across the session's arms, so
   they are the right shape on the wrong baseline — the gate's own before-and-after below is
   re-measured from a wiped volume.*

   *Better, and not yet the flat goodput the search lane got.* At 120/s it opens at 77 ok/s and
   falls to zero within three seconds. Measured mid-arm: `mailbox_depth` 51 against
   `mailbox_predicted_wait_ms` 684 — the gate admits a queue that consumes ~70% of the budget,
   and this lane's service distribution has a p90 around **5×** its p50 (32ms against 152ms at
   30/s). Twice the mean does not reserve against that spread, so the back of an admitted queue
   times out. The worker pool never had this problem because its service time is milliseconds
   against a one-second budget.

   ✅ **Closed by predicting against a measured spread**, 2026-09-16 — which is F7's own open
   question (*a percentile of the service distribution rather than twice its mean*) answered
   with a run. A decaying log-bucketed histogram now sits beside the EWMAs: recording is one
   `fetch_add` against the EWMA's `fetch_update` CAS loop, two generations rotate every 2s so it
   tracks a node whose load changed, and the figures are computed once per rotation and cached
   in atomics — so the admission check stays the single load F7 built it to cost. The first cut
   multiplied out the p90 and the final form does not; see the sweep below.

   **Measured same-session, each arm from a wiped volume re-seeded to 200,000 documents, so both
   columns start from an identical index:**

   | offered | before | after |
   |---|---|---|
   | 30/s (under capacity) | 29 ok/s, 292,000 docs, 0 shed, green | 29 ok/s, 292,000 docs, 0 shed, green |
   | 120/s | **6 ok/s**, 58,000 docs, 1,170 × 503, **1,084 × 408** | **57 ok/s**, 573,000 docs, 1,224 × 503, **0 × 408** |
   | 300/s | **1 ok/s**, 8,000 docs, 4,527 × 503, **1,492 × 408** | **59 ok/s**, 585,000 docs, 4,827 × 503, 38 × 408 |
   | search, 3,000/s | 916 ok/s | 904 ok/s — inside the noise floor |
   | search, 3,000/s p50 | 893ms | **820ms**, once the lane went tail-aware too |

   Goodput is flat at ~57–59 ok/s from 120/s through 300/s where it used to collapse to zero in
   three seconds, nearly ten times the documents land, and the timeouts are gone: the refusals
   are 503s a client can act on. The under-capacity arm is untouched and still reports green.
   The 1.3% between the search columns is well inside the ~8% spread this document already
   records for back-to-back runs; what it shows is that the extra `fetch_add` per sample costs
   nothing measurable. (The search lane was held back from tail-aware admission at this point,
   and stopped being so later the same day — below.)

   ***A percentile is not automatically the tail, and that is the trap in the idea.*** With 90%
   of samples at 8ms and 10% at 200ms, the p90 sits exactly on the boundary and reports **8ms** —
   the body, not the tail. Which quantile separates them depends on how heavy the tail is, so
   "reserve a percentile" is a choice that has to be made against a measured distribution rather
   than assumed. A test pins it.

   ✅ **Both of those were closed the same day**, and the second one contradicted the reasoning
   that opened it.

   *The reserve is now `d·µ + k·σ√d`.* The wait ahead of an arrival is a **sum** of service
   times, so its mean scales with the depth and its spread only with the root of it — a
   per-request quantile multiplied by depth grows the margin linearly and over-predicts a deep
   queue. The histogram computes mean and standard deviation of the closed generation (in `f64`,
   once per rotation, because a bucket edge squared overflows `u64`), and `k` is a confidence
   level rather than a fudge factor. Swept at 300/s, three repeats' worth of arms from a wiped
   volume each:

   | form | 120/s | 300/s |
   |---|---|---|
   | p90 × depth | 57 ok/s, 573,000 docs, **0 × 408** | 59 ok/s, 585,000 docs, 38 × 408 |
   | `d·µ + 2σ√d` | — | 51 ok/s, 508,000 docs, 85 × 408 |
   | **`d·µ + 3σ√d`** | 56 ok/s, 564,000 docs, **0 × 408** | 57 ok/s, 572,500 docs, **6 × 408** |

   Throughput is flat across all three — every pair is inside the spread this document records
   for back-to-back runs — so the choice is made on the tail, where `k = 3` leaves **6 timeouts
   against 38**. `k = 2` is worse than both on both counts, which is the reading worth keeping:
   the theoretically tidy value is not the measured one, and under saturation service times are
   correlated rather than independent, so the √d model wants a wider `k` than its own statistics
   suggest.

   *The search lane is tail-aware too, and the reason to do it was not the one expected.* It was
   held back because F7's arms were measured against a mean and changing the basis without a run
   would invalidate them. The run says goodput is untouched and **latency is not**: at 3,000/s
   offered, 899 ok/s at p50 893ms / p99 904ms becomes 894 ok/s at p50 **820ms** / p99 **828ms** —
   0.6% on throughput, which is noise, against ~8% on the number an SLA is written against.
   Predicting conservatively holds a shallower queue, and a shallower queue is a faster one at
   the same goodput. The under-capacity arm is unchanged: 299 ok/s, nothing shed, p99 5.71ms.

   It also closed a reporting gap: **the node published no service percentiles at all**, so an
   operator could see how much work was queued but not how long the node's own work was taking,
   and could not tell a node that got slower from one that got busier.

   ***One bug worth keeping, because it was F7's failure rebuilt inside its own fix.*** The first
   cut decremented the lane counter after the `await`. `TimeoutLayer` drops the request future on
   timeout — under overload, most of them — so the increment survived and the decrement never
   ran. Measured: `mailbox_depth` **63 on an idle node**, every subsequent request refused, for
   good. An RAII guard now holds the slot, so a cancelled ask gives it back; a test drops one
   without awaiting anything. The service estimate moved too: sampled at the caller it could only
   be taken from uncontended asks, which are the fastest, and it settled near the idle p50. It is
   measured inside the actor now, where start-to-finish is service with no queue in it.
2. ✅ **Bound what health waits on.** Done 2026-09-16, above. The aim was "take health off the
   actor"; what it needed was for the guards it already had to be capable of firing — one shared
   deadline, sized from the request timeout, and a body that says which fields are fallbacks.
   Worth having done independently of 1: it is the difference between a degraded node and an
   undiagnosable one.
3. ✅ **Feed bulk into the service estimate**, so the blended figure the door uses reflects the
   workload with by far the largest per-request cost. **Done 2026-09-17 by `0836df2`** — and by
   the route item 1 had considered and declined, rather than the one this item proposed.
   `BulkWrite` and `BulkDelete` became worker-eligible in `handle_client_op`, so a fan-out is
   served off the pool instead of holding the mailbox for its whole duration. `OpClass::of` maps
   both to `OpClass::Bulk`, and `record_service` folds every sample into the blend
   `service_ewma_us` as well as into the class's own EWMA — so the door's pre-body estimate
   sees bulk cost, which is what this item asked for.

   **Item 1's mailbox gate is still load-bearing, for a smaller lane.** A bulk write that needs
   a schema written hands itself back as `UseActor` and reaches the actor through
   `ask_orchestrator`, which still refuses against the mailbox `QueueLoad` before queueing, and
   that lane folds its own samples at the actor (`mailbox_lane.record_service`). Two lanes, two
   blends, each predicting against what it actually serves — which is what item 1 built the
   second set of counters for.

   **The fix is in and its effect is unmeasured.** That is
   [M6](#m6--close-and-re-measure-the-bulk-lane), and the caveat at the head of this entry.

**Caveats, so these are not over-read.** Single 20s runs, harness co-located. The 30/s arm wrote
292,000 documents, so capacity during the later arms was below the probed 56/s — "≥2×" is a
floor and the true multiple is higher. Neither affects the finding, which is qualitative: zero
goodput and zero refusals. Not measured: the single-write path (`Write` *is* worker-eligible, so it
should be covered, and that is an assumption until an arm says so), and whether a retrying client
deepens any of this.

---

## G. Code health, reviewed at 0.3.1

Reviewed 2026-08-16, after the paging and MCP work landed; re-checked 2026-08-26, when every
item was still present. Nothing here changes behaviour; each item is a place the code now says
one thing twice, or where the next feature will cost more than it should. Ordered by what each
buys, not by effort.

CH8–CH12 were added by the 2026-09-01 write/read/delete review and are the same kind of item:
write-path allocations and duplication that cost latency but are not defects.

### CH1 — One scatter-gather, written twice

✅ `engine_search` and `orch_search` were ~150-line near-duplicates: the same shard fan-out,
gather loop, sort-key stamping, merge, window application, projection and response assembly.
The paging change had to be made in both, and was — which is the warning, not the reassurance:
the next change to one of them would have been forgotten in the other. The gather now lives
once in `ScatterCtx::gather`, the borrowed view both lanes build — the actor from its own shard
map, the engine from its `ArcSwap` snapshot — same shape as `BulkCtx`. What stayed per-lane is
what the lanes genuinely own: the empty-shards early return and `load_schema`.

**2026-08-26:** the 0.3.2 sort work is the second change that had to be made twice.

**2026-09-01:** the OB3 forwarding fix is the third. `engine_write` and `orch_write` both had to
learn to hand a remote shard on, and `engine_delete`/`orch_delete` with them — which is CH10 and
CH1 charging the same toll on the write side.

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

**2026-09-17:** the server half is done — [L11](#l11--node_orchestratorrs-is-13669-lines-and-holds-four-actors)
landed the split, and the merge primitives, sort keys and validation live in
`node/search.rs` as this item always wanted. `storage/src/lib.rs` keeps the same
disease; its cure is [L12](#l12--storagesrclibrs-is-9961-lines-of-which-one-impl-block-is-4460).

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

### CH4 — The window bound is spelled twice

✅ **Done 2026-10-05.** `SearchWindow::checked` (server) and `check_limit` +
`check_offset_window` (the `mcp` crate) enforced the same rule with independently maintained
arithmetic and error text, and the refusal text had already drifted between them. Both checks
remain — the schema is the `mcp` crate's promise, and a promise nothing enforces describes
nothing — but the arithmetic and its test vectors now live once in
`cameodb_mcp::checked_search_window` (`tools/limits.rs`), exported beside the
`DEFAULT_MAX_*` bounds so the advertised ceiling and the enforced one cannot drift. The
dispatcher resolves the window once per call rather than running a limit check and a window
check in sequence; the refusal text unified on the server's wording.

### CH5 — Sort type conversion, four times inline

✅ **Done 2026-10-05.** `crates/server/src/mcp/search.rs` converted `cameodb_mcp::SortSpec` ↔
`storage::SortSpec` with the same written-out match in four places — including a
storage→MCP→storage round trip that existed only to merge an argument sort with an inline
one. The `From` impls this item pictured cannot be written: both types are foreign to the
only crate that sees both, and the `mcp` crate deliberately holds no dependency on
`storage`. One `to_storage_sort` beside the call sites carries the translation instead, and
the round trip collapsed into `sort.map(to_storage_sort).or(parsed_sort)`.

### CH6 — The federated merge clones every hit

✅ **Done 2026-09-19.** `search_across_indexes` clones each hit out of the response it already owns in order to stamp
`_index_source` on the copy. Taking the array with `as_array_mut` + `std::mem::take` stamps in
place. Bounded by page size rather than corpus size, so this is the hot line of the federated
path being untidy rather than slow — worth doing when the function is next open.

**2026-09-19, re-read by [M0](#m0--the-architecture-review-and-the-order-of-work) (M0-e):**
"untidy rather than slow" understates it on the surface that matters. On the MCP path the hits
*are* the documents an agent asked for, so the clone doubles peak memory of every federated
response rather than costing a pointer copy. Carried in M0's step 7, and **done the same day**:
`get_mut("hits")`, `std::mem::take`, stamp in place. The response is owned by the merge loop and
dropped at the end of the iteration, so the hits move out of it for a pointer swap.

### CH7 — The string-fast collector repeats the macro's body

📋 `collect_sorted!` in `storage/src/lib.rs` covers the u64/i64/f64/date branches; the
string-fast branch writes the same `MultiCollector` block out by hand because its key type is
`String` rather than a copyable numeric. Fold it in by parameterizing the collector expression,
so the next change to how a sorted search counts its total touches one place.

### CH8 — The single-write path clones the whole schema and document

✅ **Done** 2026-09-01. `apply_write` now clones the schema only when the document carries a
field the schema has not seen (the `has_new_field` guard), and moves the body into shadow
filtering instead of cloning it — the two hot-path clones removed as part of the OB4 reorder.

### CH9 — Bulk validation clones the batch, and Tantivy docs are built inside the transaction

✅ **Done** 2026-09-07. The `apply_write` half landed 2026-09-01 with the OB4 reorder; the two
remaining pieces are now done too.

- **`parallel_validate_schema` no longer clones the batch.** It took `&[DocPayload]` and called
  `docs.to_vec()` to get the `'static` that `spawn_blocking` needs — a second copy of the whole
  request body, allocated and dropped per bulk write, to run read-only checks over it. The batch
  is taken by value and handed back beside the verdicts, which costs a `Vec` of pointers and
  copies nothing. `staged_schema_validation` threads the ownership through. The schema clone
  stays and should: it is one field map, bounded by declared fields rather than by batch size.
  `block_in_place` would have removed the clone *and* a thread hop, and was rejected — every
  `#[tokio::test]` in the crate is current-thread flavoured, so it would panic for the next test
  that bulk-writes past the inline threshold.
- **`apply_batch` builds its Tantivy documents before it opens the transaction.** Shadow
  filtering, the stored-body serialisation and the `add_json_value_to_doc` pass all read the
  batch and the schema and touch no table, so none of them needed to hold the redb write lock.
  The transaction is now the two `insert`s and the bookkeeping that depends on what they
  displaced. OB5's semantics are untouched: `existed_before` still comes from what redb reported
  inside the transaction, and `record_final` still keeps first-occurrence order.

**Neither is a measurable throughput win, and that was predicted rather than discovered.** Three
binaries — neither half, the first, both — built from the same tree and run interleaved, 3
repeats of a 500-document bulk load at each durability setting:

| | both halves | first half | neither |
|---|---|---|---|
| `wal_sync = true` | 12,147 docs/s | 12,134 | 12,255 |
| `wal_sync = false` | 15,304 | 15,470 | 15,162 |

Everything inside 1–2%, with overlapping ranges. The clone is why: measured directly, copying a
400-document, 1.77 MB batch costs **133 µs** against ~47 ms of batch service time — 0.3%, under
the noise floor of any harness in this document.

**Why the second half measures as nothing here, which is not the same as being nothing.** The
bench settles the schema during seeding and then writes steady-state, so nothing else opens a
write transaction on the store while it runs. The contention that does exist is narrower than the
whole write path and absent from this workload: `persist_schema_to_stores` writes each shard's
schema from `spawn_blocking` (`node_orchestrator.rs` 7767), off the writer thread, so a schema
write can block behind an open batch transaction — during evolution and initial creation, which
is exactly what a steady-state bench does not exercise.

So the reasons to have done it are the ones [F4](#f4--the-bulk-paths-asked-the-coordinator-before-they-knew-they-needed-to)
gives: the work was unconditional and unnecessary. What is left after the null result is real
enough to name — a second copy of the request body no longer held live across validation, the
filtered body dropped once its bytes and its document exist rather than surviving the
transaction, and a refused value that fails before any table is opened instead of by dropping a
transaction that had already staged rows.

That last one is a behaviour change in *when*, not in *what*, and nothing pinned it:
`batch_refusal_test.rs` now does, asserting that one bad value writes none of its batch and
leaves the index usable. It passes against the pre-change code as well, which is what makes it
evidence that the restructure preserved the guarantee rather than merely evidence that the new
code agrees with itself.

### CH10 — `engine_write` and `orch_write` are near-duplicates

✅ **Done** 2026-10-12, with `engine_delete`/`orch_delete` in the same sweep — the OB3 fix had
charged this toll on both pairs. The shared body is `WriteCtx`, the borrowed view the pattern
established: the actor builds it from its own fields, a worker from the engine's `ArcSwap`
snapshots. `gate` owns validation, the stable-schema cache populate, effective-key derivation
and ring routing; `dispatch`/`dispatch_delete` own the shard lookup, the request build and the
response shape. What stays per-lane is the divergence that defines each: a worker answers
`NeedsActor` where the actor calls `forward_op_to_owner`, and only the actor runs the
`staged_schema_validation` slow path — which now ends by routing through the same ctx.

The gate needed the schema cache, so its machinery — `get`/`put`/`put_arc`, byte-identical on
both types — became `schema_cache_get`/`schema_cache_put`/`schema_cache_put_arc`, free
functions over the `ArcSwap` map. That is a slice of [L15](#l15--the-schema-cache-machinery-exists-three-times)
paid early: two of its three spellings are one now (the differing `load_schema`s remain its
question).

### CH11 — Routing-key derivation is written four times, with two algorithms

✅ **Done** 2026-09-16. The precedence "document's routing field → caller's routing key → id →
hash of the document" was spelled out in four places, and the two "hash the document" rungs used
*different* hashes: `xxh3_64` of the whole document in `write.rs`, hex of a 64-byte JSON prefix in
`derive_routing_key_from_doc`. One ladder now, in two functions that differ only in what the
caller is holding — `routing_key_for` where the routing field is in hand, and
`routing_key_without_schema` for the rungs below it.

**It unified onto the orchestrator's derivation, not the prettier one, and that choice is the
whole point.** The HTTP layer's hash fed a *hint*, which picks a node before any schema is
resolved; `effective_routing_key` decides which shard a document lands on. A hint may change
freely — a wrong one costs the forwarding hop [OB3](#ob3--a-single-write-or-delete-can-land-on-the-wrong-shard)
bounded — whereas changing the shard rung would move unkeyed documents to different shards across
an upgrade, and leave a second copy behind on the old one. A test pins that the surviving rung is
the orchestrator's.

*The severity as filed was right and the obvious reading of it is not.* Reviewed as a live
routing inconsistency; it is not. Per-document placement was already single-sourced, and
`the_routing_key_comes_from_the_document_before_the_caller` records that the hint-decides-shard
hazard was a real bug and was fixed earlier. What was left was four spellings that had drifted
apart and would have been scattered across modules by [L11](#l11--node_orchestratorrs-is-13669-lines-and-holds-four-actors)'s
split, where a disagreement stops being visible in one grep. Closed before the split for that
reason.

Left alone deliberately: the delete path's `routing_key.or(id)` (`write.rs` ~135). A delete has
no document, so the lower rungs do not exist for it, and the comment there already explains that
the schema-aware refusal downstream is what catches a wrong hint.

### CH12 — Write-path serialization and round-trip waste

✅ **Done** 2026-09-18. The small ones collected, none of which alone justifies an item. The
first two landed ahead of the public 0.3.3; both were smaller than described, and neither needed
the mechanism proposed here:

- ✅ **The bulk response is no longer serialized twice.** Gating the measurement behind
  log-enablement was the proposal, and it would have bought nothing in the default deployment,
  where `info` is on. A bulk response is `items_written` plus one reason per failed item, so its
  size *is* its reasons: summing their lengths costs nothing, needs no gate, and is what the
  large-response warning was reading a whole serialization to learn. The line reports items
  written, error count and reason bytes.
- ✅ **`handle_remote` no longer clones the op per attempt.** "Move the owned op on the final
  attempt" would have left the first attempt cloning defensively, which is the case that matters
  because it is the case that normally succeeds. No clone is needed at all: `try_remote` took the
  op by value and then called `ask(&op)` — the send path serializes from a reference and only ever
  borrowed it, so taking ownership obliged every caller to hand over a copy it could not get back.
  It borrows now. The two fan-outs still clone once per peer, which is real: those futures run
  concurrently and each needs its own.
- ✅ Bulk writes never use the worker pool, so a large `BulkWrite` serializes on the orchestrator
  actor mailbox for its full duration, blocking other actor-served operations. **Filed here as
  efficiency and it was not** — [OB12](#ob12--the-schema-gate-deadlocked-a-fan-out-against-itself)
  is what that blocking cost once something on the write path needed an answer from a peer that
  was itself inside a write.

  **The blocking itself is gone.** `BulkWrite` and `BulkDelete` are worker-eligible now: the
  fan-out — validation, routing, per-shard batches, bounded remote forwarding, per-item
  accounting — is written once against `BulkCtx`, the borrowed view the actor builds from its
  fields and a worker builds from the engine's `ArcSwap` snapshots. What stayed on the mailbox
  is the one thing that needs its serialisation: a batch that has to *write* a schema defers
  back as `UseActor`, because `staged_schema_validation` evolving two concurrent bulks into
  one index is the race the mailbox exists to prevent. Bulk service is estimated on its own
  class, so a fan-out's cost is no longer blended into a single write's reserve.

  **Closed from both sides** 2026-09-16. [F8](#f8--the-overload-gates-do-not-cover-the-bulk-write-path)
  reached it from the overload side: the lane is gated, so a metadata read arriving behind a bulk
  write is refused rather than queued behind it. This closes the other side for the read that
  mattered — `GetIdentity` is answered by the worker engine from an immutable identity and an
  `ArcSwap` shard map, so it never enters the mailbox at all.

  *And health stopped asking twice.* It called `shard_count()` and then `GetIdentity`, two actor
  round-trips out of one shared budget, when `GetIdentity` already reports `total_shards` and
  `GetShardCount`'s handler returned the same `shards.len()`. The first ask could spend the
  budget the second needed. One call now; `RouterActor::shard_count`, the `GetShardCount` message
  and its handler had no other caller and are gone.

  Measured under a bulk ingest at twice capacity on a one-second budget, health's `degraded` list
  went from `["active_shards", "node_id", "total_indexes"]` to **`["total_indexes"]`** — it now
  reports its real identity and shard count while shedding, and two probes in five answered in
  ~2ms rather than spending the whole actor budget.

  `ListIndexes` stayed on the actor by choice — moving it meant extracting the
  ~190-line aggregation, and the two different `load_schema`s it called were
  [L15](#l15--the-schema-cache-machinery-exists-three-times)'s question to settle first.
  **Moved** 2026-09-18. Both `load_schema`s now delegate to `SchemaCache::schema_for`, so
  there is one load-schema semantics to share and nothing to choose between. The
  aggregation is the free function `list_indexes` in `node/orchestrator.rs`, taking the
  shard map, the `SchemaCache` and the identity as borrows — the actor's
  `orch_list_indexes` delegates to it, and the engine calls it on its `ArcSwap` snapshot.
  `ListIndexes` and `ListClusterIndexes` are worker-eligible, so "what indexes exist" —
  and the local half of the cluster listing, which is the same body — no longer queues
  behind a bulk write. `GetShardStats` was always asked on a shard clone rather than the
  shard's mailbox, so the worker consults the same state the actor did.
- ✅ **The writer thread's two groups are merged** 2026-09-16, and the comment that said they
  already were is now true. `write_groups` and `batch_groups` were drained by separate phases,
  so an index that received both kinds in one drain paid two `apply_batch_and_maybe_commit`
  calls — two redb transactions, and with `wal_sync` on two fsyncs — for work one transaction
  covers. Only the indexes that received *both* take the merged path; a drain holding one kind
  runs exactly the code it did before, which keeps the common case untouched. Singles are
  ordered ahead of batches, the order the two phases applied them in, so which write wins a
  duplicated id does not change.

  **It fires, and it does not measure.** Instrumented over a 12s run with single writes and bulk
  writes against one index: 134 merged transactions against 1,077 ordinary coalesced ones — so
  roughly one transaction in nine was saved. Three repeats of a 30s mixed arm at writer
  saturation (1,092 single writes/s and 19 bulk/s, both offered open-loop):

  | repeat | before, mean / p90 | after, mean / p90 |
  |---|---|---|
  | 1 | 28.87ms / 41.13ms | 24.99ms / 29.40ms |
  | 2 | 29.05ms / 40.17ms | 25.66ms / 31.75ms |
  | 3 | 27.39ms / 37.96ms | **28.85ms / 45.93ms** |

  Two repeats favour the merge by 13–28% and the third reverses it, so the ranges overlap and
  there is no result here. *The first run alone read as a 28% cut to p90 and that figure is
  noise* — recorded because a single arm of this workload is exactly convincing enough to be
  quoted. Throughput cannot show anything either way: both generators are open-loop at a fixed
  rate, so before and after both deliver 1,092 ok/s by construction.

  So this lands for [CH9](#ch9--bulk-validation-clones-the-batch-and-tantivy-docs-are-built-inside-the-transaction)'s
  reason rather than a performance one: the second transaction was unconditional and
  unnecessary, and the code now matches the comment that has described it since before it was
  true. An `fsync` saved is real work removed even where a 30s arm cannot see it.

  *What the measurement did surface is dead weight.* `apply_batch_and_maybe_commit` returns a
  `new_docs` count that the writer thread splits across callers with careful integer remainder
  arithmetic — and **both callers discard it**, at `node_orchestrator.rs` ~6232 and ~6290
  (`let (sequences, _new_docs)`). The split is preserved here rather than removed, to keep this
  change to one thing; taking the figure out of the channel type belongs with
  [L17](#l17--dead-code-and-stale-suppressions).

---

## H. Observed

Defects found outside a phase's own work. An entry keeps its evidence here even once it is
scoped — OB1 was Phase 18's first item and OB2 is what J1 fixes — because the observation and
the plan answer different questions, and splitting one across two places loses the reason it was
opened.

### OB1 — `fast: false` is not honoured on a numeric field

✅ **Done** 2026-08-27, as the first item of
[Phase 18](#j-phase-18--field-types-facet-and-json--partial) and ahead of
[J2](#j2--a-json-field-should-mean-subfield-addressing), whose `fast` override it would otherwise
have eaten. Observed 2026-08-13, carried outside the repository, filed here 2026-08-26. The
evidence below is what was found; what landed is at the end of the entry.

A `PUT /api/{index}/_config` declaring an i64 field with `"fast": false` reads back from
`GET /api/{index}/_config` as `"fast": true`. **Reproduced 2026-08-27** on a running node while
auditing the sort rules, and it has a consequence worth recording: because every numeric and date
field is forced fast, the refusal `unsortable_sort_field` exists to deliver — "a numeric field
must be declared fast to sort" — is unreachable for those types. What reaches it in practice is a
boolean, ip, json or facet field.

**Mechanism confirmed 2026-08-27**, and it is not `FieldDef::new` — it is
`normalize_after_deserialization`, which ends its numeric arm with an unconditional
`field_def.fast = true;` under a comment reading "should be fast by default for range queries".
A default is what it meant; an assignment is what it is, and it runs on every deserialization, so
a declared `false` is overwritten every time the schema is read.

**The reason it cannot simply be made conditional** is one line above it in the struct:
`#[serde(default)] pub fast: bool`. A `bool` with a serde default cannot tell an absent key from
an explicit `false` — both arrive as `false` — so there is nothing for a condition to test. The
fix is therefore structural rather than a one-word edit: `fast` has to be three-state on the wire,
`Option<bool>`, resolved to a concrete value once during normalization. That is compatible in both
directions — a stored `"fast": true` or `false` reads back as `Some(..)`, and serializing a
resolved value emits the same concrete boolean any existing reader expects — so no schema on disk
changes shape.

**Fixing it is not retroactive**, which is worth knowing before it is fixed.
`orch_create_config` normalizes *before* storing, so the forced `true` is already baked into every
stored schema record and the column was built to match — record and index agree on every index
that exists. So the fix changes nothing for existing data: it only lets a *new* index express
`false`, and then that index gets no column. What follows for such an index is exactly the
contract already documented — ranges and comparisons keep working, since those need no column, and
a sort on the field is refused with a 400 naming it. The refusal becomes reachable rather than
changing.

A prerequisite rather than a curiosity, which is why it went first:
[J2](#j2--a-json-field-should-mean-subfield-addressing) defaults a `json` field to `fast` and lets
a caller turn it off, which is precisely the override this defect ate.

Not an MCP defect, and not a sort defect: the engine's fast-column guard refuses a genuinely
non-fast sort correctly. What was wrong is that the config said one thing and the index did
another, which is exactly the distinction `searchable` and `sortable` exist to report.

**What landed.** `fast` is `Option<bool>` on the wire, skipped when absent, and
`FieldDef::is_fast()` is now the only correct way to read it — `None` resolves through
`FieldDef::fast_by_default`, which states each type's default in one place instead of leaving it
implicit in an arm. `normalize_after_deserialization` resolves the value once, before any per-type
arm can reach it, and writes the concrete result back; the numeric arm that did the overwriting is
gone. A shadow field resolves to `false` whatever it declared, since it is never added to the
Tantivy index and a column it cannot have is not a claim worth reporting.

The compatibility prediction held in both directions: a normalized schema still serialises a plain
boolean for every field, so `_config`, the index listing and the MCP schema surfaces read exactly
what they read before, and no stored schema changes shape. So did the contract prediction — on a
field that declined its column, equality, ranges and comparisons all work and only the sort is
refused, naming the field.

Pinned by `crates/storage/tests/fast_column_declaration_test.rs` — the declaration survives, the
default still applies to a field that declared nothing, the built index carries no column for the
one that declined, and the sort is refused while the ranges are not — and by
`a_numeric_field_can_decline_the_fast_column_it_gets_by_default` in
`crates/server/tests/node_http_api.rs`, which is the reported symptom end to end: `PUT` with
`"fast": false`, `GET` reads `false`, `sortable` is `false`, a range returns its hits and a sort is
a `400`. All four storage tests were run against the restored override before the fix and all four
failed, the index-level one reporting `{"defaulted", "asked", "declined"}` as the sortable set.
`docs/API_REFERENCE.md` now documents the three states and what declining a column costs.

### OB2 — A `facet` field cannot be written to

✅ **Done** 2026-08-31. Found 2026-08-27 while auditing what the MCP syntax reference advertises,
and scoped the same day as [J1](#j1--a-facet-field-cannot-be-written-to), where the fix landed.
The evidence stays here; the plan is there.

Every JSON value shape is refused. `staged_schema_validation` infers a type from the value and
compares it with the declared one: a string infers `Text` (or `Date`, or `Ip`), a number infers a
numeric type, an object infers `Json`, an array infers `Text`. Nothing infers `Facet`, so a
document naming a declared facet field fails with `Type mismatch for field 'category': expected
Facet, got Text` whatever it carries. Confirmed against `"/electronics/phones"`,
`"electronics/phones"`, `["/electronics/phones"]` and `{"path": "…"}`.

**Everything below the validator is already built.** `create_schema_from_definition` declares the
column with `add_facet_field`, both write paths have a `TantivyFieldType::Facet` arm calling
`add_facet`, `normalize_facet_query` quotes the path so the grammar accepts it, and the type
round-trips through the schema record and back from Tantivy. The type is declarable, queryable in
principle, and unwritable in fact.

**Which makes it an advertised operator for a field no document can carry.** `field:/path/to/value`
is in the operator table, `facet` is in the field-type table, `hint_for_type("facet")` renders a
per-field hint, `describe_index` would show it in `query_hints` for any index declaring one, and
the orchestrator skill tells an agent that a facet path matches everything under it. An agent
following that guidance cannot be wrong about the syntax and cannot ever meet the data.

The fix belongs in the validator — accept a string for a declared `Facet` field, and let the
storage layer's existing arm index it — not in the reference, since deleting the operator would
document a bug as a decision. Worth checking `ip` and `boolean` for the same shape while there:
a string infers `Ip` only when it parses as an address, so a declared `ip` field is writable, but
the inference is the same single-guess design.

**The panic underneath it is fixed** (2026-08-27), and had to be first. `Facet: From<&str>` is
`from_text(..).unwrap()`, so `add_facet` panics on a value that is empty or does not begin with
`/` — on the shard's writer thread, from a document body, and with `panic = "abort"` in the
release profile that ends the process. This item was the only thing preventing it, which made the
defect load-bearing: relaxing the validator without fixing the constructor call would have turned
an unusable field type into a way to stop a node with one document. Values are now checked where
they enter the index, on both write paths, and refused by name; the replay path skips and warns
instead, since the value is already committed and failing an index open over one field serves
nobody. With the panic fixed, the remaining question — does the type earn its place — was a
decision about the feature, not about safety, and it landed as [J1](#j1--a-facet-field-cannot-be-written-to).

### OB3 — A single write or delete can land on the wrong shard

✅ **Done** 2026-09-01.

On an index that routes by a non-key field, the single-write and single-delete handlers default
their routing hint to `id` (`write.rs:37`, `write.rs:91`). The engine then re-derives the key
from the schema's routing field (`effective_routing_key`, `node_orchestrator.rs`) and routes by
the ring — but if that shard is on another node, both `engine_write` and `orch_write` answered
`"Shard not found"` instead of forwarding. The bulk path routes every document by the schema
field and forwards, so this was a single-path divergence, not a routing-model gap.

**What landed.** `engine_write`/`engine_delete` now return the op back (`NeedsActor`) when the
ring's target is not a local shard, and `orch_write`/`orch_delete` forward it to the owning node
through `forward_op_to_owner` (reusing the peer pool the bulk paths use). `effective_delete_routing_key`
no longer lets a caller-supplied key retarget a key-routed index: on a default or shadow index the
id routes whatever was sent, and on a tenant index a missing or empty key is refused. Pinned by the
updated `a_delete_routes_by_the_id_unless_the_index_routes_by_something_else`.

**And exactly one hop, added 2026-09-01 reviewing the fix.** Forwarding had no bound: the node an
op is forwarded to runs the same routing decision, from *its* ring view and *its* copy of the
shard assignments, and those disagree while membership is changing. Two nodes each certain the
other owns the shard would pass one write between them until something timed out — once per
write, on every single write and delete, where before this fix the same disagreement was a plain
error. `ClientOp::Write` and `ClientOp::Delete` now carry `forwarded`, set by the node that
forwards and refused by the node that receives one already set; the error names the disagreement
so a retry lands after the views converge. The flag is `#[serde(default)]` and kameo encodes a
remote message with `rmp_serde::to_vec_named`, so a peer that predates it decodes as a first hop —
pinned by `an_op_without_the_forwarded_flag_reads_as_a_first_hop`.

Not closed here: the *bulk* forwarding paths have the same unbounded shape, and always did. They
are one call each and the same flag fits them; left out because a bulk forward carries per-item
accounting that a refusal has to answer in, which is [OB9](#ob9--a-bulk-delete-drops-a-peers-per-id-errors)'s
territory rather than this item's.

### OB4 — The single-write path stages Tantivy before redb commits

✅ **Done** 2026-09-01.

`apply_write` ran `delete_term`/`add_document` *inside* the open redb transaction, before
`write_txn.commit()`. `apply_batch` committed redb first and applied Tantivy after. If the redb
commit failed, the single path had already buffered the document in the IndexWriter; the next
commit flushed a document redb never accepted — the "search index ahead of the document store"
hazard, with no WAL entry to repair it, and exactly the asymmetry the batch ordering exists to
prevent.

**What landed.** `apply_write` now commits redb first and applies Tantivy after, in the same
shape as `apply_batch` (the Put arm stages the Tantivy document before `begin_write` and applies
it after `commit`; the Delete arm removes the row in the transaction and `delete_term`s after).
The write/delete paths can no longer leave a document buffered in the writer that redb does not
have.

### OB5 — One batch can index the same id twice

✅ **Done** 2026-09-01.

`apply_batch` issued one `delete_term` per *updated* id but `add_document` for *every* put. Two
puts of the same id in one batch therefore became one delete plus two adds: two documents under
one id term, while redb kept only the last row. Reachable by a bulk write carrying a duplicate id,
or by two single writes of one id coalesced into one `apply_batch`.

**What landed.** `apply_batch` now coalesces to the last operation per id — a Vec kept in
first-occurrence order, with an index map that overwrites an id's entry in place on a repeat — so
each distinct id contributes one Tantivy document and the add order (which breaks score ties)
stays deterministic. Pinned by `a_batch_keeps_the_last_document_of_a_repeated_id`.

**The other half, found reviewing the fix on 2026-09-01 and now closed with it.** Coalescing the
*adds* left the *deletes* reading from the wrong place. Whether an id needed its prior version
removed came from what each `data_table.insert` displaced — and a delete earlier in the same
batch had already emptied that row, so a re-put of a committed id looked new, no `delete_term`
was issued, and the document from an earlier batch stayed in the index beside the replacement.
Two hits for one id, both reading back as the new body, because the read path joins to redb by
id and redb held one row. Reachable without a hand-built batch: `handle_delete` deliberately
sends a delete as a `WalOp::Delete` so it coalesces with the writes that arrive with it.

`existed_before` is now decided on an id's first appearance and never revised — the `Delete` arm
reads `remove`'s return value for exactly this — and the Tantivy pass interleaves the removal and
the add per id rather than running two passes over two differently-derived lists. Pinned by
`a_batch_replaces_a_committed_document_it_deletes_and_puts_again` and
`a_batch_that_puts_then_deletes_an_id_leaves_no_document`.

### OB6 — Evolving an already-indexed field's type silently unindexes its values

✅ **Done** 2026-09-01.

`should_evolve_field_static` upgraded an *indexed* field's declared type with no `indexed` guard
and no rebuild. The Tantivy column kept its old type, so `add_json_value_to_doc` wrote the new
type against the old handle: Tantivy silently skips non-matching values for `Str` fields, and
errors on numeric→numeric — the value stored in redb and never indexed.

**What landed.** `evolve_field` now refuses to change an indexed field's type: the type is pinned
to the column the index built for it, and a change goes through a schema edit and a rebuild. A
non-indexed field still evolves freely (it has no column yet). Pinned by
`an_indexed_fields_type_is_pinned_to_its_column`.

### OB7 — The validator and the writer disagree about floats in integer fields

✅ **Done** 2026-09-01.

`scalar_type_is_storable(I64|U64, F64) => true` waved a float into an integer field, but the
writer reads it with `as_i64()`/`as_u64()`, gets `None`, and skips it — the document stored with
the field silently unindexed.

**What landed.** `scalar_type_is_storable` no longer accepts `F64` in an `I64`/`U64` field:
`String`→`Text` is the only widening the write path performs on its own. A float in an integer
field is now a `400` naming the field, so the validator and the writer agree. Pinned by the new
case in `a_value_the_declared_type_cannot_hold_is_a_bad_request`.

**A second disagreement of the same kind, found 2026-09-01 and closed with it.** Typing a list by
its elements — right for a field nobody has declared, and landed the same day — made
`infer_field_type` answer `I64` for `[1, 2]`, and `unstorable_scalar` asks `infer_field_type`
about each element of a list. So `[[1, 2]]` under a declared `i64` field passed: the inner list
"is" an `I64`. The writer flattens exactly one level and reads each element with `as_i64()`,
which returns `None` for a list, so the value was skipped and the document stored with the field
unindexed. `unstorable_scalar` now refuses an array or an object before asking what type it
infers as, which is the only reading under which the writer and the validator agree.

### OB8 — A paged search loses its page on the streaming fan-out

✅ **Done** 2026-09-02, found 2026-09-01.

`handle_broadcast_streaming` matched `ClientOp::Search` and dropped `offset` (`offset: _` →
`offset: None`), and `route_and_handle_inner` sends a plain `Search` op down that path when
routing is `Broadcast` and `enable_streaming_search` is on. On a multi-node cluster with
streaming enabled — the default, and what the shipped docker config sets — a paged
`POST /api/{index}/search` silently returned page 1 at every offset, including offsets past the
end of the result set. A standalone node was never affected: an unkeyed search there routes
`Local` and reaches `orch_search` with the real window, which is why this stayed hidden.

**What landed.** The offset is honoured rather than refused, because the machinery to honour it
was already there — `order_hit_blocks` applies whatever `SearchWindow` it is handed, and the
non-streaming fan-out was already a working reference for the same three steps. Two of the three
were missing here: the offset had to survive the match, and every source had to be asked for
`offset + limit` rather than `limit` so the merge has enough to page through. A source that
skipped its own `offset` would drop rows belonging on the page, so the skip is still applied
once, after the merge.

The reading of the op is now one function, `search_window_for`, shared by both fan-outs. They
each had their own `match &op` and disagreed twice: the streaming one discarded the offset, and
they resolved a `Stream`'s limit differently (its own, versus falling through to the node
default). A `Stream` has no offset to read — the HTTP route refuses one rather than accepting one
it would not honour — and that is now the only difference between them. Pinned by
`a_fan_out_reads_the_page_off_the_operation`.

The response reports `offset` alongside `limit`, as the non-streaming path already did. Without
it a caller cannot tell a correct page from page 1 returned twice, which is most of why this
survived.

**Not fixable before `2d13f4c`.** While the early-exit `break` that commit removed could discard
a source, a page assembled from whichever sources answered first is not the page that was asked
for, wherever the skip is applied. Every source always answers now.

**Verified on the 3-node compose cluster**, 46 documents over 9 shards, sorted `n desc`. Before:
every offset in 0/10/20/30/40 returned `[46…37]`, and `limit=10 offset=50` returned ten hits.
After: the five pages tile the full order exactly with no duplicates, deep pages with a small
limit are right (`limit=5 offset=41` → `[5,4,3,2,1]`), an offset past the end returns nothing with
`total_hits` still 46, and a sorted page is identical across five runs. Unsorted paging tiles the
order too. Both fan-outs pass the same script, with `enable_streaming_search` on and off. The
`offset + limit` ceiling still applies — `offset=10000, limit=10` is refused with the message
that says a deep page costs what a large limit costs — so the widened fetch adds no exposure.

Still true, and unchanged by this: no test in the workspace enables clustering, so nothing here is
covered by a regression test. A single-node test cannot reach the branch at all.

### OB9 — A bulk delete drops a peer's per-id errors

✅ **Done** 2026-09-02, found 2026-09-01.

`forward_bulk_delete_to_remote` read a peer's `errors` array, logged it, and returned only
`items_deleted` — so per-id failures on a peer arrived as a shortfall in the count with nothing
to explain it, breaking `items_received == items_deleted + errors.len()`.

**Two more paths broke the same invariant**, and fixing only the peer's reasons would have left
it broken, so all three landed together:

- **A local shard batch that failed** produced one reason for however many ids were in it.
- **A forward that never reached a node** produced one reason for the whole slice — the case
  that shows up the moment a node goes down, and the one measured below.
- **A node with no known address** produced one reason for its whole group.

**What landed.** `forward_bulk_delete_to_remote` returns `(deleted, reasons)` as the bulk-write
forward already did, and every failure path names each id it lost rather than the group. The
counting shell of `renumber_reasons` is now `balance_reasons`, shared by both paths: too few
reasons is padded so a shortfall is stated rather than left to be found by subtracting, too many
is folded onto the last reason so nothing anyone took the trouble to send is dropped.

The delete side needed no renumbering, which is where this differs from the ROADMAP's own
proposal. A write's reasons name positions, and a position is only meaningful in the batch it was
numbered against — hence `remote_rejections`. A delete's reasons name **ids**, and an id means
the same thing on every node, so `remote_delete_rejections` passes a peer's reason through as the
peer said it. Only a reason in some other shape — "the shard did not take the batch" — is
attributed to the node that said it, because there the node is the useful fact. The caller gets
one flat list keyed by id whichever node handled the id, matching how its own local failures
already read.

`orch_bulk_delete` now ends with the same `debug_assert_eq!` plus release-mode `error!` the bulk
write path has, so a path that stops accounting for what it drops becomes a test failure rather
than an unbalanced answer.

**Verified on the 3-node compose cluster.** With one node stopped, deleting 30 ids:

| | before | after |
|---|---|---|
| `items_deleted` | 17 | 17 |
| `errors` | **1** — "remote forwarding failed", naming no id | **13**, one per id |
| adds up to 30 | **no — 18** | yes |

Each of the 13 names a distinct id from the batch, so the caller knows exactly what to retry;
the retry once the cluster was whole deleted all 30 with no errors. Also balanced: a healthy
delete of all 30, absent ids counted as deleted rather than refused (the delete is idempotent —
`handle_batch_delete` returns one sequence per id it was given), an entry with an empty id, and
the degraded batch coordinated from each surviving node in turn.

**Not covered by the cluster test:** a peer that answers with per-id reasons of its own. Both
nodes split by the same shard map, so a peer never re-forwards a share it was given, and
provoking it needs fault injection. `remote_delete_rejections` is unit-tested for that branch —
pass-through, attribution, an id from another batch, padding and surplus — and its counting path
runs on every healthy cross-node delete above.

### OB10 — The record limit meant different things depending on how the wire split the line

✅ **Done** 2026-09-01, reviewing the 35f8fbc stream fix.

Moving the oversize check after the complete lines are drained fixed the false positives it was
written for, and left the check reachable only for a line *still arriving* — the buffer has
outgrown the limit and no newline has appeared yet. A line only just over the limit never gets
there: the chunk carrying the overflowing bytes carries the terminating newline too, the drain
sees a complete line and takes that path, and nothing measured it. The same file was accepted or
refused depending on framing, which is the one thing a caller cannot plan around.

**What landed.** The limit is applied to a complete line as well, in one wording shared by both
paths (`oversized_line`). Pinned by
`a_write_stream_refuses_an_oversized_line_delivered_whole`, which sets a 1 MB ceiling and sends a
line a kilobyte over — the delta is what puts the overflow and the newline in one chunk, and
without the fix that line is parsed and written.

### OB11 — Reasons and totals that went missing on the way out

✅ **Done** 2026-09-01, reviewing the 59dd785/35f8fbc accounting work.

Two small holes in code whose whole subject is not losing track of anything. `renumber_reasons`
returns exactly one reason per item unwritten, which is what makes a bulk answer add up — and it
discarded every reason past that count, so a peer that said more than the arithmetic expected had
the surplus dropped silently. A surplus is now folded onto the last reason: the count still holds
and nothing said is lost.

The `debug_assert_eq!` accounting invariants in `orch_bulk_write` and the write-stream handler are
compiled out of the build that actually serves the caller the unbalanced total. Both now log at
`error!` on the same condition, so the assertion catches an unaccounting path in a test and the
log catches it in production.

### OB12 — The schema gate deadlocked a fan-out against itself

✅ **Done** 2026-09-02, found verifying the 0.3.3 perf work on the compose cluster rather than by
reading anything. **The most serious defect of this cycle**, and it was in unreleased code:
[3f6fdb7](#the-order-of-work)'s gate, which had been verified on a path that happened not to fan
out.

The gate runs inside `NodeOrchestrator`'s message handler. A bulk write holds that mailbox for its
whole duration — which is [CH12](#ch12--write-path-serialization-and-round-trip-waste)'s fourth
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
— rather than by reading the message text, which is what bfe5fde's classification work exists to
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

### OB13 — The health endpoint fails under overload, but not for the reason it looked like

✅ **Investigated, closed into [F7](#f7--the-request-timeout-sheds-the-client-not-the-work) and
fixed by it on the search path** 2026-09-15 — **reopened on the write path and fixed there too**
2026-09-16. Under bulk overload health returned 408 at 1,001ms again on a different mechanism:
it is actor-served and queues behind the `BulkWrite` holding the orchestrator mailbox, where F7's
exemptions cover only the semaphore and the backlog gate, and its own `HEALTH_ACTOR_TIMEOUT`
could not fire — five seconds per call, four sequential calls, against a one-second request
budget. Bounded now against the node's resolved timeout, with a `degraded` field naming what fell
back: 200 on twelve of twelve probes at ~505ms. `/_admin/workers` stayed at 200 in 0.7ms
throughout, which is what localised it. See
[F8](#f8--the-overload-gates-do-not-cover-the-bulk-write-path); everything below stands as the
search-path record.

*Twice now this endpoint has failed for a reason other than the one it looked like* — first the
layer order, which was innocent, then "actor-served", which was true but hid that the guard was
simply larger than the budget. The lasting rule is the one the fix encodes: a timeout inside a
request must be derived from that request's own budget, never written as a constant beside it.

**The original observation**, the same day: `/_cluster/health` returned 408 during the F7
collapse. It does — but the layering explanation was wrong, and the fix it implied would not
have worked.

**Confirmed fixed by F7's dequeue rejection**, the same day: under 3,000/s against the node that
produced the 1001.8ms all-408 row below, sixteen consecutive probes returned **200 in ~160ms**.
No change was made to the health endpoint or to the layer order, which is the evidence that the
layering was never the cause. Re-measured 2026-09-16 against a binary built from the commit
before the fix, in the same session: that node answered its third probe onward with a 408 at
1,001ms while the current one held 200 at ~130ms, sixteen of sixteen.

**The original reading.** `HEALTH_PATH` is exempt from the concurrency guard (`routes.rs` 105) so
that "a load balancer would [not] evict a node that was merely busy". `TimeoutLayer` is applied
*after* the guard (`routes.rs` 241 against 112) and a later `Router::layer` is the **outer** one,
so a health request skips the semaphore and is still wrapped by the timeout. That much is
factually true. The inference — that reordering the layers would fix it — is not.

**Measured.** Health probed every 0.2–1s from a process separate from the load generator, while
the node was driven open-loop with release binaries co-located, 5,000- and 200,000-document
indices. The `timeout` column is the configured value; at the 128-admission rows the value the
node resolved is 60s either way — see the derivation noted in
[F7](#f7--the-request-timeout-sheds-the-client-not-the-work), which `check-config` prints.

**Apple M1** (4+4 cores, capacity ~9,300/s at 5k and 8 read threads):

| admission | timeout | index | offered | health p50 | max | codes |
|---|---|---|---|---|---|---|
| — | — | 5k | idle | 1.6ms | 1.8ms | all 200 |
| **128 (default)** | 30s | 5k | 4,000/s | 0.9ms | 1.8ms | all 200 |
| **128 (default)** | 30s | 5k | 9,000/s | 3.1ms | 13.6ms | all 200 |
| **128 (default)** | 30s | 5k | 14,000/s | 11.3ms | 21.0ms | all 200 |
| 3000 | 300s | 5k | 14,000/s | 12.4ms | 373.8ms | all 200 |
| 3000 | 300s | 200k | 3,000/s | 117.6ms | 119.4ms | all 200 |
| 3000 | **1s** | 200k | 3,000/s | **1001.8ms** | 1002.5ms | **all 408** |

**Apple M5 Pro** (5+10 cores, capacity ~26,500/s at 5k and 8 read threads), 2026-09-15. The
overload rows are placed at this machine's own knee rather than at the M1's absolute rate, since
14,000/s is comfortably *below* it here and would not be an overload row at all:

| admission | timeout | index | offered | health p50 | max | codes |
|---|---|---|---|---|---|---|
| — | — | 5k | idle | 1.9ms | 28.1ms | all 200 |
| **128 (default)** | 30s | 5k | 4,000/s | 0.6ms | 4.4ms | all 200 |
| **128 (default)** | 30s | 5k | 9,000/s | 0.7ms | 4.5ms | all 200 |
| **128 (default)** | 30s | 5k | 14,000/s | 0.8ms | 5.1ms | all 200 |
| **128 (default)** | 30s | 5k | 40,000/s *(50% past knee)* | 3.1ms | 7.8ms | all 200 |
| 3000 | 300s | 5k | 40,000/s | 3.3ms | 7.0ms | all 200 |
| 3000 | 300s | 200k | 3,000/s | 76.2ms | 80.8ms | all 200 |
| 3000 | **1s** | 200k | 3,000/s | **1001.5ms** | 1012.9ms | **all 408** |

**It does not reproduce at defaults, on either machine.** 21ms worst case on the M1 and 7.8ms on
the M5, both at 50% past their own knee, every probe 200. No load balancer evicts on that, and the
exemption is doing its job. The M5's 28.1ms idle maximum is its first probe against a cold
process and is the largest number in its column — which is the sense of scale to keep when reading
the overload rows beneath it.

**The last two rows of each table are the finding**, and they reproduce across a hardware
generation. They are the same node, the same index, the same offered rate and the same admission
depth. The only difference is `request_timeout_secs`, and it moves health from a comfortable
118ms (M1) or 76ms (M5) to a hard 408 at ~1001ms on both. So the queue is not what health is
waiting behind — the anonymous body is built from atomics and never reaches the read pool
(`health.rs` 78-81), which those 118ms and 76ms rows confirm directly. What starves it is the
**churn**: at a 1s timeout the node fires ~3,000 timeouts a second, and each one drops a future,
builds a 408, releases a permit and admits a replacement. That recycling saturates the request
runtime, and the health task cannot get a worker slot.

**That the 408 latency is ~1001ms on both machines, while the healthy latency differs by 1.5×, is
itself the evidence.** Health is not being made slow and then timing out; it is waiting out the
timeout exactly, because it never runs at all until the deadline fires. Hardware moves the 76ms
against 118ms and leaves the 1001ms where it is.

Which is [F7](#f7--the-request-timeout-sheds-the-client-not-the-work)'s own mechanism, seen from
the side. **Reordering the layers would not have helped**: it would convert a 408 into a >1s 200,
which a load-balancer probe treats identically. Fixing F7 — rejecting at dequeue so a request
whose deadline has passed never runs, which removes the recycling as well as the wasted work —
removes this too. No separate change is warranted, and the layer ordering should be left alone
until something demonstrates a problem it actually causes.

**Kept as an entry** because the wrong fix was one commit away, and because the default-config
row is worth having on record: it is the evidence that the exemption works where it matters.

---

### OB14 — A timed-out request never leaves the worker pool, and the node degrades until it is restarted

✅ **Found and fixed 2026-09-25**, by the first [M6](#m6--close-and-re-measure-the-bulk-lane) arm, on the
M5 Pro against `91fc382`, release build, harness co-located. This is the run F8's re-measurement
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
[`QueueLoad`](#f8--the-overload-gates-do-not-cover-the-bulk-write-path) predicts against and the
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
  That the body degrades to fallbacks instead of hanging is [F8](#f8--the-overload-gates-do-not-cover-the-bulk-write-path)
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
absent. [M4](#m4--per-key-resource-quotas) and [M5](#m5--per-index-capability-subtraction) bound
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
[M6](#m6--close-and-re-measure-the-bulk-lane) for the mechanisms, the two tests that fail against
the code they pin, and the overload run where goodput degrades to 89 ok/s instead of collapsing to
zero while health answers in 1.4ms.

**Reproduction, from a wiped volume**: start a node with the config above; run
`cameodb-bench --mode bulk --batch-size 100 --concurrency 1 --duration 10 --seed-docs 5000
--keep-index` and read the gap on `/_admin/workers` (0); run the same with
`--batch-size 500 --concurrency 8 --seed-docs 0` and read it again. It equals the `408` count and
stays there.

---

### OB15 — Every refused request was an `ERROR` line, and under write overload the logging cost half the goodput

✅ **Found and fixed 2026-09-25**, by the single-write arm of [M6](#m6--close-and-re-measure-the-bulk-lane)
session 3, on the M5 Pro against `c54ae33`, release build, harness co-located, F8 protocol.

**The shape.** Bulk and read overload degraded cleanly; single-write overload did not. At 2× the
write capacity (4,000/s offered against ~2,000/s sustained) goodput fell to **1,116 ok/s**, 15%
of requests ended as `408`, ~1,000 as transport errors, and `/_cluster/health` took up to 2.2s,
some probes answering `408` or refused at the socket. The pre-M4 baseline `079ad0b` did the same
(1,290 ok/s), so it was older than any of the M work.

**The cause was the log.** `TraceLayer::new_for_http()` logs every 5xx at `ERROR`, and
`AppError` logged every 503 at `ERROR` again — so each refusal cost a line, written synchronously
on the main runtime, where single-write jobs, their shard hand-offs and health all run. A default
node filters at `ERROR` (`tracing_subscriber::fmt::init` with `RUST_LOG` unset), so these were the
*only* lines it wrote, and nothing an operator would normally set turned them off: `RUST_LOG=warn`
measured 1,040 ok/s. 51,000 lines in a 20s arm at 4,000/s; 152,000 (31 MB) at 8,000/s. Reads
logged 90,000 and were not hurt, because search work runs on its own runtime.

Isolated before changing code — same binary, same arm, only the filter changed:

| `c54ae33`, writes at 4,000/s | ok/s | `408` | health max |
|---|---|---|---|
| default filter | 1,116 | 10,218 | 1,990ms, failures |
| `RUST_LOG=warn` | 1,040 | 10,335 | 1,715ms, failures |
| `RUST_LOG=info,tower_http=off`, two runs | 2,506 / 2,419 | 201 / 1,111 | 681 / 564ms, all 200 |

It was also an amplifier on an internet-exposed node: every request a caller can get refused cost
a line, at the one level no deployment filters out.

**The fix.** Refusals are counted, not logged one by one. `http_server/shed.rs` hooks
`TraceLayer`'s `on_response`, which sees every response — the door's and the concurrency guard's
503s, the timeout's 408s, the limiter's 429s — and emits one `WARN` per 10s naming how many of
each, the first refusal after a quiet spell at once. Its `on_failure` leaves 503 to that summary
and logs every other 5xx exactly as before. `AppError` marks admission and dequeue refusals as
`shed` and logs them, and every 429, at `DEBUG`; any other 503 — a peer that cannot be reached, a
schema the cluster cannot agree — keeps its `ERROR` line, because an operator has to see those.
The dequeue refusal now reads `request abandoned` rather than `read abandoned`, since the worker
pool raises it for writes too, and three per-shard `INFO` lines per bulk request moved to `DEBUG`.

**Measured after**, fixed binary against `c54ae33` back to back, default filter on both:

| writes offered | fixed | `c54ae33` |
|---|---|---|
| 2,000/s | 1,987 ok/s, sustained | 1,987 ok/s, sustained |
| 4,000/s (2×) | **2,621 ok/s**, 0 × `408`, 0 transport, health ≤ 518ms | 1,056 ok/s, 9,043 × `408`, health ≤ 1,740ms, 4 failures |
| 8,000/s (4×) | **2,541 ok/s**, 0 × `408`, 0 transport, health ≤ 510ms | 982 ok/s, 7,711 × `408`, 712 transport |
| 4,000 → 300/s | 2,659 ok/s, then 293 from the first second | 1,206, then 293 |

Goodput is flat from 2× to 4× overload and every refusal is a `503` a client can act on — the
lane now degrades the way the other two do. Bulk and reads did not move (bulk 105/112 ok/s at
120/300 offered, reads 884/872/871 ok/s). At `RUST_LOG=warn` the same 4,000/s arm wrote 124 lines
in total, the summary reporting 13,956 and 14,723 refusals per 10s window.

**What it does not cover.** The summary is a `WARN`, so a node at the default filter prints none
of it; the counters on `/_admin/workers` are the always-on figure. `health actor budget exhausted`
is still an `ERROR` per identified probe under overload — bounded by the probe rate, and
unreachable anonymously once authentication is on, so left alone. And the writer is still a
synchronous `stderr`/`stdout` write on the runtime: a non-blocking appender would stop *any*
future burst from stalling it, at the price of dropping lines when its buffer fills. That is a
separate decision and not taken here.

---

### OB16 — Closing an index from another thread lost the writes in flight on it

✅ **Found and fixed 2026-09-26**, by the pre-release concurrency audit (two static audits of
`storage` and `server`, verified against the code, then a regression test run on both trees).

**The shape.** Past the open-index cap ([M1](#m1--bound-resident-memory-against-index-count)),
admitting an index closes a colder one from whichever thread is opening — a search on the read
pool, `get_or_create_index` from the blocking pool — and closing commits. `apply_write` and
`apply_batch` reserved their sequences, committed redb, and only then took the writer mutex, for
the Tantivy add. A close landing in that window found the mutex free, read `current_seq` —
including the reserved, unadded sequences — committed without them, checkpointed them as
durable, truncated their WAL entries and dropped the writer. The writer thread then added the
documents to the detached writer. They stayed in redb, so every `id:` lookup found them, and were
gone from the search index permanently: the replay that should have restored them had had its WAL
truncated. `an_evicted_index_keeps_its_documents` read by key and could not see it. The cap
defaults to 8–256 per node, so this is the multi-tenant case 0.3.5 exists for.

**The fix.** A write holds its writer from before it reserves a sequence until the document is
added and counted (`lock_live_writer`), and confirms after locking that the writer is still the
one the shard holds, reopening if a close got in first (`WriterClosed`, retriable, after three).
`close_index` only `try_lock`s, skips a busy index, and holds the writer through the commit, the
checkpoint and the teardown. `commit_index` reads the sequence to stamp under the lock, where it
is now exact, and no longer holds the `writers` shard guard across the commit — the OB14 shape.
Lock order: writer mutex → schema lock → redb write slot; nothing holding a later one waits on an
earlier one.

**Pinned by** `closing_an_index_from_another_thread_loses_no_write_in_flight` (cap 1, a search
thread contending with a writing thread, counting what a *search* finds): **464 of 600** on the
old tree, 600 on the new.

### OB17 — A schema edit and an evolving write overwrote each other

✅ **Found and fixed 2026-09-26**, same audit. The writer thread evolves the schema row when a
document brings a new field; `set_default_fields`, `update_field_indexing` and
`store_schema_and_cache` edit it from the blocking pool. Each read the schema, changed a copy and
wrote it back with nothing serializing them, so either could erase the other — a field, or the
operator's `default_fields` — in redb and in the cache. Separately, `get_schema_cached` loaded a
schema on a miss (slow: a redb read and a Tantivy open) and `insert`ed it over whatever a
committed write had cached meanwhile, so the cache went backwards and the next evolving write
wrote a row without the newer field. A per-index schema lock (`lock_schema`) now spans every
read-modify-write, and the loader only fills a vacant entry. **Pinned by**
`a_schema_edit_and_an_evolving_write_lose_neither_change` (fails on the old tree) and
`a_loaded_schema_does_not_overwrite_one_cached_meanwhile`.

### OB18 — A streaming search ran outside the concurrency limit, and waited forever on a stalled client

✅ **Found and fixed 2026-09-26**, same audit. `route_and_handle_stream` spawns the search and
returns a body; the semaphore permit and the request timeout both ended when the handler
returned, so the search held no permit, and a client that kept its connection open without
reading parked the task on `send().await` — holding the whole result — for as long as the
connection lived. And the node's own comment said `tokio::spawn` inherits the
`REQUEST_STARTED_AT` task-local; it does not, so the task's deadline checks started from zero.
The permit now moves into the body for a response marked `StreamedBody`, a line the client has not
taken within `STREAM_STALL_TIMEOUT` (30 s) ends the stream, and the stamp is carried across the
spawn by hand. **Pinned by** `a_streamed_body_holds_its_permit_until_it_ends` and
`a_stream_abandons_a_client_that_stops_reading`.

### OB19 — Two clustered deadlocks through the coordinator's mailbox

✅ **Found and fixed 2026-09-26**, same audit; the second while fixing the first. Standalone nodes
were not affected. (1) `DeleteIndexCluster` awaited the local orchestrator's `DeleteIndex` from
inside the coordinator's handler, while orchestrator handlers ask the coordinator
(`peer_schema_for`, `forward_op_to_owner`, shard registration) — no timeout on either side. (2)
`ExchangeShardsWithPeer` awaited the peer coordinator's `QueryClusterState` from inside its own
handler, and the peer runs the same exchange on the same timer; kameo remote asks carry no reply
timeout by default. The coordinator now answers `GetDeleteTargets` from its own state and the HTTP
task runs `delete_index_cluster`; the exchange snapshots what it reads and runs in a spawned task,
its results arriving as `MergeRemoteShards` messages. Not pinned by a test: reproducing either
needs a two-node cluster and an interleaving the suite cannot force.

### OB20 — A fresh cluster can keep a partial ring, and nothing repairs it

✅ **Found and fixed 2026-09-26** by the new cluster suite (`scripts/validate/cluster.sh`, opt-in: three
nodes in Docker). Started together, **4 of 5** fresh clusters failed to converge within 60 s:
three times one node held 8 of the 12 shards for the whole minute, all three nodes reporting
`connected_nodes` 3; once node3 stayed connected to no one. After every restart of two nodes the ring converged within a
second; started seeds first — the shipped compose file's order — it usually did, but not always.

**Why the ring stays partial.** On a fresh start the shard maps change hands in one burst of
about 50 ms. The node that ended at 8 fetched `GetShardAssignments` only from node1, five times
(one per duplicate connection) and each time before node1 had merged node3; node3's pushes to it
are fire-and-forget `tell`s and did not arrive. After that, shard maps move only when a node's
*own* shards change (`RegisterLocalShards` triggers the stable-phase exchange), so a map missed in
the burst is missed until the next local shard change. The exchange that should repair it cannot:
`QueryClusterState` records the caller's state in `last_seen_state` and then asks
`remote_needs_update`, whose first test is whether the caller's state equals `last_seen_state`, so
`needs_full_sync` is always `false` and the exchange never pushes.

**Why node3 stayed alone.** Seeds are dialed once at startup; a dial that finds the seed not yet
listening is never retried (`RequestBootstrapRedial` is a stub).

**Baseline**, `cameodb:validate` at `f2df2ab`, OrbStack on an M5 Pro, two runs:

| | run 1 | run 2 |
|---|---|---|
| fresh start converged | 1 of 1 | 1 of 5 |
| storm: new-index writes, cluster-wide | 47 ops, 0.75 ok/s, p50 10.1 s | 47 ops, 0.78 ok/s, p50 10.1 s |
| storm: index deletes | 16, 13 × `503`, 1 timeout | 16, 13 × `503` |
| every node answers after the storm, and 60 s later | yes | yes |
| restart rounds converged / data intact | 3 of 3 / 30 of 30 | 3 of 3 / 30 of 30 |
| node1: connections established; closed; peer-lost for 6 real restarts | 34; 26; 6 | 34; 26; 6 |

The storm numbers are the schema-canvass stall — each new-index write waits on peers'
orchestrator mailboxes — and are what the cluster Phase 3 work moves. The connection counts are
the duplicate dials: two peers, and four or five connections to each on first contact.

**The fix, in three steps.**

1. *One connection per peer* (`swarm/mod.rs`). A peer is discovered on its first connection and
   lost with its last — the first `ConnectionClosed` used to mark a live peer lost while its
   other connections stayed up — and a Kademlia routing update dials only a peer not already
   connected or being dialed (`PeerCondition::DisconnectedAndNotDialing`).
2. *Seed redial*. A node with no peers redials its seeds from the swarm loop, 1 s doubling to
   30 s; `RequestBootstrapRedial`, a stub until now, asks for a check at once.
3. *Anti-entropy*. The merge no longer deduplicates on the pushed (generation, checksum) — that
   key was written by three handlers with three meanings, and every pull carries `(0, 0)`, so
   only a node's first pull from a peer ever merged. It is idempotent, and moves the generation,
   ring and snapshot only when a shard is added, removed or changes owner or tokens, not when its
   counts change. `QueryClusterState` answers `needs_full_sync` by checksum. Every node pulls each
   connected peer's map every 10 s (`spawn_shard_map_sync`), from spawned tasks with a 5 s bound
   on each step, so no mailbox waits on a peer. No wire change.

The suite gained a deterministic check for (2): node3 started 10 s before the seeds.

| | before | after |
|---|---|---|
| fresh start converged | 1 of 5 | **5 of 5**, each in under 1 s |
| node3 started before the seeds | alone after 60 s | **joined in 4 s** |
| seeds started first | usually converged; one run left node1 at 8 of 12 | not needed |
| node1: connections established; closed (6 real restarts) | 34; 26 | **8; 6** |
| dial failures, all nodes | 1–6 per node | **0** |
| ring rebuilds per node, whole run | — | 2, both at startup |

Storm and restart results are unchanged, as expected: the storm is bound by the schema canvass.

A Docker build also turned out able to link crates from an older build — the `target` cache is
shared and cargo judges freshness by mtime, which `COPY` preserves — and it surfaced here as an
image missing this week's storage changes. The build now touches the workspace sources first.

### OB21 — A peer that stops answering: detected, refused at once, and bounded while it lasts

✅ **Fixed 2026-09-26.** The cluster suite gained a frozen-peer phase: node3 is paused for
60 s (`docker pause` — the process stops, its TCP stays up) while node1 serves single writes, bulk
writes and searches.

**Fixed.** (1) The orchestrator's three forwards — bulk write, bulk delete, a write for a shard
owned elsewhere — run on its mailbox, and only the transport bounded each step; the registry
lookup had no bound and a schema resend doubled the wait. `RemotePeerPool::converse` now puts the
lookup, the ask and the resend under one deadline (the remote timeout) and answers
`PeerUnreachable`. (2) Peer references were evicted only on `PeerLost`; a request that fails to
reach a peer now evicts them, so a restarted peer is looked up afresh. (3) Topology went to the
orchestrator through a 16-slot queue that dropped the newest ring when full; it is a `watch`
channel now. No dropped ring was seen in any saved log — the risk was latent.

**Measured after (1)–(3), before 5.4** — the same as before them, because none of that changes what a frozen peer costs:

| during a 60 s freeze of node3 | result |
|---|---|
| single writes to keys node1 or node2 own | unaffected, 0.01 s |
| single writes to node3's keys | `503` after 20 s (two router attempts at libp2p's 10 s stream-open timeout) |
| bulk batches | node3's share fails after 10 s; the rest is written |
| searches | complete at the 5 s broadcast timeout, without node3's hits |
| node1's health | green, 3 connected, throughout |
| requests already delivered to node3 when it froze | wait the full 60 s request timeout |

**Fixed as well — 5.4, detecting a frozen peer.** (a) libp2p ping on every connection
(`ping_interval_secs`, `ping_timeout_secs`, 10 s each); libp2p 0.47 no longer closes a connection on
a failed ping, so the swarm does, and `PeerLost` follows when it was the last one. libp2p lets the
first miss pass, so detection takes up to 2 × interval + timeout + 10 s (the second ping's
stream-open timeout): ~40 s at the defaults, **35 s measured**. (b) A lost owner is
`RoutingDecision::Unavailable` — `503` at once; a search counts a lost peer as a failed node without
dispatching to it; the peer pool refuses lost peers, so bulk and forwarded shares fail at once too.
(c) The swarm redials every peer whose last connection closed, by peer id with backoff, so a peer
lost while others stay connected comes back. (d) The suite checks detection, fast failure after it,
and that no node failed a ping under the storm. Those refusals log at `DEBUG` — at 182 per minute in
the freeze, one `ERROR` each was the OB15 pattern again.

| during a 60 s freeze of node3 | before | after |
|---|---|---|
| single writes completed / refused `503` | ~46 / 8 | **515 / 171** |
| requests started once the loss was noticed | 20 s writes, 5 s searches | **all within 0.03 s** (506) |
| node1's health | green throughout | yellow **35 s** in |
| failed pings under the write-and-delete storm | — | **0** |

**Fixed as well — 5.5, a panic at shutdown.** A peer ask the coordinator spawned while the node was
shutting down was still waiting when the swarm stopped, and kameo 0.22 `unwrap`s the dropped reply
channel (`request/ask.rs:1010`); seen once in the baseline runs, contained to that task. Every
coordinator task that talks to a peer now goes through `spawn_peer_task`, which races it against a
cancellation token that `ShutdownSwarm` fires before it stops the swarm. Pinned by a test that
fails without the cancel.

Two things skewed early runs and are worth knowing before reading any cluster numbers: the Mac
sleeping mid-run (the suite now holds the Mac awake with `caffeinate`), and a fault landing
while a restarted node was still canvassing peers for a schema — the suite now warms the index
through every node first.

### OB22 — An orchestrator waited on peers while holding its mailbox

✅ **Found and fixed 2026-09-26.** The cluster suite gained a cross-node phase that sends requests
through all three nodes at once, as a load balancer does. Until then no phase made two nodes wait
on each other: the storm's writes all carry id `a`, so every index was minted on the one node that
owns that key, and the frozen-peer phase writes through node1 alone.

**The shape.** The orchestrator's mailbox runs one message at a time, and every request from a
peer lands in it. Four paths asked a peer from inside that mailbox and waited for the answer:
the schema canvass on a first write to a new index, the same canvass when an index delete looks
up a name this node does not hold, a forward of a write or delete whose shard is elsewhere, and
a bulk batch's shares. Two nodes on any of those paths at once each waited on the other's
mailbox until the canvass timeout (5 s) or the peer timeout (60 s), with everything queued
behind them — work that kept running for minutes after its clients had given up.

**Fixed.** (1) A bulk write or delete runs on the node that received it. It was routed whole by
its first document's key, which shipped two batches in three to another node's mailbox, where
the fan-out ran. The fan-out splits by document, so no route for the whole batch saves a hop.
(2) The index delete's lookup (`FindSchemaInCluster`) runs on a worker; the canvass it shares
with the write path is `SchemaCanvass`, which holds nothing of the actor. (3) A first write's
canvass runs in a task, and the op comes back to the mailbox as `MintAfterCanvass` to decide and
save the schema; the `ClientOp` reply is a kameo `DelegatedReply`, so the answer on the wire is
unchanged. While an index is being minted, `GetRawSchema` for it answers an error rather than
`null`: the asker refuses its own mint (retryable `503`) instead of inventing a second schema,
which is what the timeout used to do by accident. (4) Every forward — `forward_later`, and a
first-hop bulk's fan-out over an owned snapshot of the shard map and ring — is handed back as
`Answer::Later` and runs in a task.

**Measured** — `scripts/validate/cluster.sh`, 3 nodes, 20 s per cross-node load and 60 s of storm,
2 writers per node:

| | before | after |
|---|---|---|
| bulk writes through every node, index held by all | 6 batches, all `408` at 60 s | **10,197**, p50 0.01 s, max 0.18 s |
| new indexes, varied ids, through every node | 14, 0.1/s, 11 × `503`, p50 10 s | **2,687**, 134/s, p50 0.04 s |
| new indexes by bulk writes, through every node | 12, all `408` or `503` (measured with (1)–(3) in place) | **354**, 17.7/s, p50 0.33 s |
| storm: new-index writes | 47, 0.75/s, p50 10.08 s | **4,425**, 73.8/s, p50 0.08 s |
| storm: index deletes | 16, 13 × `503` and 2 timeouts | **1,195**, 19.9/s, p50 0.07 s, none refused |
| suite | 31 of 33 | **34 of 34**, `ERROR` 0 on every node |

**Fixed as well — the same index minted by several nodes at once.** With the canvass off the
mailbox, two nodes minting one index refused each other at once (the mint gate above), and
under a steady burst nobody won: first writes through every node to one new index, 6 at a time
for 20 indexes, were refused 88 times in 120, and 11 of the 20 indexes were never created. Now a
mint's canvass carries the asker's id (`GetRawSchema::minting_by`, defaulted), a node minting the
same index answers `SchemaBeingMinted` (verdict `minting`) and records the asker as a rival, and
each contender decides in `MintAfterCanvass` over every rival it heard of or was asked by: the
lowest node id mints (`mint_winner`), the rest wait off the mailbox for its schema and adopt it
(up to 5 s, then a retryable `503`). Any two contenders learn of each other, or one had already
saved, because deciding and saving happen within one mailbox message — so they agree, and the
lowest id never yields. An older peer reads the new verdict as a fault and refuses, as before.

| first writes to one new index, 6 nodes' writers at the same moment, 20 indexes | before | after |
|---|---|---|
| writes answered | 32 of 120, 88 × `503` | **120 of 120**, p50 0.04 s, max 0.07 s |
| indexes created | 9 of 20 | **20 of 20**, each minted by exactly one node |

**Fixed as well — a peer's ops on a lane of their own.** A forwarded share, a single write the
router sent to its owner and the local half of a peer's search still ran inside the receiving
node's mailbox. They waited on no peer, so they could not deadlock, but they were served one at a
time. `Message<ClientOp>` is now the peer entry only: what the worker lane can serve
(`worker_eligible`) runs on a peer lane — `engine.execute` in a task, under a semaphore sized like
the worker pool's in-flight capacity — and one that needs the actor comes back as `OnActor`, the
message this node's own router now uses, so a declined op is never offered to a worker twice.
The lane is separate from the worker pool on purpose: first hops on workers wait on peers' lanes,
and the lanes' work waits on no peer, so they always drain. Full, the op waits on the mailbox as
before, which is the back-pressure a peer used to get.

| 8 writers per node, 20 s, fresh cluster each | mailbox | peer lane |
|---|---|---|
| bulk batches through every node | 446–582/s, p99 0.15–0.29 s | **1,165–1,312/s**, p99 0.04–0.07 s |
| single writes through every node | 3,802–3,948/s | 3,887–4,108/s |
| new indexes by bulk writes | 18.3/s, p50 1.34 s | 18.4/s, p50 1.34 s |

A bulk mint's cost did not move, so it is not the mailbox, as this entry first guessed: it is the
mint itself — saving the schema and opening the index on every shard of every node — which is
the actor's by design. Single writes were never mailbox-bound at this load.

**Fixed as well — a streaming search's local half.** With `enable_streaming_search` on (the
default), `handle_broadcast_streaming` asked this node's own shards through the orchestrator's
mailbox — kept that way by the L16 refactor to preserve behaviour, not for a reason — so every
clustered search with no routing key served its local half one at a time, behind index creation
and schema edits. It now passes `handle_client_op`, as the non-streaming path does.

| searches through every node, 2 per node, 18 s | mailbox | worker pool |
|---|---|---|
| while bulk writes create indexes on every node | 39.5/s, p50 0.15 s, p99 0.50 s | **164/s**, p50 < 0.01 s, p99 0.32 s |
| mailbox idle | 1,671–1,721/s | 1,877/s |

**A single node pays nothing for any of it.** A standalone A/B against the commit before this
entry, closed- and open-loop, found every arm inside run-to-run spread — see
[M6](#m6--close-and-re-measure-the-bulk-lane), session 4.

### OB23 — Recreating an index after dropping its schema lost most of what was written

✅ **Found and fixed 2026-09-27**, loading `examples/data/booksummaries.tsv` after
`delete books --delete-schema` on a running node: `loaded=1030 failed=500` for 16,559 documents,
reasons that contradicted each other (`title` "expected I64" in one batch, `publication_date` in
another), and `An index writer was killed` on every batch to one shard. Reproduced on a fresh node
with the current build; a plain load, and an upgrade from 0.3.3 or 0.3.4, were never affected.

**Four defects, one chain.**

1. **The drop's record read as a schema.** `delete_schema` keeps a row versioned above the dropped
   schema so a write in flight cannot reinstall it. `GET /_config` returned it (no fields), so the
   loader saw a schema, applied none of the types the file's header declares, and the node typed
   every field from its documents.
2. **The minted schema was never cached** — the root cause. A write cached its schema only if it
   evolved or nothing was cached. A mint's sampling adds the fields itself, so nothing evolved, and
   the record stayed cached: every later batch read "no schema" and minted again from its own
   sample — 42 mints in one load — each overwriting the stored types while each shard's Tantivy
   index kept the first mint's.
3. **The writer added values by the stored type, not the built column.** When the two disagreed an
   integer reached a date column, Tantivy's indexing thread died (`Expected a Date for field
   "publication_date"`), and the dead writer stayed cached: every later write refused, every commit
   failed, nothing buffered since the last commit committed — until restart.
4. **The loader under-reported.** It counted the hundred reasons the node lists and ignored
   `suppressed_errors`, and each batch's line numbers restarted at 1.

**Fixed, in the same order.** A dropped index reads as absent everywhere a schema is reported —
`404` from `GET /_config`, "no schema" to a peer's canvass on either end, so an older peer still
sending the record is judged too — and the index minted over it is versioned above the record.
A write caches whatever schema validation settled on (`SchemaCache::keep_settled`: the handle is
no longer the cached `Arc`), so an index is minted once. Values are added by the built column
(`writable_type`) on the write, batch and replay paths, and saving a schema that retypes a built
column logs a warning — refusing it would break the designed edit-then-rebuild flow. A writer that
dies anyway is retired: the next use reopens the index and replays from the checkpoint, and a
write that met it is answered as written, since it was durable in redb first. The loader counts
listed plus suppressed, names each reason by its file line (or JSON document number), and treats a
schema with no fields as none.

| `booksummaries.tsv`, load → `delete --delete-schema` → load | before | after |
|---|---|---|
| second load | 1,030 written, "500" failed | **16,559 written, 0 failed** |
| mints in the second load | 42 | 0 (the loader declares the header's types) |
| writes without a declared schema after the drop, then commit | writer dead, **0 of 60 searchable** | 60 of 60 |
| a declared type that refuses 4,150 rows | `failed=` the listed 100 per batch | `loaded=12409 failed=4150`, lines as in the file |

Each test fails against the code it pins: `a_dropped_index_is_minted_once_by_the_writes_that_recreate_it`
(0 of 60 searchable on the old caching rule), `a_value_is_added_by_the_column_the_index_built`,
and the two dead-writer tests, which fail with retirement disabled.

### OB24 — `a AND NOT b` and `a OR NOT b` parsed cleanly and answered wrongly

✅ **Done 2026-10-05**, observed while scoping 0.3.6. Tantivy's grammar accepts `NOT leaf` in
every position a leaf can sit, but repairs an all-negative clause only at the top level —
`make_non_negative` appends a `?*` there and nowhere else. A `NOT` arm nested under `Must` or
`Should` therefore matched no documents, and the failure was silent: `title:rust AND NOT
tag:draft` answered 0 hits instead of the rust-but-not-draft set, `title:rust OR NOT tag:nosuch`
answered exactly `title:rust` with the `NOT` arm contributing nothing, and a bare `NOT
tag:draft` matched the right set while reporting `clause was dropped: Only excluding terms
given` — the correct answer wearing a false report. Verified on a store rather than read off the
grammar: rust/systems, go/systems, rust/draft, through `validate_query` and
`search_documents` alike.

**What landed.** `normalize_not_clauses` in `storage/src/query.rs`, first of the normalization
passes in `prepare_query_parser`, rewrites the text while it still says what the caller wrote:
`a AND NOT b` to `a AND -b`, `a OR NOT b` and a leading, grouped or signed `NOT` to `(* -b)` —
a `-b` appended to the disjunction instead would drop documents matching both arms — and
`a NOT b` to the `a -b` it already evaluated as. A run of `NOT`s collapses by parity, so
`NOT NOT a` is `a`. `NOT` counts only in uppercase at a leaf boundary, outside quotes and
outside `[]`/`{}` ranges and sets: `notes:x`, `a-NOT b`, `title:"a NOT b"` and `tag: IN [a NOT]`
are all untouched. The rewrite stays visible — `normalized_query` reports the engine's form,
which is the property [A6](#a6--the-syntax-reference-has-drifted-from-the-engine) established.

**The boundary kept.** Inside `field:( ... )` only the `-` rewrites apply: the `*` in `(* -x)`
would take the field's scope and `field:*` is refused, so `title:(NOT a)` is left for the parser
to report rather than rewritten into a form that means something else.
`not_query_forms_test.rs` pins the hits, the rewrites and the non-operators.

### F9 — Commit on a clock, not a count

✅ **Done 2026-09-26.** Found by the same pre-release profiling session: stack samples of bulk at
saturation (release build with symbols, `sample` on macOS) showed each shard writer **41%** of its
time in `join` — `prepare_commit` waiting for the indexer to flush — while the indexer sat **46%**
idle waiting for documents. A commit was triggered at `default_batch_size` operations (1,000 on a
new index), so bulk committed on nearly every drain and the two threads took turns. Raising the
threshold to 50,000 as a probe took 500-document batches from 21k to 151k docs/s.

The same rule left a trickle unsearchable: the idle commit fires only after a pause, so writes
arriving steadily below 1,000 waited for the thousandth — about 17 minutes at one per second.

**The policy now.** An index commits once its oldest uncommitted write has waited
`[search] commit_interval_ms` (2,000 by default); 20× the old count is a backstop bounding the WAL
tail; the idle commit (`supervisor_timeout_secs`, now 3 s, a second above the interval) takes the
tail of a burst. The writer answers a drain's callers first and commits once per index at the end of the
drain, so no write waits for a commit and a failed commit no longer fails a durable write. The
idle supervisor also no longer drops a nudge that arrives while its own commit runs. `0` keeps the
count-only policy, which the storage tests are written against.

**Measured**, release build, 4 shards, `wal_sync = true`, M5 Pro, back to back:

| load | before | 1 s interval | **2 s interval (default)** |
|---|---|---|---|
| bulk, batch 5,000, concurrency 32 | 183,528 docs/s, p50 845 ms | 344,668, p50 437 ms | **327,691**, p50 446 ms |
| bulk, batch 500, concurrency 16 | 19,746 docs/s, p50 420 ms | 130,595, p50 45 ms | **129,674**, p50 53 ms |
| single writes, concurrency 16 | 611 ok/s | 521 | **588** |
| single writes, concurrency 64 | 1,734 ok/s | 1,672 | **1,836** |

With `commit_interval_ms = 0` the new build does 627 and 1,710 single writes/s, so the other fixes
in this session cost nothing, and what moves single writes is the cadence. At 400–1,700
writes/s the count committed each shard every 2.4–6.6 s; at 1 s the clock committed every second
and 16 concurrent writers lost 15%, because on this machine every sync is `F_FULLFSYNC`, a
full-device flush (measured 297/s from one thread, 631/s from eight; plain `fsync`
15,000–31,000/s). At 2 s single writes are back inside run-to-run noise and bulk keeps its gain,
so 2 s with a 3 s idle commit is the default. A Linux host pays far less per commit.

**What the profile also settled.** The idle CPU on this box is not lock contention: with
`wal_sync = false` the same node did **65,285 single writes/s on 9.5 cores**, and reads scaled
linearly with `search_threads` to **11,593 searches/s on 12.8 cores** — which is why
`search_threads` now defaults to the available cores rather than a fixed 8. Durable single writes
on macOS are bound by the flush rate. Write-scaling claims should be confirmed on Linux.

---

## J. Phase 18 — Field types: Facet and JSON ◐ Partial

Scoped 2026-08-27, measured against a running engine rather than read off the source.

Two field types exist in the schema and are advertised in the query reference. `facet` could not
be written to, and is fixed — [J1](#j1--a-facet-field-cannot-be-written-to) landed 2026-08-31, so a
declared facet field now takes a path and matches a hierarchy. `json` can be written to, and still
behaves exactly like `text` — same terms, same matches, no subfield addressing. That is the
remaining item, and it is not only a fix: it alters what an index means, which is why it is
planned rather than patched.

**The order is fixed, and one item was a prerequisite rather than a preference.**
[OB1](#ob1--fast-false-is-not-honoured-on-a-numeric-field) went first and **landed 2026-08-27**: J2
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

### J1 — A facet field cannot be written to

✅ **Done** 2026-08-31. Smallest item here: one arm in the validator, then documentation.

`staged_schema_validation` infers a type from each JSON value and compares it with the declared
one. Nothing inferred `Facet` — a string inferred `Text`, a number a numeric type — so every
document naming a declared facet field was refused with a type mismatch, whatever it carried.
Filed as [OB2](#ob2--a-facet-field-cannot-be-written-to).

**What landed.** The validator now asks the parser the writer uses — `storage::facet_path_error` —
instead of `infer_field_type`, so a declared `facet` field accepts a string that is a path, and a
list of them judged element by element, and refuses a non-path by name rather than by type.
`infer_field_type` still never *infers* a facet: a path-shaped string in a field the schema has not
seen stays `text`, because a Unix path is not a hierarchy anyone asked for. Judged in the validator
rather than left to the writer, so a bad path is a per-document rejection in a bulk write rather
than a failure of the whole shard batch. Pinned end to end by
`a_facet_field_takes_a_path_and_a_list_of_them` and `one_unparseable_path_refuses_a_list_of_facets`
in `crates/server/tests/node_http_api.rs`.

**Everything below the validator was already built**, which is what made this small.
`create_schema_from_definition` declares the column with `add_facet_field`, both write paths call
`add_facet`, `normalize_facet_query` quotes the path so the grammar accepts it (without which
`cat:/a/b` parses as a *regex* — see J3), and since 2026-08-27 a value that is not a path is
refused by name rather than panicking the writer thread.

**What Tantivy's facet gives, measured** on `a=/electronics/phones`, `b=/electronics/phones/cases`,
`c=/electronics`, `d=/refurb/electronics/phones`, `e=/Electronics/Phones`:

| Query | Matches | |
|---|---|---|
| `cat:/electronics` | a, b, c | a parent matches every descendant |
| `cat:/electronics/phones` | a, b | at any level |
| `cat:/Electronics/Phones` | e | case-sensitive: no tokenizer runs |
| — | **not d** | anchored at the root, and matched on level boundaries |

The mechanism is worth recording because it is not what a reader assumes: the hierarchy is a
property of what was *written*, not of the query. Tantivy's `FacetTokenizer` emits the value and
every one of its ancestors, so `/electronics/phones/cases` writes four terms and a parent query is
an ordinary single-term lookup. One term per level per document; no prefix scan, no range.

**No migration, and not merely a small one.** A facet field had never accepted a document, so no
index held facet data and there was nothing to reindex, reconcile or read two ways. The type gained
a behaviour where it had none.

**Decisions considered but not decided here.** Whether a facet field should also be `fast` — today a
sort naming one is refused, and nothing has asked for it. Whether `/` remains the only separator a
caller may write. Neither blocked the item, and both stay open.

**Explicit non-goal: facet counts.** `FacetCollector` is not used anywhere and is not part of
making the type work. Counting documents per level is an aggregation feature, and CameoDB has no
aggregations — [Phase 19](#k-phase-19--field-metrics-min-and-max--planned) adds min and max on a
fast field and nothing else — which is worth saying plainly, because per-level counts are what most
people mean when they ask for facets. What this item delivers is a hierarchical *filter*.

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

  **This depended on [OB1](#ob1--fast-false-is-not-honoured-on-a-numeric-field), which landed
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
  outage, and [OB2](#ob2--a-facet-field-cannot-be-written-to) records one such panic reached from a
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
non-goal of [J1](#j1--a-facet-field-cannot-be-written-to). Min and max are worth their surface
because they answer a question about a field's range that nothing else in the API answers; the rest
are a different phase with a different design question, and each one merges differently.

**Rejected, and recorded so it is not proposed again.** `Column::min_value()` and `max_value()` in
`tantivy-columnar` give whole-column extremes with no scan at all, which is tempting and wrong:
they ignore the query and they ignore deleted documents, whose rows are removed from redb at once
and from the index at the next commit. They answer "what did this column ever hold", not "what does
this query match" — a different question, and one that gets more wrong the longer an index has been
written to.

---

## L. Post-0.3.4 review — preparing the next cycle 📋 Planned

Reviewed 2026-09-08 against the 0.3.4 cut: the 37 commits since v0.3.3 (~6,980 insertions, of
which ~3,430 are non-test) read end to end, plus a workspace-wide posture audit. The verdict on
the release is that it stands — the panic containment is layered and each layer is tested,
including against the built binary; the correctness fixes (checkpoint ordering, the schema
tombstone, the parser whitespace fold, the bounded SSE channel) are root-cause fixes rather than
symptom patches. Nothing below is a defect in what 0.3.4 changed.

What the review found instead is what surrounds the changes: four small defects adjacent to
them, the remainder of the security surface the hardening did not reach, and a structural fact
that has been true for three releases and gets more expensive each one — every 0.3.3 and 0.3.4
fix had to be made inside one of five files, four of which are above 2,800 lines.

**The splitting rule for L11–L13, agreed 2026-09-08:** few files, grouped by feature and
architectural meaning — related changes should land in one file, occasionally split across two
or three. No per-function-family fragmentation: a module split that leaves you hunting twenty
files for one call chain has traded one readability problem for another. Where a file legitimately
stays large (the orchestrator's dispatch core), that size is a statement about what the code is,
and is kept together deliberately.

**Why this is one group and not scattered across C, G and H.** The items come from one review,
answer one question — *what does the next development cycle inherit* — and are sequenced
together ([L20](#l20--the-retrospective-and-the-sequence-into-the-next-cycle)). They are held
here, untouched, until 0.3.4 has settled and been delivered. What happens then is also part of
the group: a retrospective over the 0.3.3/0.3.4 stability-and-security cycle, whose output is
the goal set for the cycle after it — which of these items becomes the next phase, in which
order, and which are deliberately not taken up.

Fidelity note: the line numbers were read on 2026-09-08 and will drift; the named functions and
the shapes are the durable part.

### L1 — Size-cache invalidation by substring evicts neighbouring indexes

**Defect.** ✅ **Done** 2026-09-08. Cache re-keyed from the formatted
`{shard}:{fast|full}:{index}` string to a `(include_data_size, index)` tuple, with one
`invalidate_size_cache` helper removing both flavours exactly at the two (identical) old
`retain` sites — the helper also absorbed two of L3's nine poison-unwraps. Covered by the
regression test `invalidating_one_indexs_cached_sizes_leaves_its_neighbours_alone` in storage,
and `cargo check --workspace` green.

**Original entry.** `index_size_cache` was keyed by the formatted string
`{shard}:{fast|full}:{index}`, and both invalidation sites (`storage/src/lib.rs` ~5402 and
~7138) evict with `key.contains(&format!(":{}", index))`. Committing or deleting index `a`
therefore also evicts the cached sizes of `ab`, `aa`, `ba` and anything else containing the
substring — silent cross-index eviction, invisible to every test because a cache that misses too
often still answers correctly. Key the cache by a `(mode, index)` tuple instead of a formatted
string; the lock-poison unwraps on the same lines belong to L3.

### L2 — `get_highest_indexed_seq` returns a value its own comment calls wrong

**Defect.** ✅ **Done** 2026-09-08. The unreadable-checkpoint branch now returns a new
`StoreError::CorruptIndex` (the scan orders on the `_seq` fast field but the top document carries
no stored `_seq` value to trust), which `checkpoint_seq` propagates and `recover_index` turns
into a failed index open — the posture its own comment already stated. Covered by the regression
test `a_checkpoint_scan_it_cannot_read_fails_instead_of_lying` in storage (a fast-but-not-stored
`_seq` drives exactly that branch: the sort key exists, the stored read finds nothing), full
`cargo test -p storage` and `cargo check --workspace --all-targets` green.

**Original entry.** The unreadable-checkpoint branch in `storage/src/lib.rs` (~3805–3813)
logged an error and returned an `Ok` carrying what the comment itself called a *wrong* value
(`u64::MAX - inverted_sort_key`-style reconstruction). Every caller treats the answer as truth —
the right shape for "this should never happen" is `Err`, so the path cannot silently seed a
replay window from a number nobody can trust.

### L3 — Poison panics, nine sites, inconsistent with the unwind posture

**Defect.** ✅ **Done** 2026-09-08. Every remaining `.lock().unwrap()` on the three mutexes now
recovers with `unwrap_or_else(|poisoned| poisoned.into_inner())` — four on `index_size_cache`
(`storage/lib.rs` `shutdown`, `invalidate_size_cache`, both ends of `batch_measure_all_indexes`;
the fifth and sixth cache sites were already absorbed by L1's re-keying), three on
`writer_monitor_handle` (spawn and both ends of `shutdown_writer` in `node_orchestrator.rs`) and
one on `runtime_join_handle` (`swarm/mod.rs` `wait_for_shutdown`). Covered by the regression
tests `a_poisoned_size_cache_does_not_poison_the_calls_that_follow` (storage) and
`a_poisoned_join_handle_slot_does_not_poison_shutdown` (server), both of which poison the mutex
through a real panic before the call; full `cargo test -p storage`, `cargo test -p server` and
`cargo check --workspace --all-targets` green.

**Original entry.** The release chose `panic = "unwind"` so a contained panic costs one
request — but nine `.lock().unwrap()` sites panic again on a poisoned mutex: five on
`index_size_cache` (`storage/lib.rs` ~3705, 5402, 7137, 7708, 7788), three on
`writer_monitor_handle` (`node_orchestrator.rs` ~4900, 5035, 5049) and one on
`runtime_join_handle` (`swarm/mod.rs` ~133). Everywhere else the codebase recovers with
`into_inner()` (`audit.rs`, `session.rs`). One poisoned stats or shutdown call currently makes
every later one panic. Make the nine match the rest.

### L4 — The `unsafe impl Send/Sync` on `HybridStore` is redundant-or-unsound

**Defect.** ✅ **Done** 2026-09-08. The two `unsafe impl` lines were deleted, and the compiler
proves `Send + Sync` for `HybridStore` on its own — `cargo check --workspace --all-targets`,
full `cargo test -p storage` and `cargo test -p server` all green, so the impls were dead weight
and not load-bearing for any thread-safety claim.

**Original entry.** `storage/lib.rs` ~7906–7908 hand-implements `Send`/`Sync` with a
comment claiming every component is already `Send + Sync`. If the comment is true the impls are
dead weight and the compiler will prove it when they are deleted; if it is false the impls are
unsound. Either way, remove them.

### L5 — Legacy-SSE work outlives its request and bypasses the guards

**Security, medium.** ✅ **Done.** The per-session bound is `max_in_flight` on
`SessionLimits`, enforced inside `McpSession::start_request` — the check and the map insert
run under the one lock, so a refusal cannot race a registration. A session at its bound gets
`429 Too Many Requests` with `Retry-After` on the POST itself (`in_flight_refusal`), the same
admission shape the HTTP guards make; notifications are exempt because their spawned task is
trivial (a notification is answered with silence before any dispatch). The number is
configurable as `mcp.max_in_flight_per_session`, default **32** — past what an agent's
parallel calls reach for, since the bound exists for the loop that does not stop. `0` is
refused at load. Covered by `a_session_refuses_past_its_in_flight_bound_until_one_finishes`
(refusal at the cap, slot freed when a task's guard drops, bound holds again).

The `tool_calls_per_minute` default stays `0`, deliberately: `ratelimit.rs` documents that an
upgrade must not start refusing calls it used to serve, and the in-flight bound is what
bounds the runaway-loop case the finding names — a non-zero rate would change behaviour for
every existing deployment without being the fix this item needed.

**Original entry.** Every `POST /mcp/messages` `tokio::spawn`s the full
`handle_rpc_request` and answers 202 (`mcp/transport.rs` ~295–326). The concurrency semaphore
(`routes.rs` ~100–119) releases its permit at the 202, the timeout never applies to the work,
and the MCP rate limiter is inert by default — so one session can hold an unbounded number of
concurrent searches and the per-session in-flight map grows with them. Bound per-session
in-flight requests, and consider a small non-zero default for `tool_calls_per_minute`: the
threat model in `ratelimit.rs` already names the runaway-agent-loop scenario this is open to.

### L6 — A `GET /mcp` listening stream without a session is unbounded

**Security, medium.** ✅ **Done.** Refused rather than capped: `streamable_listen_handler` now
requires the `MCP-Session-Id` header and answers `400` (`anonymous_listener_refusal`) when it
is absent. Capping anonymous listeners was the alternative; refusing is the honest answer
because the stream carries keep-alives and nothing else — the server never initiates requests —
so a session-less one could only ever occupy a connection, and a client handed the refusal
learns to `initialize` first. `ListeningStream`'s guard is a `ListenerGuard` rather than an
`Option` — a stream now always belongs to a session. Covered by
`a_listening_stream_must_belong_to_a_session` in `mcp_federated` (anonymous → 400, unknown →
404, live session → 200).

**Original entry.** A `GET /mcp` with no `MCP-Session-Id` is answered with an
infinite keep-alive stream (`mcp/transport.rs` ~481–516). It creates no session (so
`max_sessions` never bounds it), holds no semaphore permit and no timeout (both end when the
headers stream), and repeats per connection. Refuse pre-`initialize` listeners, or cap a global
anonymous-listener count.

### L7 — MCP tool errors leak what HTTP deliberately masks

**Security, medium.** ✅ **Done** 2026-09-19. Tool failures now travel as a typed
`ToolError{Caller, Internal}` end to end — `McpBackend`'s every method returns it, the tool
dispatcher raises `Caller` for the refusals it owns (arguments, bounds, scope, unsupported
names), and `server/mcp/diagnostics.rs::tool_error` classifies a routing error by
`RemoteVerdict`: `BadRequest`/`NotFound` keep the message written for the caller;
`Unavailable`, `SchemaRequired` and `ServerFault` are masked. `rpc.rs` answers with
`into_response_text()` — caller text verbatim, internal as `Internal server error`, the same
mask HTTP puts on a `500` — while `record_tool_call` audits `detail()`, so the operator's
record keeps what the caller cannot see. The federated `errors` array is masked per entry the
same way, since it rides inside a successful result. Tests pin each verdict arm, the
envelope-level mask, and the caller passthrough.

**Original entry.** `rpc.rs` ~329–338 put any tool error verbatim into the response text, and
`server/mcp/search.rs` forwarded `OrchestratorError::to_string()` — shard and storage
diagnostics, forwarded peer fault text — while the HTTP layer masks exactly this class on 5xx
([C7](#c7--a-500-printed-the-nodes-internal-error-text) closed this on HTTP; MCP had no
equivalent).

### L8 — The streaming-ingest error vector grows one string per bad line

**Security, medium.** ✅ **Done** 2026-09-08. `write_stream_handler` now collects its reasons
through a `BoundedErrors` helper that keeps the first `MAX_LISTED_STREAM_ERRORS` (100) and
counts the rest, and the response carries `errors` (the listed) plus a `suppressed_errors`
count. The accounting arithmetic (`written + listed + suppressed == documents`) is unchanged,
so the `debug_assert!` and the partial-vs-ok status still hold. Covered by the regression test
`a_stream_of_bad_lines_reports_a_bounded_number_of_reasons` in `node_http_api` (5 000 garbage
lines → ≤100 listed reasons + a 4 900 suppressed count), the existing stream tests still green,
full `cargo test -p server` and `cargo check --workspace --all-targets` clean.

**Original entry.** `write_stream_handler` (`http_server/write.rs` ~259–341) pushed one
formatted error per oversized or unparseable line into a `Vec<String>` unbounded within the
request. The wire body limit (~128 MB) caps the input, not the amplification: 2-byte garbage
lines become tens of millions of serde error strings, all held at once and then serialized
whole into the response. Keep the first N reasons plus a count of the rest — the NDJSON
error-reporting contract survives that. Sits beside
[C5](#c5--a-decompression-cap-on-the-streaming-ingest-path), which bounds the same path's
*input* amplification.

### L9 — A `Writer` key can mint indexes

**Security, medium.** ✅ **Done.** Posture decided: implicit creation is now a config gate —
`security.implicit_index_creation`, default `true`, so upgrades keep today's behavior. The
refusal sits at the one place minting is decided: the `NoneHeld` arm of
`staged_schema_validation`, where sampling has confirmed no schema exists anywhere — a
`Validation` error naming the remedy (`PUT /api/{index}/_config`, the `index-admin` route).
Adopting a peer's schema or applying a forwarded `schema_body` is *not* gated: those apply an
existing declaration rather than mint one. `SecurityConfig` lost its derived `Default` for a
manual one (`derive` would have defaulted the bool `false`, silently inverting the gate on
`..Default::default()`); the plumbing is `CameoDbConfig → NodeConfig → engine`, the same route
`default_search_limit` takes. The capability reference now documents both postures: `write`
includes minting indexes unless the gate is off. Tests: parse/default test in `config/tests`,
and an end-to-end test proving a write to an unknown index is refused `400` with the gate off,
that explicit `_config` creation still succeeds, and that the subsequent write lands.

### L10 — The low findings, in one place

**Security, low.** ✅ **Done** 2026-09-08. Five of six items fixed; the sixth is a
documentation finding:

- `mcp/transport.rs` `caller()`: now warns on every request that arrives with no authz
  extension, so a trust boundary that fails open is no longer silent. The permissive default
  is kept (security-off is the intended state it represents), but the `warn!` is load-bearing.
- `http_server/health.rs`: the anonymous health check now returns from local liveness atomics
  only — the coordinator actor round-trip moved below the `!identified` early return, so a
  health flood no longer becomes mailbox pressure.
- `routes.rs` `fallback_handler`: echoes `uri.path()` only, not `uri.to_string()` — the query
  string (which may carry tokens) no longer reaches the response body the trace layer logs.
- Index-name validation: `validate_index_name` is now `pub(crate)` and called in the `authorize`
  middleware on every request whose classified route carries an `{index}` segment, so the strict
  check `PUT /_config` always had is the one every write, search, and admin route gets. The
  fault-injection trap index names were renamed to pass it (`__fault_panic_write_op__` →
  `fault_panic_write_op`, `__fault_kill_writer__` → `fault_kill_writer`).
- `serde-saphyr` / `hickory-proto`: **confirmed — there is no CI.** The project has no
  `.github/workflows/` directory; `cargo deny check` and `cargo audit` run only via the manual
  `scripts/validate/deps.sh` gate, which skips both tools when absent and still passes. The
  `review-by 2026-11-01` exceptions in `deny.toml` are not checked automatically. This is a
  decision item for the retrospective in [L20](#l20--the-retrospective-and-the-sequence-into-the-next-cycle),
  not a code fix.
- HTTP search routes: `search_handler` and `search_stream_handler` now call
  `state.tool_limiter.check(key_id, 1)` — the same per-key token bucket the MCP surface uses,
  keyed by the same `key_id`, off by the same default (`tool_calls_per_minute: 0` →
  `Verdict::Allow`). A refused caller gets a 429 with a retry-after.

Full `cargo test -p server` (275 unit + 56 node_http_api + all integration suites including
panic isolation), `cargo test -p cameodb_mcp`, and `cargo check --workspace --all-targets` green.

**Original entry.** Six items, each a few lines to fix:

- `mcp/transport.rs` ~85–87: `caller()` fails open to `McpUnrestricted` when the authz extension
  is absent — unreachable today, wrong default at a trust boundary. Warn loudly or refuse.
- `http_server/health.rs` ~76–82: an anonymous health check still pays a coordinator actor
  round-trip while being deliberately semaphore-exempt — a health flood becomes mailbox
  pressure. Anonymous callers need only the local liveness atomics.
- `routes.rs` ~295–303: the fallback 404 echoes the raw URI, query string included, into a body
  the trace layer logs in full — while the audit layer takes care to strip it. Echo the path
  only.
- Index-name validation is two-tier: strict on `PUT /api/{index}/_config`
  (`catalogue.rs`), lax on write/search (`resolve_index_dir` blocks traversal, nothing
  else). One shared validator on every path.
- `serde-saphyr 0.0.28` is a very young parsing crate in the operator-config path; the
  hickory-proto advisories ride a `review-by` of 2026-11-01. Both are fine *if* `cargo deny
  check` runs in CI on every PR — confirm it does.
- HTTP search routes have no rate limiter while the MCP surface does
  ([C8](#c8--rest-has-no-rate-limit-and-anonymous-mcp-callers-share-one-bucket) is the same
  item, recorded earlier). Reuse the token bucket keyed per key — off by default, as with MCP.

### L11 — `node_orchestrator.rs` is 13,669 lines and holds four actors

**Decomposition.** ✅ **Done 2026-09-17.** One module containing four actors, a worker pool, an
engine, ~60 free helpers, ~25 wire types and ~90 tests, split by the layout below. The landed
sizes: `mod.rs` 1,131 (module doc, consts, wire and error types, re-exports), `search.rs`
1,373, `shard.rs` 2,096, `router.rs` 1,686, `orchestrator.rs` 6,643, `tests.rs` 3,161. Two
notes beyond the pure move: the single-module privacy model flattened to `pub(crate)` on
items, fields and inherent methods — the visibility the code already relied on — and
`validate_document` became the free function CH10's gate already treated it as, with its three
call sites de-qualified. The directory landed as `node/` rather than `node_orchestrator/` —
the file names already say what each part does — so `crate::node::…` is the import path,
re-exported from the root's `pub use` as before. 319 bin, 56 `node_http_api`, 50
`mcp_federated` and 41 client tests pass; clippy clean.

**Deliberately coarse: a handful of files grouped by architectural role, not one file per
function family.** The point of the split is that a change to one actor, or to the read-side
machinery, happens in one place — not that no file ever exceeds a line budget. Convert to a
directory module — the crate already does this for `http_server/`, `swarm/`, `admin/` — with
`mod.rs` re-exporting so every `use crate::node::…` site compiles untouched:

```
node/
├── mod.rs           (~1,000) module doc (keep the thread-topology essay), shared consts,
│                              every wire/message type, and the error types
├── search.rs        (~1,700) the read-side machinery: merge primitives (this is what CH2
│                              always wanted), validation, sort keys, reason accounting
├── shard.rs         (~1,900) MicroshardActor, the writer thread and its monitor, warmup,
│                              WriterLiveness and ReadPoolHealth
├── router.rs        (~1,600) RouterActor, whole
├── orchestrator.rs  (~5,200) NodeOrchestrator, the engine and the worker pool — the dispatch
│                              core, and deliberately still the big file
└── tests.rs         (~2,300) the unit tests, moved out as one
```

`orchestrator.rs` staying large is the design, not a failure: it is the one place "how a client
operation flows" is read, and grouping beats fragmenting it across per-file quotas. If it keeps
growing *after* the L15/L16 dedup shrinks it, the engine and the worker pool are the natural
next extraction — not a first move. Longest functions to break while there: `handle_broadcast`
(568 lines), `spawn_writer_thread` (350), `orch_bulk_write` (318). Move only, first — the dedup
passes are L15 and L16.

**Amended 2026-09-19 by [M0](#m0--the-architecture-review-and-the-order-of-work).** Two claims
above have since been corrected in place. The flattening to `pub(crate)` was *not* "the
visibility the code already relied on": it was true of the 15 names that cross the boundary and
false of the other ~129, which had been visible to nothing inside the single file — O1 put them
back behind `pub(super)` and named the 15 in `node/mod.rs`. And the next extraction was not the
engine or the worker pool but **admission**, on a different test: not that `orchestrator.rs` had
grown — it had not, 6,649 against the 6,643 it landed at — but that admission is a subsystem
whose invariants are properties of the whole set, which is what F8 cost a production-shaped bug
to establish. The file is 5,782 lines now, beside `admission.rs` (783) and `routing.rs` (172).

### L12 — `storage/src/lib.rs` is 9,961 lines, of which one impl block is 4,460

**Decomposition.** ✅ **Done** 2026-09-19. Five siblings, grouped by what the code *is* — the
pure sections lifted verbatim, then `impl HybridStore` split into a second impl block along the
write/read boundary it already drew:

```
storage/src/
├── lib.rs      (465)   crate docs, StorageConfig, StoreError, SortSpec/SortOrder,
│                       SearchOutcome/QueryValidation, stats + warmup wire types, re-exports
├── query.rs    (970)   the pure query machinery: whitespace/date/prefix/shadow passes,
│                       field-reference scanning, parser preparation, discarded-clause reports
├── schema.rs   (1,683) the data model: FieldDef, IndexSchema, SchemaState, date typing,
│                       document building, WAL/StoredDoc types, tokenizers
├── store.rs    (3,356) HybridStore + the write path and everything that guards it: lifecycle,
│                       get_or_create_index, apply_write/apply_batch, commit and checkpoint,
│                       recovery and warmup
├── search.rs   (1,605) reader pool, read caches, search_documents, validate_query, stats
└── tests.rs    (2,160) the unit tests, verbatim (mod tests keeps its name behind an allow)
```

`lib.rs` re-exports by glob (`pub use query::*` etc.), so every `storage::X` path the ~12
consuming files and all integration tests import resolves unchanged. The long functions moved
whole rather than shrinking — `search_documents` (560 lines) into `search.rs`, `apply_batch`
(376) and `get_or_create_index` (278) into `store.rs`.

Same privacy flattening as L11: items, `HybridStore` fields and the private methods called
across the two impl blocks went `pub(crate)` — the visibility the single file already relied
on — and `use crate::*` carries intra-crate names through the glob re-exports. `search.rs` is
`pub(crate) use`d because it holds no public items; everything it exports is on `HybridStore`
itself.

### L13 — `cli.rs`, `config.rs` and `cluster_coordinator.rs` (5,387 / 2,955 / 2,840 lines)

**Decomposition.** ✅ **Done** 2026-09-19. Three files, same coarse rule — grouped by feature,
tests out:

- `cli.rs` → `cli/`: `mod.rs` (881 — the clap grammar, `run_cli` dispatch, the list/output
  helpers, `pub(crate) use` globs), `ingest.rs` (2,309 — source/compression detection, the
  JSON chunk parsers, schema detection, the CSV/JSON loaders), `shell.rs` (1,660 —
  `InteractiveSession`, `IndexCompleter`, the interactive dispatch), `tests.rs` (383). The
  interactive help is generated now: a `usage` module holds one string per command, the help
  table is built from it, and every `Usage:` error in the dispatch quotes the same strings —
  the hand-written copy had already drifted (it said `search … [limit]` where the grammar is
  `[limit N]`, and the delete error spelled placeholders differently than the help).
- `config.rs` (1,610 — the model, loading, validation) + `config/overrides.rs` (419 — the
  `OVERRIDES` table, `CliOverrides`, unknown-key reporting, the moved-key adoption protocol,
  `cli_help`) + `config/tests.rs` (1,001). The stale module-doc TOML example now names real
  sections (`network.http`, `storage.data_paths`, `search.*`), and the misplaced
  `impl StorageConfig` follows its struct. The `default_*` functions stayed in `config.rs`
  as the `config_defaults!` macro — L19 had already collapsed them, and the serde
  `#[serde(default = "…")]` attributes resolve the names in the config module's scope.
- `cluster_coordinator.rs` → `cluster_coordinator/`: `mod.rs` (12 — decls and re-exports),
  `coordinator.rs` (2,376 — the actor and every handler), `messages.rs` (235 — the ~30 wire
  types, the contiguous block they always were), `tests.rs` (237).

Same privacy flattening as L11/L12: `pub(crate)` on the items, fields and methods the single
files already shared (`Override`'s fields, `CliOverrides`'s lookup methods, the coordinator's
`decide_route`/`rebuild_ring` and fields, the cli cross-file calls). Every `crate::cli::X`,
`config::X` and `crate::cluster_coordinator::X` path resolves through the glob re-exports.

### L14 — `OrchestratorError::Io` is the wire's catch-all, and semantics round-trip through strings

**Simplification.** ✅ **Done** 2026-09-18. The `io::Error` kind channel is gone; three
dedicated variants carry what `ErrorKind` was carrying, and the verdicts were assigned by
reading the sites rather than by the kind they happened to use:

- `Validation(String)` → `BadRequest` — the `InvalidInput`/`InvalidData` sites: a routing
  key the index needs, a document the schema refuses, an id the body did not carry, and
  the shard boundary's `FieldNotFound`/`QueryParser` mapping. `is_caller_error` and
  `verdict` keep their `Io`-kind arms for genuine `io::Error`s from the `#[from]` path.
- `NotReady(String)` → `Unavailable` — "No shards", the not-initialized writer
  channels, store handles and pools, and the absent local orchestrator. These answered
  `500` as `NotFound`-kind `io::Error`s, which reported the node at fault for still
  starting up; `503` is the same "not now, worth retrying" `PeerUnreachable` already
  carries.
- `Missing(String)` → `ServerFault`, preserved — "Local shard {id} not found", "No
  local stores", a routed shard no node owns. Internal inconsistencies, not the caller's
  absence: a `NotFound` verdict would have answered `404` for the node's own internals,
  which is why the sketch's `NotFound` variant name did not survive contact with the
  sites.

Four "remote orchestrator for node {} not found" sites moved to the existing
`PeerUnreachable` — its literal purpose, `500` → `503`.

The `RemoteError` impls shrank to mapping rather than re-derivation: `Validation` ↔
`InvalidInput`, `Missing` ↔ `NotFound`, `NotReady` → `Io` (the microshard wire has no
"not now" kind, and the far side reads a fault as it did before). Inbound,
`RemoteError::InvalidInput` rebuilds `Validation` instead of a kind-tagged `Io` — which
also fixes a hole where a peer's `InvalidData` flattened to `Io` on the way out and
arrived as a `500`. A genuine `io::Error` still keeps its kind through the same mapping.

### L15 — The schema-cache machinery exists three times

**Simplification.** ✅ **Done 2026-09-17.** `get`/`put`/`put_arc` had already become free
functions under CH10; they are now the methods of `SchemaCache`, the `ArcSwap` map behind a
newtype both `OrchestratorEngine` and `NodeOrchestrator` hold as `Arc<SchemaCache>` — so the
version-ordering rule and the delete-path `remove` have one home. "Load schema from the first
shard's store" is `schema_from_shards`/`schema_from_store`, one `spawn_blocking` + verdict
mapping for every reader, and the per-owner `load_schema`/`durable_schema` bodies are one-line
delegations to `SchemaCache::schema_for`/`durable`.

### L16 — `handle_broadcast` and `handle_broadcast_streaming` are one fan-out written twice

**Simplification.** ✅ **Done 2026-09-17.** The shared phase is `broadcast_fanout`: the
counter, `GetKnownPeers`, window widening (`widen_broadcast_op`), the per-peer timeout, the
concurrency cap, dispatch-ordinal tagging and the local+remote join return a
`BroadcastFanout` — the local result and every peer's `(node_id, answer-or-timeout)` in
dispatch order. The local future and the merge stay per-path: the non-streaming merge folds
each response through `push_hits` into `BroadcastStats`; the streaming merge keeps its
source-keyed blocks. Along the way: `StreamingSearchResult` is gone — the fan-out carries
typed results, and the streaming local block no longer drops hits that lack `_score` or
round-trips scores through `f32`.

### L17 — Dead code and stale suppressions

**Simplification.** ✅ **Done** 2026-09-17. Each named item resolved, and the sweep found more
of the same kind along the way:

- `StreamingSearchResult::Local.{shard_id, took_ms}` are gone — `shard_id` was always
  `Uuid::nil()`, `took_ms` was never read. The variant carries only `results`.
- The `new_docs` count threaded through the writer channel — split across callers with
  integer-remainder arithmetic, then discarded by both (`let (sequences, _new_docs)`) — is out
  of `WriteResultsChannel` and `MergedWriteReply`. `apply_batch_and_maybe_commit` still
  computes it; its ~20 test callers keep their signature.
- `GetWorkerStats` deleted outright — no callers, no handler.
- The blanket `#[allow(dead_code)]` / `cfg_attr(not(test), ...)` sites in the orchestrator
  were stripped from types the bulk move made live; the unmasking surfaced exactly one real
  corpse, `MergedWriteReply::Batch`'s op-count field, also gone.
- cli's dead `_extension` parameter went with the `source_extension` helper that existed only
  to feed it; the coordinator's `_inactive_nodes` was already cleaned.
- The broadcast hot path's per-request `info!` — the per-peer loop, the fan-out summary, the
  streaming banner, `try_remote` — is `debug!`.

### L18 — `cli.rs` says the same thing three ways

**Simplification.** ✅ **Done 2026-09-18.** One `JsonIngestPipeline` runs the single-pass
protocol — buffer the sample, name the id field, emit the schema, replay, then batch —
and reports readiness as `JsonIngestEvent`s; the HTTP loader awaits them inline, the
reader loader's blocking producer sends them down a channel to the same
`deliver_json_ingest_event` match. `id_field_rank` + `detect_id_field_index` are the one
ranking; `detect_id_field` and `detect_id_field_name` keep only their different
projections (CSV canonicalizes exact/hash matches to lowercase, JSON keeps the spelling).
`finalize_csv_schema` is the one finalizer, called from `detect_schema_from_csv` and the
loader's `csv_sample_schema`; `CsvIngest` holds the batch state both loader phases write
through. Three fixes fell out: the small-file CSV branch now gets the index-marking and
type hints the "same logic" comment always promised, the loader no longer prints "Schema
was missing" when the index already had one, and an empty HTTP source loads zero
documents instead of erroring — the reader path's existing leniency, unified.

### L19 — `config.rs` validates by repetition

**Simplification.** ✅ **Done 2026-09-18.** `validate` is five `validate_*` methods —
`security`, `mcp`, `network`, `storage`, `memory` — called in the same order the checks
always ran in, with their rationale comments moved verbatim (the checks are bespoke and
cross-field, so per-section split won over table-driving). `adopt_moved_settings` writes
the take/adopt/warn protocol once as `adopt_moved`, called four times. The 45 one-line
`default_*` functions are one `config_defaults!` macro invocation — each constant written
once, serde still naming the same functions. And `merge_configs` is deleted rather than
implemented: the file parse already layers over serde defaults, so "merge" was always
replacement — the comment that promised per-key merging was the false abstraction.

### L20 — The retrospective, and the sequence into the next cycle

✅ **Run 2026-09-19**, against the 52 commits since the 0.3.4 cut (`433f8b9`): 96 files,
+40,139/−31,629, of which the moves are the bulk. The first thing the retrospective has to
record is that it did not gate anything — L1–L19 were all delivered before it ran. L1–L4, L8
and L10 landed 2026-09-08; L11–L19, then L5–L7 and L9, landed 2026-09-17 → 09-19. The sequence
below was followed in substance and not in order: L17 went first, and L14, L18 and L19
(simplifications, step 4) landed before L12 and L13 (splits, step 3). Nothing broke because of
it, and the gate should be read for what it turned out to be — a plan that was correct enough
to execute without being consulted.

**What the stability-and-security posture bought.** Two things that are visible in the tree
rather than in the release notes. The first is that the defect class changed: L1–L4 were four
defects *adjacent* to 0.3.4's changes rather than in them, and each was the kind that no test
catches — a cache that evicts too much still answers correctly, an `Ok` carrying a number its
own comment calls wrong is still an `Ok`, a poisoned-mutex panic only appears after another
panic, and a redundant `unsafe impl` is indistinguishable from a load-bearing one until it is
deleted. Finding them needed a read, not a suite. The second is that the security remainder
(L5–L10) closed the surface the hardening phase had reached past: the MCP transport now bounds
what a session holds, refuses what it cannot serve, and masks what HTTP already masked. Both
groups were bounded, and both landed in a working week.

**What it cost in review surface.** The review's own framing — *every 0.3.3 and 0.3.4 fix had
to be made inside one of five files* — is no longer true, and the four fixes that landed after
the splits are the first evidence. [L6](#l6--a-get-mcp-listening-stream-without-a-session-is-unbounded)
touched one production file; [L5](#l5--legacy-sse-work-outlives-its-request-and-bypasses-the-guards)
touched two plus its config; [L7](#l7--mcp-tool-errors-leak-what-http-deliberately-masks)
spanned the `mcp` crate and `server/src/mcp/`, which is a crate boundary and the shape the
change actually has; [L9](#l9--a-writer-key-can-mint-indexes)'s gate touched its config, its
enforcement point and the orchestrator that consults it. None of them reopened a 13,000-line
file. That is four commits over two days, so it is a signal and not yet a result — the measure
is whether it survives a feature phase.

**Decision — the fault-injection feature stays test-only.** It is a cargo feature with nine
`cfg` sites across `main.rs`, `http_server/routes.rs` and `node/shard.rs`, off in every shipped
build, and a `+fault-injection` marker in `--version` that `panic_isolation.rs` asserts in both
directions: a seam build must declare itself, an ordinary build must not claim the marker.
Those two assertions are what make a test-only seam safe to keep, and they already exist.
Promoting it would put panic seams in a shipped binary for no stated caller; removing it would
delete the only thing that tests panic containment against a real built binary, which this
review called the strongest part of 0.3.4. Kept, unchanged, and recorded here so it is not
re-opened as an open question.

**Decision — CI is deliberately not the next step.** [L10](#l10--the-low-findings-in-one-place)'s
sixth finding is confirmed and stands: `.github/` holds six templates and no workflows, so
`cargo deny check` and `cargo audit` run only through `scripts/validate/deps.sh`, which `skip`s
both when the tools are not installed and still passes, and the `review-by` dates in
`deny.toml` are enforced only by that same manual script. The cost is now measured rather than
argued: RUSTSEC-2026-0285 (rustls, medium) was published 2026-09-14 and was found on 2026-09-19
by running the gate by hand, five days later. **The project stays on manual testing and
deployment through 0.3.5 regardless** — that is a deliberate choice about where effort goes
before 0.4.0, not an oversight, and automating the gate is deferred to the 0.4.0 cleanup where
it belongs beside the rest of the release machinery. What the choice obliges instead is a
standing rule: `scripts/validate/all.sh` runs at every release cut with `cargo-audit` and
`cargo-deny` installed, and a `skip` line in its output is a failed gate, not a pass.

**The measures, answered.**

1. **Four to six production files apiece — met, with one exception named.** `node/` is five
   (`orchestrator` 6,649, `shard` 2,085, `router` 1,671, `search` 1,373, `mod` 1,163);
   `storage/src/` is five (`store` 3,372, `schema` 1,689, `search` 1,615, `query` 973, `lib`
   463); `cli/` is three, `config` is two — both below the band, which the coarse rule permits.
   `cluster_coordinator/` is the exception: `coordinator.rs` is 2,376 of the original 2,840,
   so 84% of the file moved intact and what landed was a tests-and-wire-types extraction rather
   than a decomposition. Recorded as such rather than counted as an equal fifth.
2. **One architectural concern touches one file — holding, on four data points.** See the
   review-surface paragraph above. Re-check after the next feature phase, not before.
3. **CH2's two tracked sizes stop growing between releases — not yet answerable.** No release
   has been cut since the splits. Since they landed, `node/` is +41 lines and `storage/src/` is
   +0, which is two days of evidence and not a trend. The measure also needs its baseline
   restated now that both units are directories and the old single-file figures counted tests
   that are siblings today: the comparison at 0.3.5 is `node/` 16,131 and `storage/src/` 10,273,
   totals including tests.
4. **The 0.4.0 cleanup list — unmet, all four, and now re-dated.** None were done and none
   carried a date. They are `adopt_moved_settings` (`config.rs` 883), the `max_response_bytes`
   migration (`config.rs` 912), `RouteShard`'s stub (`cluster_coordinator/coordinator.rs` 2150,
   still logging its own deprecation) and the coordinator's shard-query TODO (`coordinator.rs`
   1564 — the ~1794 this list recorded was the pre-split line). All four are re-dated to the
   0.4.0 cut and are not 0.3.5 work; automating the dependency gate joins them there.

**Bookkeeping the retrospective had to correct.** The `Done` stamps in this group had drifted
badly enough to be worth naming, in a document whose dependency exceptions are enforced by
date. L7 read 2026-10-06, L17 read 2026-10-12, and L18 and L19 read 2026-10-26 — all dates in
the future when written. L11, L15 and L16 read 2026-09-09 against commits of 2026-09-17. All
seven are corrected to their commit dates above. L1–L4, L8 and L10 check out exactly; L12 and
L13 read 2026-09-19 against commits made late on 2026-09-18, a night's drift, left as
recorded. Separately, [F8](#f8--the-overload-gates-do-not-cover-the-bulk-write-path)
was headed 📋 while two of its three items were ✅ and measured; its marker is corrected to ◐.
(Corrected again on 2026-09-25, to ✅ — the third item had been done on 2026-09-17, two days
before this paragraph was written. See [M6](#m6--close-and-re-measure-the-bulk-lane).)

**Output — the goal set for 0.3.5.** The next release is a stability-and-performance patch, and
its target is a node that can be exposed on the internet as a shared, multi-tenant test
deployment: many keys, index-scoped, in one process. That target is what orders the list, and
it is narrower than "everything open". Full statement in
[M — The 0.3.5 goal set](#m-the-035-goal-set--multi-tenant-exposure--planned).

**Original entry.** 📋 Planned, and the gate on everything above. Once 0.3.4 is delivered and
settled, run the retrospective over the 0.3.3/0.3.4 cycle — what the stability-and-security
posture bought, what it cost in review surface (every fix inside one of five files), and what
the fault-injection feature should become (kept test-only, promoted, or removed — a decision,
not a default). Its output is this group's sequencing and the next cycle's goals:

1. **L1–L4 now**, small and independent — they are defects on today's code.
2. **L8 and the L10 bundle next**, same shape: small, bounded, security-adjacent.
3. **The moves in L11–L13** (tests and standalone types first, then the coarse feature groups
   each item names) *before* the next feature phase starts, so the next review does not read a
   13k-line diff context again.
4. **L14–L19 after the splits**, each with the per-module tests that the split creates.
5. **L5–L7 and L9 are done** — the decisions landed as bound, refuse, classify, and gate
   (`security.implicit_index_creation`, default on) respectively.

Measures of success for the group: the five tracked files each become a small set of
feature-grouped files (four to six production files apiece, not fifteen); a change of one
architectural concern touches one file; CH2's two tracked sizes stop growing between releases;
and the 0.4.0 cleanup list
(`adopt_moved_settings`, the `max_response_bytes` migration, `RouteShard`'s stub, the
coordinator's shard-query TODO at ~1794) is either done or re-dated.

---

## M. The 0.3.5 goal set — multi-tenant exposure 📋 Planned

Set by the [L20](#l20--the-retrospective-and-the-sequence-into-the-next-cycle) retrospective,
2026-09-19. **0.3.5 is a stability-and-performance patch whose target is one concrete
deployment: a node reachable from the internet, serving several tenants out of a single
process, used to try real workloads.** Every item below is here because that deployment is
unsafe or unmeasured without it; everything else open in this document is deliberately not
0.3.5 work.

**[M0](#m0--the-architecture-review-and-the-order-of-work) comes first and is not one of the
eight.** It is the 2026-09-19 architecture review — organization, dependencies and exposure,
then the CPU, memory and disk paths — and it carries the order of work for this whole group.
Three of its steps precede M1–M8 outright, and one of them re-scopes what M1 *is*.

**What isolation the design can and cannot give, stated before the list.** A key carries a role
and an `allowed_indexes` allow-list, and listings are filtered through the same predicate
(`authz.rs` `retain_indexes`), so one tenant cannot read, write or enumerate another's indexes.
That is isolation of *data and capability*, and it already works. What a single process cannot
give is isolation of *resources*: tenants share the read pool, the writer arenas, the admission
gates and one disk. 0.3.5's job is therefore not to pretend otherwise but to put a ceiling on
what any one key can consume, so that a noisy tenant is refused rather than absorbed. A tenant
who needs a guaranteed share needs their own node, and that should be said in the deployment
docs rather than engineered around.

**The patch-release constraint, from [C3](#c3--fail-closed-on-unauthenticated-internal)'s
lesson.** *A patch release must not stop a working deployment over a value nobody wrote.* Every
limit below therefore ships defaulted to its current behaviour — unlimited, or off — exactly as
`security.implicit_index_creation` shipped defaulted to `true`. An operator opts into the
ceiling; an upgrade changes nothing until they do.

### M0 — The architecture review, and the order of work

✅ **Reviewed 2026-09-19** — the crate graph, the module boundaries left by
[L11](#l11--node_orchestratorrs-is-13669-lines-and-holds-four-actors)–[L13](#l13--clirs-configrs-and-cluster_coordinatorrs-5387--2955--2840-lines),
and all four operation paths — single write, bulk write, the search fan-out, streaming ingest —
read end to end from the HTTP handler down through `storage` into redb and tantivy.

**Nothing below was measured.** The findings divide in two, and the division is load-bearing:
those where the code either does the thing or does not, stated as fact and quoted by function;
and those whose *size* depends on the workload, marked ⏱ and owed a run before any number is
claimed. [F7](#f7--the-request-timeout-sheds-the-client-not-the-work) and
[F8](#f8--the-overload-gates-do-not-cover-the-bulk-write-path) are why that line is drawn: this
node has twice behaved differently from how the reasoning predicted. Line numbers were read on
2026-09-19 and will drift; the named functions are the durable part.

**The verdict on the engine.** It is in good shape, and the bulk write path is the best of it —
`apply_batch` hoists every CPU pass out of the redb transaction, the writer loop reuses its
buffers across drains, the tantivy pass is id-deduped so a batch naming an id twice ends with
one document, and a drain is one transaction. None of what follows is a defect in an algorithm.
What the review found is a dimension: **several things scale with the number of distinct index
names a node has touched, and that is precisely the dimension
[M](#m-the-035-goal-set--multi-tenant-exposure--planned) grows.**

#### Organization, dependencies and exposure

- **O1 — the privacy flattening overshot, and it is the main cost of the split.**
  `node/orchestrator.rs` carries **234 `pub(crate)` markers over 144 distinct items**. What
  `node/` actually needs to export is **~18 names** — `ClientOp`, `NodeOrchestrator`,
  `OrchestratorError`, `OpClass`, `QueueLoad`, `SearchWindow`, `RouterActor`, `RemoteVerdict`,
  `ReadPoolHealth`, `DocPayload`, `order_hit_blocks` and a few more. Inside the single file
  none of the other ~126 was visible to anything; the split made them crate-visible in one
  sweep, and nothing now stops `http_server/` reaching into orchestrator internals. L11 and
  L12 both describe this as "the visibility the code already relied on", which is true of the
  handful that cross the boundary and false of the rest. `pub(super)` confines an item to
  `node/` and restores what one file gave for free.

  ✅ **Done 2026-09-19.** The estimate was close: **15** names, not ~18. The compiler
  enumerated them rather than a judgement call — scope the globs to `node`, flip every marker
  to `pub(super)`, build, and widen back exactly what breaks. `node/mod.rs` now carries the
  whole boundary as one named `pub(crate) use` list, and the globs the submodules reach each
  other through are `pub(in crate::node)`, so a new `pub(super)` item cannot escape by being
  swept up in one. `orchestrator.rs` went from **233 `pub(crate)` declarations to 20**, with
  184 now `pub(super)` and the rest gone to the two extractions; across `node/` the names
  reachable as `crate::node::*` went from **162 to 48**, and 33 of those 48 are declared in
  `mod.rs` itself, which is the boundary file. What still crosses from the submodules is 22
  top-level items — the 15 named, plus 7 that appear
  only as the *type* of something in the list (`OrchestratorWorkerTx`, `ShardPlacement`,
  `MailboxLane`, `MicroshardActor` and the three `/_admin/workers` report structs) — and 48
  methods or fields on them. Two widenings carry a written reason at the declaration rather
  than a marker on its own: `NodeOrchestrator::shards`, because `crate::admin::memory`
  implements the admin-memory messages for the actor from outside `node/`, and
  `QueueLoad::refuse`, because the HTTP front door has to refuse with the same error and the
  same counter. Those two are the honest residue of the review's "nothing stops `http_server/`
  reaching into orchestrator internals": now two things can, each named and each for a reason.
- **O2 — `orchestrator.rs` is five concerns, and one of them should leave.** The dispatch core
  (`OrchestratorEngine`, `NodeOrchestrator`, the worker loop, the `Message` impls, ~3,900
  lines) is what L11 defends and it is right to. Beside it sit the admission and load
  accounting (`OpClass`, `WorkerCounters`, `ServiceHistogram`, `DispatchCounters`, `QueueLoad`,
  `MailboxLane`, `MailboxSlot` and the stats structs, ~700 lines), the schema machinery
  (`SchemaCache`, sampling, evolution, `staged_schema_validation`, `parallel_validate_schema`,
  ~600 lines) and placement and pinning (`CoreLayout`, `ShardPlacement`, `WriterPin`, ~220).
  **Admission is the one to extract first**: it is the whole F7/F8 machinery, it carries
  invariants of its own, and F8's worst bug — the lane counter decremented after the `await`,
  so a cancelled request leaked its slot and an *idle* node read `mailbox_depth 63` and refused
  everything for good — was an invariant-locality failure. Those invariants want one file.
  This is not L11's growth trigger firing (`orchestrator.rs` is 6,649 against the 6,643 it
  landed at, so it has not grown); it is the coherence question, which answers differently.

  ✅ **Done 2026-09-19.** `node/admission.rs`, 783 lines: `OpClass`, `WorkerCounters`, the
  `SERVICE_*` constants, `ServiceHistogram`, `DispatchCounters`, `QueueLoad`, `MailboxLane`,
  `MailboxSlot` and the four `/_admin/workers` report structs. `orchestrator.rs` is **5,782
  lines**, down from 6,649. The module doc states the invariant that had no home — every
  increment matched by exactly one decrement on every exit path, cancellation included — and
  names F8 as what it cost to learn, so the next reader meets the rule beside the counters
  rather than in a retrospective. The schema machinery and the placement types stay where they
  are: they are the next candidates, not this cut.
- **O3 — the split scattered one family.** Five routing-key functions sit together around
  `orchestrator.rs` 1192–1360; `derive_routing_key_from_doc` sits at ~6493, five thousand
  lines from its siblings. [CH11](#ch11--routing-key-derivation-is-written-four-times-with-two-algorithms)
  consolidated the *algorithms*; the move re-scattered the *family*.

  ✅ **Done 2026-09-19.** `node/routing.rs`, 172 lines, holds the whole ladder — a module
  rather than an adjacency, so the next split moves the file instead of scattering the members
  again. Two defects surfaced in the move: `derive_routing_key_from_doc`'s doc claimed a rung
  it does not implement ("if the document has an `id` field, use that directly" — that is the
  ladder's rung 3, one level up), and it declared its own prefix bound `pub(crate)` inside a
  function body, where the marker means nothing. Both corrected; the doc now says what the
  function does and why a truncated prefix costs placement skew and never correctness.
- **O4 — `storage`'s glob re-exports are an ungated public API.** `lib.rs` does
  `pub use query::*`, `pub use schema::*`, `pub use store::*` — 74 public items, and a new
  `pub` in any of those modules silently widens the crate's public API with no review step.
  (`search.rs` is correctly `pub(crate) use`, as L12 intended.) Named re-export lists make each
  widening deliberate; the glob makes it invisible.

  ✅ **Done 2026-09-25.** `lib.rs` names what it exports: `FieldReference` and `field_references`
  from `query`; `FieldDef`, `IndexSchema`, `SchemaFieldUpdate`, `SchemaFields`, `SchemaState`,
  `TantivyFieldType`, `WalOp` and six functions from `schema`; `HybridStore` from `store`. The list
  is every name another crate, or storage's own integration tests and examples, actually uses,
  plus the two types that appear in public signatures (`FieldReference`, returned by
  `field_references`; `SchemaFields`, returned by `get_or_create_index`). The three modules stay
  `pub(crate) use …::*` for the crate's own wiring — an explicit import outranks a glob, so the two
  coexist — which makes the lists the whole public surface. Of the 20 `pub` items the globs had
  been exporting, the four nothing outside used were narrowed to `pub(crate)`: the two description
  limits, `reconstruct_shadow_fields_owned` and `OpenIndex`. `SchemaFields` was narrowed too and
  put back when the compiler flagged it as private in `get_or_create_index`'s public signature —
  which is the review step the glob never offered.
- **O5 — the `server → client` edge is deliberate and should stay recorded as such.** The
  binary is one artifact: `main.rs` dispatches to `client::run_cli()`, so the server links the
  whole CLI — clap, the interactive shell, the CSV and compression paths. It costs binary size
  and link surface and nothing on a request path. Recorded here so the edge is not mistaken for
  a layering accident by the next reader.

#### Memory — the index dimension is unbounded in eleven places

- **M0-a — the eviction unit is wrong, and this rewrites [M1](#m1--bound-resident-memory-against-index-count).**
  `store.rs` declares **eleven per-index maps**, each keyed by index name and each unbounded:
  `writers`, `readers`, `current_seq`, `operations_counter`, `read_cache`, `budget_cache`,
  `schema_cache`, `fields_cache`, `index_init_locks`, `warmed_generations`, `warmup_states`,
  plus `index_size_cache` under a `Mutex`. [E5](#e5--a-cap-on-open-index-writers) caps one of
  them. Evicting a writer leaves the other ten resident — `readers` holds an `IndexReader` with
  its segment readers and fast-field caches, `read_cache` holds document bodies. **Cap the
  index, not the writer.**

  **Ten as of 2026-09-19**: `read_cache` is gone, deleted rather than capped — see **M0-k**.
  It was one of the two large ones, so the extent M1 has to bound is smaller than this entry
  describes, and the large one that remains is `readers`.
- **M0-b — threads scale with open indexes too, and E5 does not say so.** Each `IndexWriter` is
  built with `indexer_num_threads` (default 1) plus `merge_num_threads` (default 2): **three OS
  threads per open index**, on top of the arena. The thread-topology essay in `node/mod.rs`
  states this correctly and E5 does not. Two hundred tenant indexes is ~600 threads and
  ≥12.8 GiB of arenas before a document is served.
- **M0-c — the read cache is bounded per index and evicts arbitrarily.**
  `MAX_CACHE_ENTRIES_PER_INDEX` is 1024 in `search.rs::insert_into_cache`, so the real ceiling
  is 1024 × index count. Eviction takes `entries.keys().next()` — arbitrary `HashMap` order,
  neither LRU nor FIFO — so under pressure the hot set can go while cold entries stay.

  ✅ **Done 2026-09-19.** CLOCK, with the reference bit an `AtomicBool` inside the cached body
  so a *hit* can set it through the shared `DashMap` guard `get_from_cache` already takes —
  recording a hit any other way would make every read acquire the entry exclusively, which
  costs more than the policy is worth. A body starts its first round **unreferenced**, which is
  the part worth writing down: marking a freshly cached body as referenced is defensible, and
  would leave the cache defenceless against a scan, since every body a sequential pass touched
  would arrive protected and the hand would evict whatever the scan had not reached yet. A body
  now has to be asked for a second time to earn its second chance. The ring carries names an
  invalidation has emptied rather than paying an O(ring) scan per invalidated id on the writer
  thread; the sweep drops them as it reaches them and compacts in bulk if they outnumber the
  live ones. The test fills the cache twice over with cold traffic while re-reading one key;
  under the old policy it fails on every run, at a different insert each time, which is the
  arbitrariness stated as evidence.

  ⚠️ **Superseded the same day, and the work deleted with it.** **M0-k** removed the cache
  outright, which takes the CLOCK policy, its test and the fixed defect with it. The finding was
  correct — the eviction *was* arbitrary — and fixing it was the wrong move, because it
  answered "which body should go" without first asking whether the cache should exist.
  Recorded rather than quietly dropped: one of the four items in step 7's "cheap and certain
  set" turned out to be maintenance on something that should not have been there, and a review
  that ranks fixes should be read as also asking, of each one, whether the thing being fixed
  earns its place.
- **M0-d — a deep clone per bulk batch that an `Arc` already covers.**
  `parallel_validate_schema` takes `&IndexSchema` and clones it whole for the rayon path, while
  its caller holds the `Arc<IndexSchema>` that `load_schema` returned. Taking the `Arc` removes
  a field-map clone from every batch above 64 documents. The comment defending the clone as
  "bounded by field count" is true and beside the point.

  ✅ **Done 2026-09-19**, and wider than the entry proposed. `parallel_validate_schema` takes
  `&Arc<IndexSchema>` and bumps a refcount where it deep-copied a field map. Following it up
  the call chain, `staged_schema_validation` now takes `&mut Arc<IndexSchema>` and mutates
  through `Arc::make_mut`, which removed two more clones neither this entry nor the review had
  named: the actor-path single write did `(*schema).clone()` unconditionally before asking
  whether anything needed changing, and the actor-path bulk write did `Arc::unwrap_or_clone`.
  Both are copy-on-write now, so a write that turns out not to evolve the schema — the large
  majority — pays nothing at all for the possibility. Two more fell out at the other end: both
  call sites wrote the result back with `SchemaCache::put`, which deep-copies its argument to
  build an `Arc`, where `put_arc` was already there for exactly the caller that has one. Five
  schema clones removed from the write path, from an entry that named one.
- **M0-e — [CH6](#ch6--the-federated-merge-clones-every-hit) is worse than its own entry says.**
  `mcp/search.rs` deep-clones every hit to stamp `_index_source` on the copy, out of a response
  it already owns. On the MCP surface the hits *are* the documents, so it doubles peak memory of
  every federated response. `as_array_mut` + `mem::take` is the whole fix. Re-read CH6's
  "untidy rather than slow" against that.

  ✅ **Done 2026-09-19.** Exactly that: `get_mut("hits")`, `mem::take`, stamp in place. The
  response is owned by the merge loop and dropped at the end of the iteration, so the hits move
  out of it for the price of a pointer swap.

#### CPU — the search path repeats per-request work once per shard

- **M0-f ⏱ — query preparation is shard-independent and runs per shard.**
  `prepare_query_parser` runs the whitespace fold, the shadow rewrite, the date and facet
  normalisations, then the prefix pass, then rebuilds `default_query_fields` by walking every
  indexed field and checking its tantivy `FieldType`, then constructs a `QueryParser` — and
  every shard does all of it against the same query and the same schema. The gather loop
  already knows: its own comment reads *"Every shard parses the same query string"*, and it
  fans out `query.to_string()` per shard regardless. Only the final `parse_query_lenient` needs
  the per-shard index. The normalisations are schema-only work that belongs once in
  `ScatterCtx::gather`; `default_query_fields` belongs in `SchemaFields`, which is already
  cached per index. **The waste grows with the shard count added to buy parallelism**, which is
  what makes it worth doing — but how much of a search it is depends on query shape and field
  count, so it is owed an arm before any figure is claimed.

  ✅ **Measured 2026-09-19 — and the premise does not hold, so this is declined.**
  `cargo run -p storage --release --example query_prep_cost`: 500 documents of *fixed* width,
  2 000 searches per cell, schema width the only variable, best of three.

  | indexed fields | `alpha`, top 10 | `f0:alpha`, top 10 | `f0:alpha`, count only |
  |---|---|---|---|
  | 5 | 20.0 µs | 14.9 µs | 2.5 µs |
  | 25 | 37.8 µs | 12.0 µs | 2.9 µs |
  | 100 | 90.4 µs | 14.6 µs | 5.1 µs |
  | 200 | 160.0 µs (**8.0×**) | 17.2 µs (**1.15×**) | 8.1 µs (**3.2×**) |

  The field-qualified columns are the measurement, because a qualified query never touches the
  default-field set and so separates preparation from execution. The count-only column is the
  sharp instrument: no documents are fetched, so it is preparation, parsing and counting and
  little else. Preparation does scale with schema width — about **30 ns per indexed field**, so
  some 5.6 µs at two hundred fields — and that is the whole of what hoisting could recover, per
  extra shard. Against a search that costs 17 µs qualified and 160 µs unqualified at that width,
  and against paying for it by moving schema-dependent rewriting to a level where shard schemas
  are documented as legitimately divergent, it is not worth it.

  **Two candidate changes were implemented, measured and reverted**, which is the point of the
  ⏱ marker and the reason this entry is longer than a "done" would be.

  - *Make the date and facet passes read the query for field names instead of scanning the whole
    schema.* Genuine algorithmic improvement — O(query) rather than O(schema width), and it
    would have made the three rewrite passes agree on what counts as naming a field. Measured
    **identical**: 19.0 µs against 18.8 µs at two hundred fields. Reverted.
  - *Cache `default_query_fields` on `SchemaFields`*, which this entry proposed by name. Safe —
    a Tantivy schema is fixed when its index is created, so the derivation cannot go stale — and
    it does measure, at 20 000 searches per cell where noise is small enough to see it:
    **5.53 µs against 5.77 µs**, tight both sides. That is 0.24 µs at two hundred fields, about
    1 ns per field, and roughly 7% of the field-dependent cost; the other 93% is
    `QueryParser::for_index` and the parse themselves being handed two hundred default fields,
    which no cache reaches. A new invariant across two construction sites for 0.24 µs is the
    same trade [M0-h](#m0--the-architecture-review-and-the-order-of-work) refused, so it was
    refused here too. Reverted.

  **And the harness was wrong first.** Its first version declared wide schemas *and* wrote wide
  documents, so a top-10 search deserialised ten documents of N values each and the run reported
  a 10× "preparation" slope that was nothing of the kind. A second version plumbed a `limit`
  argument that never reached the call sites, so the count column silently measured the same
  thing as the column beside it. Both are recorded because this entry is the one that claims a
  negative, and a negative is only as good as the instrument: the harness now fixes document
  width, and takes `SEARCHES` and `FIELDS` from the environment so a single cell can be run with
  ten times the samples.

- **M0-k — the document read cache was a third layer over two that already work, and is
  deleted.** Raised on review of the measurements above: redb has its own page cache, sized by
  `calculate_cache_size` at 32 MB per shard on the floor and tiered up by database size, and the
  operating system's page cache sits under that. `read_cache` held 1024 document bodies per index
  on top of both, mirroring rows of `data_<index>`. Two details decided it. `get_from_cache`
  returned `bytes.clone()` — the same memcpy redb's `to_vec` performs from a page it already
  holds — so the only work it removed was one B-tree descent. And `get_batch_by_keys`, the path
  a search takes to fetch its hits, opened the redb read transaction *and* the table before
  consulting the cache, so a total hit still paid for the transaction the cache existed to avoid.

  Measured with `cargo run -p storage --release --example read_cache_value`, 20 000 documents,
  50 000 reads per workload, three runs each side:

  | workload | with the cache | without |
  |---|---|---|
  | hot — 100 keys, every read hits | 0.11 µs | 0.45–0.50 µs |
  | uniform — all 20 000 keys, nothing hits twice | 0.73 µs | **0.47–0.49 µs** |
  | search, top 10 | 86.5–87.9 µs | 88.5–90.8 µs |

  The uniform row is the finding: a scanning read got **35% faster** by deleting the cache,
  because every one of them was paying a miss, an insert and an eviction. Against that, a hot
  key set loses 0.34 µs per read — under 1% of an HTTP request — and a search loses ~2.5 µs
  of 88. The underlying read with no cache of ours is 0.47 µs, which is redb and the OS doing
  their job.

  **The performance is not the main argument.** `read_cache` is why `cache_generation` existed:
  a protocol whose whole purpose was to stop the cache serving a body a write had superseded,
  guarding a race the code documented at length. Deleting the cache deletes that surface —
  `get_by_key` reads redb and is right by construction. It also removes one of the two large
  per-index maps **M0-a** counts, and moots **M0-c**. The rule this settles on, and the one
  worth carrying into M1: *cache our own objects and derived metadata — schemas, field maps,
  budgets, counters, handles — and do not cache what redb and tantivy are already caching.* By
  that rule `read_cache` was the only offender of the thirteen maps in `store.rs`.

  One methodological note, because it changes a number reported earlier in the day. The first
  A/B was run through a `NO_READ_CACHE` environment switch checked inside `get_from_cache`, and
  `std::env::var` on every read is not free: it put the "without" side at 0.60–0.63 µs rather
  than 0.47. The figures above are from the real removal. An instrument in the hot path is part
  of the measurement.
- **M0-j — a wide schema makes an *unqualified* query expensive, and nothing bounds that.**
  Found while measuring M0-f, and the one real result of that run. Every indexed text field is a
  default search field, so a bare term is expanded into a disjunction across all of them: at two
  hundred fields `alpha` costs **160.0 µs against 20.0 µs at five** — 8.0×, close to linear
  in field count. That is genuine execution, not preparation — the posting lists are really read
  — so it cannot be hoisted or cached away; the query is simply doing what it was asked to do.
  It matters here because schema width is the *tenant's* choice while the cost lands on the
  node, and [M](#m-the-035-goal-set--multi-tenant-exposure--planned) is about putting a
  multi-tenant node on the internet. It belongs with
  [M8](#m8--re-decide-the-query-complexity-caps), whose subject is exactly what a single query
  may cost, and it gives that decision the number it did not have. ⏱ for the *fan-out* multiple:
  this is one shard, and a broadcast search multiplies it.

#### Disk

- **M0-g — every commit stats the whole index directory.** `commit_index` calls
  `get_optimal_memory_budget`, which calls `index_size_bytes` — a `read_dir` plus a
  `metadata()` per file. A fifty-segment tantivy index is some three hundred files, so that is
  ~300 `stat` syscalls per commit, on the writer thread, in the window between the tantivy
  commit and the checkpoint transaction. It feeds a commit-*cadence* heuristic that needs no
  per-commit precision: recompute on a TTL or every N commits. The cheapest item in this
  review.

  ✅ **Done 2026-09-19.** `commit_index` no longer measures at all; `should_commit_writer` is
  the one place that does, behind a 30-second TTL carried on the cache entry itself rather than
  in a twelfth per-index map. Cheapest, and it also removed a *second* measuring path — the
  fallback inside `should_commit_writer` — leaving one. The regression guard is the timestamp:
  it is stamped when the budget is measured, so an unchanged one after three commits is proof
  no walk happened.

  **Measured 2026-09-19, and it does not show — which is the honest result.** `cameodb-bench`
  bulk mode, 1 shard, a commit every 100 operations, 500-document batches, alternating against
  `0e2c1b2`: 12 922 / 12 883 docs/s baseline against 13 013 / 12 885 on this build, p99 within
  noise of each other. The reason is measurable too: after that run the index directory holds
  **65 files**, not the ~300 this entry's fifty-segment figure assumes, so the walk removed some
  66 syscalls from a request that spends ~150 ms indexing 500 documents. The syscalls are gone —
  that part is mechanical and the timestamp test proves it — but the saving is invisible at this
  index size and grows with segment count, so no throughput claim is made for it.
- **M0-h — dead code that documents behaviour the engine does not have, and the decision it
  needs.** `StorageConfig::get_bulk_operation_budget` is called by nothing in production —
  its only caller is `crates/storage/tests/bulk_memory_budget_test.rs`. Its doc describes bulk
  writes receiving 1.5× and 2× arenas; they do not, because the writer's
  `memory_budget_per_thread` is fixed when the writer is built. Being `pub` on a library type,
  no dead-code lint ever saw it, which is how [L17](#l17--dead-code-and-stale-suppressions)'s
  sweep missed it. **This is a decision, and both branches are cheap:** either *activate* it —
  wire the batch-size scaling into the bulk path so the documented behaviour becomes real, and
  keep the test as its proof — or *delete* it together with `bulk_memory_budget_test.rs`, since
  a test whose only subject is an uncalled function pins nothing the product does. What it must
  not stay is what it is: a documented, tested claim about an engine that behaves otherwise.

  ✅ **Decided and done 2026-09-19 — deleted.** Activating it means making a writer's arena
  depend on batch size, and `memory_budget_per_thread` is fixed when the writer is built, so
  the only honest implementation rebuilds the writer per batch — which discards what it has
  buffered and forces a commit. That is not the cheap branch the entry assumed, and the
  direction is wrong besides: [M1](#m1--bound-resident-memory-against-index-count) is about
  *bounding* writer arenas against index count, and this would inflate them 2× on the bulk
  path, unmeasured. Deleted with its test. `bulk_memory_budget_test.rs` was **not** deleted
  whole, as the entry proposed: two of its three tests cover `get_optimal_memory_budget`, which
  is live and which M0-g has just made more load-bearing, so the file is renamed
  `memory_budget_test.rs` and keeps them. Its header now records what was removed and why, so
  the deletion is discoverable from the place someone would look for the behaviour.

#### The differentiator, and where it actually costs

Schema evolution and stream-correctness detection are the differentiation and are not up for
trade. Having traced them: **the validation is not the cost.** It is already tiered — inline at
or below 64 documents, rayon above, one validator so a document gets the same verdict at any
batch size, which is the disagreement L-group work already closed. The cost is structural:

- **M0-i — every evolving write pays two transactions and two fsyncs.** In `apply_write`, the
  data transaction commits, and *then* `persist_schema_evolution` opens a second `begin_write`
  with `Durability::Immediate` and commits again. The code knows the seam it leaves: the error
  arm logs `CRITICAL: Schema evolution failed after data commit. Data was saved but schema may
  be inconsistent.` So on the workload that *is* the differentiation — a stream teaching an
  index its own shape — every field-introducing write costs two fsyncs and carries an
  acknowledged inconsistency window. **Folding the schema row into the same redb transaction as
  the WAL and data inserts closes both at once**: one fsync instead of two, and redb makes the
  pair atomic so the window cannot exist. `apply_batch` already proves the pattern — it opens
  one transaction for everything that has to be atomic. This makes the differentiating feature
  cheaper *and* stronger, which is the rare direction and the reason it leads the list.

  ✅ **Done 2026-09-19.** The schema row is written into the document's own transaction, its
  bytes serialised outside it like `doc_bytes` already were. `persist_schema_evolution` stays
  for `update_field_indexing`, which is a metadata-only change with no document to ride along
  with, and its doc now says so. One consequence had to be decided rather than inherited: a
  schema row was always `Durability::Immediate`, and the data transaction is `Immediate` only
  when `wal_sync` is on, so the folded transaction commits durably whenever it carries a
  schema — which is the *same* single fsync the separate schema transaction was already paying,
  now covering the document as well rather than in addition to it.

  A third defect turned up in the fold, unnamed by the review. The evolved schema was written
  into `schema_cache` **before** the transaction opened, optimistically, and again after
  `persist_schema_evolution` returned. So any failure between those two points left the cache
  holding a field the store had never been told about — and one such failure is ordinary, not
  exotic: a document that introduces a new field *and* carries a bad value for an indexed one
  evolves the schema and then fails while its Tantivy document is built. The cache moves only
  after the commit now. That is what the new
  `crates/storage/tests/schema_evolution_atomicity_test.rs` pins, and it fails against the old
  ordering with the cache reporting `["note", "id", "payload"]` against a store that has two.
  What no in-process test can show is the atomicity itself — that needs a crash between two
  commits, and there is now no between.

  **Measured 2026-09-19.** `cargo run -p storage --release --example evolving_write_cost`, 400
  writes per cell, three rounds alternating between this build and `0e2c1b2`, on one developer
  machine (macOS, so every pinning path is a no-op) — relative figures, not an SLA.

  | 400 writes, `wal_sync=true` | baseline | this build |
  |---|---|---|
  | every write introduces a field | 2817 ms | **1503 ms** |
  | no write introduces a field | 1304 ms | 1304 ms |
  | ratio, evolving ÷ plain | **2.16** | **1.15** |

  **1.88× on the shipped default**, and the ratio is the clearer statement of it: an evolving
  write cost twice a plain one because it paid two fsyncs, and now costs roughly what a plain
  one costs. The plain column is identical across builds, which is what says the gain is on the
  evolving path rather than a general shift in the machine. With `wal_sync=false`, where both
  builds pay exactly one fsync, the gain is 1.08× — the second transaction's own overhead, and
  the right size for what is left after the fsync is not the difference.

  The harness is an example rather than a test: it asserts nothing and takes tens of seconds, so
  it does not belong in a gate, but without it this number could not be taken again. It exists
  because `cameodb-bench` declares its schema up front, deliberately, and so never exercises the
  write this item is about.

#### The order of work

Ranked by impact × certainty ÷ effort. Items 1–3 and 7 are mechanically verifiable — the code
either does the thing or it does not. Item 6 is ⏱ and is owed a run.

| # | Step | Lands in |
|---|---|---|
| 1 | ✅ Fold schema evolution into the data transaction (**M0-i**) — done 2026-09-19 | this group |
| 2 | Re-scope the cap to the *index* — all eleven maps — and record the three-threads-per-index fact (**M0-a**, **M0-b**) | [M1](#m1--bound-resident-memory-against-index-count) |
| 3 | ✅ Stop the per-commit directory walk (**M0-g**) — done 2026-09-19 | this group |
| 4 | ✅ `pub(crate)` → `pub(super)` across `node/`, and one named boundary list in `node/mod.rs` (**O1**) — done 2026-09-19 | this group |
| 5 | ✅ Extract the admission subsystem from `orchestrator.rs` (**O2** ✅); re-unite the routing-key family (**O3** ✅); replace `storage`'s glob re-exports with named lists (**O4** ✅) | this group |
| 6 | ✅ ⏱ Hoist shard-independent query preparation out of the fan-out (**M0-f**) — measured 2026-09-19 and **declined**; the measurement instead found **M0-j** | this group, beside [M6](#m6--close-and-re-measure-the-bulk-lane) |
| 7 | ✅ The cheap and certain set: the `Arc` in `parallel_validate_schema`, CH6's per-hit clone, the read cache's arbitrary eviction, and the delete-or-activate decision on `get_bulk_operation_budget` and its test (**M0-d**, **M0-e**, **M0-c**, **M0-h**) — done 2026-09-19 | this group, [CH6](#ch6--the-federated-merge-clones-every-hit) |

Steps 1–3 come before [M1](#m1--bound-resident-memory-against-index-count)–[M8](#m8--re-decide-the-query-complexity-caps) start, because 2 changes what M1 is and 1 and 3 touch the
paths M6 will measure. Steps 4 and 5 come before the next feature phase, for
[L20](#l20--the-retrospective-and-the-sequence-into-the-next-cycle)'s reason: a review should
not have to read around an avoidable surface twice.

**Where this stands.** All seven steps are closed, on 2026-09-19: 4 and 5 first (see below),
then 1, 3 and 7 in their ranked order, then 6. Step 2 was a re-scoping of
[M1](#m1--bound-resident-memory-against-index-count)'s own entry rather than code, and is
recorded there. Step 6 closed by being **declined on its measurement** rather than done, which
is what ⏱ is for; it left **M0-j** behind, for
[M8](#m8--re-decide-the-query-complexity-caps). **O4**, the last of step 5, closed on
2026-09-25, and with it this group. Together the finished steps removed two fsyncs' worth of
work and a window from every evolving write, ~300 `stat` syscalls from every commit, three
schema deep-copies from the write path, a deep copy of every hit from every federated MCP
response, and a documented claim the engine never implemented; they added four regression tests,
three of which fail against the code they replaced. Step 6 removed nothing, and that is its
result: it was declined on its own measurement.

**Steps 4 and 5 ran first, on 2026-09-19**, out of the ranked order and deliberately: they are
the only steps that change where the other five are *read*, and every one of them lands in
`node/` or in files the boundary now fences. Doing them after would have meant writing the M0-a
through M0-i work against a module layout that was about to move under it. O4 is what is left of
step 5; it is in `storage`, touches no `node/` path, and can ride with whichever storage-side
step reaches it first. `cargo clippy --workspace --all-targets` is clean and
**Validated 2026-09-19.** `scripts/validate/all.sh` against the release binary: deps 7, unit 1,
posture 46, auth 114, tls 9, remote-sources 4, artifact 8 — **7 suites, 189 checks, 0 failed,
0 skipped**, which is the standard
[L20](#l20--the-retrospective-and-the-sequence-into-the-next-cycle) set. A node-level A/B
against `0e2c1b2` on the mixed workload found no regression and no gain (375 → 373 writes/s,
19 008 → 18 652 searches/s, p99 34.0 → 35.0 ms and 600 → 597 µs) — expected, because the
harness declares its schema and so never evolves one. The change that *is* visible is recorded
under M0-i.

`scripts/validate/unit.sh` reports 829 tests across 44 targets, 0 skipped. Steps 4 and 5 are
behaviour-preserving by construction — visibility narrowing and code motion, no logic edited —
and the suite is the evidence, not the argument; steps 1, 3 and 7 do change behaviour, and each
carries a test that fails against what it replaced, except M0-e, whose change is a move of a
value this code already owns.

### M1 — Bound resident memory against index count

✅ **Done 2026-09-20**, and it was the blocker for the deployment rather than one item among
several.

`limits.max_open_indexes` bounds how many indexes a node holds open at once. Past it, the least
recently used index is committed and closed; its data is untouched and the next reference
reopens it. Unset, it is derived from `limits.total_memory_limit_mb` divided by the smallest
writer arena and clamped to `[8, 256]` — 32 at the defaults — for the reason
`effective_max_body_size_mb` is derived rather than chosen: an operator who has said how much
memory the node may use has already said most of it, and a second number to keep in step with
the first is a number that drifts.

**The unit is the index.** `close_index` commits the writer and drops all ten maps keyed by that
name; `delete_index_data` now calls the same cache-drop rather than carrying its own copy of
the list. Evicting the writer alone — which is what [E5](#e5--a-cap-on-open-index-writers)
asked for and what the admin endpoint still does — would leave `readers` resident, and that is
the largest of the ten now that the document cache is gone.

**The cap is a count, not a byte budget, and that is **M0-b** cashed in.** An open index costs
an arena *and* `indexer_num_threads + merge_num_threads` OS threads. Megabytes bound the first
and leave the second to grow with however many names the workload touches, which on a node
whose tenants choose their own names is not a number this process picks. A count bounds both,
and the posture line prints what a given cap implies in each currency.

**Enforced, not guaranteed — deliberately.** A victim whose writer mutex or init lock is not
free is passed over rather than waited for. Waiting would mean blocking an opener on someone
else's commit and, in the case that actually matters, on a lock the calling thread already
holds, which is a deadlock rather than a delay. When nothing can be closed the index is
admitted over the cap and the overshoot is logged: exceeding a cap is recoverable and the next
admission tries again, while a deadlocked writer thread is not. Skipping an index that is being
*opened* closes the other hole — `get_or_create_index` admits before the writer exists, so
closing that name would drop caches its opener is about to repopulate and leave it open but
uncounted, a hole in the very count the cap is of.

**"Recently used" means used, not opened.** `get_by_key` reads redb directly and opens nothing,
and the first version of the LRU therefore did not count it — so an index answering key lookups
all day read as the coldest thing on the shard. `the_coldest_index_is_the_one_evicted` is what
caught it; a search-only tenant would have hit the same thing through a different door.

**A behaviour change, and worth saying plainly:** every release before this one held indexes
open without limit. A node with more live indexes than the derived cap will now evict and
reopen, which costs a reopen on the next reference and nothing else. An operator who wants the
old behaviour sets a number past their index count and owns the arithmetic; there is no setting
for "unbounded", because unbounded here is what this release exists to stop.

Three tests, each failing against the uncapped build: the open set stays within its cap across
forty names, an evicted index still has its documents, and the index that goes is the coldest.
`/_admin/memory` reports the open count and cap per shard as well as summed — a node evicting
hard on one shard while another idles has a routing problem, not a capacity one, and the sum is
what hides that.

**The original finding.** This is
[E5](#e5--a-cap-on-open-index-writers), promoted: `writers` is a `DashMap` that grows with the
number of distinct index names written to, every entry holds a live Tantivy `IndexWriter` with
its own arena (`indexer_memory_budget`, default 64 MiB, scaled further by the optimal-budget
calculation), and nothing evicts by count or by total budget. E5 already names the exact pattern
this release is built for — *"a tenant-per-index or date-partitioned pattern reaches an
uncomfortable footprint quickly"* — which stops being a hypothetical the moment tenants pick
their own index names.

**Re-scoped 2026-09-19 by [M0](#m0--the-architecture-review-and-the-order-of-work), and the
change is not a detail — it is what this item is.** Capping `writers` caps one of *eleven*
per-index maps (**M0-a**). The other ten stay resident behind an evicted writer, and two of
them are the large ones: `readers` holds an `IndexReader` with its segment readers and
fast-field caches, `read_cache` holds document bodies at 1024 entries *per index* (**M0-c**).
**The unit of eviction is the index, not the writer** — one closing path that drops the whole
per-index set, with the admin endpoint's drop-and-rebuild as its inner step rather than its
whole extent. Two consequences to write into the design before it starts:

- **The budget being capped is threads as well as bytes (M0-b).** Every open `IndexWriter`
  carries `indexer_num_threads` (default 1) plus `merge_num_threads` (default 2) — three OS
  threads per open index, which the thread-topology essay in `node/mod.rs` states and E5 does
  not. A cap expressed only in megabytes leaves the thread count uncapped, and at two hundred
  tenant indexes that is the ~600 threads before the ~12.8 GiB.
- **`read_cache` is gone, so this item has one fewer extent to bound (M0-k, 2026-09-19).** It
  held 1024 document bodies per index — one of the two large maps, and 1024 × index count of
  resident memory that this item would otherwise have had to cap. It was deleted rather than
  capped, because it was a third caching layer over redb's page cache and the operating
  system's, and measured net-negative on any access pattern that is not a small hot set. The
  large map that remains is `readers`, which holds a tantivy `IndexReader` with its segment
  readers and fast-field caches, and that one is not duplicating anything: it *is* the object.

### M2 — Cap decompressed bytes on the streaming ingest path

✅ **Done 2026-09-20.** The cap is in, and getting there turned up two bugs the finding had
not suspected.

The cap itself is what [C5](#c5--a-decompression-cap-on-the-streaming-ingest-path) asked for.
`write_stream_handler` takes a raw `Body`, which no extractor limit reaches, so it counts each
chunk against `AppState::max_body_size_bytes` and stops the moment the total passes it. The
refusal is a 413 carrying the same summary the success path returns, with `"status":
"refused"` — because a stream cut mid-body leaves documents written, and whether those are in
the index is the one thing the caller cannot work out for itself.

**The threat was unreachable, for a reason worth recording.** The first run of the new test
sent a gzip body and got back 400 *"expected value at line 1 column 1"*: the NDJSON parser had
been handed raw gzip. `routes.rs` used `tower_http::decompression::DecompressionLayer`, whose
own documentation opens *"Decompresses **response** bodies of the underlying service"*. The
request-side layer is `RequestDecompressionLayer` — a different type in the same module, one
word apart. No request body was ever inflated, for any codec, and both comments in the layer
stack said otherwise: *"Allow compressed requests — decompresses before the body limit above"*
and *"a compression bomb is measured expanded, not compressed"*. Two confident comments stood
in for the one test that would have caught it. Had the cap gone in alone, it would have been
unreachable code asserted by a test passing for the wrong reason — the shape
[M0-h](#m0--the-architecture-review-and-the-order-of-work) had just finished deleting.

**The layer was not merely absent; it was costing.** It sat *outside* `CompressionLayer`, and
it fills in `accept-encoding: br` on any request lacking one. So a response to a client that
sent no `accept-encoding` was brotli-compressed by the inner layer and brotli-*de*compressed by
the outer one before it left the process. `CompressionLayer` has been configured and
unreachable since it went in: no client ever received a compressed body, and every response
paid to produce one. Measured both ways against the old stack and the new before it was
believed, and pinned by `a_response_is_compressed_only_when_the_client_asks`.

With the swap to `RequestDecompressionLayer` making compressed ingest real, the codec set
became a decision rather than an inheritance: `decompression-gzip` and `decompression-deflate`
join the `decompression-br` that was already there, because brotli alone is not what an ingest
client sends. An encoding outside that set now gets 415 with an `accept-encoding` list, where
before it got a 400 complaining about the caller's documents.

Four tests, one claim each:
`a_write_stream_refuses_a_body_that_inflates_past_the_limit` (8 MB of NDJSON, a few kB on the
wire, against a 1 MB ceiling — with the cap disabled it answers 200 having written all 20,000
documents, which is C5's threat made visible), `a_write_stream_takes_a_deflate_body`,
`an_unsupported_content_encoding_is_refused_as_unsupported`, and
`a_response_is_compressed_only_when_the_client_asks`.

### M3 — Meter the write surface, and give anonymous callers their own bucket

✅ **Done 2026-09-20.** The remainder of
[C8](#c8--rest-has-no-rate-limit-and-anonymous-mcp-callers-share-one-bucket), which closes with
it. Both halves, and a third thing the first half turned out to need.

**The write surface is metered in documents, not requests.** All five routes —
`write_handler`, `delete_document_handler`, `bulk_write_handler`, `bulk_delete_handler`,
`write_stream_handler` — now charge before they do any of the work. The unit is the point: a
`_bulk` body may carry thousands of documents, and charging it the single token a search costs
would have left the expensive direction — the one that also grows the disk — bounded by nothing
but `max_concurrent_requests`, which is a bound on instantaneous concurrency and not on rate.
So the charge is what the request asks the node to index or remove: one for a single write or
delete, the array length for the two bulk routes, and one charge per micro-batch for the NDJSON
stream. A delete costs what a write costs, because it is a document through the writer, a commit
and a merge.

**A second setting rather than a second use of the first.** `[security.limits]
write_documents_per_minute` and `write_burst`, both `0` — off — by default, and deliberately
*not* falling back to `tool_calls_per_minute`. An operator who set that chose a number for tool
calls; charging their bulk imports against it on upgrade would be a release that broke ingest
for everyone who had taken the earlier advice. The two meters are separate budgets end to end,
which `a_spent_write_allowance_does_not_refuse_a_search` pins.

**The streaming route is the one that cannot charge up front**, because how many documents the
body holds is not knowable until it has been read. It charges a micro-batch at a time and, when
the allowance runs out, stops and answers `429` carrying the same summary the decompressed-size
refusal from [M2](#m2--cap-decompressed-bytes-on-the-streaming-ingest-path) gives — `items_written`,
`lines_received`, `batches`, plus `retry_after_secs`. A bare 429 would leave the caller unable
to tell which half of its file is in the index, which on a partially-consumed stream is the one
thing it cannot work out for itself.

**Anonymous callers get their own bucket, and the map stays bounded.** Where there is no
`key_id` the caller's address is the subject: an IPv4 address, or an IPv6 **/64**, since a
single host is routinely handed a whole one and metering per address would let one machine mint
2^64 buckets — the unbounded-map problem that keying by `key_id` exists to avoid. An IPv4-mapped
address folds back to its IPv4 form, so a dual-stack listener does not hand one caller two
allowances. Past 4096 groups per meter, buckets that have refilled to capacity are dropped: a
full bucket and an absent one admit exactly the same next request, so nothing is given away.
If every tracked group is still spending, further addresses share the old single bucket — which
is to say the worst case under an address flood is exactly the behaviour this node had before,
and no worse. A key always outranks the address it connected from; two keys behind one NAT are
two tenants.

**What the first half needed.** The subject is decided once, in `authorize`, where the key and
the socket are both in hand, and travels to handlers as a `Caller` extension and to the MCP
dispatcher inside the identity handle — `McpAuthz` gained a defaulted `peer_addr()`, since
`/mcp` is one JSON-RPC path and everything below it sees only what the gate attached. The peer
address is now read on every request rather than only when the audit trail is on; it is one
extension lookup either way. `ToolRateLimiter` became `RateLimiter` because it no longer meters
only tools, and a 429 now carries `Retry-After` — the limiter always knows its number, and the
bucket's whole contract is that obeying it works, which is worth nothing to a caller never told
the number in a form a client library reads.

**`check-config` reports it.** A new `rate` rule names which meters are set, and warns on any
non-`local` profile when either is open — including the case M3 itself is about, reads metered
and writes not, which it calls the wrong half.

Nineteen tests. Thirteen in `ratelimit.rs` for the buckets (separation of the two meters, the
per-document charge, per-address metering, the /64, the mapped address, the bound on the map,
and that a full bucket is the only thing forgotten); three in `authz.rs` for the wire, which is
what a bucket test structurally cannot see — an address that never reaches the limiter leaves
every unit test passing; and six end-to-end in `tests/write_rate_limit.rs` against the real
binary, for the status codes and the `Retry-After` header an SDK is built to hide. All four
enforcement tests were run against the code with the charge removed and fail there; the two
that assert the surface stays *open* — unmetered by default, and unmetered by a tool rate alone
— pass either way, which is what they are for.

### M4 — Per-key resource quotas

✅ **Done 2026-09-25.** Built per *tenant* rather than per key, which is what the heading's
"single tenant" meant: a key carries `tenant = "acme"`, keys naming one tenant share its quota,
and `[security.tenants.acme]` sets `max_indexes` and `max_bytes`. Both `0`, unlimited, by
default, and a tenant with no entry has no ceiling — an upgrade changes nothing until an
operator writes one. Document count was dropped from the minimum set: bytes bound the disk,
count bounds resident memory, and a document cap bounds neither better than those two do.

**Ownership is a stamp on the schema, written once at the mint.** `IndexSchema.tenant` is set
where the index is created and nowhere else — not refreshed on later writes, so a tenant cannot
shed usage by having another key write once. Two holes were found wiring it and closed before
enforcement landed: a body-supplied `tenant` on a write or `PUT /_config` (now overwritten from
the key at the handler), and an admin re-declaration silently unstamping an index and handing
its owner their quota back (now preserved from durable state unless the prior record is a
deletion, in which case the declaring key owns what it creates).

**`max_indexes` is exact, at both mints.** The implicit mint in `staged_schema_validation` and
the explicit one in `orch_create_config` both run on the orchestrator mailbox, so a count taken
there from durable schemas cannot race another mint. Counted fresh per mint rather than cached —
mints are rare, and a running total is the thing that drifts. Clearing an index's data keeps its
schema and so its slot; `?delete_schema=true` frees it. The integration test caught that
distinction, and the test was wrong, not the code.

**`max_bytes` is checked against a reading, and the overshoot is stated.** A per-write exact
figure is a directory walk per index, the cost `commit_index` had removed. Instead
`TenantQuotas` holds a reading — the listing's `total_size_bytes` per index, summed under its
owner — refreshed off the write path, single-flight, when a write finds it older than 10s. A
stale reading still decides, so no write waits on a measurement; a refresh that fails or hangs
(30s timeout) keeps the previous reading and clears its flag, so it cannot wedge the table. The
cost is up to one interval of ingest past the ceiling, and that is in CONFIGURATION.md, not
glossed. Bytes are charged to the index's owner, not the writer: an admin writing into a tenant's
index at its ceiling is refused too. Checked on all four write paths (engine and actor, single
and bulk); for a write that mints, the minting tenant is the owner, so a fresh index is no way
round the ceiling. Per node: in a cluster each node checks its own share.

**Refused `403`, with its own verdict.** `OrchestratorError::QuotaExceeded` →
`RemoteVerdict::QuotaExceeded` (wire tag `quota-exceeded`), so a peer's refusal of a forwarded
write stays a refusal rather than reading as a `500` that invites a retry that cannot succeed.
Not `400` (nothing is malformed) and not `503` (retrying unchanged will not help). MCP maps it to
a caller error.

Tests: eight unit tests on the decisions (no entry is no ceiling, `0` is unlimited per ceiling,
refused *at* the cap and only for that tenant, unowned writes never refused, no reading allows,
byte ceiling per tenant, a stale reading decides then is replaced, owner follows stamp or mint),
the verdict surviving the wire, and two integration tests against the real binary — the
index ceiling on both mint paths with another tenant, the operator and an emptied index checked
alongside, and the byte ceiling refusing within one refresh interval, including through a fresh
index and an admin key.

### M5 — Per-index capability subtraction

✅ **Done 2026-09-25.** [C1](#c1--per-index-role-overrides) promoted, and its own entry already
named the reason: *risk if unfixed: multi-tenant isolation*.

`index_overrides = { audit = "reader" }` on a key stanza: on the named index the key holds that
role instead of its own. Written as a role rather than a capability list because a key's
authority has to stay legible at a glance, and because an override that can only name an
existing role cannot invent an authority the vocabulary does not have.

**Subtraction is enforced at load, not assumed.** An override holding a capability the key's own
role does not is refused at startup, naming the capabilities it would have added — the failure
worth preventing is an operator writing an override believing it restricts a key and it quietly
granting instead. Silently clamping was the other candidate and is worse: it honours a config
nobody wrote. An override naming an index outside `allowed_indexes` is refused too, because it
reads as protection and provides none.

**Both surfaces, which was the part that could have been a fiction.** The HTTP gate checks at
`decide`, after scope and separately from the role check, so *"your role cannot do this anywhere"*
and *"your role cannot do this here"* stay distinguishable to the caller and in the log. `/mcp`
could not reuse that: a single JSON-RPC path cannot be classified from the outside, so the
dispatcher checks capability before any argument is decoded and cannot know which index is in
play. `McpAuthz` therefore gained `has_on(capability, index)` — defaulted to the
index-independent answer so a host without the notion is unaffected — and `check_index`, which
every index-naming tool already passes through, now asks it. Enforcing on REST alone would have
left the surface where an agent does the writing unguarded.

Seven tests: the subtraction withholding write on one index and nowhere else, read surviving it,
the refusal naming role/index/capability, escalation refused at load, an out-of-scope override
refused at load, scope refused before the subtraction is consulted, and the MCP identity
honouring it. Verified live: `403 this key is restricted to role 'reader' on index 'audit',
which does not hold the 'write' capability`, with writes elsewhere still `200`.

### M6 — Close and re-measure the bulk lane

✅ **Done 2026-09-25 — the exit criterion is met; see session 3 below. Confirmed on the cut
binary 2026-09-27, session 4, with OB22 in it.** ◐ until then: measurement
only, and the first session found a blocker. Corrected 2026-09-25.
This entry was written on
2026-09-19 claiming [F8](#f8--the-overload-gates-do-not-cover-the-bulk-write-path) item 3 as the
last of its three still open. It was already closed: `0836df2` landed it on 2026-09-17, two days
after F8 was written and two days before this entry said otherwise. Bulk ops are worker-eligible,
carry their own `OpClass`, and fold into the blend the door judges on — see
[F8 item 3](#f8--the-overload-gates-do-not-cover-the-bulk-write-path) for what shipped.

**F8's marker has now been wrong twice, in the same direction, for the same reason.**
[L20](#l20--the-retrospective-and-the-sequence-into-the-next-cycle) moved it 📋 → ◐ on
2026-09-19 because two of its three items were already done; this correction moves it ◐ → ✅
because the third was too, on a day that fell between those two events. The pattern is specific
and worth naming: an item is written up from a measurement, the fix lands in a later session
under a different heading, and nobody walks back to the entry that predicted it. Reading the code
before scheduling the work is [M7](#m7--redact-the-cluster-psk-in-debug)'s lesson, and it cost
twelve days there against eight here.

**So no code is owed and the measurement is the whole item** — which was always the larger half.
Three arms, one session, one harness:

- **The bulk lane**, which is the item's name. Every table in F8 was taken against a binary where
  a bulk write held the actor mailbox for its entire fan-out. `0836df2` replaced that design and
  nothing has re-measured it, so the exit criterion — goodput that degrades rather than
  collapsing — has no evidence either way.
- **The single-write lane**, which F8's caveats flag and no arm has ever covered: `Write` *is*
  worker-eligible, so it should be covered, and that is an assumption until an arm says so.
  Whether a retrying client deepens any of it is the other half of the same caveat.
- **The read lane** — **M0-f rides the same session**: shard-independent query preparation is
  repeated per shard, and its size is owed an arm rather than an estimate.

**The harness is ready.** [F2](#f2--an-open-loop-load-generator)'s `cameodb-bench` takes
`--mode bulk|write|search`, `--rate` and `--rate-steps`, offers Poisson arrivals from a seed, and
judges itself first — reporting a run `INVALID as a statement about the node` rather than
presenting a number it cannot stand behind.

**Expect two runs, not one.** [M4](#m4--per-key-resource-quotas) and
[M5](#m5--per-index-capability-subtraction) add refusal paths on the write ingress and will move
the overload curve by design, and the exit criterion is a statement about the shipped binary. This
run is the baseline that attributes the curve to `0836df2`; a shorter confirmation arm belongs at
the cut.

The F7/F8 precedent is the standard here: this lane's behaviour has twice been worse than the
reasoning predicted, and only a run has ever settled it.

**Session 1, 2026-09-25 — the bulk lane, and it did not reach an open-loop arm.** Three closed-loop
arms on the F8 protocol. Two results, one expected and one not.

*Expected, and it closes the question F8 left open.* `dispatch.round_robin_sends` is **non-zero on
every bulk arm** — 269, 217, 559 — against the **0** F8 measured, which was the tell that no bulk
work reached the worker pool at all. Bulk is served on the pool, `OpClass::Bulk` is carrying its
samples, and `0836df2` does what its message says. The third time this lane has been measured is
the first time the gates can see it.

*Not expected.* The capacity probe wedged the node. Root-caused and fixed the same day as
[OB14](#ob14--a-timed-out-request-never-leaves-the-worker-pool-and-the-node-degrades-until-it-is-restarted):
a `DashMap` self-deadlock in `should_commit_writer` parked every shard's writer thread once its
budget entry passed a 30s TTL, and each write that then timed out left a worker slot behind. With
it fixed the same arm pair runs clean at 26,000 docs/s, four arms back to back hold `gap 0` and
`green`, and `SIGTERM` stops the node again.

**The exit criterion — goodput that degrades rather than collapses — is still unmeasured**, and
saying otherwise from these arms would be the mistake this entry was just corrected for: they are
closed-loop, and the criterion is an open-loop statement. What changed is that the lane now stays
up long enough to ask. The three arms, from a wiped volume, in one session, are still owed.

**Session 2, 2026-09-25 — the scaling sweep.** Closed-loop, release profile (LTO, stripped),
M5 Pro, 15 cores, harness co-located, one index per node, each configuration from a wiped volume.
Single runs: read the shapes, not the third digit.

*Shard count, at a load point that actually saturates (concurrency 64, batch 2000):*

| shards | bulk docs/s | CPU cores of 15 | pool gap |
|---|---|---|---|
| 1 | 122,045 | 1.0 | 0 |
| 2 | 163,185 | 1.4 | 0 |
| **4** | **172,120** | **1.7** | 0 |
| 8 | 109,904 | 1.4 | 0 |

Sharding pays to about four and then reverses: 1→2 is +34%, 2→4 is +5%, 4→8 is **−36%**. A bulk
request fans out to every shard and waits for the slowest, so each added shard buys parallelism
and pays a tail-latency tax on every request; past four the tax wins. **The default of 4 is the
right default**, which is worth knowing rather than assuming.

*Load shape, at 4 shards — far the larger lever, and batch size is most of it:*

| concurrency | batch | in-flight docs | docs/s | p50 | cores |
|---|---|---|---|---|---|
| 8 | 500 | 4k | 15,117 | 267ms | 0.6 |
| 16 | 500 | 8k | 25,342 | 324ms | 0.8 |
| 64 | 2,000 | 128k | 157,514 | 725ms | 1.7 |
| 128 | 2,000 | 256k | 147,830 | 1,629ms | 2.0 |
| 32 | 5,000 | 160k | 185,388 | 787ms | 2.3 |
| 32 | 10,000 | 320k | 265,052 | 1,014ms | 3.1 |
| **8** | **50,000** | **400k** | **320,997** | **1,055ms** | 3.3 |
| **32** | **20,000** | **640k** | **324,435** | 1,520ms | 3.8 |
| 16 | 50,000 | 800k | 251,655 | 2,111ms | 3.1 |
| 32 | 50,000 | 1.6M | 233,579 | 4,921ms | 2.9 |

**28,261 → 324,435 docs/s, 11.5×, with no code change.** Batch size is the dominant term and
keeps paying to 20,000; concurrency is the smaller one and turns against you early. The two are
not independent — what the node responds to is roughly **documents in flight**, the product of
the two — and the plateau sits near 400,000 of them at about **320,000 docs/s**. Past that,
offering more buys latency and nothing else: 1.6M in flight is *slower* than 400k and five times
the p50.

**Where the two reversals matter for an operator.** At batch 500 more concurrency helps and the
knee is 64. At batch 20,000 concurrency 32 already beats 64, and at batch 50,000 concurrency 8
beats both — 320,997 docs/s at a p50 of 1,055ms against 233,579 at 4,921ms for four times the
concurrency. **The best operating point is few large batches, not many small ones**, and an
importer tuned the other way pays for it twice, in throughput and in tail latency.

*What is not the limiter, tested rather than assumed* (4 shards, concurrency 16, batch 500):

| variant | docs/s | cores |
|---|---|---|
| baseline, `wal_sync = true` | 28,261 | 0.8 |
| `wal_sync = false` | **22,006** | 0.6 |
| `indexer_num_threads = 2` | 27,219 | 0.8 |
| `indexer_num_threads = 4` | 27,384 | 0.8 |

Durability is not the constraint — turning fsync off made it *slower* — and indexer threads do
nothing. **CPU peaked at 3.9 of 15 cores, 26%, at the 324,000 docs/s plateau**, and sat at 0.6–0.8
for every configuration below it. The ceiling is not the box.

**The pool stayed honest throughout.** `round_robin_sends` minus `jobs_completed` read **0** after
every one of the twenty-eight arms above, across eleven-fold swings in offered load — which is
[OB14](#ob14--a-timed-out-request-never-leaves-the-worker-pool-and-the-node-degrades-until-it-is-restarted)'s
fix holding under far more load than the run that found it.

**The diagnosis, and it is the same structure as OB14 seen from the other side.** High latency at
low concurrency (267ms for a 500-document batch at concurrency 8), throughput that rises with
offered load, CPU flat: the write path is **latency- and serialization-bound, not CPU-bound**.
One writer thread per shard, a oneshot round-trip per slice, coalescing in between — that is what
caps it, and it is the same machinery a single parked task was able to take down. The remaining
~9× of this box is behind that, and it is 0.4.0-shaped work rather than a patch.

**A correction worth keeping.** The first pass of this sweep ran at concurrency 16 / batch 500 and
concluded that *one shard beats four*. That was an artefact of offering too little load: at a
point that saturates, four beats one by 41%. A scaling claim taken below the knee measures the
harness, which is [F2](#f2--an-open-loop-load-generator)'s own lesson arriving by a different road.

**Not measured here.** Search throughput — the read arms ran after the write arms, so each
configuration searched an index of a different size and the numbers are not comparable across
rows. Open-loop behaviour, which is the exit criterion and is still owed.

**The three weaknesses OB14 exposed are now closed too** — ✅ 2026-09-25, in the same session.
They were not closed by OB14's fix and did not need to be; each is a way for *any* stuck
operation to take the pool down, and a node should survive one rather than depend on there never
being one.

- **The pool's accounting is RAII.** `PoolSlot` holds `in_flight`, `outstanding`, the completion
  tally and the semaphore permit, and releases all four in `Drop` — so they are released however
  a job ends, rather than by statements at the tail of a task that may never reach it. A job that
  leaves without finishing increments the new `jobs_dropped` counter, reported on
  `/_admin/workers`: the point is that the next leak of this kind is *audible*, which this one
  was not until the node stopped serving. `jobs_completed` and `jobs_dropped` are about the
  *work*; a capped job still answers its caller with an error, so nobody waits on a channel that
  will never be written.
- **F7's dequeue check is reachable under saturation.** The loop now receives first, checks the
  deadline *before* waiting for a permit, and checks it again once one is in hand — the wait for
  capacity is itself spent budget. An already-dead job is refused without occupying a slot a live
  one could use.
- **A worker task is bounded.** `WORKER_LIVENESS_MULTIPLE` (twenty budgets, ceiling five minutes)
  reclaims the slot of an operation that has stopped being slow and started being stuck. It is a
  liveness backstop and not a deadline — F7 deliberately lets admitted work finish, and a cap
  tight enough to act as a deadline would shed work about to succeed. Firing it logs at `error`
  and counts as dropped, because it is a defect report rather than a shedding path. No budget
  configured still means the pre-F7 contract, uncapped.

**Both tests were checked against the code they pin, not merely written.**
`an_operation_that_never_returns_costs_one_slot_not_the_pool` fails without the cap — *"a parked
job was never answered, its slot is still held"* — and `a_stale_job_is_shed_even_when_every_permit_is_held`
fails against the old ordering. The second needed correcting first: an earlier draft passed
against the old ordering because the liveness cap fired inside its window and shed the job for the
wrong reason. Its budget is now long enough that only the ordering can explain the result, which
is the difference between a test and a decoration.

**Measured end to end on the F8 overload configuration** — 1s timeout, `max_concurrent_requests`
3000, bulk at concurrency 256:

| | before | after |
|---|---|---|
| goodput under overload | **0 ok/s** | **89 ok/s** |
| refused at admission | 0 | **42,312** |
| `abandoned` — F7's shed | **0, unreachable** | **5, the guard fires** |
| `jobs_dropped` | — | **0** |
| `in_flight` at rest after | **pinned at the width** | **0** |
| `/_cluster/health` under load | `408` at 1,001ms | **`200` at 1.4ms, green** |

Goodput degrades instead of collapsing, the door refuses cheaply instead of absorbing, and the
node answers its own health probe throughout. **This is the exit criterion's shape but not its
evidence**: these arms are closed-loop, and the criterion is an open-loop statement. The three
arms are still owed.

*One reading note for whoever runs them.* `round_robin_sends` minus `jobs_completed` is a leak
indicator only while nothing is being shed — a job refused at dequeue is sent and never completed,
by design, so once `abandoned` is non-zero the difference equals it. The gauges that mean
"something was lost" are `in_flight`, `queue_depth` and `jobs_dropped`, and all three read zero
above.

**Capacity is not comparable to F8's tables and should not be read against them.** The probe
measured 11 bulk req/s at concurrency 4 against F8's 56, but the binary, the batch size, the seed
and the machine all differ, and the probe was itself degrading as it ran. It is a number for
choosing open-loop rates, not a regression.

**Session 3, 2026-09-25 — the three open-loop arms, and the exit criterion.** From a wiped volume
per arm, in one session: F8 protocol (4 shards, `search_threads = 2`,
`max_concurrent_requests = 3000`, `request_timeout_secs = 1`, `wal_sync = true`), 200,000
documents seeded, Poisson arrivals, 20s steps, release build, harness co-located. Every arm ran
on `c54ae33` and on `079ad0b` — the build session 2 measured, before M5, M4, M8 and O4 — back to
back, so drift lands on both. Harness lag p99 stayed under 1.5ms and no arm dropped an arrival.

| lane | offered | `c54ae33` ok/s | `079ad0b` ok/s | refused, `c54ae33` |
|---|---|---|---|---|
| bulk, batch 500 | 60/s | 59, sustained | 59 | — |
| | 120/s | **105** (52,419 docs/s) | 104 | 275 × 503, **0 × 408** |
| | 300/s | **116** (57,754 docs/s) | 108 | 3,724 × 503, **0 × 408** |
| single write | 2,000/s | 1,987, sustained | 1,987 | — |
| | 4,000/s | 1,116 | 1,290 | 46,455 × 503, **10,218 × 408** |
| | 8,000/s | 951 | 1,045 | 132,520 × 503, 8,325 × 408 |
| read | 1,000 / 2,000 / 4,000/s | **836 / 852 / 847** | 876 / 876 / 860 | 503 only, 0 × 408 |

*Bulk and read meet the criterion outright.* Goodput is flat from 2× to 5× overload with the
excess refused as `503` — F8 measured **0 ok/s and 100% `408`** at the same bulk rates — health
answered `200` throughout, and an overload-then-relief ramp (300 → 15 bulk/s, 4,000 → 300
searches/s) served the full offered rate from the first second of relief, where F7 measured
4–12s of zero. `jobs_dropped` read 0 on every arm of both binaries; the pool's `gap` equals
`abandoned` exactly, which is the dequeue check shedding, not a leak.

*The single-write lane did not*, on either binary: at 2× it kept about half its capacity, 15% of
requests timed out, and health failed. That was
[OB15](#ob15--every-refused-request-was-an-error-line-and-under-write-overload-the-logging-cost-half-the-goodput)
— one `ERROR` line per refusal, written synchronously on the runtime the write path runs on —
found, isolated and fixed in this session. On the fixed binary the lane holds **2,621 ok/s at
2× and 2,541 at 4×**, zero `408`, health ≤ 518ms, full rate from the first second of relief; bulk
and read did not move.

*M4, M5, M8 and O4 cost nothing measurable.* Every pair is inside run-to-run noise, closed-loop
probes included (bulk 27 req/s on both, writes 643 against 645/s). Reads read ~3% lower on
`c54ae33` in this session and higher than both on the fixed binary (884/872/871) in the next, so
that difference is noise and not M8's query preparation.

**The criterion is met** on the binary that carries the OB15 fix: goodput that degrades rather
than collapsing, on the bulk lane and the single-write lane, with the read lane holding too. What
remains is the short confirmation arm at the cut, which is release mechanics rather than this
item.

**Session 4, 2026-09-27 — the confirmation run, and OB22 costs a single node nothing.** The
question this time was narrower than the criterion: [OB22](#ob22--an-orchestrator-waited-on-peers-while-holding-its-mailbox)
rebuilt how the orchestrator answers — deferred answers, a peer lane, bulk always handled
locally, health counting indexes off the request path — and was measured only on a cluster.
So an A/B on one standalone node: `a05e5bd` (the last commit before OB22) against `9c0fd9a`, both
release builds, M5 Pro, harness co-located, each binary from a wiped volume, two rounds with the
order reversed in the second so drift lands on both. The two arms that looked borderline after
two rounds were given four more.

*Closed loop, F5 protocol* (defaults, 4 shards, `wal_sync`, 5,000 seeded, 5 s + 20 s per arm):

| arm | `a05e5bd` | `9c0fd9a` | Δ |
|---|---|---|---|
| search c8 / c16, ok/s | 22,353 / 31,010 | 22,454 / 30,774 | +0.5% / −0.8% |
| write c8 / c16, ok/s | 376 / 602 | 377 / 609 | +0.3% / +1.2% |
| mixed c8, search / write ok/s | 19,573 / 360 | 19,582 / 360 | 0% |
| mixed c16, search ok/s, n = 6 | 22,128 | 21,803 | −1.5% |
| bulk c4 / c16, batch 500, docs/s | 48,492 / 131,367 | 47,963 / 129,764 | −1.1% / −1.2% |
| bulk c64 × batch 2,000, docs/s, n = 6 | 303,707 | 301,648 | −0.7% |

After two rounds the bulk peak read −3.9% and mixed-c16 search −2.5%; neither survived the extra
rounds. The bulk peak's ranges overlap (293k–314k against 298k–310k) and mixed-c16's paired
differences run from −4.8% to +3.1% — while the box itself drifted ~10% between rounds, on both
binaries alike.

*Open loop, the session 3 protocol*, with the bulk lane pushed further because its capacity has
moved (below):

| lane | offered/s | `a05e5bd` ok/s | `9c0fd9a` ok/s | refused |
|---|---|---|---|---|
| bulk, batch 500 | 60 / 120 / 300 | 59 / 118 / 300 | 59 / 119 / 302 | ≤ 29 × 503 |
| | 600 | 540 (270k docs/s) | 545 (272k docs/s) | 9–11% × 503 |
| | 1,500 | 536 | 536 | 64% × 503, 27–46 transport, **0 × 408** |
| single write | 2,000 / 4,000 / 8,000 | 1,987 / 2,630 / 2,487 | 1,987 / 2,634 / 2,437 | 503 only, **0 × 408** |
| read, 200k docs | 1,000 / 2,000 / 4,000 | 730 / 722 / 711 | 717 / 707 / 703 | 503 only, **0 × 408** |
| relief: bulk / write / read | 1,500 → 60, 8,000 → 300, 4,000 → 300 | full rate from the first second | the same | none after the drop |

Goodput is flat from just over capacity to about 3× it on the bulk and single-write lanes, and
to about 5× on reads, the excess refused as `503`. After every arm on both binaries
`jobs_dropped` and `in_flight` read 0, and all 8,250 health probes answered `200`. **The criterion still holds on the binary
being cut, and nothing in OB22 moved a single node's numbers outside run-to-run spread.**

*Two absolute shifts since session 3, on both binaries, so not OB22's and not isolated here.*
The bulk lane went from 116 ok/s at 300 offered to ~540 sustained — nearly 5× — which is most
likely [F9](#f9--commit-on-a-clock-not-a-count): session 3's commit by count committed on nearly
every batch of a bulk load. Reads went the other way, 703–730 against 836–884; that one is
unexplained and wants its own A/B before it is called anything.

*Three findings, all on both binaries. The first two are fixed — ✅ 2026-09-27, below — and the
third is a sizing note:*

- **A health probe's `GetIdentity` ask runs past its budget under write overload** — half of a 1 s
  request timeout, so health p99 sits at ~505 ms and every such probe logs an `ERROR` line.
  `9c0fd9a` logged half as many (1,241 against 2,377 over the session), because OB22's index
  counts took the `ListIndexes` ask off the probe; `GetIdentity` is the other half. The node's
  identity does not change while it runs, so the ask is avoidable, and a line per probe is
  [OB15](#ob15--every-refused-request-was-an-error-line-and-under-write-overload-the-logging-cost-half-the-goodput)'s
  shape on a smaller scale. **Fixed:** health reads the identity from state set at startup and
  the shard count from the lock-free shard placement — every shard enters the shard map and the
  placement's live set in one step and none leaves, which a test now pins. Nothing on the probe
  asks the orchestrator any more. Under bulk overload health p99 fell from 502 ms to 0.9 ms and
  the node's `ERROR` lines from 116 to none.
- **A search whose every shard was abandoned for budget answers `500 Internal Server Error`** —
  "no shard could run this query … spent 1000ms of a 1000ms budget before a worker could start
  it". That is F7's shed, and a shed should be the `503` the rest of the node answers; `500`
  tells a client not to retry what it should retry. **Fixed:** a gather whose every shard was
  shed answers with the shed itself, and a shed shard no longer outvotes the shards that refused
  a query as the caller's mistake. The test fails against the old code with exactly the measured
  shape. On the large-index read arm: 49 `500`s and 405 `ERROR` lines per run before, none
  after, with health p99 at 0.8 ms against 506 ms.
- **Reads on a large, still-merging index collapse under a 1 s timeout.** A first pass ran the
  read arm after the bulk and write arms, against ~5M documents, and took 7–31 ok/s with ~1,000
  `408`s per step on both binaries. That arm was discarded as a comparison against session 3 —
  which read a freshly seeded 200k — but the regime is real: at `search_threads = 2`, searches
  over that index cost close to the whole budget.

### M7 — Redact the cluster PSK in `Debug`

✅ **Done 2026-09-19 — by discovering it had been done on 2026-09-07.**
[C6](#c6--redact-the-cluster-psk-in-debug) carries the detail. Every route the key could take
out of the process was already closed: redacted `Debug` on both the config and the resolved key,
`skip_serializing` on the field, a fingerprint in the swarm's log, a scrubbing `Drop`, and a
refusal that reports a malformed key's length and nothing else.

One gap was real and is now closed. `psk_is_not_printable_or_serializable` pinned the `Debug` and
serialization routes, but nothing pinned the refusal — `load_psk`'s error message is the one
place a malformed secret reaches an operator's log, and a comment claiming it is safe is not a
test. `a_refused_psk_does_not_appear_in_its_own_error` covers both arms of that check, a value of
the wrong length and one of the right length that is not hex, and it fails when the message is
made to echo the value.

**Cost of the item as planned: zero. Cost of not checking first: the twelve days C6 spent
marked planned.** Worth remembering before the next "cheap and unchanged" item is picked up —
read the code before scheduling the work.

### M8 — Re-decide the query complexity caps

✅ **Closed 2026-09-25 — prefix floor and default-field cap shipped; the clause cap deferred.** Originally
📋 **a decision, not necessarily code — and its premise is now true.** [C2](#c2--query-complexity-caps)
was deferred on the reasoning that *rate limiting already bounds what a key costs the node per
unit time*. That reasoning is sound, and the premise was false on the write surface until
[M3](#m3--meter-the-write-surface-and-give-anonymous-callers-their-own-bucket) closed it on
2026-09-20. What is left is to re-read C2 against a multi-tenant node and either re-affirm the
deferral, or take it up.

**Where a cap would go, corrected 2026-09-25.** There are three `parse_query_lenient` call sites,
not two, and since [L12](#l12--storagesrclibrs-is-9961-lines-of-which-one-impl-block-is-4460) they
are all in `storage/src/search.rs`: one in `validate_query`, which parses for its errors and
discards the query, and two in `search_documents`. The split bears on where a refusal belongs — a
cap enforced at the `validate_query` site can *report* the limit to a caller asking whether a query
is acceptable, instead of only refusing it at execution. The same file's header already records
one unauthenticated way to wedge a search thread through this parser
(`fold_untokenizable_whitespace`, `storage/src/query.rs`), which is the shape of reasoning this
decision is being asked to generalise.

One thing M3 changed that bears on the decision: the rate limiter now meters an *unidentified*
caller too, so the deferral no longer rests on a node having issued keys.

**2026-09-19 — [M0-j](#m0--the-architecture-review-and-the-order-of-work) gives this a number.**
An unqualified term is expanded across every indexed text field, so on a two-hundred-field index
`alpha` costs 160 µs against 20 µs on a five-field one — 8×, close to linear, and that is
real execution rather than overhead. Schema width is the tenant's choice and the cost is the
node's, which is the shape of problem a complexity cap exists for. Re-reading C2 now has this to
read against, and a cap on the *default-field count a bare term may expand to* is a candidate the
original entry did not consider, alongside the ones it did.

**2026-09-25 — the prefix half is taken up; the clause cap is still to decide.** Reading C2
against the code found what M0-j had not measured: tantivy's only built-in expansion cap is its
*phrase* prefix's `max_expansions = 50`, applied per segment (verified — one segment returned 50,
several returned all 100). An unquoted `field:pre*` is not a prefix to tantivy at all: the `*`
is dropped and `pre` matched as a term. CameoDB's rewrite into a range is what makes it a
prefix — and a range has no limit (`range_query.rs:116` passes `None`). Measured on a 10M-document
shard, a one-character prefix covered 625k terms on a hash field and cost **164.7 ms**; two cost
10.6 ms, three 0.7 ms; it scales with the shard (the 1M run was 8–13× smaller).

Shipped: **`[security.limits] min_prefix_length`, default 2** (`0` expands any). A shorter prefix
is matched as the literal term and noted, not refused — the treatment an unrewritable prefix
already had, so one short clause does not cost the caller the rest of the query. Two was chosen
over three because it removes the row that grows unbounded while keeping the two-character
prefixes people actually type. And the silent cases were closed while in the function: a prefix
with no field, a leading or inner wildcard, and a prefix inside a field group each matched a
literal term with no word to the caller, and each is now reported. Detected from tantivy's own
grammar parse rather than a text scan, and only where the field's analyzer really drops the `*`
— a raw field keeps it in the term, so `id:a*b` is an exact match and says nothing.

Then, the same day: **`expand_unqualified_prefix`** (off by default) turns a bare `pre*` from a
reported literal into one prefix range per text default field. Off because its cost is one range
per default field, and nothing yet bounds how many default fields there are — which is the next
item. The rewrite is placed by a text scan (it has to know *where* to write) but licensed by the
grammar: it runs only when the scan and tantivy's parse agree on which bare prefixes the query
holds, and declines to a note otherwise. The query settings also moved into one
`storage::QueryPolicy` on `StorageConfig`, so the next bound is a field there rather than
another edit to every place a config is built.

**Then B: `max_default_fields`, default 64, and a per-index `default_fields`.** Measured on a
realistic corpus rather than M0-j's 500 documents, which had understated it by some fifty times:
where the fields share a vocabulary the cost of an unqualified query grows about fourfold per
doubling of the fields, and with the shard — a 5-term query at 1M documents cost 120 ms across 32
fields, 513 ms across 64, and at 200k documents 5.2 s across 400. Where each field has its own
vocabulary, 400 fields cost 0.25 ms. Past the cap a bare term is **narrowed, not refused**: the
index's declared list in order, or its fields by name — name, because each shard's own field order
is a hash-map accident and every shard must pick the same fields. Refusal was proposed and
declined: a wide index without a declared list is a legitimate thing to have. What makes the
narrowing honest is that it is reported — `searched_by_default`, `default_fields_truncated`,
`default_search` per field, through `GET /_config`, the listing and the MCP tools, and on each
search it narrowed as `_narrowed_default_fields` with an MCP `_warning`, carried the way
`_approximate_sort` is — rather than attached as a discarded clause, which would have made every
MCP search on a wide index fail. Set only when the query had an unqualified clause, so a
qualified query on a wide index stays clean.
Query-time only: no reindex, and existing indexes pick it up on start. The list is in the schema
fingerprint only when declared, so no schema written before it changes thumbprint.

With the fields bounded, `expand_unqualified_prefix` defaults **on** (decided 2026-09-25): a
bare prefix costs one range per default field, and there are now at most 64 of them. The clause
cap (C) is left as it is — B bounds the width, `min_prefix_length` bounds each range, and M8
closes here.

Deferred, not done: a cap on clauses after expansion (terms × default fields), which would bound
a long query the way B bounds a wide schema; and cancelling a search already running on a read
thread, which is 0.4.0 work.

**Deliberately not in 0.3.5:** [D1–D3](#d-phase-15--high-availability-reindex-replication--migration--planned)
(reindex, replication, migration), [A1](#a1--mcp-streaming) and [A5](#a5--semantic-routing),
[J2](#j2--a-json-field-should-mean-subfield-addressing)/[J3](#j3--the-flattening-lane-and-the-reference-that-describes-neither-lane-correctly),
[K1–K3](#k-phase-19--field-metrics-min-and-max--planned),
[B1](#b1--2f2--cpu-arenas-for-write--read--merge)/[B2](#b2--2f3--per-arena-jemalloc-stats),
and the four 0.4.0 cleanup items together with automating the dependency gate — all of which
are 0.4.0 or later. [CH3](#ch3--cursor-paging-search_after) is the closest call: deep paging is
a per-tenant cost multiplier and cursor paging is the fix, but it is a feature with a surface
change and it does not belong in a patch.

**Exit criteria for 0.3.5.** Resident memory is a function of data held rather than of index
names touched (M1, shown by a run that opens far more indexes than the cap); a compressed
ingest cannot expand past a stated ceiling (M2); every write route refuses past a per-key rate
(M3) and past a per-tenant total (M4); one key can be granted read-only on one index while keeping
write elsewhere (M5); an open-loop arm on the bulk lane and on the single-write lane both show
goodput that degrades rather than collapsing (M6); an evolving write costs one transaction and
one fsync rather than two, with no window between the document and the schema that made it
valid (M0-i); `node/` exports the ~18 names it is used for rather than 144 (M0 step 4);
`get_bulk_operation_budget` is either wired to the bulk path or deleted with its test, and not
both documented and dead (M0-h); and `scripts/validate/all.sh` runs clean at the cut, with
`cargo-audit` and `cargo-deny` installed and no `skip` line in its output.

## N. Pre-0.3.6 whole-code review 📋 Planned

Reviewed 2026-10-07, at `ee3db6d`, on the question *what is interconnected, and what in it is
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
2. **After the cut, in this order — N11, N12, N13, N14, N15, N16; N17 as each file is next touched.** N12 and N11 first because they
   delete code and remove the copies N2–N8 grew in; N16 last, because the engine as single owner
   removes most of what the split would otherwise move.

### N1 — `HEAD` requests skip authorization

**Defect.** ✅ **Done** 2026-10-07. `classify` takes a `HEAD` as the `GET` axum serves it
with, so the gate asks of it what it asks of the `GET`. Covered by `a_head_needs_what_its_get_needs`
(unit, against `decide`) and `a_head_request_is_authorized_as_the_get_it_runs` (the binary: a
reader's `HEAD /_admin/audit` answered 200 before the change and 403 after).

**Original entry.** **Checked.** `authz::ROUTES` has no `HEAD` rows, so `classify("HEAD",
…)` is `None`, and `decide` lets any valid key through on an unclassified path to "a 404 from the
router" (`authz.rs:169`, `:607`). axum serves `HEAD` with the `get()` handler, so the router
runs the `GET` handler. A reader key scoped to one index can therefore reach `/_admin/memory`,
`/_admin/workers` and `/_admin/audit` (the body is stripped, the status and `Content-Length` are
not), and can probe an index outside its scope with `HEAD /api/{other}/_config` and read the
200-or-404. That request also skips `validate_index_name`, and its audit record carries no index.
`HEAD /mcp` reaches the MCP route with no `McpAuthzRef`, which the MCP transport covers with
`unrestricted()` and a warning on every request (`mcp/src/transport.rs:102`; whether session
ownership refuses it was not followed through). The guard test
`every_mounted_route_is_classified` parses `.route(` calls and cannot see the implicit `HEAD`.

**Change.** Fold `HEAD` into `GET` in `classify`, before the match. Test that a reader key is
refused `HEAD /_admin/memory` with 403, and that `HEAD /api/{index}/_config` answers as the
`GET` does for the same key. Longer term (N14): one declarative route table — method, path,
access, handler — replaces the three hand-kept lists (`routes.rs`, `authz::ROUTES`, the startup
banner) and makes this class of gap impossible.

### N2 — A batched write never learns a new field

**Defect.** ✅ **Done** 2026-10-07, and smaller than first filed. Through the server, every write —
single, coalesced or `_bulk` — has its fields learned upstream by `staged_schema_validation`
before it reaches storage (`a_field_learned_from_writes_widens_instead_of_refusing` covers
`_bulk`), so storage finds them described. Storage's own learning is the guard for a write that
reaches it otherwise — a recreation racing a drop (`concurrency_chaos.rs`), a caller of the
crate — and on that path only the single write learned. The review read storage alone and missed
the orchestrator above it.

Fixed as proposed: `apply_write` is `apply_batch` of one; the batch learns in its prepare step,
under the schema lock, and commits the evolved schema in its own transaction (durably, whatever
`wal_sync` says), caching it once durable; `SchemaFields::document` is the one Tantivy document
builder for the write and the WAL replay, which now reads its columns through
`load_fields_from_existing_index`; `add_operations(index, n)` is the one counter bump. Covered by
`a_batch_learns_what_single_writes_learn` (failed before the change: the batch learned nothing)
and `a_refused_batch_leaves_the_schema_alone`.

**Original entry.** **Checked**, in storage only. `HybridStore::apply_write` evolves the schema from the
document (`store.rs:2632`, `evolve_from_document`); `apply_batch` does not. The only call to
`evolve_from_document` is the single-write path. The server sends one write through `apply_write`
and two or more coalesced writes through `apply_batch` (`shard.rs:944–963`), and a `_bulk` goes
through `apply_batch` always. So whether a field a document introduces appears in the schema —
which `merge_learned`, the thumbprint and the readiness check all depend on — is decided by whether
the writer thread happened to coalesce it. WAL replay (`store.rs:1260`) builds the Tantivy
document a third way, rebuilding `indexed_fields` by hand (a copy of
`load_fields_from_existing_index`, `:1491`) and calling `writable_type` directly.

The two paths copy the existence and dropped-index checks, the init-lock and live-writer retry
block, the schema load, the sequence reservation, the table definitions, the durability choice
and the document loop (`:2660–2689` ≈ `:3449–3480`); only `apply_write` evolves, only
`apply_batch` invalidates the size cache.

**Change.** `apply_write(op)` becomes `apply_batch(vec![op])` and returns its one result;
evolution moves into the batch's prepare step so a batch evolves once, under the schema lock, and
persists in its data transaction. One `build_tantivy_doc(fields, schema, id, seq, blob, bad_value)`
serves both writes and the replay. A test writes the same new field through a single write, a
two-write batch and a `_bulk`, and asserts one schema.

### N3 — A peer's 400 during a schema change is answered as a 503

**Defect.** ✅ **Done** 2026-10-07. The prepare round reads a node's error by its verdict through
`refuses_the_change` — `BadRequest` and `QuotaExceeded` are the caller's answer, returned with the
peer's text — so a peer's refusal is a 400 again rather than "not every node could be asked".
Covered by `a_peer_refusal_is_read_by_its_verdict`, which pins the classification for a peer's
`Remote` refusal, a local `Validation`, and the unreachable and older-build cases that stay 503.
No other arm in `schema_change.rs` or `orchestrator.rs` matches a peer's error by variant.

**Original entry.** **Checked.** In `run_change`'s prepare loop the arm
`Err(err @ OrchestratorError::Validation(_))` (`schema_change.rs:321`) can never match for a peer:
every error a peer sends deserializes to `OrchestratorError::Remote { verdict, .. }`
(`node/mod.rs:741–765`). A peer refusing a tokenizer an older build cannot build therefore falls
to the arm below, is counted as "not every node could be asked", and returns 503 — advice to retry
a change that no retry will make. The comment on the arm describes what it was meant to do.

**Change.** Match on `err.verdict()`: `BadRequest` and `QuotaExceeded` are the caller's answer,
with the peer's text. A test with a peer that refuses (the cluster suite already has the older-build
tokenizer case) asserts a 400.

### N4 — Node identity is loaded twice, and a broken file is silently replaced

**Defect.** ✅ **Done** 2026-10-07. `swarm::load_node_identity` is called once, in `main`, and the
`Keypair` moves into `DistributedCluster` (which held a path only to load it again). A file that
is there but unreadable, or holds a key that does not decode, stops the start with the file named
and left untouched; a failed save stops it too; a file from an earlier build with no key is the
one given a new key. `cluster::NodeIdentity::load_or_create` is deleted, with its examples and
tests. Covered by `a_node_identity_is_generated_once_and_then_kept`,
`a_damaged_node_identity_stops_the_start`, `a_node_identity_without_a_key_is_given_one` and, through
the binary, `a_node_with_a_damaged_identity_does_not_start`. `ARCHITECTURE.md` now says the UUID
is v5 from the key and what a damaged file does.

**Original entry.** **Checked.** `main.rs:375` calls `load_or_generate_keypair` and throws
the keypair away (`_keypair`); `swarm/mod.rs:406` calls it again. Inside it, a corrupt
`node_identity.json`, or a keypair that does not decode, makes a new key with a `warn!` and goes
on (`:631–663`) — and since the node's UUID is derived from the key, the node becomes a different
node, which is exactly the harm `NodeIdentity::save`'s own doc describes. A failed save only warns
(`:690`), after which the second call can generate a *different* key from the first.

**Change.** Load once in `main` and pass the `Keypair` into `DistributedCluster`. An unparseable
file, an undecodable key, or a failed save is a refusal to start, with the path named; only a
missing file generates. Delete `cluster::NodeIdentity::load_or_create` (`cluster/src/lib.rs:289`),
which production does not call and which writes non-atomically where `save` does not.

### N5 — The query rewrites find a field name by substring

**Defect.** ✅ **Done** 2026-10-07. `rewrite_clause_values` walks `field_references` and splices
each claimed clause's value; the date rewrite (four value readers — range, set, comparison,
literal — in one pass), the facet quoting and the prefix rewrite all run on it. An unquoted value
ends at whitespace or at the `)` closing its group, for every rewrite. Covered by
`a_date_rewrite_touches_only_its_own_clauses`, `a_facet_rewrite_touches_only_its_own_clauses` and
`a_field_name_inside_a_phrase_is_not_a_prefix_clause`, all three failing before the change:
`update:2024` was rewritten as a date beside a field named `date`, `subcat:/a/b` was quoted as a
facet, and a prefix inside a phrase was noted.

**Original entry.** **Checked** for the mechanism; the example below is **reported**, not
run. `normalize_date_ranges`, `_in_sets`, `_comparisons` and `_literals` (`query.rs:57–263`),
`normalize_facet_query` (`:2072`) and `normalize_prefix_query` (`:1728`) each `find("{field}:")`
with no token boundary and no awareness of quoted phrases. With a date field `date`, the clause
`update:2024` contains `date:` and is rewritten; text inside a phrase is rewritten too.
`field_references` (`:1209`) was written to be the one shared scanner, and only
`rewrite_shadow_fields` uses it.

**Change.** One pass driven by `field_references` spans — which already know the boundary and
the quoting — dispatching on the field's type, in place of four passes per date field and one per
string field. Tests: `update:2024` beside a `date` field, a field name inside a quoted phrase, a
field name that is the suffix of another.

### N6 — Three client errors that lose or mislabel data

**Defect.** ✅ **Done** 2026-10-07. `impl FromStr for TantivyFieldType` in storage is the one table
of type names — the schema's `Deserialize` and the CSV header hint both read it — and a header's
hint is what follows the last dot, only when it names a type. Every SDK request goes through one
`send`/`checked`/`send_json` path, so every refusal is an `HttpFailure` carrying its status and the
loader's `--recreate` advice is reachable. Covered by `a_header_hint_is_the_type_after_the_last_dot`,
`a_header_hint_reads_type_names_as_a_schema_does`, and an SDK `put_index_config` refusal asserting
`failure_status == Some(409)` in `a_retype_rebuilds_an_empty_index_and_is_refused_on_a_populated_one`.
`sdk.rs` is 200 lines shorter.

**Original entry.** **Checked** (all three).

- **Dotted headers collapse.** `parse_header_with_hint` (`client/src/cli/ingest.rs:100`) splits at
  the first `.`, so `geo.lat` and `geo.lon` both become the column `geo`, and the second value
  overwrites the first. Nothing tests it.
- **The type aliases have drifted from storage.** `map_type_hint` (`:107`) maps `u64` to `I64`
  where storage's `U64` is unsigned, and `string` to `Text` where storage's `String` is
  untokenized; it lacks `unsigned`, `datetime`, `bytes`, `facet` and others storage accepts
  (`storage/src/schema.rs:186–220`).
- **The `--recreate` hint cannot appear.** `put_index_config` (`sdk.rs:480`) returns a plain
  `bail!`, so `failure_status(&err) == Some(409)` in `load_data_from_source`
  (`ingest.rs:2363`) is never true, though the server answers 409 (`catalogue.rs:159`). Only
  `search`, `write_document` and `bulk_index` return the typed `HttpFailure`.

**Change.** `impl FromStr for TantivyFieldType` in storage, used by its `Deserialize` and by the
header hint, and the split taken at the last `.` and only when the suffix is a known type. One
private `send(RequestBuilder, what) -> Result<Response>` in the SDK that always yields
`HttpFailure` with `refusal_text` and the hint, so each of the 17 request methods is three lines
and the 409 reaches the loader.

### N7 — The first shard's memory budget is the whole node's

**Defect.** ✅ **Done** 2026-10-07, and wider than filed: the same divisor sets each shard's share
of the open-index cap, so on a first boot the first shard could hold the node's whole cap open.
`create_initial_shards(count)` creates up to `max_shards` shards, each budgeted as one of that
many; `shard_runtime(id, total)` builds what a shard runs with for creation and hydration alike;
`create_shard(id, total)` replaces `handle_propose_shard`, and the `ProposeShard` wrapper is gone,
with the per-shard coordinator registration that `main` repeated for every shard. Hydration opens
only up to `max_shards` and leaves the rest on disk, where it used to open them and drop them
without a shutdown. Covered by `a_first_boot_budgets_every_shard_as_one_of_all`: three shards each
budgeted as one of three, then a restart under a cap of two opening two, each budgeted as one of
two.

**Original entry.** **Checked.** On a first boot `main.rs:476` calls `handle_propose_shard`
once per shard, and each call passes `total_shards = self.shards.len() + 1`
(`orchestrator.rs:4776`), so shard *k* of *N* is built with `HybridStore::new(cfg, k)`, which
divides the cache budget by *k* (`storage/src/store.rs:406`): the first shard budgets the node's
whole cache for itself, the last a *N*th of it. A restart's hydrate passes *N* and gets the
division right, so a node's memory behaviour differs between its first boot and every later one.
Hydrate also starts shards beyond `max_shards` and then drops them without a shutdown (`:4604`).

**Change.** One `create_shards(n)` that passes `n` to every shard, sharing a `shard_runtime(total,
pin)` with hydrate — the construction is written twice today (`:4547–4583`, `:4772–4790`) — and a
filter against `max_shards` before anything is spawned.

### N8 — A streaming search broadcast returns local hits only

**Defect.** ✅ **Done** 2026-10-07, confirmed first: on two clustered nodes with
`enable_streaming_search = false`, a streamed search of 200 documents answered 94 hits — what the
receiving node held — and 200 after the change. `ClientOp::normalized` reads a `Stream` as the
`Search` it names where an op enters a node (the router for this node's requests, the
orchestrator's handler for a peer's); the stream route builds a `Search`, and `Stream` stays on
the wire only for a peer on an earlier build. The `Stream` arms past entry, the unreachable search
and write merges in `handle_broadcast` and the fan-out field only they read are gone. Confirming it
found a second defect, filed and fixed on its own: two nodes on one host could not form a cluster,
because a seed was taken for this node on its IP alone.

**Original entry.** **Reported**, from reading `router.rs`. `handle_broadcast` returns early for `Search`
(`router.rs:983`), and a broadcast `Stream` falls to `_ => first result` (`:1406`), so with
`enable_streaming_search = false` the answer is the first node's rows. Beside it are arms that
cannot run: the Search merge arm marked "Unreachable" (`:1100–1167`), the Write and BulkWrite arm
(`:1168`, since writes are refused at `:673`), and the `handle_broadcast_request` pass-through
(`:1650`).

**Change.** Convert `Stream` to `Search` at the router's entry, and delete the `Stream` arms in
`handle_client_op` and `execute` and the dead arms above.

### N9 — Storage `shutdown` is a fourth commit path, with the race fixed elsewhere

**Defect.** ✅ **Done** 2026-10-07, and smaller than filed. The race does not exist today: every
write holds its writer from reserving its sequence until its document is added, so a sequence
`shutdown` read before locking belonged to a write finished by the time it had the lock, and the
commit contained it. What was real is the copy itself — its own lock loop, which waited out a
poisoned lock until it timed out — and the wait done while iterating `writers`, holding one of the
map's shards. `shutdown` now collects the writers first and commits each through
`commit_locked_writer` and `checkpoint_after_commit`, as `close_index` does, locking with
`lock_writer_within`. Covered by `shutdown_commits_a_writer_it_had_to_wait_for`: a writer held on
another thread through the shutdown is committed and checkpointed once released, and the next
open replays nothing. The subset of maps `shutdown` clears is left to N15.

**Original entry.** **Reported.** `HybridStore::shutdown` (`store.rs:584–643`) reads
`current_seq` before it locks the writer — the order `commit_locked_writer`'s doc (`:2211`)
describes as the bug it fixed — and its comment at `:588` says the opposite. It iterates
`self.writers` while spinning up to five seconds on a `try_lock`, holding a map shard guard while
it waits on a mutex, which is the shape OB14 deadlocked on. It clears a different subset of the
per-index maps than `drop_index_caches` does.

**Change.** Collect the `Arc`s first, then lock with the timeout and call `commit_locked_writer`
and `checkpoint_after_commit` as `close_index` does; N15 removes the "different subset" by
making the per-index state one thing.

### N10 — Storage errors are flattened, and lose their HTTP status

**Defect.** ✅ **Done** 2026-10-07. `orchestrator::blocking` runs a store call on the blocking pool
and keeps its error as `OrchestratorError::Storage`, so its verdict reaches the caller; the schema
reads, the schema persists, the apply, the record and document counts, the unbuilt-field check and
`validate_query` all go through it, and the persists and the apply run their shards with
`try_join_all` as before. The writer thread answers every caller of a failed batch with
`StoreError::duplicate`, which copies the variants that decide a status — not found, closed or
panicked writer, invalid name, refused value — and keeps the message of the library errors, all of
them faults of the node. Covered by `a_store_error_keeps_its_verdict`. The typed `NotSortable`
variant and typed validator errors are left to N14, since both answer correctly today.

**Original entry.** **Reported.** `OrchestratorError::Storage(#[from] StoreError)` has
verdicts — `IndexNotFound` is 404, a closed or panicked writer 503, `InvalidIndexName` and
`InvalidFieldValue` 400 — and these paths turn the error into `Io`, which is always 500:
`schema_from_store` (`orchestrator.rs:1017`, on every `load_schema`), `persist_schema_to_stores`
(`:3833`), `orch_apply_schema` (`:6145`). The writer thread rewrites every failure as
`StoreError::Serialization` (`shard.rs:870`, `:989`, `:1058`), so a 400-class
`InvalidFieldValue` reaches every caller of a coalesced batch as an internal error. "Not
sortable" travels as `StoreError::Io(InvalidInput)` and is decoded by error kind
(`search.rs:627`, `node/mod.rs:907`); the schema validators return `Result<(), String>`
(`schema.rs:1499`, `:1540`, `:1577`, `:1651`) and `IndexNotFound` carries sentences.

**Change.** One `blocking(store, f)` helper (a `JoinError` to `Io`, a `StoreError` to `Storage`
through `?`); `StoreError` made shareable (`Arc<StoreError>` fanned out from the writer, read
through by `verdict()`); a `NotSortable { field, reason }` variant; typed validator errors.

### N10a — A search of an index that does not exist answers 404

**Defect.** ✅ **Done** 2026-10-08, added to the 0.3.6 cut. A search of a name no node held — never
created, or deleted with its schema — answered `200`, no hits and `shards: 4 of 4 responded`, so a
misspelled index read as a query that matched nothing; `GET /_config` and `DELETE` already said
404. `ScatterCtx::gather` refuses an index its node holds no live schema for
(`StoreError::IndexNotFound`), and the broadcast merges — plain and streaming — count a source that
holds none as absent rather than failed: the answer is a 404 only when every source is absent, so
a node that has not yet heard of a new index neither fails the search nor hides the nodes that
have. The streamed route runs the search before the response starts (`route_and_handle_stream` is
`async` and returns the refusal), so a refusal there is its status, not a `200` with an `_error`
line — which also fixes the unsortable-field and overload refusals on that route. The 404 body
carries `"code": "index_not_found"`, which a path no route serves does not. Covered by
`searching_an_index_that_does_not_exist_is_a_not_found`, `a_refused_streamed_search_answers_with_its_status`,
the unit tests of the merge rule, and `probe missing` in the cluster suite (every node, before the
write, just after it through each node, and after the delete).

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

---

# Part II — Archive

Delivered work, kept in full rather than summarised away. Three kinds of entry earn their
place here beyond the record of what shipped:

- **Measurements**, so a claim about performance can be checked against the run that produced
  it rather than repeated from memory.
- **Rejections** — things built and removed, or designed and turned down. Each says why, so
  the reasoning that produced them does not produce them again.
- **Corrections**, where an item's own premise turned out to be wrong. Those are the entries
  most worth reading before opening adjacent work.

## Phases 1–9 — Foundations through advanced architecture ✅ Done

**Phase 1**: Storage Durability & WAL Recovery ✅ Done
- Added Sequence ID to Schema for WAL tracking
- Implemented WAL Replay with get_last_indexed_seq/recover_index
- Integrated automatic recovery during index open
- Shortened critical section with optimized serialization

**Phase 2**: Shadow Field Replacement ✅ Done  
- Replaced shadow field scanning with O(1) HashSet lookup
- Implemented shadow field replacement logic
- Optimized move semantics for performance
- Fixed shadow field behavior in document reconstruction

**Phase 3**: Index Warmup & Recovery ✅ Done
- Added automatic index warmup on startup
- Implemented recovery procedures for index consistency
- Enhanced index management with proper error handling

**Phase 4**: Basic Actor System ✅ Done
- Built Kameo-based actor system for shard management
- Implemented MicroshardActor with message handling
- Added StorageCommand enum for thread-safe operations
- Created writer thread pattern for isolation

**Phase 5**: Cluster Coordination ✅ Done
- Implemented distributed cluster coordination with DHT
- Added consistent hashing ring for node distribution
- Created ClusterCoordinator for swarm management
- Integrated peer discovery and metadata exchange

**Phase 6**: Storage Performance Optimizations ✅ Done
- Optimized I/O patterns with batch WAL recovery
- Implemented granular thread pool architecture
- Added writer thread write coalescing
- Enhanced ACID-compliant commit optimization
- Configured Redb cache sizes (64MB read, 32MB write)
- Verified bulk memory budget scaling with comprehensive tests

**Phase 7**: Code Review Issues & Critical Fixes ✅ Done
- Fixed read runtime resource leak with Drop trait implementation
- Prevented writer thread starvation with bounded drain limit (max 64 commands)
- Corrected batch coalescing math using integer arithmetic with remainder distribution
- All critical bugs and resource leaks resolved

**Phase 8**: RouterActor & Architecture Enhancements ✅ Done
- Implemented worker pool pattern bypassing actor mailbox for hot-path operations
- Added lock-free intelligent caching (schema cache, fingerprint index, routing ring)
- Delegated routing decisions to ClusterCoordinator
- Optimized scatter-gather with streaming search

**Phase 9**: Advanced Architecture Optimizations ✅ Done
- Parallel schema evolution: staged Rayon validation followed by sequential evolution with concurrent persistence (50‑70% faster on multi-shard clusters).
- Remote connection pooling: shared `RemotePeerPool` with channel-aware caching, automatic invalidation on `PeerLost`, and full integration across RouterActor, NodeOrchestrator bulk forwarding, and ClusterCoordinator remotes.

*Note: Phases 1-9 are fully completed with all optimizations implemented and tested.*

## Phase 10 — Field Projection for Search Responses ✅ Done

**Implementation Summary:**
- **HTTP Layer**: Extended `SearchPayload` with `fields: Option<Vec<String>>` and implemented `parse_query_keywords()` to extract `limit` and `return` keywords from query strings. Both `search_handler` and `search_stream_handler` now support field projection.
- **Routing Layer**: Updated `ClientOp::Search` and `ClientOp::Stream` to carry `fields` parameter through all routing paths (local, remote, broadcast, streaming).
- **Execution Layer**: Created `apply_field_projection()` helper that filters JSON documents while preserving metadata fields (those starting with `_`). Integrated into both `engine_search()` and `orch_search()` methods.

**Query Syntax**: `<tantivy_query> [limit <n>] [return <field1,field2,...>]`  
**Example**: `title:rust return title,author,year` returns only those three fields plus metadata.

## Phase 11 — Read/Write Workflow Hot-Path Optimizations ✅ Done

**Implementation Summary:**
1. **Remove Tantivy ID roundtrip in search hits** ✅ — Direct extraction of stored `id` field values from Tantivy search results, eliminating per-hit JSON parse overhead.
2. **Tighten duplicate work inside `apply_batch()`** ✅ — Reuse schema and prepared document state; eliminate repeated shadow filtering and re-serialization.
3. **Enforce configured shard and remote concurrency limits** ✅ — Bounded concurrency in scatter-gather paths.
4. **Reduce worker-pool coordination contention** ✅ — Lower-contention queue design for hot-path workers.
5. **Improve early-termination and result-merge behavior** ✅ — Bounded top-K merging with score-aware pruning.
6. **Implement true end-to-end search streaming** ✅ — Incremental NDJSON streaming with backpressure-aware fan-in.
7. **Implement incremental write-stream ingestion** ✅ — Incremental NDJSON decoding with bounded ingestion.

## Phase 11.5 — Jemalloc Memory Management ✅ Done

**Implementation Summary:**
- **Jemalloc integration**: Integrated `tikv-jemallocator` and `tikv-jemalloc-sys` (with `stats` feature) on Linux targets for production memory management.
- **Admin HTTP endpoints**: Added `GET /_admin/memory` (stats) and `POST /_admin/memory/purge` (manual purge with optional `force` flag).
- **Admin CLI commands**: Added `admin memory stats` and `admin memory purge [--force]` to the interactive CLI and command-line client.
- **Typed response structs**: `AdminMemoryReport`, `ProcessMemoryStats`, `JemallocStats` with platform-aware field omission (null fields excluded from JSON).
- **Cross-platform stats**: Linux uses `/proc/self/status`, macOS uses `proc_pidinfo` syscall, Windows uses `wmic process` — all providing RSS, VSZ, and thread count.
- **Jemalloc purge**: Decay-based purge (respects `dirty_decay_ms`) and aggressive purge (bypasses timers). Returns `process` (before) and `process_after_purge` snapshots plus `purge_result`.
- **Systemd service tuning**: `cameodb.service` ships with production `MALLOC_CONF`: `background_thread:true,percpu_arena:percpu,oversize_threshold:0,dirty_decay_ms:2000,muzzy_decay_ms:0`.

**Default `MALLOC_CONF` rationale:**
- `dirty_decay_ms:2000` — balances throughput for 8-32 parallel writers while keeping memory pressure reasonable. Override via `systemctl edit cameodb` if RSS becomes a concern.

## Phase 12 — MCP Server Integration for AI Agents

◐ **Partial** — steps 1–6 and completion-track items 1–4 are here; what remains is in
[Part I, section A](#a-phase-12--mcp-server-integration--partial).

### The phase as scoped

**Objective**: Implement a Model Context Protocol (MCP) server within CameoDB to expose search capabilities as tools for AI agents, enabling efficient context retrieval from indexed datasets.

**Architecture Goals:**
- Single CameoDB binary with MCP exposed through the existing HTTP server
- HTTP/SSE network transport using a shared-port model
- New `crates/mcp` package defines its own `axum::Router` but does not start a separate server
- Main `server` crate nests the MCP router into the existing application router and shares the same `AppState`
- Expose search and metadata capabilities as MCP tools while reusing the stable search path
- Support both local and cluster-wide operations through existing `RouterActor` and `ClusterCoordinator`
- Enable session-aware JSON-RPC message handling and streaming results for large datasets

**Implementation Steps:**

1. **Workspace & Dependencies** ✅ Done
   - Create `crates/mcp` package and add it to the workspace `Cargo.toml`
   - Add required dependencies to `crates/mcp/Cargo.toml`: `axum`, `axum-extra`, `tokio`, `serde`, `serde_json`, and an MCP/JSON-RPC Rust SDK
   - Add the new `cameodb_mcp` crate as a dependency of the main `server` crate
   - Keep MCP transport inside the existing application runtime; do not start a second HTTP server

2. **MCP Router & Transport Layer** ✅ Done
   - Create `crates/mcp/src/server.rs` with a function returning `Router<AppState>`
   - Implement `GET /sse` to establish SSE transport and register client sessions
   - Implement `POST /messages` to receive JSON-RPC messages, map them to sessions, and route them to MCP handlers
   - Mount the MCP router from `crates/server/src/http_server.rs` using `.nest()` on the existing Axum app
   - Reuse the main shared `AppState` so MCP handlers can call the same routing and cluster services as HTTP APIs

3. **MCP Protocol Session Handling** ✅ Done
   - Implement MCP session registry and connection lifecycle management
   - Support initialize, ping, capabilities negotiation, tools listing, and tools invocation over JSON-RPC
   - Correct notification handling (notifications/initialized, notifications/cancelled return no response per JSON-RPC spec)
   - Define transport-safe error mapping from CameoDB failures into MCP error responses
   - Add bounded session cleanup, heartbeat handling, and backpressure-aware streaming behavior

4. **Core MCP Tools** ✅ Done (MCP naming convention: verb-first snake_case, with title/annotations)
   - **`search_index`**: Execute full-text search on a single index
     - Parameters: `index`, `query`, `limit`, `fields` (optional projection)
     - Returns: JSON array of matching documents with scores
     - Tool description includes full Tantivy query syntax quick reference and field-type operator matrix
   - **`search_across_indexes`**: Federated search across multiple indexes
     - Parameters: `indexes[]`, `query`, `limit`
     - Returns: Combined results with `_index_source` metadata and per-index field projection
   - **`describe_index`**: Retrieve schema and statistics for a single index
     - Parameters: `index`
     - Returns: Complete field definitions, types, document count, size
   - **`validate_query`**: Field-type-aware CameoDB query syntax validation, unknown field detection, structural checks (quotes/parens), fuzzy "did you mean" suggestions, and full syntax reference with agent pro tips
   - **`get_catalog_stats`**: Document, field and byte totals across the catalogue; one index's statistics come from `describe_index`
   - **`list_indexes`**: Enumerate all available indexes with schemas
     - Parameters: none
     - Returns: All index schemas with metadata (leverages existing `/_indexes` endpoint)
   - **MCP README** (`crates/mcp/README.md`): Full query syntax reference with operator examples and field-type compatibility table

5. **Advanced MCP Features** ✅ Done
   - **Field Projection**: Auto-suggest relevant fields based on partial input
   - All tools include `title`, property `description`s, and `annotations` (`readOnlyHint`, `openWorldHint`) per MCP draft spec
   - **Streaming Support**: 📋 Planned — Large result sets via MCP streaming protocol
   - **Semantic Routing**: 📋 Planned — Auto-select best index(es) for query intent

6. **MCP Resource Providers** ✅ Done
   - Expose indexes as MCP resources for exploration
   - Provide schema documentation as resources
   - Enable agents to discover available datasets dynamically

7. **Security & Access Control** ➡️ Moved to Phase 14
   - Authentication, authorization, TLS, and hardening are tracked as a dedicated
     security project — see [Phase 14](#phase-14--security-hardening).
   - MCP-specific security (rate limiting, query complexity, audit logging) is
     covered under Phase 14 Stage C once the core auth layer exists.

**Expected Benefits:**
- Enable AI agents to query structured/unstructured data efficiently
- Provide grounded context for LLM responses from real datasets
- Support RAG (Retrieval-Augmented Generation) workflows
- Unlock new use cases: semantic search, knowledge retrieval, fact-checking
- Position CameoDB as AI-native search infrastructure

**Success Metrics:**
- MCP server responds to all standard tool calls correctly
- Search latency < 100ms for typical agent queries
- Support concurrent agent sessions without degradation
- Compatible with major MCP clients (Claude Desktop, custom agents)

### The completion track — items 1–4 ✅ Done

Ordered by cost on 2026-08-15. Two of the four turned out to be engine work surfaced by the
MCP tools rather than MCP work: what an agent could see was a broken endpoint and a validator
that validates nothing, and in both cases the cause sat under the tool.

1. ~~**`PATCH /api/{index}/_schema` does not work.**~~ ✅ Landed 2026-08-15, and it was three
   defects rather than the one recorded here, each hiding the next. **(a)** The endpoint answered
   `500` for every index that had ever been written to. The cause was not `CreateConfig` as such:
   persisting *any* schema against a live index stranded its writer, because
   `store_schema_and_cache` evicts the field cache while `get_or_create_index`'s fast path
   requires the writer *and* the fields, so a live index fell through to the slow path and opened
   a second `IndexWriter` against a lockfile the first still held. `apply_write`, `apply_batch`
   and `invalidate_schema_cache` all armed the same trap; it was reachable with no HTTP involved
   at all. Fixed where it belongs — a cached writer with no cached fields now rebuilds the field
   handles from its own index, which is also what keeps the pair in step by construction.
   **(b)** The handler round-tripped the schema through the `GetConfig` response, and that shape
   carries only `fields` and `description`, so serde defaults silently reset `routing_field_name`
   to `id` — changing which shard a document routes to — along with `version`, `fingerprint`,
   `created_at` and `updated_at`. The round trip is gone: `ClientOp::UpdateSchema` edits the
   stored struct in place. **(c)** The interesting one, and the place this item's own premise was
   half wrong in both directions. A Tantivy schema is fixed at `Index::create_in_dir` from the
   fields that are `indexed` at that moment, so a field first seen in a later document has no
   column and setting its flag does not make it searchable *now*.
   **But the stored schema is a declaration, and the index is rebuilt from it** —
   `delete_index_data(delete_schema = false)` then re-ingest, which is a path that already
   exists and works (asserted end to end in `schema_promotion_test.rs`). Marking the field is
   therefore the *first step* of making it searchable, and a first attempt at this item refused
   it with `409`, which blocked the only route there. Corrected 2026-08-15: the edit is applied
   and the field reported under `pending_reindex_fields` with a note saying what completes it.
   Nothing is silently wrong in between — a query naming the field reports the clause as
   discarded and the MCP layer refuses the search outright.
   One more thing that first attempt got wrong: it required every shard to accept the edit.
   Shards normally agree, and both schema-creation paths ensure it — a declared schema is fanned
   out to every shard, and an inferred one is sampled from up to 200 documents and persisted
   everywhere before the first write lands. The exception is semi-structured input written a
   document at a time, where a field only some documents carry reaches only some shards; there
   the divergence is legitimate, since those shards genuinely cannot answer a query on it. A
   single shard's "unknown" was refusing edits the other shards could apply. A name is now refused only when *every*
   shard says it is unknown, planned across all shards before any of them writes.
   Eight engine tests and five against a real node process.
2. ~~**`validate_query` cannot actually validate a query.**~~ ✅ Landed 2026-08-15.
   `HybridStore::validate_query` parses against an index without searching it, and the tool
   reports what it found. The engine work was the point: resolving a field name needs a built
   Tantivy index, so nothing above the storage layer could answer the question. It parses through
   the *same* path a search takes — one `prepare_query_parser` now builds the normalization and
   the default field set for the search path, the count-only path and validation, which had been
   three copies of the same twenty lines. A validator that parsed differently from the search
   would be worse than none.
   Syntax errors and unmatched clauses are reported separately, because they are fixed
   differently: `parses` plus `syntax_errors` with the parser's own message and position, against
   `discarded_clauses` for what parses and can never match. `normalized_query` is returned too —
   a query is rewritten before it runs and that rewrite is where a surprising result usually comes
   from. `parses` is `null`, never `true`, when the index could not be checked, so an unchecked
   query cannot read as a passing one.
   The gap it closes, measured rather than asserted: `title:`, `title:[2020 TO`,
   `year:{2020 TO 2021` and `AND title:rust` all balance their quotes and parentheses — so the
   old structural check passed every one — and none of them parse. A test asserts each one's
   balance before asserting it fails, so the reason the case is there stays visible. Another
   asserts that what validation calls discarded is exactly what a search discards, which is the
   property that makes checking first worth a round trip. Seven engine tests
   (`crates/storage/tests/query_validation_test.rs`), four over MCP against a real node
   (`crates/server/tests/mcp_discarded_clauses.rs`).
   **Not done, deliberately:** the tool still returns the static syntax reference. Moving that to
   `instructions` and a `cameodb://syntax` resource is a change to the tool's contract rather than
   a fix to it, and the tool's own description currently tells agents to call it with no arguments
   for exactly that text — so it belongs with [A2, the documentation
   pass](#a2--the-documentation-pass), where the description, the instructions and the README
   change together
3. ~~**One structured description of an index, built once.**~~ ✅ Landed 2026-08-15. The engine
   produces one per-index shape and `GET /_indexes`, `GET /_cluster/_indexes` and
   `GET /api/{index}/_config` all return it; the bundled client, the MCP tools and the HTTP
   listing render it rather than each composing their own. Identity is `name` everywhere and
   `fields` is an ordered array whose entries all carry the same keys — the survey that preceded
   this found **seven** properties spelled differently across the callers (`field` against a map
   key, `type` against `field_type`, `shadow` against `is_shadow`, `hint` against `query_hint`,
   and three flags that were present, absent or only-when-true depending on who emitted them).
   **The round trips are gone**, which was the larger cost: `cameodb list indexes` was `1 + N`
   *sequential* requests and `list index <name>` was 2; the REPL was `1 + 2N`, because its
   completion cache re-fetched every schema the command it had just run had already read. All
   are one request now, as is MCP `list_indexes`, which was `1 + N`. MCP's listing still projects
   down to the lean catalogue entry — that was a deliberate context decision and it now costs
   nothing, since the data already arrives.
   **The server disagreed with itself, which the item did not record.** `GET /_cluster/_indexes`
   dropped `memory_*` and `warm_shards` from its rollup while keeping them one level down in
   `nodes[]`, so one response described an index two ways; the merge went through a private
   struct that lacked the fields. Sizes were summed as *already-rounded megabytes* across nodes,
   losing up to a megabyte per node — they are bytes now, rounded once at display. Two live bugs
   turned up too: `cameodb://indexes/{index}/schema` answered `null` for every index, reading a
   key `describe_index` had already removed; and `_seq` was filtered from some field lists and
   not others, so one `validate_query` response reported two different field counts.
   **`searchable` is the fact that made this worth doing in the engine.** `indexed` is what the
   schema declares; `searchable` is whether the built index has a column. They differ exactly for
   the field item 1 reports as `pending_reindex`, and nothing above the engine can see the
   difference — the MCP tools had been calling such a field queryable, so an agent querying it got
   silence. `is_queryable()` was `indexed || is_shadow`; it is `searchable || is_shadow` now, the
   shadow half kept because a shadow field names the identifier, which is answered from redb
   rather than the search index.
   Carried forward as
   [A4](#a4--what-a-schema-listing-says-about-id-for-projection-and-for-sorting) and closed there
   on 2026-08-31, the other way round: `id` stays offered, because `id:VALUE` still answers on such
   an index and withdrawing it would make `validate_query` call the working form unknown. The
   projection was made to work instead, and the `id` entry carries `returned_as` naming the field
   the hits use in its place.

4. ~~**Paging: `offset` on a search.**~~ ✅ Landed 2026-08-15. `offset` on both HTTP search routes,
   both MCP tools, the SDK, a `--offset` flag and the query grammar (`limit 10 offset 20`) — the
   last of those because the client and the REPL express a search *entirely* through that grammar,
   so a paging option that existed only as a JSON field was one they could not reach.
   **The skip is applied once, after the merge, and `and_offset` is not used.** Tantivy's own is
   the right tool for one segment and the wrong one for a scatter-gather: every hit on a page may
   come from a single source, so a source that skipped `offset` of *its own* hits drops rows that
   belong on the page. Each source is asked for `offset + limit` from the front instead. This was
   not a theoretical hazard — the federated tool shipped doing exactly that, applying the offset
   per index *and* again at the merge, so page 2 was page 3 of an order built from the wrong
   candidates. `crates/server/tests/mcp_federated.rs` now fixes it in place with an interleaved
   fixture, where a per-index skip returns different *documents* rather than a different order.
   **Both decisions the item named were made, and the second was got wrong first.** The HTTP API
   did get it at the same time — but the bound went only on the MCP tools, and that route had
   never bounded `limit` either, so `{"limit": 10, "offset": 500000000}` was a request that reads
   as ten documents and hands the node an allocation the caller sizes (Tantivy's collector
   allocates `2 × limit` up front, before matching anything). `SearchWindow::checked` is now the
   one rule, applied by every surface, over `offset + limit` — and counting the node's default
   limit when none is given, which an earlier check read as zero.
   **The third note — "restrict paging to FAST-field sorts or say so where an agent will read
   it" — was answered by removing the approximation where it can be removed and reporting it where
   it cannot.** A text field declared `fast` now builds the string fast column, so its sort is a
   true lexicographic order over every match and pages through correctly. Without one, the
   response carries `_approximate_sort` and a `_warning` saying the order is over a sample and
   does not page — in the response, not the node's log, which is where the first attempt put it.
   `sortable` joins `searchable` on every field description for the same reason `searchable`
   exists: `fast` is a declaration, and only the engine knows whether the column was built.
   **Still open:** a text field cannot be made `sortable` after its index holds data — the column
   is written at index time and `PATCH /_schema` edits `indexed` only. That is
   [D1, reindex](#d1--reindex), and the gap is reported rather than hidden.

## Phase 13 — Thread-Per-Core & Memory Operations

◐ **Partial** — Stages 1, 2a–2e and 2f.1 are here; Stages 2f.2 and 2f.3 are in
[Part I, section B](#b-phase-13--stage-2f--partial).

**Objective**: Eliminate cross-core wakeups and cache thrashing on the write hot path, improve memory observability, and extract admin code into maintainable modules. Each stage is linear, flag-gated, and independently testable.

### Current Architecture Analysis

**Existing Threading Model:**
- **Tokio Async Runtimes (2 separate)**:
  - Main runtime: HTTP server (axum), kameo actors, orchestrator workers
  - Dedicated read runtime: `multi_thread` builder, threads named `cameodb-read`, threads = `config.search_threads` or `max(2, cpu_cores / 2)`

- **Orchestrator Worker Pool** (async, mailbox-bypass):
  - One `mpsc::channel::<OrchestratorJob>` per worker (not shared)
  - `worker_count = max(1, min(local_shards * 2, cpu_cores * 2))`
  - Dispatch is round-robin via `OrchestratorWorkerTx::try_send` (atomic counter, fall-through on Full)
  - Workers are tokio tasks on the main runtime — NOT pinned

- **Per-Shard Dedicated Writer Thread** (sync OS thread):
  - One OS thread per shard, named `writer-shard-<uuid>`
  - Receives `StorageCommand` over bounded `mpsc::channel` (capacity = 1024)
  - Implements write coalescing: blocks on first command, then `try_recv` drains up to 256 more
  - Strictly serializes writes per shard (required by redb single-writer semantics)

**Current Hot-Path Trace (Write):**
```
HTTP req on axum tokio worker (any core)
  → AppState::router.route_and_handle(op, ...)
  → OrchestratorWorkerTx::try_send (round-robin)      [atomic fetch_add]
  → Orchestrator worker tokio task on main rt (any core, may migrate)
  → engine.execute(op) → engine_write(...)
  → MicroshardActor.handle_write_via_channel
  → writer-shard-<uuid> OS thread (pinned in Stage 1)
  → reply via oneshot back across all the layers
```

### Stage 1 — Writer Thread Core Pinning ✅ Done

- Added `core_affinity = "0.8"` dependency to `crates/server/Cargo.toml`
- Added `writer_core_affinity: bool` to `NodeConfig`, `StorageConfig`, and `MicroshardActor`
- When enabled, each shard's writer thread pins to `core_ids[xxh3_64(shard_uuid_bytes) % num_cores]`
- Configurable via `[storage].writer_core_affinity` in `cameodb.toml` (default: true)

---

### Stage 2a — Shard-Affine Worker Dispatch ✅ Done

**Risk:** Low | **LOC:** ~80 | **Prerequisite:** None

**Goal:** Replace round-robin dispatch with shard-affine routing so that operations targeting the same shard always land on the same worker, reducing cross-core wakeups when writer pinning is enabled.

**Implementation:**
- Add `affinity_shard: Option<Uuid>` to `OrchestratorJob::Execute`
- Add `try_send_affine(&self, job, shard_id: Option<Uuid>)` to `OrchestratorWorkerTx`
  - When `shard_id` is `Some`, route to `workers[ordinal(shard_id) % worker_count]`
  - Fall through to neighboring workers on `Full` (preserve throughput)
  - When `shard_id` is `None` (broadcast/scatter), fall back to round-robin
- In `handle_client_op`, extract routing key from `ClientOp::Write` before dispatch
- Engine fast path: `engine_write` skips redundant `route_write` ring lookup when `affinity_shard` is `Some`
- Flag-gated via `shard_affine_dispatch` config, default `false` preserves round-robin behavior

**Expected Impact:**
- Eliminates 1 cross-core wakeup per write when writer pinning is enabled
- Cache locality: `Arc<HybridStore>`, `routing_ring`, `schema_cache` stay hot on same worker
- Zero impact on broadcast/scatter operations (round-robin fallback)

---

### Stage 2b — Extract Admin Memory Module ✅ Done

**Risk:** Low | **LOC:** ~200 (mostly move) | **Prerequisite:** None (independent of 2a)

**Goal:** Move memory-related types and functions out of the 6700-line `node_orchestrator.rs` into a dedicated module for maintainability and testability.

**Implementation:**
- Create `crates/server/src/admin/memory.rs` (new module)
- Move into it:
  - `ProcessMemoryStats`, `JemallocStats`, `AdminMemoryReport` structs
  - `read_process_memory_stats()` (all platform variants)
  - `read_jemalloc_stats()`, `call_memory_purge()`
  - `PurgeAdminMemory` message struct
- Add `pub mod admin;` to `main.rs` and `use` imports in `node_orchestrator.rs`
- No behavioral changes — pure refactoring

---

### Stage 2c — Per-Index Memory Stats ✅ Done

**Risk:** Low | **LOC:** ~5 | **Prerequisite:** Stage 2b

**Goal:** Add per-index memory visibility in the `/_indexes` response.

**2c.1 — Auto-Purge Timer:** ⏭️ Skipped
- Jemalloc's built-in `dirty_decay_ms` auto-release is working stably; no additional timer needed.

**2c.2 — Per-Index Memory in `/_indexes`:** ✅ Done
- Added `memory_mb` field to each index in the `list_indexes` response
- Derived from `redb_bytes + tantivy_bytes` per index (always present, not gated by `include_data_size`)
- Helps operators identify bloated indexes without hitting `/_admin/memory`

---

### Stage 2d — Co-Locate Writer Pinning with Worker Placement ✅ Done

**Risk:** Low | **LOC:** ~15 | **Prerequisite:** Stage 2a

**Goal:** Ensure the writer thread for shard X lands on the same core as the worker that handles shard X's operations.

**Implementation (delivered):**
- In `NodeOrchestrator::spawn_worker_pool`, when `shard_affine_dispatch && writer_core_affinity` are both enabled, force `worker_count = cpu_cores`.
- Worker and writer both derive from the shard's dense ordinal, so for any shard S the worker handling S dispatches into the writer pinned on the matching core.
- Tokio worker tasks aren't OS-pinned, but the scheduler keeps frequently-running tasks near their last core under sustained load — co-locating dispatch with the writer thread maximizes that locality.
- Behind a config gate: default behavior (either flag off) preserves the existing `min(local_shards * 2, cpu_cores * 2)` worker sizing.

**Superseded (2026-08-08):** originally hashed — `xxh3(shard_id) % worker_count` against
`xxh3(shard_id) % num_cores`. Both sides agreed, but the hash domain is the shard set, which
is smaller than the core count, so it collided: measured with the shipped defaults (4 shards,
8 cores), 40 affine writes reached 3 of 8 workers and two shards' writers shared a core.
Replaced by `ShardPlacement`, which assigns a dense ordinal per shard. Same guarantee, no
collisions — the same run now reaches 4 of 4 possible workers, one writer per core.

---

### Stage 2e — Per-Worker Single-Thread Runtimes ✅ Done

**Risk:** Medium | **LOC:** ~70 | **Prerequisite:** Stages 2a + 2d

**Goal:** Convert workers from `tokio::spawn` on main runtime to dedicated `current_thread` runtimes pinned per core — completing the thread-per-core model for the write hot path.

**Implementation (delivered):**
- Extracted worker body into `orchestrator_worker_loop` helper (one body, two spawn paths).
- New config flag `[storage].worker_core_affinity` (default: `false`). Requires `shard_affine_dispatch` AND `writer_core_affinity` to take effect; otherwise silently no-op.
- When all three flags are on, `spawn_worker_pool`:
  - Sizes `worker_count = num_cores` (inherited from Stage 2d alignment).
  - Spawns each worker as a dedicated `std::thread::Builder` thread named `orch-worker-N`.
  - Pins the OS thread to `CoreLayout::core_for(worker_id)` via `core_affinity::set_for_current`.
  - Runs an isolated `tokio::runtime::Builder::new_current_thread()` runtime with `max_blocking_threads(4)` (kept tiny because search delegates to the shared `read_runtime` and writes go through the pinned writer thread).
  - Falls back gracefully on macOS / when pinning fails (logged, runs unpinned on a dedicated thread).
- `NodeOrchestrator.worker_threads: Vec<std::thread::JoinHandle<()>>` stores handles; `shutdown_worker_pool` sends shutdown messages then joins them via `spawn_blocking`.

**Why minimal:**
- No new `[runtime]` config section — just one boolean. A `CoreLayout` now exists, but only as the single source of which cores this process may use (`get_core_ids()` reconciled with `available_parallelism()`, so a cgroup CPU quota cannot make worker sizing and pin targets count different cores). Splitting it into reserved / per-shard / read-pool sets is still deferred — that is Stage 2f.2's work.
- No changes to `OrchestratorJob`, `OrchestratorWorkerTx`, `OrchestratorEngine`, `RouterActor`, `MicroshardActor`, or `engine.execute()` body — they work identically across both runtimes.
- The shared `read_runtime` continues handling all heavy I/O, preserving search throughput.

**Wakeup math:**
- Default mode: router-task → mpsc → worker-task → channel → writer-thread (cross-core wakeup if worker scheduled away from writer's pinned core).
- Pinned mode: router-task → mpsc cross-runtime → worker-thread (pinned core C) → channel → writer-thread (pinned core C) — second hop becomes a same-core mpsc push (no wakeup syscall). Cache locality wins for schema cache, routing ring, and shard map.

**Edge cases handled:**
1. Broadcast/scatter — `affinity_shard = None`, falls through to round-robin send across pinned workers.
2. Dynamic shard creation — workers already cover all cores; the new shard takes the next ordinal, which determines its worker.
3. `current_thread` runtime — fine because the worker only awaits channels and delegates blocking work elsewhere.
4. Shutdown — JoinHandles ensure runtimes drop before the orchestrator returns.

---

### Pinning, verified against `/proc` ✅ Done

Shard placement was reworked 2026-08-08: dense ordinals replace `xxh3(shard_id) % n` on both
the dispatch and the writer-pinning sides, and a single `CoreLayout` reconciles
`get_core_ids()` with `available_parallelism()`. `/_admin/workers` reports the pin *outcome*
per worker and per shard, not the request.

**Verified on Linux (aarch64 container, 8 cores) 2026-08-08: 8/8 workers pinned to their
target cores and all four writer threads to cores 0–3, confirmed independently against
`Cpus_allowed_list` in `/proc/<pid>/task/*/status` — one CPU per worker thread, one per
writer, no collisions.** Pinning is a no-op on macOS, so it must be validated on Linux; the
whole suite passes there too.

That the placement is correct is not an argument that it pays: Stages 2d and 2e cost
throughput rather than gaining it, twice measured, and both flags stay `false`. See
[The affinity flags, measured](#the-affinity-flags-measured) and
[Worker concurrency, measured](#worker-concurrency-measured).

### Worker and dispatch observability, for Stage 2a ✅ Done

**Implementation Summary:**
- **Per-worker atomic counters**: Added `WorkerCounters` struct with `queue_depth` (AtomicUsize) and `jobs_completed` (AtomicU64) to track per-worker queue state and throughput.
- **Dispatch-level counters**: Added `DispatchCounters` struct tracking `affine_sends`, `affine_full_fallbacks`, `round_robin_sends`, and `actor_mailbox_fallbacks` (all AtomicU64) to measure dispatch behavior.
- **Counter wiring**: Integrated counters into `OrchestratorWorkerTx::try_send` and `try_send_affine` to increment on send, and into `orchestrator_worker_loop` to decrement queue depth and increment jobs completed on receive.
- **Snapshot API**: Added `OrchestratorWorkerTx::snapshot()` method to generate `WorkerPoolReport` with per-worker stats (id, core_id, queue_depth, queue_capacity, jobs_completed) and dispatch metrics.
- **RouterActor integration**: Added `RouterActor::admin_worker_stats()` method to expose worker pool stats via direct method call (no kameo message routing needed for this admin endpoint).
- **HTTP endpoint**: Added `GET /_admin/workers` route and handler in `http_server.rs` returning JSON `WorkerPoolReport`.
- **Client SDK**: Added `admin_worker_stats()` method in `crates/client/src/sdk.rs` with corresponding response structs (`AdminWorkersResponse`, `WorkerStatsResponse`, `DispatchStatsResponse`).
- **CLI integration**: Added `AdminCommand::Workers` variant and dispatch handling in both command-line and interactive REPL modes, with tab-completion support and help text updates.

**Usage:**
- HTTP: `GET /_admin/workers` returns JSON with worker pool state and dispatch metrics
- CLI: `cameodb admin workers` displays the same stats in formatted JSON
- REPL: `admin workers` command in interactive shell

### Stage 2f — CPU Arenas & Per-Arena Jemalloc Stats ◐ Partial

**Risk:** Medium | **LOC:** ~250 | **Prerequisite:** Stage 2e, plus a latency harness for the parts whose value is unproven

2f.1 is below; 2f.2 and 2f.3 are in [Part I, section B](#b-phase-13--stage-2f--partial).

#### 2f.1 — Tantivy Merge Thread Control ✅ Done

- Merge thread count is configurable via `StorageConfig.merge_num_threads` (default: **2**)
- Implemented via `tantivy::indexer::IndexWriterOptions::builder()` with explicit `num_merge_threads()`
- Replaces Tantivy's default of 4 merge threads, preventing mmap storms on memory-constrained nodes. Two rather than one is deliberate: it leaves headroom to merge in parallel under load instead of serialising compaction behind a single thread
- Note the count is **per open index**, so merge threads scale with how many indices are open, not with shard count

### Phase 13 Execution Order & Risk Matrix

| Order | Stage | Risk | LOC | Prerequisite | Gain |
|-------|-------|------|-----|-------------|------|
| **1** | 2a: Shard-affine dispatch | Low | ~50 | None | Eliminates 1 cross-core wakeup/write |
| **2** | 2b: Extract memory module | Low | ~200 | None | Maintainability, testability |
| **3** | 2c: Auto-purge + per-index memory | Low | ~70 | 2b | Operational safety, observability |
| **4** | 2d: Co-locate writer pinning | Low | ~10 | 2a | Full core co-location |
| **5** | 2e: Per-shard single-thread rt | Medium | ~150 | 2a+2d | True thread-per-core |
| **6** | 2f: CPU arenas + per-arena stats | Medium | ~250 | 2e + harness | Merge threads stop sharing the writer's core; diagnostics (2f.1 done; 2f.2 analysed, 2f.3 planned) |

**Success Metrics:**
- Write p99 latency reduced by 20-40% under high concurrent load
- Cache miss rate reduced on shard-specific data structures
- No degradation in throughput for broadcast/scatter operations
- Clean rollback path via config flags at each stage
- Memory module independently testable with unit tests
- Auto-purge prevents RSS creep under sustained writes

## Phase 14 — Security Hardening

◐ **Partial** — A1–A5, B1–B3, C1 and C2 are here; Stage C3 and the deferred complexity caps
are in [Part I, section C](#c-phase-14--security-hardening--partial).

**Objective**: Close the security gaps identified in the code security review (2026-07-30). The remaining critical gap is that CameoDB has **no authentication and no authorization** — every HTTP and MCP endpoint is open. TLS (B2), index-name validation (A1), and CORS wiring (A2) are done. This phase turns CameoDB from a trusted-LAN-only system into one that can be safely exposed to untrusted networks.

**Current state (verified by audit):**
- ✅ No hardcoded secrets, no command execution, no regex/ReDoS surface, no SSRF
- ✅ libp2p cluster transport already uses Noise encryption
- ⚠️ All HTTP/MCP endpoints unauthenticated (write, delete, admin included) — the one remaining critical gap. `/_admin/*` can now be removed entirely with `admin_enabled = false`, and the `external` profile refuses to start until B1 lands
- ✅ Index names validated at creation and resolved through `HybridStore::index_dir()`, which rejects any name that is not a single path component (Stage A1)
- ✅ `cors_allowed_origins` wired into the router with fail-fast validation; default is now `[]` (no cross-origin access) and `"*"` is local-only (Stage A2)
- ✅ TLS on HTTP via rustls (Stage B2), verified serving; default bind is now `127.0.0.1:9480` and a reachable bind requires a declared security profile
- ✅ Cluster join gated by an optional PSK; required by the `internal` and `external` profiles
- ✅ Wire-level body limit, per-record cap, request timeout, and concurrency shedding, all verified live by `scripts/validate/posture.sh`
- ✅ `CAMEODB_ACCEPT_INVALID_CERTS` removed entirely; replaced with per-command `--insecure` flag

### Execution Order (impact-per-effort ranked)

| Order | Stage | Effort | Impact | Risk if unfixed |
|-------|-------|--------|--------|-----------------|
| **1** | A1: Index name validation | ✅ Done | Critical | Arbitrary dir deletion (RCE-adjacent) |
| **2** | A2: CORS config wiring | ✅ Done | High | Drive-by browser attacks on local instances |
| **3** | A3: `ACCEPT_INVALID_CERTS` removal | ✅ Done | Medium | Accidental TLS bypass |
| **4** | A4: Body limits + concurrency caps | ✅ Done | High | Memory DoS / decompression bomb |
| **5** | A5: Security tooling (`cargo audit`, `cargo deny`) | ✅ Done (manual) | Medium | Silent vulnerable deps |
| **6** | B1: API key authentication + index scoping | ✅ Done | Critical | Was full unauthenticated R/W/D access |
| **7** | B2: HTTPS/TLS via rustls | ✅ Done | High | Traffic interception |
| **8** | B3: Cluster join secret (PSK) | ✅ Done | High | Rogue node data access |
| **9** | C1: MCP rate limiting + query complexity | ✅ Done (caps deferred) | Medium | Agent-driven resource exhaustion |
| **10** | C2: Audit logging | ✅ Done | Medium | No forensic trail |
| **11** | C3: Per-index role overrides | ~2 days (was ~5+), **the only stage left** | Medium | Multi-tenant isolation |

The B1 estimate is up from the original ~3–5 days for two reasons, both decided deliberately
(see B1 below): index scoping applies to **every** role rather than read-only keys, and MCP
enforcement reaches per-tool and per-index rather than stopping at the path. The second is
why C3 drops — most of what it described is B1's scoping mechanism, leaving only per-index
*overrides* on top of it.

### Stage A — Quick Wins (no protocol changes)

**A1 — Index Name Validation** ✅ Done
- Two-tier approach at the HTTP boundary (`http_server.rs`):
  1. **Index creation** (`PUT /api/{index}/_config`): `validate_index_name()` rejects `..`, path separators, empty, length > 255, non-alphanumeric first character, and anything outside `[A-Za-z0-9_.-]`. This is the only route where a new name enters the system.
  2. **Delete** (`DELETE /api/{index}`): requires the index to exist; returns 404 when absent and 500 when the lookup itself fails
- Defense-in-depth at the storage boundary: `HybridStore::index_dir()` resolves every caller-supplied name and rejects anything that is not a single normal path component. The check is **lexical**, not `canonicalize()`-based, so it also holds for indexes that do not exist yet — the case where a traversal name would otherwise reach `create_dir_all` and escape the shard. Applied to `get_or_create_index` (creates dirs), `delete_index_data` (removes dirs, validated before any mutation), and both `Index::open_in_dir` slow paths.
- Tests: 7 unit tests on `validate_index_name`, 3 on `resolve_index_dir`, plus an end-to-end test that drives the real write and delete paths with `../victim`, `..`, `../../etc`, and `a/b` and asserts nothing outside the shard is created or removed

**A2 — Wire CORS Config** ✅ Done
- ✅ Replaced hardcoded `CorsLayer::permissive()` with origins from `network.http.cors_allowed_origins`, threaded through `create_router`
- ✅ Explicit methods (`GET/POST/PUT/PATCH/DELETE`) and headers (`Content-Type`, `Authorization`) for the non-wildcard path
- ✅ Credentials are never combined with a wildcard origin (`permissive()` does not set them)
- ✅ Fail-fast validation in `CameoDbConfig::validate()`: rejects an empty list, `"*"` mixed with specific origins, origins that are not valid header values, and origins without a scheme — a typo can no longer degrade silently into deny-all
- ✅ Effective policy is logged at startup (`warn!` for wildcard, `info!` with the origin list otherwise)
- ✅ Default is now `[]` — no cross-origin browser access. CORS governs browsers only, so this costs API and MCP clients nothing while removing the drive-by surface that mattered precisely because no endpoint requires auth
- ✅ `"*"` is accepted only under the `local` profile; `internal` and `external` reject it
- ✅ `mcp-session-id` and `accept` are allowed request headers and `mcp-session-id` is exposed, so restricting origins no longer breaks browser-based MCP clients — a collision between this stage and Phase 12 that the original change introduced

**A3 — TLS Bypass Handling** ✅ Done
- Removed `CAMEODB_ACCEPT_INVALID_CERTS` environment variable entirely
- Replaced with `--insecure` flag: per-command for single operations, per-session for interactive REPL
- No global TLS bypass via environment variables; must be explicitly requested via CLI flag

**A4 — DoS Hardening** ✅ Done (re-done; first attempt did not hold)
- ✅ Lowered default `max_record_size_mb` from 512MB → 64MB; all derived limits (HTTP body, Kameo remote messaging, request timeout) scale accordingly
- ✅ Added `max_concurrent_requests` to `HttpConfig` (default: 128) with CLI/env override (`--max-concurrent-requests` / `CAMEODB_MAX_CONCURRENT_REQUESTS`); semaphore-based concurrency guard middleware rejects excess requests with HTTP 503
- ✅ `DefaultBodyLimit` after `DecompressionLayer` so compression bombs are measured expanded
- ✅ `RequestBodyLimitLayer` counts bytes on the wire. **The earlier claim that a second `DefaultBodyLimit` capped raw wire bytes was wrong**: `DefaultBodyLimit` is an extractor-level limit, so handlers taking a raw `Body` — the NDJSON streaming ingest path — were unbounded. A 150 MB single-line request under a 1 MB configured limit was accepted and drove RSS from 44 MB to 889 MB
- ✅ Per-record cap inside `write_stream_handler`: an unterminated line can no longer buffer the whole request allowance
- ✅ `TimeoutLayer` wired to `effective_request_timeout_secs()`. **`request_timeout_secs` was previously never applied to HTTP at all**, so the concurrency guard made a DoS *cheaper*: four trickle uploads at 300 B/s held every permit indefinitely and took the node offline, health check included
- ✅ `/_cluster/health` exempted from the concurrency guard; 503 responses carry `Retry-After`
- ✅ Config validation rejects `max_concurrent_requests = 0`; posture rules bound concurrency × body size jointly
- ✅ Verified by `scripts/validate/posture.sh` (413 on both limit paths, 408 at the configured timeout, health available while saturated)

**A5 — Security Tooling** ✅ Done (manual, by design)
- ✅ `cargo audit` installed (v0.22.2), runs clean — 0 vulnerabilities across 588 dependencies
- ✅ `cargo-deny` installed (v0.20.2) with `deny.toml` covering advisories, bans (wildcard deny, duplicate warn), licenses (permissive allowlist, copyleft deny), and sources (crates.io only)
- ✅ Fixed wildcard path dependencies in `server` and `client` Cargo.toml (added explicit version constraints)
- ✅ Fixed unparseable `FSL-1.1-Apache-2.0` license fields → `Apache-2.0` (valid SPDX; actual FSL license file remains in repo)
- ✅ Documented 3 transitive advisories from libp2p 0.56.0 (hickory-proto vulnerabilities + unmaintained `paste`) with ignore reasons — no upstream fix available yet
- ✅ `scripts/validate/deps.sh` runs `cargo fmt --check`, `cargo clippy -D warnings`, `cargo audit`, and `cargo deny check`
- ✅ Advisory exceptions carry `review-by` dates; the script fails once one expires, so an exception cannot quietly outlive its justification
- ✅ Added `CDLA-Permissive-2.0` to the licence allowlist (Mozilla CA bundle via `rustls-platform-verifier`), reviewed as a permissive data licence
- **No CI by decision.** Execution is manual; [RELEASE-CHECKLIST.md](RELEASE-CHECKLIST.md) is the record

### Stage B — Core Auth & Transport Security (the "auth project")

**B1 — API Key Authentication with Capability and Index Scoping** ✅ Done · was Critical

Design agreed 2026-08-08. This replaces an earlier sketch whose route matrix did not match
the router that exists — it named `POST /api/{index}/write`, `POST /api/{index}/bulk`, and
`GET /api/{index}/search`, none of which are real paths, and omitted four routes entirely
including the streaming ingest path that Stage A4 had already had to fix once. The table
below is transcribed from `create_router` and is guarded by a test rather than by review.

*Capabilities, not roles, are what routes require.* Roles are bundles of capabilities, so
the route table stays role-agnostic and C3 can add per-index overrides without touching it.

| Capability | Covers |
|------------|--------|
| `Read` | search, streaming search, read config, list indexes |
| `Write` | document write, streaming ingest, bulk |
| `IndexAdmin` | create index, schema evolution, delete index |
| `NodeAdmin` | `/_admin/*` — memory, purge, workers, commit, evict-writer |

`admin` = all four · `writer` = Read + Write · `reader` = Read.

Renamed from the earlier `user` / `restricted`: those two were not on the same axis, and
"restricted" was described as read-only *MCP* access while the same sketch also granted it
HTTP search. Nothing has shipped with the old names.

- **Transport**: `Authorization: Bearer <key>`, header-only. A key in a query parameter is a
  non-goal — it lands in access logs and `Referer` headers.
- **Config** — entry-level `key_hash` or `key_hash_file`, the exact `psk` / `psk_file`
  analogue from B3: inline wins, the file is permission-checked, world-readable warns.
  ```toml
  [security]
  enabled = false                        # off by default; the posture rules decide if that is allowed

  [[security.api_keys]]
  key_hash = "sha256:3f9a…"              # or: key_hash_file = "/etc/cameodb/keys/ops"
  role  = "admin"
  label = "ops-team"                     # audit identity, not a secret

  [[security.api_keys]]
  key_hash_file = "/etc/cameodb/keys/team-a"
  role  = "writer"
  label = "team-a"
  allowed_indexes = ["docs", "wiki"]     # honored for every role; omitted = all indexes
  ```
- **The config never holds a usable credential.** `cameodb keygen --role <r> [--label <l>]
  [--allowed-indexes a,b]` mints a key, prints it once, and prints the stanza to paste.
- **Key format is enforced at authentication time**: a presented token must match
  `cameo_v1_<43 base64url chars>` before it is hashed. This is what makes an unsalted
  SHA-256 defensible — a hand-chosen passphrase can never authenticate even if someone
  pastes its digest into the config. Verification hashes the token and compares digests with
  `subtle::ConstantTimeEq` across all entries; `sha2`, `subtle`, `zeroize`, `hex`, and `rand`
  are already in `Cargo.lock` transitively, so `cargo deny` and `cargo audit` see nothing new.
- **Secrets follow the `ClusterPsk` precedent**: redacted `Debug`, never serialized, scrubbed
  on drop. `key_id` (first 8 hex of the digest) plus `label` are the log identity; the key
  itself never reaches a log line.
- **Env overrides**: `CAMEODB_SECURITY_ENABLED`, `CAMEODB_API_KEY_HASH`, `CAMEODB_API_KEY_ROLE`
  for the single-key case. Note the earlier sketch gave the *server* `CAMEODB_API_KEY` — that
  is a plaintext key, which contradicts hash-only storage, and it collides with the name the
  *client* needs the moment both run in one compose file. `CAMEODB_API_KEY` is client-only.
- **Backward compatibility**: auth off by default. The earlier sketch also wanted a fail-fast
  when `bind = 0.0.0.0` without auth; dropped, because the posture rules already answer that
  question per profile (Warn under `internal`, Fail under `external`). Two mechanisms
  disagreeing about one condition is how this rots.

- **Route table — deny by default.** Classification lives in one table keyed by (method, path
  pattern). The middleware runs *before* routing, extracts the index segment lexically, and
  enforces capability and scope centrally, so no handler can forget to check.

  | Route | Requires | Index-scoped |
  |-------|----------|--------------|
  | `GET /_cluster/health` | public (minimal body) / `Read` (full body) | — |
  | `POST /api/{index}/search` | `Read` | yes |
  | `POST /api/{index}/search/stream` | `Read` | yes |
  | `GET /api/{index}/_config` | `Read` | yes |
  | `GET /_indexes` | `Read` | **filtered** |
  | `GET /_cluster/_indexes` | `Read` | **filtered** |
  | `PUT /api/{index}/document` | `Write` | yes |
  | `POST /api/{index}/document/stream` | `Write` | yes |
  | `POST /api/{index}/_bulk` | `Write` | yes |
  | `PUT /api/{index}/_config` | `IndexAdmin` | yes |
  | `PATCH /api/{index}/_schema` | `IndexAdmin` | yes |
  | `DELETE /api/{index}` | `IndexAdmin` | yes |
  | `GET /_admin/memory`, `POST /_admin/memory/purge`, `GET /_admin/workers` | `NodeAdmin` | — |
  | `POST /_admin/index/{index}/commit`, `POST …/evict-writer` | `NodeAdmin` | yes |
  | `POST\|GET\|DELETE /mcp/*` | `Read` + per-tool check inside | inside |
  | anything else (fallback) | **deny** | — |

  Consequences accepted deliberately: an unknown path answers **401 without a key and 404
  with one**, since auth precedes routing — which also stops path-existence probing. Named
  access to a disallowed index is **403**, while *listing* filters silently: asking by name
  deserves an honest answer, enumeration does not.

  Completeness is guarded by a test that `include_str!`s `http_server.rs`, extracts every
  `.route("…")` literal, and fails if any lacks a classification. A new route cannot ship
  unclassified, which a hand-maintained matrix could not promise.

- **Layer placement** in the existing stack:
  ```
  TraceLayer → CORS → AUTH → Timeout → ConcurrencyGuard → wire body limit
    → Decompression → extractor limit → Compression → routes
  ```
  Inside CORS, so browser preflight `OPTIONS` — which never carries `Authorization` — still
  gets its headers. Outside the concurrency guard and the body limits, so a 401 flood neither
  takes a semaphore permit nor gets its body buffered; `/_cluster/health` is exempted the way
  the guard already exempts it. Accepted cost: rejecting before the body is read means hyper
  drops the connection instead of reusing it.

- **MCP enforcement reaches the tool, not just the path.** `/mcp` is a single JSON-RPC
  endpoint, so path-level middleware cannot see which tool or index is in play.
  - New `McpAuthz` trait **in the mcp crate** (`allows_index`, `has(Capability)`, `key_id`),
    implemented by the server's auth context, so identity threads router → dispatch →
    `McpBackend` without the mcp crate learning any server types.
  - `tool_capability(name) -> Option<Capability>` with a deny default, so the day a write
    tool is added it fails closed instead of inheriting `Read`.
  - `list_indexes` filters to the caller's scope; `search_index` 403s a named disallowed
    index; `search_across_indexes` 403s rather than silently returning partial results.
  - Auth enforced on the GET (SSE) and DELETE session routes too, not only the POST. Sessions
    record the creating `key_id` and reject a request presenting a different key.

- **Client SDK + CLI**: `--api-key`, `--api-key-file`, `CAMEODB_API_KEY`; precedence inline >
  file > env, matching the server's PSK convention. `--api-key` is documented as `ps`-visible.
  The client **refuses to send a key to a plaintext non-loopback URL** unless `--insecure`,
  and in the REPL `connect <different-origin>` **drops** the key rather than forwarding it —
  the same failure the `TlsTrust` split already had to fix once.

- **Posture rules** — the stubbed `auth` check becomes evaluated:

  | Condition | Outcome |
  |-----------|---------|
  | enabled + ≥1 key | Pass — *N keys: 1 admin, 2 writer, …* |
  | `external` + disabled | **Fail** (unchanged) |
  | `internal` + disabled | Warn (unchanged wording) |
  | `local` + disabled | Pass — "unauthenticated (loopback only)", mirroring how `tls` passes plaintext for `local`. A profile that warns on every boot teaches operators to ignore warnings |
  | enabled + **0 keys** | **Fail** — every request would 401; fail loudly, not silently |
  | enabled + no key holding `Write`/`IndexAdmin` | Warn — read-only node |
  | enabled + TLS off + non-loopback bind | Warn under `internal` (tokens in the clear); `external` already fails on `tls` |
  | `admin_api` rule | "reachable off-box and unauthenticated" becomes Pass once auth and an admin key exist |

- **Trust boundary, stated so it is not assumed away**: enforcement is at the HTTP/MCP
  ingress, where identity exists. Peer-to-peer traffic is kameo-over-libp2p and is trusted by
  the B3 PSK, so **index scoping is not a defense against a rogue cluster member**. This also
  corrects the earlier C3 sketch, which proposed enforcing at the `RouterActor` boundary —
  that boundary is driven by peers as well as by HTTP, where no API key exists.

- **Non-goals, recorded so they are not re-litigated**: no lockout or throttle on failed auth
  (against a 256-bit key it buys nothing and is itself a DoS lever — count and log for C2);
  no general hot config reload — the key ring alone is hot-swappable since
  `POST /_admin/keys/reload` and SIGHUP landed; tenants, limits and audit still bind at
  startup and a reload reports changes to them as not applied.

- **Order of work**:
  1. ✅ **Landed 2026-08-08.** `[security]` config + key types + `keygen` + posture rules +
     `check-config`, with no enforcement. `crates/server/src/auth.rs` holds the whole
     credential model: `Capability` / `Role` bundles, `ApiKey` (redacted `Debug`, zeroized on
     drop, minted from `getrandom`), `KeyDigest` (constant-time `PartialEq`), and `KeyRing`
     with the shape gate in front of the hash. Two deviations from the sketch above, both
     deliberate:
     - The `auth` posture rule reports the configured keys but **still fails `external`**,
       because the middleware does not exist yet. A posture that claimed a guarantee the
       router does not make is the one failure mode this module exists to prevent, so the
       `external` Fail and the `admin_api` wording flip in step 2, not here.
     - `key_hash_file` warns when it is **writable** by group or others, not when it is
       readable. A digest is not a secret, so a readable hash file is not a leak — but a
       writable one lets anyone mint themselves a role, which is worse than the case
       `psk_file` warns about.
  2. ✅ **Landed 2026-08-08.** `crates/server/src/authz.rs`: the route table, the
     `classify` matcher, and the `authorize` middleware, mounted inside CORS and outside the
     timeout, the concurrency guard and both body limits. Deny by default — an unclassified
     path needs a key like any other. Health now answers an anonymous caller with liveness
     alone. Both posture outcomes step 1 left pending are flipped, so `external` starts for
     the first time. Beyond the sketch:
     - **Index scoping for named routes landed here too**, not in step 3. The middleware
       already had the `{index}` segment in hand, and shipping a `allowed_indexes` setting
       that parsed but did nothing would have told operators their key was scoped when it
       was not. What remains for step 3 is *list filtering*, which is a handler change.
     - **An index-scoped key is refused at `/mcp`.** MCP is one JSON-RPC path, so the scope
       cannot be enforced from outside it until step 4. Refusing beats letting a scoped key
       read every index through the side door.
     - `scripts/validate/auth.sh` (56 checks) landed with it rather than waiting for step 6:
       a middleware in the wrong place in the layer stack passes every unit test there is.
  3. ✅ **Landed 2026-08-08.** `filter_index_listing` in `authz.rs` narrows a listing to
     the caller's scope, applied by `/_indexes` and `/_cluster/_indexes`. The cluster
     response repeats every index name under each node that answered, with its own count, so
     the filter recurses and rewrites both — a top-level-only filter would have leaked the
     same names one level down. An entry whose shape it does not recognise is **dropped**:
     if the listing changes underneath it, the failure has to be a missing row, not a leak.
  4. ✅ **Landed 2026-08-08.** `McpAuthz` / `McpCapability` / `McpAuthzRef` in the mcp
     crate, implemented by the server for `Authz`, so identity reaches the dispatcher without
     the mcp crate learning a server type. `tool_capability` denies by default and is held to
     `mcp_tools()` by a completeness test. `call_tool` checks the capability *before* parsing
     arguments, then the named index; `search_across_indexes` refuses the whole call rather than
     narrowing it, because partial results that look complete are worse than an error.
     Sessions are bound to the `key_id` that created them on all three verbs. The `/mcp`
     refusal for index-scoped keys is gone, and with it the posture note that advertised it.
     Two deviations from the sketch:
     - **Backend methods take the caller only where they enumerate.** Methods that *name*
       their index are checked once in `call_tool`; `list_indexes`, `get_catalog_stats`,
       `list_resources` and `read_resource` take an `McpAuthzRef`, because only the
       implementation knows which part of its response is a list of index names.
     - **`read_resource` checks the scope itself.** A URI like
       `cameodb://indexes/payroll/schema` is a read of `payroll`, and only the host knows
       that. Not being *offered* a URI is not the same as being refused it, so both are
       tested.
  5. ✅ **Landed 2026-08-08**, taken before steps 3–4: with authentication enforced but no
     way for `cameodb client` to present a key, enabling `[security]` locked an operator out
     of their own tooling, which is the gap most likely to be hit first. `Credential` in
     `crates/client/src/sdk.rs` mirrors the server's `ApiKey` (redacted `Debug`, zeroized on
     drop, `key_id` fingerprint), and the key rides in the `http` client's default headers —
     so every existing call site carries it and none can be forgotten — while `source_http`
     is built without it, keeping the database key off requests to third-party data sources.
     Four deviations from the sketch above:
     - **Precedence is file > inline, not inline > file.** clap resolves each flag against
       its own environment variable first, so the remaining question was only which of the
       two wins. Preferring the file means a stale `CAMEODB_API_KEY` left exported in a shell
       cannot silently override the key a command names explicitly.
     - **The plaintext gate is its own flag, `--allow-plaintext-key`, not `--insecure`.**
       `--insecure` accepts a bad certificate on a connection that is still encrypted; this
       puts a bearer token on the wire in the clear. Folding them together would repeat the
       exact mistake the `TlsTrust` split was made to fix. Loopback is exempt, which is what
       keeps the single-node default usable without any flag.
     - **`HealthResponse` had to be made partial.** Step 2 shrank the anonymous health body
       to `status` alone, but the client's struct still required `node_id` and
       `active_shards` — so an anonymous 200 would have failed to *parse*. A shrunk response
       is only safe once every reader of it tolerates the shrink.
     - **A latent bug surfaced**: the SDK asked for `/_admin/index/{index}/evict_writer`
       while the route has always been `evict-writer`, so that command had never worked. With
       authentication in front of the router its 404 would have become a 401 — an unrelated
       bug wearing an auth costume. Fixed, and `auth.sh` now drives the command end to end.
  6a. ✅ **Landed 2026-08-08.** Hardening found by auditing what steps 1–5 left:
     - `keygen --key-out` / `--hash-out` write the two files the design already assumed an
       operator would create by hand — `0600`, `create_new` so neither is ever overwritten,
       and files written before anything is printed. Closes the loop between `keygen`,
       `key_hash_file` and `--api-key-file`.
     - `Authz::Anonymous` now **denies** in both places it was permissive (the `McpAuthz`
       impl and index-listing filter). Unreachable today because no MCP or listing route is
       `Public` — which is why it must not be the permissive branch, since reclassifying one
       later would silently open it.
     - The unauthenticated-refusal log is thinned to the first few, then powers of two, then
       every hundred thousand. It was one `warn!` per request, so anyone who could reach the
       port could fill the disk with a loop. A 403 still gets a line each: it needs a valid
       key first, so its volume is bounded by someone who already holds credentials.
     - `tools/list` is filtered by capability, and a tool with no row is not advertised —
       the deny default applies to the catalogue as much as to the call.
     - REPL `key file <path>` / `key <api-key>` / `key show` / `key clear`, so `connect`
       dropping a key is no longer a dead end that needs a restart.
     - The client says which credential won when both `--api-key` and `--api-key-file` are
       given, instead of silently preferring the file.
     - Fixed a stale line in `keygen`'s own guidance claiming requests were not yet checked.
  6b. ✅ **Landed 2026-08-08.** The docs tree had never mentioned authentication; only the
     README had. Added `## Security and Posture` to `docs/CONFIGURATION.md` (profiles,
     `[security]`, roles × capabilities, key minting, file modes, rotation, and the cluster
     PSK, which was also undocumented), a `### Authentication` section to
     `docs/API_REFERENCE.md` with the capability required per endpoint and what 401 vs 403
     mean, `## Securing a Deployment` to `docs/DEPLOYMENT.md`, a `## Security` section to
     `docker/README.md`, commented key material in the docker config, compose file and
     systemd unit, and the CHANGELOG entry. Two **examples that could not start** were fixed
     rather than documented around:
     - `docker/cameodb-docker.toml` declared no `profile` while binding `0.0.0.0`, so it
       failed the posture gate the previous commit added. Now `internal` — what a published
       container port actually is — with `cors_allowed_origins = []` to match.
     - The recommended production config in `docs/CONFIGURATION.md` had the same problem and
       enabled the cluster with no PSK. Rewritten as an `external` node with TLS, two keys and
       a PSK, then verified to pass `check-config` with zero warnings.
     The systemd unit gained `ExecStartPre=cameodb check-config`, so a node refuses to start
     in a posture its config does not satisfy before the port ever opens.

- **`scripts/validate/auth.sh` proves** (111 checks, in `all.sh`): 401 on every classified
  route bare · 403 per wrong role per capability class · preflight passes without a key ·
  unknown path 401 → 404 · health minimal vs full · scoped key allowed / denied, including
  against a percent-encoded index name · an unauthenticated flood does not shed
  authenticated requests (which is what proves the layer order) · no key in any log line,
  and `key_id` in place of one · `check-config` fails `external` + auth-off and passes
  `external` + auth-on + TLS · the bundled client authenticating from a flag, a file and the
  environment, refusing a malformed key before sending it, refusing to carry one to a
  non-loopback plaintext host, and explaining a 401 and a 403 differently · `/_indexes` and
  `/_cluster/_indexes` filtered to a key's scope, count included · every MCP tool that names
  an index refused off-scope, the catalog and the resource list filtered, a resource URI
  refused when read directly, an unknown tool refused rather than dispatched · an MCP session
  refused to any key but the one that opened it, on POST, GET and DELETE.

**B2 — HTTPS/TLS via rustls** ✅ Done (the first implementation never ran)
- **The original implementation panicked on every TLS startup** and was marked complete without a single HTTPS request being served. `axum-server/tls-rustls` force-enables `rustls/aws-lc-rs` while libp2p-quic enables `rustls/ring`; rustls 0.23 refuses to pick between two providers, and the panic landed *after* the startup banner, so it read as a healthy boot
- Fixed by using `axum-server/tls-rustls-no-provider` and installing `ring` explicitly at the top of `main`, on both the server and client paths
- TLS material is now loaded before storage init and before the banner, so bad certificates fail early and legibly
- Graceful shutdown under TLS via `axum_server::Handle`; previously the drain signal only reached the plaintext listener and every TLS shutdown burned the full 10 s timeout before cutting in-flight requests
- Implemented axum-server with rustls for HTTPS support; config `[network.http.tls] enabled, cert_file, key_file`
- Added TLS validation to config (cert/key file existence, required fields when enabled)
- Client-side: added `--insecure` flag for accepting invalid TLS certificates (self-signed certs in development)
- Per-command `--insecure` for remote schema/data loading operations (fine-grained control)
- Removed `CAMEODB_ACCEPT_INVALID_CERTS` environment variable (simplified to flag-only interface)
- Documentation updated with TLS configuration, Linux system certificate paths, and security best practices
- Single TLS stack across the workspace: `reqwest/rustls-no-provider` replaced native-tls, verified against `dl.cameodb.com` and other real sources. `rustls-platform-verifier` uses the OS trust store, which is what native-tls provided and what a corporate CA needs. Vendored OpenSSL is gone from every build path
- Optional mTLS for client verification later

**B3 — Cluster Join Authentication** ✅ Done
- PSK for libp2p swarm via `pnet` (XSalsa20 private network encryption)
- Config `[network.cluster] psk` (inline hex string) and `psk_file` (path to file)
- CLI overrides: `--cluster-psk`, `--cluster-psk-file`; env: `CAMEODB_CLUSTER_PSK`, `CAMEODB_CLUSTER_PSK_FILE`
- When PSK is set, TCP is wrapped with PnetConfig and QUIC is disabled (pnet only supports TCP)
- PSK fingerprint logged at startup (not the key itself) for operational verification
- Config validation: warns if cluster enabled without PSK; validates hex format (64 chars = 32 bytes)
- Covers kameo remote messaging (all libp2p protocols are gated by the pnet handshake)
- Disabled by default (backward compatible); opt-in for production clusters
- ✅ Format validation lives in `load_psk()` alone; `validate()` calls the same path, so a config that validates is one the swarm can start with
- ✅ The key is held in a `ClusterPsk` newtype that redacts its `Debug`, is never serialized, and zeroizes on drop; `psk_file` permissions are checked and a world-readable file warns
- ✅ A PSK combined with a `/quic-v1` address is rejected at config time rather than failing as a dial error, since `pnet` wraps TCP only
- Wording corrected: PSK is a **membership gate**, not a confidentiality upgrade — the transport is already encrypted by Noise
- Future: PSK rotation with primary + secondary for zero-downtime rolling upgrades

### Stage C — Defense in Depth (post-auth)

**C1 — MCP-Specific Limits** ✅ Done (rate limiting; complexity caps deferred)
- ✅ **Rate limiting landed 2026-08-10.** `[security.limits]`, a token bucket per key, off by
  default. Enforced in the `tools/call` arm *before* the capability check, so a refusal does
  not leak which tools the key could otherwise call, and the budget is shared across tools
  because what it bounds is the node's cost, not any one tool's frequency. Metered by
  `key_id` rather than by session: a session id is chosen by the caller's host, so metering
  per session would let an agent reset its own limit by reconnecting. Policy lives in the
  server crate behind a `McpBackend` hook — the `mcp` crate keeps no deployment opinions,
  the same split B1 used for authorization. Nine tests, three of them end to end
  (`crates/server/tests/mcp_rate_limit.rs`)
- 💭 **Query complexity caps — deferred, not planned.** Max boolean clauses, max
  prefix-expansion terms, and wiring the existing per-request timeout into the MCP path.
  Judged unnecessary at this stage of the auth module: rate limiting already bounds what a
  key costs the node per unit time, which is the resource-exhaustion risk C1 was written
  for, and a per-request timeout already exists on the HTTP path. A single expensive query
  is a different and much narrower problem than a loop of ordinary ones. Revisit if a
  workload appears where one query — not a stream of them — is the threat; the two
  `parse_query_lenient` call sites in `crates/storage/src/lib.rs` are where a cap would go
- Per-key index scoping is covered in B1 (`allowed_indexes`, all roles), as is the failure counting this stage would rate-limit on

**C2 — Audit Logging** ✅ Done
- `[security.audit]`, off by default. The prediction that this stage would be "a sink rather
  than a re-plumbing" held: [`decide`](crates/server/src/authz.rs) already resolved `key_id`,
  `label`, `role` and index at one chokepoint, and nothing about that had to move
- **Detail for reads, totals for writes** — the one design decision that was *not* obvious.
  A knowledge base ingests far more than it retrieves (the working assumption is ~100k:1), so
  at the measured ~6 900 writes/s a record per write buries the handful of reads worth
  looking at. Writes fold into a per-key, per-index count flushed every `rollup_secs`; reads,
  MCP tool calls and admin actions keep a line each
- The same rule keeps the trail from being a DoS lever. A refusal of a **valid** key is
  listed — its volume is bounded by the credentials in circulation, and it is the shape of
  both a misconfiguration and a stolen key. A refusal of an *unidentified* caller is counted,
  because its volume is chosen by anyone who can reach the port. This is the same reasoning
  that already thins those `warn!` lines in `should_log_refusal`
- Off the request path entirely: a timestamp and a non-blocking hand-off to a dedicated OS
  thread — not a tokio task, so the trail keeps draining while the runtime is saturated,
  which is when it matters most. A full queue drops, counts, and writes a `gap` record naming
  the loss; silent loss would make the file lie about what it contains
- Two sinks: a bounded in-memory ring served by `GET /_admin/audit` (node-admin, and reading
  it is itself audited), and an optional rotating JSON Lines file. Also emitted on the
  `tracing` target `cameodb::audit`, so an existing log collector gets it for free
- `record_query_text` is off by default and documented as keeping *data*, not metadata: a
  search for a person's name records that name
- A redb table was considered and rejected — it would couple the trail to the storage engine
  being healthy, which is precisely when it is needed, and put a WAL fsync on the request path
- MCP needed its own hook (`McpBackend::record_tool_call`): from the HTTP layer every agent
  call is `POST /mcp`, and which tool and index are in play exist only inside the dispatcher.
  Same host-owns-the-policy split as B1 and C1 — the mcp crate keeps no deployment opinions
- 14 unit tests over the rollup, ring, rotation and drop accounting; 9 integration tests
  driving a real node with three keys (`crates/server/tests/audit_trail.rs`), including that
  no key — accepted or rejected — ever reaches the trail
- Costs nothing measurable on the write path: three repeats each way, audit off vs on with a
  file sink, and the arms overlap completely (see "What the audit trail costs, measured").
  The claim is bounded rather than absolute — that is three repeats on a laptop, and the read
  path, where a record really is serialized per request, was not measured
- 💭 **Not done, and deliberately:** no query interface beyond "most recent N", no
  retention policy beyond file rotation, no signing or tamper-evidence. The first is what a
  log collector is for; the last would need a threat model where the node itself is
  untrusted, which is not the one this stage was written against

**C3 — Per-Index Role Overrides** 📋 Planned — the only stage still open; it lives in [Part I, C1](#c1--per-index-role-overrides).

### TLS Inventory (verified 2026-08-07)

| Component | Current TLS | Notes |
|-----------|-------------|-------|
| HTTP server | ✅ rustls via axum-server | Implemented with `[network.http.tls]` config (enabled, cert_file, key_file) |
| Client SDK (`reqwest 0.13`) | ✅ rustls + `ring`, OS trust store via `rustls-platform-verifier` | No TLS feature flags; `--insecure` (server) and `--insecure-source` (data sources) are separate |
| musl static builds | ✅ rustls + `ring`; no vendored OpenSSL, no C toolchain | Image needs `ca-certificates`; verify per target with `scripts/validate/remote-sources.sh` |
| libp2p cluster transport | ✅ Noise (`noise::Config`) + yamux mux, optional `pnet` PSK | Noise provides confidentiality; the PSK gates membership (B3). QUIC is disabled when a PSK is set |
| kameo remote messaging | ✅ rides libp2p swarm | inherits Noise encryption and the B3 membership gate |
| Client TLS bypass | ✅ explicit flags only | `--insecure` (server connection) and `--insecure-source` (remote sources) are independent; no env-var bypass |

**Success Metrics:**
- No unauthenticated write/delete path reachable once `[security] enabled = true`
- Every route in `create_router` carries a capability classification, enforced by a test that
  reads the router's own source — an unclassified route is denied, not allowed
- The `external` profile starts: TLS on, auth on, `/_admin/*` off, verified by `check-config`
- Path-traversal regression tests pass in `scripts/validate/unit.sh`
- `cargo audit` and `cargo deny` green via `scripts/validate/deps.sh`
- TLS + auth enabled = zero plaintext credentials on the wire; no key in any log line
- Cluster rejects unknown peers without a valid PSK

(Metrics say `scripts/validate/`, not CI: there is no CI by decision — see Stage A5 and
[RELEASE-CHECKLIST.md](RELEASE-CHECKLIST.md).)

## Phase 16 — Boot & OOM Recovery at Scale

◐ **Partial** — the analysis, the 2026-08-19 rewrite and Stage 7 are here; four items
remain in [Part I, section E](#e-phase-16--boot--oom-recovery-at-scale--partial).

**Objective**: Bring OOM-kill recovery time on a 30 TB dataset spread across 16 shards down
to the near-zero the underlying engines individually promise. Analysed 2026-08-18 after a
report of multi-minute recovery on exactly that shape; the analysis follows, then what was
built from it on 2026-08-19 — which is not the six-stage list the analysis proposed, and
"What shipped" explains why.

**Audience**: any deployment large enough that the WAL tail between Tantivy commits can grow
past a few thousand entries per index before the supervisor idle timeout fires. The 30 TB /
16-shard report is the case that surfaced it; the fixes are bounded by data volume rather than
shard count, so a single very large shard hits the same wall.

### Why this is a cameodb problem, not a redb or Tantivy one

The redb and Tantivy recovery models each describe a *single-engine* boot:

- **redb** uses shadow paging / copy-on-write. Uncommitted transactions are discarded by
  pointing back at the last immutable commit root, so an unclean restart takes ~0 extra
  seconds and scales with file-open latency, not transaction-log size. Verified in this
  codebase: `HybridStore::new` calls `builder.open(&kv_path)` and nothing more — there is no
  replay loop at the redb layer.
- **Tantivy** keeps immutable segments on disk and memory-maps them, so boot has no warm-up
  phase that parses data back into a managed heap. Uncommitted indexing queue work is lost on
  OOM, safely reverting to the last `.commit()`. Verified: `open_tantivy_index` is
  `Index::open_in_dir` plus tokenizer registration.

cameodb does not treat Tantivy's commit as the durability boundary. It writes every op into
its **own WAL** stored inside redb tables (`wal_<index>`), alongside a `data_<index>` table
holding the full document, and batches Tantivy commits behind a threshold plus a supervisor
idle timeout. The two commits — redb (WAL + data, `Durability::Immediate`) and Tantivy
(index segment + fsync) — are **not atomic**. On OOM, redb has durable WAL entries that
Tantivy never indexed, so **replay is required**, and that replay is the entire recovery
cost. Neither library documents this pattern because neither was designed to be
cross-synchronised with the other; the bridge is cameodb's.

### Hot points, in boot order

#### HP1 — `get_highest_indexed_seq` fallback: full TopDocs scan on huge indices

When `get_persisted_committed_seq` returns `None` (no `_recovery_meta` entry — first boot
after the feature shipped, or any index that has never had a successful `commit_index` since
it landed), `recover_index` falls back to `get_highest_indexed_seq`, which runs
`TopDocs::with_limit(1).order_by_u64_field("_seq", Order::Desc)` against `AllQuery`. On a
~1.9 TB-per-shard index this is a fast-field scan across every segment — O(segments × docs)
even though it returns one document. With many indices in this state, Phase 1 becomes a
sequence of full-index searches before any replay starts.

#### HP2 — Phase 1 opens a full `IndexWriter` per non-synced index, 16 shards concurrently

`recover_indices` → `get_or_create_index` for every non-synced index. Each call opens the
Tantivy index, creates an `IndexWriter` with `num_worker_threads` + `num_merge_threads` OS
threads and `memory_budget_per_thread × num_worker_threads` of arena memory, then runs
`recover_index`. For a "very large" index (>8 GB) `get_optimal_memory_budget` returns
`max_budget_bytes` (up to 512 MB). Parallelism *within* a shard is capped at
`available_parallelism()`, but **16 shards run their `recover_indices` concurrently** — each
in its own `spawn_blocking`, fired from `MicroshardActor::start` without awaiting — so the
node holds `16 × cores` IndexWriters, thread pools and arenas at once. On the same 30 TB
dataset that just OOM'd, this is a strong candidate for re-triggering OOM during recovery.

#### HP3 — Phase 2 warmup: `warm_segment` page-fault storm on every index

Phase 2 walks **every** index (synced or not), sorted smallest-first, and for each calls
`warm_segment` on every `SegmentReader`, forcing `segment_reader.inverted_index(field)` for
every indexed field — building and caching term dictionaries. For a 1.9 TB shard this faults
in a large mmap region. It runs on a single thread per shard (`warmup-shard-<id>`),
sequentially across that shard's indices, so 16 threads × sequential huge-index warming is
sustained random IO across the fleet for a long time.

#### HP4 — WAL replay segment storm → post-recovery merge storm

`recovery_commit_threshold` is `max(default_batch_size × 10, 25_000)`. Each threshold commit
during replay seals a new Tantivy segment plus an fsync. If the OOM happened during a bulk
import or high-throughput window, the WAL tail per index can be large (bounded by
`should_commit_writer`, up to ~20× `default_batch_size`). Replaying 20k ops at a 25k
threshold produces ~1 segment, but across many indices × 16 shards this yields hundreds of
small segments → a Tantivy merge thread storm after recovery → slow queries and more memory
pressure, exactly when the node is most fragile.

#### HP5 — First-request inline recovery

`MicroshardActor::start` does not await `recover_indices`, so a shard becomes routable while
its background recovery is still running. The **first write** to an index that background
recovery has not reached yet calls `get_or_create_index` → `recover_index` **synchronously
on the writer thread**, blocking it for the full replay duration. The first read to an
unwarm index opens a cold Tantivy reader via `get_reader`. "Routable" therefore does not
mean "fast" — real traffic pays the recovery cost inline, and a write to a large un-recovered
index can stall the writer thread for minutes.

#### HP6 — `is_index_fully_synced` metadata churn

For every index, `recover_indices` calls `is_index_fully_synced`, which opens two redb
tables (`_recovery_meta` and `wal_<index>`) in separate read transactions. Metadata-only,
but at scale (many indices × 16 shards) it is a long tail of small redb operations on a
single `Database` per shard. Not the dominant cost, but it contributes.
### What shipped, 2026-08-19

The six stages below the analysis were written as six independent patches to the existing
bridge. They were not taken in that form. Reading them together made it clear that four of the
six — Stage 1's backfill, Stage 2's skip-writers-for-synced-indices, Stage 5's don't-block-on-
recovery, Stage 6's batched sync check — were all working around the same root cause: **cameodb
could not cheaply answer "is this index in sync?"**, so it had to open Tantivy, or scan a fast
field, or keep a redb mirror in step, to find out. Fix that one question and four of the six
hot points stop existing rather than getting cheaper.

**The checkpoint moved into Tantivy's commit payload.** `IndexWriter::prepare_commit` takes an
arbitrary string that Tantivy writes into `meta.json` as part of the commit itself; cameodb
stamps the redb WAL sequence the commit covers into it. Because the stamp is written by the
same operation that publishes the segments, it cannot describe segments that a crash prevented
from landing — the failure mode a separately-written checkpoint always has in one direction or
the other. That removes the reason `_recovery_meta` had to be authoritative, and with it the
`_seq` fallback scan that made a first boot after the feature shipped O(segments × docs) per
index.

**An empty WAL became the boot-time proof of sync.** A commit deletes the WAL entries it
covers, so `wal_<index>` holds exactly the writes Tantivy may be missing and nothing else.
Phase 1 is now a single redb read transaction per shard asking each WAL table for its last key
— a B-tree descent, not a scan. An idle 30 TB index costs the same as an empty one, and no
Tantivy index, writer or searcher is opened for it. Recovery time became a function of what was
in flight when the process stopped, which is the property the whole phase was chasing.

| Hot point | Outcome |
|-----------|---------|
| HP1 — full `TopDocs` scan on the `None` fallback | **Gone.** The checkpoint is O(1) from `meta.json`. The scan survives as a last resort for an index whose last commit predates both the payload and `_recovery_meta`, and it now backfills its answer so it runs at most once per index, ever — Stage 1's migration, made lazy and self-healing instead of a boot-time walk or a `migrate` subcommand |
| HP2 — an `IndexWriter` per non-synced index, × 16 shards | **Gone for synced indices** (none are opened at all) and **bounded for the rest** by a process-global semaphore, so a per-shard limit can no longer multiply by the shard count |
| HP3 — warmup page-fault storm | **Bounded.** Phase 2 runs under a 60s budget, smallest-index-first, and logs what it skipped; the remainder warms on first access through the existing path. Stage 3's options 2 and 3 remain available if 60s proves wrong at 30 TB |
| HP4 — replay segment storm | **Partly.** Mid-replay commits still checkpoint, but now by stamping the payload rather than writing redb, so each one is a Tantivy commit and nothing else. The steady-state max-WAL-size trigger (Stage 4.2) is **not done** |
| HP5 — first-request inline recovery | **Gone in practice.** The inline path still exists, but it now runs the same two-number check as boot, so a write to an un-recovered index pays for its own tail rather than for a scan of the corpus |
| HP6 — `is_index_fully_synced` metadata churn | **Gone.** The function is deleted; partitioning is one read transaction for the whole shard |

**A silent data-loss bug fell out of the rewrite.** Seeding the sequence counter needs a
durable high-water mark, and the old code took it from the WAL alone. A commit truncates the
WAL, so every cleanly stopped index reopened with an empty one and restarted numbering at zero
— reissuing sequences it had already spent. The next crash then compared its tail against a
checkpoint far *above* it, concluded there was nothing to replay, and dropped those documents
from the search index while redb still held them. It self-healed after one commit, which is why
it had gone unnoticed. `writes_after_a_clean_restart_are_replayed_after_a_crash` in
`crates/storage/tests/recovery_checkpoint_test.rs` covers it; against the old seeding it fails
with 100 documents found instead of 105.

**Still open:** Stage 4.2 (a max-WAL-size commit trigger, so a bursty writer cannot accumulate
a large tail before the operation-count threshold fires — this is now the only thing that
bounds worst-case replay length), and Stage 3's hot-set and field-scoped warming if the time
budget proves too blunt.

### Stage 7 — Shrink the write path ✅ Done 2026-08-19

Two changes to what a write puts on disk, both of which the recovery rework made available.

#### The WAL stopped storing the document

A `wal_<index>` entry held the whole `WalOp` — body included — while the same redb transaction
wrote that body to `data_<index>`. Every write therefore serialised the document twice and
fsynced it twice, on the hot path, forever. Entries are now one tag byte plus the document id:
about 6 bytes where a 1 KB document previously wrote over a thousand.

The WAL's job is to name *which* documents Tantivy may be behind on, and the authoritative body
is in `data_<index>` in the same transaction, so recovery reads the id and lets the committed row
decide the operation. A row means index the document as it now stands; no row means it was
deleted. The two cases are exact, because a put always writes the row and a delete always removes
it, atomically with the WAL append being read.

The consequence worth stating on its own: replay now **converges on committed state** rather than
re-enacting a log, so each id is applied once. A tail that wrote one document twenty times costs
one Tantivy operation, not twenty; a put later deleted in the same tail costs one, not two. That
makes replay cheaper than the thing it replaced *and* shorter, which is the opposite of the usual
trade for storing less.

Entries written by earlier builds still decode — only their id is taken — so an upgrade replays a
tail left behind by the previous build with no migration.

#### `_seq` is no longer declared on new indices

`STORED | FAST` u64 on every document: 8 bytes in the row store plus a columnar entry re-merged
on every segment merge, disproportionate because the Tantivy document holds only `id` and the
indexed fields. Its one reader was the checkpoint scan the commit payload replaced.

Done the way this section previously argued it had to be — `SchemaFields::seq` is `Option<Field>`,
`load_fields_from_existing_index` tolerates the field's absence, and both write paths and the
replay path stamp it only when the index has it. An index built with the column keeps it, is
still written to, and still recovers through it, so nothing on disk changes shape and no
migration is required. Rebuilding an index drops the field. `checkpoint_seq` skips straight to 0
when there is no column to scan, which is correct: an index without one was built after commits
started carrying a payload, so the only way to reach that rung is an index that has never
committed.

Two behaviour changes fall out, both of them corrections. `normalize_after_deserialization` no
longer invents a field the caller never declared. And `_seq:>0`, which used to resolve and match
nothing meaningful, is now reported as an unknown field on any index built without it.

#### The two bugs this audit turned up, both fixed

- **`PUT /api/{index}/_config` answered with `_seq` in `field_names`** — it is the one listing
  that bypasses `describe_fields`, where every other endpoint filters the field out, and it
  normalizes the schema first, which used to insert it.
- **`sort=_seq` was accepted and silently degraded across shards** — `fast`, so every check
  passed and the shard-local order was right, but bodies come from redb, which has no `_seq` key,
  so nothing was stamped for the scatter-gather merge to order by. Now refused like any unknown
  field, matching what `sortable_fields` always advertised.

#### What is still not covered by a test

- **An index built by an older build, opened by this one.** The compatibility path is real and
  exercised in the decoder unit tests, but an end-to-end fixture is unbuildable in-repo:
  `create_schema_from_definition` no longer declares `_seq`, so there is no longer any way to
  *create* a legacy-shaped index to open. Verifying it needs a checked-in fixture index or a
  build-flag seam.
- **A legacy WAL tail replaying end to end**, for the same reason. `decode_wal_entry` is unit
  tested against both formats, and the replay body above it is format-agnostic by construction.

### Success metrics

- OOM-kill recovery time on a 30 TB / 16-shard node drops from "multi-minute" to **under
  60 seconds for a clean WAL tail**, bounded by the size of the un-replayed tail rather than
  by total corpus size.
- Recovery does not re-trigger OOM on the same dataset that just OOM'd: peak RSS during boot
  stays under the configured `total_memory_limit_mb`.
- First-write latency to any index during the recovery window is bounded by the queue depth,
  not by the replay duration of that index.
- No steady-state throughput regression: the `cameodb-bench` mixed read/write arm at c16
  matches the figures in "Mixed read/write load, measured" within run-to-run spread.

**Not yet measured on the reporting node.** The change is verified by the storage suite and by
construction; the 30 TB / 16-shard figures above are still the target, not a result.

### Non-goals, recorded so they are not re-litigated

- **Removing the cameodb WAL.** It is the correctness boundary that makes the dual-engine
  design safe: redb is the source of truth, Tantivy is a derived, eventually-consistent
  search index. The work above makes the replay bounded, lazy and memory-safe, not absent.
- **Atomic redb + Tantivy commit.** A two-phase commit across the engines would eliminate
  the WAL tail entirely but pays a per-write fsync on both engines — the opposite of the
  batching design that gives cameodb its write throughput. The WAL + checkpoint model is the
  right trade; this phase makes its worst case cheap.
- **Changing Tantivy's `ReloadPolicy::Manual`.** The manual reload is deliberate (no
  per-index meta-file watcher thread, no cache-discarding redundant reloads) and is not the
  cause of slow recovery. Phase 2 warming is the lever, not the reload policy.

## Phase 17 — Record Deletion ✅ Done

Scoped and delivered 2026-08-26/27. CameoDB could delete an *index* and never a *record*; this
phase is that gap, closed.

**The storage engine already did it.** `WalOp::Delete` exists and both write paths handle it:
`apply_write` removes the `data_<index>` row and issues `delete_term` in the transaction that
appends the WAL entry, and `apply_batch` does the same through `PreparedKind::Delete`. Recovery
needs nothing either — Stage 7 made a WAL entry the document id alone and let the committed row
decide the operation, so *no row means deleted* is already the replay rule. A delete also
survives coalescing correctly without new code: put-then-delete of one id in a single batch
resolves because Tantivy applies `delete_term` to documents added earlier in the same commit,
and the redb `insert` then `remove` leaves nothing behind.

What was missing was everything above the shard: no `ClientOp` variant, no route, no
authorization row, no SDK or CLI, no docs. That part was small. What made this a phase rather than
an item is that two defects in shipped code stood in front of it — one of them fatal to the
feature, both of them already wrong before it — and looking for the delete path is what found
them. That is the entry worth reading here: the feature is ordinary, and what it turned up is not.

The items are in the order they were done, which is cost order: the two defects first, since
delete was unshippable without them, then the guard that makes the new route's authorization row
mandatory, then the feature itself.

### I1 — The document read cache is never invalidated

✅ **Done** 2026-08-26 — found, reproduced and fixed in the same pass.

`HybridStore::read_cache` was populated by every search that hydrates a body — `get_by_key` and
`get_batch_by_keys` both insert into it — and cleared in exactly one place, `delete_index_data`.
Neither `apply_write` nor `apply_batch` touched it. So a row that changed under a cached entry was
never noticed, up to the 1024-entries-per-index FIFO churning it out.

Reproduced against the storage crate — put, read, put, read, delete, read:

```
after put v1   : {"json_blob":{"id":"d1","title":"v1"}}
after put v2   : {"json_blob":{"id":"d1","title":"v1"}}   ← stale update
after delete   : {"json_blob":{"id":"d1","title":"v1"}}   ← deleted document still served
batch after del: 1 row
```

**It was a live correctness defect for updates, not only a blocker for deletion.** An updated
document kept serving its previous body to any caller that had read it before the update. It
stayed invisible because the entry point is a search's body hydration and the eviction is a FIFO,
so the staleness had a short and unpredictable life on a busy index — and none at all on an index
nobody reads twice.

For deletion it would have been fatal rather than merely wrong. An `id:VALUE` query is answered
entirely from redb by design, so with the cache stale a deleted record comes back indefinitely and
the delete appears not to have happened.

**What landed.** The touched ids are dropped from the cache by the same code that mutates the
rows — `apply_write` for one id, `apply_batch` for the whole batch, borrowed out of `tantivy_ops`
rather than collected into a second vector. One `DashMap` entry lock and a `HashMap` remove per
id, on a path already inside a redb transaction.

Removing the entries is not sufficient on its own, and the second half is the subtle one. The
removal has to happen *after* the redb commit — invalidating first leaves a window where the row
is still the old one — and a reader that opened its transaction before that commit legitimately
still sees the pre-write row. If it caches that body after the removal, the staleness is back and
nothing will take it out again. So `IndexReadCache` now carries a generation beside its entries:
a reader reads it before opening its transaction and quotes it back to `insert_into_cache`, which
declines anything a write has superseded since. Both sides touch the struct under the same
`DashMap` entry guard, so the check and the insert cannot interleave with a bump and a removal —
whichever side gets the guard first, no stale body survives. `delete_index_data` bumps rather than
dropping the whole entry, for the same reason: a fresh entry starting from zero would re-admit a
reader mid-flight.

Two tests: `a_changed_row_is_not_served_from_the_read_cache` covers update and delete on both
write paths, and `a_body_read_before_a_write_is_refused_by_the_cache` drives the two halves of the
race in the order that produces it, since a single thread cannot interleave them.

Fixed alongside, in the same file and for the same reason: `apply_batch` invalidated the per-index
size cache only when a batch wrote or updated a row, so a batch of pure deletes left `/_indexes`
reporting the pre-delete size until `index_cache_expiry`.

### I2 — The shard-affine hint decides the shard, not just the worker

✅ **Done** 2026-08-26 — found by reading the delete routing path, fixed in the same pass. It
sat behind `shard_affine_dispatch`, which defaults `false`, so it was latent rather than active.

`engine_write` derives the effective routing key from the document —
`extract_routing_value(doc, schema.routing_field)` first — but then takes the target shard from
`affinity_shard` whenever that hint names a live local shard, and the hint was computed by the
router from `routing_key.or(id)`. On an index whose routing field is a real, non-key field those
two disagree, and the hint wins.

The consequence is a document on a shard the ring does not believe owns it. Searches still find
it, because they are scatter-gather. What breaks is the next write of the same id through a path
with no hint — the actor-mailbox fallback when a worker queue is full, for instance: that one
routes by the routing field, lands on the other shard, and the id now exists twice. Scatter-gather
returns both copies and a delete would remove one.

**What landed.** The hint chooses the worker; the ring chooses the shard. `route_write` is an
xxh3 and a `BTreeMap` range descent, which is not a saving worth a class of divergence in front of
a redb transaction. Stage 2a's stated purpose — "eliminates 1 cross-core wakeup per write" — is
dispatch, and dispatch is all it now decides: `try_send_affine` still routes by the shard's dense
ordinal onto the worker co-located with its pinned writer thread.

The divergence is now unrepresentable rather than merely unused. `affinity_shard` was removed from
`OrchestratorEngine::execute` and from `engine_write` altogether, so nothing on the execution path
holds a shard hint it could route by. It stays on `OrchestratorJob::Execute`, where dispatch reads
it, and the worker closure binds it as `_affinity_shard` to say so.

The routing rule it used to overrule was written out identically in three places — the engine fast
path and both halves of `orch_write` — so it is now one `effective_routing_key` helper with the
precedence documented as an ordered list, pinned by
`the_routing_key_comes_from_the_document_before_the_caller`. A rule that has to agree with itself
in three copies is not a rule.

Delete inherits none of this and could not have reproduced it anyway: with no document to override
the key, a delete's hint and its final routing key derive from the same value, so the hint always
names the shard the ring names.

### I3 — The route-classification guard compares paths, not methods

✅ **Done** 2026-08-26.

`every_mounted_route_is_classified` reads `http_server/routes.rs` and asserts every mounted path
has a row in `ROUTES`. It compares *paths*: `is_classified` matches `rule.pattern` and ignores
`rule.method`. So a second method on an already-classified path satisfies the guard with no row
of its own — `classify` then returns `None`, which denies, so the failure is closed rather than
open, but it is silent and it presents as every request to the new endpoint being refused
authentication.

Nothing exploited it: `/api/{index}/_config` carries `PUT` and `GET` on separate `.route()` calls
and both are classified. But nothing forced that, and I4 adds `DELETE` to a path that already has
`PUT`, which is exactly the shape the guard could not see.

**What landed.** `mounted_routes` yields (method, path) pairs, and both directions of the check
match on both halves. Three things the parser needed beyond the method name itself: each `.route(`
call is bounded by paren matching rather than by the next call, so method names cannot leak in from
whatever follows the last route in a chain; the chained `.get(…)` of a method router counts as well
as a bare `get(…)`, which is how the MCP transport's three verbs on one path are finally seen as
three routes rather than one; and the token match requires a word boundary, so a handler named
`set_budget(` does not read as a `get`. `HEALTH_PATH` is resolved in the parser instead of being
special-cased by the test.

A parser is only a guard while it parses everything, so `every_route_call_is_accounted_for` fails
on a call whose path it cannot read or whose method it cannot find. That replaces the
literal-count arithmetic it grew out of, and is stronger: it catches an unreadable path expression
rather than only a second constant-named route.

### I4 — Delete a document by id

✅ **Done** 2026-08-26.

```
DELETE /api/{index}/document?id=<id>[&routing_key=<key>]
```

Answers with what a write answers, one word apart:

```json
{"id":"book_001","result":"deleted","version":1042,"shard_id":"…"}
```

**Why the id is in the query and not in the path.** `DELETE /api/{index}/document/{id}` was the
first shape considered and is rejected twice over. `authz::match_pattern` understands one
placeholder, `{index}`, so a second segment means changing the matcher that decides every
request's authorization — a poor trade for a URL shape. And ids here are arbitrary strings:
authz classifies the raw path while the handler receives the decoded one, so an id containing
`%2F` makes the two disagree about what is being deleted. A body on `DELETE` was the second
candidate and loses to proxies that strip it. The query form has the precedent anyway —
`DELETE /api/{index}?delete_schema=true` already carries its parameters there.

**Capability: `Write`.** A key that can write can already overwrite any document with anything,
so withholding deletion from it protects nothing. `auth.rs` also reserves "something in between"
for per-index overrides (C1) rather than a fourth capability, and this is not the case to break
that with.

**The op.** A new `ClientOp::Delete { index, id, routing_key }`, routed exactly as a write is:
`route_and_handle` with `OperationType::Write` and a routing hint of `routing_key.or(id)`, then
`resolve_local`, the worker pool, `engine_delete`, the shard's `StorageCommand::Write` carrying a
`WalOp::Delete`, and the writer thread, which coalesces it alongside concurrent puts to the same
index. Three things follow from that alignment rather than from new code:

- **Remote forwarding is free.** `try_remote` sends the `ClientOp` itself over
  `cameo.orchestrator.client_op`, so a new variant crosses nodes with no transport work.
- **`engine_delete` never defers to the actor.** A delete cannot evolve a schema, so it is the
  first operation that is wholly engine-servable: no `WorkerOutcome::UseActor` arm, no
  `&mut NodeOrchestrator`, no mailbox serialization point in front of it.
- **Affinity applies unchanged**, provided `ClientOp::Delete` is added to *both* the
  `is_worker_eligible` match and the affinity-hint arm in `RouterActor::handle_client_op`. Miss
  the second and every delete lands on an arbitrary worker and cross-core-wakes the target
  shard's pinned writer thread, which is the cost Stages 2a, 2d and 2e exist to remove.

**Routing without a document.** A write reads its routing key out of the document; a delete has
only an id. The schema decides, with no I/O:

| Index shape | Route |
|---|---|
| `routing_field == "id"` — the default | key = id; unicast, exact |
| `routing_field` is a shadow field (`sha1`, `sha256`, …) | the shadow value *is* the key, so key = id; unicast, exact |
| custom non-key routing field, caller sent `routing_key` | key = `routing_key`; unicast, exact |
| custom non-key routing field, no `routing_key` | **refused, 400, naming the field to supply** |

Refusing the last case is deliberate — see the non-goals. The caller's path is the one the
engine would have to take anyway: search `id:VALUE`, read the routing field off the document,
delete with it.

**Two guards the storage path needs.** `apply_write` opens through `get_or_create_index`, which
*creates* the index when it is absent, so a delete naming an unknown index would bring one into
existence; check first, and answer without creating anything. And `apply_batch` invalidates the
per-index size cache only when a batch wrote or updated a row, so a batch of pure deletes leaves
`/_indexes` reporting the pre-delete size until `index_cache_expiry` — cosmetic, bounded, fixed
while passing.

**Visibility, to be documented rather than smoothed over.** An `id:VALUE` lookup is immediately
consistent, because that path is answered from redb and skips Tantivy entirely (once I1 is
fixed). A query-matched hit is consistent within `supervisor_timeout_secs` — 5 seconds by
default, sooner if the commit threshold arrives first, and the delete path must call
`signal_supervisor` for that timer to exist at all. In between, the hit's body is skipped but
`total_hits` still counts it, because the count comes from the Tantivy collector while bodies
come from redb. Subtracting skipped documents from the count would trade a visible artifact for
broken paging arithmetic; the artifact is the better of the two.

### I5 — Delete documents in bulk

✅ **Done** 2026-08-27.

```
POST /api/{index}/_bulk/delete
```

Body is a list of ids, or of `{"id", "routing_key"}` objects for a custom-routing index, and the
two shapes may be mixed — a list of bare ids is what almost every caller has, and making them wrap
each one in an object to say nothing extra is a worse API than accepting both. `POST` rather than
`DELETE` because a body on `DELETE` is what proxies mangle, and `_bulk` keeps the name the write
side already uses. Answers as `_bulk` does:

```json
{"items_received":2,"items_deleted":2,"errors":[],"took_ms":3}
```

Mechanically it is `orch_bulk_write` with the document work removed: route each id by I4's rule,
group by shard, hand each shard one `Vec<WalOp::Delete>` through
`handle_batch_write_via_channel`, group the remainder by owning node and forward. One redb
transaction per shard, the same coalescing, no new machinery.

**What landed as designed**, with one decision the design had not settled: an id that cannot be
routed is an error against that id rather than a failed batch. A batch may span tenants, so one id
missing its routing key says nothing about the others, and refusing the whole request would throw
away the work that was routable. An empty body is still a `400` — that is a malformed request, not
a no-op.

### I6 — Deletion in the SDK, the CLI and the documentation

✅ **Done** 2026-08-27.

- `sdk.rs`: `delete_document(index, id, routing_key)` and `delete_documents(index, ids)`, beside
  `write_document` and `bulk_index`. They landed with I4 and I5 respectively, because the
  end-to-end tests should drive the client that ships rather than a hand-rolled request.
- CLI: `delete <index> --id <ID>…`, `--ids-file <PATH>` and `--routing-key <KEY>`, in both the
  command line and the REPL, with completion and help text. `delete <index>` with no ids named
  still means the index, which is what it has always meant — and the mistake that had to be made
  impossible is the other direction, so an ids file that yields nothing is an error rather than a
  fall-through to deleting the index. One id takes the single-document route, several take the
  bulk one, and `--delete-schema` with ids named is refused as a contradiction.
- `docs/API_REFERENCE.md` § Document Operations: both endpoints, the capability table, the
  routing rule with the two-step that finds a routing key, and a **What deletion promises**
  section — idempotence, the 404 for a missing index, and the two visibility tiers as a table.
  That last one is what a caller would otherwise discover by being surprised.

### Non-goals, recorded so they are not re-litigated

- **Fanning a keyless delete out to every shard.** It is *correct* — a shard that lacks the id
  removes nothing — and it is still refused. It costs `shards × nodes` writer transactions to
  remove one row, it would vivify the index on every shard it touched, `handle_broadcast`'s
  non-search arm returns the first successful response rather than merging, and
  `route_and_handle_inner` already prohibits broadcasting a write in as many words. A caller
  who cannot supply the routing key can find it with one search.
- **Reporting whether the record existed.** Delete answers `"result": "deleted"` whether or not
  a row went away, exactly as a write answers `"result": "created"` for an overwrite.
  Distinguishing them means threading a per-operation outcome back through the writer thread's
  reply-splitting loop, which is a change to the hot path for a status word.
- **`_delete_by_query`.** The honest answer to the keyless case above, and a phase of its own:
  it needs a search-then-delete loop, a decision about what consistency it promises while
  documents are still arriving, and I1 fixed underneath it.
- **An MCP delete tool.** The server's own instructions promise that "ingestion happens
  elsewhere and no tool here writes". Deletion stays on the HTTP and SDK surface.
- **Streaming deletion (`_bulk/delete/stream`).** Ids are small; a bulk POST carries a great
  many of them. Worth revisiting only against a workload that overruns the body limit.

### What deletion gives 2f.2

B1 records that Tantivy's merge threads inherit the mask of whichever thread built the
`IndexWriter`, so an index created by writing to it — the normal path — confines
`merge_thread_*` and `segment_updater` to the same single core as the writer they contend with,
and asks that 2f.2 not be attempted "without a specific hypothesis neither measurement covers".

Deletion is that hypothesis. `delete_term` reclaims no bytes until a merge rewrites the segment,
so a delete-heavy index is precisely the workload whose throughput is gated by merge capacity —
the one case where `merge_num_threads = 2` meaning two threads timesharing one core is the
binding constraint rather than a curiosity. Shipping this phase gives 2f.2 a workload that can
falsify it.

Nothing else about deletion touches the memory work: a delete allocates an id and a WAL entry of
about six bytes, and its Tantivy side is an opstamp and a `Term` in the delete queue, with no
document buffer. It does count as one operation toward `should_commit_writer`, whose threshold is
sized in documents — which is asymmetric and correct, since a commit is what makes the delete
searchable.

## 0.3.2 hardening, 2026-08-19/20 ✅ Done

Filed 2026-08-26. Five commits landed between this file's last update and the 0.3.2 cut, none
of them recorded here at the time. None belongs to a numbered phase; each is a defect found by
looking at what the phase work above had left behind. `CHANGELOG.md` carries the operator-facing
account — this is the record that they happened, and what each says about where to look next.

**A sort the index could not answer came back as an empty result page.** A sort fails in every
shard at once or in none of them, and scatter-gather reports the first as a partial failure:
`200`, `hits: []`, `total_hits: 0`, and the reason only in per-shard `errors`. A caller reading
the hits — which is every caller — saw "nothing matched" for a query that was never run. The
sort field is now checked before the fan-out and an unusable one is a `400` naming it.

The question asked is the engine's own — *can I order by this column?* — rather than the
narrower *does a column of this name exist*, because the first check shipped asking the narrow
one and the other kind still got through: a boolean, any non-text field without a fast column,
and `_seq` on every index built before the field was retired. An index a test creates never
records `_seq`, which is why the suite could not see it.

**A sort by a shadow field did not work, and `sort=id` on an index that has one was ordered per
shard.** A shadow field is the document key under the source's own name and the query path
already maps it to `id`; the sort path did not. The same mapping was missing on the way back,
so a merge looking for `id` found no key to order by and returned each shard's block in turn —
the right documents in the wrong order, with nothing in the response saying so.

**A fully discarded clause emptied a query and answered `200`.** A dropped clause was described
everywhere as *widening* the result. It also narrows a disjunction, and empties a query that had
nothing else to run — a zero indistinguishable from "no document matches". The engine reports
that as `SearchOutcome::emptied` and the orchestrator refuses on it before the fan-out, where an
unrunnable sort is already refused for the same reason. A partial drop still answers with
`_discarded_clauses`.

**Shutdown reported file handles it never released.** The worker pool routes through a clone of
each shard actor, and a cloned actor carries its own `Arc<HybridStore>`, so index mmaps, the
tantivy writer lock and the redb database outlived every shutdown. Stores are taken first and
the engine snapshot republished without them; `HybridStore::shutdown` clears `readers` and
`writers` itself.

**`node_identity.json` was rewritten on every boot with byte-identical content** — truncate in
place, no rename, no fsync — so a crash in that window left JSON that no longer parses, and the
next boot generated a fresh keypair and came up under a UUID the persisted shard assignments no
longer name. The write now goes through a temp file and a rename, happens only when the identity
differs, and creates the file `0600` rather than at the umask's `0644`. A `node_key` posture
check reports the mode, warning rather than failing since the file may be orchestrator-managed.

**The dedicated read pool was torn down by `Drop`** after shutdown had already logged a clean
exit — the one teardown with neither a timeout nor a wait. It is now Phase 4 of 5, after the
shards and before the coordinator.

**The request-timeout validation probe was failing in the shape of the defect it exists to
detect.** `--limit-rate` cannot be combined with `--max-time`: curl sleeps for as long as the
bytes already sent require and never wakes to check the deadline. The server had answered `408`
on time throughout. The body is now fed down a pipe as a chunked upload. Three blind spots
around it were closed at the same time, and an HTTP/2 section was added — the listener serves
h2c on the same port unprompted and nothing in the suite had ever sent an h2 frame.

**Two logging corrections**: with clustering disabled the swarm is a placeholder, but the caller
announced a cluster port, a DHT and a listen address anyway, pointing anyone debugging
connectivity at a socket nothing opened; and an empty `cors_allowed_origins` logged "restricting
to configured origins" with none configured, when deny-all is the intended default.

**One posture check gained a middle verdict.** The profile ceiling bounds a flood from outside
but never asked whether the node can hold what it admits, and the defaults land exactly on the
external ceiling while allowing eight times `total_memory_limit_mb`. Off loopback that now warns
rather than fails, since reaching it takes every admitted request carrying a full body at once.

## Measurements

Every figure below is **closed-loop** — service time at a fixed concurrency, not an SLA.
See [F2](#f2--an-open-loop-load-generator) for what that costs and what it blocks.

### **The affinity flags, measured**

Recorded 2026-08-09 with `cameodb-bench` against a Linux node in a container (aarch64,
8 cores, 8 shards, `wal_sync = true`), client on the host, three repeats per arm, medians.
Every arm ran from an empty data volume.

| Arm | write ok/s @c16 | write p90 | write p99 | search ok/s @c16 | search p99 |
|---|---|---|---|---|---|
| no affinity at all | 3 339 | 6.57ms | 11.59ms | — | — |
| `writer_core_affinity` only (the default) | 3 375 | 6.63ms | 11.12ms | 5 055 | 8.3ms |
| `+ shard_affine_dispatch` | 2 797 | 10.15ms | 17.39ms | 4 850 | 8.9ms |
| `+ worker_core_affinity` | 2 815 | 9.94ms | 18.90ms | 4 320 | 16.5ms |

The write regression held at concurrency 8, 16 and 32 — 13% to 20% — so it is not an artifact
of one operating point. Pinning was confirmed to have actually taken effect in each arm via
`/_admin/workers` (8/8 workers on their target cores, writers on cores 0–7), not assumed.

**Stages 2d and 2e are a loss as built, and the reason is Stage 2's worker loop, not the
pinning.** A worker awaits `execute` inline, so it carries exactly one operation; enabling
affine dispatch forces `worker_count` from `min(shards × 2, cores × 2)` down to `cores`, which
halves the node's in-flight operations. That would be fine if workers were CPU-bound, but an
operation is mostly spent awaiting the shard writer — the node sat at ~135% CPU of 800%
available during the write runs. Affine assignment also loads workers unevenly where
round-robin does not, which is why the loss persists even at concurrency 8 where 8 workers
should be enough. Searches fail differently: they *are* CPU-heavy (~530% during search runs)
and fan out across every shard, so pinning the driving worker to one core is a plain loss.

Writer pinning itself is free — neutral against no affinity at all — and is what gives the
other two something to align to, so it stays on.

The dense-ordinal placement work was still worth doing: it is what makes the flags
measurable at all, and `xxh3`-based placement was strictly worse than what was measured here
(it left cores empty).

> **Superseded in part, 2026-08-10.** The closing prediction here — that the flags would pay
> off once a worker could carry several operations — was tested and is wrong. The diagnosis
> of *why* the flags lose was incomplete rather than the measurement: see "Worker
> concurrency, measured" below.

### **Worker concurrency, measured**

Recorded 2026-08-10 with `cameodb-bench`, same rig as above (Linux container, aarch64,
8 cores, 8 shards, `wal_sync = true`), client on the host, three repeats per point, medians,
every run from an empty data volume. The host was otherwise idle — a first attempt at this
sweep ran while the machine was compiling and produced nothing but noise.

**How wide should a worker be?** `default.toml`, single writes, concurrency 64:

| width | write ok/s | p50 | p90 | p99 |
|---|---|---|---|---|
| 1 (the old inline loop) | 4 178 | 11.32ms | 29.30ms | 81.75ms |
| 2 | 5 438 | 9.26ms | 18.61ms | 78.78ms |
| 4 | 6 826 | 7.58ms | 11.25ms | 77.08ms |
| **8 (chosen)** | **7 118** | 7.68ms | **10.45ms** | 61.53ms |
| 16 | 6 444 | 7.97ms | 11.42ms | 88.44ms |

**+70% throughput and p90 down 64%** against the pre-change baseline, and the curve turns
over rather than flattening: every width-8 repeat beat every width-16 repeat, and width 8's
worst repeat beat width 4's median. Eight is a measured peak, not the largest value tried.

The width-8 point was then re-measured on the final build — constant compiled in, sweep hook
removed, worker loop refactored to take its operation runner as a parameter — and came back
at **6 901 ok/s median over six runs** (5 690 … 7 079) against the sweep's 7 118 over three.
That is +65% rather than +70% on the same baseline. The two sets overlap and the spread is
wider than the gap, so this is not evidence of a cost in the refactor; it is the honest width
of the measurement. Quote the range, not the best number in it.

The control matters as much as the sweep. At **concurrency 16 the same widths are flat**
(4 293 / 3 906 / 4 023 / 4 156 ok/s, within run-to-run spread), and they should be: the
default pool is 16 workers, so even at width 1 the node can hold every request a closed-loop
client at c16 has outstanding. Width only buys anything once demand exceeds `worker_count`.
Read that as the scope of the win — this is a saturation fix, not a free speed-up.

**The affinity flags were re-measured at width 8, and they still lose.** This is the part
that did not go as predicted:

| Arm | write ok/s @c64 | write p99 | search ok/s @c16 | search p99 |
|---|---|---|---|---|
| default (writer pinning only) | 7 118 | 61.53ms | 6 326 | 5.75ms |
| `+ shard_affine_dispatch` | 5 393 | 141.62ms | 6 298 | 5.78ms |
| `+ worker_core_affinity` | 6 735 | 92.78ms | 5 618 | 8.62ms |

Affine dispatch costs 24% of write throughput with a worker eight operations wide, and the
separation is clean: default's *worst* repeat (6 995) beat every affinity repeat. The affine
and pinned write arms are noisier than the default one and their ranges overlap, so the
ordering *between* them is not resolved here — only that both sit below the default.

So the earlier diagnosis was half right. Halving `worker_count` did hurt, but it was never
the whole story, and the surviving cause is the constraint itself: a job for shard S may only
run on worker `S % worker_count`, so any instantaneous skew across shards leaves workers idle
while their neighbours queue. Round-robin cannot be unlucky that way. Searches confirm the
split from the other side — affine dispatch is *neutral* for them, because searches dispatch
round-robin regardless, while pinning the driving worker costs 11% and half again on p99.

Both flags stay off, now for a reason that has survived a test designed to overturn it.

### **What the audit trail costs, measured**

Recorded 2026-08-10 — and **not on the rig every other figure here comes from**. This ran
natively on the macOS development host, so the absolute numbers are not comparable to
anything else in this document; only the two arms are comparable to each other.

`--mode write --concurrency 64 --duration 20`, security off in both arms so the only variable
is the trail, three repeats each, arms alternated, host otherwise idle.

| `[security.audit]` | Repeats (ok/s) | Median | Within-arm spread |
|---|---|---|---|
| `enabled = false` | 1 803, 1 637, 1 695 | 1 695 | 10.1% |
| `enabled = true`, file sink | 1 753, 1 740, 1 610 | 1 740 | 8.9% |

**The cost is below this measurement's noise floor.** The arms overlap completely and the
median difference (+2.7%) runs the *wrong way* — the audited arm was nominally faster, which
is a statement about the spread and not about auditing. What can be claimed is bounded: on
this host, at this sample size, a difference large enough to matter would have shown, and did
not. That is not the same as "free", and this is three repeats on a laptop.

The design predicts as much. A write costs one `Instant`, one timestamp format and a
`try_send` on the emitting side; the record is then folded into a `HashMap` entry rather than
serialized, so the per-write work never includes the JSON. The 44 171 writes of one run
produced **three lines**:

```json
{"event":"write_stats","index":"bench","ops":16821,"errors":0,"window_start":"…07.212Z"}
{"event":"write_stats","index":"bench","ops":15498,"errors":0,"window_start":"…16.737Z"}
{"event":"write_stats","index":"bench","ops":11852,"errors":0,"window_start":"…26.749Z"}
```

while the `PUT /_config`, the commit, the two `/_admin/workers` polls and the `DELETE` each
kept their own. That ratio — five figures of ingest against a handful of lines — is the whole
argument for rolling writes up, and it is what the file actually contains.

Not measured, and worth knowing before trusting the above: the read path, where a record *is*
serialized per request; and the trail under a workload heavy enough to overrun the queue,
which is the case the `gap` record exists for. Both want the open-loop generator that item 4
already blocks on.

### **Mixed read/write load, measured**

Recorded 2026-08-10, same rig. **Every performance number this repository had published
until now was taken with writes alone or searches alone.** Running them together is a
different machine, and the question it answers — should reads and writes be isolated onto
separate cores? — could not have been answered by any earlier arm.

| workload | alone @c16 | in mixed @c16 | change |
|---|---|---|---|
| writes | 4 074 ok/s, p99 8.5ms | 1 776 ok/s, p99 27.0ms | **−56%, p99 3.2x** |
| searches | 5 880 ok/s, p99 6.5ms | 3 284 ok/s, p99 15.4ms | −44%, p99 2.4x |

Container CPU during the mixed runs sat at **~620% of 800% — one and a half to two cores
idle** while both workloads lost roughly half their throughput. Work is being lost, not
shared, so there is real headroom here.

**It is not a core-contention problem, and core isolation would not fix it.** Three results
say so, and the first was a hypothesis this section set out to confirm:

- **Unpinning the writers changed nothing** (1 758 vs 1 776 ok/s). The theory was that a
  pinned writer returning from fsync cannot resume until *its* core is free, even with other
  cores idle. Measured, and false.
- **Capping the read pool helps a little, consistently**: `search_threads` 16 -> 6 moved
  search p99 15.44 -> 12.49ms and write p99 27.0 -> 23.1ms, and collapsed run-to-run spread
  from 1 477-1 853 to 1 837-1 842. `= 8`, the code default, lands between the two. Bounded
  read concurrency is the mechanism this design already chose over partitioning, and it
  works — modestly.
- **The write path is waiting on disk, not CPU.** With `wal_sync = false` under the same
  mixed load, writes go 1 837 -> 3 416 ok/s (+86%), write p99 23.1 -> 12.1ms, and CPU rises
  to ~724% as searches finally start competing for cores in earnest.

Partitioning cores would take cores from searches — the one workload here that *is*
CPU-bound, drawing ~600% of 800% on its own — to give them to writers that need ~127% and
spend most of it blocked in `fsync`. That is the same trade `worker_core_affinity` already
lost by 11%.

**What actually limits mixed writes is the cost of each commit.** The shard writer already
coalesces: it drains every queued command and merges same-index writes into one redb
transaction, so one fsync serves the whole group. Measured mean group size:

| case | write ok/s | mean coalesced group | implied cost per commit |
|---|---|---|---|
| pure write @c64 | 6 669 | 4.49 | ~5.4ms |
| pure write @c16 | 4 165 | 2.40 | ~4.6ms |
| mixed @c16 | 1 597 | 2.49 | **~12.5ms** |

Coalescing does not degrade under mixed load — 2.49 against 2.40. The *commit* gets three
times more expensive, because tantivy segment reads contend with WAL fsync for IO and page
cache. And the group cannot grow to absorb it: the writer commits whatever is queued at that
instant, and at concurrency 16 across 8 shards only about two writes are ever in flight per
shard.

The obvious response is a **bounded linger before commit** — wait a short, capped interval
for more writes rather than committing the two already queued, amortising a 12.5ms fsync
over 8 writes instead of 2.5.

**It was built and measured on 2026-08-10, and it does not pay. The code was removed.**
Lingering only when the instant drain already found company (so an isolated write never
waits), swept at 200µs / 500µs / 1000µs against a 0µs control, three repeats at c16 mixed
and c16 pure, then six at c64 where it had the best chance:

| linger | mixed write ok/s @c16 | pure write ok/s @c16 | pure write ok/s @c64 (n=6) |
|---|---|---|---|
| 0 (control) | 1 851 | 4 272 | 6 435 |
| 200µs | 1 938 | 4 429 | 6 803 |
| 500µs | 1 782 | 4 299 | — |
| 1000µs | 1 718 | 4 348 | — |

Every arm has a bad repeat and the between-arm gaps are smaller than the within-arm scatter;
at c64 the two distributions overlap almost entirely and the 200µs arm owns the single worst
run of the twelve. Nothing here is resolvable.

The arithmetic says why, and it is the useful part. At c16 the node writes ~1 850/s across 8
shards — 231/s per shard, so **0.046 writes arrive at a shard during a 200µs window**; at
c64 it is still only ~0.18. Worse, the bench client is closed-loop, so it cannot issue the
next write until the current one is answered: the writer would be waiting for writes that
cannot arrive until it commits and replies. **The linger waits on itself.**

A linger can only work where many independent clients hold requests outstanding at once —
an open-loop arrival process. Do not rebuild it from the reasoning above without first
having a workload generator that can produce one; against this harness it is untestable, and
against a closed-loop client it is provably useless. **That generator now exists** —
[F2](#f2--an-open-loop-load-generator), 2026-09-15, `cameodb-bench --rate` with Poisson
arrivals — so the question is open again and is answerable. It has not been asked yet. The remaining honest levers on mixed
write cost are the fsync itself (device, `wal_sync`, WAL placement) rather than how the
writer groups.

## Settled decisions

Questions asked and answered. Two are rejections, kept so they are not rebuilt.

1. ~~**A latency harness.**~~ ✅ Landed 2026-08-09 as `cameodb-bench` (`crates/bench`): percentiles for writes and searches, the node's `took_ms` beside the client-observed figure, and the worker-pool delta over the measured window. Closed-loop, so runs are comparable at equal concurrency rather than being an SLA
2. ~~**Document and default the affinity flags.**~~ ✅ Landed 2026-08-09, and the answer was *no*: see [The affinity flags, measured](#the-affinity-flags-measured). Both stay `false`, now present and explained in `cameodb.example.toml`, `crates/server/cameodb.toml`, `docker/cameodb-docker.toml` and `docs/CONFIGURATION.md`
3. ~~**Give a worker more than one operation at a time.**~~ ✅ Landed 2026-08-10. A worker now carries up to 8 operations, bounded by a semaphore acquired *before* the receive so the channel stays the backpressure signal. Worth **+65-70% write throughput and −64% on p90** where the pool is the constraint, and nothing where it is not — see [Worker concurrency, measured](#worker-concurrency-measured). It did *not* redeem the affinity flags, which was the other reason to do it
4. ~~**A bounded linger before the writer commits.**~~ ❌ Built and rejected 2026-08-10 — no measurable gain at any concurrency tested, and the arrival arithmetic says there cannot be one against a closed-loop client. Removed; the reasoning is recorded in [Mixed read/write load, measured](#mixed-readwrite-load-measured) so it is not rebuilt. **An open-loop load generator was the prerequisite for revisiting it**, and [F2](#f2--an-open-loop-load-generator) landed it on 2026-09-15. The rejection stands on the evidence taken at the time; it is no longer unfalsifiable

