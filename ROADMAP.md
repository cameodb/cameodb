# CameoDB Development & Optimization Plan

Active work is at the front of this file; everything delivered is in
[Part II — Archive](#part-ii--archive), kept in full. The archive is not decoration: the
measurements and the rejected options are what stop a settled question being reopened, and
several entries exist precisely to say *do not build this again*.

**Last reconciled against the code: 2026-08-26, at 0.3.2.** What that check changed is
recorded under [Reconciliation](#reconciliation-2026-08-26).

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
| 12 — MCP server integration | ◐ Partial | Streaming, semantic routing, the documentation pass, compliance tests and benchmarks, two schema-listing defects, and the syntax reference's drift from the engine |
| 13 — Thread-per-core & memory operations | ◐ Partial | Stage 2f.2 (CPU arenas) and 2f.3 (per-arena jemalloc stats) — both with the evidence against 2f.2 |
| 14 — Security hardening | ◐ Partial | Stage C3 only (per-index role overrides); complexity caps deferred |
| 15 — HA: reindex, replication, migration | 📋 Planned | All three stages |
| 16 — Boot & OOM recovery at scale | ◐ Partial | Stage 4.2, Stage 3's deeper warming options, and the measurement on the reporting node |
| 17 — Record deletion | ✅ Done | — |
| Code health — reviewed at 0.3.1 | 📋 Planned | Seven items, none behavioural |

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
`cameodb-bench` still closed-loop, and the four code-health duplications all still present.

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
| [A4](#a4--what-a-schema-listing-says-about-id-for-projection-and-for-sorting) | What a schema listing says about `id`, for projection and for sorting | 12 | 2026-08-15 | 📋 |
| [A5](#a5--semantic-routing) | Semantic routing | 12 | 2026-08-05 | 📋 |
| [A6](#a6--the-syntax-reference-has-drifted-from-the-engine) | The syntax reference has drifted from the engine | 12 | 2026-08-27 | 📋 |
| [A7](#a7--a-short-page-and-a-stale-count-say-nothing-about-why) | A short page and a stale count say nothing about why | 12 | 2026-08-27 | 📋 |
| [B1](#b1--2f2--cpu-arenas-for-write--read--merge) | 2f.2 — CPU arenas for write / read / merge | 13 | 2026-08-08 | 📋 |
| [B2](#b2--2f3--per-arena-jemalloc-stats) | 2f.3 — per-arena jemalloc stats | 13 | 2026-08-08 | 📋 |
| [C1](#c1--per-index-role-overrides) | Per-index role overrides | 14 | 2026-07-30 | 📋 |
| [C2](#c2--query-complexity-caps) | Query complexity caps | 14 | 2026-08-10 | 💭 |
| [D1](#d1--reindex) | Reindex | 15 | 2026-08-15 | 📋 |
| [D2](#d2--replication) | Replication | 15 | 2026-08-15 | 📋 |
| [D3](#d3--migration) | Migration | 15 | 2026-08-15 | 📋 |
| [E1](#e1--stage-42--a-max-wal-size-commit-trigger) | Stage 4.2 — a max-WAL-size commit trigger | 16 | 2026-08-19 | 📋 |
| [E2](#e2--stage-3s-deeper-warming-options) | Stage 3's deeper warming options | 16 | 2026-08-19 | 📋 |
| [E3](#e3--measure-recovery-on-the-reporting-node) | Measure recovery on the reporting node | 16 | 2026-08-19 | 📋 |
| [E4](#e4--two-compatibility-paths-with-no-end-to-end-test) | Two compatibility paths with no end-to-end test | 16 | 2026-08-19 | 📋 |
| [F1](#f1--the-cost-of-a-durable-commit-under-read-load) | The cost of a durable commit under read load | — | 2026-08-10 | 📋 |
| [F2](#f2--an-open-loop-load-generator) | An open-loop load generator | — | 2026-08-10 | 📋 |
| [F3](#f3--take-unkeyed-searches-off-the-coordinator) | Take unkeyed searches off the coordinator | — | 2026-08-10 | 📋 |
| [CH1](#ch1--one-scatter-gather-written-twice) … [CH7](#ch7--the-string-fast-collector-repeats-the-macros-body) | Code health, seven items | — | 2026-08-16 | 📋 |
| [OB1](#ob1--fast-false-is-not-honoured-on-a-numeric-field) | `fast: false` is not honoured on a numeric field | — | 2026-08-13 | 📋 |
| [OB2](#ob2--a-facet-field-cannot-be-written-to) | A `facet` field cannot be written to | — | 2026-08-27 | 📋 |

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

- **Nothing in `docs/` mentions MCP setup.** `API_REFERENCE.md`, `CONFIGURATION.md`,
  `DEPLOYMENT.md`, `ARCHITECTURE.md` and `DEVELOPMENT.md` name MCP only in passing. An
  operator reading the documentation tree never reaches the crate README.
- **Index-design guidance for agent context** — how to shape an index so an agent can use it,
  including the cheap mitigation for [D1](#d1--reindex): declare the fields up front with
  `PUT /api/{index}/_config`, or let the first write carry them.
- **The syntax reference's home.** `validate_query` called with no arguments still returns the
  static reference, and the tool's own description tells agents to do exactly that. Moving it
  to `instructions` and a `cameodb://syntax` resource is a change to the tool's contract
  rather than a fix to it, which is why it was held for this item — the description, the
  instructions and the README have to change together.
- **`crates/mcp/README.md`'s "Recent Changes" section is stale**, describing v0.2.3 and v0.1.0
  while the crate is at 0.3.2. It duplicates what `CHANGELOG.md` records properly; delete it
  rather than maintain two histories.

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

📋 **Planned.** Two halves, both about the same field and both fixed in `describe_fields`.

**Projection.** On an index with a shadow field, `describe_fields` in `node_orchestrator.rs`
still describes `id` as an ordinary field although no document returns one — reconstruction
answers with the shadow name *instead of* `id`. So `id` should stop being offered as something
to *project*, while remaining something to query, and something to sort by. Opened by
completion track item 3.

**Sorting**, found by the syntax audit on 2026-08-27. `id` is declared `STRING | STORED` and
never `FAST`, so `sortable_fields` does not contain it and the listing reports
`"sortable": false` — for `id` and for the shadow name that stands for it. Sorting by either is
a supported contract with an end-to-end test (`a_shadow_field_sorts_by_the_key_it_stands_for`,
0.3.2): `unsortable_sort_field` lets both through deliberately, and the result is an approximate
text sort that reports itself as one. Meanwhile `SORT_RULES` tells an agent that `sortable` is
how it knows what can be ordered. So the one field an agent most wants to sort a shadow index by
is advertised as unsortable, and an agent that believes the guidance will never try it.

The fix is to stop deriving that flag from the fast column alone and report what the engine
actually accepts — which means `id`, a shadow name, and any text or string field are sortable
*approximately*, and only a numeric or date field needs `fast`. Three states rather than two, or
one flag plus the honesty about which kind of sort it is.

Do it with [A2](#a2--the-documentation-pass), since the fix and the prose describing field
shapes land in the same place.

### A5 — Semantic routing

📋 **Planned.** Auto-select the best index or indexes for a query's intent, so an agent that
does not know the catalogue does not have to enumerate it. Carried from Phase 12 step 5;
nothing depends on it and nothing blocks it.

### A6 — The syntax reference has drifted from the engine

📋 **Planned**, audited 2026-08-27 against the query path. Cheapest item in this section and the
one that changes most agent behaviour per line edited, so it goes first.

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

**Verified aligned, recorded so it is not re-audited:** the `id:value` fast-path caveat matches
`parse_exact_id_query` condition for condition; a discarded clause does fail an MCP call;
`offset` and its `offset + limit` bound; the approximate-sort mechanics and `_approximate_sort`;
the shadow-field prose; `deny_unknown_fields` on every tool's arguments; the read-only hints and
the deliberate absence of `outputSchema`. The read-only claims in `INSTRUCTIONS` and the
orchestrator skill survive Phase 17 unchanged — deletion is a write, and no tool here writes.

### A7 — A short page and a stale count say nothing about why

📋 **Planned**, and new with [Phase 17](#phase-17--record-deletion--done): deletion made a
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
result as a whole one"* — which an agent cannot honour on a signal it is not given. Two decisions
to make rather than one fix: whether the short page earns a `_warning` note (cheap, and the
existing channel), and whether a document count should be honest about uncommitted deletions at
all, given that the alternative is reading a count from redb that no search agrees with.

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
the same operation as a rebuilt index, from a different source.

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

---

## F. Engine and performance, not filed under a phase

### F1 — The cost of a durable commit under read load

📋 **Planned.** What the rejected linger was meant to paper over, still open: a commit costs
~12.5ms with searches running against ~4.6ms without, and `wal_sync = false` recovers +86% of
write throughput. The lever is **the fsync itself** — WAL device and placement, or a durability
level between "every commit" and "none" — not how the writer groups writes.

The reasoning is in [Mixed read/write load, measured](#mixed-readwrite-load-measured), which
also records why the linger cannot be rebuilt from that reasoning alone.

### F2 — An open-loop load generator

📋 **Planned**, and a prerequisite for three other items. `cameodb-bench` is closed-loop by
construction and says so in its own help text — verified 2026-08-26 — which means it measures
service time at a fixed concurrency and cannot produce an independent arrival process.

Blocking:

- [F1](#f1--the-cost-of-a-durable-commit-under-read-load), and any revisit of the bounded
  linger, which is *provably* untestable against a closed-loop client
- The audit trail's **read path**, where a record really is serialized per request, and the
  trail **under a queue-overrunning workload**, which is the case the `gap` record exists for
- [A3](#a3--protocol-compliance-tests-and-agent-query-benchmarks)'s agent-query latency figures

It is worth having on its own account regardless: every performance number in this document is
closed-loop, and each one should say so.

### F3 — Take unkeyed searches off the coordinator

📋 **Planned.** A keyed write now resolves locally from the published ring and shard placement,
but a search still pays a mailbox round trip to a single actor because the decision depends on
cluster size, which the router has no cheap way to know. Needs the node count published
alongside the ring. Verified still the case 2026-08-26.

---

## G. Code health, reviewed at 0.3.1

Reviewed 2026-08-16, after the paging and MCP work landed; re-checked 2026-08-26, when every
item was still present. Nothing here changes behaviour; each item is a place the code now says
one thing twice, or where the next feature will cost more than it should. Ordered by what each
buys, not by effort.

### CH1 — One scatter-gather, written twice

📋 `engine_search` and `orch_search` in `node_orchestrator.rs` are ~150-line near-duplicates:
the same shard fan-out, gather loop, sort-key stamping, merge, window application, projection
and response assembly. The paging change had to be made in both, and was — which is the
warning, not the reassurance: the next change to one of them will be forgotten in the other.
Extract the shared gather-merge-respond into one function; the two callers differ only in where
the shard map comes from.

**2026-08-26:** the 0.3.2 sort work is the second change that had to be made twice.

### CH2 — The merge primitives deserve their own module

📋 `SearchWindow`, `order_hit_blocks`, `order_shard_hits`, `compare_hits_by_field`,
`stamp_sort_keys` and their tests are a coherent, self-contained unit inside
`node_orchestrator.rs` — and they are the unit every paging invariant lives in, imported by the
HTTP and MCP surfaces alike. A `search_merge` module shrinks the file everyone edits and gives
those invariants one place to be read. `storage/src/lib.rs` has the same disease and the same
cure — the sorted-collector logic, query preparation and schema description are separable.

**2026-08-26:** both files grew rather than shrank since the review —
`node_orchestrator.rs` 9,300 → **9,683** lines, `storage/src/lib.rs` 7,600 → **8,293**.

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

📋 `SearchWindow::checked` (server) and `check_limit` + `check_offset_window` (the `mcp` crate)
enforce the same rule with independently maintained arithmetic and error text. The double check
is deliberate — the schema is the `mcp` crate's promise, and a promise nothing enforces
describes nothing — but two spellings of one rule drift, and the refusal text already differs
between them. Keep both checks; share the arithmetic and the test vectors, so a change to the
rule cannot land in one crate only.

### CH5 — Sort type conversion, four times inline

📋 `crates/server/src/mcp/search.rs` converts `cameodb_mcp::SortSpec` ↔ `storage::SortSpec` with
the same written-out match in four places. One pair of `From` impls, next to the type that owns
the shape.

### CH6 — The federated merge clones every hit

📋 `search_across_indexes` clones each hit out of the response it already owns in order to stamp
`_index_source` on the copy. Taking the array with `as_array_mut` + `std::mem::take` stamps in
place. Bounded by page size rather than corpus size, so this is the hot line of the federated
path being untidy rather than slow — worth doing when the function is next open.

### CH7 — The string-fast collector repeats the macro's body

📋 `collect_sorted!` in `storage/src/lib.rs` covers the u64/i64/f64/date branches; the
string-fast branch writes the same `MultiCollector` block out by hand because its key type is
`String` rather than a copyable numeric. Fold it in by parameterizing the collector expression,
so the next change to how a sorted search counts its total touches one place.

---

## H. Observed, not yet scoped

### OB1 — `fast: false` is not honoured on a numeric field

📋 **Open**, observed 2026-08-13 and filed here 2026-08-26; it had been carried outside the
repository until now.

A `PUT /api/{index}/_config` declaring an i64 field with `"fast": false` reads back from
`GET /api/{index}/_config` as `"fast": true`. **Reproduced 2026-08-27** on a running node while
auditing the sort rules, and it has a consequence worth recording: because every numeric and date
field is forced fast, the refusal `unsortable_sort_field` exists to deliver — "a numeric field
must be declared fast to sort" — is unreachable for those types. What reaches it in practice is a
boolean, ip, json or facet field. Mechanism still unconfirmed. `FieldDef::new` forces
`fast` for `I64` / `U64` / `F64` / `Date`, so the likely cause is a write path re-deriving a
declared field through it rather than preserving what the caller declared — but the one
schema-evolution call site that reaches `FieldDef::new` only adds fields not already present,
so the path has to be found before the fix is written.

Not an MCP defect, and not a sort defect: the engine's fast-column guard refuses a genuinely
non-fast sort correctly. What is wrong is that the config says one thing and the index does
another, which is exactly the distinction `searchable` and `sortable` exist to report.

### OB2 — A `facet` field cannot be written to

📋 **Open**, found 2026-08-27 while auditing what the MCP syntax reference advertises.

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
   Still open, and carried forward as
   [A4](#a4--what-a-schema-listing-says-about-id-for-projection-and-for-sorting): on an index with a shadow field, `id` is described as
   an ordinary field although no document returns one — the identifier comes back under the shadow
   name — so `id` should stop being offered as something to *project*, while remaining something
   to query.

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
  no hot config reload (rotation is add-key → migrate → remove-key → restart, already better
  than the PSK's "stop every node" gap).

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
against a closed-loop client it is provably useless. The remaining honest levers on mixed
write cost are the fsync itself (device, `wal_sync`, WAL placement) rather than how the
writer groups.

## Settled decisions

Questions asked and answered. Two are rejections, kept so they are not rebuilt.

1. ~~**A latency harness.**~~ ✅ Landed 2026-08-09 as `cameodb-bench` (`crates/bench`): percentiles for writes and searches, the node's `took_ms` beside the client-observed figure, and the worker-pool delta over the measured window. Closed-loop, so runs are comparable at equal concurrency rather than being an SLA
2. ~~**Document and default the affinity flags.**~~ ✅ Landed 2026-08-09, and the answer was *no*: see [The affinity flags, measured](#the-affinity-flags-measured). Both stay `false`, now present and explained in `cameodb.example.toml`, `crates/server/cameodb.toml`, `docker/cameodb-docker.toml` and `docs/CONFIGURATION.md`
3. ~~**Give a worker more than one operation at a time.**~~ ✅ Landed 2026-08-10. A worker now carries up to 8 operations, bounded by a semaphore acquired *before* the receive so the channel stays the backpressure signal. Worth **+65-70% write throughput and −64% on p90** where the pool is the constraint, and nothing where it is not — see [Worker concurrency, measured](#worker-concurrency-measured). It did *not* redeem the affinity flags, which was the other reason to do it
4. ~~**A bounded linger before the writer commits.**~~ ❌ Built and rejected 2026-08-10 — no measurable gain at any concurrency tested, and the arrival arithmetic says there cannot be one against a closed-loop client. Removed; the reasoning is recorded in [Mixed read/write load, measured](#mixed-readwrite-load-measured) so it is not rebuilt. **An open-loop load generator is the prerequisite for revisiting it** — and is worth having anyway, since every number in this document is closed-loop

