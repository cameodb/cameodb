# Changelog

All notable changes to CameoDB are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.3] - 2026-08-31

### Added

- **Record deletion.** CameoDB could delete an index and never a document. Two endpoints, both
  needing `write` — a key that can write can already overwrite any document with anything, so
  withholding removal protects nothing:

  ```
  DELETE /api/{index}/document?id=<id>[&routing_key=<key>]
  POST   /api/{index}/_bulk/delete          body: ["b1", {"id": "b2", "routing_key": "acme"}]
  ```

  The single delete answers what a write answers, one word apart: the id, `"result": "deleted"`,
  the sequence it took and the shard that served it. The bulk one answers as `_bulk` does, and
  takes either entry shape — a list of bare ids is what almost every caller has, and making them
  wrap each one in an object to say nothing extra is a worse API than accepting both.

  The id travels in the query rather than in a path segment, and both alternatives were rejected
  on their merits. `DELETE /api/{index}/document/{id}` would need a second placeholder in the
  matcher that classifies every request for authorization, and an id is an arbitrary string:
  authorization reads the raw path while the handler reads the decoded one, so an id containing
  `%2F` is two different documents to the two of them. A body on `DELETE` is what proxies drop.
  Deleting an index already carries its parameters in the query.

  Almost none of this is new machinery. `WalOp::Delete` has always existed and both storage write
  paths have always handled it, so a delete is coalesced into the same transaction as the writes
  that arrive with it, counts toward the same commit threshold, and recovers through the same WAL
  replay. It is also the first operation the engine can always serve: carrying no document, it
  cannot present a field the schema does not know, so there is no schema-evolution path to defer
  to the actor.

  **Deleting is idempotent.** An id the index does not hold is answered as `deleted`, the same way
  writing over an existing document is answered as `created`. A missing *index* is a 404, and the
  delete creates nothing on its way to saying so.

  **Visibility has two tiers, because two engines answer.** An `id:VALUE` lookup is served from the
  key-value store, so a delete is visible there immediately; a content query is served by the
  search index, where the removal lands at the next commit — the idle timeout, sooner under load,
  or at once through the admin commit endpoint. Until then the document is counted in `total_hits`
  but absent from `hits`, which is the honest reading of a mid-flight delete: reducing the count
  instead would break the arithmetic that pages through results.

  **Routing.** On a normal index, and on one with a shadow key such as `sha1`, the id routes the
  delete on its own. On an index that routes by some other field — a tenant, a customer — the id
  does not say which shard holds the row, so the delete must carry the same `routing_key` the
  write used, and without it the request is refused with a `400` naming the field. Fanning a
  keyless delete out to every shard would be correct and is deliberately not done: it costs
  shards × nodes writer transactions to remove one row, and the router already refuses to
  broadcast a write. Deleting by query is not available; the ids have to be named.

- **`CameoClient::delete_document` and `CameoClient::delete_documents`**, and `delete <index>
  --id <ID[,ID…]>` in the CLI and the REPL, with `--ids-file <PATH>` for a longer list and
  `--routing-key <KEY>` for a custom-routing index. One `--id` may name several ids
  comma-separated, and the flag also repeats: `--id b1,b2` and `--id b1 --id b2` are the same
  request, and they compose. A file line is one id taken whole rather than a list, which is what
  keeps an id containing a comma nameable at all.

  `delete <index>` naming no documents still deletes the index, which is what it has always
  meant. The direction that had to be made impossible is the other one, so *naming* ids and
  yielding none — an empty ids file, `--id ,`, `--id ""` — is refused rather than falling through
  to deleting the index. The command line and the REPL share the one function that decides this,
  so the comma rule and the refusal cannot drift apart; the REPL gained `--ids-file` on the way,
  which it had not had. `delete <index>` with no ids named still deletes the index,
  which is what it has always meant; the mistake that had to be impossible is the other
  direction, so an ids file that yields no ids is an error rather than a fall-through.

### Changed

- **The MCP query-syntax reference now describes the engine it documents.** It is a single source
  rendered into four surfaces — the `search_index` description, the answer `validate_query`
  returns, the per-field `query_hint` on `describe_index` and `list_indexes`, and the crate
  README — so an agent builds queries from it rather than from experiment, and a wrong entry is
  wrong in four places. An audit against the query path, then against a running node, found:

  - **`_seq` was documented as "present in every index and technically queryable"**, having been
    retired: a new index never declares it, every field listing filters the name, and a sort
    naming it is refused. The rule is deleted, not corrected — an agent never sees the field.
  - **Date literals were understated to two of about ten forms.** A datetime with no zone,
    `YYYY/MM/DD`, `YYYY.MM.DD`, `YYYYMMDD`, `YYYYMMDDHHMM`, `YYYYMMDDHHMMSS`, Unix epoch
    seconds, a bare month and a bare year all parse, and all work in a range, a comparison and an
    `IN` set. Two rules came out of probing rather than reading: a bare date is an **exact
    instant**, so `created:2024-06-15` means midnight and matches nothing unless a document sits
    on that second — a day is a range — and a literal containing a space **must be quoted**, or
    the value ends at the space and `12:00:00` is read as a new clause. A literal outside the
    representable range is clamped rather than refused.
  - **An unsortable field is a refusal, not a degraded result.** The rules said such a field
    "needs a fast column to be sorted at all"; the request actually fails with `cannot sort by
    'FIELD'` and the reason, before any shard runs. Boolean, ip, json and facet are the types
    this catches in practice.
  - **A shadow field can be sorted**, approximately, ordering by the identifier it stands for and
    reporting `_approximate_sort` under the name the hits carry it by. The shadow rule covered
    querying and projection and was silent on the third thing the field is for.
  - **`limit 0` — count-only, and the cheapest way to ask how many documents match** — was
    documented only in a hand-written schema string, so it was missing from the reference and the
    README.
  - Two caveats gained the behaviour they were thinner than: a prefix the analyzer cannot reduce
    to one term is matched as that term exactly and reported, and a facet path ends at the first
    space.

- **A bulk write no longer stores documents that failed validation.** It collected the validation
  errors, logged that it would continue with the valid documents, and then did neither: nothing
  was dropped, so an invalid document was written exactly as if it had passed, and nothing was
  reported, so the response counted it among the successes. A single write refused the same
  document outright, which made the rule a property of which endpoint the caller used rather than
  of the index.

  The validation summary now carries each failure's position in the batch, the rejected rows are
  left unwritten, and their reasons reach the caller in the response's `errors` — so
  `items_received` minus `items_written` is accounted for. Rejecting the whole batch was the other
  defensible policy and is not this one's: a bulk write already reports partial success, and one
  unstorable row in an import is no reason to discard the rows around it.

  **This changes what an existing pipeline sees.** A caller that was writing documents which fail
  schema validation — a type mismatch, or a shadow field disagreeing with the identifier — will
  see `items_written` fall and `errors` appear where neither did before. Those documents were
  being stored wrong, and the count that called them written was the thing that was false.

- **A document whose shadow field disagrees with its identifier is refused.** A shadow field is
  the document key under the source's own name and nothing holds a second copy of the value: the
  write path strips the field out of the stored body and the read path writes the key back under
  it. That round trip returns what was written only while the two agree, so a document saying
  otherwise had the value it carried silently and unrecoverably discarded —
  `{"id": "AAA", "md5": "BBB"}` came back as `md5: "AAA"`, and `md5:BBB` matched nothing while
  `md5:AAA` matched. Both write paths now refuse it, naming the field and both values.

  The rest of the design already rested on this invariant: `document_key_field` picks any one
  shadow name to read the key back under and reconstruction writes the key under every one, both
  defensible only if all of them mean the key. The bundled importer checks it before promoting a
  column to shadow; this is the same rule for a write arriving by any other route. A document may
  still omit the shadow field entirely, which is the ordinary shape of a rewritten document.

### Fixed

- **A shadow field only worked alone.** It is the document key under the source's own name, and
  only the bare `sha256:VALUE` lookup ever resolved: named inside a larger query the clause was
  dropped and reported, and a projection asking for `id` returned a document with nothing in it.
  The code meant to handle this parsed the query as a JSON query DSL that this engine does not
  speak, so it matched nothing and had never done anything.

  A shadow reference is now rewritten to `id` before the parser sees it, so it composes like any
  other clause — in a conjunction, a negation, a range, a set or a prefix — and a projection
  naming `id` is rewritten to the name the hits carry. Sorting by either name orders by the
  identifier and reports `_approximate_sort` under the name the documents use.

- **The key-value fast path and the parser disagreed about the same identifier.** A whole-query
  `id:VALUE` is answered from the key-value store without parsing, and it took the value
  literally: `sha256:urn\:x\:1` looked up a key with the backslash still in it and found
  nothing, while the same clause inside a conjunction reached the parser, which resolves the
  escape, and returned the document. `sha256:VALUE^2` missed for the same reason — a boost is
  syntax the key-value store cannot answer.

  Escapes are resolved before the lookup, and `^` now falls through to the search index, which
  understands it. An identifier that genuinely contains one stays reachable by escaping it, and
  identically from both paths. `~` is deliberately left on the fast path: Tantivy reads it as
  slop only after a quoted phrase, so against a bare term it is an ordinary character an
  identifier may contain.

- **`validate_query` reported fields the query did not name.** Three separate scanners answered
  "where are the field names in this query", and the one furthest from the engine knew nothing of
  phrases, ranges, or where a value begins. It read `00` out of an RFC3339 timestamp and `https`
  out of a URL, so a working query came back with two or three confident warnings about fields
  the index does not have — from the tool the guidance sends an agent to when it doubts a query,
  which is the one place a false alarm is indistinguishable from a real one.

  There is now one scanner, in the engine: the shadow rewriter splices over its spans, the schema
  check classifies its names, and the MCP layer lists them, so the three cannot drift. It also
  splits a token at `(`, since a parenthesis ends one clause and begins another without needing a
  space — read whole, `AND(sha256:x)` named a field called `AND(sha256`.

- **`describe_index` listed `id` and a shadow field as unrelated fields.** On such an index both
  are searchable text fields standing for the same value, and `id` is the one an agent reaches
  for — it is the field every other index has, and the syntax reference calls it the fastest
  retrieval there is. Querying it works, so the description cannot simply drop it; without
  something relating the two, hits carrying no `id` had no explanation.

  The `id` entry now carries `returned_as`, naming the field the hits use in its place, on
  `describe_index` and on `validate_query`'s `available_fields`. Omitting `id` instead was
  rejected: `id:VALUE` still answers on such an index, so a description without it would make
  `validate_query` report the working form as an unknown field.

- **A declared `id` type contradicted the key the index builds.** The index builder skips `id` and
  creates the key itself — raw-tokenized, stored, never fast, whatever the schema declared — but
  enrichment pinned only the indexed, stored and tokenizer flags. The declared type and `fast`
  survived, and three readers trusted them: `_config` reported the declaration, the slow write
  validation compared it against the `Text` it infers for a key, and a sort merge built its key
  from it.

  An index declaring `id` as `i64` therefore refused every document of a batch large enough to
  take the slow path — 1200 sent, 1200 rejected against a type the index does not use. One
  declaring it `date` answered an ascending sort with `k20, k30, k10`, because the merge key
  parsed each identifier as a date, got nothing, and sorted every hit last. Neither reported an
  error. `_config` also showed `fast: true` next to `sortable: false`, the mismatch `can_be_fast`
  already settles for the types that carry no column. Enrichment now pins the type and `fast`
  alongside the other three.

- **A facet value the type could not hold would have aborted the node.**
  `Facet: From<&str>` is `Facet::from_text(path).unwrap()`, and a facet path must be non-empty and
  begin with `/` — so `add_facet(field, "electronics/phones")` panics, on the shard's writer
  thread, from a document body. With `panic = "abort"` in the release profile that ends the
  process rather than the request.

  Nothing reached it: the orchestrator infers `Text` from every JSON string and refuses it against
  a declared `facet` field, which is why a facet field cannot be written to at all. That refusal
  was load-bearing by accident, and the wrong thing to rely on — making facets writable would have
  turned a dead field type into a way to stop a node from a document. The value is now checked
  where it enters the index, on both write paths, and refused as a bad value naming the field and
  what a facet path looks like. The replay path skips it with a warning instead: the value is
  already committed, and failing an index open over one field of one document serves nobody.

- **A malformed document answered `500 Internal server error`.** Every document-validation
  refusal — a missing inner `id`, a value the declared type cannot hold — is the caller's fault,
  and `500` is both wrong about that and an instruction to retry a request that cannot succeed.
  They are now `400`s naming the field.

  Two things were in the way. The write handlers classified errors by text rather than through
  `AppError::from_route`, and `io::ErrorKind::InvalidData` — which every one of those refusals
  carries — was not among the kinds it read as a bad request. Worse, `ask_orchestrator` formatted
  the actor's error into a string and wrapped it in `io::Error::other`, flattening the kind to
  `Other`, so *any* operation answered through the actor mailbox arrived at the HTTP layer
  unclassifiable. It now returns the handler's own error unchanged and describes only the delivery
  failures, which the handler cannot describe for itself.

- **A search that could not return every document it counted said nothing about it.** A search
  counts matches in the search index and fetches bodies from the key-value store, and a deletion
  clears the store first — so between a delete and the next commit a match is counted and has no
  body, and `total_hits` of five arrives with four hits. Reachable in ordinary operation only
  since record deletion shipped, and invisible in the hits themselves, every one of which is
  real. An MCP search now carries a `_warning` saying how many documents could not be read back
  and why, on the one condition paging cannot explain: fewer hits than `min(limit, total_hits −
  offset)`. Silent on a full page, a last page holding the remainder, an empty result and a
  count-only query.

  Document counts keep their number and gain a stated boundary. `document_count` is the count in
  the search index as of its last commit, which is exactly what `total_hits` is — reading redb
  instead would give a figure no search agrees with, and two honest-looking numbers that never
  match is worse than one number with its meaning written down. `describe_index`, `list_indexes`
  and `get_catalog_stats` now say so.

- **Two tool descriptions shipped runs of nine and five literal spaces** after every paragraph
  break. A `\` at the end of a line in a Rust string swallows the indentation that follows; a
  `\n` written into the string keeps it. A tool description sits in the caller's context for the
  whole session, and this is the one class of defect in it that no reviewer notices, because the
  source looks right. A test now walks every description and refuses a run of two spaces outside
  the syntax reference's own padded table.

- **A document read once and then updated or deleted kept serving its previous body.** The
  per-index read cache in front of redb is populated by every search that hydrates a document
  body, and until now it was cleared in exactly one place: deleting the whole index. Neither the
  single-write path nor the batch path touched it, so a row that changed under a cached entry was
  never noticed — an update served the body from before it, and the only thing that eventually
  corrected either was the 1024-entries-per-index FIFO evicting the entry.

  The window was short and unpredictable, which is why it went unseen: the entry point is a search
  hydration, so it needed the same document to be read and then written. It is now impossible.
  The ids a write touched are dropped from the cache by the same code that writes the rows, after
  the redb transaction commits rather than before.

  Removing the entries is not sufficient on its own. A reader that opened its transaction before
  the write committed legitimately still sees the pre-write row, and if it installs that body
  after the removal, nothing will take it out again. Each index's cache therefore carries a
  generation: a reader reads it before opening its transaction and quotes it back when it caches,
  and an insert whose generation a write has moved on from is declined. Both sides touch the cache
  under the same entry guard, so the check and the insert cannot interleave with a write's bump and
  removal.

  Two regression tests cover it — update and delete on both write paths, and the two halves of the
  reader/writer race driven in the order that produces it, which a single thread cannot interleave.

- **Shard-affine dispatch could place a document on a shard the routing ring did not own.** The
  hint that picks a worker is computed from the request's routing key before any schema is loaded,
  while the write itself routes by the document's own routing field, which outranks it. On an index
  whose routing field is a real, non-key field the two disagree — and the write took the hint. The
  result was a document on a shard the ring believed belonged elsewhere: invisible to searches,
  which are scatter-gather, until the same id was written again through a path that carries no hint
  and landed on the shard the ring does name, leaving one id in two places for a search to return
  twice.

  The hint now chooses only the worker, which is what it exists for and what actually saves the
  cross-core wakeup; the ring always chooses the shard. It is no longer possible to express the
  divergence: the hint is not passed to the engine at all. Only deployments with
  `shard_affine_dispatch = true`, which is not the default, could reach this.

  The routing rule the hint used to overrule was written out identically in three places, so it is
  now one function with its precedence documented and pinned by a test: the document's routing
  field, then the caller's routing key, then the id, then a hash of the document.

- **A batch of deletions left the index size and document count stale** in `/_indexes` until the
  measurement cache expired: the invalidation asked whether the batch had written or updated a
  document, and a batch of pure deletions does neither.

- **The route-authorization guard compared paths and ignored methods.** The test that proves
  every mounted route has a row in the authorization table matched on the path alone, so a second
  verb on an already-classified path satisfied it while every request to that verb was refused
  authentication. Closed rather than open, but silent — and it is exactly the shape a new verb on
  an existing resource takes, which is what record deletion needed. The guard now matches
  (method, path) in both directions, and fails outright on a route it cannot parse rather than
  quietly leaving it out of the check.

- **One MCP test failed intermittently on a timing figure.** The regression test that pins a tool
  result to one shape whatever protocol revision a client states compared whole response
  envelopes, `took_ms` included — so two calls that agreed on every hit, score and count failed
  the comparison when one took 10 ms and the other 1 ms. Durations are now flattened on both sides
  before the comparison; everything else still has to match exactly.

## [0.3.2] - 2026-08-20

### Fixed

- **`PUT /api/{index}/_config` reported `_seq` as a field of the index.** `_seq` is the engine's
  internal WAL sequence number. Every other listing hides it, but the create-config response is
  the one that does not go through the shared field-describing path — and it normalizes the
  submitted schema first, which *inserts* `_seq`. Creating an index therefore advertised a field
  the caller never declared and cannot use.

- **`sort=_seq` was accepted and silently returned a partial ordering.** The field is `fast`, so
  every check in the sort path passed and results were ordered correctly *within* a shard. But
  document bodies are served from redb, which has no `_seq` key, so no sort key was stamped on
  the results and a scatter-gather merge across shards had nothing to order by. The engine now
  refuses it like any other unknown field, which is what `sortable_fields` already advertised.

- **A sort the index could not answer came back as an empty result page.** A sort fails in every
  shard at once or in none of them, and a scatter-gather reports the first as a partial failure:
  `200`, `hits: []`, `total_hits: 0`, and the reason only in per-shard `errors`. A caller reading
  the hits saw "nothing matched" for a query that was never run.

  The sort field is now checked before the fan-out and an unusable one is a `400` naming it. The
  question asked is the engine's own — "can I order by this column?" — rather than the narrower
  "does a column of this name exist", because both refusals reach the caller identically. So a
  boolean or any other non-text field with no fast column is refused, and so is `_seq`, whose
  schema record survives on every index built before the field was retired: looking the name up
  in the schema found it there and passed it through, on precisely the indexes that have it.
  Nothing that worked before is refused: a field with no fast column still sorts approximately,
  as `_approximate_sort` reports, and `id` still sorts.

- **`sort` by a shadow field did not work, and `sort=id` on an index that has one was ordered
  per shard.** A shadow field is the document key under the source's own name, and the query
  path already maps it to `id` — a sort did not, so `sort=sha1` named a column that does not
  exist and matched the empty-page case above. It now maps the same way the query does, in the
  engine, so every caller of `search_documents` gets it and not only the HTTP router.

  The same mapping was missing on the way back, which is why `sort=id` was affected too:
  reconstruction answers with the shadow name *instead of* `id`, so a merge looking for `id` on
  the hits found no key to order by and returned each shard's block in turn — the right
  documents in the wrong order, with nothing in the response saying so. The key is now read
  under the name the document carries. `_approximate_sort` reports the field the caller asked
  for rather than the column the engine ordered on.

- **An index could be re-examined by recovery on every boot forever.** If the process stopped
  between a Tantivy commit and the WAL truncation that follows it, the WAL kept entries the
  checkpoint already covered. Recovery correctly replayed nothing — but it also left them there,
  and startup reads a non-empty WAL as "needs recovery", so the index opened an `IndexWriter` to
  discover there was nothing to do at every subsequent boot, and only stopped once it happened to
  take another write. Recovery now finishes the truncation, which it can prove is safe.

- **Documents written after a clean restart could disappear from the search index.** The WAL
  sequence counter was seeded from the WAL alone, and a commit truncates the WAL — so every
  cleanly stopped index reopened with an empty one and restarted numbering at zero, reissuing
  sequence numbers it had already spent. If the process was then killed before the next commit,
  recovery compared that reissued tail against a checkpoint far above it, concluded there was
  nothing to replay, and skipped it. redb still held the documents and `GET` by key returned
  them; they were simply missing from every search. The counter is now seeded from the durable
  checkpoint as well, which is monotonic across restarts by construction.

### Changed

- **The WAL no longer stores a second copy of every document.** A `wal_<index>` entry held the
  whole operation, body included, while the same transaction wrote that body to `data_<index>` —
  so every write serialised the document twice and fsynced it twice. Entries are now the document
  id and one tag byte. On a 1 KB document that is roughly 6 bytes where there were over a
  thousand, and it is on the hot path of every write.

  Nothing is lost by it. The WAL's job is to say *which* documents Tantivy may be behind on, and
  the authoritative body is in `data_<index>` in the same transaction. Recovery reads the id and
  lets the committed row decide the operation: a row means index the document as it now stands,
  no row means it was deleted — a put always writes the row and a delete always removes it, so
  the two cases are exact.

  Replay therefore converges on committed state instead of re-enacting a log, which makes it
  strictly less work: each id is applied once, so a tail that wrote one document twenty times, or
  put a document and then deleted it, resolves in a single Tantivy operation rather than twenty
  or two. WAL entries written by earlier builds still decode — only their id is taken — so an
  upgrade needs no migration of a tail left behind by the process that died.

- **New indices no longer carry the `_seq` field.** It was a `stored` + `fast` u64 on every
  document, costing 8 bytes in the row store plus a columnar entry re-merged on every segment
  merge, and its only reader was the checkpoint scan that the commit payload replaced. Since the
  Tantivy document holds just `id` and the indexed fields — bodies live in redb — it was a large
  share of a narrow index's doc store.

  Indices that already have the column keep it, are still written to, and still recover through
  it, so no migration is required and no on-disk index changes shape. Rebuilding an index drops
  the field. One visible consequence: `_seq` was always resolvable in a query and matched
  nothing useful; on an index built without it, `_seq:>0` is now correctly reported as an unknown
  field.

- **Crash recovery is bounded by what was in flight, not by how much data the node holds.**
  Recovery on a large multi-shard node took minutes, because deciding whether an index needed
  replay could require opening it and ordering its `_seq` fast field — O(segments × docs) per
  index, on every boot, for any index not yet carrying recovery metadata.

  The checkpoint now travels inside Tantivy's own commit payload, written by the same operation
  that publishes the segments, so it cannot describe a segment set a crash prevented from
  landing. Since a commit deletes the WAL entries it covers, an empty `wal_<index>` is now
  sufficient proof that an index is in sync: startup partitions every index in a single redb
  read transaction per shard, asking each WAL table for its last key. An idle 30 TB index costs
  one B-tree descent, the same as an empty one, and no Tantivy index, writer or searcher is
  opened for it. Only indices with an actual tail are replayed, and only over that tail.

  Existing data needs no migration. An index whose last commit predates the stamp falls back to
  the `_recovery_meta` row, and one with neither is scanned once and backfilled, so the
  expensive path runs at most once per index rather than on every boot.

- **Recovery can no longer re-trigger the OOM it is recovering from.** The number of indices
  holding an `IndexWriter` during replay is now capped process-wide rather than per shard, so a
  16-shard node no longer allocates 16 × cores indexing arenas at once to recover.

- **Startup reader warmup runs under a 60-second budget.** Warming faults term dictionaries in
  from mmap across every shard simultaneously; past that point the IO storm costs the queries
  already arriving more than it saves the ones that have not. Indices are warmed smallest-first,
  what is skipped is logged, and the remainder warms on first access as before.

### Added

- **`HybridStore::pending_wal_entries`** — how many writes are waiting for Tantivy to catch up on
  an index. This is the quantity that decides recovery cost, and zero is what lets startup skip an
  index entirely; a value that keeps climbing means commits are not keeping pace with writes.

- **`HybridStore::has_open_writer`** — whether an index currently holds an `IndexWriter`, the
  expensive per-index resource. Distinguishes a boot that scales with in-flight writes from one
  that scales with stored data.

### Documentation

- **`wal_sync = false` is now described accurately** in the configuration guide, the example
  config, and the "speed" profile that emits it. It was listed only as a risk of losing recent
  writes. The sharper hazard is that redb's durability drops to `None`, so a kill can lose rows
  Tantivy had already committed — leaving the derived search index *ahead* of the document store
  it is derived from. Recovery only replays forward and cannot repair that, so searches return
  hits whose documents no longer exist until they are rewritten or the index is rebuilt.

## [0.3.1] - 2026-08-16

### Added

- **Paging: `offset` on a search.** `total_hits` reported how many documents matched and there was
  no way to reach the eleventh. Both HTTP search routes, both MCP search tools, the bundled SDK
  (`search(..., offset, ...)`), the `cameodb search --offset` flag and the query grammar
  (`title:rust limit 10 offset 20`) now take one, and every search response reports the `offset` it
  ran with beside its `limit`.

  The skip is applied **once, after merging**, never inside a source. Every shard and every index
  is asked for `offset + limit` hits from the front, because all of one page may come from one of
  them — a source that skipped `offset` of its own hits would drop rows belonging on the page and
  promote rows that do not. This is also why Tantivy's `and_offset` is not used: right for one
  segment, wrong for a scatter-gather.

  The consequence a caller has to know is that **`max_search_limit` bounds `offset + limit`**, not
  either alone: the window is what gets fetched, so a deep page costs what a large limit costs. The
  bound is enforced on every surface, and an `offset` sent with no `limit` counts the node's
  default against it.

  Paging past the end returns an empty page with `total_hits` intact and a warning naming the last
  offset that holds a document — not an error, and not a report that the query matched nothing.

- **Text and string fields can be sorted exactly.** Declaring such a field `fast` now builds the
  string fast column Tantivy orders on by term ordinal, giving a true lexicographic order over
  every match instead of the old approximation. The column is written at index time, so the
  declaration has to be in place before the data is.

- **`sortable` on every field description**, beside `searchable`, and for the same reason: `fast`
  is a declaration and `sortable` is whether the built index carries the column a sort orders on.
  They differ for a field declared after the index was built, and only the engine can tell — a
  numeric sort on a `fast` field with no column fails, and a text sort on one silently returns an
  approximate order. Reported by `GET /api/{index}/_config`, `GET /_indexes`,
  `GET /_cluster/_indexes` and `describe_index`.

- **An approximate sort is reported on the response that carries it.** A sort on a text field with
  no fast column returns the alphabetical order of the top-scoring candidates rather than of every
  match, and the hits look exactly like an exact answer. The response now carries
  `_approximate_sort` naming the field, and the MCP tools add a `_warning` explaining what it means
  for the order and why paging through it does not work. It was previously a line in the node's
  log, where the caller could not see it.

- **`validate_query` runs the real parser.** It checked that quotes and parentheses balanced and
  that named fields existed, which left out the case the tool is recommended for: a query that
  balances fine and still does not parse. `title:`, `title:[2020 TO`, `year:{2020 TO 2021` and a
  leading `AND` all pass a structural check and none of them parse. The tool now reports `parses`,
  `syntax_errors` (the parser's own message, with position), `normalized_query` (what the engine
  actually runs after rewriting) and `discarded_clauses` (clauses that parse but can never match).
  `parses` is `null` rather than `true` when the query could not be checked, so an unchecked query
  never reads as a passing one. Backed by a new `HybridStore::validate_query`, which parses
  against an index without searching it — field-name resolution needs a built index, so nothing
  above the engine could answer this.

### Fixed

- **A federated page was the wrong page.** `search_across_indexes` passed the caller's `offset` down
  to each index *and* applied it again to the merged result, so page 2 was page 3 of an order
  assembled from the wrong candidates — each index having already dropped its own first `offset`
  hits, which are not the same documents as the merged first `offset`. Each index is now asked for
  `offset + limit` from the front and the skip happens once, after the merge.

- **`POST /api/{index}/search` enforced no limit at all.** Neither `limit` nor `offset` was checked
  against `max_search_limit` on the HTTP surface — the ceiling existed only on the MCP tools. Since
  the engine fetches `offset + limit` hits and Tantivy's collector allocates against that number
  before matching anything, `{"limit": 10, "offset": 500000000}` was a request that looks like ten
  documents and asks the node for an allocation of its own choosing. Both routes now bound the
  window and refuse with `400`.

- **`POST /api/{index}/search/stream` accepted an `offset` and ignored it.** Both routes share one
  payload type, so the field deserialized and was dropped: a client paging over the stream received
  page 1 every time with nothing saying so. A non-zero offset is now refused, naming the route that
  does page.

- **A page past the end was reported as a query that matched nothing.** The MCP zero-results advice
  reads only the query text, so an agent that paged too far was told its quoted phrase or `AND`
  clause was too narrow — about a query that had matched hundreds of documents. The two cases are
  now told apart, and the paging one names the last offset that holds a hit.

- **An omitted `limit` counted as zero when bounding a window**, so `offset` at exactly
  `max_search_limit` passed a check the engine then exceeded by the default limit. The advertised
  ceiling is now the enforced one.

- **The cluster index listing no longer describes the same index two ways.** `GET
  /_cluster/_indexes` dropped `memory_*` and `warm_shards` from its top-level rollup while
  keeping them in the nested per-node array, because the merge went through a private struct
  that lacked those fields. It now renders the per-index shape a single node renders.
- **`cameodb://indexes/{index}/schema` returned `null` for every index.** The resource read a
  `schema` key that `describe_index` had already removed, so it paid for the lookup and answered
  nothing. No test covered it.
- **`validate_query` reported two different field counts in one response**, because `_seq` was
  filtered out of some field lists and not others. It is filtered in one place now.
- **`PATCH /api/{index}/_schema` works.** It answered `500` for every index that had ever been
  written to. The cause was not the endpoint: persisting *any* schema against an index whose
  writer is open stranded that writer, because storing a schema evicts the field-handle cache
  while the writer cache keeps the writer, and the acquisition path needed both — so a live
  index fell through to opening a second `IndexWriter` against a lockfile the first still held.
  A cached writer with no cached field handles now rebuilds them from its own index. The same
  trap was reachable from the write and bulk-write paths with no HTTP involved.
- **A schema edit no longer erases the rest of the schema.** The handler read the schema out
  through the `GET /_config` response and wrote it back, and that response carries only `fields`
  and `description` — so every edit silently reset `routing_field_name` to `id`, changing which
  shard a document routes to, along with `version`, `fingerprint`, `created_at` and `updated_at`.
  The schema is now edited in place and nothing round-trips through a response shape.

### Changed

- **One description of an index, built once by the engine.** `GET /_indexes`,
  `GET /_cluster/_indexes` and `GET /api/{index}/_config` now return the same per-index shape,
  and every field in it carries the same keys: `name`, `type`, `indexed`, `stored`, `fast`,
  `shadow`, `searchable`, and `description` where one was written. Identity is `name`
  everywhere, and `fields` is an ordered array (`id` first, then alphabetical) rather than a map
  in one place and a name list in another. **Breaking**: the listing's `field_names` is replaced
  by `fields`; `/api/{index}/_config` returns `{name, description?, field_count, fields[]}`
  instead of a map keyed by field name; `field_type` is now `type` and `is_shadow` is `shadow`.
  - **The listing carries field types for the first time.** It previously gave names only, so
    every caller fetched the schema again per index to learn the types — the bundled client
    sequentially. `cameodb list indexes` and `list index <name>` now make **one** request
    instead of `1 + N` and `2`; the interactive REPL made `1 + 2N`, because its completion cache
    re-fetched every schema the command had just read.
  - **`searchable` is new, and is not `indexed`.** `indexed` is what the schema declares;
    `searchable` is whether the built index has a column for the field. They differ for a field
    declared after the index was built, which is `indexed` and yet matches nothing until the
    index data is rebuilt. Only the engine can tell them apart, so it now reports it rather than
    leaving each caller to guess — the MCP tools previously called such a field queryable.
  - **Sizes are reported in bytes** (`index_size_bytes`, `memory_bytes`, `data_size_bytes`)
    instead of pre-rounded megabytes. The cluster listing sums these across nodes, and summing
    values already rounded to whole megabytes lost up to a megabyte per node.
- **A field type is reported in lowercase, matching every other surface.** A schema serialized
  `Date` and `Boolean` while the query syntax reference, the per-field query hints and the
  deserializer's own canonical list all say `date` and `boolean` — so an agent reading a schema
  and then reading how to query it was given two spellings of one type. Serialization now
  delegates to the same function those surfaces use, so a new type cannot introduce a third
  spelling. **Backward compatible in both directions**: deserialization already lowercased before
  matching, so schemas persisted with the capitalized form still load, and API callers may still
  send either form (plus the existing aliases such as `integer`, `datetime`, `bool`). Only
  consumers that string-match the capitalized output need to change.
- **`PATCH /api/{index}/_schema` reports a field it cannot make searchable yet, instead of
  quietly succeeding.** A Tantivy index only has columns for the fields declared when it was
  built, so marking a later-discovered field `indexed` does not make it searchable right away.
  The edit is still applied — the stored schema is the declaration the index is rebuilt from, so
  this is the first step of declare-then-reingest — and the response now names those fields under
  `pending_reindex_fields` with a note saying what completes the change. Until the rebuild, a
  query naming such a field matches nothing and reports the clause as discarded rather than
  returning a narrower answer silently. See `docs/API_REFERENCE.md`.
- A field name **no shard** recognises refuses the whole request with `409`; a name absent from
  only some shards does not. Shards normally agree — a declared schema is fanned out to all of
  them, and an inferred one is sampled from the first 200 documents and persisted everywhere
  before the first write — but semi-structured input written a document at a time can leave a
  field on only the shards that received it, and those are exactly the shards that can act on it.
- An empty `field_updates` is now `400` rather than a success that changed nothing.

## [0.3.0] - 2026-08-10

The authentication release. A node can now require a credential on every route, decide what
that credential may do and which indexes it may touch, meter what it costs, and keep a record
of what it did — none of which existed in 0.2.x. Alongside it, the write path was measured
and reworked rather than tuned by assumption.

Headline changes, each detailed below: API key authentication with capabilities and per-key
index scoping (enforced at one ingress chokepoint, MCP included), HTTPS via rustls, a cluster
pre-shared key, security posture profiles that refuse to start a misconfigured node, MCP tool
rate limiting, an audit trail, and +65-70% write throughput from letting a worker carry more
than one operation at a time.

**Upgrading from 0.2.x:** every new security feature is off by default — authentication, TLS,
rate limiting and the audit trail — so none of them has to be adopted. Two changed defaults
can still stop a node that started before: 0.2.3 shipped `bind_address = "0.0.0.0"` and
`cors_allowed_origins = ["*"]` in its example config, and a non-loopback bind now requires
`[node] profile` while `"*"` is accepted only under `local`. Run `cameodb check-config -c
<your config>` before upgrading — it reports both. See Migration below, and to turn security
on start from `cameodb keygen`; [docs/CONFIGURATION.md](docs/CONFIGURATION.md) and
[docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) have the detail.

### Added
- **API key authentication.** Off by default. With `[security] enabled = true`, every route
  except `/_cluster/health` requires `Authorization: Bearer <key>`. Keys are `cameo_v1_`
  followed by 256 bits of OS entropy; the config stores only `sha256:<hex>`, compared in
  constant time, so a leaked config file holds nothing that can authenticate. The key-shape
  check runs before hashing, which is what makes an unsalted digest defensible: a passphrase
  or a UUID can never authenticate regardless of what digest is configured.
  - `cameodb keygen --role <admin|writer|reader>` mints one, printing the key to stdout and
    the config stanza to stderr. `--key-out` / `--hash-out` write the two files instead —
    `0600`, never overwriting an existing one.
  - Three roles bundle four capabilities: `admin` (all), `writer` (read + write), `reader`
    (read). `allowed_indexes` restricts a key to named indexes for any role.
  - Authorization is one route table and one middleware in front of the router, mounted
    inside CORS and outside the timeout, concurrency guard and body limits — a refused
    request takes no permit and buffers no body. Deny by default: an unclassified path needs
    a key like any other, so no handler can forget to check because no handler checks. Unit
    tests read the router's own source and fail the build if a route has no row.
  - Scoping holds through enumeration, not only when an index is named: `/_indexes`,
    `/_cluster/_indexes`, the MCP catalog and the MCP resource list return only what a key
    may see, counts included.
  - MCP is authorized per tool and per index, not just at the endpoint, with a capability
    table that denies by default. Sessions are bound to the key that opened them on all three
    verbs, and `tools/list` advertises only what the caller could call.
  - An anonymous caller gets `{"status": …}` from the health endpoint and nothing more. Node
    identity and cluster shape are free reconnaissance for anyone who can reach the port.
  - `--api-key`, `--api-key-file` and `CAMEODB_API_KEY` on the client, with the key in the
    HTTP client's default headers so no call site can omit it — and never on the client used
    for remote data sources. `--allow-plaintext-key` is deliberately separate from
    `--insecure`: one accepts a bad certificate on an encrypted connection, the other puts a
    bearer token on the wire in the clear. In the REPL a key is bound to its origin, and
    `key file <path>` / `key show` / `key clear` change it mid-session.
  - Rotation is add key → restart → migrate clients → remove key → restart. Keys are read at
    startup; there is no hot reload, and no lockout on failed authentication (against a
    256-bit key it buys nothing and is itself a denial-of-service lever).
- **Security posture profiles.** `[node] profile = "local" | "internal" | "external"`
  declares how far a node can be reached; the server enforces the matching rules and refuses
  to start if the config contradicts them. Profiles assert, they never rewrite values.
  Omitting it is valid only for a loopback bind, which infers `local`. The names describe
  reach rather than an environment (`dev`, `staging`) because every rule keys off the bind
  address — a lifecycle name invites picking by what the box is for and being rejected for it.
- `cameodb check-config [-c <path>]` prints the posture matrix and exits non-zero on
  failure — the manual equivalent of a CI gate.
- `--profile` / `CAMEODB_PROFILE` override.
- `[network.http] admin_enabled` (default `true`) removes the unauthenticated `/_admin/*`
  routes entirely when disabled; required off by the `external` profile.
- **TLS/HTTPS for the HTTP server**, via `axum-server` with rustls. `[network.http.tls]` takes
  `enabled`, `cert_file` and `key_file`; the config layer requires both paths when enabled and
  checks the files exist, and the material is loaded before storage init and before the startup
  banner, so a bad certificate fails early rather than mid-flight.
- `--insecure` on the client accepts an invalid certificate — per command for a single
  operation, and per session in the REPL, where it persists across `connect`.
- `--insecure-source`, separate from `--insecure`. Accepting an untrusted data source no
  longer disables verification on the connection to CameoDB itself.
- **Rate limiting for MCP tool calls** (`[security.limits]`, Phase 14 C1). Authentication
  answers *who* and `allowed_indexes` answers *what*; neither has anything to say about
  **how often**. The caller that matters here is not an attacker but a legitimate `reader`
  key held by an agent that loops on `search_indexes` — every call authorized, and each one
  fanning out across every shard.
  A token bucket per key, because agent traffic is bursty and a fixed window either refuses
  the flurry or never bites. Charged before the tool runs *and* before the per-tool
  capability check, so a rate-limited caller cannot infer which tools it would otherwise be
  allowed to call. The budget is shared across tools: it bounds what a key costs the node,
  not how often it may call one thing. Refusals name a retry delay and come back as an MCP
  tool error rather than a transport failure, since the request was well-formed and the tool
  simply did not run.
  Off by default. The policy lives in the server crate behind a `McpBackend` hook, so the
  `mcp` crate stays free of deployment opinions exactly as it does for authorization. Nine
  tests: six over the bucket arithmetic, three driving a real node over HTTP to prove the
  config actually reaches the dispatcher.
- **An audit trail** (`[security.audit]`, Phase 14 C2). Refusals have always been logged, so
  a node could say who it turned away — but successful access was a `debug!` line, which
  meant it could not say who legitimately read which index. That is the question an incident
  asks, and it now has an answer.
  The design's one non-obvious decision is **detail for reads, totals for writes**. A
  knowledge base ingests far more than it retrieves, so at the measured ~6 900 writes/s a
  record per write would bury the handful of reads worth looking at; writes fold into a
  per-key, per-index count flushed on an interval, while reads, MCP tool calls and admin
  actions keep a line each. The same rule keeps the trail from becoming a denial-of-service
  lever: a refusal of a *valid* key is listed, since its volume is bounded by the credentials
  in circulation, but a refusal of an unidentified caller is counted, since its volume is
  chosen by whoever can reach the port.
  Nothing touches the request path — emitting is a timestamp and a non-blocking hand-off to a
  dedicated OS thread rather than a tokio task, so the trail keeps draining while the runtime
  is saturated. A full queue drops the record, counts it, and writes a `gap` record naming
  the loss, because a trail that quietly skips entries lies about what it contains.
  Two sinks: a bounded in-memory ring served by `GET /_admin/audit` (node-admin, and reading
  it is itself audited), and an optional rotating JSON Lines file. Every record is also a
  `tracing` event on the target `cameodb::audit`, so an existing log collector gets it
  without a second path being configured. No key ever appears in a record — the `key_id` is
  the digest prefix minted for exactly this — and a test asserts it for accepted and rejected
  tokens alike. `record_query_text` is off by default and documented as keeping *data*: a
  search for a person's name records that name.
  MCP needed its own hook (`McpBackend::record_tool_call`), because from the HTTP layer every
  agent call is `POST /mcp` and which tool and index are in play exist only inside the
  dispatcher — the same host-owns-the-policy split used for authorization and rate limiting.
  Off by default, with a posture row that says so. 14 unit tests and 9 integration tests
  against a real node with three keys.
- **`cameodb-bench`, a latency harness** (`crates/bench`, not shipped). Reports
  p50/p90/p95/p99/p99.9 for writes and searches, the node's own `took_ms` beside the
  client-observed figure so the gap shows queueing rather than query cost, and per-worker job
  counts, core placement and dispatch counters over the measured window. Closed-loop, and it
  says so: compare runs at equal concurrency rather than reading the percentiles as an SLA.
  `scripts/testing/load-test.sh` remains as a smoke test but is now marked not to be quoted —
  it forks `curl` per request and times it in bash, so its latencies are process spawn.
  The harness doubles as a worked example of the client SDK: it depends on `client` and never
  on the server crate, and issues no request the SDK cannot express. `--mode bulk` measures
  batched ingest per request and reports docs/s, which on a 4-shard node showed batching
  worth ~9× at 50 documents per request and ~24× at 500 against one-per-request writes.
- **`CameoClient::write_document`.** Writing one document was reachable over HTTP but absent
  from the SDK, so any consumer needing it had to hand-roll the request.
- **Every shipped config is now parsed by a test**, and asserted to name no setting no field
  claims and to state all three affinity flags. Nothing checked the files this repository
  ships, which is how two flags stayed missing from all of them for a whole phase.
- **Integration tests for the server, which had none.** `crates/server` carried 160 unit
  tests and zero end-to-end coverage: it is a binary-only crate, so `tests/` has no library
  to link against, and a `NodeOrchestrator` needs a data directory, threads and a socket
  before it does anything — which is why every existing test covers a pure helper.
  `crates/server/tests/node_http_api.rs` starts the built binary as a subprocess on a free
  port with a temporary data directory and drives it through the shipped SDK, so the config
  loader, the routes and the client are the ones that actually ship. Six tests covering
  startup from a config file, write-then-read by id, commit-then-search by content, index
  creation via the listing, and the two contracts below.
  Writing them immediately turned up an API sharp edge worth pinning: `write_document` takes
  an `id` parameter but the document body must *also* contain `id`, and omitting it answers
  **500 Internal Server Error** for what is plainly a client error. Both that and the fact
  that searching an unknown index returns an empty result rather than failing are now tests,
  so neither can change silently.
- **`scripts/validate/artifact.sh`**, checking what a Linux release binary actually links
  against: no interpreter, no `NEEDED` entries, and the hardening that is supposed to be
  there. Every one of those properties is silently droppable — rustc falls back from
  `-static-pie` to `-static` with a warning when the linker refuses it — so "we passed the
  flag" and "the binary has the property" needed to become different claims. Runs in a
  container when the host has no `readelf`, and starts the binary when the host can execute
  it. Wired into `all.sh`.
- [`scripts/validate/`](scripts/validate/README.md): manual validation suite (deps, unit,
  posture, auth, tls, remote-sources, artifact) with a single `all.sh` entry point, plus
  [RELEASE-CHECKLIST.md](RELEASE-CHECKLIST.md). There is no CI by design; this is the gate.
- **`scripts/release/`, a four-stage release pipeline** (`build`, `sbom`, `sign`, `publish`)
  driven by `release.sh --stage`, staging into `dist/<version>/`. The version is read from
  `crates/*/Cargo.toml` and never passed in, so a filename cannot disagree with what the binary
  reports — the previous script carried its own hardcoded `0.2.3` and labelled 0.3.0 packages
  with it. The DEB and RPM now wrap the same binary that ships standalone instead of a
  separately rebuilt one, each stage refuses to run on a tree the previous stage did not
  complete, every signature is verified against the public key downloaders actually fetch, and
  `publish.sh` is a dry run until `--commit`.

- **Docker builds take a corporate CA** through a vendor-neutral `corporate-ca` build secret
  (`--mount=type=secret,id=corporate-ca`, plus `update-ca-certificates` in the first apt-get
  chain), renamed from the previous `zscaler`.
### Changed
- **An orchestrator worker carries eight operations at once instead of one.** The worker loop
  awaited `execute` inline, which made `worker_count` the node's entire operation concurrency
  — and an operation is mostly spent *awaiting* a shard writer rather than burning CPU, so
  the pool sat idle while requests queued. Worth **+65-70% write throughput (4 178 → 6 901-
  7 118 ok/s across two measurement sets) and −64% on p90 (29.30ms → 10.45ms)** on an 8-core
  node at concurrency 64. The width is a constant, not a setting: swept 1/2/4/8/16,
  throughput peaks at 8 and *falls* at 16, with every width-8 repeat beating every width-16
  repeat.
  Read it as a saturation fix. At concurrency 16 against the default 16-worker pool the same
  sweep is flat, because even one operation per worker already covers what the client has
  outstanding; the win starts where demand exceeds `worker_count`.
  The permit is taken *before* the receive, so a saturated worker stops draining its channel
  and the existing backpressure — queue fills, dispatch falls through to a neighbour — works
  unchanged. Shutdown now waits for accepted operations to answer: on the pinned path the
  loop is the argument to `block_on`, so returning would drop the worker's runtime and cancel
  in-flight work, handing those callers a dropped channel instead of a reply.
- **The CPU affinity flags are documented and measured, and the recommendation is to leave
  them off.** `shard_affine_dispatch` and `worker_core_affinity` were absent from every shipped
  config and from `docs/CONFIGURATION.md`; both are now stated explicitly, with what they cost.
  Measured with `cameodb-bench` on an 8-core Linux node, three repeats per arm:
  shard-affine dispatch costs 13-20% of write throughput at concurrency 8, 16 and 32 and
  roughly doubles write p90, and after workers went eight operations wide it still costs 24% at
  concurrency 64 — default's worst repeat beat every affinity repeat. Pinning the workers on
  top adds nothing to writes and takes a further 11-15% off search throughput with p99 roughly
  doubled. `writer_core_affinity` measured neutral and stays on.
  The cause is the constraint itself rather than the pinning, which the width change above was
  expected to absolve and did not: a shard's jobs may only run on `S % worker_count`, so skew
  idles workers while their neighbours queue, and round-robin cannot be unlucky that way.
  Searches confirm it from the other side — affine dispatch is neutral for them, since they
  dispatch round-robin regardless. No default changed; what changed is that the choice is now
  visible, evidenced, and explained by a reason that survived a test meant to overturn it.
- **`docker/cameodb-docker.toml` shipped `search_threads = 16`** — double the code default,
  on containers typically given 4-8 cores. It now ships the default 8, with the sizing rule
  written next to it. The read pool shares cores with the pinned shard writers, so allowing
  more concurrent searches than there are cores moves queueing from the pool into the kernel
  and charges the write path for it. Measured on an 8-core node under simultaneous read and
  write load, 16 was worse than 8 on every axis — search p99 15.44ms vs 13.46ms, write
  throughput 1 776 vs 1 895 ok/s — and several times less predictable run to run.
- **Mixed read/write load is now measured, and documented.** Every performance figure this
  project had published was taken with writes alone or searches alone. Run together on an
  8-core node, each drops by roughly half (writes 4 074 -> 1 776 ok/s, searches 5 880 ->
  3 284) **while one and a half cores sit idle** — so the loss is not capacity.
  It is also not core placement, which is worth stating because the obvious fix is to
  isolate readers from writers: unpinning the shard writers changes nothing (1 758 vs
  1 776 ok/s), and partitioning cores would take them from searches, the only CPU-bound
  party here, to give them to writers that spend their time blocked in `fsync`. What the
  measurement does show is the cost of a durable commit tripling under read load —
  ~4.6ms to ~12.5ms — because segment reads contend with WAL fsync for IO and page cache.
  `wal_sync = false` recovers +86% of write throughput under the same load.
  Recorded in ROADMAP "Mixed read/write load, measured".
  A **bounded linger before commit was built to exploit this and then removed**: the writer
  already merges every queued write into one transaction, but commits whatever is queued at
  that instant — about 2.5 writes — so waiting briefly for more looked like free
  amortisation. Measured at 200/500/1000µs against a no-linger control, it produced nothing
  distinguishable from noise at c16 or c64. The arithmetic explains it: only ~0.05 writes
  arrive at a given shard during a 200µs window (~0.18 at c64), and a closed-loop client
  cannot issue the next write until this one is answered — so the writer would wait for
  writes that cannot arrive until it stops waiting. The negative result and its precondition
  (an open-loop load generator) are recorded rather than the code.
- **`build-musl.sh` builds in a container by default, and both architectures.** It was
  x86_64-only and always used `cargo zigbuild`, which produces a *less* hardened binary than
  the published image: zig's linker does not advertise `-static-pie`, so rustc silently falls
  back to `-static` and the result is fully static but loads at a fixed address. The script
  now prefers a Linux container matching the target architecture — the same toolchain the
  Dockerfile uses — takes an arch argument (`x86_64` | `aarch64` | `both`), keeps zigbuild as
  the no-Docker fallback, and checks what it produced instead of assuming. Documented, with
  the aarch64 caveats, in `docs/BUILDING.md`.
- **The Dockerfile's manual `rust-std` fallback works.** It runs when `rustup component add`
  cannot reach the target, and both halves were wrong: the download URL carried a date prefix
  the canonical path does not use, and the tarball was extracted without `-J` or
  `--strip-components=1`, so `install.sh` was never where the next line looked for it.
- **`.cargo/config.toml` is tracked.** `.gitignore` excluded the whole `.cargo/` directory, so
  the file carrying the musl link flags, the hardening flags and jemalloc's page size existed
  only on machines that happened to have it: a fresh clone built release binaries with none
  of them, silently, and nothing in the tree said so. Now only credentials are ignored.
- **`.cargo/config.toml` covers `aarch64-unknown-linux-musl`**, which had no section at all
  and so got none of the hardening x86_64 gets. It deliberately does *not* set
  `relocation-model=pie`: static-pie is broken on that target — forcing it links and then
  segfaults before `main`, on a hello-world crate with no dependencies — which is why rustc
  defaults it to `-no-pie`. aarch64 binaries load at a fixed address as a result; the reason
  is recorded next to the flags and reported as a SKIP by `artifact.sh` so it resurfaces if
  the toolchain is ever fixed. Also documents that `JEMALLOC_SYS_WITH_LG_PAGE = "12"` fixes
  jemalloc to 4 KiB pages, which aborts at startup on aarch64 hosts configured with 16 KiB or
  64 KiB pages — fine for the platforms currently targeted, a decision to revisit before
  shipping aarch64 packages to distros outside them.
- **Shard-affine dispatch no longer collides.** Worker selection and writer-thread pinning
  both hashed the shard id, and the hash domain (the shard set) is smaller than the core
  count: with the shipped defaults — 4 shards, 8 cores — 40 affine writes reached 3 of 8
  workers and two shards' writer threads shared a core. Both now derive from a dense
  per-shard ordinal, so the same run reaches one worker per shard with one writer per core.
  Searches are unaffected; they round-robin the whole pool as before.
- **Worker sizing and thread pinning count the same cores.** Sizing read
  `available_parallelism()` while pinning indexed `core_affinity::get_core_ids()`. Under a
  cgroup CPU quota those disagree — `docker --cpus=4` on a 32-core host reports 4 and 32 —
  so the co-location the design exists for silently stopped holding. A single `CoreLayout`
  now reconciles them.
- **Keyed operations skip the coordinator.** Every write and every search took a mailbox
  round trip to a single actor to ask where to route, in front of a worker pool built to
  avoid exactly that. A keyed operation whose shard is local is now decided from the
  published routing ring and shard placement, both already in hand. Unkeyed operations
  (searches) still ask — that decision depends on cluster size.
- **`GET /_admin/workers` reports pinning outcomes, not requests.** It previously showed
  `pinned: true` and a `core_id` per worker on hosts where every `set_for_current` call had
  failed, which is every call on macOS. `pinned` is replaced by `pinning_requested` plus
  `pinned_workers`; `hash_aligned` is now `core_aligned` (there is no hash any more); each
  worker carries both `target_core_id` and the `core_id` it actually took; and a new `shards`
  section reports per-shard ordinal, requested core, taken core, and whether the shard is
  serving.
- **`[search] supervisor_timeout_secs` was silently ignored.** The idle-commit supervisor read
  `CAMEODB_SUPERVISOR_TIMEOUT_SECS` from the environment directly rather than from the config,
  so the setting in a config file and the `--supervisor-timeout-secs` flag both did nothing —
  the environment variable appeared to work only because it bypassed the config system
  entirely. It now comes from the config, which still maps that variable onto the field, so
  the env var keeps working and the file and flag start working. Its doc comment also claimed
  a default of 10 while the code used 5; the code was right.
- **The client SDK's worker-report type went stale when the node's field names changed.**
  `AdminWorkersResponse` still required `pinned` and `hash_aligned`, so `cameodb client admin
  workers` would have failed to parse a report from the node it shipped with. Every field is
  now `#[serde(default)]`: a client and a node version independently, and a renamed or added
  counter should degrade to a zero rather than take down the whole report. Covered by tests
  that parse both the current payload and a sparse one.
- **`scripts/validate/auth.sh` file-mode checks were broken on Linux.** They tried BSD `stat -f`
  first and fell back to GNU `stat -c`, but GNU `stat` reads `-f` as "filesystem", takes the
  format string as a filename, still exits 0 for the operand that existed, and returns a
  paragraph of filesystem info — so the fallback never fired and the comparison failed against
  files that were correctly 0600. Order reversed; macOS rejects `-c` cleanly, which is what
  makes GNU-first safe on both.
- **Hot-path logging moved to `debug`.** One search at `RUST_LOG=info` emitted seven lines —
  two per-request routing lines from the coordinator, a handler line carrying the caller's
  query text, and one `No tantivy reader found` warning *per shard* for the normal case of an
  index with no commits. A write and a search now emit none.
- **Single TLS stack.** The client moved from native-tls/vendored OpenSSL to
  `reqwest/rustls-no-provider`. `rustls-platform-verifier` uses the OS trust store, which
  is what native-tls provided and what a corporate CA requires — verified against
  `dl.cameodb.com` and other real sources. Vendored OpenSSL and `aws-lc-rs` are gone from
  every build path, so musl and Windows cross-builds no longer need a C toolchain for TLS.
  The `client/native-tls*` features were removed; build invocations no longer pass them.
- **Default bind is now `127.0.0.1`** (was `0.0.0.0`). A reachable bind additionally
  requires a declared profile.
- **Default `cors_allowed_origins` is now `[]`** (was `["*"]`). CORS governs browsers only,
  so this costs API and MCP clients nothing; `"*"` is accepted only under `local`. An empty
  list is no longer a config error.
- Cluster PSK is held in a `ClusterPsk` newtype that redacts its `Debug`, is never
  serialized, and zeroizes on drop. Format validation lives in `load_psk()` alone, which
  `validate()` now calls, so a config that validates is one the swarm can start with.
  `psk_file` permissions are checked, and a PSK combined with a `/quic-v1` address is
  rejected at config time (`pnet` wraps TCP only).
- Every index path is built through `HybridStore::index_dir`, including internally sourced
  names, so the traversal guarantee has one construction site.
- Renamed the cluster messaging `default_max_concurrent_requests` to
  `default_messaging_max_concurrent_requests` to distinguish it from the HTTP knob.
- `deny.toml`: advisory exceptions carry `review-by` dates that `deps.sh` enforces; added
  `CDLA-Permissive-2.0` for the Mozilla CA bundle shipped via `rustls-platform-verifier`.

### Fixed
- **Writes to an index with no schema returned 500.** The worker pool's engine holds `ArcSwap`
  snapshots, so it can read a schema but not evolve one — that needs the actor. It signalled
  this by returning a sentinel error, and the caller, which had already moved the op into the
  worker job, had nothing left to retry with and surfaced the sentinel to the client instead.
  Since a new index has an empty schema, *every* first write to an index failed. The engine
  now hands the op back (`WorkerOutcome::UseActor`) and the caller retries it on the actor,
  with no clone on the fast path. Creating an index by writing to it works again.
- **Auto-created indexes were write-only.** Fields inferred when an index is first written
  were marked non-indexed, so documents went in and nothing but `id` could find them again —
  permanently, because a tantivy schema is fixed at index creation and nothing promotes a
  field afterwards. Fields discovered at creation are now indexed (and, as before, not
  stored: hits are rebuilt from redb). Fields that arrive later stay unindexed, which is the
  only thing tantivy allows; they exist for redb/tantivy schema parity. The bundled client
  already applied this rule before PUTting a detected schema, so `cameodb data load` was
  unaffected — the two now agree instead of one compensating for the other.
- **The initial schema was never persisted, and fields past the sampling limit were dropped.**
  Sampling filled the in-memory schema, which made the evolution stage — the only thing that
  wrote to storage — decide there was nothing new, so the storage layer re-derived its own
  schema from the document. Two more places recomputed "is this initial creation?" from
  `fields.is_empty()` *after* sampling had filled it, reading false on exactly the call where
  it was true; one of them selected a validator that does not report new fields, so a field
  first appearing past document 200 of a bulk load never reached the schema at all.

### Security

Two of these affected a published version: the unbounded streaming ingest and the missing HTTP
request timeout, both present in 0.2.3. The rest were introduced and fixed inside this release
cycle — API keys, TLS, the posture gate and restricted CORS are all new in 0.3.0 — so no 0.2.x
node was ever exposed to them, and nothing here calls for a credential rotation.

- **The client's remote-source fetches used the credential-carrying HTTP client.** `CameoClient`
  builds two: one with the API key in its default headers for CameoDB, and one with no
  credential for the schema and data URLs a caller supplies, because those name somebody else's
  host. Four of the five source fetches used the first — `fetch_source_prefix_bytes`,
  `open_csv_source`, `for_each_json_document_in_http_source` and
  `load_data_from_http_json_source_single_pass` — so `schema detect` and `data load` against an
  `http(s)://` source presented the caller's bearer token to that host, in the clear over
  plaintext. The same mix-up left `--insecure-source` with no effect on those paths while
  `--insecure` wrongly governed them, since source trust was being read from the server's
  setting. All five now use the credential-free client. Introduced and fixed inside this
  release cycle — API keys did not exist in 0.2.3, so no published version ever sent one to a
  source host — and recorded here because the guarantee is stated as a feature above.
  `scripts/validate/remote-sources.sh` was the check that caught it.
- **Corporate CA certificates were silently dropped by both compose files.** The `zscaler` →
  `corporate-ca` rename reached the Dockerfiles but not `docker-compose.yml`,
  `docker-compose-cluster.yml` or `docs/BUILDING.md`, and a secret id that does not match the
  one the Dockerfile mounts fails without an error — the build reports "No corporate CA
  certificate provided" and produces an image that cannot reach a TLS-intercepting proxy. All
  of them now use `corporate-ca`, sourced from `CAMEODB_CA_CERT` (default `/dev/null`, so a
  build with no corporate CA needs nothing set). `scripts/build/docker-push.sh` reads the same
  variable; its hardcoded path had also disagreed with the one the docs told you to use.
- **The shipped Docker config could not start.** It declared no `[node] profile` while binding
  `0.0.0.0`, which the posture check refuses rather than guessing at — so the example config
  failed the gate the same release added. Now `profile = "internal"`, which is what a published
  container port actually is, with `cors_allowed_origins = []` to match.
- **`port` under `[network.cluster]` was silently ignored.** The field is `cluster_port`, and
  unrecognised keys were not reported, so every shipped config and the configuration guide set
  a cluster port that had no effect.
- **Unrecognised-key detection reported every `Option` field as a typo.** The schema it
  compared against was built by serializing to TOML, which drops `None`, so `node.profile`,
  `tls.cert_file`, `tls.key_file` and `cluster.psk_file` were all flagged as unknown settings.
- **TLS never worked.** The server panicked on every HTTPS startup — `axum-server/tls-rustls`
  force-enables `rustls/aws-lc-rs` while libp2p-quic enables `rustls/ring`, and rustls 0.23
  refuses to choose between two providers. The panic landed after the startup banner, so a
  failed boot looked like a successful one. Now uses `tls-rustls-no-provider` with `ring`
  installed explicitly at the top of `main`.
- **Streaming ingest ignored every body limit.** `DefaultBodyLimit` only constrains
  extractors, so `POST /api/{index}/document/stream`, which takes a raw `Body`, was
  unbounded: a 150 MB single-line request under a 1 MB configured limit was accepted and
  drove RSS from 44 MB to 889 MB. Added `RequestBodyLimitLayer` (wire bytes) and a
  per-record cap inside the handler.
- **`request_timeout_secs` was never applied to HTTP.** With no timeout, the new
  concurrency guard made denial of service *cheaper*: four uploads at 300 B/s held every
  permit indefinitely and took the node offline, health check included. Added
  `TimeoutLayer`, exempted `/_cluster/health` from the guard, and added `Retry-After` to
  the 503.
- **TLS lost graceful shutdown.** The drain signal only reached the plaintext listener, so
  every TLS shutdown burned the full 10 s timeout and then cut in-flight requests. Now
  driven by `axum_server::Handle`.
- **Restricting CORS broke browser MCP clients.** `mcp-session-id` was neither an allowed
  request header nor exposed on responses, so the Streamable HTTP transport could not work
  from a browser once origins were restricted.

### Removed
- `CAMEODB_ACCEPT_INVALID_CERTS`, replaced by `--insecure` and `--insecure-source`.

### Migration
- Configs binding a non-loopback address must add `[node] profile = "..."`.
- Configs with `cors_allowed_origins = ["*"]` must list explicit origins or use `[]`, unless
  the profile is `local`.
- `CAMEODB_ACCEPT_INVALID_CERTS` no longer does anything. It governed both connections, so
  replace it with `--insecure` for the connection to CameoDB, `--insecure-source` for a remote
  schema or data URL, or both.
- Build scripts passing `--features client/native-tls-vendored` must drop the flag.
- Docker builds passing the `zscaler` build secret must pass `corporate-ca`.

---

## [0.2.3] - 2026-06-30

### Added
- MCP Streamable HTTP transport mode alongside existing SSE transport
- Worker pool statistics endpoint (`GET /_admin/workers`) with per-worker and dispatch metrics
- Admin CLI commands for worker stats (`cameodb admin workers`)
- Shard-affine worker dispatch: route operations targeting the same shard to the same worker
- Hash-space alignment between worker pool and writer thread pinning
- Pinned worker runtimes: dedicated `current_thread` tokio runtimes per core when all affinity flags enabled
- Configurable Tantivy merge thread count via `StorageConfig.merge_num_threads` (default: 1)
  - Implemented via `IndexWriterOptions::builder()` with explicit `num_merge_threads()`
- Per-index memory stats (`memory_mb` field) in `/_indexes` response
- Admin memory module extracted to `crates/server/src/admin/memory.rs`
- `--force` flag for aggressive jemalloc purge bypassing decay timers
- Cross-node field-sort merge with date normalization and i64 key support
- `limit 0` as count-only query mode
- Inline query sort modifiers in search syntax

### Changed
- Upgraded kameo to 0.22, yamux to 0.14, tikv-jemalloc to 0.7
- Removed `axum-extra` dependency (inlined functionality)
- Upgraded Rust toolchain to 1.95
- Migrated cluster state serialization from bincode to JSON
- Use lenient query parsing in storage layer
- Restrict default search fields to text types only
- Upgraded core dependencies to latest versions

### Fixed
- Corrected WAL checkpoint semantics and index initialization races
- Fixed two-phase index warmup with persisted recovery metadata
- Stopped reloading readers twice per commit and warming discarded segments
- Fixed read/write pool sizing and serialized index deletion
- Corrected sequence counter initialization (Tantivy descending sort key inversion)
- Fixed federated search document sort and projection for MCP agents
- Preserved projection field order and expanded sort capabilities

---

## [0.2.2] - 2026-03-20

### Added
- MCP server integration for AI agent search capabilities
  - 6 MCP tools: `search_index`, `search_indexes`, `get_index`, `validate_query`, `get_index_stats`, `list_indexes`
  - MCP prompts capability with `cameodb-orchestrator` skill for agent context injection
  - 4 resource URIs for index exploration (indexes, metadata, schema, stats)
  - Spec-compliant SSE transport with session lifecycle management
  - Direct HTTP JSON-RPC transport mode
  - Field-type-aware query validation with syntax reference and "did you mean" suggestions
  - Compact field list and deduplicated query hints in index metadata
- Transparent gzip and zip compression support for all data sources
- Sort support for search queries with inline syntax and JSON payload options
- Inline query modifiers applied to MCP search tools
- Graceful MCP server shutdown with session cleanup
- Release build profile with thin LTO and reduced codegen units
- Comprehensive query syntax reference in MCP README

### Changed
- Upgraded workspace dependencies to latest stable versions
- Restructured MCP index metadata responses (removed schema, added compact field display)
- Streamlined Docker build configuration with unified `release-docker` profile
- Upgraded Rust toolchain to 1.94
- Improved Docker CA certificate handling with conditional corporate proxy support

### Fixed
- Corrected sequence counter initialization by inverting Tantivy descending sort keys
- Ensured all CSV fields are marked as indexed during single-pass data loading

---

## [0.2.1] - 2026-01-17

### Added
- Interactive CLI shell with rustyline-based completion, history persistence, and field-aware query suggestions
- CSV/TSV schema detection and bulk data ingestion with delimiter auto-detection
- Tab completion for schema, data, delete, and connect commands
- File path completion for data loading commands
- Interactive delete command with confirmation prompt and `--delete-schema` flag
- Connection management with `connect` command in interactive REPL
- Comprehensive JSON/JSONL/NDJSON support with automatic format detection and schema inference
- True end-to-end search streaming with incremental NDJSON response delivery
- Incremental NDJSON write-stream ingestion with bounded micro-batching
- Bounded top-K merge with score-aware pruning for distributed search results
- Bounded concurrency for scatter-gather search operations
- Field projection for search responses with inline query syntax (`return field1,field2`)
- Query modifier completion and hints for CLI search
- 4-phase graceful shutdown with tiered redb cache sizing and per-shard memory budgeting
- `u64::MAX` guard to sequence counter initialization with corruption detection
- `stream_batch_size` config and CPU-scaled shard hydration concurrency
- Background index warmup with bounded concurrency
- RPM packaging support with systemd integration and cross-compilation
- Cosign signing and verification for release artifacts
- Fingerprint-based schema versioning with pre-computed shadow field cache and routing field auto-detection
- Deterministic shard placement across multiple storage paths with balanced UUID mining
- Index-only Tantivy storage strategy to eliminate redundant field storage (50-80% index size reduction)
- Per-index idle-timeout commit supervision with async RwLock migration
- Parallel schema validation and document routing using rayon for bulk write performance
- Parallel local shard processing in bulk write operations
- Configurable WAL durability with environment variable override
- NDJSON streaming support for search results with per-hit chunked responses
- Brotli compression/decompression support
- CLI client mode with clap-based command interface
- `exact` field type with untokenized string indexing for efficient exact match queries

### Changed
- Migrated from `serde_yaml` to `serde_yml`
- Migrated route path syntax from `:param` to `{param}` for axum 0.8 compatibility
- Disabled default reqwest features and removed OpenSSL dependency from Docker build
- Replaced SHA256 with XXH3 for consistent hashing (improved performance)
- Implemented DHT-based shard discovery and ring reconstruction from persisted state
- Topology subscription pattern for real-time ring updates to orchestrator
- Event-driven cluster metadata persistence and state reconciliation
- Schema-driven field indexing with per-field Tantivy mapping and `fields_cache`
- Early-exit search optimization with local-first result merging and global limit enforcement
- Remote bulk write forwarding with shard-aware routing and XXH3-based routing hints
- Upsert semantics for Tantivy indexing (delete existing documents before add operations)
- Bidirectional shard metadata push to fix race condition between early and late joining peers
- Traffic light health model (green/yellow/red) based on missing node count
- Deterministic node identity from libp2p PeerId with push-based shard synchronization
- Standardized terminology: "bootstrap" → "seed" nodes, "writer" → "indexer" memory
- Reorganized config schema with node identity, network sections, and search defaults
- Parallel reader warmup during node startup using DashMap for concurrent caches
- Smart reader cache refresh strategy after commits
- Index statistics caching with separate fast/full modes and hybrid redb size estimation

### Fixed
- Schema cache staleness by always loading from Tantivy source of truth
- Prevented `id` field evolution during schema updates
- Date parsing robustness with Tantivy DateTime range clamping for out-of-bounds dates
- Self-dial prevention with DNS-to-IP conversion and IPv4 preference for seed node resolution
- Cluster state management with accurate node tracking and health transitions

---

## [0.2.0] - 2025-12-28

### Added
- Distributed actor system with Kameo remote actors and Docker cluster deployment
- Multi-tenant hybrid storage with production-grade optimizations
- Bulk write API with optimized batch processing and shard-aware distribution
- Automatic shard initialization and routing-key based write distribution
- Comprehensive configuration system with TOML/YAML/ENV support
- HTTP API with streaming search and channel-based result aggregation
- MicroshardActor and RouterActor with distributed search
- Index listing API with comprehensive statistics and multi-dataset ingestion support
- Dynamic memory budgets and smart commit strategy for multi-tenant performance
- Solr-style query timing metrics with shard aggregation
- Schema-driven selective indexing with PATCH API and `default_indexed=false` for new fields
- Optimized write path with routing key defaults, zero-copy serialization, and budget caching
- Schema caching in NodeOrchestrator with ordered field serialization
- Deterministic routing key derivation with document ID fallback
- Remote actor wiring and cross-node scatter-gather for distributed operations
- Routing key-based shard lookup and ring distribution tests
- Swarm event wiring to coordinator with shard registration and tracking
- ClusterCoordinator actor with message-based swarm lifecycle management
- Dedicated swarm runtime task with graceful shutdown controls
- Kademlia DHT swarm adoption with config documentation
- NodeOrchestrator `ClientOp` message handler with routing and schema validation
- Cluster-wide coordinated index deletion with single-node routing optimization
- Intelligent shard exchange with generation-based deduplication
- Lightweight index listing endpoint with schema preloading
- Cached directory size calculation with deterministic keys
- Streaming write endpoint and renamed search stream route for API consistency
- DELETE `/api/{index}` endpoint for permanent index deletion
- Environment variable support for node configuration
- Node name in cluster peer information and API responses

### Changed
- Upgraded axum to 0.8 and tower-http to 0.6
- Renamed binary from `server` to `cameodb`
- Standardized data directory path from `cameodb-data/` to `data/cameodb/`
- Replaced string-based field types with `TantivyFieldType` enum
- Enhanced schema evolution with type inference and compatibility rules
- Persisted schema updates to all local shards
- Added support for numeric, date, and boolean field types in schema evolution
- Marked new schema fields as indexed by default during evolution
- Included `id` field in schema evolution and explicit definition in index creation

### Fixed
- Applied clippy lint fixes for Rust 2024 compliance

---

## [0.1.0] - 2025-11-21

### Added
- Initial CameoDB implementation with hybrid storage and distributed topology
- Hybrid storage engine combining redb (KV store) and Tantivy (full-text search)
- Microshard architecture: each shard contains both a redb file and Tantivy directory
- Sequence ID tracking and WAL (Write-Ahead Log) for durability and recovery
- WAL replay with `get_last_indexed_seq`/`recover_index` and automatic recovery on index open
- Shadow field replacement with O(1) HashSet lookup
- Automatic index warmup on startup with recovery procedures
- Kameo-based actor system for shard management with `MicroshardActor`
- `StorageCommand` enum for thread-safe operations
- Writer thread pattern for async/sync isolation
- Consistent hashing ring for node distribution
- Basic HTTP API with search, write, and bulk operations
- Configuration system with TOML support
- Development scripts and tooling
- Project structure with workspace crates: `cluster`, `storage`, `server`, `client`
