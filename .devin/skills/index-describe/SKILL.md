---
name: index-describe
description: Write or improve the index and field descriptions of a CameoDB index from its live data — profile the index (by exact counts, or from whole records sampled by id when it holds tens of millions of rows), verify every claim, write affirmative descriptions of what each field holds and is for, and produce a PUT /_config body that changes descriptions only. Use when asked to describe, document or enhance the schema of an index, or after `schema detect` / `data load` created one with no descriptions.
---

# Describe a CameoDB index from its data

Nothing in CameoDB infers a description (`FieldDef::new` leaves it `None`), and `schema detect` /
`data load` write types, tokenizers and fast columns only. A description is what an agent reads to choose
an index and build a query, so it has to be true of the data, not of the plan the data was made from. This
skill produces descriptions from evidence and a schema body that applies them.

Scripts (stdlib Python, run with `python3 -I`):
- `profile_index.py` — exact per-field counts and a spread sample, as a readable report and JSON. For
  indexes up to a few million rows.
- `sample_index.py` — whole records sampled per row kind and year, read by id from the key-value store,
  with shares weighted by year and the real range of every fast field. For large indexes (§2b).
- `schema_body.py` — the live schema read (`GET /_config`, a complete definition the node takes back as
  it is) with new descriptions, as a `PUT` body; checks lengths, shows the diff, `--apply` sends it and
  confirms nothing but descriptions moved.

## 1. Find the node and the index

- Node: `http://127.0.0.1:9480` unless told otherwise; a node with security on needs `--key`.
- `GET /_indexes` (or MCP `list_indexes`) for the index, its count and current description;
  `GET /api/{index}/_config` (or MCP `describe_index`) for fields, types, tokenizers, `sortable`, `default_search`.
- **Save the complete schema first**: the whole `GET /api/{index}/_config` response, unedited, to
  `<index>.v<version>.<thumbprint>.json` in the project's schema history folder (create it beside the schema
  source when there is none). That file is the previous version once anything changes; nothing is applied
  without it. `schema_body.py` does this on every run and again after `--apply`. A saved read restores
  the index as it was (`PUT` it back); the restored index's `thumbprint` equal to the file's proves it.
- Look for the project that loads the index: a schema source file (e.g. `schema.py`), loader, extractor
  docs. They say what each field was *meant* to be; the data says what it *is*. Where they disagree the
  data wins and the disagreement is reported.
- Ask the data owner what the data cannot say: which fields current processing writes and which it no longer
  does (and what replaced them), what a default value stands for, what an opaque code means. Their answer is
  a source like any other; the data still has to agree with it.
- Pick the profiling path by size: up to a few million rows §2a, beyond that §2b. Time one presence count
  on the largest text field first (`limit 0`); if it takes more than a few seconds, use §2b whatever the size.

## 2a. Profile a small index

```bash
python3 -I profile_index.py --index <index> --stratify <row-kind field> --spread <year-like i64 field> \
    --json <scratch>/profile.json > <scratch>/profile.md
```

- `--stratify`: the field that says what kind of row this is (`unit_type`, `doc_type`, `kind`). Without one,
  "which rows carry this field" cannot be answered. Pass `--strata a,b,c` for rare kinds the sample misses.
- `--spread`: a fast integer field over which coverage changes (usually a year). Gives, per field, the first
  and last value and bands of how many rows per bucket carry it (`1990–2014 none, 2015–2026 all`).
- Reads per field: exact presence count, presence per row kind, coverage over the spread, min/max, exact
  counts of each value when a field shows few, and sample examples, list lengths, text lengths.

Know its limits:
- `field:*` is refused and `field:[* TO *]` matches nothing, so presence is a range from the type's lowest term
  (`string ["" TO *]`, `text [0 TO *]`). json, bytes, ip and facet fields have no range: sample only.
- The value list comes from the sample, so rare values are missing; `values_coverage` below 100% says so.
  Look up the rest in the project's vocabulary or extractor before listing values in a description.
- Coverage bands are a share of *all* rows in the bucket. For a field on one row kind only (an act-level
  field), count it within that kind: `field:["" TO *] AND kind:act AND year:[a TO b]`.

## 2b. Profile a large index

A presence range on a text field walks every term the field holds. On hundreds of millions of rows one such
count can run for minutes on a high-cardinality field (hashes, URLs), and §2a runs one per field: hours of
load on a node others are using. Measure from whole records instead, and count only what stays cheap: term
queries, ranges on fast fields, and sorted `limit 1` searches.

```bash
python3 -I sample_index.py --index <index> --spread <fast date/int field> \
    --stratum <name>=<term query> [--stratum …] --json <scratch>/sample.json > <scratch>/sample.md
```

- **Whole records come from the store.** `id:<key>` alone (or `<shadow>:<key>`) skips the search index and
  returns every stored value, not only stored/fast columns. A search with a `fields` list returns only
  those; on an index where most fields are unstored, that list cannot show their values.
- **Strata are term queries.** Find the row-kind field (record type, status, category) and its values
  first, then pass one `--stratum` per kind, rare ones included. On a tokenized field a stored value
  `Model64 Variant/Sub` is matched by `model64`, not `model`: read a few records before trusting a zero.
- **Spread is a fast date or integer field**, one bucket per year (or value). The script counts each bucket,
  so a load concentrated in a few years shows up in the report.
- **Shares are weighted by the bucket counts.** The sample takes the same few rows from every year, so its
  raw share over-represents sparse years. The weighted share is what to write ("about half the rows").
- **Fast fields get exact ranges.** For each fast date or number: real min and max with the default value
  left out (`[1970-01-02 TO *]`, `[1 TO *]`), and how many rows hold the default (`1970-01-01`, `0`).
- **Settle each claim with a term count**, not a text range: `status:archived`,
  `updated_at:[* TO 1970-01-02}`, `kind:"x" AND year:2020`. A count that times out is not zero: say so
  and drop the claim.
- Keep `--per-year` and `--per-cell` small (a few hundred records in all); one record read is cheap, a
  thousand searches against a busy node are not.

## 3. Look at rows

The report shows shapes; rows show meaning. With the MCP `search_index` tool (or `POST /search`):
- a few whole rows per row kind, early and late in the spread;
- one complete record read in order (e.g. `act_key:<key>` sorted by the order field) to see how rows relate;
- for each claim in an existing description that the profile puts in doubt, a count that settles it
  (`klasa:["" TO *] AND unit_type:act AND year:[1990 TO 2012]` against `unit_type:act AND year:[1990 TO 2012]`).

Vocabulary-coded fields (ids, IRIs): map the most frequent ids to labels from the project's vocabulary files
so the description can name them.

## 4. Write the descriptions

Write `<scratch>/desc.json`: `{"description": "...", "fields": {"<name>": "...", ...}}`.

**Index (≤ 512 characters).** What the dataset is, its span, what one row is, how to get one hit per record,
how to read a record in order, which field searches which text, and what holds only for part of the data.

**Field (≤ 200 characters).** One line, in this order, as far as it fits:
1. What the value is, in the reader's terms (not the code's).
2. Where it is: every row, or which row kinds; which years or parts. "Act rows only" and "from 2015" are the
   facts that most often make a query wrong when left out.
3. Format and real examples, or the common values with labels for coded ones.
4. How to query it when not obvious: exact quoted match on raw strings, list values, `sort` field pairs,
   the field to use instead.

**Write affirmatively.** A description says what the field *is* and what it is *for*, as a statement the
reader can act on. It is not a list of what is wrong with it.
- Lead with meaning and purpose: "When the record was first received from any source", not "Created timestamp".
- State the real range, without defaults and marginal values: "from 2010-12", "6–10", taken from the
  real min/max (§2b), not from a 1970 placeholder or a single outlier.
- Name a default value as the convention it is, once: "0 (1970-01-01) when not set". Not "warning", "bogus",
  "placeholder", "noise".
- A field that current processing no longer writes is described by what it holds and where the current
  value lives: "Last update as recorded by earlier processing; the current value is in updated_at."
- A field that is never queried directly says where to search instead: "Nested event objects; search
  event_type for event kinds." An unindexed field says "Returned with the record;
  search <x> for …", or simply what it holds when nothing searches it.
- A field the sample never showed is described by what it holds and which rows carry it: "Line items, on
  invoice records". No "none in the sample".
- Mixed contents are described as contents: "Category names and their codes (Retail, C02)".
- What only holds for part of the data is scope, stated plainly: "invoice records", "records with
  attachments", "records processed before 2025".

Rules:
- Every statement is checked against the profile or a count. Copy nothing from an old description unchecked.
- Values exactly as stored — case and diacritics matter on `raw` fields (`RJEŠENJE`, `A` vs `a`).
- No counts that go stale with the next load: "about two thirds of acts", not "66,093". Ranges that only grow
  are written as "from 2015", not "2015–2026".
- No history and no implementation (`nn_links.py` produced it) unless the reader needs it to trust the value.
- A data defect is a finding for the loader, reported to the user, not written into the schema. If it
  changes query results, the description states the affirmative fact that lets a reader avoid it ("0 when
  not set", "the current value is in X"), not the defect.
- Describe `id` too: its pattern is what lets a reader parse or build keys.

## 5. Build, check and hand over

```bash
python3 -I schema_body.py --index <index> --descriptions <scratch>/desc.json --out <path>/<index>.described.json
```

It refuses to write a body with an over-long description or an unknown field, and prints old → new for every
change. Read the diff once more against the profile.

Run every example query a description gives (`limit 0`): it must parse and match something.

To prove the body applies in place without touching the real index: create a probe index by `PUT`ting the
saved read of the live schema (its thumbprint must come out equal to the live index's), write one document
(`PUT /api/<probe>/document` with `{"id": …, "doc": {…}}`), commit, `PUT` the body, check the document still
answers, then `DELETE /api/<probe>?delete_schema=true`.

Then report to the user, before anything is applied:
- the body's path and the diff;
- claims of the old descriptions that the data contradicts;
- data defects found on the way (they are findings for the loader or extractor, not for the schema);
- questions for the data owner: fields whose purpose or current use the data alone did not settle.

**Applying** (`--apply`, or `PUT` the body) needs the user's go-ahead, and the saved previous version: any
other path that PUTs the schema (the project's own schema script, a loader's reset) saves the live schema the
same way before and after. A description is not a built column,
so the node takes the `PUT` on an index holding documents and rebuilds nothing; `version` goes up by one.
Check what the project's loader does with that: a loader that fingerprints its own schema file, descriptions
included, will refuse the next load when only the live index changed, or demand a full reload when the file
changes. Update the project's schema source in the same step, and say plainly if its loader needs to stop
hashing descriptions.
