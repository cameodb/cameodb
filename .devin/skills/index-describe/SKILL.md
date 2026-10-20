---
name: index-describe
description: Write or improve the index and field descriptions of a CameoDB index from its live data — profile the index, sample rows through the API or MCP tools, verify every claim, and produce a PUT /_config body that changes descriptions only. Use when asked to describe, document or enhance the schema of an index, or after `schema detect` / `data load` created one with no descriptions.
---

# Describe a CameoDB index from its data

Nothing in CameoDB infers a description (`FieldDef::new` leaves it `None`), and `schema detect` /
`data load` write types, tokenizers and fast columns only. A description is what an agent reads to choose
an index and build a query, so it has to be true of the data, not of the plan the data was made from. This
skill produces descriptions from evidence and a schema body that applies them.

Scripts (stdlib Python, run with `python3 -I`):
- `profile_index.py` — counts and a spread sample, as a readable report and JSON.
- `schema_body.py` — the live schema plus new descriptions as a `PUT` body; checks lengths, shows the diff, `--apply` sends it.

## 1. Find the node and the index

- Node: `http://127.0.0.1:9480` unless told otherwise; a node with security on needs `--key`.
- `GET /_indexes` (or MCP `list_indexes`) for the index, its count and current description;
  `GET /api/{index}/_config` (or MCP `describe_index`) for fields, types, tokenizers, `sortable`, `default_search`.
- **Save the complete schema first**: the whole `GET /api/{index}/_config` response, unedited, to
  `<index>.v<version>.<thumbprint>.json` in the project's schema history folder (create it beside the schema
  source when there is none). That file is the previous version once anything changes; nothing is applied
  without it. `schema_body.py` does this on every run and again after `--apply`.
- Look for the project that loads the index: a schema source file (e.g. `schema.py`), loader, extractor
  docs. They say what each field was *meant* to be; the data says what it *is*. Where they disagree the
  data wins and the disagreement is reported.

## 2. Profile

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

Rules:
- Every statement is checked against the profile or a count. Copy nothing from an old description unchecked.
- Values exactly as stored — case and diacritics matter on `raw` fields (`RJEŠENJE`, `A` vs `a`).
- No counts that go stale with the next load: "about two thirds of acts", not "66,093". Ranges that only grow
  are written as "from 2015", not "2015–2026".
- No history and no implementation (`nn_links.py` produced it) unless the reader needs it to trust the value.
- A known data defect that changes results gets a short warning in the field's description; the defect
  itself goes in the report to the user, not in the schema.
- Describe `id` too: its pattern is what lets a reader parse or build keys.

## 5. Build, check and hand over

```bash
python3 -I schema_body.py --index <index> --descriptions <scratch>/desc.json --out <path>/<index>.described.json
```

It refuses to write a body with an over-long description or an unknown field, and prints old → new for every
change. Read the diff once more against the profile.

Run every example query a description gives (`limit 0`): it must parse and match something.

To prove the body applies in place without touching the real index: create a probe index from the project's
schema source (its thumbprint in `stats.schema` must equal the live index's), write one document
(`PUT /api/<probe>/document` with `{"id": …, "doc": {…}}`), commit, `PUT` the body, check the document still
answers, then `DELETE /api/<probe>?delete_schema=true`.

Then report to the user, before anything is applied:
- the body's path and the diff;
- claims of the old descriptions that the data contradicts;
- data defects found on the way (they are findings for the loader or extractor, not for the schema).

**Applying** (`--apply`, or `PUT` the body) needs the user's go-ahead, and the saved previous version: any
other path that PUTs the schema (the project's own schema script, a loader's reset) saves the live schema the
same way before and after. A description is not a built column,
so the node takes the `PUT` on an index holding documents and rebuilds nothing; `version` goes up by one.
Check what the project's loader does with that: a loader that fingerprints its own schema file, descriptions
included, will refuse the next load when only the live index changed, or demand a full reload when the file
changes. Update the project's schema source in the same step, and say plainly if its loader needs to stop
hashing descriptions.
