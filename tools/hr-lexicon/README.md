# hr-lexicon: data for the Croatian analyzers

Builds the data behind the `hr_stem` and `hr_stem_fold` tokenizers into
[`crates/storage/src/analysis/croatian/data`](../../crates/storage/src/analysis/croatian/data).
The runtime is [`crates/storage/src/analysis/croatian`](../../crates/storage/src/analysis/croatian/mod.rs).
Nothing here runs at build or run time of CameoDB: the generated files are committed.

## What the analyzer does

Each word becomes its dictionary form (`članka`, `člancima` → `članak`; `Umagu` → `umag`), so a
query for any form finds the others. Per word, the answer is a rewrite: strip a few chars from the
end and append a string. It comes from:

1. **Exceptions**: an fst of the forms the suffix rules get wrong (329k of the 1.7M known forms).
2. **Suffix rules**: an fst of reversed word endings (up to 7 chars) and the rewrite each implies,
   learned from the same data; the longest ending that applies wins. A word no rule covers is
   left as it is. Rules cover the words the lexicon lacks, and store the ~81% of known forms they
   already get right, which is why only exceptions are stored.
3. **Cache**: the 16k most frequent forms' final answers, built on first use.

`hr_stem_fold` folds diacritics first (`AsciiFoldingFilter`) and uses a second model built from the
corpus folded the same way, so accented text and a query typed without diacritics take one path to
one term.

Measured against hand-checked Croatian (hr500k, hrLex; see `eval/gate.py`): base-form query F1
97.1% (`hr_stem`) and 96.9% (`hr_stem_fold`); a base form typed without diacritics reaches 97.6% of
the accented occurrences. Data: 5.45 MB for both; about 40 ns per token on frequent words and 200
ns on rare ones.

## Sources and licences

| Input | Use | Licence |
|---|---|---|
| CLASSLA-web.hr 2.0, CLARIN.SI [11356/2079](http://hdl.handle.net/11356/2079): 5.9M web texts, 3.46B tokens, with lemmas | the model | **CC0** |
| `overrides.tsv`: our corrections | the model | ours (Apache-2.0) |
| hrLex 1.3 ([11356/1232](http://hdl.handle.net/11356/1232)), hr500k 1.0 ([11356/1792](http://hdl.handle.net/11356/1792)) | evaluation only, downloaded by `eval/gate.py` | CC BY-SA 4.0 |

The generated data derives only from the CC0 corpus and our overrides, and is distributed under
the storage crate's licence. Nothing is derived from the Ljubešić–Pandžić stemmer or from Snowball's
Serbian stemmer (its descendant, LGPL lineage). The CC BY-SA sets measure quality; nothing from
them — no entry, no rule — goes into the data or the overrides.

## The extract

The corpus file (`CLASSLA-web.hr.2.0.vert.tar.gz`, 26.8 GB, MD5 `b1456e27ffe6c9267f53bf634dfb78f6`)
is read once into an extract of token counts, which is what the build reads:
`agg_s10.tsv.gz` (98 MB), lines `form<TAB>lemma<TAB>upos<TAB>count`, a balanced sample of 579M
tokens: a text is in it when the first 32 bits of `md5(text id)`, over 2^32, times 60, floor below 10
— spread over every site of the crawl, not the first few. SHA-256
`f1f46753f4f33dc7767f2e3950ad27f3c03e649de32ac4e191118f674ed25ec2`; `build.py` refuses another.

The extract is too large for the repository and is kept alongside it by the maintainers, with the
measurements behind the choices here. To regenerate it from the public corpus (about 25 minutes to
download with 8 parallel streams, 25 to process, 27 GB of disk):

```
tools/hr-lexicon/corpus/download.sh /data/classla              # rclone; size-checked
python3 tools/hr-lexicon/corpus/full_pass.py /data/classla/CLASSLA-web.hr.2.0.vert.tar.gz out/
gzip -9 -n -c out/agg_s10.tsv > agg_s10.tsv.gz                 # -n: reproducible bytes
```

`full_pass.py` also writes the smaller nested samples (`agg_s1` … `agg_s9`) and counts for the
Legal genre and government sites, which the learning-curve measurements used. Why this size: the learning curve over 59M–579M
tokens is flat for common words from the start; rare grammar (imperfect, aorist, palatalization
under folding) keeps gaining up to about 465M.

## Rebuilding

Needs Python 3.9+ and a Rust toolchain.

```
python3 tools/hr-lexicon/build.py /path/to/agg_s10.tsv.gz      # ~1 minute; writes the data dir
python3 tools/hr-lexicon/eval/gate.py /path/to/agg_s10.tsv.gz  # scores it; fails below the floors
cargo test -p storage --lib analysis                           # runtime reproduces the build
```

Identical inputs give identical bytes (`data/MANIFEST.txt` records every checksum), so a diff of
the data directory is a diff of behaviour. `data/sample.tsv` lists 3,600 words with the terms the
build computed; the storage tests check the runtime reproduces every one, so the review of a
rebuild is the diff of that file.

**Any change to the committed data changes the terms some words index as.** Indexes written
before it keep the old terms and stop matching queries for those words. A rebuild therefore ships
with a CHANGELOG entry naming `hr_stem` / `hr_stem_fold` and telling users to reindex fields that
use them.

## Build steps (`lexicon.py`)

1. **Participle rule.** The corpus sometimes gives a passive participle its own lemma (`određen`),
   sometimes the verb (`odrediti`), splitting one word in two. Where forms tagged ADJ are
   lemmatized both ways, the participle lemma becomes the verb — only for lemmas shaped like a
   participle (ending in *-n*/*-t*), whose verb shares their stem, with the paired evidence at least
   1% of the lemma's uses. (Unguarded, a few mistagged tokens turned the preposition *na* into a
   verb; the F1 scores did not notice, because the whole family moved together.)
2. **Lemma overrides** from `overrides.tsv`.
3. **Most frequent lemma per form**, forms seen at least twice.
4. **Form overrides.**
5. **Suffix rules** learned from the result (endings up to 7 chars, a rule needs 3 word types),
   then pruned to the endings that decide something (310k → 45k, same output).

## Overrides

`overrides.tsv`: `kind<TAB>from<TAB>to<TAB>note`, lowercase with diacritics.

- `lemma umago umag` gives every form whose corpus lemma is `umago` the lemma `umag` — one line
  fixes a family (here, the Italian name of Umag leaking in as the lemma of *Umaga*, *Umagu*).
- `form vodnjana vodnjan` fixes one form.

The folded model's entries are derived by folding both sides. Write entries from evidence in the
documents being indexed or general knowledge of Croatian, never copied from hrLex or CLASSLA model
output. Only forms seen repeatedly are worth an entry: a word seen once may be a typing mistake. A
recurring typo (copied through tables issue after issue) may get an entry pointing at the correct
word's lemma, so the correctly typed query finds it.

## Reviewing a document collection

`review/` finds override candidates in a vocabulary of the documents to be indexed (lines
`word<TAB>count<TAB>capitalized count<TAB>context`). Set `HR_LEXICON_EXTRACT` to the extract's path.

```
python3 tools/hr-lexicon/review/analyze.py Umagu Vodnjana fazana   # what both analyzers return
python3 tools/hr-lexicon/review/review.py vocab.tsv                  # reports in review/out
```

- `split.txt`: word pairs differing only by an inflectional ending but reaching different terms
  (`Vodnjan` / `Vodnjana`), proper nouns first. Most are fine (names, genuine ambiguity); the ones
  that split a place or institution are the candidates.
- `junk.txt`: frequent words whose lemma is not itself a known word — how the *na* bug was found.
  Standard adjective lemmas rare on their own (`komunalan`) show up here too and are correct.
- `guessed.txt`: frequent words the lexicon lacks, and what the rules make of them.
- `terms.txt`: the key terms of Croatian administrative text, with every form found.

## Data format

Per model (`plain`, `folded`): `exceptions.fst` (form → rewrite), `guesser.fst` (reversed ending →
rewrite), `hot.fst` (the cached forms), `adds.bin` (u32 LE count *N*, *N*+1 u32 LE offsets, the
strings). A rewrite is one u64: `add_id << 7 | chars_to_strip`. The fst files are written by
`fstbuild/`, pinned to the `tantivy-fst` version the runtime reads them with. The runtime's
constants (`MAX_CTX`, `MIN_STEM`, `STRIP_BITS`) must equal the ones in `MANIFEST.txt`; a test checks.
