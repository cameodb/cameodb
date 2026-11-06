# zh-fold: the table behind `cjk_fold`

Builds the traditional-to-simplified fold of the `cjk_fold` tokenizer into
[`crates/storage/src/analysis/cjk/data`](../../crates/storage/src/analysis/cjk/data).
The runtime is [`crates/storage/src/analysis/cjk`](../../crates/storage/src/analysis/cjk/mod.rs).
Nothing here runs at build or run time of CameoDB: the generated files are committed.

## What the table is

One character to one: each traditional Chinese character to its simplified form, 2,974 pairs,
written as two sorted `[u16; 2974]` arrays (keys and values, 12 KB), which the runtime
binary-searches. It is a fold, not a conversion. It only has to give the text and the query one
form, so none of OpenCC's phrase tables, which choose the correct traditional character from
context, are needed. Every key and value is in U+3400–U+9FFF, and the fold is idempotent.

## Sources and licences

| Input | Use | Licence |
|---|---|---|
| OpenCC `data/dictionary/TSCharacters.txt`, commit `fae2a0a911c30764e669a7d8b0591cd8e852934a` | the table: first value of each entry | Apache-2.0 |
| `overrides.tsv`: our reviewed pairs | the table | ours (Apache-2.0) |
| Unihan `kSimplifiedVariant` (Unicode 18.0) | cited as the reference of each override; no file copied | — |

The table is a derived work of OpenCC's list, changed as described below. Apache-2.0 §4 is met by
the credit in `MANIFEST.txt` and in the comment heading `fold_table.rs`. OpenCC ships no `NOTICE`.

## Build

```
mkdir -p /tmp/opencc && curl -sfL -o /tmp/opencc/TSCharacters.txt \
  https://raw.githubusercontent.com/BYVoid/OpenCC/fae2a0a911c30764e669a7d8b0591cd8e852934a/data/dictionary/TSCharacters.txt
uv run --project tools/zh-fold tools/zh-fold/build.py /tmp/opencc/TSCharacters.txt
cargo test -p storage --lib analysis::cjk
```

`build.py` refuses a source file with another sha256. Steps:

1. Read the 4,148 entries and take the first value of each. 3,221 change the character.
2. Apply `overrides.tsv`.
3. Keep the pairs whose characters are both in the BMP. This drops 256 pairs of rare extension
   characters, which measured 39 occurrences on the sample below.
4. Resolve chains, so that `fold(fold(c)) == fold(c)`.

It writes `fold_table.rs` and `MANIFEST.txt` (source, commit, overrides' sha256, counts, file
checksum). The storage tests check the arrays and the checksum.

## Overrides

OpenCC leaves some characters unchanged because both forms exist in simplified. Traditional books
still write the traditional form for the simplified word. Each override is checked against Unihan
`kSimplifiedVariant`. The reviewed list was measured on 15 Chinese books from Project Gutenberg
(1.4 M characters, about 99% traditional): the share of corpus occurrences that a query typed in
simplified would miss.

| Table | Missed |
|---|---|
| OpenCC alone | 0.65% |
| OpenCC with the overrides | 0.24% |

- **著→着** alone was 63% of OpenCC's misses (看著 for 看着).
- **Kept:** 藉→借, 瞭→了, 衹→只, 彞→彝, 嶴→岙, 薴→苎, 唚→吣, 垵→埯, 礆→硷.
  - 薴 also reaches 苎 through OpenCC's 薴→苧→苎.
- **Left out after review:**
  - 幺→么 and 阪→坂: both stand as written in simplified (幺 'smallest', 大阪).
  - 戠→只: not a simplification of the same word.
  - 覆→复: 覆 'cover' is its own word; 答覆→答复 is lost until a corpus shows it.
- **What is left unmatched should stay unmatched.** It is pairs that are different words in both
  scripts (呆/待, 象/像, 坐/座).

A new override needs a reason and a reference on its line. A change that alters the table changes
the terms `cjk_fold` writes, so it ships with a CHANGELOG note to reindex `cjk_fold` fields.
