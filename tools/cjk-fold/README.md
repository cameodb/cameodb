# cjk-fold: the tables behind `cjk_fold`, `cjk_ja_fold` and `cjk_ko_fold`

Builds the three fold tables of the CJK tokenizers into
[`crates/storage/src/analysis/cjk/data`](../../crates/storage/src/analysis/cjk/data).
The runtime is [`crates/storage/src/analysis/cjk`](../../crates/storage/src/analysis/cjk/mod.rs).
Nothing here runs at build or run time of CameoDB: the generated files are committed.

## What the tables are

Each table maps one character to one. It is written as two sorted `[u16; N]` arrays, keys and
values, which the runtime binary-searches. Each is a fold, not a conversion: it only has to give
the text and the query one form, so no context and no phrase tables are needed. Every key and
value is a 3-byte UTF-8 character, so a token is rewritten in place, and every table is
idempotent.

| Table | Analyzer | Maps | Pairs | Size |
|---|---|---|---|---|
| `zh_table.rs` | `cjk_fold` | traditional Chinese to simplified | 2,974 | 12 KB |
| `ja_table.rs` | `cjk_ja_fold` | Japanese old-style kanji (旧字体) to the post-war forms | 905 | 4 KB |
| `ko_table.rs` | `cjk_ko_fold` | Korean Hanja to their Hangul reading | 8,465 | 34 KB |

Keys are in U+3400–U+9FFF. The Japanese and Korean keys also include the compatibility
ideographs U+F900–U+FAFF. The Chinese and Japanese values are Han characters; the Korean values
are Hangul syllables. The Korean initial-sound rule (두음법칙) is applied at run time to the first
Hanja of a run, not in the table.

## Sources and licences

| Input | Use | Licence |
|---|---|---|
| OpenCC `data/dictionary/TSCharacters.txt`, commit `fae2a0a911c30764e669a7d8b0591cd8e852934a` | `zh`: first value of each entry | Apache-2.0 |
| OpenCC `data/dictionary/JPShinjitaiCharacters.txt`, same commit | `ja`: reversed | Apache-2.0 |
| Unihan 18.0.0 (`Unihan.zip`): `kJapaneseNewVariant` | `ja` | Unicode License v3 |
| Unihan 18.0.0: `kHangul` | `ko` | Unicode License v3 |
| Unihan 18.0.0: `kSimplifiedVariant`, `kZVariant`, `kSemanticVariant` | cited as the reference of overrides; checked by the build, no data copied | — |
| `zh-overrides.tsv`, `ja-overrides.tsv`: our reviewed pairs | `zh`, `ja` | ours (Apache-2.0) |

- *OpenCC:* the tables are derived works of its lists, changed as described below. Apache-2.0
  §4 is met by the credit in `MANIFEST.txt` and in the comment heading each table. OpenCC ships no
  `NOTICE`.
- *Unihan:* the Unicode License v3 asks for its notice with the data or its documentation. It is
  in [`licenses/LICENSE-UNICODE-3.0`](../../licenses/LICENSE-UNICODE-3.0), and each table derived
  from Unihan names it in its heading comment.

## Build

```
mkdir -p /tmp/cjk-fold/opencc
for f in TSCharacters.txt JPShinjitaiCharacters.txt; do
  curl -sfL -o /tmp/cjk-fold/opencc/$f \
    https://raw.githubusercontent.com/BYVoid/OpenCC/fae2a0a911c30764e669a7d8b0591cd8e852934a/data/dictionary/$f
done
curl -sfL -o /tmp/cjk-fold/Unihan.zip https://www.unicode.org/Public/18.0.0/ucd/Unihan.zip
uv run --project tools/cjk-fold tools/cjk-fold/build.py --opencc /tmp/cjk-fold/opencc --unihan /tmp/cjk-fold/Unihan.zip
cargo test -p storage --lib analysis::cjk
```

`build.py` refuses a source file with another sha256. It writes the three tables and
`MANIFEST.txt`: the sources, their checksums, the overrides' sha256, the counts and each file's
checksum. The storage tests check the arrays and the checksums.

## Chinese (`zh_table.rs`)

1. Read the 4,148 entries of `TSCharacters.txt` and take the first value of each. 3,221 change
   the character.
2. Apply `zh-overrides.tsv`.
3. Keep the pairs whose characters are both in the BMP. This drops 256 pairs of rare extension
   characters, which measured 39 occurrences on the sample below.
4. Resolve chains, so that `fold(fold(c)) == fold(c)`.

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

## Japanese (`ja_table.rs`)

1. Reverse `JPShinjitaiCharacters.txt`: each line is a new form and the old forms it replaces
   (408 old forms). Six old forms sit under two new ones. The file names the one to take in a
   `# @reverse-prefer:` comment (鹽→塩, 畫→画, 鋪→舗, 莊→荘, 鬥→闘, 驅→駆), and the build refuses
   such a conflict without one.
2. Add Unihan `kJapaneseNewVariant` (364 pairs). It agrees with the reversed OpenCC list on every
   character both hold.
3. Apply `ja-overrides.tsv`.
4. Fold the compatibility ideographs U+F900–U+FAFF to their canonical characters (460), so they
   take the table from there (神 U+FA19 → 神 U+795E).
5. Keep BMP pairs and resolve chains (47, e.g. 劒→劍→剣).

The table was measured on the 21 Japanese books of Project Gutenberg (509,622 kanji, mostly
1900s–1920s fiction). The two sources fold 5.84% of the kanji occurrences. They both lack
the commonest old forms there, which are variants of one component that Unicode encodes apart:
逹 342, 强 241, 靑 238, 淸 172, 敎 166, 飮 119, 吿 50 and 尙 49. These are the overrides, each with
its reference.
- **The build checks each Unihan reference** against `Unihan_Variants.txt`, so a line cannot cite
  a variant Unihan does not list.
- **Left out after review** (listed at the top of `ja-overrides.tsv`):
  - 呑→吞 points the wrong way: 呑 is the Japanese standard form.
  - Some pairs are different words, such as 籐 (rattan) and 藤 (wisteria). Some semantic variants
    are wrong for Japanese, such as 只→子 and 捌→八.
  - The 1956 書きかえ pairs, two characters that the reform replaced one by the other (慾→欲,
    歎→嘆, 廻→回), stay apart until a corpus shows the fold is wanted.
- **No standard kanji (常用, 2010) is a key**, so modern text passes unchanged.

## Korean (`ko_table.rs`)

Unihan `kHangul` gives each Hanja its readings with source flags. The table takes one reading
per character: the one flagged `E` (basic education Hanja), then `0` (KS X 1001), then the
first. 383 characters have several readings, and for 74 the one taken is not the first listed
(樂 → 락). 60 characters outside the BMP are dropped.

The runtime adds two steps the table cannot hold:
- *The initial-sound rule* on the first Hanja of a run: ㄹ becomes ㅇ before ㅑ ㅒ ㅕ ㅖ ㅛ ㅠ ㅣ and ㄴ
  before the other vowels, and ㄴ becomes ㅇ before ㅕ ㅛ ㅠ ㅣ (女子 여자, 子女 자녀, 李 이, 樂園 낙원).
- *Conjoining jamo* (NFD) are composed into syllables.

There is no Korean corpus to measure on. Project Gutenberg's one Korean item, a Korean–English
dictionary, writes 25 words in Hanja; all 25 fold to its own Hangul headwords (家族 가족, 工夫 공부,
無窮花 무궁화). The fold is lossy where Korean is ambiguous:
- one reading serves each character (金 is 금, not the surname 김);
- a word that starts inside a run of Hanja keeps its dictionary reading (新女性 → 신녀성).

## Changing a table

A new override needs a reason and a reference on its line. A change that alters a table changes
the terms its analyzer writes, so it ships with a CHANGELOG note to reindex the fields that use
that analyzer.
