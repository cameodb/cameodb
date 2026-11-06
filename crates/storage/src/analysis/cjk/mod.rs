//! Chinese, Japanese and Korean, searchable by word: the `cjk` and `cjk_fold` analyzers.
//!
//! Chinese and Japanese put no spaces between words, and every Han, kana or Hangul character is
//! alphanumeric, so `SimpleTokenizer` makes one term of a whole clause between two punctuation
//! marks and only a query for exactly that clause matches. No dictionary is needed to do better.
//! [`CjkBigram`] indexes each character alone *and* paired with the next, both at the first
//! character's position (Lucene's `CJKBigramFilter` with `outputUnigrams`). A query word the
//! analyzer splits becomes a phrase over those positions, so `道` matches the character wherever it
//! stands, and `孔子` matches only where 孔 and 子 stand together.
//!
//! [`ChineseFold`] maps each traditional character to its simplified form ahead of the pairs, so
//! `學而` and `学而` are one term. The table is a fold, not a conversion: it only has to give the
//! text and the query one form, never correct simplified text, so one character goes to one, with
//! no context. Traditional to simplified is the direction that folds (many to one); the reverse
//! branches (发 → 發 or 髮). For Chinese only: the table would take Japanese old-style kanji to
//! Chinese forms Japanese text does not use, so `cjk` is the analyzer for Japanese and Korean.

mod data {
    include!("data/fold_table.rs");
}

use tantivy::tokenizer::{Token, TokenFilter, TokenStream, Tokenizer};

/// Whether `c` is part of a CJK run: Han (unified, extensions and compatibility ideographs, with
/// `々` `〆` `〇`), Hiragana, Katakana (with halfwidth and the supplements) and Hangul (syllables
/// and jamo). Bopomofo is left out, as Lucene leaves it out.
fn is_cjk(c: char) -> bool {
    matches!(c as u32,
        0x1100..=0x11FF       // Hangul Jamo
        | 0x3005..=0x3007     // 々 〆 〇
        | 0x3040..=0x30FF     // Hiragana, Katakana
        | 0x3130..=0x318F     // Hangul Compatibility Jamo
        | 0x31F0..=0x31FF     // Katakana Phonetic Extensions
        | 0x3400..=0x4DBF     // CJK Extension A
        | 0x4E00..=0x9FFF     // CJK Unified Ideographs
        | 0xA960..=0xA97F     // Hangul Jamo Extended-A
        | 0xAC00..=0xD7FF     // Hangul Syllables, Jamo Extended-B
        | 0xF900..=0xFAFF     // CJK Compatibility Ideographs
        | 0xFF66..=0xFF9F     // Halfwidth Katakana
        | 0x1B000..=0x1B16F   // Kana Supplement, Extended-A, Small Kana Extension
        | 0x20000..=0x3FFFF // CJK Extensions B–H, Compatibility Supplement
    )
}

/// The token filter splitting CJK runs into characters and pairs. Placed after the lower-caser,
/// and ahead of the token cap, so a clause longer than the cap is split rather than dropped.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct CjkBigram;

impl TokenFilter for CjkBigram {
    type Tokenizer<T: Tokenizer> = CjkBigramFilter<T>;

    fn transform<T: Tokenizer>(self, tokenizer: T) -> CjkBigramFilter<T> {
        CjkBigramFilter { inner: tokenizer }
    }
}

#[derive(Clone)]
pub(crate) struct CjkBigramFilter<T> {
    inner: T,
}

impl<T: Tokenizer> Tokenizer for CjkBigramFilter<T> {
    type TokenStream<'a> = CjkBigramTokenStream<T::TokenStream<'a>>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        CjkBigramTokenStream {
            tail: self.inner.token_stream(text),
            shift: 0,
            source: String::new(),
            pieces: Vec::new(),
            next: 0,
            token: Token::default(),
            own: false,
        }
    }
}

/// One output token cut from the current input token: its bytes in `source`, its position, and
/// its offsets in the original text.
#[derive(Clone, Copy)]
struct Piece {
    start: usize,
    end: usize,
    position: usize,
    offset_from: usize,
    offset_to: usize,
}

pub(crate) struct CjkBigramTokenStream<T> {
    tail: T,
    /// Positions added so far: a token split into `n` positions moves every later one by `n - 1`.
    shift: usize,
    /// The input token being split; `pieces` index into it. Both are kept, so a stream allocates
    /// only while its longest token grows.
    source: String,
    pieces: Vec<Piece>,
    next: usize,
    /// The token handed out while pieces are emitted.
    token: Token,
    /// Whether `token` (a piece) or the tail's token (passed through) is current.
    own: bool,
}

impl<T: TokenStream> CjkBigramTokenStream<T> {
    /// Cut the tail's current token into pieces, or return `false` when it has no CJK in it and
    /// passes through whole.
    fn split(&mut self) -> bool {
        let token = self.tail.token();
        if token.text.is_ascii() || !token.text.chars().any(is_cjk) {
            return false;
        }
        self.source.clear();
        self.source.push_str(&token.text);
        self.pieces.clear();
        self.next = 0;
        // Offsets inside the token are exact only while its text is as long as the original:
        // lowercasing can change a letter's length (`İ`), and an offset off by one could land
        // inside a character. Then every piece carries the whole token's offsets.
        let exact = token.offset_to - token.offset_from == token.text.len();
        let offsets = |start: usize, end: usize| {
            if exact {
                (token.offset_from + start, token.offset_from + end)
            } else {
                (token.offset_from, token.offset_to)
            }
        };
        let mut position = token.position + self.shift;
        let mut chars = self.source.char_indices().peekable();
        while let Some((start, c)) = chars.next() {
            if is_cjk(c) {
                let end = start + c.len_utf8();
                let (offset_from, offset_to) = offsets(start, end);
                self.pieces.push(Piece {
                    start,
                    end,
                    position,
                    offset_from,
                    offset_to,
                });
                if let Some(&(_, next)) = chars.peek().filter(|(_, next)| is_cjk(*next)) {
                    let pair_end = end + next.len_utf8();
                    let (offset_from, offset_to) = offsets(start, pair_end);
                    self.pieces.push(Piece {
                        start,
                        end: pair_end,
                        position,
                        offset_from,
                        offset_to,
                    });
                }
            } else {
                // A run outside CJK: one piece, as `default` would have emitted it alone.
                let mut end = start + c.len_utf8();
                while let Some(&(at, next)) = chars.peek() {
                    if is_cjk(next) {
                        break;
                    }
                    end = at + next.len_utf8();
                    chars.next();
                }
                let (offset_from, offset_to) = offsets(start, end);
                self.pieces.push(Piece {
                    start,
                    end,
                    position,
                    offset_from,
                    offset_to,
                });
            }
            position += 1;
        }
        // The input token held one position; it now holds `position - first`.
        self.shift = position - token.position - 1;
        self.token.position_length = token.position_length;
        true
    }
}

impl<T: TokenStream> TokenStream for CjkBigramTokenStream<T> {
    fn advance(&mut self) -> bool {
        if self.own && self.next < self.pieces.len() {
            let piece = self.pieces[self.next];
            self.next += 1;
            self.token.text.clear();
            self.token
                .text
                .push_str(&self.source[piece.start..piece.end]);
            self.token.position = piece.position;
            self.token.offset_from = piece.offset_from;
            self.token.offset_to = piece.offset_to;
            return true;
        }
        if !self.tail.advance() {
            return false;
        }
        self.own = self.split();
        if self.own {
            return self.advance();
        }
        self.tail.token_mut().position += self.shift;
        true
    }

    fn token(&self) -> &Token {
        if self.own {
            &self.token
        } else {
            self.tail.token()
        }
    }

    fn token_mut(&mut self) -> &mut Token {
        if self.own {
            &mut self.token
        } else {
            self.tail.token_mut()
        }
    }
}

/// The folded form of `c`: simplified for a traditional character in the table, `c` otherwise.
fn fold(c: char) -> char {
    // Every key is in U+3400–U+9FFF; anything else, Latin and kana included, returns here.
    if !('\u{3400}'..='\u{9FFF}').contains(&c) {
        return c;
    }
    match data::KEYS.binary_search(&(c as u16)) {
        Ok(at) => char::from_u32(u32::from(data::VALUES[at])).unwrap_or(c),
        Err(_) => c,
    }
}

/// The token filter folding traditional Chinese to simplified, one character to one. Placed
/// ahead of [`CjkBigram`], so the pairs are made of folded characters.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ChineseFold;

impl TokenFilter for ChineseFold {
    type Tokenizer<T: Tokenizer> = ChineseFoldFilter<T>;

    fn transform<T: Tokenizer>(self, tokenizer: T) -> ChineseFoldFilter<T> {
        ChineseFoldFilter { inner: tokenizer }
    }
}

#[derive(Clone)]
pub(crate) struct ChineseFoldFilter<T> {
    inner: T,
}

impl<T: Tokenizer> Tokenizer for ChineseFoldFilter<T> {
    type TokenStream<'a> = ChineseFoldTokenStream<T::TokenStream<'a>>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        ChineseFoldTokenStream {
            tail: self.inner.token_stream(text),
            buffer: String::new(),
        }
    }
}

pub(crate) struct ChineseFoldTokenStream<T> {
    tail: T,
    /// Where a folded token is written before it is swapped in, kept so a stream allocates once.
    buffer: String,
}

impl<T: TokenStream> TokenStream for ChineseFoldTokenStream<T> {
    fn advance(&mut self) -> bool {
        if !self.tail.advance() {
            return false;
        }
        let text = &mut self.tail.token_mut().text;
        // U+3400–U+9FFF is encoded with a lead byte of 0xE3–0xE9; a token without one has
        // nothing to fold. Keys and values are all 3-byte characters, so the length never changes
        // and the token's offsets stay exact.
        if text.bytes().any(|b| (0xE3..=0xE9).contains(&b)) && text.chars().any(|c| fold(c) != c) {
            self.buffer.clear();
            self.buffer.extend(text.chars().map(fold));
            std::mem::swap(text, &mut self.buffer);
        }
        true
    }

    fn token(&self) -> &Token {
        self.tail.token()
    }

    fn token_mut(&mut self) -> &mut Token {
        self.tail.token_mut()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analysis::register_tokenizers;
    use sha2::{Digest, Sha256};
    use tantivy::collector::TopDocs;
    use tantivy::query::QueryParser;
    use tantivy::schema::{
        IndexRecordOption, STORED, Schema, TextFieldIndexing, TextOptions, Value,
    };
    use tantivy::snippet::SnippetGenerator;
    use tantivy::{Index, TantivyDocument, doc};

    /// (text, position, offset_from, offset_to) of every token `tokenizer` makes of `text`.
    fn tokens(tokenizer: &str, text: &str) -> Vec<(String, usize, usize, usize)> {
        let index = Index::create_in_ram(Schema::builder().build());
        register_tokenizers(&index);
        let mut analyzer = index.tokenizers().get(tokenizer).unwrap();
        let mut stream = analyzer.token_stream(text);
        let mut out = Vec::new();
        while stream.advance() {
            let t = stream.token();
            out.push((t.text.clone(), t.position, t.offset_from, t.offset_to));
        }
        out
    }

    fn terms(tokenizer: &str, text: &str) -> Vec<String> {
        tokens(tokenizer, text).into_iter().map(|t| t.0).collect()
    }

    fn at(list: &[(&str, usize)]) -> Vec<(String, usize)> {
        list.iter().map(|&(t, p)| (t.to_string(), p)).collect()
    }

    fn positioned(tokenizer: &str, text: &str) -> Vec<(String, usize)> {
        tokens(tokenizer, text)
            .into_iter()
            .map(|t| (t.0, t.1))
            .collect()
    }

    /// An index with one `cjk` field, and the ids of the documents a query string finds in it,
    /// best first, with their scores and the highlighted snippet of the best.
    struct Fixture {
        index: Index,
        body: tantivy::schema::Field,
    }

    impl Fixture {
        fn new(tokenizer: &str, docs: &[&str]) -> Fixture {
            let mut builder = Schema::builder();
            let indexing = TextFieldIndexing::default()
                .set_tokenizer(tokenizer)
                .set_index_option(IndexRecordOption::WithFreqsAndPositions);
            let body = builder.add_text_field(
                "body",
                TextOptions::default()
                    .set_indexing_options(indexing)
                    .set_stored(),
            );
            let id = builder.add_u64_field("id", STORED);
            let index = Index::create_in_ram(builder.build());
            register_tokenizers(&index);
            let mut writer = index.writer_with_num_threads(1, 15_000_000).unwrap();
            for (n, text) in docs.iter().enumerate() {
                writer
                    .add_document(doc!(body => *text, id => n as u64))
                    .unwrap();
            }
            writer.commit().unwrap();
            Fixture { index, body }
        }

        fn search(&self, query: &str) -> Vec<(usize, f32)> {
            let reader = self.index.reader().unwrap();
            let searcher = reader.searcher();
            let query = QueryParser::for_index(&self.index, vec![self.body])
                .parse_query(query)
                .unwrap();
            let id = self.index.schema().get_field("id").unwrap();
            searcher
                .search(&query, &TopDocs::with_limit(10).order_by_score())
                .unwrap()
                .into_iter()
                .map(|(score, address)| {
                    let doc: TantivyDocument = searcher.doc(address).unwrap();
                    let n = doc.get_first(id).and_then(|v| v.as_u64()).unwrap();
                    (n as usize, score)
                })
                .collect()
        }

        fn found(&self, query: &str) -> Vec<usize> {
            let mut ids: Vec<usize> = self.search(query).into_iter().map(|(n, _)| n).collect();
            ids.sort();
            ids
        }

        fn highlight(&self, query: &str, text: &str) -> String {
            let reader = self.index.reader().unwrap();
            let query = QueryParser::for_index(&self.index, vec![self.body])
                .parse_query(query)
                .unwrap();
            let generator =
                SnippetGenerator::create(&reader.searcher(), &*query, self.body).unwrap();
            generator.snippet(text).to_html()
        }
    }

    /// The step everything rests on: a phrase whose terms share offsets — a character and the
    /// pair it starts — matches only where the characters stand together, scores, and highlights.
    #[test]
    fn a_phrase_with_terms_at_one_offset_matches_scores_and_highlights() {
        let docs = [
            "子曰：學而時習之，不亦說乎？",
            "孔子曰：孔子聖人也。",
            "孔明見子，子不語。",
            "Confucius 孔子 said",
        ];
        let f = Fixture::new("cjk", &docs);
        // A pair: only where 孔 and 子 stand together, not doc 2, which holds both apart.
        assert_eq!(f.found("孔子"), vec![1, 3]);
        // Two occurrences score above one.
        let hits = f.search("孔子");
        assert_eq!(hits[0].0, 1, "{hits:?}");
        assert!(hits[0].1 > hits[1].1 && hits[1].1 > 0.0, "{hits:?}");
        // A character, wherever it stands, inside a clause or alone.
        assert_eq!(f.found("子"), vec![0, 1, 2, 3]);
        // A clause, and part of one.
        assert_eq!(f.found("學而時習之"), vec![0]);
        assert_eq!(f.found("而時習"), vec![0]);
        assert!(f.found("學時").is_empty());
        // Latin beside CJK in one field still matches as `default` would.
        assert_eq!(f.found("confucius"), vec![3]);
        // A snippet ends at its last token, so trailing punctuation is not in it.
        assert_eq!(
            f.highlight("孔子", docs[1]),
            "<b>孔子</b>曰：<b>孔子</b>聖人也"
        );
        assert_eq!(
            f.highlight("時習", docs[0]),
            "子曰：學而<b>時習</b>之，不亦說乎"
        );
    }

    #[test]
    fn cjk_emits_characters_and_pairs_at_the_first_characters_position() {
        assert_eq!(positioned("cjk", "道"), at(&[("道", 0)]));
        assert_eq!(
            positioned("cjk", "孔子"),
            at(&[("孔", 0), ("孔子", 0), ("子", 1)])
        );
        assert_eq!(
            positioned("cjk", "學而時習之"),
            at(&[
                ("學", 0),
                ("學而", 0),
                ("而", 1),
                ("而時", 1),
                ("時", 2),
                ("時習", 2),
                ("習", 3),
                ("習之", 3),
                ("之", 4),
            ])
        );
        // Pairs never cross punctuation, and later tokens move past the positions a run took.
        assert_eq!(
            positioned("cjk", "子曰，學 Hello"),
            at(&[("子", 0), ("子曰", 0), ("曰", 1), ("學", 2), ("hello", 3)])
        );
        // Offsets point at the characters in the original text.
        assert_eq!(
            tokens("cjk", "x 孔子")[1..],
            [
                ("孔".to_string(), 1, 2, 5),
                ("孔子".to_string(), 1, 2, 8),
                ("子".to_string(), 2, 5, 8)
            ]
        );
    }

    #[test]
    fn cjk_leaves_text_outside_runs_as_default_does() {
        // A mixed token: the Latin part is one term, lowercased, as `default` makes it.
        assert_eq!(
            positioned("cjk", "iPhone手机 2024年"),
            at(&[
                ("iphone", 0),
                ("手", 1),
                ("手机", 1),
                ("机", 2),
                ("2024", 3),
                ("年", 4)
            ])
        );
        let latin = "The SHA-256 of x is e3b0c442, naïve Größe";
        assert_eq!(terms("cjk", latin), terms("default", latin));
        assert_eq!(positioned("cjk", latin), positioned("default", latin));
    }

    #[test]
    fn cjk_bigrams_japanese_and_korean() {
        assert_eq!(terms("cjk", "東京へ"), ["東", "東京", "京", "京へ", "へ"]);
        assert_eq!(
            terms("cjk", "カタカナ"),
            ["カ", "カタ", "タ", "タカ", "カ", "カナ", "ナ"]
        );
        assert_eq!(
            terms("cjk", "한국어 문법"),
            ["한", "한국", "국", "국어", "어", "문", "문법", "법"]
        );
        let f = Fixture::new("cjk", &["東京都に住んでいます", "京都の東"]);
        assert_eq!(f.found("東京"), vec![0]);
        assert_eq!(f.found("京都"), vec![0, 1]);
    }

    #[test]
    fn cjk_fold_joins_traditional_and_simplified() {
        for (a, b) in [
            ("學而", "学而"),
            ("為", "爲"),
            ("為", "为"),
            ("裏", "裡"),
            ("裏", "里"),
            ("著", "着"),
            ("看著", "看着"),
            ("一個", "一个"),
            ("藉口", "借口"),
            ("瞭解", "了解"),
        ] {
            assert_eq!(terms("cjk_fold", a), terms("cjk_fold", b), "{a} ≡ {b}");
        }
        // Different words that both exist in both scripts stay apart.
        for (a, b) in [
            ("象", "像"),
            ("坐", "座"),
            ("呆", "待"),
            ("覆", "复"),
            ("幺", "么"),
        ] {
            assert_ne!(terms("cjk_fold", a), terms("cjk_fold", b), "{a} ≠ {b}");
        }
        // `cjk` does not fold.
        assert_ne!(terms("cjk", "學而"), terms("cjk", "学而"));
        let f = Fixture::new("cjk_fold", &["學而時習之，不亦說乎", "他看著窗外", "学习"]);
        assert_eq!(f.found("学而时习"), vec![0]);
        assert_eq!(f.found("看着"), vec![1]);
        assert_eq!(f.found("學習"), vec![2]);
        assert_eq!(f.highlight("看着", "他看著窗外"), "他<b>看著</b>窗外");
    }

    #[test]
    fn cjk_fold_leaves_kana_hangul_and_latin_alone() {
        for text in ["ひらがな カタカナ", "한국어", "Größe ABC 123"] {
            assert_eq!(terms("cjk_fold", text), terms("cjk", text), "{text}");
        }
        // Japanese is bigrammed by `cjk_fold` too, but its kanji would be folded: Japanese fields
        // take `cjk`.
        assert_eq!(terms("cjk", "學校"), ["學", "學校", "校"]);
    }

    /// A clause longer than the token cap is split, not dropped; a Latin atom over it still is.
    #[test]
    fn the_cap_applies_to_pieces() {
        let clause = "道".repeat(60); // 180 bytes, over the 128-byte cap as one token
        assert_eq!(terms("cjk", &clause).len(), 119);
        let atom = format!("{}道", "0".repeat(129));
        assert_eq!(terms("cjk", &atom), ["道"]);
    }

    #[test]
    fn the_table_is_sorted_in_range_and_idempotent() {
        assert_eq!(data::KEYS.len(), 2_974);
        assert!(data::KEYS.windows(2).all(|w| w[0] < w[1]));
        for (&k, &v) in data::KEYS.iter().zip(&data::VALUES) {
            for c in [k, v] {
                assert!((0x3400..=0x9FFF).contains(&c), "U+{c:04X}");
            }
            assert_ne!(k, v);
            let folded = char::from_u32(u32::from(v)).unwrap();
            assert_eq!(fold(folded), folded, "U+{k:04X} → U+{v:04X} folds again");
        }
    }

    #[test]
    fn the_manifest_matches_the_committed_table() {
        let manifest = include_str!("data/MANIFEST.txt");
        let line = manifest
            .lines()
            .find(|l| l.starts_with("file\tfold_table.rs\t"))
            .expect("fold_table.rs line");
        let digest = Sha256::digest(include_bytes!("data/fold_table.rs"));
        let hex: String = digest.iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(
            line.rsplit('\t').next(),
            Some(hex.as_str()),
            "regenerate the manifest"
        );
        assert!(manifest.contains(&format!("pairs {}", data::KEYS.len())));
    }
}
