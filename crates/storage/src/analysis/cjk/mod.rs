//! Chinese, Japanese and Korean, searchable by word: the `cjk`, `cjk_fold`, `cjk_ja_fold` and
//! `cjk_ko_fold` analyzers.
//!
//! Chinese and Japanese put no spaces between words, and every Han, kana or Hangul character is
//! alphanumeric, so `SimpleTokenizer` makes one term of a whole clause between two punctuation
//! marks and only a query for exactly that clause matches. No dictionary is needed to do better.
//! [`CjkBigram`] indexes each character alone *and* paired with the next, both at the first
//! character's position (Lucene's `CJKBigramFilter` with `outputUnigrams`). A query word the
//! analyzer splits becomes a phrase over those positions, so `道` matches the character wherever it
//! stands, and `孔子` matches only where 孔 and 子 stand together.
//!
//! Three folds run ahead of the pairs, one per language, each giving two spellings of one word a
//! single form. A fold is not a conversion: it only has to give the text and the query the same
//! form, never correct text, so one character goes to one, with no context.
//!
//! - [`CharFold::CHINESE`] (`cjk_fold`) maps traditional characters to simplified, so `學而` and
//!   `学而` are one term. Traditional to simplified is the direction that folds (many to one); the
//!   reverse branches (发 → 發 or 髮).
//! - [`CharFold::JAPANESE`] (`cjk_ja_fold`) maps old-style kanji to the post-war forms, so `淸吉`
//!   and `清吉` are one term. Japanese simplified differently from Chinese (氣 is 気, not 气), so
//!   neither table may take the other's language.
//! - [`KoreanFold`] (`cjk_ko_fold`) reads each Hanja as its Hangul syllable, so `家族` and `가족`
//!   are one term, with the initial-sound rule on the first Hanja of a run.

mod data {
    pub(super) mod zh {
        include!("data/zh_table.rs");
    }
    pub(super) mod ja {
        include!("data/ja_table.rs");
    }
    pub(super) mod ko {
        include!("data/ko_table.rs");
    }
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

/// One generated table: `KEYS` sorted, each key's folded form at the same index of `VALUES`.
/// Every key is a Han character (U+3400–U+9FFF) or a compatibility ideograph (U+F900–U+FAFF).
pub(crate) struct Table {
    keys: &'static [u16],
    values: &'static [u16],
}

impl Table {
    /// The folded form of `c`, or `None` when the table leaves it as it is.
    fn get(&self, c: char) -> Option<char> {
        // Anything outside the key ranges, Latin, kana and Hangul included, returns here.
        if !matches!(c as u32, 0x3400..=0x9FFF | 0xF900..=0xFAFF) {
            return None;
        }
        let at = self.keys.binary_search(&(c as u16)).ok()?;
        char::from_u32(u32::from(self.values[at]))
    }

    fn fold(&self, c: char) -> char {
        self.get(c).unwrap_or(c)
    }
}

static ZH: Table = Table {
    keys: &data::zh::KEYS,
    values: &data::zh::VALUES,
};
static JA: Table = Table {
    keys: &data::ja::KEYS,
    values: &data::ja::VALUES,
};
static KO: Table = Table {
    keys: &data::ko::KEYS,
    values: &data::ko::VALUES,
};

/// Whether a token may hold a key of any table: U+3400–U+9FFF has a UTF-8 lead byte of
/// 0xE3–0xE9, U+F900–U+FAFF one of 0xEF. A token without either has nothing to fold.
fn may_fold(text: &str) -> bool {
    text.bytes().any(|b| matches!(b, 0xE3..=0xE9 | 0xEF))
}

/// The token filter folding one character to one by a [`Table`]: traditional Chinese to
/// simplified, or old-style kanji to new. Placed ahead of [`CjkBigram`], so the pairs are made of
/// folded characters.
#[derive(Clone, Copy)]
pub(crate) struct CharFold {
    table: &'static Table,
}

impl CharFold {
    /// Traditional Chinese to simplified: `cjk_fold`. For Chinese only: it would take Japanese
    /// old-style kanji to Chinese forms Japanese text does not use.
    pub(crate) const CHINESE: CharFold = CharFold { table: &ZH };
    /// Japanese old-style kanji (旧字体) to the post-war forms: `cjk_ja_fold`.
    pub(crate) const JAPANESE: CharFold = CharFold { table: &JA };
}

impl TokenFilter for CharFold {
    type Tokenizer<T: Tokenizer> = CharFoldFilter<T>;

    fn transform<T: Tokenizer>(self, tokenizer: T) -> CharFoldFilter<T> {
        CharFoldFilter {
            inner: tokenizer,
            table: self.table,
        }
    }
}

#[derive(Clone)]
pub(crate) struct CharFoldFilter<T> {
    inner: T,
    table: &'static Table,
}

impl<T: Tokenizer> Tokenizer for CharFoldFilter<T> {
    type TokenStream<'a> = CharFoldTokenStream<T::TokenStream<'a>>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        CharFoldTokenStream {
            tail: self.inner.token_stream(text),
            table: self.table,
            buffer: String::new(),
        }
    }
}

pub(crate) struct CharFoldTokenStream<T> {
    tail: T,
    table: &'static Table,
    /// Where a folded token is written before it is swapped in, kept so a stream allocates once.
    buffer: String,
}

impl<T: TokenStream> TokenStream for CharFoldTokenStream<T> {
    fn advance(&mut self) -> bool {
        if !self.tail.advance() {
            return false;
        }
        let text = &mut self.tail.token_mut().text;
        // Keys and values are all 3-byte characters, so the length never changes and the token's
        // offsets stay exact.
        if may_fold(text) && text.chars().any(|c| self.table.get(c).is_some()) {
            self.buffer.clear();
            self.buffer.extend(text.chars().map(|c| self.table.fold(c)));
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

/// A Hangul syllable is an initial (19), a vowel (21) and an optional final (27 and none):
/// `0xAC00 + (initial * 21 + vowel) * 28 + final`.
const SYLLABLE_BASE: u32 = 0xAC00;
const SYLLABLE_LAST: u32 = 0xD7A3;
const VOWELS: u32 = 21;
const FINALS: u32 = 28;

/// The initial-sound rule (두음법칙) on the first syllable of a word read from Hanja, as South
/// Korean spelling writes it: ㄹ before ㅑ ㅒ ㅕ ㅖ ㅛ ㅠ ㅣ becomes silent ㅇ, and before any other
/// vowel ㄴ (李 리 → 이, 歷史 력사 → 역사, 樂園 락원 → 낙원); ㄴ before ㅕ ㅛ ㅠ ㅣ becomes ㅇ
/// (女子 녀자 → 여자).
fn initial_sound(syllable: char) -> char {
    const NIEUN: u32 = 2;
    const RIEUL: u32 = 5;
    const IEUNG: u32 = 11;
    let code = syllable as u32;
    if !(SYLLABLE_BASE..=SYLLABLE_LAST).contains(&code) {
        return syllable;
    }
    let index = code - SYLLABLE_BASE;
    let (initial, vowel, last) = (
        index / (VOWELS * FINALS),
        index / FINALS % VOWELS,
        index % FINALS,
    );
    // Vowel indexes: ㅑ 2, ㅒ 3, ㅕ 6, ㅖ 7, ㅛ 12, ㅠ 17, ㅣ 20.
    let to = match (initial, vowel) {
        (RIEUL, 2 | 3 | 6 | 7 | 12 | 17 | 20) | (NIEUN, 6 | 12 | 17 | 20) => IEUNG,
        (RIEUL, _) => NIEUN,
        _ => return syllable,
    };
    char::from_u32(SYLLABLE_BASE + (to * VOWELS + vowel) * FINALS + last).unwrap_or(syllable)
}

/// A leading consonant and a vowel of the conjoining jamo (U+1100–U+1112, U+1161–U+1175), as
/// Hangul arrives decomposed (NFD), joined into their syllable.
fn compose(lead: char, vowel: char) -> Option<char> {
    let (l, v) = (lead as u32, vowel as u32);
    if (0x1100..=0x1112).contains(&l) && (0x1161..=0x1175).contains(&v) {
        char::from_u32(SYLLABLE_BASE + ((l - 0x1100) * VOWELS + (v - 0x1161)) * FINALS)
    } else {
        None
    }
}

/// A syllable without a final and a trailing jamo consonant (U+11A8–U+11C2), joined.
fn add_final(syllable: char, trail: char) -> Option<char> {
    let (s, t) = (syllable as u32, trail as u32);
    let open =
        (SYLLABLE_BASE..=SYLLABLE_LAST).contains(&s) && (s - SYLLABLE_BASE).is_multiple_of(FINALS);
    if open && (0x11A8..=0x11C2).contains(&t) {
        char::from_u32(s + (t - 0x11A7))
    } else {
        None
    }
}

/// The token filter reading Korean Hanja as Hangul, for `cjk_ko_fold`. Placed ahead of
/// [`CjkBigram`]. Within a token it:
///
/// - composes conjoining jamo into syllables, the Hangul part of NFC, so decomposed text matches
///   a query typed normally;
/// - replaces each Hanja by its reading, one syllable per character, by Unihan `kHangul`;
/// - applies the initial-sound rule to the first Hanja of each run, so `女子` folds to `여자` and
///   `子女` to `자녀`.
///
/// One reading per character is lossy where Korean is ambiguous (金 is 금, never the surname 김),
/// and a word starting inside a Hanja run keeps its dictionary initial (新女性 → 신녀성).
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct KoreanFold;

impl TokenFilter for KoreanFold {
    type Tokenizer<T: Tokenizer> = KoreanFoldFilter<T>;

    fn transform<T: Tokenizer>(self, tokenizer: T) -> KoreanFoldFilter<T> {
        KoreanFoldFilter { inner: tokenizer }
    }
}

#[derive(Clone)]
pub(crate) struct KoreanFoldFilter<T> {
    inner: T,
}

impl<T: Tokenizer> Tokenizer for KoreanFoldFilter<T> {
    type TokenStream<'a> = KoreanFoldTokenStream<T::TokenStream<'a>>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        KoreanFoldTokenStream {
            tail: self.inner.token_stream(text),
            buffer: String::new(),
        }
    }
}

pub(crate) struct KoreanFoldTokenStream<T> {
    tail: T,
    /// Where a folded token is written before it is swapped in, kept so a stream allocates once.
    buffer: String,
}

impl<T: TokenStream> TokenStream for KoreanFoldTokenStream<T> {
    fn advance(&mut self) -> bool {
        if !self.tail.advance() {
            return false;
        }
        let text = &mut self.tail.token_mut().text;
        // Conjoining jamo have the lead byte 0xE1 (U+1000–U+1FFF); with neither jamo nor a key,
        // the token passes as it is.
        let jamo = text.bytes().any(|b| b == 0xE1);
        if !jamo && !(may_fold(text) && text.chars().any(|c| KO.get(c).is_some())) {
            return true;
        }
        self.buffer.clear();
        let mut in_run = false;
        for c in text.chars() {
            if let Some(reading) = KO.get(c) {
                self.buffer.push(if in_run {
                    reading
                } else {
                    initial_sound(reading)
                });
                in_run = true;
                continue;
            }
            in_run = false;
            let joined = self
                .buffer
                .chars()
                .next_back()
                .and_then(|last| compose(last, c).or_else(|| add_final(last, c)));
            match joined {
                Some(syllable) => {
                    self.buffer.pop();
                    self.buffer.push(syllable);
                }
                None => self.buffer.push(c),
            }
        }
        // A Hanja and its syllable are both 3 bytes, so only composed jamo shorten the token;
        // then [`CjkBigram`] gives each piece the whole token's offsets.
        std::mem::swap(text, &mut self.buffer);
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
        // Japanese is bigrammed by `cjk_fold` too, but its kanji would be folded to Chinese forms:
        // Japanese fields take `cjk_ja_fold`.
        assert_eq!(terms("cjk_fold", "気氣"), terms("cjk_fold", "気气"));
    }

    #[test]
    fn cjk_ja_fold_joins_old_and_new_kanji() {
        for (a, b) in [
            ("淸吉", "清吉"),
            ("讀書", "読書"),
            ("氣持", "気持"),
            ("學校", "学校"),
            ("國", "国"),
            ("鹽", "塩"),
            ("畫", "画"),
            ("畵", "画"),
            ("靑い", "青い"),
            ("强い", "強い"),
            ("劒", "剣"),
            // A compatibility ideograph is the character it stands for, then folds on: 神 U+FA19.
            ("\u{FA19}", "神"),
        ] {
            assert_eq!(
                terms("cjk_ja_fold", a),
                terms("cjk_ja_fold", b),
                "{a} ≡ {b}"
            );
        }
        // The Chinese simplified forms stay apart from the Japanese ones, and a 書きかえ pair,
        // two characters a reform replaced one by the other, is left apart until a corpus asks.
        for (a, b) in [("気", "气"), ("図", "图"), ("慾", "欲"), ("呑", "吞")] {
            assert_ne!(
                terms("cjk_ja_fold", a),
                terms("cjk_ja_fold", b),
                "{a} ≠ {b}"
            );
        }
        for text in [
            "ひらがな カタカナ",
            "한국어",
            "Größe ABC 123",
            "清吉は東京へ",
        ] {
            assert_eq!(terms("cjk_ja_fold", text), terms("cjk", text), "{text}");
        }
        let f = Fixture::new(
            "cjk_ja_fold",
            &["淸吉と云ふ若い刺靑師", "清吉は青年", "國語"],
        );
        assert_eq!(f.found("清吉"), vec![0, 1]);
        assert_eq!(f.found("刺青"), vec![0]);
        assert_eq!(f.found("国語"), vec![2]);
        assert_eq!(
            f.highlight("清吉", "淸吉と云ふ若い刺靑師"),
            "<b>淸吉</b>と云ふ若い刺靑師"
        );
    }

    #[test]
    fn cjk_ko_fold_reads_hanja_as_hangul() {
        for (a, b) in [
            ("家族", "가족"),
            ("工夫", "공부"),
            ("無窮花", "무궁화"),
            // The initial-sound rule on the first Hanja of a run, not inside it.
            ("女子", "여자"),
            ("子女", "자녀"),
            ("李", "이"),
            ("歷史", "역사"),
            ("樂園", "낙원"),
            ("老人", "노인"),
            ("勞動", "노동"),
            // Mixed script in one word, as Korean writes it: the run ends at the Hangul.
            ("家族과", "가족과"),
            ("韓國語", "한국어"),
        ] {
            assert_eq!(
                terms("cjk_ko_fold", a),
                terms("cjk_ko_fold", b),
                "{a} ≡ {b}"
            );
        }
        assert_eq!(terms("cjk_ko_fold", "子女"), ["자", "자녀", "녀"]);
        // Conjoining jamo (NFD) are the syllables they spell.
        assert_eq!(
            terms(
                "cjk_ko_fold",
                "\u{1112}\u{1161}\u{11AB}\u{1100}\u{116E}\u{11A8}"
            ),
            terms("cjk_ko_fold", "한국")
        );
        for text in ["ひらがな カタカナ", "한국어 문법", "Größe ABC 123"] {
            assert_eq!(terms("cjk_ko_fold", text), terms("cjk", text), "{text}");
        }
        let f = Fixture::new("cjk_ko_fold", &["家族 a family", "우리 가족", "女子大學"]);
        assert_eq!(f.found("가족"), vec![0, 1]);
        assert_eq!(f.found("家族"), vec![0, 1]);
        assert_eq!(f.found("여자"), vec![2]);
        assert_eq!(f.found("family"), vec![0]);
        assert_eq!(f.highlight("가족", "家族 a family"), "<b>家族</b> a family");
    }

    #[test]
    fn the_initial_sound_rule() {
        for (from, to) in [
            ('녀', '여'),
            ('뇨', '요'),
            ('뉴', '유'),
            ('니', '이'),
            ('랴', '야'),
            ('력', '역'),
            ('례', '예'),
            ('룡', '용'),
            ('류', '유'),
            ('림', '임'),
            ('라', '나'),
            ('래', '내'),
            ('로', '노'),
            ('뢰', '뇌'),
            ('루', '누'),
            ('릉', '능'),
            ('나', '나'),
            ('노', '노'),
            ('가', '가'),
            ('A', 'A'),
        ] {
            assert_eq!(initial_sound(from), to, "{from}");
        }
    }

    /// A clause longer than the token cap is split, not dropped; a Latin atom over it still is.
    #[test]
    fn the_cap_applies_to_pieces() {
        let clause = "道".repeat(60); // 180 bytes, over the 128-byte cap as one token
        assert_eq!(terms("cjk", &clause).len(), 119);
        let atom = format!("{}道", "0".repeat(129));
        assert_eq!(terms("cjk", &atom), ["道"]);
    }

    /// Each table: its size, sorted keys in range, values in range and never keys themselves (the
    /// fold is idempotent).
    #[test]
    fn the_tables_are_sorted_in_range_and_idempotent() {
        type Ranges = &'static [(u16, u16)];
        const HAN: Ranges = &[(0x3400, 0x9FFF)];
        const HAN_OR_COMPAT: Ranges = &[(0x3400, 0x9FFF), (0xF900, 0xFAFF)];
        const HANGUL: Ranges = &[(0xAC00, 0xD7A3)];
        let inside = |ranges: Ranges, c: u16| ranges.iter().any(|&(lo, hi)| (lo..=hi).contains(&c));
        let cases: [(&str, &Table, usize, Ranges, Ranges); 3] = [
            ("zh", &ZH, 2_974, HAN, HAN),
            ("ja", &JA, 905, HAN_OR_COMPAT, HAN),
            ("ko", &KO, 8_465, HAN_OR_COMPAT, HANGUL),
        ];
        for (name, table, size, keys_in, values_in) in cases {
            assert_eq!(table.keys.len(), size, "{name}");
            assert_eq!(table.values.len(), size, "{name}");
            assert!(table.keys.windows(2).all(|w| w[0] < w[1]), "{name}");
            for (&k, &v) in table.keys.iter().zip(table.values) {
                assert!(inside(keys_in, k), "{name} key U+{k:04X}");
                assert!(inside(values_in, v), "{name} value U+{v:04X}");
                assert_ne!(k, v, "{name}");
                let folded = char::from_u32(u32::from(v)).unwrap();
                assert_eq!(
                    table.fold(folded),
                    folded,
                    "{name} U+{k:04X} → U+{v:04X} folds again"
                );
            }
        }
    }

    #[test]
    fn the_manifest_matches_the_committed_tables() {
        let manifest = include_str!("data/MANIFEST.txt");
        for (name, bytes, pairs) in [
            (
                "zh_table.rs",
                &include_bytes!("data/zh_table.rs")[..],
                ZH.keys.len(),
            ),
            (
                "ja_table.rs",
                &include_bytes!("data/ja_table.rs")[..],
                JA.keys.len(),
            ),
            (
                "ko_table.rs",
                &include_bytes!("data/ko_table.rs")[..],
                KO.keys.len(),
            ),
        ] {
            let file = format!("file\t{name}\t");
            let line = manifest
                .lines()
                .find(|l| l.starts_with(&file))
                .unwrap_or_else(|| panic!("{name} line"));
            let hex: String = Sha256::digest(bytes)
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect();
            assert_eq!(
                line.rsplit('\t').next(),
                Some(hex.as_str()),
                "regenerate the manifest"
            );
            let counts = format!("counts\t{name}\t");
            let line = manifest
                .lines()
                .find(|l| l.starts_with(&counts))
                .unwrap_or_else(|| panic!("{name} counts"));
            assert!(line.ends_with(&format!("pairs {pairs}")), "{line}");
        }
    }
}
