//! Croatian: the analysis behind `hr_stem` and `hr_stem_fold`.
//!
//! A word becomes its dictionary form — `članka`, `člankom` and `članci` all become `članak`,
//! `Umagu` becomes `umag` — so a query for one form finds every other. The knowledge is data, built
//! by `tools/hr-lexicon` from a CC0 web corpus of 3 billion words annotated with lemmas, plus a
//! short list of our own corrections; `data/MANIFEST.txt` records the inputs and checksums.
//!
//! Per word, the answer is a rewrite: strip a few chars from the end, append a string. It comes
//! from, in order:
//!
//! 1. a cache of the 16k most frequent forms' final answers, built on first use;
//! 2. the exceptions fst: the forms the suffix rules get wrong (about a fifth of the 1.7 million
//!    known forms — the rest the rules already get right, so they are not stored);
//! 3. the suffix rules: the longest of the word's last seven chars that the guesser fst holds,
//!    walked in one pass over the word reversed; a word none of them covers is left as it is.
//!
//! All three give the same answer for a known form; the cache and the exceptions only skip work or
//! correct the rules. Everything maps straight from the binary, so nothing is parsed at startup
//! beyond the small add-string offsets, and a data file that fails to load fails the first
//! analysis loudly rather than silently indexing unstemmed terms.
//!
//! `hr_stem_fold` uses a second model built from the corpus with diacritics removed and reads its
//! input folded, so `Fažani` and `fazani` reach the same term by construction rather than by a
//! second lookup that could disagree.

use std::collections::HashMap;
use std::sync::LazyLock;

use tantivy::tokenizer::{Token, TokenFilter, TokenStream, Tokenizer};
use tantivy_fst::raw::{Fst, Output};
use tantivy_fst::{Map, Streamer};
use xxhash_rust::xxh3::Xxh3DefaultBuilder;

/// Longest suffix the rules read, in chars. Must equal `MAX_CTX` in `tools/hr-lexicon/guesser.py`.
const MAX_CTX: usize = 7;
/// A rule never leaves fewer chars than this. Must equal `MIN_STEM` in the same file.
const MIN_STEM: usize = 2;
/// Low bits of a rewrite: chars to strip; the rest is the add-string id. `STRIP_BITS` in `build.py`.
const STRIP_BITS: u32 = 7;

/// One model's data, borrowed from the binary.
struct Files {
    exceptions: &'static [u8],
    guesser: &'static [u8],
    adds: &'static [u8],
    hot: &'static [u8],
}

macro_rules! files {
    ($model:literal) => {
        Files {
            exceptions: include_bytes!(concat!("data/", $model, ".exceptions.fst")),
            guesser: include_bytes!(concat!("data/", $model, ".guesser.fst")),
            adds: include_bytes!(concat!("data/", $model, ".adds.bin")),
            hot: include_bytes!(concat!("data/", $model, ".hot.fst")),
        }
    };
}

static PLAIN: LazyLock<Model> = LazyLock::new(|| Model::load("plain", files!("plain")));
static FOLDED: LazyLock<Model> = LazyLock::new(|| Model::load("folded", files!("folded")));

/// A rewrite, decoded: how many chars to strip from the end, and what to append.
type Rewrite = u64;

struct Model {
    exceptions: Map<&'static [u8]>,
    guesser: Fst<&'static [u8]>,
    add_offsets: Vec<u32>,
    add_bytes: &'static str,
    /// Final answers for the most frequent forms; `None` means the word stays as it is.
    hot: HashMap<Box<str>, Option<Rewrite>, Xxh3DefaultBuilder>,
}

impl Model {
    fn load(name: &str, files: Files) -> Model {
        let broken = |what: &str| -> ! {
            panic!(
                "the {name} Croatian model's {what} is corrupt; rebuild it with tools/hr-lexicon"
            )
        };
        let exceptions = Fst::new(files.exceptions).unwrap_or_else(|_| broken("exceptions fst"));
        let guesser = Fst::new(files.guesser).unwrap_or_else(|_| broken("guesser fst"));
        let hot = Fst::new(files.hot).unwrap_or_else(|_| broken("hot-form fst"));

        let word = |i: usize| -> u32 {
            let b = files
                .adds
                .get(i * 4..i * 4 + 4)
                .unwrap_or_else(|| broken("add strings"));
            u32::from_le_bytes(b.try_into().unwrap())
        };
        let count = word(0) as usize;
        let add_offsets: Vec<u32> = (1..=count + 1).map(word).collect();
        let start = (count + 2) * 4;
        let add_bytes = files
            .adds
            .get(start..)
            .and_then(|b| std::str::from_utf8(b).ok())
            .filter(|s| s.len() == add_offsets[count] as usize)
            .unwrap_or_else(|| broken("add strings"));

        let mut model = Model {
            exceptions: Map::from(exceptions),
            guesser,
            add_offsets,
            add_bytes,
            hot: HashMap::default(),
        };
        let mut hot_map = HashMap::with_capacity_and_hasher(16_384, Xxh3DefaultBuilder);
        let hot = Map::from(hot);
        let mut forms = hot.stream();
        while let Some((form, _)) = forms.next() {
            let form = std::str::from_utf8(form).unwrap_or_else(|_| broken("hot-form fst"));
            hot_map.insert(form.into(), model.find(form));
        }
        model.hot = hot_map;
        model
    }

    /// The rewrite for `word`, if any: an exception, else the longest suffix rule that applies.
    fn find(&self, word: &str) -> Option<Rewrite> {
        self.exceptions.get(word).or_else(|| self.guess(word))
    }

    fn guess(&self, word: &str) -> Option<Rewrite> {
        // Walk the reversed word through the fst of reversed contexts, collecting a candidate at
        // every char boundary where a context ends; the longest that leaves a stem wins.
        let mut hits = [0 as Rewrite; MAX_CTX];
        let mut found = 0;
        let (mut node, mut out) = (self.guesser.root(), Output::zero());
        let mut utf8 = [0u8; 4];
        'walk: for c in word.chars().rev().take(MAX_CTX) {
            for &b in c.encode_utf8(&mut utf8).as_bytes() {
                match node.find_input(b) {
                    Some(i) => {
                        let t = node.transition(i);
                        out = out.cat(t.out);
                        node = self.guesser.node(t.addr);
                    }
                    None => break 'walk,
                }
            }
            if node.is_final() {
                hits[found] = out.cat(node.final_output()).value();
                found += 1;
            }
        }
        let chars = word.chars().count();
        hits[..found]
            .iter()
            .rev()
            .copied()
            .find(|&rw| chars >= strip_chars(rw) + MIN_STEM)
    }

    fn apply(&self, rewrite: Rewrite, word: &mut String) {
        let strip = strip_chars(rewrite);
        if strip > 0 {
            // A rewrite only ever strips a suffix of the word it was chosen for, and `find` has
            // checked the word is long enough.
            let cut = word
                .char_indices()
                .rev()
                .nth(strip - 1)
                .map_or(0, |(i, _)| i);
            word.truncate(cut);
        }
        let id = (rewrite >> STRIP_BITS) as usize;
        let (from, to) = (
            self.add_offsets[id] as usize,
            self.add_offsets[id + 1] as usize,
        );
        word.push_str(&self.add_bytes[from..to]);
    }

    fn stem(&self, word: &mut String) {
        let rewrite = match self.hot.get(word.as_str()) {
            Some(cached) => *cached,
            None => self.find(word),
        };
        if let Some(rewrite) = rewrite {
            self.apply(rewrite, word);
        }
    }
}

fn strip_chars(rewrite: Rewrite) -> usize {
    (rewrite & ((1 << STRIP_BITS) - 1)) as usize
}

/// Token filter replacing each token with its Croatian dictionary form. Expects lowercase input;
/// the folded variant also expects its input folded (`AsciiFoldingFilter` before it).
#[derive(Clone, Copy)]
pub(crate) struct CroatianStemmer {
    model: &'static LazyLock<Model>,
}

impl CroatianStemmer {
    /// For `hr_stem`: text as written, diacritics kept.
    pub(crate) fn plain() -> Self {
        CroatianStemmer { model: &PLAIN }
    }

    /// For `hr_stem_fold`: input already folded to plain letters.
    pub(crate) fn folded() -> Self {
        CroatianStemmer { model: &FOLDED }
    }
}

impl TokenFilter for CroatianStemmer {
    type Tokenizer<T: Tokenizer> = CroatianStemmerFilter<T>;

    fn transform<T: Tokenizer>(self, tokenizer: T) -> CroatianStemmerFilter<T> {
        CroatianStemmerFilter {
            inner: tokenizer,
            model: self.model,
        }
    }
}

#[derive(Clone)]
pub(crate) struct CroatianStemmerFilter<T> {
    inner: T,
    model: &'static LazyLock<Model>,
}

impl<T: Tokenizer> Tokenizer for CroatianStemmerFilter<T> {
    type TokenStream<'a> = CroatianStemmerTokenStream<T::TokenStream<'a>>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        CroatianStemmerTokenStream {
            tail: self.inner.token_stream(text),
            model: LazyLock::force(self.model),
        }
    }
}

pub(crate) struct CroatianStemmerTokenStream<T> {
    tail: T,
    model: &'static Model,
}

impl<T: TokenStream> TokenStream for CroatianStemmerTokenStream<T> {
    fn advance(&mut self) -> bool {
        if !self.tail.advance() {
            return false;
        }
        self.model.stem(&mut self.tail.token_mut().text);
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

    fn stem(model: &Model, word: &str) -> String {
        let mut w = word.to_string();
        model.stem(&mut w);
        w
    }

    /// The runtime against the model it was built from: `sample.tsv` holds, for 3,600 words —
    /// the most frequent, a spread of the rest, words only the rules handle, every override — the
    /// terms `tools/hr-lexicon` computed. A difference means the runtime and the build disagree
    /// on the format or the rule walk, and indexes would get terms no evaluation ever scored.
    #[test]
    fn both_models_reproduce_the_build() {
        let sample = include_str!("data/sample.tsv");
        let mut checked = 0;
        for line in sample.lines() {
            let mut cols = line.split('\t');
            let (word, plain, folded) = (
                cols.next().unwrap(),
                cols.next().unwrap(),
                cols.next().unwrap(),
            );
            assert_eq!(stem(&PLAIN, word), plain, "hr_stem of {word}");
            // the folded model's input is folded first, by the analyzer chain
            let ascii: String = word
                .chars()
                .map(|c| match c {
                    'č' | 'ć' => 'c',
                    'đ' => 'd',
                    'š' => 's',
                    'ž' => 'z',
                    c => c,
                })
                .collect();
            assert_eq!(stem(&FOLDED, &ascii), folded, "hr_stem_fold of {word}");
            checked += 1;
        }
        assert!(checked > 3_000, "sample has only {checked} words");
    }

    /// Answers from the cache equal answers from the lookup it skips.
    #[test]
    fn the_hot_cache_changes_no_answer() {
        for model in [&*PLAIN, &*FOLDED] {
            for (form, cached) in &model.hot {
                assert_eq!(*cached, model.find(form), "{form}");
            }
            assert_eq!(model.hot.len(), 16_384);
        }
    }

    /// The parameters the build recorded are the ones compiled in here.
    #[test]
    fn the_manifest_matches_the_runtime_constants() {
        let manifest = include_str!("data/MANIFEST.txt");
        let params = manifest
            .lines()
            .find(|l| l.starts_with("params\t"))
            .expect("params line");
        for expected in [
            format!("max_ctx={MAX_CTX}"),
            format!("min_stem={MIN_STEM}"),
            format!("strip_bits={STRIP_BITS}"),
        ] {
            assert!(
                params.split_whitespace().any(|p| p == expected),
                "{params} lacks {expected}"
            );
        }
    }
}
