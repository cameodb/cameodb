//! Accents put back on the suffixes a Snowball stemmer reads them on, after folding.
//!
//! A `_fold` analyzer folds before it stems, so text and a query typed without accents take one
//! path to one term. But the Spanish, French and Portuguese stemmers know some suffixes only with
//! their accent: folded, `negociación`, `située` and `situações` keep endings the stemmer would have
//! taken off, and stop meeting their other forms. Measured on the UD treebanks, folding first cost
//! 2.6 (es), 2.7 (fr) and 1.9 (pt) points of forms reaching their lemma's term.
//!
//! This step writes the accent back on those suffixes before the stemmer runs, and the analyzer
//! folds once more after it. It reads only folded text, so a word and its accent-free spelling are
//! still one input and one term.
//!
//! **Only suffixes no unaccented word ends in.** `-cion` is always `-ción`; `-io` is as often a
//! noun (`edificio`) as the preterite `-ió`, and restoring it lost more than it won. Each table
//! keeps the suffixes that helped on the treebank and that the language never writes without the
//! accent, so the step does not fit one sample.

use tantivy::tokenizer::{Token, TokenFilter, TokenStream, Tokenizer};

/// Folded suffix, and the spelling the stemmer knows. Longest first: the first match decides.
pub(crate) type SuffixTable = &'static [(&'static str, &'static str)];

/// The nouns in `-ción` and the imperfect and conditional endings. The preterite `-ió` and the
/// imperfect `-ía` are left folded: they are also the endings of nouns.
pub(crate) const SPANISH: SuffixTable = &[
    ("abamos", "ábamos"),
    ("aramos", "áramos"),
    ("iamos", "íamos"),
    ("cion", "ción"),
    ("ian", "ían"),
];

/// The feminine participles and the `-ier`/`-ière` adjectives. A bare `-e` is left folded: it is
/// as often a word's own `e` as a participle's `é`.
pub(crate) const FRENCH: SuffixTable = &[
    ("ieres", "ières"),
    ("iere", "ière"),
    ("ees", "ées"),
    ("ee", "ée"),
];

/// The nasal endings, which the plural step reads too (`decisoes` reaches `decisão`). Also
/// restoring `-cao` and `-ções` won nothing more and merged words the accent-exact stemmer keeps
/// apart.
pub(crate) const PORTUGUESE: SuffixTable = &[("oes", "ões"), ("aes", "ães"), ("ao", "ão")];

/// The fewest bytes a suffix must leave in front of it: a short word is more often a word than an
/// ending (`mao`, `cao`).
const MIN_STEM: usize = 2;

/// The token filter: placed after the fold, which it relies on for the suffixes to be written
/// without accents, and before the stemmer.
#[derive(Clone, Copy, Debug)]
pub(crate) struct SuffixAccents(pub(crate) SuffixTable);

impl TokenFilter for SuffixAccents {
    type Tokenizer<T: Tokenizer> = SuffixAccentsFilter<T>;

    fn transform<T: Tokenizer>(self, tokenizer: T) -> SuffixAccentsFilter<T> {
        SuffixAccentsFilter {
            inner: tokenizer,
            table: self.0,
        }
    }
}

#[derive(Clone)]
pub(crate) struct SuffixAccentsFilter<T> {
    inner: T,
    table: SuffixTable,
}

impl<T: Tokenizer> Tokenizer for SuffixAccentsFilter<T> {
    type TokenStream<'a> = SuffixAccentsTokenStream<T::TokenStream<'a>>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        SuffixAccentsTokenStream {
            tail: self.inner.token_stream(text),
            table: self.table,
        }
    }
}

pub(crate) struct SuffixAccentsTokenStream<T> {
    tail: T,
    table: SuffixTable,
}

impl<T: TokenStream> TokenStream for SuffixAccentsTokenStream<T> {
    fn advance(&mut self) -> bool {
        if !self.tail.advance() {
            return false;
        }
        restore(&mut self.tail.token_mut().text, self.table);
        true
    }

    fn token(&self) -> &Token {
        self.tail.token()
    }

    fn token_mut(&mut self) -> &mut Token {
        self.tail.token_mut()
    }
}

/// Put the accent back on `word`'s suffix, if `table` has one for it.
pub(crate) fn restore(word: &mut String, table: SuffixTable) {
    let found = table
        .iter()
        .find(|(folded, _)| word.len() >= folded.len() + MIN_STEM && word.ends_with(folded));
    if let Some((folded, accented)) = found {
        word.truncate(word.len() - folded.len());
        word.push_str(accented);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn restored(word: &str, table: SuffixTable) -> String {
        let mut word = word.to_string();
        restore(&mut word, table);
        word
    }

    #[test]
    fn each_table_is_longest_first() {
        for table in [SPANISH, FRENCH, PORTUGUESE] {
            for (i, (a, _)) in table.iter().enumerate() {
                for (b, _) in &table[i + 1..] {
                    assert!(!b.ends_with(a), "{b} after {a} is never reached");
                }
            }
        }
    }

    #[test]
    fn suffixes_take_their_accent() {
        assert_eq!(restored("negociacion", SPANISH), "negociación");
        assert_eq!(restored("tenian", SPANISH), "tenían");
        assert_eq!(restored("situee", FRENCH), "située");
        assert_eq!(restored("premieres", FRENCH), "premières");
        assert_eq!(restored("situacoes", PORTUGUESE), "situacões");
        assert_eq!(restored("alemaes", PORTUGUESE), "alemães");
    }

    #[test]
    fn short_words_and_other_endings_stay() {
        assert_eq!(restored("cion", SPANISH), "cion");
        assert_eq!(restored("edificio", SPANISH), "edificio");
        assert_eq!(restored("cao", PORTUGUESE), "cao");
        assert_eq!(restored("livre", FRENCH), "livre");
    }
}
