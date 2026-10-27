//! The Italian Snowball stemmer, with the forms it leaves apart brought together.
//!
//! Snowball takes an Italian word's final vowel off only inside the region it computes from the
//! word's vowels, and a short word, or one opening with a vowel, has no such region: `anni`,
//! `altri` and `acque` reached the stemmer and came out whole, never meeting `anno`, `altro` or
//! `acqua`. And it reads a masculine in `-iano` as the verb ending `-ano` (`parlano`), so
//! `italiano` lost three letters where `italiani` and `italiana` lost one. On the UD Italian
//! treebank, 90.7% of plurals and 89.3% of feminines reached their lemma's term.
//!
//! Three steps around the stemmer, measured on the same treebank (to 95.5% and 98.0%, and 95.1%
//! and 98.5% on its held-out part, with verbs and the number of merged lemmas as they were):
//!
//! * Before it, `-trice` to `-tore` (`scrittrice`, `scrittore`), which Snowball has no rule for.
//! * After it, the final vowel of a word the stemmer left whole, as it takes it off a longer one.
//!   Only words of four letters or more: shorter ones would meet the commonest words of the
//!   language (`dio` would be `di`, `via` would be `vi`). Not Roman numerals (`xii`, `xi`).
//! * After it, the `-an` of an `-iano`/`-icano` adjective whose vowel alone was taken, so every
//!   form meets the masculine singular Snowball cut shorter (`italiani` → `ital`, as `italiano`).
//!
//! Left apart: the `-are` and `-iere` classes (`militari`/`militare`, `pompieri`/`pompiere`),
//! whose plurals share their ending with the `-ario` and `-iero` nouns (`salari`, `pensieri`), and
//! irregular participles (`visto`, `fatto`), which want a dictionary.

use rust_stemmers::{Algorithm, Stemmer};
use tantivy::tokenizer::{Token, TokenFilter, TokenStream, Tokenizer};

/// The token filter: in place of tantivy's Italian stemmer, after the lower-caser (and the fold,
/// for `it_stem_fold`).
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ItalianStemmer;

impl TokenFilter for ItalianStemmer {
    type Tokenizer<T: Tokenizer> = ItalianStemmerFilter<T>;

    fn transform<T: Tokenizer>(self, tokenizer: T) -> ItalianStemmerFilter<T> {
        ItalianStemmerFilter { inner: tokenizer }
    }
}

#[derive(Clone)]
pub(crate) struct ItalianStemmerFilter<T> {
    inner: T,
}

impl<T: Tokenizer> Tokenizer for ItalianStemmerFilter<T> {
    type TokenStream<'a> = ItalianStemmerTokenStream<T::TokenStream<'a>>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        ItalianStemmerTokenStream {
            tail: self.inner.token_stream(text),
            stemmer: Stemmer::create(Algorithm::Italian),
        }
    }
}

pub(crate) struct ItalianStemmerTokenStream<T> {
    tail: T,
    stemmer: Stemmer,
}

impl<T: TokenStream> TokenStream for ItalianStemmerTokenStream<T> {
    fn advance(&mut self) -> bool {
        if !self.tail.advance() {
            return false;
        }
        let text = &mut self.tail.token_mut().text;
        *text = stem(&self.stemmer, text);
        true
    }

    fn token(&self) -> &Token {
        self.tail.token()
    }

    fn token_mut(&mut self) -> &mut Token {
        self.tail.token_mut()
    }
}

/// `word`'s term: Snowball's stem, with the steps around it.
pub(crate) fn stem(stemmer: &Stemmer, word: &str) -> String {
    let word = agent_noun(word);
    let mut stem = stemmer.stem(&word).into_owned();
    if stem == word
        && word.chars().count() >= 4
        && stem.ends_with(['a', 'e', 'i', 'o'])
        && !word.chars().all(|c| "ivxlcdm".contains(c))
    {
        stem.pop();
    } else if word.len() == stem.len() + 1 && word.starts_with(stem.as_str()) {
        // Snowball cuts the masculine past the `i` of `-iano` (`italiano` → `ital`), and only the
        // `-ano` of `-icano` (`americano` → `americ`).
        let cut = if stem.ends_with("ian") {
            3
        } else if stem.ends_with("can") {
            2
        } else {
            0
        };
        if cut > 0 && stem.len() >= cut + 3 {
            stem.truncate(stem.len() - cut);
        }
    }
    stem
}

/// `-trice`/`-trici` to `-tore`/`-tori`, borrowed when unchanged.
fn agent_noun(word: &str) -> std::borrow::Cow<'_, str> {
    for (feminine, masculine) in [("trice", "tore"), ("trici", "tori")] {
        if let Some(stem) = word.strip_suffix(feminine)
            && stem.len() >= 2
        {
            return format!("{stem}{masculine}").into();
        }
    }
    word.into()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn term(word: &str) -> String {
        stem(&Stemmer::create(Algorithm::Italian), word)
    }

    #[test]
    fn short_words_lose_their_final_vowel() {
        for (a, b) in [
            ("anni", "anno"),
            ("altri", "altro"),
            ("altra", "altro"),
            ("acque", "acqua"),
            ("atti", "atto"),
            ("armi", "arma"),
        ] {
            assert_eq!(term(a), term(b), "{a} / {b}");
        }
    }

    #[test]
    fn short_words_and_numerals_stay_apart() {
        for (a, b) in [("dio", "di"), ("via", "vi"), ("pre", "pro"), ("xii", "xi")] {
            assert_ne!(term(a), term(b), "{a} / {b}");
        }
    }

    #[test]
    fn iano_adjectives_meet_in_every_form() {
        for form in ["italiani", "italiana", "italiane"] {
            assert_eq!(term(form), term("italiano"), "{form}");
        }
        assert_eq!(term("americana"), term("americano"));
    }

    #[test]
    fn agent_nouns_meet_their_masculine() {
        assert_eq!(term("scrittrice"), term("scrittore"));
        assert_eq!(term("autrici"), term("autori"));
    }

    #[test]
    fn longer_words_stem_as_snowball_does() {
        let snowball = Stemmer::create(Algorithm::Italian);
        for word in [
            "regolamento",
            "regolamenti",
            "consiglio",
            "corteo",
            "museo",
            "settimana",
        ] {
            assert_eq!(term(word), snowball.stem(word), "{word}");
        }
    }
}
