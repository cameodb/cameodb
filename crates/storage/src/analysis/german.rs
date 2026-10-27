//! German written without umlauts, ahead of the stemmer.
//!
//! The German Snowball stemmer already takes `ä` `ö` `ü` to `a` `o` `u` and `ß` to `ss`, so a
//! query that simply drops the dots finds the word. But a keyboard without umlauts writes them the
//! German way, `ae` `oe` `ue`: `Haeuser` stemmed apart from `Häuser`, and on the UD German treebank
//! only 10.6% of words with an umlaut were found by their `ae`/`oe`/`ue` spelling.
//!
//! Both spellings are taken to the plain vowel here, as Lucene's German normalization does:
//! `ae` → `a`, `oe` → `o`, and `ue` → `u` except after a vowel or `q`, where it is a vowel of its
//! own (`neue`, `Feuer`, `Quelle`). The rule also rewrites words that never had an umlaut
//! (`Michael`, `Poet`), but text and query take the same path, so only a word that then meets
//! another one matters: on the treebank, one pair of the 5,549 lemmas did.

use tantivy::tokenizer::{Token, TokenFilter, TokenStream, Tokenizer};

/// The token filter: placed after the lower-caser, and before the stemmer.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct GermanSpelling;

impl TokenFilter for GermanSpelling {
    type Tokenizer<T: Tokenizer> = GermanSpellingFilter<T>;

    fn transform<T: Tokenizer>(self, tokenizer: T) -> GermanSpellingFilter<T> {
        GermanSpellingFilter { inner: tokenizer }
    }
}

#[derive(Clone)]
pub(crate) struct GermanSpellingFilter<T> {
    inner: T,
}

impl<T: Tokenizer> Tokenizer for GermanSpellingFilter<T> {
    type TokenStream<'a> = GermanSpellingTokenStream<T::TokenStream<'a>>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        GermanSpellingTokenStream {
            tail: self.inner.token_stream(text),
            buffer: String::new(),
        }
    }
}

pub(crate) struct GermanSpellingTokenStream<T> {
    tail: T,
    /// Where a rewritten token is written before it is swapped in, kept so a stream allocates once.
    buffer: String,
}

impl<T: TokenStream> TokenStream for GermanSpellingTokenStream<T> {
    fn advance(&mut self) -> bool {
        if !self.tail.advance() {
            return false;
        }
        let text = &mut self.tail.token_mut().text;
        if normalize(text, &mut self.buffer) {
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

/// Write `token` with its umlauts, in either spelling, as plain vowels into `out`, and whether
/// anything changed.
pub(crate) fn normalize(token: &str, out: &mut String) -> bool {
    if !token.contains(['ä', 'ö', 'ü', 'ß'])
        && !["ae", "oe", "ue"].iter().any(|d| token.contains(d))
    {
        return false;
    }
    out.clear();
    let mut prev = None;
    let mut chars = token.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            'a' | 'o' | 'u' if chars.peek() == Some(&'e') => {
                out.push(c);
                let vowel_of_its_own = c == 'u' && prev.is_some_and(|p| "aeiouyäöüq".contains(p));
                if !vowel_of_its_own {
                    chars.next();
                }
            }
            'ä' => out.push('a'),
            'ö' => out.push('o'),
            'ü' => out.push('u'),
            'ß' => out.push_str("ss"),
            _ => out.push(c),
        }
        prev = Some(c);
    }
    out != token
}

#[cfg(test)]
mod tests {
    use super::*;

    fn normalized(token: &str) -> String {
        let mut out = String::new();
        if normalize(token, &mut out) {
            out
        } else {
            token.to_string()
        }
    }

    #[test]
    fn both_spellings_of_an_umlaut_meet() {
        for (umlaut, digraph) in [
            ("häuser", "haeuser"),
            ("größe", "groesse"),
            ("über", "ueber"),
            ("schöne", "schoene"),
            ("straße", "strasse"),
            ("müller", "mueller"),
        ] {
            assert_eq!(
                normalized(umlaut),
                normalized(digraph),
                "{umlaut} / {digraph}"
            );
        }
    }

    #[test]
    fn ue_after_a_vowel_or_q_stays() {
        for word in ["neue", "feuer", "quelle", "bauer", "steuer"] {
            assert_eq!(normalized(word), word);
        }
    }
}
