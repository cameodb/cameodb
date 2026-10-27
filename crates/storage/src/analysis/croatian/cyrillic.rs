//! Serbian Cyrillic to Latin, ahead of the Croatian stemmer.
//!
//! Serbian is written in both scripts, and the stemmer's models know only Latin: a Cyrillic word
//! reached them unchanged and was left as it was, so `камен` neither met `kamen` nor lost its
//! ending. Mapped first, a Cyrillic word takes the path its Latin spelling takes, to the same term.
//!
//! The mapping is the standard one, letter for letter, three letters to digraphs (`љ` `lj`, `њ`
//! `nj`, `џ` `dž`). It is lossless in this direction, so nothing is decided here that the stored
//! text cannot be read back for.
//!
//! **Per token, not per letter.** A token is mapped only when every Cyrillic letter in it is one of
//! the Serbian alphabet's thirty; any other — Russian `ы`, Ukrainian `і`, Macedonian `ѓ` — leaves
//! the whole token as it was. Mapping the letters it knows would turn Russian `язык` into `яzыk`,
//! a term of neither script that no query in either would ever write.

use tantivy::tokenizer::{Token, TokenFilter, TokenStream, Tokenizer};

/// The token filter: placed after the lower-caser, which has already taken every capital to its
/// small letter, so only the small letters are mapped.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct SerbianCyrillic;

impl TokenFilter for SerbianCyrillic {
    type Tokenizer<T: Tokenizer> = SerbianCyrillicFilter<T>;

    fn transform<T: Tokenizer>(self, tokenizer: T) -> SerbianCyrillicFilter<T> {
        SerbianCyrillicFilter { inner: tokenizer }
    }
}

#[derive(Clone)]
pub(crate) struct SerbianCyrillicFilter<T> {
    inner: T,
}

impl<T: Tokenizer> Tokenizer for SerbianCyrillicFilter<T> {
    type TokenStream<'a> = SerbianCyrillicTokenStream<T::TokenStream<'a>>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        SerbianCyrillicTokenStream {
            tail: self.inner.token_stream(text),
            buffer: String::new(),
        }
    }
}

pub(crate) struct SerbianCyrillicTokenStream<T> {
    tail: T,
    /// Where a mapped token is written before it is swapped in, kept so a stream allocates once.
    buffer: String,
}

impl<T: TokenStream> TokenStream for SerbianCyrillicTokenStream<T> {
    fn advance(&mut self) -> bool {
        if !self.tail.advance() {
            return false;
        }
        let text = &mut self.tail.token_mut().text;
        if to_latin(text, &mut self.buffer) {
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

/// Write `token` in Latin into `out`, and whether it was mapped: `false` for a token with no
/// Cyrillic in it, or with a Cyrillic letter outside the Serbian alphabet, which stay as they are.
pub(crate) fn to_latin(token: &str, out: &mut String) -> bool {
    // Every letter of the Cyrillic block (U+0400–U+04FF) is encoded with a lead byte of 0xD0–0xD3,
    // and no byte of any other character is one; a token without them is passed over in one scan.
    if !token.bytes().any(|b| (0xD0..=0xD3).contains(&b)) {
        return false;
    }
    out.clear();
    for c in token.chars() {
        match latin(c) {
            Some(latin) => out.push_str(latin),
            None if is_cyrillic(c) => return false,
            None => out.push(c),
        }
    }
    true
}

fn is_cyrillic(c: char) -> bool {
    ('\u{0400}'..='\u{04FF}').contains(&c)
}

/// The Latin for one small letter of the Serbian Cyrillic alphabet.
fn latin(c: char) -> Option<&'static str> {
    Some(match c {
        'а' => "a",
        'б' => "b",
        'в' => "v",
        'г' => "g",
        'д' => "d",
        'ђ' => "đ",
        'е' => "e",
        'ж' => "ž",
        'з' => "z",
        'и' => "i",
        'ј' => "j",
        'к' => "k",
        'л' => "l",
        'љ' => "lj",
        'м' => "m",
        'н' => "n",
        'њ' => "nj",
        'о' => "o",
        'п' => "p",
        'р' => "r",
        'с' => "s",
        'т' => "t",
        'ћ' => "ć",
        'у' => "u",
        'ф' => "f",
        'х' => "h",
        'ц' => "c",
        'ч' => "č",
        'џ' => "dž",
        'ш' => "š",
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mapped(token: &str) -> Option<String> {
        let mut out = String::new();
        to_latin(token, &mut out).then_some(out)
    }

    #[test]
    fn every_letter_of_the_alphabet_maps() {
        let cyrillic = "абвгдђежзијклљмнњопрстћуфхцчџш";
        let expected = "abvgdđežzijklljmnnjoprstćufhcčdžš";
        assert_eq!(cyrillic.chars().count(), 30);
        assert_eq!(mapped(cyrillic).as_deref(), Some(expected));
        for c in cyrillic.chars() {
            assert!(latin(c).is_some(), "{c}");
        }
    }

    #[test]
    fn a_token_with_any_other_cyrillic_letter_stays_whole() {
        for token in [
            "язык",   // Russian я, ы
            "їжак",   // Ukrainian ї
            "ѓавол",  // Macedonian ѓ
            "ѕвезда", // Macedonian ѕ
            "Камен",  // a capital: the lower-caser runs first, so none reaches here
        ] {
            assert_eq!(mapped(token), None, "{token}");
        }
    }

    #[test]
    fn latin_digits_and_mixed_tokens() {
        assert_eq!(mapped("kamen"), None);
        assert_eq!(mapped("čašа"), Some("čaša".to_string())); // Latin, then a Cyrillic а
        assert_eq!(mapped("2024"), None);
        assert_eq!(mapped("б2"), Some("b2".to_string()));
    }
}
