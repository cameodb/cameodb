//! Portuguese plurals to their singular, and feminines to their masculine, ahead of the Snowball
//! stemmer.
//!
//! Snowball's Portuguese algorithm strips derivational and verb suffixes but has no plural step:
//! it reaches `inform` from both `informação` and `informações` only because `-ação`/`-ações` is one
//! of its suffixes. The commonest plural classes it leaves apart — `decisão`/`decisões`,
//! `municipal`/`municipais`, `possível`/`possíveis`, `lei`/`leis` — so a query for one form missed
//! the other. Reduced to the singular first, both take one path through the stemmer to one term.
//!
//! The rules follow the plural step of the RSLP stemmer (Orengo & Huyck, 2001): suffixes tried in
//! order, each with a shortest stem it leaves and words it does not apply to, a word passed over by
//! one rule still open to the next. Where RSLP decides by suffix alone, the written accent decides
//! here, because it is what tells the classes apart:
//!
//! * `-eis` is `-vel` (`possíveis`), `-il` (`fáceis`, an accented stem) or a diphthong (`leis`).
//! * `-is` is `-il` (`fuzis`, `civis`) only after a consonant in an unaccented stem; `táxis`, `júris`
//!   and `leis` keep their vowel.
//! * A final `-s` after an accented vowel is the singular's own (`país`, `português`, `através`),
//!   not a plural ending.
//!
//! A second step, [`masculine`], takes the feminines Snowball leaves apart to their masculine
//! (`obrigatória`, `portuguesa`).
//!
//! A singular passes through unchanged, so a word the rules misread still meets itself: the cost of
//! a miss is a plural that does not reach its singular, never a term that stops matching.

use tantivy::tokenizer::{Token, TokenFilter, TokenStream, Tokenizer};

/// The token filter: placed after the lower-caser, and before the stemmer, which expects the
/// accents this step reads.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct PortuguesePlural;

impl TokenFilter for PortuguesePlural {
    type Tokenizer<T: Tokenizer> = PortuguesePluralFilter<T>;

    fn transform<T: Tokenizer>(self, tokenizer: T) -> PortuguesePluralFilter<T> {
        PortuguesePluralFilter { inner: tokenizer }
    }
}

#[derive(Clone)]
pub(crate) struct PortuguesePluralFilter<T> {
    inner: T,
}

impl<T: Tokenizer> Tokenizer for PortuguesePluralFilter<T> {
    type TokenStream<'a> = PortuguesePluralTokenStream<T::TokenStream<'a>>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        PortuguesePluralTokenStream {
            tail: self.inner.token_stream(text),
        }
    }
}

pub(crate) struct PortuguesePluralTokenStream<T> {
    tail: T,
}

impl<T: TokenStream> TokenStream for PortuguesePluralTokenStream<T> {
    fn advance(&mut self) -> bool {
        if !self.tail.advance() {
            return false;
        }
        let text = &mut self.tail.token_mut().text;
        if let Some(singular) = singular(text) {
            *text = singular;
        }
        if let Some(masculine) = masculine(text) {
            *text = masculine;
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

/// Plurals whose singular no suffix rule reaches: an oxytone in `-s`, whose accent the plural
/// drops or moves, and a stressed diphthong the `-óis` rule would read as `-ol`.
const IRREGULAR: &[(&str, &str)] = &[
    ("heróis", "herói"),
    ("meses", "mês"),
    ("países", "país"),
    ("deuses", "deus"),
    ("gases", "gás"),
];

struct Rule {
    suffix: &'static str,
    replacement: &'static str,
    /// The fewest characters the rule may leave in front of the replacement.
    min_stem: usize,
    /// What the stem — the word without `suffix` — must be like.
    stem: fn(&str) -> bool,
    exceptions: &'static [&'static str],
}

const fn rule(suffix: &'static str, replacement: &'static str, min_stem: usize) -> Rule {
    Rule {
        suffix,
        replacement,
        min_stem,
        stem: |_| true,
        exceptions: &[],
    }
}

/// In order; the first rule that applies decides.
const RULES: &[Rule] = &[
    // bons, homens, jardins
    rule("ns", "m", 1),
    // decisões, nações, leões
    rule("ões", "ão", 1),
    // capitães, pães; mães is mãe
    Rule {
        exceptions: &["mães"],
        ..rule("ães", "ão", 1)
    },
    // municipais, jornais
    Rule {
        exceptions: &["cais", "pais", "mais", "jamais", "demais", "ademais"],
        ..rule("ais", "al", 1)
    },
    // papéis, hotéis, anéis
    rule("éis", "el", 1),
    // possíveis, móveis, níveis
    rule("veis", "vel", 1),
    // túneis, níqueis: the two common accented -el nouns outside -vel
    Rule {
        stem: |stem| matches!(stem, "tún" | "níqu"),
        ..rule("eis", "el", 1)
    },
    // fáceis, úteis, frágeis; an accented stem is a paroxytone in -il (pôneis is pônei)
    Rule {
        stem: has_accent,
        exceptions: &["pôneis", "jóqueis"],
        ..rule("eis", "il", 1)
    },
    // anzóis, lençóis, faróis
    rule("óis", "ol", 1),
    // fuzis, barris, civis; not táxis (accented) or leis (a diphthong)
    Rule {
        stem: |stem| !has_accent(stem) && stem.ends_with(|c: char| !is_vowel(c)),
        exceptions: &["lápis", "pires"],
        ..rule("is", "il", 2)
    },
    // portugueses, ingleses; not teses (too short a stem) or hipóteses (accented)
    Rule {
        stem: |stem| !has_accent(stem),
        ..rule("eses", "ês", 3)
    },
    // vezes, luzes, rapazes
    rule("zes", "z", 1),
    // mulheres, flores, lugares; a singular in -re is rare, and only some meet their plural
    // through the stemmer (árvore does, hectare does not)
    Rule {
        exceptions: &["hectares"],
        ..rule("res", "r", 3)
    },
    // cônsules, males
    rule("les", "l", 3),
    // pés, cafés, sofás, avós, robôs: an oxytone in a vowel takes a bare -s. Not -ís or -ês, which
    // are nearly all singulars (país, português, mês), nor the singulars listed.
    Rule {
        stem: |stem| stem.ends_with(['é', 'á', 'ó', 'ô']),
        exceptions: &[
            "através", "revés", "convés", "invés", "viés", "gás", "atrás", "aliás", "lilás",
            "satanás", "após", "nós", "sós", "retrós",
        ],
        ..rule("s", "", 1)
    },
    // casas, anos, partes, irmãs; not país or português, whose -s follows the stressed vowel. Not
    // -mos either: that is mostly the verb's first person plural, and taking only its -s turned it
    // into a noun's -o (fazemos, fazemo), which the stemmer then misread as one.
    Rule {
        // An accented -imos is an adjective or noun (próximos, empréstimos): no verb ending
        // in -imos carries a written accent.
        stem: |stem| {
            !stem.ends_with(is_stressed)
                && (!stem.ends_with("mo") || (stem.ends_with("imo") && has_accent(stem)))
        },
        exceptions: &[
            "lápis", "pires", "mas", "mais", "jamais", "demais", "ademais", "cais", "pois",
            "depois", "menos", "ônibus", "vírus", "bônus", "tênis", "óculos",
        ],
        ..rule("s", "", 2)
    },
];

/// The singular of `word`, or `None` when it is not a plural the rules recognise.
pub(crate) fn singular(word: &str) -> Option<String> {
    if !word.ends_with('s') || word.chars().count() < 3 {
        return None;
    }
    if let Some(&(_, singular)) = IRREGULAR.iter().find(|(plural, _)| *plural == word) {
        return Some(singular.to_string());
    }
    RULES.iter().find_map(|rule| {
        let stem = word.strip_suffix(rule.suffix)?;
        (stem.chars().count() >= rule.min_stem
            && (rule.stem)(stem)
            && !rule.exceptions.contains(&word))
        .then(|| format!("{stem}{}", rule.replacement))
    })
}

/// Feminine nouns in `-esa` that are not the feminine of an `-ês`: rewritten, they would only
/// take a key no other word has, but they are common enough to keep as written.
const NOT_FEMININE_OF_ES: &[&str] = &[
    "empresa",
    "despesa",
    "defesa",
    "surpresa",
    "sobremesa",
    "represa",
    "framboesa",
    "princesa",
];

/// The masculine of a singular feminine `word`, for the classes Snowball leaves apart, or `None`.
///
/// Snowball takes `-a` and `-o` off a long enough word itself, so most feminines already meet
/// their masculine. It misses three classes, each told apart from a verb by its written accent,
/// which the verb endings that look the same (`fazia`, `fizera`) never carry:
///
/// * `-ia` after an accented stem (`obrigatória`, `média`, `própria`): Snowball reads it as the
///   imperfect and takes the `i` too. Not `-ência` or `-ância`, which Snowball rewrites itself
///   (`existência` meets `existente`), nor `-éia`.
/// * `-era` after an accented stem (`inúmera`, `petrolífera`): read as the pluperfect.
/// * `-esa` for `-ês` (`portuguesa`, `francesa`), which Snowball has no rule for.
pub(crate) fn masculine(word: &str) -> Option<String> {
    if word == "boa" {
        return Some("bom".to_string());
    }
    if let Some(stem) = word.strip_suffix("ia") {
        let snowball_suffix = word.ends_with("ência") || word.ends_with("ância");
        // `-éia` is the old spelling of the diphthong `-eia`, whose masculine is `-eu` (européia,
        // europeu), not `-éio`. `-oria` is no verb's ending, so it needs no accent to be told
        // apart; that keeps `obrigatoria`, folded or typed without accents, meeting `obrigatorio`.
        let feminine = (has_accent(stem) && !stem.ends_with('é')) || stem.ends_with("or");
        if feminine && !snowball_suffix {
            return Some(format!("{stem}io"));
        }
    }
    if let Some(stem) = word.strip_suffix("era")
        && has_accent(stem)
    {
        return Some(format!("{stem}ero"));
    }
    if let Some(stem) = word.strip_suffix("esa")
        && stem.chars().count() >= 3
        && !has_accent(stem)
        && !NOT_FEMININE_OF_ES.contains(&word)
    {
        return Some(format!("{stem}ês"));
    }
    None
}

/// A vowel carrying the written stress: acute or circumflex. The tilde marks nasality and sits on
/// the plural's own vowel (`irmãs`), so it is not one.
fn is_stressed(c: char) -> bool {
    matches!(c, 'á' | 'é' | 'í' | 'ó' | 'ú' | 'â' | 'ê' | 'ô')
}

fn has_accent(stem: &str) -> bool {
    stem.contains(is_stressed)
}

fn is_vowel(c: char) -> bool {
    matches!(c, 'a' | 'e' | 'i' | 'o' | 'u') || is_stressed(c) || matches!(c, 'ã' | 'õ' | 'à')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_plural_class_reaches_its_singular() {
        for (plural, expected) in [
            ("bons", "bom"),
            ("homens", "homem"),
            ("decisões", "decisão"),
            ("leões", "leão"),
            ("capitães", "capitão"),
            ("mães", "mãe"),
            ("municipais", "municipal"),
            ("papéis", "papel"),
            ("possíveis", "possível"),
            ("túneis", "túnel"),
            ("fáceis", "fácil"),
            ("úteis", "útil"),
            ("pôneis", "pônei"),
            ("lençóis", "lençol"),
            ("fuzis", "fuzil"),
            ("civis", "civil"),
            ("táxis", "táxi"),
            ("leis", "lei"),
            ("portugueses", "português"),
            ("teses", "tese"),
            ("hipóteses", "hipótese"),
            ("vezes", "vez"),
            ("mulheres", "mulher"),
            ("cônsules", "cônsul"),
            ("casas", "casa"),
            ("irmãs", "irmã"),
            ("meses", "mês"),
            ("países", "país"),
        ] {
            assert_eq!(singular(plural).as_deref(), Some(expected), "{plural}");
        }
    }

    #[test]
    fn feminines_snowball_misses_reach_the_masculine() {
        for (feminine, expected) in [
            ("obrigatória", "obrigatório"),
            ("obrigatoria", "obrigatorio"),
            ("média", "médio"),
            ("própria", "próprio"),
            ("inúmera", "inúmero"),
            ("portuguesa", "português"),
            ("francesa", "francês"),
            ("boa", "bom"),
        ] {
            assert_eq!(masculine(feminine).as_deref(), Some(expected), "{feminine}");
        }
    }

    #[test]
    fn verbs_and_other_feminines_stay() {
        for word in [
            "fazia",      // imperfect: no accent
            "fizera",     // pluperfect: no accent
            "existência", // Snowball's own -ência
            "européia",   // -éia is the diphthong of europeu
            "empresa",
            "mesa",
            "casa",
        ] {
            assert_eq!(masculine(word), None, "{word}");
        }
    }

    #[test]
    fn singulars_and_invariant_words_pass_through() {
        for word in [
            "país",
            "português",
            "através",
            "lápis",
            "ônibus",
            "mais",
            "cais",
            "os",
            "decisão",
            "lei",
        ] {
            assert_eq!(singular(word), None, "{word}");
        }
    }
}
