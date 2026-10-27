//! Text analysis: the tokenizers a text field can name, and the one place they are registered.
//!
//! A tokenizer name in a schema is an on-disk contract. The analyzer behind it decides which
//! terms a document writes, and query-time analysis resolves the same name from the same index,
//! so the two agree only while the pipeline behind a name is unchanged. Changing one raises
//! nothing — documents already written simply stop matching the queries that found them — so a
//! change to what a name produces is a breaking change: it ships only with a CHANGELOG entry
//! telling users which fields to reindex. Names say which language, not which algorithm
//! (`hr_stem` is a dictionary lemmatizer, `it_stem` a Snowball stemmer): users pick by language.

mod croatian;

use tantivy::Index;
use tantivy::tokenizer::{
    AsciiFoldingFilter, Language, LowerCaser, RemoveLongFilter, SimpleTokenizer, Stemmer,
    TextAnalyzer,
};

pub(crate) use croatian::{CroatianStemmer, SerbianCyrillic};

/// Longest token, in bytes, that the tokenizing analyzers keep.
///
/// Tantivy's own `default` and `en_stem` cap tokens at 40 bytes, which silently drops the long
/// atoms this engine is routinely asked to match on — hex digests, base64 blobs, opaque keys.
/// A dropped token is invisible: the document indexes, the field reports itself as indexed, and
/// the term simply does not exist to be searched. Both are re-registered under their original
/// names with this cap, and every language analyzer uses it from the start.
///
/// Deliberately a constant rather than a [`StorageConfig`] knob. The cap decides which terms
/// exist on disk, so two shards holding the same data under different caps would answer the
/// same query differently, and nothing in a response would say why.
pub(crate) const MAX_INDEXED_TOKEN_LEN: usize = 128;

/// Every tokenizer a field may name, in the order an error lists them.
///
/// `raw` and `whitespace` are tantivy's builtins, registered by `Index` itself; the rest are
/// registered by [`register_tokenizers`]. A schema naming anything else is refused where it is
/// declared ([`IndexSchema::validate_tokenizers`]): accepted, it would store, take writes into the
/// WAL, and then fail every commit because the writer cannot find the analyzer.
pub(crate) const TOKENIZERS: &[&str] = &[
    "default",
    "raw",
    "whitespace",
    "en_stem",
    "de_stem",
    "es_stem",
    "fr_stem",
    "hr_stem",
    "hr_stem_fold",
    "it_stem",
    "it_stem_fold",
];

/// The languages tantivy stems itself, by tokenizer name. Croatian is not among them — see
/// [`CroatianStemmer`].
const TANTIVY_STEMMED: &[(&str, Language)] = &[
    ("en_stem", Language::English),
    ("de_stem", Language::German),
    ("es_stem", Language::Spanish),
    ("fr_stem", Language::French),
    ("it_stem", Language::Italian),
];

/// Whether `name` is a tokenizer this engine can build an index with.
pub(crate) fn is_known_tokenizer(name: &str) -> bool {
    TOKENIZERS.contains(&name)
}

/// Registers this engine's analyzers on an index.
///
/// A `TokenizerManager` is per-[`Index`]-instance and in-memory — nothing about it is persisted
/// with the index — so every instance handed out must pass through here. Both constructors
/// ([`open_tantivy_index`] and [`create_tantivy_index`]) do, and they are the only two in the
/// workspace; an `Index` built any other way silently falls back to the 40-byte builtins, lacks
/// every language analyzer, and writes terms that disagree with the rest of the shard.
pub(crate) fn register_tokenizers(index: &Index) {
    let manager = index.tokenizers();

    // `RemoveLongFilter` keeps tokens strictly shorter than its limit, so the limit is one past
    // the longest token to keep. Off by one here and a digest of exactly the cap disappears.
    //
    // Filter order matches tantivy's own construction of `default` and `en_stem`. Term bytes
    // are whatever the last filter emits, so a reordering here would not raise anything — it
    // would just stop matching the terms already on disk. Stemmers expect lowercase input and
    // do not lowercase themselves.
    let lowercased = || {
        TextAnalyzer::builder(SimpleTokenizer::default())
            .filter(RemoveLongFilter::limit(MAX_INDEXED_TOKEN_LEN + 1))
            .filter(LowerCaser)
    };

    manager.register("default", lowercased().build());
    for &(name, language) in TANTIVY_STEMMED {
        manager.register(name, lowercased().filter(Stemmer::new(language)).build());
    }

    // Folded before stemming for the reason `hr_stem_fold` is: `citta` and `città` then take one
    // path to one term. Measured on the Italian gazette, stemming first and folding after lets an
    // accent-free query miss 10% of accented tokens — exactly the `-ità` nouns and `-erà` futures
    // (`sanita`, `attivita`). The price: those stem shorter (`sanità` → `san`) and a future
    // singular no longer meets its other forms. `it_stem` keeps the accent-exact stems.
    manager.register(
        "it_stem_fold",
        lowercased()
            .filter(AsciiFoldingFilter)
            .filter(Stemmer::new(Language::Italian))
            .build(),
    );

    // Serbian Cyrillic is mapped to Latin right after the lower-caser, which it relies on for
    // capitals, and ahead of everything that knows only Latin: the fold, which then takes `ђ`
    // through `đ` to `d` as it takes the Latin letter, and the stemmer. The token cap is applied
    // to the text as written, so a token at the cap whose `џ` becomes `dž` comes out a byte or
    // two over it; the cap is there to keep long atoms, not to bound a term.
    manager.register(
        "hr_stem",
        lowercased()
            .filter(SerbianCyrillic)
            .filter(CroatianStemmer::plain())
            .build(),
    );
    // Folded *before* stemming, into a model built from the corpus folded the same way: text and
    // a query typed without diacritics then take one path to one term. Folding after stemming
    // looked up `fazana` and `fažana` separately and could land them on different words
    // (`fazan`, pheasant, and `fažana`); 8% of the gazette's accented words did.
    manager.register(
        "hr_stem_fold",
        lowercased()
            .filter(SerbianCyrillic)
            .filter(AsciiFoldingFilter)
            .filter(CroatianStemmer::folded())
            .build(),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every analyzer named in [`TOKENIZERS`], as tokens, from an index set up the way the store
    /// sets one up.
    fn analyze(tokenizer: &str, text: &str) -> Vec<String> {
        let index = Index::create_in_ram(tantivy::schema::Schema::builder().build());
        register_tokenizers(&index);
        let mut analyzer = index
            .tokenizers()
            .get(tokenizer)
            .unwrap_or_else(|| panic!("{tokenizer} is not registered"));
        let mut stream = analyzer.token_stream(text);
        let mut tokens = Vec::new();
        while stream.advance() {
            tokens.push(stream.token().text.clone());
        }
        tokens
    }

    /// The list validation reads and the registration an index gets are two separate places;
    /// a name in one and not the other passes validation and then fails every commit.
    #[test]
    fn every_listed_tokenizer_is_registered() {
        let index = Index::create_in_ram(tantivy::schema::Schema::builder().build());
        register_tokenizers(&index);
        for name in TOKENIZERS {
            assert!(
                index.tokenizers().get(name).is_some(),
                "{name} is listed as available but no index registers it"
            );
        }
    }

    #[test]
    fn hr_stem_reaches_the_dictionary_form() {
        // Case endings of a noun, an adjective and a verb, including the alternations a suffix
        // stemmer splits: fleeting a (članak/članka), sibilarization (točka/točki), jat (rijeka/rijeci).
        for (text, term) in [
            ("članak članka člankom članci članaka", "članak"),
            ("točka točke točki točaka", "točka"),
            ("Istarska ISTARSKE istarskoj istarskog", "istarski"),
            ("odlučio odlučila odlučili odlučiti", "odlučiti"),
            ("odlučuje odlučuju odlučivati", "odlučivati"),
            ("rijeka rijeke rijeci", "rijeka"),
        ] {
            let terms = analyze("hr_stem", text);
            assert!(terms.iter().all(|t| t == term), "{text}: {terms:?}");
        }
        // Istrian places, including the ones the corpus lemmatized wrongly and overrides fix.
        for (text, term) in [
            ("Umag Umaga Umagu", "umag"),
            ("Vodnjan Vodnjana Vodnjanu", "vodnjan"),
            ("Buje Buja Bujama", "buje"),
            ("Fažana Fažani Fažanu", "fažana"),
        ] {
            let terms = analyze("hr_stem", text);
            assert!(terms.iter().all(|t| t == term), "{text}: {terms:?}");
        }
        // Terms keep their diacritics: this analyzer does not fold. (Common words typed without
        // them often still find their term, because the web corpus has them lemmatized that way:
        // `zupanije` gives `županija` too. `hr_stem_fold` makes that hold for every word.)
        assert_eq!(analyze("hr_stem", "Županije"), vec!["županija".to_string()]);
        assert_eq!(analyze("hr_stem", "zupanije"), vec!["županija".to_string()]);
        // Words the dictionary lacks still lose their endings by the suffix rules.
        assert_eq!(analyze("hr_stem", "qtočkama"), analyze("hr_stem", "qtočka"));
    }

    #[test]
    fn hr_stem_fold_matches_queries_written_without_diacritics() {
        for (plain, accented) in [
            ("zupanije", "županije"),
            ("proracuna", "proračuna"),
            ("opcinsko", "općinsko"),
            ("vijece", "vijeće"),
            ("gradanima", "građanima"),
            // the case the first design got wrong: `fazana` looked up alone found `fazan`
            ("fazana", "Fažana"),
            ("fazani", "Fažani"),
        ] {
            assert_eq!(
                analyze("hr_stem_fold", plain),
                analyze("hr_stem_fold", accented),
                "{plain} must find {accented}"
            );
        }
        // And it still reaches the dictionary form.
        assert_eq!(
            analyze("hr_stem_fold", "Člancima"),
            vec!["clanak".to_string()]
        );
    }

    #[test]
    fn hr_analyzers_read_serbian_cyrillic() {
        for (cyrillic, latin) in [
            ("камен", "kamen"),
            ("Камена каменом", "kamena kamenom"),
            ("ћерка", "ćerka"),
            ("ђак ђаци", "đak đaci"),
            ("љубав", "ljubav"),
            ("њива", "njiva"),
            ("џеп", "džep"),
            ("чаша", "čaša"),
            ("шума", "šuma"),
            ("жена", "žena"),
            ("ЉУБЉАНА Џеп", "Ljubljana Džep"),
            ("Народне песме", "narodne pesme"),
        ] {
            for tokenizer in ["hr_stem", "hr_stem_fold"] {
                assert_eq!(
                    analyze(tokenizer, cyrillic),
                    analyze(tokenizer, latin),
                    "{tokenizer}: {cyrillic} must find {latin}"
                );
            }
        }
        // `hr_stem` keeps the diacritics the mapping writes, so `ћ` and `ч` stay apart as `ć` and
        // `č` do; `hr_stem_fold` joins them as it joins the Latin letters.
        assert_ne!(analyze("hr_stem", "ћерка"), analyze("hr_stem", "черка"));
        assert_eq!(
            analyze("hr_stem_fold", "ћерка"),
            analyze("hr_stem_fold", "черка")
        );
        // A token with a letter outside the Serbian alphabet is left whole, never half-mapped.
        assert_eq!(analyze("hr_stem", "язык"), vec!["язык".to_string()]);
        assert_eq!(
            analyze("hr_stem_fold", "ѓавол"),
            analyze("default", "ѓавол")
        );
    }

    /// The Cyrillic mapping changes nothing for Latin text: every word of the stemmer's sample
    /// analyzes as it did through the chain without it.
    #[test]
    fn hr_analyzers_analyze_latin_as_before() {
        let before = |stemmer: CroatianStemmer, fold: bool| {
            let chain = TextAnalyzer::builder(SimpleTokenizer::default())
                .filter(RemoveLongFilter::limit(MAX_INDEXED_TOKEN_LEN + 1))
                .filter(LowerCaser);
            if fold {
                chain.filter(AsciiFoldingFilter).filter(stemmer).build()
            } else {
                chain.filter(stemmer).build()
            }
        };
        let terms = |analyzer: &mut TextAnalyzer, text: &str| {
            let mut stream = analyzer.token_stream(text);
            let mut tokens = Vec::new();
            while stream.advance() {
                tokens.push(stream.token().text.clone());
            }
            tokens
        };
        let words: Vec<&str> = include_str!("croatian/data/sample.tsv")
            .lines()
            .filter_map(|line| line.split('\t').next())
            .collect();
        assert!(words.len() > 3_000);
        for (name, mut old) in [
            ("hr_stem", before(CroatianStemmer::plain(), false)),
            ("hr_stem_fold", before(CroatianStemmer::folded(), true)),
        ] {
            for word in &words {
                assert_eq!(analyze(name, word), terms(&mut old, word), "{name}: {word}");
            }
        }
    }

    /// Measurement, not a check: Cyrillic text against the same text transliterated by hand.
    ///
    /// `HR_CYRILLIC_SAMPLE` names a JSON-lines file of `{"cyr": ..., "lat": ...}` pairs. Reports,
    /// per analyzer, how many tokens reach a different term than their reference spelling (the
    /// mapping's error, expected zero), and how many the stemmer changed at all.
    #[test]
    #[ignore = "measurement; needs HR_CYRILLIC_SAMPLE"]
    fn measure_hr_cyrillic_against_a_reference() {
        let path = std::env::var("HR_CYRILLIC_SAMPLE").expect("HR_CYRILLIC_SAMPLE");
        let sample = std::fs::read_to_string(&path).expect("read the sample");
        for tokenizer in ["hr_stem", "hr_stem_fold"] {
            let (mut tokens, mut differ, mut stemmed) = (0usize, 0usize, 0usize);
            for line in sample.lines().filter(|line| !line.trim().is_empty()) {
                let pair: serde_json::Value = serde_json::from_str(line).expect("a JSON line");
                let (cyr, lat) = (pair["cyr"].as_str().unwrap(), pair["lat"].as_str().unwrap());
                let (from_cyr, from_lat) = (analyze(tokenizer, cyr), analyze(tokenizer, lat));
                // Under `hr_stem_fold` "changed" counts the fold of a diacritic as well.
                let plain = analyze("default", lat);
                assert_eq!(from_cyr.len(), from_lat.len(), "token count: {cyr}");
                tokens += from_cyr.len();
                differ += from_cyr
                    .iter()
                    .zip(&from_lat)
                    .filter(|(a, b)| a != b)
                    .count();
                stemmed += from_lat.iter().zip(&plain).filter(|(a, b)| a != b).count();
            }
            println!(
                "{tokenizer}: {tokens} tokens, {differ} differ from the reference, \
                 {stemmed} ({:.1}%) changed by the stemmer",
                100.0 * stemmed as f64 / tokens.max(1) as f64
            );
        }
    }

    #[test]
    fn it_stem_fold_matches_queries_written_without_accents() {
        for (plain, accented) in [
            ("citta", "Città"),
            ("attivita", "attività"),
            ("sanita", "SANITÀ"),
            ("perche", "perché"),
            ("procedera", "procederà"),
        ] {
            assert_eq!(
                analyze("it_stem_fold", plain),
                analyze("it_stem_fold", accented),
                "{plain} must find {accented}"
            );
        }
        // Still a stemmer: plurals meet their singular.
        assert_eq!(
            analyze("it_stem_fold", "regolamenti"),
            analyze("it_stem_fold", "regolamento")
        );
        // `it_stem` keeps accents, so there the plain spelling can be another term.
        assert_ne!(analyze("it_stem", "sanita"), analyze("it_stem", "sanità"));
    }

    #[test]
    fn tantivy_stemmers_conflate_plurals() {
        for (tokenizer, singular, plural) in [
            // Not `delibera`/`delibere`: Snowball reads `-ere` as an infinitive (`delib`).
            ("it_stem", "regolamento", "regolamenti"),
            ("it_stem", "Consiglio", "consigli"),
            ("de_stem", "Gemeinde", "Gemeinden"),
            ("fr_stem", "municipal", "municipaux"),
            ("es_stem", "concejal", "concejales"),
        ] {
            assert_eq!(
                analyze(tokenizer, singular),
                analyze(tokenizer, plural),
                "{tokenizer}: {singular} / {plural}"
            );
        }
    }

    /// Every tokenizing analyzer keeps a token of exactly the cap, not only the two that
    /// existed when the cap was introduced.
    #[test]
    fn language_analyzers_keep_tokens_up_to_the_cap() {
        let digest = "0".repeat(MAX_INDEXED_TOKEN_LEN);
        let over = "0".repeat(MAX_INDEXED_TOKEN_LEN + 1);
        for name in TOKENIZERS
            .iter()
            .filter(|name| !matches!(**name, "raw" | "whitespace"))
        {
            assert_eq!(analyze(name, &digest), vec![digest.clone()], "{name}");
            assert!(analyze(name, &over).is_empty(), "{name}");
        }
    }
}
