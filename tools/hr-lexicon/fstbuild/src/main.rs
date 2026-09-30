//! Writes the Croatian analyzer's data files from the tables tools/hr-lexicon/build.py prepares.
//!
//! usage: hr-lexicon-fstbuild WORK_DIR OUT_DIR
//! For each model (`plain`, `folded`): `<model>.exceptions.tsv` and `<model>.guesser.tsv`
//! (key TAB value, sorted) become fst maps, `<model>.hot.tsv` (keys) an fst map with value 0,
//! and `<model>.adds.tsv` (one string per line, in id order) the packed `<model>.adds.bin`.
use std::fs;
use std::path::Path;

use tantivy_fst::MapBuilder;

fn map(src: &Path, dst: &Path, with_values: bool) {
    let text = fs::read_to_string(src).unwrap_or_else(|e| panic!("{}: {e}", src.display()));
    let mut rows: Vec<(&str, u64)> = text
        .lines()
        .map(|l| match l.split_once('\t') {
            Some((k, v)) if with_values => (k, v.parse().expect("value")),
            _ => (l, 0),
        })
        .collect();
    // fst wants byte order; the Python side sorts by code point, which differs for some chars
    rows.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
    let mut b = MapBuilder::memory();
    for (k, v) in rows {
        b.insert(k, v).unwrap_or_else(|e| panic!("{}: {k}: {e}", src.display()));
    }
    fs::write(dst, b.into_inner().unwrap()).unwrap();
}

fn adds(src: &Path, dst: &Path) {
    let text = fs::read_to_string(src).unwrap();
    let strings: Vec<&str> = text.split_terminator('\n').collect();
    let mut out = Vec::new();
    out.extend_from_slice(&(strings.len() as u32).to_le_bytes());
    let mut offset = 0u32;
    for s in &strings {
        out.extend_from_slice(&offset.to_le_bytes());
        offset += s.len() as u32;
    }
    out.extend_from_slice(&offset.to_le_bytes());
    for s in &strings {
        out.extend_from_slice(s.as_bytes());
    }
    fs::write(dst, out).unwrap();
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let (work, out) = (Path::new(&args[1]), Path::new(&args[2]));
    for model in ["plain", "folded"] {
        let p = |f: &str| work.join(format!("{model}.{f}"));
        let o = |f: &str| out.join(format!("{model}.{f}"));
        map(&p("exceptions.tsv"), &o("exceptions.fst"), true);
        map(&p("guesser.tsv"), &o("guesser.fst"), true);
        map(&p("hot.tsv"), &o("hot.fst"), false);
        adds(&p("adds.tsv"), &o("adds.bin"));
    }
}
