//! The README carries the query syntax table so it can be read without opening the source, and
//! this keeps that copy honest: it is generated from `syntax`, not written by hand.
//!
//! `UPDATE_DOCS=1 cargo test -p cameodb_mcp readme` rewrites the block; without it the test
//! compares and fails on a difference.

use cameodb_mcp::syntax::{README_BEGIN, README_END, markdown_reference};

fn readme_path() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("README.md")
}

#[test]
fn the_readme_syntax_table_matches_the_table() {
    let path = readme_path();
    let readme = std::fs::read_to_string(&path).expect("read README.md");

    let begin = readme
        .find(README_BEGIN)
        .expect("README must carry the generated-syntax begin marker");
    let end = readme
        .find(README_END)
        .expect("README must carry the generated-syntax end marker");
    assert!(begin < end, "README syntax markers are out of order");

    let block_start = begin + README_BEGIN.len();
    let current = &readme[block_start..end];
    let expected = format!("\n\n{}\n", markdown_reference());

    if std::env::var_os("UPDATE_DOCS").is_some() {
        if current != expected {
            let updated = format!("{}{}{}", &readme[..block_start], expected, &readme[end..]);
            std::fs::write(&path, updated).expect("write README.md");
        }
        return;
    }

    assert_eq!(
        current, expected,
        "the README syntax table has drifted from `syntax`; regenerate with \
         UPDATE_DOCS=1 cargo test -p cameodb_mcp readme"
    );
}
