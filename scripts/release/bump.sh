#!/usr/bin/env bash
# Set the version of the whole workspace — step 1 of RELEASE-CHECKLIST.md.
#
#   scripts/release/bump.sh <new-version>        # e.g. 0.3.8
#
# Rewrites what carries the number, and nothing else:
#   crates/*/Cargo.toml       each crate's own `version`, and the version pinned beside every
#                             internal `path` dependency
#   Cargo.lock                the workspace members only, via `cargo update --workspace`; no
#                             third-party dependency moves
#   docs/ARCHITECTURE.md      the "**Version:**" line
#   docs/DEPLOYMENT.md        the cameodb-<version>-x64.msi file name in the install examples
# History — CHANGELOG, ROADMAP, the release checklist, "since 0.3.7" notes — is never touched.
#
# Refuses a version that is not X.Y.Z, one that is not newer than the current one, and a tree
# whose crates already disagree. It checks afterwards that all six manifests agree and that no
# old pin is left. Moving [Unreleased] in CHANGELOG.md is step 2 and stays by hand.
set -euo pipefail
cd "$(dirname "$0")/../.."

die() { echo "bump: $*" >&2; exit 1; }

new="${1:-}"
[ $# -eq 1 ] && [ "$new" != "-h" ] && [ "$new" != "--help" ] || {
    echo "usage: $(basename "$0") <new-version>   (X.Y.Z)" >&2
    exit 2
}
[[ "$new" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || die "'$new' is not a version of the form X.Y.Z"

manifests=(crates/*/Cargo.toml)
versions="$(grep -h '^version = ' "${manifests[@]}" | sort -u)"
[ "$(printf '%s\n' "$versions" | wc -l | tr -d ' ')" -eq 1 ] || die "the crates already disagree on the version:
$(grep -n '^version = ' "${manifests[@]}")"
old="$(printf '%s' "$versions" | sed 's/^version = "//; s/"$//')"

[ "$new" != "$old" ] || die "the workspace is already at $old"
[ "$(printf '%s\n%s\n' "$old" "$new" | sort -V | tail -1)" = "$new" ] ||
    die "$new is older than the current $old"

# In place without `sed -i`, whose flags differ between GNU and BSD: the file keeps its inode and
# mode.
rewrite() { # rewrite <file> <sed expression>...
    local file="$1" tmp
    shift
    tmp="$(mktemp)"
    sed "$@" "$file" > "$tmp"
    cat "$tmp" > "$file"
    rm -f "$tmp"
}

# The dots are escaped: "0.3.7" must not match "0x3y7".
old_re="${old//./\\.}"
for m in "${manifests[@]}"; do
    rewrite "$m" \
        -e "s/^version = \"$old_re\"/version = \"$new\"/" \
        -e "/path = /s/version = \"$old_re\"/version = \"$new\"/"
done
rewrite docs/ARCHITECTURE.md -e "s/^\\*\\*Version:\\*\\* $old_re\$/**Version:** $new/"
rewrite docs/DEPLOYMENT.md -e "s/cameodb-$old_re-x64\\.msi/cameodb-$new-x64.msi/g"

cargo update --workspace --offline > /dev/null 2>&1 ||
    cargo update --workspace > /dev/null 2>&1 ||
    die "cargo could not refresh Cargo.lock; the manifests are changed, so run: cargo update --workspace"

# What was promised, checked.
after="$(grep -h '^version = ' "${manifests[@]}" | sort -u)"
[ "$after" = "version = \"$new\"" ] || die "the manifests do not agree after the bump:
$(grep -n '^version = ' "${manifests[@]}")"
if grep -n "path = .*version = \"$old_re\"" "${manifests[@]}"; then
    die "an internal dependency still pins $old"
fi
grep -q "^\\*\\*Version:\\*\\* $new\$" docs/ARCHITECTURE.md || die "docs/ARCHITECTURE.md has no '**Version:** $new' line"

echo "bump: $old -> $new"
git diff --stat -- "${manifests[@]}" Cargo.lock docs/ARCHITECTURE.md docs/DEPLOYMENT.md 2>/dev/null || true
echo "next: move [Unreleased] in CHANGELOG.md, then 'cargo run --bin cameodb -- --version' to confirm"
