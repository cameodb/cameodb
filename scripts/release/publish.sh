#!/usr/bin/env bash
# Stage 4 — checksum everything, then copy the staged release into the web project.
#
#   scripts/release/publish.sh            # dry run: report exactly what would change
#   scripts/release/publish.sh --commit   # actually copy
#
# Dry run is the default because this stage writes into another repository's published tree,
# overwriting files that are already live. It prints, per file, whether it is new, unchanged,
# or replacing something — and for a replacement, what it is replacing.
#
# Checksums are written as `<hash>  <basename>`, not with an absolute path. The published
# .sha256 files currently read
#     a319fd07...  /Users/gc/code/cameodb-web/public/downloads/linux/cameodb
# which both leaks the maintainer's directory layout and makes `shasum -c cameodb.sha256`
# fail for every downloader, since that path does not exist on their machine. With a bare
# basename it works from the download directory, which is where anyone would run it.

set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

COMMIT=0
[ "${1:-}" = "--commit" ] && COMMIT=1

[ -d "$DIST" ] || die "nothing staged at $DIST — run scripts/release/build.sh first"
refuse_if_unhardened
[ -d "$WEB_ROOT" ] || die "web downloads directory not found at $WEB_ROOT (override with CAMEODB_WEB=...)"

require_tool shasum

section "checksums $VERSION"

while IFS= read -r f; do
    ( cd "$(dirname "$f")" && shasum -a 256 "$(basename "$f")" > "$(basename "$f").sha256" )
    ok "$(rel_to_dist "$f").sha256"
done < <(staged_artifacts)

# One aggregate file as well, so a downloader can verify the whole release in a single
# `shasum -c SHA256SUMS` instead of one command per artifact. Paths are relative to the
# staging root, which is the same layout as the published tree.
SUMS="$DIST/SHA256SUMS"
: > "$SUMS"
while IFS= read -r f; do
    ( cd "$DIST" && shasum -a 256 "$(rel_to_dist "$f")" ) >> "$SUMS"
done < <(staged_artifacts)
ok "SHA256SUMS ($(wc -l < "$SUMS" | tr -d ' ') entries)"

( cd "$DIST" && shasum -c SHA256SUMS > /dev/null ) || die "SHA256SUMS does not verify against the staged files"
ok "SHA256SUMS verifies"

# ---------------------------------------------------------------- manifest

# The record of what this release actually is. Built here rather than by hand because the
# question it answers — which commit produced the binary a user downloaded — is asked months
# later, when the tree has moved on.
MANIFEST="$DIST/MANIFEST.txt"
{
    printf 'CameoDB %s\n' "$VERSION"
    printf 'commit    %s%s\n' "$(git -C "$PROJECT_ROOT" rev-parse HEAD)" \
        "$([ -n "$(git -C "$PROJECT_ROOT" status --porcelain)" ] && printf ' (dirty at build time)')"
    printf 'built     %s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
    printf 'signed by %s\n' "$COSIGN_PUB"
    printf '\n%-52s %10s  %s\n' 'artifact' 'size' 'signed'
    while IFS= read -r f; do
        printf '%-52s %10s  %s\n' "$(rel_to_dist "$f")" \
            "$(du -h "$f" | cut -f1 | tr -d ' ')" \
            "$([ -f "$f.bundle" ] && printf yes || printf 'NO')"
    done < <(staged_artifacts)
} > "$MANIFEST"
ok "MANIFEST.txt"

# ---------------------------------------------------------------- publish

section "publish → $WEB_ROOT"

[ "$COMMIT" -eq 1 ] || info "dry run — nothing will be written; re-run with --commit"

# Refuse to publish an unsigned or unverifiable release. This is the last point at which a
# missing .bundle is cheap to fix; afterwards it is a live download with no signature.
missing_sigs=0
while IFS= read -r f; do
    [ -f "$f.bundle" ] || { warn "no signature for $(rel_to_dist "$f")"; missing_sigs=$((missing_sigs + 1)); }
done < <(staged_artifacts)
[ "$missing_sigs" -eq 0 ] || die "$missing_sigs artifact(s) unsigned — run scripts/release/sign.sh"

copy_one() {
    local src="$1" dest="$2" rel
    rel="${dest#"$WEB_ROOT"/}"

    if [ ! -f "$dest" ]; then
        printf '  %sadd%s      %s\n' "$_c_green" "$_c_off" "$rel"
    elif cmp -s "$src" "$dest"; then
        printf '  %ssame%s     %s\n' "$_c_yellow" "$_c_off" "$rel"
        return 0
    else
        printf '  %sreplace%s  %s\n' "$_c_red" "$_c_off" "$rel"
        printf '           was %s  %s\n' "$(du -h "$dest" | cut -f1 | tr -d ' ')" \
            "$(shasum -a 256 "$dest" | cut -c1-16)…"
        printf '           now %s  %s\n' "$(du -h "$src" | cut -f1 | tr -d ' ')" \
            "$(shasum -a 256 "$src" | cut -c1-16)…"
    fi

    if [ "$COMMIT" -eq 1 ]; then
        mkdir -p "$(dirname "$dest")"
        cp "$src" "$dest"
    fi
}

# Artifacts, their signatures and their per-file checksums keep the platform subdirectory
# layout the site already links to. The SBOMs, SHA256SUMS and the manifest sit at the root,
# next to cosign.pub, which is where dl.cameodb.com serves them from.
for platform in mac linux windows; do
    [ -d "$DIST/$platform" ] || continue
    for f in "$DIST/$platform"/*; do
        [ -f "$f" ] || continue
        # `*.version` is the record of what the Windows machine reported, used by the build
        # stage to check the staged .exe. It is not an artifact: `staged_artifacts` excludes it,
        # so it carries no signature and no checksum, and publishing it would put the one
        # unsigned, unhashed file in the download directory next to the signed ones.
        case "$f" in *.version) continue ;; esac
        copy_one "$f" "$WEB_ROOT/$platform/$(basename "$f")"
    done
done

for f in "$DIST"/*.json "$SUMS" "$MANIFEST"; do
    [ -f "$f" ] || continue
    copy_one "$f" "$WEB_ROOT/$(basename "$f")"
    # if-statements rather than `[ … ] && copy_one`: a false test as the last command in the
    # loop body returns non-zero, which under `set -e` would end the run silently.
    if [ -f "$f.bundle" ]; then copy_one "$f.bundle" "$WEB_ROOT/$(basename "$f").bundle"; fi
    if [ -f "$f.sha256" ]; then copy_one "$f.sha256" "$WEB_ROOT/$(basename "$f").sha256"; fi
done

# Nothing here deletes: a published artifact may be linked from release notes, an issue, or a
# user's script, and removing it on the strength of a version bump is not this script's call.
# But version-stamped files from an earlier release do linger, so name them — otherwise
# downloads/linux/ silently accumulates every version ever shipped.
stale="$(find "$WEB_ROOT" -type f \( -name 'cameodb[-_]*' \) ! -name "*${VERSION}*" 2>/dev/null | sort)"
if [ -n "$stale" ]; then
    section "stale in $WEB_ROOT (left in place)"
    printf '%s\n' "$stale" | while IFS= read -r f; do info "${f#"$WEB_ROOT"/}"; done
    info ""
    info "from earlier releases. Remove by hand if the site no longer links to them."
fi

section "done"
if [ "$COMMIT" -eq 1 ]; then
    info "published to $WEB_ROOT"
    info "the web project now has uncommitted changes — review and commit them there"
else
    info "dry run only. To publish:  scripts/release/publish.sh --commit"
fi
