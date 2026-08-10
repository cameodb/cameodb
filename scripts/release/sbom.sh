#!/usr/bin/env bash
# Stage 2 — generate the SPDX and CycloneDX SBOMs that ship with the release.
#
#   scripts/release/sbom.sh
#
# Writes dist/<version>/cameodb.spdx.json and cameodb.cyclonedx.json.
#
# Scope is the workspace's Cargo.lock and nothing else. That is a narrower scan than
# `syft dir:.`, on purpose — the previously published SBOM was a full directory scan, which
# produced a 30MB document whose SPDX `name` was the literal string
# "/Users/gc/code/cameodb", whose 882 "packages" included ten copies of actions/setup-node
# and actions/checkout picked up from workflow files under the tree, and which listed 147k
# individual files. None of that describes the shipped binary. A consumer scanning it for
# CVEs would be matching against the maintainer's laptop.
#
# Two flags do the narrowing, and they are not interchangeable:
#   --override-default-catalogers rust-cargo-lock-cataloger   name a cataloger (--select-catalogers
#                                                             only accepts tags, not names)
#   --select-catalogers -file                                 drop the file cataloger, which syft
#                                                             re-adds by default and which is the
#                                                             entire 30MB
#
# --source-name/--source-version replace the scan path in the document header, so no local
# filesystem layout reaches a published file.

set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

cd "$PROJECT_ROOT"

require_tool syft "brew install syft"
require_tool jq

mkdir -p "$DIST"

SPDX="$DIST/cameodb.spdx.json"
CDX="$DIST/cameodb.cyclonedx.json"

section "SBOM $VERSION"
info "syft $(syft version 2>/dev/null | grep -i '^Version:' | awk '{print $2}')"

syft_scan() {
    syft dir:"$PROJECT_ROOT" \
        --override-default-catalogers rust-cargo-lock-cataloger \
        --select-catalogers -file \
        --source-name cameodb \
        --source-version "$VERSION" \
        -o "$1=$2" 2> >(grep -v 'no file catalogers selected' >&2 || true)
}

step "SPDX"
syft_scan spdx-json "$SPDX"
step "CycloneDX"
syft_scan cyclonedx-json "$CDX"

# The scan is cheap to get wrong and expensive to notice: a cataloger name change or a stray
# default would silently reintroduce the directory scan. So assert the properties that made
# the old document wrong, rather than trusting the flags held.
spdx_pkgs="$(jq '.packages | length' "$SPDX")"
cdx_comps="$(jq '.components | length' "$CDX")"
lock_deps="$(grep -c '^name = ' Cargo.lock)"

[ "$spdx_pkgs" -ge $((lock_deps / 2)) ] \
    || die "SPDX lists $spdx_pkgs packages against $lock_deps in Cargo.lock — the cataloger selection is wrong"
ok "SPDX $spdx_pkgs packages, CycloneDX $cdx_comps components (Cargo.lock: $lock_deps)"

[ "$(jq -r '.name' "$SPDX")" = "cameodb" ] \
    || die "SPDX name is '$(jq -r '.name' "$SPDX")', expected 'cameodb' — a scan path leaked into the document"
ok "no scan path in the document header"

for f in "$SPDX" "$CDX"; do
    if grep -q "$HOME" "$f"; then
        die "$(basename "$f") contains an absolute path under $HOME"
    fi
    ok "$(basename "$f")  $(du -h "$f" | cut -f1)  no local paths"
done

[ "$(jq '(.files // []) | length' "$SPDX")" -le 1 ] \
    || warn "SPDX carries per-file entries — the file cataloger came back, expect a large document"
