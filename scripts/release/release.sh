#!/usr/bin/env bash
# Drive the release pipeline.
#
#   scripts/release/release.sh                       # build → sbom  (stops before signing)
#   scripts/release/release.sh --stage build
#   scripts/release/release.sh --stage sign,publish
#   scripts/release/release.sh --stage all --commit  # everything, including the copy to the web project
#
# Stages, in order:
#   build     compile and stage mac + linux binaries and the DEB/RPM packages
#   sbom      generate the SPDX and CycloneDX documents
#   sign      cosign sign-blob every staged file, and verify each signature
#   publish   checksum, write the manifest, copy into the web project
#
# The default is `build,sbom` rather than `all` on purpose: the Windows executable is built on
# a different machine and has to be dropped into dist/<version>/windows/ before signing, so
# every release has a manual gap in the middle. Signing everything before that file arrives
# would publish a release with Windows missing.
#
# Flags are passed through: --commit reaches publish, --allow-unhardened reaches build.

set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

HERE="$(dirname "${BASH_SOURCE[0]}")"

STAGES="build,sbom"
COMMIT=""
ALLOW_UNHARDENED=""

while [ $# -gt 0 ]; do
    case "$1" in
        --stage)   STAGES="$2"; shift 2 ;;
        --stage=*) STAGES="${1#--stage=}"; shift ;;
        --commit)  COMMIT="--commit"; shift ;;
        --allow-unhardened) ALLOW_UNHARDENED="--allow-unhardened"; shift ;;
        -h|--help) usage "${BASH_SOURCE[0]}" ;;
        *) die "unknown argument '$1' (see --help)" ;;
    esac
done

[ "$STAGES" = "all" ] && STAGES="build,sbom,sign,publish"

# Validate up front. Discovering a typo'd stage name after a six-minute build is a poor trade.
for s in $(printf '%s' "$STAGES" | tr ',' ' '); do
    case "$s" in
        build|sbom|sign|publish) ;;
        *) die "unknown stage '$s' (build, sbom, sign, publish, or all)" ;;
    esac
done

# Run in pipeline order whatever order they were typed in, and drop repeats. Each stage consumes
# what the previous one produced, so `--stage sign,sbom` would sign the tree and *then* write the
# SBOMs into it, leaving the one file that describes the release as the one file with no
# signature — publish refuses that, but only after the signing pass has already been spent.
_ordered=""
for _canon in build sbom sign publish; do
    case ",$STAGES," in
        *",$_canon,"*) _ordered="${_ordered:+$_ordered,}$_canon" ;;
    esac
done
STAGES="$_ordered"

printf '%s%s CameoDB %s — stages: %s %s\n' "$_c_bold" '::' "$VERSION" "$STAGES" "$_c_off"

for s in $(printf '%s' "$STAGES" | tr ',' ' '); do
    case "$s" in
        build)   "$HERE/build.sh" ${ALLOW_UNHARDENED:+$ALLOW_UNHARDENED} ;;
        sbom)    "$HERE/sbom.sh" ;;
        sign)    "$HERE/sign.sh" ;;
        publish) "$HERE/publish.sh" ${COMMIT:+$COMMIT} ;;
    esac
done

section "pipeline complete"
info "staged at $DIST"

# What is still pending, in pipeline order: everything after the last stage that just ran. A
# stage earlier than that one is assumed to have run in a previous invocation — the tree is the
# state, and re-running build to satisfy the hint would discard it. Deriving this rather than
# printing a fixed line is the point: `--stage build` used to be told to go to `sign,publish`,
# which skips sbom entirely and publishes a release with no SBOM in it.
_todo=""
for _canon in build sbom sign publish; do
    case ",$STAGES," in
        *",$_canon,"*) _todo="" ;;
        *)             _todo="${_todo:+$_todo,}$_canon" ;;
    esac
done

if [ -n "$_todo" ]; then
    # publish alone is worth naming directly: it is a dry run until --commit, and the hint is the
    # only place that says so.
    if [ "$_todo" = "publish" ]; then
        info "next: scripts/release/publish.sh          # dry run, then --commit"
    elif case ",$_todo," in *,sign,*) true ;; *) false ;; esac && [ ! -f "$DIST/windows/cameodb.exe" ]; then
        info "next: drop the Windows .exe into $DIST/windows/, then --stage $_todo"
    else
        info "next: scripts/release/release.sh --stage $_todo"
    fi
fi
