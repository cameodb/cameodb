#!/usr/bin/env bash
# Shared state for the release pipeline.
#
# Everything the stages disagree about is decided here exactly once: the version, where
# artifacts are staged, what a Linux architecture is called in three different naming
# conventions, and which external tools have to exist. A stage that needed its own copy of
# any of those is how `build-packages.sh` came to ship 0.2.3-labelled packages out of a 0.3.0
# tree, so there are no per-stage constants below this file.

set -uo pipefail

_c_green=$'\033[0;32m'
_c_red=$'\033[0;31m'
_c_yellow=$'\033[0;33m'
_c_blue=$'\033[0;34m'
_c_bold=$'\033[1m'
_c_off=$'\033[0m'

section() { printf '\n%s== %s ==%s\n' "$_c_bold" "$1" "$_c_off"; }
info()    { printf '  %s\n' "$1"; }
step()    { printf '%s→ %s%s\n' "$_c_blue" "$1" "$_c_off"; }
ok()      { printf '  %sok%s   %s\n' "$_c_green" "$_c_off" "$1"; }
warn()    { printf '  %swarn%s %s\n' "$_c_yellow" "$_c_off" "$1"; }
die()     { printf '\n%serror%s %s\n' "$_c_red" "$_c_off" "$1" >&2; exit 1; }

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

# The version is read from the tree, never passed in. A release where the argument and the
# manifests disagree is a release where the binary reports one number and its filename
# claims another, and nothing downstream would catch it.
release_version() {
    local versions
    versions="$(grep -h '^version = ' "$PROJECT_ROOT"/crates/*/Cargo.toml | sort -u)"
    if [ "$(printf '%s\n' "$versions" | wc -l | tr -d ' ')" -ne 1 ]; then
        die "crates disagree on the version — fix the manifests first:
$(grep -n '^version = ' "$PROJECT_ROOT"/crates/*/Cargo.toml)"
    fi
    printf '%s' "$versions" | sed 's/^version = "//; s/"$//'
}

VERSION="${CAMEODB_VERSION:-$(release_version)}"
[ -n "$VERSION" ] || die "could not determine the version from crates/*/Cargo.toml"

# Staging root. `dist/` is already gitignored, and staging is what makes the pipeline
# restartable: every stage after `build` reads from here, not from target/, so re-signing or
# re-hashing does not depend on a build tree that may have been cleaned in between.
DIST="$PROJECT_ROOT/dist/$VERSION"

# Where the published tree lives. Overridable so a dry run can be pointed at a scratch copy.
WEB_ROOT="${CAMEODB_WEB:-/Users/gc/code/cameodb-web/public/downloads}"

# A directory that exists for nothing else, at mode 700. The key used to sit in
# /usr/local/share/ca-certificates/, which is where build-packages.sh and docker-push.sh read the
# corporate CA from: a directory something will eventually treat as certs-to-distribute.
COSIGN_KEY="${COSIGN_KEY:-$HOME/.cosign/cosign.key}"
# The public half is whatever downloaders will actually fetch, so verification uses that copy
# rather than a local one — it checks the key users get, not the key we still have.
COSIGN_PUB="${COSIGN_PUB:-$WEB_ROOT/cosign.pub}"

# Linux architectures to build. x86_64 alone by default: that is what
# downloads/linux/ currently publishes, and adding a second arch changes public filenames.
LINUX_ARCHS="${LINUX_ARCHS:-x86_64}"

# The primary arch keeps the bare `cameodb` name so published URLs stay valid; any other arch
# is suffixed. Three conventions for one thing — rust triple, deb, rpm — hence three lookups.
PRIMARY_ARCH="x86_64"

triple_for()  { case "$1" in x86_64) echo x86_64-unknown-linux-musl ;; aarch64) echo aarch64-unknown-linux-musl ;; *) die "unknown arch '$1' (x86_64 or aarch64)" ;; esac; }
deb_arch_for() { case "$1" in x86_64) echo amd64 ;; aarch64) echo arm64 ;; esac; }
rpm_arch_for() { case "$1" in x86_64) echo x86_64 ;; aarch64) echo aarch64 ;; esac; }

# Bare binary name inside dist/<ver>/linux/ for a given arch.
linux_bin_name() {
    [ "$1" = "$PRIMARY_ARCH" ] && { echo cameodb; return; }
    echo "cameodb-$1"
}

require_tool() {
    command -v "$1" > /dev/null 2>&1 || die "$1 not installed${2:+ — $2}"
}

# Each stage script runs standalone as well as under release.sh, which invites passing
# release.sh's flags to a stage — `build.sh --stage sbom`. Ignoring the flag runs a full
# rebuild and looks precisely like `--stage` being broken, so every stage refuses an argument
# it does not implement rather than proceeding with a different meaning than the one typed.
reject_unknown_arg() {
    die "unknown argument '$1' to $(basename "$0") — try '$(basename "$0") --help'.
  Stage selection belongs to the driver:
  scripts/release/release.sh --stage build|sbom|sign|publish"
}

# --help prints the script's own header comment. Derived rather than written twice: a second
# copy of the usage is a copy that goes stale, and a hardcoded line range (which is how this
# started) silently truncates the help the first time the header grows a line.
usage() {
    awk '
        NR == 1 && /^#!/ { next }                       # skip the shebang
        /^#/             { sub(/^#[[:space:]]?/, ""); print; next }
        { exit }                                        # stop at the first non-comment line
    ' "$1"
    exit 0
}

# Every file in the staging tree that is a release artifact, i.e. the things that get signed
# and hashed. Signatures and checksums are themselves in the tree, so they are excluded by
# extension rather than by trying to remember what was added when.
staged_artifacts() {
    [ -d "$DIST" ] || return 0
    find "$DIST" -type f \
        ! -name '*.bundle' \
        ! -name '*.sha256' \
        ! -name 'SHA256SUMS' \
        ! -name 'MANIFEST.txt' \
        ! -name '*.version' \
        ! -name '.unhardened' \
        ! -name '.DS_Store' \
        | sort
}

# A staging tree built with --allow-unhardened must not be signed or published. The signing
# and checksum stages cannot detect it themselves — an unhardened binary signs and hashes just
# as cleanly — so the build stage leaves a marker and they refuse on it.
refuse_if_unhardened() {
    [ -f "$DIST/.unhardened" ] || return 0
    die "$DIST was built with --allow-unhardened and is not publishable:
$(sed 's/^/  /' "$DIST/.unhardened")
  Rebuild through the container path: scripts/release/build.sh"
}

# Path relative to the staging root — what the manifest and the checksum files should say,
# never an absolute path. The published .sha256 files currently carry the signing machine's
# directory layout, which leaks a local path and makes `shasum -c` fail for everyone else.
rel_to_dist() { printf '%s' "${1#"$DIST"/}"; }
