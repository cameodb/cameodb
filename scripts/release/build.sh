#!/usr/bin/env bash
# Stage 1 — build every artifact this machine can build, and stage it under dist/<version>/.
#
#   scripts/release/build.sh [--allow-unhardened]
#
# What gets built here:
#   mac/cameodb                     native host build (Apple Silicon)
#   linux/cameodb                   static musl, via scripts/build/build-musl.sh
#   linux/cameodb_<v>_<arch>.deb    packaged from the binary above — not rebuilt
#   linux/cameodb-<v>-1.<arch>.rpm  packaged from the binary above — not rebuilt
#
# Windows is not built here. Build it on the Windows machine and drop the .exe into
# dist/<version>/windows/ before running the sign stage; see scripts/release/README.md.
#
# The DEB and RPM deliberately wrap the *same* binary that ships standalone, via
# `--no-build`. build-packages.sh instead rebuilds under the `release-docker` profile (thin LTO,
# 4 codegen units), which meant the binary inside the packages was a different, less
# optimized artifact than the one next to it in the same directory — invisible to anyone
# comparing them, since both report the same --version.

set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

ALLOW_UNHARDENED=0
[ "${1:-}" = "--allow-unhardened" ] && ALLOW_UNHARDENED=1

cd "$PROJECT_ROOT"

require_tool cargo
require_tool cargo-deb "cargo install cargo-deb"
require_tool cargo-generate-rpm "cargo install cargo-generate-rpm"

section "release $VERSION"
info "staging into $DIST"

# A release must come out of a tree whose state is recorded. Anything else is unreproducible
# by the time someone asks which commit a published binary came from.
if [ -n "$(git status --porcelain)" ]; then
    warn "working tree is dirty — the commit recorded in MANIFEST.txt will not describe these artifacts exactly"
    git status --short | sed 's/^/       /'
fi
info "commit $(git rev-parse --short HEAD)"

mkdir -p "$DIST/mac" "$DIST/linux" "$DIST/windows"

# ---------------------------------------------------------------- macOS (host native)

section "macOS"
step "cargo build --release"
cargo build --release

MAC_BIN="target/release/cameodb"
[ -f "$MAC_BIN" ] || die "expected $MAC_BIN"

# arm64-only is a deliberate choice (see scripts/release/README.md), so assert it rather than
# publishing whatever the host happened to produce. A published Mach-O of the wrong
# architecture fails at exec time with "bad CPU type", which no checksum or signature catches.
mac_arch="$(lipo -archs "$MAC_BIN" 2>/dev/null || echo unknown)"
[ "$mac_arch" = "arm64" ] || die "mac binary is '$mac_arch', expected arm64 — downloads/mac/ is published as Apple Silicon only"
ok "arm64 Mach-O"

"$MAC_BIN" --version > /dev/null || die "mac binary does not run"
ok "runs: $("$MAC_BIN" --version | head -1)"

cp "$MAC_BIN" "$DIST/mac/cameodb"

# ---------------------------------------------------------------- Linux (static musl)

for arch in $LINUX_ARCHS; do
    triple="$(triple_for "$arch")"
    bin_name="$(linux_bin_name "$arch")"

    section "Linux $arch"

    # build-musl.sh runs scripts/validate/artifact.sh at the end and exits non-zero on a
    # failed hardening check. The zigbuild path always fails the PIE check — zig's linker
    # refuses -static-pie and rustc silently downgrades to -static — so a release build has
    # to go through Docker. --allow-unhardened is for rehearsing the pipeline, not shipping.
    if [ "$ALLOW_UNHARDENED" -eq 1 ]; then
        warn "hardening failures tolerated (--allow-unhardened) — do not publish this build"
        # Leave a marker in the staging tree. Otherwise the later stages cannot tell a
        # rehearsal build from a real one: a non-PIE binary signs and hashes exactly as
        # cleanly as a hardened one, and the difference is invisible in the published tree.
        printf 'Staged with --allow-unhardened on %s.\nAt least one scripts/validate/artifact.sh check failed. Not publishable.\n' \
            "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" > "$DIST/.unhardened"
        step "scripts/build/build-musl.sh release $arch"
        scripts/build/build-musl.sh release "$arch" || warn "artifact validation reported failures"
    else
        [ "${BUILD_WITH:-auto}" = "zig" ] && die "BUILD_WITH=zig cannot produce a static-pie binary; unset it to use the container path, or pass --allow-unhardened for a test run"
        docker info > /dev/null 2>&1 || die "Docker is not running — the container path is required for a release build (or pass --allow-unhardened)"
        step "scripts/build/build-musl.sh release $arch"
        scripts/build/build-musl.sh release "$arch"
    fi

    linux_bin="target/$triple/release/cameodb"
    [ -f "$linux_bin" ] || die "expected $linux_bin"
    cp "$linux_bin" "$DIST/linux/$bin_name"
    ok "$bin_name  $(du -h "$linux_bin" | cut -f1)"

    # ---- packages, from that exact binary

    deb_name="cameodb_${VERSION}_$(deb_arch_for "$arch").deb"
    rpm_name="cameodb-${VERSION}-1.$(rpm_arch_for "$arch").rpm"

    # --no-strip because macOS strip cannot process a Linux ELF; the release profile already
    # sets strip = true, so the binary arrives stripped and cargo-deb's warning is cosmetic.
    step "cargo deb → $deb_name"
    cargo deb --no-build --no-strip -p server \
        --target "$triple" --profile release \
        --output "$DIST/linux/$deb_name" > /dev/null
    ok "$deb_name  $(du -h "$DIST/linux/$deb_name" | cut -f1)"

    # --auto-req disabled: the binary is fully static, so there are no shared-library
    # requirements to discover, and letting rpm guess adds dependencies that do not exist.
    step "cargo generate-rpm → $rpm_name"
    cargo generate-rpm -p crates/server \
        --target "$triple" --profile release --auto-req disabled \
        -o "$DIST/linux/$rpm_name" \
        --set-metadata "package.name=\"cameodb\"" > /dev/null
    ok "$rpm_name  $(du -h "$DIST/linux/$rpm_name" | cut -f1)"
done

# ---------------------------------------------------------------- Windows (elsewhere)

section "Windows"
WIN_EXE="$DIST/windows/cameodb.exe"
if [ -f "$WIN_EXE" ]; then
    # Checking only for existence would let a truncated scp, a stray shortcut, or an .exe from
    # the wrong target reach the sign stage — and a signature over a broken file verifies
    # perfectly, so nothing downstream would catch it. This host cannot execute a PE binary, so
    # inspect the header instead.
    win_desc="$(file -b "$WIN_EXE")"
    case "$win_desc" in
        *"PE32+"*"x86-64"*) ok "PE32+ x86-64 executable  $(du -h "$WIN_EXE" | cut -f1)" ;;
        *"PE32"*)           die "windows/cameodb.exe is 32-bit ($win_desc) — expected an x86_64 build" ;;
        *)                  die "windows/cameodb.exe is not a Windows executable: $win_desc" ;;
    esac

    # Shipping a stale .exe is the mistake a two-machine release invites, and nothing on this
    # host can catch it: the binary cannot be executed here, and the version is not recoverable
    # from the file — `CARGO_PKG_VERSION` does not survive as a greppable ASCII string in any of
    # our binaries, Windows or otherwise. So the check has to happen where the binary runs.
    # Optional sidecar, produced on the Windows machine:
    #     cameodb.exe --version > cameodb.exe.version
    if [ -f "$WIN_EXE.version" ]; then
        win_reported="$(tr -d '\r' < "$WIN_EXE.version" | head -1)"
        case "$win_reported" in
            *"$VERSION"*) ok "reports '$win_reported' (recorded on the Windows machine)" ;;
            *)            die "windows/cameodb.exe reports '$win_reported', but this release is $VERSION" ;;
        esac
    else
        warn "version of windows/cameodb.exe is unverified — nothing on macOS can determine it"
        info "to check it, run this on the Windows machine and copy the file over:"
        info "  cameodb.exe --version > cameodb.exe.version"
    fi
    info "will be signed and hashed with the rest"
else
    warn "no dist/$VERSION/windows/cameodb.exe"
    info "build it on the Windows machine, then copy it to exactly that path:"
    info "  cargo build --release                      # on Windows → target\\release\\cameodb.exe"
    info "  scp …:target/release/cameodb.exe $DIST/windows/cameodb.exe"
    info "then re-run this stage to validate it, or go straight to --stage sign"
fi

section "staged"
staged_artifacts | while read -r f; do info "$(rel_to_dist "$f")"; done
