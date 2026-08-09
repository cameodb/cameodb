#!/usr/bin/env bash
# Build a fully static Linux binary (musl): no libc, no libgcc, no interpreter.
#
#   ./scripts/build/build-musl.sh [profile] [arch]
#
#   profile   release (default) | dev | release-docker | any custom profile
#   arch      x86_64 (default) | aarch64 | both
#
# Two ways to get there, and they do not produce the same artifact:
#
#   container  (default when Docker is available) — native rustc + lld inside a Linux image.
#              Produces static-pie with RELRO and BIND_NOW. This is what the Dockerfile does,
#              so it is also what the published image contains.
#   zigbuild   (fallback, and forced with BUILD_WITH=zig) — cross-compiles from macOS with no
#              Linux toolchain. Convenient, but zig's linker does not advertise `-static-pie`,
#              so rustc silently falls back to `-static`: the binary is still fully static but
#              loses ASLR and BIND_NOW. Fine for local testing, not for a release.
#
# Either way the result is checked, not assumed — see scripts/validate/artifact.sh.
#
# TLS is rustls with the ring provider (no C toolchain, no vendored OpenSSL); outbound HTTPS
# verifies against the system trust store via rustls-platform-verifier.

set -euo pipefail

PROFILE="${1:-release}"
ARCH="${2:-x86_64}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

case "$ARCH" in
    x86_64)  TARGETS=("x86_64-unknown-linux-musl") ;;
    aarch64) TARGETS=("aarch64-unknown-linux-musl") ;;
    both)    TARGETS=("x86_64-unknown-linux-musl" "aarch64-unknown-linux-musl") ;;
    *) echo "unknown arch '$ARCH' (expected x86_64, aarch64 or both)" >&2; exit 2 ;;
esac

# Cargo's built-in "dev" profile is the one exception to "output dir == profile name": it
# builds into a directory literally called "debug" for historical reasons. Every other
# profile (release, release-docker, custom ones) uses its own name.
OUT_DIR="$PROFILE"
[ "$PROFILE" = "dev" ] && OUT_DIR="debug"

METHOD="${BUILD_WITH:-auto}"
if [ "$METHOD" = "auto" ]; then
    if docker info > /dev/null 2>&1; then METHOD="container"; else METHOD="zig"; fi
fi

build_in_container() {
    local target="$1"
    # The builder image has to match the *target* architecture, not the host's. `ring`
    # compiles per-architecture assembly with the image's `musl-gcc`, which is native-only:
    # an aarch64 builder asked for an x86_64 target fails with "unrecognized command-line
    # option '-m64'". On a host of the other architecture Docker emulates, which works and
    # is slow — that is the price of a cross-arch build here.
    local platform image
    case "$target" in
        x86_64-*)  platform="linux/amd64"; image="cameo-builder-base-amd64:latest" ;;
        aarch64-*) platform="linux/arm64"; image="cameo-builder-base:latest" ;;
        *) echo "no builder image for $target" >&2; exit 2 ;;
    esac
    if ! docker image inspect "$image" > /dev/null 2>&1; then
        echo "Building $image (one time; emulated if it is not this host's architecture)"
        docker build --platform "$platform" -f docker/Dockerfile.builder -t "$image" docker/
    fi
    # A per-target volume: two architectures sharing one CARGO_TARGET_DIR thrash each other's
    # build scripts, and the emulated one is far too slow to rebuild casually.
    local vol="cameodb-musl-target-${target%%-*}"
    docker volume create "$vol" > /dev/null
    docker run --rm --platform "$platform" \
        -v "$ROOT":/src \
        -v "${CARGO_HOME:-$HOME/.cargo}/registry":/usr/local/cargo/registry \
        -v "$vol":/target \
        -e CARGO_TARGET_DIR=/target \
        -w /src "$image" \
        cargo build --profile "$PROFILE" --target "$target" --bin cameodb --no-default-features
    # The target dir is a volume so the container's root-owned artifacts never land in the
    # working tree; copy the one file out.
    mkdir -p "target/$target/$OUT_DIR"
    docker run --rm --platform "$platform" \
        -v "$vol":/target -v "$ROOT/target/$target/$OUT_DIR":/out \
        "$image" cp "/target/$target/$OUT_DIR/cameodb" /out/cameodb
}

build_with_zig() {
    local target="$1"
    command -v zig > /dev/null || { echo "zig not installed (brew install zig)" >&2; exit 2; }
    command -v cargo-zigbuild > /dev/null || { echo "cargo install cargo-zigbuild" >&2; exit 2; }
    # Zig's archiver, so jemalloc's static archive is a GNU ar archive that ld.lld can read.
    # macOS's own ar writes a BSD archive; ld.lld cannot consume it and silently produces an
    # empty libjemalloc.a, which surfaces much later as undefined `mallocx`/`sdallocx`.
    AR="zig ar" RANLIB="zig ranlib" \
        cargo zigbuild --profile "$PROFILE" --target "$target" --bin cameodb --no-default-features
}

for target in "${TARGETS[@]}"; do
    echo "Building ${target}, profile ${PROFILE}, via ${METHOD}"
    case "$METHOD" in
        container) build_in_container "$target" ;;
        zig)       build_with_zig "$target" ;;
        *) echo "unknown BUILD_WITH '$METHOD' (expected container or zig)" >&2; exit 2 ;;
    esac
    echo "  target/$target/$OUT_DIR/cameodb"
done

echo
for target in "${TARGETS[@]}"; do
    "$ROOT/scripts/validate/artifact.sh" "target/$target/$OUT_DIR/cameodb"
done
