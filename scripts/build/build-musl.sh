#!/bin/bash
# Build script for x86_64-unknown-linux-musl target using zigbuild
# Uses native-tls-vendored with Zig's C compiler for maximum HTTPS compatibility

set -e

TARGET="x86_64-unknown-linux-musl"
PROFILE="${1:-release}"

# Use Zig's archiver/ranlib so that jemalloc's static archive is in GNU ar
# format that musl's ld.lld can read. macOS system ar creates a BSD archive
# that ld.lld fails to consume, resulting in empty libjemalloc.a and undefined
# jemalloc symbols (sdallocx, mallocx, rallocx, mallctl) at link time.
export AR="zig ar"
export RANLIB="zig ranlib"

echo "Building for $TARGET with profile $PROFILE using native-tls-vendored..."

# Use --profile rather than interpolating a `--$PROFILE` flag: cargo has no literal
# `--debug` flag (debug/dev is the default with no flag at all), so a naive
# `--$PROFILE` breaks for anything other than "release". `--profile <name>` works
# uniformly for "release", "dev", and custom profiles (e.g. "release-docker").
cargo zigbuild \
    --profile "$PROFILE" \
    --target "$TARGET" \
    --no-default-features \
    --features client/native-tls-vendored

# Cargo's built-in "dev" profile is the one exception to "output dir == profile
# name" — it builds into a directory literally called "debug" for historical
# reasons. Every other profile (release, release-docker, custom ones) uses its
# own name as the output directory.
OUT_DIR="$PROFILE"
if [ "$PROFILE" = "dev" ]; then
    OUT_DIR="debug"
fi

echo "Build complete! Binary location:"
echo "target/$TARGET/$OUT_DIR/cameodb"
