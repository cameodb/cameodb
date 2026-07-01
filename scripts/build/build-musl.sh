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

cargo zigbuild \
    --$PROFILE \
    --target $TARGET \
    --no-default-features \
    --features client/native-tls-vendored

echo "Build complete! Binary location:"
echo "target/$TARGET/$PROFILE/cameodb"
