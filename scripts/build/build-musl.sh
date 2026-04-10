#!/bin/bash
# Build script for x86_64-unknown-linux-musl target using zigbuild
# Uses native-tls-vendored with Zig's C compiler for maximum HTTPS compatibility

set -e

TARGET="x86_64-unknown-linux-musl"
PROFILE="${1:-release}"

echo "Building for $TARGET with profile $PROFILE using native-tls-vendored..."

cargo zigbuild \
    --$PROFILE \
    --target $TARGET \
    --no-default-features \
    --features client/native-tls-vendored

echo "Build complete! Binary location:"
echo "target/$TARGET/$PROFILE/cameodb"
