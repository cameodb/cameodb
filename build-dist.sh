#!/bin/bash
# High-speed cross-compilation build script with persistent caching
# Uses named volumes to cache dependencies and compiled artifacts across runs

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
BUILDER_IMAGE="cameo-builder-base"
TARGET="x86_64-unknown-linux-musl"
VERSION="0.2.2"

echo -e "${BLUE}🚀 Starting CameoDB distribution build...${NC}"

# 1. Ensure the builder image is up to date (uses builder.Dockerfile, NOT the production Dockerfile)
echo -e "${YELLOW}📦 Building/Updating builder image...${NC}"
docker buildx build --platform linux/amd64 \
  --builder cameo-builder \
  --load \
  --secret id=zscaler,src=/tmp/buildkit-ca/zscaler.crt \
  -t "${BUILDER_IMAGE}" -f builder.Dockerfile .

# 2. Create named volumes if they don't exist
echo -e "${YELLOW}📋 Ensuring cache volumes exist...${NC}"
docker volume inspect cameo-cargo-cache >/dev/null 2>&1 || docker volume create cameo-cargo-cache
docker volume inspect cameo-target-cache >/dev/null 2>&1 || docker volume create cameo-target-cache

# 3. Verify Zscaler certificate exists
if [[ ! -f "/tmp/buildkit-ca/zscaler.crt" ]]; then
    echo -e "${RED}❌ Error: Zscaler certificate not found at /tmp/buildkit-ca/zscaler.crt${NC}"
    echo -e "${RED}   Please ensure the corporate CA certificate is available.${NC}"
    exit 1
fi

# 4. Run the high-speed cross-compilation
echo -e "${YELLOW}🔨 Building CameoDB with persistent caches...${NC}"
docker run --rm --platform linux/amd64 \
  -v "$PWD":/workspace \
  -v cameo-cargo-cache:/usr/local/cargo/registry \
  -v cameo-target-cache:/workspace/target \
  -v /tmp/buildkit-ca/zscaler.crt:/usr/local/share/ca-certificates/zscaler.crt:ro \
  -e RUST_LOG=info \
  -e SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt \
  -e CARGO_HTTP_CAINFO=/etc/ssl/certs/ca-certificates.crt \
  -e CC_x86_64_unknown_linux_musl=musl-gcc \
  -e AR_x86_64_unknown_linux_musl=ar \
  -e RANLIB_x86_64_unknown_linux_musl=ranlib \
  "${BUILDER_IMAGE}" bash -c "
    set -euo pipefail
    
    # Update certificates to trust corporate CA
    echo '🔐 Updating CA certificates...'
    cat /usr/local/share/ca-certificates/zscaler.crt >> /etc/ssl/certs/ca-certificates.crt
    update-ca-certificates
    
    # Build the binary with automatic stripping
    # Use CARGO_PROFILE_RELEASE_LTO=false to avoid OOM during linking in Docker
    echo '🏗️  Building CameoDB binary...'
    CARGO_PROFILE_RELEASE_LTO=false \
    CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16 \
    cargo build --release --target ${TARGET} \
      --no-default-features \
      --features client/native-tls-vendored
    
    # Verify binary was built and stripped
    echo '📊 Binary info:'
    ls -lh target/${TARGET}/release/cameodb
    file target/${TARGET}/release/cameodb
    echo 'ℹ️  Note: Binary is larger due to LTO disabled for Docker memory constraints'
    
    # Generate Debian Package
    echo '📦 Generating DEB package...'
    cargo deb --target ${TARGET} --no-build -p server \
      --output target/${TARGET}/release/cameodb_${VERSION}_amd64.deb
    
    # Generate RPM Package  
    echo '📦 Generating RPM package...'
    cargo generate-rpm -p crates/server --target ${TARGET} --auto-req disabled \
      -o target/${TARGET}/release/cameodb-${VERSION}-1.x86_64.rpm \
      --set-metadata 'package.name=\"cameodb\"'
    
    echo '✅ Build completed successfully!'
  "

# 5. Display results
echo -e "${GREEN}✅ Build completed!${NC}"
echo -e "${BLUE}📁 Generated artifacts:${NC}"
echo -e "   • Binary: target/${TARGET}/release/cameodb"
echo -e "   • DEB:    target/${TARGET}/release/cameodb_${VERSION}_amd64.deb"
echo -e "   • RPM:    target/${TARGET}/release/cameodb-${VERSION}-1.x86_64.rpm"

# 6. Show cache statistics
echo -e "${BLUE}💾 Cache statistics:${NC}"
echo -e "   • Cargo registry cache: cameo-cargo-cache"
echo -e "   • Target build cache:   cameo-target-cache"

# 7. Show file sizes
echo -e "${BLUE}📏 Artifact sizes:${NC}"
ls -lh "target/${TARGET}/release/cameodb"* 2>/dev/null || true

echo -e "${GREEN}🎉 Ready for distribution!${NC}"
