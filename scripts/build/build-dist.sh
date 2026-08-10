#!/bin/bash
# Cross-compilation build script with persistent Docker caching
# Builds binary + DEB/RPM packages for the specified architecture
#
# Usage:
#   ./scripts/build/build-dist.sh              # Build amd64 (default)
#   ./scripts/build/build-dist.sh arm64        # Build arm64
#   ./scripts/build/build-dist.sh amd64 arm64  # Build both architectures
#
# Can be run from any directory (auto-detects project root)

set -euo pipefail

# Determine script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

cd "$PROJECT_ROOT"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

BUILDER_IMAGE="cameo-builder-base"
# Read from the manifests, never hardcoded. This was literally `VERSION="0.2.3"`, so a 0.3.0
# tree produced cameodb_0.2.3_amd64.deb containing a binary that answers 0.3.0 to --version —
# and that mismatched pair is what reached the downloads directory.
VERSION="$(grep -h '^version = ' "$PROJECT_ROOT"/crates/*/Cargo.toml | sort -u | sed 's/^version = "//; s/"$//')"
if [ "$(printf '%s\n' "$VERSION" | wc -l | tr -d ' ')" -ne 1 ] || [ -z "$VERSION" ]; then
    echo -e "${RED}crates disagree on the version:${NC}" >&2
    grep -n '^version = ' "$PROJECT_ROOT"/crates/*/Cargo.toml >&2
    exit 1
fi
CORPORATE_CA_CERT="/usr/local/share/ca-certificates/corporate-ca.crt"

# Parse architectures from args (default: amd64)
ARCHS=("${@:-amd64}")

resolve_target() {
    case "$1" in
        amd64|x86_64)
            echo "x86_64-unknown-linux-musl"
            ;;
        arm64|aarch64)
            echo "aarch64-unknown-linux-musl"
            ;;
        *)
            echo -e "${RED}Unsupported architecture: $1${NC}" >&2
            exit 1
            ;;
    esac
}

deb_arch() {
    case "$1" in
        amd64|x86_64) echo "amd64" ;;
        arm64|aarch64) echo "arm64" ;;
    esac
}

rpm_arch() {
    case "$1" in
        amd64|x86_64) echo "x86_64" ;;
        arm64|aarch64) echo "aarch64" ;;
    esac
}

echo -e "${BLUE}Starting CameoDB distribution build for: ${ARCHS[*]}${NC}"

BUILDER_ARGS=()
if [[ -f "${CORPORATE_CA_CERT}" ]]; then
    BUILDER_ARGS+=(--secret "id=corporate-ca,src=${CORPORATE_CA_CERT}")
fi

# Build each architecture
for ARCH in "${ARCHS[@]}"; do
    TARGET=$(resolve_target "$ARCH")
    DEB_ARCH=$(deb_arch "$ARCH")
    RPM_ARCH=$(rpm_arch "$ARCH")
    PROFILE="release-docker"
    OUTPUT_DIR="target/${TARGET}/${PROFILE}"
    ARCH_BUILDER_IMAGE="${BUILDER_IMAGE}-${ARCH}"

    echo -e "${YELLOW}Building builder image for ${ARCH}...${NC}"
    docker buildx build \
      --builder cameo-builder \
      --load \
      --provenance=false \
      --platform linux/${ARCH} \
      ${BUILDER_ARGS[@]:+"${BUILDER_ARGS[@]}"} \
      -t "${ARCH_BUILDER_IMAGE}" -f docker/Dockerfile.builder "$PROJECT_ROOT"

    # Create per-arch named volumes
    docker volume inspect cameo-cargo-cache-${ARCH} >/dev/null 2>&1 || docker volume create cameo-cargo-cache-${ARCH}
    docker volume inspect cameo-target-cache-${ARCH} >/dev/null 2>&1 || docker volume create cameo-target-cache-${ARCH}

    echo -e "${YELLOW}Building CameoDB for ${ARCH} (${TARGET})...${NC}"

    TARGET_ENV=$(echo "${TARGET}" | tr '-' '_')

    docker run --rm --platform linux/${ARCH} \
      -v "$PROJECT_ROOT":/workspace \
      -v cameo-cargo-cache-${ARCH}:/usr/local/cargo/registry \
      -v cameo-target-cache-${ARCH}:/workspace/target \
      -e RUST_LOG=info \
      -e SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt \
      -e CARGO_HTTP_CAINFO=/etc/ssl/certs/ca-certificates.crt \
      -e CC_${TARGET_ENV}=musl-gcc \
      -e AR_${TARGET_ENV}=ar \
      -e RANLIB_${TARGET_ENV}=ranlib \
      "${ARCH_BUILDER_IMAGE}" bash -c "
        set -euo pipefail

        echo 'Building CameoDB binary (profile: ${PROFILE})...'
        cargo build --profile ${PROFILE} --target ${TARGET} \
          --no-default-features

        echo 'Binary info:'
        ls -lh ${OUTPUT_DIR}/cameodb
        file ${OUTPUT_DIR}/cameodb

        echo 'Generating DEB package...'
        cargo deb --target ${TARGET} --no-build -p server --profile ${PROFILE} \
          --output ${OUTPUT_DIR}/cameodb_${VERSION}_${DEB_ARCH}.deb

        echo 'Generating RPM package...'
        cargo generate-rpm -p crates/server --target ${TARGET} --profile ${PROFILE} --auto-req disabled \
          -o ${OUTPUT_DIR}/cameodb-${VERSION}-1.${RPM_ARCH}.rpm \
          --set-metadata 'package.name=\"cameodb\"'

        echo 'Build completed for ${ARCH}!'
      "

    echo -e "${GREEN}Artifacts for ${ARCH}:${NC}"
    echo -e "   Binary: ${OUTPUT_DIR}/cameodb"
    echo -e "   DEB:    ${OUTPUT_DIR}/cameodb_${VERSION}_${DEB_ARCH}.deb"
    echo -e "   RPM:    ${OUTPUT_DIR}/cameodb-${VERSION}-1.${RPM_ARCH}.rpm"
done

echo ""
echo -e "${BLUE}Artifact sizes:${NC}"
for ARCH in "${ARCHS[@]}"; do
    TARGET=$(resolve_target "$ARCH")
    ls -lh "target/${TARGET}/release-docker/cameodb"* 2>/dev/null || true
done

echo -e "${GREEN}Ready for distribution!${NC}"
