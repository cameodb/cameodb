#!/bin/bash
# Build and push multi-platform Docker images to DockerHub
#
# Usage:
#   ./scripts/build/docker-push.sh                   # Build + push latest tag
#   ./scripts/build/docker-push.sh 0.2.2             # Build + push with version tag
#   ./scripts/build/docker-push.sh 0.2.2 --no-push   # Build both platforms, no push
#
# Can be run from any directory (auto-detects project root)
#
# Prerequisites:
#   1. Docker Desktop with buildx enabled
#   2. Logged in to DockerHub: docker login
#   3. BuildKit builder created (script will create if missing)

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

REPO="goranc/cameodb"
BUILDER_NAME="cameo-builder"
PLATFORMS="linux/amd64,linux/arm64"
# Corporate CA for TLS-intercepting proxies, passed to the build as the `corporate-ca`
# secret. Override with CAMEODB_CA_CERT=/path/to/ca.crt; skipped when absent or empty.
CORPORATE_CA_CERT="${CAMEODB_CA_CERT:-/var/tmp/buildkit-ca/corporate-ca.crt}"

# Parse arguments
VERSION="${1:-}"
NO_PUSH=false
for arg in "$@"; do
    if [ "$arg" = "--no-push" ]; then
        NO_PUSH=true
    fi
done

# Build tag list
TAGS=("-t" "${REPO}:latest")
if [[ -n "$VERSION" && "$VERSION" != "--no-push" ]]; then
    TAGS+=("-t" "${REPO}:${VERSION}")
fi

echo -e "${BLUE}CameoDB Docker Multi-Platform Build${NC}"
echo -e "  Repository:  ${REPO}"
echo -e "  Platforms:   ${PLATFORMS}"
echo -e "  Tags:        ${TAGS[*]}"
echo -e "  Push:        $(if $NO_PUSH; then echo 'no'; else echo 'yes'; fi)"
echo ""

# 1. Ensure buildx builder exists
if ! docker buildx inspect "${BUILDER_NAME}" >/dev/null 2>&1; then
    echo -e "${YELLOW}Creating buildx builder: ${BUILDER_NAME}${NC}"
    docker buildx create --name "${BUILDER_NAME}" --use \
        --driver docker-container \
        --driver-opt image=moby/buildkit:master \
        >/dev/null 2>&1 || true
fi

# 2. Prepare build arguments
BUILD_ARGS=()
if [[ -s "${CORPORATE_CA_CERT}" ]]; then
    BUILD_ARGS+=(--secret "id=corporate-ca,src=${CORPORATE_CA_CERT}")
    echo -e "${YELLOW}Using CA certificate: ${CORPORATE_CA_CERT}${NC}"
fi

if [[ "$NO_PUSH" == true ]]; then
    # Build every platform the push would publish. This used to build linux/amd64 alone, which
    # made the rehearsal worthless for the arch it skipped: an aarch64-only break compiled for
    # the first time during the real `--push`, after the amd64 manifest was already uploaded.
    # On an arm64 host it was also the emulated arch that got checked and the native one that
    # did not.
    echo -e "${YELLOW}Building ${PLATFORMS} (no push)...${NC}"

    # A manifest list only fits in the local image store when the containerd snapshotter is
    # enabled; the classic store rejects it with "docker exporter does not currently support
    # exporting manifest lists". Where it fits, one build both verifies and loads. Where it
    # does not, verify with cacheonly and load the host arch in a second pass.
    if docker info --format '{{.DriverStatus}}' 2>/dev/null | grep -q 'io.containerd.snapshotter'; then
        OUTPUT_ARGS=(--load)
    else
        OUTPUT_ARGS=(--output type=cacheonly)
    fi

    docker buildx build \
        --builder "${BUILDER_NAME}" \
        --platform "${PLATFORMS}" \
        "${OUTPUT_ARGS[@]}" \
        --sbom=true \
        ${BUILD_ARGS[@]:+"${BUILD_ARGS[@]}"} \
        "${TAGS[@]}" \
        -f Dockerfile \
        "$PROJECT_ROOT"

    if [[ "${OUTPUT_ARGS[0]}" == "--output" ]]; then
        # cacheonly leaves nothing runnable behind, so re-export the host arch for the
        # `docker run` below. Every layer is already in the build cache, so this is an export,
        # not a second compile.
        HOST_PLATFORM="linux/$(docker version --format '{{.Server.Arch}}')"
        echo ""
        echo -e "${YELLOW}Loading ${HOST_PLATFORM} into the local image store...${NC}"
        docker buildx build \
            --builder "${BUILDER_NAME}" \
            --platform "${HOST_PLATFORM}" \
            --load \
            ${BUILD_ARGS[@]:+"${BUILD_ARGS[@]}"} \
            "${TAGS[@]}" \
            -f Dockerfile \
            "$PROJECT_ROOT"
    fi
    
    echo ""
    echo -e "${GREEN}Build complete!${NC}"
    echo -e "  Verified:    ${PLATFORMS}"
    echo -e "  Local image: ${REPO}:latest"
    echo ""
    echo -e "${BLUE}Test with:${NC}"
    echo "  docker run --rm ${REPO}:latest --version"
else
    # Build and push multi-platform
    echo -e "${YELLOW}Building and pushing multi-platform image...${NC}"
    docker buildx build \
        --builder "${BUILDER_NAME}" \
        --platform "${PLATFORMS}" \
        --push \
        --sbom=true \
        ${BUILD_ARGS[@]:+"${BUILD_ARGS[@]}"} \
        "${TAGS[@]}" \
        -f Dockerfile \
        "$PROJECT_ROOT"
    
    echo ""
    echo -e "${GREEN}Build and push complete!${NC}"
    echo -e "  Image: ${REPO}:latest"
    [[ -n "$VERSION" ]] && echo -e "  Tag:   ${REPO}:${VERSION}"
    echo ""
    echo -e "${BLUE}Verify with:${NC}"
    echo "  docker buildx imagetools inspect ${REPO}:latest"
fi
