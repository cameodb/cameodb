#!/bin/bash
# Build and push multi-platform Docker images to DockerHub
#
# Usage:
#   ./scripts/build/docker-push.sh                   # Build + push latest tag
#   ./scripts/build/docker-push.sh 0.2.2             # Build + push with version tag
#   ./scripts/build/docker-push.sh 0.2.2 --no-push   # Build only (no push)
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
    # Build only, load into local Docker
    echo -e "${YELLOW}Building for local platform only (no push)...${NC}"
    docker buildx build \
        --builder "${BUILDER_NAME}" \
        --platform linux/amd64 \
        --load \
        --sbom=true \
        ${BUILD_ARGS[@]:+"${BUILD_ARGS[@]}"} \
        "${TAGS[@]}" \
        -f Dockerfile \
        "$PROJECT_ROOT"
    
    echo ""
    echo -e "${GREEN}Build complete!${NC}"
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
