#!/bin/bash
# Generate SBOM for CameoDB using syft 1.42.3
# Outputs both SPDX JSON and CycloneDX JSON formats for public distribution
#
# Usage:
#   ./scripts/security/generate-sbom.sh                   # Generate SBOMs from Docker image (latest)
#   ./scripts/security/generate-sbom.sh 0.2.2             # Generate SBOMs for specific version
#   ./scripts/security/generate-sbom.sh --native          # Generate SBOMs from native binary (macOS/Linux)
#   ./scripts/security/generate-sbom.sh --source          # Generate SBOMs from source code
#   ./scripts/security/generate-sbom.sh --output ./sboms  # Output to specific directory
#
# Can be run from any directory (auto-detects project root)
# syft 1.42.3 compatible

set -euo pipefail

# Determine script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

cd "$PROJECT_ROOT"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m'

# Default values
VERSION="latest"
MODE="docker"  # docker, native, or source
IMAGE="goranc/cameodb"
OUTPUT_DIR="${SCRIPT_DIR}"  # Default to scripts/security/

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --native)
            MODE="native"
            shift
            ;;
        --source)
            MODE="source"
            shift
            ;;
        --docker)
            MODE="docker"
            shift
            ;;
        --output)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        -*)
            echo -e "${RED}Unknown option: $1${NC}"
            echo "Usage: $0 [version|--native|--source|--docker] [--output <dir>]"
            exit 1
            ;;
        *)
            VERSION="$1"
            shift
            ;;
    esac
done

# Ensure output directory exists
mkdir -p "$OUTPUT_DIR"

SPDX_FILE="${OUTPUT_DIR}/cameodb.spdx.json"
CYCLONEDX_FILE="${OUTPUT_DIR}/cameodb.cyclonedx.json"

# syft 1.42.3 compatible - uses: syft <source> -o <format>=<output>
check_syft() {
    if ! command -v syft &> /dev/null; then
        echo -e "${RED}syft not found. Please install syft 1.42.3+${NC}"
        echo "  brew install syft  (macOS)"
        echo "  https://github.com/anchore/syft/releases (Linux)"
        exit 1
    fi
    local syft_version
    syft_version=$(syft version 2>/dev/null | head -1 | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' || echo "unknown")
    echo -e "${BLUE}Using syft version: ${syft_version}${NC}"
}

generate_docker_sbom() {
    local full_image="${IMAGE}:${VERSION}"
    echo -e "${BLUE}Generating SBOMs for Docker image: ${full_image}${NC}"
    
    # Pull image if not present locally
    if ! docker image inspect "$full_image" &> /dev/null; then
        echo -e "${YELLOW}Image not found locally, pulling from DockerHub...${NC}"
        docker pull "$full_image"
    fi
    
    # Generate SPDX format
    echo -e "${BLUE}  → Generating SPDX...${NC}"
    syft "$full_image" -o "spdx-json=$SPDX_FILE"
    
    # Generate CycloneDX format
    echo -e "${BLUE}  → Generating CycloneDX...${NC}"
    syft "$full_image" -o "cyclonedx-json=$CYCLONEDX_FILE"
}

generate_native_sbom() {
    local binary_path=""
    local target_dir=""
    # Detect platform
    case "$(uname -sm)" in
        "Darwin arm64"|"Darwin ARM64")
            target_dir="${PROJECT_ROOT}/target/aarch64-apple-darwin/release"
            ;;
        "Darwin x86_64"|"Darwin amd64")
            target_dir="${PROJECT_ROOT}/target/x86_64-apple-darwin/release"
            ;;
        "Linux aarch64"|"Linux arm64")
            target_dir="${PROJECT_ROOT}/target/aarch64-unknown-linux-musl/release-docker"
            ;;
        "Linux x86_64"|"Linux amd64")
            target_dir="${PROJECT_ROOT}/target/x86_64-unknown-linux-musl/release-docker"
            ;;
        *)
            target_dir="${PROJECT_ROOT}/target/release"
            ;;
    esac
    # Find the binary
    if [[ -f "${target_dir}/cameodb" ]]; then
        binary_path="${target_dir}/cameodb"
    elif [[ -f "${PROJECT_ROOT}/target/release/cameodb" ]]; then
        binary_path="${PROJECT_ROOT}/target/release/cameodb"
    else
        echo -e "${RED}Native binary not found. Expected:${NC}"
        echo "  ${target_dir}/cameodb"
        echo ""
        echo -e "${YELLOW}Build first with:${NC}"
        echo "  cargo build --release  # macOS native"
        echo "  cargo zigbuild --profile release-docker --target x86_64-unknown-linux-musl  # Linux cross-compile"
        exit 1
    fi
    echo -e "${BLUE}Generating SBOMs for native binary: ${binary_path}${NC}"
    
    # Generate SPDX format
    echo -e "${BLUE}  → Generating SPDX...${NC}"
    syft "$binary_path" -o "spdx-json=$SPDX_FILE"
    
    # Generate CycloneDX format
    echo -e "${BLUE}  → Generating CycloneDX...${NC}"
    syft "$binary_path" -o "cyclonedx-json=$CYCLONEDX_FILE"
}

generate_source_sbom() {
    echo -e "${BLUE}Generating SBOMs from source code (Cargo.lock)...${NC}"
    
    # Generate SPDX format
    echo -e "${BLUE}  → Generating SPDX...${NC}"
    syft dir:"$PROJECT_ROOT" -o "spdx-json=$SPDX_FILE"
    
    # Generate CycloneDX format
    echo -e "${BLUE}  → Generating CycloneDX...${NC}"
    syft dir:"$PROJECT_ROOT" -o "cyclonedx-json=$CYCLONEDX_FILE"
}

# Main execution
check_syft

case "$MODE" in
    docker)
        generate_docker_sbom
        ;;
    native)
        generate_native_sbom
        ;;
    source)
        generate_source_sbom
        ;;
    *)
        echo -e "${RED}Unknown mode: $MODE${NC}"
        exit 1
        ;;
esac

# Verify outputs
echo ""
echo -e "${GREEN}SBOMs generated successfully!${NC}"

if [[ -f "$SPDX_FILE" ]]; then
    size=$(du -h "$SPDX_FILE" | cut -f1)
    pkgs=$(jq '.packages | length' "$SPDX_FILE" 2>/dev/null || echo "N/A")
    echo -e "  SPDX:      ${size}  (~${pkgs} packages)"
fi

if [[ -f "$CYCLONEDX_FILE" ]]; then
    size=$(du -h "$CYCLONEDX_FILE" | cut -f1)
    comps=$(jq '.components | length' "$CYCLONEDX_FILE" 2>/dev/null || echo "N/A")
    echo -e "  CycloneDX: ${size}  (~${comps} components)"
fi

echo ""
echo -e "${BLUE}Files written to:${NC} $(cd "$OUTPUT_DIR" && pwd)"
echo "  ${SPDX_FILE}"
echo "  ${CYCLONEDX_FILE}"

echo ""
echo -e "${BLUE}Publish to make them available at:${NC}"
echo "  https://dl.cameodb.com/cameodb.spdx.json"
echo "  https://dl.cameodb.com/cameodb.cyclonedx.json"

echo ""
echo -e "${YELLOW}Upload commands:${NC}"
echo "  scp ${SPDX_FILE} ${CYCLONEDX_FILE} user@dl.cameodb.com:/var/www/dl.cameodb.com/"
