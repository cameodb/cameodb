#!/bin/bash

# CameoDB Development Dependencies Setup
# This script installs and verifies all dependencies needed for CameoDB development

set -e  # Exit on any error

echo "🔧 Setting up CameoDB development environment..."

# Check if running on supported OS
case "$(uname -s)" in
    Darwin*)    OS_TYPE="macOS" ;;
    Linux*)     OS_TYPE="Linux" ;;
    *)          echo "❌ Unsupported OS. This script supports macOS and Linux only."; exit 1 ;;
esac

echo "📍 Detected OS: $OS_TYPE"

# Check Rust installation
if ! command -v rustc &> /dev/null; then
    echo "❌ Rust is not installed. Please install Rust first:"
    echo "   curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh"
    exit 1
fi

echo "✅ Rust $(rustc --version | cut -d' ' -f2) detected"

# Check required tools
REQUIRED_TOOLS=("curl" "jq")
MISSING_TOOLS=()

for tool in "${REQUIRED_TOOLS[@]}"; do
    if ! command -v "$tool" &> /dev/null; then
        MISSING_TOOLS+=("$tool")
    else
        echo "✅ $tool is available"
    fi
done

# Install missing tools based on OS
if [ ${#MISSING_TOOLS[@]} -gt 0 ]; then
    echo "📦 Installing missing tools: ${MISSING_TOOLS[*]}"
    
    case $OS_TYPE in
        "macOS")
            if ! command -v brew &> /dev/null; then
                echo "❌ Homebrew is required but not installed. Please install it first:"
                echo "   /bin/bash -c \"\$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)\""
                exit 1
            fi
            for tool in "${MISSING_TOOLS[@]}"; do
                brew install "$tool"
            done
            ;;
        "Linux")
            # Try different package managers
            if command -v apt-get &> /dev/null; then
                sudo apt-get update
                for tool in "${MISSING_TOOLS[@]}"; do
                    sudo apt-get install -y "$tool"
                done
            elif command -v dnf &> /dev/null; then
                sudo dnf install -y "${MISSING_TOOLS[@]}"
            elif command -v yum &> /dev/null; then
                for tool in "${MISSING_TOOLS[@]}"; do
                    sudo yum install -y "$tool"
                done
            elif command -v pacman &> /dev/null; then
                for tool in "${MISSING_TOOLS[@]}"; do
                    sudo pacman -S --noconfirm "$tool"
                done
            else
                echo "❌ No supported package manager found. Please install manually: ${MISSING_TOOLS[*]}"
                exit 1
            fi
            ;;
    esac
fi

# Verify Cargo is working
echo "🔍 Verifying Rust toolchain..."
if ! cargo --version &> /dev/null; then
    echo "❌ Cargo is not working properly"
    exit 1
fi

echo "✅ Cargo $(cargo --version | cut -d' ' -f2) is working"

# Verify Python version (3.9+)
echo "🔍 Checking Python (3.9+)..."
if command -v python3 &> /dev/null; then
    PY_CHECK=$(python3 - <<'PY'
import sys
from sys import version_info
if version_info < (3, 9):
    print(f"Python {version_info.major}.{version_info.minor}.{version_info.micro}")
    raise SystemExit(1)
print(f"Python {version_info.major}.{version_info.minor}.{version_info.micro}")
PY
    )
    if [ $? -eq 0 ]; then
        echo "✅ ${PY_CHECK} detected"
    else
        echo "❌ Python 3.9+ required. Detected ${PY_CHECK}."
        exit 1
    fi
else
    echo "❌ python3 is not installed. Please install Python 3.9 or newer."
    exit 1
fi

# Verify Docker installation
echo "🔍 Checking Docker..."
if command -v docker &> /dev/null; then
    echo "✅ $(docker --version | head -n1)"
else
    echo "❌ Docker is not installed or not on PATH. Please install Docker Desktop/Engine."
    exit 1
fi

# Check if we can build the project
echo "🏗️  Testing project build..."
if cargo check --workspace &> /dev/null; then
    echo "✅ Project builds successfully"
else
    echo "❌ Project build failed. Run 'cargo check --workspace' for details"
    exit 1
fi

# Create data directory if it doesn't exist
if [ ! -d "data" ]; then
    mkdir -p data
    echo "✅ Created data directory"
fi

echo ""
echo "🎉 CameoDB development environment setup complete!"
echo ""
echo "Next steps:"
echo "  1. Build the project: cargo build --release"
echo "  2. Run tests: cargo test --workspace"  
echo "  3. Start the server: cargo run --release --bin cameodb"
echo "  4. Test the API: ./scripts/testing/test-api.sh"
echo ""
echo "For more information, see README.md and docs/ directory."
