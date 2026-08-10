#!/bin/bash

# CameoDB development environment check.
#
# Verifies everything needed to build, test and validate CameoDB, installs what it can, and
# reports what it cannot. Required tools fail the run; optional ones are reported and the run
# continues — you can develop, test and run a node without Docker or a cross-compiler.
#
# `--no-build` skips the compile at the end, which is the slow part.

set -uo pipefail

MIN_RUST_MINOR=85   # edition 2024
FAILURES=0
WARNINGS=0

ok()    { echo "✅ $*"; }
warn()  { echo "⚠️  $*"; WARNINGS=$((WARNINGS + 1)); }
fail()  { echo "❌ $*"; FAILURES=$((FAILURES + 1)); }
note()  { echo "   $*"; }

SKIP_BUILD=0
[ "${1:-}" = "--no-build" ] && SKIP_BUILD=1

echo "🔧 Checking the CameoDB development environment..."
echo ""

case "$(uname -s)" in
    Darwin*)    OS_TYPE="macOS" ;;
    Linux*)     OS_TYPE="Linux" ;;
    *)          echo "❌ Unsupported OS. This script supports macOS and Linux only."; exit 1 ;;
esac
echo "📍 $OS_TYPE ($(uname -m))"
echo ""

# --- Compiler and linker ------------------------------------------------------------------
# Checked before Rust: without a linker, cargo fails at the last step of a long build with an
# error that names `cc` rather than the missing toolchain.
echo "🔨 Toolchain"
if [ "$OS_TYPE" = "macOS" ]; then
    if xcode-select -p > /dev/null 2>&1; then
        ok "Xcode Command Line Tools at $(xcode-select -p)"
    else
        fail "Xcode Command Line Tools are missing — nothing will link"
        note "Install with: xcode-select --install"
    fi
elif ! command -v cc > /dev/null 2>&1; then
    fail "no C compiler on PATH — nothing will link"
    note "Debian/Ubuntu: sudo apt-get install build-essential"
    note "Fedora/RHEL:   sudo dnf groupinstall 'Development Tools'"
else
    ok "$(cc --version 2>/dev/null | head -n1)"
fi

# --- Rust ---------------------------------------------------------------------------------
# The version matters: this workspace is edition 2024. A toolchain older than 1.85 fails
# while parsing Cargo.toml, with a message that never mentions the version.
if ! command -v rustc > /dev/null 2>&1; then
    fail "Rust is not installed"
    note "Install with: curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh"
else
    RUST_VERSION=$(rustc --version | cut -d' ' -f2)
    RUST_MINOR=$(echo "$RUST_VERSION" | cut -d. -f2)
    RUST_MAJOR=$(echo "$RUST_VERSION" | cut -d. -f1)
    if [ "$RUST_MAJOR" -gt 1 ] || [ "$RUST_MINOR" -ge "$MIN_RUST_MINOR" ]; then
        ok "Rust $RUST_VERSION (edition 2024 needs 1.$MIN_RUST_MINOR+)"
    else
        fail "Rust $RUST_VERSION is too old — this workspace is edition 2024, which needs 1.$MIN_RUST_MINOR+"
        note "Update with: rustup update stable"
    fi
    command -v cargo > /dev/null 2>&1 && ok "cargo $(cargo --version | cut -d' ' -f2)" \
        || fail "cargo is not on PATH"
fi
echo ""

# --- Required command-line tools ----------------------------------------------------------
echo "🧰 Required tools"
MISSING_TOOLS=()
for tool in curl jq; do
    if command -v "$tool" > /dev/null 2>&1; then
        ok "$tool"
    else
        MISSING_TOOLS+=("$tool")
    fi
done

if [ ${#MISSING_TOOLS[@]} -gt 0 ]; then
    echo "📦 Installing: ${MISSING_TOOLS[*]}"
    case $OS_TYPE in
        "macOS")
            if ! command -v brew > /dev/null 2>&1; then
                fail "Homebrew is required to install ${MISSING_TOOLS[*]}"
                note '/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"'
            else
                for tool in "${MISSING_TOOLS[@]}"; do
                    brew install "$tool" && ok "$tool installed" || fail "could not install $tool"
                done
            fi
            ;;
        "Linux")
            if command -v apt-get > /dev/null 2>&1; then
                sudo apt-get update && sudo apt-get install -y "${MISSING_TOOLS[@]}"
            elif command -v dnf > /dev/null 2>&1; then
                sudo dnf install -y "${MISSING_TOOLS[@]}"
            elif command -v yum > /dev/null 2>&1; then
                sudo yum install -y "${MISSING_TOOLS[@]}"
            elif command -v pacman > /dev/null 2>&1; then
                sudo pacman -S --noconfirm "${MISSING_TOOLS[@]}"
            else
                fail "no supported package manager; install manually: ${MISSING_TOOLS[*]}"
            fi
            ;;
    esac
fi
echo ""

# --- Validation gate prerequisites --------------------------------------------------------
# These are the reason this section exists. `validate/deps.sh` SKIPs both when they are
# absent and still reports success, so a machine without them passes a gate that never ran
# the supply-chain half.
echo "🛡️  Validation gate"
for tool in cargo-audit cargo-deny; do
    subcommand=${tool#cargo-}
    if cargo "$subcommand" --version > /dev/null 2>&1; then
        ok "$tool"
    else
        warn "$tool is missing — scripts/validate/deps.sh will SKIP it and still pass"
        note "Install with: cargo install $tool"
    fi
done

if command -v openssl > /dev/null 2>&1; then
    ok "openssl ($(openssl version 2>/dev/null | cut -d' ' -f1-2))"
else
    warn "openssl is missing — scripts/validate/tls.sh cannot generate a test certificate"
fi
echo ""

# --- Optional: cross-compilation and containers --------------------------------------------
echo "🐳 Optional"
if command -v docker > /dev/null 2>&1; then
    ok "$(docker --version | head -n1)"
    docker info > /dev/null 2>&1 || warn "the docker daemon is not running"
else
    warn "Docker is not installed — needed only for musl release builds and the compose cluster"
    note "Native development, tests and validation do not require it"
fi

for tool in zig cargo-zigbuild; do
    if command -v "$tool" > /dev/null 2>&1; then
        ok "$tool"
    else
        warn "$tool is missing — the no-Docker musl cross-compilation path is unavailable"
        [ "$tool" = "zig" ] && note "Install with: brew install zig" \
                            || note "Install with: cargo install cargo-zigbuild"
    fi
done

if command -v python3 > /dev/null 2>&1; then
    if python3 -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 9) else 1)'; then
        ok "$(python3 --version)"
    else
        warn "$(python3 --version) is older than 3.9 — the examples/ ingest scripts need 3.9+"
    fi
else
    warn "python3 is missing — needed only for the examples/ ingest scripts"
fi
echo ""

# --- Build --------------------------------------------------------------------------------
if [ "$SKIP_BUILD" -eq 0 ] && [ "$FAILURES" -eq 0 ]; then
    echo "🏗️  Building (cargo check --workspace)..."
    if cargo check --workspace > /dev/null 2>&1; then
        ok "the workspace compiles"
    else
        fail "the workspace does not compile — run 'cargo check --workspace' for details"
    fi
    echo ""
elif [ "$FAILURES" -ne 0 ]; then
    echo "⏭️  Skipping the build: fix the failures above first."
    echo ""
fi

[ -d data ] || { mkdir -p data && ok "created ./data"; }

# --- Summary ------------------------------------------------------------------------------
echo "----------------------------------------"
if [ "$FAILURES" -ne 0 ]; then
    echo "❌ $FAILURES required item(s) missing, $WARNINGS optional."
    echo "   CameoDB will not build until the failures above are resolved."
    exit 1
fi

if [ "$WARNINGS" -ne 0 ]; then
    echo "✅ Ready to build and test — with $WARNINGS optional item(s) missing (listed above)."
else
    echo "🎉 Everything is present."
fi

cat <<'EOF'

Next steps:
  1. Build:    cargo build --release
  2. Test:     cargo test --workspace
  3. Validate: ./scripts/validate/all.sh
  4. Run:      cargo run --release --bin cameodb
  5. Try it:   ./scripts/testing/test-api.sh

Setup guide: docs/DEVELOPMENT.md
EOF
