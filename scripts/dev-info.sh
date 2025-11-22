#!/bin/bash

# CameoDB Development Information Script
# Quick overview of available scripts and project status

echo "🗂️  CameoDB Development Scripts"
echo "==============================="
echo ""

echo "📁 Available Scripts:"
echo ""

echo "🛠️  Setup Scripts:"
echo "   ./scripts/setup/install-deps.sh    - Install development dependencies"
echo "   ./scripts/setup/init-cluster.sh    - Initialize development cluster"
echo ""

echo "🧪 Testing Scripts:"
echo "   ./scripts/testing/test-api.sh      - API endpoint testing"
echo "   ./scripts/testing/load-test.sh     - Load testing (default: 10 users, 50 requests each)"
echo ""

echo "📊 Data Scripts:"
echo "   ./scripts/data/sample-data.sh      - Generate sample data (default: 100 documents)"
echo ""

echo "🔧 Operations Scripts:"
echo "   ./scripts/ops/health-check.sh      - Comprehensive health check"
echo ""

echo "📋 Quick Start:"
echo "   1. Setup:      ./scripts/setup/install-deps.sh"
echo "   2. Build:      cargo build --release"
echo "   3. Start:      cargo run --release --bin server"
echo "   4. Test:       ./scripts/testing/test-api.sh"
echo "   5. Load data:  ./scripts/data/sample-data.sh"
echo ""

# Show current project status if possible
if command -v cargo &> /dev/null; then
    echo "🏗️  Project Status:"
    
    # Check if project builds
    if cargo check --workspace &> /dev/null; then
        echo "   ✅ Project builds successfully"
    else
        echo "   ❌ Project has build errors (run: cargo check --workspace)"
    fi
    
    # Check if server binary exists
    if [ -f "target/release/server" ]; then
        echo "   ✅ Release binary available"
    else
        echo "   ⚠️  Release binary not found (run: cargo build --release)"
    fi
    
    # Check if server is running
    if curl -s "http://localhost:9480/_cluster/health" &> /dev/null; then
        echo "   ✅ Server is running on port 9480"
    else
        echo "   ℹ️  Server not running (start with: cargo run --release --bin server)"
    fi
    
    echo ""
fi

echo "📚 Documentation:"
echo "   ./scripts/README.md         - Detailed script documentation"
echo "   ./docs/                     - Project documentation"
echo "   ./ARCHITECTURE.md           - System architecture"
echo ""

echo "💡 Need help? Check the README files or run scripts with --help (if supported)"
