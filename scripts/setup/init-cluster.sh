#!/bin/bash

# CameoDB Cluster Initialization Script
# Initializes a development cluster with sample shards and data

set -e

echo "🚀 Initializing CameoDB development cluster..."

# Configuration
DEFAULT_PORT=9480
PORT=${1:-$DEFAULT_PORT}
DATA_DIR="data/cameodb"

# Check if cameodb is already running
if curl -s "http://localhost:$PORT/_cluster/health" &> /dev/null; then
    echo "⚠️  CameoDB appears to be running on port $PORT"
    echo "   Stop it first or use a different port: $0 <port>"
    exit 1
fi

# Ensure data directory exists
mkdir -p "$DATA_DIR"

# Build the project if needed
if [ ! -f "target/release/cameodb" ] || [ "crates/server/src/main.rs" -nt "target/release/cameodb" ]; then
    echo "🏗️  Building CameoDB..."
    cargo build --release --bin cameodb
fi

# Start cameodb in background
echo "🌟 Starting CameoDB on port $PORT..."
cargo run --release --bin cameodb &
SERVER_PID=$!

# Function to cleanup on exit
cleanup() {
    echo ""
    echo "🛑 Stopping CameoDB..."
    kill $SERVER_PID 2>/dev/null || true
    wait $SERVER_PID 2>/dev/null || true
}
trap cleanup EXIT

# Wait for cameodb to start
echo "⏳ Waiting for CameoDB to start..."
for i in {1..30}; do
    if curl -s "http://localhost:$PORT/_cluster/health" &> /dev/null; then
        break
    fi
    sleep 1
    if [ $i -eq 30 ]; then
        echo "❌ CameoDB failed to start within 30 seconds"
        exit 1
    fi
done

echo "✅ CameoDB is running!"

# Check cluster health
echo "🔍 Checking cluster health..."
HEALTH_RESPONSE=$(curl -s "http://localhost:$PORT/_cluster/health")
echo "   $HEALTH_RESPONSE"

# Add some sample data
echo "📝 Adding sample documents..."

# Sample documents for testing
SAMPLES=(
    '{"id": "doc1", "doc": {"title": "CameoDB Introduction", "content": "CameoDB is a distributed hybrid-search database built in Rust", "category": "documentation", "tags": ["rust", "database", "search"]}}'
    '{"id": "doc2", "doc": {"title": "Getting Started Guide", "content": "Learn how to set up and use CameoDB for your applications", "category": "tutorial", "tags": ["guide", "setup", "tutorial"]}}'
    '{"id": "doc3", "doc": {"title": "API Reference", "content": "Complete API documentation for CameoDB HTTP endpoints", "category": "reference", "tags": ["api", "http", "reference"]}}'
    '{"id": "doc4", "doc": {"title": "Performance Benchmarks", "content": "CameoDB performance metrics and optimization techniques", "category": "performance", "tags": ["benchmarks", "optimization", "performance"]}}'
    '{"id": "doc5", "doc": {"title": "Architecture Overview", "content": "Understanding CameoDB distributed architecture and design principles", "category": "architecture", "tags": ["distributed", "architecture", "design"]}}'
)

for sample in "${SAMPLES[@]}"; do
    RESPONSE=$(curl -s -X PUT \
        -H "Content-Type: application/json" \
        -d "$sample" \
        "http://localhost:$PORT/api/development/document")
    
    DOC_ID=$(echo "$sample" | jq -r '.id')
    if echo "$RESPONSE" | jq -e '.result == "created"' &> /dev/null; then
        echo "✅ Added document: $DOC_ID"
    else
        echo "⚠️  Failed to add document: $DOC_ID"
        echo "   Response: $RESPONSE"
    fi
done

# Test search functionality
echo ""
echo "🔍 Testing search functionality..."

# Test basic search
SEARCH_RESPONSE=$(curl -s -X POST \
    -H "Content-Type: application/json" \
    -d '{"query": "CameoDB", "limit": 5}' \
    "http://localhost:$PORT/api/development/search")

RESULT_COUNT=$(echo "$SEARCH_RESPONSE" | jq length 2>/dev/null || echo "0")
echo "   Found $RESULT_COUNT documents matching 'CameoDB'"

# Test streaming search
echo "🌊 Testing streaming search..."
STREAM_RESPONSE=$(timeout 3s curl -s -X POST \
    -H "Content-Type: application/json" \
    -d '{"query": "database"}' \
    "http://localhost:$PORT/api/development/stream" | head -c 200)

if [ -n "$STREAM_RESPONSE" ]; then
    echo "✅ Streaming search is working"
else
    echo "⚠️  Streaming search test inconclusive"
fi

echo ""
echo "🎉 CameoDB cluster initialization complete!"
echo ""
echo "Cluster Status:"
echo "  • CameoDB running on: http://localhost:$PORT"
echo "  • Health endpoint: http://localhost:$PORT/_cluster/health"
echo "  • Sample documents: 5 documents added to 'development' index"
echo ""
echo "Next Steps:"
echo "  • Health check: curl http://localhost:$PORT/_cluster/health"
echo "  • Search: curl -X POST -H 'Content-Type: application/json' -d '{\"query\": \"rust\", \"limit\": 3}' http://localhost:$PORT/api/development/search"
echo ""
echo "Press Ctrl+C to stop CameoDB."
echo ""

# Keep server running until interrupted
wait $SERVER_PID
