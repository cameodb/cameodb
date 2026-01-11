#!/bin/bash

# Simple script to test CameoDB HTTP API endpoints
echo "Testing CameoDB HTTP API..."

# Start CameoDB in background from workspace root
cargo run --release --bin cameodb &
SERVER_PID=$!

# Wait for CameoDB to start
sleep 5

echo "Testing endpoints..."

# Test health endpoint
echo "1. Health Check:"
curl -s http://localhost:9480/_cluster/health | jq .

# Test search endpoint (empty index)
echo -e "\n2. Search Test:"
curl -s -X POST \
  -H "Content-Type: application/json" \
  -d '{"query": "test", "limit": 10}' \
  http://localhost:9480/api/testindex/search | jq .

# Test write endpoint
echo -e "\n3. Write Test:"
curl -s -X PUT \
  -H "Content-Type: application/json" \
  -d '{"id": "doc1", "doc": {"title": "Test Document", "content": "This is a test"}}' \
  http://localhost:9480/api/testindex/document | jq .

# Test streaming endpoint (will timeout after 2 seconds)
echo -e "\n4. Stream Test (first 200 chars):"
timeout 2s curl -s -X POST \
  -H "Content-Type: application/json" \
  -d '{"query": "test"}' \
  http://localhost:9480/api/testindex/search/stream | head -c 200

echo -e "\n\nAPI tests completed!"

# Clean up
kill $SERVER_PID 2>/dev/null
