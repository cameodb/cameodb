#!/bin/bash

# CameoDB Health Check Script
# Comprehensive health monitoring for CameoDB instances

set -e

# Configuration
DEFAULT_PORT=9480
PORT=${1:-$DEFAULT_PORT}
TIMEOUT=${2:-10}

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    local status=$1
    local message=$2
    case $status in
        "OK")     echo -e "${GREEN}✅ $message${NC}" ;;
        "WARN")   echo -e "${YELLOW}⚠️  $message${NC}" ;;
        "ERROR")  echo -e "${RED}❌ $message${NC}" ;;
        "INFO")   echo -e "${BLUE}ℹ️  $message${NC}" ;;
    esac
}

echo "🏥 CameoDB Health Check"
echo "======================="
echo ""

# Check 1: CameoDB Connectivity
print_status "INFO" "Checking CameoDB connectivity on port $PORT..."

if ! curl -s --max-time $TIMEOUT "http://localhost:$PORT/_cluster/health" &> /dev/null; then
    print_status "ERROR" "Cannot connect to CameoDB on port $PORT"
    echo ""
    echo "Troubleshooting:"
    echo "  1. Check if CameoDB is running: ps aux | grep cameodb"
    echo "  2. Check if port is correct: netstat -an | grep $PORT"
    echo "  3. Start CameoDB: cargo run --release --bin cameodb"
    exit 1
fi

print_status "OK" "CameoDB is accessible"

# Check 2: Health Endpoint
print_status "INFO" "Fetching cluster health..."

HEALTH_RESPONSE=$(curl -s --max-time $TIMEOUT "http://localhost:$PORT/_cluster/health")
if [ $? -ne 0 ]; then
    print_status "ERROR" "Failed to get health response"
    exit 1
fi

# Parse health response
STATUS=$(echo "$HEALTH_RESPONSE" | jq -r '.status // "unknown"')
NODE_ID=$(echo "$HEALTH_RESPONSE" | jq -r '.node_id // "unknown"') 
ACTIVE_SHARDS=$(echo "$HEALTH_RESPONSE" | jq -r '.active_shards // 0')

if [ "$STATUS" = "green" ]; then
    print_status "OK" "Cluster status: $STATUS"
else
    print_status "WARN" "Cluster status: $STATUS (expected: green)"
fi

print_status "INFO" "Node ID: ${NODE_ID:0:8}..."
print_status "INFO" "Active shards: $ACTIVE_SHARDS"

# Check 3: API Endpoints
print_status "INFO" "Testing API endpoints..."

# Test search endpoint (should handle empty index gracefully)
SEARCH_TEST=$(curl -s --max-time $TIMEOUT -X POST \
    -H "Content-Type: application/json" \
    -d '{"query": "test", "limit": 1}' \
    "http://localhost:$PORT/api/healthcheck/search")

if echo "$SEARCH_TEST" | jq -e 'type == "array"' &> /dev/null; then
    print_status "OK" "Search endpoint responding correctly"
else
    print_status "WARN" "Search endpoint response format unexpected"
    echo "   Response: $SEARCH_TEST"
fi

# Test write endpoint
WRITE_TEST=$(curl -s --max-time $TIMEOUT -X PUT \
    -H "Content-Type: application/json" \
    -d '{"id": "health-check-'.$(date +%s)'", "doc": {"title": "Health Check", "content": "Automated health check document"}}' \
    "http://localhost:$PORT/api/healthcheck/document")

if echo "$WRITE_TEST" | jq -e '.result == "created"' &> /dev/null; then
    print_status "OK" "Write endpoint working correctly"
    WRITE_DOC_ID=$(echo "$WRITE_TEST" | jq -r '.id')
    print_status "INFO" "Test document created: $WRITE_DOC_ID"
else
    print_status "WARN" "Write endpoint test failed"
    echo "   Response: $WRITE_TEST"
fi

# Test streaming endpoint (with timeout)
print_status "INFO" "Testing streaming endpoint..."
STREAM_TEST=$(timeout 3s curl -s -X POST \
    -H "Content-Type: application/json" \
    -d '{"query": "health"}' \
    "http://localhost:$PORT/api/healthcheck/stream" | head -c 100)

if [ -n "$STREAM_TEST" ]; then
    print_status "OK" "Streaming endpoint responding"
else
    print_status "WARN" "Streaming endpoint test inconclusive"
fi

# Check 4: Performance Metrics
print_status "INFO" "Basic performance check..."

# Measure response time for health endpoint
START_TIME=$(date +%s%N)
curl -s --max-time $TIMEOUT "http://localhost:$PORT/_cluster/health" > /dev/null
END_TIME=$(date +%s%N)
RESPONSE_TIME=$(( (END_TIME - START_TIME) / 1000000 ))  # Convert to milliseconds

if [ $RESPONSE_TIME -lt 100 ]; then
    print_status "OK" "Health endpoint response time: ${RESPONSE_TIME}ms"
elif [ $RESPONSE_TIME -lt 500 ]; then
    print_status "WARN" "Health endpoint response time: ${RESPONSE_TIME}ms (>100ms)"
else
    print_status "ERROR" "Health endpoint response time: ${RESPONSE_TIME}ms (>500ms)"
fi

# Check 5: System Resources (if available)
if command -v ps &> /dev/null; then
    print_status "INFO" "Checking CameoDB process..."
    SERVER_PID=$(pgrep -f "target.*cameodb" | head -1)
    if [ -n "$SERVER_PID" ]; then
        print_status "OK" "CameoDB process found (PID: $SERVER_PID)"
        
        # Get memory usage if available
        if command -v ps &> /dev/null; then
            MEMORY_MB=$(ps -o rss= -p "$SERVER_PID" 2>/dev/null | awk '{print int($1/1024)}')
            if [ -n "$MEMORY_MB" ]; then
                if [ $MEMORY_MB -lt 100 ]; then
                    print_status "OK" "Memory usage: ${MEMORY_MB}MB"
                elif [ $MEMORY_MB -lt 500 ]; then
                    print_status "WARN" "Memory usage: ${MEMORY_MB}MB (>100MB)"
                else
                    print_status "ERROR" "Memory usage: ${MEMORY_MB}MB (>500MB)"
                fi
            fi
        fi
    else
        print_status "WARN" "CameoDB process not found via pgrep"
    fi
fi

echo ""
echo "📊 Health Check Summary"
echo "======================"
echo "CameoDB: http://localhost:$PORT"
echo "Status: $STATUS"
echo "Node: ${NODE_ID:0:12}..."
echo "Shards: $ACTIVE_SHARDS"
echo "Response Time: ${RESPONSE_TIME}ms"

# Overall health assessment
OVERALL_STATUS="HEALTHY"
if [ "$STATUS" != "green" ] || [ $RESPONSE_TIME -gt 500 ]; then
    OVERALL_STATUS="DEGRADED"
fi

if [ "$OVERALL_STATUS" = "HEALTHY" ]; then
    print_status "OK" "Overall Status: $OVERALL_STATUS"
    exit 0
else
    print_status "WARN" "Overall Status: $OVERALL_STATUS"
    exit 1
fi
