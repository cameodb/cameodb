#!/bin/bash

# CameoDB Load Testing Script
# Performs basic load testing on CameoDB HTTP API

set -e

# Configuration
DEFAULT_PORT=9480
PORT=${1:-$DEFAULT_PORT}
CONCURRENT_USERS=${2:-10}
REQUESTS_PER_USER=${3:-50}
INDEX_NAME="loadtest"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

print_status() {
    local status=$1
    local message=$2
    case $status in
        "OK")     echo -e "${GREEN}✅ $message${NC}" ;;
        "WARN")   echo -e "${YELLOW}⚠️  $message${NC}" ;;
        "ERROR")  echo -e "${RED}❌ $message${NC}" ;;
    esac
}

echo "🚀 CameoDB Load Test"
echo "==================="
echo "Target: http://localhost:$PORT"
echo "Concurrent Users: $CONCURRENT_USERS"
echo "Requests per User: $REQUESTS_PER_USER"
echo "Total Requests: $((CONCURRENT_USERS * REQUESTS_PER_USER))"
echo ""

# Check if CameoDB is running
if ! curl -s "http://localhost:$PORT/_cluster/health" &> /dev/null; then
    print_status "ERROR" "CameoDB is not running on port $PORT"
    exit 1
fi

print_status "OK" "CameoDB is accessible"

# Create temporary directory for results
TEMP_DIR=$(mktemp -d)
trap "rm -rf $TEMP_DIR" EXIT

# Function to run write load test
run_write_test() {
    local user_id=$1
    local results_file="$TEMP_DIR/write_results_$user_id"
    local success_count=0
    local error_count=0
    
    for i in $(seq 1 $REQUESTS_PER_USER); do
        local doc_id="load-test-u${user_id}-d${i}"
        local start_time=$(date +%s%N)
        
        local response=$(curl -s -w "%{http_code}" -X PUT \
            -H "Content-Type: application/json" \
            -d "{\"id\": \"$doc_id\", \"doc\": {\"title\": \"Load Test Document $doc_id\", \"content\": \"This is a load test document created by user $user_id in iteration $i\", \"user_id\": $user_id, \"iteration\": $i}}" \
            "http://localhost:$PORT/api/$INDEX_NAME/document")
        
        local end_time=$(date +%s%N)
        local response_time=$(( (end_time - start_time) / 1000000 ))
        local http_code="${response: -3}"
        
        if [ "$http_code" = "200" ]; then
            ((success_count++))
        else
            ((error_count++))
        fi
        
        echo "$response_time" >> "$results_file"
    done
    
    echo "$user_id,$success_count,$error_count" > "$TEMP_DIR/summary_write_$user_id"
}

# Function to run search load test
run_search_test() {
    local user_id=$1
    local results_file="$TEMP_DIR/search_results_$user_id"
    local success_count=0
    local error_count=0
    
    local queries=("load" "test" "document" "user" "iteration")
    
    for i in $(seq 1 $REQUESTS_PER_USER); do
        local query="${queries[$((i % ${#queries[@]}))]}"
        local start_time=$(date +%s%N)
        
        local response=$(curl -s -w "%{http_code}" -X POST \
            -H "Content-Type: application/json" \
            -d "{\"query\": \"$query\", \"limit\": 10}" \
            "http://localhost:$PORT/api/$INDEX_NAME/search")
        
        local end_time=$(date +%s%N)
        local response_time=$(( (end_time - start_time) / 1000000 ))
        local http_code="${response: -3}"
        
        if [ "$http_code" = "200" ]; then
            ((success_count++))
        else
            ((error_count++))
        fi
        
        echo "$response_time" >> "$results_file"
    done
    
    echo "$user_id,$success_count,$error_count" > "$TEMP_DIR/summary_search_$user_id"
}

# Phase 1: Write Load Test
echo "📝 Phase 1: Write Load Test"
echo "Starting $CONCURRENT_USERS concurrent write processes..."

write_pids=()
write_start_time=$(date +%s)

for user_id in $(seq 1 $CONCURRENT_USERS); do
    run_write_test $user_id &
    write_pids+=($!)
done

# Wait for all write processes to complete
for pid in "${write_pids[@]}"; do
    wait $pid
done

write_end_time=$(date +%s)
write_duration=$((write_end_time - write_start_time))

print_status "OK" "Write load test completed in ${write_duration}s"

# Calculate write statistics
write_total_success=0
write_total_errors=0
write_response_times=()

for user_id in $(seq 1 $CONCURRENT_USERS); do
    if [ -f "$TEMP_DIR/summary_write_$user_id" ]; then
        IFS=',' read -r uid success errors < "$TEMP_DIR/summary_write_$user_id"
        write_total_success=$((write_total_success + success))
        write_total_errors=$((write_total_errors + errors))
    fi
    
    if [ -f "$TEMP_DIR/write_results_$user_id" ]; then
        while read -r time; do
            write_response_times+=($time)
        done < "$TEMP_DIR/write_results_$user_id"
    fi
done

# Phase 2: Search Load Test (after writes are done)
echo ""
echo "🔍 Phase 2: Search Load Test"
echo "Starting $CONCURRENT_USERS concurrent search processes..."

search_pids=()
search_start_time=$(date +%s)

for user_id in $(seq 1 $CONCURRENT_USERS); do
    run_search_test $user_id &
    search_pids+=($!)
done

# Wait for all search processes to complete
for pid in "${search_pids[@]}"; do
    wait $pid
done

search_end_time=$(date +%s)
search_duration=$((search_end_time - search_start_time))

print_status "OK" "Search load test completed in ${search_duration}s"

# Calculate search statistics
search_total_success=0
search_total_errors=0
search_response_times=()

for user_id in $(seq 1 $CONCURRENT_USERS); do
    if [ -f "$TEMP_DIR/summary_search_$user_id" ]; then
        IFS=',' read -r uid success errors < "$TEMP_DIR/summary_search_$user_id"
        search_total_success=$((search_total_success + success))
        search_total_errors=$((search_total_errors + errors))
    fi
    
    if [ -f "$TEMP_DIR/search_results_$user_id" ]; then
        while read -r time; do
            search_response_times+=($time)
        done < "$TEMP_DIR/search_results_$user_id"
    fi
done

# Calculate statistics
calculate_stats() {
    local arr=("$@")
    local count=${#arr[@]}
    local sum=0
    local min=${arr[0]}
    local max=${arr[0]}
    
    for time in "${arr[@]}"; do
        sum=$((sum + time))
        if [ $time -lt $min ]; then min=$time; fi
        if [ $time -gt $max ]; then max=$time; fi
    done
    
    local avg=$((sum / count))
    
    # Calculate 95th percentile (approximate)
    local sorted=($(printf '%s\n' "${arr[@]}" | sort -n))
    local p95_index=$(( count * 95 / 100 ))
    local p95=${sorted[$p95_index]}
    
    echo "$avg,$min,$max,$p95"
}

echo ""
echo "📊 Load Test Results"
echo "==================="

# Write results
if [ ${#write_response_times[@]} -gt 0 ]; then
    IFS=',' read -r write_avg write_min write_max write_p95 <<< $(calculate_stats "${write_response_times[@]}")
    write_rps=$((write_total_success / write_duration))
    
    echo "Write Operations:"
    echo "  Total Requests: $((write_total_success + write_total_errors))"
    echo "  Successful: $write_total_success"
    echo "  Errors: $write_total_errors"
    echo "  Duration: ${write_duration}s"
    echo "  Requests/sec: $write_rps"
    echo "  Response Times (ms):"
    echo "    Average: $write_avg"
    echo "    Min: $write_min"
    echo "    Max: $write_max"
    echo "    95th percentile: $write_p95"
fi

echo ""

# Search results  
if [ ${#search_response_times[@]} -gt 0 ]; then
    IFS=',' read -r search_avg search_min search_max search_p95 <<< $(calculate_stats "${search_response_times[@]}")
    search_rps=$((search_total_success / search_duration))
    
    echo "Search Operations:"
    echo "  Total Requests: $((search_total_success + search_total_errors))"
    echo "  Successful: $search_total_success"
    echo "  Errors: $search_total_errors"
    echo "  Duration: ${search_duration}s"
    echo "  Requests/sec: $search_rps"
    echo "  Response Times (ms):"
    echo "    Average: $search_avg"
    echo "    Min: $search_min"
    echo "    Max: $search_max"
    echo "    95th percentile: $search_p95"
fi

# Overall assessment
total_errors=$((write_total_errors + search_total_errors))
if [ $total_errors -eq 0 ]; then
    print_status "OK" "Load test completed successfully with no errors"
elif [ $total_errors -lt 10 ]; then
    print_status "WARN" "Load test completed with $total_errors errors (acceptable)"
else
    print_status "ERROR" "Load test completed with $total_errors errors (concerning)"
fi

echo ""
echo "Test data created in index '$INDEX_NAME' - you may want to clean it up later."
