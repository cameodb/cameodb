#!/bin/bash

# CameoDB Sample Data Generator
# Generates and loads sample data for development and testing

set -e

# Configuration
DEFAULT_PORT=9480
PORT=${1:-$DEFAULT_PORT}
INDEX_NAME=${2:-"sample"}
DOC_COUNT=${3:-100}

echo "📊 Generating $DOC_COUNT sample documents for index '$INDEX_NAME'..."

# Check if CameoDB is running
if ! curl -s "http://localhost:$PORT/_cluster/health" &> /dev/null; then
    echo "❌ CameoDB is not running on port $PORT"
    echo "   Start it first: cargo run --release --bin cameodb"
    exit 1
fi

# Sample data categories and content
CATEGORIES=("technology" "science" "business" "education" "entertainment" "sports" "health" "travel")
TOPICS=("artificial intelligence" "machine learning" "blockchain" "quantum computing" "biotechnology" "renewable energy" "space exploration" "robotics" "cybersecurity" "data science")
ADJECTIVES=("innovative" "revolutionary" "advanced" "cutting-edge" "efficient" "scalable" "robust" "intelligent" "sophisticated" "comprehensive")

# Function to generate random element from array
random_element() {
    local arr=("$@")
    echo "${arr[RANDOM % ${#arr[@]}]}"
}

# Function to generate sample document
generate_document() {
    local id=$1
    local category=$(random_element "${CATEGORIES[@]}")
    local topic=$(random_element "${TOPICS[@]}")
    local adjective=$(random_element "${ADJECTIVES[@]}")
    
    local title="$(echo "$adjective $topic" | sed 's/\b\w/\U&/g')"
    local content="This document explores $adjective approaches to $topic in the context of modern $category. It provides comprehensive insights and practical applications for developers and researchers working with distributed systems and database technologies."
    
    # Add some variety to content length
    if [ $((RANDOM % 3)) -eq 0 ]; then
        content="$content This extended analysis covers implementation details, performance considerations, and best practices for production environments."
    fi
    
    local tags='["'$(echo "$topic" | tr ' ' '-')'", "'$category'", "'$adjective'"]'
    
    cat << EOF
{
  "id": "sample-$id",
  "doc": {
    "title": "$title",
    "content": "$content",
    "category": "$category",
    "topic": "$topic",
    "tags": $tags,
    "doc_id": $id,
    "generated_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
    "word_count": $(echo "$content" | wc -w | tr -d ' ')
  }
}
EOF
}

# Progress tracking
success_count=0
error_count=0

echo "🚀 Starting data generation..."
echo "   Server: http://localhost:$PORT"
echo "   Index: $INDEX_NAME"
echo "   Documents: $DOC_COUNT"
echo ""

# Generate and upload documents
for i in $(seq 1 $DOC_COUNT); do
    # Show progress every 10 documents
    if [ $((i % 10)) -eq 0 ] || [ $i -eq 1 ]; then
        echo "📝 Progress: $i/$DOC_COUNT documents..."
    fi
    
    # Generate document JSON
    doc_json=$(generate_document $i)
    
    # Upload document
    response=$(curl -s -X PUT \
        -H "Content-Type: application/json" \
        -d "$doc_json" \
        "http://localhost:$PORT/api/$INDEX_NAME/document")
    
    # Check if upload was successful
    if echo "$response" | jq -e '.result == "created"' &> /dev/null; then
        ((success_count++))
    else
        ((error_count++))
        if [ $error_count -le 5 ]; then  # Show first 5 errors only
            echo "⚠️  Error uploading document $i: $response"
        fi
    fi
    
    # Small delay to avoid overwhelming CameoDB
    sleep 0.01
done

echo ""
echo "✅ Data generation complete!"
echo "   Successfully uploaded: $success_count documents"
if [ $error_count -gt 0 ]; then
    echo "   Errors: $error_count documents"
fi

# Test search with generated data
echo ""
echo "🔍 Testing search with generated data..."

# Test searches for different topics
test_queries=("technology" "machine learning" "innovative" "database")

for query in "${test_queries[@]}"; do
    search_response=$(curl -s -X POST \
        -H "Content-Type: application/json" \
        -d "{\"query\": \"$query\", \"limit\": 3}" \
        "http://localhost:$PORT/api/$INDEX_NAME/search")
    
    result_count=$(echo "$search_response" | jq length 2>/dev/null || echo "0")
    echo "   Query '$query': $result_count results"
done

echo ""
echo "🎉 Sample data setup complete!"
echo ""
echo "You can now test queries like:"
echo "  curl -X POST -H 'Content-Type: application/json' \\"
echo "    -d '{\"query\": \"machine learning\", \"limit\": 5}' \\"
echo "    http://localhost:$PORT/api/$INDEX_NAME/search"
