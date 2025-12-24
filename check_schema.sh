#!/bin/bash
# Check the actual schema stored in the database

echo "=== Checking books index schema ==="
curl -s http://localhost:9480/_config/books | jq '.'

echo ""
echo "=== Checking which fields are indexed ==="
curl -s http://localhost:9480/_config/books | jq '.fields | to_entries[] | select(.value.indexed == true) | .key'

echo ""
echo "=== Checking which fields are NOT indexed ==="
curl -s http://localhost:9480/_config/books | jq '.fields | to_entries[] | select(.value.indexed == false) | .key'
