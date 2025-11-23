#!/usr/bin/env python3
"""Quick test script to verify shard distribution."""

import requests
import json

BASE_URL = "http://localhost:9480"

def test_distribution():
    # Test with routing keys
    test_docs = [
        {"id": "doc1", "routing_key": "key1", "doc": {"title": "Document 1"}},
        {"id": "doc2", "routing_key": "key2", "doc": {"title": "Document 2"}},
        {"id": "doc3", "routing_key": "key3", "doc": {"title": "Document 3"}},
        {"id": "doc4", "routing_key": "key4", "doc": {"title": "Document 4"}},
        {"id": "doc5", "routing_key": "key5", "doc": {"title": "Document 5"}},
    ]
    
    print("Testing consistent hashing distribution:")
    shard_counts = {}
    
    for doc in test_docs:
        response = requests.put(f"{BASE_URL}/api/test/document", json=doc)
        if response.status_code == 200:
            result = response.json()
            shard_id = result.get("shard_id")
            shard_counts[shard_id] = shard_counts.get(shard_id, 0) + 1
            print(f"  {doc['id']} -> shard {shard_id}")
        else:
            print(f"  Failed to write {doc['id']}: {response.status_code}")
    
    print(f"\nShard distribution: {shard_counts}")
    
    # Test round-robin (no routing_key)
    print("\nTesting round-robin distribution:")
    rr_shard_counts = {}
    
    for i in range(8):
        doc = {"id": f"rr_doc{i}", "doc": {"title": f"Round Robin Doc {i}"}}
        response = requests.put(f"{BASE_URL}/api/test/document", json=doc)
        if response.status_code == 200:
            result = response.json()
            shard_id = result.get("shard_id")
            rr_shard_counts[shard_id] = rr_shard_counts.get(shard_id, 0) + 1
            print(f"  rr_doc{i} -> shard {shard_id}")
        else:
            print(f"  Failed to write rr_doc{i}: {response.status_code}")
    
    print(f"\nRound-robin distribution: {rr_shard_counts}")

if __name__ == "__main__":
    test_distribution()
