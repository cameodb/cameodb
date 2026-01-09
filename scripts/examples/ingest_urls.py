#!/usr/bin/env python3
"""Load URL data into CameoDB via HTTP bulk API."""

import argparse
import ast
import json
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

DEFAULT_BASE_URL = "http://localhost:9480"
DEFAULT_INDEX = "urls"
DEFAULT_DATA_PATH = Path("scripts/data/urls.csv")
DEFAULT_BATCH_SIZE = 1000
DEFAULT_MAX_BATCH_BYTES = 2 * 1024 * 1024  # 2MB


@dataclass
class BatchBuffer:
    docs: List[Dict[str, Any]]
    bytes_used: int = 0

    def append(self, doc: Dict[str, Any], size_bytes: int) -> None:
        self.docs.append(doc)
        self.bytes_used += size_bytes

    def reset(self) -> List[Dict[str, Any]]:
        payload = self.docs
        self.docs = []
        self.bytes_used = 0
        return payload

    def __bool__(self) -> bool:  # pragma: no cover - convenience helper
        return bool(self.docs)


def parse_urls(urls_text: str) -> List[str]:
    try:
        parsed = ast.literal_eval(urls_text)
    except (SyntaxError, ValueError):
        return []
    if isinstance(parsed, list):
        # Deduplicate while preserving order
        seen = set()
        deduped = []
        for u in parsed:
            u_str = str(u)
            if u_str and u_str not in seen:
                seen.add(u_str)
                deduped.append(u_str)
        return deduped
    return []


def build_document(line: str) -> Optional[Dict[str, Any]]:
    # Expecting: "sha1","['url1','url2']"
    parts = [p.strip().strip('"') for p in line.split(",", maxsplit=1)]
    if len(parts) != 2:
        return None
    sha1, urls_raw = parts
    if not sha1:
        return None
    urls = parse_urls(urls_raw)
    if not urls:
        return None
    return {
        "id": sha1,
        # routing_key omitted: server will hash `id` for sharding
        "doc": {
            "id": sha1,  # required by CameoDB validators
            "urls": urls,
        },
    }


def get_cluster_health(base_url: str) -> Optional[Dict[str, Any]]:
    """Get cluster health information including active shard count."""
    try:
        url = f"{base_url.rstrip('/')}/_cluster/health"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException:
        return None


def send_batch(session: requests.Session, base_url: str, index: str, batch: List[Dict[str, Any]]) -> Dict[str, Any]:
    url = f"{base_url.rstrip('/')}/api/{index}/_bulk"
    response = session.post(url, json=batch, timeout=30)
    if response.status_code >= 400:
        # surface body to help debug (e.g., proxy/gateway errors)
        sys.stderr.write(f"HTTP {response.status_code}: {response.text[:2000]}\n")
    response.raise_for_status()
    return response.json()


def ingest(
    base_url: str,
    index: str,
    data_path: Path,
    dry_run: bool = False,
    batch_size: int = DEFAULT_BATCH_SIZE,
    max_batch_bytes: int = DEFAULT_MAX_BATCH_BYTES,
) -> None:
    """Ingest URL data using batch processing for optimal performance."""
    if not data_path.exists():
        raise SystemExit(f"Data file not found: {data_path}")

    # Get cluster health to show actual shard count
    health = get_cluster_health(base_url)
    active_shards = health.get("active_shards", "unknown") if health else "unknown"
    cluster_name = health.get("cluster_name", "unknown") if health else "unknown"
    
    print(
        f"Starting batch ingestion with max batch size: "
        f"{batch_size}, max bytes: {max_batch_bytes // 1024 // 1024}MB"
    )
    print(f"Target index: '{index}' (will use {active_shards} shards)")
    print(f"Cluster: {cluster_name}")
    if health:
        print(f"Cluster status: {health.get('status', 'unknown')}")
    print()

    session = requests.Session()
    session.trust_env = False  # ignore proxy env vars (avoid Squid issues)

    with data_path.open(encoding="utf-8") as handle:
        buffer = BatchBuffer(docs=[])
        total_processed = 0
        total_indexed = 0
        batch_count = 0
        start_time = time.time()

        def document_size_bytes(doc_payload: Dict[str, Any]) -> int:
            return len(json.dumps(doc_payload, ensure_ascii=False).encode("utf-8"))

        def flush_batch() -> None:
            nonlocal batch_count, total_processed, total_indexed
            if not buffer.docs:
                return

            docs_to_send = buffer.reset()
            batch_count += 1
            batch_start = time.time()

            try:
                if not dry_run:
                    result = send_batch(session, base_url, index, docs_to_send)
                    batch_indexed = result.get("items_written", 0)
                    items_received = result.get("items_received", len(docs_to_send))
                    errors = result.get("errors", [])
                    
                    # Calculate operation information from actual response
                    total_operations = len(docs_to_send)
                    failed_operations = len(errors)
                    successful_operations = total_operations - failed_operations
                else:
                    batch_indexed = len(docs_to_send)
                    items_received = len(docs_to_send)
                    total_operations = len(docs_to_send)
                    failed_operations = 0
                    successful_operations = total_operations

                batch_time = time.time() - batch_start
                total_indexed += batch_indexed

                print(
                    f"Batch {batch_count}: {batch_indexed}/{items_received} docs indexed "
                    f"({successful_operations}/{total_operations} operations successful, {failed_operations} failed) "
                    f"in {batch_time:.2f}s"
                )

                if failed_operations > 0:
                    print(
                        f"  Warning: {failed_operations} operations failed in batch {batch_count}"
                    )
            except Exception as exc:  # pragma: no cover - network failure path
                print(f"Batch {batch_count} failed: {exc}")
                total_processed += len(docs_to_send)

        for line_num, line in enumerate(handle, 1):
            line = line.strip()
            if not line:
                continue
            doc = build_document(line)
            if not doc:
                continue

            if dry_run and line_num <= 5:
                try:
                    print(f"Document {line_num}:")
                    print(json.dumps(doc, ensure_ascii=False, indent=2))
                    print("-" * 50)
                except BrokenPipeError:
                    return

            doc_size = document_size_bytes(doc)
            buffer.append(doc, doc_size)

            if len(buffer.docs) >= batch_size or (
                max_batch_bytes and buffer.bytes_used > max_batch_bytes
            ):
                flush_batch()

        flush_batch()

    total_time = time.time() - start_time

    if dry_run:
        print(f"Dry run completed: {total_processed} documents processed in {total_time:.2f}s")
        if health:
            print(f"  Index '{index}' will be created with {active_shards} shards")
    else:
        docs_per_sec = total_indexed / total_time if total_time > 0 else 0
        print(f"\nIngestion completed:")
        print(f"  Total processed: {total_processed} documents")
        print(f"  Total indexed: {total_indexed} documents")
        print(f"  Batches sent: {batch_count}")
        print(f"  Total time: {total_time:.2f}s")
        print(f"  Throughput: {docs_per_sec:.1f} docs/sec")
        print(f"  Index: '{index}'")
        if health:
            print(f"  Index created with {active_shards} shards")


def main() -> None:
    parser = argparse.ArgumentParser(description="Load URL data into CameoDB with batch processing")
    parser.add_argument(
        "--base-url",
        default=DEFAULT_BASE_URL,
        help=f"CameoDB HTTP base URL (default: {DEFAULT_BASE_URL})",
    )
    parser.add_argument(
        "--index",
        default=DEFAULT_INDEX,
        help=f"Target index name (default: {DEFAULT_INDEX})",
    )
    parser.add_argument(
        "--data",
        type=Path,
        default=DEFAULT_DATA_PATH,
        help=f"Path to URL data CSV file (default: {DEFAULT_DATA_PATH})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print sample documents instead of sending to CameoDB",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Maximum documents per batch (default: {DEFAULT_BATCH_SIZE})",
    )
    parser.add_argument(
        "--max-batch-mb",
        type=int,
        default=DEFAULT_MAX_BATCH_BYTES // 1024 // 1024,
        help=f"Maximum batch size in MB (default: {DEFAULT_MAX_BATCH_BYTES // 1024 // 1024})",
    )
    args = parser.parse_args()
    max_batch_bytes = args.max_batch_mb * 1024 * 1024

    ingest(
        base_url=args.base_url,
        index=args.index,
        data_path=args.data,
        dry_run=args.dry_run,
        batch_size=args.batch_size,
        max_batch_bytes=max_batch_bytes,
    )


if __name__ == "__main__":
    main()
