#!/usr/bin/env python3
"""Load Book Summaries data into CameoDB via HTTP API with batch processing."""

import argparse
import json
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

DEFAULT_BASE_URL = "http://localhost:9480"
DEFAULT_INDEX = "books"
DEFAULT_DATA_PATH = Path("scripts/data/booksummaries.txt")
DEFAULT_BATCH_SIZE = 200
DEFAULT_MAX_BATCH_BYTES = 4 * 1024 * 1024  # 4MB


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


def parse_genres(genres_json: str) -> List[str]:
    """Parse the JSON genres field and extract genre names."""
    if not genres_json or genres_json.strip() == "":
        return []
    
    try:
        genres_dict = json.loads(genres_json)
        if isinstance(genres_dict, dict):
            return list(genres_dict.values())
        return []
    except (json.JSONDecodeError, TypeError):
        return []


def parse_publication_date(date_str: str) -> Optional[str]:
    """Parse publication date, handling various formats."""
    date_str = (date_str or "").strip()
    if not date_str:
        return None
    
    # Handle ISO date format (YYYY-MM-DD)
    if len(date_str) == 10 and date_str.count('-') == 2:
        return date_str
    
    # Handle year-only format
    if len(date_str) == 4 and date_str.isdigit():
        return f"{date_str}-01-01"
    
    return date_str


def build_document(line: str) -> Optional[Dict[str, Any]]:
    """Build a document payload for batch insertion from a tab-separated line."""
    # Split by tabs - the format is:
    # book_id \t freebase_id \t title \t author \t publication_date \t genres_json \t summary
    parts = line.strip().split('\t')
    
    if len(parts) < 7:
        return None
    
    book_id, freebase_id, title, author, pub_date, genres_json, summary = parts[:7]
    
    # Skip if missing essential fields
    if not book_id or not title:
        return None
    
    # Parse genres from JSON
    genres = parse_genres(genres_json)
    
    # Build the document content
    doc_content: Dict[str, Any] = {
        "book_id": book_id.strip(),
        "freebase_id": freebase_id.strip() if freebase_id else None,
        "title": title.strip(),
        "author": author.strip() if author else None,
        "publication_date": parse_publication_date(pub_date),
        "genres": genres,
        "summary": summary.strip() if summary else "",
    }
    
    # Create searchable body text from key fields
    body_parts = []
    if doc_content.get("title"):
        body_parts.append(doc_content["title"])
    if doc_content.get("author"):
        body_parts.append(f"Author: {doc_content['author']}")
    if doc_content.get("summary"):
        body_parts.append(doc_content["summary"])
    if genres:
        body_parts.append(f"Genres: {', '.join(genres)}")
    
    doc_content["body"] = " | ".join(body_parts)
    
    # Use book_id as the document ID
    doc_content["id"] = book_id
    
    # Build the DocPayload format for bulk API
    payload = {
        "id": book_id,
        "doc": {k: v for k, v in doc_content.items() if v is not None}
    }

    # Always include routing_key to leverage consistent hashing by default
    payload["routing_key"] = book_id
        
    return payload


def send_batch(base_url: str, index: str, batch: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Send a batch of documents to CameoDB bulk API."""
    url = f"{base_url.rstrip('/')}/api/{index}/_bulk"

    try:
        response = requests.post(url, json=batch, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        raise SystemExit(f"Failed to send batch: {e}")


def ingest(
    base_url: str,
    index: str,
    data_path: Path,
    dry_run: bool = False,
    batch_size: int = DEFAULT_BATCH_SIZE,
    max_batch_bytes: int = DEFAULT_MAX_BATCH_BYTES,
) -> None:
    """Ingest book summaries data using batch processing for optimal performance."""
    if not data_path.exists():
        raise SystemExit(f"Data file not found: {data_path}")

    print(
        "Starting batch ingestion with max batch size: "
        f"{batch_size}, max bytes: {max_batch_bytes // 1024 // 1024}MB"
    )

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
                    result = send_batch(base_url, index, docs_to_send)
                    batch_indexed = result.get("items_indexed", 0)
                    successful_shards = result.get("successful_shards", 0)
                    failed_shards = result.get("failed_shards", 0)
                else:
                    batch_indexed = len(docs_to_send)
                    successful_shards = 4  # Assume 4 shards for dry run
                    failed_shards = 0

                batch_time = time.time() - batch_start
                total_indexed += batch_indexed
                total_processed += len(docs_to_send)

                print(
                    f"Batch {batch_count}: {batch_indexed}/{len(docs_to_send)} docs indexed "
                    f"({successful_shards} shards success, {failed_shards} failed) "
                    f"in {batch_time:.2f}s"
                )

                if failed_shards > 0:
                    print(
                        f"  Warning: {failed_shards} shards failed in batch {batch_count}"
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

            if dry_run and line_num <= 5:  # Show first 5 docs in dry run
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

        # Send remaining documents in final batch
        flush_batch()

    total_time = time.time() - start_time

    if dry_run:
        print(f"Dry run completed: {total_processed} documents processed in {total_time:.2f}s")
    else:
        docs_per_sec = total_indexed / total_time if total_time > 0 else 0
        print(f"\nIngestion completed:")
        print(f"  Total processed: {total_processed} documents")
        print(f"  Total indexed: {total_indexed} documents")
        print(f"  Batches sent: {batch_count}")
        print(f"  Total time: {total_time:.2f}s")
        print(f"  Throughput: {docs_per_sec:.1f} docs/sec")
        print(f"  Index: '{index}'")


def main() -> None:
    parser = argparse.ArgumentParser(description="Load Book Summaries data into CameoDB with batch processing")
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
        help=f"Path to book summaries data file (default: {DEFAULT_DATA_PATH})",
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
