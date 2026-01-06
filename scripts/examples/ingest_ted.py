#!/usr/bin/env python3
"""Load TED Talks CSV data into CameoDB via HTTP API with batch processing."""

import argparse
import csv
import datetime
import json
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

DEFAULT_BASE_URL = "http://localhost:9480"
DEFAULT_INDEX = "ted"
DEFAULT_CSV_PATH = Path("scripts/data/youtube_ted_2024_03_17.csv")
DEFAULT_BATCH_SIZE = 400
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


def parse_duration_to_seconds(raw: str) -> Optional[int]:
    raw = raw.strip()
    if not raw:
        return None
    parts = raw.split(":")
    try:
        if len(parts) == 3:
            h, m, s = map(int, parts)
            return h * 3600 + m * 60 + s
        if len(parts) == 2:
            m, s = map(int, parts)
            return m * 60 + s
        return int(raw)
    except ValueError:
        return None


def parse_bool(raw: str) -> bool:
    return str(raw).strip().lower() in {"true", "1", "yes"}


def parse_datetime(date_str: str, time_str: str) -> Optional[str]:
    date_str = (date_str or "").strip()
    time_str = (time_str or "").strip()
    if not date_str:
        return None
    iso = date_str
    if time_str:
        iso = f"{date_str}T{time_str}"
    try:
        dt = datetime.datetime.fromisoformat(iso)
        return dt.isoformat()
    except ValueError:
        return None


def build_document(row: Dict[str, str]) -> Optional[Dict[str, Any]]:
    """Build a document payload for batch insertion."""
    video_id = row.get("videoId")
    if not video_id:
        return None

    tags: List[str] = [t.strip() for t in row.get("tags", "").split(",") if t.strip()]
    topic_categories = [t.strip() for t in row.get("topicCategories", "").split(",") if t.strip()]
    
    # Build the document content
    doc_content: Dict[str, Any] = {
        "title": (row.get("title") or "").strip(),
        "speaker": (row.get("speaker") or "").strip(),
        "channel": (row.get("channelTitle") or "").strip(),
        "description": row.get("videoDescription") or "",
        "tags": tags,
        "topic_categories": topic_categories,
        "category_id": safe_int(row.get("videoCategoryId")),
        "category_label": row.get("videoCategoryLabel") or None,
        "view_count": safe_int(row.get("viewCount"), default=0),
        "like_count": safe_int(row.get("likeCount"), default=0),
        "comment_count": safe_int(row.get("commentCount"), default=0),
        # Force caption to text to avoid schema inference as boolean
        "caption": "true" if parse_bool(row.get("caption", "false")) else "false",
        "published_at": parse_datetime(row.get("release_date", ""), row.get("release_time", "")),
        "duration_seconds": parse_duration_to_seconds(row.get("duration", "")),
    }
    
    # Create searchable body text from key fields
    body_parts = []
    if doc_content.get("title"):
        body_parts.append(doc_content["title"])
    if doc_content.get("speaker"):
        body_parts.append(f"Speaker: {doc_content['speaker']}")
    if doc_content.get("description"):
        body_parts.append(doc_content["description"])
    if tags:
        body_parts.append(f"Tags: {', '.join(tags)}")
    
    doc_content["body"] = " | ".join(body_parts)
    
    # Build the DocPayload format for bulk API
    doc_content["id"] = video_id

    payload = {
        "id": video_id,
        "doc": {k: v for k, v in doc_content.items() if v is not None}
    }

    # Always include routing_key to leverage consistent hashing by default
    payload["routing_key"] = video_id
        
    return payload


def safe_int(raw: Optional[str], default: Optional[int] = None) -> Optional[int]:
    if raw is None:
        return default
    raw = raw.strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


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
    csv_path: Path,
    dry_run: bool = False,
    batch_size: int = DEFAULT_BATCH_SIZE,
    max_batch_bytes: int = DEFAULT_MAX_BATCH_BYTES,
) -> None:
    """Ingest TED talks data using batch processing for optimal performance."""
    if not csv_path.exists():
        raise SystemExit(f"CSV file not found: {csv_path}")

    print(
        "Starting batch ingestion with max batch size: "
        f"{batch_size}, max bytes: {max_batch_bytes // 1024 // 1024}MB"
    )

    with csv_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle, delimiter=";")

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
                result = send_batch(base_url, index, docs_to_send)
                batch_indexed = result.get("items_written", 0)
                errors = result.get("errors", [])
                successful_shards = 4 - len(errors)  # Calculate from errors
                failed_shards = len(errors)

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

        for row in reader:
            doc = build_document(row)
            if not doc:
                continue

            if dry_run:
                try:
                    print(json.dumps(doc, ensure_ascii=False))
                except BrokenPipeError:
                    return
                total_processed += 1
                continue

            doc_size = document_size_bytes(doc)
            buffer.append(doc, doc_size)

            if len(buffer.docs) >= batch_size or (
                max_batch_bytes and buffer.bytes_used > max_batch_bytes
            ):
                flush_batch()

        # Send remaining documents in final batch
        if dry_run:
            total_processed += len(buffer.docs)
        else:
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
    parser = argparse.ArgumentParser(description="Load TED Talks CSV into CameoDB with batch processing")
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
        "--csv",
        type=Path,
        default=DEFAULT_CSV_PATH,
        help=f"Path to TED CSV (default: {DEFAULT_CSV_PATH})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print documents instead of sending to CameoDB",
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
        csv_path=args.csv,
        dry_run=args.dry_run,
        batch_size=args.batch_size,
        max_batch_bytes=max_batch_bytes,
    )


if __name__ == "__main__":
    main()
