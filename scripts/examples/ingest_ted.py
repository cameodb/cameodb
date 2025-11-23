#!/usr/bin/env python3
"""Load TED Talks CSV data into CameoDB via HTTP API with batch processing."""

import argparse
import csv
import datetime
import json
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

DEFAULT_BASE_URL = "http://localhost:9480"
DEFAULT_INDEX = "ted"
DEFAULT_CSV_PATH = Path("scripts/data/youtube_ted_2024_03_17.csv")
DEFAULT_BATCH_SIZE = 500
DEFAULT_MAX_BATCH_BYTES = 5 * 1024 * 1024  # 5MB


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


def build_document(row: Dict[str, str], round_robin: bool = False) -> Optional[Dict[str, Any]]:
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
        "caption": parse_bool(row.get("caption", "false")),
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
    payload = {
        "id": video_id,
        "doc": {k: v for k, v in doc_content.items() if v is not None}
    }
    
    # Add routing_key for consistent hashing, or omit for round-robin
    if not round_robin:
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
    round_robin: bool = False,
    batch_size: int = DEFAULT_BATCH_SIZE,
    max_batch_bytes: int = DEFAULT_MAX_BATCH_BYTES
) -> None:
    """Ingest TED talks data using batch processing for optimal performance."""
    if not csv_path.exists():
        raise SystemExit(f"CSV file not found: {csv_path}")

    print(f"Starting batch ingestion with max batch size: {batch_size}, max bytes: {max_batch_bytes // 1024 // 1024}MB")
    
    with csv_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle, delimiter=";")
        
        current_batch = []
        total_processed = 0
        total_indexed = 0
        batch_count = 0
        start_time = time.time()
        
        for row in reader:
            doc = build_document(row, round_robin)
            if not doc:
                continue

            if dry_run:
                try:
                    print(json.dumps(doc, ensure_ascii=False))
                except BrokenPipeError:
                    # Downstream pipe closed (e.g., head). Exit gracefully.
                    return
                total_processed += 1
                continue

            current_batch.append(doc)
            
            # Check if we should send the current batch
            should_send = False
            
            # Always check document count limit first
            if len(current_batch) >= batch_size:
                should_send = True
            else:
                # Check byte size limit - calculate size of current batch
                batch_bytes = sum(len(json.dumps(d, ensure_ascii=False).encode('utf-8')) for d in current_batch)
                if batch_bytes > max_batch_bytes:
                    should_send = True
            
            if should_send:
                batch_count += 1
                batch_start = time.time()
                
                try:
                    result = send_batch(base_url, index, current_batch)
                    batch_indexed = result.get("items_indexed", 0)
                    successful_shards = result.get("successful_shards", 0)
                    failed_shards = result.get("failed_shards", 0)
                    
                    batch_time = time.time() - batch_start
                    total_indexed += batch_indexed
                    total_processed += len(current_batch)
                    
                    print(f"Batch {batch_count}: {batch_indexed}/{len(current_batch)} docs indexed "
                          f"({successful_shards} shards success, {failed_shards} failed) "
                          f"in {batch_time:.2f}s")
                    
                    if failed_shards > 0:
                        print(f"  Warning: {failed_shards} shards failed in batch {batch_count}")
                        
                except Exception as e:
                    print(f"Batch {batch_count} failed: {e}")
                    # Continue with next batch instead of failing completely
                    total_processed += len(current_batch)
                
                current_batch = []
        
        # Send remaining documents in final batch
        if current_batch and not dry_run:
            batch_count += 1
            batch_start = time.time()
            
            try:
                result = send_batch(base_url, index, current_batch)
                batch_indexed = result.get("items_indexed", 0)
                successful_shards = result.get("successful_shards", 0)
                failed_shards = result.get("failed_shards", 0)
                
                batch_time = time.time() - batch_start
                total_indexed += batch_indexed
                total_processed += len(current_batch)
                
                print(f"Final batch {batch_count}: {batch_indexed}/{len(current_batch)} docs indexed "
                      f"({successful_shards} shards success, {failed_shards} failed) "
                      f"in {batch_time:.2f}s")
                      
            except Exception as e:
                print(f"Final batch {batch_count} failed: {e}")
                total_processed += len(current_batch)
        
        elif current_batch and dry_run:
            total_processed += len(current_batch)
    
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
        "--round-robin",
        action="store_true",
        help="Use round-robin distribution instead of consistent hashing",
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
        args.base_url, 
        args.index, 
        args.csv, 
        args.dry_run, 
        args.round_robin,
        args.batch_size,
        max_batch_bytes
    )


if __name__ == "__main__":
    main()
