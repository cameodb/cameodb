#!/usr/bin/env python3
"""Load TED Talks CSV data into CameoDB via HTTP API."""

import argparse
import csv
import datetime
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

DEFAULT_BASE_URL = "http://localhost:9480"
DEFAULT_INDEX = "ted"
DEFAULT_CSV_PATH = Path("scripts/data/youtube_ted_2024_03_17.csv")


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


def build_document(row: Dict[str, str]) -> Dict[str, Any]:
    tags: List[str] = [t.strip() for t in row.get("tags", "").split(",") if t.strip()]
    topic_categories = [t.strip() for t in row.get("topicCategories", "").split(",") if t.strip()]
    doc: Dict[str, Any] = {
        "id": row.get("videoId"),
        "routing_key": row.get("videoId"),
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
    return {k: v for k, v in doc.items() if v is not None}


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


def ingest(base_url: str, index: str, csv_path: Path, dry_run: bool = False, round_robin: bool = False) -> None:
    if not csv_path.exists():
        raise SystemExit(f"CSV file not found: {csv_path}")

    url = f"{base_url.rstrip('/')}/api/{index}/document"
    with csv_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle, delimiter=";")
        total = 0
        for row in reader:
            doc = build_document(row)
            doc_id = doc.get("id")
            if not doc_id:
                continue

            if dry_run:
                try:
                    print(json.dumps(doc, ensure_ascii=False))
                except BrokenPipeError:
                    # Downstream pipe closed (e.g., head). Exit gracefully.
                    return
                total += 1
                continue

            # Include routing_key to ensure distribution across shards
            payload = {
                "id": doc_id, 
                "doc": {k: v for k, v in doc.items() if k not in ("id", "routing_key")}
            }
            
            # Add routing_key for consistent hashing, or omit for round-robin
            if not round_robin:
                payload["routing_key"] = doc.get("routing_key")  # Uses videoId for consistent routing
            response = requests.put(url, json=payload, timeout=15)
            if response.status_code != 200:
                raise SystemExit(
                    f"Failed to ingest videoId={doc.get('id')}: {response.status_code} {response.text}"
                )
            total += 1

    print(f"Ingested {total} documents into index '{index}'")


def main() -> None:
    parser = argparse.ArgumentParser(description="Load TED Talks CSV into CameoDB")
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

    args = parser.parse_args()
    ingest(args.base_url, args.index, args.csv, args.dry_run, args.round_robin)


if __name__ == "__main__":
    main()
