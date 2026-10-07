#!/usr/bin/env python3
"""Load the TED talks YouTube metadata into CameoDB, reshaping fields on the way.

The same file loads with no code at all:

    cameodb client data load ted examples/data/youtube_ted_2024.csv

That keeps every column as written, typed by its values: counts as numbers, `caption` as a
boolean, `release_date` as a date, the channel and category labels as whole values. This script
is the hand-written alternative for what it leaves as text:

- `tags` and `topicCategories`, comma-separated, become lists, one value per tag;
- `release_date` and `release_time` become one timestamp, `published_at`;
- `duration`, written `08:01` or `01:02:03`, becomes `duration_seconds`, to sort and range on;
- fields are renamed to snake_case.

The source is semicolon-separated with a plain header.
"""

import csv
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from loader import BatchLoader, arguments, declare_schema, describe_cluster

DEFAULT_INDEX = "ted"
DEFAULT_DATA = Path("examples/data/youtube_ted_2024.csv")
DEFAULT_BATCH_SIZE = 4000

# `video_id` makes the id and is kept as a shadow field: found by `video_id:FzhI2D_kaCY`, stored
# once in the document key. `string` fields are matched whole, and sorted on when `fast`; `text` fields
# are searched by word.
SCHEMA: Dict[str, Any] = {
    "description": "TED talks on YouTube: one document per video, with its counts as of 2024.",
    "id_fields": ["video_id"],
    "fields": {
        "video_id": {"field_type": "text", "indexed": False, "is_shadow": True},
        "title": {"field_type": "text"},
        "speaker": {"field_type": "text"},
        "channel": {"field_type": "string", "fast": True},
        "description": {"field_type": "text"},
        "tags": {"field_type": "text", "description": "One value per tag."},
        "topic_categories": {"field_type": "text", "description": "One value per topic."},
        "category_id": {"field_type": "i64"},
        "category_label": {"field_type": "string", "fast": True},
        "view_count": {"field_type": "i64"},
        "like_count": {"field_type": "i64", "description": "Absent where the source has NA."},
        "comment_count": {"field_type": "i64", "description": "Absent where the source has NA."},
        "caption": {"field_type": "boolean"},
        "published_at": {"field_type": "date", "fast": True},
        "duration_seconds": {"field_type": "i64"},
    },
}


def count(cell: Optional[str]) -> Optional[int]:
    """A whole number, or `None` for `NA`, an empty cell, or anything else that is not one."""
    try:
        return int((cell or "").strip())
    except ValueError:
        return None


def items(cell: Optional[str]) -> List[str]:
    """The comma-separated values in a cell, without `NA` and `None`."""
    return [item.strip() for item in (cell or "").split(",") if item.strip() not in ("", "NA", "None")]


def seconds(duration: Optional[str]) -> Optional[int]:
    """`08:01` or `01:02:03` in seconds; `None` for anything else."""
    parts = (duration or "").strip().split(":")
    if not 2 <= len(parts) <= 3 or not all(part.isdigit() for part in parts):
        return None
    total = 0
    for part in parts:
        total = total * 60 + int(part)
    return total


def published_at(date: Optional[str], time: Optional[str]) -> Optional[str]:
    """`2024-03-15` at `16:13:13` as `2024-03-15T16:13:13Z`; the date alone when there is no time."""
    date, time = (date or "").strip(), (time or "").strip()
    if not date:
        return None
    return f"{date}T{time}Z" if time else date


def build_payload(row: Dict[str, str]) -> Optional[Dict[str, Any]]:
    """One talk as a bulk payload, or `None` for a row with no video id. Absent values are left
    out rather than sent as zero: a talk whose likes the source does not know has none recorded."""
    video_id = (row.get("videoId") or "").strip()
    if not video_id:
        return None
    caption = (row.get("caption") or "").strip().lower()
    doc: Dict[str, Any] = {
        "video_id": video_id,
        "title": (row.get("title") or "").strip(),
        "speaker": (row.get("speaker") or "").strip(),
        "channel": (row.get("channelTitle") or "").strip(),
        "description": (row.get("videoDescription") or "").strip(),
        "tags": items(row.get("tags")),
        "topic_categories": items(row.get("topicCategories")),
        "category_id": count(row.get("videoCategoryId")),
        "category_label": (row.get("videoCategoryLabel") or "").strip(),
        "view_count": count(row.get("viewCount")),
        "like_count": count(row.get("likeCount")),
        "comment_count": count(row.get("commentCount")),
        "caption": caption == "true" if caption in ("true", "false") else None,
        "published_at": published_at(row.get("release_date"), row.get("release_time")),
        "duration_seconds": seconds(row.get("duration")),
    }
    doc = {name: value for name, value in doc.items() if value not in (None, "", [])}
    return {"id": video_id, "routing_key": video_id, "doc": doc}


def main() -> None:
    args = arguments("Load the TED talks YouTube metadata into CameoDB.", DEFAULT_INDEX, DEFAULT_DATA, DEFAULT_BATCH_SIZE)
    if not args.data.exists():
        raise SystemExit(f"Data file not found: {args.data}")

    if args.dry_run:
        print(json.dumps(SCHEMA, indent=2))
    else:
        describe_cluster(args.base_url)
        declare_schema(args.base_url, args.index, SCHEMA)

    loader = BatchLoader(args.base_url, args.index, args.batch_size, args.max_batch_mb, args.dry_run)
    skipped = 0
    with args.data.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle, delimiter=";"):
            payload = build_payload(row)
            if payload is None:
                skipped += 1
                continue
            loader.add(payload)

    loader.finish(args.index)
    if skipped:
        print(f"  {skipped:,} rows had no videoId and were skipped.")


if __name__ == "__main__":
    main()
