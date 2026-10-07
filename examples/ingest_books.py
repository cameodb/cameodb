#!/usr/bin/env python3
"""Load the CMU Book Summaries into CameoDB, reshaping the genres on the way.

The same file loads with no code at all:

    cameodb client data load books examples/data/booksummaries.tsv

That keeps every column as written. Its genres stay one JSON object per book, mapping Freebase
ids to names — searchable by word, but not as a list of genres. This script is the hand-written
alternative: it declares the schema itself and turns each object into the list of genre names,
so each genre is a value of its own and the Freebase ids stay out of the searched text.

The source is tab-separated with a typed header:
book_id, freebase_id, title, author, publication_date, genres, summary.
"""

import csv
import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

from loader import BatchLoader, arguments, declare_schema, describe_cluster

DEFAULT_INDEX = "books"
DEFAULT_DATA = Path("examples/data/booksummaries.tsv")
DEFAULT_BATCH_SIZE = 2000

# `book_id` makes the id and is kept as a shadow field: found by `book_id:620`, stored once in
# the document key rather than indexed twice. `id_fields` records it, so a later
# `cameodb client data load` into this index keys rows the same way.
SCHEMA: Dict[str, Any] = {
    "description": "CMU Book Summaries: one document per book, with its plot summary.",
    "id_fields": ["book_id"],
    "fields": {
        "book_id": {"field_type": "text", "indexed": False, "is_shadow": True},
        "freebase_id": {"field_type": "string", "description": "Freebase machine id, e.g. /m/0hhy."},
        "title": {"field_type": "text"},
        "author": {"field_type": "text"},
        "publication_date": {
            "field_type": "date",
            "fast": True,
            "description": "As the source writes it: a year, a year and month, or a full date.",
        },
        "genres": {"field_type": "text", "description": "Genre names, one value each."},
        "summary": {"field_type": "text"},
    },
}

# A date field holds 1677-09-21 to 2262-04-11; a book dated earlier is stored as written but
# searched and sorted as 1677-09-21.
EARLIEST_INDEXED_DATE = "1677-09-21"


def before_indexed_range(date: str) -> bool:
    """Whether a `YYYY`, `YYYY-MM` or `YYYY-MM-DD` date falls before what a date field holds,
    read as the engine reads it: a year is its January 1st, a month its first day."""
    if not re.fullmatch(r"\d{4}(-\d{2}(-\d{2})?)?", date):
        return False
    # The range starts 12 minutes into that day, so the day itself is before it.
    return (date + "-01-01"[len(date) - 4 :])[:10] <= EARLIEST_INDEXED_DATE


def genre_names(cell: str) -> List[str]:
    """The genre names in a cell holding `{"/m/…": "Name", …}`; none for anything else."""
    try:
        genres = json.loads(cell) if cell.strip() else {}
    except json.JSONDecodeError:
        return []
    return [name for name in genres.values() if isinstance(name, str)] if isinstance(genres, dict) else []


def build_payload(row: Dict[str, str]) -> Optional[Dict[str, Any]]:
    """One book as a bulk payload, or `None` for a row with no id. Empty cells are left out."""
    book_id = (row.get("book_id") or "").strip()
    if not book_id:
        return None
    doc: Dict[str, Any] = {
        "book_id": book_id,
        "freebase_id": row.get("freebase_id", "").strip(),
        "title": row.get("title", "").strip(),
        "author": row.get("author", "").strip(),
        # Every shape the source uses — `1962`, `1962-05`, `1962-05-17` — is one a date field
        # reads, so the value is sent as written.
        "publication_date": row.get("publication_date", "").strip(),
        "genres": genre_names(row.get("genres", "")),
        "summary": row.get("summary", "").strip(),
    }
    doc = {name: value for name, value in doc.items() if value not in ("", [])}
    return {"id": book_id, "routing_key": book_id, "doc": doc}


def main() -> None:
    args = arguments("Load the CMU Book Summaries into CameoDB.", DEFAULT_INDEX, DEFAULT_DATA, DEFAULT_BATCH_SIZE)
    if not args.data.exists():
        raise SystemExit(f"Data file not found: {args.data}")

    if args.dry_run:
        print(json.dumps(SCHEMA, indent=2))
    else:
        describe_cluster(args.base_url)
        declare_schema(args.base_url, args.index, SCHEMA)

    loader = BatchLoader(args.base_url, args.index, args.batch_size, args.max_batch_mb, args.dry_run)
    skipped = 0
    early_dates = 0
    # Summaries are long; the default field size limit would refuse some of them.
    csv.field_size_limit(sys.maxsize)
    with args.data.open(newline="", encoding="utf-8") as handle:
        rows = csv.reader(handle, delimiter="\t")
        # The header types its columns (`book_id.text`); the names are what precede the dot.
        header = [name.split(".")[0] for name in next(rows)]
        for values in rows:
            payload = build_payload(dict(zip(header, values)))
            if payload is None:
                skipped += 1
                continue
            early_dates += before_indexed_range(payload["doc"].get("publication_date", ""))
            loader.add(payload)

    loader.finish(args.index)
    if skipped:
        print(f"  {skipped:,} rows had no book_id and were skipped.")
    if early_dates:
        print(
            f"  {early_dates:,} books are dated before 1677-09-21: stored as written, but searched "
            f"and sorted as that date."
        )


if __name__ == "__main__":
    main()
