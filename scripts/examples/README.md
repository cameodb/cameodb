# CameoDB Examples

This directory contains runnable examples for ingesting data into CameoDB.

## TED Talks CSV Ingestion

The `ingest_ted.py` script loads TED Talks metadata from the CSV file shipped under
`scripts/data/youtube_ted_2024_03_17.csv` and writes documents into a CameoDB index via
the HTTP API.

### Prerequisites

- CameoDB server running locally (defaults assume `http://localhost:9480`).
- At least one shard available. Recent builds automatically create shards on first start
  using the `init_shards` setting in `cameodb.toml`.
- Python 3.9+ with the `requests` package installed (`python -m pip install requests`).

### Usage

```bash
# Dry run: inspect JSON payloads without writing
python scripts/examples/ingest_ted.py --dry-run | head

# Ingest into default index "ted"
python scripts/examples/ingest_ted.py

# Specify a different index or CSV
python scripts/examples/ingest_ted.py --index talks --csv path/to/file.csv

# Target a remote CameoDB node
python scripts/examples/ingest_ted.py --base-url http://node1:9480
```

### Document Schema

Each CSV row is transformed into a JSON document with the following fields:

| Field              | Type          | Description                                      |
| ------------------ | ------------- | ------------------------------------------------ |
| `id`               | string        | YouTube video ID, used as document ID            |
| `routing_key`      | string        | Same as `id`; ensures deterministic shard choice |
| `title`            | string        | Talk title                                       |
| `speaker`          | string        | Speaker name                                     |
| `channel`          | string        | Channel title                                    |
| `description`      | string        | Full video description                           |
| `tags`             | array<string> | Tags parsed from CSV                             |
| `topic_categories` | array<string> | Topic categories parsed from CSV                 |
| `category_id`      | int?          | YouTube category ID                              |
| `category_label`   | string?       | YouTube category label                           |
| `view_count`       | int           | View count                                       |
| `like_count`       | int           | Like count                                       |
| `comment_count`    | int           | Comment count                                    |
| `caption`          | bool          | Whether captions are available                   |
| `published_at`     | datetime?     | ISO timestamp built from release date/time       |
| `duration_seconds` | int?          | Duration in seconds                              |

### Notes

- The CSV uses semicolons (`;`) as delimiters.
- Use `--dry-run` to verify parsed output before ingestion.
- You can rerun the script safely; documents with the same `id` will be overwritten by
  the current `PUT` semantics.
