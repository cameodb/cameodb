# CameoDB Examples

This directory contains runnable examples for ingesting data into CameoDB using optimized batch processing.

## 📊 Available Datasets

| Dataset | Script | Records | Format | Batch Size | Memory Limit | Description |
|---------|--------|---------|--------|------------|-------------|-------------|
| **TED Talks** | `ingest_ted.py` | ~4,600 | CSV (semicolon) | **4000 docs / 16MB** | YouTube TED talks metadata with descriptions |
| **Book Summaries** | `ingest_books.py` | 16,559 | TSV (tab) | **2000 docs / 16MB** | CMU Book Summaries with plot synopses |

## 🚀 Quick Start

```bash
# Prerequisites: CameoDB running on localhost:9480
# Install dependencies: pip install requests

# Ingest TED talks
python3 scripts/examples/ingest_ted.py

# Ingest book summaries  
python3 scripts/examples/ingest_books.py

# Test with dry run first
python3 scripts/examples/ingest_ted.py --dry-run
python3 scripts/examples/ingest_books.py --dry-run

# Custom data files
python3 scripts/examples/ingest_ted.py --data /path/to/custom/ted.csv
python3 scripts/examples/ingest_books.py --data /path/to/custom/books.txt

# Size analysis (check memory usage and safety)
python3 scripts/examples/size_analysis.py
python3 scripts/examples/size_analysis.py 1500 books  # Test custom batch size
```

---

## 🎤 TED Talks CSV Ingestion

The `ingest_ted.py` script loads TED Talks metadata from the CSV file shipped under
`scripts/data/youtube_ted_2024.csv` and writes documents into a CameoDB index via
the HTTP API.

### Prerequisites

- CameoDB running locally (defaults assume `http://localhost:9480`).
- At least one shard available. Recent builds automatically create shards on first start
  using the `num_shards_init` setting in `cameodb.toml`.
- Python 3.9+ with the `requests` package installed (`python -m pip install requests`).

### Usage

```bash
# Dry run: inspect JSON payloads (shows target index + payload)
python scripts/examples/ingest_ted.py --dry-run | head

# Ingest into default index "ted"
python scripts/examples/ingest_ted.py

# Specify a different index or CSV
python scripts/examples/ingest_ted.py --index talks --csv path/to/file.csv

# Target a remote CameoDB node
python scripts/examples/ingest_ted.py --base-url http://node1:9480

# Inspect recognized schema for an index
curl -s http://localhost:9480/api/ted/_config | jq
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

---

## 📚 Book Summaries Ingestion

The `ingest_books.py` script loads the CMU Book Summaries dataset into CameoDB using the bulk API for optimal performance.

### Dataset

The script processes the CMU Book Summaries dataset (`booksummaries.tsv`), which contains:
- **16,559 books** with plot summaries
- Tab-separated format with fields: book_id, freebase_id, title, author, publication_date, genres_json, summary
- Genres stored as JSON objects with Freebase mappings
- Summaries ranging from short descriptions to detailed plot synopses

### Usage

#### Basic Usage
```bash
# Ingest all books into the 'books' index
python3 scripts/examples/ingest_books.py

# Use custom index name
python3 scripts/examples/ingest_books.py --index literature

# Use custom CameoDB node
python3 scripts/examples/ingest_books.py --base-url http://localhost:8080
```

#### Dry Run (Testing)
```bash
# Test parsing without sending data
python3 scripts/examples/ingest_books.py --dry-run

# Test with smaller batches
python3 scripts/examples/ingest_books.py --dry-run --batch-size 200
```

#### Performance Tuning
```bash
# Larger batches for faster ingestion (still routed via consistent hashing)
python3 scripts/examples/ingest_books.py --batch-size 1000 --max-batch-mb 4
```

### Document Structure

Each book is indexed with the following fields:

```json
{
  "id": "620",
  "doc": {
    "book_id": "620",
    "freebase_id": "/m/0hhy",
    "title": "Animal Farm",
    "author": "George Orwell",
    "publication_date": "1945-08-17",
    "genres": ["Roman à clef", "Satire", "Children's literature", "Speculative fiction", "Fiction"],
    "summary": "Old Major, the old boar on the Manor Farm, calls the animals...",
    "body": "Animal Farm | Author: George Orwell | Old Major, the old boar... | Genres: Roman à clef, Satire, Children's literature, Speculative fiction, Fiction"
  }
}
```

### Performance Optimizations
- **Batch Processing**: Optimized batch sizes (2000 books, 4000 TED, 10000 URLs)
- **Memory Management**: 32MB limit (50% safety margin under 64MB Kameo limit)
- **Smart Batching**: Automatic batch size adjustment based on document size
- **Error Handling**: Detailed error reporting with failed operation counts/sec with optimized batching (Rust 2024 performance improvements)
- **Supervised Smart Commits**: Dynamic commit thresholds based on memory budgets (32MB-512MB) with eventual durability guarantees via async supervision
- **Parallel Sharding**: Automatic document distribution across multiple shards
- **Cluster-Aware**: Real-time cluster health monitoring and accurate shard reporting

### Command Line Options

| Option | Books Default | TED Default | URLs Default | Description |
|--------|--------------|------------|--------------|-------------|
| `--base-url` | `http://localhost:9480` | `http://localhost:9480` | `http://localhost:9480` | CameoDB HTTP base URL |
| `--index` | `books` | `ted` | `urls` | Target index name |
| `--data` | `scripts/data/booksummaries.tsv` | `scripts/data/youtube_ted_2024.csv` | `scripts/data/urls.csv` | Path to data file |
| `--dry-run` | `false` | `false` | `false` | Print sample documents instead of sending |
| `--batch-size` | **2000** | 4000 | 10000 | Maximum documents per batch |
| `--max-batch-mb` | **16** | 16 | 16 | Maximum batch size in MB (50% safety margin under 64MB Kameo limit) |

### Example Output

```
Starting batch ingestion with max batch size: 2000, max bytes: 16MB
Target index: 'books' (will use 4 shards)
Cluster: cameodb-cluster
Cluster status: green

Batch 1: 2000/2000 docs indexed (2000/2000 operations successful, 0 failed) in 1.45s
Batch 2: 2000/2000 docs indexed (2000/2000 operations successful, 0 failed) in 1.39s
...
Batch 9: 559/559 docs indexed (559/559 operations successful, 0 failed) in 0.44s

✅ Ingestion completed successfully
📊 Total: 16,559 docs indexed in 9 batches
🚀 Performance: ~5,000 docs/sec with Kameo-aligned batching
🔍 Cluster: 4 active shards across 1 nodes
```
