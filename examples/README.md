# CameoDB Examples

Two datasets ship under `examples/data/`, and each loads two ways: with the client, which needs
no code, and with a Python script that reshapes values on the way in.

| Dataset | File | Rows | Format |
|---------|------|------|--------|
| **Book Summaries** (CMU) | `booksummaries.tsv` | 16,559 | Tab-separated, typed header (`book_id.text`, `publication_date.date`, …) |
| **TED talks** (YouTube metadata, 2024) | `youtube_ted_2024.csv` | 4,641 | Semicolon-separated |

## Loading with the client

The client reads the source, types every column by its values (a header hint such as
`publication_date.date` wins), picks the id, declares the schema and loads:

```bash
# What the source holds, and why each column becomes the field it does
cameodb client schema detect examples/data/booksummaries.tsv --report

cameodb client data load books examples/data/booksummaries.tsv
cameodb client data load ted examples/data/youtube_ted_2024.csv
```

Values are kept as written, trimmed: an empty cell, and in a numeric, date or boolean field a
missing marker such as `NA`, is stored as no value; in a text field `NA` stays text. For TED
talks the schema comes out as counts in `i64`, `caption` as a `boolean`, `release_date` as a
`date`, the channel and category labels as `string` categories, and `videoId` as the id. For
books, `book_id` is the id and `publication_date` a date — years, year-months and full dates
alike are searched as dates.

Loading again into an index that has a schema reads only the source's first batch ahead and
keeps the schema as it is. See the [client README](../crates/client/README.md) for every option.

## Loading with the Python scripts

The scripts are worked examples of loading through the HTTP API with transformations written by
hand. They declare the schema with `PUT /api/{index}/_config`, then send documents to
`POST /api/{index}/_bulk` in batches — the shared part is in `loader.py`.

```bash
python3 -m pip install requests

python3 examples/ingest_books.py            # into index "books"
python3 examples/ingest_ted.py              # into index "ted"

# The schema and the first documents, sending nothing
python3 examples/ingest_ted.py --dry-run

# Another index, node or file
python3 examples/ingest_books.py --index literature --base-url http://node1:9480 --data path/to/books.tsv
```

| Option | Default | |
|--------|---------|---|
| `--base-url` | `http://localhost:9480` | CameoDB node |
| `--index` | `books` / `ted` | Target index |
| `--data` | the file under `examples/data/` | Source file |
| `--batch-size` | 2000 / 4000 | Documents per batch |
| `--max-batch-mb` | 16 | Bytes per batch |
| `--dry-run` | off | Print the schema and three documents; send nothing |

What each script does that the client does not:

| | Client | Script |
|---|---|---|
| **Books `genres`** | the source's JSON object (`{"/m/06nbt": "Satire", …}`) as text | the list of genre names |
| **TED `tags`, `topicCategories`** | the comma-separated text | lists, one value per tag (`tags`, `topic_categories`) |
| **TED release** | `release_date` (date) and `release_time` (text) | one timestamp, `published_at` |
| **TED `duration`** | `08:01`, as text | `duration_seconds`, to sort and range on |
| **Field names** | as in the source | snake_case |

Both keep the id as a shadow field — searchable by its own name (`book_id:620`,
`video_id:FzhI2D_kaCY`), stored once in the document key — and record it in the schema's
`id_fields`, so a later client load into the same index keys rows the same way.

Running a script again over its own index replaces each document. Declaring a different schema
over an index that holds documents is refused with `409`: delete the documents first
(`curl -X DELETE http://localhost:9480/api/books` keeps the schema), or use another `--index`.

## What to look at after loading

```bash
curl -s http://localhost:9480/api/books/_config | jq          # the schema, as every node holds it
cameodb client search books 'genres:"Science Fiction"' --limit 5
cameodb client search ted 'tags:climate AND viewCount:>=1000000'      # loaded by the client
cameodb client search ted 'tags:climate AND view_count:>=1000000'     # loaded by ingest_ted.py
```

A date field holds 1677-09-21 to 2262-04-11. The book summaries date 48 books earlier, as far
back as the year 398; they are stored as written but searched and sorted as 1677-09-21, and both
the client and `ingest_books.py` count them when they load.

Documents become searchable within a few seconds of being written, when the index commits.
