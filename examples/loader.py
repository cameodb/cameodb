"""What the example loaders share: declaring a schema, sending batches, and reporting.

Each example script is a source file read by hand-written transformations into documents. The
loading itself is the same for every source and lives here: the schema is declared before the
first document, documents go to `POST /api/{index}/_bulk` in batches bounded by count and size,
and the run ends with what the node said about every batch.

`cameodb client data load` does all of this with no code — it types each column from the values
and loads the source as it is. A script like these is for the cases it leaves to you: values
reshaped on the way in (a JSON object reduced to a list, two columns made one), and fields named
otherwise than in the source.
"""

import argparse
import json
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

DEFAULT_BASE_URL = "http://localhost:9480"
DEFAULT_MAX_BATCH_MB = 16
REQUEST_TIMEOUT_SECS = 60


def arguments(description: str, index: str, data: Path, batch_size: int) -> argparse.Namespace:
    """The options every example takes."""
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help=f"CameoDB HTTP URL (default: {DEFAULT_BASE_URL})")
    parser.add_argument("--index", default=index, help=f"Target index (default: {index})")
    parser.add_argument("--data", type=Path, default=data, help=f"Source file (default: {data})")
    parser.add_argument("--batch-size", type=int, default=batch_size, help=f"Documents per batch (default: {batch_size})")
    parser.add_argument("--max-batch-mb", type=int, default=DEFAULT_MAX_BATCH_MB, help=f"Bytes per batch, in MB (default: {DEFAULT_MAX_BATCH_MB})")
    parser.add_argument("--dry-run", action="store_true", help="Print the schema and the first documents; send nothing")
    return parser.parse_args()


def describe_cluster(base_url: str) -> None:
    """Say which cluster the run writes to. An index spans every shard of the cluster."""
    try:
        response = requests.get(f"{base_url.rstrip('/')}/_cluster/health", timeout=10)
        response.raise_for_status()
        health = response.json()
    except requests.exceptions.RequestException as err:
        raise SystemExit(f"CameoDB is not answering at {base_url}: {err}")
    nodes = health.get("total_nodes", 1)
    shards = health.get("cluster_total_shards", health.get("active_shards", "?"))
    print(
        f"Cluster '{health.get('cluster_name', 'standalone')}' ({health.get('status', '?')}): "
        f"{health.get('connected_nodes', nodes)} of {nodes} nodes connected, {shards} shards"
    )


def declare_schema(base_url: str, index: str, schema: Dict[str, Any]) -> None:
    """Declare the index's schema with `PUT /api/{index}/_config` before anything is written.

    Declared again over an index that holds documents, a schema that changes a built column
    (a type, a tokenizer, the id) is refused with 409: delete the documents first
    (`DELETE /api/{index}` keeps the schema), or load into a new index.
    """
    url = f"{base_url.rstrip('/')}/api/{index}/_config"
    response = requests.put(url, json=schema, timeout=REQUEST_TIMEOUT_SECS)
    if response.status_code >= 400:
        raise SystemExit(f"The schema for '{index}' was refused ({response.status_code}): {response.text}")
    print(f"Schema declared for '{index}' (version {response.json().get('version', '?')})")


class BatchLoader:
    """Send documents to an index in batches bounded by count and by bytes."""

    def __init__(self, base_url: str, index: str, batch_size: int, max_batch_mb: int, dry_run: bool):
        self.url = f"{base_url.rstrip('/')}/api/{index}/_bulk"
        self.batch_size = max(1, batch_size)
        self.max_bytes = max_batch_mb * 1024 * 1024
        self.dry_run = dry_run
        self.pending: List[Dict[str, Any]] = []
        self.pending_bytes = 0
        self.batches = 0
        self.sent = 0
        self.written = 0
        self.refused = 0
        self.first_refusal: Optional[str] = None
        self.started = time.time()

    def add(self, payload: Dict[str, Any]) -> None:
        """Queue one `{"id", "routing_key", "doc"}` payload; a full batch is sent."""
        if self.dry_run and self.sent + len(self.pending) < 3:
            print(json.dumps(payload, ensure_ascii=False, indent=2))
        size = len(json.dumps(payload, ensure_ascii=False).encode("utf-8"))
        if self.pending and self.pending_bytes + size > self.max_bytes:
            self.flush()
        self.pending.append(payload)
        self.pending_bytes += size
        if len(self.pending) >= self.batch_size:
            self.flush()

    def flush(self) -> None:
        if not self.pending:
            return
        batch, self.pending, self.pending_bytes = self.pending, [], 0
        self.batches += 1
        self.sent += len(batch)
        if self.dry_run:
            self.written += len(batch)
            return
        try:
            response = requests.post(self.url, json=batch, timeout=REQUEST_TIMEOUT_SECS)
        except requests.exceptions.RequestException as err:
            raise SystemExit(f"Batch {self.batches} could not be sent after {self.written} documents were written: {err}")
        if response.status_code >= 400:
            raise SystemExit(
                f"Batch {self.batches} was refused ({response.status_code}) after {self.written} "
                f"documents were written: {response.text}"
            )
        result = response.json()
        self.written += result.get("items_written", 0)
        errors = result.get("errors") or []
        self.refused += len(errors)
        if errors and self.first_refusal is None:
            self.first_refusal = json.dumps(errors[0], ensure_ascii=False)

    def finish(self, index: str) -> None:
        """Send what is left and say what the node did with every document."""
        self.flush()
        elapsed = time.time() - self.started
        rate = self.written / elapsed if elapsed > 0 else 0.0
        verb = "Would write" if self.dry_run else "Wrote"
        print(
            f"{verb} {self.written:,} of {self.sent:,} documents to '{index}' in {self.batches} "
            f"batches, {elapsed:.1f} s ({rate:,.0f} documents/s)"
        )
        if self.refused:
            print(f"  {self.refused:,} documents were refused; the first: {self.first_refusal}")
        if not self.dry_run:
            print("  Searchable within a few seconds, once the index commits.")
