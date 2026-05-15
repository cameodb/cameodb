<div align="center">
  <h1>CameoDB</h1>
  <p><strong>A high-performance, distributed hybrid-search database built in Rust.</strong></p>
  
  [![Rust](https://img.shields.io/badge/rust-1.80%2B-blue.svg)](https://www.rust-lang.org)
  [![License](https://img.shields.io/badge/license-Apache--2.0-green.svg)](LICENSE)
</div>

## ✨ What is CameoDB?

CameoDB is a decentralized, leaderless database that combines the durability of an ACID-compliant Key-Value store (`redb`) with the power of a full-text inverted index engine (`tantivy`). 

By leveraging the **Kameo** actor framework and **Tokio**'s async runtime, CameoDB provides blazingly fast full-text search across distributed nodes—without the overhead of a central master.

## 🚀 Key Features

* **Hybrid Storage Architecture:** Every shard is an atomic unit containing both a KV store (for data/WAL) and an inverted index (for full-text search).
* **Decentralized Topology:** Zero master nodes. Utilizes Consistent Hashing and a custom Kademlia DHT behavior for seamless peer discovery and routing.
* **Supervised Smart Commits:** Intelligently batches writes with micro-second precision to optimize throughput while maintaining strict crash durability.
* **Tiered Cache Sizing:** Dynamically budgets memory across active shards based on system RAM to ensure steady-state operational safety.
* **Sophisticated Schema Detection:** Automatically infers and maps indices from raw data payloads via versatile columnar structural testing and verifiable format detections.
* **Intelligent Data Loader:** Robust, zero-copy ingestion pipeline that transparently handles multiple formats (`CSV`, `TSV`, `JSON`, `JSONL`) and on-the-fly decompression (`Gzip`, `Bzip2`, `Zstd`, `XZ`, `LZ4`, `Deflate`). It supports loading from local disk, distributed network files, and directly streaming from HTTP(S) endpoints.
* **Consistent Hybrid Recovery:** Guarantees data consistency during startup by automatically recovering and syncing uncommitted records from the ACID datastore (KV) into the search index.
* **Graceful Shutdowns:** Multi-phase process ensuring zero WAL replay on clean reboots.
* **Production Memory Management:** Jemalloc allocator with per-CPU arenas, background purge threads, and runtime admin endpoints for memory diagnostics and manual intervention.
* **Writer Core Pinning:** Optional CPU core affinity for shard writer threads to improve cache locality and reduce scheduling jitter on the write hot path.

## 📦 Quick Start (Docker)

Get a single-node CameoDB instance up and running instantly using Docker.

```bash
# Pre-create data directory with user permissions
mkdir -p $(pwd)/data/cameodb
chown -R 65532:65532 $(pwd)/data/cameodb

# 1. Run the CameoDB Server
docker run -d \
  --name cameodb-server \
  -p 9480:9480 \
  -p 9580:9580 \
  -v $(pwd)/data/cameodb:/data/cameodb \
  goranc/cameodb:latest

# 2. Launch the Interactive CLI (connects to the server)
docker run -it --rm \
  --network host \
  goranc/cameodb:latest \
  client --interactive --connect http://localhost:9480
```
*(CameoDB's HTTP API is now available on `http://localhost:9480`)*

For more deployment options (Docker Compose, multi-node clusters), see the [Deployment Guide](docs/DEPLOYMENT.md).

## 📚 Documentation

Dive deeper into CameoDB's architecture and APIs:

- 📖 **[HTTP API Reference](docs/API_REFERENCE.md)**: Full details on Search, Ingestion, and Index Management.
- 🏗️ **[Architecture & Routing](docs/ARCHITECTURE.md)**: How the distributed leaderless routing works under the hood.
- ⚙️ **[Configuration Guide](docs/CONFIGURATION.md)**: Adjusting shard counts, cache sizes, and swarm ports.
- 📦 **[Building & Packaging](docs/BUILDING.md)**: Instructions for compiling cross-platform binaries and generating RPM/DEB packages.
- 📊 **[Data Ingestion Examples](examples/README.md)**: Sample python scripts and datasets (TED Talks, Book Summaries) to try out right away.

## 🛠️ Single Binary Architecture

CameoDB is distributed as a single, self-contained executable. It acts as the database server, a powerful CLI client, a schema operator, and a bulk data loader.

```bash
# Start the database server (HTTP API available on port 9480)
cameodb
```

### 🦸 Zero to Hero (Interactive CLI)
The easiest way to explore CameoDB is through its interactive REPL, which provides auto-completion, history, and colorized JSON outputs.

```bash
# Launch the interactive client (connects to localhost:9480 by default)
cameodb client --interactive

# Inside the REPL, try:
cameodb@localhost:9480 ▶ health
cameodb@localhost:9480 ▶ schema detect ./examples/data/booksummaries.tsv
cameodb@localhost:9480 ▶ data load books ./examples/data/booksummaries.tsv
cameodb@localhost:9480 ▶ search books "title:Hitchhiker" limit 10
cameodb@localhost:9480 ▶ admin memory stats
cameodb@localhost:9480 ▶ admin memory purge --force
```

### 🗜️ Supported Ingestion Formats
The CLI client features a robust, zero-copy ingestion pipeline that transparently handles:
- **Formats:** `CSV`, `TSV`, `JSON` (Documents/Arrays), and `JSONL/NDJSON`.
- **Compression:** Automatically detects and decompresses `Gzip (.gz)`, `Bzip2 (.bz2)`, `Zstd (.zst)`, `XZ (.xz)`, `LZ4 (.lz4)`, and `Deflate` formats on the fly.
- **Sources:** Ingest data from local disk files, mounted network paths, or by streaming directly from public `HTTP/HTTPS` URLs.

## 🤝 Contributing

We welcome contributions! Whether it's adding new features, fixing bugs, or improving documentation, check out our [Contributing Guidelines](.github/CONTRIBUTING.md) to get started.

Please review our [Code of Conduct](.github/CODE_OF_CONDUCT.md) before participating in the community.

If you discover a vulnerability, please refer to our [Security Policy](.github/SECURITY.md).

## 📄 License

This project is licensed under the [Apache License 2.0](LICENSE) - see the LICENSE file for details.
