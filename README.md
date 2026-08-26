<div align="center">
  <h1>CameoDB</h1>
  <p><strong>A high-performance, distributed hybrid-search database built in Rust.</strong></p>
  
  [![Rust](https://img.shields.io/badge/rust-1.85%2B-blue.svg)](https://www.rust-lang.org)
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
* **TLS/HTTPS Support:** Native HTTPS support via rustls for encrypted client connections and secure API access.

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

## 🔐 TLS/HTTPS Configuration

CameoDB supports native HTTPS via rustls for encrypted client connections. This is essential for production deployments where data security is required.

### Quick Start with TLS

1. **Generate a self-signed certificate** (for development or a test node — nothing verifies it, so both ends have to be yours):
```bash
# Create a self-signed certificate and private key
openssl req -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem -days 365 -nodes \
  -subj "/C=US/ST=State/L=City/O=Organization/CN=localhost"
```

2. **Configure TLS in cameodb.toml**:
```toml
[network.http]
bind_address = "0.0.0.0"
port = 9480

[network.http.tls]
enabled = true
cert_file = "/path/to/cert.pem"
key_file = "/path/to/key.pem"
```

3. **Start the server**:
```bash
cameodb
# Server will now be available on https://localhost:9480
```

### Production Certificates

For production deployments, use certificates from a trusted Certificate Authority (CA):

1. **Obtain certificates** from Let's Encrypt, DigiCert, or your organization's CA
2. **Configure the paths** in your `cameodb.toml`:
```toml
[network.http.tls]
enabled = true
cert_file = "/etc/letsencrypt/live/yourdomain.com/fullchain.pem"
key_file = "/etc/letsencrypt/live/yourdomain.com/privkey.pem"
```

### Linux System Certificate Paths

For production Linux deployments, follow standard filesystem hierarchy and security practices:

**Recommended directory structure:**
```
/etc/cameodb/certs/          # CameoDB-specific certificate directory
├── cert.pem                 # Certificate file (644 root:root)
├── key.pem                  # Private key file (600 root:root)
└── ca-cert.pem              # CA certificate for client validation (644 root:root)
```

**Standard Linux certificate locations:**
- **System-wide certificates**: `/etc/ssl/certs/` (symlinks to actual certs)
- **Private keys**: `/etc/ssl/private/` (restricted access, 700 root:root)
- **Let's Encrypt**: `/etc/letsencrypt/live/<domain>/`
- **Custom applications**: `/etc/<appname>/certs/` (CameoDB approach)

**Security best practices for production:**
```bash
# Create certificate directory
sudo mkdir -p /etc/cameodb/certs
sudo chmod 755 /etc/cameodb/certs

# Copy certificates with proper permissions
sudo cp cert.pem /etc/cameodb/certs/
sudo cp key.pem /etc/cameodb/certs/

# Set restrictive permissions
sudo chmod 644 /etc/cameodb/certs/cert.pem    # Certificate can be world-readable
sudo chmod 600 /etc/cameodb/certs/key.pem     # Private key must be restricted
sudo chown root:root /etc/cameodb/certs/*.pem
```

**Systemd service integration:**
When running CameoDB as a systemd service, ensure the service user can read the certificates:
```ini
[Service]
User=cameodb
Group=cameodb
# Add read access to certificates
ExecStartPre=/bin/chmod 644 /etc/cameodb/certs/cert.pem
ExecStartPre=/bin/chmod 640 /etc/cameodb/certs/key.pem
```

**Configuration with standard paths:**
```toml
[network.http.tls]
enabled = true
cert_file = "/etc/cameodb/certs/cert.pem"
key_file = "/etc/cameodb/certs/key.pem"
```

This approach follows the **Filesystem Hierarchy Standard (FHS)** and ensures proper isolation and security for production deployments.

### Client Connection with TLS

When connecting to a TLS-enabled CameoDB server:

```bash
# Interactive CLI with HTTPS
cameodb client --interactive --connect https://localhost:9480

# Only for a self-signed certificate you issued yourself, on a node you control
cameodb client --interactive --connect https://localhost:9480 --insecure
```

**`--insecure-source` for remote data sources:**
```bash
# Fetch a schema from an external HTTPS URL whose certificate does not validate
cameodb client schema detect https://external.com/schema.csv --insecure-source

# Same for a data load
cameodb client data load myindex https://external.com/data.csv --insecure-source
```

The two flags are deliberately separate and neither implies the other:

| Flag | Relaxes verification for |
|------|--------------------------|
| `--insecure` | the connection to the CameoDB server |
| `--insecure-source` | remote schema/data source URLs only |

Accepting an untrusted data source must not also stop verifying the connection carrying
your writes, which is what a single combined flag did.

### TLS Configuration Options

| Option | Description | Required |
|--------|-------------|----------|
| `enabled` | Enable/disable TLS (default: `false`) | No |
| `cert_file` | Path to PEM certificate file | Yes (when enabled) |
| `key_file` | Path to PEM private key file | Yes (when enabled) |

### Security Considerations

- **Certificate Validation**: Clients validate server certificates by default. `--insecure` accepts an unverified certificate, so use it only for a certificate you issued yourself on a node you control — never to work around a validation failure you did not expect.
- **TLS Stack**: rustls with the `ring` provider on both sides. Outbound HTTPS from the client verifies against the OS trust store (`rustls-platform-verifier`), so a corporate CA installed system-wide is honoured. No OpenSSL, vendored or otherwise.
- **Key Permissions**: Ensure private key files have restricted permissions (`chmod 600 key.pem`).
- **Certificate Rotation**: Update certificates before expiration; CameoDB requires restart to load new certificates.
- **Mutual TLS (mTLS)**: Not currently supported. All client connections are accepted if the TLS handshake succeeds.
- **Verification**: `scripts/validate/tls.sh` starts a real HTTPS listener and checks that bad certificate material fails before startup completes.

### Troubleshooting TLS

**Certificate errors**: Verify certificate and key files are valid PEM format:
```bash
openssl x509 -in cert.pem -text -noout  # Validate certificate
openssl rsa -in key.pem -check -noout   # Validate private key
```

**Connection refused**: Ensure the server is listening on HTTPS (check startup logs for "HTTPS Server starting on https://").

**Client certificate verification failed**: Use `--insecure` flag for self-signed certificates, or add the CA certificate to your system's trust store.

## 📚 Documentation

Dive deeper into CameoDB's architecture and APIs:

- 📖 **[HTTP API Reference](docs/API_REFERENCE.md)**: Full details on Search, Ingestion, and Index Management.
- 🏗️ **[Architecture & Routing](docs/ARCHITECTURE.md)**: How the distributed leaderless routing works under the hood.
- ⚙️ **[Configuration Guide](docs/CONFIGURATION.md)**: Adjusting shard counts, cache sizes, and swarm ports.
- 📦 **[Building & Packaging](docs/BUILDING.md)**: Instructions for compiling cross-platform binaries and generating RPM/DEB packages.
- 💻 **[Development Setup](docs/DEVELOPMENT.md)**: Getting a clean macOS or Linux machine ready to build, test and validate CameoDB.
- 🚢 **[Deployment](docs/DEPLOYMENT.md)**: Running CameoDB as a service, in containers, and as a cluster.
- 🧭 **[Architecture Decisions](docs/ADR.md)**: Why the system is shaped the way it is.
- 🧪 **[Scripts](scripts/README.md)**: Build, setup, validation and testing scripts, and what each one checks.
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
cameodb@localhost:9480 ▶ delete books --id 12345
cameodb@localhost:9480 ▶ admin memory stats
cameodb@localhost:9480 ▶ admin memory purge --force
```

### 🗜️ Supported Ingestion Formats
The CLI client features a robust, zero-copy ingestion pipeline that transparently handles:
- **Formats:** `CSV`, `TSV`, `JSON` (Documents/Arrays), and `JSONL/NDJSON`.
- **Compression:** Automatically detects and decompresses `Gzip (.gz)`, `Bzip2 (.bz2)`, `Zstd (.zst)`, `XZ (.xz)`, `LZ4 (.lz4)`, and `Deflate` formats on the fly.
- **Sources:** Ingest data from local disk files, mounted network paths, or by streaming directly from public `HTTP/HTTPS` URLs.

## 🔒 Security

### Authentication

Off by default; when enabled, every route except liveness requires
`Authorization: Bearer <key>`. Keys are minted by the server and stored only as SHA-256
digests, so a leaked config file contains nothing that can authenticate:

```bash
cameodb keygen --role writer --label team-a --allowed-indexes docs,wiki
```

The key is printed once to stdout and the `[[security.api_keys]]` stanza to stderr. Three
roles bundle four capabilities — `admin` (everything), `writer` (read and write), `reader`
(read only) — and `allowed_indexes` restricts a key to named indexes for every role.

`--key-out` and `--hash-out` write the two files the rest of the design already expects,
created `0600` and never overwritten:

```bash
cameodb keygen --role writer --label team-a \
  --key-out ~/.cameodb/team-a.key \        # for the client's --api-key-file
  --hash-out /etc/cameodb/keys/team-a       # for the config's key_hash_file
```

```toml
[security]
enabled = true

[[security.api_keys]]
key_hash = "sha256:…"          # or key_hash_file = "/etc/cameodb/keys/team-a"
role = "writer"
label = "team-a"
allowed_indexes = ["docs", "wiki"]
```

The bundled client presents a key three ways. A file or the environment is preferred over
`--api-key`, whose value is visible in `ps` on most systems:

```bash
cameodb client --api-key-file ~/.cameodb/team-a.key list indexes
export CAMEODB_API_KEY="cameo_v1_…"     # or CAMEODB_API_KEY_FILE
cameodb client search docs "invoice"
```

The client will not send a key over plaintext HTTP to anything but loopback — pass
`--allow-plaintext-key` when the hop is already protected by a tunnel or a mesh. In the
interactive shell, a key is bound to the origin it was given for: `connect` elsewhere drops
it rather than handing your credential to whatever host was typed, and `connect` back
restores it. `key file <path>`, `key show` and `key clear` change the credential mid-session.

Scoping holds everywhere a key can reach, not only where it names an index: `/_indexes` and
the MCP catalog list only the indexes a key may see, each MCP tool is checked against the
capability it needs and the index it names, and an MCP session may only be continued by the
key that opened it.

`tools/list` advertises only the tools a caller could actually call, so an agent is never
offered one that will be refused.

Not yet: an MCP client presents its key as an HTTP header, so a client that cannot set one
cannot authenticate. Keys are read at startup — adding or revoking one means add, migrate,
remove, restart. Every MCP tool today is a read; the capability table denies by default, so
a write tool added later fails closed until it is classified.

### Security profiles

Declare how far a node can be reached and the server enforces the rules that go with that
answer, refusing to start if the rest of the config contradicts it:

```toml
[node]
profile = "internal"   # local | internal | external
```

| | `local` | `internal` | `external` |
|---|---|---|---|
| Reachable from | this machine only | a trusted network | untrusted networks |
| Bind address | loopback only | any | any |
| TLS | optional | warned if off | **required** |
| CORS `"*"` | allowed (warned) | rejected | rejected |
| `/_admin/*` | allowed | allowed | **must be disabled** |
| Cluster PSK | warned | **required** | **required** |
| Authentication | optional | warned if off | **required** |

Choose by who can reach the bind address, not by what the environment is for. The names are
deliberately not `dev` / `staging` / `prod`: every rule keys off the bind address, so a
lifecycle name invites picking by what the box is *for* and being rejected for it. A test node
other people can reach is `internal`; `local` means loopback and nothing else, whether it is
running a test suite or a production single-node deployment on the same host as its client.

Omitting `profile` is valid only for a loopback bind, which infers `local`; a node reachable
from other hosts must state its posture. Check a config without starting the node:

```bash
cameodb check-config -c cameodb.toml
```

Defaults are loopback-bound with no cross-origin browser access.

### Other controls

- **TLS/HTTPS**: rustls with the `ring` provider (see [TLS Configuration](#tlshttps-configuration) above)
- **Cluster PSK**: membership gate for the libp2p swarm (XSalsa20 via `pnet`), configured with `[network.cluster] psk_file` or `psk`. It controls *who may join*; the transport is already encrypted by Noise regardless. Enabling it disables QUIC, since `pnet` wraps TCP only.
- **Request limits**: wire-level body limit, per-record cap, request timeout, and a concurrency guard that sheds with 503 while leaving `/_cluster/health` answerable.
- **Supply Chain**: `cargo audit` and `cargo deny` (config in `deny.toml`); advisory exceptions carry review dates that `scripts/validate/deps.sh` enforces.
- **Verification**: [`scripts/validate/`](scripts/validate/README.md) is the manual gate — there is no CI. Run it before a release and record the result in [RELEASE-CHECKLIST.md](RELEASE-CHECKLIST.md).

If you discover a vulnerability, please refer to our [Security Policy](.github/SECURITY.md).

## 🤝 Contributing

We welcome contributions! Whether it's adding new features, fixing bugs, or improving documentation, check out our [Contributing Guidelines](.github/CONTRIBUTING.md) to get started.

Please review our [Code of Conduct](.github/CODE_OF_CONDUCT.md) before participating in the community.

## 📄 License

This project is licensed under the [Apache License 2.0](LICENSE) - see the LICENSE file for details.
