# CameoDB

**The search database engine for AI.** An ACID key-value store, a document model and a full-text search engine in one Rust binary, with an MCP surface agents can search through.

This image runs a single CameoDB node. It is a minimal distroless image, runs as the non-root user `65532`, and is built for `linux/amd64` and `linux/arm64`.

## Quick start

```bash
mkdir -p "$(pwd)/data/cameodb"
chown -R 65532:65532 "$(pwd)/data/cameodb"

docker run -d \
  --name cameodb \
  -p 9480:9480 \
  -p 9580:9580 \
  -v "$(pwd)/data/cameodb:/data/cameodb" \
  cameodb/cameodb:latest
```

The HTTP API is now on `http://localhost:9480`. Health check: `curl http://localhost:9480/_cluster/health`.

Open the interactive CLI against it:

```bash
docker run -it --rm --network host \
  cameodb/cameodb:latest \
  client --interactive --connect http://localhost:9480
```

## Tags

| Tag | Meaning |
|---|---|
| `latest` | the newest release |
| `X.Y.Z` | a specific release, e.g. `0.3.10`; pin this in production |

Read the [changelog](https://github.com/cameodb/cameodb/blob/main/CHANGELOG.md) before upgrading: a release can change what it writes to the data directory, and not every release can be rolled back.

## Ports

| Port | Use |
|---|---|
| `9480` | HTTP API |
| `9580` | cluster (peer-to-peer) traffic; publish it only for a multi-node cluster |

## Volumes and configuration

| Path | Use |
|---|---|
| `/data/cameodb` | the data directory. Mount a volume here, owned by uid `65532`, or the data is lost with the container |
| `/etc/cameodb/cameodb.toml` | the config. The image carries a default; mount your own over it to change it |

```bash
docker run -d --name cameodb -p 9480:9480 \
  -v "$(pwd)/cameodb.toml:/etc/cameodb/cameodb.toml:ro" \
  -v "$(pwd)/data/cameodb:/data/cameodb" \
  cameodb/cameodb:latest
```

Check a config before starting a node with it:

```bash
docker run --rm -v "$(pwd)/cameodb.toml:/etc/cameodb/cameodb.toml:ro" \
  cameodb/cameodb:latest check-config -c /etc/cameodb/cameodb.toml
```

The default config binds `0.0.0.0` and declares `profile = "internal"`. A node that binds a non-loopback address must declare how far it can be reached (`local`, `internal` or `external`), and a config that contradicts the profile refuses to start.

## Authentication

Authentication is off by default. Turn it on for anything other than a throwaway node. A node holds SHA-256 digests of keys, never the keys themselves:

```bash
# Create a key and its digest (the key is shown once; keep it)
docker run --rm -v "$(pwd)/keys:/keys" cameodb/cameodb:latest \
  keygen --role admin --label ops --hash-out /keys/ops

# Start the node with that one key's digest
docker run -d --name cameodb -p 9480:9480 \
  -e CAMEODB_SECURITY_ENABLED=true \
  -e CAMEODB_API_KEY_HASH=sha256:<digest> \
  -e CAMEODB_API_KEY_ROLE=admin \
  -v "$(pwd)/data/cameodb:/data/cameodb" \
  cameodb/cameodb:latest
```

With authentication on, every route except `/_cluster/health` requires a key. Make sure digest files you mount are readable by uid `65532`.

## Docker Compose

```yaml
services:
  cameodb:
    image: cameodb/cameodb:latest
    ports:
      - "9480:9480"
    user: "65532:65532"
    environment:
      - RUST_LOG=info
    volumes:
      - ./data/cameodb:/data/cameodb
```

A three-node cluster example is in the repository's [docker/](https://github.com/cameodb/cameodb/tree/main/docker) directory.

## Documentation

- [Deployment guide](https://github.com/cameodb/cameodb/blob/main/docs/DEPLOYMENT.md)
- [Configuration reference](https://github.com/cameodb/cameodb/blob/main/docs/CONFIGURATION.md)
- [API reference](https://github.com/cameodb/cameodb/blob/main/docs/API_REFERENCE.md)
- [MCP server](https://github.com/cameodb/cameodb/blob/main/docs/MCP.md)

## License

Multi-license: the core crates are Apache-2.0, the client SDK and bench harness are MIT, and the server is FSL-1.1-ALv2 (converts to Apache-2.0 two years after release). See [LICENSE](https://github.com/cameodb/cameodb/blob/main/LICENSE).
