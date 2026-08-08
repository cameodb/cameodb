# 🐳 CameoDB Docker Architecture

## 📊 Docker Implementation Analysis

### **Patterns used in CameoDB**
The following patterns are adopted for CameoDB:

#### **✅ Multi-Stage Build Strategy**
- **Builder Stage**: Full Rust toolchain with cross-compilation support
- **Runtime Stage**: Minimal distroless image for security and efficiency
- **Layer Caching**: Optimized copying order (manifests first, then source)

#### **✅ Cross-Platform Support**
- **Architecture Detection**: Automatic `TARGETARCH` handling for amd64/arm64
- **Musl Static Linking**: Self-contained binaries with no runtime dependencies
- **Cross-Compilation**: GCC toolchains for ARM64 builds on amd64 hosts

#### **✅ Security Best Practices**
- **Distroless Base**: `gcr.io/distroless/static:latest` - no shell, minimal attack surface
- **Non-Root User**: Uses `nonroot:nonroot` user for container execution
- **Minimal Dependencies**: Only essential files copied to final image

## 🏗️ CameoDB Docker Architecture

### **File Structure**
```
cameodb/
├── Dockerfile                     # Multi-stage Dockerfile for production builds
├── .dockerignore                  # Files to exclude from the build context
└── docker/
    ├── cameodb-docker.toml        # Container-optimized configuration
    ├── docker-compose.yml         # Single-node deployment configuration
    ├── docker-compose-cluster.yml # 3-node cluster deployment configuration
    └── README.md                  # Docker-specific documentation
```

### **Multi-Stage Build Process**

#### **Builder Stage Features**
```dockerfile
# Rust 1.94 with musl/gnu static linking
ARG RUST_VERSION=1.94
ARG TARGET_ABI=musl
FROM rust:${RUST_VERSION}-slim AS builder

RUN rustup default ${RUST_VERSION}

# Cross-compilation support and SSL dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl-dev \
    musl-tools \
    gcc-aarch64-linux-gnu \
    perl \
    make \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Corporate CA certificate support (any TLS-intercepting proxy: Zscaler, Netskope, …)
RUN --mount=type=secret,id=corporate-ca,dst=/usr/local/share/ca-certificates/corporate-ca.crt \
    if [ -f /usr/local/share/ca-certificates/corporate-ca.crt ]; then \
        cat /usr/local/share/ca-certificates/corporate-ca.crt >> /etc/ssl/certs/ca-certificates.crt && \
        update-ca-certificates; \
    fi

# Optimized caching with mount cache
WORKDIR /src
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    set -e; \
    export SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt; \
    export PKG_CONFIG_ALLOW_CROSS=1; \
    TARGET_TRIPLE=""; \
    if [ "${TARGET_ABI}" = "musl" ]; then \
        export OPENSSL_STATIC=1; \
        case "${TARGETARCH}" in \
            "amd64") TARGET_TRIPLE="x86_64-unknown-linux-musl";; \
            "arm64") TARGET_TRIPLE="aarch64-unknown-linux-musl";; \
            *) echo "Unsupported architecture: ${TARGETARCH}"; exit 1;; \
        esac; \
    else \
        case "${TARGETARCH}" in \
            "amd64") TARGET_TRIPLE="x86_64-unknown-linux-gnu";; \
            "arm64") TARGET_TRIPLE="aarch64-unknown-linux-gnu";; \
            *) echo "Unsupported architecture: ${TARGETARCH}"; exit 1;; \
        esac; \
    fi; \
    rustup target add "${TARGET_TRIPLE}"; \
    cargo build --profile release-docker --target "${TARGET_TRIPLE}" --bin cameodb \
        --no-default-features; \
    cp "/src/target/${TARGET_TRIPLE}/release-docker/cameodb" /src/cameodb
```

#### **Runtime Stage Features**
```dockerfile
# Conditional distroless runtime based on TARGET_ABI
ARG TARGET_ABI
FROM gcr.io/distroless/static:nonroot AS runtime-musl
FROM gcr.io/distroless/cc-debian12:nonroot AS runtime-gnu
FROM runtime-${TARGET_ABI} AS runtime

# Security-first approach (USER already set by :nonroot base)
USER nonroot:nonroot

# Essential configuration only
COPY --from=builder --chown=nonroot:nonroot /src/cameodb /usr/local/bin/cameodb
COPY --chown=nonroot:nonroot docker/cameodb-docker.toml /etc/cameodb/cameodb.toml
COPY --from=builder --chown=nonroot:nonroot /build-data/cameodb /data/cameodb

ENV CAMEODB_CONFIG=/etc/cameodb/cameodb.toml
ENV CAMEODB_DATA_DIR=/data/cameodb

VOLUME ["/data/cameodb"]

EXPOSE 9480 9580

# Health check integration
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD ["/usr/local/bin/cameodb", "--version"]

ENTRYPOINT ["/usr/local/bin/cameodb"]
CMD ["--config", "/etc/cameodb/cameodb.toml"]
```

### **Distributed Deployment Architecture**

#### **3-Node Cluster Configuration**
```yaml
services:
  cameodb-node1: # Primary node
    ports: ["9481:9480", "9581:9580"]
    user: "65532:65532"
    environment:
      - RUST_LOG=debug
      - CAMEODB_NODE_LABEL=cameodb-node-1
      - CAMEODB_CLUSTER_NAME=cameodb-production
      - CAMEODB_CLUSTER_ENABLED=true
      - CAMEODB_HTTP_BIND_ADDRESS=cameodb-node1
      - CAMEODB_HTTP_PORT=9480
      - CAMEODB_CLUSTER_BIND_ADDRESS=cameodb-node1
      - CAMEODB_CLUSTER_PORT=9580
      - CAMEODB_SEED_NODES=cameodb-node1:9580,cameodb-node2:9580
      - CAMEODB_CLUSTER_NODES=cameodb-node1:9580,cameodb-node2:9580,cameodb-node3:9580
      - CAMEODB_MAX_BODY_SIZE_MB=200
      - CAMEODB_DEFAULT_BATCH_SIZE=5000
    networks:
      cameodb-cluster:
        ipv4_address: 172.20.0.10
      
  cameodb-node2: # Secondary node
    ports: ["9482:9480", "9582:9580"]
    user: "65532:65532"
    depends_on:
      cameodb-node1:
        condition: service_healthy
    environment:
      - RUST_LOG=debug
      - CAMEODB_NODE_LABEL=cameodb-node-2
      - CAMEODB_CLUSTER_NAME=cameodb-production
      - CAMEODB_CLUSTER_ENABLED=true
      - CAMEODB_HTTP_BIND_ADDRESS=cameodb-node2
      - CAMEODB_HTTP_PORT=9480
      - CAMEODB_CLUSTER_BIND_ADDRESS=cameodb-node2
      - CAMEODB_CLUSTER_PORT=9580
      - CAMEODB_SEED_NODES=cameodb-node1:9580,cameodb-node2:9580
      - CAMEODB_CLUSTER_NODES=cameodb-node1:9580,cameodb-node2:9580,cameodb-node3:9580
      - CAMEODB_MAX_BODY_SIZE_MB=200
      - CAMEODB_DEFAULT_BATCH_SIZE=5000
    networks:
      cameodb-cluster:
        ipv4_address: 172.20.0.11
      
  cameodb-node3: # Secondary node  
    ports: ["9483:9480", "9583:9580"]
    user: "65532:65532"
    depends_on:
      cameodb-node1:
        condition: service_healthy
    environment:
      - RUST_LOG=debug
      - CAMEODB_NODE_LABEL=cameodb-node-3
      - CAMEODB_CLUSTER_NAME=cameodb-production
      - CAMEODB_CLUSTER_ENABLED=true
      - CAMEODB_HTTP_BIND_ADDRESS=cameodb-node3
      - CAMEODB_HTTP_PORT=9480
      - CAMEODB_CLUSTER_BIND_ADDRESS=cameodb-node3
      - CAMEODB_CLUSTER_PORT=9580
      - CAMEODB_SEED_NODES=cameodb-node1:9580,cameodb-node2:9580
      - CAMEODB_CLUSTER_NODES=cameodb-node1:9580,cameodb-node2:9580,cameodb-node3:9580
      - CAMEODB_MAX_BODY_SIZE_MB=200
      - CAMEODB_DEFAULT_BATCH_SIZE=5000
    networks:
      cameodb-cluster:
        ipv4_address: 172.20.0.12
    
  nginx-lb: # Nginx load balancer with upstream cluster
    image: nginx:alpine
    ports: ["9480:80"]
    networks:
      - cameodb-cluster
    depends_on:
      - cameodb-node1
      - cameodb-node2
      - cameodb-node3
    # Inline nginx config proxying to internal node addresses
    # client_max_body_size: 200M for large batch writes
```

#### **Network Topology**
```
Docker Host (macOS)
├── External Access
│   ├── :9481 → Node 1 HTTP
│   ├── :9482 → Node 2 HTTP  
│   ├── :9483 → Node 3 HTTP
│   └── :9480 → Load Balancer
├── Cluster Network (172.20.0.0/16)
│   ├── cameodb-node1 (172.20.0.10)
│   ├── cameodb-node2 (172.20.0.11)
│   └── cameodb-node3 (172.20.0.12)
└── Internal Communication
    ├── :9480 → HTTP API (internal)
    └── :9580 → Cluster/Kameo (internal)
```

## 🎯 Key Optimizations

### **Build Performance**
- **Layer Caching**: Cargo dependencies cached separately from source
- **Multi-Platform**: Single Dockerfile handles amd64/arm64
- **Docker Buildx**: Concurrent multi-architecture builds supported

### **Runtime Efficiency**  
- **Static Binary**: ~10MB final image size
- **Distroless**: Minimal attack surface, no package manager
- **Resource Limits**: Designed for 512MB RAM per container

### **Security Features**
- **Non-Root Execution**: All processes run as `nonroot` user
- **Immutable Base**: Distroless prevents runtime modifications  
- **Minimal Dependencies**: Only essential files in final image

## 🚀 Usage Examples

### **Single-Node Deployment**
```bash
# From the project root
mkdir -p data/cameodb
docker-compose -f docker/docker-compose.yml up -d --build
```

### **Multi-Node Cluster**
```bash
# From the project root
mkdir -p data/cameodb/node{1,2,3}
docker-compose -f docker/docker-compose-cluster.yml up -d --build
```

### **Cross-Platform Builds**
```bash
# Build for Apple Silicon Macs
docker buildx build --platform linux/arm64 -t cameodb:arm64 .

# Build for Intel/AMD systems  
docker buildx build --platform linux/amd64 -t cameodb:amd64 .

# Multi-platform registry push
docker buildx build --platform linux/amd64,linux/arm64 \
  -t registry.example.com/cameodb:latest --push .
```

## 📊 Performance Characteristics

### **Build Times**
- **Cold Build**: ~5-8 minutes (depends on dependencies)
- **Incremental**: ~1-2 minutes (with layer caching)
- **Multi-Platform**: ~10-15 minutes (parallel builds)

### **Image Sizes**
- **Builder Image**: ~1.2GB (with Rust toolchain)
- **Final Image**: ~15-20MB (static binary + config)
- **Registry Transfer**: <50MB compressed

### **Runtime Resources**
- **Memory**: 512MB minimum per node (1.5GB total for cluster)
- **CPU**: 1/3 allocation per node (4+ cores recommended)
- **Storage**: Persistent volumes for data durability

This Docker architecture provides production-ready containerization following industry best practices while optimizing for CameoDB's distributed actor model and hybrid storage requirements.
