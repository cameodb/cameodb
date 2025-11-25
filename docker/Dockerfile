# syntax=docker/dockerfile:1
#
# Dockerfile for CameoDB Distributed Deployment
# Optimized for Docker Compose multi-node setup

################################################################################
# Builder Stage - Same as main Dockerfile but with distributed optimizations
################################################################################
ARG RUST_VERSION=1.75
FROM rust:${RUST_VERSION}-slim AS builder

RUN rustup default ${RUST_VERSION}

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    musl-tools \
    gcc-aarch64-linux-gnu \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

ARG TARGETARCH
WORKDIR /src

# Configure cargo for cross-compilation
RUN mkdir -p .cargo && \
    echo '[target.x86_64-unknown-linux-musl]' >> .cargo/config.toml && \
    echo 'linker = "musl-gcc"' >> .cargo/config.toml && \
    echo '[target.aarch64-unknown-linux-musl]' >> .cargo/config.toml && \
    echo 'linker = "aarch64-linux-gnu-gcc"' >> .cargo/config.toml

# Copy workspace files (from parent directory context)
COPY Cargo.toml Cargo.lock ./
COPY crates/ ./crates/
COPY cameodb.toml ./

# Build with distributed features enabled
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    set -e; \
    \
    TARGET_TRIPLE=""; \
    case "${TARGETARCH}" in \
        "amd64") TARGET_TRIPLE="x86_64-unknown-linux-musl";; \
        "arm64") TARGET_TRIPLE="aarch64-unknown-linux-musl";; \
        *) echo "Unsupported architecture: ${TARGETARCH}"; exit 1;; \
    esac; \
    \
    echo "Building CameoDB (distributed) for: ${TARGETARCH}, target: ${TARGET_TRIPLE}"; \
    \
    rustup target add "${TARGET_TRIPLE}"; \
    cargo build --release --target "${TARGET_TRIPLE}" --bin server; \
    \
    cp "/src/target/${TARGET_TRIPLE}/release/server" /src/cameodb;

################################################################################
# Runtime Stage - Optimized for distributed cluster deployment
################################################################################
FROM gcr.io/distroless/static:latest AS runtime

# Copy binary
COPY --from=builder --chown=nonroot:nonroot /src/cameodb /usr/local/bin/cameodb

# Copy configuration template (will be overridden by environment variables)
COPY --chown=nonroot:nonroot cameodb.toml /etc/cameodb/cameodb.toml

# Create data directory
USER nonroot:nonroot

# Environment variables for distributed deployment
ENV CAMEODB_CONFIG=/etc/cameodb/cameodb.toml
ENV CAMEODB_DATA_DIR=/data/cameodb-data
ENV RUST_LOG=info
ENV RUST_BACKTRACE=1

# Expose ports
EXPOSE 9480 9580

# Health check for cluster readiness
HEALTHCHECK --interval=15s --timeout=5s --start-period=30s --retries=3 \
    CMD ["/usr/local/bin/cameodb", "--version"]

# Entrypoint optimized for container orchestration
ENTRYPOINT ["/usr/local/bin/cameodb"]
CMD ["--config", "/etc/cameodb/cameodb.toml"]
