# syntax=docker/dockerfile:1

################################################################################
# STAGE 1: Builder (Needs Internet & Certs)
################################################################################
ARG RUST_VERSION=1.90
FROM rust:${RUST_VERSION}-slim AS builder

# 1. Install system build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    musl-tools \
    gcc-aarch64-linux-gnu \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# 2. MOUNT SECRET (Transient)
# This mounts the file from your host to the destination ONLY for this command.
# It is NOT saved to the image layer.
RUN --mount=type=secret,id=zscaler,dst=/usr/local/share/ca-certificates/Zscaler.crt \
    update-ca-certificates

# 3. Configure Cargo to use the system store (which now trusts Zscaler in memory)
RUN mkdir -p /usr/local/cargo && \
    echo '[http]' >> /usr/local/cargo/config.toml && \
    echo 'cainfo = "/etc/ssl/certs/ca-certificates.crt"' >> /usr/local/cargo/config.toml

RUN rustup default ${RUST_VERSION}

ARG TARGETARCH
WORKDIR /src

# 4. Linker Configuration
RUN mkdir -p .cargo && \
    echo '[target.x86_64-unknown-linux-musl]' >> .cargo/config.toml && \
    echo 'linker = "musl-gcc"' >> .cargo/config.toml && \
    echo '[target.aarch64-unknown-linux-musl]' >> .cargo/config.toml && \
    echo 'linker = "aarch64-linux-gnu-gcc"' >> .cargo/config.toml

# 5. Prepare Data Directory (Permission Fix)
RUN mkdir -p /build-data/cameodb

COPY Cargo.toml Cargo.lock ./
COPY crates/ ./crates/

# 6. Build (Uses the cert implicitly via cargo)
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    set -e; \
    export SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt; \
    export OPENSSL_STATIC=1; \
    export PKG_CONFIG_ALLOW_CROSS=1; \
    TARGET_TRIPLE=""; \
    case "${TARGETARCH}" in \
        "amd64") TARGET_TRIPLE="x86_64-unknown-linux-musl";; \
        "arm64") TARGET_TRIPLE="aarch64-unknown-linux-musl";; \
        *) echo "Unsupported architecture: ${TARGETARCH}"; exit 1;; \
    esac; \
    rustup target add "${TARGET_TRIPLE}"; \
    cargo build --release --target "${TARGET_TRIPLE}" --bin cameodb; \
    cp "/src/target/${TARGET_TRIPLE}/release/cameodb" /src/cameodb;

################################################################################
# STAGE 2: Runtime (Offline / Clean)
################################################################################
FROM gcr.io/distroless/static:latest AS runtime

# We COPY the binary and the config.
# We DO NOT copy the certificates.
COPY --from=builder --chown=nonroot:nonroot /src/cameodb /usr/local/bin/cameodb
COPY --chown=nonroot:nonroot docker/cameodb-docker.toml /etc/cameodb/cameodb.toml
COPY --from=builder --chown=nonroot:nonroot /build-data/cameodb /data/cameodb

ENV CAMEODB_CONFIG=/etc/cameodb/cameodb.toml
ENV CAMEODB_DATA_DIR=/data/cameodb

VOLUME ["/data/cameodb"]

EXPOSE 9480 9580

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD ["/usr/local/bin/cameodb", "--version"]

USER nonroot:nonroot

ENTRYPOINT ["/usr/local/bin/cameodb"]
CMD ["--config", "/etc/cameodb/cameodb.toml"]