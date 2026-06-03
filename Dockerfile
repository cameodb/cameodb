# syntax=docker/dockerfile:1

################################################################################
# STAGE 1: Builder
################################################################################
ARG RUST_VERSION=1.95
ARG TARGET_ABI=musl
FROM rust:${RUST_VERSION}-slim AS builder

ARG TARGET_ABI
ARG TARGETARCH

# 1. Install system build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    xz-utils \
    libssl-dev \
    musl-tools \
    gcc-aarch64-linux-gnu \
    perl \
    make \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# 2. Trust corporate CA certificate (if provided)
RUN --mount=type=secret,id=zscaler,dst=/usr/local/share/ca-certificates/zscaler.crt \
    if [ -f /usr/local/share/ca-certificates/zscaler.crt ]; then \
        echo "Zscaler certificate detected, adding to CA bundle..." && \
        mkdir -p /etc/ssl/certs && \
        cat /usr/local/share/ca-certificates/zscaler.crt >> /etc/ssl/certs/ca-certificates.crt && \
        update-ca-certificates && \
        echo "CA certificates updated successfully"; \
    else \
        echo "No Zscaler certificate provided, using system defaults"; \
    fi
ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt
ENV CURL_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt
ENV REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt
ENV CARGO_HTTP_CAINFO=/etc/ssl/certs/ca-certificates.crt

# 3. Configure Cargo to use system CA store
RUN mkdir -p /usr/local/cargo && \
    echo '[http]' >> /usr/local/cargo/config.toml && \
    echo 'cainfo = "/etc/ssl/certs/ca-certificates.crt"' >> /usr/local/cargo/config.toml

RUN rustup default ${RUST_VERSION}

WORKDIR /src

# 4. Configure target-specific linker
RUN mkdir -p .cargo && \
    if [ "$TARGET_ABI" = "musl" ] && [ "$TARGETARCH" = "amd64" ]; then \
        echo '[target.x86_64-unknown-linux-musl]' >> .cargo/config.toml && \
        echo 'linker = "musl-gcc"' >> .cargo/config.toml; \
    elif [ "$TARGET_ABI" = "musl" ] && [ "$TARGETARCH" = "arm64" ]; then \
        echo '[target.aarch64-unknown-linux-musl]' >> .cargo/config.toml && \
        echo 'linker = "musl-gcc"' >> .cargo/config.toml && \
        echo 'rustflags = ["-C", "target-feature=+crt-static"]' >> .cargo/config.toml; \
    elif [ "$TARGET_ABI" = "gnu" ] && [ "$TARGETARCH" = "arm64" ]; then \
        echo '[target.aarch64-unknown-linux-gnu]' >> .cargo/config.toml && \
        echo 'linker = "aarch64-linux-gnu-gcc"' >> .cargo/config.toml; \
    fi

# 5. Prepare data directory
RUN mkdir -p /build-data/cameodb

COPY Cargo.toml Cargo.lock ./
COPY crates/ ./crates/

# 6. Build — uses release-docker profile (thin LTO for memory-constrained builders)
#    Profile defined in Cargo.toml: inherits release with lto="thin", codegen-units=4
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    set -e; \
    export SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt; \
    export CURL_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt; \
    export REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt; \
    export CARGO_HTTP_CAINFO=/etc/ssl/certs/ca-certificates.crt; \
    export CARGO_HTTP_CHECK_REVOKE=false; \
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
    rustup target add "${TARGET_TRIPLE}" || \
    rustup component add rust-std --target "${TARGET_TRIPLE}" || ( \
        echo "rustup failed, trying manual download..."; \
        RUST_STD_URL="https://static.rust-lang.org/dist/2026-04-16/rust-std-1.95.0-${TARGET_TRIPLE}.tar.xz"; \
        curl -k -L -o /tmp/rust-std.tar.xz "${RUST_STD_URL}" && \
        mkdir -p /tmp/rust-std && \
        tar -xf /tmp/rust-std.tar.xz -C /tmp/rust-std && \
        /tmp/rust-std/rust-std-1.95.0-${TARGET_TRIPLE}/install.sh --prefix=$(rustup show home) && \
        rustup target add "${TARGET_TRIPLE}"; \
    ); \
    cargo build --profile release-docker --target "${TARGET_TRIPLE}" --bin cameodb \
        --no-default-features \
        --features client/native-tls-vendored; \
    cp "/src/target/${TARGET_TRIPLE}/release-docker/cameodb" /src/cameodb;

################################################################################
# STAGE 2: Runtime (Offline / Clean)
################################################################################
# Use static distroless for musl (fully static), cc-debian12 for gnu (needs glibc + libgcc)
ARG TARGET_ABI
FROM gcr.io/distroless/static:nonroot AS runtime-musl
FROM gcr.io/distroless/cc-debian12:nonroot AS runtime-gnu
FROM runtime-${TARGET_ABI} AS runtime

# We COPY the binary and the config.
# We DO NOT copy the certificates.
COPY --from=builder --chown=nonroot:nonroot /src/cameodb /usr/local/bin/cameodb
COPY --chown=nonroot:nonroot docker/cameodb-docker.toml /etc/cameodb/cameodb.toml
COPY --from=builder --chown=nonroot:nonroot /build-data/cameodb /data/cameodb

ENV CAMEODB_CONFIG=/etc/cameodb/cameodb.toml
ENV CAMEODB_DATA_DIR=/data/cameodb

# Set user before VOLUME to ensure proper ownership
USER nonroot:nonroot

VOLUME ["/data/cameodb"]

EXPOSE 9480 9580

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD ["/usr/local/bin/cameodb", "--version"]

ENTRYPOINT ["/usr/local/bin/cameodb"]
CMD ["--config", "/etc/cameodb/cameodb.toml"]