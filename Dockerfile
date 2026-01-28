# syntax=docker/dockerfile:1

################################################################################
# STAGE 1: Builder (Needs Internet & Certs)
################################################################################
ARG RUST_VERSION=1.90
ARG TARGET_ABI=gnu
ARG USE_ZIG=false
FROM rust:${RUST_VERSION}-slim AS builder

# Forward build args to inside the builder stage
ARG TARGET_ABI
ARG USE_ZIG

# 1. Install system build dependencies (Zig optional)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl-dev \
    musl-tools \
    gcc-aarch64-linux-gnu \
    pkg-config \
    wget \
    xz-utils \
    && rm -rf /var/lib/apt/lists/*

# 2. Trust Zscaler (if provided) before any downloads
RUN --mount=type=secret,id=zscaler,dst=/usr/local/share/ca-certificates/Zscaler.crt \
    update-ca-certificates

# 2.1. Also ensure Cargo trusts the cert bundle
RUN --mount=type=secret,id=zscaler,dst=/usr/local/share/ca-certificates/Zscaler.crt \
    mkdir -p /etc/ssl/certs && \
    cat /usr/local/share/ca-certificates/Zscaler.crt >> /etc/ssl/certs/ca-certificates.crt

ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt

# 3. Install Zig (provides complete C toolchain for musl cross-compilation). Skip when TARGET_ABI=gnu.
ARG ZIG_VERSION=0.13.0
RUN if [ "$USE_ZIG" = "true" ] && [ "$TARGET_ABI" = "musl" ]; then \
    wget -q "https://ziglang.org/download/${ZIG_VERSION}/zig-linux-$(uname -m)-${ZIG_VERSION}.tar.xz" -O zig.tar.xz && \
    tar -xJf zig.tar.xz && \
    mv zig-linux-$(uname -m)-${ZIG_VERSION} /usr/local/zig && \
    rm zig.tar.xz && \
    ln -s /usr/local/zig/zig /usr/local/bin/zig; \
    fi

# 3. Configure Cargo to use the system store (which now trusts Zscaler in memory)
RUN mkdir -p /usr/local/cargo && \
    echo '[http]' >> /usr/local/cargo/config.toml && \
    echo 'cainfo = "/etc/ssl/certs/ca-certificates.crt"' >> /usr/local/cargo/config.toml

RUN rustup default ${RUST_VERSION}

ARG TARGETARCH
WORKDIR /src

# 4. Configure Cargo toolchain
RUN mkdir -p .cargo && \
    if [ "$TARGET_ABI" = "musl" ] && [ "$USE_ZIG" = "true" ] && [ "$TARGETARCH" = "amd64" ]; then \
        echo '[target.x86_64-unknown-linux-musl]' >> .cargo/config.toml && \
        echo 'linker = "zig"' >> .cargo/config.toml && \
        echo 'rustflags = ["-C", "link-arg=cc", "-C", "link-arg=-target", "-C", "link-arg=x86_64-linux-musl"]' >> .cargo/config.toml; \
    elif [ "$TARGET_ABI" = "musl" ] && [ "$TARGETARCH" = "amd64" ]; then \
        echo '[target.x86_64-unknown-linux-musl]' >> .cargo/config.toml && \
        echo 'linker = "musl-gcc"' >> .cargo/config.toml; \
    elif [ "$TARGET_ABI" = "musl" ] && [ "$TARGETARCH" = "arm64" ]; then \
        echo '[target.aarch64-unknown-linux-musl]' >> .cargo/config.toml && \
        echo 'linker = "aarch64-linux-gnu-gcc"' >> .cargo/config.toml; \
    elif [ "$TARGET_ABI" = "gnu" ] && [ "$TARGETARCH" = "arm64" ]; then \
        echo '[target.aarch64-unknown-linux-gnu]' >> .cargo/config.toml && \
        echo 'linker = "aarch64-linux-gnu-gcc"' >> .cargo/config.toml; \
    fi

# 5. Set environment variables for OpenSSL vendored build when using Zig (only for x86_64)
ENV CC_x86_64_unknown_linux_musl="zig cc -target x86_64-linux-musl" \
    AR_x86_64_unknown_linux_musl="zig ar" \
    CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER="zig"

# 5. Prepare Data Directory (Permission Fix)
RUN mkdir -p /build-data/cameodb

COPY Cargo.toml Cargo.lock ./
COPY crates/ ./crates/

# 6. Build (Uses the cert implicitly via cargo)
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
        rustup target add "${TARGET_TRIPLE}"; \
        cargo build --release --target "${TARGET_TRIPLE}" --bin cameodb \
            --no-default-features \
            --features client/native-tls-vendored; \
    else \
        case "${TARGETARCH}" in \
            "amd64") TARGET_TRIPLE="x86_64-unknown-linux-gnu";; \
            "arm64") TARGET_TRIPLE="aarch64-unknown-linux-gnu";; \
            *) echo "Unsupported architecture: ${TARGETARCH}"; exit 1;; \
        esac; \
        rustup target add "${TARGET_TRIPLE}"; \
        cargo build --release --target "${TARGET_TRIPLE}" --bin cameodb; \
    fi; \
    cp "/src/target/${TARGET_TRIPLE}/release/cameodb" /src/cameodb;

################################################################################
# STAGE 2: Runtime (Offline / Clean)
################################################################################
# Use static distroless for musl (fully static), cc-debian12 for gnu (needs glibc + libgcc)
ARG TARGET_ABI
FROM gcr.io/distroless/static:latest AS runtime-musl
FROM gcr.io/distroless/cc-debian12:latest AS runtime-gnu
FROM runtime-${TARGET_ABI} AS runtime

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