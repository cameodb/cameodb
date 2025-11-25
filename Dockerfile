# syntax=docker/dockerfile:1

################################################################################
# Builder Stage
#
# Multi-stage build for CameoDB with cross-platform support (amd64/arm64).
# Uses Rust musl targets for static compilation and minimal runtime dependencies.
################################################################################
ARG RUST_VERSION=1.75
FROM rust:${RUST_VERSION}-slim AS builder

# Force rustup to use the specific Rust version
RUN rustup default ${RUST_VERSION}

# Install build dependencies for static compilation
# - musl-tools: Static linking support
# - gcc-aarch64-linux-gnu: ARM64 cross-compilation
# - pkg-config: Dependency discovery
# - libssl-dev: SSL/TLS support (if needed)
RUN apt-get update && apt-get install -y --no-install-recommends \
    musl-tools \
    gcc-aarch64-linux-gnu \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# TARGETARCH is automatically provided by Docker BuildKit
ARG TARGETARCH
WORKDIR /src

# Configure cargo for cross-compilation with musl targets
RUN mkdir -p .cargo && \
    echo '[target.x86_64-unknown-linux-musl]' >> .cargo/config.toml && \
    echo 'linker = "musl-gcc"' >> .cargo/config.toml && \
    echo '[target.aarch64-unknown-linux-musl]' >> .cargo/config.toml && \
    echo 'linker = "aarch64-linux-gnu-gcc"' >> .cargo/config.toml

# Copy manifests first for better Docker layer caching
COPY Cargo.toml Cargo.lock ./

# Copy workspace configuration and source code
COPY crates/ ./crates/
COPY cameodb.toml ./

# Build the server binary for the target architecture
# Use build cache mounts for faster subsequent builds
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
    echo "Building CameoDB for architecture: ${TARGETARCH}, using target: ${TARGET_TRIPLE}"; \
    \
    rustup target add "${TARGET_TRIPLE}"; \
    cargo build --release --target "${TARGET_TRIPLE}" --bin server; \
    \
    # Copy binary to consistent location for final stage
    cp "/src/target/${TARGET_TRIPLE}/release/server" /src/cameodb;

################################################################################
# Final Runtime Stage
#
# Minimal production image using distroless base for security.
# Contains only the CameoDB binary and essential configuration.
################################################################################
FROM gcr.io/distroless/static:latest AS runtime

# Create directories for CameoDB data and configuration
# Note: distroless doesn't have shell, so we do this in the builder if needed
COPY --from=builder --chown=nonroot:nonroot /src/cameodb /usr/local/bin/cameodb

# Copy default configuration file
COPY --chown=nonroot:nonroot cameodb.toml /etc/cameodb/cameodb.toml

# Copy sample data if it exists (optional)
# COPY --chown=nonroot:nonroot data/ /data/

# Use non-root user for security (distroless provides 'nonroot' user)
USER nonroot:nonroot

# Set environment variables
ENV CAMEODB_CONFIG=/etc/cameodb/cameodb.toml
ENV CAMEODB_DATA_DIR=/data/cameodb-data

# Expose ports for HTTP API and cluster communication
EXPOSE 9480 9580

# Health check (distroless doesn't have curl, so we use a simple approach)
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD ["/usr/local/bin/cameodb", "--version"]

# Set the entrypoint
ENTRYPOINT ["/usr/local/bin/cameodb"]
CMD ["--config", "/etc/cameodb/cameodb.toml"]
