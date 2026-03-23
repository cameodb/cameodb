# Builder image for cross-compilation distribution builds
# This image bakes in all heavy dependencies so they aren't downloaded every build
FROM rust:1.90-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    musl-tools \
    binutils \
    gcc-aarch64-linux-gnu \
    pkg-config \
    libssl-dev \
    rpm \
    build-essential \
    file \
    && rm -rf /var/lib/apt/lists/*

# Trust corporate CA certificate if provided (for TLS-intercepting proxies)
RUN --mount=type=secret,id=zscaler,dst=/usr/local/share/ca-certificates/zscaler.crt \
    if [ -f /usr/local/share/ca-certificates/zscaler.crt ]; then \
        cat /usr/local/share/ca-certificates/zscaler.crt >> /etc/ssl/certs/ca-certificates.crt && \
        update-ca-certificates; \
    fi

ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt
ENV CARGO_HTTP_CAINFO=/etc/ssl/certs/ca-certificates.crt

# Use curl for rustup downloads to better handle system CA certificates
ENV RUSTUP_USE_CURL=1

RUN rustup target add x86_64-unknown-linux-musl
RUN rustup target add aarch64-unknown-linux-musl

RUN cargo install cargo-deb cargo-generate-rpm

WORKDIR /workspace
