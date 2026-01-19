# Cross-Compilation Build Guide

## Building for x86_64-unknown-linux-musl

### Recommended: Using native-tls-vendored with zigbuild

For musl targets, use `native-tls-vendored` with `cargo-zigbuild` for maximum HTTPS compatibility:

```bash
cargo zigbuild --release --target x86_64-unknown-linux-musl \
    --no-default-features \
    --features client/native-tls-vendored
```

Or use the convenience script:

```bash
./scripts/build-musl.sh
```

**Benefits:**
- ✅ Works with all HTTPS sites (Vercel, etc.)
- ✅ Zig provides complete C toolchain (no glibc/musl compatibility issues)
- ✅ Self-contained binary
- ✅ Same approach used in Docker builds

**How it works:** Zig's C compiler provides a complete libc implementation that's compatible with musl, avoiding the glibc symbol issues that occur with traditional cross-compilation.

### Alternative: Using rustls-tls

For faster builds with pure Rust TLS:

```bash
cargo zigbuild --release --target x86_64-unknown-linux-musl \
    --no-default-features \
    --features client/rustls-tls
```

**Pros:**
- Faster compilation
- Pure Rust (no C dependencies)
- Smaller binary size

**Cons:**
- May have certificate validation issues with some sites

## Building for native macOS/Linux (glibc)

For local development on macOS/Linux (system TLS):

```bash
cargo build --release
```

## Docker builds

### Native (glibc) image (Apple Silicon host)

Uses host glibc toolchain; no Zig/OpenSSL vendoring needed:

```bash
docker build \
  --build-arg TARGET_ABI=gnu \
  --build-arg USE_ZIG=false \
  -t cameodb:latest \
  --secret id=zscaler,src=/usr/local/share/ca-certificates/Zscaler.crt \
  .
```

### Musl (static) image with Zig + native-tls-vendored

Builds a static musl binary using Zig’s C toolchain and vendored OpenSSL:

```bash
docker build \
  --build-arg TARGET_ABI=musl \
  --build-arg USE_ZIG=true \
  -t cameodb:latest \
  --secret id=zscaler,src=/usr/local/share/ca-certificates/Zscaler.crt \
  .
```

**When to choose which:**
- Use **native/glibc** for typical container runtimes where glibc is available.
- Use **musl** when you need a fully static binary or strict MUSL environments.

### Default: Using system native-tls

For local development on macOS/Linux:

```bash
cargo build --release
```

Uses system TLS libraries (default feature).

## Feature Configuration

The `client` crate supports the following TLS backends:

- `native-tls` (default): Uses system TLS (SecureTransport on macOS, OpenSSL on Linux)
- `rustls-tls`: Pure Rust TLS implementation (recommended for musl/Docker builds)
- `native-tls-vendored`: Bundles and compiles OpenSSL from source

## Docker Build

The Dockerfile is configured to build with `native-tls-vendored` using Zig as the C compiler, matching the local zigbuild approach:

```dockerfile
# Install Zig for cross-compilation
ARG ZIG_VERSION=0.13.0
RUN wget -q https://ziglang.org/download/${ZIG_VERSION}/zig-linux-$(uname -m)-${ZIG_VERSION}.tar.xz && \
    tar -xf zig-linux-$(uname -m)-${ZIG_VERSION}.tar.xz && \
    mv zig-linux-$(uname -m)-${ZIG_VERSION} /usr/local/zig && \
    ln -s /usr/local/zig/zig /usr/local/bin/zig

# Configure Zig as C compiler for OpenSSL vendored build
ENV CC_x86_64_unknown_linux_musl="zig cc -target x86_64-linux-musl"
ENV CC_aarch64_unknown_linux_musl="zig cc -target aarch64-linux-musl"

# Build with native-tls-vendored
cargo build --release --target "${TARGET_TRIPLE}" --bin cameodb \
    --no-default-features \
    --features client/native-tls-vendored
```

Build the Docker image:
```bash
docker build -t cameodb:latest .
```

**Why Zig + native-tls-vendored?**
- ✅ Same approach as local zigbuild (consistency)
- ✅ Zig provides complete C toolchain compatible with musl
- ✅ Works with all HTTPS sites (maximum compatibility)
- ✅ No glibc/musl symbol conflicts

## Troubleshooting

### OpenSSL linking errors with musl

If you see errors like:
```
undefined reference to `__isoc23_strtol'
```

This is a glibc/musl compatibility issue with vendored OpenSSL. Use `rustls-tls` instead:
```bash
cargo zigbuild --release --target x86_64-unknown-linux-musl \
    --no-default-features \
    --features client/rustls-tls
```

### Certificate validation errors

If you encounter certificate validation errors with `rustls-tls`, the site may have unusual certificate requirements. Try `native-tls` for local development or investigate the specific certificate issue.

### OpenSSL not found during cross-compilation

If you see:
```
Could not find directory of OpenSSL installation
```

Either use `rustls-tls` (recommended) or install OpenSSL development packages for your target platform.
