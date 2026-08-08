# Cross-Compilation Build Guide

## Building for x86_64-unknown-linux-musl

### Recommended: Using the build script

For musl targets, use the convenience script, which builds via `cargo-zigbuild`. TLS is rustls with the `ring` provider, so no C toolchain or vendored OpenSSL is involved:

```bash
./scripts/build/build-musl.sh
```

**Benefits:**
- ✅ Works with all HTTPS sites (Vercel, etc.)
- ✅ Zig provides complete C toolchain (no glibc/musl compatibility issues)
- ✅ Self-contained binary
- ✅ Same approach used in Docker builds
- ✅ Sets `AR`/`RANLIB` to Zig's archiver — see the warning below

**How it works:** Zig's C compiler provides a complete libc implementation that's compatible with musl, avoiding the glibc symbol issues that occur with traditional cross-compilation.

> **⚠️ If you run `cargo zigbuild` directly instead of the script**, you must export `AR="zig ar"` and `RANLIB="zig ranlib"` first. Any target using `target_env = "musl"` pulls in `tikv-jemallocator`/`tikv-jemalloc-sys`, which compile jemalloc's C sources via `configure`/`make`. That build step never sets `AR`/`RANLIB` itself, so without the exports it silently falls back to macOS's native `ar`/`ranlib`. Those tools don't understand the ELF `.o` files Zig's cross-compiler produces — `ranlib` prints `warning: archive member '...' not a mach-o file` and drops every unrecognized member while rebuilding the archive's symbol index, leaving a valid but **empty** `libjemalloc.a` (just a `__.SYMDEF` header, no object code). The failure only shows up later, at link time, as `undefined symbol: mallocx` / `rallocx` / `sdallocx` / `mallctl` — misleading because the actual defect is in an archiving step several layers upstream of the link error.
>
> ```bash
> export AR="zig ar"
> export RANLIB="zig ranlib"
> cargo zigbuild --release --target x86_64-unknown-linux-musl \
>     --no-default-features \
> > ```

### Alternative: Using rustls-tls

For faster builds with pure Rust TLS:

```bash
export AR="zig ar"
export RANLIB="zig ranlib"
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

## Building for Windows

### Preparing Windows Machine for Development

#### 1. Install Rust via rustup

Download and run the official rustup installer:

```powershell
# Download and run the installer
# Visit: https://rustup.rs/
# Or use PowerShell:
winget install Rustlang.Rust.MSVC
```

Or download `rustup-init.exe` directly from https://rustup.rs/

#### 2. Install Visual Studio Build Tools

Windows builds require the Microsoft Visual C++ Build Tools:

**Option A: Visual Studio Community (Recommended)**
1. Download Visual Studio Community from https://visualstudio.microsoft.com/
2. During installation, select "Desktop development with C++"
3. Ensure these components are included:
   - MSVC v143 - VS 2022 C++ x64/x86 build tools
   - Windows 11 SDK (or Windows 10 SDK)
   - C++ tools for CMake

**Option B: Visual Studio Build Tools (Standalone)**
1. Download Build Tools from https://visualstudio.microsoft.com/downloads/
2. Select "C++ build tools" workload
3. Include the same MSVC and SDK components as above

#### 3. Verify Installation

Open a new terminal (PowerShell or Command Prompt) and verify:

```powershell
# Check Rust installation
rustc --version
cargo --version

# Check MSVC toolchain
rustup toolchain list
```

You should see the MSVC toolchain: `stable-x86_64-pc-windows-msvc`

#### 4. Additional MSVC Components (if needed)

If you encounter build errors, you may need additional components:

```powershell
# Using Visual Studio Installer, add these components:
# - MSVC v143 - VS 2022 C++ x64/x86 build tools (Latest)
# - Windows 11 SDK (minimum required version)
# - C++ tools for CMake
# - ATL support (if building with certain C++ dependencies)
```

### Native Windows Build

Once the machine is prepared, build CameoDB:

```powershell
cargo build --release
```

The binary will be available at:
```
target\release\cameodb.exe
```

#### Troubleshooting Common Windows Build Issues

**Linker.exe not found error:**

If you get an error like:
```
error: linker `link.exe` not found
```

**Solutions:**

1. **Restart PowerShell/Command Prompt** after Visual Studio installation
2. **Use Developer Command Prompt**: Open "Developer Command Prompt for VS 2022" from Start Menu
3. **Verify MSVC toolchain is active**:
   ```powershell
   rustup toolchain list
   rustup default stable-x86_64-pc-windows-msvc
   ```
4. **Reinstall Visual Studio Build Tools** with these specific components:
   - MSVC v143 - VS 2022 C++ x64/x86 build tools
   - Windows 11 SDK (minimum version 10.0.22000.0)
   - C++ tools for CMake

**"Could not find MSVC" error:**

1. Run from Developer Command Prompt for VS 2022
2. Or manually set up the environment:
   ```powershell
   # Find VS installation path
   "C:\Program Files\Microsoft Visual Studio\2022\Community\VC\Auxiliary\Build\vcvars64.bat"
   ```

**"cl.exe not found" error:**

This indicates the C++ compiler isn't in PATH. Use Developer Command Prompt or ensure Visual Studio Build Tools are properly installed.

**Interactive shell (REPL) cursor spacing issues:**

If you experience text appearing far to the right and then jumping to the correct position in the interactive client shell on Windows PowerShell:

1. **Cause**: Windows PowerShell has issues with ANSI escape codes in colored prompts, causing cursor positioning problems
2. **Solution**: The code automatically detects Windows and uses:
   - Plain text prompts (no colors) on Windows to avoid ANSI escape code issues
   - Simplified Rustyline configuration for better Windows compatibility
3. **Result**: Windows users get a clean `cameodb@localhost ▶ ` prompt without cursor positioning issues
4. **Alternative terminals**: For colored prompts, consider using:
   - Windows Terminal (recommended for best experience)
   - Git Bash
   - WSL2 terminal
5. **PowerShell-specific**: If you want colored prompts in PowerShell, try:
   ```powershell
   # Use Windows Terminal or update PowerShell to latest version
   winget install Microsoft.WindowsTerminal
   ```

### Cross-compilation to Windows from macOS/Linux

#### Using cargo-xwin (Recommended)

Install cargo-xwin for cross-compilation:

```bash
cargo install cargo-xwin
```

Build for Windows x64:

```bash
cargo xwin build --release --target x86_64-pc-windows-msvc
```

Build for Windows ARM64:

```bash
cargo xwin build --release --target aarch64-pc-windows-msvc
```

**Benefits:**
- ✅ Complete Windows toolchain via XWin
- ✅ Handles MSVC toolchain automatically
- ✅ Works on macOS and Linux hosts
- ✅ Produces native Windows executables

#### Using traditional cross-compilation

Install the Windows target:

```bash
rustup target add x86_64-pc-windows-msvc
```

Build for Windows:

```bash
cargo build --release --target x86_64-pc-windows-msvc
```

**Note:** Traditional cross-compilation requires MSVC toolchain components to be available on your system.

### TLS Configuration for Windows

Windows builds support the same TLS backends:

- TLS is rustls with the `ring` provider on every platform; there are no TLS feature flags to choose
- `rustls-tls`: Pure Rust TLS implementation
- Outbound HTTPS verifies against the Windows certificate store via `rustls-platform-verifier`

Example with rustls-tls:

```bash
cargo xwin build --release --target x86_64-pc-windows-msvc \
    --no-default-features \
    --features client/rustls-tls
```

### Windows-specific Considerations

- **Console Window**: Use `windows-subsystem = "windows"` in Cargo.toml for GUI applications
- **Path Separators**: Rust handles path separators automatically
- **Permissions**: Windows uses ACLs instead of Unix permissions
- **Services**: For Windows service deployment, consider additional service-specific configuration
- **Signal Handling**: The application uses cross-platform signal handling:
  - Ctrl+C (SIGINT) works on all platforms
  - SIGTERM (systemctl stop) is only available on Unix systems
  - On Windows, only Ctrl+C shutdown is supported
- **Network Bindings**: Windows may require administrator privileges for ports below 1024

## Docker builds

### Corporate CA certificates

Behind a TLS-intercepting proxy, the build needs your CA or `cargo fetch` fails on every
crates.io request. It is passed as a BuildKit secret named **`corporate-ca`** — the id the
Dockerfile mounts, and a mismatched name fails silently, reporting "No corporate CA
certificate provided" and producing an image that cannot reach the proxy.

```bash
export CAMEODB_CA_CERT=/usr/local/share/ca-certificates/corporate-ca.crt
```

`scripts/build/docker-push.sh` reads the same variable, defaulting to
`/var/tmp/buildkit-ca/corporate-ca.crt`.

The examples below use that variable. Without a corporate CA, use `/dev/null` — the build
skips an empty file rather than special-casing it:

```bash
export CAMEODB_CA_CERT=/dev/null
```

The secret is mounted into the build stage only and never reaches the runtime image. Compose
does this for you: `docker compose build` reads `CAMEODB_CA_CERT` and defaults it to
`/dev/null`.

### Native (glibc) image (Apple Silicon host)

Uses host glibc toolchain; no Zig/OpenSSL vendoring needed:

```bash
docker build \
  --build-arg TARGET_ABI=gnu \
  --build-arg USE_ZIG=false \
  -t cameodb:latest \
  --secret id=corporate-ca,src=$CAMEODB_CA_CERT \
  .
```

### Musl (static) image with Zig

Builds a static musl binary using Zig’s C toolchain and vendored OpenSSL:

```bash
docker build \
  --build-arg TARGET_ABI=musl \
  --build-arg USE_ZIG=true \
  -t cameodb:latest \
  --secret id=corporate-ca,src=$CAMEODB_CA_CERT \
  .
```

**When to choose which:**
- Use **native/glibc** for typical container runtimes where glibc is available.
- Use **musl** when you need a fully static binary or strict MUSL environments.

### Default: rustls with the system trust store

For local development on macOS/Linux:

```bash
cargo build --release
```

Uses system TLS libraries (default feature).

## Feature Configuration

The `client` crate supports the following TLS backends:

- TLS is rustls with the `ring` provider; outbound HTTPS verifies against the system trust store (Keychain on macOS, `/etc/ssl/certs` on Linux)
- `rustls-tls`: Pure Rust TLS implementation (recommended for musl/Docker builds)
- Static and musl builds need `ca-certificates` present in the image; verify with `scripts/validate/remote-sources.sh`

## Docker Build

### Multi-Architecture Non-Root Docker Images

The Dockerfile builds secure, non-root Docker images for both `amd64` and `arm64` platforms using distroless base images.

#### Key Features:
- **✅ Non-root execution**: Runs as `nonroot:nonroot` (UID/GID 65532)
- **✅ Multi-architecture**: Supports `linux/amd64` and `linux/arm64`
- **✅ Static musl builds**: Fully static binaries for portability
- **✅ Distroless runtime**: Minimal attack surface, no shell
- **✅ OpenSSL vendoring**: No external TLS dependencies

#### Build Configuration:

```dockerfile
# Default to musl for static linking
ARG TARGET_ABI=musl
ARG USE_ZIG=false

# Use distroless nonroot base images
FROM gcr.io/distroless/static:nonroot AS runtime-musl
FROM gcr.io/distroless/cc-debian12:nonroot AS runtime-gnu

# Explicit non-root user
USER nonroot:nonroot

# Copy with proper ownership
COPY --from=builder --chown=nonroot:nonroot /src/cameodb /usr/local/bin/cameodb
```

#### Build Commands:

**Multi-arch build (recommended):**
```bash
docker buildx build \
  --builder cameo-builder \
  --platform linux/amd64,linux/arm64 \
  --build-arg TARGET_ABI=musl \
  -t goranc/cameodb:latest \
  --secret id=corporate-ca,src=$CAMEODB_CA_CERT \
  --push \
  .
```

**Single architecture build:**
```bash
docker build \
  --build-arg TARGET_ABI=musl \
  -t goranc/cameodb:latest \
  --secret id=corporate-ca,src=$CAMEODB_CA_CERT \
  .
```

#### Build Options:

| TARGET_ABI | USE_ZIG | Result |
|------------|---------|--------|
| `musl` | `false` (default) | Static musl with musl-gcc |
| `musl` | `true` | Static musl with Zig toolchain |
| `gnu` | `false` | Dynamic glibc build |

#### Why This Configuration:

- **Non-root by default**: All images run as non-root user for security
- **Distroless base**: Minimal runtime, no package manager or shell
- **Static linking**: No runtime dependencies, works across Linux distributions
- **Multi-arch**: Single image supports both Intel and ARM platforms
- **OpenSSL vendored**: No external TLS library dependencies

#### Verification:

```bash
# Verify non-root execution
docker run --rm --entrypoint id goranc/cameodb:latest
# Expected: uid=65532(nonroot) gid=65532(nonroot)

# Test binary functionality
docker run --rm goranc/cameodb:latest --version

# Test specific architectures
docker run --rm --platform linux/amd64 goranc/cameodb:latest --version
docker run --rm --platform linux/arm64 goranc/cameodb:latest --version
```

#### Recent Fixes (Non-Root Issue Resolution):

**Problem**: Previously, `amd64` images were running as root while `arm64` correctly ran as non-root.

**Root Causes Fixed**:
1. **Ring crate Zig dependency**: Environment variables for Zig were being set unconditionally, forcing `ring` crate to use non-existent `zig` compiler
2. **Missing USER instruction**: When using variable `FROM runtime-${TARGET_ABI}`, the USER from base distroless images wasn't being inherited
3. **Cross-compilation linker**: `arm64` musl builds needed proper `musl-gcc` configuration

**Solutions Applied**:
```dockerfile
# 1. Conditional Zig environment variables (only when USE_ZIG=true)
if [ "${USE_ZIG}" = "true" ] && [ "${TARGETARCH}" = "amd64" ]; then
    export CC_x86_64_unknown_linux_musl="zig cc -target x86_64-linux-musl"
    export AR_x86_64_unknown_linux_musl="zig ar"
    export CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER="zig"
fi

# 2. Explicit USER instruction for non-root execution
USER nonroot:nonroot

# 3. Proper musl-gcc configuration for arm64
[target.aarch64-unknown-linux-musl]
linker = "musl-gcc"
rustflags = [
    "-C", "target-feature=+crt-static",
    "-C", "relocation-model=pie",
    "-C", "relro-level=full", 
    "-C", "link-arg=-pie",
    "-C", "link-arg=-static",
    "-C", "link-arg=-Wl,-z,now",
    "-C", "link-arg=-Wl,-z,relro",
    "-C", "link-arg=-fstack-protector-strong",
    "-C", "link-arg=-D_FORTIFY_SOURCE=2"
]
```

**Result**: Both architectures now consistently run as non-root user with secure, static binaries.

## Troubleshooting

### OpenSSL linking errors with musl

If you see errors like:
```
undefined reference to `__isoc23_strtol'
```

This is a glibc/musl compatibility issue with vendored OpenSSL. Use `rustls-tls` instead:
```bash
export AR="zig ar"
export RANLIB="zig ranlib"
cargo zigbuild --release --target x86_64-unknown-linux-musl \
    --no-default-features \
    --features client/rustls-tls
```

### Certificate validation errors

Certificate validation errors usually mean the issuing CA is missing from the OS trust store — install it there (this is also what a TLS-inspecting corporate proxy requires). `--insecure-source` bypasses verification for a remote data source and should stay a development-only measure.

### Undefined symbol errors: `mallocx`, `rallocx`, `sdallocx`, `mallctl`

If linking fails with errors like:
```
ld.lld: error: undefined symbol: mallocx
ld.lld: error: undefined symbol: mallctl
```

You ran `cargo zigbuild` directly without exporting `AR`/`RANLIB` first (see the warning in [Building for x86_64-unknown-linux-musl](#building-for-x86_64-unknown-linux-musl) above). Either use `./scripts/build/build-musl.sh`, or export `AR="zig ar"` and `RANLIB="zig ranlib"` before your `cargo zigbuild` invocation. As a quick diagnostic, check whether `libjemalloc.a` in the build output is suspiciously small (a healthy archive is several MB; an empty one built with the wrong `ranlib` is under 100 bytes):
```bash
find target -name "libjemalloc.a" -exec ls -la {} \;
```

### OpenSSL not found during cross-compilation

If you see:
```
Could not find directory of OpenSSL installation
```

Either use `rustls-tls` (recommended) or install OpenSSL development packages for your target platform.
## 📦 RPM Package Building

CameoDB supports building RPM packages for x86_64 Linux distributions using cargo-zigbuild for cross-compilation.

### Prerequisites

Install the required cargo extensions for cross-compilation and RPM generation:
```bash
# Install cargo-zigbuild for cross-compilation
cargo install cargo-zigbuild

# Install cargo-generate-rpm for RPM package generation
cargo install cargo-generate-rpm
```

### Build RPM Package

**Option 1: Native x86_64 Linux Build (Recommended for hardened executables)**
```bash
# Build hardened executable with security mitigations (flags in .cargo/config.toml)
cargo build --release --target x86_64-unknown-linux-musl

# OR override with explicit RUSTFLAGS:
RUSTFLAGS="-C relocation-model=pie -C relro-level=full -C link-arg=-Wl,-z,now -C link-arg=-fstack-protector -C link-arg=-D_FORTIFY_SOURCE=2" \
cargo build --release --target x86_64-unknown-linux-musl

# Generate RPM package (run from project root directory)
cargo generate-rpm -p crates/server --target x86_64-unknown-linux-musl --auto-req disabled \
  -o target/x86_64-unknown-linux-musl/release/cameodb-0.2.2-1.x86_64.rpm \
  --set-metadata 'package.name="cameodb"'
```

**Option 2: Cross-compilation with cargo-zigbuild (supports hardening)**

`cargo generate-rpm` needs its own `--target` invocation, so this option can't go through `./scripts/build/build-musl.sh` directly — `AR`/`RANLIB` must be exported manually (see the warning in [Building for x86_64-unknown-linux-musl](#building-for-x86_64-unknown-linux-musl)):
```bash
export AR="zig ar"
export RANLIB="zig ranlib"

# Build hardened binary for Linux x86_64 musl target (flags in .cargo/config.toml)
cargo zigbuild --release --target x86_64-unknown-linux-musl \
    --no-default-features \

# OR override with explicit RUSTFLAGS:
RUSTFLAGS="-C target-feature=+crt-static -C relocation-model=pie -C relro-level=full -C link-arg=-pie -C link-arg=-static -C link-arg=-Wl,-z,now -C link-arg=-Wl,-z,relro -C link-arg=-fstack-protector-strong -C link-arg=-D_FORTIFY_SOURCE=2" \
cargo zigbuild --release --target x86_64-unknown-linux-musl \
    --no-default-features \

# Generate RPM package with standard naming (run from project root directory)
cargo generate-rpm -p crates/server --target x86_64-unknown-linux-musl --auto-req disabled \
  -o target/x86_64-unknown-linux-musl/release/cameodb-0.2.2-1.x86_64.rpm \
  --set-metadata 'package.name="cameodb"'

# The RPM package will be available at:
# target/x86_64-unknown-linux-musl/release/cameodb-0.2.2-1.x86_64.rpm
```

**Option 3: DEB Package Generation (Ubuntu/Debian)**
```bash
# Install cargo-deb
cargo install cargo-deb

# Build hardened binary using Docker (native musl toolchain)
# This avoids Zig cross-compilation issues with C dependencies
# IMPORTANT: Use --platform linux/amd64 to get x86_64 container (not ARM64)
# Use pre-built builder image (dependencies pre-installed)
# Build the builder image once:
docker buildx build --platform linux/amd64 \
  --builder cameo-builder \
  --load \
  -t cameo-builder -f docker/Dockerfile.builder .

# Then use it for fast builds:
docker run --rm --platform linux/amd64 \
  -v "$PWD":/workspace -w /workspace \
  -v $CAMEODB_CA_CERT:/usr/local/share/ca-certificates/corporate-ca.crt:ro \
  -e CC_x86_64_unknown_linux_musl=musl-gcc \
  -e AR_x86_64_unknown_linux_musl=ar \
  -e RANLIB_x86_64_unknown_linux_musl=ranlib \
  cameo-builder bash -c "
    cat /usr/local/share/ca-certificates/corporate-ca.crt >> /etc/ssl/certs/ca-certificates.crt && \
    export SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt && \
    cargo build --release --target x86_64-unknown-linux-musl \
      --no-default-features \
    "

# Generate DEB package (run on host after Docker build)
# Use --no-build to package the existing binary without rebuilding
# Use --no-strip on macOS (macOS strip/objcopy don't support Linux binaries)
# Note: Binary is automatically stripped by Cargo's [profile.release] strip = "symbols"
# The debug symbols warning from cargo-deb is cosmetic and can be ignored.
cargo deb --no-build --no-strip --target x86_64-unknown-linux-musl -p server

# With custom output path (follows DEB naming standards)
cargo deb --no-build --no-strip --target x86_64-unknown-linux-musl -p server \
  --output target/x86_64-unknown-linux-musl/release/cameodb_0.2.2_amd64.deb

# The DEB package will be available at:
# target/x86_64-unknown-linux-musl/debian/cameodb_0.2.2_amd64.deb
# OR with custom output: target/x86_64-unknown-linux-musl/release/cameodb_0.2.2_amd64.deb
```

**Option 4: Automated Build Script (Recommended for CI/CD)**
```bash
# Use the optimized build script with persistent caching
# This script handles both RPM and DEB package generation in one run
./scripts/build/build-dist.sh
```

The `build-dist.sh` script provides:
- **Persistent Docker volumes** for cargo registry and target cache (dramatic speed improvements on subsequent builds)
- **Corporate CA certificate handling** for network trust
- **Automatic binary stripping** via Cargo profile optimization
- **Both RPM and DEB package generation** in a single run
- **Colored output and progress indicators**

**Prerequisites for build-dist.sh:**
```bash
# Make the script executable
chmod +x build-dist.sh

# Ensure Docker buildx builder is running
docker buildx ls
```

### Signing Release Artifacts

Cosign 2.x defaults to the new bundle format. Generate one `.bundle` file per artifact and ship it together with the binary and `cosign.pub` so downstream users can verify releases.

```bash
cosign sign-blob \
  --key /usr/local/share/ca-certificates/cosign.key \
  --bundle target/release/cameodb.bundle \
  target/release/cameodb

cosign sign-blob \
  --key /usr/local/share/ca-certificates/cosign.key \
  --bundle target/x86_64-unknown-linux-musl/release/cameodb.bundle \
  target/x86_64-unknown-linux-musl/release/cameodb

cosign sign-blob \
  --key /usr/local/share/ca-certificates/cosign.key \
  --bundle target/x86_64-unknown-linux-musl/release/cameodb-0.2.2-1.x86_64.rpm.bundle \
  target/x86_64-unknown-linux-musl/release/cameodb-0.2.2-1.x86_64.rpm

cosign sign-blob \
  --key /usr/local/share/ca-certificates/cosign.key \
  --bundle target/x86_64-unknown-linux-musl/release/cameodb_0.2.2_amd64.deb.bundle \
  target/x86_64-unknown-linux-musl/release/cameodb_0.2.2_amd64.deb
```

**Verification example:**

```bash
cosign verify-blob \
  --key cosign.pub \
  --bundle cameodb.bundle \
  cameodb
```

If you need legacy `.sig`/`.cert` files instead, add `--legacy-signatures` (or set `COSIGN_EXPERIMENTAL=0`) and keep the previous `--output-signature` / `--output-certificate` flags.

**Note**: Two approaches for hardening flags:
1. **Pre-configured**: Hardening flags are set in `.cargo/config.toml` and applied automatically
2. **Explicit override**: Use `RUSTFLAGS="..."` to override or customize flags as shown above

Hardening flags explained:
- `-C target-feature=+crt-static` enables static C runtime linking
- `-C relocation-model=pie` enables Position Independent Executable for ASLR support
- `-C relro-level=full` enables Full RELRO (Relocation Read-Only) 
- `-C link-arg=-pie` + `-C link-arg=-static` creates static PIE executable (separated flags)
- `-C link-arg=-Wl,-z,now` enables immediate symbol binding
- `-C link-arg=-Wl,-z,relro` enables RELRO protection
- `-C link-arg=-fstack-protector-strong` enables strong stack protection against buffer overflows
- `-C link-arg=-D_FORTIFY_SOURCE=2` enables fortified memory functions for additional safety
- `opt-level = 3` (release profile) required for fortified functions to work properly
- Both cargo build and cargo-zigbuild support these rustc-native flags

**Windows Hardening** (when building for Windows targets):
- `/SDL` enables Security Development Lifecycle checks (equivalent to VS /SDL)
- `/DYNAMICBASE` enables ASLR (Address Space Layout Randomization)
- `/HIGHENTROPYVA` enables 64-bit ASLR with high entropy
- `/NXCOMPAT` enables DEP (Data Execution Prevention)
- `/GUARD:CF` enables Control Flow Guard

**Verification**: 
- For dynamic binaries (gnu): `file` shows "pie executable"
- For static binaries (musl): `file` shows "executable" but hardening is still applied
- Use `greadelf -d` or check binary headers to verify PIE and RELRO on static binaries
- Fortified functions replace unsafe C library calls with checked versions

### RPM Package Contents

- **Binary**: `/usr/local/bin/cameodb` (statically linked, no external dependencies)
- **Config**: `/etc/cameodb/cameodb.toml`
- **Service**: `/usr/lib/systemd/system/cameodb.service`
- **User/Group**: `cameodb` (created automatically during install)
- **Data Directory**: `/var/lib/cameodb` (created with proper permissions)

### DEB Package Contents

- **Binary**: `/usr/local/bin/cameodb` (statically linked, no external dependencies)
- **Config**: `/etc/cameodb/cameodb.toml` (marked as config file, preserved on upgrades)
- **Service**: `/lib/systemd/system/cameodb.service`
- **User/Group**: `cameodb` (created automatically during install)
- **Data Directory**: `/var/lib/cameodb` (created with proper permissions)

### Installation on Target System

**For RPM-based systems (RHEL, CentOS, Fedora):**
```bash
# Verify RPM package before installation
rpm -qpi cameodb-0.2.2-1.x86_64.rpm

# Check package contents
rpm -qpl cameodb-0.2.2-1.x86_64.rpm

# Install the RPM package
sudo rpm -i cameodb-0.2.2-1.x86_64.rpm

# Start and enable the service
sudo systemctl daemon-reload
sudo systemctl enable cameodb
sudo systemctl start cameodb
```

**For DEB-based systems (Ubuntu, Debian):**
```bash
# Verify DEB package before installation
dpkg -I cameodb_0.2.2_amd64.deb

# Check package contents
dpkg -c cameodb_0.2.2_amd64.deb

# Install the DEB package
sudo dpkg -i cameodb_0.2.2_amd64.deb

# Start and enable the service
sudo systemctl daemon-reload
sudo systemctl enable cameodb
sudo systemctl start cameodb
```
# Check status
sudo systemctl status cameodb

---

