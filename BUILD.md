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

- `native-tls` (default): Uses Windows Schannel
- `rustls-tls`: Pure Rust TLS implementation
- `native-tls-vendored`: Not typically needed on Windows

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
  --secret id=zscaler,src=/tmp/buildkit-ca/zscaler.crt \
  --push \
  .
```

**Single architecture build:**
```bash
docker build \
  --build-arg TARGET_ABI=musl \
  -t goranc/cameodb:latest \
  --secret id=zscaler,src=/tmp/buildkit-ca/zscaler.crt \
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
