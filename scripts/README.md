# CameoDB Scripts and Utilities

This directory contains scripts and utilities for CameoDB development, testing, and operations. All scripts follow database industry best practices and provide comprehensive tooling for the entire development lifecycle.

## Quick Start

For a quick overview of all available scripts and project status:

```bash
./scripts/setup/dev-info.sh
```

## Directory Structure & Scripts

### 🚀 `release/` - Release pipeline

**Start here for anything that will be published.** Four stages — build, sbom, sign, publish —
that turn a clean tree into signed, checksummed artifacts in `cameodb-web/public/downloads/`.
The version is read from `crates/*/Cargo.toml` and never passed as an argument.

```bash
scripts/release/release.sh --stage build,sbom    # binaries, DEB, RPM, SPDX, CycloneDX
#   ... build cameodb.exe on the Windows machine → dist/<version>/windows/
scripts/release/release.sh --stage sign
scripts/release/publish.sh                       # dry run: add / same / replace report
scripts/release/publish.sh --commit
```

- **Prerequisites**: Docker (required — the zigbuild path cannot produce a static-pie binary
  and the build stage refuses it), plus `syft`, `cosign`, `cargo-deb`, `cargo-generate-rpm`, `jq`
- **Audience**: release engineers
- **Details**: [release/README.md](release/README.md), and
  [RELEASE-CHECKLIST.md](../RELEASE-CHECKLIST.md) for the surrounding procedure

The scripts below are the underlying mechanics. Use them directly for local builds and
debugging; use the pipeline for releases.

### 📦 `build/` - Build and Distribution

#### `build-musl.sh`
**Purpose**: Build a fully static Linux binary (musl) — no interpreter, no `NEEDED` entries, runs on `gcr.io/distroless/static`
- **Features**:
  - Builds in a Linux container matching the target architecture by default, which is the same toolchain the `Dockerfile` uses and therefore matches the published image. Falls back to `cargo-zigbuild` when Docker is unavailable, or with `BUILD_WITH=zig`
  - **The two methods do not produce the same artifact.** Zig's linker does not advertise `-static-pie`, so rustc falls back to `-static` with only a warning: still fully static, but not position-independent. Prefer the container path for anything you ship
  - Checks the result with `validate/artifact.sh` rather than assuming the flags took
  - TLS is rustls with the `ring` provider (no vendored OpenSSL, no C toolchain)
  - Under the zig path, exports `AR="zig ar"` / `RANLIB="zig ranlib"` so jemalloc's C build archives correctly — without this, macOS's native `ranlib` silently produces an empty `libjemalloc.a` (it can't parse the ELF objects Zig's cross-compiler emits), and the failure only surfaces later as `undefined symbol: mallocx`/`mallctl` at link time. See [docs/BUILDING.md](../docs/BUILDING.md) for the full explanation
- **Usage**:
  ```bash
  ./scripts/build/build-musl.sh                    # release, x86_64
  ./scripts/build/build-musl.sh release aarch64    # release, aarch64
  ./scripts/build/build-musl.sh release both       # both architectures
  ./scripts/build/build-musl.sh dev                # dev build (binary lands in target/.../debug/)
  BUILD_WITH=zig ./scripts/build/build-musl.sh     # force the no-Docker path
  ```
- **Prerequisites**: Docker; or `zig` and `cargo-zigbuild` (`brew install zig && cargo install cargo-zigbuild`) for the fallback. A cross-architecture container build runs emulated and is slow
- **Audience**: Developers building/testing Linux binaries locally on macOS

#### `docker-push.sh`
**Purpose**: Build and push multi-platform Docker images to DockerHub
- **Features**:
  - Multi-platform builds (amd64 + arm64)
  - Automatic buildx builder management
  - Corporate CA certificate support
  - `--no-push` builds both platforms without publishing
- **Usage**:
  ```bash
  ./scripts/build/docker-push.sh               # Build + push latest
  ./scripts/build/docker-push.sh 0.3.0         # Build + push version tag
  ./scripts/build/docker-push.sh --no-push     # Build both platforms, no push
  ```
- **Audience**: DevOps, release engineers, CI/CD

#### `build-packages.sh`
**Purpose**: Cross-compilation build script for binaries and packages
- **Features**:
  - Persistent Docker caching for fast rebuilds
  - DEB and RPM package generation
  - Multi-architecture support (amd64, arm64)
  - Corporate CA certificate handling
- **Usage**:
  ```bash
  ./scripts/build/build-packages.sh              # Build amd64
  ./scripts/build/build-packages.sh arm64        # Build arm64
  ./scripts/build/build-packages.sh amd64 arm64  # Build both
  ```
- **Outputs**: Binary, DEB package, RPM package
- **Audience**: DevOps, release engineers

### 🔒 `security/` - Security and Compliance

#### `generate-sbom.sh`
**Purpose**: Generate Software Bill of Materials (SBOM) for supply chain security
- **Features**:
  - SPDX 2.3 and CycloneDX 1.5 formats
  - Scan from Docker image, native binary, or source code
  - Configurable output directory
- **Usage**:
  ```bash
  ./scripts/security/generate-sbom.sh            # From Docker image
  ./scripts/security/generate-sbom.sh --native   # From native binary
  ./scripts/security/generate-sbom.sh --source   # From source code
  ./scripts/security/generate-sbom.sh --output ./sboms
  ```
- **Prerequisites**: syft 1.42.3+ (`brew install syft`)
- **Audience**: Security engineers, compliance teams

### 🛠️ `setup/` - Development Environment Setup

#### `install-deps.sh`
**Purpose**: Automated setup of CameoDB development environment
- **Features**: 
  - Cross-platform support (macOS/Linux)
  - Auto-detects and installs missing dependencies (curl, jq)  
  - Verifies Rust toolchain and project compilation
  - Creates necessary data directories
- **Usage**: `./scripts/setup/install-deps.sh`
- **Audience**: New developers, CI/CD systems

#### `init-cluster.sh [port]`
**Purpose**: Initialize development cluster with sample data and shards
- **Features**:
  - Creates sample shards with realistic data
  - Validates CameoDB startup and data ingestion
  - Interactive mode - keeps CameoDB running until Ctrl+C
- **Usage**: 
  ```bash
  ./scripts/setup/init-cluster.sh        # Default port 9480
  ./scripts/setup/init-cluster.sh 8080   # Custom port
  ```
- **Audience**: Developers, demo environments

#### `config-manager.sh <command> [options]`
**Purpose**: Configuration management and template generation
- **Commands**:
  - `generate [file]` - Generate sample configuration
  - `validate [file]` - Validate configuration syntax
  - `env-template` - Show environment variables
  - `multi-disk` - Multi-disk configuration template
  - `performance` - High-performance configuration
  - `minimal` - Minimal/development configuration
- **Usage**:
  ```bash
  ./scripts/setup/config-manager.sh generate          # Basic config
  ./scripts/setup/config-manager.sh performance       # High-perf config
  ./scripts/setup/config-manager.sh validate cameodb.toml
  ```
- **Audience**: DevOps, system administrators, developers

### 🧪 `testing/` - Testing and Validation

#### `test-api.sh`
**Purpose**: Comprehensive API endpoint testing
- **Features**:
  - Tests all HTTP endpoints (health, search, write, stream)
  - JSON response validation
  - Automatic CameoDB startup and cleanup
  - NDJSON streaming test with timeout
- **Usage**: `./scripts/testing/test-api.sh`
- **Requirements**: CameoDB must be running or script will start it
- **Audience**: Developers, QA, CI/CD pipelines

#### `load-test.sh [port] [users] [requests_per_user]` — ⚠️ smoke test only
**Purpose**: Rough concurrent-load smoke test
- **Do not use its numbers.** It forks `curl` per request and times it in bash, so at any
  real concurrency the latencies it reports are process spawn and connection setup, not
  CameoDB. Use `cameodb-bench` for anything you intend to compare or record.
- **Usage**:
  ```bash
  ./scripts/testing/load-test.sh                    # Default: 10 users, 50 requests each
  ```
- **Output**: Creates test data in 'loadtest' index

#### `cameodb-bench` — the latency harness
**Purpose**: Percentile latency for writes and searches, plus worker-pool placement
- **Usage**:
  ```bash
  cargo run -p bench -- --url http://localhost:9480 --mode mixed --concurrency 8 --duration 30
  ```
- **Reports**: p50/p90/p95/p99/p99.9 for writes and searches, the node's own `took_ms`
  beside the client-observed figure (the gap is queueing), and per-worker job counts,
  core placement and dispatch counters over the measured window
- **Caveat**: closed-loop — compare runs at equal `--concurrency`, and run it off-box when
  the numbers matter, or the generator competes for the cores under test
- **Source**: [crates/bench](../crates/bench/README.md), which doubles as a worked example
  of the client SDK
- **Audience**: Performance engineers, DevOps

### 📊 `data/` - Data Management

#### `sample-data.sh [port] [index] [count]`
**Purpose**: Generate realistic sample data for development and testing
- **Features**:
  - Configurable document count (default: 100)
  - Realistic document structure with categories, topics, tags
  - Progress tracking and error reporting
  - Automatic search validation after data load
- **Usage**:
  ```bash
  ./examples/data/sample-data.sh                     # 100 docs in 'sample' index
  ./examples/data/sample-data.sh 9480 mydata 500     # 500 docs in 'mydata' index
  ```
- **Data Types**: Technology, science, business, education, entertainment, sports, health, travel
- **Audience**: Developers, QA, demo environments

### 🔧 `ops/` - Operations and Monitoring

#### `health-check.sh [port] [timeout]`
**Purpose**: Comprehensive health monitoring and diagnostics
- **Features**:
  - CameoDB connectivity and response time testing
  - API endpoint validation (search, write, stream, health)
  - Performance metrics and memory usage monitoring
  - Colored output with clear status indicators
  - Overall health assessment with exit codes
- **Usage**:
  ```bash
  ./scripts/ops/health-check.sh         # Default port 9480, 10s timeout
  ./scripts/ops/health-check.sh 8080 5  # Port 8080, 5s timeout
  ```
- **Exit Codes**: 0 = healthy, 1 = degraded
- **Audience**: DevOps, monitoring systems, production operations

## Utility Scripts

### `setup/dev-info.sh`
**Purpose**: Quick project overview and script documentation
- **Features**:
  - Lists all available scripts with descriptions
  - Shows current project build status
  - Displays CameoDB running status  
  - Quick start guide and documentation links
- **Usage**: `./scripts/setup/dev-info.sh`
- **Audience**: All developers, new contributors

## Usage Guidelines

### Running Scripts

**Scripts in `build/` and `security/`** can be run from any directory (auto-detect project root):

```bash
# ✅ Works from anywhere
./scripts/build/docker-push.sh
./scripts/security/generate-sbom.sh
```

**All other scripts** must be run from the **workspace root directory**:

```bash
# ✅ Correct - from workspace root
./scripts/testing/test-api.sh
./scripts/setup/install-deps.sh

# ❌ Wrong - from scripts directory
cd scripts && ./testing/test-api.sh
```

### Common Workflows

#### New Developer Setup
```bash
./scripts/setup/install-deps.sh     # Install dependencies
cargo build --release               # Build project
./scripts/setup/init-cluster.sh     # Start with sample data
./scripts/testing/test-api.sh       # Validate installation
```

#### Development Testing
```bash
./examples/data/sample-data.sh       # Load test data
cargo run -p bench -- --duration 30  # Performance testing (percentiles)
./scripts/ops/health-check.sh       # System health
```

#### CI/CD Integration
```bash
./scripts/setup/install-deps.sh     # Environment setup
cargo test --workspace              # Unit tests
./scripts/testing/test-api.sh       # Integration tests
./scripts/ops/health-check.sh       # Health validation
```

## Dependency and Toolchain Updates

Nothing here is automated. Dependencies move when someone decides to move them, and that
decision is worth making with the dry run in front of you — an update that quietly picks up
a security fix and one that quietly picks up a storage-engine release produce the same
`Cargo.lock` diff and the same green build.

### Dry run — what would move, before anything moves

```bash
rustup check                                   # toolchains: installed vs. available
cargo update --dry-run --verbose               # dependencies: what moves, what stays behind a major
cargo outdated --workspace --root-deps-only    # narrowed to deps this workspace declares itself
```

`cargo update --dry-run` prints two kinds of line. `Updating a -> b` is a semver-compatible
move it would make. `Unchanged x (available: y)`, which only appears under `--verbose`, is a
crate held back by a major-version bound — those need a manifest edit and usually a code
change, so they are never part of a routine update.

⚠️ **Do not add `--workspace` to `cargo update`.** It does not mean "everything". It means
"relock only the workspace members", which suppresses every transitive dependency. A run
carrying that flag reported nothing to do while an advisory fix sat one line below the cut.

To find out why a version is pinned where it is:

```bash
cargo tree --invert <crate>                    # who depends on it
cargo tree --invert <crate>@<version>          # when two majors coexist in the graph
```

### Apply and verify

```bash
cargo update                                   # semver-compatible only; touches Cargo.lock, nothing else
cargo check --workspace --all-targets
scripts/validate/deps.sh                       # fmt, clippy -D warnings, cargo audit, cargo deny
scripts/validate/unit.sh                       # cargo test --workspace
```

`Cargo.lock` should be the entire diff. If anything under `crates/*/Cargo.toml` also
changed, it was not a routine update — see the next section. Revert at any point with
`git checkout Cargo.lock`.

A storage-engine bump (`redb`) earns one check that semver cannot give you, because a
lockfile diff says nothing about whether existing databases still open:

```bash
R=$(echo ~/.cargo/registry/src/index.crates.io-*)
diff "$R"/redb-<old>/src/tree_store/page_store/header.rs \
     "$R"/redb-<new>/src/tree_store/page_store/header.rs
```

An unchanged `FILE_FORMAT_VERSION` is the claim worth having; the changelog alone is not.

### Major bumps

A crate behind a major version is its own change with its own commit. Before taking one,
check whether it removes a version from the graph or merely adds one — a direct dependency
bumped while transitive dependents still hold the old major leaves both compiled in, which
costs build time and buys nothing:

```bash
cargo tree --invert <crate>@<current-version>  # anything here but our own crates means the old version stays
cargo upgrade --incompatible -p <crate>        # cargo-edit; rewrites Cargo.toml
```

### Tooling

The cargo subcommands the release and validation paths lean on are installed globally, not
pinned by the repo, so they drift independently of `Cargo.lock` and nothing in a normal
build notices.

```bash
for c in cargo-audit cargo-deny cargo-outdated cargo-edit \
         cargo-deb cargo-generate-rpm cargo-zigbuild flamegraph; do
    cargo search "$c" --limit 1 | grep "^$c "  # dry run: latest published version
done
cargo install --list                           # what is actually installed right now
```

⚠️ **Install one at a time.** `cargo install a b c` prints
`Summary Successfully installed a, b, c!` and exits 0 whether or not it replaced anything —
a batch of five was seen to replace exactly one and report all five as installed. The loop
form, re-read against `cargo install --list`, is what actually tells you it worked:

```bash
for c in cargo-outdated cargo-edit cargo-deb; do cargo install "$c" --locked; done
cargo install --list                           # trust this, not the summary line
```

`cargo-zigbuild` gets its own step, because rustc drops link arguments its linker refuses
with only a warning — so a version change there is not proven by a build that succeeds.
`build-musl.sh` takes the container path by default, which does not touch zigbuild at all;
exercising the new one means asking for it:

```bash
cargo install cargo-zigbuild --locked
BUILD_WITH=zig scripts/build/build-musl.sh
scripts/validate/artifact.sh                   # the check that catches a silently dropped flag
```

Expect `artifact.sh` to report the aarch64 binary as static-but-not-PIE on this path; that
is the documented zigbuild limitation, and it is why `release/build.sh` refuses
`BUILD_WITH=zig` without `--allow-unhardened`. Releases ship from the container path.

## Configuration & Customization

### Default Settings
- **CameoDB Port**: 9480
- **Health Timeout**: 10 seconds
- **Load Test**: 10 users, 50 requests each
- **Sample Data**: 100 documents

### Environment Variables
Scripts respect these environment variables when available:
- `CAMEODB_PORT`: Default CameoDB port
- `CAMEODB_HOST`: Default CameoDB host (default: localhost)

### Data Directories
- **Production Data**: `./data/cameodb/` (git-ignored, created at runtime)
- **Test Data**: `/tmp/cameodb_tests/` (temporary, auto-cleanup with UUID isolation)

## Contributing

### Adding New Scripts

1. **Placement**: Choose appropriate subdirectory based on purpose
2. **Naming**: Use kebab-case (`my-script.sh`)
3. **Permissions**: Make executable (`chmod +x`)
4. **Structure**: Follow existing patterns:
   ```bash
   #!/bin/bash
   set -e  # Exit on error
   
   # Configuration with defaults
   DEFAULT_PORT=9480
   PORT=${1:-$DEFAULT_PORT}
   
   # Clear documentation and help
   # Main functionality
   # Error handling and cleanup
   ```

5. **Documentation**: Update this README with script details
6. **Testing**: Test from workspace root directory

### Best Practices
- **Error Handling**: Use `set -e` and proper cleanup
- **User Feedback**: Provide clear status messages with colors/emojis
- **Configuration**: Support command-line parameters with sensible defaults  
- **Cross-Platform**: Support both macOS and Linux when possible
- **Self-Documenting**: Include usage examples in script headers

## Requirements

### System Dependencies
- **Bash**: Version 4.0+ (most scripts)
- **curl**: HTTP client for API interactions
- **jq**: JSON processing and validation
- **timeout**: Command execution limits (GNU coreutils)

### CameoDB Dependencies
- **Rust Toolchain**: 1.90.0+ with Cargo (Rust 2024 Edition)
- **CameoDB Project**: Must be built (`cargo build --release`)

### Development Tools (Optional)
- **git**: Version control
- **tree**: Directory structure visualization
- **htop/ps**: Process monitoring

### Cargo Subcommands
Installed globally rather than pinned by the repo, so they drift on their own — see
[Dependency and Toolchain Updates](#dependency-and-toolchain-updates) for the dry run and
the one-at-a-time install.

- **cargo-audit**, **cargo-deny**: required by `validate/deps.sh`; it skips the check if either is missing
- **cargo-zigbuild**: the no-Docker fallback path in `build/build-musl.sh` (`BUILD_WITH=zig`)
- **cargo-deb**, **cargo-generate-rpm**: distro packages (`build/build-packages.sh`)
- **cargo-outdated**, **cargo-edit**: dependency surveys and manifest bumps
- **flamegraph**: profiling

## Troubleshooting

### Common Issues

**"Command not found: jq"**
- Run `./scripts/setup/install-deps.sh` to install missing dependencies

**"CameoDB not running on port 9480"**
- Start CameoDB: `cargo run --release --bin cameodb`
- Or use init script: `./scripts/setup/init-cluster.sh`

**"Permission denied"**
- Make script executable: `chmod +x scripts/path/to/script.sh`

**"Project build failed"**
- Verify Rust installation: `cargo --version`
- Check dependencies: `cargo check --workspace`

### Configuring Memory Limits for systemd

CameoDB has proactive memory budgets (per-shard writer caps, cache sizing from
`total_memory_limit_mb`, admission control via bounded worker semaphores) but no reactive
runtime OOM defense — nothing polls RSS and sheds load when usage climbs. Without a
systemd-level ceiling, a memory spike (bulk indexing on a many-shard node, a merge storm,
or a burst of large queries) grows the process until the kernel OOM killer sends SIGKILL,
which skips WAL drain and clean shutdown. The packaged unit
(`/usr/lib/systemd/system/cameodb.service` on RPM distros, `/lib/systemd/system/` on DEB)
ships without `MemoryHigh`/`MemoryMax` for that reason — add them per-host.

**Production unit for a memory-boxed deployment** (edit the installed unit, or prefer a
drop-in via `sudo systemctl edit cameodb.service` so package upgrades don't clobber it):

```ini
[Unit]
Description=CameoDB Database Server
After=network.target

[Service]
Type=simple
MemoryHigh=140G
MemoryMax=160G
Environment=MALLOC_CONF=background_thread:true,percpu_arena:percpu,oversize_threshold:0,dirty_decay_ms:1000,muzzy_decay_ms:0
ExecStartPre=/usr/local/bin/cameodb check-config --config /etc/cameodb/cameodb.toml
ExecStart=/usr/local/bin/cameodb --config /etc/cameodb/cameodb.toml
Restart=on-failure
User=cameodb
Group=cameodb
ProtectSystem=full
PrivateTmp=true

[Install]
WantedBy=multi-user.target
```

**What each line does:**
- `MemoryHigh=140G` — cgroup throttle: the kernel reclaims pages rather than killing.
  First line of defense; set to the steady-state RSS you expect from the `[search]` budgets.
- `MemoryMax=160G` — hard ceiling. systemd sends **SIGTERM** (graceful drain, WAL flush,
  clean restart) instead of the kernel's SIGKILL. Set above `MemoryHigh` to leave a
  throttle band before the kill.
- `dirty_decay_ms:1000` (down from the default 2000) — returns dirty jemalloc pages to the
  OS faster. Matters on hosts with many cores: `percpu_arena:percpu` creates one arena per
  CPU, and each retains dirty pages for `dirty_decay_ms`. On a 64-core box the default
  2000 ms can retain 10-20 GB across arenas during bulk indexing.
- `ExecStartPre=... check-config` — refuses to start in a posture the config does not
  satisfy, before the port opens. Do not drop this when editing the unit.

**Sizing guidance.** `MemoryHigh` should sit above the process's expected steady-state RSS
but below physical RAM minus OS/page-cache headroom. With 16 shards at
`indexer_memory_max_mb = 2048` and `total_memory_limit_mb = 96000`, steady-state during
bulk load is ~70-90 GB; `MemoryHigh=140G` / `MemoryMax=160G` on a 187 GB host leaves
headroom for spikes, page cache, and the OS. With 0 swap, there is no cushion — the
ceiling is the only thing between a spike and SIGKILL.

**Apply and verify:**
```bash
sudo systemctl daemon-reload
sudo systemctl restart cameodb
systemctl show cameodb.service -p MemoryHigh -p MemoryMax -p ExecStartPre
```
`MemoryHigh` should report `150323855360` (140 GiB in bytes) and `MemoryMax`
`171798691840` (160 GiB). If either shows `infinity`, the drop-in did not apply — check
`systemctl status cameodb` for parse errors.

**Watch after restart** (during the next bulk load):
- `GET /_admin/memory` — `process.vm_rss_kb` and `jemalloc.resident` should plateau
  around the sized steady state, not climb toward `MemoryMax`.
- `GET /_admin/workers` — `in_flight` vs `in_flight_capacity` per worker. At-capacity
  `in_flight` means the writer is the bottleneck (expected during bulk); deep
  `queue_depth` beside low `in_flight` means the semaphore is too tight.

### Getting Help
- Run `./scripts/setup/dev-info.sh` for project overview
- Check individual script headers for usage examples
- See `./docs/` directory for detailed project documentation
- Review `./docs/ARCHITECTURE.md` for system design information
