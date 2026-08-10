# Development Environment

Getting a clean macOS or Linux machine ready to build, test and validate CameoDB.

> Looking for the cluster implementation plan that used to live here? Its two standing
> decisions became [ADR 004](ADR.md) and [ADR 005](ADR.md); the phase checklists are in
> [ROADMAP.md](../ROADMAP.md).

## What you need

| Tool | Why | Required? |
|---|---|---|
| **Rust 1.85+** | The workspace is `edition = "2024"`. Older toolchains fail while *parsing* `Cargo.toml`, with an error that does not mention the version | **Yes** |
| **A C toolchain** | Linking. Xcode Command Line Tools on macOS; `build-essential` or equivalent on Linux | **Yes** |
| `jq` | Every validation and testing script parses JSON with it | **Yes** |
| `curl` | Same. Preinstalled on macOS and most Linux | **Yes** |
| `cargo-audit`, `cargo-deny` | The supply-chain half of `scripts/validate/deps.sh`. **Without them that suite SKIPs both and still reports success** — the gate passes without ever having run | Strongly recommended |
| `openssl` | `scripts/validate/tls.sh` generates a throwaway certificate; the suite skips without it | For TLS validation |
| **Docker** | musl release builds (the container path is the one that matches the published image), and the multi-node compose files | For releases and cluster work |
| `zig`, `cargo-zigbuild` | The no-Docker fallback for musl cross-compilation | Optional |
| **Python 3.9+** | `examples/ingest_*.py` and the sample-data scripts | For the examples |

Nothing in the build needs OpenSSL as a *library*: TLS is rustls with the `ring` provider,
so there is no C crypto dependency and nothing to vendor.

## macOS, from a clean machine

Written against Apple Silicon; the commands are identical on Intel.

```bash
# 1. Compiler and linker
xcode-select --install                     # skip if `xcode-select -p` already prints a path

# 2. Homebrew, if you do not have it
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# 3. Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source "$HOME/.cargo/env"

# 4. Everything else
brew install jq openssl
cargo install cargo-audit cargo-deny       # a few minutes; they compile from source

# 5. Optional, for cross-compiling Linux binaries without Docker
brew install zig
cargo install cargo-zigbuild
rustup target add aarch64-unknown-linux-musl x86_64-unknown-linux-musl

# 6. Docker Desktop, if you will build release artifacts or run a cluster
#    https://www.docker.com/products/docker-desktop/
```

Then check the machine before trusting it:

```bash
./scripts/setup/install-deps.sh
```

It verifies each prerequisite, reports what is missing with the command that installs it, and
builds the workspace. Anything it calls *optional* is genuinely optional — you can develop,
test and run a node without Docker.

## Linux, from a clean machine

```bash
sudo apt-get update && sudo apt-get install -y build-essential pkg-config jq curl openssl
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh && source "$HOME/.cargo/env"
cargo install cargo-audit cargo-deny
./scripts/setup/install-deps.sh
```

Substitute `dnf`/`yum`/`pacman` as appropriate; `install-deps.sh` recognises all four.

## Build, test, validate

```bash
cargo build --release            # target/release/cameodb, plus the bundled client
cargo test --workspace           # unit tests and the integration suites
./scripts/validate/all.sh        # the gate: deps, unit, posture, auth, tls, remote-sources, artifact
```

`validate/all.sh` builds a release binary and drives a real node — expect a few minutes on a
first run. Read its summary rather than only its exit code: a suite that **SKIPs** is a check
that did not run, and the most common cause is a missing `cargo-audit` or `cargo-deny`.

Run a subset by name, or a suite on its own:

```bash
./scripts/validate/all.sh posture tls    # named suites only
./scripts/validate/auth.sh               # ~111 checks against a live node with real keys
CAMEODB_BIN=/path/to/cameodb ./scripts/validate/all.sh   # test a binary you did not just build
```

What each suite proves, and why none of them could be a unit test, is tabulated in
[scripts/validate/README.md](../scripts/validate/README.md).

## Running a node

```bash
cargo run --release --bin cameodb                      # ./cameodb.toml, or built-in defaults
cargo run --release --bin cameodb -- -c path/to.toml   # explicit config
./target/release/cameodb generate-config > cameodb.toml
./target/release/cameodb check-config -c cameodb.toml  # posture report, no server started
```

Config resolution, highest precedence first: command-line options, `CAMEODB_*` environment
variables, the config file, then defaults. The file itself is found via `-c`, then
`CAMEODB_CONFIG`, then `./cameodb.toml`, `./config/cameodb.toml`,
`/etc/cameodb/cameodb.toml`, `/etc/cameodb/config.toml`.

Then, in another shell:

```bash
./scripts/testing/test-api.sh        # exercises the HTTP surface
./scripts/ops/health-check.sh        # health, memory, worker pool
./examples/data/sample-data.sh       # 100 sample documents
cargo run --release --bin cameodb -- client health
```

Full settings reference: [CONFIGURATION.md](CONFIGURATION.md). Endpoint reference:
[API_REFERENCE.md](API_REFERENCE.md).

## Where macOS differs from where it runs

Your development machine cannot exercise several things production depends on. Knowing which
saves a day of chasing a difference that is not a bug:

- **Core pinning is a no-op.** macOS exposes no thread-affinity API, so `core_affinity` fails
  silently and `/_admin/workers` reports every worker unpinned. Stage 1/2d/2e behaviour has
  to be validated on Linux — see the ROADMAP note recording exactly that.
- **jemalloc is not used.** It is gated to `target_env = "musl"` and Linux-gnu, and the
  `#[global_allocator]` is `#[cfg(target_os = "linux")]`. macOS builds use the system
  allocator, so `/_admin/memory` reports no jemalloc block and the `JEMALLOC_SYS_WITH_LG_PAGE`
  pin in `.cargo/config.toml` is inert here. (That matters on Apple Silicon, whose 16 KiB
  pages would otherwise collide with a jemalloc compiled for 4 KiB.)
- **Static musl binaries cannot be produced natively.** Use Docker or zigbuild; see below.
- **`scripts/validate/artifact.sh` SKIPs** without a Linux binary under `target/`, and needs
  `readelf` or Docker to inspect one.

## Cross-compiling Linux binaries

```bash
./scripts/build/build-musl.sh                  # release, x86_64
./scripts/build/build-musl.sh release aarch64
./scripts/build/build-musl.sh release both
./scripts/validate/artifact.sh target/aarch64-unknown-linux-musl/release/cameodb
```

The script prefers a Linux container matching the target architecture, falling back to
`cargo-zigbuild` when Docker is unavailable or with `BUILD_WITH=zig`. **The two paths do not
produce the same artifact** — zig's linker does not advertise `-static-pie`, so the binary is
static but not position-independent. Prefer the container path for anything you ship.

On Apple Silicon, `aarch64` builds in a native container and `x86_64` builds under emulation,
which is markedly slower. Build the architecture you are testing and leave `both` for release
day. Full detail, including the `AR`/`RANLIB` trap when invoking `cargo zigbuild` by hand, is
in [BUILDING.md](BUILDING.md).

## Workspace layout

| Crate | Contents |
|---|---|
| `crates/server` | The `cameodb` binary: HTTP, MCP, orchestrator, auth, config. Binary-only, so its `tests/` drive the built binary as a subprocess |
| `crates/client` | The `CameoClient` SDK, and the `cameodb client` CLI |
| `crates/storage` | The redb + tantivy hybrid store |
| `crates/cluster` | Consistent-hash ring, identity, membership |
| `crates/mcp` | The MCP protocol layer. Deliberately holds no deployment policy — the server crate supplies authorization, rate limiting and audit through trait hooks |
| `crates/bench` | `cameodb-bench`, the latency harness. Not shipped |

## Common tasks

| Task | Command |
|---|---|
| Fast type-check | `cargo check --workspace` |
| Lint as the gate does | `cargo clippy --workspace --all-targets -- -D warnings` |
| Format | `cargo fmt --all` |
| One test by name | `cargo test -p server --bin cameodb <substring>` |
| Integration suite only | `cargo test -p server --test audit_trail` |
| Log level | `RUST_LOG=debug cargo run --bin cameodb` |
| Audit trail only | `RUST_LOG=warn,cameodb::audit=info` |
| Benchmark | `cargo run --release -p bench -- --mode write --concurrency 64` |
| What scripts exist | `./scripts/setup/dev-info.sh` |

## Troubleshooting

**`feature edition2024 is required` / errors parsing `Cargo.toml`** — the toolchain predates
1.85. `rustup update stable`.

**`linker 'cc' not found`** on macOS — Xcode Command Line Tools are missing.
`xcode-select --install`.

**`undefined symbol: mallocx` / `mallctl`** when cross-compiling — you invoked
`cargo zigbuild` directly without `AR="zig ar"` and `RANLIB="zig ranlib"`. Use
`build-musl.sh`, which sets both. The full explanation is in [BUILDING.md](BUILDING.md).

**`validate/all.sh` says all checks passed but reports SKIPs** — read the SKIP lines. The
usual pair is `cargo audit` and `cargo deny`, and until they are installed nothing has
checked your dependencies for advisories or licence violations.

**Tests fail on ports** — the integration suites bind ephemeral ports and start real nodes.
A previous run killed mid-test can leave a `cameodb` process holding a data directory:
`pkill -f 'target/debug/cameodb'`.

**Builds feel slow** — `.cargo/config.toml` sets `jobs = 8`. On a machine with more cores,
raise it or remove the line to use the default.
