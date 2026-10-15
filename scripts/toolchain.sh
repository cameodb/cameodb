#!/usr/bin/env bash
# The pinned Rust toolchain, read from rust-toolchain.toml. Sourced, not run.
#
# rust-toolchain.toml is the only place the compiler version is written. Everything that builds
# outside rustup's reach — the Docker images, the musl builder images — takes it from here, and
# every release binary is checked against it, because a binary records the compiler that built
# it and nothing else in the release flow would notice a drift: 0.3.6's Linux binary was built by
# 1.95.0 from a builder image cached two releases earlier, while the repo said 1.98.1.

_TOOLCHAIN_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# The pinned version, e.g. 1.99.0.
rust_version() {
    local version
    version="$(sed -n 's/^channel *= *"\([^"]*\)".*/\1/p' "$_TOOLCHAIN_ROOT/rust-toolchain.toml")"
    if [ -z "$version" ]; then
        echo "no channel in $_TOOLCHAIN_ROOT/rust-toolchain.toml" >&2
        return 1
    fi
    printf '%s\n' "$version"
}

# The commit hash of the pinned rustc — what a binary built by it records as /rustc/<hash>.
rust_commit() {
    rustc "+$(rust_version)" -vV | sed -n 's/^commit-hash: //p'
}

# The rustc commit hashes a binary records, one per line: normally exactly one.
binary_rustc_commits() {
    LC_ALL=C grep -aoE '/rustc/[0-9a-f]{40}' "$1" | sed 's|/rustc/||' | sort -u
}
