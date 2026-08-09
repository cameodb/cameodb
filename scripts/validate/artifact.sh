#!/usr/bin/env bash
# What a Linux release binary actually links against, and how it is hardened.
#
#   scripts/validate/artifact.sh [binary…]
#
# With no argument, checks every musl binary already built under target/.
#
# This exists because the interesting properties are all silently droppable. rustc falls back
# from `-static-pie` to `-static` with a warning when the linker does not advertise support,
# `cargo zigbuild` triggers exactly that, and the aarch64-musl target defaults to `-no-pie`
# in the first place — so "we pass the flag" and "the binary has the property" are different
# claims. Only the second one is checked here.
#
# Reading the ELF needs binutils for the target, which macOS does not have, so the checks run
# in a Linux container when the host cannot do it natively.

set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

ROOT="$(repo_root)"
cd "$ROOT" || exit 2

# `readelf`/`file` over one binary, however this host can manage it.
#
# Emits a small key=value block so the caller parses one format regardless of which path ran.
probe() {
    local binary="$1"
    local script='
      printf "kind=%s\n" "$(file -b "$B" | grep -o "static-pie linked\|statically linked\|dynamically linked" | head -1)"
      printf "interp=%s\n" "$(readelf -l "$B" 2>/dev/null | grep -c "Requesting program interpreter")"
      printf "needed=%s\n" "$(readelf -d "$B" 2>/dev/null | grep -c "(NEEDED)")"
      printf "relro=%s\n"  "$(readelf -l "$B" 2>/dev/null | grep -c "GNU_RELRO")"
      printf "bindnow=%s\n" "$(readelf -d "$B" 2>/dev/null | grep -c "BIND_NOW")"
      printf "stack=%s\n"  "$(readelf -l "$B" 2>/dev/null | grep -A1 "GNU_STACK" | grep -c "RWE")"
      printf "arch=%s\n"   "$(file -b "$B" | grep -o "x86-64\|ARM aarch64" | head -1)"
    '
    if command -v readelf > /dev/null 2>&1; then
        B="$binary" bash -c "$script"
    elif command -v docker > /dev/null 2>&1; then
        docker run --rm -v "$(cd "$(dirname "$binary")" && pwd)/$(basename "$binary")":/b:ro \
            -e B=/b cameo-builder-base:latest bash -c "$script" 2>/dev/null
    else
        return 1
    fi
}

check_binary() {
    local binary="$1"
    section "$(basename "$(dirname "$(dirname "$binary")")") — $binary"

    if [ ! -f "$binary" ]; then
        skip "$binary (not built)"
        return 0
    fi

    local out
    if ! out="$(probe "$binary")" || [ -z "$out" ]; then
        skip "$binary (no readelf here, and no container to borrow one from)"
        return 0
    fi
    local kind interp needed relro bindnow stack arch
    kind=$(sed -n 's/^kind=//p' <<< "$out")
    interp=$(sed -n 's/^interp=//p' <<< "$out")
    needed=$(sed -n 's/^needed=//p' <<< "$out")
    relro=$(sed -n 's/^relro=//p' <<< "$out")
    bindnow=$(sed -n 's/^bindnow=//p' <<< "$out")
    stack=$(sed -n 's/^stack=//p' <<< "$out")
    arch=$(sed -n 's/^arch=//p' <<< "$out")

    # The contract: nothing is resolved at load time, on any host, ever.
    check_eq "no dynamic loader" "0" "$interp"
    check_eq "no shared library dependencies" "0" "$needed"
    if [ "$kind" = "dynamically linked" ]; then
        fail "statically linked" "file(1) reports: $kind"
    else
        pass "statically linked ($kind, $arch)"
    fi

    # Hardening. A static binary that is not PIE loads at a fixed address on every host.
    #
    # Required on x86_64, where rustc defaults to static-pie. Not available on aarch64-musl:
    # forcing `-static-pie` there links and then segfaults before main, on a hello-world crate
    # with no dependencies, so it is the toolchain rather than anything CameoDB links. That is
    # reported rather than failed — but if it ever starts passing, the toolchain has been
    # fixed and .cargo/config.toml should turn it on.
    if [ "$kind" = "static-pie linked" ]; then
        pass "position independent (ASLR applies)"
    elif [ "$arch" = "ARM aarch64" ]; then
        skip "position independent — aarch64-musl static-pie segfaults; binary loads at a fixed address"
    else
        fail "position independent (ASLR applies)" \
            "static but not PIE. rustc drops -static-pie silently when the linker refuses it — cargo zigbuild does. Build via the container path (docs/BUILDING.md)."
    fi

    check_eq "read-only relocations (RELRO)" "1" "$relro"

    # BIND_NOW needs a dynamic section to bind, which a non-PIE static binary does not have.
    # Nothing is resolved lazily there either, so the property holds by construction.
    if [ "$kind" = "static-pie linked" ]; then
        check_eq "relocations bound at load (BIND_NOW)" "1" "$bindnow"
    else
        pass "no lazy binding (static, no dynamic section)"
    fi

    check_eq "non-executable stack" "0" "$stack"

    # A hardened binary that does not start is not a release artifact. Only meaningful when
    # this host can execute the target architecture.
    local runnable=""
    case "$(uname -s)/$(uname -m)/$arch" in
        Linux/aarch64/"ARM aarch64") runnable=yes ;;
        Linux/x86_64/"x86-64")       runnable=yes ;;
    esac
    if [ -n "$runnable" ]; then
        if "$binary" --version > /dev/null 2>&1; then
            pass "starts and reports its version"
        else
            fail "starts and reports its version" \
                "exited $? — a static-pie aarch64 build fails exactly this way"
        fi
    else
        skip "starts and reports its version (this host cannot execute $arch)"
    fi
}

TARGETS=("$@")
if [ ${#TARGETS[@]} -eq 0 ]; then
    while IFS= read -r found; do TARGETS+=("$found"); done < <(
        ls -1 target/*-unknown-linux-musl/release/cameodb \
              target/*-unknown-linux-musl/release-docker/cameodb 2>/dev/null
    )
fi

if [ ${#TARGETS[@]} -eq 0 ]; then
    section "release artifacts"
    skip "no musl binary under target/ (build one: scripts/build/build-musl.sh)"
    summary
    exit $?
fi

for binary in "${TARGETS[@]}"; do
    check_binary "$binary"
done

summary
exit $?
