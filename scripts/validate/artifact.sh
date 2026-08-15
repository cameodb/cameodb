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

PROBE_ERR="$(mktemp)"
trap 'rm -f "$PROBE_ERR"' EXIT

# Which image can lend a readelf, and how to run it.
#
# The build (scripts/build/build-musl.sh) has to match the image to the *target*, because
# musl-gcc only compiles for its own architecture. Reading an ELF is not like that: an arm64
# readelf describes an x86_64 binary exactly as an x86_64 readelf does. So borrow whichever
# builder image this host already has, preferring its own architecture so nothing runs under
# emulation. Asking for one specific image is what made this skip on a host that had the other.
PROBE_IMAGE=""
PROBE_PLATFORM=""
PROBE_BLOCKED=""

# The CLI existing is not the daemon answering, and both paths below need the answer, so ask
# once. `docker info` is the difference between "no container runtime" and a run that fails
# once per binary.
DOCKER_OK=""
docker_ready() {
    if [ -z "$DOCKER_OK" ]; then
        if command -v docker > /dev/null 2>&1 && docker info > /dev/null 2>&1; then
            DOCKER_OK=yes
        else
            DOCKER_OK=no
        fi
    fi
    [ "$DOCKER_OK" = yes ]
}

# The builder image for one target architecture, whether or not this host shares it.
# Executing a binary — unlike reading one — does need the architectures to agree.
image_for_arch() {
    case "$1" in
        "x86-64")      printf 'cameo-builder-base-amd64:latest linux/amd64' ;;
        "ARM aarch64") printf 'cameo-builder-base:latest linux/arm64' ;;
        *)             return 1 ;;
    esac
}

select_probe_image() {
    if ! command -v docker > /dev/null 2>&1; then
        PROBE_BLOCKED="no docker on this host"
        return 1
    fi
    if ! docker_ready; then
        PROBE_BLOCKED="docker is installed but no daemon is answering (start OrbStack)"
        return 1
    fi
    local candidates i
    case "$(uname -m)" in
        arm64 | aarch64)
            candidates=(cameo-builder-base:latest linux/arm64 cameo-builder-base-amd64:latest linux/amd64) ;;
        *)
            candidates=(cameo-builder-base-amd64:latest linux/amd64 cameo-builder-base:latest linux/arm64) ;;
    esac
    for ((i = 0; i < ${#candidates[@]}; i += 2)); do
        if docker image inspect "${candidates[i]}" > /dev/null 2>&1; then
            PROBE_IMAGE="${candidates[i]}"
            PROBE_PLATFORM="${candidates[i + 1]}"
            return 0
        fi
    done
    PROBE_BLOCKED="no builder image here (build one: scripts/build/build-musl.sh, or"
    PROBE_BLOCKED="$PROBE_BLOCKED docker build -f docker/Dockerfile.builder -t ${candidates[0]} docker/)"
    return 1
}

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
    elif [ -n "$PROBE_IMAGE" ]; then
        # Keep stderr: a failed run has to be able to say why, rather than reaching the caller
        # as an empty result indistinguishable from having no container at all.
        docker run --rm --platform "$PROBE_PLATFORM" \
            -v "$(cd "$(dirname "$binary")" && pwd)/$(basename "$binary")":/b:ro \
            -e B=/b "$PROBE_IMAGE" bash -c "$script" 2> "$PROBE_ERR"
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
        local why="${PROBE_BLOCKED:-$(tail -1 "$PROBE_ERR")}"
        skip "$binary (no readelf here; ${why:-the container probe returned nothing})"
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

    # A hardened binary that does not start is not a release artifact. Run it natively where
    # the host allows; otherwise borrow the builder image for the binary's own architecture and
    # let the runtime emulate it. Emulation is enough for what this check is for — a
    # static-pie that segfaults before main does it under emulation too — but it is not a
    # statement about how the binary behaves on real hardware, so the verdict says how it ran.
    local runnable="" exec_image="" exec_platform=""
    case "$(uname -s)/$(uname -m)/$arch" in
        Linux/aarch64/"ARM aarch64") runnable=native ;;
        Linux/x86_64/"x86-64")       runnable=native ;;
    esac
    if [ -z "$runnable" ] && docker_ready; then
        local pair
        if pair="$(image_for_arch "$arch")"; then
            exec_image="${pair%% *}"
            exec_platform="${pair##* }"
            docker image inspect "$exec_image" > /dev/null 2>&1 && runnable=container
        fi
    fi

    case "$runnable" in
        native)
            if "$binary" --version > /dev/null 2>&1; then
                pass "starts and reports its version"
            else
                fail "starts and reports its version" \
                    "exited $? — a static-pie aarch64 build fails exactly this way"
            fi
            ;;
        container)
            local reported status detail
            reported="$(docker run --rm --platform "$exec_platform" \
                -v "$(cd "$(dirname "$binary")" && pwd)/$(basename "$binary")":/b:ro \
                "$exec_image" /b --version 2> "$PROBE_ERR")"
            status=$?
            if [ "$status" -eq 0 ]; then
                pass "starts and reports its version (emulated $arch: $(head -1 <<< "$reported"))"
            elif grep -qi 'exec format error\|exec user process' "$PROBE_ERR"; then
                # No binfmt/Rosetta for this architecture. Nothing was learned about the binary.
                skip "starts and reports its version ($arch cannot be emulated on this host)"
            else
                # The failure this check exists for is silent: a binary that dies before main
                # writes nothing, and the exit status is the whole message.
                detail="$(tail -1 "$PROBE_ERR")"
                if [ -z "$detail" ]; then
                    detail="exited $status"
                    [ "$status" -ge 128 ] && detail="$detail (killed by signal $((status - 128)))"
                fi
                fail "starts and reports its version" "under emulation in $exec_image: $detail"
            fi
            ;;
        *)
            skip "starts and reports its version (this host cannot execute $arch, and has no image that can)"
            ;;
    esac
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

# Only borrow a container when this host cannot read an ELF itself, and say so when it does:
# the numbers below then carry where they came from.
if ! command -v readelf > /dev/null 2>&1; then
    if select_probe_image; then
        printf 'readelf via %s (%s)\n' "$PROBE_IMAGE" "$PROBE_PLATFORM"
    fi
fi

for binary in "${TARGETS[@]}"; do
    check_binary "$binary"
done

summary
exit $?
