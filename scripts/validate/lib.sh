#!/usr/bin/env bash
# Shared helpers for the validation suite.
#
# The suite is run by hand — there is no CI — so every check prints its own verdict as it
# goes. A run that scrolls past without a FAIL line is the record that the checks passed.

set -uo pipefail

PASS_COUNT=0
FAIL_COUNT=0
SKIP_COUNT=0
FAILED_CHECKS=()

_c_green=$'\033[0;32m'
_c_red=$'\033[0;31m'
_c_yellow=$'\033[0;33m'
_c_bold=$'\033[1m'
_c_off=$'\033[0m'

section() { printf '\n%s== %s ==%s\n' "$_c_bold" "$1" "$_c_off"; }

pass() {
    PASS_COUNT=$((PASS_COUNT + 1))
    printf '  %sPASS%s %s\n' "$_c_green" "$_c_off" "$1"
}

fail() {
    FAIL_COUNT=$((FAIL_COUNT + 1))
    FAILED_CHECKS+=("$1")
    printf '  %sFAIL%s %s\n' "$_c_red" "$_c_off" "$1"
    [ $# -gt 1 ] && printf '       %s\n' "$2"
    return 0
}

skip() {
    SKIP_COUNT=$((SKIP_COUNT + 1))
    printf '  %sSKIP%s %s\n' "$_c_yellow" "$_c_off" "$1"
}

# check <description> <expected> <actual>
check_eq() {
    if [ "$2" = "$3" ]; then
        pass "$1 ($3)"
    else
        fail "$1" "expected '$2', got '$3'"
    fi
}

summary() {
    printf '\n%s%s%s\n' "$_c_bold" "----------------------------------------" "$_c_off"
    printf 'passed: %d   failed: %d   skipped: %d\n' "$PASS_COUNT" "$FAIL_COUNT" "$SKIP_COUNT"
    if [ "$FAIL_COUNT" -gt 0 ]; then
        printf '%sFAILED:%s\n' "$_c_red" "$_c_off"
        for c in "${FAILED_CHECKS[@]}"; do printf '  - %s\n' "$c"; done
        return 1
    fi
    printf '%sAll checks passed.%s\n' "$_c_green" "$_c_off"
    return 0
}

repo_root() { git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel; }

# Locate the cameodb binary, preferring an explicit override.
cameodb_bin() {
    if [ -n "${CAMEODB_BIN:-}" ]; then
        printf '%s' "$CAMEODB_BIN"
        return 0
    fi
    local root
    root="$(repo_root)"
    for candidate in "$root/target/release/cameodb" "$root/target/debug/cameodb"; do
        [ -x "$candidate" ] && { printf '%s' "$candidate"; return 0; }
    done
    return 1
}

require_bin() {
    local bin
    if ! bin="$(cameodb_bin)"; then
        printf 'No cameodb binary found. Build one first:\n  cargo build --release\n' >&2
        exit 2
    fi
    printf '%s' "$bin"
}

# wait_for_http <url> <seconds> — succeeds as soon as the endpoint answers.
wait_for_http() {
    local url="$1" limit="${2:-30}" i=0
    while [ "$i" -lt "$limit" ]; do
        if curl -sk -m 2 -o /dev/null "$url"; then return 0; fi
        sleep 1
        i=$((i + 1))
    done
    return 1
}

# stop_server <pidfile-or-pid>
stop_server() {
    local pid="$1"
    [ -z "$pid" ] && return 0
    kill "$pid" 2>/dev/null || true
    local i=0
    while kill -0 "$pid" 2>/dev/null && [ "$i" -lt 20 ]; do sleep 1; i=$((i + 1)); done
    kill -9 "$pid" 2>/dev/null || true
}
