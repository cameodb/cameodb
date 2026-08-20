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

# Falls back to walking up from this file when git is unavailable — artifact.sh is meant to
# be runnable inside a bare Linux container, which has readelf but no git and no checkout.
repo_root() {
    git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel 2>/dev/null && return 0
    ( cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd )
}

# Locate the cameodb binary, preferring an explicit override.
#
# Release is preferred over debug because that is what ships. That makes a debug build the
# easy mistake: `cargo build` leaves a newer target/debug alongside a stale target/release,
# and the suite goes on quietly probing the old binary — so a defect that was just fixed
# still fails, and one that was just introduced still passes. Say so rather than guess.
_STALE_BIN_WARNED=""
cameodb_bin() {
    if [ -n "${CAMEODB_BIN:-}" ]; then
        printf '%s' "$CAMEODB_BIN"
        return 0
    fi
    local root release debug
    root="$(repo_root)"
    release="$root/target/release/cameodb"
    debug="$root/target/debug/cameodb"
    if [ -z "$_STALE_BIN_WARNED" ] && [ -x "$release" ] && [ -x "$debug" ] && [ "$debug" -nt "$release" ]; then
        _STALE_BIN_WARNED=1
        printf '%sWarning:%s target/debug/cameodb is newer than target/release/cameodb.\n' \
            "$_c_yellow" "$_c_off" >&2
        printf '         Testing the release build anyway. Run `cargo build --release`, or set\n' >&2
        printf '         CAMEODB_BIN=%s to test the debug build deliberately.\n' "$debug" >&2
    fi
    for candidate in "$release" "$debug"; do
        [ -x "$candidate" ] && { printf '%s' "$candidate"; return 0; }
    done
    return 1
}

# require_free_port <port> — refuse to run when something already holds the port.
#
# `wait_for_http` only proves that *something* answers. A node left behind by an aborted run
# answers just as well as the one the suite meant to start — with whatever config it was given
# — while the server this suite launched exits on a failed bind. Every check then reports on a
# stranger, and the ones that happen to agree with its config pass. Stop instead.
require_free_port() {
    local port="$1" holder=""
    # `lsof` is asked first and trusted alone where it exists, because `nc -z` has to open a
    # real connection to answer. A single-shot listener counts that as its one client and
    # exits, so the probe clears the very port it was asked about and then reports it free —
    # and the `lsof` line that should name the culprit comes back empty.
    if command -v lsof > /dev/null 2>&1; then
        holder="$(lsof -nP -iTCP:"$port" -sTCP:LISTEN 2>/dev/null | tail -n +2)"
        [ -z "$holder" ] && return 0
    elif ! nc -z 127.0.0.1 "$port" > /dev/null 2>&1; then
        return 0
    fi
    printf '%sPort %s is already in use%s — refusing to probe a server this suite did not start.\n' \
        "$_c_red" "$port" "$_c_off" >&2
    [ -n "$holder" ] && printf '%s\n' "$holder" >&2
    printf 'Stop it, or point this suite elsewhere with its port override, and re-run.\n' >&2
    exit 2
}

# discard_work <dir> — drop a suite's scratch directory, but only on a clean run.
#
# The server logs in it are the only account of *why* a check failed, and a FAIL that deletes
# its own evidence costs a full re-run to say anything about itself.
discard_work() {
    local dir="$1"
    [ -z "$dir" ] && return 0
    if [ "$FAIL_COUNT" -gt 0 ]; then
        printf '\nScratch directory kept for inspection: %s\n' "$dir" >&2
        return 0
    fi
    rm -rf "$dir"
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
