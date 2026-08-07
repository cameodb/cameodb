#!/usr/bin/env bash
# Workspace test suite.
#
# Split out from the rest so `all.sh` can report it as its own line, and so a slow
# integration run can be skipped deliberately rather than by editing a command.

set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

ROOT="$(repo_root)"
cd "$ROOT" || exit 2

section "cargo test --workspace"
log="$(mktemp)"
trap 'rm -f "$log"' EXIT

if cargo test --workspace "$@" > "$log" 2>&1; then
    totals="$(grep -c '^test result: ok' "$log")"
    cases="$(grep -oE '^test result: ok\. [0-9]+' "$log" | awk '{s+=$4} END {print s+0}')"
    pass "$cases test(s) across $totals target(s)"
else
    fail "cargo test --workspace" "$(grep -E '^(test result: FAILED|---- |error)' "$log" | head -20)"
fi

summary
