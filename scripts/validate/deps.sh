#!/usr/bin/env bash
# Supply-chain and lint gate.
#
# Nothing here runs automatically, so it runs here — before a release, and whenever the
# lockfile changes. `cargo deny` carries dated advisory exceptions; if one has expired
# this script says so, which is the mechanism that stops "temporarily ignored" from
# meaning "ignored forever".

set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

ROOT="$(repo_root)"
cd "$ROOT" || exit 2

run_tool() {
    local name="$1"; shift
    if ! command -v "$1" > /dev/null && [ "$1" != "cargo" ]; then
        skip "$name (not installed)"
        return 0
    fi
    if "$@" > /tmp/validate-$$.log 2>&1; then
        pass "$name"
    else
        fail "$name" "$(tail -25 /tmp/validate-$$.log)"
    fi
    rm -f /tmp/validate-$$.log
}

section "formatting and lints"
run_tool "cargo fmt --check" cargo fmt --all -- --check
run_tool "cargo clippy (warnings denied)" cargo clippy --workspace --all-targets -- -D warnings

section "advisories and licences"
if cargo audit --version > /dev/null 2>&1; then
    run_tool "cargo audit" cargo audit
else
    skip "cargo audit (install with: cargo install cargo-audit)"
fi

if cargo deny --version > /dev/null 2>&1; then
    run_tool "cargo deny check" cargo deny check
else
    skip "cargo deny (install with: cargo install cargo-deny)"
fi

section "advisory exception expiry"
# Each ignore in deny.toml carries a `review-by` date in its reason. An exception past its
# date is reported here rather than quietly persisting for another year.
today="$(date +%Y-%m-%d)"
expired=0
while IFS= read -r line; do
    id="$(sed -n 's/.*id = "\([^"]*\)".*/\1/p' <<< "$line")"
    date_field="$(sed -n 's/.*review-by \([0-9-]\{10\}\).*/\1/p' <<< "$line")"
    [ -z "$id" ] && continue
    if [ -z "$date_field" ]; then
        fail "advisory $id has a review-by date" "add 'review-by YYYY-MM-DD' to its reason in deny.toml"
        continue
    fi
    if [[ "$date_field" < "$today" ]]; then
        fail "advisory $id exception is current" "review-by $date_field has passed; recheck for an upstream fix"
        expired=$((expired + 1))
    else
        pass "advisory $id exception valid until $date_field"
    fi
done < <(grep -E '^\s*\{\s*id = ' deny.toml 2>/dev/null)

summary
