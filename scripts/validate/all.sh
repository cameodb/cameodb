#!/usr/bin/env bash
# Run the whole validation suite and print one table.
#
#   scripts/validate/all.sh                 # everything
#   scripts/validate/all.sh posture tls     # named suites only
#
# Requires a built binary (cargo build --release, or set CAMEODB_BIN).

set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

ALL_SUITES=(deps unit posture auth tls remote-sources artifact)
SUITES=("${@:-}")
[ -z "${SUITES[0]:-}" ] && SUITES=("${ALL_SUITES[@]}")

declare -a RESULTS
started="$(date '+%Y-%m-%d %H:%M:%S')"
overall=0

for suite in "${SUITES[@]}"; do
    script="$SCRIPT_DIR/$suite.sh"
    if [ ! -x "$script" ]; then
        printf 'Unknown suite: %s (available: %s)\n' "$suite" "${ALL_SUITES[*]}" >&2
        exit 2
    fi
    printf '\n%s########## %s ##########%s\n' "$_c_bold" "$suite" "$_c_off"
    if "$script"; then
        RESULTS+=("PASS $suite")
    else
        RESULTS+=("FAIL $suite")
        overall=1
    fi
done

printf '\n%s================ SUMMARY ================%s\n' "$_c_bold" "$_c_off"
printf 'started: %s\nfinished: %s\nbinary: %s\n\n' \
    "$started" "$(date '+%Y-%m-%d %H:%M:%S')" "$(cameodb_bin || echo '(none)')"
for r in "${RESULTS[@]}"; do
    case "$r" in
        PASS*) printf '  %sPASS%s %s\n' "$_c_green" "$_c_off" "${r#PASS }" ;;
        FAIL*) printf '  %sFAIL%s %s\n' "$_c_red" "$_c_off" "${r#FAIL }" ;;
    esac
done

if [ "$overall" -ne 0 ]; then
    printf '\n%sValidation failed.%s Record the failures in RELEASE-CHECKLIST.md.\n' "$_c_red" "$_c_off"
else
    printf '\n%sValidation passed.%s Paste this summary into RELEASE-CHECKLIST.md for the release.\n' "$_c_green" "$_c_off"
fi
exit "$overall"
