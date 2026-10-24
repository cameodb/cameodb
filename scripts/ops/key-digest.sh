#!/usr/bin/env bash
# Print the `sha256:<hex>` digest of an API key held in a file — the value for `key_hash` or
# `key_hash_file` — and nothing else. The key itself is never printed.
#
# The digest covers the 43-character body only, not the `cameo_v1_` prefix. Use this to
# recompute the digest of a key minted by 0.3.7 or earlier, whose configured digest covered
# the whole string: the key itself is unchanged and keeps working once the server holds the
# digest printed here.
#
#   scripts/ops/key-digest.sh ~/keys/ops.key

set -euo pipefail

if [ $# -ne 1 ] || [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
    echo "usage: $(basename "$0") <key-file>" >&2
    exit 2
fi
if [ ! -r "$1" ]; then
    echo "error: cannot read $1" >&2
    exit 1
fi

key="$(tr -d '[:space:]' < "$1")"
body="${key#cameo_v1_}"
if [ "$body" = "$key" ] || ! [[ "$body" =~ ^[A-Za-z0-9_-]{43}$ ]]; then
    unset key body
    echo "error: $1 does not hold a CameoDB API key (cameo_v1_ then 43 base64url characters)" >&2
    exit 1
fi

if command -v sha256sum > /dev/null 2>&1; then
    hex="$(printf '%s' "$body" | sha256sum | cut -d' ' -f1)"
else
    hex="$(printf '%s' "$body" | shasum -a 256 | cut -d' ' -f1)"
fi
unset key body
printf 'sha256:%s\n' "$hex"
