#!/usr/bin/env bash
# Replace whole-token key digests with body-only ones, once, on the node that holds them.
#
# Up to 0.3.7 a configured digest was SHA-256 of the whole key, `cameo_v1_` included; it is now
# SHA-256 of the 43-character body. For each key file in KEY_DIR this computes both digests,
# finds the files in HASH_DIR holding the old one, and replaces it with the new one. Files are
# matched by digest, not by name, and rewritten in place, so owner, group and mode are kept:
# nothing to chown afterwards. A file the cameodb service cannot read is flagged with the fix.
# Neither key nor anything derived from it but its digests is printed.
#
#   scripts/ops/key-rehash.sh [--write] KEY_DIR [HASH_DIR]
#
# KEY_DIR holds the key files, one key per file. HASH_DIR holds the key_hash_file digests and
# defaults to /etc/cameodb/keys. Without --write it only reports what it would change. Run it as root, alongside the upgrade to the build that
# hashes the body: an older node does not accept the new digests. Then reload:
#
#   systemctl reload cameodb

set -euo pipefail

write=0
if [ "${1:-}" = "--write" ]; then
    write=1
    shift
fi
if [ "${1:-}" = "-h" ] || [ "${1:-}" = "--help" ] || [ $# -lt 1 ] || [ $# -gt 2 ]; then
    echo "usage: $(basename "$0") [--write] KEY_DIR [HASH_DIR]   (HASH_DIR: /etc/cameodb/keys)" >&2
    exit 2
fi
key_dir="$1"
hash_dir="${2:-/etc/cameodb/keys}"
for dir in "$key_dir" "$hash_dir"; do
    if [ ! -d "$dir" ]; then
        echo "error: not a directory: $dir" >&2
        exit 1
    fi
done

sha256() {
    if command -v sha256sum > /dev/null 2>&1; then
        sha256sum | cut -d' ' -f1
    else
        shasum -a 256 | cut -d' ' -f1
    fi
}

# GNU form first, BSD second: GNU stat reads -f as "filesystem" and still exits 0.
owner_mode() {
    stat -c '%U:%G %a' "$1" 2>/dev/null || stat -f '%Su:%Sg %OLp' "$1"
}

# The rewrite keeps owner, group and mode, so a file the service could not read before still
# cannot. Say so here rather than at the reload. The service runs as cameodb.
check_readable() {
    local owner group mode
    read -r owner mode <<< "$(owner_mode "$1")"
    group="${owner#*:}"
    owner="${owner%%:*}"
    if [ "$owner" != "cameodb" ] && ! { [ "$group" = "cameodb" ] && (((8#$mode >> 3) & 4)); } \
        && ! (((8#$mode) & 4)); then
        echo "       ⚠️  $1 is $owner:$group $mode, not readable by cameodb:" \
            "chgrp cameodb '$1' && chmod 0640 '$1'"
    fi
}

tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT

changed=0
unmatched=0
for key_file in "$key_dir"/*; do
    [ -f "$key_file" ] || continue
    name="$(basename "$key_file")"
    key="$(tr -d '[:space:]' < "$key_file")"
    body="${key#cameo_v1_}"
    if [ "$body" = "$key" ] || ! [[ "$body" =~ ^[A-Za-z0-9_-]{43}$ ]]; then
        echo "skip   $name: not a cameo_v1_ key"
        continue
    fi
    old="sha256:$(printf '%s' "$key" | sha256)"
    new="sha256:$(printf '%s' "$body" | sha256)"
    unset key body

    hits="$(grep -lF "$old" "$hash_dir"/* 2>/dev/null || true)"
    if [ -z "$hits" ]; then
        if grep -qlF "$new" "$hash_dir"/* 2>/dev/null; then
            echo "done   $name: already body-only (${new:7:8})"
        else
            echo "absent $name: no digest for this key in $hash_dir"
            unmatched=$((unmatched + 1))
        fi
        continue
    fi
    while IFS= read -r hash_file; do
        echo "change $name: $hash_file ($(owner_mode "$hash_file"))  ${old:7:8} -> ${new:7:8}"
        if [ "$write" = 1 ]; then
            sed "s/$old/$new/g" "$hash_file" > "$tmp"
            cat "$tmp" > "$hash_file"
        fi
        check_readable "$hash_file"
        changed=$((changed + 1))
    done <<< "$hits"
done

# A digest pasted inline into the config is not in HASH_DIR, and is left for a hand edit.
config="$(dirname "$hash_dir")/cameodb.toml"
if [ -f "$config" ] && grep -q 'key_hash *= *"sha256:' "$config"; then
    echo "note   $config has inline key_hash entries; recompute those with key-digest.sh"
fi

if [ "$write" = 1 ]; then
    echo "$changed file(s) rewritten, $unmatched key(s) without a digest. Now: systemctl reload cameodb"
else
    echo "$changed file(s) would change, $unmatched key(s) without a digest. Re-run with --write."
fi
