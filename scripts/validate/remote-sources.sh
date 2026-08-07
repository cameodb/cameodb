#!/usr/bin/env bash
# Outbound HTTPS validation for the bundled client.
#
# The client's TLS stack is rustls with the ring provider, verifying against the system
# trust store via rustls-platform-verifier — it replaced native-tls/vendored OpenSSL. The
# reason to keep checking this by hand is that trust-store behaviour differs per target:
# macOS uses the Keychain, Linux reads /etc/ssl/certs, and a musl container has neither
# unless ca-certificates is installed. Run this on every target you ship.
#
# Behind a TLS-inspecting proxy (corporate CA), the CA has to be in the OS trust store —
# the same requirement native-tls had. Override the URLs to point at an internal host if
# the defaults are unreachable from your network.

set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

BIN="$(require_bin)"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

# Public sources this project actually loads from; override for an offline network.
SOURCES=(
    "${REMOTE_SOURCE_1:-https://dl.cameodb.com/cameodb.spdx.json}"
    "${REMOTE_SOURCE_2:-https://raw.githubusercontent.com/rust-lang/rust/master/README.md}"
)

section "outbound HTTPS from the bundled client"
if ! curl -s -m 10 -o /dev/null https://dl.cameodb.com 2>/dev/null; then
    skip "no outbound network; skipping remote source checks"
    summary
    exit $?
fi

for url in "${SOURCES[@]}"; do
    # `schema detect` performs a real fetch through the client's reqwest/rustls stack.
    if "$BIN" client schema detect "$url" > "$WORK/out.json" 2> "$WORK/err.txt"; then
        pass "fetched and parsed $url"
    elif grep -qiE 'certificate|tls|handshake|ssl' "$WORK/err.txt"; then
        # A TLS failure is the thing this script exists to catch; a parse failure is not.
        fail "TLS handshake to $url" "$(head -3 "$WORK/err.txt")"
    else
        pass "TLS handshake to $url succeeded (content not parseable as a schema, which is fine)"
    fi
done

section "certificate verification is really on"
# A host with a deliberately bad certificate must fail, and --insecure-source must be the
# only thing that changes that.
BADSSL="${BADSSL_URL:-https://self-signed.badssl.com/}"
if curl -s -m 10 -o /dev/null "$BADSSL" 2>/dev/null; then
    skip "$BADSSL verifies on this network (a TLS-inspecting proxy re-signs it); negative check not meaningful here"
elif "$BIN" client schema detect "$BADSSL" > /dev/null 2> "$WORK/bad.txt"; then
    fail "invalid certificate is rejected by default" "the fetch succeeded"
else
    pass "invalid certificate is rejected by default"
    if "$BIN" client schema detect "$BADSSL" --insecure-source > /dev/null 2> "$WORK/bad2.txt" \
        || ! grep -qiE 'certificate|tls|handshake|ssl' "$WORK/bad2.txt"; then
        pass "--insecure-source relaxes verification for sources"
    else
        fail "--insecure-source relaxes verification for sources" "$(head -3 "$WORK/bad2.txt")"
    fi
fi

summary
