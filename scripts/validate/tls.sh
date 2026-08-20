#!/usr/bin/env bash
# TLS validation: the server must actually serve HTTPS, and must refuse bad material
# loudly and early rather than after announcing itself as started.
#
# This exists because TLS shipped completely broken — rustls had two crypto providers in
# the dependency graph and panicked at first use, *after* the startup banner printed. No
# unit test can catch that; only binding a real socket can.

set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

BIN="$(require_bin)"
WORK="$(mktemp -d)"
PORT="${TLS_PORT:-19491}"
BASE="https://127.0.0.1:$PORT"
SERVER_PID=""

cleanup() {
    stop_server "$SERVER_PID"
    discard_work "$WORK"
}
trap cleanup EXIT

# Before anything binds: a leftover node on this port would answer every probe below.
require_free_port "$PORT"

if ! command -v openssl > /dev/null; then
    skip "openssl not available; cannot generate test certificates"
    summary
    exit $?
fi

section "certificate material"
# rustls refuses an X.509 v1 certificate outright (UnsupportedCertVersion), and `openssl
# req -x509` only emits v3 when the certificate carries at least one extension — the system
# LibreSSL on macOS emits v1 without one. So ask for the extensions a server certificate
# should have anyway, and assert the version here: a v1 certificate makes every check below
# read as "the server cannot serve HTTPS" when what happened is that this probe generated
# something no TLS stack will load.
openssl req -x509 -newkey rsa:2048 -keyout "$WORK/key.pem" -out "$WORK/cert.pem" \
    -days 1 -nodes -subj "/CN=localhost" \
    -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" \
    -addext "basicConstraints=critical,CA:FALSE" > /dev/null 2>&1
cert_version="$(openssl x509 -in "$WORK/cert.pem" -noout -text 2>/dev/null \
    | sed -n 's/.*Version: \([0-9]\).*/\1/p' | head -1)"
if [ ! -s "$WORK/cert.pem" ]; then
    fail "generated a self-signed certificate" "$(openssl version) produced no certificate"
elif [ "$cert_version" != "3" ]; then
    fail "generated a self-signed certificate" \
        "$(openssl version) produced an X.509 v${cert_version:-?} certificate; rustls loads v3 only"
else
    pass "generated a self-signed certificate (X.509 v3)"
fi

write_config() {
    cat > "$WORK/node.toml" <<EOF
[node]
label = "tls-probe"
profile = "local"

[network.http]
bind_address = "127.0.0.1"
port = $PORT
cors_allowed_origins = []

[network.http.tls]
enabled = true
cert_file = "$1"
key_file = "$2"

[network.cluster]
enabled = false

[storage]
data_paths = ["$WORK/data"]
num_shards_init = 1
max_shards_per_node = 2
EOF
}

section "serving HTTPS"
write_config "$WORK/cert.pem" "$WORK/key.pem"
"$BIN" --config "$WORK/node.toml" > "$WORK/server.log" 2>&1 &
SERVER_PID=$!
if wait_for_http "$BASE/_cluster/health" 40; then
    pass "HTTPS listener accepted a connection"
else
    fail "HTTPS listener accepted a connection" "$(tail -20 "$WORK/server.log")"
fi

code="$(curl -sk -o /dev/null -w '%{http_code}' -m 10 "$BASE/_cluster/health")"
check_eq "health over TLS" "200" "$code"

if grep -qi 'panic' "$WORK/server.log"; then
    fail "no panic during TLS startup" "$(grep -i -A3 panic "$WORK/server.log" | head -10)"
else
    pass "no panic during TLS startup"
fi

# A certificate error must be fatal at the client, i.e. verification is really happening.
if curl -s -o /dev/null -m 10 "$BASE/_cluster/health" 2>/dev/null; then
    fail "self-signed cert is rejected without --insecure"
else
    pass "self-signed cert is rejected without --insecure"
fi

section "shutdown drains promptly under TLS"
t_start=$(date +%s)
stop_server "$SERVER_PID"; SERVER_PID=""
elapsed=$(( $(date +%s) - t_start ))
# Before the axum_server::Handle fix this always burned the full 10s drain timeout.
if [ "$elapsed" -le 8 ]; then
    pass "TLS server drained in ${elapsed}s"
else
    fail "TLS server drained promptly" "took ${elapsed}s (drain signal not reaching the TLS listener?)"
fi

section "bad TLS material fails fast"
fail_fast_case() {
    local name="$1" cert="$2" key="$3"
    write_config "$cert" "$key"
    if timeout 60 "$BIN" --config "$WORK/node.toml" > "$WORK/bad.log" 2>&1; then
        fail "$name" "server started anyway"
    else
        if grep -qi 'panic' "$WORK/bad.log"; then
            fail "$name" "failed by panicking rather than reporting an error"
        else
            pass "$name"
        fi
    fi
}

fail_fast_case "missing certificate file is an error, not a panic" "$WORK/absent.pem" "$WORK/key.pem"

printf 'not a certificate\n' > "$WORK/garbage.pem"
fail_fast_case "malformed certificate is an error, not a panic" "$WORK/garbage.pem" "$WORK/key.pem"

# The failure must arrive before the node claims to be up.
if grep -q 'Press Ctrl+C to shutdown' "$WORK/bad.log"; then
    fail "TLS failure is reported before the startup banner"
else
    pass "TLS failure is reported before the startup banner"
fi

summary
