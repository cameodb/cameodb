#!/usr/bin/env bash
# Live posture probes: start a node with deliberately tight limits and confirm the
# hardening actually behaves that way over the wire.
#
# Every check here corresponds to a defect that shipped and was not caught by unit tests,
# because each one only fails in a real HTTP stack: a body limit that applies to some
# handlers and not others, a concurrency guard that starves the health endpoint, a
# timeout that was configured but never wired into the router.

set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

BIN="$(require_bin)"
WORK="$(mktemp -d)"
PORT="${POSTURE_PORT:-19490}"
BASE="http://127.0.0.1:$PORT"
SERVER_PID=""

cleanup() {
    stop_server "$SERVER_PID"
    rm -rf "$WORK"
}
trap cleanup EXIT

cat > "$WORK/node.toml" <<EOF
max_record_size_mb = 1

[node]
label = "posture-probe"
profile = "local"

[network.http]
bind_address = "127.0.0.1"
port = $PORT
request_timeout_secs = 5
max_body_size_mb = 8
max_concurrent_requests = 4
cors_allowed_origins = ["https://app.example.com"]
admin_enabled = true

[network.cluster]
enabled = false

[storage]
data_paths = ["$WORK/data"]
num_shards_init = 1
max_shards_per_node = 2
EOF

section "check-config"
if "$BIN" check-config -c "$WORK/node.toml" > "$WORK/check.txt" 2>&1; then
    pass "check-config accepts the probe config"
else
    fail "check-config accepts the probe config" "$(cat "$WORK/check.txt")"
fi

section "startup"
"$BIN" --config "$WORK/node.toml" > "$WORK/server.log" 2>&1 &
SERVER_PID=$!
if wait_for_http "$BASE/_cluster/health" 40; then
    pass "server is serving"
else
    fail "server is serving" "$(tail -20 "$WORK/server.log")"
    summary
    exit 1
fi

status() { curl -s -o /dev/null -w '%{http_code}' -m 30 "$@"; }

section "request body limits"
# One record over the per-record cap, delivered as a single unterminated line. This is the
# shape that bypassed DefaultBodyLimit entirely and drove RSS to 889 MB in review.
head -c $((3 * 1024 * 1024)) /dev/zero | tr '\0' 'x' > "$WORK/big-line.txt"
{ printf '{"id":"a","doc":{"t":"'; cat "$WORK/big-line.txt"; printf '"}}'; } > "$WORK/oneline.ndjson"
check_eq "oversized single record on /document/stream is rejected" "413" \
    "$(status -X POST "$BASE/api/probe/document/stream" -H 'content-type: application/x-ndjson' --data-binary @"$WORK/oneline.ndjson")"

# Many small records adding up past the wire limit: caught by the byte counter, not the
# per-record cap.
: > "$WORK/many.ndjson"
for _ in $(seq 1 120); do
    awk 'BEGIN{ s=""; for(i=0;i<800;i++) s=s "z"; for(j=0;j<100;j++) printf "{\"id\":\"m%d\",\"doc\":{\"t\":\"%s\"}}\n", j, s }' >> "$WORK/many.ndjson"
done
check_eq "oversized total body on /document/stream is rejected" "413" \
    "$(status -X POST "$BASE/api/probe/document/stream" -H 'content-type: application/x-ndjson' --data-binary @"$WORK/many.ndjson")"

check_eq "oversized body on /_bulk is rejected" "413" \
    "$(status -X POST "$BASE/api/probe/_bulk" -H 'content-type: application/json' --data-binary @"$WORK/many.ndjson")"

# Memory must not track the size of a rejected request.
if command -v ps > /dev/null; then
    rss_kb="$(ps -o rss= -p "$SERVER_PID" | tr -d ' ')"
    if [ "${rss_kb:-0}" -lt 500000 ]; then
        pass "RSS stayed bounded after oversized requests ($((rss_kb / 1024)) MB)"
    else
        fail "RSS stayed bounded after oversized requests" "$((rss_kb / 1024)) MB resident"
    fi
fi

section "concurrency guard and timeouts"
# Saturate every permit with trickle uploads, then confirm the two behaviours that matter:
# liveness still answers, and everything else sheds load politely.
: > "$WORK/slow.ndjson"
for i in $(seq 1 200); do printf '{"id":"s%d","doc":{"t":"y"}}\n' "$i" >> "$WORK/slow.ndjson"; done
TRICKLE_PIDS=()
for _ in 1 2 3 4; do
    curl -s -o /dev/null --limit-rate 300 -m 60 -X POST "$BASE/api/probe/document/stream" \
        -H 'content-type: application/x-ndjson' -H 'expect:' --data-binary @"$WORK/slow.ndjson" &
    TRICKLE_PIDS+=($!)
done
sleep 3

check_eq "health stays available while saturated" "200" "$(status "$BASE/_cluster/health")"
check_eq "excess requests shed with 503" "503" \
    "$(status -X POST "$BASE/api/probe/search" -H 'content-type: application/json' -d '{"query":"x"}')"
if curl -s -D - -o /dev/null -m 10 -X POST "$BASE/api/probe/search" \
    -H 'content-type: application/json' -d '{"query":"x"}' | grep -qi '^retry-after:'; then
    pass "503 carries Retry-After"
else
    fail "503 carries Retry-After"
fi

wait "${TRICKLE_PIDS[@]}" 2>/dev/null
sleep 1

# On an idle server the same trickle upload must hit the request timeout, not hang.
t_start=$(date +%s)
code="$(status --limit-rate 200 -m 60 -X POST "$BASE/api/probe/document/stream" \
    -H 'content-type: application/x-ndjson' -H 'expect:' --data-binary @"$WORK/slow.ndjson")"
elapsed=$(( $(date +%s) - t_start ))
check_eq "slow request times out with 408" "408" "$code"
if [ "$elapsed" -le 10 ]; then
    pass "timeout fired near the configured 5s (${elapsed}s)"
else
    fail "timeout fired near the configured 5s" "took ${elapsed}s"
fi

section "CORS"
hdrs="$(curl -s -D - -o /dev/null -X OPTIONS "$BASE/api/probe/search" \
    -H 'Origin: https://app.example.com' -H 'Access-Control-Request-Method: POST' \
    -H 'Access-Control-Request-Headers: mcp-session-id')"
if grep -qi 'access-control-allow-origin: https://app.example.com' <<< "$hdrs"; then
    pass "configured origin is allowed"
else
    fail "configured origin is allowed" "$hdrs"
fi
if grep -qi 'access-control-allow-headers:.*mcp-session-id' <<< "$hdrs"; then
    pass "mcp-session-id is an allowed request header"
else
    fail "mcp-session-id is an allowed request header" "$hdrs"
fi
if curl -s -D - -o /dev/null -H 'Origin: https://app.example.com' "$BASE/_cluster/health" \
    | grep -qi 'access-control-expose-headers:.*mcp-session-id'; then
    pass "mcp-session-id is exposed to browser clients"
else
    fail "mcp-session-id is exposed to browser clients"
fi
if curl -s -D - -o /dev/null -H 'Origin: https://evil.example' "$BASE/_cluster/health" \
    | grep -qi 'access-control-allow-origin'; then
    fail "unlisted origin is refused"
else
    pass "unlisted origin is refused"
fi

section "admin API gating"
check_eq "/_admin/* present when enabled" "200" "$(status "$BASE/_admin/memory")"
stop_server "$SERVER_PID"; SERVER_PID=""
sed -i.bak 's/^admin_enabled = true/admin_enabled = false/' "$WORK/node.toml"
"$BIN" --config "$WORK/node.toml" > "$WORK/server2.log" 2>&1 &
SERVER_PID=$!
if wait_for_http "$BASE/_cluster/health" 40; then
    check_eq "/_admin/* absent when disabled" "404" "$(status "$BASE/_admin/memory")"
    check_eq "normal API still serving" "200" "$(status "$BASE/_cluster/health")"
else
    fail "server restarts with admin disabled" "$(tail -20 "$WORK/server2.log")"
fi

section "graceful shutdown"
t_start=$(date +%s)
stop_server "$SERVER_PID"; SERVER_PID=""
elapsed=$(( $(date +%s) - t_start ))
if [ "$elapsed" -le 12 ]; then
    pass "shutdown completed in ${elapsed}s"
else
    fail "shutdown completed promptly" "took ${elapsed}s"
fi

section "posture rejections"
# The presets have to refuse the combinations they claim to refuse.
reject_case() {
    local name="$1" body="$2"
    printf '%s\n' "$body" > "$WORK/case.toml"
    if "$BIN" check-config -c "$WORK/case.toml" > "$WORK/case.out" 2>&1; then
        fail "$name" "check-config accepted it"
    else
        pass "$name"
    fi
}

reject_case "reachable bind without a declared profile" "$(cat <<EOF
[network.http]
bind_address = "0.0.0.0"
[storage]
data_paths = ["$WORK/data"]
EOF
)"
reject_case "internal profile with wildcard CORS" "$(cat <<EOF
[node]
profile = "internal"
[network.http]
bind_address = "0.0.0.0"
cors_allowed_origins = ["*"]
[storage]
data_paths = ["$WORK/data"]
EOF
)"
reject_case "internal profile clustered without a PSK" "$(cat <<EOF
[node]
profile = "internal"
[network.http]
bind_address = "0.0.0.0"
cors_allowed_origins = []
[network.cluster]
enabled = true
[storage]
data_paths = ["$WORK/data"]
EOF
)"
reject_case "external profile without TLS" "$(cat <<EOF
[node]
profile = "external"
[network.http]
bind_address = "0.0.0.0"
cors_allowed_origins = []
admin_enabled = false
[storage]
data_paths = ["$WORK/data"]
EOF
)"
reject_case "PSK combined with a QUIC address" "$(cat <<EOF
[node]
profile = "internal"
[network.http]
bind_address = "0.0.0.0"
cors_allowed_origins = []
[network.cluster]
enabled = true
psk = "$(printf 'ab%.0s' {1..32})"
seed_nodes = ["/ip4/10.0.0.5/udp/9580/quic-v1"]
[storage]
data_paths = ["$WORK/data"]
EOF
)"

summary
