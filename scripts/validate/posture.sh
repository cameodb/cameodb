#!/usr/bin/env bash
# Live posture probes: start a node with deliberately tight limits and confirm the
# hardening actually behaves that way over the wire.
#
# Most checks here correspond to a defect that shipped and was not caught by unit tests,
# because each one only fails in a real HTTP stack: a body limit that applies to some
# handlers and not others, a concurrency guard that starves the health endpoint, a
# timeout that was configured but never wired into the router.
#
# The HTTP/2 section is the exception, and covers a gap rather than a regression: the
# listener speaks h2c on the same port with nothing to opt into, over separate framing and
# body-streaming code, on the schedule of a transitive dependency. Nothing else in the suite
# has ever sent an h2 frame.

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
    discard_work "$WORK"
}
trap cleanup EXIT

# Before anything binds: a leftover node on this port would answer every probe below.
require_free_port "$PORT"

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
body() { curl -s -m 30 "$@"; }

section "writes that grow the schema"
# The worker pool's engine holds ArcSwap snapshots, so it can read a schema but not evolve
# one — that needs the actor. It signals this by handing the op back, and the caller has to
# retry it there. When that retry was missing, every write to an index with no schema (which
# is every index on first use) came back as a 500. Nothing caught it because the rest of the
# suite creates the schema with PUT _config before it writes.
json='content-type: application/json'
check_eq "a write to an index with no schema is accepted" "200" \
    "$(status -X PUT "$BASE/api/autoschema/document" -H "$json" \
        -d '{"id":"a1","doc":{"id":"a1","title":"first"}}')"

# The actor evolves the schema and publishes it into the same ArcSwap the engine reads, so
# the next write takes the fast path. That it also succeeds is the point: a retry that only
# worked once would mean the schema never reached the engine.
check_eq "the next write to that index is accepted" "200" \
    "$(status -X PUT "$BASE/api/autoschema/document" -H "$json" \
        -d '{"id":"a2","doc":{"id":"a2","title":"second"}}')"

# A document carrying an unknown field needs the schema to grow again, mid-life this time.
check_eq "a write that adds a new field is accepted" "200" \
    "$(status -X PUT "$BASE/api/autoschema/document" -H "$json" \
        -d '{"id":"a3","doc":{"id":"a3","title":"third","author":"ada"}}')"

if body "$BASE/api/autoschema/_config" | grep -q '"author"'; then
    pass "the new field reached the stored schema"
else
    fail "the new field reached the stored schema" "$(body "$BASE/api/autoschema/_config")"
fi

# Accepting the write is not the same as keeping it. Commit, then read one back.
status -X POST "$BASE/_admin/index/autoschema/commit" > /dev/null
if body -X POST "$BASE/api/autoschema/search" -H "$json" -d '{"query":"id:a1"}' \
    | grep -q '"first"'; then
    pass "a document written before the schema existed is retrievable"
else
    fail "a document written before the schema existed is retrievable" \
        "$(body -X POST "$BASE/api/autoschema/search" -H "$json" -d '{"query":"id:a1"}')"
fi

# A tantivy schema is fixed when the index is created, so fields inferred at that moment are
# the only ones that can ever be searchable. Leaving them non-indexed produced an index you
# could write to and then only query by id.
if body -X POST "$BASE/api/autoschema/search" -H "$json" -d '{"query":"title:first"}' \
    | grep -q '"a1"'; then
    pass "a field inferred at index creation is searchable"
else
    fail "a field inferred at index creation is searchable" \
        "$(body -X POST "$BASE/api/autoschema/search" -H "$json" -d '{"query":"title:first"}')"
fi

# The other half of the rule, asserted so it stays deliberate: a field that arrives after the
# index exists cannot be added to tantivy's schema, so it is carried for redb parity only.
#
# `searchable` is the field's own account of that: it reports false, so a caller reading the
# schema learns a query naming this field cannot match without the index being rebuilt, rather
# than inferring it from `indexed`. Both are asserted because they answer different questions —
# whether tantivy holds the field, and whether a query may rely on it.
author_field="$(body "$BASE/api/autoschema/_config" | grep -o '{"name":"author"[^}]*}')"
if [ -n "$author_field" ] \
    && grep -q '"indexed":false' <<< "$author_field" \
    && grep -q '"searchable":false' <<< "$author_field"; then
    pass "a field added after creation is carried unindexed and reports searchable:false"
else
    fail "a field added after creation is carried unindexed and reports searchable:false" \
        "$(body "$BASE/api/autoschema/_config")"
fi

# Type inference samples the first 200 documents. A field that first appears past that point
# is found by validation instead, and it is still part of the same initial creation — so it
# has to be indexed too, or one load would produce two classes of field.
awk 'BEGIN{
    printf "["
    for (i = 0; i < 250; i++) {
        if (i) printf ","
        printf "{\"id\":\"b%d\",\"doc\":{\"id\":\"b%d\",\"title\":\"doc %d\"", i, i, i
        if (i >= 220) printf ",\"late\":\"past the sampling limit\""
        printf "}}"
    }
    printf "]"
}' > "$WORK/bulk.json"
status -X POST "$BASE/api/sampled/_bulk" -H "$json" --data-binary @"$WORK/bulk.json" > /dev/null
status -X POST "$BASE/_admin/index/sampled/commit" > /dev/null
if body -X POST "$BASE/api/sampled/search" -H "$json" -d '{"query":"late:sampling","limit":1}' \
    | grep -q '"total_hits":30'; then
    pass "a field first seen past the sampling limit is still indexed"
else
    fail "a field first seen past the sampling limit is still indexed" \
        "$(body "$BASE/api/sampled/_config")"
fi

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

section "HTTP/2"
# The same port serves h2c, with no config to opt into it, so every limit checked above has a
# second protocol it has to hold on. Framing and body streaming are a separate path inside
# hyper for h2, and until this section existed nothing in the suite ever spoke it: an h2
# bump could have broken negotiation outright and this file would still have reported a
# clean run. The `h2` crate is a transitive dependency that moves on its own schedule, which
# is exactly the kind of change no unit test here would notice.
if ! curl -V | grep -i '^Features:' | grep -qw 'HTTP2'; then
    skip "curl was built without HTTP/2; cannot probe h2c"
else
    # Prior knowledge rather than an Upgrade handshake: there is no TLS on this listener to
    # carry ALPN, and prior knowledge is what a service-mesh or gRPC-style client actually
    # sends to a plaintext backend. The negotiated version is asserted alongside the status
    # because curl falls back to HTTP/1.1 without complaint — a check reading only the status
    # would pass while never once speaking h2.
    h2status() { curl -s -o /dev/null -w '%{http_version} %{http_code}' -m 30 --http2-prior-knowledge "$@"; }

    check_eq "liveness answers over h2c" "2 200" "$(h2status "$BASE/_cluster/health")"
    check_eq "an API route answers over h2c" "2 200" \
        "$(h2status -X POST "$BASE/api/probe/search" -H "$json" -d '{"query":"x"}')"

    # The wire-level body limit is a layer, not a handler, so it should not care which
    # protocol carried the bytes. Asserted rather than assumed, because this is the very
    # limit that already turned out to cover some handlers and not others.
    check_eq "oversized body on /_bulk is rejected over h2c" "2 413" \
        "$(h2status -X POST "$BASE/api/probe/_bulk" -H "$json" --data-binary @"$WORK/many.ndjson")"
    check_eq "oversized body on /document/stream is rejected over h2c" "2 413" \
        "$(h2status -X POST "$BASE/api/probe/document/stream" \
            -H 'content-type: application/x-ndjson' --data-binary @"$WORK/many.ndjson")"
fi

section "concurrency guard and timeouts"
# Saturate every permit with trickle uploads, then confirm the two behaviours that matter:
# liveness still answers, and everything else sheds load politely.
#
# The body is fed slowly down a pipe rather than throttled with curl's `--limit-rate`, because
# `--limit-rate` cannot be combined with a deadline. To hold the average rate curl sleeps for
# as long as the bytes it has already sent require, and it does not wake to check `--max-time`
# while sleeping: at 200 B/s it sailed 16 s past a 60 s deadline in one run out of three here
# and then reported 000. A run that hits that reports "no response" and a minutes-long
# elapsed, which is precisely what a timeout that never fired looks like — the check failing
# in the shape of the defect it exists to detect. Feeding from a pipe leaves curl in poll(),
# so the deadline holds and a real 408 comes back.
trickle_body() {
    local i
    for i in $(seq 1 200); do
        # Every one of these requests ends with the server timing it out and curl exiting, so
        # losing the reader is the normal finish, not a fault: stop feeding instead of
        # reporting a broken pipe 195 times and sleeping out the rest of the loop, which kept
        # the subshell alive for 40 s and made `wait` below block on nothing.
        printf '{"id":"s%d","doc":{"t":"y"}}\n' "$i" 2>/dev/null || return 0
        sleep 0.2
    done
}
# Chunked upload via `-T -`, so the request is genuinely in flight while the body dribbles in.
# `--data-binary @-` would buffer stdin to the end first and send it all at once, which holds
# no permit for any length of time and would make every check below vacuous.
trickle_post() {
    trickle_body | LC_ALL=C curl -s -o /dev/null -w '%{http_code} %{time_total}' -m 30 \
        -X POST "$BASE/api/probe/document/stream" \
        -H 'content-type: application/x-ndjson' -H 'expect:' -T - 2>/dev/null
}

TRICKLE_PIDS=()
for _ in 1 2 3 4; do
    trickle_post > /dev/null &
    TRICKLE_PIDS+=($!)
done
# The permits are only held until the server times these out at 5 s, so the three checks
# below have to land inside that window — hence a short wait here, not a generous one.
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
#
# Timed on curl's own clock rather than `date`: that is the clock `--max-time` is enforced
# against, so the reading and the deadline cannot disagree about what happened.
read -r code elapsed <<< "$(trickle_post)"
# A bare `000` means no response line arrived at all, which is worth naming rather than
# reporting as a status mismatch — it is what a dropped connection looks like, and it reads
# identically to a timeout that never fired.
if [ "$code" = "408" ]; then
    pass "slow request times out with 408 ($code)"
elif [ "$code" = "000" ]; then
    fail "slow request times out with 408" \
        "no response at all; connection dropped after ${elapsed}s on curl's clock"
else
    fail "slow request times out with 408" "expected '408', got '$code'"
fi
# curl reports fractional seconds; the threshold is coarse enough to compare on whole ones.
secs="${elapsed%%.*}"
if [ "${secs:-999}" -le 10 ]; then
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

section "idle-commit timeout is configurable"
# A write is committed either once enough operations accumulate or after the index has been
# idle for `supervisor_timeout_secs` — and until it commits, the document is not searchable.
# A single write never reaches the operation threshold, so this measures the timeout alone.
#
# The setting reached the config struct for a long time and was then ignored: the supervisor
# read CAMEODB_SUPERVISOR_TIMEOUT_SECS from the environment directly, so the config file and
# the --supervisor-timeout-secs flag did nothing. A unit test on the config cannot catch that
# — only starting a node and watching when the document appears can.
sed -i.bak 's/^admin_enabled = false/admin_enabled = true/' "$WORK/node.toml"
printf '\n[search]\nsupervisor_timeout_secs = 1\n' >> "$WORK/node.toml"
"$BIN" --config "$WORK/node.toml" > "$WORK/server3.log" 2>&1 &
SERVER_PID=$!
if wait_for_http "$BASE/_cluster/health" 40; then
    # Query an ordinary indexed field, not `id`. An `id:` lookup is answered without going
    # through a committed tantivy segment, so it is visible immediately and would pass this
    # check no matter what the timeout did — as it did when this was first written.
    status -X PUT "$BASE/api/idle/document" -H "$json" \
        -d '{"id":"i1","doc":{"id":"i1","title":"zebracrossing"}}' > /dev/null

    # The window has to be tight enough to tell the configured 1s from the 5s default, or
    # this passes whether or not the setting is honoured. 3s leaves margin over 1s and
    # refuses 5s.
    found=""
    for _ in $(seq 1 12); do
        if body -X POST "$BASE/api/idle/search" -H "$json" -d '{"query":"title:zebracrossing"}' \
            | grep -q '"i1"'; then
            found=yes
            break
        fi
        sleep 0.25
    done

    if [ -n "$found" ]; then
        pass "a lone write becomes searchable on the configured 1s idle timeout"
    else
        fail "a lone write becomes searchable on the configured 1s idle timeout" \
            "not searchable within 3s — the configured timeout is ignored and the 5s default is in force"
    fi
else
    fail "server restarts with a configured idle timeout" "$(tail -20 "$WORK/server3.log")"
fi
stop_server "$SERVER_PID"; SERVER_PID=""

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
reject_case "authentication enabled with no keys" "$(cat <<EOF
[node]
profile = "local"
[security]
enabled = true
[storage]
data_paths = ["$WORK/data"]
EOF
)"
reject_case "api key with a hash but no role" "$(cat <<EOF
[node]
profile = "local"
[security]
enabled = true
[[security.api_keys]]
key_hash = "sha256:$(printf 'ab%.0s' {1..32})"
[storage]
data_paths = ["$WORK/data"]
EOF
)"
reject_case "api key hash that is not a sha256 digest" "$(cat <<EOF
[node]
profile = "local"
[security]
enabled = true
[[security.api_keys]]
key_hash = "hunter2"
role = "admin"
[storage]
data_paths = ["$WORK/data"]
EOF
)"

section "keygen"
# The one code path that ever sees a key. stdout must be the key and nothing else, so that
# piping it into a secret store cannot capture stray guidance along with it.
KEY="$("$BIN" keygen --role writer --label validate 2>"$WORK/keygen.err")"
if [[ "$KEY" =~ ^cameo_v1_[A-Za-z0-9_-]{43}$ ]]; then
    pass "keygen prints one well-formed key on stdout"
else
    fail "keygen prints one well-formed key on stdout" "$KEY"
fi
if grep -q "$KEY" "$WORK/keygen.err"; then
    fail "the key never appears in the guidance" "keygen echoed the key to stderr as well"
else
    pass "the key never appears in the guidance"
fi

# The stanza keygen prints has to be one check-config accepts — otherwise the tool hands
# operators a config that does not load.
KEYGEN_HASH="$(grep -o 'sha256:[0-9a-f]\{64\}' "$WORK/keygen.err" | head -1)"
cat > "$WORK/keygen.toml" <<EOF
[node]
profile = "local"
[security]
enabled = true
[[security.api_keys]]
key_hash = "$KEYGEN_HASH"
role = "writer"
label = "validate"
[storage]
data_paths = ["$WORK/data"]
EOF
if "$BIN" check-config -c "$WORK/keygen.toml" > "$WORK/keygen.check" 2>&1; then
    pass "the stanza keygen prints is one check-config accepts"
else
    fail "the stanza keygen prints is one check-config accepts" "$(cat "$WORK/keygen.check")"
fi
if grep -q "$KEY" "$WORK/keygen.check"; then
    fail "no key reaches the posture report" "$(cat "$WORK/keygen.check")"
else
    pass "no key reaches the posture report"
fi

summary
