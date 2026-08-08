#!/usr/bin/env bash
# Live authentication and authorization probes.
#
# The unit tests in `crates/server/src/authz.rs` decide every case against the route table
# directly. This suite proves the other half: that the decision is actually *in the request
# path*, in the right place in the layer stack, with the right status codes and headers on
# the wire. A middleware that is correct but mounted in the wrong order — or not mounted —
# passes every unit test there is.

set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

BIN="$(require_bin)"
WORK="$(mktemp -d)"
PORT="${AUTH_PORT:-19492}"
BASE="http://127.0.0.1:$PORT"
SERVER_PID=""

cleanup() {
    stop_server "$SERVER_PID"
    rm -rf "$WORK"
}
trap cleanup EXIT

# mint <role> <label> [indexes] — sets $KEY and $HASH. Called directly rather than through a
# command substitution, because a subshell would swallow the second of the two.
KEY=""
HASH=""
mint() {
    local role="$1" label="$2" indexes="${3:-}"
    local args=(keygen --role "$role" --label "$label")
    [ -n "$indexes" ] && args+=(--allowed-indexes "$indexes")
    KEY="$("$BIN" "${args[@]}" 2>"$WORK/$label.stanza")"
    HASH="$(grep -o 'sha256:[0-9a-f]\{64\}' "$WORK/$label.stanza" | head -1)"
}

section "keys"
mint admin  ops;          ADMIN_KEY="$KEY";  ADMIN_HASH="$HASH"
mint writer ingest;       WRITER_KEY="$KEY"; WRITER_HASH="$HASH"
mint reader agent;        READER_KEY="$KEY"; READER_HASH="$HASH"
mint writer tenant docs;  SCOPED_KEY="$KEY"; SCOPED_HASH="$HASH"
if [ -n "$ADMIN_HASH" ] && [ -n "$SCOPED_HASH" ] && [ "$ADMIN_KEY" != "$WRITER_KEY" ]; then
    pass "minted four distinct keys with their hashes"
else
    fail "minted four distinct keys with their hashes"
    summary; exit 1
fi

cat > "$WORK/node.toml" <<EOF
[node]
label = "auth-probe"
profile = "local"

[network.http]
bind_address = "127.0.0.1"
port = $PORT
max_concurrent_requests = 4
request_timeout_secs = 10
cors_allowed_origins = ["https://app.example.com"]
admin_enabled = true

[security]
enabled = true

[[security.api_keys]]
key_hash = "$ADMIN_HASH"
role = "admin"
label = "ops"

[[security.api_keys]]
key_hash = "$WRITER_HASH"
role = "writer"
label = "ingest"

[[security.api_keys]]
key_hash = "$READER_HASH"
role = "reader"
label = "agent"

[[security.api_keys]]
key_hash = "$SCOPED_HASH"
role = "writer"
label = "tenant"
allowed_indexes = ["docs"]

[storage]
data_paths = ["$WORK/data"]
num_shards_init = 1
max_shards_per_node = 2
EOF

section "startup"
# `info` explicitly: the key identity lines this suite checks for are logged at that level,
# and the default filter is quieter. Asking for the level being asserted is part of the test.
RUST_LOG="${RUST_LOG:-info}" "$BIN" --config "$WORK/node.toml" > "$WORK/server.log" 2>&1 &
SERVER_PID=$!
if wait_for_http "$BASE/_cluster/health" 40; then
    pass "server is serving with authentication enabled"
else
    fail "server is serving with authentication enabled" "$(tail -20 "$WORK/server.log")"
    summary
    exit 1
fi

# code [key] <curl args…> — status code, with an optional bearer key as the first argument.
code() {
    local key="$1"; shift
    if [ -n "$key" ]; then
        curl -s -o /dev/null -w '%{http_code}' -m 20 -H "Authorization: Bearer $key" "$@"
    else
        curl -s -o /dev/null -w '%{http_code}' -m 20 "$@"
    fi
}

json='content-type: application/json'
DOC='{"id":"probe-1","doc":{"id":"probe-1","title":"probe"}}'
QUERY='{"query":"probe"}'

section "every route refuses an anonymous caller"
check_eq "POST /api/{index}/search"          "401" "$(code '' -X POST "$BASE/api/docs/search" -H "$json" -d "$QUERY")"
check_eq "POST /api/{index}/search/stream"   "401" "$(code '' -X POST "$BASE/api/docs/search/stream" -H "$json" -d "$QUERY")"
check_eq "GET  /api/{index}/_config"         "401" "$(code '' "$BASE/api/docs/_config")"
check_eq "GET  /_indexes"                    "401" "$(code '' "$BASE/_indexes")"
check_eq "GET  /_cluster/_indexes"           "401" "$(code '' "$BASE/_cluster/_indexes")"
check_eq "PUT  /api/{index}/document"        "401" "$(code '' -X PUT "$BASE/api/docs/document" -H "$json" -d "$DOC")"
check_eq "POST /api/{index}/document/stream" "401" "$(code '' -X POST "$BASE/api/docs/document/stream" -H 'content-type: application/x-ndjson' -d "$DOC")"
check_eq "POST /api/{index}/_bulk"           "401" "$(code '' -X POST "$BASE/api/docs/_bulk" -H "$json" -d "[$DOC]")"
check_eq "PUT  /api/{index}/_config"         "401" "$(code '' -X PUT "$BASE/api/docs/_config" -H "$json" -d '{}')"
check_eq "PATCH /api/{index}/_schema"        "401" "$(code '' -X PATCH "$BASE/api/docs/_schema" -H "$json" -d '{}')"
check_eq "DELETE /api/{index}"               "401" "$(code '' -X DELETE "$BASE/api/docs")"
check_eq "GET  /_admin/memory"               "401" "$(code '' "$BASE/_admin/memory")"
check_eq "POST /_admin/memory/purge"         "401" "$(code '' -X POST "$BASE/_admin/memory/purge")"
check_eq "GET  /_admin/workers"              "401" "$(code '' "$BASE/_admin/workers")"
check_eq "POST /_admin/index/{index}/commit" "401" "$(code '' -X POST "$BASE/_admin/index/docs/commit")"
check_eq "POST /_admin/…/evict-writer"       "401" "$(code '' -X POST "$BASE/_admin/index/docs/evict-writer")"
check_eq "POST /mcp"                         "401" "$(code '' -X POST "$BASE/mcp" -H "$json" -d '{}')"
check_eq "GET  /mcp (SSE stream)"            "401" "$(code '' "$BASE/mcp")"
check_eq "DELETE /mcp (end session)"         "401" "$(code '' -X DELETE "$BASE/mcp")"

# A 401 has to say what to send. Without this a client is left guessing the scheme.
if curl -s -D - -o /dev/null -m 10 "$BASE/_indexes" | grep -qi '^www-authenticate: *Bearer'; then
    pass "401 carries WWW-Authenticate: Bearer"
else
    fail "401 carries WWW-Authenticate: Bearer"
fi

section "a key is presented in the Authorization header and nowhere else"
check_eq "query parameter is not a credential" "401" \
    "$(code '' "$BASE/_indexes?api_key=$READER_KEY")"
check_eq "a non-Bearer scheme is refused" "401" \
    "$(curl -s -o /dev/null -w '%{http_code}' -m 10 -H "Authorization: Basic $READER_KEY" "$BASE/_indexes")"
check_eq "an unrecognised key is 401, not 403" "401" \
    "$(code 'cameo_v1_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA' "$BASE/_indexes")"
check_eq "a key-shaped string is not enough" "401" \
    "$(code 'hunter2' "$BASE/_indexes")"

section "roles reach exactly their capabilities"
# Create the index first, with the only key that may: a write to an index that does not
# exist fails for reasons that have nothing to do with authorization, and a suite that
# cannot tell those two apart is not proving anything.
check_eq "admin may create an index" "200" \
    "$(code "$ADMIN_KEY" -X PUT "$BASE/api/docs/_config" -H "$json" \
        -d '{"name":"docs","fields":{"title":{"field_type":"text","indexed":true,"stored":true}}}')"

check_eq "reader may search"              "200" "$(code "$READER_KEY" -X POST "$BASE/api/docs/search" -H "$json" -d "$QUERY")"
check_eq "reader may list indexes"        "200" "$(code "$READER_KEY" "$BASE/_indexes")"
check_eq "reader may not write"           "403" "$(code "$READER_KEY" -X PUT "$BASE/api/docs/document" -H "$json" -d "$DOC")"
check_eq "reader may not delete an index" "403" "$(code "$READER_KEY" -X DELETE "$BASE/api/docs")"
check_eq "reader may not reach /_admin"   "403" "$(code "$READER_KEY" "$BASE/_admin/memory")"

check_eq "writer may write"               "200" "$(code "$WRITER_KEY" -X PUT "$BASE/api/docs/document" -H "$json" -d "$DOC")"
check_eq "writer may not evolve a schema" "403" "$(code "$WRITER_KEY" -X PATCH "$BASE/api/docs/_schema" -H "$json" -d '{"field_updates":{}}')"
check_eq "writer may not purge memory"    "403" "$(code "$WRITER_KEY" -X POST "$BASE/_admin/memory/purge")"

check_eq "admin may reach /_admin"        "200" "$(code "$ADMIN_KEY" "$BASE/_admin/memory")"
check_eq "admin may evict a writer"       "200" "$(code "$ADMIN_KEY" -X POST "$BASE/_admin/index/docs/evict-writer")"

section "index scope"
check_eq "a scoped key works on its index"        "200" "$(code "$SCOPED_KEY" -X POST "$BASE/api/docs/search" -H "$json" -d "$QUERY")"
check_eq "a scoped key is refused elsewhere"      "403" "$(code "$SCOPED_KEY" -X POST "$BASE/api/payroll/search" -H "$json" -d "$QUERY")"
check_eq "the scope holds on writes too"          "403" "$(code "$SCOPED_KEY" -X PUT "$BASE/api/payroll/document" -H "$json" -d "$DOC")"
check_eq "and on the admin routes that name one"  "403" "$(code "$SCOPED_KEY" -X POST "$BASE/_admin/index/payroll/commit")"
# Percent-encoding must not be a way around the comparison. Fail-closed is the answer: the
# check runs on the raw segment, so an encoded name is refused rather than let through.
check_eq "an encoded index name does not slip past" "403" \
    "$(code "$SCOPED_KEY" -X POST "$BASE/api/%64ocs/search" -H "$json" -d "$QUERY")"

section "MCP"
MCP_INIT='{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"probe","version":"1"}}}'
mcp_accept='accept: application/json, text/event-stream'
check_eq "a reader key reaches MCP" "200" \
    "$(code "$READER_KEY" -X POST "$BASE/mcp" -H "$json" -H "$mcp_accept" -d "$MCP_INIT")"
# Until per-tool scoping lands (B1 step 4) an index-scoped key would escape its scope
# through MCP, so it is refused at the door instead.
check_eq "an index-scoped key is refused at MCP" "403" \
    "$(code "$SCOPED_KEY" -X POST "$BASE/mcp" -H "$json" -H "$mcp_accept" -d "$MCP_INIT")"

section "health"
check_eq "health answers without a key" "200" "$(code '' "$BASE/_cluster/health")"
anon_body="$(curl -s -m 10 "$BASE/_cluster/health")"
auth_body="$(curl -s -m 10 -H "Authorization: Bearer $READER_KEY" "$BASE/_cluster/health")"
# An anonymous caller gets liveness. Node identity, cluster size and index counts are a
# free reconnaissance report, and a load balancer needs none of them.
if grep -q 'node_id' <<< "$anon_body"; then
    fail "anonymous health withholds node and cluster detail" "$anon_body"
else
    pass "anonymous health withholds node and cluster detail"
fi
if grep -q 'status' <<< "$anon_body"; then
    pass "anonymous health still reports liveness"
else
    fail "anonymous health still reports liveness" "$anon_body"
fi
if grep -q 'node_id' <<< "$auth_body" && grep -q 'active_shards' <<< "$auth_body"; then
    pass "an identified caller gets the full body"
else
    fail "an identified caller gets the full body" "$auth_body"
fi

section "the bundled client presents a key"
# `cameodb client` is how most people will meet an authenticated node. If it cannot carry a
# key, enabling authentication means locking yourself out of your own tooling.
CLIENT=("$BIN" client -c "$BASE")

printf '%s' "$READER_KEY" > "$WORK/reader.key"
chmod 600 "$WORK/reader.key"

if "${CLIENT[@]}" health > "$WORK/c.out" 2>&1; then
    pass "client health works without a key (health is public)"
else
    fail "client health works without a key (health is public)" "$(cat "$WORK/c.out")"
fi
# The anonymous body is now status-only. A client that still expected node_id would fail to
# parse a 200 — which is the interesting failure, not the 401.
if grep -q 'node_id' "$WORK/c.out"; then
    fail "client renders the shrunk anonymous health body" "$(cat "$WORK/c.out")"
else
    pass "client renders the shrunk anonymous health body"
fi

if "${CLIENT[@]}" list indexes > "$WORK/c.out" 2>&1; then
    fail "client without a key is refused" "$(cat "$WORK/c.out")"
else
    pass "client without a key is refused"
fi
# Being refused is not enough: the message has to name the flag that fixes it.
if grep -q -- '--api-key-file' "$WORK/c.out"; then
    pass "the refusal names the flag that fixes it"
else
    fail "the refusal names the flag that fixes it" "$(cat "$WORK/c.out")"
fi

if "${CLIENT[@]}" --api-key "$READER_KEY" list indexes > "$WORK/c.out" 2>&1; then
    pass "--api-key authenticates"
else
    fail "--api-key authenticates" "$(cat "$WORK/c.out")"
fi

if "${CLIENT[@]}" --api-key-file "$WORK/reader.key" list indexes > "$WORK/c.out" 2>&1; then
    pass "--api-key-file authenticates"
else
    fail "--api-key-file authenticates" "$(cat "$WORK/c.out")"
fi

if CAMEODB_API_KEY="$READER_KEY" "${CLIENT[@]}" list indexes > "$WORK/c.out" 2>&1; then
    pass "CAMEODB_API_KEY authenticates"
else
    fail "CAMEODB_API_KEY authenticates" "$(cat "$WORK/c.out")"
fi

# A key file named on the command line beats one left over in the environment, so a stale
# export cannot silently redirect which identity a command runs as.
UNKNOWN_KEY="cameo_v1_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
if CAMEODB_API_KEY="$UNKNOWN_KEY" "${CLIENT[@]}" --api-key-file "$WORK/reader.key" \
    list indexes > "$WORK/c.out" 2>&1; then
    pass "an explicit key file wins over an exported key"
else
    fail "an explicit key file wins over an exported key" "$(cat "$WORK/c.out")"
fi

if "${CLIENT[@]}" --api-key "$READER_KEY" admin memory stats > "$WORK/c.out" 2>&1; then
    fail "a reader key is refused on an admin command" "$(cat "$WORK/c.out")"
else
    pass "a reader key is refused on an admin command"
fi
if grep -q 'role or allowed_indexes' "$WORK/c.out"; then
    pass "a 403 is explained as a scope problem, not a credentials problem"
else
    fail "a 403 is explained as a scope problem, not a credentials problem" "$(cat "$WORK/c.out")"
fi

if "${CLIENT[@]}" --api-key "$ADMIN_KEY" admin memory stats > "$WORK/c.out" 2>&1; then
    pass "an admin key reaches an admin command"
else
    fail "an admin key reaches an admin command" "$(cat "$WORK/c.out")"
fi
# The SDK asked for /evict_writer while the route is /evict-writer, so this path answered
# 404 for as long as it has existed. With authentication in front of the router it would
# have started answering 401 instead — an unrelated bug wearing an auth costume.
if "${CLIENT[@]}" --api-key "$ADMIN_KEY" admin index docs evict-writer > "$WORK/c.out" 2>&1; then
    pass "admin index evict-writer reaches its route"
else
    fail "admin index evict-writer reaches its route" "$(cat "$WORK/c.out")"
fi

# 192.0.2.0/24 is TEST-NET-1 and routes nowhere. The refusal has to happen before any
# connection is attempted, which is also why this check does not hang.
if "$BIN" client -c "http://192.0.2.1:9480" --api-key "$READER_KEY" list indexes \
    > "$WORK/c.out" 2>&1; then
    fail "a key is not sent to another host over plaintext" "$(cat "$WORK/c.out")"
else
    pass "a key is not sent to another host over plaintext"
fi
if grep -q -- '--allow-plaintext-key' "$WORK/c.out"; then
    pass "the plaintext refusal names its override"
else
    fail "the plaintext refusal names its override" "$(cat "$WORK/c.out")"
fi
# Loopback is exempt: the token never leaves the machine, which is what keeps the
# single-node default usable without a flag.
if "${CLIENT[@]}" --api-key "$READER_KEY" list indexes > /dev/null 2>&1; then
    pass "loopback needs no override"
else
    fail "loopback needs no override"
fi

if "${CLIENT[@]}" --api-key "hunter2" list indexes > "$WORK/c.out" 2>&1; then
    fail "a malformed key is refused before it is sent" "$(cat "$WORK/c.out")"
else
    pass "a malformed key is refused before it is sent"
fi
if grep -q 'cameo_v1_' "$WORK/c.out"; then
    pass "the malformed-key message says what a key looks like"
else
    fail "the malformed-key message says what a key looks like" "$(cat "$WORK/c.out")"
fi

cp "$WORK/reader.key" "$WORK/loose.key"
chmod 644 "$WORK/loose.key"
"${CLIENT[@]}" --api-key-file "$WORK/loose.key" list indexes > "$WORK/c.out" 2>&1
if grep -q 'readable by other users' "$WORK/c.out"; then
    pass "a world-readable key file is called out"
else
    fail "a world-readable key file is called out" "$(cat "$WORK/c.out")"
fi

# Whatever else it prints, it must never print the key itself.
if grep -q "$READER_KEY" "$WORK/c.out"; then
    fail "the client never echoes a key"
else
    pass "the client never echoes a key"
fi

section "unknown paths"
check_eq "anonymous probing learns nothing" "401" "$(code '' "$BASE/api/secret/_internal")"
check_eq "an authenticated caller gets an honest 404" "404" \
    "$(code "$READER_KEY" "$BASE/api/secret/_internal")"

section "layer order"
# Preflight never carries Authorization, so auth must sit inside CORS or every browser
# client breaks on the first request it makes.
hdrs="$(curl -s -D - -o /dev/null -m 10 -X OPTIONS "$BASE/api/docs/search" \
    -H 'Origin: https://app.example.com' -H 'Access-Control-Request-Method: POST')"
if grep -qi 'access-control-allow-origin: https://app.example.com' <<< "$hdrs"; then
    pass "CORS preflight succeeds without a key"
else
    fail "CORS preflight succeeds without a key" "$hdrs"
fi

# The other half of the placement: auth sits *outside* the concurrency guard and both body
# limits, so a refused request never takes a permit and never has its body buffered.
#
# Proving that needs requests that *would* hold a permit if they got past auth, hence the
# trickle uploads: there are 4 permits and 8 anonymous uploads, so if the 401 were decided
# inside the guard, every permit would be held for the length of an upload and the
# authenticated request below would be shed with 503.
: > "$WORK/slow.ndjson"
for i in $(seq 1 200); do printf '{"id":"f%d","doc":{"id":"f%d","title":"y"}}\n' "$i" "$i" >> "$WORK/slow.ndjson"; done
FLOOD_PIDS=()
for _ in $(seq 1 8); do
    curl -s -o /dev/null --limit-rate 300 -m 30 -X POST "$BASE/api/docs/document/stream" \
        -H 'content-type: application/x-ndjson' -H 'expect:' --data-binary @"$WORK/slow.ndjson" &
    FLOOD_PIDS+=($!)
done
sleep 2
flood_code="$(code "$READER_KEY" -X POST "$BASE/api/docs/search" -H "$json" -d "$QUERY")"
# Wait on the flood alone. A bare `wait` would also wait on the server, which does not exit.
wait "${FLOOD_PIDS[@]}" 2>/dev/null
check_eq "an unauthenticated flood does not shed authenticated requests" "200" "$flood_code"

section "keys never reach the logs"
leaked=0
for key in "$ADMIN_KEY" "$WRITER_KEY" "$READER_KEY" "$SCOPED_KEY"; do
    grep -q "$key" "$WORK/server.log" && leaked=1
done
if [ "$leaked" -eq 0 ]; then
    pass "no key appears in the server log"
else
    fail "no key appears in the server log"
fi
# The `key_id` is what an audit line is supposed to carry instead.
if grep -q 'key_id' "$WORK/server.log"; then
    pass "the log identifies keys by key_id"
else
    fail "the log identifies keys by key_id" "$(tail -5 "$WORK/server.log")"
fi

section "check-config and the external profile"
external() {
    local name="$1" body="$2" expect="$3"
    printf '%s\n' "$body" > "$WORK/ext.toml"
    if "$BIN" check-config -c "$WORK/ext.toml" > "$WORK/ext.out" 2>&1; then
        check_eq "$name" "$expect" "accept"
    else
        check_eq "$name" "$expect" "reject"
    fi
}

# Certificate material, so the TLS rule is satisfied by something real.
openssl req -x509 -newkey rsa:2048 -nodes -keyout "$WORK/key.pem" -out "$WORK/cert.pem" \
    -days 1 -subj "/CN=localhost" > /dev/null 2>&1

external "external with auth off is refused" "$(cat <<EOF
[node]
profile = "external"
[network.http]
bind_address = "0.0.0.0"
cors_allowed_origins = []
admin_enabled = false
[network.http.tls]
enabled = true
cert_file = "$WORK/cert.pem"
key_file = "$WORK/key.pem"
[storage]
data_paths = ["$WORK/data"]
EOF
)" "reject"

external "external with auth, keys and TLS is accepted" "$(cat <<EOF
[node]
profile = "external"
[network.http]
bind_address = "0.0.0.0"
cors_allowed_origins = []
admin_enabled = false
[network.http.tls]
enabled = true
cert_file = "$WORK/cert.pem"
key_file = "$WORK/key.pem"
[security]
enabled = true
[[security.api_keys]]
key_hash = "$ADMIN_HASH"
role = "admin"
label = "ops"
[storage]
data_paths = ["$WORK/data"]
EOF
)" "accept"

summary
