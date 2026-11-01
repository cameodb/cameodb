#!/usr/bin/env bash
# Keys and tenants provisioned through the node, live.
#
# The store, the loader and the gate are unit-tested against files and rings. This suite runs
# what a provisioning service does against a real node: keys minted through the bundled client
# and `/_keys` for users and agents with no tenant, a hand-written users file reloaded by the
# watcher, a company tenant beside individuals, a partner provisioner held to its tenants, and
# all of it surviving a restart — with no minted key in the log or on disk.

set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

BIN="$(require_bin)"
WORK="$(mktemp -d)"
PORT="${PROVISIONING_PORT:-19493}"
BASE="http://127.0.0.1:$PORT"
SERVER_PID=""

cleanup() {
    stop_server "$SERVER_PID"
    discard_work "$WORK"
}
trap cleanup EXIT

require_free_port "$PORT"
mkdir -p "$WORK/keys" "$WORK/keys.d" "$WORK/data"

# The operator's keys: written as files the node reads and cannot change.
"$BIN" keygen --role admin --label ops --hash-out "$WORK/keys/ops" --key-out "$WORK/ops.key" 2>/dev/null
"$BIN" keygen --role provisioner --label portal \
    --hash-out "$WORK/keys/portal" --key-out "$WORK/portal.key" 2>/dev/null
"$BIN" keygen --role provisioner --label partner \
    --hash-out "$WORK/keys/partner" --key-out "$WORK/partner.key" 2>/dev/null
ADMIN_KEY="$(cat "$WORK/ops.key")"
PORTAL_KEY="$(cat "$WORK/portal.key")"
PARTNER_KEY="$(cat "$WORK/partner.key")"

# A users file written by hand, as a deployment without the API would keep one.
HAND_KEY="$("$BIN" keygen --role reader --label hand@company.com 2>"$WORK/hand.stanza")"
HAND_HASH="$(grep -o 'sha256:[0-9a-f]\{64\}' "$WORK/hand.stanza" | head -1)"
cat > "$WORK/keys.d/users.toml" <<EOF
[[security.api_keys]]
key_hash = "$HAND_HASH"
role = "reader"
label = "hand@company.com"
allowed_indexes = ["docs-*"]
EOF

cat > "$WORK/node.toml" <<EOF
include = ["keys.d/*.toml"]

[node]
label = "provisioning-probe"
profile = "local"

[network.http]
bind_address = "127.0.0.1"
port = $PORT
request_timeout_secs = 10

[storage]
data_paths = ["$WORK/data"]
num_shards_init = 1
max_shards_per_node = 2

[security]
enabled = true

[security.managed_files]
dir = "keys.d"
keys = "keys.toml"
tenant = "{tenant}.toml"

[[security.api_keys]]
key_hash_file = "$WORK/keys/ops"
role = "admin"
label = "ops"

[[security.api_keys]]
key_hash_file = "$WORK/keys/portal"
role = "provisioner"
label = "portal"

[[security.api_keys]]
key_hash_file = "$WORK/keys/partner"
role = "provisioner"
label = "partner"
allowed_tenants = ["partner-x-*"]
EOF

start_node() {
    RUST_LOG="${RUST_LOG:-info}" "$BIN" --config "$WORK/node.toml" >> "$WORK/server.log" 2>&1 &
    SERVER_PID=$!
    wait_for_http "$BASE/_cluster/health" 40
}

section "startup"
if "$BIN" check-config -c "$WORK/node.toml" >/dev/null 2>&1; then
    pass "check-config accepts include and managed_files"
else
    fail "check-config accepts include and managed_files" "$("$BIN" check-config -c "$WORK/node.toml" 2>&1 | tail -5)"
fi
if start_node; then
    pass "server is serving"
else
    fail "server is serving" "$(tail -20 "$WORK/server.log")"
    summary; exit 1
fi

json='content-type: application/json'
# code <key> <curl args…> — the status code of a request made with <key>.
code() {
    local key="$1"; shift
    curl -s -o /dev/null -w '%{http_code}' -m 20 -H "Authorization: Bearer $key" "$@"
}
# body <key> <curl args…> — the body.
body() {
    local key="$1"; shift
    curl -s -m 20 -H "Authorization: Bearer $key" "$@"
}
# client <key file> <args…> — the bundled client, with nothing inherited from the shell.
client() {
    local key_file="$1"; shift
    env -u CAMEODB_API_KEY -u CAMEODB_API_KEY_FILE NO_COLOR=1 \
        "$BIN" client -c "$BASE" --api-key-file "$key_file" "$@"
}
write_doc() { code "$1" -X PUT "$BASE/api/$2/document" -H "$json" -d "{\"id\":\"$3\",\"doc\":{\"t\":\"$4\"}}"; }
search() { code "$1" -X POST "$BASE/api/$2/search" -H "$json" -d '{"query":"*"}'; }
field() { python3 -I -c 'import json,sys; print(json.load(sys.stdin)[sys.argv[1]])' "$1"; }

section "keys without tenants, minted through the client"
client "$WORK/portal.key" keys create --role writer --label jane@company.com \
    --indexes 'docs-*' > "$WORK/jane.key" 2> "$WORK/jane.about"
JANE_KEY="$(cat "$WORK/jane.key")"
case "$JANE_KEY" in
    cameo_v1_*) pass "keys create puts the key alone on stdout" ;;
    *) fail "keys create puts the key alone on stdout" "$(cat "$WORK/jane.about")" ;;
esac
JANE_ID="$(grep -o '"key_id": "[0-9a-f]*"' "$WORK/jane.about" | grep -o '[0-9a-f]\{8\}')"
client "$WORK/portal.key" keys create --role reader --label agent:search \
    --indexes 'docs-public' --key-out "$WORK/agent.key" 2>/dev/null
AGENT_KEY="$(cat "$WORK/agent.key" 2>/dev/null)"
check_eq "--key-out writes the key owner-only" "600" "$(stat -f '%Lp' "$WORK/agent.key" 2>/dev/null || stat -c '%a' "$WORK/agent.key")"

check_eq "jane writes under docs-*"      "200" "$(write_doc "$JANE_KEY" docs-public 1 hello)"
check_eq "jane is refused outside it"    "403" "$(write_doc "$JANE_KEY" wiki 1 x)"
check_eq "the agent reads its index"     "200" "$(search "$AGENT_KEY" docs-public)"
check_eq "the agent writes nothing"      "403" "$(write_doc "$AGENT_KEY" docs-public 2 y)"
check_eq "the agent reads nothing else"  "403" "$(search "$AGENT_KEY" docs-private)"
check_eq "/_keys lists both, revision 2" "2" "$(body "$PORTAL_KEY" "$BASE/_keys" | field revision)"
check_eq "a stale If-Match is refused"   "412" "$(code "$PORTAL_KEY" -X POST "$BASE/_keys" -H "$json" -H 'If-Match: 1' -d '{"role":"reader"}')"
check_eq "an admin key is not minted"    "403" "$(code "$PORTAL_KEY" -X POST "$BASE/_keys" -H "$json" -d '{"role":"admin"}')"
check_eq "a bad pattern is refused"      "400" "$(code "$PORTAL_KEY" -X POST "$BASE/_keys" -H "$json" -d '{"role":"reader","allowed_indexes":["*"]}')"
check_eq "the provisioner reads no data" "403" "$(search "$PORTAL_KEY" docs-public)"
check_eq "a data key manages no keys"    "403" "$(code "$JANE_KEY" "$BASE/_keys")"

section "a hand-written users file, and the watcher"
check_eq "the hand-written key reads docs-*" "200" "$(search "$HAND_KEY" docs-public)"
check_eq "and is not the API's to list" "False" \
    "$(body "$PORTAL_KEY" "$BASE/_keys" | python3 -I -c 'import json,sys; print(any(k["label"]=="hand@company.com" for k in json.load(sys.stdin)["keys"]))')"
LATE_KEY="$("$BIN" keygen --role reader --label late 2>"$WORK/late.stanza")"
LATE_HASH="$(grep -o 'sha256:[0-9a-f]\{64\}' "$WORK/late.stanza" | head -1)"
printf '[[security.api_keys]]\nkey_hash = "%s"\nrole = "reader"\nallowed_indexes = ["wiki"]\n' \
    "$LATE_HASH" > "$WORK/keys.d/.late.toml.tmp"
mv "$WORK/keys.d/.late.toml.tmp" "$WORK/keys.d/late.toml"
late=000
for _ in 1 2 3 4 5 6 7 8 9 10; do
    late="$(search "$LATE_KEY" wiki)"
    [ "$late" = 200 ] || [ "$late" = 404 ] && break
    sleep 0.5
done
case "$late" in
    200|404) pass "a file dropped into keys.d is loaded without a reload ($late)" ;;
    *) fail "a file dropped into keys.d is loaded without a reload" "status $late" ;;
esac

section "a company tenant beside individuals"
check_eq "tenant acme is created" "200" \
    "$(code "$PORTAL_KEY" -X PUT "$BASE/_tenants/acme" -H "$json" -d '{"max_indexes":3,"allowed_indexes":["acme-docs","acme-shared"]}')"
ANA_KEY="$(body "$PORTAL_KEY" -X POST "$BASE/_tenants/acme/keys" -H "$json" -d '{"role":"writer","label":"ana@acme.com"}' | field key)"
EVE_KEY="$(body "$PORTAL_KEY" -X POST "$BASE/_keys" -H "$json" -d '{"role":"reader","label":"eve@mail.com","allowed_indexes":["eve-notes","acme-shared"]}' | field key)"
check_eq "ana writes acme-docs"           "200" "$(write_doc "$ANA_KEY" acme-docs 1 plan)"
check_eq "ana writes acme-shared"         "200" "$(write_doc "$ANA_KEY" acme-shared 1 note)"
check_eq "ana is refused on docs-public"  "403" "$(write_doc "$ANA_KEY" docs-public 9 x)"
check_eq "eve reads the shared index"     "200" "$(search "$EVE_KEY" acme-shared)"
check_eq "eve is refused on acme-docs"    "403" "$(search "$EVE_KEY" acme-docs)"
check_eq "jane is refused on acme-docs"   "403" "$(search "$JANE_KEY" acme-docs)"
check_eq "the listing names the owner" "acme" \
    "$(body "$ADMIN_KEY" "$BASE/_indexes" | python3 -I -c 'import json,sys; print([i.get("tenant") for i in json.load(sys.stdin)["indexes"] if i["name"]=="acme-docs"][0])')"
check_eq "a tenant owning indexes is not deleted" "409" "$(code "$PORTAL_KEY" -X DELETE "$BASE/_tenants/acme")"
check_eq "a tenant cannot take the keys file's name" "409" \
    "$(code "$PORTAL_KEY" -X PUT "$BASE/_tenants/keys" -H "$json" -d '{}')"

section "a partner provisioner is held to its tenants"
check_eq "partner creates partner-x-co"       "200" "$(code "$PARTNER_KEY" -X PUT "$BASE/_tenants/partner-x-co" -H "$json" -d '{}')"
check_eq "partner is refused on acme"         "403" "$(code "$PARTNER_KEY" "$BASE/_tenants/acme")"
check_eq "partner scopes only to its names"   "403" "$(code "$PARTNER_KEY" -X PUT "$BASE/_tenants/partner-x-co" -H "$json" -d '{"allowed_indexes":["acme-*"]}')"
check_eq "partner mints no tenant-less keys"  "403" "$(code "$PARTNER_KEY" -X POST "$BASE/_keys" -H "$json" -d '{"role":"reader"}')"
check_eq "partner lists only its own" "['partner-x-co']" \
    "$(body "$PARTNER_KEY" "$BASE/_tenants" | python3 -I -c 'import json,sys; print([t["tenant"] for t in json.load(sys.stdin)["tenants"]])')"

section "revocation"
client "$WORK/portal.key" keys revoke "$JANE_ID" >/dev/null 2>&1
check_eq "a revoked key is refused at once" "401" "$(search "$JANE_KEY" docs-public)"
check_eq "the other keys keep working"      "200" "$(search "$EVE_KEY" acme-shared)"

section "restart"
stop_server "$SERVER_PID"
if start_node; then pass "the node restarts on the files it wrote"; else fail "the node restarts on the files it wrote" "$(tail -20 "$WORK/server.log")"; fi
check_eq "ana's tenant key survives"    "200" "$(search "$ANA_KEY" acme-docs)"
check_eq "eve's key survives"           "200" "$(search "$EVE_KEY" acme-shared)"
check_eq "the hand-written key survives" "200" "$(search "$HAND_KEY" docs-public)"
check_eq "jane stays revoked"           "401" "$(search "$JANE_KEY" docs-public)"

section "no minted key is written down"
leaked=""
for key in "$JANE_KEY" "$AGENT_KEY" "$ANA_KEY" "$EVE_KEY"; do
    if grep -rqF "$key" "$WORK/server.log" "$WORK/keys.d" "$WORK/node.toml"; then leaked="yes"; fi
done
if [ -z "$leaked" ]; then
    pass "no minted key in the log, the managed files or the config"
else
    fail "no minted key in the log, the managed files or the config"
fi

summary
