#!/usr/bin/env bash
# Keys and tenants provisioned through the node, on a real DEB install under systemd.
#
# provisioning.sh runs a bare binary as the invoking user. This suite installs the package into
# a Linux container with systemd as PID 1 and checks what only an installed node has: the
# folders the postinst creates and who owns them, the unit's ProtectSystem sandbox letting the
# node write `keys.d` (and nothing else under /etc), the watcher picking up a dropped file, the
# service restarting and the package reinstalling on top of keys it wrote.
#
# Needs Docker and a .deb for the container's architecture:
#   ./scripts/build/build-packages.sh arm64      # native on Apple Silicon, like the portal
#   ./scripts/validate/provisioning-linux.sh
#
# Not part of the default `all.sh` run — it needs Docker and a privileged container.
#
# Environment:
#   PACKAGE   the .deb to install (default: the newest target/*-linux-musl/release/cameodb_*.deb for this arch)
#   PLATFORM  container platform (default: the Docker host's, so arm64 on Apple Silicon)

set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"
ROOT="$(repo_root)"

for tool in docker curl python3; do
    command -v "$tool" > /dev/null 2>&1 || { skip "$tool not available; this suite needs docker, curl and python3"; summary; exit 0; }
done

# run_bounded <seconds> <command...> — see cluster.sh: a wedged Docker VM must not hang the run.
run_bounded() {
    local secs="$1"; shift
    "$@" &
    local pid=$!
    # Its output goes nowhere: a watchdog holding this function's stdout open would keep every
    # `$(...)` capture of it waiting for the full timeout.
    ( sleep "$secs"; kill "$pid" 2> /dev/null ) > /dev/null 2>&1 &
    local watchdog=$!
    wait "$pid"
    local rc=$?
    kill "$watchdog" 2> /dev/null
    wait "$watchdog" 2> /dev/null
    return "$rc"
}

ARCH="$(docker version --format '{{.Server.Arch}}' 2> /dev/null)"
case "$ARCH" in
    arm64|aarch64) DEB_ARCH=arm64 ;;
    amd64|x86_64) DEB_ARCH=amd64 ;;
    *) skip "Docker reports architecture '$ARCH'"; summary; exit 0 ;;
esac
PLATFORM="${PLATFORM:-linux/$DEB_ARCH}"

section "package"
PACKAGE="${PACKAGE:-}"
if [ -z "$PACKAGE" ]; then
    # shellcheck disable=SC2012
    PACKAGE="$(ls -t "$ROOT"/target/*-unknown-linux-musl/release/cameodb_*"_$DEB_ARCH.deb" 2> /dev/null | head -1)"
fi
if [ -z "$PACKAGE" ] || [ ! -f "$PACKAGE" ]; then
    fail "a .deb for $DEB_ARCH exists" "none under target/*-unknown-linux-musl/release; run ./scripts/build/build-packages.sh $DEB_ARCH"
    summary; exit 1
fi
# A package older than the code under test would pass against yesterday's behaviour.
newest_src="$(git -C "$ROOT" log -1 --format=%ct -- crates Cargo.toml Cargo.lock 2> /dev/null || echo 0)"
built="$(stat -c %Y "$PACKAGE" 2> /dev/null || stat -f %m "$PACKAGE")"
if [ "$built" -lt "$newest_src" ]; then
    fail "the package is newer than the last source commit" "$PACKAGE predates HEAD; rebuild with build-packages.sh $DEB_ARCH"
    summary; exit 1
fi
pass "$(basename "$PACKAGE")"

IMAGE="cameodb-systemd-suite:$DEB_ARCH"
PROJECT="cameodb-provlinux-$$"
CTR="$PROJECT"
WORK="$(mktemp -d)"
cleanup() {
    docker rm -f "$CTR" > /dev/null 2>&1
    discard_work "$WORK"
}
trap cleanup EXIT

if ! docker image inspect "$IMAGE" > /dev/null 2>&1; then
    printf '  building %s\n' "$IMAGE"
    # A file, not stdin: run_bounded backgrounds the call, and a background job reads /dev/null.
    cat > "$WORK/Dockerfile" <<'EOF'
FROM debian:bookworm-slim
ENV container=docker DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
        systemd systemd-sysv dbus curl python3 ca-certificates procps \
    && rm -rf /var/lib/apt/lists/*
STOPSIGNAL SIGRTMIN+3
CMD ["/sbin/init"]
EOF
    if ! run_bounded 600 docker build -q --platform "$PLATFORM" -t "$IMAGE" "$WORK" > "$WORK/image.log" 2>&1
    then
        fail "the systemd image builds" "$(tail -3 "$WORK/image.log" | tr '\n' ' ')"
        summary; exit 1
    fi
fi

# systemd as PID 1 needs the cgroup tree and tmpfs /run, /tmp; --privileged is the portable way.
if ! run_bounded 60 docker run -d --name "$CTR" --platform "$PLATFORM" --privileged --cgroupns=host \
        --tmpfs /run --tmpfs /run/lock -v /sys/fs/cgroup:/sys/fs/cgroup:rw \
        -v "$PACKAGE":/pkg/cameodb.deb:ro "$IMAGE" > /dev/null; then
    fail "the systemd container starts" "docker run failed"
    summary; exit 1
fi
dx() { run_bounded 120 docker exec "$CTR" "$@"; }

booted=""
for _ in $(seq 1 30); do
    state="$(dx systemctl is-system-running 2> /dev/null || true)"
    case "$state" in running|degraded) booted=1; break ;; esac
    sleep 1
done
[ -n "$booted" ] && pass "systemd is PID 1 ($state)" || { fail "systemd is PID 1" "state: ${state:-none}"; summary; exit 1; }

PORT=9480
BASE="http://127.0.0.1:$PORT"
json='content-type: application/json'
# These run inside the container: the node there listens on 0.0.0.0:9480 under the shipped config.
code() { local key="$1"; shift; dx curl -s -o /dev/null -w '%{http_code}' -m 20 -H "Authorization: Bearer $key" "$@"; }
body() { local key="$1"; shift; dx curl -s -m 20 -H "Authorization: Bearer $key" "$@"; }
client() {
    local key_file="$1"; shift
    dx env -u CAMEODB_API_KEY NO_COLOR=1 cameodb client -c "$BASE" --api-key-file "$key_file" "$@"
}
write_doc() { code "$1" -X PUT "$BASE/api/$2/document" -H "$json" -d "{\"id\":\"$3\",\"doc\":{\"t\":\"$4\"}}"; }
search() { code "$1" -X POST "$BASE/api/$2/search" -H "$json" -d '{"query":"*"}'; }
field() { python3 -I -c 'import json,sys; print(json.load(sys.stdin)[sys.argv[1]])' "$1"; }
mode_owner() { dx stat -c '%U:%G %a' "$1" 2> /dev/null | tr -d '\r'; }
wait_serving() {
    local i
    for i in $(seq 1 40); do
        dx curl -s -m 2 -o /dev/null "$BASE/_cluster/health" && return 0
        sleep 1
    done
    return 1
}

section "install"
if dx dpkg -i /pkg/cameodb.deb > "$WORK/dpkg.log" 2>&1; then
    pass "dpkg -i installs the package"
else
    fail "dpkg -i installs the package" "$(dx dpkg -i /pkg/cameodb.deb 2>&1 | tail -5)"
    summary; exit 1
fi
check_eq "keys.d belongs to the service"  "cameodb:cameodb 750" "$(mode_owner /etc/cameodb/keys.d)"
check_eq "keys is the operator's"         "root:cameodb 750"    "$(mode_owner /etc/cameodb/keys)"
check_eq "the config is not the service's to write" "root:cameodb 640" "$(mode_owner /etc/cameodb/cameodb.toml)"
if wait_serving; then pass "the packaged node starts under systemd"; else fail "the packaged node starts under systemd" "$(dx journalctl -u cameodb -n 20 --no-pager 2>&1)"; summary; exit 1; fi
check_eq "the service runs as cameodb" "cameodb" "$(dx ps -o user= -C cameodb | head -1 | tr -d ' \r')"

section "the operator's setup"
dx install -d -m 700 /root/k
for role in admin:ops provisioner:portal; do
    dx cameodb keygen --role "${role%%:*}" --label "${role##*:}" \
        --hash-out "/etc/cameodb/keys/${role##*:}" --key-out "/root/k/${role##*:}.key" > /dev/null 2>&1
done
dx chown root:cameodb /etc/cameodb/keys/ops /etc/cameodb/keys/portal
dx chmod 640 /etc/cameodb/keys/ops /etc/cameodb/keys/portal
ADMIN_KEY="$(dx cat /root/k/ops.key | tr -d '\r\n')"
PORTAL_KEY="$(dx cat /root/k/portal.key | tr -d '\r\n')"
# What the shipped config's own comments say to do: uncomment include, enable security, name the
# managed files, add the operator's keys.
# Not dx: that backgrounds the call, which reads no stdin.
docker exec -i "$CTR" python3 -I - <<'EOF'
p = "/etc/cameodb/cameodb.toml"
s = open(p).read()
for old, new in (
    ('# include = ["keys.d/*.toml"]', 'include = ["keys.d/*.toml"]'),
    ("[security]\nenabled = false", "[security]\nenabled = true"),
):
    assert old in s, old
    s = s.replace(old, new, 1)
s += '''
[security.managed_files]
dir = "keys.d"
keys = "keys.toml"
tenant = "{tenant}.toml"

[[security.api_keys]]
key_hash_file = "/etc/cameodb/keys/ops"
role = "admin"
label = "ops"

[[security.api_keys]]
key_hash_file = "/etc/cameodb/keys/portal"
role = "provisioner"
label = "portal"
'''
open(p, "w").write(s)
EOF
if dx runuser -u cameodb -- cameodb check-config -c /etc/cameodb/cameodb.toml > "$WORK/check.log" 2>&1; then
    pass "check-config accepts the edited config as the service user"
else
    fail "check-config accepts the edited config as the service user" "$(tail -5 "$WORK/check.log")"
fi
dx systemctl restart cameodb
if wait_serving; then pass "the node restarts with authentication on"; else fail "the node restarts with authentication on" "$(dx journalctl -u cameodb -n 20 --no-pager 2>&1)"; summary; exit 1; fi
check_eq "unauthenticated requests are refused" "401" "$(dx curl -s -o /dev/null -w '%{http_code}' "$BASE/_indexes")"

section "keys minted under the sandbox"
client /root/k/portal.key keys create --role writer --label jane@company.com --indexes 'docs-*' \
    > "$WORK/jane.key" 2> "$WORK/jane.about"
JANE_KEY="$(tr -d '\r\n' < "$WORK/jane.key")"
case "$JANE_KEY" in
    cameo_v1_*) pass "keys create works with /etc read-only to the node" ;;
    *) fail "keys create works with /etc read-only to the node" "$(cat "$WORK/jane.about") / $(dx journalctl -u cameodb -n 10 --no-pager 2>&1 | tail -4)" ;;
esac
JANE_ID="$(grep -o '"key_id": "[0-9a-f]*"' "$WORK/jane.about" | grep -o '[0-9a-f]\{8\}')"
check_eq "the node wrote keys.toml, owned by the service" "cameodb:cameodb 600" "$(mode_owner /etc/cameodb/keys.d/keys.toml)"
check_eq "no temp file left in keys.d" "keys.toml" "$(dx ls -A /etc/cameodb/keys.d | tr -d '\r' | tr '\n' ' ' | sed 's/ $//')"
check_eq "jane writes under docs-*"   "200" "$(write_doc "$JANE_KEY" docs-public 1 hello)"
check_eq "jane is refused outside it" "403" "$(write_doc "$JANE_KEY" wiki 1 x)"
check_eq "the node cannot write elsewhere under /etc" "denied" \
    "$(dx runuser -u cameodb -- sh -c 'touch /etc/cameodb/probe 2>/dev/null && echo wrote || echo denied' | tr -d '\r')"
check_eq "the operator's config is unchanged by the node" "root:cameodb 640" "$(mode_owner /etc/cameodb/cameodb.toml)"

section "the watcher"
HAND_KEY="$(dx cameodb keygen --role reader --label hand@company.com 2> "$WORK/hand.stanza" | tr -d '\r\n')"
HAND_HASH="$(grep -o 'sha256:[0-9a-f]\{64\}' "$WORK/hand.stanza" | head -1)"
dx sh -c "printf '[[security.api_keys]]\nkey_hash = \"$HAND_HASH\"\nrole = \"reader\"\nallowed_indexes = [\"docs-*\"]\n' > /etc/cameodb/keys.d/.users.tmp \
    && chown cameodb:cameodb /etc/cameodb/keys.d/.users.tmp && mv /etc/cameodb/keys.d/.users.tmp /etc/cameodb/keys.d/users.toml"
late=000
for _ in $(seq 1 20); do
    late="$(search "$HAND_KEY" docs-public)"
    [ "$late" = 200 ] && break
    sleep 0.5
done
check_eq "a file dropped into keys.d is live without a reload (inotify)" "200" "$late"

section "a company tenant"
check_eq "tenant acme is created" "200" \
    "$(code "$PORTAL_KEY" -X PUT "$BASE/_tenants/acme" -H "$json" -d '{"allowed_indexes":["acme-docs"]}')"
check_eq "its file is written by the node" "cameodb:cameodb 600" "$(mode_owner /etc/cameodb/keys.d/acme.toml)"
ANA_KEY="$(body "$PORTAL_KEY" -X POST "$BASE/_tenants/acme/keys" -H "$json" -d '{"role":"writer","label":"ana@acme.com"}' | field key)"
check_eq "ana writes acme-docs"          "200" "$(write_doc "$ANA_KEY" acme-docs 1 plan)"
check_eq "ana is refused on docs-public" "403" "$(write_doc "$ANA_KEY" docs-public 9 x)"

section "revocation, restart, reinstall"
client /root/k/portal.key keys revoke "$JANE_ID" > /dev/null 2>&1
check_eq "a revoked key is refused at once" "401" "$(search "$JANE_KEY" docs-public)"
dx systemctl restart cameodb
if wait_serving; then pass "systemctl restart brings the node back"; else fail "systemctl restart brings the node back" "$(dx journalctl -u cameodb -n 20 --no-pager 2>&1)"; fi
check_eq "ana's key survives a restart"   "200" "$(search "$ANA_KEY" acme-docs)"
check_eq "the hand-written key survives"  "200" "$(search "$HAND_KEY" docs-public)"
check_eq "jane stays revoked"             "401" "$(search "$JANE_KEY" docs-public)"
dx systemctl reload cameodb
sleep 1
check_eq "systemctl reload keeps every key" "200" "$(search "$ANA_KEY" acme-docs)"

if dx dpkg -i /pkg/cameodb.deb > "$WORK/reinstall.log" 2>&1; then pass "the package reinstalls over a node with managed keys"; else fail "the package reinstalls over a node with managed keys" "$(tail -5 "$WORK/reinstall.log")"; fi
if wait_serving; then pass "the node is serving after the upgrade"; else fail "the node is serving after the upgrade" "$(dx journalctl -u cameodb -n 20 --no-pager 2>&1)"; fi
check_eq "ana's key survives an upgrade"  "200" "$(search "$ANA_KEY" acme-docs)"
check_eq "the config edits survive"       "1"   "$(dx grep -c '^include = \["keys.d/\*.toml"\]' /etc/cameodb/cameodb.toml | tr -d '\r')"
check_eq "keys.d keeps its owner"         "cameodb:cameodb 750" "$(mode_owner /etc/cameodb/keys.d)"

section "no minted key is written down"
leaked=0
for k in "$JANE_KEY" "$ANA_KEY"; do
    dx grep -rqF "$k" /etc/cameodb /var/log/cameodb /var/lib/cameodb 2> /dev/null && leaked=$((leaked + 1))
    dx journalctl -u cameodb --no-pager 2> /dev/null | grep -qF "$k" && leaked=$((leaked + 1))
done
check_eq "no minted key in /etc, the logs, the journal or the data" 0 "$leaked"

section "purge"
dx dpkg -P cameodb > "$WORK/purge.log" 2>&1 || true
check_eq "purge removes the managed keys with /etc/cameodb" "gone" "$(dx test -e /etc/cameodb && echo present || echo gone | tr -d '\r')"

summary
