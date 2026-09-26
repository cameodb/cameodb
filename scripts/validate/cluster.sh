#!/usr/bin/env bash
# Cluster validation: three nodes in Docker, and the properties only a real cluster has.
#
#   formation   nodes started together find each other and agree on one ring; a node
#               started before its seeds joins once they are up
#   storm       new-index writes and index deletes on every node at once leave every node
#               answering — the load that once deadlocked orchestrator and coordinator mailboxes
#               across nodes (OB19)
#   cross-node  requests through every node at once, as behind a load balancer: new indexes
#               minted on every node by single and by bulk writes, one index minted by every
#               node at the same moment (exactly one mints, the rest adopt), and bulk writes
#               whose shares cross between every pair of nodes; no request fails or stalls, and every node answers afterwards
#   restart     two of three nodes restarted together rejoin, converge and serve, and no
#               committed document is lost
#   frozen peer node3 paused: node1 keeps answering, and after it resumes the ring converges,
#               every node serves and no document is lost
#   logs        no panic; plus the connection and peer-loss counts the cluster work moves
#
# Checks are PASS/FAIL. Lines starting METRIC are measurements, not verdicts: latency and
# timeouts under the storm, time to converge, and connection churn. Compare them across builds.
#
# Not part of the default `all.sh` run — it needs Docker and takes several minutes:
#   scripts/validate/all.sh cluster
#   CLUSTER_IMAGE=cameodb:pre scripts/validate/cluster.sh     # an image already built

set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "$SCRIPT_DIR/lib.sh"

ROOT="$(repo_root)"
PROBE="$SCRIPT_DIR/cluster_probe.py"
PORT_BASE="${CLUSTER_PORT_BASE:-19481}"
PORTS=("$PORT_BASE" "$((PORT_BASE + 1))" "$((PORT_BASE + 2))")
FORM_SECS="${CLUSTER_FORM_SECS:-60}"
FORM_ROUNDS="${CLUSTER_FORM_ROUNDS:-3}"
LATE_SEED_SECS=10
STORM_SECS="${CLUSTER_STORM_SECS:-60}"
STORM_WRITERS="${CLUSTER_STORM_WRITERS:-2}"
STORM_DELETERS="${CLUSTER_STORM_DELETERS:-1}"
IDLE_SECS="${CLUSTER_IDLE_SECS:-60}"
ROUNDS="${CLUSTER_RESTART_ROUNDS:-3}"
FREEZE_SECS="${CLUSTER_FREEZE_SECS:-60}"
CROSS_SECS="${CLUSTER_CROSS_SECS:-20}"
CROSS_WRITERS="${CLUSTER_CROSS_WRITERS:-2}"
SAMEMINT_ROUNDS="${CLUSTER_SAMEMINT_ROUNDS:-20}"
SURVIVORS=30
PROJECT="cameodb-validate"
WORK=""

export CLUSTER_NODES="http://127.0.0.1:${PORTS[0]},http://127.0.0.1:${PORTS[1]},http://127.0.0.1:${PORTS[2]}"

for tool in docker python3 curl; do
    if ! command -v "$tool" > /dev/null 2>&1; then
        skip "$tool not available; the cluster suite needs docker, python3 and curl"
        summary
        exit $?
    fi
done

# run_bounded <seconds> <command...> — a Docker call that never returns would otherwise hang
# the suite with no verdict. This has happened: a wedged VM left `docker restart` blocked for
# minutes while every node was unreachable.
run_bounded() {
    local secs="$1"; shift
    "$@" &
    local pid=$!
    ( sleep "$secs"; kill "$pid" 2> /dev/null ) &
    local watchdog=$!
    wait "$pid"
    local rc=$?
    kill "$watchdog" 2> /dev/null
    wait "$watchdog" 2> /dev/null
    return "$rc"
}

compose() { run_bounded 120 docker compose -p "$PROJECT" -f "$WORK/compose.yml" "$@"; }

probe() { python3 "$PROBE" "$@" | tee "$WORK/last-probe.out"; }
# The index prefix a `samemint` probe printed on its last line.
probe_last_prefix() { sed -n 's/.*index prefix \([a-z0-9]*\)$/\1/p' "$WORK/last-probe.out" | tail -1; }

# check_cmd <description> <command...> — PASS or FAIL on the exit code, with the command's
# last line as the evidence, and every line it printed shown above the verdict.
check_cmd() {
    local desc="$1"; shift
    local out rc
    out="$("$@" 2>&1)"
    rc=$?
    printf '%s\n' "$out" | sed '$d' | sed '/^$/d'
    if [ "$rc" -eq 0 ]; then
        pass "$desc: $(printf '%s\n' "$out" | tail -1 | sed 's/^ *//')"
    else
        fail "$desc" "$(printf '%s\n' "$out" | tail -1 | sed 's/^ *//')"
    fi
    return "$rc"
}

collect_logs() {
    local tag="$1" n
    mkdir -p "$WORK/logs"
    for n in 1 2 3; do
        run_bounded 30 docker logs "$PROJECT-node$n" 2>&1 \
            | sed $'s/\x1b\\[[0-9;]*m//g' > "$WORK/logs/$tag-node$n.log"
    done
}

cleanup() {
    [ -n "$WORK" ] && [ -f "$WORK/compose.yml" ] && compose down -t 5 > /dev/null 2>&1
    if [ -n "${CLUSTER_KEEP:-}" ] && [ -n "$WORK" ]; then
        printf '\nKept for inspection (CLUSTER_KEEP): %s\n' "$WORK" >&2
        return
    fi
    discard_work "$WORK"
}
trap cleanup EXIT

# A Mac that sleeps mid-run suspends the probes and the VM together, and the run then measures
# the sleep. Hold the machine awake for as long as this script lives.
if command -v caffeinate > /dev/null 2>&1; then
    caffeinate -i -s -w $$ &
fi

for p in "${PORTS[@]}"; do require_free_port "$p"; done
WORK="$(mktemp -d)"
# A throwaway key per run; the "internal" profile refuses a clustered node without one.
PSK="$(python3 -c 'import secrets; print(secrets.token_hex(32))')"

section "image"
IMAGE="${CLUSTER_IMAGE:-}"
if [ -z "$IMAGE" ]; then
    IMAGE="cameodb:validate"
    # The same CA handling as scripts/build/docker-push.sh: behind a TLS-inspecting proxy the
    # build's `cargo fetch` fails on every crates.io request without it.
    ca="${CAMEODB_CA_CERT:-/var/tmp/buildkit-ca/corporate-ca.crt}"
    build_args=()
    [ -s "$ca" ] && build_args+=(--secret "id=corporate-ca,src=$ca" --build-arg "CORPORATE_CA_ID=$ca")
    printf '  building %s from %s (log: %s)\n' "$IMAGE" "$ROOT/Dockerfile" "$WORK/build.log"
    if ! run_bounded 1800 docker build ${build_args[@]+"${build_args[@]}"} -t "$IMAGE" -f "$ROOT/Dockerfile" "$ROOT" \
        > "$WORK/build.log" 2>&1; then
        fail "image builds" "$(tail -3 "$WORK/build.log" | tr '\n' ' ')"
        summary
        exit 1
    fi
fi
version="$(run_bounded 60 docker run --rm "$IMAGE" --version 2>&1 | tail -1)"
case "$version" in
    *+fault-injection*) fail "image is a clean build" "$IMAGE reports $version"; summary; exit 1 ;;
    cameodb*) pass "image $IMAGE runs ($version)" ;;
    *) fail "image $IMAGE runs" "--version printed: $version"; summary; exit 1 ;;
esac

# One compose file per run, written here rather than shipped: the ports, image and data paths
# are all per-run, and a fresh data directory per start sidesteps a Docker Desktop / OrbStack
# bind mount that goes stale when a path is deleted and recreated under it.
write_compose() {
    local data="$1" n
    mkdir -p "$data/node1" "$data/node2" "$data/node3"
    chmod -R 777 "$data"
    {
        echo "services:"
        for n in 1 2 3; do
            cat <<EOF
  node$n:
    image: $IMAGE
    container_name: $PROJECT-node$n
    user: "65532:65532"
    environment:
      - RUST_LOG=info
      - CAMEODB_NODE_LABEL=validate-node-$n
      - CAMEODB_CLUSTER_NAME=cameodb-validate
      - CAMEODB_CLUSTER_ENABLED=true
      - CAMEODB_HTTP_BIND_ADDRESS=node$n
      - CAMEODB_HTTP_PORT=9480
      - CAMEODB_CLUSTER_BIND_ADDRESS=node$n
      - CAMEODB_CLUSTER_PORT=9580
      - CAMEODB_SEED_NODES=node1:9580,node2:9580
      - CAMEODB_CLUSTER_NODES=node1:9580,node2:9580,node3:9580
      - CAMEODB_CLUSTER_PSK=$PSK
$(for kv in ${CLUSTER_NODE_ENV:-}; do printf '      - %s\n' "$kv"; done)
    ports:
      - "127.0.0.1:${PORTS[$((n - 1))]}:9480"
    volumes:
      - $data/node$n:/data/cameodb
    networks: [validate]
EOF
        done
        echo "networks:"
        echo "  validate:"
        echo "    driver: bridge"
    } > "$WORK/compose.yml"
}

wait_nodes() {
    local p
    for p in "$@"; do
        wait_for_http "http://127.0.0.1:$p/_cluster/health" 60 || return 1
    done
}

section "formation ($FORM_ROUNDS rounds, all three nodes started together)"
# Several rounds, because what goes wrong here is a race: on a fresh start the nodes exchange
# shard maps in one burst of a few tens of milliseconds, and a node that misses one peer's map
# in that burst has been seen to keep a partial ring for good. One round passing proves little.
formed=0
for r in $(seq 1 "$FORM_ROUNDS"); do
    [ "$r" -gt 1 ] && compose down -t 5 > /dev/null 2>&1
    write_compose "$WORK/data-together-$r"
    compose up -d > /dev/null 2>&1
    wait_nodes "${PORTS[@]}" || fail "round $r: all three nodes start" "a node never answered /_cluster/health"
    if check_cmd "round $r: every node sees every node and the same ring" probe converge "$FORM_SECS"; then
        formed=1
    else
        formed=0
        collect_logs "formation-$r"
    fi
done

section "late seeds (node3 started ${LATE_SEED_SECS}s before the seeds)"
# Every dial node3 makes at startup is refused. Nothing dials a node that is not a seed, so it
# joins only if it keeps redialing the seeds itself. Deterministic, unlike the rounds above.
compose down -t 5 > /dev/null 2>&1
write_compose "$WORK/data-late-seeds"
compose up -d node3 > /dev/null 2>&1
wait_nodes "${PORTS[2]}"
sleep "$LATE_SEED_SECS"
compose up -d node1 node2 > /dev/null 2>&1
wait_nodes "${PORTS[0]}" "${PORTS[1]}"
if check_cmd "node3 started first joins once the seeds are up" probe converge "$FORM_SECS"; then
    formed=1
else
    formed=0
    collect_logs late-seeds
fi

if [ "$formed" -eq 0 ]; then
    # The rest of the suite still has something to say about a cluster that formed. Start one
    # the way the shipped compose file does — seeds first — and carry on, so one failure here
    # does not hide the others.
    compose down -t 5 > /dev/null 2>&1
    write_compose "$WORK/data-staggered"
    compose up -d node1 > /dev/null 2>&1; wait_nodes "${PORTS[0]}"
    compose up -d node2 > /dev/null 2>&1; wait_nodes "${PORTS[1]}"
    compose up -d node3 > /dev/null 2>&1; wait_nodes "${PORTS[2]}"
    if ! check_cmd "started seeds first, every node sees every node and the same ring" \
        probe converge "$FORM_SECS"; then
        collect_logs formation-staggered
        summary
        exit 1
    fi
fi

section "storm (${STORM_SECS}s: $STORM_WRITERS new-index writers and $STORM_DELETERS create+delete loop per node)"
probe storm "$STORM_SECS" "$STORM_WRITERS" "$STORM_DELETERS"
check_cmd "every node writes and deletes right after the storm" probe probe 30
sleep "$IDLE_SECS"
check_cmd "every node writes and deletes after ${IDLE_SECS}s idle" probe probe 30
check_cmd "the ring is still converged after the storm" probe converge 10
check_cmd "no node was declared lost for answering pings late under the storm" probe pings-clean

section "cross-node (${CROSS_SECS}s each, $CROSS_WRITERS writers per node, every node at once)"
# The storm mints every index on one node (its writes all carry id "a"), and the frozen-peer
# phase writes through node1 alone, so neither makes two nodes wait on each other. These do.
check_cmd "new indexes minted on every node at once: none refused or stalled" \
    probe mints "$CROSS_SECS" "$CROSS_WRITERS"
check_cmd "bulk index written through one node" probe seed crossbulk 3
check_cmd "bulk index answers fast through every node" probe warm crossbulk
check_cmd "bulk writes through every node at once: none refused or stalled" \
    probe bulks "$CROSS_SECS" "$CROSS_WRITERS" crossbulk
check_cmd "new indexes minted by bulk writes through every node at once: none refused or stalled" \
    probe bulkmints "$CROSS_SECS" "$CROSS_WRITERS"
# Several nodes minting the *same* index at once: one of them must mint, the rest adopt its
# schema. The writes all succeeding is half of it; the other half is in the logs, where a node
# that minted says so — two for one index would be two schemas, built once and never merged.
if check_cmd "one new index written through every node at the same moment ($SAMEMINT_ROUNDS rounds): none refused or stalled" \
    probe samemint "$SAMEMINT_ROUNDS" "$CROSS_WRITERS"; then :; fi
samemint_prefix="$(probe_last_prefix)"
if [ -n "$samemint_prefix" ]; then
    minted="$(for n in 1 2 3; do
        run_bounded 30 docker logs "$PROJECT-node$n" 2>&1 | sed $'s/\x1b\\[[0-9;]*m//g' \
            | grep -o "initial schema creation index=${samemint_prefix}[0-9]*x" | sort -u
    done | sort | uniq -c)"
    indexes="$(printf '%s\n' "$minted" | grep -c . || true)"
    twice="$(printf '%s\n' "$minted" | awk '$1 > 1' | grep -c . || true)"
    if [ "$indexes" -eq "$SAMEMINT_ROUNDS" ] && [ "$twice" -eq 0 ]; then
        pass "each of those indexes was minted by exactly one node: $indexes of $SAMEMINT_ROUNDS"
    else
        fail "each of those indexes was minted by exactly one node" \
            "$indexes of $SAMEMINT_ROUNDS minted, $twice by more than one node"
    fi
fi
check_cmd "every node writes and deletes right after the cross-node load" probe probe 30
check_cmd "the ring is still converged after the cross-node load" probe converge 10

section "restart (node2 and node3 together, $ROUNDS rounds)"
check_cmd "seed documents written" probe seed survivor "$SURVIVORS"
# Past the idle commit (3s by default), so every one of them is durable before a restart.
sleep 6
check_cmd "seed documents visible from every node" probe count survivor "$SURVIVORS"
for r in $(seq 1 "$ROUNDS"); do
    if ! run_bounded 120 docker restart -t 5 "$PROJECT-node2" "$PROJECT-node3" > /dev/null; then
        fail "round $r: docker restart returned" "docker did not restart the nodes"
        continue
    fi
    wait_nodes "${PORTS[1]}" "${PORTS[2]}" || fail "round $r: restarted nodes answer" "no /_cluster/health"
    t0=$(date +%s)
    if check_cmd "round $r: rejoined and converged" probe converge "$FORM_SECS"; then
        printf 'METRIC restart round %s converged %ss after HTTP answered\n' "$r" "$(( $(date +%s) - t0 ))"
    fi
    check_cmd "round $r: every node writes and deletes" probe probe 30
done
check_cmd "no committed document lost across $ROUNDS restarts" probe count survivor "$SURVIVORS"

section "frozen peer (node3 paused ${FREEZE_SECS}s)"
# A process that stops without closing its connections — hung, swapped out, paused — is the
# failure a node cannot see from a closed socket. The kernel keeps its TCP up, so what the
# others notice, and how long each request waits on it, is up to the cluster layer.
check_cmd "writes to a fresh index before the freeze" probe seed frozen 3
# Settle first: a node back from the restart rounds canvasses its peers for this index's
# schema on its first write, and a freeze landing inside that canvass measures the timing.
check_cmd "the fresh index answers fast through every node before the freeze" probe warm frozen
fault_from=5
fault_to=$((fault_from + FREEZE_SECS))
probe fault $((fault_to + 20)) "$fault_from" "$fault_to" frozen > "$WORK/fault.txt" 2>&1 &
fault_pid=$!
sleep "$fault_from"
run_bounded 60 docker pause "$PROJECT-node3" > /dev/null || fail "docker paused node3" "docker pause failed"
sleep "$FREEZE_SECS"
run_bounded 60 docker unpause "$PROJECT-node3" > /dev/null || fail "docker resumed node3" "docker unpause failed"
wait "$fault_pid"
grep '^METRIC' "$WORK/fault.txt"
while IFS= read -r line; do
    case "$line" in
        "RESULT PASS "*) pass "${line#RESULT PASS }" ;;
        "RESULT FAIL "*) fail "${line#RESULT FAIL }" ;;
    esac
done < <(grep '^RESULT' "$WORK/fault.txt")
grep -q '^RESULT' "$WORK/fault.txt" || fail "the fault probe reported" "$(tail -3 "$WORK/fault.txt" | tr '\n' ' ')"
check_cmd "node3 resumed: every node sees every node and the same ring" probe converge "$FORM_SECS"
check_cmd "node3 resumed: every node writes and deletes" probe probe 30
check_cmd "no committed document lost across the freeze" probe count survivor "$SURVIVORS"

section "logs"
collect_logs final
panics=$(cat "$WORK"/logs/*.log | grep -c 'panicked' || true)
check_eq "no panic in any node log" 0 "$panics"
# Node1 is never restarted, so what it logged is an exact count against a known truth: two
# real peer departures per round.
n1="$WORK/logs/final-node1.log"
printf 'METRIC node1 peer-lost %s for %s real peer restarts and 1 freeze, connections closed %s\n' \
    "$(grep -c 'ClusterCoordinator: peer lost' "$n1")" "$((2 * ROUNDS))" \
    "$(grep -c 'Connection closed with' "$n1")"
# Two peers each, so anything past two connections per node is a duplicate dial.
for n in 1 2 3; do
    f="$WORK/logs/final-node$n.log"
    printf 'METRIC node%s connections established %s, ERROR %s, WARN %s\n' "$n" \
        "$(grep -c 'Connection established with' "$f")" \
        "$(grep -c ' ERROR ' "$f")" \
        "$(grep ' WARN ' "$f" | grep -vc 'posture:')"
done
probe stats
# The most frequent WARN/ERROR messages across all nodes, with ids, indexes and times folded so
# that repeats group. The counts above say how much; this says what.
printf '  most frequent WARN/ERROR lines, all nodes:\n'
grep -hE ' (WARN|ERROR) ' "$WORK"/logs/final-node*.log | grep -v 'posture:' \
    | sed -E 's/^[0-9T:.Z-]+ +//; s/actor\.handle_message\{[^}]*\}: //;
              s/[0-9a-f]{8}-[0-9a-f-]{27}/<uuid>/g; s/12D3KooW[A-Za-z0-9]+/<peer>/g;
              s/ConnectionId\([0-9]+\)/ConnectionId(<n>)/g; s/[a-z]+[0-9]+n[0-9]+x[0-9]+/<index>/g' \
    | cut -c1-160 | sort | uniq -c | sort -rn | head -8 | sed 's/^/    /'

summary
