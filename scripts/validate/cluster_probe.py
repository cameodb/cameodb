#!/usr/bin/env python3
"""HTTP side of the cluster suite. cluster.sh owns the containers; this owns the requests.

Every subcommand exits 0 when its property holds and 1 when it does not, and prints what it
saw either way, so the shell side can turn the exit code into a PASS or FAIL line and still
show the evidence. Nodes come from CLUSTER_NODES, a comma-separated list of base URLs.

  converge <seconds>                       every node sees every node, and agrees on the ring
  storm <seconds> <writers> <deleters>     the actor-cycle load; prints METRIC lines
  probe <timeout>                          a fresh-index write and an index delete on every node
  seed <index> <count>                     write <count> documents, spread across the nodes
  count <index> <count>                    every node finds exactly <count> of them
  stats                                    the swarm counters from each node's health body
  warm <index>                             a bulk batch through every node until each answers fast
  fault <seconds> <from> <to> <index>      node1's health, searches and writes while a peer is
                                           faulted between <from> and <to>; prints METRIC lines
"""
import collections
import json
import os
import sys
import threading
import time
import urllib.error
import urllib.request

NODES = [n for n in os.environ.get("CLUSTER_NODES", "").split(",") if n]
OP_TIMEOUT = 20


def call(method, url, body=None, timeout=OP_TIMEOUT):
    """(status, seconds, parsed body or None). Status is an int, or a word for no response."""
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    started = time.time()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read()
            status = r.status
    except urllib.error.HTTPError as e:
        raw = e.read()
        status = e.code
    except Exception as e:  # timeout, reset, refused
        return ("timeout" if "timed out" in str(e) else type(e).__name__), time.time() - started, None
    try:
        parsed = json.loads(raw) if raw else None
    except ValueError:
        parsed = None
    return status, time.time() - started, parsed


def health(node):
    status, _, body = call("GET", node + "/_cluster/health", timeout=5)
    return body if status == 200 and isinstance(body, dict) else None


def ring_view():
    """One line per node, and whether the views agree with each other and with the nodes."""
    views = [health(n) for n in NODES]
    lines = []
    for node, v in zip(NODES, views):
        if v is None:
            lines.append(f"{node} no health")
        else:
            lines.append(f"{node} connected {v.get('connected_nodes')}/{len(NODES)} "
                         f"ring {v.get('cluster_total_shards')} local {v.get('active_shards')}")
    if any(v is None for v in views):
        return False, lines
    # The ring every node should hold is the sum of what each node owns. Agreeing with each
    # other is not enough: three nodes that all missed the same peer agree too.
    owned = sum(v.get("active_shards") or 0 for v in views)
    ok = owned > 0 and all(v.get("connected_nodes") == len(NODES)
                           and v.get("cluster_total_shards") == owned for v in views)
    return ok, lines


def cmd_converge(seconds):
    started = time.time()
    while True:
        ok, lines = ring_view()
        if ok:
            print(f"converged in {time.time() - started:.0f}s ({lines[0].split(' ', 1)[1]})")
            return 0
        if time.time() - started >= seconds:
            print(f"not converged after {seconds}s: " + "; ".join(lines))
            return 1
        time.sleep(1)


def percentile(values, p):
    if not values:
        return 0.0
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(round(p / 100 * (len(ordered) - 1))))]


def cmd_storm(seconds, writers, deleters):
    """Drive the two ask edges that once formed a cycle across nodes, on every node at once.

    A write to a brand-new index makes the orchestrator canvass its peers for a schema from
    inside its mailbox; a cluster-wide index delete fans out to every node. A deadlock between
    them is permanent, so it shows in `probe` afterwards, not here. Here it shows as the
    latency and timeout numbers this prints, which are the baseline the cluster work moves.
    """
    counts = collections.defaultdict(collections.Counter)
    took = collections.defaultdict(list)
    lock = threading.Lock()
    stop = threading.Event()
    run = f"{int(time.time()) % 100000}"

    def record(kind, status, seconds_):
        with lock:
            counts[kind][str(status)] += 1
            took[kind].append(seconds_)

    def writer(node, tag):
        i = 0
        while not stop.is_set():
            status, t, _ = call("PUT", f"{node}/api/w{run}n{tag}x{i}/document",
                                {"id": "a", "doc": {"title": f"t{i}", "n": i}})
            record("new-index write", status, t)
            i += 1

    def deleter(node, tag):
        j = 0
        while not stop.is_set():
            idx = f"d{run}n{tag}x{j}"
            status, t, _ = call("PUT", f"{node}/api/{idx}/document",
                                {"id": "a", "doc": {"title": "gone soon"}})
            record("create for delete", status, t)
            status, t, _ = call("DELETE", f"{node}/api/{idx}")
            record("index delete", status, t)
            j += 1

    threads = []
    for k, node in enumerate(NODES):
        threads += [threading.Thread(target=writer, args=(node, f"{k}{t}"), daemon=True)
                    for t in range(writers)]
        threads += [threading.Thread(target=deleter, args=(node, f"{k}{t}"), daemon=True)
                    for t in range(deleters)]
    for t in threads:
        t.start()
    time.sleep(seconds)
    stop.set()
    # A call parked in a stuck actor returns only at its own timeout.
    for t in threads:
        t.join(OP_TIMEOUT + 5)

    for kind in ("new-index write", "create for delete", "index delete"):
        c = counts[kind]
        total = sum(c.values())
        ok = c.get("200", 0) + c.get("201", 0)
        other = {k: v for k, v in sorted(c.items()) if k not in ("200", "201")}
        t = took[kind]
        print(f"METRIC storm {kind:<17} {total:>4} ops  {ok / seconds:5.2f} ok/s  "
              f"p50 {percentile(t, 50):5.2f}s  p99 {percentile(t, 99):5.2f}s  "
              f"max {max(t, default=0):5.2f}s  not-ok {json.dumps(other)}")
    return 0


def cmd_probe(timeout):
    ok = True
    stamp = int(time.time() * 1000) % 10**9
    for k, node in enumerate(NODES):
        idx = f"probe{k}x{stamp}"
        w, wt, _ = call("PUT", f"{node}/api/{idx}/document",
                        {"id": "p", "doc": {"title": "probe"}}, timeout=timeout)
        d, dt, _ = call("DELETE", f"{node}/api/{idx}", timeout=timeout)
        good = w in (200, 201) and d == 200
        ok &= good
        print(f"  {node}: write {w} in {wt:.1f}s, delete {d} in {dt:.1f}s"
              + ("" if good else "   <-- stuck or refused"))
    print(f"all {len(NODES)} nodes answered" if ok else "a node is stuck or refusing")
    return 0 if ok else 1


def cmd_seed(index, count):
    failed = 0
    for i in range(count):
        node = NODES[i % len(NODES)]
        status, _, _ = call("PUT", f"{node}/api/{index}/document",
                            {"id": f"d{i}", "doc": {"title": "survivor", "n": i}})
        failed += status not in (200, 201)
    print(f"wrote {count - failed}/{count} documents to '{index}'")
    return 0 if failed == 0 else 1


def cmd_count(index, count):
    ok = True
    for node in NODES:
        status, _, body = call("POST", f"{node}/api/{index}/search",
                               {"query": "title:survivor", "limit": 1})
        hits = body.get("total_hits") if isinstance(body, dict) else None
        good = status == 200 and hits == count
        ok &= good
        print(f"  {node}: {status}, {hits} of {count} found" + ("" if good else "   <-- lost"))
    print(f"every node finds all {count}" if ok else "a node is missing documents")
    return 0 if ok else 1


def cmd_stats():
    """The swarm counters each node keeps, as METRIC lines. Always exits 0."""
    for node in NODES:
        v = health(node) or {}
        print(f"METRIC {node} dial_failures {v.get('dial_failures')} "
              f"bootstrap_successes {v.get('bootstrap_successes')} "
              f"routing_updates {v.get('routing_updates')}")
    return 0


def cmd_warm(index, limit=60):
    """Send a bulk batch through every node until all of them answer inside a second.

    A node that restarted holds no schema for an index minted while it was away, and its first
    write there canvasses the peers from inside its mailbox — seconds, when a peer restarted
    with it. A fault injected in that window lands on requests already queued behind the
    canvass, which then wait out the whole request timeout, and the fault's numbers measure
    that timing rather than the build. This is the settling step before one.
    """
    started = time.time()
    rounds = 0
    while True:
        rounds += 1
        slowest = []
        for k, node in enumerate(NODES):
            docs = [{"id": f"warm{rounds}n{k}d{d}", "doc": {"title": "warm", "n": d}}
                    for d in range(30)]
            status, took, _ = call("POST", f"{node}/api/{index}/_bulk", docs, timeout=OP_TIMEOUT)
            slowest.append((node, status, took))
        if all(st in (200, 201) and took < 1.0 for _, st, took in slowest):
            print(f"every node answers in under 1s after {time.time() - started:.0f}s "
                  f"({rounds} round{'s' if rounds > 1 else ''})")
            return 0
        if time.time() - started >= limit:
            print("not settled after {}s: {}".format(limit, "; ".join(
                f"{n} {st} in {t:.1f}s" for n, st, t in slowest)))
            return 1
        time.sleep(1)


def cmd_fault(seconds, fault_from, fault_to, index):
    """What the first node's clients see while another node is faulted.

    The shell side does the faulting on the same clock: it starts this, waits `fault_from`
    seconds, faults the peer, and restores it at `fault_to`. Health is polled every second,
    searches fan out to every node, and single writes go to `index` (which must already exist,
    so no write here mints a schema). Exits 1 if health ever failed to answer in 5 s — the one
    property that must hold on a node whose peer is gone.
    """
    node = NODES[0]
    started = time.time()
    events = []
    lock = threading.Lock()

    def rec(kind, at, status, took, extra=None):
        with lock:
            events.append((at - started, kind, status, took, extra))

    def loop(kind, fn, pause):
        while time.time() - started < seconds:
            at = time.time()
            status, took, extra = fn()
            rec(kind, at, status, took, extra)
            time.sleep(max(0, pause - (time.time() - at)))

    def health_once():
        status, took, body = call("GET", node + "/_cluster/health", timeout=5)
        view = (body.get("connected_nodes"), body.get("status")) if isinstance(body, dict) else None
        return status, took, view

    def search_once():
        status, took, _ = call("POST", f"{node}/api/survivor/search",
                               {"query": "title:survivor", "limit": 1})
        return status, took, None

    counter = iter(range(10**9))
    counter_lock = threading.Lock()

    def next_id():
        with counter_lock:
            return next(counter)

    # Past the router's own give-up (two attempts at the transport timeout), so a write the
    # cluster refused reads as its status, not as this client losing patience first.
    write_timeout = 45

    def write_once():
        i = next_id()
        status, took, _ = call("PUT", f"{node}/api/{index}/document",
                               {"id": f"f{i}", "doc": {"title": "during", "n": i}},
                               timeout=write_timeout)
        return status, took, None

    def bulk_once():
        # Spread over every shard, so each batch has a share for the frozen node.
        base = next_id() * 1000
        docs = [{"id": f"b{base + k}", "doc": {"title": "during", "n": k}} for k in range(30)]
        status, took, body = call("POST", f"{node}/api/{index}/_bulk", docs, timeout=write_timeout)
        written = body.get("items_written") if isinstance(body, dict) else None
        return status, took, written

    # Several single writers: one parked on a key the frozen node owns must not hide whether
    # writes to every other key kept going.
    loops = [("health", health_once, 1.0), ("search", search_once, 0.5),
             ("bulk", bulk_once, 0.5)] + [("write", write_once, 0.2)] * 4
    threads = [threading.Thread(target=loop, args=a, daemon=True) for a in loops]
    for t in threads:
        t.start()
    for t in threads:
        t.join(seconds + write_timeout + 30)

    def phase(t):
        return "before" if t < fault_from else ("during" if t < fault_to else "after")

    for kind in ("write", "bulk", "search"):
        for ph in ("before", "during", "after"):
            es = [e for e in events if e[1] == kind and phase(e[0]) == ph]
            if not es:
                continue
            t = [e[3] for e in es]
            ok = sum(1 for e in es if e[2] in (200, 201))
            other = collections.Counter(str(e[2]) for e in es if e[2] not in (200, 201))
            docs = ""
            if kind == "bulk":
                written = sum(e[4] or 0 for e in es)
                docs = f"  docs {written}/{30 * len(es)}"
            print(f"METRIC fault {kind:<6} {ph:<6} {len(es):>4} ops  ok {ok:>4}  "
                  f"p50 {percentile(t, 50):5.2f}s  max {max(t):5.2f}s  not-ok {json.dumps(dict(other))}"
                  f"{docs}")
    health = sorted(e for e in events if e[1] == "health")
    healthy_view = health[0][4] if health else None
    noticed = next((e[0] for e in health
                    if fault_from <= e[0] < fault_to + 5 and e[4] != healthy_view), None)
    print("METRIC fault node1 health noticed the fault "
          + (f"{noticed - fault_from:.0f}s after it began" if noticed is not None
             else f"never (stayed {healthy_view})"))
    failed = [e for e in health if e[2] != 200]
    print(f"node1 health answered {len(health) - len(failed)} of {len(health)} polls within 5s")
    return 0 if health and not failed else 1


def main():
    if not NODES:
        sys.exit("CLUSTER_NODES is not set")
    cmd, args = sys.argv[1], sys.argv[2:]
    if cmd == "converge":
        return cmd_converge(int(args[0]))
    if cmd == "storm":
        return cmd_storm(*(int(a) for a in args[:3]))
    if cmd == "probe":
        return cmd_probe(int(args[0]))
    if cmd == "seed":
        return cmd_seed(args[0], int(args[1]))
    if cmd == "count":
        return cmd_count(args[0], int(args[1]))
    if cmd == "stats":
        return cmd_stats()
    if cmd == "warm":
        return cmd_warm(args[0])
    if cmd == "fault":
        return cmd_fault(int(args[0]), int(args[1]), int(args[2]), args[3])
    sys.exit(f"unknown subcommand: {cmd}")


if __name__ == "__main__":
    sys.exit(main())
