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
    sys.exit(f"unknown subcommand: {cmd}")


if __name__ == "__main__":
    sys.exit(main())
