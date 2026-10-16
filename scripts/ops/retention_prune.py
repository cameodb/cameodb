#!/usr/bin/env python3
"""Retention pruning for a CameoDB index, in three explicit steps.

    select  search for every document whose date field is older than a cutoff and
            write their ids to a file (one id per line) plus a manifest
    delete  read that file and send the ids to POST /api/{index}/_bulk/delete in batches
    count   how many documents match the cutoff right now (before: the select, after: ~0)
    check   verify the setup: Python, connection, key, index, field, disk (read-only unless
            --probe-write / --probe-admin are given)
    fields  list the date fields that can serve as the cutoff, with populated count,
            min/max and how many fall before the cutoff (pick one for --field)

CameoDB has no delete-by-query, no search cursor, and caps one search at
`max_search_limit` (10000) hits. `select` therefore splits the time range into windows,
bisects any window that is too big, and walks each window with a keyset
(`sort field:asc`, restart at the last timestamp, drop ids already seen on it).
Windows run in parallel and are resumable: a finished window leaves a `.done` marker
next to the output file.

Stdlib only. Auth: CAMEODB_API_KEY (Bearer). `select`/`count` need `read`, `delete` needs
`write`, `--commit` needs `node-admin`.
"""
import argparse
import datetime as dt
import json
import os
import shutil
import sys
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

FLOOR = dt.datetime(1980, 1, 1)
PAGE = 10000  # server max_search_limit
FMT = "%Y-%m-%dT%H:%M:%SZ"
lock = threading.Lock()


# ---------------------------------------------------------------- http
class Api:
    def __init__(self, base, index, key, timeout):
        self.base, self.index, self.timeout = base.rstrip("/"), index, timeout
        self.headers = {"Content-Type": "application/json"}
        if key:
            self.headers["Authorization"] = "Bearer " + key

    def call(self, method, path, body=None, retries=5):
        data = None if body is None else json.dumps(body).encode()
        for attempt in range(retries + 1):
            req = urllib.request.Request(self.base + path, data=data, method=method, headers=self.headers)
            try:
                with urllib.request.urlopen(req, timeout=self.timeout) as r:
                    return json.loads(r.read() or b"{}")
            except urllib.error.HTTPError as e:
                detail = e.read().decode(errors="replace")[:300]
                if e.code in (429, 502, 503, 504) and attempt < retries:
                    time.sleep(min(2 ** attempt, 30))
                    continue
                raise SystemExit(f"HTTP {e.code} on {method} {path}: {detail}")
            except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
                if attempt < retries:
                    time.sleep(min(2 ** attempt, 30))
                    continue
                raise SystemExit(f"{method} {path} failed after {retries} retries: {e}")

    def search(self, query, limit, fields=None, sort=None):
        body = {"query": query, "limit": limit}
        if fields:
            body["fields"] = fields
        if sort:
            body["sort"] = sort
        d = self.call("POST", f"/api/{self.index}/search", body)
        if d.get("_discarded_clauses"):
            raise SystemExit(f"server dropped clauses {d['_discarded_clauses']} in: {query}")
        return d


def ts(t):
    return t.strftime(FMT)


def parse(s):
    return dt.datetime.strptime(s, FMT)


def rng(field, lo, hi):
    """lo inclusive, hi exclusive."""
    return f"{field}:[{ts(lo)} TO {ts(hi)}}}"


# ---------------------------------------------------------------- fields
def date_fields(api):
    """Date fields with a fast column: the only ones a range cutoff and a sort both work on."""
    cfg = api.call("GET", f"/api/{api.index}/_config")
    return sorted(f["name"] for f in cfg["fields"]
                  if f["type"] == "date" and f.get("fast") and f.get("indexed"))


def require_date_field(api, field):
    ok = date_fields(api)
    if field not in ok:
        raise SystemExit(f"'{field}' is not an indexed, fast date field of '{api.index}'. "
                         f"Candidates ({len(ok)}): {', '.join(ok)}\nSee: fields subcommand")


def cmd_fields(a, api):
    cutoff = resolve_cutoff(a)
    names = date_fields(api)
    total = api.search("*", 0)["total_hits"]

    def one(f):
        # `[* TO *]` (both sides open) matches nothing on this server, so bound it explicitly
        any_ = f"{f}:[1900-01-01T00:00:00Z TO 2200-01-01T00:00:00Z]"
        pop = api.search(any_, 0)["total_hits"]
        if pop == 0:
            return f, pop, None, None, 0, 0
        lo = api.search(any_, 1, [f], {"field": f, "order": "asc"})["hits"]
        hi = api.search(any_, 1, [f], {"field": f, "order": "desc"})["hits"]
        old = api.search(f"{f}:[* TO {ts(cutoff)}}}", 0)["total_hits"]
        def one_val(h, pick):
            v = h[0][f] if h else None
            return pick(v) if isinstance(v, list) else v  # multi-valued field: hit lists every value
        multi = any(isinstance(h[0][f], list) for h in (lo, hi) if h)
        sent = api.search(f"{f}:[* TO {ts(FLOOR)}}}", 0)["total_hits"] if lo and one_val(lo, min) < ts(FLOOR) else 0
        return (f + (" *" if multi else ""), pop, one_val(lo, min), one_val(hi, max), old, sent)

    with ThreadPoolExecutor(a.workers) as ex:
        rows = list(ex.map(one, names))
    print(f"index '{api.index}': {total:,} documents, cutoff {ts(cutoff)}\n")
    print(f"{'field':<34}{'populated':>13}{'%':>6}  {'min':<21}{'max':<21}{'older than cutoff':>19}{'%':>6}")
    for f, pop, lo, hi, old, _ in rows:
        print(f"{f:<34}{pop:>13,}{pop / max(total, 1):>6.0%}  {lo or '-':<21}{hi or '-':<21}"
              f"{old:>19,}{old / max(total, 1):>6.0%}")
    for f, *_, sent in rows:
        if sent:
            print(f"\n{f.rstrip(' *')}: {sent:,} documents carry a placeholder date before {ts(FLOOR)} "
                  "(e.g. 1970-01-01). They are counted above as older than the cutoff but `select` skips them.")
    print("\n* multi-valued: min/max are of the hit's own values; `select` needs one value per document, so "
          "it refuses these.\nA document without the field is never selected. Fields ingested later or refreshed by "
          "re-scans (last_seen, *_record_timestamp, scanned_on) move over time, so the selection is a "
          "snapshot taken when `select` runs.")
    return 0


# ---------------------------------------------------------------- cutoff
DEFAULT_MONTHS = 6


def cutoff_for_months(today, months):
    """First day of the month, `months` back: keeps `months` previous months + the current one."""
    m = today.year * 12 + today.month - 1 - months
    return dt.datetime(m // 12, m % 12 + 1, 1)


def resolve_cutoff(a):
    """An exact --older-than date wins; otherwise --months (default 6) counts back from today."""
    if a.older_than:
        return parse_date(a.older_than)
    return cutoff_for_months(dt.date.today(), a.months)


def parse_date(s):
    for f in (FMT, "%Y-%m-%d"):
        try:
            return dt.datetime.strptime(s, f)
        except ValueError:
            pass
    raise SystemExit(f"bad date '{s}', use YYYY-MM-DD or {FMT}")


# ---------------------------------------------------------------- windows
def month_starts(lo, hi):
    cur = dt.datetime(lo.year, lo.month, 1)
    while cur < hi:
        yield cur
        cur = dt.datetime(cur.year + cur.month // 12, cur.month % 12 + 1, 1)


def build_windows(api, field, lo, hi, max_docs, workers):
    """Month windows, bisected until each holds <= max_docs docs (or is 1 hour wide)."""
    def count(a, b):
        return api.search(rng(field, a, b), 0)["total_hits"]

    def split(a, b):
        n = count(a, b)
        if n == 0:
            return []
        if n <= max_docs or (b - a) <= dt.timedelta(hours=1):
            return [(a, b, n)]
        mid = a + (b - a) / 2
        mid = mid.replace(microsecond=0)
        return split(a, mid) + split(mid, b)

    months = list(month_starts(lo, hi))
    edges = [(max(m, lo), min(dt.datetime(m.year + m.month // 12, m.month % 12 + 1, 1), hi)) for m in months]
    out = []
    with ThreadPoolExecutor(workers) as ex:
        for res in ex.map(lambda e: split(*e), edges):
            out.extend(res)
    return sorted(out)


# ---------------------------------------------------------------- select
def fetch_window(api, field, a, b, expected, path, id_field):
    """Keyset walk of [a, b). Returns number of ids written."""
    tmp = path + ".part"
    got, cur, boundary = 0, a, set()
    with open(tmp, "w") as f:
        while True:
            d = api.search(rng(field, cur, b), PAGE, [id_field, field], {"field": field, "order": "asc"})
            hits = d["hits"]
            fresh = []
            for h in hits:
                i = h.get(id_field) or h.get("id")
                if i is None:
                    raise SystemExit(f"hit without '{id_field}': {h}")
                if i not in boundary:
                    fresh.append((i, h[field]))
            for i, _ in fresh:
                f.write(i + "\n")
            got += len(fresh)
            # total_hits counts index entries whose body is already gone, so a full page can
            # come back short; stats.fetched_docs is what the shards actually returned
            if d.get("stats", {}).get("fetched_docs", len(hits)) < PAGE:
                break
            if not hits:
                raise SystemExit(f"a full page of {PAGE} had no readable documents after {ts(cur)}")
            if any(isinstance(h[field], list) for h in hits):
                raise SystemExit(f"'{field}' is multi-valued; the keyset walk needs one value per document")
            last = hits[-1][field]
            if hits[0][field] == last:
                raise SystemExit(f"{PAGE} documents share first_seen {last}; cannot page past them")
            cur = parse(last)
            boundary = {h.get(id_field) or h.get("id") for h in hits if h[field] == last}
    os.replace(tmp, path)
    return got


def cmd_select(a, api):
    field = a.field
    require_date_field(api, field)
    cutoff = resolve_cutoff(a)
    if a.newer_than:
        lo = parse_date(a.newer_than)
    else:
        # floor: epoch (1970-01-01) is a "no value" sentinel in fields like last_seen, not a real date
        d = api.search(rng(field, FLOOR, cutoff), 1, [field], {"field": field, "order": "asc"})
        if not d["hits"]:
            print("nothing older than the cutoff")
            return 0
        lo = parse(d["hits"][0][field]).replace(minute=0, second=0)
        print(f"lower bound {ts(lo)} (values before {ts(FLOOR)} are treated as placeholders and never selected; "
              "use --newer-than to change)", flush=True)
    total = api.search(rng(field, lo, cutoff), 0)["total_hits"]
    print(f"{field} in [{ts(lo)}, {ts(cutoff)}): {total:,} documents", flush=True)
    if total == 0:
        return 0

    parts = a.output + ".parts"
    os.makedirs(parts, exist_ok=True)
    wins = build_windows(api, field, lo, cutoff, a.window_max_docs, a.workers)
    print(f"{len(wins)} windows (<= {a.window_max_docs:,} docs each), {a.workers} workers", flush=True)

    manifest_windows, bad, ghosts, written, t0 = [], [], {}, 0, time.time()

    def work(w):
        s, e, n = w
        base = os.path.join(parts, ts(s).replace(":", ""))
        marker = base + ".done"
        if os.path.exists(marker):
            return w, json.load(open(marker))["got"], True
        got = fetch_window(api, field, s, e, n, base + ".ids", a.id_field)
        json.dump({"expected": n, "got": got}, open(marker, "w"))
        return w, got, False

    with ThreadPoolExecutor(a.workers) as ex:
        futs = [ex.submit(work, w) for w in wins]
        for k, fu in enumerate(as_completed(futs), 1):
            (s, e, n), got, resumed = fu.result()
            written += got
            manifest_windows.append({"from": ts(s), "to": ts(e), "expected": n, "got": got})
            if got > n:
                bad.append((ts(s), n, got))
            elif got < n:
                ghosts[ts(s)] = n - got
            if k % 25 == 0 or k == len(wins):
                print(f"  {k}/{len(wins)} windows, {written:,} ids, {time.time() - t0:.0f}s", flush=True)

    if bad:
        print("\nWINDOW MISMATCH (more ids returned than counted, so documents arrived during the walk):", file=sys.stderr)
        for s, n, g in sorted(bad):
            print(f"  {s}: expected {n}, got {g}", file=sys.stderr)
        print("Re-run select (finished windows are kept; delete the "
              ".done file of a bad window to redo it). Final file NOT assembled.", file=sys.stderr)
        return 2

    seen = 0
    with open(a.output, "w") as out:
        for m in sorted(manifest_windows, key=lambda x: x["from"]):
            p = os.path.join(parts, m["from"].replace(":", "") + ".ids")
            with open(p) as f:
                for line in f:
                    out.write(line)
                    seen += 1
    manifest = {
        "created": ts(dt.datetime.utcnow()), "index": api.index, "field": field,
        "since": ts(lo), "before": ts(cutoff), "total_expected": total,
        "ids_written": seen, "index_entries_without_body": sum(ghosts.values()), "windows": sorted(manifest_windows, key=lambda x: x["from"]),
    }
    json.dump(manifest, open(a.output + ".manifest.json", "w"), indent=1)
    print(f"\n{seen:,} ids -> {a.output}\nmanifest -> {a.output}.manifest.json")
    if seen != total:
        print(f"note: {total - seen:,} counted entries had no document body (already-deleted docs the index "
              "has not purged yet); they have no id to delete and are not in the file", file=sys.stderr)
    return 0


# ---------------------------------------------------------------- delete
def batches(path, size, skip):
    with open(path) as f:
        batch, n = [], 0
        for line in f:
            i = line.strip()
            if not i:
                continue
            n += 1
            if n <= skip:
                continue
            batch.append(i)
            if len(batch) == size:
                yield batch
                batch = []
        if batch:
            yield batch


def cmd_delete(a, api):
    total = sum(1 for _ in open(a.ids_file))
    prog = a.ids_file + ".progress"
    done = int(open(prog).read()) if os.path.exists(prog) and not a.restart else 0
    print(f"{a.ids_file}: {total:,} ids, {done:,} already deleted, index '{api.index}' at {api.base}")
    if not a.execute:
        print("DRY RUN. Nothing sent. Inspect the file, then add --execute.")
        return 0

    def send(b):
        d = api.call("POST", f"/api/{api.index}/_bulk/delete", b)
        if d.get("errors") or d.get("items_deleted") != len(b):
            raise SystemExit(f"batch not fully applied: {json.dumps(d)[:500]}")
        return len(b)

    t0, sent = time.time(), 0
    gen = batches(a.ids_file, a.batch_size, done)
    if a.max_batches:
        import itertools
        gen = itertools.islice(gen, a.max_batches)
    with ThreadPoolExecutor(a.workers) as ex:
        # map() yields in submission order, so `done` only ever covers a contiguous prefix
        for k, n in enumerate(ex.map(send, gen), 1):
            done += n
            sent += n
            open(prog, "w").write(str(done))
            if k % 20 == 0:
                rate = sent / (time.time() - t0)
                print(f"  {done:,}/{total:,} deleted ({rate:,.0f} ids/s)", flush=True)
    print(f"deleted {sent:,} ids this run, {done:,}/{total:,} overall")
    if a.commit:
        api.call("POST", f"/_admin/index/{api.index}/commit")
        print("commit requested")
    if sent:
        print("the files on disk keep their size: to return the space, stop the node and run "
              "`cameodb compact -c <config>` (--dry-run first shows how much)")
    return 0


def cmd_count(a, api):
    require_date_field(api, a.field)
    cutoff = resolve_cutoff(a)
    n = api.search(f"{a.field}:[* TO {ts(cutoff)}}}", 0)["total_hits"]
    all_ = api.search("*", 0)["total_hits"]
    print(f"{a.field} < {ts(cutoff)}: {n:,} of {all_:,} documents ({n / max(all_, 1):.1%})")
    print("(content-query counts lag deletes until the next commit)")
    return 0


# ---------------------------------------------------------------- check
ID_BYTES = 65  # sha256 hex + newline


def cmd_check(a, api):
    results = []

    def report(level, what, detail=""):
        results.append(level)
        print(f"  [{level}] {what}" + (f": {detail}" if detail else ""), flush=True)

    def attempt(fn):
        try:
            return fn(), None
        except SystemExit as e:
            return None, str(e)

    v = sys.version_info
    if v < (3, 6):
        report("FAIL", "python", f"{v.major}.{v.minor} is too old, need 3.6+")
    elif v < (3, 8):
        report("WARN", "python", f"{v.major}.{v.minor}; tested on 3.9, 3.8+ recommended")
    else:
        report("PASS", "python", f"{v.major}.{v.minor}.{v.micro}")
    report("PASS", "stdlib modules", "urllib, concurrent.futures, argparse, json")

    print(f"  server {api.base}, index '{api.index}', field '{a.field}'")
    report("PASS" if api.headers.get("Authorization") else "WARN", "CAMEODB_API_KEY",
           "set" if api.headers.get("Authorization") else "not set; fine only if the server has auth disabled")

    h, err = attempt(lambda: api.call("GET", "/_cluster/health", retries=0))
    if err:
        report("FAIL", "server reachable", err.splitlines()[0])
        print("\nCannot continue without the server.")
        return 1
    report("PASS", "server reachable", f"status {h.get('status', '?')}")

    cfg, err = attempt(lambda: api.call("GET", f"/api/{api.index}/_config", retries=0))
    if err:
        report("FAIL", f"read access to index '{api.index}'", err.splitlines()[0])
        return 1
    report("PASS", f"read access to index '{api.index}'", f"{cfg.get('field_count', len(cfg['fields']))} fields")

    ok = sorted(f["name"] for f in cfg["fields"] if f["type"] == "date" and f.get("fast") and f.get("indexed"))
    if a.field in ok:
        report("PASS", f"field '{a.field}'", "indexed fast date field")
    else:
        report("FAIL", f"field '{a.field}'", f"not an indexed fast date field; candidates: {', '.join(ok)}")

    cutoff = resolve_cutoff(a)
    if a.field not in ok:
        report("SKIP", "search and disk", "needs a valid field")
        return 1
    total, err = attempt(lambda: api.search("*", 0)["total_hits"])
    old, err2 = attempt(lambda: api.search(f"{a.field}:[* TO {ts(cutoff)}}}", 0)["total_hits"])
    if err or err2:
        report("FAIL", "search", (err or err2).splitlines()[0])
    else:
        report("PASS", "search", f"{total:,} documents; {old:,} older than {ts(cutoff)} ({old / max(total, 1):.1%})")
        path = os.path.abspath(a.path)
        try:
            free = shutil.disk_usage(path).free
            need = old * ID_BYTES * 2  # assembled file plus the per-window parts kept beside it
            lvl = "PASS" if free > need else "FAIL"
            report(lvl, f"disk at {path}", f"{free / 1e9:.1f} GB free, a full select needs ~{need / 1e9:.1f} GB "
                   f"(ids file + .parts; less if selected in slices)")
        except OSError as e:
            report("FAIL", f"disk at {path}", str(e))

    if a.probe_write:
        probe = "0" * 64
        d, err = attempt(lambda: api.call("POST", f"/api/{api.index}/_bulk/delete", [probe], retries=0))
        if err:
            report("FAIL", "write role (bulk delete of a nonexistent id)", err.splitlines()[0])
        else:
            report("PASS", "write role (bulk delete of a nonexistent id)", f"items_deleted={d.get('items_deleted')}")
    else:
        report("SKIP", "write role", "add --probe-write (deletes one nonexistent id)")
    if a.probe_admin:
        d, err = attempt(lambda: api.call("POST", f"/_admin/index/{api.index}/commit", retries=0))
        report("FAIL" if err else "PASS", "admin role (index commit, needed for delete --commit)",
               err.splitlines()[0] if err else "commit accepted")
    else:
        report("SKIP", "admin role", "add --probe-admin (requests an index commit)")

    bad = results.count("FAIL")
    print(f"\n{results.count('PASS')} passed, {results.count('WARN')} warnings, {bad} failed")
    return 1 if bad else 0


# ---------------------------------------------------------------- main
def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--url", default=os.environ.get("CAMEODB_URL", "http://alt-dbtest:9480"))
    p.add_argument("--index", default="searchdata")
    p.add_argument("--field", default="first_seen", help="date field the cutoff applies to")
    p.add_argument("--timeout", type=int, default=120)
    sub = p.add_subparsers(dest="cmd", required=True)

    def cutoff_args(sp):
        g = sp.add_mutually_exclusive_group()
        g.add_argument("--older-than", metavar="DATE",
                       help="exact cutoff, YYYY-MM-DD or YYYY-MM-DDTHH:MM:SSZ; documents strictly older are selected")
        g.add_argument("--months", type=int, default=DEFAULT_MONTHS,
                       help=f"retain this many previous months plus the current one (default {DEFAULT_MONTHS}); "
                            "the cutoff is the first day of the oldest retained month")

    sp = sub.add_parser("select", help="write the id list of documents older than the cutoff")
    cutoff_args(sp)
    sp.add_argument("--output", required=True, help="id file to write, one id per line")
    sp.add_argument("--newer-than", metavar="DATE",
                    help="lower bound, inclusive, to select in slices (e.g. one year at a time)")
    sp.add_argument("--id-field", default="sha256", help="key as hits report it (shadow field of the index)")
    sp.add_argument("--workers", type=int, default=6)
    sp.add_argument("--window-max-docs", type=int, default=500000,
                    help="split time windows until each holds at most this many documents; "
                         "only sets the unit of parallel work and resume, selects nothing less")
    sp.set_defaults(fn=cmd_select)

    sp = sub.add_parser("delete", help="bulk-delete the ids in a file")
    sp.add_argument("ids_file")
    sp.add_argument("--batch-size", type=int, default=5000)
    sp.add_argument("--workers", type=int, default=2)
    sp.add_argument("--max-batches", type=int, help="stop after N batches (trial run)")
    sp.add_argument("--commit", action="store_true", help="POST /_admin/index/{index}/commit at the end")
    sp.add_argument("--restart", action="store_true", help="ignore the .progress file")
    sp.add_argument("--execute", action="store_true", help="actually delete; without it this is a dry run")
    sp.set_defaults(fn=cmd_delete)

    sp = sub.add_parser("count", help="documents older than the cutoff")
    cutoff_args(sp)
    sp.set_defaults(fn=cmd_count)

    sp = sub.add_parser("check", help="verify the setup (Python, connection, key, index, field, disk)")
    cutoff_args(sp)
    sp.add_argument("--path", default=".", help="directory the id file will be written to (disk space check)")
    sp.add_argument("--probe-write", action="store_true",
                    help="test the write role by bulk-deleting one nonexistent id (idempotent)")
    sp.add_argument("--probe-admin", action="store_true",
                    help="test the admin role by requesting an index commit")
    sp.set_defaults(fn=cmd_check)

    sp = sub.add_parser("fields", help="list candidate date fields with counts")
    cutoff_args(sp)
    sp.add_argument("--workers", type=int, default=6)
    sp.set_defaults(fn=cmd_fields)

    a = p.parse_args()
    api = Api(a.url, a.index, os.environ.get("CAMEODB_API_KEY"), a.timeout)
    sys.exit(a.fn(a, api))


if __name__ == "__main__":
    main()
