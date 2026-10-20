#!/usr/bin/env python3
"""Profile a CameoDB index so its descriptions can be written from the data rather than from memory.

The search API has no aggregations, so everything here is a search: `limit 0` counts for presence and
for each value of a low-cardinality field, `limit 1` sorted searches for a range, and a sample of rows
spread over a stratify field (a row kind) and a spread field (usually a year) for what only values show
— examples, lengths, whether a field holds lists. Counts are exact; everything taken from the sample is
marked as such.

    python3 profile_index.py --index narodne_novine --stratify unit_type --spread year \
        --json profile.json > profile.md

Presence is a range from the lowest term of the field's type, because `field:*` and `field:[* TO *]`
are refused as presence tests. On a text field the range starts at "0", so a value whose every term
sorts below it is missed; json, bytes, ip and facet fields have no range at all and are profiled from the
sample only. Stdlib only.
"""
import argparse, json, sys, time, urllib.error, urllib.request
from collections import Counter

PRESENCE = {
    'string': '["" TO *]',
    'text': '[0 TO *]',
    'i64': '[-9223372036854775808 TO *]',
    'u64': '[0 TO *]',
    'f64': '[-1e308 TO *]',
    'date': '[1677-09-21 TO *]',
}


class Node:
    def __init__(self, url, index, key=None):
        self.url, self.index, self.key, self.calls = url.rstrip('/'), index, key, 0

    def call(self, method, path, body=None):
        headers = {'Content-Type': 'application/json'}
        if self.key:
            headers['Authorization'] = f'Bearer {self.key}'
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(self.url + path, data=data, method=method, headers=headers)
        self.calls += 1
        try:
            with urllib.request.urlopen(req, timeout=120) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            return {'error': e.read().decode(errors='replace')[:300], 'status': e.code}

    def search(self, query, limit=0, fields=None, sort=None, array=None):
        body = {'query': query, 'limit': limit}
        if fields:
            body['fields'] = fields
        if sort:
            body['sort'] = {'field': sort[0], 'order': sort[1]}
        if array is not None:
            body['array'] = array
        return self.call('POST', f'/api/{self.index}/search', body)

    def count(self, query):
        r = self.search(query)
        return None if 'error' in r or r.get('_discarded_clauses') else r.get('total_hits')


def quote(value):
    return '"' + str(value).replace('\\', '\\\\').replace('"', '\\"') + '"'


def term(field, value):
    return f'{field}:{value}' if isinstance(value, (int, float)) else f'{field}:{quote(value)}'


def values_of(v):
    return v if isinstance(v, list) else [v]


def shorten(v, n=140):
    s = v if isinstance(v, str) else json.dumps(v, ensure_ascii=False)
    return s if len(s) <= n else s[:n] + '…'


def spread_buckets(node, field, query, ftype, max_buckets):
    """The spread field's values to sample over: each value when there are few, else equal ranges."""
    lo = node.search(query, 1, [field], (field, 'asc')).get('hits') or [{}]
    hi = node.search(query, 1, [field], (field, 'desc')).get('hits') or [{}]
    lo, hi = lo[0].get(field), hi[0].get(field)
    if lo is None or hi is None or ftype not in ('i64', 'u64'):
        return [], lo, hi
    if hi - lo + 1 <= max_buckets:
        return [(v, v) for v in range(lo, hi + 1)], lo, hi
    step = (hi - lo + 1) / max_buckets
    edges = [lo + round(i * step) for i in range(max_buckets)] + [hi + 1]
    return [(edges[i], edges[i + 1] - 1) for i in range(max_buckets)], lo, hi


def bucket_query(field, b):
    return f'{field}:{b[0]}' if b[0] == b[1] else f'{field}:[{b[0]} TO {b[1]}]'


def main():
    ap = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    ap.add_argument('--url', default='http://127.0.0.1:9480')
    ap.add_argument('--index', required=True)
    ap.add_argument('--key', help='API key, when the node has security on')
    ap.add_argument('--stratify', help='low-cardinality field whose values are the row kinds (e.g. unit_type)')
    ap.add_argument('--strata', help='comma-separated stratify values, when the sample may miss rare ones')
    ap.add_argument('--spread', help='numeric fast field to spread the sample and coverage over (e.g. year)')
    ap.add_argument('--per-cell', type=int, default=4, help='rows sampled per stratify value x spread bucket')
    ap.add_argument('--buckets', type=int, default=40, help='most spread buckets')
    ap.add_argument('--max-categories', type=int, default=40, help='most distinct values a field may show to be counted value by value')
    ap.add_argument('--json', dest='json_out', help='write the full profile here')
    args = ap.parse_args()

    node = Node(args.url, args.index, args.key)
    started = time.time()
    config = node.call('GET', f'/api/{args.index}/_config')
    if 'error' in config:
        sys.exit(f'{args.index}: {config["error"]}')
    fields = {f['name']: f for f in config['fields']}
    total = node.count('*')

    def presence(name):
        rng = PRESENCE.get(fields[name]['type'])
        return f'{name}:{rng}' if rng and fields[name].get('indexed') else None

    buckets, spread_lo, spread_hi = [], None, None
    if args.spread:
        buckets, spread_lo, spread_hi = spread_buckets(node, args.spread, presence(args.spread) or '*',
                                                       fields[args.spread]['type'], args.buckets)
    bucket_total = {b: node.count(bucket_query(args.spread, b)) for b in buckets}

    # Row kinds: the ones named, and what a look at every spread bucket sees, each counted.
    strata = {}
    if args.stratify:
        seen = Counter(args.strata.split(',') if args.strata else [])
        for b in buckets or [None]:
            q = bucket_query(args.spread, b) if b else '*'
            for h in node.search(q, 500, [args.stratify]).get('hits', []):
                seen.update(values_of(h.get(args.stratify)))
        for v in seen:
            if v is not None:
                strata[v] = node.count(term(args.stratify, v))

    # The sample: a few rows from every row kind in every spread bucket.
    sample, cells = [], [(term(args.stratify, s) if s is not None else None) for s in (strata or [None])]
    for sq in cells:
        for b in buckets or [None]:
            parts = [p for p in (sq, b and bucket_query(args.spread, b)) if p]
            hits = node.search(' AND '.join(parts) or '*', args.per_cell, array=50).get('hits', [])
            sample.extend(hits)
    for h in sample:
        for k in [k for k in h if k.startswith('_') or k == 'shard_id']:
            h.pop(k, None)

    profile = {'index': args.index, 'documents': total, 'description': config.get('description'),
               'default_fields': config.get('default_fields'), 'stratify': args.stratify, 'strata': strata,
               'spread': args.spread, 'spread_range': [spread_lo, spread_hi], 'sample_rows': len(sample), 'fields': {}}

    for name, f in fields.items():
        p = {'type': f['type'], 'tokenizer': f.get('tokenizer'), 'sortable': f.get('sortable'),
             'default_search': f.get('default_search'), 'description': f.get('description')}
        pq = presence(name)
        p['present'] = node.count(pq) if pq else None
        vals = [h[name] for h in sample if h.get(name) not in (None, '', [])]
        p['sample_present'] = len(vals)
        lists = [v for v in vals if isinstance(v, list)]
        if lists:
            p['list_rows'] = len(lists)
            p['list_len'] = {'avg': round(sum(map(len, lists)) / len(lists), 1), 'max': max(map(len, lists))}
        flat = [x for v in vals for x in values_of(v) if not isinstance(x, (dict, list))]
        freq = Counter(flat)
        p['sample_distinct'] = len(freq)
        p['examples'] = [shorten(v) for v, _ in freq.most_common(6)] if flat else [shorten(v) for v in vals[:3]]
        if f['type'] in ('text',) and flat:
            p['avg_chars'] = round(sum(len(str(x)) for x in flat) / len(flat))

        # Which row kinds carry it, and over which part of the spread.
        if pq and p['present']:
            if strata:
                p['by_stratum'] = {s: c for s in strata if (c := node.count(f'{pq} AND {term(args.stratify, s)}'))}
            if args.spread and name != args.spread:
                first = node.search(pq, 1, [args.spread], (args.spread, 'asc')).get('hits') or [{}]
                last = node.search(pq, 1, [args.spread], (args.spread, 'desc')).get('hits') or [{}]
                p['spread_first'], p['spread_last'] = first[0].get(args.spread), last[0].get(args.spread)
                if buckets and p['present'] < total:
                    p['by_spread'] = {bucket_query(args.spread, b).split(':', 1)[1]:
                                      round(node.count(f'{pq} AND {bucket_query(args.spread, b)}') / bucket_total[b], 3)
                                      for b in buckets if bucket_total[b]}
            if f.get('sortable') and f['type'] in ('i64', 'u64', 'f64', 'date'):
                lo = node.search(pq, 1, [name], (name, 'asc')).get('hits') or [{}]
                hi = node.search(pq, 1, [name], (name, 'desc')).get('hits') or [{}]
                p['min'], p['max'] = lo[0].get(name), hi[0].get(name)
            elif f['type'] in ('i64', 'u64', 'f64', 'date') and flat:
                p['sample_min'], p['sample_max'] = min(flat), max(flat)

        # A field showing few values is counted value by value; `coverage` says whether that is all of it.
        if (pq and p['present'] and f['type'] in ('string', 'i64', 'u64') and 0 < len(freq) <= args.max_categories
                and len(flat) >= 3 * len(freq)):
            counts = {v: node.count(term(name, v)) for v, _ in freq.most_common()}
            p['values'] = dict(sorted(counts.items(), key=lambda kv: -(kv[1] or 0)))
            p['values_coverage'] = round(sum(c or 0 for c in counts.values()) / p['present'], 3)
        profile['fields'][name] = p

    profile['queries'] = node.calls
    profile['seconds'] = round(time.time() - started, 1)
    if args.json_out:
        with open(args.json_out, 'w', encoding='utf-8') as fh:
            json.dump(profile, fh, ensure_ascii=False, indent=1)
    print(report(profile))


BANDS = ((0.95, 'all'), (0.5, 'most'), (0.05, 'some'), (0.0001, 'few'), (0, 'none'))


def bands(shares):
    """Runs of spread buckets by how much of their rows carry the field: `1990–2014 few, 2015–2026 all`."""
    runs = []
    for k, share in shares.items():
        band = next(name for floor, name in BANDS if share >= floor)
        if runs and runs[-1][2] == band:
            runs[-1][1] = k
        else:
            runs.append([k, k, band])
    return ', '.join(f'{a}–{b} {band}' if a != b else f'{a} {band}' for a, b, band in runs)


def report(pr):
    out = [f"# {pr['index']}: {pr['documents']:,} documents, {len(pr['fields'])} fields",
           f"sample {pr['sample_rows']} rows, {pr['queries']} queries, {pr['seconds']}s",
           f"description ({len(pr['description'] or '')}/512): {pr['description'] or '—'}"]
    if pr['strata']:
        out.append(f"{pr['stratify']}: " + ', '.join(f'{k} {v:,}' for k, v in sorted(pr['strata'].items(), key=lambda kv: -kv[1])))
    if pr['spread']:
        out.append(f"{pr['spread']}: {pr['spread_range'][0]}–{pr['spread_range'][1]}")
    for name, p in pr['fields'].items():
        pres = f"{p['present']:,} ({p['present'] / pr['documents']:.1%})" if p['present'] is not None else f"sample {p['sample_present']}/{pr['sample_rows']}"
        out.append(f"\n## {name}  [{p['type']}{'/' + p['tokenizer'] if p['tokenizer'] else ''}{', sortable' if p['sortable'] else ''}]  present {pres}")
        out.append(f"desc ({len(p['description'] or '')}/200): {p['description'] or '—'}")
        if p.get('by_stratum'):
            out.append('rows: ' + ', '.join(f"{k} {v:,}/{pr['strata'][k]:,}" for k, v in sorted(p['by_stratum'].items(), key=lambda kv: -kv[1])))
        if p.get('spread_first') is not None:
            line = f"{pr['spread']}: {p['spread_first']}–{p['spread_last']}"
            if p.get('by_spread'):
                line += '  share of rows: ' + bands(p['by_spread'])
            out.append(line)
        for k in ('min', 'sample_min'):
            if k in p:
                out.append(f"range{' (sample)' if k.startswith('sample') else ''}: {p[k]} – {p[k.replace('min', 'max')]}")
        if 'list_rows' in p:
            out.append(f"lists in {p['list_rows']}/{p['sample_present']} sampled rows, avg {p['list_len']['avg']}, max {p['list_len']['max']}")
        if 'values' in p:
            out.append(f"values (coverage {p['values_coverage']:.0%}): " + ', '.join(f'{shorten(k, 60)} {v:,}' for k, v in p['values'].items() if v is not None))
        elif p['examples']:
            out.append(f"examples ({p['sample_distinct']} distinct in sample): " + ' | '.join(p['examples']))
        if 'avg_chars' in p:
            out.append(f"avg {p['avg_chars']} chars")
    return '\n'.join(out)


if __name__ == '__main__':
    main()
