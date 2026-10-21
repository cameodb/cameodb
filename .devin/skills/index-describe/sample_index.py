#!/usr/bin/env python3
"""Profile a large CameoDB index from whole records instead of per-field counts.

On tens of millions of rows a presence count on a text field (`field:[0 TO *]`) walks every term of the
field and can run for minutes; `profile_index.py` runs one per field. This script asks only what stays cheap
at any size:

- term counts for each stratum (a row kind: `kind:invoice`, `status:archived`) and for each
  year of a fast date or integer spread field;
- a few ids per stratum x year, by a sorted `limit` search on the spread field;
- each sampled record read whole with `id:<key>` alone, answered from the key-value store;
- per fast numeric or date field, the real range (default value left out) and how many rows hold the
  default (0, 1970-01-01), all from sorted `limit 1` searches and term counts.

Per field the report gives the share of rows carrying it, weighted by how many rows each year holds (the
sample over-represents sparse years), the strata carrying it, the years it spans, shape (list, object
keys) and the common values.

    python3 sample_index.py --index <index> --spread created_at \
        --stratum invoice=kind:invoice --stratum archived=status:archived \
        --json sample.json > sample.md

Stdlib only.
"""
import argparse, json, sys, time, urllib.error, urllib.request
from collections import Counter, defaultdict

DEFAULT = {'date': '1970-01-01T00:00:00Z', 'i64': 0, 'u64': 0, 'f64': 0.0}


class Node:
    def __init__(self, url, index, key=None, timeout=60):
        self.url, self.index, self.key, self.timeout, self.calls = url.rstrip('/'), index, key, timeout, 0

    def call(self, method, path, body=None):
        headers = {'Content-Type': 'application/json'}
        if self.key:
            headers['Authorization'] = f'Bearer {self.key}'
        data = json.dumps(body).encode() if body is not None else None
        self.calls += 1
        try:
            req = urllib.request.Request(self.url + path, data=data, method=method, headers=headers)
            with urllib.request.urlopen(req, timeout=self.timeout) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            return {'error': e.read().decode(errors='replace')[:300]}
        except (urllib.error.URLError, TimeoutError, OSError) as e:
            return {'error': f'{type(e).__name__}: {e}'}

    def search(self, query, limit=0, fields=None, sort=None):
        body = {'query': query, 'limit': limit}
        if fields:
            body['fields'] = fields
        if sort:
            body['sort'] = {'field': sort[0], 'order': sort[1]}
        return self.call('POST', f'/api/{self.index}/search', body)

    def count(self, query):
        r = self.search(query)
        return None if 'error' in r or r.get('_discarded_clauses') else r.get('total_hits')


def empty(v):
    # 1970-01-01 is how an unset date is stored: it counts as no value.
    return v in (None, '', [], {}, DEFAULT['date']) or (isinstance(v, list) and all(empty(x) for x in v))


def flat(v):
    if isinstance(v, list):
        for x in v:
            yield from flat(x)
    else:
        yield v


def years(node, field, ftype):
    """The spread field's years, from its real minimum to its maximum."""
    real = f'{field}:[1970-01-02 TO *]' if ftype == 'date' else f'{field}:[1 TO *]'
    lo = (node.search(real, 1, [field], (field, 'asc')).get('hits') or [{}])[0].get(field)
    hi = (node.search(real, 1, [field], (field, 'desc')).get('hits') or [{}])[0].get(field)
    if lo is None or hi is None:
        sys.exit(f'{field}: no values to spread over')
    if ftype == 'date':
        return [(y, f'{field}:[{y}-01-01 TO {y + 1}-01-01}}') for y in range(int(lo[:4]), int(hi[:4]) + 1)]
    return [(y, f'{field}:{y}') for y in range(lo, hi + 1)]


def fast_range(node, name, ftype):
    real = {'date': f'{name}:[1970-01-02 TO *]', 'f64': f'{name}:[0.0001 TO *]'}.get(ftype, f'{name}:[1 TO *]')
    zero = f'{name}:[* TO 1970-01-02}}' if ftype == 'date' else f'{name}:0'
    lo = (node.search(real, 1, [name], (name, 'asc')).get('hits') or [{}])[0].get(name)
    hi = (node.search(real, 1, [name], (name, 'desc')).get('hits') or [{}])[0].get(name)
    # A list field sorts by one of its values; the hit's list may still hold the default.
    lo = min((x for x in lo if x != DEFAULT[ftype]), default=None) if isinstance(lo, list) else lo
    hi = max((x for x in hi if x != DEFAULT[ftype]), default=None) if isinstance(hi, list) else hi
    return {'real': node.count(real), 'default': node.count(zero), 'default_value': DEFAULT[ftype], 'min': lo, 'max': hi}


def main():
    ap = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    ap.add_argument('--url', default='http://127.0.0.1:9480')
    ap.add_argument('--index', required=True)
    ap.add_argument('--key', help='API key, when the node has security on')
    ap.add_argument('--spread', required=True, help='fast date or integer field to sample across, one bucket per year/value')
    ap.add_argument('--stratum', action='append', default=[], metavar='NAME=QUERY',
                    help='a row kind to sample on its own, e.g. invoice=kind:invoice (repeatable)')
    ap.add_argument('--per-year', type=int, default=3, help='rows per year for the whole index')
    ap.add_argument('--per-cell', type=int, default=1, help='rows per stratum x year')
    ap.add_argument('--timeout', type=int, default=60, help='seconds before one request is given up')
    ap.add_argument('--json', dest='json_out')
    args = ap.parse_args()

    node = Node(args.url, args.index, args.key, args.timeout)
    started = time.time()
    config = node.call('GET', f'/api/{args.index}/_config')
    if 'error' in config:
        sys.exit(f'{args.index}: {config["error"]}')
    fields = {f['name']: f for f in config['fields']}
    key_name = next((n for n, f in fields.items() if f.get('shadow')), 'id')
    total = node.count('*')
    spread = years(node, args.spread, fields[args.spread]['type'])
    per_year = {y: node.count(q) or 0 for y, q in spread}

    strata = {'all': '*'}
    strata.update(s.split('=', 1) for s in args.stratum)
    counts, picks = {}, defaultdict(set)
    for name, q in strata.items():
        counts[name] = node.count(q)
        if not counts[name]:
            continue
        k = args.per_year if name == 'all' else args.per_cell
        for y, yq in spread:
            if not per_year[y]:
                continue
            for h in node.search(f'({q}) AND {yq}', k, [key_name], (args.spread, 'desc')).get('hits') or []:
                if h.get(key_name) or h.get('id'):
                    picks[h.get(key_name) or h.get('id')].add((name, y))

    docs = []
    for key, cells in picks.items():
        r = node.search(f'id:{key}', 1)
        if r.get('hits'):
            d = r['hits'][0]
            d.pop('_score', None)
            d['_cells'] = sorted(cells)
            docs.append(d)

    by_stratum = defaultdict(list)
    all_by_year = defaultdict(list)
    for d in docs:
        for s, y in d['_cells']:
            by_stratum[s].append(d)
            if s == 'all':
                all_by_year[y].append(d)
    weight = sum(per_year[y] for y in all_by_year)

    profile = {'index': args.index, 'documents': total, 'key': key_name, 'spread': args.spread, 'per_year': per_year,
               'strata': {s: {'query': strata[s], 'count': counts.get(s), 'sampled': len(by_stratum[s])} for s in strata},
               'sample_rows': len(docs), 'fields': {}}
    for name, f in fields.items():
        k = key_name if name == 'id' else name
        have = [d for d in docs if not empty(d.get(k))]
        p = {'type': f['type'], 'tokenizer': f.get('tokenizer'), 'indexed': f.get('indexed'), 'fast': f.get('fast'),
             'description': f.get('description'), 'sampled': len(have)}
        if weight:
            p['share'] = round(sum(per_year[y] * sum(1 for d in ds if not empty(d.get(k))) / len(ds)
                                   for y, ds in all_by_year.items()) / weight, 4)
        p['strata'] = {s: f'{sum(1 for d in ds if not empty(d.get(k)))}/{len(ds)}' for s, ds in by_stratum.items()
                       if any(not empty(d.get(k)) for d in ds)}
        ys = sorted(y for d in have for s, y in d['_cells'])
        if ys:
            p['years'] = [ys[0], ys[-1]]
        p['shape'] = dict(Counter(type(d[k]).__name__ for d in have))
        dicts = [x for d in have for x in flat(d[k]) if isinstance(x, dict)]
        if dicts:
            p['object_keys'] = sorted({kk for x in dicts for kk in x})
        vals = Counter(json.dumps(x, ensure_ascii=False)[:100] for d in have for x in flat(d[k])
                       if not isinstance(x, dict) and not empty(x))
        p['distinct'] = len(vals)
        p['common'] = vals.most_common(8)
        if f.get('fast') and f.get('indexed') and f['type'] in DEFAULT:
            p['range'] = fast_range(node, name, f['type'])
        profile['fields'][name] = p

    profile['queries'], profile['seconds'] = node.calls, round(time.time() - started, 1)
    if args.json_out:
        with open(args.json_out, 'w', encoding='utf-8') as fh:
            json.dump(profile, fh, ensure_ascii=False, indent=1)
    print(report(profile))


def report(pr):
    out = [f"# {pr['index']}: {pr['documents']:,} documents, {len(pr['fields'])} fields, key {pr['key']}",
           f"sample {pr['sample_rows']} whole records, {pr['queries']} queries, {pr['seconds']}s",
           f"{pr['spread']} per year: " + ', '.join(f'{y} {n:,}' for y, n in pr['per_year'].items()),
           'strata: ' + ', '.join(f"{s} {v['count'] or 0:,} ({v['sampled']} sampled)" for s, v in pr['strata'].items())]
    for name, p in pr['fields'].items():
        share = f"{p['share']:.1%} of rows" if 'share' in p else ''
        flags = ''.join([f"/{p['tokenizer']}" if p['tokenizer'] else '', '' if p['indexed'] else ' NOT-INDEXED',
                         ' fast' if p['fast'] else ''])
        out.append(f"\n## {name} [{p['type']}{flags}] {share}, {p['sampled']}/{pr['sample_rows']} sampled"
                   + (f", years {p['years'][0]}–{p['years'][1]}" if 'years' in p else ''))
        if p['strata']:
            out.append('strata: ' + ' '.join(f'{s} {v}' for s, v in p['strata'].items()))
        if 'range' in p:
            r = p['range']
            if r['real'] is not None and pr['documents']:
                out[-1 if not p['strata'] else -2] += f", exactly {r['real'] / pr['documents']:.1%} set"
            out.append(f"range {r['min']} – {r['max']} over {r['real'] or 0:,} rows; default {r['default_value']} on {r['default'] or 0:,}")
        if p['shape']:
            out.append(f"shape {p['shape']}" + (f", objects {p['object_keys']}" if 'object_keys' in p else ''))
        if p['common']:
            out.append(f"common ({p['distinct']} distinct): " + ' | '.join(f'{v}×{n}' for v, n in p['common']))
    return '\n'.join(out)


if __name__ == '__main__':
    main()
