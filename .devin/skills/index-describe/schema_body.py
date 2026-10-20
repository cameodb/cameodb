#!/usr/bin/env python3
"""Build a `PUT /api/{index}/_config` body: the index's live schema with new descriptions.

    python3 schema_body.py --index narodne_novine --descriptions desc.json --out body.json
    python3 schema_body.py --index narodne_novine --descriptions desc.json --out body.json --apply

`desc.json` is {"description": "...", "fields": {"<field>": "...", ...}}; a field it leaves out keeps the
description it has, and "" clears one. Everything else — types, tokenizers, fast columns, default fields,
id fields — is copied from `GET /_config`, so the body changes descriptions and nothing else. The live
schema is saved first, complete as GET reports it (<index>.v<version>.<thumbprint>.json), and the new one after
--apply, so every version the node held is on record. A
description is not a built column: a node accepts the body on an index holding documents and rebuilds
nothing. The body is checked before it is written: lengths (index 512, field 200 characters), names the
index does not have, and the old and new text of every change. `--apply` sends it.
"""
import argparse, json, os, sys, urllib.error, urllib.request

MAX_INDEX, MAX_FIELD = 512, 200


def call(url, method, path, key=None, body=None):
    headers = {'Content-Type': 'application/json'}
    if key:
        headers['Authorization'] = f'Bearer {key}'
    req = urllib.request.Request(url.rstrip('/') + path, method=method, headers=headers,
                                 data=json.dumps(body).encode() if body is not None else None)
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode(errors='replace')


def save_version(live, directory, index):
    """The schema exactly as GET /_config reports it, as <index>.v<version>.<thumbprint>.json; an existing file is kept."""
    path = os.path.join(directory, f"{index}.v{live.get('version')}.{live.get('thumbprint', 'none')}.json")
    os.makedirs(directory, exist_ok=True)
    if not os.path.exists(path):
        with open(path, 'w', encoding='utf-8') as fh:
            json.dump(live, fh, ensure_ascii=False, indent=4)
            fh.write('\n')
    return path


def put_field(f, description):
    """A field as `PUT` takes it, from the shape `GET` reports it in."""
    out = {'name': f['name'], 'field_type': f['type'], 'indexed': f.get('indexed', True),
           'stored': f.get('stored', False), 'fast': f.get('fast', False), 'is_shadow': f.get('shadow', False)}
    for k in ('tokenizer', 'index_record_option'):
        if f.get(k):
            out[k] = f[k]
    if description:
        out['description'] = description
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    ap.add_argument('--url', default='http://127.0.0.1:9480')
    ap.add_argument('--index', required=True)
    ap.add_argument('--key')
    ap.add_argument('--descriptions', required=True)
    ap.add_argument('--out', required=True)
    ap.add_argument('--apply', action='store_true', help='PUT the body to the index')
    ap.add_argument('--history', help='where each version of the live schema is saved (default: beside --out)')
    args = ap.parse_args()

    status, live = call(args.url, 'GET', f'/api/{args.index}/_config', args.key)
    if status != 200:
        sys.exit(f'{args.index}: {status} {live}')
    with open(args.descriptions, encoding='utf-8') as fh:
        new = json.load(fh)
    fields = {f['name']: f for f in live['fields']}
    problems, changes = [], []

    unknown = sorted(set(new.get('fields', {})) - set(fields))
    problems += [f'{n}: the index has no such field' for n in unknown]
    index_desc = new.get('description', live.get('description'))
    if index_desc and len(index_desc) > MAX_INDEX:
        problems.append(f'index description: {len(index_desc)} characters, the limit is {MAX_INDEX}')
    if index_desc != live.get('description'):
        changes.append(('(index)', live.get('description'), index_desc))

    body_fields = {}
    for name, f in fields.items():
        desc = new.get('fields', {}).get(name, f.get('description'))
        if desc and len(desc) > MAX_FIELD:
            problems.append(f'{name}: {len(desc)} characters, the limit is {MAX_FIELD}')
        if (desc or None) != f.get('description'):
            changes.append((name, f.get('description'), desc))
        body_fields[name] = put_field(f, desc)

    body = {'description': index_desc, 'fields': body_fields}
    if live.get('default_fields'):
        body['default_fields'] = live['default_fields']
    if live.get('id_fields'):
        body['id_fields'] = live['id_fields']

    saved = save_version(live, args.history or os.path.dirname(os.path.abspath(args.out)), args.index)
    print(f'live schema saved as {saved}')

    for name, old, desc in changes:
        print(f'## {name}\n  - {old or "—"}\n  + {desc or "—"}' + (f'  ({len(desc)})' if desc else ''))
    undescribed = [n for n, f in body_fields.items() if not f.get('description')]
    print(f'\n{len(changes)} changed, {len(fields) - len(undescribed)}/{len(fields)} fields described'
          + (f'; none: {", ".join(undescribed)}' if undescribed else ''))
    if problems:
        print('\nnot written:\n' + '\n'.join(f'  {p}' for p in problems))
        sys.exit(1)
    with open(args.out, 'w', encoding='utf-8') as fh:
        json.dump(body, fh, ensure_ascii=False, indent=2)
    print(f'wrote {args.out}')
    if args.apply:
        status, resp = call(args.url, 'PUT', f'/api/{args.index}/_config', args.key, body)
        print(f'PUT {args.index}: {status} {json.dumps(resp, ensure_ascii=False) if isinstance(resp, dict) else resp}')
        if status != 200:
            sys.exit(1)
        _, after = call(args.url, 'GET', f'/api/{args.index}/_config', args.key)
        print(f'new schema saved as {save_version(after, args.history or os.path.dirname(os.path.abspath(args.out)), args.index)}')


if __name__ == '__main__':
    main()
