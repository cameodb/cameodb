"""One pass over the local CLASSLA-web.hr 2.0 vertical file (CC0) -> (form, lemma, upos) counts.

The file is ordered by website, so a prefix is a few sites, not a sample. Instead every document
gets a stable value u = md5(text id) in [0, 1), and bucket b = floor(u * 60) for u < 10/60. Sample k
is buckets 0..k-1: nested (sample 3 contains sample 2), spread over the whole corpus, and each
bucket is ~1/60 of it. Written as agg_s<k>.tsv, cumulative, for k = 1..10.

Also counted, separately, whatever their bucket:
  agg_legal.tsv     documents whose genre label is Legal
  agg_localgov.tsv  Croatian government and Istrian local-government sites
A truncated input is an error: tar's exit status is checked, never assumed.
usage: full_pass.py CORPUS.vert.tar.gz OUT_DIR
"""
import array, collections, hashlib, json, re, subprocess, sys, time

BUCKETS = 10
PARTS = 60
ATTR = re.compile(r'(\w+)="([^"]*)"')
LOCALGOV = re.compile(r'(^|\.)(gov\.hr|istra-istria\.hr|pula\.hr|porec\.hr|rovinj\.hr|rovinj-rovigno\.hr|umag\.hr|'
                      r'pazin\.hr|labin\.hr|buzet\.hr|novigrad\.hr|buje\.hr|vodnjan\.hr|fazana\.hr|medulin\.hr|'
                      r'vrsar\.hr|funtana\.hr|tar-vabriga\.hr|brtonigla\.hr|grisignana\.hr|groznjan\.hr|motovun\.hr|'
                      r'oprtalj\.hr|visnjan\.hr|vizinada\.hr|kanfanar\.hr|zminj\.hr|tinjan\.hr|svetvincenat\.hr|'
                      r'barban\.hr|marcana\.hr|liznjan\.hr|kras\.hr|kaštelir-labinci\.hr|nn\.hr|sabor\.hr|vlada\.hr)$')

corpus, out = sys.argv[1], sys.argv[2]
counts = {}                                   # key -> array of per-bucket counts
legal = collections.Counter(); localgov = collections.Counter()
stats = {'texts': 0, 'texts_sampled': [0] * BUCKETS, 'tokens_sampled': [0] * BUCKETS,
         'tokens_total': 0, 'tokens_legal': 0, 'tokens_localgov': 0,
         'genre_sampled': collections.Counter(), 'domains_sampled': collections.Counter()}
bucket = None; is_legal = is_gov = False
t0 = time.time()
tar = subprocess.Popen(['tar', '-xzOf', corpus], stdout=subprocess.PIPE, text=True, encoding='utf-8', errors='replace')
for line in tar.stdout:
    if line[0] == '<':
        if line.startswith('<text '):
            a = dict(ATTR.findall(line))
            stats['texts'] += 1
            u = int(hashlib.md5(a.get('id', '').encode()).hexdigest()[:8], 16) / 2**32
            b = int(u * PARTS)
            bucket = b if b < BUCKETS else None
            is_legal = 'Legal' in a.get('genre', '')
            is_gov = bool(LOCALGOV.search(a.get('domain', '')))
            if bucket is not None:
                stats['texts_sampled'][bucket] += 1
                stats['genre_sampled'][a.get('genre', '?')] += 1
                stats['domains_sampled'][a.get('domain', '?')] += 1
            if stats['texts'] % 500_000 == 0:
                print(f'{stats["texts"]} texts, {stats["tokens_total"] / 1e6:.0f}M tokens, '
                      f'{time.time() - t0:.0f}s', file=sys.stderr, flush=True)
        continue
    stats['tokens_total'] += 1
    if bucket is None and not is_legal and not is_gov:
        continue
    f = line.rstrip('\n').split('\t')
    if len(f) < 4:
        continue
    lemma = f[1].rsplit('-', 1)[0] if '-' in f[1] else f[1]
    key = f'{f[0]}\t{lemma}\t{f[3]}'
    if bucket is not None:
        c = counts.get(key)
        if c is None:
            c = counts[key] = array.array('I', bytes(4 * BUCKETS))
        c[bucket] += 1
        stats['tokens_sampled'][bucket] += 1
    if is_legal:
        legal[key] += 1; stats['tokens_legal'] += 1
    if is_gov:
        localgov[key] += 1; stats['tokens_localgov'] += 1
rc = tar.wait()
if rc != 0:
    sys.exit(f'tar exited {rc}: the corpus file is truncated or corrupt; nothing written')

for k in range(1, BUCKETS + 1):
    with open(f'{out}/agg_s{k}.tsv', 'w', encoding='utf-8') as o:
        for key, c in counts.items():
            n = sum(c[:k])
            if n: o.write(f'{key}\t{n}\n')
for name, ctr in (('legal', legal), ('localgov', localgov)):
    with open(f'{out}/agg_{name}.tsv', 'w', encoding='utf-8') as o:
        for key, n in ctr.items(): o.write(f'{key}\t{n}\n')
stats['genre_sampled'] = dict(stats['genre_sampled'].most_common())
stats['domains_sampled'] = dict(stats['domains_sampled'].most_common(300))
stats['seconds'] = time.time() - t0
json.dump(stats, open(f'{out}/stats.json', 'w'), indent=1)
print(f'done: {stats["texts"]} texts, {stats["tokens_total"]} tokens, sampled '
      f'{sum(stats["tokens_sampled"])}, legal {stats["tokens_legal"]}, localgov {stats["tokens_localgov"]}',
      file=sys.stderr)
