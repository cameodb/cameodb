#!/usr/bin/env python3
"""Override candidates from a vocabulary of the documents to be indexed.

  split.txt    families split by the analyzer: word pairs that differ only by an inflectional ending
               (Vodnjan / Vodnjana) but get different terms; proper nouns first
  junk.txt     frequent words whose lemma is not itself a word the corpus knows (žminjkk, brtonigl)
  guessed.txt  frequent words the lexicon does not hold, with what the guesser makes of them
  terms.txt    the gazette's key terms: every form found, and the ones that miss the family

    HR_LEXICON_EXTRACT=... python3 tools/hr-lexicon/review/review.py VOCAB.tsv

VOCAB.tsv: word (lowercase), count, capitalized count, example context — one line per word.
Reports go to review/out. Only words seen repeatedly are worth an override: a word seen once
may be a typing mistake.
"""
import collections, os, re, sys
from model import load

OUT = os.path.join(os.path.dirname(__file__), 'out')
WORD = re.compile(r'^[a-zčćđšž]{2,}$')
ENDINGS = ['', 'a', 'e', 'i', 'o', 'u', 'om', 'em', 'oj', 'ama', 'ima', 'ovi', 'ove', 'ova', 'ovima', 'evi', 'eve']
TERMS = ('odluka rješenje zaključak pravilnik program plan proračun statut poslovnik izvješće izvještaj ispravak '
         'sporazum oglas natječaj ugovor zakon članak stavak točka alineja prilog županija skupština župan '
         'vijeće općina grad odbor odjel povjerenstvo komisija ured tijelo načelnik gradonačelnik vijećnik '
         'naknada komunalni prostorni uređenje sredstvo rashod prihod iznos godina razdoblje djelatnost '
         'ustanova udruga škola bolnica zdravstvo stipendija nabava imenovanje razrješenje suglasnost '
         'izmjena dopuna provedba financiranje korisnik vlasništvo nekretnina zemljište cesta luka more '
         'obala otok turizam poljoprivreda gospodarstvo kultura sport mladež').split()

plain, _ = load()
vocab = []
os.makedirs(OUT, exist_ok=True)
for line in open(sys.argv[1], encoding='utf-8'):
    w, n, cap, ctx = line.rstrip('\n').split('\t', 3)
    if WORD.match(w): vocab.append((w, int(n), int(cap), ctx))
count = {w: n for w, n, _, _ in vocab}
capr = {w: c / n for w, n, c, _ in vocab}
ctxs = {w: x for w, _, _, x in vocab}
out = {w: plain(w) for w, *_ in vocab}

# split families: words sharing a stem, differing by an inflectional ending, with different outputs
by_stem = collections.defaultdict(set)
for w in count:
    for e in ENDINGS:
        if w.endswith(e) and len(w) - len(e) >= 3:
            by_stem[w[:len(w) - len(e)] if e else w].add(w)
families = {}
for stem, ws in by_stem.items():
    if len(ws) < 2 or len({out[w] for w in ws}) < 2: continue
    key = frozenset(ws)
    if key in families: continue
    families[key] = stem
rows = []
for ws, stem in families.items():
    ws = sorted(ws, key=lambda w: -count[w])
    total = sum(count[w] for w in ws)
    terms = collections.Counter()
    for w in ws: terms[out[w]] += count[w]
    minority = total - terms.most_common(1)[0][1]
    proper = sum(count[w] * capr[w] for w in ws) / total
    if minority >= 3: rows.append((proper >= 0.5, minority, stem, ws, total, proper))
rows.sort(key=lambda r: (not r[0], -r[1]))
with open(os.path.join(OUT, 'split.txt'), 'w', encoding='utf-8') as o:
    o.write('# stem  [proper-noun share]  tokens on minority terms / family tokens\n#   form (count) -> term\n')
    for proper, minority, stem, ws, total, pr in rows:
        o.write(f'{stem}  [{pr:.0%} capitalized]  {minority}/{total}\n')
        for w in ws: o.write(f'    {w} ({count[w]}) -> {out[w]}\n')

# junk lemmas: the lemma of a known word is not itself a known word
with open(os.path.join(OUT, 'junk.txt'), 'w', encoding='utf-8') as o:
    o.write('# count  form -> lemma (lemma never seen as a word)   context\n')
    for w, n, _, ctx in vocab:
        if n >= 3 and w in plain.lex and out[w] != w and out[w] not in plain.lex:
            o.write(f'{n:>7}  {w} -> {out[w]}    | {ctx}\n')

# guessed: frequent words the lexicon lacks
with open(os.path.join(OUT, 'guessed.txt'), 'w', encoding='utf-8') as o:
    o.write('# count  capitalized share  form -> guessed term   context\n')
    for w, n, _, ctx in vocab:
        if n >= 10 and w not in plain.lex:
            o.write(f'{n:>7}  {capr[w]:>4.0%}  {w} -> {out[w]}    | {ctx}\n')

# key terms: all gazette forms by term, and near forms that land elsewhere
with open(os.path.join(OUT, 'terms.txt'), 'w', encoding='utf-8') as o:
    for t in TERMS:
        fam = sorted((w for w in count if out[w] == t), key=lambda w: -count[w])
        stem = t[:-1] if t[-1] in 'aeio' else t
        near = sorted((w for w in count if w.startswith(stem[:max(3, len(stem) - 1)]) and len(w) <= len(t) + 4
                       and out[w] != t and any(w == stem + e or w == t + e for e in ENDINGS)), key=lambda w: -count[w])
        o.write(f'{t}: {sum(count[w] for w in fam):,} tokens, {len(fam)} forms: {", ".join(fam[:14])}\n')
        if near: o.write('    elsewhere: ' + ', '.join(f'{w} ({count[w]}) -> {out[w]}' for w in near[:10]) + '\n')

for f in ('split', 'junk', 'guessed', 'terms'):
    lines = [l for l in open(os.path.join(OUT, f + '.txt'), encoding='utf-8') if l.strip() and not l.startswith('#')]
    n = len(lines) if f in ('junk', 'guessed') else sum(1 for l in lines if not l.startswith('    '))
    print(f'{f}.txt: {n} entries')
