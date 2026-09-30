"""Per-phenomenon test sets from hrLex (evaluation only; hrLex is CC BY-SA 4.0).

Each category is a set of (form, lemma, weight) pairs where getting the form to the lemma's term
exercises one Croatian difficulty. Weights are hrLex corpus frequencies.
usage: categories.py hrLex_v1.3.gz OUT_DIR
"""
import collections, gzip, re, sys

WORD = re.compile(r'^[a-zčćđšž]+$')
C = 'bcčćdđfghjklmnprsštvzž'
SIBIL = {'k': 'c', 'g': 'z', 'h': 's'}
PALAT = {'k': 'č', 'g': 'ž', 'h': 'š'}
SUPPLETIVE = {'čovjek', 'dijete', 'dobar', 'zao', 'velik', 'malen', 'mali', 'loš', 'dug', 'kratak'}
ISTRIA = ['buje', 'buzet', 'labin', 'novigrad', 'pazin', 'poreč', 'pula', 'rovinj', 'umag', 'vodnjan',
          'bale', 'barban', 'brtonigla', 'cerovlje', 'fažana', 'funtana', 'gračišće', 'grožnjan',
          'kanfanar', 'karojba', 'kršan', 'lanišće', 'ližnjan', 'lupoglav', 'marčana', 'medulin',
          'motovun', 'oprtalj', 'pićan', 'raša', 'svetvinčenat', 'tinjan', 'višnjan', 'vižinada',
          'vrsar', 'žminj', 'istra']
VERB_FORMS = {'n': 'infinitive', 'r': 'present', 'a': 'aorist', 'e': 'imperfect', 'm': 'imperative', 'p': 'l-participle'}

cats = collections.defaultdict(collections.Counter)
for line in gzip.open(sys.argv[1], 'rt', encoding='utf-8'):
    f = line.rstrip('\n').split('\t')
    if len(f) < 7 or int(f[6]) <= 0: continue
    form, lemma, msd, upos, feats, n = f[0].lower(), f[1].lower(), f[2], f[4], f[3], int(f[6])
    if not (WORD.match(form) and WORD.match(lemma)): continue
    key = (form, lemma, upos)
    if upos in ('NOUN', 'ADJ') and re.search(f'[{C}]a[kcnlr]$', lemma) and form != lemma \
            and form.startswith(lemma[:-2] + lemma[-1]):
        cats['fleeting_a'][key] += n
    if upos in ('NOUN', 'ADJ') and len(lemma) > 3:
        stem = lemma[:-1] if lemma[-1] in 'aeio' else lemma
        if stem and stem[-1] in SIBIL and form.startswith(stem[:-1] + SIBIL[stem[-1]]):
            cats['sibilarization'][key] += n
    if upos in ('NOUN', 'VERB') and len(lemma) > 3:
        stem = re.sub(r'(ti|ći|a|e|o|i)$', '', lemma)
        if stem and stem[-1] in PALAT and form.startswith(stem[:-1] + PALAT[stem[-1]]):
            cats['palatalization'][key] += n
    i = lemma.find('ije')
    if i > 0 and form[:i] == lemma[:i] and not form[i:].startswith('ije'):
        cats['jat_alternation'][key] += n
    if upos == 'VERB' and msd.startswith('Vm') and len(msd) > 2 and msd[2] in VERB_FORMS:
        cats['verb_' + VERB_FORMS[msd[2]]][key] += n
    if upos == 'ADJ' and re.search(r'(ti|ći)$', lemma):
        cats['passive_participle'][key] += n
    if upos == 'ADJ' and ('Degree=comparative' in feats or 'Degree=superlative' in feats):
        cats['adj_comparison'][key] += n
    if lemma in SUPPLETIVE and form[:3] != lemma[:3]:
        cats['suppletion'][key] += n
    if upos == 'PROPN' and lemma in ISTRIA:
        cats['istria_places'][key] += n

for name, agg in sorted(cats.items()):
    with open(f'{sys.argv[2]}/cat_{name}.tsv', 'w', encoding='utf-8') as o:
        for (f, l, p), n in agg.items(): o.write(f'{f}\t{l}\t{p}\t{n}\n')
    print(f'{name:22} {len(agg):7} pairs {sum(agg.values()):12} tokens', file=sys.stderr)
