"""The Croatian analyzer's model: lexicon + participle rule + overrides + guesser.

build() reads the corpus extract and applies, in order:
  1. participle rule: a participle lemma (određen) becomes its verb (odrediti) when forms tagged
     ADJ are lemmatized both ways often enough (see participle_map)
  2. lemma overrides (overrides.tsv, kind "lemma")
  3. the most frequent lemma per form, keeping forms seen at least MIN_COUNT times
  4. form overrides (kind "form")
then trains the guesser on the result. The folded model (hr_stem_fold) is built the same way from
the pairs after steps 1-2, with forms and lemmas folded; its input at run time is folded first.
"""
import collections, gzip, re, unicodedata

from guesser import Guesser

MIN_COUNT = 2
PART_SHARE = 0.01
SKIP_POS = {'PUNCT', 'SYM', 'X'}
WORD = re.compile(r'^[a-zčćđšž]+$')


def fold(s):
    """Diacritics off, as tantivy's AsciiFoldingFilter does for Croatian letters (đ -> d)."""
    return ''.join(c for c in unicodedata.normalize('NFD', s.replace('đ', 'd').replace('Đ', 'D'))
                   if unicodedata.category(c) != 'Mn')


def read_overrides(path):
    lemma, form = {}, {}
    for n, line in enumerate(open(path, encoding='utf-8'), 1):
        if not line.strip() or line.startswith('#'):
            continue
        kind, a, b = line.rstrip('\n').split('\t')[:3]
        if kind not in ('lemma', 'form') or not (WORD.match(a) and WORD.match(b)):
            raise SystemExit(f'{path}:{n}: expected "lemma|form<TAB>from<TAB>to" in lowercase Croatian letters')
        (lemma if kind == 'lemma' else form)[a] = b
    return lemma, form


def participle_map(adj, share, lemma_total):
    """Participle lemma -> verb lemma (određen -> odrediti). Evidence: forms tagged ADJ that the
    corpus lemmatizes both ways. Guards, so only real participles move: the lemma looks like one
    (ends in -n or -t), the verb shares its stem (folded, up to the last four letters), and the paired
    evidence covers at least `share` of all the lemma's occurrences, whatever their part of speech.
    Without them, a few mistagged tokens turned the preposition "na" into a verb."""
    pair = collections.Counter()
    for f, c in adj.items():
        verbs = [(l, n) for l, n in c.items() if l.endswith(('ti', 'ći'))]
        for l, n in c.items():
            if l.endswith(('ti', 'ći')) or not l.endswith(('n', 't')) or len(l) < 4:
                continue
            k = max(3, len(l) - 4)
            for v, m in verbs:
                if fold(v[:k]) == fold(l[:k]):
                    pair[(l, v)] += min(n, m)
    best = {}
    for (pl, v), n in pair.items():
        if n > best.get(pl, (None, 0))[1]:
            best[pl] = (v, n)
    return {pl: v for pl, (v, n) in best.items() if n >= 3 and n >= share * lemma_total[pl]}


class Model:
    """form -> lemma for the known forms, the guesser for the rest."""

    def __init__(self, per, form_over):
        rows = [(f, c.most_common(1)[0][0], sum(c.values())) for f, c in per.items()]
        rows = [r for r in rows if r[2] >= MIN_COUNT]
        self.lex = {f: l for f, l, _ in rows}
        self.count = {f: n for f, _, n in rows}
        self.lex.update(form_over)
        self.guesser = Guesser(list(self.lex.items()))

    def __call__(self, w):
        return self.lex[w] if w in self.lex else self.guesser.lemma(w)


def build(extract, overrides):
    """(plain, folded) models from the extract (form, lemma, upos, count; gzipped TSV)."""
    lemma_over, form_over = read_overrides(overrides)
    per = collections.defaultdict(collections.Counter)
    adj = collections.defaultdict(collections.Counter)
    lemma_total = collections.Counter()
    with gzip.open(extract, 'rt', encoding='utf-8') as fh:
        for line in fh:
            f, l, p, n = line.rstrip('\n').split('\t')
            if p in SKIP_POS:
                continue
            f, l = f.lower(), l.lower()
            if WORD.match(f) and WORD.match(l):
                per[f][l] += int(n)
                lemma_total[l] += int(n)
                if p == 'ADJ':
                    adj[f][l] += int(n)
    remap = participle_map(adj, PART_SHARE, lemma_total)
    remap.update(lemma_over)
    del adj
    for c in per.values():
        for l in [l for l in c if l in remap]:
            c[remap[l]] += c.pop(l)
    plain = Model(per, form_over)
    fper = collections.defaultdict(collections.Counter)
    for f, c in per.items():
        for l, n in c.items():
            fper[fold(f)][fold(l)] += n
    del per
    folded = Model(fper, {fold(a): fold(b) for a, b in form_over.items()})
    return plain, folded
