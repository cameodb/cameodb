#!/usr/bin/env python3
"""Quality gate for the Croatian analyzer data: run it before committing a rebuilt data set.

    python3 tools/hr-lexicon/eval/gate.py /path/to/extract/agg_s10.tsv.gz

Builds the two models exactly as build.py does and scores them against hand-checked Croatian:
  hr500k   lemmas in 500k words of running text (news, web)
  hrLex    the inflectional lexicon, split by lemma hash; the held-out half is the one reported
  phenomena  recall on fleeting a, sibilarization, palatalization, jat, verb tenses, participles...
  ascii    under hr_stem_fold, a base form typed without diacritics reaches the accented forms
Each score must reach its floor below (the values of the committed data, less a small margin);
the script exits non-zero otherwise. The metric is the search one: a query for a word's base form,
what share of the word's occurrences it reaches (recall) and what share of what it reaches is that
word (precision), frequency-weighted.

The gold data is CC BY-SA 4.0: it is downloaded into eval/.data (checksums pinned), used to
measure, and never committed, copied into overrides, or shipped.
"""
import collections, gzip, hashlib, os, re, subprocess, sys, urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, '..'))
sys.path.insert(0, HERE)
import lexicon
from score import Gold

DATA = os.path.join(HERE, '.data')
CLARIN = 'https://www.clarin.si/repository/xmlui/bitstream/handle/11356'
SOURCES = {
    'hrLex_v1.3.gz': (f'{CLARIN}/1232/hrLex_v1.3.gz', '94fcccc237cccf2256f382ece8d4ccc523bf80fd73f5e32b858f43412f46f9df'),
    'hr500k-train.conllu.gz': (f'{CLARIN}/1792/hr500k-train.conllu.gz', '5b5c35a9bfc6429957c88e6eaf55f26579fb1004f8a414adf8958c237f9c97d7'),
    'hr500k-dev.conllu.gz': (f'{CLARIN}/1792/hr500k-dev.conllu.gz', 'f40b383ff9ced625174a18fa26fbf309d1f70f4046079986e33a355ba6ed4163'),
    'hr500k-test.conllu.gz': (f'{CLARIN}/1792/hr500k-test.conllu.gz', '3c7ab74831d02d105d871ce94f4e6224aadb80c630abf9fb9b2ac2e96abbd641'),
}
# Floors: committed data's scores less a margin. Raise them when the data improves.
FLOORS = {
    'F1 hr500k': 0.968, 'F1 hrlex_held': 0.965, 'F1 fold hr500k': 0.966, 'F1 fold hrlex_held': 0.962,
    'ascii hr500k': 0.965,
    'cat fleeting_a': 0.93, 'cat sibilarization': 0.97, 'cat palatalization': 0.98, 'cat jat_alternation': 0.97,
    'cat passive_participle': 0.96, 'cat verb_present': 0.96, 'cat verb_aorist': 0.935, 'cat verb_imperfect': 0.94,
}
OPEN_POS = {'NOUN', 'ADJ', 'VERB', 'ADV'}
WORD = re.compile(r'^[a-zčćđšž]+$')


def fetch():
    os.makedirs(DATA, exist_ok=True)
    for name, (url, digest) in SOURCES.items():
        path = os.path.join(DATA, name)
        if not os.path.exists(path):
            print(f'downloading {name}', file=sys.stderr, flush=True)
            urllib.request.urlretrieve(url + '?isAllowed=y', path + '.part')
            os.rename(path + '.part', path)
        h = hashlib.sha256(open(path, 'rb').read()).hexdigest()
        if h != digest:
            raise SystemExit(f'{path}: sha256 {h}, expected {digest}')


def gold():
    """gold_<set>.tsv (form, lemma, upos, weight, half) and words.txt, as score.Gold reads them."""
    out = os.path.join(DATA, 'gold')
    if os.path.exists(os.path.join(out, 'words.txt')):
        return out
    os.makedirs(out, exist_ok=True)
    keep = lambda f, l, p: p in OPEN_POS and WORD.match(f) and WORD.match(l)
    sets = {'hrlex': collections.Counter(), 'hr500k': collections.Counter()}
    for line in gzip.open(os.path.join(DATA, 'hrLex_v1.3.gz'), 'rt', encoding='utf-8'):
        f = line.rstrip('\n').split('\t')
        if len(f) >= 7 and int(f[6]) > 0 and keep(f[0].lower(), f[1].lower(), f[4]):
            sets['hrlex'][(f[0].lower(), f[1].lower(), f[4])] += int(f[6])
    for part in ('train', 'dev', 'test'):
        for line in gzip.open(os.path.join(DATA, f'hr500k-{part}.conllu.gz'), 'rt', encoding='utf-8'):
            if not line.strip() or line[0] == '#':
                continue
            f = line.split('\t')
            if '-' not in f[0] and '.' not in f[0] and keep(f[1].lower(), f[2].lower(), f[3]):
                sets['hr500k'][(f[1].lower(), f[2].lower(), f[3])] += 1
    words = set()
    for name, agg in sets.items():
        with open(os.path.join(out, f'gold_{name}.tsv'), 'w', encoding='utf-8') as o:
            for (f, l, p), n in agg.items():
                o.write(f'{f}\t{l}\t{p}\t{n}\t{hashlib.md5(l.encode()).digest()[0] & 1}\n')
                words.update((f, l))
    with open(os.path.join(out, 'words.txt'), 'w', encoding='utf-8') as o:
        o.write(''.join(w + '\n' for w in sorted(words)))
    return out


def main():
    if len(sys.argv) != 2:
        raise SystemExit(__doc__)
    fetch()
    g = Gold(gold())
    cats_dir = os.path.join(DATA, 'cats')
    if not os.path.isdir(cats_dir):
        os.makedirs(cats_dir)
        subprocess.run([sys.executable, os.path.join(HERE, 'categories.py'), os.path.join(DATA, 'hrLex_v1.3.gz'), cats_dir], check=True)
    print('building models (a minute or two)...', file=sys.stderr, flush=True)
    plain, folded = lexicon.build(sys.argv[1], os.path.join(HERE, '..', 'overrides.tsv'))
    fold_term = lambda w: folded(lexicon.fold(w))

    scores = {}
    rep = g.report([plain(w) for w in g.words])
    frep = g.report([fold_term(w) for w in g.words])
    for k in ('hr500k', 'hrlex_held'):
        scores[f'F1 {k}'] = rep[k]['ALL']['F1']
        scores[f'F1 fold {k}'] = frep[k]['ALL']['F1']
    tot = hit = 0
    for f, l, p, n, h in g.sets['hr500k']:
        if lexicon.fold(l) != l:
            tot += n
            hit += n * (fold_term(lexicon.fold(l)) == fold_term(f))
    scores['ascii hr500k'] = hit / tot
    for fn in sorted(os.listdir(cats_dir)):
        pairs = [x.rstrip('\n').split('\t') for x in open(os.path.join(cats_dir, fn), encoding='utf-8')]
        total = sum(int(r[3]) for r in pairs)
        scores[f'cat {fn[4:-4]}'] = sum(int(r[3]) for r in pairs if plain(r[0]) == plain(r[1])) / total

    failed = 0
    for k, v in scores.items():
        floor = FLOORS.get(k)
        mark = '' if floor is None else ('ok' if v >= floor else 'BELOW FLOOR')
        failed += mark == 'BELOW FLOOR'
        print(f'{k:<28}{v:>8.1%}   {"" if floor is None else f"floor {floor:.1%}":<14}{mark}')
    if failed:
        raise SystemExit(f'{failed} score(s) below the floor')
    print('gate passed')


if __name__ == '__main__':
    main()
