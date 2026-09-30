"""Scores a stemmer against the gold sets from prepare.py.

The question each metric answers is the search one: a query for a word's base form (its lemma) —
what share of the word's occurrences does it reach (recall), and what share of what it reaches is
that word (precision)? Both are weighted by frequency. Stemmers are expected to conflate some
derivations (vrijedan/vrijednost); those count against precision here, which is the conservative
reading.
"""
import collections, json, os, sys

POS = ('NOUN', 'ADJ', 'VERB', 'ADV')

class Gold:
    def __init__(self, cache):
        self.words = open(f'{cache}/words.txt', encoding='utf-8').read().split('\n')[:-1]
        self.sets = {}
        for fn in sorted(os.listdir(cache)):
            if fn.startswith('gold_') and fn.endswith('.tsv'):
                rows = [l.rstrip('\n').split('\t') for l in open(f'{cache}/{fn}', encoding='utf-8')]
                self.sets[fn[5:-4]] = [(f, l, p, int(n), int(h)) for f, l, p, n, h in rows]

    def score(self, stems, name, half=None):
        """Recall, precision, F1 overall and per POS. `half` restricts the queries (lemmas) to one
        hrLex half; the pool a query competes in is always the whole set."""
        S = stems if isinstance(stems, dict) else dict(zip(self.words, stems))
        rows = self.sets[name]
        by_s = collections.Counter(); by_sl = collections.Counter()
        lf = collections.Counter(); lpos = {}
        rn = collections.Counter(); rd = collections.Counter()
        for f, l, p, n, h in rows:
            by_s[S[f]] += n; by_sl[(S[f], l)] += n
            if half is not None and h != half: continue
            lf[l] += n; lpos.setdefault(l, p); rd[p] += n
            if S[f] == S[l]: rn[p] += n
        pn = collections.Counter(); pd = collections.Counter()
        for l, n in lf.items():
            s = S[l]
            if by_s[s]:
                pn[lpos[l]] += n * by_sl[(s, l)] / by_s[s]; pd[lpos[l]] += n
        def f1(r, p): return 2 * r * p / (r + p) if r + p else 0.0
        out = {}
        for p in POS + ('ALL',):
            num_r = sum(rn.values()) if p == 'ALL' else rn[p]; den_r = sum(rd.values()) if p == 'ALL' else rd[p]
            num_p = sum(pn.values()) if p == 'ALL' else pn[p]; den_p = sum(pd.values()) if p == 'ALL' else pd[p]
            if den_r and den_p:
                r, pr = num_r / den_r, num_p / den_p
                out[p] = {'R': r, 'P': pr, 'F1': f1(r, pr)}
        return out

    def report(self, stems):
        res = {}
        for name in self.sets:
            if name == 'hrlex':
                res['hrlex_tune'] = self.score(stems, name, 0)
                res['hrlex_held'] = self.score(stems, name, 1)
            else:
                res[name] = self.score(stems, name)
        return res

def table(res):
    lines = [f'{"set":12} {"R":>6} {"P":>6} {"F1":>6}   ' + '  '.join(f'{p:>11}' for p in POS[:3])]
    for name, r in res.items():
        a = r['ALL']
        lines.append(f'{name:12} {a["R"]:6.1%} {a["P"]:6.1%} {a["F1"]:6.1%}   '
                     + '  '.join(f'{r[p]["R"]:5.1%}/{r[p]["P"]:5.1%}' if p in r else f'{"-":>11}' for p in POS[:3]))
    return '\n'.join(lines)

if __name__ == '__main__':
    g = Gold(sys.argv[1])
    stems = open(sys.argv[2], encoding='utf-8').read().split('\n')[:-1]
    res = g.report(stems)
    print(table(res))
    if len(sys.argv) > 3: json.dump(res, open(sys.argv[3], 'w'), indent=1)
