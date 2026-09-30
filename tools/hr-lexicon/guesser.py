"""Suffix-rewrite lemma guesser, learned from (form, lemma) pairs.

Each pair teaches one rewrite: past their common prefix, strip what is left of the form and add
what is left of the lemma (članka -> članak: strip "ka", add "ak"). The rewrite is recorded under
every suffix of the form that covers the stripped part, from that part itself up to MAX_CTX
characters. An unknown word takes the rewrite that is most common under its longest suffix with
enough support; it counts word types, not tokens, so a handful of frequent irregular words does not
decide the rule for every rare word that happens to end the same way.

The runtime (crates/storage/src/analysis/croatian) walks the same contexts, longest first, with the
same MIN_STEM check; its constants must equal these.
"""
import collections

MAX_CTX = 7
MIN_SUPPORT = 3
MIN_STEM = 2


def rewrite(form, lemma):
    k = 0
    while k < min(len(form), len(lemma)) and form[k] == lemma[k]:
        k += 1
    return form[k:], lemma[k:]


class Guesser:
    def __init__(self, pairs, min_support=MIN_SUPPORT, max_ctx=MAX_CTX):
        self.min_support, self.max_ctx = min_support, max_ctx
        table = collections.defaultdict(collections.Counter)
        for form, lemma in pairs:
            strip, add = rewrite(form, lemma)
            if len(form) - len(strip) < MIN_STEM:
                continue
            for n in range(max(len(strip), 1), min(max_ctx, len(form)) + 1):
                table[form[-n:]][(strip, add)] += 1
        # Keep, per context, only the winning rewrite.
        self.best = {}
        for ctx, c in table.items():
            if sum(c.values()) >= min_support:
                self.best[ctx] = c.most_common(1)[0][0]

    def lemma(self, word):
        for n in range(min(self.max_ctx, len(word)), 0, -1):
            rw = self.best.get(word[-n:])
            if rw:
                strip, add = rw
                if word.endswith(strip) and len(word) - len(strip) >= MIN_STEM:
                    return word[:len(word) - len(strip)] + add
        return word

    def pruned(self):
        """The contexts that decide something. A context goes when the nearest shorter context in
        the table holds the same rewrite, or when it holds the identity and no shorter one exists:
        the longest-match walk then reaches an equal answer without it. Output is unchanged."""
        keep = {}
        for ctx, rw in self.best.items():
            shorter = next((self.best[ctx[-n:]] for n in range(len(ctx) - 1, 0, -1) if ctx[-n:] in self.best), None)
            if shorter == rw or (shorter is None and rw == ('', '')):
                continue
            keep[ctx] = rw
        return keep
