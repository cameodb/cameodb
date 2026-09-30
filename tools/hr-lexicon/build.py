#!/usr/bin/env python3
"""Builds the data behind hr_stem and hr_stem_fold into crates/storage/src/analysis/croatian/data.

    python3 tools/hr-lexicon/build.py /path/to/extract/agg_s10.tsv.gz

For each of the two models (plain, and folded for hr_stem_fold) it writes:
  <model>.exceptions.fst  form -> rewrite, only for the forms the guesser gets wrong
  <model>.guesser.fst     reversed suffix context -> rewrite, pruned to the contexts that decide
  <model>.adds.bin        the strings rewrites append (u32 LE count N, N+1 u32 LE offsets, bytes)
  <model>.hot.fst         the most frequent forms, whose answers the runtime caches
and, for both:
  sample.tsv              word, hr_stem term, hr_stem_fold term: the runtime must reproduce these
  MANIFEST.txt            inputs, parameters, sizes and checksums

A rewrite is one u64: (add id << 7) | chars to strip. The stripped part is always a suffix of the
word it applies to, so only its length is stored. The FSTs are written by fstbuild/, pinned to the
tantivy-fst the runtime reads them with. Identical inputs give identical bytes.
"""
import argparse, hashlib, os, subprocess, sys, tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import lexicon
from guesser import MAX_CTX, MIN_STEM, rewrite

EXTRACT_SHA256 = 'f1f46753f4f33dc7767f2e3950ad27f3c03e649de32ac4e191118f674ed25ec2'
STRIP_BITS = 7
HOT = 16_384
OUT = os.path.join(HERE, '..', '..', 'crates', 'storage', 'src', 'analysis', 'croatian', 'data')


def sha256(path):
    h = hashlib.sha256()
    with open(path, 'rb') as fh:
        for block in iter(lambda: fh.read(1 << 20), b''):
            h.update(block)
    return h.hexdigest()


def tables(model, work, name):
    """The model as the runtime stores it; returns counts for the manifest."""
    g = model.guesser
    exceptions = sorted((f, l) for f, l in model.lex.items() if g.lemma(f) != l)
    contexts = g.pruned()
    rewrites = [rewrite(f, l) for f, l in exceptions] + list(contexts.values())
    uses = {}
    for _, add in rewrites:
        uses[add] = uses.get(add, 0) + 1
    adds = sorted(uses, key=lambda a: (-uses[a], a))
    add_id = {a: i for i, a in enumerate(adds)}

    def value(rw):
        strip, add = rw
        if len(strip) >= 1 << STRIP_BITS:
            raise SystemExit(f'a rewrite strips {len(strip)} chars; the format allows {(1 << STRIP_BITS) - 1}')
        return add_id[add] << STRIP_BITS | len(strip)

    def write(fname, rows):
        with open(os.path.join(work, f'{name}.{fname}'), 'w', encoding='utf-8') as o:
            for row in rows:
                o.write('\t'.join(map(str, row)) + '\n')

    write('exceptions.tsv', ((f, value(rewrite(f, l))) for f, l in exceptions))
    write('guesser.tsv', ((ctx[::-1], value(rw)) for ctx, rw in sorted(contexts.items())))
    write('adds.tsv', ((a,) for a in adds))
    hot = sorted(model.count, key=lambda f: (-model.count[f], f))[:HOT]
    write('hot.tsv', ((f,) for f in hot))
    return {'forms': len(model.lex), 'exceptions': len(exceptions), 'guesser contexts': len(g.best),
            'contexts kept': len(contexts), 'add strings': len(adds), 'hot forms': len(hot)}


def sample(plain, folded, overrides):
    """Words whose terms the runtime must reproduce: the most frequent forms, a spread of the rest,
    words the lexicon lacks (guesser), every override, and a few that once went wrong."""
    by_count = sorted(plain.count, key=lambda f: (-plain.count[f], f))
    spread = sorted(plain.lex, key=lambda f: hashlib.md5(f.encode()).hexdigest())
    unknown = ['q' + f for f in spread[3000:3600]]
    lemma_over, form_over = lexicon.read_overrides(overrides)
    words = by_count[:1500] + spread[:1500] + unknown + sorted(form_over) + [
        'na', 'ni', 'korištenje', 'fažana', 'fazana', 'umagu', 'županije', 'zupanije', '2024', 'a1b2']
    seen, rows = set(), []
    for w in words:
        if w not in seen:
            seen.add(w)
            rows.append((w, plain(w), folded(lexicon.fold(w))))
    return rows


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('extract', help='agg_s10.tsv.gz from the CLASSLA-web.hr 2.0 pass (see README)')
    ap.add_argument('--out', default=OUT)
    ap.add_argument('--overrides', default=os.path.join(HERE, 'overrides.tsv'))
    ap.add_argument('--any-extract', action='store_true', help='skip the extract checksum (experiments only)')
    args = ap.parse_args()
    digest = sha256(args.extract)
    if digest != EXTRACT_SHA256 and not args.any_extract:
        raise SystemExit(f'{args.extract}: sha256 {digest}, expected {EXTRACT_SHA256}')

    print('building models (a minute or two)...', file=sys.stderr, flush=True)
    plain, folded = lexicon.build(args.extract, args.overrides)
    os.makedirs(args.out, exist_ok=True)
    with tempfile.TemporaryDirectory() as work:
        counts = {name: tables(m, work, name) for name, m in (('plain', plain), ('folded', folded))}
        subprocess.run(['cargo', 'run', '--quiet', '--release', '--manifest-path',
                        os.path.join(HERE, 'fstbuild', 'Cargo.toml'), '--', work, args.out], check=True)
    rows = sample(plain, folded, args.overrides)
    with open(os.path.join(args.out, 'sample.tsv'), 'w', encoding='utf-8') as o:
        for r in rows:
            o.write('\t'.join(r) + '\n')

    files = sorted(f for f in os.listdir(args.out) if f != 'MANIFEST.txt')
    with open(os.path.join(args.out, 'MANIFEST.txt'), 'w', encoding='utf-8') as o:
        o.write('# Generated by tools/hr-lexicon/build.py; do not edit. See tools/hr-lexicon/README.md.\n')
        o.write(f'source\tCLASSLA-web.hr 2.0 (CC0), balanced sample agg_s10.tsv.gz sha256 {digest}\n')
        o.write(f'overrides\t{os.path.basename(args.overrides)} sha256 {sha256(args.overrides)}\n')
        o.write(f'params\tmin_count={lexicon.MIN_COUNT} part_share={lexicon.PART_SHARE} max_ctx={MAX_CTX} '
                f'min_stem={MIN_STEM} strip_bits={STRIP_BITS} hot={HOT}\n')
        for name, c in counts.items():
            o.write(f'{name}\t' + ', '.join(f'{k} {v}' for k, v in c.items()) + '\n')
        o.write(f'sample\t{len(rows)} words\n')
        for f in files:
            p = os.path.join(args.out, f)
            o.write(f'file\t{f}\t{os.path.getsize(p)}\t{sha256(p)}\n')
    total = sum(os.path.getsize(os.path.join(args.out, f)) for f in files if f != 'sample.tsv')
    print(f'{args.out}: {total / 1e6:.2f} MB of analyzer data', file=sys.stderr)


if __name__ == '__main__':
    main()
