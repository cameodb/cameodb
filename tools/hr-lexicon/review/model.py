"""The two models, cached for the review scripts: set HR_LEXICON_EXTRACT to the extract's path.
The cache (review/out/models.pickle) is rebuilt when overrides.tsv or the model code changes."""
import os, pickle, sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, '..'))
import lexicon
from lexicon import fold  # noqa: F401  (re-exported for the scripts)


def load():
    extract = os.environ.get('HR_LEXICON_EXTRACT')
    if not extract:
        raise SystemExit('set HR_LEXICON_EXTRACT to the path of agg_s10.tsv.gz (see ../README.md)')
    cache = os.path.join(HERE, 'out', 'models.pickle')
    deps = [os.path.join(HERE, '..', f) for f in ('overrides.tsv', 'lexicon.py', 'guesser.py')]
    if os.path.exists(cache) and all(os.path.getmtime(cache) > os.path.getmtime(d) for d in deps):
        with open(cache, 'rb') as fh:
            return pickle.load(fh)
    print('building models (a minute or two)...', file=sys.stderr, flush=True)
    models = lexicon.build(extract, deps[0])
    os.makedirs(os.path.dirname(cache), exist_ok=True)
    with open(cache, 'wb') as fh:
        pickle.dump(models, fh, protocol=pickle.HIGHEST_PROTOCOL)
    return models
