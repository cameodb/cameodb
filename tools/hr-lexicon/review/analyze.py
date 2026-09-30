#!/usr/bin/env python3
"""What hr_stem and hr_stem_fold return for some words.

    HR_LEXICON_EXTRACT=... python3 tools/hr-lexicon/review/analyze.py Umagu Vodnjana fazana
    echo "u Gradu Umagu" | HR_LEXICON_EXTRACT=... python3 tools/hr-lexicon/review/analyze.py -
"""
import re, sys
from model import load, fold

plain, folded = load()
words = re.findall(r'[^\W\d_]+', sys.stdin.read()) if sys.argv[1:] == ['-'] else sys.argv[1:]
for w in words:
    lo = w.lower()
    tag = '' if lo in plain.lex else '  (suffix rules)'
    print(f'{w:<20} hr_stem {plain(lo):<20} hr_stem_fold {folded(fold(lo)):<20}{tag}')
