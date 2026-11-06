"""Builds the fold tables behind cjk_fold, cjk_ja_fold and cjk_ko_fold into crates/storage/src/analysis/cjk/data.

    uv run --project tools/cjk-fold tools/cjk-fold/build.py --opencc DIR --unihan Unihan.zip

DIR holds OpenCC's TSCharacters.txt and JPShinjitaiCharacters.txt at the pinned commit; Unihan.zip is
Unicode 18.0.0's. Each source is refused at any other checksum. Every table is one character to one,
both in the Basic Multilingual Plane, chains resolved so the fold is idempotent. It writes:
  zh_table.rs   traditional Chinese -> simplified (OpenCC TSCharacters, first value, zh-overrides.tsv)
  ja_table.rs   old-style kanji -> new (OpenCC JPShinjitaiCharacters reversed, Unihan
                kJapaneseNewVariant, ja-overrides.tsv, compatibility ideographs to their canonical form)
  ko_table.rs   Hanja -> Hangul reading (Unihan kHangul: the reading flagged E, then 0, then the first)
  MANIFEST.txt  the sources, their checksums, the overrides' sha256, the counts and each file's checksum
Each .rs holds KEYS and VALUES, two sorted [u16; N] arrays the runtime binary-searches. Identical inputs
give identical bytes.
"""
import argparse, hashlib, io, os, re, sys, unicodedata, zipfile

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, '..', '..', 'crates', 'storage', 'src', 'analysis', 'cjk', 'data')
TOOL = 'tools/cjk-fold'

OPENCC_COMMIT = 'fae2a0a911c30764e669a7d8b0591cd8e852934a'
OPENCC_URL = f'https://raw.githubusercontent.com/BYVoid/OpenCC/{OPENCC_COMMIT}/data/dictionary/'
TS_SHA256 = '9ff46a7d30e5765375eb13d33f2b03a34d298913caf2b120380679f33ae1642d'
JP_SHA256 = '12cec7250b873ef52b36d8f92218d4f92c0aaf5d8cd7c58fe42d9785bdcdc43a'
UNIHAN_VERSION = '18.0.0'
UNIHAN_URL = f'https://www.unicode.org/Public/{UNIHAN_VERSION}/ucd/Unihan.zip'
UNIHAN_SHA256 = '4c93ea9c1f636451729a840978f1667a53886af37ba854fdcce109721c63d43e'

# Every key and value of the Chinese table falls in U+3400–U+9FFF. The Japanese and Korean keys may also
# be compatibility ideographs (U+F900–U+FAFF), and the Korean values are Hangul syllables. All of these
# are 3-byte UTF-8 characters, so the runtime rewrites a token's bytes in place.
HAN = (0x3400, 0x9FFF)
COMPAT = (0xF900, 0xFAFF)
HANGUL = (0xAC00, 0xD7A3)
REFERENCE = re.compile(r'Unihan (k\w+Variant) U\+([0-9A-F]{4,5}) → U\+([0-9A-F]{4,5})')


def inside(c, *ranges):
    return any(lo <= ord(c) <= hi for lo, hi in ranges)


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def read_pinned(path, want, what):
    with open(path, 'rb') as fh:
        data = fh.read()
    if sha256(data) != want:
        sys.exit(f'{path}: sha256 {sha256(data)}, want {want} ({what})')
    return data


def dictionary_lines(data):
    for line in data.decode('utf-8').splitlines():
        if line.startswith('#') or not line.strip():
            continue
        key, values = line.split('\t')
        yield key, values.split(' ')


def unihan_fields(zdata, member, fields):
    """{field: {char: value}} for the named fields of one Unihan file."""
    out = {f: {} for f in fields}
    with zipfile.ZipFile(io.BytesIO(zdata)) as z:
        for line in z.read(member).decode('utf-8').splitlines():
            parts = line.split('\t')
            if len(parts) == 3 and parts[1] in out:
                out[parts[1]][chr(int(parts[0][2:], 16))] = parts[2]
    return out


def variants(value):
    """The characters of a Unihan variant value: `U+5F37<kMatthews U+5F4A` -> ['強', '彊']."""
    return [chr(int(v.split('<')[0][2:], 16)) for v in value.split()]


def read_overrides(path, check=None):
    overrides = {}
    with open(path, encoding='utf-8') as fh:
        for n, line in enumerate(fh, 1):
            if line.startswith('#') or not line.strip():
                continue
            fields = line.rstrip('\n').split('\t')
            if len(fields) != 4 or not all(fields) or len(fields[0]) != 1 or len(fields[1]) != 1:
                sys.exit(f'{os.path.basename(path)}:{n}: want one character, one character, reason, reference')
            if fields[0] in overrides:
                sys.exit(f'{os.path.basename(path)}:{n}: {fields[0]} twice')
            if check:
                check(n, *fields)
            overrides[fields[0]] = fields[1]
    return overrides


def resolve(pairs):
    """Each key taken to the end of its chain, so fold(fold(c)) == fold(c); identities dropped."""
    out = {}
    for key in pairs:
        seen, value = {key}, pairs[key]
        while value in pairs and pairs[value] != value:
            if value in seen:
                sys.exit(f'cycle through {key}')
            seen.add(value)
            value = pairs[value]
        if value != key:
            out[key] = value
    return out


def bmp_pairs(pairs):
    return {k: v for k, v in pairs.items() if len(k) == 1 and len(v) == 1 and ord(k) < 0x10000 and ord(v) < 0x10000}


# ---------------------------------------------------------------- the three tables

def zh_table(ts, overrides):
    source = dict((k, v[0]) for k, v in dictionary_lines(ts))
    first = {k: v for k, v in source.items() if k != v}
    merged = {**first, **overrides}
    bmp = bmp_pairs(merged)
    table = resolve(bmp)
    counts = {
        'entries': len(source),
        'changed by first value': len(first),
        'overrides': len(overrides),
        'overrides adding a pair': sum(1 for k in overrides if k not in first),
        'dropped outside the BMP': len(merged) - len(bmp),
        'chains resolved': sum(1 for k, v in bmp.items() if table.get(k) != v),
        'pairs': len(table),
    }
    return table, counts, ((HAN,), (HAN,))


def ja_table(jp, new_variant, overrides):
    # JPShinjitaiCharacters lists a new form and the old forms it replaces. Where one old form sits under
    # two keys (鹽 under 塩 and 䀋), the file says which one the reverse direction takes, in a comment line
    # `# @reverse-prefer: 鹽 塩`; every such conflict has one, and the build refuses one without.
    opencc, prefer = {}, {}
    for line in jp.decode('utf-8').splitlines():
        if line.startswith('# @reverse-prefer:'):
            old, new = line.split(':', 1)[1].split()
            prefer[old] = new
    for new, olds in dictionary_lines(jp):
        for old in olds:
            if old == new:
                continue
            if old in opencc and opencc[old] != new and old not in prefer:
                sys.exit(f'JPShinjitaiCharacters: {old} under {opencc[old]} and {new}, no @reverse-prefer')
            opencc[old] = prefer.get(old, new)
    unihan = {k: variants(v)[0] for k, v in new_variant.items()}
    merged = {**opencc, **unihan, **overrides}
    # Compatibility ideographs are the same characters as their canonical forms; they fold as those do.
    compat = {}
    for code in range(COMPAT[0], COMPAT[1] + 1):
        c = chr(code)
        canonical = unicodedata.normalize('NFC', c)
        if canonical != c and len(canonical) == 1:
            compat[c] = canonical
    merged = {**compat, **merged}
    bmp = bmp_pairs(merged)
    table = resolve(bmp)
    counts = {
        'opencc old forms': len(opencc),
        'unihan kJapaneseNewVariant': len(unihan),
        'unihan differing from opencc': sum(1 for k, v in unihan.items() if k in opencc and opencc[k] != v),
        'overrides': len(overrides),
        'compatibility ideographs': len(compat),
        'dropped outside the BMP': len(merged) - len(bmp),
        'chains resolved': sum(1 for k, v in bmp.items() if table.get(k) != v),
        'pairs': len(table),
    }
    return table, counts, ((HAN, COMPAT), (HAN,))


def ko_reading(value):
    """The reading a Hanja folds to: the one flagged E (basic education Hanja), then 0 (KS X 1001),
    then the first listed. `낙:0 락:0E 악:0N 요:0N` -> 락."""
    readings = [r.split(':') for r in value.split()]
    for flag in ('E', '0'):
        for reading, flags in readings:
            if flag in flags:
                return reading
    return readings[0][0]


def ko_table(hangul):
    pairs = {k: ko_reading(v) for k, v in hangul.items()}
    bmp = bmp_pairs(pairs)
    table = {k: v for k, v in bmp.items() if inside(k, HAN, COMPAT)}
    counts = {
        'unihan kHangul': len(hangul),
        'with several readings': sum(1 for v in hangul.values() if len(v.split()) > 1),
        'not the first reading': sum(1 for v in hangul.values() if ko_reading(v) != v.split()[0].split(':')[0]),
        'dropped outside the BMP': len(pairs) - len(bmp),
        'pairs': len(table),
    }
    return table, counts, ((HAN, COMPAT), (HANGUL,))


# ---------------------------------------------------------------- output

def rust_array(name, values):
    rows = [', '.join(f'0x{v:04X}' for v in values[i:i + 12]) for i in range(0, len(values), 12)]
    body = ''.join(f'    {row},\n' for row in rows)
    return f'pub(crate) static {name}: [u16; {len(values)}] = [\n{body}];\n'


def write_table(out, name, table, ranges, header):
    keys_in, values_in = ranges
    for k, v in table.items():
        if not inside(k, *keys_in) or not inside(v, *values_in):
            sys.exit(f'{name}: {k}→{v} (U+{ord(k):04X}→U+{ord(v):04X}) is outside its ranges')
        if table.get(v, v) != v:
            sys.exit(f'{name}: {k}→{v} is not idempotent')
    keys = sorted(table, key=ord)
    path = os.path.join(out, name)
    with open(path, 'w', encoding='utf-8', newline='\n') as o:
        o.write(f'// Generated by {TOOL}/build.py; do not edit. See {TOOL}/README.md.\n')
        o.write(''.join(f'// {line}\n' for line in header))
        o.write('\n')
        o.write(rust_array('KEYS', [ord(k) for k in keys]))
        o.write('\n')
        o.write(rust_array('VALUES', [ord(table[k]) for k in keys]))
    with open(path, 'rb') as fh:
        data = fh.read()
    return f'file\t{name}\t{len(data)}\t{sha256(data)}\n'


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument('--opencc', required=True, help=f'directory with TSCharacters.txt and JPShinjitaiCharacters.txt, from {OPENCC_URL}')
    ap.add_argument('--unihan', required=True, help=f'Unihan.zip, from {UNIHAN_URL}')
    ap.add_argument('--out', default=OUT)
    args = ap.parse_args()

    ts = read_pinned(os.path.join(args.opencc, 'TSCharacters.txt'), TS_SHA256, f'OpenCC {OPENCC_COMMIT}')
    jp = read_pinned(os.path.join(args.opencc, 'JPShinjitaiCharacters.txt'), JP_SHA256, f'OpenCC {OPENCC_COMMIT}')
    zdata = read_pinned(args.unihan, UNIHAN_SHA256, f'Unihan {UNIHAN_VERSION}')
    var = unihan_fields(zdata, 'Unihan_Variants.txt',
                        ('kJapaneseNewVariant', 'kZVariant', 'kSemanticVariant', 'kSimplifiedVariant'))
    hangul = unihan_fields(zdata, 'Unihan_Readings.txt', ('kHangul',))['kHangul']

    def check_reference(n, old, new, _reason, reference):
        """A reference naming a Unihan variant must be what Unihan says."""
        m = REFERENCE.search(reference)
        if not m:
            return
        field, a, b = m.group(1), chr(int(m.group(2), 16)), chr(int(m.group(3), 16))
        if a != old or b not in variants(var.get(field, {}).get(a, '')):
            sys.exit(f'ja-overrides.tsv:{n}: Unihan {field} of {old} U+{ord(old):04X} is '
                     f'{var.get(field, {}).get(old, "absent")}, not U+{ord(b):04X}')

    zh_over = os.path.join(HERE, 'zh-overrides.tsv')
    ja_over = os.path.join(HERE, 'ja-overrides.tsv')
    tables = [
        ('zh_table.rs', *zh_table(ts, read_overrides(zh_over)), [
            f'Derived from OpenCC TSCharacters.txt (Apache-2.0, commit {OPENCC_COMMIT}),',
            'changed: the first value of each entry, reduced to BMP pairs, chains resolved, with the',
            f'overrides of {TOOL}/zh-overrides.tsv. Traditional characters in KEYS, each folded',
            'form at the same index of VALUES.']),
        ('ja_table.rs', *ja_table(jp, var['kJapaneseNewVariant'], read_overrides(ja_over, check_reference)), [
            f'Derived from OpenCC JPShinjitaiCharacters.txt (Apache-2.0, commit {OPENCC_COMMIT}), reversed,',
            f'and Unihan {UNIHAN_VERSION} kJapaneseNewVariant (Unicode License v3, licenses/LICENSE-UNICODE-3.0),',
            f'with the overrides of {TOOL}/ja-overrides.tsv and the compatibility ideographs folded to',
            'their canonical forms; reduced to BMP pairs, chains resolved. Old-style kanji in KEYS, each',
            'new form at the same index of VALUES.']),
        ('ko_table.rs', *ko_table(hangul), [
            f'Derived from Unihan {UNIHAN_VERSION} kHangul (Unicode License v3, licenses/LICENSE-UNICODE-3.0):',
            'one reading per Hanja (flagged E, then 0, then the first), BMP only. Hanja in KEYS, each',
            'Hangul reading at the same index of VALUES; the initial-sound rule is applied at run time.']),
    ]
    os.makedirs(args.out, exist_ok=True)
    manifest = [
        f'# Generated by {TOOL}/build.py; do not edit. See {TOOL}/README.md.\n',
        f'source\tOpenCC data/dictionary/TSCharacters.txt (Apache-2.0) commit {OPENCC_COMMIT} sha256 {TS_SHA256}\n',
        f'source\tOpenCC data/dictionary/JPShinjitaiCharacters.txt (Apache-2.0) commit {OPENCC_COMMIT} sha256 {JP_SHA256}\n',
        f'source\tUnihan.zip {UNIHAN_VERSION} (Unicode License v3) sha256 {UNIHAN_SHA256}\n',
        f'overrides\tzh-overrides.tsv sha256 {sha256(open(zh_over, "rb").read())}\n',
        f'overrides\tja-overrides.tsv sha256 {sha256(open(ja_over, "rb").read())}\n',
    ]
    for name, table, counts, ranges, header in tables:
        manifest.append(f'counts\t{name}\t' + ', '.join(f'{k} {v}' for k, v in counts.items()) + '\n')
        manifest.append(write_table(args.out, name, table, ranges, header))
        print(f'{name}: ' + ', '.join(f'{k} {v}' for k, v in counts.items()))
    with open(os.path.join(args.out, 'MANIFEST.txt'), 'w', encoding='utf-8', newline='\n') as o:
        o.writelines(manifest)


if __name__ == '__main__':
    main()
