#!/bin/bash
# Retention pruning wrapper: select -> review -> delete.
#
#   retention-prune.sh check  [CUTOFF] [--path DIR] [--probe-write] [--probe-admin]
#                                                         verify the setup before a long run
#   retention-prune.sh fields [CUTOFF]                    candidate date fields: populated, min/max, older than cutoff
#   retention-prune.sh count  [CUTOFF]                    documents older than the cutoff right now
#   retention-prune.sh select --output FILE [CUTOFF] [--newer-than DATE]
#                                                         write the id list of documents older than the cutoff
#   retention-prune.sh review FILE                        checks of the id list (lines, unique, format, first/last)
#   retention-prune.sh delete FILE [--execute] [...]      bulk delete; dry run unless --execute
#
# Deleting frees no disk by itself. POST /_admin/index/<index>/merge returns the search index's
# share while the node runs; the documents' share stays inside the node's redb files until the
# node is stopped and `cameodb compact -c <config>` runs (`--dry-run` first reports how much).
#
# CUTOFF is --months N (retain N previous months + the current one, default 6) or an exact
# --older-than YYYY-MM-DD. Further options are forwarded: retention_prune.py <command> -h.
#
# Env: CAMEODB_URL (default http://alt-dbtest:9480), CAMEODB_API_KEY, INDEX (default searchdata),
#      FIELD (default first_seen; e.g. FIELD=rca2_last_seen).

set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
PY="$HERE/retention_prune.py"
INDEX="${INDEX:-searchdata}"
FIELD="${FIELD:-first_seen}"
export CAMEODB_URL="${CAMEODB_URL:-http://alt-dbtest:9480}"
common=(--index "$INDEX" --field "$FIELD")

cmd="${1:-}"; shift || true
case "$cmd" in
  check|fields|count|select)
    python3 "$PY" "${common[@]}" "$cmd" "$@"
    [ "$cmd" = select ] && echo "Next: $0 review <the --output file>" || true
    ;;
  review)
    out="${1:?id file}"
    echo "lines:     $(wc -l < "$out")"
    echo "unique:    $(sort -u "$out" | wc -l)   (must equal lines)"
    echo "non-hex64: $(grep -vcE '^[0-9a-f]{64}$' "$out" || true)   (0 expected for sha256 ids)"
    echo "first/last:"; head -n1 "$out"; tail -n1 "$out"
    if [ -f "$out.manifest.json" ]; then echo "manifest:"; head -c 600 "$out.manifest.json"; echo " ..."; fi
    echo "Spot-check a few ids before deleting: id:<value> return $FIELD"
    ;;
  delete)
    out="${1:?id file}"; shift
    python3 "$PY" "${common[@]}" delete "$out" "$@"
    ;;
  *)
    sed -n '2,18p' "$0"; exit 1 ;;
esac
