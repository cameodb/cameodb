#!/usr/bin/env bash
# Parallel-range download of CLASSLA-web.hr 2.0 (CC0) with rclone. The server honours byte ranges,
# so rclone splits the file into chunks fetched over several connections; a failed chunk is retried
# on its own (--low-level-retries) and the file only appears under its final name when complete.
set -euo pipefail
BASE="https://www.clarin.si/repository/xmlui/bitstream/handle/11356/2079/"
FILE=CLASSLA-web.hr.2.0.vert.tar.gz
OUT_DIR=${1:?output directory}
STREAMS=${STREAMS:-8}
rclone copyto --http-url "$BASE" ":http:$FILE" "$OUT_DIR/$FILE" \
  --multi-thread-streams "$STREAMS" --multi-thread-cutoff 64M --multi-thread-chunk-size 64M \
  --low-level-retries 20 --retries 3 --stats 60s --stats-one-line -v
want=28742141283; have=$(wc -c < "$OUT_DIR/$FILE" | tr -d ' ')
[[ $have == "$want" ]] || { echo "size mismatch: $have != $want" >&2; exit 1; }
# The repository publishes this MD5 (METS record of 11356/2079).
md5=$( (md5sum "$OUT_DIR/$FILE" 2>/dev/null || md5 -r "$OUT_DIR/$FILE") | cut -d' ' -f1)
[[ $md5 == b1456e27ffe6c9267f53bf634dfb78f6 ]] || { echo "md5 mismatch: $md5" >&2; exit 1; }
echo "complete: $have bytes, md5 ok"
