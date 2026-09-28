#!/usr/bin/env bash
# Builds a reproducible, temporally stratified sample of one DEBS 2022 weekday CSV.
#
# A head slice alone would cover only the pre-open and the opening auction, so the
# profile would describe the busiest, least typical part of the session. Sampling
# evenly spaced byte ranges instead covers the whole day for a fraction of the
# download.
#
# Chunks are kept as separate files on purpose: rows are contiguous in source order
# only WITHIN a chunk, so any ordering analysis (per-symbol backward time jumps)
# must not span a boundary.
set -euo pipefail

RECORD_ID=6382482
DOI="10.5281/zenodo.6382482"
LICENSE="CC-BY-NC-SA-4.0"
ATTRIBUTION="Frischbier, Tahir, Doblander, Hormann, Mayer, Jacobsen - DEBS 2022 Grand Challenge Data Set: Trading Data"

FILE="${FILE:-debs2022-gc-trading-day-08-11-21.csv}"
CHUNKS="${CHUNKS:-20}"
CHUNK_MB="${CHUNK_MB:-48}"
HEAD_MB="${HEAD_MB:-8}"
OUT_DIR="${OUT_DIR:-data/slice}"
PROV="${PROV:-analysis/out/provenance.json}"

BASE="https://zenodo.org/records/${RECORD_ID}/files/${FILE}?download=1"
mkdir -p "$OUT_DIR" "$(dirname "$PROV")"

# Size and checksum come from the Zenodo record itself, not from what we downloaded.
meta=$(curl -sS --max-time 60 "https://zenodo.org/api/records/${RECORD_ID}")
read -r TOTAL MD5 <<<"$(printf '%s' "$meta" | FILE="$FILE" python3 -c "
import json, os, sys
r = json.load(sys.stdin)
e = next(f for f in r['files'] if f['key'] == os.environ['FILE'])
print(e['size'], e['checksum'])")"

head_bytes=$((HEAD_MB * 1024 * 1024))
chunk_bytes=$((CHUNK_MB * 1024 * 1024))

# Chunk 00 starts at byte 0, so it carries the comment block and the header row.
# Its final line is truncated by the range request, so drop it.
curl -sS --max-time 900 -r "0-$((head_bytes - 1))" "$BASE" \
  | sed '$d' > "$OUT_DIR/chunk_00.csv"

offsets="0"
usable=$((TOTAL - head_bytes - chunk_bytes))
for i in $(seq 1 "$CHUNKS"); do
  off=$((head_bytes + (usable * i) / (CHUNKS + 1)))
  # Interior chunks start mid-line and end mid-line: drop both partials.
  curl -sS --max-time 900 -r "${off}-$((off + chunk_bytes - 1))" "$BASE" \
    | tail -n +2 | sed '$d' > "$(printf '%s/chunk_%02d.csv' "$OUT_DIR" "$i")"
  offsets="${offsets},${off}"
  printf 'chunk %2d/%d @ %d\n' "$i" "$CHUNKS" "$off" >&2
done

slice_bytes=$(cat "$OUT_DIR"/chunk_*.csv | wc -c | tr -d ' ')
rows=$(cat "$OUT_DIR"/chunk_*.csv | grep -cv '^#' || true)
checksums=$(cd "$OUT_DIR" && for f in chunk_*.csv; do
  printf '"%s":"%s",' "$f" "$(md5 -q "$f" 2>/dev/null || md5sum "$f" | cut -d' ' -f1)"
done)

OFFSETS="$offsets" CHECKSUMS="${checksums%,}" TOTAL="$TOTAL" MD5="$MD5" \
SLICE_BYTES="$slice_bytes" ROWS="$rows" HEAD_BYTES="$head_bytes" \
CHUNK_BYTES="$chunk_bytes" CHUNKS="$CHUNKS" FILE="$FILE" BASE="$BASE" \
DOI="$DOI" LICENSE="$LICENSE" ATTRIBUTION="$ATTRIBUTION" RECORD_ID="$RECORD_ID" \
python3 - "$PROV" <<'PY'
import datetime, json, os, sys

env = os.environ
json.dump({
    "source_url": f"https://zenodo.org/records/{env['RECORD_ID']}",
    "download_url": env["BASE"],
    "doi": env["DOI"],
    "license": env["LICENSE"],
    "attribution": env["ATTRIBUTION"],
    "file": env["FILE"],
    "file_size_bytes": int(env["TOTAL"]),
    "file_checksum": env["MD5"],
    "sampling_method": {
        "kind": "stratified byte ranges",
        "rationale": "a head-only slice covers the pre-open and opening auction only; "
                     "evenly spaced ranges cover the whole session",
        "head_bytes": int(env["HEAD_BYTES"]),
        "chunks": int(env["CHUNKS"]),
        "chunk_bytes": int(env["CHUNK_BYTES"]),
        "offsets": [int(o) for o in env["OFFSETS"].split(",")],
        "partial_line_handling": "interior chunks drop their first line; every chunk drops its last line",
        "ordering_caveat": "source order holds within a chunk only, never across chunks",
    },
    "slice_bytes": int(env["SLICE_BYTES"]),
    "sampled_fraction": round(int(env["SLICE_BYTES"]) / int(env["TOTAL"]), 6),
    "slice_rows_excluding_comments": int(env["ROWS"]),
    "chunk_checksums": json.loads("{" + env["CHECKSUMS"] + "}"),
    "retrieved_utc": datetime.datetime.now(datetime.UTC).isoformat(),
}, open(sys.argv[1], "w"), indent=2)
PY

echo "slice: $OUT_DIR ($slice_bytes bytes, $rows rows)" >&2
