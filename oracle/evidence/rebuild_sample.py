"""Rebuild the #4 stratified sample from a local copy of the full weekday file.

fetch_slice.sh downloads byte ranges from Zenodo; this cuts the same ranges from the file
already on disk, using the offsets recorded in analysis/out/provenance.json, and checks
every chunk against the MD5 recorded there. The chunks are concatenated in order into one
file, so the oracle reads the sample as one day file.

Run from the repository root:  python3 oracle/evidence/rebuild_sample.py [DATA_DIR]
"""

import hashlib
import json
import sys
from pathlib import Path

data = Path(sys.argv[1] if len(sys.argv) > 1 else "data")
prov = json.loads(Path("analysis/out/provenance.json").read_text())
sm = prov["sampling_method"]


def drop_last_line(b: bytes) -> bytes:  # sed '$d'
    b = b[:-1] if b.endswith(b"\n") else b
    return b[: b.rfind(b"\n") + 1]


parts = []
with (data / prov["file"]).open("rb") as f:
    for i, offset in enumerate(sm["offsets"]):
        f.seek(offset)
        b = f.read(sm["head_bytes"] if i == 0 else sm["chunk_bytes"])
        if i:
            b = b[b.index(b"\n") + 1 :]  # tail -n +2
        b = drop_last_line(b)
        name = f"chunk_{i:02d}.csv"
        if hashlib.md5(b).hexdigest() != prov["chunk_checksums"][name]:
            sys.exit(f"{name}: MD5 differs from provenance.json")
        parts.append(b)

out = data / "slice" / prov["file"].replace(".csv", ".slice.csv")
out.parent.mkdir(exist_ok=True)
out.write_bytes(b"".join(parts))
print(f"{len(parts)} chunks match provenance.json; wrote {out} ({out.stat().st_size} bytes)")
