"""Check every full-day byte against the retained dataset provenance."""
import hashlib
import json
import sys
from pathlib import Path

root = Path(__file__).resolve().parents[3]
source = Path(sys.argv[1])
out = root / 'docs/validation/2026-11-01-full-week/inputs.json'
provenance = json.loads(out.with_name('provenance.json').read_text())
assert len(provenance) == 7
assert {p.name for p in source.glob('*.csv')} == {p['file'] for p in provenance}
results = []
for item in provenance:
    path = source / item['file']
    md5, sha = hashlib.md5(), hashlib.sha256()
    with path.open('rb') as handle:
        for block in iter(lambda: handle.read(8 << 20), b''):
            md5.update(block)
            sha.update(block)
    result = dict(file=path.name, bytes=path.stat().st_size, md5=md5.hexdigest(), sha256=sha.hexdigest())
    assert result['bytes'] == item['size_bytes'], result
    assert 'md5:' + result['md5'] == item['published_checksum'], result
    assert result['sha256'] == item['sha256'], result
    results.append(result)
    print(path.name, 'verified', flush=True)
out.write_text(json.dumps(dict(source=str(source), files=results, passed=True), indent=2) + '\n')
