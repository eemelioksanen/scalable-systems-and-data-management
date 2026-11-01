"""Prepare small adversarial inputs and independent references without changing the full week."""
from __future__ import annotations

import json
import shutil
from pathlib import Path

from ema_check.inject import Options, inject, price_event
from ema_oracle.run import Config, run

ROOT = Path(__file__).resolve().parents[3]
DATA = ROOT / 'data/issue9'
OUT = ROOT / 'oracle/out/issue9'
FIELDS = ['symbol', 'secType', 'windowEnd', 'close', 'ema38', 'ema100']


def ties(files):
    groups = {}
    for path in files:
        for line in path.read_bytes().splitlines():
            event = price_event(line)
            if event:
                groups.setdefault(event, []).append(line)
    return {k: rows for k, rows in groups.items() if len(rows) > 1}


def main():
    source = DATA / 'semantics'
    source.mkdir(parents=True, exist_ok=True)
    for path in (ROOT / 'fixtures/semantics').glob('*.csv'):
        shutil.copyfile(path, source / path.name)
    files = sorted(source.glob('*.csv'))
    run(Config(files, OUT / 'semantics', command=['fixtures.py', 'semantics']))
    injected = DATA / 'disorder'
    injection = inject(files, injected, Options(disorder_ms=300000, seed=9))
    reordered = sorted(injected.glob('*.csv'))
    assert ties(files) == ties(reordered)
    run(Config(reordered, OUT / 'disorder', command=['fixtures.py', 'disorder']))
    evidence = ROOT / 'docs/validation/2026-11-01-full-week'
    (evidence / 'injection.json').write_text(json.dumps(dict(**injection, equalTimestampTiesPreserved=True), indent=2) + '\n')


if __name__ == '__main__':
    main()
