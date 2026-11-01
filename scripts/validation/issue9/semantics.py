"""Assert fixture identities, boundaries, gaps and carried state using hand-derived expectations."""
from __future__ import annotations

import json
from pathlib import Path
import polars as pl

ROOT = Path(__file__).resolve().parents[3]
EXPECTED = json.loads((ROOT / 'fixtures/semantics/expected.json').read_text())
RAW = ROOT / 'correctness/out/issue9'
EVIDENCE = ROOT / 'docs/validation/2026-11-01-full-week'
FIELDS = ['symbol', 'secType', 'windowEnd', 'close', 'ema38', 'ema100']


def expected_table(key):
    return pl.DataFrame(EXPECTED[key]).with_columns(pl.col('windowEnd') - 7_200_000).sort(['symbol', 'windowEnd'])


def main():
    expected = expected_table('windows')
    actual = pl.read_parquet(RAW / 'semantics/export/ema_results.parquet').sort(['symbol', 'windowEnd'])
    assert actual.select(FIELDS[:4]).equals(expected.select(FIELDS[:4]))
    for field in FIELDS[4:]:
        assert (actual[field] - expected[field]).abs().max() <= 1e-9
    adv_fields = ['symbol', 'secType', 'windowEnd', 'type']
    advisories = pl.read_parquet(RAW / 'semantics/export/advisories.parquet').sort(['symbol', 'windowEnd'])
    assert advisories.select(adv_fields).equals(expected_table('advisories').select(adv_fields))
    reordered = pl.read_parquet(RAW / 'disorder/export/ema_results.parquet').sort(['symbol', 'windowEnd'])
    assert actual.select(FIELDS).equals(reordered.select(FIELDS))
    reordered_adv = pl.read_parquet(RAW / 'disorder/export/advisories.parquet').sort(['symbol', 'windowEnd'])
    assert advisories.select(adv_fields + ['ema38', 'ema100']).equals(reordered_adv.select(adv_fields + ['ema38', 'ema100']))
    original_oracle = pl.read_parquet(ROOT / 'oracle/out/issue9/semantics/ema_results.parquet').sort(['symbol', 'windowEnd'])
    reordered_oracle = pl.read_parquet(ROOT / 'oracle/out/issue9/disorder/ema_results.parquet').sort(['symbol', 'windowEnd'])
    assert original_oracle.select(FIELDS).equals(reordered_oracle.select(FIELDS))
    selected = ['EDGE.NL', 'SWING.FR']
    subset = pl.read_parquet(RAW / 'subset/export/ema_results.parquet').sort(['symbol', 'windowEnd'])
    assert subset.select(FIELDS).equals(actual.filter(pl.col('symbol').is_in(selected)).select(FIELDS))
    subset_adv = pl.read_parquet(RAW / 'subset/export/advisories.parquet').sort(['symbol', 'windowEnd'])
    assert subset_adv.select(adv_fields + ['ema38', 'ema100']).equals(advisories.filter(pl.col('symbol').is_in(selected)).select(adv_fields + ['ema38', 'ema100']))
    assert subset.select('symbol', 'windowEnd').n_unique() == subset.height
    assert subset_adv.select(adv_fields).n_unique() == subset_adv.height
    assert not pl.read_parquet(RAW / 'subset/export/late_events.parquet').height
    reconciliation = {}
    for name in ['semantics', 'disorder', 'subset']:
        folder = RAW / name
        job = json.loads((folder / 'flink-job.json').read_text())
        export = json.loads((folder / 'export/manifest.json').read_text())
        assert job['job']['state'] == 'FINISHED'
        assert all(t['complete'] for t in export['topics'].values())
        q1 = next(v for n, v in job['vertices'].items() if 'q1-ema' in n)
        read = sum(s['metrics']['read-records'] for s in q1['subtasks'])
        written = sum(s['metrics']['write-records'] for s in q1['subtasks'])
        assert read == 71 and written == actual.height
        reconciliation[name] = dict(priceEvents=read, evaluatedWindows=written, topics=export['topics'])
    (EVIDENCE / 'edge-cases.json').write_text(json.dumps(dict(
        passed=True, reconciliation=reconciliation, handDerivedWindows=actual.height, handDerivedAdvisories=advisories.height,
        subsetSymbols=selected, subsetWindows=subset.height, subsetAdvisories=subset_adv.height,
        disorderInvariant=True, equalTimestampClose=actual.filter(pl.col('symbol') == 'TIE.ETR')['close'].item(),
        cases=['exact boundary and millisecond straggler', 'equal timestamps after millisecond truncation',
               'sparse windows without decay', 'missing trading date and time', 'stale prior-day close',
               'simultaneously open windows', 'overnight EMA carry', 'finite final window',
               'publication filters both result and advisory identities']), indent=2) + '\n')


if __name__ == '__main__':
    main()
