"""Prove the existing gate rejects missing, duplicated and numerically wrong actual outputs."""
from __future__ import annotations

import json
import shutil
from pathlib import Path
import polars as pl
from ema_check.compare import Inputs, compare

ROOT = Path(__file__).resolve().parents[3]
RAW = ROOT / 'correctness/out/issue9'
results = {}
for name in ['missing', 'duplicate', 'ema']:
    folder = RAW / ('control-' + name)
    shutil.copytree(RAW / 'semantics/export', folder)
    path = folder / 'ema_results.parquet'
    frame = pl.read_parquet(path)
    if name == 'missing':
        frame = frame.slice(1)
    elif name == 'duplicate':
        frame = pl.concat([frame, frame.head(1)])
    else:
        frame = frame.with_row_index().with_columns(
            pl.when(pl.col('index') == 0).then(pl.col('ema38') + 0.01).otherwise(pl.col('ema38')).alias('ema38')).drop('index')
    frame.write_parquet(path)
    report = compare(Inputs(oracle=ROOT / 'oracle/out/issue9/semantics', job=folder,
                           out=folder / 'compare', job_late_ticks=0, job_late_events=folder / 'late_events.parquet'))
    assert report['pass'] is False
    if name == 'missing':
        assert report['ema']['byKind']['oracleOnly'] == 1
    elif name == 'duplicate':
        assert report['ema']['duplicateRows']['job'] > 0
    else:
        assert report['ema']['maxAbsDiffBeyondTolerance']['ema38'] > 0.009
    results[name] = report
(ROOT / 'docs/validation/2026-11-01-full-week/negative-controls.json').write_text(json.dumps(results, indent=2) + '\n')
