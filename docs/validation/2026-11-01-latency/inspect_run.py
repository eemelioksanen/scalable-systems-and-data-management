"""Recheck a retained live run and expose model residuals: python inspect_run.py RUN_DIR."""
import csv
import json
import sys
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / 'correctness'))
from ema_check.latency import Feed, Watermark, prometheus_samples, stats

run = Path(sys.argv[1])
config = json.loads((run / 'config.json').read_text())
summary = json.loads((run / 'summary.json').read_text())
latency = summary['latency']
manifest = json.loads((run / 'export/manifest.json').read_text())
replay = json.loads((run / 'replay.json').read_text())
assert config['status'] == 'COMPLETE'
assert summary['reconciliation']['ok']
assert all(check['ok'] for check in summary['reconciliation']['checks'])
assert latency['complete'] and latency['measured'] > 0
assert latency['watermarkLag']['samples'] > 0
assert all(topic['complete'] for topic in manifest['topics'].values())
assert manifest['topics']['ticks']['rows'] == latency['model']['priceEvents']
assert latency['exported'] == latency['measured'] + sum(latency['excluded'].values())
rows = list(csv.DictReader((run.parent / 'runs.csv').open()))
row = next(row for row in rows if row['run'] == config['runId'])
assert int(row['latencyResults']) == latency['measured']
assert int(row['watermarkSamples']) == latency['watermarkLag']['samples']
for stage in ('emitLatency', 'outputLatency', 'windowWait', 'boundWait', 'alignmentWait', 'watermarkWait', 'queueing', 'sinkHandoff'):
    for q in (50, 95, 99):
        assert float(row[f'{stage}P{q}Ms']) == latency['stagesMs'][stage][f'p{q}']
assert all(p['records'] == p['endOffset'] - p['startOffset'] for p in manifest['topics']['ticks']['partitions'].values())

events = pl.read_parquet(run / 'export/price_events.parquet')
results = pl.read_parquet(run / 'export/ema_results.parquet')
settings = config['effective']
feed = Feed(events)
model = Watermark(events, latency['model']['partitions'], settings['watermarkBoundMs'],
                  settings['idlenessMs'], config['times']['jobRunning'])
start = summary['measurementInterval']['startMs']
end = summary['measurementInterval']['endMs']
measured = results.filter((pl.col('maxIngestTs') >= start) & (pl.col('maxIngestTs') <= end)
                          & (pl.col('emitTs') < latency['drainStartMs']))
residuals = []
for row in measured.iter_rows(named=True):
    ready = model.passed(row['windowEnd'] - 1)
    if ready is not None:
        aligned = max(row['maxIngestTs'], feed.reached(row['windowEnd']),
                      feed.reached(row['windowEnd'] + settings['watermarkBoundMs']), ready)
        residuals.append({**{k: row[k] for k in ('symbol', 'windowEnd', 'maxIngestTs', 'emitTs')},
                          'modelReadyMs': ready, 'queueingMs': row['emitTs'] - aligned})
negative = [row for row in residuals if row['queueingMs'] < 0]
metrics = json.loads((run / 'metrics.json').read_text())
samples = prometheus_samples(metrics, 'q1InputWatermark', 'q1InputWatermarkScrapeTs')
residual_wall = []
ahead_event = []
for at, actual in samples:
    predicted = model.at(at)
    if start <= at <= end and actual > 0 and predicted is not None:
        ahead_event.append(predicted - actual)
        ready = model.passed(actual)
        if ready is not None:
            residual_wall.append(at - ready)
unique_samples = sorted(set(samples))
large_jumps = []
for i, (at, actual) in enumerate(unique_samples):
    predicted = model.at(at)
    if start <= at <= end and actual > 0 and predicted is not None and predicted - actual > 100_000:
        large_jumps.append({'sampleMs': at, 'actual': actual, 'model': predicted,
                            'aheadEventMs': predicted - actual,
                            'modelAdvancedBeforeSampleMs': at - model.passed(predicted),
                            'nextSample': unique_samples[i + 1] if i + 1 < len(unique_samples) else None})
partitions = []
for p in latency['model']['partitions']:
    subset = events.filter(pl.col('partition') == p).sort('kafkaTs')
    partitions.append({'partition': p, 'prices': subset.height,
                       'maxAdmissionGapMs': subset['kafkaTs'].diff().max() if subset.height > 1 else None})
report = {
    'verified': True, 'partitions': partitions,
    'distinctWatermarkScrapes': len({at for at, value in samples if start <= at <= end and value > 0}),
    'largeModelJumps': large_jumps,
    'queueing': {**stats([r['queueingMs'] for r in residuals]),
                 'min': min(r['queueingMs'] for r in residuals)},
    'negativeQueueing': {'count': len(negative), 'examples': sorted(negative, key=lambda r: r['queueingMs'])[:10]},
    'modelAheadEventMs': {**stats(ahead_event), 'min': min(ahead_event),
                          'negative': sum(v < 0 for v in ahead_event)},
    'watermarkSampleAgeSinceModelReadyMs': {**stats(residual_wall), 'min': min(residual_wall)},
    'excludedBySymbol': {
        'warmup': results.filter(pl.col('maxIngestTs') < start).group_by('symbol').len().sort('symbol').to_dicts(),
        'drain': results.filter((pl.col('maxIngestTs') >= start) & (pl.col('maxIngestTs') <= end)
                                & (pl.col('emitTs') >= latency['drainStartMs'])).select('symbol', 'windowEnd', 'maxIngestTs', 'emitTs').sort('windowEnd', 'symbol').to_dicts(),
    },
    'replay': {'counts': replay['counts'], 'timing': replay['timing'], 'pacing': replay['pacing']},
}
print(json.dumps(report, indent=2))
