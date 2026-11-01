"""Reconcile retained raw runs, check deterministic values, and retain small review evidence."""
from __future__ import annotations

import hashlib
import json
import re
import shutil
import subprocess
from array import array
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[3]
RAW = ROOT / 'correctness/out/issue9'
EVIDENCE = ROOT / 'docs/validation/2026-11-01-full-week'
TABLES = {
    'ema_results.parquet': ['symbol', 'secType', 'windowEnd', 'close', 'ema38', 'ema100'],
    'advisories.parquet': ['symbol', 'secType', 'windowEnd', 'type', 'ema38', 'ema100'],
}


def load(path):
    return json.loads(path.read_text())


def digest(path):
    frame = pl.read_parquet(path, columns=TABLES[path.name]).sort(['symbol', 'windowEnd'])
    checksum = hashlib.sha256()
    for field in frame.columns:
        values = frame[field].to_list()
        if frame[field].dtype == pl.Float64:
            checksum.update(array('d', values).tobytes())
        elif frame[field].dtype == pl.Int64:
            checksum.update(array('q', values).tobytes())
        else:
            checksum.update('\n'.join(values).encode())
    return checksum.hexdigest()


def retain(source, target):
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, target)


def summarize(name, reference):
    run = RAW / name
    replay, flink, export = (load(run / p) for p in ['replay.json', 'flink-job.json', 'export/manifest.json'])
    oracle = load(ROOT / 'oracle/out/issue9' / reference / 'manifest.json')
    vertices = flink['vertices']
    def total(fragment, metric):
        selected = [v for n, v in vertices.items() if fragment in n]
        assert len(selected) == 1, list(vertices)
        return sum(s['metrics'][metric] for s in selected[0]['subtasks'])
    checks = {
        'sevenWholeInputs': len(replay['files']) == 7,
        'acknowledgedEqualsOracleDataRows': replay['counts']['acknowledged'] == oracle['counts']['total']['dataRows'],
        'freshInputOffsets': all(p['startOffset'] == 0 and p['endOffset'] == p['records'] for p in replay['offsets']),
        'allFilesComplete': all(f['complete'] for f in replay['files']),
        'noReplaySelection': not any(replay['selection'].values()),
        'acknowledgedEqualsRead': replay['counts']['acknowledged'] == total('Source: ticks', 'read-records'),
        'sourcePricesEqualOracle': total('Source: ticks', 'write-records') == oracle['counts']['total']['accepted'],
        'pricesReachQ1': total('Source: ticks', 'write-records') == total('q1-ema', 'read-records'),
        'q1EqualsAllWindowsPlusLate': total('q1-ema', 'write-records') == oracle['results']['windows'] + export['topics']['late-ticks']['records'],
        'sinkEqualsExports': total('subscription-filter', 'write-records') == sum(t['records'] for t in export['topics'].values()),
        'jobFinished': flink['job']['state'] == 'FINISHED',
        'exportComplete': all(t['complete'] for t in export['topics'].values()),
        'admitAll': export['topics']['late-ticks']['records'] == oracle['counts']['total']['excluded'] == oracle['counts']['total']['lateRejected'] == 0,
    }
    comparison = load(run / 'compare/report.json')
    checks['comparisonPasses'] = comparison['pass']
    assert all(checks.values()), checks
    values = {file: digest(run / 'export' / file) for file in TABLES}
    expected = {file: digest(ROOT / 'oracle/out/issue9' / reference / file) for file in TABLES}
    assert values == expected, (values, expected)
    for path in ['config.json', 'replay.json', 'flink-job.json', 'export/manifest.json', 'compare/report.json', 'commands.jsonl']:
        retain(run / path, EVIDENCE / 'runs' / name / path)
    summary = dict(checks=checks, rows=replay['counts']['acknowledged'], prices=total('q1-ema', 'read-records'),
                   outputs={k: v['records'] for k, v in export['topics'].items()}, digests=values,
                   exactOracleValues=True)
    (EVIDENCE / 'runs' / name / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
    return summary


def main():
    maven = (RAW / 'maven.log').read_text()
    assert 'BUILD SUCCESS' in maven
    totals = re.findall(r'^\[INFO\] Tests run: (\d+), Failures: 0, Errors: 0, Skipped: 0\s*$', maven, re.M)
    java_tests = sum(map(int, totals))
    python_tests = int(re.search(r'(\d+) passed in ', (RAW / 'python-tests.log').read_text())[1])
    assert java_tests == 168 and python_tests == 75
    edge = load(EVIDENCE / 'edge-cases.json')
    assert edge['passed'] and edge['disorderInvariant']
    controls = load(EVIDENCE / 'negative-controls.json')
    assert set(controls) == {'missing', 'duplicate', 'ema'} and all(not r['pass'] for r in controls.values())
    idle = load(RAW / 'idle/config.json')
    assert (idle['completedBeforeDrain'], idle['completedAfterDrain'], idle['emptyPartitions']) == (2, 3, 7)
    assert load(RAW / 'idle/compare/report.json')['pass']
    subprocess.run(['git', 'diff', '--exit-code', '35eb8f69e9d95775256a994469bb333f36e61423', 'HEAD', '--', 'flink-job/src', 'producer/src', 'correctness/ema_check'], cwd=ROOT, check=True)
    late = load(EVIDENCE.parent / '2026-11-01-late-events/verification.json')
    assert late['liveIdentitiesMatchInjector'] and late['liveLateCount'] == 13 and late['liveComparisonPass']
    results = {name: summarize(name, 'full-week') for name in ['week-1', 'week-2']}
    assert results['week-1']['digests'] == results['week-2']['digests']
    retain(ROOT / 'oracle/out/issue9/full-week/manifest.json', EVIDENCE / 'oracle/full-week.json')
    original = pl.read_parquet(ROOT / 'oracle/out/issue9/full-week/ema_results.parquet')
    original.group_by('exchange').agg(pl.len().alias('windows'), pl.col('symbol').n_unique().alias('symbols'),
                                     pl.col('priceEvents').sum()).sort('exchange').write_csv(EVIDENCE / 'exchanges.csv')
    original.with_columns(day=pl.from_epoch(pl.col('windowEnd') - 1 + 7_200_000, time_unit='ms').dt.date()).group_by('day').agg(
        pl.len().alias('windows'), pl.col('symbol').n_unique().alias('symbols'), pl.col('priceEvents').sum()).sort('day').write_csv(EVIDENCE / 'days.csv')
    symbol_days = original.with_columns(day=(pl.col('windowEnd') - 1 + 7_200_000) // 86_400_000).group_by('symbol').agg(pl.col('day').n_unique().alias('days'), pl.len().alias('windows'))
    coverage = dict(weekend={file: counts for file, counts in load(ROOT / 'oracle/out/issue9/full-week/manifest.json')['counts']['perFile'].items() if '-13-' in file or '-14-' in file}, sparseSymbolsWithOneWindow=symbol_days.filter(pl.col('windows') == 1).height, symbolsAcrossSeveralDays=symbol_days.filter(pl.col('days') > 1).height)
    (EVIDENCE / 'coverage.json').write_text(json.dumps(coverage, indent=2) + '\n')
    for name in ['semantics', 'disorder', 'subset', 'idle']:
        for relative in ['config.json', 'replay.json', 'flink-job.json', 'export/manifest.json', 'compare/report.json', 'commands.jsonl', 'before-drain.json']:
            source = RAW / name / relative
            if source.exists():
                retain(source, EVIDENCE / 'runs' / name / relative)
        source = ROOT / 'oracle/out/issue9' / name / 'manifest.json'
        if source.exists():
            retain(source, EVIDENCE / 'oracle' / (name + '.json'))
    retain(RAW / 'build.json', EVIDENCE / 'build.json')
    for file in ['images.txt', 'podman-version.txt', 'compose-version.txt', 'vm-resources.txt', 'maven.log', 'python-tests.log']:
        retain(RAW / file, EVIDENCE / 'environment' / file)
    checksums = []
    for base in [RAW, ROOT / 'oracle/out/issue9']:
        for path in sorted(base.rglob('*')):
            if path.is_file():
                checksum = hashlib.sha256()
                with path.open('rb') as handle:
                    for block in iter(lambda: handle.read(4 << 20), b''):
                        checksum.update(block)
                checksums.append(dict(path=str(path.relative_to(ROOT)), bytes=path.stat().st_size, sha256=checksum.hexdigest()))
    (EVIDENCE / 'raw-checksums.json').write_text(json.dumps(checksums, indent=2) + '\n')
    (EVIDENCE / 'verification.json').write_text(json.dumps(dict(passed=True, fullWeek=True, cleanReplay=True, handDerivedEdges=True, subsetPublication=True, boundedDisorder=True, idlePartitions=True, finalWindow=True, negativeControls=True, lateInjection='Reused unchanged parent runtime evidence: ../2026-11-01-late-events/verification.json', tests=dict(java=java_tests, python=python_tests)), indent=2) + '\n')
    (EVIDENCE / 'full-week.json').write_text(json.dumps(dict(passFullWeek=True, deterministicCleanReplay=True, runs=results), indent=2) + '\n')


if __name__ == '__main__':
    main()
