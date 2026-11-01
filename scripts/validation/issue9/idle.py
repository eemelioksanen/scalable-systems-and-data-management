"""Prove an active partition can close windows while seven empty partitions are idle."""
from __future__ import annotations

import datetime as dt
import json
import re
from pathlib import Path

from run import ROOT, OUT, NETWORK, command, compose, python_container, rest, save, setup, wait


def main():
    directory = OUT / 'idle'
    directory.mkdir(parents=True, exist_ok=False)
    dataset = ROOT / 'data/issue9/idle'
    dataset.mkdir(parents=True, exist_ok=True)
    lines = []
    for n in range(3):
        time = dt.datetime(2021, 11, 8, 9) + dt.timedelta(minutes=n * 6)
        fields = [''] * 40
        fields[:4] = ['IDLE.NL', 'E', '08-11-2021', time.strftime('%H:%M:%S.000')]
        lines.extend([','.join(fields)] * 256)
        fields[21], fields[23], fields[26] = str(100 + n), time.strftime('%H:%M:%S.000'), '08-11-2021'
        lines.append(','.join(fields))
    file = 'idle-08-11-21.csv'
    (dataset / file).write_text('\n'.join(lines) + '\n')
    oracle = '/repo/oracle/out/issue9/idle'
    python_container('ema_oracle', ['/data/' + file, '--out', oracle, '--zone-offset', '+02:00'], directory, dataset=dataset, memory='512m')
    setup(directory)
    compose(['exec', '-T', 'kafka', '/opt/kafka/bin/kafka-console-producer.sh', '--bootstrap-server',
             'kafka:9092', '--topic', 'subscriptions', '--reader-property', 'parse.key=true',
             '--reader-property', 'key.separator=|'], directory,
            stdin='idle|{"trader":"idle","symbols":["*"]}\n')
    submitted = compose(['exec', '-T', 'jobmanager', 'flink', 'run', '-d', '-p', '2',
        '-Dexecution.checkpointing.interval=10s', '-c', 'fi.aalto.cse4780.trading.TradingTrendsJob',
        '/opt/job/flink-job.jar', '--watermark-bound-ms', '1000', '--idleness-ms', '1000', '--zone-offset', '+02:00'], directory)
    job_id = re.search(r'JobID ([0-9a-f]{32})', submitted)[1]
    wait(lambda: rest('jobs/' + job_id), lambda j: all(v['status'] == 'RUNNING' for v in j['vertices']), 180)
    replay = command(['podman', 'run', '--rm', '--name', 'issue9-replayer', '--memory', '768m',
        '--network', NETWORK, '-e', 'BOOTSTRAP_SERVERS=kafka:9092',
        '-v', f'{ROOT}/producer/target/producer.jar:/app/producer.jar:ro', '-v', f'{dataset}:/data:ro',
        'docker.io/library/eclipse-temurin:17-jre', 'java', '-Xmx512m', '-cp', '/app/producer.jar',
        'fi.aalto.cse4780.replay.Replayer', '--input', '/data/' + file, '--mode', 'realtime',
        '--rate', '60', '--run-id', 'issue9-idle'], directory)
    (directory / 'replay.json').write_text(replay)
    report = json.loads(replay)
    assert report['status'] == 'COMPLETE' and report['counts']['acknowledged'] == 771
    empty = sum(p['records'] == 0 for p in report['offsets'])
    assert empty == 7
    def vertices():
        job = rest('jobs/' + job_id)
        return {v['name']: rest(f'jobs/{job_id}/vertices/{v["id"]}') for v in job['vertices']}
    def q1_written():
        vertex = next(v for n, v in vertices().items() if 'q1-ema' in n)
        return sum(s['metrics']['write-records'] for s in vertex['subtasks'])
    before = wait(q1_written, lambda n: n == 2, 60)
    save(directory / 'before-drain.json', dict(job=rest('jobs/' + job_id), vertices=vertices(), completedWindows=before))
    compose(['exec', '-T', 'jobmanager', 'flink', 'stop', '--drain', '--savepointPath',
             'file:///tmp/issue9-idle-savepoint', job_id], directory)
    job = wait(lambda: rest('jobs/' + job_id), lambda j: j['state'] in ['FINISHED', 'FAILED'], 120)
    assert job['state'] == 'FINISHED'
    final_vertices = vertices()
    final_q1 = next(v for n, v in final_vertices.items() if 'q1-ema' in n)
    after = sum(s['metrics']['write-records'] for s in final_q1['subtasks'])
    assert after == 3
    save(directory / 'flink-job.json', dict(job=job, vertices=final_vertices))
    export = '/repo/correctness/out/issue9/idle/export'
    python_container('ema_check', ['export', '--bootstrap-servers', 'kafka:9092', '--zone-offset', '+02:00',
        '--replay-report', '/repo/correctness/out/issue9/idle/replay.json', '--out', export], directory, network=NETWORK, memory='512m')
    python_container('ema_check', ['compare', '--oracle', oracle, '--job', export, '--job-late-ticks', '0',
        '--job-late-events', export + '/late_events.parquet', '--out', '/repo/correctness/out/issue9/idle/compare'], directory, memory='512m')
    save(directory / 'config.json', dict(boundMs=1000, idlenessMs=1000, zoneOffset='+02:00', rate=60,
        partitions=8, parallelism=2, completedBeforeDrain=before, completedAfterDrain=after, emptyPartitions=empty))
    (directory / 'stack.log').write_text(compose(['logs', '--no-color'], directory))
    compose(['down', '--volumes'], directory)
    print('idle completed', flush=True)


if __name__ == '__main__':
    main()
