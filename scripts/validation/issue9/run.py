"""Isolated correctness runs. Requires built jars and the cached project images."""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import subprocess
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / 'correctness/out/issue9'
COMPOSE = ROOT / 'scripts/validation/issue9/compose.yml'
PROJECT = 'issue9'
NETWORK = PROJECT + '_default'
PORT = os.environ.get('ISSUE9_FLINK_PORT', '8109')
ENV = dict(os.environ, ISSUE9_ROOT=str(ROOT))


def save(path, obj):
    path.write_text(json.dumps(obj, indent=2) + '\n')


def command(args, directory, *, stdin=None):
    args = [str(a) for a in args]
    with (directory / 'commands.jsonl').open('a') as log:
        log.write(json.dumps(dict(utc=dt.datetime.now(dt.timezone.utc).isoformat(), argv=args)) + '\n')
    with (directory / 'stderr.log').open('a') as log:
        result = subprocess.run(args, cwd=ROOT, env=ENV, input=stdin, text=True, stdout=subprocess.PIPE,
                                stderr=log, timeout=14400)
    if result.returncode:
        raise RuntimeError(f'{args} exited {result.returncode}: {result.stdout[-2000:]}')
    return result.stdout


def compose(args, directory, **kwargs):
    return command(['podman', 'compose', '-p', PROJECT, '-f', COMPOSE, *args], directory, **kwargs)


def rest(path):
    with urllib.request.urlopen(f'http://127.0.0.1:{PORT}/{path}', timeout=20) as reply:
        return json.load(reply)


def wait(read, ready, timeout=7200):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            last = read()
            if ready(last):
                return last
        except (OSError, ValueError, RuntimeError):
            pass
        time.sleep(2)
    raise TimeoutError(str(last))


def python_container(module, args, directory, *, dataset=None, network='none', memory='4g'):
    image = 'localhost/trading-trends/' + ('oracle' if module == 'ema_oracle' else 'correctness') + ':dev'
    mounts = ['-v', f'{ROOT}:/repo', '-v', f'{ROOT}/oracle:/oracle:ro',
              '-v', f'{ROOT}/correctness:/correctness:ro', '-v', f'{ROOT}/fixtures:/fixtures:ro']
    if dataset:
        mounts += ['-v', f'{dataset}:/data:ro']
    return command(['podman', 'run', '--rm', '--memory', memory, '--cpus', '2', '--network', network,
                    '-e', 'POLARS_MAX_THREADS=2', '-e', 'PYTHONPATH=/oracle:/correctness',
                    '-e', 'ORACLE_GIT_REV=' + subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
                    '-e', 'ORACLE_GIT_DIRTY=0', *mounts, image, '-m', module, *args], directory)


def oracle(name, dataset, inputs):
    directory = OUT / name
    directory.mkdir(parents=True, exist_ok=False)
    output = f'/repo/oracle/out/issue9/{name}'
    result = python_container('ema_oracle', [*[f'/data/{p}' for p in inputs], '--out', output,
                                            '--zone-offset', '+02:00'], directory, dataset=dataset, memory='5g')
    (directory / 'oracle-stdout.json').write_text(result)


def setup(directory):
    compose(['down', '--volumes'], directory)
    compose(['up', '-d'], directory)
    wait(lambda: rest('overview'), lambda j: j['slots-total'] == 2, 180)
    kafka = ['/opt/kafka/bin/kafka-topics.sh', '--bootstrap-server', 'kafka:9092']
    wait(lambda: compose(['exec', '-T', 'kafka', *kafka, '--list'], directory), lambda _: True, 180)
    for topic in ['ticks', 'ema-results', 'advisories', 'late-ticks', 'subscriptions']:
        compose(['exec', '-T', 'kafka', *kafka, '--create', '--topic', topic, '--partitions',
                 '1' if topic == 'subscriptions' else '8', '--config', 'retention.ms=-1'], directory)


def run(name, dataset, inputs, oracle_name, subscriptions, bound):
    directory = OUT / name
    directory.mkdir(parents=True, exist_ok=False)
    setup(directory)
    lines = ''.join(f'{s["trader"]}|{json.dumps(s)}\n' for s in subscriptions)
    compose(['exec', '-T', 'kafka', '/opt/kafka/bin/kafka-console-producer.sh', '--bootstrap-server',
             'kafka:9092', '--topic', 'subscriptions', '--reader-property', 'parse.key=true',
             '--reader-property', 'key.separator=|'], directory, stdin=lines)
    replay = command(['podman', 'run', '--rm', '--name', 'issue9-replayer', '--memory', '768m',
                      '--network', NETWORK, '-e', 'BOOTSTRAP_SERVERS=kafka:9092',
                      '-v', f'{ROOT}/producer/target/producer.jar:/app/producer.jar:ro',
                      '-v', f'{dataset}:/data:ro', 'docker.io/library/eclipse-temurin:17-jre',
                      'java', '-Xmx512m', '-cp', '/app/producer.jar', 'fi.aalto.cse4780.replay.Replayer',
                      *[a for p in inputs for a in ['--input', '/data/' + p]],
                      '--mode', 'preload', '--run-id', name], directory)
    (directory / 'replay.json').write_text(replay)
    report = json.loads(replay)
    assert report['status'] == 'COMPLETE'
    job_args = ['--bounded', '--watermark-bound-ms', str(bound), '--idleness-ms', '0', '--zone-offset', '+02:00']
    submission = compose(['exec', '-T', 'jobmanager', 'flink', 'run', '-d', '-p', '2', '-c',
                          'fi.aalto.cse4780.trading.TradingTrendsJob', '/opt/job/flink-job.jar', *job_args], directory)
    job_id = re.search(r'JobID ([0-9a-f]{32})', submission)[1]
    job = wait(lambda: rest('jobs/' + job_id), lambda j: j['state'] in ['FINISHED', 'FAILED', 'CANCELED'])
    vertices = {v['name']: rest(f'jobs/{job_id}/vertices/{v["id"]}') for v in job['vertices']}
    save(directory / 'flink-job.json', dict(job=job, vertices=vertices))
    assert job['state'] == 'FINISHED', rest(f'jobs/{job_id}/exceptions')
    export_dir = f'/repo/correctness/out/issue9/{name}/export'
    exported = python_container('ema_check', ['export', '--bootstrap-servers', 'kafka:9092', '--zone-offset',
                               '+02:00', '--replay-report', f'/repo/correctness/out/issue9/{name}/replay.json',
                               '--out', export_dir], directory, network=NETWORK, memory='2g')
    (directory / 'export-stdout.json').write_text(exported)
    manifest = json.loads(exported)
    late = manifest['topics']['late-ticks']['records']
    save(directory / 'config.json', dict(inputs=inputs, dataset=str(dataset), jobArgs=job_args,
                                       subscriptions=subscriptions, project=PROJECT, partitions=8, parallelism=2))
    if oracle_name:
        compared = python_container('ema_check', ['compare', '--oracle', '/repo/oracle/out/issue9/' + oracle_name,
                        '--job', export_dir, '--job-late-ticks', str(late), '--job-late-events', export_dir + '/late_events.parquet',
                        '--out', f'/repo/correctness/out/issue9/{name}/compare'], directory)
        (directory / 'compare-stdout.json').write_text(compared)
    (directory / 'stack.log').write_text(compose(['logs', '--no-color'], directory))
    compose(['down', '--volumes'], directory)
    print(name, 'completed', flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['oracle', 'run', 'down'])
    parser.add_argument('--name', required=True)
    parser.add_argument('--dataset', type=Path)
    parser.add_argument('--input', nargs='+')
    parser.add_argument('--oracle')
    parser.add_argument('--bound', type=int, default=64265001)
    parser.add_argument('--symbols', nargs='+', default=['*'])
    args = parser.parse_args()
    if args.action == 'oracle':
        oracle(args.name, args.dataset.resolve(), args.input)
    elif args.action == 'run':
        run(args.name, args.dataset.resolve(), args.input, args.oracle, [dict(trader='issue9', symbols=args.symbols)], args.bound)
    else:
        OUT.mkdir(parents=True, exist_ok=True)
        compose(['down', '--volumes'], OUT)


if __name__ == '__main__':
    main()
