"""Run the job with only --bounded on a preloaded stack and compare it with the oracle.

The harness states the bound and idleness in every run, so it never runs the job's own defaults (D-38). This
submits the jar with --bounded alone, to fresh output topics, on the stack a harness preload has left up with
its `ticks` and subscriptions. A bounded job's counters vanish when it finishes, so lateTicks is sampled from
every TaskManager's Prometheus endpoint until the job ends, keeping each subtask's last values. Their sum is
final when it shows q1-ema having received every price event that Flink's record of the job counts.

Run from the repository root, after a preload of the oracle's input:

    python3 docs/validation/2026-10-13-weekday/bounded_defaults.py --oracle oracle/out/monday --name monday-bounded-defaults

The export, the comparison and record.json go to correctness/out/weekday/<name>/.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
FLINK = "http://127.0.0.1:" + os.environ.get("FLINK_HOST_PORT", "8081")
JOB_CLASS = "fi.aalto.cse4780.trading.TradingTrendsJob"
JAR = "/opt/job/flink-job.jar"
SAMPLE_SLEEP_MS = 100
METRIC = re.compile(r'^flink_taskmanager_job_task_operator_(lateTicks|numRecordsIn)\{(.*)\} (\S+)$')


def sh(*args: str, check: bool = True) -> str:
    result = subprocess.run(args, cwd=ROOT, capture_output=True, text=True)
    if check and result.returncode:
        raise RuntimeError(f"exit {result.returncode}: {' '.join(args)}\n{result.stderr[-2000:]}")
    return result.stdout


def compose(*args: str) -> str:
    return sh("docker", "compose", *args)


def flink(path: str):
    with urllib.request.urlopen(f"{FLINK}{path}", timeout=30) as response:
        return json.loads(response.read())


def kafka_topics(*args: str) -> str:
    return compose("exec", "-T", "kafka", "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "kafka:9092", *args)


def sampler(hosts: list[str]) -> subprocess.Popen:
    """One shell in the jobmanager container that prints every TaskManager's q1-ema counters, then a marker line."""
    fetch = "; ".join(f"curl -s -m 2 http://{h}:9249/metrics | grep -E '^flink_taskmanager_job_task_operator_"
                      f"(lateTicks|numRecordsIn)[{{]'" for h in hosts)
    loop = f"while true; do {fetch}; echo '#'; sleep {SAMPLE_SLEEP_MS / 1000}; done"
    return subprocess.Popen(["docker", "compose", "exec", "-T", "jobmanager", "bash", "-c", loop], cwd=ROOT,
                            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)


def read_samples(process: subprocess.Popen, job_id: str, ended) -> tuple[int, dict | None]:
    """Reads samples until the job has ended; returns their number and the sum of each subtask's last counters.

    A subtask's counters vanish when it finishes, and the subtasks do not finish together, so one sample
    seldom holds every subtask's final values. The counters only grow, so each subtask's last sample is its latest.
    """
    samples, subtasks = 0, {}
    for line in process.stdout:
        line = line.strip()
        if line == "#":
            samples += 1
            if ended():
                break
            continue
        match = METRIC.match(line)
        if not match or f'job_id="{job_id}"' not in match.group(2) or 'operator_name="q1_ema"' not in match.group(2):
            continue
        subtask = re.search(r'subtask_index="(\d+)"', match.group(2)).group(1)
        name = "lateTicks" if match.group(1) == "lateTicks" else "q1RecordsIn"
        subtasks.setdefault(subtask, {})[name] = int(float(match.group(3)))
    process.terminate()
    if not subtasks:
        return samples, None
    return samples, {"lateTicks": sum(s.get("lateTicks", 0) for s in subtasks.values()),
                     "q1RecordsIn": sum(s.get("q1RecordsIn", 0) for s in subtasks.values()), "subtasks": len(subtasks)}


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--oracle", type=Path, required=True, help="oracle output directory for the preloaded input")
    parser.add_argument("--name", required=True, help="results go to correctness/out/weekday/<name>/")
    parser.add_argument("--zone-offset", default="+02:00", help="the job's default (D-3); recorded by the export")
    parser.add_argument("--note", default="", help="which preload left this stack up")
    args = parser.parse_args()

    out = ROOT / "correctness" / "out" / "weekday" / args.name
    if out.exists():
        sys.exit(f"{out} exists: remove it or choose another --name")
    topics = {"ema": f"ema-results-{args.name}", "advisories": f"advisories-{args.name}"}
    partitions = re.search(r"PartitionCount: (\d+)", kafka_topics("--describe", "--topic", "ticks")).group(1)
    for topic in topics.values():
        kafka_topics("--create", "--topic", topic, "--partitions", partitions)

    overview = flink("/overview")
    hosts = [tm["id"].split(":")[0] for tm in flink("/taskmanagers")["taskmanagers"]]
    command = ["flink", "run", "-d", "-c", JOB_CLASS, JAR, "--bounded",
               "--ema-topic", topics["ema"], "--advisories-topic", topics["advisories"]]
    process = sampler(hosts)
    job_id = re.search(r"JobID ([0-9a-f]{32})", compose("exec", "-T", "jobmanager", *command)).group(1)
    print(f"job {job_id}", flush=True)

    state = {"job": None, "checked": 0.0}

    def ended() -> bool:
        if time.monotonic() - state["checked"] >= 0.5:
            state["job"], state["checked"] = flink(f"/jobs/{job_id}"), time.monotonic()
        return state["job"]["state"] in ("FINISHED", "FAILED", "CANCELED")

    samples, last = read_samples(process, job_id, ended)
    job = flink(f"/jobs/{job_id}")
    vertices = {v["name"]: {"readRecords": v["metrics"]["read-records"], "writeRecords": v["metrics"]["write-records"]}
                for v in job["vertices"]}
    final = last is not None and last["q1RecordsIn"] == vertices["q1-ema"]["readRecords"]

    out.mkdir(parents=True)
    sh("correctness/run.sh", "export", "--zone-offset", args.zone_offset, "--ema-topic", topics["ema"],
       "--advisories-topic", topics["advisories"], "--out", f"/out/weekday/{args.name}/export")
    late = ["--job-late-ticks", str(last["lateTicks"])] if final else []
    report = json.loads(sh("correctness/run.sh", "compare", "--oracle", str(args.oracle), "--job", str(out / "export"),
                           *late, "--out", f"/out/weekday/{args.name}/compare", check=False))

    record = {
        "what": "The job jar submitted with --bounded and no watermark flags, on a stack holding a preloaded input",
        "code": {"revision": sh("git", "rev-parse", "HEAD").strip(),
                 "dirty": bool(sh("git", "status", "--porcelain", "--untracked-files=no").strip())},
        "jarSha256": compose("exec", "-T", "jobmanager", "sha256sum", JAR).split()[0],
        "command": command,
        "stack": {"partitions": int(partitions), "taskmanagers": overview["taskmanagers"],
                  "slots": overview["slots-total"], "parallelism": max(v["parallelism"] for v in job["vertices"]),
                  "note": args.note},
        "oracle": str(args.oracle),
        "job": {"id": job_id, "state": job["state"], "durationMs": job["duration"], "vertices": vertices},
        "counters": {"sleepBetweenSamplesMs": SAMPLE_SLEEP_MS, "samples": samples, "lastPerSubtaskSum": last, "final": final},
    }
    (out / "record.json").write_text(json.dumps(record, indent=2) + "\n")
    print(json.dumps({"counters": record["counters"], "pass": report["pass"], "problems": report["problems"],
                      "emaDifferences": report["ema"]["differences"],
                      "advisoryDifferences": sum(report["advisories"]["byKind"].values())}, indent=2))
    sys.exit(0 if job["state"] == "FINISHED" and report["pass"] else 1)


if __name__ == "__main__":
    main()
