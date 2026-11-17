#!/usr/bin/env python3
"""Runs a benchmark experiment on the Compose stack: reset, replay, job, collection (D-36).

    python3 bench/run.py bench/configs/subset12-preload-p2.json [--repeats N] [--no-build]

Each repeat gets a fresh stack and its own directory under bench/results/raw/<name>-<UTC stamp>/.
bench/README.md describes the config and every output file.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import platform
import re
import shlex
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "bench" / "results" / "raw"
FLINK = "http://127.0.0.1:" + os.environ.get("FLINK_HOST_PORT", "8081")
PROMETHEUS = "http://127.0.0.1:" + os.environ.get("PROMETHEUS_HOST_PORT", "9090")
JOB_CLASS = "fi.aalto.cse4780.trading.TradingTrendsJob"
CHECKPOINTING = ["-Dexecution.checkpointing.interval=10s", "-Drestart-strategy.type=none"]
SCRAPE_S = 5

Q1_WATERMARK = 'flink_taskmanager_job_task_operator_currentInputWatermark{operator_name="q1_ema"}'

# The measurements of interfaces.md, "Metrics (D-35)", grouped as that table describes.
QUERIES = {
    "ticksIntoKafka": 'sum(rate(kafka_server_brokertopicmetrics_messagesin_total{topic="ticks"}[15s]))',
    "ticksReadPerSubtask": 'sum by (subtask_index) (rate(flink_taskmanager_job_task_operator_KafkaSourceReader_KafkaConsumer_records_consumed_total{operator_name="Source:_ticks"}[15s]))',
    "priceEventsPerS": "sum(rate(flink_taskmanager_job_task_operator_deserializer_ticks_priceEvents[15s]))",
    "lag": 'sum(flink_taskmanager_job_task_operator_pendingRecords{operator_name="Source:_ticks"})',
    "lagFromBrokerOffsets": 'sum(kafka_log_logendoffset{topic="ticks"}) - sum(flink_taskmanager_job_task_operator_KafkaSourceReader_topic_partition_currentOffset{topic="ticks"} + 1)',
    "idleMsPerS": "sum by (task_name, subtask_index) (rate(flink_taskmanager_job_task_accumulateIdleTimeMs[15s]))",
    "backPressuredMsPerS": "sum by (task_name, subtask_index) (rate(flink_taskmanager_job_task_accumulateBackPressuredTimeMs[15s]))",
    "q1RecordsInPerSubtask": 'sum by (subtask_index) (flink_taskmanager_job_task_operator_numRecordsIn{operator_name="q1_ema"})',
    "emaStates": "sum by (subtask_index) (flink_taskmanager_job_task_operator_emaStates)",
    "openWindows": "sum by (subtask_index) (flink_taskmanager_job_task_operator_openWindows)",
    "stateBytes": "flink_jobmanager_job_lastCheckpointFullSize",
    "cpuCores": 'max by (name) (rate(container_cpu_usage_seconds_total{name!=""}[15s]))',
    "hostCpuCores": 'rate(container_cpu_usage_seconds_total{id="/"}[15s])',
    "memoryBytes": 'max by (name) (container_memory_working_set_bytes{name!=""})',
    "hostMemoryBytes": 'container_memory_working_set_bytes{id="/"}',
    "flinkHeapBytes": "flink_taskmanager_Status_JVM_Memory_Heap_Used",
    "kafkaHeapBytes": 'jvm_memory_used_bytes{job="kafka", area="heap"}',
    "flinkGcMsPerS": "flink_taskmanager_Status_JVM_GarbageCollector_All_TimeMsPerSecond",
    "kafkaGcSPerS": 'rate(jvm_gc_collection_seconds_sum{job="kafka"}[30s])',
    "networkReceiveBytesPerS": 'sum by (name) (max by (name, interface) (rate(container_network_receive_bytes_total{name!=""}[15s])))',
    "networkTransmitBytesPerS": 'sum by (name) (max by (name, interface) (rate(container_network_transmit_bytes_total{name!=""}[15s])))',
    "topicBytesInPerS": "sum by (topic) (rate(kafka_server_brokertopicmetrics_bytesin_total[15s]))",
    "topicBytesOutPerS": "sum by (topic) (rate(kafka_server_brokertopicmetrics_bytesout_total[15s]))",
    "brokerRequestHandlerIdle": "kafka_server_request_handler_avg_idle_ratio",
    "brokerNetworkIdle": "kafka_network_processor_avg_idle_ratio",
    "q1InputWatermark": Q1_WATERMARK,
    "q1InputWatermarkScrapeTs": f"timestamp({Q1_WATERMARK})",
}

# interfaces.md, "Counts (D-28)", by the operator that counts them.
COUNTERS = {
    "source": ["priceEvents", "nonPriceEvents", "nonPositivePriceEvents", "missingTimestampEvents", "malformedEvents", "systemDateEvents"],
    "q1-ema": ["lateTicks", "evaluatedWindows", "buyCrossovers", "sellCrossovers"],
    "subscription-filter": ["publishedResults", "publishedAdvisories", "suppressedResults"],
}
SPLIT = COUNTERS["source"][:5]

# The design-comparison variants (D-42): each key's choices, the baseline first.
VARIANTS = {
    "operator": ("process", "window"),
    "emptyWindows": ("skip", "carry-forward"),
    "stateBackend": ("hashmap", "rocksdb"),
    "ticksEncoding": ("csv", "binary"),
    "filtering": ("flink", "replayer"),
}
JOB_FLAGS = {"operator": "--operator", "emptyWindows": "--empty-windows", "stateBackend": "--state-backend",
             "ticksEncoding": "--ticks-encoding"}


def utc_stamp() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def iso_ms(value: str) -> int:
    return int(dt.datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000)


def http_json(url: str, method: str = "GET", timeout: float = 30):
    request = urllib.request.Request(url, method=method)
    with urllib.request.urlopen(request, timeout=timeout) as response:
        body = response.read()
    return json.loads(body) if body else None


def wait_for(description: str, read, ready, timeout: float, interval: float = 2):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            last = read()
            if ready(last):
                return last
        except (urllib.error.URLError, TimeoutError, ConnectionError, KeyError, ValueError) as error:
            last = repr(error)
        time.sleep(interval)
    raise RuntimeError(f"timed out after {timeout:.0f} s waiting for {description}: {last}")


def prometheus(path: str, **params) -> dict:
    url = f"{PROMETHEUS}/api/v1/{path}?{urllib.parse.urlencode(params)}"
    reply = http_json(url, timeout=60)
    if reply.get("status") != "success":
        raise RuntimeError(f"Prometheus {path} failed: {reply}")
    return reply["data"]


def scalar(query: str, at: float | None = None) -> float | None:
    params = {"query": query} if at is None else {"query": query, "time": f"{at:.3f}"}
    result = prometheus("query", **params)["result"]
    return float(result[0]["value"][1]) if result else None


def md5(path: Path) -> str:
    digest = hashlib.md5()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 22), b""):
            digest.update(block)
    return digest.hexdigest()


class Config:
    """The experiment file, checked once before anything runs."""

    def __init__(self, path: Path, repeats: int | None):
        raw = json.loads(path.read_text())
        self.raw = raw
        self.name = raw["name"]
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", self.name):
            raise ValueError(f"name {self.name!r} must be usable as a directory name")
        self.mode = raw["mode"]
        if self.mode not in ("preload", "live"):
            raise ValueError("mode must be preload or live")
        self.inputs = [Path(p) for p in raw["input"]]
        for p in self.inputs:
            resolved = (ROOT / p).resolve()
            if not resolved.is_relative_to((ROOT / "data").resolve()) or not resolved.exists():
                raise ValueError(f"input {p} must exist under data/, which Compose mounts at /data")
        self.replay_args = [str(a) for a in raw.get("replayArgs", [])]
        if self.mode == "live" and "--rate" not in self.replay_args:
            raise ValueError("a live run needs --rate in replayArgs")
        if {"--mode", "--run-id", "--encoding", "--early-filter"} & set(self.replay_args):
            raise ValueError("replayArgs must not set --mode, --run-id, --encoding or --early-filter; "
                             "the harness does, the last two from variant")
        variant = raw.get("variant", {})
        unknown = set(variant) - set(VARIANTS)
        if unknown:
            raise ValueError(f"unknown variant keys {sorted(unknown)}; known: {sorted(VARIANTS)}")
        self.variant = {key: variant.get(key, choices[0]) for key, choices in VARIANTS.items()}
        for key, value in self.variant.items():
            if value not in VARIANTS[key]:
                raise ValueError(f"variant.{key} must be one of {VARIANTS[key]}: {value!r}")
        job = raw["job"]
        # No defaults here, so every record states the bound and idleness it ran with (D-38 has the job's).
        self.bound_ms = int(job["watermarkBoundMs"])
        self.idleness_ms = int(job["idlenessMs"])
        self.zone_offset = job["zoneOffset"]
        self.job_args = [str(a) for a in job.get("args", [])]
        if set(JOB_FLAGS.values()) & set(self.job_args):
            raise ValueError("job.args must not set the variant flags; use variant")
        stack = raw.get("stack", {})
        self.partitions = int(stack.get("partitions", 8))
        self.taskmanagers = int(stack.get("taskmanagers", 1))
        self.slots = int(stack.get("slots", 2))
        self.parallelism = int(stack.get("parallelism", 2))
        if self.parallelism > self.taskmanagers * self.slots:
            raise ValueError(f"parallelism {self.parallelism} needs more than {self.taskmanagers} x {self.slots} slots")
        self.checkpointing = bool(raw.get("checkpointing", False))
        self.subscriptions = raw.get("subscriptions", [{"trader": "all", "symbols": ["*"]}])
        for s in self.subscriptions:
            if "|" in s["trader"] or not isinstance(s["symbols"], list):
                raise ValueError(f"bad subscription {s}")
        self.repeats = repeats or int(raw.get("repeats", 1))
        self.warmup_s = float(raw.get("warmupS", 0))
        self.timeout_s = float(raw.get("timeoutS", 3600))
        self.finish = raw.get("finish", "bounded" if self.mode == "preload" else "drain")
        if self.finish not in ("bounded", "drain") or (self.mode == "live" and self.finish != "drain"):
            raise ValueError("finish must be bounded or drain, and drain in a live run")
        if self.checkpointing and self.finish == "bounded":
            raise ValueError("checkpointing needs finish drain: a bounded job waits for a final checkpoint, "
                             "which distorts its duration, and a short one takes none")
        self.oracle = Path(raw["oracle"]) if raw.get("oracle") else None
        selecting = {"--limit", "--symbols", "--symbols-file", "--exchanges"} & set(self.replay_args)
        if self.oracle and selecting:
            raise ValueError(f"the oracle covers whole files, so {sorted(selecting)} would make every run differ")
        if self.oracle and not (ROOT / self.oracle / "manifest.json").exists():
            raise ValueError(f"{self.oracle} holds no oracle output")
        if self.oracle:
            parameters = json.loads((ROOT / self.oracle / "manifest.json").read_text())["parameters"]
            oracle_policy = parameters.get("emptyWindows", "skip")
            if oracle_policy != self.variant["emptyWindows"]:
                raise ValueError(f"{self.oracle} skips or carries empty windows as {oracle_policy!r}, the job as "
                                 f"{self.variant['emptyWindows']!r}: run the oracle with --empty-windows "
                                 f"{self.variant['emptyWindows']}")

    def effective(self) -> dict:
        return {"mode": self.mode, "finish": self.finish, "partitions": self.partitions,
                "taskmanagers": self.taskmanagers, "slots": self.slots, "parallelism": self.parallelism,
                "watermarkBoundMs": self.bound_ms, "idlenessMs": self.idleness_ms, "zoneOffset": self.zone_offset,
                "warmupS": self.warmup_s, "subscribers": len(self.subscriptions), "variant": self.variant}

    def replayer_variant_args(self) -> list[str]:
        return ["--encoding", self.variant["ticksEncoding"],
                *(["--early-filter"] if self.variant["filtering"] == "replayer" else [])]

    def job_variant_args(self) -> list[str]:
        return [arg for key, flag in JOB_FLAGS.items() for arg in (flag, self.variant[key])]

    @property
    def all_subscribed(self) -> bool:
        return any("*" in s["symbols"] for s in self.subscriptions)

    def env(self) -> dict:
        return dict(os.environ, TICKS_PARTITIONS=str(self.partitions), TM_SLOTS=str(self.slots),
                    PARALLELISM=str(self.parallelism))

    def container_inputs(self) -> list[str]:
        return ["/data/" + str((ROOT / p).resolve().relative_to((ROOT / "data").resolve())) for p in self.inputs]

    def input_files(self) -> list[Path]:
        files = []
        for p in self.inputs:
            p = ROOT / p
            files += sorted(p.glob("*.csv")) if p.is_dir() else [p]
        return files


class Run:
    def __init__(self, cfg: Config, experiment: Path, repeat: int, inputs: list[dict]):
        self.cfg = cfg
        self.dir = experiment / f"r{repeat}"
        self.dir.mkdir(parents=True)
        self.id = f"{experiment.name}-r{repeat}"
        self.inputs = inputs
        self.env = cfg.env()
        self.times: dict[str, int] = {}
        self.record: dict = {"runId": self.id, "status": "RUNNING"}

    def sh(self, *args, input: str | None = None, timeout: float = 600, check: bool = True,
           stdout_path: Path | None = None, watch=None) -> str:
        """Runs a command with its stderr in commands.log; returns stdout unless it goes to stdout_path.

        watch, if given, is called every few seconds while the command runs; if it raises, the command is stopped.
        """
        command = [str(a) for a in args]
        print("+ " + shlex.join(command), flush=True)
        log_path = self.dir / "commands.log"
        with log_path.open("a") as log:
            log.write(f"\n+ {shlex.join(command)}\n")
            log.flush()
            out = stdout_path.open("w") if stdout_path else subprocess.PIPE
            try:
                if watch is None:
                    result = subprocess.run(command, cwd=ROOT, env=self.env, input=input, text=True,
                                            timeout=timeout, stdout=out, stderr=log)
                else:
                    result = self._watched(command, out, log, timeout, watch)
            finally:
                if stdout_path:
                    out.close()
            output = result.stdout or ""
            log.write(output)
        if check and result.returncode:
            raise RuntimeError(f"exit {result.returncode}: {shlex.join(command)}\n{log_path.read_text()[-4000:]}")
        return output

    def _watched(self, command: list[str], out, log, timeout: float, watch) -> subprocess.CompletedProcess:
        process = subprocess.Popen(command, cwd=ROOT, env=self.env, text=True, stdout=out, stderr=log)
        deadline = time.monotonic() + timeout
        try:
            while True:
                try:
                    process.wait(timeout=SCRAPE_S)
                    return subprocess.CompletedProcess(command, process.returncode, None)
                except subprocess.TimeoutExpired:
                    if time.monotonic() > deadline:
                        raise
                    watch()
        except BaseException:
            process.terminate()
            process.wait(timeout=60)
            raise

    def compose(self, *args, **kwargs) -> str:
        return self.sh("docker", "compose", *args, **kwargs)

    def mark(self, name: str, epoch_ms: int | None = None):
        self.times[name] = epoch_ms if epoch_ms is not None else int(time.time() * 1000)

    def save(self, name: str, value):
        (self.dir / name).write_text(json.dumps(value, indent=2) + "\n")

    # Phases

    def reset(self):
        self.mark("resetStart")
        self.compose("down", "--volumes", "--remove-orphans", timeout=300)
        self.compose("up", "-d", "--scale", f"taskmanager={self.cfg.taskmanagers}", timeout=600)
        self.compose("wait", "kafka-init", timeout=300)
        tms, slots = self.cfg.taskmanagers, self.cfg.taskmanagers * self.cfg.slots
        wait_for(f"{tms} TaskManagers with {slots} slots", lambda: http_json(f"{FLINK}/overview"),
                 lambda o: o["taskmanagers"] == tms and o["slots-total"] == slots, timeout=180)
        targets = {"flink": 1 + tms, "kafka": 1, "cadvisor": 1}
        for job, count in targets.items():
            wait_for(f"Prometheus scraping {count} {job} target(s)",
                     lambda job=job: scalar(f'count(up{{job="{job}"}} == 1)'),
                     lambda n, count=count: n == count, timeout=120)
        self.mark("resetEnd")

    def subscribe(self):
        lines = "".join(f'{s["trader"]}|{json.dumps(s, separators=(",", ":"))}\n' for s in self.cfg.subscriptions)
        self.compose("exec", "-T", "kafka", "/opt/kafka/bin/kafka-console-producer.sh", "--bootstrap-server",
                     "kafka:9092", "--topic", "subscriptions", "--reader-property", "parse.key=true",
                     "--reader-property", "key.separator=|", input=lines)

    def replay(self, mode: str, job_id: str | None = None) -> dict:
        args = []
        for path in self.cfg.container_inputs():
            args += ["--input", path]
        watch = (lambda: self.running(job_id)) if job_id else None
        container = f"{self.id}-replay"
        try:
            self.compose("run", "--rm", "-T", "--name", container, "producer", "fi.aalto.cse4780.replay.Replayer",
                         *args, "--mode", mode, "--run-id", self.id, *self.cfg.replayer_variant_args(),
                         *self.cfg.replay_args,
                         stdout_path=self.dir / "replay.json", timeout=self.cfg.timeout_s, check=False, watch=watch)
        except BaseException:
            # Stopping the Compose client does not always stop the container it started.
            self.sh("docker", "stop", "-t", "30", container, check=False, timeout=60)
            raise
        report = json.loads((self.dir / "replay.json").read_text())
        if report.get("status") != "COMPLETE":
            raise RuntimeError(f"replay ended {report.get('status')}; see replay.json")
        self.mark("replayStart", iso_ms(report["timing"]["startedAt"]))
        self.mark("replayEnd", iso_ms(report["timing"]["finishedAt"]))
        return report

    def submit(self, bounded: bool) -> str:
        flags = CHECKPOINTING if self.cfg.checkpointing else []
        job_args = ["--watermark-bound-ms", self.cfg.bound_ms, "--idleness-ms", self.cfg.idleness_ms,
                    "--zone-offset", self.cfg.zone_offset, *(["--bounded"] if bounded else []),
                    *self.cfg.job_variant_args(), *self.cfg.job_args]
        output = self.compose("exec", "-T", "jobmanager", "flink", "run", "-d", *flags, "-c", JOB_CLASS,
                              "/opt/job/flink-job.jar", *job_args)
        match = re.search(r"JobID ([0-9a-f]{32})", output)
        if not match:
            raise RuntimeError(f"no JobID in the flink run output:\n{output[-2000:]}")
        job_id = match.group(1)
        self.record["job"] = {"id": job_id, "bounded": bounded, "flinkRunFlags": flags,
                              "args": [str(a) for a in job_args]}
        self.record["job"]["effective"] = self.job_effective(job_id)
        return job_id

    def job_effective(self, job_id: str) -> dict:
        """The configuration the job resolved and published as its global job parameters, checked against the run's."""
        effective = http_json(f"{FLINK}/jobs/{job_id}/config")["execution-config"]["user-config"]
        expected = {**{k: self.cfg.variant[k] for k in JOB_FLAGS}, "watermarkBoundMs": str(self.cfg.bound_ms),
                    "idlenessMs": str(self.cfg.idleness_ms)}
        differing = {k: (v, effective.get(k)) for k, v in expected.items() if effective.get(k) != v}
        if differing:
            raise RuntimeError(f"the job runs another configuration than the run's (requested, job): {differing}")
        return effective

    def wait_ended(self, job_id: str) -> dict:
        return wait_for(f"job {job_id} to end", lambda: http_json(f"{FLINK}/jobs/{job_id}"),
                        lambda j: j["state"] in ("FINISHED", "FAILED", "CANCELED"),
                        timeout=self.cfg.timeout_s, interval=1)

    def running(self, job_id: str) -> dict:
        job = http_json(f"{FLINK}/jobs/{job_id}")
        if job["state"] in ("FAILED", "CANCELED", "FINISHED"):
            raise RuntimeError(f"the job ended {job['state']} while the harness waited for it")
        return job

    def wait_drained(self, job_id: str, report: dict):
        """The unbounded job has read every replayed record and passed each price event to q1-ema."""
        written = sum(p["endOffset"] or 0 for p in report["offsets"])
        read = 'sum(flink_taskmanager_job_task_operator_KafkaSourceReader_topic_partition_currentOffset{topic="ticks"} + 1)'
        wait_for(f"the job to read all {written} records", lambda: self.running(job_id) and scalar(read),
                 lambda n: n == written, timeout=self.cfg.timeout_s, interval=SCRAPE_S)
        handed = "sum(flink_taskmanager_job_task_operator_deserializer_ticks_priceEvents)"
        received = 'sum(flink_taskmanager_job_task_operator_numRecordsIn{operator_name="q1_ema"})'
        wait_for("q1-ema to receive every price event",
                 lambda: self.running(job_id) and (scalar(handed), scalar(received)),
                 lambda pair: pair[0] is not None and pair[0] == pair[1], timeout=self.cfg.timeout_s, interval=SCRAPE_S)
        time.sleep(SCRAPE_S + 1)

    def drain(self, job_id: str) -> dict:
        """Ends an unbounded job the way end of input ends a bounded one: every remaining window fires."""
        self.compose("exec", "-T", "jobmanager", "flink", "stop", "--drain", "--savepointPath",
                     "file:///tmp/flink-savepoints", job_id, timeout=600)
        return self.wait_ended(job_id)

    # Collection

    def flink_job(self, job_id: str) -> dict:
        job = http_json(f"{FLINK}/jobs/{job_id}")
        vertices = {v["name"]: http_json(f"{FLINK}/jobs/{job_id}/vertices/{v['id']}") for v in job["vertices"]}
        exceptions = http_json(f"{FLINK}/jobs/{job_id}/exceptions") if job["state"] == "FAILED" else None
        detail = {"job": job, "vertices": vertices, "exceptions": exceptions}
        self.save("flink-job.json", detail)
        return detail

    def metrics(self, start_ms: int, end_ms: int) -> dict:
        start, end = start_ms / 1000 - 3 * SCRAPE_S, end_ms / 1000 + 2 * SCRAPE_S
        pulled = {"start": start, "end": end, "stepS": SCRAPE_S, "queries": {}}
        for name, query in QUERIES.items():
            data = prometheus("query_range", query=query, start=f"{start:.3f}", end=f"{end:.3f}", step=f"{SCRAPE_S}s")
            pulled["queries"][name] = {"query": query, "result": data["result"]}
        self.save("metrics.json", pulled)
        return pulled

    def counters(self, start_ms: int, end_ms: int) -> dict:
        at = end_ms / 1000 + 2 * SCRAPE_S
        window = max(1, int(at - start_ms / 1000 + 1))
        values = {}
        for group, names in COUNTERS.items():
            for name in names:
                metric = ("flink_taskmanager_job_task_operator_deserializer_ticks_" if group == "source"
                          else "flink_taskmanager_job_task_operator_") + name
                value = scalar(f"sum(last_over_time({metric}[{window}s]))", at=at)
                values[name] = None if value is None else int(value)
        return values

    def export(self) -> dict:
        ticks = (["--ticks-topic", "ticks", "--ticks-encoding", self.cfg.variant["ticksEncoding"]]
                 if self.cfg.mode == "live" else [])
        self.sh("correctness/run.sh", "export", "--zone-offset", self.cfg.zone_offset,
                "--replay-report", self.dir / "replay.json", *ticks,
                "--out", f"/out/bench/{self.id}/export", timeout=1800)
        return self._collect("export", "manifest.json")

    def latency(self) -> dict:
        if self.cfg.variant["emptyWindows"] == "carry-forward":
            report = {"complete": False, "problems": [
                "carried windows are published with the next window and have no admission of their own (D-42)"]}
            (self.dir / "latency").mkdir()
            self.save("latency/latency.json", report)
            return report
        interval = self.interval()
        self.sh("correctness/run.sh", "latency", "--export", self.dir / "export",
                "--replay-report", self.dir / "replay.json", "--metrics", self.dir / "metrics.json",
                "--bound-ms", self.cfg.bound_ms, "--idleness-ms", self.cfg.idleness_ms,
                "--interval-start-ms", interval[0], "--interval-end-ms", interval[1],
                "--idle-from-ms", self.times["jobRunning"], "--drain-start-ms", self.times["drainStart"],
                "--out", f"/out/bench/{self.id}/latency", timeout=1800)
        return self._collect("latency", "latency.json")

    def compare(self, late_ticks: int | None) -> dict:
        late = (["--job-late-ticks", str(late_ticks),
                 "--job-late-events", self.dir / "export" / "late_events.parquet"]
                if late_ticks is not None else [])
        self.sh("correctness/run.sh", "compare", "--oracle", self.cfg.oracle, "--job", self.dir / "export",
                *late, "--out", f"/out/bench/{self.id}/compare", timeout=1800, check=False)
        return self._collect("compare", "report.json")

    def _collect(self, part: str, file: str) -> dict:
        """Moves a correctness tool's output from correctness/out/, where its container writes, into the run."""
        scratch = ROOT / "correctness" / "out" / "bench" / self.id
        shutil.move(str(scratch / part), str(self.dir / part))
        scratch.rmdir()
        return json.loads((self.dir / part / file).read_text())

    def describe(self) -> dict:
        """The effective configuration of the stack this run used."""
        self.compose("config", stdout_path=self.dir / "compose.yml")
        containers = []
        for cid in self.compose("ps", "-q").split():
            info = json.loads(self.sh("docker", "inspect", cid))[0]
            host = info.get("HostConfig", {})
            containers.append({"name": info["Name"].lstrip("/"), "image": info["Config"]["Image"],
                               "imageId": info["Image"], "nanoCpus": host.get("NanoCpus"),
                               "cpuQuota": host.get("CpuQuota"), "memoryBytes": host.get("Memory")})
        before = time.time()
        container_ms = int(self.compose("exec", "-T", "jobmanager", "date", "+%s%3N").strip().splitlines()[-1])
        after = time.time()
        return {
            "containers": containers,
            "taskmanagers": http_json(f"{FLINK}/taskmanagers")["taskmanagers"],
            "flinkConfig": http_json(f"{FLINK}/jobmanager/config"),
            "flinkVersion": http_json(f"{FLINK}/config").get("flink-version"),
            "clock": {"containerMinusHostMs": container_ms - int((before + after) * 500),
                      "roundTripMs": int((after - before) * 1000)},
        }

    def salvage(self):
        """Keeps what explains a failed run before the next reset deletes it."""
        job_id = self.record.get("job", {}).get("id")
        if not job_id:
            return
        try:
            job = (json.loads((self.dir / "flink-job.json").read_text()) if (self.dir / "flink-job.json").exists()
                   else self.flink_job(job_id))["job"]
            if not (self.dir / "metrics.json").exists():
                end = job["end-time"] if job["end-time"] > 0 else int(time.time() * 1000)
                self.metrics(self.times.get("replayStart", job["start-time"]), end)
        except Exception as error:
            print(f"could not collect the failed run: {error}", file=sys.stderr, flush=True)

    # Derived

    def reconcile(self, report: dict, flink: dict, counters: dict, outputs: dict) -> dict:
        v = {name: vertex["aggregated"]["metrics"] for name, vertex in flink["vertices"].items()}
        source = v.get("Source: ticks", {})
        q1 = v.get("q1-ema", {})
        sinks = next((m for name, m in v.items() if name.startswith("subscription-filter")), {})
        topics = outputs["export"]["topics"]
        exported_ema = topics.get("ema-results", {}).get("records")
        exported_adv = topics.get("advisories", {}).get("records")
        late_export = topics.get("late-ticks", {})
        late_final = late_export.get("complete", False)
        counters["lateTicks"] = late_export.get("records") if late_final else None

        def total(metrics, key):
            return metrics.get(key, {}).get("sum")

        checks = [
            ("replayed records = records the job read", report["counts"]["acknowledged"], total(source, "read-records")),
            ("price events out of the source = into q1-ema", total(source, "write-records"), total(q1, "read-records")),
            ("records the job wrote = records exported", total(sinks, "write-records"),
             None if exported_ema is None or exported_adv is None else exported_ema + exported_adv),
        ]
        price_events = topics.get("ticks", {})
        if price_events:
            checks.append(("price events out of the source = exported from ticks", total(source, "write-records"),
                           price_events.get("rows") if price_events.get("complete") else None))
        if self.cfg.all_subscribed:
            checks.append(("q1 outputs = exported ema-results + late-ticks", total(q1, "write-records"),
                           None if exported_ema is None or not late_final else exported_ema + counters["lateTicks"]))
        # A counter is final when its last scrape agrees with the job graph Flink keeps after the job ends.
        final = {
            "split": sum(counters[n] or 0 for n in SPLIT) == total(source, "read-records")
                     and counters["priceEvents"] == total(source, "write-records"),
            "lateTicks": late_final,
        }
        if self.cfg.oracle:
            oracle = json.loads((ROOT / self.cfg.oracle / "manifest.json").read_text())["counts"]["total"]
            # The replayer rejects blank lines and rows of the wrong field count; the oracle counts blank
            # lines apart and the rest as data.
            checks += [
                ("replayed data and rejected rows = oracle data and blank rows",
                 report["counts"]["dataRows"] + report["counts"]["rejected"],
                 oracle["dataRows"] + oracle.get("blank", 0)),
                ("price events = oracle price events", total(source, "write-records"), oracle["accepted"]),
            ]
            if final["split"]:
                # An early filter leaves the non-price rows in the replayer's count instead of the job's (D-42).
                non_price = counters["nonPriceEvents"] + report["counts"].get("filtered", 0)
                checks += [
                    ("non-price events, the job's and the replayer's filtered = oracle's", non_price,
                     oracle["nonPrice"]),
                    ("non-positive prices = oracle's", counters["nonPositivePriceEvents"], oracle["nonPositivePrice"]),
                    ("events dated from Date = oracle's", counters["systemDateEvents"], oracle["systemDateEvents"]),
                ]
        rows = [{"check": name, "left": left, "right": right, "ok": left is not None and left == right}
                for name, left, right in checks]
        return {"ok": all(r["ok"] for r in rows), "checks": rows, "countersFinal": final}

    def interval(self) -> tuple[int, int]:
        """The measurement interval: the job's run in a preload, the replay in a live run, minus the warm-up."""
        warmup = int(self.cfg.warmup_s * 1000)
        if self.cfg.mode == "preload":
            return self.times["jobStart"] + warmup, self.times["jobEnd"]
        return self.times["replayStart"] + warmup, self.times["replayEnd"]

    def summarize(self, report: dict, flink: dict, pulled: dict, counters: dict, recon: dict, outputs: dict) -> dict:
        job = flink["job"]
        duration_ms = job["duration"]
        per_vertex = {}
        for name, vertex in flink["vertices"].items():
            subtasks = vertex["subtasks"]
            records = [s["metrics"]["read-records"] for s in subtasks]
            mean = sum(records) / len(records) if records else 0
            per_vertex[name] = {
                "parallelism": vertex["parallelism"],
                "readRecords": records,
                "writeRecords": [s["metrics"]["write-records"] for s in subtasks],
                "busyMs": [s["metrics"]["accumulated-busy-time"] for s in subtasks],
                "idleMs": [s["metrics"]["accumulated-idle-time"] for s in subtasks],
                "backPressuredMs": [s["metrics"]["accumulated-backpressured-time"] for s in subtasks],
                "durationMs": [s["duration"] for s in subtasks],
                "readSkew": round(max(records) / mean, 4) if mean else None,
            }
        source = flink["vertices"].get("Source: ticks", {}).get("subtasks", [])
        read = [s["metrics"]["read-records"] for s in source]
        span_ms = max(s["end-time"] for s in source) - min(s["start-time"] for s in source) if source else 0
        interval = self.interval()
        series = {}
        for name, entry in pulled["queries"].items():
            series[name] = []
            for s in entry["result"]:
                points = [float(val) for t, val in s["values"] if interval[0] / 1000 <= t <= interval[1] / 1000]
                points = [p for p in points if p == p]
                series[name].append({"labels": s["metric"], "points": len(points),
                                     "mean": sum(points) / len(points) if points else None,
                                     "max": max(points) if points else None})
        summary = {
            "runId": self.id,
            "mode": self.cfg.mode,
            "finish": self.cfg.finish,
            "jobState": job["state"],
            "measurementInterval": {"startMs": interval[0], "endMs": interval[1]},
            "replay": {"records": report["counts"]["acknowledged"], "durationMs": report["timing"]["durationMs"],
                       "recordsPerSecond": report["timing"]["recordsPerSecond"], "pacing": report.get("pacing")},
            "job": {"durationMs": duration_ms, "sourceSpanMs": span_ms, "recordsRead": sum(read),
                    "recordsPerSecond": round(sum(read) / span_ms * 1000, 1)
                                        if self.cfg.finish == "bounded" and span_ms > 0 else None,
                    "vertices": per_vertex},
            "counters": counters,
            "reconciliation": recon,
            "outputs": {topic: info.get("records") for topic, info in outputs["export"]["topics"].items()},
            "metrics": series,
        }
        if "latency" in outputs:
            summary["latency"] = outputs["latency"]
        if "compare" in outputs:
            c = outputs["compare"]
            identical = (not c["ema"]["differences"] and not c["advisories"]["byKind"]
                         and not any(c["ema"]["duplicateRows"].values()) and not any(c["advisories"]["duplicateRows"].values()))
            only_late_unknown = all(p.startswith("the job's lateTicks was not given") for p in c["problems"])
            summary["correctness"] = {"verdict": "pass" if c["pass"] else
                                      "unverified" if identical and only_late_unknown else "fail",
                                      "problems": c["problems"],
                                      "emaDifferences": c["ema"]["differences"], "advisoryDifferences":
                                      sum(c["advisories"]["byKind"].values()) if c["advisories"]["byKind"] else 0,
                                      "precision": c["advisories"]["precision"], "recall": c["advisories"]["recall"]}
        return summary

    def execute(self, session: dict):
        try:
            self.reset()
            self.subscribe()
            if self.cfg.mode == "preload":
                report = self.replay("preload")
            job_id = self.submit(bounded=self.cfg.finish == "bounded")
            self.mark("jobSubmitted")
            if self.cfg.finish == "bounded":
                job = self.wait_ended(job_id)
            else:
                wait_for("every job task to run", lambda: self.running(job_id),
                         lambda j: all(v["status"] == "RUNNING" for v in j["vertices"]), timeout=180, interval=1)
                self.mark("jobRunning")
                if self.cfg.mode == "live":
                    report = self.replay("realtime", job_id)
                self.wait_drained(job_id, report)
                self.mark("drainStart")
                job = self.drain(job_id)
            self.mark("jobStart", job["start-time"])
            self.mark("jobEnd", job["end-time"])
            start = self.times["replayStart"] if self.cfg.mode == "preload" else job["start-time"]
            time.sleep(2 * SCRAPE_S)
            flink = self.flink_job(job_id)
            pulled = self.metrics(start, job["end-time"])
            if job["state"] != "FINISHED":
                raise RuntimeError(f"the job ended {job['state']}; see flink-job.json")
            counters = self.counters(start, job["end-time"])
            self.record["stack"] = self.describe()
            outputs = {"export": self.export()}
            recon = self.reconcile(report, flink, counters, outputs)
            if self.cfg.mode == "live":
                outputs["latency"] = self.latency()
            if self.cfg.oracle:
                final = recon["countersFinal"]["lateTicks"]
                outputs["compare"] = self.compare(counters["lateTicks"] if final else None)
            summary = self.summarize(report, flink, pulled, counters, recon, outputs)
            self.save("summary.json", summary)
            valid = recon["ok"] and summary.get("correctness", {}).get("verdict") != "fail"
            self.record["status"] = "COMPLETE" if valid else "INVALID"
        except BaseException as error:
            self.record["status"] = "FAILED" if isinstance(error, Exception) else "INTERRUPTED"
            self.record["error"] = str(error) or type(error).__name__
            self.salvage()
            raise
        finally:
            self.record.update({**session, "effective": self.cfg.effective(), "times": self.times, "inputs": self.inputs})
            self.save("config.json", self.record)


def git(*args: str) -> str:
    return subprocess.run(["git", *args], cwd=ROOT, capture_output=True, text=True).stdout.strip()


def hardware() -> dict:
    host = {"platform": platform.platform(), "machine": platform.machine(), "cpus": os.cpu_count()}
    if sys.platform == "darwin":
        sysctl = lambda key: subprocess.run(["sysctl", "-n", key], capture_output=True, text=True).stdout.strip()
        host.update(cpuModel=sysctl("machdep.cpu.brand_string"), memoryBytes=int(sysctl("hw.memsize") or 0))
    elif Path("/proc/cpuinfo").exists():
        model = re.search(r"model name\s*:\s*(.*)", Path("/proc/cpuinfo").read_text())
        mem = re.search(r"MemTotal:\s*(\d+) kB", Path("/proc/meminfo").read_text())
        host.update(cpuModel=model and model.group(1), memoryBytes=mem and int(mem.group(1)) * 1024)
    # Docker and Podman answer with different schemas; both are kept verbatim.
    engine = subprocess.run(["docker", "info", "--format", "{{json .}}"], capture_output=True, text=True)
    return {"host": host, "engine": json.loads(engine.stdout) if engine.returncode == 0 else engine.stderr.strip()}


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("config", type=Path)
    parser.add_argument("--repeats", type=int, help="override the config's repeats")
    parser.add_argument("--no-build", action="store_true", help="use the images already built")
    args = parser.parse_args()
    cfg = Config(args.config, args.repeats)

    inputs = [{"path": str(f.relative_to(ROOT)), "bytes": f.stat().st_size, "md5": md5(f)} for f in cfg.input_files()]
    if cfg.oracle:
        oracle = json.loads((ROOT / cfg.oracle / "manifest.json").read_text())
        ours = sorted((Path(i["path"]).name, i["md5"]) for i in inputs)
        theirs = sorted((i["file"], i.get("md5")) for i in oracle.get("inputs", []))
        if ours != theirs:
            raise SystemExit(f"{cfg.oracle} was computed from other files: {theirs} against {ours}")
    if not args.no_build:
        subprocess.run(["docker", "compose", "--profile", "tools", "build"], cwd=ROOT, env=cfg.env(), check=True)

    experiment = RESULTS / f"{cfg.name}-{utc_stamp()}"
    experiment.mkdir(parents=True)
    print(f"experiment {experiment}", flush=True)
    session = {
        "experiment": experiment.name,
        "config": cfg.raw,
        "configFile": str(args.config),
        "code": {"revision": git("rev-parse", "HEAD"), "branch": git("rev-parse", "--abbrev-ref", "HEAD"),
                 "dirty": bool(git("status", "--porcelain", "--untracked-files=no"))},
        "checkpointing": cfg.checkpointing,
        "hardware": hardware(),
    }
    complete = 0
    for repeat in range(1, cfg.repeats + 1):
        run = Run(cfg, experiment, repeat, inputs)
        print(f"== {run.id}", flush=True)
        try:
            run.execute(session)
        except Exception as error:
            print(f"FAILED {run.id}: {error}", file=sys.stderr, flush=True)
        if run.record["status"] == "INVALID":
            print(f"INVALID {run.id}: reconciliation or correctness failed; see summary.json", file=sys.stderr, flush=True)
        complete += run.record["status"] == "COMPLETE"
    print(f"{complete} of {cfg.repeats} runs complete in {experiment}", flush=True)
    sys.exit(0 if complete == cfg.repeats else 1)


if __name__ == "__main__":
    main()
