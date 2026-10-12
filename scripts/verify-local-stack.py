#!/usr/bin/env python3

import argparse
import json
import os
from pathlib import Path
import shlex
import shutil
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.admin import ConfigResource, ConfigResourceType, KafkaAdminClient, NewTopic


ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "scripts" / "validation"
TOPICS = {"ticks": 8, "ema-results": 8, "advisories": 8, "stats": 1, "subscriptions": 1}
INPUT = "stack-check-in"
OUTPUT = "stack-check-out"
RECORD = b"VERIFY.NL,E,08-11-2021,09:00:01.1234,19.2800,09:00:01.1000,08-11-2021"


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def get_json(url):
    with urllib.request.urlopen(url, timeout=10) as response:
        return json.load(response)


def wait_for(description, read, ready, timeout=120):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            last = read()
            if ready(last):
                return last
        except (urllib.error.URLError, TimeoutError, ConnectionError) as error:
            last = str(error)
        time.sleep(2)
    raise RuntimeError(f"Timed out waiting for {description}: {last}")


def free_ports():
    sockets = [socket.socket() for _ in range(5)]
    try:
        for connection in sockets:
            connection.bind(("127.0.0.1", 0))
        return [connection.getsockname()[1] for connection in sockets]
    finally:
        for connection in sockets:
            connection.close()


class Verification:
    def __init__(self, output):
        self.output = output
        self.project = "trading-trends-check-" + uuid.uuid4().hex[:12]
        self.test_image = f"{self.project}/test:dev"
        kafka, flink, prometheus, grafana, viz = free_ports()
        self.env = dict(os.environ, KAFKA_HOST_PORT=str(kafka), FLINK_HOST_PORT=str(flink),
                        PROMETHEUS_HOST_PORT=str(prometheus), GRAFANA_HOST_PORT=str(grafana), VIZ_HOST_PORT=str(viz),
                        TICKS_PARTITIONS="8", TM_SLOTS="2", PARALLELISM="2", COMPOSE_PROFILES="")
        self.bootstrap = f"localhost:{kafka}"
        self.flink = f"http://127.0.0.1:{flink}"
        self.prometheus = f"http://127.0.0.1:{prometheus}"
        self.grafana = f"http://127.0.0.1:{grafana}"
        self.results = {"project": self.project, "started": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                        "ports": {"kafka": kafka, "flink": flink, "prometheus": prometheus, "grafana": grafana, "viz": viz},
                        "checks": {}, "devcontainer": "not requested", "status": "running"}

    def record(self, name, value):
        self.results["checks"][name] = value
        print(f"PASS {name}: {json.dumps(value)}", flush=True)
        self.save()

    def save(self):
        (self.output / "results.json").write_text(json.dumps(self.results, indent=2) + "\n")

    def run(self, *args, input=None, timeout=600, check=True):
        command = [str(arg) for arg in args]
        print("+ " + shlex.join(command), flush=True)
        with (self.output / "commands.log").open("a+") as log:
            log.write("\n+ " + shlex.join(command) + "\n")
            log.flush()
            offset = log.tell()
            result = subprocess.run(command, cwd=ROOT, env=self.env, input=input, text=True,
                                    stdout=log, stderr=subprocess.STDOUT, timeout=timeout)
            log.seek(offset)
            output = log.read()
        if check and result.returncode:
            raise RuntimeError(f"Command failed ({result.returncode}): {shlex.join(command)}\n{output[-6000:]}")
        return output

    def compose(self, *args, **kwargs):
        return self.run("docker", "compose", "--project-directory", ROOT, "-f", ROOT / "docker-compose.yml",
                        "-p", self.project, *args, **kwargs)

    def capacity(self, managers):
        overview = wait_for(f"{managers} TaskManagers and {managers * 2} slots",
                            lambda: get_json(self.flink + "/overview"),
                            lambda value: value["taskmanagers"] == managers and value["slots-total"] == managers * 2)
        registered = get_json(self.flink + "/taskmanagers")["taskmanagers"]
        require(len(registered) == managers and all(item["slotsNumber"] == 2 for item in registered),
                f"Unexpected TaskManager registrations: {registered}")
        self.record(f"capacity-{managers}", overview)

    def consume(self, topic, expected):
        consumer = KafkaConsumer(bootstrap_servers=self.bootstrap, enable_auto_commit=False,
                                 consumer_timeout_ms=30000, request_timeout_ms=10000)
        try:
            partition = TopicPartition(topic, 0)
            consumer.assign([partition])
            consumer.seek_to_beginning()
            received = []
            for message in consumer:
                received.append(message.value)
                if len(received) == len(expected):
                    break
            require(received == expected, f"Unexpected records in {topic}: {received}")
            require(consumer.end_offsets([partition])[partition] == len(expected),
                    f"Unexpected extra records in {topic}")
        finally:
            consumer.close()

    def kafka(self):
        admin = KafkaAdminClient(bootstrap_servers=self.bootstrap, request_timeout_ms=10000)
        try:
            topics = admin.describe_topics(list(TOPICS))
            observed = {item["name"]: len(item["partitions"]) for item in topics}
            require(observed == TOPICS, f"Unexpected topics: {topics}")
            config = admin.describe_configs([ConfigResource(ConfigResourceType.TOPIC, "subscriptions")])
            cleanup_policy = config["topic"]["subscriptions"]["cleanup.policy"]["value"]
            require(cleanup_policy == "compact", f"subscriptions cleanup.policy={cleanup_policy}")
            brokers = admin.describe_cluster()["brokers"]
            require(len(brokers) == 1 and f"{brokers[0]['host']}:{brokers[0]['port']}" == self.bootstrap,
                    f"Wrong host advertisement: {brokers}")
            admin.create_topics([NewTopic(name, 1, 1) for name in [INPUT, OUTPUT]])
            self.record("kafka-topics", {"partitions": observed, "subscriptions": cleanup_policy, "brokers": brokers})
        finally:
            admin.close()
        producer = KafkaProducer(bootstrap_servers=self.bootstrap, acks="all")
        try:
            producer.send(INPUT, key=b"VERIFY.NL", value=RECORD).get(timeout=30)
        finally:
            producer.close(timeout=10)
        self.consume(INPUT, [RECORD])
        consumed = self.compose("exec", "-T", "kafka", "/opt/kafka/bin/kafka-console-consumer.sh",
                                "--bootstrap-server", "kafka:9092", "--topic", INPUT, "--from-beginning",
                                "--max-messages", "1", "--timeout-ms", "30000")
        require(RECORD.decode() in consumed.splitlines(), "Container listener did not return the host-produced record")
        self.compose("exec", "-T", "kafka", "/opt/kafka/bin/kafka-console-producer.sh",
                     "--bootstrap-server", "kafka:9092", "--topic", INPUT, input="container-listener\n")
        self.consume(INPUT, [RECORD, b"container-listener"])
        self.record("kafka-roundtrip", "host and container clients produced and consumed the same records")

    def metrics(self, managers):
        jobs = {"flink": managers + 1, "kafka": 1, "cadvisor": 1}
        targets = wait_for("healthy scrape targets", lambda: get_json(self.prometheus + "/api/v1/targets"),
                           lambda value: sorted(target["labels"]["job"] for target in value["data"]["activeTargets"])
                           == sorted(job for job, count in jobs.items() for _ in range(count))
                           and all(target["health"] == "up" for target in value["data"]["activeTargets"]))
        datasources = wait_for("Grafana datasource", lambda: get_json(self.grafana + "/api/datasources"),
                               lambda value: any(item["name"] == "Prometheus" for item in value))
        uid = next(item["uid"] for item in datasources if item["name"] == "Prometheus")
        taskmanagers = (f'container_label_com_docker_compose_project="{self.project}",'
                        'container_label_com_docker_compose_service="taskmanager"')
        # Each query's expected series count, and the value every series must have (None: any positive value).
        queries = [("flink_jobmanager_numRegisteredTaskManagers", 1, managers),
                   ("flink_taskmanager_Status_JVM_Memory_Heap_Used", managers, None),
                   (f'kafka_server_brokertopicmetrics_messagesin_total{{topic="{INPUT}"}}', 1, 2),
                   (f"max by (name) (container_memory_working_set_bytes{{{taskmanagers}}})", managers, None)]
        samples = {}
        for query, count, expected in queries:
            encoded = urllib.parse.urlencode({"query": query})
            for name, base in [("prometheus", self.prometheus),
                               ("grafana", self.grafana + f"/api/datasources/proxy/uid/{uid}")]:
                result = wait_for(f"{query} via {name}", lambda: get_json(base + "/api/v1/query?" + encoded),
                                  lambda value: value["status"] == "success" and len(value["data"]["result"]) == count
                                  and all(float(item["value"][1]) == expected for item in value["data"]["result"]
                                          if expected is not None))
                series = result["data"]["result"]
                require(all(float(item["value"][1]) > 0 for item in series), f"Invalid metric sample: {series}")
                samples[f"{name}:{query}"] = series
        (self.output / f"metrics-{managers}.json").write_text(json.dumps({"targets": targets, "samples": samples}, indent=2) + "\n")
        self.record(f"metrics-{managers}", f"{managers + 3} healthy targets; Flink, Kafka and cAdvisor samples "
                                           "via Prometheus and Grafana")

    def devcontainer(self, executable):
        label = f"stack-verification={self.project}"
        self.results["devcontainer"] = "running"
        result = self.run(executable, "up", "--workspace-folder", ROOT, "--id-label", label,
                          "--mount-workspace-git-root", "false", timeout=1800)
        details = json.loads(result.strip().splitlines()[-1])
        require(details["outcome"] == "success", f"Dev-container startup failed: {details}")
        self.run(executable, "exec", "--workspace-folder", ROOT, "--id-label", label,
                 "mvn", "-B", "-ntp", "clean", "verify", timeout=1200)
        self.run(executable, "exec", "--workspace-folder", ROOT, "--id-label", label,
                 "docker", "version")
        self.run(executable, "exec", "--workspace-folder", ROOT, "--id-label", label,
                 "docker", "compose", "version")
        self.results["devcontainer"] = "passed"
        self.record("devcontainer", "Maven clean verify and Docker/Compose access inside the configured container")

    def verify(self, devcontainer):
        self.results["devcontainer"] = "pending" if devcontainer else "not requested"
        self.results["revision"] = self.run("git", "rev-parse", "HEAD").strip()
        self.results["worktree"] = self.run("git", "status", "--short").strip()
        self.run("docker", "version")
        self.run("docker", "compose", "version")
        self.compose("config")
        self.run("docker", "build", "--progress=plain", "--target", "test", "--no-cache-filter", "test",
                 "-t", self.test_image, ".", timeout=1200)
        self.run("docker", "run", "--rm", "--mount",
                 f"type=bind,source={FIXTURES / 'LoggingCheck.java'},target=/tmp/LoggingCheck.java,readonly",
                 self.test_image, "java", "-cp", "/src/producer/target/producer.jar", "/tmp/LoggingCheck.java")
        self.compose("build", "kafka", "jobmanager", "producer", "viz", timeout=1200)
        self.record("container-build", "runtime images, Maven clean verify, and packaged producer logging")
        self.compose("up", "-d", "--no-build", "--scale", "taskmanager=1")
        self.compose("wait", "kafka-init", timeout=180)
        self.kafka()
        self.capacity(1)
        self.metrics(1)
        self.compose("exec", "-T", "jobmanager", "sh", "-c",
                     "cat > /tmp/verify.sql && /opt/flink/bin/sql-client.sh -f /tmp/verify.sql",
                     input=(FIXTURES / "kafka-roundtrip.sql").read_text(), timeout=240)
        self.consume(OUTPUT, [RECORD, b"container-listener"])
        self.record("flink-kafka-connector", "packaged connector read and wrote both synthetic records on the running cluster")
        self.compose("up", "-d", "--no-build", "--scale", "taskmanager=2")
        self.capacity(2)
        self.metrics(2)
        self.compose("logs", "--no-color")
        self.compose("stop")
        self.compose("start", "--wait", timeout=180)
        self.consume(INPUT, [RECORD, b"container-listener"])
        self.record("stop-start", "Kafka records retained")
        self.compose("down", "--volumes", timeout=180)
        self.compose("up", "-d", "--no-build", "--scale", "taskmanager=1")
        self.compose("wait", "kafka-init", timeout=180)
        admin = KafkaAdminClient(bootstrap_servers=self.bootstrap, request_timeout_ms=10000)
        try:
            require(set(admin.list_topics()) == set(TOPICS), "Reset retained unexpected Kafka topics")
        finally:
            admin.close()
        self.record("reset", "synthetic topics and data removed; five application topics recreated")
        self.compose("down", "--volumes", timeout=180)
        if devcontainer:
            self.devcontainer(devcontainer)

    def cleanup(self):
        self.compose("ps", "-a", check=False)
        self.compose("logs", "--no-color", check=False)
        self.compose("down", "--volumes", timeout=180)
        ids = self.run("docker", "ps", "-aq", "--filter", f"label=stack-verification={self.project}").split()
        if ids:
            self.run("docker", "rm", "-f", "-v", *ids)
        self.run("docker", "image", "rm", self.test_image, f"{self.project}/kafka:dev", f"{self.project}/flink-job:dev",
                 f"{self.project}/producer:dev", f"{self.project}/viz:dev", check=False)


def main():
    parser = argparse.ArgumentParser(description="Build and exercise a disposable local stack; always remove its containers afterwards.")
    parser.add_argument("--output", type=Path, help="New directory for commands.log, results.json, and metrics")
    parser.add_argument("--devcontainer", metavar="CLI", help="Also verify dev-container Maven and Docker access using this CLI executable")
    args = parser.parse_args()
    if args.devcontainer and not shutil.which(args.devcontainer):
        parser.error(f"Dev-container CLI not found: {args.devcontainer}")
    if args.output:
        args.output.mkdir(parents=True, exist_ok=False)
        output = args.output.resolve()
    else:
        output = Path(tempfile.mkdtemp(prefix="trading-trends-verification-"))
    verification = Verification(output)
    print(f"Evidence: {output}\nDisposable project: {verification.project}", flush=True)
    try:
        verification.verify(args.devcontainer)
        verification.results["status"] = "passed"
    except BaseException as error:
        verification.results.update(status="failed", error=str(error))
        raise
    finally:
        try:
            verification.cleanup()
        except Exception as error:
            verification.results.update(status="failed", cleanup_error=str(error))
            raise
        finally:
            verification.results["finished"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            verification.save()
    print(f"PASS local stack. Evidence: {output}", flush=True)


if __name__ == "__main__":
    main()
