#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../../.."
: "${ISSUE9_PYTHON:?Set ISSUE9_PYTHON to Python 3.11+ with correctness/requirements.txt installed}"
out="${1:?Pass the directory for build, test and environment evidence}"
mkdir -p "$out"
podman version > "$out/podman-version.txt"
podman compose version > "$out/compose-version.txt"
podman machine inspect --format '{{.Resources}}' > "$out/vm-resources.txt"
podman image inspect docker.io/library/flink:2.2.1-java17 docker.io/trading-trends/kafka:dev \
  localhost/trading-trends/oracle:dev localhost/trading-trends/correctness:dev \
  --format '{{.Id}} {{.RepoTags}}' > "$out/images.txt"
podman run --rm -v "$PWD":/src -v trading-trends-m2:/root/.m2 -w /src \
  docker.io/library/maven:3.9-eclipse-temurin-17 mvn -B -ntp clean verify > "$out/maven.log" 2>&1
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=correctness:oracle "$ISSUE9_PYTHON" -m pytest correctness/tests oracle/tests > "$out/python-tests.log" 2>&1
"$ISSUE9_PYTHON" - "$out" <<'PY'
import hashlib
import json
import subprocess
import sys
from pathlib import Path

files = ['flink-job/target/flink-job.jar', 'producer/target/producer.jar',
         'oracle/ema_oracle/run.py', 'correctness/ema_check/compare.py', 'correctness/ema_check/export.py']
metadata = dict(runtimeRevision=subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
                files={p: hashlib.sha256(Path(p).read_bytes()).hexdigest() for p in files})
(Path(sys.argv[1]) / 'build.json').write_text(json.dumps(metadata, indent=2) + '\n')
PY
