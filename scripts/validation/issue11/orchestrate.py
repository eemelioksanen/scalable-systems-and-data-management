"""Runs the #11 matrix unattended.

1. Oracles the configs compare against (skipped when already computed).
2. Scale track: bench/configs/issue11/scale/*, one at a time on the whole host (Compose project trading-trends).
3. Calibration: is a lane's measurement changed by the other lane running? Lane A runs a p4 week preload and a
   300x live replay alone, then again while lane B runs the same. Lanes run in parallel only if the concurrent
   runs stay within the tolerances below; otherwise every lane config runs one at a time on lane A.
4. Lane track: bench/configs/issue11/lane/*.

Each step is logged to bench/results/raw/issue11-orchestrate.log with its exit status; a failed run does not stop the
rest. Run from the repository root:

    setsid nohup python3 scripts/validation/issue11/orchestrate.py > /dev/null 2>&1 &
"""

import json
import os
import shutil
import statistics
import subprocess
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
HERE = ROOT / "scripts/validation/issue11"
CONFIGS = ROOT / "bench/configs/issue11"
RAW = ROOT / "bench/results/raw"
LOG = RAW / "issue11-orchestrate.log"
DAY = "data/daily/debs2022-gc-trading-day"

THROUGHPUT_TOLERANCE = 0.03  # relative change in mean job records/s
QUEUEING_TOLERANCE_MS = 25  # change in mean queueing p95, the stage a neighbour's load would show in

lock = threading.Lock()


def log(msg: str) -> None:
    line = f"{time.strftime('%Y-%m-%dT%H:%M:%S')} {msg}"
    with lock:
        with LOG.open("a") as f:
            f.write(line + "\n")
        print(line, flush=True)


def lane_env(lane: str | None) -> dict:
    env = dict(os.environ)
    if lane is None:
        for key in ("COMPOSE_PROJECT_NAME", "COMPOSE_NETWORK", "COMPOSE_FILE", "LANE_CPUSET"):
            env.pop(key, None)
        return env
    for line in (HERE / f"lane-{lane}.env").read_text().splitlines():
        key, value = line.removeprefix("export ").split("=", 1)
        env[key] = value.strip('"')
    env["PATH"] = f"{HERE / 'bin'}:{env['PATH']}"
    return env


def sh(cmd: list[str], env: dict | None = None, tag: str = "") -> int:
    log(f"[{tag}] start: {' '.join(cmd)}")
    t0 = time.monotonic()
    with (RAW / f"issue11-{tag or 'host'}.out").open("a") as out:
        code = subprocess.run(cmd, cwd=ROOT, env=env, stdout=out, stderr=subprocess.STDOUT).returncode
    log(f"[{tag}] exit {code} after {time.monotonic() - t0:.0f}s: {' '.join(cmd)}")
    return code


def run(config: Path, lane: str | None) -> Path | None:
    """Runs one config; returns its experiment directory."""
    before = set(RAW.glob(f"{json.loads(config.read_text())['name']}-*"))
    sh([sys.executable, "bench/run.py", "--no-build", str(config.relative_to(ROOT))], lane_env(lane), lane or "host")
    after = set(RAW.glob(f"{json.loads(config.read_text())['name']}-*")) - before
    return max(after) if after else None


def oracles() -> None:
    jobs = {
        "week": [],
        "week-carry-forward": ["--empty-windows", "carry-forward"],
        "mon-bound1s": ["--watermark-bound-ms", "1000"],
        "mon-bound60s": ["--watermark-bound-ms", "60000"],
    }
    procs = []
    for name, args in jobs.items():
        if (ROOT / "oracle/out" / name / "manifest.json").exists():
            log(f"[oracle] {name} exists, skipped")
            continue
        files = sorted(str(p.relative_to(ROOT)) for p in (ROOT / "data/daily").glob("*.csv"))
        if name.startswith("mon"):
            files = [f"{DAY}-08-11-21.csv"]
        t = threading.Thread(target=sh, args=(["oracle/run.sh", "--out", f"/out/{name}", *args, *files], None, f"oracle-{name}"))
        t.start()
        procs.append(t)
    for t in procs:
        t.join()


def tag_images() -> None:
    for lane in ("a", "b"):
        for image in ("kafka", "flink-job", "producer", "viz"):
            subprocess.run(["docker", "tag", f"trading-trends/{image}:dev", f"issue11-lane-{lane}/{image}:dev"], check=True)


def down(lane: str | None) -> None:
    env = lane_env(lane)
    subprocess.run(["docker", "compose", "down", "--volumes"], cwd=ROOT, env=env,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def calib_config(source: str, name: str) -> Path:
    cfg = json.loads((CONFIGS / "lane" / f"{source}.json").read_text())
    cfg["name"] = name
    path = CONFIGS / "calib" / f"{name}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(cfg, indent=2) + "\n")
    return path


def summaries(exp: Path | None) -> list[dict]:
    if exp is None:
        return []
    out = []
    for r in sorted(exp.glob("r*")):
        cfg, summ = r / "config.json", r / "summary.json"
        if cfg.exists() and summ.exists() and json.loads(cfg.read_text()).get("status") == "COMPLETE":
            out.append(json.loads(summ.read_text()))
    return out


def mean(values: list[float]) -> float | None:
    return statistics.fmean(values) if values else None


def calibrate() -> bool:
    for lane in ("a", "b"):
        down(lane)
    solo_p, solo_l = calib_config("lane-base-p4", "calib-solo-p4"), calib_config("lane-live-300x", "calib-solo-live-300x")
    conc_p, conc_l = calib_config("lane-base-p4", "calib-conc-p4"), calib_config("lane-live-300x", "calib-conc-live-300x")
    nb_p, nb_l = calib_config("lane-base-p4", "calib-neighbour-p4"), calib_config("lane-live-300x", "calib-neighbour-live-300x")

    results = {"solo-p4": run(solo_p, "a"), "solo-live": run(solo_l, "a")}
    # Concurrent: lane B starts the same workload a few seconds earlier so lane A's measured phase overlaps it.
    for key, mine, neighbour in (("conc-p4", conc_p, nb_p), ("conc-live", conc_l, nb_l)):
        t = threading.Thread(target=run, args=(neighbour, "b"))
        t.start()
        time.sleep(5)
        results[key] = run(mine, "a")
        t.join()
    down("b")

    tput = {k: mean([s["job"]["recordsPerSecond"] for s in summaries(results[k])]) for k in ("solo-p4", "conc-p4")}
    queue = {k: mean([s["latency"]["stagesMs"]["queueing"]["p95"] for s in summaries(results[k])
                      if s.get("latency", {}).get("complete")]) for k in ("solo-live", "conc-live")}
    verdict = {"throughput": tput, "queueingP95Ms": queue,
               "experiments": {k: str(v.relative_to(ROOT)) if v else None for k, v in results.items()}}
    ok = None not in tput.values() and None not in queue.values()
    if ok:
        verdict["throughputChange"] = tput["conc-p4"] / tput["solo-p4"] - 1
        verdict["queueingP95ChangeMs"] = queue["conc-live"] - queue["solo-live"]
        ok = (abs(verdict["throughputChange"]) <= THROUGHPUT_TOLERANCE
              and abs(verdict["queueingP95ChangeMs"]) <= QUEUEING_TOLERANCE_MS)
    verdict["tolerances"] = {"throughput": THROUGHPUT_TOLERANCE, "queueingP95Ms": QUEUEING_TOLERANCE_MS}
    verdict["parallel"] = ok
    (RAW / "issue11-calibration.json").write_text(json.dumps(verdict, indent=2) + "\n")
    log(f"[calib] {json.dumps(verdict)}")
    return ok


# Rough minutes per repeat, to balance the two lanes.
def cost(config: Path) -> float:
    cfg = json.loads(config.read_text())
    if cfg["mode"] == "preload":
        return 6
    rate = int(cfg["replayArgs"][cfg["replayArgs"].index("--rate") + 1])
    # A 3 M-row slice spans 09:00 to 09:16:42; the whole day from 09:00 about 8.5 h.
    return 3 + (17 / rate if "--limit" in cfg["replayArgs"] else 510 / rate)


def lanes(parallel: bool) -> None:
    configs = sorted((CONFIGS / "lane").glob("*.json"), key=cost, reverse=True)
    if not parallel:
        for c in configs:
            run(c, "a")
        down("a")
        return
    queues: dict[str, list[Path]] = {"a": [], "b": []}
    load = {"a": 0.0, "b": 0.0}
    for c in configs:
        lane = min(load, key=load.get)
        queues[lane].append(c)
        load[lane] += cost(c)
    log(f"[lanes] a={[c.stem for c in queues['a']]} b={[c.stem for c in queues['b']]}")

    def worker(lane: str) -> None:
        for c in queues[lane]:
            run(c, lane)
        down(lane)

    threads = [threading.Thread(target=worker, args=(lane,)) for lane in queues]
    for t in threads:
        t.start()
    for t in threads:
        t.join()


def main() -> None:
    RAW.mkdir(parents=True, exist_ok=True)
    steps = sys.argv[1:] or ["oracles", "scale", "calibrate", "lanes"]
    log(f"orchestrate {steps}")
    if "oracles" in steps:
        oracles()
    if "scale" in steps:
        for c in sorted((CONFIGS / "scale").glob("*.json")):
            run(c, None)
        down(None)
    if "calibrate" in steps or "lanes" in steps:
        tag_images()
    parallel = calibrate() if "calibrate" in steps else "parallel" in steps
    if "lanes" in steps:
        lanes(parallel)
    if "lanes-host" in steps:  # the lane configs one at a time on the whole host, as the calibration required
        for c in sorted((CONFIGS / "lane").glob("*.json"), key=cost, reverse=True):
            run(c, None)
        down(None)
    if "scale-2tm" in steps:  # added after p16 on one TaskManager ran out of heap in 2 of 3 repeats
        for c in sorted((CONFIGS / "scale").glob("*-2tm.json")):
            run(c, None)
        down(None)
    if "restore" in steps:
        sh(["docker", "compose", "up", "-d"], lane_env(None), "restore")
    log("orchestrate done")


if __name__ == "__main__":
    main()
