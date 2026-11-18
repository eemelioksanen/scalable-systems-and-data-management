"""Writes the #11 experiment configs to bench/configs/issue11/.

Two groups:
- scale/: parallelism and partition scaling, run one at a time on the whole host.
- lane/: everything else at p4 on 8 partitions, run on a stack pinned to 4 cores / 8 threads.

Trader subscriptions are generated here with a fixed seed so every repeat and every rerun gets the same lists.
"""

import csv
import json
import random
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "bench/configs/issue11"
WEEK = ["data/daily"]
MONDAY = ["data/daily/debs2022-gc-trading-day-08-11-21.csv"]
REPEATS = 3
SEED = 11
SYMBOLS_PER_TRADER = 20

PRELOAD_JOB = {"watermarkBoundMs": 64265001, "idlenessMs": 0, "zoneOffset": "+02:00"}  # D-38 finite input
LIVE_JOB = {"watermarkBoundMs": 1000, "idlenessMs": 30000, "zoneOffset": "+02:00"}  # D-38 live default
ALL = [{"trader": "all", "symbols": ["*"]}]


def stack(parallelism: int, partitions: int = 8) -> dict:
    return {"partitions": partitions, "taskmanagers": 1, "slots": parallelism, "parallelism": parallelism}


def preload(name: str, **extra) -> dict:
    cfg = {"name": name, "mode": "preload", "repeats": REPEATS, "input": WEEK, "warmupS": 15,
           "job": PRELOAD_JOB, "stack": stack(4), "subscriptions": ALL, "oracle": "oracle/out/week"}
    cfg.update(extra)
    return cfg


def live(name: str, rate: int, limit: int | None = None, job: dict = LIVE_JOB, oracle: str | None = "oracle/out/mon-bound1s") -> dict:
    args = ["--rate", str(rate), "--pace-from", "2021-11-08T09:00"]
    if limit:
        args += ["--limit", str(limit)]
        oracle = None  # the oracle covers whole files
    cfg = {"name": name, "mode": "live", "repeats": REPEATS, "input": MONDAY, "replayArgs": args, "warmupS": 25,
           "job": job, "stack": stack(4), "subscriptions": ALL}
    if oracle:
        cfg["oracle"] = oracle
    return cfg


def traders(count: int, skewed: bool) -> list[dict]:
    with open(ROOT / "analysis/out/week/symbols.csv") as f:
        rows = [(r["symbol"], int(r["accepted_price_events"])) for r in csv.DictReader(f)]
    rows = [r for r in rows if r[1] > 0]
    symbols = [s for s, _ in rows]
    weights = [w for _, w in rows]
    rng = random.Random(f"{SEED}-{count}-{'skewed' if skewed else 'uniform'}")
    subs = []
    for i in range(count):
        if skewed:  # weighted by price events without replacement: popular symbols dominate
            chosen: set[str] = set()
            while len(chosen) < SYMBOLS_PER_TRADER:
                chosen.add(rng.choices(symbols, weights)[0])
        else:
            chosen = set(rng.sample(symbols, SYMBOLS_PER_TRADER))
        trader = f"trader-{i + 1:04d}"
        subs.append({"trader": trader, "symbols": sorted(chosen)})
    return subs


def main() -> None:
    configs: dict[str, dict] = {}

    for partitions, levels in ((8, (1, 2, 4, 8)), (16, (4, 8, 16))):
        for p in levels:
            name = f"scale-p{p}-{partitions}part"
            configs[f"scale/{name}"] = preload(name, stack=stack(p, partitions))

    lane = {"lane-base-p4": preload("lane-base-p4")}
    for ex in ("FR", "FR,NL"):
        name = "lane-exch-" + ex.replace(",", "-").lower()
        lane[name] = preload(name, replayArgs=["--exchanges", ex])
        del lane[name]["oracle"]  # the harness refuses an oracle with --exchanges
    for count in (1, 10, 100, 1000):
        for skewed in (False, True):
            name = f"lane-traders-{count}-{'skewed' if skewed else 'uniform'}"
            lane[name] = preload(name, subscriptions=traders(count, skewed))
            del lane[name]["oracle"]  # the oracle publishes every symbol, a subset subscription does not
    for key, value, oracle in (("operator", "window", "oracle/out/week"),
                               ("stateBackend", "rocksdb", "oracle/out/week"),
                               ("ticksEncoding", "binary", "oracle/out/week"),
                               ("filtering", "replayer", "oracle/out/week"),
                               ("emptyWindows", "carry-forward", "oracle/out/week-carry-forward")):
        name = f"lane-variant-{value}"
        lane[name] = preload(name, variant={key: value}, oracle=oracle)
    lane["lane-live-1x-slice"] = live("lane-live-1x-slice", 1, limit=3_000_000)
    lane["lane-live-10x-slice"] = live("lane-live-10x-slice", 10, limit=3_000_000)
    for rate in (100, 300, 1000):
        lane[f"lane-live-{rate}x"] = live(f"lane-live-{rate}x", rate)
    lane["lane-live-100x-b60s"] = live("lane-live-100x-b60s", 100, job={**LIVE_JOB, "watermarkBoundMs": 60000},
                                        oracle="oracle/out/mon-bound60s")
    for name, cfg in lane.items():
        configs[f"lane/{name}"] = cfg

    for path, cfg in configs.items():
        target = OUT / f"{path}.json"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(cfg, indent=2) + "\n")
    print(f"wrote {len(configs)} configs to {OUT.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
