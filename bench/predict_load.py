#!/usr/bin/env python3
"""Per-subtask load of the ticks source and q1-ema, predicted from the week's per-symbol counts.

    bench/.venv/bin/python bench/predict_load.py [--partitions 8 16] [--parallelism 1 2 4 8] [--exchanges FR,NL]

The source's subtasks read whole Kafka partitions, assigned as the Kafka connector's enumerator does; q1-ema's
subtasks own key groups of the symbol, as Flink's keyBy assigns them under the default max parallelism, which the job
does not override. Prints one CSV row per partition count and parallelism: each subtask's records and the skew, max
over mean.
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "analysis" / "src"))
from daily_parse import kafka_hash  # noqa: E402

MASK = 0xFFFFFFFF


def signed(x: int) -> int:
    x &= MASK
    return x - (1 << 32) if x & 0x80000000 else x


def rotl(x: int, r: int) -> int:
    return ((x << r) | (x >> (32 - r))) & MASK


def java_hash(s: str) -> int:
    """String.hashCode for ASCII, which every symbol is (D-1)."""
    h = 0
    for ch in s:
        h = (31 * h + ord(ch)) & MASK
    return h


def flink_murmur(code: int) -> int:
    """org.apache.flink.util.MathUtils.murmurHash."""
    code = rotl(code * 0xCC9E2D51 & MASK, 15) * 0x1B873593 & MASK
    code = (rotl(code, 13) * 5 + 0xE6546B64) & MASK
    code ^= 4
    code ^= code >> 16
    code = code * 0x85EBCA6B & MASK
    code ^= code >> 13
    code = code * 0xC2B2AE35 & MASK
    code ^= code >> 16
    v = signed(code)
    return v if v >= 0 else (-v if v != -(1 << 31) else 0)


def max_parallelism(parallelism: int) -> int:
    """Flink's default: the next power of two of 1.5 x parallelism, at least 128."""
    target = parallelism + parallelism // 2
    return min(max(1 << (target - 1).bit_length(), 128), 32768)


def q1_subtask(symbol: str, parallelism: int) -> int:
    groups = max_parallelism(parallelism)
    return flink_murmur(java_hash(symbol)) % groups * parallelism // groups


def source_subtask(partition: int, parallelism: int, topic: str = "ticks") -> int:
    start = (java_hash(topic) * 31 & 0x7FFFFFFF) % parallelism
    return (start + partition) % parallelism


def skew(loads: list[int]) -> float:
    mean = sum(loads) / len(loads)
    return max(loads) / mean if mean else float("nan")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--symbols", type=Path, default=ROOT / "analysis" / "out" / "week" / "symbols.csv")
    parser.add_argument("--partitions", type=int, nargs="+", default=[8, 16, 32])
    parser.add_argument("--parallelism", type=int, nargs="+", default=[1, 2, 4, 6, 8, 16, 32])
    parser.add_argument("--exchanges", help="only symbols with these suffixes, e.g. FR,NL")
    args = parser.parse_args()

    with args.symbols.open() as f:
        rows = list(csv.DictReader(f))
    if args.exchanges:
        keep = {e.strip() for e in args.exchanges.split(",")}
        rows = [r for r in rows if r["symbol"].rsplit(".", 1)[-1] in keep]
    out = csv.writer(sys.stdout)
    out.writerow(["partitions", "parallelism", "sourceRecords", "sourceSkew", "q1PriceEvents", "q1Skew"])
    for partitions in args.partitions:
        for p in args.parallelism:
            source, q1 = [0] * p, [0] * p
            for r in rows:
                source[source_subtask(kafka_hash(r["symbol"]) % partitions, p)] += int(r["raw_events"])
                q1[q1_subtask(r["symbol"], p)] += int(r["accepted_price_events"])
            out.writerow([partitions, p, "/".join(map(str, source)), f"{skew(source):.4f}",
                          "/".join(map(str, q1)), f"{skew(q1):.4f}"])


if __name__ == "__main__":
    main()
