"""Latency of a live replay, from admission to output, split into the waits that make it up (#37).

Every timestamp here is wall-clock epoch milliseconds from one host's clock. No 2021 event time
is ever subtracted from one: event times only decide when the replayed feed and the watermark
reached a window's end. bench/README.md, "Latency", defines each stage.
"""

from __future__ import annotations

import bisect
import datetime as dt
import functools
import heapq
import json
import math
from pathlib import Path

import polars as pl

STAGES = ("windowWait", "boundWait", "alignmentWait", "queueing", "sinkHandoff")
PERCENTILES = (50, 95, 99)

_SECONDS_PER_DAY = 86_400
_EPOCH = dt.date(1970, 1, 1).toordinal()


def zone_offset_ms(text: str) -> int:
    """The job's `--zone-offset` (`Z` or `+HH:MM`) in milliseconds."""
    return int(dt.datetime.strptime("+0000" if text in ("Z", "z") else text, "%z").utcoffset().total_seconds() * 1000)


def price_event(value: bytes | None, zone_offset_ms: int) -> dict | None:
    """The event time of a `ticks` value that the job treats as a price event, else None.

    Mirrors the job's TickParser on the 7-field projection, including `Date` for a missing
    `Trading date` (D-25) and truncation beyond milliseconds (D-22).
    """
    if value is None:
        return None
    fields = value.decode("utf-8", "replace").split(",")
    if len(fields) != 7 or not fields[0] or not fields[4]:
        return None
    try:
        price = float(fields[4])
    except ValueError:
        return None
    if not math.isfinite(price) or price <= 0:
        return None
    date = fields[6] or fields[2]
    time = fields[5]
    if not date or not time:
        return None
    day, millis = _epoch_day(date), _millis_of_day(time)
    if day is None or millis is None:
        return None
    return {"eventTime": day * _SECONDS_PER_DAY * 1000 + millis - zone_offset_ms}


@functools.lru_cache(maxsize=64)
def _epoch_day(date: str) -> int | None:
    if len(date) != 10 or date[2] != "-" or date[5] != "-" or not (date[:2] + date[3:5] + date[6:]).isdigit():
        return None
    try:
        return dt.date(int(date[6:]), int(date[3:5]), int(date[:2])).toordinal() - _EPOCH
    except ValueError:
        return None


def _millis_of_day(time: str) -> int | None:
    if len(time) < 8 or time[2] != ":" or time[5] != ":" or not (time[:2] + time[3:5] + time[6:8]).isdigit():
        return None
    hours, minutes, seconds = int(time[:2]), int(time[3:5]), int(time[6:8])
    if hours > 23 or minutes > 59 or seconds > 59:
        return None
    millis = 0
    if len(time) > 8:
        fraction = time[9:]
        if time[8] != "." or not 1 <= len(fraction) <= 9 or not fraction.isdigit():
            return None
        millis = int(fraction[:3].ljust(3, "0"))
    return ((hours * 60 + minutes) * 60 + seconds) * 1000 + millis


class Feed:
    """When the replayed price events first reached an event time, on any partition."""

    def __init__(self, events: pl.DataFrame):
        ordered = events.sort("kafkaTs", "partition", "offset")
        self.times: list[int] = ordered["kafkaTs"].to_list()
        self.frontier: list[int] = ordered["eventTime"].cum_max().to_list()

    def reached(self, event_time: int) -> int | None:
        i = bisect.bisect_left(self.frontier, event_time)
        return self.times[i] if i < len(self.times) else None

    def at(self, wall_ms: int) -> int | None:
        i = bisect.bisect_right(self.times, wall_ms)
        return self.frontier[i - 1] if i else None


class Watermark:
    """The job's watermark as the replay's admissions alone would drive it (docs/interfaces.md).

    Each partition's watermark is its largest event time minus the bound minus 1 ms. A partition
    is idle once it has had no price event for the idleness timeout, counted from `start_ms` for
    one that has had none, and active again with its next event. The watermark is the minimum
    over active partitions, held while all are idle, and never decreases. It changes at the
    admission times of price events and at idleness expiries; `times` and `values` hold those
    changes. Everything after admission (broker append, fetch, the source's periodic watermark
    emission, the idleness check interval, network buffers) is left out, so the job's watermark
    trails this one by what the `queueing` stage measures.
    """

    def __init__(self, events: pl.DataFrame, partitions: list[int], bound_ms: int, idleness_ms: int, start_ms: int):
        ordered = events.sort("kafkaTs", "partition", "offset").select("kafkaTs", "partition", "eventTime")
        highest: dict[int, float] = {p: -math.inf for p in partitions}
        last = {p: start_ms for p in partitions}
        active = set(partitions)
        expiries = [(start_ms + idleness_ms, p) for p in partitions] if idleness_ms > 0 else []
        heapq.heapify(expiries)
        self.times: list[int] = []
        self.values: list[int] = []
        current = -math.inf

        def update(at: int):
            nonlocal current
            if active:
                candidate = min(highest[p] for p in active) - bound_ms - 1
                if candidate > current:
                    current = candidate
                    self.times.append(at)
                    self.values.append(int(candidate))

        def expire(until: float):
            while expiries and expiries[0][0] <= until:
                at, p = heapq.heappop(expiries)
                if last[p] + idleness_ms > at:
                    heapq.heappush(expiries, (last[p] + idleness_ms, p))
                else:
                    active.discard(p)
                    update(at)

        for at, p, event_time in ordered.iter_rows():
            expire(at)
            if event_time > highest[p]:
                highest[p] = event_time
            last[p] = at
            if p not in active:
                active.add(p)
                heapq.heappush(expiries, (at + idleness_ms, p))
            update(at)
        expire(math.inf)

    def passed(self, event_time: int) -> int | None:
        """When the watermark first reached `event_time`."""
        i = bisect.bisect_left(self.values, event_time)
        return self.times[i] if i < len(self.times) else None

    def at(self, wall_ms: int) -> int | None:
        i = bisect.bisect_right(self.times, wall_ms)
        return self.values[i - 1] if i else None


def stats(values: list[float]) -> dict:
    """Count, mean, nearest-rank percentiles and maximum."""
    if not values:
        return {"n": 0}
    ordered = sorted(values)
    rank = lambda q: ordered[max(0, math.ceil(q / 100 * len(ordered)) - 1)]
    return {"n": len(ordered), "mean": sum(ordered) / len(ordered),
            **{f"p{q}": rank(q) for q in PERCENTILES}, "max": ordered[-1]}


def decompose(results: pl.DataFrame, feed: Feed, watermark: Watermark, bound_ms: int,
              interval: tuple[int, int], drain_start_ms: int | None) -> dict:
    """Each measured result's latency and its stages, with what the interval leaves out.

    A result is measured when its last contributing price event was admitted inside the
    measurement interval and the job emitted it before the drain began. The stages run along
    one chain of instants: admission of the window's last price event, the feed reaching the
    window's end, the feed reaching the end plus the bound, the modelled watermark passing the
    end, the job emitting the result, and the output record's timestamp. Each instant is at
    least the one before it, except emission, so the stages add up to the total and only
    `queueing` can be negative, where the model's watermark is later than the job's.
    """
    excluded = {"beforeInterval": 0, "afterInterval": 0, "drain": 0}
    unmodelled = 0
    columns: dict[str, list[float]] = {name: [] for name in (*STAGES, "watermarkWait", "emitLatency", "outputLatency")}
    for window_end, admitted, emitted, output in results.select("windowEnd", "maxIngestTs", "emitTs", "kafkaTs").iter_rows():
        if admitted < interval[0]:
            excluded["beforeInterval"] += 1
            continue
        if admitted > interval[1]:
            excluded["afterInterval"] += 1
            continue
        if drain_start_ms is not None and emitted >= drain_start_ms:
            excluded["drain"] += 1
            continue
        columns["emitLatency"].append(emitted - admitted)
        columns["outputLatency"].append(output - admitted)
        ready = watermark.passed(window_end - 1)
        if ready is None:
            unmodelled += 1
            continue
        window = max(admitted, feed.reached(window_end))
        bound = max(window, feed.reached(window_end + bound_ms))
        aligned = max(bound, ready)
        for name, value in (("windowWait", window - admitted), ("boundWait", bound - window),
                            ("alignmentWait", aligned - bound), ("watermarkWait", aligned - window),
                            ("queueing", emitted - aligned), ("sinkHandoff", output - emitted)):
            columns[name].append(value)
    return {
        "exported": results.height,
        "measured": len(columns["emitLatency"]),
        "excluded": excluded,
        "unmodelled": unmodelled,
        "negativeQueueing": sum(1 for v in columns["queueing"] if v < 0),
        "stagesMs": {name: stats(values) for name, values in columns.items()},
    }


def watermark_lag(samples: list[tuple[int, int]], feed: Feed, watermark: Watermark, interval: tuple[int, int]) -> dict:
    """The job's watermark against the feed, at the instants Prometheus read it.

    `eventMs`: event time the feed had reached minus the watermark. `wallMs`: how long before the
    sample the feed had first reached the watermark's event time. Both include the bound.
    `modelAheadEventMs`: the modelled watermark minus the job's at the same instant.
    """
    event, wall, ahead = [], [], []
    for at, value in samples:
        if not interval[0] <= at <= interval[1] or value <= 0:
            continue
        frontier, reached, modelled = feed.at(at), feed.reached(value), watermark.at(at)
        if frontier is None:
            continue
        event.append(frontier - value)
        if reached is not None:
            wall.append(at - reached)
        if modelled is not None:
            ahead.append(modelled - value)
    return {"samples": len(event), "eventMs": stats(event), "wallMs": stats(wall), "modelAheadEventMs": stats(ahead)}


def prometheus_samples(metrics: dict, values: str, stamps: str) -> list[tuple[int, int]]:
    """(scrape time ms, value) pairs from a range query and its `timestamp()` twin, one per scrape."""
    def series(name):
        result = metrics.get("queries", {}).get(name, {}).get("result", [])
        return {json.dumps({k: v for k, v in s["metric"].items() if k != "__name__"}, sort_keys=True): dict(s["values"])
                for s in result}
    stamped = series(stamps)
    pairs = set()
    for labels, points in series(values).items():
        stamps_at = stamped.get(labels, {})
        for t, value in points.items():
            if t in stamps_at and value not in ("NaN", "+Inf", "-Inf"):
                pairs.add((round(float(stamps_at[t]) * 1000), round(float(value))))
    return sorted(pairs)


def latency(export_dir: Path, replay_report: Path, metrics: Path | None, bound_ms: int, idleness_ms: int,
            interval: tuple[int, int], idle_from_ms: int, drain_start_ms: int | None, out: Path) -> dict:
    results = pl.read_parquet(export_dir / "ema_results.parquet")
    events = pl.read_parquet(export_dir / "price_events.parquet")
    partitions = [p["partition"] for p in json.loads(replay_report.read_text())["offsets"]]
    feed = Feed(events)
    model = Watermark(events, partitions, bound_ms, idleness_ms, idle_from_ms)
    samples = prometheus_samples(json.loads(metrics.read_text()), "q1InputWatermark", "q1InputWatermarkScrapeTs") if metrics else []
    report = {
        "interval": {"startMs": interval[0], "endMs": interval[1]},
        "drainStartMs": drain_start_ms,
        "model": {"boundMs": bound_ms, "idlenessMs": idleness_ms, "partitions": partitions,
                  "idleFromMs": idle_from_ms, "priceEvents": events.height, "watermarkChanges": len(model.times)},
        "timestampTypes": {"admission": sorted(set(events["kafkaTsType"].to_list())),
                           "output": sorted(set(results["kafkaTsType"].to_list()))},
        **decompose(results, feed, model, bound_ms, interval, drain_start_ms),
        "watermarkLag": watermark_lag(samples, feed, model, interval),
    }
    out.mkdir(parents=True, exist_ok=True)
    (out / "latency.json").write_text(json.dumps(report, indent=2) + "\n")
    return report
