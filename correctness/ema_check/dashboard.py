"""Check what the dashboard shows against the topics and the oracle (#13).

The input is a capture written by `viz/web/scripts/capture.mjs`, which applies the WebSocket
messages with the frontend's own reducer. Its views are compared with two references:

- the export of `ema-results` and `advisories` (`export`), which is what the job published, so
  the dashboard must show exactly that within its span, nothing more and nothing less;
- an oracle run, which says what those values must be (D-18): equal closes, EMAs within the
  tolerance, and the same advisories for every window the dashboard shows. A symbol subscribed
  during the run is published from some window on; its earlier windows are absent on the
  dashboard, but its first advisory after that is still measured against its unpublished history.

Every check reports a count of mismatches and a few examples. The gate passes only if all are 0.
"""

from __future__ import annotations

import json
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path

import polars as pl

from .compare import TOLERANCE

EXAMPLES = 5


def _offset(zone: str) -> timezone:
    sign = -1 if zone.startswith("-") else 1
    hours, minutes = zone[1:].split(":")
    return timezone(sign * timedelta(hours=int(hours), minutes=int(minutes)))


def shown_time(window_end: int, zone: str) -> str:
    """The feed's time column: `Mon 08-11-2021 09:05` at the zone offset."""
    t = datetime.fromtimestamp(window_end / 1000, _offset(zone))
    return f"{t.strftime('%a')} {t.strftime('%d-%m-%Y %H:%M')}"


def _sign(ema38: float, ema100: float) -> str:
    return "+" if ema38 > ema100 else "-" if ema38 < ema100 else "0"


def _check(name: str, mismatches: list, total: int) -> dict:
    return {"name": name, "checked": total, "mismatches": len(mismatches), "examples": mismatches[:EXAMPLES],
            "pass": len(mismatches) == 0}


def _rows(df: pl.DataFrame, fields: list[str]) -> list[dict]:
    return df.select(fields).to_dicts()


def _close(a: float, b: float, tolerance: float) -> bool:
    return math.isfinite(a) and math.isfinite(b) and abs(a - b) <= tolerance


def check(capture_path: Path, job: Path, oracle: Path, out: Path, expect_traders: Path | None = None,
          tolerance: float = TOLERANCE) -> dict:
    cap = json.loads(capture_path.read_text())
    zone = (cap.get("clock") or {}).get("zoneOffset", "+02:00")
    span_end = cap["spanEnd"]
    span_start = span_end - cap["slots"] * cap["slotMs"]

    def in_span(df: pl.DataFrame) -> pl.DataFrame:
        return df.filter((pl.col("windowEnd") > span_start) & (pl.col("windowEnd") <= span_end))

    topic_ema = in_span(pl.read_parquet(job / "ema_results.parquet")).unique(["symbol", "windowEnd"], keep="last")
    topic_adv = in_span(pl.read_parquet(job / "advisories.parquet")).unique(["symbol", "windowEnd"], keep="last")
    oracle_all_ema = pl.read_parquet(oracle / "ema_results.parquet")
    # Each symbol's first window over the whole input, which the dashboard may never have seen.
    oracle_first = dict(oracle_all_ema.group_by("symbol").agg(pl.col("windowEnd").min()).iter_rows())
    oracle_ema = in_span(oracle_all_ema)
    oracle_adv = in_span(pl.read_parquet(oracle / "advisories.parquet"))

    t_ema = {(r["symbol"], r["windowEnd"]): r for r in _rows(topic_ema, ["symbol", "windowEnd", "secType", "close", "ema38", "ema100"])}
    o_ema = {(r["symbol"], r["windowEnd"]): r for r in _rows(oracle_ema, ["symbol", "windowEnd", "secType", "close", "ema38", "ema100"])}
    t_adv = {(r["symbol"], r["windowEnd"]): r for r in _rows(topic_adv, ["symbol", "windowEnd", "type", "ema38", "ema100", "maxIngestTs", "emitTs"])}
    o_adv = {(r["symbol"], r["windowEnd"]): r for r in _rows(oracle_adv, ["symbol", "windowEnd", "type", "ema38", "ema100"])}

    checks = []

    # Heatmap: one cell per published window in the span, signed like the topic and the oracle.
    shown_cells = {(row["symbol"], w): sign for row in cap["rows"] for w, sign in row["cells"]}
    topic_cells = {k: _sign(r["ema38"], r["ema100"]) for k, r in t_ema.items()}
    diff = [{"key": list(k), "shown": shown_cells.get(k), "topic": topic_cells.get(k)}
            for k in sorted(set(shown_cells) | set(topic_cells)) if shown_cells.get(k) != topic_cells.get(k)]
    checks.append(_check("heatmapCellsEqualTopic", diff, len(topic_cells)))
    diff = [{"key": list(k), "shown": s, "oracle": None if k not in o_ema else _sign(o_ema[k]["ema38"], o_ema[k]["ema100"])}
            for k, s in sorted(shown_cells.items())
            if k not in o_ema or _sign(o_ema[k]["ema38"], o_ema[k]["ema100"]) != s]
    checks.append(_check("heatmapCellsEqualOracle", diff, len(shown_cells)))
    sec_types: dict[str, set] = {}
    for (sym, _), r in t_ema.items():
        sec_types.setdefault(sym, set()).add(r["secType"])
    meta = [{"symbol": row["symbol"], "shown": [row["secType"], row["exchange"]], "topic": sorted(sec_types.get(row["symbol"], []))}
            for row in cap["rows"]
            if row["symbol"] in sec_types and (sec_types[row["symbol"]] != {row["secType"]}
                                               or row["exchange"] != row["symbol"].rsplit(".", 1)[-1])]
    checks.append(_check("heatmapSecTypeAndExchange", meta, len(cap["rows"])))

    # Chart: every point of each charted symbol, exactly as published and within the oracle's tolerance.
    charted = cap["series"]
    by_symbol: dict[str, dict] = {}
    for (sym, w), r in t_ema.items():
        by_symbol.setdefault(sym, {})[w] = r
    diff_topic, diff_oracle, total_points = [], [], 0
    for symbol, s in sorted(charted.items()):
        shown = {p[0]: p for p in s["points"]}
        published = by_symbol.get(symbol, {})
        total_points += len(published)
        for w in sorted(set(shown) | set(published)):
            p, r = shown.get(w), published.get(w)
            if p is None or r is None or (p[1], p[2], p[3]) != (r["close"], r["ema38"], r["ema100"]):
                diff_topic.append({"symbol": symbol, "windowEnd": w, "shown": p, "topic": None if r is None else [r["close"], r["ema38"], r["ema100"]]})
        for w, p in sorted(shown.items()):
            o = o_ema.get((symbol, w))
            if o is None or p[1] != o["close"] or not _close(p[2], o["ema38"], tolerance) or not _close(p[3], o["ema100"], tolerance):
                diff_oracle.append({"symbol": symbol, "windowEnd": w, "shown": p, "oracle": None if o is None else [o["close"], o["ema38"], o["ema100"]]})
    checks.append(_check("chartPointsEqualTopic", diff_topic, total_points))
    checks.append(_check("chartPointsEqualOracle", diff_oracle, total_points))

    # Feed: exactly the published advisories in the span, which are the oracle's for the shown windows.
    feed = {(a["symbol"], a["windowEnd"]): a for a in cap["feed"]}
    diff = [{"key": list(k), "shown": None if k not in feed else [feed[k]["type"], feed[k]["ema38"], feed[k]["ema100"]],
             "topic": None if k not in t_adv else [t_adv[k]["type"], t_adv[k]["ema38"], t_adv[k]["ema100"]]}
            for k in sorted(set(feed) | set(t_adv))
            if k not in feed or k not in t_adv
            or (feed[k]["type"], feed[k]["ema38"], feed[k]["ema100"]) != (t_adv[k]["type"], t_adv[k]["ema38"], t_adv[k]["ema100"])]
    checks.append(_check("feedEqualsTopic", diff, len(t_adv)))
    expected = {k: a for k, a in o_adv.items() if k in shown_cells}
    diff = [{"key": list(k), "shown": None if k not in feed else feed[k]["type"], "oracle": None if k not in expected else expected[k]["type"]}
            for k in sorted(set(feed) | set(expected))
            if k not in feed or k not in expected or feed[k]["type"] != expected[k]["type"]
            or not _close(feed[k]["ema38"], expected[k]["ema38"], tolerance)
            or not _close(feed[k]["ema100"], expected[k]["ema100"], tolerance)]
    checks.append(_check("feedEqualsOracleForShownWindows", diff, len(expected)))
    diff = [{"key": [a["symbol"], a["windowEnd"]], "shown": a["shown"], "expected": shown_time(a["windowEnd"], zone)}
            for a in cap["feed"] if a["shown"] != shown_time(a["windowEnd"], zone)]
    checks.append(_check("feedTimesAtZoneOffset", diff, len(cap["feed"])))
    diff = [{"key": [a["symbol"], a["windowEnd"]], "shown": a["firstWindow"], "oracle": oracle_first.get(a["symbol"]) == a["windowEnd"]}
            for a in cap["feed"] if a["firstWindow"] != (oracle_first.get(a["symbol"]) == a["windowEnd"])]
    checks.append(_check("firstWindowFlagEqualsOracle", diff, len(cap["feed"])))
    diff = []
    for a in cap["feed"]:
        t = t_adv.get((a["symbol"], a["windowEnd"]))
        want = None if t is None or t["maxIngestTs"] is None or t["emitTs"] is None else t["emitTs"] - t["maxIngestTs"]
        if a["latencyMs"] != want:
            diff.append({"key": [a["symbol"], a["windowEnd"]], "shown": a["latencyMs"], "topic": want})
    checks.append(_check("feedLatencyEqualsTopic", diff, len(cap["feed"])))
    want = [[a["symbol"], a["windowEnd"]] for a in cap["feed"] if not a["firstWindow"]]
    checks.append(_check("defaultFeedHidesOnlyFirstWindowBuys", [] if cap["feedDefault"] == want else
                         [{"shown": len(cap["feedDefault"]), "expected": len(want)}], len(cap["feed"])))
    order = [a["windowEnd"] for a in cap["feed"]]
    checks.append(_check("feedNewestFirst", [] if order == sorted(order, reverse=True) else ["out of order"], len(order)))

    # Markers: each charted symbol's markers are its advisories in the feed, at their EMA38.
    feed_by_symbol: dict[str, list] = {}
    for a in cap["feed"]:
        feed_by_symbol.setdefault(a["symbol"], []).append([a["windowEnd"], a["type"], a["ema38"]])
    diff, total = [], 0
    for symbol, s in sorted(charted.items()):
        mine = sorted(feed_by_symbol.get(symbol, []))
        total += len(mine)
        if sorted(s["markers"]) != mine:
            diff.append({"symbol": symbol, "markers": s["markers"], "feed": mine})
    checks.append(_check("chartMarkersEqualFeed", diff, total))

    # Subscriptions: each trader's feed is the advisories of the symbols in their current list.
    diff = []
    for trader, symbols in sorted(cap["traders"].items()):
        want = sorted((a["symbol"], a["windowEnd"]) for a in cap["feed"] if "*" in symbols or a["symbol"] in symbols)
        got = sorted(tuple(k) for k in cap["feeds"].get(trader, []))
        if got != want:
            diff.append({"trader": trader, "shown": len(got), "expected": len(want)})
    checks.append(_check("feedPerTraderFollowsCurrentList", diff, len(cap["traders"])))
    if expect_traders is not None:
        want = {t: sorted(set(v)) for t, v in json.loads(expect_traders.read_text()).items()}
        got = {t: sorted(v) for t, v in cap["traders"].items()}
        checks.append(_check("tradersEqualSubscriptionsWritten", [] if got == want else [{"shown": got, "expected": want}], len(want)))

    # Heatmap order: newest published crossover first, first-window BUYs not counting, symbols
    # without one last, ties by symbol.
    last = {}
    for a in cap["feed"]:
        if a["firstWindow"]:
            continue
        last[a["symbol"]] = max(last.get(a["symbol"], a["windowEnd"]), a["windowEnd"])
    want = sorted((row["symbol"] for row in cap["rows"]),
                  key=lambda s: (s not in last, -last.get(s, 0), s))
    checks.append(_check("heatmapSortedByCrossover", [] if cap["heatmapByCrossover"] == want else
                         [{"shown": cap["heatmapByCrossover"][:10], "expected": want[:10]}], len(want)))

    # Clock: the latest window is the newest published windowEnd.
    newest = max((w for _, w in t_ema), default=None)
    results_ms = (cap.get("clock") or {}).get("resultsMs")
    checks.append(_check("clockLatestWindow", [] if results_ms == newest else [{"shown": results_ms, "topic": newest}], 1))

    # Not a check: where each symbol's publication starts, and the oracle advisories before it.
    first_shown = {}
    for sym, w in shown_cells:
        first_shown[sym] = min(first_shown.get(sym, w), w)
    o_adv_by_symbol: dict[str, list] = {}
    for sym, w in o_adv:
        o_adv_by_symbol.setdefault(sym, []).append(w)
    cells_per_symbol: dict[str, int] = {}
    for sym, _ in shown_cells:
        cells_per_symbol[sym] = cells_per_symbol.get(sym, 0) + 1
    publication = []
    for sym in sorted(first_shown):
        publication.append({"symbol": sym, "firstShownWindowEnd": first_shown[sym], "firstShown": shown_time(first_shown[sym], zone),
                            "windowsShown": cells_per_symbol[sym],
                            "advisoriesShown": len(feed_by_symbol.get(sym, [])),
                            "oracleAdvisoriesBeforePublication": sum(1 for w in o_adv_by_symbol.get(sym, []) if w < first_shown[sym])})
    report = {
        "capture": str(capture_path),
        "job": str(job),
        "oracle": str(oracle),
        "zoneOffset": zone,
        "span": {"start": span_start, "end": span_end, "startShown": shown_time(span_start, zone), "endShown": shown_time(span_end, zone)},
        "shown": {"symbols": len(cap["rows"]), "cells": len(shown_cells), "advisories": len(feed),
                  "charted": len(charted), "traders": len(cap["traders"])},
        "checks": checks,
        "publication": publication,
        "pass": all(c["pass"] for c in checks),
    }
    out.mkdir(parents=True, exist_ok=True)
    (out / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    return report
