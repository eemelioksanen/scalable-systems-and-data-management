import copy
import json

import polars as pl

from ema_check import dashboard

SLOT = 300_000
T0 = 1_636_355_100_000  # 09:05 on 08-11-2021 at +02:00


def _write(directory, ema, adv):
    directory.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(ema, schema=["symbol", "secType", "windowEnd", "close", "ema38", "ema100"], orient="row").write_parquet(directory / "ema_results.parquet")
    pl.DataFrame(adv, schema=["symbol", "windowEnd", "type", "ema38", "ema100"], orient="row").write_parquet(directory / "advisories.parquet")


def _fixture(tmp_path):
    ema = [("A.FR", "E", T0, 10.0, 0.5, 0.2), ("A.FR", "E", T0 + SLOT, 9.0, 0.4, 0.6), ("B.NL", "I", T0 + SLOT, 5.0, 0.1, 0.05)]
    oracle_adv = [("A.FR", T0, "BUY", 0.5, 0.2), ("A.FR", T0 + SLOT, "SELL", 0.4, 0.6), ("B.NL", T0, "BUY", 0.07, 0.01),
                  ("B.NL", T0 + SLOT, "BUY", 0.1, 0.05)]
    # B.NL was subscribed from its second window on: its first window is not published.
    _write(tmp_path / "oracle", ema + [("B.NL", "I", T0, 4.0, 0.07, 0.01)], oracle_adv)
    _write(tmp_path / "job", ema, [a for a in oracle_adv if (a[0], a[1]) != ("B.NL", T0)])
    feed = [
        {"symbol": "B.NL", "windowEnd": T0 + SLOT, "type": "BUY", "secType": "I", "exchange": "NL", "ema38": 0.1, "ema100": 0.05, "shown": "Mon 08-11-2021 09:10"},
        {"symbol": "A.FR", "windowEnd": T0 + SLOT, "type": "SELL", "secType": "E", "exchange": "FR", "ema38": 0.4, "ema100": 0.6, "shown": "Mon 08-11-2021 09:10"},
        {"symbol": "A.FR", "windowEnd": T0, "type": "BUY", "secType": "E", "exchange": "FR", "ema38": 0.5, "ema100": 0.2, "shown": "Mon 08-11-2021 09:05"},
    ]
    cap = {
        "spanEnd": T0 + SLOT, "slots": 288, "slotMs": SLOT,
        "clock": {"zoneOffset": "+02:00", "resultsMs": T0 + SLOT},
        "traders": {"t1": ["A.FR"], "all": ["*"]},
        "rows": [{"symbol": "A.FR", "secType": "E", "exchange": "FR", "cells": [[T0, "+"], [T0 + SLOT, "-"]]},
                 {"symbol": "B.NL", "secType": "I", "exchange": "NL", "cells": [[T0 + SLOT, "+"]]}],
        "heatmapByCrossover": ["A.FR", "B.NL"],
        "feed": feed,
        "feeds": {"t1": [["A.FR", T0 + SLOT], ["A.FR", T0]], "all": [[a["symbol"], a["windowEnd"]] for a in feed]},
        "series": {"A.FR": {"firstWindowEnd": T0, "points": [[T0, 10.0, 0.5, 0.2], [T0 + SLOT, 9.0, 0.4, 0.6]],
                            "markers": [[T0, "BUY", 0.5], [T0 + SLOT, "SELL", 0.4]]}},
    }
    return cap


def _run(tmp_path, cap):
    path = tmp_path / "capture.json"
    path.write_text(json.dumps(cap))
    return dashboard.check(path, tmp_path / "job", tmp_path / "oracle", tmp_path / "out")


def _failed(report):
    return sorted(c["name"] for c in report["checks"] if not c["pass"])


def test_a_faithful_capture_passes_and_reports_late_publication(tmp_path):
    report = _run(tmp_path, _fixture(tmp_path))
    assert _failed(report) == []
    assert report["pass"]
    b = next(p for p in report["publication"] if p["symbol"] == "B.NL")
    assert b["oracleAdvisoriesBeforePublication"] == 1


def test_a_missing_marker_a_wrong_time_and_a_wrong_trader_feed_are_caught(tmp_path):
    cap = _fixture(tmp_path)
    bad = copy.deepcopy(cap)
    bad["series"]["A.FR"]["markers"].pop()
    bad["feed"][2]["shown"] = "Mon 08-11-2021 07:05"
    bad["feeds"]["t1"].pop()
    assert _failed(_run(tmp_path, bad)) == ["chartMarkersEqualFeed", "feedPerTraderFollowsCurrentList", "feedTimesAtZoneOffset"]


def test_a_dropped_advisory_or_a_changed_value_is_caught(tmp_path):
    cap = _fixture(tmp_path)
    bad = copy.deepcopy(cap)
    del bad["feed"][0]
    bad["series"]["A.FR"]["points"][0][2] = 0.5000001
    bad["rows"][1]["cells"][0][1] = "-"
    failed = _failed(_run(tmp_path, bad))
    assert {"feedEqualsTopic", "feedEqualsOracleForShownWindows", "chartPointsEqualTopic", "chartPointsEqualOracle",
            "heatmapCellsEqualTopic", "heatmapCellsEqualOracle"} <= set(failed)
