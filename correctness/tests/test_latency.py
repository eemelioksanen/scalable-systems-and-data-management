import json

import polars as pl
import pytest

from ema_check.latency import Feed, Watermark, decompose, latency, price_event, prometheus_samples, stats, watermark_lag

DAY = 18939  # 08-11-2021
MINUTE = 60_000


def tick(last="10.5", trading_time="09:00:00.1234", trading_date="08-11-2021", date="08-11-2021"):
    return f"S.NL,E,{date},09:00:00.000,{last},{trading_time},{trading_date}".encode()


@pytest.mark.parametrize("value, expected", [
    (tick(), DAY * 86_400_000 + 9 * 3_600_000 + 123),
    (tick(trading_date=""), DAY * 86_400_000 + 9 * 3_600_000 + 123),
    (tick(trading_time="09:00:00"), DAY * 86_400_000 + 9 * 3_600_000),
    (tick(trading_time="09:00:00.5"), DAY * 86_400_000 + 9 * 3_600_000 + 500),
    (tick(last=""), None),
    (tick(last="0"), None),
    (tick(last="x"), None),
    (tick(trading_time=""), None),
    (tick(trading_date="", date=""), None),
    (tick(trading_date="31-02-2021"), None),
    (tick(trading_time="24:00:00"), None),
    (b"S.NL,E,08-11-2021,09:00:00.000,10.5,09:00:00", None),
    (tick(last="1_0"), None),
    (tick(last="inf"), None),
    (tick(last="\x01 1d\t"), DAY * 86_400_000 + 9 * 3_600_000 + 123),
    (tick(last="1e1d"), DAY * 86_400_000 + 9 * 3_600_000 + 123),
    (tick(trading_time="09:00:0\u00b2"), None),
])
def test_price_events_follow_the_job_parser(value, expected):
    parsed = price_event(value, 0)
    assert (parsed and parsed["eventTime"]) == expected


def test_zone_offset_moves_event_time_back():
    assert price_event(tick(), 2 * 3_600_000)["eventTime"] == price_event(tick(), 0)["eventTime"] - 2 * 3_600_000


def events(rows):
    """(admitted at, partition, event time) rows, offsets in admission order."""
    return pl.DataFrame([{"kafkaTs": t, "partition": p, "offset": i, "eventTime": e, "kafkaTsType": "CreateTime"}
                         for i, (t, p, e) in enumerate(rows)])


def test_watermark_waits_for_the_slowest_active_partition():
    feed = events([(100, 0, 5 * MINUTE + 1000), (200, 1, 4 * MINUTE), (300, 1, 5 * MINUTE + 1000)])
    model = Watermark(feed, [0, 1], bound_ms=1000, idleness_ms=0, start_ms=0)
    assert model.passed(5 * MINUTE - 1) == 300
    assert model.at(250) == 4 * MINUTE - 1001


def test_idleness_releases_a_quiet_partition_and_an_empty_one():
    feed = events([(1_000, 0, 10), (2_000, 1, 10), (3_000, 1, 5 * MINUTE + 1000)])
    model = Watermark(feed, [0, 1, 2], bound_ms=1000, idleness_ms=30_000, start_ms=500)
    assert model.passed(5 * MINUTE - 1) == 31_000


def test_partitions_idling_at_the_same_instant_hold_the_watermark():
    for first, second in ((0, 1), (1, 0)):
        model = Watermark(events([(0, first, 10), (0, second, 100)]), [0, 1], bound_ms=0, idleness_ms=30, start_ms=0)
        assert model.at(1_000) == 9


def test_watermark_holds_while_every_partition_is_idle_and_never_decreases():
    feed = events([(0, 0, 5 * MINUTE), (10_000, 1, 6 * MINUTE), (50_000, 2, MINUTE)])
    model = Watermark(feed, [0, 1, 2], bound_ms=0, idleness_ms=30_000, start_ms=0)
    assert model.at(29_999) is None
    assert model.at(30_000) == 6 * MINUTE - 1
    assert model.at(100_000) == 6 * MINUTE - 1


def results(rows):
    return pl.DataFrame([{"windowEnd": w, "maxIngestTs": a, "emitTs": e, "kafkaTs": o} for w, a, e, o in rows])


def test_stages_add_up_and_the_interval_excludes_warmup_and_drain():
    rows = [(0, 1, 5 * MINUTE - 10), (1_000, 0, 5 * MINUTE + 1_000), (2_000, 1, 5 * MINUTE + 1_000), (9_000, 0, 10 * MINUTE + 1_000)]
    feed = events(rows)
    model = Watermark(feed, [0, 1], bound_ms=1000, idleness_ms=0, start_ms=0)
    out = decompose(results([(5 * MINUTE, 0, 2_100, 2_105), (5 * MINUTE, 500, 2_100, 2_105), (10 * MINUTE, 9_000, 9_500, 9_501)]),
                    Feed(feed), model, 1000, interval=(400, 10_000), drain_start_ms=9_400)
    assert out["measured"] == 1 and out["excluded"] == {"beforeInterval": 1, "afterInterval": 0, "drain": 1}
    stages = {k: v["mean"] for k, v in out["stagesMs"].items()}
    assert stages == {"windowWait": 500, "boundWait": 0, "alignmentWait": 1_000, "watermarkWait": 1_000, "queueing": 100,
                      "sinkHandoff": 5, "emitLatency": 1_600, "outputLatency": 1_605}


def test_a_result_the_model_never_releases_is_counted_not_decomposed():
    feed = events([(0, 0, 1), (1, 1, 1)])
    model = Watermark(feed, [0, 1], bound_ms=0, idleness_ms=0, start_ms=0)
    out = decompose(results([(5 * MINUTE, 0, 50, 51)]), Feed(feed), model, 0, (0, 100), None)
    assert out["unmodelled"] == 1 and out["stagesMs"]["emitLatency"]["n"] == 1 and out["stagesMs"]["queueing"]["n"] == 0


def test_nearest_rank_percentiles_are_observed_values():
    s = stats(list(range(1, 101)))
    assert (s["p50"], s["p95"], s["p99"], s["max"]) == (50, 95, 99, 100)


def test_watermark_lag_pairs_values_with_their_scrape_times():
    metrics = {"queries": {
        "q1InputWatermark": {"result": [{"metric": {"__name__": "wm", "subtask_index": "0"},
                                         "values": [[10, "120000"], [15, "120000"], [20, "-9.2e18"]]}]},
        "q1InputWatermarkScrapeTs": {"result": [{"metric": {"subtask_index": "0"},
                                                 "values": [[10, "9.5"], [15, "9.5"], [20, "19.5"]]}]},
    }}
    samples = prometheus_samples(metrics, "q1InputWatermark", "q1InputWatermarkScrapeTs")
    assert samples == [(9_500, 120_000), (19_500, -9_200_000_000_000_000_000)]
    three = {"queries": {name: {"result": [{"metric": {"subtask_index": str(i)}, "values": [[10, v]]}
                                            for i, v in enumerate(values)]}
                         for name, values in (("wm", ["100", "100", "200"]), ("ts", ["9.5"] * 3))}}
    assert prometheus_samples(three, "wm", "ts") == [(9_500, 100), (9_500, 100), (9_500, 200)]
    feed = events([(1_000, 0, 100_000), (2_000, 0, 121_000), (9_000, 0, 125_000)])
    model = Watermark(feed, [0], bound_ms=1000, idleness_ms=0, start_ms=0)
    lag = watermark_lag(samples, Feed(feed), model, (0, 30_000))
    assert lag["samples"] == 1
    assert (lag["eventMs"]["p50"], lag["wallMs"]["p50"], lag["modelAheadEventMs"]["p50"]) == (5_000, 7_500, 3_999)


def manifest(tmp_path, complete=True):
    (tmp_path / "manifest.json").write_text(json.dumps({"topics": {
        "ema-results": {"file": "ema_results.parquet", "complete": True},
        "ticks": {"file": "price_events.parquet", "complete": complete}}}))


def test_latency_refuses_an_incomplete_ticks_export(tmp_path):
    manifest(tmp_path, complete=False)
    out = latency(tmp_path, tmp_path / "unused.json", None, 1000, 30_000, (0, 2_000), 0, None, tmp_path / "out")
    assert out == {"complete": False, "problems": ["price_events.parquet is missing or incomplete"]}


def test_latency_writes_its_report(tmp_path):
    manifest(tmp_path)
    feed = events([(0, 0, 1), (1_000, 0, 5 * MINUTE + 1_000)])
    feed.write_parquet(tmp_path / "price_events.parquet")
    results([(5 * MINUTE, 0, 1_200, 1_201)]).with_columns(kafkaTsType=pl.lit("CreateTime")).write_parquet(tmp_path / "ema_results.parquet")
    report = tmp_path / "replay.json"
    report.write_text(json.dumps({"offsets": [{"partition": 0}]}))
    out = latency(tmp_path, report, None, 1000, 30_000, (0, 2_000), 0, None, tmp_path / "out")
    assert out["timestampTypes"] == {"admission": ["CreateTime"], "output": ["CreateTime"]}
    assert out["stagesMs"]["queueing"]["mean"] == 200
    assert json.loads((tmp_path / "out" / "latency.json").read_text())["measured"] == 1
