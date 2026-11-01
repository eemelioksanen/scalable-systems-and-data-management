import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest

spec = importlib.util.spec_from_file_location("benchmark_run", Path(__file__).resolve().parents[2] / "bench/run.py")
benchmark = importlib.util.module_from_spec(spec)
spec.loader.exec_module(benchmark)


def vertex(read, written):
    return {"aggregated": {"metrics": {"read-records": {"sum": read}, "write-records": {"sum": written}}}}


@pytest.mark.parametrize("complete", [True, False])
def test_bounded_late_count_comes_from_export_not_stale_scrape(complete):
    run = object.__new__(benchmark.Run)
    run.cfg = SimpleNamespace(all_subscribed=True, oracle=None)
    counters = {name: None for name in benchmark.SPLIT}
    counters.update(lateTicks=0)
    outputs = {"export": {"topics": {
        "ema-results": {"records": 2}, "advisories": {"records": 1},
        "late-ticks": {"records": 3, "complete": complete},
    }}}
    flink = {"vertices": {"Source: ticks": vertex(10, 10), "q1-ema": vertex(10, 5),
                          "subscription-filter -> sinks": vertex(2, 3)}}
    result = run.reconcile({"counts": {"acknowledged": 10}}, flink, counters, outputs)
    assert result["countersFinal"]["lateTicks"] == complete
    assert counters["lateTicks"] == (3 if complete else None)
    assert result["ok"] == complete
