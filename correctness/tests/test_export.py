import json
from types import SimpleNamespace

import polars as pl
import pytest
from kafka import TopicPartition

from ema_check.export import LATE_SCHEMA, _read_topic, export, resolve_sources


def test_resolves_replay_indices_without_guessing_filenames(tmp_path):
    report = tmp_path / "replay.json"
    report.write_text(json.dumps({"status": "COMPLETE", "files": [
        {"index": 0, "path": "/data/daily/day.csv"},
        {"index": 1, "path": "/data/daily/next.csv"},
    ]}))
    events = pl.DataFrame({"sourceFileIndex": [1, 0, 9, None]})
    assert resolve_sources(events, report)["sourceFile"].to_list() == ["next.csv", "day.csv", None, None]
    assert resolve_sources(events, None)["sourceFile"].null_count() == 4


@pytest.mark.parametrize("report", [
    {"status": "FAILED"},
    {"status": "COMPLETE", "dryRun": True},
    {"status": "COMPLETE", "files": [{"index": 0, "path": "a/day.csv"}, {"index": 1, "path": "b/day.csv"}]},
])
def test_rejects_ambiguous_or_incomplete_replay_reports(tmp_path, report):
    path = tmp_path / "replay.json"
    path.write_text(json.dumps(report))
    with pytest.raises(ValueError):
        resolve_sources(pl.DataFrame({"sourceFileIndex": [0]}), path)


class Consumer:
    def __init__(self, records):
        self.records = records
        self.topic = None

    def partitions_for_topic(self, topic):
        return {0}

    def beginning_offsets(self, tps):
        return {tp: 0 for tp in tps}

    def end_offsets(self, tps):
        return {tp: len(self.records.get(tp.topic, [])) for tp in tps}

    def assign(self, tps):
        self.topic = tps[0].topic

    def seek_to_beginning(self, *tps):
        pass

    def poll(self, **kwargs):
        return {TopicPartition(self.topic, 0): [SimpleNamespace(offset=i, value=json.dumps(row))
                for i, row in enumerate(self.records.get(self.topic, []))]}

    def position(self, tp):
        return len(self.records.get(tp.topic, []))

    def close(self):
        pass


def test_missing_source_header_is_exported_but_incomplete():
    record = {"sourceFileIndex": None, "sourceLine": None, "symbol": "S.NL", "windowEnd": 300000}
    df, info = _read_topic(Consumer({"late-ticks": [record]}), "late-ticks", LATE_SCHEMA, 1)
    assert df.height == info["records"] == 1
    assert not info["complete"]


@pytest.mark.parametrize("mapped", [True, False])
def test_export_records_provenance_completeness(tmp_path, monkeypatch, mapped):
    from ema_check import export as module
    record = {"sourceFileIndex": 0, "sourceLine": 7, "symbol": "S.NL", "windowEnd": 300000}
    monkeypatch.setattr(module, "KafkaConsumer", lambda **kw: Consumer({"late-ticks": [record]}))
    report = tmp_path / "replay.json"
    report.write_text(json.dumps({"status": "COMPLETE", "files": [{"index": 0, "path": "/data/day.csv"}]}))
    manifest = export("unused", "ema-results", "advisories", tmp_path, "Z", replay_report=report if mapped else None)
    assert manifest["topics"]["late-ticks"]["complete"] == mapped
    assert pl.read_parquet(tmp_path / "late_events.parquet")["sourceFile"].to_list() == ["day.csv" if mapped else None]


def test_empty_late_export_proves_zero_without_report(tmp_path, monkeypatch):
    from ema_check import export as module
    monkeypatch.setattr(module, "KafkaConsumer", lambda **kw: Consumer({}))
    manifest = export("unused", "ema-results", "advisories", tmp_path, "Z")
    assert manifest["topics"]["late-ticks"]["complete"]
    assert manifest["topics"]["late-ticks"]["records"] == 0
