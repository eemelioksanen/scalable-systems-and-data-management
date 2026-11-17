import json
from pathlib import Path
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
        return {TopicPartition(self.topic, 0): [
            SimpleNamespace(offset=i, value=row if isinstance(row, bytes) else json.dumps(row), timestamp=1_000 + i, timestamp_type=0)
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


def test_export_keeps_output_timestamps_and_the_price_events_of_ticks(tmp_path, monkeypatch):
    from ema_check import export as module
    result = {"symbol": "S.NL", "secType": "E", "windowEnd": 300000, "close": 1.0, "ema38": 1.0, "ema100": 1.0,
              "maxIngestTs": 900, "emitTs": 950}
    ticks = [b"S.NL,E,08-11-2021,09:00:00.000,,,", b"S.NL,E,08-11-2021,09:00:00.000,10.5,09:00:01.000,"]
    monkeypatch.setattr(module, "KafkaConsumer", lambda **kw: Consumer({"ema-results": [result], "ticks": ticks}))
    manifest = export("unused", "ema-results", "advisories", tmp_path, "+02:00", ticks_topic="ticks")
    exported = pl.read_parquet(tmp_path / "ema_results.parquet")
    assert exported.select("kafkaTs", "kafkaTsType").row(0) == (1_000, "CreateTime")
    assert manifest["topics"]["ticks"] | {"partitions": None} == {
        "file": "price_events.parquet", "encoding": "csv", "partitions": None, "records": 2, "rows": 1, "complete": True}
    events = pl.read_parquet(tmp_path / "price_events.parquet")
    assert events.select("offset", "kafkaTs", "eventTime").row(0) == (1, 1_001, 1636354801000)


def test_export_decodes_binary_ticks(tmp_path, monkeypatch):
    from ema_check import export as module
    rows = [line.split("\t") for line in (Path(__file__).resolve().parents[2] / "fixtures/semantics/ticks-binary.tsv")
            .read_text().splitlines()[:2]]
    events = {}
    for encoding in ("csv", "binary"):
        values = [r[2].encode() if encoding == "csv" else bytes.fromhex(r[3]) for r in rows]
        monkeypatch.setattr(module, "KafkaConsumer", lambda **kw: Consumer({"ticks": values}))
        out = tmp_path / encoding
        manifest = export("unused", "ema-results", "advisories", out, "Z", ticks_topic="ticks", ticks_encoding=encoding)
        assert manifest["topics"]["ticks"]["encoding"] == encoding
        events[encoding] = pl.read_parquet(out / "price_events.parquet")
    assert events["binary"].height == 2
    assert events["binary"].equals(events["csv"])
