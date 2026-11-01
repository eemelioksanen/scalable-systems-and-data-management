"""Export the job's output topics to Parquet in the oracle's format (D-16, D-18).

Each topic is read from its start offsets up to the end offsets taken when the export starts.
The manifest records both per partition, and an export is complete only if every partition was
read from offset 0 to its end with one record per offset. A topic still being written to, or one whose
oldest records were deleted, is therefore reported rather than compared.
"""

from __future__ import annotations

import datetime as dt
import json
import time
from pathlib import Path

import polars as pl
from kafka import KafkaConsumer, TopicPartition

EMA_SCHEMA = {
    "symbol": pl.String, "secType": pl.String, "windowEnd": pl.Int64, "close": pl.Float64,
    "ema38": pl.Float64, "ema100": pl.Float64, "maxIngestTs": pl.Int64, "emitTs": pl.Int64,
}
ADVISORY_SCHEMA = {
    "symbol": pl.String, "secType": pl.String, "windowEnd": pl.Int64, "type": pl.String,
    "ema38": pl.Float64, "ema100": pl.Float64, "maxIngestTs": pl.Int64, "emitTs": pl.Int64,
}
LATE_SCHEMA = {"sourceFileIndex": pl.Int32, "sourceLine": pl.Int64, "symbol": pl.String, "windowEnd": pl.Int64}
POSITION_SCHEMA = {"partition": pl.Int32, "offset": pl.Int64}


def _read_topic(consumer: KafkaConsumer, topic: str, schema: dict, stall_s: float) -> tuple[pl.DataFrame, dict]:
    partitions = sorted(consumer.partitions_for_topic(topic) or ())
    if not partitions:
        raise ValueError(f"topic {topic} does not exist")
    tps = [TopicPartition(topic, p) for p in partitions]
    start, end = consumer.beginning_offsets(tps), consumer.end_offsets(tps)
    consumer.assign(tps)
    consumer.seek_to_beginning(*tps)

    # Python's float() rounds correctly, so each value is the double the job serialized.
    columns: dict[str, list] = {name: [] for name in [*schema, *POSITION_SCHEMA]}
    read = {tp.partition: 0 for tp in tps}
    incomplete_records = 0
    pending = {tp for tp in tps if start[tp] < end[tp]}
    last_progress = time.monotonic()
    while pending and time.monotonic() - last_progress < stall_s:
        for tp, records in consumer.poll(timeout_ms=1000, max_records=50_000).items():
            for rec in records:
                if rec.offset >= end[tp]:
                    break
                value = json.loads(rec.value)
                if any(value.get(name) is None for name in schema):
                    incomplete_records += 1
                for name in schema:
                    columns[name].append(value.get(name))
                columns["partition"].append(tp.partition)
                columns["offset"].append(rec.offset)
                read[tp.partition] += 1
            last_progress = time.monotonic()
        pending = {tp for tp in pending if consumer.position(tp) < end[tp]}

    info_partitions = {
        str(tp.partition): {"startOffset": start[tp], "endOffset": end[tp], "records": read[tp.partition]}
        for tp in tps
    }
    problems = []
    if pending:
        problems.append(f"no progress for {stall_s:.0f} s before the end offsets on partitions {sorted(tp.partition for tp in pending)}")
    if any(p["records"] != p["endOffset"] - p["startOffset"] for p in info_partitions.values()):
        problems.append("fewer records than offsets: records are missing or the topic is transactional or compacted")
    if incomplete_records:
        problems.append(f"{incomplete_records} records lack a field or have a null one")
    if any(p["startOffset"] > 0 for p in info_partitions.values()):
        problems.append("a partition does not start at offset 0: records were deleted, e.g. by retention")
    info = {
        "partitions": info_partitions,
        "records": sum(read.values()),
        "complete": not problems,
        **({"problem": "; ".join(problems)} if problems else {}),
    }
    return pl.DataFrame(columns, schema={**schema, **POSITION_SCHEMA}), info


def resolve_sources(df: pl.DataFrame, replay_report: Path | None) -> pl.DataFrame:
    files = {}
    if replay_report is not None:
        report = json.loads(replay_report.read_text())
        if report.get("status") != "COMPLETE" or report.get("dryRun"):
            raise ValueError("late-event provenance requires a completed, non-dry replay report")
        for file in report["files"]:
            index, name = file["index"], Path(file["path"]).name
            if index in files or name in files.values():
                raise ValueError("replay files must have unique indices and basenames")
            files[index] = name
    return df.with_columns(sourceFile=pl.Series(
        [files.get(index) for index in df["sourceFileIndex"]], dtype=pl.String,
    ))


def export(bootstrap: str, ema_topic: str, advisories_topic: str, out: Path, zone_offset: str,
           stall_s: float = 30.0, late_topic: str = "late-ticks", replay_report: Path | None = None) -> dict:
    out.mkdir(parents=True, exist_ok=True)
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap,
        group_id=None,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        max_partition_fetch_bytes=16 << 20,
    )
    try:
        manifest: dict = {
            "tool": "ema_check.export",
            "createdUtc": dt.datetime.now(dt.UTC).isoformat(timespec="seconds"),
            "bootstrap": bootstrap,
            "zoneOffset": zone_offset,
            "topics": {},
        }
        for topic, schema, name in (
            (ema_topic, EMA_SCHEMA, "ema_results"),
            (advisories_topic, ADVISORY_SCHEMA, "advisories"),
            (late_topic, LATE_SCHEMA, "late_events"),
        ):
            df, info = _read_topic(consumer, topic, schema, stall_s)
            if name == "late_events":
                df = resolve_sources(df, replay_report)
                if df["sourceFile"].null_count():
                    info["complete"] = False
                    info["problem"] = "; ".join(filter(None, [info.get("problem"),
                        "late events lack source filenames: supply the matching --replay-report"]))
                if df.select("sourceFileIndex", "sourceLine").is_duplicated().any():
                    info["complete"] = False
                    info["problem"] = "; ".join(filter(None, [info.get("problem"),
                        "duplicate late-event source identities: reset output topics before replaying"]))
            df.sort(["symbol", "windowEnd", "partition", "offset"]).write_parquet(out / f"{name}.parquet")
            manifest["topics"][topic] = {"file": f"{name}.parquet", **info}
    finally:
        consumer.close()
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest
