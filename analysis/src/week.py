import argparse
import datetime as dt
import gc
import json
import os
import sys
import time
from pathlib import Path

os.environ.setdefault("POLARS_MAX_THREADS", "2")

import duckdb
import polars as pl

from daily import DATA_CATEGORIES, export, peak_rss_bytes, quote, rates_summary, save_json, skew_tables, watermark_tables
from daily_parse import DAY_MS, WINDOW_MS, offset_ms
from fetch_day import checksum


DAYS = tuple(str(dt.date(2021, 11, day)) for day in range(8, 15))
POSITION_STRIDE = 2**32


def create_prices(con, checkpoints):
    sources = []
    for index, (day, path) in enumerate(checkpoints):
        sources.append(f"""SELECT {quote(day)} AS source_day,
            {index}::BIGINT*{POSITION_STRIDE}+source_line AS source_line,
            source_line AS physical_line, symbol, sec_type, exchange, event_ms, system_ms, price, trading_at_0100,
            (event_ms//{WINDOW_MS}+1)*{WINDOW_MS} AS window_end,
            sec_type='I' AND trading_at_0100 AND matches_open_high_low AS summary_candidate
            FROM read_parquet({quote(path)}) WHERE category='price' AND event_ms IS NOT NULL""")
    con.execute("CREATE OR REPLACE TABLE prices AS " + " UNION ALL ".join(sources))


def save_closes(con, path, predicate="true"):
    con.execute(f"""COPY (SELECT symbol,
        arg_max(sec_type,struct_pack(t:=event_ms,p:=source_line)) AS secType,
        window_end AS windowEnd,
        arg_max(price,struct_pack(t:=event_ms,p:=source_line)) AS close,
        count(*) AS priceEvents,max(event_ms) AS closeEventTime,
        'debs2022-gc-trading-day-' || strftime(CAST(arg_max(source_day,
            struct_pack(t:=event_ms,p:=source_line)) AS DATE),'%d-%m-%y') || '.csv' AS closeSourceFile,
        arg_max(physical_line,struct_pack(t:=event_ms,p:=source_line))::BIGINT AS closeSourceLine
        FROM prices WHERE {predicate} GROUP BY symbol,window_end ORDER BY symbol,window_end)
        TO {quote(path)} (FORMAT PARQUET)""")


def save_global_late(con, path, bound):
    con.execute(f"""COPY (SELECT source_line FROM (
        SELECT source_line,window_end,max(event_ms) OVER (ORDER BY source_line
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS previous_max FROM prices)
        WHERE previous_max-window_end+1>{bound}) TO {quote(path)} (FORMAT PARQUET)""")


def compare_results(reference, actual, reference_advisories, actual_advisories):
    joined = reference.join(actual, on=["symbol", "windowEnd"], how="full", coalesce=True, suffix="_actual")
    missing = pl.col("close_actual").is_null()
    changed_close = (pl.col("close") != pl.col("close_actual")).fill_null(False)
    error = pl.max_horizontal((pl.col("ema38") - pl.col("ema38_actual")).abs(),
                              (pl.col("ema100") - pl.col("ema100_actual")).abs())
    keys = ["symbol", "windowEnd", "type"]
    return {
        "missing_windows": int(joined.select(missing.sum()).item()),
        "added_windows": int(joined.select(pl.col("close").is_null().sum()).item()),
        "changed_existing_closes": int(joined.select(changed_close.sum()).item()),
        "changed_existing_emas_over_1e_minus_9": int(joined.select((error > 1e-9).sum()).item()),
        "max_abs_ema_difference": joined.select(error.max()).item(),
        "advisories_removed": reference_advisories.join(actual_advisories, on=keys, how="anti").height,
        "advisories_added": actual_advisories.join(reference_advisories, on=keys, how="anti").height,
    }


def summarize_days(daily_out, out):
    summaries = []
    for day in DAYS:
        folder = daily_out / day
        manifest = json.loads((folder / "manifest.json").read_text())
        counts = manifest["counts"]
        summaries.append({"day": day, "data_rows": sum(value for name, value in counts.items()
                          if name not in {"comment", "header", "blank", "accepted_price_events", "windows"}),
                          "accepted_price_events": counts["accepted_price_events"], "windows": counts["windows"],
                          "wall_seconds": manifest["wall_seconds"], "peak_rss_bytes": manifest["peak_rss_bytes"]})
    pl.DataFrame(summaries).write_csv(out / "days.csv")
    for name in ("taxonomy", "quality", "rates_summary", "trading_rates_summary", "activity_spikes",
                 "coverage", "timestamp_gaps", "row_reconciliation"):
        frames = [pl.read_csv(daily_out / day / f"{name}.csv").with_columns(day=pl.lit(day)) for day in DAYS]
        pl.concat(frames, how="diagonal_relaxed").write_csv(out / f"daily_{name}.csv")


def analyze(con, checkpoints, daily_out, out, cache):
    create_prices(con, checkpoints)
    symbol_paths = [str(daily_out / day / "symbols.csv") for day, _ in checkpoints]
    quality = ("missing_trading_dates", "invalid_event_timestamps", "date_mismatches",
               "system_lags_over_2s", "trading_lags_over_2s")
    con.execute(f"""CREATE OR REPLACE TABLE symbols AS SELECT symbol,
        sum(raw_events) AS raw_events, sum(accepted_price_events) AS accepted_price_events,
        {','.join(f'sum({name}) AS {name}' for name in quality)}
        FROM read_csv({quote_list(symbol_paths)},union_by_name=true) GROUP BY symbol""")
    export(con, out, "symbols", "SELECT * FROM symbols ORDER BY raw_events DESC,symbol")
    skew_tables(con, out)
    rows = [str(path) for _, path in checkpoints]
    con.execute(f"""CREATE OR REPLACE TABLE seconds AS SELECT exchange,system_ms//1000 AS second,
        count(*) AS events,count(*) FILTER (WHERE category='price') AS price_events
        FROM read_parquet({quote_list(rows)}) WHERE {DATA_CATEGORIES} AND system_ms IS NOT NULL
        GROUP BY exchange,second""")
    export(con, out, "rates_summary", rates_summary("seconds"))
    export(con, out, "replay_targets", f"""WITH targets AS (
        SELECT 'week_observed_p99' AS label,p99_events_per_second AS rate
            FROM read_csv({quote(out / 'rates_summary.csv')}) WHERE exchange='ALL'
        UNION ALL SELECT 'week_observed_peak',peak_events_per_second
            FROM read_csv({quote(out / 'rates_summary.csv')}) WHERE exchange='ALL'
        UNION ALL SELECT 'busiest_daily_p99',max(p99_events_per_second)
            FROM read_csv({quote(out / 'daily_rates_summary.csv')}) WHERE exchange='ALL')
        SELECT label,multiplier,rate*multiplier AS offered_events_per_second
        FROM targets CROSS JOIN (VALUES (1),(10),(100)) m(multiplier) ORDER BY label,multiplier""")
    con.execute("""CREATE OR REPLACE TABLE windows AS SELECT symbol,window_end,
        arg_max(price,struct_pack(t:=event_ms,p:=source_line)) AS close,
        count(*) AS price_events, count(DISTINCT source_day) AS source_days
        FROM prices GROUP BY symbol,window_end""")
    start = (dt.date.fromisoformat(DAYS[0]) - dt.date(1970, 1, 1)).days * DAY_MS - offset_ms("+02:00")
    inside_week = f"w.window_end>{start} AND w.window_end<={start + 7 * DAY_MS}"
    export(con, out, "window_statistics", f"""SELECT s.symbol,2016 AS calendar_windows,
        count(w.window_end) AS nonempty_windows,
        count(*) FILTER (WHERE {inside_week}) AS nonempty_calendar_windows,
        2016-count(*) FILTER (WHERE {inside_week}) AS empty_calendar_windows,
        count(*) FILTER (WHERE w.source_days>1) AS windows_spanning_files
        FROM symbols s LEFT JOIN windows w USING(symbol) GROUP BY s.symbol ORDER BY s.symbol""")
    export(con, out, "summary_candidates", """SELECT source_day,physical_line,symbol,sec_type,
        system_ms,event_ms,price,system_ms-event_ms AS gap_ms FROM prices WHERE summary_candidate
        ORDER BY source_day,physical_line""")
    export(con, out, "summary_candidate_groups", """SELECT source_day,symbol,count(*) AS events,
        count(DISTINCT price) AS distinct_prices,min(physical_line) AS first_line,max(physical_line) AS last_line
        FROM prices WHERE summary_candidate GROUP BY source_day,symbol ORDER BY source_day,symbol""")
    export(con, out, "index_0100_events", """SELECT source_day,physical_line,symbol,
        system_ms,event_ms,price,summary_candidate FROM prices
        WHERE sec_type='I' AND trading_at_0100 ORDER BY source_day,physical_line""")
    export(con, out, "index_0100_groups", """SELECT source_day,symbol,count(*) AS events,
        count(*) FILTER (WHERE summary_candidate) AS matching_summary_events
        FROM prices WHERE sec_type='I' AND trading_at_0100
        GROUP BY source_day,symbol ORDER BY source_day,symbol""")
    export(con, out, "trading_lag_bands", """SELECT source_day,sec_type,
        count(*) FILTER (WHERE system_ms-event_ms>2000) AS over_2s,
        count(*) FILTER (WHERE system_ms-event_ms>3600000) AS over_1h,
        count(*) FILTER (WHERE system_ms-event_ms>43200000) AS over_12h,
        count(*) FILTER (WHERE summary_candidate) AS summary_candidates
        FROM prices GROUP BY source_day,sec_type ORDER BY source_day,sec_type""")
    watermark_tables(con, out, cache)
    save_closes(con, cache / "admit_all.parquet")
    scenarios = [{"scenario": "exclude_summary_candidates", "partitions": None, "bound_ms": None,
                  "predicate": "NOT summary_candidate"}]
    no_loss_bound = con.execute(f"SELECT coalesce(max(min_bound_ms),0) FROM read_parquet({quote(cache / 'late_candidates_8.parquet')})").fetchone()[0]
    for bound in sorted({0, 1, 25, 264, 60000, no_loss_bound}):
        scenarios.append({"scenario": f"partitions8_bound{bound}", "partitions": 8, "bound_ms": bound,
                          "predicate": f"source_line NOT IN (SELECT source_line FROM read_parquet({quote(cache / 'late_candidates_8.parquet')}) WHERE min_bound_ms>{bound})"})
    global_late = cache / "global_late_60000.parquet"
    save_global_late(con, global_late, 60000)
    scenarios.append({"scenario": "partitions1_bound60000", "partitions": 1, "bound_ms": 60000,
                      "predicate": f"source_line NOT IN (SELECT source_line FROM read_parquet({quote(global_late)}))"})
    for scenario in scenarios:
        scenario["discarded_events"] = con.execute("SELECT count(*) FROM prices WHERE NOT (" + scenario["predicate"] + ")").fetchone()[0]
        save_closes(con, cache / f"{scenario['scenario']}.parquet", scenario.pop("predicate"))
    export(con, out, "late_events_8", f"""SELECT p.source_day,p.physical_line,p.symbol,p.sec_type,p.event_ms,
        p.system_ms,p.price,p.summary_candidate,l.partition,l.min_bound_ms
        FROM read_parquet({quote(cache / 'late_candidates_8.parquet')}) l JOIN prices p USING(source_line)
        ORDER BY p.source_line""")
    return scenarios, {
        "accepted_price_events": con.execute("SELECT count(*) FROM prices").fetchone()[0],
        "windows": con.execute("SELECT count(*) FROM windows").fetchone()[0],
        "windows_spanning_files": con.execute("SELECT count(*) FROM windows WHERE source_days>1").fetchone()[0],
        "summary_candidates": con.execute("SELECT count(*) FROM prices WHERE summary_candidate").fetchone()[0],
    }


def quote_list(paths):
    return "[" + ",".join(quote(path) for path in paths) + "]"


def oracle_crosscheck(results, advisories, reference_path):
    from ema_oracle.run import ADVISORY_COLUMNS, EMA_COLUMNS, _content_sha256
    reference = json.loads(reference_path.read_text())
    normalized = results.with_columns(
        (pl.col("windowEnd") + offset_ms("+02:00")).alias("windowEnd"),
        (pl.col("closeEventTime") + offset_ms("+02:00")).alias("closeEventTime"),
        exchange=pl.col("symbol").str.extract(r"\.([^.]+)$", 1),
    ).select(EMA_COLUMNS)
    normalized_advisories = advisories.with_columns(
        (pl.col("windowEnd") + offset_ms("+02:00")).alias("windowEnd"),
        exchange=pl.col("symbol").str.extract(r"\.([^.]+)$", 1),
    ).select(ADVISORY_COLUMNS)
    checks = []
    for name, frame in (("ema_results.parquet", normalized), ("advisories.parquet", normalized_advisories)):
        actual = _content_sha256(frame)
        expected = reference["outputs"][name]["contentSha256"]
        checks.append({"artifact": name, "expected_content_sha256": expected,
                       "actual_content_sha256": actual, "matches": actual == expected})
    return {"reference": str(reference_path), "reference_sha256": checksum(reference_path, "sha256"),
            "normalization": "Shift UTC+2 timestamps by +7200000 ms to compare with the recorded Z run",
            "checks": checks}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cache", type=Path, default=Path("data/analysis"))
    parser.add_argument("--daily-out", type=Path, default=Path("analysis/out/daily"))
    parser.add_argument("--out", type=Path, default=Path("analysis/out/week"))
    parser.add_argument("--oracle", type=Path, default=Path("oracle"))
    args = parser.parse_args()
    started = time.monotonic()
    args.out.mkdir(parents=True, exist_ok=True)
    cache = args.cache / "week"
    cache.mkdir(parents=True, exist_ok=True)
    (args.out / "manifest.json").unlink(missing_ok=True)
    inputs, checkpoints = [], []
    common_calculation = None
    for day in DAYS:
        manifest_path = args.daily_out / day / "manifest.json"
        manifest = json.loads(manifest_path.read_text())
        parsed_path = args.cache / day / "parsed.json"
        parsed = json.loads(parsed_path.read_text())
        checkpoint = args.cache / day / "rows.parquet"
        if manifest["day"] != day or manifest["zone_offset"] != "+02:00" or parsed["identity"] != manifest["identity"]:
            raise ValueError(f"Inconsistent daily checkpoint: {day}")
        sources = {name: manifest["source_sha256"][name]
                   for name in ("daily.py", "daily_parse.py", "daily_plot.py", "fetch_day.py")}
        calculation = {"sources": sources, "dependencies": manifest["dependencies"], "python": manifest["python"]}
        if common_calculation is not None and calculation != common_calculation:
            raise ValueError(f"Daily calculation source hashes or dependency versions differ: {day}")
        common_calculation = calculation
        if checksum(checkpoint, "sha256") != parsed["sha256"]:
            raise ValueError(f"Checkpoint checksum mismatch: {day}")
        for name, expected in manifest["artifacts"].items():
            if checksum(manifest_path.parent / name, "sha256") != expected["sha256"]:
                raise ValueError(f"Daily artifact checksum mismatch: {day}/{name}")
        inputs.append({"day": day, "daily_manifest_sha256": checksum(manifest_path, "sha256"),
                       "raw_sha256": manifest["identity"]["input_sha256"], "checkpoint_sha256": parsed["sha256"],
                       "revision": manifest["revision"], "source_sha256": sources})
        checkpoints.append((day, checkpoint))
    summarize_days(args.daily_out, args.out)
    con = duckdb.connect(str(cache / "working.duckdb"))
    con.execute("SET memory_limit='2GB'")
    con.execute("SET threads=4")
    con.execute("SET temp_directory=?", [str(cache / "spill")])
    con.execute("SET max_temp_directory_size='12GB'")
    scenarios, counts = analyze(con, checkpoints, args.daily_out, args.out, cache)
    con.close()
    gc.collect()
    sys.path.insert(0, str(args.oracle.resolve()))
    from ema_oracle.ema import compute
    reference, reference_advisories = compute(pl.read_parquet(cache / "admit_all.parquet"))
    reference.write_parquet(cache / "admit_all_emas.parquet")
    reference_advisories.write_parquet(cache / "admit_all_advisories.parquet")
    counts["advisories"] = reference_advisories.height
    crosscheck = oracle_crosscheck(reference, reference_advisories, args.oracle / "evidence/2026-09-28-week.json")
    save_json(args.out / "oracle_crosscheck.json", crosscheck)
    if not all(check["matches"] for check in crosscheck["checks"]):
        raise ValueError("Full-week results differ from the recorded oracle; inspect oracle_crosscheck.json")
    for scenario in scenarios:
        print("EMA comparison: " + scenario["scenario"], flush=True)
        actual, advisories = compute(pl.read_parquet(cache / f"{scenario['scenario']}.parquet"))
        scenario.update(compare_results(reference, actual, reference_advisories, advisories))
        scenario["advisories"] = advisories.height
        if scenario["scenario"] == "partitions1_bound60000":
            bounded_check = oracle_crosscheck(actual, advisories, args.oracle / "evidence/2026-09-28-week-bound60s.json")
            save_json(args.out / "oracle_bounded_crosscheck.json", bounded_check)
            if not all(check["matches"] for check in bounded_check["checks"]):
                raise ValueError("Single-partition 60 s model differs from recorded source-order oracle")
        del actual, advisories
        gc.collect()
    pl.DataFrame(scenarios).write_csv(args.out / "scenario_effects.csv")
    from week_plot import plot
    plot(args.out)
    manifest = {
        "days": DAYS, "zone_offset": "+02:00", "inputs": inputs, "counts": counts,
        "wall_seconds": round(time.monotonic() - started, 3), "peak_rss_bytes": peak_rss_bytes(),
        "duckdb": duckdb.__version__, "polars": pl.__version__,
        "revision": os.environ.get("ANALYSIS_REVISION"),
        "worktree_status": os.environ.get("ANALYSIS_WORKTREE_STATUS"),
        "source_sha256": {p.name: checksum(p, "sha256") for p in sorted(Path(__file__).parent.glob("*.py"))},
        "oracle_source_sha256": {p.name: checksum(p, "sha256") for p in sorted((args.oracle / "ema_oracle").glob("*.py"))},
        "artifacts": {p.name: {"bytes": p.stat().st_size, "sha256": checksum(p, "sha256")}
                      for p in sorted(args.out.iterdir()) if p.is_file()},
        "limits": ["Source-order model with active partitions; no periodic-emission or idleness simulation",
                   "Summary exclusion is an ablation, not an accepted parser policy",
                   "EMA calculation reuses the oracle and is not an independent oracle validation"],
    }
    save_json(args.out / "manifest.json", manifest)
    print(json.dumps({"counts": counts, "wall_seconds": manifest["wall_seconds"],
                      "peak_rss_bytes": manifest["peak_rss_bytes"]}), flush=True)


if __name__ == "__main__":
    main()
