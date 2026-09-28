import argparse
import datetime as dt
import importlib.metadata
import json
import os
import platform
import resource
import shutil
import subprocess
import sys
import time
from pathlib import Path

os.environ.setdefault("POLARS_MAX_THREADS", "2")
os.environ.setdefault("MPLCONFIGDIR", "/tmp/trading-analysis-matplotlib")

import duckdb
import polars as pl

from daily_parse import BATCH_LINES, DAY_MS, PARTITION_COUNTS, ROW_GROUP_SIZE, WINDOW_MS, kafka_hash, offset_ms, parse_to_parquet
from fetch_day import checksum


DATA_CATEGORIES = "category NOT IN ('comment', 'header', 'blank')"


def quote(value):
    return "'" + str(value).replace("'", "''") + "'"


def save_json(path, value):
    partial = path.with_suffix(path.suffix + ".partial")
    partial.write_text(json.dumps(value, indent=2, default=str) + "\n")
    partial.replace(path)


def peak_rss_bytes():
    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return value if sys.platform == "darwin" else value * 1024


def export(con, out, name, sql):
    path = out / f"{name}.csv"
    con.execute(f"COPY ({sql}) TO {quote(path)} (HEADER, DELIMITER ',')")
    print(f"saved {path.name}", flush=True)
    return path


def rates_summary(source):
    return f"""WITH groups AS (
        SELECT * FROM {source} UNION ALL SELECT 'ALL', second, sum(events), sum(price_events) FROM {source} GROUP BY second
        ), spans AS (SELECT exchange, min(second) AS lo, max(second) AS hi FROM groups GROUP BY exchange),
        dense AS (SELECT s.exchange, r.second, coalesce(g.events,0) AS events, coalesce(g.price_events,0) AS price_events
            FROM spans s CROSS JOIN LATERAL range(s.lo,s.hi+1) r(second)
            LEFT JOIN groups g ON g.exchange=s.exchange AND g.second=r.second)
        SELECT exchange, min(second) AS first_second, max(second) AS last_second, count(*) AS observation_seconds,
        count(*) FILTER (WHERE events=0) AS empty_seconds, sum(events) AS events,
        avg(events) AS mean_events_per_second, max(events) AS peak_events_per_second,
        quantile_disc(events,0.99) AS p99_events_per_second,
        max(price_events) AS peak_price_events_per_second, quantile_disc(price_events,0.99) AS p99_price_events_per_second
        FROM dense GROUP BY exchange ORDER BY exchange"""


def prepare(args, cache):
    identity = {
        "input_sha256": checksum(args.input, "sha256"),
        "input_bytes": args.input.stat().st_size,
        "parser_sha256": checksum(Path(__file__).with_name("daily_parse.py"), "sha256"),
        "polars": pl.__version__, "zone_offset": args.zone_offset,
    }
    path, manifest = cache / "rows.parquet", cache / "parsed.json"
    if path.exists() and manifest.exists():
        saved = json.loads(manifest.read_text())
        if saved["identity"] == identity and "parse_metrics" in saved and saved["sha256"] == checksum(path, "sha256"):
            print("reusing verified parsed checkpoint", flush=True)
            return path, identity, saved["parse_metrics"], True
    partial = path.with_suffix(".parquet.partial")
    print("parsing daily file into a streaming Parquet checkpoint", flush=True)
    started = time.monotonic()
    parse_to_parquet(args.input, partial, offset_ms(args.zone_offset))
    partial.replace(path)
    metrics = {"wall_seconds": round(time.monotonic() - started, 3), "peak_rss_bytes": peak_rss_bytes(),
               "polars_threads": pl.thread_pool_size(), "batch_lines": BATCH_LINES, "row_group_size": ROW_GROUP_SIZE}
    save_json(manifest, {"identity": identity, "sha256": checksum(path, "sha256"), "parse_metrics": metrics})
    return path, identity, metrics, False


def basic_tables(con, out, cache, day_start):
    export(con, out, "row_reconciliation", "SELECT category, count(*) AS rows FROM rows GROUP BY category ORDER BY category")
    export(con, out, "taxonomy", f"SELECT exchange, sec_type, category, count(*) AS rows FROM rows WHERE {DATA_CATEGORIES} GROUP BY ALL ORDER BY ALL")
    export(con, out, "field_counts", f"SELECT field_count, count(*) AS rows FROM rows WHERE {DATA_CATEGORIES} GROUP BY ALL ORDER BY ALL")
    quality = ["missing_trading_date", "missing_trading_time", "missing_all_dates", "date_mismatch", "malformed_ask", "malformed_bid", "unexpected_symbol"]
    export(con, out, "quality", f"""SELECT exchange, sec_type, count(*) AS price_rows,
        {', '.join(f'count(*) FILTER (WHERE {name}) AS {name}' for name in quality)},
        count(*) FILTER (WHERE event_ms IS NULL) AS invalid_event_timestamp,
        count(*) FILTER (WHERE system_ms IS NULL) AS invalid_system_timestamp,
        count(*) FILTER (WHERE event_ms IS NOT NULL) AS accepted_price_events,
        count(*) FILTER (WHERE trading_at_0100) AS trading_at_0100,
        count(*) FILTER (WHERE trading_at_0100 AND matches_open_high_low) AS at_0100_matching_summary
        FROM rows WHERE category = 'price' GROUP BY ALL ORDER BY ALL""")
    export(con, out, "timestamp_gaps", """SELECT exchange, sec_type, count(*) AS events,
        min(system_ms-event_ms) AS min_ms, quantile_disc(system_ms-event_ms, 0.5) AS p50_ms,
        quantile_disc(system_ms-event_ms, 0.99) AS p99_ms, max(system_ms-event_ms) AS max_ms,
        count(*) FILTER (WHERE system_ms < event_ms) AS negative,
        count(*) FILTER (WHERE system_ms-event_ms > 3600000) AS over_one_hour,
        count(*) FILTER (WHERE system_ms % 1000 = 0) AS whole_second_system,
        count(*) FILTER (WHERE event_ms % 1000 = 0) AS whole_second_trading,
        count(*) FILTER (WHERE system_ms // 1000 = event_ms // 1000) AS same_second
        FROM rows WHERE category='price' AND event_ms IS NOT NULL AND system_ms IS NOT NULL GROUP BY ALL ORDER BY ALL""")
    export(con, out, "timestamp_precision", """SELECT trading_fraction_digits, system_fraction_digits, price_fraction_digits,
        count(*) AS events FROM rows WHERE category='price' GROUP BY ALL ORDER BY ALL""")
    export(con, out, "timestamp_gap_histogram", """SELECT floor((system_ms-event_ms)/10)*10 AS gap_bucket_ms,
        count(*) AS events FROM prices WHERE system_ms IS NOT NULL GROUP BY ALL ORDER BY ALL""")
    export(con, out, "timestamp_residues", """SELECT clock, residue_ms, count(*) AS events FROM (
        SELECT 'system' AS clock, system_ms % 1000 AS residue_ms FROM prices WHERE system_ms IS NOT NULL
        UNION ALL SELECT 'trading', event_ms % 1000 FROM prices) GROUP BY ALL ORDER BY ALL""")
    export(con, out, "suspicious_timestamp_examples", """SELECT source_line, symbol, sec_type, system_ms, event_ms,
        system_ms-event_ms AS gap_ms, price, matches_open_high_low, date_mismatch
        FROM rows WHERE category='price' AND (trading_at_0100 OR date_mismatch OR abs(system_ms-event_ms)>3600000)
        ORDER BY source_line LIMIT 100""")
    con.execute(f"""CREATE OR REPLACE TABLE symbols AS SELECT symbol, min(exchange) AS exchange,
        string_agg(DISTINCT sec_type,'|' ORDER BY sec_type) AS sec_type,
        count(*) AS raw_events, count(*) FILTER (WHERE category='price') AS price_events,
        count(*) FILTER (WHERE category='price' AND event_ms IS NOT NULL) AS accepted_price_events,
        count(*) FILTER (WHERE category='price' AND missing_trading_date) AS missing_trading_dates,
        count(*) FILTER (WHERE category='price' AND event_ms IS NULL) AS invalid_event_timestamps,
        count(*) FILTER (WHERE category='price' AND date_mismatch) AS date_mismatches,
        count(*) FILTER (WHERE category='price' AND event_ms-system_ms>2000) AS system_lags_over_2s,
        count(*) FILTER (WHERE category='price' AND system_ms-event_ms>2000) AS trading_lags_over_2s
        FROM rows WHERE {DATA_CATEGORIES} AND category!='malformed_row' GROUP BY symbol""")
    export(con, out, "symbols", "SELECT * FROM symbols ORDER BY raw_events DESC, symbol, sec_type")
    export(con, out, "coverage", f"""SELECT exchange, min(system_ms) AS first_system_ms, max(system_ms) AS last_system_ms,
        min(event_ms) FILTER (WHERE category='price') AS first_trading_ms,
        max(event_ms) FILTER (WHERE category='price') AS last_trading_ms,
        count(*) FILTER (WHERE system_ms IS NULL) AS invalid_system_rows
        FROM rows WHERE {DATA_CATEGORIES} GROUP BY exchange ORDER BY exchange""")
    export(con, out, "system_timestamp_quality", f"""WITH sequence AS (
        SELECT category,exchange,source_line,system_ms,
        lag(system_ms) OVER (ORDER BY source_line) AS previous_ms
        FROM rows WHERE {DATA_CATEGORIES})
        SELECT exchange,category,count(*) AS rows,
        count(*) FILTER (WHERE system_ms IS NULL) AS missing_or_invalid_system_time,
        count(*) FILTER (WHERE system_ms<previous_ms) AS backward_steps_from_previous_file_row,
        max(greatest(0,previous_ms-system_ms)) AS largest_step_from_previous_file_row_ms
        FROM sequence GROUP BY ALL ORDER BY ALL""")
    export(con, out, "gap_outliers", """SELECT source_line,symbol,sec_type,system_ms,event_ms,
        system_ms-event_ms AS gap_ms,price FROM prices
        WHERE abs(system_ms-event_ms)>2000 ORDER BY source_line""")
    con.execute(f"""CREATE OR REPLACE TABLE seconds AS
        SELECT exchange, system_ms // 1000 AS second, count(*) AS events,
        count(*) FILTER (WHERE category='price') AS price_events
        FROM rows WHERE {DATA_CATEGORIES} AND system_ms IS NOT NULL GROUP BY ALL""")
    con.execute(f"COPY seconds TO {quote(cache / 'rates_second.parquet')} (FORMAT PARQUET)")
    export(con, out, "rates_minute", """SELECT exchange, second // 60 * 60 AS minute,
        sum(events) AS events, sum(price_events) AS price_events, max(events) AS peak_events_second
        FROM seconds GROUP BY ALL ORDER BY minute, exchange""")
    export(con, out, "trading_rates_minute", """SELECT exchange, event_ms//60000*60 AS minute,
        count(*) AS price_events FROM prices GROUP BY ALL ORDER BY minute, exchange""")
    export(con, out, "rates_summary", rates_summary("seconds"))
    con.execute("""CREATE OR REPLACE TABLE trading_seconds AS SELECT exchange,event_ms//1000 AS second,
        count(*) AS events,count(*) AS price_events FROM prices GROUP BY ALL""")
    export(con, out, "trading_rates_summary", rates_summary("trading_seconds"))
    export(con, out, "activity_spikes", f"""WITH groups AS (
        SELECT exchange,second,events FROM seconds UNION ALL SELECT 'ALL',second,sum(events) FROM seconds GROUP BY second),
        bands AS (SELECT label,{day_start//1000}+lo*3600 AS lo,{day_start//1000}+hi*3600 AS hi
          FROM (VALUES ('morning_07_to_10',7,10),('evening_16_to_19',16,19)) v(label,lo,hi)),
        dense AS (SELECT e.exchange,b.label,r.second,coalesce(g.events,0) AS events FROM
          (SELECT DISTINCT exchange FROM groups) e CROSS JOIN bands b CROSS JOIN LATERAL range(b.lo,b.hi) r(second)
          LEFT JOIN groups g ON g.exchange=e.exchange AND g.second=r.second)
        SELECT exchange,label,min(second) AS first_second,max(second) AS last_second,count(*) AS observation_seconds,
          sum(events) AS events, max(events) AS peak_events_per_second,
          arg_max(second,struct_pack(rate:=events,earliest:=-second)) AS first_peak_second,
          quantile_disc(events,0.99) AS p99_events_per_second FROM dense GROUP BY ALL ORDER BY ALL""")
    export(con, out, "replay_targets", f"""SELECT label,multiplier,rate*multiplier AS offered_events_per_second
        FROM read_csv({quote(out / 'rates_summary.csv')}) s,
        LATERAL (VALUES ('system_time_p99',s.p99_events_per_second),('system_time_peak',s.peak_events_per_second)) r(label,rate),
        (VALUES (1),(10),(100)) m(multiplier) WHERE s.exchange='ALL' ORDER BY label,multiplier""")
    export(con, out, "peak_seconds", """SELECT * FROM seconds QUALIFY row_number() OVER
        (PARTITION BY exchange ORDER BY events DESC, second) <= 10 ORDER BY exchange, events DESC, second""")
    con.execute(f"""CREATE OR REPLACE TABLE windows AS SELECT symbol, window_end,
        arg_max(price, struct_pack(t := event_ms, p := source_line)) AS close,
        arg_max(source_line, struct_pack(t := event_ms, p := source_line)) AS close_source_line,
        count(*) AS price_events FROM prices GROUP BY symbol, window_end""")
    con.execute(f"COPY (SELECT * FROM windows ORDER BY symbol, window_end) TO {quote(cache / 'windows.parquet')} (FORMAT PARQUET)")
    export(con, out, "window_statistics", f"""WITH spans AS (
        SELECT exchange, min(system_ms)//{WINDOW_MS} AS lo, max(system_ms)//{WINDOW_MS} AS hi
        FROM rows WHERE {DATA_CATEGORIES} AND system_ms IS NOT NULL GROUP BY exchange),
        counts AS (SELECT symbol, count(*) AS nonempty_windows,
          count(*) FILTER (WHERE window_end>{day_start} AND window_end<={day_start+DAY_MS}) AS nonempty_calendar_windows,
          min(window_end) AS first_window_end, max(window_end) AS last_window_end,
          min(price_events) AS min_events, quantile_disc(price_events,0.5) AS p50_events,
          quantile_disc(price_events,0.99) AS p99_events, max(price_events) AS max_events FROM windows GROUP BY symbol),
        session_counts AS (SELECT w.symbol, count(*) AS nonempty_session_windows FROM windows w
          JOIN (SELECT DISTINCT symbol, exchange FROM symbols) sy USING(symbol) JOIN spans s USING(exchange)
          WHERE w.window_end//{WINDOW_MS}-1 BETWEEN s.lo AND s.hi GROUP BY w.symbol)
        SELECT sy.*, coalesce(c.nonempty_windows,0) AS nonempty_windows, c.first_window_end, c.last_window_end,
          288 AS calendar_windows, 288-coalesce(c.nonempty_calendar_windows,0) AS empty_calendar_windows,
          s.hi-s.lo+1 AS observed_exchange_windows,
          s.hi-s.lo+1-coalesce(sc.nonempty_session_windows,0) AS empty_observed_exchange_windows,
          c.min_events, c.p50_events, c.p99_events, c.max_events
        FROM symbols sy LEFT JOIN counts c USING(symbol) LEFT JOIN spans s USING(exchange)
        LEFT JOIN session_counts sc USING(symbol) ORDER BY sy.symbol, sy.sec_type""")
    export(con, out, "window_event_histogram", "SELECT price_events, count(*) AS windows FROM windows GROUP BY ALL ORDER BY ALL")
    export(con, out, "symbol_disorder", """WITH d AS (SELECT *,
        lag(event_ms) OVER (PARTITION BY symbol ORDER BY source_line) AS previous_ms,
        max(event_ms) OVER (PARTITION BY symbol ORDER BY source_line ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS previous_max
        FROM prices)
        SELECT symbol, count(*) AS events, count(previous_ms) AS consecutive_pairs,
        count(*) FILTER (WHERE event_ms<previous_ms) AS backward_pairs,
        count(*) FILTER (WHERE event_ms=previous_ms) AS equal_pairs,
        max(greatest(0,previous_ms-event_ms)) AS max_backward_ms,
        max(greatest(0,previous_max-event_ms)) AS max_running_disorder_ms
        FROM d GROUP BY symbol ORDER BY max_running_disorder_ms DESC, symbol""")


def skew_tables(con, out):
    symbols = con.execute("SELECT symbol, sum(raw_events), sum(accepted_price_events) FROM symbols GROUP BY symbol ORDER BY symbol").fetchall()
    con.execute("CREATE OR REPLACE TABLE hashes(symbol VARCHAR, hash BIGINT)")
    con.executemany("INSERT INTO hashes VALUES (?,?)", [(symbol, kafka_hash(symbol)) for symbol, _, _ in symbols])
    skew = []
    for label, index in [("raw_events", 1), ("accepted_price_events", 2)]:
        values = sorted(row[index] for row in symbols)
        total, count = sum(values), len(values)
        gini = 2 * sum((i+1)*value for i, value in enumerate(values))/(count*total)-(count+1)/count if total else 0
        for k in (1, 10, 50, 100, 500):
            skew.append((label, count, sum(value == 0 for value in values), gini, k, sum(values[-k:])/total if total else 0))
    pl.DataFrame(skew, schema=["population", "symbols", "zero_event_symbols", "gini", "k", "top_k_share"], orient="row").write_csv(out / "skew.csv")
    counts = ",".join(f"({n})" for n in PARTITION_COUNTS)
    export(con, out, "partition_load", f"""SELECT n AS partitions, p AS partition,
        coalesce(sum(s.raw_events),0) AS raw_events, coalesce(sum(s.accepted_price_events),0) AS accepted_price_events,
        count(DISTINCT s.symbol) AS symbols FROM (VALUES {counts}) v(n)
        CROSS JOIN LATERAL range(n) r(p) LEFT JOIN hashes h ON h.hash % n=p
        LEFT JOIN symbols s ON s.symbol=h.symbol GROUP BY n,p ORDER BY n,p""")
    quality = ("missing_trading_dates","invalid_event_timestamps","date_mismatches","system_lags_over_2s","trading_lags_over_2s")
    export(con,out,"partition_timestamp_quality", f"""SELECT n AS partitions,h.hash%n AS partition,
        {','.join(f'sum(s.{name}) AS {name}' for name in quality)}
        FROM (VALUES {counts}) v(n) CROSS JOIN hashes h JOIN symbols s USING(symbol)
        GROUP BY n,partition ORDER BY n,partition""")


def watermark_tables(con, out, cache):
    results, disorder = [], []
    for partitions in PARTITION_COUNTS:
        print(f"watermark envelope: {partitions} partitions", flush=True)
        con.execute(f"""CREATE OR REPLACE TABLE admission AS WITH progress AS (
            SELECT p.*, h.hash % {partitions} AS partition,
              lag(event_ms) OVER (PARTITION BY h.hash % {partitions} ORDER BY source_line) AS previous_event,
              max(event_ms) OVER (PARTITION BY h.hash % {partitions} ORDER BY source_line
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS previous_max
            FROM prices p JOIN hashes h USING(symbol))
            SELECT *, greatest(0,previous_max-window_end+1) AS min_bound_ms,
              greatest(0,previous_max-event_ms) AS disorder_ms FROM progress""")
        disorder.extend((partitions, *row) for row in con.execute("""SELECT partition, count(*),
            quantile_disc(disorder_ms,0.5), quantile_disc(disorder_ms,0.99), max(disorder_ms), max(min_bound_ms),
            count(previous_event),count(*) FILTER (WHERE event_ms<previous_event)
            FROM admission GROUP BY partition ORDER BY partition""").fetchall())
        max_bound = con.execute("SELECT coalesce(max(min_bound_ms),0) FROM admission").fetchone()[0]
        bounds = sorted({0, 1, 10, 25, 50, 100, 264, 500, 1000, 60000, max_bound})
        for bound in bounds:
            late = con.execute("SELECT count(*) FROM admission WHERE min_bound_ms>?", [bound]).fetchone()[0]
            if late:
                missing, changed = con.execute("""WITH admitted AS (SELECT symbol, window_end,
                    arg_max(price, struct_pack(t:=event_ms,p:=source_line)) AS close
                    FROM admission WHERE min_bound_ms<=? GROUP BY symbol, window_end)
                    SELECT count(*) FILTER (WHERE a.close IS NULL), count(*) FILTER (WHERE a.close IS NOT NULL AND a.close!=w.close)
                    FROM windows w LEFT JOIN admitted a USING(symbol,window_end)""", [bound]).fetchone()
            else:
                missing, changed = 0, 0
            results.append((partitions, bound, late, missing, changed, missing+changed))
        if partitions == 8:
            con.execute(f"COPY (SELECT source_line,symbol,event_ms,system_ms,window_end,price,partition,min_bound_ms FROM admission WHERE min_bound_ms>0 ORDER BY source_line) TO {quote(cache / 'late_candidates_8.parquet')} (FORMAT PARQUET)")
    pl.DataFrame(results, schema=["partitions", "bound_ms", "discarded_events", "missing_windows", "changed_existing_closes", "changed_closes"], orient="row").write_csv(out / "watermark_bounds.csv")
    pl.DataFrame(disorder, schema=["partitions", "partition", "events", "p50_disorder_ms", "p99_disorder_ms", "max_disorder_ms", "min_bound_all_events_ms", "consecutive_pairs", "backward_pairs"], orient="row").write_csv(out / "partition_disorder.csv")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input", type=Path)
    parser.add_argument("--day", type=dt.date.fromisoformat, required=True)
    parser.add_argument("--zone-offset", default="+02:00")
    parser.add_argument("--cache", type=Path, default=Path("data/analysis"))
    parser.add_argument("--out", type=Path, default=Path("analysis/out/daily"))
    parser.add_argument("--memory-limit", default="2GB")
    args = parser.parse_args()
    try:
        offset_ms(args.zone_offset)
    except ValueError as exc:
        parser.error(str(exc))
    started = time.monotonic()
    cache, out = args.cache / str(args.day), args.out / str(args.day)
    cache.mkdir(parents=True, exist_ok=True)
    out.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(cache).free < 15 * 1024**3:
        raise SystemExit("Need at least 15 GiB free for analysis checkpoints and spill")
    (out / "manifest.json").unlink(missing_ok=True)
    parsed, identity, parse_metrics, reused = prepare(args, cache)
    con = duckdb.connect(str(cache / "working.duckdb"))
    con.execute("SET memory_limit=?", [args.memory_limit])
    con.execute("SET threads=4")
    con.execute("SET temp_directory=?", [str(cache / "spill")])
    con.execute("SET max_temp_directory_size='12GB'")
    con.execute(f"CREATE OR REPLACE VIEW rows AS SELECT * FROM read_parquet({quote(parsed)})")
    con.execute(f"""CREATE OR REPLACE TABLE prices AS SELECT source_line,symbol,exchange,sec_type,
        event_ms,system_ms,price,(event_ms//{WINDOW_MS}+1)*{WINDOW_MS} AS window_end
        FROM rows WHERE category='price' AND event_ms IS NOT NULL""")
    day_start = (args.day-dt.date(1970,1,1)).days*DAY_MS-offset_ms(args.zone_offset)
    basic_tables(con, out, cache, day_start)
    skew_tables(con, out)
    watermark_tables(con, out, cache)
    counts = dict(con.execute("SELECT category,count(*) FROM rows GROUP BY category").fetchall())
    counts["accepted_price_events"] = con.execute("SELECT count(*) FROM prices").fetchone()[0]
    counts["windows"] = con.execute("SELECT count(*) FROM windows").fetchone()[0]
    con.close()
    from daily_plot import plot
    plot(out, args.zone_offset)
    provenance_path = args.input.with_suffix(".json")
    provenance = json.loads(provenance_path.read_text()) if provenance_path.exists() else None
    if provenance and provenance["sha256"] != identity["input_sha256"]:
        raise SystemExit("Input differs from downloaded provenance")
    peak = peak_rss_bytes()
    manifest = {
        "day": str(args.day), "input": str(args.input), "identity": identity, "provenance": provenance,
        "zone_offset": args.zone_offset, "memory_limit_duckdb": args.memory_limit,
        "counts": counts, "wall_seconds": round(time.monotonic()-started,3), "peak_rss_bytes": peak,
        "checkpoint_reused": reused, "parse_metrics": parse_metrics,
        "python": platform.python_version(), "duckdb": duckdb.__version__, "polars": pl.__version__,
        "revision": os.environ.get("ANALYSIS_REVISION") or subprocess.check_output(["git","rev-parse","HEAD"], text=True).strip(),
        "worktree_status": os.environ.get("ANALYSIS_WORKTREE_STATUS", "not supplied; source hashes identify executed files"),
        "source_sha256": {p.name: checksum(p,"sha256") for p in sorted(Path(__file__).parent.glob("*.py"))},
        "dependencies": {name: importlib.metadata.version(name) for name in ("duckdb","polars","numpy","matplotlib")},
        "command": sys.argv,
        "artifacts": {p.name: {"bytes":p.stat().st_size,"sha256":checksum(p,"sha256")} for p in sorted(out.iterdir()) if p.is_file()},
        "checkpoint": str(cache),
        "limits": ["One day only; no full-week policy claim", "Partition-local aggressive watermark envelope; active partitions only",
                   "Replay scheduling, periodic watermark emission, and idleness require runtime verification",
                   "Observed exchange span is a dataset interval, not an official trading calendar",
                   "System timestamp is a recorded last-update time, not verified feed arrival time",
                   "Changed-close counts describe each model, not a bound over all replay schedules"],
    }
    save_json(out / "manifest.json", manifest)
    print(json.dumps({"out":str(out),"counts":counts,"wall_seconds":manifest["wall_seconds"],"peak_rss_bytes":peak}), flush=True)


if __name__ == "__main__":
    main()
