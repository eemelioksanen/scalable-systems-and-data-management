"""Profiles the sampled weekday slice and writes the evidence tables the data contract cites.

Pass 1 streams each chunk in Python. It is the only way to attach a source position
that genuinely reflects file order: DuckDB's CSV reader is parallel, so row order in
its output is not the order in the file, and the ordering analysis depends on it.

Pass 2 runs the aggregate queries in DuckDB over the Parquet that pass 1 produced.
"""
from __future__ import annotations

import csv, io, json, os, re, sys
from collections import Counter
from pathlib import Path

import duckdb

DATA = Path(os.environ.get("DATA_DIR") or ("/data" if Path("/data").exists() else "data"))
OUT = Path(os.environ.get("OUT_DIR") or ("/out" if Path("/out").exists() else "analysis/out"))
SLICE = DATA / "slice"
PARSED_CSV = SLICE / "parsed.csv"
PARSED = SLICE / "parsed.parquet"

COLUMNS = [
    "id", "sec_type", "date", "time", "ask", "ask_volume", "bid", "bid_volume",
    "ask_time", "day_high_ask", "close", "currency", "day_high_ask_time", "day_high",
    "isin", "auction_price", "day_low_ask", "day_low", "day_low_ask_time", "open",
    "nominal_value", "last", "last_volume", "trading_time", "total_volume",
    "mid_price", "trading_date", "profit", "current_price", "related_indices",
    "day_high_bid_time", "day_low_bid_time", "open_time", "last_trade_time",
    "close_time", "day_high_time", "day_low_time", "bid_time", "auction_time",
]
KEEP = ["id", "sec_type", "date", "time", "ask", "bid", "close", "open", "last",
        "last_volume", "trading_time", "total_volume", "mid_price", "trading_date",
        "current_price"]
HEADER_FIRST_FIELD = "ID"


def stream_chunks() -> dict:
    """Parses every chunk, writes Parquet with chunk + source position, returns raw-line stats."""
    stats = {
        "chunk_files": [], "comment_lines": 0, "header_lines": 0, "data_lines": 0,
        "field_count_histogram": Counter(), "lines_with_double_quote": 0,
        "lines_with_crlf": 0, "lines_with_lf_only": 0, "malformed_lines": 0,
        "rows_with_trailing_delimiter": 0,
    }
    fout = open(PARSED_CSV, "w", newline="")
    writer = csv.writer(fout)
    writer.writerow(["chunk", "src_pos"] + KEEP)

    for path in sorted(SLICE.glob("chunk_*.csv")):
        chunk = path.stem
        stats["chunk_files"].append(chunk)
        with open(path, "rb") as fh:
            for src_pos, raw in enumerate(fh):
                if raw.endswith(b"\r\n"):
                    stats["lines_with_crlf"] += 1
                elif raw.endswith(b"\n"):
                    stats["lines_with_lf_only"] += 1
                line = raw.decode("utf-8", errors="replace").rstrip("\r\n")
                if not line:
                    continue
                if line.startswith("#"):
                    stats["comment_lines"] += 1
                    continue
                if b'"' in raw:
                    stats["lines_with_double_quote"] += 1
                fields = next(csv.reader(io.StringIO(line)))
                stats["field_count_histogram"][len(fields)] += 1
                if fields[0] == HEADER_FIRST_FIELD:
                    stats["header_lines"] += 1
                    continue
                # Data rows end with a trailing comma, so they carry one unnamed empty
                # field beyond the 39 the header declares. Rejecting those would reject
                # every row in the file.
                if len(fields) == len(COLUMNS) + 1 and fields[-1] == "":
                    stats["rows_with_trailing_delimiter"] += 1
                    fields = fields[:-1]
                if len(fields) != len(COLUMNS):
                    stats["malformed_lines"] += 1
                    continue
                stats["data_lines"] += 1
                row = dict(zip(COLUMNS, fields))
                writer.writerow([chunk, src_pos] + [row[c] for c in KEEP])
    fout.close()
    stats["field_count_histogram"] = dict(sorted(stats["field_count_histogram"].items()))
    return stats


def frac_digits(series: str) -> str:
    """SQL fragment: number of digits after the decimal point, or NULL when absent."""
    return (f"CASE WHEN {series} IS NULL OR {series} = '' OR position('.' in {series}) = 0 "
            f"THEN NULL ELSE length({series}) - position('.' in {series}) END")


def md_table(rows, headers) -> str:
    if not rows:
        return "_(no rows)_\n"
    widths = [max(len(str(h)), *(len(str(r[i])) for r in rows)) for i, h in enumerate(headers)]
    out = ["| " + " | ".join(str(h).ljust(w) for h, w in zip(headers, widths)) + " |",
           "|" + "|".join("-" * (w + 2) for w in widths) + "|"]
    for r in rows:
        out.append("| " + " | ".join(str(c).ljust(w) for c, w in zip(r, widths)) + " |")
    return "\n".join(out) + "\n"


def q(con, sql: str, title: str, out: list[str], csv_name: str | None = None):
    rel = con.sql(sql)
    rows, headers = rel.fetchall(), rel.columns
    out.append(f"\n### {title}\n\n" + md_table(rows, headers))
    if csv_name:
        with open(OUT / csv_name, "w", newline="") as fh:
            w = csv.writer(fh)
            w.writerow(headers)
            w.writerows(rows)


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    if not list(SLICE.glob("chunk_*.csv")):
        print(f"no chunks in {SLICE}; run analysis/src/fetch_slice.sh first", file=sys.stderr)
        return 1

    raw = stream_chunks()
    con = duckdb.connect()
    con.execute("SET temp_directory = ?", [str(OUT / ".tmp")])
    # Parquet conversion is a one-off so the aggregate queries below are not re-parsing
    # CSV each time. Order is never relied on: chunk and src_pos are explicit columns.
    # DuckDB's CSV reader maps an empty field to NULL, which makes every `col = ''`
    # test below silently false. The distinction matters here -- an absent value and an
    # empty one are the same thing in this feed -- so normalise to empty string once.
    cols = ", ".join(f"coalesce({c}, '') AS {c}" for c in KEEP)
    con.execute(f"""COPY (SELECT chunk, src_pos, {cols}
                          FROM read_csv('{PARSED_CSV}', header=true, all_varchar=true))
                    TO '{PARSED}' (FORMAT parquet, COMPRESSION zstd)""")
    con.execute(f"CREATE VIEW t AS SELECT * FROM read_parquet('{PARSED}')")
    # A price event is a row carrying a trade price. Zero is tracked apart from empty:
    # it is syntactically present but is not a traded price.
    con.execute("""
        CREATE VIEW c AS SELECT *,
          regexp_extract(id, '\\.([A-Za-z]+)$', 1) AS exchange,
          TRY_CAST(last AS DOUBLE) AS last_num,
          CASE
            WHEN last IS NOT NULL AND last <> '' AND TRY_CAST(last AS DOUBLE) > 0 THEN 'price'
            WHEN last IS NOT NULL AND last <> '' THEN 'price_zero'
            WHEN ask <> '' OR bid <> '' THEN 'quote'
            WHEN close = '' AND open = '' AND mid_price = '' AND total_volume = ''
                 AND current_price = '' AND trading_date = '' THEN 'empty_payload'
            WHEN TRY_CAST(nullif(close, '') AS DOUBLE) = 0
                 AND TRY_CAST(nullif(open, '') AS DOUBLE) = 0 THEN 'zero_filled'
            ELSE 'other'
          END AS event_class
        FROM t
    """)

    doc: list[str] = [
        "# Weekday slice profile\n",
        "Generated by `analysis/src/profile.py`. Provenance and sampling method: "
        "`analysis/out/provenance.json`.\n",
        "\n## 1. File structure\n",
        f"\n- chunks: `{', '.join(raw['chunk_files'])}`\n"
        f"- comment lines (`#`): **{raw['comment_lines']:,}**\n"
        f"- header lines: **{raw['header_lines']:,}**\n"
        f"- data rows parsed: **{raw['data_lines']:,}**\n"
        f"- rows with a trailing delimiter (unnamed empty 40th field): "
        f"**{raw['rows_with_trailing_delimiter']:,}**\n"
        f"- malformed rows (field count neither {len(COLUMNS)} nor {len(COLUMNS)+1}): "
        f"**{raw['malformed_lines']:,}**\n"
        f"- lines containing a double quote: **{raw['lines_with_double_quote']:,}**\n"
        f"- CRLF line endings: **{raw['lines_with_crlf']:,}**, bare LF: **{raw['lines_with_lf_only']:,}**\n"
        f"- field-count histogram: `{raw['field_count_histogram']}`\n"
        f"- declared columns: **{len(COLUMNS)}**\n",
    ]

    doc.append("\n## 2. Row taxonomy\n")
    q(con, """SELECT event_class, count(*) AS rows,
                     round(100.0 * count(*) / sum(count(*)) OVER (), 3) AS pct
              FROM c GROUP BY 1 ORDER BY rows DESC""",
      "By event class", doc, "taxonomy_overall.csv")
    q(con, """SELECT exchange, sec_type, event_class, count(*) AS rows
              FROM c GROUP BY 1,2,3 ORDER BY exchange, sec_type, rows DESC""",
      "By exchange and security type", doc, "taxonomy_by_exchange_sectype.csv")
    q(con, """SELECT exchange, count(*) AS rows, count(DISTINCT id) AS symbols
              FROM c GROUP BY 1 ORDER BY rows DESC""", "Exchange split", doc)
    q(con, "SELECT sec_type, count(*) AS rows, count(DISTINCT id) AS symbols FROM c GROUP BY 1 ORDER BY rows DESC",
      "Security types present", doc)

    doc.append("\n## 3. Symbol key format\n")
    q(con, """SELECT regexp_matches(id, '^[A-Za-z0-9]+\\.[A-Za-z]+$') AS matches_symbol_dot_exchange,
                     count(*) AS rows, count(DISTINCT id) AS symbols
              FROM c GROUP BY 1""", "ID shape", doc)
    q(con, "SELECT id FROM c WHERE NOT regexp_matches(id, '^[A-Za-z0-9]+\\.[A-Za-z]+$') GROUP BY 1 LIMIT 20",
      "IDs not matching SYMBOL.EXCHANGE", doc)

    doc.append("\n## 4. Numeric and timestamp precision\n")
    q(con, f"""SELECT {frac_digits('last')} AS decimals, count(*) AS price_rows
               FROM c WHERE event_class IN ('price','price_zero') GROUP BY 1 ORDER BY 1""",
      "Decimal places in `Last` (price rows)", doc)
    q(con, f"""SELECT {frac_digits('time')} AS frac_digits, count(*) AS rows
               FROM c GROUP BY 1 ORDER BY 1""", "Fractional digits in system `Time`", doc)
    q(con, f"""SELECT {frac_digits('trading_time')} AS frac_digits, count(*) AS rows
               FROM c WHERE event_class = 'price' GROUP BY 1 ORDER BY 1""",
      "Fractional digits in `Trading time` (price rows)", doc)
    q(con, """SELECT right(trading_time, 3) AS last_3_frac_chars, count(*) AS price_rows
              FROM c WHERE event_class = 'price' AND trading_time <> ''
              GROUP BY 1 ORDER BY price_rows DESC LIMIT 12""",
      "Most common fractional values in `Trading time`", doc)

    doc.append("\n## 5. Price fields: is `Last` the right one?\n")
    q(con, """SELECT
                count(*) FILTER (WHERE last <> '')          AS last_present,
                count(*) FILTER (WHERE close <> '')         AS close_present,
                count(*) FILTER (WHERE current_price <> '') AS current_price_present,
                count(*) FILTER (WHERE ask <> '')           AS ask_present,
                count(*) FILTER (WHERE bid <> '')           AS bid_present,
                count(*) FILTER (WHERE mid_price <> '')     AS mid_price_present
              FROM c""", "Field population across all rows", doc)
    q(con, """SELECT
                count(*) AS price_rows,
                count(*) FILTER (WHERE close = '')  AS close_empty_on_price_rows,
                count(*) FILTER (WHERE close <> '' AND TRY_CAST(close AS DOUBLE) = 0) AS close_zero_on_price_rows,
                count(*) FILTER (WHERE close <> '' AND TRY_CAST(close AS DOUBLE) = last_num) AS close_equals_last
              FROM c WHERE event_class = 'price'""",
      "`Close` on rows that carry a trade price", doc)
    q(con, """SELECT count(*) AS rows_with_close_but_no_last
              FROM c WHERE close <> '' AND TRY_CAST(close AS DOUBLE) > 0 AND (last = '' OR last IS NULL)""",
      "Rows where `Close` is set but `Last` is not", doc)

    doc.append("\n## 6. System time vs trading time\n")
    q(con, """SELECT
                count(*) AS price_rows,
                count(*) FILTER (WHERE trading_time = '') AS missing_trading_time,
                count(*) FILTER (WHERE trading_date = '') AS missing_trading_date,
                count(*) FILTER (WHERE trading_date <> '' AND trading_date <> date) AS trading_date_ne_date,
                count(*) FILTER (WHERE trading_date = '' AND time <> '') AS no_trading_date_but_has_system_time
              FROM c WHERE event_class = 'price'""", "Completeness on price rows", doc)
    q(con, """SELECT trading_date, count(*) AS price_rows
              FROM c WHERE event_class = 'price' GROUP BY 1 ORDER BY price_rows DESC LIMIT 10""",
      "`Trading date` values on price rows", doc)
    q(con, """SELECT
                min(time) AS min_system_time, max(time) AS max_system_time,
                min(trading_time) FILTER (WHERE trading_time <> '') AS min_trading_time,
                max(trading_time) FILTER (WHERE trading_time <> '') AS max_trading_time
              FROM c WHERE event_class = 'price'""", "Timestamp coverage (price rows)", doc)
    q(con, """WITH g AS (
                SELECT date_diff('millisecond',
                         strptime(trading_time, '%H:%M:%S.%g'),
                         strptime(time, '%H:%M:%S.%g')) AS gap_ms
                FROM c WHERE event_class = 'price' AND trading_time <> '' AND time <> '')
              SELECT count(*) AS n, min(gap_ms) AS min_ms,
                     quantile_cont(gap_ms, 0.5) AS p50_ms,
                     quantile_cont(gap_ms, 0.99) AS p99_ms, max(gap_ms) AS max_ms,
                     count(*) FILTER (WHERE gap_ms < 0) AS negative_gaps
              FROM g""", "System `Time` minus `Trading time`", doc)

    doc.append("\n## 7. Ordering and ties (decides the tie-break rule)\n")
    doc.append("\nSource order holds within a chunk only, so every query here is partitioned "
               "by chunk as well as by symbol.\n")
    q(con, """WITH p AS (
                SELECT chunk, id, src_pos, trading_time,
                       lag(trading_time) OVER (PARTITION BY chunk, id ORDER BY src_pos) AS prev_tt
                FROM c WHERE event_class = 'price' AND trading_time <> '')
              SELECT count(*) AS consecutive_price_pairs,
                     count(*) FILTER (WHERE trading_time < prev_tt) AS backward_jumps,
                     count(*) FILTER (WHERE trading_time = prev_tt) AS equal_timestamps,
                     round(100.0 * count(*) FILTER (WHERE trading_time < prev_tt)
                           / nullif(count(*) FILTER (WHERE prev_tt IS NOT NULL), 0), 6) AS pct_backward
              FROM p WHERE prev_tt IS NOT NULL""",
      "Per-symbol backward jumps in `Trading time`, in file order", doc, "ordering_backward_jumps.csv")
    q(con, """WITH d AS (
                SELECT chunk, id, trading_time, count(*) AS n,
                       count(DISTINCT last) AS distinct_prices
                FROM c WHERE event_class = 'price' AND trading_time <> ''
                GROUP BY 1,2,3 HAVING count(*) > 1)
              SELECT count(*) AS timestamp_collisions,
                     sum(n) AS rows_involved,
                     count(*) FILTER (WHERE distinct_prices > 1) AS collisions_with_differing_price
              FROM d""",
      "Same symbol, identical `Trading time`", doc, "ordering_collisions.csv")

    doc.append("\n## 8. Windowing inputs\n")
    q(con, """SELECT count(DISTINCT id) AS symbols_with_price_events,
                     count(*) AS price_rows,
                     round(count(*) * 1.0 / count(DISTINCT id), 2) AS mean_price_rows_per_symbol
              FROM c WHERE event_class = 'price'""", "Symbols carrying trades", doc)
    q(con, """SELECT id, count(*) AS price_rows
              FROM c WHERE event_class = 'price' GROUP BY 1 ORDER BY price_rows DESC LIMIT 15""",
      "Most active symbols in the slice", doc, "top_symbols.csv")

    (OUT / "profile.md").write_text("".join(doc))
    (OUT / "raw_line_stats.json").write_text(json.dumps(raw, indent=2))
    print(f"wrote {OUT/'profile.md'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
