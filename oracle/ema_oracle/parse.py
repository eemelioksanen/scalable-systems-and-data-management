"""Read one DEBS 2022 trading CSV file into classified rows and price events.

Every rule here cites the data contract (docs/data-contract.md) or the decision log:

- D-21: comment lines are skipped by their `#` prefix, not by a count; a data row has 39
  fields, or 40 when the trailing delimiter adds an empty last field; no quoting; an empty
  field is null; columns are matched by position.
- D-1: `ID` verbatim is the symbol key; the exchange is its suffix.
- D-2 (refined): a price event is a row whose `Last` is non-empty and > 0.
- D-3, D-25: event time is `Trading date` + `Trading time`, with `Date` substituted for a
  missing `Trading date` (the default missing-date policy), at one configurable UTC offset.
- D-22, D-26: event time is epoch milliseconds; fraction digits beyond milliseconds are
  truncated.
- D-23: source position is (file order, physical line number).

The file is read as one string column per physical line, so the row index is the line
number itself: blank and comment lines keep their place instead of being skipped by the
CSV reader.
"""

from __future__ import annotations

import datetime as dt
import re
from pathlib import Path

import polars as pl

# Column positions (data contract §2). Header names carry stray spaces, so never match by name.
COL_ID = 0
COL_SEC_TYPE = 1
COL_DATE = 2
COL_LAST = 21
COL_TRADING_TIME = 23
COL_TRADING_DATE = 26
DECLARED_COLUMNS = 39

HEADER_PREFIX = "ID,SecType,"

# Row categories, in the order they are tested. Every physical line gets exactly one.
CATEGORIES = (
    "comment",
    "header",
    "blank",
    "malformedRow",
    "nonPrice",
    "malformedPrice",
    "nonPositivePrice",
    "missingTimestamp",
    "malformedTimestamp",
    "accepted",
)

MISSING_DATE_POLICIES = ("system-date", "drop")

_PRICE = r"^-?[0-9]+(\.[0-9]+)?$"
_DATE = r"^([0-9]{2})-([0-9]{2})-([0-9]{4})$"
_TIME = r"^([0-9]{2}):([0-9]{2}):([0-9]{2})(?:\.([0-9]+))?$"
_FILE_DATE = re.compile(r"(\d{2})-(\d{2})-(\d{2})(?!\d)")


def parse_zone_offset(text: str) -> int:
    """Return a UTC offset such as `Z`, `+01:00` or `-0530` in milliseconds.

    Mirrors the Flink job's `--zone-offset` flag (D-19), which takes the same values.
    """
    if text in ("Z", "z", "+00:00", "-00:00"):
        return 0
    m = re.fullmatch(r"([+-])(\d{2}):?(\d{2})?", text)
    if not m:
        raise ValueError(f"invalid zone offset {text!r}; expected Z or +HH:MM")
    sign = 1 if m.group(1) == "+" else -1
    hours, minutes = int(m.group(2)), int(m.group(3) or 0)
    if hours > 18 or minutes > 59:
        raise ValueError(f"zone offset out of range: {text!r}")
    return sign * (hours * 3_600_000 + minutes * 60_000)


def file_date(path: Path) -> dt.date:
    """The trading day a file belongs to, from its `DD-MM-YY` name suffix.

    File date is the first component of the D-23 source position, so an input whose name
    carries no date cannot be ordered and is rejected rather than guessed.
    """
    matches = _FILE_DATE.findall(path.name)
    if not matches:
        raise ValueError(f"cannot order {path.name}: no DD-MM-YY date in the file name")
    d, m, y = matches[-1]
    return dt.date(2000 + int(y), int(m), int(d))


def order_inputs(paths: list[Path]) -> list[Path]:
    """Source order across files: by file date, then by name."""
    return sorted(paths, key=lambda p: (file_date(p), p.name))


def _field(i: int) -> pl.Expr:
    return pl.col("_f").list.get(i, null_on_oob=True)


def classify(path: Path, zone_offset_ms: int, missing_date: str) -> pl.LazyFrame:
    """One row per physical line of `path`, with its category and, if accepted, its event.

    Output columns: sourceLine, category, symbol, secType, eventTime, price, systemDate,
    staleTradingDate. The event columns are null unless category == "accepted".
    """
    if missing_date not in MISSING_DATE_POLICIES:
        raise ValueError(f"missing-date policy must be one of {MISSING_DATE_POLICIES}")

    lines = pl.scan_csv(
        path,
        has_header=False,
        separator="\x1f",  # never occurs in the data, so each physical line is one field
        quote_char=None,
        schema={"line": pl.String},
        encoding="utf8-lossy",
    ).with_row_index("sourceLine", offset=1)

    line = pl.col("line")
    fields = lines.with_columns(_f=line.str.split(",")).with_columns(_n=pl.col("_f").list.len())

    symbol = _field(COL_ID)
    last = _field(COL_LAST)
    trading_time = _field(COL_TRADING_TIME)
    trading_date = _field(COL_TRADING_DATE)
    system_date = _field(COL_DATE)

    structural_ok = (
        (pl.col("_n") == DECLARED_COLUMNS)
        | ((pl.col("_n") == DECLARED_COLUMNS + 1) & (_field(DECLARED_COLUMNS) == ""))
    ) & (symbol != "")

    has_trading_date = trading_date != ""
    if missing_date == "system-date":
        date_text = pl.when(has_trading_date).then(trading_date).when(system_date != "").then(system_date)
    else:
        date_text = pl.when(has_trading_date).then(trading_date)

    # The regex pins the layout; strptime then rejects impossible dates such as 31-02.
    day = pl.when(date_text.str.contains(_DATE)).then(
        date_text.str.strptime(pl.Date, "%d-%m-%Y", strict=False)
    )
    epoch_day = day.cast(pl.Int32).cast(pl.Int64)

    t = trading_time.str.extract_groups(_TIME)
    hh = t.struct.field("1").cast(pl.Int64, strict=False)
    mm = t.struct.field("2").cast(pl.Int64, strict=False)
    ss = t.struct.field("3").cast(pl.Int64, strict=False)
    # D-26: keep milliseconds, truncate anything finer; ".5" means 500 ms.
    frac_ms = t.struct.field("4").str.slice(0, 3).str.pad_end(3, "0").cast(pl.Int64, strict=False).fill_null(0)
    ms_of_day = pl.when((hh < 24) & (mm < 60) & (ss < 60)).then(hh * 3_600_000 + mm * 60_000 + ss * 1_000 + frac_ms)

    staged = fields.with_columns(
        _last=last,
        _price=last.cast(pl.Float64, strict=False),
        _ttime=trading_time,
        _dtext=date_text,
        _event_time=epoch_day * 86_400_000 + ms_of_day - zone_offset_ms,
        _structural_ok=structural_ok,
        _system_date_used=~has_trading_date,
        _stale=has_trading_date & (system_date != "") & (trading_date != system_date),
    )
    category = (
        pl.when(line.str.starts_with("#"))
        .then(pl.lit("comment"))
        .when(line.str.starts_with(HEADER_PREFIX))
        .then(pl.lit("header"))
        .when(line.is_null() | (line == ""))
        .then(pl.lit("blank"))
        .when(~pl.col("_structural_ok"))
        .then(pl.lit("malformedRow"))
        .when(pl.col("_last") == "")
        .then(pl.lit("nonPrice"))
        .when(~pl.col("_last").str.contains(_PRICE))
        .then(pl.lit("malformedPrice"))
        .when(pl.col("_price") <= 0)
        .then(pl.lit("nonPositivePrice"))
        .when((pl.col("_ttime") == "") | pl.col("_dtext").is_null())
        .then(pl.lit("missingTimestamp"))
        .when(pl.col("_event_time").is_null())
        .then(pl.lit("malformedTimestamp"))
        .otherwise(pl.lit("accepted"))
    )

    accepted = pl.col("category") == "accepted"
    return staged.with_columns(category=category).select(
        "sourceLine",
        "category",
        symbol=pl.when(accepted).then(symbol),
        secType=pl.when(accepted).then(_field(COL_SEC_TYPE)),
        eventTime=pl.when(accepted).then(pl.col("_event_time")),
        price=pl.when(accepted).then(pl.col("_price")),
        systemDate=accepted & pl.col("_system_date_used"),
        staleTradingDate=accepted & pl.col("_stale"),
    )


def read_file(path: Path, zone_offset_ms: int, missing_date: str) -> tuple[dict[str, int], pl.DataFrame]:
    """Return (row counts by category, accepted price events in source order)."""
    rows = classify(path, zone_offset_ms, missing_date)
    counts_lf = rows.group_by("category").agg(
        rows=pl.len(),
        systemDate=pl.col("systemDate").sum(),
        staleTradingDate=pl.col("staleTradingDate").sum(),
    )
    events_lf = rows.filter(pl.col("category") == "accepted").select(
        "sourceLine", "symbol", "secType", "eventTime", "price"
    )
    counts_df, events = pl.collect_all([counts_lf, events_lf], engine="streaming")

    counts = {c: 0 for c in CATEGORIES}
    counts["systemDateEvents"] = 0
    counts["staleTradingDateEvents"] = 0
    for rec in counts_df.iter_rows(named=True):
        counts[rec["category"]] = rec["rows"]
        counts["systemDateEvents"] += rec["systemDate"]
        counts["staleTradingDateEvents"] += rec["staleTradingDate"]
    counts["lines"] = sum(counts[c] for c in CATEGORIES)
    counts["dataRows"] = counts["lines"] - counts["comment"] - counts["header"] - counts["blank"]
    return counts, events.sort("sourceLine")
