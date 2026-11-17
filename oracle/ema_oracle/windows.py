"""Window assignment, event admission, and close selection.

- Windows are five-minute tumbling windows aligned to the epoch in the configured offset;
  a window is identified by its exclusive end (interfaces.md, D-16).
- The close is `max by (eventTime, source position)` over the window's admitted events
  (D-4, D-23), where source position is (file order, physical line).
- Empty windows are skipped (D-6). `carry_forward` gives the D-42 alternative.
- Admission: a batch run sees every event, a streaming run does not. For each event the
  oracle records `minBoundMs`, the smallest watermark bound that admits it when the price
  events arrive one at a time in source order. See README.md, "Late events".
"""

from __future__ import annotations

import polars as pl

WINDOW_MS = 300_000
EMPTY_WINDOW_POLICIES = ("skip", "carry-forward")


def window_end(event_time: pl.Expr) -> pl.Expr:
    return (event_time // WINDOW_MS + 1) * WINDOW_MS


def with_admission(events: pl.DataFrame, prior_max: int | None) -> tuple[pl.DataFrame, int | None]:
    """Add windowEnd and minBoundMs to one file's events, given in source order.

    A bounded-out-of-orderness watermark after seeing a maximum event time M is M - B - 1,
    and a window [W - 5 min, W) is evaluated once the watermark reaches W - 1, so an event
    is late iff the maximum seen before it satisfies M - B >= W. The smallest admitting
    bound is therefore max(0, M - W + 1). `prior_max` carries M across files.
    """
    seen_before = pl.col("eventTime").cum_max().shift(1)
    out = events.with_columns(windowEnd=window_end(pl.col("eventTime"))).with_columns(
        minBoundMs=pl.max_horizontal(
            pl.lit(0, pl.Int64),
            pl.max_horizontal(seen_before, pl.lit(prior_max, pl.Int64)) - pl.col("windowEnd") + 1,
        )
    )
    seen = [m for m in (prior_max, events["eventTime"].max()) if m is not None]
    return out, max(seen) if seen else None


def file_candidates(events: pl.DataFrame, file_index: int) -> pl.DataFrame:
    """The latest admitted event per (symbol, window) within one file, plus the event count."""
    return (
        events.sort(["symbol", "windowEnd", "eventTime", "sourceLine"])
        .group_by(["symbol", "windowEnd"], maintain_order=True)
        .agg(
            pl.col("secType", "price", "eventTime", "sourceLine").last(),
            priceEvents=pl.len(),
        )
        .with_columns(fileIndex=pl.lit(file_index, pl.Int32))
    )


def select_closes(candidates: list[pl.DataFrame], file_names: list[str]) -> pl.DataFrame:
    """Merge per-file candidates into one close per (symbol, window), sorted by symbol and window.

    A window can draw events from more than one file (a trade reported in the next day's
    file), so the per-file maxima are merged under the same (eventTime, file, line) order.
    """
    names = pl.DataFrame(
        {"fileIndex": list(range(len(file_names))), "closeSourceFile": file_names},
        schema={"fileIndex": pl.Int32, "closeSourceFile": pl.String},
    )
    return (
        pl.concat(candidates)
        .sort(["symbol", "windowEnd", "eventTime", "fileIndex", "sourceLine"])
        .group_by(["symbol", "windowEnd"], maintain_order=True)
        .agg(
            pl.col("secType", "price", "eventTime", "fileIndex", "sourceLine").last(),
            priceEvents=pl.col("priceEvents").sum(),
        )
        .join(names, on="fileIndex", how="left")
        .select(
            "symbol",
            "secType",
            "windowEnd",
            close=pl.col("price"),
            priceEvents=pl.col("priceEvents").cast(pl.Int64),
            closeEventTime=pl.col("eventTime"),
            closeSourceFile=pl.col("closeSourceFile"),
            closeSourceLine=pl.col("sourceLine").cast(pl.Int64),
        )
        .sort(["symbol", "windowEnd"])
    )


def carry_forward(closes: pl.DataFrame) -> pl.DataFrame:
    """Fill each gap between a symbol's consecutive windows with windows that repeat the earlier close.

    A filled window keeps the earlier window's close, secType and close diagnostics, with
    priceEvents 0. Nothing is added after a symbol's last window. `closes` is sorted by
    (symbol, windowEnd), and so is the result.
    """
    return (
        closes.with_columns(
            windowEnd=pl.int_ranges(
                pl.col("windowEnd"),
                pl.col("windowEnd").shift(-1).over("symbol").fill_null(pl.col("windowEnd") + WINDOW_MS),
                WINDOW_MS,
                dtype=pl.Int64,
            ),
            carriedFrom=pl.col("windowEnd"),
        )
        .explode("windowEnd", empty_as_null=False)
        .with_columns(priceEvents=pl.when(pl.col("windowEnd") == pl.col("carriedFrom")).then(pl.col("priceEvents")).otherwise(0))
        .drop("carriedFrom")
    )
