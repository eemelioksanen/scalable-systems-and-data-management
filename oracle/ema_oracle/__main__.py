"""Command line: python -m ema_oracle [options] CSV..."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from . import parse, windows
from .run import Config, run


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="python -m ema_oracle", description=__doc__)
    p.add_argument("inputs", nargs="+", type=Path, help="DEBS 2022 day files; ordered by the DD-MM-YY date in their names")
    p.add_argument("--out", type=Path, default=Path(os.environ.get("ORACLE_OUT", "oracle/out")), help="output directory (default: oracle/out)")
    p.add_argument("--zone-offset", default="+02:00", help="UTC offset of trading timestamps, as the Flink job's flag (D-3, D-32); default +02:00")
    p.add_argument(
        "--missing-trading-date",
        choices=parse.MISSING_DATE_POLICIES,
        default="system-date",
        help="missing Trading date: substitute Date (D-25, default) or drop the event (D-10 alternative)",
    )
    p.add_argument(
        "--watermark-bound-ms",
        type=int,
        default=None,
        help="reject events a streaming run with this bound would treat as late (source-order model); default admit all",
    )
    p.add_argument(
        "--empty-windows",
        choices=windows.EMPTY_WINDOW_POLICIES,
        default="skip",
        help="windows without a price event: skip them (D-6, default) or carry the previous close forward (D-42)",
    )
    p.add_argument("--exclude-events", type=Path, default=None, help="CSV/Parquet of (sourceFile, sourceLine) to reject, e.g. the job's late events")
    p.add_argument("--no-input-checksums", dest="input_checksums", action="store_false", help="skip MD5 of the inputs")
    p.add_argument("--no-precision-check", dest="precision_check", action="store_false", help="skip the 50-digit decimal recomputation")
    args = p.parse_args(argv)

    cfg = Config(
        inputs=args.inputs,
        out=args.out,
        zone_offset=args.zone_offset,
        missing_trading_date=args.missing_trading_date,
        watermark_bound_ms=args.watermark_bound_ms,
        empty_windows=args.empty_windows,
        exclude_events=args.exclude_events,
        input_checksums=args.input_checksums,
        precision_check=args.precision_check,
        command=["python", "-m", "ema_oracle", *(argv if argv is not None else sys.argv[1:])],
    )
    manifest = run(cfg)
    summary = {k: manifest[k] for k in ("results", "precision", "timingsSeconds", "peakAnonMemoryMb")}
    summary["counts"] = manifest["counts"]["total"]
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
