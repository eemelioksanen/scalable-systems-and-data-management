"""Command line: python -m ema_check export|compare|dashboard|inject|latency ..."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from . import compare, dashboard, export, inject, latency


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="python -m ema_check", description=__doc__)
    sub = p.add_subparsers(dest="command", required=True)

    e = sub.add_parser("export", help="dump results, advisories and late events to Parquet")
    e.add_argument("--bootstrap-servers", default=os.environ.get("BOOTSTRAP_SERVERS", "localhost:29092"))
    e.add_argument("--ema-topic", default="ema-results")
    e.add_argument("--advisories-topic", default="advisories")
    e.add_argument("--late-topic", default="late-ticks")
    e.add_argument("--replay-report", type=Path, help="completed replay JSON mapping src file indices to filenames")
    e.add_argument("--zone-offset", required=True, help="the job's --zone-offset, recorded for the comparison (D-3)")
    e.add_argument("--ticks-topic", help="also export this topic's price events, for latency")
    e.add_argument("--stall-seconds", type=float, default=30.0, help="give up after this long without a record before the end offsets")
    e.add_argument("--out", type=Path, required=True)

    c = sub.add_parser("compare", help="diff an export against an oracle run (D-18)")
    c.add_argument("--oracle", type=Path, required=True, help="oracle output directory")
    c.add_argument("--job", type=Path, required=True, help="export directory")
    c.add_argument("--job-late-ticks", type=int, help="the job's lateTicks count at the end of the run")
    c.add_argument("--job-late-events", type=Path, help="CSV or Parquet with sourceFile, sourceLine, symbol and windowEnd")
    c.add_argument("--tolerance", type=float, default=compare.TOLERANCE, help="absolute EMA tolerance (D-18)")
    c.add_argument("--drift-ulps", type=float, default=compare.DRIFT_ULPS, help="EMA differences up to this many ulps count as precision drift")
    c.add_argument("--out", type=Path, required=True)

    d = sub.add_parser("dashboard", help="check a dashboard capture against the topics and the oracle (#13)")
    d.add_argument("--capture", type=Path, required=True, help="JSON from viz/web/scripts/capture.mjs")
    d.add_argument("--job", type=Path, required=True, help="export directory of the same run")
    d.add_argument("--oracle", type=Path, required=True, help="oracle output directory")
    d.add_argument("--expect-traders", type=Path, help="JSON of each trader's list as last written to subscriptions")
    d.add_argument("--tolerance", type=float, default=compare.TOLERANCE, help="absolute EMA tolerance (D-18)")
    d.add_argument("--out", type=Path, required=True)

    lt = sub.add_parser("latency", help="split a live run's latency into its waits (#37)")
    lt.add_argument("--export", type=Path, required=True, help="export directory with price_events.parquet")
    lt.add_argument("--replay-report", type=Path, required=True)
    lt.add_argument("--metrics", type=Path, help="the harness's metrics.json, for watermark lag")
    lt.add_argument("--bound-ms", type=int, required=True)
    lt.add_argument("--idleness-ms", type=int, required=True)
    lt.add_argument("--interval-start-ms", type=int, required=True)
    lt.add_argument("--interval-end-ms", type=int, required=True)
    lt.add_argument("--idle-from-ms", type=int, required=True, help="when the job's sources started, for partitions without events")
    lt.add_argument("--drain-start-ms", type=int, help="results emitted from here on were fired by the drain")
    lt.add_argument("--out", type=Path, required=True)

    i = sub.add_parser("inject", help="rewrite day files with bounded disorder and late events")
    i.add_argument("inputs", nargs="+", type=Path)
    i.add_argument("--disorder-ms", type=int, default=0, help="largest added watermark bound an event can need")
    i.add_argument("--late-rate", type=float, default=0.0, help="probability that a price event is made late")
    i.add_argument("--late-by-ms", type=int, default=0, help="a late event needs a bound above this to be admitted")
    i.add_argument("--seed", type=int, default=0)
    i.add_argument("--out", type=Path, required=True)

    args = p.parse_args(argv)
    if args.command == "export":
        result = export.export(args.bootstrap_servers, args.ema_topic, args.advisories_topic, args.out, args.zone_offset, args.stall_seconds, args.late_topic, args.replay_report, args.ticks_topic)
    elif args.command == "compare":
        result = compare.compare(compare.Inputs(
            oracle=args.oracle, job=args.job, out=args.out, job_late_ticks=args.job_late_ticks,
            job_late_events=args.job_late_events, tolerance=args.tolerance, drift_ulps=args.drift_ulps,
        ))
    elif args.command == "dashboard":
        result = dashboard.check(args.capture, args.job, args.oracle, args.out, args.expect_traders, args.tolerance)
        result = {k: v for k, v in result.items() if k != "publication"}
    elif args.command == "latency":
        result = latency.latency(args.export, args.replay_report, args.metrics, args.bound_ms, args.idleness_ms,
                                 (args.interval_start_ms, args.interval_end_ms), args.idle_from_ms,
                                 args.drain_start_ms, args.out)
    else:
        opts = inject.Options(disorder_ms=args.disorder_ms, late_rate=args.late_rate, late_by_ms=args.late_by_ms, seed=args.seed)
        result = inject.inject(args.inputs, args.out, opts)
    print(json.dumps(result, indent=2))
    return 0 if result.get("pass", True) else 1


if __name__ == "__main__":
    raise SystemExit(main())
