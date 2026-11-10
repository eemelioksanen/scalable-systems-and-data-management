# Dashboard end-to-end check (#13), 2026-11-10

The check passed. What the dashboard shows equals what the job published and what the oracle computes. This holds for every heatmap cell, chart point, marker, feed row, shown time and per-trader feed, including through a mid-run subscription change. The procedure and the list of checks are in [viz/README.md](../../../viz/README.md#end-to-end-check). D-41 records the design.

## Run

- Source revision `d230b86` plus the uncommitted changes of the #13 branch. Docker Compose stack, fresh topics, one TaskManager, 8 `ticks` partitions.
- Inputs: Monday and Tuesday day files, 11 symbols, Monday unpaced and Tuesday from 09:00 at 120×. Replay `COMPLETE`, 6,494,792 records ([replay.json](replay.json)).
- Live job with its defaults: 1 s watermark bound, 30 s idleness. It was stopped with `flink stop --drain` after the replay. It rejected no events (`late-ticks` is empty, [export-manifest.json](export-manifest.json)).
- Reference: the oracle on both day files (`oracle/out/mon-tue`, 5,016 advisories over all symbols), not committed (gitignored, regenerated in about 40 s).
- Command: `scripts/validation/dashboard/run.sh` ([run.log](run.log)).

## Subscriptions

| Trader | Before the job | At feed time Tue 12:00 |
|---|---|---|
| trader-a | ASML.NL, RDSA.NL, EUS5D.NL | unchanged |
| trader-b | AIR.FR, INVDS.FR, A1EXV9.ETR | adds IPLU3.FR |
| trader-x | ISAMD.FR, A1CX3T.ETR | tombstone |
| trader-c | – | I3BP.NL, A0S3AS.ETR, ISAMD.FR |

The effects are visible in the outputs:

- **A1CX3T.ETR** was published from 09:10 for 36 windows, then stopped. Its oracle SELL at 16:55 is absent from both the topic and the dashboard.
- **IPLU3.FR**, **I3BP.NL** and **A0S3AS.ETR** were first published at 12:05 and 12:10. IPLU3.FR's SELL at 15:40 and BUY at 16:55 are shown with the oracle's EMAs. They are measured against Monday's unpublished history, so its chart has no first-window marker.
- **ISAMD.FR** stayed published, by trader-x and then by trader-c, and shows both its 11:05 SELL and its 16:05 BUY.

## Result

The span is Mon 22:30 to Tue 22:30. Monday's first-window BUYs left it, which is why the topic's 18 advisories show as 10.

| Capture | Symbols | Cells | Advisories | Charted | Checks failed |
|---|---:|---:|---:|---:|---:|
| [live](check-live.json): connected through the run, state from deltas | 11 | 1,208 | 10 | 1 | 0 of 14 |
| [final](check-final.json): fresh snapshot after the drain | 11 | 1,208 | 10 | 11 | 0 of 14 |

The shown advisories, newest first: A0S3AS.ETR SELL 21:45, A1EXV9.ETR SELL 20:45, EUS5D.NL SELL 20:30, IPLU3.FR BUY 16:55, I3BP.NL SELL 16:35, INVDS.FR BUY 16:10, ISAMD.FR BUY 16:05, IPLU3.FR SELL 15:40, INVDS.FR SELL 13:20, ISAMD.FR SELL 11:05. Each check report lists, per symbol, the first window shown.

The captures themselves are [capture-live.json](capture-live.json) and [capture-final.json](capture-final.json); [traders.json](traders.json) is the lists last written, and [health.json](health.json) the backend's counts at the end.

## Figures

[`report/figures/`](../../../report/figures/) holds `dashboard-overview.png` (INVDS.FR) and `dashboard-trader.png` (`?trader=trader-b`, IPLU3.FR). They were captured from the same stack after the check with `scripts/validation/dashboard/figures.sh`. The viz container was rebuilt between the check and the figures, for marker size and the first-window label, and re-read the same topics. Its feed time then comes from a fresh sample of the tail of `ticks`, so the trader figure's clock shows a feed time of 17:45. D-33 already notes that the feed time is meaningful only during a live replay.

## Limits

- One run, 11 symbols, on one machine. It checks correctness of the path, not dashboard load at all 4,840 symbols. The full-symbol heatmap and snapshot sizes are the D-33 design figures, not measured here.
- The check reads the frontend reducer's state, which the canvases draw, not the pixels. The figures were inspected by eye.
- The subscription filter uses each trader's current list (D-41), so the per-trader check compares against the lists last written.
