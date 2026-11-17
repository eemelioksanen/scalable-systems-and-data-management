# Dashboard

A live view of what the Flink job publishes to `ema-results` and `advisories` (D-33, D-41, D-43): a watchlist with each symbol's 24-hour trend, a one-symbol chart with BUY/SELL markers, the EMA spread and market breadth, the advisories and a replay clock. It reads published results only and recomputes nothing: the markers and the feed are the `advisories` records, not crossovers derived from the EMAs. Operational metrics stay in Prometheus. The design is in [the spec](../docs/superpowers/specs/2026-10-11-dashboard-ema-results-design.md), with the additions of #13 in D-41 and the terminal layout of D-43.

## Views

A three-column terminal (D-43): the watchlist on the left, the selected symbol in the centre, the signals on the right, under a top bar and a ticker.

- **Top bar**: *results as of*, the newest published `windowEnd`, which is the time the views describe; a status; the feed time and wall-clock time, smaller; then the breadth at the newest window in which a listed symbol has a result, the BUY and SELL counts and median latency of the listed signals, the zone offset and the connection. The status is *window open* (until the next window's end) while the feed is within one window of the results, *waiting for the next window* once it is further ahead, and *feed idle* when no ticks arrive. No gap between feed and results is shown in minutes: a window is published only once the next one has started (spec §3.1 (7)) and the watermark has passed it, and a gap in simulated minutes scales with the replay rate, so 30 s of watermark wait reads as an hour at 120×. Watermark lag is an operational metric, queried from Prometheus (D-35, D-40).
- **Ticker**: the watchlist's first 40 symbols with their newest close and EMA spread. Clicking one charts it.
- **Filters** at the top of the watchlist apply to the watchlist, the ticker, breadth and the signals: exchange (FR, NL, ETR), SecType (equity, index) and subscription (a trader's current list from `subscriptions`). The first-window toggle sits on the signals. They are kept in the query string, e.g. `/?exchange=NL&trader=trader-a&sort=flip&first=1#RDSA.NL`, so a view can be linked or captured.
- **First-window BUYs** are hidden by default. Every symbol's first window gives a BUY only because both EMAs start at 0 (D-7): 4,970 of the week's 11,355 advisories, and every one of Monday's. They are published and correct, but no breakout, and they would otherwise bury the crossovers. They are recognised from the record alone: a first window has EMA38 = close·2/39 and EMA100 = close·2/101, so EMA38 / EMA100 = 101/39 whatever the close. On the oracle's week output this rule picks exactly the 4,970 first windows and none of the other 6,385 advisories. The chart always draws them, hollow, with the first-window note; the signals and the crossover sort skip them unless shown.
- **Watchlist** (left): one row per symbol, with its latest advisory (or latest flip under that sort), a trend strip of the last 288 windows (green where EMA38 > EMA100, red below, published advisories solid), its newest close and its EMA spread with BULL or BEAR. Sorted by latest crossover (the newest published advisory, default), latest sign flip, or symbol. Only the rows in view are rendered.
- **Symbol** (centre): the newest window's close, EMA spread and EMAs, or the hovered window's; then three charts on the span's time axis, the last 288 windows. *Price*: close, EMA38 and EMA100, with ▲ BUY and ▼ SELL at each advisory's EMA38 and window end. *EMA spread*: EMA38 − EMA100 as a percentage of EMA100, so a crossover is a change of sign. A first-window BUY needs none (D-7), and an advisory decided on a window that is not shown, before the span or the subscription, can sit at the left edge without one. *Market breadth*: per window, the share of the listed symbols with EMA38 above EMA100, counting only symbols with a window there.
- **Signals** (right): every advisory in the span, newest first, with its window end at the job's zone offset, side, symbol, SecType, both EMAs to four decimals and its latency: wall-clock time from admitting the window's last trade to the job emitting the advisory (`emitTs − maxIngestTs`, D-40). It stops before the Kafka sink, so it leaves out D-40's `sinkHandoff`. It renders the newest 400 rows that pass the filters; a new one flashes once. Clicking a row charts the symbol. The latency p50, p95 and maximum of the listed advisories sit under it.

A subscription filter uses the trader's **current** list: the topic is compacted, so the latest list per trader is all a restarted reader can rebuild, as the job does too. It therefore also shows a symbol's advisories from before the trader subscribed, if another trader's subscription had them published. The job publishes per symbol, not per trader (D-17); this filter is the per-trader view of that output.

## Running

`docker compose up` starts it at http://localhost:8088 (`VIZ_HOST_PORT` changes the host port). The job publishes only subscribed symbols, so write a subscription before submitting the job (see the main README).

`GET /health` reports how many results, advisories and subscription records it has read, how many were malformed or older than the retained span, the advisories held, the number of traders, symbols and open browser sessions, and both clock values.

| Option | Default | Meaning |
|---|---|---|
| `--bootstrap-servers` | `$BOOTSTRAP_SERVERS`, else `localhost:29092` | Kafka |
| `--port` | 8088 | HTTP and WebSocket port |
| `--zone-offset` | `+02:00` | The job's `--zone-offset` (D-3), used to show times. The two must match. |
| `--results-topic` | `ema-results` | |
| `--advisories-topic` | `advisories` | |
| `--subscriptions-topic` | `subscriptions` | Read for the subscription filter |
| `--ticks-topic` | `ticks` | Read for the replay clock |
| `--flush-ms` | 250 | How often browsers get updates |

## What it keeps

The last 288 five-minute windows of every published symbol, ending at the newest `windowEnd` seen in `ema-results`. A result older than that is dropped and counted. A result for a window already held replaces it, so a job rerun without a topic reset is harmless. Advisories follow the same span and the same replacement by (`symbol`, `windowEnd`); the span moves with `ema-results` only, so an advisory read before its result is kept. The browser keeps the heatmap as one sign per symbol and slot, the full series of the selected symbol only, and every advisory in the span: the whole week has 11,355, so this is small.

The traders' lists are kept whole, as the latest record per trader; a tombstone removes the trader, and a malformed record is counted and ignored, by the job's rules (D-17).

The feed time is the minimum `Date`+`Time` among the rows added to the tail of `ticks` since the last look (at most 32 per partition, once a second), and it never moves backward: the replayer's source-clock rule, which ignores the stale times of each day file's opening snapshot. It does not go back after `ticks` is reset either; restart the dashboard for a new replay.

## WebSocket protocol

Endpoint `/ws`. Every message is a JSON object with a `type`. All sends happen on one thread, every `--flush-ms`; a session with nothing new gets nothing.

```text
client → {"type":"select","symbol":"RDSA.NL"}

server → {"type":"snapshot","spanEnd":1636464900000,"slots":288,"slotMs":300000,
          "symbols":[{"symbol":"RDSA.NL","secType":"E","exchange":"NL","lastFlip":1636464600000,"cells":"++--…",
                     "last":[1636464900000,19.24,19.21,19.23]}],
          "series":{…}|null,
          "advisories":[["RDSA.NL",1636464600000,"SELL","E","NL",19.21,19.23],…],
          "traders":{"trader-a":["ASML.NL","RDSA.NL"],"all":["*"]},
          "clock":{…}}
server → {"type":"series","series":{"symbol":"RDSA.NL","firstWindowEnd":1636355100000,"points":[[windowEnd,close,ema38,ema100],…]}}
server → {"type":"delta","spanEnd":…,
          "cells":[["RDSA.NL",1636464900000,"+","E","NL",1636464900000,19.24,19.21,19.23]],
          "points":{"symbol":"RDSA.NL","points":[[windowEnd,close,ema38,ema100],…]}|null,
          "advisories":[…], "traders":{…}|null,
          "clock":{…}}

clock = {"wallMs":…,"feedMs":…|null,"feedIdleSince":…|null,"resultsMs":…|null,"zoneOffset":"+02:00"}
```

- `cells` in a snapshot is 288 characters, oldest slot first: `+` EMA38 > EMA100, `-` below, `0` equal, `.` no window. A delta cell is `[symbol, windowEnd, sign, secType, exchange, lastFlip, close, ema38, ema100]`. A snapshot row's `last` is its newest retained window as `[windowEnd, close, ema38, ema100]`, or null; the client keeps the newer of that and each delta cell.
- `lastFlip` is the latest retained window whose sign differs from the window before it; a symbol's first window counts as a flip from 0 (D-7). It is null once that flip has left the span.
- `points` in a delta are the changed windows of the session's selected symbol; the client merges them by `windowEnd`.
- An advisory is `[symbol, windowEnd, type, secType, exchange, ema38, ema100, maxIngestTs, emitTs]`; the last two may be null. A snapshot carries every advisory in the span, oldest first; a delta carries the new or replaced ones, which the client merges by (`symbol`, `windowEnd`). Both sides drop advisories that leave the span.
- `traders` maps each trader to their current list, `["*"]` for every symbol. A delta carries all of it when it changed since the session's last message, else null.
- On connect the server sends a snapshot; the client then sends `select` and gets a `series`. A reconnect always starts from a new snapshot, and missed deltas are never replayed.
- A browser more than 4 MB of messages behind is disconnected and reconnects to a fresh snapshot.

## Development

```bash
scripts/mvn.sh -pl producer,viz -am test     # backend tests
cd viz/web && npm ci && npm test             # frontend tests (Vitest)
cd viz/web && npm run dev                    # Vite dev server, proxying /ws and /health to localhost:8088
```

Set `VIZ_BACKEND=host:port` for the dev server to use another backend. The Docker build runs the frontend tests, builds it, and serves it from the jar.

## End-to-end check

[`scripts/validation/dashboard/run.sh`](../scripts/validation/dashboard/run.sh) runs the whole path, replay → Kafka → Flink → output topics → WebSocket → the frontend's reducer, and checks what the views show. On a fresh stack, with the oracle's output for Monday and Tuesday:

```bash
docker compose down --volumes && docker compose up -d --build && docker compose build producer
oracle/run.sh data/daily/debs2022-gc-trading-day-08-11-21.csv data/daily/debs2022-gc-trading-day-09-11-21.csv --out /out/mon-tue
scripts/validation/dashboard/run.sh
```

A first day alone has almost only each symbol's first-window BUY, since crossovers need converged EMAs. So the script replays 13 symbols: 11 that cross on Tuesday, and two whose first trade is on Tuesday, so that a first-window BUY stays in the span. Monday is sent unpaced and Tuesday paced from 09:00 at 120× (`RATE`). Three traders subscribe before the job starts. At a feed time of Tuesday 12:00 the script changes them: trader-b adds IPLU3.FR, which crosses at 15:40 and 16:55; trader-x is removed with a tombstone, which stops A1CX3T.ETR before its 16:55 SELL; and a new trader-c keeps ISAMD.FR published and adds two symbols never published before. After the replay it drains the job, exports the topics, and captures the dashboard twice with [`web/scripts/capture.mjs`](web/scripts/capture.mjs). The capture connects like a browser and applies every message with `src/state.js`. The first capture stays connected through the whole run, so its state is built from deltas. The second connects fresh at the end, so its state comes from a snapshot, and charts every symbol. `correctness/run.sh dashboard` then checks each capture against the export and the oracle:

| Check | Holds when |
|---|---|
| `heatmapCellsEqualTopic`, `heatmapCellsEqualOracle` | Every published window in the span has a cell, no other does, and its sign is the topic's and the oracle's. |
| `chartPointsEqualTopic`, `chartPointsEqualOracle` | Every charted point equals the published record, with the oracle's close and EMAs within 1e-9. |
| `feedEqualsTopic` | The feed is exactly the published advisories in the span, with their EMAs. |
| `feedEqualsOracleForShownWindows` | It is also exactly the oracle's advisories for the windows shown, so a symbol subscribed mid-run gets its first crossover measured against its unpublished history. |
| `feedTimesAtZoneOffset`, `feedNewestFirst` | The shown times are `windowEnd` at the job's offset, newest first. |
| `chartMarkersEqualFeed` | Each chart's markers are its symbol's advisories. |
| `feedPerTraderFollowsCurrentList`, `tradersEqualSubscriptionsWritten` | Each trader's feed is the advisories of their current list, and the lists equal what was last written. |
| `firstWindowFlagEqualsOracle`, `defaultFeedHidesOnlyFirstWindowBuys` | An advisory is marked first-window exactly when it is at the oracle's first window of its symbol, and the default feed leaves out those and nothing else. |
| `feedLatencyEqualsTopic` | Each latency shown is the record's `emitTs − maxIngestTs`. |
| `heatmapSortedByCrossover`, `clockLatestWindow` | The heatmap order (first-window BUYs not counting) and the results-as-of time. |

The report also lists, per symbol, the first window shown and how many oracle advisories came before it. [The recorded run](../docs/validation/2026-11-10-dashboard/README.md) passed every check.

## Figures

[`scripts/validation/dashboard/figures.sh`](../scripts/validation/dashboard/figures.sh) captures the running dashboard with headless Chromium into `report/figures/`, at fixed views given by the query string and hash. It needs a Chromium or Chrome on the host (`CHROME`, default `chromium`), unlike everything else here.
