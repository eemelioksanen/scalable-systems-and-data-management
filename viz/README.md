# Dashboard

A live view of what the Flink job publishes to `ema-results` and `advisories` (D-33, D-41): a trend heatmap, a one-symbol chart with BUY/SELL markers, an advisory feed and a replay clock. It reads published results only and recomputes nothing: the markers and the feed are the `advisories` records, not crossovers derived from the EMAs. Operational metrics stay in Grafana. The design is in [the spec](../docs/superpowers/specs/2026-10-11-dashboard-ema-results-design.md), with the additions of #13 in D-41.

## Views

- **Filters** across the top apply to the heatmap and the feed: exchange (FR, NL, ETR), SecType (equity, index) and subscription, a trader's current list from `subscriptions`. They are kept in the query string, e.g. `/?exchange=NL&trader=trader-a&sort=flip#RDSA.NL`, so a view can be linked or captured.
- **Trend heatmap** (left): symbols × the last 288 windows, blue where EMA38 > EMA100 and orange below. A published advisory's window is drawn in the darker line color, so crossovers stand out. Sorted by latest crossover (the newest published advisory, default), latest sign flip, or symbol. The right column shows the latest advisory, or the latest flip under that sort.
- **Symbol chart** (top right): close, EMA38 and EMA100, with ▲ BUY and ▼ SELL markers at the advisory's EMA38 and window end.
- **Advisory feed** (bottom right): every advisory in the span, newest first, with its window end at the job's zone offset, the symbol, SecType, exchange and both EMAs. It renders the newest 400 rows that pass the filters. Clicking a row charts the symbol.
- **Replay clock** (top bar): feed time, latest window and wall-clock time.

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
          "symbols":[{"symbol":"RDSA.NL","secType":"E","exchange":"NL","lastFlip":1636464600000,"cells":"++--…"}],
          "series":{…}|null,
          "advisories":[["RDSA.NL",1636464600000,"SELL","E","NL",19.21,19.23],…],
          "traders":{"trader-a":["ASML.NL","RDSA.NL"],"all":["*"]},
          "clock":{…}}
server → {"type":"series","series":{"symbol":"RDSA.NL","firstWindowEnd":1636355100000,"points":[[windowEnd,close,ema38,ema100],…]}}
server → {"type":"delta","spanEnd":…,
          "cells":[["RDSA.NL",1636464900000,"+","E","NL",1636464900000]],
          "points":{"symbol":"RDSA.NL","points":[[windowEnd,close,ema38,ema100],…]}|null,
          "advisories":[…], "traders":{…}|null,
          "clock":{…}}

clock = {"wallMs":…,"feedMs":…|null,"feedIdleSince":…|null,"resultsMs":…|null,"zoneOffset":"+02:00"}
```

- `cells` in a snapshot is 288 characters, oldest slot first: `+` EMA38 > EMA100, `-` below, `0` equal, `.` no window. A delta cell is `[symbol, windowEnd, sign, secType, exchange, lastFlip]`.
- `lastFlip` is the latest retained window whose sign differs from the window before it; a symbol's first window counts as a flip from 0 (D-7). It is null once that flip has left the span.
- `points` in a delta are the changed windows of the session's selected symbol; the client merges them by `windowEnd`.
- An advisory is `[symbol, windowEnd, type, secType, exchange, ema38, ema100]`. A snapshot carries every advisory in the span, oldest first; a delta carries the new or replaced ones, which the client merges by (`symbol`, `windowEnd`). Both sides drop advisories that leave the span.
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

A first day alone has almost only each symbol's first-window BUY, since crossovers need converged EMAs. So the script replays 11 symbols, chosen because they cross on Tuesday, with Monday unpaced and Tuesday from 09:00 at 120× (`RATE`). Three traders subscribe before the job starts. At a feed time of Tuesday 12:00 the script changes them: trader-b adds IPLU3.FR, which crosses at 15:40 and 16:55; trader-x is removed with a tombstone, which stops A1CX3T.ETR before its 16:55 SELL; and a new trader-c keeps ISAMD.FR published and adds two symbols never published before. After the replay it drains the job, exports the topics, and captures the dashboard twice with [`web/scripts/capture.mjs`](web/scripts/capture.mjs). The capture connects like a browser and applies every message with `src/state.js`. The first capture stays connected through the whole run, so its state is built from deltas. The second connects fresh at the end, so its state comes from a snapshot, and charts every symbol. `correctness/run.sh dashboard` then checks each capture against the export and the oracle:

| Check | Holds when |
|---|---|
| `heatmapCellsEqualTopic`, `heatmapCellsEqualOracle` | Every published window in the span has a cell, no other does, and its sign is the topic's and the oracle's. |
| `chartPointsEqualTopic`, `chartPointsEqualOracle` | Every charted point equals the published record, with the oracle's close and EMAs within 1e-9. |
| `feedEqualsTopic` | The feed is exactly the published advisories in the span, with their EMAs. |
| `feedEqualsOracleForShownWindows` | It is also exactly the oracle's advisories for the windows shown, so a symbol subscribed mid-run gets its first crossover measured against its unpublished history. |
| `feedTimesAtZoneOffset`, `feedNewestFirst` | The shown times are `windowEnd` at the job's offset, newest first. |
| `chartMarkersEqualFeed` | Each chart's markers are its symbol's advisories. |
| `feedPerTraderFollowsCurrentList`, `tradersEqualSubscriptionsWritten` | Each trader's feed is the advisories of their current list, and the lists equal what was last written. |
| `heatmapSortedByCrossover`, `clockLatestWindow` | The heatmap order and the latest-window clock. |

The report also lists, per symbol, the first window shown and how many oracle advisories came before it. [The recorded run](../docs/validation/2026-11-10-dashboard/README.md) passed every check.

## Figures

[`scripts/validation/dashboard/figures.sh`](../scripts/validation/dashboard/figures.sh) captures the running dashboard with headless Chromium into `report/figures/`, at fixed views given by the query string and hash. It needs a Chromium or Chrome on the host (`CHROME`, default `chromium`), unlike everything else here.
