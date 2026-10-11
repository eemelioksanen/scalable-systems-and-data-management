# Dashboard over `ema-results`: design

Issue: [#26](https://github.com/eemelioksanen/scalable-systems-and-data-management/issues/26). Roadmap §5 (Smart visualization) and "Verification: local smoke test". Formats: [interfaces.md](../../interfaces.md).

## Goal

A local web dashboard that shows, live, what the Query 1 job publishes to `ema-results`: a one-symbol chart, a market-wide trend heatmap and a replay clock. It reads published results only and never recomputes EMAs. It must stay within bounded memory in the backend and the browser through a full-week replay, and leave room for #13 to add `advisories`.

Targets: one-symbol chart by 25.10.2026; heatmap and replay clock by 1.11.2026.

Out of scope: `advisories`, the advisory feed and per-trader delivery (#13); operational metrics (Grafana); cloud deployment.

## Decisions taken in brainstorming

| Topic | Decision |
|---|---|
| Backend | New Maven module `viz/`: Java 17, `kafka-clients`, Jackson, Javalin 6 (HTTP + WebSocket). One shaded jar. |
| Frontend | React + Vite in `viz/web/`, built in a Node stage of the `Dockerfile` and served as static files by the backend. uPlot for the chart; the heatmap is a custom canvas. |
| Architecture | One process: a shared in-memory store, fed by one Kafka consumer, pushed to browsers as coalesced deltas (approach A). Kafka Streams and per-browser consumers were rejected. |
| History | The last 288 five-minute windows (24 h of simulated time) per symbol. |
| Replay clock | Both the feed time (tail of `ticks`) and the latest evaluated window (`ema-results`), next to wall-clock time. |
| Heatmap rows | All published symbols, virtualized, sorted by most recent sign flip, filterable by exchange and SecType. |
| Layout | Mockup A, "side by side": heatmap on the left as the navigator, chart on the right, clock bar across the top. |
| Visual style | Light mode, white background, near-black text (`#111418`), compact. Blue `#2563eb` = EMA38 above EMA100, orange `#d97706` = below, the same pair in the chart lines and the heatmap. No decorative animation; the only repaint not driven by data is the wall clock, once a second. |

These are recorded as **D-33** in [decisions.md](../../decisions.md), status *proposed*.

## Architecture

```
ema-results ──► ResultsConsumer ──┐
                                  ├──► Store ──► Broadcaster ──(every 250 ms)──► WebSocket sessions ──► React app
ticks (tail) ─► FeedClock ────────┘                    ▲
                                                       └── select(symbol) from each session
```

All four backend components live in one JVM process, `viz`, which also serves the built frontend and `/health`.

### ResultsConsumer

- One thread, one `KafkaConsumer<String,String>` on `ema-results`. No consumer group: it assigns itself every partition and seeks to the earliest offset at startup, so a restarted backend rebuilds its state from the topic.
- Parses each value into `Result(symbol, secType, windowEnd, close, ema38, ema100)`. `maxIngestTs` and `emitTs` are ignored. A record that fails to parse, or lacks one of these fields, increments `malformedResults` and is skipped.
- Hands each `Result` to the store. It depends on Kafka only through the `Consumer` interface, so tests use the client's `MockConsumer`.

### Store

The single source of truth, guarded by one lock (writes come from two threads, reads from the broadcaster).

- **Span.** `latestWindowEnd` is the highest `windowEnd` seen. The retained span is the 288 slots ending there: `(latestWindowEnd − 288 × 300 000, latestWindowEnd]`. A result older than the span is dropped and counted as `staleResults`. When `latestWindowEnd` advances, entries that fall out of the span are evicted. Memory is therefore bounded by symbols × 288, about 50 MB for all ~5,500 symbols with primitive arrays.
- **Per symbol.** `secType`, the exchange (the `ID` suffix, D-1), and the retained windows sorted by `windowEnd`, each with `close`, `ema38`, `ema100` and `sign = signum(ema38 − ema100)` (−1, 0 or +1).
- **Duplicates.** A result for a (`symbol`, `windowEnd`) already held replaces it in place. This makes a job rerun without a topic reset (interfaces.md, Restart and replay) harmless.
- **Out of order.** A result inside the span but older than the symbol's newest window is inserted in order.
- **Flip.** `lastFlip` is the `windowEnd` of the latest retained window whose sign differs from the previous retained window's sign. A symbol's first-ever window counts as a flip from 0. Because both EMAs start at 0 (D-7), that window has EMA38 > EMA100 and is the BUY that every symbol gets. If no retained window is a flip, `lastFlip` is null and the symbol sorts after every symbol that has one, alphabetically.
- **First window.** The store records each symbol's first `windowEnd` seen since startup, so the chart can mark the zero-initialized BUY. This is exact when the backend has read the topic from its first record, which it always does.
- **Change tracking.** Every mutation appends (`symbol`, `windowEnd`) to a pending-changes set that the broadcaster drains.

### FeedClock

- Once a second, for each `ticks` partition, reads the records after the last offset it has seen, at most the final 32 per partition (256 in total with 8 partitions). It uses its own consumer with no group.
- Feeds the new rows, ordered by record timestamp, into the replayer's `SourceClock` (made public in `producer/`; `viz` depends on the `producer` module). That is the same rule that paces live replay: the minimum `Date`+`Time` over the last 256 rows, never moving backward, which ignores the stale opening snapshot of each day file ([replayer.md](../../replayer.md#the-source-clock)).
- `SourceClock` reads the local fields as UTC epoch ms. `feedMs` subtracts the zone offset so it is a true epoch value comparable with `windowEnd`.
- If no new rows arrive, `feedMs` holds and `feedIdleSince` records when it stopped moving.
- After a preload, the topic already holds the whole input, so `feedMs` shows the end of the input as soon as the backend starts. Feed time is only meaningful during a live replay.

### Broadcaster

- Every 250 ms, drains the store's pending changes and sends each open session one `delta` containing the heatmap cells that changed, the series points for that session's selected symbol only, and the clock. Sessions with nothing new get nothing.
- A session whose outgoing buffer exceeds 4 MB is closed. The browser reconnects and receives a fresh snapshot, so a slow browser cannot make the backend buffer grow.

## WebSocket protocol

Endpoint `/ws`. Every message is a JSON object with a `type`.

```text
client → {"type":"select","symbol":"RDSA.NL"}

server → {"type":"snapshot",
          "spanEnd":1636464900000, "slots":288, "slotMs":300000,
          "symbols":[{"symbol":"RDSA.NL","secType":"E","exchange":"NL","lastFlip":1636464600000,
                      "cells":"++++--…"}],          // 288 chars, oldest first: '+' '-' '0' or '.' (no window)
          "series":{"symbol":"RDSA.NL","firstWindowEnd":1636355100000,
                    "points":[[windowEnd,close,ema38,ema100],…]} | null,
          "clock":{…}}

server → {"type":"series", "series":{…}}            // reply to select

server → {"type":"delta", "spanEnd":…,
          "cells":[["RDSA.NL",1636464900000,"+","E","NL",1636464900000]],  // symbol, windowEnd, sign, secType, exchange, lastFlip
          "points":[[windowEnd,close,ema38,ema100],…],   // selected symbol only
          "clock":{…}}

clock = {"wallMs":…, "feedMs":…|null, "feedIdleSince":…|null, "resultsMs":…|null, "zoneOffset":"+02:00"}
```

- On connect the server sends a `snapshot` with `series: null`. The client then sends `select`.
- `cells` as a 288-character string keeps a full snapshot of ~5,500 symbols near 1.6 MB.
- There is no replay of missed deltas. A reconnect always starts from a new snapshot.
- #13 extends this by adding an `advisories` field to `series` and `delta`; existing fields do not change.

## Frontend

React app in `viz/web/`, layout A.

- **`useDashboardSocket`** owns the connection, reconnects with exponential backoff (1 s doubling to 30 s), and holds the state in a reducer: a snapshot replaces it, a delta patches it, a series replaces the selected series. It sends `select` on connect and whenever the selection changes.
- **Heatmap state** is one `Int8Array` of symbols × 288, indexed by `slot = (windowEnd / 300000) mod 288`, so advancing `spanEnd` only clears the slots that left the span. This is about 1.6 MB for 5,500 symbols. The selected symbol's series is the only other history the browser holds.
- **ClockBar.** Feed time, latest window with the gap to the feed time ("results 2 min behind feed"), wall-clock time, the zone offset pill ("UTC+02:00, the job's `--zone-offset`"), and a connection pill: live, reconnecting, or "waiting for Kafka". When the feed has been idle for over 10 s it says "feed idle".
- **Heatmap (left).** A canvas with virtual scrolling that draws only the visible rows. Cell colors: blue = EMA38 > EMA100, orange = below, light grey = no window in that slot, mid grey = equal. Rows are sorted by `lastFlip`, newest first. Exchange (FR, NL, ETR) and SecType (E, I) chips filter them. Clicking a row selects that symbol. A legend sits under the axis.
- **SymbolChart (right).** uPlot with close (near-black, 1 px), EMA38 (blue) and EMA100 (orange) against `windowEnd` at the zone offset. A step of more than one slot between consecutive points is drawn as a gap, not interpolated, because empty windows are skipped (D-6). A dashed marker on `firstWindowEnd` reads "first window: BUY from EMA₀ = 0". The header shows the symbol, SecType, exchange and "n of 288 windows with trades". The footnote says the price currency is not published (`ticks` carries 7 of 39 columns, D-24, and the outputs carry none, D-16) and that the time axis is the exclusive window end.
- **Selection.** Until the viewer picks one, the top row of the sorted heatmap is selected. The selection is kept in the URL hash.

### Empty and sparse states

| State | Shown |
|---|---|
| No results received | A full-panel message: no results yet. The job publishes only subscribed symbols and holds results until it has read `subscriptions`, so write a subscription before submitting the job. Shows the README's `["*"]` command. |
| Selected symbol has one window | Points instead of lines, plus the first-window note. |
| Selected symbol left the span | "No windows in the last 24 h" with the symbol kept selected. |
| Disconnected | Connection pill reads "reconnecting"; the last data stays visible, dimmed. |
| Kafka unreachable | `feedMs` and `resultsMs` are null; the clock bar says "waiting for Kafka". |

## Error handling

- The Kafka client retries connection failures on its own. Neither consumer thread exits on a transient error; an unexpected exception is logged and the loop continues after one second.
- A missing topic is logged once and retried every 5 s.
- `GET /health` returns `{"resultsRead","malformedResults","staleResults","symbols","sessions","feedMs","resultsMs"}`.

## Packaging and configuration

- **Maven.** `viz` joins the root `<modules>`. Javalin is added to the parent's `dependencyManagement`. The shade plugin builds `viz/target/viz.jar` with main class `fi.aalto.cse4780.viz.Dashboard`.
- **Dockerfile.** A `viz-web` stage (Node) runs `npm ci`, `npm test` and `npm run build`. The `build` stage copies its output to `viz/src/main/resources/static/` before `mvn package`, so the jar serves it. That path is gitignored. A `viz` runtime stage runs the jar on `eclipse-temurin:17-jre` with `-Xmx512m`.
- **Compose.** Service `viz`, host port `${VIZ_HOST_PORT:-8088}`, `depends_on: kafka-init` (completed), `BOOTSTRAP_SERVERS=kafka:9092`. Started by the default `docker compose up`.
- **Flags.** `--bootstrap-servers` (env `BOOTSTRAP_SERVERS`), `--port` (8088), `--zone-offset` (`+02:00`, must equal the job's), `--results-topic` (`ema-results`), `--ticks-topic` (`ticks`). The history depth is fixed at 288.
- **Local development.** `npm run dev` in `viz/web/` with Vite proxying `/ws` and `/health` to `localhost:8088`.

## Testing

No test needs Docker or a broker.

- **Store:** span eviction when `latestWindowEnd` advances; dropping results older than the span; in-place replacement of a duplicate; in-order insertion of an out-of-order result; sign including equal EMAs; `lastFlip`, including the first window; first-window tracking; the pending-changes set.
- **Result parsing:** the interfaces.md example record; missing fields; non-JSON; extra fields ignored.
- **FeedClock:** stale opening-snapshot rows do not move it, it never goes backward, only unseen offsets are fed, the zone offset is applied, and idle detection works.
- **ResultsConsumer:** with `MockConsumer`, records reach the store and a malformed record is counted.
- **Broadcaster:** a session receives only changes since its last flush and only its selected symbol's points; an idle session receives nothing.
- **WebSocket (Javalin test tools):** connect → snapshot; `select` → series; a store change → delta; reconnect → fresh snapshot.
- **Frontend (Vitest):** reducer handling of snapshot, delta, series and span advance; slot indexing; gap detection; heatmap sort and filters.

`mvn verify` runs the Java tests; the `viz-web` Docker stage runs Vitest.

## Smoke test

The roadmap's local smoke test, limited to Query 1, recorded under `docs/validation/` like the existing evidence:

1. `docker compose up -d --build`
2. Write the `["*"]` subscription (README command).
3. Submit the job.
4. Replay a subset live: `--mode realtime --rate 10 --pace-from 2021-11-08T09:00 --symbols <a dozen symbols>`.
5. `curl localhost:8088/health` shows `resultsRead` growing, then the dashboard at `http://localhost:8088` fills: the chart updates every five simulated minutes, the heatmap gains columns, and the feed time runs ahead of the latest window.

## Documentation

- `viz/README.md`: running, flags, the WebSocket protocol, development workflow.
- README: a Dashboard section and the `viz` row in the UI table.
- decisions.md: D-33 (proposed) with the table above, including that currency is not labelled because no topic carries it.
- interfaces.md: `viz` is already listed as a reader of `ema-results`; add that it also reads the tail of `ticks` for the replay clock.
