# Dashboard

A live view of what the Flink job publishes to `ema-results` (D-33): a trend heatmap, a one-symbol chart and a replay clock. It reads published results only and recomputes nothing. Operational metrics stay in Grafana. The design is in [the spec](../docs/superpowers/specs/2026-10-11-dashboard-ema-results-design.md).

## Running

`docker compose up` starts it at http://localhost:8088 (`VIZ_HOST_PORT` changes the host port). The job publishes only subscribed symbols, so write a subscription before submitting the job (see the main README).

`GET /health` reports how many results it has read, how many were malformed or older than the retained span, the number of symbols and open browser sessions, and both clock values.

| Option | Default | Meaning |
|---|---|---|
| `--bootstrap-servers` | `$BOOTSTRAP_SERVERS`, else `localhost:29092` | Kafka |
| `--port` | 8088 | HTTP and WebSocket port |
| `--zone-offset` | `+02:00` | The job's `--zone-offset` (D-3), used to show times. The two must match. |
| `--results-topic` | `ema-results` | |
| `--ticks-topic` | `ticks` | Read for the replay clock |
| `--flush-ms` | 250 | How often browsers get updates |

## What it keeps

The last 288 five-minute windows of every published symbol, ending at the newest `windowEnd` seen. A result older than that is dropped and counted. A result for a window already held replaces it, so a job rerun without a topic reset is harmless. The browser keeps the heatmap as one sign per symbol and slot, and the full series of the selected symbol only.

The feed time is the minimum `Date`+`Time` among the rows added to the tail of `ticks` since the last look (at most 32 per partition, once a second), and it never moves backward: the replayer's source-clock rule, which ignores the stale times of each day file's opening snapshot. It does not go back after `ticks` is reset either; restart the dashboard for a new replay.

## WebSocket protocol

Endpoint `/ws`. Every message is a JSON object with a `type`. All sends happen on one thread, every `--flush-ms`; a session with nothing new gets nothing.

```text
client → {"type":"select","symbol":"RDSA.NL"}

server → {"type":"snapshot","spanEnd":1636464900000,"slots":288,"slotMs":300000,
          "symbols":[{"symbol":"RDSA.NL","secType":"E","exchange":"NL","lastFlip":1636464600000,"cells":"++--…"}],
          "series":{…}|null,"clock":{…}}
server → {"type":"series","series":{"symbol":"RDSA.NL","firstWindowEnd":1636355100000,"points":[[windowEnd,close,ema38,ema100],…]}}
server → {"type":"delta","spanEnd":…,
          "cells":[["RDSA.NL",1636464900000,"+","E","NL",1636464900000]],
          "points":{"symbol":"RDSA.NL","points":[[windowEnd,close,ema38,ema100],…]}|null,
          "clock":{…}}

clock = {"wallMs":…,"feedMs":…|null,"feedIdleSince":…|null,"resultsMs":…|null,"zoneOffset":"+02:00"}
```

- `cells` in a snapshot is 288 characters, oldest slot first: `+` EMA38 > EMA100, `-` below, `0` equal, `.` no window. A delta cell is `[symbol, windowEnd, sign, secType, exchange, lastFlip]`.
- `lastFlip` is the latest retained window whose sign differs from the window before it; a symbol's first window counts as a flip from 0 (D-7). It is null once that flip has left the span.
- `points` in a delta are the changed windows of the session's selected symbol; the client merges them by `windowEnd`.
- On connect the server sends a snapshot; the client then sends `select` and gets a `series`. A reconnect always starts from a new snapshot, and missed deltas are never replayed.
- A browser more than 4 MB of messages behind is disconnected and reconnects to a fresh snapshot.

## Development

```bash
scripts/mvn.sh -pl producer,viz -am test     # backend tests
cd viz/web && npm ci && npm test             # frontend tests (Vitest)
cd viz/web && npm run dev                    # Vite dev server, proxying /ws and /health to localhost:8088
```

Set `VIZ_BACKEND=host:port` for the dev server to use another backend. The Docker build runs the frontend tests, builds it, and serves it from the jar.
