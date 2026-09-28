# Decision log

Record every design decision here, with who made it, when, and why. The report's architecture section and each member's individual-contribution answers are written from this log.

Status is one of: **proposed**, **accepted**, **superseded (by D-n)**.

## Query semantics (week 1)

The Flink job (`flink-job/`) and the oracle (`oracle/`) must implement exactly these. Accepting a proposal requires EDA evidence (see `docs/ROADMAP.md` §3).

| ID | Topic | Decision | Status | Evidence / notes |
|---|---|---|---|---|
| D-1 | Symbol key | `ID` column as-is (e.g. `RDSA.NL`). `exchange` is derived from the suffix. | proposed | Confirm the ID format in EDA. |
| D-2 | Price event | A row whose `Last` is non-null. All other rows are quote or housekeeping events: counted, then dropped before `keyBy`. | proposed | |
| D-3 | Event time | `Trading date` + `Trading time` parsed to epoch ms. One timezone interpretation, used everywhere. | proposed | Whole-hour offsets keep 5-min windows aligned to midnight. |
| D-4 | Close | The price with the latest event time in the window. Ties broken by arrival order. | proposed | Order across exchanges is not guaranteed (spec §2.1). |
| D-5 | Window firing | Fire when the event-time watermark passes the window end. Bounded out-of-orderness with idleness. Late events go to a side output and are counted. | proposed | Bound = ? (from EDA disorder analysis). |
| D-6 | Empty windows | No price event in a window: EMA unchanged, no output, no crossover check. | proposed | The carry-forward alternative is an ablation. |
| D-7 | EMA init | EMA₀ = 0 exactly as the spec says. | proposed | Causes a BUY in every symbol's first window. Flag it in the viz and report. |
| D-8 | Day boundaries | EMA state carries over across days and the weekend. | proposed | |
| D-9 | Subscriptions | EMAs are computed for all symbols. Results are emitted only for subscribed symbols, via a compacted `subscriptions` topic and broadcast state. | proposed | |
| D-10 | Missing or stale timestamps | Drop them, or fall back to `Date`+`Time`? | open | Measure the counts in EDA first. |

## Stack and infrastructure

| ID | Topic | Decision | Status | Evidence / notes |
|---|---|---|---|---|
| D-11 | Stream processor | Apache Flink 2.2.1 (Java 17) + Kafka 4.3.1 (KRaft). | accepted | Event-time windows, keyed state, horizontal scaling. The Kafka connector (`5.0.0-2.2`) is not yet released for Flink 2.3. |
| D-12 | Build | Maven runs inside Docker (multi-stage `Dockerfile`). The IDE runs in a dev container. No JDK on hosts. | accepted | |

## Log

Add later decisions below, newest first.

<!--
### D-n: <title> (YYYY-MM-DD, <name>)
Context: …
Decision: …
Alternatives considered: …
Consequences / measurements: …
-->
