# CS-E4780 Course Project: Implementation & Data Analysis Roadmap

## Context

Source documents: `CS_E4780_Course_Project_2026.pdf` (task) and `Evaluation_Criteria_26.pdf` (rubric). The task is to build a streaming system over the DEBS 2022 Grand Challenge "Trading Data" set (Zenodo 10.5281/zenodo.6382482): 289M events, 5504 equities/indices, 3 exchanges (FR 55%, NL 28%, ETR 17%), one CSV per day, 8–14 Nov 2021.

- **Query 1:** EMA₃₈ and EMA₁₀₀ per symbol over 5-min tumbling event-time windows. Windows are aligned to midnight, and a window is evaluated when the next one starts.
- **Query 2:** a BUY advisory when EMA₃₈ crosses above EMA₁₀₀, a SELL advisory when it crosses below.
- **Smart visualization:** optional, but it earns the "Exceptional" level on creative explorations.
- **Deliverables:** a public GitHub repo plus a 4–5 page ACM-format report. The deadline is **27.11.2026**, about 8.5 weeks from now.

**Choices already made:** Kafka + Apache Flink (Java). Team of 3 or more. Develop locally with Docker Compose, then run the final benchmarks on cloud VMs.

**Grading (26 pts):**

| Part | Points | Criteria |
|---|---|---|
| Implementation | 12 | Functionality, Performance, Scalability, Creative explorations (3 each) |
| Report | 12 | Design, Implementation explanation, Evaluation, Academic writing (3 each) |
| Teamwork | 2 | Individual contributions, Teamwork |

Every roadmap item below is there to earn the "Exceptional (3)" level on one of these criteria.

---

## 1. Semantics to pin down in week 1 (write them in `docs/decisions.md`)

These decisions drive both correctness and the report. The Flink job and the oracle must implement exactly the same ones.

| Topic | Decision (recommended) | Note |
|---|---|---|
| Symbol key | `ID` column as-is, e.g. `RDSA.NL`. Derive `exchange` from the suffix. | Confirm the format in EDA. |
| Price event | A row whose `Last` is non-null. Every other row is a quote or housekeeping event. | Count those per type. Drop them before `keyBy`. |
| Event time | `Trading date` (DD-MM-YYYY) + `Trading time` (HH:MM:SS.mmm), parsed into epoch ms. | Measured as millisecond precision, so epoch ms is lossless (D-20). `Trading date` is absent on 92.1% of price rows; `Date` substitutes (D-23). Pick one timezone interpretation and use it everywhere. Whole-hour offsets keep 5-min windows aligned to midnight. |
| Close | The price with the **latest event time** in the window, not the last one to arrive. Break ties by arrival order. | This matters because order is not guaranteed across exchanges. |
| Window firing | Fire when the event-time watermark passes the window end. Use a bounded-out-of-orderness watermark with idleness. | Set the bound from the disorder measured in EDA. Send late events to a side output and count them. |
| Empty windows | If a symbol has no price event in a window, skip it: EMA unchanged, no output, no crossover check. | Document it. The alternative (carry the last close forward) is an ablation. |
| EMA init | Implement EMA₀ = 0 exactly as the spec says. | Side effect: every symbol gets a bullish crossover in its first window. Flag it in the viz and report. A warm-up variant is optional. |
| Day boundaries | EMA state carries over across days and the weekend. There is no daily reset. | |
| Subscriptions | Compute EMAs for **all** symbols. Only emit for subscribed symbols. | The subscription set lives in a compacted Kafka topic and reaches Flink as broadcast state. |
| Missing or stale timestamps | Price rows with a missing trading time, or a trading date ≠ system date. | Measure these in EDA first, then choose between dropping them and falling back to `Date`+`Time`. |

---

## 2. Architecture

```
 data/*.csv ──► Replayer (Java) ──► Kafka `ticks` (N partitions, key = symbol)
                  modes: preload | max-speed | real-time×k | subset(exchange/symbols)
                                        │
            Kafka `subscriptions` ──┐   ▼
            (compacted)             │  Flink job
                                    │   KafkaSource + WatermarkStrategy(boundedOOO, idleness)
                                    │   → parse/normalize → side-output stats (non-price, housekeeping, malformed, late)
                                    │   → keyBy(symbol)
                                    │   → EMA operator (Q1) ─► crossover (Q2)
                                    └──► broadcast filter (subscribed only)
                                        │
                     Kafka `ema-results`, `advisories`, `stats`
                                        │
                  ┌─────────────────────┴───────────────────┐
             Viz backend (WebSocket)                   Prometheus ◄─ Flink/Kafka/node metrics
             → web dashboard                           → Grafana (throughput, latency, backpressure, CPU)
```

- **Main operator:** one `KeyedProcessFunction` per symbol. Its `ValueState` holds the current window's close and timestamp plus `ema38`, `ema100` and the previous EMAs. It registers an event-time timer at the window end. When the timer fires it computes Q1, runs the Q2 check, and emits.
- **Alternative to benchmark:** the `TumblingEventTimeWindows` + `ProcessWindowFunction` pair, followed by a keyed EMA function.
- **Latency measurement:** the replayer stamps each record with its ingest wall-clock time (the Kafka record timestamp). Each output carries (a) processing latency (emit time minus the arrival of the window's last event) and (b) watermark lag.
- **Benchmark isolation:** in *preload* mode, the whole week is written into Kafka first and Flink then reads from the earliest offset. Throughput is then measured without the replayer as a bottleneck. Latency runs use *live replay*.

**Repo layout:**

```
docker-compose.yml            # Kafka (KRaft), Flink JM + TM (scalable), Prometheus, Grafana, viz
Dockerfile                    # multi-stage: Maven build → Flink image with job jar, JRE image for producer
.devcontainer/                # containerized JDK 17 + Maven for the IDE (no host JDK needed)
pom.xml                       # Maven multi-module, Java 17, Flink 2.2.1, Kafka connector 5.0.0-2.2
producer/                     # CSV replayer → Kafka
flink-job/                    # Q1+Q2 job, operator tests
oracle/                       # Python + DuckDB/Polars reference implementation
analysis/                     # EDA notebooks, figures for the report
viz/                          # dashboard backend + frontend
bench/                        # experiment scripts, configs, results/*.csv, plotting
infra/                        # prometheus.yml, grafana provisioning, cloud setup scripts
report/                       # ACM LaTeX (acmart), synced with Overleaf
docs/decisions.md             # decision log (feeds the individual-contribution answers)
data/                         # raw CSVs (gitignored)
```

---

## 3. Data analysis (EDA) → `analysis/`

Use DuckDB directly on the CSVs (skip the `#` comment header lines), plus Polars and matplotlib. The results feed the watermark and partitioning choices, the benchmark targets, and the report's "strong focus on dataset analysis".

1. **Schema and volume:** rows per day, exchange and SecType. Reproduce the 289M total and the 55/28/17 split, and the 239.5M equity vs 49.5M index split.
2. **Event taxonomy:** classify rows as price, bid/ask quote, or housekeeping (empty payload). Break down by exchange and SecType. Check that price events are about 49M for indices and about 10M for equities.
3. **Arrival rate:** events/s over time per day. Measure peak rate, p99 per-second rate, trading-hours shape and open/close spikes. This sets the replay rates and the "peak rate" test.
4. **Long tail:** events per symbol and price events per symbol, shown as a rank-frequency plot on log-log axes. Report top-k share, a Gini coefficient, and the number of symbols with no price events. Simulate key-to-partition load to predict skew.
5. **Time quality:** missing trading time/date. Gap between `Trading time` and system `Time`. Stale prices (trading date ≠ date). Per-symbol out-of-orderness (backward jumps in event time within arrival order). These numbers set the watermark bound and the late-event policy.
6. **Window statistics:** price events per (symbol, window). Share of empty windows per symbol, which justifies the empty-window policy.
7. **Oracle outputs:** crossovers per day, exchange and SecType, and the most active symbols. Used as a sanity check and as report material.

**Oracle (`oracle/`):** a batch implementation of exactly the semantics in §1. It writes `ema_results.parquet` and `advisories.parquet` for the full week. It is the ground truth for correctness.

---

## 4. Evaluation plan → `bench/`

Every run is scripted: config → run → collect → `results/*.csv`. Repeat each run 3 times and report mean ± std.

| Rubric target | Experiment |
|---|---|
| Correctness | Diff Flink output against the oracle for the full week. EMAs must match within 1e-9. Advisories are compared as sets, with precision and recall reported. Also: inject bounded shuffling and show the results do not change, and report late-event counts beyond the bound. |
| Throughput | Preload mode: wall-clock time and events/s for the full week at parallelism 1/2/4/8/16. Plot the speedup curve. |
| Latency | Live replay at 1×, 10× and 100× real time and at peak: p50/p95/p99 processing latency. Keep the 5-min window delay (inherent to the spec) separate from system latency. |
| Scalability: exchanges | Run with 1, then 2, then all 3 exchanges (replayer subset mode). |
| Scalability: rate | Raise the replay rate until backpressure or consumer lag appears, to find the saturation point. |
| Scalability: resources | Add Flink TaskManagers/slots, first on one large VM (vertical), then across VMs (horizontal). |
| Scalability: subscribers | 1 → 10 → 100 → 1000 simulated traders, with symbol sets sampled from the long tail and uniformly. |
| Long tail / skew | Records and state per subtask, and busy-time imbalance. Compare Kafka partition counts. Optionally add hot-key-aware partitioning. |
| Resource usage | CPU, memory, GC, network, Kafka lag, and state size, taken from Prometheus. |
| Design-space ablations | KeyedProcessFunction vs Window API. Early filtering in the replayer vs in Flink. HashMap vs RocksDB state backend. Watermark bound vs latency vs late-event rate. JSON vs binary serialization. Empty-window policy. |

**Cloud:** reuse the compose file. First run on one large VM (for example 16–32 vCPU) with `--scale taskmanager=N`. Then run multi-node with Docker Swarm (`docker stack deploy`), keeping the replayer and Kafka on separate nodes from Flink.

---

## 5. Smart visualization → `viz/`

- **Backend:** consumes `ema-results` and `advisories` and pushes updates over WebSocket.
- **Frontend:** plain JS or React with TradingView Lightweight Charts or ECharts. It has four views:
  1. **Symbol chart:** close, EMA₃₈ and EMA₁₀₀ lines with ▲ BUY / ▼ SELL markers, mirroring the spec's Fig. 2 and 3.
  2. **Advisory feed:** live, and filterable by exchange, SecType and subscription.
  3. **Trend heatmap:** symbols × time, colored by the sign of EMA₃₈ − EMA₁₀₀ and sorted by most recent crossover. This is the "cut through the noise" view.
  4. **Replay clock** showing simulated time next to wall-clock time.
- **Operational metrics:** kept separate in Grafana.

---

## 6. Workstreams (3+ people)

| Stream | Owns | Report sections |
|---|---|---|
| **A: Data & correctness** | EDA, oracle, correctness diffing, operator unit tests (with B) | Dataset analysis, correctness evaluation |
| **B: Stream core** | Flink job: parsing/normalization, watermarks, EMA state, crossover, subscriptions, ablation variants | Architecture, implementation |
| **C: Platform & eval** | Docker Compose, replayer, metrics stack, bench harness, cloud runs | Implementation (infra), performance/scalability |
| **D: Viz** (4th person, or shared by A and C) | Dashboard | Visualization, creative exploration |

Everyone keeps notes in `docs/decisions.md` and on PRs. From those, each member writes their own answers to the rubric's three contribution questions.

---

## 7. Timeline (today 28.9 → deadline 27.11)

| Week | Dates | Goals | Milestone |
|---|---|---|---|
| 1 | 28.9–4.10 | Download the dataset (check disk space first). Repo skeleton and Maven modules. Compose stack runs (Kafka + Flink + Prometheus/Grafana). EDA items 1–2. Draft `decisions.md`. | Stack runs locally |
| 2 | 5–11.10 | EDA items 3–6. Oracle for the full week. Replayer v1 (preload + rate-controlled). Flink skeleton: source → parse → watermark → keyBy. | Oracle output exists |
| 3 | 12–18.10 | Q1 EMA operator plus test-harness unit tests (first window, empty windows, out-of-order, ties). Output to Kafka. Diff one day against the oracle. | Q1 correct on 1 day |
| 4 | 19–25.10 | Q2 crossovers. Subscriptions via broadcast state. Full-week oracle diff. Viz v0 (one symbol chart). | **M1: functionally complete** |
| 5 | 26.10–1.11 | Latency instrumentation. Bench harness. Local tuning (serialization, object reuse, parallelism, state backend). Viz v1 (feed + heatmap). | Automated local benchmarks |
| 6 | 2–8.11 | Cloud deployment. All scalability experiments and ablations, run once. | **M2: all experiments run** |
| 7 | 9–15.11 | Repeat runs and produce final plots. Bug fixes. Viz polish. Full report draft. | Report draft |
| 8 | 16–22.11 | Report revision (4–5 pages, ACM template, consistent citations). Individual contributions. README with reproduction steps. Make the repo public. | Submission-ready |
| 9 | 23–27.11 | Buffer. Final check. Submit. | **Submit 27.11** |

---

## 8. Report outline (ACM, 4–5 pages)

1. **Abstract**
2. **Introduction:** market data, CEP, what was built, headline numbers.
3. **Dataset analysis:** volumes, event taxonomy, rates, long tail, time quality. Drawn from §3. The rubric asks for a "strong focus" here.
4. **System architecture:** components, partitioning by symbol, state, watermarks, subscriptions, the design space and the reasoning behind each choice.
5. **Implementation:** handling of tick, housekeeping and metadata events; normalization; windowing and EMA state; crossover detection; tooling and hardware.
6. **Evaluation:** correctness, throughput, latency, scalability (exchanges, rate, resources, subscribers), long-tail skew, resource usage, ablations.
7. **Visualization**
8. **Conclusion**
9. **Individual contributions:** each member answers the three rubric questions.

---

## Verification (how we know each phase is done)

- **Local smoke test:** `docker compose up`, then the replayer loads a 1-hour sample, then advisories appear in Kafka and on the dashboard.
- **Unit tests:** `mvn test` runs the Flink operator test-harness cases with hand-computed EMAs and crossovers.
- **Correctness gate:** `oracle/diff.py` against the Flink output topics for the full week shows zero EMA mismatches beyond tolerance and matching advisory sets. This gate must pass before any performance numbers are reported.
- **Reproducibility:** `bench/run.sh <experiment>` regenerates each results CSV, and `bench/plot.py` regenerates each report figure.

---

## Development environment (no host JDK or Maven)

Only Docker is needed on the host. Python 3 covers the oracle and the EDA.

- **Build:** `docker compose build` runs Maven inside the `maven:3.9-eclipse-temurin-17` image (stage `build` of the root `Dockerfile`). A BuildKit cache mount keeps `~/.m2` between builds, so dependencies download only once.
- **Run tests:** `docker build --target test .` runs `mvn verify`.
- **IDE:** open the repo in the dev container (`.devcontainer/`). VS Code uses "Reopen in Container"; IntelliJ uses "Remote Development → Dev Containers". The IDE then gets JDK 17 + Maven inside the container.
- **Disk:** check the size on Zenodo before downloading. 289M rows is likely tens of GB uncompressed, and a full-week Kafka preload needs roughly the same again. Keep `data/` and the Docker volumes on a disk with ≥ 100 GB free, or work from a per-day or per-exchange sample locally and run full-week experiments on the cloud VMs.
