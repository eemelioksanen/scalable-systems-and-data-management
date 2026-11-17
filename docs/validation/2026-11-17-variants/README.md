# Design-comparison variants (D-42, #23)

Revision `ba5c1c21f9826957f13d22e8f57e33bb0cbd9cbb` passed the variant check on 17 November 2026: every variant of D-42 ran through `bench/run.py` on the hand-derived semantics fixtures and matched its reference. [runs.json](runs.json) records each run, and [decisions.tsv](decisions.tsv) is the decision trail of the change. This is correctness evidence on synthetic fixtures, not a performance measurement.

## Environment

macOS on arm64, Podman 6.1.3 with a 7-CPU, 8-GiB VM, Docker Compose 5.7.1. The isolated Compose project was `issue23`, with Flink on host port 18081, Prometheus on 19090 and Kafka on 39092, so the default `trading-trends` stack was left alone. One TaskManager with 2 slots, parallelism 2, 8 `ticks` partitions, one `*` subscription, zone offset `+02:00`. Every run recorded a clean tree at that revision.

The earlier Podman runs needed two local workarounds, and this one used the same two ([baselines](../2026-10-12-baselines/README.md)). Neither changes the stack under test:

- `COMPOSE_FILE` added [the latency check's override](../2026-11-01-latency/podman.override.yml), which drops cAdvisor's `/var/lib/docker` mount (D-35).
- `docker` was a wrapper that drops `--user` from `docker run`, because rootless Podman cannot map this Mac's user ID into the oracle and correctness containers.

## What ran

[scripts/validation/issue23/run.sh](../../../scripts/validation/issue23/run.sh) copies `fixtures/semantics/*.csv` to `data/issue23/semantics/`, runs the oracle on them with `--empty-windows skip` and `carry-forward`, then runs one `bench/run.py` config per variant from [configs/](../../../scripts/validation/issue23/configs/). [report.py](../../../scripts/validation/issue23/report.py) then writes `runs.json` and hashes each run's exported results over the D-18 fields.

Every preload used `--bounded` with an 86,400,000 ms bound and idleness off, because the fixture's day 2 line 63 needs 84,450,001 ms. The live run used the same bound, so its output does not depend on timing (see below).

| Config | Variant | Sent | `ema-results` | `advisories` | Reference | Verdict |
|---|---|---|---|---|---|---|
| baseline | none | 80 | 61 | 11 | admit-all oracle | pass |
| window | `operator: window` | 80 | 61 | 11 | admit-all oracle | pass |
| rocksdb | `stateBackend: rocksdb` | 80 | 61 | 11 | admit-all oracle | pass |
| binary | `ticksEncoding: binary` | 80 | 61 | 11 | admit-all oracle | pass |
| early-filter | `filtering: replayer` | 78, 2 filtered | 61 | 11 | admit-all oracle | pass |
| carry-forward | `emptyWindows: carry-forward` | 80 | 250 | 11 | carry-forward oracle | pass |
| all | all five at once | 78, 2 filtered | 250 | 11 | carry-forward oracle | pass |
| live-binary | live at 2000×, `window` and `binary` | 80 | 61 | 11 | admit-all oracle | pass |

- **All 8 runs are `COMPLETE`.** Each passed every reconciliation check and the comparison with its oracle reference: 0 EMA differences, 0 advisory differences, 0 late events.
- **Each job ran the requested variant.** The configuration the job published at `/jobs/<id>/config` equals the run's `variant`, bound and idleness in every run.
- **Same output where D-42 says so.** The window, RocksDB, binary and early-filter runs published bit for bit the baseline's results and advisories: one SHA-256 over the D-18 fields for all five. Carry-forward published the 250 windows and 11 advisories the fixture states by hand, and all variants at once published the same as carry-forward alone.
- **RocksDB was in use.** The RocksDB runs' TaskManager logged `Loading state backend via factory org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackendFactory`; the others logged `State backend is set to heap memory ... HashMapStateBackend`.
- **Early filtering.** The replayer sent 78 records and filtered the fixture's 2 quote and housekeeping rows. The reconciliation matched the job's non-price count plus the 2 filtered rows with the oracle's.
- **Live binary run.** The correctness export read all 80 binary `ticks` records back and decoded the fixture's 71 price events, and the latency tool ran on them. Every window was released by the drain, so the run measured no latency: it shows the binary path works live, not how fast. The fixture's system times include `00:00` rows that hold the replayer's source clock still, so the replay arrives unpaced. A 1 s bound would then make line 63's lateness depend on watermark timing (D-28).

## Tests at the same revision

- `scripts/mvn.sh verify`: 226 Java tests pass, 46 replayer, 129 job and 51 dashboard. Among them, `VariantsTest` runs every operator, empty-window policy and state backend against `fixtures/semantics/expected.json`, both operators against the golden sequences and a late event, and checks that each backend reaches the keyed operators. `BinaryTicksContractTest` and the replayer's `BinaryTicksTest` pin the encoder and decoder to `fixtures/semantics/ticks-binary.tsv`.
- `pytest correctness/tests oracle/tests` with Polars 1.44.2 on Python 3.13: 124 tests pass, including the oracle's carry-forward expectations and the export's binary decoder against the same file.
- `docker build --target test .`, the README's clean test build, failed at first: `.dockerignore` left `fixtures/semantics/` out of the build context, which the new tests read. With it added in the next commit, which changes nothing the stack's images contain, the target ran the same 226 tests and passed.

## Not covered

- Performance of any variant. #11 measures them.
- The full week. Its day files are not in this worktree.
- Binary output topics. D-42 keeps the outputs JSON.
- The dashboard's feed clock during a binary replay, which reads `ticks` as CSV.

## Reproduce

From the repository root, with the stack's images buildable:

```bash
scripts/validation/issue23/run.sh
uv run --with polars==1.44.2 python scripts/validation/issue23/report.py
```

Under rootless Podman, set `COMPOSE_FILE` and the `docker` wrapper as above first. The raw runs stay in the ignored `bench/results/raw/issue23-*`.
