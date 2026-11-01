# Full-week correctness gate for both queries

The gate passed. [verification.json](verification.json) records all required checks; both complete full-week runs equal the oracle bit for bit with zero late, missing, extra or duplicate records. Advisory precision and recall are both 1.

This directory retains the checks for [issue #9](https://github.com/eemelioksanen/scalable-systems-and-data-management/issues/9), on top of the late-event exporter in [PR #42](https://github.com/eemelioksanen/scalable-systems-and-data-management/pull/42). Runtime code is parent revision `e144981b3246cca5a4c1860513584e24fe17fa5a`. The changes here are validation scripts and evidence. No latency feature files were changed.

## Scope and reference

The input is every byte of all seven DEBS daily CSV files, 8–14 November 2021, including both weekend files. [provenance.json](provenance.json) retains the published download identities, source URL, attribution and license from the existing daily analysis. [inputs.json](inputs.json) records newly computed sizes, MD5 and SHA-256 checksums. Every size and published MD5 must match, as must the retained SHA-256. The source directory is mounted read-only and is never copied or rewritten. Both weekend files contain zero valid positive-price events, so their rows are consumed and classified but produce no windows. Saturday also contains 5,179 non-positive prices; [coverage.json](coverage.json) retains both files' categories.

The independent oracle reads the full week in one invocation and carries EMA state across files. It admits every valid price event, without exclusions or a watermark rejection model. Both implementations use `+02:00`. The job uses the D-38 finite-input baseline, `--bounded --watermark-bound-ms 64265001 --idleness-ms 0`. This covers Wednesday's 5,506 delayed summary rows. A wildcard subscription is preloaded before submitting the job, so the publication filter exposes every calculated window and advisory.

The oracle's [manifest](oracle/full-week.json) retains every input category, every output checksum, its precision recomputation and the admission profile. Its reference has 289,116,808 data rows, 59,391,670 valid price events, 1,857,457 windows across 4,970 symbols and 11,355 advisories. It excludes and rejects zero valid events. The 50-digit precision check finds 1,270 windows in six symbols whose binary64 value differs from decimal by more than 1e-9, with no advisory disagreements. Those are arithmetic diagnostics, not permission to relax the comparison.

## Checks and retained evidence

| Requirement | Check |
|---|---|
| Full week, all symbols and FR/NL/ETR | Two independent full-week runs in [full-week.json](full-week.json), per-exchange totals in [exchanges.csv](exchanges.csv), per-day totals in [days.csv](days.csv). |
| Closing prices, identities, EMA tolerance and advisories | Each run's `compare/report.json` checks exact closes and identities, absolute EMA error ≤1e-9, exact advisory identities, precision/recall, and duplicates before joins. No mismatch class is waived. |
| No excluded events or pipeline loss | Each run's `summary.json` reconciles acknowledged rows, Flink source reads, source price events, q1 inputs, q1 outputs and sink writes against complete Kafka exports and oracle totals. Empty, complete `late-ticks` exports prove zero late events even after bounded metrics disappear. |
| Fresh deterministic clean replay | `week-2` recreates the isolated Kafka/Flink project and replays all seven files again. Canonical result/advisory digests exclude run timestamps and Kafka positions. Both runs must equal the oracle bit for bit, with no missing, extra or duplicate records. |
| Exact boundaries, sparse/missing/stale timestamps, simultaneous windows and overnight state | [edge-cases.json](edge-cases.json), the existing [hand-derived fixture](../../../fixtures/semantics/derivation.md), and the `semantics` run. Its 24-hour bound admits the fixture's stale prior-day close. This bound is separate from the full-week baseline. |
| Bounded disorder with stable equal-time ties | Five-minute disorder with seed 9 on both semantic day files, [injection.json](injection.json). The fixture check compares original and rewritten raw equal-time sequences, both oracles, and both actual job exports. |
| Subset publication | A fresh `subset` run subscribes only to `EDGE.NL` and `SWING.FR`. Both result and advisory records must equal the corresponding rows of the wildcard run, with no other symbols or duplicates. q1 must still calculate every fixture window. |
| Idle partitions and final-window completion | A paced `idle` run has eight input partitions, only one populated. Two windows must be emitted while the job is still RUNNING; drain must then complete the third. Its comparison must pass with zero late events. |
| Beyond-bound late injection | Reuses the parent's [paced late-event verification](../2026-11-01-late-events/README.md): 13 injected and exported identities agree including multiplicity, the matching oracle excludes precisely those positions, and both queries compare without differences. The job source is unchanged from that evidence. |
| Gate detects errors | [negative-controls.json](negative-controls.json) removes one actual window, duplicates one, and changes an EMA by 0.01 in separate copies. Every altered export must fail; the EMA control must report its magnitude. |

Raw replay reports, Flink terminal job/vertex records, export manifests, comparison reports and exact argv logs are retained under `runs/`. Complete raw Parquet exports, oracle tables, comparison difference/duplicate tables, stack logs and test logs remain in this worktree's ignored `correctness/out/issue9/` and `oracle/out/issue9/`. [raw-checksums.json](raw-checksums.json) inventories those files by path, size and SHA-256. Empty difference tables are retained too. No unmatched output is discarded. The raw Parquet files are reproducible local evidence, not committed dataset copies.

The supported path is the clean replay in [interfaces.md](../../interfaces.md#restart-and-replay-d-28). This check does not claim checkpoint recovery or exactly-once sinks. Replaying into existing output topics would mix copies; every run here starts with fresh topics. Recreating the input also changes `maxIngestTs`, so both run-specific timestamps are excluded from value digests.

## Reproduce

Use the seven complete day files with the checksums above. The entry script builds the jars, runs both test suites and captures all required environment/test files before starting the week. A fresh checkout needs the cached images named in [compose.yml](../../../scripts/validation/issue9/compose.yml), plus the pinned oracle and correctness images. Build the pinned Python images if they are not cached:

```bash
podman build -t trading-trends/oracle:dev oracle
podman build -t trading-trends/correctness:dev correctness
```

Use Python 3.11+ with `correctness/requirements.txt` installed for the small fixture and report checks. The full-week oracle always runs in the pinned Python 3.12 container. The recorded small checks used an existing Python 3.14 environment with Polars 1.44.2; its environment was read without editing the other worktree.

```bash
export ISSUE9_DATASET=/absolute/path/to/the/seven/daily/files
export ISSUE9_PYTHON=/absolute/path/to/python-with-correctness-dependencies
bash scripts/validation/issue9/check.sh
```

The script refuses existing run directories instead of overwriting evidence. Use a fresh worktree/output directory for a new complete run. Keep `data/issue9/disorder` absent before fixture preparation. The script affects only Compose project `issue9`, exposes Flink on host port 8109, and mounts the external dataset read-only. `ISSUE9_FLINK_PORT` selects another free port. On an interrupted attempt, inspect the retained logs, then remove only this task's stack:

```bash
python3 scripts/validation/issue9/run.py down --name cleanup
```

The stack uses Podman 6.1.4 and Docker Compose 5.6.1 through `podman compose`, on a 7-CPU, 8-GiB VM. Kafka, JobManager, TaskManager and replay container memory are capped. The standalone oracle is capped at 5 GiB and two CPUs, with two Polars threads. It finished at 3,288 MiB peak anonymous memory. These are correctness runs, not performance measurements. The shared Maven cache and pre-existing images are retained.

The decision trail is [decisions.tsv](decisions.tsv). Review findings and their disposition are recorded alongside the final evidence.
