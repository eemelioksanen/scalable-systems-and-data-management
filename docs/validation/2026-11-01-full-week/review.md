# Independent review

The first review used the same pinned bundle, merge-base and runtime source `e144981b3246cca5a4c1860513584e24fe17fa5a`. Its patch SHA-256 was `efbc123fe0f60d7633c9401fdca5dfab57f64c0fdf18fb82ac9238560d107c2e`. There were 71 pending validation/evidence files and no runtime feature changes.

## Standards, GPT-6.1 Sol

One P2 reproducibility finding. `report.py` required six environment/test files that neither `check.sh` nor the documented preparation created. A fresh run would finish the workloads and then fail reporting. The reviewer verified all 169 raw files against the inventory and the 168/75 test counts against the logs. No other material Standards findings. Simple dictionaries and the small amount of repetition did not justify further abstraction.

Limits: no workload/test rerun, independent Parquet comparison or source CSV rehash.

## Standards and trail, Claude Opus 5.5

The raw artifacts supported the stated tests, idle behavior, negative controls, subset/disorder checks and parent late-event reuse. No runtime correctness finding. It identified a medium recording weakness in the idle summary's fixed counts and low findings about local-only trail paths, the pending review record, fixed diagnostic/test counts and optional repetition between the bounded and live runners. It also requested an explicit explanation that the weekend files have no accepted positive-price events.

Limits: no workload rerun, raw checksum audit, full-week Parquet inspection, parent late Parquet inspection or T3 transcript read.

## Spec, GPT-6.1 Sol

The same P2 reproduction finding; no other material Spec gap. The reviewer independently read both full-week Parquet exports and the oracle. Each has 1,857,457 unique windows and 11,355 unique advisories, with exact equality across all selected identity/value fields. Difference, duplicate and late tables are empty. It also checked 64 raw export/comparison checksums, complete seven-file replays, fresh offsets, the terminal REST/log evidence, live idle snapshot and separate scenario coverage. The trail agrees with the artifacts.

Limits: no source CSV rehash, test or live workload rerun. Parent late evidence was checked through its retained identity, comparison and reconciliation artifacts; that parent's raw Parquet is outside this worktree. Git confirms unchanged relevant sources.

## Findings ledger

| ID | Severity | Decision |
|---|---|---|
| S1, both Sol reviewers | Should fix | Added `prepare.sh`, called by `check.sh`, to capture all six required files and run the Java/Python suites before full-week work. Verified capture into a fresh output directory. |
| C1, Opus | Should fix | Idle summary now records measured before/after q1 output counts and measured empty partitions; it asserts the final q1 count. Re-ran the live idle check. The original comparison had already checked three output windows against the independent oracle. |
| C2, Opus | Nit | Preserved append-only history and added the committed-copy path map below and a trail entry. |
| C3, Opus | Nit | Added this review record after the first pass. |
| C4, Opus | Nit | Parse test counts from successful logs; derive the equal-time close from the checked actual table. Summary booleans remain verdicts guarded by assertions. |
| C5, Opus | Nit, declined | Keep explicit bounded and live orchestration. They have different lifecycle and assertions, and already share setup, commands, polling and Python-container helpers. More abstraction is not needed for this validation task. |
| C6, Opus | Nit | Documented weekend input categories and absence of valid positive-price events. |

Earlier trail references to local raw files resolve to these committed copies: `correctness/out/issue9/maven.log` and `python-tests.log` are under `environment/`; `oracle/out/issue9/full-week/manifest.json` is `oracle/full-week.json`; `correctness/out/issue9/<run>/` is `runs/<run>/` for replay, config, Flink job, export manifest and comparison report. Complete Parquet/stack outputs stay local with SHA-256 in `raw-checksums.json`.

A focused Sol re-check covers the changed capture, measured idle summaries, parsed test counts and this ledger. The full-week jobs are not repeated for these evidence-capture fixes; their raw comparisons and digests remain unchanged.

## Focused re-check, GPT-6.1 Sol

Round two approved the current behavior diff, pinned by patch SHA-256 `d57b580000377433c614ed875fd141a70d8019f23c5d43f82adcfd8047aabf1e`. S1, C1 and C4 are resolved. The reviewer inspected the fresh preparation directory with all seven generated files and successful 168/75 test logs, measured RUNNING/FINISHED idle snapshots, empty-partition counts and the new comparison. No material findings remain. Declining C5 was considered reasonable. This pass did not rerun workloads or Parquet comparisons.

Only this review outcome and final cleanup/trail notes were added after that behavior review. The final bundle records those prose additions separately. The implementer's self-audit also paged this task's T3 activity through position 227 and checked the trail against the actual artifacts; it found no unsupported pass claim.
