# Experiment records

Copy `template.json` to a run-specific JSON file. Keep one record per repetition, including failed or partial attempts. `null` means unknown, never zero or a default. The initial template is planned and records no measurements.

Use relative repository paths or stable revision-pinned URLs for retained outputs. For large ignored raw outputs, record their location plus a checksum manifest, and explain how to reproduce them. Never commit the dataset, credentials or large raw exports. Record a source revision and dirty patch if applicable, not just the revision of a later evidence commit.

`schema.json` is JSON Schema draft 2020-12. Validate with a JSON Schema tool, for example `check-jsonschema --schemafile report/experiments/schema.json report/experiments/template.json`. A valid shape does not prove the run complete: a recorded run may still have unknown fields. Only claim reproducibility after filling applicable fields, explaining inapplicable fields in `limitations`, and verifying every linked artifact.

- Dataset checksums identify both original files and derived input. Include checksum algorithm, subset exchanges/symbols/dates, filter command, disorder seed and transformed-file manifest where relevant.
- Hardware describes host and VM separately. Software records engine, Compose, OS, architecture, Java, Flink, Kafka, clients and exact image identities or lock/config files.
- Topology and resources describe nodes, service placement, TaskManagers, slots per TaskManager, parallelism, topic partition counts, CPU/memory/heap limits and effective configuration.
- Replay and subscriptions retain exact arguments, mode, target and achieved rate, startup order, subscription payload or file, trader count and symbol selection/seed. Record watermark, idleness, checkpoint and timezone settings.
- Repetitions retain the configuration group, run index, planned/completed counts, failures/exclusions and summary method. The roadmap plans three comparable performance repetitions with mean and standard deviation. Correctness reruns serve a different purpose.
- Measurement fields define interval, warm-up/drain exclusions, throughput numerator/denominator, latency boundaries and clocks. Link raw results, effective config, commands, manifests, reconciliation, oracle comparisons and derived tables/figures, with limitations.

## Reuse the merged evidence

These retained checks predate this skeleton. Do not relabel them as new experiments or repeat benchmarks just to populate the template. Runtime revisions differ from the evidence snapshot, which is [f28621890e77a18fd28791467406f4e3c2469652](https://github.com/eemelioksanen/scalable-systems-and-data-management/tree/f28621890e77a18fd28791467406f4e3c2469652).

| Evidence | Runtime revision and run identity | What it establishes | Limits |
|---|---|---|---|
| [Late events](../../docs/validation/2026-11-01-late-events/README.md) | `35eb8f69e9d95775256a994469bb333f36e61423`; `verification.json` live/bounded checks, `replay.json`, `injection.json` | Synthetic injected/exported late identities; matched oracle exclusions; bounded admit-all check | Not a full-week correctness or performance run; benchmark reconciliation is exercised separately from the complete harness |
| [Full week](../../docs/validation/2026-11-01-full-week/README.md) | `e144981b3246cca5a4c1860513584e24fe17fa5a`; `runs/week-1`, `runs/week-2`, `full-week.json` | Two fresh full-week replays against the oracle, both queries, all exchanges and weekends | Correctness evidence; no checkpoint recovery or exactly-once claim; raw exports are ignored, checksums retained |
| [Live latency](../../docs/validation/2026-11-01-latency/README.md) | `065eaa6502ca571ad2149e8541bcea2df9d88f58`; `issue37-latency-subset12-live-100x-20261101T210052Z-r1` | One real 100x 12-symbol Monday run, complete latency export and reconciliation | No oracle comparison or repetition study; modeled readiness and 5 s scrapes; output visibility unobserved; collection resumed after failure |

For final report claims cite the exact run/config and limitations, not merely this index. The existing READMEs and JSON manifests remain authoritative. Do not treat their local VM observations as cloud or multi-node evidence.
