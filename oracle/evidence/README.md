# Oracle runs

Each JSON file is the `manifest.json` of one run: revision, command, parameters, input
sizes and MD5s, every count, the admission profile, the precision report, and the row
counts and SHA-256 of every output. The outputs themselves are regenerated, not committed.

All runs used `--zone-offset Z` and `--missing-trading-date system-date`, with no exclusions.
Only `week-bound60s` sets a watermark bound.

| Run | Input | Command (host) |
|---|---|---|
| [`2026-09-28-sample.json`](2026-09-28-sample.json) | the #4 stratified sample of 08-11-21, 1,015,019,421 bytes, MD5 `0b499f88f5932e1ec64f7739239cde42` | `python3 oracle/evidence/rebuild_sample.py` then `oracle/run.sh --out /out/sample data/slice/debs2022-gc-trading-day-08-11-21.slice.csv` |
| [`2026-09-28-weekday-08-11-21.json`](2026-09-28-weekday-08-11-21.json) | `debs2022-gc-trading-day-08-11-21.csv` | `oracle/run.sh --out /out/weekday data/debs2022-gc-trading-day-08-11-21.csv` |
| [`2026-09-28-week.json`](2026-09-28-week.json) | all seven day files, 08-11-21 to 14-11-21 | `oracle/run.sh --out /out/week data/debs2022-gc-trading-day-*.csv` |
| [`2026-09-28-week-bound60s.json`](2026-09-28-week-bound60s.json) | the same | `oracle/run.sh --out /out/week-bound60s --watermark-bound-ms 60000 data/debs2022-gc-trading-day-*.csv` |

The MD5 of every day file in the manifests matches the Zenodo record. The sample is cut
from the local weekday file by [`rebuild_sample.py`](rebuild_sample.py), which checks all
21 chunks against the MD5s in `analysis/out/provenance.json`, so it is byte-identical to
what `fetch_slice.sh` downloads.

## Results

| | sample | weekday 08-11-21 | week | week, 60 s bound |
|---|---|---|---|---|
| data rows | 11,784,671 | 53,994,554 | 289,116,808 | same |
| accepted price events | 2,389,887 | 11,794,520 | 59,391,670 | same |
| of which `Date` substituted (D-25) | 2,201,703 | 10,873,721 | 54,479,627 | same |
| `Last` = 0 (nonPositivePrice) | 17,593 | 62,212 | 316,382 | same |
| malformed, missing timestamp | 0 | 0 | 0 | same |
| rejected as late | | | | 5,506 |
| symbols / windows | 3,905 / 123,101 | 4,840 / 373,126 | 4,970 / 1,857,457 | 4,970 / 1,855,645 |
| BUY / SELL | 3,905 / 0 | 4,840 / 0 | 7,772 / 3,583 | 7,769 / 3,580 |
| events late in source order at bound 0 | 5 | 79 | 5,858 | |
| smallest bound admitting all of them | 25 ms | 25 ms | 64,265,001 ms (see below) | |
| binary64 drift from exact: max / worst symbol | 5.8e-10 (13 ulps), `A1EXY2.ETR` | 1.4e-9 (24 ulps), `IC2MV.FR` | 2.3e-9 (33 ulps), `A1EXY2.ETR` | same |
| windows over 1e-9 on drift alone | 0 | 50 (2 symbols) | 1,270 (6 symbols) | 1,139 (6 symbols) |
| advisory disagreements / near ties | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 |
| wall time / peak heap | 4 s / 1.2 GB | 17 s / 3.5 GB | 122 s / 3.8 GB | 85 s / 5.4 GB |

The week totals match the roadmap's reference figures: 289 M events, of which about 59 M
are price events (49 M index and 10 M equity).

| trading day | windows | price events | BUY | SELL |
|---|---|---|---|---|
| Mon 08-11 | 373,126 | 11,794,520 | 4,840 | 0 |
| Tue 09-11 | 371,307 | 11,900,121 | 82 | 94 |
| Wed 10-11 | 373,987 (+1,812 at 01:05 without a bound) | 12,068,273 | 609 | 983 |
| Thu 11-11 | 368,727 | 11,835,403 | 973 | 1,320 |
| Fri 12-11 | 370,310 | 11,793,353 | 1,268 | 1,186 |

The weekend files, 13-11 and 14-11, contain no price events. 13-11 has 5,179 rows with
`Last` = 0 and 14-11 has none with `Last` set, so the week's last window is Friday's.

## Findings

**Cross-check against the EDA.** Every sample count agrees exactly with the independent
profile in [`docs/data-contract.md`](../../docs/data-contract.md): 11,784,671 data rows;
2,389,887 price rows; 17,593 `price_zero`; 9,377,191 rows without `Last` (empty payload,
quote, other and zero-filled together); 2,201,703 price rows without `Trading date`.

**Monday's advisories are all the zero-start BUY.** Starting from EMA₀ = 0 (D-7), EMA38 climbs
faster than EMA100 and stays above it unless the price falls by roughly the gap between
them, relative to the price: about 3% after one window, a third after 35, still 13% after
100. No symbol falls that far on the first day. For a symbol that trades in every window
(about 108 a day), the gap is still 11% at the start of the second day and 1% at its end,
which is why Tuesday has fewer than 200 advisories and the later days over 1,500 each.

**A burst of summary rows at the end of 10-11.** Lines 64,441,528 to 64,453,580 of
`debs2022-gc-trading-day-10-11-21.csv` carry system time 18:56 but `Trading time`
01:00:00.000. They cover 1,812 FR and NL indices, three rows each, and each row's `Last`
copies that index's `Open`, `Day's high` or `Day's low`. They look like a re-broadcast of the
day's summary rather than trades. Under the recorded contract they are price events (D-2),
with event time 01:00 (D-3). That has two consequences:

- A batch run that admits everything puts them in a 01:05 window ahead of the day's trading,
  and every later EMA of those 1,812 symbols changes. They account for all 5,506 events
  with a bound above 264 ms.
- A streaming run receives them some 18 hours after that window closed, so it rejects them
  as late at any realistic bound. The 60 s run rejects exactly these 5,506 events and
  nothing else.

Compared with each other, the two reference runs differ in 684,661 windows beyond 1e-9 in
those 1,812 symbols, and in 3.7% of advisories. **So the correctness check (#9) must use a
bounded run, `--watermark-bound-ms` equal to the job's bound, not the admit-all output.**
Whether these rows should be price events at all is a question for the timestamp policy
(D-10, #4). If that policy drops them, both implementations should drop them in parsing, and
the admit-all and bounded runs will then agree.

**Other disorder is small.** Apart from the burst, 352 events in the week arrive after the
maximum event time has passed their window's end, and 264 ms admits all of them. This is
evidence for D-5, not a decision: the job's watermark trails this source-order model.

**Precision.** On the largest index values (about 1.1e6), binary64 rounding alone moves an
EMA up to 2.3e-9 from the exact value, and 1,270 windows in 6 symbols exceed the 1e-9 tolerance
of D-18 for that reason alone. The advisories are unaffected: every BUY/SELL decision is the same
in exact and binary64 arithmetic, and no window comes within 1e-9 of a tie.

**Determinism.** Re-running the weekday gave identical file and content checksums for every
output.
