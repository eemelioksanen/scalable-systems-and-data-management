# Oracle runs

Each JSON file is the `manifest.json` of one run: revision, command, parameters, input
sizes and MD5s, every count, the admission profile, the precision report, and the row
counts and SHA-256 of every output. The outputs themselves are regenerated, not committed.

All runs used the defaults: `--zone-offset Z`, `--missing-trading-date system-date`, no
watermark bound, no exclusions.

| Run | Input | Command (host) |
|---|---|---|
| [`2026-09-28-sample.json`](2026-09-28-sample.json) | the #4 stratified sample of 08-11-21, 1,015,019,421 bytes, MD5 `0b499f88f5932e1ec64f7739239cde42` | `python3 oracle/evidence/rebuild_sample.py` then `oracle/run.sh --out /out/sample data/slice/debs2022-gc-trading-day-08-11-21.slice.csv` |
| [`2026-09-28-weekday-08-11-21.json`](2026-09-28-weekday-08-11-21.json) | `debs2022-gc-trading-day-08-11-21.csv`, MD5 `a08013f09a6b77355dae8b3ce141624d` (matches Zenodo) | `oracle/run.sh --out /out/weekday data/debs2022-gc-trading-day-08-11-21.csv` |
| full week | pending download | `oracle/run.sh --out /out/week data/debs2022-gc-trading-day-*.csv` |

The sample is cut from the local weekday file by
[`rebuild_sample.py`](rebuild_sample.py), which checks all 21 chunks against the MD5s in
`analysis/out/provenance.json`, so it is byte-identical to what `fetch_slice.sh` downloads.

## Results

| | sample | weekday 08-11-21 |
|---|---|---|
| data rows | 11,784,671 | 53,994,554 |
| accepted price events | 2,389,887 | 11,794,520 |
| of which `Date` substituted (D-25) | 2,201,703 | 10,873,721 |
| `Last` = 0 (nonPositivePrice) | 17,593 | 62,212 |
| malformed, missing timestamp | 0 | 0 |
| symbols / windows | 3,905 / 123,101 | 4,840 / 373,126 |
| BUY / SELL | 3,905 / 0 | 4,840 / 0 |
| events late in source order at bound 0 / smallest bound admitting all | 5 / 25 ms | 79 / 25 ms |
| binary64 drift from exact: max / worst symbol | 5.8e-10 (13 ulps), `A1EXY2.ETR` | 1.4e-9 (24 ulps), `IC2MV.FR` |
| windows over 1e-9 on drift alone | 0 | 50 (2 symbols) |
| advisory disagreements / near ties | 0 / 0 | 0 / 0 |
| wall time / peak heap | 4 s / 1.2 GB | 17 s / 3.5 GB |

**Cross-check against the EDA.** Every sample count agrees exactly with the independent
profile in [`docs/data-contract.md`](../../docs/data-contract.md): 11,784,671 data rows;
2,389,887 price rows; 17,593 `price_zero`; 9,377,191 rows without `Last` (empty payload,
quote, other and zero-filled together); 2,201,703 price rows without `Trading date`.

**Every advisory is the zero-start BUY.** Starting from EMA₀ = 0 (D-7), EMA38 climbs faster
than EMA100 and stays above it unless the price falls by roughly the gap between them, relative to the price:
about 3% after one window, a third after 35, still 13% after 100. No symbol falls that far
on the first day, so the only advisories are the first-window BUYs. For a symbol that
trades in every window (about 108 a day), the gap is still 11% at the start of the second
day and 1% at its end, so crossovers driven by the market start during the second day.

**Determinism.** Re-running the weekday gave identical file and content checksums for every
output.
