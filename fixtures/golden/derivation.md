# Hand-worked EMA and advisory sequences

Ground truth for the EMA and crossover semantics, worked by hand so that the Flink
operator and the oracle can be checked against arithmetic nobody generated with the
implementation under test.

`analysis/src/golden.py` recomputes these and writes `synthetic.json` and `real.json`.
It asserts the properties claimed below, so the fixtures and this document cannot drift
apart silently.

## Definitions

For a window *n* with closing price *c* and smoothing factor over *j* windows:

```
alpha_j    = 2 / (1 + j)
EMA_j(n)   = c * alpha_j + EMA_j(n-1) * (1 - alpha_j)
EMA_j(0)   = 0
```

```
alpha_38  = 2/39  = 0.0512820512820512820...
alpha_100 = 2/101 = 0.0198019801980198019...
```

An advisory is evaluated on consecutive **emitted** windows — windows with no trade are
skipped entirely and are not "previous window" for this purpose:

```
BUY   when  EMA38(n-1) <= EMA100(n-1)  and  EMA38(n) >  EMA100(n)
SELL  when  EMA38(n-1) >= EMA100(n-1)  and  EMA38(n) <  EMA100(n)
```

The non-strict inequality on the previous window is what makes the zero-initialised first
window fire: `EMA38(0) = EMA100(0) = 0` satisfies `<=`, so any positive close produces a
BUY. That is a consequence of the spec, not a modelling choice — see question 3 in the
draft questions for the course staff.

## Sequence 1: synthetic (`synthetic.json`)

Fifty windows, chosen to exercise every branch. The close is held constant within each
phase on purpose: with a fixed close the recurrence collapses to

```
EMA_j(n) = c * (1 - (1 - alpha_j)^n)
```

so any row can be checked directly rather than by carrying rounding through fifty steps.

- **Windows 1-24, close = 100.00.** Window 1 emits the forced BUY. EMA38 then stays above
  EMA100 for the whole phase, so no advisory repeats — this is the required interval
  without a repeated advisory.
- **Windows 25-50, close = 5.00.** Both averages decay toward the new level, EMA38 faster.
  It crosses below EMA100 at window 46, emitting the SELL.

| n | close | EMA38 | EMA100 | advisory |
|---|---|---|---|---|
| 1 | 100.00 | 5.1282051282 | 1.9801980198 | **BUY** |
| 2 | 100.00 | 9.9934253780 | 3.9211841976 | |
| 3 | 100.00 | 14.6091471535 | 5.8237350056 | |
| 23 | 100.00 | 70.2042932058 | 36.8726034682 | |
| 24 | 100.00 | 71.7322781696 | 38.1226509243 | |
| 25 | 5.00 | 68.3101100584 | 37.4667568466 | |
| 26 | 5.00 | 65.0634377477 | 36.8238507704 | |
| 44 | 5.00 | 28.2851099125 | 27.2024808396 | |
| 45 | 5.00 | 27.0910017118 | 26.7628277537 | |
| 46 | 5.00 | 25.9581298292 | 26.3318806694 | **SELL** |
| 47 | 5.00 | 24.8833539405 | 25.9094671908 | |
| 50 | 5.00 | 21.9785655043 | 24.6917552305 | |

### Worked by hand

**Window 1**, from `EMA38(0) = EMA100(0) = 0`, close 100:

```
EMA38(1)  = 100 * 2/39  + 0 * 37/39  = 200/39  = 5.128205128205...
EMA100(1) = 100 * 2/101 + 0 * 99/101 = 200/101 = 1.980198019802...
```

`EMA38(0) <= EMA100(0)` holds (both 0) and `EMA38(1) > EMA100(1)`, so **BUY**.

**Window 2**, close 100:

```
EMA38(2)  = 100 * 2/39  + 5.128205128205 * 37/39  = 5.128205 + 4.865220 = 9.993425378...
EMA100(2) = 100 * 2/101 + 1.980198019802 * 99/101 = 1.980198 + 1.940986 = 3.921184198...
```

`EMA38(1) > EMA100(1)`, so neither inequality's first clause holds: **no advisory**. This
is the start of the quiet interval.

**Closed form check at window 24**, close 100 throughout:

```
EMA38(24)  = 100 * (1 - (37/39)^24)  = 100 * (1 - 0.282677...) = 71.7322781696
EMA100(24) = 100 * (1 - (99/101)^24) = 100 * (1 - 0.618773...) = 38.1226509243
```

matching the table, which confirms 24 steps of the recurrence carry no drift.

**Window 46**, the crossover. Phase 2 starts from `EMA38(24) = 71.7322781696` and
`EMA100(24) = 38.1226509243` and holds close 5, so after *m* windows of phase 2:

```
EMA38(24+m)  = 5 + (71.7322781696 - 5) * (37/39)^m
EMA100(24+m) = 5 + (38.1226509243 - 5) * (99/101)^m
```

At *m* = 21 (window 45): `27.0910017118 > 26.7628277537`, still bullish.
At *m* = 22 (window 46): `25.9581298292 < 26.3318806694`.

`EMA38(45) >= EMA100(45)` and `EMA38(46) < EMA100(46)`, so **SELL**.

The crossover takes 22 windows because that is genuinely how long the two smoothing
factors take to converge from that separation — solving
`66.73 * (37/39)^m = 33.12 * (99/101)^m` gives *m* ≈ 21.4. It is not a number chosen to
make the fixture short.


## Sequence 2: real symbol (`real.json`)

The busiest symbol in the sampled slice, `A0X7J3.ETR` (`secType` = `I`),
through the same code path. The synthetic sequence proves the branches; this one proves
that the parser, the window assignment and the arithmetic agree on data that actually
exists.

| n | windowEnd | close | EMA38 | EMA100 | advisory |
|---|---|---|---|---|---|
| 1 | 1636362900000 | 19473.560000 | 998.6441025641 | 385.6150495050 | **BUY** |
| 2 | 1636363200000 | 19481.400000 | 1946.4777383300 | 763.7494049603 |  |
| 3 | 1636364100000 | 19463.780000 | 2844.8009312362 | 1134.0470405056 |  |
| 4 | 1636364400000 | 19461.490000 | 3696.9388321984 | 1496.9667030699 |  |
| 5 | 1636365600000 | 19472.200000 | 4505.9265843934 | 1852.9119168705 |  |
| 6 | 1636365900000 | 19469.930000 | 5273.3113749373 | 2201.7637601008 |  |
| 7 | 1636366800000 | 19466.810000 | 6001.1830992995 | 2543.6458638611 |  |
| 8 | 1636367100000 | 19470.700000 | 6691.9275557457 | 2878.8350546758 |  |

Three things worth reading off it:

**The zero-initialisation artefact is visible at real prices.** The close is around
19,473 but EMA38 after one window is only 998.64 — exactly `19473.56 * 2/39`. Both
averages spend many windows climbing from 0 toward the price level. The window-1 BUY is
produced by that warm-up, not by anything in the market. This is the effect flagged in
D-7.

**Windows with no trade are absent, not zero-filled.** Between window 2
(1636363200000) and window 3 (1636364100000) the gap is 900,000 ms — three
window widths, so two five-minute windows carried no trade and were skipped entirely.
Across these 16 emitted windows, 20 windows were skipped. The EMA is
unchanged across such a gap and no crossover is evaluated, which is decision D-6 observed
on real data rather than asserted.

**The close is chosen by event time, not by arrival.** Each window's close is
`max by (event_time, source_position)` over that window's trades — the rule D-4 and D-21
fix, and the reason `profile.py` records a source position at all.

Regenerate with `analysis/run.sh analysis/src/golden.py`. The symbol is whichever has the
most price events in the slice, so a different sample may select a different one; set
`GOLDEN_SYMBOL` to pin it.
