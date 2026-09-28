# Questions for the course staff

Drafted from the week-1 inspection of the DEBS 2022 trading data (see
[data-contract.md](data-contract.md)). **These are sent by a team member, not automatically.**

Each question states what the spec says, why it is ambiguous, the assumption we are
coding against in the meantime, and what would have to change if the answer differs.
Where a question is already settled by evidence rather than by interpretation, it is
marked as such and is asked only for confirmation.

---

## 1. Empty windows

**Spec:** EMA is defined per symbol over 5-minute tumbling windows, with the window's
closing price as input.

**Ambiguity:** the spec does not say what happens when a symbol has no trade in a window.
Two readings are defensible: skip the window entirely (no EMA update, no output, no
crossover check), or treat the window as having occurred and carry the previous close
forward (EMA updates, and a crossover can fire with no trade behind it).

**Our assumption:** skip. A window with no trade produces no output and leaves EMA state
untouched (decision D-6).

**Impact if wrong:** the advisory count changes materially. Most symbols in this dataset
are thinly traded, so carry-forward would emit EMA updates and crossovers for long
stretches in which nothing was actually traded. It also changes the meaning of "previous
window" in the crossover inequalities.

---

## 2. Overnight and weekend state

**Spec:** the EMA recurrence is defined over consecutive windows, without reference to
trading sessions.

**Ambiguity:** whether EMA state resets at the start of each trading day, or carries
across the overnight gap and the weekend. The dataset spans Mon 08.11 to Sun 14.11.2021,
so this arises six times.

**Our assumption:** state carries. There is no daily reset (decision D-8).

**Impact if wrong:** a daily reset re-triggers the zero-initialisation effect in
question 3 every morning, producing a BUY for every actively traded symbol at the start
of every day. That is a large, systematic difference in the advisory set.

---

## 3. Zero-initialised EMAs and the first window

**Spec:** EMA_j^(0) = 0.

**Consequence we want confirmed:** with both EMAs starting at 0, the first window
carrying any positive close gives EMA38 > EMA100 (because 2/39 > 2/101), while the
previous values were equal at 0. Under the crossover inequalities that is a BUY. So
*every* symbol emits a BUY in its first non-empty window, with no market meaning.

**Question:** is that intended and expected in our output, or should the first window be
treated as warm-up and suppressed?

**Our assumption:** intended. We implement EMA_0 = 0 literally, emit the advisory, and
flag it in the report and the visualisation rather than suppressing it.

**Impact if wrong:** roughly one spurious BUY per traded symbol, which dominates the
advisory counts on day 1 and would otherwise skew any precision/recall comparison.

---

## 4. Event order and the window close

**Spec:** §2.1 notes that ordering across exchanges is not guaranteed.

**Ambiguity:** "the closing price of a window" is not defined for the case where trades
for one symbol arrive out of order. Is the close the trade with the latest *trading
time*, or the last one to *arrive*?

**Our assumption:** the trade with the latest `Trading time`. Ties are broken by source
position, which for us is the Kafka offset (the replayer preserves file order per symbol,
and the symbol is the partition key).

**Evidence:** our sampled weekday shows how often this is actually contended — see the
ordering section of [data-contract.md](data-contract.md). If per-symbol trade time is
already monotonic in file order, the two readings coincide and the question is moot for
this dataset.

---

## 5. Late events versus evaluating the next window

**Spec:** a window is evaluated when the next one starts.

**Ambiguity:** "when the next one starts" is a statement about event time, but a streaming
system only learns that a window is complete from a watermark. If a trade for window *w*
arrives after *w* has been evaluated, do we (a) drop it, (b) restate *w* and any advisory
derived from it, or (c) fold it into the next window?

**Our assumption:** (a) drop, count, and report. Late events go to a side output and the
count is published, so the cost of the choice is measurable rather than hidden.

**Impact if wrong:** (b) means emitting retractions, which changes the output contract
and the downstream visualisation. We would rather know now than retrofit it.

---

## 6. Completing the final window

**Ambiguity:** at the end of the input there is no "next window" to trigger evaluation of
the last one. Should the final partially-observed window per symbol be evaluated and
emitted, or discarded?

**Our assumption:** emit it, by advancing the watermark to +infinity at end of input.
This is what Flink does by default at the end of a bounded source.

**Impact if wrong:** a small, systematic difference of at most one window per symbol
between our output and the reference — enough to make a correctness diff fail even when
everything else is right, which is why we would like it pinned down.

---

## 7. Time zone wording for November dates

**Spec:** the task text refers to **CEST**.

**Problem:** Central European Summer Time ended on 31 October 2021. Every date in this
dataset (08.-14.11.2021) falls in **CET (UTC+1)**, not CEST (UTC+2). Taking "CEST"
literally would shift every timestamp by one hour relative to the local trading day.

**Question:** should we read "CEST" as "the local exchange time zone" — that is, CET for
these dates — or literally as UTC+2?

**Our assumption:** local exchange time, i.e. CET/UTC+1 for this dataset.

**Why it matters for windowing, and why it is limited:** the spec aligns 5-minute windows
to midnight. Any whole-hour offset preserves that alignment, so the *set* of window
boundaries is identical under either reading and no trade moves to a different window
relative to its neighbours. What changes is the absolute `windowEnd` timestamp we publish
(by one hour) and which trades fall on which calendar day. We therefore want the wording
settled for the report and for any comparison against a reference implementation, but it
does not destabilise the query semantics themselves.

---

## 8. Which price field is the trade price

**Mostly settled by inspection, asked only to confirm.** The data has several
price-shaped columns: `Last`, `Close`, `Current price`, `Ask`, `Bid`, `Mid price`. We take
a price event to be a row with a non-empty, non-zero `Last`, and the 5-minute close to be
the `Last` of the window's latest trade. `Close` in this feed is the previous session's
closing price carried on housekeeping rows, not an intra-day 5-minute close. Measured
field population is in [data-contract.md](data-contract.md).

**Question:** is `Last` the intended input to the EMA, as we have assumed?
