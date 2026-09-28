# Interfaces

> Measured evidence for these formats, and the corrections D-19 to D-23 make to
> them, are in [data-contract.md](data-contract.md).

These are the contracts between the replayer (`producer/`), the Flink job (`flink-job/`), the oracle (`oracle/`) and the visualization (`viz/`). Change them only through a new entry in [decisions.md](decisions.md) (D-13 to D-18 cover this document), so nobody codes against a stale format.

## Kafka topics

| Topic | Written by | Read by | Key | Value | Partitions |
|---|---|---|---|---|---|
| `ticks` | replayer | Flink job | symbol (`ID`) | CSV line, 7 raw fields | 8 locally (`TICKS_PARTITIONS`) |
| `ema-results` | Flink job | viz, correctness check | symbol | JSON | 8 |
| `advisories` | Flink job | viz, correctness check | symbol | JSON | 8 |
| `subscriptions` | subscription generator | Flink job (broadcast) | trader ID | JSON, or a tombstone | 1, compacted |
| `stats` | Flink job | (open: decision on event counts) | – | – | 1 |

All keys and string values are UTF-8. Partition counts are revisited for the cloud runs.

## `ticks` (D-13, D-14, D-15)

The replayer sends raw data and does no parsing. All parsing, timestamp normalization and event classification happen in the Flink job.

- **One record per CSV data row**, including non-price and housekeeping rows. Comment lines (`#…`) and any header row are skipped.
- **Key:** the `ID` column, as-is. The default Kafka partitioner is used.
- **Value:** a projection of 7 of the CSV's 39 columns, in this order (D-22). Fields are comma-separated, an empty field means null, and there is no quoting. Note the source rows carry 40 fields, the last one empty (D-19):

  ```
  ID,SecType,Date,Time,Last,Trading time,Trading date
  ```

  Illustrative examples, first a price event and then a quote or housekeeping event:

  ```
  RDSA.NL,E,08-11-2021,09:00:01.123,19.280000,09:00:01.100,08-11-2021
  RDSA.NL,E,08-11-2021,09:00:01.456,,,
  ```

- **Record timestamp:** the wall-clock time (epoch ms) when the replayer sent the record. This is the ingest time used for latency measurements.
- **Ordering:** the replayer keeps file order per symbol: day files in date order, lines in file order. Producer idempotence is on (the Kafka default), so retries don't reorder records.
  - Because the key is the symbol, all of a symbol's events land in one partition. **The Kafka offset is therefore the arrival order** used to break ties when picking a window's close (D-4).
  - The oracle uses (file date, line number) as the same arrival order.

## `ema-results` (D-16)

The Flink job emits one JSON record per (symbol, window) when **both** of these hold:

- the window had at least one price event (D-6), and
- at least one trader subscribes to the symbol (D-9).

```json
{"symbol":"RDSA.NL","secType":"E","windowEnd":1636362300000,"close":19.28,"ema38":19.2712,"ema100":19.2655,"maxIngestTs":1790000000123,"emitTs":1790000000456}
```

| Field | Type | Meaning |
|---|---|---|
| `symbol` | string | Symbol key (D-1). |
| `secType` | string | `E` (equity) or `I` (index), copied from the input event. The viz uses it for filtering. |
| `windowEnd` | long | Exclusive end of the 5-minute window, epoch ms under the timezone interpretation of D-3. |
| `close` | double | Price of the window's closing event (D-4). |
| `ema38`, `ema100` | double | EMA values after this window, written at full precision (no rounding). |
| `maxIngestTs` | long | Latest record timestamp among the window's price events. |
| `emitTs` | long | Wall-clock time (epoch ms) when the Flink job emitted the record. |

The last two fields support the latency definitions still under consideration. Which one is reported is an open decision.

## `advisories` (D-16)

The Flink job emits one JSON record per detected crossover:

- `BUY` when equations (1) and (2) of the spec hold (EMA₃₈ crosses above EMA₁₀₀).
- `SELL` when equations (3) and (4) hold (EMA₃₈ crosses below).

```json
{"symbol":"RDSA.NL","secType":"E","windowEnd":1636362300000,"type":"BUY","ema38":19.2712,"ema100":19.2655,"maxIngestTs":1790000000123,"emitTs":1790000000456}
```

The fields mean the same as in `ema-results`.

## `subscriptions` (D-17)

- **Key:** the trader ID, e.g. `trader-0042`.
- **Value:** `{"trader":"trader-0042","symbols":["RDSA.NL"]}`. This replaces that trader's previous list.
  - `"symbols":["*"]` subscribes to every symbol. Correctness runs use this.
  - A **tombstone** (null value) removes the trader.
- Compaction keeps the latest list per trader. A (re)started job reads the topic from the beginning to rebuild the current subscription state.
- The Flink job emits each result once per symbol, not once per trader. Delivering results to individual traders is the visualization backend's job.

## Oracle output and correctness check (D-18)

- **Oracle output:** `oracle/out/ema_results.parquet` and `oracle/out/advisories.parquet`. Both are gitignored.
  - They use the same field names as the topics, without `maxIngestTs` and `emitTs`.
  - They cover all symbols.
- **Flink side:** a dump of `ema-results` and `advisories` from a run with a `["*"]` subscription.
- **EMA check:** join on (`symbol`, `windowEnd`). A row is a mismatch if any of these holds:
  - `close` differs at all
  - `ema38` or `ema100` differs by more than **1e-9** (absolute)
  - the row exists on only one side

  Report the mismatch counts. They must all be 0.
- **Advisory check:** compare the two sets of (`symbol`, `windowEnd`, `type`). Report precision and recall. Both must be 1.0.
