"""Rewrite day files with bounded disorder and late events, deterministically from a seed.

Bounded disorder. Let M_i be the largest event time among the price rows up to and including
row i in source order. Each price row gets a random delay d_i in [0, B] and the sort key
c_i = M_i + d_i; every other row gets c_i = M_i. Rows are written in order of (c_i, i). A
price row whose (symbol, event time) occurred earlier takes c_i = max(c_i, c_earlier), so
equal-timestamp events of a symbol keep their order and every window keeps its close (D-23).

Since M_i <= c_i <= M_i + B, a row written before row i has an event time of at most M_i + B.
In the oracle's admission model each event therefore needs at most B more watermark bound
than in the original file. A run whose bound covers the original's need plus B admits every
event, and its results must equal those of the original input. Keys never fall below the
running maximum, so rows stream through a heap holding about B of event time.

Late events. A price row is chosen with probability `late_rate` and held back until the rows
already written reach an event time of at least its window end plus L. The model then needs a
bound above L to admit it; `late_events.parquet` lists each one with the bound it needs. A row
still held at the end of its file is written there, and listed only if that makes it late.

Event times follow the oracle's default missing-date policy (D-25). Under its `drop` policy the
bound claim does not hold, because rows the oracle drops still raise M_i here.

Each file is rewritten under its own name, so the oracle and the replayer order the outputs
as they ordered the inputs. Rows never move between files.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import heapq
import json
import random
import re
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path

import polars as pl

WINDOW_MS = 300_000
COL_ID, COL_DATE, COL_LAST, COL_TRADING_TIME, COL_TRADING_DATE = 0, 2, 21, 23, 26
NO_TIME = -(1 << 62)
LATE_SCHEMA = {"sourceFile": pl.String, "sourceLine": pl.Int64, "symbol": pl.String, "minBoundMs": pl.Int64}
# Keep equal to oracle/ema_oracle/parse.py, or the injector moves rows the oracle reads differently.
_PRICE = re.compile(rb"-?[0-9]+(\.[0-9]+)?")
_DATE = re.compile(rb"([0-9]{2})-([0-9]{2})-([0-9]{4})")
_FILE_DATE = re.compile(r"(\d{2})-(\d{2})-(\d{2})(?!\d)")
_TIME = re.compile(rb"([0-9]{2}):([0-9]{2}):([0-9]{2})(?:\.([0-9]+))?")


@dataclass
class Options:
    disorder_ms: int = 0
    late_rate: float = 0.0
    late_by_ms: int = 0
    seed: int = 0


_epoch_days: dict[bytes, int | None] = {}


def _epoch_day(text: bytes) -> int | None:
    if text not in _epoch_days:
        m = _DATE.fullmatch(text)
        try:
            _epoch_days[text] = (dt.date(int(m[3]), int(m[2]), int(m[1])) - dt.date(1970, 1, 1)).days if m else None
        except ValueError:
            _epoch_days[text] = None
    return _epoch_days[text]


def price_event(line: bytes) -> tuple[bytes, int] | None:
    """(symbol, event time in ms) of a price row (D-2, D-3, D-21, D-25, D-26), else None.

    The time is local to the trading timestamps. A zone offset that is a whole number of
    5-minute windows shifts every event equally and keeps the windows aligned, so ordering,
    windows and late bounds do not need it. Other offsets would misplace window ends.
    """
    f = line.rstrip(b"\r").split(b",")
    if not (len(f) == 39 or (len(f) == 40 and not f[39])) or not f[COL_ID] or f[0].startswith(b"#"):
        return None
    if not _PRICE.fullmatch(f[COL_LAST]) or float(f[COL_LAST]) <= 0:
        return None
    day = _epoch_day(f[COL_TRADING_DATE] or f[COL_DATE])
    t = _TIME.fullmatch(f[COL_TRADING_TIME])
    if day is None or t is None:
        return None
    hh, mm, ss = int(t[1]), int(t[2]), int(t[3])
    if hh > 23 or mm > 59 or ss > 59:
        return None
    ms = int(t[4][:3].ljust(3, b"0")) if t[4] else 0
    return f[COL_ID], day * 86_400_000 + hh * 3_600_000 + mm * 60_000 + ss * 1000 + ms


def _disorder(lines: Iterable[bytes], bound_ms: int, rng: random.Random, state: dict) -> Iterator[tuple[bytes, tuple[bytes, int] | None]]:
    heap: list[tuple[int, int, bytes, tuple[bytes, int] | None]] = []
    last_key: dict[tuple[bytes, int], int] = {}
    running_max = state["max"]
    prune_at = 1_000_000
    for i, line in enumerate(lines):
        event = price_event(line)
        if event is not None:
            running_max = max(running_max, event[1])
            key = running_max + (rng.randint(0, bound_ms) if bound_ms else 0)
            key = max(key, last_key.get(event, key))
            last_key[event] = key
        else:
            key = running_max
        while heap and heap[0][0] <= running_max:
            _, _, out, ev = heapq.heappop(heap)
            yield out, ev
        if key == running_max and not heap:
            yield line, event
        else:
            heapq.heappush(heap, (key, i, line, event))
        if len(last_key) > prune_at:
            last_key = {k: v for k, v in last_key.items() if v > running_max}
            prune_at = max(1_000_000, 2 * len(last_key))
    while heap:
        _, _, out, ev = heapq.heappop(heap)
        yield out, ev
    state["max"] = running_max


def _hold_late(rows: Iterable[tuple[bytes, tuple[bytes, int] | None]], opts: Options, rng: random.Random, state: dict) -> Iterator[tuple[bytes, dict | None]]:
    held: list[tuple[int, int, bytes, tuple[bytes, int], int]] = []
    written_max = state["max"]
    for n, (line, event) in enumerate(rows):
        if event is not None and opts.late_rate and rng.random() < opts.late_rate:
            window_end = (event[1] // WINDOW_MS + 1) * WINDOW_MS
            heapq.heappush(held, (window_end + opts.late_by_ms, n, line, event, window_end))
            continue
        yield line, None
        if event is not None:
            written_max = max(written_max, event[1])
        while held and held[0][0] <= written_max:
            _, _, out, (symbol, _), window_end = heapq.heappop(held)
            yield out, {"symbol": symbol.decode(), "minBoundMs": written_max - window_end + 1}
    for _, _, out, (symbol, event_time), window_end in sorted(held, key=lambda h: h[1]):
        bound = written_max - window_end + 1
        yield out, ({"symbol": symbol.decode(), "minBoundMs": bound} if bound > 0 else None)
        written_max = max(written_max, event_time)
    state["max"] = written_max


def _lines(path: Path) -> Iterator[bytes]:
    with path.open("rb") as f:
        for line in f:
            yield line[:-1] if line.endswith(b"\n") else line


def _md5(path: Path) -> str:
    h = hashlib.md5()
    with path.open("rb") as f:
        while block := f.read(16 << 20):
            h.update(block)
    return h.hexdigest()


def source_order(path: Path) -> tuple[dt.date, str]:
    """Files in the oracle's and the replayer's order: by the DD-MM-YY date in the name."""
    matches = _FILE_DATE.findall(path.name)
    if not matches:
        raise ValueError(f"cannot order {path.name}: no DD-MM-YY date in the file name")
    d, m, y = matches[-1]
    return dt.date(2000 + int(y), int(m), int(d)), path.name


def inject(inputs: list[Path], out: Path, opts: Options) -> dict:
    """Rewrite `inputs`, taken in source order, into `out`."""
    if out.resolve() in {p.resolve().parent for p in inputs}:
        raise ValueError("the output directory must differ from the inputs' directory")
    names = [p.name for p in inputs]
    if len(set(names)) != len(names):
        raise ValueError("input file names must be unique: each is rewritten under its own name")
    if out.exists() and any(out.iterdir()):
        raise ValueError(f"{out} is not empty: the replayer would also read the files already there")
    out.mkdir(parents=True, exist_ok=True)
    disorder_rng, late_rng = random.Random(f"{opts.seed}:disorder"), random.Random(f"{opts.seed}:late")
    disorder_state, late_state = {"max": NO_TIME}, {"max": NO_TIME}
    files, late_rows = [], []
    for path in sorted(inputs, key=source_order):
        target = out / path.name
        with target.open("wb") as f:
            rows = _hold_late(_disorder(_lines(path), opts.disorder_ms, disorder_rng, disorder_state), opts, late_rng, late_state)
            for line_no, (line, late) in enumerate(rows, start=1):
                f.write(line + b"\n")
                if late is not None:
                    late_rows.append({"sourceFile": path.name, "sourceLine": line_no, **late})
        files.append({"file": path.name, "inputMd5": _md5(path), "outputMd5": _md5(target)})

    # Not a .csv: the replayer would read it as a day file.
    pl.DataFrame(late_rows, schema=LATE_SCHEMA).write_parquet(out / "late_events.parquet")
    manifest = {
        "tool": "ema_check.inject",
        "options": {"disorderMs": opts.disorder_ms, "lateRate": opts.late_rate, "lateByMs": opts.late_by_ms, "seed": opts.seed},
        "files": files,
        "lateEvents": len(late_rows),
    }
    (out / "injection.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest
