import re
import tempfile
from itertools import islice
from pathlib import Path

import polars as pl


DAY_MS = 86_400_000
WINDOW_MS = 300_000
PARTITION_COUNTS = (1, 2, 4, 8, 16, 32)
BATCH_LINES = 100_000
ROW_GROUP_SIZE = 50_000


def offset_ms(value):
    if value == "Z":
        return 0
    match = re.fullmatch(r"([+-])(\d{2}):(\d{2})", value)
    if not match:
        raise ValueError("offset must be Z or +/-HH:MM")
    hours, minutes = map(int, match.groups()[1:])
    if minutes > 59 or hours > 18 or (hours == 18 and minutes):
        raise ValueError("offset must be within +/-18:00")
    return (1 if match[1] == "+" else -1) * (hours * 60 + minutes) * 60_000


def timestamp(date, time, zone):
    day = pl.when(date.str.contains(r"^\d{2}-\d{2}-\d{4}$")).then(
        date.str.strptime(pl.Date, "%d-%m-%Y", strict=False)
    ).cast(pl.Int64)
    parts = time.str.extract_groups(r"^(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?$")
    hour, minute, second = [parts.struct.field(str(i)).cast(pl.Int64, strict=False) for i in (1, 2, 3)]
    fraction = parts.struct.field("4").str.slice(0, 3).str.pad_end(3, "0").cast(pl.Int64, strict=False).fill_null(0)
    return pl.when((hour < 24) & (minute < 60) & (second < 60)).then(
        day * DAY_MS + hour * 3_600_000 + minute * 60_000 + second * 1000 + fraction - zone
    )


def classify(lines, zone):
    fields = lines.with_columns(_fields=pl.col("line").fill_null("").str.split(","))
    f = lambda i: pl.col("_fields").list.get(i, null_on_oob=True).fill_null("")
    line = pl.col("line").fill_null("")
    size = pl.col("_fields").list.len()
    structural = ((size == 39) | ((size == 40) & (f(39) == ""))) & (f(0) != "") & ~line.str.contains('"', literal=True)
    price = f(21).cast(pl.Float64, strict=False)
    valid_price = f(21).str.contains(r"^[+-]?\d+(?:\.\d+)?$") & price.is_finite()
    event_date = pl.when(f(26) != "").then(f(26)).otherwise(f(2))
    payload_fields = pl.col("_fields").list.slice(4, 35).list.eval(pl.element() != "").list.sum()
    category = (
        pl.when(line == "").then(pl.lit("blank"))
        .when(line.str.starts_with("#")).then(pl.lit("comment"))
        .when(line.str.starts_with("ID,SecType,")).then(pl.lit("header"))
        .when(~structural).then(pl.lit("malformed_row"))
        .when((f(21) != "") & ~valid_price.fill_null(False)).then(pl.lit("malformed_price"))
        .when(price > 0).then(pl.lit("price"))
        .when(price == 0).then(pl.lit("price_zero"))
        .when(price < 0).then(pl.lit("price_negative"))
        .when((f(4) != "") | (f(6) != "")).then(pl.lit("quote"))
        .when(payload_fields == 0).then(pl.lit("empty_payload"))
        .otherwise(pl.lit("other"))
    )
    return fields.select(
        "source_line", category.alias("category"), f(0).alias("symbol"), f(1).alias("sec_type"),
        f(0).str.extract(r"\.([A-Za-z]+)$", 1).alias("exchange"),
        timestamp(f(2), f(3), zone).alias("system_ms"),
        timestamp(event_date, f(23), zone).alias("event_ms"),
        price.alias("price"),
        (f(26) == "").alias("missing_trading_date"),
        (f(23) == "").alias("missing_trading_time"),
        ((f(26) == "") & (f(2) == "")).alias("missing_all_dates"),
        ((f(26) != "") & (f(2) != "") & (f(26) != f(2))).alias("date_mismatch"),
        f(23).str.extract(r"\.(\d+)$", 1).str.len_chars().alias("trading_fraction_digits"),
        f(3).str.extract(r"\.(\d+)$", 1).str.len_chars().alias("system_fraction_digits"),
        f(21).str.extract(r"\.(\d+)$", 1).str.len_chars().alias("price_fraction_digits"),
        ((f(4) != "") & ~f(4).cast(pl.Float64, strict=False).is_finite().fill_null(False)).alias("malformed_ask"),
        ((f(6) != "") & ~f(6).cast(pl.Float64, strict=False).is_finite().fill_null(False)).alias("malformed_bid"),
        ~f(0).str.contains(r"^[A-Za-z0-9]+\.[A-Za-z]+$").alias("unexpected_symbol"),
        pl.any_horizontal([price == f(i).cast(pl.Float64, strict=False) for i in (19, 13, 17)]).fill_null(False).alias("matches_open_high_low"),
        (f(23) == "01:00:00.000").alias("trading_at_0100"),
        size.alias("field_count"), payload_fields.alias("payload_fields"),
    )


def scan_reference(path: Path, zone: int):
    lines = pl.scan_csv(path, has_header=False, separator="\x1f", quote_char=None,
                        schema={"line": pl.String}, encoding="utf8", low_memory=True)
    return classify(lines.with_row_index("source_line", offset=1), zone)


def parse_to_parquet(source: Path, destination: Path, zone: int, batch_lines=BATCH_LINES):
    with tempfile.TemporaryDirectory(prefix="parse-", dir=destination.parent) as temporary:
        parts = Path(temporary)
        source_line = 1
        with source.open(encoding="utf-8", newline="\n") as stream:
            for batch, lines in enumerate(iter(lambda: list(islice(stream, batch_lines)), [])):
                frame = pl.DataFrame({"line": [line.rstrip("\r\n") for line in lines]}).lazy()
                classify(frame.with_row_index("source_line", offset=source_line), zone).sink_parquet(
                    parts / f"{batch:08d}.parquet", compression="zstd", row_group_size=ROW_GROUP_SIZE,
                    engine="streaming",
                )
                source_line += len(lines)
        if source_line == 1:
            raise ValueError("input contains no physical lines")
        pl.scan_parquet(str(parts / "*.parquet")).sink_parquet(
            destination, compression="zstd", row_group_size=ROW_GROUP_SIZE, engine="streaming",
        )


def kafka_hash(symbol):
    data = symbol.encode("utf-8")
    mask = 0xFFFFFFFF
    h = (0x9747B28C ^ len(data)) & mask
    end = len(data) - len(data) % 4
    for i in range(0, end, 4):
        k = int.from_bytes(data[i:i + 4], "little")
        k = (k * 0x5BD1E995) & mask
        k ^= k >> 24
        k = (k * 0x5BD1E995) & mask
        h = ((h * 0x5BD1E995) & mask) ^ k
    tail = data[end:]
    if tail:
        h ^= int.from_bytes(tail, "little")
        h = (h * 0x5BD1E995) & mask
    h ^= h >> 13
    h = (h * 0x5BD1E995) & mask
    h ^= h >> 15
    return h & 0x7FFFFFFF
