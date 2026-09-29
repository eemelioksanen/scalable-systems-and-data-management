import sys
import tempfile
import unittest
import hashlib
import json
from pathlib import Path
from unittest.mock import patch

import polars as pl
import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from daily_parse import classify, kafka_hash, offset_ms, parse_to_parquet, scan_reference
from daily import basic_tables, rates_summary, watermark_tables
import fetch_day


def row(**values):
    fields = [""] * 39
    fields[:4] = ["TEST.FR", "E", "08-11-2021", "10:00:00.000"]
    names = {"symbol": 0, "sec_type": 1, "system_time": 3, "last": 21, "trading_time": 23, "trading_date": 26, "ask": 4, "isin": 14}
    for key, value in values.items():
        fields[names[key]] = value
    return ",".join(fields) + ","


class DailyParsingTests(unittest.TestCase):
    def test_kafka_hashes_match_java_reference(self):
        references = pl.read_csv(Path(__file__).with_name("kafka_hashes.csv"))
        for symbol, expected in references.iter_rows():
            self.assertEqual(kafka_hash(symbol), expected, symbol)

    def parse(self, rows):
        return classify(pl.DataFrame({"line": rows}).lazy().with_row_index("source_line", offset=1), offset_ms("+02:00")).collect()

    def test_taxonomy_checks_every_payload_field_and_separates_bad_prices(self):
        result = self.parse([row(), row(isin="FR123"), row(ask="2"), row(last="0"),
                             row(last="-1"), row(last="NaN"), row(last="inf"),
                             row(last="10", trading_time="10:00:00.001")])
        self.assertEqual(result["category"].to_list(), ["empty_payload", "other", "quote", "price_zero",
                                                       "price_negative", "malformed_price", "malformed_price", "price"])

    def test_fallback_keeps_explicit_stale_date_and_rejects_invalid_time(self):
        result = self.parse([row(last="10", trading_time="10:00:00.123"),
                             row(last="11", trading_time="10:00:00.123", trading_date="07-11-2021"),
                             row(last="12", trading_time="25:00:00.000"),
                             row(last="13", trading_time="10:00:00.000", trading_date="31-02-2021")])
        first, stale, bad_time, bad_date = result["event_ms"].to_list()
        self.assertEqual(first, 1636358400123)
        self.assertEqual(first - stale, 86_400_000)
        self.assertTrue(result["date_mismatch"][1])
        self.assertIsNone(bad_time)
        self.assertIsNone(bad_date)

    def test_physical_source_positions_survive_blank_and_comment_lines(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "rows.csv"
            path.write_bytes(("# embedded\rreturn\r\n\n" + row(last="1", trading_time="10:00:00.000") + "\n").encode())
            result = scan_reference(path, 0).collect()
            self.assertEqual(result["source_line"].to_list(), [1, 2, 3])
            self.assertEqual(result["category"].to_list(), ["comment", "blank", "price"])
            output = Path(tmp) / "rows.parquet"
            parse_to_parquet(path, output, 0, batch_lines=2)
            self.assertTrue(pl.read_parquet(output).equals(result))


class WatermarkTests(unittest.TestCase):
    def test_window_boundary_ties_sparse_symbol_and_close_loss(self):
        with tempfile.TemporaryDirectory() as tmp:
            con = duckdb.connect()
            con.execute("CREATE TABLE prices(source_line BIGINT,symbol VARCHAR,event_ms BIGINT,system_ms BIGINT,price DOUBLE,window_end BIGINT)")
            con.executemany("INSERT INTO prices VALUES (?,?,?,?,?,?)", [
                (1,"A.FR",299999,299999,10,300000),
                (2,"B.FR",300024,300024,100,600000),
                (3,"A.FR",299999,300025,11,300000),
                (4,"A.FR",1,300026,7,300000),
                (5,"C.FR",299999,300027,8,300000),
                (6,"A.FR",300000,300028,20,600000),
            ])
            con.execute("CREATE TABLE hashes(symbol VARCHAR, hash BIGINT)")
            con.executemany("INSERT INTO hashes VALUES (?,?)", [("A.FR",0),("B.FR",1),("C.FR",0)])
            con.execute("""CREATE TABLE windows AS SELECT symbol,window_end,
                arg_max(price,struct_pack(t:=event_ms,p:=source_line)) AS close
                FROM prices GROUP BY symbol,window_end""")
            watermark_tables(con,Path(tmp),Path(tmp))
            results=pl.read_csv(Path(tmp)/"watermark_bounds.csv")
            zero=results.filter((pl.col("partitions")==1)&(pl.col("bound_ms")==0)).row(0,named=True)
            self.assertEqual((zero["discarded_events"],zero["missing_windows"],zero["changed_existing_closes"]),(3,1,1))
            twenty_five=results.filter((pl.col("partitions")==1)&(pl.col("bound_ms")==25)).row(0,named=True)
            self.assertEqual(twenty_five["discarded_events"],0)
            split=results.filter((pl.col("partitions")==2)&(pl.col("bound_ms")==0)).row(0,named=True)
            self.assertEqual(split["discarded_events"],0)
            con.close()


class DownloadTests(unittest.TestCase):
    def test_completed_partial_is_verified_and_promoted_without_another_transfer(self):
        with tempfile.TemporaryDirectory() as tmp:
            name="debs2022-gc-trading-day-08-11-21.csv"
            content=b"complete download\n"
            partial=Path(tmp)/(name+".partial")
            partial.write_bytes(content)
            metadata={"files":[{"key":name,"size":len(content),"checksum":"md5:"+hashlib.md5(content).hexdigest()}]}
            with patch.object(sys,"argv",["fetch_day.py","2021-11-08","--data-dir",tmp]), \
                 patch.object(fetch_day.subprocess,"check_output",return_value=json.dumps(metadata)), \
                 patch.object(fetch_day.subprocess,"run") as transfer:
                fetch_day.main()
                transfer.assert_not_called()
            self.assertEqual((Path(tmp)/name).read_bytes(),content)
            self.assertFalse(partial.exists())


class DenominatorTests(unittest.TestCase):
    def test_rate_p99_includes_unobserved_seconds(self):
        con=duckdb.connect()
        con.execute("CREATE TABLE seconds(exchange VARCHAR,second BIGINT,events BIGINT,price_events BIGINT)")
        con.execute("INSERT INTO seconds VALUES ('FR',0,2,1),('FR',200,1,1)")
        result=con.execute(rates_summary("seconds")).fetchall()
        for values in result:
            self.assertEqual(values[3:6],(201,199,3))
            self.assertEqual(values[7:9],(2,0))
        con.close()

    def test_window_denominators_include_zero_price_symbols_and_count_each_key_once(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            lines=[row(symbol="S.FR",sec_type="E",last="10",trading_time="10:00:00.000"),
                   row(symbol="S.FR",sec_type="I",system_time="10:10:00.000",last="20",trading_time="10:10:00.000"),
                   row(symbol="Q.FR",system_time="10:05:00.000",ask="1")]
            classify(pl.DataFrame({"line":lines}).lazy().with_row_index("source_line",offset=1),0).collect().write_parquet(root/"rows.parquet")
            con=duckdb.connect()
            con.execute("CREATE TABLE rows AS SELECT * FROM read_parquet(?)",[str(root/"rows.parquet")])
            con.execute("""CREATE TABLE prices AS SELECT source_line,symbol,exchange,sec_type,event_ms,system_ms,price,
                (event_ms//300000+1)*300000 AS window_end FROM rows WHERE category='price' AND event_ms IS NOT NULL""")
            basic_tables(con,root,root,1636329600000)
            result=pl.read_csv(root/"window_statistics.csv")
            self.assertEqual(result.height,2)
            by_symbol={r["symbol"]:r for r in result.iter_rows(named=True)}
            self.assertEqual(by_symbol["S.FR"]["sec_type"],"E|I")
            self.assertEqual(by_symbol["S.FR"]["empty_calendar_windows"],286)
            self.assertEqual(by_symbol["S.FR"]["empty_observed_exchange_windows"],1)
            self.assertEqual(by_symbol["Q.FR"]["empty_calendar_windows"],288)
            self.assertEqual(by_symbol["Q.FR"]["empty_observed_exchange_windows"],3)
            con.close()


if __name__ == "__main__":
    unittest.main()
