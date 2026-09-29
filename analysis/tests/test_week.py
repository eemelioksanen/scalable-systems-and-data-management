import sys
import tempfile
import unittest
from pathlib import Path

import duckdb
import polars as pl

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "analysis/src"))
sys.path.insert(0, str(ROOT / "oracle"))

from daily import watermark_tables
from ema_oracle.ema import compute
from week import POSITION_STRIDE, compare_results, create_prices, save_closes, save_global_late


class ContinuousWeekTests(unittest.TestCase):
    def test_global_oracle_gate_can_reject_an_event_that_partition_model_admits(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            con = duckdb.connect()
            con.execute("CREATE TABLE prices(source_line BIGINT,symbol VARCHAR,event_ms BIGINT,system_ms BIGINT,price DOUBLE,window_end BIGINT)")
            con.executemany("INSERT INTO prices VALUES (?,?,?,?,?,?)", [
                (1, "A.FR", 299999, 299999, 10.0, 300000),
                (2, "B.FR", 400000, 400000, 1.0, 600000),
                (3, "A.FR", 299999, 400001, 20.0, 300000),
            ])
            con.execute("CREATE TABLE hashes(symbol VARCHAR,hash BIGINT)")
            con.execute("INSERT INTO hashes VALUES ('A.FR',0),('B.FR',1)")
            con.execute("""CREATE TABLE windows AS SELECT symbol,window_end,
                arg_max(price,struct_pack(t:=event_ms,p:=source_line)) AS close
                FROM prices GROUP BY symbol,window_end""")
            save_global_late(con, root / "global.parquet", 60000)
            self.assertEqual(pl.read_parquet(root / "global.parquet")["source_line"].to_list(), [3])
            watermark_tables(con, root, root)
            bounds = pl.read_csv(root / "watermark_bounds.csv").filter(pl.col("bound_ms") == 60000)
            self.assertEqual(bounds.filter(pl.col("partitions") == 1)["discarded_events"].item(), 1)
            self.assertEqual(bounds.filter(pl.col("partitions") == 8)["discarded_events"].item(), 0)
            con.close()

    def test_cross_file_progress_ties_and_ema_state(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            checkpoints = []
            days = [
                ("2021-11-08", [(1, "A.FR", 299999, 10.0, False), (2, "B.FR", 300024, 2.0, False)]),
                ("2021-11-09", [(1, "A.FR", 299999, 20.0, False), (2, "C.FR", 1, 5.0, True),
                                (3, "A.FR", 600001, 30.0, False)]),
            ]
            for day, records in days:
                rows = [{"source_line": line, "symbol": symbol, "sec_type": "I", "exchange": "FR",
                         "event_ms": timestamp, "system_ms": timestamp, "price": price,
                         "category": "price", "trading_at_0100": candidate,
                         "matches_open_high_low": candidate}
                        for line, symbol, timestamp, price, candidate in records]
                path = root / f"{day}.parquet"
                pl.DataFrame(rows).write_parquet(path)
                checkpoints.append((day, path))
            con = duckdb.connect()
            create_prices(con, checkpoints)
            self.assertEqual(con.execute("SELECT count(DISTINCT source_line) FROM prices").fetchone()[0], 5)
            self.assertEqual(con.execute("SELECT physical_line FROM prices WHERE source_line=?",
                                         [POSITION_STRIDE + 1]).fetchone()[0], 1)
            save_closes(con, root / "baseline.parquet")
            baseline = pl.read_parquet(root / "baseline.parquet")
            self.assertEqual(baseline.filter(pl.col("symbol") == "A.FR")["close"].to_list(), [20.0, 30.0])
            con.execute("""CREATE TABLE windows AS SELECT symbol,window_end,
                arg_max(price,struct_pack(t:=event_ms,p:=source_line)) AS close
                FROM prices GROUP BY symbol,window_end""")
            con.execute("CREATE TABLE hashes(symbol VARCHAR, hash BIGINT)")
            con.executemany("INSERT INTO hashes VALUES (?,?)", [("A.FR", 0), ("B.FR", 1), ("C.FR", 0)])
            watermark_tables(con, root, root)
            bounds = pl.read_csv(root / "watermark_bounds.csv")
            one = bounds.filter((pl.col("partitions") == 1) & (pl.col("bound_ms") == 0)).row(0, named=True)
            two = bounds.filter((pl.col("partitions") == 2) & (pl.col("bound_ms") == 0)).row(0, named=True)
            self.assertEqual((one["discarded_events"], one["missing_windows"], one["changed_existing_closes"]), (2, 1, 1))
            self.assertEqual(two["discarded_events"], 0)
            save_closes(con, root / "dropped.parquet", f"source_line NOT IN ({POSITION_STRIDE + 1},{POSITION_STRIDE + 2})")
            reference, reference_advisories = compute(baseline)
            actual, advisories = compute(pl.read_parquet(root / "dropped.parquet"))
            effects = compare_results(reference, actual, reference_advisories, advisories)
            self.assertEqual(effects["missing_windows"], 1)
            self.assertEqual(effects["changed_existing_closes"], 1)
            self.assertEqual(effects["changed_existing_emas_over_1e_minus_9"], 2)
            self.assertEqual(effects["advisories_removed"], 1)
            self.assertEqual(effects["advisories_added"], 0)
            self.assertEqual(con.execute("SELECT count(*) FROM prices WHERE summary_candidate").fetchone()[0], 1)
            con.close()


if __name__ == "__main__":
    unittest.main()
