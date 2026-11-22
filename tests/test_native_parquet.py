import os
import subprocess
import tempfile
import unittest
from datetime import date
from pathlib import Path
import duckdb

REPO_ROOT = Path(__file__).resolve().parent.parent
CLI_BIN = REPO_ROOT / ("native_parquet_cli.exe" if os.name == "nt" else "native_parquet_cli")


class TestNativeParquet(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not CLI_BIN.exists():
            raise RuntimeError(f"Missing {CLI_BIN.name}")

    def test_empty_and_sample_tables_duckdb_parity(self):
        with tempfile.TemporaryDirectory() as tmp:
            tdir = Path(tmp)
            subprocess.run([str(CLI_BIN), "test_samples", str(tdir)], check=True)
            con = duckdb.connect()

            # 1. Empty user_daily schema & 0 rows
            u_desc = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{(tdir / 'empty_user.parquet').as_posix()}')").fetchall()
            self.assertEqual([(r[0], r[1]) for r in u_desc], [
                ("user_id", "VARCHAR"), ("day", "DATE"), ("sessions", "BIGINT"),
                ("events", "BIGINT"), ("revenue_cents", "BIGINT"), ("top_category", "VARCHAR"),
            ])
            self.assertEqual(con.execute(f"SELECT count(*) FROM read_parquet('{(tdir / 'empty_user.parquet').as_posix()}')").fetchone()[0], 0)

            # 2. Empty campaign_daily schema & 0 rows
            c_desc = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{(tdir / 'empty_campaign.parquet').as_posix()}')").fetchall()
            self.assertEqual([(r[0], r[1]) for r in c_desc], [
                ("campaign_id", "VARCHAR"), ("channel", "VARCHAR"), ("day", "DATE"),
                ("sessions", "BIGINT"), ("purchasing_sessions", "BIGINT"),
                ("revenue_cents", "BIGINT"), ("assisted_sessions", "BIGINT"),
            ])
            self.assertEqual(con.execute(f"SELECT count(*) FROM read_parquet('{(tdir / 'empty_campaign.parquet').as_posix()}')").fetchone()[0], 0)

            # 3. Sample user_daily values (including NULL, quotes, UTF-8)
            u_rows = con.execute(f"SELECT * FROM read_parquet('{(tdir / 'sample_user.parquet').as_posix()}')").fetchall()
            self.assertEqual(u_rows, [
                ("u_1", date(2026, 10, 1), 2, 10, 5000, "electronics"),
                ("u_2", date(2026, 10, 2), 1, 3, 0, None),
                ('u_"3"', date(2026, 10, 3), 4, 12, 9999, "café,books"),
            ])

            # 4. Sample campaign_daily values
            c_rows = con.execute(f"SELECT * FROM read_parquet('{(tdir / 'sample_campaign.parquet').as_posix()}')").fetchall()
            self.assertEqual(c_rows, [
                ("camp_1", "search", date(2026, 10, 1), 10, 4, 25000, 12),
                ("organic", "organic", date(2026, 10, 2), 5, 1, 1500, 0),
            ])
            con.close()

    def test_2m_rows_benchmark_and_duckdb_read(self):
        with tempfile.TemporaryDirectory() as tmp:
            out_pq = Path(tmp) / "bench_2m.parquet"
            res = subprocess.run([str(CLI_BIN), "bench", str(out_pq), "2000000"],
                                 capture_output=True, text=True, check=True)
            print("\n" + res.stdout.strip(), f"| size={out_pq.stat().st_size / 1e6:.1f} MB")
            con = duckdb.connect()
            cnt, rev_sum = con.execute(
                f"SELECT count(*), sum(revenue_cents) FROM read_parquet('{out_pq.as_posix()}')"
            ).fetchone()
            self.assertEqual(cnt, 2_000_000)
            self.assertEqual(rev_sum, 20000 * sum(i * 100 for i in range(100)))
            con.close()


if __name__ == "__main__":
    unittest.main()
