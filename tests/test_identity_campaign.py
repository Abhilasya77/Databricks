import unittest
from datetime import date
import duckdb
from pipeline.identity_campaign import (
    SEVEN_DAYS_MS,
    build_canonical_map,
    compute_campaign_daily,
)


class TestIdentityCampaign(unittest.TestCase):
    def test_union_find_edge_cases(self):
        pairs = [
            ("u5", "u3"),
            ("u3", "u1"),
            ("u1", "u5"),   # duplicate & reversed
            ("u9", "u9"),   # self-link
            ("u9", "u8"),
            ("a", "A"),     # uppercase 'A' < lowercase 'a'
            ("", "u10"),    # empty ID dropped
        ]
        cmap = build_canonical_map(pairs)
        self.assertEqual(cmap["u5"], "u1")
        self.assertEqual(cmap["u3"], "u1")
        self.assertEqual(cmap["u1"], "u1")
        self.assertEqual(cmap["u9"], "u8")
        self.assertEqual(cmap["a"], "A")
        self.assertNotIn("u10", cmap)

    def test_attribution_and_assists(self):
        con = duckdb.connect()
        con.execute("""
            CREATE TABLE sessions (
                session_id VARCHAR,
                person_id VARCHAR,
                session_start_ms BIGINT,
                day DATE,
                revenue_cents BIGINT,
                has_purchase BOOLEAN
            )
        """)
        t0 = 1_700_000_000_000
        con.executemany("INSERT INTO sessions VALUES (?, ?, ?, ?, ?, ?)", [
            ("s1", "u1", t0, date(2026, 1, 10), 5000, True),
            ("s2", "u2", t0, date(2026, 1, 10), 1000, False),  # organic
        ])

        campaigns = {"c1": "search", "c2": "social"}
        clicks = [
            ("k1", "u1", "c1", t0 - SEVEN_DAYS_MS),  # exactly 7 days before
            ("k2", "u1", "c1", t0 - 1000),           # duplicate campaign assist
            ("k3", "u1", "c2", t0 - 500),            # tied latest ts
            ("k0", "u1", "c1", t0 - 500),            # tie-breaker: 'k0' < 'k3' -> c1 wins
            ("k4", "u1", "c2", t0 + 1),              # 1ms after session -> ignored
        ]

        res = compute_campaign_daily(con, "sessions", clicks, campaigns).fetchall()
        rows = {r[0]: r for r in res}

        self.assertEqual(rows["c1"], ("c1", "search", date(2026, 1, 10), 1, 1, 5000, 1))
        self.assertEqual(rows["c2"], ("c2", "social", date(2026, 1, 10), 0, 0, 0, 1))
        self.assertEqual(rows["organic"], ("organic", "organic", date(2026, 1, 10), 1, 0, 1000, 0))


if __name__ == "__main__":
    unittest.main()