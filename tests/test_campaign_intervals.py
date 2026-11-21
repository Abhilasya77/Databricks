import unittest
import duckdb
from pipeline.identity_campaign import (
    SEVEN_DAYS_MS,
    build_canonical_map,
    compute_campaign_daily,
)


class TestCampaignIntervalsV5(unittest.TestCase):
    def _run_query(self, sessions_sql, clicks, campaigns):
        con = duckdb.connect()
        con.execute(f"CREATE TABLE sessions AS {sessions_sql}")
        rows = compute_campaign_daily(con, "sessions", clicks, campaigns).fetchall()
        return {
            (r[0], str(r[2])): (r[3], r[4], r[5], r[6])
            for r in rows
        }

    def test_exact_7_day_boundary(self):
        t0 = 1_700_000_000_000
        sessions_sql = f"""
            SELECT 's1' AS session_id, 'u1' AS person_id, {t0 + SEVEN_DAYS_MS}::BIGINT AS session_start_ms,
                   DATE '2026-01-10' AS day, 500::BIGINT AS revenue_cents, TRUE AS has_purchase
            UNION ALL
            SELECT 's2', 'u1', {t0 + SEVEN_DAYS_MS + 1}::BIGINT, DATE '2026-01-10', 200::BIGINT, FALSE
        """
        clicks = [("k1", "u1", "c1", t0)]
        res = self._run_query(sessions_sql, clicks, {"c1": "search"})
        self.assertEqual(res[("c1", "2026-01-10")], (1, 1, 500, 1))
        self.assertEqual(res[("organic", "2026-01-10")], (1, 0, 200, 0))

    def test_touching_and_overlapping_intervals_no_double_count(self):
        t0 = 1_700_000_000_000
        sessions_sql = f"""
            SELECT 's1' AS session_id, 'u1' AS person_id, {t0 + SEVEN_DAYS_MS}::BIGINT AS session_start_ms,
                   DATE '2026-01-10' AS day, 100::BIGINT AS revenue_cents, TRUE AS has_purchase
        """
        clicks = [
            ("k1", "u1", "c1", t0),
            ("k2", "u1", "c1", t0 + 1000),
            ("k3", "u1", "c1", t0 + SEVEN_DAYS_MS),
        ]
        res = self._run_query(sessions_sql, clicks, {"c1": "search"})
        self.assertEqual(res[("c1", "2026-01-10")], (1, 1, 100, 1))

    def test_repeated_clicks_shared_device_and_multiple_campaigns(self):
        t0 = 1_700_000_000_000
        cmap = build_canonical_map([("u_b", "u_a"), ("u_c", "u_b")])
        p = cmap["u_c"]
        self.assertEqual(p, "u_a")

        sessions_sql = f"""
            SELECT 's1' AS session_id, '{p}' AS person_id, {t0 + 5000}::BIGINT AS session_start_ms,
                   DATE '2026-01-10' AS day, 300::BIGINT AS revenue_cents, TRUE AS has_purchase
            UNION ALL
            SELECT 's2', '{p}', {t0 + 9000}::BIGINT, DATE '2026-01-10', 100::BIGINT, FALSE
        """
        clicks = [
            ("k1", p, "c1", t0),
            ("k2", p, "c1", t0),
            ("k3", p, "c1", t0 + 1000),
            ("k4", p, "c2", t0 + 2000),
            ("k5", p, "c2", t0 + 2000),
        ]
        res = self._run_query(sessions_sql, clicks, {"c1": "email", "c2": "social"})
        self.assertEqual(res[("c2", "2026-01-10")], (2, 1, 400, 2))
        self.assertEqual(res[("c1", "2026-01-10")], (0, 0, 0, 2))


if __name__ == "__main__":
    unittest.main()
