@'
import time
import duckdb
from pipeline.identity_campaign import compute_campaign_daily, SEVEN_DAYS_MS


def run_benchmark():
    con = duckdb.connect()
    con.execute("""
        CREATE TABLE sessions AS
        SELECT
            's' || i AS session_id,
            'u' || (i % 500) AS person_id,
            1700000000000 + (i * 60000) AS session_start_ms,
            DATE '2026-01-10' AS day,
            100::BIGINT AS revenue_cents,
            (i % 3 = 0) AS has_purchase
        FROM range(50000) t(i)
    """)
    clicks = [
        ("k" + str(i), "u" + str(i % 500), "c" + str(i % 50), 1700000000000 + (i * 30000))
        for i in range(100000)
    ]
    camps = {"c" + str(i): "search" for i in range(50)}

    t0 = time.perf_counter()
    res_v5 = compute_campaign_daily(con, "sessions", clicks, camps).fetchall()
    t_v5 = time.perf_counter() - t0

    v4_query = f"""
    WITH winning_click_candidates AS (
        SELECT person_id, click_ts_ms, ARG_MIN(campaign_id, click_id) AS campaign_id
        FROM _clicks
        GROUP BY person_id, click_ts_ms
    ),
    attributed_sessions AS (
        SELECT
            CASE
                WHEN c.click_ts_ms IS NOT NULL AND c.click_ts_ms >= s.session_start_ms - {SEVEN_DAYS_MS}
                THEN c.campaign_id ELSE 'organic'
            END AS campaign_id,
            s.day,
            COUNT(*) AS sessions,
            SUM(CASE WHEN s.has_purchase THEN 1 ELSE 0 END) AS purchasing_sessions,
            SUM(s.revenue_cents) AS revenue_cents
        FROM sessions s
        ASOF LEFT JOIN winning_click_candidates c
          ON s.person_id = c.person_id AND s.session_start_ms >= c.click_ts_ms
        GROUP BY 1, 2
    ),
    person_campaigns AS (
        SELECT DISTINCT person_id, campaign_id FROM _clicks
    ),
    session_campaigns AS (
        SELECT s.session_id, s.person_id, s.day, s.session_start_ms, pc.campaign_id
        FROM sessions s
        JOIN person_campaigns pc ON s.person_id = pc.person_id
    ),
    assists AS (
        SELECT sc.campaign_id, sc.day, COUNT(*) AS assisted_sessions
        FROM session_campaigns sc
        ASOF JOIN _clicks c
          ON sc.person_id = c.person_id
         AND sc.campaign_id = c.campaign_id
         AND sc.session_start_ms >= c.click_ts_ms
        WHERE c.click_ts_ms >= sc.session_start_ms - {SEVEN_DAYS_MS}
        GROUP BY 1, 2
    ),
    combined AS (
        SELECT
            COALESCE(a.campaign_id, ast.campaign_id) AS campaign_id,
            COALESCE(a.day, ast.day) AS day,
            COALESCE(a.sessions, 0) AS sessions,
            COALESCE(a.purchasing_sessions, 0) AS purchasing_sessions,
            COALESCE(a.revenue_cents, 0) AS revenue_cents,
            COALESCE(ast.assisted_sessions, 0) AS assisted_sessions
        FROM attributed_sessions a
        FULL OUTER JOIN assists ast
          ON a.campaign_id = ast.campaign_id AND a.day = ast.day
    )
    SELECT
        comb.campaign_id,
        CASE WHEN comb.campaign_id = 'organic' THEN 'organic' ELSE camp.channel END AS channel,
        CAST(comb.day AS DATE) AS day,
        CAST(comb.sessions AS BIGINT) AS sessions,
        CAST(comb.purchasing_sessions AS BIGINT) AS purchasing_sessions,
        CAST(comb.revenue_cents AS BIGINT) AS revenue_cents,
        CAST(comb.assisted_sessions AS BIGINT) AS assisted_sessions
    FROM combined comb
    LEFT JOIN _campaigns camp ON comb.campaign_id = camp.campaign_id
    ORDER BY day, campaign_id
    """

    t0 = time.perf_counter()
    res_v4 = con.sql(v4_query).fetchall()
    t_v4 = time.perf_counter() - t0

    assert res_v4 == res_v5, "Mismatch between V4 and V5 results!"
    print(f"Exact match:                  {res_v4 == res_v5}")
    print(f"V4 (session_campaigns+ASOF):  {t_v4:.3f}s")
    print(f"V5 (merged 7-day intervals):  {t_v5:.3f}s")
    print(f"Speedup:                      {t_v4 / t_v5:.2f}x faster")


if __name__ == "__main__":
    run_benchmark()
'@ | Set-Content -Path benchmark_campaign_intervals.py -Encoding UTF8