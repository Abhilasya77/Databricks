from __future__ import annotations

import csv
import glob
import os
import tempfile
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import duckdb

# Use the strict, fixture-validated CSV and timestamp routines shared with events.
if __package__:
    from .events_sessions import read_strict_csv, parse_timestamp_ms
else:
    from events_sessions import read_strict_csv, parse_timestamp_ms

SEVEN_DAYS_MS = 7 * 24 * 60 * 60 * 1000


class UnionFind:
    """Disjoint Set Union with path compression, union by size, and byte-wise min ID tracking."""

    def __init__(self) -> None:
        self.parent: Dict[str, str] = {}
        self.size: Dict[str, int] = {}
        self.min_id: Dict[str, str] = {}

    def _add(self, x: str) -> None:
        if x not in self.parent:
            self.parent[x] = x
            self.size[x] = 1
            self.min_id[x] = x

    def find(self, x: str) -> str:
        root = x
        while self.parent[root] != root:
            root = self.parent[root]
        while self.parent[x] != root:
            nxt = self.parent[x]
            self.parent[x] = root
            x = nxt
        return root

    def union(self, a: str, b: str) -> None:
        self._add(a)
        self._add(b)
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return
        if self.size[ra] < self.size[rb]:
            ra, rb = rb, ra
        self.parent[rb] = ra
        self.size[ra] += self.size[rb]
        if self.min_id[rb].encode("utf-8") < self.min_id[ra].encode("utf-8"):
            self.min_id[ra] = self.min_id[rb]


def build_canonical_map(link_pairs: Iterable[Tuple[str, str]]) -> Dict[str, str]:
    """Return {user_id: canonical_person_id} where canonical_person_id is byte-wise smallest."""
    uf = UnionFind()
    for pair in link_pairs:
        if not pair or len(pair) != 2:
            continue
        u1, u2 = pair
        if not u1 or not u2:
            continue
        uf.union(u1, u2)
    return {u: uf.min_id[uf.find(u)] for u in uf.parent}


def load_links_from_dir(links_dir: str) -> Dict[str, str]:
    """Build transitive identity map, using the exact challenge CSV dialect."""
    uf = UnionFind()
    for path in sorted(glob.glob(os.path.join(links_dir, "*.csv"))):
        for a, b in read_strict_csv(Path(path), 2):
            if a and b:
                uf.union(a, b)
    return {x: uf.min_id[uf.find(x)] for x in uf.parent}


def parse_ts_to_ms(ts_str: str) -> Optional[int]:
    """Strict F1/F2/F3 timestamp implementation shared with the event parser."""
    return parse_timestamp_ms(ts_str)


def load_campaigns(campaigns_csv_path: str) -> Dict[str, str]:
    """Read campaigns.csv and return {campaign_id: channel}."""
    campaigns: Dict[str, str] = {}
    if not os.path.isfile(campaigns_csv_path):
        return campaigns
    with open(campaigns_csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cid = row.get("campaign_id")
            channel = row.get("channel")
            if cid and channel is not None:
                campaigns[cid] = channel
    return campaigns


def load_and_clean_clicks(
    clicks_dir: str,
    campaigns: Dict[str, str],
    canonical_map: Dict[str, str],
    window_start_ms: int,
    window_end_ms: int,
) -> List[Tuple[str, str, str, int]]:
    """
    Read all CSV files in clicks/, clean, deduplicate by click_id,
    validate timestamps & campaigns, filter to [window_start - 7d, window_end),
    and map user_id -> canonical person_id.
    """
    if not os.path.isdir(clicks_dir):
        return []

    min_ts = window_start_ms - SEVEN_DAYS_MS
    seen_click_ids = set()
    cleaned: List[Tuple[str, str, str, int]] = []
    get_canonical = canonical_map.get

    for path in sorted(glob.glob(os.path.join(clicks_dir, "*.csv"))):
        for click_id, user_id, campaign_id, ts_str in read_strict_csv(Path(path), 4):

                if not click_id or not user_id:
                    continue
                if click_id in seen_click_ids:
                    continue
                # Deduplication precedes validation per rules.md.
                seen_click_ids.add(click_id)
                if campaign_id not in campaigns:
                    continue

                ts_ms = parse_ts_to_ms(ts_str)
                if ts_ms is None or not (min_ts <= ts_ms < window_end_ms):
                    continue

                cleaned.append((click_id, get_canonical(user_id, user_id), campaign_id, ts_ms))

    return cleaned


def _bulk_load_clicks(
    con: duckdb.DuckDBPyConnection,
    cleaned_clicks: List[Tuple[str, str, str, int]],
) -> None:
    """Bulk-load cleaned clicks into DuckDB via temp CSV + native read_csv (avoids slow executemany)."""
    con.execute("DROP TABLE IF EXISTS _clicks")
    if not cleaned_clicks:
        con.execute("""
            CREATE TEMP TABLE _clicks (
                click_id VARCHAR,
                person_id VARCHAR,
                campaign_id VARCHAR,
                click_ts_ms BIGINT
            )
        """)
        return

    # /tmp is a 64 MB tmpfs in the official read-only container.  Write the
    # bulk staging file to the disk-backed /scratch mount instead.
    scratch = Path(os.environ.get("SCRATCH_DIR") or tempfile.gettempdir())
    scratch.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", suffix=".csv", delete=False, encoding="utf-8", newline="", dir=scratch
    ) as tmp:
        writer = csv.writer(tmp, lineterminator="\n")
        writer.writerows(cleaned_clicks)
        tmp_path = tmp.name

    try:
        duckdb_path = tmp_path.replace("\\", "/")
        con.execute(f"""
            CREATE TEMP TABLE _clicks AS
            SELECT * FROM read_csv(
                '{duckdb_path}',
                header=false,
                columns={{
                    'click_id': 'VARCHAR',
                    'person_id': 'VARCHAR',
                    'campaign_id': 'VARCHAR',
                    'click_ts_ms': 'BIGINT'
                }}
            )
        """)
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


def compute_campaign_daily(
    con: duckdb.DuckDBPyConnection,
    sessions_table: str,
    cleaned_clicks: List[Tuple[str, str, str, int]],
    campaigns: Dict[str, str],
    output_parquet_path: Optional[str] = None,
) -> duckdb.DuckDBPyRelation:
    """
    Compute 7-day last-click attribution, assists, and campaign_daily using ASOF JOIN.
    sessions_table must have:
      session_id, person_id, session_start_ms, day (DATE), revenue_cents, has_purchase
    """
    _bulk_load_clicks(con, cleaned_clicks)

    con.execute("DROP TABLE IF EXISTS _campaigns")
    con.execute("""
        CREATE TEMP TABLE _campaigns (
            campaign_id VARCHAR,
            channel VARCHAR
        )
    """)
    if campaigns:
        con.executemany("INSERT INTO _campaigns VALUES (?, ?)", list(campaigns.items()))

    query = f"""
    WITH winning_click_candidates AS (
        -- Pre-deduplicate ties on (person_id, click_ts_ms) using smallest click_id
        SELECT
            person_id,
            click_ts_ms,
            ARG_MIN(campaign_id, click_id) AS campaign_id
        FROM _clicks
        GROUP BY person_id, click_ts_ms
    ),
    attributed_sessions AS (
        -- O(N log N) ASOF LEFT JOIN for latest click at or before session_start_ms
        SELECT
            CASE
                WHEN c.click_ts_ms IS NOT NULL
                 AND c.click_ts_ms >= s.session_start_ms - {SEVEN_DAYS_MS}
                THEN c.campaign_id
                ELSE 'organic'
            END AS campaign_id,
            s.day,
            COUNT(*) AS sessions,
            SUM(CASE WHEN s.has_purchase THEN 1 ELSE 0 END) AS purchasing_sessions,
            SUM(s.revenue_cents) AS revenue_cents
        FROM {sessions_table} s
        ASOF LEFT JOIN winning_click_candidates c
          ON s.person_id = c.person_id
         AND s.session_start_ms >= c.click_ts_ms
        GROUP BY 1, 2
    ),
    person_campaigns AS (
        SELECT DISTINCT person_id, campaign_id
        FROM _clicks
    ),
    session_campaigns AS (
        SELECT
            s.session_id,
            s.person_id,
            s.day,
            s.session_start_ms,
            pc.campaign_id
        FROM {sessions_table} s
        JOIN person_campaigns pc
          ON s.person_id = pc.person_id
    ),
    assists AS (
        -- Check only the latest click per (person_id, campaign_id) via ASOF JOIN
        -- Eliminates intermediate blowup for users with thousands of clicks
        SELECT
            sc.campaign_id,
            sc.day,
            COUNT(*) AS assisted_sessions
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
          ON a.campaign_id = ast.campaign_id
         AND a.day = ast.day
    )
    SELECT
        comb.campaign_id,
        CASE
            WHEN comb.campaign_id = 'organic' THEN 'organic'
            ELSE camp.channel
        END AS channel,
        CAST(comb.day AS DATE) AS day,
        CAST(comb.sessions AS BIGINT) AS sessions,
        CAST(comb.purchasing_sessions AS BIGINT) AS purchasing_sessions,
        CAST(comb.revenue_cents AS BIGINT) AS revenue_cents,
        CAST(comb.assisted_sessions AS BIGINT) AS assisted_sessions
    FROM combined comb
    LEFT JOIN _campaigns camp
      ON comb.campaign_id = camp.campaign_id
    ORDER BY day, campaign_id
    """

    rel = con.sql(query)
    if output_parquet_path:
        con.execute(f"COPY ({query}) TO '{output_parquet_path}' (FORMAT parquet)")
    return rel