import csv
import glob
import os
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Tuple
import duckdb

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
    """Read all CSV files in links/ and build the canonical ID map."""
    if not os.path.isdir(links_dir):
        return {}
    pairs: List[Tuple[str, str]] = []
    for path in sorted(glob.glob(os.path.join(links_dir, "*.csv"))):
        with open(path, "r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            next(reader, None)  # skip header
            for row in reader:
                if len(row) == 2 and row[0] != "" and row[1] != "":
                    pairs.append((row[0], row[1]))
    return build_canonical_map(pairs)


def parse_ts_to_ms(ts_str: str) -> Optional[int]:
    """
    Strict timestamp parser -> UTC epoch milliseconds.
    Verify exact format rules against rules.md and your teammate's parser.
    """
    if not ts_str:
        return None
    try:
        if ts_str.endswith("Z"):
            dt = datetime.fromisoformat(ts_str[:-1] + "+00:00")
        else:
            dt = datetime.fromisoformat(ts_str)
            if dt.tzinfo is None:
                return None
        return int(round(dt.astimezone(timezone.utc).timestamp() * 1000))
    except ValueError:
        return None


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

    for path in sorted(glob.glob(os.path.join(clicks_dir, "*.csv"))):
        with open(path, "r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            next(reader, None)  # skip header
            for row in reader:
                if len(row) != 4:
                    continue
                click_id, user_id, campaign_id, ts_str = row

                if not click_id or not user_id:
                    continue
                if click_id in seen_click_ids:
                    continue
                if campaign_id not in campaigns:
                    continue

                ts_ms = parse_ts_to_ms(ts_str)
                if ts_ms is None:
                    continue
                if not (min_ts <= ts_ms < window_end_ms):
                    continue

                seen_click_ids.add(click_id)
                person_id = canonical_map.get(user_id, user_id)
                cleaned.append((click_id, person_id, campaign_id, ts_ms))

    return cleaned


def compute_campaign_daily(
    con: duckdb.DuckDBPyConnection,
    sessions_table: str,
    cleaned_clicks: List[Tuple[str, str, str, int]],
    campaigns: Dict[str, str],
    output_parquet_path: Optional[str] = None,
) -> duckdb.DuckDBPyRelation:
    """
    Compute 7-day last-click attribution, assists, and campaign_daily.
    sessions_table must have:
      session_id, person_id, session_start_ms, day (DATE), revenue_cents, has_purchase
    """
    con.execute("DROP TABLE IF EXISTS _clicks")
    con.execute("""
        CREATE TEMP TABLE _clicks (
            click_id VARCHAR,
            person_id VARCHAR,
            campaign_id VARCHAR,
            click_ts_ms BIGINT
        )
    """)
    if cleaned_clicks:
        con.executemany("INSERT INTO _clicks VALUES (?, ?, ?, ?)", cleaned_clicks)

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
    WITH eligible_clicks AS (
        SELECT
            s.session_id,
            s.day,
            s.revenue_cents,
            s.has_purchase,
            c.click_id,
            c.campaign_id,
            c.click_ts_ms
        FROM {sessions_table} s
        JOIN _clicks c
          ON s.person_id = c.person_id
         AND c.click_ts_ms >= s.session_start_ms - {SEVEN_DAYS_MS}
         AND c.click_ts_ms <= s.session_start_ms
    ),
    winning_clicks AS (
        SELECT session_id, campaign_id
        FROM (
            SELECT
                session_id,
                campaign_id,
                ROW_NUMBER() OVER (
                    PARTITION BY session_id
                    ORDER BY click_ts_ms DESC, click_id ASC
                ) AS rn
            FROM eligible_clicks
        )
        WHERE rn = 1
    ),
    attributed_sessions AS (
        SELECT
            COALESCE(w.campaign_id, 'organic') AS campaign_id,
            s.day,
            COUNT(*) AS sessions,
            SUM(CASE WHEN s.has_purchase THEN 1 ELSE 0 END) AS purchasing_sessions,
            SUM(s.revenue_cents) AS revenue_cents
        FROM {sessions_table} s
        LEFT JOIN winning_clicks w
          ON s.session_id = w.session_id
        GROUP BY 1, 2
    ),
    assists AS (
        SELECT
            campaign_id,
            day,
            COUNT(DISTINCT session_id) AS assisted_sessions
        FROM eligible_clicks
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
