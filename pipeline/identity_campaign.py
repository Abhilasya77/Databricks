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
    cleaned_clicks: Optional[List[Tuple[str, str, str, int]]],
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


def load_clicks_duckdb(
    con: duckdb.DuckDBPyConnection,
    clicks_dir: str,
    campaigns: Dict[str, str],
    canonical_map: Dict[str, str],
    window_start_ms: int,
    window_end_ms: int,
) -> None:
    """Native multi-threaded DuckDB click ingestion (V6) with strict CSV parity."""
    import glob as _glob
    import json as _json
    import tempfile as _tempfile
    from pathlib import Path as _Path

    con.execute("DROP TABLE IF EXISTS _clicks")
    con.execute("DROP TABLE IF EXISTS _campaigns")
    con.execute("CREATE TEMP TABLE _campaigns (campaign_id VARCHAR, channel VARCHAR)")
    if campaigns:
        con.executemany("INSERT INTO _campaigns VALUES (?, ?)", list(campaigns.items()))

    files = sorted(_glob.glob(os.path.join(clicks_dir, "*.csv"))) if os.path.isdir(clicks_dir) else []
    if not files or not campaigns:
        con.execute("""
            CREATE TEMP TABLE _clicks (
                click_id VARCHAR,
                person_id VARCHAR,
                campaign_id VARCHAR,
                click_ts_ms BIGINT
            )
        """)
        return

    con.execute("DROP TABLE IF EXISTS _canonical_map")
    con.execute("CREATE TEMP TABLE _canonical_map (user_id VARCHAR, person_id VARCHAR)")
    if canonical_map:
        scratch = _Path(os.environ.get("SCRATCH_DIR") or _tempfile.gettempdir())
        scratch.mkdir(parents=True, exist_ok=True)
        with _tempfile.NamedTemporaryFile(
            "w", suffix=".ndjson", delete=False, encoding="utf-8", newline="\n", dir=scratch
        ) as tmp:
            for u, p in canonical_map.items():
                tmp.write(_json.dumps({"user_id": u, "person_id": p}, ensure_ascii=False) + "\n")
            map_path = tmp.name
        map_path_sql = map_path.replace("\\", "/")
        try:
            con.execute(f"""
                INSERT INTO _canonical_map
                SELECT user_id, person_id FROM read_json(
                    '{map_path_sql}',
                    format='newline_delimited',
                    columns={{'user_id': 'VARCHAR', 'person_id': 'VARCHAR'}}
                )
            """)
        finally:
            if os.path.exists(map_path):
                os.remove(map_path)

    min_ts = window_start_ms - SEVEN_DAYS_MS
    duck_files = [f.replace("\\", "/") for f in files]
    files_sql = "[" + ", ".join("'" + f.replace("'", "''") + "'" for f in duck_files) + "]"

    def unquote_expr(col: str) -> str:
        return (
            f"CASE "
            f"WHEN NOT contains({col}, '\"') THEN {col} "
            f"WHEN length({col}) >= 2 AND starts_with({col}, '\"') AND ends_with({col}, '\"') "
            f"     AND NOT contains(substr({col}, 2, length({col}) - 2), '\"') "
            f"THEN substr({col}, 2, length({col}) - 2) "
            f"ELSE NULL END"
        )

    con.execute(f"""
        CREATE TEMP TABLE _clicks AS
        WITH file_lines AS (
            SELECT
                filename,
                generate_subscripts(lines, 1) AS rn,
                unnest(lines) AS raw_line
            FROM (
                SELECT
                    filename,
                    string_split(content, chr(10)) AS lines
                FROM read_text({files_sql})
            )
        ),
        raw_lines AS (
            SELECT
                filename,
                rn,
                string_split(raw_line, ',') AS parts
            FROM file_lines
            WHERE rn > 1
              AND raw_line IS NOT NULL
              AND raw_line != ''
        ),
        parsed_fields AS (
            SELECT
                filename,
                rn,
                {unquote_expr('parts[1]')} AS click_id,
                {unquote_expr('parts[2]')} AS user_id,
                {unquote_expr('parts[3]')} AS campaign_id,
                {unquote_expr('parts[4]')} AS ts_str
            FROM raw_lines
            WHERE len(parts) = 4
        ),
        valid_ids AS (
            SELECT filename, rn, click_id, user_id, campaign_id, ts_str
            FROM parsed_fields
            WHERE click_id IS NOT NULL AND click_id != ''
              AND user_id IS NOT NULL AND user_id != ''
              AND campaign_id IS NOT NULL
              AND ts_str IS NOT NULL
        ),
        deduped AS (
            SELECT
                click_id,
                ARG_MIN(user_id, (filename, rn)) AS user_id,
                ARG_MIN(campaign_id, (filename, rn)) AS campaign_id,
                ARG_MIN(ts_str, (filename, rn)) AS ts_str
            FROM valid_ids
            GROUP BY click_id
        ),
        with_ts AS (
            SELECT
                d.click_id,
                d.user_id,
                d.campaign_id,
                CASE
                    WHEN regexp_matches(d.ts_str, '^[0-9]+$') THEN
                        CASE
                            WHEN length(COALESCE(NULLIF(ltrim(d.ts_str, '0'), ''), '0')) <= 18
                            THEN CAST(COALESCE(NULLIF(ltrim(d.ts_str, '0'), ''), '0') AS BIGINT)
                            ELSE NULL
                        END
                    WHEN length(d.ts_str) = 20
                     AND regexp_matches(d.ts_str, '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}T[0-9]{{2}}:[0-9]{{2}}:[0-9]{{2}}Z$') THEN
                        epoch_ms(try_strptime(d.ts_str, '%Y-%m-%dT%H:%M:%SZ'))
                    WHEN length(d.ts_str) = 25
                     AND regexp_matches(d.ts_str, '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}T[0-9]{{2}}:[0-9]{{2}}:[0-9]{{2}}[+-][0-9]{{2}}:[0-9]{{2}}$')
                     AND CAST(substr(d.ts_str, 21, 2) AS INT) <= 23
                     AND CAST(substr(d.ts_str, 24, 2) AS INT) <= 59 THEN
                        epoch_ms(try_strptime(substr(d.ts_str, 1, 19), '%Y-%m-%dT%H:%M:%S'))
                        - (CASE WHEN substr(d.ts_str, 20, 1) = '+' THEN 1 ELSE -1 END)
                          * (CAST(substr(d.ts_str, 21, 2) AS BIGINT) * 60 + CAST(substr(d.ts_str, 24, 2) AS BIGINT))
                          * 60000
                    ELSE NULL
                END AS click_ts_ms
            FROM deduped d
            JOIN _campaigns c ON d.campaign_id = c.campaign_id
        )
        SELECT
            w.click_id,
            COALESCE(m.person_id, w.user_id) AS person_id,
            w.campaign_id,
            w.click_ts_ms
        FROM with_ts w
        LEFT JOIN _canonical_map m ON w.user_id = m.user_id
        WHERE w.click_ts_ms IS NOT NULL
          AND w.click_ts_ms >= {min_ts}
          AND w.click_ts_ms < {window_end_ms}
    """)


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
    if cleaned_clicks is not None:
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
    click_gaps AS (
        SELECT
            person_id,
            campaign_id,
            click_ts_ms,
            CASE
                WHEN click_ts_ms - LAG(click_ts_ms) OVER (
                    PARTITION BY person_id, campaign_id ORDER BY click_ts_ms
                ) <= {SEVEN_DAYS_MS}
                THEN 0 ELSE 1
            END AS is_new_interval
        FROM _clicks
    ),
    click_islands AS (
        SELECT
            person_id,
            campaign_id,
            click_ts_ms,
            SUM(is_new_interval) OVER (
                PARTITION BY person_id, campaign_id ORDER BY click_ts_ms
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS island_id
        FROM click_gaps
    ),
    merged_intervals AS (
        SELECT
            person_id,
            campaign_id,
            MIN(click_ts_ms) AS start_ms,
            MAX(click_ts_ms) + {SEVEN_DAYS_MS} AS end_ms
        FROM click_islands
        GROUP BY person_id, campaign_id, island_id
    ),
    assists AS (
        SELECT
            iv.campaign_id,
            s.day,
            COUNT(*) AS assisted_sessions
        FROM {sessions_table} s
        JOIN merged_intervals iv
          ON s.person_id = iv.person_id
         AND s.session_start_ms >= iv.start_ms
         AND s.session_start_ms <= iv.end_ms
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