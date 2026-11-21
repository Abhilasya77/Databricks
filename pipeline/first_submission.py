"""First complete baseline for the Shopping Sessions challenge.

A correctness-first integration candidate, NOT an optimized top-three solution.
Keeps events_sessions.py unchanged and supplies a separate provisional
identity/campaign implementation until the teammate's module is ready.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date
from pathlib import Path
from typing import Mapping
import csv
import os
import time
import sys
from contextlib import nullcontext

from events_sessions import (
    Session, input_parts, load_window, read_strict_csv, parse_timestamp_ms,
    compute_events_sessions,
)

LOOKBACK_MS = 7 * 86_400_000


def build_identity_map(data_dir: str | Path) -> dict[str, str]:
    """Union linked IDs transitively; canonical name is byte-wise smallest."""
    parent: dict[str, str] = {}
    size: dict[str, int] = {}

    def find(x: str) -> str:
        if x not in parent:
            parent[x] = x
            size[x] = 1
            return x
        root = x
        while parent[root] != root:
            root = parent[root]
        while parent[x] != root:
            next_x = parent[x]
            parent[x] = root
            x = next_x
        return root

    for path in input_parts(data_dir, 'links'):
        for a, b in read_strict_csv(path, 2):
            if not a or not b:
                continue
            ra, rb = find(a), find(b)
            if ra == rb:
                continue
            if size[ra] < size[rb]:
                ra, rb = rb, ra
            parent[rb] = ra
            size[ra] += size[rb]

    smallest: dict[str, str] = {}
    for x in parent:
        root = find(x)
        best = smallest.get(root)
        if best is None or x.encode('utf-8') < best.encode('utf-8'):
            smallest[root] = x
    return {x: smallest[find(x)] for x in parent}


def load_campaigns(data_dir: str | Path) -> dict[str, str]:
    with (Path(data_dir) / 'campaigns.csv').open(encoding='utf-8', newline='') as fh:
        return {row['campaign_id']: row['channel'] for row in csv.DictReader(fh)}


def load_clicks(data_dir: str | Path, identity: Mapping[str, str],
                campaigns: Mapping[str, str], window: tuple[int, int]):
    """Return canonical person -> chronologically sorted valid clicks.

    A click is (ts_ms, click_id, campaign_id). Click duplicates are
    identical after CSV parsing per the challenge specification.
    """
    seen: set[str] = set()
    by_person = defaultdict(list)
    start, end = window
    for part in input_parts(data_dir, 'clicks'):
        for click_id, user_id, campaign_id, raw_ts in read_strict_csv(part, 4):
            if not click_id or click_id in seen:
                continue
            seen.add(click_id)
            if not user_id or campaign_id not in campaigns:
                continue
            ts = parse_timestamp_ms(raw_ts)
            if ts is None or not (start - LOOKBACK_MS <= ts < end):
                continue
            by_person[identity.get(user_id, user_id)].append((ts, click_id, campaign_id))
    for person_clicks in by_person.values():
        person_clicks.sort(key=lambda c: (c[0], c[1].encode('utf-8')))
    return by_person


def make_campaign_daily(data_dir: str | Path, sessions: list[Session],
                        identity: Mapping[str, str], window: tuple[int, int]):
    """Credit each session to the best click and count distinct assisting campaigns."""
    channels = load_campaigns(data_dir)
    clicks_by_person = load_clicks(data_dir, identity, channels, window)
    sessions_by_person = defaultdict(list)
    for s in sessions:
        sessions_by_person[s.person_id].append(s)

    credited = defaultdict(lambda: [0, 0, 0])
    assisted = defaultdict(int)

    for person, person_sessions in sessions_by_person.items():
        person_sessions.sort(key=lambda s: s.session_start_ms)
        pc = clicks_by_person.get(person, ())
        left = right = 0
        active_campaigns = Counter()
        best = None
        for s in person_sessions:
            start = s.session_start_ms
            cutoff = start - LOOKBACK_MS
            # Add clicks at or before session_start. Sorted by timestamp, click ID.
            while right < len(pc) and pc[right][0] <= start:
                click = pc[right]
                active_campaigns[click[2]] += 1
                if (best is None or click[0] > best[0] or
                    (click[0] == best[0] and click[1].encode('utf-8') < best[1].encode('utf-8'))):
                    best = click
                right += 1
            while left < right and pc[left][0] < cutoff:
                campaign = pc[left][2]
                active_campaigns[campaign] -= 1
                if not active_campaigns[campaign]:
                    del active_campaigns[campaign]
                left += 1
            # If the latest click expired, ALL clicks expired.
            if best is not None and best[0] < cutoff:
                best = None
            winner = best[2] if best else 'organic'
            key = (winner, s.day)
            row = credited[key]
            row[0] += 1
            row[1] += int(s.has_purchase)
            row[2] += s.revenue_cents
            for campaign in active_campaigns:
                assisted[(campaign, s.day)] += 1

    result = []
    for campaign, day in credited.keys() | assisted.keys():
        count, buying, revenue = credited.get((campaign, day), [0, 0, 0])
        result.append({
            'campaign_id': campaign,
            'channel': 'organic' if campaign == 'organic' else channels[campaign],
            'day': day,
            'sessions': count,
            'purchasing_sessions': buying,
            'revenue_cents': revenue,
            'assisted_sessions': assisted.get((campaign, day), 0),
        })
    return result


def _sessions_to_duckdb(con, sessions, scratch: Path):
    """Bulk-load C++ sessions for teammate's ASOF attribution (not row-by-row SQL)."""
    import json
    import tempfile

    con.execute("DROP TABLE IF EXISTS sessions")
    if isinstance(sessions, (str, Path)):
        # Fast path: C++ already emitted valid, schema-checked session JSONL.
        # DuckDB reads it directly, without Python object construction or rewrite.
        path = Path(sessions)
        if path.stat().st_size == 0:
            con.execute("""CREATE TEMP TABLE sessions (
                session_id BIGINT, person_id VARCHAR, session_start_ms BIGINT,
                day DATE, revenue_cents BIGINT, has_purchase BOOLEAN
            )""")
        else:
            safe_path = str(path).replace("'", "''")
            con.execute(f"""CREATE TEMP TABLE sessions AS
                SELECT session_id::BIGINT AS session_id,
                       person_id::VARCHAR AS person_id,
                       session_start_ms::BIGINT AS session_start_ms,
                       CAST(day AS DATE) AS day,
                       revenue_cents::BIGINT AS revenue_cents,
                       has_purchase::BOOLEAN AS has_purchase
                FROM read_json('{safe_path}', format='newline_delimited', columns={{
                    'session_id':'BIGINT', 'person_id':'VARCHAR',
                    'session_start_ms':'BIGINT', 'day':'VARCHAR',
                    'revenue_cents':'BIGINT', 'has_purchase':'BOOLEAN'
                }})
            """)
        return
    if not sessions:
        con.execute("""CREATE TEMP TABLE sessions (
            session_id BIGINT, person_id VARCHAR, session_start_ms BIGINT,
            day DATE, revenue_cents BIGINT, has_purchase BOOLEAN
        )""")
        return

    scratch.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', prefix='sessions_',
                                     dir=scratch, encoding='utf-8',
                                     newline='\n', delete=False) as f:
        path = Path(f.name)
        for session in sessions:
            f.write(json.dumps({
                'session_id': session.session_id,
                'person_id': session.person_id,
                'session_start_ms': session.session_start_ms,
                'day': session.day.isoformat(),
                'revenue_cents': session.revenue_cents,
                'has_purchase': session.has_purchase,
            }, ensure_ascii=False, separators=(',', ':')) + '\n')
    try:
        safe_path = str(path).replace("'", "''")
        con.execute(f"""CREATE TEMP TABLE sessions AS
            SELECT session_id::BIGINT AS session_id,
                   person_id::VARCHAR AS person_id,
                   session_start_ms::BIGINT AS session_start_ms,
                   CAST(day AS DATE) AS day,
                   revenue_cents::BIGINT AS revenue_cents,
                   has_purchase::BOOLEAN AS has_purchase
            FROM read_json('{safe_path}', format='newline_delimited', columns={{
                'session_id':'BIGINT', 'person_id':'VARCHAR',
                'session_start_ms':'BIGINT', 'day':'VARCHAR',
                'revenue_cents':'BIGINT', 'has_purchase':'BOOLEAN'
            }})
        """)
    finally:
        path.unlink(missing_ok=True)


def compute_both(data_dir: str | Path):
    """V4: validated native event path + teammate's optimized campaign ASOF SQL."""
    from identity_campaign import (
        load_links_from_dir, load_campaigns, load_clicks_duckdb,
        compute_campaign_daily,
    )
    import duckdb

    data_dir = Path(data_dir)
    profile = os.environ.get('SHOPPING_PROFILE') == '1'
    last = time.perf_counter()
    def mark(label):
        nonlocal last
        now = time.perf_counter()
        if profile:
            print(f'PROFILE {label}_seconds={now-last:.3f}', file=sys.stderr)
        last = now

    identity = load_links_from_dir(str(data_dir / 'links'))
    mark('identity_links')
    native = os.environ.get('SHOPPING_NATIVE_EVENTS', '1') == '1'
    if native:
        from native_events_bridge import native_event_files
        engine_context = native_event_files(data_dir, identity)
    else:
        engine_context = nullcontext(None)

    with engine_context as native_result:
        if native:
            sessions, users = native_result
        else:
            sessions, users = compute_events_sessions(data_dir, identity)
        mark('events_and_user_daily')

        start_ms, end_ms = load_window(data_dir)
        campaigns = load_campaigns(str(data_dir / 'campaigns.csv'))
        scratch = Path(os.environ.get('SCRATCH_DIR') or '/tmp')
        scratch.mkdir(parents=True, exist_ok=True)
        con = duckdb.connect()
        ncpu = max(1, int(os.environ.get('NCPU') or os.cpu_count() or 1))
        mem_gb = max(1, int(os.environ.get('MEM_GB') or 8))
        con.execute(f'SET threads = {ncpu}')
        con.execute(f"SET memory_limit = '{max(1,int(mem_gb*0.8))}GB'")
        con.execute(f"SET temp_directory = '{str(scratch / 'duckdb_tmp').replace(chr(39),chr(39)*2)}'")
        con.execute('SET preserve_insertion_order = false')
        try:
            _sessions_to_duckdb(con, sessions, scratch)
            mark('sessions_duckdb_load')
            load_clicks_duckdb(con, str(data_dir / 'clicks'), campaigns,
                               identity, start_ms, end_ms)
            mark('clicks_duckdb_load')
            rel = compute_campaign_daily(con, 'sessions', None, campaigns)
            names = ['campaign_id','channel','day','sessions','purchasing_sessions',
                     'revenue_cents','assisted_sessions']
            result = [dict(zip(names, row)) for row in rel.fetchall()]
            mark('campaign_sql_and_fetch')
        finally:
            con.close()
        return users, result
