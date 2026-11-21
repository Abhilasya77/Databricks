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


def compute_both(data_dir: str | Path):
    identity = build_identity_map(data_dir)
    sessions, users = compute_events_sessions(data_dir, identity)
    campaigns = make_campaign_daily(data_dir, sessions, identity, load_window(data_dir))
    return users, campaigns
