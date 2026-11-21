#!/usr/bin/env python3
"""Validate events/session portion against the official development answer.

Usage:
    python3 scripts/check_user_daily_dev.py --data dev --expected dev-answer

This test checks user_daily only. It does not validate campaign_daily, Parquet,
Docker or contest performance. It deliberately does not mutate /output.
"""
from __future__ import annotations

import argparse
import csv
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'pipeline'))
from events_sessions import (
    input_parts, read_strict_csv, load_window, load_clean_events,
    load_products, build_sessions, make_user_daily,
)


def build_test_identity_map(data_dir: Path) -> dict[str, str]:
    """Independent test resolver. Production resolver belongs to teammate."""
    parent: dict[str, str] = {}
    sizes: dict[str, int] = {}

    def find(x: str) -> str:
        if x not in parent:
            parent[x] = x
            sizes[x] = 1
            return x
        root = x
        while parent[root] != root:
            root = parent[root]
        while parent[x] != root:
            nxt = parent[x]
            parent[x] = root
            x = nxt
        return root

    for part in input_parts(data_dir, 'links'):
        for a, b in read_strict_csv(part, 2):
            if not a or not b:
                continue
            ra, rb = find(a), find(b)
            if ra != rb:
                if sizes[ra] < sizes[rb]:
                    ra, rb = rb, ra
                parent[rb] = ra
                sizes[ra] += sizes[rb]

    smallest: dict[str, str] = {}
    for user_id in parent:
        root = find(user_id)
        candidate = smallest.get(root)
        if candidate is None or user_id.encode('utf-8') < candidate.encode('utf-8'):
            smallest[root] = user_id
    return {user_id: smallest[find(user_id)] for user_id in parent}


def normalized(row: dict) -> tuple[str, ...]:
    return tuple('' if row.get(k) is None else str(row[k]) for k in
                 ('user_id', 'day', 'sessions', 'events', 'revenue_cents', 'top_category'))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--data', type=Path, default=Path('dev'))
    parser.add_argument('--expected', type=Path, default=Path('dev-answer'))
    args = parser.parse_args()
    expect_file = args.expected / 'user_daily.csv'
    if not (args.data / 'manifest.json').is_file():
        parser.error(f'Dataset missing: {args.data}/manifest.json. Generate the dev set first.')
    if not expect_file.is_file():
        parser.error(f'Expected answer missing: {expect_file}. Unzip dev-answer.zip first.')

    start = time.perf_counter()
    identity_map = build_test_identity_map(args.data)
    t1 = time.perf_counter()
    window = load_window(args.data)
    events = load_clean_events(args.data, identity_map, window)
    t2 = time.perf_counter()
    sessions = build_sessions(events, load_products(args.data))
    t3 = time.perf_counter()
    user_rows = make_user_daily(sessions)
    t4 = time.perf_counter()
    with expect_file.open('r', encoding='utf-8', newline='') as f:
        expected = [normalized(row) for row in csv.DictReader(f)]
    actual = [normalized(row) for row in user_rows]
    expected.sort()
    actual.sort()
    t5 = time.perf_counter()

    print(f'Identity resolver (test only): {t1-start:.3f} s')
    print(f'Parse/dedup/validate events:   {t2-t1:.3f} s')
    print(f'Sort/sessionize events:        {t3-t2:.3f} s')
    print(f'Aggregate user_daily:         {t4-t3:.3f} s')
    print(f'Compare expected CSV:         {t5-t4:.3f} s')
    print(f'Total local test runtime:      {t5-start:.3f} s')
    print(f'Clean events: {len(events):,}; sessions: {len(sessions):,}')
    print(f'user_daily: {len(actual):,} rows; expected: {len(expected):,} rows')

    if actual == expected:
        print('PASS — user_daily matches official expected rows exactly')
        return 0

    print('FAIL — user_daily differs from official answer')
    got_set, want_set = set(actual), set(expected)
    print('Missing example rows:', list(sorted(want_set - got_set))[:5])
    print('Extra example rows:', list(sorted(got_set - want_set))[:5])
    return 1


if __name__ == '__main__':
    raise SystemExit(main())
