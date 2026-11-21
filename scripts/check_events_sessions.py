"""Check just OUR portion against all official fixture user_daily tables.

This test has a tiny independent link resolver so it can run before the teammate's
production identity module is finished. Does not check campaign_daily or Parquet.
"""
from __future__ import annotations
import csv
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'pipeline'))
from events_sessions import compute_events_sessions, input_parts, read_strict_csv


def fixture_identity_map(folder):
    parent = {}
    def find(x):
        parent.setdefault(x, x)
        if parent[x] != x:
            parent[x] = find(parent[x])
        return parent[x]
    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            if ra.encode('utf-8') < rb.encode('utf-8'):
                parent[rb] = ra
            else:
                parent[ra] = rb
    for path in input_parts(folder, 'links'):
        for a, b in read_strict_csv(path, 2):
            if a and b:
                union(a, b)
    return {x: find(x) for x in parent}


def rows_in_csv(path):
    with open(path, newline='', encoding='utf-8') as f:
        return list(csv.DictReader(f))


def normalized(row):
    return tuple('' if row[col] is None else str(row[col]) for col in
                 ('user_id', 'day', 'sessions', 'events', 'revenue_cents', 'top_category'))


def check_fixture(folder):
    sessions, rows = compute_events_sessions(folder / 'input', fixture_identity_map(folder / 'input'))
    expected = rows_in_csv(folder / 'expected' / 'user_daily.csv')
    actual = sorted(normalized(row) for row in rows)
    want = sorted(normalized(row) for row in expected)
    return actual == want, actual, want


def main():
    root = Path(__file__).resolve().parents[1] / 'fixtures'
    failures = []
    for folder in sorted(root.iterdir()):
        if not folder.is_dir():
            continue
        correct, actual, expected = check_fixture(folder)
        print(f"{'PASS' if correct else 'FAIL'} {folder.name} — user_daily")
        if not correct:
            failures.append(folder.name)
            print('  expected:', expected)
            print('  actual:  ', actual)
    print(f'\n{36-len(failures)}/36 user_daily fixture comparisons passed')
    if failures:
        sys.exit(1)


if __name__ == '__main__':
    main()
