#!/usr/bin/env python3
"""Verify baseline rows for BOTH output tables against all 36 fixtures.
Run locally before Docker: python3 scripts/check_both_baseline.py
"""
from pathlib import Path
import sys
import csv
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'pipeline'))
from first_submission import compute_both


def normalize(row, keys):
    return tuple('' if row.get(k) is None else str(row[k]) for k in keys)


def compare(table, actual, folder):
    with (folder / 'expected' / (table + '.csv')).open(encoding='utf-8', newline='') as fh:
        reader = csv.DictReader(fh)
        keys = reader.fieldnames
        expected = sorted(normalize(r, keys) for r in reader)
    got = sorted(normalize(r, keys) for r in actual)
    if got != expected:
        print('  expected first 5:', expected[:5])
        print('  actual first 5:  ', got[:5])
    return got == expected


def main():
    root = ROOT / 'fixtures'
    if not root.is_dir():
        sys.exit(f'Missing fixtures directory: {root}')
    failed = []
    started = time.perf_counter()
    for fx in sorted(f for f in root.iterdir() if f.is_dir()):
        users, campaigns = compute_both(fx / 'input')
        ok1 = compare('user_daily', users, fx)
        ok2 = compare('campaign_daily', campaigns, fx)
        print(('PASS' if ok1 and ok2 else 'FAIL'), fx.name,
              'user_daily:', ok1, 'campaign_daily:', ok2)
        if not ok1 or not ok2:
            failed.append(fx.name)
    print(f'{36-len(failed)}/36 complete row fixture comparisons passed in {time.perf_counter()-started:.3f}s')
    return int(bool(failed))

if __name__ == '__main__':
    raise SystemExit(main())
