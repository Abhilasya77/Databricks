#!/usr/bin/env python3
"""Cross-check V4 identity/click parsing with the 36 validated fixtures.

No Docker or DuckDB required. Full parquet/ASOF tests still mandatory.
"""
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'pipeline'))
from first_submission import build_identity_map, load_campaigns as old_campaigns, load_clicks as old_clicks
from identity_campaign import (load_links_from_dir, load_campaigns,
                               load_and_clean_clicks, parse_ts_to_ms)
from events_sessions import load_window


def main():
    cases = sorted(x for x in (ROOT / 'fixtures').iterdir() if x.is_dir())
    for fx in cases:
        data = fx / 'input'
        if not data.exists():
            data = fx / 'data'
        old_id = build_identity_map(data)
        new_id = load_links_from_dir(str(data / 'links'))
        assert old_id == new_id, f'{fx.name}: identity map mismatch'
        window = load_window(data)
        camp = load_campaigns(str(data / 'campaigns.csv'))
        assert camp == old_campaigns(data), f'{fx.name}: campaigns mismatch'
        old = old_clicks(data, old_id, camp, window)
        new = load_and_clean_clicks(str(data / 'clicks'), camp, new_id, *window)
        normalized = {}
        for click_id, person, campaign, ts in new:
            normalized.setdefault(person, []).append((ts, click_id, campaign))
        norm = {p: sorted(rows, key=lambda row: (row[0], row[1].encode('utf-8')))
                for p, rows in normalized.items()}
        assert dict(old) == norm, f'{fx.name}: cleaned clicks mismatch'
        print('PASS', fx.name)

    # Regression cases that datetime.fromisoformat() incorrectly accepted/rejected.
    assert parse_ts_to_ms('1791071890000') is not None
    assert parse_ts_to_ms('0001791071890000') == parse_ts_to_ms('1791071890000')
    for invalid in ('2026-10-01t12:00:00Z', '2026-10-01T12:00:00.1Z',
                    '2026-10-01T12:00:00', '2026-10-01T12:00:00z',
                    '2026-10-01T12:00:00+24:00', '١٧٩١٠٧١٨٩٠٠٠'):
        assert parse_ts_to_ms(invalid) is None, repr(invalid)
    print(f'{len(cases)}/{len(cases)} input fixture comparisons PASS; strict timestamps PASS')


if __name__ == '__main__':
    main()
