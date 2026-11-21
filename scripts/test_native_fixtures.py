#!/usr/bin/env python3
"""Check native event processing against the validated Python reference on all fixtures.
This test uses only Python's standard library. No Docker/DuckDB required.
"""
import json
import struct
import subprocess
import sys
import tempfile
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'pipeline'))
from first_submission import build_identity_map
from events_sessions import compute_events_sessions


def write_identity_map(identity, path):
    with open(path, 'wb') as f:
        for src, dst in identity.items():
            for value in (src, dst):
                data = value.encode('utf-8')
                f.write(struct.pack('<I', len(data)))
                f.write(data)


def load_jsonl(path):
    with open(path, encoding='utf-8', newline='\n') as f:
        return [json.loads(line) for line in f]


def compare_case(case, binary):
    data = case / 'input'
    if not data.exists():
        data = case / 'data'
    mapping = build_identity_map(data)
    ref_sessions, ref_user_daily = compute_events_sessions(data, mapping)
    with tempfile.TemporaryDirectory() as t:
        tmp = Path(t)
        write_identity_map(mapping, tmp/'identity.bin')
        proc = subprocess.run([str(binary), str(data), str(tmp/'identity.bin'), str(tmp/'users.jsonl'), str(tmp/'sessions.jsonl')], capture_output=True, text=True)
        if proc.returncode:
            raise AssertionError(proc.stderr)
        users = load_jsonl(tmp/'users.jsonl')
        sessions = load_jsonl(tmp/'sessions.jsonl')

    def normalized_user(r):
        return (r['user_id'], str(r['day']), r['sessions'], r['events'], r['revenue_cents'], r['top_category'])

    a = sorted(map(normalized_user, users))
    b = sorted(map(normalized_user, ref_user_daily))
    if a != b:
        raise AssertionError(f'user_daily mismatch: {set(a)^set(b)}')

    def norm_session(s):
        return (s.person_id, s.session_start_ms, str(s.day), s.events, s.revenue_cents, s.has_purchase)
    c = [(s['person_id'], s['session_start_ms'], s['day'], s['events'], s['revenue_cents'], s['has_purchase']) for s in sessions]
    d = list(map(norm_session, ref_sessions))
    if c != d:
        raise AssertionError(f'sessions mismatch: {set(c)^set(d)}')
    print('PASS', case.name, 'sessions', len(c), 'users', len(a))


def main():
    binary = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else ROOT / 'native_events'
    fixtures = ROOT / 'fixtures'
    if not fixtures.exists():
        fixtures = Path('/mnt/data/work_sessions/fixtures')
    cases = sorted(p for p in fixtures.iterdir() if p.is_dir())
    for case in cases:
        compare_case(case, binary)
    print(f'{len(cases)}/{len(cases)} native user_daily and session fixtures passed')

if __name__ == '__main__': main()
