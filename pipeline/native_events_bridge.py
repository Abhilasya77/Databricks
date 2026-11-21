"""Optional C++ event-processing backend for the Shopping Sessions pipeline.

Companion to ../native_events.cpp. The interface returns the same Session and
user_daily types as events_sessions.compute_events_sessions(), so our campaign
module can remain unchanged. The native binary receives an exact identity map
as length-prefixed UTF-8 records; no separators or normalization are applied.
"""
from __future__ import annotations

from datetime import date
import json
import os
from pathlib import Path
import struct
import subprocess
import tempfile

from events_sessions import Session


def compute_events_sessions_native(data_dir, identity_map, scratch_dir=None):
    binary = os.environ.get('NATIVE_EVENTS_BINARY', '/app/native_events')
    binary_path = Path(binary)
    if not binary_path.is_file():
        raise FileNotFoundError(f'Native event engine not found: {binary}')
    root = Path(scratch_dir or os.environ.get('SCRATCH_DIR') or tempfile.gettempdir())
    root.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='native_events_', dir=root) as temporary:
        folder = Path(temporary)
        ids = folder / 'identity.bin'
        with ids.open('wb') as f:
            for src, dst in identity_map.items():
                for value in (src, dst):
                    b = value.encode('utf-8')
                    f.write(struct.pack('<I', len(b)))
                    f.write(b)
        users_path = folder / 'users.jsonl'
        sessions_path = folder / 'sessions.jsonl'
        subprocess.run([str(binary_path), str(data_dir), str(ids),
                        str(users_path), str(sessions_path)], check=True)
        with users_path.open(encoding='utf-8', newline='\n') as f:
            users = [json.loads(line) for line in f]
        for row in users:
            row['day'] = date.fromisoformat(row['day'])
        sessions = []
        with sessions_path.open(encoding='utf-8', newline='\n') as f:
            for line in f:
                record = json.loads(line)
                sessions.append(Session(
                    record['session_id'], record['person_id'],
                    record['session_start_ms'], date.fromisoformat(record['day']),
                    events=record['events'],
                    revenue_cents=record['revenue_cents'],
                    has_purchase=record['has_purchase'],
                ))
        return sessions, users
