"""Shopping Sessions — event cleaning, session construction, and user_daily.

First correctness-focused implementation. It uses only the Python standard library;
its outputs are Python records to hand to the team's attribution/Parquet writer.

This module does not do click attribution or write either Parquet table.
The caller must provide an identity map constructed according to rules.md.

Do not submit this module alone: the competition requires BOTH output tables.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Mapping
import csv
import json
import re

DAY_MS = 86_400_000
SESSION_GAP_MS = 1_800_000
LATE_MS = DAY_MS
EPOCH_ORDINAL = date(1970, 1, 1).toordinal()

# All digits are ASCII by the official rule (not Unicode's \d).
F1 = re.compile(r"^([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})Z$")
F2 = re.compile(r"^([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})([+-])([0-9]{2}):([0-9]{2})$")
DIGITS = re.compile(r"^[0-9]+$")


def parse_timestamp_ms(raw: str) -> int | None:
    """Parse exactly F1, F2 or positive epoch-ms digits from rules.md."""
    if not raw:
        return None
    if raw.isascii() and DIGITS.fullmatch(raw):
        # Avoid Python's max-integer-string limit on very long F3 input.
        digits = raw.lstrip("0") or "0"
        if len(digits) > 18:
            return None  # Out of any dataset window and 64-bit representable range.
        return int(digits)

    match = F1.fullmatch(raw)
    if match:
        y, month, day, hour, minute, second = map(int, match.groups())
        offset_min = 0
    else:
        match = F2.fullmatch(raw)
        if match is None:
            return None
        ys, ms, ds, hs, mins, ss, sign, off_h, off_m = match.groups()
        y, month, day, hour, minute, second = map(int, (ys, ms, ds, hs, mins, ss))
        oh, om = int(off_h), int(off_m)
        if oh > 23 or om > 59:
            return None
        offset_min = (oh * 60 + om) * (1 if sign == '+' else -1)

    if hour > 23 or minute > 59 or second > 59:
        return None
    try:
        ordinal = date(y, month, day).toordinal()
    except ValueError:
        return None
    return ((ordinal - EPOCH_ORDINAL) * 86400 + hour * 3600 + minute * 60 + second - 60 * offset_min) * 1000


def read_strict_csv(path: Path, nfields: int):
    """Read exactly the stated restricted CSV dialect; preserve trailing CR/spaces.

    The spec guarantees values never contain comma, quote or LF. Thus every
    comma is a separator, and quotes are only allowed around the entire field.
    CSV's common CRLF normalization would be WRONG for this challenge.
    """
    with path.open('r', encoding='utf-8', newline='\n') as source:
        next(source, None)  # All files have one header row.
        for line in source:
            if line.endswith('\n'):
                line = line[:-1]  # CR is ordinary data; DO NOT strip it.
            if not line:
                continue
            fields = line.split(',')
            if len(fields) != nfields:
                continue
            values = []
            for value in fields:
                if '"' not in value:
                    values.append(value)
                elif len(value) >= 2 and value[0] == '"' and value[-1] == '"' and '"' not in value[1:-1]:
                    values.append(value[1:-1])
                else:
                    break
            else:
                yield tuple(values)


def input_parts(data_dir: str | Path, category: str):
    return sorted((Path(data_dir) / category).glob('*.csv'))


def load_products(data_dir: str | Path) -> dict[str, str]:
    with (Path(data_dir) / 'products.csv').open('r', encoding='utf-8', newline='') as f:
        return {row['product_id']: row['category'] for row in csv.DictReader(f)}


def load_window(data_dir: str | Path) -> tuple[int, int]:
    with (Path(data_dir) / 'manifest.json').open('r', encoding='utf-8') as f:
        window = json.load(f)
    start = parse_timestamp_ms(window['window_start'])
    end = parse_timestamp_ms(window['window_end'])
    if start is None or end is None or start >= end:
        raise ValueError('Invalid manifest window')
    return start, end


@dataclass(slots=True)
class CleanEvent:
    person_id: str
    ts_ms: int
    event_type: str
    product_id: str
    purchase_cents: int


def load_clean_events(data_dir: str | Path, identity_map: Mapping[str, str], window: tuple[int, int]) -> list[CleanEvent]:
    """Parse -> earliest-ingest dedup -> validate -> drop late -> canonicalize.

    This initial correctness version holds deduped raw events in memory.
    Large-data speed/memory optimization is a separate step after validation.
    """
    kept: dict[str, tuple[int, tuple[str, ...]]] = {}
    for part in input_parts(data_dir, 'events'):
        for columns in read_strict_csv(part, 7):
            event_id, user_id, raw_ts, raw_ingest, event_type, product_id, price_cents = columns
            if not event_id:
                continue
            # ingest_ts is guaranteed valid F1 by the rules.
            ingest_ms = parse_timestamp_ms(raw_ingest)
            if ingest_ms is None:
                raise ValueError('Unexpected invalid ingest_ts in event source')
            existing = kept.get(event_id)
            if existing is None or ingest_ms < existing[0]:
                kept[event_id] = (ingest_ms, columns)

    start, end = window
    result = []
    for ingest_ms, (_, user_id, raw_ts, _, event_type, product_id, price_cents) in kept.values():
        if not user_id or not product_id or event_type not in ('view', 'cart', 'purchase'):
            continue
        ts_ms = parse_timestamp_ms(raw_ts)
        if ts_ms is None or not (start <= ts_ms < end) or (ingest_ms - ts_ms) > LATE_MS:
            continue
        purchase_cents = 0
        if event_type == 'purchase':
            if not DIGITS.fullmatch(price_cents):
                continue
            # Remove leading zeros before conversion; this also handles
            # arbitrarily long (invalid) numeric fields safely.
            significant = price_cents.lstrip('0') or '0'
            if len(significant) > 8:
                continue
            purchase_cents = int(significant)
            if not (1 <= purchase_cents <= 10_000_000):
                continue
        result.append(CleanEvent(identity_map.get(user_id, user_id), ts_ms, event_type, product_id, purchase_cents))
    return result


@dataclass(slots=True)
class Session:
    session_id: int
    person_id: str
    session_start_ms: int
    day: date
    events: int = 0
    revenue_cents: int = 0
    has_purchase: bool = False
    category_revenue: dict[str, int] = field(default_factory=dict)


def _utc_day(ts_ms: int) -> date:
    return date.fromordinal(EPOCH_ORDINAL + ts_ms // DAY_MS)


def build_sessions(events: list[CleanEvent], product_categories: Mapping[str, str]) -> list[Session]:
    """Events for each person in timestamp order; gap > 30 minutes splits.

    Events with the exact same timestamp are always assigned together.
    Each session is dated by its START; purchases past midnight stay there.
    """
    events.sort(key=lambda e: (e.person_id, e.ts_ms))
    sessions: list[Session] = []
    previous_person: str | None = None
    previous_ts = 0
    current: Session | None = None
    for event in events:
        if current is None or event.person_id != previous_person or event.ts_ms - previous_ts > SESSION_GAP_MS:
            current = Session(len(sessions), event.person_id, event.ts_ms, _utc_day(event.ts_ms))
            sessions.append(current)
        current.events += 1
        if event.event_type == 'purchase':
            current.has_purchase = True
            current.revenue_cents += event.purchase_cents
            category = product_categories.get(event.product_id, 'unknown')
            current.category_revenue[category] = current.category_revenue.get(category, 0) + event.purchase_cents
        previous_person = event.person_id
        previous_ts = event.ts_ms
    return sessions


def make_user_daily(sessions: list[Session]) -> list[dict]:
    """Exactly one row per (canonical person ID, UTC session-start date)."""
    aggregate = {}
    for session in sessions:
        key = (session.person_id, session.day)
        if key not in aggregate:
            aggregate[key] = [0, 0, 0, defaultdict(int)]
        row = aggregate[key]
        row[0] += 1
        row[1] += session.events
        row[2] += session.revenue_cents
        for category, revenue in session.category_revenue.items():
            row[3][category] += revenue

    results = []
    for (person, day), (count, events, revenue, cat_revenue) in aggregate.items():
        # The canonical person and all category tie breaks are byte-wise.
        top_category = min(cat_revenue, key=lambda c: (-cat_revenue[c], c.encode('utf-8'))) if cat_revenue else None
        results.append({
            'user_id': person, 'day': day, 'sessions': count,
            'events': events, 'revenue_cents': revenue,
            'top_category': top_category,
        })
    return results


def compute_events_sessions(data_dir: str | Path, identity_map: Mapping[str, str]):
    window = load_window(data_dir)
    events = load_clean_events(data_dir, identity_map, window)
    sessions = build_sessions(events, load_products(data_dir))
    return sessions, make_user_daily(sessions)
