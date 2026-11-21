"""Shopping Sessions — event cleaning, session construction, and user_daily.

Correctness-validated implementation with faster timestamp and CSV parsing. It uses only the Python standard library;
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
    """Strict F1/F2/epoch-ms parser optimized for the buildathon dialect.

    Unlike permissive datetime parsers, this rejects lower-case separators,
    malformed zones, Unicode digits and fractional seconds. Ingest timestamps
    also go through this parser, preserving the previous module interface.
    """
    if not raw:
        return None

    # An epoch timestamp consists ONLY of ASCII digits; leading zeros are legal.
    # Remove zeros before int() to avoid Python's huge-integer string guard.
    if '0' <= raw[0] <= '9' and raw.isascii() and raw.isdigit():
        significant = raw.lstrip('0') or '0'
        if len(significant) > 18:
            return None  # far outside every valid dataset time window
        return int(significant)

    length = len(raw)
    if length != 20 and length != 25:
        return None
    if (raw[4] != '-' or raw[7] != '-' or raw[10] != 'T' or
        raw[13] != ':' or raw[16] != ':' or not raw.isascii() or
        not (raw[0:4].isdigit() and raw[5:7].isdigit() and
             raw[8:10].isdigit() and raw[11:13].isdigit() and
             raw[14:16].isdigit() and raw[17:19].isdigit())):
        return None

    if length == 20:
        if raw[19] != 'Z':
            return None
        offset_ms = 0
    else:
        if (raw[19] not in ('+', '-') or raw[22] != ':' or
            not raw[20:22].isdigit() or not raw[23:25].isdigit()):
            return None
        off_h = int(raw[20:22])
        off_m = int(raw[23:25])
        if off_h > 23 or off_m > 59:
            return None
        sign = 1 if raw[19] == '+' else -1
        offset_ms = sign * (off_h * 60 + off_m) * 60_000

    hour = int(raw[11:13])
    minute = int(raw[14:16])
    second = int(raw[17:19])
    if hour > 23 or minute > 59 or second > 59:
        return None
    try:
        day_ordinal = date.fromisoformat(raw[:10]).toordinal()
    except ValueError:
        return None
    return ((day_ordinal - EPOCH_ORDINAL) * DAY_MS +
            hour * 3_600_000 + minute * 60_000 + second * 1000 - offset_ms)


def read_strict_csv(path: Path, nfields: int):
    """Parse this competition's restricted CSV, preserving literal CR characters.

    No field ever contains commas/newlines/escaped quotes. A no-quote fast path
    avoids parsing every field separately on simple rows. Quoted rows still
    receive full validation: only an entire field may be quoted.
    """
    with path.open('r', encoding='utf-8', newline='\n') as source:
        next(source, None)
        for line in source:
            if line.endswith('\n'):
                line = line[:-1]
            if not line:
                continue
            parts = line.split(',')
            if len(parts) != nfields:
                continue
            if '"' not in line:
                yield tuple(parts)
                continue
            fields = []
            for item in parts:
                if '"' not in item:
                    fields.append(item)
                elif len(item) >= 2 and item[0] == '"' and item[-1] == '"' and '"' not in item[1:-1]:
                    fields.append(item[1:-1])
                else:
                    break
            else:
                yield tuple(fields)


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
