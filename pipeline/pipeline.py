#!/usr/bin/env python3
"""Shopping Sessions: first complete correctness-first submission candidate.

Standalone fallback integration until the teammate's implementation is ready.
Input paths, output paths, and the Parquet schema follow rules.md.
The intermediate NDJSON is written only to SCRATCH_DIR, never /data or the
read-only root filesystem. No network access is required at run time.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path
import json
import os
import sys
import time
from contextlib import nullcontext

import duckdb
from first_submission import compute_both, compute_native_jsonl_paths

DATA = Path(os.environ.get('DATA_DIR', '/data'))
OUT = Path(os.environ.get('OUTPUT_DIR', '/output'))
SCRATCH = Path(os.environ.get('SCRATCH_DIR', '/scratch'))
NCPU = max(1, int(os.environ.get('NCPU', os.cpu_count() or 1)))
MEM_GB = max(1, int(os.environ.get('MEM_GB', '8')))

SCHEMAS = {
    'user_daily': [
        ('user_id', 'VARCHAR'), ('day', 'DATE'), ('sessions', 'BIGINT'),
        ('events', 'BIGINT'), ('revenue_cents', 'BIGINT'),
        ('top_category', 'VARCHAR'),
    ],
    'campaign_daily': [
        ('campaign_id', 'VARCHAR'), ('channel', 'VARCHAR'), ('day', 'DATE'),
        ('sessions', 'BIGINT'), ('purchasing_sessions', 'BIGINT'),
        ('revenue_cents', 'BIGINT'), ('assisted_sessions', 'BIGINT'),
    ],
}


def quote_sql(path: Path) -> str:
    return "'" + str(path).replace("'", "''") + "'"


def write_table(con, name: str, rows: list[dict]) -> None:
    schema = SCHEMAS[name]
    out_dir = OUT / name
    out_dir.mkdir(parents=True, exist_ok=True)
    parquet = out_dir / 'part-0.parquet'
    typed_empty = ', '.join(f'NULL::{typ} AS "{col}"' for col, typ in schema)
    if isinstance(rows, (str, Path)):
        # Read the native engine's existing JSONL directly, never constructing
        # a Python list or writing a redundant NDJSON copy.
        native_path = Path(rows)
        json_schema = ', '.join(
            f"'{col}': '{'VARCHAR' if typ == 'DATE' else typ}'"
            for col, typ in schema
        )
        select_cols = ', '.join(f'CAST("{col}" AS {typ}) AS "{col}"'
                                for col, typ in schema)
        if native_path.stat().st_size == 0:
            con.execute(f"COPY (SELECT {typed_empty} WHERE FALSE) TO {quote_sql(parquet)} (FORMAT PARQUET)")
        else:
            con.execute(f"""COPY (
                SELECT {select_cols}
                FROM read_json({quote_sql(native_path)},
                    format='newline_delimited', columns={{{json_schema}}})
            ) TO {quote_sql(parquet)} (FORMAT PARQUET)""")
        return
    if not rows:
        con.execute(f"COPY (SELECT {typed_empty} WHERE FALSE) TO {quote_sql(parquet)} (FORMAT PARQUET)")
        return

    ndjson = SCRATCH / (name + '.ndjson')
    with ndjson.open('w', encoding='utf-8', newline='\n') as f:
        for row in rows:
            converted = {k: v.isoformat() if isinstance(v, date) else v
                         for k, v in row.items()}
            f.write(json.dumps(converted, ensure_ascii=False, separators=(',', ':')) + '\n')

    json_schema = ', '.join(
        f"'{col}': '{'VARCHAR' if typ == 'DATE' else typ}'"
        for col, typ in schema
    )
    select_cols = ', '.join(f'CAST("{col}" AS {typ}) AS "{col}"'
                            for col, typ in schema)
    query = f"""
        COPY (
            SELECT {select_cols}
            FROM read_json(
                {quote_sql(ndjson)}, format='newline_delimited',
                columns={{{json_schema}}}
            )
        ) TO {quote_sql(parquet)} (FORMAT PARQUET)
    """
    try:
        con.execute(query)
    finally:
        ndjson.unlink(missing_ok=True)


def main():
    SCRATCH.mkdir(parents=True, exist_ok=True)
    OUT.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f'SET threads = {NCPU}')
    con.execute(f"SET memory_limit = '{max(1, int(MEM_GB * 0.8))}GB'")
    con.execute(f'SET temp_directory = {quote_sql(SCRATCH / "duckdb_tmp")}')
    con.execute('SET preserve_insertion_order = false')

    begin = time.perf_counter()
    try:
        if os.environ.get('SHOPPING_NATIVE_EVENTS', '1') == '1':
            # Retain scratch files until both DuckDB COPY statements complete.
            with compute_native_jsonl_paths(DATA) as (users_path, campaigns_path):
                computed = time.perf_counter()
                write_table(con, 'user_daily', users_path)
                write_table(con, 'campaign_daily', campaigns_path)
                finished = time.perf_counter()
                print(f'Native direct JSONL: compute={computed-begin:.3f}s, '
                      f'write={finished-computed:.3f}s, total={finished-begin:.3f}s',
                      file=sys.stderr)
        else:
            users, campaigns = compute_both(DATA)
            processed = time.perf_counter()
            write_table(con, 'user_daily', users)
            write_table(con, 'campaign_daily', campaigns)
            finished = time.perf_counter()
            print(f'Baseline: rows user={len(users)}, campaign={len(campaigns)}, '
                  f'compute={processed-begin:.3f}s, write={finished-processed:.3f}s, '
                  f'total={finished-begin:.3f}s', file=sys.stderr)
    finally:
        con.close()


if __name__ == '__main__':
    main()
