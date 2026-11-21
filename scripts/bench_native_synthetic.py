#!/usr/bin/env python3
"""Generate synthetic data and compare reference vs compiled engine timings and output.
A smoke benchmark, NOT an official competition workload.
"""
import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path
import random
import shutil
import sys
import time
import os

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'pipeline'))
from first_submission import compute_both


def generate(path, n, clicks_n):
    if path.exists(): shutil.rmtree(path)
    for folder in ('events','clicks','links'):(path/folder).mkdir(parents=True,exist_ok=True)
    (path/'manifest.json').write_text('{"window_start":"2026-10-01T00:00:00Z","window_end":"2026-10-15T00:00:00Z"}')
    (path/'products.csv').write_text('product_id,category\np1,shoes\np2,shirts\np3,books\n')
    (path/'campaigns.csv').write_text('campaign_id,channel\nc1,social\nc2,search\n')
    random.seed(1)
    persons=max(2,n//12)
    start=datetime(2026,10,1,tzinfo=timezone.utc)
    with (path/'events'/'part-1.csv').open('w') as f:
        f.write('event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents\n')
        for i in range(n):
            u=random.randrange(persons)
            sec=random.randrange(14*86400)
            ts=start+timedelta(seconds=sec)
            ingest=ts+timedelta(minutes=10)
            event_type='purchase' if i%30==0 else ('cart' if i%4==0 else 'view')
            f.write(f'e{i},u{u},{ts:%Y-%m-%dT%H:%M:%SZ},{ingest:%Y-%m-%dT%H:%M:%SZ},{event_type},p{i%3+1},{1500 if event_type=="purchase" else ""}\n')
    with (path/'clicks'/'part-1.csv').open('w') as f:
        f.write('click_id,user_id,campaign_id,ts\n')
        for i in range(clicks_n):
            u=random.randrange(persons);sec=random.randrange(14*86400)
            ts=start+timedelta(seconds=sec)
            f.write(f'c{i},u{u},c{i%2+1},{ts:%Y-%m-%dT%H:%M:%SZ}\n')
    with (path/'links'/'part-1.csv').open('w') as f:f.write('user_id_a,user_id_b\n')


def run(path, n, clicks):
    generate(path,n,clicks)
    old=os.environ.get('SHOPPING_NATIVE_EVENTS')
    times={}
    outputs={}
    try:
        for flag,label in [('0','python'),('1','native')]:
            os.environ['SHOPPING_NATIVE_EVENTS']=flag
            t=time.perf_counter();outputs[label]=compute_both(path)
            times[label]=time.perf_counter()-t
        def norm(rows):return sorted(tuple(str(v) for v in row.values()) for row in rows)
        for idx,name in [(0,'user_daily'),(1,'campaign_daily')]:
            if norm(outputs['python'][idx]) != norm(outputs['native'][idx]):
                raise AssertionError(f'{name} mismatch on synthetic data')
        print(f'PASSED both tables: {n:,} events, {clicks:,} clicks')
        print(f'Python total {times["python"]:.3f}s; native-event total {times["native"]:.3f}s')
        print(f'Observed speedup: {times["python"]/times["native"]:.2f}x (synthetic, machine-dependent)')
    finally:
        if old is None:os.environ.pop('SHOPPING_NATIVE_EVENTS',None)
        else:os.environ['SHOPPING_NATIVE_EVENTS']=old

if __name__=='__main__':
    ap=argparse.ArgumentParser()
    ap.add_argument('--events',type=int,default=200000)
    ap.add_argument('--clicks',type=int,default=40000)
    ap.add_argument('--output',default='/tmp/native_sessions_synthetic')
    a=ap.parse_args()
    run(Path(a.output),a.events,a.clicks)
