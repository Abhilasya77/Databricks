"""Run from repository root after compiling native_campaign_cli.
This tests the Buildathon's unusual strict LF-only, whole-field-only CSV dialect.
"""
from pathlib import Path
import subprocess
import tempfile
from datetime import datetime, timezone

root = Path(__file__).resolve().parent
binary = root / 'native_campaign_cli'
if not binary.exists():
    raise SystemExit('Compile first: g++ -std=c++17 -O3 -DNDEBUG -DNATIVE_CAMPAIGN_CLI native_campaign.cpp -o native_campaign_cli')

cases = [
    ('plain valid click', 'k1,u1,c1,2026-10-03T09:00:00Z', True),
    ('whole-field quotes valid', '"k1","u1","c1","2026-10-03T09:00:00Z"', True),
    ('embedded escaped quote invalid', '"ck""1",u1,c1,2026-10-03T09:00:00Z', False),
    ('quoted comma invalid', '"k,1",u1,c1,2026-10-03T09:00:00Z', False),
    ('unquoted quote invalid', 'k"1,u1,c1,2026-10-03T09:00:00Z', False),
    ('LF only; CR timestamp invalid', 'k1,u1,c1,2026-10-03T09:00:00Z\r', False),
    ('epoch out of int64 range invalid', 'k1,u1,c1,9223372036854775808', False),
    ('very long epoch invalid', 'k1,u1,c1,999999999999999999999999999999999', False),
    ('valid epoch', 'k1,u1,c1,1791018000000', True),
]
with tempfile.TemporaryDirectory() as temp:
    data = Path(temp)
    (data / 'clicks').mkdir()
    (data / 'campaigns.csv').write_text('campaign_id,channel\nc1,search\nc2,social\n')
    (data / 'canon.tsv').write_text('')
    session_ts = int(datetime(2026, 10, 3, 9, 10, tzinfo=timezone.utc).timestamp() * 1000)
    (data / 'sessions.tsv').write_text(f'u1\t2026-10-03\t{session_ts}\t1200\t1\n')
    for label, row, valid in cases:
        (data / 'clicks' / 'part.csv').write_bytes(
            ('click_id,user_id,campaign_id,ts\n' + row + '\n' +
             'k2,u1,c2,2026-10-03T08:00:00Z\n').encode('utf-8'))
        output = data / 'out.tsv'
        subprocess.run([
            str(binary), str(data), '1790812800000', '1792022400000',
            str(data / 'canon.tsv'), str(data / 'sessions.tsv'), str(output),
        ], check=True, capture_output=True)
        result = output.read_text()
        expected = ('c1\tsearch' if valid else 'c2\tsocial')
        assert result.startswith(expected), f'{label}: got {result!r}; expected {expected!r}'
        print('PASS:', label)
print('9/9 focused regression cases passed')
