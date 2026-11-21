import os
import subprocess
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from pipeline.identity_campaign import SEVEN_DAYS_MS

REPO_ROOT = Path(__file__).resolve().parent.parent
CLI_BIN = REPO_ROOT / ("native_campaign_cli.exe" if os.name == "nt" else "native_campaign_cli")


def iso_to_ms(iso_str: str) -> int:
    dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00")).astimezone(timezone.utc)
    return int(dt.timestamp() * 1000)


def write_lf(path: Path, content: str) -> None:
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        f.write(content)


def run_native_cli(data_dir: Path, ws_ms: int, we_ms: int, canon_map: dict, sessions: list):
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        canon_tsv = tmp_path / "canon.tsv"
        sess_tsv = tmp_path / "sess.tsv"
        out_tsv = tmp_path / "out.tsv"

        with open(canon_tsv, "w", encoding="utf-8", newline="\n") as f:
            for k, v in canon_map.items():
                f.write(f"{k}\t{v}\n")

        with open(sess_tsv, "w", encoding="utf-8", newline="\n") as f:
            for person_id, day, start_ms, rev_cents, has_purchase in sessions:
                f.write(f"{person_id}\t{day}\t{start_ms}\t{rev_cents}\t{1 if has_purchase else 0}\n")

        subprocess.run(
            [str(CLI_BIN), str(data_dir), str(ws_ms), str(we_ms), str(canon_tsv), str(sess_tsv), str(out_tsv)],
            capture_output=True,
            text=True,
            check=True,
        )

        rows = []
        with open(out_tsv, "r", encoding="utf-8") as f:
            for line in f:
                line = line.rstrip("\r\n")
                if not line:
                    continue
                cid, ch, day, sess, purch, rev, ast = line.split("\t")
                rows.append((cid, ch, day, int(sess), int(purch), int(rev), int(ast)))
        return rows


class TestNativeCampaign(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not CLI_BIN.exists():
            raise RuntimeError(f"Missing {CLI_BIN.name}")

    def test_exact_7day_cutoff(self):
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            (d / "clicks").mkdir()
            write_lf(d / "campaigns.csv", "campaign_id,channel\ncamp1,search\n")
            write_lf(
                d / "clicks" / "part0.csv",
                "click_id,user_id,campaign_id,ts\n"
                "ck1,u1,camp1,2026-01-01T00:00:00Z\n",
            )
            ws = iso_to_ms("2026-01-01T00:00:00Z")
            we = iso_to_ms("2026-01-15T00:00:00Z")
            sessions = [
                ("u1", "2026-01-08", ws + SEVEN_DAYS_MS, 1000, True),
                ("u1", "2026-01-08", ws + SEVEN_DAYS_MS + 1, 500, False),
            ]
            rows = run_native_cli(d, ws, we, {}, sessions)
            self.assertEqual(rows, [
                ("camp1", "search", "2026-01-08", 1, 1, 1000, 1),
                ("organic", "organic", "2026-01-08", 1, 0, 500, 0),
            ])

    def test_equal_time_click_id_tiebreak(self):
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            (d / "clicks").mkdir()
            write_lf(d / "campaigns.csv", "campaign_id,channel\ncampA,email\ncampB,social\n")
            write_lf(
                d / "clicks" / "part0.csv",
                "click_id,user_id,campaign_id,ts\n"
                "ck_z,u1,campB,2026-01-02T10:00:00Z\n"
                "ck_a,u1,campA,2026-01-02T10:00:00Z\n",
            )
            ws = iso_to_ms("2026-01-01T00:00:00Z")
            we = iso_to_ms("2026-01-10T00:00:00Z")
            t_sess = iso_to_ms("2026-01-02T10:05:00Z")
            sessions = [("u1", "2026-01-02", t_sess, 2500, True)]
            rows = run_native_cli(d, ws, we, {}, sessions)
            self.assertEqual(rows, [
                ("campA", "email", "2026-01-02", 1, 1, 2500, 1),
                ("campB", "social", "2026-01-02", 0, 0, 0, 1),
            ])

    def test_linked_identities(self):
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            (d / "clicks").mkdir()
            write_lf(d / "campaigns.csv", "campaign_id,channel\ncamp1,paid\n")
            write_lf(
                d / "clicks" / "part0.csv",
                "click_id,user_id,campaign_id,ts\n"
                "ck1,device_a,camp1,2026-01-03T08:00:00Z\n",
            )
            ws = iso_to_ms("2026-01-01T00:00:00Z")
            we = iso_to_ms("2026-01-10T00:00:00Z")
            canon = {"device_a": "person_1", "device_b": "person_1"}
            t_sess = iso_to_ms("2026-01-03T09:00:00Z")
            sessions = [("person_1", "2026-01-03", t_sess, 4200, True)]
            rows = run_native_cli(d, ws, we, canon, sessions)
            self.assertEqual(rows, [("camp1", "paid", "2026-01-03", 1, 1, 4200, 1)])

    def test_distinct_campaign_assists(self):
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            (d / "clicks").mkdir()
            write_lf(d / "campaigns.csv", "campaign_id,channel\ncamp1,search\ncamp2,display\n")
            write_lf(
                d / "clicks" / "part0.csv",
                "click_id,user_id,campaign_id,ts\n"
                "ck1,u1,camp1,2026-01-01T01:00:00Z\n"
                "ck2,u1,camp1,2026-01-01T02:00:00Z\n"
                "ck3,u1,camp2,2026-01-01T03:00:00Z\n",
            )
            ws = iso_to_ms("2026-01-01T00:00:00Z")
            we = iso_to_ms("2026-01-10T00:00:00Z")
            t_sess = iso_to_ms("2026-01-01T04:00:00Z")
            sessions = [("u1", "2026-01-01", t_sess, 900, False)]
            rows = run_native_cli(d, ws, we, {}, sessions)
            self.assertEqual(rows, [
                ("camp1", "search", "2026-01-01", 0, 0, 0, 1),
                ("camp2", "display", "2026-01-01", 1, 0, 900, 1),
            ])

    def test_quoted_last_column_escaped_quotes_rejected_if_invalid_ts(self):
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            (d / "clicks").mkdir()
            write_lf(d / "campaigns.csv", "campaign_id,channel\ncamp1,search\n")
            write_lf(
                d / "clicks" / "part0.csv",
                'click_id,user_id,campaign_id,ts\n'
                'ck_bad,u1,camp1,"2026-10-03T08:00:00Z""extra"\n'
                'ck_good,u1,camp1,2026-10-03T08:00:00Z\n',
            )
            ws = iso_to_ms("2026-10-01T00:00:00Z")
            we = iso_to_ms("2026-10-15T00:00:00Z")
            t_sess = iso_to_ms("2026-10-03T09:00:00Z")
            rows = run_native_cli(d, ws, we, {}, [("u1", "2026-10-03", t_sess, 1000, True)])
            self.assertEqual(rows, [("camp1", "search", "2026-10-03", 1, 1, 1000, 1)])


if __name__ == "__main__":
    unittest.main()

