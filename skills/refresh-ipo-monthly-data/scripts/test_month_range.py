"""Integration regression for the PowerShell audit; uses only temporary fake data."""
import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

SHELL=shutil.which('pwsh') or shutil.which('powershell')

@unittest.skipUnless(SHELL,'PowerShell unavailable')
class MonthRangeTests(unittest.TestCase):
    def run_case(self,start,end,extra=()):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            for relative,value in [
                ('data/discovered/ipo_events.json',{'stocks':[]}),
                ('data/manual_fundamentals.json',{}),
                ('ledger.json',{'rangeStart':start,'rangeEndExclusive':end,'sources':[{}, {}, {}],'candidates':[]})]:
                p=root/relative;p.parent.mkdir(parents=True,exist_ok=True);p.write_text(json.dumps(value),encoding='utf-8')
            return subprocess.run([SHELL,'-NoProfile','-File',str(Path(__file__).with_name('audit_monthly_ipo_data.ps1')),
                '-RepoPath',str(root),'-CandidateLedgerPath','ledger.json','-AsOfDate','2026-10-31',*extra],capture_output=True)

    def test_three_month_range(self):
        result=self.run_case('2026-10-01','2027-01-01',('-RangeStart','2026-10-01','-RangeEndExclusive','2027-01-01'))
        self.assertEqual(result.returncode,0,result.stdout+result.stderr)

    def test_year_crossing(self):
        result=self.run_case('2026-12-01','2027-03-01',('-RangeStart','2026-12-01','-RangeEndExclusive','2027-03-01'))
        self.assertEqual(result.returncode,0,result.stdout+result.stderr)

    def test_default_still_two_months(self):
        self.assertEqual(self.run_case('2026-10-01','2026-12-01').returncode,0)

    def test_wrong_ledger_range_fails(self):
        self.assertNotEqual(self.run_case('2026-10-01','2027-01-01').returncode,0)

    def test_partial_range_fails(self):
        self.assertNotEqual(self.run_case('2026-10-01','2027-01-01',('-RangeStart','2026-10-01')).returncode,0)

if __name__=='__main__':unittest.main()
