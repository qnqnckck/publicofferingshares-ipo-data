import unittest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from check_verified_delta import dart_subscription_dates, exact_feed_removal_result, issue_additions, latest_dart_search_filing, permitted_null_issue, phase_errors, rows


class VerifiedDeltaPolicyTests(unittest.TestCase):
    def test_preexisting_errors_are_carried_but_new_errors_are_visible(self):
        base = {("missing_x", "old"), ("source_conflict", "held")}
        candidate = {("missing_x", "old"), ("missing_y", "new")}
        self.assertEqual(issue_additions(base, candidate), {("missing_y", "new")})

    def test_only_explicit_null_target_gap_is_tolerated(self):
        stock = {"listingDate": None, "fundamentals": {"marketCapKrw": None}}
        self.assertTrue(permitted_null_issue(
            "target", "target", "missing_review", "fundamentals.marketCapKrw",
            {"missing_review"}, {"fundamentals.marketCapKrw"}, stock))
        self.assertFalse(permitted_null_issue(
            "other", "target", "missing_review", "fundamentals.marketCapKrw",
            {"missing_review"}, {"fundamentals.marketCapKrw"}, stock))
        stock["fundamentals"]["marketCapKrw"] = 100
        self.assertFalse(permitted_null_issue(
            "target", "target", "missing_review", "fundamentals.marketCapKrw",
            {"missing_review"}, {"fundamentals.marketCapKrw"}, stock))

    def test_feed_rows_are_explicit(self):
        self.assertEqual(rows({"generatedAt": "x", "stocks": [{"id": "a"}]}), [{"id": "a"}])
        self.assertEqual(rows([{"id": "b"}]), [{"id": "b"}])
        with self.assertRaises(ValueError):
            rows({"items": []})

    def test_only_explicit_feed_rows_can_be_removed(self):
        old = [{"id": "keep", "value": 1}, {"id": "remove", "value": 2}]
        self.assertEqual(exact_feed_removal_result(old, [old[0]], ["remove"]), (True, ""))
        self.assertFalse(exact_feed_removal_result(old, [old[0]], ["missing"])[0])
        self.assertFalse(exact_feed_removal_result(old, old, ["remove"])[0])
        self.assertFalse(exact_feed_removal_result(old, [{"id": "keep", "value": 9}], ["remove"])[0])
        self.assertFalse(exact_feed_removal_result(old, [old[0]], ["remove", "remove"])[0])

    def test_current_dart_search_extracts_top_filing(self):
        html = """<table><tr><td>1</td><td><a>회사</a></td>
          <td><a onclick=\"openReportViewer('20261111000281','')\">증권발행실적보고서</a></td>
          <td>회사</td><td>2026.11.11</td></tr></table>"""
        self.assertEqual(latest_dart_search_filing(html), {
            "rcpNo": "20261111000281",
            "filingDate": "2026.11.11",
            "title": "증권발행실적보고서",
        })
        self.assertIsNone(latest_dart_search_filing("<html>no filing rows</html>"))

    def test_latest_schedule_report_dates_are_parsed_from_official_body(self):
        body = """<table><tr><td>청약개시일</td><td>청약종료일</td><td>납입기일</td></tr>
        <tr><td>코스닥시장 공모</td><td>2026년 11월 03일</td><td>2026년 11월 04일</td><td>2026년 11월 06일</td></tr></table>"""
        self.assertEqual(dart_subscription_dates(body), ("2026-11-03", "2026-11-04"))
        dotted = body.replace("2026년 11월 03일", "2026.11.03").replace("2026년 11월 04일", "2026.11.04")
        self.assertEqual(dart_subscription_dates(dotted), ("2026-11-03", "2026-11-04"))
        self.assertIsNone(dart_subscription_dates("report without schedule"))

    def test_phase_parser_does_not_hide_errors(self):
        import tempfile
        from pathlib import Path
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "audit.log"
            path.write_text("ERROR [missing_x] one\nWARNING [pending] two\nERROR [conflict] three\n")
            self.assertEqual(phase_errors(path), {("missing_x", "one"), ("conflict", "three")})


    def test_prospectus_general_investor_adjacent_rows(self):
        # Shape of saved official DART rcpNo=20261110000261 (MBD).
        table = """<TABLE><TR><TD rowspan='4'>청약기일</TD>
        <TD rowspan='2'>기관투자자</TD><TD>개시일</TD><TD>2026년 11월 12일(목)</TD></TR>
        <TR><TD>종료일</TD><TD>2026년 11월 13일(금)</TD></TR>
        <TR><TD rowspan='2'>일반투자자</TD><TD>개시일</TD><TD>2026년 11월 12일(목)</TD></TR>
        <TR><TD>종료일</TD><TD>2026년 11월 13일(금)</TD></TR></TABLE>"""
        self.assertEqual(dart_subscription_dates(table), ("2026-11-12", "2026-11-13"))
        for original, replacement in (("11월 12일", "11월 32일"),
                                      ("11월 13일", ""), ("개시일", "납입기일"),
                                      ("11월 12일", "11월 14일")):
            with self.subTest(replacement=replacement):
                self.assertIsNone(dart_subscription_dates(table.replace(original, replacement)))
        self.assertIsNone(dart_subscription_dates(table + table.replace("11월 12일", "11월 11일")))

    def test_legacy_schedule_invalid_and_conflicting_dates_fail_closed(self):
        self.assertIsNone(dart_subscription_dates("일반투자자 청약일: 2026.02.30 ~ 2026.03.01"))
        self.assertIsNone(dart_subscription_dates(
            "일반투자자 청약일: 2026.11.12 ~ 2026.11.13 "
            "일반투자자 청약일: 2026.11.11 ~ 2026.11.13"))


if __name__ == "__main__":
    unittest.main()


class WeekdayExplicitRangeTests(unittest.TestCase):
    def test_weekday_range(self):
        self.assertEqual(dart_subscription_dates('일반청약자 청약일: 2026년 11월 12일(목) ~ 2026년 11월 13일(금)'), ('2026-11-12', '2026-11-13'))

    def test_weekday_negative_ranges(self):
        for line in [
            '일반청약자 청약일: 2026년 13월 12일(목) ~ 2026년 13월 13일(금)',
            '일반청약자 청약일: 2026년 11월 31일(목) ~ 2026년 11월 32일(금)',
            '일반청약자 청약일: 2026년 11월 13일(금) ~ 2026년 11월 12일(목)',
            '일반청약자 청약일: 2026년 11월 12일(목) ~ 13일(금)',
            '일반청약자 청약일: 2026년 11월 12일(목) ~',
            '일반청약자 청약일: 2026년 11월 12일(목) ~ 2026년 11월 13일(금) 일반청약자 청약일: 2026년 11월 14일(토) ~ 2026년 11월 15일(일)',
        ]:
            with self.subTest(line=line):
                self.assertIsNone(dart_subscription_dates(line))
