import unittest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from check_verified_delta import issue_additions, permitted_null_issue, phase_errors, rows


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

    def test_phase_parser_does_not_hide_errors(self):
        import tempfile
        from pathlib import Path
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "audit.log"
            path.write_text("ERROR [missing_x] one\nWARNING [pending] two\nERROR [conflict] three\n")
            self.assertEqual(phase_errors(path), {("missing_x", "one"), ("conflict", "three")})


if __name__ == "__main__":
    unittest.main()
