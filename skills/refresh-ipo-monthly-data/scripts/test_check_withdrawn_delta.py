import hashlib
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from check_withdrawn_delta import check, exact_feed_removal_result


class WithdrawnDeltaTests(unittest.TestCase):
    def test_feed_removal_is_exact_and_order_preserving(self):
        old = [{"id": "a"}, {"id": "b"}, {"id": "c"}]
        self.assertEqual(exact_feed_removal_result(old, [old[0], old[2]], ["b"]), (True, ""))
        self.assertFalse(exact_feed_removal_result(old, old, ["b"])[0])
        self.assertFalse(exact_feed_removal_result(old, [old[2], old[0]], ["b"])[0])
        self.assertFalse(exact_feed_removal_result(old, [old[0], old[2]], ["b", "b"])[0])

    def _candidate(self, temp):
        root = Path(temp)
        repo = root / "repo"
        repo.mkdir()
        for rel in ("data/discovered", "data", "ipo_competition_data/stocks", "ipo_competition_data/yearly"):
            (repo / rel).mkdir(parents=True, exist_ok=True)
        targets = [
            {"id": "A_2026-11-12", "company": "A", "sourceName": "a", "rawFile": "raw/a.html", "sourceUrl": "https://dart.example/a", "rawSha256": None, "filingDate": "2026-11-11"},
            {"id": "B_2026-11-12", "company": "B", "sourceName": "b", "rawFile": "raw/b.html", "sourceUrl": "https://dart.example/b", "rawSha256": None, "filingDate": "2026-11-11"},
        ]
        events, details, entries = [], {}, []
        sources = []
        for t in targets:
            event = {"id": t["id"], "company": t["company"], "subscriptionStart": "2026-11-12", "subscriptionEnd": "2026-11-13"}
            stock = {"id": t["id"], "company": t["company"], "snapshots": [{"capturedAt": "old"}]}
            events.append(event)
            details[t["id"]] = stock
            raw = (root / t["rawFile"])
            raw.parent.mkdir(parents=True, exist_ok=True)
            raw.write_text("official " + t["id"])
            digest = hashlib.sha256(raw.read_bytes()).hexdigest()
            t["rawSha256"] = digest
            started, ended = "2026-11-12T08:00:00+09:00", "2026-11-12T08:00:01+09:00"
            sources.append({"name": t["sourceName"], "url": t["sourceUrl"], "status": 200, "sha256": digest, "bytes": raw.stat().st_size, "rawFile": t["rawFile"], "requestStartedAtKst": started, "responseCompletedAtKst": ended})
            entries.append({"id": t["id"], "company": t["company"], "lifecycleStatus": "withdrawn", "filedDate": t["filingDate"], "originalEvent": event, "originalStockDetail": stock, "withdrawalEvidence": {"sourceUrl": t["sourceUrl"], "rawFile": t["rawFile"], "rawSha256": digest, "requestStartedAtKst": started, "responseCompletedAtKst": ended, "timezone": "Asia/Seoul", "language": "ko", "filingType": "철회신고서"}})
        canonical = {"schemaVersion": 1, "generatedAt": "base", "stocks": events}
        (repo / "data/discovered/ipo_events.json").write_text(json.dumps(canonical))
        (repo / "data/withdrawn_ipo_events.json").write_text(json.dumps({"schemaVersion": 1, "entries": []}))
        feed_paths = ["ipo_competition_data/index.json", "ipo_competition_data/upcoming.json", "ipo_competition_data/dashboard.json", "ipo_competition_data/yearly/2026.json"]
        for t in targets:
            (repo / f"ipo_competition_data/stocks/{t['id']}.json").write_text(json.dumps(details[t["id"]]))
        for feed in feed_paths:
            (repo / feed).parent.mkdir(parents=True, exist_ok=True)
            (repo / feed).write_text(json.dumps({"generatedAt": "base", "stocks": [{"id": t["id"], "company": t["company"]} for t in targets] + [{"id": "KEEP"}]}))

        def git(*args):
            return subprocess.check_output(["git", "-C", str(repo), *args], text=True).strip()
        git("init", "-q")
        git("config", "user.email", "test@example.invalid")
        git("config", "user.name", "Test")
        git("add", ".")
        git("commit", "-qm", "base")
        base = git("rev-parse", "HEAD")
        canonical["stocks"] = []
        (repo / "data/discovered/ipo_events.json").write_text(json.dumps(canonical))
        (repo / "data/withdrawn_ipo_events.json").write_text(json.dumps({"schemaVersion": 1, "entries": entries}))
        for t in targets:
            (repo / f"ipo_competition_data/stocks/{t['id']}.json").unlink()
        for feed in feed_paths:
            p = repo / feed
            data = json.loads(p.read_text())
            data["generatedAt"] = "2026-11-12T09:00:00"
            data["stocks"] = [row for row in data["stocks"] if row["id"] == "KEEP"]
            p.write_text(json.dumps(data))
        source_record = root / "sources.json"
        source_record.write_text(json.dumps({"sources": sources}))
        manifest = {
            "baseHead": base,
            "asOfDate": "2026-11-12",
            "targets": targets,
            "canonicalEventPath": "data/discovered/ipo_events.json",
            "withdrawalArchivePath": "data/withdrawn_ipo_events.json",
            "feedPaths": feed_paths,
            "expectedFeedPaths": feed_paths,
            "generation": {"generatedAt": "2026-11-12T09:00:00", "observedAtKst": "2026-11-12T09:00:00+09:00"},
            "allowedChangedPaths": ["data/discovered/ipo_events.json", "data/withdrawn_ipo_events.json"] + [f"ipo_competition_data/stocks/{t['id']}.json" for t in targets] + feed_paths,
        }
        manifest_path = root / "manifest.json"
        manifest_path.write_text(json.dumps(manifest))
        baseline_phase, candidate_phase = root / "baseline-phase.log", root / "candidate-phase.log"
        baseline_phase.write_text("ERROR [old] carried\n")
        candidate_phase.write_text("ERROR [old] carried\n")
        baseline_comp, candidate_comp = root / "baseline-comp.json", root / "candidate-comp.json"
        baseline_comp.write_text(json.dumps({"issues": [{"code": "old", "eventId": "OTHER", "severity": "ERROR"}]}))
        candidate_comp.write_text(json.dumps({"issues": [{"code": "old", "eventId": "OTHER", "severity": "ERROR"}]}))
        return locals()

    def test_exact_withdrawal_candidate_passes_and_preserves_carried_errors(self):
        with tempfile.TemporaryDirectory() as temp:
            x = self._candidate(temp)
            result = check(x["repo"], x["manifest_path"], x["source_record"], x["root"], x["baseline_phase"], x["candidate_phase"], x["baseline_comp"], x["candidate_comp"])
            self.assertEqual(result["status"], "PASS", result["failures"])
            self.assertTrue(result["fullAudit"]["residualErrorsRemain"])
            self.assertEqual(result["fullAudit"]["newPhaseErrors"], 0)

    def test_non_target_feed_change_fails(self):
        with tempfile.TemporaryDirectory() as temp:
            x = self._candidate(temp)
            feed = x["repo"] / "ipo_competition_data/index.json"
            data = json.loads(feed.read_text())
            data["stocks"].append({"id": "UNRELATED"})
            feed.write_text(json.dumps(data))
            result = check(x["repo"], x["manifest_path"], x["source_record"], x["root"], x["baseline_phase"], x["candidate_phase"], x["baseline_comp"], x["candidate_comp"])
            self.assertEqual(result["status"], "FAIL")
            self.assertTrue(any("feed membership/scope" in x for x in result["failures"]))

    def test_archive_loss_fails(self):
        with tempfile.TemporaryDirectory() as temp:
            x = self._candidate(temp)
            archive = x["repo"] / "data/withdrawn_ipo_events.json"
            data = json.loads(archive.read_text())
            data["entries"][0]["originalStockDetail"]["snapshots"] = []
            archive.write_text(json.dumps(data))
            result = check(x["repo"], x["manifest_path"], x["source_record"], x["root"], x["baseline_phase"], x["candidate_phase"], x["baseline_comp"], x["candidate_comp"])
            self.assertEqual(result["status"], "FAIL")
            self.assertTrue(any("not preserved exactly" in x for x in result["failures"]))


if __name__ == "__main__":
    unittest.main()
