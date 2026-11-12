"""Fail-closed validation for source-backed IPO withdrawal removals.

This gate is intentionally separate from the active single-target verified-delta
gate. It accepts only an exact multi-target withdrawal disposition, preserves
the original event and detail in a withdrawal ledger, removes all active feed
and detail membership, and rejects unrelated changes or new audit errors.
"""
import argparse
from datetime import datetime
from datetime import date
import json
from pathlib import Path, PurePosixPath
import sys
from urllib.parse import urlparse

from check_verified_delta import (
    base_json,
    git,
    issue_additions,
    issue_report,
    locate_event,
    phase_errors,
    read_json,
    rows,
    sha256,
)


def changed_paths(repo, base):
    tracked = set(filter(None, git(repo, "diff", "--name-only", base, "--").splitlines()))
    staged = set(filter(None, git(repo, "diff", "--cached", "--name-only", base, "--").splitlines()))
    untracked = set(filter(None, git(repo, "ls-files", "--others", "--exclude-standard").splitlines()))
    return tracked | staged | untracked


def exact_feed_removal_result(old_rows, new_rows, removal_ids):
    ids = list(removal_ids)
    if not ids or len(ids) != len(set(ids)):
        return False, "removal ids must be non-empty and unique"
    old_ids = [row.get("id") for row in old_rows]
    new_ids = [row.get("id") for row in new_rows]
    for item_id in ids:
        if old_ids.count(item_id) != 1:
            return False, f"target must occur exactly once in base: {item_id}"
        if item_id in new_ids:
            return False, f"target remains in candidate: {item_id}"
    expected = [row for row in old_rows if row.get("id") not in set(ids)]
    if new_rows != expected:
        return False, "candidate feed differs beyond declared removals"
    return True, ""


def check(repo, manifest_path, source_record_path, source_root,
          baseline_phase, candidate_phase, baseline_completeness,
          candidate_completeness):
    repo = Path(repo).resolve()
    source_root = Path(source_root).resolve()
    manifest = read_json(manifest_path)
    failures = []
    base = manifest["baseHead"]
    if git(repo, "rev-parse", "HEAD") != base:
        failures.append("candidate HEAD differs from manifest baseHead")

    actual = changed_paths(repo, base)
    expected = set(manifest["allowedChangedPaths"])
    if actual != expected:
        failures.append(f"changed path scope mismatch; unexpected={sorted(actual-expected)}, missing={sorted(expected-actual)}")

    targets = manifest.get("targets", [])
    ids = [t.get("id") for t in targets]
    if len(ids) != 2 or len(set(ids)) != 2 or not all(ids):
        failures.append("withdrawal manifest must contain exactly two unique target IDs")
    canonical_path = manifest["canonicalEventPath"]
    archive_path = manifest["withdrawalArchivePath"]
    active_doc = read_json(repo / canonical_path)
    base_events = base_json(repo, base, canonical_path)
    archive = read_json(repo / archive_path) if (repo / archive_path).is_file() else {}
    base_archive = base_json(repo, base, archive_path)
    archive_entries = archive.get("entries", []) if isinstance(archive, dict) else []
    base_archive_entries = base_archive.get("entries", []) if isinstance(base_archive, dict) else []
    if archive.get("schemaVersion") != 1 or not isinstance(archive_entries, list):
        failures.append("withdrawal archive schema is invalid")
    if base_events is not None:
        old_active_rows = base_events.get("stocks", [])
        new_active_rows = active_doc.get("stocks", [])
        expected_active_rows = [x for x in old_active_rows if x.get("id") not in set(ids)]
        if new_active_rows != expected_active_rows:
            failures.append("canonical active events differ beyond the declared withdrawals")
    expected_archived_ids = set(ids) | {x.get("id") for x in base_archive_entries if isinstance(x, dict)}
    if {x.get("id") for x in archive_entries if isinstance(x, dict)} != expected_archived_ids:
        failures.append("withdrawal archive target set changed unexpectedly")
    if [x for x in archive_entries if x.get("id") not in set(ids)] != [x for x in base_archive_entries if x.get("id") not in set(ids)]:
        failures.append("pre-existing withdrawal archive entries changed")

    source_record = read_json(source_record_path)
    sources = {s.get("name"): s for s in source_record.get("sources", []) if isinstance(s, dict)}
    as_of = manifest.get("asOfDate")
    for target in targets:
        item_id = target["id"]
        old_target_events = [x for x in (base_events or {}).get("stocks", []) if x.get("id") == item_id]
        base_event = locate_event(base_events, item_id) if base_events is not None else None
        candidate_event = locate_event(active_doc, item_id)
        archived = [e for e in archive_entries if isinstance(e, dict) and e.get("id") == item_id]
        if len(old_target_events) != 1 or base_event is None:
            failures.append(f"baseline canonical event missing or duplicated: {item_id}")
        if candidate_event is not None:
            failures.append(f"withdrawn target remains in canonical active events: {item_id}")

        stock_rel = f"ipo_competition_data/stocks/{item_id}.json"
        base_stock = base_json(repo, base, stock_rel)
        if base_stock is None:
            failures.append(f"baseline stock detail missing: {item_id}")
        elif base_stock.get("id") != item_id or base_stock.get("company") != target.get("company"):
            failures.append(f"baseline stock detail identity mismatch: {item_id}")
        if base_event is not None and (base_event.get("id") != item_id or base_event.get("company") != target.get("company")):
            failures.append(f"baseline canonical event identity mismatch: {item_id}")
        if (repo / stock_rel).exists():
            failures.append(f"withdrawn target detail remains publicly addressable: {item_id}")
        if len(archived) != 1:
            failures.append(f"withdrawal archive must contain exactly one row: {item_id}")
            continue
        row = archived[0]
        if row.get("lifecycleStatus") != "withdrawn" or row.get("company") != target.get("company"):
            failures.append(f"withdrawal archive identity/status mismatch: {item_id}")
        if row.get("originalEvent") != base_event:
            failures.append(f"original canonical event was not preserved exactly: {item_id}")
        if row.get("originalStockDetail") != base_stock:
            failures.append(f"original stock detail/history was not preserved exactly: {item_id}")

        evidence = row.get("withdrawalEvidence", {})
        source = sources.get(target.get("sourceName"), {})
        if evidence.get("sourceUrl") != target.get("sourceUrl") or source.get("url") != target.get("sourceUrl"):
            failures.append(f"withdrawal source URL mismatch: {item_id}")
        if source.get("status") != 200 or source.get("sha256") != target.get("rawSha256") or source.get("bytes", 0) <= 0:
            failures.append(f"official source record status/hash mismatch: {item_id}")
        if evidence.get("rawSha256") != target.get("rawSha256") or evidence.get("rawFile") != target.get("rawFile"):
            failures.append(f"archive/source raw file identity mismatch: {item_id}")
        raw_rel = PurePosixPath(target.get("rawFile", ""))
        raw_path = (source_root / Path(*raw_rel.parts)).resolve()
        if ".." in raw_rel.parts or source_root not in raw_path.parents:
            failures.append(f"unsafe raw source path: {item_id}")
        elif not raw_path.is_file() or sha256(raw_path) != target.get("rawSha256"):
            failures.append(f"official source raw hash mismatch: {item_id}")
        for key in ("requestStartedAtKst", "responseCompletedAtKst"):
            if evidence.get(key) != source.get(key):
                failures.append(f"archive/source timestamp mismatch: {item_id}/{key}")
            try:
                if datetime.fromisoformat(source[key]).utcoffset() is None:
                    raise ValueError()
            except (KeyError, TypeError, ValueError):
                failures.append(f"source timestamp/timezone missing: {item_id}/{key}")
        url = urlparse(target.get("sourceUrl", ""))
        if evidence.get("filingType") != "철회신고서" or evidence.get("language") != "ko" or evidence.get("timezone") != "Asia/Seoul":
            failures.append(f"withdrawal source type/language/timezone mismatch: {item_id}")
        try:
            filing_date = date.fromisoformat(target.get("filingDate", ""))
            as_of_date = date.fromisoformat(as_of or "")
            date_valid = filing_date <= as_of_date
        except (TypeError, ValueError):
            date_valid = False
        if url.scheme != "https" or not url.netloc or not date_valid:
            failures.append(f"invalid official withdrawal source/date: {item_id}")
        if target.get("filingDate") != row.get("filedDate"):
            failures.append(f"withdrawal filing date mismatch/future date: {item_id}")

    feed_paths = manifest.get("feedPaths", [])
    if set(feed_paths) != set(manifest.get("expectedFeedPaths", [])):
        failures.append("feed path set differs from the explicit expected feed set")
    base_feed_membership = set()
    for rel in filter(None, git(repo, "ls-tree", "-r", "--name-only", base, "ipo_competition_data").splitlines()):
        if not rel.endswith(".json") or "/stocks/" in rel:
            continue
        data = base_json(repo, base, rel)
        try:
            if data is not None and any(row.get("id") in set(ids) for row in rows(data)):
                base_feed_membership.add(rel)
        except ValueError:
            continue
    if base_feed_membership != set(feed_paths):
        failures.append(f"declared feed set differs from actual baseline membership: {sorted(base_feed_membership)}")
    generated_at = manifest.get("generation", {}).get("generatedAt")
    generation_observed = manifest.get("generation", {}).get("observedAtKst")
    if not generated_at or not generation_observed:
        failures.append("actual feed generation timestamp evidence missing")
    else:
        try:
            observed = datetime.fromisoformat(generation_observed)
            if observed.utcoffset() is None or generated_at not in generation_observed:
                raise ValueError()
        except (TypeError, ValueError):
            failures.append("feed generation timestamp is invalid or not timezone-aware")
    for path in feed_paths:
        old_data = base_json(repo, base, path)
        new_path = repo / path
        if old_data is None or not new_path.is_file():
            failures.append(f"feed missing from baseline or candidate: {path}")
            continue
        new_data = read_json(new_path)
        try:
            old_rows, new_rows = rows(old_data), rows(new_data)
        except ValueError as exc:
            failures.append(f"invalid feed structure {path}: {exc}")
            continue
        ok, reason = exact_feed_removal_result(old_rows, new_rows, ids)
        if not ok:
            failures.append(f"feed membership/scope check failed for {path}: {reason}")
        if new_data.get("generatedAt") != generated_at:
            failures.append(f"feed generatedAt does not match actual generation evidence: {path}")
    for path in (repo / "ipo_competition_data").rglob("*.json"):
        rel = path.relative_to(repo).as_posix()
        if "/stocks/" in rel:
            continue
        try:
            data = read_json(path)
            if any(row.get("id") in set(ids) for row in rows(data)):
                failures.append(f"withdrawn target remains in an unlisted public feed: {rel}")
        except (ValueError, TypeError):
            continue

    phase_before, phase_after = phase_errors(baseline_phase), phase_errors(candidate_phase)
    new_phase = issue_additions(phase_before, phase_after)
    for code, message in new_phase:
        failures.append(f"new phase audit error: [{code}] {message}")
    comp_before, before_report = issue_report(baseline_completeness)
    comp_after, after_report = issue_report(candidate_completeness)
    new_comp = issue_additions(comp_before, comp_after)
    for code, event_id, field, detail, severity in new_comp:
        failures.append(f"new completeness audit error: {code}/{event_id}/{field}")

    return {
        "status": "PASS" if not failures else "FAIL",
        "policy": "withdrawn_multi_target_delta_v1",
        "baseHead": base,
        "targetIds": ids,
        "actualChangedPaths": sorted(actual),
        "fullAudit": {
            "baselinePhaseErrors": len(phase_before),
            "candidatePhaseErrors": len(phase_after),
            "newPhaseErrors": len(new_phase),
            "baselineCompletenessErrors": len(comp_before),
            "candidateCompletenessErrors": len(comp_after),
            "newCompletenessErrors": len(new_comp),
            "residualErrorsRemain": bool(phase_after or comp_after),
            "carriedForwardUnchangedOutOfScope": sorted(set((x[0], x[1], x[2], x[3], x[4]) for x in comp_after if x[1] not in ids and x in comp_before)),
            "interpretation": "unchanged out-of-scope audit issues remain open; only new issues block"
        },
        "failures": failures,
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    for name in ("repo", "manifest", "source-record", "source-root", "baseline-phase", "candidate-phase", "baseline-completeness", "candidate-completeness", "report"):
        ap.add_argument("--" + name, required=True)
    args = ap.parse_args()
    result = check(args.repo, args.manifest, args.source_record, args.source_root,
                   args.baseline_phase, args.candidate_phase,
                   args.baseline_completeness, args.candidate_completeness)
    Path(args.report).write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
