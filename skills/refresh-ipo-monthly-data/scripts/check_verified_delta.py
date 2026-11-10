"""Fail-closed validation for publishing a verified IPO data delta.

The full monthly/completeness audits still run and their outstanding issues are
reported. This gate permits only unchanged, carried issues and explicitly
declared null fields; it rejects new errors, target-scope integrity errors,
unexpected file changes, bad provenance, or feed drift.
"""
import argparse
from datetime import datetime
import hashlib
import json
import math
from pathlib import Path, PurePosixPath
import re
import subprocess
import sys
from urllib.parse import urlparse


def read_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))


def sha256(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def at(obj, dotted):
    value = obj
    for key in dotted.split("."):
        if not isinstance(value, dict) or key not in value:
            return None
        value = value[key]
    return value


def set_issue_signature(issue):
    return tuple(str(issue.get(k, "")) for k in ("code", "eventId", "field", "detail", "severity"))


def permitted_null_issue(event_id, target, code, field, allowed_codes, allowed_fields, stock):
    return (event_id == target and code in allowed_codes and field in allowed_fields
            and at(stock, field) is None)


def issue_additions(baseline, candidate):
    return set(candidate) - set(baseline)


def phase_errors(path):
    found = set()
    for line in Path(path).read_text(encoding="utf-8-sig").splitlines():
        if not line.startswith("ERROR "):
            continue
        match = re.match(r"ERROR \[([^]]+)\] (.*)", line)
        if match:
            found.add((match.group(1), match.group(2)))
    return found


def issue_report(path):
    report = read_json(path)
    return {set_issue_signature(x) for x in report.get("issues", []) if x.get("severity") == "ERROR"}, report


def git(repo, *args):
    return subprocess.check_output(["git", "-c", "core.quotePath=false", "-C", str(repo), *args], text=True, stderr=subprocess.PIPE).strip()


def base_json(repo, base, rel):
    try:
        raw = subprocess.check_output(["git", "-C", str(repo), "show", f"{base}:{rel}"], stderr=subprocess.DEVNULL)
    except subprocess.CalledProcessError:
        return None
    return json.loads(raw.decode("utf-8-sig"))


def rows(data):
    if isinstance(data, list):
        return data
    if isinstance(data, dict) and isinstance(data.get("stocks"), list):
        return data["stocks"]
    raise ValueError("feed must be a list or an object with a stocks list")


def changed_leaf_paths(before, after, prefix=""):
    """Return changed JSON-pointer leaves, including additions/removals."""
    if isinstance(before, dict) and isinstance(after, dict):
        changed = set()
        for key in set(before) | set(after):
            path = prefix + "/" + str(key).replace("~", "~0").replace("/", "~1")
            if key not in before or key not in after:
                changed.add(path)
            else:
                changed |= changed_leaf_paths(before[key], after[key], path)
        return changed
    if isinstance(before, list) and isinstance(after, list):
        return set() if before == after else {prefix or "/"}
    return set() if before == after else {prefix or "/"}


def locate_event(data, target):
    if isinstance(data, dict):
        if data.get("id") == target:
            return data
        for value in data.values():
            found = locate_event(value, target)
            if found is not None:
                return found
    elif isinstance(data, list):
        for value in data:
            found = locate_event(value, target)
            if found is not None:
                return found
    return None


def feed_value(stock, field):
    mapping = {
        "offerPrice": "fundamentals.offerPrice",
        "institutionCompetitionRate": "fundamentals.institutionCompetitionRate",
        "institutionParticipants": "fundamentals.institutionParticipants",
        "lockupCommitmentRate": "fundamentals.lockupCommitmentRate",
        "latestCompetitionRate": "snapshots.0.competitionRate",
        "score": "analysis.score.overall",
        "grade": "analysis.score.grade",
        "decisionLevel": "analysis.decision.level",
        "expectedGainRate": "analysis.expectedReturn.expectedListingGainRate",
    }
    dotted = mapping.get(field, field)
    if dotted.startswith("snapshots."):
        parts = dotted.split(".")
        try:
            return stock[parts[0]][int(parts[1])][parts[2]]
        except (KeyError, IndexError, TypeError, ValueError):
            return None
    return at(stock, dotted)


def check(repo, manifest_path, source_record_path, source_root,
          baseline_phase, candidate_phase, baseline_completeness, candidate_completeness):
    repo = Path(repo).resolve()
    manifest = read_json(manifest_path)
    failures, notes = [], []
    base = manifest["baseHead"]
    if git(repo, "rev-parse", "HEAD") != base:
        failures.append("candidate HEAD differs from manifest baseHead")

    tracked = set(filter(None, git(repo, "diff", "--name-only", base, "--").splitlines()))
    staged = set(filter(None, git(repo, "diff", "--cached", "--name-only", base, "--").splitlines()))
    untracked = set(filter(None, git(repo, "ls-files", "--others", "--exclude-standard").splitlines()))
    actual = tracked | staged | untracked
    expected_paths = set(manifest["allowedChangedPaths"])
    if actual != expected_paths:
        failures.append(f"changed path scope mismatch; unexpected={sorted(actual-expected_paths)}, missing={sorted(expected_paths-actual)}")

    target = manifest["target"]["id"]
    stock_rel = f"ipo_competition_data/stocks/{target}.json"
    stock_path = repo / stock_rel
    if not stock_path.is_file():
        failures.append("target stock detail is missing")
        stock = {}
    else:
        stock = read_json(stock_path)
        if stock.get("id") != target or stock.get("company") != manifest["target"]["company"]:
            failures.append("target stock identity mismatch")
        if stock.get("identifiers", {}).get("stockCode") != manifest["target"]["stockCode"]:
            failures.append("target stock code mismatch")
        for field, expected in manifest["verifiedFields"].items():
            value = at(stock, field)
            if isinstance(expected, float):
                if not isinstance(value, (float, int)) or not math.isclose(value, expected, rel_tol=1e-12, abs_tol=1e-12):
                    failures.append(f"verified field mismatch: {field}")
            elif value != expected:
                failures.append(f"verified field mismatch: {field}")
        for field in manifest.get("retainedNullFields", []):
            if at(stock, field) is not None:
                failures.append(f"declared missing field is not null: {field}")
        old = base_json(repo, base, stock_rel)
        if old:
            old_snaps = old.get("snapshots", [])
            new_snaps = stock.get("snapshots", [])
            if new_snaps[:len(old_snaps)] != old_snaps:
                failures.append("existing target snapshots were not preserved as a prefix")
        if stock.get("snapshots", []) != manifest.get("expectedSnapshots", stock.get("snapshots", [])):
            failures.append("target snapshot contents differ from declared expected state")
        old = base_json(repo, base, stock_rel)
        actual_leafs = changed_leaf_paths(old, stock)
        allowed = set(manifest.get("allowedStockLeafPointers", []))
        if actual_leafs != allowed:
            failures.append(f"target detail field scope mismatch; unexpected={sorted(actual_leafs-allowed)}, missing={sorted(allowed-actual_leafs)}")

    for path in manifest["changedDataPaths"]:
        p = repo / path
        if not p.is_file():
            failures.append(f"changed data file missing: {path}")
            continue
        try:
            new_data = read_json(p)
        except Exception as exc:
            failures.append(f"invalid JSON {path}: {exc}")
            continue
        old_data = base_json(repo, base, path)
        if path == stock_rel or path in manifest.get("canonicalPaths", []):
            continue
        if path not in manifest["feedPaths"]:
            failures.append(f"data path is not classified as target, canonical input, or feed: {path}")
            continue
        try:
            new_rows = rows(new_data)
            old_rows = rows(old_data) if old_data is not None else []
        except ValueError as exc:
            failures.append(f"invalid feed structure {path}: {exc}")
            continue
        new_target = [x for x in new_rows if x.get("id") == target]
        old_target = [x for x in old_rows if x.get("id") == target]
        membership = manifest["feedMembership"].get(path, "preserve")
        if membership == "one" and len(new_target) != 1:
            failures.append(f"target must occur exactly once in {path}")
        if membership == "none" and new_target:
            failures.append(f"target must not occur in {path}")
        if membership == "preserve" and len(new_target) != len(old_target):
            failures.append(f"target membership changed unexpectedly in {path}")
        if [x for x in new_rows if x.get("id") != target] != [x for x in old_rows if x.get("id") != target]:
            failures.append(f"non-target feed rows changed in {path}")
        if membership == "one" and not old_target and len(new_target) == 1:
            reference_path = manifest.get("newMembershipReferencePath")
            if not reference_path:
                failures.append(f"new target membership lacks a declared reference row: {path}")
            else:
                reference_data = read_json(repo / reference_path)
                reference_rows = rows(reference_data)
                reference_target = [x for x in reference_rows if x.get("id") == target]
                if len(reference_target) != 1 or new_target[0] != reference_target[0]:
                    failures.append(f"new target feed row must exactly match the declared canonical feed row: {path}")
        for row in new_target:
            if row.get("path") != f"stocks/{target}.json":
                failures.append(f"bad target path in {path}")
            for field in manifest.get("feedParityFields", []):
                if field in row and row[field] != feed_value(stock, field):
                    failures.append(f"target feed/detail mismatch in {path}: {field}")
        if len(old_target) == len(new_target) == 1:
            actual_feed_fields = changed_leaf_paths(old_target[0], new_target[0])
            expected_feed_fields = set(manifest.get("allowedFeedLeafPointers", {}).get(path, []))
            if actual_feed_fields != expected_feed_fields:
                failures.append(f"target feed field scope mismatch in {path}; unexpected={sorted(actual_feed_fields-expected_feed_fields)}, missing={sorted(expected_feed_fields-actual_feed_fields)}")

    event_path = repo / manifest["canonicalEventPath"]
    event = locate_event(read_json(event_path), target)
    if event is None:
        failures.append("canonical event input is missing the target")
    else:
        old_event = locate_event(base_json(repo, base, manifest["canonicalEventPath"]), target)
        if old_event is None:
            failures.append("canonical baseline event is missing the target")
        else:
            actual_leafs = changed_leaf_paths(old_event, event)
            expected_leafs = set(manifest.get("allowedCanonicalEventLeafPointers", []))
            if actual_leafs != expected_leafs:
                failures.append(f"canonical event field scope mismatch; unexpected={sorted(actual_leafs-expected_leafs)}, missing={sorted(expected_leafs-actual_leafs)}")
        for field, expected in manifest["canonicalVerifiedFields"].items():
            if at(event, field) != expected:
                failures.append(f"canonical event differs from verified field: {field}")
    manual_path = repo / manifest["canonicalManualPath"]
    manual = read_json(manual_path).get(target)
    if not isinstance(manual, dict):
        failures.append("canonical manual fundamentals are missing the target")
    else:
        old_manual_all = base_json(repo, base, manifest["canonicalManualPath"])
        old_manual = old_manual_all.get(target) if isinstance(old_manual_all, dict) else None
        if not isinstance(old_manual, dict):
            failures.append("canonical baseline manual fundamentals are missing the target")
        else:
            actual_leafs = changed_leaf_paths(old_manual, manual)
            expected_leafs = set(manifest.get("allowedCanonicalManualLeafPointers", []))
            if actual_leafs != expected_leafs:
                failures.append(f"canonical manual field scope mismatch; unexpected={sorted(actual_leafs-expected_leafs)}, missing={sorted(expected_leafs-actual_leafs)}")
        for field, expected in manifest["verifiedManualFields"].items():
            if manual.get(field) != expected:
                failures.append(f"manual canonical field mismatch: {field}")

    for path in manifest.get("assertNotMemberPaths", []):
        p = repo / path
        if p.exists() and any(row.get("id") == target for row in rows(read_json(p))):
            failures.append(f"target has unexpected membership in {path}")

    source_record = read_json(source_record_path)
    if source_record.get("issuer") != manifest["target"]["issuer"] or str(source_record.get("stockCode")) != str(manifest["target"]["stockCode"]):
        failures.append("source record issuer/stock identity mismatch")
    if source_record.get("language") != "ko" or source_record.get("timezone") != "Asia/Seoul":
        failures.append("source language/timezone mismatch")
    retail = source_record.get("dailyRetailObservation", {})
    for field in manifest.get("sourceNullRetailFields", []):
        if retail.get(field) is not None:
            failures.append(f"source record has an unexpected retail value: {field}")
    if stock.get("snapshots", []) != [] and manifest.get("requireEmptyRetailSnapshots", False):
        failures.append("retail snapshots must remain empty when no live source values were observed")
    source_root = Path(source_root).resolve()
    for name, source in source_record.get("sources", {}).items():
        url = urlparse(source.get("url", ""))
        raw_rel = PurePosixPath(source.get("rawFile", ""))
        raw_path = (source_root / Path(*raw_rel.parts)).resolve()
        if url.scheme != "https" or not url.netloc or source.get("httpStatus") != 200:
            failures.append(f"source is not successful official HTTPS: {name}")
        if ".." in raw_rel.parts or source_root not in raw_path.parents:
            failures.append(f"unsafe source raw path: {name}")
        elif not raw_path.is_file() or sha256(raw_path) != source.get("rawSha256"):
            failures.append(f"source raw hash mismatch: {name}")
        for stamp in ("requestStartedAtKst", "responseCompletedAtKst"):
            try:
                if datetime.fromisoformat(source[stamp]).utcoffset() is None:
                    raise ValueError()
            except (KeyError, TypeError, ValueError):
                failures.append(f"source observation timestamp/timezone missing: {name}/{stamp}")

    base_phase = phase_errors(baseline_phase)
    new_phase = phase_errors(candidate_phase)
    phase_additions = issue_additions(base_phase, new_phase)
    allowed_phase_codes = set(manifest.get("allowedNewNullIssueCodes", []))
    allowed_null = set(manifest.get("retainedNullFields", []))
    phase_field_map = manifest.get("phaseNullFieldMap", {})
    for code, message in new_phase:
        if target in message:
            field = phase_field_map.get(code)
            if not permitted_null_issue(target, target, code, field, allowed_phase_codes, allowed_null, stock):
                failures.append(f"hard target-scope phase error: [{code}] {message}")
    for code, message in phase_additions:
        field = phase_field_map.get(code)
        if not (target in message and permitted_null_issue(target, target, code, field, allowed_phase_codes, allowed_null, stock)):
            failures.append(f"new phase audit error: [{code}] {message}")
    base_comp, baseline_comp_report = issue_report(baseline_completeness)
    new_comp, candidate_comp_report = issue_report(candidate_completeness)
    comp_additions = issue_additions(base_comp, new_comp)
    for code, event_id, field, detail, severity in comp_additions:
        if not permitted_null_issue(event_id, target, code, field, {"missing_review"}, allowed_null, stock):
            failures.append(f"new completeness audit error: {code}/{event_id}/{field}")
    for issue in candidate_comp_report.get("issues", []):
        if issue.get("severity") == "ERROR" and issue.get("eventId") == target:
            if permitted_null_issue(issue.get("eventId"), target, issue.get("code"), issue.get("field"), {"missing_review"}, allowed_null, stock):
                continue
            failures.append(f"hard target-scope audit error: {issue.get('code')}/{target}/{issue.get('field','')}")

    report = {
        "status": "PASS" if not failures else "FAIL",
        "policy": "verified_delta_with_deferred_nulls_v1",
        "baseHead": base,
        "targetId": target,
        "actualChangedPaths": sorted(actual),
        "fullAudit": {
            "baselinePhaseErrors": len(base_phase),
            "candidatePhaseErrors": len(new_phase),
            "newPhaseErrors": len(phase_additions),
            "baselineCompletenessErrors": len(base_comp),
            "candidateCompletenessErrors": len(new_comp),
            "newCompletenessErrors": len(comp_additions),
            "residualErrorsRemain": bool(new_phase or new_comp),
            "carriedForwardUnchangedOutOfScope": sorted(set_issue_signature(x) for x in candidate_comp_report.get("issues", []) if x.get("severity") == "ERROR" and x.get("eventId") != target and set_issue_signature(x) in {set_issue_signature(y) for y in baseline_comp_report.get("issues", []) if y.get("severity") == "ERROR"}),
            "interpretation": "full audits are reported, not claimed green; unchanged out-of-scope issues remain visible, new errors and target-scope integrity errors block"
        },
        "failures": failures,
        "notes": notes
    }
    return report


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
