"""Fail-closed validation for publishing a verified IPO data delta.

The full monthly/completeness audits still run and their outstanding issues are
reported. This gate permits only unchanged, carried issues and explicitly
declared null fields; it rejects new errors, target-scope integrity errors,
unexpected file changes, bad provenance, or feed drift.
"""
import argparse
from datetime import datetime, timezone
from html.parser import HTMLParser
import hashlib
import json
import math
from pathlib import Path, PurePosixPath
import re
import subprocess
import sys
from urllib.parse import urlparse, parse_qs


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


def exact_feed_removal_result(old_rows, new_rows, removal_ids):
    """Check that a feed differs only by explicitly declared row removals."""
    ids = list(removal_ids)
    if len(ids) != len(set(ids)):
        return False, "duplicate declared removal id"
    old_ids = [row.get("id") for row in old_rows]
    new_ids = [row.get("id") for row in new_rows]
    for item_id in ids:
        if old_ids.count(item_id) != 1:
            return False, f"removed id must occur exactly once in base: {item_id}"
        if item_id in new_ids:
            return False, f"removed id remains in candidate: {item_id}"
    expected = [row for row in old_rows if row.get("id") not in set(ids)]
    if new_rows != expected:
        return False, "candidate rows differ beyond declared removals"
    return True, ""


def latest_dart_search_filing(html_text):
    """Read the first filing row shown by DART's descending issuer search."""
    for row in re.findall(r"<tr\b[^>]*>(.*?)</tr>", html_text, re.S | re.I):
        rcp = re.search(r"openReportViewer\('([^']+)'", row)
        if not rcp:
            continue
        date_tokens = re.findall(r"<td[^>]*>\s*(20\d{2}\.\d{2}\.\d{2})\s*</td>", row, re.S | re.I)
        anchor = re.search(r"openReportViewer\('[^']+'[^>]*>(.*?)</a>", row, re.S | re.I)
        if not date_tokens or not anchor:
            return None
        title = re.sub(r"<[^>]+>", " ", anchor.group(1))
        title = re.sub(r"\s+", " ", title).strip()
        return {"rcpNo": rcp.group(1), "filingDate": date_tokens[0], "title": title}
    return None


def dart_document_text(html_text):
    class TextOnly(HTMLParser):
        def __init__(self):
            super().__init__()
            self.parts = []

        def handle_data(self, data):
            self.parts.append(data)

    parser = TextOnly()
    parser.feed(html_text)
    return re.sub(r"\s+", " ", " ".join(parser.parts)).strip()


def dart_subscription_dates(html_text):
    text = dart_document_text(html_text)
    date = r"(?:20\d{2}\s*년\s*\d{1,2}\s*월\s*\d{1,2}\s*일|20\d{2}\.\d{1,2}\.\d{1,2})"
    match = re.search(
        rf"청약개시일\s*청약종료일\s*납입기일\s*코스닥시장\s*공모\s*({date})\s*({date})",
        text,
    )

    def iso(value):
        digits = re.findall(r"\d+", value)
        return f"{int(digits[0]):04d}-{int(digits[1]):02d}-{int(digits[2]):02d}"

    if match:
        return iso(match.group(1)), iso(match.group(2))

    # A final prospectus can be the issuer's newest filing after subscription.
    # Accept its explicit general-investor schedule only when both endpoints are
    # written in the same line and resolve to one unique date pair.
    end_date = r"(?:(?:20\d{2}\s*년\s*)?\d{1,2}\s*월\s*\d{1,2}\s*일|20\d{2}\.\d{1,2}\.\d{1,2})"
    explicit_pairs = re.findall(
        rf"일반(?:투자자|청약자)\s*청약(?:일|기일)\s*[:：]\s*({date})\s*[~～]\s*({end_date})",
        text,
    )

    def iso_end(value, start):
        end_parts = [int(x) for x in re.findall(r"\d+", value)]
        start_year = int(re.findall(r"\d+", start)[0])
        if len(end_parts) == 2:
            end_parts.insert(0, start_year)
        return f"{end_parts[0]:04d}-{end_parts[1]:02d}-{end_parts[2]:02d}"

    pairs = {(iso(start), iso_end(end, start)) for start, end in explicit_pairs}
    return next(iter(pairs)) if len(pairs) == 1 else None


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
    source_root = Path(source_root).resolve()
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
        expected_generated_at = manifest.get("feedGeneratedAt", {}).get(path)
        if expected_generated_at and new_data.get("generatedAt") != expected_generated_at:
            failures.append(f"feed generatedAt differs from recorded actual generation time: {path}")
        new_target = [x for x in new_rows if x.get("id") == target]
        old_target = [x for x in old_rows if x.get("id") == target]
        membership = manifest["feedMembership"].get(path, "preserve")
        if membership in ("one", "canonical_window") and len(new_target) != 1:
            failures.append(f"target must occur exactly once in {path}")
        if membership == "none" and new_target:
            failures.append(f"target must not occur in {path}")
        if membership == "preserve" and len(new_target) != len(old_target):
            failures.append(f"target membership changed unexpectedly in {path}")
        removal_ids = manifest.get("allowedNonTargetRemovals", {}).get(path, [])
        if any(item_id == target for item_id in removal_ids):
            failures.append(f"target id cannot be declared as a non-target removal in {path}")
        if membership == "canonical_window":
            window_spec = manifest.get("canonicalWindow", {}).get(path, {})
            reference_path = manifest.get("newMembershipReferencePath")
            reference_data = read_json(repo / reference_path) if reference_path else None
            try:
                reference_rows = rows(reference_data) if reference_data is not None else []
            except ValueError:
                reference_rows = []
            reference_ids = [row.get("id") for row in reference_rows]
            old_by_id = {row.get("id"): row for row in old_rows}
            new_by_id = {row.get("id"): row for row in new_rows}
            reference_by_id = {row.get("id"): row for row in reference_rows}
            additions = manifest.get("allowedNonTargetAdditions", {}).get(path, [])
            actual_additions = set(new_by_id) - set(old_by_id) - {target}
            actual_removals = set(old_by_id) - set(new_by_id)
            if not window_spec or not reference_path:
                failures.append(f"canonical-window feed lacks explicit spec/reference: {path}")
            if (len(reference_rows) != window_spec.get("limit") or
                    window_spec.get("limit") != 60 or
                    len(reference_ids) != len(set(reference_ids))):
                failures.append(f"canonical-window reference must contain exactly 60 unique rows: {path}")
            generator_path = window_spec.get("generatorPath")
            generator_sha = window_spec.get("generatorSha256")
            generator_file = repo / generator_path if generator_path else None
            generator_text = generator_file.read_text(encoding="utf-8") if generator_file and generator_file.is_file() else ""
            if (not generator_file or not generator_file.is_file() or sha256(generator_file) != generator_sha or
                    f"feedItems(recent, {window_spec.get('limit')})" not in generator_text):
                failures.append(f"canonical-window generator/hash/60-row cap mismatch: {path}")
            # Bind the candidate's generatedAt and exact output bytes to a producer
            # execution record. The record itself is hash-pinned by the manifest;
            # this makes a forged/nonzero execution fail closed without a circular
            # manifest hash inside the record.
            execution = manifest.get("generationExecutionEvidence", {})
            record_rel = execution.get("recordPath")
            record_hash = execution.get("recordSha256")
            record_file = source_root / record_rel if record_rel else None
            try:
                record_safe = bool(record_file and record_file.resolve().is_relative_to(source_root))
            except (OSError, ValueError):
                record_safe = False
            if (not record_safe or not record_file.is_file() or not record_hash or
                    sha256(record_file) != record_hash):
                failures.append(f"canonical-window producer execution record missing or hash mismatch: {path}")
            else:
                try:
                    execution_record = read_json(record_file)
                    started = datetime.fromisoformat(execution_record["startedAtUtc"])
                    completed = datetime.fromisoformat(execution_record["completedAtUtc"])
                    generated = datetime.fromisoformat(execution_record["generatorReportedAt"])
                    if generated.tzinfo is None:
                        from zoneinfo import ZoneInfo
                        generated = generated.replace(tzinfo=ZoneInfo("Asia/Seoul"))
                    if started.tzinfo is None or completed.tzinfo is None:
                        raise ValueError("producer execution times must carry timezone offsets")
                    generated_utc = generated.astimezone(timezone.utc)
                    if execution_record.get("status") != "completed" or execution_record.get("exitCode") != 0:
                        failures.append(f"canonical-window producer did not complete successfully: {path}")
                    if not (started.astimezone(timezone.utc) <= generated_utc <= completed.astimezone(timezone.utc)):
                        failures.append(f"canonical-window generatedAt is outside producer execution interval: {path}")
                    if execution_record.get("generatorSha256") != generator_sha:
                        failures.append(f"canonical-window producer generator hash mismatch: {path}")
                    if execution_record.get("generatorReportedAt") != expected_generated_at:
                        failures.append(f"canonical-window producer generatedAt differs from manifest: {path}")
                    if execution_record.get("candidateRecentOutputSha256") != sha256(p):
                        failures.append(f"canonical-window producer output SHA differs from candidate: {path}")
                    if execution_record.get("referenceOutputSha256") != sha256(repo / reference_path):
                        failures.append(f"canonical-window producer output SHA differs from reference: {path}")
                    canonical_rel = execution_record.get("canonicalOutputPath")
                    canonical_file = source_root / canonical_rel if canonical_rel else None
                    if (not canonical_file or not canonical_file.resolve().is_relative_to(source_root) or
                            not canonical_file.is_file() or
                            execution_record.get("canonicalRecentOutputSha256") != sha256(canonical_file)):
                        failures.append(f"canonical-window canonical generator output missing or hash mismatch: {path}")
                except Exception as exc:
                    failures.append(f"canonical-window producer execution record is invalid: {exc}")
            if len(new_rows) != 60 or [row.get("id") for row in new_rows] != reference_ids:
                failures.append(f"candidate membership/order differs from exact canonical 60-row window: {path}")
            if actual_additions != set(additions) or actual_removals != set(removal_ids):
                failures.append(f"canonical-window additions/removals differ from exact manifest scope: {path}")
            if target not in reference_by_id or (set(additions) - set(reference_by_id)):
                failures.append(f"canonical-window target/additions are absent from reference: {path}")
            for item_id, row in new_by_id.items():
                if item_id == target or item_id not in old_by_id:
                    if reference_by_id.get(item_id) != row:
                        failures.append(f"canonical-window new/target row differs from reference: {path}/{item_id}")
                elif old_by_id[item_id] != row:
                    failures.append(f"canonical-window retained row changed outside target scope: {path}/{item_id}")
            if not reference_path:
                failures.append(f"canonical-window membership lacks reference path: {path}")
        else:
            exact, reason = exact_feed_removal_result(
                [x for x in old_rows if x.get("id") != target],
                [x for x in new_rows if x.get("id") != target],
                removal_ids,
            )
            if not exact:
                failures.append(f"non-target feed scope mismatch in {path}: {reason}")
        if membership in ("one", "canonical_window"):
            reference_path = manifest.get("newMembershipReferencePath")
            if not reference_path:
                failures.append(f"target membership lacks a declared reference row: {path}")
            else:
                reference_data = read_json(repo / reference_path)
                reference_rows = rows(reference_data)
                reference_target = [x for x in reference_rows if x.get("id") == target]
                if len(reference_target) != 1 or new_target[0] != reference_target[0]:
                    failures.append(f"target feed row must exactly match the declared canonical feed row: {path}")
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

    declared_removals = {
        (path, item_id)
        for path, ids in manifest.get("allowedNonTargetRemovals", {}).items()
        for item_id in ids
    }
    target_removals = set()
    for path in manifest.get("feedPaths", []):
        if manifest.get("feedMembership", {}).get(path) != "none":
            continue
        old_feed = base_json(repo, base, path)
        try:
            old_rows = rows(old_feed) if old_feed is not None else []
        except ValueError:
            old_rows = []
        if sum(row.get("id") == target for row in old_rows):
            target_removals.add((path, target))
    evidence_removals = set()
    as_of = manifest.get("membershipAsOfDate")
    for evidence in manifest.get("membershipRemovalEvidence", []):
        path, item_id = evidence.get("path"), evidence.get("id")
        evidence_removals.add((path, item_id))
        if path not in manifest.get("feedPaths", []):
            failures.append(f"membership removal evidence names a non-feed path: {path}")
            continue
        if item_id == target and (path, item_id) not in target_removals:
            failures.append(f"target membership removal is not declared by a baseline row and membership=none: {item_id}")
        if item_id != target and (path, item_id) not in declared_removals:
            failures.append(f"non-target membership removal is not declared in the manifest: {item_id}")
        if evidence.get("type") == "canonical_recent_window_eviction":
            spec = manifest.get("canonicalWindow", {}).get(path, {})
            reference_path = manifest.get("newMembershipReferencePath")
            reference_file = repo / reference_path if reference_path else None
            reference_data = read_json(reference_file) if reference_file and reference_file.is_file() else None
            try:
                reference_rows = rows(reference_data) if reference_data is not None else []
            except ValueError:
                reference_rows = []
            old_feed = base_json(repo, base, path)
            try:
                old_rows = rows(old_feed) if old_feed is not None else []
            except ValueError:
                old_rows = []
            generator_path = spec.get("generatorPath")
            generator_file = repo / generator_path if generator_path else None
            if path != "ipo_competition_data/recent.json":
                failures.append(f"recent-window eviction evidence used for non-recent feed: {path}/{item_id}")
            if sum(row.get("id") == item_id for row in old_rows) != 1:
                failures.append(f"recent-window eviction must remove one baseline row: {item_id}")
            if any(row.get("id") == item_id for row in reference_rows):
                failures.append(f"recent-window eviction remains in canonical window reference: {item_id}")
            if (not reference_file or not reference_file.is_file() or
                    sha256(reference_file) != evidence.get("referenceSha256") or
                    evidence.get("referencePath") != reference_path or
                    len(reference_rows) != 60 or len({row.get("id") for row in reference_rows}) != 60):
                failures.append(f"recent-window eviction reference hash/count/identity mismatch: {item_id}")
            if (not generator_file or not generator_file.is_file() or
                    sha256(generator_file) != spec.get("generatorSha256") or
                    spec.get("limit") != 60 or evidence.get("limit") != 60 or
                    f"feedItems(recent, 60)" not in generator_file.read_text(encoding="utf-8")):
                failures.append(f"recent-window eviction generator/cap mismatch: {item_id}")
            if evidence.get("asOfDate") != manifest.get("membershipAsOfDate"):
                failures.append(f"recent-window eviction as-of date mismatch: {item_id}")
            continue
        old_feed = base_json(repo, base, path)
        try:
            old_rows = rows(old_feed) if old_feed is not None else []
        except ValueError:
            old_rows = []
        old_matches = [x for x in old_rows if x.get("id") == item_id]
        if len(old_matches) != 1:
            failures.append(f"membership removal base row missing or duplicated: {path}/{item_id}")
            continue
        base_event_doc = base_json(repo, base, manifest["canonicalEventPath"])
        event = locate_event(base_event_doc, item_id) if base_event_doc is not None else None
        removed_stock = base_json(repo, base, f"ipo_competition_data/stocks/{item_id}.json")
        if not event or not removed_stock:
            failures.append(f"membership removal lacks canonical event/detail evidence: {item_id}")
            continue
        row = old_matches[0]
        start, end = event.get("subscriptionStart"), event.get("subscriptionEnd")
        normalize_issuer = lambda value: re.sub(r"[\s㈜()（）.,·]", "", str(value or ""))
        if (normalize_issuer(evidence.get("issuer")) != normalize_issuer(event.get("company"))
                or normalize_issuer(evidence.get("issuer")) != normalize_issuer(removed_stock.get("company"))):
            failures.append(f"membership removal source issuer does not match canonical event/detail: {item_id}")
        expected_start, expected_end = evidence.get("subscriptionStart"), evidence.get("subscriptionEnd")
        if (start, end) != (expected_start, expected_end):
            failures.append(f"membership removal schedule differs from canonical event: {item_id}")
        if (row.get("subscriptionStart"), row.get("subscriptionEnd")) != (start, end):
            failures.append(f"membership removal schedule differs from feed row: {item_id}")
        if (removed_stock.get("subscriptionStart"), removed_stock.get("subscriptionEnd")) != (start, end):
            failures.append(f"membership removal schedule differs from stock detail: {item_id}")
        source_url = event.get("sourceUrl")
        if source_url != evidence.get("sourceUrl") or not source_url.startswith("https://"):
            failures.append(f"membership removal source URL mismatch: {item_id}")
        observed_url = urlparse(evidence.get("observedUrl", ""))
        raw_rel = PurePosixPath(evidence.get("rawFile", ""))
        raw_path = (source_root / Path(*raw_rel.parts)).resolve()
        if observed_url.scheme != "https" or not observed_url.netloc or evidence.get("httpStatus") != 200:
            failures.append(f"membership removal source fetch is not successful HTTPS: {item_id}")
        if ".." in raw_rel.parts or source_root not in raw_path.parents:
            failures.append(f"unsafe membership removal raw path: {item_id}")
        elif not raw_path.is_file() or sha256(raw_path) != evidence.get("rawSha256"):
            failures.append(f"membership removal raw hash mismatch: {item_id}")
        stamps = []
        for stamp in ("requestStartedAtKst", "responseCompletedAtKst"):
            try:
                value = datetime.fromisoformat(evidence[stamp])
                if value.utcoffset() is None:
                    raise ValueError()
                stamps.append(value)
            except (KeyError, TypeError, ValueError):
                failures.append(f"membership removal source timestamp/timezone missing: {item_id}/{stamp}")
        if len(stamps) == 2 and stamps[1] < stamps[0]:
            failures.append(f"membership removal source timestamps are reversed: {item_id}")
        if evidence.get("httpStatus") != 200 or evidence.get("bytes", 0) <= 0:
            failures.append(f"membership removal source response is empty or unsuccessful: {item_id}")
        if evidence.get("asOfDate") != as_of or not as_of:
            failures.append(f"membership removal as-of date mismatch: {item_id}")
        try:
            from datetime import date
            date.fromisoformat(as_of)
            start_date = date.fromisoformat(start)
            end_date = date.fromisoformat(end)
            eligible = end_date < date.fromisoformat(as_of) if path.endswith("active.json") else start_date <= date.fromisoformat(as_of)
            if not eligible:
                failures.append(f"membership removal is not time-eligible for {path}: {item_id}")
        except (TypeError, ValueError):
            failures.append(f"membership removal has invalid date: {item_id}")
        latest = evidence.get("latestFilingSearch")
        if not isinstance(latest, dict):
            failures.append(f"membership removal lacks current DART filing-search evidence: {item_id}")
            continue
        search_url = urlparse(latest.get("url", ""))
        search_rel = PurePosixPath(latest.get("rawFile", ""))
        search_path = (source_root / Path(*search_rel.parts)).resolve()
        if search_url.scheme != "https" or search_url.netloc != "dart.fss.or.kr" or not search_url.path.endswith("/dsab001/search.ax") or latest.get("httpStatus") != 200:
            failures.append(f"current DART filing-search source is not successful official HTTPS: {item_id}")
        search_issuer = (parse_qs(search_url.query).get("textCrpNm") or [None])[0]
        if (normalize_issuer(latest.get("issuer")) != normalize_issuer(event.get("company"))
                or normalize_issuer(search_issuer) != normalize_issuer(event.get("company"))):
            failures.append(f"current DART filing-search issuer/query does not match canonical event: {item_id}")
        search_path_safe = ".." not in search_rel.parts and source_root in search_path.parents
        if not search_path_safe:
            failures.append(f"unsafe current DART filing-search raw path: {item_id}")
        elif not search_path.is_file() or sha256(search_path) != latest.get("rawSha256"):
            failures.append(f"current DART filing-search raw hash mismatch: {item_id}")
        search_stamps = []
        for stamp in ("requestStartedAtKst", "responseCompletedAtKst"):
            try:
                value = datetime.fromisoformat(latest[stamp])
                if value.utcoffset() is None:
                    raise ValueError()
                search_stamps.append(value)
            except (KeyError, TypeError, ValueError):
                failures.append(f"current DART filing-search timestamp/timezone missing: {item_id}/{stamp}")
        if len(search_stamps) == 2 and search_stamps[1] < search_stamps[0]:
            failures.append(f"current DART filing-search timestamps are reversed: {item_id}")
        try:
            newest_date = datetime.strptime(latest.get("latestFilingDate", ""), "%Y.%m.%d").date()
            cutoff_date = datetime.fromisoformat(as_of).date()
            if newest_date > cutoff_date:
                failures.append(f"current DART newest filing date is after membership as-of date: {item_id}")
        except (TypeError, ValueError):
            failures.append(f"current DART newest filing date is invalid: {item_id}")
        if search_path_safe and search_path.is_file() and sha256(search_path) == latest.get("rawSha256"):
            parsed = latest_dart_search_filing(search_path.read_text(encoding="utf-8", errors="replace"))
            declared = {"rcpNo": latest.get("latestRcpNo"), "filingDate": latest.get("latestFilingDate"), "title": latest.get("latestFilingTitle")}
            if parsed != declared:
                failures.append(f"current DART newest filing does not match captured search result: {item_id}")
        schedule_report = evidence.get("latestScheduleReport")
        if not isinstance(schedule_report, dict):
            failures.append(f"membership removal lacks issuer-bound latest schedule report: {item_id}")
            continue
        report_url = urlparse(schedule_report.get("url", ""))
        report_query = parse_qs(report_url.query)
        report_rel = PurePosixPath(schedule_report.get("rawFile", ""))
        report_path = (source_root / Path(*report_rel.parts)).resolve()
        if (report_url.scheme != "https" or report_url.netloc != "dart.fss.or.kr"
                or report_url.path != "/report/viewer.do" or schedule_report.get("httpStatus") != 200):
            failures.append(f"latest schedule report is not a successful official DART report: {item_id}")
        if (schedule_report.get("issuer") is None
                or normalize_issuer(schedule_report.get("issuer")) != normalize_issuer(event.get("company"))
                or normalize_issuer(schedule_report.get("issuer")) != normalize_issuer(removed_stock.get("company"))):
            failures.append(f"latest schedule report issuer does not match canonical event/detail: {item_id}")
        latest_title = str(latest.get("latestFilingTitle", ""))
        normalized_latest_title = re.sub(r"^\[[^]]+\]\s*", "", latest_title).strip()
        eligible_schedule_title = (normalized_latest_title == "증권발행실적보고서"
                                   or normalized_latest_title in ("투자설명서", "증권신고서(지분증권)"))
        if (not eligible_schedule_title
                or schedule_report.get("reportTitle") != latest_title
                or schedule_report.get("rcpNo") != latest.get("latestRcpNo")
                or (report_query.get("rcpNo") or [None])[0] != latest.get("latestRcpNo")):
            failures.append(f"latest schedule report is not the exact newest issuer filing: {item_id}")
        if ".." in report_rel.parts or source_root not in report_path.parents:
            failures.append(f"unsafe latest schedule report raw path: {item_id}")
        elif not report_path.is_file() or sha256(report_path) != schedule_report.get("rawSha256"):
            failures.append(f"latest schedule report raw hash mismatch: {item_id}")
        report_stamps = []
        for stamp in ("requestStartedAtKst", "responseCompletedAtKst"):
            try:
                value = datetime.fromisoformat(schedule_report[stamp])
                if value.utcoffset() is None:
                    raise ValueError()
                report_stamps.append(value)
            except (KeyError, TypeError, ValueError):
                failures.append(f"latest schedule report timestamp/timezone missing: {item_id}/{stamp}")
        if len(report_stamps) == 2 and report_stamps[1] < report_stamps[0]:
            failures.append(f"latest schedule report timestamps are reversed: {item_id}")
        if report_path.is_file() and sha256(report_path) == schedule_report.get("rawSha256"):
            observed_schedule = dart_subscription_dates(report_path.read_text(encoding="utf-8", errors="replace"))
            expected_schedule = (start, end)
            declared_schedule = (schedule_report.get("subscriptionStart"), schedule_report.get("subscriptionEnd"))
            if observed_schedule != expected_schedule or declared_schedule != expected_schedule:
                failures.append(f"latest issuer report schedule does not match canonical event/feed dates: {item_id}")
    if declared_removals | target_removals != evidence_removals:
        failures.append("declared target/non-target membership removals and evidence records differ")

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
        if target in message and (code, message) not in base_phase:
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
    baseline_target_signatures = {sig for sig in base_comp if sig[1] == target}
    for issue in candidate_comp_report.get("issues", []):
        if issue.get("severity") == "ERROR" and issue.get("eventId") == target:
            signature = set_issue_signature(issue)
            if signature in baseline_target_signatures:
                continue
            if permitted_null_issue(issue.get("eventId"), target, issue.get("code"), issue.get("field"), {"missing_review"}, allowed_null, stock):
                continue
            failures.append(f"new hard target-scope audit error: {issue.get('code')}/{target}/{issue.get('field','')}")

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
            "carriedForwardUnchangedTarget": sorted(set_issue_signature(x) for x in candidate_comp_report.get("issues", []) if x.get("severity") == "ERROR" and x.get("eventId") == target and set_issue_signature(x) in base_comp),
            "carriedForwardUnchangedOutOfScope": sorted(set_issue_signature(x) for x in candidate_comp_report.get("issues", []) if x.get("severity") == "ERROR" and x.get("eventId") != target and set_issue_signature(x) in {set_issue_signature(y) for y in baseline_comp_report.get("issues", []) if y.get("severity") == "ERROR"}),
            "interpretation": "full audits are reported, not claimed green; only exact target fields and memberships named by the manifest are in scope; unchanged audit issues remain open, while new errors, changed-field integrity errors, or feed mismatches block"
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
