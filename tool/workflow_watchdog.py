#!/usr/bin/env python3
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.error import HTTPError
from urllib.request import Request, urlopen


@dataclass
class WatchTarget:
    workflow: str
    max_age_minutes: int
    branch: str = "main"


def _github_request(
    method: str,
    url: str,
    token: str,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any] | list[Any] | None:
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "publicofferingshares-ipo-watchdog",
    }
    data = None
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = Request(url, data=data, headers=headers, method=method)
    with urlopen(req, timeout=30) as response:
        body = response.read().decode("utf-8", errors="ignore").strip()
    if not body:
        return None
    return json.loads(body)


def _parse_github_dt(value: str | None) -> datetime | None:
    text = (value or "").strip()
    if not text:
        return None
    with_timezone = text.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(with_timezone).astimezone(UTC)
    except ValueError:
        return None


def _dispatch_workflow(
    owner_repo: str,
    token: str,
    target: WatchTarget,
) -> None:
    url = (
        f"https://api.github.com/repos/{owner_repo}/actions/workflows/"
        f"{target.workflow}/dispatches"
    )
    _github_request("POST", url, token, {"ref": target.branch})
    print(f"DISPATCHED: {target.workflow} on {target.branch}")


def _latest_runs(
    owner_repo: str,
    token: str,
    target: WatchTarget,
) -> list[dict[str, Any]]:
    url = (
        f"https://api.github.com/repos/{owner_repo}/actions/workflows/"
        f"{target.workflow}/runs?branch={target.branch}&per_page=10"
    )
    payload = _github_request("GET", url, token)
    if not isinstance(payload, dict):
        return []
    runs = payload.get("workflow_runs", [])
    return [run for run in runs if isinstance(run, dict)]


def _ensure_recent_run(
    owner_repo: str,
    token: str,
    target: WatchTarget,
    now: datetime,
) -> None:
    runs = _latest_runs(owner_repo, token, target)
    active_run = next(
        (
            run
            for run in runs
            if str(run.get("status", "")).lower() in {"queued", "in_progress"}
        ),
        None,
    )
    if active_run is not None:
        run_id = active_run.get("id")
        status = active_run.get("status")
        print(f"ACTIVE: {target.workflow} run={run_id} status={status}")
        return

    latest_run = runs[0] if runs else None
    latest_started_at = _parse_github_dt(
        None if latest_run is None else latest_run.get("run_started_at"),
    ) or _parse_github_dt(
        None if latest_run is None else latest_run.get("created_at"),
    )
    if latest_started_at is None:
        print(f"MISSING: {target.workflow} has no recent runs; dispatching")
        _dispatch_workflow(owner_repo, token, target)
        return

    age = now - latest_started_at
    threshold = timedelta(minutes=target.max_age_minutes)
    if age > threshold:
        print(
            f"STALE: {target.workflow} age={int(age.total_seconds() // 60)}m "
            f"threshold={target.max_age_minutes}m; dispatching",
        )
        _dispatch_workflow(owner_repo, token, target)
        return

    conclusion = latest_run.get("conclusion")
    run_id = latest_run.get("id")
    print(
        f"OK: {target.workflow} run={run_id} conclusion={conclusion} "
        f"age={int(age.total_seconds() // 60)}m",
    )


def main() -> int:
    owner_repo = os.environ.get("GITHUB_REPOSITORY", "").strip()
    token = os.environ.get("GITHUB_TOKEN", "").strip()
    if not owner_repo or not token:
        raise SystemExit("GITHUB_REPOSITORY and GITHUB_TOKEN are required.")

    now = datetime.now(UTC)
    targets = [
        WatchTarget(
            workflow="video_ocr_secondary_ingest.yml",
            max_age_minutes=int(os.environ.get("WATCHDOG_OCR_MAX_AGE_MINUTES", "35")),
        ),
        WatchTarget(
            workflow="ipo_competition_batch.yml",
            max_age_minutes=int(os.environ.get("WATCHDOG_BATCH_MAX_AGE_MINUTES", "1560")),
        ),
    ]

    for target in targets:
        try:
            _ensure_recent_run(owner_repo, token, target, now)
        except HTTPError as exc:
            print(f"ERROR: {target.workflow} http={exc.code} {exc.reason}")
            return 1
        except Exception as exc:  # pragma: no cover
            print(f"ERROR: {target.workflow} {exc}")
            return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
