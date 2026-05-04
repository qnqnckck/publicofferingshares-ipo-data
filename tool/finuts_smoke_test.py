#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

from video_ocr_secondary_ingest import SecondaryVideoOcrIngest


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Smoke test Finuts broker AJAX/session access for one IPO page.",
    )
    parser.add_argument(
        "--finuts-url",
        required=True,
        help="Full Finuts IPO detail URL, e.g. https://www.finuts.co.kr/html/ipo/ipoView.php?ipo_sn=100535",
    )
    parser.add_argument(
        "--deposit-manwon",
        type=int,
        default=100,
        help="search_scscs_wrtm value in 만원 units. Default: 100",
    )
    return parser


def _summarize_rows(rows: list[dict[str, Any]], *, limit: int = 10) -> list[dict[str, Any]]:
    summary: list[dict[str, Any]] = []
    for row in rows[:limit]:
        summary.append(
            {
                "broker": row.get("SCRT_CO_NM"),
                "competitionRate": row.get("SCSCS_CMPET_RT"),
                "equalExpectedShares": row.get("EQLTY_STOCK_CNT"),
                "allocatedShares": row.get("ALTMNT_CNT"),
                "proportionalUnits": row.get("PROP_CMPET_ALTMNT"),
                "fee": row.get("FEE"),
            }
        )
    return summary


def main() -> int:
    args = build_parser().parse_args()

    finuts_id = os.environ.get("FINUTS_ID", "").strip()
    finuts_password = os.environ.get("FINUTS_PASSWORD", "").strip()
    if not finuts_id or not finuts_password:
        raise SystemExit("FINUTS_ID and FINUTS_PASSWORD are required.")

    ingest = SecondaryVideoOcrIngest(
        config_path=Path("data/video_ocr_sources.json"),
        broker_snapshot_dir=Path("data/broker_snapshots"),
        dry_run=True,
    )

    method = "session"
    try:
        jugansa_rows, altmnt_rows = ingest._fetch_finuts_ajax_rows_via_session(
            args.finuts_url,
            search_deposit_manwon=args.deposit_manwon,
        )
    except Exception as exc:
        print(f"SESSION_ERROR: {exc}")
        jugansa_rows, altmnt_rows = [], []

    if not jugansa_rows:
        method = "playwright_fallback"
        jugansa_rows, altmnt_rows = ingest._fetch_finuts_ajax_rows(
            args.finuts_url,
            search_deposit_manwon=args.deposit_manwon,
        )

    result = {
        "finutsUrl": args.finuts_url,
        "method": method,
        "jugansaCount": len(jugansa_rows),
        "altmntCount": len(altmnt_rows),
        "jugansaPreview": _summarize_rows(jugansa_rows),
        "altmntPreview": _summarize_rows(altmnt_rows),
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))

    if not jugansa_rows:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
