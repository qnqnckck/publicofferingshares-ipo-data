#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def normalize_broker_alias(value: str) -> str:
    compact = re.sub(r"\s+", "", value)
    for suffix in ["투자증권", "증권", "증권㈜", "(주)", "주식회사", "-"]:
        compact = compact.replace(suffix, "")
    return compact


def build_source_entry(
    stock_id: str,
    company: str,
    lead_managers: list[str],
    finuts_url: str,
    deposit_manwon: float,
    fundamentals: dict[str, Any],
) -> dict[str, Any]:
    brokers = []
    for manager in lead_managers:
        name = str(manager).strip()
        if not name:
            continue
        aliases = sorted({name, normalize_broker_alias(name)} - {""})
        brokers.append(
            {
                "name": name,
                "aliases": aliases,
                "offeredShares": None,
                "equalAllocationShares": None,
                "proportionalAllocationShares": None,
                "depositRate": 0.5,
                "feeKrw": 2000,
                "ocrHints": {
                    "competitionPatterns": [
                        r"청약\s*경쟁률\s*(\d+(?:\.\d+)?)",
                        r"경쟁률\s*(\d+(?:\.\d+)?)",
                    ],
                    "proportionalCompetitionPatterns": [
                        r"비례\s*경쟁률\s*(\d+(?:\.\d+)?)",
                        r"비례\s*(\d+(?:\.\d+)?)\s*대\s*1",
                    ],
                    "applicationCountPatterns": [
                        r"청약건수\s*(\d+(?:,\d{3})*)",
                        r"건수\s*(\d+(?:,\d{3})*)",
                    ],
                },
            }
        )

    return {
        "id": stock_id,
        "company": company,
        "capturedAtKst": datetime.now().replace(microsecond=0).isoformat(),
        "source": "finuts_member_secondary",
        "sourceLabel": "manual_finuts_backfill",
        "sourceUrl": finuts_url,
        "finutsUrl": finuts_url,
        "finutsSearchDepositManwon": deposit_manwon,
        "offerPrice": fundamentals.get("offerPrice"),
        "brokers": brokers,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--stock-id", required=True)
    parser.add_argument("--seed-path", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--finuts-url", default="")
    parser.add_argument("--deposit-manwon", default="100")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    stock_id = args.stock_id.strip()
    seed_payload = read_json(Path(args.seed_path))
    stocks = seed_payload.get("stocks", [])

    stock = None
    for item in stocks:
        if not isinstance(item, dict):
            continue
        if str(item.get("id", "")).strip() == stock_id:
            stock = item
            break
    if stock is None:
        print(f"Cannot find stock_id={stock_id} in seed path: {args.seed_path}")
        return 1

    company = str(stock.get("company", "")).strip()
    if not company:
        print(f"Stock {stock_id} has no company name")
        return 1

    lead_managers = stock.get("leadManagers", [])
    if not isinstance(lead_managers, list) or not lead_managers:
        print(f"Stock {stock_id} has no leadManagers in seed")
        return 1

    finuts_url = str(args.finuts_url).strip()
    deposit_manwon = float(str(args.deposit_manwon).strip() or "100")
    fundamentals = stock.get("fundamentals", {})
    if not isinstance(fundamentals, dict):
        fundamentals = {}

    config: dict[str, Any] = {
        "schemaVersion": 1,
        "sources": [],
        "catalog": [],
    }

    if finuts_url:
        config["sources"].append(
            build_source_entry(
                stock_id=stock_id,
                company=company,
                lead_managers=lead_managers,
                finuts_url=finuts_url,
                deposit_manwon=deposit_manwon,
                fundamentals=fundamentals,
            )
        )
    else:
        config["scheduleAutoload"] = {
            "enabled": True,
            "seedPath": "data/ipo_competition_seed.json",
            "daysBeforeStart": 365,
            "daysAfterEnd": 30,
            "todayOnly": False,
            "marketOpenHourKst": 8,
            "marketCloseHourKst": 19,
            "finutsAutodiscover": True,
        }

    output_path = Path(args.output)
    output_path.write_text(
        json.dumps(config, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(f"wrote {output_path}")
    print(f"mode={'manual' if finuts_url else 'autodiscover'}")
    print(f"stock_id={stock_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
