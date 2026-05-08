#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]
DISCOVERED_PATH = ROOT / "data" / "discovered" / "ipo_events.json"
OUTCOMES_DIR = ROOT / "data" / "outcomes"
FINUTS_URL = "https://www.finuts.co.kr/html/task/ipo/ipoListQuery.php"


def safe_id(value: str) -> str:
    normalized = value.strip().lower()
    normalized = re.sub(r"[^0-9a-z가-힣_-]+", "_", normalized)
    normalized = re.sub(r"_+", "_", normalized)
    return normalized.strip("_")


def normalize_company_key(company: str) -> str:
    compact = (
        company.lower()
        .strip()
        .replace("(주)", "")
        .replace("주식회사", "")
        .replace(" ", "")
    )
    return "".join(ch for ch in compact if ch.isalnum() or ("가" <= ch <= "힣"))


def parse_date(value: Any) -> str | None:
    text = str(value or "").strip()
    if not text or text == "9999-99-99":
        return None
    digits = re.sub(r"[^0-9]", "", text)
    if len(digits) < 8:
        return None
    return f"{digits[:4]}-{digits[4:6]}-{digits[6:8]}"


def to_int(value: Any) -> int | None:
    if value is None:
        return None
    text = str(value).replace(",", "").strip()
    if not text:
        return None
    try:
        return int(text)
    except ValueError:
        return None


def split_managers(value: str | None) -> list[str]:
    if not value:
        return []
    return [
        item.strip()
        for item in re.split(r"[,/·|;]", value)
        if item and item.strip()
    ]


@dataclass
class FinutsEvent:
    ipo_sn: str
    company: str
    key: str
    security_type: str
    subscription_start: str | None
    subscription_end: str | None
    demand_forecast_date: str | None
    listing_date: str | None
    price_min: int | None
    price_max: int | None
    offer_price: int | None
    lead_managers: list[str]

    @property
    def finuts_url(self) -> str:
        return f"https://www.finuts.co.kr/html/ipo/ipoView.php?ipo_sn={self.ipo_sn}"

    @property
    def stock_id(self) -> str:
        return safe_id(f"{self.company}_{self.subscription_start or ''}")


def fetch_finuts_events() -> list[FinutsEvent]:
    req = Request(
        FINUTS_URL,
        data=urlencode({"active": "ipo-011", "search_text": ""}).encode(),
        headers={
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "X-Requested-With": "XMLHttpRequest",
            "User-Agent": "Mozilla/5.0",
        },
    )
    with urlopen(req, timeout=30) as response:
        payload = json.loads(response.read().decode("utf-8", "ignore"))

    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in payload.get("data", []):
        if not isinstance(row, dict):
            continue
        ipo_sn = str(row.get("IPO_SN", "")).strip()
        company = str(row.get("ENT_NM", "")).strip()
        if not ipo_sn or not company:
            continue
        grouped.setdefault(ipo_sn, []).append(row)

    events: list[FinutsEvent] = []
    for rows in grouped.values():
        by_code = {
            str(row.get("SCHDL_SE_CD", "")).strip().upper(): row for row in rows
        }
        primary = by_code.get("S") or by_code.get("D") or by_code.get("L") or rows[0]
        company = str(primary.get("ENT_NM", "")).strip()
        if not company:
            continue
        subscription_start = parse_date((by_code.get("S") or {}).get("BGNG_YMD"))
        subscription_end = parse_date((by_code.get("S") or {}).get("END_YMD")) or subscription_start
        demand_forecast_date = parse_date((by_code.get("D") or {}).get("BGNG_YMD"))
        listing_date = parse_date((by_code.get("L") or {}).get("BGNG_YMD")) or parse_date(
            primary.get("IPO_DATE")
        )
        lead_managers = split_managers(
            str(
                primary.get("MNGR_NM")
                or primary.get("LEAD_MNGR_NM")
                or primary.get("CMN_MNGR_NM")
                or ""
            ).strip()
        )
        events.append(
            FinutsEvent(
                ipo_sn=str(primary.get("IPO_SN", "")).strip(),
                company=company,
                key=normalize_company_key(company),
                security_type=str(primary.get("SE_CD", "")).strip().upper(),
                subscription_start=subscription_start,
                subscription_end=subscription_end,
                demand_forecast_date=demand_forecast_date,
                listing_date=listing_date,
                price_min=to_int(primary.get("BAND_BGNG_AMT")),
                price_max=to_int(primary.get("BAND_END_AMT")),
                offer_price=to_int(primary.get("PSS_PRC")),
                lead_managers=lead_managers,
            )
        )
    return events


def build_stock(event: FinutsEvent) -> dict[str, Any]:
    market = "KOSDAQ"
    if "KOSPI" in event.security_type:
        market = "KOSPI"
    return {
        "id": event.stock_id,
        "company": event.company,
        "market": market,
        "industry": "",
        "subscriptionStart": event.subscription_start,
        "subscriptionEnd": event.subscription_end,
        "leadManagers": event.lead_managers,
        "identifiers": {
            "subscriptionKey": f"{event.company}_{(event.subscription_start or '').replace('-', '')}_{(event.subscription_end or event.subscription_start or '').replace('-', '')}",
            "normalizedCompany": event.key,
            "corpCode": None,
            "stockCode": None,
            "kindCode": None,
            "isin": None,
        },
        "fundamentals": {
            "offerPrice": event.offer_price,
            "priceBandMin": event.price_min,
            "priceBandMax": event.price_max,
            "topBandConfirmation": None,
            "institutionCompetitionRate": None,
            "institutionParticipants": None,
            "lockupCommitmentRate": None,
            "floatRate": None,
            "marketCapKrw": None,
            "publicAllocationShares": None,
            "hasPutbackRight": False,
            "putbackSummary": None,
        },
        "outcome": None,
        "snapshots": [],
    }


def load_wrapped_rows(path: Path, key: str) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict) and isinstance(payload.get(key), list):
        return [row for row in payload[key] if isinstance(row, dict)]
    return []


def write_outcomes(events: list[FinutsEvent], *, backfill_years: int) -> None:
    cutoff_year = datetime.now().year - backfill_years
    by_year: dict[int, dict[str, dict[str, Any]]] = {}

    for file in OUTCOMES_DIR.glob("*.json"):
        try:
            year = int(file.stem)
        except ValueError:
            continue
        rows = load_wrapped_rows(file, "outcomes")
        by_year[year] = {
            safe_id(str(row.get("id") or row.get("company") or "")): row for row in rows
        }

    for event in events:
        year_value = None
        if event.listing_date:
            year_value = int(event.listing_date[:4])
        elif event.subscription_start:
            year_value = int(event.subscription_start[:4])
        if year_value is None or year_value < cutoff_year:
            continue
        row = {
            "id": event.stock_id,
            "company": event.company,
            "listingDate": event.listing_date,
            "offerPrice": event.offer_price,
            "openPrice": None,
            "highPrice": None,
            "closePrice": None,
            "sourceUrl": event.finuts_url,
        }
        by_year.setdefault(year_value, {})[event.stock_id] = row

    OUTCOMES_DIR.mkdir(parents=True, exist_ok=True)
    for year, rows_by_id in by_year.items():
        target = OUTCOMES_DIR / f"{year}.json"
        payload = {
            "schemaVersion": 1,
            "outcomes": sorted(
                rows_by_id.values(),
                key=lambda item: (
                    item.get("listingDate") or "",
                    item.get("company") or "",
                ),
                reverse=True,
            ),
        }
        target.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Sync discovered IPO baseline rows from Finuts only.",
    )
    parser.add_argument("--backfill-years", type=int, default=3)
    args = parser.parse_args()

    events = fetch_finuts_events()
    stocks = [build_stock(event) for event in events if event.subscription_start]
    payload = {
        "schemaVersion": 1,
        "generatedAt": datetime.now().isoformat(),
        "stocks": sorted(
            stocks,
            key=lambda item: (item.get("subscriptionStart") or "", item.get("company") or ""),
            reverse=True,
        ),
    }
    DISCOVERED_PATH.parent.mkdir(parents=True, exist_ok=True)
    DISCOVERED_PATH.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    write_outcomes(events, backfill_years=args.backfill_years)
    print(f"updated {DISCOVERED_PATH} with {len(stocks)} stock rows from Finuts")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
