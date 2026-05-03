#!/usr/bin/env python3
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "ipo_competition_data"
FINUTS_URL = "https://www.finuts.co.kr/html/task/ipo/ipoListQuery.php"


def normalize_company_key(company: str) -> str:
    compact = (
        company.lower()
        .strip()
        .replace("(주)", "")
        .replace("㈜", "")
        .replace("주식회사", "")
        .replace(" ", "")
    )
    return "".join(ch for ch in compact if ch.isalnum() or ("가" <= ch <= "힣"))


def parse_date(value: Any) -> date | None:
    text = str(value or "").strip()
    if not text or text == "9999-99-99":
        return None
    try:
        return datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError:
        return None


@dataclass
class FinutsEvent:
    company: str
    key: str
    security_type: str
    subscription_start: date | None
    subscription_end: date | None
    listing_date: date | None
    price_min: int | None
    price_max: int | None
    offer_price: int | None


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
        primary = by_code.get("S") or by_code.get("L") or by_code.get("D") or rows[0]
        company = str(primary.get("ENT_NM", "")).strip()
        subscription_start = parse_date((by_code.get("S") or {}).get("BGNG_YMD"))
        subscription_end = parse_date((by_code.get("S") or {}).get("END_YMD"))
        listing_date = parse_date((by_code.get("L") or {}).get("BGNG_YMD")) or parse_date(
            primary.get("IPO_DATE")
        )
        if subscription_start is None and subscription_end is None and listing_date is None:
            continue
        price_min = int(str(primary.get("BAND_BGNG_AMT") or "0") or "0")
        price_max = int(str(primary.get("BAND_END_AMT") or "0") or "0")
        offer_price = int(str(primary.get("PSS_PRC") or "0") or "0")
        events.append(
            FinutsEvent(
                company=company,
                key=normalize_company_key(company),
                security_type=str(primary.get("SE_CD", "")).strip().upper(),
                subscription_start=subscription_start,
                subscription_end=subscription_end,
                listing_date=listing_date,
                price_min=price_min or None,
                price_max=price_max or None,
                offer_price=offer_price or None,
            )
        )
    return events


def load_feed_items() -> dict[str, dict[str, Any]]:
    items: dict[str, dict[str, Any]] = {}
    for name in ["active.json", "upcoming.json", "index.json"]:
        payload = json.loads((DATA_DIR / name).read_text())
        for item in payload.get("stocks", []):
            if not isinstance(item, dict):
                continue
            company = str(item.get("company", "")).strip()
            if not company:
                continue
            items.setdefault(normalize_company_key(company), item)
    return items


def main() -> int:
    today = date.today()
    horizon = today + timedelta(days=21)
    finuts_events = fetch_finuts_events()
    feed_items = load_feed_items()

    missing: list[str] = []
    mismatched: list[str] = []
    for event in finuts_events:
        if event.security_type not in {"IPO", "SPAC"}:
            continue
        anchor = event.subscription_start or event.listing_date
        if anchor is None or anchor < today or anchor > horizon:
            continue
        feed = feed_items.get(event.key)
        if feed is None:
            missing.append(
                f"{event.company} missing from public feeds ({event.subscription_start}~{event.subscription_end}, listing={event.listing_date})"
            )
            continue
        if str(feed.get("subscriptionStart") or "") != str(event.subscription_start or ""):
            mismatched.append(
                f"{event.company} subscriptionStart mismatch finuts={event.subscription_start} feed={feed.get('subscriptionStart')}"
            )
        if str(feed.get("subscriptionEnd") or "") != str(event.subscription_end or ""):
            mismatched.append(
                f"{event.company} subscriptionEnd mismatch finuts={event.subscription_end} feed={feed.get('subscriptionEnd')}"
            )

    if not missing and not mismatched:
        print("OK: future Finuts schedule is reflected in public feeds.")
        return 0

    for line in missing:
        print(f"MISSING: {line}")
    for line in mismatched:
        print(f"MISMATCH: {line}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
