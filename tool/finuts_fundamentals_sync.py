#!/usr/bin/env python3
from __future__ import annotations

import argparse
import html
import json
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]
MANUAL_FUNDAMENTALS_PATH = ROOT / "data" / "manual_fundamentals.json"
OUTCOMES_DIR = ROOT / "data" / "outcomes"
IDENTIFIERS_PATH = ROOT / "data" / "identifiers" / "ipo_identifiers.json"
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


def to_float(value: str | None) -> float | None:
    if value is None:
        return None
    text = value.replace(",", "").strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


@dataclass
class FinutsEvent:
    ipo_sn: str
    company: str
    key: str
    subscription_start: str | None
    subscription_end: str | None
    demand_forecast_date: str | None
    listing_date: str | None
    price_min: int | None
    price_max: int | None
    offer_price: int | None

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
        events.append(
            FinutsEvent(
                ipo_sn=str(primary.get("IPO_SN", "")).strip(),
                company=company,
                key=normalize_company_key(company),
                subscription_start=parse_date((by_code.get("S") or {}).get("BGNG_YMD")),
                subscription_end=parse_date((by_code.get("S") or {}).get("END_YMD"))
                or parse_date((by_code.get("S") or {}).get("BGNG_YMD")),
                demand_forecast_date=parse_date((by_code.get("D") or {}).get("BGNG_YMD")),
                listing_date=parse_date((by_code.get("L") or {}).get("BGNG_YMD"))
                or parse_date(primary.get("IPO_DATE")),
                price_min=to_int(primary.get("BAND_BGNG_AMT")),
                price_max=to_int(primary.get("BAND_END_AMT")),
                offer_price=to_int(primary.get("PSS_PRC")),
            )
        )
    return events


def fetch_text(url: str) -> str:
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(req, timeout=30) as response:
        return response.read().decode("utf-8", "ignore")


def plain_text(raw: str) -> str:
    text = html.unescape(raw)
    text = re.sub(r"(?is)<script.*?</script>", " ", text)
    text = re.sub(r"(?is)<style.*?</style>", " ", text)
    text = re.sub(r"(?i)<br\s*/?>", " ", text)
    text = re.sub(r"(?s)<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def parse_labeled_number(text: str, labels: list[str]) -> str | None:
    for label in labels:
        pattern = re.compile(
            rf"{re.escape(label)}[^0-9]{{0,40}}([0-9][0-9,]*(?:\.[0-9]+)?)",
            re.IGNORECASE,
        )
        match = pattern.search(text)
        if match:
            return match.group(1)
    return None


def parse_labeled_int(text: str, labels: list[str]) -> int | None:
    return to_int(parse_labeled_number(text, labels))


def parse_labeled_rate(text: str, labels: list[str]) -> float | None:
    return to_float(parse_labeled_number(text, labels))


def parse_labeled_percent(text: str, labels: list[str]) -> float | None:
    value = parse_labeled_rate(text, labels)
    if value is None:
        return None
    return value / 100 if value > 1 else value


def parse_detail_fundamentals(text: str) -> dict[str, Any]:
    normalized = plain_text(text)
    offer_price = parse_labeled_int(normalized, ["확정공모가", "확정 공모가"])
    institution_competition = parse_labeled_rate(normalized, ["기관 경쟁률", "기관경쟁률"])
    institution_participants = parse_labeled_int(
        normalized,
        ["참여건수", "참여 건수", "참여기관", "참여 기관"],
    )
    lockup_rate = parse_labeled_percent(
        normalized,
        ["의무보유확약 비율", "의무보유확약률", "의무보유 확약"],
    )
    public_allocation = parse_labeled_int(
        normalized,
        ["일반청약자 배정", "일반 투자자 배정", "일반청약 배정"],
    )
    market_cap = parse_labeled_int(normalized, ["예상 시가총액", "시가총액"])

    return {
        "offerPrice": offer_price,
        "institutionCompetitionRate": institution_competition,
        "institutionParticipants": institution_participants,
        "lockupCommitmentRate": lockup_rate,
        "publicAllocationShares": public_allocation,
        "marketCapKrw": market_cap,
    }


def load_manual_rows() -> list[dict[str, Any]]:
    if not MANUAL_FUNDAMENTALS_PATH.exists():
        return []
    payload = json.loads(MANUAL_FUNDAMENTALS_PATH.read_text(encoding="utf-8"))
    if isinstance(payload, dict) and isinstance(payload.get("stocks"), list):
        return [row for row in payload["stocks"] if isinstance(row, dict)]
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    return []


def write_manual_rows(rows: list[dict[str, Any]]) -> None:
    MANUAL_FUNDAMENTALS_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {"stocks": rows}
    MANUAL_FUNDAMENTALS_PATH.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def load_identifier_aliases() -> dict[str, str]:
    if not IDENTIFIERS_PATH.exists():
        return {}
    payload = json.loads(IDENTIFIERS_PATH.read_text(encoding="utf-8"))
    rows = []
    if isinstance(payload, dict):
        if isinstance(payload.get("identifiers"), list):
            rows = payload["identifiers"]
        elif isinstance(payload.get("stocks"), list):
            rows = payload["stocks"]
    elif isinstance(payload, list):
        rows = payload
    aliases: dict[str, str] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        row_id = str(row.get("id") or "").strip()
        company = str(row.get("company") or "").strip()
        identifiers = row.get("identifiers") if isinstance(row.get("identifiers"), dict) else {}
        subscription_key = str(identifiers.get("subscriptionKey") or "").strip()
        normalized_company = str(identifiers.get("normalizedCompany") or "").strip()
        for key in (row_id, subscription_key, normalized_company, company):
            key = key.strip()
            if key:
                aliases[key] = company
                aliases[safe_id(key)] = company
    return aliases


def load_outcome_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict) and isinstance(payload.get("outcomes"), list):
        return [row for row in payload["outcomes"] if isinstance(row, dict)]
    return []


def update_outcome(event: FinutsEvent) -> None:
    if not event.listing_date and not event.offer_price:
        return
    year = int((event.listing_date or event.subscription_start or str(datetime.now().year))[:4])
    OUTCOMES_DIR.mkdir(parents=True, exist_ok=True)
    path = OUTCOMES_DIR / f"{year}.json"
    rows = {
        safe_id(str(row.get("id") or row.get("company") or "")): row
        for row in load_outcome_rows(path)
    }
    rows[event.stock_id] = {
        "id": event.stock_id,
        "company": event.company,
        "listingDate": event.listing_date,
        "offerPrice": event.offer_price,
        "openPrice": None,
        "highPrice": None,
        "closePrice": None,
        "sourceUrl": event.finuts_url,
    }
    payload = {
        "schemaVersion": 1,
        "outcomes": sorted(
            rows.values(),
            key=lambda item: ((item.get("listingDate") or ""), (item.get("company") or "")),
            reverse=True,
        ),
    }
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Sync manual fundamentals overrides from Finuts detail pages.",
    )
    parser.add_argument(
        "--mode",
        choices=["demand-today", "all", "target"],
        default="demand-today",
    )
    parser.add_argument("--stock-id", default="")
    parser.add_argument("--company", default="")
    args = parser.parse_args()

    today = datetime.now().date().isoformat()
    events = fetch_finuts_events()
    identifier_aliases = load_identifier_aliases()

    if args.mode == "target":
        stock_id = args.stock_id.strip()
        company_key = normalize_company_key(args.company) if args.company.strip() else ""
        stock_id_company = ""
        if stock_id:
            stock_id_company = identifier_aliases.get(stock_id) or identifier_aliases.get(
                safe_id(stock_id)
            ) or ""
        stock_id_company_key = (
            normalize_company_key(stock_id_company) if stock_id_company else ""
        )
        targets = [
            event
            for event in events
            if (stock_id and event.stock_id == stock_id)
            or (stock_id and safe_id(event.stock_id) == safe_id(stock_id))
            or (
                stock_id
                and safe_id(f"{event.company}_{(event.subscription_start or '')[:4]}")
                == safe_id(stock_id)
            )
            or (stock_id_company_key and event.key == stock_id_company_key)
            or (company_key and event.key == company_key)
        ]
    elif args.mode == "all":
        targets = events
    else:
        targets = [event for event in events if event.demand_forecast_date == today]

    if not targets:
        print("No Finuts fundamentals targets matched.")
        return 0

    merged = {
        safe_id(str(row.get("id") or row.get("company") or "")): row
        for row in load_manual_rows()
    }

    updated = 0
    for event in targets:
        detail = fetch_text(event.finuts_url)
        fundamentals = parse_detail_fundamentals(detail)
        non_null = {key: value for key, value in fundamentals.items() if value is not None}
        if event.price_min is not None:
            non_null.setdefault("priceBandMin", event.price_min)
        if event.price_max is not None:
            non_null.setdefault("priceBandMax", event.price_max)
        if event.offer_price is not None:
            non_null.setdefault("offerPrice", event.offer_price)
        if not non_null:
            continue
        manual_row_id = args.stock_id.strip() if args.mode == "target" and args.stock_id.strip() else event.stock_id
        merged[safe_id(manual_row_id)] = {
            "id": manual_row_id,
            "company": event.company,
            "fundamentals": non_null,
        }
        update_outcome(event)
        updated += 1

    rows = sorted(
        merged.values(),
        key=lambda row: (str(row.get("id") or ""), str(row.get("company") or "")),
    )
    write_manual_rows(rows)
    print(f"updated {updated} Finuts fundamentals overrides")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
