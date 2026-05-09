#!/usr/bin/env python3
from __future__ import annotations

import argparse
import html
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timedelta
from contextlib import suppress
from http.cookiejar import CookieJar
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.request import HTTPCookieProcessor, Request, build_opener, urlopen
from urllib.parse import parse_qsl, quote, quote_plus, urlencode, urlparse, urlunparse
from zoneinfo import ZoneInfo


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def _is_valid_company_name(value: str | None) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    lowered = text.lower()
    if "://" in lowered or lowered.startswith("file:"):
        return False
    if lowered.startswith("/mnt/") or lowered.startswith("\\\\") or re.match(r"^[a-z]:[/\\\\]", lowered):
        return False
    if "/" in text or "\\" in text:
        return False
    if re.search(r"\.(jpg|jpeg|png|webp|gif|bmp|heic|svg|mp4|webm)$", lowered):
        return False
    if "harness/static" in lowered:
        return False
    return True


def _to_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(str(value).replace(",", "").strip())
    except ValueError:
        return None


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(str(value).replace(",", "").strip())
    except ValueError:
        return None


def _require_bin(name: str) -> str:
    resolved = shutil.which(name)
    if not resolved:
        raise RuntimeError(f"Required binary not found: {name}")
    return resolved


def _parse_number(text: str, patterns: list[str]) -> float | None:
    compact = text.replace(",", "")
    for pattern in patterns:
        match = re.search(pattern, compact, flags=re.IGNORECASE | re.MULTILINE)
        if not match:
            continue
        raw = next((group for group in match.groups() if group), None)
        if raw is None:
            continue
        try:
            return float(raw)
        except ValueError:
            continue
    return None


@dataclass
class BrokerExtraction:
    name: str
    offered_shares: int | None
    equal_allocation_shares: int | None
    proportional_allocation_shares: int | None
    expected_equal_shares: float | None
    deposit_rate: float | None
    fee_krw: int | None
    competition_rate: float | None
    proportional_competition_rate: float | None
    application_count: int | None

    def to_json(self) -> dict[str, Any]:
        subscribed_shares = None
        if self.offered_shares and self.competition_rate:
            subscribed_shares = int(round(self.offered_shares * self.competition_rate))
        return {
            "name": self.name,
            "offeredShares": self.offered_shares,
            "equalAllocationShares": self.equal_allocation_shares,
            "proportionalAllocationShares": self.proportional_allocation_shares,
            "expectedEqualShares": self.expected_equal_shares,
            "applicationCount": self.application_count,
            "subscribedShares": subscribed_shares,
            "competitionRate": self.competition_rate,
            "proportionalCompetitionRate": self.proportional_competition_rate,
            "depositRate": self.deposit_rate,
            "feeKrw": self.fee_krw,
        }


class SecondaryVideoOcrIngest:
    def __init__(
        self,
        config_path: Path,
        broker_snapshot_dir: Path,
        dry_run: bool,
        include_all_stocks: bool = False,
        debug_frame_dir: Path | None = None,
    ) -> None:
        self.config_path = config_path
        self.broker_snapshot_dir = broker_snapshot_dir
        self.dry_run = dry_run
        self.include_all_stocks = include_all_stocks
        self.debug_frame_dir = debug_frame_dir
        cookies_path_value = os.environ.get("YOUTUBE_COOKIES_PATH", "").strip()
        self.youtube_cookies_path = Path(cookies_path_value) if cookies_path_value else None
        self.finuts_id = os.environ.get("FINUTS_ID", "").strip()
        self.finuts_password = os.environ.get("FINUTS_PASSWORD", "").strip()

    def run(self) -> int:
        config = _read_json(self.config_path)
        sources = self._resolve_sources(config)
        if not isinstance(sources, list) or not sources:
            print("No video OCR sources configured.")
            return 0

        extracted_rows: list[dict[str, Any]] = []
        with tempfile.TemporaryDirectory(prefix="video-ocr-secondary-") as tmp:
            tmpdir = Path(tmp)
            for source in sources:
                if not isinstance(source, dict):
                    continue
                row = self._extract_source(tmpdir, source)
                if row:
                    extracted_rows.append(row)

        if not extracted_rows:
            print("No OCR snapshots extracted.")
            return 0

        rows_by_year: dict[str, list[dict[str, Any]]] = {}
        for row in extracted_rows:
            year = str(row["capturedAt"])[:4]
            rows_by_year.setdefault(year, []).append(row)

        for year, rows in rows_by_year.items():
            target = self.broker_snapshot_dir / f"{year}.json"
            payload = {"schemaVersion": 1, "snapshots": []}
            if target.exists():
                payload = _read_json(target)
            current_rows = payload.get("snapshots", [])
            if not isinstance(current_rows, list):
                current_rows = []
            payload["schemaVersion"] = 1
            payload["snapshots"] = self._merge_rows(current_rows, rows)
            if self.dry_run:
                print(f"[dry-run] would update {target} with {len(rows)} row(s)")
            else:
                _write_json(target, payload)
                print(f"updated {target} with {len(rows)} row(s)")

        return 0

    def _resolve_sources(self, config: dict[str, Any]) -> list[dict[str, Any]]:
        base_sources = [
            source
            for source in config.get("sources", [])
            if isinstance(source, dict)
        ]
        catalog_sources = [
            source
            for source in config.get("catalog", [])
            if isinstance(source, dict)
        ]
        schedule_autoload = config.get("scheduleAutoload", {})
        if not isinstance(schedule_autoload, dict) or not schedule_autoload.get("enabled"):
            return base_sources

        seed_path_value = str(
            schedule_autoload.get("seedPath", "data/ipo_competition_seed.json"),
        ).strip()
        seed_path = Path(seed_path_value)
        if not seed_path.is_absolute():
            seed_path = self.config_path.parent.parent / seed_path
        if not seed_path.exists():
            print(f"schedule autoload skipped; missing seed path: {seed_path}")
            return base_sources

        days_before_start = _to_int(schedule_autoload.get("daysBeforeStart")) or 2
        days_after_end = _to_int(schedule_autoload.get("daysAfterEnd")) or 0

        seed_payload = _read_json(seed_path)
        stocks = seed_payload.get("stocks", [])
        if not isinstance(stocks, list):
            return base_sources

        now_kst = datetime.now(ZoneInfo("Asia/Seoul"))
        now_date = now_kst.date()
        schedule_open_hour = _to_float(schedule_autoload.get("marketOpenHourKst"))
        schedule_close_hour = _to_float(schedule_autoload.get("marketCloseHourKst"))
        existing_ids = {str(source.get("id", "")).strip() for source in base_sources}
        catalog_by_id = {
            str(source.get("id", "")).strip(): source
            for source in catalog_sources
            if str(source.get("id", "")).strip()
        }
        finuts_entries_by_company: dict[str, dict[str, Any]] = {}
        autodiscover_enabled = bool(schedule_autoload.get("finutsAutodiscover", True))
        autodiscover_loaded = False

        autoload_sources: list[dict[str, Any]] = []
        for stock in stocks:
            if not isinstance(stock, dict):
                continue
            stock_id = str(stock.get("id", "")).strip()
            if not stock_id or stock_id in existing_ids:
                continue
            source = catalog_by_id.get(stock_id)
            if source is None:
                # Prefer seeded Finuts source URLs from existing stock snapshots.
                # This avoids hitting Finuts autodiscovery for stocks we already know.
                source = self._build_finuts_source_from_stock(
                    stock,
                    None,
                    now_kst,
                )
            if source is None and autodiscover_enabled:
                if not autodiscover_loaded:
                    autodiscover_loaded = True
                    with suppress(Exception):
                        finuts_entries_by_company = self._discover_finuts_entries()
                source = self._build_finuts_source_from_stock(
                    stock,
                    finuts_entries_by_company.get(
                        self._normalize_company_name(str(stock.get("company", "")).strip())
                    ),
                    now_kst,
                )
            if source is None:
                continue
            if self.include_all_stocks:
                autoload_sources.append(source)
                existing_ids.add(stock_id)
                continue
            start = _parse_date(str(stock.get("subscriptionStart", "")).strip())
            end = _parse_date(str(stock.get("subscriptionEnd", "")).strip()) or start
            if start is None:
                continue
            window_start = start - timedelta(days=days_before_start)
            window_end = end + timedelta(days=days_after_end)
            if not (window_start <= now_date <= window_end):
                continue
            if start <= now_date <= end:
                open_hour = _to_float(source.get("marketOpenHourKst"))
                close_hour = _to_float(source.get("marketCloseHourKst"))
                if open_hour is None:
                    open_hour = schedule_open_hour
                if close_hour is None:
                    close_hour = schedule_close_hour
                if open_hour is not None and close_hour is not None:
                    current_hour = (
                        now_kst.hour + (now_kst.minute / 60) + (now_kst.second / 3600)
                    )
                    if current_hour < open_hour or current_hour > close_hour:
                        continue
                autoload_sources.append(source)
                existing_ids.add(stock_id)

        if autoload_sources:
            print(
                "schedule autoload sources:",
                ", ".join(str(source.get("id")) for source in autoload_sources),
            )
        return [*base_sources, *autoload_sources]

    def _discover_finuts_entries(self) -> dict[str, dict[str, Any]]:
        entries: dict[str, dict[str, Any]] = {}
        ajax_entries = self._fetch_finuts_list_entries()
        if ajax_entries:
            for entry in ajax_entries:
                normalized = self._normalize_company_name(entry.get("company", ""))
                if not normalized:
                    continue
                entries[normalized] = entry
        if not entries:
            for cat in ("04", "05", "06"):
                url = f"https://www.finuts.co.kr/html/ipo/ipoList.php?cat={cat}"
                try:
                    with urlopen(url, timeout=30) as response:
                        html_text = response.read().decode("utf-8", errors="ignore")
                except Exception as exc:
                    print(f"finuts autodiscover skipped for cat {cat}: {exc}")
                    continue
                for entry in self._extract_finuts_entries_from_html(html_text):
                    normalized = self._normalize_company_name(entry.get("company", ""))
                    if not normalized:
                        continue
                    entries[normalized] = entry
        if entries:
            print(f"finuts autodiscover entries: {len(entries)}")
        return entries

    def _fetch_finuts_list_entries(self) -> list[dict[str, Any]]:
        url = "https://www.finuts.co.kr/html/task/ipo/ipoListQuery.php"
        payload = urlencode({"active": "ipo-011", "search_text": ""}).encode()
        request = Request(
            url,
            data=payload,
            headers={
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "X-Requested-With": "XMLHttpRequest",
            },
        )
        try:
            with urlopen(request, timeout=30) as response:
                body = response.read().decode("utf-8", errors="ignore")
        except Exception as exc:
            print(f"finuts ajax autodiscover skipped: {exc}")
            return []
        with suppress(json.JSONDecodeError):
            payload_obj = json.loads(body)
            rows = payload_obj.get("data", [])
            if isinstance(rows, list):
                entries: dict[str, dict[str, Any]] = {}
                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    ipo_sn = str(row.get("IPO_SN", "")).strip()
                    company = self._strip_html(str(row.get("ENT_NM", "")).strip())
                    if not ipo_sn or not company:
                        continue
                    entries[ipo_sn] = {
                        "ipoSn": ipo_sn,
                        "securityType": str(row.get("SE_CD", "")).strip(),
                        "company": company,
                        "finutsUrl": f"https://www.finuts.co.kr/html/ipo/ipoView.php?ipo_sn={ipo_sn}",
                    }
                return list(entries.values())
        return []

    def _extract_finuts_entries_from_html(self, html_text: str) -> list[dict[str, Any]]:
        entries: dict[str, dict[str, Any]] = {}
        patterns = [
            re.compile(
                r"""<tr[^>]*onclick="ipoView\((\d+),'([^']+)'\)"[^>]*>.*?"""
                r"""<td[^>]*>.*?</td>\s*<td>(.*?)</td>""",
                flags=re.IGNORECASE | re.DOTALL,
            ),
            re.compile(
                r"""<div class="date-group" onclick="ipoView\((\d+),'([^']+)'\)".*?"""
                r"""<ul><li>(.*?)</li>""",
                flags=re.IGNORECASE | re.DOTALL,
            ),
        ]
        for pattern in patterns:
            for match in pattern.finditer(html_text):
                ipo_sn, security_type, company_raw = match.groups()
                company = self._strip_html(company_raw)
                if not company:
                    continue
                entries[ipo_sn] = {
                    "ipoSn": ipo_sn,
                    "securityType": security_type,
                    "company": company,
                    "finutsUrl": f"https://www.finuts.co.kr/html/ipo/ipoView.php?ipo_sn={ipo_sn}",
                }
        return list(entries.values())

    def _build_finuts_source_from_stock(
        self,
        stock: dict[str, Any],
        finuts_entry: dict[str, Any] | None,
        now_kst: datetime,
    ) -> dict[str, Any] | None:
        stock_id = str(stock.get("id", "")).strip()
        company = str(stock.get("company", "")).strip()
        fundamentals = stock.get("fundamentals", {})
        if not isinstance(fundamentals, dict):
            fundamentals = {}
        finuts_url = ""
        if isinstance(finuts_entry, dict):
            finuts_url = str(finuts_entry.get("finutsUrl", "")).strip()
        if not finuts_url:
            snapshots = stock.get("snapshots", [])
            if isinstance(snapshots, list):
                for snapshot in snapshots:
                    if not isinstance(snapshot, dict):
                        continue
                    candidate = str(snapshot.get("sourceUrl", "")).strip()
                    if "finuts.co.kr/html/ipo/ipoView.php?ipo_sn=" in candidate:
                        finuts_url = candidate
                        break
        if not finuts_url:
            return None
        offer_price = _to_int(fundamentals.get("offerPrice"))
        lead_managers = stock.get("leadManagers", [])
        if (
            not stock_id
            or not company
            or not _is_valid_company_name(company)
            or not isinstance(lead_managers, list)
            or not lead_managers
        ):
            return None
        brokers: list[dict[str, Any]] = []
        for manager in lead_managers:
            name = str(manager).strip()
            if not name:
                continue
            aliases = sorted(
                {
                    name,
                    self._short_broker_alias(name),
                    self._normalize_broker_name(name),
                }
                - {""}
            )
            brokers.append(
                {
                    "name": name,
                    "aliases": aliases,
                    "depositRate": 0.5,
                    "feeKrw": 2000,
                }
            )
        if not brokers:
            return None
        return {
            "id": stock_id,
            "company": company,
            "capturedAtKst": now_kst.replace(microsecond=0).isoformat(),
            "source": "finuts_member_secondary",
            "sourceLabel": "finuts",
            "sourceUrl": finuts_url,
            "finutsUrl": finuts_url,
            "finutsSearchDepositManwon": 100,
            "offerPrice": offer_price,
            "brokers": brokers,
        }

    def _normalize_company_name(self, value: str) -> str:
        compact = self._strip_html(value)
        compact = re.sub(r"\s+", "", compact)
        compact = compact.replace("(주)", "").replace("주식회사", "")
        return compact.lower()

    def _strip_html(self, value: str) -> str:
        return html.unescape(re.sub(r"<[^>]+>", "", value)).strip()

    def _short_broker_alias(self, value: str) -> str:
        compact = re.sub(r"\s+", "", value)
        for suffix in ["투자증권", "증권", "증권㈜", "(주)", "주식회사"]:
            compact = compact.replace(suffix, "")
        return compact

    def _extract_source(self, tmpdir: Path, source: dict[str, Any]) -> dict[str, Any] | None:
        now_kst = datetime.now(ZoneInfo("Asia/Seoul"))
        youtube_url = str(source.get("youtubeUrl", "")).strip()
        finuts_url = str(source.get("finutsUrl", "")).strip()
        timestamp_seconds = int(source.get("timestampSeconds", 0) or 0)
        live_stream = bool(source.get("liveStream"))
        company = str(source.get("company", "")).strip() or None
        if company and not _is_valid_company_name(company):
            print(f"skip suspicious company source {source.get('id')}: {company}")
            return None
        source_label = str(source.get("sourceLabel", "")).strip() or None
        image_path_value = str(source.get("imagePath", "")).strip()
        frame_path = tmpdir / f"{source['id']}.png"
        if finuts_url and self.finuts_id and self.finuts_password:
            finuts_brokers = self._extract_brokers_from_finuts(source, finuts_url)
            if finuts_brokers:
                print(f"using finuts session data for {source.get('id')}")
                aggregate_offered = sum(item.offered_shares or 0 for item in finuts_brokers)
                aggregate_subscribed = sum(
                    int(round((item.offered_shares or 0) * (item.competition_rate or 0)))
                    for item in finuts_brokers
                )
                aggregate_rate = _to_float(source.get("aggregateCompetitionRate"))
                if not aggregate_rate and aggregate_offered > 0:
                    aggregate_rate = aggregate_subscribed / aggregate_offered
                return {
                    "id": source.get("id"),
                    "company": source.get("company"),
                    "capturedAt": now_kst.replace(microsecond=0).isoformat(),
                    "source": source.get("source", "finuts_member_secondary"),
                    "sourceUrl": source.get("sourceUrl", finuts_url),
                    "aggregateCompetitionRate": aggregate_rate,
                    "brokers": [item.to_json() for item in finuts_brokers],
                    "aggregate": {
                        "offeredShares": aggregate_offered or None,
                        "subscribedShares": aggregate_subscribed or None,
                        "competitionRate": aggregate_rate or None,
                    },
                }
            print(
                f"finuts session unavailable for {source.get('id')}; "
                "falling back to browser/OCR path",
            )
        if image_path_value:
            image_path = Path(image_path_value)
            if not image_path.is_absolute():
                image_path = self.config_path.parent.parent / image_path
            if image_path.exists():
                frame_path.write_bytes(image_path.read_bytes())
            elif youtube_url:
                print(
                    f"missing imagePath for {source.get('id')}, falling back to browser capture: {image_path}"
                )
            else:
                print(f"skip missing imagePath for {source.get('id')}: {image_path}")
                return None
        if not frame_path.exists():
            if not youtube_url:
                print(f"skip invalid source config: {source.get('id')}")
                return None
            browser_error: Exception | None = None
            try:
                self._capture_frame_with_browser(
                    youtube_url,
                    timestamp_seconds,
                    frame_path,
                    live_stream=live_stream,
                    capture_selector=str(source.get("captureSelector", "")).strip() or None,
                    company=company,
                    source_label=source_label,
                )
            except Exception as exc:
                browser_error = exc
                print(f"browser capture failed for {source.get('id')}: {exc}")
            if not frame_path.exists():
                _require_bin("yt-dlp")
                _require_bin("ffmpeg")
                try:
                    self._extract_frame(youtube_url, timestamp_seconds, frame_path)
                except Exception:
                    if browser_error is not None:
                        print(
                            f"skip source after browser and yt-dlp failures: {source.get('id')}"
                        )
                        return None
                    raise

        extracted_brokers, text = self._extract_brokers_from_frame(source, frame_path)

        if not extracted_brokers and company:
            for candidate_url in self._discover_candidate_youtube_urls(company):
                if candidate_url == youtube_url:
                    continue
                retry_path = tmpdir / f"{source['id']}_candidate.png"
                with suppress(Exception):
                    if retry_path.exists():
                        retry_path.unlink()
                try:
                    self._capture_frame_with_browser(
                        candidate_url,
                        timestamp_seconds,
                        retry_path,
                        live_stream=live_stream,
                        capture_selector=str(source.get("captureSelector", "")).strip() or None,
                        company=company,
                        source_label=source_label,
                    )
                except Exception as exc:
                    print(f"candidate browser capture failed for {source.get('id')}: {candidate_url} ({exc})")
                    continue
                candidate_brokers, candidate_text = self._extract_brokers_from_frame(source, retry_path)
                if candidate_brokers:
                    print(f"using discovered youtube candidate for {source.get('id')}: {candidate_url}")
                    frame_path.write_bytes(retry_path.read_bytes())
                    extracted_brokers = candidate_brokers
                    text = candidate_text
                    source["youtubeUrl"] = candidate_url
                    source["sourceUrl"] = candidate_url
                    break

        if not extracted_brokers:
            preview = re.sub(r"\s+", " ", text).strip()[:240]
            if preview:
                print(f"ocr preview for {source.get('id')}: {preview}")
            print(f"no brokers extracted for {source.get('id')}")
            return None

        aggregate_rate = _to_float(source.get("aggregateCompetitionRate"))
        if not aggregate_rate:
            aggregate_rate = max((item.competition_rate or 0 for item in extracted_brokers), default=0)
        aggregate_offered = sum(item.offered_shares or 0 for item in extracted_brokers)
        aggregate_subscribed = sum(
            int(round((item.offered_shares or 0) * (item.competition_rate or 0)))
            for item in extracted_brokers
        )
        return {
            "id": source.get("id"),
            "company": source.get("company"),
            "capturedAt": source.get("capturedAtKst"),
            "source": source.get("source", "youtube_video_ocr_secondary"),
            "sourceUrl": source.get("sourceUrl", youtube_url),
            "aggregateCompetitionRate": aggregate_rate,
            "brokers": [item.to_json() for item in extracted_brokers],
            "aggregate": {
                "offeredShares": aggregate_offered or None,
                "subscribedShares": aggregate_subscribed or None,
                "competitionRate": aggregate_rate or None,
            },
        }

    def _extract_brokers_from_finuts(
        self,
        source: dict[str, Any],
        finuts_url: str,
    ) -> list[BrokerExtraction]:
        try:
            jugansa_rows, altmnt_rows = self._fetch_finuts_ajax_rows(
                finuts_url,
                search_deposit_manwon=int(source.get("finutsSearchDepositManwon", 100) or 100),
            )
        except Exception as exc:
            print(f"skip finuts source after fetch failure: {source.get('id')} ({exc})")
            return []
        if not jugansa_rows:
            return []

        offer_price = _to_int(source.get("offerPrice"))
        search_deposit_krw = (
            int(source.get("finutsSearchDepositManwon", 100) or 100) * 10000
        )
        extracted_brokers: list[BrokerExtraction] = []
        for broker_cfg in source.get("brokers", []):
            if not isinstance(broker_cfg, dict):
                continue
            jugansa = self._match_finuts_broker_row(broker_cfg, jugansa_rows)
            if jugansa is None:
                print(f"skip broker with no finuts match: {broker_cfg.get('name')}")
                continue
            altmnt = self._match_finuts_broker_row(broker_cfg, altmnt_rows)

            offered_shares = _to_int(jugansa.get("ALTMNT_CNT")) or _to_int(
                broker_cfg.get("offeredShares")
            )
            equal_supply = round(offered_shares / 2) if offered_shares else _to_int(
                broker_cfg.get("equalAllocationShares")
            )
            proportional_supply = (
                round(offered_shares / 2)
                if offered_shares
                else _to_int(broker_cfg.get("proportionalAllocationShares"))
            )

            expected_equal = _to_float(jugansa.get("EQLTY_STOCK_CNT"))

            deposit_rate = _to_float(broker_cfg.get("depositRate")) or 0.5
            proportional_shares = (
                _to_float(altmnt.get("PROP_CMPET_ALTMNT"))
                if altmnt is not None
                else None
            )
            proportional_rate = None
            if (
                proportional_shares is not None
                and proportional_shares > 0
                and offer_price
                and offer_price > 0
                and deposit_rate > 0
            ):
                deposit_for_one = search_deposit_krw / proportional_shares
                proportional_rate = deposit_for_one / (offer_price * deposit_rate)

            extracted_brokers.append(
                BrokerExtraction(
                    name=str(broker_cfg.get("name", "")).strip(),
                    offered_shares=offered_shares,
                    equal_allocation_shares=equal_supply,
                    proportional_allocation_shares=proportional_supply,
                    expected_equal_shares=expected_equal,
                    deposit_rate=deposit_rate,
                    fee_krw=_to_int(jugansa.get("FEE")) or _to_int(broker_cfg.get("feeKrw")),
                    competition_rate=_to_float(jugansa.get("SCSCS_CMPET_RT")),
                    proportional_competition_rate=proportional_rate,
                    application_count=None,
                )
            )
        return extracted_brokers

    def _fetch_finuts_ajax_rows(
        self,
        finuts_url: str,
        *,
        search_deposit_manwon: int,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        if not self.finuts_id or not self.finuts_password:
            return [], []
        try:
            return self._fetch_finuts_ajax_rows_via_session(
                finuts_url,
                search_deposit_manwon=search_deposit_manwon,
            )
        except Exception as exc:
            print(f"finuts session fetch failed: {finuts_url} ({exc})")
            return [], []

    def _fetch_finuts_ajax_rows_via_session(
        self,
        finuts_url: str,
        *,
        search_deposit_manwon: int,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        def _open_with_retry(request: str | Request, *, timeout: int = 30) -> str:
            attempts = 3
            last_error: Exception | None = None
            for attempt in range(1, attempts + 1):
                try:
                    return opener.open(request, timeout=timeout).read().decode(
                        "utf-8",
                        errors="ignore",
                    )
                except Exception as exc:
                    last_error = exc
                    if attempt >= attempts:
                        break
                    time.sleep(1.0 * attempt)
            if last_error is not None:
                raise last_error
            return ""

        parsed = urlparse(finuts_url)
        return_path = parsed.path
        if parsed.query:
            return_path = f"{parsed.path}?{parsed.query}"
        login_url = (
            f"{parsed.scheme}://{parsed.netloc}/html/user/login.php?"
            f"url={quote(return_path, safe='/?=&')}"
        )
        cookie_jar = CookieJar()
        opener = build_opener(HTTPCookieProcessor(cookie_jar))
        opener.addheaders = [
            ("User-Agent", "Mozilla/5.0"),
        ]
        login_page = _open_with_retry(login_url)
        token_match = re.search(
            r'<input[^>]+name="_token"[^>]+value="([^"]+)"',
            login_page,
            flags=re.IGNORECASE,
        )
        token = token_match.group(1).strip() if token_match else ""
        if not token:
            return [], []
        login_payload = urlencode(
            {
                "user_id": self.finuts_id,
                "user_pwd": self.finuts_password,
                "save_id": "",
                "_token": token,
            }
        ).encode()
        login_request = Request(
            f"{parsed.scheme}://{parsed.netloc}/html/task/user/ajaxMemberLoginCheck.php",
            data=login_payload,
            headers={
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "X-Requested-With": "XMLHttpRequest",
                "Referer": login_url,
            },
        )
        login_response = _open_with_retry(login_request)
        if '"S"' not in login_response and "S" != login_response.strip():
            return [], []
        ipo_sn = self._extract_finuts_ipo_sn(finuts_url)
        if not ipo_sn:
            return [], []
        jugansa_request = Request(
            f"{parsed.scheme}://{parsed.netloc}/html/task/ipo/ajaxJugansaList.php",
            data=urlencode({"ipo_sn": ipo_sn}).encode(),
            headers={
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "X-Requested-With": "XMLHttpRequest",
                "Referer": finuts_url,
            },
        )
        altmnt_request = Request(
            f"{parsed.scheme}://{parsed.netloc}/html/task/ipo/ajaxAltmntList.php",
            data=urlencode(
                {
                    "ipo_sn": ipo_sn,
                    "search_scscs_wrtm": str(search_deposit_manwon),
                }
            ).encode(),
            headers={
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "X-Requested-With": "XMLHttpRequest",
                "Referer": finuts_url,
            },
        )
        jugansa_text = _open_with_retry(jugansa_request)
        altmnt_text = _open_with_retry(altmnt_request)
        jugansa_rows = json.loads(jugansa_text) if jugansa_text else []
        altmnt_rows = json.loads(altmnt_text) if altmnt_text else []
        if not isinstance(jugansa_rows, list):
            jugansa_rows = []
        if not isinstance(altmnt_rows, list):
            altmnt_rows = []
        return (
            [row for row in jugansa_rows if isinstance(row, dict)],
            [row for row in altmnt_rows if isinstance(row, dict)],
        )

    def _extract_finuts_ipo_sn(self, finuts_url: str) -> str | None:
        query = dict(parse_qsl(urlparse(finuts_url).query, keep_blank_values=True))
        ipo_sn = str(query.get("ipo_sn", "")).strip()
        return ipo_sn or None

    def _match_finuts_broker_row(
        self,
        broker_cfg: dict[str, Any],
        rows: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        aliases = [
            str(value).strip()
            for value in [broker_cfg.get("name"), *(broker_cfg.get("aliases") or [])]
            if str(value).strip()
        ]
        normalized_aliases = [self._normalize_broker_name(value) for value in aliases]
        for row in rows:
            row_name = self._normalize_broker_name(str(row.get("SCRT_CO_NM", "")).strip())
            if not row_name:
                continue
            if row_name in normalized_aliases:
                return row
            for alias in normalized_aliases:
                if alias and (alias in row_name or row_name in alias):
                    return row
        return None

    def _normalize_broker_name(self, value: str) -> str:
        compact = re.sub(r"\s+", "", value)
        for suffix in ["투자증권", "증권", "증권㈜", "(주)", "주식회사"]:
            compact = compact.replace(suffix, "")
        return compact.upper()

    def _extract_brokers_from_frame(
        self,
        source: dict[str, Any],
        frame_path: Path,
    ) -> tuple[list[BrokerExtraction], str]:
        crop = source.get("crop")
        if isinstance(crop, dict):
            self._crop_image(frame_path, crop)

        self._prepare_image_for_ocr(frame_path)
        text = self._ocr_image(frame_path)
        self._write_debug_outputs(source, frame_path, text)
        extracted_brokers: list[BrokerExtraction] = []
        for broker_cfg in source.get("brokers", []):
            if not isinstance(broker_cfg, dict):
                continue
            hints = broker_cfg.get("ocrHints", {})
            competition = _parse_number(text, list(hints.get("competitionPatterns", [])))
            proportional_competition = _parse_number(
                text,
                list(hints.get("proportionalCompetitionPatterns", [])),
            )
            application_count = _parse_number(
                text,
                list(hints.get("applicationCountPatterns", [])),
            )
            if competition is None and proportional_competition is None and application_count is None:
                print(f"skip broker with no OCR match: {broker_cfg.get('name')}")
                continue
            extracted_brokers.append(
                BrokerExtraction(
                    name=str(broker_cfg.get("name", "")).strip(),
                    offered_shares=_to_int(broker_cfg.get("offeredShares")),
                    equal_allocation_shares=_to_int(broker_cfg.get("equalAllocationShares")),
                    proportional_allocation_shares=_to_int(
                        broker_cfg.get("proportionalAllocationShares")
                    ),
                    expected_equal_shares=None,
                    deposit_rate=_to_float(broker_cfg.get("depositRate")),
                    fee_krw=_to_int(broker_cfg.get("feeKrw")),
                    competition_rate=competition,
                    proportional_competition_rate=proportional_competition,
                    application_count=int(application_count) if application_count else None,
                )
            )
        return extracted_brokers, text

    def _write_debug_outputs(
        self,
        source: dict[str, Any],
        frame_path: Path,
        text: str,
    ) -> None:
        if self.debug_frame_dir is None:
            return
        self.debug_frame_dir.mkdir(parents=True, exist_ok=True)
        source_id = str(source.get("id", "unknown")).strip() or "unknown"
        target_image = self.debug_frame_dir / f"{source_id}.png"
        target_text = self.debug_frame_dir / f"{source_id}.txt"
        target_json = self.debug_frame_dir / f"{source_id}.json"
        target_image.write_bytes(frame_path.read_bytes())
        target_text.write_text(text, encoding="utf-8")
        target_json.write_text(
            json.dumps(source, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )

    def _extract_frame(self, youtube_url: str, timestamp_seconds: int, frame_path: Path) -> None:
        stream_url = self._resolve_stream_url(youtube_url)
        subprocess.run(
            [
                "ffmpeg",
                "-y",
                "-ss",
                str(timestamp_seconds),
                "-i",
                stream_url,
                "-frames:v",
                "1",
                str(frame_path),
            ],
            check=True,
            capture_output=True,
            text=True,
        )

    def _capture_frame_with_browser(
        self,
        youtube_url: str,
        timestamp_seconds: int,
        frame_path: Path,
        *,
        live_stream: bool = False,
        capture_selector: str | None = None,
        company: str | None = None,
        source_label: str | None = None,
    ) -> None:
        try:
            from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
            from playwright.sync_api import sync_playwright
        except ImportError as exc:
            raise RuntimeError("playwright is required for browser capture") from exc

        target_url = self._youtube_url_with_timestamp(youtube_url, timestamp_seconds)
        embed_url = self._youtube_embed_url(youtube_url, timestamp_seconds, live_stream=live_stream)
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(
                headless=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--autoplay-policy=no-user-gesture-required",
                    "--disable-dev-shm-usage",
                ],
            )
            context = browser.new_context(
                locale="ko-KR",
                viewport={"width": 1600, "height": 900},
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                color_scheme="light",
            )
            if self.youtube_cookies_path and self.youtube_cookies_path.exists():
                with suppress(Exception):
                    context.add_cookies(self._load_browser_cookies(self.youtube_cookies_path))
            page = context.new_page()
            try:
                page.goto(embed_url if live_stream else target_url, wait_until="domcontentloaded", timeout=120000)
                self._dismiss_youtube_overlays(page)
                page.wait_for_timeout(3500)
                video = page.locator("video").first
                video.wait_for(state="attached", timeout=45000)
                with suppress(Exception):
                    page.evaluate(
                        """() => {
                            const video = document.querySelector('video');
                            if (!video) return;
                            video.muted = true;
                            video.volume = 0;
                            video.play?.();
                        }"""
                    )
                with suppress(Exception):
                    play_button = page.locator(".ytp-large-play-button, .ytp-play-button").first
                    if play_button.is_visible(timeout=1000):
                        play_button.click(timeout=2000)
                if live_stream and self._is_youtube_player_blocked(page):
                    self._open_live_watch_result(page, company=company, source_label=source_label)
                    self._dismiss_youtube_overlays(page)
                    page.wait_for_timeout(3500)
                    video = page.locator("video").first
                    video.wait_for(state="attached", timeout=45000)
                    with suppress(Exception):
                        page.evaluate(
                            """() => {
                                const video = document.querySelector('video');
                                if (!video) return;
                                video.muted = true;
                                video.volume = 0;
                                video.play?.();
                            }"""
                        )
                effective_timestamp = timestamp_seconds
                if not live_stream and effective_timestamp <= 0:
                    with suppress(Exception):
                        duration = page.evaluate(
                            """() => {
                                const video = document.querySelector('video');
                                return video ? Number(video.duration || 0) : 0;
                            }"""
                        )
                        if isinstance(duration, (int, float)) and duration > 5:
                            effective_timestamp = max(int(duration) - 3, 0)
                if not live_stream:
                    with suppress(Exception):
                        page.evaluate(
                            """(seconds) => {
                                const video = document.querySelector('video');
                                if (!video) return;
                                video.muted = true;
                                video.pause();
                                if (seconds > 0) {
                                  video.currentTime = seconds;
                                }
                            }""",
                            effective_timestamp,
                        )
                page.wait_for_timeout(3500)
                self._dismiss_youtube_overlays(page)
                player = page.locator(capture_selector or "#movie_player").first
                try:
                    if live_stream:
                        video.wait_for(state="visible", timeout=10000)
                        video.screenshot(path=str(frame_path))
                    elif capture_selector:
                        player.wait_for(state="visible", timeout=10000)
                        player.screenshot(path=str(frame_path))
                    else:
                        video.screenshot(path=str(frame_path))
                except PlaywrightTimeoutError:
                    page.screenshot(path=str(frame_path), full_page=False)
                except Exception:
                    page.screenshot(path=str(frame_path), full_page=False)
            finally:
                context.close()
                browser.close()

    def _is_youtube_player_blocked(self, page: Any) -> bool:
        with suppress(Exception):
            text = page.locator("body").inner_text(timeout=2000)
            normalized = re.sub(r"\s+", " ", text)
            for marker in [
                "오류 153",
                "Error 153",
                "동영상을 재생할 수 없습니다",
                "재생할 수 없습니다",
                "다른 웹사이트",
                "video unavailable",
            ]:
                if marker.lower() in normalized.lower():
                    return True
        return False

    def _open_live_watch_result(
        self,
        page: Any,
        *,
        company: str | None = None,
        source_label: str | None = None,
    ) -> None:
        terms = " ".join(
            part for part in [company or "", "공모주린이", "LIVE"] if part.strip()
        ).strip() or (source_label or "")
        if not terms:
            return
        page.goto(
            f"https://www.youtube.com/results?search_query={quote_plus(terms)}",
            wait_until="domcontentloaded",
            timeout=120000,
        )
        self._dismiss_youtube_overlays(page)
        page.wait_for_timeout(3000)
        candidates = [
            "ytd-video-renderer a#video-title",
            "ytd-compact-video-renderer a#video-title",
            "a#video-title",
            "a#thumbnail",
        ]
        lowered_company = (company or "").lower()
        for selector in candidates:
            with suppress(Exception):
                links = page.locator(selector)
                count = min(links.count(), 12)
                for index in range(count):
                    link = links.nth(index)
                    title = ""
                    with suppress(Exception):
                        title = (link.get_attribute("title") or link.inner_text(timeout=1000) or "").strip()
                    normalized = title.lower()
                    if lowered_company and lowered_company not in normalized:
                        continue
                    with suppress(Exception):
                        link.click(timeout=3000)
                        page.wait_for_load_state("domcontentloaded", timeout=120000)
                        page.wait_for_timeout(3000)
                        return

    def _dismiss_youtube_overlays(self, page: Any) -> None:
        candidates = [
            "button[aria-label='Accept all']",
            "button[aria-label='모두 수락']",
            "button:has-text('Accept all')",
            "button:has-text('모두 수락')",
            "button:has-text('I agree')",
            "button:has-text('동의')",
            "button[aria-label='닫기']",
            "button[aria-label='Close']",
        ]
        for selector in candidates:
            with suppress(Exception):
                locator = page.locator(selector).first
                if locator.is_visible(timeout=500):
                    locator.click(timeout=1000)
                    page.wait_for_timeout(500)
        with suppress(Exception):
            page.evaluate(
                """() => {
                    const video = document.querySelector('video');
                    if (video) {
                      video.controls = false;
                    }
                    const selectors = [
                      '.ytp-gradient-top',
                      '.ytp-gradient-bottom',
                      '.ytp-chrome-top',
                      '.ytp-chrome-bottom',
                      '.ytp-pause-overlay',
                      '.ytp-ce-element',
                      '.ytp-cards-teaser',
                      '.ytp-cued-thumbnail-overlay',
                      'tp-yt-paper-dialog',
                    ];
                    for (const selector of selectors) {
                      for (const node of document.querySelectorAll(selector)) {
                        node.style.display = 'none';
                      }
                    }
                }"""
            )

    def _youtube_url_with_timestamp(self, youtube_url: str, timestamp_seconds: int) -> str:
        if timestamp_seconds <= 0:
            return youtube_url
        parsed = urlparse(youtube_url)
        query = dict(parse_qsl(parsed.query, keep_blank_values=True))
        query["t"] = f"{timestamp_seconds}s"
        return urlunparse(
            parsed._replace(query=urlencode(query, doseq=True)),
        )

    def _youtube_embed_url(
        self,
        youtube_url: str,
        timestamp_seconds: int,
        *,
        live_stream: bool = False,
    ) -> str:
        parsed = urlparse(youtube_url)
        video_id = ""
        if parsed.netloc.endswith("youtu.be"):
            video_id = parsed.path.strip("/")
        elif "youtube.com" in parsed.netloc:
            query = dict(parse_qsl(parsed.query, keep_blank_values=True))
            video_id = str(query.get("v", "")).strip()
            if not video_id and "/embed/" in parsed.path:
                video_id = parsed.path.split("/embed/", 1)[1].split("/", 1)[0]
        if not video_id:
            return self._youtube_url_with_timestamp(youtube_url, timestamp_seconds)

        params = {
            "autoplay": "1",
            "mute": "1",
            "playsinline": "1",
            "controls": "0",
            "rel": "0",
        }
        if not live_stream and timestamp_seconds > 0:
            params["start"] = str(timestamp_seconds)
        return f"https://www.youtube.com/embed/{video_id}?{urlencode(params)}"

    def _discover_candidate_youtube_urls(self, company: str) -> list[str]:
        query = f"{company} 공모주린이 LIVE"
        attempts = [
            self._yt_dlp_command(
                f"ytsearch5:{query}",
                "--flat-playlist",
                "--dump-single-json",
                "--no-warnings",
            ),
            self._yt_dlp_command(
                f"ytsearch3:{query}",
                "--flat-playlist",
                "--dump-single-json",
                "--no-warnings",
            ),
        ]
        for command in attempts:
            result = subprocess.run(
                command,
                check=False,
                capture_output=True,
                text=True,
            )
            if result.returncode != 0:
                continue
            try:
                payload = json.loads(result.stdout)
            except json.JSONDecodeError:
                continue
            entries = payload.get("entries") or []
            urls: list[str] = []
            lowered_company = company.lower()
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                video_id = str(entry.get("id", "")).strip()
                title = str(entry.get("title", "")).strip()
                channel = str(entry.get("channel", "")).strip()
                if not video_id:
                    continue
                normalized = f"{title} {channel}".lower()
                if lowered_company not in normalized:
                    continue
                urls.append(f"https://www.youtube.com/watch?v={video_id}")
            if urls:
                return urls
        return []

    def _resolve_stream_url(self, youtube_url: str) -> str:
        attempts = [
            self._yt_dlp_command(
                "-g",
                "-f",
                "best[ext=mp4]/best",
                "--extractor-args",
                "youtube:player_client=android",
                youtube_url,
            ),
            self._yt_dlp_command(
                "-g",
                "-f",
                "best[ext=mp4]/best",
                "--extractor-args",
                "youtube:player_client=web",
                youtube_url,
            ),
            self._yt_dlp_command(
                "-g",
                "-f",
                "best",
                youtube_url,
            ),
        ]
        last_error = "unknown yt-dlp failure"
        for command in attempts:
            result = subprocess.run(
                command,
                check=False,
                capture_output=True,
                text=True,
            )
            if result.returncode == 0:
                stream_url = next(
                    (line.strip() for line in result.stdout.splitlines() if line.strip()),
                    "",
                )
                if stream_url:
                    return stream_url
                last_error = "yt-dlp returned success but no stream url"
                continue
            last_error = result.stderr.strip() or result.stdout.strip() or (
                f"yt-dlp exited with code {result.returncode}"
            )
        raise RuntimeError(f"failed to resolve stream url for {youtube_url}: {last_error}")

    def _yt_dlp_command(self, *args: str) -> list[str]:
        command = ["yt-dlp"]
        if self.youtube_cookies_path and self.youtube_cookies_path.exists():
            command.extend(["--cookies", str(self.youtube_cookies_path)])
        command.extend(args)
        return command

    def _load_browser_cookies(self, cookies_path: Path) -> list[dict[str, Any]]:
        cookies: list[dict[str, Any]] = []
        for line in cookies_path.read_text(encoding="utf-8", errors="ignore").splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            parts = stripped.split("\t")
            if len(parts) != 7:
                continue
            domain, include_subdomains, path, secure, expires, name, value = parts
            host_only = include_subdomains.upper() != "TRUE"
            expires_value = float(expires) if expires.isdigit() else -1
            cookie = {
                "name": name,
                "value": value,
                "domain": domain.lstrip("."),
                "path": path or "/",
                "httpOnly": False,
                "secure": secure.upper() == "TRUE",
            }
            if expires_value > 0:
                cookie["expires"] = expires_value
            if host_only:
                cookie["domain"] = domain.lstrip(".")
            cookies.append(cookie)
        return cookies

    def _crop_image(self, frame_path: Path, crop: dict[str, Any]) -> None:
        x = _to_int(crop.get("x")) or 0
        y = _to_int(crop.get("y")) or 0
        width = _to_int(crop.get("width")) or 0
        height = _to_int(crop.get("height")) or 0
        if width <= 0 or height <= 0:
            return
        from PIL import Image

        image = Image.open(frame_path)
        cropped = image.crop((x, y, x + width, y + height))
        cropped.save(frame_path)

    def _prepare_image_for_ocr(self, frame_path: Path) -> None:
        from PIL import Image, ImageEnhance, ImageFilter

        image = Image.open(frame_path).convert("L")
        width, height = image.size
        if width > 0 and height > 0:
            image = image.resize((width * 2, height * 2))
        image = ImageEnhance.Contrast(image).enhance(1.8)
        image = image.filter(ImageFilter.SHARPEN)
        image.save(frame_path)

    def _ocr_image(self, frame_path: Path) -> str:
        try:
            from rapidocr_onnxruntime import RapidOCR
        except ImportError as exc:
            raise RuntimeError("rapidocr_onnxruntime is required") from exc

        engine = RapidOCR()
        result, _ = engine(str(frame_path))
        if not result:
            return ""
        return "\n".join(item[1] for item in result if len(item) >= 2)

    def _merge_rows(
        self,
        current_rows: list[dict[str, Any]],
        incoming_rows: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        def row_key(row: dict[str, Any]) -> tuple[str, str]:
            return (str(row.get("id", "")), str(row.get("source", "")))

        merged: dict[tuple[str, str], dict[str, Any]] = {}
        for row in current_rows:
            if isinstance(row, dict):
                merged[row_key(row)] = row
        for row in incoming_rows:
            key = row_key(row)
            previous = merged.get(key)
            if previous is None:
                merged[key] = row
                continue
            if str(row.get("capturedAt", "")) >= str(previous.get("capturedAt", "")):
                merged[key] = row
        return sorted(merged.values(), key=lambda row: str(row.get("capturedAt", "")))


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config",
        default="data/video_ocr_sources.json",
        help="JSON source configuration file",
    )
    parser.add_argument(
        "--broker-snapshot-dir",
        default="data/broker_snapshots",
        help="Target broker snapshot directory",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and print without writing any files",
    )
    parser.add_argument(
        "--all-stocks",
        action="store_true",
        help="Attempt Finuts matching for every stock in the seed file, ignoring the active subscription window",
    )
    parser.add_argument(
        "--debug-frame-dir",
        default="",
        help="Optional directory to write captured OCR debug frames and text",
    )
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    config_path = Path(args.config)
    if not config_path.exists():
        print(
            f"Config file not found: {config_path}. Copy data/video_ocr_sources.example.json first.",
            file=sys.stderr,
        )
        return 1
    runner = SecondaryVideoOcrIngest(
        config_path=config_path,
        broker_snapshot_dir=Path(args.broker_snapshot_dir),
        dry_run=args.dry_run,
        include_all_stocks=args.all_stocks,
        debug_frame_dir=Path(args.debug_frame_dir) if args.debug_frame_dir else None,
    )
    return runner.run()


def _parse_date(value: str) -> datetime.date | None:
    if not value:
        return None
    with suppress(ValueError):
        return datetime.fromisoformat(value).date()
    return None


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
