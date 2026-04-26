#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


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
            "applicationCount": self.application_count,
            "subscribedShares": subscribed_shares,
            "competitionRate": self.competition_rate,
            "proportionalCompetitionRate": self.proportional_competition_rate,
            "depositRate": self.deposit_rate,
            "feeKrw": self.fee_krw,
        }


class SecondaryVideoOcrIngest:
    def __init__(self, config_path: Path, broker_snapshot_dir: Path, dry_run: bool) -> None:
        self.config_path = config_path
        self.broker_snapshot_dir = broker_snapshot_dir
        self.dry_run = dry_run

    def run(self) -> int:
        config = _read_json(self.config_path)
        sources = config.get("sources", [])
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

    def _extract_source(self, tmpdir: Path, source: dict[str, Any]) -> dict[str, Any] | None:
        youtube_url = str(source.get("youtubeUrl", "")).strip()
        timestamp_seconds = int(source.get("timestampSeconds", 0) or 0)
        image_path_value = str(source.get("imagePath", "")).strip()
        frame_path = tmpdir / f"{source['id']}.png"
        if image_path_value:
            image_path = Path(image_path_value)
            if not image_path.is_absolute():
                image_path = self.config_path.parent.parent / image_path
            if not image_path.exists():
                print(f"skip missing imagePath for {source.get('id')}: {image_path}")
                return None
            frame_path.write_bytes(image_path.read_bytes())
        else:
            if not youtube_url:
                print(f"skip invalid source config: {source.get('id')}")
                return None
            _require_bin("yt-dlp")
            _require_bin("ffmpeg")
            self._extract_frame(youtube_url, timestamp_seconds, frame_path)

        crop = source.get("crop")
        if isinstance(crop, dict):
            self._crop_image(frame_path, crop)

        text = self._ocr_image(frame_path)
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
                    deposit_rate=_to_float(broker_cfg.get("depositRate")),
                    fee_krw=_to_int(broker_cfg.get("feeKrw")),
                    competition_rate=competition,
                    proportional_competition_rate=proportional_competition,
                    application_count=int(application_count) if application_count else None,
                )
            )

        if not extracted_brokers:
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

    def _resolve_stream_url(self, youtube_url: str) -> str:
        attempts = [
            [
                "yt-dlp",
                "-g",
                "-f",
                "best[ext=mp4]/best",
                "--extractor-args",
                "youtube:player_client=android",
                youtube_url,
            ],
            [
                "yt-dlp",
                "-g",
                "-f",
                "best[ext=mp4]/best",
                "--extractor-args",
                "youtube:player_client=web",
                youtube_url,
            ],
            [
                "yt-dlp",
                "-g",
                "-f",
                "best",
                youtube_url,
            ],
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
    )
    return runner.run()


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
