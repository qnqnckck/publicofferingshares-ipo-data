#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--stock-id", required=True)
    parser.add_argument("--ipo-data-dir", default="ipo_competition_data/stocks")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    stock_path = Path(args.ipo_data_dir) / f"{args.stock_id.strip()}.json"
    if not stock_path.exists():
        print(f"Stock file not found: {stock_path}")
        return 1

    payload: dict[str, Any] = json.loads(
        stock_path.read_text(encoding="utf-8"),
    )
    snapshots = payload.get("snapshots", [])
    if not isinstance(snapshots, list) or not snapshots:
        print(f"No snapshots found after backfill: {stock_path}")
        return 1

    print(f"snapshot_count={len(snapshots)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
