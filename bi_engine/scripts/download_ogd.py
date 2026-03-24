#!/usr/bin/env python3
from __future__ import annotations

import csv
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import httpx
from dotenv import load_dotenv


OGD_URL = "https://data.gov.in/resource/list-all-active-companies"
CACHE_MAX_AGE = timedelta(days=7)


def fail(message: str) -> None:
    print(message, file=sys.stderr)
    raise SystemExit(1)


def get_csv_path() -> Path:
    load_dotenv()
    value = os.getenv("OGD_CSV_PATH", "").strip()
    if not value:
        fail("OGD_CSV_PATH is required.")
    return Path(value)


def is_fresh(path: Path) -> bool:
    if not path.is_file():
        return False
    modified_at = datetime.fromtimestamp(path.stat().st_mtime)
    return datetime.now() - modified_at < CACHE_MAX_AGE


def count_records(path: Path) -> int:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        try:
            next(reader)
        except StopIteration:
            return 0
        return sum(1 for _ in reader)


def main() -> None:
    csv_path = get_csv_path()
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    if is_fresh(csv_path):
        print("Using cached file")
        return

    try:
        with httpx.Client(follow_redirects=True, timeout=120.0) as client:
            response = client.get(OGD_URL)
            response.raise_for_status()
    except httpx.HTTPError as exc:
        fail(f"Failed to download OGD CSV: {exc}")

    csv_path.write_bytes(response.content)
    size_bytes = csv_path.stat().st_size
    record_count = count_records(csv_path)

    print(f"Downloaded {size_bytes} bytes")
    print(f"Record count: {record_count}")


if __name__ == "__main__":
    main()
