#!/usr/bin/env python3
"""Download MCA company master data from data.gov.in API."""
from __future__ import annotations

import csv
import os
import sys
import time
from pathlib import Path

import httpx

API_KEY = os.getenv("DATA_GOV_API_KEY", "")
RESOURCE_ID = "4dbe5667-7b6b-41d7-82af-211562424d9a"
BASE_URL = f"https://api.data.gov.in/resource/{RESOURCE_ID}"
BATCH_SIZE = 5000
OUTPUT_PATH = Path(os.getenv("OGD_CSV_PATH", "/root/bi/data/ogd_companies.csv"))


def fetch_batch(offset: int) -> tuple[list[dict], int]:
    params = {
        "api-key": API_KEY,
        "format": "json",
        "limit": BATCH_SIZE,
        "offset": offset,
    }
    for attempt in range(3):
        try:
            r = httpx.get(BASE_URL, params=params, timeout=120.0)
            r.raise_for_status()
            data = r.json()
            total = int(data.get("total", 0))
            records = data.get("records", [])
            return records, total
        except Exception as exc:
            if attempt == 2:
                print(f"  ERROR at offset={offset}: {exc}", file=sys.stderr)
                return [], 0
            time.sleep(5)
    return [], 0


def main() -> None:
    if not API_KEY:
        print("DATA_GOV_API_KEY is required.", file=sys.stderr)
        raise SystemExit(1)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Get total count first
    _, total = fetch_batch(0)
    if total == 0:
        print("No records returned. Check API key.", file=sys.stderr)
        raise SystemExit(1)

    print(f"Total records to download: {total:,}")

    # Resume support: count existing rows
    offset = 0
    write_mode = "w"
    write_header = True
    if OUTPUT_PATH.exists():
        with OUTPUT_PATH.open("r", encoding="utf-8") as f:
            existing = sum(1 for _ in f)
        if existing > 1:
            offset = existing - 1  # subtract header row
            write_mode = "a"
            write_header = False
            print(f"Resuming from offset {offset:,} ({offset:,} records already downloaded)")

    downloaded = offset

    with OUTPUT_PATH.open(write_mode, newline="", encoding="utf-8") as fh:
        writer = None
        while offset < total:
            records, _ = fetch_batch(offset)
            if not records:
                print(f"  Empty response at offset {offset}, retrying in 10s...", flush=True)
                time.sleep(10)
                records, _ = fetch_batch(offset)
                if not records:
                    print(f"  Giving up at offset {offset}. Re-run to resume.", file=sys.stderr)
                    break

            if writer is None:
                writer = csv.DictWriter(fh, fieldnames=list(records[0].keys()))
                if write_header:
                    writer.writeheader()

            writer.writerows(records)
            downloaded += len(records)
            offset += len(records)

            pct = (downloaded / total) * 100
            print(f"  {downloaded:,} / {total:,} ({pct:.1f}%)", flush=True)

            if len(records) < BATCH_SIZE:
                break

            time.sleep(0.5)

    print(f"\nDone. {downloaded:,} records saved to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
