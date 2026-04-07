"""
One-time historical price backfill from BSE Bhav Copy archive.

Downloads Bhav Copy for the specified date range and inserts into price_daily.
Rate-limited to 1 request/second to avoid BSE rate-limiting.

Usage:
    python -m scripts.backfill_prices --start 2022-01-01 --end 2024-12-31

Estimate: ~750 trading days × ~5500 rows = ~4M rows. Runs in ~15-20 minutes.
Run off-hours to avoid impacting the main pipeline.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from datetime import date

import asyncpg

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from quant.market_data import ingest_bhav_records
from quant.migrations import run_quant_migrations
from quant.scrapers.bhav_copy_fetcher import fetch_bhav_copy

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


async def backfill(start: date, end: date) -> None:
    pool = await asyncpg.create_pool(os.environ["DATABASE_URL"], min_size=2, max_size=5)
    await run_quant_migrations(pool)

    current = start
    total_inserted = 0
    days_fetched = 0

    while current <= end:
        # Skip weekends
        if current.weekday() >= 5:
            current = current.replace(day=current.day + 1) if current.day < 28 else date(
                current.year + (current.month // 12),
                (current.month % 12) + 1 if current.month < 12 else 1,
                1,
            )
            from datetime import timedelta
            current += timedelta(days=1)
            continue

        records = await fetch_bhav_copy(current)
        if records:
            inserted = await ingest_bhav_records(pool, records)
            total_inserted += inserted
            days_fetched += 1
            logger.info("Backfilled %s: %d records (total so far: %d)", current, len(records), total_inserted)
        else:
            logger.debug("No data for %s (holiday or future date)", current)

        from datetime import timedelta
        current += timedelta(days=1)
        await asyncio.sleep(1.0)  # Rate limit: 1 req/sec

    await pool.close()
    logger.info("Backfill complete. %d trading days, %d total rows inserted.", days_fetched, total_inserted)


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill historical BSE prices")
    parser.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--end",   required=True, help="End date YYYY-MM-DD")
    args = parser.parse_args()

    start = date.fromisoformat(args.start)
    end   = date.fromisoformat(args.end)
    asyncio.run(backfill(start, end))


if __name__ == "__main__":
    main()
