"""
One-time CIN bridge builder.

Fetches BSE and NSE equity master data, matches CINs, and populates ticker_bridge.

Companies with a CIN directly in the BSE master → inserted immediately.
Companies without a CIN → queued for entity resolution (capped at 50/day
to protect the 500/month Claude API budget).

Usage:
    python -m scripts.build_cin_bridge [--max-llm 50]
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys

import asyncpg

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from quant.cin_bridge import upsert_ticker_mapping
from quant.migrations import run_quant_migrations
from quant.scrapers.bse_bridge_scraper import fetch_bse_listings
from quant.scrapers.nse_bridge_scraper import fetch_nse_listings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


async def build_bridge(max_llm_per_run: int = 50) -> None:
    db_url = os.environ["DATABASE_URL"]
    pool = await asyncpg.create_pool(db_url, min_size=2, max_size=5)

    await run_quant_migrations(pool)
    logger.info("Quant tables ensured.")

    # 1. Fetch BSE and NSE listings
    logger.info("Fetching BSE listings...")
    bse_listings = await fetch_bse_listings()
    logger.info("Fetching NSE listings...")
    nse_listings = await fetch_nse_listings()

    # Build ISIN → NSE symbol map
    isin_to_nse: dict[str, str] = {l.isin: l.nse_symbol for l in nse_listings if l.isin}

    direct_match = 0
    fuzzy_queued = 0
    llm_queued_today = 0

    async with pool.acquire() as conn:
        for listing in bse_listings:
            if listing.cin:
                # Direct CIN match — verify it exists in master_entities
                exists = await conn.fetchval(
                    "SELECT 1 FROM master_entities WHERE cin = $1", listing.cin
                )
                if not exists:
                    logger.debug("CIN %s from BSE not in master_entities — skipping", listing.cin)
                    continue

                await upsert_ticker_mapping(
                    conn,
                    cin=listing.cin,
                    exchange="BSE",
                    ticker_symbol=listing.bse_code,
                    isin=listing.isin,
                    bse_code=listing.bse_code,
                    company_name_listed=listing.company_name,
                    sector_listed=listing.sector,
                    source="BSE_MASTER",
                )
                direct_match += 1

                # Also add NSE symbol if ISIN is in NSE list
                nse_symbol = isin_to_nse.get(listing.isin)
                if nse_symbol:
                    await upsert_ticker_mapping(
                        conn,
                        cin=listing.cin,
                        exchange="NSE",
                        ticker_symbol=nse_symbol,
                        isin=listing.isin,
                        company_name_listed=listing.company_name,
                        sector_listed=listing.sector,
                        source="NSE_EQUITY_L",
                    )
            else:
                # No CIN in BSE master — queue for entity resolution
                if llm_queued_today >= max_llm_per_run:
                    break

                already_queued = await conn.fetchval(
                    """
                    SELECT 1 FROM entity_resolution_queue
                    WHERE raw_name = $1 AND identifier_type = 'BSE_CODE'
                    LIMIT 1
                    """,
                    listing.company_name,
                )
                if already_queued:
                    continue

                await conn.execute(
                    """
                    INSERT INTO entity_resolution_queue
                        (raw_name, identifier_type, identifier_value, source, created_at)
                    VALUES ($1, 'BSE_CODE', $2, 'BSE_MASTER', NOW())
                    ON CONFLICT DO NOTHING
                    """,
                    listing.company_name,
                    listing.bse_code,
                )
                fuzzy_queued += 1
                llm_queued_today += 1

    await pool.close()

    logger.info(
        "Bridge build complete. Direct matches: %d, Queued for resolution: %d",
        direct_match,
        fuzzy_queued,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Build CIN ↔ ticker bridge")
    parser.add_argument(
        "--max-llm",
        type=int,
        default=50,
        help="Max entity resolution LLM calls this run (default 50)",
    )
    args = parser.parse_args()
    asyncio.run(build_bridge(max_llm_per_run=args.max_llm))


if __name__ == "__main__":
    main()
