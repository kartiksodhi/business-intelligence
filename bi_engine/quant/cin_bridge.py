"""
CIN ↔ NSE/BSE ticker bridge.

Core mapping layer: given a CIN, return its exchange ticker symbol(s).
Given a ticker, return its CIN.

The bridge is populated by build_cin_bridge.py (one-time) and
refreshed monthly by quant_scheduler.py.
"""

from __future__ import annotations

import logging
from typing import Optional

import asyncpg

logger = logging.getLogger(__name__)


async def get_tickers_for_cin(
    db: asyncpg.Connection,
    cin: str,
) -> list[dict]:
    """
    Return all active ticker mappings for a CIN.
    Result: [{"exchange": "BSE", "ticker_symbol": "500325", "isin": "..."}, ...]
    """
    rows = await db.fetch(
        """
        SELECT exchange, ticker_symbol, isin, bse_code, sector_listed, market_cap_cr
        FROM ticker_bridge
        WHERE cin = $1 AND is_active = TRUE
        """,
        cin,
    )
    return [dict(r) for r in rows]


async def get_cin_for_ticker(
    db: asyncpg.Connection,
    ticker_symbol: str,
    exchange: str = "BSE",
) -> Optional[str]:
    """Return the CIN for a given ticker symbol, or None if not mapped."""
    row = await db.fetchrow(
        """
        SELECT cin FROM ticker_bridge
        WHERE ticker_symbol = $1 AND exchange = $2 AND is_active = TRUE
        """,
        ticker_symbol,
        exchange,
    )
    return row["cin"] if row else None


async def get_all_listed_cins(db: asyncpg.Connection) -> list[str]:
    """Return all CINs that have at least one active ticker mapping."""
    rows = await db.fetch(
        "SELECT DISTINCT cin FROM ticker_bridge WHERE is_active = TRUE"
    )
    return [r["cin"] for r in rows]


async def upsert_ticker_mapping(
    db: asyncpg.Connection,
    cin: str,
    exchange: str,
    ticker_symbol: str,
    isin: Optional[str] = None,
    bse_code: Optional[str] = None,
    company_name_listed: Optional[str] = None,
    sector_listed: Optional[str] = None,
    source: str = "BSE_MASTER",
) -> None:
    """
    Insert or update a CIN ↔ ticker mapping.
    On conflict (cin, exchange), update symbol and verification timestamp.
    """
    await db.execute(
        """
        INSERT INTO ticker_bridge
            (cin, exchange, ticker_symbol, isin, bse_code,
             company_name_listed, sector_listed, source, last_verified_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
        ON CONFLICT (cin, exchange) DO UPDATE
            SET ticker_symbol        = EXCLUDED.ticker_symbol,
                isin                 = COALESCE(EXCLUDED.isin, ticker_bridge.isin),
                bse_code             = COALESCE(EXCLUDED.bse_code, ticker_bridge.bse_code),
                company_name_listed  = COALESCE(EXCLUDED.company_name_listed, ticker_bridge.company_name_listed),
                sector_listed        = COALESCE(EXCLUDED.sector_listed, ticker_bridge.sector_listed),
                last_verified_at     = NOW(),
                is_active            = TRUE
        """,
        cin, exchange, ticker_symbol, isin, bse_code,
        company_name_listed, sector_listed, source,
    )


async def deactivate_stale_mappings(
    db: asyncpg.Connection,
    days_stale: int = 45,
) -> int:
    """Mark mappings not verified in `days_stale` days as inactive (likely delisted)."""
    result = await db.execute(
        """
        UPDATE ticker_bridge
        SET is_active = FALSE
        WHERE last_verified_at < NOW() - ($1 || ' days')::INTERVAL
          AND is_active = TRUE
        """,
        str(days_stale),
    )
    # Parse "UPDATE N" result string
    n = int(result.split()[-1]) if result else 0
    if n:
        logger.info("Deactivated %d stale ticker mappings", n)
    return n
