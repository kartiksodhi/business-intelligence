"""
Market data layer.

Wraps price_daily table for queries needed by factor_engine, backtester,
and portfolio_engine. Also handles Bhav Copy ingestion.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Optional

import asyncpg

logger = logging.getLogger(__name__)

# Nifty 50 index BSE code (used as benchmark)
NIFTY50_INDEX_CODE = "999901"
SENSEX_INDEX_CODE  = "999904"


async def get_close_price(
    db: asyncpg.Connection,
    ticker_symbol: str,
    trade_date: date,
    exchange: str = "BSE",
    max_lag_days: int = 5,
) -> Optional[float]:
    """
    Return close price on or nearest after trade_date (up to max_lag_days).
    Handles holidays/weekends by looking forward.
    """
    row = await db.fetchrow(
        """
        SELECT close_price, trade_date
        FROM price_daily
        WHERE ticker_symbol = $1
          AND exchange = $2
          AND trade_date BETWEEN $3 AND $3 + ($4 || ' days')::INTERVAL
        ORDER BY trade_date ASC
        LIMIT 1
        """,
        ticker_symbol,
        exchange,
        trade_date,
        str(max_lag_days),
    )
    return float(row["close_price"]) if row else None


async def get_forward_price(
    db: asyncpg.Connection,
    ticker_symbol: str,
    from_date: date,
    days_forward: int,
    exchange: str = "BSE",
) -> Optional[float]:
    """
    Return the close price closest to from_date + days_forward.
    ±3 day tolerance for holidays.
    """
    target = from_date + timedelta(days=days_forward)
    row = await db.fetchrow(
        """
        SELECT close_price
        FROM price_daily
        WHERE ticker_symbol = $1
          AND exchange = $2
          AND trade_date BETWEEN $3 - INTERVAL '3 days' AND $3 + INTERVAL '3 days'
        ORDER BY ABS(trade_date - $3) ASC
        LIMIT 1
        """,
        ticker_symbol,
        exchange,
        target,
    )
    return float(row["close_price"]) if row else None


async def get_average_daily_value(
    db: asyncpg.Connection,
    ticker_symbol: str,
    exchange: str = "BSE",
    lookback_days: int = 30,
) -> Optional[float]:
    """
    Return average daily traded value (in crores) over last N days.
    Used by portfolio_engine for liquidity filter (min ₹50L ADV = 0.5 cr).
    """
    row = await db.fetchrow(
        """
        SELECT AVG(value_cr) AS adv
        FROM price_daily
        WHERE ticker_symbol = $1
          AND exchange = $2
          AND trade_date >= CURRENT_DATE - ($3 || ' days')::INTERVAL
          AND value_cr IS NOT NULL
        """,
        ticker_symbol,
        exchange,
        str(lookback_days),
    )
    return float(row["adv"]) if row and row["adv"] else None


async def ingest_bhav_records(
    pool: asyncpg.Pool,
    records: list[dict],
    batch_size: int = 500,
) -> int:
    """
    Bulk-insert Bhav Copy records into price_daily.
    Idempotent: ON CONFLICT DO NOTHING.
    Returns number of rows inserted.
    """
    if not records:
        return 0

    inserted = 0
    async with pool.acquire() as conn:
        for i in range(0, len(records), batch_size):
            batch = records[i : i + batch_size]
            await conn.executemany(
                """
                INSERT INTO price_daily
                    (ticker_symbol, exchange, trade_date, open_price, high_price,
                     low_price, close_price, prev_close, volume, value_cr,
                     delivery_qty, delivery_pct)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
                ON CONFLICT (ticker_symbol, exchange, trade_date) DO NOTHING
                """,
                [
                    (
                        r["ticker_symbol"], r["exchange"], r["trade_date"],
                        r.get("open_price"), r.get("high_price"), r.get("low_price"),
                        r["close_price"], r.get("prev_close"), r.get("volume"),
                        r.get("value_cr"), r.get("delivery_qty"), r.get("delivery_pct"),
                    )
                    for r in batch
                ],
            )
            inserted += len(batch)

    logger.info("Ingested %d price records", inserted)
    return inserted


async def get_latest_trade_date(
    db: asyncpg.Connection,
    exchange: str = "BSE",
) -> Optional[date]:
    """Return the most recent trade date in price_daily."""
    row = await db.fetchrow(
        "SELECT MAX(trade_date) AS latest FROM price_daily WHERE exchange = $1",
        exchange,
    )
    return row["latest"] if row else None
